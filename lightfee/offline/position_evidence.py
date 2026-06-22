"""Per-position lifecycle and ledger evidence for production audits."""

from __future__ import annotations

from decimal import Decimal
import re
from typing import Any


_LIVE_RECOVERED_RE = re.compile(
    r"^live-recovered:(?P<symbol>[^:]+):(?P<long>[^:\-]+)->(?P<short>[^:]+)$"
)

_ABNORMAL_REASON_MARKERS = (
    "fallback_live_",
    "passive_close_hedge_deadline_compensated_flat",
    "terminal_maker_filled_unhedged_retry",
    "drive_unhedged_gap",
)


def build_position_evidence_matrix(
    *,
    events: list[dict[str, Any]],
    ledger_rows: list[dict[str, Any]],
    quick_flat_threshold_ms: int = 120_000,
) -> dict[str, Any]:
    positions: dict[str, dict[str, Any]] = {}

    for event in sorted(events, key=lambda rec: int(rec.get("ts_ms") or 0)):
        kind = str(event.get("kind") or "")
        payload = event.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        owner_id = _event_owner_id(kind, payload)
        if not owner_id:
            continue
        position = positions.setdefault(owner_id, _new_position(owner_id))
        _apply_event(position, kind, int(event.get("ts_ms") or 0), payload)

    for position in positions.values():
        _finalize_position_identity(position)

    unattributed_rows: list[dict[str, Any]] = []
    for ledger_row in ledger_rows:
        matched_id = _match_ledger_row(ledger_row, positions)
        if not matched_id:
            unattributed_rows.append({
                **ledger_row,
                "unattributed_reason": _unattributed_reason(ledger_row, positions),
            })
            continue
        _add_ledger_row(positions[matched_id], ledger_row)

    summary_counts: dict[str, int] = {}
    for position in positions.values():
        classification = _classify_position(position, quick_flat_threshold_ms)
        position["classification"] = classification
        summary_counts[f"{classification}_count"] = (
            summary_counts.get(f"{classification}_count", 0) + 1
        )

    summary_counts.setdefault("normal_count", 0)
    summary_counts.setdefault("seconds_open_close_count", 0)
    summary_counts.setdefault("abnormal_quick_terminal_count", 0)
    summary_counts.setdefault("abnormal_recovered_count", 0)
    summary_counts.setdefault("admission_aborted_no_open_count", 0)
    summary_counts["unattributed_ledger_row_count"] = len(unattributed_rows)
    summary_counts["unattributed_ledger_reason_counts"] = _reason_counts(
        row["unattributed_reason"] for row in unattributed_rows
    )

    return {
        "positions": positions,
        "summary": dict(sorted(summary_counts.items())),
        "unattributed_ledger_rows": unattributed_rows,
    }


def _new_position(position_id: str) -> dict[str, Any]:
    return {
        "position_id": position_id,
        "symbol": "",
        "venues": [],
        "events": {},
        "event_kinds": [],
        "reasons": [],
        "has_entry_opened": False,
        "has_entry_aborted": False,
        "open_ts_ms": 0,
        "terminal_ts_ms": 0,
        "entry_reason": "",
        "terminal_reason": "",
        "close_source": "",
        "order_ids": [],
        "client_order_ids": [],
        "exchange_truth_flat": None,
        "exchange_truth_no_open_orders": None,
        "duration_ms": None,
        "ledger": {
            "row_count": 0,
            "net_amount": "0",
            "fee_amount": "0",
            "rows": [],
        },
    }


def _event_owner_id(kind: str, payload: dict[str, Any]) -> str:
    for key in ("position_id", "entry_id", "internal_entry_id", "pending_id"):
        value = str(payload.get(key) or "")
        if value:
            return value
    if kind == "entry.aborted":
        symbol = str(payload.get("symbol") or "").upper()
        return str(payload.get("entry_id") or payload.get("pending_id") or symbol)
    return ""


def _apply_event(position: dict[str, Any], kind: str, ts_ms: int, payload: dict[str, Any]) -> None:
    position["events"][kind] = int(position["events"].get(kind, 0) or 0) + 1
    position["event_kinds"].append(kind)
    symbol = str(payload.get("symbol") or "").upper()
    if symbol and not position["symbol"]:
        position["symbol"] = symbol
    _merge_venues(position, payload)
    _merge_order_anchors(position, payload)
    _merge_exchange_truth(position, payload)
    reason = str(
        payload.get("terminal_reason")
        or payload.get("reason")
        or payload.get("source")
        or ""
    )
    if reason and reason not in position["reasons"]:
        position["reasons"].append(reason)
    if kind == "entry.opened":
        position["has_entry_opened"] = True
        position["open_ts_ms"] = position["open_ts_ms"] or ts_ms
        position["entry_reason"] = position["entry_reason"] or reason
    if kind == "entry.aborted":
        position["has_entry_aborted"] = True
        position["terminal_ts_ms"] = ts_ms
        position["entry_reason"] = position["entry_reason"] or reason
    if kind in {
        "runtime.position_lifecycle_terminal",
        "recovery.flat",
        "exit.closed",
        "exit.reconciled",
        "entry.passive_unfilled",
    }:
        position["terminal_ts_ms"] = max(int(position["terminal_ts_ms"] or 0), ts_ms)
        terminal_reason = str(
            payload.get("terminal_reason")
            or payload.get("reason")
            or payload.get("source")
            or ""
        )
        if terminal_reason:
            position["terminal_reason"] = terminal_reason
        source = str(payload.get("source") or "")
        if source:
            position["close_source"] = source


def _merge_venues(position: dict[str, Any], payload: dict[str, Any]) -> None:
    venues = list(position.get("venues") or [])
    for key in ("long_venue", "short_venue", "maker_venue", "hedge_venue", "venue"):
        venue = str(payload.get(key) or "").lower()
        if venue and venue not in venues:
            venues.append(venue)
    raw_venues = payload.get("venues")
    if isinstance(raw_venues, list):
        for item in raw_venues:
            venue = str(item or "").lower()
            if venue and venue not in venues:
                venues.append(venue)
    position["venues"] = venues


def _merge_order_anchors(position: dict[str, Any], payload: dict[str, Any]) -> None:
    for target_key, payload_keys in {
        "order_ids": ("order_id", "maker_order_id", "hedge_order_id", "order_ids"),
        "client_order_ids": (
            "client_order_id",
            "maker_client_order_id",
            "hedge_client_order_id",
            "client_order_ids",
        ),
    }.items():
        values = list(position.get(target_key) or [])
        for payload_key in payload_keys:
            raw = payload.get(payload_key)
            items = raw if isinstance(raw, list) else [raw]
            for item in items:
                text = str(item or "")
                if text and text not in values:
                    values.append(text)
        position[target_key] = values


def _merge_exchange_truth(position: dict[str, Any], payload: dict[str, Any]) -> None:
    truth_payload = payload.get("exchange_truth")
    truth = truth_payload if isinstance(truth_payload, dict) else {}
    flat = _first_bool(
        payload.get("exchange_truth_flat"),
        payload.get("live_truth_flat"),
        truth.get("exchange_truth_flat"),
        truth.get("live_truth_flat"),
        truth.get("flat"),
    )
    no_open_orders = _first_bool(
        payload.get("exchange_truth_no_open_orders"),
        payload.get("live_truth_no_open_orders"),
        truth.get("exchange_truth_no_open_orders"),
        truth.get("live_truth_no_open_orders"),
        truth.get("open_orders_flat"),
    )
    if flat is not None:
        position["exchange_truth_flat"] = flat
    if no_open_orders is not None:
        position["exchange_truth_no_open_orders"] = no_open_orders


def _finalize_position_identity(position: dict[str, Any]) -> None:
    match = _LIVE_RECOVERED_RE.match(str(position.get("position_id") or ""))
    if match:
        if not position["symbol"]:
            position["symbol"] = match.group("symbol").upper()
        venues = [match.group("long").lower(), match.group("short").lower()]
        position["venues"] = list(dict.fromkeys([*position.get("venues", []), *venues]))
    if position["open_ts_ms"] and position["terminal_ts_ms"]:
        position["duration_ms"] = max(
            0,
            int(position["terminal_ts_ms"]) - int(position["open_ts_ms"]),
        )


def _match_ledger_row(row: dict[str, Any], positions: dict[str, dict[str, Any]]) -> str:
    explicit_id = str(
        row.get("position_id")
        or row.get("entry_id")
        or row.get("owner_id")
        or ""
    )
    if explicit_id in positions:
        return explicit_id

    symbol = str(row.get("symbol") or "").upper()
    venue = str(row.get("venue") or "").lower()
    ts_ms = _safe_int(row.get("ts_ms"))
    candidates: list[tuple[int, str]] = []
    for position_id, position in positions.items():
        if symbol and str(position.get("symbol") or "").upper() != symbol:
            continue
        venues = [str(item).lower() for item in position.get("venues", [])]
        if venue and venues and venue not in venues:
            continue
        start = int(position.get("open_ts_ms") or position.get("terminal_ts_ms") or 0)
        end = int(position.get("terminal_ts_ms") or position.get("open_ts_ms") or 0)
        if start and end and ts_ms:
            low = min(start, end) - 10 * 60_000
            high = max(start, end) + 10 * 60_000
            if not (low <= ts_ms <= high):
                continue
            candidates.append((abs(ts_ms - end), position_id))
        else:
            candidates.append((0, position_id))
    if len(candidates) == 1:
        return candidates[0][1]
    if candidates:
        candidates.sort()
        return candidates[0][1]
    return ""


def _add_ledger_row(position: dict[str, Any], row: dict[str, Any]) -> None:
    ledger = position["ledger"]
    amount = _decimal(row.get("amount") or row.get("income") or row.get("pnl"))
    fee = _decimal(row.get("fee") or row.get("commission"))
    ledger["row_count"] += 1
    ledger["net_amount"] = str(_decimal(ledger["net_amount"]) + amount + fee)
    ledger["fee_amount"] = str(_decimal(ledger["fee_amount"]) + fee)
    ledger["rows"].append(dict(row))


def _classify_position(position: dict[str, Any], quick_flat_threshold_ms: int) -> str:
    position_id = str(position.get("position_id") or "")
    reasons = " ".join(str(item) for item in position.get("reasons", []))
    ledger_rows = int(position.get("ledger", {}).get("row_count") or 0)
    if position_id.startswith("live-recovered:"):
        return "abnormal_recovered"
    if position.get("has_entry_aborted") and not position.get("has_entry_opened"):
        return "aborted_with_ledger" if ledger_rows else "admission_aborted_no_open"
    abnormal = any(marker in reasons for marker in _ABNORMAL_REASON_MARKERS)
    duration = position.get("duration_ms")
    quick = (
        isinstance(duration, int)
        and duration <= int(quick_flat_threshold_ms)
        and position.get("has_entry_opened")
    )
    if quick and abnormal:
        return "abnormal_quick_terminal"
    if quick:
        return "seconds_open_close"
    if abnormal:
        return "abnormal"
    return "normal" if position.get("has_entry_opened") else "diagnostic_only"


def _unattributed_reason(row: dict[str, Any], positions: dict[str, dict[str, Any]]) -> str:
    symbol = str(row.get("symbol") or "").upper()
    if not symbol:
        return "audit_missing_symbol"
    same_symbol = [
        position
        for position in positions.values()
        if str(position.get("symbol") or "").upper() == symbol
    ]
    if not same_symbol:
        return "business_ledger_without_lifecycle"
    if any(
        position.get("has_entry_aborted") and not position.get("has_entry_opened")
        for position in same_symbol
    ):
        return "business_aborted_entry_with_ledger_gap"
    return "audit_unmatched_time_or_venue"


def _reason_counts(reasons: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for reason in reasons:
        text = str(reason or "unknown")
        counts[text] = counts.get(text, 0) + 1
    return dict(sorted(counts.items()))


def _decimal(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _first_bool(*values: Any) -> bool | None:
    for value in values:
        if isinstance(value, bool):
            return value
    return None
