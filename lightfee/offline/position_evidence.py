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
    "passive_close_hedge_deadline_breached",
    "passive_close_min_notional_compensated_flat",
    "passive_close_unhedged_residual",
    "terminal_maker_filled_unhedged_retry",
    "drive_unhedged_gap",
    "exit.close_residual_detected",
    "residual",
)

_ROOT_CAUSE_AUDIT_MATCHER_GAP = "audit_matcher_gap"
_ROOT_CAUSE_BUSINESS_EVENT_MISSING_ANCHOR = "business_event_missing_anchor"
_ROOT_CAUSE_EXCHANGE_LEDGER_FIELD_GAP = "exchange_ledger_field_gap"
_ROOT_CAUSE_TRUE_ABNORMAL_CLOSE_PATH = "true_abnormal_close_path"
_ROOT_CAUSE_REPORT_SEMANTICS_BUG = "report_accounting_semantics_bug"


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
    for index, ledger_row in enumerate(ledger_rows):
        matched = _match_ledger_row(ledger_row, positions)
        matched_id = str(matched.get("owner_id") or "")
        if not matched_id:
            unattributed_rows.append(
                _decorate_unmatched_ledger_row(
                    ledger_row,
                    positions,
                    row_index=index,
                    evidence_refs=list(matched.get("evidence_refs") or []),
                )
            )
            continue
        _add_ledger_row(
            positions[matched_id],
            {
                **ledger_row,
                "owner_id": matched_id,
                "match_confidence": matched.get("match_confidence") or "unknown",
                "unattributed_reason": "",
                "evidence_refs": list(matched.get("evidence_refs") or []),
            },
        )

    summary_counts: dict[str, int] = {}
    for position in positions.values():
        _finalize_lifecycle_completeness(position)
        classification = _classify_position(position, quick_flat_threshold_ms)
        position["classification"] = classification
        position["root_cause"] = _position_root_cause(position, classification)
        position["solution"] = _position_solution(position, classification)
        summary_counts[f"{classification}_count"] = (
            summary_counts.get(f"{classification}_count", 0) + 1
        )

    abnormal_positions = [
        _position_summary(position)
        for position in positions.values()
        if _is_abnormal_classification(str(position.get("classification") or ""))
    ]
    normal_gaps = [
        _normal_lifecycle_gap(position)
        for position in positions.values()
        if _normal_lifecycle_gap(position)
    ]

    summary_counts.setdefault("normal_count", 0)
    summary_counts.setdefault("seconds_open_close_count", 0)
    summary_counts.setdefault("abnormal_quick_terminal_count", 0)
    summary_counts.setdefault("abnormal_recovered_count", 0)
    summary_counts.setdefault("abnormal_count", 0)
    summary_counts.setdefault("admission_aborted_no_open_count", 0)
    summary_counts["abnormal_position_count"] = len(abnormal_positions)
    summary_counts["normal_lifecycle_ledger_gap_count"] = len(normal_gaps)
    summary_counts["unattributed_ledger_row_count"] = len(unattributed_rows)
    summary_counts["unattributed_ledger_reason_counts"] = _reason_counts(
        row["unattributed_reason"] for row in unattributed_rows
    )

    return {
        "positions": positions,
        "summary": dict(sorted(summary_counts.items())),
        "unattributed_ledger_rows": unattributed_rows,
        "abnormal_positions": abnormal_positions,
        "normal_lifecycle_ledger_gaps": normal_gaps,
    }


def derive_ledger_rows_from_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in sorted(events, key=lambda rec: int(rec.get("ts_ms") or 0)):
        if str(event.get("kind") or "") != "exit.reconciled":
            continue
        payload = event.get("payload", {})
        if not isinstance(payload, dict):
            continue
        base = {
            "ts_ms": int(event.get("ts_ms") or payload.get("reconciled_at_ms") or 0),
            "position_id": str(payload.get("position_id") or ""),
            "symbol": str(payload.get("symbol") or "").upper(),
            "source": str(payload.get("source") or ""),
            "evidence_refs": [
                f"event:exit.reconciled:{payload.get('position_id') or ''}",
            ],
        }
        for income_type, amount in (
            ("REALIZED_PNL", payload.get("price_pnl")),
            ("FUNDING_FEE", payload.get("funding_pnl_quote")),
        ):
            if _decimal(amount) != Decimal("0"):
                rows.append({
                    **base,
                    "income_type": income_type,
                    "amount": str(_decimal(amount)),
                    "fee": "0",
                })
        entry_fee = _decimal(payload.get("entry_fee_quote"))
        exit_fee = _decimal(payload.get("exit_fee_quote"))
        if (
            entry_fee
            or exit_fee
            or "entry_fee_quote" in payload
            or "exit_fee_quote" in payload
        ):
            rows.append({
                **base,
                "income_type": "COMMISSION",
                "amount": str(-(entry_fee + exit_fee)),
                "fee": "0",
            })

        trade_probe = payload.get("trade_probe_status")
        trade_probe_status = trade_probe if isinstance(trade_probe, dict) else {}
        for leg in ("long", "short"):
            if str(trade_probe_status.get(leg) or "") != "missing":
                continue
            rows.append({
                **base,
                "income_type": "MISSING_TRADE_STATEMENT",
                "leg": leg,
                "venue": _venue_from_reconciled_leg(payload, leg),
                "order_id": str(payload.get(f"{leg}_order_id") or ""),
                "client_order_id": str(payload.get(f"{leg}_client_order_id") or ""),
                "quantity": payload.get(f"{leg}_closed_qty"),
                "amount": "0",
                "fee": "0",
                "force_unattributed": True,
                "candidate_owner_id": str(payload.get("position_id") or ""),
                "unattributed_reason": "exchange_statement_leg_missing",
                "root_cause": _ROOT_CAUSE_EXCHANGE_LEDGER_FIELD_GAP,
                "solution": (
                    "Backfill this close leg from venue trade history or statement API "
                    "using order/client ids before treating the PnL row as complete."
                ),
            })
    return rows


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
        "trade_ids": [],
        "exchange_truth_flat": None,
        "exchange_truth_no_open_orders": None,
        "duration_ms": None,
        "financials": {
            "price_pnl": "0",
            "funding": "0",
            "entry_fee": "0",
            "exit_fee": "0",
            "net": "0",
        },
        "lifecycle_completeness": {},
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
    if kind == "runtime.funding_capture_state_updated":
        position["has_funding_capture"] = True
    if kind == "exit.reconciled":
        position["has_exit_reconciled"] = True
        financials = position["financials"]
        financials["price_pnl"] = str(_decimal(payload.get("price_pnl")))
        financials["funding"] = str(_decimal(payload.get("funding_pnl_quote")))
        financials["entry_fee"] = str(_decimal(payload.get("entry_fee_quote")))
        financials["exit_fee"] = str(_decimal(payload.get("exit_fee_quote")))
        financials["net"] = str(_decimal(payload.get("net_quote")))
    if kind == "order.filled":
        position["has_order_filled"] = True


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
        "trade_ids": ("trade_id", "trade_ids", "exec_id", "exec_ids"),
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


def _match_ledger_row(
    row: dict[str, Any],
    positions: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if row.get("force_unattributed") is True:
        return {
            "owner_id": "",
            "match_confidence": "none",
            "evidence_refs": list(row.get("evidence_refs") or []),
        }
    explicit_id = str(
        row.get("position_id")
        or row.get("entry_id")
        or row.get("owner_id")
        or ""
    )
    if explicit_id in positions:
        return {
            "owner_id": explicit_id,
            "match_confidence": "explicit_owner_id",
            "evidence_refs": [f"position_id:{explicit_id}"],
        }

    for row_key, position_key, confidence in (
        ("order_id", "order_ids", "order_id"),
        ("client_order_id", "client_order_ids", "client_order_id"),
        ("trade_id", "trade_ids", "trade_id"),
    ):
        row_value = str(row.get(row_key) or "")
        if not row_value:
            continue
        matches = [
            position_id
            for position_id, position in positions.items()
            if row_value in {str(item) for item in position.get(position_key, [])}
        ]
        if len(matches) == 1:
            return {
                "owner_id": matches[0],
                "match_confidence": confidence,
                "evidence_refs": [f"{row_key}:{row_value}"],
            }

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
        has_strong_window = bool(position.get("open_ts_ms") and position.get("terminal_ts_ms"))
        has_aborted_window = bool(position.get("has_entry_aborted") and position.get("terminal_ts_ms"))
        has_recovered_window = str(position_id).startswith("live-recovered:") and bool(
            position.get("terminal_ts_ms")
        )
        if start and end and ts_ms and venue and (
            has_strong_window or has_aborted_window or has_recovered_window
        ):
            low = min(start, end) - 10 * 60_000
            high = max(start, end) + 10 * 60_000
            if not (low <= ts_ms <= high):
                continue
            candidates.append((abs(ts_ms - end), position_id))
    if len(candidates) == 1:
        return {
            "owner_id": candidates[0][1],
            "match_confidence": "symbol_venue_time",
            "evidence_refs": [
                f"symbol:{symbol}",
                f"venue:{venue}",
                f"ts_ms:{ts_ms}",
            ],
        }
    if candidates:
        candidates.sort()
        if len(candidates) == 1 or candidates[0][0] < candidates[1][0]:
            return {
                "owner_id": candidates[0][1],
                "match_confidence": "symbol_venue_time",
                "evidence_refs": [
                    f"symbol:{symbol}",
                    f"venue:{venue}",
                    f"ts_ms:{ts_ms}",
                ],
            }
    return {
        "owner_id": "",
        "match_confidence": "none",
        "evidence_refs": [],
    }


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
    existing_reason = str(row.get("unattributed_reason") or "")
    if existing_reason:
        return existing_reason
    if not any(
        row.get(key)
        for key in (
            "position_id",
            "entry_id",
            "owner_id",
            "order_id",
            "client_order_id",
            "trade_id",
        )
    ):
        return "audit_missing_durable_anchor"
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


def _decorate_unmatched_ledger_row(
    row: dict[str, Any],
    positions: dict[str, dict[str, Any]],
    *,
    row_index: int,
    evidence_refs: list[str],
) -> dict[str, Any]:
    reason = _unattributed_reason(row, positions)
    return {
        **row,
        "owner_id": "",
        "candidate_owner_id": str(row.get("candidate_owner_id") or ""),
        "match_confidence": "none",
        "unattributed_reason": reason,
        "root_cause": str(row.get("root_cause") or _unattributed_root_cause(reason)),
        "solution": str(row.get("solution") or _unattributed_solution(reason)),
        "evidence_refs": evidence_refs
        or list(row.get("evidence_refs") or [])
        or [f"ledger_row:{row_index}"],
    }


def _unattributed_root_cause(reason: str) -> str:
    if reason in {"audit_missing_durable_anchor", "audit_unmatched_time_or_venue"}:
        return _ROOT_CAUSE_AUDIT_MATCHER_GAP
    if reason in {"exchange_statement_leg_missing", "audit_missing_symbol"}:
        return _ROOT_CAUSE_EXCHANGE_LEDGER_FIELD_GAP
    if reason in {"business_ledger_without_lifecycle", "business_aborted_entry_with_ledger_gap"}:
        return _ROOT_CAUSE_BUSINESS_EVENT_MISSING_ANCHOR
    return _ROOT_CAUSE_AUDIT_MATCHER_GAP


def _unattributed_solution(reason: str) -> str:
    if reason == "audit_missing_durable_anchor":
        return (
            "Emit or fetch order_id/client_order_id/trade_id for this ledger row; "
            "do not attribute it by symbol alone."
        )
    if reason == "exchange_statement_leg_missing":
        return (
            "Backfill the missing venue statement/trade leg and keep the row as an "
            "audit gap until durable order or trade anchors are available."
        )
    if reason == "business_ledger_without_lifecycle":
        return (
            "Create a recovered lifecycle owner from exchange truth before the "
            "ledger row is counted in position PnL."
        )
    if reason == "business_aborted_entry_with_ledger_gap":
        return (
            "Run the entry abort truth gate before terminal abort; if fill/fee/position "
            "truth exists, route it into pending/recovery ownership."
        )
    return "Investigate venue, timestamp, side, and durable id mismatch before attribution."


def _finalize_lifecycle_completeness(position: dict[str, Any]) -> None:
    events = position.get("events") or {}
    position["lifecycle_completeness"] = {
        "entry_opened": bool(position.get("has_entry_opened")),
        "terminal": bool(events.get("runtime.position_lifecycle_terminal")),
        "recovery_flat": bool(events.get("recovery.flat")),
        "exit_reconciled": bool(events.get("exit.reconciled")),
        "ledger_rows": bool(int(position.get("ledger", {}).get("row_count") or 0)),
        "funding_capture": bool(events.get("runtime.funding_capture_state_updated")),
        "order_filled": bool(events.get("order.filled")),
    }


def _is_abnormal_classification(classification: str) -> bool:
    return classification in {
        "abnormal",
        "abnormal_quick_terminal",
        "abnormal_recovered",
        "aborted_with_ledger",
    }


def _position_root_cause(position: dict[str, Any], classification: str) -> str:
    if classification in {"abnormal", "abnormal_quick_terminal", "abnormal_recovered"}:
        return _ROOT_CAUSE_TRUE_ABNORMAL_CLOSE_PATH
    if classification == "aborted_with_ledger":
        return _ROOT_CAUSE_BUSINESS_EVENT_MISSING_ANCHOR
    if _normal_lifecycle_missing(position):
        return _ROOT_CAUSE_BUSINESS_EVENT_MISSING_ANCHOR
    return ""


def _position_solution(position: dict[str, Any], classification: str) -> str:
    if classification in {"abnormal", "abnormal_quick_terminal", "abnormal_recovered"}:
        return (
            "Before terminal fallback or compensation, refresh order truth, trade fills, "
            "and live positions; persist durable close anchors for later ledger attribution."
        )
    if classification == "aborted_with_ledger":
        return (
            "Run the entry abort truth gate before terminal abort; if fill/fee/position "
            "truth exists, route it into pending/recovery ownership."
        )
    if _normal_lifecycle_missing(position):
        return (
            "Backfill the missing lifecycle/reconciliation anchors and keep the position "
            "out of clean-normal accounting until the evidence chain is complete."
        )
    return ""


def _position_summary(position: dict[str, Any]) -> dict[str, Any]:
    return {
        "position_id": position.get("position_id") or "",
        "symbol": position.get("symbol") or "",
        "venues": list(position.get("venues") or []),
        "classification": position.get("classification") or "",
        "open_ts_ms": position.get("open_ts_ms") or 0,
        "terminal_ts_ms": position.get("terminal_ts_ms") or 0,
        "duration_ms": position.get("duration_ms"),
        "terminal_reason": position.get("terminal_reason") or "",
        "close_source": position.get("close_source") or "",
        "abnormal_evidence": _abnormal_evidence_markers(position),
        "reasons": list(position.get("reasons") or []),
        "order_ids": list(position.get("order_ids") or []),
        "client_order_ids": list(position.get("client_order_ids") or []),
        "financials": dict(position.get("financials") or {}),
        "ledger": {
            "row_count": int(position.get("ledger", {}).get("row_count") or 0),
            "net_amount": position.get("ledger", {}).get("net_amount") or "0",
            "fee_amount": position.get("ledger", {}).get("fee_amount") or "0",
        },
        "lifecycle_completeness": dict(position.get("lifecycle_completeness") or {}),
        "root_cause": position.get("root_cause") or "",
        "solution": position.get("solution") or "",
    }


def _abnormal_evidence_markers(position: dict[str, Any]) -> list[str]:
    reasons = [str(item) for item in position.get("reasons") or []]
    evidence: list[str] = []
    for reason in reasons:
        if any(marker in reason for marker in _ABNORMAL_REASON_MARKERS):
            evidence.append(reason)
    if str(position.get("position_id") or "").startswith("live-recovered:"):
        evidence.append("live-recovered")
    return sorted(set(evidence))


def _normal_lifecycle_gap(position: dict[str, Any]) -> dict[str, Any]:
    if not position.get("has_entry_opened"):
        return {}
    classification = str(position.get("classification") or "")
    if _is_abnormal_classification(classification):
        return {}
    missing = _normal_lifecycle_missing(position)
    if not missing:
        return {}
    summary = _position_summary(position)
    summary["missing"] = missing
    summary["root_cause"] = _ROOT_CAUSE_BUSINESS_EVENT_MISSING_ANCHOR
    summary["solution"] = (
        "Complete the missing event/ledger anchors or classify the position as an "
        "audit gap instead of normal clean."
    )
    return summary


def _normal_lifecycle_missing(position: dict[str, Any]) -> list[str]:
    completeness = position.get("lifecycle_completeness") or {}
    required = (
        ("entry.opened", "entry_opened"),
        ("runtime.position_lifecycle_terminal", "terminal"),
        ("recovery.flat", "recovery_flat"),
        ("exit.reconciled", "exit_reconciled"),
        ("ledger_rows", "ledger_rows"),
        ("order.filled", "order_filled"),
    )
    return [event_name for event_name, key in required if not completeness.get(key)]


def _venue_from_reconciled_leg(payload: dict[str, Any], leg: str) -> str:
    legs = payload.get(f"{leg}_legs")
    if isinstance(legs, list):
        for item in legs:
            if isinstance(item, dict) and item.get("venue"):
                return str(item.get("venue") or "").lower()
    return ""


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
