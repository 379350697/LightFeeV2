from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from lightfee.engine.recovery_decision_core import (
    RecoveryEvidenceSnapshot,
    V1RecoveryDecisionCore,
    pending_close_owner_counts,
    pending_passive_close_evidence,
)
from lightfee.engine.recovery_ledger import RecoveryLedger
from lightfee.engine.recovery_owner_index import RecoveryOwnerIndex
from lightfee.engine.v1_lifecycle_closure import build_v1_lifecycle_closure_table
from lightfee.ops.position_side_semantics import side_matches_business_leg


EXPECTED_VENUES = {"aster", "binance", "bitget", "bybit", "gate", "hyperliquid", "okx"}
KNOWN_GOOD_RESOLVERS = {"1.1.1.1", "1.0.0.1", "8.8.8.8", "8.8.4.4", "9.9.9.9"}
FIXTURE_MARKET_OBSERVED_AT_MS = 1710000075000


@dataclass(frozen=True)
class HealthReport:
    name: str
    ok: bool
    severity: str = "info"
    fingerprints: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class HealthSummary:
    ok: bool
    critical_count: int
    warning_count: int
    reports: list[HealthReport]


def _execstart_lines(unit_text: str) -> list[str]:
    return [
        line.split("=", 1)[1].strip()
        for line in unit_text.splitlines()
        if line.strip().startswith("ExecStart=")
    ]


def _has_environment_file(unit_text: str) -> bool:
    return any(
        line.strip().startswith("EnvironmentFile=")
        for line in unit_text.splitlines()
    )


def analyze_systemd_unit(name: str, text: str) -> HealthReport:
    fingerprints: list[str] = []
    details: dict[str, Any] = {"service": name}
    execs = _execstart_lines(text)
    details["execstart"] = execs
    is_sidecar = "sidecar" in name
    is_live = name.endswith("lightfee-live.service") or "live" in name

    if not execs:
        fingerprints.append("missing_execstart")
    command = " ".join(execs)
    if (is_sidecar or is_live) and "--config" not in command:
        fingerprints.append("missing_explicit_config")
    if "config/example.toml" in command or command.endswith("example.toml"):
        fingerprints.append("example_config_in_production")
    if is_sidecar and not _has_environment_file(text):
        fingerprints.append("missing_environment_file")
    if is_sidecar and "opportunity_input_sidecar" in command and "live.auto.toml" not in command:
        fingerprints.append("rust_sidecar_without_live_auto_config")

    return HealthReport(
        name=f"systemd:{name}",
        ok=not fingerprints,
        severity="critical" if fingerprints else "info",
        fingerprints=fingerprints,
        details=details,
    )


def _quote_venues(snapshot: dict[str, Any]) -> set[str]:
    venues: set[str] = set()
    quotes = snapshot.get("quotes", {})
    if isinstance(quotes, dict):
        for key, raw in quotes.items():
            if isinstance(raw, dict) and raw.get("venue"):
                venues.add(str(raw["venue"]).lower())
            elif isinstance(key, str) and ":" in key:
                venues.add(key.split(":", 1)[0].lower())
    return venues


def _fixture_quote_count(snapshot: dict[str, Any]) -> int:
    count = 0
    quotes = snapshot.get("quotes", {})
    if isinstance(quotes, dict):
        for raw in quotes.values():
            if not isinstance(raw, dict):
                continue
            try:
                if float(raw.get("bid", 0)) == 100.0 and float(raw.get("ask", 0)) == 100.0:
                    count += 1
            except (TypeError, ValueError):
                continue
    return count


def analyze_sidecar_snapshot(
    snapshot: dict[str, Any],
    *,
    now_ms: int,
    max_age_ms: int,
) -> HealthReport:
    fingerprints: list[str] = []
    observed = int(snapshot.get("market_observed_at_ms") or snapshot.get("published_at_ms") or 0)
    age_ms = now_ms - observed if observed else None
    venues = _quote_venues(snapshot)
    missing = sorted(EXPECTED_VENUES - venues)
    fixture_quotes = _fixture_quote_count(snapshot)

    if observed == FIXTURE_MARKET_OBSERVED_AT_MS:
        fingerprints.append("fixture_timestamp")
    if age_ms is None or age_ms < 0 or age_ms > max_age_ms:
        fingerprints.append("snapshot_stale_or_missing_timestamp")
    if len(venues) < len(EXPECTED_VENUES):
        fingerprints.append("quote_venue_count_lt_7")
    if fixture_quotes >= 2:
        fingerprints.append("fixture_100_quotes")

    return HealthReport(
        name="sidecar_snapshot",
        ok=not fingerprints,
        severity="critical" if fingerprints else "info",
        fingerprints=fingerprints,
        details={
            "observed_at_ms": observed,
            "age_ms": age_ms,
            "quote_venues": sorted(venues),
            "missing_venues": missing,
            "fixture_quote_count": fixture_quotes,
            "degraded_venues": list(snapshot.get("degraded_venues", [])),
        },
    )


def _safe_abs_quantity(value: Any) -> float:
    try:
        return abs(float(value or 0.0))
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _live_position_details(exchange_truth: dict[str, Any]) -> list[dict[str, Any]]:
    live: list[dict[str, Any]] = []
    for venue, positions in (exchange_truth.get("positions") or {}).items():
        if not isinstance(positions, dict):
            continue
        for symbol, pos in positions.items():
            if not isinstance(pos, dict):
                continue
            qty = _safe_abs_quantity(pos.get("quantity"))
            if qty <= 1e-9:
                continue
            live.append({
                "venue": str(pos.get("venue") or venue).lower(),
                "symbol": str(pos.get("symbol") or symbol).upper(),
                "side": str(pos.get("side") or "").lower(),
                "quantity": qty,
                "entry_price": pos.get("entry_price"),
            })
    return live


def _live_open_order_details(exchange_truth: dict[str, Any]) -> list[dict[str, Any]]:
    live: list[dict[str, Any]] = []
    for venue, orders_by_symbol in (exchange_truth.get("open_orders") or {}).items():
        if not isinstance(orders_by_symbol, dict):
            continue
        for symbol, rows in orders_by_symbol.items():
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                qty = _safe_abs_quantity(row.get("quantity") or row.get("qty"))
                if qty <= 1e-9:
                    continue
                live.append({
                    "venue": str(row.get("venue") or venue).lower(),
                    "symbol": str(row.get("symbol") or symbol).upper(),
                    "side": str(row.get("side") or "").lower(),
                    "quantity": qty,
                    "price": row.get("price"),
                    "reduce_only": bool(row.get("reduce_only", False)),
                    "order_id": row.get("order_id"),
                })
    return live


def _local_expected_legs(state: dict[str, Any]) -> list[dict[str, Any]]:
    legs: list[dict[str, Any]] = []
    for pos in state.get("open_positions", []) or state.get("positions", []) or []:
        if not isinstance(pos, dict):
            continue
        symbol = str(pos.get("symbol") or "").upper()
        qty = _safe_abs_quantity(pos.get("quantity") or pos.get("matched_quantity"))
        if not symbol or qty <= 1e-9:
            continue
        long_venue = str(pos.get("long_venue") or "").lower()
        short_venue = str(pos.get("short_venue") or "").lower()
        if long_venue:
            legs.append({
                "venue": long_venue,
                "symbol": symbol,
                "expected_side": "long",
                "expected_quantity": qty,
                "position_id": pos.get("position_id"),
            })
        if short_venue:
            legs.append({
                "venue": short_venue,
                "symbol": symbol,
                "expected_side": "short",
                "expected_quantity": qty,
                "position_id": pos.get("position_id"),
            })
    return legs


def _side_matches(actual: str, expected: str) -> bool:
    return side_matches_business_leg(actual, expected)


def _exchange_truth_position_mismatches(
    state: dict[str, Any], exchange_truth: dict[str, Any]
) -> list[dict[str, Any]]:
    live_positions = _live_position_details(exchange_truth)
    live_by_key = {
        (p["venue"], p["symbol"]): p
        for p in live_positions
    }
    expected_legs = _local_expected_legs(state)
    expected_keys = {(leg["venue"], leg["symbol"]) for leg in expected_legs}
    mismatches: list[dict[str, Any]] = []

    for leg in expected_legs:
        key = (leg["venue"], leg["symbol"])
        live = live_by_key.get(key)
        live_qty = float(live.get("quantity", 0.0)) if live else 0.0
        live_side = str(live.get("side", "")) if live else ""
        expected_qty = float(leg["expected_quantity"])
        if (
            abs(live_qty - expected_qty) > 1e-9
            or not _side_matches(live_side, str(leg["expected_side"]))
        ):
            mismatches.append({
                "check": "local_live_leg_missing_or_quantity_mismatch",
                **leg,
                "live_quantity": live_qty,
                "live_side": live_side,
            })

    if expected_legs:
        for live in live_positions:
            key = (live["venue"], live["symbol"])
            if key not in expected_keys:
                mismatches.append({
                    "check": "unexpected_live_position",
                    **live,
                })

    return mismatches


def _pending_entry_live_conflict_summary(
    state: dict[str, Any],
    exchange_truth: dict[str, Any],
) -> dict[str, Any]:
    live_by_key = {
        (p["venue"], p["symbol"]): p
        for p in _live_position_details(exchange_truth)
    }
    open_orders = {
        (o["venue"], o["symbol"]): []
        for o in _live_open_order_details(exchange_truth)
    }
    for order in _live_open_order_details(exchange_truth):
        open_orders.setdefault((order["venue"], order["symbol"]), []).append(order)

    details: list[dict[str, Any]] = []
    for pending in state.get("pending_entries", []) or []:
        if not isinstance(pending, dict):
            continue
        symbol = str(pending.get("symbol") or "").upper()
        maker_fill = _safe_abs_quantity(pending.get("maker_leg_filled"))
        hedge_fill = _safe_abs_quantity(pending.get("hedge_leg_filled"))
        if not symbol or (maker_fill <= 1e-9 and hedge_fill <= 1e-9):
            continue
        maker_leg = _maker_leg_text(pending.get("maker_leg") or pending.get("maker_side"))
        if maker_leg not in {"long", "short"}:
            continue
        legs = []
        if maker_leg == "long":
            long_qty, short_qty = maker_fill, hedge_fill
        else:
            long_qty, short_qty = hedge_fill, maker_fill
        for venue, expected_side, expected_qty in (
            (str(pending.get("long_venue") or "").lower(), "long", long_qty),
            (str(pending.get("short_venue") or "").lower(), "short", short_qty),
        ):
            if not venue or expected_qty <= 1e-9:
                continue
            live = live_by_key.get((venue, symbol), {})
            live_qty = _safe_abs_quantity(live.get("quantity"))
            live_side = str(live.get("side") or "").lower()
            live_matches = (
                live_qty > 1e-9
                and abs(live_qty - expected_qty) <= 1e-9
                and _side_matches(live_side, expected_side)
            )
            legs.append({
                "venue": venue,
                "symbol": symbol,
                "expected_side": expected_side,
                "expected_quantity": expected_qty,
                "live_quantity": live_qty,
                "live_side": live_side,
                "live_position_confirmed": live_matches,
                "open_orders": open_orders.get((venue, symbol), []),
                "owner": "pending_entry",
            })
        if legs:
            conflict_reasons: list[str] = []
            for leg in legs:
                venue = str(leg.get("venue") or "")
                if _safe_abs_quantity(leg.get("live_quantity")) <= 1e-9:
                    conflict_reasons.append(
                        f"{venue} fill evidence conflicts with {venue} live flat"
                    )
                elif not bool(leg.get("live_position_confirmed")):
                    conflict_reasons.append(
                        f"{venue} fill evidence conflicts with {venue} live mismatch"
                    )
            if any(_safe_abs_quantity(leg.get("live_quantity")) > 1e-9 for leg in legs):
                conflict_reasons.append("live position owned by pending conflict")
            details.append({
                "pending_id": str(
                    pending.get("pending_id")
                    or pending.get("position_id")
                    or symbol
                ),
                "symbol": symbol,
                "maker_leg": str(pending.get("maker_leg") or ""),
                "maker_leg_filled": maker_fill,
                "hedge_leg_filled": hedge_fill,
                "legs": legs,
                "conflict_reasons": sorted(set(conflict_reasons)),
                "next_action": "owned_pending_entry_live_conflict_cleanup",
            })
    return {"count": len(details), "details": details}


def _maker_leg_text(value: Any) -> str:
    if hasattr(value, "value"):
        value = value.value
    text = str(value or "").lower()
    if text == "buy":
        return "long"
    if text == "sell":
        return "short"
    return text


def _runtime_progress_summary(
    state: dict[str, Any],
    *,
    now_ms: int,
    progress_budget_ms: int,
    last_scan_age_ms: int | None,
    current_state_age_ms: int | None,
) -> tuple[str, bool, bool, dict[str, Any]]:
    raw_progress = state.get("runtime_progress")
    runtime_progress = dict(raw_progress) if isinstance(raw_progress, dict) else {}
    last_lane_progress_ms = _safe_int(runtime_progress.get("last_lane_progress_ms"))
    active_lane = str(runtime_progress.get("active_lane") or "")
    active_lane_started_ms = _safe_int(runtime_progress.get("active_lane_started_ms"))
    active_lane_budget_ms = _safe_int(runtime_progress.get("active_lane_budget_ms"))
    loop_iteration_started_ms = _safe_int(runtime_progress.get("loop_iteration_started_ms"))
    loop_iteration_completed_ms = _safe_int(runtime_progress.get("loop_iteration_completed_ms"))

    last_lane_progress_age_ms = (
        now_ms - last_lane_progress_ms if last_lane_progress_ms > 0 else None
    )
    active_lane_age_ms = (
        now_ms - active_lane_started_ms
        if active_lane and active_lane_started_ms > 0
        else None
    )
    active_lane_overdue = bool(runtime_progress.get("active_lane_overdue"))
    if (
        active_lane_age_ms is not None
        and active_lane_age_ms >= 0
        and active_lane_budget_ms > 0
        and active_lane_age_ms > active_lane_budget_ms
    ):
        active_lane_overdue = True

    recent_last_scan = (
        last_scan_age_ms is not None
        and last_scan_age_ms >= 0
        and last_scan_age_ms <= progress_budget_ms
    )
    recent_lane_progress = (
        last_lane_progress_age_ms is not None
        and last_lane_progress_age_ms >= 0
        and last_lane_progress_age_ms <= progress_budget_ms
    )
    active_bounded_lane = (
        bool(active_lane)
        and active_lane_age_ms is not None
        and active_lane_age_ms >= 0
        and active_lane_budget_ms > 0
        and active_lane_age_ms <= active_lane_budget_ms
        and not active_lane_overdue
    )
    exporter_fresh = (
        current_state_age_ms is not None
        and current_state_age_ms >= 0
        and current_state_age_ms <= progress_budget_ms
    )

    if recent_last_scan:
        progress_source = "last_scan"
    elif recent_lane_progress:
        progress_source = "runtime_lane"
    elif active_bounded_lane:
        progress_source = "active_bounded_lane"
    elif exporter_fresh:
        progress_source = "exporter_only"
    else:
        progress_source = "none"

    runtime_progress_details = {
        "loop_iteration_started_ms": loop_iteration_started_ms,
        "loop_iteration_completed_ms": loop_iteration_completed_ms,
        "last_lane_progress_ms": last_lane_progress_ms,
        "last_lane_progress_age_ms": last_lane_progress_age_ms,
        "active_lane": active_lane,
        "active_lane_started_ms": active_lane_started_ms,
        "active_lane_age_ms": active_lane_age_ms,
        "active_lane_budget_ms": active_lane_budget_ms,
        "active_lane_overdue": active_lane_overdue,
    }
    runtime_progress_valid = recent_last_scan or recent_lane_progress or active_bounded_lane
    exporter_only_progress = progress_source == "exporter_only"
    return (
        progress_source,
        exporter_only_progress,
        runtime_progress_valid,
        runtime_progress_details,
    )


def _weak_order_truth_events(state: dict[str, Any]) -> list[dict[str, Any]]:
    weak: list[dict[str, Any]] = []
    for rec in _state_journal_events(state):
        kind = str(rec.get("kind") or "")
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue
        fill_status = str(payload.get("order_truth_fill_status") or "").lower()
        evidence_status = str(payload.get("order_truth_evidence_status") or "").lower()
        if not fill_status and not evidence_status:
            continue
        if (
            fill_status == "confirmed_fill"
            and evidence_status == "available"
            and payload.get("terminal_without_truth") is not True
        ):
            continue
        weak.append({
            "kind": kind,
            "position_id": payload.get("position_id"),
            "symbol": payload.get("symbol"),
            "venue": payload.get("hedge_venue") or payload.get("venue"),
            "order_id": payload.get("order_id") or payload.get("accepted_order_id"),
            "client_order_id": (
                payload.get("client_order_id")
                or payload.get("accepted_client_order_id")
            ),
            "order_truth_fill_status": fill_status,
            "order_truth_evidence_status": evidence_status,
            "order_truth_decision": payload.get("order_truth_decision"),
            "order_truth_missing_evidence": (
                payload.get("order_truth_missing_evidence") or []
            ),
        })
    return weak


def analyze_current_state(
    state: dict[str, Any],
    *,
    now_ms: int,
    max_tick_age_ms: int,
    require_exchange_truth: bool = False,
) -> HealthReport:
    fingerprints: list[str] = []
    last_tick_ms = int(state.get("last_tick_ms") or 0)
    tick_age_ms = now_ms - last_tick_ms if last_tick_ms else None
    generated_at_ms = 0
    try:
        generated_at_ms = int(state.get("generated_at_ms", 0) or 0)
    except (TypeError, ValueError):
        generated_at_ms = 0
    current_state_age_ms = now_ms - generated_at_ms if generated_at_ms > 0 else None
    open_count = int(state.get("open_position_count") or 0)
    pending_entries = int(state.get("pending_entry_count") or 0)
    pending_close_owners = pending_close_owner_counts(state)
    pending_closes = pending_close_owners.pending_close_count
    pending_passive_closes = pending_close_owners.pending_passive_close_count
    pending_close_reconciliations = (
        pending_close_owners.pending_close_reconciliation_count
    )
    pending_close_owner_count = pending_close_owners.pending_close_owner_count
    pending_residual_repairs = int(state.get("pending_residual_repair_count") or 0)
    exchange_truth = state.get("exchange_truth")
    exchange_truth_available = (
        isinstance(exchange_truth, dict)
        and bool(exchange_truth.get("available"))
    )
    exchange_truth_confidence = (
        str(exchange_truth.get("confidence", ""))
        if isinstance(exchange_truth, dict)
        else ""
    )
    exchange_truth_high_confidence_flat = (
        exchange_truth_available
        and exchange_truth_confidence == "high"
        and not bool(exchange_truth.get("has_nonzero_position"))
        and not bool(exchange_truth.get("has_open_order"))
        if isinstance(exchange_truth, dict)
        else False
    )
    recovery_decision = _recovery_decision_payload(state, exchange_truth)
    # V1 distinguishes durable accounting reconciliation from live execution
    # recovery. The former remains operator-visible, but a trusted-flat account
    # has no order-management work that makes a restart unsafe.
    background_close_reconciliation_pending = (
        open_count == 0
        and pending_entries == 0
        and pending_closes == 0
        and pending_passive_closes == 0
        and pending_close_reconciliations > 0
        and pending_residual_repairs == 0
        and exchange_truth_high_confidence_flat
        and recovery_decision.get("kind") == "RUNNING_WITH_EVIDENCE_GAP"
        and recovery_decision.get("evidence_quality")
        == "background_close_reconciliation"
        and recovery_decision.get("entry_allowed") is True
    )
    clean = (
        open_count == 0
        and pending_entries == 0
        and pending_closes == 0
        and pending_passive_closes == 0
        and pending_residual_repairs == 0
    )
    last_scan = state.get("last_scan")
    last_scan_ts_ms = 0
    if isinstance(last_scan, dict):
        try:
            last_scan_ts_ms = int(last_scan.get("ts_ms", 0) or 0)
        except (TypeError, ValueError):
            last_scan_ts_ms = 0
    last_scan_age_ms = now_ms - last_scan_ts_ms if last_scan_ts_ms > 0 else None

    if state.get("lifecycle") != "running":
        fingerprints.append("live_lifecycle_not_running")
    if state.get("risk_mode") == "fail_closed" and clean and not state.get("recovery_blocked_reason"):
        fingerprints.append("stale_fail_closed_clean_state")
    if pending_close_owner_count and not background_close_reconciliation_pending:
        fingerprints.append("pending_close_owner_present")
    if state.get("last_scan") is None:
        fingerprints.append("last_scan_missing")
    exchange_truth_mismatches: list[dict[str, Any]] = []
    v1_lifecycle_closure = _v1_lifecycle_closure_payload(state, exchange_truth, now_ms)
    pending_entry_live_conflicts = (
        _pending_entry_live_conflict_summary(state, exchange_truth)
        if isinstance(exchange_truth, dict)
        else {"count": 0, "details": []}
    )
    exchange_truth_available_flat = (
        exchange_truth_available
        and not bool(exchange_truth.get("has_nonzero_position"))
        and not bool(exchange_truth.get("has_open_order"))
        if isinstance(exchange_truth, dict)
        else False
    )
    progress_budget_ms = max(int(max_tick_age_ms or 0) * 4, 60_000)
    (
        progress_source,
        exporter_only_progress,
        recent_runtime_progress,
        runtime_progress,
    ) = _runtime_progress_summary(
        state,
        now_ms=now_ms,
        progress_budget_ms=progress_budget_ms,
        last_scan_age_ms=last_scan_age_ms,
        current_state_age_ms=current_state_age_ms,
    )
    tick_stale = tick_age_ms is None or tick_age_ms < 0 or tick_age_ms > max_tick_age_ms
    tick_stale_suppressed_by_runtime_progress = (
        tick_stale
        and clean
        and exchange_truth_available_flat
        and recent_runtime_progress
    )
    if tick_stale and not tick_stale_suppressed_by_runtime_progress:
        fingerprints.append("live_tick_stale")
        if exporter_only_progress:
            fingerprints.append("exporter_only_progress")
    if require_exchange_truth:
        if not isinstance(exchange_truth, dict):
            fingerprints.append("exchange_truth_missing")
        elif not exchange_truth_available:
            fingerprints.append("exchange_truth_unavailable")
        elif exchange_truth_confidence != "high":
            fingerprints.append("exchange_truth_confidence_not_high")
    if isinstance(exchange_truth, dict) and exchange_truth.get("available") and exchange_truth.get("confidence") == "high":
        leg_mismatches = _exchange_truth_position_mismatches(state, exchange_truth)
        if leg_mismatches:
            fingerprints.append("exchange_truth_mismatch")
            fingerprints.append("local_exchange_position_mismatch")
            if any(m.get("check") == "unexpected_live_position" for m in leg_mismatches):
                fingerprints.append("nonzero_live_position")
            exchange_truth_mismatches.extend(leg_mismatches)
        if clean and exchange_truth.get("has_nonzero_position"):
            if "exchange_truth_mismatch" not in fingerprints:
                fingerprints.append("exchange_truth_mismatch")
            if "nonzero_live_position" not in fingerprints:
                fingerprints.append("nonzero_live_position")
            for venue, positions in (exchange_truth.get("positions") or {}).items():
                if not isinstance(positions, dict):
                    continue
                for symbol, pos in positions.items():
                    if not isinstance(pos, dict):
                        continue
                    try:
                        qty = abs(float(pos.get("quantity") or 0.0))
                    except (TypeError, ValueError):
                        qty = 0.0
                    if qty > 1e-9:
                        exchange_truth_mismatches.append({
                            "check": "unexpected_live_position",
                            "venue": str(pos.get("venue") or venue),
                            "symbol": str(pos.get("symbol") or symbol),
                            "side": pos.get("side"),
                            "quantity": qty,
                            "entry_price": pos.get("entry_price"),
                        })
        if clean and exchange_truth.get("has_open_order"):
            if "exchange_truth_mismatch" not in fingerprints:
                fingerprints.append("exchange_truth_mismatch")
            if "live_open_order" not in fingerprints:
                fingerprints.append("live_open_order")
            for order in _live_open_order_details(exchange_truth):
                exchange_truth_mismatches.append({
                    "check": "unexpected_live_open_order",
                    **order,
                })
    weak_order_truth_events = _weak_order_truth_events(state)
    if weak_order_truth_events:
        fingerprints.append("order_truth_gap_unresolved")

    warning_only_fingerprints = {
        "last_scan_missing",
        "pending_close_owner_present",
    }
    severity = (
        "critical"
        if any(fp not in warning_only_fingerprints for fp in fingerprints)
        else "warning"
    )
    return HealthReport(
        name="current_state",
        ok=not fingerprints,
        severity=severity if fingerprints else "info",
        fingerprints=fingerprints,
        details={
            "tick_age_ms": tick_age_ms,
            "risk_mode": state.get("risk_mode"),
            "lifecycle": state.get("lifecycle"),
            "open_position_count": open_count,
            "pending_entry_count": pending_entries,
            "pending_close_count": pending_closes,
            "pending_passive_close_count": pending_passive_closes,
            "pending_close_reconciliation_count": pending_close_reconciliations,
            "pending_close_owner_count": pending_close_owner_count,
            "background_close_reconciliation_pending": (
                background_close_reconciliation_pending
            ),
            "pending_residual_repair_count": pending_residual_repairs,
            "last_scan_age_ms": last_scan_age_ms,
            "current_state_age_ms": current_state_age_ms,
            "progress_source": progress_source,
            "exporter_only_progress": exporter_only_progress,
            "runtime_progress": runtime_progress,
            "tick_stale_suppressed_by_runtime_progress": tick_stale_suppressed_by_runtime_progress,
            "exchange_truth_required": require_exchange_truth,
            "exchange_truth_available": exchange_truth_available,
            "exchange_truth_confidence": exchange_truth_confidence,
            "recovery_decision": recovery_decision,
            "v1_lifecycle_closure": v1_lifecycle_closure,
            "exchange_truth_mismatches": exchange_truth_mismatches,
            "pending_entry_live_conflicts": pending_entry_live_conflicts,
            "weak_order_truth_events": weak_order_truth_events,
        },
    )


def _v1_lifecycle_closure_payload(
    state: dict[str, Any],
    exchange_truth: Any,
    now_ms: int,
) -> dict[str, Any]:
    existing = state.get("v1_lifecycle_closure")
    if isinstance(existing, dict) and existing.get("version"):
        return dict(existing)
    events = _state_journal_events(state)
    owner_index = RecoveryOwnerIndex.from_state_and_journal(state, events)
    return build_v1_lifecycle_closure_table(
        local_state=state,
        exchange_truth=exchange_truth if isinstance(exchange_truth, dict) else None,
        generated_at_ms=now_ms,
        events=events,
        owner_index=owner_index,
    ).to_dict()


def _state_journal_events(state: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("journal_events", "events", "recent_events"):
        events = state.get(key)
        if isinstance(events, list):
            return [event for event in events if isinstance(event, dict)]
    return []


def _recovery_decision_payload(
    state: dict[str, Any],
    exchange_truth: Any,
) -> dict[str, Any]:
    truth = exchange_truth if isinstance(exchange_truth, dict) else None
    events = _state_journal_events(state)
    owner_index = RecoveryOwnerIndex.from_state_and_journal(state, events)
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local=state,
        exchange_truth=truth,
        owner_index=owner_index,
    )
    decision = V1RecoveryDecisionCore().decide(
        RecoveryEvidenceSnapshot(
            local_open_positions=_state_collection_or_count(
                state, "open_positions", "open_position_count"
            ),
            pending_entries=_state_collection_or_count(
                state, "pending_entries", "pending_entry_count"
            ),
            residual_repairs=_state_collection_or_count(
                state, "pending_residual_repairs", "pending_residual_repair_count"
            ),
            passive_closes=pending_passive_close_evidence(state),
            exchange_truth=truth,
            prior_recovery_block_reason=state.get("recovery_blocked_reason"),
            recovery_work_items=tuple(ledger.work_items),
        )
    )
    return {
        "kind": decision.kind.value,
        "evidence_quality": decision.evidence_quality,
        "entry_allowed": decision.entry_allowed,
        "block_reason": decision.block_reason,
        "clear_reason": decision.clear_reason,
        "diagnostic_severity": decision.diagnostic_severity,
    }


def _state_collection_or_count(
    state: dict[str, Any],
    collection_key: str,
    count_key: str,
) -> tuple[Any, ...]:
    collection = state.get(collection_key)
    if isinstance(collection, dict):
        return tuple(collection.values())
    if isinstance(collection, (list, tuple, set)):
        return tuple(collection)
    count = int(state.get(count_key) or 0)
    return tuple({"source": count_key} for _ in range(max(count, 0)))


def analyze_resolver_config(text: str) -> HealthReport:
    nameservers: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("nameserver ") and len(line.split()) >= 2:
            nameservers.append(line.split()[1])
        elif line.startswith("servers="):
            nameservers.extend(
                item.strip()
                for item in line.split("=", 1)[1].split(",")
                if item.strip()
            )
    fingerprints: list[str] = []
    if not nameservers:
        fingerprints.append("resolver_missing_nameserver")
    elif nameservers[0] not in KNOWN_GOOD_RESOLVERS:
        fingerprints.append("unverified_resolver_first")
    return HealthReport(
        name="resolver_config",
        ok=not fingerprints,
        severity="critical" if fingerprints else "info",
        fingerprints=fingerprints,
        details={"nameservers": nameservers},
    )


def summarize_reports(reports: Iterable[HealthReport]) -> HealthSummary:
    items = list(reports)
    critical = sum(1 for r in items if not r.ok and r.severity == "critical")
    warning = sum(1 for r in items if not r.ok and r.severity == "warning")
    return HealthSummary(
        ok=critical == 0 and warning == 0,
        critical_count=critical,
        warning_count=warning,
        reports=items,
    )
