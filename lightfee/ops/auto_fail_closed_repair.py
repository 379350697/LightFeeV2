"""Safe repair helper for stale auto fail-closed operator latches."""

from __future__ import annotations

from typing import Any

from lightfee.engine.recovery import clear_stale_fail_closed_if_recovery_clean
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode

SAFE_TO_REPAIR = "safe_to_repair_auto_latch"
PRESERVE_OPERATOR_LATCH = "operator_latch_must_preserve"
UNSAFE_TRUTH_OR_CLEANUP = "unsafe_truth_or_cleanup_required"

_FLAT_TERMINAL_STATUSES = {"flat", "already_flat", "resolved_flat"}
_CLEARABLE_STALE_BLOCKERS = {
    "startup_recovery_pending_work_without_open_positions",
    "startup_recovery_pending_work_without_open_position",
    "startup_recovery_clean_stale_fail_closed",
    "unpaired_live_position_terminal_flat",
    "unpaired_live_position_recovered_flat",
}


def _enum_value(value: Any) -> str | None:
    if value is None:
        return None
    return str(value.value if hasattr(value, "value") else value)


def _mapping_or_attr(source: Any, name: str, default: Any = None) -> Any:
    if isinstance(source, dict):
        return source.get(name, default)
    return getattr(source, name, default)


def _operator_requested_mode(state: Any) -> str | None:
    operator = _mapping_or_attr(state, "operator", {})
    requested = _mapping_or_attr(operator, "requested_mode")
    return _enum_value(requested)


def _has_any(container: Any) -> bool:
    if container is None:
        return False
    if isinstance(container, (dict, list, tuple, set)):
        return bool(container)
    try:
        return bool(container)
    except Exception:
        return True


def _all_unpaired_recoveries_terminal_flat(recoveries: Any) -> bool:
    if not recoveries:
        return True
    if isinstance(recoveries, dict):
        items = recoveries.values()
    elif isinstance(recoveries, list):
        items = recoveries
    else:
        return False
    for item in items:
        if not isinstance(item, dict):
            return False
        status = str(item.get("terminal_status") or "").strip().lower()
        if status not in _FLAT_TERMINAL_STATUSES:
            return False
    return True


def _has_operator_fail_closed_evidence(journal_events: list[dict[str, Any]]) -> bool:
    for event in journal_events:
        if not isinstance(event, dict) or event.get("kind") != "ops.command_applied":
            continue
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        command = str(payload.get("command") or "").replace("-", "_").lower()
        if command == "fail_closed":
            return True
    return False


def _truth_has_open_order(exchange_truth: dict[str, Any]) -> bool:
    if "has_open_order" in exchange_truth:
        return bool(exchange_truth.get("has_open_order"))
    open_orders = exchange_truth.get("open_orders")
    if isinstance(open_orders, list):
        return bool(open_orders)
    if isinstance(open_orders, dict):
        for value in open_orders.values():
            if isinstance(value, list) and value:
                return True
            if isinstance(value, dict) and _truth_has_open_order({"open_orders": value}):
                return True
    return False


def _truth_has_nonzero_position(exchange_truth: dict[str, Any]) -> bool:
    if "has_nonzero_position" in exchange_truth:
        return bool(exchange_truth.get("has_nonzero_position"))
    positions = exchange_truth.get("positions")
    if isinstance(positions, list):
        items = positions
    elif isinstance(positions, dict):
        items = []
        for value in positions.values():
            if isinstance(value, dict):
                items.extend(value.values())
            elif isinstance(value, list):
                items.extend(value)
    else:
        items = []
    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            if abs(float(item.get("quantity") or item.get("qty") or 0.0)) > 1e-9:
                return True
        except (TypeError, ValueError):
            return True
    return False


def _exchange_truth_high_confidence_flat(exchange_truth: dict[str, Any] | None) -> bool:
    if not isinstance(exchange_truth, dict):
        return False
    available = bool(exchange_truth.get("available", exchange_truth.get("truth_available")))
    confidence = str(exchange_truth.get("confidence") or "").lower()
    has_nonzero_position = _truth_has_nonzero_position(exchange_truth)
    has_open_order = _truth_has_open_order(exchange_truth)
    return available and confidence == "high" and not has_nonzero_position and not has_open_order


def _local_state_has_cleanup_work(state: Any) -> bool:
    work_fields = (
        "open_positions",
        "pending_entries",
        "pending_closes",
        "pending_passive_closes",
        "pending_residual_repairs",
        "live_recovery_reduce_only_pairs",
    )
    return any(_has_any(_mapping_or_attr(state, field)) for field in work_fields)


def _classification_result(
    classification: str,
    *,
    reasons: list[str],
    state: Any,
    journal_events: list[dict[str, Any]],
    exchange_truth: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "classification": classification,
        "apply_allowed": classification == SAFE_TO_REPAIR,
        "reasons": reasons,
        "has_operator_fail_closed_evidence": _has_operator_fail_closed_evidence(journal_events),
        "operator_requested_mode": _operator_requested_mode(state),
        "risk_mode": _enum_value(_mapping_or_attr(state, "risk_mode")),
        "lifecycle": _enum_value(_mapping_or_attr(state, "lifecycle")),
        "recovery_blocked_reason": _mapping_or_attr(state, "recovery_blocked_reason"),
        "exchange_truth_summary": {
            "available": bool(exchange_truth.get("available", exchange_truth.get("truth_available"))) if isinstance(exchange_truth, dict) else False,
            "confidence": str(exchange_truth.get("confidence") or "") if isinstance(exchange_truth, dict) else "",
            "has_nonzero_position": _truth_has_nonzero_position(exchange_truth) if isinstance(exchange_truth, dict) else None,
            "has_open_order": _truth_has_open_order(exchange_truth) if isinstance(exchange_truth, dict) else None,
        },
    }


def classify_auto_fail_closed_latch(
    state: Any,
    *,
    journal_events: list[dict[str, Any]],
    exchange_truth: dict[str, Any] | None,
) -> dict[str, Any]:
    """Classify whether a stale fail-closed operator latch can be repaired."""
    reasons: list[str] = []
    if _has_operator_fail_closed_evidence(journal_events):
        return _classification_result(
            PRESERVE_OPERATOR_LATCH,
            reasons=["operator_fail_closed_evidence"],
            state=state,
            journal_events=journal_events,
            exchange_truth=exchange_truth,
        )

    if _operator_requested_mode(state) != GlobalRiskMode.FAIL_CLOSED.value:
        reasons.append("operator_latch_not_fail_closed")
    if _enum_value(_mapping_or_attr(state, "risk_mode")) != GlobalRiskMode.FAIL_CLOSED.value:
        reasons.append("risk_mode_not_fail_closed")
    if not _exchange_truth_high_confidence_flat(exchange_truth):
        reasons.append("exchange_truth_not_high_confidence_flat")
    if _local_state_has_cleanup_work(state):
        reasons.append("local_cleanup_work_present")
    recoveries = _mapping_or_attr(state, "unpaired_live_position_recoveries", [])
    if not _all_unpaired_recoveries_terminal_flat(recoveries):
        reasons.append("unpaired_recovery_not_terminal_flat")

    blocker = _mapping_or_attr(state, "recovery_blocked_reason")
    if blocker and str(blocker) not in _CLEARABLE_STALE_BLOCKERS:
        reasons.append("recovery_blocker_not_clearable")

    if reasons:
        return _classification_result(
            UNSAFE_TRUTH_OR_CLEANUP,
            reasons=reasons,
            state=state,
            journal_events=journal_events,
            exchange_truth=exchange_truth,
        )

    return _classification_result(
        SAFE_TO_REPAIR,
        reasons=[],
        state=state,
        journal_events=journal_events,
        exchange_truth=exchange_truth,
    )


def repair_auto_fail_closed_latch(
    state: Any,
    *,
    journal_events: list[dict[str, Any]],
    exchange_truth: dict[str, Any] | None,
    apply: bool = False,
    journal: Any = None,
    ts_ms: int | None = None,
) -> dict[str, Any]:
    """Dry-run or apply a safe stale auto fail-closed latch repair.

    Apply removes the pseudo operator latch and stale blocker, then delegates
    stale fail-closed release to the existing recovery cleanup path.
    """
    result = classify_auto_fail_closed_latch(
        state,
        journal_events=journal_events,
        exchange_truth=exchange_truth,
    )
    result["dry_run"] = not apply
    result["applied"] = False
    if not apply:
        return result
    if not result["apply_allowed"]:
        result["refused"] = True
        return result

    previous_risk_mode = _enum_value(_mapping_or_attr(state, "risk_mode"))
    previous_blocker = _mapping_or_attr(state, "recovery_blocked_reason")
    state.operator.requested_mode = None
    if previous_blocker:
        state.recovery_blocked_reason = None
        state.recovery_blocked_at_ms = 0
    if _mapping_or_attr(state, "lifecycle") is None:
        state.lifecycle = EngineLifecycle.RISK_ONLY
    cleared = clear_stale_fail_closed_if_recovery_clean(state, None)
    residual_blockers: list[str] = []
    if not cleared and _enum_value(_mapping_or_attr(state, "risk_mode")) == GlobalRiskMode.FAIL_CLOSED.value:
        residual_blockers.append("stale_fail_closed_not_cleared_by_recovery_core")

    result.update(
        {
            "applied": True,
            "previous_risk_mode": previous_risk_mode,
            "new_risk_mode": _enum_value(_mapping_or_attr(state, "risk_mode")),
            "residual_blockers": residual_blockers,
        }
    )
    if journal is not None and ts_ms is not None:
        event_kind = (
            "runtime.auto_fail_closed_recovered"
            if not residual_blockers
            else "runtime.auto_fail_closed_cleanup_failed"
        )
        journal.append_critical(
            ts_ms,
            event_kind,
            {
                "reason": "stale_auto_fail_closed_operator_latch_repaired",
                "source": "repair_auto_fail_closed_latch",
                "symbols": [],
                "venues": [],
                "cleanup_actions": [
                    "clear_pseudo_operator_latch",
                    "clear_stale_recovery_blocker",
                    "clear_stale_fail_closed_if_recovery_clean",
                ],
                "exchange_truth_summary": result["exchange_truth_summary"],
                "previous_risk_mode": previous_risk_mode,
                "new_risk_mode": result["new_risk_mode"],
                "residual_blockers": residual_blockers,
            },
        )
    return result
