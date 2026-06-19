"""Shared live-trading business contract helpers.

The functions in this module are intentionally pure.  Runtime paths can keep
their existing control flow while sharing the same admission, quantity, terminal
truth, and diagnosis vocabulary.
"""

from __future__ import annotations

from typing import Any


DETERMINISTIC_ENTRY_ADMISSION_REASONS = frozenset({
    "bybit_trading_terms_required",
    "insufficient_balance_admission_blocked",
    "insufficient_margin_admission_blocked",
    "leverage_admission_blocked",
    "max_notional_admission_blocked",
    "venue_auth_invalid",
})


def classify_business_event_kind(
    kind: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    text = str(kind or "")
    payload = payload or {}
    if text == "execution.dual_taker_armed":
        return {
            "phase": "PENDING_ENTRY",
            "terminality": "terminal_fallback_armed",
            "action_taken": "execute_terminal_taker_fallback",
            "action_evidence_kind": text,
            "diagnostic_severity": "info",
            "owner_id": str(
                payload.get("entry_id") or payload.get("position_id") or ""
            ),
        }
    if text in {
        "runtime.entry_quote_rewarm_scheduled_after_rest_stale",
        "runtime.entry_quote_rewarm_terminal_stale",
        "runtime.entry_quote_revalidate_resolved",
        "runtime.entry_quote_revalidate_failed",
    }:
        return {
            "phase": "ENTRY_QUOTE_LEASE",
            "terminality": (
                "terminal"
                if text != "runtime.entry_quote_rewarm_scheduled_after_rest_stale"
                else "active"
            ),
            "action_taken": str(payload.get("action_taken") or ""),
            "action_evidence_kind": text,
            "diagnostic_severity": "info",
            "owner_id": _venue_symbol_owner(payload),
        }
    return {}


def quote_rewarm_handoff_contract(
    *,
    phase: str,
    status: str,
    configured_action: str,
    terminal_kind: str = "",
) -> dict[str, str]:
    if str(phase or "") != "quote_rewarm":
        return {}
    action = str(configured_action or "")
    if terminal_kind:
        return {
            "action_taken": action,
            "action_evidence_kind": str(terminal_kind),
            "diagnostic_severity": "info",
        }
    if str(status or "") == "hard_over_budget":
        return {
            "action_taken": action,
            "action_evidence_kind": "business_contract.quote_rewarm_hard_timeout",
            "diagnostic_severity": "production_issue",
        }
    return {}


def close_order_error_resolution_contract(
    *,
    kind: str,
    payload: dict[str, Any],
    current_exchange_truth_clean: bool,
    position_terminal_match: bool,
    order_terminal_match: bool,
    has_order_identity: bool,
    is_post_only_close_reject: bool | None = None,
) -> dict[str, Any]:
    if not current_exchange_truth_clean:
        return {"resolved": False, "resolution_bucket": ""}
    post_only = (
        bool(is_post_only_close_reject)
        if is_post_only_close_reject is not None
        else _payload_is_post_only_close_reject(payload)
    )
    reduce_only = _payload_is_reduce_only_terminal_flat_reject(payload)
    zero_fill = (
        str(kind or "") == "order.uncertain"
        and "zero fill" in _payload_reason_text(payload)
    )
    if post_only:
        return {
            "resolved": bool(position_terminal_match),
            "resolution_bucket": "post_only_boundary_reject",
        }
    if reduce_only or zero_fill:
        resolved = bool(
            order_terminal_match
            or (not has_order_identity and position_terminal_match)
        )
        return {
            "resolved": resolved,
            "resolution_bucket": (
                "reduce_only_terminal_flat"
                if reduce_only
                else "zero_fill_terminal_flat"
            ),
        }
    return {"resolved": False, "resolution_bucket": ""}


def entry_admission_blocks_candidate(reason: str, block_scope: str) -> bool:
    if str(block_scope or "").lower() == "venue":
        return True
    return str(reason or "") in DETERMINISTIC_ENTRY_ADMISSION_REASONS


def entry_admission_aggregation_key(
    *,
    stage: str,
    venue: str,
    symbol: str,
    reason: str,
    block_scope: str,
) -> str:
    return ":".join([
        str(stage or "unknown"),
        str(venue or "multiple").lower(),
        str(symbol or "*").upper(),
        str(reason or "unknown"),
        str(block_scope or "symbol").lower(),
    ])


def classify_entry_quantity_contract(
    *,
    raw_quantity: float,
    common_quantity: float,
    effective_quantity: float,
    epsilon: float = 1e-9,
) -> dict[str, Any]:
    raw = _safe_float(raw_quantity)
    common = _safe_float(common_quantity)
    effective = _safe_float(effective_quantity)
    residual = max(raw - common, 0.0)
    if effective <= epsilon or common <= epsilon:
        status = "blocked_unhedgeable_quantity"
    elif residual > epsilon:
        status = "hedgeable_adjusted"
    else:
        status = "hedgeable"
    return {
        "quantity_contract_status": status,
        "unhedgeable_residual_quantity": residual,
    }


def close_reconciliation_evidence_fields(
    *,
    long_quantity: float,
    short_quantity: float,
    duplicate_close_leg_suppressed_count: int = 0,
    epsilon: float = 1e-12,
) -> dict[str, Any]:
    long_found = _safe_float(long_quantity) > epsilon
    short_found = _safe_float(short_quantity) > epsilon
    complete = long_found and short_found
    trade_probe_status = {
        "long": "found" if long_found else "missing",
        "short": "found" if short_found else "missing",
    }
    if complete:
        reason = ""
        statement_status = "complete"
    elif long_found:
        reason = "missing_short_close_trade_statement"
        statement_status = "partial"
    elif short_found:
        reason = "missing_long_close_trade_statement"
        statement_status = "partial"
    else:
        reason = "missing_both_close_trade_statements"
        statement_status = "missing"
    if not complete and duplicate_close_leg_suppressed_count > 0:
        reason = f"{reason}_after_duplicate_leg_suppression"
    return {
        "evidence_gap_reason": reason,
        "statement_probe_status": statement_status,
        "trade_probe_status": trade_probe_status,
    }


def passive_close_has_terminal_truth(payload: dict[str, Any]) -> bool:
    truth = payload.get("exchange_truth")
    if not isinstance(truth, dict):
        return False
    if truth.get("truth_available") is False:
        return False
    positions_flat = truth.get("positions_flat")
    if not isinstance(positions_flat, bool):
        positions = truth.get("positions")
        if isinstance(positions, list):
            position_items = [item for item in positions if isinstance(item, dict)]
            positions_flat = bool(position_items) and all(
                abs(_safe_float((item or {}).get("quantity"))) <= 1e-9
                for item in position_items
            )
        else:
            positions_flat = False
    open_orders_flat = truth.get("open_orders_flat")
    if not isinstance(open_orders_flat, bool):
        open_order_truth = truth.get("open_order_truth")
        if isinstance(open_order_truth, list):
            open_order_items = [
                item for item in open_order_truth if isinstance(item, dict)
            ]
            open_orders_flat = bool(open_order_items) and all(
                bool((item or {}).get("open_orders_empty"))
                for item in open_order_items
            )
        else:
            open_orders_flat = False
    return bool(positions_flat) and bool(open_orders_flat)


def diagnose_issue_counts(payload: dict[str, Any], kind: str) -> dict[str, int]:
    if kind == "execution.entry_quantity_plan":
        status = str(payload.get("quantity_contract_status") or "")
        blocked = int(status.startswith("blocked_"))
        return {"entry_quantity_contract_blocked_count": blocked}
    if kind == "exit.reconciled":
        gap = payload.get("evidence_gap") is True
        return {"close_reconciliation_evidence_gap_count": int(gap)}
    if kind == "runtime.entry_admission_venue_degraded":
        return {
            "admission_degraded_suppressed_count": _safe_int(
                payload.get("suppressed_count")
            )
        }
    return {}


def _payload_is_reduce_only_terminal_flat_reject(payload: dict[str, Any]) -> bool:
    request_context = _payload_request_context(payload)
    if not _boolish(request_context.get("reduce_only")):
        return False
    exchange_error = _exchange_error_dict(payload)
    code = str(
        payload.get("exchange_code")
        or exchange_error.get("exchange_code")
        or payload.get("code")
        or exchange_error.get("code")
        or ""
    ).strip()
    reason = _payload_reason_text(payload)
    return (
        code == "-2022"
        or "reduceonly order is rejected" in reason
        or "reduce only order is rejected" in reason
        or "reduce-only order is rejected" in reason
    )


def _payload_is_post_only_close_reject(payload: dict[str, Any]) -> bool:
    request_context = _payload_request_context(payload)
    reason = _payload_reason_text(payload)
    return (
        ("post only" in reason or "post-only" in reason)
        and _boolish(request_context.get("post_only"))
        and _boolish(request_context.get("reduce_only"))
    )


def _payload_reason_text(payload: dict[str, Any]) -> str:
    exchange_error = _exchange_error_dict(payload)
    return str(
        payload.get("reason")
        or payload.get("error")
        or payload.get("exchange_msg")
        or payload.get("msg")
        or exchange_error.get("exchange_msg")
        or exchange_error.get("raw_body")
        or exchange_error.get("msg")
        or ""
    ).lower()


def _venue_symbol_owner(payload: dict[str, Any]) -> str:
    venue = str(payload.get("venue") or "").lower()
    symbol = str(payload.get("symbol") or "").upper()
    return f"{venue}:{symbol}" if venue and symbol else ""


def _exchange_error_dict(payload: dict[str, Any]) -> dict[str, Any]:
    exchange_error = payload.get("exchange_error")
    return exchange_error if isinstance(exchange_error, dict) else {}


def _payload_request_context(payload: dict[str, Any]) -> dict[str, Any]:
    request_context = payload.get("request_context")
    if isinstance(request_context, dict):
        return request_context
    exchange_error = _exchange_error_dict(payload)
    request_context = exchange_error.get("request_context")
    return request_context if isinstance(request_context, dict) else {}


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return max(int(value or 0), 0)
    except (TypeError, ValueError):
        return 0
