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
