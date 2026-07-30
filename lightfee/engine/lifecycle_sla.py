"""Lightweight lifecycle SLA budget table for live diagnostics.

The table is intentionally small: it names the phase budget, the minimal
containment action, and the truth source expected for terminality. Runtime code
can keep using the existing pending/close/recovery flows.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LifecyclePhaseBudget:
    phase: str
    soft_ms: int
    hard_ms: int
    action: str
    truth_source: str


DEFAULT_PHASE_BUDGETS: dict[str, LifecyclePhaseBudget] = {
    "quote_rewarm": LifecyclePhaseBudget(
        phase="quote_rewarm",
        soft_ms=10000,
        hard_ms=30000,
        action="skip_candidate_after_hard_rewarm",
        truth_source="fresh_quote_lease_or_ws_bbo",
    ),
    "candidate_lease": LifecyclePhaseBudget(
        phase="candidate_lease",
        soft_ms=30000,
        hard_ms=60000,
        action="expire_candidate_and_rescan",
        truth_source="fresh_scan_shortlist_candidate",
    ),
    "selected_pre_submit": LifecyclePhaseBudget(
        phase="selected_pre_submit",
        soft_ms=0,
        hard_ms=15000,
        action="cancel_selection_and_rescan",
        truth_source="execution.entry_selected_without_submit_or_order_id",
    ),
    "maker_resting": LifecyclePhaseBudget(
        phase="maker_resting",
        soft_ms=30000,
        hard_ms=60000,
        action="cancel_then_reconcile_open_orders_trades_positions",
        truth_source="open_orders_trades_positions",
    ),
    "pending_entry": LifecyclePhaseBudget(
        phase="pending_entry",
        soft_ms=60000,
        hard_ms=120000,
        action="v1_force_terminal_cancel_reconcile_finalize_abort",
        truth_source="pending_entry_terminality_from_order_fill_position_truth",
    ),
    "open_position_due_close": LifecyclePhaseBudget(
        phase="open_position_due_close",
        soft_ms=60000,
        hard_ms=300000,
        action="escalate_close_due_recovery_without_age_forced_exit",
        truth_source="funding_settlement_or_risk_close_due",
    ),
    "close_terminal": LifecyclePhaseBudget(
        phase="close_terminal",
        soft_ms=60000,
        hard_ms=300000,
        action="aggressive_escalation_then_recovery_or_residual",
        truth_source="exchange_flat_no_open_orders_or_residual_work",
    ),
    "recovery_terminal": LifecyclePhaseBudget(
        phase="recovery_terminal",
        soft_ms=60000,
        hard_ms=300000,
        action="block_new_risk_and_escalate_recovery",
        truth_source="exchange_position_open_order_truth",
    ),
}


def phase_budgets_from_strategy(
    strategy: object | None = None,
) -> dict[str, LifecyclePhaseBudget]:
    """Return diagnostic labels, not a second set of runtime deadlines.

    V2 exposed these as strategy settings although none of the corresponding
    business flows consumed a common contract.  V1's live lifecycle owns the
    real pending-entry/close/recovery deadlines.  Keep this stable table only
    for quote-rewarm scheduling and diagnosis, so configuration cannot imply a
    control plane that does not exist.
    """

    del strategy
    return dict(DEFAULT_PHASE_BUDGETS)


def classify_phase_age(age_ms: int, budget: LifecyclePhaseBudget) -> str:
    if budget.hard_ms > 0 and age_ms >= budget.hard_ms:
        return "hard_over_budget"
    if budget.soft_ms > 0 and age_ms >= budget.soft_ms:
        return "soft_over_budget"
    return "ok"
