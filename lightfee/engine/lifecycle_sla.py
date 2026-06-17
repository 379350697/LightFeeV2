"""Lightweight lifecycle SLA budget table for live diagnostics.

The table is intentionally small: it names the phase budget, the minimal
fail-closed action, and the truth source expected for terminality. Runtime code
can keep using the existing pending/close/recovery flows.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LifecyclePhaseBudget:
    phase: str
    soft_ms: int
    hard_ms: int
    action: str
    truth_source: str


def _positive_int(value: Any, fallback: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return fallback
    return parsed if parsed >= 0 else fallback


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
    "entry_selected_terminal": LifecyclePhaseBudget(
        phase="entry_selected_terminal",
        soft_ms=120000,
        hard_ms=300000,
        action="abort_reconcile_cleanup_fail_closed",
        truth_source="selected_to_terminal_runtime_lifecycle",
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
    strategy: Any | None = None,
) -> dict[str, LifecyclePhaseBudget]:
    """Return lifecycle budgets, honoring lightweight StrategyConfig overrides."""

    def cfg_ms(name: str, fallback: int) -> int:
        return _positive_int(getattr(strategy, name, fallback), fallback)

    candidate_hard_ms = cfg_ms("candidate_lease_ms", 60000)
    selected_hard_ms = cfg_ms("selected_submit_deadline_ms", 15000)
    maker_soft_ms = cfg_ms("maker_resting_soft_ms", 30000)
    maker_hard_ms = cfg_ms("maker_resting_hard_ms", 60000)
    pending_soft_ms = cfg_ms("pending_entry_force_terminal_after_ms", 60000)
    pending_hard_ms = cfg_ms("pending_entry_hard_ceiling_ms", 120000)
    entry_selected_soft_ms = cfg_ms("entry_selected_warning_ms", 120000)
    entry_selected_hard_ms = cfg_ms("entry_selected_terminal_sla_ms", 300000)
    close_soft_ms = cfg_ms("close_terminal_soft_ms", 60000)
    close_hard_ms = cfg_ms("close_terminal_hard_ms", 300000)
    recovery_hard_ms = cfg_ms("recovery_terminal_hard_ms", 300000)

    defaults = DEFAULT_PHASE_BUDGETS
    return {
        "quote_rewarm": defaults["quote_rewarm"],
        "candidate_lease": LifecyclePhaseBudget(
            phase="candidate_lease",
            soft_ms=min(30000, candidate_hard_ms),
            hard_ms=candidate_hard_ms,
            action=defaults["candidate_lease"].action,
            truth_source=defaults["candidate_lease"].truth_source,
        ),
        "selected_pre_submit": LifecyclePhaseBudget(
            phase="selected_pre_submit",
            soft_ms=0,
            hard_ms=selected_hard_ms,
            action=defaults["selected_pre_submit"].action,
            truth_source=defaults["selected_pre_submit"].truth_source,
        ),
        "maker_resting": LifecyclePhaseBudget(
            phase="maker_resting",
            soft_ms=maker_soft_ms,
            hard_ms=maker_hard_ms,
            action=defaults["maker_resting"].action,
            truth_source=defaults["maker_resting"].truth_source,
        ),
        "pending_entry": LifecyclePhaseBudget(
            phase="pending_entry",
            soft_ms=pending_soft_ms,
            hard_ms=pending_hard_ms,
            action=defaults["pending_entry"].action,
            truth_source=defaults["pending_entry"].truth_source,
        ),
        "entry_selected_terminal": LifecyclePhaseBudget(
            phase="entry_selected_terminal",
            soft_ms=entry_selected_soft_ms,
            hard_ms=entry_selected_hard_ms,
            action=defaults["entry_selected_terminal"].action,
            truth_source=defaults["entry_selected_terminal"].truth_source,
        ),
        "open_position_due_close": defaults["open_position_due_close"],
        "close_terminal": LifecyclePhaseBudget(
            phase="close_terminal",
            soft_ms=close_soft_ms,
            hard_ms=close_hard_ms,
            action=defaults["close_terminal"].action,
            truth_source=defaults["close_terminal"].truth_source,
        ),
        "recovery_terminal": LifecyclePhaseBudget(
            phase="recovery_terminal",
            soft_ms=min(60000, recovery_hard_ms),
            hard_ms=recovery_hard_ms,
            action=defaults["recovery_terminal"].action,
            truth_source=defaults["recovery_terminal"].truth_source,
        ),
    }


def classify_phase_age(age_ms: int, budget: LifecyclePhaseBudget) -> str:
    if budget.hard_ms > 0 and age_ms >= budget.hard_ms:
        return "hard_over_budget"
    if budget.soft_ms > 0 and age_ms >= budget.soft_ms:
        return "soft_over_budget"
    return "ok"
