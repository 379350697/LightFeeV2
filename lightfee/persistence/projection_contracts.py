"""Projection boundaries and fact classification for V2 journal-to-fact layer.

Defines which journal kinds are projected into queryable tables and which
stay journal-only, matching the classification in:
docs/superpowers/specs/2026-05-12-v2-journal-fact-store-projection-design.md
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Projected fact groups — these journal kinds are materialized into SQLite
# ---------------------------------------------------------------------------

PROJECTED_ORDER_KINDS: frozenset[str] = frozenset({
    "order.submitted",
    "order.filled",
    "order.rejected",
    "order.uncertain",
})

PROJECTED_ENTRY_EXIT_KINDS: frozenset[str] = frozenset({
    "entry.opened",
    "entry.aborted",
    "entry.aborted_failed_pending_retained",
    "entry.hedge_drive_cancel_replace",
    "entry.hedge_drive_no_adapter",
    "entry.hedge_drive_reprice",
    "entry.hedge_rejected_residual",
    "entry.pending_registered",
    "entry.residual_detected",
    "exit.closed",
    "exit.close_chunk_submitted",
    "exit.close_order_intent_claimed",
    "exit.passive_close_registered",
    "exit.close_residual_detected",
    "exit.partial_closed",
    "exit.passive_close_advance_blocked_maker_under_chunk",
    "exit.passive_close_advance_blocked_unhedged",
    "exit.passive_close_advanced_chunk",
    "exit.passive_close_amend_failed",
    "exit.passive_close_amend_requested",
    "exit.passive_close_amend_succeeded",
    "exit.passive_close_cancel_error",
    "exit.passive_close_cancel_not_supported",
    "exit.passive_close_cancel_replace_blocked_double_order_risk",
    "exit.passive_close_cancel_replace_completed",
    "exit.passive_close_cancel_replace_requested",
    "exit.passive_close_cancel_replace_submit_failed",
    "exit.passive_close_chunk_filled",
    "exit.passive_close_created",
    "exit.passive_close_dual_taker_drive",
    "exit.passive_close_fallback_aggressive",
    "exit.passive_close_fallback_aggressive_null_result",
    "exit.passive_close_fallback_complete",
    "exit.passive_close_fallback_unavailable",
    "exit.passive_close_fallback_unhedged_failed",
    "exit.passive_close_fallback_zero_fill_no_pending",
    "exit.passive_close_hedge_error",
    "exit.passive_close_hedge_filled",
    "exit.passive_close_hedge_incomplete",
    "exit.passive_close_hedge_non_retryable_escalated",
    "exit.passive_close_hedge_partial",
    "exit.passive_close_invalid_aligned_price",
    "exit.passive_close_maintain_no_price_hint",
    "exit.passive_close_maintain_no_resting_price",
    "exit.passive_close_maintain_no_target_price",
    "exit.passive_close_maintain_no_tick_size",
    "exit.passive_close_maker_leg_l2_missing",
    "exit.passive_close_maker_leg_selected",
    "exit.passive_close_maker_progress",
    "exit.passive_close_maker_reduce_only_rejected",
    "exit.passive_close_maker_submit_error",
    "exit.passive_close_maker_submit_max_failures_escalated",
    "exit.passive_close_maker_submitted",
    "exit.passive_close_missing_l2_or_tick",
    "exit.passive_close_missing_l2_tick_escalated",
    "exit.passive_close_no_adapter",
    "exit.passive_close_not_supported",
    "exit.passive_close_orphaned",
    "exit.passive_close_partial_cycle_complete",
    "exit.passive_close_recovery_cleared_flat",
    "exit.passive_close_recovery_probe_flat",
    "exit.passive_close_recovery_resumed",
    "exit.passive_close_resolved",
    "exit.passive_close_unhedged_residual",
    "exit.pending_close_registered",
    "exit.reconciled",
    "exit.billing_unreconciled",
    "exit.billing_evidence_unavailable",
    "exit.billing_evidence_debt_registered",
    "exit.retry_wait",
})

PROJECTED_SCAN_KINDS: frozenset[str] = frozenset({
    "scan.completed",
    "scan.no_entry_diagnostics",
    "scan.runtime_gate_blocked",
})

PROJECTED_RISK_KINDS: frozenset[str] = frozenset({
    "risk.death_line_triggered",
    "risk.death_protection_close_initiated",
    "risk.death_triggered",
    "risk.delever_close_initiated",
    "risk.delever_limit_reached",
    "risk.delever_line_triggered",
    "risk.delever_recovered",
    "risk.delever_triggered",
    "risk.entry_pause_cleared",
    "risk.fail_closed_entered",
    "risk.global_mode_changed",
    "risk.line_disabled",
    "risk.single_side_protection_failed",
    "risk.single_side_protection_triggered",
    "risk.single_side_protection_unavailable",
    "risk.warning_cleared",
    "risk.warning_line_triggered",
    "risk.warning_triggered",
})

PROJECTED_L2_HEALTH_KINDS: frozenset[str] = frozenset({
    "runtime.local_l2_sequence_gap",
    "runtime.local_l2_sync_failed",
})

PROJECTED_EXECUTION_KINDS: frozenset[str] = frozenset({
    "execution.dual_taker_armed",
    "execution.entry_liquidity_blocked",
    "execution.maker_event_lane_wake",
    "execution.passive_cycle_zero_fill",
    "execution.passive_phase_switched",
})

PROJECTED_LEDGER_BRIDGE_KINDS: frozenset[str] = frozenset({
    "entry.compensated",
    "execution.compensation_failed",
    "exit.compensated",
    "runtime.position_lifecycle_terminal",
})

# ---------------------------------------------------------------------------
# All projected kinds (union of all projection groups)
# ---------------------------------------------------------------------------

ALL_PROJECTED_KINDS: frozenset[str] = (
    PROJECTED_ORDER_KINDS
    | PROJECTED_ENTRY_EXIT_KINDS
    | PROJECTED_SCAN_KINDS
    | PROJECTED_RISK_KINDS
    | PROJECTED_L2_HEALTH_KINDS
    | PROJECTED_EXECUTION_KINDS
    | PROJECTED_LEDGER_BRIDGE_KINDS
)

# ---------------------------------------------------------------------------
# Journal-first kinds — these stay in the journal for replay authority. Some
# recovery kinds may also receive rebuildable ledger rows for attribution.
# ---------------------------------------------------------------------------

JOURNAL_ONLY_RECOVERY_KINDS: frozenset[str] = frozenset({
    "recovery.blocked",
    "recovery.flat",
    "recovery.live_detected",
    "recovery.mismatch_detected",
    "recovery.mismatch_flattened",
    "recovery.resumed",
})

JOURNAL_ONLY_LIFECYCLE_KINDS: frozenset[str] = frozenset({
    "pending_entry.viability_blocked",
    "runtime.entry_blocked_lifecycle",
    "runtime.entry_blocked_lifecycle_selection",
    "runtime.lifecycle_changed",
    "runtime.risk_mode_changed",
    "runtime.booting",
    "runtime.running",
    "runtime.stopped",
    "runtime.started",
    "runtime.reconciling",
    "runtime.reconciling_complete",
    "runtime.fail_closed",
    "runtime.recovery_blocked",
    "runtime.snapshot_missing",
    "runtime.snapshot_stale",
})

JOURNAL_ONLY_DIAGNOSTIC_KINDS: frozenset[str] = frozenset({
    "entry.opportunity_funnel",
    "runtime.active_position_tick",
    "runtime.active_tick_error",
    "runtime.adapter_shutdown_error",
    "runtime.candidates_tradeable",
    "scan.shortlist_ready",
    "runtime.entry_dispatched",
    "runtime.entry_dispatch_error",
    "runtime.entry_owner_claimed",
    "runtime.entry_owner_claim_retained",
    "runtime.entry_owner_handoff_complete",
    "runtime.entry_owner_handoff_incomplete",
    "runtime.entry_skipped_duplicate_client_order_id",
    "runtime.entry_skipped_existing_pending",
    "runtime.entry_skipped_no_quote",
    "runtime.entry_skipped_planner_rejected",
    "runtime.maker_event_lane_wake",
    "runtime.maker_event_reprice",
    "runtime.maker_event_reprice_error",
    "runtime.maker_event_tick_error",
    "runtime.normal_close_routing_aggressive",
    "runtime.normal_close_routing_passive",
    "runtime.passive_close_recovery_result",
    "runtime.passive_close_tick_error",
    "runtime.pending_entry_registered",
    "runtime.position_opened",
    "runtime.rate_limit_reload_error",
    "runtime.risk_plan_generated",
    "runtime.risk_snapshot_fetch_error",
    "runtime.tick_error",
})

JOURNAL_ONLY_SIDECAR_KINDS: frozenset[str] = frozenset({
    "sidecar.candidate_published",
})

ALL_JOURNAL_ONLY_KINDS: frozenset[str] = (
    JOURNAL_ONLY_RECOVERY_KINDS
    | JOURNAL_ONLY_LIFECYCLE_KINDS
    | JOURNAL_ONLY_DIAGNOSTIC_KINDS
    | JOURNAL_ONLY_SIDECAR_KINDS
)

# ---------------------------------------------------------------------------
# Fact table mapping — which projection table each fact group writes to
# ---------------------------------------------------------------------------

FACT_TABLE_MAP: dict[str, str] = {
    **{k: "order_facts" for k in PROJECTED_ORDER_KINDS},
    **{k: "entry_exit_facts" for k in PROJECTED_ENTRY_EXIT_KINDS},
    **{k: "scan_facts" for k in PROJECTED_SCAN_KINDS},
    **{k: "risk_counter_facts" for k in PROJECTED_RISK_KINDS},
    **{k: "local_l2_health_facts" for k in PROJECTED_L2_HEALTH_KINDS},
    **{k: "diagnostic_facts" for k in PROJECTED_EXECUTION_KINDS},
    **{k: "trade_ledger_events" for k in PROJECTED_LEDGER_BRIDGE_KINDS},
}

# All fact table names that the projection layer writes into
PROJECTION_TABLES: tuple[str, ...] = (
    "projected_facts",
    "order_facts",
    "entry_exit_facts",
    "scan_facts",
    "risk_counter_facts",
    "local_l2_health_facts",
    "diagnostic_facts",
    "trade_ledger_events",
    "position_ledger",
    "position_pnl_facts",
    "order_ledger",
    "fill_ledger",
)

# ---------------------------------------------------------------------------
# Classifier API
# ---------------------------------------------------------------------------


def is_projected_kind(kind: str) -> bool:
    """Return True if this journal kind should be projected into a fact table."""
    return kind in ALL_PROJECTED_KINDS


def is_journal_only_kind(kind: str) -> bool:
    """Return True if this journal kind must stay journal-first."""
    return kind in ALL_JOURNAL_ONLY_KINDS


def classify_kind(kind: str) -> str:
    """Classify a journal kind as 'projected', 'journal_only', or 'unclassified'.

    Every journal kind in the codebase must resolve to exactly one of these
    three buckets.  An 'unclassified' result means the kind has not yet been
    assigned to either the projection or journal-only set.
    """
    if kind in ALL_PROJECTED_KINDS:
        return "projected"
    if kind in ALL_JOURNAL_ONLY_KINDS:
        return "journal_only"
    return "unclassified"


def fact_table_for_kind(kind: str) -> str | None:
    """Return the target fact table name for a projected kind, or None."""
    return FACT_TABLE_MAP.get(kind)
