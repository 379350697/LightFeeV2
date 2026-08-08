import json
from pathlib import Path

from lightfee.engine.loop_control import _export_current_state_snapshot
from lightfee.engine.state import EngineState
from lightfee.ops.production_health import analyze_current_state
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from scripts.diagnose_live import _build_production_acceptance_gate

REPO_ROOT = Path(__file__).resolve().parents[2]


def _clean_exchange_truth():
    return {
        "available": True,
        "truth_available": True,
        "confidence": "high",
        "has_nonzero_position": False,
        "has_open_order": False,
        "positions": {},
        "open_orders": {},
    }


def test_unavailable_truth_without_local_work_is_nonblocking_evidence_gap():
    from lightfee.engine.v1_lifecycle_closure import build_v1_lifecycle_closure_table

    table = build_v1_lifecycle_closure_table(
        local_state={
            "lifecycle": "running",
            "risk_mode": "running",
            "open_position_count": 0,
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "pending_residual_repair_count": 0,
        },
        exchange_truth={
            "available": False,
            "truth_available": False,
            "confidence": "low",
            "missing_evidence": ["exchange_truth_fetch_timeout"],
            "errors": ["timeout"],
        },
        generated_at_ms=1770000000000,
    )

    payload = table.to_dict()
    assert payload["summary"]["entry_allowed"] is True
    assert payload["summary"]["recovery_block_policy"] == "warn_evidence_gap"
    assert payload["summary"]["recovery_decision_kind"] == "RUNNING_WITH_EVIDENCE_GAP"
    assert payload["rows"][0]["phase"] == "RECOVERY_TRUTH"
    assert payload["rows"][0]["evidence_class"] == "partial_evidence_gap"


def test_orphan_non_reduce_open_order_blocks_as_v1_live_artifact():
    from lightfee.engine.v1_lifecycle_closure import build_v1_lifecycle_closure_table

    table = build_v1_lifecycle_closure_table(
        local_state={
            "lifecycle": "running",
            "risk_mode": "running",
            "open_position_count": 0,
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "pending_residual_repair_count": 0,
        },
        exchange_truth={
            "available": True,
            "truth_available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": True,
            "positions": {},
            "open_orders": {
                "bybit": {
                    "BTCUSDT": {
                        "symbol": "BTCUSDT",
                        "venue": "bybit",
                        "quantity": 1.0,
                        "price": 100.0,
                        "reduce_only": False,
                        "order_id": "order-1",
                    }
                }
            },
        },
        generated_at_ms=1770000000000,
    )

    payload = table.to_dict()
    assert payload["summary"]["entry_allowed"] is False
    assert payload["summary"]["recovery_block_reason"] == "orphan_maker_order"
    assert payload["summary"]["recovery_decision_kind"] == "BLOCK_OR_FLATTEN_LIVE_ARTIFACT"
    assert any(
        row["phase"] == "OPEN_POSITION"
        and row["recovery_policy"] == "block_or_flatten_live_artifact"
        for row in payload["rows"]
    )


def test_owned_pending_entry_live_conflict_projects_as_live_artifact_row():
    from lightfee.engine.recovery_ledger import (
        ExchangeArtifact,
        RecoveryDecision,
        RecoveryLedger,
        RecoveryOwner,
        RecoveryWorkItem,
    )
    from lightfee.engine.v1_lifecycle_closure import build_v1_lifecycle_closure_table

    ledger = RecoveryLedger(
        work_items=[
            RecoveryWorkItem(
                kind="owned_pending_entry_live_conflict",
                symbol="HOMEUSDT",
                venues=frozenset({"bybit"}),
                artifacts=(
                    ExchangeArtifact(
                        kind="position",
                        symbol="HOMEUSDT",
                        venue="bybit",
                        side="sell",
                        quantity=1600.0,
                    ),
                ),
                owner=RecoveryOwner(
                    owner_type="pending_entry",
                    owner_id="entry-home",
                    confidence="owned",
                ),
                decision=RecoveryDecision(
                    outcome="pending_entry_live_conflict_requires_cleanup",
                    reason="pending_entry_positive_fill_conflicts_with_live_truth",
                ),
                blocking=True,
            )
        ]
    )

    table = build_v1_lifecycle_closure_table(
        local_state={"pending_entries": {}},
        exchange_truth=_clean_exchange_truth(),
        generated_at_ms=1770000000000,
        recovery_ledger=ledger,
    ).to_dict()

    assert table["summary"]["entry_allowed"] is False
    assert table["summary"]["recovery_block_reason"] == "owned_pending_entry_live_conflict"
    row = next(
        row
        for row in table["rows"]
        if row["terminality"] == "owned_pending_entry_live_conflict"
    )
    assert row["phase"] == "OPEN_POSITION"
    assert row["entry_policy"] == "block_all_new_risk"
    assert row["recovery_policy"] == "block_or_flatten_live_artifact"


def test_pending_entry_with_live_order_retains_owner_and_blocks_removal():
    from lightfee.engine.v1_lifecycle_closure import build_v1_lifecycle_closure_table

    table = build_v1_lifecycle_closure_table(
        local_state={
            "lifecycle": "running",
            "risk_mode": "running",
            "pending_entries": {
                "entry-1": {
                    "pending_id": "entry-1",
                    "position_id": "entry-1",
                    "symbol": "BTCUSDT",
                    "maker_leg_filled": 0.0,
                    "hedge_leg_filled": 0.0,
                }
            },
        },
        exchange_truth={
            "available": True,
            "truth_available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": True,
            "open_orders": {
                "bybit": {
                    "BTCUSDT": {
                        "symbol": "BTCUSDT",
                        "venue": "bybit",
                        "quantity": 1.0,
                        "reduce_only": False,
                    }
                }
            },
        },
        generated_at_ms=1770000000000,
    )

    row = next(row for row in table.to_dict()["rows"] if row["phase"] == "PENDING_ENTRY")
    assert row["owner_id"] == "entry-1"
    assert row["terminality"] == "retain_live_open_order"
    assert row["entry_policy"] == "block_conflicting_new_risk"
    assert row["recovery_policy"] == "manage_pending_entry"


def test_pending_entry_positive_fill_single_leg_live_truth_blocks_open_terminality():
    from lightfee.engine.v1_lifecycle_closure import build_v1_lifecycle_closure_table

    table = build_v1_lifecycle_closure_table(
        local_state={
            "lifecycle": "running",
            "risk_mode": "running",
            "pending_entries": {
                "entry-1781373126018-HOMEUSDT": {
                    "pending_id": "entry-1781373126018-HOMEUSDT",
                    "position_id": "entry-1781373126018-HOMEUSDT",
                    "symbol": "HOMEUSDT",
                    "long_venue": "okx",
                    "short_venue": "bybit",
                    "maker_leg_filled": 1600.0,
                    "hedge_leg_filled": 1600.0,
                }
            },
        },
        exchange_truth={
            "available": True,
            "truth_available": True,
            "confidence": "high",
            "has_nonzero_position": True,
            "has_open_order": False,
            "positions": {
                "okx": {"HOMEUSDT": {"quantity": 0.0, "side": "Side.BUY"}},
                "bybit": {"HOMEUSDT": {"quantity": 1600.0, "side": "Side.SELL"}},
            },
            "open_orders": {},
        },
        generated_at_ms=1781373163000,
    )

    row = next(row for row in table.to_dict()["rows"] if row["phase"] == "PENDING_ENTRY")
    assert row["owner_id"] == "entry-1781373126018-HOMEUSDT"
    assert row["terminality"] == "positive_fill_live_truth_conflict"
    assert row["entry_policy"] == "block_conflicting_new_risk"
    assert row["recovery_policy"] == "manage_pending_entry"
    assert row["details"]["decision_reason"] == "positive_fill_conflicts_with_live_unmatched_truth"
    assert row["details"]["matched_quantity"] == 1600.0
    assert row["details"]["live_long_quantity"] == 0.0
    assert row["details"]["live_short_quantity"] == 1600.0
    assert row["details"]["live_balanced_quantity"] == 0.0


def test_residual_dust_rows_split_tolerated_and_blocking_abnormal():
    from lightfee.engine.v1_lifecycle_closure import build_v1_lifecycle_closure_table

    table = build_v1_lifecycle_closure_table(
        local_state={
            "pending_residual_repairs": [
                {
                    "task_id": "dust-ok",
                    "symbol": "SAHARAUSDT",
                    "origin": "entry_open",
                    "terminal_reason": "exchange_min_notional_dust",
                    "residual_ratio": 0.015,
                },
                {
                    "task_id": "dust-bad",
                    "symbol": "SAHARAUSDT",
                    "origin": "entry_open",
                    "terminal_reason": "exchange_min_notional_dust",
                    "residual_ratio": 0.03,
                },
            ],
        },
        exchange_truth=_clean_exchange_truth(),
        generated_at_ms=1770000000000,
    )

    rows = {
        row["owner_id"]: row
        for row in table.to_dict()["rows"]
        if row["phase"] == "RESIDUAL_REPAIR"
    }
    assert rows["dust-ok"]["terminality"] == "terminal_dust_tolerated"
    assert rows["dust-ok"]["entry_policy"] == "allow_after_terminal"
    assert rows["dust-bad"]["terminality"] == "retain_residual_repair"
    assert rows["dust-bad"]["entry_policy"] == "block_conflicting_new_risk"


def test_ws_bbo_scope_flags_full_universe_hot_path_regression():
    from lightfee.engine.v1_lifecycle_closure import build_v1_lifecycle_closure_table

    table = build_v1_lifecycle_closure_table(
        local_state={
            "runtime_market_data_config": {
                "entry_readiness_provider_effective": "ws_bbo_quote_lease",
                "local_l2_effective_enabled": False,
            },
            "last_scan": {
                "quote_revalidate_candidate_scope": "full_shortlist",
                "quote_revalidate_candidate_count": 50,
                "quote_revalidate_all_target_count": 100,
                "quote_revalidate_target_count": 100,
                "quote_revalidate_skipped_untracked_count": 0,
            },
        },
        exchange_truth=_clean_exchange_truth(),
        generated_at_ms=1770000000000,
    )

    payload = table.to_dict()
    assert payload["performance_scope"]["entry_quote_scope"] == "full_shortlist"
    assert payload["performance_scope"]["full_universe_hot_path_detected"] is True
    assert any(
        row["phase"] == "ENTRY_QUOTE_LEASE"
        and row["diagnostic_severity"] == "critical"
        and row["recovery_policy"] == "diagnostic_regression"
        for row in payload["rows"]
    )


def test_recent_cloud_event_kinds_are_mapped_or_diagnostic_only():
    from lightfee.engine.v1_lifecycle_closure import map_lifecycle_event_kind

    recent_event_kinds = [
        "runtime.booting",
        "runtime.running",
        "runtime.started",
        "runtime.stopped",
        "runtime.shutdown_stage",
        "runtime.live_scan_revalidate_required",
        "runtime.live_scan_recovery_warmup",
        "runtime.order_quote_stale_health_summary",
        "runtime.private_ws_started",
        "runtime.private_ws_stopped",
        "runtime.reconciling",
        "runtime.recovery_block_reconcile_attempt",
        "runtime.recovery_fail_closed",
        "scan.no_entry_diagnostics",
        "startup.order_path_preflight",
        "startup.trading_preflight",
        "runtime.entry_quote_revalidate_targeted",
        "runtime.entry_quote_revalidate_failed",
        "runtime.entry_quote_evidence_resolved_by_ws_bbo",
        "runtime.entry_blocked_gate",
        "runtime.last_good_revalidated_by_entry_quote_truth",
        "runtime.order_quote_stale_skipped",
        "runtime.quote_stale",
        "runtime.perp_liquidity_stale_advisory",
        "runtime.ws_bbo_dynamic_ws_started",
        "runtime.snapshot_fallback_last_good",
        "runtime.candidate_symbol_skipped",
        "runtime.candidates_tradeable",
        "runtime.tradeable_candidates_catalog_filtered",
        "runtime.entry_oi_targeted_refresh_started",
        "runtime.entry_oi_targeted_refresh_resolved",
        "runtime.entry_oi_targeted_refresh_failed",
        "scan.strategy_shortlist_ready",
        "scan.shortlist_ready",
        "execution.entry_liquidity_advisory",
        "execution.entry_liquidity_blocked",
        "execution.entry_leverage_ready",
        "execution.entry_leverage_unavailable",
        "entry.abort_maker_cancel_requested",
        "entry.cleanup_leg_exposure",
        "entry.aborted",
        "entry.opened",
        "runtime.position_opened",
        "runtime.reconciling_complete",
        "exit.passive_close_fallback_terminal_flat",
        "exit.passive_close_hedge_duplicate_client_order_reconciled",
        "exit.passive_close_hedge_confirmed_after_ack",
        "exit.passive_close_terminal_zero_qty_reduce_only_evidence",
        "execution.entry_residual_dust_tolerated",
        "execution.residual_repair_terminal",
        "recovery.flat",
        "runtime.position_lifecycle_terminal",
        "runtime.current_state_heartbeat_loop_export_error",
        "entry.passive_unfilled",
        "execution.direction_drift_blocked",
        "execution.entry_quantity_plan",
        "execution.entry_selected",
        "execution.hedge_deadline_started",
        "execution.passive_small_fill_buffering",
        "execution.passive_small_fill_buffer_expired",
        "execution.passive_cycle_zero_fill",
        "execution.passive_phase_switched",
        "order.passive_submitted",
        "order.reconcile_query",
        "order.reconcile_resolution",
        "order.reconcile_result",
        "order.submit_attempt",
        "order.submit_result",
        "order.submitted",
        "passive_maintenance.cancel_issued",
        "passive_maintenance.cancel_rest_timeout",
        "passive_maintenance.cancel_try_window",
        "passive_maintenance.maker_progress",
        "passive_maintenance.zero_fill_cycle",
        "pending_entry.hedge_submit_attempt",
        "pending_entry.hedge_submit_result",
        "pending_entry.maker_progress_applied",
        "pending_entry.missing_hedge_detected",
        "pending_entry.pending_entry_finalized",
        "pending_entry.removed_by_v1_lifecycle_closure",
        "pending_entry.terminalizer_decision",
        "reconciliation.entry_abandoned_flat",
        "reconciliation.entry_flat_not_found_terminal_cleared",
        "reconciliation.entry_flat_unresolved_maker_retained",
        "entry.opportunity_funnel",
        "review.candidate_shortlisted",
        "runtime.active_position_tick",
        "runtime.close_price_evidence_fallback",
        "runtime.close_price_evidence_rewarm_failed",
        "runtime.close_price_evidence_rest_rewarm_succeeded",
        "runtime.close_price_evidence_stale",
        "runtime.close_price_evidence_ws_bbo_used",
        "runtime.close_price_evidence_ws_rewarm_succeeded",
        "runtime.passive_close_deadline_fallback_armed",
        "runtime.entry_dispatched",
        "runtime.funding_capture_state_updated",
        "runtime.normal_close_routing_passive",
        "runtime.pending_entry_registered",
        "runtime.position_drift_correction_verified",
        "runtime.position_drift_detected",
        "runtime.position_drift_flatten_leg",
        "exit.accepted_order_truth_gap_registered",
        "exit.pending_close_reconciliation_registered",
        "exit.reconciled",
        "runtime.position_drift_corrected",
        "reconciliation.entry_abandon_retained_unresolved_maker",
        "reconciliation.entry_resolved",
        "review.candidate_rejected",
        "runtime.entry_post_only_bbo_repriced",
        "runtime.entry_post_only_reject_cooldown",
        "runtime.maker_event_no_ws_bbo_quote",
        "runtime.position_drift_skipped_passive_close_owner",
        "execution.hedge_deadline_breached",
        "exit.compensated",
        "exit.retry_wait",
        "exit.close_chunk_submitted",
        "exit.close_residual_detected",
        "exit.closed",
        "exit.reconciliation_abandoned",
        "runtime.risk_mode_changed",
        "runtime.stale_fail_closed_cleared",
        "runtime.entry_quote_rewarm_scheduled_after_rest_stale",
        "entry.pending_registered",
        "runtime.entry_owner_claimed",
        "runtime.entry_owner_handoff_complete",
        "runtime.entry_admission_venue_degraded",
        "runtime.entry_admission_venue_recovered",
        "risk.warning_triggered",
        "risk.warning_cleared",
    ]

    unmapped = [kind for kind in recent_event_kinds if map_lifecycle_event_kind(kind) is None]
    assert unmapped == []
    assert map_lifecycle_event_kind("execution.hedge_deadline_breached") == "PASSIVE_CLOSE"
    assert map_lifecycle_event_kind("exit.compensated") == "PASSIVE_CLOSE"
    assert map_lifecycle_event_kind("exit.retry_wait") == "PASSIVE_CLOSE"
    assert map_lifecycle_event_kind("exit.close_chunk_submitted") == "PASSIVE_CLOSE"
    assert map_lifecycle_event_kind("exit.close_residual_detected") == "RESIDUAL_REPAIR"
    assert map_lifecycle_event_kind("exit.closed") == "PASSIVE_CLOSE"
    assert map_lifecycle_event_kind("runtime.risk_mode_changed") == "RECOVERY_TRUTH"
    assert map_lifecycle_event_kind("runtime.recovery_fail_closed") == "RECOVERY_TRUTH"
    assert map_lifecycle_event_kind("runtime.stale_fail_closed_cleared") == "RECOVERY_TRUTH"
    assert (
        map_lifecycle_event_kind("runtime.entry_quote_rewarm_scheduled_after_rest_stale")
        == "ENTRY_QUOTE_LEASE"
    )
    assert map_lifecycle_event_kind("entry.pending_registered") == "PENDING_ENTRY"
    assert map_lifecycle_event_kind("runtime.entry_owner_claimed") == "DIAGNOSTIC_ONLY"
    assert (
        map_lifecycle_event_kind("runtime.entry_owner_handoff_complete")
        == "DIAGNOSTIC_ONLY"
    )
    assert (
        map_lifecycle_event_kind("runtime.entry_admission_venue_degraded")
        == "DIAGNOSTIC_ONLY"
    )
    assert (
        map_lifecycle_event_kind("runtime.entry_admission_venue_recovered")
        == "DIAGNOSTIC_ONLY"
    )
    assert map_lifecycle_event_kind("risk.warning_triggered") == "OPEN_POSITION"
    assert map_lifecycle_event_kind("risk.warning_cleared") == "OPEN_POSITION"


def test_exported_positions_alias_prevents_diagnose_orphan_drift():
    from lightfee.engine.v1_lifecycle_closure import build_v1_lifecycle_closure_table

    table = build_v1_lifecycle_closure_table(
        local_state={
            "lifecycle": "risk_only",
            "risk_mode": "running",
            "open_position_count": 1,
            "positions": [
                {
                    "position_id": "entry-1781286800856-HOMEUSDT",
                    "symbol": "HOMEUSDT",
                    "long_venue": "okx",
                    "short_venue": "bybit",
                    "quantity": 1500.0,
                }
            ],
        },
        exchange_truth={
            "available": True,
            "truth_available": True,
            "confidence": "high",
            "has_nonzero_position": True,
            "has_open_order": False,
            "positions": {
                "okx": {
                    "HOMEUSDT": {
                        "symbol": "HOMEUSDT",
                        "venue": "okx",
                        "quantity": 1500.0,
                        "side": "Side.BUY",
                    }
                },
                "bybit": {
                    "HOMEUSDT": {
                        "symbol": "HOMEUSDT",
                        "venue": "bybit",
                        "quantity": 1500.0,
                        "side": "Side.SELL",
                    }
                },
            },
            "open_orders": {
                "okx": {"HOMEUSDT": []},
                "bybit": {"HOMEUSDT": []},
            },
        },
        generated_at_ms=1781286983860,
    ).to_dict()

    row_keys = {row["row_key"] for row in table["rows"]}
    assert "open_position:entry-1781286800856-HOMEUSDT" in row_keys
    assert not any(":unpaired_live_position:" in key for key in row_keys)
    assert table["summary"]["recovery_block_reason"] is None


def test_unchanged_rows_reuse_previous_closure_decision_id():
    from lightfee.engine.v1_lifecycle_closure import build_v1_lifecycle_closure_table

    previous = build_v1_lifecycle_closure_table(
        local_state={"runtime_progress": {"active_lane": "scan"}},
        exchange_truth=_clean_exchange_truth(),
        generated_at_ms=1770000000000,
    ).to_dict()
    previous["rows"][0]["closure_decision_id"] = "stable-decision-id"

    current = build_v1_lifecycle_closure_table(
        local_state={"runtime_progress": {"active_lane": "scan"}},
        exchange_truth=_clean_exchange_truth(),
        generated_at_ms=1770000001000,
        previous_table=previous,
    ).to_dict()

    assert current["rows"][0]["closure_decision_id"] == "stable-decision-id"


def test_entry_gate_decision_reads_closure_summary_only():
    from lightfee.engine.v1_lifecycle_closure import entry_gate_from_closure

    allowed, reason = entry_gate_from_closure(
        {
            "summary": {
                "entry_allowed": False,
                "entry_block_reason": "",
                "recovery_block_reason": "orphan_maker_order",
            }
        }
    )

    assert allowed is False
    assert reason == "orphan_maker_order"


def test_closure_event_fields_select_owner_row_for_release_events():
    from lightfee.engine.v1_lifecycle_closure import (
        build_v1_lifecycle_closure_table,
        closure_event_fields,
    )

    table = build_v1_lifecycle_closure_table(
        local_state={
            "pending_entries": {
                "entry-1": {
                    "pending_id": "entry-1",
                    "symbol": "BTCUSDT",
                    "maker_leg_filled": 0.0,
                    "hedge_leg_filled": 0.0,
                }
            },
        },
        exchange_truth=_clean_exchange_truth(),
        generated_at_ms=1770000000000,
    ).to_dict()

    fields = closure_event_fields(
        table,
        phase="PENDING_ENTRY",
        owner_id="entry-1",
    )

    assert fields["closure_phase"] == "PENDING_ENTRY"
    assert fields["closure_row_key"] == "pending_entry:entry-1"
    assert fields["closure_decision_id"].startswith("v1lc-")


def test_current_state_export_includes_v1_lifecycle_closure(tmp_path):
    state = EngineState(
        lifecycle=EngineLifecycle.RUNNING,
        risk_mode=GlobalRiskMode.RUNNING,
    )
    path = tmp_path / "current.json"

    _export_current_state_snapshot(state, str(path))

    payload = json.loads(path.read_text())
    closure = payload["v1_lifecycle_closure"]
    assert closure["version"] == "v1.lifecycle_closure.v1"
    assert closure["summary"]["entry_allowed"] is True
    assert closure["rows"]


def test_diagnose_gate_exposes_existing_v1_lifecycle_closure_payload():
    closure = {
        "version": "v1.lifecycle_closure.v1",
        "summary": {"entry_allowed": True},
        "rows": [],
        "unmapped_event_kinds": [],
        "performance_scope": {},
    }
    gate = _build_production_acceptance_gate(
        [],
        {
            "lifecycle": "running",
            "risk_mode": "running",
            "open_position_count": 0,
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "pending_residual_repair_count": 0,
            "v1_lifecycle_closure": closure,
        },
        _clean_exchange_truth(),
    )

    assert gate["v1_lifecycle_closure"] == closure


def test_production_health_exposes_existing_v1_lifecycle_closure_payload():
    closure = {
        "version": "v1.lifecycle_closure.v1",
        "summary": {"entry_allowed": True},
        "rows": [],
        "unmapped_event_kinds": [],
        "performance_scope": {},
    }
    report = analyze_current_state(
        {
            "schema": "lightfee.current_state.v1",
            "generated_at_ms": 1770000000000,
            "lifecycle": "running",
            "risk_mode": "running",
            "last_tick_ms": 1770000000000,
            "last_scan": {"ts_ms": 1770000000000},
            "open_position_count": 0,
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "pending_residual_repair_count": 0,
            "exchange_truth": _clean_exchange_truth(),
            "v1_lifecycle_closure": closure,
        },
        now_ms=1770000001000,
        max_tick_age_ms=10_000,
    )

    assert report.details["v1_lifecycle_closure"] == closure


def test_static_runtime_snapshot_refreshes_closure_before_export():
    source = (REPO_ROOT / "lightfee/engine/runtime.py").read_text()
    start = source.index("def _maybe_export_current_state_snapshot")
    end = source.index("def _entry_quote_lease_max_age_ms")
    body = source[start:end]

    assert "_refresh_runtime_market_data_config_state()" in body
    assert "_refresh_v1_lifecycle_closure_state(now_ms)" in body
    assert body.index("_refresh_runtime_market_data_config_state()") < body.index(
        "_refresh_v1_lifecycle_closure_state(now_ms)"
    ) < body.index("        maybe_export_current_state_snapshot(")


def test_static_entry_recovery_gate_reads_v1_lifecycle_closure():
    source = (REPO_ROOT / "lightfee/engine/entry_gate_runtime.py").read_text()
    start = source.index("def _gate_recovery_ledger")
    end = source.index("def _gate_entry_sizing", start)
    body = source[start:end]

    assert "_v1_lifecycle_entry_gate_decision()" in body
    assert "recovery_decision.entry_allowed" not in body
    assert "ledger.allows_new_entry" not in body


def test_static_release_paths_attach_closure_decision_ids():
    pending_entry = (REPO_ROOT / "lightfee/engine/pending_entry_runtime.py").read_text()
    residual_repair = (
        REPO_ROOT / "lightfee/engine/residual_repair_runtime.py"
    ).read_text()
    passive_close = (REPO_ROOT / "lightfee/engine/passive_close.py").read_text()

    pending_start = pending_entry.index("def _remove_pending_entry_after_terminal_decision")
    pending_end = pending_entry.index("async def _complete_pending_entry_terminal_removal")
    pending_body = pending_entry[pending_start:pending_end]
    residual_start = residual_repair.index("def _terminalize_residual_repair_task")
    residual_end = residual_repair.index("def _reschedule_pending_residual_repair_task")
    residual_body = residual_repair[residual_start:residual_end]
    passive_start = passive_close.index("def _emit_passive_close_terminal_resolution")
    passive_end = passive_close.index("def _clear_live_flat_state")
    passive_body = passive_close[passive_start:passive_end]

    assert "closure_decision_id" in pending_body
    assert "closure_decision_id" in residual_body
    assert "closure_decision_id" in passive_body


def test_static_ops_boundaries_consume_lifecycle_closure_payload():
    loop_control = (REPO_ROOT / "lightfee/engine/loop_control.py").read_text()
    diagnose = (REPO_ROOT / "scripts/diagnose_live.py").read_text()
    production_health = (
        REPO_ROOT / "lightfee/ops/production_health.py"
    ).read_text()

    assert '"v1_lifecycle_closure": v1_lifecycle_closure' in loop_control
    assert "_v1_lifecycle_closure_payload(" in diagnose
    assert '"v1_lifecycle_closure": v1_lifecycle_closure' in diagnose
    assert "_v1_lifecycle_closure_payload(" in production_health
    assert '"v1_lifecycle_closure": v1_lifecycle_closure' in production_health
