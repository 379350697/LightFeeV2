"""State fidelity verification: prove remaining V1 state semantics are complete.

Validates that V2 state/recovery/replay preserves all active V1 semantics
and identifies any proven-missing fields or behaviors.

This is a verification-first approach per the gap-triage spec:
- Prove what's already sufficient
- Only flag real losses that need code changes
- Do not start a broad state rewrite
"""

from __future__ import annotations

import time
import pytest
from lightfee.engine.state import (
    EngineState,
    OpenPosition,
    PendingEntry,
    PendingClose,
    PendingPassiveClose,
    PendingPassiveLegFill,
    PersistedCloseExecutionLeg,
    PassiveOrderManagerRuntime,
    PassivePhaseState,
    PassiveExecutionPhase,
    ActiveMakerLeg,
    OperatorControlState,
)
from lightfee.engine.recovery import (
    build_recovery_snapshot,
    build_persistent_state_view,
    build_recovery_dedup_index,
    classify_startup_recovery_state,
    needs_reconciliation,
    is_safe_to_resume,
    has_lifecycle_blocking_work,
    normalize_engine_state,
    recover_from_snapshot,
    _restore_state_from_snapshot_dict,
)
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from lightfee.core.domain import OrderFill, Venue, Side


# ---------------------------------------------------------------------------
# Field completeness: verify no V1 business fields are missing
# ---------------------------------------------------------------------------

class TestFieldCompletenessVerification:
    """Prove current V2 state fields are sufficient for V1 semantics."""

    def test_openposition_has_all_v1_semantic_fields(self):
        """OpenPosition preserves all V1-required semantic fields."""
        import dataclasses
        v2_fields = {f.name for f in dataclasses.fields(OpenPosition)}

        # These are the fields required by V1 business semantics
        required = {
            "position_id", "symbol", "long_venue", "short_venue",
            "long_quantity", "short_quantity", "matched_quantity",
            "long_entry_price", "short_entry_price", "opened_at_ms",
            "review_id", "opportunity_origin_tags", "opportunity_hint_source",
            "long_entry_fee_quote", "short_entry_fee_quote",
            "realized_price_pnl_quote", "realized_exit_fee_quote",
            "risk_delever_realized_price_pnl_quote", "risk_delever_realized_exit_fee_quote",
            "protection_realized_price_pnl_quote", "protection_realized_exit_fee_quote",
            "captured_funding_quote", "funding_captured",
            "funding_edge_bps_entry", "total_funding_edge_bps_entry", "expected_edge_bps_entry",
            "peak_net_quote", "current_net_quote",
            "settlement_half_closed_quantity", "settlement_half_closed_at_ms",
            "last_risk_action_at_ms", "risk_delever_step_count",
            "last_risk_reason", "single_side_protection_triggered",
            "funding_timestamp_ms", "exit_after_first_stage",
            "opportunity_type", "second_stage_enabled_at_entry",
            "second_funding_timestamp_ms", "second_stage_funding_captured",
            "second_stage_funding_quote",
            "transfer_state_at_entry", "entry_liquidity_source_at_entry",
            "long_entry_vwap", "short_entry_vwap",
            "entry_capacity_constrained",
            "advisories", "blocked_reasons",
            "entry_quality_markout_5s_emitted", "entry_quality_markout_30s_emitted",
            "exit_reason",
        }
        missing = required - v2_fields
        assert not missing, f"V1 business fields missing from OpenPosition: {missing}"

    def test_enginestate_has_all_v1_semantic_fields(self):
        """EngineState preserves all V1 lifecycle and control fields."""
        import dataclasses
        v2_fields = {f.name for f in dataclasses.fields(EngineState)}

        required = {
            "lifecycle", "risk_mode", "operator",
            "open_positions", "pending_entries", "pending_closes",
            "pending_passive_closes", "run_id",
            "venue_health",
            "recovery_blocked_reason", "recovery_blocked_at_ms",
            "global_risk_reason",
            "pending_residual_repairs", "live_recovery_reduce_only_pairs",
            "venue_entry_cooldowns", "venue_market_data_degradations",
            "transfer_truth", "entry_liquidity_qualification_records",
            "pending_close_reconciliations",
            "retained_local_l2_books", "local_l2_books_snapshot",
            "local_l2_session_snapshot",
        }
        missing = required - v2_fields
        assert not missing, f"V1 business fields missing from EngineState: {missing}"

    def test_pendingentry_has_all_v1_semantic_fields(self):
        """PendingEntry preserves all V1 metadata and recovery fields."""
        import dataclasses
        v2_fields = {f.name for f in dataclasses.fields(PendingEntry)}

        required = {
            "pending_id", "symbol", "long_venue", "short_venue",
            "target_quantity", "long_side", "short_side", "created_at_ms",
            "metadata",
            "maker_order_id", "hedge_order_id",
            "maker_client_order_id", "hedge_client_order_id",
            "maker_leg_filled", "hedge_leg_filled",
            "deadline_ms", "fallback_route",
            "uncertain_outcome",
            "reconcile_attempt", "reconcile_next_attempt_ms",
            "entry_type", "maker_price",
            "long_quantity", "short_quantity",
            "run_id", "entry_route", "outcome",
        }
        missing = required - v2_fields
        assert not missing, f"V1 business fields missing from PendingEntry: {missing}"


# ---------------------------------------------------------------------------
# Round-trip: prove state survives serialization/deserialization
# ---------------------------------------------------------------------------

class TestStateRoundTrip:
    """Prove state survives full round-trip through persistent state view."""

    def _make_populated_state(self) -> EngineState:
        state = EngineState(
            lifecycle=EngineLifecycle.RUNNING,
            risk_mode=GlobalRiskMode.RUNNING,
            run_id="test-run-001",
            started_at_ms=10_000_000,
            last_tick_ms=10_010_000,
            tick_count=100,
            global_risk_reason=None,
            recovery_blocked_reason=None,
        )
        state.operator = OperatorControlState(
            requested_mode=None,
            pending_reconcile=False,
        )
        pos = OpenPosition(
            position_id="pos-1",
            symbol="ETH-USDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=2.0,
            short_quantity=2.0,
            long_entry_price=3000.0,
            short_entry_price=3005.0,
            opened_at_ms=10_000_100,
            review_id="review-abc",
            opportunity_origin_tags=["tag-a", "tag-b"],
            opportunity_hint_source="sidecar",
            matched_quantity=2.0,
            captured_funding_quote=0.5,
            funding_captured=True,
            peak_net_quote=15.0,
            current_net_quote=10.0,
            realized_price_pnl_quote=5.0,
            realized_exit_fee_quote=0.5,
            risk_delever_realized_price_pnl_quote=1.0,
            protection_realized_price_pnl_quote=2.0,
            long_entry_fee_quote=1.0,
            short_entry_fee_quote=1.0,
            funding_edge_bps_entry=3.0,
            total_funding_edge_bps_entry=2.5,
            expected_edge_bps_entry=2.0,
            transfer_state_at_entry="normal",
            entry_liquidity_source_at_entry="sidecar",
            long_entry_vwap=2999.0,
            short_entry_vwap=3004.0,
            entry_capacity_constrained=False,
            advisories=["low-liquidity"],
            blocked_reasons=[],
            entry_quality_markout_5s_emitted=True,
            entry_quality_markout_30s_emitted=False,
            settlement_half_closed_quantity=0.5,
            settlement_half_closed_at_ms=10_005_000,
            exit_reason=None,
            risk_delever_step_count=0,
            last_risk_reason=None,
            single_side_protection_triggered=False,
            funding_timestamp_ms=10_000_000,
            opportunity_type="aligned",
            second_stage_enabled_at_entry=True,
        )
        state.open_positions["pos-1"] = pos

        pe = PendingEntry(
            pending_id="pe-1",
            symbol="ETH-USDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            target_quantity=2.0,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=10_000_050,
            metadata={"score": 0.95},
            maker_order_id="maker-1",
            hedge_order_id="hedge-1",
            maker_client_order_id="client-maker-1",
            hedge_client_order_id="client-hedge-1",
            run_id="test-run-001",
            entry_route="standard",
            outcome="",
        )
        state.pending_entries["pe-1"] = pe

        return state

    def test_persistent_view_includes_all_fields(self):
        state = self._make_populated_state()
        view = build_persistent_state_view(state)

        # Top-level fields
        assert view["lifecycle"] == "running"
        assert view["risk_mode"] == "running"
        assert view["run_id"] == "test-run-001"
        assert view["open_position_count"] == 1
        assert view["pending_entry_count"] == 1

        # Open position fields in serialized dict
        positions = view["open_positions"]
        assert "pos-1" in positions
        p = positions["pos-1"]
        assert p["position_id"] == "pos-1"
        assert p["symbol"] == "ETH-USDT"
        assert p["review_id"] == "review-abc"
        assert p["opportunity_origin_tags"] == ["tag-a", "tag-b"]
        assert p["transfer_state_at_entry"] == "normal"
        assert p["entry_liquidity_source_at_entry"] == "sidecar"
        assert p["long_entry_vwap"] == 2999.0
        assert p["short_entry_vwap"] == 3004.0
        assert p["entry_capacity_constrained"] is False
        assert p["advisories"] == ["low-liquidity"]
        assert p["entry_quality_markout_5s_emitted"] is True
        assert p["risk_delever_realized_price_pnl_quote"] == 1.0
        assert p["protection_realized_price_pnl_quote"] == 2.0
        assert p["settlement_half_closed_quantity"] == 0.5
        assert p["funding_edge_bps_entry"] == 3.0

        # Pending entry
        entries = view["pending_entries"]
        assert "pe-1" in entries
        e = entries["pe-1"]
        assert e["symbol"] == "ETH-USDT"
        assert e["target_quantity"] == 2.0
        assert e["uncertain_outcome"] is False
        assert e["entry_type"] == ""
        assert e["maker_price"] == 0.0

    def test_deserialize_preserves_semantic_fields(self):
        state = self._make_populated_state()
        view = build_persistent_state_view(state)

        # Deserialize and verify
        restored = _restore_state_from_snapshot_dict(view)
        assert restored.run_id == "test-run-001"
        assert restored.lifecycle == EngineLifecycle.RUNNING

        pos = restored.open_positions.get("pos-1")
        assert pos is not None
        assert pos.review_id == "review-abc"
        assert pos.opportunity_origin_tags == ["tag-a", "tag-b"]
        assert pos.opportunity_hint_source == "sidecar"
        assert pos.transfer_state_at_entry == "normal"
        assert pos.entry_liquidity_source_at_entry == "sidecar"
        assert pos.long_entry_vwap == 2999.0
        assert pos.advisories == ["low-liquidity"]
        assert pos.entry_quality_markout_5s_emitted is True
        assert pos.risk_delever_realized_price_pnl_quote == 1.0
        assert pos.protection_realized_price_pnl_quote == 2.0
        assert pos.settlement_half_closed_quantity == 0.5
        assert pos.funding_edge_bps_entry == 3.0

        pe = restored.pending_entries.get("pe-1")
        assert pe is not None
        assert pe.symbol == "ETH-USDT"
        assert pe.run_id == "test-run-001"

    def test_passive_close_order_evidence_roundtrips_for_live_flat_reconciliation(self):
        """Restart retains every known close order, not only the last aggregate fill."""
        state = self._make_populated_state()
        position = state.open_positions["pos-1"]
        state.pending_passive_closes[position.position_id] = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            short_stage="exit_short",
            long_stage="exit_long",
            target_quantity=2.0,
            chunk_quantities=[1.0, 1.0],
            active_chunk_index=1,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_submit_attempt=3,
                maker_submit_consecutive_failures=2,
                missing_l2_tick_consecutive_count=2,
            ),
            maker_fill=PendingPassiveLegFill(
                quantity=1.0,
                order_id="maker-last-order",
                client_order_id="maker-last-client",
            ),
            hedge_fill=PendingPassiveLegFill(
                quantity=1.0,
                order_id="hedge-last-order",
                client_order_id="hedge-last-client",
            ),
            long_legs=[
                PersistedCloseExecutionLeg(
                    fill=OrderFill(
                        venue=Venue.BINANCE,
                        symbol=position.symbol,
                        side=Side.SELL,
                        quantity=1.0,
                        price=3001.0,
                        order_id="long-close-order-1",
                        client_order_id="long-close-client-1",
                        fee_quote=0.1,
                        filled_at_ms=101,
                    ),
                    client_order_id="long-close-client-1",
                    submit_started_at_ms=100,
                    latency_ms=5,
                )
            ],
            short_legs=[
                PersistedCloseExecutionLeg(
                    fill=OrderFill(
                        venue=Venue.OKX,
                        symbol=position.symbol,
                        side=Side.BUY,
                        quantity=1.0,
                        price=3002.0,
                        order_id="short-close-order-1",
                        client_order_id="short-close-client-1",
                        fee_quote=0.2,
                        filled_at_ms=102,
                    ),
                    client_order_id="short-close-client-1",
                    submit_started_at_ms=101,
                    latency_ms=6,
                )
            ],
            passive_manager_runtimes={
                "binance": PassiveOrderManagerRuntime(consecutive_failures=2)
            },
            small_fill_min_notional_attempts=1,
            last_small_fill_missing_quantity=0.25,
            small_fill_buffer_started_at_ms=99,
            ops_count_this_window=4,
            ops_window_started_at_ms=98,
        )

        view = build_persistent_state_view(state)
        restored = _restore_state_from_snapshot_dict(view)
        pending = restored.pending_passive_closes[position.position_id]

        assert pending.position_snapshot is restored.open_positions[position.position_id]
        assert pending.phase_state.maker_submit_attempt == 3
        assert pending.phase_state.maker_submit_consecutive_failures == 2
        assert pending.phase_state.missing_l2_tick_consecutive_count == 2
        assert pending.long_legs[0].fill is not None
        assert pending.long_legs[0].fill.order_id == "long-close-order-1"
        assert pending.long_legs[0].client_order_id == "long-close-client-1"
        assert pending.short_legs[0].fill is not None
        assert pending.short_legs[0].fill.order_id == "short-close-order-1"
        assert pending.passive_manager_runtimes["binance"].consecutive_failures == 2
        assert pending.small_fill_min_notional_attempts == 1
        assert pending.last_small_fill_missing_quantity == 0.25
        assert pending.small_fill_buffer_started_at_ms == 99
        assert pending.ops_count_this_window == 4
        assert pending.ops_window_started_at_ms == 98

        # V1 persists the passive-close-owned position snapshot independently.
        # It remains recoverable even if the primary open-position owner is
        # absent from an inconsistent pre-recovery snapshot.
        view["open_positions"] = {}
        restored_without_primary_owner = _restore_state_from_snapshot_dict(view)
        orphan_pending = restored_without_primary_owner.pending_passive_closes[
            position.position_id
        ]
        assert orphan_pending.position_snapshot is not None
        assert orphan_pending.position_snapshot.position_id == position.position_id
        assert orphan_pending.position_snapshot.symbol == position.symbol


# ---------------------------------------------------------------------------
# Recovery semantics
# ---------------------------------------------------------------------------

class TestRecoverySemantics:
    """Prove recovery logic preserves V1 semantics."""

    def test_build_recovery_snapshot_clean(self):
        state = EngineState()
        snap = build_recovery_snapshot(state)
        assert snap.has_open_positions is False
        assert snap.has_pending_entries is False
        assert snap.has_pending_closes is False
        assert snap.ambiguous_state is False

    def test_build_recovery_snapshot_with_positions(self):
        state = EngineState(lifecycle=EngineLifecycle.BOOTING)
        state.open_positions["p1"] = OpenPosition(
            position_id="p1", symbol="BTC-USDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=1.0, short_quantity=1.0,
            long_entry_price=50000.0, short_entry_price=50000.0,
            opened_at_ms=1000,
        )
        snap = build_recovery_snapshot(state)
        assert snap.has_open_positions is True
        assert snap.ambiguous_state is True  # BOOTING + open positions

    def test_classify_clean_state(self):
        state = EngineState(lifecycle=EngineLifecycle.RUNNING)
        assert classify_startup_recovery_state(state) == "clean"

    def test_classify_recovery_needed(self):
        state = EngineState(lifecycle=EngineLifecycle.BOOTING)
        state.open_positions["p1"] = OpenPosition(
            position_id="p1", symbol="BTC-USDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=1.0, short_quantity=1.0,
            long_entry_price=50000.0, short_entry_price=50000.0,
            opened_at_ms=1000,
        )
        assert classify_startup_recovery_state(state) == "recovery_needed"

    def test_needs_reconciliation_with_positions(self):
        state = EngineState()
        state.open_positions["p1"] = OpenPosition(
            position_id="p1", symbol="BTC-USDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=1.0, short_quantity=1.0,
            long_entry_price=50000.0, short_entry_price=50000.0,
            opened_at_ms=1000,
        )
        assert needs_reconciliation(state) is True

    def test_needs_reconciliation_clean(self):
        state = EngineState()
        assert needs_reconciliation(state) is False

    def test_is_safe_to_resume_clean(self):
        state = EngineState(lifecycle=EngineLifecycle.RUNNING)
        assert is_safe_to_resume(state) is True

    def test_is_safe_to_resume_fail_closed(self):
        state = EngineState(lifecycle=EngineLifecycle.RISK_ONLY)
        state.risk_mode = GlobalRiskMode.FAIL_CLOSED
        assert is_safe_to_resume(state) is False

    def test_has_lifecycle_blocking_work(self):
        state = EngineState()
        assert has_lifecycle_blocking_work(state) is False

        state.open_positions["p1"] = OpenPosition(
            position_id="p1", symbol="BTC-USDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=1.0, short_quantity=1.0,
            long_entry_price=50000.0, short_entry_price=50000.0,
            opened_at_ms=1000,
        )
        assert has_lifecycle_blocking_work(state) is True

    def test_normalize_removes_dust(self):
        state = EngineState()
        state.open_positions["dust1"] = OpenPosition(
            position_id="dust1", symbol="BTC-USDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=0.0, short_quantity=0.0,
            long_entry_price=50000.0, short_entry_price=50000.0,
            opened_at_ms=1000,
        )
        state.open_positions["valid"] = OpenPosition(
            position_id="valid", symbol="ETH-USDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=2.0, short_quantity=2.0,
            long_entry_price=3000.0, short_entry_price=3000.0,
            opened_at_ms=1000,
        )
        normalize_engine_state(state)
        assert "dust1" not in state.open_positions
        assert "valid" in state.open_positions

    def test_dedup_index_from_pending_entries(self):
        state = EngineState()
        pe = PendingEntry(
            pending_id="pe-1", symbol="BTC-USDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            target_quantity=1.0, long_side=Side.BUY, short_side=Side.SELL,
            created_at_ms=1000,
            maker_client_order_id="client-m-1",
            hedge_client_order_id="client-h-1",
        )
        state.pending_entries["pe-1"] = pe

        index = build_recovery_dedup_index(state)
        assert index["client-m-1"] == "pe-1"
        assert index["client-h-1"] == "pe-1"


# ---------------------------------------------------------------------------
# Paper outcome state: verify tracker state is self-contained
# ---------------------------------------------------------------------------

class TestPaperOutcomeStateIntegration:
    """Paper outcome tracker state is self-contained (doesn't need EngineState fields)."""

    def test_paper_outcome_events_flow_through_journal(self):
        """Paper outcome events are journal events, not persistent state fields."""
        from lightfee.offline.paper_outcome import (
            PaperOutcomeConfig,
            PaperOutcomeTracker,
            PaperOpportunityRegistration,
        )

        config = PaperOutcomeConfig(
            tracking_enabled=True,
            finalist_limit=2,
            markout_secs=[300],
            settlement_grace_secs=0,
        )
        tracker = PaperOutcomeTracker(config)
        reg = PaperOpportunityRegistration(
            paper_id="p1", review_id=None, symbol="BTC-USDT",
            pair_id="binance:BTC-USDT->okx:BTC-USDT",
            long_venue="binance", short_venue="okx",
            finalist_rank=0, selected_real_trade=False,
            not_selected_reason=None, registered_at_ms=1000,
            target_settlement_ts_ms=None, markout_secs=[300],
            entry_notional_quote=1000.0,
            fee_quote=1.0,
            expected_funding_quote=5.0,
            entry_slippage_quote=0.5,
        )
        tracker.register(reg)

        snaps = {"binance:BTC-USDT": {"mid": 1.01}, "okx:BTC-USDT": {"mid": 1.02}}
        events = tracker.evaluate_due(301_000, snaps)
        assert len(events) == 1
        assert events[0]["kind"] == "opportunity.paper_markout"

        # These events are designed to be written to the journal and analyzed offline.
        # The paper tracker state is not part of EngineState — it's a live-only structure.
        # This is consistent with V1 where paper_outcome_tracker is part of
        # the engine struct but its state surfaces only through journal events.
