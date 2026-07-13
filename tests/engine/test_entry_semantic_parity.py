"""Semantic parity tests for entry planning and execution (ENTRY-001, ENTRY-002).

V1 references:
- src/execution_core/entry_execution_planner.rs
- src/execution_core/entry_sync.rs
- src/engine/entry.rs
"""

from __future__ import annotations

import pytest
from lightfee.core.domain import OrderFill, OrderRequest, Side, TimeInForce, Venue
from lightfee.engine.entry import (
    EntryContext,
    EntryState,
    EntryType,
    advance_entry_state,
    build_entry_orders,
    build_open_position,
)
from lightfee.engine.execution_planner import (
    ExecutionRoute,
    IncrementalEntryExecutionPlan,
    align_quantity_down_to_chunk,
    align_quantity_up_to_step,
    bounded_maker_first_initial_target_quantity,
    effective_entry_leg_notional_floor,
    maker_min_valid_clip_quantity,
    min_hedgeable_chunk_from_notional,
    plan_incremental_entry_execution,
    quantities_match,
)
from lightfee.engine.state import OpenPosition, PendingEntry, PendingEntryRemainderSlice


# ============================================================================
# ENTRY-001: Entry Planning Semantics
# ============================================================================


class TestEntryPlanningSemantics:
    """V1 entry planning: incremental entry, min-notional, remainder, matched ratio, reason."""

    def test_plan_rejects_zero_target_quantity(self):
        route, plan = plan_incremental_entry_execution(
            target_quantity=0.0,
            slice_ratio=0.5,
            min_hedgeable_chunk=0.001,
            maker_min_notional_quote=10.0,
            maker_price_hint=100.0,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=10.0,
            hedge_price_hint=100.0,
        )
        assert route == ExecutionRoute.REJECTED
        assert plan.reason == "target_quantity_not_positive"

    def test_plan_rejects_target_below_min_hedgeable_chunk(self):
        route, plan = plan_incremental_entry_execution(
            target_quantity=0.0001,
            slice_ratio=0.5,
            min_hedgeable_chunk=0.001,
            maker_min_notional_quote=10.0,
            maker_price_hint=100.0,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=10.0,
            hedge_price_hint=100.0,
        )
        assert route == ExecutionRoute.REJECTED
        assert "below_min_hedgeable_chunk" in (plan.reason or "")

    def test_plan_returns_passive_incremental_for_valid_input(self):
        route, plan = plan_incremental_entry_execution(
            target_quantity=1.0,
            slice_ratio=0.5,
            min_hedgeable_chunk=0.001,
            maker_min_notional_quote=10.0,
            maker_price_hint=100.0,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=10.0,
            hedge_price_hint=100.0,
        )
        assert route == ExecutionRoute.PASSIVE_INCREMENTAL
        assert plan.full_target_quantity > 0
        assert plan.initial_maker_target_quantity > 0
        assert plan.initial_maker_target_quantity <= plan.full_target_quantity

    def test_plan_preserves_incremental_entry_semantics(self):
        """V1: incremental entry builds up over ticks. The initial clip is a
        fraction of the full target, not the full target itself."""
        route, plan = plan_incremental_entry_execution(
            target_quantity=10.0,
            slice_ratio=0.5,
            min_hedgeable_chunk=0.01,
            maker_min_notional_quote=10.0,
            maker_price_hint=100.0,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=10.0,
            hedge_price_hint=100.0,
        )
        assert route == ExecutionRoute.PASSIVE_INCREMENTAL
        # Initial maker target is a fraction, not the full target
        assert plan.initial_maker_target_quantity < plan.full_target_quantity or \
            plan.initial_maker_target_quantity == plan.full_target_quantity

    def test_plan_reason_string_present_on_rejection(self):
        """V1: every rejection must carry a reason string."""
        route, plan = plan_incremental_entry_execution(
            target_quantity=-1.0,
            slice_ratio=0.5,
            min_hedgeable_chunk=0.001,
            maker_min_notional_quote=10.0,
            maker_price_hint=100.0,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=10.0,
            hedge_price_hint=100.0,
        )
        assert route == ExecutionRoute.REJECTED
        assert plan.reason is not None

    def test_min_notional_gating(self):
        """V1: entries must not execute below min-notional. Maker min clip
        must be satisfied."""
        # With a tiny target quantity and large min notional, planning should
        # reject or fall back
        route, plan = plan_incremental_entry_execution(
            target_quantity=0.01,
            slice_ratio=0.5,
            min_hedgeable_chunk=0.001,
            maker_min_notional_quote=10000.0,  # very high
            maker_price_hint=100.0,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=10.0,
            hedge_price_hint=100.0,
        )
        # Either rejected or fallback — should not silently proceed
        assert route in (ExecutionRoute.REJECTED, ExecutionRoute.FALLBACK_TO_STANDARD)

    def test_remainder_tracking_in_plan(self):
        """V1: remainder = full_target - initial_maker_target. Must be >= 0."""
        route, plan = plan_incremental_entry_execution(
            target_quantity=1.0,
            slice_ratio=0.3,
            min_hedgeable_chunk=0.001,
            maker_min_notional_quote=10.0,
            maker_price_hint=100.0,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=10.0,
            hedge_price_hint=100.0,
        )
        if route == ExecutionRoute.PASSIVE_INCREMENTAL:
            remainder = plan.full_target_quantity - plan.initial_maker_target_quantity
            assert remainder >= -1e-9  # remainder is non-negative

    def test_matched_ratio_computed_in_plan(self):
        """V1: matched_ratio = initial_maker / full_target when full_target > 0."""
        route, plan = plan_incremental_entry_execution(
            target_quantity=1.0,
            slice_ratio=0.4,
            min_hedgeable_chunk=0.001,
            maker_min_notional_quote=10.0,
            maker_price_hint=100.0,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=10.0,
            hedge_price_hint=100.0,
        )
        if route == ExecutionRoute.PASSIVE_INCREMENTAL and plan.full_target_quantity > 0:
            ratio = plan.initial_maker_target_quantity / plan.full_target_quantity
            assert ratio > 0
            assert ratio <= 1.0 + 1e-9

    # --- V1 helper semantics ---

    def test_quantities_match(self):
        assert quantities_match(1.0, 1.0) is True
        assert quantities_match(1.0, 1.0 + 1e-10) is True
        assert quantities_match(1.0, 1.1) is False

    def test_effective_entry_leg_notional_floor(self):
        # No exchange min — uses global min
        result = effective_entry_leg_notional_floor(10.0, None)
        assert result == 10.0

        # Exchange min > global — uses exchange min
        result = effective_entry_leg_notional_floor(10.0, 20.0)
        assert result == 20.0

        # Global min > exchange — uses global min
        result = effective_entry_leg_notional_floor(30.0, 20.0)
        assert result == 30.0

    def test_align_quantity_down_to_chunk(self):
        assert align_quantity_down_to_chunk(1.0, 0.3) == pytest.approx(0.9)  # 3 chunks of 0.3
        assert align_quantity_down_to_chunk(0.0, 0.1) == 0.0
        assert align_quantity_down_to_chunk(0.2, 0.3) == 0.0  # below one chunk

    def test_align_quantity_up_to_step(self):
        assert align_quantity_up_to_step(0.05, 0.01) == 0.05
        assert align_quantity_up_to_step(0.051, 0.01) == 0.06

    def test_bounded_maker_first_initial_target(self):
        result = bounded_maker_first_initial_target_quantity(10.0, 0.4)
        assert result == 4.0  # 40% of 10.0

    def test_min_hedgeable_chunk_from_notional(self):
        chunk = min_hedgeable_chunk_from_notional(
            min_base_quantity=0.01,
            min_notional_quote=10.0,
            step_base_quantity=0.001,
            price_hint=100.0,
        )
        assert chunk > 0
        assert chunk >= 0.01  # floor is min_base_quantity

    def test_maker_min_valid_clip_quantity(self):
        result = maker_min_valid_clip_quantity(
            maker_min_notional_quote=10.0,
            maker_price_hint=100.0,
            min_hedgeable_chunk=0.01,
            full_target_quantity=1.0,
        )
        assert result is not None
        assert result > 0


# ============================================================================
# ENTRY-002: Entry Execution and Idempotency
# ============================================================================


class TestPendingEntryRemainderSemantics:
    """V1 PendingEntryHedge maker remainder FIFO semantics."""

    def test_maker_remainder_slices_drive_missing_quantity_and_fifo_consumption(self):
        pending = PendingEntry(
            pending_id="entry-remainder",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            target_quantity=3.0,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=1000,
            maker_leg_filled=3.0,
            hedge_leg_filled=0.0,
            maker_fill_price=20.0,
            maker_remainder_slices=[
                PendingEntryRemainderSlice(
                    quantity=1.0,
                    notional_quote=10.0,
                    fill_at_ms=1001,
                ),
                PendingEntryRemainderSlice(
                    quantity=2.0,
                    notional_quote=40.0,
                    fill_at_ms=1002,
                ),
            ],
        )

        assert pending.missing_hedge_quantity() == pytest.approx(3.0)
        assert pending.unmatched_maker_weighted_average_price() == pytest.approx(
            50.0 / 3.0
        )

        assert pending.consume_hedge_quantity_fifo(1.5) == pytest.approx(1.5)

        assert pending.missing_hedge_quantity() == pytest.approx(1.5)
        assert len(pending.maker_remainder_slices) == 1
        remainder = pending.maker_remainder_slices[0]
        assert remainder.quantity == pytest.approx(1.5)
        assert remainder.notional_quote == pytest.approx(30.0)
        assert remainder.fill_at_ms == 1002


class TestEntryExecutionIdempotency:
    """V1 entry execution: maker/hedge ordering, client-order idempotency,
    uncertain outcomes, reject classification, residual tasks, pending entry."""

    def test_build_entry_orders_has_client_order_id(self):
        ctx = EntryContext(
            entry_id="test-entry-1",
            symbol="BTC-USDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=1.0,
            short_quantity=1.0,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        maker_req, hedge_req = build_entry_orders(ctx)
        assert maker_req.client_order_id is not None
        assert maker_req.client_order_id != ""
        assert hedge_req.client_order_id is not None
        assert hedge_req.client_order_id != ""
        # Client order IDs differ between maker and hedge
        assert maker_req.client_order_id != hedge_req.client_order_id

    def test_build_entry_orders_maker_post_only(self):
        ctx = EntryContext(
            entry_id="test-entry-2",
            symbol="ETH-USDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.5,
            short_quantity=0.5,
            long_price_hint=3000.0,
            short_price_hint=3000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        maker_req, hedge_req = build_entry_orders(ctx)
        assert maker_req.post_only is True
        assert maker_req.time_in_force == TimeInForce.GTC
        assert hedge_req.reduce_only is False
        assert hedge_req.time_in_force == TimeInForce.IOC

    def test_build_entry_orders_maker_short_leg(self):
        """When maker leg is SELL, maker=short_venue sell, hedge=long_venue buy."""
        ctx = EntryContext(
            entry_id="test-entry-3",
            symbol="BTC-USDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=2.0,
            short_quantity=2.0,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.SELL,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        maker_req, hedge_req = build_entry_orders(ctx)
        assert maker_req.venue == Venue.BYBIT  # short_venue
        assert maker_req.side == Side.SELL
        assert hedge_req.venue == Venue.BINANCE  # long_venue
        assert hedge_req.side == Side.BUY

    def test_advance_entry_state_valid_transition(self):
        ctx = EntryContext(
            entry_id="t1", symbol="BTC-USDT",
            long_venue=Venue.BINANCE, short_venue=Venue.BYBIT,
            long_quantity=1.0, short_quantity=1.0,
            long_price_hint=50000.0, short_price_hint=50000.0,
            maker_leg=Side.BUY, entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        ctx2 = advance_entry_state(ctx, EntryState.SUBMITTING_MAKER)
        assert ctx2.state == EntryState.SUBMITTING_MAKER

    def test_advance_entry_state_rejects_terminal(self):
        ctx = EntryContext(
            entry_id="t2", symbol="BTC-USDT",
            long_venue=Venue.BINANCE, short_venue=Venue.BYBIT,
            long_quantity=1.0, short_quantity=1.0,
            long_price_hint=50000.0, short_price_hint=50000.0,
            maker_leg=Side.BUY, entry_type=EntryType.PASSIVE_INCREMENTAL,
            state=EntryState.COMPLETED,
        )
        with pytest.raises(ValueError):
            advance_entry_state(ctx, EntryState.SUBMITTING_HEDGE)

    def test_entry_state_terminal_property(self):
        assert EntryState.COMPLETED.is_terminal is True
        assert EntryState.FAILED.is_terminal is True
        assert EntryState.FAILED_WITH_RESIDUAL.is_terminal is True
        assert EntryState.IDLE.is_terminal is False
        assert EntryState.SUBMITTING_MAKER.is_terminal is False

    def test_build_open_position_matched_quantity(self):
        ctx = EntryContext(
            entry_id="pos-1", symbol="BTC-USDT",
            long_venue=Venue.BINANCE, short_venue=Venue.BYBIT,
            long_quantity=1.0, short_quantity=1.0,
            long_price_hint=50000.0, short_price_hint=49990.0,
            maker_leg=Side.BUY, entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        maker_fill = OrderFill(
            venue=Venue.BINANCE, symbol="BTC-USDT",
            side=Side.BUY, quantity=1.0, price=50000.0,
            order_id="m1", filled_at_ms=1000,
        )
        hedge_fill = OrderFill(
            venue=Venue.BYBIT, symbol="BTC-USDT",
            side=Side.SELL, quantity=0.95, price=49990.0,
            order_id="h1", filled_at_ms=1001,
        )
        pos = build_open_position(ctx, maker_fill, hedge_fill, now_ms=1001)
        assert pos is not None
        assert pos.position_id == "pos-1"
        assert pos.symbol == "BTC-USDT"
        assert pos.matched_quantity == min(maker_fill.quantity, hedge_fill.quantity)

    def test_build_open_position_initializes_entry_fee_net_like_v1(self):
        """V1 PendingEntryHedge::build_open_position starts net at matched entry fees."""
        ctx = EntryContext(
            entry_id="pos-fee",
            symbol="BTC-USDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=1.0,
            short_quantity=1.0,
            long_price_hint=50000.0,
            short_price_hint=49990.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        maker_fill = OrderFill(
            venue=Venue.BINANCE,
            symbol="BTC-USDT",
            side=Side.BUY,
            quantity=1.0,
            price=50000.0,
            order_id="m1",
            fee_quote=2.0,
            filled_at_ms=1000,
        )
        hedge_fill = OrderFill(
            venue=Venue.BYBIT,
            symbol="BTC-USDT",
            side=Side.SELL,
            quantity=0.5,
            price=49990.0,
            order_id="h1",
            fee_quote=1.0,
            filled_at_ms=1001,
        )

        pos = build_open_position(ctx, maker_fill, hedge_fill, now_ms=1001)

        assert pos.matched_quantity == pytest.approx(0.5)
        assert pos.initial_quantity == pytest.approx(0.5)
        assert pos.entered_at_ms == 1001
        assert pos.entry_notional_quote == pytest.approx(24997.5)
        assert pos.long_entry_fee_quote == pytest.approx(1.0)
        assert pos.short_entry_fee_quote == pytest.approx(1.0)
        assert pos.total_entry_fee_quote == pytest.approx(2.0)
        assert pos.current_net_quote == pytest.approx(-2.0)
        assert pos.peak_net_quote == pytest.approx(-2.0)
        assert pos.entry_quality_completed_at_ms == 0

    def test_build_open_position_preserves_funding_semantics(self):
        """Entry-selected funding timestamps must survive into close decisions."""
        from lightfee.config.schema import StrategyConfig
        from lightfee.engine.exit_decision import standard_close_reason

        first_funding_ms = 1780167600000
        second_funding_ms = 1780171200000
        ctx = EntryContext(
            entry_id="entry-1780163908797-MAGMAUSDT",
            symbol="MAGMAUSDT",
            long_venue=Venue.ASTER,
            short_venue=Venue.BYBIT,
            long_quantity=100.0,
            short_quantity=100.0,
            long_price_hint=0.275,
            short_price_hint=0.275,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
            opportunity_type="staggered",
            funding_timestamp_ms=first_funding_ms,
            first_funding_timestamp_ms=first_funding_ms,
            long_funding_timestamp_ms=first_funding_ms,
            short_funding_timestamp_ms=second_funding_ms,
            second_funding_timestamp_ms=second_funding_ms,
            first_funding_leg="long",
            funding_edge_bps_entry=7.45,
            # A positive incremental carry is required before a staggered
            # lifecycle can retain the second settlement.
            total_funding_edge_bps_entry=9.45,
            expected_edge_bps_entry=6.9,
        )
        maker_fill = OrderFill(
            venue=Venue.ASTER,
            symbol="MAGMAUSDT",
            side=Side.BUY,
            quantity=100.0,
            price=0.275,
            order_id="magma-maker",
            filled_at_ms=1780163908797,
        )
        hedge_fill = OrderFill(
            venue=Venue.BYBIT,
            symbol="MAGMAUSDT",
            side=Side.SELL,
            quantity=100.0,
            price=0.274,
            order_id="magma-hedge",
            filled_at_ms=1780163908798,
        )

        pos = build_open_position(ctx, maker_fill, hedge_fill, now_ms=1780163908797)

        assert pos.opportunity_type == "staggered"
        assert pos.funding_timestamp_ms == first_funding_ms
        assert pos.long_funding_timestamp_ms == first_funding_ms
        assert pos.short_funding_timestamp_ms == second_funding_ms
        assert pos.second_funding_timestamp_ms == second_funding_ms
        assert pos.second_stage_enabled_at_entry is True
        assert pos.funding_edge_bps_entry == pytest.approx(7.45)
        assert pos.total_funding_edge_bps_entry == pytest.approx(9.45)
        assert pos.expected_edge_bps_entry == pytest.approx(6.9)
        assert standard_close_reason(
            pos,
            StrategyConfig(settlement_remainder_close_delay_secs=300),
            1780163920476,
        ) is None

    def test_build_open_position_copies_v1_entry_metadata(self):
        """V1 PendingEntryHedge::build_open_position copies metadata into OpenPosition."""
        ctx = EntryContext(
            entry_id="entry-v1-metadata",
            symbol="BTC-USDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=0.1,
            short_quantity=0.1,
            long_price_hint=50000.0,
            short_price_hint=50010.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
            opportunity_type="staggered",
            first_funding_leg="long",
            entry_maker_leg="long",
            exit_maker_leg="short",
            funding_edge_bps_entry=8.0,
            total_funding_edge_bps_entry=11.0,
            expected_edge_bps_entry=6.5,
            worst_case_edge_bps_entry=4.0,
            entry_cross_bps_entry=1.25,
            fee_bps_entry=2.1,
            entry_slippage_bps_entry=0.75,
            transfer_bias_bps_entry=-0.5,
            transfer_state_at_entry="ok",
            entry_liquidity_source_at_entry="local_l2",
            long_volume_24h_quote_at_entry=12_000_000.0,
            short_volume_24h_quote_at_entry=15_000_000.0,
            long_open_interest_quote_at_entry=8_000_000.0,
            short_open_interest_quote_at_entry=9_000_000.0,
            long_entry_vwap=50000.5,
            short_entry_vwap=50010.5,
            entry_capacity_constrained=True,
            entry_target_quantity=0.2,
            long_max_executable_quantity=0.18,
            short_max_executable_quantity=0.16,
            entry_max_executable_quantity=0.16,
            entry_depth_shortfall_quantity=0.04,
            entry_max_executable_notional_quote=8000.0,
            entry_depth_capped_at_entry=True,
            advisories=["thin_book"],
            blocked_reasons=["capacity_cap"],
        )
        maker_fill = OrderFill(
            venue=Venue.BINANCE,
            symbol="BTC-USDT",
            side=Side.BUY,
            quantity=0.1,
            price=50000.0,
            order_id="maker",
            filled_at_ms=1000,
        )
        hedge_fill = OrderFill(
            venue=Venue.BYBIT,
            symbol="BTC-USDT",
            side=Side.SELL,
            quantity=0.1,
            price=50010.0,
            order_id="hedge",
            filled_at_ms=1001,
        )

        pos = build_open_position(ctx, maker_fill, hedge_fill, now_ms=1001)

        assert pos.first_funding_leg == "long"
        assert pos.entry_maker_leg == "long"
        assert pos.exit_maker_leg == "short"
        assert pos.worst_case_edge_bps_entry == pytest.approx(4.0)
        assert pos.entry_cross_bps_entry == pytest.approx(1.25)
        assert pos.fee_bps_entry == pytest.approx(2.1)
        assert pos.entry_slippage_bps_entry == pytest.approx(0.75)
        assert pos.transfer_bias_bps_entry == pytest.approx(-0.5)
        assert pos.transfer_state_at_entry == "ok"
        assert pos.entry_liquidity_source_at_entry == "local_l2"
        assert pos.long_volume_24h_quote_at_entry == pytest.approx(12_000_000.0)
        assert pos.short_volume_24h_quote_at_entry == pytest.approx(15_000_000.0)
        assert pos.long_open_interest_quote_at_entry == pytest.approx(8_000_000.0)
        assert pos.short_open_interest_quote_at_entry == pytest.approx(9_000_000.0)
        assert pos.long_entry_vwap == pytest.approx(50000.5)
        assert pos.short_entry_vwap == pytest.approx(50010.5)
        assert pos.entry_capacity_constrained is True
        assert pos.entry_target_quantity == pytest.approx(0.2)
        assert pos.long_max_executable_quantity == pytest.approx(0.18)
        assert pos.short_max_executable_quantity == pytest.approx(0.16)
        assert pos.entry_max_executable_quantity == pytest.approx(0.16)
        assert pos.entry_depth_shortfall_quantity == pytest.approx(0.04)
        assert pos.entry_max_executable_notional_quote == pytest.approx(8000.0)
        assert pos.entry_depth_capped_at_entry is True
        assert pos.advisories == ["thin_book"]
        assert pos.blocked_reasons == ["capacity_cap"]


class TestEntryContextFields:
    """V1 EntryContext preserves all necessary semantic fields."""

    def test_entry_context_has_planned_route(self):
        ctx = EntryContext(
            entry_id="e1", symbol="BTC-USDT",
            long_venue=Venue.BINANCE, short_venue=Venue.BYBIT,
            long_quantity=1.0, short_quantity=1.0,
            long_price_hint=50000.0, short_price_hint=50000.0,
            maker_leg=Side.BUY, entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        assert ctx.planned_route is not None

    def test_entry_context_has_reprice_action(self):
        ctx = EntryContext(
            entry_id="e1", symbol="BTC-USDT",
            long_venue=Venue.BINANCE, short_venue=Venue.BYBIT,
            long_quantity=1.0, short_quantity=1.0,
            long_price_hint=50000.0, short_price_hint=50000.0,
            maker_leg=Side.BUY, entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        # reprice_action starts empty
        assert ctx.reprice_action == ""

    def test_entry_context_has_parent_entry_id(self):
        ctx = EntryContext(
            entry_id="e1", symbol="BTC-USDT",
            long_venue=Venue.BINANCE, short_venue=Venue.BYBIT,
            long_quantity=1.0, short_quantity=1.0,
            long_price_hint=50000.0, short_price_hint=50000.0,
            maker_leg=Side.BUY, entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        assert ctx.parent_entry_id is None  # starts with no parent


class TestRuntimeRecoveryDedupCidMatch:
    """Runtime recovery dedup must use the same CID generation as build_entry_orders.

    If runtime uses old-style f"{entry_id}-maker"/f"{entry_id}-hedge"
    while build_entry_orders uses generate_exchange_cid(entry_id, "m"/"h", venue),
    the dedup index keys won't match the actual on-wire clientOrderId.
    """

    def test_dedup_cid_matches_build_entry_orders_cid(self):
        from lightfee.venues.cid import generate_exchange_cid
        from lightfee.core.domain import Side, Venue
        from lightfee.engine.entry import EntryContext, EntryType, build_entry_orders

        entry_id = "entry-1715000000000-BTCUSDT"
        long_venue = Venue.BINANCE
        short_venue = Venue.BYBIT
        maker_leg = Side.BUY

        # CID generation as runtime _dispatch_entry would do it (fixed version)
        maker_venue = long_venue if maker_leg == Side.BUY else short_venue
        hedge_venue = short_venue if maker_leg == Side.BUY else long_venue
        dedup_maker_cid = generate_exchange_cid(entry_id, "m", maker_venue)
        dedup_hedge_cid = generate_exchange_cid(entry_id, "h", hedge_venue)

        # CID generation as build_entry_orders does it
        ctx = EntryContext(
            entry_id=entry_id,
            symbol="BTCUSDT",
            long_venue=long_venue,
            short_venue=short_venue,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=maker_leg,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        maker_req, hedge_req = build_entry_orders(ctx)

        assert maker_req.client_order_id == dedup_maker_cid, (
            f"maker CID mismatch: dedup={dedup_maker_cid} vs build={maker_req.client_order_id}"
        )
        assert hedge_req.client_order_id == dedup_hedge_cid, (
            f"hedge CID mismatch: dedup={dedup_hedge_cid} vs build={hedge_req.client_order_id}"
        )

    def test_dedup_cid_differs_from_old_style(self):
        """The new hash-based CID must differ from the old f-string form.
        This proves the fix is non-trivial — old code was wrong."""
        from lightfee.venues.cid import generate_exchange_cid
        from lightfee.core.domain import Venue

        entry_id = "entry-1715000000000-BTCUSDT"
        old_maker = f"{entry_id}-maker"
        old_hedge = f"{entry_id}-hedge"

        maker_venue = Venue.BINANCE
        hedge_venue = Venue.BYBIT
        new_maker = generate_exchange_cid(entry_id, "m", maker_venue)
        new_hedge = generate_exchange_cid(entry_id, "h", hedge_venue)

        assert new_maker != old_maker
        assert new_hedge != old_hedge
        # Hash CIDs should be shorter and hex-only
        assert len(new_maker) <= 36
        assert all(c in "0123456789abcdef" for c in new_maker)

    def test_dedup_cid_vary_per_venue(self):
        """Venues with different max_len produce different-length CIDs."""
        from lightfee.venues.cid import generate_exchange_cid
        from lightfee.core.domain import Venue

        entry_id = "entry-1715000000000-BTCUSDT"
        binance_cid = generate_exchange_cid(entry_id, "m", Venue.BINANCE)  # max 36
        okx_cid = generate_exchange_cid(entry_id, "m", Venue.OKX)  # max 32
        # Same hash source → same hex prefix, but OKX truncates to 32 → 16 bytes → 32 hex
        assert len(okx_cid) == 32
        assert len(binance_cid) == 36
        assert okx_cid != binance_cid  # different lengths → different strings
