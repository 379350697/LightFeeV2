"""Task 2: Entry planner contract tests matching Rust V1 plan_incremental_entry_execution.

Rust references:
- src/execution_core/entry_execution_planner.rs: plan_incremental_entry_execution
- src/execution_core/entry_execution_planner.rs: bounded_maker_first_initial_target_quantity
- src/execution_core/entry_execution_planner.rs: maker_min_valid_clip_quantity
- src/engine/entry.rs: effective_entry_leg_notional_floor
- src/engine/entry.rs: align_quantity_down_to_chunk
- src/engine/entry.rs: min_hedgeable_chunk_from_spec
"""

from __future__ import annotations

import math

import pytest

from lightfee.core.domain import Side, Venue
from lightfee.engine.execution_planner import (
    ExecutionRoute,
    align_quantity_down_to_chunk,
    bounded_maker_first_initial_target_quantity,
    common_executable_quantity_step,
    effective_entry_leg_notional_floor,
    maker_min_valid_clip_quantity,
    min_hedgeable_chunk_from_notional,
    plan_incremental_entry_execution,
)
from lightfee.engine.entry import EntryContext, EntryState, EntryType
from lightfee.risk.budgets import RiskBudgets


# ---------------------------------------------------------------------------
# effective_entry_leg_notional_floor — V1 line 430
# ---------------------------------------------------------------------------


class TestEffectiveEntryLegNotionalFloor:
    def test_global_only_returns_global(self):
        assert effective_entry_leg_notional_floor(8.0, None) == 8.0

    def test_exchange_lower_returns_global(self):
        assert effective_entry_leg_notional_floor(8.0, 5.0) == 8.0

    def test_exchange_higher_returns_exchange(self):
        assert effective_entry_leg_notional_floor(8.0, 10.0) == 10.0

    def test_global_zero_returns_exchange(self):
        assert effective_entry_leg_notional_floor(0.0, 5.0) == 5.0

    def test_both_zero_returns_zero(self):
        assert effective_entry_leg_notional_floor(0.0, None) == 0.0

    def test_global_negative_returns_zero(self):
        assert effective_entry_leg_notional_floor(-5.0, None) == 0.0


# ---------------------------------------------------------------------------
# align_quantity_down_to_chunk — V1 line 4558
# ---------------------------------------------------------------------------


class TestAlignQuantityDownToChunk:
    def test_exact_chunks(self):
        assert align_quantity_down_to_chunk(10.0, 2.0) == 10.0

    def test_floor_behavior(self):
        assert align_quantity_down_to_chunk(9.5, 2.0) == 8.0

    def test_below_one_chunk_returns_zero(self):
        assert align_quantity_down_to_chunk(1.5, 2.0) == 0.0

    def test_zero_quantity_returns_zero(self):
        assert align_quantity_down_to_chunk(0.0, 2.0) == 0.0

    def test_negative_quantity_returns_zero(self):
        assert align_quantity_down_to_chunk(-5.0, 2.0) == 0.0

    def test_non_finite_quantity_returns_zero(self):
        assert align_quantity_down_to_chunk(float("inf"), 2.0) == 0.0
        assert align_quantity_down_to_chunk(float("nan"), 2.0) == 0.0

    def test_zero_or_tiny_chunk_returns_quantity(self):
        assert align_quantity_down_to_chunk(5.0, 0.0) == 5.0
        assert align_quantity_down_to_chunk(5.0, 1e-12) == 5.0

    def test_non_finite_chunk_returns_quantity(self):
        assert align_quantity_down_to_chunk(5.0, float("nan")) == 5.0


class TestCommonExecutableQuantityStep:
    def test_uses_the_smallest_grid_legal_for_both_legs(self):
        assert common_executable_quantity_step(0.002, 0.003) == pytest.approx(0.006)

    def test_preserves_okx_integer_base_contract_grid(self):
        assert common_executable_quantity_step(0.001, 100.0) == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# bounded_maker_first_initial_target_quantity — V1 line 33
# ---------------------------------------------------------------------------


class TestBoundedMakerFirstInitialTargetQuantity:
    def test_half_slice_returns_half(self):
        result = bounded_maker_first_initial_target_quantity(10.0, 0.5)
        assert result == pytest.approx(5.0)

    def test_ratio_at_one_returns_full_target(self):
        result = bounded_maker_first_initial_target_quantity(10.0, 1.0)
        assert result == pytest.approx(10.0)

    def test_ratio_near_one_returns_full_target(self):
        result = bounded_maker_first_initial_target_quantity(10.0, 0.999999999)
        assert result == pytest.approx(10.0)

    def test_zero_target_returns_zero(self):
        assert bounded_maker_first_initial_target_quantity(0.0, 0.5) == 0.0

    def test_negative_target_returns_zero(self):
        assert bounded_maker_first_initial_target_quantity(-1.0, 0.5) == 0.0

    def test_tiny_target_below_chunk_returns_target(self):
        result = bounded_maker_first_initial_target_quantity(0.01, 0.5)
        assert result == pytest.approx(0.005)

    def test_slice_ratio_zero_bounds_to_full_target(self):
        # V1: silced <= 1e-9 falls back to full target (line 42)
        result = bounded_maker_first_initial_target_quantity(10.0, 0.0)
        assert result == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# maker_min_valid_clip_quantity — V1 line 210
# ---------------------------------------------------------------------------


class TestMakerMinValidClipQuantity:
    def test_returns_quantity_from_notional_and_price(self):
        result = maker_min_valid_clip_quantity(
            maker_min_notional_quote=100.0,
            maker_price_hint=50.0,
            min_hedgeable_chunk=0.01,
            full_target_quantity=10.0,
        )
        # raw = 100/50 = 2.0, ceil(2.0/0.01)*0.01 = 2.0
        assert result == pytest.approx(2.0)

    def test_aligns_up_to_chunk(self):
        result = maker_min_valid_clip_quantity(
            maker_min_notional_quote=100.0,
            maker_price_hint=60.0,
            min_hedgeable_chunk=1.0,
            full_target_quantity=10.0,
        )
        # raw = 100/60 = 1.666..., ceil(1.666.../1.0)*1.0 = 2.0
        assert result == pytest.approx(2.0)

    def test_capped_by_max_of_full_target_and_raw(self):
        # V1: .min(full_target_quantity.max(raw_quantity))
        # When raw=10 > full_target=3, max(3,10)=10, min(10,10)=10
        result = maker_min_valid_clip_quantity(
            maker_min_notional_quote=500.0,
            maker_price_hint=50.0,
            min_hedgeable_chunk=1.0,
            full_target_quantity=3.0,
        )
        assert result == pytest.approx(10.0)

    def test_no_min_notional_returns_none(self):
        result = maker_min_valid_clip_quantity(
            maker_min_notional_quote=0.0,
            maker_price_hint=50.0,
            min_hedgeable_chunk=1.0,
            full_target_quantity=10.0,
        )
        assert result is None

    def test_no_price_hint_returns_none(self):
        result = maker_min_valid_clip_quantity(
            maker_min_notional_quote=100.0,
            maker_price_hint=None,
            min_hedgeable_chunk=1.0,
            full_target_quantity=10.0,
        )
        assert result is None

    def test_non_finite_min_notional_returns_none(self):
        result = maker_min_valid_clip_quantity(
            maker_min_notional_quote=float("nan"),
            maker_price_hint=50.0,
            min_hedgeable_chunk=1.0,
            full_target_quantity=10.0,
        )
        assert result is None

    def test_zero_hedgeable_chunk_uses_raw_quantity(self):
        result = maker_min_valid_clip_quantity(
            maker_min_notional_quote=100.0,
            maker_price_hint=50.0,
            min_hedgeable_chunk=0.0,
            full_target_quantity=10.0,
        )
        assert result == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# min_hedgeable_chunk_from_notional — V1 line 4583
# ---------------------------------------------------------------------------


class TestMinHedgeableChunkFromNotional:
    def test_returns_max_of_min_base_and_notional_quantity(self):
        result = min_hedgeable_chunk_from_notional(
            min_base_quantity=0.001,
            min_notional_quote=10.0,
            step_base_quantity=0.001,
            price_hint=50000.0,
        )
        # notional_qty = 10.0/50000.0 = 0.0002
        # max(0.001, 0.0002) = 0.001
        # align up to step: ceil(0.001/0.001)*0.001 = 0.001
        assert result == pytest.approx(0.001)

    def test_notional_larger_than_base(self):
        result = min_hedgeable_chunk_from_notional(
            min_base_quantity=0.001,
            min_notional_quote=100.0,
            step_base_quantity=0.001,
            price_hint=50000.0,
        )
        # notional_qty = 100.0/50000.0 = 0.002
        # max(0.001, 0.002) = 0.002
        # align up to step: ceil(0.002/0.001)*0.001 = 0.002
        assert result == pytest.approx(0.002)

    def test_missing_price_hint_raises(self):
        with pytest.raises(ValueError, match="price_hint"):
            min_hedgeable_chunk_from_notional(
                min_base_quantity=0.001,
                min_notional_quote=10.0,
                step_base_quantity=0.001,
                price_hint=None,
            )

    def test_zero_price_hint_raises(self):
        with pytest.raises(ValueError, match="price_hint"):
            min_hedgeable_chunk_from_notional(
                min_base_quantity=0.001,
                min_notional_quote=10.0,
                step_base_quantity=0.001,
                price_hint=0.0,
            )

    def test_no_min_notional_uses_min_base_only(self):
        result = min_hedgeable_chunk_from_notional(
            min_base_quantity=0.01,
            min_notional_quote=0.0,
            step_base_quantity=0.01,
            price_hint=50000.0,
        )
        assert result == pytest.approx(0.01)


# ---------------------------------------------------------------------------
# plan_incremental_entry_execution — V1 line 49
# ---------------------------------------------------------------------------


class TestPlanIncrementalEntryExecution:
    """Rust V1 plan_incremental_entry_execution behavioral contract."""

    def test_zero_target_rejected(self):
        route, plan = plan_incremental_entry_execution(
            target_quantity=0.0,
            slice_ratio=0.5,
            min_hedgeable_chunk=0.001,
            maker_min_notional_quote=10.0,
            maker_price_hint=50000.0,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=10.0,
            hedge_price_hint=50000.0,
        )
        assert route == ExecutionRoute.REJECTED
        assert plan.reason == "target_quantity_not_positive"

    def test_negative_target_rejected(self):
        route, plan = plan_incremental_entry_execution(
            target_quantity=-1.0,
            slice_ratio=0.5,
            min_hedgeable_chunk=0.001,
            maker_min_notional_quote=10.0,
            maker_price_hint=50000.0,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=10.0,
            hedge_price_hint=50000.0,
        )
        assert route == ExecutionRoute.REJECTED

    def test_non_finite_target_rejected(self):
        route, plan = plan_incremental_entry_execution(
            target_quantity=float("nan"),
            slice_ratio=0.5,
            min_hedgeable_chunk=0.001,
            maker_min_notional_quote=10.0,
            maker_price_hint=50000.0,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=10.0,
            hedge_price_hint=50000.0,
        )
        assert route == ExecutionRoute.REJECTED

    def test_target_below_min_hedgeable_chunk_rejected(self):
        route, plan = plan_incremental_entry_execution(
            target_quantity=0.3,
            slice_ratio=0.5,
            min_hedgeable_chunk=1.0,
            maker_min_notional_quote=0.0,
            maker_price_hint=None,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=0.0,
            hedge_price_hint=None,
        )
        assert route == ExecutionRoute.REJECTED
        assert plan.reason == "target_below_min_hedgeable_chunk"

    def test_maker_min_clip_exceeds_full_target_rejected(self):
        route, plan = plan_incremental_entry_execution(
            target_quantity=0.001,
            slice_ratio=0.5,
            min_hedgeable_chunk=0.001,
            maker_min_notional_quote=100.0,  # very high min notional
            maker_price_hint=50000.0,  # needs 0.002 base qty
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=0.0,
            hedge_price_hint=None,
        )
        assert route == ExecutionRoute.REJECTED
        assert "maker" in plan.reason

    def test_maker_min_clip_too_close_to_full_target_fallback(self):
        """V1 test: planner_falls_back_when_minimum_clip_is_too_close_to_full_target"""
        route, plan = plan_incremental_entry_execution(
            target_quantity=10.0,
            slice_ratio=0.5,
            min_hedgeable_chunk=2.0,
            maker_min_notional_quote=41.959,
            maker_price_hint=4.1959,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=0.0,
            hedge_price_hint=None,
        )
        assert route == ExecutionRoute.FALLBACK_TO_STANDARD
        assert "maker_min_clip" in plan.reason

    def test_passive_with_raised_clip_to_min_notional(self):
        """V1 test: planner_raises_initial_passive_clip_to_maker_min_notional_chunk"""
        route, plan = plan_incremental_entry_execution(
            target_quantity=12.0,
            slice_ratio=0.5,
            min_hedgeable_chunk=2.0,
            maker_min_notional_quote=41.959,
            maker_price_hint=4.1959,
            max_initial_clip_ratio=0.9,
            hedge_min_notional_quote=0.0,
            hedge_price_hint=None,
        )
        assert route == ExecutionRoute.PASSIVE_INCREMENTAL
        assert plan.full_target_quantity == pytest.approx(12.0)
        assert plan.initial_maker_target_quantity == pytest.approx(10.0)

    def test_preserves_chunk_alignment_no_floor(self):
        """V1 test: planner_preserves_existing_chunk_alignment_when_no_maker_floor_applies"""
        route, plan = plan_incremental_entry_execution(
            target_quantity=0.26,
            slice_ratio=0.1,
            min_hedgeable_chunk=0.1,
            maker_min_notional_quote=0.0,
            maker_price_hint=None,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=0.0,
            hedge_price_hint=None,
        )
        assert route == ExecutionRoute.PASSIVE_INCREMENTAL
        assert plan.full_target_quantity == pytest.approx(0.2)
        assert plan.initial_maker_target_quantity == pytest.approx(0.1)

    def test_hedge_remainder_below_min_notional_fallback(self):
        """V1 test: planner_falls_back_when_only_one_chunk_and_hedge_min_notional_requires_two"""
        route, plan = plan_incremental_entry_execution(
            target_quantity=0.18,
            slice_ratio=0.5,
            min_hedgeable_chunk=0.18,
            maker_min_notional_quote=0.0,
            maker_price_hint=None,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=18.0,
            hedge_price_hint=100.0,
        )
        assert route == ExecutionRoute.FALLBACK_TO_STANDARD
        assert plan.reason == "hedge_remainder_below_min_notional"

    def test_hedge_min_notional_satisfied_passive(self):
        """V1 test: planner_uses_passive_when_two_chunks_and_hedge_remainder_above_min_notional"""
        route, plan = plan_incremental_entry_execution(
            target_quantity=0.36,
            slice_ratio=0.5,
            min_hedgeable_chunk=0.18,
            maker_min_notional_quote=0.0,
            maker_price_hint=None,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=18.0,
            hedge_price_hint=100.0,
        )
        assert route == ExecutionRoute.PASSIVE_INCREMENTAL
        assert plan.full_target_quantity == pytest.approx(0.36)
        assert plan.initial_maker_target_quantity == pytest.approx(0.18)

    def test_both_minimums_exceed_target_fallback(self):
        """V1: planner_falls_back_when_maker_and_hedge_minimums_exceed_full_target"""
        route, plan = plan_incremental_entry_execution(
            target_quantity=0.20,
            slice_ratio=0.5,
            min_hedgeable_chunk=0.18,
            maker_min_notional_quote=18.0,
            maker_price_hint=100.0,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=18.0,
            hedge_price_hint=100.0,
        )
        assert route == ExecutionRoute.FALLBACK_TO_STANDARD

    def test_no_hedgeable_chunk_uses_raw_target(self):
        """When min_hedgeable_chunk is zero, target not aligned."""
        route, plan = plan_incremental_entry_execution(
            target_quantity=5.0,
            slice_ratio=0.5,
            min_hedgeable_chunk=0.0,
            maker_min_notional_quote=0.0,
            maker_price_hint=None,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=0.0,
            hedge_price_hint=None,
        )
        assert route == ExecutionRoute.PASSIVE_INCREMENTAL
        assert plan.full_target_quantity == pytest.approx(5.0)
        assert plan.initial_maker_target_quantity == pytest.approx(2.5)

    def test_slice_ratio_one_returns_full_target_as_maker(self):
        route, plan = plan_incremental_entry_execution(
            target_quantity=10.0,
            slice_ratio=1.0,
            min_hedgeable_chunk=1.0,
            maker_min_notional_quote=0.0,
            maker_price_hint=None,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=0.0,
            hedge_price_hint=None,
        )
        assert route == ExecutionRoute.PASSIVE_INCREMENTAL
        assert plan.initial_maker_target_quantity == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# Budget-gated entry — RiskBudgets integration
# ---------------------------------------------------------------------------


class TestBudgetGatedEntry:
    def test_budget_allows_entry_within_limits(self):
        budgets = RiskBudgets(
            max_concurrent_positions=2,
            max_single_venue_exposure_quote=1000.0,
            max_symbol_exposure_quote=500.0,
        )
        allowed, reason = budgets.check_entry("binance", "BTCUSDT", 100.0)
        assert allowed is True
        assert reason == ""

    def test_budget_blocks_when_position_limit_reached(self):
        budgets = RiskBudgets(
            max_concurrent_positions=1,
            max_single_venue_exposure_quote=1000.0,
            max_symbol_exposure_quote=500.0,
        )
        budgets.record_entry("binance", "BTCUSDT", 100.0)
        allowed, reason = budgets.check_entry("bybit", "ETHUSDT", 100.0)
        assert not allowed
        assert "max_concurrent_positions" in reason

    def test_budget_blocks_venue_exposure_exceeded(self):
        budgets = RiskBudgets(
            max_concurrent_positions=2,
            max_single_venue_exposure_quote=150.0,
            max_symbol_exposure_quote=500.0,
        )
        budgets.record_entry("binance", "BTCUSDT", 100.0)
        allowed, reason = budgets.check_entry("binance", "ETHUSDT", 100.0)
        assert not allowed
        assert "venue exposure" in reason

    def test_budget_blocks_symbol_exposure_exceeded(self):
        budgets = RiskBudgets(
            max_concurrent_positions=2,
            max_single_venue_exposure_quote=1000.0,
            max_symbol_exposure_quote=100.0,
        )
        budgets.record_entry("binance", "BTCUSDT", 80.0)
        allowed, reason = budgets.check_entry("bybit", "BTCUSDT", 50.0)
        assert not allowed
        assert "symbol exposure" in reason

    def test_budget_records_entry_and_exit(self):
        budgets = RiskBudgets(max_concurrent_positions=2)
        budgets.record_entry("binance", "BTCUSDT", 100.0)
        assert budgets.current_position_count == 1
        budgets.record_exit("binance", "BTCUSDT", 100.0)
        assert budgets.current_position_count == 0

    def test_budget_zero_limits_allow_all(self):
        budgets = RiskBudgets(
            max_concurrent_positions=0,
            max_single_venue_exposure_quote=0.0,
            max_symbol_exposure_quote=0.0,
        )
        allowed, reason = budgets.check_entry("binance", "BTCUSDT", 1_000_000.0)
        assert allowed is True

    def test_rejected_entry_does_not_consume_budget(self):
        budgets = RiskBudgets(max_concurrent_positions=1)
        assert budgets.current_position_count == 0
        # Simulate: check then reject without recording
        allowed, _ = budgets.check_entry("binance", "BTCUSDT", 100.0)
        assert allowed is True
        assert budgets.current_position_count == 0


# ---------------------------------------------------------------------------
# EntryContext budget integration
# ---------------------------------------------------------------------------


class TestEntryContextWithBudget:
    def test_entry_context_carries_plan_route(self):
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
            planned_route=ExecutionRoute.PASSIVE_INCREMENTAL,
        )
        assert ctx.planned_route == ExecutionRoute.PASSIVE_INCREMENTAL
        assert ctx.state == EntryState.IDLE

    def test_entry_context_defaults_to_idle(self):
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.SELL,
            entry_type=EntryType.STANDARD_DUAL_TAKER,
        )
        assert ctx.state == EntryState.IDLE
        assert ctx.maker_fill is None
        assert ctx.hedge_fill is None
