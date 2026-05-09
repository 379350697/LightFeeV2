"""Tests for entry execution planner matching Rust reference behavior."""

import pytest

from lightfee.engine.execution_planner import ExecutionRoute, plan_entry_execution


class TestExecutionPlanner:
    def test_rejects_zero_quantity(self):
        route, clip, reason = plan_entry_execution(0, 100, 10, 0.01, 0.01)
        assert route == ExecutionRoute.REJECTED
        assert "zero_target" in reason

    def test_falls_back_when_maker_min_too_large(self):
        # maker_min_clip > max_initial_clip (0.8 * target)
        route, clip, reason = plan_entry_execution(
            target_quantity=1.0, price_hint=100, min_notional_quote=10,
            maker_min_clip=0.9, hedge_chunk=0.01,
        )
        assert route == ExecutionRoute.FALLBACK_TO_STANDARD

    def test_falls_back_when_clip_too_close_to_target(self):
        # maker_min_clip=0.09 > max_initial(0.08) → fallback
        route, clip, reason = plan_entry_execution(
            target_quantity=0.1, price_hint=1000, min_notional_quote=10,
            maker_min_clip=0.09, hedge_chunk=0.01,
        )
        assert route == ExecutionRoute.FALLBACK_TO_STANDARD

    def test_returns_passive_incremental_for_valid_input(self):
        route, clip, reason = plan_entry_execution(
            target_quantity=1.0, price_hint=100, min_notional_quote=10,
            maker_min_clip=0.1, hedge_chunk=0.01,
        )
        assert route == ExecutionRoute.PASSIVE_INCREMENTAL
        assert clip > 0
        assert reason == ""
