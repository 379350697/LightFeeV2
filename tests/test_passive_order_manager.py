"""Tests for V1 PassiveOrderManager (passive_order_manager.rs)."""

import pytest
from lightfee.engine.passive_order_manager import (
    PassiveOrderManager,
    PassiveOrderManagerProfile,
    PassiveOrderDecisionInput,
    PassiveOrderManagerDecisionType,
    PassiveSkipReason,
    PassiveReplaceReason,
    PassiveCooldownReason,
    passive_price_distance_bps,
    passive_tick_distance,
)


def _profile(**overrides) -> PassiveOrderManagerProfile:
    kwargs = dict(
        reprice_threshold_ticks=2,
        cancel_replace_threshold_ticks=5,
        min_amend_interval_ms=300,
        reprice_threshold_bps=2.0,
        cancel_replace_threshold_bps=6.0,
        ops_bucket_capacity=8.0,
        ops_bucket_refill_per_sec=8.0,
        working_timeout_ms=3000,
        max_ops_per_sec=8,
        max_consecutive_failures=5,
        failure_cooldown_ms=5000,
        follow_market_reprice_enabled=True,
        prefer_amend_over_cancel=True,
    )
    kwargs.update(overrides)
    return PassiveOrderManagerProfile(**kwargs)


def _buy_input(**overrides) -> PassiveOrderDecisionInput:
    kwargs = dict(
        tick_size=1.0,
        target_price=101.0,
        current_price=100.0,
        resting_since_ms=0,
        target_quantity=1.0,
        supports_amend=True,
    )
    kwargs.update(overrides)
    return PassiveOrderDecisionInput(**kwargs)


class TestPassiveOrderManagerDecide:
    """V1 PassiveOrderManager::decide semantics."""

    def test_hold_below_reprice_threshold(self):
        """Small price move → Hold (BelowRepriceThreshold)."""
        manager = PassiveOrderManager(_profile())
        decision = manager.decide(_buy_input(), 100)
        assert decision.kind == PassiveOrderManagerDecisionType.HOLD
        assert decision.skip_reason == PassiveSkipReason.BELOW_REPRICE_THRESHOLD

    def test_amend_when_supported_and_price_moves(self):
        """Sufficient price move → Amend when amend is supported."""
        manager = PassiveOrderManager(_profile())
        decision = manager.decide(_buy_input(current_price=97.0), 2000)
        assert decision.kind == PassiveOrderManagerDecisionType.AMEND
        assert decision.new_price == 101.0

    def test_cancel_replace_after_timeout(self):
        """Resting past working_timeout_ms → CancelReplace."""
        manager = PassiveOrderManager(_profile())
        decision = manager.decide(
            _buy_input(current_price=110.0, resting_since_ms=100),
            3200,  # elapsed = 3100ms > 3000ms working_timeout_ms
        )
        assert decision.kind == PassiveOrderManagerDecisionType.CANCEL_REPLACE
        assert decision.replace_reason == PassiveReplaceReason.TIMEOUT

    def test_cancel_replace_when_amend_unsupported(self):
        """Large deviation + amend unsupported → CancelReplace."""
        manager = PassiveOrderManager(_profile())
        decision = manager.decide(
            _buy_input(current_price=90.0, supports_amend=False),
            2000,
        )
        assert decision.kind == PassiveOrderManagerDecisionType.CANCEL_REPLACE
        assert decision.replace_reason == PassiveReplaceReason.AMEND_UNSUPPORTED

    def test_hold_min_amend_interval_not_elapsed(self):
        """Recent operation → Hold for min_amend_interval."""
        manager = PassiveOrderManager(_profile())
        manager.note_operation(1000)
        decision = manager.decide(_buy_input(current_price=97.0), 1100)
        assert decision.kind == PassiveOrderManagerDecisionType.HOLD
        assert decision.skip_reason == PassiveSkipReason.MIN_AMEND_INTERVAL_NOT_ELAPSED

    def test_hold_ops_budget_exceeded(self):
        """Token bucket empty → Hold (OpsBudgetExceeded)."""
        manager = PassiveOrderManager(_profile())
        for offset in range(8):
            manager.note_operation(1000 + offset)
        decision = manager.decide(_buy_input(current_price=90.0), 1050)
        assert decision.kind == PassiveOrderManagerDecisionType.HOLD
        assert decision.skip_reason == PassiveSkipReason.OPS_BUDGET_EXCEEDED

    def test_token_bucket_refills(self):
        """After waiting, ops bucket refills and amend becomes possible."""
        manager = PassiveOrderManager(_profile())
        for offset in range(8):
            manager.note_operation(1000 + offset)
        # Exhausted at t=1050
        decision = manager.decide(_buy_input(current_price=90.0), 1050)
        assert decision.kind == PassiveOrderManagerDecisionType.HOLD

        # After 400ms, enough tokens refilled (8/sec * 0.4s = 3.2 tokens)
        decision = manager.decide(_buy_input(current_price=97.0), 1400)
        assert decision.kind == PassiveOrderManagerDecisionType.AMEND

    def test_cooldown_after_consecutive_failures(self):
        """After max_consecutive_failures, manager enters cooldown."""
        profile = _profile()
        manager = PassiveOrderManager(profile)
        for step in range(5):
            manager.note_failure(1000 + step)
        decision = manager.decide(_buy_input(), 1100)
        assert decision.kind == PassiveOrderManagerDecisionType.COOLDOWN
        assert decision.cooldown_reason == PassiveCooldownReason.ACTIVE_COOLDOWN
        assert decision.until_ms == 1004 + profile.failure_cooldown_ms

    def test_cooldown_expires(self):
        """After cooldown expires, normal operations resume."""
        profile = _profile()
        manager = PassiveOrderManager(profile)
        for step in range(5):
            manager.note_failure(1000 + step)
        # In cooldown at t=1100
        decision = manager.decide(_buy_input(), 1100)
        assert decision.kind == PassiveOrderManagerDecisionType.COOLDOWN

        # After cooldown expires — no resting time → no timeout
        cooldown_end = 1004 + profile.failure_cooldown_ms
        decision = manager.decide(
            _buy_input(current_price=97.0, resting_since_ms=None),
            cooldown_end + 1,
        )
        assert decision.kind == PassiveOrderManagerDecisionType.AMEND

    def test_note_success_resets_failures(self):
        """Success resets consecutive_failures counter."""
        profile = _profile()
        manager = PassiveOrderManager(profile)
        for step in range(4):  # one less than max
            manager.note_failure(1000 + step)
        manager.note_success(1005)
        assert manager.consecutive_failures == 0
        assert not manager.is_in_cooldown(1005)

    def test_place_when_no_current_price(self):
        """When current_price is None (first placement), return Place."""
        manager = PassiveOrderManager(_profile())
        decision = manager.decide(
            _buy_input(current_price=None),
            100,
        )
        assert decision.kind == PassiveOrderManagerDecisionType.PLACE

    def test_hold_missing_book_data(self):
        """Missing target_price or tick_size → Hold."""
        manager = PassiveOrderManager(_profile())
        decision = manager.decide(
            _buy_input(target_price=None),
            100,
        )
        assert decision.kind == PassiveOrderManagerDecisionType.HOLD
        assert decision.skip_reason == PassiveSkipReason.MISSING_BOOK_DATA


class TestPassiveOrderManagerHelpers:
    def test_distance_bps(self):
        assert passive_price_distance_bps(100.0, 101.0, 100.0) == pytest.approx(100.0)

    def test_distance_bps_none_inputs(self):
        assert passive_price_distance_bps(None, 100.0, 100.0) is None
        assert passive_price_distance_bps(100.0, None, 100.0) is None
        assert passive_price_distance_bps(100.0, 100.0, None) is None

    def test_tick_distance(self):
        assert passive_tick_distance(100.0, 102.0, 0.5) == pytest.approx(4.0)

    def test_runtime_dict_roundtrip(self):
        """Runtime dict captures all manager state."""
        profile = _profile()
        manager = PassiveOrderManager(profile)
        manager.note_operation(1000)
        d = manager.runtime_dict()
        assert "last_action_at_ms" in d
        assert "ops_bucket_tokens" in d
        assert d["ops_in_window"] >= 1  # one operation consumed
        assert "cooldown_until_ms" in d
        # consecutive_failures is 0 after successful operations
        assert d["consecutive_failures"] == 0
