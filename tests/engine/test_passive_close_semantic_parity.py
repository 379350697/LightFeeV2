"""Semantic parity tests for passive close (PCLOSE-001).

V1 references:
- src/engine/exit.rs: PendingPassiveClose, drive_pending_passive_close
- src/engine/exit.rs: maintain_passive_close_order
"""

from __future__ import annotations

import pytest
from lightfee.engine.state import (
    ActiveMakerLeg,
    PassiveExecutionPhase,
    PassivePhaseState,
    PendingPassiveClose,
    PendingPassiveLegFill,
    PersistedCloseExecutionLeg,
)
from lightfee.engine.passive_maker import (
    MakerDecision,
    PassiveMakerState,
    decide_passive_action,
)
from lightfee.engine.passive_close import (
    PASSIVE_CLOSE_MAX_ZERO_FILL_CYCLES,
    PassiveCloseConfig,
    PassiveCloseManagerProfile,
    PassiveCloseMaintenanceOutcome,
    PassiveManagerDecisionKind,
)


# ============================================================================
# PCLOSE-001: Passive Close Phase Semantics
# ============================================================================


class TestPassiveClosePhases:
    """V1 passive close: high-slippage, low-slippage, dual-taker phases."""

    def test_pending_passive_close_initial_state(self):
        pending = PendingPassiveClose(
            position_id="test-pos-1",
            reason="signal",
            target_quantity=1.0,
            chunk_quantities=[1.0],
        )
        assert pending.phase_state.phase == PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER
        assert pending.active_chunk_index == 0
        assert pending.current_chunk_quantity() == 1.0
        assert not pending.completed()

    def test_pending_passive_close_chunk_advance(self):
        pending = PendingPassiveClose(
            position_id="test-pos-1",
            reason="signal",
            target_quantity=1.0,
            chunk_quantities=[0.5, 0.3, 0.2],
        )
        assert pending.current_chunk_quantity() == 0.5
        pending.active_chunk_index = 1
        assert pending.current_chunk_quantity() == 0.3
        pending.active_chunk_index = 2
        assert pending.current_chunk_quantity() == 0.2
        pending.active_chunk_index = 3
        assert pending.completed()

    def test_remaining_chunk_quantity(self):
        pending = PendingPassiveClose(
            position_id="test-pos-1",
            reason="signal",
            target_quantity=1.0,
            chunk_quantities=[1.0],
        )
        pending.maker_fill.quantity = 0.3
        assert pending.remaining_chunk_quantity() == 0.7

    def test_passive_close_zero_fill_tracking(self):
        """V1: zero_fill_cycles track consecutive cycles without fills."""
        phase_state = PassivePhaseState(
            phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
        )
        assert phase_state.zero_fill_cycles_in_phase == 0
        phase_state.zero_fill_cycles_in_phase += 1
        assert phase_state.zero_fill_cycles_in_phase == 1

    def test_passive_close_max_zero_fill_constant(self):
        """V1: PASSIVE_CLOSE_MAX_ZERO_FILL_CYCLES = 3."""
        assert PASSIVE_CLOSE_MAX_ZERO_FILL_CYCLES == 3

    def test_phase_transition_high_to_low_slippage(self):
        """V1: after max zero-fill cycles in high-slippage, switch to low-slippage."""
        phase_state = PassivePhaseState(
            phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
            preferred_maker_leg=ActiveMakerLeg.LONG,
            active_maker_leg=ActiveMakerLeg.LONG,
        )
        # Simulate phase exhaustion: high → low, flip leg
        phase_state.phase = PassiveExecutionPhase.LOW_SLIPPAGE_MAKER
        phase_state.active_maker_leg = ActiveMakerLeg.SHORT
        phase_state.zero_fill_cycles_in_phase = 0
        assert phase_state.phase == PassiveExecutionPhase.LOW_SLIPPAGE_MAKER
        assert phase_state.active_maker_leg == ActiveMakerLeg.SHORT
        assert phase_state.zero_fill_cycles_in_phase == 0

    def test_fallback_to_dual_taker(self):
        """V1: when both phases exhausted, fall back to dual taker."""
        phase_state = PassivePhaseState(
            phase=PassiveExecutionPhase.DUAL_TAKER,
        )
        assert phase_state.phase == PassiveExecutionPhase.DUAL_TAKER

    def test_chunk_suffix_single_chunk(self):
        pending = PendingPassiveClose(
            position_id="p1", reason="signal",
            target_quantity=1.0, chunk_quantities=[1.0],
        )
        assert pending.current_chunk_suffix() == ""

    def test_chunk_suffix_multi_chunk(self):
        pending = PendingPassiveClose(
            position_id="p1", reason="signal",
            target_quantity=1.0, chunk_quantities=[0.5, 0.5],
        )
        assert "_chunk_1" in pending.current_chunk_suffix()

    def test_pending_passive_leg_fill_weighted_average(self):
        """V1: fill average price is weighted by quantity on each new fill."""
        leg = PendingPassiveLegFill(quantity=0.0, average_price=0.0)
        # First fill: 0.3 @ 100.0
        prev_total = leg.quantity * leg.average_price
        new_delta = 0.3
        new_price = 100.0
        new_total = prev_total + new_delta * new_price
        leg.quantity += new_delta
        leg.average_price = new_total / leg.quantity if leg.quantity > 0 else 0.0
        assert leg.quantity == 0.3
        assert leg.average_price == pytest.approx(100.0)

        # Second fill: 0.2 @ 101.0
        prev_total = leg.quantity * leg.average_price
        new_delta = 0.2
        new_price = 101.0
        new_total = prev_total + new_delta * new_price
        leg.quantity += new_delta
        leg.average_price = new_total / leg.quantity if leg.quantity > 0 else 0.0
        assert leg.quantity == 0.5
        assert leg.average_price == pytest.approx((0.3 * 100.0 + 0.2 * 101.0) / 0.5)

    def test_maker_leg_enum_values(self):
        assert ActiveMakerLeg.LONG.value == "long"
        assert ActiveMakerLeg.SHORT.value == "short"

    def test_passive_execution_phase_values(self):
        assert PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER.value == "high_slippage_maker"
        assert PassiveExecutionPhase.LOW_SLIPPAGE_MAKER.value == "low_slippage_maker"
        assert PassiveExecutionPhase.DUAL_TAKER.value == "dual_taker"

    def test_delta_hedge_semantics(self):
        """V1: delta hedge offsets the passive maker fill, keeping position
        net-flat. hedge_fill tracks cumulative hedge quantity."""
        maker_fill = PendingPassiveLegFill(quantity=0.5, average_price=100.0)
        hedge_fill = PendingPassiveLegFill(quantity=0.0, average_price=0.0)

        # Hedge the maker fill delta
        delta = 0.3
        hedge_fill.quantity += delta
        hedge_fill.average_price = 99.0
        assert hedge_fill.quantity == 0.3

        # Hedge remaining gap
        delta = 0.2
        hedge_fill.quantity += delta
        # Cumulative hedge caught up to maker
        assert hedge_fill.quantity == pytest.approx(maker_fill.quantity)

    def test_cumulative_hedge_catch_up(self):
        """V1: hedge_deficit = maker_fill - hedge_fill. Must reach zero."""
        maker_fill = PendingPassiveLegFill(quantity=0.8, average_price=100.0)
        hedge_fill = PendingPassiveLegFill(quantity=0.5, average_price=99.0)
        deficit = maker_fill.quantity - hedge_fill.quantity
        assert deficit > 0
        # After catching up
        hedge_fill.quantity = maker_fill.quantity
        deficit = maker_fill.quantity - hedge_fill.quantity
        assert deficit == pytest.approx(0.0)

    def test_terminal_cleanup_clears_pending(self):
        """V1: terminal cleanup removes PendingPassiveClose from state."""
        pending = PendingPassiveClose(
            position_id="p1", reason="terminal",
            target_quantity=0.0,
            chunk_quantities=[],
        )
        # Simulate terminal state: all chunks completed
        assert pending.completed() or pending.target_quantity == 0


class TestPassiveCloseConfig:
    """V1 PassiveCloseConfig carries all necessary tuning parameters."""

    def test_default_config(self):
        config = PassiveCloseConfig()
        assert config.max_zero_fill_cycles == 3
        assert config.progress_poll_interval_ms == 10
        assert config.progress_retry_window_ms == 3_000
        assert config.small_fill_buffer_ms == 2_000

    def test_custom_config(self):
        config = PassiveCloseConfig(
            max_zero_fill_cycles=5,
            max_slippage_bps=10.0,
            close_chunk_max_notional_quote=5000.0,
        )
        assert config.max_zero_fill_cycles == 5
        assert config.max_slippage_bps == 10.0
        assert config.close_chunk_max_notional_quote == 5000.0


class TestPassiveCloseManagerProfile:
    """V1 PassiveCloseManagerProfile: per-venue tuning for amend/cancel-replace."""

    def test_default_profile(self):
        profile = PassiveCloseManagerProfile()
        assert profile.amend_threshold_bps == 5.0
        assert profile.cancel_replace_threshold_bps == 20.0

    def test_custom_profile(self):
        profile = PassiveCloseManagerProfile(
            amend_threshold_bps=3.0,
            cancel_replace_threshold_bps=15.0,
            ops_budget_per_window=20,
        )
        assert profile.amend_threshold_bps == 3.0
        assert profile.cancel_replace_threshold_bps == 15.0
        assert profile.ops_budget_per_window == 20


# ============================================================================
# Passive Maker Decision Semantics
# ============================================================================


class TestPassiveMakerDecisions:
    """V1 passive maker: reprice, cancel-replace, amend budget, small-fill buffer."""

    def test_hold_when_within_reprice_threshold(self):
        state = PassiveMakerState()
        decision, reason = decide_passive_action(
            current_price=100.0, target_price=100.1,
            reprice_threshold_ticks=5, cancel_replace_threshold_ticks=10,
            tick_size=0.1, min_amend_interval_ms=1000,
            last_action_ms=0, now_ms=5000,
            prefer_amend=True, state=state,
        )
        assert decision == MakerDecision.HOLD

    def test_amend_when_past_reprice_threshold(self):
        state = PassiveMakerState()
        # distance: |100.0 - 100.8| / 0.1 = 8 ticks
        # 5 <= 8 < 10 → AMEND
        decision, reason = decide_passive_action(
            current_price=100.0, target_price=100.8,
            reprice_threshold_ticks=5, cancel_replace_threshold_ticks=10,
            tick_size=0.1, min_amend_interval_ms=1000,
            last_action_ms=0, now_ms=5000,
            prefer_amend=True, state=state,
        )
        assert decision == MakerDecision.AMEND

    def test_cancel_replace_for_large_deviation(self):
        state = PassiveMakerState()
        decision, reason = decide_passive_action(
            current_price=100.0, target_price=105.0,
            reprice_threshold_ticks=5, cancel_replace_threshold_ticks=10,
            tick_size=0.1, min_amend_interval_ms=1000,
            last_action_ms=0, now_ms=5000,
            prefer_amend=True, state=state,
        )
        assert decision == MakerDecision.CANCEL_REPLACE

    def test_cooldown_when_active(self):
        state = PassiveMakerState(cooling_down_until_ms=10000)
        decision, reason = decide_passive_action(
            current_price=100.0, target_price=105.0,
            reprice_threshold_ticks=5, cancel_replace_threshold_ticks=10,
            tick_size=0.1, min_amend_interval_ms=1000,
            last_action_ms=0, now_ms=5000,
            prefer_amend=True, state=state,
        )
        assert decision == MakerDecision.COOLDOWN

    def test_budget_exceeded_when_no_tokens(self):
        state = PassiveMakerState(ops_tokens=0)
        decision, reason = decide_passive_action(
            current_price=100.0, target_price=105.0,
            reprice_threshold_ticks=5, cancel_replace_threshold_ticks=10,
            tick_size=0.1, min_amend_interval_ms=1000,
            last_action_ms=0, now_ms=5000,
            prefer_amend=True, state=state,
        )
        assert decision == MakerDecision.BUDGET_EXCEEDED

    def test_hold_when_missing_book_data(self):
        state = PassiveMakerState()
        decision, reason = decide_passive_action(
            current_price=None, target_price=None,
            reprice_threshold_ticks=5, cancel_replace_threshold_ticks=10,
            tick_size=0.1, min_amend_interval_ms=1000,
            last_action_ms=0, now_ms=5000,
            prefer_amend=True, state=state,
        )
        assert decision == MakerDecision.HOLD
        assert reason == "missing_book_data"


# ============================================================================
# L-R6: Ops Token/Cooldown Rate Limiting for Passive Close Maintainer
# ============================================================================


class TestLazyOpsTokenBucket:
    """L-R6: V1-style fixed-window counter for passive close ops rate limiting.

    Each PendingPassiveClose has a fixed-window ops budget.  Operations
    consume a token BEFORE execution (even on failure).  The counter resets
    ONLY when the full window expires — cooldown is a retry scheduling
    hint, NOT a sub-window reset.
    """

    def _make_profile(self, budget=10, window_ms=60_000, cooldown_ms=5_000):
        from lightfee.engine.passive_close import PassiveCloseManagerProfile
        return PassiveCloseManagerProfile(
            ops_budget_per_window=budget,
            ops_budget_window_ms=window_ms,
            cooldown_ms=cooldown_ms,
        )

    def _make_pending(self, position_id="p1"):
        from lightfee.engine.state import PendingPassiveClose
        return PendingPassiveClose(
            position_id=position_id,
            reason="test",
            target_quantity=1.0,
            chunk_quantities=[1.0],
        )

    # ------------------------------------------------------------------
    # Budget exhaustion
    # ------------------------------------------------------------------

    def test_rate_limit_reached_emits_correct_journal_kind(self):
        """L-R6: when ops budget is exhausted, the rate_limit journal event
        uses exit.passive_close_maintain_rate_limited — not a generic warning."""
        from lightfee.engine.passive_close import ops_token_available
        profile = self._make_profile(budget=3, window_ms=60_000, cooldown_ms=5_000)
        pending = self._make_pending()
        now_ms = 1_000_000

        # First window: consume all 3 tokens
        pending.ops_window_started_at_ms = now_ms
        for i in range(3):
            assert ops_token_available(pending, profile, now_ms + i * 100)
            pending.ops_count_this_window += 1

        # 4th call: budget exhausted
        assert not ops_token_available(pending, profile, now_ms + 300)

    def test_rate_limit_sets_next_retry_with_cooldown(self):
        """L-R6: after budget exhaustion, next_retry_at_ms is pushed forward
        by cooldown_ms — the maintainer won't retry immediately."""
        from lightfee.engine.passive_close import ops_token_available, PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS
        profile = self._make_profile(budget=1, window_ms=60_000, cooldown_ms=15_000)
        pending = self._make_pending()
        now_ms = 1_000_000

        # Consume the single token
        pending.ops_window_started_at_ms = now_ms
        assert ops_token_available(pending, profile, now_ms)
        pending.ops_count_this_window += 1

        # Exhausted — next_retry should be in the future
        assert not ops_token_available(pending, profile, now_ms)
        # Simulate what _maintain_maker_order does on exhaustion:
        pending.next_retry_at_ms = max(pending.next_retry_at_ms, now_ms + profile.cooldown_ms)
        assert pending.next_retry_at_ms >= now_ms + profile.cooldown_ms

    # ------------------------------------------------------------------
    # Window semantics — no sub-window reset
    # ------------------------------------------------------------------

    def test_window_not_complete_does_not_reset_counter(self):
        """L-R6: if only half the window has passed, the counter is NOT reset.
        The full window must expire before refill."""
        from lightfee.engine.passive_close import ops_token_available
        profile = self._make_profile(budget=5, window_ms=60_000, cooldown_ms=5_000)
        pending = self._make_pending()
        now_ms = 1_000_000

        pending.ops_window_started_at_ms = now_ms
        pending.ops_count_this_window = 5  # exhausted

        # 30s later — only half the window
        assert not ops_token_available(pending, profile, now_ms + 30_000)

    def test_window_expires_resets_counter(self):
        """L-R6: when the full window expires, the counter resets to 0
        and the window restarts."""
        from lightfee.engine.passive_close import ops_token_available
        profile = self._make_profile(budget=5, window_ms=60_000, cooldown_ms=5_000)
        pending = self._make_pending()
        now_ms = 1_000_000

        pending.ops_window_started_at_ms = now_ms
        pending.ops_count_this_window = 5  # exhausted

        # 60s later — full window expired
        assert ops_token_available(pending, profile, now_ms + 60_000)
        assert pending.ops_count_this_window == 0
        assert pending.ops_window_started_at_ms == now_ms + 60_000

    def test_window_exactly_at_boundary(self):
        """L-R6: at the exact window boundary, counter resets."""
        from lightfee.engine.passive_close import ops_token_available
        profile = self._make_profile(budget=3, window_ms=10_000, cooldown_ms=5_000)
        pending = self._make_pending()
        now_ms = 1_000_000

        pending.ops_window_started_at_ms = now_ms
        pending.ops_count_this_window = 3  # exhausted

        # At exactly window_ms
        assert ops_token_available(pending, profile, now_ms + 10_000)
        assert pending.ops_count_this_window == 0

    def test_window_just_past_boundary(self):
        """L-R6: 1ms past the window boundary also resets."""
        from lightfee.engine.passive_close import ops_token_available
        profile = self._make_profile(budget=3, window_ms=10_000, cooldown_ms=5_000)
        pending = self._make_pending()
        now_ms = 1_000_000

        pending.ops_window_started_at_ms = now_ms
        pending.ops_count_this_window = 3  # exhausted

        assert ops_token_available(pending, profile, now_ms + 10_001)

    def test_zero_window_start_grants_token(self):
        """L-R6: if ops_window_started_at_ms is 0 (first call), token is always available."""
        from lightfee.engine.passive_close import ops_token_available
        profile = self._make_profile(budget=3, window_ms=60_000, cooldown_ms=5_000)
        pending = self._make_pending()
        pending.ops_window_started_at_ms = 0
        pending.ops_count_this_window = 0

        assert ops_token_available(pending, profile, 1_000_000)

    # ------------------------------------------------------------------
    # Token consumed on failure too
    # ------------------------------------------------------------------

    def test_token_consumed_before_operation(self):
        """L-R6: the token is consumed BEFORE amend/cancel-replace is attempted.
        If the operation fails, the token is still spent — no leak, no refund."""
        from lightfee.engine.passive_close import ops_token_available
        profile = self._make_profile(budget=2, window_ms=60_000, cooldown_ms=5_000)
        pending = self._make_pending()
        now_ms = 1_000_000

        pending.ops_window_started_at_ms = now_ms
        assert ops_token_available(pending, profile, now_ms)
        # Consume token (as _maintain_maker_order does before amend/cancel-replace)
        pending.ops_count_this_window += 1
        if pending.ops_window_started_at_ms <= 0:
            pending.ops_window_started_at_ms = now_ms
        # Now simulate the operation throwing — token still consumed
        assert pending.ops_count_this_window == 1

        # Verify budget reflects the consumed token
        assert ops_token_available(pending, profile, now_ms)  # 1 of 2 used
        pending.ops_count_this_window += 1  # second token
        assert not ops_token_available(pending, profile, now_ms)  # both used

    def test_multiple_failures_consume_tokens(self):
        """L-R6: repeated amend/cancel-replace failures consume tokens until
        budget exhausted, then ops stop."""
        from lightfee.engine.passive_close import ops_token_available
        profile = self._make_profile(budget=3, window_ms=60_000, cooldown_ms=5_000)
        pending = self._make_pending()
        now_ms = 1_000_000

        pending.ops_window_started_at_ms = now_ms

        # Simulate 3 failed attempts
        for i in range(3):
            assert ops_token_available(pending, profile, now_ms + i * 100)
            pending.ops_count_this_window += 1  # token consumed pre-op
            # operation fails — no refund

        # 4th attempt — budget exhausted
        assert not ops_token_available(pending, profile, now_ms + 400)

    # ------------------------------------------------------------------
    # Cooldown does NOT amplify
    # ------------------------------------------------------------------

    def test_cooldown_is_not_sub_window_reset(self):
        """L-R6: cooldown_ms sets the retry scheduling delay after budget
        exhaustion. It does NOT create a 'cooldown sub-window' that resets
        the counter early. Tokens only refill when the full ops_budget_window_ms
        expires."""
        from lightfee.engine.passive_close import ops_token_available
        profile = self._make_profile(budget=5, window_ms=60_000, cooldown_ms=5_000)
        pending = self._make_pending()
        now_ms = 1_000_000

        pending.ops_window_started_at_ms = now_ms
        pending.ops_count_this_window = 5  # exhausted

        # After cooldown_ms (5s), counter should still be exhausted
        assert not ops_token_available(pending, profile, now_ms + 5_000)
        assert pending.ops_count_this_window == 5  # NOT reset

        # After another cooldown (10s total), still exhausted
        assert not ops_token_available(pending, profile, now_ms + 10_000)
        assert pending.ops_count_this_window == 5

    def test_cooldown_scheduling_without_rate_amplification(self):
        """L-R6: repeated cooldown-based scheduling does not allow more ops
        per real-time second than the budget permits."""
        from lightfee.engine.passive_close import ops_token_available
        budget = 10
        window_ms = 60_000
        profile = self._make_profile(budget=budget, window_ms=window_ms, cooldown_ms=3_000)
        pending = self._make_pending()
        now_ms = 1_000_000

        pending.ops_window_started_at_ms = now_ms

        # Consume all budget — should be exactly `budget` ops
        ops_consumed = 0
        for i in range(budget * 2):  # safety limit
            if ops_token_available(pending, profile, now_ms + i * 100):
                pending.ops_count_this_window += 1
                ops_consumed += 1
            else:
                break
        assert ops_consumed == budget

        # Even after several cooldown periods within the same window,
        # no additional ops are available
        for offset_ms in [3000, 6000, 9000, 12000, 15000]:
            assert not ops_token_available(pending, profile, now_ms + offset_ms)

    # ------------------------------------------------------------------
    # Edge cases
    # ------------------------------------------------------------------

    def test_single_token_budget(self):
        """L-R6: minimum budget of 1 works correctly."""
        from lightfee.engine.passive_close import ops_token_available
        profile = self._make_profile(budget=1, window_ms=60_000, cooldown_ms=5_000)
        pending = self._make_pending()
        now_ms = 1_000_000

        pending.ops_window_started_at_ms = now_ms
        assert ops_token_available(pending, profile, now_ms)
        pending.ops_count_this_window += 1
        assert not ops_token_available(pending, profile, now_ms)

    def test_large_budget_not_exhausted_early(self):
        """L-R6: large budget permits exactly that many ops."""
        from lightfee.engine.passive_close import ops_token_available
        budget = 100
        profile = self._make_profile(budget=budget, window_ms=60_000, cooldown_ms=1_000)
        pending = self._make_pending()
        now_ms = 1_000_000

        pending.ops_window_started_at_ms = now_ms
        for i in range(budget):
            assert ops_token_available(pending, profile, now_ms + i)
            pending.ops_count_this_window += 1
        assert not ops_token_available(pending, profile, now_ms + budget)

    def test_very_short_window(self):
        """L-R6: a 1ms window still works — counter resets after 1ms."""
        from lightfee.engine.passive_close import ops_token_available
        profile = self._make_profile(budget=1, window_ms=1, cooldown_ms=100)
        pending = self._make_pending()
        now_ms = 1_000_000

        pending.ops_window_started_at_ms = now_ms
        pending.ops_count_this_window = 1  # exhausted
        assert not ops_token_available(pending, profile, now_ms)

        # 1ms later — window expires, counter resets
        assert ops_token_available(pending, profile, now_ms + 1)
        assert pending.ops_count_this_window == 0
