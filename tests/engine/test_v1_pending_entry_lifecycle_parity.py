from __future__ import annotations

from types import SimpleNamespace

import pytest

from lightfee.config.schema import StrategyConfig
from lightfee.core.domain import PassiveOrderState, Side, Venue
from lightfee.engine.state import (
    PendingEntry,
    PendingEntryPassivePhaseState,
    PendingEntryRemainderSlice,
    PendingPassiveOrder,
)


def _pending_entry(**overrides) -> PendingEntry:
    values = {
        "pending_id": "entry-v1-lifecycle",
        "symbol": "BTCUSDT",
        "long_venue": Venue.BINANCE,
        "short_venue": Venue.OKX,
        "target_quantity": 3.0,
        "long_side": Side.BUY,
        "short_side": Side.SELL,
        "created_at_ms": 1_000,
        "maker_leg": "long",
        "maker_leg_filled": 0.0,
        "hedge_leg_filled": 0.0,
        "maker_price": 20.0,
        "maker_fill_price": 20.0,
        "passive_order": PendingPassiveOrder(
            order_id="maker-1",
            client_order_id="cid-maker-1",
            limit_price=20.0,
            target_quantity=3.0,
            accepted_at_ms=1_000,
            timeout_at_ms=7_000,
            last_progress_state=PassiveOrderState.OPEN,
        ),
        "phase_state": PendingEntryPassivePhaseState(
            execution_kind="entry",
            preferred_maker_leg="long",
            active_maker_leg="long",
            phase="high_slippage_maker",
            cycle_attempt=1,
            phase_started_at_ms=1_000,
            cycle_started_at_ms=1_000,
        ),
    }
    values.update(overrides)
    return PendingEntry(**values)


def test_v1_pending_entry_hedge_remainder_fifo_consumes_slices():
    """V1: PendingEntryHedge remainder FIFO consumes maker slices in order."""

    pending = _pending_entry(
        maker_leg_filled=3.0,
        hedge_leg_filled=0.0,
        maker_remainder_slices=[
            PendingEntryRemainderSlice(quantity=1.0, notional_quote=10.0, fill_at_ms=1001),
            PendingEntryRemainderSlice(quantity=2.0, notional_quote=40.0, fill_at_ms=1002),
        ],
    )

    assert pending.missing_hedge_quantity() == pytest.approx(3.0)
    assert pending.consume_hedge_quantity_fifo(1.5) == pytest.approx(1.5)
    assert pending.missing_hedge_quantity() == pytest.approx(1.5)
    assert len(pending.maker_remainder_slices) == 1
    assert pending.maker_remainder_slices[0].quantity == pytest.approx(1.5)
    assert pending.maker_remainder_slices[0].notional_quote == pytest.approx(30.0)


def test_v1_handle_zero_fill_records_delay_before_repost():
    """V1: handle_pending_entry_zero_fill_completion records delay first."""

    from lightfee.engine.pending_entry_lifecycle import record_pending_entry_zero_fill_cycle

    strategy = StrategyConfig()
    strategy.maker_cycle_retry_delays_ms = [500, 1000]
    pending = _pending_entry()

    delay_ms = record_pending_entry_zero_fill_cycle(pending, strategy, now_ms=2_000)

    assert delay_ms == 500
    assert pending.phase_state is not None
    assert pending.phase_state.zero_fill_cycles_in_phase == 1
    assert pending.phase_state.cycle_attempt == 1
    assert pending.phase_state.next_cycle_delay_ms == 500
    assert pending.phase_state.hedge_deadline_at_ms is None
    assert pending.repost_attempt_count == 1
    assert pending.passive_attempt_count == 0
    assert pending.next_progress_poll_ms == 2_500


def test_v1_handle_zero_fill_switches_high_slippage_to_low_slippage_after_budget():
    """V1: handle_pending_entry_zero_fill_completion switches high to low phase."""

    from lightfee.engine.pending_entry_lifecycle import advance_pending_entry_zero_fill_phase

    strategy = StrategyConfig()
    strategy.pending_entry_phase_zero_fill_budget = 2
    pending = _pending_entry()
    assert pending.phase_state is not None
    pending.phase_state.zero_fill_cycles_in_phase = 2
    pending.phase_state.cycle_attempt = 2
    pending.repost_attempt_count = 2
    pending.passive_attempt_count = 1

    action = advance_pending_entry_zero_fill_phase(
        pending,
        strategy,
        now_ms=3_000,
        candidate=SimpleNamespace(blocked=False, blocked_reasons=[]),
    )

    assert action.kind == "submit_next_cycle"
    assert action.reason == "phase_switched_to_low_slippage_maker"
    assert pending.maker_leg == "short"
    assert pending.phase_state.phase == "low_slippage_maker"
    assert pending.phase_state.active_maker_leg == "short"
    assert pending.phase_state.zero_fill_cycles_in_phase == 0
    assert pending.phase_state.cycle_attempt == 0
    assert pending.phase_state.next_cycle_delay_ms is None
    assert pending.phase_state.hedge_deadline_at_ms is None
    assert pending.phase_state.phase_started_at_ms == 3_000
    assert pending.repost_attempt_count == 0
    assert pending.passive_attempt_count == 0


def test_v1_handle_zero_fill_same_phase_ignores_legacy_global_repost_count():
    """V1: same-phase zero-fill repost is not cut off by legacy repost_count."""

    from lightfee.engine.pending_entry_lifecycle import advance_pending_entry_zero_fill_phase

    strategy = StrategyConfig()
    strategy.pending_entry_phase_zero_fill_budget = 2
    strategy.maker_entry_max_reposts = 1
    pending = _pending_entry(repost_count=99)
    assert pending.phase_state is not None
    pending.phase_state.zero_fill_cycles_in_phase = 1
    pending.phase_state.cycle_attempt = 1
    pending.repost_attempt_count = 1

    action = advance_pending_entry_zero_fill_phase(
        pending,
        strategy,
        now_ms=3_000,
        candidate=SimpleNamespace(blocked=False, blocked_reasons=[]),
    )

    assert action.kind == "submit_next_cycle"
    assert action.reason == ""
    assert pending.phase_state.phase == "high_slippage_maker"
    assert pending.repost_count == 99


def test_v1_low_slippage_maker_exhaustion_transitions_to_dual_taker():
    """V1: low_slippage_maker zero-fill exhaustion arms dual_taker."""

    from lightfee.engine.pending_entry_lifecycle import advance_pending_entry_zero_fill_phase

    strategy = StrategyConfig()
    strategy.pending_entry_phase_zero_fill_budget = 2
    pending = _pending_entry()
    assert pending.phase_state is not None
    pending.phase_state.phase = "low_slippage_maker"
    pending.phase_state.zero_fill_cycles_in_phase = 2

    action = advance_pending_entry_zero_fill_phase(
        pending,
        strategy,
        now_ms=4_000,
        candidate=SimpleNamespace(blocked=False, blocked_reasons=[]),
    )

    assert action.kind == "trigger_dual_taker"
    assert action.reason == "maker_entry_dual_taker_after_phase_exhaustion"
    assert pending.phase_state.phase == "dual_taker"
    assert pending.phase_state.next_cycle_delay_ms is None
    assert pending.phase_state.hedge_deadline_at_ms is None


def test_v1_apply_pending_entry_passive_progress_pushes_remainder_slice():
    """V1: apply_pending_entry_passive_progress positive delta creates remainder."""

    from lightfee.engine.pending_entry_lifecycle import apply_pending_entry_passive_progress

    pending = _pending_entry(maker_leg_filled=1.0, maker_fill_price=10.0)
    progress = SimpleNamespace(
        state=PassiveOrderState.PARTIALLY_FILLED,
        cumulative_quantity=2.5,
        average_price=12.0,
        observed_at_ms=2_500,
    )

    changed = apply_pending_entry_passive_progress(pending, progress)

    assert changed is True
    assert pending.maker_leg_filled == pytest.approx(2.5)
    assert pending.maker_fill_price == pytest.approx(12.0)
    assert len(pending.maker_remainder_slices) == 1
    assert pending.maker_remainder_slices[0].quantity == pytest.approx(1.5)
    assert pending.maker_remainder_slices[0].notional_quote == pytest.approx(18.0)
    assert pending.maker_remainder_slices[0].fill_at_ms == 2_500
    assert pending.passive_order is not None
    assert pending.passive_order.last_progress_state == PassiveOrderState.PARTIALLY_FILLED


def test_v1_terminal_taker_fallback_skips_blocked_frozen_candidate():
    """V1: try_terminal_taker_fallback skips non-tradeable frozen candidate."""

    from lightfee.engine.pending_entry_lifecycle import decide_terminal_taker_fallback

    action = decide_terminal_taker_fallback(
        candidate={"blocked": True, "blocked_reasons": ["risk_budget"]},
        terminal_reason="maker_entry_dual_taker_after_phase_exhaustion",
    )

    assert action.kind == "skip_fallback"
    assert action.reason == "candidate_not_tradeable_after_terminal_reprice"
    assert action.evidence["blocked_reasons"] == ["risk_budget"]


def test_v1_terminal_taker_fallback_without_frozen_candidate_is_explicit_skip():
    """V1 no-frozen rediscovery is out-of-scope, never synthetic tradeable."""

    from lightfee.engine.pending_entry_lifecycle import (
        candidate_for_terminal_taker_fallback,
        terminal_recheck_is_tradeable,
    )

    pending = _pending_entry(frozen_candidate=None)

    candidate = candidate_for_terminal_taker_fallback(pending)
    action = terminal_recheck_is_tradeable(candidate)

    assert candidate is None
    assert action.kind == "blocked"
    assert action.reason == "candidate_not_tradeable_after_terminal_reprice"


def test_v1_force_standard_terminal_fallback_decision_defers_to_runtime_open():
    """V1: ForceStandard open is a runtime boundary after tradeable decision."""

    from lightfee.engine.pending_entry_lifecycle import decide_terminal_taker_fallback

    action = decide_terminal_taker_fallback(
        candidate={"blocked": False, "blocked_reasons": [], "entry_notional_quote": 50.0},
        terminal_reason="maker_entry_dual_taker_after_phase_exhaustion",
    )

    assert action.kind == "fallback_to_taker"
    assert action.reason == "maker_entry_terminal_zero_fill"
    assert action.evidence["terminal_reason"] == (
        "maker_entry_dual_taker_after_phase_exhaustion"
    )


def test_v1_submit_pending_entry_passive_cycle_accept_resets_cycle_state():
    """V1: submit_pending_entry_passive_cycle accepted ack resets delay/deadline."""

    from lightfee.engine.pending_entry_lifecycle import (
        note_pending_entry_passive_cycle_accepted,
    )

    pending = _pending_entry()
    assert pending.phase_state is not None
    pending.phase_state.zero_fill_cycles_in_phase = 1
    pending.phase_state.next_cycle_delay_ms = 500
    pending.phase_state.hedge_deadline_at_ms = 2_500

    note_pending_entry_passive_cycle_accepted(
        pending,
        order_id="maker-2",
        client_order_id="cid-maker-2",
        accepted_at_ms=2_200,
        limit_price=19.5,
        target_quantity=2.5,
        passive_attempt_count=2,
        rest_timeout_ms=6_000,
    )

    assert pending.passive_order is not None
    assert pending.passive_order.order_id == "maker-2"
    assert pending.passive_order.client_order_id == "cid-maker-2"
    assert pending.passive_order.limit_price == 19.5
    assert pending.passive_order.target_quantity == 2.5
    assert pending.passive_order.accepted_at_ms == 2_200
    assert pending.passive_order.timeout_at_ms == 8_200
    assert pending.phase_state.cycle_attempt == 2
    assert pending.phase_state.next_cycle_delay_ms is None
    assert pending.phase_state.hedge_deadline_at_ms is None
    assert pending.phase_state.cycle_started_at_ms == 2_200
    assert pending.repost_attempt_count == 1
    assert pending.passive_attempt_count == 2
    assert pending.passive_ops_total == 1


def test_v1_submit_pending_entry_passive_cycle_finalizes_depleted_quantity():
    """V1: submit_pending_entry_passive_cycle finalizes depleted quantity."""

    from lightfee.engine.pending_entry_lifecycle import prepare_pending_entry_passive_cycle

    pending = _pending_entry(target_quantity=3.0, maker_leg_filled=3.0)

    action = prepare_pending_entry_passive_cycle(pending, normalized_quantity=0.0)

    assert action.kind == "finalized"
    assert action.reason == "remaining_quantity_depleted"
    assert action.evidence["remaining_quantity"] == pytest.approx(0.0)


def test_v1_submit_pending_entry_passive_cycle_finalizes_below_minimum_quantity():
    """V1: submit_pending_entry_passive_cycle finalizes below-minimum normalized size."""

    from lightfee.engine.pending_entry_lifecycle import prepare_pending_entry_passive_cycle

    pending = _pending_entry(target_quantity=3.0, maker_leg_filled=1.0)

    action = prepare_pending_entry_passive_cycle(pending, normalized_quantity=0.0)

    assert action.kind == "finalized"
    assert action.reason == "remaining_quantity_below_minimum"
    assert action.evidence["remaining_quantity"] == pytest.approx(2.0)
    assert action.evidence["normalized_quantity"] == pytest.approx(0.0)


def test_v1_try_repost_pending_entry_remainder_respects_repost_limit():
    """V1: try_repost_pending_entry_remainder stops at maker_entry_max_reposts."""

    from lightfee.engine.pending_entry_lifecycle import prepare_pending_entry_remainder_repost

    strategy = StrategyConfig()
    strategy.maker_entry_max_reposts = 2
    pending = _pending_entry(target_quantity=3.0, maker_leg_filled=1.0, repost_attempt_count=2)

    action = prepare_pending_entry_remainder_repost(
        pending,
        strategy,
        normalized_quantity=2.0,
    )

    assert action.kind == "finalized"
    assert action.reason == "max_reposts_reached"
    assert action.evidence["remaining_quantity"] == pytest.approx(2.0)


def test_v1_try_repost_pending_entry_remainder_respects_passive_attempt_limit():
    """V1: try_repost_pending_entry_remainder stops at passive attempt limit."""

    from lightfee.engine.pending_entry_lifecycle import prepare_pending_entry_remainder_repost

    strategy = StrategyConfig()
    strategy.maker_entry_max_reposts = 5
    pending = _pending_entry(
        target_quantity=3.0,
        maker_leg_filled=1.0,
        repost_attempt_count=1,
        passive_attempt_count=3,
    )

    action = prepare_pending_entry_remainder_repost(
        pending,
        strategy,
        normalized_quantity=2.0,
        passive_attempt_limit=3,
    )

    assert action.kind == "finalized"
    assert action.reason == "max_passive_attempts_reached"
    assert action.evidence["remaining_quantity"] == pytest.approx(2.0)


def test_v1_try_repost_pending_entry_remainder_accept_increments_repost_count():
    """V1: try_repost_pending_entry_remainder accepted ack increments repost count."""

    from lightfee.engine.pending_entry_lifecycle import (
        note_pending_entry_remainder_repost_accepted,
    )

    pending = _pending_entry(target_quantity=3.0, maker_leg_filled=1.0)
    pending.repost_attempt_count = 1
    pending.passive_attempt_count = 1

    note_pending_entry_remainder_repost_accepted(
        pending,
        order_id="maker-repost-2",
        client_order_id="cid-maker-repost-2",
        accepted_at_ms=4_000,
        limit_price=20.5,
        target_quantity=2.0,
        passive_attempt_count=2,
        rest_timeout_ms=6_000,
    )

    assert pending.repost_attempt_count == 2
    assert pending.passive_attempt_count == 2
    assert pending.passive_order is not None
    assert pending.passive_order.order_id == "maker-repost-2"
    assert pending.passive_order.timeout_at_ms == 10_000
