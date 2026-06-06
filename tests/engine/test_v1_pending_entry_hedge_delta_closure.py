from __future__ import annotations

import pytest

from lightfee.config.schema import StrategyConfig
from lightfee.core.domain import PassiveOrderState, Side, Venue
from lightfee.engine.state import (
    PendingEntry,
    PendingEntryPassivePhaseState,
    PendingEntryRemainderSlice,
    PendingPassiveOrder,
)


def _strategy(**overrides) -> StrategyConfig:
    strategy = StrategyConfig()
    strategy.maker_entry_progress_poll_ms = 250
    strategy.maker_min_notional_accumulation_attempts = 3
    strategy.maker_hedge_soft_deadline_ms = 800
    strategy.maker_hedge_deadline_ms = 2_500
    strategy.passive_small_fill_buffer_notional_quote = 25.0
    strategy.passive_small_fill_buffer_max_wait_ms = 1_500
    for name, value in overrides.items():
        setattr(strategy, name, value)
    return strategy


def _pending_entry(**overrides) -> PendingEntry:
    values = {
        "pending_id": "entry-v1-hedge-delta",
        "symbol": "BTCUSDT",
        "long_venue": Venue.BINANCE,
        "short_venue": Venue.OKX,
        "target_quantity": 2.0,
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
            client_order_id="maker-cid-1",
            limit_price=20.0,
            target_quantity=2.0,
            accepted_at_ms=1_000,
            timeout_at_ms=10_000,
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
        "maker_remainder_slices": [
            PendingEntryRemainderSlice(
                quantity=0.5,
                notional_quote=10.0,
                fill_at_ms=1_100,
            )
        ],
    }
    values.update(overrides)
    return PendingEntry(**values)


def test_v1_releasable_hedge_quantity_blocks_sub_chunk_delta():
    from lightfee.engine.pending_entry_hedge_delta import releasable_hedge_quantity

    assert releasable_hedge_quantity(0.49, 0.5) == pytest.approx(0.0)


def test_v1_releasable_hedge_quantity_rounds_down_to_whole_chunks():
    from lightfee.engine.pending_entry_hedge_delta import releasable_hedge_quantity

    assert releasable_hedge_quantity(1.24, 0.5) == pytest.approx(1.0)


def test_v1_small_fill_below_chunk_buffers_when_not_terminal_or_canceling():
    from lightfee.engine.pending_entry_hedge_delta import (
        PendingEntryHedgeabilityPlan,
        decide_pending_entry_hedge_delta_pre_submit,
    )

    pending = _pending_entry()

    decision = decide_pending_entry_hedge_delta_pre_submit(
        pending,
        strategy=_strategy(),
        hedgeability_plan=PendingEntryHedgeabilityPlan(min_hedgeable_chunk=1.0),
        normalized_quantity=None,
        min_notional_violation=None,
        now_ms=2_000,
        maker_progress_updated=True,
    )

    assert decision.kind == "buffer_small_fill"
    assert decision.event == "execution.pending_entry_hedge_chunk_buffering"
    assert decision.releasable_quantity == pytest.approx(0.0)
    assert pending.next_progress_poll_ms == 2_250
    assert pending.phase_state is not None
    assert pending.phase_state.small_fill_min_notional_attempts == 0
    assert decision.evidence["adapter_calls"] == []


def test_v1_terminal_or_canceling_small_fill_counts_attempt_immediately():
    from lightfee.engine.pending_entry_hedge_delta import (
        PendingEntryHedgeabilityPlan,
        decide_pending_entry_hedge_delta_pre_submit,
    )

    pending = _pending_entry()
    assert pending.passive_order is not None
    pending.passive_order.cancel_requested_at_ms = 2_000

    decision = decide_pending_entry_hedge_delta_pre_submit(
        pending,
        strategy=_strategy(),
        hedgeability_plan=PendingEntryHedgeabilityPlan(min_hedgeable_chunk=1.0),
        normalized_quantity=0.0,
        min_notional_violation=None,
        now_ms=2_000,
        maker_progress_updated=False,
    )

    assert decision.kind == "wait_min_notional_accumulation"
    assert decision.event == "execution.min_notional_accumulating"
    assert decision.evidence["terminal_or_canceling"] is True
    assert decision.evidence["attempt"] == 1
    assert pending.phase_state is not None
    assert pending.phase_state.small_fill_min_notional_attempts == 1
    assert pending.phase_state.hedge_deadline_at_ms is None
    assert decision.evidence["adapter_calls"] == []


def test_v1_min_notional_accumulation_clears_hedge_deadline_and_keeps_pending():
    from lightfee.engine.pending_entry_hedge_delta import (
        PendingEntryHedgeabilityPlan,
        decide_pending_entry_hedge_delta_pre_submit,
    )

    pending = _pending_entry()
    assert pending.phase_state is not None
    pending.phase_state.hedge_deadline_at_ms = 9_999

    decision = decide_pending_entry_hedge_delta_pre_submit(
        pending,
        strategy=_strategy(),
        hedgeability_plan=PendingEntryHedgeabilityPlan(min_hedgeable_chunk=0.1),
        normalized_quantity=0.5,
        min_notional_violation=(10.0, 25.0),
        now_ms=2_000,
        maker_progress_updated=True,
    )

    assert decision.kind == "wait_min_notional_accumulation"
    assert decision.event == "execution.min_notional_accumulating"
    assert decision.next_progress_poll_ms == 2_250
    assert pending.phase_state.small_fill_min_notional_attempts == 1
    assert pending.phase_state.hedge_deadline_at_ms is None
    assert pending.next_progress_poll_ms == 2_250
    assert decision.evidence["adapter_calls"] == []


def test_v1_min_notional_attempt_exhaustion_returns_abort_and_flatten_action():
    from lightfee.engine.pending_entry_hedge_delta import (
        PendingEntryHedgeabilityPlan,
        decide_pending_entry_hedge_delta_pre_submit,
    )

    pending = _pending_entry()
    assert pending.phase_state is not None
    pending.phase_state.small_fill_min_notional_attempts = 2

    decision = decide_pending_entry_hedge_delta_pre_submit(
        pending,
        strategy=_strategy(maker_min_notional_accumulation_attempts=3),
        hedgeability_plan=PendingEntryHedgeabilityPlan(min_hedgeable_chunk=0.1),
        normalized_quantity=0.5,
        min_notional_violation=(10.0, 25.0),
        now_ms=2_000,
        maker_progress_updated=True,
    )

    assert decision.kind == "abort_and_flatten"
    assert decision.event == "execution.min_notional_abort_and_flatten"
    assert decision.evidence["attempt"] == 3
    assert pending.phase_state.small_fill_min_notional_attempts == 3
    assert pending.phase_state.hedge_deadline_at_ms is None
    assert decision.evidence["adapter_calls"] == []


def test_v1_passive_small_fill_buffer_waits_until_deadline():
    from lightfee.engine.pending_entry_hedge_delta import (
        PendingEntryHedgeabilityPlan,
        decide_pending_entry_hedge_delta_pre_submit,
    )

    pending = _pending_entry(
        maker_remainder_slices=[
            PendingEntryRemainderSlice(quantity=0.5, notional_quote=10.0, fill_at_ms=1_100),
        ],
    )

    decision = decide_pending_entry_hedge_delta_pre_submit(
        pending,
        strategy=_strategy(),
        hedgeability_plan=PendingEntryHedgeabilityPlan(min_hedgeable_chunk=0.1),
        normalized_quantity=0.5,
        min_notional_violation=None,
        now_ms=2_000,
        maker_progress_updated=True,
    )

    assert decision.kind == "wait_passive_small_fill_buffer"
    assert decision.event == "execution.passive_small_fill_buffering"
    assert decision.next_progress_poll_ms == 2_250
    assert pending.next_progress_poll_ms == 2_250
    assert pending.phase_state is not None
    assert pending.phase_state.small_fill_min_notional_attempts == 0
    assert decision.evidence["adapter_calls"] == []


def test_v1_passive_small_fill_buffer_expiry_releases_submit_action():
    from lightfee.engine.pending_entry_hedge_delta import (
        PendingEntryHedgeabilityPlan,
        decide_pending_entry_hedge_delta_pre_submit,
    )

    pending = _pending_entry(
        maker_remainder_slices=[
            PendingEntryRemainderSlice(quantity=0.5, notional_quote=10.0, fill_at_ms=1_100),
        ],
    )

    decision = decide_pending_entry_hedge_delta_pre_submit(
        pending,
        strategy=_strategy(),
        hedgeability_plan=PendingEntryHedgeabilityPlan(min_hedgeable_chunk=0.1),
        normalized_quantity=0.5,
        min_notional_violation=None,
        now_ms=2_700,
        maker_progress_updated=True,
    )

    assert decision.kind == "submit_hedge"
    assert decision.event == "execution.passive_small_fill_buffer_expired"
    assert decision.normalized_quantity == pytest.approx(0.5)
    assert decision.evidence["buffered_elapsed_ms"] == 1_600
    assert decision.evidence["adapter_calls"] == []


def test_v1_entry_hedge_deadline_extends_for_fresh_progressing_small_hedge():
    from lightfee.engine.pending_entry_hedge_delta import (
        HedgeDeadlineStatus,
        adaptive_entry_hedge_deadline_decision,
    )

    decision = adaptive_entry_hedge_deadline_decision(
        hedge_elapsed_ms=2_900,
        base_soft_deadline_ms=800,
        base_hard_deadline_ms=2_500,
        hedge_notional_quote=40.0,
        quote_fresh=True,
        has_execution_progress=True,
        reconciled=False,
    )

    assert decision.status == HedgeDeadlineStatus.SOFT_BREACHED
    assert decision.effective_hard_deadline_ms == 3_550
    assert decision.effective_soft_deadline_ms == 1_325


def test_v1_reconciled_without_progress_does_not_extend_deadline():
    from lightfee.engine.pending_entry_hedge_delta import (
        HedgeDeadlineStatus,
        adaptive_entry_hedge_deadline_decision,
    )

    decision = adaptive_entry_hedge_deadline_decision(
        hedge_elapsed_ms=2_501,
        base_soft_deadline_ms=800,
        base_hard_deadline_ms=2_500,
        hedge_notional_quote=40.0,
        quote_fresh=True,
        has_execution_progress=False,
        reconciled=True,
    )

    assert decision.status == HedgeDeadlineStatus.HARD_BREACHED
    assert decision.effective_hard_deadline_ms == 2_500
    assert decision.effective_soft_deadline_ms == 800
