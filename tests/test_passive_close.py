"""V1 passive close executor focused tests — real path-driven coverage.

Rust references:
- src/engine/exit.rs: start_pending_passive_close (line 1603)
- src/engine/exit.rs: drive_pending_passive_close (line 1710)
- src/engine/exit.rs: maintain_passive_close_order (line 860)
- src/engine/exit.rs: process_pending_passive_closes (line 2987)
- src/engine/entry.rs: align_passive_price_to_tick (line 4646)
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import (
    OrderFill,
    OrderRequest,
    PassiveOrderAck,
    PassiveOrderAmendRequest,
    PassiveOrderProgress,
    PassiveOrderState,
    PositionSnapshot,
    Side,
    TimeInForce,
    Venue,
)
from lightfee.engine.close_executor import CloseExecutionLeg
from lightfee.engine.exit import CloseExecution
from lightfee.engine.passive_close import (
    PASSIVE_CLOSE_MAX_MISSING_L2_TICK_FAILURES,
    PASSIVE_CLOSE_MAX_MAKER_SUBMIT_FAILURES,
    PASSIVE_CLOSE_MAX_ZERO_FILL_CYCLES,
    PASSIVE_CLOSE_PROGRESS_POLL_INTERVAL_MS,
    PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS,
    HedgeDeltaResult,
    PassiveCloseConfig,
    PassiveCloseExecutor,
    PassiveCloseManagerProfile,
    PassiveManagerDecisionKind,
)
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.engine.state import (
    ActiveMakerLeg,
    EngineState,
    OpenPosition,
    PassiveExecutionPhase,
    PassivePhaseState,
    PendingPassiveClose,
    PendingPassiveLegFill,
    PersistedCloseExecutionLeg,
)
from lightfee.persistence.journal import Journal


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _open_journal():
    d = tempfile.mkdtemp()
    j = Journal(Path(d) / "test.log")
    j.open()
    return j


def _make_position(**overrides) -> OpenPosition:
    defaults = dict(
        position_id="p001",
        symbol="BTCUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.OKX,
        long_quantity=0.01,
        short_quantity=0.01,
        long_entry_price=50000.0,
        short_entry_price=50000.0,
        opened_at_ms=1000000,
        matched_quantity=0.01,
    )
    defaults.update(overrides)
    return OpenPosition(**defaults)


def _make_order_fill(
    venue=Venue.BINANCE, symbol="BTCUSDT", side=Side.BUY,
    quantity=0.01, price=50000.0, order_id="f001", fee_quote=2.5,
) -> OrderFill:
    return OrderFill(
        venue=venue, symbol=symbol, side=side,
        quantity=quantity, price=price,
        order_id=order_id, fee_quote=fee_quote,
        filled_at_ms=1000,
    )


def _make_passive_ack(
    venue=Venue.BINANCE, symbol="BTCUSDT", side=Side.SELL,
    order_id="pa001", client_order_id="", price=50000.0, quantity=0.01,
) -> PassiveOrderAck:
    return PassiveOrderAck(
        venue=venue, symbol=symbol, side=side,
        order_id=order_id, client_order_id=client_order_id,
        price=price, quantity=quantity,
        accepted_at_ms=1000, state=PassiveOrderState.OPEN,
    )


def _make_passive_progress(
    venue=Venue.BINANCE, symbol="BTCUSDT", side=Side.SELL,
    order_id="pa001", client_order_id="",
    cumulative_quantity=0.005, average_price=50001.0,
    fee_quote=1.25, state=PassiveOrderState.PARTIALLY_FILLED,
) -> PassiveOrderProgress:
    return PassiveOrderProgress(
        venue=venue, symbol=symbol, side=side,
        order_id=order_id, client_order_id=client_order_id,
        cumulative_quantity=cumulative_quantity,
        average_price=average_price,
        fee_quote=fee_quote,
        last_fill_time_ms=2000,
        state=state,
        observed_at_ms=2000,
    )


def _mock_adapter_with_tick(venue=Venue.BINANCE, tick=0.01):
    """Create a mock adapter with price_tick_size returning the given tick."""
    adapter = MagicMock(spec=VenueAdapter)
    adapter.venue = venue
    adapter.price_tick_size = lambda symbol=None, _tick=tick: _tick
    return adapter


def _mock_adapter_passive_ok(venue=Venue.BINANCE):
    """Create a mock adapter supporting passive order submission."""
    adapter = _mock_adapter_with_tick(venue)
    adapter.submit_passive_order = AsyncMock(return_value=_make_passive_ack(
        venue=venue, order_id="oid-1", client_order_id="cid-1", price=50000.0,
    ))
    adapter.query_passive_order_progress = AsyncMock(return_value=None)
    adapter.place_order = AsyncMock(return_value=_make_order_fill(venue=venue))
    return adapter


# ---------------------------------------------------------------------------
# Passive contract + progress tests (kept from original — testing real behavior)
# ---------------------------------------------------------------------------

class TestPassiveOrderContract:
    def test_passive_ack_attributes(self):
        ack = PassiveOrderAck(
            venue=Venue.BINANCE, symbol="BTCUSDT", side=Side.SELL,
            order_id="abc123", client_order_id="cid-1",
            price=50000.0, quantity=0.01,
            accepted_at_ms=1000, state=PassiveOrderState.OPEN,
        )
        assert ack.order_id == "abc123"
        assert ack.client_order_id == "cid-1"
        assert ack.state == PassiveOrderState.OPEN

    def test_price_tick_size_from_spec_positive(self):
        from lightfee.venues.specs import binance_spec
        tick = binance_spec().price_tick
        assert tick > 0

    def test_passive_progress_fields(self):
        prog = PassiveOrderProgress(
            venue=Venue.BINANCE, symbol="BTCUSDT", side=Side.SELL,
            order_id="o1", client_order_id="c1",
            cumulative_quantity=0.005, average_price=50100.0,
            fee_quote=1.5, last_fill_time_ms=5000,
            state=PassiveOrderState.PARTIALLY_FILLED,
            observed_at_ms=5000,
        )
        assert prog.cumulative_quantity == 0.005
        assert prog.average_price == 50100.0
        assert prog.state == PassiveOrderState.PARTIALLY_FILLED


class TestPassiveProgressAndHedge:
    def test_apply_maker_progress_updates_fill(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        pending = PendingPassiveClose(
            position_id="p001",
            reason="funding_capture",
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
        )
        pending.maker_fill = PendingPassiveLegFill()

        progress = _make_passive_progress(
            cumulative_quantity=0.003, average_price=50100.0,
            state=PassiveOrderState.PARTIALLY_FILLED,
        )
        executor._apply_maker_progress(pending, progress, 1000)

        assert pending.maker_fill.quantity == 0.003
        assert pending.maker_fill.average_price == 50100.0
        assert len(pending.long_legs) == 1
        assert pending.long_legs[0].fill.quantity == 0.003
        assert pending.long_legs[0].fill.side == Side.SELL

    def test_apply_maker_progress_accumulates(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        pending = PendingPassiveClose(
            position_id="p001",
            reason="funding_capture",
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.SHORT,
            ),
        )
        pending.maker_fill = PendingPassiveLegFill()

        p1 = _make_passive_progress(
            cumulative_quantity=0.003, average_price=50100.0, side=Side.BUY,
        )
        executor._apply_maker_progress(pending, p1, 1000)
        assert pending.maker_fill.quantity == 0.003
        assert len(pending.short_legs) == 1

        p2 = _make_passive_progress(
            cumulative_quantity=0.007, average_price=50200.0, side=Side.BUY,
        )
        executor._apply_maker_progress(pending, p2, 2000)
        assert pending.maker_fill.quantity == 0.007
        assert len(pending.short_legs) == 2
        assert pending.short_legs[1].fill.quantity == 0.004

    def test_terminal_filled_persists_full_maker_leg(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        pending = PendingPassiveClose(
            position_id="p001",
            reason="funding_capture",
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
        )
        pending.maker_fill = PendingPassiveLegFill()

        progress = _make_passive_progress(
            cumulative_quantity=0.01, average_price=50150.0,
            state=PassiveOrderState.FILLED,
        )
        executor._apply_maker_progress(pending, progress, 5000)

        assert pending.maker_fill.quantity == 0.01
        assert len(pending.long_legs) == 1
        assert pending.long_legs[0].fill.quantity == 0.01
        assert pending.long_legs[0].fill.price == 50150.0

    def test_no_duplicate_on_unchanged_cumulative(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        pending = PendingPassiveClose(
            position_id="p001",
            reason="funding_capture",
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
        )
        pending.maker_fill = PendingPassiveLegFill(quantity=0.005, average_price=50100.0)

        progress = _make_passive_progress(cumulative_quantity=0.005)
        executor._apply_maker_progress(pending, progress, 1000)
        assert len(pending.long_legs) == 0


# ---------------------------------------------------------------------------
# Repricing L2 + tick
# ---------------------------------------------------------------------------

class TestPassiveRepricing:
    def test_l2_mid_from_injected_resolver(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 50123.5)
        mid = executor._resolve_local_l2_mid(Venue.BINANCE, "BTCUSDT")
        assert mid == 50123.5

    def test_l2_mid_zero_without_resolver_or_adapter(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        mid = executor._resolve_local_l2_mid(Venue.BINANCE, "BTCUSDT")
        assert mid == 0.0

    def test_tick_size_from_spec(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        tick = executor._get_tick_size(Venue.BINANCE, "BTCUSDT")
        assert tick > 0


# ---------------------------------------------------------------------------
# Fallback DUAL_TAKER
# ---------------------------------------------------------------------------

class TestFallbackToAggressive:
    def test_needs_aggressive_fallback_dual_taker(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        pending = PendingPassiveClose(
            position_id="p001",
            reason="funding_capture",
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(phase=PassiveExecutionPhase.DUAL_TAKER),
        )
        assert executor.needs_aggressive_fallback(pending) is True

    def test_needs_aggressive_fallback_false_for_maker(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        pending = PendingPassiveClose(
            position_id="p001",
            reason="funding_capture",
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER),
        )
        assert executor.needs_aggressive_fallback(pending) is False

    def test_fallback_unavailable_no_close_executor(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(phase=PassiveExecutionPhase.DUAL_TAKER),
        )
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor._fallback_to_aggressive_close(state, pending, position)
        )
        assert result is False
        assert pending.next_retry_at_ms > 0

    def test_fallback_zero_residual_returns_true(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(phase=PassiveExecutionPhase.DUAL_TAKER),
            maker_fill=PendingPassiveLegFill(quantity=0.01),
            hedge_fill=PendingPassiveLegFill(quantity=0.01),
        )
        state.pending_passive_closes[position.position_id] = pending
        result = asyncio.run(
            executor._fallback_to_aggressive_close(state, pending, position)
        )
        assert result is True

    def test_fallback_clears_when_live_positions_are_flat(self):
        """V1: flat pending passive close is reconciled before retrying fallback."""
        journal = _open_journal()
        from lightfee.engine.close_executor import CloseExecutor

        class FlatAdapter(VenueAdapter):
            def __init__(self, venue):
                self._venue = venue

            @property
            def venue(self):
                return self._venue

            async def place_order(self, request):
                raise AssertionError("flat recovery must not submit new close orders")

            async def fetch_position(self, symbol):
                return PositionSnapshot(
                    venue=self._venue, symbol=symbol,
                    side=Side.BUY, quantity=0.0, entry_price=0.0,
                    observed_at_ms=1000,
                )

        long_adapter = FlatAdapter(Venue.ASTER)
        short_adapter = FlatAdapter(Venue.BYBIT)
        mock_close_exec = MagicMock(spec=CloseExecutor)
        mock_close_exec.execute_close = AsyncMock()

        executor = PassiveCloseExecutor(
            {Venue.ASTER: long_adapter, Venue.BYBIT: short_adapter}, journal,
        )
        executor.set_close_executor(mock_close_exec)
        state = EngineState()
        position = _make_position(
            symbol="PROVEUSDT",
            long_venue=Venue.ASTER,
            short_venue=Venue.BYBIT,
            long_quantity=82.0,
            short_quantity=82.0,
            matched_quantity=82.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=82.0,
            chunk_quantities=[82.0],
            phase_state=PassivePhaseState(phase=PassiveExecutionPhase.DUAL_TAKER),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor._fallback_to_aggressive_close(state, pending, position)
        )

        assert result is True
        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions
        mock_close_exec.execute_close.assert_not_called()
        kinds = [record["kind"] for record in journal.read_all()]
        assert "recovery.flat" in kinds


# ---------------------------------------------------------------------------
# Recovery probe
# ---------------------------------------------------------------------------

class TestRecoveryProbe:
    def test_recovery_clears_when_live_flat(self):
        journal = _open_journal()
        from lightfee.core.contracts import VenueAdapter as VA

        class FlatAdapter(VA):
            @property
            def venue(self):
                return Venue.BINANCE
            async def place_order(self, request):
                raise NotImplementedError
            async def fetch_position(self, symbol):
                return PositionSnapshot(
                    venue=Venue.BINANCE, symbol=symbol,
                    side=Side.BUY, quantity=0.0, entry_price=0.0,
                    observed_at_ms=1000,
                )

        adapter = FlatAdapter()
        executor = PassiveCloseExecutor(
            {Venue.BINANCE: adapter, Venue.OKX: adapter}, journal,
        )
        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
        )
        state.pending_passive_closes[position.position_id] = pending
        state.open_positions = {}

        result = asyncio.run(
            executor.recover_passive_close(
                state, position.position_id,
                {Venue.BINANCE: adapter, Venue.OKX: adapter},
            )
        )
        assert result == "cleared_flat"
        assert position.position_id not in state.pending_passive_closes

    def test_recovery_resumes_when_position_open(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            next_retry_at_ms=99999999,
        )
        state.pending_passive_closes[position.position_id] = pending
        state.open_positions[position.position_id] = position

        result = asyncio.run(
            executor.recover_passive_close(state, position.position_id, {})
        )
        assert result == "resumed"
        assert pending.next_retry_at_ms == 0

    def test_recovery_ambiguous_when_cannot_confirm(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
        )
        state.pending_passive_closes[position.position_id] = pending
        state.open_positions = {}

        result = asyncio.run(
            executor.recover_passive_close(state, position.position_id, {})
        )
        assert result == "ambiguous"
        assert position.position_id in state.pending_passive_closes


# ---------------------------------------------------------------------------
# Start pending passive close
# ---------------------------------------------------------------------------

class TestStartPendingPassiveClose:
    def test_start_creates_pending_record(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)
        state = EngineState()
        position = _make_position()

        pending = asyncio.run(
            executor.start_pending_passive_close(
                state, position, "funding_capture",
                long_price_hint=50000.0, short_price_hint=50000.0,
            )
        )
        assert pending is not None
        assert pending.position_id == position.position_id
        assert pending.reason == "funding_capture"
        assert pending.chunk_count() >= 1
        assert pending.phase_state.phase == PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER
        assert position.position_id in state.pending_passive_closes

    def test_start_rejects_duplicate(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)
        state = EngineState()
        position = _make_position()

        p1 = asyncio.run(
            executor.start_pending_passive_close(
                state, position, "funding_capture",
                long_price_hint=50000.0, short_price_hint=50000.0,
            )
        )
        assert p1 is not None
        p2 = asyncio.run(
            executor.start_pending_passive_close(
                state, position, "funding_capture",
                long_price_hint=50000.0, short_price_hint=50000.0,
            )
        )
        assert p2 is None

    def test_start_rejects_zero_quantity(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        state = EngineState()
        position = _make_position(matched_quantity=0.0, long_quantity=0.0, short_quantity=0.0)
        pending = asyncio.run(
            executor.start_pending_passive_close(state, position, "funding_capture")
        )
        assert pending is None


# ---------------------------------------------------------------------------
# Multi-chunk
# ---------------------------------------------------------------------------

class TestMultiChunkPassiveClose:
    def test_multi_chunk_created(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor(
            {}, journal,
            config_overrides={"close_chunk_max_notional_quote": 500.0},
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)
        state = EngineState()
        position = _make_position(matched_quantity=0.05)

        pending = asyncio.run(
            executor.start_pending_passive_close(
                state, position, "funding_capture",
                long_price_hint=50000.0, short_price_hint=50000.0,
            )
        )
        assert pending is not None
        assert pending.chunk_count() > 1

    def test_chunk_advance_resets_phase_state(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        state = EngineState()
        position = _make_position(matched_quantity=0.05)
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.05,
            chunk_quantities=[0.01, 0.01, 0.01, 0.01, 0.01],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.LOW_SLIPPAGE_MAKER,
                preferred_maker_leg=ActiveMakerLeg.LONG,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=0.01),
            hedge_fill=PendingPassiveLegFill(quantity=0.01),
        )
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(executor._advance_chunk(state, pending))
        assert result is True
        assert pending.active_chunk_index == 1
        assert pending.phase_state.phase == PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER
        assert pending.maker_fill.quantity == 0.0
        assert pending.hedge_fill.quantity == 0.0


# ===========================================================================
# ROOT-INVARIANT TESTS — real path-driven, no fake/paper/shadow coverage
# ===========================================================================


class TestAdvanceChunkRootInvariant:
    """Test 1: _advance_chunk() directly refuses unhedged advance."""

    def test_advance_blocked_when_hedge_behind_maker(self):
        """maker_fill=0.01, hedge_fill=0.005, chunk=0.01 → _advance_chunk returns False."""
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=0.01),
            hedge_fill=PendingPassiveLegFill(quantity=0.005),
        )
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(executor._advance_chunk(state, pending))

        assert result is False
        assert pending.active_chunk_index == 0  # NOT advanced
        assert pending.maker_fill.quantity == 0.01  # NOT reset
        assert pending.hedge_fill.quantity == 0.005  # NOT reset
        assert position.position_id in state.pending_passive_closes  # NOT removed
        assert pending.next_retry_at_ms > 0  # retry scheduled

        # Verify journal has blocked_unhedged
        events = journal.read_all()
        blocked_events = [e for e in events if e.get("kind") == "exit.passive_close_advance_blocked_unhedged"]
        assert len(blocked_events) == 1

    def test_advance_blocked_when_maker_under_chunk(self):
        """maker_fill=0.005, hedge_fill=0.005, chunk=0.01 → _advance_chunk returns False."""
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=0.005),
            hedge_fill=PendingPassiveLegFill(quantity=0.005),
        )
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(executor._advance_chunk(state, pending))

        assert result is False
        assert pending.active_chunk_index == 0
        assert pending.maker_fill.quantity == 0.005  # NOT reset
        assert position.position_id in state.pending_passive_closes

        events = journal.read_all()
        blocked_events = [e for e in events if e.get("kind") == "exit.passive_close_advance_blocked_maker_under_chunk"]
        assert len(blocked_events) == 1

    def test_advance_succeeds_when_both_full(self):
        """maker_fill=0.01, hedge_fill=0.01, chunk=0.01 → _advance_chunk returns True."""
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                preferred_maker_leg=ActiveMakerLeg.LONG,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=0.01),
            hedge_fill=PendingPassiveLegFill(quantity=0.01),
        )
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(executor._advance_chunk(state, pending))

        assert result is True
        # Single-chunk → completed → finalized → removed from state
        assert position.position_id not in state.pending_passive_closes


class TestTerminalMakerFillHedgeFail:
    """Test 2: terminal maker FILLED + hedge error → chunk NOT advanced."""

    def test_maker_filled_hedge_exception_does_not_advance(self):
        """Maker terminal FILLED, hedge adapter raises exception → drive returns False, index=0."""
        journal = _open_journal()

        # Mock maker adapter: progress returns FILLED at full chunk
        maker_adapter = _mock_adapter_passive_ok(Venue.BINANCE)
        maker_adapter.query_passive_order_progress = AsyncMock(return_value=_make_passive_progress(
            venue=Venue.BINANCE, side=Side.SELL,
            cumulative_quantity=0.01, average_price=50000.0,
            state=PassiveOrderState.FILLED,
        ))

        # Mock hedge adapter: place_order raises exception
        hedge_adapter = _mock_adapter_with_tick(Venue.OKX)
        hedge_adapter.place_order = AsyncMock(side_effect=Exception("hedge timeout"))
        hedge_adapter.submit_passive_order = AsyncMock(return_value=_make_passive_ack(venue=Venue.OKX))

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: maker_adapter, Venue.OKX: hedge_adapter}, journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)

        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="oid-maker",
                maker_client_order_id="cid-maker",
                maker_resting_limit_price=50000.0,
            ),
            maker_fill=PendingPassiveLegFill(),
            hedge_fill=PendingPassiveLegFill(),
        )
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False)
        )

        assert result is False
        assert pending.active_chunk_index == 0  # NOT advanced
        assert pending.hedge_fill.quantity == 0.0  # hedge had 0 fill
        assert position.position_id in state.pending_passive_closes  # still pending

        # Journal should have hedge_error
        events = journal.read_all()
        hedge_errors = [e for e in events if e.get("kind") == "exit.passive_close_hedge_error"]
        assert len(hedge_errors) >= 1

    def test_maker_filled_hedge_partial_no_advance(self):
        """Maker FILLED at chunk, hedge IOC only fills half → no advance, pending retained."""
        journal = _open_journal()

        maker_adapter = _mock_adapter_passive_ok(Venue.BINANCE)
        maker_adapter.query_passive_order_progress = AsyncMock(return_value=_make_passive_progress(
            venue=Venue.BINANCE, side=Side.SELL,
            cumulative_quantity=0.01, average_price=50000.0,
            state=PassiveOrderState.FILLED,
        ))

        hedge_adapter = _mock_adapter_with_tick(Venue.OKX)
        hedge_adapter.place_order = AsyncMock(return_value=_make_order_fill(
            venue=Venue.OKX, side=Side.BUY, quantity=0.005, price=50001.0,
        ))
        hedge_adapter.submit_passive_order = AsyncMock(return_value=_make_passive_ack(venue=Venue.OKX))

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: maker_adapter, Venue.OKX: hedge_adapter}, journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)

        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="oid-maker",
                maker_client_order_id="cid-maker",
                maker_resting_limit_price=50000.0,
            ),
            maker_fill=PendingPassiveLegFill(),
            hedge_fill=PendingPassiveLegFill(),
        )
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False)
        )

        assert result is False
        assert pending.active_chunk_index == 0  # NOT advanced
        assert pending.hedge_fill.quantity == 0.005  # partial fill recorded
        assert position.position_id in state.pending_passive_closes

    def test_hedge_catches_up_on_next_cycle_then_advances(self):
        """First cycle: maker=0.01 filled, hedge only gets 0.005. Second cycle: hedge catches up → advance."""
        journal = _open_journal()

        call_count = [0]

        maker_adapter = _mock_adapter_passive_ok(Venue.BINANCE)
        # Return FILLED on both cycles
        maker_adapter.query_passive_order_progress = AsyncMock(return_value=_make_passive_progress(
            venue=Venue.BINANCE, side=Side.SELL,
            cumulative_quantity=0.01, average_price=50000.0,
            state=PassiveOrderState.FILLED,
        ))

        hedge_adapter = _mock_adapter_with_tick(Venue.OKX)
        def hedge_fill_side_effect(request, _count=call_count):
            _count[0] += 1
            if _count[0] == 1:
                # First call: partial fill 0.005
                return _make_order_fill(
                    venue=Venue.OKX, side=Side.BUY, quantity=0.005, price=50001.0,
                )
            else:
                # Second call: fill remaining 0.005
                return _make_order_fill(
                    venue=Venue.OKX, side=Side.BUY, quantity=0.005, price=50001.0,
                    order_id="f-hedge-2",
                )
        hedge_adapter.place_order = AsyncMock(side_effect=hedge_fill_side_effect)

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: maker_adapter, Venue.OKX: hedge_adapter}, journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)

        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="oid-maker",
                maker_client_order_id="cid-maker",
                maker_resting_limit_price=50000.0,
            ),
            maker_fill=PendingPassiveLegFill(),
            hedge_fill=PendingPassiveLegFill(),
        )
        state.pending_passive_closes[position.position_id] = pending

        # First drive cycle
        result1 = asyncio.run(
            executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False)
        )
        assert result1 is False
        assert pending.active_chunk_index == 0
        assert pending.hedge_fill.quantity == 0.005

        # Second drive cycle — hedge catches up
        pending.next_retry_at_ms = 0  # allow immediate retry
        result2 = asyncio.run(
            executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False)
        )
        # Should advance now — chunk complete, position removed
        assert position.position_id not in state.pending_passive_closes


class TestPartialMakerFillGradualCatchUp:
    """Test 4: non-terminal partial maker fill → gradual hedge catch-up."""

    def test_partial_fill_no_advance_until_both_full(self):
        """Step-by-step: maker partially fills, hedge catches up gradually."""
        journal = _open_journal()

        progress_states = [
            # (cumulative_maker_qty, hedge_fill_qty, should_advance, description)
            (0.004, 0.002, False, "first partial: maker=0.004, hedge=0.002"),
            (0.004, 0.004, False, "hedge caught up but maker not full"),
            (0.010, 0.008, False, "maker full but hedge behind"),
            (0.010, 0.010, True,  "both full → advance"),
        ]

        for maker_qty, hedge_qty, should_advance, desc in progress_states:
            _j = _open_journal()
            executor = PassiveCloseExecutor({}, _j)
            state = EngineState()
            position = _make_position()
            pending = PendingPassiveClose(
                position_id=position.position_id,
                reason="funding_capture",
                position_snapshot=position,
                target_quantity=0.01,
                chunk_quantities=[0.01],
                active_chunk_index=0,
                phase_state=PassivePhaseState(
                    phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                    active_maker_leg=ActiveMakerLeg.LONG,
                ),
                maker_fill=PendingPassiveLegFill(quantity=maker_qty),
                hedge_fill=PendingPassiveLegFill(quantity=hedge_qty),
            )
            state.pending_passive_closes[position.position_id] = pending

            result = asyncio.run(executor._advance_chunk(state, pending))

            if should_advance:
                assert result is True, f"FAIL: {desc}"
            else:
                assert result is False, f"FAIL: {desc}"
                assert pending.active_chunk_index == 0, f"FAIL: {desc} — index advanced"
                assert pending.maker_fill.quantity == maker_qty, f"FAIL: {desc} — maker fill reset"
                assert pending.hedge_fill.quantity == hedge_qty, f"FAIL: {desc} — hedge fill reset"


class TestFallbackResidualReal:
    """Test 3: fallback residual strict validation with real mock flow."""

    def test_fallback_paired_residual_total_quantity(self):
        """maker=0.4, hedge=0.4, chunk=1.0 → paired_residual=0.6 sent to close_executor."""
        journal = _open_journal()

        from lightfee.engine.close_executor import CloseExecutor
        mock_close_exec = MagicMock(spec=CloseExecutor)
        captured_total_qty = []
        captured_stages = []

        async def fake_execute_close(position, reason, now_ms, long_price_hint,
                                     short_price_hint, total_quantity, state,
                                     short_stage="", long_stage=""):
            captured_total_qty.append(total_quantity)
            captured_stages.append((short_stage, long_stage))
            return CloseExecution(
                position_id=position.position_id, reason=reason,
                long_close_price=50000.0, short_close_price=50000.0,
                long_close_qty=total_quantity or 0, short_close_qty=total_quantity or 0,
                long_fee_quote=1.0, short_fee_quote=1.0,
                realized_price_pnl_quote=0.0, funding_pnl_quote=0.0, net_quote=-2.0,
            )

        mock_close_exec.execute_close = AsyncMock(side_effect=fake_execute_close)

        executor = PassiveCloseExecutor({}, journal)
        executor.set_close_executor(mock_close_exec)
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)

        state = EngineState()
        position = _make_position(matched_quantity=1.0, long_quantity=1.0, short_quantity=1.0)
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=1.0,
            chunk_quantities=[1.0],
            phase_state=PassivePhaseState(phase=PassiveExecutionPhase.DUAL_TAKER),
            maker_fill=PendingPassiveLegFill(quantity=0.4, average_price=50000.0),
            hedge_fill=PendingPassiveLegFill(quantity=0.4, average_price=50000.0),
        )
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor._fallback_to_aggressive_close(state, pending, position)
        )

        assert len(captured_total_qty) == 1
        assert abs(captured_total_qty[0] - 0.6) < 1e-9
        assert captured_stages == [("exit_short", "exit_long")]

    def test_fallback_unhedged_fails_blocks_aggressive_close(self):
        """maker=0.4, hedge=0.2, chunk=1.0 → unhedged=0.2 must succeed before aggressive close."""
        journal = _open_journal()

        from lightfee.engine.close_executor import CloseExecutor
        mock_close_exec = MagicMock(spec=CloseExecutor)

        hedge_adapter = _mock_adapter_with_tick(Venue.OKX)
        # Hedge fails
        hedge_adapter.place_order = AsyncMock(side_effect=Exception("hedge unavailable"))
        # submit_passive_order needed for the unhedged hedge delta
        hedge_adapter.submit_passive_order = AsyncMock(return_value=_make_passive_ack(venue=Venue.OKX))

        executor = PassiveCloseExecutor(
            {Venue.OKX: hedge_adapter, Venue.BINANCE: _mock_adapter_passive_ok(Venue.BINANCE)},
            journal,
        )
        executor.set_close_executor(mock_close_exec)
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)

        state = EngineState()
        position = _make_position(
            matched_quantity=1.0, long_quantity=1.0, short_quantity=1.0,
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=1.0,
            chunk_quantities=[1.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=0.4, average_price=50000.0),
            hedge_fill=PendingPassiveLegFill(quantity=0.2, average_price=50000.0),
        )
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor._fallback_to_aggressive_close(state, pending, position)
        )

        assert result is False
        # Aggressive close was NOT called (unhedged hedge failed)
        mock_close_exec.execute_close.assert_not_called()
        # Passive pending still exists
        assert position.position_id in state.pending_passive_closes

        events = journal.read_all()
        failed_events = [e for e in events if e.get("kind") == "exit.passive_close_fallback_unhedged_failed"]
        assert len(failed_events) == 1

    def test_fallback_unhedged_terminal_reduceonly_rechecks_flat_and_clears(self):
        """A terminal reduce-only hedge reject after a stale probe must clear once live venues are flat."""
        journal = _open_journal()

        from lightfee.engine.close_executor import CloseExecutor

        class SequencedFlatAdapter(VenueAdapter):
            def __init__(self, venue, fetch_quantities, place_error=None):
                self._venue = venue
                self._fetch_quantities = list(fetch_quantities)
                self._place_error = place_error
                self.place_order_calls = 0

            @property
            def venue(self):
                return self._venue

            async def place_order(self, request):
                self.place_order_calls += 1
                if self._place_error is not None:
                    raise self._place_error
                return _make_order_fill(
                    venue=self._venue,
                    symbol=request.symbol,
                    side=request.side,
                    quantity=request.quantity,
                    price=request.price or 1.0,
                )

            async def fetch_position(self, symbol):
                qty = self._fetch_quantities.pop(0) if self._fetch_quantities else 0.0
                return PositionSnapshot(
                    venue=self._venue,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=qty,
                    entry_price=1.0 if qty else 0.0,
                    observed_at_ms=1000,
                )

        bybit = SequencedFlatAdapter(Venue.BYBIT, [0.0, 0.0])
        aster = SequencedFlatAdapter(
            Venue.ASTER,
            [1874.0, 0.0],
            OrderSubmitError(
                SubmitFailureClass.REJECTED,
                'HTTP 400: {"code":-2022,"msg":"ReduceOnly Order is rejected."}',
            ),
        )
        adapters = {Venue.BYBIT: bybit, Venue.ASTER: aster}
        executor = PassiveCloseExecutor(adapters, journal)
        executor.set_close_executor(CloseExecutor(adapters, journal))
        executor.set_l2_mid_resolver(lambda venue, symbol: 0.0134)

        state = EngineState()
        position = _make_position(
            position_id="entry-gmt",
            symbol="GMTUSDT",
            long_venue=Venue.BYBIT,
            short_venue=Venue.ASTER,
            long_quantity=1874.0,
            short_quantity=1874.0,
            matched_quantity=1874.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            target_quantity=1874.0,
            chunk_quantities=[1874.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=1874.0, average_price=0.012817),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor._fallback_to_aggressive_close(state, pending, position)
        )

        assert result is True
        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions
        assert aster.place_order_calls == 1
        kinds = [record["kind"] for record in journal.read_all()]
        assert "exit.passive_close_fallback_terminal_flat" in kinds

    def test_fallback_paired_terminal_reduceonly_rechecks_flat_and_clears(self):
        """Paired fallback must not loop when both close legs report already-flat terminal rejects."""
        journal = _open_journal()

        from lightfee.engine.close_executor import CloseExecutor

        class TerminalFlatAdapter(VenueAdapter):
            def __init__(self, venue, fetch_quantities, place_error):
                self._venue = venue
                self._fetch_quantities = list(fetch_quantities)
                self._place_error = place_error
                self.place_order_calls = 0

            @property
            def venue(self):
                return self._venue

            async def place_order(self, request):
                self.place_order_calls += 1
                raise self._place_error

            async def fetch_position(self, symbol):
                qty = self._fetch_quantities.pop(0) if self._fetch_quantities else 0.0
                return PositionSnapshot(
                    venue=self._venue,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=qty,
                    entry_price=1.0 if qty else 0.0,
                    observed_at_ms=1000,
                )

        aster = TerminalFlatAdapter(
            Venue.ASTER,
            [533.0, 0.0, 0.0],
            OrderSubmitError(
                SubmitFailureClass.REJECTED,
                'HTTP 400: {"code":-2022,"msg":"ReduceOnly Order is rejected."}',
            ),
        )
        bybit = TerminalFlatAdapter(
            Venue.BYBIT,
            [0.0, 0.0, 0.0],
            OrderSubmitError(
                SubmitFailureClass.REJECTED,
                "bybit retCode=110017 retMsg=current position is zero, cannot fix reduce-only order qty",
            ),
        )
        adapters = {Venue.ASTER: aster, Venue.BYBIT: bybit}
        executor = PassiveCloseExecutor(adapters, journal)
        executor.set_close_executor(CloseExecutor(adapters, journal))
        executor.set_l2_mid_resolver(lambda venue, symbol: 0.045)

        state = EngineState()
        position = _make_position(
            position_id="entry-lyn",
            symbol="LYNUSDT",
            long_venue=Venue.ASTER,
            short_venue=Venue.BYBIT,
            long_quantity=533.0,
            short_quantity=533.0,
            matched_quantity=533.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            target_quantity=533.0,
            chunk_quantities=[533.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
                active_maker_leg=ActiveMakerLeg.SHORT,
            ),
            maker_fill=PendingPassiveLegFill(quantity=0.0),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor._fallback_to_aggressive_close(state, pending, position)
        )

        assert result is True
        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions
        assert bybit.place_order_calls == 1
        assert aster.place_order_calls == 1
        kinds = [record["kind"] for record in journal.read_all()]
        assert "exit.passive_close_fallback_terminal_flat" in kinds

    def test_fallback_zero_fill_no_pending_does_not_clear(self):
        """Aggressive close returns zero fill AND no PendingClose → don't clear passive pending."""
        journal = _open_journal()

        from lightfee.engine.close_executor import CloseExecutor
        mock_close_exec = MagicMock(spec=CloseExecutor)

        # Mock returns zero-fill close with no PendingClose created
        async def fake_zero_fill(position, reason, now_ms, long_price_hint,
                                 short_price_hint, total_quantity, state,
                             short_stage="", long_stage=""):
            return CloseExecution(
                position_id=position.position_id, reason=reason,
                long_close_price=0.0, short_close_price=0.0,
                long_close_qty=0.0, short_close_qty=0.0,
                long_fee_quote=0.0, short_fee_quote=0.0,
                realized_price_pnl_quote=0.0, funding_pnl_quote=0.0, net_quote=0.0,
            )

        mock_close_exec.execute_close = AsyncMock(side_effect=fake_zero_fill)

        executor = PassiveCloseExecutor({}, journal)
        executor.set_close_executor(mock_close_exec)
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)

        state = EngineState()
        position = _make_position(matched_quantity=1.0, long_quantity=1.0, short_quantity=1.0)
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=1.0,
            chunk_quantities=[1.0],
            phase_state=PassivePhaseState(phase=PassiveExecutionPhase.DUAL_TAKER),
            maker_fill=PendingPassiveLegFill(quantity=0.4, average_price=50000.0),
            hedge_fill=PendingPassiveLegFill(quantity=0.4, average_price=50000.0),
        )
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor._fallback_to_aggressive_close(state, pending, position)
        )

        assert result is False
        # Passive pending NOT cleared
        assert position.position_id in state.pending_passive_closes

        events = journal.read_all()
        zero_fill_events = [e for e in events if e.get("kind") == "exit.passive_close_fallback_zero_fill_no_pending"]
        assert len(zero_fill_events) == 1


class TestMaintainFailClosed:
    """Test 4: maintain/reprice fail-closed on missing L2/tick data."""

    def test_maintain_no_tick_size_journals_and_sets_retry(self):
        """tick_size <= 0 → journal, retry, no amend/cancel_replace call."""
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        # Override tick size to return 0
        executor._get_tick_size = lambda venue, symbol: 0.0

        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_resting_limit_price=50000.0,
            ),
        )

        asyncio.run(
            executor._maintain_maker_order(
                state, pending, position,
                Venue.BINANCE, Side.SELL, "long", 50100.0, 0.01,
            )
        )

        assert pending.next_retry_at_ms > 0

        events = journal.read_all()
        no_tick = [e for e in events if e.get("kind") == "exit.passive_close_maintain_no_tick_size"]
        assert len(no_tick) == 1

    def test_maintain_no_price_hint_journals_and_sets_retry(self):
        """price_hint <= 0 → journal, retry, no amend/cancel_replace."""
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        executor._get_tick_size = lambda venue, symbol: 0.01
        executor.set_l2_quote_resolver(lambda venue, symbol: (50099.99, 50100.01))

        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_resting_limit_price=50000.0,
            ),
        )

        asyncio.run(
            executor._maintain_maker_order(
                state, pending, position,
                Venue.BINANCE, Side.SELL, "long", 0.0, 0.01,
            )
        )

        assert pending.next_retry_at_ms > 0

        events = journal.read_all()
        no_price = [e for e in events if e.get("kind") == "exit.passive_close_maintain_no_price_hint"]
        assert len(no_price) == 1

    def test_maintain_no_resting_price_journals_and_sets_retry(self):
        """current_price is None → journal, retry."""
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        executor._get_tick_size = lambda venue, symbol: 0.01
        executor.set_l2_quote_resolver(lambda venue, symbol: (50099.99, 50100.01))

        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_resting_limit_price=None,  # no resting price
            ),
        )

        asyncio.run(
            executor._maintain_maker_order(
                state, pending, position,
                Venue.BINANCE, Side.SELL, "long", 50100.0, 0.01,
            )
        )

        assert pending.next_retry_at_ms > 0

        events = journal.read_all()
        no_resting = [e for e in events if e.get("kind") == "exit.passive_close_maintain_no_resting_price"]
        assert len(no_resting) == 1

    def test_submit_maker_fails_without_l2_mid(self):
        """When L2 mid is 0, _submit_maker_order returns False."""
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
        )
        state.pending_passive_closes[position.position_id] = pending

        success = asyncio.run(
            executor._submit_maker_order(
                state, pending, position,
                Venue.BINANCE, Side.SELL, "long", 0.0, 0.01,
            )
        )
        assert success is False
        assert pending.phase_state.maker_order_id == ""

    def test_submit_maker_fails_with_zero_tick_size(self):
        """When tick_size is 0, _submit_maker_order fails closed."""
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        executor._get_tick_size = lambda venue, symbol: 0.0
        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
        )
        state.pending_passive_closes[position.position_id] = pending

        success = asyncio.run(
            executor._submit_maker_order(
                state, pending, position,
                Venue.BINANCE, Side.SELL, "long", 50000.0, 0.01,
            )
        )
        assert success is False
        assert pending.phase_state.maker_order_id == ""

    def test_submit_maker_uses_dynamic_bybit_tick_before_static_spec(self):
        """V1 parity: passive close uses symbol metadata tick before VenueSpec default."""
        from lightfee.venues.specs import get_spec
        from lightfee.venues.symbol_rules import get_symbol_rules_cache

        class FakeBybitTransport:
            _spec = get_spec(Venue.BYBIT)

            def _venue_symbol(self, symbol):
                return symbol

            async def _public_get(self, path, params=None):
                assert path == "/v5/market/instruments-info"
                assert params == {"category": "linear", "symbol": "ALTUSDT"}
                return {
                    "result": {
                        "list": [
                            {
                                "symbol": "ALTUSDT",
                                "priceFilter": {"tickSize": "0.000001"},
                                "lotSizeFilter": {
                                    "qtyStep": "1",
                                    "minOrderQty": "1",
                                    "minNotionalValue": "5",
                                },
                            }
                        ]
                    }
                }

        get_symbol_rules_cache().clear()
        journal = _open_journal()
        adapter = _mock_adapter_with_tick(Venue.BYBIT, tick=0.01)
        adapter._transport = FakeBybitTransport()
        adapter.submit_passive_order = AsyncMock(return_value=_make_passive_ack(
            venue=Venue.BYBIT,
            symbol="ALTUSDT",
            side=Side.BUY,
            order_id="bybit-passive-1",
            client_order_id="cid-1",
            price=0.007802,
            quantity=2789.0,
        ))
        executor = PassiveCloseExecutor({Venue.BYBIT: adapter}, journal)
        state = EngineState()
        position = _make_position(
            position_id="entry-1779479522323-ALTUSDT",
            symbol="ALTUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=2789.0,
            short_quantity=2789.0,
            matched_quantity=2789.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=2789.0,
            chunk_quantities=[2789.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.LOW_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.SHORT,
            ),
        )
        state.pending_passive_closes[position.position_id] = pending

        try:
            success = asyncio.run(
                executor._submit_maker_order(
                    state, pending, position,
                    Venue.BYBIT, Side.BUY, "short", 0.0078025, 2789.0,
                )
            )
        finally:
            get_symbol_rules_cache().clear()

        assert success is True
        sent_request = adapter.submit_passive_order.await_args.args[0]
        assert sent_request.price == pytest.approx(0.007802)
        invalid_price = [
            e for e in journal.read_all()
            if e.get("kind") == "exit.passive_close_invalid_aligned_price"
        ]
        assert invalid_price == []

    def test_submit_maker_infers_tick_from_l2_quote_before_static_spec(self):
        """V1 parity: when metadata is absent, infer passive tick from L2 quote precision."""
        journal = _open_journal()
        adapter = _mock_adapter_with_tick(Venue.BYBIT, tick=0.01)
        adapter.submit_passive_order = AsyncMock(return_value=_make_passive_ack(
            venue=Venue.BYBIT,
            symbol="ALTUSDT",
            side=Side.BUY,
            order_id="bybit-passive-quote-1",
            client_order_id="cid-quote-1",
            price=0.007802,
            quantity=2789.0,
        ))
        executor = PassiveCloseExecutor({Venue.BYBIT: adapter}, journal)
        executor.set_l2_quote_resolver(lambda venue, symbol: (0.007802, 0.007803))
        state = EngineState()
        position = _make_position(
            position_id="entry-1779479522323-ALTUSDT",
            symbol="ALTUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=2789.0,
            short_quantity=2789.0,
            matched_quantity=2789.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=2789.0,
            chunk_quantities=[2789.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.LOW_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.SHORT,
            ),
        )
        state.pending_passive_closes[position.position_id] = pending

        success = asyncio.run(
            executor._submit_maker_order(
                state, pending, position,
                Venue.BYBIT, Side.BUY, "short", 0.0078025, 2789.0,
            )
        )

        assert success is True
        sent_request = adapter.submit_passive_order.await_args.args[0]
        assert sent_request.price == pytest.approx(0.007802)
        invalid_price = [
            e for e in journal.read_all()
            if e.get("kind") == "exit.passive_close_invalid_aligned_price"
        ]
        assert invalid_price == []

    def test_submit_maker_ignores_rule_cache_spec_fallback_before_l2_quote(self):
        """V1 parity: failed metadata lookup must not outrank quote tick inference."""
        from lightfee.venues.specs import get_spec
        from lightfee.venues.symbol_rules import get_symbol_rules_cache

        class FailingBybitTransport:
            _spec = get_spec(Venue.BYBIT)

            def _venue_symbol(self, symbol):
                return symbol

            async def _public_get(self, path, params=None):
                raise RuntimeError("metadata unavailable")

        get_symbol_rules_cache().clear()
        journal = _open_journal()
        adapter = _mock_adapter_with_tick(Venue.BYBIT, tick=0.01)
        adapter._transport = FailingBybitTransport()
        adapter.submit_passive_order = AsyncMock(return_value=_make_passive_ack(
            venue=Venue.BYBIT,
            symbol="ALTUSDT",
            side=Side.BUY,
            order_id="bybit-passive-fallback-1",
            client_order_id="cid-fallback-1",
            price=0.007802,
            quantity=2789.0,
        ))
        executor = PassiveCloseExecutor({Venue.BYBIT: adapter}, journal)
        executor.set_l2_quote_resolver(lambda venue, symbol: (0.007802, 0.007803))
        state = EngineState()
        position = _make_position(
            position_id="entry-1779479522323-ALTUSDT",
            symbol="ALTUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=2789.0,
            short_quantity=2789.0,
            matched_quantity=2789.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=2789.0,
            chunk_quantities=[2789.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.LOW_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.SHORT,
            ),
        )
        state.pending_passive_closes[position.position_id] = pending

        try:
            success = asyncio.run(
                executor._submit_maker_order(
                    state, pending, position,
                    Venue.BYBIT, Side.BUY, "short", 0.0078025, 2789.0,
                )
            )
        finally:
            get_symbol_rules_cache().clear()

        assert success is True
        sent_request = adapter.submit_passive_order.await_args.args[0]
        assert sent_request.price == pytest.approx(0.007802)

    def test_submit_maker_infers_tick_for_spec_fallback_only_venue(self):
        """V1 parity: venues without dynamic rule fetch still use quote precision first."""
        from lightfee.venues.specs import get_spec
        from lightfee.venues.symbol_rules import get_symbol_rules_cache

        class GateSpecOnlyTransport:
            _spec = get_spec(Venue.GATE)

        get_symbol_rules_cache().clear()
        journal = _open_journal()
        adapter = _mock_adapter_with_tick(Venue.GATE, tick=0.01)
        adapter._transport = GateSpecOnlyTransport()
        adapter.submit_passive_order = AsyncMock(return_value=_make_passive_ack(
            venue=Venue.GATE,
            symbol="ALTUSDT",
            side=Side.BUY,
            order_id="gate-passive-quote-1",
            client_order_id="cid-gate-1",
            price=0.007802,
            quantity=2789.0,
        ))
        executor = PassiveCloseExecutor({Venue.GATE: adapter}, journal)
        executor.set_l2_quote_resolver(lambda venue, symbol: (0.007802, 0.007803))
        state = EngineState()
        position = _make_position(
            position_id="entry-1779479522323-ALTUSDT",
            symbol="ALTUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.GATE,
            long_quantity=2789.0,
            short_quantity=2789.0,
            matched_quantity=2789.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=2789.0,
            chunk_quantities=[2789.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.LOW_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.SHORT,
            ),
        )
        state.pending_passive_closes[position.position_id] = pending

        try:
            success = asyncio.run(
                executor._submit_maker_order(
                    state, pending, position,
                    Venue.GATE, Side.BUY, "short", 0.0078025, 2789.0,
                )
            )
        finally:
            get_symbol_rules_cache().clear()

        assert success is True
        sent_request = adapter.submit_passive_order.await_args.args[0]
        assert sent_request.price == pytest.approx(0.007802)

    def test_passive_tick_missing_metadata_and_quote_returns_zero_not_static_spec(self):
        """Strict V1 parity: no metadata/quote means no passive tick, not spec fallback."""
        journal = _open_journal()
        adapter = _mock_adapter_with_tick(Venue.BYBIT, tick=0.01)
        executor = PassiveCloseExecutor({Venue.BYBIT: adapter}, journal)

        tick_size = asyncio.run(
            executor._get_passive_tick_size(
                Venue.BYBIT,
                "ALTUSDT",
                target_price=0.0078025,
                side=Side.BUY,
            )
        )

        assert tick_size == 0.0


class TestMakerLegSelection:
    """Test 5: maker leg selection uses L2 data, not fee-only monkeypatch."""

    def test_select_long_when_long_cost_higher(self):
        """Higher long taker cost → LONG selected."""
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        # Inject L2 mid so long has higher cost estimate
        # Long=Binance (taker_fee=4bps typically), Short=OKX (taker_fee=5bps)
        # But we want LONG selected, so make long cost higher via L2
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)
        # Override cost estimates to make long > short
        executor._estimate_venue_taker_cost_bps = lambda venue, symbol, l2_mid=0.0: (
            10.0 if venue == Venue.BINANCE else 5.0
        )

        position = _make_position(long_venue=Venue.BINANCE, short_venue=Venue.OKX)
        leg = executor._select_preferred_maker_leg(position)
        assert leg == ActiveMakerLeg.LONG

    def test_select_short_when_short_cost_higher(self):
        """Higher short taker cost → SHORT selected."""
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)
        executor._estimate_venue_taker_cost_bps = lambda venue, symbol, l2_mid=0.0: (
            5.0 if venue == Venue.BINANCE else 10.0
        )

        position = _make_position(long_venue=Venue.BINANCE, short_venue=Venue.OKX)
        leg = executor._select_preferred_maker_leg(position)
        assert leg == ActiveMakerLeg.SHORT

    def test_select_long_on_tie(self):
        """Equal costs → LONG (V1 tie-break)."""
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)
        executor._estimate_venue_taker_cost_bps = lambda venue, symbol, l2_mid=0.0: 5.0

        position = _make_position(long_venue=Venue.BINANCE, short_venue=Venue.OKX)
        leg = executor._select_preferred_maker_leg(position)
        assert leg == ActiveMakerLeg.LONG

    def test_start_pending_uses_selected_leg(self):
        """start_pending_passive_close uses _select_preferred_maker_leg."""
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        executor._select_preferred_maker_leg = lambda pos: ActiveMakerLeg.SHORT
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)
        state = EngineState()
        position = _make_position()

        pending = asyncio.run(
            executor.start_pending_passive_close(
                state, position, "funding_capture",
                long_price_hint=50000.0, short_price_hint=50000.0,
            )
        )
        assert pending is not None
        assert pending.phase_state.preferred_maker_leg == ActiveMakerLeg.SHORT
        assert pending.phase_state.active_maker_leg == ActiveMakerLeg.SHORT

    def test_l2_missing_journals_data_gap(self):
        """When L2 mid is unavailable for one venue, _select_preferred_maker_leg journals it."""
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        # Long=Binance: L2 available (mid=50000), Short=OKX: L2 missing (mid=0)
        def asymmetric_resolver(venue, symbol):
            if venue == Venue.BINANCE:
                return 50000.0
            return 0.0
        executor.set_l2_mid_resolver(asymmetric_resolver)
        executor._estimate_venue_taker_cost_bps = lambda venue, symbol, l2_mid=0.0: 5.0

        position = _make_position(long_venue=Venue.BINANCE, short_venue=Venue.OKX)
        leg = executor._select_preferred_maker_leg(position)

        # Tie (both cost=5.0) → LONG
        assert leg == ActiveMakerLeg.LONG

        events = journal.read_all()
        l2_missing = [e for e in events if e.get("kind") == "exit.passive_close_maker_leg_l2_missing"]
        assert len(l2_missing) == 1


class TestAmendCancelReplace:
    """Test 6: amend unsupported → cancel-replace; cancel fail → no double-order."""

    def test_amend_not_implemented_falls_back_to_cancel_replace(self):
        """When amend raises NotImplementedError, cancel-replace is used."""
        journal = _open_journal()
        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="old-oid",
                maker_client_order_id="old-cid",
                maker_resting_limit_price=50000.0,
            ),
        )
        state.pending_passive_closes[position.position_id] = pending

        mock_adapter = _mock_adapter_with_tick(Venue.BINANCE)
        mock_adapter.amend_passive_order = AsyncMock(side_effect=NotImplementedError)
        mock_adapter.cancel_passive_order = AsyncMock(return_value=_make_passive_ack(order_id="old-oid"))
        mock_adapter.submit_passive_order = AsyncMock(return_value=_make_passive_ack(
            order_id="new-oid", client_order_id="new-cid", price=50100.0,
        ))

        executor = PassiveCloseExecutor({Venue.BINANCE: mock_adapter}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 50100.0)
        executor.set_l2_quote_resolver(lambda venue, symbol: (50099.99, 50100.01))

        asyncio.run(
            executor._cancel_replace_maker_order(
                state, pending, position, Venue.BINANCE, Side.SELL,
                "long", 50100.0, 0.01, 0.01, 50100.0,
            )
        )
        assert pending.phase_state.maker_order_id == "new-oid"

    def test_cancel_fails_old_order_alive_blocks_new_order(self):
        """When cancel fails and old order is still alive → refuse new order (double-order guard)."""
        journal = _open_journal()
        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="old-oid",
                maker_client_order_id="old-cid",
                maker_resting_limit_price=50000.0,
            ),
        )
        state.pending_passive_closes[position.position_id] = pending

        mock_adapter = _mock_adapter_with_tick(Venue.BINANCE)
        # Cancel fails
        mock_adapter.cancel_passive_order = AsyncMock(side_effect=Exception("cancel failed"))
        # Query shows old order still OPEN
        mock_adapter.query_passive_order_progress = AsyncMock(return_value=_make_passive_progress(
            state=PassiveOrderState.OPEN, cumulative_quantity=0.0,
        ))
        mock_adapter.submit_passive_order = AsyncMock()

        executor = PassiveCloseExecutor({Venue.BINANCE: mock_adapter}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 50100.0)
        executor.set_l2_quote_resolver(lambda venue, symbol: (50099.99, 50100.01))

        asyncio.run(
            executor._cancel_replace_maker_order(
                state, pending, position, Venue.BINANCE, Side.SELL,
                "long", 50100.0, 0.01, 0.01, 50100.0,
            )
        )

        # New order was NOT submitted (double-order guard)
        mock_adapter.submit_passive_order.assert_not_called()
        assert pending.next_retry_at_ms > 0

        events = journal.read_all()
        blocked = [e for e in events if e.get("kind") == "exit.passive_close_cancel_replace_blocked_double_order_risk"]
        assert len(blocked) == 1

    def test_cancel_fails_old_order_dead_proceeds(self):
        """When cancel fails but old order is dead → new order proceeds."""
        journal = _open_journal()
        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="old-oid",
                maker_client_order_id="old-cid",
                maker_resting_limit_price=50000.0,
            ),
        )
        state.pending_passive_closes[position.position_id] = pending

        mock_adapter = _mock_adapter_with_tick(Venue.BINANCE)
        mock_adapter.cancel_passive_order = AsyncMock(side_effect=Exception("cancel failed"))
        # Query shows old order FILLED (dead)
        mock_adapter.query_passive_order_progress = AsyncMock(return_value=_make_passive_progress(
            state=PassiveOrderState.FILLED, cumulative_quantity=0.01,
        ))
        mock_adapter.submit_passive_order = AsyncMock(return_value=_make_passive_ack(
            order_id="new-oid", client_order_id="new-cid", price=50100.0,
        ))

        executor = PassiveCloseExecutor({Venue.BINANCE: mock_adapter}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 50100.0)
        executor.set_l2_quote_resolver(lambda venue, symbol: (50099.99, 50100.01))

        asyncio.run(
            executor._cancel_replace_maker_order(
                state, pending, position, Venue.BINANCE, Side.SELL,
                "long", 50100.0, 0.01, 0.01, 50100.0,
            )
        )

        # New order WAS submitted (old is dead)
        mock_adapter.submit_passive_order.assert_called_once()
        assert pending.phase_state.maker_order_id == "new-oid"


class TestHedgeDeltaResultStructure:
    def test_hedge_returns_structured_result(self):
        result = HedgeDeltaResult(requested=0.01, filled=0.01, residual=0.0, success=True)
        assert result.requested == 0.01
        assert result.filled == 0.01
        assert result.residual == 0.0
        assert result.success is True

    def test_hedge_failure_result(self):
        result = HedgeDeltaResult(
            requested=0.01, filled=0.0, residual=0.01, success=False,
            error="venue timeout",
        )
        assert result.success is False
        assert result.residual == 0.01

    def test_hedge_partial_fill_result(self):
        result = HedgeDeltaResult(
            requested=0.01, filled=0.004, residual=0.006, success=False,
            error="partial_fill",
        )
        assert result.success is False
        assert result.filled == 0.004
        assert result.residual == 0.006


class TestCancelReplaceQueryFailureFailClosed:
    """Test A: cancel fails AND query fails → block new order (double-order guard).

    When both cancel_passive_order AND query_passive_order_progress throw exceptions,
    _probe_order_dead returns False (fail-closed) and _cancel_replace_maker_order
    must NOT submit the new maker order. Journal must contain the blocked event.
    """

    def test_cancel_fails_query_fails_blocks_new_order(self):
        journal = _open_journal()
        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="old-oid",
                maker_client_order_id="old-cid",
                maker_resting_limit_price=50000.0,
            ),
        )
        state.pending_passive_closes[position.position_id] = pending

        mock_adapter = _mock_adapter_with_tick(Venue.BINANCE)
        # Cancel fails
        mock_adapter.cancel_passive_order = AsyncMock(side_effect=Exception("cancel timeout"))
        # Query also fails — _probe_order_dead returns False (fail-closed)
        mock_adapter.query_passive_order_progress = AsyncMock(side_effect=Exception("query timeout"))
        mock_adapter.submit_passive_order = AsyncMock()

        executor = PassiveCloseExecutor({Venue.BINANCE: mock_adapter}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 50100.0)

        asyncio.run(
            executor._cancel_replace_maker_order(
                state, pending, position, Venue.BINANCE, Side.SELL,
                "long", 50100.0, 0.01, 0.01, 50100.0,
            )
        )

        # New maker order NOT submitted (double-order guard triggered)
        mock_adapter.submit_passive_order.assert_not_called()
        assert pending.next_retry_at_ms > 0
        # Old maker order id preserved (not overwritten)
        assert pending.phase_state.maker_order_id == "old-oid"
        assert pending.phase_state.maker_client_order_id == "old-cid"

        events = journal.read_all()
        blocked = [e for e in events if e.get("kind") == "exit.passive_close_cancel_replace_blocked_double_order_risk"]
        assert len(blocked) == 1


class TestNonTerminalPartialFillHedgeGapClosure:
    """Test B: non-terminal partial maker fill → hedge gap continuously closed.

    When maker stays at PARTIALLY_FILLED with the same cumulative across cycles,
    the hedge gap (maker_fill - hedge_fill) must be re-submitted even though
    maker_fill_delta == 0. Tests the unhedged_gap-based hedge logic end-to-end
    through drive_pending_passive_close.
    """

    def test_partial_fill_hedge_gap_repeated_until_closed(self):
        """First drive: maker=0.004, hedge gets 0.002 → gap 0.002 remains.
        Second drive: maker still 0.004, hedge gets 0.002 → gap closed.
        """
        journal = _open_journal()

        hedge_calls = [0]
        maker_adapter = _mock_adapter_passive_ok(Venue.BINANCE)
        # Maker stays PARTIALLY_FILLED cumulative=0.004 both cycles
        maker_adapter.query_passive_order_progress = AsyncMock(return_value=_make_passive_progress(
            venue=Venue.BINANCE, side=Side.SELL,
            cumulative_quantity=0.004, average_price=50000.0,
            state=PassiveOrderState.PARTIALLY_FILLED,
        ))

        hedge_adapter = _mock_adapter_with_tick(Venue.OKX)
        def hedge_fill_side_effect(request, _hc=hedge_calls):
            _hc[0] += 1
            return _make_order_fill(
                venue=Venue.OKX, side=Side.BUY, quantity=0.002, price=50001.0,
                order_id=f"f-hedge-{_hc[0]}",
            )
        hedge_adapter.place_order = AsyncMock(side_effect=hedge_fill_side_effect)

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: maker_adapter, Venue.OKX: hedge_adapter}, journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)

        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="oid-maker",
                maker_client_order_id="cid-maker",
                maker_resting_limit_price=50000.0,
            ),
            maker_fill=PendingPassiveLegFill(),
            hedge_fill=PendingPassiveLegFill(),
        )
        state.pending_passive_closes[position.position_id] = pending

        # --- First drive cycle ---
        result1 = asyncio.run(
            executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False)
        )
        assert result1 is False
        assert pending.maker_fill.quantity == 0.004
        assert pending.hedge_fill.quantity == 0.002  # only 0.002 filled
        assert pending.active_chunk_index == 0  # NOT advanced
        assert pending.next_retry_at_ms > 0  # retry scheduled
        assert position.position_id in state.pending_passive_closes

        # --- Second drive cycle — clear retry, maker still at 0.004 ---
        pending.next_retry_at_ms = 0
        result2 = asyncio.run(
            executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False)
        )
        assert result2 is False  # still not complete (maker not full)
        assert pending.hedge_fill.quantity == 0.004  # hedge caught up
        assert pending.active_chunk_index == 0  # still NOT advanced (maker not full)
        assert position.position_id in state.pending_passive_closes

        # Hedge was called exactly twice
        assert hedge_calls[0] == 2

        events = journal.read_all()
        incomplete = [e for e in events if e.get("kind") == "exit.passive_close_hedge_incomplete"]
        assert len(incomplete) == 1  # only first cycle was incomplete

    def test_full_catchup_then_advance_to_terminal(self):
        """Step 3: maker fills to 0.01, hedge catches up residual → advance."""
        journal = _open_journal()

        hedge_calls = [0]
        progress_states = [
            # First call: PARTIALLY_FILLED 0.004, second: PARTIALLY_FILLED 0.004
            _make_passive_progress(
                venue=Venue.BINANCE, side=Side.SELL,
                cumulative_quantity=0.004, average_price=50000.0,
                state=PassiveOrderState.PARTIALLY_FILLED,
            ),
            _make_passive_progress(
                venue=Venue.BINANCE, side=Side.SELL,
                cumulative_quantity=0.004, average_price=50000.0,
                state=PassiveOrderState.PARTIALLY_FILLED,
            ),
            # Third call: FILLED at full chunk
            _make_passive_progress(
                venue=Venue.BINANCE, side=Side.SELL,
                cumulative_quantity=0.01, average_price=50000.0,
                state=PassiveOrderState.FILLED,
            ),
        ]
        progress_iter = iter(progress_states)

        maker_adapter = _mock_adapter_passive_ok(Venue.BINANCE)
        maker_adapter.query_passive_order_progress = AsyncMock(side_effect=lambda *a, **kw: next(progress_iter))

        hedge_adapter = _mock_adapter_with_tick(Venue.OKX)
        def hedge_fill_side_effect(request, _hc=hedge_calls):
            _hc[0] += 1
            if _hc[0] <= 2:
                # First 2 calls return 0.002 each (partial fills for drives 1 and 2)
                return _make_order_fill(
                    venue=Venue.OKX, side=Side.BUY, quantity=0.002, price=50001.0,
                    order_id=f"f-hedge-{_hc[0]}",
                )
            else:
                # Third call (terminal FILLED drive) returns 0.006 to close the gap
                return _make_order_fill(
                    venue=Venue.OKX, side=Side.BUY, quantity=0.006, price=50001.0,
                    order_id=f"f-hedge-{_hc[0]}",
                )
        hedge_adapter.place_order = AsyncMock(side_effect=hedge_fill_side_effect)

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: maker_adapter, Venue.OKX: hedge_adapter}, journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)

        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                preferred_maker_leg=ActiveMakerLeg.LONG,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="oid-maker",
                maker_client_order_id="cid-maker",
                maker_resting_limit_price=50000.0,
            ),
            maker_fill=PendingPassiveLegFill(),
            hedge_fill=PendingPassiveLegFill(),
        )
        state.pending_passive_closes[position.position_id] = pending

        # Drive 1: maker=0.004, hedge gap=0.004 → gets 0.002 (first incomplete cycle)
        result1 = asyncio.run(
            executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False)
        )
        assert result1 is False

        # Drive 2: maker still 0.004, hedge gap=0.002 → gets 0.002 (gap closed, partial cycle complete)
        pending.next_retry_at_ms = 0
        result2 = asyncio.run(
            executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False)
        )
        assert result2 is False
        assert pending.hedge_fill.quantity == 0.004

        # Drive 3: maker FILLED at 0.01, hedge gap=0.006 → gets 0.006 → advance
        pending.next_retry_at_ms = 0
        result3 = asyncio.run(
            executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False)
        )
        # After full advance → finalized → removed from pending
        assert position.position_id not in state.pending_passive_closes
        assert pending.hedge_fill.quantity >= 0.01
        assert pending.maker_fill.quantity == 0.01


# ===========================================================================
# DUAL_TAKER drive consumption — the drive loop must route DUAL_TAKER to
# _fallback_to_aggressive_close instead of re-entering maker submit/poll.
# ===========================================================================


class TestDualTakerDriveConsumption:
    """Test that drive_pending_passive_close consumes DUAL_TAKER state."""

    def test_dual_taker_pending_routes_to_aggressive_fallback(self):
        """Pending with phase=DUAL_TAKER, valid position, chunk=1.0 →
        drive calls _fallback_to_aggressive_close and does NOT call maker submit."""
        journal = _open_journal()

        from lightfee.engine.close_executor import CloseExecutor
        captured_total_qty = []

        async def fake_execute_close(position, reason, now_ms, long_price_hint,
                                     short_price_hint, total_quantity, state,
                                     short_stage="", long_stage=""):
            captured_total_qty.append(total_quantity)
            return CloseExecution(
                position_id=position.position_id, reason=reason,
                long_close_price=50000.0, short_close_price=50000.0,
                long_close_qty=total_quantity or 0, short_close_qty=total_quantity or 0,
                long_fee_quote=1.0, short_fee_quote=1.0,
                realized_price_pnl_quote=0.0, funding_pnl_quote=0.0, net_quote=-2.0,
            )

        mock_close_exec = MagicMock(spec=CloseExecutor)
        mock_close_exec.execute_close = AsyncMock(side_effect=fake_execute_close)

        # Mock adapter with submit_passive_order — must NOT be called
        maker_adapter = _mock_adapter_passive_ok(Venue.BINANCE)

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: maker_adapter, Venue.OKX: _mock_adapter_with_tick(Venue.OKX)},
            journal,
        )
        executor.set_close_executor(mock_close_exec)
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)

        state = EngineState()
        position = _make_position(matched_quantity=1.0, long_quantity=1.0, short_quantity=1.0)
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=1.0,
            chunk_quantities=[1.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(),
            hedge_fill=PendingPassiveLegFill(),
        )
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False)
        )

        # _fallback_to_aggressive_close executed → close_executor called with total_quantity=1.0
        assert len(captured_total_qty) == 1
        assert abs(captured_total_qty[0] - 1.0) < 1e-9

        # Maker submit was NOT called (DUAL_TAKER → straight to fallback)
        maker_adapter.submit_passive_order.assert_not_called()

        # Passive pending cleaned up after successful fallback
        assert position.position_id not in state.pending_passive_closes

        # Journal has dual_taker_drive
        events = journal.read_all()
        dual_taker_drives = [e for e in events if e.get("kind") == "exit.passive_close_dual_taker_drive"]
        assert len(dual_taker_drives) == 1

    def test_submit_unsupported_to_dual_taker_then_fallback_path(self):
        """First drive: submit_passive_order raises NotImplementedError →
        phase becomes DUAL_TAKER, returns False.
        Second drive: DUAL_TAKER consumed → aggressive fallback, no maker submit.
        """
        journal = _open_journal()

        from lightfee.engine.close_executor import CloseExecutor
        captured_total_qty = []

        async def fake_execute_close(position, reason, now_ms, long_price_hint,
                                     short_price_hint, total_quantity, state,
                                     short_stage="", long_stage=""):
            captured_total_qty.append(total_quantity)
            return CloseExecution(
                position_id=position.position_id, reason=reason,
                long_close_price=50000.0, short_close_price=50000.0,
                long_close_qty=total_quantity or 0, short_close_qty=total_quantity or 0,
                long_fee_quote=1.0, short_fee_quote=1.0,
                realized_price_pnl_quote=0.0, funding_pnl_quote=0.0, net_quote=-2.0,
            )

        mock_close_exec = MagicMock(spec=CloseExecutor)
        mock_close_exec.execute_close = AsyncMock(side_effect=fake_execute_close)

        maker_adapter = _mock_adapter_with_tick(Venue.BINANCE)
        maker_adapter.submit_passive_order = AsyncMock(side_effect=NotImplementedError)
        maker_adapter.place_order = AsyncMock(return_value=_make_order_fill(venue=Venue.BINANCE))

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: maker_adapter, Venue.OKX: _mock_adapter_passive_ok(Venue.OKX)},
            journal,
        )
        executor.set_close_executor(mock_close_exec)
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)
        executor.set_l2_quote_resolver(lambda venue, symbol: (49999.99, 50000.01))

        state = EngineState()
        position = _make_position(matched_quantity=1.0, long_quantity=1.0, short_quantity=1.0)
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=1.0,
            chunk_quantities=[1.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(),
            hedge_fill=PendingPassiveLegFill(),
        )
        state.pending_passive_closes[position.position_id] = pending

        # --- First drive: submit raises NotImplementedError, phase → DUAL_TAKER ---
        result1 = asyncio.run(
            executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False)
        )
        assert result1 is False
        assert pending.phase_state.phase == PassiveExecutionPhase.DUAL_TAKER
        assert position.position_id in state.pending_passive_closes

        # Verify not_supported was journaled
        events1 = journal.read_all()
        not_supported = [e for e in events1 if e.get("kind") == "exit.passive_close_not_supported"]
        assert len(not_supported) == 1

        # --- Second drive: DUAL_TAKER consumed → aggressive fallback ---
        pending.next_retry_at_ms = 0
        result2 = asyncio.run(
            executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False)
        )

        # Close executor was called with total_quantity=1.0 (current chunk residual)
        assert len(captured_total_qty) == 1
        assert abs(captured_total_qty[0] - 1.0) < 1e-9

        # Maker submit was called exactly once (first drive only), not on second
        assert maker_adapter.submit_passive_order.call_count == 1

        # Pending cleaned up
        assert position.position_id not in state.pending_passive_closes

        # Journal has dual_taker_drive
        events2 = journal.read_all()
        dual_taker_drives = [e for e in events2 if e.get("kind") == "exit.passive_close_dual_taker_drive"]
        assert len(dual_taker_drives) == 1


class TestCancelReplaceSubmitFailure:
    """Test that _cancel_replace_maker_order consumes _submit_maker_order return value."""

    def test_replace_submit_not_implemented_no_completed_journal(self):
        """Cancel succeeds but replacement submit raises NotImplementedError →
        no 'completed' journal, phase=DUAL_TAKER, pending retained."""
        journal = _open_journal()
        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="old-oid",
                maker_client_order_id="old-cid",
                maker_resting_limit_price=50000.0,
            ),
        )
        state.pending_passive_closes[position.position_id] = pending

        mock_adapter = _mock_adapter_with_tick(Venue.BINANCE)
        mock_adapter.cancel_passive_order = AsyncMock(return_value=_make_passive_ack(order_id="old-oid"))
        mock_adapter.submit_passive_order = AsyncMock(side_effect=NotImplementedError)

        executor = PassiveCloseExecutor({Venue.BINANCE: mock_adapter}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 50100.0)
        executor.set_l2_quote_resolver(lambda venue, symbol: (50099.99, 50100.01))

        asyncio.run(
            executor._cancel_replace_maker_order(
                state, pending, position, Venue.BINANCE, Side.SELL,
                "long", 50100.0, 0.01, 0.01, 50100.0,
            )
        )

        # Phase set to DUAL_TAKER
        assert pending.phase_state.phase == PassiveExecutionPhase.DUAL_TAKER

        # Pending NOT cleared
        assert position.position_id in state.pending_passive_closes

        events = journal.read_all()
        completed = [e for e in events if e.get("kind") == "exit.passive_close_cancel_replace_completed"]
        assert len(completed) == 0  # NOT completed

        submit_failed = [e for e in events if e.get("kind") == "exit.passive_close_cancel_replace_submit_failed"]
        assert len(submit_failed) == 1

    def test_replace_submit_l2_invalid_no_completed_journal(self):
        """Cancel succeeds but replacement submit fails (no L2/tick) →
        no 'completed' journal, retry set, pending retained."""
        journal = _open_journal()
        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="old-oid",
                maker_client_order_id="old-cid",
                maker_resting_limit_price=50000.0,
            ),
        )
        state.pending_passive_closes[position.position_id] = pending

        mock_adapter = _mock_adapter_with_tick(Venue.BINANCE)
        mock_adapter.cancel_passive_order = AsyncMock(return_value=_make_passive_ack(order_id="old-oid"))
        # submit_passive_order succeeds but _submit_maker_order will fail because tick_size=0
        mock_adapter.submit_passive_order = AsyncMock(return_value=_make_passive_ack(
            order_id="new-oid", client_order_id="new-cid", price=50100.0,
        ))

        executor = PassiveCloseExecutor({Venue.BINANCE: mock_adapter}, journal)
        executor._get_tick_size = lambda venue, symbol: 0.0  # force L2/tick failure

        asyncio.run(
            executor._cancel_replace_maker_order(
                state, pending, position, Venue.BINANCE, Side.SELL,
                "long", 50100.0, 0.01, 0.01, 50100.0,
            )
        )

        # Pending NOT cleared
        assert position.position_id in state.pending_passive_closes

        # Retry set (not DUAL_TAKER, so retry_delay is set)
        assert pending.next_retry_at_ms > 0

        events = journal.read_all()
        completed = [e for e in events if e.get("kind") == "exit.passive_close_cancel_replace_completed"]
        assert len(completed) == 0  # NOT completed

        submit_failed = [e for e in events if e.get("kind") == "exit.passive_close_cancel_replace_submit_failed"]
        assert len(submit_failed) == 1

    def test_replace_submit_success_still_completed(self):
        """Cancel succeeds, replacement submit succeeds →
        'completed' journal present, maker_order_id updated."""
        journal = _open_journal()
        state = EngineState()
        position = _make_position()
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="old-oid",
                maker_client_order_id="old-cid",
                maker_resting_limit_price=50000.0,
            ),
        )
        state.pending_passive_closes[position.position_id] = pending

        mock_adapter = _mock_adapter_with_tick(Venue.BINANCE)
        mock_adapter.cancel_passive_order = AsyncMock(return_value=_make_passive_ack(order_id="old-oid"))
        mock_adapter.submit_passive_order = AsyncMock(return_value=_make_passive_ack(
            order_id="new-oid", client_order_id="new-cid", price=50100.0,
        ))

        executor = PassiveCloseExecutor({Venue.BINANCE: mock_adapter}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 50100.0)
        executor.set_l2_quote_resolver(lambda venue, symbol: (50099.99, 50100.01))

        asyncio.run(
            executor._cancel_replace_maker_order(
                state, pending, position, Venue.BINANCE, Side.SELL,
                "long", 50100.0, 0.01, 0.01, 50100.0,
            )
        )

        assert pending.phase_state.maker_order_id == "new-oid"

        events = journal.read_all()
        completed = [e for e in events if e.get("kind") == "exit.passive_close_cancel_replace_completed"]
        assert len(completed) == 1
        assert completed[0]["payload"]["new_order_id"] == "new-oid"

        submit_failed = [e for e in events if e.get("kind") == "exit.passive_close_cancel_replace_submit_failed"]
        assert len(submit_failed) == 0


# ---------------------------------------------------------------------------
# Production path semantic tests — coroutine safety, reduce-only escalation,
# L2/tick missing escalation, retry backoff
# ---------------------------------------------------------------------------


class TestProductionPathCoroutineSafety:
    """Verify _resolve_local_l2_mid never creates unawaited coroutines."""

    def test_no_coroutine_created_for_async_adapter(self):
        """_resolve_local_l2_mid with no injected resolver returns 0.0
        without calling adapter.fetch_market_snapshot (which is async)."""
        import warnings
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        # No l2_mid_resolver injected — should return 0.0 cleanly
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = executor._resolve_local_l2_mid(Venue.BINANCE, "BTCUSDT")
        assert result == 0.0
        coroutine_warnings = [
            x for x in w
            if "coroutine" in str(x.message).lower() and "never awaited" in str(x.message).lower()
        ]
        assert len(coroutine_warnings) == 0, (
            f"Got coroutine warning: {[str(x.message) for x in coroutine_warnings]}"
        )

    def test_with_injected_resolver_returns_mid(self):
        """Injected resolver is used, no adapter call attempted."""
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)
        result = executor._resolve_local_l2_mid(Venue.BINANCE, "BTCUSDT")
        assert result == 50000.0

    def test_resolver_returns_zero_falls_back_to_zero(self):
        """Resolver returns 0 → 0.0, not fallback to async adapter."""
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 0.0)
        result = executor._resolve_local_l2_mid(Venue.BINANCE, "BTCUSDT")
        assert result == 0.0


class TestReduceOnlyRejectedEscalation:
    """Reduce-only rejected → immediate DUAL_TAKER escalation, no infinite retry."""

    def test_recovered_terminal_rejected_maker_order_escalates_without_spin(self):
        """Recovered terminal rejected maker order must not be polled forever."""
        journal = _open_journal()
        maker_adapter = _mock_adapter_with_tick(Venue.BINANCE)
        maker_adapter.query_passive_order_progress = AsyncMock(return_value=_make_passive_progress(
            venue=Venue.BINANCE,
            side=Side.SELL,
            order_id="rejected-maker",
            client_order_id="rejected-client",
            cumulative_quantity=0.0,
            average_price=0.0,
            state=PassiveOrderState.REJECTED,
        ))
        maker_adapter.submit_passive_order = AsyncMock()

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: maker_adapter, Venue.OKX: _mock_adapter_passive_ok(Venue.OKX)},
            journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)

        state = EngineState()
        position = _make_position(matched_quantity=1.0, long_quantity=1.0, short_quantity=1.0)
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=1.0,
            chunk_quantities=[1.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.LOW_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                zero_fill_cycles_in_phase=PASSIVE_CLOSE_MAX_ZERO_FILL_CYCLES - 1,
                maker_order_id="rejected-maker",
                maker_client_order_id="rejected-client",
                maker_resting_limit_price=50000.0,
            ),
            maker_fill=PendingPassiveLegFill(),
            hedge_fill=PendingPassiveLegFill(),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(asyncio.wait_for(
            executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False),
            timeout=0.1,
        ))

        assert result is False
        assert maker_adapter.query_passive_order_progress.await_count == 1
        maker_adapter.submit_passive_order.assert_not_called()
        assert pending.phase_state.phase == PassiveExecutionPhase.DUAL_TAKER
        assert pending.phase_state.maker_order_id == ""
        events = journal.read_all()
        terminal_events = [
            e for e in events
            if e.get("kind") == "exit.passive_close_maker_terminal_no_fill"
        ]
        assert len(terminal_events) == 1

    def test_order_submit_error_rejected_escalates_to_dual_taker(self):
        """OrderSubmitError with is_rejected=True transitions to DUAL_TAKER."""
        journal = _open_journal()
        mock_adapter = _mock_adapter_with_tick(Venue.BINANCE)
        rejected_error = OrderSubmitError(
            SubmitFailureClass.REJECTED,
            "Binance API error: code=-2022, msg=ReduceOnly Order is rejected.",
        )
        mock_adapter.submit_passive_order = AsyncMock(side_effect=rejected_error)

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: mock_adapter, Venue.OKX: _mock_adapter_passive_ok(Venue.OKX)},
            journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)
        executor.set_l2_quote_resolver(lambda venue, symbol: (49999.99, 50000.01))

        state = EngineState()
        position = _make_position(matched_quantity=1.0, long_quantity=1.0, short_quantity=1.0)
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=1.0,
            chunk_quantities=[1.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(),
            hedge_fill=PendingPassiveLegFill(),
        )
        state.pending_passive_closes[position.position_id] = pending

        asyncio.run(
            executor._submit_maker_order(
                state, pending, position,
                Venue.BINANCE, Side.SELL, "long", 50000.0, 1.0,
            )
        )

        # Must have escalated to DUAL_TAKER
        assert pending.phase_state.phase == PassiveExecutionPhase.DUAL_TAKER, (
            f"Expected DUAL_TAKER after reduce-only rejection, got {pending.phase_state.phase}"
        )

    def test_order_submit_error_uncertain_backs_off_with_escalation(self):
        """OrderSubmitError with is_rejected=False counts failures, backs off,
        escalates to DUAL_TAKER after PASSIVE_CLOSE_MAX_MAKER_SUBMIT_FAILURES."""
        journal = _open_journal()
        mock_adapter = _mock_adapter_with_tick(Venue.BINANCE)
        uncertain_error = OrderSubmitError(
            SubmitFailureClass.UNCERTAIN,
            "Network timeout during order submit",
        )
        mock_adapter.submit_passive_order = AsyncMock(side_effect=uncertain_error)

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: mock_adapter, Venue.OKX: _mock_adapter_passive_ok(Venue.OKX)},
            journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)
        executor.set_l2_quote_resolver(lambda venue, symbol: (49999.99, 50000.01))

        state = EngineState()
        position = _make_position(matched_quantity=1.0, long_quantity=1.0, short_quantity=1.0)
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=1.0,
            chunk_quantities=[1.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(),
            hedge_fill=PendingPassiveLegFill(),
        )
        state.pending_passive_closes[position.position_id] = pending

        # Submit PASSIVE_CLOSE_MAX_MAKER_SUBMIT_FAILURES times
        for i in range(PASSIVE_CLOSE_MAX_MAKER_SUBMIT_FAILURES):
            phase_before = pending.phase_state.phase
            asyncio.run(
                executor._submit_maker_order(
                    state, pending, position,
                    Venue.BINANCE, Side.SELL, "long", 50000.0, 1.0,
                )
            )
            if i < PASSIVE_CLOSE_MAX_MAKER_SUBMIT_FAILURES - 1:
                # Still in maker phase, backoff applied
                assert pending.phase_state.phase == PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER, (
                    f"Attempt {i}: still in maker phase"
                )
                assert pending.next_retry_at_ms > 0
            else:
                # Last attempt: escalated
                assert pending.phase_state.phase == PassiveExecutionPhase.DUAL_TAKER, (
                    f"Attempt {i}: expected escalation to DUAL_TAKER"
                )

        # Verify failure counter
        assert pending.phase_state.maker_submit_consecutive_failures == PASSIVE_CLOSE_MAX_MAKER_SUBMIT_FAILURES


class TestMissingL2TickEscalation:
    """Missing L2/tick data escalates after max consecutive failures."""

    def test_missing_l2_escalates_after_max_failures(self):
        """When price_hint is 0 three consecutive times, escalate to DUAL_TAKER."""
        journal = _open_journal()
        mock_adapter = _mock_adapter_with_tick(Venue.BINANCE)
        # submit is never called because L2 is missing

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: mock_adapter},
            journal,
        )
        # No L2 resolver → price_hint will be 0
        # No l2_mid_resolver set

        state = EngineState()
        position = _make_position(matched_quantity=1.0, long_quantity=1.0, short_quantity=1.0)
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=1.0,
            chunk_quantities=[1.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(),
            hedge_fill=PendingPassiveLegFill(),
        )
        state.pending_passive_closes[position.position_id] = pending

        for i in range(PASSIVE_CLOSE_MAX_MISSING_L2_TICK_FAILURES):
            asyncio.run(
                executor._submit_maker_order(
                    state, pending, position,
                    Venue.BINANCE, Side.SELL, "long", 0.0, 1.0,
                )
            )
            if i < PASSIVE_CLOSE_MAX_MISSING_L2_TICK_FAILURES - 1:
                assert pending.phase_state.phase == PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER
            else:
                assert pending.phase_state.phase == PassiveExecutionPhase.DUAL_TAKER, (
                    f"Attempt {i}: expected DUAL_TAKER escalation"
                )

        assert pending.phase_state.missing_l2_tick_consecutive_count == PASSIVE_CLOSE_MAX_MISSING_L2_TICK_FAILURES

    def test_missing_l2_counter_resets_on_data_available(self):
        """Missing L2 counter resets to 0 when L2 data becomes available."""
        journal = _open_journal()
        mock_adapter = _mock_adapter_with_tick(Venue.BINANCE)
        mock_adapter.submit_passive_order = AsyncMock(
            return_value=_make_passive_ack(order_id="ok-oid")
        )

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: mock_adapter},
            journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)
        executor.set_l2_quote_resolver(lambda venue, symbol: (49999.99, 50000.01))

        state = EngineState()
        position = _make_position(matched_quantity=1.0, long_quantity=1.0, short_quantity=1.0)
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=1.0,
            chunk_quantities=[1.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                missing_l2_tick_consecutive_count=2,  # near the edge
            ),
            maker_fill=PendingPassiveLegFill(),
            hedge_fill=PendingPassiveLegFill(),
        )
        state.pending_passive_closes[position.position_id] = pending

        # Successful submit should reset the counter
        asyncio.run(
            executor._submit_maker_order(
                state, pending, position,
                Venue.BINANCE, Side.SELL, "long", 50000.0, 1.0,
            )
        )

        assert pending.phase_state.missing_l2_tick_consecutive_count == 0
        assert pending.phase_state.maker_submit_consecutive_failures == 0


class TestPassiveCloseBackoffBehavior:
    """Backoff increases with consecutive failures."""

    def test_missing_l2_backoff_increases(self):
        """Each missing L2 failure increases the retry delay."""
        journal = _open_journal()
        mock_adapter = _mock_adapter_with_tick(Venue.BINANCE)

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: mock_adapter},
            journal,
        )
        # No L2 resolver → price_hint = 0

        state = EngineState()
        position = _make_position(matched_quantity=1.0, long_quantity=1.0, short_quantity=1.0)
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=1.0,
            chunk_quantities=[1.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(),
            hedge_fill=PendingPassiveLegFill(),
        )
        state.pending_passive_closes[position.position_id] = pending

        prev_retry_at = 0
        for i in range(PASSIVE_CLOSE_MAX_MISSING_L2_TICK_FAILURES):
            asyncio.run(
                executor._submit_maker_order(
                    state, pending, position,
                    Venue.BINANCE, Side.SELL, "long", 0.0, 1.0,
                )
            )
            if i < PASSIVE_CLOSE_MAX_MISSING_L2_TICK_FAILURES - 1:
                assert pending.next_retry_at_ms >= prev_retry_at, (
                    f"Attempt {i}: backoff should increase"
                )
                prev_retry_at = pending.next_retry_at_ms


class TestHedgeNonRetryableEscalation:
    """Hedge side reduce-only rejected escalates to DUAL_TAKER."""

    def test_is_non_retryable_hedge_error_reduce_only(self):
        """_is_non_retryable_hedge_error detects reduce-only rejections."""
        executor = PassiveCloseExecutor({}, _open_journal())
        assert executor._is_non_retryable_hedge_error(
            "HTTP 400: {\"code\":-2022,\"msg\":\"ReduceOnly Order is rejected.\"}"
        )
        assert executor._is_non_retryable_hedge_error("ReduceOnly Order is rejected")
        assert not executor._is_non_retryable_hedge_error("Network timeout")
        assert not executor._is_non_retryable_hedge_error("")

    def test_hedge_reduce_only_escalates_in_drive_loop(self):
        """When hedge fails with reduce-only rejection in the drive loop,
        the pending phase transitions to DUAL_TAKER."""
        journal = _open_journal()

        maker_adapter = _mock_adapter_with_tick(Venue.BINANCE)
        # Maker submit succeeds
        maker_ack = _make_passive_ack(order_id="maker-oid-001")
        maker_adapter.submit_passive_order = AsyncMock(return_value=maker_ack)
        # Maker progress shows FILLED
        maker_adapter.query_passive_order_progress = AsyncMock(
            return_value=PassiveOrderProgress(
                venue=Venue.BINANCE,
                symbol="BTCUSDT",
                side=Side.SELL,
                order_id="maker-oid-001",
                cumulative_quantity=1.0,
                average_price=50000.0,
                state=PassiveOrderState.FILLED,
            )
        )
        # Hedge (place_order on OKX) fails with reduce-only
        hedge_adapter = _mock_adapter_passive_ok(Venue.OKX)
        from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
        hedge_adapter.place_order = AsyncMock(
            side_effect=OrderSubmitError(
                SubmitFailureClass.REJECTED,
                "HTTP 400: {\"code\":-2022,\"msg\":\"ReduceOnly Order is rejected.\"}",
            )
        )

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: maker_adapter, Venue.OKX: hedge_adapter},
            journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)
        executor.set_l2_quote_resolver(lambda venue, symbol: (49999.99, 50000.01))

        state = EngineState()
        position = _make_position(
            matched_quantity=1.0, long_quantity=1.0, short_quantity=1.0,
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=1.0,
            chunk_quantities=[1.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=1.0, average_price=50000.0),
            hedge_fill=PendingPassiveLegFill(),
            short_stage="exit_short",
            long_stage="exit_long",
        )
        state.pending_passive_closes[position.position_id] = pending

        # Drive once: maker already filled, hedge will fail
        asyncio.run(
            executor.drive_pending_passive_close(state, position.position_id)
        )

        # Should have escalated to DUAL_TAKER
        assert pending.phase_state.phase == PassiveExecutionPhase.DUAL_TAKER, (
            f"Expected DUAL_TAKER after non-retryable hedge error, got {pending.phase_state.phase}"
        )

        events = journal.read_all()
        escalated = [
            e for e in events
            if e.get("kind") == "exit.passive_close_hedge_non_retryable_escalated"
        ]
        assert len(escalated) == 1
        assert "ReduceOnly" in escalated[0]["payload"]["hedge_error"]


class TestPassiveManagerProfileAndConfig:
    def test_default_profile(self):
        profile = PassiveCloseManagerProfile()
        assert profile.amend_threshold_bps == 5.0
        assert profile.max_consecutive_failures == 3

    def test_config_overrides(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor(
            {}, journal,
            config_overrides={
                "max_zero_fill_cycles": 5,
                "default_tick_size": 0.5,
                "close_chunk_max_notional_quote": 1000.0,
            },
        )
        assert executor._config.max_zero_fill_cycles == 5
        assert executor._config.default_tick_size == 0.5
        assert executor._config.close_chunk_max_notional_quote == 1000.0
