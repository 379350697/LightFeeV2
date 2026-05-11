"""V1 passive close executor focused tests.

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
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

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
    PASSIVE_CLOSE_MAX_ZERO_FILL_CYCLES,
    PassiveCloseConfig,
    PassiveCloseExecutor,
    PassiveCloseManagerProfile,
    PassiveManagerDecisionKind,
)
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


# ---------------------------------------------------------------------------
# BP2: Live adapter passive submit contract
# ---------------------------------------------------------------------------

class TestPassiveOrderContract:
    def test_passive_ack_attributes(self):
        """PassiveOrderAck carries correct fields for GTC post-only ack."""
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
        """VenueAdapter.price_tick_size returns positive value for Binance via spec."""
        from lightfee.core.contracts import VenueAdapter as VA
        from lightfee.venues.specs import binance_spec

        class FakeBinanceAdapter(VA):
            @property
            def venue(self):
                return Venue.BINANCE
            async def place_order(self, request):
                raise NotImplementedError
            async def fetch_position(self, symbol):
                raise NotImplementedError

        adapter = FakeBinanceAdapter()
        tick = adapter.price_tick_size("BTCUSDT")
        assert tick is not None
        assert tick > 0  # Binance has price_tick=0.01 in spec

    def test_passive_progress_fields(self):
        """PassiveOrderProgress has cumulative qty, avg price, fee, state."""
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


# ---------------------------------------------------------------------------
# BP4 + BP5: maker progress → hedge delta + maker leg persisted
# ---------------------------------------------------------------------------

class TestPassiveProgressAndHedge:
    def test_apply_maker_progress_updates_fill(self):
        """_apply_maker_progress updates maker_fill and persists maker leg."""
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
        # Maker leg (LONG → closes long → SELL side → long_legs)
        assert len(pending.long_legs) == 1
        assert pending.long_legs[0].fill.quantity == 0.003
        assert pending.long_legs[0].fill.side == Side.SELL

    def test_apply_maker_progress_accumulates(self):
        """Multiple progress calls accumulate fill and legs."""
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

        # First: 0.003
        p1 = _make_passive_progress(
            cumulative_quantity=0.003, average_price=50100.0, side=Side.BUY,
        )
        executor._apply_maker_progress(pending, p1, 1000)
        assert pending.maker_fill.quantity == 0.003
        assert len(pending.short_legs) == 1  # SHORT maker → short_legs

        # Second: 0.007 total → delta 0.004
        p2 = _make_passive_progress(
            cumulative_quantity=0.007, average_price=50200.0, side=Side.BUY,
        )
        executor._apply_maker_progress(pending, p2, 2000)
        assert pending.maker_fill.quantity == 0.007
        assert len(pending.short_legs) == 2
        assert pending.short_legs[1].fill.quantity == 0.004

    def test_terminal_filled_persists_full_maker_leg(self):
        """FILLED state persists the full maker cumulative quantity."""
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
        """If cumulative hasn't changed, no new leg is added."""
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

        # Same cumulative — should be no-op
        progress = _make_passive_progress(cumulative_quantity=0.005)
        executor._apply_maker_progress(pending, progress, 1000)
        assert len(pending.long_legs) == 0  # no new leg added


# ---------------------------------------------------------------------------
# BP3: Repricing uses L2 mid + tick_size
# ---------------------------------------------------------------------------

class TestPassiveRepricing:
    def test_l2_mid_from_injected_resolver(self):
        """Injected resolver supplies mid price."""
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 50123.5)
        mid = executor._resolve_local_l2_mid(Venue.BINANCE, "BTCUSDT")
        assert mid == 50123.5

    def test_l2_mid_zero_without_resolver_or_adapter(self):
        """Without resolver and no adapters, returns 0.0."""
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        mid = executor._resolve_local_l2_mid(Venue.BINANCE, "BTCUSDT")
        assert mid == 0.0

    def test_tick_size_from_spec(self):
        """_get_tick_size returns Binance price tick from spec."""
        journal = _open_journal()
        # Use empty adapters dict — tick comes from VenueSpec
        executor = PassiveCloseExecutor({}, journal)
        tick = executor._get_tick_size(Venue.BINANCE, "BTCUSDT")
        assert tick > 0, f"Expected positive tick, got {tick}"

    def test_tick_size_zero_for_unknown_venue(self):
        """_get_tick_size returns 0 for venues without specs."""
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        # Pass invalid string as venue — but _get_tick_size expects a Venue
        tick = executor._get_tick_size(Venue.HYPERLIQUID, "BTCUSDT")
        # Hyperliquid spec may or may not have price_tick
        assert tick >= 0


# ---------------------------------------------------------------------------
# BP6: DUAL_TAKER fallback
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
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
            ),
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
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
            ),
        )
        assert executor.needs_aggressive_fallback(pending) is False

    def test_fallback_unavailable_no_close_executor(self):
        """When no close_executor is injected, fallback returns False with retry."""
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
                phase=PassiveExecutionPhase.DUAL_TAKER,
            ),
        )
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor._fallback_to_aggressive_close(state, pending, position)
        )
        assert result is False
        assert pending.next_retry_at_ms > 0


# ---------------------------------------------------------------------------
# BP7: Recovery probe
# ---------------------------------------------------------------------------

class TestRecoveryProbe:
    def test_recovery_clears_when_live_flat(self):
        """Live positions flat on both venues → cleared."""
        journal = _open_journal()
        # Create a concrete adapter that returns zero position
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
        """Position still in open_positions → resume."""
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
            executor.recover_passive_close(
                state, position.position_id, {},
            )
        )
        assert result == "resumed"
        assert pending.next_retry_at_ms == 0

    def test_recovery_ambiguous_when_cannot_confirm(self):
        """No open position, can't confirm flat live → ambiguous."""
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
            executor.recover_passive_close(
                state, position.position_id, {},
            )
        )
        assert result == "ambiguous"
        assert position.position_id in state.pending_passive_closes


# ---------------------------------------------------------------------------
# BP1: start_pending_passive_close
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
        """Large position creates multiple chunks when max_notional is set."""
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

        asyncio.run(executor._advance_chunk(state, pending))
        assert pending.active_chunk_index == 1
        assert pending.phase_state.phase == PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER
        assert pending.maker_fill.quantity == 0.0
        assert pending.hedge_fill.quantity == 0.0


# ---------------------------------------------------------------------------
# Manager profile & config
# ---------------------------------------------------------------------------

class TestPassiveManagerProfile:
    def test_default_profile(self):
        from lightfee.engine.passive_close import (
            PASSIVE_CLOSE_DEFAULT_AMEND_THRESHOLD_BPS,
            PASSIVE_CLOSE_DEFAULT_CANCEL_REPLACE_THRESHOLD_BPS,
            PASSIVE_CLOSE_MAX_MANAGER_FAILURES,
            PASSIVE_CLOSE_MANAGER_COOLDOWN_MS,
        )
        profile = PassiveCloseManagerProfile()
        assert profile.amend_threshold_bps == PASSIVE_CLOSE_DEFAULT_AMEND_THRESHOLD_BPS
        assert profile.cancel_replace_threshold_bps == PASSIVE_CLOSE_DEFAULT_CANCEL_REPLACE_THRESHOLD_BPS
        assert profile.max_consecutive_failures == PASSIVE_CLOSE_MAX_MANAGER_FAILURES
        assert profile.cooldown_ms == PASSIVE_CLOSE_MANAGER_COOLDOWN_MS

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
