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
import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from lightfee.core.contracts import VenueAdapter

# Monkeypatch VenueAdapter to define fetch_open_orders by default returning []
# so mock adapters in tests are trusted to have no open orders.
async def _default_fetch_open_orders(self, symbol: str) -> list:
    return []
VenueAdapter.fetch_open_orders = _default_fetch_open_orders

from lightfee.core.domain import (
    OrderFill,
    OrderFillReconciliation,
    OrderRequest,
    PassiveOrderAck,
    PassiveOrderAmendRequest,
    PassiveOrderProgress,
    PassiveOrderState,
    PositionSnapshot,
    Side,
    TimeInForce,
    Venue,
    VenueMarketQuote,
    VenueMarketSnapshot,
)
from lightfee.engine.close_executor import CloseExecutionLeg
from lightfee.engine.exit import CloseExecution
import lightfee.engine.passive_close as passive_close_module
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
    adapter.normalize_quantity = AsyncMock(side_effect=lambda symbol, quantity: quantity)
    adapter.query_passive_order_progress = AsyncMock(return_value=None)
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


def _attach_bybit_min_notional_transport(adapter, min_notional="5"):
    """Attach a Bybit instruments-info stub with an explicit minNotionalValue."""
    from lightfee.venues.specs import get_spec
    from lightfee.venues.symbol_rules import get_symbol_rules_cache

    class FakeBybitTransport:
        _spec = get_spec(Venue.BYBIT)

        def _venue_symbol(self, symbol):
            return symbol

        async def _public_get(self, path, params=None):
            assert path == "/v5/market/instruments-info"
            symbol = (params or {}).get("symbol", "BEATUSDT")
            return {
                "result": {
                    "list": [
                        {
                            "symbol": symbol,
                            "priceFilter": {"tickSize": "0.0001"},
                            "lotSizeFilter": {
                                "qtyStep": "1",
                                "minOrderQty": "1",
                                "minNotionalValue": str(min_notional),
                            },
                        }
                    ]
                }
            }

    get_symbol_rules_cache().clear()
    adapter._transport = FakeBybitTransport()
    return adapter


def _attach_okx_instrument_transport(adapter, *, venue_symbol="SPACE-USDT-SWAP"):
    """Attach an OKX instruments stub with official sizing metadata."""
    from lightfee.venues.specs import get_spec
    from lightfee.venues.symbol_rules import get_symbol_rules_cache

    class FakeOkxTransport:
        _spec = get_spec(Venue.OKX)

        def _venue_symbol(self, symbol):
            return venue_symbol

        async def _public_get(self, path, params=None):
            assert path == "/api/v5/public/instruments"
            assert params == {"instType": "SWAP", "instId": venue_symbol}
            return {
                "data": [
                    {
                        "instId": venue_symbol,
                        "tickSz": "0.000001",
                        "lotSz": "1",
                        "minSz": "1",
                        "ctVal": "100",
                        "maxMktSz": "100000",
                    }
                ]
            }

    get_symbol_rules_cache().clear()
    adapter._transport = FakeOkxTransport()
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


class TestMakerProgressTruthGate:
    def test_maker_progress_query_timeout_does_not_consume_zero_fill_cycle(self):
        """Order truth outage is retryable truth gap, not a no-fill maker cycle."""
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
                zero_fill_cycles_in_phase=0,
            ),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        maker_adapter = _mock_adapter_with_tick(Venue.BINANCE)
        maker_adapter.query_passive_order_progress = AsyncMock(
            side_effect=TimeoutError("order query timeout")
        )
        maker_adapter.submit_passive_order = AsyncMock()
        hedge_adapter = _mock_adapter_passive_ok(Venue.OKX)

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: maker_adapter, Venue.OKX: hedge_adapter},
            journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)
        executor.set_l2_quote_resolver(lambda venue, symbol: (49999.99, 50000.01))

        result = asyncio.run(
            executor.drive_pending_passive_close(
                state,
                position.position_id,
                wait_until_terminal=False,
            )
        )

        assert result is False
        assert pending.phase_state.zero_fill_cycles_in_phase == 0
        assert pending.phase_state.phase == PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER
        maker_adapter.submit_passive_order.assert_not_called()
        events = journal.read_all()
        truth_gap = [
            e["payload"] for e in events
            if e.get("kind") == "exit.passive_close_order_truth_unavailable"
        ]
        assert truth_gap
        assert truth_gap[-1]["source"] == "poll_maker_progress"
        assert truth_gap[-1]["next_action"] == "retry_progress_poll"


class TestPassiveProgressAndHedge:
    def test_apply_maker_progress_updates_fill(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor(
            {},
            journal,
            config_overrides={"runtime_mode": "paper"},
        )
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
        executor = PassiveCloseExecutor(
            {}, journal, config_overrides={"runtime_mode": "paper"},
        )
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
        executor = PassiveCloseExecutor(
            {}, journal, config_overrides={"runtime_mode": "paper"},
        )
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

    def test_source_aware_ws_bbo_quote_labels_hedge_reference_price(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        executor.set_l2_quote_resolver(
            lambda venue, symbol: (99.0, 101.0, "ws_bbo_quote_lease")
        )

        buy_price, buy_source = asyncio.run(
            executor._resolve_hedge_reference_price(
                Venue.BINANCE, "BTCUSDT", Side.BUY, 0.0,
            )
        )
        sell_price, sell_source = asyncio.run(
            executor._resolve_hedge_reference_price(
                Venue.BINANCE, "BTCUSDT", Side.SELL, 0.0,
            )
        )

        assert buy_price == 101.0
        assert buy_source == "ws_bbo_quote_lease_best_ask"
        assert sell_price == 99.0
        assert sell_source == "ws_bbo_quote_lease_best_bid"

    def test_source_aware_ws_bbo_mid_labels_hedge_reference_price(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        executor.set_l2_quote_resolver(lambda venue, symbol: None)
        executor.set_l2_mid_resolver(lambda venue, symbol: (100.5, "ws_bbo_quote_lease"))

        price, source = asyncio.run(
            executor._resolve_hedge_reference_price(
                Venue.BINANCE, "BTCUSDT", Side.BUY, 0.0,
            )
        )

        assert price == 100.5
        assert source == "ws_bbo_quote_lease_mid"

    def test_ws_bbo_maker_leg_gap_journal_uses_quote_source_not_local_l2(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)
        executor.set_l2_mid_resolver(
            lambda venue, symbol: (
                50000.0 if venue == Venue.BINANCE else 0.0,
                "ws_bbo_quote_lease",
            )
        )
        executor._estimate_venue_taker_cost_bps = lambda venue, symbol, l2_mid=0.0: 5.0

        position = _make_position(long_venue=Venue.BINANCE, short_venue=Venue.OKX)
        leg = executor._select_preferred_maker_leg(position)

        assert leg == ActiveMakerLeg.LONG
        events = journal.read_all()
        ws_bbo_missing = [
            e for e in events
            if e.get("kind") == "exit.passive_close_maker_leg_quote_evidence_missing"
        ]
        assert len(ws_bbo_missing) == 1
        payload = ws_bbo_missing[0]["payload"]
        assert payload["long_price_source"] == "ws_bbo_quote_lease"
        assert payload["short_price_source"] == "ws_bbo_quote_lease"
        selected = [
            e for e in events
            if e.get("kind") == "exit.passive_close_maker_leg_selected"
        ][-1]["payload"]
        assert selected["long_price_evidence_available"] is True
        assert selected["short_price_evidence_available"] is False
        assert selected["long_price_source"] == "ws_bbo_quote_lease"

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

    def test_fallback_unavailable_past_v1_deadline_enters_fail_closed(self):
        """V1 parity: fallback execution unavailable cannot keep retrying after deadline."""
        from lightfee.risk.modes import GlobalRiskMode

        journal = _open_journal()
        executor = PassiveCloseExecutor(
            {},
            journal,
            config_overrides={"maker_hedge_deadline_ms": 800},
        )
        state = EngineState()
        position = _make_position(
            matched_quantity=1.0,
            long_quantity=1.0,
            short_quantity=1.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=1.0,
            chunk_quantities=[1.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
                phase_started_at_ms=1_000,
            ),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending
        executor._now_ms = lambda: 1_801

        result = asyncio.run(
            executor._fallback_to_aggressive_close(state, pending, position)
        )

        assert result is False
        assert state.risk_mode == GlobalRiskMode.FAIL_CLOSED
        assert pending.next_retry_at_ms == 0
        kinds = [record["kind"] for record in journal.read_all()]
        assert "execution.close_deadline_breached" in kinds
        assert "exit.passive_close_fallback_unavailable" not in kinds

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

    def test_beatusdt_recovered_start_clears_live_flat_before_orders(self):
        """Recovered passive close must prove live flat before hedge/maker work."""
        journal = _open_journal()
        long_adapter = _mock_adapter_passive_ok(Venue.OKX)
        short_adapter = _mock_adapter_passive_ok(Venue.BYBIT)
        long_adapter.fetch_position = AsyncMock(return_value=PositionSnapshot(
            venue=Venue.OKX, symbol="BEATUSDT", side=Side.BUY,
            quantity=0.0, entry_price=0.0, observed_at_ms=3000,
        ))
        short_adapter.fetch_position = AsyncMock(return_value=PositionSnapshot(
            venue=Venue.BYBIT, symbol="BEATUSDT", side=Side.SELL,
            quantity=0.0, entry_price=0.0, observed_at_ms=3000,
        ))
        executor = PassiveCloseExecutor(
            {Venue.OKX: long_adapter, Venue.BYBIT: short_adapter}, journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 0.0)
        state = EngineState()
        position = _make_position(
            position_id="live-recovered:BEATUSDT:okx->bybit",
            symbol="BEATUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            long_quantity=6_000.0,
            short_quantity=6_000.0,
            matched_quantity=6_000.0,
        )
        state.open_positions[position.position_id] = position
        state.recovery_blocked_reason = "startup_recovery_pending_work_without_open_positions"
        state.recovery_blocked_at_ms = 1234

        pending = asyncio.run(
            executor.start_pending_passive_close(
                state, position, "funding_capture",
                long_price_hint=0.0, short_price_hint=0.0,
            )
        )

        assert pending is None
        assert position.position_id not in state.open_positions
        assert position.position_id not in state.pending_passive_closes
        long_adapter.submit_passive_order.assert_not_called()
        short_adapter.submit_passive_order.assert_not_called()
        long_adapter.place_order.assert_not_called()
        short_adapter.place_order.assert_not_called()

        kinds = [e.get("kind") for e in journal.read_all()]
        assert "recovery.flat" in kinds
        assert "runtime.position_drift_corrected" in kinds
        assert "recovery.legacy_block_cleared" in kinds
        assert "order.submit_attempt" not in kinds
        assert "exit.passive_close_hedge_dust_aborted" not in kinds
        assert state.recovery_blocked_reason is None
        assert state.recovery_blocked_at_ms == 0


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
        executor = PassiveCloseExecutor(
            {}, journal, config_overrides={"runtime_mode": "paper"},
        )
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

    def test_terminal_small_maker_fill_below_min_notional_compensates_flat(self):
        """V1 parity: terminal maker dust aborts and compensates in the same drive."""
        journal = _open_journal()

        from lightfee.engine.close_executor import CloseExecutor

        class SequencedPositionAdapter(VenueAdapter):
            def __init__(self, venue, snapshots):
                self._venue = venue
                self._snapshots = list(snapshots)
                self.place_order_calls = []

            @property
            def venue(self):
                return self._venue

            async def normalize_quantity(self, symbol, quantity):
                return quantity

            async def place_order(self, request):
                self.place_order_calls.append(request)
                return OrderFill(
                    venue=self._venue,
                    symbol=request.symbol,
                    side=request.side,
                    quantity=request.quantity,
                    price=request.price or 1.0211,
                    order_id=f"{self._venue.value}-fill",
                    client_order_id=request.client_order_id,
                    fee_quote=0.0,
                    filled_at_ms=2000,
                )

            async def fetch_position(self, symbol):
                if self._snapshots:
                    qty, side = self._snapshots.pop(0)
                else:
                    qty, side = 0.0, Side.SELL
                return PositionSnapshot(
                    venue=self._venue,
                    symbol=symbol,
                    side=side,
                    quantity=qty,
                    entry_price=1.0211 if qty else 0.0,
                    observed_at_ms=2000,
                )

        class MakerAdapter(SequencedPositionAdapter):
            async def query_passive_order_progress(self, symbol, order_id, client_order_id, side):
                return PassiveOrderProgress(
                    venue=self._venue,
                    symbol=symbol,
                    side=side,
                    order_id=order_id,
                    client_order_id=client_order_id,
                    cumulative_quantity=2.0,
                    average_price=1.0211,
                    fee_quote=0.0,
                    last_fill_time_ms=1500,
                    state=PassiveOrderState.FILLED,
                    observed_at_ms=1500,
                )

        okx = MakerAdapter(Venue.OKX, [(0.0, Side.BUY), (0.0, Side.BUY)])
        bybit = SequencedPositionAdapter(
            Venue.BYBIT,
            [(2.0, Side.SELL), (2.0, Side.SELL), (0.0, Side.SELL)],
        )
        _attach_bybit_min_notional_transport(bybit)
        adapters = {Venue.OKX: okx, Venue.BYBIT: bybit}
        executor = PassiveCloseExecutor(adapters, journal)
        executor.set_close_executor(CloseExecutor(adapters, journal))
        executor.set_l2_mid_resolver(lambda venue, symbol: 1.0211)

        state = EngineState()
        position = _make_position(
            position_id="entry-beat-terminal-dust",
            symbol="BEATUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            long_quantity=20.0,
            short_quantity=20.0,
            matched_quantity=20.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=20.0,
            chunk_quantities=[20.0],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="oid-maker",
                maker_client_order_id="cid-maker",
            ),
            maker_fill=PendingPassiveLegFill(),
            hedge_fill=PendingPassiveLegFill(),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False)
        )

        assert result is True
        assert [request.quantity for request in bybit.place_order_calls] == [2.0]
        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions

        kinds = [record["kind"] for record in journal.read_all()]
        assert "execution.min_notional_accumulating" in kinds
        assert "execution.min_notional_abort_and_flatten" in kinds
        assert "exit.compensated" in kinds
        assert "exit.passive_close_fallback_unhedged_failed" not in kinds

    def test_terminal_maker_filled_bybit_dust_gap_uses_guard_and_live_flat_cleanup(self):
        """Terminal FILLED UBUSDT gap below Bybit dynamic min qty must not submit hedge."""
        journal = _open_journal()

        maker_adapter = _mock_adapter_passive_ok(Venue.BINANCE)
        maker_adapter.query_passive_order_progress = AsyncMock(return_value=_make_passive_progress(
            venue=Venue.BINANCE, symbol="UBUSDT", side=Side.SELL,
            cumulative_quantity=1.0, average_price=0.01,
            state=PassiveOrderState.FILLED,
        ))
        maker_adapter.fetch_position = AsyncMock(return_value=PositionSnapshot(
            venue=Venue.BINANCE, symbol="UBUSDT", side=Side.BUY,
            quantity=0.0, entry_price=0.0, observed_at_ms=2000,
        ))

        hedge_adapter = _mock_adapter_with_tick(Venue.BYBIT)
        hedge_adapter.normalize_quantity = AsyncMock(return_value=0.0)
        hedge_adapter.place_order = AsyncMock(return_value=_make_order_fill(
            venue=Venue.BYBIT, symbol="UBUSDT", side=Side.BUY, quantity=1.0, price=0.01,
        ))
        hedge_adapter.fetch_position = AsyncMock(return_value=PositionSnapshot(
            venue=Venue.BYBIT, symbol="UBUSDT", side=Side.SELL,
            quantity=0.0, entry_price=0.0, observed_at_ms=2000,
        ))

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: maker_adapter, Venue.BYBIT: hedge_adapter}, journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 0.01)

        state = EngineState()
        position = _make_position(
            position_id="entry-ubusdt",
            symbol="UBUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=1.0,
            short_quantity=1.0,
            matched_quantity=1.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=1.0,
            chunk_quantities=[1.0],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="oid-maker",
                maker_client_order_id="cid-maker",
                maker_resting_limit_price=0.01,
            ),
            maker_fill=PendingPassiveLegFill(),
            hedge_fill=PendingPassiveLegFill(),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False)
        )

        assert result is True
        hedge_adapter.normalize_quantity.assert_awaited_once_with("UBUSDT", 1.0)
        hedge_adapter.place_order.assert_not_called()
        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions

        kinds = [e.get("kind") for e in journal.read_all()]
        assert "exit.passive_close_hedge_dust_aborted" in kinds
        assert "recovery.flat" in kinds
        assert "runtime.position_drift_corrected" in kinds

    def test_terminal_maker_filled_okx_dust_logs_official_rule_source(self):
        """OKX normalized-zero dust must carry instrument metadata evidence."""
        from lightfee.venues.symbol_rules import get_symbol_rules_cache

        journal = _open_journal()

        maker_adapter = _mock_adapter_passive_ok(Venue.BINANCE)
        maker_adapter.query_passive_order_progress = AsyncMock(return_value=_make_passive_progress(
            venue=Venue.BINANCE, symbol="SPACEUSDT", side=Side.SELL,
            cumulative_quantity=39.0, average_price=0.006,
            state=PassiveOrderState.FILLED,
        ))
        maker_adapter.fetch_position = AsyncMock(return_value=PositionSnapshot(
            venue=Venue.BINANCE, symbol="SPACEUSDT", side=Side.BUY,
            quantity=0.0, entry_price=0.0, observed_at_ms=2000,
        ))

        hedge_adapter = _mock_adapter_with_tick(Venue.OKX)
        _attach_okx_instrument_transport(hedge_adapter)
        hedge_adapter.normalize_quantity = AsyncMock(return_value=0.0)
        hedge_adapter.place_order = AsyncMock(return_value=_make_order_fill(
            venue=Venue.OKX, symbol="SPACEUSDT", side=Side.BUY, quantity=39.0, price=0.006,
        ))
        hedge_adapter.fetch_position = AsyncMock(return_value=PositionSnapshot(
            venue=Venue.OKX, symbol="SPACEUSDT", side=Side.SELL,
            quantity=0.0, entry_price=0.0, observed_at_ms=2000,
        ))

        try:
            executor = PassiveCloseExecutor(
                {Venue.BINANCE: maker_adapter, Venue.OKX: hedge_adapter}, journal,
            )
            executor.set_l2_mid_resolver(lambda venue, symbol: 0.006)

            state = EngineState()
            position = _make_position(
                position_id="entry-spaceusdt",
                symbol="SPACEUSDT",
                long_venue=Venue.BINANCE,
                short_venue=Venue.OKX,
                long_quantity=39.0,
                short_quantity=39.0,
                matched_quantity=39.0,
            )
            pending = PendingPassiveClose(
                position_id=position.position_id,
                reason="funding_capture",
                position_snapshot=position,
                target_quantity=39.0,
                chunk_quantities=[39.0],
                active_chunk_index=0,
                phase_state=PassivePhaseState(
                    phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                    active_maker_leg=ActiveMakerLeg.LONG,
                    maker_order_id="oid-maker",
                    maker_client_order_id="cid-maker",
                    maker_resting_limit_price=0.006,
                ),
                maker_fill=PendingPassiveLegFill(),
                hedge_fill=PendingPassiveLegFill(),
            )
            state.open_positions[position.position_id] = position
            state.pending_passive_closes[position.position_id] = pending

            result = asyncio.run(
                executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False)
            )
        finally:
            get_symbol_rules_cache().clear()

        assert result is True
        hedge_adapter.place_order.assert_not_called()
        dust = next(
            e["payload"] for e in journal.read_all()
            if e.get("kind") == "exit.passive_close_hedge_dust_aborted"
        )
        assert dust["symbol"] == "SPACEUSDT"
        assert dust["venue_symbol"] == "SPACE-USDT-SWAP"
        assert dust["min_notional_source"] == "instrument"
        assert dust["rule_source"] == "instrument"
        assert dust["rule_min_quantity"] == 1.0
        assert dust["rule_qty_step"] == 1.0
        assert dust["rule_ct_val"] == 100.0

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
            {Venue.BINANCE: maker_adapter, Venue.OKX: hedge_adapter},
            journal,
            config_overrides={"runtime_mode": "paper"},
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
            {Venue.BINANCE: maker_adapter, Venue.OKX: hedge_adapter},
            journal,
            config_overrides={"runtime_mode": "paper"},
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
            {Venue.BINANCE: maker_adapter, Venue.OKX: hedge_adapter},
            journal,
            config_overrides={"runtime_mode": "paper"},
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

    def test_delta_hedge_resolves_market_price_when_local_hint_zero(self):
        """price_hint=0 must not become a fake min_notional_rejected notional=0."""
        journal = _open_journal()
        maker_adapter = _mock_adapter_passive_ok(Venue.BINANCE)
        maker_adapter.query_passive_order_progress = AsyncMock(return_value=_make_passive_progress(
            venue=Venue.BINANCE, symbol="BEATUSDT", side=Side.SELL,
            cumulative_quantity=6_000.0, average_price=0.0,
            state=PassiveOrderState.FILLED,
        ))
        hedge_adapter = _mock_adapter_with_tick(Venue.BYBIT)
        hedge_adapter.normalize_quantity = AsyncMock(return_value=6_000.0)
        hedge_adapter.fetch_market_snapshot = AsyncMock(return_value=VenueMarketSnapshot(
            venue=Venue.BYBIT,
            observed_at_ms=3000,
            quotes=(VenueMarketQuote(symbol="BEATUSDT", bid=0.0019, ask=0.0021),),
        ))
        hedge_adapter.place_order = AsyncMock(return_value=_make_order_fill(
            venue=Venue.BYBIT, symbol="BEATUSDT", side=Side.BUY,
            quantity=6_000.0, price=0.0021,
        ))
        executor = PassiveCloseExecutor(
            {Venue.BINANCE: maker_adapter, Venue.BYBIT: hedge_adapter}, journal,
            config_overrides={
                "runtime_mode": "paper",
                "small_fill_buffer_notional_quote": 10.0,
            },
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 0.0)

        state = EngineState()
        position = _make_position(
            position_id="entry-beatusdt-price",
            symbol="BEATUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=6_000.0,
            short_quantity=6_000.0,
            matched_quantity=6_000.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=6_000.0,
            chunk_quantities=[6_000.0],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="oid-maker",
                maker_client_order_id="cid-maker",
                maker_resting_limit_price=0.002,
            ),
            maker_fill=PendingPassiveLegFill(),
            hedge_fill=PendingPassiveLegFill(),
        )
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor.drive_pending_passive_close(
                state, position.position_id, wait_until_terminal=False,
            )
        )

        assert result is True
        hedge_adapter.fetch_market_snapshot.assert_awaited_once_with(["BEATUSDT"])
        hedge_adapter.place_order.assert_awaited_once()
        kinds = [e.get("kind") for e in journal.read_all()]
        assert "exit.passive_close_hedge_dust_aborted" not in kinds

    def test_delta_hedge_classifies_missing_price_separately_from_min_notional(self):
        journal = _open_journal()
        maker_adapter = _mock_adapter_passive_ok(Venue.BINANCE)
        maker_adapter.query_passive_order_progress = AsyncMock(return_value=_make_passive_progress(
            venue=Venue.BINANCE, symbol="BEATUSDT", side=Side.SELL,
            cumulative_quantity=6_000.0, average_price=0.0,
            state=PassiveOrderState.FILLED,
        ))
        hedge_adapter = _mock_adapter_with_tick(Venue.BYBIT)
        hedge_adapter.normalize_quantity = AsyncMock(return_value=6_000.0)
        hedge_adapter.fetch_market_snapshot = AsyncMock(side_effect=RuntimeError("ticker unavailable"))
        hedge_adapter.place_order = AsyncMock(return_value=_make_order_fill(
            venue=Venue.BYBIT, symbol="BEATUSDT", side=Side.BUY,
            quantity=6_000.0, price=0.0,
        ))
        executor = PassiveCloseExecutor(
            {Venue.BINANCE: maker_adapter, Venue.BYBIT: hedge_adapter}, journal,
            config_overrides={
                "runtime_mode": "paper",
                "small_fill_buffer_notional_quote": 10.0,
            },
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 0.0)

        state = EngineState()
        position = _make_position(
            position_id="entry-beatusdt-price-missing",
            symbol="BEATUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=6_000.0,
            short_quantity=6_000.0,
            matched_quantity=6_000.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=6_000.0,
            chunk_quantities=[6_000.0],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="oid-maker",
                maker_client_order_id="cid-maker",
                maker_resting_limit_price=0.002,
            ),
            maker_fill=PendingPassiveLegFill(),
            hedge_fill=PendingPassiveLegFill(),
        )
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor.drive_pending_passive_close(
                state, position.position_id, wait_until_terminal=False,
            )
        )

        assert result is True
        hedge_adapter.place_order.assert_awaited_once()
        kinds = [e.get("kind") for e in journal.read_all()]
        assert "exit.passive_close_hedge_dust_aborted" not in kinds

    def test_delta_hedge_uses_bybit_instrument_min_notional_not_buffer(self):
        """Bybit hard min-notional comes from instruments-info, not local buffer."""
        from lightfee.venues.specs import get_spec
        from lightfee.venues.symbol_rules import get_symbol_rules_cache

        class FakeBybitTransport:
            _spec = get_spec(Venue.BYBIT)

            def _venue_symbol(self, symbol):
                return symbol

            async def _public_get(self, path, params=None):
                assert path == "/v5/market/instruments-info"
                assert params == {"category": "linear", "symbol": "BEATUSDT"}
                return {
                    "result": {
                        "list": [
                            {
                                "symbol": "BEATUSDT",
                                "priceFilter": {"tickSize": "0.0001"},
                                "lotSizeFilter": {
                                    "qtyStep": "1",
                                    "minOrderQty": "1",
                                    "minNotionalValue": "1",
                                },
                            }
                        ]
                    }
                }

        get_symbol_rules_cache().clear()
        journal = _open_journal()
        hedge_adapter = _mock_adapter_with_tick(Venue.BYBIT)
        hedge_adapter._transport = FakeBybitTransport()
        hedge_adapter.normalize_quantity = AsyncMock(return_value=2.0)
        hedge_adapter.place_order = AsyncMock(return_value=_make_order_fill(
            venue=Venue.BYBIT, symbol="BEATUSDT", side=Side.BUY,
            quantity=2.0, price=1.0,
        ))
        executor = PassiveCloseExecutor(
            {Venue.BYBIT: hedge_adapter}, journal,
            config_overrides={"small_fill_buffer_notional_quote": 10.0},
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 1.0)

        position = _make_position(
            position_id="entry-beatusdt-dynamic-min",
            symbol="BEATUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            matched_quantity=2.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=2.0,
            chunk_quantities=[2.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=2.0, average_price=1.0),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
        )

        result = asyncio.run(
            executor._submit_hedge_for_delta(
                EngineState(), pending, position, 2.0, maker_terminal=True,
            )
        )

        assert result.success is True
        hedge_adapter.place_order.assert_awaited_once()
        kinds = [e.get("kind") for e in journal.read_all()]
        assert "exit.passive_close_hedge_dust_aborted" not in kinds

    def test_okx_market_snapshot_matches_swap_inst_id_for_price(self):
        journal = _open_journal()
        adapter = _mock_adapter_with_tick(Venue.OKX)
        adapter.fetch_market_snapshot = AsyncMock(return_value=VenueMarketSnapshot(
            venue=Venue.OKX,
            observed_at_ms=3000,
            quotes=(
                VenueMarketQuote(symbol="OTHER-USDT-SWAP", bid=1.0, ask=1.1),
                VenueMarketQuote(symbol="BEAT-USDT-SWAP", bid=0.0019, ask=0.0021),
            ),
        ))
        executor = PassiveCloseExecutor({Venue.OKX: adapter}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 0.0)

        price, source = asyncio.run(
            executor._resolve_hedge_reference_price(
                Venue.OKX, "BEATUSDT", Side.BUY, 0.0,
            )
        )

        assert price == pytest.approx(0.0021)
        assert source == "market_snapshot_best_ask"

    def test_generic_delta_hedge_bybit_dust_gap_uses_same_guard(self):
        """Non-terminal delta hedge must reuse the same pre-submit dust guard."""
        journal = _open_journal()

        maker_adapter = _mock_adapter_passive_ok(Venue.BINANCE)
        maker_adapter.query_passive_order_progress = AsyncMock(return_value=_make_passive_progress(
            venue=Venue.BINANCE, symbol="UBUSDT", side=Side.SELL,
            cumulative_quantity=1.0, average_price=20.0,
            state=PassiveOrderState.PARTIALLY_FILLED,
        ))

        hedge_adapter = _mock_adapter_with_tick(Venue.BYBIT)
        hedge_adapter.normalize_quantity = AsyncMock(return_value=0.0)
        hedge_adapter.place_order = AsyncMock(return_value=_make_order_fill(
            venue=Venue.BYBIT, symbol="UBUSDT", side=Side.BUY, quantity=1.0, price=20.0,
        ))

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: maker_adapter, Venue.BYBIT: hedge_adapter}, journal,
            config_overrides={"small_fill_buffer_notional_quote": 0.0},
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 20.0)

        state = EngineState()
        position = _make_position(
            position_id="entry-ubusdt-generic",
            symbol="UBUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=20.0,
            short_quantity=20.0,
            matched_quantity=20.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=20.0,
            chunk_quantities=[20.0],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="oid-maker",
                maker_client_order_id="cid-maker",
                maker_resting_limit_price=20.0,
            ),
            maker_fill=PendingPassiveLegFill(),
            hedge_fill=PendingPassiveLegFill(),
        )
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False)
        )

        assert result is False
        hedge_adapter.normalize_quantity.assert_awaited_once_with("UBUSDT", 1.0)
        hedge_adapter.place_order.assert_not_called()
        assert pending.hedge_fill.quantity == 0.0
        assert position.position_id in state.pending_passive_closes

        kinds = [e.get("kind") for e in journal.read_all()]
        assert "exit.passive_close_hedge_dust_aborted" in kinds
        assert "exit.passive_close_hedge_error" not in kinds

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
            executor = PassiveCloseExecutor(
                {},
                _j,
                config_overrides={"runtime_mode": "paper"},
            )
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

    def test_v1_small_maker_fill_below_min_notional_compensates_flat(self):
        """Terminal maker dust must enter V1 abort/compensate semantics, not retry."""
        journal = _open_journal()

        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.engine.recovery import build_persistent_state_view

        class SequencedPositionAdapter(VenueAdapter):
            def __init__(self, venue, snapshots):
                self._venue = venue
                self._snapshots = list(snapshots)
                self.place_order_calls = []

            @property
            def venue(self):
                return self._venue

            async def normalize_quantity(self, symbol, quantity):
                return quantity

            async def place_order(self, request):
                self.place_order_calls.append(request)
                return OrderFill(
                    venue=self._venue,
                    symbol=request.symbol,
                    side=request.side,
                    quantity=request.quantity,
                    price=request.price or 1.0211,
                    order_id=f"{self._venue.value}-fill",
                    client_order_id=request.client_order_id,
                    fee_quote=0.0,
                    filled_at_ms=2000,
                )

            async def fetch_position(self, symbol):
                if self._snapshots:
                    qty, side = self._snapshots.pop(0)
                else:
                    qty, side = 0.0, Side.SELL
                return PositionSnapshot(
                    venue=self._venue,
                    symbol=symbol,
                    side=side,
                    quantity=qty,
                    entry_price=1.0211 if qty else 0.0,
                    observed_at_ms=2000,
                )

        okx = SequencedPositionAdapter(Venue.OKX, [(0.0, Side.BUY), (0.0, Side.BUY)])
        bybit = SequencedPositionAdapter(
            Venue.BYBIT,
            [(2.0, Side.SELL), (2.0, Side.SELL), (0.0, Side.SELL)],
        )
        _attach_bybit_min_notional_transport(bybit)
        adapters = {Venue.OKX: okx, Venue.BYBIT: bybit}
        executor = PassiveCloseExecutor(
            adapters,
            journal,
            config_overrides={"runtime_mode": "paper"},
        )
        executor.set_close_executor(CloseExecutor(adapters, journal))
        executor.set_l2_mid_resolver(lambda venue, symbol: 1.0211)

        state = EngineState()
        position = _make_position(
            position_id="entry-beat-dust",
            symbol="BEATUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            long_quantity=20.0,
            short_quantity=20.0,
            matched_quantity=20.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=20.0,
            chunk_quantities=[20.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=2.0, average_price=1.0211),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
            next_retry_at_ms=0,
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor._fallback_to_aggressive_close(state, pending, position)
        )

        assert result is True
        assert [request.quantity for request in bybit.place_order_calls] == [2.0]
        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions
        assert pending.next_retry_at_ms == 0

        kinds = [record["kind"] for record in journal.read_all()]
        assert "execution.min_notional_accumulating" in kinds
        assert "execution.min_notional_abort_and_flatten" in kinds
        assert "exit.compensated" in kinds
        assert "exit.passive_close_fallback_unhedged_failed" not in kinds

        persisted = build_persistent_state_view(state)
        assert position.position_id not in persisted["open_positions"]
        assert position.position_id not in persisted["pending_passive_closes"]

    def test_beatusdt_stale_local_live_one_sided_rebuilds_close_target(self):
        """Fallback must close live one-sided exposure, not stale 2-BEAT delta."""
        journal = _open_journal()

        from lightfee.engine.close_executor import CloseExecutor

        class LiveAdapter(VenueAdapter):
            def __init__(self, venue, snapshots):
                self._venue = venue
                self._snapshots = list(snapshots)
                self.place_order_calls = []

            @property
            def venue(self):
                return self._venue

            async def normalize_quantity(self, symbol, quantity):
                return quantity

            async def place_order(self, request):
                self.place_order_calls.append(request)
                return OrderFill(
                    venue=self._venue,
                    symbol=request.symbol,
                    side=request.side,
                    quantity=request.quantity,
                    price=request.price or 1.0211,
                    order_id=f"{self._venue.value}-close",
                    client_order_id=request.client_order_id,
                    fee_quote=0.0,
                    filled_at_ms=2000,
                )

            async def fetch_position(self, symbol):
                if self._snapshots:
                    qty, side = self._snapshots.pop(0)
                else:
                    qty, side = 0.0, Side.SELL
                return PositionSnapshot(
                    venue=self._venue,
                    symbol=symbol,
                    side=side,
                    quantity=qty,
                    entry_price=1.0211 if qty else 0.0,
                    observed_at_ms=2000,
                )

        okx = LiveAdapter(Venue.OKX, [(0.0, Side.BUY), (0.0, Side.BUY)])
        bybit = LiveAdapter(Venue.BYBIT, [(20.0, Side.SELL), (0.0, Side.SELL)])
        adapters = {Venue.OKX: okx, Venue.BYBIT: bybit}
        executor = PassiveCloseExecutor(
            adapters, journal, config_overrides={"runtime_mode": "paper"},
        )
        executor.set_close_executor(CloseExecutor(adapters, journal))
        executor.set_l2_mid_resolver(lambda venue, symbol: 1.0211)

        state = EngineState()
        state.last_error = "pending passive close failed for entry-beatusdt"
        position = _make_position(
            position_id="entry-beatusdt",
            symbol="BEATUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            long_quantity=20.0,
            short_quantity=20.0,
            matched_quantity=20.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=20.0,
            chunk_quantities=[20.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=2.0, average_price=1.0211),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor._fallback_to_aggressive_close(state, pending, position)
        )

        assert result is True
        assert okx.place_order_calls == []
        assert len(bybit.place_order_calls) == 1
        request = bybit.place_order_calls[0]
        assert request.quantity == 20.0
        assert request.side == Side.BUY
        assert request.reduce_only is True
        assert request.time_in_force == TimeInForce.IOC
        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions
        assert state.last_error is None

        kinds = [record["kind"] for record in journal.read_all()]
        assert "exit.passive_close_live_one_sided_flatten" in kinds
        assert "exit.passive_close_fallback_unhedged_failed" not in kinds

    def test_live_one_sided_error_force_closes_and_marks_problem(self):
        """If normal one-sided flatten fails, force-close live exposure and mark it."""
        journal = _open_journal()

        from lightfee.engine.close_executor import CloseExecutor

        class ForceCloseAdapter(VenueAdapter):
            def __init__(self, venue, snapshots, *, first_order_raises=False):
                self._venue = venue
                self._snapshots = list(snapshots)
                self._first_order_raises = first_order_raises
                self.place_order_calls = []

            @property
            def venue(self):
                return self._venue

            async def normalize_quantity(self, symbol, quantity):
                return quantity

            async def place_order(self, request):
                self.place_order_calls.append(request)
                if self._first_order_raises:
                    self._first_order_raises = False
                    raise RuntimeError("simulated one-sided IOC failure")
                self._snapshots = [(0.0, Side.SELL)]
                return OrderFill(
                    venue=self._venue,
                    symbol=request.symbol,
                    side=request.side,
                    quantity=request.quantity,
                    price=request.price or 1.0211,
                    order_id=f"{self._venue.value}-force-close",
                    client_order_id=request.client_order_id,
                    fee_quote=0.0,
                    filled_at_ms=2000,
                )

            async def fetch_position(self, symbol):
                if self._snapshots:
                    qty, side = self._snapshots.pop(0)
                else:
                    qty, side = 0.0, Side.SELL
                return PositionSnapshot(
                    venue=self._venue,
                    symbol=symbol,
                    side=side,
                    quantity=qty,
                    entry_price=1.0211 if qty else 0.0,
                    observed_at_ms=2000,
                )

        okx = ForceCloseAdapter(Venue.OKX, [(0.0, Side.BUY), (0.0, Side.BUY)])
        bybit = ForceCloseAdapter(
            Venue.BYBIT,
            [(20.0, Side.SELL), (20.0, Side.SELL), (0.0, Side.SELL)],
            first_order_raises=True,
        )
        adapters = {Venue.OKX: okx, Venue.BYBIT: bybit}
        executor = PassiveCloseExecutor(
            adapters, journal, config_overrides={"runtime_mode": "paper"},
        )
        executor.set_close_executor(CloseExecutor(adapters, journal))
        executor.set_l2_mid_resolver(lambda venue, symbol: 1.0211)

        state = EngineState()
        position = _make_position(
            position_id="entry-force-one-sided",
            symbol="BEATUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            long_quantity=20.0,
            short_quantity=20.0,
            matched_quantity=20.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=20.0,
            chunk_quantities=[20.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor._fallback_to_aggressive_close(state, pending, position)
        )

        assert result is True
        assert len(bybit.place_order_calls) == 2
        assert bybit.place_order_calls[-1].reduce_only is True
        assert bybit.place_order_calls[-1].time_in_force == TimeInForce.IOC
        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions

        records = journal.read_all()
        kinds = [record["kind"] for record in records]
        assert "exit.passive_close_live_one_sided_error" in kinds
        assert "exit.passive_close_live_one_sided_force_close_problem" in kinds
        assert "exit.compensated" in kinds
        assert "exit.passive_close_fallback_terminal_flat" in kinds
        terminals = [
            record["payload"]
            for record in records
            if record["kind"] == "runtime.position_lifecycle_terminal"
        ]
        assert terminals
        assert terminals[-1]["position_id"] == position.position_id
        assert terminals[-1]["problem"] is True
        assert terminals[-1]["terminal_reason"] == "passive_close_live_one_sided_force_close_problem"
        assert terminals[-1]["problem_reason"] == "normal_one_sided_flatten_failed_force_close"
        assert terminals[-1]["client_order_ids"]

    def test_ack_only_live_one_sided_flatten_registers_truth_gap_without_clear(self):
        """ACK-only one-sided flatten requires order/live truth before terminal clear."""
        journal = _open_journal()

        class AckOnlyAdapter(VenueAdapter):
            def __init__(self, venue, *, quantity, side):
                self._venue = venue
                self._quantity = quantity
                self._side = side
                self.place_order_calls = []

            @property
            def venue(self):
                return self._venue

            async def normalize_quantity(self, symbol, quantity):
                return quantity

            async def place_order(self, request):
                self.place_order_calls.append(request)
                error = OrderSubmitError(
                    SubmitFailureClass.UNCERTAIN,
                    "order accepted but fill not confirmed",
                )
                error.order_ack_only = True
                error.accepted_order_id = "ack-one-sided-oid"
                error.accepted_client_order_id = request.client_order_id
                error.fill_confirmation_missing_fields = ["fill", "order_state"]
                raise error

            async def fetch_position(self, symbol):
                return PositionSnapshot(
                    venue=self._venue,
                    symbol=symbol,
                    side=self._side,
                    quantity=self._quantity,
                    entry_price=1.0211 if self._quantity else 0.0,
                    observed_at_ms=2000,
                )

        class FlatAdapter(AckOnlyAdapter):
            async def place_order(self, request):
                raise AssertionError("flat leg should not be ordered")

        okx = FlatAdapter(Venue.OKX, quantity=0.0, side=Side.BUY)
        bybit = AckOnlyAdapter(Venue.BYBIT, quantity=20.0, side=Side.SELL)
        state = EngineState()
        position = _make_position(
            position_id="entry-ack-one-sided",
            symbol="BEATUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            long_quantity=20.0,
            short_quantity=20.0,
            matched_quantity=20.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=20.0,
            chunk_quantities=[20.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        executor = PassiveCloseExecutor({Venue.OKX: okx, Venue.BYBIT: bybit}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 1.0211)

        result = asyncio.run(
            executor._flatten_live_one_sided_position(
                state,
                pending,
                position,
                venue=Venue.BYBIT,
                live_snapshot=PositionSnapshot(
                    venue=Venue.BYBIT,
                    symbol="BEATUSDT",
                    side=Side.SELL,
                    quantity=20.0,
                    entry_price=1.0211,
                    observed_at_ms=2000,
                ),
                leg_label="short",
            )
        )

        assert result is False
        assert position.position_id in state.pending_passive_closes
        assert position.position_id in state.open_positions
        assert len(state.pending_close_reconciliations) == 1
        reconciliation = state.pending_close_reconciliations[0]
        assert reconciliation["kind"] == "accepted_order_truth_gap"
        assert reconciliation["order_truth_state"] == "ack_only_accepted"
        assert reconciliation["truth_required_by"] == "accepted_order_truth_gap"
        assert reconciliation["short_legs"][0]["order_id"] == "ack-one-sided-oid"
        assert reconciliation["short_legs"][0]["client_order_id"]

        records = journal.read_all()
        kinds = [record["kind"] for record in records]
        assert "exit.accepted_order_truth_gap_registered" in kinds
        assert "exit.passive_close_live_one_sided_force_close_problem" not in kinds
        assert "runtime.position_lifecycle_terminal" not in kinds

    def test_one_sided_flatten_requires_open_order_flat_proof(self):
        """Do not submit one-sided reduce-only while an exchange order may still manage it."""
        journal = _open_journal()
        state = EngineState()
        position = _make_position(
            position_id="entry-one-sided-open-order",
            symbol="BEATUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            long_quantity=20.0,
            short_quantity=20.0,
            matched_quantity=20.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=20.0,
            chunk_quantities=[20.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        bybit = _mock_adapter_with_tick(Venue.BYBIT)
        bybit.fetch_open_orders = AsyncMock(return_value=[{
            "order_id": "still-live-maker",
            "client_order_id": "maker-cid",
            "status": "open",
        }])
        bybit.place_order = AsyncMock(side_effect=AssertionError("must not flatten with open order"))
        executor = PassiveCloseExecutor({Venue.BYBIT: bybit}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 1.0211)

        result = asyncio.run(
            executor._flatten_live_one_sided_position(
                state,
                pending,
                position,
                venue=Venue.BYBIT,
                live_snapshot=PositionSnapshot(
                    venue=Venue.BYBIT,
                    symbol="BEATUSDT",
                    side=Side.SELL,
                    quantity=20.0,
                    entry_price=1.0211,
                    observed_at_ms=2000,
                ),
                leg_label="short",
            )
        )

        assert result is False
        bybit.fetch_open_orders.assert_awaited_once()
        bybit.place_order.assert_not_called()
        assert position.position_id in state.pending_passive_closes
        assert position.position_id in state.open_positions
        kinds = [record["kind"] for record in journal.read_all()]
        assert "exit.passive_close_live_one_sided_truth_gap" in kinds
        assert "exit.passive_close_live_one_sided_flatten" not in kinds

    def test_live_flat_clear_passes_real_exchange_truth_to_recovery_core(self):
        journal = _open_journal()
        state = EngineState()
        position = _make_position(
            position_id="entry-live-flat-truth",
            symbol="BEATUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            long_quantity=20.0,
            short_quantity=20.0,
            matched_quantity=20.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=20.0,
            chunk_quantities=[20.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        okx = _mock_adapter_with_tick(Venue.OKX)
        bybit = _mock_adapter_with_tick(Venue.BYBIT)
        okx.fetch_position = AsyncMock(return_value=PositionSnapshot(
            venue=Venue.OKX,
            symbol="BEATUSDT",
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=3000,
        ))
        bybit.fetch_position = AsyncMock(return_value=PositionSnapshot(
            venue=Venue.BYBIT,
            symbol="BEATUSDT",
            side=Side.SELL,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=3001,
        ))
        okx.fetch_open_orders = AsyncMock(return_value=[])
        bybit.fetch_open_orders = AsyncMock(return_value=[])

        captured_truth: list[dict] = []
        original_decide = passive_close_module.V1RecoveryDecisionCore.decide

        def capture_decide(core, snapshot):
            captured_truth.append(snapshot.exchange_truth)
            return original_decide(core, snapshot)

        executor = PassiveCloseExecutor({Venue.OKX: okx, Venue.BYBIT: bybit}, journal)
        with patch.object(
            passive_close_module.V1RecoveryDecisionCore,
            "decide",
            capture_decide,
        ):
            result = asyncio.run(
                executor._clear_if_live_flat(
                    state,
                    pending,
                    position,
                    source="test_live_flat_truth",
                )
            )

        assert result is True
        assert captured_truth
        truth = captured_truth[-1]
        assert truth["truth_available"] is True
        assert truth["positions"]
        assert {item["venue"] for item in truth["positions"]} == {"okx", "bybit"}
        assert all(item["quantity"] == 0.0 for item in truth["positions"])
        assert truth["open_orders"] == []
        assert truth["open_order_truth"]
        assert all(item["open_orders_empty"] is True for item in truth["open_order_truth"])

    def test_live_flat_force_close_problem_does_not_enqueue_reconciliation_in_paper(self):
        """V1: pending-close reconciliation work is a live-runtime concern."""
        journal = _open_journal()
        executor = PassiveCloseExecutor(
            {},
            journal,
            config_overrides={"runtime_mode": "paper"},
        )

        state = EngineState()
        position = _make_position(
            position_id="entry-force-reconcile",
            symbol="BEATUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            long_quantity=20.0,
            short_quantity=20.0,
            matched_quantity=20.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=20.0,
            chunk_quantities=[20.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        executor._clear_live_flat_state(
            state,
            pending,
            position,
            source="passive_close_live_one_sided_force_close_problem",
            actual_long_size=0.0,
            actual_short_size=0.0,
            extra={
                "flattened_venue": Venue.BYBIT.value,
                "flattened_quantity": 20.0,
                "problem": True,
                "force_close_client_order_ids": ["bybit-force-cid"],
                "force_close_order_ids": ["bybit-force-order"],
            },
        )

        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions
        assert state.pending_close_reconciliations == []
        assert "exit.pending_close_reconciliation_registered" not in [
            record["kind"] for record in journal.read_all()
        ]

    def test_live_flat_cleanup_resolves_passive_close_terminal(self):
        """V1 parity: live-flat passive cleanup is still a close resolution."""
        journal = _open_journal()
        executor = PassiveCloseExecutor(
            {},
            journal,
            config_overrides={"runtime_mode": "live"},
        )

        state = EngineState()
        position = _make_position(
            position_id="entry-live-flat-resolved",
            symbol="SAHARAUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            long_quantity=1330.0,
            short_quantity=1330.0,
            matched_quantity=1330.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=1330.0,
            chunk_quantities=[1330.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        executor._clear_live_flat_state(
            state,
            pending,
            position,
            source="passive_close_live_one_sided_flattened",
            actual_long_size=0.0,
            actual_short_size=0.0,
            extra={
                "flattened_venue": Venue.BYBIT.value,
                "flattened_quantity": 1330.0,
                "single_leg_fast_flatten": True,
            },
            exchange_truth={
                "truth_available": True,
                "positions_flat": True,
                "open_orders_flat": True,
            },
        )

        records = journal.read_all()
        resolved = [
            record["payload"]
            for record in records
            if record["kind"] == "exit.passive_close_resolved"
        ]
        assert resolved
        assert resolved[-1]["position_id"] == position.position_id
        assert resolved[-1]["resolution_source"] == "passive_close_live_one_sided_flattened"
        assert resolved[-1]["problem"] is False
        assert resolved[-1]["single_leg_fast_flatten"] is True
        assert resolved[-1]["exchange_truth"]["positions_flat"] is True
        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions

    def test_live_final_chunk_flattens_trusted_single_leg_exchange_truth(self):
        """Trusted one-sided final truth is actionable close work, not a wait loop."""
        journal = _open_journal()

        class FinalTruthAdapter(VenueAdapter):
            def __init__(self, venue: Venue, snapshots: list[tuple[float, Side]]):
                self._venue = venue
                self._snapshots = list(snapshots)
                self.place_order_calls: list[OrderRequest] = []

            @property
            def venue(self):
                return self._venue

            def price_tick_size(self, symbol=None):
                return 0.0001

            async def normalize_quantity(self, symbol, quantity):
                return quantity

            async def fetch_open_orders(self, symbol):
                return []

            async def fetch_position(self, symbol):
                if self._snapshots:
                    quantity, side = self._snapshots.pop(0)
                else:
                    quantity, side = 0.0, Side.SELL
                return PositionSnapshot(
                    venue=self._venue,
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    entry_price=0.03731 if quantity else 0.0,
                    observed_at_ms=4_000,
                )

            async def place_order(self, request):
                self.place_order_calls.append(request)
                self._snapshots = [(0.0, Side.SELL), (0.0, Side.SELL)]
                return OrderFill(
                    venue=self._venue,
                    symbol=request.symbol,
                    side=request.side,
                    quantity=request.quantity,
                    price=request.price or 0.03731,
                    order_id=f"{self._venue.value}-flatten",
                    client_order_id=request.client_order_id,
                    fee_quote=0.0,
                    filled_at_ms=4_050,
                )

        okx = FinalTruthAdapter(
            Venue.OKX,
            [(0.0, Side.BUY), (0.0, Side.BUY), (0.0, Side.BUY)],
        )
        bybit = FinalTruthAdapter(
            Venue.BYBIT,
            [(425.0, Side.SELL), (425.0, Side.SELL), (0.0, Side.SELL)],
        )

        executor = PassiveCloseExecutor(
            {Venue.OKX: okx, Venue.BYBIT: bybit},
            journal,
            config_overrides={"runtime_mode": "live"},
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 0.03731)
        state = EngineState()
        position = _make_position(
            position_id="entry-1781859127568-ESPORTSUSDT",
            symbol="ESPORTSUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            long_quantity=425.0,
            short_quantity=425.0,
            matched_quantity=425.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=425.0,
            chunk_quantities=[425.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="maker-oid",
                maker_client_order_id="maker-cid",
            ),
            maker_fill=PendingPassiveLegFill(
                quantity=425.0,
                average_price=0.03733,
                order_id="maker-oid",
                client_order_id="maker-cid",
                last_fill_time_ms=3_000,
            ),
            hedge_fill=PendingPassiveLegFill(
                quantity=425.0,
                average_price=0.03731,
                order_id="hedge-oid",
                client_order_id="hedge-cid",
                last_fill_time_ms=3_010,
            ),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        first_result = asyncio.run(executor._advance_chunk(state, pending))

        assert first_result is True
        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions
        assert okx.place_order_calls == []
        assert len(bybit.place_order_calls) == 1
        request = bybit.place_order_calls[0]
        assert request.side is Side.BUY
        assert request.quantity == pytest.approx(425.0)
        assert request.reduce_only is True
        assert request.time_in_force is TimeInForce.IOC
        records = journal.read_all()
        kinds = [record["kind"] for record in records]
        assert "exit.passive_close_live_one_sided_flatten" in kinds
        assert "exit.passive_close_waiting_exchange_flat_truth" not in kinds
        resolved = [
            record["payload"]
            for record in records
            if record["kind"] == "exit.passive_close_resolved"
        ]
        assert resolved
        assert resolved[-1]["resolution_source"] == "passive_close_live_one_sided_flattened"
        assert resolved[-1]["exchange_truth"]["truth_available"] is True
        assert all(
            item["open_orders_empty"] is True
            for item in resolved[-1]["exchange_truth"]["open_order_truth"]
        )

    def test_live_recovered_final_chunk_without_snapshot_clears_on_trusted_flat_truth(self):
        """Recovered pending close records clear only after live flat truth proof."""
        journal = _open_journal()

        okx = _mock_adapter_with_tick(Venue.OKX)
        bybit = _mock_adapter_with_tick(Venue.BYBIT)
        okx.fetch_position = AsyncMock(side_effect=[
            PositionSnapshot(
                venue=Venue.OKX,
                symbol="ESPORTSUSDT",
                side=Side.BUY,
                quantity=0.0,
                entry_price=0.0,
                observed_at_ms=3_000,
            ),
            PositionSnapshot(
                venue=Venue.OKX,
                symbol="ESPORTSUSDT",
                side=Side.BUY,
                quantity=0.0,
                entry_price=0.0,
                observed_at_ms=4_000,
            ),
        ])
        bybit.fetch_position = AsyncMock(side_effect=[
            PositionSnapshot(
                venue=Venue.BYBIT,
                symbol="ESPORTSUSDT",
                side=Side.SELL,
                quantity=425.0,
                entry_price=0.03731,
                observed_at_ms=3_001,
            ),
            PositionSnapshot(
                venue=Venue.BYBIT,
                symbol="ESPORTSUSDT",
                side=Side.SELL,
                quantity=0.0,
                entry_price=0.0,
                observed_at_ms=4_001,
            ),
            PositionSnapshot(
                venue=Venue.BYBIT,
                symbol="ESPORTSUSDT",
                side=Side.SELL,
                quantity=0.0,
                entry_price=0.0,
                observed_at_ms=4_101,
            ),
        ])
        okx.fetch_open_orders = AsyncMock(return_value=[])
        bybit.fetch_open_orders = AsyncMock(return_value=[])

        executor = PassiveCloseExecutor(
            {Venue.OKX: okx, Venue.BYBIT: bybit},
            journal,
            config_overrides={"runtime_mode": "live"},
        )
        state = EngineState()
        position = _make_position(
            position_id="entry-recovered-nosnapshot-ESPORTSUSDT",
            symbol="ESPORTSUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            long_quantity=425.0,
            short_quantity=425.0,
            matched_quantity=425.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=None,
            target_quantity=425.0,
            chunk_quantities=[425.0],
            active_chunk_index=1,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="maker-oid",
                maker_client_order_id="maker-cid",
            ),
            maker_fill=PendingPassiveLegFill(
                quantity=425.0,
                average_price=0.03733,
                order_id="maker-oid",
                client_order_id="maker-cid",
                last_fill_time_ms=3_000,
            ),
            hedge_fill=PendingPassiveLegFill(
                quantity=425.0,
                average_price=0.03731,
                order_id="hedge-oid",
                client_order_id="hedge-cid",
                last_fill_time_ms=3_010,
            ),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        first_result = asyncio.run(executor._finalize_passive_close(state, pending))

        assert first_result is True
        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions
        assert pending.position_snapshot is position
        records = journal.read_all()
        kinds = [record["kind"] for record in records]
        assert "exit.passive_close_waiting_exchange_flat_truth" not in kinds
        resolved = [
            record["payload"]
            for record in records
            if record["kind"] == "exit.passive_close_resolved"
        ]
        assert resolved
        assert all(
            item["quantity"] == 0.0
            for item in resolved[-1]["exchange_truth"]["positions"]
        )
        assert all(
            item["open_orders_empty"] is True
            for item in resolved[-1]["exchange_truth"]["open_order_truth"]
        )
        okx.submit_passive_order.assert_not_called()
        bybit.submit_passive_order.assert_not_called()

    def test_live_missing_position_snapshot_waiting_event_carries_truth_attempt(self):
        """Every waiting-exchange-truth event should explain missing proof locally."""
        journal = _open_journal()
        state = EngineState()
        pending = PendingPassiveClose(
            position_id="entry-missing-position-snapshot",
            reason="funding_capture",
            position_snapshot=None,
            target_quantity=10.0,
            chunk_quantities=[10.0],
            active_chunk_index=1,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="maker-oid",
                maker_client_order_id="maker-cid",
            ),
            maker_fill=PendingPassiveLegFill(quantity=10.0),
            hedge_fill=PendingPassiveLegFill(quantity=10.0),
        )
        state.pending_passive_closes[pending.position_id] = pending
        executor = PassiveCloseExecutor(
            {},
            journal,
            config_overrides={"runtime_mode": "live"},
        )

        result = asyncio.run(executor._finalize_passive_close(state, pending))

        assert result is False
        assert pending.position_id in state.pending_passive_closes
        waiting_payload = next(
            record["payload"]
            for record in journal.read_all()
            if record["kind"] == "exit.passive_close_waiting_exchange_flat_truth"
        )
        truth_attempt = waiting_payload["exchange_truth_attempt"]
        assert truth_attempt["truth_available"] is False
        assert truth_attempt["positions_flat"] is None
        assert truth_attempt["open_orders_flat"] is None
        assert truth_attempt["positions"] == []
        assert truth_attempt["open_order_truth"] == []
        assert truth_attempt["missing_evidence"] == ["position_snapshot"]
        assert truth_attempt["source"] == "passive_close_final_missing_position_snapshot"

    def test_live_flat_force_close_problem_keeps_close_reconciliation_work(self):
        """V1: lifecycle can clear flat while fill/PnL reconciliation continues."""
        journal = _open_journal()
        executor = PassiveCloseExecutor(
            {},
            journal,
            config_overrides={"runtime_mode": "live"},
        )

        state = EngineState()
        position = _make_position(
            position_id="entry-force-reconcile",
            symbol="BEATUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            long_quantity=20.0,
            short_quantity=20.0,
            matched_quantity=20.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=20.0,
            chunk_quantities=[20.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        executor._clear_live_flat_state(
            state,
            pending,
            position,
            source="passive_close_live_one_sided_force_close_problem",
            actual_long_size=0.0,
            actual_short_size=0.0,
            extra={
                "flattened_venue": Venue.BYBIT.value,
                "flattened_quantity": 20.0,
                "problem": True,
                "force_close_client_order_ids": ["bybit-force-cid"],
                "force_close_order_ids": ["bybit-force-order"],
            },
        )

        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions
        assert len(state.pending_close_reconciliations) == 1
        reconciliation = state.pending_close_reconciliations[0]
        assert reconciliation["position_id"] == position.position_id
        assert reconciliation["symbol"] == "BEATUSDT"
        assert reconciliation["reason"] == "funding_capture"
        assert reconciliation["created_cycle"] == 0
        assert reconciliation["next_attempt_ms"] == reconciliation["closed_at_ms"]
        assert reconciliation["position_snapshot"]["short_venue"] == Venue.BYBIT.value
        assert reconciliation["short_legs"] == [{
            "venue": Venue.BYBIT.value,
            "order_id": "bybit-force-order",
            "client_order_id": "bybit-force-cid",
            "quantity": 20.0,
            "average_price": 0.0,
            "fee_quote": 0.0,
        }]

    def test_beatusdt_live_imbalanced_under_min_excess_compensates_flat(self):
        """Both live legs nonzero but imbalanced must not retry stale local dust."""
        journal = _open_journal()

        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.engine.recovery import build_persistent_state_view

        class LiveAdapter(VenueAdapter):
            def __init__(self, venue, snapshots):
                self._venue = venue
                self._snapshots = list(snapshots)
                self.place_order_calls = []

            @property
            def venue(self):
                return self._venue

            async def normalize_quantity(self, symbol, quantity):
                return quantity

            async def place_order(self, request):
                self.place_order_calls.append(request)
                return OrderFill(
                    venue=self._venue,
                    symbol=request.symbol,
                    side=request.side,
                    quantity=request.quantity,
                    price=request.price or 1.0211,
                    order_id=f"{self._venue.value}-flatten",
                    client_order_id=request.client_order_id,
                    fee_quote=0.0,
                    filled_at_ms=2000,
                )

            async def fetch_position(self, symbol):
                if self._snapshots:
                    qty, side = self._snapshots.pop(0)
                else:
                    qty, side = 0.0, Side.SELL
                return PositionSnapshot(
                    venue=self._venue,
                    symbol=symbol,
                    side=side,
                    quantity=qty,
                    entry_price=1.0211 if qty else 0.0,
                    observed_at_ms=2000,
                )

        okx = LiveAdapter(
            Venue.OKX,
            [
                (18.0, Side.BUY),
                (18.0, Side.BUY),
                (0.0, Side.BUY),
            ],
        )
        bybit = LiveAdapter(
            Venue.BYBIT,
            [
                (20.0, Side.SELL),
                (20.0, Side.SELL),
                (0.0, Side.SELL),
            ],
        )
        _attach_bybit_min_notional_transport(bybit)
        adapters = {Venue.OKX: okx, Venue.BYBIT: bybit}
        executor = PassiveCloseExecutor(adapters, journal)
        executor.set_close_executor(CloseExecutor(adapters, journal))
        executor.set_l2_mid_resolver(lambda venue, symbol: 1.0211)

        state = EngineState()
        position = _make_position(
            position_id="entry-beatusdt-imbalanced",
            symbol="BEATUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            long_quantity=20.0,
            short_quantity=20.0,
            matched_quantity=20.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=20.0,
            chunk_quantities=[20.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=2.0, average_price=1.0211),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor._fallback_to_aggressive_close(state, pending, position)
        )

        assert result is True
        assert [request.quantity for request in bybit.place_order_calls] == [20.0]
        assert [request.quantity for request in okx.place_order_calls] == [18.0]
        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions
        assert pending.next_retry_at_ms == 0

        kinds = [record["kind"] for record in journal.read_all()]
        assert "exit.passive_close_live_imbalanced" in kinds
        assert "execution.min_notional_abort_and_flatten" in kinds
        assert "exit.compensated" in kinds
        assert "exit.passive_close_fallback_unhedged_failed" not in kinds

        persisted = build_persistent_state_view(state)
        assert position.position_id not in persisted["open_positions"]
        assert position.position_id not in persisted["pending_passive_closes"]

    def test_live_flat_fallback_clears_pending_open_and_last_error(self):
        """Fallback starts from live truth; already-flat venues remove all local state."""
        journal = _open_journal()

        okx = _mock_adapter_with_tick(Venue.OKX)
        bybit = _mock_adapter_with_tick(Venue.BYBIT)
        okx.fetch_position = AsyncMock(return_value=PositionSnapshot(
            venue=Venue.OKX, symbol="BEATUSDT", side=Side.BUY,
            quantity=0.0, entry_price=0.0, observed_at_ms=2000,
        ))
        bybit.fetch_position = AsyncMock(return_value=PositionSnapshot(
            venue=Venue.BYBIT, symbol="BEATUSDT", side=Side.SELL,
            quantity=0.0, entry_price=0.0, observed_at_ms=2000,
        ))
        okx.place_order = AsyncMock()
        bybit.place_order = AsyncMock()
        executor = PassiveCloseExecutor({Venue.OKX: okx, Venue.BYBIT: bybit}, journal)

        state = EngineState()
        state.last_error = "pending passive close failed for entry-beatusdt-flat"
        position = _make_position(
            position_id="entry-beatusdt-flat",
            symbol="BEATUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            matched_quantity=20.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=20.0,
            chunk_quantities=[20.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=2.0, average_price=1.0211),
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
        assert state.last_error is None
        okx.place_order.assert_not_called()
        bybit.place_order.assert_not_called()

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

    def test_fallback_unhedged_bybit_duplicate_reconciles_fill_and_clears(self):
        """V1 parity: duplicate orderLinkId on Bybit hedge is recovered via client id lookup."""
        journal = _open_journal()

        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.venues.cid import compact_client_order_id

        class FlatAdapter(VenueAdapter):
            def __init__(self, venue):
                self._venue = venue

            @property
            def venue(self):
                return self._venue

            async def place_order(self, request):
                return _make_order_fill(
                    venue=self._venue,
                    symbol=request.symbol,
                    side=request.side,
                    quantity=request.quantity,
                    price=request.price or 1.0,
                )

            async def fetch_position(self, symbol):
                return PositionSnapshot(
                    venue=self._venue,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=0.0,
                    entry_price=0.0,
                    observed_at_ms=1000,
                )

            async def query_passive_order_progress(self, symbol, order_id, client_order_id, side):
                return None

        class FilledMakerAdapter(FlatAdapter):
            async def query_passive_order_progress(self, symbol, order_id, client_order_id, side):
                return PassiveOrderProgress(
                    venue=self._venue,
                    symbol=symbol,
                    side=side,
                    order_id=order_id,
                    client_order_id=client_order_id,
                    cumulative_quantity=534.0,
                    average_price=0.04496,
                    fee_quote=0.0,
                    last_fill_time_ms=1500,
                    state=PassiveOrderState.FILLED,
                    observed_at_ms=1500,
                )

        class DuplicateBybitAdapter(FlatAdapter):
            def __init__(self):
                super().__init__(Venue.BYBIT)
                self.place_order_calls = 0
                self.reconciliation_lookups = []

            async def place_order(self, request):
                self.place_order_calls += 1
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    "bybit order failed: bybit retCode=110072 retMsg=OrderLinkedID is duplicate",
                )

            async def fetch_order_fill_reconciliation(self, symbol, order_id, client_order_id=None):
                self.reconciliation_lookups.append((symbol, order_id, client_order_id))
                return OrderFillReconciliation(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=534.0,
                    average_price=0.04508,
                    order_id="95dbe960-6b01-4259-958b-02ef11bb6dbc",
                    client_order_id=client_order_id,
                    fee_quote=0.0123,
                    filled_at_ms=2000,
                    metadata={
                        "evidence_source": "bybit_execution_list",
                        "queried_endpoints": ["/v5/execution/list"],
                        "response_classification": "filled",
                    },
                )

        binance = FilledMakerAdapter(Venue.BINANCE)
        bybit = DuplicateBybitAdapter()
        adapters = {Venue.BINANCE: binance, Venue.BYBIT: bybit}
        executor = PassiveCloseExecutor(adapters, journal)
        executor.set_close_executor(CloseExecutor(adapters, journal))
        executor.set_l2_mid_resolver(lambda venue, symbol: 0.04508)

        state = EngineState()
        position = _make_position(
            position_id="entry-1779551578130-LYNUSDT",
            symbol="LYNUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=534.0,
            short_quantity=534.0,
            matched_quantity=534.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=534.0,
            chunk_quantities=[534.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="1513843783",
                maker_client_order_id="lfex99b5bef67012c096",
            ),
            maker_fill=PendingPassiveLegFill(
                quantity=534.0,
                average_price=0.04496,
                order_id="1513843783",
                client_order_id="lfex99b5bef67012c096",
            ),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False)
        )

        expected_cid = compact_client_order_id(position.position_id, "exit_short_hedge")
        assert result is True
        assert bybit.place_order_calls == 1
        assert bybit.reconciliation_lookups == [("LYNUSDT", "", expected_cid)]
        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions
        assert pending.hedge_fill.quantity == 534.0
        assert pending.hedge_fill.order_id == "95dbe960-6b01-4259-958b-02ef11bb6dbc"

        kinds = [record["kind"] for record in journal.read_all()]
        assert "exit.passive_close_hedge_duplicate_client_order_reconciled" in kinds
        assert "exit.passive_close_resolved" in kinds
        assert "exit.passive_close_hedge_error" not in kinds

    def test_ack_only_delta_hedge_error_preserves_order_truth_gap_evidence(self):
        """Accepted taker hedge ACK without fill is a pending truth gap, not a plain failure."""
        journal = _open_journal()

        ack_error = OrderSubmitError(
            SubmitFailureClass.UNCERTAIN,
            "order accepted (id=ack-oid) but fill not confirmed",
        )
        ack_error.order_ack_only = True
        ack_error.accepted_order_id = "ack-oid"
        ack_error.accepted_client_order_id = "ack-cid"
        ack_error.fill_confirmation_missing_fields = ["executedQty", "cumQty"]
        ack_error.exchange_response_body = (
            '{"retCode":0,"result":{"orderId":"ack-oid","orderLinkId":"ack-cid"}}'
        )

        adapter = _mock_adapter_with_tick(Venue.BYBIT)
        adapter.place_order = AsyncMock(side_effect=ack_error)
        adapter.fetch_order_fill_reconciliation = AsyncMock(return_value=None)
        executor = PassiveCloseExecutor({Venue.BYBIT: adapter}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 1.0)
        executor.set_l2_quote_resolver(lambda venue, symbol: (0.99, 1.01))

        state = EngineState()
        position = _make_position(
            position_id="entry-passive-ack-only",
            symbol="EDENUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=10.0,
            short_quantity=10.0,
            matched_quantity=10.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=10.0,
            chunk_quantities=[10.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=10.0, average_price=1.0),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
        )

        result = asyncio.run(
            executor._submit_hedge_for_delta(
                state,
                pending,
                position,
                10.0,
                maker_terminal=True,
            )
        )

        assert result.success is False
        assert result.truth_gap is True
        assert result.residual == pytest.approx(10.0)
        assert result.accepted_order_id == "ack-oid"
        assert result.accepted_client_order_id == "ack-cid"
        records = journal.read_all()
        payload = [
            record["payload"] for record in records
            if record["kind"] == "exit.passive_close_hedge_ack_pending_reconcile"
        ][-1]
        assert payload["order_ack_only"] is True
        assert payload["accepted_order_id"] == "ack-oid"
        assert payload["accepted_client_order_id"] == "ack-cid"
        assert payload["fill_confirmation_missing_fields"] == ["executedQty", "cumQty"]
        assert "fill_confirmation" in payload["missing_evidence"]
        assert payload["order_truth_probe_paths"]["rest_order_status"] == "GET /v5/order/realtime"
        assert payload["next_action"] == "reconcile_accepted_order_or_probe_live_position"
        assert payload["fill_reconciliation_attempted"] is True
        assert payload["fill_reconciliation_result"] == "missing_or_zero_fill"
        kinds = [record["kind"] for record in records]
        assert "exit.accepted_order_truth_gap_registered" in kinds
        assert "exit.passive_close_hedge_error" not in kinds
        assert len(state.pending_close_reconciliations) == 1
        assert state.pending_close_reconciliations[0]["kind"] == "accepted_order_truth_gap"

    def test_ack_only_delta_hedge_reconciled_after_uncertain_submit_is_classified_info(self):
        """Accepted taker hedge ACK reconciled by client id is contained evidence, not abnormal close."""
        journal = _open_journal()

        ack_error = OrderSubmitError(
            SubmitFailureClass.UNCERTAIN,
            "order accepted (id=ack-oid) but fill not confirmed",
        )
        ack_error.order_ack_only = True
        ack_error.accepted_order_id = "ack-oid"
        ack_error.accepted_client_order_id = "ack-cid"
        ack_error.fill_confirmation_missing_fields = ["executedQty", "cumQty"]

        adapter = _mock_adapter_with_tick(Venue.BYBIT)
        adapter.place_order = AsyncMock(side_effect=ack_error)
        adapter.fetch_order_fill_reconciliation = AsyncMock(
            return_value=OrderFillReconciliation(
                venue=Venue.BYBIT,
                symbol="EDENUSDT",
                side=Side.BUY,
                quantity=10.0,
                average_price=1.01,
                order_id="ack-oid",
                client_order_id="ack-cid",
                filled_at_ms=1781416809425,
                metadata={
                    "evidence_source": "bybit_execution_list",
                    "queried_endpoints": ["/v5/execution/list"],
                    "response_classification": "filled",
                },
            )
        )
        executor = PassiveCloseExecutor({Venue.BYBIT: adapter}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 1.0)
        executor.set_l2_quote_resolver(lambda venue, symbol: (0.99, 1.01))

        state = EngineState()
        position = _make_position(
            position_id="entry-passive-ack-reconciled",
            symbol="EDENUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=10.0,
            short_quantity=10.0,
            matched_quantity=10.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=10.0,
            chunk_quantities=[10.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=10.0, average_price=1.0),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
        )

        result = asyncio.run(
            executor._submit_hedge_for_delta(
                state,
                pending,
                position,
                10.0,
                maker_terminal=True,
            )
        )

        assert result.success is True
        records = journal.read_all()
        payload = [
            record["payload"] for record in records
            if record["kind"] == "exit.passive_close_hedge_confirmed_after_ack"
        ][-1]
        assert payload["classification"] == "accepted_ack_confirmed"
        assert payload["severity"] == "info"
        assert payload["order_submit_uncertain"] is True
        assert payload["decision"] == "accepted_order_reconciled_by_client_id"
        assert payload["residual"] == pytest.approx(0.0)
        assert "exit.passive_close_hedge_reconciled_after_error" not in [
            record["kind"] for record in records
        ]

    def test_active_truth_gap_reconciliation_weak_positive_retain_pending(self):
        """Active accepted-order truth gaps require resolver-confirmed fill truth."""
        journal = _open_journal()

        adapter = _mock_adapter_with_tick(Venue.BYBIT)
        adapter.place_order = AsyncMock()
        adapter.fetch_order_fill_reconciliation = AsyncMock(
            return_value=OrderFillReconciliation(
                venue=Venue.BYBIT,
                symbol="EDENUSDT",
                side=Side.BUY,
                quantity=10.0,
                average_price=1.01,
                order_id="ack-oid",
                client_order_id="ack-cid",
                metadata={
                    "evidence_source": "bybit_order_realtime",
                    "response_classification": "accepted_ack_without_execution",
                    "queried_endpoints": ["/v5/order/realtime"],
                },
            )
        )
        executor = PassiveCloseExecutor({Venue.BYBIT: adapter}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 1.0)
        executor.set_l2_quote_resolver(lambda venue, symbol: (0.99, 1.01))

        state = EngineState()
        position = _make_position(
            position_id="entry-passive-active-gap",
            symbol="EDENUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=10.0,
            short_quantity=10.0,
            matched_quantity=10.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=10.0,
            chunk_quantities=[10.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=10.0, average_price=1.0),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
        )
        state.pending_close_reconciliations.append(
            {
                "position_id": position.position_id,
                "symbol": position.symbol,
                "kind": "accepted_order_truth_gap",
                "venue": Venue.BYBIT.value,
                "leg": "short",
                "original_payload": {
                    "accepted_order_id": "ack-oid",
                    "accepted_client_order_id": "ack-cid",
                },
                "short_legs": [
                    {
                        "venue": Venue.BYBIT.value,
                        "order_id": "ack-oid",
                        "client_order_id": "ack-cid",
                    }
                ],
            }
        )

        result = asyncio.run(
            executor._submit_hedge_for_delta(
                state,
                pending,
                position,
                10.0,
                maker_terminal=True,
            )
        )

        assert result.success is False
        assert result.truth_gap is True
        assert pending.hedge_fill.quantity == 0.0
        assert len(state.pending_close_reconciliations) == 1
        records = journal.read_all()
        payload = [
            record["payload"] for record in records
            if record["kind"] == "exit.passive_close_hedge_ack_reconcile_in_progress"
        ][-1]
        assert payload["fill_reconciliation_result"] == "truth_gap"
        assert payload["order_truth_fill_status"] == "truth_gap"
        assert payload["order_truth_evidence_status"] == "unavailable"
        assert payload["terminal_without_truth"] is False

    def test_bybit_delta_hedge_ack_without_qty_confirms_via_reconciliation(self):
        """Bybit create-order ACK may be async and carry no fill quantity."""
        journal = _open_journal()

        adapter = _mock_adapter_with_tick(Venue.BYBIT)
        adapter.place_order = AsyncMock(
            return_value=OrderFill(
                venue=Venue.BYBIT,
                symbol="EDENUSDT",
                side=Side.BUY,
                quantity=0.0,
                price=0.0,
                order_id="ack-oid",
                client_order_id="ack-cid",
            )
        )
        adapter.fetch_order_fill_reconciliation = AsyncMock(
            return_value=OrderFillReconciliation(
                venue=Venue.BYBIT,
                symbol="EDENUSDT",
                side=Side.BUY,
                quantity=10.0,
                average_price=1.01,
                order_id="ack-oid",
                client_order_id="ack-cid",
                filled_at_ms=1781416809425,
                metadata={
                    "evidence_source": "bybit_execution_list",
                    "queried_endpoints": ["/v5/execution/list"],
                    "response_classification": "filled",
                },
            )
        )
        executor = PassiveCloseExecutor({Venue.BYBIT: adapter}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 1.0)
        executor.set_l2_quote_resolver(lambda venue, symbol: (0.99, 1.01))

        state = EngineState()
        position = _make_position(
            position_id="entry-passive-bybit-ack",
            symbol="EDENUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=10.0,
            short_quantity=10.0,
            matched_quantity=10.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=10.0,
            chunk_quantities=[10.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=10.0, average_price=1.0),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
        )

        result = asyncio.run(
            executor._submit_hedge_for_delta(
                state,
                pending,
                position,
                10.0,
                maker_terminal=True,
            )
        )

        assert result.success is True
        adapter.fetch_order_fill_reconciliation.assert_awaited_once_with(
            "EDENUSDT",
            "ack-oid",
            "ack-cid",
        )
        records = journal.read_all()
        payload = [
            record["payload"] for record in records
            if record["kind"] == "exit.passive_close_hedge_confirmed_after_ack"
        ][-1]
        assert payload["classification"] == "accepted_ack_confirmed"
        assert payload["order_submit_uncertain"] is False
        assert payload["residual"] == pytest.approx(0.0)
        assert "exit.passive_close_hedge_reconciled_after_error" not in [
            record["kind"] for record in records
        ]

    def test_ack_only_terminal_hedge_live_flat_resolves_without_deadline_fail_closed(self):
        """Bybit ACK-only close is resolved by live-flat truth, not by fail-closed compensation."""
        journal = _open_journal()

        ack_error = OrderSubmitError(
            SubmitFailureClass.UNCERTAIN,
            "order accepted (id=ack-oid) but fill not confirmed",
        )
        ack_error.order_ack_only = True
        ack_error.accepted_order_id = "ack-oid"
        ack_error.accepted_client_order_id = "ack-cid"
        ack_error.fill_confirmation_missing_fields = ["executedQty", "cumQty"]
        ack_error.exchange_response_body = (
            '{"retCode":0,"result":{"orderId":"ack-oid","orderLinkId":"ack-cid"}}'
        )

        maker = _mock_adapter_passive_ok(Venue.OKX)
        maker.query_passive_order_progress = AsyncMock(return_value=_make_passive_progress(
            venue=Venue.OKX,
            symbol="KATUSDT",
            side=Side.SELL,
            order_id="maker-oid",
            client_order_id="maker-cid",
            cumulative_quantity=7000.0,
            average_price=0.00679,
            state=PassiveOrderState.FILLED,
        ))
        maker.fetch_position = AsyncMock(return_value=PositionSnapshot(
            venue=Venue.OKX,
            symbol="KATUSDT",
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=3000,
        ))
        maker.fetch_open_orders = AsyncMock(return_value=[])

        hedge = _mock_adapter_with_tick(Venue.BYBIT)
        hedge.place_order = AsyncMock(side_effect=ack_error)
        hedge.fetch_order_fill_reconciliation = AsyncMock(return_value=None)
        hedge.fetch_position = AsyncMock(return_value=PositionSnapshot(
            venue=Venue.BYBIT,
            symbol="KATUSDT",
            side=Side.SELL,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=3001,
        ))
        hedge.fetch_open_orders = AsyncMock(return_value=[])

        executor = PassiveCloseExecutor({Venue.OKX: maker, Venue.BYBIT: hedge}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 0.00679)
        executor.set_l2_quote_resolver(lambda venue, symbol: (0.00678, 0.0068))

        state = EngineState()
        position = _make_position(
            position_id="entry-katusdt-ack-flat",
            symbol="KATUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            long_quantity=7000.0,
            short_quantity=7000.0,
            matched_quantity=7000.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=7000.0,
            chunk_quantities=[7000.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="maker-oid",
                maker_client_order_id="maker-cid",
            ),
            maker_fill=PendingPassiveLegFill(quantity=0.0),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor.drive_pending_passive_close(
                state, position.position_id, wait_until_terminal=False,
            )
        )

        assert result is True
        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions
        kinds = [record["kind"] for record in journal.read_all()]
        assert "exit.passive_close_hedge_ack_pending_reconcile" in kinds
        assert "exit.accepted_order_truth_gap_registered" in kinds
        assert "exit.passive_close_resolved" in kinds
        assert "exit.passive_close_hedge_error" not in kinds
        assert "exit.passive_close_hedge_deadline_fail_closed" not in kinds
        assert "exit.compensated" not in kinds

    def test_ack_only_terminal_hedge_not_flat_retains_pending_without_fake_green(self):
        """ACK-only truth gap remains pending when live position truth is not flat."""
        journal = _open_journal()

        ack_error = OrderSubmitError(
            SubmitFailureClass.UNCERTAIN,
            "order accepted (id=ack-oid) but fill not confirmed",
        )
        ack_error.order_ack_only = True
        ack_error.accepted_order_id = "ack-oid"
        ack_error.accepted_client_order_id = "ack-cid"

        maker = _mock_adapter_passive_ok(Venue.OKX)
        maker.query_passive_order_progress = AsyncMock(return_value=_make_passive_progress(
            venue=Venue.OKX,
            symbol="KATUSDT",
            side=Side.SELL,
            order_id="maker-oid",
            client_order_id="maker-cid",
            cumulative_quantity=7000.0,
            average_price=0.00679,
            state=PassiveOrderState.FILLED,
        ))
        maker.fetch_position = AsyncMock(return_value=PositionSnapshot(
            venue=Venue.OKX,
            symbol="KATUSDT",
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=3000,
        ))
        maker.fetch_open_orders = AsyncMock(return_value=[])

        hedge = _mock_adapter_with_tick(Venue.BYBIT)
        hedge.place_order = AsyncMock(side_effect=ack_error)
        hedge.fetch_order_fill_reconciliation = AsyncMock(return_value=None)
        hedge.fetch_position = AsyncMock(return_value=PositionSnapshot(
            venue=Venue.BYBIT,
            symbol="KATUSDT",
            side=Side.SELL,
            quantity=7000.0,
            entry_price=0.00679,
            observed_at_ms=3001,
        ))
        hedge.fetch_open_orders = AsyncMock(return_value=[])

        executor = PassiveCloseExecutor({Venue.OKX: maker, Venue.BYBIT: hedge}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 0.00679)
        executor.set_l2_quote_resolver(lambda venue, symbol: (0.00678, 0.0068))

        state = EngineState()
        position = _make_position(
            position_id="entry-katusdt-ack-not-flat",
            symbol="KATUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            long_quantity=7000.0,
            short_quantity=7000.0,
            matched_quantity=7000.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=7000.0,
            chunk_quantities=[7000.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="maker-oid",
                maker_client_order_id="maker-cid",
            ),
            maker_fill=PendingPassiveLegFill(quantity=0.0),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor.drive_pending_passive_close(
                state, position.position_id, wait_until_terminal=False,
            )
        )

        assert result is False
        assert position.position_id in state.pending_passive_closes
        assert position.position_id in state.open_positions
        assert pending.next_retry_at_ms > 0
        assert pending.hedge_fill.quantity == 0.0
        kinds = [record["kind"] for record in journal.read_all()]
        assert "exit.passive_close_hedge_ack_pending_reconcile" in kinds
        assert "exit.passive_close_hedge_ack_live_truth_pending" in kinds
        assert "exit.passive_close_resolved" not in kinds
        assert "exit.passive_close_hedge_error" not in kinds
        assert "exit.passive_close_hedge_deadline_fail_closed" not in kinds

        pending.next_retry_at_ms = 0
        second_result = asyncio.run(
            executor.drive_pending_passive_close(
                state, position.position_id, wait_until_terminal=False,
            )
        )

        assert second_result is False
        assert hedge.place_order.await_count == 1
        assert hedge.fetch_order_fill_reconciliation.await_count == 2
        kinds = [record["kind"] for record in journal.read_all()]
        assert "exit.passive_close_hedge_ack_reconcile_in_progress" in kinds

    def test_ubusdt_bybit_duplicate_partial_retries_remaining_with_new_cid(self):
        """UBUSDT regression: partial duplicate evidence retries remaining reduce-only."""
        journal = _open_journal()

        from lightfee.engine.close_executor import CloseExecutor

        class FilledMakerAdapter(VenueAdapter):
            @property
            def venue(self):
                return Venue.BINANCE

            async def place_order(self, request):
                return _make_order_fill(
                    venue=Venue.BINANCE,
                    symbol=request.symbol,
                    side=request.side,
                    quantity=request.quantity,
                    price=request.price or 1.0,
                )

            async def fetch_position(self, symbol):
                return None

            async def query_passive_order_progress(self, symbol, order_id, client_order_id, side):
                return PassiveOrderProgress(
                    venue=Venue.BINANCE,
                    symbol=symbol,
                    side=side,
                    order_id=order_id,
                    client_order_id=client_order_id,
                    cumulative_quantity=400.0,
                    average_price=1.0,
                    fee_quote=0.0,
                    last_fill_time_ms=1500,
                    state=PassiveOrderState.FILLED,
                    observed_at_ms=1500,
                )

        class PartialDuplicateBybitAdapter(VenueAdapter):
            def __init__(self):
                self.place_order_calls = []
                self.reconciliation_lookups = []

            @property
            def venue(self):
                return Venue.BYBIT

            async def place_order(self, request):
                self.place_order_calls.append(request)
                if len(self.place_order_calls) == 1:
                    raise OrderSubmitError(
                        SubmitFailureClass.REJECTED,
                        "bybit order failed: bybit retCode=110072 retMsg=OrderLinkedID is duplicate",
                    )
                return _make_order_fill(
                    venue=Venue.BYBIT,
                    symbol=request.symbol,
                    side=request.side,
                    quantity=request.quantity,
                    price=request.price or 1.0,
                    order_id="retry-oid",
                )

            async def fetch_order_fill_reconciliation(self, symbol, order_id, client_order_id=None):
                self.reconciliation_lookups.append((symbol, order_id, client_order_id))
                return OrderFillReconciliation(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=100.0,
                    average_price=1.0,
                    order_id="partial-oid",
                    client_order_id=client_order_id,
                    filled_at_ms=1600,
                    metadata={
                        "evidence_source": "bybit_execution_list",
                        "queried_endpoints": ["/v5/execution/list"],
                        "response_classification": "filled",
                    },
                )

            async def fetch_position(self, symbol):
                return PositionSnapshot(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=400.0,
                    entry_price=1.0,
                    observed_at_ms=1700,
                )

            async def query_passive_order_progress(self, symbol, order_id, client_order_id, side):
                return None

        binance = FilledMakerAdapter()
        bybit = PartialDuplicateBybitAdapter()
        adapters = {Venue.BINANCE: binance, Venue.BYBIT: bybit}
        executor = PassiveCloseExecutor(
            adapters, journal, config_overrides={"runtime_mode": "paper"},
        )
        executor.set_close_executor(CloseExecutor(adapters, journal))
        executor.set_l2_mid_resolver(lambda venue, symbol: 1.0)

        state = EngineState()
        position = _make_position(
            position_id="entry-ubusdt-passive-partial",
            symbol="UBUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=400.0,
            short_quantity=400.0,
            matched_quantity=400.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=400.0,
            chunk_quantities=[400.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="maker-oid",
                maker_client_order_id="maker-cid",
            ),
            maker_fill=PendingPassiveLegFill(
                quantity=400.0,
                average_price=1.0,
                order_id="maker-oid",
                client_order_id="maker-cid",
            ),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False)
        )

        assert result is True
        assert len(bybit.place_order_calls) == 2
        assert bybit.place_order_calls[0].client_order_id != bybit.place_order_calls[1].client_order_id
        assert bybit.place_order_calls[1].reduce_only is True
        assert bybit.place_order_calls[1].quantity == pytest.approx(300.0)
        assert pending.hedge_fill.quantity == pytest.approx(400.0)
        assert pending.hedge_fill.client_order_id == bybit.place_order_calls[1].client_order_id
        assert pending.short_legs[-1].client_order_id == bybit.place_order_calls[1].client_order_id
        assert position.position_id not in state.pending_passive_closes
        payload = [
            record["payload"] for record in journal.read_all()
            if record["kind"] == "order.reconcile_result"
        ][-1]
        assert payload["status"] == "partial"
        assert payload["target_qty"] == pytest.approx(400.0)
        assert payload["reconciled_qty"] == pytest.approx(100.0)
        assert payload["live_qty"] == pytest.approx(400.0)
        assert payload["remaining_qty"] == pytest.approx(300.0)

    def test_ubusdt_bybit_duplicate_partial_retry_failure_backs_off(self):
        """Partial evidence followed by retry failure must not escape the state machine."""
        journal = _open_journal()

        class PartialDuplicateRetryFailsAdapter(VenueAdapter):
            def __init__(self):
                self.place_order_calls = []

            @property
            def venue(self):
                return Venue.BYBIT

            async def place_order(self, request):
                self.place_order_calls.append(request)
                if len(self.place_order_calls) == 1:
                    raise OrderSubmitError(
                        SubmitFailureClass.REJECTED,
                        "bybit order failed: bybit retCode=110072 retMsg=OrderLinkedID is duplicate",
                    )
                raise OrderSubmitError(
                    SubmitFailureClass.UNCERTAIN,
                    "retry submit timed out after duplicate reconciliation",
                )

            async def fetch_order_fill_reconciliation(self, symbol, order_id, client_order_id=None):
                return OrderFillReconciliation(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=100.0,
                    average_price=1.0,
                    order_id="partial-oid",
                    client_order_id=client_order_id,
                    filled_at_ms=1600,
                    metadata={
                        "evidence_source": "bybit_execution_list",
                        "queried_endpoints": ["/v5/execution/list"],
                        "response_classification": "filled",
                    },
                )

            async def fetch_position(self, symbol):
                return PositionSnapshot(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=400.0,
                    entry_price=1.0,
                    observed_at_ms=1700,
                )

        bybit = PartialDuplicateRetryFailsAdapter()
        executor = PassiveCloseExecutor({Venue.BYBIT: bybit}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 1.0)
        position = _make_position(
            position_id="entry-ubusdt-passive-retry-fails",
            symbol="UBUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=400.0,
            short_quantity=400.0,
            matched_quantity=400.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=400.0,
            chunk_quantities=[400.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=400.0, average_price=1.0),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
        )

        result = asyncio.run(
            executor._submit_hedge_for_delta(
                EngineState(), pending, position, 400.0, maker_terminal=True,
            )
        )

        assert result.success is False
        assert result.filled == pytest.approx(100.0)
        assert result.residual == pytest.approx(300.0)
        assert result.error == "duplicate_client_order_id_retry_failed"
        assert len(bybit.place_order_calls) == 2
        assert pending.hedge_fill.quantity == pytest.approx(100.0)
        kinds = [record["kind"] for record in journal.read_all()]
        assert "exit.passive_close_hedge_duplicate_client_order_retry_failed" in kinds

    def test_ubusdt_bybit_duplicate_no_evidence_backs_off_no_blind_retry(self):
        """UBUSDT regression: no duplicate evidence must not place a new cid."""
        journal = _open_journal()

        class DuplicateNoEvidenceBybitAdapter(VenueAdapter):
            def __init__(self):
                self.place_order_calls = []

            @property
            def venue(self):
                return Venue.BYBIT

            async def place_order(self, request):
                self.place_order_calls.append(request)
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    "bybit order failed: bybit retCode=110072 retMsg=OrderLinkedID is duplicate",
                )

            async def fetch_order_fill_reconciliation(self, symbol, order_id, client_order_id=None):
                return None

            async def fetch_position(self, symbol):
                return PositionSnapshot(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=400.0,
                    entry_price=1.0,
                    observed_at_ms=1700,
                )

        bybit = DuplicateNoEvidenceBybitAdapter()
        executor = PassiveCloseExecutor({Venue.BYBIT: bybit}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 1.0)
        position = _make_position(
            position_id="entry-ubusdt-passive-none",
            symbol="UBUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=400.0,
            short_quantity=400.0,
            matched_quantity=400.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=400.0,
            chunk_quantities=[400.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=400.0, average_price=1.0),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
        )

        result = asyncio.run(
            executor._submit_hedge_for_delta(
                EngineState(), pending, position, 400.0, maker_terminal=True,
            )
        )

        assert result.success is False
        assert result.error == "duplicate_client_order_id_backoff"
        assert len(bybit.place_order_calls) == 1
        payload = [
            record["payload"] for record in journal.read_all()
            if record["kind"] == "order.reconcile_result"
        ][-1]
        assert payload["status"] == "none"
        assert payload["next_action"] == "backoff_recheck"

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
        assert bybit.place_order_calls == 0
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

    def test_fallback_zero_fill_no_pending_past_deadline_enters_fail_closed(self):
        """V1 parity: repeated zero close result after fallback deadline is fail-closed."""
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.risk.modes import GlobalRiskMode

        journal = _open_journal()
        mock_close_exec = MagicMock(spec=CloseExecutor)

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

        executor = PassiveCloseExecutor(
            {},
            journal,
            config_overrides={"maker_hedge_deadline_ms": 800},
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
                phase_started_at_ms=1_000,
            ),
            maker_fill=PendingPassiveLegFill(quantity=0.4, average_price=50000.0),
            hedge_fill=PendingPassiveLegFill(quantity=0.4, average_price=50000.0),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending
        executor._now_ms = lambda: 1_801

        result = asyncio.run(
            executor._fallback_to_aggressive_close(state, pending, position)
        )

        assert result is False
        assert state.risk_mode == GlobalRiskMode.FAIL_CLOSED
        assert pending.next_retry_at_ms == 0
        kinds = [record["kind"] for record in journal.read_all()]
        assert "execution.close_deadline_breached" in kinds
        assert "exit.passive_close_fallback_zero_fill_no_pending" not in kinds


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

    def test_capability_disables_aster_and_hyperliquid_passive_amend(self):
        journal = _open_journal()
        executor = PassiveCloseExecutor({}, journal)

        assert executor._passive_amend_supported(Venue.BINANCE) is True
        assert executor._passive_amend_supported(Venue.ASTER) is False
        assert executor._passive_amend_supported(Venue.HYPERLIQUID) is False

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

    def test_okx_amend_invalid_request_type_falls_back_to_cancel_replace(self):
        """OKX amend endpoint 405/50115 is an amend-path failure, not a generic retry."""
        journal = _open_journal()
        state = EngineState()
        position = _make_position(
            position_id="entry-okx-amend-50115",
            symbol="ZECUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BINANCE,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.1,
            chunk_quantities=[0.1],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="old-okx-oid",
                maker_client_order_id="old-okx-cid",
                maker_resting_limit_price=10.0,
            ),
        )
        state.pending_passive_closes[position.position_id] = pending

        amend_error = OrderSubmitError(
            SubmitFailureClass.REJECTED,
            'HTTP 405: {"code":"50115","data":[],"msg":"Invalid request type"}',
        )
        amend_error.endpoint = "POST /api/v5/trade/amend-order"
        amend_error.http_status = 405
        amend_error.exchange_code = "50115"
        amend_error.exchange_msg = "Invalid request type"

        mock_adapter = _mock_adapter_with_tick(Venue.OKX)
        mock_adapter.amend_passive_order = AsyncMock(side_effect=amend_error)
        mock_adapter.cancel_passive_order = AsyncMock(return_value=_make_passive_ack(
            venue=Venue.OKX,
            order_id="old-okx-oid",
            client_order_id="old-okx-cid",
        ))
        mock_adapter.submit_passive_order = AsyncMock(return_value=_make_passive_ack(
            venue=Venue.OKX,
            order_id="new-okx-oid",
            client_order_id="new-okx-cid",
            price=10.2,
            quantity=0.1,
        ))

        executor = PassiveCloseExecutor({Venue.OKX: mock_adapter}, journal)
        executor.set_l2_mid_resolver(lambda venue, symbol: 10.2)
        executor.set_l2_quote_resolver(lambda venue, symbol: (10.19, 10.21))

        asyncio.run(
            executor._amend_maker_order(
                state,
                pending,
                position,
                Venue.OKX,
                Side.SELL,
                "long",
                10.2,
                0.1,
                0.1,
                10.2,
            )
        )

        assert pending.phase_state.maker_order_id == "new-okx-oid"
        events = journal.read_all()
        fallback = [
            event["payload"] for event in events
            if event["kind"] == "exit.passive_close_amend_unsupported_cancel_replace"
        ][-1]
        assert fallback["venue"] == "okx"
        assert fallback["reason"] == "okx_amend_invalid_request_type"
        assert fallback["exchange_code"] == "50115"
        assert "exit.passive_close_amend_failed" not in [event["kind"] for event in events]

    def test_okx_amend_failure_reconciles_and_retains_live_order_identity(self):
        """OKX amend failures leave the old order alive by default; query before acting."""
        journal = _open_journal()
        state = EngineState()
        position = _make_position(
            position_id="entry-okx-amend-open",
            symbol="SAHARAUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=1330.0,
            chunk_quantities=[1330.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="old-okx-oid",
                maker_client_order_id="old-okx-cid",
                maker_resting_limit_price=0.043,
            ),
        )
        state.pending_passive_closes[position.position_id] = pending

        amend_error = OrderSubmitError(
            SubmitFailureClass.UNCERTAIN,
            "OKX amend-order timeout",
        )
        amend_error.endpoint = "POST /api/v5/trade/amend-order"

        mock_adapter = _mock_adapter_with_tick(Venue.OKX)
        mock_adapter.amend_passive_order = AsyncMock(side_effect=amend_error)
        mock_adapter.query_passive_order_progress = AsyncMock(return_value=_make_passive_progress(
            venue=Venue.OKX,
            symbol="SAHARAUSDT",
            side=Side.SELL,
            order_id="old-okx-oid",
            client_order_id="old-okx-cid",
            cumulative_quantity=120.0,
            average_price=0.043,
            state=PassiveOrderState.PARTIALLY_FILLED,
        ))
        mock_adapter.cancel_passive_order = AsyncMock()
        mock_adapter.submit_passive_order = AsyncMock()

        executor = PassiveCloseExecutor({Venue.OKX: mock_adapter}, journal)

        asyncio.run(
            executor._amend_maker_order(
                state,
                pending,
                position,
                Venue.OKX,
                Side.SELL,
                "long",
                0.044,
                1210.0,
                0.001,
                0.044,
            )
        )

        mock_adapter.query_passive_order_progress.assert_awaited_once()
        mock_adapter.cancel_passive_order.assert_not_called()
        mock_adapter.submit_passive_order.assert_not_called()
        assert pending.phase_state.maker_order_id == "old-okx-oid"
        assert pending.phase_state.maker_client_order_id == "old-okx-cid"
        assert pending.maker_fill.quantity == pytest.approx(120.0)
        records = journal.read_all()
        kinds = [event["kind"] for event in records]
        assert "exit.passive_close_amend_order_truth_retained" in kinds
        assert "exit.passive_close_amend_failed" not in kinds

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

    def test_cancel_ack_old_order_alive_blocks_replacement_until_terminal_truth(self):
        """Cancel ACK is not terminal truth; old order must be dead before replacement."""
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
        mock_adapter.query_passive_order_progress = AsyncMock(return_value=_make_passive_progress(
            state=PassiveOrderState.OPEN,
            cumulative_quantity=0.0,
            order_id="old-oid",
            client_order_id="old-cid",
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

        mock_adapter.query_passive_order_progress.assert_awaited_once()
        mock_adapter.submit_passive_order.assert_not_called()
        assert pending.phase_state.maker_order_id == "old-oid"
        assert pending.phase_state.maker_client_order_id == "old-cid"
        assert pending.next_retry_at_ms > 0

        events = journal.read_all()
        blocked = [
            e["payload"] for e in events
            if e.get("kind") == "exit.passive_close_cancel_replace_blocked_double_order_risk"
        ]
        assert len(blocked) == 1
        assert blocked[-1]["reason"] == "cancel_ack_without_terminal_order_truth"
        assert blocked[-1]["next_action"] == "retry_cancel_replace_after_order_truth"

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


class TestProcessPendingPassiveCloseLiveFlatReconcile:
    def _arrange_live_flat_cleanup(self):
        journal = _open_journal()

        long_adapter = _mock_adapter_passive_ok(Venue.BINANCE)
        short_adapter = _mock_adapter_with_tick(Venue.BYBIT)
        for adapter, venue, side in (
            (long_adapter, Venue.BINANCE, Side.BUY),
            (short_adapter, Venue.BYBIT, Side.SELL),
        ):
            adapter.fetch_position = AsyncMock(return_value=PositionSnapshot(
                venue=venue, symbol="UBUSDT", side=side,
                quantity=0.0, entry_price=0.0, observed_at_ms=3000,
            ))
            adapter.place_order = AsyncMock()
        long_adapter.submit_passive_order = AsyncMock()
        short_adapter.submit_passive_order = AsyncMock()

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: long_adapter, Venue.BYBIT: short_adapter}, journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 0.01)

        state = EngineState()
        position = _make_position(
            position_id="entry-flat-ubusdt",
            symbol="UBUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=1.0,
            short_quantity=1.0,
            matched_quantity=1.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=1.0,
            chunk_quantities=[1.0],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=1.0, average_price=0.01),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
            next_retry_at_ms=0,
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending
        return state, position, journal, executor, long_adapter, short_adapter

    def test_process_pending_passive_closes_clears_live_flat_state_before_hedge(self):
        """Pending passive close with both exchange legs flat must be removed locally."""
        state, position, journal, executor, long_adapter, short_adapter = (
            self._arrange_live_flat_cleanup()
        )

        remaining = asyncio.run(executor.process_pending_passive_closes(state, now_ms=3000))

        assert remaining == set()
        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions
        long_adapter.place_order.assert_not_called()
        short_adapter.place_order.assert_not_called()
        long_adapter.submit_passive_order.assert_not_called()
        short_adapter.submit_passive_order.assert_not_called()

        kinds = [e.get("kind") for e in journal.read_all()]
        assert "recovery.flat" in kinds
        assert "runtime.position_drift_corrected" in kinds

    def test_live_flat_cleanup_normalizes_dict_shaped_pending_close_reconciliation_queue(self):
        state, position, journal, executor, long_adapter, short_adapter = (
            self._arrange_live_flat_cleanup()
        )
        position.position_id = "entry-1780771924982-BABYUSDT"
        position.symbol = "BABYUSDT"
        position.long_venue = Venue.OKX
        position.short_venue = Venue.BYBIT
        executor._adapters = {Venue.OKX: long_adapter, Venue.BYBIT: short_adapter}
        long_adapter.venue = Venue.OKX
        pending = state.pending_passive_closes.pop("entry-flat-ubusdt")
        pending.position_id = position.position_id
        pending.position_snapshot = position
        pending.reason = "pending_passive_close_flat_probe"
        state.open_positions.clear()
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending
        state.pending_close_reconciliations = {
            position.position_id: {
                "position_id": position.position_id,
                "symbol": position.symbol,
                "kind": "final",
                "closed_at_ms": 1780771929000,
            }
        }

        asyncio.run(executor.process_pending_passive_closes(state, now_ms=3000))

        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions
        assert isinstance(state.pending_close_reconciliations, list)
        kinds = [e.get("kind") for e in journal.read_all()]
        assert "recovery.flat" in kinds
        assert "runtime.passive_close_tick_error" not in kinds

    def test_live_flat_cleanup_does_not_emit_terminal_success_when_registration_fails(self, monkeypatch):
        state, position, journal, executor, *_ = self._arrange_live_flat_cleanup()
        pending = state.pending_passive_closes[position.position_id]

        def fail_registration(*args, **kwargs):
            raise RuntimeError("simulated queue registration failure")

        monkeypatch.setattr(
            executor,
            "_register_close_reconciliation_after_live_flat",
            fail_registration,
        )

        try:
            executor._clear_live_flat_state(
                state,
                pending,
                position,
                source="pending_passive_close_flat_probe",
                actual_long_size=0.0,
                actual_short_size=0.0,
            )
        except RuntimeError:
            pass

        assert position.position_id in state.pending_passive_closes
        assert position.position_id in state.open_positions
        kinds = [e.get("kind") for e in journal.read_all()]
        assert "runtime.position_lifecycle_terminal" not in kinds
        assert "exit.passive_close_live_flat_cleanup_failed" in kinds

    def test_live_flat_cleanup_does_not_emit_terminal_success_when_core_clear_fails(self, monkeypatch):
        state, position, journal, executor, *_ = self._arrange_live_flat_cleanup()
        pending = state.pending_passive_closes[position.position_id]

        def fail_core_clear(*args, **kwargs):
            raise RuntimeError("simulated core clear failure")

        monkeypatch.setattr(
            passive_close_module,
            "clear_legacy_recovery_block_via_core",
            fail_core_clear,
        )

        try:
            executor._clear_live_flat_state(
                state,
                pending,
                position,
                source="pending_passive_close_flat_probe",
                actual_long_size=0.0,
                actual_short_size=0.0,
            )
        except RuntimeError:
            pass

        assert position.position_id in state.pending_passive_closes
        assert position.position_id in state.open_positions
        kinds = [e.get("kind") for e in journal.read_all()]
        assert "runtime.position_drift_detected" not in kinds
        assert "exit.passive_close_fallback_terminal_flat" not in kinds
        assert "runtime.position_lifecycle_terminal" not in kinds
        assert "recovery.flat" not in kinds
        assert "runtime.position_drift_corrected" not in kinds
        assert "exit.passive_close_live_flat_cleanup_failed" in kinds

    def test_live_flat_cleanup_restores_recovery_risk_state_when_legacy_clear_journal_fails(self, monkeypatch):
        from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode

        state, position, journal, executor, *_ = self._arrange_live_flat_cleanup()
        pending = state.pending_passive_closes[position.position_id]
        state.lifecycle = EngineLifecycle.RISK_ONLY
        state.risk_mode = GlobalRiskMode.FAIL_CLOSED
        state.recovery_blocked_reason = (
            "startup_recovery_pending_work_without_open_positions"
        )
        state.recovery_blocked_at_ms = 123
        state.global_risk_reason = (
            "startup_recovery_pending_work_without_open_positions"
        )
        original_append = journal.append

        def fail_legacy_clear_journal(kind, *args, **kwargs):
            if kind == "recovery.legacy_block_cleared":
                raise RuntimeError("simulated legacy clear journal failure")
            return original_append(kind, *args, **kwargs)

        monkeypatch.setattr(journal, "append", fail_legacy_clear_journal)

        executor._clear_live_flat_state(
            state,
            pending,
            position,
            source="pending_passive_close_flat_probe",
            actual_long_size=0.0,
            actual_short_size=0.0,
        )

        assert position.position_id in state.pending_passive_closes
        assert position.position_id in state.open_positions
        assert state.lifecycle == EngineLifecycle.RISK_ONLY
        assert state.risk_mode == GlobalRiskMode.FAIL_CLOSED
        assert (
            state.recovery_blocked_reason
            == "startup_recovery_pending_work_without_open_positions"
        )
        assert state.recovery_blocked_at_ms == 123
        assert (
            state.global_risk_reason
            == "startup_recovery_pending_work_without_open_positions"
        )
        events = journal.read_all()
        kinds = [e.get("kind") for e in events]
        assert "runtime.position_lifecycle_terminal" not in kinds
        assert "recovery.flat" not in kinds
        assert "runtime.position_drift_corrected" not in kinds
        failure = next(
            e for e in events
            if e.get("kind") == "exit.passive_close_live_flat_cleanup_failed"
        )
        assert failure["payload"]["reason"] == "recovery_core_clear_failed"

    def test_live_flat_cleanup_records_v1_recovery_payload_fields(self):
        """V1 recovery logs exact flat-probe position and venue sizing evidence."""
        state, position, journal, executor, *_ = self._arrange_live_flat_cleanup()

        asyncio.run(executor.process_pending_passive_closes(state, now_ms=3000))

        drift = next(
            e for e in journal.read_all()
            if e.get("kind") == "runtime.position_drift_detected"
        )
        data = drift["payload"]
        assert data["position_id"] == position.position_id
        assert data["symbol"] == "UBUSDT"
        assert data["long_venue"] == "binance"
        assert data["short_venue"] == "bybit"
        assert data["expected_size"] == 1.0
        assert data["old_quantity"] == 1.0
        assert data["actual_long_size"] == 0.0
        assert data["actual_short_size"] == 0.0
        assert data["new_quantity"] == 0.0
        assert data["source"] == "pending_passive_close_flat_probe"

    @pytest.mark.parametrize(
        "last_error",
        [
            "pending passive close failed for entry-flat-ubusdt",
            "Bybit ReduceOnly Order is rejected for UBUSDT: position is flat",
        ],
    )
    def test_live_flat_cleanup_clears_matching_last_error(self, last_error):
        state, _, _, executor, *_ = self._arrange_live_flat_cleanup()
        state.last_error = last_error

        asyncio.run(executor.process_pending_passive_closes(state, now_ms=3000))

        assert state.last_error is None

    def test_live_flat_cleanup_syncs_current_state_view_without_position(self):
        state, position, _, executor, *_ = self._arrange_live_flat_cleanup()

        asyncio.run(executor.process_pending_passive_closes(state, now_ms=3000))

        from lightfee.engine.loop_control import _export_current_state_snapshot

        path = Path(tempfile.mkdtemp()) / "live-state-current.json"
        _export_current_state_snapshot(state, str(path))
        data = json.loads(path.read_text())
        assert data["open_position_count"] == 0
        assert all(
            item["position_id"] != position.position_id
            for item in data["open_positions"]
        )
        assert data["pending_passive_close_count"] == 0

    def test_live_flat_cleanup_persistent_state_view_drops_pending_and_open(self):
        state, position, _, executor, *_ = self._arrange_live_flat_cleanup()

        asyncio.run(executor.process_pending_passive_closes(state, now_ms=3000))

        from lightfee.engine.recovery import build_persistent_state_view

        persisted = build_persistent_state_view(state)
        assert position.position_id not in persisted["open_positions"]
        assert position.position_id not in persisted["pending_passive_closes"]
        assert persisted["open_position_count"] == 0
        assert persisted["pending_passive_close_count"] == 0

    def test_xcnusdt_recovered_live_flat_cleanup_records_diagnostic_payload(self):
        """XCNUSDT recovered passive close: both venues flat clears with V1 payload."""
        state, position, journal, executor, long_adapter, short_adapter = (
            self._arrange_live_flat_cleanup()
        )
        position.position_id = "live-recovered:XCNUSDT:bybit->aster"
        position.symbol = "XCNUSDT"
        position.long_venue = Venue.BYBIT
        position.short_venue = Venue.ASTER
        position.long_quantity = 5070.0
        position.short_quantity = 5070.0
        position.matched_quantity = 5070.0
        pending = state.pending_passive_closes.pop("entry-flat-ubusdt")
        pending.position_id = position.position_id
        pending.position_snapshot = position
        pending.target_quantity = 5070.0
        pending.chunk_quantities = [5070.0]
        pending.maker_fill.quantity = 0.0
        pending.hedge_fill.quantity = 0.0
        pending.phase_state.active_maker_leg = ActiveMakerLeg.SHORT
        state.open_positions.clear()
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending
        executor._adapters = {Venue.BYBIT: long_adapter, Venue.ASTER: short_adapter}
        long_adapter.venue = Venue.BYBIT
        short_adapter.venue = Venue.ASTER
        long_adapter.fetch_position = AsyncMock(return_value=PositionSnapshot(
            venue=Venue.BYBIT, symbol="XCNUSDT", side=Side.BUY,
            quantity=0.0, entry_price=0.0, observed_at_ms=3000,
        ))
        short_adapter.fetch_position = AsyncMock(return_value=PositionSnapshot(
            venue=Venue.ASTER, symbol="XCNUSDT", side=Side.SELL,
            quantity=0.0, entry_price=0.0, observed_at_ms=3000,
        ))

        asyncio.run(executor.process_pending_passive_closes(state, now_ms=3000))

        assert position.position_id not in state.open_positions
        assert position.position_id not in state.pending_passive_closes
        drift = next(
            e for e in journal.read_all()
            if e.get("kind") == "runtime.position_drift_detected"
        )
        assert drift["payload"]["position_id"] == position.position_id
        assert drift["payload"]["actual_long_size"] == 0.0
        assert drift["payload"]["actual_short_size"] == 0.0

    def test_xcnusdt_recovered_one_side_live_nonzero_records_diagnostic_event(self):
        """XCNUSDT one-sided live exposure must not be cleared without evidence."""
        state, position, journal, executor, long_adapter, short_adapter = (
            self._arrange_live_flat_cleanup()
        )
        position.position_id = "live-recovered:XCNUSDT:bybit->aster"
        position.symbol = "XCNUSDT"
        position.long_venue = Venue.BYBIT
        position.short_venue = Venue.ASTER
        position.matched_quantity = 5070.0
        pending = state.pending_passive_closes.pop("entry-flat-ubusdt")
        pending.position_id = position.position_id
        pending.position_snapshot = position
        pending.target_quantity = 5070.0
        pending.chunk_quantities = [5070.0]
        pending.phase_state.active_maker_leg = ActiveMakerLeg.SHORT
        state.open_positions.clear()
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending
        executor._adapters = {Venue.BYBIT: long_adapter, Venue.ASTER: short_adapter}
        long_adapter.fetch_position = AsyncMock(return_value=PositionSnapshot(
            venue=Venue.BYBIT, symbol="XCNUSDT", side=Side.BUY,
            quantity=0.0, entry_price=0.0, observed_at_ms=3000,
        ))
        short_adapter.fetch_position = AsyncMock(return_value=PositionSnapshot(
            venue=Venue.ASTER, symbol="XCNUSDT", side=Side.SELL,
            quantity=-5070.0, entry_price=0.0005, observed_at_ms=3000,
        ))

        remaining = asyncio.run(executor.process_pending_passive_closes(state, now_ms=3000))

        assert remaining == {position.position_id}
        assert position.position_id in state.open_positions
        event = next(
            e for e in journal.read_all()
            if e.get("kind") == "exit.passive_close_recovery_probe_diagnostic"
        )
        payload = event["payload"]
        assert payload["position_id"] == position.position_id
        assert payload["symbol"] == "XCNUSDT"
        assert payload["long_venue"] == "bybit"
        assert payload["short_venue"] == "aster"
        assert payload["local_quantity"] == 5070.0
        assert payload["matched_quantity"] == 5070.0
        assert payload["live_long_size"] == 0.0
        assert payload["live_short_size"] == 5070.0
        assert payload["decision"] == "not_flat"
        assert payload["next_action"] == "continue_pending_passive_close"

    def test_xcnusdt_recovered_live_fetch_partial_failure_records_retry_diagnostic(self):
        """Partial live fetch failure must retry conservatively and explain why."""
        state, position, journal, executor, long_adapter, short_adapter = (
            self._arrange_live_flat_cleanup()
        )
        position.position_id = "live-recovered:XCNUSDT:bybit->aster"
        position.symbol = "XCNUSDT"
        position.long_venue = Venue.BYBIT
        position.short_venue = Venue.ASTER
        position.matched_quantity = 5070.0
        pending = state.pending_passive_closes.pop("entry-flat-ubusdt")
        pending.position_id = position.position_id
        pending.position_snapshot = position
        pending.target_quantity = 5070.0
        pending.chunk_quantities = [5070.0]
        pending.phase_state.active_maker_leg = ActiveMakerLeg.SHORT
        state.open_positions.clear()
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending
        executor._adapters = {Venue.BYBIT: long_adapter, Venue.ASTER: short_adapter}
        long_adapter.fetch_position = AsyncMock(return_value=PositionSnapshot(
            venue=Venue.BYBIT, symbol="XCNUSDT", side=Side.BUY,
            quantity=0.0, entry_price=0.0, observed_at_ms=3000,
        ))
        short_adapter.fetch_position = AsyncMock(side_effect=RuntimeError("aster timeout"))

        remaining = asyncio.run(executor.process_pending_passive_closes(state, now_ms=3000))

        assert remaining == {position.position_id}
        assert position.position_id in state.open_positions
        event = next(
            e for e in journal.read_all()
            if e.get("kind") == "exit.passive_close_recovery_probe_diagnostic"
        )
        payload = event["payload"]
        assert payload["decision"] == "probe_incomplete"
        assert payload["next_action"] == "retry_live_flat_probe"
        assert payload["live_long_size"] == 0.0
        assert payload["live_short_size"] is None
        assert "aster timeout" in payload["live_short_error"]

    def test_open_orders_transport_request_fallback_scenarios(self):
        """Test the _transport._request fallback logic for open orders query."""
        from unittest.mock import AsyncMock, MagicMock
        from lightfee.core.contracts import VenueAdapter
        from lightfee.core.domain import Venue
        from lightfee.engine.passive_close import PassiveCloseExecutor

        executor = PassiveCloseExecutor({}, _open_journal())

        # 1. Adapter missing
        adapters = {}
        result, error = asyncio.run(
            executor._probe_venue_open_orders_flat(Venue.BINANCE, "UBUSDT", adapters)
        )
        assert result is None
        assert error == "adapter_missing"

        # 2. Adapter exists but has no fetch_open_orders and no transport
        class DummyAdapterNoTransport(VenueAdapter):
            fetch_open_orders = None
            @property
            def venue(self):
                return Venue.BINANCE
            async def place_order(self, request):
                pass
            async def fetch_position(self, symbol):
                pass

        adapter_no_transport = DummyAdapterNoTransport()
        adapters = {Venue.BINANCE: adapter_no_transport}
        result, error = asyncio.run(
            executor._probe_venue_open_orders_flat(Venue.BINANCE, "UBUSDT", adapters)
        )
        assert result is None
        assert error == "open_orders_query_unsupported"

        # 3. Adapter has transport but no fetch_open_orders, and transport request returns empty (flat)
        class DummyTransport:
            def __init__(self):
                self._request = AsyncMock(return_value=[])

        class DummyAdapterWithTransport(VenueAdapter):
            fetch_open_orders = None
            def __init__(self):
                self._transport = DummyTransport()
            @property
            def venue(self):
                return Venue.BINANCE
            async def place_order(self, request):
                pass
            async def fetch_position(self, symbol):
                pass

        adapter_with_transport = DummyAdapterWithTransport()
        adapters = {Venue.BINANCE: adapter_with_transport}
        result, error = asyncio.run(
            executor._probe_venue_open_orders_flat(Venue.BINANCE, "UBUSDT", adapters)
        )
        assert result is True
        assert error is None
        adapter_with_transport._transport._request.assert_called_once_with(
            "GET", "/fapi/v1/openOrders",
            params={"symbol": "UBUSDT"},
            private=True,
        )

        # 4. Adapter has transport, and request returns active orders (not flat)
        adapter_with_transport._transport._request = AsyncMock(return_value=[{"orderId": "123"}])
        result, error = asyncio.run(
            executor._probe_venue_open_orders_flat(Venue.BINANCE, "UBUSDT", adapters)
        )
        assert result is False
        assert "open_orders_count=1" in error

        # 5. Adapter has transport, and request raises error (untrusted)
        adapter_with_transport._transport._request = AsyncMock(side_effect=RuntimeError("connection error"))
        result, error = asyncio.run(
            executor._probe_venue_open_orders_flat(Venue.BINANCE, "UBUSDT", adapters)
        )
        assert result is None
        assert "connection error" in error

        # 6. OKX format dictionary parsing
        okx_transport = DummyTransport()
        okx_transport._request = AsyncMock(return_value={"data": [{"ordId": "okx123"}]})
        class OKXAdapter(VenueAdapter):
            fetch_open_orders = None
            def __init__(self):
                self._transport = okx_transport
            @property
            def venue(self):
                return Venue.OKX
            async def place_order(self, request):
                pass
            async def fetch_position(self, symbol):
                pass
        okx_adapter = OKXAdapter()
        adapters = {Venue.OKX: okx_adapter}
        result, error = asyncio.run(
            executor._probe_venue_open_orders_flat(Venue.OKX, "UBUSDT", adapters)
        )
        assert result is False
        assert "open_orders_count=1" in error
        okx_transport._request.assert_called_once_with(
            "GET", "/api/v5/trade/orders-pending",
            params={"instId": "UB-USDT-SWAP"},
            private=True,
        )

        # 7. Bybit format dictionary parsing
        bybit_transport = DummyTransport()
        bybit_transport._request = AsyncMock(return_value={"result": {"list": [{"orderId": "bybit123"}]}})
        class BybitAdapter(VenueAdapter):
            fetch_open_orders = None
            def __init__(self):
                self._transport = bybit_transport
            @property
            def venue(self):
                return Venue.BYBIT
            async def place_order(self, request):
                pass
            async def fetch_position(self, symbol):
                pass
        bybit_adapter = BybitAdapter()
        adapters = {Venue.BYBIT: bybit_adapter}
        result, error = asyncio.run(
            executor._probe_venue_open_orders_flat(Venue.BYBIT, "UBUSDT", adapters)
        )
        assert result is False
        assert "open_orders_count=1" in error
        bybit_transport._request.assert_called_once_with(
            "GET", "/v5/order/realtime",
            params={
                "category": "linear",
                "symbol": "UBUSDT",
                "settleCoin": "USDT",
            },
            private=True,
        )


class TestNonTerminalPartialFillHedgeGapClosure:
    """Test B: non-terminal partial maker fill → hedge gap continuously closed.

    When maker stays at PARTIALLY_FILLED with the same cumulative across cycles,
    the hedge gap (maker_fill - hedge_fill) must be re-submitted even though
    maker_fill_delta == 0. Tests the unhedged_gap-based hedge logic end-to-end
    through drive_pending_passive_close.
    """

    def test_unhedged_gap_past_v1_hedge_deadline_enters_fail_closed(self):
        """V1: passive close hedge hard breach enters fail-closed and compensates."""
        from lightfee.engine.close_executor import CloseExecutor
        from lightfee.risk.modes import GlobalRiskMode

        journal = _open_journal()
        maker_adapter = _mock_adapter_passive_ok(Venue.BINANCE)
        maker_adapter.query_passive_order_progress = AsyncMock(return_value=None)
        hedge_adapter = _mock_adapter_with_tick(Venue.OKX)
        hedge_adapter.place_order = AsyncMock(
            side_effect=AssertionError("hard-breached hedge gap must not submit another hedge")
        )

        close_executor = MagicMock(spec=CloseExecutor)
        close_executor.compensate_failed_full_close = AsyncMock()

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: maker_adapter, Venue.OKX: hedge_adapter},
            journal,
            config_overrides={"maker_hedge_deadline_ms": 800},
        )
        executor.set_close_executor(close_executor)
        executor.set_l2_mid_resolver(lambda venue, symbol: 50000.0)
        executor._now_ms = lambda: 1_801

        state = EngineState()
        position = _make_position(
            matched_quantity=1.0,
            long_quantity=1.0,
            short_quantity=1.0,
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=1.0,
            chunk_quantities=[1.0],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="oid-maker",
                maker_client_order_id="cid-maker",
                maker_resting_limit_price=50000.0,
            ),
            maker_fill=PendingPassiveLegFill(
                quantity=0.4,
                average_price=50000.0,
                last_fill_time_ms=1_000,
            ),
            hedge_fill=PendingPassiveLegFill(quantity=0.1, average_price=50000.0),
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor.drive_pending_passive_close(state, position.position_id, wait_until_terminal=False)
        )

        assert result is False
        assert state.risk_mode == GlobalRiskMode.FAIL_CLOSED
        assert pending.next_retry_at_ms == 0
        hedge_adapter.place_order.assert_not_called()
        close_executor.compensate_failed_full_close.assert_awaited_once()
        kinds = [record["kind"] for record in journal.read_all()]
        assert "execution.hedge_deadline_breached" in kinds
        assert "exit.passive_close_hedge_incomplete" not in kinds

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
            {Venue.BINANCE: maker_adapter, Venue.OKX: hedge_adapter},
            journal,
            config_overrides={"runtime_mode": "paper"},
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
            {Venue.BINANCE: maker_adapter, Venue.OKX: hedge_adapter},
            journal,
            config_overrides={"runtime_mode": "paper"},
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


class TestPassiveCloseMakerLegLiveTruthPrecheck:
    """Live-truth precheck before first maker submit."""

    def test_bybit_submit_110017_order_qty_truncated_routes_to_live_truth_closure(self):
        """Bybit 110017 after submit is terminal zero-qty evidence, not generic retry."""
        journal = _open_journal()

        class SubmitThenFlatAdapter(VenueAdapter):
            def __init__(self, venue, side, quantities, *, submit_error=None):
                self._venue = venue
                self._side = side
                self._quantities = list(quantities)
                self.submit_passive_calls = 0
                self.place_order_calls = []
                self._submit_error = submit_error

            @property
            def venue(self):
                return self._venue

            async def fetch_position(self, symbol):
                quantity = self._quantities.pop(0) if self._quantities else 0.0
                return PositionSnapshot(
                    venue=self._venue,
                    symbol=symbol,
                    side=self._side,
                    quantity=quantity,
                    entry_price=0.026 if quantity else 0.0,
                    observed_at_ms=1781531688000,
                )

            async def normalize_quantity(self, symbol, quantity):
                return quantity

            async def submit_passive_order(self, request):
                self.submit_passive_calls += 1
                if self._submit_error is not None:
                    raise self._submit_error
                return PassiveOrderAck(
                    venue=self._venue,
                    symbol=request.symbol,
                    side=request.side,
                    order_id=f"{self._venue.value}-maker",
                    client_order_id=request.client_order_id,
                    price=request.price or 0.026,
                    quantity=request.quantity,
                    accepted_at_ms=1781531688001,
                )

            async def place_order(self, request):
                self.place_order_calls.append(request)
                self._quantities = [0.0, 0.0]
                return OrderFill(
                    venue=self._venue,
                    symbol=request.symbol,
                    side=request.side,
                    quantity=request.quantity,
                    price=request.price or 0.026,
                    order_id=f"{self._venue.value}-flatten",
                    client_order_id=request.client_order_id,
                    fee_quote=0.0,
                    filled_at_ms=1781531688002,
                )

        submit_error = OrderSubmitError(
            SubmitFailureClass.REJECTED,
            "bybit passive order failed: bybit retCode=110017 "
            "retMsg=orderQty will be truncated to zero.",
        )
        submit_error.exchange_response_body = (
            '{"retCode":110017,"retMsg":"orderQty will be truncated to zero."}'
        )

        long_adapter = SubmitThenFlatAdapter(
            Venue.OKX,
            Side.BUY,
            [1800.0, 1800.0, 1800.0, 0.0, 0.0],
        )
        short_adapter = SubmitThenFlatAdapter(
            Venue.BYBIT,
            Side.SELL,
            [1800.0, 0.0, 0.0, 0.0],
            submit_error=submit_error,
        )
        executor = PassiveCloseExecutor(
            {Venue.OKX: long_adapter, Venue.BYBIT: short_adapter},
            journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 0.0265)
        executor.set_l2_quote_resolver(lambda venue, symbol: (0.0264, 0.0266))

        state = EngineState()
        position = _make_position(
            position_id="entry-1781531687393-HOMEUSDT",
            symbol="HOMEUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            long_quantity=1800.0,
            short_quantity=1800.0,
            matched_quantity=1800.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=1800.0,
            chunk_quantities=[1800.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.SHORT,
            ),
            maker_fill=PendingPassiveLegFill(quantity=0.0),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
            next_retry_at_ms=0,
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor.drive_pending_passive_close(
                state, position.position_id, wait_until_terminal=False,
            )
        )

        assert result is True
        assert short_adapter.submit_passive_calls == 1
        assert len(long_adapter.place_order_calls) == 1
        assert state.pending_passive_closes == {}
        assert state.open_positions == {}
        kinds = [record["kind"] for record in journal.read_all()]
        assert "exit.passive_close_terminal_zero_qty_reduce_only_evidence" in kinds
        assert "exit.passive_close_live_one_sided_flatten" in kinds
        assert "exit.passive_close_resolved" in kinds

    def test_one_sided_flatten_settling_does_not_submit_maker_same_cycle(self):
        """A live flatten action consumes this drive even if flat truth has not settled."""
        journal = _open_journal()

        class SettlingLiveTruthAdapter(VenueAdapter):
            def __init__(self, venue, side, quantities):
                self._venue = venue
                self._side = side
                self._quantities = list(quantities)
                self.place_order_calls = []
                self.submit_passive_order = AsyncMock(
                    side_effect=OrderSubmitError(
                        SubmitFailureClass.REJECTED,
                        "bybit retCode=110017 retMsg=current position is zero, cannot fix reduce-only order qty",
                    )
                )

            @property
            def venue(self):
                return self._venue

            async def fetch_position(self, symbol):
                quantity = self._quantities.pop(0) if self._quantities else 0.0
                return PositionSnapshot(
                    venue=self._venue,
                    symbol=symbol,
                    side=self._side,
                    quantity=quantity,
                    entry_price=1.0 if quantity else 0.0,
                    observed_at_ms=1780560000000,
                )

            async def normalize_quantity(self, symbol, quantity):
                return quantity

            async def place_order(self, request):
                self.place_order_calls.append(request)
                return OrderFill(
                    venue=self._venue,
                    symbol=request.symbol,
                    side=request.side,
                    quantity=request.quantity,
                    price=request.price or 1.0,
                    order_id=f"{self._venue.value}-flatten",
                    client_order_id=request.client_order_id,
                    fee_quote=0.0,
                    filled_at_ms=1780560000001,
                )

        long_adapter = SettlingLiveTruthAdapter(Venue.OKX, Side.SELL, [23.3, 23.3])
        short_adapter = SettlingLiveTruthAdapter(Venue.BYBIT, Side.BUY, [0.0, 0.0])
        executor = PassiveCloseExecutor(
            {Venue.OKX: long_adapter, Venue.BYBIT: short_adapter},
            journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 1.0)
        executor.set_l2_quote_resolver(lambda venue, symbol: (0.999, 1.001))

        state = EngineState()
        position = _make_position(
            position_id="entry-axs-maker-flat-settling",
            symbol="AXSUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            long_quantity=23.3,
            short_quantity=23.3,
            matched_quantity=23.3,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="first_stage_capture",
            position_snapshot=position,
            target_quantity=23.3,
            chunk_quantities=[23.3],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.SHORT,
            ),
            maker_fill=PendingPassiveLegFill(quantity=0.0),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
            next_retry_at_ms=0,
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor.drive_pending_passive_close(
                state, position.position_id, wait_until_terminal=False,
            )
        )

        assert result is False
        short_adapter.submit_passive_order.assert_not_called()
        assert len(long_adapter.place_order_calls) == 1
        assert position.position_id in state.pending_passive_closes
        assert state.pending_passive_closes[position.position_id].next_retry_at_ms > 0

        kinds = [record["kind"] for record in journal.read_all()]
        assert "exit.passive_close_live_one_sided_flatten" in kinds
        assert "exit.passive_close_maker_submit_error" not in kinds

    def test_flat_maker_leg_flattens_other_live_leg_without_submit(self):
        """If the chosen maker leg is already flat, do not submit a doomed maker order."""
        journal = _open_journal()

        class LiveTruthAdapter(VenueAdapter):
            def __init__(self, venue, side, quantity):
                self._venue = venue
                self._side = side
                self._quantity = quantity
                self.place_order_calls = []
                self.submit_passive_order = AsyncMock(
                    side_effect=OrderSubmitError(
                        SubmitFailureClass.REJECTED,
                        "bybit retCode=110017 retMsg=current position is zero, cannot fix reduce-only order qty",
                    )
                )

            @property
            def venue(self):
                return self._venue

            async def fetch_position(self, symbol):
                return PositionSnapshot(
                    venue=self._venue,
                    symbol=symbol,
                    side=self._side,
                    quantity=self._quantity,
                    entry_price=0.02 if self._quantity else 0.0,
                    observed_at_ms=1779970000000,
                )

            async def normalize_quantity(self, symbol, quantity):
                return quantity

            async def place_order(self, request):
                self.place_order_calls.append(request)
                self._quantity = 0.0
                return OrderFill(
                    venue=self._venue,
                    symbol=request.symbol,
                    side=request.side,
                    quantity=request.quantity,
                    price=request.price or 0.02,
                    order_id=f"{self._venue.value}-flatten",
                    client_order_id=request.client_order_id,
                    fee_quote=0.0,
                    filled_at_ms=1779970000001,
                )

        long_adapter = LiveTruthAdapter(Venue.BINANCE, Side.BUY, 780.0)
        short_adapter = LiveTruthAdapter(Venue.BYBIT, Side.SELL, 0.0)
        executor = PassiveCloseExecutor(
            {Venue.BINANCE: long_adapter, Venue.BYBIT: short_adapter},
            journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 0.02)
        executor.set_l2_quote_resolver(lambda venue, symbol: (0.0199, 0.0201))

        state = EngineState()
        position = _make_position(
            position_id="entry-home-maker-flat",
            symbol="HOMEUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=780.0,
            short_quantity=780.0,
            matched_quantity=780.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=780.0,
            chunk_quantities=[780.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.SHORT,
            ),
            maker_fill=PendingPassiveLegFill(quantity=0.0),
            hedge_fill=PendingPassiveLegFill(quantity=0.0),
            next_retry_at_ms=0,
        )
        state.open_positions[position.position_id] = position
        state.pending_passive_closes[position.position_id] = pending

        result = asyncio.run(
            executor.drive_pending_passive_close(
                state, position.position_id, wait_until_terminal=False,
            )
        )

        assert result is True
        short_adapter.submit_passive_order.assert_not_called()
        assert len(long_adapter.place_order_calls) == 1
        request = long_adapter.place_order_calls[0]
        assert request.side == Side.SELL
        assert request.quantity == 780.0
        assert request.reduce_only is True
        assert request.time_in_force == TimeInForce.IOC
        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions

        kinds = [record["kind"] for record in journal.read_all()]
        assert "exit.passive_close_live_one_sided_flatten" in kinds
        assert "exit.passive_close_maker_submit_error" not in kinds


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

    def test_terminal_no_fill_adopts_matching_live_reduce_only_order(self):
        """A terminal/no-fill progress result cannot clear owner if the order is still live."""
        journal = _open_journal()
        maker_adapter = _mock_adapter_with_tick(Venue.BYBIT)
        maker_adapter.query_passive_order_progress = AsyncMock(return_value=_make_passive_progress(
            venue=Venue.BYBIT,
            side=Side.BUY,
            order_id="bybit-close-live",
            client_order_id="bybit-close-client",
            cumulative_quantity=0.0,
            average_price=0.0,
            state=PassiveOrderState.REJECTED,
        ))
        maker_adapter.fetch_open_orders = AsyncMock(return_value=[
            {
                "venue": "bybit",
                "symbol": "GENIUSUSDT",
                "side": "buy",
                "quantity": 60.0,
                "price": 0.3963,
                "reduce_only": True,
                "order_id": "bybit-close-live",
                "client_order_id": "bybit-close-client",
            }
        ])
        maker_adapter.submit_passive_order = AsyncMock()

        executor = PassiveCloseExecutor(
            {Venue.BYBIT: maker_adapter, Venue.BINANCE: _mock_adapter_passive_ok(Venue.BINANCE)},
            journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 0.3963)

        state = EngineState()
        position = _make_position(
            position_id="entry-genius",
            symbol="GENIUSUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            matched_quantity=60.0,
            long_quantity=60.0,
            short_quantity=60.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=60.0,
            chunk_quantities=[60.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.LOW_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.SHORT,
                maker_order_id="bybit-close-live",
                maker_client_order_id="bybit-close-client",
                maker_resting_limit_price=0.3963,
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
        assert pending.phase_state.phase == PassiveExecutionPhase.LOW_SLIPPAGE_MAKER
        assert pending.phase_state.maker_order_id == "bybit-close-live"
        assert pending.phase_state.maker_client_order_id == "bybit-close-client"
        assert pending.phase_state.maker_resting_limit_price == pytest.approx(0.3963)
        maker_adapter.submit_passive_order.assert_not_called()
        kinds = [event["kind"] for event in journal.read_all()]
        assert "exit.passive_close_existing_reduce_only_order_adopted" in kinds
        assert "exit.passive_close_maker_terminal_no_fill" in kinds

    def test_bybit_110017_with_existing_reduce_only_order_retains_pending(self):
        """110017 with nonzero live exposure and a matching close order is covered, not flat."""
        journal = _open_journal()

        class BybitCoveredAdapter(VenueAdapter):
            def __init__(self):
                submit_error = OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    "bybit passive order failed: bybit retCode=110017 retMsg=orderQty will be truncated to zero.",
                )
                submit_error.exchange_response_body = (
                    '{"retCode":110017,"retMsg":"orderQty will be truncated to zero."}'
                )
                self.submit_passive_order = AsyncMock(side_effect=submit_error)
                self.fetch_open_orders = AsyncMock(return_value=[
                    {
                        "venue": "bybit",
                        "symbol": "GENIUSUSDT",
                        "side": "buy",
                        "quantity": 60.0,
                        "price": 0.3963,
                        "reduce_only": True,
                        "order_id": "existing-close",
                        "client_order_id": "existing-close-client",
                    }
                ])

            @property
            def venue(self):
                return Venue.BYBIT

            async def place_order(self, request):
                raise AssertionError("one-sided IOC must not be submitted")

            async def fetch_position(self, symbol):
                return PositionSnapshot(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=60.0,
                    entry_price=0.396,
                    observed_at_ms=1781856334226,
                )

            async def normalize_quantity(self, symbol, quantity):
                return quantity

            def price_tick_size(self, symbol=None):
                return 0.0001

        bybit = BybitCoveredAdapter()
        executor = PassiveCloseExecutor(
            {Venue.BYBIT: bybit, Venue.BINANCE: _mock_adapter_passive_ok(Venue.BINANCE)},
            journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 0.3963)
        executor.set_l2_quote_resolver(lambda venue, symbol: (0.3962, 0.3964))

        state = EngineState()
        position = _make_position(
            position_id="entry-genius",
            symbol="GENIUSUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            matched_quantity=60.0,
            long_quantity=60.0,
            short_quantity=60.0,
        )
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=60.0,
            chunk_quantities=[60.0],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.LOW_SLIPPAGE_MAKER,
                active_maker_leg=ActiveMakerLeg.SHORT,
                maker_submit_attempt=0,
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
        assert position.position_id in state.pending_passive_closes
        assert pending.phase_state.maker_order_id == "existing-close"
        kinds = [event["kind"] for event in journal.read_all()]
        assert "exit.passive_close_reduce_only_quantity_covered_by_open_order" in kinds
        assert "exit.passive_close_existing_reduce_only_order_adopted" in kinds

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
        terminal_payload = terminal_events[0]["payload"]
        assert terminal_payload["exchange_code"] == ""
        assert terminal_payload["exchange_msg"] == ""
        assert terminal_payload["raw_error"] == ""

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
