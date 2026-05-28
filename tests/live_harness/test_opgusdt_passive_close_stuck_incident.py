"""P0 harness: OPGUSDT passive close stuck — live truth convergence.

Incident: maker filled 10/13, exchange shows long=0 short=10,
_advance_chunk blocks forever because maker_fill < chunk_quantity.
Fix: maker terminal filled under-filled chunk is handled, try live flat first,
then escalate to DUAL_TAKER.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path
from typing import Any

import pytest

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import (
    OrderFill,
    PassiveOrderProgress,
    PassiveOrderState,
    PositionSnapshot,
    Side,
    Venue,
)
from lightfee.engine.passive_close import (
    PassiveCloseExecutor,
    HedgeDeltaResult,
)
from lightfee.engine.state import (
    ActiveMakerLeg,
    EngineState,
    OpenPosition,
    PassiveExecutionPhase,
    PassivePhaseState,
    PendingPassiveClose,
    PendingPassiveLegFill,
)
from lightfee.persistence.journal import Journal


pytestmark = pytest.mark.live_harness

FIXTURE = Path("tests/fixtures/live_incidents/2026-05-28/opgusdt_passive_close_stuck.jsonl")


def _events() -> list[dict]:
    return [json.loads(line) for line in FIXTURE.read_text().splitlines() if line.strip()]


def _open_journal():
    d = tempfile.mkdtemp()
    j = Journal(Path(d) / "test.log")
    j.open()
    return j


def _make_opgusdt_position() -> OpenPosition:
    return OpenPosition(
        position_id="entry-1779940325179-OPGUSDT",
        symbol="OPGUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.OKX,
        long_quantity=13.0,
        short_quantity=13.0,
        long_entry_price=0.95,
        short_entry_price=0.96,
        opened_at_ms=1779940325179,
        matched_quantity=13.0,
    )


class _OneSidedResidualAdapter(VenueAdapter):
    """Mock adapter for passive close testing."""

    def __init__(self, venue: Venue, *, flat: bool, raise_fetch: bool = False, raise_open_orders: bool = False):
        self._venue = venue
        self._flat = flat
        self.raise_fetch = raise_fetch
        self.raise_open_orders = raise_open_orders
        self.fetch_calls = 0
        self.passive_progress: PassiveOrderProgress | None = None
        self.open_orders: list[dict] = []
        self._place_order_calls: list[Any] = []

    @property
    def venue(self):
        return self._venue

    async def fetch_position(self, symbol: str) -> PositionSnapshot | None:
        self.fetch_calls += 1
        if self.raise_fetch and self.fetch_calls > 1:
            raise RuntimeError("API Error")
        if self._flat:
            return PositionSnapshot(
                venue=self._venue, symbol=symbol,
                side=Side.BUY, quantity=0.0, entry_price=0.0,
                observed_at_ms=1779965469000,
            )
        return PositionSnapshot(
            venue=self._venue, symbol=symbol,
            side=Side.SELL, quantity=10.0, entry_price=0.96,
            observed_at_ms=1779965469000,
        )

    async def fetch_open_orders(self, symbol: str) -> list[dict]:
        if self.raise_open_orders:
            raise RuntimeError("API Error")
        return list(self.open_orders)

    async def place_order(self, request: Any) -> OrderFill:
        self._place_order_calls.append(request)
        return OrderFill(
            venue=self._venue,
            symbol=request.symbol,
            side=request.side,
            quantity=request.quantity,
            price=0.95,
            order_id="flatten-001",
            filled_at_ms=1779965470000,
        )

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        return quantity

    async def query_passive_order_progress(
        self, symbol: str, order_id: str, client_order_id: str, side: Side = Side.BUY
    ) -> PassiveOrderProgress | None:
        return self.passive_progress


class TestOpgUsdtIncident:

    def test_fixture_captures_advance_blocked_sequence(self):
        """Verify fixture contains the stuck advance sequence."""
        events = _events()
        kinds = [e["kind"] for e in events]
        assert "exit.passive_close_advance_blocked_maker_under_chunk" in kinds
        assert "exit.passive_close_recovery_probe_diagnostic" in kinds
        assert "runtime.position_drift_correction_failed" in kinds

        # Verify quantities match OPGUSDT scenario
        blocked = [e for e in events if e["kind"] == "exit.passive_close_advance_blocked_maker_under_chunk"][0]
        assert blocked["payload"]["maker_quantity"] == 10.0
        assert blocked["payload"]["chunk_quantity"] == 13.0
        assert blocked["payload"]["deficit"] == 3.0

    @pytest.mark.asyncio
    async def test_filled_under_chunk_escalates_dual_taker(self):
        """If maker FILLED but maker_fill < chunk, and not flat, escalate to DUAL_TAKER."""
        journal = _open_journal()
        # binance (long/maker) is flat, okx (short/hedge) is NOT flat (short=10.0)
        long_adapter = _OneSidedResidualAdapter(Venue.BINANCE, flat=True)
        short_adapter = _OneSidedResidualAdapter(Venue.OKX, flat=False)

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal,
        )

        state = EngineState()
        position = _make_opgusdt_position()
        state.open_positions[position.position_id] = position

        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=13.0,
            chunk_quantities=[13.0],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                preferred_maker_leg=ActiveMakerLeg.LONG,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="maker-001",
                maker_client_order_id="maker-client-001",
            ),
            maker_fill=PendingPassiveLegFill(
                quantity=10.0, average_price=0.95,
                order_id="maker-001",
            ),
            hedge_fill=PendingPassiveLegFill(
                quantity=10.0, average_price=0.96,
                order_id="hedge-001",
            ),
        )
        state.pending_passive_closes[position.position_id] = pending

        # Mock the progress poll to return FILLED (maker_fill = 10.0, hedge_fill = 10.0)
        long_adapter.passive_progress = PassiveOrderProgress(
            venue=Venue.BINANCE,
            symbol="OPGUSDT",
            side=Side.SELL,
            order_id="maker-001",
            client_order_id="maker-client-001",
            state=PassiveOrderState.FILLED,
            cumulative_quantity=10.0,
            average_price=0.95,
            observed_at_ms=1779965469000,
        )

        # Drive the loop
        res = await executor.drive_pending_passive_close(state, pending.position_id, wait_until_terminal=True)
        assert res is False, "should not be terminalized"

        # Check phase is escalated to DUAL_TAKER
        assert pending.phase_state.phase == PassiveExecutionPhase.DUAL_TAKER

        # Journal should show the under_chunk event
        events = journal.read_all()
        kinds = [e["kind"] for e in events]
        assert "exit.passive_close_maker_filled_under_chunk" in kinds
        ev = [e for e in events if e["kind"] == "exit.passive_close_maker_filled_under_chunk"][0]
        assert ev["payload"]["decision"] == "try_live_flat_then_dual_taker"

    @pytest.mark.asyncio
    async def test_filled_under_chunk_live_flat_clears(self):
        """If maker FILLED but maker_fill < chunk, and exchange IS flat, clear state."""
        journal = _open_journal()
        # both flat
        long_adapter = _OneSidedResidualAdapter(Venue.BINANCE, flat=True)
        short_adapter = _OneSidedResidualAdapter(Venue.OKX, flat=True)

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal,
        )

        state = EngineState()
        position = _make_opgusdt_position()
        state.open_positions[position.position_id] = position

        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=13.0,
            chunk_quantities=[13.0],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                preferred_maker_leg=ActiveMakerLeg.LONG,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="maker-001",
                maker_client_order_id="maker-client-001",
            ),
            maker_fill=PendingPassiveLegFill(
                quantity=10.0, average_price=0.95,
                order_id="maker-001",
            ),
            hedge_fill=PendingPassiveLegFill(
                quantity=10.0, average_price=0.96,
                order_id="hedge-001",
            ),
        )
        state.pending_passive_closes[position.position_id] = pending

        long_adapter.passive_progress = PassiveOrderProgress(
            venue=Venue.BINANCE,
            symbol="OPGUSDT",
            side=Side.SELL,
            order_id="maker-001",
            client_order_id="maker-client-001",
            state=PassiveOrderState.FILLED,
            cumulative_quantity=10.0,
            average_price=0.95,
            observed_at_ms=1779965469000,
        )

        res = await executor.drive_pending_passive_close(state, pending.position_id, wait_until_terminal=True)
        assert res is True, "should be terminalized and cleared"

        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions

        events = journal.read_all()
        kinds = [e["kind"] for e in events]
        assert "exit.passive_close_maker_filled_under_chunk" in kinds
        assert "exit.passive_close_fallback_terminal_flat" in kinds

    @pytest.mark.asyncio
    async def test_fetch_position_failure_retains_state(self):
        """If fetch_position fails, clear flat is untrusted, retain state."""
        journal = _open_journal()
        # both flat, but long adapter raises error on fetch (only on second call, so probe succeeds but snap fails)
        long_adapter = _OneSidedResidualAdapter(Venue.BINANCE, flat=True, raise_fetch=True)
        short_adapter = _OneSidedResidualAdapter(Venue.OKX, flat=True)

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal,
        )

        state = EngineState()
        position = _make_opgusdt_position()
        state.open_positions[position.position_id] = position

        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=13.0,
            chunk_quantities=[13.0],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                preferred_maker_leg=ActiveMakerLeg.LONG,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="maker-001",
                maker_client_order_id="maker-client-001",
            ),
            maker_fill=PendingPassiveLegFill(quantity=10.0),
            hedge_fill=PendingPassiveLegFill(quantity=10.0),
        )
        state.pending_passive_closes[position.position_id] = pending

        long_adapter.passive_progress = PassiveOrderProgress(
            venue=Venue.BINANCE,
            symbol="OPGUSDT",
            side=Side.SELL,
            order_id="maker-001",
            client_order_id="maker-client-001",
            state=PassiveOrderState.FILLED,
            cumulative_quantity=10.0,
        )

        res = await executor.drive_pending_passive_close(state, pending.position_id, wait_until_terminal=True)
        assert res is False
        assert pending.phase_state.phase == PassiveExecutionPhase.DUAL_TAKER
        assert position.position_id in state.pending_passive_closes

        events = journal.read_all()
        kinds = [e["kind"] for e in events]
        assert "exit.passive_close_clear_flat_untrusted" in kinds
        ev = [e for e in events if e["kind"] == "exit.passive_close_clear_flat_untrusted"][0]
        assert ev["payload"]["live_truth_trusted"] is False
        assert ev["payload"]["decision"] == "retain_pending"

    @pytest.mark.asyncio
    async def test_open_order_truth_failure_retains_state(self):
        """If fetch_open_orders fails during flatness probe, clear flat is untrusted, retain state."""
        journal = _open_journal()
        # both flat, but okx raises error on open orders
        long_adapter = _OneSidedResidualAdapter(Venue.BINANCE, flat=True)
        short_adapter = _OneSidedResidualAdapter(Venue.OKX, flat=True, raise_open_orders=True)

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal,
        )

        state = EngineState()
        position = _make_opgusdt_position()
        # Do not include position in open_positions to force _probe_live_flatness path
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=13.0,
            chunk_quantities=[13.0],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                preferred_maker_leg=ActiveMakerLeg.LONG,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="",
            ),
            maker_fill=PendingPassiveLegFill(quantity=10.0),
            hedge_fill=PendingPassiveLegFill(quantity=10.0),
        )
        state.pending_passive_closes[position.position_id] = pending

        # Call recover_passive_close with adapters
        res = await executor.recover_passive_close(state, pending.position_id, {Venue.BINANCE: long_adapter, Venue.OKX: short_adapter})
        assert res == "ambiguous", "should be ambiguous because probe was untrusted and position not in state"

        # Check diagnostic event in journal
        events = journal.read_all()
        kinds = [e["kind"] for e in events]
        assert "exit.passive_close_recovery_probe_diagnostic" in kinds
        ev = [e for e in events if e["kind"] == "exit.passive_close_recovery_probe_diagnostic"][0]
        assert ev["payload"]["decision"] == "position_flat_but_open_orders_untrusted"
        assert ev["payload"]["open_order_truth_trusted"] is False

    @pytest.mark.asyncio
    async def test_live_flat_with_open_orders_retains(self):
        """If position flat but open orders present, retain state."""
        journal = _open_journal()
        long_adapter = _OneSidedResidualAdapter(Venue.BINANCE, flat=True)
        short_adapter = _OneSidedResidualAdapter(Venue.OKX, flat=True)
        # short adapter has open orders
        short_adapter.open_orders = [{"orderId": "resting-001"}]

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal,
        )

        state = EngineState()
        position = _make_opgusdt_position()
        # Do not include position in open_positions to force _probe_live_flatness path
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=13.0,
            chunk_quantities=[13.0],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                preferred_maker_leg=ActiveMakerLeg.LONG,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="",
            ),
            maker_fill=PendingPassiveLegFill(quantity=10.0),
            hedge_fill=PendingPassiveLegFill(quantity=10.0),
        )
        state.pending_passive_closes[position.position_id] = pending

        res = await executor.recover_passive_close(state, pending.position_id, {Venue.BINANCE: long_adapter, Venue.OKX: short_adapter})
        assert res == "ambiguous"

        events = journal.read_all()
        kinds = [e["kind"] for e in events]
        assert "exit.passive_close_recovery_probe_diagnostic" in kinds
        ev = [e for e in events if e["kind"] == "exit.passive_close_recovery_probe_diagnostic"][0]
        assert ev["payload"]["decision"] == "position_flat_but_open_orders_untrusted"
        assert ev["payload"]["open_order_truth_trusted"] is True  # Query succeeded, but truth is non-flat

    @pytest.mark.asyncio
    async def test_opgusdt_passive_close_full_closed_loop_clear(self):
        """Under-filled maker terminal order triggers escalation to DUAL_TAKER,
        which executes fallback close, flattening one-sided position,
        submitting order, updating exchange truth to flat, and clearing state.
        """
        journal = _open_journal()
        # Start: long (Binance) is flat, short (OKX) is NOT flat (short=10.0)
        long_adapter = _OneSidedResidualAdapter(Venue.BINANCE, flat=True)
        short_adapter = _OneSidedResidualAdapter(Venue.OKX, flat=False)

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal,
        )
        executor.set_l2_mid_resolver(lambda venue, symbol: 0.95)

        state = EngineState()
        position = _make_opgusdt_position()
        state.open_positions[position.position_id] = position

        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=13.0,
            chunk_quantities=[13.0],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
                preferred_maker_leg=ActiveMakerLeg.LONG,
                active_maker_leg=ActiveMakerLeg.LONG,
                maker_order_id="maker-001",
                maker_client_order_id="maker-client-001",
            ),
            maker_fill=PendingPassiveLegFill(
                quantity=10.0, average_price=0.95,
                order_id="maker-001",
            ),
            hedge_fill=PendingPassiveLegFill(
                quantity=10.0, average_price=0.96,
                order_id="hedge-001",
            ),
        )
        state.pending_passive_closes[position.position_id] = pending

        # Mock maker order progress poll to return FILLED
        long_adapter.passive_progress = PassiveOrderProgress(
            venue=Venue.BINANCE,
            symbol="OPGUSDT",
            side=Side.SELL,
            order_id="maker-001",
            client_order_id="maker-client-001",
            state=PassiveOrderState.FILLED,
            cumulative_quantity=10.0,
            average_price=0.95,
            observed_at_ms=1779965469000,
        )

        # Drive 1: maker FILLED but maker_fill (10) < chunk (13).
        # Should NOT block, should escalate to DUAL_TAKER and return False.
        res1 = await executor.drive_pending_passive_close(state, pending.position_id, wait_until_terminal=True)
        assert res1 is False
        assert pending.phase_state.phase == PassiveExecutionPhase.DUAL_TAKER
        assert position.position_id in state.pending_passive_closes
        assert position.position_id in state.open_positions

        # Inject fake CloseExecutor to pass isinstance check
        from lightfee.engine.close_executor import CloseExecutor
        class _FakeCloseExecutor(CloseExecutor):
            def __init__(self):
                pass
        executor._close_executor = _FakeCloseExecutor()

        # Define hook in OKX adapter: when place_order is called, set self._flat = True
        orig_place_order = short_adapter.place_order
        async def mock_place_order(request):
            short_adapter._flat = True
            return await orig_place_order(request)
        short_adapter.place_order = mock_place_order

        # Drive 2: now in DUAL_TAKER phase.
        # It should run fallback_to_aggressive_close, detect OKX is not flat (short=10),
        # call _flatten_live_one_sided_position, place close order of 10 on OKX,
        # which sets short_adapter._flat = True, check live positions again (now both flat),
        # clear state and return True!
        res2 = await executor.drive_pending_passive_close(state, pending.position_id, wait_until_terminal=True)
        assert res2 is True
        assert position.position_id not in state.pending_passive_closes
        assert position.position_id not in state.open_positions

        # Verify that OKX adapter received a place_order call
        assert len(short_adapter._place_order_calls) == 1
        close_req = short_adapter._place_order_calls[0]
        assert close_req.side == Side.BUY
        assert close_req.quantity == 10.0
        assert close_req.reduce_only is True

        # Check journal events
        events = journal.read_all()
        kinds = [e["kind"] for e in events]
        assert "exit.passive_close_maker_filled_under_chunk" in kinds
        assert "exit.passive_close_live_one_sided_flatten" in kinds
        assert "exit.passive_close_fallback_terminal_flat" in kinds

    @pytest.mark.asyncio
    async def test_opgusdt_live_one_sided_uses_live_entry_price_without_l2(self):
        """Production OPGUSDT replay: OKX is one-sided short, local L2/market
        price is unavailable, but live position entry_price is enough evidence
        for reduce-only min-notional admission.
        """
        journal = _open_journal()
        long_adapter = _OneSidedResidualAdapter(Venue.BINANCE, flat=True)
        short_adapter = _OneSidedResidualAdapter(Venue.OKX, flat=False)

        executor = PassiveCloseExecutor(
            {Venue.BINANCE: long_adapter, Venue.OKX: short_adapter},
            journal,
        )

        state = EngineState()
        position = _make_opgusdt_position()
        state.open_positions[position.position_id] = position
        pending = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=13.0,
            chunk_quantities=[13.0],
            active_chunk_index=0,
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
                preferred_maker_leg=ActiveMakerLeg.LONG,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
            maker_fill=PendingPassiveLegFill(quantity=10.0, average_price=0.95),
            hedge_fill=PendingPassiveLegFill(quantity=10.0, average_price=0.96),
        )
        state.pending_passive_closes[position.position_id] = pending

        orig_place_order = short_adapter.place_order

        async def mock_place_order(request):
            short_adapter._flat = True
            return await orig_place_order(request)

        short_adapter.place_order = mock_place_order
        live_short = await short_adapter.fetch_position("OPGUSDT")

        res = await executor._flatten_live_one_sided_position(
            state,
            pending,
            position,
            venue=Venue.OKX,
            live_snapshot=live_short,
            leg_label="short",
        )

        assert res is True
        assert len(short_adapter._place_order_calls) == 1
        close_req = short_adapter._place_order_calls[0]
        assert close_req.side == Side.BUY
        assert close_req.quantity == 10.0
        assert close_req.reduce_only is True
        assert close_req.price == pytest.approx(0.96)

        events = journal.read_all()
        kinds = [e["kind"] for e in events]
        assert "exit.passive_close_hedge_dust_aborted" not in kinds
        assert "exit.passive_close_live_one_sided_flatten" in kinds
