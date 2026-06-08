from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import pytest

from lightfee.core.domain import (
    OrderFill,
    OrderRequest,
    PassiveOrderProgress,
    PassiveOrderState,
    PositionSnapshot,
    Side,
    Venue,
)
from lightfee.engine.close_executor import CloseExecutor
from lightfee.engine.passive_close import PassiveCloseExecutor
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
from tests.test_live_entry_hedge_root_fix import _FakeVenueAdapter, _make_open_runtime


pytestmark = pytest.mark.live_harness


def _open_journal() -> Journal:
    path = Path(tempfile.mkdtemp()) / "events.jsonl"
    journal = Journal(path)
    journal.open()
    return journal


def _position(
    *,
    venue: Venue,
    symbol: str,
    side: Side,
    quantity: float,
    entry_price: float = 0.0,
    observed_at_ms: int = 1780033200000,
) -> PositionSnapshot:
    return PositionSnapshot(
        venue=venue,
        symbol=symbol,
        side=side,
        quantity=quantity,
        entry_price=entry_price,
        observed_at_ms=observed_at_ms,
    )


class _LiveOneSidedAdapter:
    def __init__(
        self,
        venue: Venue,
        position: PositionSnapshot | None,
        *,
        passive_progress: PassiveOrderProgress | None = None,
    ):
        self._venue = venue
        self.position = position
        self.passive_progress = passive_progress
        self.place_order_calls: list[OrderRequest] = []
        self.fetch_position_calls: list[str] = []
        self.fetch_market_snapshot_calls: list[list[str]] = []

    @property
    def venue(self) -> Venue:
        return self._venue

    async def fetch_position(self, symbol: str) -> PositionSnapshot | None:
        self.fetch_position_calls.append(symbol)
        return self.position

    async def fetch_open_orders(self, symbol: str) -> list[dict[str, Any]]:
        return []

    async def fetch_market_snapshot(self, symbols: list[str]) -> Any:
        self.fetch_market_snapshot_calls.append(symbols)
        raise RuntimeError("ticker unavailable")

    async def query_passive_order_progress(
        self,
        symbol: str,
        order_id: str,
        client_order_id: str,
        side: Side,
    ) -> PassiveOrderProgress | None:
        return self.passive_progress

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        return quantity

    async def place_order(self, request: OrderRequest) -> OrderFill:
        self.place_order_calls.append(request)
        self.position = _position(
            venue=self._venue,
            symbol=request.symbol,
            side=request.side.opposite(),
            quantity=0.0,
        )
        return OrderFill(
            venue=request.venue,
            symbol=request.symbol,
            side=request.side,
            quantity=request.quantity,
            price=0.0,
            order_id="jct-compensate-fill",
            filled_at_ms=1780033201000,
        )


@pytest.mark.asyncio
async def test_jctusdt_terminal_maker_missing_price_compensates_in_real_drive_path():
    journal = _open_journal()
    bybit = _LiveOneSidedAdapter(
        Venue.BYBIT,
        _position(
            venue=Venue.BYBIT,
            symbol="JCTUSDT",
            side=Side.BUY,
            quantity=5900.0,
            entry_price=0.0,
        ),
    )
    binance = _LiveOneSidedAdapter(
        Venue.BINANCE,
        _position(
            venue=Venue.BINANCE,
            symbol="JCTUSDT",
            side=Side.SELL,
            quantity=0.0,
            entry_price=0.0,
        ),
        passive_progress=PassiveOrderProgress(
            venue=Venue.BINANCE,
            symbol="JCTUSDT",
            side=Side.BUY,
            order_id="short-maker-filled",
            client_order_id="short-maker-cid",
            cumulative_quantity=5900.0,
            average_price=0.004026,
            fee_quote=0.0,
            last_fill_time_ms=1780033200000,
            state=PassiveOrderState.FILLED,
            observed_at_ms=1780033200000,
        ),
    )
    adapters = {Venue.BYBIT: bybit, Venue.BINANCE: binance}
    executor = PassiveCloseExecutor(adapters, journal)
    executor.set_close_executor(CloseExecutor(adapters, journal))
    executor.set_l2_mid_resolver(lambda _venue, _symbol: 0.0)

    state = EngineState()
    position = OpenPosition(
        position_id="entry-1779998121561-JCTUSDT",
        symbol="JCTUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.BINANCE,
        long_quantity=5900.0,
        short_quantity=5900.0,
        long_entry_price=0.004024,
        short_entry_price=0.0040263149153,
        opened_at_ms=1779998236737,
        matched_quantity=5900.0,
    )
    pending = PendingPassiveClose(
        position_id=position.position_id,
        reason="funding_capture",
        position_snapshot=position,
        target_quantity=5900.0,
        chunk_quantities=[5900.0],
        phase_state=PassivePhaseState(
            phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
            active_maker_leg=ActiveMakerLeg.SHORT,
            maker_order_id="short-maker-filled",
            maker_client_order_id="short-maker-cid",
            maker_resting_limit_price=0.004026,
        ),
        maker_fill=PendingPassiveLegFill(quantity=0.0),
        hedge_fill=PendingPassiveLegFill(quantity=0.0),
    )
    state.open_positions[position.position_id] = position
    state.pending_passive_closes[position.position_id] = pending

    result = await executor.drive_pending_passive_close(
        state,
        position.position_id,
        wait_until_terminal=False,
    )

    assert result is True
    assert len(bybit.place_order_calls) == 1
    close_req = bybit.place_order_calls[0]
    assert close_req.side == Side.SELL
    assert close_req.quantity == pytest.approx(5900.0)
    assert close_req.reduce_only is True
    assert close_req.price is None
    assert position.position_id not in state.open_positions
    assert position.position_id not in state.pending_passive_closes

    events = journal.read_all()
    kinds = [event["kind"] for event in events]
    assert "exit.passive_close_hedge_dust_aborted" not in kinds
    assert "execution.min_notional_abort_and_flatten" not in kinds
    assert "exit.compensated" not in kinds
    assert "exit.passive_close_hedge_filled" in kinds
    assert "exit.passive_close_resolved" in kinds
    assert pending.next_retry_at_ms == 0


@pytest.mark.asyncio
async def test_jctusdt_live_one_sided_missing_price_compensates_instead_of_retrying():
    journal = _open_journal()
    bybit = _LiveOneSidedAdapter(
        Venue.BYBIT,
        _position(
            venue=Venue.BYBIT,
            symbol="JCTUSDT",
            side=Side.BUY,
            quantity=5900.0,
            entry_price=0.0,
        ),
    )
    binance = _LiveOneSidedAdapter(
        Venue.BINANCE,
        _position(
            venue=Venue.BINANCE,
            symbol="JCTUSDT",
            side=Side.SELL,
            quantity=0.0,
            entry_price=0.0,
        ),
    )
    adapters = {Venue.BYBIT: bybit, Venue.BINANCE: binance}
    executor = PassiveCloseExecutor(adapters, journal)
    executor.set_close_executor(CloseExecutor(adapters, journal))
    executor.set_l2_mid_resolver(lambda _venue, _symbol: 0.0)

    state = EngineState()
    position = OpenPosition(
        position_id="entry-1779998121561-JCTUSDT",
        symbol="JCTUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.BINANCE,
        long_quantity=5900.0,
        short_quantity=5900.0,
        long_entry_price=0.004024,
        short_entry_price=0.0040263149153,
        opened_at_ms=1779998236737,
        matched_quantity=5900.0,
    )
    pending = PendingPassiveClose(
        position_id=position.position_id,
        reason="funding_capture",
        position_snapshot=position,
        target_quantity=5900.0,
        chunk_quantities=[5900.0],
        phase_state=PassivePhaseState(
            phase=PassiveExecutionPhase.DUAL_TAKER,
            active_maker_leg=ActiveMakerLeg.LONG,
        ),
        maker_fill=PendingPassiveLegFill(quantity=5900.0, average_price=0.004024),
        hedge_fill=PendingPassiveLegFill(quantity=0.0),
    )
    state.open_positions[position.position_id] = position
    state.pending_passive_closes[position.position_id] = pending

    result = await executor._fallback_to_aggressive_close(state, pending, position)

    assert result is True
    assert len(bybit.place_order_calls) == 1
    close_req = bybit.place_order_calls[0]
    assert close_req.side == Side.SELL
    assert close_req.quantity == pytest.approx(5900.0)
    assert close_req.reduce_only is True
    assert close_req.price is None
    assert position.position_id not in state.open_positions
    assert position.position_id not in state.pending_passive_closes

    events = journal.read_all()
    kinds = [event["kind"] for event in events]
    assert "exit.passive_close_hedge_dust_aborted" not in kinds
    assert "exit.compensated" not in kinds
    assert "exit.passive_close_live_one_sided_flatten" in kinds
    assert "exit.passive_close_fallback_terminal_flat" in kinds
    assert pending.next_retry_at_ms == 0


@pytest.mark.asyncio
async def test_partiusdt_exhausted_paused_live_nonzero_residual_repairs_when_tradeable(
    tmp_path,
):
    runtime = _make_open_runtime(tmp_path)
    now_ms = 1780033750007
    runtime.state.live_recovery_reduce_only_pairs.append({
        "pair_id": "partiusdt:okx->binance",
        "symbol": "PARTIUSDT",
    })
    runtime.state.pending_residual_repairs.append({
        "position_id": "entry-1780012335422-PARTIUSDT",
        "pair_id": "partiusdt:okx->binance",
        "symbol": "PARTIUSDT",
        "origin": "entry_open",
        "repair_venue": "okx",
        "repair_side": "sell",
        "repair_quantity": 50.0,
        "created_at_ms": 1780012377743,
        "deadline_ms": 1780012407743,
        "retry_count": 3,
        "attempt_count": 3,
        "last_attempt_at_ms": now_ms - 5000,
        "next_attempt_ms": 0,
        "last_error": "residual_repair_live_position_nonzero",
        "local_entry_paused": True,
    })

    okx = _FakeVenueAdapter(Venue.OKX)
    okx.position = _position(
        venue=Venue.OKX,
        symbol="PARTIUSDT",
        side=Side.SELL,
        quantity=50.0,
        entry_price=0.04726,
        observed_at_ms=now_ms,
    )
    okx.place_order_fill = OrderFill(
        venue=Venue.OKX,
        symbol="PARTIUSDT",
        side=Side.BUY,
        quantity=50.0,
        price=0.04726,
        order_id="parti-repair-fill",
        filled_at_ms=now_ms,
    )
    binance = _FakeVenueAdapter(Venue.BINANCE)
    binance.position = _position(
        venue=Venue.BINANCE,
        symbol="PARTIUSDT",
        side=Side.SELL,
        quantity=0.0,
        entry_price=0.0,
        observed_at_ms=now_ms,
    )
    runtime._venue_adapters = {Venue.OKX: okx, Venue.BINANCE: binance}

    await runtime._recover_residual_repairs(now_ms)

    assert len(okx._place_order_calls) == 1
    req = okx._place_order_calls[0]
    assert req.venue == Venue.OKX
    assert req.side == Side.BUY
    assert req.quantity == pytest.approx(50.0)
    assert req.reduce_only is True
    assert req.time_in_force is not None
    assert runtime.state.pending_residual_repairs == []
    assert runtime.state.live_recovery_reduce_only_pairs == []

    events = runtime.journal.read_all()
    kinds = [event["kind"] for event in events]
    assert "execution.residual_repair_side_rebuilt_from_live_truth" in kinds
    assert "execution.residual_repair_paused" not in kinds
    assert "execution.residual_repair_completed" in kinds


@pytest.mark.asyncio
async def test_partiusdt_resumed_residual_submit_failure_respects_backoff(tmp_path):
    runtime = _make_open_runtime(tmp_path)
    now_ms = 1780033750007
    runtime.state.live_recovery_reduce_only_pairs.append({
        "pair_id": "partiusdt:okx->binance",
        "symbol": "PARTIUSDT",
    })
    runtime.state.pending_residual_repairs.append({
        "position_id": "entry-1780012335422-PARTIUSDT",
        "pair_id": "partiusdt:okx->binance",
        "symbol": "PARTIUSDT",
        "origin": "entry_open",
        "repair_venue": "okx",
        "repair_side": "sell",
        "repair_quantity": 50.0,
        "created_at_ms": 1780012377743,
        "deadline_ms": 1780012407743,
        "retry_count": 3,
        "attempt_count": 3,
        "last_attempt_at_ms": now_ms - 5000,
        "next_attempt_ms": 0,
        "last_error": "residual_repair_live_position_nonzero",
        "local_entry_paused": True,
    })

    okx = _FakeVenueAdapter(Venue.OKX)
    okx.position = _position(
        venue=Venue.OKX,
        symbol="PARTIUSDT",
        side=Side.SELL,
        quantity=50.0,
        entry_price=0.04726,
        observed_at_ms=now_ms,
    )
    okx.place_order_raises = RuntimeError("exchange temporarily unavailable")
    binance = _FakeVenueAdapter(Venue.BINANCE)
    binance.position = _position(
        venue=Venue.BINANCE,
        symbol="PARTIUSDT",
        side=Side.SELL,
        quantity=0.0,
        entry_price=0.0,
        observed_at_ms=now_ms,
    )
    runtime._venue_adapters = {Venue.OKX: okx, Venue.BINANCE: binance}

    await runtime._recover_residual_repairs(now_ms)

    assert len(okx._place_order_calls) == 1
    task = runtime.state.pending_residual_repairs[0]
    assert task["local_entry_paused"] is True
    assert task["last_error"] == "exchange temporarily unavailable"
    assert task["next_attempt_ms"] > now_ms

    await runtime._recover_residual_repairs(now_ms + 1)

    assert len(okx._place_order_calls) == 1
