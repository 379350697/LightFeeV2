from __future__ import annotations

from pathlib import Path

import pytest

from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig, StrategyConfig
from lightfee.core.domain import (
    OrderFill,
    OrderFillReconciliation,
    OrderRequest,
    PositionSnapshot,
    Side,
    TimeInForce,
    Venue,
)
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.engine.passive_close import PassiveCloseExecutor
from lightfee.engine.runtime import LiveRuntime
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


def _open_journal(path: Path) -> Journal:
    journal = Journal(path)
    journal.open()
    return journal


class _RecoveredFlatAdapter:
    def __init__(self, venue: Venue, symbol: str) -> None:
        self.venue = venue
        self.symbol = symbol
        self.fetch_position_calls: list[str] = []
        self.place_order_calls: list[OrderRequest] = []
        self.submit_passive_order_calls: list[OrderRequest] = []

    async def fetch_open_orders(self, symbol: str) -> list[dict]:
        return []

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        self.fetch_position_calls.append(symbol)
        return PositionSnapshot(
            venue=self.venue,
            symbol=self.symbol,
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1779803978233,
        )

    async def fetch_all_positions(self) -> list[PositionSnapshot]:
        return []

    async def place_order(self, request: OrderRequest) -> OrderFill:
        self.place_order_calls.append(request)
        return OrderFill(
            venue=request.venue,
            symbol=request.symbol,
            side=request.side,
            quantity=0.0,
            price=0.0,
        )

    async def submit_passive_order(self, request: OrderRequest):
        self.submit_passive_order_calls.append(request)
        raise AssertionError("recovered live-flat harness must not submit passive orders")

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        return quantity

    def price_tick_size(self, symbol: str | None = None) -> float:
        return 0.0001


def _beatusdt_recovered_state() -> tuple[EngineState, OpenPosition]:
    state = EngineState()
    position = OpenPosition(
        position_id="live-recovered:BEATUSDT:okx->bybit",
        symbol="BEATUSDT",
        long_venue=Venue.OKX,
        short_venue=Venue.BYBIT,
        long_quantity=6000.0,
        short_quantity=6000.0,
        long_entry_price=0.002,
        short_entry_price=0.002,
        opened_at_ms=1779803978233,
        matched_quantity=6000.0,
    )
    pending = PendingPassiveClose(
        position_id=position.position_id,
        reason="funding_capture",
        position_snapshot=position,
        target_quantity=6000.0,
        chunk_quantities=[6000.0],
        active_chunk_index=0,
        phase_state=PassivePhaseState(
            phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
            active_maker_leg=ActiveMakerLeg.LONG,
        ),
        maker_fill=PendingPassiveLegFill(quantity=6000.0, average_price=0.002),
        hedge_fill=PendingPassiveLegFill(quantity=0.0),
        next_retry_at_ms=0,
    )
    state.open_positions[position.position_id] = position
    state.pending_passive_closes[position.position_id] = pending
    state.recovery_blocked_reason = "startup_recovery_pending_work_without_open_positions"
    state.recovery_blocked_at_ms = 1779803978000
    return state, position


@pytest.mark.asyncio
async def test_beatusdt_recovered_passive_close_live_flat_clears_before_orders(tmp_path):
    journal = _open_journal(tmp_path / "beat-events.jsonl")
    okx = _RecoveredFlatAdapter(Venue.OKX, "BEATUSDT")
    bybit = _RecoveredFlatAdapter(Venue.BYBIT, "BEATUSDT")
    executor = PassiveCloseExecutor({Venue.OKX: okx, Venue.BYBIT: bybit}, journal)
    executor.set_l2_mid_resolver(lambda venue, symbol: 0.0)
    state, position = _beatusdt_recovered_state()

    remaining = await executor.process_pending_passive_closes(state, now_ms=1779803978233)

    assert remaining == set()
    assert position.position_id not in state.open_positions
    assert position.position_id not in state.pending_passive_closes
    assert state.recovery_blocked_reason is None
    assert state.recovery_blocked_at_ms == 0
    assert okx.place_order_calls == []
    assert bybit.place_order_calls == []
    assert okx.submit_passive_order_calls == []
    assert bybit.submit_passive_order_calls == []

    events = journal.read_all()
    kinds = [event["kind"] for event in events]
    assert "exit.passive_close_recovery_probe_flat" in kinds
    assert "recovery.flat" in kinds
    assert "runtime.position_drift_corrected" in kinds
    assert "runtime.stale_recovery_block_cleared" in kinds
    assert "order.submit_attempt" not in kinds
    assert "exit.passive_close_hedge_dust_aborted" not in kinds


class _BybitDuplicateOldFillLiveNonzeroAdapter:
    def __init__(self) -> None:
        self.venue = Venue.BYBIT
        self.fetch_position_calls: list[str] = []
        self.fill_reconcile_calls: list[tuple[str, str, str | None]] = []
        self.place_order_calls: list[OrderRequest] = []

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        self.fetch_position_calls.append(symbol)
        return PositionSnapshot(
            venue=Venue.BYBIT,
            symbol="BIOUSDT",
            side=Side.BUY,
            quantity=1444.0,
            entry_price=0.03321,
            observed_at_ms=1779803978233 + len(self.fetch_position_calls),
        )

    async def fetch_order_fill_reconciliation(
        self,
        symbol: str,
        order_id: str,
        client_order_id: str | None = None,
    ) -> OrderFillReconciliation:
        self.fill_reconcile_calls.append((symbol, order_id, client_order_id))
        return OrderFillReconciliation(
            venue=Venue.BYBIT,
            symbol="BIOUSDT",
            side=Side.SELL,
            quantity=1444.0,
            average_price=0.03320,
            order_id="old-filled-duplicate-order",
            client_order_id=client_order_id,
            filled_at_ms=1779803977000,
        )

    async def place_order(self, request: OrderRequest) -> OrderFill:
        self.place_order_calls.append(request)
        if len(self.place_order_calls) == 1:
            raise OrderSubmitError(
                SubmitFailureClass.REJECTED,
                "bybit order failed: bybit retCode=110072 retMsg=OrderLinkedID is duplicate",
            )
        return OrderFill(
            venue=request.venue,
            symbol=request.symbol,
            side=request.side,
            quantity=request.quantity,
            price=0.03320,
            order_id="fresh-reduce-only-order",
            client_order_id=request.client_order_id,
            filled_at_ms=1779803979000,
        )

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        return quantity

    def passive_metadata(self, symbol: str) -> dict:
        return {
            "min_notional": 0.0,
            "price_tick": 0.00001,
            "quantity_step": 1.0,
            "max_quantity": 0.0,
        }


def _live_runtime(tmp_path) -> LiveRuntime:
    config = AppConfig(
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "runtime-events.jsonl"),
            snapshot_path=str(tmp_path / "runtime-state.json"),
        ),
        runtime=RuntimeConfig(mode="paper"),
        strategy=StrategyConfig(),
    )
    runtime = LiveRuntime(config)
    runtime.journal.open()
    return runtime


@pytest.mark.asyncio
async def test_biousdt_bybit_duplicate_old_fill_live_nonzero_retries_fresh_cid(tmp_path):
    runtime = _live_runtime(tmp_path)
    adapter = _BybitDuplicateOldFillLiveNonzeroAdapter()
    runtime._venue_adapters[Venue.BYBIT] = adapter

    result = await runtime._cleanup_failed_leg_exposure(
        Venue.BYBIT,
        "BIOUSDT",
        "live-recovery:probe:BIOUSDT:bybit",
        "live_recovery_mismatch",
    )

    assert result is True
    assert len(adapter.place_order_calls) == 2
    duplicate_attempt, fresh_attempt = adapter.place_order_calls
    assert duplicate_attempt.client_order_id != fresh_attempt.client_order_id
    assert fresh_attempt.reduce_only is True
    assert fresh_attempt.time_in_force == TimeInForce.IOC
    assert fresh_attempt.side == Side.SELL
    assert fresh_attempt.quantity == pytest.approx(1444.0)

    events = runtime.journal.read_all()
    reconcile_payload = [
        event["payload"]
        for event in events
        if event["kind"] == "entry.cleanup_duplicate_client_order_reconcile_result"
    ][-1]
    assert reconcile_payload["classification"] == "stale_full_live_nonzero"
    assert reconcile_payload["decision"] == "retry_new_client_order_id"
    assert reconcile_payload["live_qty"] == pytest.approx(1444.0)
    assert reconcile_payload["retry_qty"] == pytest.approx(1444.0)
    assert reconcile_payload["order_id"] == "old-filled-duplicate-order"
    assert not any(
        event["kind"] == "recovery.live_mismatch_flattened"
        for event in events
    )
