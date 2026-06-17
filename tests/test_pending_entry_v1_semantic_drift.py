from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace

import pytest

from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig, StrategyConfig
from lightfee.core.domain import (
    OrderFillReconciliation,
    PositionSnapshot,
    Side,
    TimeInForce,
    Venue,
)
from lightfee.engine.recovery_ledger import RecoveryLedger
from lightfee.engine.recovery_owner_index import RecoveryOwnerIndex
from lightfee.engine.reconciliation import OrderReconciler, PositionReconciliationResult
from lightfee.engine.runtime import LiveRuntime
from lightfee.engine.state import OpenPosition, PendingEntry
from lightfee.marketdata.resilience import ConnectionHealth
from lightfee.persistence.journal import Journal
from lightfee.persistence.snapshot_store import SnapshotStore
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from tests.fake_adapters import FakeVenueAdapter, make_fake_fill, make_uncertain_error


@pytest.fixture
def tmp_journal(tmp_path):
    journal = Journal(str(tmp_path / "events.jsonl"))
    journal.open()
    yield journal
    journal.close()


@pytest.fixture
def config(tmp_path):
    return AppConfig(
        runtime=RuntimeConfig(poll_interval_ms=1000),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "snapshot.json"),
        ),
        strategy=StrategyConfig(local_l2_enabled=False),
    )


def _mark_live(config):
    config.runtime.mode = "live"
    return config


@dataclass
class _CapturingReconciler:
    result: PositionReconciliationResult
    calls: list[dict] = field(default_factory=list)

    async def reconcile_position(self, **kwargs):
        self.calls.append(kwargs)
        return self.result

    def drain_order_diagnostics(self):
        return []


class _EvidenceAdapter:
    def __init__(self, venue: Venue, *, position_qty: float, diagnostics: list[dict]):
        self.venue = venue
        self.position_qty = position_qty
        self._diagnostics = diagnostics

    async def fetch_order_fill_reconciliation(self, symbol, order_id="", client_order_id=""):
        return None

    async def fetch_position(self, symbol):
        return PositionSnapshot(
            venue=self.venue,
            symbol=symbol,
            side=Side.BUY,
            quantity=self.position_qty,
            entry_price=0.0,
            observed_at_ms=1234,
        )

    def drain_order_diagnostics(self):
        events = self._diagnostics
        self._diagnostics = []
        return events


class _NoFillReconciliationAdapter:
    async def fetch_order_fill_reconciliation(self, symbol, order_id="", client_order_id=""):
        return None

    async def shutdown(self):
        return None

    def drain_order_diagnostics(self):
        return []


class _LivePositionAdapter(_NoFillReconciliationAdapter):
    def __init__(self, position: PositionSnapshot):
        self.position = position

    async def fetch_position(self, symbol):
        return self.position


class _LivePositionOpenOrdersAdapter(_LivePositionAdapter):
    async def fetch_open_orders(self, symbol):
        return []


class _OwnedConflictCleanupAdapter(FakeVenueAdapter):
    async def fetch_open_orders(self, symbol):
        return []


class _ZeroFillOwnedConflictCleanupAdapter(_OwnedConflictCleanupAdapter):
    async def fetch_order_fill_reconciliation(self, symbol, order_id="", client_order_id=""):
        return OrderFillReconciliation(
            venue=self.venue,
            symbol=symbol,
            side=Side.BUY,
            quantity=0.0,
            average_price=0.0,
            order_id=order_id,
            client_order_id=client_order_id,
            metadata={
                "response_classification": "detail_found;fills_empty",
                "queried_endpoints": ["/api/v5/trade/order", "/api/v5/trade/fills"],
            },
        )


class _TerminalNoFillOpenMakerAdapter(_NoFillReconciliationAdapter):
    def __init__(self, *, order_id: str, client_order_id: str):
        self.order_id = order_id
        self.client_order_id = client_order_id
        self.open_order_calls: list[str] = []

    async def fetch_order_fill_reconciliation(self, symbol, order_id="", client_order_id=""):
        return OrderFillReconciliation(
            venue=Venue.BYBIT,
            symbol=symbol,
            side=Side.BUY,
            quantity=0.0,
            average_price=0.0,
            order_id=order_id,
            client_order_id=client_order_id,
            metadata={"status": "canceled"},
        )

    async def fetch_open_orders(self, symbol):
        self.open_order_calls.append(symbol)
        return [
            {
                "orderId": self.order_id,
                "orderLinkId": self.client_order_id,
                "symbol": symbol,
                "side": "Buy",
                "reduceOnly": False,
            }
        ]


class _TerminalNoFillOpenMakerFlatPositionAdapter(_TerminalNoFillOpenMakerAdapter):
    async def fetch_position(self, symbol):
        return PositionSnapshot(
            venue=Venue.BYBIT,
            symbol=symbol,
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1780584323000,
        )


class _TerminalNoFillUnavailableOpenOrdersAdapter(_NoFillReconciliationAdapter):
    def __init__(self, *, venue: Venue = Venue.BYBIT, error: str = "open order timeout"):
        self.venue = venue
        self.error = error
        self.open_order_calls: list[str] = []
        self.position_calls: list[str] = []

    async def fetch_order_fill_reconciliation(self, symbol, order_id="", client_order_id=""):
        return OrderFillReconciliation(
            venue=self.venue,
            symbol=symbol,
            side=Side.BUY,
            quantity=0.0,
            average_price=0.0,
            order_id=order_id,
            client_order_id=client_order_id,
            metadata={"status": "canceled"},
        )

    async def fetch_open_orders(self, symbol):
        self.open_order_calls.append(symbol)
        raise TimeoutError(self.error)

    async def fetch_position(self, symbol):
        self.position_calls.append(symbol)
        return PositionSnapshot(
            venue=self.venue,
            symbol=symbol,
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1780584320000,
        )


class _TerminalNoFillClearOpenOrdersFlatPositionAdapter(_NoFillReconciliationAdapter):
    def __init__(self, *, venue: Venue = Venue.BYBIT):
        self.venue = venue
        self.open_order_calls: list[str] = []
        self.position_calls: list[str] = []

    async def fetch_order_fill_reconciliation(self, symbol, order_id="", client_order_id=""):
        return OrderFillReconciliation(
            venue=self.venue,
            symbol=symbol,
            side=Side.BUY,
            quantity=0.0,
            average_price=0.0,
            order_id=order_id,
            client_order_id=client_order_id,
            metadata={"status": "canceled"},
        )

    async def fetch_open_orders(self, symbol):
        self.open_order_calls.append(symbol)
        return []

    async def fetch_position(self, symbol):
        self.position_calls.append(symbol)
        return PositionSnapshot(
            venue=self.venue,
            symbol=symbol,
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1780584325000,
        )


class _CancelableUnavailableOpenOrdersAdapter(_TerminalNoFillUnavailableOpenOrdersAdapter):
    def __init__(self):
        super().__init__(error="open order truth unavailable before cancel")
        self.cancel_calls: list[dict] = []

    async def cancel_passive_order(self, symbol, order_id="", client_order_id=None):
        self.cancel_calls.append({
            "symbol": symbol,
            "order_id": order_id,
            "client_order_id": client_order_id,
        })


class _TerminalNoFillLivePositionAdapter(_NoFillReconciliationAdapter):
    def __init__(self, position: PositionSnapshot):
        self.position = position

    async def fetch_order_fill_reconciliation(self, symbol, order_id="", client_order_id=""):
        return OrderFillReconciliation(
            venue=self.position.venue,
            symbol=symbol,
            side=self.position.side,
            quantity=0.0,
            average_price=0.0,
            order_id=order_id,
            client_order_id=client_order_id,
            metadata={"status": "canceled"},
        )

    async def fetch_open_orders(self, symbol):
        return []

    async def fetch_position(self, symbol):
        return self.position


class _NormalizingAdapter(_NoFillReconciliationAdapter):
    def __init__(self, *, normalized_quantity: float):
        self.normalized_quantity = normalized_quantity

    async def normalize_quantity(self, symbol, quantity):
        return self.normalized_quantity


class _RecordingFillAdapter(_NoFillReconciliationAdapter):
    def __init__(self):
        self.fill_reconciliation_calls: list[dict] = []

    async def fetch_order_fill_reconciliation(self, symbol, order_id="", client_order_id=""):
        self.fill_reconciliation_calls.append({
            "symbol": symbol,
            "order_id": order_id,
            "client_order_id": client_order_id,
        })
        return None


class _CloseLegFillAdapter(_NoFillReconciliationAdapter):
    def __init__(self, venue: Venue, fills: dict[tuple[str, str], OrderFillReconciliation]):
        self.venue = venue
        self.fills = fills
        self.fill_reconciliation_calls: list[dict] = []

    async def fetch_order_fill_reconciliation(self, symbol, order_id="", client_order_id=""):
        self.fill_reconciliation_calls.append({
            "symbol": symbol,
            "order_id": order_id,
            "client_order_id": client_order_id,
        })
        return self.fills.get((order_id, client_order_id))

    async def fetch_position(self, symbol):
        return PositionSnapshot(
            venue=self.venue,
            symbol=symbol,
            side=Side.BUY if self.venue == Venue.OKX else Side.SELL,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=3000,
        )


class _UnavailableCloseLegAdapter(_CloseLegFillAdapter):
    def __init__(self, venue: Venue, *, live_quantity: float):
        super().__init__(venue, {})
        self.live_quantity = live_quantity
        self.position_calls: list[str] = []

    async def fetch_position(self, symbol):
        self.position_calls.append(symbol)
        return PositionSnapshot(
            venue=self.venue,
            symbol=symbol,
            side=Side.BUY if self.venue == Venue.OKX else Side.SELL,
            quantity=self.live_quantity,
            entry_price=0.0,
            observed_at_ms=3000,
        )


class _PrivateConfirmedCloseLegAdapter(_CloseLegFillAdapter):
    def __init__(
        self,
        venue: Venue,
        fills: dict[tuple[str, str], OrderFillReconciliation],
        *,
        cached_position: PositionSnapshot,
    ):
        super().__init__(venue, fills)
        self._cached_position = cached_position
        self.supports_private_health = True

    def private_ws_worker_count(self):
        return 1

    def cached_private_connection_health(self):
        return ConnectionHealth()

    def cached_position(self, symbol):
        if symbol == self._cached_position.symbol:
            return self._cached_position
        return None


class _MetadataAdapter(_NoFillReconciliationAdapter):
    def __init__(self, venue: Venue, metadata: dict, *, venue_symbol: str | None = None):
        self.venue = venue
        self.venue_symbol = venue_symbol
        self._transport = SimpleNamespace(
            _symbol_metadata=metadata,
            _spec=SimpleNamespace(
                venue_id=venue,
                contract_size=1.0,
                quantity_step=0.001,
                min_notional=0.0,
            ),
        )

    def passive_metadata(self, symbol):
        return {
            "min_notional": 0.0,
            "quantity_step": 0.001,
            "price_tick": 0.0001,
        }

    def _venue_symbol(self, symbol: str) -> str:
        return self.venue_symbol or symbol


def _runtime(config, tmp_journal, reconciler):
    runtime = LiveRuntime(
        config,
        venue_adapters={Venue.BINANCE: object(), Venue.BYBIT: object()},
    )
    runtime.journal = tmp_journal
    runtime.reconciler = reconciler
    return runtime


def _open_position(**overrides) -> OpenPosition:
    values = {
        "position_id": "active-pos",
        "symbol": "ARIAUSDT",
        "long_venue": Venue.OKX,
        "short_venue": Venue.BYBIT,
        "long_quantity": 5.0,
        "short_quantity": 5.0,
        "long_entry_price": 1.0,
        "short_entry_price": 1.01,
        "opened_at_ms": 500,
    }
    values.update(overrides)
    return OpenPosition(**values)


def _pending_close_reconciliation(**overrides) -> dict:
    values = {
        "position_id": "entry-force-reconcile",
        "symbol": "BEATUSDT",
        "kind": "final",
        "reason": "funding_capture",
        "closed_at_ms": 1000,
        "created_cycle": 1,
        "next_attempt_ms": 1000,
        "attempt_count": 0,
        "position_snapshot": {
            "position_id": "entry-force-reconcile",
            "symbol": "BEATUSDT",
            "long_venue": Venue.OKX.value,
            "short_venue": Venue.BYBIT.value,
            "long_entry_price": 1.0,
            "short_entry_price": 1.03,
        },
        "long_legs": [{
            "venue": Venue.OKX.value,
            "order_id": "okx-close-order",
            "client_order_id": "okx-close-cid",
        }],
        "short_legs": [{
            "venue": Venue.BYBIT.value,
            "order_id": "bybit-close-order",
            "client_order_id": "bybit-close-cid",
        }],
    }
    values.update(overrides)
    return values


def _pending_entry(**overrides):
    values = dict(
        pending_id="entry-v1-drift",
        symbol="MUBARAKUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.BYBIT,
        target_quantity=1758.0,
        long_side=Side.BUY,
        short_side=Side.SELL,
        created_at_ms=1000,
        uncertain_outcome=True,
        maker_order_id="maker-order",
        maker_client_order_id="maker-cid",
        hedge_client_order_id="planned-hedge-cid",
        maker_leg="long",
        outcome="maker_resting",
    )
    values.update(overrides)
    return PendingEntry(**values)


@pytest.mark.asyncio
async def test_flat_reconcile_with_not_found_maker_retains_pending_entry_like_v1(
    config, tmp_journal,
):
    result = PositionReconciliationResult(
        position_id="entry-v1-drift",
        symbol="MUBARAKUSDT",
        long_status="not_found",
        short_status="not_found",
        long_position=PositionSnapshot(
            venue=Venue.BINANCE,
            symbol="MUBARAKUSDT",
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1100,
        ),
        short_position=PositionSnapshot(
            venue=Venue.BYBIT,
            symbol="MUBARAKUSDT",
            side=Side.SELL,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1100,
        ),
        is_flat=True,
    )
    runtime = _runtime(config, tmp_journal, _CapturingReconciler(result))
    pending = _pending_entry()
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._reconcile_pending_state(now_ms=2000)

    assert pending.pending_id in runtime.state.pending_entries
    kinds = [event["kind"] for event in tmp_journal.read_all()]
    assert "reconciliation.entry_flat_unresolved_maker_retained" in kinds
    assert "reconciliation.entry_cleared_flat" not in kinds


@pytest.mark.asyncio
async def test_flat_reconcile_not_found_maker_with_zero_fill_and_live_flat_clears_pending(
    config, tmp_journal,
):
    _mark_live(config)
    maker = _TerminalNoFillClearOpenOrdersFlatPositionAdapter(venue=Venue.BYBIT)
    hedge = _TerminalNoFillClearOpenOrdersFlatPositionAdapter(venue=Venue.HYPERLIQUID)
    result = PositionReconciliationResult(
        position_id="entry-v1-drift",
        symbol="JTOUSDT",
        long_status="not_found",
        short_status="not_found",
        long_position=PositionSnapshot(
            venue=Venue.BYBIT,
            symbol="JTOUSDT",
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1780584326000,
        ),
        short_position=PositionSnapshot(
            venue=Venue.HYPERLIQUID,
            symbol="JTOUSDT",
            side=Side.SELL,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1780584326000,
        ),
        is_flat=True,
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={Venue.BYBIT: maker, Venue.HYPERLIQUID: hedge},
    )
    runtime.journal = tmp_journal
    runtime.reconciler = _CapturingReconciler(result)
    pending = _pending_entry(
        pending_id="entry-v1-drift",
        symbol="JTOUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.HYPERLIQUID,
        created_at_ms=1780584325900,
        maker_order_id="jto-maker-order",
        maker_client_order_id="jto-maker-client",
        maker_leg="long",
        maker_leg_filled=0.0,
        hedge_leg_filled=0.0,
        outcome="maker_resting",
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._reconcile_pending_state(now_ms=1780584326000)

    assert pending.pending_id not in runtime.state.pending_entries
    assert maker.open_order_calls == ["JTOUSDT"]
    assert maker.position_calls == ["JTOUSDT"]
    assert result.long_position.quantity == 0.0
    assert result.short_position.quantity == 0.0
    kinds = [event["kind"] for event in tmp_journal.read_all()]
    assert "reconciliation.entry_flat_not_found_terminal_cleared" in kinds
    assert "reconciliation.entry_flat_unresolved_maker_retained" not in kinds
    assert "entry.passive_unfilled" in kinds


@pytest.mark.asyncio
async def test_live_flat_reconcile_without_maker_order_reference_retains_pending(
    config, tmp_journal,
):
    _mark_live(config)
    result = PositionReconciliationResult(
        position_id="entry-v1-drift",
        symbol="JTOUSDT",
        long_status="not_found",
        short_status="not_found",
        long_position=PositionSnapshot(
            venue=Venue.BYBIT,
            symbol="JTOUSDT",
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1780584326000,
        ),
        short_position=PositionSnapshot(
            venue=Venue.HYPERLIQUID,
            symbol="JTOUSDT",
            side=Side.SELL,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1780584326000,
        ),
        is_flat=True,
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={Venue.BYBIT: object(), Venue.HYPERLIQUID: object()},
    )
    runtime.journal = tmp_journal
    runtime.reconciler = _CapturingReconciler(result)
    pending = _pending_entry(
        pending_id="entry-v1-drift",
        symbol="JTOUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.HYPERLIQUID,
        created_at_ms=1780584325000,
        maker_order_id="",
        maker_client_order_id="",
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._reconcile_pending_state(now_ms=1780584326000)

    assert pending.pending_id in runtime.state.pending_entries
    kinds = [event["kind"] for event in tmp_journal.read_all()]
    assert "reconciliation.entry_flat_unresolved_maker_retained" in kinds
    assert "reconciliation.entry_cleared_flat" not in kinds


@pytest.mark.asyncio
async def test_pending_close_reconciliation_uses_snapshot_after_lifecycle_flat(
    config, tmp_journal,
):
    _mark_live(config)
    long_adapter = _CloseLegFillAdapter(Venue.OKX, {
        ("okx-close-order", "okx-close-cid"): OrderFillReconciliation(
            venue=Venue.OKX,
            symbol="BEATUSDT",
            side=Side.SELL,
            quantity=20.0,
            average_price=1.0100,
            order_id="okx-close-order",
            client_order_id="okx-close-cid",
            fee_quote=0.02,
            filled_at_ms=2000,
        ),
    })
    short_adapter = _CloseLegFillAdapter(Venue.BYBIT, {
        ("bybit-force-order", "bybit-force-cid"): OrderFillReconciliation(
            venue=Venue.BYBIT,
            symbol="BEATUSDT",
            side=Side.BUY,
            quantity=20.0,
            average_price=1.0200,
            order_id="bybit-force-order",
            client_order_id="bybit-force-cid",
            fee_quote=0.03,
            filled_at_ms=2001,
        ),
    })
    runtime = LiveRuntime(config, venue_adapters={
        Venue.OKX: long_adapter,
        Venue.BYBIT: short_adapter,
    })
    runtime.journal = tmp_journal
    runtime.reconciler = _CapturingReconciler(
        PositionReconciliationResult(position_id="entry-force-reconcile", symbol="BEATUSDT")
    )
    runtime.state.pending_close_reconciliations.append({
        "position_id": "entry-force-reconcile",
        "symbol": "BEATUSDT",
        "kind": "final",
        "reason": "funding_capture",
        "closed_at_ms": 1000,
        "position_snapshot": {
            "position_id": "entry-force-reconcile",
            "symbol": "BEATUSDT",
            "long_venue": Venue.OKX.value,
            "short_venue": Venue.BYBIT.value,
            "matched_quantity": 20.0,
            "long_entry_price": 1.0,
            "short_entry_price": 1.03,
        },
        "long_legs": [{
            "venue": Venue.OKX.value,
            "order_id": "okx-close-order",
            "client_order_id": "okx-close-cid",
            "quantity": 20.0,
            "average_price": 0.0,
            "fee_quote": 0.0,
        }],
        "short_legs": [{
            "venue": Venue.BYBIT.value,
            "order_id": "bybit-force-order",
            "client_order_id": "bybit-force-cid",
            "quantity": 20.0,
            "average_price": 0.0,
            "fee_quote": 0.0,
        }],
    })

    await runtime._reconcile_pending_state(now_ms=3000)

    assert runtime.state.pending_close_reconciliations == []
    assert long_adapter.fill_reconciliation_calls == [{
        "symbol": "BEATUSDT",
        "order_id": "okx-close-order",
        "client_order_id": "okx-close-cid",
    }]
    assert short_adapter.fill_reconciliation_calls == [{
        "symbol": "BEATUSDT",
        "order_id": "bybit-force-order",
        "client_order_id": "bybit-force-cid",
    }]
    records = tmp_journal.read_all()
    kinds = [record["kind"] for record in records]
    assert "exit.reconciled" in kinds
    assert "reconciliation.pending_close_orphaned" not in kinds


@pytest.mark.asyncio
async def test_pending_close_reconciliation_is_live_only(config, tmp_journal):
    long_adapter = _CloseLegFillAdapter(Venue.OKX, {
        ("okx-close-order", "okx-close-cid"): OrderFillReconciliation(
            venue=Venue.OKX,
            symbol="BEATUSDT",
            side=Side.SELL,
            quantity=20.0,
            average_price=1.01,
            order_id="okx-close-order",
            client_order_id="okx-close-cid",
        ),
    })
    short_adapter = _CloseLegFillAdapter(Venue.BYBIT, {
        ("bybit-close-order", "bybit-close-cid"): OrderFillReconciliation(
            venue=Venue.BYBIT,
            symbol="BEATUSDT",
            side=Side.BUY,
            quantity=20.0,
            average_price=1.02,
            order_id="bybit-close-order",
            client_order_id="bybit-close-cid",
        ),
    })
    runtime = LiveRuntime(config, venue_adapters={
        Venue.OKX: long_adapter,
        Venue.BYBIT: short_adapter,
    })
    runtime.journal = tmp_journal
    runtime.reconciler = _CapturingReconciler(
        PositionReconciliationResult(position_id="entry-paper-reconcile", symbol="BEATUSDT")
    )
    runtime.state.pending_close_reconciliations.append({
        "position_id": "entry-paper-reconcile",
        "symbol": "BEATUSDT",
        "kind": "final",
        "closed_at_ms": 1000,
        "created_cycle": 0,
        "next_attempt_ms": 0,
        "position_snapshot": {
            "position_id": "entry-paper-reconcile",
            "symbol": "BEATUSDT",
            "long_venue": Venue.OKX.value,
            "short_venue": Venue.BYBIT.value,
        },
        "long_legs": [{
            "venue": Venue.OKX.value,
            "order_id": "okx-close-order",
            "client_order_id": "okx-close-cid",
        }],
        "short_legs": [{
            "venue": Venue.BYBIT.value,
            "order_id": "bybit-close-order",
            "client_order_id": "bybit-close-cid",
        }],
    })

    await runtime._reconcile_pending_state(now_ms=3000)

    assert len(runtime.state.pending_close_reconciliations) == 1
    assert long_adapter.fill_reconciliation_calls == []
    assert short_adapter.fill_reconciliation_calls == []
    assert "exit.reconciled" not in [record["kind"] for record in tmp_journal.read_all()]


@pytest.mark.asyncio
async def test_pending_close_reconciliation_waits_until_next_live_cycle(config, tmp_journal):
    _mark_live(config)
    long_adapter = _CloseLegFillAdapter(Venue.OKX, {
        ("okx-close-order", "okx-close-cid"): OrderFillReconciliation(
            venue=Venue.OKX,
            symbol="BEATUSDT",
            side=Side.SELL,
            quantity=20.0,
            average_price=1.01,
            order_id="okx-close-order",
            client_order_id="okx-close-cid",
        ),
    })
    short_adapter = _CloseLegFillAdapter(Venue.BYBIT, {
        ("bybit-close-order", "bybit-close-cid"): OrderFillReconciliation(
            venue=Venue.BYBIT,
            symbol="BEATUSDT",
            side=Side.BUY,
            quantity=20.0,
            average_price=1.02,
            order_id="bybit-close-order",
            client_order_id="bybit-close-cid",
        ),
    })
    runtime = LiveRuntime(config, venue_adapters={
        Venue.OKX: long_adapter,
        Venue.BYBIT: short_adapter,
    })
    runtime.journal = tmp_journal
    runtime.reconciler = _CapturingReconciler(
        PositionReconciliationResult(position_id="entry-next-cycle", symbol="BEATUSDT")
    )
    runtime.state.tick_count = 7
    runtime.state.pending_close_reconciliations.append({
        "position_id": "entry-next-cycle",
        "symbol": "BEATUSDT",
        "kind": "final",
        "closed_at_ms": 1000,
        "created_cycle": 7,
        "next_attempt_ms": 1000,
        "position_snapshot": {
            "position_id": "entry-next-cycle",
            "symbol": "BEATUSDT",
            "long_venue": Venue.OKX.value,
            "short_venue": Venue.BYBIT.value,
        },
        "long_legs": [{
            "venue": Venue.OKX.value,
            "order_id": "okx-close-order",
            "client_order_id": "okx-close-cid",
        }],
        "short_legs": [{
            "venue": Venue.BYBIT.value,
            "order_id": "bybit-close-order",
            "client_order_id": "bybit-close-cid",
        }],
    })

    await runtime._reconcile_pending_state(now_ms=3000)

    assert len(runtime.state.pending_close_reconciliations) == 1
    assert long_adapter.fill_reconciliation_calls == []
    assert short_adapter.fill_reconciliation_calls == []

    runtime.state.tick_count = 8
    await runtime._reconcile_pending_state(now_ms=3000)

    assert runtime.state.pending_close_reconciliations == []
    assert long_adapter.fill_reconciliation_calls == [{
        "symbol": "BEATUSDT",
        "order_id": "okx-close-order",
        "client_order_id": "okx-close-cid",
    }]
    assert short_adapter.fill_reconciliation_calls == [{
        "symbol": "BEATUSDT",
        "order_id": "bybit-close-order",
        "client_order_id": "bybit-close-cid",
    }]


@pytest.mark.asyncio
async def test_pending_close_reconciliation_drain_restores_running_lifecycle(
    config, tmp_journal,
):
    _mark_live(config)
    long_adapter = _CloseLegFillAdapter(Venue.OKX, {
        ("okx-close-order", "okx-close-cid"): OrderFillReconciliation(
            venue=Venue.OKX,
            symbol="BEATUSDT",
            side=Side.SELL,
            quantity=20.0,
            average_price=1.01,
            order_id="okx-close-order",
            client_order_id="okx-close-cid",
        ),
    })
    short_adapter = _CloseLegFillAdapter(Venue.BYBIT, {
        ("bybit-close-order", "bybit-close-cid"): OrderFillReconciliation(
            venue=Venue.BYBIT,
            symbol="BEATUSDT",
            side=Side.BUY,
            quantity=20.0,
            average_price=1.02,
            order_id="bybit-close-order",
            client_order_id="bybit-close-cid",
        ),
    })
    runtime = LiveRuntime(config, venue_adapters={
        Venue.OKX: long_adapter,
        Venue.BYBIT: short_adapter,
    })
    runtime.journal = tmp_journal
    runtime.reconciler = _CapturingReconciler(
        PositionReconciliationResult(position_id="entry-drained", symbol="BEATUSDT")
    )
    runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    runtime.state.last_error = "pending_close_reconciliations_active"
    runtime.state.tick_count = 2
    runtime.state.pending_close_reconciliations.append({
        "position_id": "entry-drained",
        "symbol": "BEATUSDT",
        "kind": "final",
        "closed_at_ms": 1000,
        "created_cycle": 1,
        "next_attempt_ms": 1000,
        "position_snapshot": {
            "position_id": "entry-drained",
            "symbol": "BEATUSDT",
            "long_venue": Venue.OKX.value,
            "short_venue": Venue.BYBIT.value,
        },
        "long_legs": [{
            "venue": Venue.OKX.value,
            "order_id": "okx-close-order",
            "client_order_id": "okx-close-cid",
        }],
        "short_legs": [{
            "venue": Venue.BYBIT.value,
            "order_id": "bybit-close-order",
            "client_order_id": "bybit-close-cid",
        }],
    })

    await runtime._reconcile_pending_state(now_ms=3000)

    assert runtime.state.pending_close_reconciliations == []
    assert runtime.state.lifecycle == EngineLifecycle.RUNNING
    assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
    assert runtime.state.last_error is None


@pytest.mark.asyncio
async def test_pending_close_reconciliation_abandons_final_when_flat_but_fill_unavailable(
    config, tmp_journal,
):
    _mark_live(config)
    long_adapter = _UnavailableCloseLegAdapter(Venue.OKX, live_quantity=0.0)
    short_adapter = _UnavailableCloseLegAdapter(Venue.BYBIT, live_quantity=0.0)
    runtime = LiveRuntime(config, venue_adapters={
        Venue.OKX: long_adapter,
        Venue.BYBIT: short_adapter,
    })
    runtime.journal = tmp_journal
    runtime.reconciler = None
    runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    runtime.state.last_error = "pending_close_reconciliations_active"
    runtime.state.tick_count = 2
    runtime.state.pending_close_reconciliations.append(_pending_close_reconciliation())

    await runtime._reconcile_pending_state(now_ms=3000)

    assert runtime.state.pending_close_reconciliations == []
    assert long_adapter.position_calls == ["BEATUSDT"]
    assert short_adapter.position_calls == ["BEATUSDT"]
    records = tmp_journal.read_all()
    abandoned = [
        record["payload"] for record in records
        if record["kind"] == "exit.reconciliation_abandoned"
    ][0]
    assert abandoned["terminal_reason"] == "fill_reconciliation_unavailable_after_terminal_budget"
    assert abandoned["attempt_count"] == 1
    assert abandoned["long_live_size"] == pytest.approx(0.0)
    assert abandoned["short_live_size"] == pytest.approx(0.0)
    assert runtime.state.lifecycle == EngineLifecycle.RUNNING
    assert runtime.state.last_error is None
    allowed, reason = runtime._gate_pending_close_reconciliation(
        SimpleNamespace(symbol="BEATUSDT", long_venue="okx", short_venue="bybit")
    )
    assert allowed is True
    assert reason == ""


@pytest.mark.asyncio
async def test_pending_close_reconciliation_processor_normalizes_dict_shaped_queue(
    config, tmp_journal,
):
    _mark_live(config)
    runtime = _runtime(config, tmp_journal, _CapturingReconciler(
        PositionReconciliationResult(position_id="entry-1780771924982-BABYUSDT", symbol="BABYUSDT")
    ))
    runtime.state.tick_count = 2
    runtime.state.pending_close_reconciliations = {
        "entry-1780771924982-BABYUSDT": {
            "position_id": "entry-1780771924982-BABYUSDT",
            "symbol": "BABYUSDT",
            "kind": "final",
            "closed_at_ms": 1780771929000,
            "created_cycle": 1,
            "next_attempt_ms": 1000,
            "position_snapshot": {
                "position_id": "entry-1780771924982-BABYUSDT",
                "symbol": "BABYUSDT",
                "long_venue": Venue.OKX.value,
                "short_venue": Venue.BYBIT.value,
            },
            "long_legs": [],
            "short_legs": [],
        }
    }

    await runtime._process_pending_close_reconciliations(now_ms=3000)

    assert isinstance(runtime.state.pending_close_reconciliations, list)
    assert all(
        isinstance(item, dict)
        for item in runtime.state.pending_close_reconciliations
    )
    assert [
        record["kind"] for record in tmp_journal.read_all()
    ].count("reconciliation.pending_close_reconciliation_invalid") == 1


@pytest.mark.asyncio
async def test_pending_close_reconciliation_processor_retains_invalid_item_with_evidence(
    config, tmp_journal,
):
    _mark_live(config)
    runtime = _runtime(config, tmp_journal, _CapturingReconciler(
        PositionReconciliationResult(position_id="entry-invalid", symbol="BABYUSDT")
    ))
    runtime.state.tick_count = 2
    runtime.state.pending_close_reconciliations = [
        "poisoned-pending-close-reconciliation"
    ]

    await runtime._process_pending_close_reconciliations(now_ms=3000)

    assert len(runtime.state.pending_close_reconciliations) == 1
    invalid = runtime.state.pending_close_reconciliations[0]
    assert invalid["invalid_pending_close_reconciliation"] is True
    assert invalid["raw_type"] == "str"
    assert [
        record["kind"] for record in tmp_journal.read_all()
    ].count("reconciliation.pending_close_reconciliation_invalid") == 1


@pytest.mark.asyncio
async def test_pending_close_reconciliation_retains_when_terminal_live_size_nonzero(
    config, tmp_journal,
):
    _mark_live(config)
    long_adapter = _UnavailableCloseLegAdapter(Venue.OKX, live_quantity=3.0)
    short_adapter = _UnavailableCloseLegAdapter(Venue.BYBIT, live_quantity=0.0)
    runtime = LiveRuntime(config, venue_adapters={
        Venue.OKX: long_adapter,
        Venue.BYBIT: short_adapter,
    })
    runtime.journal = tmp_journal
    runtime.reconciler = None
    runtime.state.tick_count = 2
    runtime.state.pending_close_reconciliations.append(_pending_close_reconciliation())

    await runtime._reconcile_pending_state(now_ms=3000)

    assert len(runtime.state.pending_close_reconciliations) == 1
    retained = runtime.state.pending_close_reconciliations[0]
    assert retained["attempt_count"] == 1
    assert retained["next_attempt_ms"] > 3000
    assert long_adapter.position_calls == ["BEATUSDT"]
    assert short_adapter.position_calls == ["BEATUSDT"]
    assert "exit.reconciliation_abandoned" not in [
        record["kind"] for record in tmp_journal.read_all()
    ]


@pytest.mark.asyncio
async def test_pending_close_reconciliation_drain_preserves_existing_reduce_only_risk_mode(
    config, tmp_journal,
):
    _mark_live(config)
    long_adapter = _CloseLegFillAdapter(Venue.OKX, {
        ("okx-close-order", "okx-close-cid"): OrderFillReconciliation(
            venue=Venue.OKX,
            symbol="BEATUSDT",
            side=Side.SELL,
            quantity=20.0,
            average_price=1.01,
            order_id="okx-close-order",
            client_order_id="okx-close-cid",
        ),
    })
    short_adapter = _CloseLegFillAdapter(Venue.BYBIT, {
        ("bybit-close-order", "bybit-close-cid"): OrderFillReconciliation(
            venue=Venue.BYBIT,
            symbol="BEATUSDT",
            side=Side.BUY,
            quantity=20.0,
            average_price=1.02,
            order_id="bybit-close-order",
            client_order_id="bybit-close-cid",
        ),
    })
    runtime = LiveRuntime(config, venue_adapters={
        Venue.OKX: long_adapter,
        Venue.BYBIT: short_adapter,
    })
    runtime.journal = tmp_journal
    runtime.reconciler = None
    runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
    runtime.state.risk_mode = GlobalRiskMode.REDUCE_ONLY
    runtime.state.last_error = "pending_close_reconciliations_active"
    runtime.state.tick_count = 2
    runtime.state.pending_close_reconciliations.append(_pending_close_reconciliation())

    await runtime._reconcile_pending_state(now_ms=3000)

    assert runtime.state.pending_close_reconciliations == []
    assert runtime.state.lifecycle == EngineLifecycle.RUNNING
    assert runtime.state.risk_mode == GlobalRiskMode.REDUCE_ONLY
    assert runtime.state.last_error is None


@pytest.mark.asyncio
async def test_pending_close_reconciliation_background_recovers_with_confirmed_open_positions(
    config, tmp_journal,
):
    _mark_live(config)
    active = _open_position()
    long_adapter = _PrivateConfirmedCloseLegAdapter(
        Venue.OKX,
        {
            ("okx-close-order", "okx-close-cid"): OrderFillReconciliation(
                venue=Venue.OKX,
                symbol="BEATUSDT",
                side=Side.SELL,
                quantity=20.0,
                average_price=1.01,
                order_id="okx-close-order",
                client_order_id="okx-close-cid",
            ),
        },
        cached_position=PositionSnapshot(
            venue=Venue.OKX,
            symbol=active.symbol,
            side=Side.BUY,
            quantity=active.long_quantity,
            entry_price=active.long_entry_price,
            observed_at_ms=3000,
        ),
    )
    short_adapter = _PrivateConfirmedCloseLegAdapter(
        Venue.BYBIT,
        {
            ("bybit-close-order", "bybit-close-cid"): OrderFillReconciliation(
                venue=Venue.BYBIT,
                symbol="BEATUSDT",
                side=Side.BUY,
                quantity=20.0,
                average_price=1.02,
                order_id="bybit-close-order",
                client_order_id="bybit-close-cid",
            ),
        },
        cached_position=PositionSnapshot(
            venue=Venue.BYBIT,
            symbol=active.symbol,
            side=Side.SELL,
            quantity=active.short_quantity,
            entry_price=active.short_entry_price,
            observed_at_ms=3000,
        ),
    )
    runtime = LiveRuntime(config, venue_adapters={
        Venue.OKX: long_adapter,
        Venue.BYBIT: short_adapter,
    })
    runtime.journal = tmp_journal
    runtime.reconciler = None
    runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    runtime.state.last_error = "pending_close_reconciliations_active"
    runtime.state.tick_count = 2
    runtime.state.open_positions[active.position_id] = active
    runtime.state.pending_close_reconciliations.append(_pending_close_reconciliation())

    await runtime._reconcile_pending_state(now_ms=3000)

    assert runtime.state.pending_close_reconciliations == []
    assert runtime.state.lifecycle == EngineLifecycle.RUNNING
    assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
    assert runtime.state.last_error is None


def test_pending_close_reconciliation_gate_uses_v1_snapshot_work(config, tmp_journal):
    runtime = _runtime(config, tmp_journal, _CapturingReconciler(
        PositionReconciliationResult(position_id="pos-close-gate", symbol="BEATUSDT")
    ))
    runtime.state.pending_close_reconciliations.append({
        "position_id": "pos-close-gate",
        "symbol": "BEATUSDT",
        "position_snapshot": {
            "symbol": "BEATUSDT",
            "long_venue": Venue.OKX.value,
            "short_venue": Venue.BYBIT.value,
        },
        "long_legs": [],
        "short_legs": [],
    })

    allowed, reason = runtime._gate_pending_close_reconciliation(
        SimpleNamespace(symbol="BEATUSDT", long_venue="okx", short_venue="bybit")
    )
    reversed_allowed, reversed_reason = runtime._gate_pending_close_reconciliation(
        SimpleNamespace(symbol="BEATUSDT", long_venue="bybit", short_venue="okx")
    )

    assert allowed is False
    assert reason == "pending_close_reconciliation_conflict"
    assert reversed_allowed is False
    assert reversed_reason == "pending_close_reconciliation_conflict"


def test_pending_close_reconciliation_gate_normalizes_dict_shaped_queue(config, tmp_journal):
    runtime = _runtime(config, tmp_journal, _CapturingReconciler(
        PositionReconciliationResult(position_id="pos-close-gate", symbol="BEATUSDT")
    ))
    runtime.state.pending_close_reconciliations = {
        "pos-close-gate": {
            "position_id": "pos-close-gate",
            "symbol": "BEATUSDT",
            "position_snapshot": {
                "symbol": "BEATUSDT",
                "long_venue": Venue.OKX.value,
                "short_venue": Venue.BYBIT.value,
            },
            "long_legs": [],
            "short_legs": [],
        }
    }

    allowed, reason = runtime._gate_pending_close_reconciliation(
        SimpleNamespace(symbol="BEATUSDT", long_venue="okx", short_venue="bybit")
    )

    assert allowed is False
    assert reason == "pending_close_reconciliation_conflict"


def test_pending_close_reconciliation_gate_blocks_malformed_task_with_top_level_evidence(config, tmp_journal):
    runtime = _runtime(config, tmp_journal, _CapturingReconciler(
        PositionReconciliationResult(position_id="pos-close-gate", symbol="BEATUSDT")
    ))
    runtime.state.pending_close_reconciliations = {
        "position_id": "pos-close-gate",
        "symbol": "BEATUSDT",
        "long_venue": Venue.OKX.value,
        "short_venue": Venue.BYBIT.value,
        "kind": "final",
        "closed_at_ms": 1780771929000,
    }

    allowed, reason = runtime._gate_pending_close_reconciliation(
        SimpleNamespace(symbol="BEATUSDT", long_venue="okx", short_venue="bybit")
    )

    assert allowed is False
    assert reason == "pending_close_reconciliation_conflict"
    assert isinstance(runtime.state.pending_close_reconciliations, list)


@pytest.mark.asyncio
async def test_pending_close_reconciliation_fetches_all_v1_leg_records(
    config, tmp_journal,
):
    _mark_live(config)
    long_adapter = _CloseLegFillAdapter(Venue.OKX, {
        ("okx-close-1", "okx-cid-1"): OrderFillReconciliation(
            venue=Venue.OKX,
            symbol="BEATUSDT",
            side=Side.SELL,
            quantity=7.0,
            average_price=1.01,
            order_id="okx-close-1",
            client_order_id="okx-cid-1",
            fee_quote=0.01,
        ),
        ("okx-close-2", "okx-cid-2"): OrderFillReconciliation(
            venue=Venue.OKX,
            symbol="BEATUSDT",
            side=Side.SELL,
            quantity=13.0,
            average_price=1.02,
            order_id="okx-close-2",
            client_order_id="okx-cid-2",
            fee_quote=0.02,
        ),
    })
    short_adapter = _CloseLegFillAdapter(Venue.BYBIT, {
        ("bybit-close-1", "bybit-cid-1"): OrderFillReconciliation(
            venue=Venue.BYBIT,
            symbol="BEATUSDT",
            side=Side.BUY,
            quantity=8.0,
            average_price=1.03,
            order_id="bybit-close-1",
            client_order_id="bybit-cid-1",
            fee_quote=0.03,
        ),
        ("bybit-close-2", "bybit-cid-2"): OrderFillReconciliation(
            venue=Venue.BYBIT,
            symbol="BEATUSDT",
            side=Side.BUY,
            quantity=12.0,
            average_price=1.04,
            order_id="bybit-close-2",
            client_order_id="bybit-cid-2",
            fee_quote=0.04,
        ),
    })
    runtime = LiveRuntime(config, venue_adapters={
        Venue.OKX: long_adapter,
        Venue.BYBIT: short_adapter,
    })
    runtime.journal = tmp_journal
    runtime.reconciler = _CapturingReconciler(
        PositionReconciliationResult(position_id="entry-force-reconcile", symbol="BEATUSDT")
    )
    runtime.state.pending_close_reconciliations.append({
        "position_id": "entry-force-reconcile",
        "symbol": "BEATUSDT",
        "kind": "final",
        "reason": "funding_capture",
        "closed_at_ms": 1000,
        "position_snapshot": {
            "position_id": "entry-force-reconcile",
            "symbol": "BEATUSDT",
            "long_venue": Venue.OKX.value,
            "short_venue": Venue.BYBIT.value,
            "long_entry_price": 1.0,
            "short_entry_price": 1.05,
            "captured_funding_quote": 0.5,
            "total_entry_fee_quote": 0.1,
        },
        "long_legs": [
            {"venue": Venue.OKX.value, "order_id": "okx-close-1", "client_order_id": "okx-cid-1"},
            {"venue": Venue.OKX.value, "order_id": "okx-close-2", "client_order_id": "okx-cid-2"},
        ],
        "short_legs": [
            {"venue": Venue.BYBIT.value, "order_id": "bybit-close-1", "client_order_id": "bybit-cid-1"},
            {"venue": Venue.BYBIT.value, "order_id": "bybit-close-2", "client_order_id": "bybit-cid-2"},
        ],
    })

    await runtime._reconcile_pending_state(now_ms=3000)

    assert runtime.state.pending_close_reconciliations == []
    assert long_adapter.fill_reconciliation_calls == [
        {"symbol": "BEATUSDT", "order_id": "okx-close-1", "client_order_id": "okx-cid-1"},
        {"symbol": "BEATUSDT", "order_id": "okx-close-2", "client_order_id": "okx-cid-2"},
    ]
    assert short_adapter.fill_reconciliation_calls == [
        {"symbol": "BEATUSDT", "order_id": "bybit-close-1", "client_order_id": "bybit-cid-1"},
        {"symbol": "BEATUSDT", "order_id": "bybit-close-2", "client_order_id": "bybit-cid-2"},
    ]
    reconciled = [
        record for record in tmp_journal.read_all()
        if record["kind"] == "exit.reconciled"
    ][0]["payload"]
    assert reconciled["long_closed_qty"] == pytest.approx(20.0)
    assert reconciled["short_closed_qty"] == pytest.approx(20.0)
    assert reconciled["exit_fee_quote"] == pytest.approx(0.10)
    assert reconciled["venue_statement_reconciled"] is True
    assert reconciled["evidence_gap"] is False
    assert reconciled["long_legs"][1]["order_id"] == "okx-close-2"
    assert reconciled["short_legs"][1]["order_id"] == "bybit-close-2"


@pytest.mark.asyncio
async def test_pending_close_reconciliation_does_not_depend_on_entry_reconciler(
    config, tmp_journal,
):
    _mark_live(config)
    long_adapter = _CloseLegFillAdapter(Venue.OKX, {
        ("okx-close-order", "okx-close-cid"): OrderFillReconciliation(
            venue=Venue.OKX,
            symbol="BEATUSDT",
            side=Side.SELL,
            quantity=20.0,
            average_price=1.01,
            order_id="okx-close-order",
            client_order_id="okx-close-cid",
        ),
    })
    short_adapter = _CloseLegFillAdapter(Venue.BYBIT, {
        ("bybit-force-order", "bybit-force-cid"): OrderFillReconciliation(
            venue=Venue.BYBIT,
            symbol="BEATUSDT",
            side=Side.BUY,
            quantity=20.0,
            average_price=1.02,
            order_id="bybit-force-order",
            client_order_id="bybit-force-cid",
        ),
    })
    runtime = LiveRuntime(config, venue_adapters={
        Venue.OKX: long_adapter,
        Venue.BYBIT: short_adapter,
    })
    runtime.journal = tmp_journal
    runtime.reconciler = None
    runtime.state.pending_close_reconciliations.append({
        "position_id": "entry-force-reconcile",
        "symbol": "BEATUSDT",
        "kind": "final",
        "closed_at_ms": 1000,
        "position_snapshot": {
            "position_id": "entry-force-reconcile",
            "symbol": "BEATUSDT",
            "long_venue": Venue.OKX.value,
            "short_venue": Venue.BYBIT.value,
            "long_entry_price": 1.0,
            "short_entry_price": 1.03,
        },
        "long_legs": [{
            "venue": Venue.OKX.value,
            "order_id": "okx-close-order",
            "client_order_id": "okx-close-cid",
        }],
        "short_legs": [{
            "venue": Venue.BYBIT.value,
            "order_id": "bybit-force-order",
            "client_order_id": "bybit-force-cid",
        }],
    })

    await runtime._reconcile_pending_state(now_ms=3000)

    assert runtime.state.pending_close_reconciliations == []
    assert "exit.reconciled" in [record["kind"] for record in tmp_journal.read_all()]


@pytest.mark.asyncio
async def test_pending_entry_does_not_query_planned_hedge_cid_before_submit(
    config, tmp_journal,
):
    result = PositionReconciliationResult(
        position_id="entry-v1-drift",
        symbol="BEATUSDT",
        long_status="uncertain",
        short_status="uncertain",
        long_position=PositionSnapshot(
            venue=Venue.BINANCE,
            symbol="BEATUSDT",
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1100,
        ),
        short_position=PositionSnapshot(
            venue=Venue.BYBIT,
            symbol="BEATUSDT",
            side=Side.SELL,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1100,
        ),
        is_flat=True,
    )
    reconciler = _CapturingReconciler(result)
    runtime = _runtime(config, tmp_journal, reconciler)
    pending = _pending_entry(symbol="BEATUSDT")
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._reconcile_pending_state(now_ms=2000)

    assert reconciler.calls[-1]["short_client_order_id"] == ""


@pytest.mark.asyncio
async def test_pending_entry_reconcile_uses_stored_hedge_cid_when_recovery_placeholder_exists(
    config, tmp_journal,
):
    result = PositionReconciliationResult(
        position_id="entry-v1-drift",
        symbol="BEATUSDT",
        long_status="uncertain",
        short_status="uncertain",
        long_position=PositionSnapshot(
            venue=Venue.BYBIT,
            symbol="BEATUSDT",
            side=Side.BUY,
            quantity=296.0,
            entry_price=0.08084,
            observed_at_ms=1100,
        ),
        short_position=PositionSnapshot(
            venue=Venue.BINANCE,
            symbol="BEATUSDT",
            side=Side.SELL,
            quantity=296.0,
            entry_price=0.08067,
            observed_at_ms=1100,
        ),
        is_flat=False,
    )
    reconciler = _CapturingReconciler(result)
    runtime = _runtime(config, tmp_journal, reconciler)
    pending = _pending_entry(
        pending_id="entry-1781671924167-ESPORTSUSDT",
        symbol="ESPORTSUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.BINANCE,
        maker_order_id="56d57705-6106-4a45-9687-3b8c2b5dca43",
        maker_client_order_id="299561add0ef7b2872584c65656167a864f7",
        hedge_order_id="entry-1781671924167-esportsusdt-recovery-short",
        hedge_client_order_id="b9f510bc61d65d84bd0ddbfad62e1db0b501",
        maker_leg="long",
        maker_leg_filled=296.0,
        hedge_leg_filled=296.0,
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._reconcile_pending_state(now_ms=2000)

    call = reconciler.calls[-1]
    assert call["short_order_id"] == "entry-1781671924167-esportsusdt-recovery-short"
    assert call["short_client_order_id"] == "b9f510bc61d65d84bd0ddbfad62e1db0b501"


@pytest.mark.asyncio
async def test_finalize_zero_fill_retains_pending_when_maker_open_order_truth_exists(
    config, tmp_journal,
):
    adapter = _TerminalNoFillOpenMakerAdapter(
        order_id="d792a623-d9e4-4c20-905f-f76a8f2efaeb",
        client_order_id="e86085435b3216fade136612525d1917e503",
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={Venue.BYBIT: adapter, Venue.HYPERLIQUID: object()},
    )
    runtime.journal = tmp_journal
    pending = _pending_entry(
        pending_id="entry-1780573948279-SEIUSDT",
        symbol="SEIUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.HYPERLIQUID,
        target_quantity=451.6244366455595,
        maker_order_id="d792a623-d9e4-4c20-905f-f76a8f2efaeb",
        maker_client_order_id="e86085435b3216fade136612525d1917e503",
        maker_leg="long",
        maker_price=0.05315,
        outcome="maker_resting",
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._finalize_pending_entry(
        pending,
        pending.pending_id,
        now_ms=1780573970000,
    )

    assert pending.pending_id in runtime.state.pending_entries
    assert pending.uncertain_outcome is True
    assert pending.reconcile_next_attempt_ms >= 1780573971000
    assert adapter.open_order_calls == ["SEIUSDT"]
    assert pending.maker_leg_filled == 0.0
    assert pending.hedge_leg_filled == 0.0

    events = tmp_journal.read_all()
    kinds = [event["kind"] for event in events]
    assert "pending_entry.finalize_deferred_maker_open_order" in kinds
    assert "entry.passive_unfilled" not in kinds
    assert "pending_entry.pending_entry_finalized" not in kinds


@pytest.mark.asyncio
async def test_finalize_zero_fill_retains_pending_when_open_order_truth_unavailable(
    config, tmp_journal,
):
    adapter = _TerminalNoFillUnavailableOpenOrdersAdapter(
        error="bybit eventual consistency timeout"
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={Venue.BYBIT: adapter, Venue.HYPERLIQUID: object()},
    )
    runtime.journal = tmp_journal
    pending = _pending_entry(
        pending_id="entry-1780584320000-JTOUSDT",
        symbol="JTOUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.HYPERLIQUID,
        target_quantity=31.0,
        maker_order_id="jto-maker-order",
        maker_client_order_id="jto-maker-client",
        maker_leg="long",
        maker_leg_filled=0.0,
        hedge_leg_filled=0.0,
        outcome="maker_resting",
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    finalized = await runtime._finalize_pending_entry(
        pending,
        pending.pending_id,
        now_ms=1780584321700,
    )

    assert finalized is False
    assert pending.pending_id in runtime.state.pending_entries
    assert pending.uncertain_outcome is True
    assert pending.reconcile_next_attempt_ms >= 1780584322700
    assert adapter.open_order_calls == ["JTOUSDT"]
    assert adapter.position_calls == ["JTOUSDT"]

    events = tmp_journal.read_all()
    kinds = [event["kind"] for event in events]
    assert "pending_entry.finalize_maker_open_order_truth_unavailable" in kinds
    assert "pending_entry.terminalizer_decision" in kinds
    decisions = [
        event["payload"]
        for event in events
        if event["kind"] == "pending_entry.terminalizer_decision"
    ]
    assert decisions[-1]["outcome"] == "deferred_missing_live_truth"
    assert decisions[-1]["allows_pending_removal"] is False
    assert "entry.passive_unfilled" not in kinds
    assert "pending_entry.pending_entry_finalized" not in kinds


@pytest.mark.asyncio
async def test_zero_fill_open_order_reappears_as_owned_pending_entry_not_orphan(
    config, tmp_journal,
):
    order_id = "jto-maker-order"
    client_order_id = "jto-maker-client"
    adapter = _TerminalNoFillOpenMakerAdapter(
        order_id=order_id,
        client_order_id=client_order_id,
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={Venue.BYBIT: adapter, Venue.HYPERLIQUID: object()},
    )
    runtime.journal = tmp_journal
    pending = _pending_entry(
        pending_id="entry-1780584320000-JTOUSDT",
        symbol="JTOUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.HYPERLIQUID,
        target_quantity=31.0,
        maker_order_id=order_id,
        maker_client_order_id=client_order_id,
        maker_leg="long",
        maker_leg_filled=0.0,
        hedge_leg_filled=0.0,
        outcome="maker_resting",
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    finalized = await runtime._finalize_pending_entry(
        pending,
        pending.pending_id,
        now_ms=1780584323000,
    )

    assert finalized is False
    assert pending.pending_id in runtime.state.pending_entries

    owner_index = RecoveryOwnerIndex.from_state(
        {
            "pending_entries": [
                {
                    "pending_id": pending.pending_id,
                    "symbol": pending.symbol,
                    "long_venue": pending.long_venue.value,
                    "short_venue": pending.short_venue.value,
                    "maker_order_id": order_id,
                    "maker_client_order_id": client_order_id,
                }
            ]
        }
    )
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={"open_positions": [], "pending_entries": []},
        exchange_truth={
            "truth_available": True,
            "positions": [],
            "open_orders": [
                {
                    "venue": "bybit",
                    "symbol": "JTOUSDT",
                    "side": "buy",
                    "quantity": 31.0,
                    "reduce_only": False,
                    "order_id": order_id,
                    "client_order_id": client_order_id,
                }
            ],
        },
        owner_index=owner_index,
    )

    item = ledger.work_items[0]
    assert item.kind == "owned_pending_entry"
    assert item.owner.owner_id == pending.pending_id
    assert item.owner.confidence == "proven"
    assert item.decision.outcome == "owned_order_cancel_requested"


@pytest.mark.asyncio
async def test_live_zero_fill_open_order_match_routes_to_deferred_live_open_order(
    config, tmp_journal,
):
    _mark_live(config)
    order_id = "jto-maker-order"
    client_order_id = "jto-maker-client"
    adapter = _TerminalNoFillOpenMakerFlatPositionAdapter(
        order_id=order_id,
        client_order_id=client_order_id,
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={Venue.BYBIT: adapter, Venue.HYPERLIQUID: object()},
    )
    runtime.journal = tmp_journal
    pending = _pending_entry(
        pending_id="entry-1780584320000-JTOUSDT",
        symbol="JTOUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.HYPERLIQUID,
        target_quantity=31.0,
        maker_order_id=order_id,
        maker_client_order_id=client_order_id,
        maker_leg="long",
        maker_leg_filled=0.0,
        hedge_leg_filled=0.0,
        outcome="maker_resting",
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    finalized = await runtime._finalize_pending_entry(
        pending,
        pending.pending_id,
        now_ms=1780584323000,
    )

    assert finalized is False
    assert pending.pending_id in runtime.state.pending_entries
    assert pending.uncertain_outcome is True

    events = tmp_journal.read_all()
    kinds = [event["kind"] for event in events]
    assert "pending_entry.finalize_deferred_maker_open_order" in kinds
    decisions = [
        event["payload"]
        for event in events
        if event["kind"] == "pending_entry.terminalizer_decision"
    ]
    assert decisions[-1]["outcome"] == "deferred_live_open_order"
    assert decisions[-1]["allows_pending_removal"] is False
    assert "entry.passive_unfilled" not in kinds
    assert "pending_entry.pending_entry_finalized" not in kinds


@pytest.mark.asyncio
async def test_live_zero_fill_without_maker_order_reference_retains_pending(
    config, tmp_journal,
):
    _mark_live(config)
    adapter = _LivePositionOpenOrdersAdapter(
        PositionSnapshot(
            venue=Venue.BYBIT,
            symbol="JTOUSDT",
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1780584324000,
        )
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={Venue.BYBIT: adapter, Venue.HYPERLIQUID: object()},
    )
    runtime.journal = tmp_journal
    pending = _pending_entry(
        pending_id="entry-1780584320000-JTOUSDT",
        symbol="JTOUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.HYPERLIQUID,
        target_quantity=31.0,
        maker_order_id="",
        maker_client_order_id="",
        maker_leg="long",
        maker_leg_filled=0.0,
        hedge_leg_filled=0.0,
        outcome="maker_resting",
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    finalized = await runtime._finalize_pending_entry(
        pending,
        pending.pending_id,
        now_ms=1780584324000,
    )

    assert finalized is False
    assert pending.pending_id in runtime.state.pending_entries
    assert pending.uncertain_outcome is True
    assert pending.reconcile_next_attempt_ms >= 1780584325000

    events = tmp_journal.read_all()
    kinds = [event["kind"] for event in events]
    assert "pending_entry.finalize_maker_order_reference_unavailable" in kinds
    decisions = [
        event["payload"]
        for event in events
        if event["kind"] == "pending_entry.terminalizer_decision"
    ]
    assert decisions[-1]["outcome"] == "deferred_missing_live_truth"
    assert decisions[-1]["reason"] == "maker_order_reference_unavailable"
    assert decisions[-1]["allows_pending_removal"] is False
    assert "entry.passive_unfilled" not in kinds
    assert "pending_entry.pending_entry_finalized" not in kinds


@pytest.mark.asyncio
async def test_live_stale_abandon_without_maker_order_reference_retains_pending(
    config, tmp_journal,
):
    _mark_live(config)
    maker = _LivePositionOpenOrdersAdapter(
        PositionSnapshot(
            venue=Venue.BYBIT,
            symbol="JTOUSDT",
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1780584327000,
        )
    )
    hedge = _LivePositionAdapter(
        PositionSnapshot(
            venue=Venue.HYPERLIQUID,
            symbol="JTOUSDT",
            side=Side.SELL,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1780584327000,
        )
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={Venue.BYBIT: maker, Venue.HYPERLIQUID: hedge},
    )
    runtime.journal = tmp_journal
    pending = _pending_entry(
        pending_id="entry-1780584320000-JTOUSDT",
        symbol="JTOUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.HYPERLIQUID,
        maker_order_id="",
        maker_client_order_id="",
        maker_leg_filled=0.0,
        hedge_leg_filled=0.0,
    )

    abandoned = await runtime._try_abandon_stale_entry(pending, pending.pending_id)

    assert abandoned is False
    events = tmp_journal.read_all()
    assert any(
        event["kind"] == "pending_entry.maker_terminal_evidence_unavailable"
        and event["payload"].get("reason") == "maker_order_reference_unavailable"
        for event in events
    )
    assert "reconciliation.entry_abandoned_flat" not in [
        event["kind"] for event in events
    ]


@pytest.mark.asyncio
async def test_live_terminal_zero_fill_with_clear_truth_allows_passive_unfilled(
    config, tmp_journal,
):
    _mark_live(config)
    adapter = _TerminalNoFillClearOpenOrdersFlatPositionAdapter()
    runtime = LiveRuntime(
        config,
        venue_adapters={Venue.BYBIT: adapter, Venue.HYPERLIQUID: object()},
    )
    runtime.journal = tmp_journal
    pending = _pending_entry(
        pending_id="entry-1780584320000-JTOUSDT",
        symbol="JTOUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.HYPERLIQUID,
        target_quantity=31.0,
        maker_order_id="jto-maker-order",
        maker_client_order_id="jto-maker-client",
        maker_leg="long",
        maker_leg_filled=0.0,
        hedge_leg_filled=0.0,
        outcome="maker_resting",
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    finalized = await runtime._finalize_pending_entry(
        pending,
        pending.pending_id,
        now_ms=1780584325000,
    )

    assert finalized is True
    assert pending.pending_id not in runtime.state.pending_entries
    assert adapter.open_order_calls == ["JTOUSDT"]
    assert adapter.position_calls == ["JTOUSDT"]

    events = tmp_journal.read_all()
    kinds = [event["kind"] for event in events]
    decisions = [
        event["payload"]
        for event in events
        if event["kind"] == "pending_entry.terminalizer_decision"
    ]
    assert decisions[-1]["outcome"] == "passive_unfilled"
    assert decisions[-1]["allows_pending_removal"] is True
    assert "entry.passive_unfilled" in kinds
    assert "pending_entry.pending_entry_finalized" in kinds


@pytest.mark.asyncio
async def test_deferred_zero_fill_owner_cancel_uses_pending_maker_order_ids(
    config, tmp_journal,
):
    adapter = _CancelableUnavailableOpenOrdersAdapter()
    runtime = LiveRuntime(
        config,
        venue_adapters={Venue.BYBIT: adapter, Venue.HYPERLIQUID: object()},
    )
    runtime.journal = tmp_journal
    pending = _pending_entry(
        pending_id="entry-1780584320000-JTOUSDT",
        symbol="JTOUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.HYPERLIQUID,
        target_quantity=31.0,
        maker_order_id="jto-maker-order",
        maker_client_order_id="jto-maker-client",
        maker_leg="long",
        maker_leg_filled=0.0,
        hedge_leg_filled=0.0,
        outcome="maker_resting",
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    finalized = await runtime._finalize_pending_entry(
        pending,
        pending.pending_id,
        now_ms=1780584321700,
    )

    assert finalized is False
    retained = runtime.state.pending_entries[pending.pending_id]

    canceled = await runtime._recover_cancel_maker_order(
        retained,
        pending.pending_id,
        reason="owned_pending_entry_live_order",
    )

    assert canceled is True
    assert adapter.cancel_calls == [
        {
            "symbol": "JTOUSDT",
            "order_id": "jto-maker-order",
            "client_order_id": "jto-maker-client",
        }
    ]


@pytest.mark.asyncio
async def test_stale_accepted_order_with_momentary_flat_position_stays_uncertain():
    adapter = _EvidenceAdapter(
        Venue.BINANCE,
        position_qty=0.0,
        diagnostics=[
            {
                "kind": "order.reconcile_query",
                "payload": {
                    "venue": "binance",
                    "symbol": "MUBARAKUSDT",
                    "order_id": "2059178915",
                    "client_order_id": "maker-cid",
                    "queried_endpoints": ["/fapi/v1/order"],
                    "endpoint_responses": [
                        {
                            "endpoint": "/fapi/v1/order",
                            "classification": "stale_accepted_order",
                        }
                    ],
                    "response_classification": "stale_accepted_order",
                    "uncertain_subtype": "stale_accepted_order",
                    "next_action": "check_live_position",
                },
            }
        ],
    )
    reconciler = OrderReconciler({Venue.BINANCE: adapter})

    result = await reconciler.reconcile_position(
        position_id="entry-v1-drift",
        symbol="MUBARAKUSDT",
        long_venue=Venue.BINANCE,
        long_order_id="2059178915",
        long_client_order_id="maker-cid",
    )

    assert result.long_status == "uncertain"
    events = reconciler.drain_order_diagnostics()
    payload = [event["payload"] for event in events if event["kind"] == "order.reconcile_result"][-1]
    assert payload["uncertain_subtype"] == "stale_accepted_order"
    assert payload["next_action"] != "clear_uncertain_state"
    assert not [event for event in events if event["kind"] == "order.reconcile_resolution"]


@pytest.mark.asyncio
async def test_uncertain_maker_order_live_position_does_not_apply_maker_progress(
    config, tmp_journal,
):
    result = PositionReconciliationResult(
        position_id="entry-me-v1-terminality",
        symbol="MEUSDT",
        long_status="uncertain",
        short_status="not_found",
        long_position=PositionSnapshot(
            venue=Venue.BYBIT,
            symbol="MEUSDT",
            side=Side.BUY,
            quantity=608.0,
            entry_price=0.07895,
            observed_at_ms=3000,
        ),
        short_position=PositionSnapshot(
            venue=Venue.OKX,
            symbol="MEUSDT",
            side=Side.SELL,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=3000,
        ),
        is_flat=False,
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.BYBIT: _NoFillReconciliationAdapter(),
            Venue.OKX: _NoFillReconciliationAdapter(),
        },
    )
    runtime.journal = tmp_journal
    runtime.reconciler = _CapturingReconciler(result)

    async def _do_not_drive_missing_hedge(*args, **kwargs):
        return False

    runtime._drive_missing_hedge_live = _do_not_drive_missing_hedge
    pending = _pending_entry(
        pending_id="entry-me-v1-terminality",
        symbol="MEUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.OKX,
        target_quantity=608.0,
        maker_leg="long",
        maker_order_id="668be726-46b4-4c68-a1ae-4257c10c6661",
        maker_client_order_id="e0da5db734dba297d0b8904aaa39a65fd7a0",
        hedge_order_id="",
        hedge_leg_filled=0.0,
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._reconcile_pending_state(now_ms=4000)

    assert pending.pending_id in runtime.state.pending_entries
    assert pending.maker_leg_filled == 0.0
    assert pending.maker_fill_price == 0.0
    events = tmp_journal.read_all()
    kinds = [event["kind"] for event in events]
    assert "pending_entry.maker_progress_applied" not in kinds
    deferred = [
        event["payload"]
        for event in events
        if event["kind"] == "pending_entry.live_position_progress_deferred"
    ]
    assert deferred[-1]["entry_id"] == "entry-me-v1-terminality"
    assert deferred[-1]["leg"] == "maker"
    assert deferred[-1]["status"] == "uncertain"
    assert deferred[-1]["reason"] == "order_terminality_not_confirmed"


@pytest.mark.asyncio
async def test_zero_fill_finalize_retains_when_live_position_truth_is_nonzero(
    config, tmp_journal,
):
    bybit_position = PositionSnapshot(
        venue=Venue.BYBIT,
        symbol="BIOUSDT",
        side=Side.BUY,
        quantity=2429.0,
        entry_price=0.02963,
        observed_at_ms=1780580073206,
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.BYBIT: _TerminalNoFillLivePositionAdapter(bybit_position),
            Venue.HYPERLIQUID: _NoFillReconciliationAdapter(),
        },
    )
    runtime.journal = tmp_journal
    pending = _pending_entry(
        pending_id="entry-1780577580703-BIOUSDT",
        symbol="BIOUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.HYPERLIQUID,
        target_quantity=810.0,
        maker_leg="long",
        maker_order_id="45e9f91a-bybit-maker",
        maker_client_order_id="70aadf0478bf44bb92de6633497714b8",
        hedge_order_id="",
        hedge_client_order_id="",
        maker_leg_filled=0.0,
        hedge_leg_filled=0.0,
        maker_fill_price=0.0,
        hedge_fill_price=0.0,
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._finalize_pending_entry(pending, pending.pending_id, 1780580073206)

    assert pending.pending_id in runtime.state.pending_entries
    events = tmp_journal.read_all()
    kinds = [event["kind"] for event in events]
    assert "entry.passive_unfilled" not in kinds
    assert "pending_entry.pending_entry_finalized" not in kinds
    deferred = [
        event["payload"]
        for event in events
        if event["kind"] == "pending_entry.finalize_deferred_maker_live_position"
    ]
    assert deferred[-1]["entry_id"] == pending.pending_id
    assert deferred[-1]["live_position_quantity"] == pytest.approx(2429.0)
    assert deferred[-1]["reason"] == "maker_live_position_truth_present"


@pytest.mark.asyncio
async def test_zero_fill_owned_live_single_leg_cleans_before_pending_release(
    config, tmp_journal,
):
    _mark_live(config)
    live_long = PositionSnapshot(
        venue=Venue.OKX,
        symbol="HOMEUSDT",
        side=Side.BUY,
        quantity=1600.0,
        entry_price=0.0288,
        observed_at_ms=1781456020992,
    )
    flat_long = PositionSnapshot(
        venue=Venue.OKX,
        symbol="HOMEUSDT",
        side=Side.BUY,
        quantity=0.0,
        entry_price=0.0,
        observed_at_ms=1781456025992,
    )
    flat_short = PositionSnapshot(
        venue=Venue.BYBIT,
        symbol="HOMEUSDT",
        side=Side.SELL,
        quantity=0.0,
        entry_price=0.0,
        observed_at_ms=1781456025992,
    )
    okx = _ZeroFillOwnedConflictCleanupAdapter(Venue.OKX)
    okx.position_snapshots = [live_long, live_long, live_long, flat_long]
    bybit = _OwnedConflictCleanupAdapter(Venue.BYBIT)
    bybit.position_snapshots = [flat_short]
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: okx,
            Venue.BYBIT: bybit,
        },
    )
    runtime.journal = tmp_journal
    pending = _pending_entry(
        pending_id="entry-1781455987631-HOMEUSDT",
        symbol="HOMEUSDT",
        long_venue=Venue.OKX,
        short_venue=Venue.BYBIT,
        target_quantity=1652.5511258004544,
        maker_leg="long",
        maker_order_id="3655876122055122944",
        maker_client_order_id="f435324a30e9a2b43818f9469e7d9317",
        hedge_order_id="",
        hedge_client_order_id="60bc2ae587d54be1ab29485651ff9d92a234",
        maker_leg_filled=0.0,
        hedge_leg_filled=0.0,
        maker_fill_price=0.0,
        hedge_fill_price=0.0,
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    finalized = await runtime._finalize_pending_entry(
        pending,
        pending.pending_id,
        1781456020992,
    )

    assert finalized is True
    assert pending.pending_id not in runtime.state.pending_entries
    assert okx.place_order_call_count == 1
    assert okx.last_request is not None
    assert okx.last_request.reduce_only is True
    assert okx.last_request.post_only is False
    assert okx.last_request.time_in_force == TimeInForce.IOC
    assert okx.last_request.side == Side.SELL
    assert okx.last_request.quantity == pytest.approx(1600.0)
    events = tmp_journal.read_all()
    kinds = [event["kind"] for event in events]
    assert "entry.opened" not in kinds
    assert "entry.passive_unfilled" not in kinds
    cleanup = [
        event["payload"]
        for event in events
        if event["kind"] == "pending_entry.owned_live_conflict_cleanup_succeeded"
    ][-1]
    assert cleanup["entry_id"] == pending.pending_id
    assert cleanup["venue"] == "okx"
    assert cleanup["live_position_side"] == "buy"
    assert cleanup["live_position_quantity"] == pytest.approx(1600.0)
    assert cleanup["reason"] == "owned_single_leg_flattened_and_fresh_truth_flat"


@pytest.mark.asyncio
async def test_positive_fill_finalize_defers_when_live_truth_is_flat(
    config, tmp_journal,
):
    _mark_live(config)
    flat_long = PositionSnapshot(
        venue=Venue.OKX,
        symbol="HOMEUSDT",
        side=Side.BUY,
        quantity=0.0,
        entry_price=0.0,
        observed_at_ms=1781293940000,
    )
    flat_short = PositionSnapshot(
        venue=Venue.BYBIT,
        symbol="HOMEUSDT",
        side=Side.SELL,
        quantity=0.0,
        entry_price=0.0,
        observed_at_ms=1781293940000,
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: _LivePositionAdapter(flat_long),
            Venue.BYBIT: _LivePositionAdapter(flat_short),
        },
    )
    runtime.journal = tmp_journal
    pending = _pending_entry(
        pending_id="entry-1781293924792-HOMEUSDT",
        symbol="HOMEUSDT",
        long_venue=Venue.OKX,
        short_venue=Venue.BYBIT,
        target_quantity=1500.0,
        maker_leg="long",
        maker_order_id="home-maker-order",
        maker_client_order_id="home-maker-cid",
        hedge_order_id="home-hedge-order",
        hedge_client_order_id="home-hedge-cid",
        maker_leg_filled=1500.0,
        hedge_leg_filled=1500.0,
        maker_fill_price=0.01531,
        hedge_fill_price=0.01529,
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    finalized = await runtime._finalize_pending_entry(
        pending,
        pending.pending_id,
        1781293940000,
    )

    assert finalized is False
    assert pending.pending_id in runtime.state.pending_entries
    assert pending.pending_id not in runtime.state.open_positions
    events = tmp_journal.read_all()
    kinds = [event["kind"] for event in events]
    assert "entry.opened" not in kinds
    assert "pending_entry.pending_entry_finalized" not in kinds
    conflict = [
        event["payload"]
        for event in events
        if event["kind"] == "pending_entry.positive_fill_live_truth_conflict"
    ][-1]
    assert conflict["entry_id"] == pending.pending_id
    assert conflict["symbol"] == "HOMEUSDT"
    assert conflict["reason"] == "positive_fill_conflicts_with_live_flat_truth"


@pytest.mark.asyncio
async def test_positive_fill_finalize_cleans_owned_live_single_leg_before_release(
    config, tmp_journal,
):
    _mark_live(config)
    live_short = PositionSnapshot(
        venue=Venue.BYBIT,
        symbol="HOMEUSDT",
        side=Side.SELL,
        quantity=1600.0,
        entry_price=0.01529,
        observed_at_ms=1781373163000,
    )
    okx = _OwnedConflictCleanupAdapter(Venue.OKX)
    bybit = _OwnedConflictCleanupAdapter(Venue.BYBIT)
    bybit.position_snapshots = [live_short, live_short]
    bybit.default_position_side = Side.SELL
    bybit.default_position_qty = 0.0
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: okx,
            Venue.BYBIT: bybit,
        },
    )
    runtime.journal = tmp_journal
    pending = _pending_entry(
        pending_id="entry-1781373126018-HOMEUSDT",
        symbol="HOMEUSDT",
        long_venue=Venue.OKX,
        short_venue=Venue.BYBIT,
        target_quantity=1600.0,
        maker_leg="long",
        maker_order_id="home-maker-order",
        maker_client_order_id="home-maker-cid",
        hedge_order_id="home-hedge-order",
        hedge_client_order_id="home-hedge-cid",
        maker_leg_filled=1600.0,
        hedge_leg_filled=1600.0,
        maker_fill_price=0.01531,
        hedge_fill_price=0.01529,
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    finalized = await runtime._finalize_pending_entry(
        pending,
        pending.pending_id,
        1781373163000,
    )

    assert finalized is True
    assert pending.pending_id not in runtime.state.pending_entries
    assert pending.pending_id not in runtime.state.open_positions
    assert bybit.place_order_call_count == 1
    assert bybit.last_request is not None
    assert bybit.last_request.reduce_only is True
    assert bybit.last_request.post_only is False
    assert bybit.last_request.time_in_force == TimeInForce.IOC
    assert bybit.last_request.side == Side.BUY
    assert bybit.last_request.quantity == pytest.approx(1600.0)
    events = tmp_journal.read_all()
    kinds = [event["kind"] for event in events]
    assert "entry.opened" not in kinds
    assert "pending_entry.pending_entry_finalized" not in kinds
    conflict = [
        event["payload"]
        for event in events
        if event["kind"] == "pending_entry.positive_fill_live_truth_conflict"
    ][-1]
    assert conflict["entry_id"] == pending.pending_id
    assert conflict["symbol"] == "HOMEUSDT"
    assert conflict["matched_quantity"] == pytest.approx(1600.0)
    assert conflict["live_long_quantity"] == pytest.approx(0.0)
    assert conflict["live_short_quantity"] == pytest.approx(1600.0)
    assert conflict["live_balanced_quantity"] == pytest.approx(0.0)
    assert conflict["reason"] == "positive_fill_conflicts_with_live_unmatched_truth"
    cleanup = [
        event["payload"]
        for event in events
        if event["kind"] == "pending_entry.owned_live_conflict_cleanup_succeeded"
    ][-1]
    assert cleanup["entry_id"] == pending.pending_id
    assert cleanup["venue"] == "bybit"
    assert cleanup["live_position_side"] == "sell"
    assert cleanup["live_position_quantity"] == pytest.approx(1600.0)
    assert cleanup["post_cleanup_live_long_quantity"] == pytest.approx(0.0)
    assert cleanup["post_cleanup_live_short_quantity"] == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_positive_fill_owned_live_single_leg_cleanup_failure_retains_pending(
    config, tmp_journal,
):
    _mark_live(config)
    live_short = PositionSnapshot(
        venue=Venue.BYBIT,
        symbol="HOMEUSDT",
        side=Side.SELL,
        quantity=1600.0,
        entry_price=0.01529,
        observed_at_ms=1781373163000,
    )
    okx = _OwnedConflictCleanupAdapter(Venue.OKX)
    bybit = _OwnedConflictCleanupAdapter(Venue.BYBIT)
    bybit.position_snapshots = [
        live_short,
        live_short,
        live_short,
        live_short,
        live_short,
    ]
    bybit.default_position_side = Side.SELL
    bybit.default_position_qty = 1600.0
    bybit.place_order_outcomes = [
        make_fake_fill(Venue.BYBIT, "HOMEUSDT", Side.BUY, 0.0, price=0.01529),
        make_uncertain_error("cleanup submit timeout"),
        make_fake_fill(Venue.BYBIT, "HOMEUSDT", Side.BUY, 0.0, price=0.01529),
    ]
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: okx,
            Venue.BYBIT: bybit,
        },
    )
    runtime.journal = tmp_journal
    pending = _pending_entry(
        pending_id="entry-1781373126018-HOMEUSDT",
        symbol="HOMEUSDT",
        long_venue=Venue.OKX,
        short_venue=Venue.BYBIT,
        target_quantity=1600.0,
        maker_leg="long",
        maker_order_id="home-maker-order",
        maker_client_order_id="home-maker-cid",
        hedge_order_id="home-hedge-order",
        hedge_client_order_id="home-hedge-cid",
        maker_leg_filled=1600.0,
        hedge_leg_filled=1600.0,
        maker_fill_price=0.01531,
        hedge_fill_price=0.01529,
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    finalized = await runtime._finalize_pending_entry(
        pending,
        pending.pending_id,
        1781373163000,
    )

    assert finalized is False
    assert pending.pending_id in runtime.state.pending_entries
    assert pending.pending_id not in runtime.state.open_positions
    assert bybit.place_order_call_count == 3
    events = tmp_journal.read_all()
    kinds = [event["kind"] for event in events]
    assert "entry.opened" not in kinds
    assert "pending_entry.owned_live_conflict_cleanup_succeeded" not in kinds
    failed = [
        event["payload"]
        for event in events
        if event["kind"] == "pending_entry.owned_live_conflict_cleanup_failed"
    ][-1]
    assert failed["entry_id"] == pending.pending_id
    assert failed["venue"] == "bybit"
    assert failed["result"] == "failed"


@pytest.mark.asyncio
async def test_positive_fill_finalize_records_balanced_live_truth_on_open(
    config, tmp_journal,
):
    _mark_live(config)
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: _LivePositionAdapter(PositionSnapshot(
                venue=Venue.OKX,
                symbol="HOMEUSDT",
                side=Side.BUY,
                quantity=1600.0,
                entry_price=0.02852,
                observed_at_ms=1781376760000,
            )),
            Venue.BYBIT: _LivePositionAdapter(PositionSnapshot(
                venue=Venue.BYBIT,
                symbol="HOMEUSDT",
                side=Side.SELL,
                quantity=1600.0,
                entry_price=0.028914,
                observed_at_ms=1781376760000,
            )),
        },
    )
    runtime.journal = tmp_journal
    pending = _pending_entry(
        pending_id="entry-1781376722066-HOMEUSDT",
        symbol="HOMEUSDT",
        long_venue=Venue.OKX,
        short_venue=Venue.BYBIT,
        target_quantity=1600.0,
        maker_leg="long",
        maker_order_id="home-maker-order",
        maker_client_order_id="home-maker-cid",
        hedge_order_id="home-hedge-order",
        hedge_client_order_id="home-hedge-cid",
        maker_leg_filled=1600.0,
        hedge_leg_filled=1600.0,
        maker_fill_price=0.02852,
        hedge_fill_price=0.028914,
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    finalized = await runtime._finalize_pending_entry(
        pending,
        pending.pending_id,
        1781376760000,
    )

    assert finalized is True
    decision = [
        event["payload"]
        for event in tmp_journal.read_all()
        if event["kind"] == "pending_entry.terminalizer_decision"
    ][-1]
    assert decision["outcome"] == "open_position"
    assert decision["reason"] == "positive_fill_terminalized_with_matched_exposure"
    assert decision["matched_quantity"] == pytest.approx(1600.0)
    assert decision["live_long_quantity"] == pytest.approx(1600.0)
    assert decision["live_short_quantity"] == pytest.approx(1600.0)
    assert decision["live_balanced_quantity"] == pytest.approx(1600.0)


@pytest.mark.asyncio
async def test_live_position_hydrates_balanced_pending_entry_and_finalizes_like_v1(
    config, tmp_journal,
):
    result = PositionReconciliationResult(
        position_id="entry-prl-v1-hydrate",
        symbol="PRLUSDT",
        long_status="filled",
        short_status="uncertain",
        long_position=PositionSnapshot(
            venue=Venue.BINANCE,
            symbol="PRLUSDT",
            side=Side.BUY,
            quantity=146.0,
            entry_price=0.1635,
            observed_at_ms=3000,
        ),
        short_position=PositionSnapshot(
            venue=Venue.BYBIT,
            symbol="PRLUSDT",
            side=Side.SELL,
            quantity=146.0,
            entry_price=0.1631,
            observed_at_ms=3000,
        ),
        is_flat=False,
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.BINANCE: _NoFillReconciliationAdapter(),
            Venue.BYBIT: _NoFillReconciliationAdapter(),
        },
    )
    runtime.journal = tmp_journal
    runtime.reconciler = _CapturingReconciler(result)
    pending = _pending_entry(
        pending_id="entry-prl-v1-hydrate",
        symbol="PRLUSDT",
        target_quantity=146.0,
        long_venue=Venue.BINANCE,
        short_venue=Venue.BYBIT,
        maker_leg_filled=146.0,
        hedge_leg_filled=0.0,
        maker_fill_price=0.1635,
        maker_order_id="193206997",
        hedge_order_id="",
        hedge_fill_price=0.0,
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._reconcile_pending_state(now_ms=4000)

    assert pending.pending_id not in runtime.state.pending_entries
    opened = runtime.state.open_positions[pending.pending_id]
    assert opened.long_quantity == pytest.approx(146.0)
    assert opened.short_quantity == pytest.approx(146.0)
    assert opened.long_entry_price == pytest.approx(0.1635)
    assert opened.short_entry_price == pytest.approx(0.1631)
    kinds = [event["kind"] for event in tmp_journal.read_all()]
    assert "pending_entry.finalize_deferred_incomplete_fill" not in kinds


@pytest.mark.asyncio
async def test_finalize_partially_matched_entry_caps_open_position_fills_and_records_residual_evidence(
    config, tmp_journal,
):
    runtime = LiveRuntime(config)
    runtime.journal = tmp_journal
    runtime._venue_adapters = {
        Venue.OKX: _MetadataAdapter(
            Venue.OKX,
            {
                "APR-USDT-SWAP": {
                    "instId": "APR-USDT-SWAP",
                    "ctVal": "1",
                    "ctType": "linear",
                    "lotSz": "1",
                    "minSz": "1",
                },
                "APRUSDT": {
                    "instId": "APR-USDT-SWAP",
                    "ctVal": "1",
                    "ctType": "linear",
                    "lotSz": "1",
                    "minSz": "1",
                },
            },
            venue_symbol="APR-USDT-SWAP",
        ),
        Venue.BINANCE: _MetadataAdapter(
            Venue.BINANCE,
            {
                "APRUSDT": {
                    "symbol": "APRUSDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                    "quantityPrecision": 0,
                },
            },
        ),
    }
    pending = _pending_entry(
        pending_id="entry-apr-partial",
        symbol="APRUSDT",
        target_quantity=108.0,
        long_venue=Venue.OKX,
        short_venue=Venue.BINANCE,
        maker_leg="long",
        maker_leg_filled=10.0,
        hedge_leg_filled=100.0,
        maker_fill_price=0.2204,
        hedge_fill_price=0.2207069,
        maker_order_id="okx-maker-oid",
        hedge_order_id="binance-hedge-oid",
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._finalize_pending_entry(pending, pending.pending_id, 3000)

    position = runtime.state.open_positions[pending.pending_id]
    assert position.matched_quantity == pytest.approx(10.0)
    assert position.long_quantity == pytest.approx(10.0)
    assert position.short_quantity == pytest.approx(10.0)
    assert position.long_fill.quantity == pytest.approx(10.0)
    assert position.short_fill.quantity == pytest.approx(10.0)
    assert len(runtime.state.pending_residual_repairs) == 1
    residual = runtime.state.pending_residual_repairs[0]
    assert residual["repair_venue"] == "binance"
    assert residual["repair_side"] == "buy"
    assert residual["repair_quantity"] == pytest.approx(90.0)

    events = tmp_journal.read_all()
    opened = [event for event in events if event["kind"] == "entry.opened"][-1]["payload"]
    assert opened["raw_maker_leg_filled"] == pytest.approx(10.0)
    assert opened["raw_hedge_leg_filled"] == pytest.approx(100.0)
    assert opened["open_maker_fill_quantity"] == pytest.approx(10.0)
    assert opened["open_hedge_fill_quantity"] == pytest.approx(10.0)
    assert opened["long_venue_metadata"]["metadata_source"] == "transport_symbol_metadata"
    assert opened["long_venue_metadata"]["ct_val"] == pytest.approx(1.0)
    assert opened["long_venue_metadata"]["ct_type"] == "linear"
    assert opened["short_venue_metadata"]["metadata_source"] == "transport_symbol_metadata"
    assert opened["short_venue_metadata"]["contract_type"] == "PERPETUAL"
    queued = [
        event for event in events
        if event["kind"] == "execution.residual_repair_queued"
    ][-1]["payload"]
    assert queued["raw_long_fill_quantity"] == pytest.approx(10.0)
    assert queued["raw_short_fill_quantity"] == pytest.approx(100.0)
    assert queued["matched_quantity"] == pytest.approx(10.0)
    assert queued["long_venue_metadata"]["ct_val"] == pytest.approx(1.0)
    assert queued["short_venue_metadata"]["contract_type"] == "PERPETUAL"


@pytest.mark.asyncio
async def test_live_position_hydration_records_before_after_exchange_truth_evidence(
    config, tmp_journal,
):
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: _LivePositionAdapter(PositionSnapshot(
                venue=Venue.OKX,
                symbol="APRUSDT",
                side=Side.BUY,
                quantity=100.0,
                entry_price=0.2204,
                observed_at_ms=3000,
            )),
            Venue.BINANCE: _LivePositionAdapter(PositionSnapshot(
                venue=Venue.BINANCE,
                symbol="APRUSDT",
                side=Side.SELL,
                quantity=100.0,
                entry_price=0.2207069,
                observed_at_ms=3000,
            )),
        },
    )
    runtime.journal = tmp_journal
    pending = _pending_entry(
        pending_id="entry-apr-live-hydrate",
        symbol="APRUSDT",
        target_quantity=108.0,
        long_venue=Venue.OKX,
        short_venue=Venue.BINANCE,
        maker_leg="long",
        maker_leg_filled=10.0,
        hedge_leg_filled=0.0,
        maker_fill_price=0.2204,
        hedge_fill_price=0.0,
    )

    hydrated = await runtime._recover_hydrate_from_live_positions(pending)

    assert hydrated is True
    events = tmp_journal.read_all()
    payload = [
        event["payload"]
        for event in events
        if event["kind"] == "pending_entry.live_position_hydrated"
    ][-1]
    assert payload["entry_id"] == "entry-apr-live-hydrate"
    assert payload["symbol"] == "APRUSDT"
    assert payload["before_maker_leg_filled"] == pytest.approx(10.0)
    assert payload["before_hedge_leg_filled"] == pytest.approx(0.0)
    assert payload["after_maker_leg_filled"] == pytest.approx(100.0)
    assert payload["after_hedge_leg_filled"] == pytest.approx(100.0)
    assert payload["live_balanced_quantity"] == pytest.approx(100.0)
    assert payload["live_positions"]["long"]["venue"] == "okx"
    assert payload["live_positions"]["short"]["venue"] == "binance"


@pytest.mark.asyncio
async def test_live_position_hydration_maps_short_maker_to_short_live_truth(
    config, tmp_journal,
):
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.BINANCE: _LivePositionAdapter(PositionSnapshot(
                venue=Venue.BINANCE,
                symbol="MIRRORUSDT",
                side=Side.BUY,
                quantity=40.0,
                entry_price=2.01,
                observed_at_ms=3000,
            )),
            Venue.OKX: _LivePositionAdapter(PositionSnapshot(
                venue=Venue.OKX,
                symbol="MIRRORUSDT",
                side=Side.SELL,
                quantity=40.0,
                entry_price=1.99,
                observed_at_ms=3000,
            )),
        },
    )
    runtime.journal = tmp_journal
    pending = _pending_entry(
        pending_id="entry-short-maker-live-hydrate",
        symbol="MIRRORUSDT",
        target_quantity=40.0,
        long_venue=Venue.BINANCE,
        short_venue=Venue.OKX,
        maker_leg="short",
        maker_leg_filled=5.0,
        hedge_leg_filled=0.0,
        maker_fill_price=0.0,
        hedge_fill_price=0.0,
    )

    hydrated = await runtime._recover_hydrate_from_live_positions(pending)

    assert hydrated is True
    assert pending.maker_leg_filled == pytest.approx(40.0)
    assert pending.hedge_leg_filled == pytest.approx(40.0)
    assert pending.maker_fill_price == pytest.approx(1.99)
    assert pending.hedge_fill_price == pytest.approx(2.01)
    payload = [
        event["payload"]
        for event in tmp_journal.read_all()
        if event["kind"] == "pending_entry.live_position_hydrated"
    ][-1]
    assert payload["maker_leg"] == "short"
    assert payload["maker_live_position"]["venue"] == "okx"
    assert payload["maker_live_position"]["side"] == "sell"
    assert payload["hedge_live_position"]["venue"] == "binance"
    assert payload["hedge_live_position"]["side"] == "buy"


@pytest.mark.asyncio
async def test_pending_entry_under_min_hedge_residual_finalizes_balanced_position(
    config, tmp_journal,
):
    result = PositionReconciliationResult(
        position_id="entry-aria-under-min",
        symbol="ARIAUSDT",
        long_status="uncertain",
        short_status="uncertain",
        is_flat=False,
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.BYBIT: _NoFillReconciliationAdapter(),
            Venue.BINANCE: _NormalizingAdapter(normalized_quantity=0.0),
        },
    )
    runtime.journal = tmp_journal
    runtime.reconciler = _CapturingReconciler(result)
    pending = _pending_entry(
        pending_id="entry-aria-under-min",
        symbol="ARIAUSDT",
        target_quantity=619.0353366004643,
        long_venue=Venue.BYBIT,
        short_venue=Venue.BINANCE,
        maker_leg="long",
        maker_leg_filled=619.0353366004643,
        hedge_leg_filled=619.0,
        maker_fill_price=0.0387,
        hedge_fill_price=0.0387,
        maker_order_id="bybit-maker-filled",
        hedge_order_id="1340395910",
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._reconcile_pending_state(now_ms=4000)

    assert pending.pending_id not in runtime.state.pending_entries
    opened = runtime.state.open_positions[pending.pending_id]
    assert opened.matched_quantity == pytest.approx(619.0)
    assert opened.long_quantity == pytest.approx(619.0)
    assert opened.short_quantity == pytest.approx(619.0)
    events = tmp_journal.read_all()
    kinds = [event["kind"] for event in events]
    assert "pending_entry.hedge_residual_below_min_notional_terminalized" in kinds
    finalized = [
        event["payload"]
        for event in events
        if event["kind"] == "pending_entry.pending_entry_finalized"
    ][-1]
    assert finalized["finalized_as"] == "open_position"
    assert finalized["balanced_quantity"] == pytest.approx(619.0)


@pytest.mark.asyncio
async def test_startup_recovery_under_min_hedge_residual_finalizes_balanced_position(
    config, tmp_journal,
):
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.BYBIT: _NoFillReconciliationAdapter(),
            Venue.BINANCE: _NormalizingAdapter(normalized_quantity=0.0),
        },
    )
    runtime.journal = tmp_journal
    pending = _pending_entry(
        pending_id="entry-aria-startup-under-min",
        symbol="ARIAUSDT",
        target_quantity=619.0353366004643,
        long_venue=Venue.BYBIT,
        short_venue=Venue.BINANCE,
        maker_leg="long",
        maker_leg_filled=619.0353366004643,
        hedge_leg_filled=619.0,
        maker_fill_price=0.0387,
        hedge_fill_price=0.0387,
        maker_order_id="bybit-maker-filled",
        hedge_order_id="1340395910",
        uncertain_outcome=True,
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._recover_pending_entry_hedges(now_ms=4000)

    assert pending.pending_id not in runtime.state.pending_entries
    opened = runtime.state.open_positions[pending.pending_id]
    assert opened.matched_quantity == pytest.approx(619.0)
    payload = [
        event["payload"]
        for event in tmp_journal.read_all()
        if event["kind"] == "pending_entry.hedge_residual_below_min_notional_terminalized"
    ][-1]
    assert payload["source"] == "startup_recovery"


@pytest.mark.asyncio
async def test_startup_recovery_imbalanced_live_truth_finalizes_balanced_position(
    config, tmp_journal,
):
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.BYBIT: _LivePositionOpenOrdersAdapter(PositionSnapshot(
                venue=Venue.BYBIT,
                symbol="ARIAUSDT",
                side=Side.BUY,
                quantity=1238.0,
                entry_price=0.0,
                observed_at_ms=3000,
            )),
            Venue.BINANCE: _LivePositionOpenOrdersAdapter(PositionSnapshot(
                venue=Venue.BINANCE,
                symbol="ARIAUSDT",
                side=Side.SELL,
                quantity=619.0,
                entry_price=0.0387,
                observed_at_ms=3000,
            )),
        },
    )
    runtime.journal = tmp_journal
    pending = _pending_entry(
        pending_id="entry-aria-live-imbalanced",
        symbol="ARIAUSDT",
        target_quantity=619.0353366004643,
        long_venue=Venue.BYBIT,
        short_venue=Venue.BINANCE,
        maker_leg="long",
        maker_leg_filled=1238.0,
        hedge_leg_filled=619.0,
        maker_price=0.03883,
        maker_fill_price=0.0,
        hedge_fill_price=0.0387,
        maker_order_id="bybit-maker-filled",
        hedge_order_id="entry-aria-live-imbalanced-recovery-short",
        uncertain_outcome=True,
        created_at_ms=1000,
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._recover_pending_entry_hedges(now_ms=200_000)

    assert pending.pending_id not in runtime.state.pending_entries
    opened = runtime.state.open_positions[pending.pending_id]
    assert opened.matched_quantity == pytest.approx(619.0)
    assert opened.long_quantity == pytest.approx(619.0)
    assert opened.short_quantity == pytest.approx(619.0)
    kinds = [event["kind"] for event in tmp_journal.read_all()]
    assert "pending_entry.live_position_imbalanced_hydrated" in kinds
    finalized = [
        event["payload"]
        for event in tmp_journal.read_all()
        if event["kind"] == "pending_entry.pending_entry_finalized"
    ][-1]
    assert finalized["finalized_as"] == "open_position"


@pytest.mark.asyncio
async def test_startup_blocked_pending_entry_retries_live_truth_recovery(
    config,
):
    SnapshotStore(config.persistence.snapshot_path).write({
        "lifecycle": "risk_only",
        "risk_mode": "fail_closed",
        "recovery_blocked_reason": "position_drift_correction_failed",
        "recovery_blocked_at_ms": 1234,
        "open_positions": {},
        "pending_entries": {
            "entry-aria-blocked": {
                "pending_id": "entry-aria-blocked",
                "symbol": "ARIAUSDT",
                "long_venue": "bybit",
                "short_venue": "binance",
                "target_quantity": 619.0353366004643,
                "long_side": "buy",
                "short_side": "sell",
                "created_at_ms": 1000,
                "uncertain_outcome": True,
                "maker_order_id": "bybit-maker-filled",
                "maker_client_order_id": "maker-cid",
                "hedge_order_id": "entry-aria-blocked-recovery-short",
                "hedge_client_order_id": "hedge-cid",
                "maker_leg": "long",
                "maker_leg_filled": 1238.0,
                "hedge_leg_filled": 619.0,
                "maker_price": 0.03883,
                "maker_fill_price": 0.0,
                "hedge_fill_price": 0.0387,
                "outcome": "filled",
            },
        },
        "pending_closes": {},
        "pending_passive_closes": {},
        "pending_residual_repairs": [],
    })
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.BYBIT: _LivePositionOpenOrdersAdapter(PositionSnapshot(
                venue=Venue.BYBIT,
                symbol="ARIAUSDT",
                side=Side.BUY,
                quantity=1238.0,
                entry_price=0.0,
                observed_at_ms=3000,
            )),
            Venue.BINANCE: _LivePositionOpenOrdersAdapter(PositionSnapshot(
                venue=Venue.BINANCE,
                symbol="ARIAUSDT",
                side=Side.SELL,
                quantity=619.0,
                entry_price=0.0387,
                observed_at_ms=3000,
            )),
        },
    )

    await runtime.start()
    await runtime.stop()

    assert runtime.state.pending_entries == {}
    opened = runtime.state.open_positions["entry-aria-blocked"]
    assert opened.matched_quantity == pytest.approx(619.0)
    assert runtime.state.lifecycle == EngineLifecycle.RUNNING
    assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
    assert runtime.state.recovery_blocked_reason is None
    kinds = [event["kind"] for event in runtime.journal.read_all()]
    assert "pending_entry.live_position_imbalanced_hydrated" in kinds
    assert "runtime.recovery_blocked" not in kinds


@pytest.mark.asyncio
async def test_startup_rejected_positive_fill_finalizes_open_and_residual(
    config, tmp_journal,
):
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.BYBIT: _LivePositionAdapter(PositionSnapshot(
                venue=Venue.BYBIT,
                symbol="SEIUSDT",
                side=Side.BUY,
                quantity=455.0,
                entry_price=0.05263,
                observed_at_ms=1780570570000,
            )),
            Venue.HYPERLIQUID: _LivePositionAdapter(PositionSnapshot(
                venue=Venue.HYPERLIQUID,
                symbol="SEIUSDT",
                side=Side.SELL,
                quantity=0.0,
                entry_price=0.0,
                observed_at_ms=1780570570000,
            )),
        },
    )
    runtime.journal = tmp_journal
    runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    runtime.state.recovery_blocked_reason = (
        "startup_recovery_pending_work_without_open_positions"
    )
    runtime.state.recovery_blocked_at_ms = 1780570589000
    pending = _pending_entry(
        pending_id="entry-1780570508073-SEIUSDT",
        symbol="SEIUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.HYPERLIQUID,
        target_quantity=455.0,
        maker_leg="long",
        maker_leg_filled=455.0,
        hedge_leg_filled=68.0,
        maker_price=0.05263,
        maker_fill_price=0.05263,
        hedge_fill_price=0.05271,
        maker_order_id="bybit-maker-filled",
        maker_client_order_id="maker-cid",
        hedge_order_id="hyperliquid-partial-fill",
        hedge_client_order_id="hedge-cid",
        outcome="rejected",
        uncertain_outcome=False,
        created_at_ms=1780570508073,
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._recover_pending_entry_hedges(now_ms=1780570590000)

    assert pending.pending_id not in runtime.state.pending_entries
    opened = runtime.state.open_positions[pending.pending_id]
    assert opened.matched_quantity == pytest.approx(68.0)
    assert opened.long_quantity == pytest.approx(68.0)
    assert opened.short_quantity == pytest.approx(68.0)
    [residual] = runtime.state.pending_residual_repairs
    assert residual["symbol"] == "SEIUSDT"
    assert residual["repair_venue"] == "bybit"
    assert residual["repair_side"] == "sell"
    assert residual["repair_quantity"] == pytest.approx(387.0)
    assert runtime.state.lifecycle == EngineLifecycle.RISK_ONLY
    assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
    assert runtime.state.recovery_blocked_reason == (
        "truth_unavailable_for_required_recovery"
    )
    events = tmp_journal.read_all()
    kinds = [event["kind"] for event in events]
    assert "reconciliation.rejected_pending_retained_with_fill" not in kinds
    finalized = [
        event["payload"]
        for event in events
        if event["kind"] == "pending_entry.pending_entry_finalized"
    ][-1]
    assert finalized["raw_maker_leg_filled"] == pytest.approx(455.0)
    assert finalized["raw_hedge_leg_filled"] == pytest.approx(68.0)
    assert finalized["balanced_quantity"] == pytest.approx(68.0)


@pytest.mark.asyncio
async def test_reconcile_rejected_positive_fill_does_not_retained_loop(
    config, tmp_journal,
):
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.BYBIT: _NoFillReconciliationAdapter(),
            Venue.HYPERLIQUID: _NoFillReconciliationAdapter(),
        },
    )
    runtime.journal = tmp_journal
    runtime.reconciler = _CapturingReconciler(PositionReconciliationResult(
        position_id="entry-1780570508073-SEIUSDT",
        symbol="SEIUSDT",
        long_status="filled",
        short_status="uncertain",
        is_flat=False,
    ))
    pending = _pending_entry(
        pending_id="entry-1780570508073-SEIUSDT",
        symbol="SEIUSDT",
        long_venue=Venue.BYBIT,
        short_venue=Venue.HYPERLIQUID,
        target_quantity=455.0,
        maker_leg="long",
        maker_leg_filled=455.0,
        hedge_leg_filled=68.0,
        maker_price=0.05263,
        maker_fill_price=0.05263,
        hedge_fill_price=0.05271,
        maker_order_id="bybit-maker-filled",
        maker_client_order_id="maker-cid",
        hedge_order_id="hyperliquid-partial-fill",
        hedge_client_order_id="hedge-cid",
        outcome="rejected",
        uncertain_outcome=True,
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._reconcile_pending_state(now_ms=1780570595000)

    assert pending.pending_id not in runtime.state.pending_entries
    assert runtime.state.open_positions[pending.pending_id].matched_quantity == pytest.approx(
        68.0
    )
    [residual] = runtime.state.pending_residual_repairs
    assert residual["repair_venue"] == "bybit"
    assert residual["repair_quantity"] == pytest.approx(387.0)
    kinds = [event["kind"] for event in tmp_journal.read_all()]
    assert "reconciliation.rejected_pending_retained_with_fill" not in kinds
    assert "recovery.rejected_pending_positive_fill_finalized" in kinds


@pytest.mark.asyncio
async def test_finalize_zero_fill_does_not_query_planned_hedge_client_order_id(
    config, tmp_journal,
):
    maker_adapter = _RecordingFillAdapter()
    hedge_adapter = _RecordingFillAdapter()
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.BINANCE: maker_adapter,
            Venue.BYBIT: hedge_adapter,
        },
    )
    runtime.journal = tmp_journal
    pending = _pending_entry(
        pending_id="entry-planned-hedge-not-submitted",
        symbol="BEATUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.BYBIT,
        maker_leg="long",
        maker_order_id="maker-order",
        maker_client_order_id="maker-cid",
        hedge_order_id="",
        hedge_client_order_id="planned-hedge-cid",
        maker_leg_filled=0.0,
        hedge_leg_filled=0.0,
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._finalize_pending_entry(pending, pending.pending_id, 4000)

    assert maker_adapter.fill_reconciliation_calls
    assert hedge_adapter.fill_reconciliation_calls == []
