from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace

import pytest

from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig, StrategyConfig
from lightfee.core.domain import PositionSnapshot, Side, Venue
from lightfee.engine.reconciliation import OrderReconciler, PositionReconciliationResult
from lightfee.engine.runtime import LiveRuntime
from lightfee.engine.state import PendingEntry
from lightfee.persistence.journal import Journal


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

    def drain_order_diagnostics(self):
        return []


class _LivePositionAdapter(_NoFillReconciliationAdapter):
    def __init__(self, position: PositionSnapshot):
        self.position = position

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
