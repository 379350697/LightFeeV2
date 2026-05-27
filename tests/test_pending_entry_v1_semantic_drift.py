from __future__ import annotations

from dataclasses import dataclass, field

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
