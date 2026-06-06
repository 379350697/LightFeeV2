from __future__ import annotations

import json
from pathlib import Path

import pytest

from lightfee.core.domain import PositionSnapshot, Side, Venue
from lightfee.engine.recovery_ledger import RecoveryLedger
from lightfee.engine.runtime import LiveRuntime
from tests.test_live_startup_preflight import make_test_config


pytestmark = pytest.mark.live_harness

FIXTURE_ROOT = Path("tests/fixtures/live_incidents/2026-06-05")


def load_incident(name: str) -> dict:
    return json.loads((FIXTURE_ROOT / name).read_text())


def test_trxusdt_open_maker_order_local_flat_is_blocking_recovery_work():
    fixture = load_incident("trxusdt_open_order_local_flat.json")
    ledger = RecoveryLedger.from_incident_fixture(fixture)

    assert any(item.blocking for item in ledger.work_items)
    assert ledger.work_items[0].kind == "orphan_maker_order"
    assert ledger.allows_new_entry(object()) is False


def test_seiusdt_positive_fill_local_false_flat_is_not_proven_flat():
    fixture = load_incident("seiusdt_positive_fill_local_false_flat.json")
    ledger = RecoveryLedger.from_incident_fixture(fixture)

    assert any(item.blocking for item in ledger.work_items)
    assert ledger.contains_positive_fill_evidence("SEIUSDT")
    assert ledger.is_proven_flat("SEIUSDT") is False


class RecoveryTruthAdapter:
    def __init__(
        self,
        venue: Venue,
        *,
        positions: dict[str, float] | None = None,
        open_orders: dict[str, list[dict]] | None = None,
        position_error: Exception | None = None,
        open_order_error: Exception | None = None,
    ) -> None:
        self.venue = venue
        self.positions = positions or {}
        self.open_orders = open_orders or {}
        self.position_error = position_error
        self.open_order_error = open_order_error
        self.position_calls: list[str] = []
        self.open_order_calls: list[str] = []

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        self.position_calls.append(symbol)
        if self.position_error is not None:
            raise self.position_error
        quantity = self.positions.get(symbol, 0.0)
        return PositionSnapshot(
            venue=self.venue,
            symbol=symbol,
            side=Side.BUY,
            quantity=quantity,
            entry_price=0.1887 if quantity else 0.0,
            observed_at_ms=1778787000000,
        )

    async def fetch_open_orders(self, symbol: str) -> list[dict]:
        self.open_order_calls.append(symbol)
        if self.open_order_error is not None:
            raise self.open_order_error
        return list(self.open_orders.get(symbol, []))


@pytest.mark.asyncio
async def test_runtime_collects_position_and_open_order_truth_for_each_requested_symbol(tmp_path):
    bybit = RecoveryTruthAdapter(
        Venue.BYBIT,
        positions={"SEIUSDT": 455.0},
        open_orders={
            "TRXUSDT": [
                {
                    "venue": "bybit",
                    "symbol": "TRXUSDT",
                    "side": "buy",
                    "quantity": 72.0,
                    "price": 0.33044,
                    "reduce_only": False,
                    "order_id": "bybit-maker",
                }
            ]
        },
    )
    okx = RecoveryTruthAdapter(Venue.OKX)
    runtime = LiveRuntime(
        make_test_config(str(tmp_path)),
        venue_adapters={Venue.BYBIT: bybit, Venue.OKX: okx},
    )

    payload = await runtime._collect_recovery_ledger_exchange_truth(
        ["SEIUSDT", "TRXUSDT"],
        1778787000000,
    )

    assert bybit.position_calls == ["SEIUSDT", "TRXUSDT"]
    assert bybit.open_order_calls == ["SEIUSDT", "TRXUSDT"]
    assert okx.position_calls == ["SEIUSDT", "TRXUSDT"]
    assert okx.open_order_calls == ["SEIUSDT", "TRXUSDT"]
    assert {item["symbol"] for item in payload["positions"]} == {"SEIUSDT", "TRXUSDT"}
    assert payload["open_orders"][0]["order_id"] == "bybit-maker"
    evidence_keys = {
        (item["venue"], item["symbol"], item["classification"])
        for item in payload["probe_evidence"]
    }
    assert ("bybit", "SEIUSDT", "position_truth") in evidence_keys
    assert ("bybit", "TRXUSDT", "open_order_truth") in evidence_keys
    assert ("okx", "SEIUSDT", "position_truth") in evidence_keys
    assert ("okx", "TRXUSDT", "open_order_truth") in evidence_keys
    assert payload["truth_available"] is True


@pytest.mark.asyncio
async def test_runtime_collects_partial_venue_error_without_dropping_successful_venue(tmp_path):
    bybit = RecoveryTruthAdapter(
        Venue.BYBIT,
        open_orders={
            "TRXUSDT": [
                {
                    "venue": "bybit",
                    "symbol": "TRXUSDT",
                    "side": "buy",
                    "quantity": 72.0,
                    "price": 0.33044,
                    "reduce_only": False,
                    "order_id": "bybit-maker",
                }
            ]
        },
    )
    okx = RecoveryTruthAdapter(
        Venue.OKX,
        open_order_error=TimeoutError("open order truth timed out"),
    )
    runtime = LiveRuntime(
        make_test_config(str(tmp_path)),
        venue_adapters={Venue.BYBIT: bybit, Venue.OKX: okx},
    )

    payload = await runtime._collect_recovery_ledger_exchange_truth(
        ["TRXUSDT"],
        1778787000000,
    )

    assert payload["truth_supported"] is True
    assert payload["truth_available"] is False
    assert payload["open_orders"][0]["order_id"] == "bybit-maker"
    assert payload["errors"] == [
        "okx:TRXUSDT:open_orders:open order truth timed out"
    ]
    assert any(
        item["venue"] == "okx"
        and item["symbol"] == "TRXUSDT"
        and item["classification"] == "open_order_truth_error"
        and item["error"] == "open order truth timed out"
        for item in payload["probe_evidence"]
    )


def test_runtime_refresh_ledger_preserves_multi_symbol_work_items(tmp_path):
    runtime = LiveRuntime(make_test_config(str(tmp_path)))
    runtime.journal.open()
    runtime.state.pending_entries["entry-sei"] = {
        "pending_id": "entry-sei",
        "symbol": "SEIUSDT",
        "long_venue": "bybit",
        "short_venue": "hyperliquid",
    }

    ledger = runtime._refresh_recovery_ledger_from_exchange_truth(
        {
            "truth_available": True,
            "positions": [],
            "open_orders": [
                {
                    "venue": "okx",
                    "symbol": "TRXUSDT",
                    "side": "buy",
                    "quantity": 72.0,
                    "price": 0.33044,
                    "reduce_only": False,
                    "order_id": "orphan-trx-maker",
                }
            ],
        },
        now_ms=1778787000000,
    )

    kinds_by_symbol = {(item.symbol, item.kind) for item in ledger.work_items}
    assert kinds_by_symbol == {
        ("SEIUSDT", "owned_pending_entry"),
        ("TRXUSDT", "orphan_maker_order"),
    }
    assert runtime.recovery_ledger is ledger
    assert runtime.recovery_decision is not None
    assert runtime.recovery_decision.block_reason == "orphan_maker_order"
    assert runtime.state.recovery_blocked_reason == "orphan_maker_order"
    runtime.journal.close()
