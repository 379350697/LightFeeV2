from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from lightfee.core.domain import (
    OrderFill,
    OrderFillReconciliation,
    PassiveOrderAck,
    PassiveOrderProgress,
    PassiveOrderState,
    PositionSnapshot,
    Side,
    Venue,
    VenueMarketQuote,
    VenueMarketSnapshot,
)
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass


def test_active_probe_defaults_to_no_live_orders():
    mod = importlib.import_module("scripts.probe_active_bybit_lifecycle")
    args = mod.parse_args(["--config", "config/live.toml"])

    assert args.venue == "bybit"
    assert args.symbol == "BTCUSDT"
    assert args.max_notional_quote == 10.0
    assert args.confirm_live_orders is False
    assert mod._live_orders_allowed(args) is False


def test_active_probe_rejects_non_bybit_and_oversized_live_run():
    mod = importlib.import_module("scripts.probe_active_bybit_lifecycle")
    non_bybit = mod.parse_args([
        "--venue", "hyperliquid",
        "--confirm-live-orders",
    ])
    too_large = mod.parse_args([
        "--max-notional-quote", "10.01",
        "--confirm-live-orders",
    ])

    with pytest.raises(ValueError, match="only supports bybit"):
        mod._validate_live_order_args(non_bybit)
    with pytest.raises(ValueError, match="max-notional-quote"):
        mod._validate_live_order_args(too_large)


@pytest.mark.asyncio
async def test_active_probe_dry_run_only_uses_precheck_and_truth(monkeypatch):
    mod = importlib.import_module("scripts.probe_active_bybit_lifecycle")

    class Adapter:
        async def fetch_market_snapshot(self, symbols):
            return VenueMarketSnapshot(
                venue=Venue.BYBIT,
                observed_at_ms=1770000000000,
                quotes=(VenueMarketQuote(symbol=symbols[0], bid=99.0, ask=101.0),),
            )

        async def fetch_position(self, symbol):
            return PositionSnapshot(
                venue=Venue.BYBIT,
                symbol=symbol,
                side=Side.BUY,
                quantity=0.0,
                entry_price=0.0,
                observed_at_ms=1770000000001,
            )

        async def fetch_all_positions(self):
            return []

        async def precheck_order_admission(self, request):
            return {"status": "ok", "precheck_endpoint": "/v5/order/pre-check"}

        async def shutdown(self):
            pass

    async def fake_build_adapter(args):
        return Adapter()

    monkeypatch.setattr(mod, "_build_bybit_adapter", fake_build_adapter)
    args = mod.parse_args(["--config", "config/live.toml", "--dry-run-precheck-only"])

    result = await mod.run_probe(args)

    assert result["ok"] is True
    assert result["mode"] == "dry_run_precheck_only"
    assert result["orders_submitted"] == 0
    assert result["admission_precheck"]["status"] == "ok"
    assert result["final_truth"]["flat"] is True


@pytest.mark.asyncio
async def test_active_probe_uses_ws_bbo_when_adapter_snapshot_has_no_quote(monkeypatch):
    mod = importlib.import_module("scripts.probe_active_bybit_lifecycle")

    class Adapter:
        async def fetch_market_snapshot(self, symbols):
            return VenueMarketSnapshot(
                venue=Venue.BYBIT,
                observed_at_ms=1770000000000,
                quotes=(),
            )

        async def fetch_position(self, symbol):
            return PositionSnapshot(
                venue=Venue.BYBIT,
                symbol=symbol,
                side=Side.BUY,
                quantity=0.0,
                entry_price=0.0,
                observed_at_ms=1770000000001,
            )

        async def fetch_all_positions(self):
            return []

        async def precheck_order_admission(self, request):
            return {"status": "ok"}

        async def shutdown(self):
            pass

    async def fake_build_adapter(args):
        return Adapter()

    async def fake_ws_bbo_quote(venue, symbol, duration_s):
        return {
            "bid": 99.0,
            "ask": 101.0,
            "mid": 100.0,
            "observed_at_ms": 1770000000002.0,
            "source": "ws_bbo_quote_lease",
        }

    monkeypatch.setattr(mod, "_build_bybit_adapter", fake_build_adapter)
    monkeypatch.setattr(mod, "_ws_bbo_quote", fake_ws_bbo_quote)
    args = mod.parse_args(["--config", "config/live.toml", "--dry-run-precheck-only"])

    result = await mod.run_probe(args)

    assert result["ok"] is True
    assert result["quote"]["source"] == "ws_bbo_quote_lease"


@pytest.mark.asyncio
async def test_active_probe_builds_only_bybit_adapter_from_config(monkeypatch):
    mod = importlib.import_module("scripts.probe_active_bybit_lifecycle")
    bybit_adapter = object()
    config = SimpleNamespace(
        runtime=SimpleNamespace(mode="live", exchange_http_timeout_ms=1234),
        venues=[
            SimpleNamespace(venue="binance"),
            SimpleNamespace(venue="bybit"),
        ],
    )

    def fail_build_adapter_map(config):
        raise AssertionError("probe must not build all venue adapters")

    def fake_build_adapter(venue, vc, mode, exchange_http_timeout_ms=0, rate_limiter=None):
        assert venue == Venue.BYBIT
        assert vc.venue == "bybit"
        assert mode == "live"
        assert exchange_http_timeout_ms == 1234
        return bybit_adapter

    monkeypatch.setattr(mod, "load_config", lambda path: config)
    monkeypatch.setattr(mod, "build_adapter_map", fail_build_adapter_map)
    monkeypatch.setattr(mod, "build_adapter", fake_build_adapter, raising=False)
    args = mod.parse_args(["--config", "config/live.toml", "--dry-run-precheck-only"])

    adapter = await mod._build_bybit_adapter(args)

    assert adapter is bybit_adapter


@pytest.mark.asyncio
async def test_active_probe_live_path_submits_passive_close_and_flattens(monkeypatch):
    mod = importlib.import_module("scripts.probe_active_bybit_lifecycle")
    calls: list[str] = []

    class Adapter:
        async def fetch_market_snapshot(self, symbols):
            return VenueMarketSnapshot(
                venue=Venue.BYBIT,
                observed_at_ms=1770000000000,
                quotes=(VenueMarketQuote(symbol=symbols[0], bid=99.0, ask=101.0),),
            )

        async def fetch_position(self, symbol):
            qty = 0.001 if "open" in calls and "closed" not in calls else 0.0
            return PositionSnapshot(
                venue=Venue.BYBIT,
                symbol=symbol,
                side=Side.BUY,
                quantity=qty,
                entry_price=100.0,
                observed_at_ms=1770000000001,
            )

        async def fetch_all_positions(self):
            pos = await self.fetch_position("BTCUSDT")
            return [pos] if pos.quantity else []

        async def precheck_order_admission(self, request):
            calls.append("precheck")
            return {"status": "ok"}

        async def place_order(self, request):
            calls.append("open")
            return OrderFill(
                venue=Venue.BYBIT,
                symbol=request.symbol,
                side=request.side,
                quantity=request.quantity,
                price=request.price_hint or 100.0,
                order_id="open-1",
                client_order_id=request.client_order_id,
                filled_at_ms=1770000000100,
            )

        async def submit_passive_order(self, request):
            calls.append("passive_close")
            return PassiveOrderAck(
                venue=Venue.BYBIT,
                symbol=request.symbol,
                side=request.side,
                order_id="close-ack-1",
                client_order_id=request.client_order_id or "",
                price=request.price or 101.0,
                quantity=request.quantity,
                accepted_at_ms=1770000000200,
                state=PassiveOrderState.UNKNOWN,
            )

        async def query_passive_order_progress(self, **kwargs):
            return PassiveOrderProgress(
                venue=Venue.BYBIT,
                symbol=kwargs["symbol"],
                side=kwargs["side"],
                order_id=kwargs["order_id"],
                client_order_id=kwargs["client_order_id"],
                cumulative_quantity=0.001,
                average_price=101.0,
                state=PassiveOrderState.FILLED,
                observed_at_ms=1770000000300,
            )

        async def fetch_order_fill_reconciliation(self, symbol, order_id, client_order_id):
            calls.append("reconcile")
            calls.append("closed")
            return OrderFillReconciliation(
                venue=Venue.BYBIT,
                symbol=symbol,
                side=Side.SELL,
                quantity=0.001,
                average_price=101.0,
                order_id=order_id,
                client_order_id=client_order_id,
                filled_at_ms=1770000000300,
            )

        async def cancel_passive_order(self, symbol, order_id, client_order_id):
            calls.append("cancel")
            return PassiveOrderAck(
                venue=Venue.BYBIT,
                symbol=symbol,
                side=Side.SELL,
                order_id=order_id,
                client_order_id=client_order_id,
                state=PassiveOrderState.CANCELED,
            )

        async def shutdown(self):
            pass

    async def fake_build_adapter(args):
        return Adapter()

    monkeypatch.setattr(mod, "_build_bybit_adapter", fake_build_adapter)
    args = mod.parse_args([
        "--config", "config/live.toml",
        "--confirm-live-orders",
        "--max-notional-quote", "1",
        "--settle-timeout-s", "0.1",
    ])

    result = await mod.run_probe(args)

    assert result["ok"] is True
    assert result["orders_submitted"] == 2
    assert result["open_order"]["classification"] == "filled"
    assert result["passive_close"]["classification"] == "ack_only_resolved"
    assert result["final_truth"]["flat"] is True
    assert calls == ["precheck", "open", "passive_close", "reconcile", "closed"]


@pytest.mark.asyncio
async def test_active_probe_reconciles_ack_only_open_without_field_drift(monkeypatch):
    mod = importlib.import_module("scripts.probe_active_bybit_lifecycle")
    calls: list[str] = []

    class Adapter:
        async def fetch_market_snapshot(self, symbols):
            return VenueMarketSnapshot(
                venue=Venue.BYBIT,
                observed_at_ms=1770000000000,
                quotes=(VenueMarketQuote(symbol=symbols[0], bid=99.0, ask=101.0),),
            )

        async def fetch_position(self, symbol):
            qty = 0.001 if "open_reconciled" in calls and "closed" not in calls else 0.0
            return PositionSnapshot(
                venue=Venue.BYBIT,
                symbol=symbol,
                side=Side.BUY,
                quantity=qty,
                entry_price=100.0,
                observed_at_ms=1770000000001,
            )

        async def fetch_all_positions(self):
            pos = await self.fetch_position("BTCUSDT")
            return [pos] if pos.quantity else []

        async def precheck_order_admission(self, request):
            return {"status": "ok"}

        async def place_order(self, request):
            calls.append("open_ack_only")
            raise OrderSubmitError(
                SubmitFailureClass.UNCERTAIN,
                "bybit order accepted but fill confirmation missing",
            )

        async def fetch_order_fill_reconciliation(self, symbol, order_id, client_order_id):
            if "passive_close" in calls:
                calls.append("closed")
                return OrderFillReconciliation(
                    venue=Venue.BYBIT,
                    symbol=symbol,
                    side=Side.SELL,
                    quantity=0.001,
                    average_price=101.0,
                    order_id=order_id,
                    client_order_id=client_order_id,
                    filled_at_ms=1770000000300,
                )
            calls.append("open_reconciled")
            return OrderFillReconciliation(
                venue=Venue.BYBIT,
                symbol=symbol,
                side=Side.BUY,
                quantity=0.001,
                average_price=100.0,
                order_id=order_id,
                client_order_id=client_order_id,
                filled_at_ms=1770000000100,
            )

        async def submit_passive_order(self, request):
            calls.append("passive_close")
            return PassiveOrderAck(
                venue=Venue.BYBIT,
                symbol=request.symbol,
                side=request.side,
                order_id="close-ack-1",
                client_order_id=request.client_order_id or "",
                quantity=request.quantity,
                state=PassiveOrderState.UNKNOWN,
            )

        async def query_passive_order_progress(self, **kwargs):
            return PassiveOrderProgress(
                venue=Venue.BYBIT,
                symbol=kwargs["symbol"],
                side=kwargs["side"],
                order_id=kwargs["order_id"],
                client_order_id=kwargs["client_order_id"],
                cumulative_quantity=0.001,
                state=PassiveOrderState.FILLED,
            )

        async def shutdown(self):
            pass

    async def fake_build_adapter(args):
        return Adapter()

    monkeypatch.setattr(mod, "_build_bybit_adapter", fake_build_adapter)
    args = mod.parse_args([
        "--config", "config/live.toml",
        "--confirm-live-orders",
        "--max-notional-quote", "1",
        "--settle-timeout-s", "0.1",
    ])

    result = await mod.run_probe(args)

    assert result["ok"] is True
    assert result["open_order"]["classification"] == "ack_only_reconciled"
    assert calls == ["open_ack_only", "open_reconciled", "passive_close", "closed"]
