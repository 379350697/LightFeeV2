from __future__ import annotations

import httpx
import pytest

from lightfee.venues.market_data import MarketDataClient
from lightfee.venues.specs import (
    aster_spec,
    binance_spec,
    bitget_spec,
    bybit_spec,
    gate_spec,
    hyperliquid_spec,
    okx_spec,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("spec_factory", [binance_spec, aster_spec])
async def test_bulk_bbo_uses_response_receipt_time_not_exchange_event_time(
    monkeypatch,
    spec_factory,
):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[{
                "symbol": "BTCUSDT",
                "bidPrice": "100",
                "askPrice": "101",
                "bidQty": "2",
                "askQty": "3",
                "time": 1_700_000_000_000,
            }],
        )

    client = MarketDataClient(spec_factory())
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    monkeypatch.setattr("lightfee.venues.market_data._now_ms", lambda: 2_000_000_000_000)
    try:
        quote = (await client.fetch_top_book_quotes(["BTCUSDT"]))[
            f"{client.venue.value}:BTCUSDT"
        ]
    finally:
        await client.close()

    assert quote.observed_at_ms == 2_000_000_000_000
    assert quote.received_at_ms == 2_000_000_000_000
    assert quote.exchange_event_at_ms == 1_700_000_000_000
    assert quote.source == "sidecar_bulk_bbo_rest"


@pytest.mark.asyncio
async def test_bulk_bbo_makes_one_lightweight_request_only():
    class FakeBybitClient(MarketDataClient):
        def __init__(self):
            super().__init__(bybit_spec())
            self.calls = []

        async def _public_get_with_received_at(self, path, params=None):
            self.calls.append((path, params))
            return ({
                "time": 1_700_000_000_000,
                "result": {"list": [{
                    "symbol": "BTCUSDT",
                    "bid1Price": "100",
                    "ask1Price": "101",
                    "bid1Size": "2",
                    "ask1Size": "3",
                }]},
            }, 2_000_000_000_000)

    client = FakeBybitClient()

    quotes = await client.fetch_top_book_quotes(["BTCUSDT"])

    assert list(quotes) == ["bybit:BTCUSDT"]
    assert client.calls == [
        ("/v5/market/tickers", {"category": "linear"}),
    ]


@pytest.mark.asyncio
async def test_okx_and_gate_bbo_sizes_use_shared_contract_metadata():
    class FakeOkxClient(MarketDataClient):
        async def _public_get_with_received_at(self, path, params=None):
            return ({"data": [{
                "instId": "BTC-USDT-SWAP",
                "bidPx": "100",
                "askPx": "101",
                "bidSz": "2",
                "askSz": "3",
                "ts": "1999",
            }]}, 2_000)

    okx = FakeOkxClient(okx_spec())
    okx._okx_contract_metadata_by_key["okx:BTCUSDT"] = (
        {"ctVal": "0.01"},
        1_900,
    )

    class FakeGateClient(MarketDataClient):
        async def _public_get_with_received_at(self, path, params=None):
            return ([{
                "contract": "BTC_USDT",
                "highest_bid": "100",
                "lowest_ask": "101",
                "highest_size": "2",
                "lowest_size": "3",
                "time_ms": "1999",
            }], 2_000)

    gate = FakeGateClient(gate_spec())
    gate._gate_contract_metadata_by_key["gate:BTCUSDT"] = (
        {"quanto_multiplier": "0.001"},
        1_900,
    )

    okx_quote = (await okx.fetch_top_book_quotes(["BTCUSDT"]))["okx:BTCUSDT"]
    gate_quote = (await gate.fetch_top_book_quotes(["BTCUSDT"]))["gate:BTCUSDT"]

    assert okx_quote.bid_size == pytest.approx(0.02)
    assert okx_quote.ask_size == pytest.approx(0.03)
    assert gate_quote.bid_size == pytest.approx(0.002)
    assert gate_quote.ask_size == pytest.approx(0.003)


@pytest.mark.asyncio
async def test_bitget_and_hyperliquid_bulk_quote_parsing():
    class FakeBitgetClient(MarketDataClient):
        async def _public_get_with_received_at(self, path, params=None):
            return ({"requestTime": "1999", "data": [{
                "symbol": "BTCUSDT",
                "bidPr": "100",
                "askPr": "101",
                "bidSz": "2",
                "askSz": "3",
            }]}, 2_000)

    class FakeHyperliquidClient(MarketDataClient):
        async def _public_post_with_received_at(self, path, body=None):
            return ([
                {"universe": [{"name": "BTC"}]},
                [{"impactPxs": ["100", "101"], "time": "1999"}],
            ], 2_000)

    bitget = FakeBitgetClient(bitget_spec())
    hyperliquid = FakeHyperliquidClient(hyperliquid_spec())

    bitget_quote = (await bitget.fetch_top_book_quotes(["BTCUSDT"]))[
        "bitget:BTCUSDT"
    ]
    hyperliquid_quote = (await hyperliquid.fetch_top_book_quotes(["BTCUSDT"]))[
        "hyperliquid:BTCUSDT"
    ]

    assert (bitget_quote.bid, bitget_quote.ask) == (100.0, 101.0)
    assert bitget_quote.observed_at_ms == 2_000
    assert hyperliquid_quote.source == "sidecar_bulk_impact_quote_rest"
    assert hyperliquid_quote.bid_size == pytest.approx(200.0)
    assert hyperliquid_quote.ask_size == pytest.approx(20_000.0 / 101.0)
