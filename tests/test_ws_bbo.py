from __future__ import annotations

import pytest


def test_ws_bbo_cache_returns_fresh_quote_and_rejects_stale():
    from lightfee.marketdata.ws_bbo import TopBookQuote, VenueBboCache

    cache = VenueBboCache()
    cache.update_quote(
        TopBookQuote(
            venue="binance",
            symbol="BTCUSDT",
            bid=50000.0,
            ask=50001.0,
            bid_size=2.0,
            ask_size=3.0,
            observed_at_ms=1000,
            received_at_ms=1001,
            source="binance_book_ticker",
        )
    )

    fresh = cache.fresh_quote("BINANCE", "btcusdt", now_ms=1200, max_age_ms=500)
    stale = cache.fresh_quote("binance", "BTCUSDT", now_ms=1800, max_age_ms=500)

    assert fresh is not None
    assert fresh.bid == 50000.0
    assert stale is None


def test_ws_bbo_cache_rejects_future_timestamp_as_non_executable():
    from lightfee.marketdata.ws_bbo import TopBookQuote, VenueBboCache

    cache = VenueBboCache()
    cache.update_quote(
        TopBookQuote(
            venue="binance",
            symbol="BTCUSDT",
            bid=99.0,
            ask=100.0,
            observed_at_ms=1_001,
        )
    )

    assert cache.fresh_quote("binance", "BTCUSDT", now_ms=1_000, max_age_ms=1_000) is None


def test_ws_bbo_cache_rejects_delayed_rest_older_than_ws_event():
    from lightfee.marketdata.ws_bbo import TopBookQuote, VenueBboCache

    cache = VenueBboCache()
    assert cache.update_quote(
        TopBookQuote(
            venue="binance",
            symbol="BTCUSDT",
            bid=100.0,
            ask=100.1,
            observed_at_ms=2_000,
            received_at_ms=2_000,
            exchange_event_at_ms=1_000,
            source="binance_book_ticker",
        )
    )

    assert not cache.update_quote(
        TopBookQuote(
            venue="binance",
            symbol="BTCUSDT",
            bid=99.0,
            ask=99.1,
            observed_at_ms=2_100,
            received_at_ms=2_100,
            exchange_event_at_ms=900,
            source="binance_rest_top_book",
        )
    )
    assert cache.get_quote("binance", "BTCUSDT").bid == 100.0

    assert cache.update_quote(
        TopBookQuote(
            venue="binance",
            symbol="BTCUSDT",
            bid=101.0,
            ask=101.1,
            observed_at_ms=2_200,
            received_at_ms=2_200,
            exchange_event_at_ms=1_100,
            source="binance_book_ticker",
        )
    )
    assert cache.get_quote("binance", "BTCUSDT").bid == 101.0


def test_ws_bbo_cache_rejects_timestamp_less_rest_over_fresh_ws_event():
    from lightfee.marketdata.ws_bbo import TopBookQuote, VenueBboCache

    cache = VenueBboCache()
    assert cache.update_quote(
        TopBookQuote(
            venue="binance",
            symbol="BTCUSDT",
            bid=100.0,
            ask=100.1,
            observed_at_ms=2_000,
            received_at_ms=2_000,
            exchange_event_at_ms=1_000,
            source="binance_book_ticker",
        )
    )

    assert not cache.update_quote(
        TopBookQuote(
            venue="binance",
            symbol="BTCUSDT",
            bid=99.0,
            ask=99.1,
            observed_at_ms=2_100,
            received_at_ms=2_100,
            exchange_event_at_ms=0,
            source="binance_rest_top_book",
        ),
        now_ms=2_100,
        current_max_age_ms=500,
    )
    assert cache.get_quote("binance", "BTCUSDT").bid == 100.0


def test_ws_bbo_cache_allows_timestamp_less_rest_after_ws_lease_expires():
    from lightfee.marketdata.ws_bbo import TopBookQuote, VenueBboCache

    cache = VenueBboCache()
    assert cache.update_quote(
        TopBookQuote(
            venue="binance",
            symbol="BTCUSDT",
            bid=100.0,
            ask=100.1,
            observed_at_ms=1_000,
            received_at_ms=1_000,
            exchange_event_at_ms=900,
            source="binance_book_ticker",
        )
    )

    assert cache.update_quote(
        TopBookQuote(
            venue="binance",
            symbol="BTCUSDT",
            bid=101.0,
            ask=101.1,
            observed_at_ms=2_100,
            received_at_ms=2_100,
            exchange_event_at_ms=0,
            source="binance_rest_top_book",
        ),
        now_ms=2_100,
        current_max_age_ms=500,
    )
    assert cache.get_quote("binance", "BTCUSDT").bid == 101.0


def test_binance_book_ticker_parser_uses_best_bid_and_ask():
    from lightfee.marketdata.ws_bbo import BinanceBboWsClient, VenueBboCache

    client = BinanceBboWsClient("binance", "BTCUSDT", VenueBboCache())
    quote = client.parse_bbo_message(
        {
            "e": "bookTicker",
            "E": 1568014460893,
            "s": "BTCUSDT",
            "b": "25.35190000",
            "B": "31.21000000",
            "a": "25.36520000",
            "A": "40.66000000",
        },
        received_at_ms=1568014460900,
    )

    assert quote is not None
    assert quote.venue == "binance"
    assert quote.symbol == "BTCUSDT"
    assert quote.bid == 25.3519
    assert quote.ask == 25.3652
    assert quote.observed_at_ms == 1568014460900
    assert quote.received_at_ms == 1568014460900
    assert quote.exchange_event_at_ms == 1568014460893


def test_binance_bbo_url_uses_official_public_route():
    from lightfee.marketdata.ws_bbo import binance_bbo_stream_url

    assert (
        binance_bbo_stream_url("BTCUSDT")
        == "wss://fstream.binance.com/public/ws/btcusdt@bookTicker"
    )


def test_okx_tickers_parser_uses_venue_symbol_mapping():
    from lightfee.marketdata.ws_bbo import OkxBboWsClient, VenueBboCache

    client = OkxBboWsClient(
        "okx",
        "BTCUSDT",
        VenueBboCache(),
        venue_symbol="BTC-USDT-SWAP",
    )
    quote = client.parse_bbo_message(
        {
            "arg": {"channel": "tickers", "instId": "BTC-USDT-SWAP"},
            "data": [
                {
                    "instId": "BTC-USDT-SWAP",
                    "bidPx": "8888.88",
                    "bidSz": "11",
                    "askPx": "8889.99",
                    "askSz": "8",
                    "ts": "1597026383085",
                }
            ],
        },
        received_at_ms=1597026383090,
    )

    assert quote is not None
    assert quote.symbol == "BTCUSDT"
    assert quote.bid == 8888.88
    assert quote.ask == 8889.99
    assert quote.observed_at_ms == 1597026383090
    assert quote.received_at_ms == 1597026383090
    assert quote.exchange_event_at_ms == 1597026383085


def test_bybit_ticker_delta_keeps_previous_bid_or_ask_until_both_present():
    from lightfee.marketdata.ws_bbo import BybitBboWsClient, VenueBboCache

    client = BybitBboWsClient("bybit", "BTCUSDT", VenueBboCache())
    snapshot = client.parse_bbo_message(
        {
            "topic": "tickers.BTCUSDT",
            "type": "snapshot",
            "ts": 1672326490000,
            "data": {
                "symbol": "BTCUSDT",
                "bid1Price": "16600.0",
                "bid1Size": "5",
                "ask1Price": "16601.0",
                "ask1Size": "4",
            },
        },
        received_at_ms=1672326490001,
    )
    delta = client.parse_bbo_message(
        {
            "topic": "tickers.BTCUSDT",
            "type": "delta",
            "ts": 1672326490100,
            "data": {"symbol": "BTCUSDT", "ask1Price": "16602.0"},
        },
        received_at_ms=1672326490101,
    )

    assert snapshot is not None
    assert delta is not None
    assert delta.bid == 16600.0
    assert delta.ask == 16602.0


def test_bitget_ticker_parser_accepts_classic_contract_fields():
    from lightfee.marketdata.ws_bbo import BitgetBboWsClient, VenueBboCache

    client = BitgetBboWsClient("bitget", "BTCUSDT", VenueBboCache())
    quote = client.parse_bbo_message(
        {
            "arg": {"instType": "USDT-FUTURES", "channel": "ticker", "instId": "BTCUSDT"},
            "data": [
                {
                    "instId": "BTCUSDT",
                    "bidPr": "87673.6",
                    "bidSz": "6.9129",
                    "askPr": "87673.7",
                    "askSz": "14.333",
                    "ts": "1766674540816",
                }
            ],
        },
        received_at_ms=1766674540817,
    )

    assert quote is not None
    assert quote.bid == 87673.6
    assert quote.ask == 87673.7


def test_gate_book_ticker_parser_uses_futures_contract_symbol():
    from lightfee.marketdata.ws_bbo import GateBboWsClient, VenueBboCache

    client = GateBboWsClient(
        "gate",
        "BTCUSDT",
        VenueBboCache(),
        venue_symbol="BTC_USDT",
    )
    quote = client.parse_bbo_message(
        {
            "time": 1615366379,
            "time_ms": 1615366379123,
            "channel": "futures.book_ticker",
            "event": "update",
            "result": {
                "t": 1615366379123,
                "s": "BTC_USDT",
                "b": "54696.6",
                "B": 37000,
                "a": "54696.7",
                "A": 47061,
            },
        },
        received_at_ms=1615366379124,
    )

    assert quote is not None
    assert quote.symbol == "BTCUSDT"
    assert quote.bid == 54696.6
    assert quote.ask == 54696.7


def test_hyperliquid_bbo_parser_uses_bbo_channel():
    from lightfee.marketdata.ws_bbo import HyperliquidBboWsClient, VenueBboCache

    client = HyperliquidBboWsClient(
        "hyperliquid",
        "BTCUSDT",
        VenueBboCache(),
        venue_symbol="BTC",
    )
    quote = client.parse_bbo_message(
        {
            "channel": "bbo",
            "data": {
                "coin": "BTC",
                "time": 1754450974231,
                "bbo": [
                    {"px": "113377.0", "sz": "7.6699"},
                    {"px": "113397.0", "sz": "0.11543"},
                ],
            },
        },
        received_at_ms=1754450974232,
    )

    assert quote is not None
    assert quote.bid == 113377.0
    assert quote.ask == 113397.0


def test_hyperliquid_multiplex_bbo_routes_each_coin_to_its_canonical_symbol():
    import json

    from lightfee.marketdata.ws_bbo import (
        HyperliquidMultiplexBboWsClient,
        VenueBboCache,
    )

    cache = VenueBboCache()
    client = HyperliquidMultiplexBboWsClient(cache)
    client._symbol_by_wire = {"BTC": "BTCUSDT", "ETH": "ETHUSDT"}
    client._wire_by_symbol = {"BTCUSDT": "BTC", "ETHUSDT": "ETH"}

    client._handle_message(
        json.dumps(
            {
                "channel": "bbo",
                "data": {
                    "coin": "ETH",
                    "time": 1754450974231,
                    "bbo": [
                        {"px": "3377.0", "sz": "7.6"},
                        {"px": "3397.0", "sz": "0.1"},
                    ],
                },
            }
        )
    )

    quote = cache.get_quote("hyperliquid", "ETHUSDT")
    assert quote is not None
    assert quote.bid == 3377.0
    assert quote.ask == 3397.0
    assert quote.source == "hyperliquid_bbo_multiplex"
    assert cache.get_quote("hyperliquid", "BTCUSDT") is None


@pytest.mark.asyncio
async def test_hyperliquid_multiplex_bbo_subscribes_all_symbols_on_one_connection(
    monkeypatch,
):
    import json

    from lightfee.marketdata import ws_bbo

    class FakeWebSocket:
        def __init__(self) -> None:
            self.sent: list[dict] = []
            self.messages = [
                json.dumps(
                    {
                        "channel": "bbo",
                        "data": {
                            "coin": "ETH",
                            "time": 1754450974231,
                            "bbo": [
                                {"px": "3377.0", "sz": "7.6"},
                                {"px": "3397.0", "sz": "0.1"},
                            ],
                        },
                    }
                )
            ]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send(self, payload: str) -> None:
            self.sent.append(json.loads(payload))

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self.messages:
                raise StopAsyncIteration
            return self.messages.pop(0)

    fake = FakeWebSocket()
    monkeypatch.setattr(ws_bbo.websockets, "connect", lambda *_a, **_k: fake)
    cache = ws_bbo.VenueBboCache()
    client = ws_bbo.HyperliquidMultiplexBboWsClient(cache)
    await client.add_symbols({"BTCUSDT": "BTC", "ETHUSDT": "ETH"})

    await client._connect_and_read()

    assert [item["subscription"]["coin"] for item in fake.sent] == ["BTC", "ETH"]
    assert cache.get_quote("hyperliquid", "ETHUSDT") is not None
    assert not client.is_connected


def test_rest_top_book_refresher_fetches_aster_bookticker_official_path():
    import httpx
    from lightfee.marketdata.ws_bbo import RestTopBookQuoteRefresher

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "symbol": "GUNUSDT",
                "bidPrice": "0.00735",
                "bidQty": "100",
                "askPrice": "0.00742",
                "askQty": "120",
                "time": 1778985599950,
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    refresher = RestTopBookQuoteRefresher(client=client, timeout_ms=250)

    quote = refresher.refresh_quote("aster", "GUNUSDT", now_ms=1778985600000)

    assert quote is not None
    assert quote.venue == "aster"
    assert quote.symbol == "GUNUSDT"
    assert quote.bid == 0.00735
    assert quote.ask == 0.00742
    assert quote.observed_at_ms == 1778985600000
    assert quote.exchange_event_at_ms == 1778985599950
    assert quote.source == "aster_rest_top_book"
    assert requests
    assert str(requests[0].url) == (
        "https://fapi.asterdex.com/fapi/v1/ticker/bookTicker?symbol=GUNUSDT"
    )


def test_rest_top_book_refresher_uses_receipt_time_for_binance_style_lease_freshness():
    import httpx
    from lightfee.marketdata.ws_bbo import RestTopBookQuoteRefresher

    exchange_transaction_time_ms = 1778985590000
    rest_received_at_ms = 1778985600000

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "symbol": "XCNUSDT",
                "bidPrice": "0.01420",
                "bidQty": "100000",
                "askPrice": "0.01421",
                "askQty": "120000",
                "time": exchange_transaction_time_ms,
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    refresher = RestTopBookQuoteRefresher(client=client, timeout_ms=250)

    result = refresher.refresh_quote_result(
        "aster",
        "XCNUSDT",
        now_ms=rest_received_at_ms,
    )

    assert result.outcome == "resolved"
    assert result.quote is not None
    assert result.quote.observed_at_ms == rest_received_at_ms
    assert result.quote.received_at_ms == rest_received_at_ms
    assert result.quote.exchange_event_at_ms == exchange_transaction_time_ms
    assert result.observed_at_ms == rest_received_at_ms
    assert result.received_at_ms == rest_received_at_ms
    assert result.exchange_event_at_ms == exchange_transaction_time_ms


def test_bbo_stream_state_reports_quote_lease_readiness_buckets():
    from lightfee.marketdata.ws_bbo import TopBookQuote, VenueBboCache, VenueBboDataPlane

    cache = VenueBboCache()
    plane = VenueBboDataPlane(cache)

    missing = plane.stream_state("binance", "BTCUSDT", now_ms=10_000, max_age_ms=1_000)
    assert missing["lease_state"] == "not_tracked"
    assert missing["reason_bucket"] == "not_tracked"
    assert missing["last_quote_age_ms"] is None

    plane.start_ws_streams("binance", ["BTCUSDT"])
    tracked = plane.stream_state("binance", "BTCUSDT", now_ms=10_000, max_age_ms=1_000)
    assert tracked["tracked"] is True
    assert tracked["connected"] is False
    assert tracked["lease_state"] == "not_connected"
    assert tracked["reason_bucket"] == "not_connected"

    client = plane._clients[("binance", "BTCUSDT")]
    client._connected = True
    client._ws = object()
    no_message = plane.stream_state("binance", "BTCUSDT", now_ms=10_000, max_age_ms=1_000)
    assert no_message["lease_state"] == "subscribed_no_message"
    assert no_message["reason_bucket"] == "subscribed_no_message"

    cache.update_quote(TopBookQuote(
        venue="binance",
        symbol="BTCUSDT",
        bid=100.0,
        ask=101.0,
        observed_at_ms=8_000,
        received_at_ms=8_000,
        source="binance_book_ticker",
    ))
    stale = plane.stream_state("binance", "BTCUSDT", now_ms=10_000, max_age_ms=1_000)
    assert stale["last_quote_age_ms"] == 2_000
    assert stale["lease_state"] == "stale_ws_quote"
    assert stale["reason_bucket"] == "stale_ws_quote"


def test_rest_top_book_refresher_fetches_bybit_linear_ticker():
    import httpx
    from lightfee.marketdata.ws_bbo import RestTopBookQuoteRefresher

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "retCode": 0,
                "time": 1778985599900,
                "result": {
                    "list": [{
                        "symbol": "ARIAUSDT",
                        "bid1Price": "0.0391",
                        "bid1Size": "500",
                        "ask1Price": "0.0393",
                        "ask1Size": "600",
                    }],
                },
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    refresher = RestTopBookQuoteRefresher(client=client, timeout_ms=250)

    quote = refresher.refresh_quote("bybit", "ARIAUSDT", now_ms=1778985600000)

    assert quote is not None
    assert quote.venue == "bybit"
    assert quote.bid == 0.0391
    assert quote.ask == 0.0393
    assert quote.observed_at_ms == 1778985600000
    assert quote.exchange_event_at_ms == 1778985599900
    assert str(requests[0].url) == (
        "https://api.bybit.com/v5/market/tickers?category=linear&symbol=ARIAUSDT"
    )


def test_rest_top_book_refresher_fetches_okx_ticker_with_venue_symbol():
    import httpx
    from lightfee.marketdata.ws_bbo import RestTopBookQuoteRefresher

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "code": "0",
                "data": [{
                    "instId": "ARIA-USDT-SWAP",
                    "bidPx": "0.0390",
                    "bidSz": "700",
                    "askPx": "0.0392",
                    "askSz": "800",
                    "ts": "1778985599980",
                }],
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    refresher = RestTopBookQuoteRefresher(client=client, timeout_ms=250)

    quote = refresher.refresh_quote("okx", "ARIAUSDT", now_ms=1778985600000)

    assert quote is not None
    assert quote.venue == "okx"
    assert quote.symbol == "ARIAUSDT"
    assert quote.bid == 0.0390
    assert quote.ask == 0.0392
    assert quote.observed_at_ms == 1778985600000
    assert quote.exchange_event_at_ms == 1778985599980
    assert str(requests[0].url) == (
        "https://www.okx.com/api/v5/market/ticker?instId=ARIA-USDT-SWAP"
    )


def test_rest_top_book_refresher_fetches_bitget_ticker():
    import httpx
    from lightfee.marketdata.ws_bbo import RestTopBookQuoteRefresher

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "code": "00000",
                "data": [{
                    "symbol": "ARIAUSDT",
                    "bidPr": "0.0390",
                    "bidSz": "700",
                    "askPr": "0.0392",
                    "askSz": "800",
                    "ts": "1778985599980",
                }],
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    refresher = RestTopBookQuoteRefresher(client=client, timeout_ms=250)

    quote = refresher.refresh_quote("bitget", "ARIAUSDT", now_ms=1778985600000)

    assert quote is not None
    assert quote.venue == "bitget"
    assert quote.symbol == "ARIAUSDT"
    assert quote.bid == 0.0390
    assert quote.ask == 0.0392
    assert quote.observed_at_ms == 1778985600000
    assert quote.exchange_event_at_ms == 1778985599980
    assert str(requests[0].url) == (
        "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES&symbol=ARIAUSDT"
    )


def test_rest_top_book_refresher_fetches_gate_futures_ticker():
    import httpx
    from lightfee.marketdata.ws_bbo import RestTopBookQuoteRefresher

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json=[{
                "contract": "ARIA_USDT",
                "highest_bid": "0.0390",
                "lowest_ask": "0.0392",
                "highest_size": "700",
                "lowest_size": "800",
                "time_ms": 1778985599980,
            }],
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    refresher = RestTopBookQuoteRefresher(client=client, timeout_ms=250)

    quote = refresher.refresh_quote("gate", "ARIAUSDT", now_ms=1778985600000)

    assert quote is not None
    assert quote.venue == "gate"
    assert quote.symbol == "ARIAUSDT"
    assert quote.bid == 0.0390
    assert quote.ask == 0.0392
    assert quote.observed_at_ms == 1778985600000
    assert quote.exchange_event_at_ms == 1778985599980
    assert str(requests[0].url) == (
        "https://api.gateio.ws/api/v4/futures/usdt/tickers?contract=ARIA_USDT"
    )


def test_rest_top_book_refresher_fetches_hyperliquid_l2_top():
    import json
    import httpx
    from lightfee.marketdata.ws_bbo import RestTopBookQuoteRefresher

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "POST"
        assert json.loads(request.content.decode()) == {
            "type": "l2Book",
            "coin": "ARIA",
        }
        return httpx.Response(
            200,
            json={
                "time": 1778985599980,
                "levels": [
                    [{"px": "0.0390", "sz": "700"}],
                    [{"px": "0.0392", "sz": "800"}],
                ],
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    refresher = RestTopBookQuoteRefresher(client=client, timeout_ms=250)

    quote = refresher.refresh_quote("hyperliquid", "ARIAUSDT", now_ms=1778985600000)

    assert quote is not None
    assert quote.venue == "hyperliquid"
    assert quote.symbol == "ARIAUSDT"
    assert quote.bid == 0.0390
    assert quote.ask == 0.0392
    assert quote.observed_at_ms == 1778985600000
    assert quote.exchange_event_at_ms == 1778985599980
    assert str(requests[0].url) == "https://api.hyperliquid.xyz/info"


def test_rest_top_book_refresher_rejects_one_sided_quote():
    import httpx
    from lightfee.marketdata.ws_bbo import RestTopBookQuoteRefresher

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "symbol": "COSUSDT",
                "bidPrice": "0",
                "askPrice": "0.001153",
                "time": 1778985599950,
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    refresher = RestTopBookQuoteRefresher(client=client, timeout_ms=250)

    assert refresher.refresh_quote("aster", "COSUSDT", now_ms=1778985600000) is None


def test_rest_top_book_refresher_reports_structured_resolved_and_throttled_results():
    import httpx
    from lightfee.marketdata.ws_bbo import RestTopBookQuoteRefresher

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "symbol": "HEMIUSDT",
                "bidPrice": "0.065",
                "askPrice": "0.066",
                "bidQty": "1000",
                "askQty": "2000",
                "time": 1778985599950,
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    refresher = RestTopBookQuoteRefresher(client=client, timeout_ms=250)

    resolved = refresher.refresh_quote_result(
        "binance",
        "HEMIUSDT",
        now_ms=1778985600000,
    )
    throttled = refresher.refresh_quote_result(
        "binance",
        "HEMIUSDT",
        now_ms=1778985600010,
    )

    assert resolved.outcome == "resolved"
    assert resolved.quote is not None
    assert resolved.bid == 0.065
    assert resolved.ask == 0.066
    assert resolved.venue_symbol == "HEMIUSDT"
    assert resolved.observed_at_ms == 1778985600000
    assert resolved.received_at_ms == 1778985600000
    assert resolved.exchange_event_at_ms == 1778985599950
    assert resolved.url.endswith("/fapi/v1/ticker/bookTicker")
    assert throttled.outcome == "throttled"
    assert throttled.attempt_interval_outcome == "min_interval_not_elapsed"
    assert throttled.quote is None


def test_rest_top_book_refresher_allows_a_shorter_explicit_attempt_interval():
    import httpx
    from lightfee.marketdata.ws_bbo import RestTopBookQuoteRefresher

    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        return httpx.Response(
            200,
            json={
                "time": 1_000,
                "levels": [
                    [{"px": "99.0", "sz": "2"}],
                    [{"px": "101.0", "sz": "3"}],
                ],
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    refresher = RestTopBookQuoteRefresher(
        client=client,
        timeout_ms=250,
        min_attempt_interval_ms=250,
    )

    first = refresher.refresh_quote_result(
        "hyperliquid",
        "BTCUSDT",
        now_ms=1_000,
    )
    throttled = refresher.refresh_quote_result(
        "hyperliquid",
        "BTCUSDT",
        now_ms=1_249,
    )
    retried = refresher.refresh_quote_result(
        "hyperliquid",
        "BTCUSDT",
        now_ms=1_250,
    )

    assert first.outcome == "resolved"
    assert throttled.outcome == "throttled"
    assert retried.outcome == "resolved"
    assert request_count == 2


@pytest.mark.asyncio
async def test_rest_top_book_refresher_async_path_uses_shared_async_client():
    import httpx
    from lightfee.marketdata.ws_bbo import RestTopBookQuoteRefresher

    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "symbol": "BTCUSDT",
                "bidPrice": "100.0",
                "askPrice": "100.1",
                "bidQty": "5",
                "askQty": "6",
                "time": 1_000,
            },
        )

    async_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    refresher = RestTopBookQuoteRefresher(
        async_client=async_client,
        timeout_ms=250,
    )
    try:
        result = await refresher.arefresh_quote_result(
            "binance",
            "BTCUSDT",
            now_ms=1_000,
        )
    finally:
        await async_client.aclose()

    assert result.outcome == "resolved"
    assert result.quote is not None
    assert result.quote.received_at_ms >= 1_000
    assert len(requests) == 1
    assert refresher._async_client is async_client


@pytest.mark.asyncio
async def test_rest_top_book_singleflight_keeps_request_for_remaining_waiter():
    import asyncio

    import httpx

    from lightfee.marketdata.ws_bbo import RestTopBookQuoteRefresher

    started = asyncio.Event()
    release = asyncio.Event()
    cancelled = asyncio.Event()
    request_count = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return httpx.Response(
            200,
            json={
                "symbol": "BTCUSDT",
                "bidPrice": "100.0",
                "askPrice": "100.1",
                "time": 1_000,
            },
        )

    async_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    refresher = RestTopBookQuoteRefresher(
        async_client=async_client,
        timeout_ms=250,
    )
    first = asyncio.create_task(
        refresher.arefresh_quote_result("binance", "BTCUSDT", now_ms=1_000)
    )
    await started.wait()
    second = asyncio.create_task(
        refresher.arefresh_quote_result("binance", "BTCUSDT", now_ms=1_000)
    )
    await asyncio.sleep(0)

    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert not cancelled.is_set()

    release.set()
    result = await second
    await async_client.aclose()

    assert result.outcome == "resolved"
    assert request_count == 1
    assert refresher._async_inflight == {}
    assert refresher._async_inflight_waiters == {}


@pytest.mark.asyncio
async def test_rest_top_book_singleflight_cancels_request_after_last_waiter():
    import asyncio

    import httpx

    from lightfee.marketdata.ws_bbo import RestTopBookQuoteRefresher

    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    refresher = RestTopBookQuoteRefresher(
        async_client=async_client,
        timeout_ms=250,
    )
    waiter = asyncio.create_task(
        refresher.arefresh_quote_result("binance", "BTCUSDT", now_ms=1_000)
    )
    await started.wait()

    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    await asyncio.wait_for(cancelled.wait(), timeout=0.25)
    await asyncio.sleep(0)
    await async_client.aclose()

    assert refresher._async_inflight == {}
    assert refresher._async_inflight_waiters == {}
    assert (
        refresher._global_async_semaphore._value
        == refresher.GLOBAL_ASYNC_CONCURRENCY
    )


@pytest.mark.asyncio
async def test_bbo_data_plane_reconciles_and_stops_untracked_clients():
    import asyncio

    from lightfee.marketdata.ws_bbo import VenueBboCache, VenueBboDataPlane

    plane = VenueBboDataPlane(VenueBboCache())
    assert plane.start_ws_streams("binance", ["BTCUSDT", "ETHUSDT"]) == 2
    old_client = plane._clients[("binance", "BTCUSDT")]
    old_client._task = asyncio.create_task(asyncio.Event().wait())

    stopped = await plane.reconcile_ws_streams({("binance", "ETHUSDT")})

    assert stopped == 1
    assert set(plane._clients) == {("binance", "ETHUSDT")}
    assert old_client._closed is True
    assert old_client._task is None


def test_rest_top_book_refresher_reports_unsupported_symbol_separately():
    import httpx
    from lightfee.marketdata.ws_bbo import RestTopBookQuoteRefresher

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400,
            json={"code": -1121, "msg": "Invalid symbol."},
            request=request,
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    refresher = RestTopBookQuoteRefresher(client=client, timeout_ms=250)

    result = refresher.refresh_quote_result(
        "binance",
        "NOPEUSDT",
        now_ms=1778985600000,
    )

    assert result.outcome == "unsupported_symbol"
    assert result.http_status == 400
    assert result.quote is None


@pytest.mark.asyncio
async def test_bbo_ws_client_records_subscription_error_control_message():
    from lightfee.marketdata.ws_bbo import OkxBboWsClient, VenueBboCache

    client = OkxBboWsClient(
        "okx",
        "BTCUSDT",
        VenueBboCache(),
        venue_symbol="BTC-USDT-SWAP",
    )

    await client._handle_message(
        '{"event":"error","code":"60012","msg":"Invalid request: bad instId"}'
    )

    state = client.state_snapshot()
    assert state["last_error"] == "ws_control_error code=60012 msg=Invalid request: bad instId"
