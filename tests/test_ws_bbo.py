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
    assert quote.observed_at_ms == 1568014460893


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
    assert quote.observed_at_ms == 1597026383085


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
    assert quote.observed_at_ms == 1778985599950
    assert quote.source == "aster_rest_top_book"
    assert requests
    assert str(requests[0].url) == (
        "https://fapi.asterdex.com/fapi/v1/ticker/bookTicker?symbol=GUNUSDT"
    )


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
    assert quote.observed_at_ms == 1778985599900
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
    assert quote.observed_at_ms == 1778985599980
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
    assert quote.observed_at_ms == 1778985599980
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
    assert quote.observed_at_ms == 1778985599980
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
    assert quote.observed_at_ms == 1778985599980
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
    assert resolved.url.endswith("/fapi/v1/ticker/bookTicker")
    assert throttled.outcome == "throttled"
    assert throttled.attempt_interval_outcome == "min_interval_not_elapsed"
    assert throttled.quote is None


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
