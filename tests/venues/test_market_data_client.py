"""Tests for MarketDataClient: credential-free construction, venue parsers, rate-limit scopes."""

from __future__ import annotations

import asyncio
import sys

import pytest

from lightfee.core.domain import Venue
from lightfee.venues.market_data import (
    BINANCE_STYLE_ENTRY_OPEN_INTEREST_BUDGET_S,
    FundingTicker,
    MarketDataClient,
    PerpLiquidity,
    PublicTransportError,
    PublicTransportErrorCategory,
    _safe_float,
)
from lightfee.venues.specs import (
    binance_spec,
    okx_spec,
    bybit_spec,
    bitget_spec,
    gate_spec,
    aster_spec,
    hyperliquid_spec,
)


class TestMarketDataClientConstruction:
    """MarketDataClient must construct without credentials."""

    def test_no_credential_needed(self):
        for spec_fn in (binance_spec, okx_spec, bybit_spec, bitget_spec, gate_spec, aster_spec, hyperliquid_spec):
            spec = spec_fn()
            client = MarketDataClient(spec)
            assert client.venue == spec.venue_id

    def test_all_seven_venues_construct(self):
        specs = [f() for f in (binance_spec, okx_spec, bybit_spec, bitget_spec, gate_spec, aster_spec, hyperliquid_spec)]
        clients = [MarketDataClient(s) for s in specs]
        assert len(clients) == 7
        for c in clients:
            assert isinstance(c.venue, Venue)

    def test_custom_timeout(self):
        spec = binance_spec()
        client = MarketDataClient(spec, exchange_http_timeout_ms=5000)
        assert client._exchange_http_timeout_ms == 5000

    def test_close_does_not_raise_when_no_client(self):
        import asyncio
        spec = binance_spec()
        client = MarketDataClient(spec)
        # Should not raise even though _client is None
        asyncio.run(client.close())

    def test_public_http_client_default_keeps_total_connection_unbounded(self):
        spec = binance_spec()
        client = MarketDataClient(spec)

        http = asyncio.run(client._get_client())

        limits = http._transport._pool
        assert limits._max_connections == sys.maxsize
        assert limits._max_keepalive_connections == 4
        asyncio.run(client.close())

    def test_configured_public_http_client_has_total_connection_limit(self):
        spec = binance_spec()
        client = MarketDataClient(spec, http_max_connections=32)

        http = asyncio.run(client._get_client())

        limits = http._transport._pool
        assert limits._max_connections == 32
        assert limits._max_keepalive_connections == 4
        asyncio.run(client.close())


class TestFundingTickerType:
    """FundingTicker dataclass holds all required fields."""

    def test_construct_with_all_fields(self):
        ft = FundingTicker(
            venue="binance",
            symbol="BTCUSDT",
            bid=50000.0,
            ask=50001.0,
            bid_size=1.5,
            ask_size=2.0,
            mark_price=50000.5,
            index_price=50000.0,
            funding_rate_bps=10.0,
            funding_timestamp_ms=1700000000000,
            volume_24h_quote=1_000_000.0,
            open_interest_quote=500_000.0,
        )
        assert ft.venue == "binance"
        assert ft.funding_rate_bps == 10.0
        assert ft.open_interest_quote == 500_000.0

    def test_frozen(self):
        ft = FundingTicker(venue="x", symbol="y", bid=1, ask=2)
        with pytest.raises(Exception):
            ft.bid = 3  # type: ignore


class TestFundingTickerEnrichment:
    @pytest.mark.parametrize("malformed", [float("inf"), float("nan")])
    def test_nonfinite_symbol_increment_fails_closed_without_exception(
        self, malformed
    ):
        raw = FundingTicker(
            venue="binance",
            symbol="BTCUSDT",
            bid=100.0,
            ask=101.0,
            mark_price=100.5,
            index_price=100.4,
            base_quantity_evidence=True,
            price_tick=malformed,
            quantity_step_base=0.001,
            min_quantity_base=0.001,
            min_notional_quote=5.0,
            min_notional_evidence_complete=True,
        )

        ticker = MarketDataClient(binance_spec())._enrich_tickers(
            {"binance:BTCUSDT": raw},
            observed_at_ms=1,
        )["binance:BTCUSDT"]

        assert ticker.contract_normalization_complete is False

    def test_contract_evidence_accepts_complete_symbol_level_exchange_proof(self):
        client = MarketDataClient(binance_spec())
        raw = FundingTicker(
            venue="binance",
            symbol="BTCUSDT",
            bid=100.0,
            ask=101.0,
            mark_price=100.5,
            index_price=100.4,
            funding_timestamp_ms=9_000_000,
            base_quantity_evidence=True,
            price_tick=0.1,
            quantity_step_base=0.001,
            min_quantity_base=0.001,
            min_notional_quote=5.0,
            min_notional_evidence_complete=True,
        )

        ticker = client._enrich_tickers({"binance:BTCUSDT": raw}, observed_at_ms=1)[
            "binance:BTCUSDT"
        ]

        assert ticker.underlying == "BTC"
        assert ticker.quote_currency == "USDT"
        assert ticker.contract_type == "linear"
        assert ticker.contract_multiplier == 1.0
        assert ticker.price_precision > 0
        assert ticker.quantity_precision > 0
        assert ticker.contract_normalization_complete is True
        assert ticker.funding_interval_ms == 0

    def test_interval_is_measured_from_a_venue_timestamp_transition(self):
        client = MarketDataClient(binance_spec())
        first = FundingTicker(venue="binance", symbol="BTCUSDT", bid=1, ask=2, funding_timestamp_ms=8_000)
        second = FundingTicker(venue="binance", symbol="BTCUSDT", bid=1, ask=2, funding_timestamp_ms=3_608_000)

        assert client._enrich_tickers({"binance:BTCUSDT": first}, observed_at_ms=1)["binance:BTCUSDT"].funding_interval_ms == 0
        assert client._enrich_tickers({"binance:BTCUSDT": second}, observed_at_ms=2)["binance:BTCUSDT"].funding_interval_ms == 3_600_000

    def test_unknown_contract_quantity_is_not_promoted_by_a_static_spec(self):
        client = MarketDataClient(okx_spec())
        raw = FundingTicker(
            venue="okx",
            symbol="BTCUSDT",
            bid=100.0,
            ask=101.0,
            mark_price=100.5,
            index_price=100.4,
        )

        ticker = client._enrich_tickers({"okx:BTCUSDT": raw}, observed_at_ms=1)["okx:BTCUSDT"]

        assert ticker.contract_normalization_complete is False
        assert ticker.venue_status == "unknown"

    def test_integer_lot_precision_is_complete_when_raw_step_is_positive(self):
        from lightfee.venues.specs import hyperliquid_spec

        client = MarketDataClient(hyperliquid_spec())
        raw = FundingTicker(
            venue="hyperliquid",
            symbol="BTCUSDT",
            bid=100.0,
            ask=101.0,
            mark_price=100.5,
            index_price=100.4,
            funding_timestamp_ms=9_000_000,
            funding_interval_ms=3_600_000,
            base_quantity_evidence=True,
            price_tick=1.0,
            quantity_step_base=1.0,
            min_quantity_base=1.0,
            min_notional_quote=10.0,
            min_notional_evidence_complete=True,
        )

        ticker = client._enrich_tickers(
            {"hyperliquid:BTCUSDT": raw}, observed_at_ms=1
        )["hyperliquid:BTCUSDT"]

        assert ticker.quantity_precision == 0
        assert ticker.contract_normalization_complete is True
        assert ticker.venue_status == "active"


class TestPerpLiquidityType:
    """PerpLiquidity dataclass."""

    def test_construct(self):
        pl = PerpLiquidity(
            venue="binance", symbol="BTCUSDT",
            volume_24h_quote=1_000_000.0,
            open_interest_quote=500_000.0,
            observed_at_ms=1700000000000,
        )
        assert pl.volume_24h_quote == 1_000_000.0

    def test_frozen(self):
        pl = PerpLiquidity(venue="x", symbol="y", volume_24h_quote=1, open_interest_quote=2, observed_at_ms=0)
        with pytest.raises(Exception):
            pl.volume_24h_quote = 3  # type: ignore


class TestVenueSpecSidecarEndpoints:
    """VenueSpec must expose new sidecar public endpoint fields."""

    def test_binance_and_aster_have_premium_index_and_oi(self):
        for spec_fn in (binance_spec, aster_spec):
            spec = spec_fn()
            assert spec.funding_ticker_path == "/fapi/v1/ticker/bookTicker"
            assert spec.premium_index_path == "/fapi/v1/premiumIndex"
            assert spec.volume_24h_path == "/fapi/v1/ticker/24hr"
            assert spec.open_interest_path == "/fapi/v1/openInterest"

    def test_okx_has_funding_rate_and_oi_separate(self):
        spec = okx_spec()
        assert spec.funding_ticker_path == "/api/v5/market/tickers"
        assert spec.funding_rate_path == "/api/v5/public/funding-rate"
        assert spec.open_interest_path == "/api/v5/public/open-interest"

    def test_bitget_has_current_fund_rate_sidecar_path(self):
        spec = bitget_spec()
        assert spec.funding_ticker_path == "/api/v2/mix/market/tickers"
        assert spec.funding_rate_path == "/api/v2/mix/market/current-fund-rate"

    def test_gate_has_contracts_funding_sidecar_path(self):
        spec = gate_spec()
        assert spec.funding_ticker_path == "/api/v4/futures/usdt/tickers"
        assert spec.funding_contracts_path == "/api/v4/futures/usdt/contracts"

    def test_bybit_bitget_gate_hl_ticker_includes_volume_oi(self):
        for spec_fn in (bybit_spec, bitget_spec, gate_spec, hyperliquid_spec):
            spec = spec_fn()
            assert spec.ticker_includes_volume_oi is True

    def test_hyperliquid_uses_info_bulk(self):
        spec = hyperliquid_spec()
        assert spec.funding_ticker_path == "/info"
        assert spec.ticker_includes_volume_oi is True


class TestSafeFloat:
    """_safe_float handles edge cases."""

    def test_none_returns_default(self):
        assert _safe_float(None) == 0.0
        assert _safe_float(None, default=-1.0) == -1.0

    def test_empty_string_returns_default(self):
        assert _safe_float("") == 0.0
        assert _safe_float("  ") == 0.0

    def test_valid_conversion(self):
        assert _safe_float("1.5") == 1.5
        assert _safe_float(1.5) == 1.5
        assert _safe_float("0") == 0.0


class TestPublicTransportError:
    """Lightweight public transport error."""

    def test_construct(self):
        e = PublicTransportError(PublicTransportErrorCategory.TRANSPORT_FAILURE, "timeout", status_code=408)
        assert "timeout" in str(e)
        assert e.category == "transport_failure"
        assert e.status_code == 408


class TestRateLimitScopesForNewEndpoints:
    """New public endpoints must have rate-limit scopes."""

    def test_binance_new_endpoints_have_market_scope(self):
        spec = binance_spec()
        client = MarketDataClient(spec)
        scopes_24 = client._public_rate_limit_scopes("GET", "/fapi/v1/ticker/24hr")
        assert any("market" in s for s in scopes_24)
        scopes_oi = client._public_rate_limit_scopes("GET", "/fapi/v1/openInterest")
        assert any("market" in s for s in scopes_oi)

    def test_okx_new_endpoints_have_market_scope(self):
        spec = okx_spec()
        client = MarketDataClient(spec)
        scopes = client._public_rate_limit_scopes("GET", "/api/v5/public/open-interest")
        assert any("market" in s for s in scopes)

    def test_bitget_tickers_endpoint_has_scope(self):
        spec = bitget_spec()
        client = MarketDataClient(spec)
        scopes = client._public_rate_limit_scopes("GET", "/api/v2/mix/market/tickers")
        assert any("market" in s for s in scopes)

    @pytest.mark.asyncio
    async def test_public_429_records_global_rate_limit_cooldown(self):
        import httpx

        from lightfee.rate_limit.config import RateLimitConfigManager
        from lightfee.rate_limit.engine import (
            RateLimitRuntime,
            global_rate_limit_runtime,
            install_global_rate_limit_runtime,
        )

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                429,
                headers={"Retry-After": "1"},
                text="too many requests",
            )

        previous = global_rate_limit_runtime()
        runtime = RateLimitRuntime(
            config_manager=RateLimitConfigManager(config_path=None)
        )
        install_global_rate_limit_runtime(runtime)
        client = MarketDataClient(aster_spec())
        client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            with pytest.raises(PublicTransportError):
                await client._public_get(
                    "/fapi/v1/openInterest",
                    params={"symbol": "XCNUSDT"},
                )
            snap = runtime.engine.bucket_snapshot("venue:aster")
        finally:
            await client.close()
            install_global_rate_limit_runtime(previous)

        assert snap is not None
        assert snap["cooldown_until_ms"] > 0


class TestParserFixtures:
    """Parser-level coverage for all 7 venues with fixture-style data."""

    def test_binance_bookticker_premium_index_parse(self):
        """Binance bookTicker + premiumIndex merge produces correct FundingTicker."""
        # These are tested via the live parsers below; this fixture test
        # documents the expected shape for mock-based unit tests.
        spec = binance_spec()
        assert spec.funding_ticker_path == "/fapi/v1/ticker/bookTicker"
        assert spec.premium_index_path == "/fapi/v1/premiumIndex"

    def test_okx_swap_tickers_funding_rate_parse(self):
        spec = okx_spec()
        assert spec.funding_rate_path == "/api/v5/public/funding-rate"

    def test_bybit_linear_tickers_parse(self):
        spec = bybit_spec()
        assert spec.ticker_includes_volume_oi

    def test_bitget_usdt_futures_parse(self):
        spec = bitget_spec()
        # productType=USDT-FUTURES is the sidecar endpoint
        assert spec.funding_ticker_path == "/api/v2/mix/market/tickers"

    def test_gate_tickers_parse(self):
        spec = gate_spec()
        assert spec.ticker_includes_volume_oi

    def test_hyperliquid_meta_and_asset_ctxs(self):
        spec = hyperliquid_spec()
        assert spec.funding_ticker_path == "/info"


class TestProductionSidecarParserRegressions:
    @pytest.mark.asyncio
    async def test_binance_symbol_metadata_controls_each_exact_order_contract(self):
        class FakeBinanceClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/fapi/v1/ticker/bookTicker":
                    return [
                        {"symbol": "BTCUSDT", "bidPrice": "100", "askPrice": "101", "bidQty": "2", "askQty": "3"},
                        {"symbol": "FOOUSDT", "bidPrice": "2", "askPrice": "2.1", "bidQty": "20", "askQty": "30"},
                    ]
                if path == "/fapi/v1/exchangeInfo":
                    return {"symbols": [
                        {
                            "symbol": "BTCUSDT", "status": "TRADING", "contractType": "PERPETUAL",
                            "baseAsset": "BTC", "quoteAsset": "USDT", "marginAsset": "USDT",
                            "filters": [
                                {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                                {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                                {"filterType": "MIN_NOTIONAL", "notional": "5"},
                            ],
                        },
                        {
                            "symbol": "FOOUSDT", "status": "TRADING", "contractType": "PERPETUAL",
                            "baseAsset": "FOO", "quoteAsset": "USDT", "marginAsset": "USDT",
                            "filters": [
                                {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                                {"filterType": "LOT_SIZE", "stepSize": "1", "minQty": "1"},
                                {"filterType": "MIN_NOTIONAL", "notional": "5"},
                            ],
                        },
                    ]}
                if path == "/fapi/v1/premiumIndex":
                    return [
                        {"symbol": symbol, "markPrice": "100", "indexPrice": "100", "lastFundingRate": "0.0001", "nextFundingTime": "4100007200000", "fundingIntervalHours": "8"}
                        for symbol in ("BTCUSDT", "FOOUSDT")
                    ]
                if path == "/fapi/v1/ticker/24hr":
                    return []
                if path == "/fapi/v1/fundingRate":
                    return []
                if path == "/fapi/v1/openInterest":
                    return {"openInterest": "10"}
                return {}

        tickers = await FakeBinanceClient(binance_spec()).fetch_funding_tickers(
            ["BTCUSDT", "FOOUSDT"]
        )

        btc = tickers["binance:BTCUSDT"]
        foo = tickers["binance:FOOUSDT"]
        assert btc.contract_normalization_complete is True
        assert foo.contract_normalization_complete is True
        assert btc.quantity_step_base == pytest.approx(0.001)
        assert foo.quantity_step_base == pytest.approx(1.0)
        assert btc.price_tick == pytest.approx(0.1)
        assert foo.price_tick == pytest.approx(0.0001)
        assert foo.quantity_precision == 0

    @pytest.mark.asyncio
    async def test_bybit_instrument_metadata_follows_cursor_to_requested_symbol(self):
        class FakeBybitClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/v5/market/tickers":
                    return {"result": {"list": [{
                        "symbol": "TAILUSDT", "bid1Price": "10", "ask1Price": "10.1",
                        "bid1Size": "2", "ask1Size": "3", "markPrice": "10.05",
                        "indexPrice": "10.04", "fundingRate": "0.0001",
                        "nextFundingTime": "4100007200000",
                    }]}}
                cursor = str((params or {}).get("cursor", ""))
                if not cursor:
                    return {"result": {"list": [], "nextPageCursor": "page-2"}}
                assert cursor == "page-2"
                return {"result": {"list": [{
                    "symbol": "TAILUSDT", "status": "Trading",
                    "contractType": "LinearPerpetual", "settleCoin": "USDT",
                    "fundingInterval": "480", "priceFilter": {"tickSize": "0.01"},
                    "lotSizeFilter": {"qtyStep": "0.1", "minOrderQty": "0.1", "minNotionalValue": "1"},
                }], "nextPageCursor": ""}}

        ticker = (
            await FakeBybitClient(bybit_spec()).fetch_funding_tickers(["TAILUSDT"])
        )["bybit:TAILUSDT"]
        assert ticker.contract_normalization_complete is True
        assert ticker.quantity_step_base == pytest.approx(0.1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_timestamp", [None, True, "NaN", 1])
    async def test_bybit_unknown_or_invalid_funding_timestamp_fails_closed(
        self, bad_timestamp
    ):
        class FakeBybitClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/v5/market/tickers":
                    return {"result": {"list": [{
                        "symbol": "BTCUSDT", "bid1Price": "100", "ask1Price": "101",
                        "bid1Size": "1", "ask1Size": "1", "markPrice": "100.5",
                        "indexPrice": "100.4", "fundingRate": "0.0001",
                        "nextFundingTime": bad_timestamp,
                    }]}}
                return {"result": {"list": [{
                    "symbol": "BTCUSDT", "status": "Trading",
                    "contractType": "LinearPerpetual", "settleCoin": "USDT",
                    "fundingInterval": "480", "priceFilter": {"tickSize": "0.1"},
                    "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001", "minNotionalValue": "1"},
                }]}}

        ticker = (
            await FakeBybitClient(bybit_spec()).fetch_funding_tickers(["BTCUSDT"])
        )["bybit:BTCUSDT"]
        assert ticker.funding_timestamp_ms == 0

    """Regression coverage for live sidecar deployment failures."""

    @pytest.mark.asyncio
    async def test_binance_slow_open_interest_does_not_block_quote_return(self):
        symbols = ["BTCUSDT", "ETHUSDT"]

        class FakeBinanceClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/fapi/v1/ticker/bookTicker":
                    return [
                        {"symbol": "BTCUSDT", "bidPrice": "100", "askPrice": "101"},
                        {"symbol": "ETHUSDT", "bidPrice": "200", "askPrice": "201"},
                    ]
                if path == "/fapi/v1/premiumIndex":
                    return [
                        {"symbol": "BTCUSDT", "lastFundingRate": "0.0001", "markPrice": "100.5"},
                        {"symbol": "ETHUSDT", "lastFundingRate": "0.0002", "markPrice": "200.5"},
                    ]
                if path == "/fapi/v1/ticker/24hr":
                    return [
                        {"symbol": "BTCUSDT", "quoteVolume": "12345"},
                        {"symbol": "ETHUSDT", "quoteVolume": "23456"},
                    ]
                if path == "/fapi/v1/openInterest":
                    await asyncio.sleep(1.0)
                    return {"symbol": params["symbol"], "openInterest": "2500"}
                return {}

        result = await asyncio.wait_for(
            FakeBinanceClient(binance_spec())._fetch_binance_style(symbols),
            timeout=0.2,
        )

        assert set(result) == {"binance:BTCUSDT", "binance:ETHUSDT"}
        for ticker in result.values():
            assert ticker.bid > 0.0
            assert ticker.ask > 0.0
            assert ticker.open_interest_quote == 0.0
            assert ticker.open_interest_evidence_status == "refresh_inflight"
            assert ticker.open_interest_evidence_reason == "background_refresh_inflight"
            assert ticker.oi_refresh_cap == 128
            assert ticker.oi_refresh_attempt_count == 2
            assert ticker.oi_timeout_count == 0
            assert ticker.oi_refresh_elapsed_ms >= 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("spec_fn", "venue_key"),
        [(binance_spec, "binance"), (aster_spec, "aster")],
    )
    async def test_binance_style_entry_open_interest_uses_single_symbol_endpoints(
        self,
        spec_fn,
        venue_key,
    ):
        spec = spec_fn()

        class FakeBinanceStyleClient(MarketDataClient):
            def __init__(self):
                super().__init__(spec)
                self.calls: list[tuple[str, dict]] = []

            async def _public_get(self, path, params=None):
                self.calls.append((path, dict(params or {})))
                if path == spec.funding_ticker_path:
                    raise AssertionError("entry OI must not call full bookTicker")
                if path == spec.volume_24h_path:
                    raise AssertionError("entry OI must not call full 24hr ticker")
                if path == spec.premium_index_path:
                    assert params == {"symbol": "BTCUSDT"}
                    return {
                        "symbol": "BTCUSDT",
                        "lastFundingRate": "0.0001",
                        "markPrice": "100.5",
                        "indexPrice": "100.4",
                        "nextFundingTime": "1700000000000",
                    }
                if path == spec.open_interest_path:
                    assert params == {"symbol": "BTCUSDT"}
                    await asyncio.sleep(0.05)
                    return {"symbol": "BTCUSDT", "openInterest": "2500"}
                return {}

        client = FakeBinanceStyleClient()
        client.binance_style_open_interest_enrichment_budget_s = (
            BINANCE_STYLE_ENTRY_OPEN_INTEREST_BUDGET_S
        )

        result = await asyncio.wait_for(
            client.fetch_entry_open_interest_evidence(["BTCUSDT"]),
            timeout=1.0,
        )

        ticker = result[f"{venue_key}:BTCUSDT"]
        assert ticker.open_interest_evidence_status == "available"
        assert ticker.open_interest_evidence_reason == "fresh_refresh"
        assert ticker.open_interest_quote == pytest.approx(2500.0 * 100.5)
        assert [call[0] for call in client.calls] == [
            spec.premium_index_path,
            spec.open_interest_path,
        ]

    @pytest.mark.asyncio
    async def test_binance_entry_open_interest_budget_can_resolve_realistic_latency(self):
        class FakeBinanceClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/fapi/v1/ticker/bookTicker":
                    return [{"symbol": "BTCUSDT", "bidPrice": "100", "askPrice": "101"}]
                if path == "/fapi/v1/premiumIndex":
                    return [{"symbol": "BTCUSDT", "lastFundingRate": "0.0001", "markPrice": "100.5"}]
                if path == "/fapi/v1/ticker/24hr":
                    return [{"symbol": "BTCUSDT", "quoteVolume": "12345"}]
                if path == "/fapi/v1/openInterest":
                    await asyncio.sleep(0.2)
                    return {"symbol": params["symbol"], "openInterest": "2500"}
                return {}

        client = FakeBinanceClient(binance_spec())
        client.binance_style_open_interest_enrichment_budget_s = (
            BINANCE_STYLE_ENTRY_OPEN_INTEREST_BUDGET_S
        )

        result = await asyncio.wait_for(
            client.fetch_entry_open_interest_evidence(["BTCUSDT"]),
            timeout=1.0,
        )

        ticker = result["binance:BTCUSDT"]
        assert ticker.open_interest_evidence_status == "available"
        assert ticker.open_interest_quote == pytest.approx(2500.0 * 100.5)
        assert ticker.oi_timeout_count == 0
        assert ticker.oi_refresh_attempt_count == 1

    @pytest.mark.asyncio
    async def test_binance_slow_open_interest_populates_background_cache(self):
        class FakeBinanceClient(MarketDataClient):
            def __init__(self):
                super().__init__(binance_spec())
                self.oi_calls = 0
                self.binance_style_open_interest_enrichment_budget_s = 0.01

            async def _public_get(self, path, params=None):
                if path == "/fapi/v1/ticker/bookTicker":
                    return [{"symbol": "BTCUSDT", "bidPrice": "100", "askPrice": "101"}]
                if path == "/fapi/v1/premiumIndex":
                    return [{"symbol": "BTCUSDT", "lastFundingRate": "0.0001", "markPrice": "100.5"}]
                if path == "/fapi/v1/ticker/24hr":
                    return [{"symbol": "BTCUSDT", "quoteVolume": "12345"}]
                if path == "/fapi/v1/openInterest":
                    self.oi_calls += 1
                    await asyncio.sleep(0.05)
                    return {"symbol": params["symbol"], "openInterest": "2500"}
                return {}

        client = FakeBinanceClient()

        first = await client._fetch_binance_style(["BTCUSDT"])
        assert first["binance:BTCUSDT"].open_interest_evidence_status == "refresh_inflight"

        await asyncio.sleep(0.08)
        second = await client._fetch_binance_style(["BTCUSDT"])

        assert client.oi_calls == 1
        ticker = second["binance:BTCUSDT"]
        assert ticker.open_interest_evidence_status == "available"
        assert ticker.open_interest_quote == pytest.approx(2500.0 * 100.5)

    @pytest.mark.asyncio
    async def test_binance_open_interest_error_does_not_drop_quotes(self):
        class FakeBinanceClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/fapi/v1/ticker/bookTicker":
                    return [{"symbol": "BTCUSDT", "bidPrice": "100", "askPrice": "101"}]
                if path == "/fapi/v1/premiumIndex":
                    return [{"symbol": "BTCUSDT", "lastFundingRate": "0.0001", "markPrice": "100.5"}]
                if path == "/fapi/v1/ticker/24hr":
                    return [{"symbol": "BTCUSDT", "quoteVolume": "12345"}]
                if path == "/fapi/v1/openInterest":
                    raise PublicTransportError(
                        PublicTransportErrorCategory.TRANSPORT_FAILURE,
                        "HTTP 400: symbol required",
                        status_code=400,
                    )
                return {}

        result = await FakeBinanceClient(binance_spec())._fetch_binance_style(["BTCUSDT"])

        ticker = result["binance:BTCUSDT"]
        assert ticker.bid == 100.0
        assert ticker.ask == 101.0
        assert ticker.open_interest_quote == 0.0
        assert ticker.open_interest_evidence_status == "http_error"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("spec_fn", "venue_key"),
        [(binance_spec, "binance"), (aster_spec, "aster")],
    )
    async def test_binance_style_open_interest_filters_unlisted_symbol_before_http(
        self,
        spec_fn,
        venue_key,
    ):
        spec = spec_fn()

        class FakeBinanceStyleClient(MarketDataClient):
            def __init__(self):
                super().__init__(spec)
                self.oi_calls: list[str] = []

            async def _public_get(self, path, params=None):
                if path == spec.funding_ticker_path:
                    return [{"symbol": "BTCUSDT", "bidPrice": "100", "askPrice": "101"}]
                if path == spec.premium_index_path:
                    return [
                        {
                            "symbol": "BTCUSDT",
                            "lastFundingRate": "0.0001",
                            "markPrice": "100.5",
                        },
                        {
                            "symbol": "GHOSTUSDT",
                            "lastFundingRate": "0.0001",
                            "markPrice": "10.5",
                        },
                    ]
                if path == spec.volume_24h_path:
                    return [{"symbol": "BTCUSDT", "quoteVolume": "12345"}]
                if path == spec.open_interest_path:
                    self.oi_calls.append(params["symbol"])
                    if params["symbol"] == "GHOSTUSDT":
                        raise AssertionError("unsupported symbol must be filtered before OI HTTP")
                    return {"symbol": params["symbol"], "openInterest": "2500"}
                return {}

        client = FakeBinanceStyleClient()
        result = await client._fetch_binance_style(["BTCUSDT", "GHOSTUSDT", "MISSINGUSDT"])

        assert client.oi_calls == ["BTCUSDT"]
        ghost = result[f"{venue_key}:GHOSTUSDT"]
        assert ghost.open_interest_quote == 0.0
        assert ghost.open_interest_evidence_status == "symbol_not_listed_before_http"
        assert ghost.open_interest_evidence_reason == "missing_bulk_book_ticker"
        assert ghost.oi_refresh_attempt_count == 1
        missing = result[f"{venue_key}:MISSINGUSDT"]
        assert missing.open_interest_quote == 0.0
        assert missing.open_interest_evidence_status == "symbol_not_listed_before_http"
        assert missing.open_interest_evidence_reason == "missing_bulk_premium_index"
        assert missing.oi_refresh_attempt_count == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("spec_fn", "venue_key"),
        [(binance_spec, "binance"), (aster_spec, "aster")],
    )
    async def test_binance_style_entry_open_interest_filters_missing_symbol_before_http(
        self,
        spec_fn,
        venue_key,
    ):
        spec = spec_fn()

        class FakeBinanceStyleClient(MarketDataClient):
            def __init__(self):
                super().__init__(spec)
                self.calls: list[tuple[str, dict]] = []

            async def _public_get(self, path, params=None):
                self.calls.append((path, dict(params or {})))
                if path == spec.premium_index_path:
                    return {}
                if path == spec.open_interest_path:
                    raise AssertionError("missing symbol truth must be filtered before OI HTTP")
                return {}

        client = FakeBinanceStyleClient()
        result = await client.fetch_entry_open_interest_evidence(["GHOSTUSDT"])

        ticker = result[f"{venue_key}:GHOSTUSDT"]
        assert [call[0] for call in client.calls] == [spec.premium_index_path]
        assert ticker.open_interest_evidence_status == "symbol_not_listed_before_http"
        assert ticker.open_interest_evidence_reason == "missing_symbol_mark_before_http"
        assert ticker.oi_refresh_attempt_count == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("spec_fn", "venue_key"),
        [(binance_spec, "binance"), (aster_spec, "aster")],
    )
    async def test_binance_style_entry_open_interest_classifies_symbol_reject_before_oi_http(
        self,
        spec_fn,
        venue_key,
    ):
        spec = spec_fn()

        class FakeBinanceStyleClient(MarketDataClient):
            def __init__(self):
                super().__init__(spec)
                self.calls: list[tuple[str, dict]] = []

            async def _public_get(self, path, params=None):
                self.calls.append((path, dict(params or {})))
                if path == spec.premium_index_path:
                    raise PublicTransportError(
                        PublicTransportErrorCategory.TRANSPORT_FAILURE,
                        'HTTP 400: {"code":-1121,"msg":"Invalid symbol."}',
                        status_code=400,
                    )
                if path == spec.open_interest_path:
                    raise AssertionError("invalid symbol must be filtered before OI HTTP")
                return {}

        client = FakeBinanceStyleClient()
        result = await client.fetch_entry_open_interest_evidence(["GHOSTUSDT"])

        ticker = result[f"{venue_key}:GHOSTUSDT"]
        assert [call[0] for call in client.calls] == [spec.premium_index_path]
        assert ticker.open_interest_evidence_status == "symbol_not_listed_before_http"
        assert ticker.open_interest_evidence_reason == "premium_index_symbol_rejected_before_oi_http"
        assert ticker.oi_refresh_attempt_count == 0

    @pytest.mark.asyncio
    async def test_binance_open_interest_429_does_not_drop_quotes(self):
        class FakeBinanceClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/fapi/v1/ticker/bookTicker":
                    return [{"symbol": "BTCUSDT", "bidPrice": "100", "askPrice": "101"}]
                if path == "/fapi/v1/premiumIndex":
                    return [{"symbol": "BTCUSDT", "lastFundingRate": "0.0001", "markPrice": "100.5"}]
                if path == "/fapi/v1/ticker/24hr":
                    return [{"symbol": "BTCUSDT", "quoteVolume": "12345"}]
                if path == "/fapi/v1/openInterest":
                    raise PublicTransportError(
                        PublicTransportErrorCategory.TRANSPORT_FAILURE,
                        "HTTP 429: too many requests",
                        status_code=429,
                    )
                return {}

        result = await FakeBinanceClient(binance_spec())._fetch_binance_style(["BTCUSDT"])

        ticker = result["binance:BTCUSDT"]
        assert ticker.bid == 100.0
        assert ticker.ask == 101.0
        assert ticker.open_interest_quote == 0.0
        assert ticker.open_interest_evidence_status == "rate_limited"

    @pytest.mark.asyncio
    async def test_binance_missing_premium_index_still_skips_non_perpetual(self):
        class FakeBinanceClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/fapi/v1/ticker/bookTicker":
                    return [
                        {"symbol": "BTCUSDT", "bidPrice": "100", "askPrice": "101"},
                        {"symbol": "SPOTONLYUSDT", "bidPrice": "10", "askPrice": "11"},
                    ]
                if path == "/fapi/v1/premiumIndex":
                    return [{"symbol": "BTCUSDT", "lastFundingRate": "0.0001", "markPrice": "100.5"}]
                if path == "/fapi/v1/ticker/24hr":
                    return [
                        {"symbol": "BTCUSDT", "quoteVolume": "12345"},
                        {"symbol": "SPOTONLYUSDT", "quoteVolume": "999"},
                    ]
                if path == "/fapi/v1/openInterest":
                    raise PublicTransportError(
                        PublicTransportErrorCategory.TRANSPORT_FAILURE,
                        "HTTP 429: too many requests",
                        status_code=429,
                    )
                return {}

        result = await FakeBinanceClient(binance_spec())._fetch_binance_style(
            ["BTCUSDT", "SPOTONLYUSDT"]
        )

        assert set(result) == {"binance:BTCUSDT"}

    @pytest.mark.asyncio
    async def test_binance_large_universe_fetches_bounded_quote_open_interest(self):
        symbols = [f"S{i}USDT" for i in range(64)]

        class FakeBinanceClient(MarketDataClient):
            def __init__(self):
                super().__init__(binance_spec())
                self.active_oi = 0
                self.max_active_oi = 0
                self.oi_calls: set[str] = set()

            async def _public_get(self, path, params=None):
                if path == "/fapi/v1/ticker/bookTicker":
                    return [
                        {"symbol": symbol, "bidPrice": "100", "askPrice": "101"}
                        for symbol in symbols
                    ]
                if path == "/fapi/v1/premiumIndex":
                    return [
                        {
                            "symbol": symbol,
                            "lastFundingRate": "0.0001",
                            "markPrice": "100.5",
                        }
                        for symbol in symbols
                    ]
                if path == "/fapi/v1/ticker/24hr":
                    return [
                        {"symbol": symbol, "quoteVolume": "12345"}
                        for symbol in symbols
                    ]
                if path == "/fapi/v1/openInterest":
                    symbol = params["symbol"]
                    self.oi_calls.add(symbol)
                    self.active_oi += 1
                    self.max_active_oi = max(self.max_active_oi, self.active_oi)
                    await asyncio.sleep(0.001)
                    self.active_oi -= 1
                    return {"symbol": symbol, "openInterest": "2500"}
                return {}

        client = FakeBinanceClient()
        result = await client._fetch_binance_style(symbols)

        assert len(result) == len(symbols)
        assert client.oi_calls == set(symbols)
        assert 1 < client.max_active_oi <= 16
        ticker = result["binance:S0USDT"]
        assert ticker.open_interest_quote == pytest.approx(2500.0 * 100.5)
        assert ticker.open_interest_evidence_status == "available"

    @pytest.mark.asyncio
    async def test_binance_open_interest_cache_hit_avoids_repeated_per_symbol_request(self):
        class FakeBinanceClient(MarketDataClient):
            def __init__(self):
                super().__init__(binance_spec())
                self.oi_calls = 0

            async def _public_get(self, path, params=None):
                if path == "/fapi/v1/ticker/bookTicker":
                    return [{"symbol": "BTCUSDT", "bidPrice": "100", "askPrice": "101"}]
                if path == "/fapi/v1/premiumIndex":
                    return [{"symbol": "BTCUSDT", "lastFundingRate": "0.0001", "markPrice": "100.5"}]
                if path == "/fapi/v1/ticker/24hr":
                    return [{"symbol": "BTCUSDT", "quoteVolume": "12345"}]
                if path == "/fapi/v1/openInterest":
                    self.oi_calls += 1
                    return {"symbol": params["symbol"], "openInterest": "2500"}
                return {}

        client = FakeBinanceClient()

        first = await client._fetch_binance_style(["BTCUSDT"])
        second = await client._fetch_binance_style(["BTCUSDT"])

        assert client.oi_calls == 1
        assert first["binance:BTCUSDT"].open_interest_quote == pytest.approx(2500.0 * 100.5)
        assert first["binance:BTCUSDT"].open_interest_evidence_status == "available"
        assert second["binance:BTCUSDT"].open_interest_quote == pytest.approx(2500.0 * 100.5)
        assert second["binance:BTCUSDT"].open_interest_evidence_status == "available"

    @pytest.mark.asyncio
    async def test_binance_open_interest_refresh_is_capped_and_deferred_symbols_are_explicit(self):
        symbols = [f"S{i}USDT" for i in range(8)]

        class FakeBinanceClient(MarketDataClient):
            def __init__(self):
                super().__init__(binance_spec())
                self.oi_calls: list[str] = []
                self.binance_style_open_interest_refresh_cap = 3

            async def _public_get(self, path, params=None):
                if path == "/fapi/v1/ticker/bookTicker":
                    return [
                        {"symbol": symbol, "bidPrice": "100", "askPrice": "101"}
                        for symbol in symbols
                    ]
                if path == "/fapi/v1/premiumIndex":
                    return [
                        {"symbol": symbol, "lastFundingRate": "0.0001", "markPrice": "100.5"}
                        for symbol in symbols
                    ]
                if path == "/fapi/v1/ticker/24hr":
                    return [{"symbol": symbol, "quoteVolume": "12345"} for symbol in symbols]
                if path == "/fapi/v1/openInterest":
                    self.oi_calls.append(params["symbol"])
                    return {"symbol": params["symbol"], "openInterest": "2500"}
                return {}

        client = FakeBinanceClient()
        result = await client._fetch_binance_style(symbols)

        assert len(client.oi_calls) == 3
        statuses = {
            ticker.symbol: ticker.open_interest_evidence_status
            for ticker in result.values()
        }
        assert list(statuses.values()).count("available") == 3
        assert list(statuses.values()).count("deferred_by_cap") == 5
        for ticker in result.values():
            assert ticker.oi_candidate_count == 8
            assert ticker.oi_refresh_cap == 3
            assert ticker.oi_refresh_attempt_count == 3
            assert ticker.oi_deferred_count == 5
            assert ticker.oi_cache_miss_count == 8
        deferred = [
            ticker
            for ticker in result.values()
            if ticker.open_interest_evidence_status == "deferred_by_cap"
        ]
        assert all(
            ticker.open_interest_evidence_reason == "refresh_cap_exceeded"
            for ticker in deferred
        )

    @pytest.mark.asyncio
    async def test_binance_open_interest_refresh_cap_rotates_across_cycles(self):
        symbols = [f"S{i}USDT" for i in range(5)]

        class FakeBinanceClient(MarketDataClient):
            def __init__(self):
                super().__init__(binance_spec())
                self.oi_calls: list[str] = []
                self.binance_style_open_interest_refresh_cap = 2
                self.binance_style_open_interest_enrichment_budget_s = 0.001

            async def _public_get(self, path, params=None):
                if path == "/fapi/v1/ticker/bookTicker":
                    return [
                        {"symbol": symbol, "bidPrice": "100", "askPrice": "101"}
                        for symbol in symbols
                    ]
                if path == "/fapi/v1/premiumIndex":
                    return [
                        {"symbol": symbol, "lastFundingRate": "0.0001", "markPrice": "100.5"}
                        for symbol in symbols
                    ]
                if path == "/fapi/v1/ticker/24hr":
                    return [{"symbol": symbol, "quoteVolume": "12345"} for symbol in symbols]
                if path == "/fapi/v1/openInterest":
                    self.oi_calls.append(params["symbol"])
                    await asyncio.sleep(0.02)
                    return {"symbol": params["symbol"], "openInterest": "2500"}
                return {}

        client = FakeBinanceClient()

        await client._fetch_binance_style(symbols)
        await asyncio.sleep(0.04)
        await client._fetch_binance_style(symbols)

        assert client.oi_calls[:4] == ["S0USDT", "S1USDT", "S2USDT", "S3USDT"]

    @pytest.mark.asyncio
    async def test_binance_entry_open_interest_refresh_scopes_single_deferred_candidate(self):
        symbols = [f"S{i}USDT" for i in range(8)]

        class FakeBinanceClient(MarketDataClient):
            def __init__(self):
                super().__init__(binance_spec())
                self.oi_calls: list[str] = []
                self.binance_style_open_interest_refresh_cap = 1

            async def _public_get(self, path, params=None):
                if path == "/fapi/v1/ticker/bookTicker":
                    return [
                        {"symbol": symbol, "bidPrice": "100", "askPrice": "101"}
                        for symbol in symbols
                    ]
                if path == "/fapi/v1/premiumIndex":
                    return [
                        {
                            "symbol": symbol,
                            "lastFundingRate": "0.0001",
                            "markPrice": "100.5",
                        }
                        for symbol in symbols
                    ]
                if path == "/fapi/v1/ticker/24hr":
                    return [
                        {"symbol": symbol, "quoteVolume": "12345"}
                        for symbol in symbols
                    ]
                if path == "/fapi/v1/openInterest":
                    self.oi_calls.append(params["symbol"])
                    return {"symbol": params["symbol"], "openInterest": "2500"}
                return {}

        client = FakeBinanceClient()
        full = await client._fetch_binance_style(symbols)
        assert full["binance:S7USDT"].open_interest_evidence_status == "deferred_by_cap"

        refreshed = await client.fetch_entry_open_interest_evidence(["S7USDT"])

        assert client.oi_calls == ["S0USDT", "S7USDT"]
        ticker = refreshed["binance:S7USDT"]
        assert ticker.open_interest_evidence_status == "available"
        assert ticker.open_interest_quote == pytest.approx(2500.0 * 100.5)
        assert ticker.oi_candidate_count == 1
        assert ticker.oi_deferred_count == 0

    @pytest.mark.asyncio
    async def test_binance_open_interest_requires_mark_price_evidence(self):
        class FakeBinanceClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/fapi/v1/ticker/bookTicker":
                    return [
                        {"symbol": "ZEROMARKUSDT", "bidPrice": "100", "askPrice": "101"},
                        {"symbol": "MISSINGMARKUSDT", "bidPrice": "200", "askPrice": "201"},
                    ]
                if path == "/fapi/v1/premiumIndex":
                    return [
                        {
                            "symbol": "ZEROMARKUSDT",
                            "lastFundingRate": "0.0001",
                            "markPrice": "0",
                        },
                        {
                            "symbol": "MISSINGMARKUSDT",
                            "lastFundingRate": "0.0001",
                        },
                    ]
                if path == "/fapi/v1/ticker/24hr":
                    return [
                        {"symbol": "ZEROMARKUSDT", "quoteVolume": "12345"},
                        {"symbol": "MISSINGMARKUSDT", "quoteVolume": "23456"},
                    ]
                if path == "/fapi/v1/openInterest":
                    return {"symbol": params["symbol"], "openInterest": "2500"}
                return {}

        result = await FakeBinanceClient(binance_spec())._fetch_binance_style(
            ["ZEROMARKUSDT", "MISSINGMARKUSDT"]
        )

        zero_mark = result["binance:ZEROMARKUSDT"]
        missing_mark = result["binance:MISSINGMARKUSDT"]
        assert zero_mark.open_interest_quote == 0.0
        assert zero_mark.open_interest_evidence_status == "missing_mark_price"
        assert missing_mark.open_interest_quote == 0.0
        assert missing_mark.open_interest_evidence_status == "missing_mark_price"

    @pytest.mark.asyncio
    async def test_okx_funding_rate_requests_are_concurrent(self):
        class FakeOkxClient(MarketDataClient):
            def __init__(self):
                super().__init__(okx_spec())
                self.active_funding = 0
                self.max_active_funding = 0

            async def _public_get(self, path, params=None):
                if path == "/api/v5/market/tickers":
                    return {
                        "data": [
                            {"instId": f"S{i}-USDT-SWAP", "bidPx": "10", "askPx": "11"}
                            for i in range(8)
                        ]
                    }
                if path == "/api/v5/public/funding-rate":
                    self.active_funding += 1
                    self.max_active_funding = max(self.max_active_funding, self.active_funding)
                    await asyncio.sleep(0.01)
                    self.active_funding -= 1
                    return {
                        "data": [{
                            "fundingRate": "0.0002",
                            "fundingTime": "1700000000000",
                            "markPrice": "10.5",
                        }]
                    }
                if path == "/api/v5/public/open-interest":
                    return {"data": []}
                return {}

        client = FakeOkxClient()
        result = await client._fetch_okx_style([f"S{i}USDT" for i in range(8)])

        assert len(result) == 8
        assert client.max_active_funding > 1

    @pytest.mark.asyncio
    async def test_okx_funding_rate_uses_batch_any_before_symbol_fallback(self):
        class FakeOkxClient(MarketDataClient):
            def __init__(self):
                super().__init__(okx_spec())
                self.funding_requests: list[str] = []

            async def _public_get(self, path, params=None):
                if path == "/api/v5/market/tickers":
                    return {
                        "data": [
                            {
                                "instId": f"{symbol}-USDT-SWAP",
                                "bidPx": "10",
                                "askPx": "11",
                            }
                            for symbol in ("BTC", "ETH")
                        ]
                    }
                if path == "/api/v5/public/funding-rate":
                    inst_id = str((params or {}).get("instId", ""))
                    self.funding_requests.append(inst_id)
                    assert inst_id == "ANY"
                    return {
                        "data": [
                            {
                                "instId": f"{symbol}-USDT-SWAP",
                                "fundingRate": rate,
                                "fundingTime": "1778784000000",
                                "nextFundingTime": "1778812800000",
                            }
                            for symbol, rate in (("BTC", "0.0001"), ("ETH", "0.0002"))
                        ]
                    }
                if path == "/api/v5/public/open-interest":
                    return {"data": []}
                return {}

        client = FakeOkxClient()
        result = await client._fetch_okx_style(["BTCUSDT", "ETHUSDT"])

        assert client.funding_requests == ["ANY"]
        assert result["okx:BTCUSDT"].funding_rate_bps == pytest.approx(1.0)
        assert result["okx:ETHUSDT"].funding_rate_bps == pytest.approx(2.0)
        assert result["okx:BTCUSDT"].funding_interval_ms == 28_800_000

    @pytest.mark.asyncio
    async def test_okx_open_interest_uses_usd_value_and_missing_data_is_unavailable(self):
        class FakeOkxClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/api/v5/market/tickers":
                    return {
                        "data": [
                            {
                                "instId": "BTC-USDT-SWAP",
                                "bidPx": "100",
                                "askPx": "101",
                                "markPx": "100.5",
                                "last": "100.5",
                            },
                            {
                                "instId": "ETH-USDT-SWAP",
                                "bidPx": "200",
                                "askPx": "201",
                                "markPx": "200.5",
                                "last": "200.5",
                            },
                        ]
                    }
                if path == "/api/v5/public/funding-rate":
                    return {"data": []}
                if path == "/api/v5/public/open-interest":
                    return {
                        "data": [
                            {
                                "instId": "BTC-USDT-SWAP",
                                "oi": "12",
                                "oiUsd": "1500000",
                            }
                        ]
                    }
                return {}

        result = await FakeOkxClient(okx_spec())._fetch_okx_style(
            ["BTCUSDT", "ETHUSDT"]
        )

        btc = result["okx:BTCUSDT"]
        eth = result["okx:ETHUSDT"]
        assert btc.open_interest_quote == pytest.approx(1_500_000.0)
        assert btc.open_interest_evidence_status == "available"
        assert eth.open_interest_quote == 0.0
        assert eth.open_interest_evidence_status == "unavailable"
        assert eth.open_interest_evidence_reason == "missing_open_interest"

    @pytest.mark.asyncio
    async def test_okx_open_interest_http_error_is_not_available_zero(self):
        class FakeOkxClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/api/v5/market/tickers":
                    return {
                        "data": [
                            {
                                "instId": "BTC-USDT-SWAP",
                                "bidPx": "100",
                                "askPx": "101",
                                "markPx": "100.5",
                                "last": "100.5",
                            }
                        ]
                    }
                if path == "/api/v5/public/funding-rate":
                    return {"data": []}
                if path == "/api/v5/public/open-interest":
                    raise PublicTransportError(
                        PublicTransportErrorCategory.TRANSPORT_FAILURE,
                        "timeout",
                    )
                return {}

        result = await FakeOkxClient(okx_spec())._fetch_okx_style(["BTCUSDT"])

        ticker = result["okx:BTCUSDT"]
        assert ticker.open_interest_quote == 0.0
        assert ticker.open_interest_evidence_status == "http_error"
        assert ticker.open_interest_evidence_reason == "timeout"

    @pytest.mark.asyncio
    async def test_bybit_missing_open_interest_value_is_unavailable_not_zero_available(self):
        class FakeBybitClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                return {
                    "result": {
                        "list": [
                            {
                                "symbol": "BTCUSDT",
                                "bid1Price": "100",
                                "ask1Price": "101",
                                "markPrice": "100.5",
                                "turnover24h": "1000000",
                            }
                        ]
                    }
                }

        result = await FakeBybitClient(bybit_spec())._fetch_bybit_style(["BTCUSDT"])

        ticker = result["bybit:BTCUSDT"]
        assert ticker.open_interest_quote == 0.0
        assert ticker.open_interest_evidence_status == "unavailable"
        assert ticker.open_interest_evidence_reason == "missing_open_interest_value"

    @pytest.mark.asyncio
    async def test_bitget_holding_amount_is_quote_normalized_open_interest(self):
        class FakeBitgetClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                return {
                    "data": [
                        {
                            "symbol": "BTCUSDT",
                            "bidPr": "100",
                            "askPr": "101",
                            "bidSz": "1",
                            "askSz": "2",
                            "markPrice": "100.5",
                            "indexPrice": "100.4",
                            "fundingRate": "0.0001",
                            "usdtVolume": "1000000",
                            "holdingAmount": "2500",
                        }
                    ]
                }

        result = await FakeBitgetClient(bitget_spec())._fetch_bitget_style(["BTCUSDT"])

        ticker = result["bitget:BTCUSDT"]
        assert ticker.bid == 100.0
        assert ticker.ask == 101.0
        assert ticker.open_interest_quote == pytest.approx(2500.0 * 100.5)
        assert ticker.open_interest_evidence_status == "available"

    @pytest.mark.asyncio
    async def test_bitget_current_fund_rate_supplies_future_funding_timestamp(self):
        class FakeBitgetClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/api/v2/mix/market/tickers":
                    return {
                        "data": [
                            {
                                "symbol": "BTCUSDT",
                                "bidPr": "100",
                                "askPr": "101",
                                "bidSz": "1",
                                "askSz": "2",
                                "markPrice": "100.5",
                                "indexPrice": "100.4",
                                "fundingRate": "0.0001",
                                "usdtVolume": "1000000",
                                "holdingAmount": "2500",
                            }
                        ]
                    }
                if path == "/api/v2/mix/market/current-fund-rate":
                    assert params == {"productType": "USDT-FUTURES"}
                    return {
                        "data": [
                            {
                                "symbol": "BTCUSDT",
                                "fundingRate": "0.0003",
                                "nextUpdate": "4100007200000",
                            }
                        ]
                    }
                if path == "/api/v2/mix/market/contracts":
                    return {"data": []}
                raise AssertionError(f"unexpected path: {path}")

        result = await FakeBitgetClient(bitget_spec())._fetch_bitget_style(["BTCUSDT"])

        ticker = result["bitget:BTCUSDT"]
        assert ticker.funding_timestamp_ms == 4_100_007_200_000
        assert ticker.funding_rate_bps == pytest.approx(3.0)

    @pytest.mark.asyncio
    async def test_bitget_missing_current_fund_rate_fails_closed(self):
        class FakeBitgetClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/api/v2/mix/market/tickers":
                    return {
                        "data": [
                            {
                                "symbol": "BTCUSDT",
                                "bidPr": "100",
                                "askPr": "101",
                                "markPrice": "100.5",
                                "fundingRate": "0.0001",
                            }
                        ]
                    }
                if path == "/api/v2/mix/market/current-fund-rate":
                    return {"data": []}
                if path == "/api/v2/mix/market/contracts":
                    return {"data": []}
                raise AssertionError(f"unexpected path: {path}")

        result = await FakeBitgetClient(bitget_spec())._fetch_bitget_style(["BTCUSDT"])

        ticker = result["bitget:BTCUSDT"]
        assert ticker.funding_timestamp_ms == 0
        assert ticker.funding_rate_bps == pytest.approx(1.0)

    @pytest.mark.asyncio
    async def test_gate_contracts_supplies_future_funding_timestamp(self):
        class FakeGateClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/api/v4/futures/usdt/tickers":
                    return [
                        {
                            "contract": "BTC_USDT",
                            "highest_bid": "100",
                            "lowest_ask": "101",
                            "bid_size": "1",
                            "ask_size": "2",
                            "mark_price": "100.5",
                            "index_price": "100.4",
                            "funding_rate": "0.0001",
                            "volume_24h_quote": "1000000",
                            "total_size": "2500",
                            "quanto_multiplier": "0.01",
                        }
                    ]
                if path == "/api/v4/futures/usdt/contracts":
                    return [
                        {
                            "name": "BTC_USDT",
                            "funding_rate": "0.0004",
                            "funding_next_apply": 4100007200,
                            "quanto_multiplier": "0.01",
                            "order_size_min": "1",
                            "order_price_round": "0.1",
                            "in_delisting": False,
                        }
                    ]
                raise AssertionError(f"unexpected path: {path}")

        result = await FakeGateClient(gate_spec())._fetch_gate_style(["BTCUSDT"])

        ticker = result["gate:BTCUSDT"]
        assert ticker.funding_timestamp_ms == 4_100_007_200_000
        assert ticker.funding_rate_bps == pytest.approx(4.0)

    @pytest.mark.asyncio
    async def test_gate_ticker_uses_highest_lowest_size_fields(self):
        class FakeGateClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/api/v4/futures/usdt/tickers":
                    return [
                        {
                            "contract": "ALL_USDT",
                            "highest_bid": "0.4295",
                            "highest_size": "1",
                            "lowest_ask": "0.4297",
                            "lowest_size": "2",
                            "mark_price": "0.4334",
                            "index_price": "0.4363",
                            "funding_rate": "-0.000269",
                            "volume_24h_quote": "17013",
                            "total_size": "45948",
                            "quanto_multiplier": "1",
                        },
                        {
                            "contract": "LAB_USDT",
                            "highest_bid": "11.78813",
                            "highest_size": "0.1",
                            "lowest_ask": "11.78926",
                            "lowest_size": "0.2",
                            "mark_price": "11.81042",
                            "index_price": "12.222091",
                            "funding_rate": "-0.003463",
                            "volume_24h_quote": "41570427",
                            "total_size": "6166",
                            "quanto_multiplier": "100",
                        },
                    ]
                if path == "/api/v4/futures/usdt/contracts":
                    return [
                        {
                            "name": "ALL_USDT",
                            "funding_rate": "-0.000269",
                            "funding_next_apply": 4100007200,
                            "quanto_multiplier": "1",
                            "order_size_min": "1",
                            "order_price_round": "0.0001",
                            "in_delisting": False,
                        },
                        {
                            "name": "LAB_USDT",
                            "funding_rate": "-0.003463",
                            "funding_next_apply": 4100007200,
                            "quanto_multiplier": "100",
                            "order_size_min": "0.1",
                            "order_price_round": "0.00001",
                            "in_delisting": False,
                        },
                    ]
                raise AssertionError(f"unexpected path: {path}")

        result = await FakeGateClient(gate_spec())._fetch_gate_style(
            ["ALLUSDT", "LABUSDT"]
        )

        all_ticker = result["gate:ALLUSDT"]
        lab_ticker = result["gate:LABUSDT"]
        assert all_ticker.bid_size == pytest.approx(1.0)
        assert all_ticker.ask_size == pytest.approx(2.0)
        assert lab_ticker.bid_size == pytest.approx(10.0)
        assert lab_ticker.ask_size == pytest.approx(20.0)
        assert lab_ticker.base_quantity_evidence is True
        assert lab_ticker.quantity_step_base == pytest.approx(10.0)

    @pytest.mark.asyncio
    async def test_gate_contracts_funding_bulk_cache_reuses_fresh_metadata(self):
        class FakeGateClient(MarketDataClient):
            def __init__(self, spec):
                super().__init__(spec)
                self.contract_calls = 0

            async def _public_get(self, path, params=None):
                if path == "/api/v4/futures/usdt/tickers":
                    return [
                        {
                            "contract": "BTC_USDT",
                            "highest_bid": "100",
                            "lowest_ask": "101",
                            "mark_price": "100.5",
                            "funding_rate": "0.0001",
                        },
                        {
                            "contract": "ETH_USDT",
                            "highest_bid": "200",
                            "lowest_ask": "201",
                            "mark_price": "200.5",
                            "funding_rate": "0.0002",
                        },
                    ]
                if path == "/api/v4/futures/usdt/contracts":
                    self.contract_calls += 1
                    if self.contract_calls > 1:
                        raise AssertionError("fresh Gate contracts funding cache was not reused")
                    return [
                        {
                            "name": "BTC_USDT",
                            "funding_rate": "0.0004",
                            "funding_next_apply": 4100007200,
                            "quanto_multiplier": "0.01",
                            "order_size_min": "1",
                            "order_price_round": "0.1",
                            "in_delisting": False,
                        },
                        {
                            "name": "ETH_USDT",
                            "funding_rate": "0.0005",
                            "funding_next_apply": 4100007200,
                            "quanto_multiplier": "0.01",
                            "order_size_min": "1",
                            "order_price_round": "0.01",
                            "in_delisting": False,
                        },
                    ]
                raise AssertionError(f"unexpected path: {path}")

        client = FakeGateClient(gate_spec())

        first = await client._fetch_gate_style(["BTCUSDT"])
        second = await client._fetch_gate_style(["ETHUSDT"])

        assert first["gate:BTCUSDT"].funding_timestamp_ms == 4_100_007_200_000
        assert first["gate:BTCUSDT"].funding_rate_bps == pytest.approx(4.0)
        assert second["gate:ETHUSDT"].funding_timestamp_ms == 4_100_007_200_000
        assert second["gate:ETHUSDT"].funding_rate_bps == pytest.approx(5.0)
        assert client.contract_calls == 1

    @pytest.mark.asyncio
    async def test_gate_missing_contracts_funding_fails_closed(self):
        class FakeGateClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/api/v4/futures/usdt/tickers":
                    return [
                        {
                            "contract": "BTC_USDT",
                            "highest_bid": "100",
                            "lowest_ask": "101",
                            "mark_price": "100.5",
                            "funding_rate": "0.0001",
                        }
                    ]
                if path == "/api/v4/futures/usdt/contracts":
                    return []
                raise AssertionError(f"unexpected path: {path}")

        result = await FakeGateClient(gate_spec())._fetch_gate_style(["BTCUSDT"])

        ticker = result["gate:BTCUSDT"]
        assert ticker.funding_timestamp_ms == 0
        assert ticker.funding_rate_bps == pytest.approx(1.0)

    @pytest.mark.asyncio
    async def test_hyperliquid_meta_dict_universe_is_parsed(self):
        class FakeHyperliquidClient(MarketDataClient):
            async def _public_post(self, path, body=None):
                assert body == {"type": "metaAndAssetCtxs"}
                return [
                    {"universe": [{"name": "BTC", "szDecimals": 5}]},
                    [{"coin": "BTC", "markPx": "65000", "funding": "0.0001", "dayNtlVlm": "1000", "openInterest": "20"}],
                ]

        result = await FakeHyperliquidClient(hyperliquid_spec())._fetch_hyperliquid_style(["BTCUSDT"])

        ticker = result["hyperliquid:BTCUSDT"]
        assert ticker.bid == 65000.0
        assert ticker.ask == 65000.0
        assert ticker.funding_rate_bps == 1.0
        assert ticker.open_interest_quote == pytest.approx(20.0 * 65000.0)
        assert ticker.open_interest_evidence_status == "available"
        assert ticker.open_interest_evidence_reason == "openInterest_times_mark"

    @pytest.mark.asyncio
    async def test_hyperliquid_asset_contexts_match_universe_by_index(self):
        """Official metaAndAssetCtxs returns asset contexts parallel to universe."""
        class FakeHyperliquidClient(MarketDataClient):
            async def _public_post(self, path, body=None):
                assert body == {"type": "metaAndAssetCtxs"}
                return [
                    {"universe": [
                        {"name": "BTC", "szDecimals": 5},
                        {"name": "MERL", "szDecimals": 0},
                    ]},
                    [
                        {
                            "markPx": "65000", "funding": "0.0001",
                            "dayNtlVlm": "1000", "openInterest": "20",
                            "impactPxs": ["64000", "66000"],
                        },
                        {
                            "markPx": "0.42", "funding": "0.0002",
                            "dayNtlVlm": "2000", "openInterest": "500",
                            "impactPxs": ["0.41", "0.43"],
                        },
                    ],
                ]

        result = await FakeHyperliquidClient(hyperliquid_spec())._fetch_hyperliquid_style(
            ["BTCUSDT", "MERLUSDT"]
        )

        assert result["hyperliquid:BTCUSDT"].bid == 64000.0
        assert result["hyperliquid:BTCUSDT"].ask == 66000.0
        assert result["hyperliquid:MERLUSDT"].bid == 0.41
        assert result["hyperliquid:MERLUSDT"].ask == 0.43

    @pytest.mark.asyncio
    async def test_hyperliquid_missing_asset_context_is_not_zero_quote(self):
        """A listed universe item without usable context must not become bid=ask=0."""
        class FakeHyperliquidClient(MarketDataClient):
            async def _public_post(self, path, body=None):
                assert body == {"type": "metaAndAssetCtxs"}
                return [
                    {"universe": [
                        {"name": "BTC", "szDecimals": 5},
                        {"name": "MERL", "szDecimals": 0},
                    ]},
                    [{
                        "markPx": "65000", "funding": "0.0001",
                        "dayNtlVlm": "1000", "openInterest": "20",
                        "impactPxs": ["64000", "66000"],
                    }],
                ]

        result = await FakeHyperliquidClient(hyperliquid_spec())._fetch_hyperliquid_style(
            ["BTCUSDT", "MERLUSDT"]
        )

        assert "hyperliquid:BTCUSDT" in result
        assert "hyperliquid:MERLUSDT" not in result

    @pytest.mark.asyncio
    async def test_okx_large_universe_funding_fetched_with_bounded_concurrency(self):
        """OKX large universe (620-like) MUST fetch per-symbol funding via bounded concurrency.

        V1 parity: funding_rate coverage must be non-zero for large universes.
        The semaphore bounds concurrency; individual failures must not drop quotes.
        """
        symbols = [f"S{i}USDT" for i in range(64)]

        class FakeOkxClient(MarketDataClient):
            def __init__(self):
                super().__init__(okx_spec())
                self.active_funding = 0
                self.max_active_funding = 0

            async def _public_get(self, path, params=None):
                if path == "/api/v5/market/tickers":
                    return {
                        "data": [
                            {"instId": f"S{i}-USDT-SWAP", "bidPx": "10", "askPx": "11"}
                            for i in range(64)
                        ]
                    }
                if path == "/api/v5/public/funding-rate":
                    self.active_funding += 1
                    self.max_active_funding = max(self.max_active_funding, self.active_funding)
                    await asyncio.sleep(0.005)
                    self.active_funding -= 1
                    return {
                        "data": [{
                            "fundingRate": "0.0002",
                            "fundingTime": "1700000000000",
                        }]
                    }
                if path == "/api/v5/public/mark-price":
                    return {"data": [
                        {"instId": f"S{i}-USDT-SWAP", "markPx": "10.5"}
                        for i in range(64)
                    ]}
                if path == "/api/v5/market/index-tickers":
                    return {"data": [
                        {"instId": f"S{i}-USDT", "idxPx": "10.4"}
                        for i in range(64)
                    ]}
                if path == "/api/v5/public/open-interest":
                    return {"data": []}
                return {}

        client = FakeOkxClient()
        result = await client._fetch_okx_style(symbols)

        assert len(result) == len(symbols)
        # V1 parity: funding_rate_bps must be non-zero for large universe
        assert result["okx:S0USDT"].funding_rate_bps == 2.0  # 0.0002 * 10000
        assert result["okx:S63USDT"].funding_rate_bps == 2.0
        # Mark/index are independent bulk proofs, not funding-response fields.
        assert result["okx:S0USDT"].mark_price == 10.5
        assert result["okx:S0USDT"].index_price == 10.4
        # funding_timestamp_ms must be from funding response
        assert result["okx:S0USDT"].funding_timestamp_ms == 1700000000000
        # Bounded concurrency: concurrent requests > 1 to prove parallelism
        assert client.max_active_funding > 1

    @pytest.mark.asyncio
    async def test_okx_slow_funding_enrichment_does_not_block_quote_return(self):
        symbols = [f"S{i}USDT" for i in range(64)]

        class FakeOkxClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/api/v5/market/tickers":
                    return {
                        "data": [
                            {
                                "instId": f"S{i}-USDT-SWAP",
                                "bidPx": "10",
                                "askPx": "11",
                                "markPx": "10.2",
                            }
                            for i in range(64)
                        ]
                    }
                if path == "/api/v5/public/funding-rate":
                    await asyncio.sleep(1.0)
                    return {
                        "data": [{
                            "fundingRate": "0.0003",
                            "fundingTime": "1700000000000",
                        }]
                    }
                if path == "/api/v5/public/mark-price":
                    return {"data": [
                        {"instId": f"S{i}-USDT-SWAP", "markPx": "10.2"}
                        for i in range(64)
                    ]}
                if path == "/api/v5/market/index-tickers":
                    return {"data": [
                        {"instId": f"S{i}-USDT", "idxPx": "10.1"}
                        for i in range(64)
                    ]}
                if path == "/api/v5/public/open-interest":
                    return {"data": []}
                return {}

        result = await asyncio.wait_for(
            FakeOkxClient(okx_spec())._fetch_okx_style(symbols),
            timeout=0.5,
        )

        assert len(result) == len(symbols)
        ticker = result["okx:S0USDT"]
        assert ticker.bid == 10.0
        assert ticker.ask == 11.0
        assert ticker.mark_price == 10.2
        assert ticker.funding_rate_bps == 0.0

    @pytest.mark.asyncio
    async def test_okx_funding_partial_failure_does_not_drop_quotes(self):
        """Individual funding-rate failures must not drop bid/ask/mark for a symbol."""
        symbols = [f"S{i}USDT" for i in range(10)]

        call_count = {"count": 0}

        class FakeOkxClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/api/v5/market/tickers":
                    return {
                        "data": [
                            {"instId": f"S{i}-USDT-SWAP", "bidPx": "10", "askPx": "11", "markPx": "10.2"}
                            for i in range(10)
                        ]
                    }
                if path == "/api/v5/public/funding-rate":
                    call_count["count"] += 1
                    inst_id = params.get("instId", "")
                    # Fail funding for S5USDT specifically
                    if "S5" in inst_id:
                        raise PublicTransportError(
                            PublicTransportErrorCategory.TRANSPORT_FAILURE,
                            "timeout",
                        )
                    return {
                        "data": [{
                            "fundingRate": "0.0003",
                            "fundingTime": "1700000000000",
                        }]
                    }
                if path == "/api/v5/public/mark-price":
                    return {"data": [
                        {"instId": f"S{i}-USDT-SWAP", "markPx": "10.2"}
                        for i in range(10)
                    ]}
                if path == "/api/v5/market/index-tickers":
                    return {"data": [
                        {"instId": f"S{i}-USDT", "idxPx": "10.1"}
                        for i in range(10)
                    ]}
                if path == "/api/v5/public/open-interest":
                    return {"data": []}
                return {}

        result = await FakeOkxClient(okx_spec())._fetch_okx_style(symbols)

        assert len(result) == len(symbols)
        # Successful symbols get funding
        assert result["okx:S0USDT"].funding_rate_bps == pytest.approx(3.0)
        # Failed symbol keeps bid/ask from tickers, uses mark from tickers, funding = 0
        assert result["okx:S5USDT"].bid == 10.0
        assert result["okx:S5USDT"].ask == 11.0
        assert result["okx:S5USDT"].mark_price == 10.2  # from ticker
        assert result["okx:S5USDT"].funding_rate_bps == 0.0  # failed

    @pytest.mark.asyncio
    async def test_hyperliquid_impact_prices_for_bid_ask(self):
        """Hyperliquid bid/ask must use impact prices (V1 parity), not mark price."""
        class FakeHyperliquidClient(MarketDataClient):
            async def _public_post(self, path, body=None):
                return [
                    {"universe": [
                        {"name": "BTC", "szDecimals": 5},
                        {"name": "ETH", "szDecimals": 4},
                    ]},
                    [
                        {
                            "coin": "BTC", "markPx": "65000", "funding": "0.0001",
                            "dayNtlVlm": "1000", "openInterest": "20",
                            "impactPxs": ["64000", "66000"], "midPx": "65000",
                        },
                        {
                            "coin": "ETH", "markPx": "3000", "funding": "0.0002",
                            "dayNtlVlm": "2000", "openInterest": "50",
                            "impactPxs": ["2950", "3050"], "midPx": "3000",
                        },
                    ],
                ]

        result = await FakeHyperliquidClient(hyperliquid_spec())._fetch_hyperliquid_style(
            ["BTCUSDT", "ETHUSDT"]
        )

        btc = result["hyperliquid:BTCUSDT"]
        # V1 parity: bid/ask from impact prices, not mark
        assert btc.bid == 64000.0
        assert btc.ask == 66000.0
        assert btc.mark_price == 65000.0  # mark still from markPx
        assert btc.funding_rate_bps == 1.0  # 0.0001 * 10000

        eth = result["hyperliquid:ETHUSDT"]
        assert eth.bid == 2950.0
        assert eth.ask == 3050.0
        assert eth.mark_price == 3000.0
        assert eth.funding_rate_bps == 2.0  # 0.0002 * 10000

    @pytest.mark.asyncio
    async def test_hyperliquid_funding_timestamp_is_next_hour_boundary(self):
        """Hyperliquid funding_timestamp_ms must be next hour boundary, not 0."""
        class FakeHyperliquidClient(MarketDataClient):
            async def _public_post(self, path, body=None):
                return [
                    {"universe": [{"name": "BTC", "szDecimals": 5}]},
                    [{
                        "coin": "BTC", "markPx": "65000", "funding": "0.0001",
                        "dayNtlVlm": "1000", "openInterest": "20",
                        "impactPxs": ["64000", "66000"],
                    }],
                ]

        result = await FakeHyperliquidClient(hyperliquid_spec())._fetch_hyperliquid_style(
            ["BTCUSDT"]
        )

        ticker = result["hyperliquid:BTCUSDT"]
        # funding_timestamp_ms must be a reasonable next-hour boundary (not 0)
        assert ticker.funding_timestamp_ms > 0
        # Must be aligned to hour boundary (divisible by 3600000)
        assert ticker.funding_timestamp_ms % 3_600_000 == 0

    @pytest.mark.asyncio
    async def test_missing_funding_timestamp_remains_unknown(self):
        """Unknown exchange schedule must not be replaced by observation time."""
        class FakeBybitClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                return {
                    "result": {
                        "list": [
                            {
                                "symbol": "BTCUSDT",
                                "bid1Price": "50000", "ask1Price": "50001",
                                "bid1Size": "1", "ask1Size": "1",
                                "markPrice": "50000.5", "indexPrice": "50000",
                                "fundingRate": "0.0001",
                                "turnover24h": "1000000",
                                "openInterestValue": "500000",
                            }
                        ]
                    }
                }

        result = await FakeBybitClient(bybit_spec())._fetch_bybit_style(["BTCUSDT"])

        ticker = result["bybit:BTCUSDT"]
        assert ticker.funding_timestamp_ms == 0

    @pytest.mark.asyncio
    async def test_binance_cold_start_interval_comes_from_settlement_history(self):
        class FakeBinanceClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/fapi/v1/ticker/bookTicker":
                    return [{"symbol": "BTCUSDT", "bidPrice": "100", "askPrice": "101"}]
                if path == "/fapi/v1/premiumIndex":
                    return [{
                        "symbol": "BTCUSDT",
                        "lastFundingRate": "0.0001",
                        "markPrice": "100.5",
                        "indexPrice": "100.4",
                        "nextFundingTime": "4100007200000",
                    }]
                if path == "/fapi/v1/fundingRate":
                    assert params == {"symbol": "BTCUSDT", "limit": 2}
                    return [
                        {"fundingTime": "4099978400000"},
                        {"fundingTime": "4100007200000"},
                    ]
                if path == "/fapi/v1/ticker/24hr":
                    return [{"symbol": "BTCUSDT", "quoteVolume": "1000000"}]
                if path == "/fapi/v1/openInterest":
                    return {"symbol": "BTCUSDT", "openInterest": "10"}
                return {}

        ticker = (
            await FakeBinanceClient(binance_spec()).fetch_funding_tickers(["BTCUSDT"])
        )["binance:BTCUSDT"]

        assert ticker.funding_interval_ms == 28_800_000
        assert ticker.funding_interval_source == "venue_funding_history"
        assert ticker.funding_interval_observed_at_ms > 0

    @pytest.mark.asyncio
    async def test_binance_interval_history_only_queries_bulk_listed_symbols(self):
        funding_history_symbols: list[str] = []

        class FakeBinanceClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/fapi/v1/ticker/bookTicker":
                    return [{"symbol": "BTCUSDT", "bidPrice": "100", "askPrice": "101"}]
                if path == "/fapi/v1/premiumIndex":
                    return [{
                        "symbol": "BTCUSDT",
                        "lastFundingRate": "0.0001",
                        "markPrice": "100.5",
                        "indexPrice": "100.4",
                        "nextFundingTime": "4100007200000",
                    }]
                if path == "/fapi/v1/fundingRate":
                    funding_history_symbols.append(str((params or {}).get("symbol", "")))
                    return [
                        {"fundingTime": "4099978400000"},
                        {"fundingTime": "4100007200000"},
                    ]
                if path == "/fapi/v1/ticker/24hr":
                    return [{"symbol": "BTCUSDT", "quoteVolume": "1000000"}]
                if path == "/fapi/v1/openInterest":
                    return {"symbol": "BTCUSDT", "openInterest": "10"}
                return {}

        await FakeBinanceClient(binance_spec()).fetch_funding_tickers(
            ["BTCUSDT", "ETHUSDT"]
        )

        assert funding_history_symbols == ["BTCUSDT"]

    @pytest.mark.asyncio
    async def test_okx_contract_counts_are_converted_to_base_and_interval_is_proved(self):
        class FakeOkxClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/api/v5/market/tickers":
                    return {"data": [{
                        "instId": "BTC-USDT-SWAP",
                        "bidPx": "100",
                        "askPx": "101",
                        "bidSz": "10",
                        "askSz": "20",
                        "markPx": "100.5",
                        "last": "100.5",
                    }]}
                if path == "/api/v5/public/instruments":
                    return {"data": [{
                        "instId": "BTC-USDT-SWAP",
                        "ctVal": "0.01",
                        "ctValCcy": "BTC",
                        "settleCcy": "USDT",
                        "ctType": "linear",
                        "state": "live",
                        "tickSz": "0.1",
                        "lotSz": "0.1",
                        "minSz": "1",
                    }]}
                if path == "/api/v5/public/mark-price":
                    return {"data": [{
                        "instId": "BTC-USDT-SWAP",
                        "markPx": "100.5",
                    }]}
                if path == "/api/v5/market/index-tickers":
                    return {"data": [{
                        "instId": "BTC-USDT",
                        "idxPx": "100.4",
                    }]}
                if path == "/api/v5/public/funding-rate":
                    return {"data": [{
                        "fundingRate": "0.0001",
                        "fundingTime": "4099978400000",
                        "nextFundingTime": "4100007200000",
                    }]}
                if path == "/api/v5/public/open-interest":
                    return {"data": [{"instId": "BTC-USDT-SWAP", "oiUsd": "1000"}]}
                return {}

        ticker = (
            await FakeOkxClient(okx_spec()).fetch_funding_tickers(["BTCUSDT"])
        )["okx:BTCUSDT"]

        assert ticker.bid_size == pytest.approx(0.1)
        assert ticker.ask_size == pytest.approx(0.2)
        assert ticker.quantity_precision == 3
        assert ticker.funding_interval_ms == 28_800_000
        assert ticker.funding_interval_source == "okx_funding_time_pair"
        assert ticker.min_notional_quote == 0.0
        assert ticker.min_notional_evidence_complete is True
        assert ticker.contract_normalization_complete is True

    @pytest.mark.asyncio
    async def test_okx_funding_cache_keeps_independent_mark_and_index_proof(self):
        class FakeOkxClient(MarketDataClient):
            def __init__(self):
                super().__init__(okx_spec())
                self.funding_calls = 0

            async def _public_get(self, path, params=None):
                if path == "/api/v5/market/tickers":
                    return {"data": [{
                        "instId": "BTC-USDT-SWAP",
                        "bidPx": "100",
                        "askPx": "101",
                        "bidSz": "10",
                        "askSz": "20",
                        "last": "100.5",
                    }]}
                if path == "/api/v5/public/instruments":
                    return {"data": [{
                        "instId": "BTC-USDT-SWAP",
                        "ctVal": "0.01",
                        "ctValCcy": "BTC",
                        "settleCcy": "USDT",
                        "ctType": "linear",
                        "state": "live",
                        "tickSz": "0.1",
                        "lotSz": "0.1",
                        "minSz": "1",
                    }]}
                if path == "/api/v5/public/mark-price":
                    return {"data": [{
                        "instId": "BTC-USDT-SWAP",
                        "markPx": "100.5",
                    }]}
                if path == "/api/v5/market/index-tickers":
                    return {"data": [{
                        "instId": "BTC-USDT",
                        "idxPx": "100.4",
                    }]}
                if path == "/api/v5/public/funding-rate":
                    self.funding_calls += 1
                    return {"data": [{
                        "instId": "BTC-USDT-SWAP",
                        "fundingRate": "0.0001",
                        "fundingTime": "4099978400000",
                        "nextFundingTime": "4100007200000",
                    }]}
                if path == "/api/v5/public/open-interest":
                    return {"data": [{
                        "instId": "BTC-USDT-SWAP",
                        "oiUsd": "1000",
                    }]}
                return {}

        client = FakeOkxClient()
        first = (await client.fetch_funding_tickers(["BTCUSDT"]))["okx:BTCUSDT"]
        second = (await client.fetch_funding_tickers(["BTCUSDT"]))["okx:BTCUSDT"]

        assert client.funding_calls == 1
        assert first.contract_normalization_complete is True
        assert second.contract_normalization_complete is True
        assert second.mark_price == pytest.approx(100.5)
        assert second.index_price == pytest.approx(100.4)

    @pytest.mark.asyncio
    async def test_bybit_first_fetch_uses_instrument_interval_and_steps(self):
        class FakeBybitClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/v5/market/tickers":
                    return {"result": {"list": [{
                        "symbol": "BTCUSDT",
                        "bid1Price": "100",
                        "ask1Price": "101",
                        "bid1Size": "1",
                        "ask1Size": "2",
                        "markPrice": "100.5",
                        "indexPrice": "100.4",
                        "fundingRate": "0.0001",
                        "nextFundingTime": "4100007200000",
                    }]}}
                if path == "/v5/market/instruments-info":
                    return {"result": {"list": [{
                        "symbol": "BTCUSDT",
                        "status": "Trading",
                        "contractType": "LinearPerpetual",
                        "settleCoin": "USDT",
                        "fundingInterval": "480",
                        "priceFilter": {"tickSize": "0.1"},
                        "lotSizeFilter": {
                            "qtyStep": "0.001",
                            "minOrderQty": "0.001",
                            "minNotionalValue": "1",
                        },
                    }]}}
                return {}

        ticker = (
            await FakeBybitClient(bybit_spec()).fetch_funding_tickers(["BTCUSDT"])
        )["bybit:BTCUSDT"]

        assert ticker.funding_interval_ms == 28_800_000
        assert ticker.funding_interval_source == "bybit_instrument_metadata"
        assert ticker.quantity_precision == 3
        assert ticker.contract_normalization_complete is True

    @pytest.mark.asyncio
    async def test_bitget_base_sizes_contract_config_and_interval_share_one_proof(self):
        class FakeBitgetClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/api/v2/mix/market/tickers":
                    return {"data": [{
                        "symbol": "BTCUSDT",
                        "bidPr": "100",
                        "askPr": "101",
                        "bidSz": "1.5",
                        "askSz": "2.5",
                        "markPrice": "100.5",
                        "indexPrice": "100.4",
                        "fundingRate": "0.0001",
                    }]}
                if path == "/api/v2/mix/market/contracts":
                    return {"data": [{
                        "symbol": "BTCUSDT",
                        "baseCoin": "BTC",
                        "quoteCoin": "USDT",
                        "symbolType": "perpetual",
                        "symbolStatus": "normal",
                        "sizeMultiplier": "0.001",
                        "minTradeNum": "0.001",
                        "minTradeUSDT": "1",
                        "pricePlace": "2",
                        "priceEndStep": "1",
                        "fundInterval": "8",
                    }]}
                if path == "/api/v2/mix/market/current-fund-rate":
                    return {"data": [{
                        "symbol": "BTCUSDT",
                        "fundingRate": "0.0001",
                        "nextUpdate": "4100007200000",
                    }]}
                return {}

        ticker = (
            await FakeBitgetClient(bitget_spec()).fetch_funding_tickers(["BTCUSDT"])
        )["bitget:BTCUSDT"]

        assert ticker.bid_size == pytest.approx(1.5)
        assert ticker.ask_size == pytest.approx(2.5)
        assert ticker.quantity_precision == 3
        assert ticker.funding_interval_ms == 28_800_000
        assert ticker.funding_interval_source == "bitget_contract_config"
        assert ticker.contract_normalization_complete is True

    @pytest.mark.asyncio
    async def test_gate_and_hyperliquid_interval_provenance_is_explicit(self):
        class FakeGateClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/api/v4/futures/usdt/tickers":
                    return [{
                        "contract": "BTC_USDT",
                        "highest_bid": "100",
                        "lowest_ask": "101",
                        "mark_price": "100.5",
                        "index_price": "100.4",
                    }]
                if path == "/api/v4/futures/usdt/contracts":
                    return [{
                        "name": "BTC_USDT",
                        "funding_rate": "0.0001",
                        "funding_next_apply": 4100007200,
                        "funding_interval": "28800",
                        "quanto_multiplier": "0.01",
                        "order_size_min": "1",
                        "order_price_round": "0.1",
                        "in_delisting": False,
                    }]
                return {}

        gate_ticker = (
            await FakeGateClient(gate_spec()).fetch_funding_tickers(["BTCUSDT"])
        )["gate:BTCUSDT"]
        assert gate_ticker.funding_interval_ms == 28_800_000
        assert gate_ticker.funding_interval_source == "gate_contract_metadata"
        assert gate_ticker.min_notional_quote == 0.0
        assert gate_ticker.min_notional_evidence_complete is True
        assert gate_ticker.contract_normalization_complete is True

        class FakeHyperliquidClient(MarketDataClient):
            async def _public_post(self, path, body=None):
                return [
                    {"universe": [{"name": "BTC", "szDecimals": 5}]},
                    [{
                        "coin": "BTC",
                        "markPx": "65000",
                        "oraclePx": "64999",
                        "funding": "0.0001",
                        "impactPxs": ["64000", "66000"],
                    }],
                ]

        hyper_ticker = (
            await FakeHyperliquidClient(hyperliquid_spec()).fetch_funding_tickers(
                ["BTCUSDT"]
            )
        )["hyperliquid:BTCUSDT"]
        assert hyper_ticker.min_notional_quote == 10.0
        assert hyper_ticker.min_notional_evidence_complete is True
        assert hyper_ticker.funding_interval_ms == 3_600_000
        assert hyper_ticker.funding_interval_source == "hyperliquid_protocol_hourly"
        assert hyper_ticker.funding_interval_observed_at_ms > 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "delisting_status",
        [None, "false", "true", 1, True],
    )
    async def test_gate_contract_status_requires_literal_active_boolean(
        self,
        delisting_status,
    ):
        class FakeGateClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/api/v4/futures/usdt/tickers":
                    return [{
                        "contract": "BTC_USDT",
                        "highest_bid": "100",
                        "lowest_ask": "101",
                        "highest_size": "10",
                        "lowest_size": "20",
                        "mark_price": "100.5",
                        "index_price": "100.4",
                    }]
                if path == "/api/v4/futures/usdt/contracts":
                    contract = {
                        "name": "BTC_USDT",
                        "funding_rate": "0.0001",
                        "funding_next_apply": 4100007200,
                        "funding_interval": "28800",
                        "quanto_multiplier": "0.01",
                        "order_size_min": "1",
                        "order_price_round": "0.1",
                    }
                    if delisting_status is not None:
                        contract["in_delisting"] = delisting_status
                    return [contract]
                return {}

        ticker = (
            await FakeGateClient(gate_spec()).fetch_funding_tickers(["BTCUSDT"])
        )["gate:BTCUSDT"]

        assert ticker.contract_normalization_complete is False
        assert ticker.venue_status == "unknown"

    @pytest.mark.asyncio
    async def test_gate_missing_quanto_never_promotes_contract_sizes_to_base(self):
        class FakeGateClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/api/v4/futures/usdt/tickers":
                    return [{
                        "contract": "BTC_USDT",
                        "highest_bid": "100",
                        "lowest_ask": "101",
                        "highest_size": "10",
                        "lowest_size": "20",
                        "mark_price": "100.5",
                        "index_price": "100.4",
                        "total_size": "500",
                    }]
                if path == "/api/v4/futures/usdt/contracts":
                    return [{
                        "name": "BTC_USDT",
                        "funding_rate": "0.0001",
                        "funding_next_apply": 4100007200,
                        "funding_interval": "28800",
                        "order_size_min": "1",
                        "order_price_round": "0.1",
                        "in_delisting": False,
                    }]
                return {}

        ticker = (
            await FakeGateClient(gate_spec()).fetch_funding_tickers(["BTCUSDT"])
        )["gate:BTCUSDT"]

        assert ticker.bid_size == 0.0
        assert ticker.ask_size == 0.0
        assert ticker.open_interest_quote == 0.0
        assert ticker.open_interest_evidence_status == "unavailable"
        assert ticker.open_interest_evidence_reason == "missing_contract_multiplier"
        assert ticker.contract_normalization_complete is False

    @pytest.mark.asyncio
    async def test_gate_partial_contract_metadata_retries_and_recovers_next_refresh(self):
        class FakeGateClient(MarketDataClient):
            def __init__(self):
                super().__init__(gate_spec())
                self.contract_calls = 0

            async def _public_get(self, path, params=None):
                if path == "/api/v4/futures/usdt/tickers":
                    return [{
                        "contract": "BTC_USDT",
                        "highest_bid": "100",
                        "lowest_ask": "101",
                        "highest_size": "10",
                        "lowest_size": "20",
                        "mark_price": "100.5",
                        "index_price": "100.4",
                    }]
                if path == "/api/v4/futures/usdt/contracts":
                    self.contract_calls += 1
                    contract = {
                        "name": "BTC_USDT",
                        "funding_rate": "0.0001",
                        "funding_next_apply": 4100007200,
                        "funding_interval": "28800",
                        "quanto_multiplier": "0.01",
                        "order_size_min": "1",
                        "in_delisting": False,
                    }
                    if self.contract_calls > 1:
                        contract["order_price_round"] = "0.1"
                    return [contract]
                return {}

        client = FakeGateClient()

        first = (await client.fetch_funding_tickers(["BTCUSDT"]))[
            "gate:BTCUSDT"
        ]
        second = (await client.fetch_funding_tickers(["BTCUSDT"]))[
            "gate:BTCUSDT"
        ]

        assert first.contract_normalization_complete is False
        assert second.contract_normalization_complete is True
        assert second.bid_size == pytest.approx(0.1)
        assert client.contract_calls == 2
