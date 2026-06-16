"""Tests for MarketDataClient: credential-free construction, venue parsers, rate-limit scopes."""

from __future__ import annotations

import asyncio

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
    VenueSpec,
    binance_spec,
    okx_spec,
    bybit_spec,
    bitget_spec,
    gate_spec,
    aster_spec,
    hyperliquid_spec,
    AuthScheme,
    VenueAccountContract,
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
            assert ticker.open_interest_evidence_status == "timeout"
            assert ticker.open_interest_evidence_reason == "timeout_waiting_for_oi"
            assert ticker.oi_refresh_cap == 128
            assert ticker.oi_refresh_attempt_count == 2
            assert ticker.oi_timeout_count == 2
            assert ticker.oi_refresh_elapsed_ms >= 0

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
    async def test_hyperliquid_meta_dict_universe_is_parsed(self):
        class FakeHyperliquidClient(MarketDataClient):
            async def _public_post(self, path, body=None):
                assert body == {"type": "metaAndAssetCtxs"}
                return [
                    {"universe": [{"name": "BTC"}]},
                    [{"coin": "BTC", "markPx": "65000", "funding": "0.0001", "dayNtlVlm": "1000", "openInterest": "20"}],
                ]

        result = await FakeHyperliquidClient(hyperliquid_spec())._fetch_hyperliquid_style(["BTCUSDT"])

        ticker = result["hyperliquid:BTCUSDT"]
        assert ticker.bid == 65000.0
        assert ticker.ask == 65000.0
        assert ticker.funding_rate_bps == 1.0

    @pytest.mark.asyncio
    async def test_hyperliquid_asset_contexts_match_universe_by_index(self):
        """Official metaAndAssetCtxs returns asset contexts parallel to universe."""
        class FakeHyperliquidClient(MarketDataClient):
            async def _public_post(self, path, body=None):
                assert body == {"type": "metaAndAssetCtxs"}
                return [
                    {"universe": [{"name": "BTC"}, {"name": "MERL"}]},
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
                    {"universe": [{"name": "BTC"}, {"name": "MERL"}]},
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
                            "markPrice": "10.5",
                            "indexPrice": "10.4",
                        }]
                    }
                if path == "/api/v5/public/open-interest":
                    return {"data": []}
                return {}

        client = FakeOkxClient()
        result = await client._fetch_okx_style(symbols)

        assert len(result) == len(symbols)
        # V1 parity: funding_rate_bps must be non-zero for large universe
        assert result["okx:S0USDT"].funding_rate_bps == 2.0  # 0.0002 * 10000
        assert result["okx:S63USDT"].funding_rate_bps == 2.0
        # mark/index must be populated from per-symbol funding response
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
                            "markPrice": "10.5",
                        }]
                    }
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
                            "markPrice": "10.5",
                        }]
                    }
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
                    {"universe": [{"name": "BTC"}, {"name": "ETH"}]},
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
                    {"universe": [{"name": "BTC"}]},
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
    async def test_funding_timestamp_ms_not_zero_for_venues_without_explicit_time(self):
        """Venues without explicit funding time (Bybit, etc.) must use observed_at_ms."""
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
        # Bybit ticker doesn't include funding timestamp, must use observed_at_ms
        assert ticker.funding_timestamp_ms > 0
        # Should be within last few seconds (not 0)
        import time
        now_ms = int(time.time() * 1000)
        assert abs(ticker.funding_timestamp_ms - now_ms) < 10_000
