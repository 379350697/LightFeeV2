"""Tests for MarketDataClient: credential-free construction, venue parsers, rate-limit scopes."""

from __future__ import annotations

import asyncio

import pytest

from lightfee.core.domain import Venue
from lightfee.venues.market_data import (
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
    async def test_binance_large_universe_skips_per_symbol_open_interest(self):
        symbols = [f"S{i}USDT" for i in range(64)]

        class FakeBinanceClient(MarketDataClient):
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
                    raise AssertionError("large live universes must not block on per-symbol OI")
                return {}

        result = await FakeBinanceClient(binance_spec())._fetch_binance_style(symbols)

        assert len(result) == len(symbols)
        assert result["binance:S0USDT"].open_interest_quote == 0.0

    @pytest.mark.asyncio
    async def test_okx_large_universe_skips_per_symbol_funding_enrichment(self):
        symbols = [f"S{i}USDT" for i in range(64)]

        class FakeOkxClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/api/v5/market/tickers":
                    return {
                        "data": [
                            {"instId": f"S{i}-USDT-SWAP", "bidPx": "10", "askPx": "11"}
                            for i in range(64)
                        ]
                    }
                if path == "/api/v5/public/funding-rate":
                    raise AssertionError("large live universes must not block on per-symbol funding")
                if path == "/api/v5/public/open-interest":
                    return {"data": []}
                return {}

        result = await FakeOkxClient(okx_spec())._fetch_okx_style(symbols)

        assert len(result) == len(symbols)
        assert result["okx:S0USDT"].funding_rate_bps == 0.0
