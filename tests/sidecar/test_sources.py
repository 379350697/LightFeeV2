"""Tests for ExchangeSource, LiquiditySource, TransferSource backing by MarketDataClient."""

from __future__ import annotations

import asyncio

import pytest

from lightfee.core.domain import Venue
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.sidecar.sources.exchange import ExchangeSource
from lightfee.sidecar.sources.liquidity import LiquiditySource
from lightfee.sidecar.sources.transfer import TransferSource
from lightfee.venues.specs import binance_spec, okx_spec, get_spec


class TestExchangeSource:
    """ExchangeSource wraps MarketDataClient to output QuoteSnapshot."""

    def test_construct_from_spec(self):
        spec = binance_spec()
        src = ExchangeSource(spec)
        assert src.venue == "binance"

    def test_construct_for_venue(self):
        src = ExchangeSource.for_venue(Venue.BINANCE)
        assert src.venue == "binance"

    def test_close(self):
        async def _run():
            src = ExchangeSource(binance_spec())
            await src.close()
        asyncio.run(_run())

    def test_from_funding_ticker(self):
        from lightfee.venues.market_data import FundingTicker
        ft = FundingTicker(
            venue="binance", symbol="BTCUSDT",
            bid=50000, ask=50001, bid_size=1.5, ask_size=2.0,
            mark_price=50000.5, index_price=50000.0,
            funding_rate_bps=10.0, funding_timestamp_ms=1700000000000,
            volume_24h_quote=1_000_000.0, open_interest_quote=500_000.0,
        )
        qs = ExchangeSource._from_funding_ticker(ft)
        assert isinstance(qs, QuoteSnapshot)
        assert qs.bid == 50000
        assert qs.funding_rate_bps == 10.0
        assert qs.open_interest == 500_000.0
        assert qs.index_price == 50000.0

    def test_funding_ticker_no_credential_required(self):
        """Critical: ExchangeSource must not require LiveCredential."""
        src = ExchangeSource.for_venue(Venue.BINANCE)
        assert src.venue == "binance"
        # No credential validation should occur

    def test_accepts_shared_public_rate_limiter(self):
        from lightfee.venues.transport import EndpointRateLimiter

        limiter = EndpointRateLimiter(1000, 8000, 50)
        src = ExchangeSource(binance_spec(), rate_limiter=limiter)

        assert src._client._rate_limiter is limiter


class TestLiquiditySource:
    """LiquiditySource wraps MarketDataClient's fetch_perp_liquidity."""

    def test_construct(self):
        src = LiquiditySource.for_venue(Venue.OKX)
        assert src.venue == "okx"

    def test_accepts_shared_public_rate_limiter(self):
        from lightfee.venues.transport import EndpointRateLimiter

        limiter = EndpointRateLimiter(1000, 8000, 50)
        src = LiquiditySource(okx_spec(), rate_limiter=limiter)

        assert src._client._rate_limiter is limiter

    def test_close(self):
        async def _run():
            src = LiquiditySource(okx_spec())
            await src.close()
        asyncio.run(_run())


class TestTransferSource:
    """TransferSource returns compatible empty results, not sentinel."""

    def test_construct(self):
        src = TransferSource.for_venue_pair(Venue.BINANCE, Venue.OKX)
        assert src.from_venue == "binance"
        assert src.to_venue == "okx"

    def test_fetch_transfer_statuses_returns_list(self):
        async def _run():
            src = TransferSource.for_venue_pair(Venue.BINANCE, Venue.OKX)
            results = await src.fetch_transfer_statuses(["USDT"])
            assert isinstance(results, list)
            assert len(results) == 1
            assert results[0].asset == "USDT"
            assert results[0].available == 0.0
            await src.close()
        asyncio.run(_run())


class TestSidecarServiceRateLimitWiring:
    def test_service_shares_public_rate_limiter_per_venue(self):
        from lightfee.config.schema import AppConfig, RuntimeConfig, VenueConfig
        from lightfee.sidecar.service import SidecarService

        config = AppConfig(
            runtime=RuntimeConfig(sidecar_snapshot_path="/tmp/unused-sidecar.json"),
            venues=[VenueConfig(venue="binance"), VenueConfig(venue="aster")],
        )

        service = SidecarService(config)

        binance_limiter = service._exchange_sources["binance"]._client._rate_limiter
        aster_limiter = service._exchange_sources["aster"]._client._rate_limiter

        assert binance_limiter is service._liquidity_sources["binance"]._client._rate_limiter
        assert aster_limiter is service._liquidity_sources["aster"]._client._rate_limiter
        assert binance_limiter is not aster_limiter
        assert binance_limiter is not None
        assert aster_limiter is not None

    def test_close(self):
        async def _run():
            src = TransferSource.for_venue_pair(Venue.BINANCE, Venue.OKX)
            await src.close()
        asyncio.run(_run())

    @pytest.mark.asyncio
    async def test_refresh_once_keeps_binance_quotes_when_open_interest_is_slow(self, tmp_path):
        from lightfee.config.schema import AppConfig, RuntimeConfig, VenueConfig
        from lightfee.sidecar.service import SidecarService
        from lightfee.venues.market_data import MarketDataClient

        class FakeBinanceClient(MarketDataClient):
            async def _public_get(self, path, params=None):
                if path == "/fapi/v1/ticker/bookTicker":
                    return [{"symbol": "BTCUSDT", "bidPrice": "100", "askPrice": "101"}]
                if path == "/fapi/v1/premiumIndex":
                    return [{"symbol": "BTCUSDT", "lastFundingRate": "0.0001", "markPrice": "100.5"}]
                if path == "/fapi/v1/ticker/24hr":
                    return [{"symbol": "BTCUSDT", "quoteVolume": "12345"}]
                if path == "/fapi/v1/openInterest":
                    await asyncio.sleep(1.0)
                    return {"symbol": params["symbol"], "openInterest": "2500"}
                return {}

        class FakeLiquiditySource:
            async def fetch_perp_liquidity(self, symbols):
                return {}

            async def close(self):
                return None

        config = AppConfig(
            symbols=["BTCUSDT"],
            runtime=RuntimeConfig(
                sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
                sidecar_funding_timeout_s=0.2,
            ),
            venues=[VenueConfig(venue="binance")],
        )
        service = SidecarService(config)
        service._exchange_sources["binance"]._client = FakeBinanceClient(binance_spec())
        service._liquidity_sources["binance"] = FakeLiquiditySource()

        try:
            snapshot = await service.refresh_once()
        finally:
            await service.close()

        assert snapshot.degraded_venues == []
        assert snapshot.acquisition_mode == "fresh_sidecar"
        quote = snapshot.quotes["binance:BTCUSDT"]
        assert quote.bid == 100.0
        assert quote.ask == 101.0
        assert quote.open_interest == 0.0
        assert quote.open_interest_evidence_status == "timeout"
        assert quote.open_interest_evidence_reason == "timeout_waiting_for_oi"
        assert quote.oi_refresh_attempt_count == 1
        assert quote.oi_timeout_count == 1
