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


class TestLiquiditySource:
    """LiquiditySource wraps MarketDataClient's fetch_perp_liquidity."""

    def test_construct(self):
        src = LiquiditySource.for_venue(Venue.OKX)
        assert src.venue == "okx"

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

    def test_close(self):
        async def _run():
            src = TransferSource.for_venue_pair(Venue.BINANCE, Venue.OKX)
            await src.close()
        asyncio.run(_run())
