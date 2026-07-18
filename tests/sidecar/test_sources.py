"""Tests for ExchangeSource and LiquiditySource backing by MarketDataClient."""

from __future__ import annotations

import asyncio
import time

import pytest

from lightfee.core.domain import Venue
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.sidecar.sources.exchange import ExchangeSource
from lightfee.sidecar.sources.liquidity import LiquiditySource
from lightfee.venues.market_data import FundingTicker
from lightfee.venues.specs import binance_spec, okx_spec


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
            venue="binance",
            symbol="BTCUSDT",
            bid=50000,
            ask=50001,
            bid_size=1.5,
            ask_size=2.0,
            mark_price=50000.5,
            index_price=50000.0,
            funding_rate_bps=10.0,
            funding_timestamp_ms=1700000000000,
            volume_24h_quote=1_000_000.0,
            open_interest_quote=500_000.0,
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

    @pytest.mark.asyncio
    async def test_failed_final_bbo_cannot_reuse_aged_funding_ticker_price(self):
        class FakeClient:
            async def fetch_funding_tickers(self, symbols, *, include_open_interest):
                return {
                    "binance:BTCUSDT": FundingTicker(
                        venue="binance",
                        symbol="BTCUSDT",
                        bid=100.0,
                        ask=101.0,
                        market_received_at_ms=123,
                        funding_rate_bps=50.0,
                    )
                }

            async def fetch_top_book_quotes(self, symbols):
                raise TimeoutError("final BBO unavailable")

        src = object.__new__(ExchangeSource)
        src.venue = "binance"
        src._client = FakeClient()

        quotes = await src.fetch_market_quotes(["BTCUSDT"])
        quote = quotes["binance:BTCUSDT"]

        assert quote.bid == 0.0
        assert quote.ask == 0.0
        assert quote.observed_at_ms == 0
        assert quote.source == "sidecar_bbo_unavailable"

    @pytest.mark.asyncio
    async def test_partial_final_bbo_invalidates_only_missing_symbol(self):
        class FakeClient:
            async def fetch_funding_tickers(self, symbols, *, include_open_interest):
                return {
                    f"binance:{symbol}": FundingTicker(
                        venue="binance",
                        symbol=symbol,
                        bid=100.0,
                        ask=101.0,
                        market_received_at_ms=123,
                    )
                    for symbol in symbols
                }

            async def fetch_top_book_quotes(self, symbols):
                from lightfee.marketdata.ws_bbo import TopBookQuote

                return {
                    "binance:BTCUSDT": TopBookQuote(
                        venue="binance",
                        symbol="BTCUSDT",
                        bid=200.0,
                        ask=201.0,
                        received_at_ms=456,
                    )
                }

        src = object.__new__(ExchangeSource)
        src.venue = "binance"
        src._client = FakeClient()

        quotes = await src.fetch_market_quotes(["BTCUSDT", "ETHUSDT"])

        assert quotes["binance:BTCUSDT"].bid == 200.0
        assert quotes["binance:BTCUSDT"].observed_at_ms == 456
        assert quotes["binance:ETHUSDT"].bid == 0.0
        assert quotes["binance:ETHUSDT"].source == "sidecar_bbo_unavailable"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["symbol_not_listed", "ambiguous_mapping"])
    async def test_known_unlisted_or_ambiguous_placeholder_is_not_a_degraded_quote(
        self,
        status,
    ):
        class FakeClient:
            async def fetch_funding_tickers(self, symbols, *, include_open_interest):
                return {
                    "binance:BTCUSDT": FundingTicker(
                        venue="binance",
                        symbol="BTCUSDT",
                        bid=100.0,
                        ask=101.0,
                        market_received_at_ms=123,
                    ),
                    "binance:NOTLISTEDUSDT": FundingTicker(
                        venue="binance",
                        symbol="NOTLISTEDUSDT",
                        bid=0.0,
                        ask=0.0,
                        open_interest_evidence_status=status,
                    ),
                }

            async def fetch_top_book_quotes(self, symbols):
                from lightfee.marketdata.ws_bbo import TopBookQuote

                return {
                    "binance:BTCUSDT": TopBookQuote(
                        venue="binance",
                        symbol="BTCUSDT",
                        bid=200.0,
                        ask=201.0,
                        received_at_ms=456,
                    )
                }

        src = object.__new__(ExchangeSource)
        src.venue = "binance"
        src._client = FakeClient()

        quotes = await src.fetch_market_quotes(["BTCUSDT", "NOTLISTEDUSDT"])

        assert set(quotes) == {"binance:BTCUSDT"}


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
                    return [
                        {
                            "symbol": "BTCUSDT",
                            "lastFundingRate": "0.0001",
                            "markPrice": "100.5",
                            "nextFundingTime": "2000000000000",
                        }
                    ]
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
        service._exchange_sources["binance"]._client._funding_interval_by_key[
            "binance:BTCUSDT"
        ] = (28_800_000, "test_fixture", int(time.time() * 1_000))
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
        assert quote.open_interest is None
        assert quote.open_interest_evidence_status == "unavailable"
        assert quote.open_interest_evidence_reason == "entry_targeted_revalidation_required"
        assert quote.oi_refresh_attempt_count == 0
        assert quote.oi_timeout_count == 0

    @pytest.mark.asyncio
    async def test_refresh_build_watermark_includes_quotes_received_during_refresh(
        self, tmp_path, monkeypatch
    ):
        from lightfee.config.schema import AppConfig, RuntimeConfig, VenueConfig
        from lightfee.sidecar.service import SidecarService

        config = AppConfig(
            symbols=["BTCUSDT"],
            runtime=RuntimeConfig(
                sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            ),
            venues=[
                VenueConfig(venue="binance", taker_fee_bps=1.0),
                VenueConfig(venue="bybit", taker_fee_bps=1.0),
            ],
        )
        service = SidecarService(config)
        quotes = {
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=100.1,
                bid_size=10.0,
                ask_size=10.0,
                funding_rate_bps=1.0,
                funding_rate_observed_at_ms=10_100,
                funding_rate_received_at_ms=10_100,
                funding_rate_source="test_fixture",
                funding_rate_sample_id="funding:binance:BTCUSDT:10100:1:20000",
                funding_timestamp_ms=20_000,
                funding_interval_ms=28_800_000,
                observed_at_ms=10_100,
                underlying="BTC",
                quote_currency="USDT",
                contract_type="linear",
                contract_multiplier=1.0,
                mark_index_source="test_fixture",
                price_precision=2,
                quantity_precision=3,
                price_tick=0.01,
                quantity_step_base=0.001,
                min_quantity_base=0.001,
                min_notional_quote=1.0,
                min_notional_evidence_complete=True,
                venue_status="active",
                contract_normalization_complete=True,
            ),
            "bybit:BTCUSDT": QuoteSnapshot(
                venue="bybit",
                symbol="BTCUSDT",
                bid=100.2,
                ask=100.3,
                bid_size=10.0,
                ask_size=10.0,
                funding_rate_bps=10.0,
                funding_rate_observed_at_ms=10_200,
                funding_rate_received_at_ms=10_200,
                funding_rate_source="test_fixture",
                funding_rate_sample_id="funding:bybit:BTCUSDT:10200:10:20000",
                funding_timestamp_ms=20_000,
                funding_interval_ms=28_800_000,
                observed_at_ms=10_200,
                underlying="BTC",
                quote_currency="USDT",
                contract_type="linear",
                contract_multiplier=1.0,
                mark_index_source="test_fixture",
                price_precision=2,
                quantity_precision=3,
                price_tick=0.01,
                quantity_step_base=0.001,
                min_quantity_base=0.001,
                min_notional_quote=1.0,
                min_notional_evidence_complete=True,
                venue_status="active",
                contract_normalization_complete=True,
            ),
        }

        async def fake_funding(*_args, **_kwargs):
            return [
                ("binance", {"binance:BTCUSDT": quotes["binance:BTCUSDT"]}, None, set()),
                ("bybit", {"bybit:BTCUSDT": quotes["bybit:BTCUSDT"]}, None, set()),
            ]

        async def fake_liquidity(*_args, **_kwargs):
            return [
                ("binance", {}, None, set()),
                ("bybit", {}, None, set()),
            ]

        clock = iter((10.0, 10.25, 10.3, 10.35))
        monkeypatch.setattr("lightfee.sidecar.service.time.time", lambda: next(clock))
        service._fetch_all_venues = fake_funding
        service._fetch_liquidity_all_venues = fake_liquidity

        try:
            snapshot = await service.refresh_once()
        finally:
            await service.close()

        assert len(snapshot.candidates) == 1
        assert snapshot.market_observed_at_ms == 10_200
        assert snapshot.candidate_build_observed_at_ms >= 10_200
        assert snapshot.candidate_build_diagnostics["directional_pair_count"] == 1
        assert snapshot.candidate_build_diagnostics["output_candidate_count"] == 1
        assert snapshot.candidate_build_diagnostics["rejection_counts"] == {}

    @pytest.mark.asyncio
    async def test_refresh_publishes_compact_spread_quotes_before_liquidity_work(
        self, tmp_path, monkeypatch
    ):
        from lightfee.config.schema import AppConfig, RuntimeConfig, VenueConfig
        from lightfee.sidecar.service import SidecarService
        from lightfee.spread.quote_snapshot import (
            load_spread_quote_snapshot,
            spread_quote_snapshot_path,
        )

        sidecar_path = tmp_path / "sidecar.json"
        service = SidecarService(
            AppConfig(
                symbols=["BTCUSDT"],
                runtime=RuntimeConfig(sidecar_snapshot_path=str(sidecar_path)),
                venues=[VenueConfig(venue="binance")],
            )
        )
        quote = QuoteSnapshot(
            venue="binance",
            symbol="BTCUSDT",
            bid=100.0,
            ask=100.1,
            bid_size=1.0,
            ask_size=1.0,
            observed_at_ms=1_100,
            underlying="BTC",
            quote_currency="USDT",
            contract_normalization_complete=True,
        )

        async def fake_funding(*_args, **_kwargs):
            return [("binance", {"binance:BTCUSDT": quote}, None, set())]

        async def fake_liquidity(*_args, **_kwargs):
            compact = load_spread_quote_snapshot(spread_quote_snapshot_path(sidecar_path))
            assert compact is not None
            assert compact.quotes["binance:BTCUSDT"].observed_at_ms == 1_100
            return [("binance", {}, None, set())]

        clock = iter((1.0, 1.2, 1.3, 1.4))
        monkeypatch.setattr("lightfee.sidecar.service.time.time", lambda: next(clock))
        service._fetch_all_venues = fake_funding
        service._fetch_liquidity_all_venues = fake_liquidity

        try:
            await service.refresh_once()
        finally:
            await service.close()

    @pytest.mark.asyncio
    async def test_liquidity_fetch_skips_venue_already_degraded_by_market_data(self, tmp_path):
        from lightfee.config.schema import AppConfig, RuntimeConfig, VenueConfig
        from lightfee.sidecar.service import SidecarService

        calls: list[str] = []

        class FakeLiquiditySource:
            def __init__(self, venue: str):
                self.venue = venue

            async def fetch_perp_liquidity(self, symbols):
                calls.append(self.venue)
                return {}

            async def close(self):
                return None

        service = SidecarService(
            AppConfig(
                symbols=["BTCUSDT"],
                runtime=RuntimeConfig(
                    sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
                ),
                venues=[VenueConfig(venue="binance"), VenueConfig(venue="bybit")],
            )
        )
        service._liquidity_sources = {
            "binance": FakeLiquiditySource("binance"),
            "bybit": FakeLiquiditySource("bybit"),
        }

        try:
            results = await service._fetch_liquidity_all_venues(
                ["BTCUSDT"],
                timeout_s=0.1,
                skip_venues={"bybit"},
            )
        finally:
            await service.close()

        assert calls == ["binance"]
        bybit = next(row for row in results if row[0] == "bybit")
        assert isinstance(bybit[2], RuntimeError)
        assert "market data degradation" in str(bybit[2])
