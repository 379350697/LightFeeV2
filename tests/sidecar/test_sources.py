"""Tests for ExchangeSource and LiquiditySource backing by MarketDataClient."""

from __future__ import annotations

import asyncio
import time

import pytest

from lightfee.core.domain import Venue
from lightfee.sidecar.snapshot import QuoteSnapshot, funding_rate_sample_id
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

    def test_funding_entry_bbo_overlay_requires_strict_identity_and_receipt_clock(self):
        from lightfee.marketdata.ws_bbo import TopBookQuote
        from lightfee.sidecar.service import _overlay_funding_entry_top_books

        base = QuoteSnapshot(
            venue="binance",
            symbol="BTCUSDT",
            bid=99.0,
            ask=102.0,
            bid_size=7.0,
            ask_size=8.0,
            bid_depth=((99.0, 7.0),),
            ask_depth=((102.0, 8.0),),
            observed_at_ms=100,
            funding_rate_bps=3.0,
        )
        top = TopBookQuote(
            venue="binance",
            symbol="BTCUSDT",
            bid=100.0,
            ask=101.0,
            bid_size=2.0,
            ask_size=3.0,
            observed_at_ms=200,
            received_at_ms=200,
            exchange_event_at_ms=190,
            source="sidecar_bulk_bbo_rest",
        )

        merged = _overlay_funding_entry_top_books(
            "binance",
            {"binance:BTCUSDT": base},
            {"binance:BTCUSDT": top},
            requested_symbols={"BTCUSDT"},
        )

        quote = merged["binance:BTCUSDT"]
        assert quote.bid == 100.0
        assert quote.ask == 101.0
        assert quote.observed_at_ms == 200
        assert quote.market_event_at_ms == 190
        assert quote.funding_rate_bps == 3.0
        assert quote.bid_depth == ()
        assert quote.ask_depth == ()

        for rejected in (
            TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=200.0,
                ask=201.0,
                observed_at_ms=99,
                received_at_ms=99,
            ),
            TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=200.0,
                ask=201.0,
                observed_at_ms=201,
                received_at_ms=202,
            ),
            TopBookQuote(
                venue="bybit",
                symbol="BTCUSDT",
                bid=200.0,
                ask=201.0,
                observed_at_ms=202,
                received_at_ms=202,
            ),
        ):
            rejected_merge = _overlay_funding_entry_top_books(
                "binance",
                {"binance:BTCUSDT": base},
                {"binance:BTCUSDT": rejected},
                requested_symbols={"BTCUSDT"},
            )
            assert rejected_merge["binance:BTCUSDT"] == base

        assert (
            _overlay_funding_entry_top_books(
                "binance",
                {},
                {"binance:BTCUSDT": top},
                requested_symbols={"BTCUSDT"},
            )
            == {}
        )

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
    async def test_funding_metadata_does_not_fetch_or_retain_a_top_book(self):
        class FakeClient:
            async def fetch_funding_tickers(self, symbols, *, include_open_interest):
                return {
                    "binance:BTCUSDT": FundingTicker(
                        venue="binance",
                        symbol="BTCUSDT",
                        bid=100.0,
                        ask=101.0,
                        funding_rate_bps=50.0,
                    )
                }

            async def fetch_top_book_quotes(self, symbols):
                raise AssertionError("funding metadata path fetched a top book")

        src = object.__new__(ExchangeSource)
        src.venue = "binance"
        src._client = FakeClient()

        quote = (await src.fetch_funding_metadata(["BTCUSDT"]))[
            "binance:BTCUSDT"
        ]

        assert quote.funding_rate_bps == 50.0
        assert quote.bid == 0.0
        assert quote.ask == 0.0
        assert quote.observed_at_ms == 0
        assert quote.source == "funding_metadata"

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
    @pytest.mark.parametrize(
        "status",
        ["symbol_not_listed", "ambiguous_mapping", "unavailable"],
    )
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

    @pytest.mark.asyncio
    async def test_zero_placeholder_is_kept_when_final_bbo_proves_listing(self):
        class FakeClient:
            async def fetch_funding_tickers(self, symbols, *, include_open_interest):
                return {
                    "binance:BTCUSDT": FundingTicker(
                        venue="binance",
                        symbol="BTCUSDT",
                        bid=0.0,
                        ask=0.0,
                        open_interest_evidence_status="unavailable",
                    )
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

        quotes = await src.fetch_market_quotes(["BTCUSDT"])

        assert quotes["binance:BTCUSDT"].bid == 200.0
        assert quotes["binance:BTCUSDT"].ask == 201.0


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
    def test_service_has_no_background_liquidity_collection_lane(self):
        from lightfee.config.schema import AppConfig, RuntimeConfig, VenueConfig
        from lightfee.sidecar.service import SidecarService

        config = AppConfig(
            runtime=RuntimeConfig(sidecar_snapshot_path="/tmp/unused-sidecar.json"),
            venues=[VenueConfig(venue="binance"), VenueConfig(venue="aster")],
        )

        service = SidecarService(config)

        binance_limiter = service._exchange_sources["binance"]._client._rate_limiter
        aster_limiter = service._exchange_sources["aster"]._client._rate_limiter
        binance_bbo_limiter = service._funding_entry_bbo_sources["binance"]._client._rate_limiter
        aster_bbo_limiter = service._funding_entry_bbo_sources["aster"]._client._rate_limiter

        assert binance_limiter is not binance_bbo_limiter
        assert aster_limiter is not aster_bbo_limiter
        assert binance_limiter is not aster_limiter
        assert binance_bbo_limiter is not aster_bbo_limiter
        assert binance_limiter is not None
        assert aster_limiter is not None
        assert not hasattr(service, "_liquidity_sources")
        assert (
            service._exchange_sources["binance"]._client._consume_global_rate_limit_budget is True
        )
        assert (
            service._funding_entry_bbo_sources["binance"]._client._consume_global_rate_limit_budget
            is False
        )

        binance_limiter.record_rate_limit_for_scopes(["venue:binance"], retry_after_ms=1_000)
        assert binance_limiter._cooldown_remaining_ms_for_scopes(["venue:binance"]) is not None
        assert binance_bbo_limiter._cooldown_remaining_ms_for_scopes(["venue:binance"]) is None

    @pytest.mark.asyncio
    async def test_funding_entry_bbo_cache_republish_never_starts_network(self, tmp_path):
        from lightfee.config.schema import AppConfig, RuntimeConfig, VenueConfig
        from lightfee.marketdata.ws_bbo import TopBookQuote
        from lightfee.sidecar.service import SidecarService

        service = SidecarService(
            AppConfig(
                symbols=["BTCUSDT"],
                runtime=RuntimeConfig(
                    sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
                ),
                venues=[VenueConfig(venue="binance")],
            ),
            enable_spread_bbo=False,
        )
        calls = 0

        async def unexpected_fetch(_symbols):
            nonlocal calls
            calls += 1
            return {}

        service._funding_entry_bbo_sources["binance"].fetch_spread_bbo = unexpected_fetch
        cached = TopBookQuote(
            venue="binance",
            symbol="BTCUSDT",
            bid=100.0,
            ask=101.0,
            observed_at_ms=123,
            received_at_ms=123,
        )
        service._funding_entry_bbo_latest_results = {
            "binance": (
                "binance",
                {"binance:BTCUSDT": cached},
                None,
                set(),
            )
        }
        service._entry_cache_only_refresh = True

        try:
            results = await service._fetch_funding_entry_bbo_all_venues(["BTCUSDT"])
        finally:
            await service.close()

        assert calls == 0
        assert results == [
            (
                "binance",
                {"binance:BTCUSDT": cached},
                None,
                set(),
            )
        ]

    @pytest.mark.asyncio
    async def test_late_funding_entry_bbo_completes_singleflight_and_wakes_republish(
        self,
        tmp_path,
        monkeypatch,
    ):
        from lightfee.config.schema import AppConfig, RuntimeConfig, VenueConfig
        from lightfee.marketdata.ws_bbo import TopBookQuote
        from lightfee.sidecar.service import SidecarService

        service = SidecarService(
            AppConfig(
                symbols=["BTCUSDT"],
                runtime=RuntimeConfig(
                    sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
                ),
                venues=[VenueConfig(venue="binance")],
            ),
            enable_spread_bbo=False,
        )
        release = asyncio.Event()
        calls = 0
        top = TopBookQuote(
            venue="binance",
            symbol="BTCUSDT",
            bid=100.0,
            ask=101.0,
            observed_at_ms=123,
            received_at_ms=123,
        )

        async def slow_fetch(_symbols):
            nonlocal calls
            calls += 1
            await release.wait()
            return {"binance:BTCUSDT": top}

        service._funding_entry_bbo_sources["binance"].fetch_spread_bbo = slow_fetch
        monkeypatch.setattr(
            "lightfee.sidecar.service.FUNDING_ENTRY_BBO_FRONTIER_S",
            0.01,
        )

        try:
            first = await service._fetch_funding_entry_bbo_all_venues(
                ["BTCUSDT"]
            )
            assert isinstance(first[0][2], TimeoutError)
            assert calls == 1
            assert not service._funding_entry_bbo_fetch_tasks["binance"].done()

            release.set()
            await asyncio.wait_for(
                service.entry_venue_republish_event.wait(),
                timeout=0.2,
            )
            service._entry_cache_only_refresh = True
            cached = await service._fetch_funding_entry_bbo_all_venues(
                ["BTCUSDT"]
            )
        finally:
            release.set()
            await service.close()

        assert calls == 1
        assert cached == [
            (
                "binance",
                {"binance:BTCUSDT": top},
                None,
                set(),
            )
        ]

    @pytest.mark.asyncio
    async def test_slow_funding_refresh_cannot_age_live_entry_bbo(
        self,
        tmp_path,
    ):
        from lightfee.config.schema import (
            AppConfig,
            RuntimeConfig,
            StrategyConfig,
            VenueConfig,
        )
        from lightfee.marketdata.ws_bbo import TopBookQuote
        from lightfee.sidecar.publisher import load_funding_entry_snapshot
        from lightfee.sidecar.service import SidecarService
        from lightfee.sidecar.snapshot import funding_rate_sample_id

        now_ms = int(time.time() * 1_000)
        funding_observed_at_ms = now_ms - 1_000
        old_market_observed_at_ms = now_ms - 10_000
        funding_timestamp_ms = now_ms + 28_800_000

        def metadata_quote(venue: str, rate_bps: float) -> QuoteSnapshot:
            return QuoteSnapshot(
                venue=venue,
                symbol="BTCUSDT",
                bid=90.0,
                ask=110.0,
                bid_size=10.0,
                ask_size=10.0,
                observed_at_ms=old_market_observed_at_ms,
                source="slow_funding_metadata_cache",
                funding_rate_bps=rate_bps,
                funding_rate_observed_at_ms=funding_observed_at_ms,
                funding_rate_event_at_ms=funding_observed_at_ms,
                funding_rate_received_at_ms=funding_observed_at_ms,
                funding_rate_source="test_fixture",
                funding_rate_sample_id=funding_rate_sample_id(
                    venue=venue,
                    symbol="BTCUSDT",
                    observed_at_ms=funding_observed_at_ms,
                    rate_bps=rate_bps,
                    funding_timestamp_ms=funding_timestamp_ms,
                ),
                funding_timestamp_ms=funding_timestamp_ms,
                funding_interval_ms=28_800_000,
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
            )

        config = AppConfig(
            symbols=["BTCUSDT"],
            runtime=RuntimeConfig(
                mode="live",
                sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
                max_market_age_ms=3_000,
            ),
            strategy=StrategyConfig(
                funding_new_entries_enabled=True,
                funding_canary_enabled=True,
                min_funding_edge_bps=1.0,
                min_expected_edge_bps=0.0,
            ),
            venues=[
                VenueConfig(venue="binance", taker_fee_bps=1.0),
                VenueConfig(venue="bybit", taker_fee_bps=1.0),
            ],
        )
        service = SidecarService(config, enable_spread_bbo=False)
        release_slow_funding = asyncio.Event()
        slow_started = {"binance": asyncio.Event(), "bybit": asyncio.Event()}

        async def slow_fetch(venue: str, _symbols):
            slow_started[venue].set()
            await release_slow_funding.wait()
            return {f"{venue}:BTCUSDT": metadata_quote(venue, 0.0)}

        for venue in ("binance", "bybit"):
            source = service._exchange_sources[venue]
            source.fetch_all = lambda symbols, current_venue=venue: slow_fetch(
                current_venue,
                symbols,
            )

            async def fresh_bbo(_symbols, current_venue=venue):
                return {
                    f"{current_venue}:BTCUSDT": TopBookQuote(
                        venue=current_venue,
                        symbol="BTCUSDT",
                        bid=100.0,
                        ask=100.1,
                        bid_size=10.0,
                        ask_size=10.0,
                        observed_at_ms=now_ms,
                        received_at_ms=now_ms,
                        exchange_event_at_ms=now_ms - 1,
                        source="funding_entry_bbo_test",
                    )
                }

            service._funding_entry_bbo_sources[venue].fetch_spread_bbo = fresh_bbo

        service._entry_venue_latest_results = {
            "binance": (
                "binance",
                {"binance:BTCUSDT": metadata_quote("binance", -50.0)},
                None,
                set(),
            ),
            "bybit": (
                "bybit",
                {"bybit:BTCUSDT": metadata_quote("bybit", 50.0)},
                None,
                set(),
            ),
        }
        service._schedule_audit_snapshot_publish = lambda *_args, **_kwargs: None

        started_at = time.monotonic()
        try:
            snapshot = await asyncio.wait_for(service.refresh_once(), timeout=1.0)
            elapsed_s = time.monotonic() - started_at
            assert all(event.is_set() for event in slow_started.values())
            assert all(not task.done() for task in service._entry_venue_fetch_tasks.values())
            compact = load_funding_entry_snapshot(service.snapshot_path)
        finally:
            release_slow_funding.set()
            await service.close()

        assert elapsed_s < 0.8
        assert len(snapshot.candidates) == 1
        assert compact is not None
        # The funding event is eight hours away, so complete static admission
        # decides this pair as outside the scan window.  V7 publishes every
        # eligible candidate, which correctly means an empty entry page here.
        assert compact.candidates == []
        assert compact.quotes == {}

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
        service._exchange_sources["binance"]._client._funding_interval_by_key["binance:BTCUSDT"] = (
            28_800_000,
            "test_fixture",
            int(time.time() * 1_000),
        )
        async def fake_bbo(*_args, **_kwargs):
            return [("binance", {}, None, set())]

        service._fetch_funding_entry_bbo_all_venues = fake_bbo

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

        async def fake_bbo(*_args, **_kwargs):
            return [
                ("binance", {}, None, set()),
                ("bybit", {}, None, set()),
            ]

        clock = iter((10.0, 10.25, 10.3, 10.35))
        monkeypatch.setattr("lightfee.sidecar.service.time.time", lambda: next(clock))
        service._fetch_all_venues = fake_funding
        service._fetch_funding_entry_bbo_all_venues = fake_bbo

        try:
            snapshot = await service.refresh_once()
        finally:
            await service.close()

        assert len(snapshot.candidates) == 1
        assert snapshot.market_observed_at_ms == 10_200
        assert snapshot.candidate_build_observed_at_ms >= 10_200
        assert snapshot.candidate_build_diagnostics["directional_pair_count"] == 2
        assert snapshot.candidate_build_diagnostics["seed_pair_count"] == 2
        assert snapshot.candidate_build_diagnostics["pair_decision_count"] == 2
        assert snapshot.candidate_build_diagnostics["eligible_frontier_complete"] is True
        assert snapshot.candidate_build_diagnostics["output_candidate_count"] == 1
        assert snapshot.candidate_build_diagnostics["rejection_counts"] == {
            "funding_edge_below_floor": 1,
        }
        assert snapshot.candidate_build_diagnostics["blocked_reason_counts"] == {
            "funding_edge_below_floor": 1,
            "outside_scan_window": 1,
        }
        timing = snapshot.candidate_build_diagnostics
        assert timing["refresh_started_at_ms"] == 10_000
        assert timing["venue_quote_observed_at_ms"] == {
            "binance": 10_100,
            "bybit": 10_200,
        }
        assert timing["candidate_build_started_at_ms"] == (
            snapshot.candidate_build_observed_at_ms
        )
        assert timing["candidate_build_completed_at_ms"] == snapshot.published_at_ms
        assert timing["entry_publish_started_at_ms"] == snapshot.published_at_ms
        assert timing["refresh_latency_quantiles_ms"] == {
            "sample_count": 1,
            "window_size": 128,
            "p50": 350,
            "p95": 350,
            "p99": 350,
        }

    @pytest.mark.asyncio
    async def test_background_audit_never_starts_a_second_venue_fetch(
        self, tmp_path
    ):
        """A missing configured symbol must not trigger an audit-wide refetch."""
        from lightfee.config.schema import AppConfig, RuntimeConfig, VenueConfig
        from lightfee.sidecar.service import SidecarService

        service = SidecarService(
            AppConfig(
                symbols=["BTCUSDT", "ETHUSDT"],
                runtime=RuntimeConfig(
                    sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
                ),
                venues=[VenueConfig(venue="binance")],
            )
        )
        now_ms = int(time.time() * 1_000)
        funding_timestamp_ms = now_ms + 28_800_000
        quote = QuoteSnapshot(
            venue="binance",
            symbol="BTCUSDT",
            bid=100.0,
            ask=101.0,
            observed_at_ms=now_ms,
            funding_rate_bps=1.0,
            funding_rate_observed_at_ms=now_ms,
            funding_rate_event_at_ms=now_ms,
            funding_rate_received_at_ms=now_ms,
            funding_rate_source="test_fixture",
            funding_rate_sample_id=funding_rate_sample_id(
                venue="binance",
                symbol="BTCUSDT",
                observed_at_ms=now_ms,
                rate_bps=1.0,
                funding_timestamp_ms=funding_timestamp_ms,
            ),
            funding_timestamp_ms=funding_timestamp_ms,
            funding_interval_ms=28_800_000,
            volume_24h_quote=10_000_000.0,
            open_interest_evidence_status="unavailable",
            open_interest_evidence_reason="entry_targeted_revalidation_required",
        )

        fetch_calls = 0

        async def fake_funding(_symbols, timeout_s):
            nonlocal fetch_calls
            fetch_calls += 1
            assert timeout_s > 0.0
            return [("binance", {"binance:BTCUSDT": quote}, None, set())]

        async def fake_bbo(_symbols):
            return [("binance", {}, None, set())]

        service._fetch_all_venues = fake_funding
        service._fetch_funding_entry_bbo_all_venues = fake_bbo
        try:
            await service.refresh_once()
            audit_task = service._audit_publish_task
            assert audit_task is not None
            await audit_task
        finally:
            await service.close()

        assert fetch_calls == 1
