from __future__ import annotations

import asyncio
from dataclasses import replace
import threading
import time

import pytest

from lightfee.config.schema import AppConfig, VenueConfig
from lightfee.marketdata.ws_bbo import RestTopBookQuoteResult, TopBookQuote
from lightfee.sidecar.service import SidecarService
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.sidecar.spread_bbo import SpreadBboDataPlane
from lightfee.sidecar.spread_bbo_service import (
    HyperliquidSpreadBboSource,
    SpreadMetadataCache,
)
from lightfee.spread.quote_snapshot import (
    FULL_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
    SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
    SpreadQuoteSnapshot,
    load_spread_quote_snapshot,
    publish_spread_quote_snapshot,
    spread_metadata_snapshot_path,
    spread_quote_snapshot_path,
)


def _metadata_quote(venue: str) -> QuoteSnapshot:
    return QuoteSnapshot(
        venue=venue,
        symbol="BTCUSDT",
        bid=90.0,
        ask=110.0,
        observed_at_ms=1,
        funding_rate_bps=1.0,
        funding_timestamp_ms=30_000_000,
        funding_interval_ms=28_800_000,
        underlying="BTC",
        quote_currency="USDT",
        contract_type="linear",
        contract_multiplier=1.0,
        mark_index_source="venue_index",
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


@pytest.mark.asyncio
async def test_sidecar_bbo_transport_isolated_but_contract_cache_shared(tmp_path):
    config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[VenueConfig(venue="okx")],
    )
    config.runtime.sidecar_snapshot_path = str(tmp_path / "sidecar.json")
    service = SidecarService(config)
    try:
        heavy = service._exchange_sources["okx"]._client
        bbo = service._spread_bbo_sources["okx"]._client

        assert heavy is not bbo
        assert heavy._client is None
        assert bbo._client is None
        assert heavy._okx_contract_metadata_by_key is bbo._okx_contract_metadata_by_key
        assert heavy._rate_limiter is not bbo._rate_limiter
        assert bbo._consume_global_rate_limit_budget is False
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_external_bbo_mode_removes_transports_from_heavy_sidecar(tmp_path):
    config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[VenueConfig(venue="okx")],
    )
    config.runtime.sidecar_snapshot_path = str(tmp_path / "sidecar.json")
    service = SidecarService(config, enable_spread_bbo=False)
    try:
        assert service.embedded_spread_bbo_enabled is False
        assert service._spread_bbo_sources == {}
        assert service._spread_bbo_rate_limiters == {}
        assert service._spread_bbo_data_plane is None
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_external_bbo_metadata_cache_refreshes_and_retains_last_good(tmp_path):
    sidecar_path = tmp_path / "sidecar.json"
    now_ms = int(time.time() * 1000)
    stale_bbo_output = _metadata_quote("okx")
    stale_bbo_output.observed_at_ms = now_ms
    publish_spread_quote_snapshot(
        SpreadQuoteSnapshot(
            published_at_ms=now_ms,
            market_observed_at_ms=now_ms,
            batch_started_at_ms=now_ms - 1,
            configured_venues=["okx"],
            degraded_venues=[],
            degraded_symbols={},
            quotes={"okx:BTCUSDT": stale_bbo_output},
            sampling_symbols=["BTCUSDT"],
        ),
        spread_quote_snapshot_path(sidecar_path),
    )
    cache = SpreadMetadataCache(sidecar_path, max_age_ms=60_000)
    # BBO output is never trusted as metadata input: its receipt timestamp was
    # refreshed by the previous producer and cannot prove metadata freshness.
    assert cache.quotes == {}

    refreshed = _metadata_quote("okx")
    refreshed.funding_rate_bps = 2.0
    refreshed.observed_at_ms = now_ms
    publish_spread_quote_snapshot(
        SpreadQuoteSnapshot(
            schema_version=FULL_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
            published_at_ms=now_ms,
            market_observed_at_ms=now_ms,
            batch_started_at_ms=now_ms,
            configured_venues=["okx"],
            degraded_venues=[],
            degraded_symbols={},
            quotes={"okx:BTCUSDT": refreshed},
        ),
        spread_metadata_snapshot_path(sidecar_path),
    )
    stop_event = asyncio.Event()
    task = asyncio.create_task(cache.run(stop_event))
    deadline = asyncio.get_running_loop().time() + 1.0
    while cache.quotes.get("okx:BTCUSDT") is None and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.01)
    assert cache.quotes["okx:BTCUSDT"].funding_rate_bps == 2.0
    assert cache.quote_eligible(cache.quotes["okx:BTCUSDT"]) is True
    assert cache.quote_eligible(
        cache.quotes["okx:BTCUSDT"],
        now_ms=now_ms + 60_001,
    ) is False

    spread_metadata_snapshot_path(sidecar_path).write_text("not-json")
    await asyncio.sleep(0.55)
    stop_event.set()
    await task
    assert cache.quotes["okx:BTCUSDT"].funding_rate_bps == 2.0


def test_metadata_overlay_captures_one_atomic_generation(tmp_path, monkeypatch):
    sidecar_path = tmp_path / "sidecar.json"
    metadata_path = spread_metadata_snapshot_path(sidecar_path)
    now_ms = int(time.time() * 1000)

    def publish_generation(funding_rate_bps: float) -> None:
        quotes = {
            f"{venue}:BTCUSDT": replace(
                _metadata_quote(venue),
                funding_rate_bps=funding_rate_bps,
                observed_at_ms=now_ms,
            )
            for venue in ("binance", "okx")
        }
        publish_spread_quote_snapshot(
            SpreadQuoteSnapshot(
                schema_version=FULL_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
                published_at_ms=now_ms,
                market_observed_at_ms=now_ms,
                batch_started_at_ms=now_ms,
                configured_venues=["binance", "okx"],
                degraded_venues=[],
                degraded_symbols={},
                quotes=quotes,
            ),
            metadata_path,
        )

    publish_generation(1.0)
    cache = SpreadMetadataCache(sidecar_path, max_age_ms=60_000)._cache
    hot_quotes = {
        key: replace(quote, bid=100.0, ask=101.0, observed_at_ms=now_ms)
        for key, quote in cache.quotes.items()
    }
    original_eligible = cache.quote_eligible
    interleaved = False

    def interleaving_eligible(quote, *, now_ms=None, generation=None):
        nonlocal interleaved
        if not interleaved:
            interleaved = True
            publish_generation(9.0)
            assert cache.refresh() is True
        return original_eligible(
            quote,
            now_ms=now_ms,
            generation=generation,
        )

    monkeypatch.setattr(cache, "quote_eligible", interleaving_eligible)
    merged, unavailable = cache.overlay_hot_quotes(hot_quotes, now_ms=now_ms)

    assert unavailable == {}
    assert {quote.funding_rate_bps for quote in merged.values()} == {1.0}
    merged_next, unavailable_next = cache.overlay_hot_quotes(
        hot_quotes,
        now_ms=now_ms,
    )
    assert unavailable_next == {}
    assert {quote.funding_rate_bps for quote in merged_next.values()} == {9.0}


def test_bbo_process_uses_slow_lane_last_good_ttl_for_metadata(tmp_path):
    config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[VenueConfig(venue="okx")],
    )
    config.runtime.sidecar_snapshot_path = str(tmp_path / "sidecar.json")
    config.runtime.sidecar_snapshot_max_age_ms = 10_000
    config.runtime.live_scan_last_good_max_age_ms = 600_000

    from lightfee.sidecar.spread_bbo_service import SpreadBboProcessService

    service = SpreadBboProcessService(config)
    assert service.metadata.max_age_ms == 600_000
    assert service.data_plane.snapshot_schema_version == SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION


@pytest.mark.asyncio
async def test_bbo_process_routes_hyperliquid_to_bbo_websocket_not_rest(tmp_path):
    config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[
            VenueConfig(venue="hyperliquid"),
            VenueConfig(venue="binance"),
        ],
    )
    config.runtime.sidecar_snapshot_path = str(tmp_path / "sidecar.json")

    from lightfee.sidecar.spread_bbo_service import SpreadBboProcessService

    service = SpreadBboProcessService(config)
    try:
        source = service.sources["hyperliquid"]
        assert source is service.hyperliquid_source
        assert isinstance(source, HyperliquidSpreadBboSource)
        assert not isinstance(source, type(service.sources["binance"]))
        client = source._new_client("BTCUSDT")
        assert client.build_subscribe_message() == {
            "method": "subscribe",
            "subscription": {"type": "bbo", "coin": "BTC"},
        }
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_hyperliquid_ws_source_preserves_receipt_clock_contract() -> None:
    now_ms = int(time.time() * 1000)
    source = HyperliquidSpreadBboSource(max_age_ms=100)
    assert source._cache.update_quote(
        TopBookQuote(
            venue="hyperliquid",
            symbol="BTCUSDT",
            bid=99.0,
            ask=101.0,
            bid_size=2.0,
            ask_size=3.0,
            observed_at_ms=now_ms - 500,
            received_at_ms=now_ms - 5,
            source="hyperliquid_bbo",
        )
    )

    quotes = await source.fetch_spread_bbo(["BTCUSDT"])

    quote = quotes["hyperliquid:BTCUSDT"]
    assert quote.observed_at_ms == now_ms - 5
    assert quote.received_at_ms == now_ms - 5
    assert quote.exchange_event_at_ms == now_ms - 500
    assert quote.source == "hyperliquid_bbo"


@pytest.mark.asyncio
async def test_hyperliquid_ws_source_starts_the_complete_bounded_universe(
    monkeypatch,
) -> None:
    from lightfee.sidecar import spread_bbo_service

    started: list[str] = []

    async def start(client) -> None:
        started.append(client.symbol)

    monkeypatch.setattr(
        spread_bbo_service.HyperliquidBboWsClient,
        "start",
        start,
    )
    source = HyperliquidSpreadBboSource(max_age_ms=1_000)

    await source.start(["BTCUSDT", "NOTLISTEDUSDT"])

    assert started == ["BTCUSDT", "NOTLISTEDUSDT"]
    assert set(source._clients) == {"BTCUSDT", "NOTLISTEDUSDT"}


@pytest.mark.asyncio
async def test_hyperliquid_ws_source_rejects_stale_cached_quote() -> None:
    now_ms = int(time.time() * 1000)
    source = HyperliquidSpreadBboSource(max_age_ms=100)
    assert source._cache.update_quote(
        TopBookQuote(
            venue="hyperliquid",
            symbol="BTCUSDT",
            bid=99.0,
            ask=101.0,
            observed_at_ms=now_ms - 5,
            received_at_ms=now_ms - 500,
            source="hyperliquid_bbo",
        )
    )

    assert await source.fetch_spread_bbo(["BTCUSDT"]) == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("connected", [True, False])
async def test_hyperliquid_ws_source_refreshes_started_stale_quote_via_rest(
    connected,
) -> None:
    now_ms = int(time.time() * 1000)
    refreshed = TopBookQuote(
        venue="hyperliquid",
        symbol="BTCUSDT",
        bid=99.5,
        ask=100.5,
        bid_size=4.0,
        ask_size=5.0,
        observed_at_ms=now_ms,
        received_at_ms=now_ms,
        exchange_event_at_ms=now_ms - 10,
        source="hyperliquid_rest_top_book",
    )

    class ConnectedClient:
        is_connected = connected

    class RestFallback:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, int]] = []

        async def arefresh_quote_result(self, venue, symbol, *, now_ms):
            self.calls.append((venue, symbol, now_ms))
            return RestTopBookQuoteResult(
                venue=venue,
                symbol=symbol,
                outcome="resolved",
                quote=refreshed,
            )

        async def aclose(self) -> None:
            return None

    fallback = RestFallback()
    source = HyperliquidSpreadBboSource(
        max_age_ms=100,
        rest_fallback=fallback,
    )
    source._clients["BTCUSDT"] = ConnectedClient()
    assert source._cache.update_quote(
        TopBookQuote(
            venue="hyperliquid",
            symbol="BTCUSDT",
            bid=99.0,
            ask=101.0,
            observed_at_ms=now_ms - 500,
            received_at_ms=now_ms - 500,
            source="hyperliquid_bbo",
        )
    )

    quotes = await source.fetch_spread_bbo(["BTCUSDT"])

    assert len(fallback.calls) == 1
    assert fallback.calls[0][:2] == ("hyperliquid", "BTCUSDT")
    assert now_ms <= fallback.calls[0][2] <= int(time.time() * 1000)
    assert quotes == {"hyperliquid:BTCUSDT": refreshed}
    assert source._cache.get_quote("hyperliquid", "BTCUSDT") == refreshed


@pytest.mark.asyncio
async def test_hyperliquid_ws_source_prewarms_before_ttl_and_keeps_fresh_cache_on_error(
) -> None:
    now_ms = int(time.time() * 1000)

    class ConnectedClient:
        is_connected = True

        async def stop(self) -> None:
            return None

    class FailedRestFallback:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, int]] = []

        async def arefresh_quote_result(self, venue, symbol, *, now_ms):
            self.calls.append((venue, symbol, now_ms))
            return RestTopBookQuoteResult(
                venue=venue,
                symbol=symbol,
                outcome="transport_failure",
            )

        async def aclose(self) -> None:
            return None

    fallback = FailedRestFallback()
    source = HyperliquidSpreadBboSource(
        max_age_ms=1_000,
        rest_fallback=fallback,
    )
    source._clients["BTCUSDT"] = ConnectedClient()
    assert source._cache.update_quote(
        TopBookQuote(
            venue="hyperliquid",
            symbol="BTCUSDT",
            bid=99.0,
            ask=101.0,
            received_at_ms=now_ms - 800,
            observed_at_ms=now_ms - 800,
            source="hyperliquid_bbo",
        )
    )

    quotes = await source.fetch_spread_bbo(["BTCUSDT"])

    assert len(fallback.calls) == 1
    assert quotes["hyperliquid:BTCUSDT"].received_at_ms == now_ms - 800
    await source.close()


def test_hyperliquid_ws_source_uses_bounded_parallel_rest_fallback() -> None:
    source = HyperliquidSpreadBboSource(max_age_ms=1_000)

    assert (
        source._rest_fallback._venue_async_semaphores["hyperliquid"]._value
        == source._rest_fallback.GLOBAL_ASYNC_CONCURRENCY
    )


@pytest.mark.asyncio
async def test_hyperliquid_ws_source_close_attempts_all_and_retains_failures() -> None:
    class StubClient:
        def __init__(self, failure: Exception | None = None) -> None:
            self.failure = failure
            self.stop_calls = 0

        async def stop(self) -> None:
            self.stop_calls += 1
            if self.failure is not None:
                raise self.failure

    source = HyperliquidSpreadBboSource(max_age_ms=100)
    stopped = StubClient()
    failed = StubClient(RuntimeError("stop failed"))
    source._clients = {"BTCUSDT": stopped, "ETHUSDT": failed}

    with pytest.raises(ExceptionGroup, match="client shutdown failed"):
        await source.close()

    assert stopped.stop_calls == 1
    assert failed.stop_calls == 1
    assert source._clients == {"ETHUSDT": failed}


@pytest.mark.asyncio
async def test_slow_venue_cannot_delay_fast_venue_publication(tmp_path):
    config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[
            VenueConfig(venue="binance"),
            VenueConfig(venue="okx"),
        ],
    )
    config.runtime.spread_sidecar_refresh_ms = 50
    config.strategy.spread_signal_ttl_ms = 250
    snapshot_path = tmp_path / "spread-quotes.json"

    class FastSource:
        async def fetch_spread_bbo(self, symbols):
            received_at_ms = int(time.time() * 1000)
            return {
                "binance:BTCUSDT": TopBookQuote(
                    venue="binance",
                    symbol="BTCUSDT",
                    bid=100.0,
                    ask=101.0,
                    bid_size=2.0,
                    ask_size=3.0,
                    observed_at_ms=received_at_ms,
                    received_at_ms=received_at_ms,
                    exchange_event_at_ms=1,
                    source="test_receipt_bbo",
                )
            }

    class SlowSource:
        async def fetch_spread_bbo(self, symbols):
            await asyncio.Future()

    metadata = {
        "binance:BTCUSDT": _metadata_quote("binance"),
        "okx:BTCUSDT": _metadata_quote("okx"),
    }
    plane = SpreadBboDataPlane(
        config,
        sources={"binance": FastSource(), "okx": SlowSource()},
        metadata_quotes=lambda: metadata,
        snapshot_path=snapshot_path,
    )
    stop_event = asyncio.Event()
    run_task = asyncio.create_task(plane.run(stop_event))
    deadline = asyncio.get_running_loop().time() + 0.5
    snapshot = None
    while asyncio.get_running_loop().time() < deadline:
        snapshot = load_spread_quote_snapshot(snapshot_path)
        if snapshot is not None:
            break
        await asyncio.sleep(0.01)
    stop_event.set()
    await run_task

    assert snapshot is not None
    assert list(snapshot.quotes) == ["binance:BTCUSDT"]
    assert "okx" in snapshot.degraded_venues
    quote = snapshot.quotes["binance:BTCUSDT"]
    assert quote.source == "test_receipt_bbo"
    assert snapshot.published_at_ms - quote.observed_at_ms <= 250


def test_exchange_event_timestamp_cannot_masquerade_as_receipt_time(tmp_path):
    config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[VenueConfig(venue="binance")],
    )
    plane = SpreadBboDataPlane(
        config,
        sources={"binance": object()},
        metadata_quotes=lambda: {
            "binance:BTCUSDT": _metadata_quote("binance"),
        },
        snapshot_path=tmp_path / "spread-quotes.json",
    )

    changed = plane._accept_venue_update(
        "binance",
        {
            "binance:BTCUSDT": TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=1_700_000_000_000,
                received_at_ms=2_000_000_000_000,
                exchange_event_at_ms=1_700_000_000_000,
            )
        },
    )

    assert changed is True
    assert plane._build_snapshot() is None
    assert plane._degraded_venues == {"binance"}


def test_identical_venue_update_does_not_trigger_a_new_generation(tmp_path):
    config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[VenueConfig(venue="binance")],
    )
    plane = SpreadBboDataPlane(
        config,
        sources={"binance": object()},
        metadata_quotes=lambda: {
            "binance:BTCUSDT": _metadata_quote("binance"),
        },
        snapshot_path=tmp_path / "spread-quotes.json",
    )
    received_at_ms = int(time.time() * 1000)
    update = {
        "binance:BTCUSDT": TopBookQuote(
            venue="binance",
            symbol="BTCUSDT",
            bid=100.0,
            ask=101.0,
            observed_at_ms=received_at_ms,
            received_at_ms=received_at_ms,
        )
    }

    assert plane._accept_venue_update("binance", update) is True
    assert plane._accept_venue_update("binance", update) is False


def test_snapshot_excludes_expired_quotes_without_blocking_fresh_venues(tmp_path):
    config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[
            VenueConfig(venue="binance"),
            VenueConfig(venue="okx"),
        ],
    )
    config.strategy.spread_signal_ttl_ms = 1_000
    plane = SpreadBboDataPlane(
        config,
        sources={"binance": object(), "okx": object()},
        metadata_quotes=lambda: {
            "binance:BTCUSDT": _metadata_quote("binance"),
            "okx:BTCUSDT": _metadata_quote("okx"),
        },
        snapshot_path=tmp_path / "spread-quotes.json",
    )
    now_ms = int(time.time() * 1000)
    plane._quotes_by_venue = {
        "binance": {
            "binance:BTCUSDT": _metadata_quote("binance"),
        },
        "okx": {
            "okx:BTCUSDT": _metadata_quote("okx"),
        },
    }
    plane._quotes_by_venue["binance"]["binance:BTCUSDT"].observed_at_ms = now_ms - 1_001
    plane._quotes_by_venue["okx"]["okx:BTCUSDT"].observed_at_ms = now_ms
    plane._degraded_venues.clear()

    snapshot = plane._build_snapshot()

    assert snapshot is not None
    assert list(snapshot.quotes) == ["okx:BTCUSDT"]
    assert snapshot.degraded_venues == ["binance"]
    assert snapshot.degraded_symbols == {"binance": ["BTCUSDT"]}
    assert snapshot.published_at_ms - snapshot.quotes["okx:BTCUSDT"].observed_at_ms <= 1_000


def test_partial_venue_refresh_replaces_old_symbol_rows(tmp_path):
    config = AppConfig(
        symbols=["BTCUSDT", "ETHUSDT"],
        venues=[VenueConfig(venue="binance")],
    )
    metadata = {
        "binance:BTCUSDT": _metadata_quote("binance"),
        "binance:ETHUSDT": replace(_metadata_quote("binance"), symbol="ETHUSDT"),
    }
    plane = SpreadBboDataPlane(
        config,
        sources={"binance": object()},
        metadata_quotes=lambda: metadata,
        snapshot_path=tmp_path / "spread-quotes.json",
    )
    now_ms = int(time.time() * 1000)

    def top(symbol: str, received_at_ms: int) -> TopBookQuote:
        return TopBookQuote(
            venue="binance",
            symbol=symbol,
            bid=100.0,
            ask=101.0,
            observed_at_ms=received_at_ms,
            received_at_ms=received_at_ms,
        )

    assert plane._accept_venue_update(
        "binance",
        {
            "binance:BTCUSDT": top("BTCUSDT", now_ms - 10),
            "binance:ETHUSDT": top("ETHUSDT", now_ms - 10),
        },
    )
    assert plane._accept_venue_update(
        "binance",
        {"binance:BTCUSDT": top("BTCUSDT", now_ms)},
    )

    snapshot = plane._build_snapshot()
    assert snapshot is not None
    assert list(snapshot.quotes) == ["binance:BTCUSDT"]
    assert snapshot.degraded_symbols == {"binance": ["ETHUSDT"]}


def test_bbo_sampling_universe_controls_fetch_and_missing_symbol_truth(tmp_path):
    config = AppConfig(
        symbols=["BTCUSDT", "ETHUSDT"],
        venues=[VenueConfig(venue="binance")],
    )
    metadata = {
        "binance:BTCUSDT": _metadata_quote("binance"),
        "binance:ETHUSDT": replace(_metadata_quote("binance"), symbol="ETHUSDT"),
    }
    plane = SpreadBboDataPlane(
        config,
        sources={"binance": object()},
        metadata_quotes=lambda: metadata,
        snapshot_path=tmp_path / "spread-quotes.json",
    )
    plane.set_sampling_symbols(["BTCUSDT"])
    now_ms = int(time.time() * 1000)

    assert plane.sampling_symbols == ("BTCUSDT",)
    assert plane._accept_venue_update(
        "binance",
        {
            "binance:BTCUSDT": TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=now_ms,
                received_at_ms=now_ms,
            )
        },
    )
    assert plane._degraded_symbols == {}

    plane.active = True
    with pytest.raises(RuntimeError, match="cannot change"):
        plane.set_sampling_symbols(["ETHUSDT"])


def test_bbo_rejects_universe_over_worst_case_recovery_budget(tmp_path):
    venues = [f"v{index}" for index in range(7)]
    symbols = [f"S{index}USDT" for index in range(19)]
    config = AppConfig(
        symbols=symbols,
        venues=[VenueConfig(venue=venue) for venue in venues],
    )
    plane = SpreadBboDataPlane(
        config,
        sources={venue: object() for venue in venues},
        metadata_quotes=dict,
        snapshot_path=tmp_path / "spread-quotes.json",
    )

    with pytest.raises(ValueError, match="exceeds worst-case pair budget"):
        plane.set_sampling_symbols(symbols)


def test_snapshot_batch_clock_uses_accepted_request_not_next_inflight_request(tmp_path):
    config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[VenueConfig(venue="binance")],
    )
    config.strategy.spread_signal_ttl_ms = 1_000
    plane = SpreadBboDataPlane(
        config,
        sources={"binance": object()},
        metadata_quotes=lambda: {
            "binance:BTCUSDT": _metadata_quote("binance"),
        },
        snapshot_path=tmp_path / "spread-quotes.json",
    )
    now_ms = int(time.time() * 1000)
    plane._request_started_at_ms["binance"] = now_ms - 20
    assert plane._accept_venue_update(
        "binance",
        {
            "binance:BTCUSDT": TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=now_ms,
                received_at_ms=now_ms,
            )
        },
    )
    plane._request_started_at_ms["binance"] = now_ms + 10

    snapshot = plane._build_snapshot()

    assert snapshot is not None
    assert snapshot.batch_started_at_ms == now_ms - 20


@pytest.mark.asyncio
async def test_sidecar_runs_bbo_on_an_independent_event_loop_thread():
    main_thread_id = threading.get_ident()
    observed: dict[str, int] = {}

    class FakePlane:
        async def run(self, stop_event):
            observed["thread_id"] = threading.get_ident()
            await stop_event.wait()

    service = object.__new__(SidecarService)
    service.config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[VenueConfig(venue="binance")],
    )
    service._spread_bbo_data_plane = FakePlane()
    service._spread_bbo_sources = {}
    stop_event = asyncio.Event()
    task = asyncio.create_task(service.run_spread_bbo_data_plane(stop_event))
    deadline = asyncio.get_running_loop().time() + 1.0
    while "thread_id" not in observed and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.01)
    stop_event.set()
    await task

    assert observed["thread_id"] != main_thread_id


@pytest.mark.asyncio
async def test_embedded_bbo_freezes_the_same_bounded_sampling_universe(tmp_path):
    config = AppConfig(
        symbols=["BTCUSDT", "ETHUSDT"],
        venues=[VenueConfig(venue="binance"), VenueConfig(venue="okx")],
    )
    config.runtime.daily_universe.enabled = True
    config.runtime.daily_universe.path = str(tmp_path / "missing-daily.json")
    config.runtime.daily_universe.max_symbols = 1
    selected: list[str] = []

    class FakePlane:
        def set_sampling_symbols(self, symbols):
            selected.extend(symbols)

        async def run(self, stop_event):
            await stop_event.wait()

    service = object.__new__(SidecarService)
    service.config = config
    service._spread_bbo_data_plane = FakePlane()
    service._spread_bbo_sources = {}
    service._last_good_quotes = {}
    for symbol, volume in (("BTCUSDT", 1_000.0), ("ETHUSDT", 2_000.0)):
        for venue in ("binance", "okx"):
            quote = replace(_metadata_quote(venue), symbol=symbol)
            quote.underlying = symbol.removesuffix("USDT")
            quote.volume_24h_quote = volume
            service._last_good_quotes[f"{venue}:{symbol}"] = quote

    stop_event = asyncio.Event()
    task = asyncio.create_task(service.run_spread_bbo_data_plane(stop_event))
    deadline = asyncio.get_running_loop().time() + 1.0
    while not selected and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.01)
    stop_event.set()
    await task

    assert selected == ["ETHUSDT"]


@pytest.mark.asyncio
async def test_embedded_bbo_bounds_oversized_default_universe(tmp_path):
    symbols = [f"S{index}USDT" for index in range(30)]
    venues = [f"v{index}" for index in range(7)]
    config = AppConfig(
        symbols=symbols,
        venues=[VenueConfig(venue=venue) for venue in venues],
    )
    selected: list[str] = []

    class FakePlane:
        def set_sampling_symbols(self, sampling_symbols):
            selected.extend(sampling_symbols)

        async def run(self, stop_event):
            await stop_event.wait()

    service = object.__new__(SidecarService)
    service.config = config
    service._spread_bbo_data_plane = FakePlane()
    service._spread_bbo_sources = {}
    service._last_good_quotes = {}
    for symbol_index, symbol in enumerate(symbols):
        for venue in venues[:2]:
            quote = replace(_metadata_quote(venue), symbol=symbol)
            quote.underlying = symbol.removesuffix("USDT")
            quote.volume_24h_quote = 1_000_000.0 - symbol_index
            service._last_good_quotes[f"{venue}:{symbol}"] = quote

    stop_event = asyncio.Event()
    task = asyncio.create_task(service.run_spread_bbo_data_plane(stop_event))
    deadline = asyncio.get_running_loop().time() + 1.0
    while not selected and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.01)
    stop_event.set()
    await task

    assert config.runtime.daily_universe.enabled is False
    assert len(selected) == 18


@pytest.mark.asyncio
async def test_sidecar_cancellation_waits_for_bbo_thread_shutdown():
    thread_exited = threading.Event()

    class FakePlane:
        async def run(self, stop_event):
            try:
                await stop_event.wait()
            finally:
                await asyncio.sleep(0.02)
                thread_exited.set()

    service = object.__new__(SidecarService)
    service.config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[VenueConfig(venue="binance")],
    )
    service._spread_bbo_data_plane = FakePlane()
    service._spread_bbo_sources = {}
    task = asyncio.create_task(service.run_spread_bbo_data_plane(asyncio.Event()))
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert thread_exited.is_set()
