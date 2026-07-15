from __future__ import annotations

import asyncio
import threading
import time

import pytest

from lightfee.config.schema import AppConfig, VenueConfig
from lightfee.marketdata.ws_bbo import TopBookQuote
from lightfee.sidecar.service import SidecarService
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.sidecar.spread_bbo import SpreadBboDataPlane
from lightfee.spread.quote_snapshot import load_spread_quote_snapshot


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
        assert (
            heavy._okx_contract_metadata_by_key
            is bbo._okx_contract_metadata_by_key
        )
        assert heavy._rate_limiter is not bbo._rate_limiter
        assert bbo._consume_global_rate_limit_budget is False
    finally:
        await service.close()


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


@pytest.mark.asyncio
async def test_sidecar_runs_bbo_on_an_independent_event_loop_thread():
    main_thread_id = threading.get_ident()
    observed: dict[str, int] = {}

    class FakePlane:
        async def run(self, stop_event):
            observed["thread_id"] = threading.get_ident()
            await stop_event.wait()

    service = object.__new__(SidecarService)
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
    service._spread_bbo_data_plane = FakePlane()
    service._spread_bbo_sources = {}
    task = asyncio.create_task(
        service.run_spread_bbo_data_plane(asyncio.Event())
    )
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert thread_exited.is_set()
