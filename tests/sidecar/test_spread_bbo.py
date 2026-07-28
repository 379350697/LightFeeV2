from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
import subprocess
import sys
import time

import pytest

from lightfee.config.schema import AppConfig, VenueConfig
from lightfee.sidecar.service import SidecarService
from lightfee.sidecar.snapshot import QuoteSnapshot


@pytest.mark.asyncio
async def test_funding_sidecar_has_no_embedded_spread_data_plane(tmp_path) -> None:
    config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[VenueConfig(venue="okx")],
    )
    config.runtime.sidecar_snapshot_path = str(tmp_path / "sidecar.json")
    service = SidecarService(config)
    try:
        assert not hasattr(service, "_spread_bbo_sources")
        assert not hasattr(service, "_spread_bbo_data_plane")
        assert not hasattr(service, "embedded_spread_bbo_enabled")
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_disabled_spread_bbo_service_waits_without_starting_data_plane(
    tmp_path, monkeypatch
) -> None:
    from lightfee.sidecar.spread_bbo_service import SpreadBboProcessService

    config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[VenueConfig(venue="okx")],
    )
    config.runtime.sidecar_snapshot_path = str(tmp_path / "sidecar.json")
    service = SpreadBboProcessService(config)
    started = False

    async def fail_if_started(_stop_event) -> None:
        nonlocal started
        started = True

    monkeypatch.setattr(service.data_plane, "run", fail_if_started)
    stop_event = asyncio.Event()
    task = asyncio.create_task(service.run(stop_event))
    await asyncio.sleep(0)

    assert service.collection_enabled is False
    assert started is False
    assert not task.done()

    stop_event.set()
    await task


def _eligible_metadata_quote(venue: str, now_ms: int) -> QuoteSnapshot:
    return QuoteSnapshot(
        venue=venue,
        symbol="BTCUSDT",
        bid=100.0,
        ask=100.1,
        bid_size=1.0,
        ask_size=1.0,
        observed_at_ms=now_ms,
        funding_timestamp_ms=now_ms + 28_800_000,
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


def test_dedicated_bbo_plane_publishes_only_fresh_identity_checked_quotes(tmp_path) -> None:
    from lightfee.marketdata.ws_bbo import TopBookQuote
    from lightfee.sidecar.spread_bbo import SpreadBboDataPlane

    now_ms = int(time.time() * 1_000)
    metadata = {
        "cheap:BTCUSDT": _eligible_metadata_quote("cheap", now_ms),
        "rich:BTCUSDT": _eligible_metadata_quote("rich", now_ms),
    }
    config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[VenueConfig(venue="cheap"), VenueConfig(venue="rich")],
    )
    plane = SpreadBboDataPlane(
        config,
        sources={"cheap": object(), "rich": object()},
        metadata_quotes=lambda: metadata,
        snapshot_path=tmp_path / "spread-quotes.json",
    )
    for venue, bid, ask in (("cheap", 100.0, 100.1), ("rich", 101.0, 101.1)):
        plane._request_started_at_ms[venue] = now_ms
        assert plane._accept_venue_update(
            venue,
            {
                f"{venue}:BTCUSDT": TopBookQuote(
                    venue=venue,
                    symbol="BTCUSDT",
                    bid=bid,
                    ask=ask,
                    bid_size=2.0,
                    ask_size=3.0,
                    observed_at_ms=now_ms,
                    received_at_ms=now_ms,
                    source="venue_bbo",
                ),
                # A mismatched result key must never cross the process boundary.
                f"{venue}:OTHER": TopBookQuote(
                    venue=venue,
                    symbol="BTCUSDT",
                    bid=bid,
                    ask=ask,
                    observed_at_ms=now_ms,
                    received_at_ms=now_ms,
                ),
            },
        )

    snapshot = plane._build_snapshot()

    assert snapshot is not None
    assert snapshot.source_mode == "sidecar_market_fast_path"
    assert set(snapshot.quotes) == {"cheap:BTCUSDT", "rich:BTCUSDT"}
    assert snapshot.quotes["cheap:BTCUSDT"].source == "venue_bbo"
    assert snapshot.quotes["rich:BTCUSDT"].bid == 101.0


def test_dedicated_bbo_plane_rejects_zero_size_as_non_executable(tmp_path) -> None:
    from lightfee.marketdata.ws_bbo import TopBookQuote
    from lightfee.sidecar.spread_bbo import SpreadBboDataPlane

    now_ms = int(time.time() * 1_000)
    metadata = {"okx:BTCUSDT": _eligible_metadata_quote("okx", now_ms)}
    config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[VenueConfig(venue="okx")],
    )
    plane = SpreadBboDataPlane(
        config,
        sources={"okx": object()},
        metadata_quotes=lambda: metadata,
        snapshot_path=tmp_path / "spread-quotes.json",
    )
    plane._request_started_at_ms["okx"] = now_ms

    assert plane._accept_venue_update(
        "okx",
        {"okx:BTCUSDT": TopBookQuote(
            venue="okx",
            symbol="BTCUSDT",
            bid=100.0,
            ask=100.1,
            bid_size=0.0,
            ask_size=1.0,
            observed_at_ms=now_ms,
            received_at_ms=now_ms,
        )},
    )
    assert plane._build_snapshot() is None
    assert plane._degraded_symbols == {"okx": {"BTCUSDT"}}


def test_hyperliquid_bbo_subscribes_only_to_listed_sampling_symbols() -> None:
    from lightfee.sidecar.spread_bbo_service import (
        _hyperliquid_listed_sampling_symbols,
    )

    now_ms = int(time.time() * 1_000)
    btc = _eligible_metadata_quote("hyperliquid", now_ms)
    eth = _eligible_metadata_quote("hyperliquid", now_ms)
    eth.symbol = "ETHUSDT"

    listed = _hyperliquid_listed_sampling_symbols(
        ["BTCUSDT", "ETHUSDT", "NOTLISTEDUSDT"],
        quotes={
            "hyperliquid:BTCUSDT": btc,
            "hyperliquid:ETHUSDT": eth,
        },
        quote_eligible=lambda quote: quote.symbol == "BTCUSDT",
    )

    assert listed == ["BTCUSDT"]


@pytest.mark.asyncio
async def test_hyperliquid_ws_gap_uses_one_bulk_rest_fallback_for_all_symbols() -> None:
    from lightfee.marketdata.ws_bbo import TopBookQuote
    from lightfee.sidecar.spread_bbo_service import HyperliquidSpreadBboSource

    class FakeBulkFallback:
        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        async def fetch_spread_bbo(
            self, symbols: list[str]
        ) -> dict[str, TopBookQuote]:
            self.calls.append(symbols)
            now_ms = int(time.time() * 1_000)
            return {
                f"hyperliquid:{symbol}": TopBookQuote(
                    venue="hyperliquid",
                    symbol=symbol,
                    bid=100.0,
                    ask=100.1,
                    bid_size=1.0,
                    ask_size=1.0,
                    observed_at_ms=now_ms,
                    received_at_ms=now_ms,
                    source="sidecar_bulk_impact_quote_rest",
                )
                for symbol in symbols
            }

        async def close(self) -> None:
            return None

    fallback = FakeBulkFallback()
    source = HyperliquidSpreadBboSource(max_age_ms=1_000, rest_fallback=fallback)
    source._clients = {"BTCUSDT": object(), "ETHUSDT": object()}

    quotes = await source.fetch_spread_bbo(["BTCUSDT", "ETHUSDT"])

    assert fallback.calls == [["BTCUSDT", "ETHUSDT"]]
    assert set(quotes) == {"hyperliquid:BTCUSDT", "hyperliquid:ETHUSDT"}


def test_standalone_spread_bbo_modules_are_available() -> None:
    assert importlib.util.find_spec("lightfee.sidecar.spread_bbo") is not None
    assert importlib.util.find_spec("lightfee.sidecar.spread_bbo_service") is not None
    assert importlib.util.find_spec("lightfee.apps.spread_bbo") is not None


def test_deployment_has_a_dedicated_spread_bbo_unit() -> None:
    systemd = Path(__file__).resolve().parents[2] / "deploy" / "systemd"
    assert (systemd / "lightfee-sidecar.service").exists()
    assert (systemd / "lightfee-live.service").exists()
    assert (systemd / "lightfee-spread-sidecar.service").exists()
    bbo_unit = systemd / "lightfee-spread-bbo.service"
    assert bbo_unit.exists()
    assert "-m lightfee.apps.spread_bbo" in bbo_unit.read_text()
    assert "lightfee-spread-bbo.service" in (
        systemd / "lightfee-spread-sidecar.service"
    ).read_text()


def test_production_entrypoints_do_not_import_retired_evidence_planes() -> None:
    project_root = Path(__file__).resolve().parents[2]
    script = """
import sys
from lightfee.apps import live, sidecar, spread_sidecar
from lightfee.engine.entry_dispatch_runtime import EntryDispatchRuntime

retired_modules = (
    "lightfee.strategy.fee_evidence",
    "lightfee.persistence.open_interest_store",
)
loaded = [name for name in retired_modules if name in sys.modules]
retired_methods = [
    name
    for name in (
        "_funding_canary_admission_reason",
        "_funding_canary_submission_reason",
        "_funding_canary_clamp_quantity",
    )
    if hasattr(EntryDispatchRuntime, name)
]
if loaded or retired_methods:
    raise SystemExit(f"loaded={loaded}; methods={retired_methods}")
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_offline_fee_evidence_reader_remains_available() -> None:
    assert importlib.util.find_spec("lightfee.strategy.fee_evidence") is not None
