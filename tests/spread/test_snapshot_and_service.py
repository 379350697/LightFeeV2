from __future__ import annotations

import json

import pytest

from lightfee.config.schema import AppConfig, RuntimeConfig, StrategyConfig, VenueConfig
from lightfee.sidecar.publisher import publish_snapshot
from lightfee.sidecar.snapshot import QuoteSnapshot, SidecarSnapshot
from lightfee.spread.models import SpreadReversionCandidate, SpreadSnapshot
from lightfee.spread.publisher import load_spread_snapshot, publish_spread_snapshot
from lightfee.spread.service import SpreadSidecarService


def _candidate() -> SpreadReversionCandidate:
    return SpreadReversionCandidate(
        candidate_id="spread:BTCUSDT:cheap->rich",
        symbol="BTCUSDT",
        long_venue="cheap",
        short_venue="rich",
        spread_mid_bps=20.0,
        executable_spread_bps=18.0,
        rolling_mean_bps=8.0,
        rolling_std_bps=4.0,
        z_score=3.0,
        net_edge_bps=12.0,
        sample_count=20,
        signal_ts_ms=1_000,
        long_quote_ts_ms=1_000,
        short_quote_ts_ms=1_000,
        entry_notional_quote=20.0,
        capacity_quote=100.0,
        signal_status="entry_ready",
        fair_price=100.05,
        liquidity_evidence_status="top_book_size_available",
        screening_reasons=[],
        history_age_ms=300_000,
    )


def test_spread_snapshot_round_trips_without_funding_candidate_shape(tmp_path) -> None:
    path = tmp_path / "spread.json"
    snapshot = SpreadSnapshot(
        published_at_ms=2_000,
        market_observed_at_ms=1_990,
        source_mode="sidecar_snapshot",
        candidates=[_candidate()],
    )

    publish_spread_snapshot(snapshot, path)
    loaded = load_spread_snapshot(path)

    assert loaded is not None
    assert loaded.schema_version == 1
    assert loaded.source_mode == "sidecar_snapshot"
    assert loaded.candidates[0].strategy_bucket == "spread_reversion"
    raw = json.loads(path.read_text())
    assert raw["source_mode"] == "sidecar_snapshot"
    assert raw["candidates"][0]["fair_price"] == pytest.approx(100.05)
    assert raw["candidates"][0]["liquidity_evidence_status"] == "top_book_size_available"
    assert raw["candidates"][0]["screening_reasons"] == []
    assert raw["candidates"][0]["history_age_ms"] == 300_000
    assert "fair_price_bps" not in raw["candidates"][0]
    assert not hasattr(loaded.candidates[0], "opportunity_type")
    assert loaded.candidates[0].liquidity_evidence_status == "top_book_size_available"


@pytest.mark.asyncio
async def test_spread_sidecar_service_uses_main_sidecar_snapshot_by_default(tmp_path) -> None:
    sidecar_path = tmp_path / "sidecar-current.json"
    config = AppConfig(
        symbols=["BTCUSDT"],
        runtime=RuntimeConfig(
            sidecar_snapshot_path=str(sidecar_path),
            spread_sidecar_snapshot_path=str(tmp_path / "spread-current.json"),
        ),
        strategy=StrategyConfig(
            spread_reversion_enabled=True,
            spread_min_samples=2,
            spread_min_history_ms=0,
            spread_fair_price_min_venues=2,
            spread_min_fair_price_confidence=0.0,
            spread_min_liquidity_capacity_ratio=1.0,
            spread_entry_z=0.0,
            spread_min_net_edge_bps=0.0,
        ),
        venues=[VenueConfig(venue="cheap"), VenueConfig(venue="rich")],
    )
    publish_snapshot(
        SidecarSnapshot(
            published_at_ms=10_000,
            market_observed_at_ms=10_000,
            quotes={
                "cheap:BTCUSDT": QuoteSnapshot(
                    venue="cheap",
                    symbol="BTCUSDT",
                    bid=99.9,
                    ask=100.0,
                    bid_size=10.0,
                    ask_size=10.0,
                    observed_at_ms=10_000,
                ),
                "rich:BTCUSDT": QuoteSnapshot(
                    venue="rich",
                    symbol="BTCUSDT",
                    bid=101.0,
                    ask=101.1,
                    bid_size=10.0,
                    ask_size=10.0,
                    observed_at_ms=10_000,
                ),
            },
        ),
        sidecar_path,
    )
    service = SpreadSidecarService(config)

    class FakeSource:
        async def fetch_all(self, symbols):
            raise AssertionError("spread sidecar must not direct-fetch when snapshot is present")

        async def close(self):
            pass

    service._exchange_sources = {"cheap": FakeSource(), "rich": FakeSource()}
    await service.refresh_once(now_ms=10_000)
    snapshot = await service.refresh_once(now_ms=10_001)

    assert snapshot.candidates
    assert snapshot.source_mode == "sidecar_snapshot"
    assert snapshot.snapshot_path.endswith("spread-current.json")
    assert (tmp_path / "spread-current.json").exists()
    loaded = load_spread_snapshot(tmp_path / "spread-current.json")
    assert loaded is not None
    assert loaded.source_mode == "sidecar_snapshot"
    assert loaded.candidates[0].long_venue == "cheap"
    assert loaded.candidates[0].short_venue == "rich"


@pytest.mark.asyncio
async def test_spread_sidecar_service_degrades_when_main_snapshot_missing_without_direct_fetch(
    tmp_path,
) -> None:
    config = AppConfig(
        symbols=["BTCUSDT"],
        runtime=RuntimeConfig(
            sidecar_snapshot_path=str(tmp_path / "missing-sidecar.json"),
            spread_sidecar_snapshot_path=str(tmp_path / "spread-current.json"),
        ),
        strategy=StrategyConfig(spread_reversion_enabled=True),
        venues=[VenueConfig(venue="binance"), VenueConfig(venue="aster")],
    )
    service = SpreadSidecarService(config)

    class FakeSource:
        async def fetch_all(self, symbols):
            raise AssertionError("missing main sidecar snapshot must not trigger direct fetch")

    service._exchange_sources = {"binance": FakeSource(), "aster": FakeSource()}
    snapshot = await service.refresh_once(now_ms=20_000)

    assert snapshot.candidates == []
    assert snapshot.source_mode == "sidecar_snapshot_unavailable"
    assert snapshot.degraded_venues == ["aster", "binance"]
    loaded = load_spread_snapshot(tmp_path / "spread-current.json")
    assert loaded is not None
    assert loaded.source_mode == "sidecar_snapshot_unavailable"


@pytest.mark.asyncio
async def test_spread_sidecar_direct_market_fallback_requires_explicit_config(
    tmp_path,
) -> None:
    config = AppConfig(
        symbols=["BTCUSDT"],
        runtime=RuntimeConfig(
            sidecar_snapshot_path=str(tmp_path / "missing-sidecar.json"),
            spread_sidecar_snapshot_path=str(tmp_path / "spread-current.json"),
            spread_sidecar_source_mode="direct_market",
            spread_sidecar_direct_fetch_enabled=True,
        ),
        strategy=StrategyConfig(
            spread_reversion_enabled=True,
            spread_min_samples=2,
            spread_min_history_ms=0,
            spread_fair_price_min_venues=2,
            spread_min_fair_price_confidence=0.0,
            spread_min_liquidity_capacity_ratio=1.0,
            spread_entry_z=0.0,
            spread_min_net_edge_bps=0.0,
        ),
        venues=[VenueConfig(venue="cheap"), VenueConfig(venue="rich")],
    )
    service = SpreadSidecarService(config)

    class FakeSource:
        def __init__(self, venue: str) -> None:
            self.venue = venue

        async def fetch_all(self, symbols):
            assert symbols == ["BTCUSDT"]
            if self.venue == "cheap":
                return {
                    "cheap:BTCUSDT": QuoteSnapshot(
                        venue="cheap",
                        symbol="BTCUSDT",
                        bid=99.9,
                        ask=100.0,
                        bid_size=10.0,
                        ask_size=10.0,
                        observed_at_ms=30_000,
                    )
                }
            return {
                "rich:BTCUSDT": QuoteSnapshot(
                    venue="rich",
                    symbol="BTCUSDT",
                    bid=101.0,
                    ask=101.1,
                    bid_size=10.0,
                    ask_size=10.0,
                    observed_at_ms=30_000,
                )
            }

    service._exchange_sources = {"cheap": FakeSource("cheap"), "rich": FakeSource("rich")}
    await service.refresh_once(now_ms=30_000)
    snapshot = await service.refresh_once(now_ms=30_001)

    assert snapshot.candidates
    assert snapshot.source_mode == "direct_market_fallback"
