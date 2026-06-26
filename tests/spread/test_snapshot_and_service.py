from __future__ import annotations

import pytest

from lightfee.config.schema import AppConfig, RuntimeConfig, StrategyConfig, VenueConfig
from lightfee.sidecar.snapshot import QuoteSnapshot
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
    )


def test_spread_snapshot_round_trips_without_funding_candidate_shape(tmp_path) -> None:
    path = tmp_path / "spread.json"
    snapshot = SpreadSnapshot(
        published_at_ms=2_000,
        market_observed_at_ms=1_990,
        candidates=[_candidate()],
    )

    publish_spread_snapshot(snapshot, path)
    loaded = load_spread_snapshot(path)

    assert loaded is not None
    assert loaded.schema_version == 1
    assert loaded.candidates[0].strategy_bucket == "spread_reversion"
    assert not hasattr(loaded.candidates[0], "opportunity_type")


@pytest.mark.asyncio
async def test_spread_sidecar_service_publishes_independent_snapshot(tmp_path) -> None:
    config = AppConfig(
        symbols=["BTCUSDT"],
        runtime=RuntimeConfig(
            spread_sidecar_snapshot_path=str(tmp_path / "spread-current.json"),
        ),
        strategy=StrategyConfig(
            spread_reversion_enabled=True,
            spread_min_samples=2,
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
                        observed_at_ms=10_000,
                    )
                }
            return {
                "rich:BTCUSDT": QuoteSnapshot(
                    venue="rich",
                    symbol="BTCUSDT",
                    bid=101.0,
                    ask=101.1,
                    observed_at_ms=10_000,
                )
            }

        async def close(self):
            pass

    service._exchange_sources = {"cheap": FakeSource("cheap"), "rich": FakeSource("rich")}
    await service.refresh_once(now_ms=10_000)
    snapshot = await service.refresh_once(now_ms=10_001)

    assert snapshot.candidates
    assert snapshot.snapshot_path.endswith("spread-current.json")
    assert (tmp_path / "spread-current.json").exists()
    loaded = load_spread_snapshot(tmp_path / "spread-current.json")
    assert loaded is not None
    assert loaded.candidates[0].long_venue == "cheap"
    assert loaded.candidates[0].short_venue == "rich"
