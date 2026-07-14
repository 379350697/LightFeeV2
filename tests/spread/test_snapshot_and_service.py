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
        candidate_id="spread:BTCUSDT:cheap:rich",
        symbol="BTCUSDT",
        long_venue="cheap",
        short_venue="rich",
        spread_mid_bps=20.0,
        executable_spread_bps=18.0,
        rolling_mean_bps=8.0,
        rolling_std_bps=4.0,
        z_score=3.0,
        net_edge_bps=12.0,
        sample_count=120,
        signal_ts_ms=1_000,
        long_quote_ts_ms=1_000,
        short_quote_ts_ms=1_000,
        entry_notional_quote=20.0,
        capacity_quote=100.0,
        signal_status="entry_ready",
        fair_price=100.05,
        canonical_venue_a="cheap",
        canonical_venue_b="rich",
        current_signed_mid_spread_bps=-20.0,
        current_executable_entry_spread_bps=-18.0,
        equilibrium_spread_bps=-8.0,
        target_exit_spread_bps=-10.0,
        gross_reversion_edge_bps=8.0,
        expected_net_edge_bps=4.0,
        worst_case_edge_bps=1.0,
        economics_complete=True,
        fee_evidence_complete=True,
        contract_normalization_status="complete",
    )


def _sidecar_snapshot(*, observed_at_ms: int = 10_000) -> SidecarSnapshot:
    return SidecarSnapshot(
        published_at_ms=observed_at_ms,
        market_observed_at_ms=observed_at_ms,
        quotes={
            "cheap:BTCUSDT": QuoteSnapshot(
                venue="cheap",
                symbol="BTCUSDT",
                bid=99.9,
                ask=100.0,
                bid_size=10.0,
                ask_size=10.0,
                observed_at_ms=observed_at_ms,
            ),
            "rich:BTCUSDT": QuoteSnapshot(
                venue="rich",
                symbol="BTCUSDT",
                bid=101.0,
                ask=101.1,
                bid_size=10.0,
                ask_size=10.0,
                observed_at_ms=observed_at_ms,
            ),
        },
    )


def test_spread_snapshot_v3_round_trips_signed_economics(tmp_path) -> None:
    path = tmp_path / "spread.json"
    publish_spread_snapshot(
        SpreadSnapshot(
            published_at_ms=2_000,
            market_observed_at_ms=1_990,
            source_mode="sidecar_snapshot",
            candidates=[_candidate()],
        ),
        path,
    )

    loaded = load_spread_snapshot(path)

    assert loaded is not None
    assert loaded.schema_version == 3
    assert loaded.candidates[0].economics_complete is True
    assert loaded.candidates[0].fee_evidence_complete is True
    assert loaded.candidates[0].canonical_venue_a == "cheap"
    assert loaded.candidates[0].gross_reversion_edge_bps == pytest.approx(8.0)
    raw = json.loads(path.read_text())
    assert raw["candidates"][0]["model_epoch"] == "v2_signed_reversion"
    assert raw["candidates"][0]["expected_net_edge_bps"] == pytest.approx(4.0)


def test_spread_snapshot_v1_stays_readable_but_is_legacy_epoch(tmp_path) -> None:
    path = tmp_path / "spread-v1.json"
    publish_spread_snapshot(
        SpreadSnapshot(schema_version=1, published_at_ms=2_000, candidates=[_candidate()]),
        path,
    )

    loaded = load_spread_snapshot(path)

    assert loaded is not None
    assert loaded.schema_version == 1
    assert loaded.candidates[0].economics_complete is False
    assert loaded.candidates[0].model_epoch == "v1_legacy"


def test_spread_snapshot_rejects_truthy_string_economics_evidence(tmp_path) -> None:
    path = tmp_path / "spread-invalid-bool.json"
    publish_spread_snapshot(
        SpreadSnapshot(published_at_ms=2_000, candidates=[_candidate()]), path
    )
    raw = json.loads(path.read_text())
    raw["candidates"][0]["economics_complete"] = "true"
    raw["candidates"][0]["fee_evidence_complete"] = "false"
    path.write_text(json.dumps(raw), encoding="utf-8")

    loaded = load_spread_snapshot(path)

    assert loaded is not None
    candidate = loaded.candidates[0]
    assert candidate.economics_complete is False
    assert candidate.fee_evidence_complete is False


def test_spread_snapshot_numeric_bool_becomes_diagnostic_only(tmp_path) -> None:
    path = tmp_path / "spread-invalid-numeric-bool.json"
    publish_spread_snapshot(
        SpreadSnapshot(published_at_ms=2_000, candidates=[_candidate()]), path
    )
    raw = json.loads(path.read_text())
    raw["candidates"][0]["expected_net_edge_bps"] = True
    path.write_text(json.dumps(raw), encoding="utf-8")

    loaded = load_spread_snapshot(path)

    assert loaded is not None
    candidate = loaded.candidates[0]
    assert candidate.economics_complete is False
    assert candidate.expected_net_edge_bps == 0.0
    assert "spread_snapshot_invalid_numeric:expected_net_edge_bps" in (
        candidate.screening_reasons
    )


def test_spread_snapshot_boolean_economics_timestamp_becomes_diagnostic_only(
    tmp_path,
) -> None:
    path = tmp_path / "spread-invalid-ts-bool.json"
    publish_spread_snapshot(
        SpreadSnapshot(published_at_ms=2_000, candidates=[_candidate()]), path
    )
    raw = json.loads(path.read_text())
    raw["candidates"][0]["economics_observed_at_ms"] = True
    path.write_text(json.dumps(raw), encoding="utf-8")

    loaded = load_spread_snapshot(path)

    assert loaded is not None
    candidate = loaded.candidates[0]
    assert candidate.economics_complete is False
    assert candidate.economics_observed_at_ms == 0
    assert "spread_snapshot_invalid_numeric:economics_observed_at_ms" in (
        candidate.screening_reasons
    )


@pytest.mark.asyncio
async def test_spread_sidecar_close_continues_after_cleanup_errors() -> None:
    closed: list[str] = []

    class FailingJournal:
        def close(self):
            closed.append("journal")
            raise RuntimeError("journal close failed")

    service = object.__new__(SpreadSidecarService)
    service._paper_journal = FailingJournal()

    await service.close()

    assert closed == ["journal"]
    assert service._paper_journal is None


@pytest.mark.asyncio
async def test_spread_sidecar_reuses_main_snapshot_and_never_direct_fetches(tmp_path) -> None:
    sidecar_path = tmp_path / "sidecar-current.json"
    spread_path = tmp_path / "spread-current.json"
    config = AppConfig(
        symbols=["BTCUSDT"],
        runtime=RuntimeConfig(
            sidecar_snapshot_path=str(sidecar_path),
            spread_sidecar_snapshot_path=str(spread_path),
        ),
        strategy=StrategyConfig(spread_reversion_enabled=True),
        venues=[VenueConfig(venue="cheap"), VenueConfig(venue="rich")],
    )
    publish_snapshot(_sidecar_snapshot(), sidecar_path)
    service = SpreadSidecarService(config)

    snapshot = await service.refresh_once(now_ms=10_000)

    assert snapshot.source_mode == "sidecar_snapshot"
    assert snapshot.snapshot_path == str(spread_path)
    assert spread_path.exists()
    assert load_spread_snapshot(spread_path) is not None


@pytest.mark.asyncio
async def test_spread_sidecar_missing_main_snapshot_is_degraded_not_synthetic(tmp_path) -> None:
    spread_path = tmp_path / "spread-current.json"
    config = AppConfig(
        symbols=["BTCUSDT"],
        runtime=RuntimeConfig(
            sidecar_snapshot_path=str(tmp_path / "missing.json"),
            spread_sidecar_snapshot_path=str(spread_path),
        ),
        strategy=StrategyConfig(spread_reversion_enabled=True),
        venues=[VenueConfig(venue="binance"), VenueConfig(venue="aster")],
    )
    service = SpreadSidecarService(config)
    snapshot = await service.refresh_once(now_ms=20_000)

    assert snapshot.candidates == []
    assert snapshot.source_mode == "sidecar_snapshot_unavailable"
    assert snapshot.degraded_venues == ["aster", "binance"]


@pytest.mark.asyncio
async def test_spread_sidecar_future_main_snapshot_is_degraded_not_fresh(tmp_path) -> None:
    sidecar_path = tmp_path / "sidecar-current.json"
    spread_path = tmp_path / "spread-current.json"
    config = AppConfig(
        symbols=["BTCUSDT"],
        runtime=RuntimeConfig(
            sidecar_snapshot_path=str(sidecar_path),
            spread_sidecar_snapshot_path=str(spread_path),
        ),
        strategy=StrategyConfig(spread_reversion_enabled=True),
        venues=[VenueConfig(venue="cheap"), VenueConfig(venue="rich")],
    )
    publish_snapshot(_sidecar_snapshot(observed_at_ms=20_001), sidecar_path)
    service = SpreadSidecarService(config)

    snapshot = await service.refresh_once(now_ms=20_000)

    assert snapshot.candidates == []
    assert snapshot.source_mode == "sidecar_snapshot_stale"
    assert snapshot.degraded_venues == ["cheap", "rich"]


@pytest.mark.asyncio
async def test_spread_sidecar_drops_individually_stale_quotes(tmp_path) -> None:
    sidecar_path = tmp_path / "sidecar-current.json"
    spread_path = tmp_path / "spread-current.json"
    config = AppConfig(
        symbols=["BTCUSDT"],
        runtime=RuntimeConfig(
            sidecar_snapshot_path=str(sidecar_path),
            spread_sidecar_snapshot_path=str(spread_path),
            sidecar_snapshot_max_age_ms=1_000,
        ),
        strategy=StrategyConfig(spread_reversion_enabled=True),
        venues=[VenueConfig(venue="cheap"), VenueConfig(venue="rich")],
    )
    snapshot = _sidecar_snapshot(observed_at_ms=10_000)
    snapshot.quotes["rich:BTCUSDT"].observed_at_ms = 8_500
    publish_snapshot(snapshot, sidecar_path)
    service = SpreadSidecarService(config)

    spread_snapshot = await service.refresh_once(now_ms=10_000)

    assert spread_snapshot.candidates == []
    assert spread_snapshot.source_mode == "sidecar_snapshot_partial"
    assert spread_snapshot.degraded_venues == ["rich"]


@pytest.mark.asyncio
async def test_spread_sidecar_rejects_when_all_quotes_are_individually_invalid(
    tmp_path,
) -> None:
    sidecar_path = tmp_path / "sidecar-current.json"
    spread_path = tmp_path / "spread-current.json"
    config = AppConfig(
        symbols=["BTCUSDT"],
        runtime=RuntimeConfig(
            sidecar_snapshot_path=str(sidecar_path),
            spread_sidecar_snapshot_path=str(spread_path),
            sidecar_snapshot_max_age_ms=1_000,
        ),
        strategy=StrategyConfig(spread_reversion_enabled=True),
        venues=[VenueConfig(venue="cheap"), VenueConfig(venue="rich")],
    )
    snapshot = _sidecar_snapshot(observed_at_ms=10_000)
    snapshot.quotes["cheap:BTCUSDT"].observed_at_ms = 8_500
    snapshot.quotes["rich:BTCUSDT"].bid = 101.2
    snapshot.quotes["rich:BTCUSDT"].ask = 101.1
    publish_snapshot(snapshot, sidecar_path)
    service = SpreadSidecarService(config)

    spread_snapshot = await service.refresh_once(now_ms=10_000)

    assert spread_snapshot.candidates == []
    assert spread_snapshot.source_mode == "sidecar_snapshot_quotes_stale"
    assert spread_snapshot.degraded_venues == ["cheap", "rich"]


def test_spread_sidecar_rejects_direct_market_fetching(tmp_path) -> None:
    config = AppConfig(
        symbols=["BTCUSDT"],
        runtime=RuntimeConfig(
            sidecar_snapshot_path=str(tmp_path / "missing.json"),
            spread_sidecar_snapshot_path=str(tmp_path / "spread-current.json"),
            spread_sidecar_source_mode="direct_market",
            spread_sidecar_direct_fetch_enabled=True,
        ),
        strategy=StrategyConfig(spread_reversion_enabled=True),
        venues=[VenueConfig(venue="cheap"), VenueConfig(venue="rich")],
    )
    with pytest.raises(ValueError, match="direct public market fetching"):
        SpreadSidecarService(config)


def test_spread_sidecar_rejects_direct_flag_even_without_direct_mode(tmp_path) -> None:
    config = AppConfig(
        symbols=["BTCUSDT"],
        runtime=RuntimeConfig(
            sidecar_snapshot_path=str(tmp_path / "missing.json"),
            spread_sidecar_snapshot_path=str(tmp_path / "spread-current.json"),
            spread_sidecar_source_mode="sidecar_snapshot",
            spread_sidecar_direct_fetch_enabled=True,
        ),
        strategy=StrategyConfig(spread_reversion_enabled=True),
        venues=[VenueConfig(venue="binance"), VenueConfig(venue="bybit")],
    )
    with pytest.raises(ValueError, match="direct public market fetching"):
        SpreadSidecarService(config)
