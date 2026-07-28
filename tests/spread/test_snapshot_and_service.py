from __future__ import annotations

from dataclasses import replace
import json

import pytest

from lightfee.config.schema import (
    AppConfig,
    PersistenceConfig,
    RuntimeConfig,
    StrategyConfig,
    VenueConfig,
)
from lightfee.sidecar.publisher import load_snapshot, publish_snapshot
from lightfee.sidecar.snapshot import (
    FundingLifecycle,
    LiquidityLifecycle,
    MarketLifecycle,
    QuoteSnapshot,
    SidecarSnapshot,
    funding_rate_sample_id,
)
from lightfee.spread.models import SpreadReversionCandidate, SpreadSnapshot
from lightfee.spread.paper_runtime import (
    SpreadPaperConfig,
    SpreadPaperJournal,
    SpreadPaperTracker,
    load_paper_checkpoint,
    publish_paper_checkpoint,
)
from lightfee.spread.publisher import load_spread_snapshot, publish_spread_snapshot
from lightfee.spread.service import SpreadSidecarService


def _quote(venue: str, *, observed_at_ms: int = 10_000) -> QuoteSnapshot:
    funding_timestamp_ms = observed_at_ms + 28_800_000
    return QuoteSnapshot(
        venue=venue,
        symbol="BTCUSDT",
        bid=99.9 if venue == "cheap" else 101.0,
        ask=100.0 if venue == "cheap" else 101.1,
        bid_size=10.0,
        ask_size=10.0,
        bid_depth=[(99.9 if venue == "cheap" else 101.0, 10.0)],
        ask_depth=[(100.0 if venue == "cheap" else 101.1, 10.0)],
        observed_at_ms=observed_at_ms,
        funding_rate_bps=1.0,
        funding_rate_observed_at_ms=observed_at_ms,
        funding_rate_event_at_ms=observed_at_ms,
        funding_rate_received_at_ms=observed_at_ms,
        funding_rate_source="test_fixture",
        funding_rate_sample_id=funding_rate_sample_id(
            venue=venue,
            symbol="BTCUSDT",
            observed_at_ms=observed_at_ms,
            rate_bps=1.0,
            funding_timestamp_ms=funding_timestamp_ms,
        ),
        funding_timestamp_ms=funding_timestamp_ms,
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


def _snapshot(
    *,
    published_at_ms: int = 10_000,
    degraded_venues: list[str] | None = None,
) -> SidecarSnapshot:
    degraded = set(degraded_venues or [])
    lifecycle_rows = {
        venue: {
            "observed_at_ms": published_at_ms,
            "symbol_count": 1,
            "coverage_usable": 0 if venue in degraded else 1,
            "degraded_reason": "test venue degraded" if venue in degraded else "",
        }
        for venue in ("cheap", "rich")
    }
    return SidecarSnapshot(
        published_at_ms=published_at_ms,
        market_observed_at_ms=published_at_ms,
        candidate_build_observed_at_ms=published_at_ms,
        candidate_build_diagnostics={
            "input_quote_count": 2,
            "requested_symbol_count": 1,
            "requested_symbols": ["BTCUSDT"],
            "requested_venues": ["cheap", "rich"],
            "directional_pair_count": 0,
            "output_candidate_count": 0,
            "future_input_quote_count": 0,
            "rejection_counts": {},
        },
        source_mode="direct_market",
        acquisition_mode="degraded_sidecar" if degraded else "fresh_sidecar",
        degraded_venues=list(degraded_venues or []),
        funding_lifecycle=[
            FundingLifecycle(venue=venue, **values)
            for venue, values in lifecycle_rows.items()
        ],
        market_lifecycle=[
            MarketLifecycle(venue=venue, **values)
            for venue, values in lifecycle_rows.items()
        ],
        liquidity_lifecycle=[
            LiquidityLifecycle(venue=venue, **values)
            for venue, values in lifecycle_rows.items()
        ],
        quotes={
            "cheap:BTCUSDT": _quote("cheap", observed_at_ms=published_at_ms),
            "rich:BTCUSDT": _quote("rich", observed_at_ms=published_at_ms),
        },
    )


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
        canonical_venue_a="cheap",
        canonical_venue_b="rich",
        equilibrium_spread_bps=-8.0,
        economics_complete=True,
        contract_normalization_status="complete",
    )


def _config(tmp_path, *, paper_enabled: bool = False) -> AppConfig:
    return AppConfig(
        symbols=["BTCUSDT"],
        venues=[
            VenueConfig(venue="cheap", taker_fee_bps=5.0),
            VenueConfig(venue="rich", taker_fee_bps=5.0),
        ],
        runtime=RuntimeConfig(
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            spread_sidecar_snapshot_path=str(tmp_path / "spread.json"),
            sidecar_snapshot_max_age_ms=60_000,
        ),
        strategy=StrategyConfig(
            spread_paper_enabled=paper_enabled,
            spread_signal_ttl_ms=1_000,
            spread_quote_skew_ms=250,
            spread_paper_require_l2_vwap=True,
        ),
        persistence=PersistenceConfig(
            spread_paper_event_log_path=str(tmp_path / "spread-paper.jsonl"),
        ),
    )


def test_spread_snapshot_round_trip_keeps_signal_and_paper_status(tmp_path) -> None:
    path = tmp_path / "spread.json"
    publish_spread_snapshot(
        SpreadSnapshot(
            published_at_ms=2_000,
            market_observed_at_ms=1_990,
            source_mode="sidecar_snapshot",
            valid_quote_count=2,
            paper_configured_enabled=True,
            paper_admission_enabled=True,
            candidates=[_candidate()],
        ),
        path,
    )

    loaded = load_spread_snapshot(path)

    assert loaded is not None
    assert loaded.source_mode == "sidecar_snapshot"
    assert loaded.valid_quote_count == 2
    assert loaded.paper_admission_enabled is True
    assert loaded.candidates[0].candidate_id == _candidate().candidate_id


@pytest.mark.asyncio
async def test_spread_sidecar_reads_the_funding_snapshot_directly(tmp_path) -> None:
    config = _config(tmp_path)
    publish_snapshot(_snapshot(), config.runtime.sidecar_snapshot_path)
    service = SpreadSidecarService(config)
    try:
        quotes, degraded, source, input_count, observed, symbols, decision = (
            await service._fetch_quotes(10_000)
        )
    finally:
        await service.close()

    assert set(quotes) == {"cheap:BTCUSDT", "rich:BTCUSDT"}
    assert degraded == set()
    assert source == "sidecar_snapshot"
    assert input_count == 2
    assert observed == 10_000
    assert symbols == {}
    assert decision == 10_000


@pytest.mark.asyncio
async def test_degraded_venue_only_removes_its_spread_quote(tmp_path) -> None:
    config = _config(tmp_path)
    publish_snapshot(
        _snapshot(degraded_venues=["cheap"]),
        config.runtime.sidecar_snapshot_path,
    )
    service = SpreadSidecarService(config)
    try:
        quotes, degraded, source, input_count, *_rest = await service._fetch_quotes(
            10_000
        )
    finally:
        await service.close()

    assert set(quotes) == {"rich:BTCUSDT"}
    assert degraded == {"cheap"}
    assert source == "sidecar_snapshot_partial"
    assert input_count == 2


@pytest.mark.asyncio
async def test_stale_funding_snapshot_disables_only_spread_input(tmp_path) -> None:
    config = _config(tmp_path)
    publish_snapshot(_snapshot(published_at_ms=1_000), config.runtime.sidecar_snapshot_path)
    service = SpreadSidecarService(config)
    try:
        quotes, degraded, source, input_count, *_rest = await service._fetch_quotes(
            10_001
        )
    finally:
        await service.close()

    assert quotes == {}
    assert degraded == {"cheap", "rich"}
    assert source == "sidecar_snapshot_stale"
    assert input_count == 2
    assert load_snapshot(config.runtime.sidecar_snapshot_path) is not None


def test_paper_jsonl_and_atomic_checkpoint_restore_open_position(tmp_path) -> None:
    journal_path = tmp_path / "paper.jsonl"
    checkpoint_path = tmp_path / "paper.checkpoint.json"
    journal = SpreadPaperJournal(journal_path)
    journal.open()
    try:
        journal.append_many([("paper.test", {"value": 1})], ts_ms=1_000)
    finally:
        journal.close()

    record = json.loads(journal_path.read_text().strip())
    assert record == {"ts_ms": 1_000, "kind": "paper.test", "payload": {"value": 1}}

    tracker = SpreadPaperTracker(
        SpreadPaperConfig(
            enabled=True,
            min_decision_latency_ms=250,
            terminal_secs=60,
            stop_z=999.0,
            require_l2_vwap=True,
            quote_ttl_ms=1_000,
        )
    )
    quotes = {"cheap:BTCUSDT": _quote("cheap", observed_at_ms=1_000), "rich:BTCUSDT": _quote("rich", observed_at_ms=1_000)}
    assert tracker.register_many(
        _candidate(),
        quotes,
        finalist_rank=0,
        decision_at_ms=1_000,
    )
    later_quotes = {
        key: replace(quote, observed_at_ms=1_250)
        for key, quote in quotes.items()
    }
    assert any(
        event["kind"] == "opportunity.paper_filled"
        for event in tracker.evaluate_due(1_250, later_quotes)
    )
    publish_paper_checkpoint(checkpoint_path, tracker.checkpoint())

    restored = SpreadPaperTracker(tracker.config)
    assert restored.restore_checkpoint(load_paper_checkpoint(checkpoint_path)) is True
    assert restored.tracked_count == 1


@pytest.mark.asyncio
async def test_corrupt_paper_checkpoint_does_not_break_funding_snapshot(tmp_path) -> None:
    config = _config(tmp_path, paper_enabled=True)
    funding_snapshot = _snapshot()
    publish_snapshot(funding_snapshot, config.runtime.sidecar_snapshot_path)
    checkpoint = tmp_path / "spread-paper.jsonl.checkpoint.json"
    checkpoint.write_text("not-json", encoding="utf-8")

    service = SpreadSidecarService(config)
    try:
        assert service._paper_tracker.enabled is False
        spread = await service.refresh_once(now_ms=10_000)
    finally:
        await service.close()

    assert spread.paper_configured_enabled is True
    assert spread.paper_admission_enabled is False
    reloaded_funding = load_snapshot(config.runtime.sidecar_snapshot_path)
    assert reloaded_funding is not None
    assert set(reloaded_funding.quotes) == set(funding_snapshot.quotes)
