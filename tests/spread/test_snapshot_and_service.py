from __future__ import annotations

import json

import pytest

from lightfee.config.schema import (
    AppConfig,
    PersistenceConfig,
    RuntimeConfig,
    StrategyConfig,
    VenueConfig,
)
from lightfee.persistence.journal import Journal
from lightfee.sidecar.publisher import load_snapshot as load_sidecar_snapshot, publish_snapshot
from lightfee.sidecar.snapshot import (
    FundingLifecycle,
    LiquidityLifecycle,
    MarketLifecycle,
    QuoteSnapshot,
    SidecarSnapshot,
)
from lightfee.spread.models import SpreadReversionCandidate, SpreadSnapshot
from lightfee.spread.publisher import load_spread_snapshot, publish_spread_snapshot
from lightfee.spread.service import (
    SpreadSidecarService,
    _publish_paper_journal_checkpoint,
    _publish_paper_journal_head,
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
    lifecycle_kwargs = {
        "observed_at_ms": observed_at_ms,
        "symbol_count": 1,
        "coverage_usable": 1,
        "degraded_reason": "",
    }
    return SidecarSnapshot(
        published_at_ms=observed_at_ms,
        market_observed_at_ms=observed_at_ms,
        candidate_build_observed_at_ms=observed_at_ms,
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
        acquisition_mode="fresh_sidecar",
        funding_lifecycle=[
            FundingLifecycle(venue=venue, **lifecycle_kwargs)
            for venue in ("cheap", "rich")
        ],
        market_lifecycle=[
            MarketLifecycle(venue=venue, **lifecycle_kwargs)
            for venue in ("cheap", "rich")
        ],
        liquidity_lifecycle=[
            LiquidityLifecycle(venue=venue, **lifecycle_kwargs)
            for venue in ("cheap", "rich")
        ],
        quotes={
            "cheap:BTCUSDT": QuoteSnapshot(
                venue="cheap",
                symbol="BTCUSDT",
                bid=99.9,
                ask=100.0,
                bid_size=10.0,
                ask_size=10.0,
                observed_at_ms=observed_at_ms,
                funding_rate_bps=1.0,
                funding_timestamp_ms=observed_at_ms + 28_800_000,
                funding_interval_ms=28_800_000,
            ),
            "rich:BTCUSDT": QuoteSnapshot(
                venue="rich",
                symbol="BTCUSDT",
                bid=101.0,
                ask=101.1,
                bid_size=10.0,
                ask_size=10.0,
                observed_at_ms=observed_at_ms,
                funding_rate_bps=2.0,
                funding_timestamp_ms=observed_at_ms + 28_800_000,
                funding_interval_ms=28_800_000,
            ),
        },
    )


def test_spread_snapshot_v4_round_trips_signed_economics_and_proof(tmp_path) -> None:
    path = tmp_path / "spread.json"
    publish_spread_snapshot(
        SpreadSnapshot(
            published_at_ms=2_000,
            market_observed_at_ms=1_990,
            source_mode="sidecar_snapshot",
            input_quote_count=20,
            valid_quote_count=18,
            paper_configured_enabled=True,
            paper_admission_enabled=True,
            paper_tracked_count=3,
            paper_admission_rejection_counts={"paper_duplicate": 2},
            candidates=[_candidate()],
        ),
        path,
    )

    loaded = load_spread_snapshot(path)

    assert loaded is not None
    assert loaded.schema_version == 4
    assert loaded.candidates[0].economics_complete is True
    assert loaded.candidates[0].fee_evidence_complete is True
    assert loaded.candidates[0].canonical_venue_a == "cheap"
    assert loaded.candidates[0].gross_reversion_edge_bps == pytest.approx(8.0)
    assert loaded.input_quote_count == 20
    assert loaded.valid_quote_count == 18
    assert loaded.paper_configured_enabled is True
    assert loaded.paper_admission_enabled is True
    assert loaded.paper_tracked_count == 3
    assert loaded.paper_admission_rejection_counts == {"paper_duplicate": 2}
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
    publish_spread_snapshot(SpreadSnapshot(published_at_ms=2_000, candidates=[_candidate()]), path)
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
    publish_spread_snapshot(SpreadSnapshot(published_at_ms=2_000, candidates=[_candidate()]), path)
    raw = json.loads(path.read_text())
    raw["candidates"][0]["expected_net_edge_bps"] = True
    path.write_text(json.dumps(raw), encoding="utf-8")

    loaded = load_spread_snapshot(path)

    assert loaded is not None
    candidate = loaded.candidates[0]
    assert candidate.economics_complete is False
    assert candidate.expected_net_edge_bps == 0.0
    assert "spread_snapshot_invalid_numeric:expected_net_edge_bps" in (candidate.screening_reasons)


def test_spread_snapshot_boolean_economics_timestamp_becomes_diagnostic_only(
    tmp_path,
) -> None:
    path = tmp_path / "spread-invalid-ts-bool.json"
    publish_spread_snapshot(SpreadSnapshot(published_at_ms=2_000, candidates=[_candidate()]), path)
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


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("expected_net_edge_bps", "12.3"),
        ("sample_count", 1.9),
    ],
)
def test_spread_snapshot_v4_rejects_coercible_numeric_scalars(
    tmp_path,
    field,
    value,
) -> None:
    path = tmp_path / "spread-coercible-number.json"
    publish_spread_snapshot(
        SpreadSnapshot(published_at_ms=2_000, candidates=[_candidate()]),
        path,
    )
    raw = json.loads(path.read_text())
    raw["candidates"][0][field] = value
    path.write_text(json.dumps(raw), encoding="utf-8")

    loaded = load_spread_snapshot(path)

    assert loaded is not None
    candidate = loaded.candidates[0]
    assert candidate.economics_complete is False
    assert f"spread_snapshot_invalid_numeric:{field}" in candidate.screening_reasons


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
async def test_refresh_paper_commits_settlement_before_terminal_evaluation(
    tmp_path,
    monkeypatch,
) -> None:
    config = AppConfig(
        symbols=["BTCUSDT"],
        runtime=RuntimeConfig(
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            spread_sidecar_snapshot_path=str(tmp_path / "spread.json"),
        ),
        persistence=PersistenceConfig(
            spread_paper_event_log_path=str(tmp_path / "paper.jsonl"),
            spread_paper_rollback_anchor_path=str(
                tmp_path / "rollback-anchor" / "spread-paper.epoch"
            ),
        ),
        strategy=StrategyConfig(
            spread_reversion_enabled=True,
            spread_paper_enabled=True,
        ),
        venues=[VenueConfig(venue="cheap"), VenueConfig(venue="rich")],
    )
    service = SpreadSidecarService(config)
    calls: list[str] = []

    def register_many(*args, **kwargs):
        calls.append("register")
        return [{"kind": "opportunity.paper_registered", "payload": {"paper_id": "p"}}]

    def record_settlements(*args, **kwargs):
        calls.append("settlement")
        return [{
            "kind": "opportunity.paper_funding_settlement_observed",
            "payload": {"paper_id": "p", "amount_quote": 1.0},
        }]

    def evaluate_due(*args, **kwargs):
        calls.append("evaluate")
        return [{
            "kind": "opportunity.paper_closed",
            "payload": {"paper_id": "p", "paper_net_quote": 2.0},
        }]

    monkeypatch.setattr(service._paper_tracker, "register_many", register_many)
    monkeypatch.setattr(
        service._paper_tracker,
        "record_observed_public_funding_settlements",
        record_settlements,
    )
    monkeypatch.setattr(service._paper_tracker, "evaluate_due", evaluate_due)

    result = await service._refresh_paper([_candidate()], {}, 1_000)

    assert result["status"] == "success"
    assert result["event_count"] == 3
    assert calls == ["register", "settlement", "evaluate"]
    records = [json.loads(line) for line in (tmp_path / "paper.jsonl").read_text().splitlines()]
    event_kinds = [
        record["kind"]
        for record in records
        if str(record.get("kind", "")).startswith("opportunity.")
    ]
    assert event_kinds == [
        "opportunity.paper_registered",
        "opportunity.paper_funding_settlement_observed",
        "opportunity.paper_closed",
    ]
    await service.close()


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
    assert snapshot.evaluated_pair_count == 1
    assert snapshot.rejection_counts == {"contract_normalization_incomplete": 1}


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
async def test_spread_production_decision_clock_is_after_snapshot_read(
    tmp_path,
    monkeypatch,
) -> None:
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
    publish_snapshot(_sidecar_snapshot(observed_at_ms=10_200), sidecar_path)
    service = SpreadSidecarService(config)
    real_load_snapshot = load_sidecar_snapshot
    clock = {"read_complete": False, "post_read_calls": 0}

    def delayed_load_snapshot(path):
        loaded = real_load_snapshot(path)
        clock["read_complete"] = True
        return loaded

    def fake_time() -> float:
        if not clock["read_complete"]:
            return 10.0
        clock["post_read_calls"] += 1
        return 10.2 + 0.1 * clock["post_read_calls"]

    monkeypatch.setattr("lightfee.spread.service.load_snapshot", delayed_load_snapshot)
    monkeypatch.setattr("lightfee.spread.service.time.time", fake_time)

    snapshot = await service.refresh_once()

    assert snapshot.source_mode != "sidecar_snapshot_stale"
    assert snapshot.decision_at_ms >= 10_200
    assert snapshot.published_at_ms >= snapshot.decision_at_ms
    assert clock["post_read_calls"] == 2


@pytest.mark.asyncio
async def test_spread_prefers_compact_quote_snapshot_over_full_snapshot(
    tmp_path,
    monkeypatch,
) -> None:
    from lightfee.spread.quote_snapshot import (
        SpreadQuoteSnapshot,
        publish_spread_quote_snapshot,
        spread_quote_snapshot_path,
    )

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
    source = _sidecar_snapshot(observed_at_ms=10_000)
    publish_spread_quote_snapshot(
        SpreadQuoteSnapshot(
            published_at_ms=10_000,
            market_observed_at_ms=10_000,
            batch_started_at_ms=9_900,
            configured_venues=["cheap", "rich"],
            degraded_venues=[],
            degraded_symbols={},
            quotes=source.quotes,
        ),
        spread_quote_snapshot_path(sidecar_path),
    )
    monkeypatch.setattr(
        "lightfee.spread.service.load_snapshot",
        lambda _path: (_ for _ in ()).throw(AssertionError("full snapshot must not be read")),
    )
    service = SpreadSidecarService(config)

    snapshot = await service.refresh_once(now_ms=10_000)

    assert snapshot.source_mode == "sidecar_snapshot"
    assert snapshot.valid_quote_count == 2


@pytest.mark.asyncio
async def test_spread_fails_closed_on_malformed_compact_snapshot(tmp_path) -> None:
    from lightfee.spread.quote_snapshot import spread_quote_snapshot_path

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
    publish_snapshot(_sidecar_snapshot(observed_at_ms=10_000), sidecar_path)
    spread_quote_snapshot_path(sidecar_path).write_text("{malformed")
    service = SpreadSidecarService(config)

    snapshot = await service.refresh_once(now_ms=10_000)

    assert snapshot.source_mode == "sidecar_snapshot_unavailable"
    assert snapshot.valid_quote_count == 0


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
    assert spread_snapshot.degraded_venues == []
    assert spread_snapshot.degraded_symbols == {"rich": ["BTCUSDT"]}


@pytest.mark.asyncio
async def test_spread_sidecar_uses_per_quote_freshness_when_global_market_watermark_is_old(
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
    snapshot.market_observed_at_ms = 8_000
    publish_snapshot(snapshot, sidecar_path)
    service = SpreadSidecarService(config)

    spread_snapshot = await service.refresh_once(now_ms=10_000)

    assert spread_snapshot.source_mode == "sidecar_snapshot"
    assert spread_snapshot.input_quote_count == 2
    assert spread_snapshot.valid_quote_count == 2
    assert spread_snapshot.market_observed_at_ms == 10_000
    assert spread_snapshot.degraded_venues == []


@pytest.mark.asyncio
async def test_spread_sidecar_excludes_degraded_venues_and_symbols(tmp_path) -> None:
    sidecar_path = tmp_path / "sidecar-current.json"
    spread_path = tmp_path / "spread-current.json"
    config = AppConfig(
        symbols=["BTCUSDT", "ETHUSDT"],
        runtime=RuntimeConfig(
            sidecar_snapshot_path=str(sidecar_path),
            spread_sidecar_snapshot_path=str(spread_path),
            sidecar_snapshot_max_age_ms=1_000,
        ),
        strategy=StrategyConfig(spread_reversion_enabled=True),
        venues=[
            VenueConfig(venue="cheap"),
            VenueConfig(venue="rich"),
            VenueConfig(venue="degraded"),
        ],
    )
    snapshot = _sidecar_snapshot(observed_at_ms=10_000)
    snapshot.quotes["degraded:BTCUSDT"] = QuoteSnapshot(
        venue="degraded",
        symbol="BTCUSDT",
        bid=102.0,
        ask=102.1,
        bid_size=10.0,
        ask_size=10.0,
        observed_at_ms=10_000,
        funding_rate_bps=1.0,
        funding_timestamp_ms=28_810_000,
        funding_interval_ms=28_800_000,
    )
    snapshot.quotes["cheap:ETHUSDT"] = QuoteSnapshot(
        venue="cheap",
        symbol="ETHUSDT",
        bid=50.0,
        ask=50.1,
        bid_size=10.0,
        ask_size=10.0,
        observed_at_ms=10_000,
        funding_rate_bps=1.0,
        funding_timestamp_ms=28_810_000,
        funding_interval_ms=28_800_000,
    )
    snapshot.quotes["rich:ETHUSDT"] = QuoteSnapshot(
        venue="rich",
        symbol="ETHUSDT",
        bid=50.2,
        ask=50.3,
        bid_size=10.0,
        ask_size=10.0,
        observed_at_ms=10_000,
        funding_rate_bps=2.0,
        funding_timestamp_ms=28_810_000,
        funding_interval_ms=28_800_000,
    )
    snapshot.degraded_venues = ["degraded"]
    snapshot.degraded_symbols = {"rich": ["ETHUSDT"]}
    snapshot.acquisition_mode = "degraded_sidecar"
    for lifecycle_name, lifecycle_type in (
        ("funding_lifecycle", FundingLifecycle),
        ("market_lifecycle", MarketLifecycle),
        ("liquidity_lifecycle", LiquidityLifecycle),
    ):
        setattr(
            snapshot,
            lifecycle_name,
            [
                lifecycle_type(
                    venue="cheap",
                    observed_at_ms=10_000,
                    symbol_count=2,
                    coverage_usable=2,
                ),
                lifecycle_type(
                    venue="rich",
                    observed_at_ms=10_000,
                    symbol_count=2,
                    coverage_usable=1,
                    degraded_reason="ETHUSDT: fetch failed",
                ),
                lifecycle_type(
                    venue="degraded",
                    observed_at_ms=10_000,
                    symbol_count=2,
                    coverage_usable=0,
                    degraded_reason="market unavailable",
                ),
            ],
        )
    snapshot.candidate_build_diagnostics["input_quote_count"] = len(snapshot.quotes)
    snapshot.candidate_build_diagnostics["requested_symbol_count"] = 2
    snapshot.candidate_build_diagnostics["requested_symbols"] = ["BTCUSDT", "ETHUSDT"]
    snapshot.candidate_build_diagnostics["requested_venues"] = [
        "cheap",
        "degraded",
        "rich",
    ]
    publish_snapshot(snapshot, sidecar_path)
    service = SpreadSidecarService(config)

    quotes, degraded, mode, input_count, _, degraded_symbols, _ = (
        await service._fetch_quotes(10_000)
    )

    assert input_count == 5
    assert set(quotes) == {"cheap:BTCUSDT", "rich:BTCUSDT", "cheap:ETHUSDT"}
    assert degraded == {"degraded"}
    assert degraded_symbols == {"rich": ["ETHUSDT"]}
    assert mode == "sidecar_snapshot_partial"


@pytest.mark.asyncio
async def test_spread_quote_filter_is_order_independent_per_symbol(tmp_path) -> None:
    sidecar_path = tmp_path / "sidecar-current.json"
    config = AppConfig(
        symbols=["BTCUSDT", "ETHUSDT"],
        runtime=RuntimeConfig(
            sidecar_snapshot_path=str(sidecar_path),
            spread_sidecar_snapshot_path=str(tmp_path / "spread-current.json"),
            sidecar_snapshot_max_age_ms=1_000,
        ),
        strategy=StrategyConfig(spread_reversion_enabled=True),
        venues=[VenueConfig(venue="cheap"), VenueConfig(venue="rich")],
    )
    snapshot = _sidecar_snapshot(observed_at_ms=10_000)
    snapshot.quotes["cheap:ETHUSDT"] = QuoteSnapshot(
        venue="cheap",
        symbol="ETHUSDT",
        bid=50.0,
        ask=50.1,
        bid_size=10.0,
        ask_size=10.0,
        observed_at_ms=10_000,
        funding_rate_bps=1.0,
        funding_timestamp_ms=28_810_000,
        funding_interval_ms=28_800_000,
    )
    snapshot.quotes["rich:ETHUSDT"] = QuoteSnapshot(
        venue="rich",
        symbol="ETHUSDT",
        bid=50.2,
        ask=50.3,
        bid_size=10.0,
        ask_size=10.0,
        observed_at_ms=8_000,
        funding_rate_bps=2.0,
        funding_timestamp_ms=28_810_000,
        funding_interval_ms=28_800_000,
    )
    for lifecycle_rows in (
        snapshot.funding_lifecycle,
        snapshot.market_lifecycle,
        snapshot.liquidity_lifecycle,
    ):
        cheap = next(row for row in lifecycle_rows if row.venue == "cheap")
        cheap.symbol_count = 2
        cheap.coverage_usable = 2
        rich = next(row for row in lifecycle_rows if row.venue == "rich")
        rich.symbol_count = 2
        rich.coverage_usable = 1
        rich.degraded_reason = "ETHUSDT: stale"
    snapshot.degraded_symbols = {"rich": ["ETHUSDT"]}
    snapshot.acquisition_mode = "degraded_sidecar"
    snapshot.candidate_build_diagnostics["input_quote_count"] = len(snapshot.quotes)
    snapshot.candidate_build_diagnostics["requested_symbol_count"] = 2
    snapshot.candidate_build_diagnostics["requested_symbols"] = ["BTCUSDT", "ETHUSDT"]
    publish_snapshot(snapshot, sidecar_path)
    service = SpreadSidecarService(config)

    quotes, degraded_venues, mode, _, _, degraded_symbols, _ = (
        await service._fetch_quotes(10_000)
    )

    assert "rich:BTCUSDT" in quotes
    assert "rich:ETHUSDT" not in quotes
    assert degraded_venues == set()
    assert degraded_symbols == {"rich": ["ETHUSDT"]}
    assert mode == "sidecar_snapshot_partial"


@pytest.mark.asyncio
async def test_spread_published_at_is_after_decision_time(tmp_path, monkeypatch) -> None:
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
    publish_snapshot(_sidecar_snapshot(observed_at_ms=10_000), sidecar_path)
    service = SpreadSidecarService(config)
    clock = iter((10.0, 10.3))
    monkeypatch.setattr("lightfee.spread.service.time.time", lambda: next(clock))

    snapshot = await service.refresh_once()

    assert snapshot.decision_at_ms == 10_000
    assert snapshot.published_at_ms == 10_300


def test_spread_snapshot_malformed_diagnostics_stay_readable(tmp_path) -> None:
    path = tmp_path / "spread-malformed-diagnostics.json"
    publish_spread_snapshot(SpreadSnapshot(published_at_ms=2_000, candidates=[_candidate()]), path)
    raw = json.loads(path.read_text())
    raw["rejection_counts"] = "not-a-map"
    raw["degraded_symbols"] = "not-a-map"
    raw["degraded_venues"] = "not-a-list"
    path.write_text(json.dumps(raw), encoding="utf-8")

    loaded = load_spread_snapshot(path)

    assert loaded is not None
    assert loaded.rejection_counts == {}
    assert loaded.degraded_symbols == {}
    assert loaded.degraded_venues == []


@pytest.mark.asyncio
async def test_spread_snapshot_is_published_only_after_paper_refresh_succeeds(
    tmp_path,
) -> None:
    sidecar_path = tmp_path / "sidecar-current.json"
    spread_path = tmp_path / "spread-current.json"
    journal_path = tmp_path / "spread-paper.jsonl"
    config = AppConfig(
        symbols=["BTCUSDT"],
        runtime=RuntimeConfig(
            sidecar_snapshot_path=str(sidecar_path),
            spread_sidecar_snapshot_path=str(spread_path),
        ),
        persistence=PersistenceConfig(
            spread_paper_event_log_path=str(journal_path),
            spread_paper_rollback_anchor_path=str(
                tmp_path / "rollback-anchor" / "spread-paper.epoch"
            ),
        ),
        strategy=StrategyConfig(
            spread_reversion_enabled=True,
            spread_paper_enabled=True,
        ),
        venues=[VenueConfig(venue="cheap"), VenueConfig(venue="rich")],
    )
    publish_snapshot(_sidecar_snapshot(observed_at_ms=10_000), sidecar_path)
    service = SpreadSidecarService(config)

    snapshot = await service.refresh_once(now_ms=10_000)

    assert snapshot.paper_configured_enabled is True
    assert snapshot.paper_admission_enabled is True
    assert snapshot.paper_refresh_status == "success"
    assert snapshot.paper_last_success_at_ms == 10_000
    assert spread_path.exists()
    await service.close()


@pytest.mark.asyncio
async def test_spread_paper_journal_failure_invalidates_replay_and_keeps_snapshot_stale(
    tmp_path,
    monkeypatch,
) -> None:
    sidecar_path = tmp_path / "sidecar-current.json"
    spread_path = tmp_path / "spread-current.json"
    config = AppConfig(
        symbols=["BTCUSDT"],
        runtime=RuntimeConfig(
            sidecar_snapshot_path=str(sidecar_path),
            spread_sidecar_snapshot_path=str(spread_path),
        ),
        persistence=PersistenceConfig(
            spread_paper_event_log_path=str(tmp_path / "spread-paper.jsonl"),
            spread_paper_rollback_anchor_path=str(
                tmp_path / "rollback-anchor" / "spread-paper.epoch"
            ),
        ),
        strategy=StrategyConfig(
            spread_reversion_enabled=True,
            spread_paper_enabled=True,
        ),
        venues=[VenueConfig(venue="cheap"), VenueConfig(venue="rich")],
    )
    publish_snapshot(_sidecar_snapshot(observed_at_ms=10_000), sidecar_path)
    service = SpreadSidecarService(config)
    monkeypatch.setattr(
        service._paper_tracker,
        "evaluate_due",
        lambda observed_ms, quotes: [
            {
                "kind": "opportunity.paper_closed",
                "payload": {"paper_id": "forced-write"},
            }
        ],
    )
    assert service._paper_journal is not None
    service._paper_journal.close()

    class FailingJournal:
        path = tmp_path / "spread-paper.jsonl"

        def append_committed_batch(self, events, *, ts_ms=None, purpose=""):
            raise OSError("disk full")

        def close(self):
            return None

    service._paper_journal = FailingJournal()

    with pytest.raises(OSError, match="disk full"):
        await service.refresh_once(now_ms=10_000)

    assert service._paper_tracker.enabled is False
    assert not spread_path.exists()
    await service.close()


def test_spread_paper_restart_disables_admission_on_uncommitted_batch(tmp_path) -> None:
    journal_path = tmp_path / "spread-paper.jsonl"
    journal = Journal(journal_path)
    journal.open()
    events = [("opportunity.paper_registered", {"paper_id": "partial"})]
    envelope = journal._build_batch_envelope(
        events,
        batch_id="partial",
    )
    journal.append("journal.batch_begin", envelope, ts_ms=10_000)
    journal.append_many(events, ts_ms=10_000)
    journal.close()
    config = AppConfig(
        symbols=["BTCUSDT"],
        runtime=RuntimeConfig(
            sidecar_snapshot_path=str(tmp_path / "sidecar-current.json"),
            spread_sidecar_snapshot_path=str(tmp_path / "spread-current.json"),
        ),
        persistence=PersistenceConfig(
            spread_paper_event_log_path=str(journal_path),
            spread_paper_rollback_anchor_path=str(
                tmp_path / "rollback-anchor" / "spread-paper.epoch"
            ),
        ),
        strategy=StrategyConfig(
            spread_reversion_enabled=True,
            spread_paper_enabled=True,
        ),
        venues=[VenueConfig(venue="cheap"), VenueConfig(venue="rich")],
    )

    service = SpreadSidecarService(config)

    assert service._paper_tracker.enabled is False
    assert service._paper_journal is not None
    service._paper_journal.close()


def test_spread_paper_restart_disables_admission_when_log_and_head_are_missing(
    tmp_path,
) -> None:
    journal_path = tmp_path / "spread-paper.jsonl"
    anchor_path = tmp_path / "rollback-anchor" / "spread-paper.epoch"
    config = AppConfig(
        persistence=PersistenceConfig(
            spread_paper_event_log_path=str(journal_path),
            spread_paper_rollback_anchor_path=str(anchor_path),
        ),
        strategy=StrategyConfig(
            spread_reversion_enabled=True,
            spread_paper_enabled=True,
        ),
    )
    first = SpreadSidecarService(config)
    assert first._paper_journal is not None
    first._paper_journal.close()
    assert journal_path.exists()
    head_path = journal_path.with_name(f"{journal_path.name}.head")
    assert head_path.exists()
    assert anchor_path.exists()
    journal_path.unlink()
    head_path.unlink()

    restarted = SpreadSidecarService(config)

    assert restarted._paper_tracker.enabled is False
    assert restarted._paper_journal is not None
    restarted._paper_journal.close()


def test_spread_paper_legacy_state_is_not_implicitly_promoted(tmp_path) -> None:
    journal_path = tmp_path / "spread-paper.jsonl"
    journal = Journal(journal_path)
    journal.open()
    journal.append(
        "opportunity.paper_registered",
        {"paper_id": "legacy-unverifiable"},
        flush=True,
        ts_ms=10_000,
    )
    journal.close()
    config = AppConfig(
        persistence=PersistenceConfig(
            spread_paper_event_log_path=str(journal_path),
            spread_paper_rollback_anchor_path=str(
                tmp_path / "rollback-anchor" / "spread-paper.epoch"
            ),
        ),
        strategy=StrategyConfig(
            spread_reversion_enabled=True,
            spread_paper_enabled=True,
        ),
    )

    service = SpreadSidecarService(config)

    assert service._paper_tracker.enabled is False
    assert service._paper_journal is not None
    service._paper_journal.close()


def test_spread_paper_legacy_state_cannot_hide_behind_new_genesis(tmp_path) -> None:
    journal_path = tmp_path / "spread-paper.jsonl"
    journal = Journal(journal_path)
    journal.open()
    journal.append(
        "opportunity.paper_registered",
        {"paper_id": "legacy-unverifiable"},
        flush=True,
        ts_ms=10_000,
    )
    journal.append_committed_batch(
        [],
        ts_ms=10_001,
        purpose="spread_paper_genesis",
    )
    journal.close()
    config = AppConfig(
        persistence=PersistenceConfig(
            spread_paper_event_log_path=str(journal_path),
            spread_paper_rollback_anchor_path=str(
                tmp_path / "rollback-anchor" / "spread-paper.epoch"
            ),
        ),
        strategy=StrategyConfig(
            spread_reversion_enabled=True,
            spread_paper_enabled=True,
        ),
    )

    service = SpreadSidecarService(config)

    assert service._paper_tracker.enabled is False
    assert service._paper_journal is not None
    service._paper_journal.close()


def test_spread_paper_missing_anchor_never_promotes_existing_genesis(tmp_path) -> None:
    journal_path = tmp_path / "spread-paper.jsonl"
    anchor_path = tmp_path / "rollback-anchor" / "spread-paper.epoch"
    journal = Journal(journal_path)
    journal.open()
    journal.append_committed_batch(
        [],
        ts_ms=10_000,
        purpose="spread_paper_genesis",
    )
    genesis = journal.committed_batch_envelopes[0]
    journal.close()
    _publish_paper_journal_head(
        journal_path.with_name(f"{journal_path.name}.head"),
        genesis,
    )
    config = AppConfig(
        persistence=PersistenceConfig(
            spread_paper_event_log_path=str(journal_path),
            spread_paper_rollback_anchor_path=str(anchor_path),
        ),
        strategy=StrategyConfig(
            spread_reversion_enabled=True,
            spread_paper_enabled=True,
        ),
    )

    service = SpreadSidecarService(config)

    assert service._paper_tracker.enabled is False
    assert not anchor_path.exists()
    assert service._paper_journal is not None
    service._paper_journal.close()


def test_spread_paper_rejects_head_more_than_one_batch_behind(tmp_path) -> None:
    journal_path = tmp_path / "spread-paper.jsonl"
    config = AppConfig(
        persistence=PersistenceConfig(
            spread_paper_event_log_path=str(journal_path),
            spread_paper_rollback_anchor_path=str(
                tmp_path / "rollback-anchor" / "spread-paper.epoch"
            ),
        ),
        strategy=StrategyConfig(
            spread_reversion_enabled=True,
            spread_paper_enabled=True,
        ),
    )
    first = SpreadSidecarService(config)
    assert first._paper_journal is not None
    first._paper_journal.append_committed_batch(
        [("opportunity.paper_registered", {"paper_id": "one"})],
        ts_ms=10_000,
        purpose="spread_paper_events",
    )
    first._paper_journal.append_committed_batch(
        [("opportunity.paper_registered", {"paper_id": "two"})],
        ts_ms=10_001,
        purpose="spread_paper_events",
    )
    first._paper_journal.close()

    restarted = SpreadSidecarService(config)

    assert restarted._paper_tracker.enabled is False
    assert restarted._paper_journal is not None
    restarted._paper_journal.close()


def test_spread_paper_rejects_journal_and_head_rollback_to_genesis(tmp_path) -> None:
    journal_path = tmp_path / "spread-paper.jsonl"
    config = AppConfig(
        persistence=PersistenceConfig(
            spread_paper_event_log_path=str(journal_path),
            spread_paper_rollback_anchor_path=str(
                tmp_path / "rollback-anchor" / "spread-paper.epoch"
            ),
        ),
        strategy=StrategyConfig(
            spread_reversion_enabled=True,
            spread_paper_enabled=True,
        ),
    )
    first = SpreadSidecarService(config)
    assert first._paper_journal is not None
    genesis = first._paper_journal.committed_batch_envelopes[0]
    first._paper_journal.append_committed_batch(
        [("opportunity.paper_registered", {"paper_id": "must-not-disappear"})],
        ts_ms=10_000,
        purpose="spread_paper_events",
    )
    envelopes = first._paper_journal.committed_batch_envelopes
    _publish_paper_journal_head(first._paper_journal_head_path, envelopes[-1])
    _publish_paper_journal_checkpoint(first._paper_journal_epoch_path, envelopes)
    first._paper_journal.close()

    journal_lines = journal_path.read_text(encoding="utf-8").splitlines()
    journal_path.write_text("\n".join(journal_lines[:2]) + "\n", encoding="utf-8")
    _publish_paper_journal_head(first._paper_journal_head_path, genesis)

    restarted = SpreadSidecarService(config)

    assert restarted._paper_tracker.enabled is False
    assert restarted._paper_journal is not None
    restarted._paper_journal.close()


def test_spread_paper_hard_capacity_disables_admission(tmp_path) -> None:
    journal_path = tmp_path / "spread-paper.jsonl"
    config = AppConfig(
        persistence=PersistenceConfig(
            spread_paper_event_log_path=str(journal_path),
            spread_paper_rollback_anchor_path=str(
                tmp_path / "rollback-anchor" / "spread-paper.epoch"
            ),
            spread_paper_event_log_hard_max_bytes=4_096,
        ),
        strategy=StrategyConfig(
            spread_reversion_enabled=True,
            spread_paper_enabled=True,
        ),
    )

    service = SpreadSidecarService(config)

    assert service._paper_tracker.enabled is False
    assert journal_path.stat().st_size == 0
    assert service._paper_journal is not None
    service._paper_journal.close()


@pytest.mark.parametrize(
    "anchor_mode",
    ["missing", "relative", "journal_directory"],
)
def test_spread_paper_requires_independent_absolute_rollback_anchor(
    tmp_path,
    anchor_mode,
) -> None:
    journal_path = tmp_path / f"spread-paper-{anchor_mode}.jsonl"
    if anchor_mode == "missing":
        anchor_path = ""
    elif anchor_mode == "relative":
        anchor_path = "relative/spread-paper.epoch"
    else:
        anchor_path = str(tmp_path / f"spread-paper-{anchor_mode}.epoch")
    config = AppConfig(
        persistence=PersistenceConfig(
            spread_paper_event_log_path=str(journal_path),
            spread_paper_rollback_anchor_path=anchor_path,
        ),
        strategy=StrategyConfig(
            spread_reversion_enabled=True,
            spread_paper_enabled=True,
        ),
    )

    service = SpreadSidecarService(config)

    assert service._paper_tracker.enabled is False
    assert journal_path.exists()
    assert journal_path.stat().st_size == 0
    assert not journal_path.with_name(f"{journal_path.name}.head").exists()
    assert service._paper_journal is not None
    service._paper_journal.close()


@pytest.mark.asyncio
async def test_spread_paper_empty_refresh_does_not_grow_state_journal(
    tmp_path,
) -> None:
    journal_path = tmp_path / "spread-paper.jsonl"
    config = AppConfig(
        runtime=RuntimeConfig(
            sidecar_snapshot_path=str(tmp_path / "missing-sidecar.json"),
            spread_sidecar_snapshot_path=str(tmp_path / "spread-current.json"),
        ),
        persistence=PersistenceConfig(
            spread_paper_event_log_path=str(journal_path),
            spread_paper_rollback_anchor_path=str(
                tmp_path / "rollback-anchor" / "spread-paper.epoch"
            ),
        ),
        strategy=StrategyConfig(
            spread_reversion_enabled=True,
            spread_paper_enabled=True,
        ),
    )
    service = SpreadSidecarService(config)
    initial_lines = journal_path.read_text(encoding="utf-8").splitlines()

    await service.refresh_once(now_ms=10_000)
    await service.refresh_once(now_ms=11_000)

    assert journal_path.read_text(encoding="utf-8").splitlines() == initial_lines
    await service.close()


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
    snapshot.degraded_symbols = {"rich": ["BTCUSDT"]}
    snapshot.acquisition_mode = "degraded_sidecar"
    rich_market = next(
        row for row in snapshot.market_lifecycle if row.venue == "rich"
    )
    rich_market.coverage_usable = 0
    rich_market.degraded_reason = "BTCUSDT: crossed BBO"
    rich_liquidity = next(
        row for row in snapshot.liquidity_lifecycle if row.venue == "rich"
    )
    rich_liquidity.coverage_usable = 0
    rich_liquidity.degraded_reason = "BTCUSDT: crossed BBO"
    publish_snapshot(snapshot, sidecar_path)
    service = SpreadSidecarService(config)

    spread_snapshot = await service.refresh_once(now_ms=10_000)

    assert spread_snapshot.candidates == []
    assert spread_snapshot.source_mode == "sidecar_snapshot_degraded"
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
