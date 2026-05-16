from __future__ import annotations

import json

import pytest

from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig, StrategyConfig
from lightfee.engine.runtime import LiveRuntime
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from lightfee.sidecar.snapshot import (
    CandidateInput,
    QuoteSnapshot,
    SidecarSnapshot,
    SnapshotFreshness,
)


@pytest.mark.asyncio
async def test_runtime_passes_live_scan_last_good_max_age_to_freshness(tmp_path, monkeypatch):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=10000,
            live_scan_last_good_max_age_ms=600000,
            max_market_age_ms=4000,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    observed: dict[str, int | None] = {}

    def fake_evaluate_snapshot_freshness(
        *,
        snapshot,
        max_age_ms,
        now_ms,
        last_good=None,
        last_good_max_age_ms=None,
        market_max_age_ms=None,
    ):
        observed["last_good_max_age_ms"] = last_good_max_age_ms
        observed["market_max_age_ms"] = market_max_age_ms
        return SnapshotFreshness.MISSING

    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: None)
    monkeypatch.setattr(
        "lightfee.engine.runtime.evaluate_snapshot_freshness",
        fake_evaluate_snapshot_freshness,
    )

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    assert observed["last_good_max_age_ms"] == 600000
    assert observed["market_max_age_ms"] == 4000


@pytest.mark.asyncio
async def test_runtime_last_good_fallback_requires_revalidate_before_entry(tmp_path, monkeypatch):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=10000,
            live_scan_recovery_success_count=1,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    snapshot = SidecarSnapshot(
        published_at_ms=1000,
        market_observed_at_ms=1000,
        acquisition_mode="last_good_sidecar",
        quotes={
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
            )
        },
        candidates=[
            CandidateInput(
                long_venue="binance",
                short_venue="bybit",
                symbol="BTCUSDT",
                funding_diff_bps=10.0,
                funding_edge_bps=10.0,
                expected_edge_bps=5.0,
                worst_case_edge_bps=2.0,
                ranking_edge_bps=10.0,
                entry_notional_quote=50.0,
                first_funding_timestamp_ms=300000,
            )
        ],
    )

    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: snapshot)
    monkeypatch.setattr(
        "lightfee.engine.runtime.evaluate_snapshot_freshness",
        lambda **_kwargs: SnapshotFreshness.LAST_GOOD_FALLBACK,
    )
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 0)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    assert runtime.state.last_scan["no_entry_reason"] == (
        "live_scan_revalidate_required:last_good_sidecar"
    )


@pytest.mark.asyncio
async def test_runtime_skips_entry_price_hints_older_than_max_order_quote_age(tmp_path, monkeypatch):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=600000,
            max_order_quote_age_ms=5000,
            live_scan_recovery_success_count=1,
        ),
        strategy=StrategyConfig(
            local_l2_enabled=False,
            entry_window_secs=600,
            min_scan_minutes_before_funding=0,
            min_funding_edge_bps=0,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    runtime.entry_executor = object()
    snapshot = SidecarSnapshot(
        published_at_ms=65000,
        market_observed_at_ms=60000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
            )
        },
        candidates=[
            CandidateInput(
                long_venue="binance",
                short_venue="bybit",
                symbol="BTCUSDT",
                funding_diff_bps=10.0,
                funding_edge_bps=10.0,
                expected_edge_bps=5.0,
                worst_case_edge_bps=2.0,
                ranking_edge_bps=10.0,
                entry_notional_quote=50.0,
                first_funding_timestamp_ms=400000,
            )
        ],
    )

    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: snapshot)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 70000)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    records = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    kinds = [record["kind"] for record in records]
    assert "runtime.order_quote_stale_skipped" in kinds
    assert "runtime.entry_skipped_no_quote" in kinds
    assert runtime.state.last_scan["selected_candidate_count"] == 1
    assert runtime.state.last_scan["dispatched_candidate_count"] == 0
    no_entry = [
        record for record in records
        if record["kind"] == "scan.no_entry_diagnostics"
    ]
    assert no_entry[-1]["payload"]["reason"] == "no_entry_dispatched"
