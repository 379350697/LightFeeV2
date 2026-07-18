from __future__ import annotations

import asyncio
import json
import time
from collections import Counter
from types import SimpleNamespace

import pytest

from lightfee.core.domain import Venue
from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig, StrategyConfig
from lightfee.engine.market_data_runtime import (
    EntryOpenInterestRefresher,
    _targeted_open_interest_observed_proof_valid,
)
from lightfee.engine.runtime import LiveRuntime
from lightfee.engine.recovery import recover_from_snapshot
from lightfee.engine.entry_dispatch_runtime import EntryDispatchRuntime
from lightfee.marketdata.l2 import L2BookStatus, LocalL2Book, PriceLevel
from lightfee.marketdata.local_l2_runtime import LocalL2BookKey
from lightfee.marketdata.open_interest import open_interest_sample_id
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from lightfee.sidecar.snapshot import (
    CandidateInput,
    LiquidityLifecycle,
    QuoteSnapshot,
    SidecarSnapshot,
    SnapshotFreshness,
    SnapshotFreshnessDecision,
    TransferLifecycle,
    has_usable_funding_payload,
)
from lightfee.sidecar.publisher import (
    funding_entry_snapshot_manifest_path,
    funding_entry_snapshot_path,
)
from lightfee.persistence.journal import Journal
from lightfee.persistence.snapshot_store import SnapshotStore


@pytest.fixture(autouse=True)
def _isolate_non_canary_snapshot_stages(monkeypatch):
    """Exercise snapshot freshness after the independent canary policy gate."""
    monkeypatch.setattr(
        EntryDispatchRuntime,
        "_funding_canary_admission_reason",
        lambda *_args, **_kwargs: "",
    )
    monkeypatch.setattr(
        EntryDispatchRuntime,
        "_funding_canary_submission_reason",
        lambda *_args, **_kwargs: "",
    )

    async def preserve_snapshot_scope(_runtime, candidates, **_kwargs):
        return list(candidates)

    # Catalog semantics have dedicated tests.  This module isolates snapshot,
    # quote and OI freshness so missing fake-adapter catalogs cannot preempt
    # the stage under test.
    monkeypatch.setattr(
        LiveRuntime,
        "_filter_candidates_supported_by_venue_catalog",
        preserve_snapshot_scope,
    )

    async def complete_account_truth(_runtime):
        return True

    # Private account-truth generation has dedicated entry-flow tests. Keep
    # this module focused on public quote/OI/snapshot semantics.
    monkeypatch.setattr(
        LiveRuntime,
        "_entry_account_truth_ready_for_tick",
        complete_account_truth,
    )


def _install_v6_snapshot_fixture(monkeypatch, snapshot) -> None:
    monkeypatch.setattr(
        "lightfee.engine.runtime.funding_entry_snapshot_identity",
        lambda _path: ("test-v6-generation", 1, 1),
    )
    monkeypatch.setattr(
        "lightfee.engine.runtime.load_funding_entry_snapshot",
        lambda _path: snapshot,
    )


class CapturingEntryExecutor:
    def __init__(self) -> None:
        self.contexts = []

    async def execute(self, ctx):
        from lightfee.engine.entry import EntryState
        from lightfee.engine.entry_sync import EntryExecutionResult
        from lightfee.engine.execution_planner import ExecutionRoute

        self.contexts.append(ctx)
        return EntryExecutionResult(
            route=ExecutionRoute.PASSIVE_INCREMENTAL,
            state=EntryState.COMPLETED,
        )


@pytest.mark.asyncio
async def test_entry_oi_singleflight_survives_caller_deadline_and_reuses_result():
    refresher = EntryOpenInterestRefresher(targeted_budget_s=1.0)

    class SlowClient:
        def __init__(self):
            self.calls = 0

        async def fetch_entry_open_interest_evidence(self, symbols, *, force_refresh):
            self.calls += 1
            await asyncio.sleep(0.03)
            symbol = symbols[0]
            sample_id = open_interest_sample_id(
                venue="binance",
                canonical_symbol=symbol,
                venue_symbol=symbol,
                observed_at_ms=1_000,
                source="test",
                raw_value=2_000_000.0,
                value_quote=2_000_000.0,
            )
            return {
                f"binance:{symbol}": SimpleNamespace(
                    open_interest_quote=2_000_000.0,
                    open_interest_evidence_status="observed",
                    open_interest_evidence_reason="test",
                    open_interest_observed_at_ms=1_000,
                    open_interest_received_at_ms=1_001,
                    open_interest_source="test",
                    open_interest_sample_id=sample_id,
                    open_interest_venue_symbol=symbol,
                    raw_open_interest=2_000_000.0,
                    raw_open_interest_unit="quote",
                    open_interest_contract_multiplier=1.0,
                    open_interest_conversion_mark_price=1.0,
                )
            }

    client = SlowClient()
    refresher._clients["binance"] = client
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            refresher.refresh_open_interest(
                "binance", "BTCUSDT", now_ms=1_010
            ),
            timeout=0.005,
        )
    await asyncio.sleep(0.04)

    payload = await refresher.refresh_open_interest(
        "binance", "BTCUSDT", now_ms=1_020
    )

    assert client.calls == 1
    assert payload["open_interest_evidence_status"] == "observed"
    assert payload["open_interest_quote"] == 2_000_000.0
    await refresher.close()


@pytest.mark.asyncio
async def test_entry_oi_force_and_normal_share_one_target_singleflight():
    refresher = EntryOpenInterestRefresher(targeted_budget_s=1.0)

    class SlowClient:
        def __init__(self):
            self.calls = 0

        async def fetch_entry_open_interest_evidence(self, symbols, *, force_refresh):
            self.calls += 1
            await asyncio.sleep(0.02)
            symbol = symbols[0]
            return {
                f"binance:{symbol}": SimpleNamespace(
                    open_interest_quote=2_000_000.0,
                    open_interest_evidence_status="observed",
                    open_interest_evidence_reason="test",
                    open_interest_observed_at_ms=2_000,
                    open_interest_received_at_ms=2_001,
                    open_interest_source="test",
                    open_interest_sample_id="sample-shared",
                    open_interest_venue_symbol=symbol,
                )
            }

    client = SlowClient()
    refresher._clients["binance"] = client
    normal = asyncio.create_task(
        refresher.refresh_open_interest("binance", "BTCUSDT", now_ms=2_010)
    )
    await asyncio.sleep(0)
    forced = asyncio.create_task(
        refresher.refresh_open_interest(
            "binance",
            "BTCUSDT",
            now_ms=2_010,
            force_refresh=True,
        )
    )

    normal_payload, forced_payload = await asyncio.gather(normal, forced)

    assert client.calls == 1
    assert normal_payload == forced_payload
    await refresher.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "expected_status"),
    [
        (RuntimeError("transport failed"), "http_error"),
        (ValueError("invalid payload"), "parse_error"),
        (asyncio.TimeoutError("request deadline"), "timeout"),
    ],
)
async def test_entry_oi_batch_preserves_exception_phase_timings(
    error,
    expected_status,
):
    refresher = EntryOpenInterestRefresher(targeted_budget_s=1.0)
    error.phase_timings = {
        "dns_ms": 3,
        "connect_ms": 5,
        "pool_wait_ms": 7,
        "rate_limit_wait_ms": 11,
        "transport_total_ms": 29,
        "http_ms": 13,
        "parse_ms": 17,
        "dns_timing_status": "observed",
    }

    class FailingClient:
        async def fetch_entry_open_interest_evidence(self, symbols, *, force_refresh):
            raise error

    refresher._clients["binance"] = FailingClient()

    result = await refresher.refresh_open_interest_batch(
        "binance",
        ["BTCUSDT"],
        now_ms=1_000,
    )

    payload = result["BTCUSDT"]
    assert payload["open_interest_evidence_status"] == expected_status
    assert payload["oi_dns_ms"] == 3
    assert payload["oi_connect_ms"] == 5
    assert payload["oi_pool_wait_ms"] == 7
    assert payload["oi_rate_limit_wait_ms"] == 11
    assert payload["oi_transport_total_ms"] == 29
    assert payload["oi_http_ms"] == 13
    assert payload["oi_parse_ms"] == 17
    assert payload["oi_dns_timing_status"] == "observed"
    await refresher.close()


@pytest.mark.asyncio
async def test_entry_oi_scheduler_is_bounded_reuses_and_reaps_on_close():
    refresher = EntryOpenInterestRefresher(targeted_budget_s=1.0)
    refresher._max_inflight = 4
    release = asyncio.Event()

    class BlockingClient:
        async def fetch_entry_open_interest_evidence(self, symbols, *, force_refresh):
            await release.wait()
            return {}

    refresher._clients["binance"] = BlockingClient()
    callers = [
        asyncio.create_task(
            refresher.refresh_open_interest(
                "binance",
                f"S{index}USDT",
                now_ms=1_000,
            )
        )
        for index in range(4)
    ]
    await asyncio.sleep(0)
    reused = asyncio.create_task(
        refresher.refresh_open_interest("binance", "S0USDT", now_ms=1_001)
    )
    await asyncio.sleep(0)

    deferred = await refresher.refresh_open_interest(
        "binance", "OVERUSDT", now_ms=1_002
    )
    metrics = refresher.scheduler_metrics(now_ms=1_010)

    assert deferred["open_interest_evidence_status"] == "deferred"
    assert deferred["open_interest_evidence_reason"] == (
        "entry_evidence_scheduler_capacity_exceeded"
    )
    assert metrics["inflight_count"] == metrics["max_inflight"] == 4
    assert metrics["reused_count"] == 1
    assert metrics["deferred_count"] == 1

    await refresher.close()
    await asyncio.gather(*callers, reused, return_exceptions=True)
    assert refresher.scheduler_metrics(now_ms=1_020)["inflight_count"] == 0
    assert refresher._inflight == {}
    assert refresher._inflight_started_at_ms == {}


@pytest.mark.asyncio
async def test_entry_oi_top32_allows_64_targets_without_slow_venue_starvation():
    refresher = EntryOpenInterestRefresher(targeted_budget_s=1.0)
    release_slow = asyncio.Event()

    class SlowClient:
        def __init__(self):
            self.active = 0
            self.max_active = 0

        async def fetch_entry_open_interest_evidence(self, symbols, *, force_refresh):
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            try:
                await release_slow.wait()
                return {}
            finally:
                self.active -= 1

    class FastClient:
        def __init__(self):
            self.calls = 0
            self.active = 0
            self.max_active = 0

        async def fetch_entry_open_interest_evidence(self, symbols, *, force_refresh):
            self.calls += 1
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            try:
                await asyncio.sleep(0)
                return {}
            finally:
                self.active -= 1

    slow_client = SlowClient()
    fast_client = FastClient()
    refresher._clients["binance"] = slow_client
    refresher._clients["bybit"] = fast_client
    slow_callers = [
        asyncio.create_task(
            refresher.refresh_open_interest(
                "binance",
                f"S{index}USDT",
                now_ms=1_000,
            )
        )
        for index in range(32)
    ]
    for _ in range(100):
        if refresher.scheduler_metrics(now_ms=1_001)["inflight_count"] == 32:
            break
        await asyncio.sleep(0)
    assert refresher.scheduler_metrics(now_ms=1_001)["inflight_count"] == 32

    fast_results = await asyncio.gather(
        *(
            refresher.refresh_open_interest(
                "bybit",
                f"F{index}USDT",
                now_ms=1_002,
            )
            for index in range(32)
        )
    )

    assert fast_client.calls == 32
    assert fast_client.max_active <= 2
    assert slow_client.max_active <= 2
    assert all(
        result["open_interest_evidence_reason"] == "missing_targeted_ticker"
        for result in fast_results
    )
    assert all(
        result["open_interest_evidence_reason"]
        != "entry_evidence_scheduler_capacity_exceeded"
        for result in fast_results
    )

    release_slow.set()
    await asyncio.gather(*slow_callers)
    await refresher.close()


@pytest.mark.asyncio
async def test_runtime_snapshot_generation_is_parsed_once_off_event_loop(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "sidecar.json"
    path.write_text("generation")
    config = AppConfig(
        runtime=RuntimeConfig(sidecar_snapshot_path=str(path)),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    calls = 0

    def slow_load(_path):
        nonlocal calls
        calls += 1
        time.sleep(0.03)
        return SidecarSnapshot(published_at_ms=1)

    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", slow_load)
    load_task = asyncio.create_task(runtime._sidecar_snapshot_for_tick())
    await asyncio.sleep(0.005)
    assert not load_task.done()
    first = await load_task
    second = await runtime._sidecar_snapshot_for_tick()

    assert calls == 1
    assert first is second


@pytest.mark.asyncio
async def test_runtime_keeps_verified_v6_cache_during_two_file_install_window(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "sidecar.json"
    funding_entry_snapshot_path(path).write_text("new-payload-being-installed")
    funding_entry_snapshot_manifest_path(path).write_text("old-manifest")
    config = AppConfig(
        runtime=RuntimeConfig(sidecar_snapshot_path=str(path)),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    cached = SidecarSnapshot(published_at_ms=1)
    runtime._sidecar_snapshot_cache = cached
    runtime._sidecar_snapshot_identity = ("funding-entry-v6", "old", 1, 1)
    legacy_loads = 0

    def legacy_load(_path):
        nonlocal legacy_loads
        legacy_loads += 1
        return SidecarSnapshot(published_at_ms=2)

    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", legacy_load)

    loaded = await runtime._sidecar_snapshot_for_tick()

    assert loaded is cached
    assert legacy_loads == 0
    assert runtime._sidecar_snapshot_load_task is None


@pytest.mark.asyncio
async def test_tick_cancels_previous_entry_prewarm_generation_before_early_return(
    tmp_path,
):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="paper",
            sidecar_snapshot_path=str(tmp_path / "missing-sidecar.json"),
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    started = asyncio.Event()
    cancelled = asyncio.Event()
    cancel_cleanup_finished = asyncio.Event()

    async def stale_prewarm() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            await asyncio.sleep(0.15)
            cancel_cleanup_finished.set()
            raise

    runtime._entry_evidence_prewarm_task = asyncio.create_task(stale_prewarm())
    await started.wait()
    previous_generation = runtime._entry_evidence_generation

    runtime.journal.open()
    try:
        started_monotonic = asyncio.get_running_loop().time()
        await runtime.tick()
        elapsed_s = asyncio.get_running_loop().time() - started_monotonic
    finally:
        runtime.journal.close()

    assert cancelled.is_set()
    assert elapsed_s < 0.08
    assert not cancel_cleanup_finished.is_set()
    assert runtime._entry_evidence_prewarm_task is None
    assert runtime._entry_evidence_generation == previous_generation + 1
    await asyncio.wait_for(cancel_cleanup_finished.wait(), timeout=0.5)
    await asyncio.sleep(0)
    assert not runtime._entry_evidence_cancel_cleanup_tasks


@pytest.mark.asyncio
async def test_entry_pipeline_shared_deadline_cancels_quote_and_oi_waiters(
    tmp_path,
    monkeypatch,
):
    now_ms = 100_000
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600_000,
            max_market_age_ms=600_000,
            live_scan_recovery_success_count=1,
        ),
        strategy=_entry_flow_strategy_config(
            local_l2_enabled=False,
            entry_readiness_provider="ws_bbo_quote_lease",
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
    runtime.entry_executor = CapturingEntryExecutor()
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    runtime._ENTRY_EVIDENCE_PIPELINE_DEADLINE_S = 0.02
    candidate = _freshness_candidate()
    snapshot = SidecarSnapshot(
        published_at_ms=now_ms,
        market_observed_at_ms=now_ms,
        acquisition_mode="fresh_sidecar",
        quotes={
            f"{venue}:BTCUSDT": _quote_with_liquidity(
                venue,
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms,
            )
            for venue in ("okx", "bybit")
        },
        candidates=[candidate],
    )
    quote_cancelled = asyncio.Event()
    quote_cancel_cleanup_finished = asyncio.Event()
    oi_cancelled = asyncio.Event()

    async def passthrough(candidates, *args, **kwargs):
        return list(candidates)

    async def slow_quote(*args, **kwargs):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            quote_cancelled.set()
            await asyncio.sleep(0.15)
            quote_cancel_cleanup_finished.set()
            raise

    async def slow_oi(*args, **kwargs):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            oi_cancelled.set()
            raise

    _install_v6_snapshot_fixture(monkeypatch, snapshot)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: now_ms)
    monkeypatch.setattr(
        runtime,
        "_filter_candidates_supported_by_venue_catalog",
        passthrough,
    )
    monkeypatch.setattr(
        runtime,
        "_filter_candidates_by_entry_balance_admission",
        passthrough,
    )
    monkeypatch.setattr(runtime, "_entry_quote_revalidate_for_candidates", slow_quote)
    monkeypatch.setattr(
        runtime,
        "_refresh_entry_candidate_open_interest_evidence",
        slow_oi,
    )

    runtime.journal.open()
    try:
        started_monotonic = asyncio.get_running_loop().time()
        await runtime.tick()
        elapsed_s = asyncio.get_running_loop().time() - started_monotonic
    finally:
        runtime.journal.close()

    assert quote_cancelled.is_set()
    assert oi_cancelled.is_set()
    assert elapsed_s < 0.08
    assert not quote_cancel_cleanup_finished.is_set()
    await asyncio.wait_for(quote_cancel_cleanup_finished.wait(), timeout=0.5)
    await asyncio.sleep(0)
    assert not runtime._entry_evidence_cancel_cleanup_tasks
    assert runtime.state.last_scan["no_entry_reason"] == (
        "entry_evidence_deadline_exceeded"
    )
    assert runtime.state.last_scan["entry_evidence_deadline_stage"] == (
        "quote_or_open_interest_revalidation"
    )


@pytest.mark.asyncio
async def test_runtime_unavailable_snapshot_resets_streak_and_preserves_last_good(
    tmp_path, monkeypatch
):
    config = AppConfig(
        runtime=RuntimeConfig(mode="paper"),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    prior_last_good = SidecarSnapshot(
        published_at_ms=900,
        acquisition_mode="fresh_sidecar",
        quotes={
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
            )
        },
    )
    unavailable = SidecarSnapshot(
        published_at_ms=1_000,
        market_observed_at_ms=1_000,
        candidate_build_observed_at_ms=1_000,
        acquisition_mode="unavailable",
        degraded_venues=["binance"],
    )
    runtime._last_good_snapshot = prior_last_good
    runtime._live_scan_success_streak = 3
    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: unavailable)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 1_050)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    assert runtime._live_scan_success_streak == 0
    assert runtime._last_good_snapshot is prior_last_good
    records = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    assert any(record["kind"] == "runtime.snapshot_unavailable" for record in records)


@pytest.mark.asyncio
async def test_runtime_names_incomplete_candidate_frontier_explicitly(
    tmp_path, monkeypatch
):
    config = AppConfig(
        runtime=RuntimeConfig(mode="paper"),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    unavailable = SidecarSnapshot(
        published_at_ms=1_000,
        market_observed_at_ms=1_000,
        candidate_build_observed_at_ms=1_000,
        acquisition_mode="unavailable",
        candidate_build_diagnostics={
            "source_data_ready": True,
            "seed_frontier_complete": False,
            "entry_frontier_ready": False,
            "seed_frontier_count": 64,
            "seed_pair_count": 4_811,
            "seed_frontier_stop_reason": "exact_frontier_limit_reached",
        },
    )
    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: unavailable)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 1_050)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    assert runtime.state.last_scan["no_entry_reason"] == "candidate_frontier_incomplete"
    records = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    event = next(
        record
        for record in records
        if record["kind"] == "runtime.candidate_frontier_incomplete"
    )
    assert event["payload"]["seed_frontier_complete"] is False
    assert event["payload"]["seed_frontier_count"] == 64
    assert event["payload"]["seed_pair_count"] == 4_811
    assert event["payload"]["seed_frontier_stop_reason"] == (
        "exact_frontier_limit_reached"
    )


class OkxMetadataAdapter:
    okx_base_quantity_step = 0.0
    trading_capability_trusted = True

    def passive_metadata(self, symbol: str) -> dict:
        return {
            "min_notional": 0.0,
            "min_quantity": 0.001,
            "quantity_step": 0.001,
        }


class BybitMetadataAdapter:
    trading_capability_trusted = True

    def passive_metadata(self, symbol: str) -> dict:
        return {
            "min_notional": 0.0,
            "min_quantity": 0.001,
            "quantity_step": 0.001,
        }


def _freshness_candidate(symbol: str = "BTCUSDT") -> CandidateInput:
    return CandidateInput(
        long_venue="okx",
        short_venue="bybit",
        symbol=symbol,
        funding_diff_bps=10.0,
        funding_edge_bps=200.0,
        expected_edge_bps=5.0,
        worst_case_edge_bps=2.0,
        ranking_edge_bps=10.0,
        entry_notional_quote=50.0,
        funding_timestamp_ms=400000,
        first_funding_timestamp_ms=400000,
        long_funding_timestamp_ms=400000,
        short_funding_timestamp_ms=400000,
        direction_consistent=True,
        interval_aligned=True,
        forecast_worst_funding_edge_bps=200.0,
        economics_complete=True,
        economics_observed_at_ms=1,
        calculation_version="v1_exact",
        model_epoch="v1_exact",
        taker_fee_evidence_complete=True,
    )


def test_funding_payload_rejects_expired_candidate_schedule_at_snapshot_watermark() -> None:
    candidate = _freshness_candidate()
    snapshot = SidecarSnapshot(
        published_at_ms=400_000,
        market_observed_at_ms=399_000,
        candidate_build_observed_at_ms=398_000,
        acquisition_mode="fresh_sidecar",
        candidates=[candidate],
    )

    assert has_usable_funding_payload(snapshot) is False


def test_funding_payload_rejects_two_expired_quote_schedules() -> None:
    snapshot = SidecarSnapshot(
        published_at_ms=400_000,
        market_observed_at_ms=399_000,
        candidate_build_observed_at_ms=398_000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote("okx", "BTCUSDT", 100.0, 101.0),
            "bybit:BTCUSDT": _quote("bybit", "BTCUSDT", 100.0, 101.0),
        },
    )

    assert has_usable_funding_payload(snapshot) is False


def _mark_final_economics_ready(candidate: CandidateInput, observed_at_ms: int) -> None:
    """Make a synthetic quote-freshness fixture viable after final repricing."""
    candidate.funding_edge_bps = 200.0
    candidate.forecast_worst_funding_edge_bps = 200.0
    candidate.economics_complete = True
    candidate.economics_observed_at_ms = observed_at_ms
    candidate.calculation_version = "v1_exact"
    candidate.model_epoch = "v1_exact"
    candidate.taker_fee_evidence_complete = True


def _candidate_lease_snapshot(candidate: CandidateInput, now_ms: int) -> SidecarSnapshot:
    return SidecarSnapshot(
        published_at_ms=now_ms,
        market_observed_at_ms=now_ms,
        quotes={
            f"{candidate.long_venue}:{candidate.symbol}": _quote_with_liquidity(
                candidate.long_venue,
                candidate.symbol,
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms,
            ),
            f"{candidate.short_venue}:{candidate.symbol}": _quote_with_liquidity(
                candidate.short_venue,
                candidate.symbol,
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms,
            ),
        },
        candidates=[candidate],
    )


def _sidecar_liquidity_required_candidate(symbol: str = "BTCUSDT") -> CandidateInput:
    return CandidateInput(
        long_venue="okx",
        short_venue="bybit",
        symbol=symbol,
        funding_diff_bps=10.0,
        funding_edge_bps=10.0,
        expected_edge_bps=5.0,
        worst_case_edge_bps=2.0,
        ranking_edge_bps=10.0,
        entry_notional_quote=50.0,
        first_funding_timestamp_ms=400000,
        sizing_liquidity_source="sidecar_perp_liquidity",
        forecast_worst_funding_edge_bps=10.0,
        economics_complete=True,
        economics_observed_at_ms=69_000,
        calculation_version="v1_exact",
        model_epoch="v1_exact",
        taker_fee_evidence_complete=True,
    )


def _install_l2_books(runtime: LiveRuntime, candidate: CandidateInput, *, observed_at_ms: int) -> None:
    for venue in (candidate.long_venue, candidate.short_venue):
        runtime.local_l2_runtime.books[
            LocalL2BookKey(venue=venue, symbol=candidate.symbol)
        ] = LocalL2Book(
            venue=venue,
            symbol=candidate.symbol,
            bids=[PriceLevel(price=100.0, quantity=10.0)],
            asks=[PriceLevel(price=101.0, quantity=10.0)],
            status=L2BookStatus.HOT,
            observed_at_ms=observed_at_ms,
        )


def _install_ws_quotes_from_snapshot(
    runtime: LiveRuntime,
    snapshot: SidecarSnapshot,
    *,
    now_ms: int,
) -> None:
    """Install deterministic final BBO evidence for downstream-stage tests."""
    from lightfee.marketdata.ws_bbo import TopBookQuote

    for quote in snapshot.quotes.values():
        bid = float(getattr(quote, "bid", 0.0) or 0.0)
        ask = float(getattr(quote, "ask", 0.0) or 0.0)
        if bid <= 0.0 or ask <= bid:
            continue
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue=str(getattr(quote, "venue", "") or ""),
                symbol=str(getattr(quote, "symbol", "") or ""),
                bid=bid,
                ask=ask,
                bid_size=max(float(getattr(quote, "bid_size", 0.0) or 0.0), 1.0),
                ask_size=max(float(getattr(quote, "ask_size", 0.0) or 0.0), 1.0),
                observed_at_ms=now_ms,
                received_at_ms=now_ms,
                source="test_ws_bbo",
            ),
            now_ms=now_ms,
        )


def _quote(venue: str, symbol: str, bid: float, ask: float) -> QuoteSnapshot:
    oi_value = 2_000_000.0
    oi_observed_at_ms = 69_000
    oi_source = "test_fixture"
    return QuoteSnapshot(
        venue=venue,
        symbol=symbol,
        bid=bid,
        ask=ask,
        observed_at_ms=oi_observed_at_ms,
        source="test_fixture",
        bid_size=100.0,
        ask_size=100.0,
        funding_rate_observed_at_ms=oi_observed_at_ms,
        funding_rate_received_at_ms=oi_observed_at_ms,
        funding_rate_source="test_fixture",
        funding_rate_sample_id=(
            f"funding:{venue}:{symbol}:{oi_observed_at_ms}:0:400000"
        ),
        funding_timestamp_ms=400_000,
        funding_interval_ms=28_800_000,
        volume_24h_quote=10_000_000.0,
        open_interest=oi_value,
        open_interest_evidence_status="observed",
        open_interest_observed_at_ms=oi_observed_at_ms,
        open_interest_received_at_ms=oi_observed_at_ms,
        open_interest_source=oi_source,
        open_interest_sample_id=open_interest_sample_id(
            venue=venue,
            canonical_symbol=symbol,
            venue_symbol=symbol,
            observed_at_ms=oi_observed_at_ms,
            source=oi_source,
            raw_value=oi_value,
            value_quote=oi_value,
        ),
        open_interest_venue_symbol=symbol,
        raw_open_interest=oi_value,
        raw_open_interest_unit="quote",
        open_interest_contract_multiplier=1.0,
    )


def _quote_with_liquidity(
    venue: str,
    symbol: str,
    *,
    volume_24h_quote: float,
    open_interest: float,
    observed_at_ms: int = 69000,
    open_interest_observed_at_ms: int | None = None,
) -> QuoteSnapshot:
    oi_observed_at_ms = (
        observed_at_ms
        if open_interest_observed_at_ms is None
        else open_interest_observed_at_ms
    )
    oi_source = "test_fixture"
    try:
        oi_sample_id = open_interest_sample_id(
            venue=venue,
            canonical_symbol=symbol,
            venue_symbol=symbol,
            observed_at_ms=oi_observed_at_ms,
            source=oi_source,
            raw_value=open_interest,
            value_quote=open_interest,
        )
    except (TypeError, ValueError, OverflowError):
        oi_sample_id = ""
    return QuoteSnapshot(
        venue=venue,
        symbol=symbol,
        bid=100.0,
        ask=101.0,
        observed_at_ms=observed_at_ms,
        source="sidecar_quote",
        bid_size=100.0,
        ask_size=100.0,
        funding_timestamp_ms=400_000,
        # BBO, funding and OI have independent evidence clocks.  Tests that
        # intentionally age the sidecar quote must not accidentally age the
        # funding-rate proof as well.
        funding_rate_observed_at_ms=oi_observed_at_ms,
        funding_rate_event_at_ms=oi_observed_at_ms,
        funding_rate_received_at_ms=oi_observed_at_ms,
        funding_rate_source="test_fixture",
        funding_rate_sample_id=(
            f"funding:{venue}:{symbol}:{oi_observed_at_ms}:0:400000"
        ),
        funding_interval_ms=28_800_000,
        volume_24h_quote=volume_24h_quote,
        open_interest=open_interest,
        open_interest_evidence_status="observed",
        open_interest_observed_at_ms=oi_observed_at_ms,
        open_interest_received_at_ms=oi_observed_at_ms,
        open_interest_source=oi_source,
        open_interest_sample_id=oi_sample_id,
        open_interest_venue_symbol=symbol,
        raw_open_interest=open_interest,
        raw_open_interest_unit="quote",
        open_interest_contract_multiplier=1.0,
    )


def _targeted_observed_oi_result(
    venue: str,
    symbol: str,
    now_ms: int,
    *,
    source: str,
    value_quote: float = 2_500_000.0,
) -> dict:
    return {
        "open_interest_quote": value_quote,
        "open_interest_evidence_status": "observed",
        "open_interest_evidence_reason": "targeted_refresh",
        "open_interest_observed_at_ms": now_ms,
        "open_interest_received_at_ms": now_ms,
        "open_interest_source": source,
        "open_interest_sample_id": open_interest_sample_id(
            venue=venue,
            canonical_symbol=symbol,
            venue_symbol=symbol,
            observed_at_ms=now_ms,
            source=source,
            raw_value=value_quote,
            value_quote=value_quote,
        ),
        "open_interest_venue_symbol": symbol,
        "raw_open_interest": value_quote,
        "raw_open_interest_unit": "quote",
        "open_interest_contract_multiplier": 1.0,
        "open_interest_conversion_mark_price": None,
    }


def test_targeted_oi_ingress_rejects_non_finite_and_forged_sample_proof():
    payload = _targeted_observed_oi_result(
        "okx",
        "BTCUSDT",
        70_000,
        source="test_targeted_refresh",
    )
    assert _targeted_open_interest_observed_proof_valid(
        venue="okx",
        symbol="BTCUSDT",
        result=payload,
    )

    non_finite = dict(payload, open_interest_quote=float("nan"))
    forged_sample = dict(payload, open_interest_sample_id="forged")
    assert not _targeted_open_interest_observed_proof_valid(
        venue="okx",
        symbol="BTCUSDT",
        result=non_finite,
    )
    assert not _targeted_open_interest_observed_proof_valid(
        venue="okx",
        symbol="BTCUSDT",
        result=forged_sample,
    )


def _entry_flow_strategy_config(**kwargs) -> StrategyConfig:
    return StrategyConfig(
        # Tests that exercise a successful first-leg dispatch must opt in
        # explicitly: the production-safe default keeps new funding entries
        # frozen until the canary gate is opened.
        funding_new_entries_enabled=True,
        pending_entry_pre_submit_hedgeable_fill_guard_enabled=False,
        **kwargs,
    )


def _read_journal_records(path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text().splitlines()
        if line.strip()
    ]


def test_runtime_no_tradeable_diagnostics_classifies_edge_window_and_domain():
    assert LiveRuntime._no_tradeable_reason_from_candidate_blockers(
        Counter({"funding_edge_below_floor": 2}),
        Counter(),
    ) == "candidate_edge_insufficient"
    assert LiveRuntime._no_tradeable_reason_from_candidate_blockers(
        Counter({"outside_scan_window": 1}),
        Counter(),
    ) == "candidate_window_mismatch"
    assert LiveRuntime._no_tradeable_reason_from_candidate_blockers(
        Counter(),
        Counter({"quote_stale": 1}),
    ) == "candidate_snapshot_domain_stale"


def test_ws_bbo_provider_resolves_stale_sidecar_quote_for_entry_freshness(tmp_path):
    from lightfee.marketdata.ws_bbo import TopBookQuote

    now_ms = 100_000
    config = AppConfig(
        runtime=RuntimeConfig(mode="live"),
        strategy=StrategyConfig(
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=1_500,
            local_l2_enabled=False,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    candidate = _freshness_candidate()
    candidate.pair_id = "btcusdt:okx->bybit"
    snapshot = SidecarSnapshot(
        published_at_ms=now_ms,
        market_observed_at_ms=now_ms,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote_with_liquidity(
                "okx",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 31_000,
                open_interest_observed_at_ms=now_ms - 100,
            ),
            "bybit:BTCUSDT": _quote_with_liquidity(
                "bybit",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 31_000,
                open_interest_observed_at_ms=now_ms - 100,
            ),
        },
        candidates=[candidate],
    )
    for venue, bid, ask in (
        ("okx", 100.0, 101.0),
        ("bybit", 100.2, 101.2),
    ):
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue=venue,
                symbol="BTCUSDT",
                bid=bid,
                ask=ask,
                bid_size=50.0,
                ask_size=60.0,
                observed_at_ms=now_ms - 100,
                received_at_ms=now_ms - 100,
                source=f"{venue}_bbo_ws",
            )
        )

    metrics = {}
    runtime.journal.open()
    try:
        filtered = runtime._filter_candidates_by_snapshot_freshness(
            [candidate],
            snapshot=snapshot,
            now_ms=now_ms,
            metrics=metrics,
            ages={},
        )
    finally:
        runtime.journal.close()

    assert filtered == [candidate]
    records = _read_journal_records(tmp_path / "events.jsonl")
    kinds = [record["kind"] for record in records]
    assert "runtime.quote_stale" not in kinds
    resolved = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.entry_quote_evidence_resolved_by_ws_bbo"
    ]
    assert len(resolved) == 2
    assert {payload["venue"] for payload in resolved} == {"okx", "bybit"}
    assert all(payload["source"] == "ws_bbo_quote_lease" for payload in resolved)
    assert all(payload["sidecar_reason"] == "quote_stale" for payload in resolved)
    assert all(payload["ws_bbo_lease_hit"] is True for payload in resolved)
    assert all(payload["rest_revalidate_hit"] is False for payload in resolved)
    assert runtime._last_snapshot_freshness_filter_blockers["quote_stale"] == 0
    assert metrics["okx|BTCUSDT|quote"] == {"fresh": 1, "stale": 0}
    assert metrics["bybit|BTCUSDT|quote"] == {"fresh": 1, "stale": 0}


def test_ws_bbo_provider_does_not_resolve_missing_or_invalid_sidecar_quote(tmp_path):
    from lightfee.marketdata.ws_bbo import TopBookQuote

    now_ms = 100_000
    config = AppConfig(
        runtime=RuntimeConfig(mode="live"),
        strategy=StrategyConfig(
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=1_500,
            local_l2_enabled=False,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    candidate = _freshness_candidate()
    snapshot = SidecarSnapshot(
        published_at_ms=now_ms,
        market_observed_at_ms=now_ms,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": QuoteSnapshot(
                venue="okx",
                symbol="BTCUSDT",
                bid=0.0,
                ask=101.0,
                bid_size=100.0,
                ask_size=100.0,
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms,
                source="sidecar_quote",
            ),
        },
        candidates=[candidate],
    )
    for venue, bid, ask in (
        ("okx", 100.0, 101.0),
        ("bybit", 100.2, 101.2),
    ):
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue=venue,
                symbol="BTCUSDT",
                bid=bid,
                ask=ask,
                bid_size=50.0,
                ask_size=60.0,
                observed_at_ms=now_ms - 100,
                received_at_ms=now_ms - 100,
                source=f"{venue}_bbo_ws",
            )
        )

    runtime.journal.open()
    try:
        filtered = runtime._filter_candidates_by_snapshot_freshness(
            [candidate],
            snapshot=snapshot,
            now_ms=now_ms,
            metrics={},
            ages={},
        )
    finally:
        runtime.journal.close()

    assert filtered == []
    records = _read_journal_records(tmp_path / "events.jsonl")
    resolved = [
        record for record in records
        if record["kind"] == "runtime.entry_quote_evidence_resolved_by_ws_bbo"
    ]
    reasons = {
        record["payload"]["reason"]
        for record in records
        if record["kind"] == "runtime.snapshot_freshness_decision"
        and record["payload"].get("domain") == "quote"
    }
    assert resolved == []
    assert reasons == {"invalid_quote", "missing_quote"}


def test_ws_bbo_provider_keeps_entry_fail_closed_when_bbo_cannot_resolve_stale_sidecar_quote(tmp_path):
    now_ms = 100_000
    config = AppConfig(
        runtime=RuntimeConfig(mode="live"),
        strategy=StrategyConfig(
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=1_500,
            local_l2_enabled=False,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    candidate = _freshness_candidate()
    snapshot = SidecarSnapshot(
        published_at_ms=now_ms,
        market_observed_at_ms=now_ms,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote_with_liquidity(
                "okx",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 31_000,
            ),
            "bybit:BTCUSDT": _quote_with_liquidity(
                "bybit",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 31_000,
            ),
        },
        candidates=[candidate],
    )

    runtime.journal.open()
    try:
        filtered = runtime._filter_candidates_by_snapshot_freshness(
            [candidate],
            snapshot=snapshot,
            now_ms=now_ms,
            metrics={},
            ages={},
        )
    finally:
        runtime.journal.close()

    assert filtered == []
    records = _read_journal_records(tmp_path / "events.jsonl")
    assert any(record["kind"] == "runtime.quote_stale" for record in records)
    assert not any(
        record["kind"] == "runtime.entry_quote_evidence_resolved_by_ws_bbo"
        for record in records
    )


@pytest.mark.asyncio
async def test_runtime_entry_quote_revalidate_prewarm_resolves_stale_top_candidate(
    tmp_path,
    monkeypatch,
):
    from lightfee.marketdata.ws_bbo import TopBookQuote

    now_ms = 100_000
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=600000,
            max_order_quote_age_ms=5_000,
            live_scan_recovery_success_count=1,
        ),
        strategy=_entry_flow_strategy_config(
            local_l2_enabled=False,
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=1_500,
            entry_window_secs=600,
            min_scan_minutes_before_funding=0,
            min_funding_edge_bps=0,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: OkxMetadataAdapter(),
            Venue.BYBIT: BybitMetadataAdapter(),
        },
    )
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    executor = CapturingEntryExecutor()
    runtime.entry_executor = executor
    candidate = _freshness_candidate()
    # This test exercises BBO prewarm and stale-quote replacement, not alpha.
    _mark_final_economics_ready(candidate, now_ms)
    snapshot = SidecarSnapshot(
        published_at_ms=now_ms,
        market_observed_at_ms=now_ms,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote_with_liquidity(
                "okx",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 10_000,
            ),
            "bybit:BTCUSDT": _quote_with_liquidity(
                "bybit",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 10_000,
            ),
        },
        candidates=[candidate],
    )

    async def prewarm_and_fill_cache(candidates, prewarm_now_ms):
        symbols = {str(getattr(c, "symbol", "") or "").upper() for c in candidates}
        runtime._entry_bbo_subscription_budgeted_keys = {
            ("okx", symbol) for symbol in symbols
        } | {("bybit", symbol) for symbol in symbols}
        runtime._entry_bbo_subscription_budget_excluded_keys = set()
        runtime._entry_bbo_subscription_per_venue_budget = 8
        for venue, bid, ask in (
            ("okx", 100.0, 101.0),
            ("bybit", 100.2, 101.2),
        ):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="BTCUSDT",
                    bid=bid,
                    ask=ask,
                    bid_size=50.0,
                    ask_size=60.0,
                    observed_at_ms=prewarm_now_ms - 25,
                    received_at_ms=prewarm_now_ms - 25,
                    source=f"{venue}_bbo_ws",
                )
            )

    _install_v6_snapshot_fixture(monkeypatch, snapshot)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: now_ms)
    monkeypatch.setattr(runtime, "_ensure_entry_bbo_active_for_candidates", prewarm_and_fill_cache)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    records = _read_journal_records(tmp_path / "events.jsonl")
    kinds = [record["kind"] for record in records]
    assert len(executor.contexts) == 1
    assert runtime.state.last_scan["dispatched_candidate_count"] == 1
    assert "runtime.order_quote_stale_skipped" not in kinds
    assert "runtime.quote_stale" not in kinds
    assert "runtime.entry_quote_revalidate_targeted" in kinds
    assert "runtime.entry_quote_revalidate_resolved" in kinds
    assert runtime.state.last_scan["quote_revalidate_target_count"] == 2
    assert runtime.state.last_scan["quote_revalidate_resolved_count"] == 2
    assert runtime.state.last_scan["quote_revalidate_failed_count"] == 0


@pytest.mark.asyncio
async def test_runtime_entry_ws_bbo_sticky_warm_set_retains_previous_target(
    tmp_path,
    monkeypatch,
):
    config = AppConfig(
        runtime=RuntimeConfig(mode="live"),
        strategy=StrategyConfig(
            local_l2_enabled=False,
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_ws_bbo_per_venue_budget=4,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    started: list[tuple[str, tuple[str, ...]]] = []
    pruned: list[set[tuple[str, str]]] = []

    def start_ws_streams(venue, symbols, adapter=None):
        started.append((venue, tuple(symbols)))
        return len(symbols)

    async def connect_ws_streams():
        return 0

    def prune_untracked_quotes(tracked_keys, now_ms, retained_max_age_ms):
        pruned.append(set(tracked_keys))

    monkeypatch.setattr(runtime.ws_bbo_data_plane, "start_ws_streams", start_ws_streams)
    monkeypatch.setattr(runtime.ws_bbo_data_plane, "connect_ws_streams", connect_ws_streams)
    monkeypatch.setattr(runtime.ws_bbo_data_plane, "prune_untracked_quotes", prune_untracked_quotes)

    first = _freshness_candidate("BTCUSDT")
    second = _freshness_candidate("ETHUSDT")

    runtime.journal.open()
    try:
        await runtime._ensure_entry_bbo_active_for_candidates([first], 100_000)
        started.clear()
        await runtime._ensure_entry_bbo_active_for_candidates([second], 110_000)
    finally:
        runtime.journal.close()

    started_by_venue = {venue: symbols for venue, symbols in started}
    assert started_by_venue["okx"] == ("ETHUSDT", "BTCUSDT")
    assert started_by_venue["bybit"] == ("ETHUSDT", "BTCUSDT")
    assert ("okx", "BTCUSDT") in pruned[-1]
    assert ("bybit", "BTCUSDT") in pruned[-1]
    assert ("okx", "ETHUSDT") in runtime._entry_bbo_subscription_budgeted_keys


@pytest.mark.asyncio
async def test_runtime_last_good_top_candidate_requires_entry_quote_truth(
    tmp_path,
    monkeypatch,
):
    from lightfee.marketdata.ws_bbo import TopBookQuote

    now_ms = 100_000
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=1_000,
            max_market_age_ms=1_000,
            max_order_quote_age_ms=5_000,
            live_scan_recovery_success_count=1,
        ),
        strategy=_entry_flow_strategy_config(
            local_l2_enabled=False,
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=1_500,
            entry_window_secs=600,
            min_scan_minutes_before_funding=0,
            min_funding_edge_bps=0,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: OkxMetadataAdapter(),
            Venue.BYBIT: BybitMetadataAdapter(),
        },
    )
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    executor = CapturingEntryExecutor()
    runtime.entry_executor = executor
    candidate = _freshness_candidate()
    _mark_final_economics_ready(candidate, now_ms)
    snapshot = SidecarSnapshot(
        published_at_ms=now_ms - 20_000,
        market_observed_at_ms=now_ms - 20_000,
        acquisition_mode="last_good_sidecar",
        quotes={
            "okx:BTCUSDT": _quote_with_liquidity(
                "okx",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 20_000,
                open_interest_observed_at_ms=now_ms - 100,
            ),
            "bybit:BTCUSDT": _quote_with_liquidity(
                "bybit",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 20_000,
                open_interest_observed_at_ms=now_ms - 100,
            ),
        },
        candidates=[candidate],
    )

    async def prewarm_and_fill_cache(candidates, prewarm_now_ms):
        runtime._entry_bbo_subscription_budgeted_keys = {
            ("okx", "BTCUSDT"),
            ("bybit", "BTCUSDT"),
        }
        runtime._entry_bbo_subscription_budget_excluded_keys = set()
        runtime._entry_bbo_subscription_per_venue_budget = 8
        for venue, bid, ask in (
            ("okx", 100.0, 101.0),
            ("bybit", 100.2, 101.2),
        ):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="BTCUSDT",
                    bid=bid,
                    ask=ask,
                    bid_size=50.0,
                    ask_size=60.0,
                    observed_at_ms=prewarm_now_ms - 25,
                    received_at_ms=prewarm_now_ms - 25,
                    source=f"{venue}_bbo_ws",
                )
            )

    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: snapshot)
    monkeypatch.setattr(
        "lightfee.engine.runtime.decide_snapshot_freshness",
        lambda **_kwargs: SnapshotFreshnessDecision(
            SnapshotFreshness.LAST_GOOD_FALLBACK,
            snapshot,
        ),
    )
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: now_ms)
    monkeypatch.setattr(runtime, "_ensure_entry_bbo_active_for_candidates", prewarm_and_fill_cache)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    records = _read_journal_records(tmp_path / "events.jsonl")
    kinds = [record["kind"] for record in records]
    assert len(executor.contexts) == 1
    assert runtime.state.last_scan["dispatched_candidate_count"] == 1
    assert "runtime.last_good_revalidated_by_entry_quote_truth" in kinds
    assert "runtime.quote_stale" not in kinds
    assert runtime.state.last_scan["quote_revalidate_resolved_count"] == 2


@pytest.mark.asyncio
async def test_runtime_entry_quote_revalidate_rest_fallback_updates_overlay_and_cache(
    tmp_path,
    monkeypatch,
):
    from lightfee.marketdata.ws_bbo import TopBookQuote

    now_ms = 100_000
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=600000,
            max_order_quote_age_ms=5_000,
            live_scan_recovery_success_count=1,
        ),
        strategy=_entry_flow_strategy_config(
            local_l2_enabled=False,
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=100,
            entry_window_secs=600,
            min_scan_minutes_before_funding=0,
            min_funding_edge_bps=0,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: OkxMetadataAdapter(),
            Venue.BYBIT: BybitMetadataAdapter(),
        },
    )
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    executor = CapturingEntryExecutor()
    runtime.entry_executor = executor
    candidate = _freshness_candidate()
    snapshot = SidecarSnapshot(
        published_at_ms=now_ms,
        market_observed_at_ms=now_ms,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote_with_liquidity(
                "okx",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 10_000,
            ),
            "bybit:BTCUSDT": _quote_with_liquidity(
                "bybit",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 10_000,
            ),
        },
        candidates=[candidate],
    )

    async def prewarm_without_quotes(candidates, prewarm_now_ms):
        runtime._entry_bbo_subscription_budgeted_keys = {
            ("okx", "BTCUSDT"),
            ("bybit", "BTCUSDT"),
        }
        runtime._entry_bbo_subscription_budget_excluded_keys = set()
        runtime._entry_bbo_subscription_per_venue_budget = 8

    class RestOnlyRefresher:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        def refresh_quote(self, venue: str, symbol: str, *, now_ms: int):
            self.calls.append((venue, symbol))
            return TopBookQuote(
                venue=venue,
                symbol=symbol,
                bid=100.0 if venue == "okx" else 100.2,
                ask=101.0 if venue == "okx" else 101.2,
                bid_size=50.0,
                ask_size=60.0,
                observed_at_ms=now_ms - 25,
                received_at_ms=now_ms - 25,
                source=f"{venue}_rest_topbook",
            )

    refresher = RestOnlyRefresher()
    runtime.ws_bbo_rest_refresher = refresher

    monkeypatch.setattr(
        "lightfee.engine.runtime.funding_entry_snapshot_identity",
        lambda _path: ("test-rest-recovery", 1, 1),
    )
    monkeypatch.setattr(
        "lightfee.engine.runtime.load_funding_entry_snapshot",
        lambda _path: snapshot,
    )
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: now_ms)
    monkeypatch.setattr(runtime, "_ensure_entry_bbo_active_for_candidates", prewarm_without_quotes)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    records = _read_journal_records(tmp_path / "events.jsonl")
    kinds = [record["kind"] for record in records]
    resolved_payloads = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.entry_quote_revalidate_resolved"
    ]

    assert len(executor.contexts) == 1
    assert runtime.state.last_scan["dispatched_candidate_count"] == 1
    assert sorted(refresher.calls) == [("bybit", "BTCUSDT"), ("okx", "BTCUSDT")]
    assert "runtime.quote_stale" not in kinds
    assert runtime.state.last_scan["quote_revalidate_target_count"] == 2
    assert runtime.state.last_scan["quote_revalidate_resolved_count"] == 2
    assert runtime.state.last_scan["quote_revalidate_failed_count"] == 0
    assert runtime.state.last_scan["quote_revalidate_sources"] == {
        "bybit_rest_topbook": 1,
        "okx_rest_topbook": 1,
    }
    assert {payload["rest_revalidate_hit"] for payload in resolved_payloads} == {True}
    assert {payload["rest_revalidate_terminal_stale"] for payload in resolved_payloads} == {False}
    assert {payload["ws_bbo_lease_hit"] for payload in resolved_payloads} == {False}
    assert {payload["sidecar_reason"] for payload in resolved_payloads} == {"quote_stale"}
    assert {payload["source"] for payload in resolved_payloads} == {
        "bybit_rest_topbook",
        "okx_rest_topbook",
    }
    assert runtime.ws_bbo_cache.get_quote("okx", "BTCUSDT").source == "okx_rest_topbook"
    assert runtime.ws_bbo_cache.get_quote("bybit", "BTCUSDT").source == "bybit_rest_topbook"


@pytest.mark.asyncio
async def test_quote_revalidation_overlay_keeps_ws_that_superseded_delayed_rest(
    tmp_path,
    monkeypatch,
):
    from lightfee.marketdata.ws_bbo import TopBookQuote

    now_ms = 100_000
    config = AppConfig(
        runtime=RuntimeConfig(mode="live", max_market_age_ms=600_000),
        strategy=_entry_flow_strategy_config(
            local_l2_enabled=False,
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=1_500,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.state.last_scan = {}
    runtime._entry_wall_clock_now_ms = lambda: now_ms
    candidate = _freshness_candidate()
    snapshot = SidecarSnapshot(
        published_at_ms=now_ms,
        market_observed_at_ms=now_ms,
        acquisition_mode="fresh_sidecar",
        quotes={
            f"{venue}:BTCUSDT": _quote_with_liquidity(
                venue,
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 10_000,
            )
            for venue in ("okx", "bybit")
        },
        candidates=[candidate],
    )

    async def activate(candidates, activation_now_ms):
        runtime._entry_bbo_subscription_budgeted_keys = {
            ("okx", "BTCUSDT"),
            ("bybit", "BTCUSDT"),
        }
        runtime._entry_bbo_subscription_budget_excluded_keys = set()

    class RacingRefresher:
        async def arefresh_quote_result(self, venue, symbol, *, now_ms):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol=symbol,
                    bid=101.0,
                    ask=101.1,
                    observed_at_ms=now_ms - 5,
                    received_at_ms=now_ms - 5,
                    exchange_event_at_ms=2_000,
                    source=f"{venue}_book_ticker",
                )
            )
            quote = TopBookQuote(
                venue=venue,
                symbol=symbol,
                bid=99.0,
                ask=99.1,
                observed_at_ms=now_ms,
                received_at_ms=now_ms,
                exchange_event_at_ms=1_000,
                source=f"{venue}_rest_top_book",
            )
            return type(
                "RefreshResult",
                (),
                {"outcome": "resolved", "quote": quote},
            )()

    runtime.ws_bbo_rest_refresher = RacingRefresher()
    monkeypatch.setattr(
        runtime,
        "_ensure_entry_bbo_active_for_candidates",
        activate,
    )

    runtime.journal.open()
    try:
        overlay, stats = await runtime._entry_quote_revalidate_for_candidates(
            [candidate],
            snapshot=snapshot,
            now_ms=now_ms,
        )
    finally:
        runtime.journal.close()

    assert set(overlay) == {("okx", "BTCUSDT"), ("bybit", "BTCUSDT")}
    assert {quote.source for quote in overlay.values()} == {
        "okx_book_ticker",
        "bybit_book_ticker",
    }
    assert {quote.bid for quote in overlay.values()} == {101.0}
    assert stats["rest_resolved_count"] == 0
    assert stats["ws_resolved_count"] == 2


@pytest.mark.asyncio
async def test_runtime_ws_bbo_quote_revalidate_uses_complete_bounded_entry_frontier(
    tmp_path,
    monkeypatch,
):
    from lightfee.marketdata.ws_bbo import TopBookQuote

    now_ms = 100_000
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=600000,
            max_order_quote_age_ms=5_000,
            live_scan_recovery_success_count=1,
            debug_journal_diagnostics_enabled=True,
        ),
        strategy=_entry_flow_strategy_config(
            local_l2_enabled=False,
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=100,
            entry_window_secs=600,
            min_scan_minutes_before_funding=0,
            min_funding_edge_bps=0,
            max_concurrent_positions=2,
            entry_local_l2_primary_count=2,
            shadow_entry_opportunity_count=1,
            entry_quote_prewarm_extra_candidate_count=24,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: OkxMetadataAdapter(),
            Venue.BYBIT: BybitMetadataAdapter(),
        },
    )
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    runtime.entry_executor = CapturingEntryExecutor()
    candidates = [
        _freshness_candidate(symbol=f"S{i:02d}USDT")
        for i in range(50)
    ]
    for index, candidate in enumerate(candidates):
        candidate.ranking_edge_bps = 100.0 - index
        candidate.funding_edge_bps = 100.0 - index
        candidate.funding_diff_bps = 100.0 - index
    snapshot = SidecarSnapshot(
        published_at_ms=now_ms,
        market_observed_at_ms=now_ms,
        acquisition_mode="fresh_sidecar",
        quotes={
            f"{venue}:{candidate.symbol}": _quote_with_liquidity(
                venue,
                candidate.symbol,
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 10_000,
            )
            for candidate in candidates
            for venue in ("okx", "bybit")
        },
        candidates=candidates,
    )
    prewarm_candidate_counts: list[int] = []
    quote_revalidate_calls: list[dict[str, int | str]] = []

    async def prewarm_without_quotes(candidates, prewarm_now_ms):
        prewarm_candidate_counts.append(len(candidates))
        runtime._entry_bbo_subscription_budgeted_keys = {
            (venue, str(candidate.symbol).upper())
            for candidate in candidates
            for venue in ("okx", "bybit")
        }
        runtime._entry_bbo_subscription_budget_excluded_keys = set()
        runtime._entry_bbo_subscription_per_venue_budget = 10

    class RecordingRestRefresher:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        def refresh_quote(self, venue: str, symbol: str, *, now_ms: int):
            self.calls.append((venue, symbol))
            return TopBookQuote(
                venue=venue,
                symbol=symbol,
                bid=100.0,
                ask=101.0,
                bid_size=50.0,
                ask_size=60.0,
                observed_at_ms=now_ms - 25,
                received_at_ms=now_ms - 25,
                source=f"{venue}_rest_topbook",
            )

    refresher = RecordingRestRefresher()
    runtime.ws_bbo_rest_refresher = refresher

    original_revalidate = runtime._entry_quote_revalidate_for_candidates

    async def record_revalidate(
        candidates,
        *,
        snapshot,
        now_ms,
        candidate_scope="",
        skipped_untracked_count=0,
        evidence_role="entry_execution",
        activation_candidates=None,
        evidence_coordinator=None,
    ):
        quote_revalidate_calls.append({
            "candidate_count": len(candidates),
            "candidate_scope": candidate_scope,
            "evidence_role": evidence_role,
            "skipped_untracked_count": skipped_untracked_count,
        })
        return await original_revalidate(
            candidates,
            snapshot=snapshot,
            now_ms=now_ms,
            candidate_scope=candidate_scope,
            skipped_untracked_count=skipped_untracked_count,
            evidence_role=evidence_role,
            activation_candidates=activation_candidates,
            evidence_coordinator=evidence_coordinator,
        )

    _install_v6_snapshot_fixture(monkeypatch, snapshot)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: now_ms)
    monkeypatch.setattr(runtime, "_ensure_entry_bbo_active_for_candidates", prewarm_without_quotes)
    monkeypatch.setattr(runtime, "_entry_quote_revalidate_for_candidates", record_revalidate)

    runtime.journal.open()
    try:
        runtime._running = True
        await runtime.tick()
        if runtime._entry_evidence_prewarm_task is not None:
            await runtime._entry_evidence_prewarm_task
    finally:
        runtime._running = False
        runtime.journal.close()

    records = _read_journal_records(tmp_path / "events.jsonl")
    probe = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.entry_quote_revalidate_probe"
    ][-1]

    # The independent preparation generation activates the complete bounded
    # frontier once; the 750ms evidence phase must not repeat activation.
    assert prewarm_candidate_counts == [32]
    assert quote_revalidate_calls == [
        {
            "candidate_count": 32,
            "candidate_scope": "top_k_entry_frontier",
            "evidence_role": "entry_execution",
            "skipped_untracked_count": 0,
        },
    ]
    assert len(refresher.calls) == 64
    assert runtime.state.last_scan["quote_revalidate_candidate_scope"] == "top_k_entry_frontier"
    assert runtime.state.last_scan["quote_revalidate_candidate_count"] == 32
    assert runtime.state.last_scan["quote_revalidate_target_count"] == 64
    assert runtime.state.last_scan["quote_revalidate_skipped_untracked_count"] == 0
    assert runtime.state.last_scan["quote_prewarm_extra_candidate_count"] == 0
    assert probe["candidate_scope"] == "top_k_entry_frontier"
    assert probe["candidate_count"] == 32
    assert probe["skipped_untracked_count"] == 0


@pytest.mark.asyncio
async def test_runtime_entry_quote_revalidate_budget_excluded_top_candidate_uses_rest(
    tmp_path,
    monkeypatch,
):
    from lightfee.marketdata.ws_bbo import TopBookQuote

    now_ms = 100_000
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=600000,
            max_order_quote_age_ms=5_000,
            live_scan_recovery_success_count=1,
        ),
        strategy=_entry_flow_strategy_config(
            local_l2_enabled=False,
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=100,
            entry_window_secs=600,
            min_scan_minutes_before_funding=0,
            min_funding_edge_bps=0,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: OkxMetadataAdapter(),
            Venue.BYBIT: BybitMetadataAdapter(),
        },
    )
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    executor = CapturingEntryExecutor()
    runtime.entry_executor = executor
    candidate = _freshness_candidate()
    snapshot = SidecarSnapshot(
        published_at_ms=now_ms,
        market_observed_at_ms=now_ms,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote_with_liquidity(
                "okx",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 10_000,
            ),
            "bybit:BTCUSDT": _quote_with_liquidity(
                "bybit",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 10_000,
            ),
        },
        candidates=[candidate],
    )

    async def prewarm_budget_excludes_top_candidate(candidates, prewarm_now_ms):
        runtime._entry_bbo_subscription_budgeted_keys = {
            ("okx", "ETHUSDT"),
            ("bybit", "ETHUSDT"),
        }
        runtime._entry_bbo_subscription_budget_excluded_keys = {
            ("okx", "BTCUSDT"),
            ("bybit", "BTCUSDT"),
        }
        runtime._entry_bbo_subscription_per_venue_budget = 1

    class RestOnlyRefresher:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        def refresh_quote(self, venue: str, symbol: str, *, now_ms: int):
            self.calls.append((venue, symbol))
            return TopBookQuote(
                venue=venue,
                symbol=symbol,
                bid=100.0 if venue == "okx" else 100.2,
                ask=101.0 if venue == "okx" else 101.2,
                bid_size=50.0,
                ask_size=60.0,
                observed_at_ms=now_ms - 25,
                received_at_ms=now_ms - 25,
                source=f"{venue}_rest_topbook",
            )

    refresher = RestOnlyRefresher()
    runtime.ws_bbo_rest_refresher = refresher

    monkeypatch.setattr(
        "lightfee.engine.runtime.funding_entry_snapshot_identity",
        lambda _path: ("test-budget-rest-recovery", 1, 1),
    )
    monkeypatch.setattr(
        "lightfee.engine.runtime.load_funding_entry_snapshot",
        lambda _path: snapshot,
    )
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: now_ms)
    monkeypatch.setattr(
        runtime,
        "_ensure_entry_bbo_active_for_candidates",
        prewarm_budget_excludes_top_candidate,
    )

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    records = _read_journal_records(tmp_path / "events.jsonl")
    kinds = [record["kind"] for record in records]
    failures = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.entry_quote_revalidate_failed"
    ]

    assert len(executor.contexts) == 1
    assert sorted(refresher.calls) == [("bybit", "BTCUSDT"), ("okx", "BTCUSDT")]
    assert runtime.state.last_scan["dispatched_candidate_count"] == 1
    assert "runtime.quote_stale" not in kinds
    assert not failures
    assert runtime.state.last_scan["quote_revalidate_target_count"] == 2
    assert runtime.state.last_scan["quote_revalidate_resolved_count"] == 2
    assert runtime.state.last_scan["quote_revalidate_failed_count"] == 0
    assert runtime.state.last_scan["budget_excluded_without_rest_count"] == 0


@pytest.mark.asyncio
async def test_runtime_entry_quote_revalidate_budget_excluded_without_rest_is_explicit(
    tmp_path,
    monkeypatch,
):
    now_ms = 100_000
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=600000,
            max_order_quote_age_ms=5_000,
            live_scan_recovery_success_count=1,
        ),
        strategy=StrategyConfig(
            funding_new_entries_enabled=True,
            local_l2_enabled=False,
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=100,
            entry_window_secs=600,
            min_scan_minutes_before_funding=0,
            min_funding_edge_bps=0,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: OkxMetadataAdapter(),
            Venue.BYBIT: BybitMetadataAdapter(),
        },
    )
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    executor = CapturingEntryExecutor()
    runtime.entry_executor = executor
    candidate = _freshness_candidate()
    snapshot = SidecarSnapshot(
        published_at_ms=now_ms,
        market_observed_at_ms=now_ms,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote_with_liquidity(
                "okx",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 10_000,
            ),
            "bybit:BTCUSDT": _quote_with_liquidity(
                "bybit",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 10_000,
            ),
        },
        candidates=[candidate],
    )

    async def prewarm_budget_excludes_top_candidate(candidates, prewarm_now_ms):
        runtime._entry_bbo_subscription_budgeted_keys = {
            ("okx", "ETHUSDT"),
            ("bybit", "ETHUSDT"),
        }
        runtime._entry_bbo_subscription_budget_excluded_keys = {
            ("okx", "BTCUSDT"),
            ("bybit", "BTCUSDT"),
        }
        runtime._entry_bbo_subscription_per_venue_budget = 1

    class NoRestCapability:
        pass

    runtime.ws_bbo_rest_refresher = NoRestCapability()

    _install_v6_snapshot_fixture(monkeypatch, snapshot)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: now_ms)
    monkeypatch.setattr(
        runtime,
        "_ensure_entry_bbo_active_for_candidates",
        prewarm_budget_excludes_top_candidate,
    )

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    records = _read_journal_records(tmp_path / "events.jsonl")
    failures = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.entry_quote_revalidate_failed"
    ]

    assert not executor.contexts
    assert runtime.state.last_scan["dispatched_candidate_count"] == 0
    assert runtime.state.last_scan["quote_revalidate_target_count"] == 2
    assert runtime.state.last_scan["quote_revalidate_resolved_count"] == 0
    assert runtime.state.last_scan["quote_revalidate_failed_count"] == 2
    assert runtime.state.last_scan["budget_excluded_without_rest_count"] == 2
    assert runtime.state.last_scan["top_quote_blocker_buckets"] == {
        "budget_excluded_without_rest": 2
    }
    assert {payload["outcome"] for payload in failures} == {
        "budget_excluded_rest_unavailable"
    }
    assert all(payload["source"] == "entry_quote_truth" for payload in failures)
    assert all(payload["age_ms"] is not None for payload in failures)
    assert all(payload["budget_ms"] == 100 for payload in failures)
    assert all(payload["ws_budget_excluded"] is True for payload in failures)


@pytest.mark.asyncio
async def test_runtime_entry_quote_revalidate_rest_throttle_is_not_invalid_quote(
    tmp_path,
    monkeypatch,
):
    from lightfee.marketdata.ws_bbo import RestTopBookQuoteResult

    now_ms = 100_000
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=600000,
            max_order_quote_age_ms=5_000,
        ),
        strategy=StrategyConfig(
            local_l2_enabled=False,
            entry_readiness_provider="ws_bbo_quote_lease",
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: OkxMetadataAdapter(),
            Venue.BYBIT: BybitMetadataAdapter(),
        },
    )
    runtime.state.last_scan = {}
    candidate = _freshness_candidate()
    snapshot = SidecarSnapshot(
        published_at_ms=now_ms,
        market_observed_at_ms=now_ms,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote_with_liquidity(
                "okx",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 10_000,
            ),
            "bybit:BTCUSDT": _quote_with_liquidity(
                "bybit",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 10_000,
            ),
        },
        candidates=[candidate],
    )

    async def prewarm_without_quotes(candidates, prewarm_now_ms):
        runtime._entry_bbo_subscription_budgeted_keys = {
            ("okx", "BTCUSDT"),
            ("bybit", "BTCUSDT"),
        }
        runtime._entry_bbo_subscription_budget_excluded_keys = set()

    class ThrottledRefresher:
        def refresh_quote_result(self, venue: str, symbol: str, *, now_ms: int):
            return RestTopBookQuoteResult(
                venue=venue,
                symbol=symbol,
                venue_symbol=symbol,
                outcome="throttled",
                endpoint="rest_topbook",
                url=f"https://example.invalid/{venue}/{symbol}",
                attempt_interval_outcome="min_interval_not_elapsed",
            )

    runtime.ws_bbo_rest_refresher = ThrottledRefresher()
    monkeypatch.setattr(
        runtime,
        "_ensure_entry_bbo_active_for_candidates",
        prewarm_without_quotes,
    )
    monkeypatch.setattr(runtime, "_entry_quote_lease_max_age_ms", lambda: 0)

    runtime.journal.open()
    try:
        overlay, stats = await runtime._entry_quote_revalidate_for_candidates(
            [candidate],
            snapshot=snapshot,
            now_ms=now_ms,
            candidate_scope="v1_primary_shadow",
        )
    finally:
        runtime.journal.close()

    records = _read_journal_records(tmp_path / "events.jsonl")
    failures = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.entry_quote_revalidate_failed"
    ]

    assert overlay == {}
    assert stats["rest_throttled_count"] == 2
    assert {payload["outcome"] for payload in failures} == {"rest_attempt_throttled"}
    assert {payload["reason_bucket"] for payload in failures} == {"rest_throttled"}
    assert all(
        payload["attempt_interval_outcome"] == "min_interval_not_elapsed"
        for payload in failures
    )
    assert "rest_invalid_quote" not in {payload["outcome"] for payload in failures}
    assert all(payload["rest_error"] == "" for payload in failures)
    assert all(payload["endpoint"] == "rest_topbook" for payload in failures)


@pytest.mark.asyncio
async def test_runtime_entry_quote_revalidate_rest_quote_stale_has_precise_bucket(
    tmp_path,
    monkeypatch,
):
    from lightfee.marketdata.ws_bbo import RestTopBookQuoteResult, TopBookQuote

    now_ms = 100_000
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=600000,
            max_order_quote_age_ms=5_000,
        ),
        strategy=StrategyConfig(
            local_l2_enabled=False,
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=100,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: OkxMetadataAdapter(),
            Venue.BYBIT: BybitMetadataAdapter(),
        },
    )
    runtime.state.last_scan = {}
    candidate = _freshness_candidate()
    snapshot = SidecarSnapshot(
        published_at_ms=now_ms,
        market_observed_at_ms=now_ms,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote_with_liquidity(
                "okx",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 10_000,
            ),
            "bybit:BTCUSDT": _quote_with_liquidity(
                "bybit",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 10_000,
            ),
        },
        candidates=[candidate],
    )

    async def prewarm_without_quotes(candidates, prewarm_now_ms):
        runtime._entry_bbo_subscription_budgeted_keys = {
            ("okx", "BTCUSDT"),
            ("bybit", "BTCUSDT"),
        }
        runtime._entry_bbo_subscription_budget_excluded_keys = set()

    class StaleRestRefresher:
        def refresh_quote_result(self, venue: str, symbol: str, *, now_ms: int):
            quote = TopBookQuote(
                venue=venue,
                symbol=symbol,
                bid=100.0,
                ask=101.0,
                observed_at_ms=now_ms - 250,
                received_at_ms=now_ms,
                source="rest_topbook",
            )
            return RestTopBookQuoteResult(
                venue=venue,
                symbol=symbol,
                venue_symbol=symbol,
                outcome="resolved",
                quote=quote,
                endpoint="rest_topbook",
                url=f"https://example.invalid/{venue}/{symbol}",
                bid=quote.bid,
                ask=quote.ask,
                observed_at_ms=quote.observed_at_ms,
            )

    runtime.ws_bbo_rest_refresher = StaleRestRefresher()
    monkeypatch.setattr(
        runtime,
        "_ensure_entry_bbo_active_for_candidates",
        prewarm_without_quotes,
    )

    runtime.journal.open()
    try:
        overlay, stats = await runtime._entry_quote_revalidate_for_candidates(
            [candidate],
            snapshot=snapshot,
            now_ms=now_ms,
            candidate_scope="v1_primary_shadow",
        )
    finally:
        runtime.journal.close()

    records = _read_journal_records(tmp_path / "events.jsonl")
    failures = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.entry_quote_revalidate_failed"
    ]

    assert overlay == {}
    assert stats["quote_lease_failure_counts"] == Counter({
        "rest_resolved_but_stale": 2,
    })
    assert {payload["reason_bucket"] for payload in failures} == {
        "rest_resolved_but_stale"
    }
    assert {payload["reason_family"] for payload in failures} == {
        "rest_invalid_quote"
    }
    assert {payload["quote_validation_reject_reason"] for payload in failures} == {
        "stale"
    }
    assert all(
        payload["rest_quote_observed_at_ms"]
        == payload["rest_quote_received_at_ms"] - 250
        for payload in failures
    )
    assert all(
        250 <= payload["rest_quote_age_ms"] < 1_000
        for payload in failures
    )
    assert all(payload["rest_revalidate_hit"] is True for payload in failures)
    assert all(payload["rest_revalidate_terminal_stale"] is True for payload in failures)
    assert all(payload["ws_bbo_lease_hit"] is False for payload in failures)
    assert all(payload["sidecar_reason"] == "quote_stale" for payload in failures)
    assert all(payload["rest_quote_bid"] == pytest.approx(100.0) for payload in failures)
    assert all(payload["rest_quote_ask"] == pytest.approx(101.0) for payload in failures)
    scheduled = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.entry_quote_rewarm_scheduled_after_rest_stale"
    ]
    assert {(payload["venue"], payload["symbol"]) for payload in scheduled} == {
        ("okx", "BTCUSDT"),
        ("bybit", "BTCUSDT"),
    }
    assert set(runtime._entry_bbo_sticky_warm_until_ms) >= {
        ("okx", "BTCUSDT"),
        ("bybit", "BTCUSDT"),
    }


def test_runtime_entry_quote_rewarm_hard_expiry_terminalizes_and_cools_down(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(mode="live"),
        strategy=StrategyConfig(
            entry_quote_lease_ttl_ms=200,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal.open()
    try:
        first = runtime.market_data_runtime._schedule_entry_quote_rewarm_after_rest_stale(
            {
                "venue": "aster",
                "symbol": "CHZUSDT",
                "pair_id": "chz-aster-binance",
                "candidate_rank": 1,
            },
            now_ms=1_000,
        )
        terminal = runtime.market_data_runtime._schedule_entry_quote_rewarm_after_rest_stale(
            {
                "venue": "aster",
                "symbol": "CHZUSDT",
                "pair_id": "chz-aster-binance",
                "candidate_rank": 1,
            },
            now_ms=31_000,
        )
        suppressed = runtime.market_data_runtime._schedule_entry_quote_rewarm_after_rest_stale(
            {
                "venue": "aster",
                "symbol": "CHZUSDT",
                "pair_id": "chz-aster-binance",
                "candidate_rank": 1,
            },
            now_ms=32_000,
        )
    finally:
        runtime.journal.close()

    assert first is not None
    assert first["sticky_warm_until_ms"] == 121_000
    assert terminal is not None
    assert terminal["action_taken"] == "skip_candidate_after_hard_rewarm"
    assert terminal["age_ms"] == 30_000
    assert suppressed is None
    records = _read_journal_records(tmp_path / "events.jsonl")
    kinds = [record["kind"] for record in records]
    assert kinds.count("runtime.entry_quote_rewarm_scheduled_after_rest_stale") == 1
    assert kinds.count("runtime.entry_quote_rewarm_terminal_stale") == 1
    assert runtime._entry_quote_rewarm_cooldown_until_ms[("aster", "CHZUSDT")] > 32_000


def test_runtime_entry_quote_rewarm_cooldown_suppresses_revalidate_target(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(mode="live"),
        strategy=StrategyConfig(
            entry_quote_lease_ttl_ms=200,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal.open()
    try:
        runtime.market_data_runtime._schedule_entry_quote_rewarm_after_rest_stale(
            {
                "venue": "aster",
                "symbol": "PRLUSDT",
                "pair_id": "prl-binance-aster",
                "candidate_rank": 1,
            },
            now_ms=1_000,
        )
        runtime.market_data_runtime._schedule_entry_quote_rewarm_after_rest_stale(
            {
                "venue": "aster",
                "symbol": "PRLUSDT",
                "pair_id": "prl-binance-aster",
                "candidate_rank": 1,
            },
            now_ms=31_000,
        )

        candidate = CandidateInput(
            long_venue="binance",
            short_venue="aster",
            symbol="PRLUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=10.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=10.0,
            entry_notional_quote=50.0,
            first_funding_timestamp_ms=400000,
        )
        snapshot = SidecarSnapshot(
            published_at_ms=32_000,
            market_observed_at_ms=32_000,
            quotes={
                "binance:PRLUSDT": _quote_with_liquidity(
                    "binance",
                    "PRLUSDT",
                    volume_24h_quote=10_000_000.0,
                    open_interest=2_000_000.0,
                    observed_at_ms=32_000,
                ),
                "aster:PRLUSDT": _quote_with_liquidity(
                    "aster",
                    "PRLUSDT",
                    volume_24h_quote=10_000_000.0,
                    open_interest=2_000_000.0,
                    observed_at_ms=31_000,
                ),
            },
            candidates=[candidate],
        )

        targets = runtime.market_data_runtime._entry_quote_revalidate_targets(
            [candidate],
            snapshot=snapshot,
            now_ms=32_000,
        )
    finally:
        runtime.journal.close()

    aster_targets = [
        target for target in targets if target["venue"] == "aster"
    ]
    assert len(aster_targets) == 1
    assert aster_targets[0]["reason"] == "entry_final_revalidation"


@pytest.mark.asyncio
async def test_runtime_expires_overdue_candidate_lease_before_dispatch(
    tmp_path,
    monkeypatch,
):
    config = AppConfig(
        runtime=RuntimeConfig(mode="live"),
        strategy=StrategyConfig(
            funding_new_entries_enabled=True,
            candidate_lease_ms=1_000,
            local_l2_enabled=False,
            entry_readiness_provider="quote_lease",
            min_scan_minutes_before_funding=0,
            max_concurrent_positions=4,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    candidate = _freshness_candidate("LEASEUSDT")
    candidate.pair_id = "leaseusdt:okx->bybit"
    candidate.candidate_revision_id = "lease-revision-1"
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: OkxMetadataAdapter(),
            Venue.BYBIT: BybitMetadataAdapter(),
        },
    )
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    runtime._live_scan_success_streak = 3
    runtime.entry_executor = CapturingEntryExecutor()
    runtime._entry_candidate_lease_started_at_ms[candidate.candidate_revision_id] = 1_000

    now_ms = 2_500
    snapshot = _candidate_lease_snapshot(candidate, now_ms)
    _install_v6_snapshot_fixture(monkeypatch, snapshot)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: now_ms)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    records = _read_journal_records(tmp_path / "events.jsonl")
    kinds = [record["kind"] for record in records]
    assert runtime.entry_executor.contexts == []
    assert runtime.state.last_scan["tradeable_count"] == 0
    assert runtime.state.last_scan["candidate_lease_expired_count"] == 1
    assert "runtime.candidate_lease_expired" in kinds
    assert "review.candidate_rejected" in kinds
    assert "execution.entry_selected" not in kinds
    expired = next(
        record["payload"]
        for record in records
        if record["kind"] == "runtime.candidate_lease_expired"
    )
    assert expired["action_taken"] == "expire_candidate_and_rescan"
    assert expired["reason"] == "candidate_lease_hard_expired"


def test_expired_opportunity_lease_remains_expired_across_ticks(tmp_path):
    config = AppConfig(
        strategy=StrategyConfig(candidate_lease_ms=1_000),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    candidate = _freshness_candidate("LEASESTABLEUSDT")
    candidate.pair_id = "leasestableusdt:okx->bybit"
    candidate.candidate_revision_id = "revision-stable"
    candidate.opportunity_lease_id = "opportunity-stable"
    runtime.state.last_scan = {}
    runtime.journal.open()
    try:
        runtime._entry_candidate_active_lease_ids = {"opportunity-stable"}
        assert runtime._filter_expired_entry_candidate_leases(
            [candidate], now_ms=1_000
        ) == [candidate]
        assert runtime._filter_expired_entry_candidate_leases(
            [candidate], now_ms=2_000
        ) == []
        assert runtime._filter_expired_entry_candidate_leases(
            [candidate], now_ms=2_100
        ) == []
    finally:
        runtime.journal.close()

    assert runtime._entry_candidate_lease_started_at_ms == {
        "opportunity-stable": 1_000
    }


def test_opportunity_lease_age_survives_snapshot_restart(tmp_path):
    config = AppConfig(
        strategy=StrategyConfig(candidate_lease_ms=60_000),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    first = LiveRuntime(config)
    candidate = _freshness_candidate("LEASEPERSISTUSDT")
    candidate.opportunity_lease_id = "lease-persist"
    first.state.last_scan = {}
    first._entry_candidate_active_lease_ids = {"lease-persist"}
    assert first._filter_expired_entry_candidate_leases(
        [candidate], now_ms=1_000
    ) == [candidate]
    first._persist_entry_opportunity_lease_ledger()

    store = SnapshotStore(tmp_path / "lease-state.json")
    store.write(first.state.to_dict())
    journal_path = tmp_path / "lease-journal.jsonl"
    journal_path.write_text("")
    restored = recover_from_snapshot(store, Journal(journal_path))

    second = LiveRuntime(config)
    second.state = restored
    second.state.last_scan = {}
    second._restore_entry_opportunity_lease_ledger()
    second._entry_candidate_active_lease_ids = {"lease-persist"}
    second.journal.open()
    try:
        assert second._filter_expired_entry_candidate_leases(
            [candidate], now_ms=61_999
        ) == []
    finally:
        second.journal.close()


def test_corrupt_opportunity_lease_row_does_not_block_snapshot_recovery(tmp_path):
    store = SnapshotStore(tmp_path / "lease-state.json")
    store.write(
        {
            "lifecycle": "running",
            "entry_opportunity_lease_ledger": {
                "valid": {"started_at_ms": 1_000, "last_seen_at_ms": 1_500},
                "bad": {"started_at_ms": "not-an-int", "last_seen_at_ms": []},
            },
        }
    )
    journal_path = tmp_path / "lease-journal.jsonl"
    journal_path.write_text("")

    restored = recover_from_snapshot(store, Journal(journal_path))

    assert restored.entry_opportunity_lease_ledger == {
        "valid": {"started_at_ms": 1_000, "last_seen_at_ms": 1_500}
    }


def test_live_submit_oi_receipt_requires_fresh_revision_bound_two_leg_proof(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_perp_liquidity_budget_ms=30_000,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    candidate = _freshness_candidate("OISUBMITUSDT")
    candidate.long_venue = "okx"
    candidate.short_venue = "bybit"
    candidate.candidate_revision_id = "revision-oi-submit"

    def row(
        venue: str,
        observed_at_ms: int,
        *,
        event_at_ms: int = 0,
        value_quote: float = 2_000_000.0,
        source: str = "test_fixture",
    ) -> dict:
        venue_symbol = "OI-SUBMIT-USDT-SWAP"
        sample_observed_at_ms = event_at_ms or observed_at_ms
        return {
            "venue": venue,
            "canonical_symbol": "OISUBMITUSDT",
            "venue_symbol": venue_symbol,
            "status": "observed",
            "observed_at_ms": observed_at_ms,
            "event_at_ms": event_at_ms,
            "received_at_ms": observed_at_ms + 1,
            "sample_id": open_interest_sample_id(
                venue=venue,
                canonical_symbol="OISUBMITUSDT",
                venue_symbol=venue_symbol,
                observed_at_ms=sample_observed_at_ms,
                source=source,
                raw_value=value_quote,
                value_quote=value_quote,
            ),
            "value_quote": value_quote,
            "raw_value": value_quote,
            "raw_unit": "quote",
            "source": source,
        }

    candidate.entry_open_interest_evidence = {
        "candidate_revision_id": candidate.candidate_revision_id,
        "long": row("okx", 90_000),
        "short": row("bybit", 90_000),
    }
    reason, _ = runtime.entry_dispatch_runtime._entry_open_interest_submit_reason(
        candidate,
        now_ms=100_000,
    )
    assert reason == ""

    candidate.entry_open_interest_evidence["long"]["sample_id"] = "tampered"
    reason, _ = runtime.entry_dispatch_runtime._entry_open_interest_submit_reason(
        candidate,
        now_ms=100_000,
    )
    assert reason == "entry_open_interest_sample_id_mismatch"
    candidate.entry_open_interest_evidence["long"] = row("okx", 90_000)

    candidate.entry_open_interest_evidence["long"] = row(
        "okx",
        99_000,
        event_at_ms=100_500,
    )
    reason, _ = runtime.entry_dispatch_runtime._entry_open_interest_submit_reason(
        candidate,
        now_ms=100_000,
    )
    assert reason == ""

    candidate.entry_open_interest_evidence["long"] = row(
        "okx",
        99_000,
        event_at_ms=60_000,
    )
    reason, evidence = runtime.entry_dispatch_runtime._entry_open_interest_submit_reason(
        candidate,
        now_ms=100_000,
    )
    assert reason == "entry_open_interest_evidence_stale"
    assert evidence["leg"] == "long"

    candidate.entry_open_interest_evidence["long"] = row("okx", 99_000)
    reason, _ = runtime.entry_dispatch_runtime._entry_open_interest_submit_reason(
        candidate,
        now_ms=100_000,
    )
    assert reason == ""

    candidate.entry_open_interest_evidence["candidate_revision_id"] = "old-revision"
    reason, _ = runtime.entry_dispatch_runtime._entry_open_interest_submit_reason(
        candidate,
        now_ms=100_000,
    )
    assert reason == "entry_open_interest_revision_mismatch"

    candidate.entry_open_interest_evidence["candidate_revision_id"] = (
        candidate.candidate_revision_id
    )
    candidate.entry_open_interest_evidence["short"] = row("bybit", 60_000)
    reason, evidence = (
        runtime.entry_dispatch_runtime._entry_open_interest_submit_reason(
            candidate,
            now_ms=100_001,
        )
    )
    assert reason == "entry_open_interest_evidence_stale"
    assert evidence["leg"] == "short"

    candidate.entry_open_interest_evidence["short"] = row(
        "bybit",
        99_000,
        value_quote=999_999.0,
    )
    reason, evidence = runtime.entry_dispatch_runtime._entry_open_interest_submit_reason(
        candidate,
        now_ms=100_001,
    )
    assert reason == "entry_open_interest_below_floor"
    assert evidence["floor_quote"] == 1_000_000.0

    candidate.entry_open_interest_evidence["short"] = row(
        "bybit",
        99_000,
        value_quote=float("nan"),
    )
    reason, _ = runtime.entry_dispatch_runtime._entry_open_interest_submit_reason(
        candidate,
        now_ms=100_001,
    )
    assert reason == "entry_open_interest_evidence_unavailable"

    candidate.evidence_candidate_revision_id = "revision-oi-submit"
    candidate.candidate_revision_id = "final-revision-oi-submit"
    candidate.opportunity_lease_id = "lease-oi-submit"
    candidate.entry_open_interest_evidence = {
        "candidate_revision_id": candidate.evidence_candidate_revision_id,
        "long": row("okx", 99_000),
        "short": row("bybit", 99_000),
    }
    candidate.entry_open_interest_evidence["long"]["sample_id"] = "tampered"
    runtime.journal.open()
    try:
        assert runtime.entry_dispatch_runtime._entry_open_interest_submit_blocked(
            candidate,
            now_ms=100_001,
        ) is True
    finally:
        runtime.journal.close()

    blocked = next(
        record["payload"]
        for record in _read_journal_records(tmp_path / "events.jsonl")
        if record["kind"] == "runtime.entry_open_interest_submit_blocked"
    )
    assert blocked["blocking_stage"] == "executor_submit"
    assert blocked["blocking_domain"] == "open_interest"
    assert blocked["blocking_status"] == "rejected"
    assert blocked["blocking_reason"] == "entry_open_interest_sample_id_mismatch"
    assert blocked["reason"] == "entry_open_interest_sample_id_mismatch"
    assert blocked["candidate_revision_id"] == "final-revision-oi-submit"
    assert blocked["evidence_candidate_revision_id"] == "revision-oi-submit"
    assert blocked["opportunity_lease_id"] == "lease-oi-submit"
    assert blocked["long_sample_id"] == "tampered"
    assert blocked["short_sample_id"] == candidate.entry_open_interest_evidence[
        "short"
    ]["sample_id"]

    candidate.entry_open_interest_evidence["long"] = row("okx", 99_000)
    candidate.entry_open_interest_evidence["short"] = row(
        "bybit",
        99_000,
        source="",
    )
    reason, _ = runtime.entry_dispatch_runtime._entry_open_interest_submit_reason(
        candidate,
        now_ms=100_001,
    )
    assert reason == "entry_open_interest_evidence_unavailable"


def test_legacy_candidate_revision_changes_with_economic_observation(tmp_path):
    config = AppConfig(
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        )
    )
    runtime = LiveRuntime(config)
    older = _freshness_candidate("REVUSDT")
    newer = _freshness_candidate("REVUSDT")
    older.candidate_revision_id = ""
    newer.candidate_revision_id = ""
    older.economics_observed_at_ms = 1_000
    newer.economics_observed_at_ms = 2_000

    older_revision = runtime._bind_entry_candidate_revision_id(older)
    newer_revision = runtime._bind_entry_candidate_revision_id(newer)

    assert older_revision
    assert newer_revision
    assert older_revision != newer_revision
    assert older.candidate_revision_id == older_revision
    assert newer.candidate_revision_id == newer_revision


def test_top_k_reprice_is_stable_immutable_and_canary_sized(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(mode="paper"),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.state.last_scan = {}
    candidate = _freshness_candidate("REPRICEUSDT")
    candidate.pair_id = "repriceusdt:okx->bybit"
    candidate.candidate_revision_id = "source-revision"
    candidate.entry_target_quantity = 0.5
    candidate.funding_canary_hard_max_entry_notional_quote = 15.0
    quotes = {
        ("okx", "REPRICEUSDT"): SimpleNamespace(
            venue="okx",
            symbol="REPRICEUSDT",
            bid=99.9,
            ask=100.0,
            observed_at_ms=10_000,
            quantity_step_base=0.01,
            min_quantity_base=0.01,
            min_notional_quote=1.0,
        ),
        ("bybit", "REPRICEUSDT"): SimpleNamespace(
            venue="bybit",
            symbol="REPRICEUSDT",
            bid=100.2,
            ask=100.3,
            observed_at_ms=10_000,
            quantity_step_base=0.01,
            min_quantity_base=0.01,
            min_notional_quote=1.0,
        ),
    }

    first = runtime._reprice_entry_candidates_for_selection(
        [candidate], market_quotes=quotes, now_ms=10_010
    )
    second = runtime._reprice_entry_candidates_for_selection(
        [candidate], market_quotes=quotes, now_ms=10_020
    )

    assert len(first) == len(second) == 1
    assert first[0] is not candidate
    assert candidate.entry_target_quantity == 0.5
    assert candidate.candidate_revision_id == "source-revision"
    assert first[0].candidate_revision_id == second[0].candidate_revision_id
    assert first[0].entry_max_leg_notional_quote <= 15.0 + 1e-9
    assert first[0].funding_canary_size_constrained is True
    assert first[0].expected_profit_quote > 0.0


def test_top_k_reprice_uses_common_grid_before_v1_priority_ranking(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(mode="paper"),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.state.last_scan = {}
    coarse = _freshness_candidate("COARSEUSDT")
    fine = _freshness_candidate("FINEUSDT")
    for candidate in (coarse, fine):
        candidate.entry_target_quantity = 0.5
        candidate.funding_canary_hard_max_entry_notional_quote = 15.0

    quotes = {}
    for symbol, step in (("COARSEUSDT", 0.1), ("FINEUSDT", 0.01)):
        quotes[("okx", symbol)] = SimpleNamespace(
            venue="okx",
            symbol=symbol,
            bid=99.9,
            ask=100.0,
            bid_size=10.0,
            ask_size=10.0,
            observed_at_ms=10_000,
            quantity_step_base=step,
            min_quantity_base=step,
            min_notional_quote=1.0,
        )
        quotes[("bybit", symbol)] = SimpleNamespace(
            venue="bybit",
            symbol=symbol,
            bid=100.2,
            ask=100.3,
            bid_size=10.0,
            ask_size=10.0,
            observed_at_ms=10_000,
            quantity_step_base=step,
            min_quantity_base=step,
            min_notional_quote=1.0,
        )

    repriced = runtime._reprice_entry_candidates_for_selection(
        [coarse, fine],
        market_quotes=quotes,
        now_ms=10_010,
    )
    by_symbol = {candidate.symbol: candidate for candidate in repriced}

    assert by_symbol["COARSEUSDT"].entry_target_quantity == pytest.approx(0.1)
    assert by_symbol["FINEUSDT"].entry_target_quantity == pytest.approx(0.14)
    assert (
        by_symbol["FINEUSDT"].expected_profit_quote
        > by_symbol["COARSEUSDT"].expected_profit_quote
    )
    for candidate in by_symbol.values():
        risk = runtime._runtime_candidate_risk_score(candidate, quotes)
        assert runtime._runtime_candidate_selection_score(
            candidate, quotes
        ) == pytest.approx(candidate.ranking_edge_bps / (1.0 + risk))


def test_top_k_reprice_rejects_cap_below_common_pair_minimum(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(mode="paper"),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.state.last_scan = {}
    candidate = _freshness_candidate("MINUSDT")
    candidate.entry_target_quantity = 0.5
    candidate.funding_canary_hard_max_entry_notional_quote = 15.0
    quotes = {
        (venue, "MINUSDT"): SimpleNamespace(
            venue=venue,
            symbol="MINUSDT",
            bid=100.0,
            ask=100.1,
            observed_at_ms=10_000,
            quantity_step_base=0.2,
            min_quantity_base=0.2,
            min_notional_quote=20.0,
        )
        for venue in ("okx", "bybit")
    }

    repriced = runtime._reprice_entry_candidates_for_selection(
        [candidate], market_quotes=quotes, now_ms=10_010
    )

    assert repriced == []
    assert runtime.state.last_scan["entry_reprice_blocker_counts"] == {
        "funding_canary_cap_below_pair_minimum": 1
    }
    assert runtime.state.last_scan["entry_reprice_blocker_samples"][0][
        "blocking_reason"
    ] == "funding_canary_cap_below_pair_minimum"


def test_top_k_reprice_does_not_mislabel_original_minimum_failure_as_canary(
    tmp_path,
):
    config = AppConfig(
        runtime=RuntimeConfig(mode="paper"),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.state.last_scan = {}
    candidate = _freshness_candidate("ORIGINALMINUSDT")
    candidate.entry_target_quantity = 0.05
    candidate.funding_canary_hard_max_entry_notional_quote = 15.0
    quotes = {
        (venue, "ORIGINALMINUSDT"): SimpleNamespace(
            venue=venue,
            symbol="ORIGINALMINUSDT",
            bid=100.0,
            ask=100.1,
            observed_at_ms=10_000,
            quantity_step_base=0.1,
            min_quantity_base=0.2,
            min_notional_quote=20.0,
        )
        for venue in ("okx", "bybit")
    }

    assert runtime._reprice_entry_candidates_for_selection(
        [candidate], market_quotes=quotes, now_ms=10_010
    ) == []
    assert runtime.state.last_scan["entry_reprice_blocker_counts"] == {
        "entry_pair_minimum_not_met": 1
    }


def test_top_k_reprice_canary_ignores_non_executable_short_ask(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(mode="paper"),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.state.last_scan = {}
    candidate = _freshness_candidate("WIDEASKUSDT")
    candidate.entry_target_quantity = 0.5
    candidate.funding_canary_hard_max_entry_notional_quote = 15.0
    quotes = {
        ("okx", "WIDEASKUSDT"): SimpleNamespace(
            venue="okx",
            symbol="WIDEASKUSDT",
            bid=99.9,
            ask=100.0,
            observed_at_ms=10_000,
            quantity_step_base=0.01,
            min_quantity_base=0.01,
            min_notional_quote=1.0,
        ),
        ("bybit", "WIDEASKUSDT"): SimpleNamespace(
            venue="bybit",
            symbol="WIDEASKUSDT",
            bid=100.2,
            ask=1_000.0,
            observed_at_ms=10_000,
            quantity_step_base=0.01,
            min_quantity_base=0.01,
            min_notional_quote=1.0,
        ),
    }

    repriced = runtime._reprice_entry_candidates_for_selection(
        [candidate], market_quotes=quotes, now_ms=10_010
    )

    assert len(repriced) == 1
    assert repriced[0].entry_target_quantity == pytest.approx(0.14)
    assert repriced[0].entry_max_leg_notional_quote <= 15.0 + 1e-9


def test_fresh_bbo_overlay_preserves_snapshot_quantity_contract(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(mode="paper"),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    base = QuoteSnapshot(
        venue="okx",
        symbol="BTCUSDT",
        bid=99.0,
        ask=101.0,
        quantity_step_base=0.001,
        min_quantity_base=0.01,
        min_notional_quote=5.0,
        open_interest=2_000_000.0,
        open_interest_evidence_status="observed",
    )
    overlay = SimpleNamespace(
        venue="okx",
        symbol="BTCUSDT",
        bid=100.0,
        ask=100.1,
        bid_size=2.0,
        ask_size=3.0,
        observed_at_ms=10_000,
        received_at_ms=10_001,
        source="rest_bbo",
    )

    merged = runtime.market_data_runtime._entry_quote_truth_market_quotes(
        {"okx:BTCUSDT": base},
        {("okx", "BTCUSDT"): overlay},
    )["okx:BTCUSDT"]

    assert merged is not base
    assert merged.bid == 100.0
    assert merged.ask == 100.1
    assert merged.quantity_step_base == 0.001
    assert merged.min_quantity_base == 0.01
    assert merged.min_notional_quote == 5.0
    assert merged.open_interest == 2_000_000.0


@pytest.mark.asyncio
async def test_runtime_entry_quote_probe_diagnostics_are_disabled_by_default(
    tmp_path,
    monkeypatch,
):
    from lightfee.marketdata.ws_bbo import TopBookQuote

    now_ms = 100_000
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=600000,
            max_order_quote_age_ms=5_000,
            live_scan_recovery_success_count=1,
        ),
        strategy=StrategyConfig(
            funding_new_entries_enabled=True,
            local_l2_enabled=False,
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=100,
            entry_window_secs=600,
            min_scan_minutes_before_funding=0,
            min_funding_edge_bps=0,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: OkxMetadataAdapter(),
            Venue.BYBIT: BybitMetadataAdapter(),
        },
    )
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    runtime.entry_executor = CapturingEntryExecutor()
    snapshot = SidecarSnapshot(
        published_at_ms=now_ms,
        market_observed_at_ms=now_ms,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote_with_liquidity(
                "okx",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 10_000,
            ),
            "bybit:BTCUSDT": _quote_with_liquidity(
                "bybit",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 10_000,
            ),
        },
        candidates=[_freshness_candidate()],
    )

    async def prewarm_without_quotes(candidates, prewarm_now_ms):
        runtime._entry_bbo_subscription_budgeted_keys = {
            ("okx", "BTCUSDT"),
            ("bybit", "BTCUSDT"),
        }
        runtime._entry_bbo_subscription_budget_excluded_keys = set()
        runtime._entry_bbo_subscription_per_venue_budget = 8

    class RestOnlyRefresher:
        def refresh_quote(self, venue: str, symbol: str, *, now_ms: int):
            return TopBookQuote(
                venue=venue,
                symbol=symbol,
                bid=100.0 if venue == "okx" else 100.2,
                ask=101.0 if venue == "okx" else 101.2,
                bid_size=50.0,
                ask_size=60.0,
                observed_at_ms=now_ms - 25,
                received_at_ms=now_ms - 25,
                source=f"{venue}_rest_topbook",
            )

    runtime.ws_bbo_rest_refresher = RestOnlyRefresher()
    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: snapshot)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: now_ms)
    monkeypatch.setattr(runtime, "_ensure_entry_bbo_active_for_candidates", prewarm_without_quotes)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    records = _read_journal_records(tmp_path / "events.jsonl")
    assert not any(
        record["kind"] == "runtime.entry_quote_revalidate_probe"
        for record in records
    )


@pytest.mark.asyncio
async def test_runtime_entry_quote_probe_diagnostics_can_be_enabled(
    tmp_path,
    monkeypatch,
):
    from lightfee.marketdata.ws_bbo import TopBookQuote

    now_ms = 100_000
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=600000,
            max_order_quote_age_ms=5_000,
            live_scan_recovery_success_count=1,
            debug_journal_diagnostics_enabled=True,
        ),
        strategy=StrategyConfig(
            funding_new_entries_enabled=True,
            local_l2_enabled=False,
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=100,
            entry_window_secs=600,
            min_scan_minutes_before_funding=0,
            min_funding_edge_bps=0,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: OkxMetadataAdapter(),
            Venue.BYBIT: BybitMetadataAdapter(),
        },
    )
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    runtime.entry_executor = CapturingEntryExecutor()
    snapshot = SidecarSnapshot(
        published_at_ms=now_ms,
        market_observed_at_ms=now_ms,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote_with_liquidity(
                "okx",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 10_000,
            ),
            "bybit:BTCUSDT": _quote_with_liquidity(
                "bybit",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 10_000,
            ),
        },
        candidates=[_freshness_candidate()],
    )

    async def prewarm_without_quotes(candidates, prewarm_now_ms):
        runtime._entry_bbo_subscription_budgeted_keys = {
            ("okx", "BTCUSDT"),
            ("bybit", "BTCUSDT"),
        }
        runtime._entry_bbo_subscription_budget_excluded_keys = set()
        runtime._entry_bbo_subscription_per_venue_budget = 8

    class RestOnlyRefresher:
        def refresh_quote(self, venue: str, symbol: str, *, now_ms: int):
            return TopBookQuote(
                venue=venue,
                symbol=symbol,
                bid=100.0 if venue == "okx" else 100.2,
                ask=101.0 if venue == "okx" else 101.2,
                bid_size=50.0,
                ask_size=60.0,
                observed_at_ms=now_ms - 25,
                received_at_ms=now_ms - 25,
                source=f"{venue}_rest_topbook",
            )

    runtime.ws_bbo_rest_refresher = RestOnlyRefresher()
    _install_v6_snapshot_fixture(monkeypatch, snapshot)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: now_ms)
    monkeypatch.setattr(runtime, "_ensure_entry_bbo_active_for_candidates", prewarm_without_quotes)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    records = _read_journal_records(tmp_path / "events.jsonl")
    probe = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.entry_quote_revalidate_probe"
    ][-1]

    assert probe["enabled"] is True
    assert probe["candidate_count"] == 1
    assert probe["target_count"] == 2
    assert probe["budgeted_target_count"] == 2
    assert probe["budget_exhausted_count"] == 0
    assert probe["cache_initial_hit_count"] == 0
    assert probe["cache_wait_hit_count"] == 0
    assert probe["rest_attempt_count"] == 2
    assert probe["rest_resolved_count"] == 2
    assert probe["failed_count"] == 0
    assert probe["resolved_sources"] == {
        "bybit_rest_topbook": 1,
        "okx_rest_topbook": 1,
    }


def test_snapshot_freshness_decisions_are_rate_limited_per_revision_and_lease(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            max_market_age_ms=600000,
            max_order_quote_age_ms=600000,
        ),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    candidate_a = _freshness_candidate()
    candidate_a.pair_id = "btcusdt:okx->bybit:a"
    candidate_a.candidate_revision_id = "revision-a"
    candidate_a.opportunity_lease_id = "lease-a"
    candidate_b = _freshness_candidate()
    candidate_b.pair_id = "btcusdt:okx->bybit:b"
    candidate_b.candidate_revision_id = "revision-b"
    candidate_b.opportunity_lease_id = "lease-b"
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=69000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": QuoteSnapshot(
                venue="okx",
                symbol="BTCUSDT",
                bid=0.0,
                ask=101.0,
                bid_size=0.0,
                ask_size=12.5,
            ),
            "bybit:BTCUSDT": _quote("bybit", "BTCUSDT", 100.2, 101.2),
        },
        candidates=[candidate_a, candidate_b],
    )

    runtime.journal.open()
    try:
        filtered = runtime._filter_candidates_by_snapshot_freshness(
            [candidate_a, candidate_b],
            snapshot=snapshot,
            now_ms=70000,
            metrics={},
            ages={},
        )
        assert filtered == []

        first_records = _read_journal_records(tmp_path / "events.jsonl")
        first_decisions = [
            record for record in first_records
            if record["kind"] == "runtime.snapshot_freshness_decision"
            and record["payload"]["reason"] == "invalid_quote"
        ]
        assert len(first_decisions) == 2
        assert {
            (
                record["payload"]["candidate_revision_id"],
                record["payload"]["opportunity_lease_id"],
            )
            for record in first_decisions
        } == {("revision-a", "lease-a"), ("revision-b", "lease-b")}
        assert all(
            record["payload"].get("suppressed_count", 0) == 0
            for record in first_decisions
        )

        runtime._filter_candidates_by_snapshot_freshness(
            [candidate_a, candidate_b],
            snapshot=snapshot,
            now_ms=70_001,
            metrics={},
            ages={},
        )

        runtime._filter_candidates_by_snapshot_freshness(
            [candidate_a, candidate_b],
            snapshot=snapshot,
            now_ms=131000,
            metrics={},
            ages={},
        )
    finally:
        runtime.journal.close()

    decisions = [
        record for record in _read_journal_records(tmp_path / "events.jsonl")
        if record["kind"] == "runtime.snapshot_freshness_decision"
        and record["payload"]["reason"] == "invalid_quote"
    ]
    assert len(decisions) == 4
    assert all(record["payload"]["compact"] is True for record in decisions[-2:])
    assert all(
        record["payload"]["suppressed_count"] == 1
        for record in decisions[-2:]
    )


def test_scan_no_entry_diagnostics_compacts_repeated_high_level_reason(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(mode="live"),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)

    first_candidates = [_freshness_candidate(f"AAA{idx}USDT") for idx in range(6)]
    second_candidates = [_freshness_candidate(f"BBB{idx}USDT") for idx in range(7)]
    first_snapshot = SidecarSnapshot(candidates=first_candidates)
    second_snapshot = SidecarSnapshot(candidates=second_candidates)

    runtime.journal.open()
    try:
        runtime._emit_scan_no_entry_diagnostics(
            reason="tradeable_candidates_waiting_for_entry_finalization_window_too_early",
            snapshot=first_snapshot,
            tradeable=first_candidates,
            selected_candidate_count=0,
            dispatched_candidate_count=0,
            remaining_slots=1,
            tradeable_selection_blocker_counts=Counter({"funding_window_too_early": 7}),
            candidate_blockers={},
            now_ms=70000,
        )
        runtime._emit_scan_no_entry_diagnostics(
            reason="tradeable_candidates_waiting_for_entry_finalization_window_too_early",
            snapshot=second_snapshot,
            tradeable=second_candidates,
            selected_candidate_count=0,
            dispatched_candidate_count=0,
            remaining_slots=1,
            tradeable_selection_blocker_counts=Counter({"funding_window_too_early": 6}),
            candidate_blockers={},
            now_ms=131000,
        )
    finally:
        runtime.journal.close()

    diagnostics = [
        record["payload"]
        for record in _read_journal_records(tmp_path / "events.jsonl")
        if record["kind"] == "scan.no_entry_diagnostics"
    ]
    assert len(diagnostics) == 2
    assert diagnostics[0].get("compact") is not True
    assert len(diagnostics[0]["candidates"]) == 6
    assert diagnostics[1]["compact"] is True
    assert diagnostics[1]["tradeable_count"] == 7
    assert "candidates" not in diagnostics[1]
    assert diagnostics[1]["suppressed_full_payload_count"] == 1


def test_scan_no_entry_pipeline_counts_use_catalog_admission_balance_stage(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(mode="live"),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    snapshot = SidecarSnapshot(
        candidates=[_freshness_candidate(f"RAW{idx}USDT") for idx in range(8)]
    )
    tradeable = [_freshness_candidate(f"PASSED{idx}USDT") for idx in range(3)]
    runtime.state.last_scan = {}
    runtime.state.last_scan.update(
        {
            "raw_candidate_count": 8,
            "strategy_tradeable_count": 5,
            "catalog_admission_balance_passed_count": 3,
            "snapshot_freshness_all_candidate_count": 0,
            "snapshot_freshness_candidate_count": 0,
        }
    )

    runtime.journal.open()
    try:
        runtime._emit_scan_no_entry_diagnostics(
            reason="no_tradeable_candidates",
            snapshot=snapshot,
            tradeable=tradeable,
            selected_candidate_count=0,
            dispatched_candidate_count=0,
            remaining_slots=1,
            tradeable_selection_blocker_counts=Counter(),
            candidate_blockers={},
            now_ms=70000,
        )
    finally:
        runtime.journal.close()

    diagnostic = next(
        record["payload"]
        for record in _read_journal_records(tmp_path / "events.jsonl")
        if record["kind"] == "scan.no_entry_diagnostics"
    )
    assert diagnostic["pipeline_counts"][
        "catalog_admission_balance_passed"
    ] == len(tradeable)


def test_runtime_snapshot_freshness_status_includes_transfer_domain(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(mode="live"),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    snapshot = SidecarSnapshot(
        transfer_lifecycle=[
            TransferLifecycle(
                from_venue="okx",
                to_venue="bybit",
                observed_at_ms=65000,
                coverage_usable=1,
            )
        ],
    )

    *_unused, statuses = runtime._snapshot_freshness_observability(
        snapshot=snapshot,
        candidates=[_freshness_candidate()],
        now_ms=70000,
    )

    key = "transfer|okx->bybit|BTCUSDT|sidecar_transfer"
    assert statuses[key]["status"] == "fresh"
    assert statuses[key]["age_ms"] == 5000


@pytest.mark.asyncio
async def test_runtime_snapshot_freshness_observability_avoids_full_candidate_scope_when_no_tradeable(
    tmp_path,
    monkeypatch,
):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            live_scan_recovery_success_count=1,
        ),
        strategy=StrategyConfig(
            min_expected_edge_bps=1000,
            entry_window_secs=600,
            min_scan_minutes_before_funding=0,
            max_concurrent_positions=2,
            shadow_entry_opportunity_count=1,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    runtime.entry_executor = CapturingEntryExecutor()
    candidates = [
        _freshness_candidate(symbol=f"SYM{idx}USDT")
        for idx in range(64)
    ]
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=69000,
        acquisition_mode="fresh_sidecar",
        candidates=candidates,
        quotes={
            f"{venue}:{candidate.symbol}": QuoteSnapshot(
                venue=venue,
                symbol=candidate.symbol,
                bid=100.0,
                ask=101.0,
            )
            for candidate in candidates
            for venue in ("okx", "bybit")
        },
        transfer_lifecycle=[
            TransferLifecycle(
                from_venue="okx",
                to_venue="bybit",
                observed_at_ms=69000,
                coverage_usable=1,
            )
        ],
    )

    observed_candidate_counts: list[int] = []

    def observe_scope(*, snapshot, candidates, now_ms):
        observed_candidate_counts.append(len(candidates))
        return {}, {}, {}, {}, {}

    _install_v6_snapshot_fixture(monkeypatch, snapshot)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 70000)
    monkeypatch.setattr(runtime, "_snapshot_freshness_observability", observe_scope)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    assert observed_candidate_counts
    assert max(observed_candidate_counts) == 0
    assert runtime.state.last_scan["candidate_count"] == 64
    assert runtime.state.last_scan["tradeable_count"] == 0
    assert runtime.state.last_scan["no_entry_reason"] == "candidate_edge_insufficient"
    diagnostic = next(
        record["payload"]
        for record in _read_journal_records(tmp_path / "events.jsonl")
        if record["kind"] == "scan.no_entry_diagnostics"
    )
    assert diagnostic["blocked_reason_counts"] == {
        "expected_edge_below_floor": 64,
        "funding_new_entries_disabled": 64,
    }
    assert diagnostic["candidate_blocked_sample_count"] == sum(
        diagnostic["blocked_reason_counts"].values()
    )
    assert len(diagnostic["candidate_blocked_samples"]) == 128
    assert all(
        {
            "blocking_stage",
            "blocking_domain",
            "blocking_status",
            "blocking_reason",
            "venue",
            "long_venue",
            "short_venue",
            "symbol",
            "sample_id",
            "pair_id",
            "candidate_revision_id",
            "opportunity_lease_id",
        }
        <= sample.keys()
        for sample in diagnostic["candidate_blocked_samples"]
    )


@pytest.mark.asyncio
async def test_runtime_snapshot_freshness_filter_uses_complete_top32_entry_frontier(
    tmp_path,
    monkeypatch,
):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            live_scan_recovery_success_count=1,
        ),
        strategy=StrategyConfig(
            funding_new_entries_enabled=True,
            local_l2_enabled=False,
            entry_readiness_provider="ws_bbo_quote_lease",
            max_concurrent_positions=6,
            entry_local_l2_primary_count=6,
            shadow_entry_opportunity_count=2,
            entry_window_secs=600,
            min_scan_minutes_before_funding=0,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    runtime.entry_executor = CapturingEntryExecutor()
    candidates = []
    for idx in range(64):
        candidate = _freshness_candidate(symbol=f"SYM{idx}USDT")
        candidate.pair_id = f"sym{idx}usdt:okx->bybit"
        candidate.ranking_edge_bps = 100.0 - idx
        _mark_final_economics_ready(candidate, 70_000)
        candidates.append(candidate)
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=69000,
        acquisition_mode="fresh_sidecar",
        quotes={
            f"okx:SYM{idx}USDT": _quote_with_liquidity(
                "okx",
                f"SYM{idx}USDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
            )
            for idx in range(64)
        }
        | {
            f"bybit:SYM{idx}USDT": _quote_with_liquidity(
                "bybit",
                f"SYM{idx}USDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
            )
            for idx in range(64)
        },
        candidates=candidates,
    )

    _install_ws_quotes_from_snapshot(runtime, snapshot, now_ms=70_000)
    authoritative_filter_counts: list[int] = []
    incremental_filter_counts: list[int] = []

    def observe_scope(candidates, **kwargs):
        if kwargs.get("record_result", True):
            authoritative_filter_counts.append(len(candidates))
        else:
            incremental_filter_counts.append(len(candidates))
        return list(candidates)

    _install_v6_snapshot_fixture(monkeypatch, snapshot)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 70000)
    monkeypatch.setattr(
        runtime,
        "_filter_candidates_by_snapshot_freshness",
        observe_scope,
    )

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    assert authoritative_filter_counts == [32, 32]
    assert incremental_filter_counts == [1] * 32
    assert runtime.state.last_scan["snapshot_freshness_filter_candidate_scope"] == (
        "top_k_entry_frontier"
    )
    assert runtime.state.last_scan["snapshot_freshness_filter_candidate_count"] == 32
    assert runtime.state.last_scan["snapshot_freshness_filter_all_candidate_count"] == 32
    assert runtime.state.last_scan["snapshot_freshness_filter_skipped_untracked_count"] == 0


def test_runtime_snapshot_fallback_health_scope_uses_v1_primary_shadow_candidates(
    tmp_path,
    monkeypatch,
):
    config = AppConfig(
        runtime=RuntimeConfig(mode="live"),
        strategy=StrategyConfig(
            local_l2_enabled=False,
            entry_readiness_provider="ws_bbo_quote_lease",
            max_concurrent_positions=6,
            entry_local_l2_primary_count=6,
            shadow_entry_opportunity_count=2,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    candidates = []
    for idx in range(64):
        candidate = _freshness_candidate(symbol=f"SYM{idx}USDT")
        candidate.pair_id = f"sym{idx}usdt:okx->bybit"
        candidate.ranking_edge_bps = 100.0 - idx
        candidates.append(candidate)
    snapshot = SidecarSnapshot(
        published_at_ms=1000,
        market_observed_at_ms=1000,
        acquisition_mode="last_good_sidecar",
        candidates=candidates,
    )
    observed_pair_ids: list[str] = []

    def observe_candidate(candidate, **_kwargs):
        observed_pair_ids.append(candidate.pair_id)
        return [
            {
                "venue": "okx",
                "symbol": str(getattr(candidate, "symbol", "") or "").upper(),
                "domain": "quote",
                "source": "sidecar_quote",
                "age_ms": 69000,
                "decision": "skip_entry",
                "reason": "quote_stale",
                "blocking": True,
            }
        ]

    monkeypatch.setattr(
        runtime,
        "_candidate_snapshot_freshness_decisions",
        observe_candidate,
    )

    payload = runtime._snapshot_health_payload(
        snapshot=snapshot,
        now_ms=70000,
        max_age_ms=10000,
        freshness="last_good_fallback",
    )

    assert observed_pair_ids == [candidate.pair_id for candidate in candidates[:8]]
    assert payload["candidate_freshness_candidate_scope"] == "v1_primary_shadow"
    assert payload["candidate_freshness_candidate_count"] == 8
    assert payload["candidate_freshness_all_candidate_count"] == 64
    assert payload["candidate_freshness_skipped_untracked_count"] == 56
    assert {
        sample["candidate_pair_id"]
        for sample in payload["candidate_freshness_scope"]
    } <= {candidate.pair_id for candidate in candidates[:8]}


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

    def fake_decide_snapshot_freshness(
        *,
        snapshot,
        max_age_ms,
        now_ms,
        last_good=None,
        last_good_max_age_ms=None,
        market_max_age_ms=None,
        usable_payload=None,
    ):
        observed["last_good_max_age_ms"] = last_good_max_age_ms
        observed["market_max_age_ms"] = market_max_age_ms
        return SnapshotFreshnessDecision(SnapshotFreshness.MISSING, None)

    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: None)
    monkeypatch.setattr(
        "lightfee.engine.runtime.decide_snapshot_freshness",
        fake_decide_snapshot_freshness,
    )

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    assert observed["last_good_max_age_ms"] == 600000
    assert observed["market_max_age_ms"] == 4000


@pytest.mark.asyncio
async def test_runtime_last_good_fallback_emits_non_blocking_revalidate_diagnostics(tmp_path, monkeypatch):
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
                funding_timestamp_ms=300_000,
                funding_interval_ms=28_800_000,
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
                    funding_timestamp_ms=300000,
                    first_funding_timestamp_ms=300000,
                    long_funding_timestamp_ms=300000,
                    short_funding_timestamp_ms=300000,
            )
        ],
    )

    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: snapshot)
    monkeypatch.setattr(
        "lightfee.engine.runtime.decide_snapshot_freshness",
        lambda **_kwargs: SnapshotFreshnessDecision(
            SnapshotFreshness.LAST_GOOD_FALLBACK,
            snapshot,
        ),
    )
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 0)

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
    fallback = next(
        record["payload"]
        for record in records
        if record["kind"] == "runtime.snapshot_fallback_last_good"
    )
    assert "fallback_duration_ms" in fallback
    assert "last_good_age_ms" in fallback
    assert "fresh_source_age_ms" in fallback
    revalidate = next(
        record["payload"]
        for record in records
        if record["kind"] == "runtime.live_scan_revalidate_required"
    )
    assert revalidate["blocking"] is False
    assert revalidate["fallback_source"] == "last_good_sidecar"
    assert revalidate["targeted_revalidate_required"] is True
    assert revalidate["targeted_revalidate_outcome"] == "required_before_entry"
    assert runtime.state.last_scan["no_entry_reason"] is None


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
        strategy=_entry_flow_strategy_config(
            local_l2_enabled=False,
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=1_500,
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
    stale_quote = _quote("binance", "BTCUSDT", 100.0, 101.0)
    stale_quote.observed_at_ms = 60_000
    snapshot = SidecarSnapshot(
        published_at_ms=65000,
        market_observed_at_ms=60000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "binance:BTCUSDT": stale_quote,
            "bybit:BTCUSDT": _quote("bybit", "BTCUSDT", 100.2, 101.2),
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
                    funding_timestamp_ms=400000,
                    first_funding_timestamp_ms=400000,
                    long_funding_timestamp_ms=400000,
                    short_funding_timestamp_ms=400000,
                economics_complete=True,
                economics_observed_at_ms=60_000,
                calculation_version="v1_exact",
                model_epoch="v1_exact",
            )
        ],
    )

    _install_v6_snapshot_fixture(monkeypatch, snapshot)
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
    assert "runtime.snapshot_freshness_decision" in kinds
    assert "runtime.quote_stale" in kinds
    assert "runtime.entry_skipped_no_quote" not in kinds
    assert runtime.state.last_scan["selected_candidate_count"] == 0
    assert runtime.state.last_scan["dispatched_candidate_count"] == 0
    no_entry = [
        record for record in records
        if record["kind"] == "scan.no_entry_diagnostics"
    ]
    assert no_entry[-1]["payload"]["reason"] == "candidate_snapshot_domain_stale"
    assert no_entry[-1]["payload"]["generic_reason"] == "no_tradeable_candidates"
    assert no_entry[-1]["payload"]["snapshot_freshness_blocked_counts"]["quote_stale"] == 1
    blocked_sample = no_entry[-1]["payload"]["snapshot_freshness_blocked_samples"][0]
    assert blocked_sample["candidate_symbol"] == "BTCUSDT"
    assert blocked_sample["candidate_pair_id"] == "btcusdt:binance->bybit"
    assert blocked_sample["domain"] == "quote"
    assert blocked_sample["venue"] == "binance"
    assert blocked_sample["source_age_ms"] == 10000
    assert blocked_sample["blocked"] is True
    assert blocked_sample["block_reason"] == "quote_stale"


@pytest.mark.asyncio
async def test_runtime_ignores_non_candidate_stale_quotes_for_entry_blockers(tmp_path, monkeypatch):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=600000,
            max_order_quote_age_ms=5000,
            live_scan_recovery_success_count=1,
        ),
        strategy=_entry_flow_strategy_config(
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
    runtime.entry_executor = CapturingEntryExecutor()
    stale_quotes = {
        f"binance:STALE{idx}USDT": QuoteSnapshot(
            venue="binance",
            symbol=f"STALE{idx}USDT",
            bid=100.0,
            ask=101.0,
            observed_at_ms=60000,
        )
        for idx in range(3000)
    }
    snapshot = SidecarSnapshot(
        published_at_ms=65000,
        market_observed_at_ms=65000,
        acquisition_mode="fresh_sidecar",
        quotes={
            **stale_quotes,
            "okx:BTCUSDT": _quote_with_liquidity(
                "okx",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=69000,
            ),
            "bybit:BTCUSDT": _quote_with_liquidity(
                "bybit",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=69000,
            ),
        },
        candidates=[_freshness_candidate()],
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
    assert "runtime.order_quote_stale_skipped" not in kinds
    assert "runtime.quote_stale" not in kinds
    no_entry = [
        record for record in records
        if record["kind"] == "scan.no_entry_diagnostics"
    ]
    if no_entry:
        assert no_entry[-1]["payload"]["reason"] != "candidate_snapshot_domain_stale"
        assert (
            no_entry[-1]["payload"]
            .get("snapshot_freshness_blocked_counts", {})
            .get("quote_stale", 0)
            == 0
        )
    freshness_decisions = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.snapshot_freshness_decision"
    ]
    assert not any(
        decision.get("reason") == "quote_stale"
        for decision in freshness_decisions
    )


@pytest.mark.asyncio
async def test_runtime_ignores_admission_blocked_candidate_stale_quotes_for_entry_blockers(
    tmp_path,
    monkeypatch,
):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=600000,
            max_order_quote_age_ms=5000,
            live_scan_recovery_success_count=1,
        ),
        strategy=_entry_flow_strategy_config(
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
    runtime.entry_executor = CapturingEntryExecutor()
    runtime.state.venue_entry_cooldowns["hyperliquid:*"] = {
        "venue": "hyperliquid",
        "symbol": "*",
        "reason": "insufficient_margin_admission_blocked",
        "source": "pending_hedge",
        "block_scope": "venue",
        "blocked_until_ms": 130000,
        "official_doc_url": (
            "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses"
        ),
        "evidence_gap": False,
    }
    blocked_candidate = CandidateInput(
        long_venue="bybit",
        short_venue="hyperliquid",
        symbol="SEIUSDT",
        funding_diff_bps=10.0,
        funding_edge_bps=10.0,
        expected_edge_bps=5.0,
        worst_case_edge_bps=2.0,
            ranking_edge_bps=10.0,
            entry_notional_quote=50.0,
            funding_timestamp_ms=400000,
            first_funding_timestamp_ms=400000,
            long_funding_timestamp_ms=400000,
            short_funding_timestamp_ms=400000,
            economics_complete=True,
            economics_observed_at_ms=65_000,
            calculation_version="v1_exact",
            model_epoch="v1_exact",
        )
    snapshot = SidecarSnapshot(
        published_at_ms=65000,
        market_observed_at_ms=65000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "bybit:SEIUSDT": QuoteSnapshot(
                venue="bybit",
                symbol="SEIUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=60000,
            ),
            "hyperliquid:SEIUSDT": QuoteSnapshot(
                venue="hyperliquid",
                symbol="SEIUSDT",
                bid=100.2,
                ask=101.2,
                observed_at_ms=60000,
            ),
        },
        candidates=[blocked_candidate],
    )

    _install_v6_snapshot_fixture(monkeypatch, snapshot)
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
    assert "runtime.entry_admission_venue_degraded" in kinds
    assert "runtime.order_quote_stale_skipped" not in kinds
    assert "runtime.quote_stale" not in kinds
    no_entry = [
        record for record in records
        if record["kind"] == "scan.no_entry_diagnostics"
    ]
    assert (
        no_entry[-1]["payload"]["reason"]
        == "tradeable_candidates_blocked_by_entry_admission"
    )


@pytest.mark.asyncio
async def test_runtime_aster_max_notional_cooldown_prunes_before_entry_prewarm(
    tmp_path,
    monkeypatch,
):
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
            funding_new_entries_enabled=True,
            local_l2_enabled=False,
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_window_secs=600,
            min_scan_minutes_before_funding=0,
            min_funding_edge_bps=0,
            entry_quote_prewarm_extra_candidate_count=24,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    runtime.entry_executor = CapturingEntryExecutor()
    runtime.state.venue_entry_cooldowns["aster:ESPORTSUSDT"] = {
        "venue": "aster",
        "symbol": "ESPORTSUSDT",
        "blocked_symbol": "ESPORTSUSDT",
        "reason": "max_notional_admission_blocked",
        "source": "pre_entry_aster_precheck",
        "block_scope": "symbol",
        "cooldown_scope": "symbol",
        "blocked_until_ms": 130000,
        "official_doc_url": "https://www.asterdex.com/",
        "evidence_gap": False,
        "requested_notional_quote": 50.0,
        "remaining_openable_notional_quote": 0.0,
    }
    candidate = CandidateInput(
        long_venue="aster",
        short_venue="bybit",
        symbol="ESPORTSUSDT",
        funding_diff_bps=10.0,
        funding_edge_bps=10.0,
        expected_edge_bps=5.0,
        worst_case_edge_bps=2.0,
            ranking_edge_bps=10.0,
            entry_notional_quote=50.0,
            funding_timestamp_ms=400000,
            first_funding_timestamp_ms=400000,
            long_funding_timestamp_ms=400000,
            short_funding_timestamp_ms=400000,
            economics_complete=True,
            economics_observed_at_ms=65_000,
            calculation_version="v1_exact",
            model_epoch="v1_exact",
        )
    snapshot = SidecarSnapshot(
        published_at_ms=65000,
        market_observed_at_ms=65000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "aster:ESPORTSUSDT": QuoteSnapshot(
                venue="aster",
                symbol="ESPORTSUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=60000,
            ),
            "bybit:ESPORTSUSDT": QuoteSnapshot(
                venue="bybit",
                symbol="ESPORTSUSDT",
                bid=100.2,
                ask=101.2,
                observed_at_ms=60000,
            ),
        },
        candidates=[candidate],
    )

    async def fail_if_prewarmed(candidates, *, snapshot, now_ms, **kwargs):
        raise AssertionError("active admission cooldown must prune before quote prewarm")

    _install_v6_snapshot_fixture(monkeypatch, snapshot)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 70000)
    monkeypatch.setattr(runtime, "_entry_quote_revalidate_for_candidates", fail_if_prewarmed)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    records = _read_journal_records(tmp_path / "events.jsonl")
    venue_degraded = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.entry_admission_venue_degraded"
    ][-1]
    assert venue_degraded["venue"] == "aster"
    assert venue_degraded["reason"] == "max_notional_admission_blocked"
    assert venue_degraded["block_scope"] == "symbol"
    assert venue_degraded["blocked_count"] == 1
    assert runtime.entry_executor.contexts == []


@pytest.mark.asyncio
async def test_runtime_invalid_quote_decision_carries_sanitized_quote_evidence(tmp_path, monkeypatch):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=600000,
            max_order_quote_age_ms=600000,
            live_scan_recovery_success_count=1,
        ),
        strategy=_entry_flow_strategy_config(
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
    runtime.entry_executor = CapturingEntryExecutor()
    candidate = _freshness_candidate()
    invalid_quote = _quote("okx", "BTCUSDT", 100.0, 101.0)
    invalid_quote.bid = 0.0
    invalid_quote.bid_size = 0.0
    invalid_quote.ask_size = 12.5
    invalid_quote.mark_price = 100.5
    invalid_quote.index_price = 100.25
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=69000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": invalid_quote,
            "bybit:BTCUSDT": _quote("bybit", "BTCUSDT", 100.2, 101.2),
        },
        candidates=[candidate],
    )
    _install_ws_quotes_from_snapshot(runtime, snapshot, now_ms=70_000)

    _install_v6_snapshot_fixture(monkeypatch, snapshot)
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
    decision = next(
        record["payload"]
        for record in records
        if record["kind"] == "runtime.snapshot_freshness_decision"
        and record["payload"]["reason"] == "invalid_quote"
    )
    assert decision["quote_bid"] == 0.0
    assert decision["quote_ask"] == 101.0
    assert decision["quote_bid_size"] == 0.0
    assert decision["quote_ask_size"] == 12.5
    assert decision["quote_mark_price"] == 100.5
    assert decision["quote_index_price"] == 100.25
    assert decision["quote_funding_timestamp_ms"] == 400000
    assert decision["invalid_quote_fields"] == ["bid", "bid_size"]

    no_entry = [
        record for record in records
        if record["kind"] == "scan.no_entry_diagnostics"
    ][-1]["payload"]
    sample = no_entry["snapshot_freshness_blocked_samples"][0]
    assert sample["quote_bid"] == 0.0
    assert sample["quote_ask"] == 101.0
    assert sample["invalid_quote_fields"] == ["bid", "bid_size"]


@pytest.mark.asyncio
async def test_runtime_treats_coarse_perp_liquidity_stale_as_advisory(tmp_path, monkeypatch):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=600000,
            max_order_quote_age_ms=600000,
            sidecar_perp_liquidity_budget_ms=3000,
            live_scan_recovery_success_count=1,
        ),
        strategy=_entry_flow_strategy_config(
            local_l2_enabled=False,
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=1_500,
            entry_window_secs=600,
            min_scan_minutes_before_funding=0,
            min_funding_edge_bps=0,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: OkxMetadataAdapter(),
            Venue.BYBIT: BybitMetadataAdapter(),
        },
    )
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    executor = CapturingEntryExecutor()
    runtime.entry_executor = executor
    candidate = _freshness_candidate()
    # A last-good sidecar is admissible only after its executable BBO is
    # refreshed.  Keep the candidate's economics timestamp current so this
    # scenario isolates the stale liquidity advisory rather than the separate
    # final-economics timestamp gate.
    _mark_final_economics_ready(candidate, 70_000)
    snapshot = SidecarSnapshot(
        published_at_ms=65000,
        market_observed_at_ms=65000,
        acquisition_mode="last_good_sidecar",
        degraded_venues=["okx"],
        quotes={
            "okx:BTCUSDT": _quote("okx", "BTCUSDT", 100.0, 101.0),
            "bybit:BTCUSDT": _quote("bybit", "BTCUSDT", 100.2, 101.2),
        },
        liquidity_lifecycle=[
            LiquidityLifecycle(
                venue="okx",
                observed_at_ms=35000,
                symbol_count=1,
                coverage_usable=1,
                degraded_reason="last_good",
            ),
            LiquidityLifecycle(
                venue="bybit",
                observed_at_ms=69000,
                symbol_count=1,
                coverage_usable=1,
            ),
        ],
        candidates=[candidate],
    )

    _install_ws_quotes_from_snapshot(runtime, snapshot, now_ms=70_000)

    _install_v6_snapshot_fixture(monkeypatch, snapshot)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 70000)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    assert len(executor.contexts) == 1
    assert runtime.state.last_scan["selected_candidate_count"] == 1
    assert runtime.state.last_scan["dispatched_candidate_count"] == 1

    records = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    degraded = next(
        record["payload"]
        for record in records
        if record["kind"] == "runtime.snapshot_degraded"
    )
    assert any(
        item["venue"] == "okx"
        and item["domain"] == "liquidity"
        and item["source_age_ms"] == 35_000
        and item["blocked"] is False
        for item in degraded["candidate_freshness_scope"]
    )


def test_runtime_paper_mode_does_not_apply_v1_live_liquidity_gate(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="paper",
            max_market_age_ms=600000,
            max_order_quote_age_ms=600000,
        ),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    candidate = _freshness_candidate()
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=69000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote_with_liquidity(
                "okx",
                "BTCUSDT",
                volume_24h_quote=6_000_000.0,
                open_interest=900_000.0,
            ),
            "bybit:BTCUSDT": _quote_with_liquidity(
                "bybit",
                "BTCUSDT",
                volume_24h_quote=3_000_000.0,
                open_interest=2_000_000.0,
            ),
        },
        candidates=[candidate],
    )
    original_records = list(runtime.state.entry_liquidity_qualification_records)

    runtime.journal.open()
    try:
        filtered = runtime._filter_candidates_by_snapshot_freshness(
            [candidate],
            snapshot=snapshot,
            now_ms=70000,
            metrics={},
            ages={},
        )
    finally:
        runtime.journal.close()

    events = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    assert filtered == [candidate]
    assert runtime.state.entry_liquidity_qualification_records == original_records
    assert not any(
        event["kind"] == "execution.entry_liquidity_blocked" for event in events
    )


def test_runtime_blocks_fresh_candidate_when_v1_open_interest_floor_fails(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            max_market_age_ms=600000,
            max_order_quote_age_ms=600000,
        ),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    candidate = _freshness_candidate()
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=69000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote_with_liquidity(
                "okx",
                "BTCUSDT",
                volume_24h_quote=6_000_000.0,
                open_interest=900_000.0,
            ),
            "bybit:BTCUSDT": _quote_with_liquidity(
                "bybit",
                "BTCUSDT",
                volume_24h_quote=3_000_000.0,
                open_interest=2_000_000.0,
            ),
        },
        liquidity_lifecycle=[
            LiquidityLifecycle(
                venue="okx",
                observed_at_ms=69000,
                symbol_count=1,
                coverage_usable=1,
            ),
            LiquidityLifecycle(
                venue="bybit",
                observed_at_ms=69000,
                symbol_count=1,
                coverage_usable=1,
            ),
        ],
        candidates=[candidate],
    )

    runtime.journal.open()
    try:
        filtered = runtime._filter_candidates_by_snapshot_freshness(
            [candidate],
            snapshot=snapshot,
            now_ms=70000,
            metrics={},
            ages={},
        )
    finally:
        runtime.journal.close()

    assert filtered == []
    records_by_venue = {
        record["venue"]: record
        for record in runtime.state.entry_liquidity_qualification_records
    }
    okx_low_sample_id = open_interest_sample_id(
        venue="okx",
        canonical_symbol="BTCUSDT",
        venue_symbol="BTCUSDT",
        observed_at_ms=69_000,
        source="test_fixture",
        raw_value=900_000.0,
        value_quote=900_000.0,
    )
    bybit_high_sample_id = open_interest_sample_id(
        venue="bybit",
        canonical_symbol="BTCUSDT",
        venue_symbol="BTCUSDT",
        observed_at_ms=69_000,
        source="test_fixture",
        raw_value=2_000_000.0,
        value_quote=2_000_000.0,
    )
    assert records_by_venue["okx"] == {
        "venue": "okx",
        "symbol": "BTCUSDT",
        "consecutive_failures": 1,
        "last_failure_at_ms": 70000,
        "suppress_until_ms": None,
        "last_class": "temporary_below_floor",
        "last_observed_open_interest_quote": 900000,
        "last_observed_open_interest_at_ms": 69000,
        "last_observed_sample_id": okx_low_sample_id,
        "counted_low_sample_ids": [okx_low_sample_id],
        "last_counted_low_sample_id": okx_low_sample_id,
        "last_structural_probe_at_ms": None,
    }
    assert records_by_venue["bybit"] == {
        "venue": "bybit",
        "symbol": "BTCUSDT",
        "consecutive_failures": 0,
        "last_failure_at_ms": None,
        "suppress_until_ms": None,
        "last_class": "eligible",
        "last_observed_open_interest_quote": 2000000,
        "last_observed_open_interest_at_ms": 69000,
        "last_observed_sample_id": bybit_high_sample_id,
        "counted_low_sample_ids": [],
        "last_counted_low_sample_id": None,
        "last_structural_probe_at_ms": None,
    }
    records = _read_journal_records(tmp_path / "events.jsonl")
    assert "execution.entry_liquidity_blocked" in [record["kind"] for record in records]
    decision = next(
        record["payload"]
        for record in records
        if record["kind"] == "runtime.snapshot_freshness_decision"
        and record["payload"]["reason"] == "perp_open_interest_below_floor"
    )
    assert decision["decision"] == "skip_entry"
    assert decision["observed_open_interest_quote"] == 900000.0
    assert decision["min_open_interest_quote"] == 1000000.0
    assert decision["eligibility_class"] == "temporary_below_floor"


def test_runtime_blocks_oi_evidence_unavailable_without_structural_suppression(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            max_market_age_ms=600000,
            max_order_quote_age_ms=600000,
        ),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    candidate = _freshness_candidate()
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=69000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": QuoteSnapshot(
                venue="okx",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=69000,
                funding_rate_observed_at_ms=69000,
                funding_rate_event_at_ms=69000,
                funding_rate_received_at_ms=69000,
                funding_rate_source="test_fixture",
                funding_rate_sample_id=(
                    "funding:binance:BTCUSDT:69000:0:400000"
                ),
                funding_timestamp_ms=400_000,
                funding_interval_ms=28_800_000,
                volume_24h_quote=6_000_000.0,
                open_interest=0.0,
                open_interest_evidence_status="unavailable",
            ),
            "bybit:BTCUSDT": _quote_with_liquidity(
                "bybit",
                "BTCUSDT",
                volume_24h_quote=3_000_000.0,
                open_interest=2_000_000.0,
            ),
        },
        liquidity_lifecycle=[
            LiquidityLifecycle(
                venue="okx",
                observed_at_ms=69000,
                symbol_count=1,
                coverage_usable=1,
            ),
            LiquidityLifecycle(
                venue="bybit",
                observed_at_ms=69000,
                symbol_count=1,
                coverage_usable=1,
            ),
        ],
        candidates=[candidate],
    )

    runtime.journal.open()
    try:
        filtered = runtime._filter_candidates_by_snapshot_freshness(
            [candidate],
            snapshot=snapshot,
            now_ms=70000,
            metrics={},
            ages={},
        )
    finally:
        runtime.journal.close()

    assert filtered == []
    bybit_high_sample_id = open_interest_sample_id(
        venue="bybit",
        canonical_symbol="BTCUSDT",
        venue_symbol="BTCUSDT",
        observed_at_ms=69_000,
        source="test_fixture",
        raw_value=2_000_000.0,
        value_quote=2_000_000.0,
    )
    assert runtime.state.entry_liquidity_qualification_records == [
        {
            "venue": "bybit",
            "symbol": "BTCUSDT",
            "consecutive_failures": 0,
            "last_failure_at_ms": None,
            "suppress_until_ms": None,
            "last_class": "eligible",
            "last_observed_open_interest_quote": 2000000,
            "last_observed_open_interest_at_ms": 69000,
            "last_observed_sample_id": bybit_high_sample_id,
            "counted_low_sample_ids": [],
            "last_counted_low_sample_id": None,
            "last_structural_probe_at_ms": None,
        },
        {
            "venue": "okx",
            "symbol": "BTCUSDT",
            "consecutive_failures": 0,
            "last_failure_at_ms": None,
            "suppress_until_ms": None,
            "last_class": None,
            "last_observed_open_interest_quote": None,
            "last_observed_open_interest_at_ms": None,
            "last_observed_sample_id": None,
            "counted_low_sample_ids": [],
            "last_counted_low_sample_id": None,
            "last_structural_probe_at_ms": None,
        },
    ]
    records = _read_journal_records(tmp_path / "events.jsonl")
    decision = next(
        record["payload"]
        for record in records
        if record["kind"] == "runtime.snapshot_freshness_decision"
        and record["payload"]["reason"] == "oi_evidence_unavailable"
    )
    assert decision["decision"] == "skip_entry"
    assert decision["targeted_revalidate_required"] is True
    assert decision["open_interest_evidence_status"] == "unavailable"
    assert not any(
        record["payload"].get("reason") == "perp_open_interest_structural"
        for record in records
        if record["kind"] == "runtime.snapshot_freshness_decision"
    )


@pytest.mark.parametrize(
    ("open_interest", "oi_observed_at_ms"),
    [
        (float("nan"), 69_000),
        ("N/A", 69_000),
        (2_000_000.0, "N/A"),
        (2_000_000.0, 70_001),
    ],
)
def test_runtime_never_admits_invalid_or_future_observed_oi(
    tmp_path,
    open_interest,
    oi_observed_at_ms,
):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            max_market_age_ms=600000,
            max_order_quote_age_ms=600000,
        ),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    candidate = _freshness_candidate()
    snapshot = SidecarSnapshot(
        published_at_ms=69_000,
        market_observed_at_ms=69_000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote_with_liquidity(
                "okx",
                "BTCUSDT",
                volume_24h_quote=6_000_000.0,
                open_interest=open_interest,
                open_interest_observed_at_ms=oi_observed_at_ms,
            ),
            "bybit:BTCUSDT": _quote_with_liquidity(
                "bybit",
                "BTCUSDT",
                volume_24h_quote=3_000_000.0,
                open_interest=2_000_000.0,
            ),
        },
        candidates=[candidate],
    )

    runtime.journal.open()
    try:
        filtered = runtime._filter_candidates_by_snapshot_freshness(
            [candidate],
            snapshot=snapshot,
            now_ms=70_000,
            metrics={},
            ages={},
        )
    finally:
        runtime.journal.close()

    assert filtered == []
    okx_record = next(
        record
        for record in runtime.state.entry_liquidity_qualification_records
        if record["venue"] == "okx"
    )
    assert okx_record["consecutive_failures"] == 0
    assert okx_record["last_class"] is None
    decisions = [
        record["payload"]
        for record in _read_journal_records(tmp_path / "events.jsonl")
        if record["kind"] == "runtime.snapshot_freshness_decision"
    ]
    assert any(
        decision.get("reason") == "oi_evidence_unavailable"
        and decision.get("open_interest_evidence_status") in {"parse_error", "stale"}
        for decision in decisions
    )


@pytest.mark.asyncio
async def test_runtime_targeted_oi_refresh_resolves_deferred_candidate_before_gate(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        "lightfee.engine.market_data_runtime.wall_clock_now_ms",
        lambda: 70_000,
    )
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            max_market_age_ms=600000,
            max_order_quote_age_ms=600000,
        ),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    candidate = _freshness_candidate()
    candidate.long_venue = "binance"
    candidate.short_venue = "aster"

    class FakeOiRefresher:
        def __init__(self):
            self.calls: list[tuple[str, str]] = []

        async def refresh_open_interest(self, venue: str, symbol: str, *, now_ms: int):
            self.calls.append((venue, symbol))
            if venue == "binance" and symbol == "BTCUSDT":
                return {
                    **_targeted_observed_oi_result(
                        venue,
                        symbol,
                        now_ms,
                        source="test_targeted_refresh",
                    ),
                    "oi_targeted_refresh_elapsed_ms": 7,
                }
            return None

    refresher = FakeOiRefresher()
    runtime.entry_open_interest_refresher = refresher
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=69000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=69000,
                funding_rate_observed_at_ms=69000,
                funding_rate_event_at_ms=69000,
                funding_rate_received_at_ms=69000,
                funding_rate_source="test_fixture",
                funding_rate_sample_id=(
                    "funding:binance:BTCUSDT:69000:0:400000"
                ),
                funding_timestamp_ms=400_000,
                funding_interval_ms=28_800_000,
                volume_24h_quote=6_000_000.0,
                open_interest=0.0,
                open_interest_evidence_status="deferred_by_cap",
                open_interest_evidence_reason="refresh_cap_exceeded",
            ),
            "aster:BTCUSDT": _quote_with_liquidity(
                "aster",
                "BTCUSDT",
                volume_24h_quote=3_000_000.0,
                open_interest=2_000_000.0,
            ),
        },
        liquidity_lifecycle=[
            LiquidityLifecycle(
                venue="binance",
                observed_at_ms=69000,
                symbol_count=1,
                coverage_usable=1,
            ),
            LiquidityLifecycle(
                venue="aster",
                observed_at_ms=69000,
                symbol_count=1,
                coverage_usable=1,
            ),
        ],
        candidates=[candidate],
    )

    runtime.journal.open()
    try:
        stats = await runtime._refresh_entry_candidate_open_interest_evidence(
            [candidate],
            snapshot=snapshot,
            now_ms=70000,
        )
        filtered = runtime._filter_candidates_by_snapshot_freshness(
            [candidate],
            snapshot=snapshot,
            now_ms=70000,
            metrics={},
            ages={},
        )
    finally:
        runtime.journal.close()

    assert refresher.calls == [("binance", "BTCUSDT")]
    assert stats["attempt_count"] == 1
    assert stats["resolved_count"] == 1
    assert filtered == [candidate]
    refreshed_quote = snapshot.quotes["binance:BTCUSDT"]
    assert refreshed_quote.open_interest == 2_500_000.0
    assert refreshed_quote.open_interest_evidence_status == "observed"
    records = _read_journal_records(tmp_path / "events.jsonl")
    assert "runtime.entry_oi_targeted_refresh_resolved" in [
        record["kind"] for record in records
    ]
    assert not any(
        record["payload"].get("reason") == "oi_evidence_unavailable"
        for record in records
        if record["kind"] == "runtime.snapshot_freshness_decision"
    )


@pytest.mark.asyncio
async def test_runtime_targeted_oi_refresh_covers_public_market_data_venues(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        "lightfee.engine.market_data_runtime.wall_clock_now_ms",
        lambda: 70_000,
    )
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            max_market_age_ms=600000,
            max_order_quote_age_ms=600000,
        ),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    candidate = _freshness_candidate()
    candidate.long_venue = "okx"
    candidate.short_venue = "bybit"

    class FakeOiRefresher:
        def __init__(self):
            self.calls: list[tuple[str, str]] = []

        async def refresh_open_interest(self, venue: str, symbol: str, *, now_ms: int):
            self.calls.append((venue, symbol))
            return _targeted_observed_oi_result(
                venue,
                symbol,
                now_ms,
                source="test_targeted_refresh",
            )

    refresher = FakeOiRefresher()
    runtime.entry_open_interest_refresher = refresher
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=69000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": QuoteSnapshot(
                venue="okx",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=69000,
                funding_rate_observed_at_ms=69000,
                funding_rate_event_at_ms=69000,
                funding_rate_received_at_ms=69000,
                funding_rate_source="test_fixture",
                funding_rate_sample_id="funding:okx:BTCUSDT:69000:0:400000",
                funding_timestamp_ms=400_000,
                funding_interval_ms=28_800_000,
                volume_24h_quote=6_000_000.0,
                open_interest=0.0,
                open_interest_evidence_status="unavailable",
                open_interest_evidence_reason="not_refreshed",
            ),
            "bybit:BTCUSDT": QuoteSnapshot(
                venue="bybit",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=69000,
                funding_rate_observed_at_ms=69000,
                funding_rate_event_at_ms=69000,
                funding_rate_received_at_ms=69000,
                funding_rate_source="test_fixture",
                funding_rate_sample_id=(
                    "funding:bybit:BTCUSDT:69000:0:400000"
                ),
                funding_timestamp_ms=400_000,
                funding_interval_ms=28_800_000,
                volume_24h_quote=6_000_000.0,
                open_interest=0.0,
                open_interest_evidence_status="unavailable",
                open_interest_evidence_reason="not_refreshed",
            ),
        },
        liquidity_lifecycle=[
            LiquidityLifecycle(
                venue="okx",
                observed_at_ms=69000,
                symbol_count=1,
                coverage_usable=1,
            ),
            LiquidityLifecycle(
                venue="bybit",
                observed_at_ms=69000,
                symbol_count=1,
                coverage_usable=1,
            ),
        ],
        candidates=[candidate],
    )

    runtime.journal.open()
    try:
        stats = await runtime._refresh_entry_candidate_open_interest_evidence(
            [candidate],
            snapshot=snapshot,
            now_ms=70000,
        )
        filtered = runtime._filter_candidates_by_snapshot_freshness(
            [candidate],
            snapshot=snapshot,
            now_ms=70000,
            metrics={},
            ages={},
        )
    finally:
        runtime.journal.close()

    assert refresher.calls == [("okx", "BTCUSDT"), ("bybit", "BTCUSDT")]
    assert stats["attempt_count"] == 2
    assert stats["resolved_count"] == 2
    assert filtered == [candidate]
    assert snapshot.quotes["okx:BTCUSDT"].open_interest_evidence_status == "observed"
    assert snapshot.quotes["bybit:BTCUSDT"].open_interest_evidence_status == "observed"
    records = _read_journal_records(tmp_path / "events.jsonl")
    assert not any(
        record["payload"].get("reason") == "oi_evidence_unavailable"
        for record in records
        if record["kind"] == "runtime.snapshot_freshness_decision"
    )


@pytest.mark.asyncio
async def test_tick_reselects_rank_five_when_completed_evidence_finalists_reject(
    tmp_path,
    monkeypatch,
):
    now_ms = 70_000
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_max_age_ms=600_000,
            max_market_age_ms=600_000,
            max_order_quote_age_ms=600_000,
            live_scan_recovery_success_count=1,
        ),
        strategy=_entry_flow_strategy_config(
            local_l2_enabled=False,
            max_concurrent_positions=1,
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
    candidates = [_freshness_candidate(f"RANK{index}USDT") for index in range(5)]
    snapshot = SidecarSnapshot(
        published_at_ms=69_000,
        market_observed_at_ms=69_000,
        acquisition_mode="fresh_sidecar",
        quotes={
            f"{venue}:{candidate.symbol}": _quote(
                venue,
                candidate.symbol,
                100.0,
                101.0,
            )
            for candidate in candidates
            for venue in (candidate.long_venue, candidate.short_venue)
        },
        candidates=candidates,
    )

    _install_v6_snapshot_fixture(monkeypatch, snapshot)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: now_ms)
    monkeypatch.setattr(
        "lightfee.engine.runtime.discover_tradeable_candidates",
        lambda rows, *_args, **_kwargs: list(rows),
    )

    async def complete_preparation(rows, **_kwargs):
        return {
            "allowed_pair_ids": {
                runtime._candidate_pair_id(candidate) for candidate in rows
            }
        }

    async def complete_oi(rows, *, evidence_coordinator=None, **_kwargs):
        assert evidence_coordinator is not None
        for index, _candidate in enumerate(rows):
            evidence_coordinator["open_interest"][index] = "ready"
            evidence_coordinator["economics"][index] = "ready"
        evidence_coordinator["selection_ready_event"].set()
        return {}

    async def complete_quotes(rows, *, evidence_coordinator=None, **_kwargs):
        assert evidence_coordinator is not None
        for index, _candidate in enumerate(rows):
            evidence_coordinator["quote"][index] = "ready"
            evidence_coordinator["economics"][index] = "ready"
        evidence_coordinator["selection_ready_event"].set()
        return {}, runtime._entry_quote_truth_empty_stats()

    monkeypatch.setattr(runtime, "_entry_preparation_for_tick", complete_preparation)
    monkeypatch.setattr(
        runtime,
        "_schedule_entry_data_plane_preparation",
        lambda _rows: None,
    )
    monkeypatch.setattr(
        runtime,
        "_refresh_entry_candidate_open_interest_evidence",
        complete_oi,
    )
    monkeypatch.setattr(
        runtime,
        "_entry_quote_revalidate_for_candidates",
        complete_quotes,
    )
    monkeypatch.setattr(
        runtime,
        "_filter_candidates_by_snapshot_freshness",
        lambda rows, **_kwargs: list(rows),
    )
    monkeypatch.setattr(
        runtime,
        "_reprice_entry_candidates_for_selection",
        lambda rows, **_kwargs: list(rows),
    )
    monkeypatch.setattr(
        runtime,
        "_filter_expired_entry_candidate_leases",
        lambda rows, **_kwargs: list(rows),
    )
    monkeypatch.setattr(
        runtime,
        "_entry_candidate_lease_is_live",
        lambda _candidate, **_kwargs: True,
    )

    selection_inputs: list[list[str]] = []

    def select_four(rows, **_kwargs):
        selection_inputs.append([candidate.symbol for candidate in rows])
        return list(rows[:4])

    dispatch_attempts: list[str] = []

    async def reject_first_four(candidate, *_args, **_kwargs):
        dispatch_attempts.append(candidate.symbol)
        return candidate.symbol == "RANK4USDT"

    monkeypatch.setattr(runtime, "_select_entry_candidates", select_four)
    monkeypatch.setattr(runtime, "_dispatch_entry", reject_first_four)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    assert dispatch_attempts == [
        "RANK0USDT",
        "RANK1USDT",
        "RANK2USDT",
        "RANK3USDT",
        "RANK4USDT",
    ]
    assert selection_inputs == [
        [candidate.symbol for candidate in candidates],
        ["RANK4USDT"],
    ]
    assert runtime.state.last_scan["dispatched_candidate_count"] == 1


@pytest.mark.asyncio
async def test_entry_oi_preserves_lower_candidate_for_final_reprice_fallback(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        "lightfee.engine.market_data_runtime.wall_clock_now_ms",
        lambda: 70_000,
    )
    config = AppConfig(
        runtime=RuntimeConfig(mode="live"),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    highest = _freshness_candidate("BTCUSDT")
    highest.long_venue = "binance"
    highest.short_venue = "aster"
    lower = _freshness_candidate("ETHUSDT")
    lower.long_venue = "okx"
    lower.short_venue = "bybit"

    class MixedLatencyRefresher:
        async def refresh_open_interest(self, venue: str, symbol: str, *, now_ms: int):
            if (venue, symbol) == ("binance", "BTCUSDT"):
                await asyncio.sleep(0.02)
                return _targeted_observed_oi_result(
                    venue,
                    symbol,
                    now_ms,
                    source="highest_candidate_fast",
                )
            await asyncio.sleep(0.5)
            return _targeted_observed_oi_result(
                venue,
                symbol,
                now_ms,
                source="lower_candidate_slow",
            )

    runtime.entry_open_interest_refresher = MixedLatencyRefresher()
    snapshot = SidecarSnapshot(
        published_at_ms=69_000,
        market_observed_at_ms=69_000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=69_000,
                open_interest=None,
                open_interest_evidence_status="timeout",
            ),
            "aster:BTCUSDT": _quote_with_liquidity(
                "aster",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
            ),
            "okx:ETHUSDT": QuoteSnapshot(
                venue="okx",
                symbol="ETHUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=69_000,
                open_interest=1_750_000.0,
                open_interest_evidence_status="timeout",
            ),
            "bybit:ETHUSDT": QuoteSnapshot(
                venue="bybit",
                symbol="ETHUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=69_000,
                open_interest=1_800_000.0,
                open_interest_evidence_status="timeout",
            ),
        },
        candidates=[highest, lower],
    )

    runtime.journal.open()
    started = time.monotonic()
    try:
        stats = await runtime._refresh_entry_candidate_open_interest_evidence(
            [highest, lower],
            snapshot=snapshot,
            now_ms=70_000,
        )
    finally:
        runtime.journal.close()
    elapsed = time.monotonic() - started

    assert elapsed < 0.7
    assert snapshot.quotes[
        "binance:BTCUSDT"
    ].open_interest_evidence_status == "observed"
    assert stats["resolved_count"] == 3
    assert stats["failed_count"] == 0
    assert stats["timeout_count"] == 0
    assert stats["entry_evidence_deadline_exceeded_count"] == 0
    assert stats["superseded_by_ready_candidate_count"] == 0
    assert snapshot.quotes["okx:ETHUSDT"].open_interest == 2_500_000.0
    assert snapshot.quotes["bybit:ETHUSDT"].open_interest == 2_500_000.0


@pytest.mark.asyncio
async def test_runtime_targeted_oi_refresh_batches_same_venue_targets(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            max_market_age_ms=600000,
            max_order_quote_age_ms=600000,
        ),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    btc_candidate = _freshness_candidate("BTCUSDT")
    eth_candidate = _freshness_candidate("ETHUSDT")
    for candidate in (btc_candidate, eth_candidate):
        candidate.long_venue = "binance"
        candidate.short_venue = "aster"

    class BatchOnlyOiRefresher:
        def __init__(self):
            self.calls: list[tuple[str, tuple[str, ...]]] = []

        async def refresh_open_interest_batch(
            self,
            venue: str,
            symbols: list[str],
            *,
            now_ms: int,
        ):
            self.calls.append((venue, tuple(symbols)))
            return {
                symbol: {
                    **_targeted_observed_oi_result(
                        venue,
                        symbol,
                        now_ms,
                        source="test_targeted_batch_refresh",
                    ),
                    "open_interest_evidence_reason": "targeted_batch_refresh",
                    "oi_targeted_refresh_elapsed_ms": 9,
                }
                for symbol in symbols
            }

        async def refresh_open_interest(self, venue: str, symbol: str, *, now_ms: int):
            raise AssertionError("same-venue OI refresh should use batch path")

    refresher = BatchOnlyOiRefresher()
    runtime.entry_open_interest_refresher = refresher
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=69000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=69000,
                volume_24h_quote=6_000_000.0,
                open_interest=0.0,
                open_interest_evidence_status="timeout",
                open_interest_evidence_reason="timeout_waiting_for_oi",
            ),
            "binance:ETHUSDT": QuoteSnapshot(
                venue="binance",
                symbol="ETHUSDT",
                bid=200.0,
                ask=201.0,
                observed_at_ms=69000,
                volume_24h_quote=7_000_000.0,
                open_interest=0.0,
                open_interest_evidence_status="timeout",
                open_interest_evidence_reason="timeout_waiting_for_oi",
            ),
            "aster:BTCUSDT": _quote_with_liquidity(
                "aster",
                "BTCUSDT",
                volume_24h_quote=3_000_000.0,
                open_interest=2_000_000.0,
            ),
            "aster:ETHUSDT": _quote_with_liquidity(
                "aster",
                "ETHUSDT",
                volume_24h_quote=3_500_000.0,
                open_interest=2_100_000.0,
            ),
        },
        liquidity_lifecycle=[
            LiquidityLifecycle(
                venue="binance",
                observed_at_ms=69000,
                symbol_count=2,
                coverage_usable=2,
            ),
            LiquidityLifecycle(
                venue="aster",
                observed_at_ms=69000,
                symbol_count=2,
                coverage_usable=2,
            ),
        ],
        candidates=[btc_candidate, eth_candidate],
    )

    runtime.journal.open()
    try:
        stats = await runtime._refresh_entry_candidate_open_interest_evidence(
            [btc_candidate, eth_candidate],
            snapshot=snapshot,
            now_ms=70000,
        )
    finally:
        runtime.journal.close()

    assert refresher.calls == [("binance", ("BTCUSDT", "ETHUSDT"))]
    assert stats["attempt_count"] == 2
    assert stats["resolved_count"] == 2
    assert snapshot.quotes["binance:BTCUSDT"].open_interest_evidence_status == "observed"
    assert snapshot.quotes["binance:ETHUSDT"].open_interest_evidence_status == "observed"
    records = _read_journal_records(tmp_path / "events.jsonl")
    assert [
        record["payload"]["symbol"]
        for record in records
        if record["kind"] == "runtime.entry_oi_targeted_refresh_resolved"
    ] == ["BTCUSDT", "ETHUSDT"]


@pytest.mark.asyncio
async def test_runtime_targeted_oi_events_trace_bounded_shared_candidate_identities(
    tmp_path,
):
    config = AppConfig(
        runtime=RuntimeConfig(mode="live"),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    candidates = []
    for index in range(26):
        candidate = _freshness_candidate("BTCUSDT")
        candidate.long_venue = "binance"
        candidate.short_venue = "bybit" if index == 0 else "aster"
        candidate.candidate_revision_id = f"revision-{index:02d}"
        candidate.opportunity_lease_id = f"lease-{index:02d}"
        candidates.append(candidate)

    class ObservedRefresher:
        async def refresh_open_interest(
            self,
            venue: str,
            symbol: str,
            *,
            now_ms: int,
            **_kwargs,
        ):
            return _targeted_observed_oi_result(
                venue,
                symbol,
                now_ms,
                source="identity_trace_test",
            )

    runtime.entry_open_interest_refresher = ObservedRefresher()
    snapshot = SidecarSnapshot(
        published_at_ms=69_000,
        market_observed_at_ms=69_000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=69_000,
                volume_24h_quote=10_000_000.0,
                open_interest=None,
                open_interest_evidence_status="timeout",
            ),
            "bybit:BTCUSDT": QuoteSnapshot(
                venue="bybit",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=69_000,
                volume_24h_quote=10_000_000.0,
                open_interest=None,
                open_interest_evidence_status="timeout",
            ),
            "aster:BTCUSDT": _quote_with_liquidity(
                "aster",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
            ),
        },
        candidates=candidates,
    )

    runtime.journal.open()
    try:
        await runtime._refresh_entry_candidate_open_interest_evidence(
            candidates,
            snapshot=snapshot,
            now_ms=70_000,
        )
    finally:
        runtime.journal.close()

    resolved = {
        record["payload"]["venue"]: record["payload"]
        for record in _read_journal_records(tmp_path / "events.jsonl")
        if record["kind"] == "runtime.entry_oi_targeted_refresh_resolved"
    }
    shared = resolved["binance"]
    assert shared["candidate_revision_ids"] == [
        f"revision-{index:02d}" for index in range(24)
    ]
    assert shared["opportunity_lease_ids"] == [
        f"lease-{index:02d}" for index in range(24)
    ]
    assert shared["candidate_revision_ids_suppressed_count"] == 2
    assert shared["opportunity_lease_ids_suppressed_count"] == 2
    assert "candidate_revision_id" not in shared
    assert "opportunity_lease_id" not in shared

    unique = resolved["bybit"]
    assert unique["candidate_revision_ids"] == ["revision-00"]
    assert unique["opportunity_lease_ids"] == ["lease-00"]
    assert unique["candidate_revision_id"] == "revision-00"
    assert unique["opportunity_lease_id"] == "lease-00"
    assert unique["candidate_revision_ids_suppressed_count"] == 0
    assert unique["opportunity_lease_ids_suppressed_count"] == 0


@pytest.mark.asyncio
async def test_runtime_targeted_oi_fallback_is_concurrent_and_hard_deadlined(
    tmp_path,
):
    config = AppConfig(
        runtime=RuntimeConfig(mode="live"),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    candidate = _freshness_candidate()
    candidate.long_venue = "binance"
    candidate.short_venue = "aster"

    class SingleOnlyOiRefresher:
        async def refresh_open_interest(
            self,
            venue: str,
            symbol: str,
            *,
            now_ms: int,
        ):
            if venue == "binance":
                await asyncio.sleep(0.05)
                return _targeted_observed_oi_result(
                    venue,
                    symbol,
                    now_ms,
                    source="single_target_fixture",
                )
            await asyncio.sleep(2.0)
            raise AssertionError("the hard deadline must cancel this target")

    runtime.entry_open_interest_refresher = SingleOnlyOiRefresher()
    snapshot = SidecarSnapshot(
        published_at_ms=70_000,
        market_observed_at_ms=70_000,
        quotes={
            f"{venue}:BTCUSDT": QuoteSnapshot(
                venue=venue,
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=70_000,
                volume_24h_quote=10_000_000.0,
                open_interest=None,
                open_interest_evidence_status="timeout",
                open_interest_evidence_reason="prior_timeout",
            )
            for venue in ("binance", "aster")
        },
        candidates=[candidate],
    )

    runtime.journal.open()
    loop = asyncio.get_running_loop()
    started = loop.time()
    try:
        stats = await runtime._refresh_entry_candidate_open_interest_evidence(
            [candidate],
            snapshot=snapshot,
            now_ms=70_000,
        )
    finally:
        runtime.journal.close()
    elapsed = loop.time() - started

    assert elapsed < 1.0
    assert stats["resolved_count"] == 1
    assert stats["timeout_count"] == 1
    assert stats["entry_evidence_deadline_exceeded_count"] == 1
    assert snapshot.quotes["binance:BTCUSDT"].open_interest == 2_500_000.0
    assert snapshot.quotes["aster:BTCUSDT"].open_interest is None
    assert (
        snapshot.quotes["aster:BTCUSDT"].open_interest_evidence_reason
        == "entry_evidence_deadline_exceeded"
    )


def test_entry_open_interest_refresher_uses_targeted_public_budget():
    refresher = EntryOpenInterestRefresher(targeted_budget_s=1.25)
    client = refresher._client_for_venue("binance")

    assert client.binance_style_open_interest_enrichment_budget_s == pytest.approx(
        1.25
    )

    import asyncio

    asyncio.run(refresher.close())


@pytest.mark.asyncio
async def test_runtime_targeted_oi_refresh_failure_keeps_fail_closed(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            max_market_age_ms=600000,
            max_order_quote_age_ms=600000,
        ),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    candidate = _freshness_candidate()
    candidate.long_venue = "binance"
    candidate.short_venue = "aster"

    class TimeoutOiRefresher:
        async def refresh_open_interest(self, venue: str, symbol: str, *, now_ms: int):
            return {
                "open_interest_quote": 0.0,
                "open_interest_evidence_status": "timeout",
                "open_interest_evidence_reason": "timeout_waiting_for_oi",
            }

    runtime.entry_open_interest_refresher = TimeoutOiRefresher()
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=69000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=69000,
                volume_24h_quote=6_000_000.0,
                open_interest=0.0,
                open_interest_evidence_status="timeout",
                open_interest_evidence_reason="timeout_waiting_for_oi",
            ),
            "aster:BTCUSDT": _quote_with_liquidity(
                "aster",
                "BTCUSDT",
                volume_24h_quote=3_000_000.0,
                open_interest=2_000_000.0,
            ),
        },
        liquidity_lifecycle=[
            LiquidityLifecycle(
                venue="binance",
                observed_at_ms=69000,
                symbol_count=1,
                coverage_usable=1,
            ),
            LiquidityLifecycle(
                venue="aster",
                observed_at_ms=69000,
                symbol_count=1,
                coverage_usable=1,
            ),
        ],
        candidates=[candidate],
    )

    runtime.journal.open()
    try:
        stats = await runtime._refresh_entry_candidate_open_interest_evidence(
            [candidate],
            snapshot=snapshot,
            now_ms=70000,
        )
        filtered = runtime._filter_candidates_by_snapshot_freshness(
            [candidate],
            snapshot=snapshot,
            now_ms=70000,
            metrics={},
            ages={},
        )
    finally:
        runtime.journal.close()

    assert stats["failed_count"] == 1
    assert filtered == []
    records = _read_journal_records(tmp_path / "events.jsonl")
    assert "runtime.entry_oi_targeted_refresh_failed" in [
        record["kind"] for record in records
    ]
    decision = next(
        record["payload"]
        for record in records
        if record["kind"] == "runtime.snapshot_freshness_decision"
        and record["payload"]["reason"] == "oi_refresh_failed"
    )
    assert decision["open_interest_evidence_status"] == "timeout"


@pytest.mark.asyncio
async def test_structural_oi_timeout_throttles_force_refresh_attempts(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(mode="live", max_market_age_ms=600000),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.state.entry_liquidity_qualification_records = [
        {
            "venue": "binance",
            "symbol": "BTCUSDT",
            "last_class": "structural_ineligibility",
            "last_structural_probe_at_ms": None,
        }
    ]
    candidate = _freshness_candidate()
    candidate.long_venue = "binance"
    candidate.short_venue = "aster"
    force_refresh_calls: list[bool] = []

    class TimeoutOiRefresher:
        async def refresh_open_interest(
            self, venue, symbol, *, now_ms, force_refresh=False, max_age_ms=30_000
        ):
            force_refresh_calls.append(bool(force_refresh))
            return {
                "open_interest_quote": None,
                "open_interest_evidence_status": "timeout",
                "open_interest_evidence_reason": "timeout_waiting_for_oi",
            }

    runtime.entry_open_interest_refresher = TimeoutOiRefresher()
    snapshot = SidecarSnapshot(
        published_at_ms=69_000,
        market_observed_at_ms=69_000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=69_000,
                open_interest=None,
                open_interest_evidence_status="timeout",
            ),
            "aster:BTCUSDT": _quote_with_liquidity(
                "aster",
                "BTCUSDT",
                volume_24h_quote=3_000_000.0,
                open_interest=2_000_000.0,
            ),
        },
        liquidity_lifecycle=[
            LiquidityLifecycle(
                venue="binance", observed_at_ms=69_000, symbol_count=1, coverage_usable=1
            ),
            LiquidityLifecycle(
                venue="aster", observed_at_ms=69_000, symbol_count=1, coverage_usable=1
            ),
        ],
        candidates=[candidate],
    )

    runtime.journal.open()
    try:
        await runtime._refresh_entry_candidate_open_interest_evidence(
            [candidate], snapshot=snapshot, now_ms=70_000
        )
        await runtime._refresh_entry_candidate_open_interest_evidence(
            [candidate], snapshot=snapshot, now_ms=70_500
        )
    finally:
        runtime.journal.close()

    assert force_refresh_calls[:2] == [True, False]
    assert runtime.state.entry_liquidity_qualification_records[0][
        "last_structural_probe_at_ms"
    ] > 0


def test_runtime_fresh_high_oi_immediately_clears_structural_suppression(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            max_market_age_ms=600000,
            max_order_quote_age_ms=600000,
        ),
        strategy=StrategyConfig(local_l2_enabled=False),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.state.entry_liquidity_qualification_records = [
        {
            "venue": "okx",
            "symbol": "BTCUSDT",
            "consecutive_failures": 3,
            "last_failure_at_ms": 10000,
            "suppress_until_ms": 1_810_000,
            "last_class": "structural_ineligibility",
            "last_observed_open_interest_quote": 900000,
            "last_observed_open_interest_at_ms": 10000,
            "last_observed_sample_id": "sample-3",
            "counted_low_sample_ids": ["sample-1", "sample-2", "sample-3"],
            "last_structural_probe_at_ms": 69500,
        }
    ]
    candidate = _freshness_candidate()
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=69000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote_with_liquidity(
                "okx",
                "BTCUSDT",
                volume_24h_quote=6_000_000.0,
                open_interest=2_000_000.0,
            ),
            "bybit:BTCUSDT": _quote_with_liquidity(
                "bybit",
                "BTCUSDT",
                volume_24h_quote=3_000_000.0,
                open_interest=2_000_000.0,
            ),
        },
        liquidity_lifecycle=[
            LiquidityLifecycle(
                venue="okx",
                observed_at_ms=69000,
                symbol_count=1,
                coverage_usable=1,
            ),
            LiquidityLifecycle(
                venue="bybit",
                observed_at_ms=69000,
                symbol_count=1,
                coverage_usable=1,
            ),
        ],
        candidates=[candidate],
    )

    runtime.journal.open()
    try:
        filtered = runtime._filter_candidates_by_snapshot_freshness(
            [candidate],
            snapshot=snapshot,
            now_ms=70000,
            metrics={},
            ages={},
        )
        assert filtered == [candidate]

        records = runtime.state.entry_liquidity_qualification_records
        okx_record = next(record for record in records if record["venue"] == "okx")
        assert okx_record["last_class"] == "eligible"
        assert okx_record["consecutive_failures"] == 0
        assert okx_record["suppress_until_ms"] is None

        okx_quote = snapshot.quotes["okx:BTCUSDT"]
        okx_quote.open_interest_observed_at_ms = 130000
        okx_quote.open_interest_received_at_ms = 130000
        okx_quote.funding_rate_observed_at_ms = 130000
        okx_quote.funding_rate_event_at_ms = 130000
        okx_quote.funding_rate_received_at_ms = 130000
        okx_quote.funding_rate_sample_id = (
            "funding:okx:BTCUSDT:130000:0:400000"
        )
        okx_quote.open_interest_sample_id = open_interest_sample_id(
            venue="okx",
            canonical_symbol="BTCUSDT",
            venue_symbol="BTCUSDT",
            observed_at_ms=130000,
            source="test_fixture",
            raw_value=2_000_000.0,
            value_quote=2_000_000.0,
        )
        bybit_quote = snapshot.quotes["bybit:BTCUSDT"]
        bybit_quote.open_interest_observed_at_ms = 130000
        bybit_quote.open_interest_received_at_ms = 130000
        bybit_quote.funding_rate_observed_at_ms = 130000
        bybit_quote.funding_rate_event_at_ms = 130000
        bybit_quote.funding_rate_received_at_ms = 130000
        bybit_quote.funding_rate_sample_id = (
            "funding:bybit:BTCUSDT:130000:0:400000"
        )
        bybit_quote.open_interest_sample_id = open_interest_sample_id(
            venue="bybit",
            canonical_symbol="BTCUSDT",
            venue_symbol="BTCUSDT",
            observed_at_ms=130000,
            source="test_fixture",
            raw_value=2_000_000.0,
            value_quote=2_000_000.0,
        )

        filtered = runtime._filter_candidates_by_snapshot_freshness(
            [candidate],
            snapshot=snapshot,
            now_ms=130000,
            metrics={},
            ages={},
        )
    finally:
        runtime.journal.close()

    assert filtered == [candidate]
    records = runtime.state.entry_liquidity_qualification_records
    okx_record = next(record for record in records if record["venue"] == "okx")
    assert okx_record["last_class"] == "eligible"
    assert okx_record["consecutive_failures"] == 0
    assert okx_record["suppress_until_ms"] is None
    journal_records = _read_journal_records(tmp_path / "events.jsonl")
    assert not any(
        record["kind"] == "runtime.snapshot_freshness_decision"
        and record["payload"].get("reason") == "perp_open_interest_structural"
        for record in journal_records
    )


@pytest.mark.asyncio
async def test_runtime_blocks_perp_liquidity_only_when_candidate_sizing_requires_it(tmp_path, monkeypatch):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=600000,
            max_order_quote_age_ms=600000,
            sidecar_perp_liquidity_budget_ms=3000,
            live_scan_recovery_success_count=1,
        ),
        strategy=StrategyConfig(
            funding_new_entries_enabled=True,
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
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: OkxMetadataAdapter(),
            Venue.BYBIT: BybitMetadataAdapter(),
        },
    )
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    executor = CapturingEntryExecutor()
    runtime.entry_executor = executor
    candidate = _sidecar_liquidity_required_candidate()
    _mark_final_economics_ready(candidate, 70_000)
    _install_l2_books(runtime, candidate, observed_at_ms=70_000)
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=69000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote("okx", "BTCUSDT", 100.0, 101.0),
            "bybit:BTCUSDT": _quote("bybit", "BTCUSDT", 100.2, 101.2),
        },
        liquidity_lifecycle=[
            LiquidityLifecycle(
                venue="okx",
                observed_at_ms=35000,
                symbol_count=1,
                coverage_usable=1,
            ),
            LiquidityLifecycle(
                venue="bybit",
                observed_at_ms=69000,
                symbol_count=1,
                coverage_usable=1,
            ),
        ],
        candidates=[candidate],
    )
    _install_ws_quotes_from_snapshot(runtime, snapshot, now_ms=70_000)

    _install_v6_snapshot_fixture(monkeypatch, snapshot)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 70000)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    assert executor.contexts == []
    assert runtime.state.last_scan["selected_candidate_count"] == 0
    records = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    assert "runtime.perp_liquidity_stale_blocking" in [
        record["kind"] for record in records
    ]
    decisions = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.snapshot_freshness_decision"
    ]
    assert any(
        d["reason"] == "perp_liquidity_stale_blocking"
        and d["decision"] == "skip_entry"
        and d["source"] == "sidecar_perp_liquidity"
        and d["observed_at_ms"] == 35000
        for d in decisions
    )


@pytest.mark.asyncio
async def test_runtime_blocks_required_sidecar_liquidity_when_current_row_has_no_usable_coverage(tmp_path, monkeypatch):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=600000,
            max_order_quote_age_ms=600000,
            sidecar_perp_liquidity_budget_ms=30000,
            live_scan_recovery_success_count=1,
        ),
        strategy=StrategyConfig(
            funding_new_entries_enabled=True,
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
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: OkxMetadataAdapter(),
            Venue.BYBIT: BybitMetadataAdapter(),
        },
    )
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    executor = CapturingEntryExecutor()
    runtime.entry_executor = executor
    candidate = _sidecar_liquidity_required_candidate()
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=69000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote("okx", "BTCUSDT", 100.0, 101.0),
            "bybit:BTCUSDT": _quote("bybit", "BTCUSDT", 100.2, 101.2),
        },
        liquidity_lifecycle=[
            LiquidityLifecycle(
                venue="okx",
                observed_at_ms=69000,
                symbol_count=0,
                coverage_usable=0,
                degraded_reason="liquidity timeout 10.0s",
            ),
            LiquidityLifecycle(
                venue="bybit",
                observed_at_ms=69000,
                symbol_count=1,
                coverage_usable=1,
            ),
        ],
        candidates=[candidate],
    )
    _install_ws_quotes_from_snapshot(runtime, snapshot, now_ms=70_000)

    _install_v6_snapshot_fixture(monkeypatch, snapshot)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 70000)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    assert executor.contexts == []
    records = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    decisions = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.snapshot_freshness_decision"
    ]
    assert any(
        d["reason"] == "perp_liquidity_stale_blocking"
        and d["decision"] == "skip_entry"
        and d["age_ms"] == 1000
        and d["coverage_usable"] == 0
        and d["degraded_reason"] == "liquidity timeout 10.0s"
        for d in decisions
    )


@pytest.mark.asyncio
async def test_runtime_does_not_block_required_sidecar_liquidity_for_other_symbol_degradation(tmp_path, monkeypatch):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=600000,
            max_order_quote_age_ms=600000,
            sidecar_perp_liquidity_budget_ms=30000,
            live_scan_recovery_success_count=1,
        ),
        strategy=_entry_flow_strategy_config(
            local_l2_enabled=True,
            entry_window_secs=600,
            min_scan_minutes_before_funding=0,
            min_funding_edge_bps=0,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: OkxMetadataAdapter(),
            Venue.BYBIT: BybitMetadataAdapter(),
        },
    )
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    executor = CapturingEntryExecutor()
    runtime.entry_executor = executor
    candidate = _sidecar_liquidity_required_candidate()
    _mark_final_economics_ready(candidate, 70_000)
    _install_l2_books(runtime, candidate, observed_at_ms=70_000)
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=69000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote("okx", "BTCUSDT", 100.0, 101.0),
            "bybit:BTCUSDT": _quote("bybit", "BTCUSDT", 100.2, 101.2),
        },
        liquidity_lifecycle=[
            LiquidityLifecycle(
                venue="okx",
                observed_at_ms=69000,
                symbol_count=2,
                coverage_usable=1,
                degraded_reason="ETHUSDT: fetch failed",
            ),
            LiquidityLifecycle(
                venue="bybit",
                observed_at_ms=69000,
                symbol_count=1,
                coverage_usable=1,
            ),
        ],
        candidates=[candidate],
    )
    _install_ws_quotes_from_snapshot(runtime, snapshot, now_ms=70_000)

    _install_v6_snapshot_fixture(monkeypatch, snapshot)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 70000)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    assert len(executor.contexts) == 1
    assert runtime.state.last_scan["selected_candidate_count"] == 1
    records = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    decisions = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.snapshot_freshness_decision"
    ]
    assert not any(
        d["reason"] == "perp_liquidity_stale_blocking"
        for d in decisions
    )


@pytest.mark.asyncio
async def test_runtime_does_not_skip_fresh_quote_and_l2_for_20s_perp_liquidity_age(tmp_path, monkeypatch):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=600000,
            max_order_quote_age_ms=600000,
            sidecar_perp_liquidity_budget_ms=3000,
            live_scan_recovery_success_count=1,
        ),
        strategy=_entry_flow_strategy_config(
            local_l2_enabled=True,
            entry_window_secs=600,
            min_scan_minutes_before_funding=0,
            min_funding_edge_bps=0,
            max_liquidity_snapshot_age_ms=5000,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: OkxMetadataAdapter(),
            Venue.BYBIT: BybitMetadataAdapter(),
        },
    )
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    executor = CapturingEntryExecutor()
    runtime.entry_executor = executor
    candidate = _freshness_candidate()
    _install_l2_books(runtime, candidate, observed_at_ms=69000)
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=69000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote("okx", "BTCUSDT", 100.0, 101.0),
            "bybit:BTCUSDT": _quote("bybit", "BTCUSDT", 100.2, 101.2),
        },
        liquidity_lifecycle=[
            LiquidityLifecycle(
                venue="okx",
                observed_at_ms=50000,
                symbol_count=1,
                coverage_usable=1,
                publish_interval_ms=20000,
            ),
            LiquidityLifecycle(
                venue="bybit",
                observed_at_ms=50000,
                symbol_count=1,
                coverage_usable=1,
                publish_interval_ms=20000,
            ),
        ],
        candidates=[candidate],
    )
    _install_ws_quotes_from_snapshot(runtime, snapshot, now_ms=70_000)

    _install_v6_snapshot_fixture(monkeypatch, snapshot)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 70000)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    assert len(executor.contexts) == 1
    assert runtime.state.last_scan["dispatched_candidate_count"] == 1
    records = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    key = "okx|BTCUSDT|liquidity"
    assert runtime.state.last_scan["snapshot_freshness_observed_age_ms"][key] == 20000
    assert runtime.state.last_scan["snapshot_freshness_publish_interval_ms"][key] == 20000
    assert runtime.state.last_scan["snapshot_freshness_budget_ms"][key] >= 20000
    decisions = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.snapshot_freshness_decision"
    ]
    assert not any(
        d["reason"] == "perp_liquidity_stale_blocking"
        for d in decisions
    )


@pytest.mark.asyncio
async def test_runtime_blocks_only_when_execution_l2_needed_by_sizing_is_stale(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(mode="live"),
        strategy=StrategyConfig(
            funding_new_entries_enabled=True,
            local_l2_enabled=True,
            max_liquidity_snapshot_age_ms=5000,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: OkxMetadataAdapter(),
            Venue.BYBIT: BybitMetadataAdapter(),
        },
    )
    runtime.entry_executor = CapturingEntryExecutor()
    candidate = _freshness_candidate()
    _install_l2_books(runtime, candidate, observed_at_ms=50000)

    runtime.journal.open()
    try:
        dispatched = await runtime._dispatch_entry(candidate, now_ms=70000, price_hint=100.5)
    finally:
        runtime.journal.close()

    assert dispatched is False
    records = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    assert "runtime.execution_l2_stale" in [record["kind"] for record in records]
    decisions = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.snapshot_freshness_decision"
    ]
    assert any(
        d["reason"] == "execution_l2_stale"
        and d["decision"] == "skip_entry"
        and d["age_ms"] == 20000
        and d["budget_ms"] == 5000
        for d in decisions
    )


@pytest.mark.asyncio
async def test_runtime_allows_entry_when_critical_snapshot_domains_are_fresh(tmp_path, monkeypatch):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=600000,
            max_order_quote_age_ms=600000,
            sidecar_perp_liquidity_budget_ms=3000,
            live_scan_recovery_success_count=1,
        ),
        strategy=_entry_flow_strategy_config(
            local_l2_enabled=True,
            entry_window_secs=600,
            min_scan_minutes_before_funding=0,
            min_funding_edge_bps=0,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: OkxMetadataAdapter(),
            Venue.BYBIT: BybitMetadataAdapter(),
        },
    )
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    executor = CapturingEntryExecutor()
    runtime.entry_executor = executor
    candidate = _freshness_candidate()
    _mark_final_economics_ready(candidate, 70_000)
    _install_l2_books(runtime, candidate, observed_at_ms=70_000)
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=69000,
        acquisition_mode="fresh_sidecar",
        quotes={
            "okx:BTCUSDT": _quote("okx", "BTCUSDT", 100.0, 101.0),
            "bybit:BTCUSDT": _quote("bybit", "BTCUSDT", 100.2, 101.2),
        },
        liquidity_lifecycle=[
            LiquidityLifecycle(venue="okx", observed_at_ms=69000, symbol_count=1, coverage_usable=1),
            LiquidityLifecycle(venue="bybit", observed_at_ms=69000, symbol_count=1, coverage_usable=1),
        ],
        candidates=[candidate],
    )
    _install_ws_quotes_from_snapshot(runtime, snapshot, now_ms=70_000)

    _install_v6_snapshot_fixture(monkeypatch, snapshot)
    monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 70000)

    runtime.journal.open()
    try:
        await runtime.tick()
    finally:
        runtime.journal.close()

    assert len(executor.contexts) == 1
    assert runtime.state.last_scan["dispatched_candidate_count"] == 1
    assert runtime.state.last_scan["snapshot_freshness_metrics"]["okx|BTCUSDT|liquidity"]["fresh"] == 1


@pytest.mark.asyncio
async def test_runtime_does_not_globally_filter_candidate_when_market_observed_stale_but_quote_and_l2_fresh(tmp_path, monkeypatch):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=5000,
            max_order_quote_age_ms=5000,
            live_scan_last_good_max_age_ms=600000,
            live_scan_recovery_success_count=1,
        ),
        strategy=_entry_flow_strategy_config(
            local_l2_enabled=True,
            entry_window_secs=600,
            min_scan_minutes_before_funding=0,
            min_funding_edge_bps=0,
            max_liquidity_snapshot_age_ms=5000,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: OkxMetadataAdapter(),
            Venue.BYBIT: BybitMetadataAdapter(),
        },
    )
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    executor = CapturingEntryExecutor()
    runtime.entry_executor = executor
    candidate = _freshness_candidate()
    _install_l2_books(runtime, candidate, observed_at_ms=69000)
    okx_quote = _quote("okx", "BTCUSDT", 100.0, 101.0)
    bybit_quote = _quote("bybit", "BTCUSDT", 100.2, 101.2)
    okx_quote.observed_at_ms = 69000
    bybit_quote.observed_at_ms = 69000
    okx_quote.source = "sidecar_quote"
    bybit_quote.source = "sidecar_quote"
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=10000,
        acquisition_mode="last_good_sidecar",
        degraded_domains=["market_observed_stale"],
        quotes={
            "okx:BTCUSDT": okx_quote,
            "bybit:BTCUSDT": bybit_quote,
        },
        candidates=[candidate],
    )
    _install_ws_quotes_from_snapshot(runtime, snapshot, now_ms=70_000)

    _install_v6_snapshot_fixture(monkeypatch, snapshot)
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
    assert len(executor.contexts) == 1
    assert runtime.state.last_scan["dispatched_candidate_count"] == 1
    assert runtime.state.last_scan["no_entry_reason"] is None
    fallback = next(
        record["payload"]
        for record in records
        if record["kind"] == "runtime.snapshot_fallback_last_good"
    )
    market_scope = next(
        sample for sample in fallback["candidate_freshness_scope"]
        if sample["candidate_symbol"] == "BTCUSDT"
        and sample["domain"] == "market_observed"
    )
    assert market_scope["candidate_pair_id"] == "btcusdt:okx->bybit"
    assert market_scope["venue"] == "global"
    assert market_scope["source_age_ms"] == 60000
    assert market_scope["fallback_duration_ms"] == 55000
    assert market_scope["blocked"] is False
    assert market_scope["block_reason"] == ""
    scoped_status = runtime.state.last_scan["snapshot_freshness_status"]
    assert scoped_status[
        "market|global|*|snapshot.market_observed_at_ms"
    ]["status"] == "stale"
    assert scoped_status["quote|okx|BTCUSDT|sidecar_quote"]["status"] == "fresh"
    assert scoped_status["quote|bybit|BTCUSDT|sidecar_quote"]["status"] == "fresh"
    assert not any(
        record["kind"] == "scan.no_entry_diagnostics"
        and record["payload"]["reason"] == "no_tradeable_candidates"
        for record in records
    )


def test_close_price_hint_rejects_stale_hot_local_l2_book(tmp_path, monkeypatch):
    from lightfee.core.domain import Venue
    from lightfee.marketdata.l2 import L2BookStatus, LocalL2Book, PriceLevel
    from lightfee.marketdata.local_l2_runtime import LocalL2BookKey

    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
        ),
        strategy=StrategyConfig(max_liquidity_snapshot_age_ms=5000),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.local_l2_runtime.books[
        LocalL2BookKey(venue="okx", symbol="BTCUSDT")
    ] = LocalL2Book(
        venue="okx",
        symbol="BTCUSDT",
        bids=[PriceLevel(price=100.0, quantity=10.0)],
        asks=[PriceLevel(price=101.0, quantity=10.0)],
        status=L2BookStatus.HOT,
        observed_at_ms=1000,
    )

    runtime.journal.open()
    try:
        assert runtime._resolve_local_l2_mid(Venue.OKX, "BTCUSDT", now_ms=7000) == 0.0
    finally:
        runtime.journal.close()

    records = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    stale = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.close_price_evidence_stale"
    ]
    assert stale[-1]["venue"] == "okx"
    assert stale[-1]["symbol"] == "BTCUSDT"
    assert stale[-1]["domain"] == "local_l2_book"
    assert stale[-1]["age_ms"] == 6000
    assert stale[-1]["budget_ms"] == 5000
    assert stale[-1]["decision"] == "reject_price_hint"
    assert stale[-1]["fallback_source"] == "none"


def test_close_price_hint_uses_fresh_ws_bbo_when_local_l2_missing(tmp_path):
    from lightfee.marketdata.ws_bbo import TopBookQuote

    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
        ),
        strategy=StrategyConfig(
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=1500,
            max_liquidity_snapshot_age_ms=300000,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.ws_bbo_cache.update_quote(
        TopBookQuote(
            venue="binance",
            symbol="BTCUSDT",
            bid=100.0,
            ask=101.0,
            observed_at_ms=1000,
            received_at_ms=1001,
            source="binance_book_ticker",
        )
    )

    runtime.journal.open()
    try:
        assert runtime._resolve_local_l2_mid(Venue.BINANCE, "BTCUSDT", now_ms=2000) == 100.5
    finally:
        runtime.journal.close()

    records = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    fallback = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.close_price_evidence_ws_bbo_used"
    ]
    assert fallback[-1]["venue"] == "binance"
    assert fallback[-1]["symbol"] == "BTCUSDT"
    assert fallback[-1]["domain"] == "ws_bbo_cache"
    assert fallback[-1]["age_ms"] == 1000
    assert fallback[-1]["budget_ms"] == 1500
    assert fallback[-1]["decision"] == "use_price_hint"
    assert fallback[-1]["outcome"] == "used_fresh_ws_bbo"


@pytest.mark.asyncio
async def test_close_price_hint_rewarms_stale_ws_bbo_with_rest_top_book(tmp_path):
    from lightfee.marketdata.ws_bbo import TopBookQuote

    now_ms = 3_000
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
        ),
        strategy=StrategyConfig(
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=1_500,
            max_liquidity_snapshot_age_ms=300_000,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.ws_bbo_cache.update_quote(
        TopBookQuote(
            venue="binance",
            symbol="BTCUSDT",
            bid=90.0,
            ask=91.0,
            observed_at_ms=1_000,
            received_at_ms=1_001,
            source="binance_book_ticker",
        )
    )

    class RestRefresher:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, int]] = []

        def refresh_quote(self, venue: str, symbol: str, *, now_ms: int):
            self.calls.append((venue, symbol, now_ms))
            return TopBookQuote(
                venue=venue,
                symbol=symbol,
                bid=100.0,
                ask=101.0,
                observed_at_ms=now_ms - 25,
                received_at_ms=now_ms - 25,
                source=f"{venue}_rest_topbook",
            )

    refresher = RestRefresher()
    runtime.ws_bbo_rest_refresher = refresher

    runtime.journal.open()
    try:
        overlay = await runtime.close_runtime._rewarm_close_price_evidence(
            [("binance", "BTCUSDT")],
            now_ms=now_ms,
        )
        assert overlay[("binance", "BTCUSDT")].source == "binance_rest_topbook"
        assert runtime._resolve_local_l2_mid(
            Venue.BINANCE, "BTCUSDT", now_ms=now_ms,
        ) == 100.5
    finally:
        runtime.journal.close()

    records = _read_journal_records(tmp_path / "events.jsonl")
    kinds = [record["kind"] for record in records]
    assert refresher.calls == [("binance", "BTCUSDT", now_ms)]
    assert "runtime.close_price_evidence_stale" not in kinds
    assert "runtime.close_price_evidence_rest_rewarm_succeeded" in kinds
    used = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.close_price_evidence_ws_bbo_used"
    ][-1]
    assert used["source"] == "binance_rest_topbook"
    assert used["outcome"] == "used_fresh_ws_bbo"


@pytest.mark.asyncio
async def test_close_price_rewarm_failure_reports_stale_quote_evidence(tmp_path):
    from lightfee.marketdata.ws_bbo import TopBookQuote

    now_ms = 3_000
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
        ),
        strategy=StrategyConfig(
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=1,
            max_liquidity_snapshot_age_ms=300_000,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.ws_bbo_cache.update_quote(
        TopBookQuote(
            venue="binance",
            symbol="BTCUSDT",
            bid=90.0,
            ask=91.0,
            observed_at_ms=2_000,
            received_at_ms=2_001,
            source="binance_book_ticker",
        )
    )

    class EmptyRestRefresher:
        def refresh_quote(self, venue: str, symbol: str, *, now_ms: int):
            return None

    runtime.ws_bbo_rest_refresher = EmptyRestRefresher()

    runtime.journal.open()
    try:
        overlay = await runtime.close_runtime._rewarm_close_price_evidence(
            [("binance", "BTCUSDT")],
            now_ms=now_ms,
        )
    finally:
        runtime.journal.close()

    assert overlay == {}
    failures = [
        record["payload"]
        for record in _read_journal_records(tmp_path / "events.jsonl")
        if record["kind"] == "runtime.close_price_evidence_rewarm_failed"
    ]
    assert failures
    assert failures[-1]["venue"] == "binance"
    assert failures[-1]["symbol"] == "BTCUSDT"
    assert failures[-1]["observed_at_ms"] == 2_000
    assert failures[-1]["age_ms"] == 1_000
    assert failures[-1]["budget_ms"] == 1
    assert failures[-1]["endpoint"] == "rest_topbook"
    assert failures[-1]["ws_budget_excluded"] is True
    assert failures[-1]["outcome"] == "rest_topbook_unavailable"


def test_ws_bbo_close_price_hint_prefers_ws_bbo_over_hot_local_l2(tmp_path):
    from lightfee.marketdata.ws_bbo import TopBookQuote

    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
        ),
        strategy=StrategyConfig(
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=1500,
            max_liquidity_snapshot_age_ms=300000,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.local_l2_runtime.books[
        LocalL2BookKey(venue="binance", symbol="BTCUSDT")
    ] = LocalL2Book(
        venue="binance",
        symbol="BTCUSDT",
        bids=[PriceLevel(price=50.0, quantity=10.0)],
        asks=[PriceLevel(price=51.0, quantity=10.0)],
        status=L2BookStatus.HOT,
        observed_at_ms=1990,
    )
    runtime.ws_bbo_cache.update_quote(
        TopBookQuote(
            venue="binance",
            symbol="BTCUSDT",
            bid=100.0,
            ask=101.0,
            observed_at_ms=1000,
            received_at_ms=1001,
            source="binance_book_ticker",
        )
    )

    runtime.journal.open()
    try:
        assert runtime._resolve_local_l2_mid(Venue.BINANCE, "BTCUSDT", now_ms=2000) == 100.5
    finally:
        runtime.journal.close()


def test_ws_bbo_close_price_hint_missing_does_not_fallback_to_local_l2(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
        ),
        strategy=StrategyConfig(
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=1500,
            max_liquidity_snapshot_age_ms=300000,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.local_l2_runtime.books[
        LocalL2BookKey(venue="binance", symbol="BTCUSDT")
    ] = LocalL2Book(
        venue="binance",
        symbol="BTCUSDT",
        bids=[PriceLevel(price=50.0, quantity=10.0)],
        asks=[PriceLevel(price=51.0, quantity=10.0)],
        status=L2BookStatus.HOT,
        observed_at_ms=1990,
    )

    runtime.journal.open()
    try:
        assert runtime._resolve_local_l2_mid(Venue.BINANCE, "BTCUSDT", now_ms=2000) == 0.0
    finally:
        runtime.journal.close()

    records = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    missing = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.close_price_evidence_missing"
    ]
    assert missing[-1]["provider"] == "ws_bbo_quote_lease"
    assert missing[-1]["domain"] == "ws_bbo_cache"
    assert missing[-1]["decision"] == "reject_price_hint"


def test_close_price_hint_rejects_stale_ws_bbo_fallback(tmp_path):
    from lightfee.marketdata.ws_bbo import TopBookQuote

    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
        ),
        strategy=StrategyConfig(
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=1500,
            max_liquidity_snapshot_age_ms=300000,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.ws_bbo_cache.update_quote(
        TopBookQuote(
            venue="binance",
            symbol="BTCUSDT",
            bid=100.0,
            ask=101.0,
            observed_at_ms=1000,
            received_at_ms=1001,
            source="binance_book_ticker",
        )
    )

    runtime.journal.open()
    try:
        assert runtime._resolve_local_l2_mid(Venue.BINANCE, "BTCUSDT", now_ms=3000) == 0.0
    finally:
        runtime.journal.close()

    records = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    stale = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.close_price_evidence_stale"
    ]
    assert stale[-1]["venue"] == "binance"
    assert stale[-1]["symbol"] == "BTCUSDT"
    assert stale[-1]["domain"] == "ws_bbo_cache"
    assert stale[-1]["age_ms"] == 2000
    assert stale[-1]["budget_ms"] == 1500
    assert stale[-1]["decision"] == "reject_price_hint"
    assert stale[-1]["fallback_source"] == "none"


def test_close_price_hint_records_missing_ws_bbo_fallback_quote(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
        ),
        strategy=StrategyConfig(
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=1500,
            max_liquidity_snapshot_age_ms=300000,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)

    runtime.journal.open()
    try:
        assert runtime._resolve_local_l2_mid(Venue.BINANCE, "STEEMUSDT", now_ms=2000) == 0.0
    finally:
        runtime.journal.close()

    records = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    missing = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.close_price_evidence_missing"
    ]
    assert missing[-1]["venue"] == "binance"
    assert missing[-1]["symbol"] == "STEEMUSDT"
    assert missing[-1]["domain"] == "ws_bbo_cache"
    assert missing[-1]["reason"] == "missing_quote"
    assert missing[-1]["budget_ms"] == 1500
    assert missing[-1]["decision"] == "reject_price_hint"


def test_ws_bbo_close_quote_resolver_records_missing_evidence(tmp_path):
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
        ),
        strategy=StrategyConfig(
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=1500,
            max_liquidity_snapshot_age_ms=300000,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)

    runtime.journal.open()
    try:
        assert runtime._resolve_close_price_hint_quote_with_source(
            Venue.BINANCE, "STEEMUSDT",
        ) is None
    finally:
        runtime.journal.close()

    records = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    missing = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.close_price_evidence_missing"
    ]
    assert missing[-1]["venue"] == "binance"
    assert missing[-1]["symbol"] == "STEEMUSDT"
    assert missing[-1]["domain"] == "ws_bbo_cache"
    assert missing[-1]["reason"] == "missing_quote"
    assert missing[-1]["provider"] == "ws_bbo_quote_lease"
    assert missing[-1]["decision"] == "reject_price_hint"


def test_ws_bbo_close_quote_resolver_records_stale_evidence(tmp_path):
    from lightfee.marketdata.ws_bbo import TopBookQuote

    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
        ),
        strategy=StrategyConfig(
            entry_readiness_provider="ws_bbo_quote_lease",
            entry_quote_lease_ttl_ms=1500,
            max_liquidity_snapshot_age_ms=300000,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(config)
    runtime.ws_bbo_cache.update_quote(
        TopBookQuote(
            venue="binance",
            symbol="STEEMUSDT",
            bid=100.0,
            ask=101.0,
            observed_at_ms=1000,
            received_at_ms=1001,
            source="binance_book_ticker",
        )
    )

    runtime.journal.open()
    try:
        assert runtime._resolve_ws_bbo_close_quote(
            Venue.BINANCE, "STEEMUSDT", now_ms=3000,
        ) is None
    finally:
        runtime.journal.close()

    records = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    stale = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.close_price_evidence_stale"
    ]
    assert stale[-1]["venue"] == "binance"
    assert stale[-1]["symbol"] == "STEEMUSDT"
    assert stale[-1]["domain"] == "ws_bbo_cache"
    assert stale[-1]["age_ms"] == 2000
    assert stale[-1]["budget_ms"] == 1500
    assert stale[-1]["provider"] == "ws_bbo_quote_lease"
    assert stale[-1]["decision"] == "reject_price_hint"
