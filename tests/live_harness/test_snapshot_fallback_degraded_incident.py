from __future__ import annotations

import json

import pytest

from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig, StrategyConfig
from lightfee.core.domain import Venue
from lightfee.engine.runtime import LiveRuntime
from lightfee.engine.entry_dispatch_runtime import EntryDispatchRuntime
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from lightfee.sidecar.snapshot import (
    CandidateInput,
    LiquidityLifecycle,
    QuoteSnapshot,
    SidecarSnapshot,
)


@pytest.fixture(autouse=True)
def _isolate_non_canary_snapshot_incident(monkeypatch):
    """This incident suite verifies snapshot semantics, not canary admission."""
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


class TrustedVenueAdapter:
    trading_capability_trusted = True
    okx_base_quantity_step = 0.001

    def passive_metadata(self, symbol: str):
        return {
            "quantity_step": 0.001,
            "min_quantity": 0.001,
            "min_notional": 0.0,
        }


def _candidate(
    symbol: str,
    *,
    sizing_liquidity_source: str = "",
) -> CandidateInput:
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
        sizing_liquidity_source=sizing_liquidity_source,
        economics_complete=True,
        economics_observed_at_ms=69_000,
        calculation_version="v1_exact",
        model_epoch="v1_exact",
        taker_fee_evidence_complete=True,
    )


def _quote(venue: str, symbol: str, observed_at_ms: int) -> QuoteSnapshot:
    # The test is about snapshot fallback scoping.  Keep its synthetic BBO
    # economically admissible under the real final-entry repricing gate.
    bid = 102.0 if venue == "bybit" else 100.0
    ask = 103.0 if venue == "bybit" else 101.0
    return QuoteSnapshot(
        venue=venue,
        symbol=symbol,
        bid=bid,
        ask=ask,
        observed_at_ms=observed_at_ms,
        source="sidecar_quote",
        bid_size=100.0,
        ask_size=100.0,
        volume_24h_quote=10_000_000.0,
        open_interest=2_000_000.0,
    )


def _runtime(tmp_path) -> LiveRuntime:
    config = AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600000,
            max_market_age_ms=5000,
            max_order_quote_age_ms=5000,
            live_scan_last_good_max_age_ms=600000,
            live_scan_recovery_success_count=1,
            sidecar_perp_liquidity_budget_ms=30000,
        ),
        strategy=StrategyConfig(
            local_l2_enabled=False,
            entry_window_secs=600,
            min_scan_minutes_before_funding=0,
            min_funding_edge_bps=0,
            max_liquidity_snapshot_age_ms=5000,
            pending_entry_pre_submit_hedgeable_fill_guard_enabled=False,
            funding_new_entries_enabled=True,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "state.json"),
        ),
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue.OKX: TrustedVenueAdapter(),
            Venue.BYBIT: TrustedVenueAdapter(),
        },
    )
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    runtime.entry_executor = CapturingEntryExecutor()
    return runtime


async def _run_tick(tmp_path, monkeypatch, snapshot: SidecarSnapshot) -> tuple[LiveRuntime, list[dict]]:
    runtime = _runtime(tmp_path)
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
    return runtime, records


def _payload(records: list[dict], kind: str) -> dict:
    return next(record["payload"] for record in records if record["kind"] == kind)


@pytest.mark.asyncio
async def test_last_good_market_observed_fallback_is_candidate_scoped_and_non_blocking(
    tmp_path, monkeypatch,
):
    candidate = _candidate("RIVERUSDT")
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=10000,
        acquisition_mode="last_good_sidecar",
        degraded_domains=["market_observed_stale"],
        quotes={
            "okx:RIVERUSDT": _quote("okx", "RIVERUSDT", 69000),
            "bybit:RIVERUSDT": _quote("bybit", "RIVERUSDT", 69000),
        },
        candidates=[candidate],
    )

    runtime, records = await _run_tick(tmp_path, monkeypatch, snapshot)

    fallback = _payload(records, "runtime.snapshot_fallback_last_good")
    scope = fallback["candidate_freshness_scope"]
    market_scope = next(
        sample for sample in scope
        if sample["candidate_symbol"] == "RIVERUSDT"
        and sample["domain"] == "market_observed"
    )
    assert market_scope["candidate_pair_id"] == "riverusdt:okx->bybit"
    assert market_scope["venue"] == "global"
    assert market_scope["source_age_ms"] == 60000
    assert market_scope["fallback_duration_ms"] == 55000
    assert market_scope["blocked"] is False
    assert market_scope["block_reason"] == ""
    # Fallback health is non-blocking, but a real first leg still requires
    # final executable BBO economics.  This fixture intentionally supplies no
    # quote lease, so the new admission contract must fail closed here.
    assert len(runtime.entry_executor.contexts) == 0
    final_blocks = [
        record for record in records
        if record["kind"] == "entry.dispatch_viability_blocked"
        and record["payload"].get("source") == "final_entry_economics"
    ]
    assert final_blocks and final_blocks[0]["payload"]["reason"] == "missing_final_executable_bbo"


@pytest.mark.asyncio
async def test_degraded_other_symbol_does_not_reject_selected_candidate(
    tmp_path, monkeypatch,
):
    candidate = _candidate("BULLAUSDT")
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=69000,
        acquisition_mode="fresh_sidecar",
        degraded_domains=["liquidity"],
        degraded_symbols={"okx": ["MONUSDT"]},
        quotes={
            "okx:BULLAUSDT": _quote("okx", "BULLAUSDT", 69000),
            "bybit:BULLAUSDT": _quote("bybit", "BULLAUSDT", 69000),
        },
        liquidity_lifecycle=[
            LiquidityLifecycle(
                venue="okx",
                observed_at_ms=69000,
                symbol_count=2,
                coverage_usable=1,
                degraded_reason="MONUSDT: stale liquidity",
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

    runtime, records = await _run_tick(tmp_path, monkeypatch, snapshot)

    degraded = _payload(records, "runtime.snapshot_degraded")
    scope = degraded["candidate_freshness_scope"]
    liquidity_scope = next(
        sample for sample in scope
        if sample["candidate_symbol"] == "BULLAUSDT"
        and sample["domain"] == "liquidity"
        and sample["venue"] == "okx"
    )
    assert liquidity_scope["candidate_pair_id"] == "bullausdt:okx->bybit"
    assert liquidity_scope["source_age_ms"] == 1000
    assert liquidity_scope["blocked"] is False
    assert liquidity_scope["block_reason"] == ""
    assert len(runtime.entry_executor.contexts) == 0
    assert any(
        record["kind"] == "entry.dispatch_viability_blocked"
        and record["payload"].get("source") == "final_entry_economics"
        for record in records
    )
    diagnostics = [record for record in records if record["kind"] == "scan.no_entry_diagnostics"]
    assert diagnostics and diagnostics[0]["payload"]["reason"] == "no_entry_dispatched"


@pytest.mark.asyncio
async def test_degraded_selected_required_liquidity_rejects_candidate_and_enters_no_entry(
    tmp_path, monkeypatch,
):
    candidate = _candidate(
        "MONUSDT",
        sizing_liquidity_source="sidecar_perp_liquidity",
    )
    snapshot = SidecarSnapshot(
        published_at_ms=69000,
        market_observed_at_ms=69000,
        acquisition_mode="fresh_sidecar",
        degraded_domains=["liquidity"],
        degraded_symbols={"okx": ["MONUSDT"]},
        quotes={
            "okx:MONUSDT": _quote("okx", "MONUSDT", 69000),
            "bybit:MONUSDT": _quote("bybit", "MONUSDT", 69000),
        },
        liquidity_lifecycle=[
            LiquidityLifecycle(
                venue="okx",
                observed_at_ms=1000,
                symbol_count=1,
                coverage_usable=1,
                degraded_reason="MONUSDT: stale liquidity",
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

    runtime, records = await _run_tick(tmp_path, monkeypatch, snapshot)

    degraded = _payload(records, "runtime.snapshot_degraded")
    scope = degraded["candidate_freshness_scope"]
    mon_scope = next(
        sample for sample in scope
        if sample["candidate_symbol"] == "MONUSDT"
        and sample["domain"] == "liquidity"
        and sample["venue"] == "okx"
    )
    assert mon_scope["candidate_pair_id"] == "monusdt:okx->bybit"
    assert mon_scope["source_age_ms"] == 69000
    assert mon_scope["blocked"] is True
    assert mon_scope["block_reason"] == "perp_liquidity_stale_blocking"

    decision = next(
        record["payload"]
        for record in records
        if record["kind"] == "runtime.snapshot_freshness_decision"
    )
    assert decision["candidate_symbol"] == "MONUSDT"
    assert decision["candidate_pair_id"] == "monusdt:okx->bybit"
    assert decision["source_age_ms"] == 69000
    assert decision["blocked"] is True
    assert decision["block_reason"] == "perp_liquidity_stale_blocking"

    no_entry = _payload(records, "scan.no_entry_diagnostics")
    assert no_entry["reason"] == "candidate_snapshot_domain_stale"
    sample = no_entry["snapshot_freshness_blocked_samples"][0]
    assert sample["candidate_symbol"] == "MONUSDT"
    assert sample["candidate_pair_id"] == "monusdt:okx->bybit"
    assert sample["domain"] == "liquidity"
    assert sample["venue"] == "okx"
    assert sample["source_age_ms"] == 69000
    assert sample["blocked"] is True
    assert sample["block_reason"] == "perp_liquidity_stale_blocking"
    assert runtime.entry_executor.contexts == []
