from __future__ import annotations

import json
from collections import Counter

import pytest

from lightfee.core.domain import Venue
from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig, StrategyConfig
from lightfee.engine.runtime import LiveRuntime
from lightfee.marketdata.l2 import L2BookStatus, LocalL2Book, PriceLevel
from lightfee.marketdata.local_l2_runtime import LocalL2BookKey
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from lightfee.sidecar.snapshot import (
    CandidateInput,
    LiquidityLifecycle,
    QuoteSnapshot,
    SidecarSnapshot,
    SnapshotFreshness,
    TransferLifecycle,
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
        funding_edge_bps=10.0,
        expected_edge_bps=5.0,
        worst_case_edge_bps=2.0,
        ranking_edge_bps=10.0,
        entry_notional_quote=50.0,
        first_funding_timestamp_ms=400000,
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


def _quote(venue: str, symbol: str, bid: float, ask: float) -> QuoteSnapshot:
    return QuoteSnapshot(
        venue=venue,
        symbol=symbol,
        bid=bid,
        ask=ask,
        bid_size=100.0,
        ask_size=100.0,
        volume_24h_quote=10_000_000.0,
        open_interest=2_000_000.0,
    )


def _quote_with_liquidity(
    venue: str,
    symbol: str,
    *,
    volume_24h_quote: float,
    open_interest: float,
    observed_at_ms: int = 69000,
) -> QuoteSnapshot:
    return QuoteSnapshot(
        venue=venue,
        symbol=symbol,
        bid=100.0,
        ask=101.0,
        observed_at_ms=observed_at_ms,
        source="sidecar_quote",
        bid_size=100.0,
        ask_size=100.0,
        volume_24h_quote=volume_24h_quote,
        open_interest=open_interest,
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
        strategy=StrategyConfig(
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

    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: snapshot)
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
        strategy=StrategyConfig(
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
            ),
            "bybit:BTCUSDT": _quote_with_liquidity(
                "bybit",
                "BTCUSDT",
                volume_24h_quote=10_000_000.0,
                open_interest=2_000_000.0,
                observed_at_ms=now_ms - 20_000,
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
        "lightfee.engine.runtime.evaluate_snapshot_freshness",
        lambda **_kwargs: SnapshotFreshness.LAST_GOOD_FALLBACK,
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
        strategy=StrategyConfig(
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

    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: snapshot)
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
    assert {payload["source"] for payload in resolved_payloads} == {
        "bybit_rest_topbook",
        "okx_rest_topbook",
    }
    assert runtime.ws_bbo_cache.get_quote("okx", "BTCUSDT").source == "okx_rest_topbook"
    assert runtime.ws_bbo_cache.get_quote("bybit", "BTCUSDT").source == "bybit_rest_topbook"


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


def test_snapshot_freshness_decisions_are_rate_limited_by_source_domain(tmp_path):
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
    candidate_b = _freshness_candidate()
    candidate_b.pair_id = "btcusdt:okx->bybit:b"
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
        assert len(first_decisions) == 1
        assert first_decisions[0]["payload"]["reason"] == "invalid_quote"
        assert first_decisions[0]["payload"].get("suppressed_count", 0) == 0

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
    assert len(decisions) == 2
    assert decisions[-1]["payload"]["compact"] is True
    assert decisions[-1]["payload"]["suppressed_count"] == 1


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
    order_quote_stale = next(
        record["payload"]
        for record in records
        if record["kind"] == "runtime.order_quote_stale_skipped"
    )
    assert order_quote_stale["count"] == 1
    assert order_quote_stale["samples"][0]["venue"] == "binance"
    assert order_quote_stale["samples"][0]["symbol"] == "BTCUSDT"
    assert order_quote_stale["samples"][0]["quote_age_ms"] == 10000
    assert order_quote_stale["samples"][0]["blocker_family"] == "stale_quote"
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
        first_funding_timestamp_ms=400000,
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
    runtime.entry_executor = CapturingEntryExecutor()
    candidate = _freshness_candidate()
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
                mark_price=100.5,
                index_price=100.25,
                funding_timestamp_ms=400000,
            ),
            "bybit:BTCUSDT": _quote("bybit", "BTCUSDT", 100.2, 101.2),
        },
        candidates=[candidate],
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
        candidates=[_freshness_candidate()],
    )

    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: snapshot)
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
    decisions = [
        record["payload"]
        for record in records
        if record["kind"] == "runtime.snapshot_freshness_decision"
    ]
    assert any(
        d["venue"] == "okx"
        and d["symbol"] == "BTCUSDT"
        and d["domain"] == "liquidity"
        and d["decision"] == "continue"
        and d["age_ms"] == 35000
        and d["reason"] == "perp_liquidity_stale_advisory"
        and d["fallback_source"] == "last_good_sidecar"
        for d in decisions
    )
    assert "runtime.perp_liquidity_stale_advisory" in [
        record["kind"] for record in records
    ]


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
    assert records_by_venue["okx"] == {
        "venue": "okx",
        "symbol": "BTCUSDT",
        "consecutive_failures": 1,
        "last_failure_at_ms": 70000,
        "suppress_until_ms": None,
        "last_class": "temporary_below_floor",
        "last_observed_open_interest_quote": 900000,
        "last_observed_open_interest_at_ms": 69000,
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


def test_runtime_structural_entry_liquidity_suppression_probes_on_v1_interval(tmp_path):
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
        assert filtered == []

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
    structural = next(
        record["payload"]
        for record in journal_records
        if record["kind"] == "runtime.snapshot_freshness_decision"
        and record["payload"]["reason"] == "perp_open_interest_structural"
    )
    assert structural["eligibility_class"] == "structural_ineligibility"
    assert structural["last_structural_probe_at_ms"] == 69500
    assert structural["endpoint"] == "sidecar_perp_liquidity"
    assert structural["source"] == "sidecar_perp_liquidity"
    assert structural["floor"] == structural["min_open_interest_quote"]
    assert structural["current_value"] == structural["observed_open_interest_quote"]
    assert structural["fallback_source"] == "fresh_sidecar"
    assert structural["targeted_revalidate_required"] is True
    assert structural["targeted_revalidate_scope"] == "entry_candidate"


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

    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: snapshot)
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

    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: snapshot)
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

    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: snapshot)
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
        strategy=StrategyConfig(
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

    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: snapshot)
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
        candidates=[_freshness_candidate()],
    )

    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: snapshot)
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
        strategy=StrategyConfig(
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
        if record["kind"] == "runtime.close_price_evidence_fallback"
    ]
    assert fallback[-1]["venue"] == "binance"
    assert fallback[-1]["symbol"] == "BTCUSDT"
    assert fallback[-1]["domain"] == "ws_bbo_cache"
    assert fallback[-1]["age_ms"] == 1000
    assert fallback[-1]["budget_ms"] == 1500
    assert fallback[-1]["decision"] == "use_price_hint"


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
