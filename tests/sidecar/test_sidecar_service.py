"""Tests for SidecarService: concurrent fetch, degradation, no Chillybot."""

from __future__ import annotations

import asyncio
import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from lightfee.config.schema import AppConfig, StrategyConfig, VenueConfig
from lightfee.sidecar.pairing import (
    FundingCandidateService,
    _funding_contract_block_reasons,
    build_same_symbol_pairs,
)
from lightfee.sidecar.service import (
    SidecarService,
    _canonical_venue_configs,
    _canonicalize_venue_quotes,
    _liquidity_lifecycle_from_quotes,
    _market_failure_reasons,
    _quote_cache_contract_eligible,
    _restorable_prior_last_good_quotes,
)
from lightfee.sidecar.snapshot import CandidateInput, QuoteSnapshot, SidecarSnapshot


def _schedule_audit_build(
    service: SidecarService,
    snapshot: SidecarSnapshot,
    candidate_service,
) -> None:
    service._schedule_audit_snapshot_publish(
        snapshot,
        candidate_service=candidate_service,
        quotes={},
        symbols=[],
        observed_at_ms=snapshot.published_at_ms,
        quote_liquidity_by_venue={},
        skip_venues=set(),
        listed_symbols_by_venue={},
        market_quality_failed_symbols={},
    )


def _complete_contract_fields(*, quantity_precision: int = 3) -> dict[str, object]:
    return {
        "underlying": "BTC",
        "quote_currency": "USDT",
        "contract_type": "linear",
        "contract_multiplier": 1.0,
        "mark_index_source": "venue_index",
        "price_precision": 2,
        "quantity_precision": quantity_precision,
        "price_tick": 0.01,
        "quantity_step_base": 1.0 if quantity_precision == 0 else 0.001,
        "min_quantity_base": 1.0 if quantity_precision == 0 else 0.001,
        "min_notional_quote": 1.0,
        "min_notional_evidence_complete": True,
        "venue_status": "active",
        "contract_normalization_complete": True,
    }


def test_recovery_constructed_calibrator_keeps_strategy_stability_contract(tmp_path):
    service = object.__new__(SidecarService)
    config = AppConfig()
    config.runtime.sidecar_snapshot_path = str(tmp_path / "snapshot.json")
    config.strategy = StrategyConfig(
        funding_forecast_min_samples=18,
        funding_forecast_stability_max_quantile_drift_bps=1.25,
    )
    service.config = config
    service.snapshot_path = config.runtime.sidecar_snapshot_path

    calibrator = service._ensure_forecast_calibrator()

    assert calibrator._min_samples == 18
    assert calibrator._max_quantile_drift_bps == 1.25


def test_funding_candidate_service_reuses_prepared_context() -> None:
    quotes = {
        "cheap:BTCUSDT": QuoteSnapshot(
            venue="cheap", symbol="BTCUSDT", bid=99.9, ask=100.0,
            funding_rate_bps=2.0, funding_timestamp_ms=1_000_000,
            funding_interval_ms=28_800_000,
        ),
        "rich:BTCUSDT": QuoteSnapshot(
            venue="rich", symbol="BTCUSDT", bid=100.3, ask=100.4,
            funding_rate_bps=8.0, funding_timestamp_ms=1_000_000,
            funding_interval_ms=28_800_000,
        ),
    }
    service = FundingCandidateService(
        strategy=StrategyConfig(
            entry_notional_cap_quote=30.0,
            live_entry_notional_cap_quote=30.0,
            funding_missing_margin_fallback_notional_quote=15.0,
        ),
        venue_fee_bps={"cheap": 1.0, "rich": 1.0},
        venue_maker_fee_bps={"cheap": 0.5, "rich": 0.5},
        venue_notional_caps={"cheap": 25.0, "rich": 25.0},
        passive_execution_enabled=False,
    )

    first = service.build(quotes, ["BTCUSDT"], observed_at_ms=1)
    allocator_id = id(service._allocator)
    second = service.build(quotes, ["BTCUSDT"], observed_at_ms=1)

    assert first == second
    assert id(service._allocator) == allocator_id


def test_liquidity_lifecycle_explains_source_quote_excluded_from_data_plane() -> None:
    """Listed symbols without an executable quote must not publish a blank gap."""
    quote = QuoteSnapshot(
        venue="okx",
        symbol="BTCUSDT",
        bid=100.0,
        ask=101.0,
        volume_24h_quote=10_000_000.0,
        open_interest=2_000_000.0,
        open_interest_evidence_status="observed",
        open_interest_observed_at_ms=10_000,
        open_interest_event_at_ms=10_000,
        open_interest_received_at_ms=10_000,
        open_interest_source="okx_open_interest",
        open_interest_sample_id="okx:BTCUSDT:10000",
        open_interest_venue_symbol="BTC-USDT-SWAP",
    )

    lifecycle = _liquidity_lifecycle_from_quotes(
        configured_venues=["okx"],
        quotes={"okx:BTCUSDT": quote},
        listed_symbols_by_venue={"okx": {"BTCUSDT", "ETHUSDT"}},
        market_quality_failed_symbols={},
        observed_at_ms=10_000,
    )

    assert lifecycle[0].symbol_count == 2
    assert lifecycle[0].coverage_usable == 1
    assert lifecycle[0].degraded_reason == "liquidity_quote_unavailable:1"


def test_canary_conservative_tier_defers_symbol_specific_buffer_to_pairing(
    tmp_path,
) -> None:
    service = object.__new__(SidecarService)
    service.config = AppConfig(
        symbols=["BTCUSDT"],
        strategy=StrategyConfig(
            funding_canary_enabled=True,
            funding_canary_require_account_fee_evidence=False,
            funding_canary_conservative_fee_buffer_bps=2.0,
        ),
        venues=[
            VenueConfig(venue="cheap", taker_fee_bps=1.0, maker_fee_bps=0.1),
            VenueConfig(venue="rich", taker_fee_bps=2.0, maker_fee_bps=0.2),
        ],
    )
    service.config.runtime.funding_fee_evidence_path = str(tmp_path / "missing.json")

    candidate_service = service._new_candidate_service(now_ms=1_000)

    assert candidate_service._fee_by_venue == {"cheap": 1.0, "rich": 2.0}
    assert candidate_service._maker_fee_by_venue == {"cheap": 0.1, "rich": 0.2}


def test_source_identity_corruption_is_quarantined_per_symbol() -> None:
    duplicate = QuoteSnapshot(
        venue="binance",
        symbol="BTCUSDT",
        bid=100.0,
        ask=101.0,
    )
    accepted, failures = _canonicalize_venue_quotes(
        "binance",
        {
            "binance:BTCUSDT": duplicate,
            "BINANCE:btcusdt": QuoteSnapshot(**duplicate.__dict__),
            "binance:ETHUSDT": QuoteSnapshot(
                venue="okx",
                symbol="ETHUSDT",
                bid=200.0,
                ask=201.0,
            ),
        },
        requested_symbols={"BTCUSDT", "ETHUSDT"},
    )

    assert accepted == {}
    assert failures == {
        "BTCUSDT": "duplicate_quote_identity",
        "ETHUSDT": "quote_source_venue_mismatch",
    }


def test_source_identity_casing_is_canonicalized_in_key_and_payload() -> None:
    accepted, failures = _canonicalize_venue_quotes(
        "okx",
        {
            "OKX:btcusdt": QuoteSnapshot(
                venue="OKX",
                symbol="btcusdt",
                bid=100.0,
                ask=101.0,
            )
        },
        requested_symbols={"BTCUSDT"},
    )

    assert failures == {}
    assert list(accepted) == ["okx:BTCUSDT"]
    assert accepted["okx:BTCUSDT"].venue == "okx"
    assert accepted["okx:BTCUSDT"].symbol == "BTCUSDT"


def test_zero_observation_is_quarantined_before_atomic_publish() -> None:
    failures = _market_failure_reasons(
        {
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=0,
                funding_rate_bps=1.0,
                funding_timestamp_ms=2_000,
                funding_interval_ms=28_800_000,
            )
        }
    )

    assert failures == {"BTCUSDT": "observed_at_ms_invalid"}


def test_duplicate_venue_aliases_share_one_operational_fetch() -> None:
    class Source:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_all(self, symbols: list[str]) -> dict[str, QuoteSnapshot]:
            self.calls += 1
            return {
                "binance:BTCUSDT": QuoteSnapshot(
                    venue="binance",
                    symbol="BTCUSDT",
                    bid=100.0,
                    ask=101.0,
                    observed_at_ms=1_000,
                    funding_rate_bps=1.0,
                    funding_timestamp_ms=2_000,
                    funding_interval_ms=28_800_000,
                )
            }

    service = object.__new__(SidecarService)
    service.config = AppConfig(
        venues=[
            VenueConfig(venue=" BINANCE ", taker_fee_bps=1.0),
            VenueConfig(venue="binance", taker_fee_bps=9.0),
        ]
    )
    source = Source()
    service._exchange_sources = {"binance": source}

    results = asyncio.run(service._fetch_all_venues(["BTCUSDT"], timeout_s=1.0))

    assert list(_canonical_venue_configs(service.config.venues)) == ["binance"]
    assert source.calls == 1
    assert [venue for venue, *_ in results] == ["binance"]


def test_partial_refresh_updates_last_good_cache_per_key() -> None:
    service = object.__new__(SidecarService)
    old_binance = QuoteSnapshot(
        venue="binance", symbol="BTCUSDT", bid=99.0, ask=100.0,
        observed_at_ms=1_000,
    )
    old_okx = QuoteSnapshot(
        venue="okx", symbol="BTCUSDT", bid=100.0, ask=101.0,
        observed_at_ms=1_000,
    )
    fresh_binance = QuoteSnapshot(
        venue="binance", symbol="BTCUSDT", bid=101.0, ask=102.0,
        observed_at_ms=2_000,
    )
    service._last_good_quotes = {
        "binance:BTCUSDT": old_binance,
        "okx:BTCUSDT": old_okx,
    }
    service._last_good_at_ms = 1_100
    service._last_good_at_ms_by_key = {
        "binance:BTCUSDT": 1_100,
        "okx:BTCUSDT": 1_100,
    }
    previous_quotes = service._last_good_quotes
    previous_epochs = service._last_good_at_ms_by_key

    service._update_last_good_quote_cache(
        {"binance:BTCUSDT": fresh_binance},
        {"binance:BTCUSDT"},
        published_at_ms=2_100,
    )

    assert service._last_good_quotes["binance:BTCUSDT"].bid == 101.0
    assert service._last_good_quotes["okx:BTCUSDT"] == old_okx
    assert service._last_good_at_ms_by_key == {
        "binance:BTCUSDT": 2_100,
        "okx:BTCUSDT": 1_100,
    }
    assert service._last_good_quotes is not previous_quotes
    assert service._last_good_at_ms_by_key is not previous_epochs
    assert previous_quotes["binance:BTCUSDT"].bid == 99.0
    assert previous_epochs["binance:BTCUSDT"] == 1_100


def test_incomplete_contract_cannot_advance_last_good_cache() -> None:
    quote = QuoteSnapshot(
        venue="binance",
        symbol="BTCUSDT",
        bid=100.0,
        ask=101.0,
        observed_at_ms=2_000,
        funding_rate_bps=1.0,
        funding_rate_observed_at_ms=1_000,
        funding_rate_received_at_ms=1_000,
        funding_rate_source="test_fixture",
        funding_rate_sample_id="funding:binance:BTCUSDT:1000:1:30000000",
        funding_timestamp_ms=30_000_000,
        funding_interval_ms=28_800_000,
        underlying="BTC",
        quote_currency="USDT",
        contract_type="linear",
        contract_multiplier=1.0,
        mark_index_source="venue_index",
        price_precision=2,
        quantity_precision=3,
        venue_status="active",
        contract_normalization_complete=False,
    )

    assert _quote_cache_contract_eligible(quote) is False
    quote.contract_normalization_complete = True
    quote.price_tick = 0.01
    quote.quantity_step_base = 0.001
    quote.min_quantity_base = 0.001
    quote.min_notional_quote = 1.0
    quote.min_notional_evidence_complete = True
    assert _quote_cache_contract_eligible(quote) is True
    quote.quantity_precision = 0
    assert _quote_cache_contract_eligible(quote) is True


def test_restart_restore_accepts_only_fresh_direct_contract_truth() -> None:
    quote = QuoteSnapshot(
        venue="binance",
        symbol="BTCUSDT",
        bid=100.0,
        ask=101.0,
        observed_at_ms=1_000,
        funding_rate_bps=1.0,
        funding_rate_observed_at_ms=1_000,
        funding_rate_event_at_ms=1_000,
        funding_rate_received_at_ms=1_000,
        funding_rate_source="binance_funding_info",
        funding_rate_sample_id="funding:binance:BTCUSDT:1000:1:30000000",
        funding_timestamp_ms=30_000_000,
        funding_interval_ms=28_800_000,
        **_complete_contract_fields(quantity_precision=0),
    )
    fresh = SidecarSnapshot(
        acquisition_mode="fresh_sidecar",
        source_mode="direct_market",
        quotes={"binance:BTCUSDT": quote},
    )

    restored = _restorable_prior_last_good_quotes(
        fresh,
        configured_venues={"binance"},
        configured_symbols={"BTCUSDT"},
        now_ms=1_500,
        max_age_ms=500,
    )

    assert list(restored) == ["binance:BTCUSDT"]
    assert restored["binance:BTCUSDT"] is not quote
    assert _restorable_prior_last_good_quotes(
        fresh,
        configured_venues={"binance"},
        configured_symbols={"BTCUSDT"},
        now_ms=1_501,
        max_age_ms=500,
    ) == {}
    fresh.acquisition_mode = "last_good_sidecar"
    assert _restorable_prior_last_good_quotes(
        fresh,
        configured_venues={"binance"},
        configured_symbols={"BTCUSDT"},
        now_ms=1_500,
        max_age_ms=500,
    ) == {}


def test_restart_restore_never_promotes_legacy_schema_market_evidence() -> None:
    quote = QuoteSnapshot(
        venue="binance",
        symbol="BTCUSDT",
        bid=100.0,
        ask=101.0,
        observed_at_ms=1_000,
        funding_rate_bps=1.0,
        funding_timestamp_ms=30_000_000,
        funding_interval_ms=28_800_000,
        **_complete_contract_fields(),
    )
    legacy = SidecarSnapshot(
        schema_version=2,
        acquisition_mode="fresh_sidecar",
        source_mode="direct_market",
        quotes={"binance:BTCUSDT": quote},
    )

    assert _restorable_prior_last_good_quotes(
        legacy,
        configured_venues={"binance"},
        configured_symbols={"BTCUSDT"},
        now_ms=1_500,
        max_age_ms=500,
    ) == {}

    legacy.schema_version = 3
    assert _restorable_prior_last_good_quotes(
        legacy,
        configured_venues={"binance"},
        configured_symbols={"BTCUSDT"},
        now_ms=1_500,
        max_age_ms=500,
    ) == {}


def test_service_restart_primes_first_outage_fallback(monkeypatch, tmp_path) -> None:
    prior = SidecarSnapshot(
        published_at_ms=1_000,
        market_observed_at_ms=1_000,
        acquisition_mode="fresh_sidecar",
        source_mode="direct_market",
        quotes={
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                observed_at_ms=1_000,
                funding_rate_bps=1.0,
                funding_rate_observed_at_ms=1_000,
                funding_rate_received_at_ms=1_000,
                funding_rate_source="test_fixture",
                funding_rate_sample_id="funding:binance:BTCUSDT:1000:1:30000000",
                funding_timestamp_ms=30_000_000,
                funding_interval_ms=28_800_000,
                **_complete_contract_fields(),
            )
        },
    )
    monkeypatch.setattr("lightfee.sidecar.service.load_snapshot", lambda _path: prior)
    monkeypatch.setattr("lightfee.sidecar.service.time.time", lambda: 1.5)
    config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[VenueConfig(venue="binance")],
    )
    config.runtime.sidecar_snapshot_path = str(tmp_path / "sidecar.json")
    config.runtime.live_scan_last_good_max_age_ms = 500

    service = SidecarService(config)

    assert list(service._inject_last_good(
        "binance", ["BTCUSDT"], now_ms=1_500
    )) == ["binance:BTCUSDT"]


@pytest.mark.asyncio
async def test_audit_writer_is_nonblocking_and_drops_overlapping_generations(
    tmp_path,
    monkeypatch,
) -> None:
    service = object.__new__(SidecarService)
    service.config = AppConfig(venues=[])
    service.snapshot_path = tmp_path / "sidecar.json"
    service._liquidity_timeout_s = 0.01
    service._audit_pending_build = None
    service._audit_publish_task = None
    service._last_audit_schedule_monotonic = 0.0
    service._audit_executor = ThreadPoolExecutor(max_workers=1)

    async def no_liquidity(*_args, **_kwargs):
        return []

    service._fetch_liquidity_all_venues = no_liquidity
    service._configured_venue_names = lambda: []
    started = threading.Event()
    release = threading.Event()

    class BlockingBuilder:
        def __init__(self) -> None:
            self.calls = 0

        def build(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                started.set()
                assert release.wait(timeout=2.0)
            return []

    builder = BlockingBuilder()
    published_generations: list[int] = []
    monkeypatch.setattr(
        "lightfee.sidecar.service.publish_snapshot",
        lambda snapshot, _path: published_generations.append(
            snapshot.published_at_ms
        ),
    )

    try:
        _schedule_audit_build(service, SidecarSnapshot(published_at_ms=1), builder)
        task = service._audit_publish_task
        assert task is not None
        for _ in range(100):
            if started.is_set():
                break
            await asyncio.sleep(0.001)
        assert started.is_set()

        # A running diagnostic audit does not queue more OI work behind itself.
        _schedule_audit_build(service, SidecarSnapshot(published_at_ms=2), builder)
        _schedule_audit_build(service, SidecarSnapshot(published_at_ms=3), builder)
        assert service._audit_pending_build is None
        assert not task.done()

        release.set()
        await asyncio.wait_for(task, timeout=2.0)
    finally:
        release.set()
        service._audit_executor.shutdown(wait=True, cancel_futures=True)

    assert published_generations == [1]
    assert builder.calls == 1
    assert service._audit_publish_task is None


@pytest.mark.asyncio
async def test_audit_schedule_skips_cache_republish_and_respects_minimum_interval(
    monkeypatch,
) -> None:
    service = object.__new__(SidecarService)
    service._audit_pending_build = None
    service._audit_publish_task = None
    service._last_audit_schedule_monotonic = 0.0
    service._entry_cache_only_refresh = True
    runs = 0

    async def run_once() -> None:
        nonlocal runs
        runs += 1
        service._audit_pending_build = None
        service._audit_publish_task = None

    service._run_audit_snapshot_writer = run_once
    clock = 100.0
    monkeypatch.setattr("lightfee.sidecar.service.time.monotonic", lambda: clock)

    _schedule_audit_build(service, SidecarSnapshot(published_at_ms=1), object())
    await asyncio.sleep(0)
    assert runs == 0

    service._entry_cache_only_refresh = False
    _schedule_audit_build(service, SidecarSnapshot(published_at_ms=2), object())
    await asyncio.sleep(0)
    assert runs == 1

    clock += 59.0
    _schedule_audit_build(service, SidecarSnapshot(published_at_ms=3), object())
    await asyncio.sleep(0)
    assert runs == 1

    clock += 1.0
    _schedule_audit_build(service, SidecarSnapshot(published_at_ms=4), object())
    await asyncio.sleep(0)
    assert runs == 2


@pytest.mark.asyncio
async def test_audit_failure_does_not_poison_next_generation(
    tmp_path,
    monkeypatch,
) -> None:
    service = object.__new__(SidecarService)
    service.config = AppConfig(venues=[])
    service.snapshot_path = tmp_path / "sidecar.json"
    service._liquidity_timeout_s = 0.01
    service._audit_pending_build = None
    service._audit_publish_task = None
    service._last_audit_schedule_monotonic = 0.0
    service._audit_executor = ThreadPoolExecutor(max_workers=1)

    async def no_liquidity(*_args, **_kwargs):
        return []

    service._fetch_liquidity_all_venues = no_liquidity
    service._configured_venue_names = lambda: []

    class FailingBuilder:
        def build(self, *_args, **_kwargs):
            raise RuntimeError("audit-build-failed")

    class HealthyBuilder:
        def build(self, *_args, **_kwargs):
            return []

    published_generations: list[int] = []
    monkeypatch.setattr(
        "lightfee.sidecar.service.publish_snapshot",
        lambda snapshot, _path: published_generations.append(
            snapshot.published_at_ms
        ),
    )

    try:
        _schedule_audit_build(
            service,
            SidecarSnapshot(published_at_ms=1),
            FailingBuilder(),
        )
        failed_task = service._audit_publish_task
        assert failed_task is not None
        await asyncio.wait_for(failed_task, timeout=2.0)
        assert published_generations == []

        service._last_audit_schedule_monotonic = 0.0
        _schedule_audit_build(
            service,
            SidecarSnapshot(published_at_ms=2),
            HealthyBuilder(),
        )
        healthy_task = service._audit_publish_task
        assert healthy_task is not None
        await asyncio.wait_for(healthy_task, timeout=2.0)
    finally:
        service._audit_executor.shutdown(wait=True, cancel_futures=True)

    assert published_generations == [2]
    assert service._audit_publish_task is None


@pytest.mark.asyncio
async def test_close_cancels_blocked_audit_without_hanging() -> None:
    service = object.__new__(SidecarService)
    service._entry_venue_fetch_tasks = {}
    service._entry_venue_late_tasks = set()
    service.entry_venue_republish_event = asyncio.Event()
    service._audit_pending_build = {"snapshot": object()}
    service._exchange_sources = {}
    service._spread_bbo_sources = {}
    service._liquidity_sources = {}
    cancelled = asyncio.Event()

    async def blocked_audit() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    class RecordingExecutor:
        def __init__(self) -> None:
            self.calls: list[tuple[bool, bool]] = []

        def shutdown(self, *, wait: bool, cancel_futures: bool) -> None:
            self.calls.append((wait, cancel_futures))

    executor = RecordingExecutor()
    service._audit_executor = executor
    service._audit_publish_task = asyncio.create_task(blocked_audit())
    await asyncio.sleep(0)

    await asyncio.wait_for(service.close(), timeout=0.1)

    assert cancelled.is_set()
    assert service._audit_publish_task is None
    assert service._audit_pending_build is None
    assert executor.calls == [(False, True)]


@pytest.mark.asyncio
async def test_entry_frontier_oracle_replaces_a_transport_omission_with_fail_closed_generation(
    tmp_path,
    monkeypatch,
) -> None:
    """The post-install oracle must never leave an omitted route executable."""
    service = object.__new__(SidecarService)
    service.snapshot_path = tmp_path / "sidecar.json"
    snapshot = SidecarSnapshot(
        candidates=[
            CandidateInput(
                long_venue="binance",
                short_venue="bybit",
                symbol="BTCUSDT",
                funding_diff_bps=1.0,
                funding_edge_bps=1.0,
                expected_edge_bps=1.0,
                worst_case_edge_bps=1.0,
                ranking_edge_bps=1.0,
                pair_id="btcusdt:binance->bybit",
                economics_complete=True,
            )
        ],
        candidate_build_diagnostics={
            "seed_pair_count": 1,
            "pair_decision_count": 1,
            "eligible_candidate_count": 1,
            "omitted_eligible_count": 0,
            "eligible_frontier_complete": True,
            "frontier_stop_reason": "all_pairs_decided",
        },
    )
    published: list[SidecarSnapshot] = []
    monkeypatch.setattr(
        "lightfee.sidecar.service.funding_entry_snapshot_identity",
        lambda _path, *, verify_digest: ("generation-1", 1, 1),
    )
    monkeypatch.setattr(
        "lightfee.sidecar.service.load_funding_entry_snapshot",
        lambda _path: SidecarSnapshot(candidates=[]),
    )
    monkeypatch.setattr(
        "lightfee.sidecar.service.publish_funding_entry_snapshot",
        lambda failed_snapshot, _path: published.append(failed_snapshot),
    )

    await service._verify_entry_frontier_oracle(
        snapshot,
        generation_id="generation-1",
        expected_ids=("btcusdt:binance->bybit",),
    )

    assert len(published) == 1
    failed = published[0].candidate_build_diagnostics
    assert failed["eligible_frontier_complete"] is False
    assert failed["omitted_eligible_count"] == 1
    assert failed["frontier_stop_reason"] == "funding_entry_opportunity_omitted"


@pytest.mark.asyncio
async def test_entry_frontier_oracle_missing_pair_id_fails_closed_before_task_start(
    tmp_path,
    monkeypatch,
) -> None:
    """An eligible row without its identity cannot wait for async auditing."""
    service = object.__new__(SidecarService)
    service.snapshot_path = tmp_path / "sidecar.json"
    service._entry_frontier_oracle_tasks = set()
    snapshot = SidecarSnapshot(
        candidates=[
            CandidateInput(
                long_venue="binance",
                short_venue="bybit",
                symbol="BTCUSDT",
                funding_diff_bps=1.0,
                funding_edge_bps=1.0,
                expected_edge_bps=1.0,
                worst_case_edge_bps=1.0,
                ranking_edge_bps=1.0,
                economics_complete=True,
            )
        ],
        candidate_build_diagnostics={
            "seed_pair_count": 1,
            "pair_decision_count": 1,
            "eligible_candidate_count": 1,
            "omitted_eligible_count": 0,
            "eligible_frontier_complete": True,
            "frontier_stop_reason": "all_pairs_decided",
        },
    )
    published: list[SidecarSnapshot] = []
    monkeypatch.setattr(
        "lightfee.sidecar.service.publish_funding_entry_snapshot",
        lambda failed_snapshot, _path: published.append(failed_snapshot),
    )

    service._schedule_entry_frontier_oracle(snapshot, "generation-1")

    assert service._entry_frontier_oracle_tasks == set()
    assert len(published) == 1
    failed = published[0].candidate_build_diagnostics
    assert failed["eligible_frontier_complete"] is False
    assert failed["omitted_eligible_count"] == 1
    assert failed["frontier_stop_reason"] == "funding_entry_opportunity_omitted"


@pytest.mark.asyncio
async def test_future_quote_is_quarantined_before_candidate_build_and_publish(
    tmp_path,
    monkeypatch,
) -> None:
    service = object.__new__(SidecarService)
    service.config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[VenueConfig(venue="binance"), VenueConfig(venue="okx")],
    )
    service.snapshot_path = tmp_path / "sidecar.json"
    service.config.runtime.sidecar_snapshot_path = str(service.snapshot_path)
    service._funding_timeout_s = 1.0
    service._liquidity_timeout_s = 1.0
    service._last_good_quotes = {}
    service._last_good_at_ms = 0
    service._last_liquidity_publish_at_ms = 0

    async def funding_results(symbols, timeout_s):
        return [
            (
                "binance",
                {
                    "binance:BTCUSDT": QuoteSnapshot(
                        venue="binance", symbol="BTCUSDT", bid=100.0, ask=101.0,
                        observed_at_ms=1_500, funding_rate_bps=1.0,
                        funding_rate_observed_at_ms=1_500,
                        funding_rate_event_at_ms=1_500,
                        funding_rate_received_at_ms=1_500,
                        funding_rate_source="binance_funding_info",
                        funding_rate_sample_id=(
                            "funding:binance:BTCUSDT:1500:1:30000000"
                        ),
                        funding_timestamp_ms=30_000_000,
                        funding_interval_ms=28_800_000,
                        **_complete_contract_fields(),
                    )
                },
                None,
                set(),
            ),
            (
                "okx",
                {
                    "okx:BTCUSDT": QuoteSnapshot(
                        venue="okx", symbol="BTCUSDT", bid=100.0, ask=101.0,
                        observed_at_ms=2_500, funding_rate_bps=-1.0,
                        funding_rate_observed_at_ms=2_500,
                        funding_rate_event_at_ms=2_500,
                        funding_rate_received_at_ms=2_500,
                        funding_rate_source="okx_funding_rate",
                        funding_rate_sample_id=(
                            "funding:okx:BTCUSDT:2500:-1:30000000"
                        ),
                        funding_timestamp_ms=30_000_000,
                        funding_interval_ms=28_800_000,
                        **_complete_contract_fields(quantity_precision=0),
                    )
                },
                None,
                set(),
            ),
        ]

    async def liquidity_results(
        symbols, timeout_s, quote_liquidity_by_venue=None, skip_venues=None,
    ):
        return [
            ("binance", {"binance:BTCUSDT": object()}, None, set()),
            ("okx", {"okx:BTCUSDT": object()}, None, set()),
        ]

    service._fetch_all_venues = funding_results
    service._fetch_liquidity_all_venues = liquidity_results
    clock = iter([1.0, 1.5, 2.0, 3.0])
    monkeypatch.setattr("lightfee.sidecar.service.time.time", lambda: next(clock))

    snapshot = await service.refresh_once()

    assert list(snapshot.quotes) == ["binance:BTCUSDT"]
    assert snapshot.candidate_build_diagnostics["quarantined_future_quote_count"] == 1
    assert snapshot.degraded_symbols == {"okx": ["BTCUSDT"]}
    assert "observed_at_ms_after_candidate_build" in (
        next(row for row in snapshot.market_lifecycle if row.venue == "okx").degraded_reason
    )
    assert list(service._last_good_quotes) == ["binance:BTCUSDT"]


def test_pairing_rejects_zero_funding_schedule_proof() -> None:
    diagnostics: dict[str, object] = {}
    quotes = {
        f"{venue}:BTCUSDT": QuoteSnapshot(
            venue=venue,
            symbol="BTCUSDT",
            bid=100.0,
            ask=101.0,
            funding_rate_bps=rate,
            funding_timestamp_ms=0,
            funding_interval_ms=28_800_000,
        )
        for venue, rate in (("binance", 1.0), ("okx", 8.0))
    }

    assert build_same_symbol_pairs(
        quotes,
        ["BTCUSDT"],
        diagnostics=diagnostics,
    ) == []
    assert diagnostics["rejection_counts"] == {"invalid_trade_quote": 2}


def test_pairing_accepts_zero_decimal_precision_as_integer_lot_proof() -> None:
    common = {
        "symbol": "BTCUSDT",
        "bid": 100.0,
        "ask": 101.0,
        "funding_timestamp_ms": 30_000_000,
        "funding_interval_ms": 28_800_000,
        **_complete_contract_fields(quantity_precision=0),
    }
    long_quote = QuoteSnapshot(venue="binance", funding_rate_bps=1.0, **common)
    short_quote = QuoteSnapshot(venue="hyperliquid", funding_rate_bps=2.0, **common)

    assert _funding_contract_block_reasons(long_quote, short_quote) == ()


def test_pairing_canonicalizes_duplicate_requested_symbols_once() -> None:
    quotes = {
        f"{venue}:BTCUSDT": QuoteSnapshot(
            venue=venue,
            symbol="BTCUSDT",
            bid=100.0,
            ask=101.0,
            funding_rate_bps=rate,
            funding_timestamp_ms=2_000,
            funding_interval_ms=28_800_000,
        )
        for venue, rate in (("binance", 1.0), ("okx", 8.0))
    }
    diagnostics: dict[str, object] = {}

    candidates = build_same_symbol_pairs(
        quotes,
        ["BTCUSDT", "btcusdt", " BTCUSDT "],
        diagnostics=diagnostics,
    )

    assert len(candidates) == 1
    assert diagnostics["requested_symbol_count"] == 1
    assert diagnostics["output_candidate_count"] == 1
    assert diagnostics["seed_pair_count"] == 2
    assert diagnostics["pair_decision_count"] == 2
    assert diagnostics["eligible_frontier_complete"] is True


class TestSidecarPairingV2:
    """Pairing must produce V2-native candidate identity fields."""

    def test_pair_id_in_candidate(self):
        q = {
            "binance:btcusdt": QuoteSnapshot(
                venue="binance", symbol="BTCUSDT",
                bid=50000, ask=50001, funding_rate_bps=5.0,
                funding_timestamp_ms=1700000001000,
                funding_interval_ms=28_800_000,
            ),
            "okx:btcusdt": QuoteSnapshot(
                venue="okx", symbol="BTCUSDT",
                bid=50100, ask=50200, funding_rate_bps=15.0,
                funding_timestamp_ms=1700000002000,
                funding_interval_ms=28_800_000,
            ),
        }
        strategy = StrategyConfig(
            entry_notional_cap_quote=40.0,
            live_entry_notional_cap_quote=30.0,
            funding_missing_margin_fallback_notional_quote=15.0,
        )
        candidates = build_same_symbol_pairs(q, ["BTCUSDT"], strategy=strategy)
        assert len(candidates) == 1
        c = candidates[0]
        assert c.pair_id == "btcusdt:binance->okx"
        assert c.long_venue == "binance"
        assert c.short_venue == "okx"

    def test_direction_consistent_using_mid(self):
        """V2 fix: direction_consistent uses mid prices, not ask."""
        q = {
            "binance:btcusdt": QuoteSnapshot(
                venue="binance", symbol="BTCUSDT",
                bid=50000, ask=50001, funding_rate_bps=5.0,
                funding_timestamp_ms=1700000001000,
                funding_interval_ms=28_800_000,
            ),
            "okx:btcusdt": QuoteSnapshot(
                venue="okx", symbol="BTCUSDT",
                bid=50100, ask=50200, funding_rate_bps=15.0,
                funding_timestamp_ms=1700000002000,
                funding_interval_ms=28_800_000,
            ),
        }
        strategy = StrategyConfig(
            entry_notional_cap_quote=40.0,
            live_entry_notional_cap_quote=30.0,
            funding_missing_margin_fallback_notional_quote=15.0,
        )
        candidates = build_same_symbol_pairs(q, ["BTCUSDT"], strategy=strategy)
        assert len(candidates) >= 1
        c = candidates[0]
        # funding_diff > 0 and short_mid >= long_mid
        assert c.direction_consistent is True

    def test_interval_aligned(self):
        q = {
            "binance:btcusdt": QuoteSnapshot(
                venue="binance", symbol="BTCUSDT",
                bid=50000, ask=50001, funding_rate_bps=5.0,
                funding_timestamp_ms=1700000000000,
                funding_interval_ms=28_800_000,
            ),
            "okx:btcusdt": QuoteSnapshot(
                venue="okx", symbol="BTCUSDT",
                bid=50100, ask=50200, funding_rate_bps=15.0,
                funding_timestamp_ms=1700000001000,  # 1 second apart
                funding_interval_ms=28_800_000,
            ),
        }
        candidates = build_same_symbol_pairs(q, ["BTCUSDT"])
        assert len(candidates) >= 1
        c = candidates[0]
        diff = abs(1700000000000 - 1700000001000)
        assert diff <= 60000
        assert c.interval_aligned is True
        assert c.opportunity_type == "aligned"

    def test_interval_staggered(self):
        q = {
            "binance:btcusdt": QuoteSnapshot(
                venue="binance", symbol="BTCUSDT",
                bid=50000, ask=50001, funding_rate_bps=-10.0,
                funding_timestamp_ms=1700000000000,
                funding_interval_ms=28_800_000,
            ),
            "okx:btcusdt": QuoteSnapshot(
                venue="okx", symbol="BTCUSDT",
                bid=50100, ask=50200, funding_rate_bps=15.0,
                funding_timestamp_ms=1700000100000,  # 100 seconds apart
                funding_interval_ms=28_800_000,
            ),
        }
        candidates = build_same_symbol_pairs(q, ["BTCUSDT"])
        assert len(candidates) >= 1
        c = candidates[0]
        assert c.interval_aligned is False
        assert c.opportunity_type == "staggered"

    def test_first_funding_leg_and_timestamps(self):
        q = {
            "binance:btcusdt": QuoteSnapshot(
                venue="binance", symbol="BTCUSDT",
                bid=50000, ask=50001, funding_rate_bps=5.0,
                funding_timestamp_ms=1700000001000,
                funding_interval_ms=28_800_000,
            ),
            "okx:btcusdt": QuoteSnapshot(
                venue="okx", symbol="BTCUSDT",
                bid=50100, ask=50200, funding_rate_bps=15.0,
                funding_timestamp_ms=1700000002000,
                funding_interval_ms=28_800_000,
            ),
        }
        candidates = build_same_symbol_pairs(q, ["BTCUSDT"])
        assert len(candidates) >= 1
        c = candidates[0]
        # binance has earlier timestamp → first_funding_leg = "long"
        assert c.first_funding_leg == "long"
        assert c.first_funding_timestamp_ms == 1700000001000
        assert c.second_funding_timestamp_ms == 1700000002000
        assert c.long_funding_timestamp_ms == 1700000001000
        assert c.short_funding_timestamp_ms == 1700000002000

    def test_entry_notional_quote_nonzero(self):
        q = {
            "binance:btcusdt": QuoteSnapshot(
                venue="binance", symbol="BTCUSDT",
                bid=50000, ask=50001, funding_rate_bps=5.0,
                funding_timestamp_ms=1700000001000,
                funding_interval_ms=28_800_000,
            ),
            "okx:btcusdt": QuoteSnapshot(
                venue="okx", symbol="BTCUSDT",
                bid=50100, ask=50200, funding_rate_bps=15.0,
                funding_timestamp_ms=1700000002000,
                funding_interval_ms=28_800_000,
            ),
        }
        strategy = StrategyConfig(
            entry_notional_cap_quote=40.0,
            live_entry_notional_cap_quote=30.0,
            funding_missing_margin_fallback_notional_quote=15.0,
        )
        candidates = build_same_symbol_pairs(q, ["BTCUSDT"], strategy=strategy)
        assert len(candidates) >= 1
        c = candidates[0]
        assert c.entry_notional_quote > 0.0
        assert c.entry_notional_quote == pytest.approx(15.0)
        assert c.entry_target_quantity * ((50001 + 50100) / 2.0) == pytest.approx(
            c.entry_notional_quote
        )

    def test_direction_inconsistent_when_short_mid_below_long_mid(self):
        """When short mid is below long mid, direction_consistent should be False."""
        q = {
            "binance:btcusdt": QuoteSnapshot(
                venue="binance", symbol="BTCUSDT",
                bid=60000, ask=60100, funding_rate_bps=5.0,
                funding_timestamp_ms=1700000001000,
                funding_interval_ms=28_800_000,
            ),
            "okx:btcusdt": QuoteSnapshot(
                venue="okx", symbol="BTCUSDT",
                bid=50000, ask=50100, funding_rate_bps=15.0,
                funding_timestamp_ms=1700000002000,
                funding_interval_ms=28_800_000,
            ),
        }
        candidates = build_same_symbol_pairs(q, ["BTCUSDT"])
        if candidates:
            c = candidates[0]
            assert c.direction_consistent is False


class TestSidecarSnapshotV2:
    """Snapshot must include all V2 candidate identity fields."""

    def test_schema_is_v5(self):
        s = SidecarSnapshot()
        assert s.schema_version == 5

    def test_candidate_has_v2_fields(self):
        c = CandidateInput(
            long_venue="a", short_venue="b", symbol="X",
            funding_diff_bps=10, funding_edge_bps=10,
            expected_edge_bps=5, worst_case_edge_bps=2, ranking_edge_bps=5,
            direction_consistent=True, interval_aligned=True,
        )
        assert c.direction_consistent is True
        assert c.interval_aligned is True

    def test_no_chillybot_strings_in_default_snapshot(self):
        s = SidecarSnapshot()
        # JSON round-trip should contain no "chillybot" string
        raw = json.dumps({
            "degraded_venues": s.degraded_venues,
            "source_mode": s.source_mode,
            "quotes": {},
            "candidates": [],
        })
        assert "chillybot" not in raw.lower()

    def test_source_mode_is_direct_market(self):
        """Default source_mode should be direct_market or empty (not Chillybot)."""
        s = SidecarSnapshot()
        assert "chillybot" not in s.source_mode.lower()
        assert "chillybot" not in s.acquisition_mode.lower()


class TestDegradation:
    """Partial venue failure must degrade, not clear all candidates."""

    def test_empty_degraded_venues_by_default(self):
        s = SidecarSnapshot()
        assert s.degraded_venues == []

    def test_degraded_venues_persist(self):
        s = SidecarSnapshot(degraded_venues=["gate", "bitget"])
        assert "gate" in s.degraded_venues
        assert "bitget" in s.degraded_venues
        assert s.candidates == []

    def test_quotes_kept_even_with_degradation(self):
        s = SidecarSnapshot(
            degraded_venues=["bad_venue"],
            quotes={"good:btcusdt": QuoteSnapshot(
                venue="good", symbol="BTCUSDT", bid=1, ask=2,
            )},
        )
        assert len(s.quotes) == 1
