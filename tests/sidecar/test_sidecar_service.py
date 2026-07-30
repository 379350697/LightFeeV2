"""Tests for SidecarService: concurrent fetch, degradation, no Chillybot."""

from __future__ import annotations

import asyncio
import json

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
    _load_funding_interval_evidence_cache,
    _liquidity_lifecycle_from_quotes,
    _market_failure_reasons,
    _quote_cache_contract_eligible,
    _restorable_prior_last_good_quotes,
)
from lightfee.sidecar.snapshot import (
    CandidateInput,
    LiquidityLifecycle,
    QuoteSnapshot,
    SidecarSnapshot,
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


@pytest.mark.asyncio
async def test_refresh_once_publishes_full_single_snapshot_without_candidate_cap(
    tmp_path,
    monkeypatch,
) -> None:
    service = object.__new__(SidecarService)
    service.config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[VenueConfig(venue="binance")],
    )
    service.snapshot_path = tmp_path / "sidecar.json"
    service.config.runtime.sidecar_snapshot_path = str(service.snapshot_path)
    service._funding_timeout_s = 1.0
    service._liquidity_timeout_s = 1.0
    service._last_good_quotes = {}
    service._last_good_at_ms = 0
    service._last_good_at_ms_by_key = {}
    service._last_liquidity_publish_at_ms = 0
    service._last_liquidity_publish_at_ms_by_key = {}

    class RecordingBuilder:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def build(self, *_args, **kwargs):
            self.calls.append(dict(kwargs))
            return []

    builder = RecordingBuilder()
    service._new_candidate_service = lambda *, now_ms=None: builder

    async def no_funding(symbols, timeout_s, **_kwargs):
        return []

    async def no_liquidity(
        symbols, timeout_s, quote_liquidity_by_venue=None, skip_venues=None,
    ):
        return []

    service._fetch_all_venues = no_funding
    service._fetch_liquidity_all_venues = no_liquidity
    monkeypatch.setattr("lightfee.sidecar.service.time.time", lambda: 1.0)
    published: list[SidecarSnapshot] = []
    monkeypatch.setattr(
        "lightfee.sidecar.service.publish_snapshot",
        lambda snapshot, _path: published.append(snapshot),
    )

    await service.refresh_once()

    assert builder.calls
    assert "max_candidates" not in builder.calls[0]
    assert published and published[0].candidates == []


def test_funding_sidecar_exposes_only_the_single_snapshot_data_plane(tmp_path) -> None:
    config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[VenueConfig(venue="binance")],
    )
    config.runtime.sidecar_snapshot_path = str(tmp_path / "sidecar.json")

    service = SidecarService(config)

    assert not hasattr(service, "refresh_in_process_entry")
    assert not hasattr(service, "in_process_entry")
    assert not hasattr(service, "embedded_spread_bbo_enabled")


def test_funding_interval_cache_round_trip_is_venue_and_symbol_scoped(tmp_path) -> None:
    cache_path = tmp_path / "sidecar.json.funding-intervals.v1.json"
    cache_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "venues": {
                    "binance": {
                        "btcusdt": {
                            "interval_ms": 28_800_000,
                            "source": "venue_funding_history",
                            "observed_at_ms": 1_000,
                        },
                        "bad": {
                            "interval_ms": 0,
                            "source": "bad",
                            "observed_at_ms": 1_000,
                        },
                    },
                    "unknown": {
                        "BTCUSDT": {
                            "interval_ms": 28_800_000,
                            "source": "ignored",
                            "observed_at_ms": 1_000,
                        }
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    restored = _load_funding_interval_evidence_cache(
        cache_path,
        configured_venues={"binance"},
    )

    assert restored == {
        "binance": {
            "binance:BTCUSDT": (28_800_000, "venue_funding_history", 1_000)
        }
    }


def test_funding_interval_cache_persist_is_non_snapshot_sidecar_state(tmp_path) -> None:
    class Source:
        def funding_interval_evidence(self):
            return {
                "binance:BTCUSDT": (
                    28_800_000,
                    "venue_funding_history",
                    1_000,
                )
            }

    service = object.__new__(SidecarService)
    service._funding_interval_cache_path = tmp_path / "intervals.json"
    service._exchange_sources = {"binance": Source()}

    service._persist_funding_interval_evidence()

    payload = json.loads(service._funding_interval_cache_path.read_text())
    assert payload["schema_version"] == 1
    assert payload["venues"]["binance"]["BTCUSDT"] == {
        "interval_ms": 28_800_000,
        "source": "venue_funding_history",
        "observed_at_ms": 1_000,
    }


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


def test_candidate_service_uses_configured_venue_fees_without_online_evidence() -> None:
    service = object.__new__(SidecarService)
    service.config = AppConfig(
        symbols=["BTCUSDT"],
        venues=[
            VenueConfig(venue="cheap", taker_fee_bps=1.0, maker_fee_bps=0.1),
            VenueConfig(venue="rich", taker_fee_bps=2.0, maker_fee_bps=0.2),
        ],
    )

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


def test_slow_refresh_reuses_cached_venue_without_delaying_fresh_venues() -> None:
    class FastSource:
        async def fetch_all(self, symbols: list[str]) -> dict[str, QuoteSnapshot]:
            return {
                "fast:BTCUSDT": QuoteSnapshot(
                    venue="fast",
                    symbol="BTCUSDT",
                    bid=100.0,
                    ask=101.0,
                    observed_at_ms=2_000,
                    funding_rate_bps=1.0,
                    funding_timestamp_ms=3_000,
                    funding_interval_ms=28_800_000,
                )
            }

    class SlowSource:
        async def fetch_all(self, symbols: list[str]) -> dict[str, QuoteSnapshot]:
            await asyncio.Event().wait()
            return {}

    cached = QuoteSnapshot(
        venue="slow",
        symbol="BTCUSDT",
        bid=99.0,
        ask=100.0,
        observed_at_ms=1_000,
        funding_rate_bps=1.0,
        funding_timestamp_ms=2_000,
        funding_interval_ms=28_800_000,
    )
    service = object.__new__(SidecarService)
    service.config = AppConfig(
        venues=[VenueConfig(venue="fast"), VenueConfig(venue="slow")]
    )
    service._exchange_sources = {"fast": FastSource(), "slow": SlowSource()}
    service._entry_venue_fetch_tasks = {}
    service._entry_venue_latest_results = {
        "slow": ("slow", {"slow:BTCUSDT": cached}, None, set())
    }
    service._entry_venue_late_tasks = set()
    service.entry_venue_republish_event = asyncio.Event()

    results = asyncio.run(
        asyncio.wait_for(
            service._fetch_all_venues(["BTCUSDT"], timeout_s=0.05),
            timeout=0.2,
        )
    )

    by_venue = {venue: (quotes, error) for venue, quotes, error, _ in results}
    assert by_venue["fast"][0]["fast:BTCUSDT"].observed_at_ms == 2_000
    assert by_venue["slow"] == ({"slow:BTCUSDT": cached}, None)


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
    # A legacy snapshot has no cadence-proof source or source timestamp.  It
    # must not become fresh interval evidence merely because the process
    # restarted; only the dedicated durable evidence cache may do that.
    assert service._exchange_sources["binance"].funding_interval_evidence(
        now_ms=1_500
    ) == {}


def test_sidecar_has_no_post_publish_frontier_oracle() -> None:
    """Candidate admission must depend on each candidate, never a global oracle."""
    assert not hasattr(SidecarService, "_schedule_entry_frontier_oracle")
    assert not hasattr(SidecarService, "_verify_entry_frontier_oracle")


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

    service._fetch_all_venues = funding_results
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
        venue_fee_bps={"binance": 1.0, "okx": 1.0},
    )

    assert len(candidates) == 1
    assert diagnostics["requested_symbol_count"] == 1
    assert diagnostics["output_candidate_count"] == 1
    assert diagnostics["directional_pair_count"] == 2


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
        candidates = build_same_symbol_pairs(
            q, ["BTCUSDT"], strategy=strategy,
            venue_fee_bps={"binance": 1.0, "okx": 1.0},
        )
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
        candidates = build_same_symbol_pairs(
            q, ["BTCUSDT"], strategy=strategy,
            venue_fee_bps={"binance": 1.0, "okx": 1.0},
        )
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
        candidates = build_same_symbol_pairs(
            q, ["BTCUSDT"], venue_fee_bps={"binance": 1.0, "okx": 1.0},
        )
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
        candidates = build_same_symbol_pairs(
            q, ["BTCUSDT"], venue_fee_bps={"binance": 1.0, "okx": 1.0},
        )
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
        candidates = build_same_symbol_pairs(
            q, ["BTCUSDT"], venue_fee_bps={"binance": 1.0, "okx": 1.0},
        )
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
        candidates = build_same_symbol_pairs(
            q, ["BTCUSDT"], strategy=strategy,
            venue_fee_bps={"binance": 1.0, "okx": 1.0},
        )
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
