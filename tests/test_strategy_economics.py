from __future__ import annotations

from dataclasses import replace
from itertools import combinations
import math
import random
import time
from types import SimpleNamespace

import pytest

from lightfee.config.schema import StrategyConfig
from lightfee.core.domain import EntryLeverageEvidence, Venue
from lightfee.engine.entry_dispatch_runtime import EntryDispatchRuntime
from lightfee.engine.entry_gate_runtime import EntryGateRuntime
from lightfee.engine.entry_readiness import QuoteLease
from lightfee.sidecar.pairing import build_same_symbol_pairs
from lightfee.sidecar.publisher import load_snapshot, publish_snapshot
from lightfee.sidecar.snapshot import (
    CandidateInput,
    FundingLifecycle,
    LiquidityLifecycle,
    MarketLifecycle,
    QuoteSnapshot,
    SidecarSnapshot,
)
from lightfee.strategy.economics import (
    FundingForecast,
    build_edge_breakdown,
    conservative_funding_edge_bps,
)
from lightfee.strategy.funding_entry_revalidator import FundingEntryRevalidator
from lightfee.strategy.funding_forecast_calibrator import FundingForecastCalibrator
from lightfee.strategy.risk_allocator import StrategyRiskAllocator


def _candidate(**overrides: object) -> CandidateInput:
    values: dict[str, object] = {
        "long_venue": "cheap",
        "short_venue": "rich",
        "symbol": "BTCUSDT",
        "funding_diff_bps": 12.0,
        "funding_edge_bps": 12.0,
        "expected_edge_bps": 5.0,
        "worst_case_edge_bps": 3.0,
        "ranking_edge_bps": 3.0,
        "entry_notional_quote": 30.0,
        "first_funding_timestamp_ms": 1_000_000,
        "second_funding_timestamp_ms": 1_000_000,
        "entry_fee_bps": 2.0,
        "exit_fee_bps": 2.0,
        "adverse_selection_bps": 1.0,
        "capital_buffer_bps": 1.0,
        "execution_buffer_bps": 2.0,
        "economics_complete": True,
        "calculation_version": "v1_exact",
        "forecast_worst_funding_edge_bps": 8.0,
    }
    values.update(overrides)
    return CandidateInput(**values)


def test_full_pairing_reaches_real_opportunity_after_high_surface_blockers() -> None:
    quotes: dict[str, QuoteSnapshot] = {}
    symbols = [f"BAD{index}USDT" for index in range(300)] + ["SPECIALUSDT"]

    def quote(
        venue: str,
        symbol: str,
        *,
        bid: float,
        ask: float,
        funding_rate_bps: float,
    ) -> QuoteSnapshot:
        return QuoteSnapshot(
            venue=venue,
            symbol=symbol,
            bid=bid,
            ask=ask,
            bid_size=10.0,
            ask_size=10.0,
            funding_rate_bps=funding_rate_bps,
            funding_rate_observed_at_ms=1_000,
            funding_rate_event_at_ms=1_000,
            funding_rate_received_at_ms=1_000,
            funding_rate_source="test_fixture",
            funding_rate_sample_id=(
                f"funding:{venue}:{symbol}:1000:{funding_rate_bps:.17g}:100000"
            ),
            funding_timestamp_ms=100_000,
            funding_interval_ms=28_800_000,
            observed_at_ms=1_000,
            underlying=symbol.removesuffix("USDT"),
            quote_currency="USDT",
            contract_type="linear",
            contract_multiplier=1.0,
            mark_index_source="venue_mark_index",
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

    for symbol in symbols[:-1]:
        quotes[f"cheap:{symbol}"] = quote(
            "cheap", symbol, bid=129.99, ask=130.0, funding_rate_bps=0.0
        )
        quotes[f"rich:{symbol}"] = quote(
            "rich", symbol, bid=100.0, ask=100.01, funding_rate_bps=100.0
        )
    quotes["cheap:SPECIALUSDT"] = quote(
        "cheap", "SPECIALUSDT", bid=99.99, ask=100.0, funding_rate_bps=0.0
    )
    quotes["rich:SPECIALUSDT"] = quote(
        "rich", "SPECIALUSDT", bid=100.2, ask=100.21, funding_rate_bps=20.0
    )
    kwargs = {
        "strategy": StrategyConfig(
            min_scan_minutes_before_funding=0,
            max_scan_minutes_before_funding=0,
            entry_window_secs=0,
            min_expected_edge_bps=-100.0,
            min_worst_case_edge_bps=-100.0,
        ),
        "venue_fee_bps": {"cheap": 1.0, "rich": 1.0},
        "venue_maker_fee_bps": {"cheap": 1.0, "rich": 1.0},
        "observed_at_ms": 1_000,
    }

    diagnostics: dict[str, object] = {}
    candidates = build_same_symbol_pairs(
        quotes,
        symbols,
        diagnostics=diagnostics,
        **kwargs,
    )
    eligible = [
        row for row in candidates if not row.blocked and row.economics_complete
    ]

    assert [row.symbol for row in eligible] == ["SPECIALUSDT"]
    assert diagnostics["directional_pair_count"] == len(symbols) * 2
    by_pair_id = {row.pair_id: row for row in candidates}
    assert by_pair_id["specialusdt:cheap->rich"].blocked is False
    assert by_pair_id["bad0usdt:cheap->rich"].blocked_reasons == [
        "expected_edge_below_floor",
        "worst_case_edge_below_floor",
    ]


def test_full_pairing_decides_seven_venue_universe_under_500ms() -> None:
    quotes: dict[str, QuoteSnapshot] = {}
    venues = [f"venue{index}" for index in range(7)]
    symbols = [f"S{index}USDT" for index in range(216)]
    for symbol_index, symbol in enumerate(symbols):
        reference = 100.0 + symbol_index / 100.0
        for venue_index, venue in enumerate(venues):
            quotes[f"{venue}:{symbol}"] = QuoteSnapshot(
                venue=venue,
                symbol=symbol,
                bid=reference - 0.01,
                ask=reference + 0.01,
                bid_size=10.0,
                ask_size=10.0,
                funding_rate_bps=float(venue_index),
                funding_rate_observed_at_ms=1_000,
                funding_rate_event_at_ms=1_000,
                funding_rate_received_at_ms=1_000,
                funding_rate_source="test_fixture",
                funding_rate_sample_id=(
                    f"funding:{venue}:{symbol}:1000:{float(venue_index):.17g}:100000"
                ),
                funding_timestamp_ms=100_000,
                funding_interval_ms=28_800_000,
                observed_at_ms=1_000,
                underlying=symbol.removesuffix("USDT"),
                quote_currency="USDT",
                contract_type="linear",
                contract_multiplier=1.0,
                mark_index_source="venue_mark_index",
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
    diagnostics: dict[str, object] = {}
    started = time.perf_counter()
    candidates = build_same_symbol_pairs(
        quotes,
        symbols,
        strategy=StrategyConfig(
            min_scan_minutes_before_funding=0,
            max_scan_minutes_before_funding=0,
            entry_window_secs=0,
            min_expected_edge_bps=-100.0,
            min_worst_case_edge_bps=-100.0,
        ),
        venue_fee_bps={venue: 1.0 for venue in venues},
        venue_maker_fee_bps={venue: 1.0 for venue in venues},
        observed_at_ms=1_000,
        diagnostics=diagnostics,
    )
    elapsed_s = time.perf_counter() - started

    assert len(candidates) == len(symbols)
    assert all(not row.blocked and row.economics_complete for row in candidates)
    assert diagnostics["directional_pair_count"] == len(symbols) * 7 * 6
    assert elapsed_s <= 0.5


def test_full_frontier_uses_configured_fees_without_online_evidence() -> None:
    quotes: dict[str, QuoteSnapshot] = {}
    venues = [f"venue{index}" for index in range(7)]
    symbols = [f"C{index}USDT" for index in range(96)]
    for symbol_index, symbol in enumerate(symbols):
        reference = 100.0 + symbol_index / 100.0
        for venue_index, venue in enumerate(venues):
            rate_bps = 0.0 if venue_index == 0 else float(24 + venue_index)
            quotes[f"{venue}:{symbol}"] = QuoteSnapshot(
                venue=venue,
                symbol=symbol,
                bid=reference - 0.01,
                ask=reference + 0.01,
                bid_size=10.0,
                ask_size=10.0,
                funding_rate_bps=rate_bps,
                funding_rate_observed_at_ms=1_000,
                funding_rate_event_at_ms=1_000,
                funding_rate_received_at_ms=1_000,
                funding_rate_source="test_fixture",
                funding_rate_sample_id=(
                    f"funding:{venue}:{symbol}:1000:{rate_bps:.17g}:100000"
                ),
                funding_timestamp_ms=100_000,
                funding_interval_ms=28_800_000,
                observed_at_ms=1_000,
                underlying=symbol.removesuffix("USDT"),
                quote_currency="USDT",
                contract_type="linear",
                contract_multiplier=1.0,
                mark_index_source="venue_mark_index",
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
    kwargs = {
        "strategy": StrategyConfig(
            min_funding_edge_bps=30.0,
            min_expected_edge_bps=-100.0,
            min_worst_case_edge_bps=-100.0,
            min_scan_minutes_before_funding=0,
            max_scan_minutes_before_funding=0,
            entry_window_secs=0,
        ),
        "venue_fee_bps": {venue: 1.0 for venue in venues},
        "venue_maker_fee_bps": {venue: 0.5 for venue in venues},
        "passive_execution_enabled": True,
        "observed_at_ms": 1_000,
    }

    diagnostics: dict[str, object] = {}
    started = time.perf_counter()
    candidates = build_same_symbol_pairs(
        quotes,
        symbols,
        diagnostics=diagnostics,
        **kwargs,
    )
    elapsed_s = time.perf_counter() - started

    assert len(candidates) == len(symbols)
    assert all(not row.blocked and row.economics_complete for row in candidates)
    assert all(row.long_taker_fee_bps == 1.0 for row in candidates)
    assert all(row.short_taker_fee_bps == 1.0 for row in candidates)
    assert diagnostics["directional_pair_count"] == len(symbols) * 7 * 6
    assert elapsed_s <= 0.5


def test_full_frontier_excludes_final_quantity_below_pair_minimum() -> None:
    def quote(
        *,
        venue: str,
        symbol: str,
        funding_rate_bps: float,
        depth_quantity: float,
        min_quantity_base: float,
        min_notional_quote: float,
    ) -> QuoteSnapshot:
        return QuoteSnapshot(
            venue=venue,
            symbol=symbol,
            bid=0.999,
            ask=1.001,
            bid_size=depth_quantity,
            ask_size=depth_quantity,
            funding_rate_bps=funding_rate_bps,
            funding_rate_observed_at_ms=1_000,
            funding_rate_event_at_ms=1_000,
            funding_rate_received_at_ms=1_000,
            funding_rate_source="test_fixture",
            funding_rate_sample_id=(
                f"funding:{venue}:{symbol}:1000:{funding_rate_bps:.17g}:100000"
            ),
            funding_timestamp_ms=100_000,
            funding_interval_ms=28_800_000,
            observed_at_ms=1_000,
            underlying=symbol.removesuffix("USDT"),
            quote_currency="USDT",
            contract_type="linear",
            contract_multiplier=1.0,
            mark_index_source="venue_mark_index",
            price_precision=3,
            quantity_precision=3,
            price_tick=0.001,
            quantity_step_base=0.001,
            min_quantity_base=min_quantity_base,
            min_notional_quote=min_notional_quote,
            min_notional_evidence_complete=True,
            venue_status="active",
            contract_normalization_complete=True,
        )

    quotes = {
        "long:THINUSDT": quote(
            venue="long",
            symbol="THINUSDT",
            funding_rate_bps=0.0,
            depth_quantity=1.0,
            min_quantity_base=10.0,
            min_notional_quote=5.0,
        ),
        "short:THINUSDT": quote(
            venue="short",
            symbol="THINUSDT",
            funding_rate_bps=20.0,
            depth_quantity=1.0,
            min_quantity_base=10.0,
            min_notional_quote=5.0,
        ),
        "long:GOODUSDT": quote(
            venue="long",
            symbol="GOODUSDT",
            funding_rate_bps=0.0,
            depth_quantity=100.0,
            min_quantity_base=1.0,
            min_notional_quote=1.0,
        ),
        "short:GOODUSDT": quote(
            venue="short",
            symbol="GOODUSDT",
            funding_rate_bps=10.0,
            depth_quantity=100.0,
            min_quantity_base=1.0,
            min_notional_quote=1.0,
        ),
    }
    kwargs = {
        "strategy": StrategyConfig(
            min_expected_edge_bps=-100.0,
            min_worst_case_edge_bps=-100.0,
            min_scan_minutes_before_funding=0,
            max_scan_minutes_before_funding=0,
            entry_window_secs=0,
        ),
        "venue_fee_bps": {"long": 1.0, "short": 1.0},
        "observed_at_ms": 1_000,
    }

    full = build_same_symbol_pairs(
        quotes,
        ["THINUSDT", "GOODUSDT"],
        **kwargs,
    )
    thin = next(row for row in full if row.symbol == "THINUSDT")
    diagnostics: dict[str, object] = {}
    complete = build_same_symbol_pairs(
        quotes,
        ["THINUSDT", "GOODUSDT"],
        diagnostics=diagnostics,
        **kwargs,
    )
    complete_viable = [
        row for row in complete if not row.blocked and row.economics_complete
    ]

    assert thin.blocked is True
    assert thin.economics_complete is False
    assert "entry_pair_minimum_not_met" in thin.blocked_reasons
    assert [row.symbol for row in complete_viable] == ["GOODUSDT"]
    assert diagnostics["directional_pair_count"] == 4


def test_full_pairing_never_stops_at_an_exact_work_budget() -> None:
    venues = [f"venue{index}" for index in range(7)]
    symbols = [f"B{index}USDT" for index in range(8)]
    quotes = {
        f"{venue}:{symbol}": QuoteSnapshot(
            venue=venue,
            symbol=symbol,
            bid=99.99,
            ask=100.01,
            bid_size=10.0,
            ask_size=10.0,
            funding_rate_bps=float(venue_index),
            funding_rate_observed_at_ms=1_000,
            funding_rate_event_at_ms=1_000,
            funding_rate_received_at_ms=1_000,
            funding_rate_source="test_fixture",
            funding_rate_sample_id=(
                f"funding:{venue}:{symbol}:1000:{float(venue_index):.17g}:100000"
            ),
            funding_timestamp_ms=100_000,
            funding_interval_ms=28_800_000,
            observed_at_ms=1_000,
            underlying=symbol.removesuffix("USDT"),
            quote_currency="USDT",
            contract_type="linear",
            contract_multiplier=1.0,
            mark_index_source="venue_mark_index",
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
        for symbol in symbols
        for venue_index, venue in enumerate(venues)
    }
    diagnostics: dict[str, object] = {}

    candidates = build_same_symbol_pairs(
        quotes,
        symbols,
        strategy=StrategyConfig(
            min_scan_minutes_before_funding=0,
            max_scan_minutes_before_funding=0,
            entry_window_secs=0,
            min_expected_edge_bps=-100.0,
            min_worst_case_edge_bps=-100.0,
        ),
        venue_fee_bps={venue: 1.0 for venue in venues},
        venue_maker_fee_bps={venue: 1.0 for venue in venues},
        observed_at_ms=1_000,
        diagnostics=diagnostics,
    )

    assert len(candidates) == len(symbols)
    assert all(not row.blocked and row.economics_complete for row in candidates)
    assert diagnostics["directional_pair_count"] == len(symbols) * 7 * 6


def test_enhanced_live_decides_every_direction_without_candidate_cap() -> None:
    venues = [f"venue{index}" for index in range(7)]
    symbols = [f"E{index}USDT" for index in range(80)]
    quotes: dict[str, QuoteSnapshot] = {}
    for symbol_index, symbol in enumerate(symbols):
        reference = 100.0 + symbol_index / 100.0
        for venue in venues:
            quotes[f"{venue}:{symbol}"] = QuoteSnapshot(
                venue=venue,
                symbol=symbol,
                bid=reference - 0.01,
                ask=reference + 0.01,
                bid_size=10.0,
                ask_size=10.0,
                funding_rate_bps=100.0,
                funding_rate_observed_at_ms=10_000,
                funding_rate_event_at_ms=10_000,
                funding_rate_received_at_ms=10_000,
                funding_rate_source="test_fixture",
                funding_rate_sample_id=(
                    f"funding:{venue}:{symbol}:10000:100:100000"
                ),
                funding_timestamp_ms=100_000,
                funding_interval_ms=28_800_000,
                predicted_funding_rate_bps=100.0,
                funding_forecast_source="test_calibrator",
                funding_forecast_sample_count=32,
                funding_forecast_uncertainty_bps=1.0,
                funding_forecast_started_at_ms=1,
                funding_forecast_distribution_stable=True,
                funding_forecast_stability_reason="stable",
                observed_at_ms=10_000,
                underlying=symbol.removesuffix("USDT"),
                quote_currency="USDT",
                contract_type="linear",
                contract_multiplier=1.0,
                mark_index_source="venue_mark_index",
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
    diagnostics: dict[str, object] = {}

    candidates = build_same_symbol_pairs(
        quotes,
        symbols,
        strategy=StrategyConfig(
            funding_economics_mode="enhanced_live",
            funding_forecast_mode="live",
            entry_notional_cap_quote=200.0,
            funding_forecast_min_samples=3,
            funding_forecast_shadow_min_days=0,
        ),
        venue_fee_bps={venue: 1.0 for venue in venues},
        venue_maker_fee_bps={venue: 1.0 for venue in venues},
        observed_at_ms=10_000,
        diagnostics=diagnostics,
    )

    assert len(candidates) == len(symbols) * 7 * 6
    assert all(row.blocked for row in candidates)
    assert diagnostics["directional_pair_count"] == len(symbols) * 42


@pytest.mark.parametrize(
    ("seed", "economics_mode", "passive_execution_enabled"),
    [
        (7, "v1_exact", False),
        (19, "v1_exact", True),
        (37, "enhanced_shadow", False),
        (53, "enhanced_live", False),
    ],
)
def test_full_pairing_matches_randomized_pairwise_oracle(
    seed: int,
    economics_mode: str,
    passive_execution_enabled: bool,
) -> None:
    rng = random.Random(seed)
    venues = [f"venue{index}" for index in range(7)]
    symbols = [f"R{index}USDT" for index in range(24)]
    quotes: dict[str, QuoteSnapshot] = {}
    for symbol_index, symbol in enumerate(symbols):
        reference = 50.0 + symbol_index * 3.0
        for venue_index, venue in enumerate(venues):
            venue_mid = reference * (1.0 + rng.uniform(-0.008, 0.008))
            half_spread = venue_mid * rng.uniform(0.00002, 0.0005)
            rate_bps = rng.uniform(-35.0, 35.0) + venue_index * 0.001
            funding_timestamp_ms = 100_000 + rng.choice((0, 30_000, 180_000))
            quotes[f"{venue}:{symbol}"] = QuoteSnapshot(
                venue=venue,
                symbol=symbol,
                bid=venue_mid - half_spread,
                ask=venue_mid + half_spread,
                bid_size=rng.uniform(0.05, 20.0),
                ask_size=rng.uniform(0.05, 20.0),
                funding_rate_bps=rate_bps,
                funding_rate_observed_at_ms=10_000,
                funding_rate_event_at_ms=10_000,
                funding_rate_received_at_ms=10_000,
                funding_rate_source="test_fixture",
                funding_rate_sample_id=(
                    f"funding:{venue}:{symbol}:10000:{rate_bps:.17g}:"
                    f"{funding_timestamp_ms}"
                ),
                funding_timestamp_ms=funding_timestamp_ms,
                funding_interval_ms=28_800_000,
                predicted_funding_rate_bps=(
                    rate_bps + ((symbol_index + venue_index) % 7 - 3) * 0.25
                ),
                funding_forecast_source="test_calibrator",
                funding_forecast_sample_count=32,
                funding_forecast_uncertainty_bps=1.0,
                funding_forecast_started_at_ms=1,
                funding_forecast_distribution_stable=True,
                funding_forecast_stability_reason="stable",
                observed_at_ms=10_000,
                underlying=symbol.removesuffix("USDT"),
                quote_currency="USDT",
                contract_type="linear",
                contract_multiplier=1.0,
                mark_index_source="venue_mark_index",
                price_precision=4,
                quantity_precision=6,
                price_tick=0.0001,
                quantity_step_base=0.000001,
                min_quantity_base=0.000001,
                min_notional_quote=0.01,
                min_notional_evidence_complete=True,
                venue_status="active",
                contract_normalization_complete=True,
            )
    fees = {venue: rng.uniform(0.5, 4.0) for venue in venues}
    maker_fees = {venue: rng.uniform(0.0, fees[venue]) for venue in venues}
    caps = {venue: rng.uniform(25.0, 250.0) for venue in venues}
    kwargs = {
        "strategy": StrategyConfig(
            funding_economics_mode=economics_mode,
            entry_notional_cap_quote=200.0,
            funding_forecast_min_samples=3,
            funding_forecast_shadow_min_days=0,
        ),
        "venue_fee_bps": fees,
        "venue_maker_fee_bps": maker_fees,
        "venue_notional_caps": caps,
        "passive_execution_enabled": passive_execution_enabled,
        "observed_at_ms": 10_000,
    }

    diagnostics: dict[str, object] = {}
    full = build_same_symbol_pairs(
        quotes,
        symbols,
        diagnostics=diagnostics,
        **kwargs,
    )
    full_eligible = [
        row for row in full if not row.blocked and row.economics_complete
    ]
    oracle_rows: list[CandidateInput] = []
    for symbol in symbols:
        symbol_quotes = [quotes[f"{venue}:{symbol}"] for venue in venues]
        for left, right in combinations(symbol_quotes, 2):
            oracle_rows.extend(
                build_same_symbol_pairs(
                    {
                        f"{left.venue}:{symbol}": left,
                        f"{right.venue}:{symbol}": right,
                    },
                    [symbol],
                    **kwargs,
                )
            )
    oracle_eligible = sorted(
        (
            row
            for row in oracle_rows
            if not row.blocked and row.economics_complete
        ),
        key=lambda row: (-row.ranking_edge_bps, row.pair_id),
    )

    assert [row.pair_id for row in full_eligible] == [
        row.pair_id for row in oracle_eligible
    ]
    assert [row.ranking_edge_bps for row in full_eligible] == pytest.approx(
        [row.ranking_edge_bps for row in oracle_eligible]
    )
    assert diagnostics["directional_pair_count"] == len(symbols) * 7 * 6


def test_edge_breakdown_has_one_expected_and_worst_formula() -> None:
    edge = build_edge_breakdown(
        gross_signal_edge_bps=3.0,
        funding_edge_bps=10.0,
        entry_cross_bps=-2.0,
        expected_exit_cross_bps=1.0,
        entry_fee_bps=1.0,
        exit_fee_bps=1.0,
        entry_slippage_bps=1.0,
        exit_slippage_bps=1.0,
        adverse_selection_bps=1.0,
        capital_buffer_bps=1.0,
        execution_buffer_bps=2.0,
        venue_risk_haircut_bps=1.0,
        observed_at_ms=1,
    )

    assert edge.expected_net_edge_bps == pytest.approx(5.0)
    assert edge.worst_case_edge_bps == pytest.approx(3.0)
    assert edge.ranking_edge_bps == edge.worst_case_edge_bps
    assert edge.model_epoch == "v1_exact"
    assert edge.candidate_fields()["model_epoch"] == "v1_exact"


def test_edge_breakdown_keeps_forecast_worst_funding_in_the_same_pure_contract() -> None:
    edge = build_edge_breakdown(
        funding_edge_bps=8.0,
        worst_case_funding_edge_bps=2.0,
        entry_cross_bps=-1.0,
        entry_fee_bps=1.0,
        execution_buffer_bps=1.0,
        observed_at_ms=1,
        economics_complete=True,
    )

    assert edge.expected_net_edge_bps == pytest.approx(6.0)
    assert edge.worst_case_edge_bps == pytest.approx(-1.0)
    assert edge.ranking_edge_bps == edge.worst_case_edge_bps


def test_live_funding_selection_uses_v1_depth_risk_priority_before_ranking_tiebreak() -> None:
    gate = object.__new__(EntryGateRuntime)
    risk_by_pair = {"higher": 2.0, "lower": 0.0}
    gate._runtime_candidate_risk_score = (  # type: ignore[method-assign]
        lambda candidate, *_args, **_kwargs: risk_by_pair[candidate.pair_id]
    )
    gate._candidate_pair_id = lambda candidate: candidate.pair_id  # type: ignore[method-assign]
    higher_edge = SimpleNamespace(
        ranking_edge_bps=8.0,
        worst_case_edge_bps=8.0,
        expected_profit_quote=100.0,
        pair_id="higher",
    )
    lower_edge = SimpleNamespace(
        ranking_edge_bps=5.0,
        worst_case_edge_bps=5.0,
        expected_profit_quote=1.0,
        pair_id="lower",
    )

    # V1 orders execution-risk-adjusted priority before it deduplicates a
    # symbol; raw ranking remains the deterministic tie-break only.
    assert gate._candidate_final_selection_sort_key(
        lower_edge
    ) < gate._candidate_final_selection_sort_key(higher_edge)

    tied_higher = SimpleNamespace(
        ranking_edge_bps=12.0,
        worst_case_edge_bps=8.0,
        expected_profit_quote=1.0,
        pair_id="z-higher",
    )
    tied_lower = SimpleNamespace(
        ranking_edge_bps=6.0,
        worst_case_edge_bps=6.0,
        expected_profit_quote=100.0,
        pair_id="a-lower",
    )
    risk_by_pair.update({"z-higher": 1.0, "a-lower": 0.0})

    # Absolute quote profit deliberately conflicts in both comparisons.  V1
    # still orders by ranking_edge_bps / (1 + risk), then raw ranking.
    assert gate._candidate_final_selection_sort_key(
        tied_higher
    ) < gate._candidate_final_selection_sort_key(tied_lower)


@pytest.mark.parametrize("value", ["true", "false", 1, 0, object()])
def test_edge_breakdown_requires_literal_boolean_true_for_economics_complete(
    value: object,
) -> None:
    edge = build_edge_breakdown(
        funding_edge_bps=8.0,
        economics_complete=value,  # type: ignore[arg-type]
    )

    assert edge.economics_complete is False


def test_edge_breakdown_rejects_boolean_numeric_components() -> None:
    edge = build_edge_breakdown(
        funding_edge_bps=True,  # type: ignore[arg-type]
        observed_at_ms=1,
        economics_complete=True,
    )

    assert edge.funding_edge_bps == 0.0
    assert edge.expected_net_edge_bps == 0.0
    assert edge.economics_complete is False


@pytest.mark.parametrize(
    "field_name",
    [
        "entry_slippage_bps",
        "exit_slippage_bps",
        "adverse_selection_bps",
        "capital_buffer_bps",
        "execution_buffer_bps",
        "venue_risk_haircut_bps",
    ],
)
def test_negative_nonfee_cost_never_becomes_edge_or_live_permission(
    field_name: str,
) -> None:
    edge = build_edge_breakdown(
        **{field_name: -50.0},
        observed_at_ms=1,
        economics_complete=True,
    )

    assert getattr(edge, field_name) == 0.0
    assert edge.expected_net_edge_bps == 0.0
    assert edge.worst_case_edge_bps == 0.0
    assert edge.economics_complete is False


@pytest.mark.parametrize("observed_at_ms", [0, True])
def test_edge_breakdown_requires_positive_observation_time_for_complete_economics(
    observed_at_ms: object,
) -> None:
    edge = build_edge_breakdown(
        funding_edge_bps=8.0,
        observed_at_ms=observed_at_ms,  # type: ignore[arg-type]
        economics_complete=True,
    )

    assert edge.economics_complete is False
    assert edge.observed_at_ms == 0


def test_non_finite_economics_fail_closed_before_live_revalidation() -> None:
    candidate = _candidate(funding_edge_bps=float("nan"))
    result = FundingEntryRevalidator().revalidate_before_first_leg(
        candidate,
        long_ask=100.0,
        short_bid=100.0,
        now_ms=1,
        config=StrategyConfig(min_expected_edge_bps=1.0, min_worst_case_edge_bps=1.0),
    )

    assert result.allowed is False
    assert result.reason == "incomplete_economics"
    assert result.edge.economics_complete is False
    assert math.isfinite(result.edge.expected_net_edge_bps)
    assert math.isfinite(result.edge.worst_case_edge_bps)


def test_non_finite_sidecar_quote_is_excluded_before_funding_candidate_ranking() -> None:
    quotes = {
        "cheap:BTCUSDT": QuoteSnapshot(
            venue="cheap",
            symbol="BTCUSDT",
            bid=99.9,
            ask=100.0,
            observed_at_ms=10,
            funding_rate_bps=2.0,
            funding_timestamp_ms=1_000_000,
            funding_interval_ms=28_800_000,
        ),
        "rich:BTCUSDT": QuoteSnapshot(
            venue="rich",
            symbol="BTCUSDT",
            bid=float("nan"),
            ask=100.4,
            funding_rate_bps=8.0,
            funding_timestamp_ms=1_000_000,
            funding_interval_ms=28_800_000,
        ),
    }

    assert build_same_symbol_pairs(quotes, ["BTCUSDT"], observed_at_ms=1) == []


def test_crossed_sidecar_quote_is_excluded_before_funding_candidate_ranking() -> None:
    quotes = {
        "cheap:BTCUSDT": QuoteSnapshot(
            venue="cheap",
            symbol="BTCUSDT",
            bid=100.1,
            ask=100.0,
            funding_rate_bps=2.0,
            funding_timestamp_ms=1_000_000,
            funding_interval_ms=28_800_000,
        ),
        "rich:BTCUSDT": QuoteSnapshot(
            venue="rich",
            symbol="BTCUSDT",
            bid=100.3,
            ask=100.4,
            observed_at_ms=10,
            funding_rate_bps=8.0,
            funding_timestamp_ms=1_000_000,
            funding_interval_ms=28_800_000,
        ),
    }

    assert build_same_symbol_pairs(quotes, ["BTCUSDT"], observed_at_ms=1) == []


@pytest.mark.parametrize("invalid_rate", [None, "bad", float("nan"), float("inf"), True])
def test_invalid_funding_scalar_rejects_pairs_without_aborting_refresh(
    invalid_rate: object,
) -> None:
    quotes = {
        "cheap:BTCUSDT": QuoteSnapshot(
            venue="cheap",
            symbol="BTCUSDT",
            bid=99.9,
            ask=100.0,
            funding_rate_bps=invalid_rate,
        ),
        "rich:BTCUSDT": QuoteSnapshot(
            venue="rich",
            symbol="BTCUSDT",
            bid=100.3,
            ask=100.4,
            funding_rate_bps=8.0,
        ),
    }
    diagnostics: dict[str, object] = {}

    assert build_same_symbol_pairs(
        quotes,
        ["BTCUSDT"],
        observed_at_ms=1,
        diagnostics=diagnostics,
    ) == []
    assert diagnostics["directional_pair_count"] == 2
    assert diagnostics["rejection_counts"] == {"invalid_trade_quote": 2}


def test_same_venue_duplicate_quotes_never_form_a_candidate() -> None:
    quotes = {
        "binance:BTCUSDT:first": QuoteSnapshot(
            venue="binance",
            symbol="BTCUSDT",
            bid=99.9,
            ask=100.0,
            funding_rate_bps=0.0,
        ),
        "binance:BTCUSDT:second": QuoteSnapshot(
            venue="binance",
            symbol="BTCUSDT",
            bid=100.3,
            ask=100.4,
            funding_rate_bps=10.0,
        ),
    }
    diagnostics: dict[str, object] = {}

    assert build_same_symbol_pairs(
        quotes,
        ["BTCUSDT"],
        observed_at_ms=1,
        diagnostics=diagnostics,
    ) == []
    assert diagnostics["directional_pair_count"] == 0
    assert diagnostics["output_candidate_count"] == 0


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("contract_multiplier", "bad"),
        ("price_precision", "bad"),
        ("quantity_precision", "bad"),
        ("funding_interval_ms", "bad"),
    ],
)
def test_bad_contract_scalar_never_aborts_pair_construction(
    field: str, invalid_value: object
) -> None:
    def quote(venue: str, rate: float) -> QuoteSnapshot:
        return QuoteSnapshot(
            venue=venue,
            symbol="BTCUSDT",
            bid=99.0,
            ask=100.0,
            funding_rate_bps=rate,
            funding_timestamp_ms=1_000,
            funding_interval_ms=28_800_000,
            underlying="BTC",
            quote_currency="USDT",
            contract_type="linear",
            contract_multiplier=1.0,
            mark_index_source="venue_mark_index",
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

    long_quote = quote("cheap", 1.0)
    short_quote = quote("rich", 5.0)
    setattr(long_quote, field, invalid_value)

    candidates = build_same_symbol_pairs(
        {"cheap:BTCUSDT": long_quote, "rich:BTCUSDT": short_quote},
        ["BTCUSDT"],
        observed_at_ms=1,
    )

    assert not candidates or all(candidate.blocked for candidate in candidates)


def test_sidecar_candidate_rejects_source_quote_after_refresh_timestamp() -> None:
    quotes = {
        "cheap:BTCUSDT": QuoteSnapshot(
            venue="cheap",
            symbol="BTCUSDT",
            bid=99.0,
            ask=100.0,
            funding_rate_bps=1.0,
            funding_timestamp_ms=2_000,
            funding_interval_ms=28_800_000,
            observed_at_ms=1_001,
        ),
        "rich:BTCUSDT": QuoteSnapshot(
            venue="rich",
            symbol="BTCUSDT",
            bid=100.0,
            ask=101.0,
            funding_rate_bps=8.0,
            funding_timestamp_ms=2_000,
            funding_interval_ms=28_800_000,
            observed_at_ms=1_000,
        ),
    }

    diagnostics: dict[str, object] = {}
    assert (
        build_same_symbol_pairs(
            quotes,
            ["BTCUSDT"],
            observed_at_ms=1_000,
            diagnostics=diagnostics,
        )
        == []
    )
    assert diagnostics["future_input_quote_count"] == 1
    assert diagnostics["rejection_counts"] == {
        "funding_edge_below_floor": 1,
        "quote_after_candidate_watermark": 1,
    }
    assert diagnostics["directional_pair_count"] == 2


def test_funding_forecast_worst_case_uses_short_lower_and_long_upper_bounds() -> None:
    long = FundingForecast.from_quote(
        venue="cheap",
        symbol="BTCUSDT",
        quoted_rate_bps=2.0,
        next_funding_timestamp_ms=1_000,
        funding_interval_ms=28_800_000,
        observed_at_ms=1,
        uncertainty_haircut_bps=1.0,
    )
    short = FundingForecast.from_quote(
        venue="rich",
        symbol="BTCUSDT",
        quoted_rate_bps=8.0,
        next_funding_timestamp_ms=1_000,
        funding_interval_ms=28_800_000,
        observed_at_ms=1,
        uncertainty_haircut_bps=1.0,
    )

    expected, worst = conservative_funding_edge_bps(long_forecast=long, short_forecast=short)
    assert expected == pytest.approx(6.0)
    assert worst == pytest.approx(4.0)
    assert long.confidence == 0.0


def test_predicted_funding_is_not_high_confidence_before_calibration_samples() -> None:
    forecast = FundingForecast.from_quote(
        venue="rich",
        symbol="BTCUSDT",
        quoted_rate_bps=8.0,
        predicted_settled_rate_bps=12.0,
        next_funding_timestamp_ms=1_000,
        funding_interval_ms=28_800_000,
        observed_at_ms=1,
        uncertainty_haircut_bps=3.0,
        sample_count=23,
        min_samples=24,
        source="exchange_predicted",
    )

    assert forecast.predicted_settled_rate_bps == 12.0
    assert forecast.lower_bound_bps == 9.0
    assert forecast.upper_bound_bps == 15.0
    assert forecast.confidence == 0.0


def test_supplied_forecast_confidence_cannot_bypass_sample_contract() -> None:
    forecast = FundingForecast.from_quote(
        venue="rich",
        symbol="BTCUSDT",
        quoted_rate_bps=8.0,
        predicted_settled_rate_bps=12.0,
        next_funding_timestamp_ms=1_000,
        funding_interval_ms=28_800_000,
        observed_at_ms=1,
        uncertainty_haircut_bps=3.0,
        sample_count=0,
        min_samples=24,
        source="exchange_predicted",
        confidence=1.0,
    )

    assert forecast.predicted_settled_rate_bps == pytest.approx(12.0)
    assert forecast.confidence == 0.0


def test_nonfinite_predicted_funding_cannot_pass_enhanced_forecast_confidence() -> None:
    forecast = FundingForecast.from_quote(
        venue="rich",
        symbol="BTCUSDT",
        quoted_rate_bps=8.0,
        predicted_settled_rate_bps=float("nan"),
        next_funding_timestamp_ms="not-an-integer",
        funding_interval_ms=float("inf"),
        observed_at_ms=float("nan"),
        uncertainty_haircut_bps=float("inf"),
        sample_count=float("nan"),
        min_samples=24,
        confidence=1.0,
    )

    assert forecast.predicted_settled_rate_bps == pytest.approx(8.0)
    assert forecast.lower_bound_bps == pytest.approx(8.0)
    assert forecast.upper_bound_bps == pytest.approx(8.0)
    assert forecast.confidence == 0.0
    assert forecast.sample_count == 0
    assert forecast.next_funding_timestamp_ms == 0
    assert forecast.funding_interval_ms == 0
    assert forecast.observed_at_ms == 0


def test_funding_calibrator_uses_only_advanced_settlement_evidence(tmp_path) -> None:
    calibrator = FundingForecastCalibrator(tmp_path / "forecast.json")
    first = QuoteSnapshot(
        venue="binance",
        symbol="BTCUSDT",
        bid=99.0,
        ask=100.0,
        funding_rate_bps=5.0,
        funding_timestamp_ms=1_000,
        funding_interval_ms=1_000,
        settled_funding_rate_bps=1.0,
        observed_at_ms=100,
    )
    # The same next timestamp is not a settlement transition, even if a
    # public endpoint repeats a last-funding field.
    calibrator.apply({"binance:BTCUSDT": first}, now_ms=100)
    assert first.funding_forecast_sample_count == 0
    assert first.funding_forecast_started_at_ms == 100

    advanced = QuoteSnapshot(
        venue="binance",
        symbol="BTCUSDT",
        bid=99.0,
        ask=100.0,
        funding_rate_bps=6.0,
        funding_timestamp_ms=2_000,
        funding_interval_ms=1_000,
        settled_funding_rate_bps=2.0,
        observed_at_ms=1_100,
    )
    calibrator.apply({"binance:BTCUSDT": advanced}, now_ms=1_100)
    assert advanced.funding_forecast_sample_count == 1
    assert advanced.funding_forecast_uncertainty_bps == pytest.approx(3.0)

    # Current-rate forecasts become usable only when an explicit configured
    # sample threshold has been met; no next-rate field is required.
    forecast = FundingForecast.from_quote(
        venue="binance",
        symbol="BTCUSDT",
        quoted_rate_bps=6.0,
        next_funding_timestamp_ms=2_000,
        funding_interval_ms=28_800_000,
        observed_at_ms=1_100,
        uncertainty_haircut_bps=advanced.funding_forecast_uncertainty_bps,
        sample_count=1,
        min_samples=1,
        source="quoted_rate",
    )
    assert forecast.confidence == 1.0
    assert forecast.lower_bound_bps == pytest.approx(3.0)


def test_funding_calibrator_ignores_future_timestamp_settlement_evidence(tmp_path) -> None:
    calibrator = FundingForecastCalibrator(tmp_path / "forecast.json")
    future = QuoteSnapshot(
        venue="binance",
        symbol="BTCUSDT",
        bid=99.0,
        ask=100.0,
        funding_rate_bps=6.0,
        funding_timestamp_ms=2_000,
        funding_interval_ms=1_000,
        settled_funding_rate_bps=2.0,
        observed_at_ms=101,
    )

    calibrator.apply({"binance:BTCUSDT": future}, now_ms=100)

    assert future.funding_forecast_sample_count == 0
    assert future.funding_forecast_distribution_stable is False
    assert future.funding_forecast_stability_reason == "future_quote_timestamp"
    assert "binance:BTCUSDT" not in calibrator._pending
    assert not calibrator._errors.get("binance:BTCUSDT")


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("observed_at_ms", "bad"),
        ("funding_timestamp_ms", "bad"),
        ("funding_interval_ms", "bad"),
        ("funding_rate_bps", "bad"),
        ("predicted_funding_rate_bps", "bad"),
        ("settled_funding_rate_bps", "bad"),
    ],
)
def test_funding_calibrator_rejects_bad_quote_scalar_without_state_pollution(
    tmp_path, field, invalid_value
) -> None:
    calibrator = FundingForecastCalibrator(tmp_path / "forecast.json")
    quote = QuoteSnapshot(
        venue="binance",
        symbol="BTCUSDT",
        bid=99.0,
        ask=100.0,
        funding_rate_bps=5.0,
        funding_timestamp_ms=1_000,
        funding_interval_ms=1_000,
        observed_at_ms=100,
    )
    setattr(quote, field, invalid_value)

    calibrator.prime({"binance:BTCUSDT": quote})
    calibrator.apply({"binance:BTCUSDT": quote}, now_ms=100)

    assert quote.funding_forecast_sample_count == 0
    assert quote.funding_forecast_distribution_stable is False
    assert quote.funding_forecast_stability_reason == "invalid_funding_evidence"
    assert "binance:BTCUSDT" not in calibrator._pending
    assert not calibrator._errors.get("binance:BTCUSDT")


def test_funding_calibrator_rejects_advanced_schedule_before_settlement_time(tmp_path) -> None:
    calibrator = FundingForecastCalibrator(tmp_path / "forecast.json")
    calibrator.apply(
        {
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance",
                symbol="BTCUSDT",
                bid=99.0,
                ask=100.0,
                funding_rate_bps=5.0,
                funding_timestamp_ms=1_000,
                funding_interval_ms=1_000,
                observed_at_ms=100,
            )
        },
        now_ms=100,
    )

    # A source may claim the next period has advanced while its own observed
    # timestamp is still before the prior settlement.  This is not a settled
    # funding fact and cannot earn calibration credit.
    premature = QuoteSnapshot(
        venue="binance",
        symbol="BTCUSDT",
        bid=99.0,
        ask=100.0,
        funding_rate_bps=6.0,
        funding_timestamp_ms=2_000,
        funding_interval_ms=1_000,
        settled_funding_rate_bps=2.0,
        observed_at_ms=900,
    )
    calibrator.apply({"binance:BTCUSDT": premature}, now_ms=900)

    assert premature.funding_forecast_sample_count == 0
    assert not calibrator._errors.get("binance:BTCUSDT")


def test_funding_calibrator_does_not_misallocate_a_multi_interval_gap(tmp_path) -> None:
    calibrator = FundingForecastCalibrator(tmp_path / "forecast.json")
    calibrator.apply(
        {
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance",
                symbol="BTCUSDT",
                bid=99.0,
                ask=100.0,
                funding_rate_bps=5.0,
                funding_timestamp_ms=1_000,
                funding_interval_ms=1_000,
                observed_at_ms=100,
            )
        },
        now_ms=100,
    )
    after_outage = QuoteSnapshot(
        venue="binance",
        symbol="BTCUSDT",
        bid=99.0,
        ask=100.0,
        funding_rate_bps=6.0,
        funding_timestamp_ms=3_000,
        funding_interval_ms=1_000,
        settled_funding_rate_bps=2.0,
        observed_at_ms=2_100,
    )

    calibrator.apply({"binance:BTCUSDT": after_outage}, now_ms=2_100)

    # The last settlement fact only identifies the immediately preceding
    # interval. It cannot calibrate a forecast that is two intervals old.
    assert after_outage.funding_forecast_sample_count == 0


def test_funding_calibrator_requires_stable_recent_error_distribution(tmp_path) -> None:
    calibrator = FundingForecastCalibrator(
        tmp_path / "forecast.json",
        min_samples=6,
        max_quantile_drift_bps=1.0,
    )
    key = "binance:BTCUSDT"
    calibrator._errors[key] = [
        (1_000, 1.0),
        (2_000, 1.0),
        (3_000, 1.0),
        (4_000, 1.0),
        (5_000, 1.0),
        (6_000, 1.0),
    ]
    stable = QuoteSnapshot(
        venue="binance",
        symbol="BTCUSDT",
        bid=99.0,
        ask=100.0,
        funding_rate_bps=6.0,
        funding_timestamp_ms=7_000,
        funding_interval_ms=1_000,
        observed_at_ms=6_000,
    )
    calibrator.apply({key: stable}, now_ms=6_000)

    assert stable.funding_forecast_distribution_stable is True
    assert stable.funding_forecast_stability_reason == "stable"
    assert stable.funding_forecast_p90_drift_bps == pytest.approx(0.0)

    # The median remains unchanged, but a recently wider tail must prevent the
    # forecast from advancing to enhanced-live.
    calibrator._errors[key] = [
        (1_000, 1.0),
        (2_000, 1.0),
        (3_000, 1.0),
        (4_000, 1.0),
        (5_000, 1.0),
        (6_000, 5.0),
    ]
    unstable = QuoteSnapshot(
        venue="binance",
        symbol="BTCUSDT",
        bid=99.0,
        ask=100.0,
        funding_rate_bps=6.0,
        funding_timestamp_ms=8_000,
        funding_interval_ms=1_000,
        observed_at_ms=6_000,
    )
    calibrator.apply({key: unstable}, now_ms=6_000)

    assert unstable.funding_forecast_distribution_stable is False
    assert unstable.funding_forecast_stability_reason == "p90_error_distribution_drift"
    assert unstable.funding_forecast_p90_drift_bps == pytest.approx(4.0)


def test_v1_and_shadow_keep_quoted_funding_gate_until_enhanced_live_is_calibrated() -> None:
    quotes = {
        "cheap:BTCUSDT": QuoteSnapshot(
            venue="cheap",
            symbol="BTCUSDT",
            bid=99.9,
            ask=100.0,
            observed_at_ms=10,
            funding_rate_bps=2.0,
            funding_rate_observed_at_ms=10,
            funding_rate_event_at_ms=10,
            funding_rate_received_at_ms=10,
            funding_rate_source="test_fixture",
            funding_rate_sample_id="funding:cheap:BTCUSDT:10:2:1000000",
            predicted_funding_rate_bps=1.0,
            funding_timestamp_ms=1_000_000,
            funding_interval_ms=28_800_000,
            ask_size=10.0,
            funding_forecast_sample_count=100,
            funding_forecast_distribution_stable=True,
            underlying="BTC",
            quote_currency="USDT",
            contract_type="linear",
            contract_multiplier=1.0,
            mark_index_source="venue_mark_and_index",
            price_precision=2,
            quantity_precision=3,
            price_tick=0.01,
            quantity_step_base=0.001,
            min_quantity_base=0.001,
            min_notional_quote=1.0,
            min_notional_evidence_complete=True,
            venue_status="active",
            contract_normalization_complete=True,
        ),
        "rich:BTCUSDT": QuoteSnapshot(
            venue="rich",
            symbol="BTCUSDT",
            bid=100.3,
            ask=100.4,
            observed_at_ms=10,
            funding_rate_bps=8.0,
            funding_rate_observed_at_ms=10,
            funding_rate_event_at_ms=10,
            funding_rate_received_at_ms=10,
            funding_rate_source="test_fixture",
            funding_rate_sample_id="funding:rich:BTCUSDT:10:8:1000000",
            predicted_funding_rate_bps=20.0,
            funding_timestamp_ms=1_000_000,
            funding_interval_ms=28_800_000,
            bid_size=10.0,
            funding_forecast_sample_count=100,
            funding_forecast_distribution_stable=True,
            underlying="BTC",
            quote_currency="USDT",
            contract_type="linear",
            contract_multiplier=1.0,
            mark_index_source="venue_mark_and_index",
                price_precision=2,
                quantity_precision=3,
                price_tick=0.01,
                quantity_step_base=0.001,
                min_quantity_base=0.001,
                min_notional_quote=1.0,
                min_notional_evidence_complete=True,
                venue_status="active",
            contract_normalization_complete=True,
        ),
    }
    shadow = StrategyConfig(
        funding_economics_mode="enhanced_shadow",
        funding_forecast_shadow_min_days=0,
    )
    shadow_candidate = build_same_symbol_pairs(
        quotes,
        ["BTCUSDT"],
        strategy=shadow,
        venue_fee_bps={"cheap": 0.5, "rich": 0.5},
        observed_at_ms=10,
    )[0]

    assert shadow_candidate.funding_edge_bps == pytest.approx(6.0)
    assert shadow_candidate.forecast_short_rate_bps == pytest.approx(20.0)
    assert shadow_candidate.forecast_ready is True
    assert shadow_candidate.calculation_version == "enhanced_shadow"
    assert shadow_candidate.model_epoch == "enhanced_shadow"

    uncalibrated = StrategyConfig(
        funding_economics_mode="enhanced_live",
        funding_forecast_mode="live",
        funding_forecast_shadow_min_days=0,
    )
    low_sample_quotes = {
        key: QuoteSnapshot(**{**quote.__dict__, "funding_forecast_sample_count": 0})
        for key, quote in quotes.items()
    }
    blocked_candidate = build_same_symbol_pairs(
        low_sample_quotes,
        ["BTCUSDT"],
        strategy=uncalibrated,
        venue_fee_bps={"cheap": 0.5, "rich": 0.5},
        observed_at_ms=10,
    )[0]
    assert blocked_candidate.economics_complete is False

    live_candidate = build_same_symbol_pairs(
        quotes,
        ["BTCUSDT"],
        strategy=uncalibrated,
        venue_fee_bps={"cheap": 0.5, "rich": 0.5},
        observed_at_ms=10,
    )[0]
    assert live_candidate.funding_edge_bps == pytest.approx(19.0)
    assert live_candidate.economics_complete is True
    assert live_candidate.calculation_version == "enhanced_live"

    unstable_quotes = {
        key: QuoteSnapshot(
            **{
                **quote.__dict__,
                "funding_forecast_distribution_stable": False,
                "funding_forecast_stability_reason": "p90_error_distribution_drift",
            }
        )
        for key, quote in quotes.items()
    }
    unstable_candidate = build_same_symbol_pairs(
        unstable_quotes,
        ["BTCUSDT"],
        strategy=uncalibrated,
        venue_fee_bps={"cheap": 0.5, "rich": 0.5},
        observed_at_ms=10,
    )[0]
    assert unstable_candidate.economics_complete is False
    assert "funding_forecast_distribution_unstable" in unstable_candidate.blocked_reasons


def test_enhanced_live_discovers_a_direction_reversed_by_calibrated_forecast() -> None:
    common = {
        "symbol": "BTCUSDT",
        "funding_timestamp_ms": 1_000_000,
        "funding_interval_ms": 28_800_000,
        "funding_forecast_sample_count": 1,
        "funding_forecast_distribution_stable": True,
        "observed_at_ms": 10,
        "underlying": "BTC",
        "quote_currency": "USDT",
        "contract_type": "linear",
        "contract_multiplier": 1.0,
        "mark_index_source": "venue_mark_and_index",
        "price_precision": 2,
        "quantity_precision": 3,
        "price_tick": 0.01,
        "quantity_step_base": 0.001,
        "min_quantity_base": 0.001,
        "min_notional_quote": 1.0,
        "min_notional_evidence_complete": True,
        "venue_status": "active",
        "contract_normalization_complete": True,
    }
    quotes = {
        "a:BTCUSDT": QuoteSnapshot(
            venue="a",
            bid=99.9,
            ask=100.0,
            ask_size=10.0,
            funding_rate_bps=8.0,
            funding_rate_observed_at_ms=10,
            funding_rate_event_at_ms=10,
            funding_rate_received_at_ms=10,
            funding_rate_source="test_fixture",
            funding_rate_sample_id="funding:a:BTCUSDT:10:8:1000000",
            predicted_funding_rate_bps=1.0,
            **common,
        ),
        "b:BTCUSDT": QuoteSnapshot(
            venue="b",
            bid=100.2,
            ask=100.3,
            bid_size=10.0,
            funding_rate_bps=1.0,
            funding_rate_observed_at_ms=10,
            funding_rate_event_at_ms=10,
            funding_rate_received_at_ms=10,
            funding_rate_source="test_fixture",
            funding_rate_sample_id="funding:b:BTCUSDT:10:1:1000000",
            predicted_funding_rate_bps=10.0,
            **common,
        ),
    }
    enhanced = StrategyConfig(
        funding_economics_mode="enhanced_live",
        funding_forecast_mode="live",
        funding_forecast_min_samples=1,
        funding_forecast_shadow_min_days=0,
    )

    legacy = build_same_symbol_pairs(
        quotes, ["BTCUSDT"], strategy=StrategyConfig(), observed_at_ms=10
    )
    candidates = build_same_symbol_pairs(
        quotes,
        ["BTCUSDT"],
        strategy=enhanced,
        venue_fee_bps={"a": 0.5, "b": 0.5},
        observed_at_ms=10,
    )

    assert not any(c.long_venue == "a" and c.short_venue == "b" for c in legacy)
    reversed_direction = next(c for c in candidates if c.long_venue == "a" and c.short_venue == "b")
    assert reversed_direction.funding_edge_bps == pytest.approx(9.0)
    assert reversed_direction.economics_complete is True


def test_truthy_non_boolean_funding_economics_cannot_pass_final_revalidation() -> None:
    candidate = _candidate(economics_complete="true")

    result = FundingEntryRevalidator().revalidate_before_first_leg(
        candidate,
        long_ask=100.0,
        short_bid=100.0,
        now_ms=1,
        config=StrategyConfig(min_expected_edge_bps=1.0, min_worst_case_edge_bps=1.0),
    )

    assert result.allowed is False
    assert result.reason == "incomplete_economics"
    assert result.edge.economics_complete is False


def test_enhanced_live_requires_the_configured_shadow_duration() -> None:
    now_ms = 8 * 24 * 60 * 60 * 1000
    quotes = {
        "cheap:BTCUSDT": QuoteSnapshot(
            venue="cheap",
            symbol="BTCUSDT",
            bid=99.9,
            ask=100.0,
            observed_at_ms=now_ms,
            funding_rate_bps=2.0,
            funding_rate_observed_at_ms=now_ms,
            funding_rate_event_at_ms=now_ms,
            funding_rate_received_at_ms=now_ms,
            funding_rate_source="test_fixture",
            funding_rate_sample_id=(
                f"funding:cheap:BTCUSDT:{now_ms}:2:{now_ms + 1_000_000}"
            ),
            predicted_funding_rate_bps=1.0,
            funding_timestamp_ms=now_ms + 1_000_000,
            funding_interval_ms=28_800_000,
            ask_size=10.0,
            funding_forecast_sample_count=100,
            funding_forecast_started_at_ms=now_ms - 6 * 24 * 60 * 60 * 1000,
            funding_forecast_distribution_stable=True,
            underlying="BTC",
            quote_currency="USDT",
            contract_type="linear",
            contract_multiplier=1.0,
            mark_index_source="venue_mark_and_index",
            price_precision=2,
            quantity_precision=3,
            price_tick=0.01,
            quantity_step_base=0.001,
            min_quantity_base=0.001,
            min_notional_quote=1.0,
            min_notional_evidence_complete=True,
            venue_status="active",
            contract_normalization_complete=True,
        ),
        "rich:BTCUSDT": QuoteSnapshot(
            venue="rich",
            symbol="BTCUSDT",
            bid=100.3,
            ask=100.4,
            observed_at_ms=now_ms,
            funding_rate_bps=8.0,
            funding_rate_observed_at_ms=now_ms,
            funding_rate_event_at_ms=now_ms,
            funding_rate_received_at_ms=now_ms,
            funding_rate_source="test_fixture",
            funding_rate_sample_id=(
                f"funding:rich:BTCUSDT:{now_ms}:8:{now_ms + 1_000_000}"
            ),
            predicted_funding_rate_bps=20.0,
            funding_timestamp_ms=now_ms + 1_000_000,
            funding_interval_ms=28_800_000,
            bid_size=10.0,
            funding_forecast_sample_count=100,
            funding_forecast_started_at_ms=now_ms - 6 * 24 * 60 * 60 * 1000,
            funding_forecast_distribution_stable=True,
            underlying="BTC",
            quote_currency="USDT",
            contract_type="linear",
            contract_multiplier=1.0,
            mark_index_source="venue_mark_and_index",
            price_precision=2,
            quantity_precision=3,
            price_tick=0.01,
            quantity_step_base=0.001,
            min_quantity_base=0.001,
            min_notional_quote=1.0,
            min_notional_evidence_complete=True,
            venue_status="active",
            contract_normalization_complete=True,
        ),
    }
    config = StrategyConfig(
        funding_economics_mode="enhanced_live",
        funding_forecast_mode="live",
        funding_forecast_min_samples=24,
        funding_forecast_shadow_min_days=7,
    )

    blocked = build_same_symbol_pairs(
        quotes,
        ["BTCUSDT"],
        strategy=config,
        venue_fee_bps={"cheap": 0.5, "rich": 0.5},
        observed_at_ms=now_ms,
    )[0]
    assert blocked.economics_complete is False
    assert blocked.forecast_ready is False
    assert blocked.forecast_shadow_age_ms == 6 * 24 * 60 * 60 * 1000

    for quote in quotes.values():
        quote.funding_forecast_started_at_ms = now_ms - 7 * 24 * 60 * 60 * 1000
    ready = build_same_symbol_pairs(
        quotes,
        ["BTCUSDT"],
        strategy=config,
        venue_fee_bps={"cheap": 0.5, "rich": 0.5},
        observed_at_ms=now_ms,
    )[0]
    assert ready.economics_complete is True
    assert ready.forecast_ready is True


def test_every_funding_mode_blocks_unproven_common_base_contracts() -> None:
    quotes = {
        "cheap:BTCUSDT": QuoteSnapshot(
            venue="cheap",
            symbol="BTCUSDT",
            bid=99.9,
            ask=100.0,
            funding_rate_bps=2.0,
            predicted_funding_rate_bps=1.0,
            funding_timestamp_ms=1_000_000,
            funding_interval_ms=28_800_000,
            ask_size=10.0,
            funding_forecast_sample_count=100,
        ),
        "rich:BTCUSDT": QuoteSnapshot(
            venue="rich",
            symbol="BTCUSDT",
            bid=100.3,
            ask=100.4,
            funding_rate_bps=8.0,
            predicted_funding_rate_bps=20.0,
            funding_timestamp_ms=1_000_000,
            funding_interval_ms=28_800_000,
            bid_size=10.0,
            funding_forecast_sample_count=100,
        ),
    }

    candidate = build_same_symbol_pairs(
        quotes,
        ["BTCUSDT"],
        strategy=StrategyConfig(
            funding_economics_mode="enhanced_live",
            funding_forecast_mode="live",
        ),
        venue_fee_bps={"cheap": 0.0, "rich": 0.0},
        observed_at_ms=10,
    )[0]

    assert candidate.blocked is True
    assert candidate.economics_complete is False
    assert "long_contract_normalization_incomplete" in candidate.blocked_reasons

    v1_candidate = build_same_symbol_pairs(
        quotes,
        ["BTCUSDT"],
        strategy=StrategyConfig(),
        venue_fee_bps={"cheap": 0.0, "rich": 0.0},
        observed_at_ms=10,
    )[0]
    assert v1_candidate.blocked is True
    assert v1_candidate.economics_complete is False
    assert "long_contract_normalization_incomplete" in v1_candidate.blocked_reasons


def test_funding_contract_normalization_requires_literal_boolean_true() -> None:
    common = {
        "symbol": "BTCUSDT",
        "funding_timestamp_ms": 1_000_000,
        "funding_interval_ms": 28_800_000,
        "underlying": "BTC",
        "quote_currency": "USDT",
        "contract_type": "linear",
        "contract_multiplier": 1.0,
        "mark_index_source": "venue_mark_and_index",
        "price_precision": 2,
        "quantity_precision": 3,
        "price_tick": 0.01,
        "quantity_step_base": 0.001,
        "min_quantity_base": 0.001,
        "min_notional_quote": 1.0,
        "min_notional_evidence_complete": True,
        "venue_status": "active",
    }
    quotes = {
        "cheap:BTCUSDT": QuoteSnapshot(
            venue="cheap",
            bid=99.9,
            ask=100.0,
            funding_rate_bps=2.0,
            contract_normalization_complete="false",
            **common,
        ),
        "rich:BTCUSDT": QuoteSnapshot(
            venue="rich",
            bid=100.3,
            ask=100.4,
            funding_rate_bps=8.0,
            contract_normalization_complete=True,
            **common,
        ),
    }

    candidate = build_same_symbol_pairs(
        quotes,
        ["BTCUSDT"],
        strategy=StrategyConfig(),
        venue_fee_bps={"cheap": 0.0, "rich": 0.0},
        observed_at_ms=10,
    )[0]

    assert candidate.economics_complete is False
    assert "long_contract_normalization_incomplete" in candidate.blocked_reasons


def test_passive_candidate_credits_spread_but_not_unverified_maker_discount() -> None:
    """Passive scoring keeps V1 price impact but floors fees at two takers."""
    common = {
        "symbol": "BTCUSDT",
        "funding_timestamp_ms": 1_000_000,
        "funding_interval_ms": 28_800_000,
        "underlying": "BTC",
        "quote_currency": "USDT",
        "contract_type": "linear",
        "contract_multiplier": 1.0,
        "mark_index_source": "venue_mark_and_index",
        "price_precision": 2,
        "quantity_precision": 3,
        "price_tick": 0.01,
        "quantity_step_base": 0.001,
        "min_quantity_base": 0.001,
        "min_notional_quote": 1.0,
        "min_notional_evidence_complete": True,
        "venue_status": "active",
        "contract_normalization_complete": True,
    }
    quotes = {
        "cheap:BTCUSDT": QuoteSnapshot(
            venue="cheap",
            bid=99.0,
            ask=100.0,
            bid_size=1.0,
            ask_size=10.0,
            funding_rate_bps=2.0,
            **common,
        ),
        "rich:BTCUSDT": QuoteSnapshot(
            venue="rich",
            bid=102.0,
            ask=104.0,
            bid_size=10.0,
            ask_size=1.0,
            funding_rate_bps=12.0,
            **common,
        ),
    }
    config = StrategyConfig(
        entry_notional_cap_quote=100.0,
        live_entry_notional_cap_quote=100.0,
        funding_missing_margin_fallback_notional_quote=20.0,
        entry_exit_reserve_bps=3.0,
        capital_buffer_bps=1.0,
        execution_buffer_bps=2.0,
    )

    passive = build_same_symbol_pairs(
        quotes,
        ["BTCUSDT"],
        strategy=config,
        venue_fee_bps={"cheap": 5.0, "rich": 7.0},
        venue_maker_fee_bps={"cheap": 1.0, "rich": 1.0},
        passive_execution_enabled=True,
        observed_at_ms=10,
    )[0]

    # V1 selects the leg with greater taker impact as maker.  Spread recovery
    # and the opposite-leg impact remain executable, but a static maker tier
    # cannot authenticate its own snapshot provenance, so it may not create
    # alpha and the fee stays at the conservative two-taker floor.
    assert passive.entry_maker_leg == "short"
    assert passive.exit_maker_leg == "short"
    assert passive.entry_fee_bps == pytest.approx(12.0)
    assert passive.exit_fee_bps == pytest.approx(12.0)
    assert passive.entry_slippage_bps == pytest.approx(passive.long_entry_slippage_bps)
    assert passive.exit_slippage_bps == pytest.approx(passive.long_exit_slippage_bps)
    raw_cross_bps = (102.0 - 100.0) / 101.0 * 10_000.0
    rich_spread_bps = (104.0 - 102.0) / 101.0 * 10_000.0
    assert passive.entry_cross_bps == pytest.approx(raw_cross_bps + rich_spread_bps)
    assert passive.expected_edge_bps == pytest.approx(
        passive.funding_edge_bps
        + passive.entry_cross_bps
        - passive.entry_fee_bps
        - passive.exit_fee_bps
        - passive.entry_slippage_bps
        - passive.exit_slippage_bps
        - passive.adverse_selection_bps
        - passive.capital_buffer_bps
        - passive.venue_risk_haircut_bps
    )
    assert passive.worst_case_edge_bps == pytest.approx(
        passive.expected_edge_bps - passive.execution_buffer_bps
    )

    taker = build_same_symbol_pairs(
        quotes,
        ["BTCUSDT"],
        strategy=config,
        venue_fee_bps={"cheap": 5.0, "rich": 7.0},
        venue_maker_fee_bps={"cheap": 1.0, "rich": 1.0},
        passive_execution_enabled=False,
        observed_at_ms=10,
    )[0]
    assert taker.entry_maker_leg == ""
    assert taker.exit_maker_leg == ""
    assert taker.entry_fee_bps == pytest.approx(12.0)
    assert taker.exit_fee_bps == pytest.approx(12.0)
    assert taker.entry_cross_bps == pytest.approx(raw_cross_bps)
    assert taker.entry_slippage_bps == pytest.approx(
        taker.long_entry_slippage_bps + taker.short_entry_slippage_bps
    )
    assert taker.exit_slippage_bps == pytest.approx(
        taker.long_exit_slippage_bps + taker.short_exit_slippage_bps
    )


def test_staggered_candidate_is_admitted_on_first_settlement_not_total_carry() -> None:
    quotes = {
        "binance:BTCUSDT": QuoteSnapshot(
            venue="binance",
            symbol="BTCUSDT",
            bid=99.9,
            ask=100.0,
            # Long receives 12 bps first; the later short settlement pays
            # 8 bps.  Its carry is incremental, not first-stage income.
            funding_rate_bps=-12.0,
            funding_timestamp_ms=1_000_000,
            funding_interval_ms=28_800_000,
            bid_size=10.0,
            ask_size=10.0,
        ),
        "okx:BTCUSDT": QuoteSnapshot(
            venue="okx",
            symbol="BTCUSDT",
            bid=100.4,
            ask=100.5,
            funding_rate_bps=-8.0,
            funding_timestamp_ms=1_060_001,
            funding_interval_ms=28_800_000,
            bid_size=10.0,
            ask_size=10.0,
        ),
    }

    candidate = build_same_symbol_pairs(
        quotes,
        ["BTCUSDT"],
        strategy=StrategyConfig(),
        venue_fee_bps={"binance": 0.0, "okx": 0.0},
        observed_at_ms=10,
    )[0]

    assert candidate.opportunity_type == "staggered"
    assert candidate.first_funding_leg == "long"
    assert candidate.funding_edge_bps == pytest.approx(12.0)
    assert candidate.first_stage_funding_edge_bps == pytest.approx(12.0)
    assert candidate.total_funding_edge_bps == pytest.approx(4.0)
    assert candidate.second_stage_incremental_funding_edge_bps == pytest.approx(-8.0)
    # The entry expected edge is based on the 12 bps first-stage receipt, not
    # the lower total that only exists if a later-stage hold is chosen.
    assert candidate.expected_edge_bps == pytest.approx(candidate.first_stage_expected_edge_bps)
    assert candidate.expected_edge_bps > candidate.total_funding_edge_bps


def test_funding_candidate_requires_valid_configured_taker_fees() -> None:
    quotes = {
        "cheap:BTCUSDT": QuoteSnapshot(
            venue="cheap",
            symbol="BTCUSDT",
            bid=99.9,
            ask=100.0,
            funding_rate_bps=2.0,
            funding_timestamp_ms=1_000_000,
            funding_interval_ms=28_800_000,
            ask_size=10.0,
        ),
        "rich:BTCUSDT": QuoteSnapshot(
            venue="rich",
            symbol="BTCUSDT",
            bid=100.3,
            ask=100.4,
            funding_rate_bps=8.0,
            funding_timestamp_ms=1_000_000,
            funding_interval_ms=28_800_000,
            bid_size=10.0,
        ),
    }

    diagnostics: dict[str, object] = {}
    missing = build_same_symbol_pairs(
        quotes,
        ["BTCUSDT"],
        strategy=StrategyConfig(),
        observed_at_ms=10,
        diagnostics=diagnostics,
    )
    explicit_zero = build_same_symbol_pairs(
        quotes,
        ["BTCUSDT"],
        strategy=StrategyConfig(),
        venue_fee_bps={"cheap": 0.0, "rich": 0.0},
        observed_at_ms=10,
    )[0]

    assert missing == []
    assert diagnostics["rejection_counts"] == {
        "configured_taker_fee_unavailable": 1,
        "funding_edge_below_floor": 1,
    }
    assert explicit_zero.long_taker_fee_bps == 0.0
    assert explicit_zero.short_taker_fee_bps == 0.0


def test_funding_candidate_rejects_boolean_configured_taker_fee() -> None:
    quotes = {
        "cheap:BTCUSDT": QuoteSnapshot(
            venue="cheap",
            symbol="BTCUSDT",
            bid=99.9,
            ask=100.0,
            funding_rate_bps=2.0,
            funding_timestamp_ms=1_000_000,
            funding_interval_ms=28_800_000,
            ask_size=10.0,
        ),
        "rich:BTCUSDT": QuoteSnapshot(
            venue="rich",
            symbol="BTCUSDT",
            bid=100.3,
            ask=100.4,
            funding_rate_bps=8.0,
            funding_timestamp_ms=1_000_000,
            funding_interval_ms=28_800_000,
            bid_size=10.0,
        ),
    }

    diagnostics: dict[str, object] = {}
    candidates = build_same_symbol_pairs(
        quotes,
        ["BTCUSDT"],
        strategy=StrategyConfig(),
        venue_fee_bps={"cheap": True, "rich": 0.0},  # type: ignore[dict-item]
        observed_at_ms=10,
        diagnostics=diagnostics,
    )

    assert candidates == []
    assert diagnostics["rejection_counts"] == {
        "configured_taker_fee_unavailable": 1,
        "funding_edge_below_floor": 1,
    }


def test_risk_allocator_returns_one_common_base_quantity_and_falls_back_on_missing_margin() -> None:
    allocation = StrategyRiskAllocator().allocate(
        long_entry_price=100.0,
        short_entry_price=110.0,
        long_max_quantity=1.0,
        short_max_quantity=0.5,
        configured_notional_cap_quote=1_000.0,
        fallback_notional_quote=21.0,
        health_buffer_ratio=0.5,
    )

    assert allocation.base_quantity == pytest.approx(0.2)
    assert allocation.long_leg_notional_quote == pytest.approx(20.0)
    assert allocation.short_leg_notional_quote == pytest.approx(22.0)
    assert allocation.evidence_complete is False
    assert "missing_margin_fallback" in allocation.constrained_by


def test_risk_allocator_caps_common_base_quantity_by_each_leg_margin() -> None:
    allocation = StrategyRiskAllocator().allocate(
        long_entry_price=100.0,
        short_entry_price=200.0,
        long_max_quantity=10.0,
        short_max_quantity=10.0,
        configured_notional_cap_quote=10_000.0,
        long_available_margin_quote=10.0,
        short_available_margin_quote=100.0,
        target_leverage=4.0,
        health_buffer_ratio=0.5,
    )

    # Long can fund 10 * 50% * 4 / 100 = 0.2 base.  The richer short
    # account must not conceal that paired constraint.
    assert allocation.base_quantity == pytest.approx(0.2)
    assert allocation.long_leg_notional_quote == pytest.approx(20.0)
    assert allocation.short_leg_notional_quote == pytest.approx(40.0)
    assert allocation.evidence_complete is True
    assert "long_margin_health" in allocation.constrained_by


def test_live_final_entry_revalidation_fails_closed_without_complete_economics() -> None:
    events: list[tuple[str, dict]] = []
    runtime = EntryDispatchRuntime(
        SimpleNamespace(
            config=SimpleNamespace(runtime=SimpleNamespace(mode="live")),
            journal=SimpleNamespace(append=lambda kind, payload: events.append((kind, payload))),
            _candidate_pair_id=lambda _candidate: "binance:bybit:BTCUSDT",
        )
    )
    candidate = SimpleNamespace(
        symbol="BTCUSDT",
        long_venue="binance",
        short_venue="bybit",
        economics_complete=False,
        economics_observed_at_ms=0,
    )

    allowed = runtime._revalidate_final_entry_economics(
        candidate=candidate,
        quote_lease=None,
        required_base_quantity=0.0,
        now_ms=1_000,
        source="test_final_gate",
    )

    assert allowed is False
    assert events == [
        (
            "entry.dispatch_viability_blocked",
            {
                "entry_id": "",
                "symbol": "BTCUSDT",
                "long_venue": "binance",
                "short_venue": "bybit",
                "candidate_pair_id": "binance:bybit:BTCUSDT",
                "pair_id": "binance:bybit:BTCUSDT",
                "reason": "incomplete_economics",
                "blocked_reasons": ["incomplete_economics"],
                "source": "test_final_gate",
                "decision": "skip_before_first_leg",
                "ts_ms": 1_000,
                "economics_complete": False,
                "economics_observed_at_ms": 0,
            },
        )
    ]


def test_live_final_entry_revalidation_selects_executable_passive_orientation() -> None:
    events: list[tuple[str, dict]] = []
    strategy = StrategyConfig(
        min_expected_edge_bps=0.0,
        min_worst_case_edge_bps=0.0,
    )
    runtime = EntryDispatchRuntime(
        SimpleNamespace(
            config=SimpleNamespace(
                runtime=SimpleNamespace(mode="live"),
                strategy=strategy,
            ),
            journal=SimpleNamespace(
                append=lambda kind, payload: events.append((kind, payload))
            ),
            _candidate_pair_id=lambda _candidate: "btcusdt:cheap->rich",
            _local_l2_effective_enabled=lambda: True,
        )
    )
    candidate = _candidate(
        entry_maker_leg="long",
        exit_maker_leg="",
        entry_target_quantity=1.0,
        entry_notional_quote=100.0,
        entry_fee_bps=2.0,
        exit_fee_bps=2.0,
        long_taker_fee_bps=1.0,
        short_taker_fee_bps=1.0,
        economics_observed_at_ms=1_000,
        model_epoch="v1_exact",
    )
    quote_lease = QuoteLease(
        pair_id="btcusdt:cheap->rich",
        symbol="BTCUSDT",
        long_venue="cheap",
        short_venue="rich",
        long_bid=100.0,
        long_ask=101.0,
        short_bid=102.0,
        short_ask=103.0,
        long_observed_at_ms=1_000,
        short_observed_at_ms=1_000,
        created_at_ms=1_000,
        expires_at_ms=2_000,
        long_bid_size=1.0,
        long_ask_size=1.0,
        short_bid_size=1.0,
        short_ask_size=1.0,
        provider="local_l2_final_vwap",
        long_buy_vwap=101.2,
        short_sell_vwap=101.4,
        long_buy_sweep_limit=101.2,
        short_sell_sweep_limit=101.4,
        long_l2_capacity_quantity=1.0,
        short_l2_capacity_quantity=0.5,
        l2_vwap_quantity=1.0,
        l2_vwap_complete=False,
    )

    assert runtime._revalidate_final_entry_economics(
        candidate=candidate,
        quote_lease=quote_lease,
        required_base_quantity=1.0,
        now_ms=1_100,
        source="final_entry_economics",
        select_passive_maker_orientation=True,
    )
    assert candidate.entry_maker_leg == "short"
    assert candidate.entry_notional_quote == pytest.approx((101.2 + 103.0) / 2.0)
    assert events[-1][0] == "runtime.entry_passive_maker_orientation_selected"
    assert events[-1][1]["previous_entry_maker_leg"] == "long"
    assert events[-1][1]["selected_entry_maker_leg"] == "short"


@pytest.mark.asyncio
async def test_live_margin_sizing_uses_conservative_one_x_until_effective_leverage_is_proven() -> (
    None
):
    events: list[tuple[str, dict]] = []

    async def margin_evidence(_venue: Venue, _now_ms: int) -> dict:
        return {"evidence_complete": True, "available_margin_quote": 10.0}

    runtime = EntryDispatchRuntime(
        SimpleNamespace(
            config=SimpleNamespace(
                strategy=SimpleNamespace(
                    live_target_leverage=10.0,
                    funding_risk_health_buffer_ratio=1.0,
                    funding_missing_margin_fallback_notional_quote=0.0,
                )
            ),
            journal=SimpleNamespace(append=lambda kind, payload: events.append((kind, payload))),
            _candidate_pair_id=lambda _candidate: "binance:aster:BTCUSDT",
            _funding_entry_margin_evidence=margin_evidence,
        )
    )

    result = await runtime._resolve_live_margin_quantity(
        candidate=SimpleNamespace(symbol="BTCUSDT"),
        now_ms=1_000,
        long_venue=Venue.BINANCE,
        short_venue=Venue.ASTER,
        long_entry_price=100.0,
        short_entry_price=100.0,
        current_quantity=1.0,
        okx_base_step=None,
        long_quantity_step=0.001,
        short_quantity_step=0.001,
    )

    assert result == (pytest.approx(0.1), True)
    payload = next(payload for kind, payload in events if kind == "runtime.entry_margin_sizing")
    assert payload["configured_target_leverage"] == 10.0
    assert payload["sizing_leverage"] == 1.0
    assert payload["leverage_evidence_complete"] is False


@pytest.mark.asyncio
async def test_live_margin_sizing_rejects_truthy_non_boolean_evidence() -> None:
    events: list[tuple[str, dict]] = []

    async def margin_evidence(_venue: Venue, _now_ms: int) -> dict:
        return {"evidence_complete": "true", "available_margin_quote": 10.0}

    runtime = EntryDispatchRuntime(
        SimpleNamespace(
            config=SimpleNamespace(
                strategy=SimpleNamespace(
                    live_target_leverage=10.0,
                    funding_risk_health_buffer_ratio=1.0,
                    funding_missing_margin_fallback_notional_quote=0.0,
                )
            ),
            journal=SimpleNamespace(append=lambda kind, payload: events.append((kind, payload))),
            _candidate_pair_id=lambda _candidate: "binance:aster:BTCUSDT",
            _funding_entry_margin_evidence=margin_evidence,
        )
    )

    result = await runtime._resolve_live_margin_quantity(
        candidate=SimpleNamespace(symbol="BTCUSDT"),
        now_ms=1_000,
        long_venue=Venue.BINANCE,
        short_venue=Venue.ASTER,
        long_entry_price=100.0,
        short_entry_price=100.0,
        current_quantity=1.0,
        okx_base_step=None,
        long_quantity_step=0.001,
        short_quantity_step=0.001,
    )

    assert result is None
    payload = next(
        payload for kind, payload in events if kind == "runtime.entry_margin_sizing_blocked"
    )
    assert payload["margin_evidence_complete"] is False


@pytest.mark.asyncio
async def test_live_margin_sizing_uses_minimum_verified_effective_leverage_for_both_legs() -> None:
    events: list[tuple[str, dict]] = []

    async def margin_evidence(_venue: Venue, _now_ms: int) -> dict:
        return {"evidence_complete": True, "available_margin_quote": 10.0}

    runtime = EntryDispatchRuntime(
        SimpleNamespace(
            config=SimpleNamespace(
                strategy=SimpleNamespace(
                    live_target_leverage=10.0,
                    funding_risk_health_buffer_ratio=1.0,
                    funding_missing_margin_fallback_notional_quote=0.0,
                )
            ),
            journal=SimpleNamespace(append=lambda kind, payload: events.append((kind, payload))),
            _candidate_pair_id=lambda _candidate: "binance:aster:BTCUSDT",
            _funding_entry_margin_evidence=margin_evidence,
        )
    )
    evidence = {
        Venue.BINANCE: EntryLeverageEvidence(
            venue=Venue.BINANCE,
            symbol="BTCUSDT",
            requested_leverage=10,
            effective_leverage=8,
            notional_quote=100.0,
            bracket_verified=True,
            account_verified=True,
            source="test",
            observed_at_ms=1_000,
        ),
        Venue.ASTER: EntryLeverageEvidence(
            venue=Venue.ASTER,
            symbol="BTCUSDT",
            requested_leverage=10,
            effective_leverage=5,
            notional_quote=100.0,
            bracket_verified=True,
            account_verified=True,
            source="test",
            observed_at_ms=1_000,
        ),
    }

    result = await runtime._resolve_live_margin_quantity(
        candidate=SimpleNamespace(symbol="BTCUSDT"),
        now_ms=1_000,
        long_venue=Venue.BINANCE,
        short_venue=Venue.ASTER,
        long_entry_price=100.0,
        short_entry_price=100.0,
        current_quantity=1.0,
        okx_base_step=None,
        long_quantity_step=0.001,
        short_quantity_step=0.001,
        leverage_evidence_by_venue=evidence,
    )

    # 10 quote collateral per leg at the paired minimum verified 5x.
    assert result == (pytest.approx(0.5), True)
    payload = next(payload for kind, payload in events if kind == "runtime.entry_margin_sizing")
    assert payload["sizing_leverage"] == 5.0
    assert payload["leverage_evidence_complete"] is True
    assert payload["leverage_evidence_reason"] == "verified_effective_leverage_min_two_legs"


@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), float("-inf")])
def test_risk_allocator_fails_closed_for_nonfinite_size_or_margin_evidence(
    bad_value: float,
) -> None:
    allocator = StrategyRiskAllocator()

    invalid_size = allocator.allocate(
        long_entry_price=100.0,
        short_entry_price=101.0,
        long_max_quantity=bad_value,
        short_max_quantity=1.0,
        configured_notional_cap_quote=30.0,
    )
    invalid_margin = allocator.allocate(
        long_entry_price=100.0,
        short_entry_price=101.0,
        long_max_quantity=1.0,
        short_max_quantity=1.0,
        configured_notional_cap_quote=30.0,
        available_margin_quote=bad_value,
    )
    invalid_admission = allocator.assess_portfolio_admission(
        open_positions=[],
        symbol="BTCUSDT",
        long_venue="cheap",
        short_venue="rich",
        long_entry_price=100.0,
        short_entry_price=101.0,
        base_quantity=bad_value,
        first_funding_timestamp_ms=1_000,
        max_concurrent_positions=0,
        max_single_venue_exposure_quote=0.0,
        max_symbol_exposure_quote=0.0,
        max_concurrent_venue_pairs=0,
        max_venue_pair_exposure_quote=0.0,
        max_global_gross_exposure_quote=0.0,
        max_settlement_bucket_exposure_quote=0.0,
        settlement_crowding_bucket_ms=300_000,
        max_correlation_group_exposure_quote=0.0,
        correlation_group_by_symbol={},
    )

    assert invalid_size.base_quantity == 0.0
    assert invalid_margin.base_quantity == 0.0
    assert invalid_size.constrained_by == ("nonfinite_risk_input",)
    assert invalid_margin.constrained_by == ("nonfinite_risk_input",)
    assert invalid_admission.allowed is False
    assert invalid_admission.reason == "nonfinite_risk_input"


def test_risk_allocator_malformed_settlement_timestamp_is_safe_and_blocks_when_required() -> None:
    admission = StrategyRiskAllocator().assess_portfolio_admission(
        open_positions=[],
        symbol="BTCUSDT",
        long_venue="cheap",
        short_venue="rich",
        long_entry_price=100.0,
        short_entry_price=101.0,
        base_quantity=0.1,
        first_funding_timestamp_ms=float("nan"),
        max_concurrent_positions=0,
        max_single_venue_exposure_quote=0.0,
        max_symbol_exposure_quote=0.0,
        max_concurrent_venue_pairs=0,
        max_venue_pair_exposure_quote=0.0,
        max_global_gross_exposure_quote=0.0,
        max_settlement_bucket_exposure_quote=10.0,
        settlement_crowding_bucket_ms=300_000,
        max_correlation_group_exposure_quote=0.0,
        correlation_group_by_symbol={},
    )

    assert admission.allowed is False
    assert admission.reason == "missing_candidate_settlement_time"


def test_risk_allocator_malformed_recovered_position_is_evidence_gap_not_exception() -> None:
    malformed = SimpleNamespace(
        symbol="BTCUSDT",
        long_venue="cheap",
        short_venue="rich",
        long_quantity="not-a-number",
        short_quantity=0.1,
        long_entry_price=100.0,
        short_entry_price=101.0,
        funding_timestamp_ms=1_000,
    )

    admission = StrategyRiskAllocator().assess_portfolio_admission(
        open_positions=[malformed],
        symbol="ETHUSDT",
        long_venue="cheap",
        short_venue="other",
        long_entry_price=100.0,
        short_entry_price=101.0,
        base_quantity=0.1,
        first_funding_timestamp_ms=1_000,
        max_concurrent_positions=0,
        max_single_venue_exposure_quote=0.0,
        max_symbol_exposure_quote=0.0,
        max_concurrent_venue_pairs=0,
        max_venue_pair_exposure_quote=1_000.0,
        max_global_gross_exposure_quote=0.0,
        max_settlement_bucket_exposure_quote=0.0,
        settlement_crowding_bucket_ms=300_000,
        max_correlation_group_exposure_quote=0.0,
        correlation_group_by_symbol={},
    )

    assert admission.allowed is False
    assert admission.reason == "incomplete_open_position_risk_evidence"


def test_post_first_fill_decision_compares_all_in_price_and_fee_loss() -> None:
    revalidator = FundingEntryRevalidator()

    decision = revalidator.decide_after_first_leg(
        complete_hedge_loss_quote=0.20,
        unwind_first_leg_loss_quote=0.10,
        complete_hedge_fee_quote=0.01,
        unwind_first_leg_fee_quote=0.20,
    )

    assert decision.action == "complete_hedge"
    assert decision.complete_hedge_loss_quote == pytest.approx(0.21)
    assert decision.unwind_first_leg_loss_quote == pytest.approx(0.30)
    assert decision.complete_hedge_price_loss_quote == pytest.approx(0.20)
    assert decision.unwind_first_leg_price_loss_quote == pytest.approx(0.10)


@pytest.mark.parametrize(
    ("maker_side", "expected_hedge", "expected_unwind"),
    [
        ("buy", 98.0, 99.0),
        ("sell", 102.0, 101.0),
    ],
)
def test_post_first_fill_market_pricing_is_shared_for_buy_and_sell_lifecycles(
    maker_side: str,
    expected_hedge: float,
    expected_unwind: float,
) -> None:
    result = FundingEntryRevalidator().decide_from_first_fill_market(
        maker_side=maker_side,
        maker_fill_price=100.0,
        quantity=1.0,
        maker_bid=99.0,
        maker_ask=101.0,
        hedge_bid=98.0,
        hedge_ask=102.0,
        hedge_fee_bps=1.0,
        unwind_fee_bps=1.0,
    )

    assert result is not None
    assert result.hedge_price == pytest.approx(expected_hedge)
    assert result.unwind_price == pytest.approx(expected_unwind)
    assert result.decision.action == "unwind_first_leg"


def test_post_first_fill_fee_lookup_refuses_unknown_or_nonfinite_venue_evidence() -> None:
    revalidator = FundingEntryRevalidator()
    known = SimpleNamespace(venue="cheap", taker_fee_bps=2.0)
    malformed = SimpleNamespace(venue="broken", taker_fee_bps=float("nan"))

    assert revalidator.taker_fee_bps_for_venue("cheap", [known]) == pytest.approx(2.0)
    assert revalidator.taker_fee_bps_for_venue("missing", [known]) is None
    assert revalidator.taker_fee_bps_for_venue("broken", [malformed]) is None
    assert revalidator.taker_fee_quote(price=100.0, quantity=0.1, fee_bps=2.0) == pytest.approx(
        0.002
    )


def test_risk_allocator_enforces_portfolio_venue_symbol_and_settlement_limits() -> None:
    existing = SimpleNamespace(
        symbol="BTCUSDT",
        long_venue="cheap",
        short_venue="rich",
        long_quantity=0.5,
        short_quantity=0.5,
        long_entry_price=100.0,
        short_entry_price=100.0,
        funding_timestamp_ms=1_200_000,
    )
    admission = StrategyRiskAllocator().assess_portfolio_admission(
        open_positions=[existing],
        symbol="ETHUSDT",
        long_venue="cheap",
        short_venue="other",
        long_entry_price=100.0,
        short_entry_price=100.0,
        base_quantity=0.6,
        first_funding_timestamp_ms=1_250_000,
        max_concurrent_positions=0,
        max_single_venue_exposure_quote=100.0,
        max_symbol_exposure_quote=1_000.0,
        max_concurrent_venue_pairs=0,
        max_venue_pair_exposure_quote=0.0,
        max_global_gross_exposure_quote=0.0,
        max_settlement_bucket_exposure_quote=200.0,
        settlement_crowding_bucket_ms=300_000,
        max_correlation_group_exposure_quote=0.0,
        correlation_group_by_symbol={},
    )

    assert admission.allowed is False
    assert admission.reason == "max_single_venue_exposure"
    assert admission.projected_venue_exposure_quote["cheap"] == pytest.approx(110.0)


def test_risk_allocator_enforces_granular_position_count_limits() -> None:
    existing = SimpleNamespace(
        symbol="BTCUSDT",
        long_venue="cheap",
        short_venue="rich",
        long_quantity=0.1,
        short_quantity=0.1,
        long_entry_price=100.0,
        short_entry_price=100.0,
        funding_timestamp_ms=1_200_000,
    )
    base = {
        "open_positions": [existing],
        "long_entry_price": 100.0,
        "short_entry_price": 100.0,
        "base_quantity": 0.1,
        "first_funding_timestamp_ms": 1_250_000,
        "max_concurrent_positions": 8,
        "max_single_venue_exposure_quote": 0.0,
        "max_symbol_exposure_quote": 0.0,
        "max_concurrent_venue_pairs": 0,
        "max_venue_pair_exposure_quote": 0.0,
        "max_global_gross_exposure_quote": 0.0,
        "max_settlement_bucket_exposure_quote": 0.0,
        "settlement_crowding_bucket_ms": 300_000,
        "max_correlation_group_exposure_quote": 0.0,
        "correlation_group_by_symbol": {},
    }
    allocator = StrategyRiskAllocator()

    per_venue = allocator.assess_portfolio_admission(
        **base,
        symbol="ETHUSDT",
        long_venue="cheap",
        short_venue="other",
        max_concurrent_positions_per_venue=1,
    )
    per_symbol = allocator.assess_portfolio_admission(
        **base,
        symbol="BTCUSDT",
        long_venue="other-a",
        short_venue="other-b",
        max_concurrent_positions_per_symbol=1,
    )
    per_pair = allocator.assess_portfolio_admission(
        **base,
        symbol="ETHUSDT",
        long_venue="cheap",
        short_venue="rich",
        max_concurrent_positions_per_venue_pair=1,
    )

    assert per_venue.reason == "max_concurrent_positions_per_venue"
    assert per_symbol.reason == "max_concurrent_positions_per_symbol"
    assert per_pair.reason == "max_concurrent_positions_per_venue_pair"


def test_risk_allocator_fails_closed_when_settlement_budget_lacks_evidence() -> None:
    missing_settlement = StrategyRiskAllocator().assess_portfolio_admission(
        open_positions=[],
        symbol="BTCUSDT",
        long_venue="cheap",
        short_venue="rich",
        long_entry_price=100.0,
        short_entry_price=100.0,
        base_quantity=0.1,
        first_funding_timestamp_ms=0,
        max_concurrent_positions=0,
        max_single_venue_exposure_quote=0.0,
        max_symbol_exposure_quote=0.0,
        max_concurrent_venue_pairs=0,
        max_venue_pair_exposure_quote=0.0,
        max_global_gross_exposure_quote=0.0,
        max_settlement_bucket_exposure_quote=10.0,
        settlement_crowding_bucket_ms=300_000,
        max_correlation_group_exposure_quote=0.0,
        correlation_group_by_symbol={},
    )

    assert missing_settlement.reason == "missing_candidate_settlement_time"


def test_final_entry_revalidation_fails_closed_when_final_book_erases_alpha() -> None:
    candidate = _candidate()
    config = StrategyConfig(min_expected_edge_bps=1.0, min_worst_case_edge_bps=0.0)

    decision = FundingEntryRevalidator().revalidate_before_first_leg(
        candidate, long_ask=101.0, short_bid=99.0, now_ms=10, config=config
    )

    assert decision.allowed is False
    assert decision.reason == "final_expected_edge_below_floor"
    assert decision.edge.entry_cross_bps < 0.0


def test_final_entry_revalidation_uses_full_l2_vwap_for_common_base_quantity() -> None:
    candidate = _candidate(entry_target_quantity=1.0)
    decision = FundingEntryRevalidator().revalidate_before_first_leg(
        candidate,
        long_ask=100.0,
        short_bid=100.2,
        long_buy_vwap=101.0,
        short_sell_vwap=99.2,
        required_base_quantity=1.0,
        l2_vwap_complete=True,
        require_l2_vwap=True,
        now_ms=10,
        config=StrategyConfig(min_expected_edge_bps=1.0, min_worst_case_edge_bps=0.0),
    )

    assert decision.allowed is False
    assert decision.reason == "final_expected_edge_below_floor"
    assert decision.long_entry_price == pytest.approx(101.0)
    assert decision.short_entry_price == pytest.approx(99.2)
    assert decision.l2_entry_slippage_bps > 0.0
    # The executable VWAP already contains the actual depth impact, so it is
    # expressed once in the signed entry cross, not deducted a second time.
    assert decision.edge.entry_slippage_bps == 0.0


def test_final_revalidation_prices_the_selected_passive_leg_and_only_hedge_vwap() -> None:
    candidate = _candidate(
        entry_maker_leg="short",
        entry_fee_bps=2.0,
        entry_slippage_bps=3.0,
        entry_target_quantity=1.0,
        long_taker_fee_bps=1.0,
        short_taker_fee_bps=1.0,
        economics_observed_at_ms=10,
    )

    decision = FundingEntryRevalidator().revalidate_before_first_leg(
        candidate,
        long_bid=99.0,
        long_ask=100.0,
        short_bid=102.0,
        short_ask=104.0,
        long_buy_vwap=101.0,
        short_sell_vwap=100.0,
        required_base_quantity=1.0,
        l2_vwap_complete=True,
        require_l2_vwap=True,
        now_ms=10,
        config=StrategyConfig(
            min_expected_edge_bps=-1_000.0,
            min_worst_case_edge_bps=-1_000.0,
        ),
    )

    assert decision.allowed is True
    # The short is post-only at its ask; only the long hedge crosses L2.
    assert decision.long_entry_price == pytest.approx(101.0)
    assert decision.short_entry_price == pytest.approx(104.0)
    assert decision.edge.entry_cross_bps == pytest.approx((104.0 - 101.0) / 102.5 * 10_000.0)
    assert decision.edge.entry_slippage_bps == 0.0
    assert decision.l2_entry_slippage_bps == pytest.approx((101.0 - 100.0) / 102.0 * 10_000.0)


def test_standard_fallback_revalidates_with_four_taker_costs() -> None:
    candidate = _candidate(
        entry_maker_leg="short",
        entry_fee_bps=1.0,
        long_taker_fee_bps=3.0,
        short_taker_fee_bps=4.0,
        entry_slippage_bps=5.0,
        entry_target_quantity=1.0,
    )

    decision = FundingEntryRevalidator().revalidate_before_first_leg(
        candidate,
        long_bid=99.0,
        long_ask=100.0,
        short_bid=102.0,
        short_ask=104.0,
        long_buy_vwap=101.0,
        short_sell_vwap=101.0,
        required_base_quantity=1.0,
        l2_vwap_complete=True,
        require_l2_vwap=True,
        execution_is_passive=False,
        now_ms=10,
        config=StrategyConfig(
            min_expected_edge_bps=-1_000.0,
            min_worst_case_edge_bps=-1_000.0,
        ),
    )

    assert decision.allowed is True
    assert decision.long_entry_price == pytest.approx(101.0)
    assert decision.short_entry_price == pytest.approx(101.0)
    assert decision.edge.entry_fee_bps == pytest.approx(7.0)
    assert decision.edge.exit_fee_bps == pytest.approx(7.0)
    assert decision.edge.entry_slippage_bps == 0.0


def test_final_entry_revalidation_rejects_incomplete_l2_capacity_before_first_leg() -> None:
    decision = FundingEntryRevalidator().revalidate_before_first_leg(
        _candidate(entry_target_quantity=1.0),
        long_ask=100.0,
        short_bid=100.2,
        required_base_quantity=1.0,
        l2_vwap_complete=False,
        require_l2_vwap=True,
        now_ms=10,
        config=StrategyConfig(),
    )

    assert decision.allowed is False
    assert decision.reason == "missing_final_l2_vwap"


@pytest.mark.parametrize(
    ("long_buy_vwap", "short_sell_vwap"),
    [
        (99.9, 100.0),  # A taker buy cannot average below best ask 100.0.
        (100.0, 100.3),  # A taker sell cannot average above best bid 100.2.
    ],
)
def test_final_entry_revalidation_rejects_impossible_l2_taker_improvement(
    long_buy_vwap: float,
    short_sell_vwap: float,
) -> None:
    decision = FundingEntryRevalidator().revalidate_before_first_leg(
        _candidate(entry_target_quantity=1.0),
        long_ask=100.0,
        short_bid=100.2,
        long_buy_vwap=long_buy_vwap,
        short_sell_vwap=short_sell_vwap,
        required_base_quantity=1.0,
        l2_vwap_complete=True,
        require_l2_vwap=True,
        now_ms=10,
        config=StrategyConfig(),
    )

    assert decision.allowed is False
    assert decision.reason == "inconsistent_final_l2_vwap"


def test_final_entry_revalidation_rejects_truthy_nonboolean_l2_evidence() -> None:
    decision = FundingEntryRevalidator().revalidate_before_first_leg(
        _candidate(entry_target_quantity=1.0),
        long_ask=100.0,
        short_bid=100.2,
        long_buy_vwap=101.0,
        short_sell_vwap=99.2,
        required_base_quantity=1.0,
        l2_vwap_complete="true",  # type: ignore[arg-type]
        require_l2_vwap=True,
        now_ms=10,
        config=StrategyConfig(),
    )

    assert decision.allowed is False
    assert decision.reason == "missing_final_l2_vwap"


def test_final_entry_revalidation_treats_truthy_passive_override_as_standard_ioc() -> None:
    candidate = _candidate(
        entry_maker_leg="short",
        entry_fee_bps=1.0,
        long_taker_fee_bps=3.0,
        short_taker_fee_bps=4.0,
        entry_target_quantity=1.0,
    )

    decision = FundingEntryRevalidator().revalidate_before_first_leg(
        candidate,
        long_bid=99.0,
        long_ask=100.0,
        short_bid=102.0,
        short_ask=104.0,
        long_buy_vwap=101.0,
        short_sell_vwap=101.0,
        required_base_quantity=1.0,
        l2_vwap_complete=True,
        require_l2_vwap=True,
        execution_is_passive="false",  # type: ignore[arg-type]
        now_ms=10,
        config=StrategyConfig(
            min_expected_edge_bps=-1_000.0,
            min_worst_case_edge_bps=-1_000.0,
        ),
    )

    assert decision.allowed is True
    assert decision.short_entry_price == pytest.approx(101.0)
    assert decision.edge.entry_fee_bps == pytest.approx(7.0)


def test_final_entry_revalidation_preserves_worst_case_and_filled_first_leg_never_abandons_risk() -> (
    None
):
    candidate = _candidate()
    config = StrategyConfig(min_expected_edge_bps=1.0, min_worst_case_edge_bps=0.0)
    revalidator = FundingEntryRevalidator()

    decision = revalidator.revalidate_before_first_leg(
        candidate, long_ask=100.0, short_bid=100.2, now_ms=10, config=config
    )
    assert decision.allowed is True
    assert decision.edge.worst_case_edge_bps < decision.edge.expected_net_edge_bps

    hedge = revalidator.decide_after_first_leg(
        complete_hedge_loss_quote=0.5, unwind_first_leg_loss_quote=1.0
    )
    unwind = revalidator.decide_after_first_leg(
        complete_hedge_loss_quote=2.0, unwind_first_leg_loss_quote=1.0
    )
    assert hedge.action == "complete_hedge"
    assert unwind.action == "unwind_first_leg"


def test_v1_final_revalidation_does_not_apply_shadow_forecast_to_first_stage_worst_case() -> None:
    candidate = _candidate(
        calculation_version="v1_exact",
        funding_edge_bps=12.0,
        # A shadow-only forecast must not silently alter the V1 gate.
        forecast_worst_funding_edge_bps=-50.0,
    )

    decision = FundingEntryRevalidator().revalidate_before_first_leg(
        candidate,
        long_ask=100.0,
        short_bid=100.2,
        now_ms=10,
        config=StrategyConfig(min_expected_edge_bps=-100.0, min_worst_case_edge_bps=-100.0),
    )

    assert decision.allowed is True
    assert decision.edge.worst_case_edge_bps == pytest.approx(
        decision.edge.expected_net_edge_bps - candidate.execution_buffer_bps
    )
