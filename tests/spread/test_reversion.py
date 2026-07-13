from __future__ import annotations

import math

import pytest

from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.reversion import (
    SpreadReversionConfig,
    SpreadStatsTracker,
    build_spread_reversion_candidates,
)


_MEAN_REVERTING_HISTORY = [
    8, 6, 4, 3, 2, 1, 0, -1, -2, -1,
    0, 1, 2, 1, 0, -1, -2, -1, 0, 1,
    2, 3, 2, 1, 0, -1, -2, -1, 0, 1,
]


def _quote(
    venue: str,
    *,
    bid: float,
    ask: float,
    observed_at_ms: int,
    symbol: str = "BTCUSDT",
    bid_size: float = 10.0,
    ask_size: float = 10.0,
    **contract: object,
) -> QuoteSnapshot:
    contract.setdefault("underlying", "BTC")
    contract.setdefault("quote_currency", "USDT")
    contract.setdefault("contract_type", "linear")
    contract.setdefault("contract_multiplier", 1.0)
    contract.setdefault("mark_index_source", "venue_mark")
    contract.setdefault("price_precision", 2)
    contract.setdefault("quantity_precision", 3)
    contract.setdefault("contract_normalization_complete", True)
    contract.setdefault("funding_timestamp_ms", observed_at_ms + 3_600_000)
    contract.setdefault("funding_interval_ms", 28_800_000)
    return QuoteSnapshot(
        venue=venue,
        symbol=symbol,
        bid=bid,
        ask=ask,
        observed_at_ms=observed_at_ms,
        bid_size=bid_size,
        ask_size=ask_size,
        **contract,
    )


def _quotes_for_signed_basis(
    basis_bps: float,
    *,
    now_ms: int,
    symbol: str = "BTCUSDT",
    half_spread: float = 0.01,
    **contract: object,
) -> dict[str, QuoteSnapshot]:
    # If a=100 and d=(a-b)/((a+b)/2)*10_000, solve b exactly.
    a_mid = 100.0
    b_mid = a_mid * (1.0 - basis_bps / 20_000.0) / (1.0 + basis_bps / 20_000.0)
    return {
        f"cheap:{symbol}": _quote(
            "cheap", bid=a_mid - half_spread, ask=a_mid + half_spread,
            observed_at_ms=now_ms, symbol=symbol, **contract,
        ),
        f"rich:{symbol}": _quote(
            "rich", bid=b_mid - half_spread, ask=b_mid + half_spread,
            observed_at_ms=now_ms, symbol=symbol, **contract,
        ),
    }


def _config(**overrides: object) -> SpreadReversionConfig:
    values: dict[str, object] = {
        "min_samples": len(_MEAN_REVERTING_HISTORY),
        "min_history_ms": 0,
        "min_fair_price_confidence": 0.0,
        "min_liquidity_capacity_ratio": 1.0,
        "entry_z": 2.0,
        "exit_z": 0.5,
        "min_net_edge_bps": 0.0,
        "live_notional_quote": 20.0,
        "max_gross_quote": 20.0,
        "slippage_reserve_bps": 0.0,
        "adverse_selection_buffer_bps": 0.0,
        "taker_fee_bps_by_venue": {"cheap": 0.0, "rich": 0.0},
        "signal_ttl_ms": 5_000,
        "quote_skew_ms": 1_000,
    }
    values.update(overrides)
    return SpreadReversionConfig(**values)


def _prewarm(tracker: SpreadStatsTracker, *, center: float = 0.0) -> None:
    for index, value in enumerate(_MEAN_REVERTING_HISTORY, start=1):
        tracker.update(
            "BTCUSDT",
            "cheap",
            "rich",
            center + value,
            observed_at_ms=index * 1_000,
            exit_half_spread_bps=2.0,
        )


def test_signed_basis_pair_identity_does_not_flip_at_zero_crossing() -> None:
    tracker = SpreadStatsTracker()
    for index, value in enumerate([-4.0, -1.0, 1.0, 4.0], start=1):
        tracker.update("BTCUSDT", "rich", "cheap", value, observed_at_ms=index * 1_000)

    state = tracker.snapshot("BTCUSDT", "cheap", "rich", now_ms=4_000)
    assert state is not None
    assert state.sample_count == 4
    assert state.first_observed_ms == 1_000


def test_out_of_order_observation_cannot_contaminate_rolling_state_or_signal() -> None:
    tracker = SpreadStatsTracker()
    tracker.update("BTCUSDT", "cheap", "rich", -1.0, observed_at_ms=1_000)
    tracker.update("BTCUSDT", "cheap", "rich", 1.0, observed_at_ms=3_000)

    late = tracker.update("BTCUSDT", "cheap", "rich", 500.0, observed_at_ms=2_000)

    assert late.sample_count == 2
    assert late.last_observed_ms == 3_000
    assert build_spread_reversion_candidates(
        _quotes_for_signed_basis(20.0, now_ms=2_000),
        ["BTCUSDT"],
        tracker=tracker,
        config=_config(min_samples=1, min_history_ms=0),
        now_ms=2_000,
    ) == []


def test_current_observation_is_not_in_its_own_zscore_and_direction_is_correct() -> None:
    tracker = SpreadStatsTracker()
    _prewarm(tracker)
    before = tracker.snapshot("BTCUSDT", "cheap", "rich", now_ms=30_000)
    assert before is not None

    candidates = build_spread_reversion_candidates(
        _quotes_for_signed_basis(20.0, now_ms=31_000),
        ["BTCUSDT"],
        tracker=tracker,
        config=_config(),
        now_ms=31_000,
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.sample_count == before.sample_count
    assert candidate.rolling_mean_bps == before.median_bps
    assert candidate.z_score > 0.0
    assert candidate.long_venue == "rich"
    assert candidate.short_venue == "cheap"
    after = tracker.snapshot("BTCUSDT", "cheap", "rich", now_ms=31_000)
    assert after is not None and after.sample_count == before.sample_count + 1


def test_negative_z_reverses_trade_direction_without_creating_a_second_series() -> None:
    tracker = SpreadStatsTracker()
    _prewarm(tracker)

    candidates = build_spread_reversion_candidates(
        _quotes_for_signed_basis(-20.0, now_ms=31_000),
        ["BTCUSDT"],
        tracker=tracker,
        config=_config(),
        now_ms=31_000,
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.z_score < 0.0
    assert candidate.long_venue == "cheap"
    assert candidate.short_venue == "rich"
    assert candidate.candidate_id == "spread:BTCUSDT:cheap->rich"


def test_gross_edge_is_reversion_space_not_the_full_observed_cross_venue_spread() -> None:
    tracker = SpreadStatsTracker()
    _prewarm(tracker, center=80.0)

    candidates = build_spread_reversion_candidates(
        _quotes_for_signed_basis(100.0, now_ms=31_000, half_spread=0.0001),
        ["BTCUSDT"],
        tracker=tracker,
        config=_config(min_net_edge_bps=-100.0),
        now_ms=31_000,
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert abs(candidate.current_signed_mid_spread_bps) == pytest.approx(100.0)
    assert 10.0 < candidate.gross_reversion_edge_bps < 30.0
    assert candidate.gross_reversion_edge_bps < abs(candidate.current_signed_mid_spread_bps)
    assert candidate.expected_exit_cross_bps == pytest.approx(-2.0)


def test_spread_candidate_notional_is_per_leg_and_cannot_exceed_total_gross_cap() -> None:
    tracker = SpreadStatsTracker()
    _prewarm(tracker)

    candidates = build_spread_reversion_candidates(
        _quotes_for_signed_basis(20.0, now_ms=31_000),
        ["BTCUSDT"],
        tracker=tracker,
        config=_config(live_notional_quote=100.0, max_gross_quote=50.0),
        now_ms=31_000,
    )

    assert len(candidates) == 1
    assert candidates[0].entry_notional_quote == pytest.approx(25.0)


def test_missing_taker_fee_evidence_blocks_spread_candidate_but_explicit_zero_is_valid() -> None:
    missing_tracker = SpreadStatsTracker()
    _prewarm(missing_tracker)
    rejections: dict[str, int] = {}

    assert build_spread_reversion_candidates(
        _quotes_for_signed_basis(20.0, now_ms=31_000),
        ["BTCUSDT"],
        tracker=missing_tracker,
        config=_config(taker_fee_bps_by_venue={}),
        now_ms=31_000,
        rejection_counts=rejections,
    ) == []
    assert rejections == {"missing_taker_fee_evidence": 1}

    valid_tracker = SpreadStatsTracker()
    _prewarm(valid_tracker)
    candidates = build_spread_reversion_candidates(
        _quotes_for_signed_basis(20.0, now_ms=31_000),
        ["BTCUSDT"],
        tracker=valid_tracker,
        config=_config(),
        now_ms=31_000,
    )

    assert len(candidates) == 1
    assert candidates[0].fee_evidence_complete is True


def test_funding_changes_ranking_once_via_the_unified_edge_contract() -> None:
    def candidate_with_rates(long_rate: float, short_rate: float):
        tracker = SpreadStatsTracker()
        _prewarm(tracker)
        quotes = _quotes_for_signed_basis(20.0, now_ms=31_000)
        # A positive z-score goes long "rich" and short "cheap".
        quotes["rich:BTCUSDT"] = _quote(
            "rich",
            bid=quotes["rich:BTCUSDT"].bid,
            ask=quotes["rich:BTCUSDT"].ask,
            observed_at_ms=31_000,
            funding_rate_bps=long_rate,
            funding_timestamp_ms=32_000,
        )
        quotes["cheap:BTCUSDT"] = _quote(
            "cheap",
            bid=quotes["cheap:BTCUSDT"].bid,
            ask=quotes["cheap:BTCUSDT"].ask,
            observed_at_ms=31_000,
            funding_rate_bps=short_rate,
            funding_timestamp_ms=32_000,
        )
        result = build_spread_reversion_candidates(
            quotes,
            ["BTCUSDT"],
            tracker=tracker,
            config=_config(),
            now_ms=31_000,
        )
        assert len(result) == 1
        return result[0]

    neutral = candidate_with_rates(0.0, 0.0)
    tailwind = candidate_with_rates(-4.0, 8.0)

    assert tailwind.ranking_edge_bps - neutral.ranking_edge_bps == pytest.approx(12.0)
    assert tailwind.score - neutral.score == pytest.approx(12.0)


def test_ar1_requires_mean_reversion_and_uses_the_true_half_life_formula() -> None:
    tracker = SpreadStatsTracker()
    _prewarm(tracker)
    state = tracker.snapshot("BTCUSDT", "cheap", "rich", now_ms=30_000)
    assert state is not None
    assert state.ar1_phi is not None and 0.0 < state.ar1_phi < 1.0
    expected_half_life = -math.log(2.0) / math.log(state.ar1_phi) * 1_000.0
    assert state.half_life_ms == pytest.approx(expected_half_life, abs=1.0)

    non_reverting = SpreadStatsTracker()
    for index, value in enumerate([-2.0, 2.0] * 16, start=1):
        non_reverting.update("BTCUSDT", "cheap", "rich", value, observed_at_ms=index * 1_000)
    rejected = non_reverting.snapshot("BTCUSDT", "cheap", "rich", now_ms=32_000)
    assert rejected is not None
    assert rejected.ar1_phi is not None and rejected.ar1_phi <= 0.0


def test_ar1_half_life_fails_closed_after_an_irregular_quote_gap() -> None:
    tracker = SpreadStatsTracker()
    for observed_at_ms, value in (
        (1_000, 3.0),
        (2_000, 2.0),
        (3_000, 1.0),
        # A large data outage cannot be hidden by averaging it into the AR(1)
        # sampling interval.
        (30_000, 0.5),
    ):
        tracker.update("BTCUSDT", "cheap", "rich", value, observed_at_ms=observed_at_ms)

    state = tracker.snapshot("BTCUSDT", "cheap", "rich", now_ms=30_000)

    assert state is not None
    assert state.ar1_phi is not None and 0.0 < state.ar1_phi < 1.0
    assert state.half_life_ms == 0


def test_rolling_eviction_checkpoint_restore_and_structural_break_cooldown() -> None:
    tracker = SpreadStatsTracker(
        window_ms=15_000,
        max_samples=3,
        short_window_ms=100,
        structural_break_sigma=2.0,
        structural_break_consecutive=1,
        structural_break_cooldown_ms=1_000,
    )
    for index, value in enumerate([-1.0, 0.0, 1.0], start=1):
        tracker.update("BTCUSDT", "cheap", "rich", value, observed_at_ms=index * 1_000)
    tracker.update("BTCUSDT", "cheap", "rich", 100.0, observed_at_ms=10_000)
    broken = tracker.snapshot("BTCUSDT", "cheap", "rich", now_ms=10_000)
    assert broken is not None
    assert broken.sample_count == 0
    assert broken.cooldown_until_ms == 11_000

    checkpoint = tracker.checkpoint(now_ms=10_000)
    restored = SpreadStatsTracker(window_ms=15_000, max_samples=3)
    restored.restore(checkpoint, now_ms=10_000)
    state = restored.snapshot("BTCUSDT", "cheap", "rich", now_ms=10_000)
    assert state is not None and state.sample_count == 0


def test_contract_normalization_and_timestamp_freshness_fail_closed() -> None:
    tracker = SpreadStatsTracker()
    _prewarm(tracker)
    incompatible = _quotes_for_signed_basis(
        20.0,
        now_ms=31_000,
        contract_type="linear",
        quote_currency="USDT",
    )
    incompatible["rich:BTCUSDT"] = _quote(
        "rich", bid=99.79, ask=99.81, observed_at_ms=31_000,
        contract_type="inverse", quote_currency="USD",
    )
    assert build_spread_reversion_candidates(
        incompatible, ["BTCUSDT"], tracker=tracker, config=_config(), now_ms=31_000
    ) == []

    inverse = _quotes_for_signed_basis(
        20.0,
        now_ms=31_000,
        contract_type="inverse",
        quote_currency="USD",
        contract_multiplier=1.0,
    )
    inverse_rejections: dict[str, int] = {}
    assert build_spread_reversion_candidates(
        inverse,
        ["BTCUSDT"],
        tracker=SpreadStatsTracker(),
        config=_config(),
        now_ms=31_000,
        rejection_counts=inverse_rejections,
    ) == []
    assert inverse_rejections == {"unsupported_contract_type_for_base_quantity_pnl": 1}

    stale = _quotes_for_signed_basis(20.0, now_ms=1_000)
    assert build_spread_reversion_candidates(
        stale, ["BTCUSDT"], tracker=SpreadStatsTracker(), config=_config(), now_ms=10_000
    ) == []


def test_non_finite_bbo_or_funding_cannot_create_or_rank_a_spread_candidate() -> None:
    tracker = SpreadStatsTracker()
    _prewarm(tracker)
    malformed_price = _quotes_for_signed_basis(20.0, now_ms=31_000)
    malformed_price["rich:BTCUSDT"] = _quote(
        "rich",
        bid=float("nan"),
        ask=99.81,
        observed_at_ms=31_000,
    )
    assert build_spread_reversion_candidates(
        malformed_price, ["BTCUSDT"], tracker=tracker, config=_config(), now_ms=31_000
    ) == []

    malformed_funding = _quotes_for_signed_basis(20.0, now_ms=32_000)
    malformed_funding["rich:BTCUSDT"] = _quote(
        "rich",
        bid=malformed_funding["rich:BTCUSDT"].bid,
        ask=malformed_funding["rich:BTCUSDT"].ask,
        observed_at_ms=32_000,
        funding_rate_bps=float("nan"),
        funding_timestamp_ms=33_000,
    )
    assert build_spread_reversion_candidates(
        malformed_funding, ["BTCUSDT"], tracker=tracker, config=_config(), now_ms=32_000
    ) == []


def test_contract_evidence_and_funding_schedule_are_required_with_stable_reasons() -> None:
    tracker = SpreadStatsTracker()
    rejection_counts: dict[str, int] = {}
    incomplete = _quotes_for_signed_basis(
        20.0,
        now_ms=31_000,
        contract_normalization_complete=False,
    )

    assert build_spread_reversion_candidates(
        incomplete,
        ["BTCUSDT"],
        tracker=tracker,
        config=_config(),
        now_ms=31_000,
        rejection_counts=rejection_counts,
    ) == []
    assert rejection_counts == {"contract_normalization_incomplete": 1}

    _prewarm(tracker)
    schedule_unknown = _quotes_for_signed_basis(20.0, now_ms=31_000, funding_interval_ms=0)
    assert build_spread_reversion_candidates(
        schedule_unknown,
        ["BTCUSDT"],
        tracker=tracker,
        config=_config(),
        now_ms=31_000,
    ) == []


@pytest.mark.parametrize("field", ("price_precision", "quantity_precision"))
def test_contract_precision_evidence_is_required(field: str) -> None:
    tracker = SpreadStatsTracker()
    _prewarm(tracker)
    rejection_counts: dict[str, int] = {}
    quotes = _quotes_for_signed_basis(20.0, now_ms=31_000, **{field: 0})

    assert build_spread_reversion_candidates(
        quotes,
        ["BTCUSDT"],
        tracker=tracker,
        config=_config(),
        now_ms=31_000,
        rejection_counts=rejection_counts,
    ) == []
    assert rejection_counts == {f"missing_{field}": 1}
