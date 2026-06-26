from __future__ import annotations

import pytest

from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.models import SpreadReversionCandidate
from lightfee.spread.modules import (
    DegradationState,
    ExitRiskClassifier,
    FundingAwarenessModel,
    LiquidityAndVenueHealthGate,
    MeanReversionQualityModel,
    SpreadRanker,
)
from lightfee.spread.reversion import (
    SpreadReversionConfig,
    SpreadStatsTracker,
    build_spread_reversion_candidates,
)


def _quote(
    venue: str,
    *,
    bid: float,
    ask: float,
    observed_at_ms: int = 10_000,
    bid_size: float = 1.0,
    ask_size: float = 1.0,
    funding_rate_bps: float = 0.0,
) -> QuoteSnapshot:
    return QuoteSnapshot(
        venue=venue,
        symbol="BTCUSDT",
        bid=bid,
        ask=ask,
        bid_size=bid_size,
        ask_size=ask_size,
        observed_at_ms=observed_at_ms,
        funding_rate_bps=funding_rate_bps,
        funding_timestamp_ms=0,
    )


def _candidate(**overrides) -> SpreadReversionCandidate:
    data = {
        "candidate_id": "spread:BTCUSDT:binance->okx",
        "symbol": "BTCUSDT",
        "long_venue": "binance",
        "short_venue": "okx",
        "spread_mid_bps": 20.0,
        "executable_spread_bps": 18.0,
        "rolling_mean_bps": 8.0,
        "rolling_std_bps": 4.0,
        "z_score": 3.0,
        "net_edge_bps": 12.0,
        "sample_count": 20,
        "signal_ts_ms": 1_000,
        "long_quote_ts_ms": 1_000,
        "short_quote_ts_ms": 1_000,
        "entry_notional_quote": 20.0,
        "capacity_quote": 100.0,
        "signal_status": "entry_ready",
        "score": 10.0,
        "rank_reason": "score=10.00",
    }
    data.update(overrides)
    return SpreadReversionCandidate(**data)


def test_fair_price_model_filters_single_venue_outlier_before_pair_scoring() -> None:
    tracker = SpreadStatsTracker()
    cfg = SpreadReversionConfig(
        min_samples=1,
        min_history_ms=0,
        min_fair_price_confidence=0.0,
        min_liquidity_capacity_ratio=1.0,
        entry_z=0.0,
        min_net_edge_bps=0.0,
        slippage_reserve_bps=0.0,
        adverse_selection_buffer_bps=0.0,
    )
    quotes = {
        "binance:BTCUSDT": _quote("binance", bid=99.99, ask=100.01),
        "okx:BTCUSDT": _quote("okx", bid=100.04, ask=100.06),
        "isolated:BTCUSDT": _quote("isolated", bid=109.90, ask=110.10),
    }

    candidates = build_spread_reversion_candidates(
        quotes,
        ["BTCUSDT"],
        tracker=tracker,
        config=cfg,
        now_ms=10_000,
    )

    assert candidates
    assert all("isolated" not in {c.long_venue, c.short_venue} for c in candidates)
    assert candidates[0].fair_price_confidence > 0.0
    assert candidates[0].fair_price == pytest.approx(100.05)
    assert candidates[0].rank_reason


def test_mean_reversion_quality_enhances_z_score_without_independent_entry() -> None:
    model = MeanReversionQualityModel(max_half_life_ms=1_800_000)

    weak = model.assess(z_score=3.0, sample_count=20, rolling_std_bps=0.01)
    strong = model.assess(z_score=3.0, sample_count=20, rolling_std_bps=4.0)

    assert weak.entry_allowed is False
    assert weak.quality < strong.quality
    assert strong.entry_allowed is True
    assert strong.half_life_ms <= 1_800_000


def test_funding_awareness_scores_direction_without_flipping_spread_legs() -> None:
    model = FundingAwarenessModel(expected_hold_ms=3_600_000)
    tailwind = model.assess(long_funding_rate_bps=-4.0, short_funding_rate_bps=8.0)
    headwind = model.assess(long_funding_rate_bps=8.0, short_funding_rate_bps=-4.0)

    assert tailwind.score_adjustment_bps > 0.0
    assert tailwind.carry_cost_bps == 0.0
    assert headwind.score_adjustment_bps < 0.0
    assert headwind.carry_cost_bps > 0.0


def test_liquidity_score_tiers_depth_instead_of_flat_pass() -> None:
    gate = LiquidityAndVenueHealthGate()

    assert gate.liquidity_score(
        capacity_quote=25.0,
        entry_notional_quote=20.0,
    ) == pytest.approx(0.25)
    assert gate.liquidity_score(
        capacity_quote=50.0,
        entry_notional_quote=20.0,
    ) == pytest.approx(0.50)
    assert gate.liquidity_score(
        capacity_quote=100.0,
        entry_notional_quote=20.0,
    ) == pytest.approx(0.75)
    assert gate.liquidity_score(
        capacity_quote=500.0,
        entry_notional_quote=20.0,
    ) == pytest.approx(1.00)
    assert gate.liquidity_score(
        capacity_quote=34.0,
        entry_notional_quote=20.0,
    ) < gate.liquidity_score(
        capacity_quote=77.0,
        entry_notional_quote=20.0,
    )


def test_spread_ranker_takes_top_candidates_without_symbol_conflicts() -> None:
    ranker = SpreadRanker(max_candidates=2)
    ranked = ranker.rank(
        [
            _candidate(candidate_id="low", symbol="BTCUSDT", score=8.0),
            _candidate(candidate_id="best-btc", symbol="BTCUSDT", score=14.0),
            _candidate(candidate_id="best-eth", symbol="ETHUSDT", score=11.0),
        ]
    )

    assert [c.candidate_id for c in ranked] == ["best-btc", "best-eth"]
    assert all(c.rank_reason for c in ranked)


def test_spread_ranker_defaults_to_top_ten_with_symbol_dedup() -> None:
    ranker = SpreadRanker(max_candidates=0)

    ranked = ranker.rank(
        [
            _candidate(candidate_id=f"eth-{i}", symbol=f"ETH{i}USDT", score=float(i))
            for i in range(12)
        ]
        + [
            _candidate(candidate_id="low-btc", symbol="BTCUSDT", score=100.0),
            _candidate(candidate_id="best-btc", symbol="BTCUSDT", score=101.0),
        ]
    )

    assert len(ranked) == 10
    assert "best-btc" in [c.candidate_id for c in ranked]
    assert "low-btc" not in [c.candidate_id for c in ranked]
    assert len({c.symbol for c in ranked}) == len(ranked)


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"signal_missing": True, "degraded_ticks": 1}, DegradationState.OBSERVE_DEGRADED),
        ({"signal_missing": True, "degraded_ticks": 4}, DegradationState.PROTECTIVE_EXIT_READY),
        ({"venue_unavailable": True, "degraded_ticks": 8}, DegradationState.FORCED_EXIT),
        ({"position_truth_confirmed": False}, DegradationState.RECOVERY_REQUIRED),
    ],
)
def test_exit_risk_classifier_separates_recoverable_and_forced_exit(kwargs, expected) -> None:
    classifier = ExitRiskClassifier(observe_ticks=2, protective_ticks=5)

    assert classifier.classify(**kwargs) == expected
