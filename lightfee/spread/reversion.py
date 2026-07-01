"""Continuous spread-reversion signal generation."""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from lightfee.config.schema import AppConfig, StrategyConfig, VenueConfig
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.models import SpreadReversionCandidate
from lightfee.spread.modules import (
    CandidateSource,
    CostModel,
    DegradationState,
    FairPriceAssessment,
    FairPriceModel,
    FundingAwarenessModel,
    LiquidityAndVenueHealthGate,
    MeanReversionQualityModel,
    SpreadRanker,
    ZScoreSignalModel,
)


@dataclass(frozen=True)
class SpreadReversionConfig:
    min_samples: int = 20
    entry_z: float = 2.0
    min_net_edge_bps: float = 5.0
    signal_ttl_ms: int = 1000
    quote_skew_ms: int = 250
    live_notional_quote: float = 20.0
    max_gross_quote: float = 50.0
    taker_fee_bps_by_venue: dict[str, float] = field(default_factory=dict)
    slippage_reserve_bps: float = 2.0
    adverse_selection_buffer_bps: float = 1.0
    expected_hold_ms: int = 30 * 60 * 1000
    fair_price_max_venue_premium_bps: float = 150.0
    fair_price_min_venues: int = 3
    min_fair_price_confidence: float = 1.0
    single_venue_dislocation_enabled: bool = False
    single_venue_dislocation_min_anchor_venues: int = 3
    min_liquidity_capacity_ratio: float = 1.25
    min_history_ms: int = 300_000
    mean_reversion_min_std_bps: float = 0.05
    mean_reversion_max_half_life_ms: int = 30 * 60 * 1000
    ranker_max_candidates: int = 10
    score_liquidity_weight: float = 8.0
    score_z_cap: float = 5.0
    liquidity_small_quote: float = 50.0
    liquidity_medium_quote: float = 100.0
    liquidity_large_quote: float = 500.0
    liquidity_small_penalty_bps: float = 60.0
    liquidity_medium_penalty_bps: float = 30.0
    liquidity_sublarge_penalty_bps: float = 10.0

    @classmethod
    def from_app_config(cls, config: AppConfig) -> "SpreadReversionConfig":
        strategy = config.strategy
        return cls(
            min_samples=int(getattr(strategy, "spread_min_samples", cls.min_samples) or 0),
            entry_z=float(getattr(strategy, "spread_entry_z", cls.entry_z) or 0.0),
            min_net_edge_bps=float(
                getattr(strategy, "spread_min_net_edge_bps", cls.min_net_edge_bps) or 0.0
            ),
            signal_ttl_ms=int(
                getattr(strategy, "spread_signal_ttl_ms", cls.signal_ttl_ms) or 0
            ),
            quote_skew_ms=int(
                getattr(strategy, "spread_quote_skew_ms", cls.quote_skew_ms) or 0
            ),
            live_notional_quote=float(
                getattr(strategy, "spread_live_notional_quote", cls.live_notional_quote)
                or 0.0
            ),
            max_gross_quote=float(
                getattr(strategy, "spread_max_gross_quote", cls.max_gross_quote) or 0.0
            ),
            taker_fee_bps_by_venue=_fee_map(config.venues),
            slippage_reserve_bps=float(
                getattr(strategy, "spread_slippage_reserve_bps", cls.slippage_reserve_bps)
                or 0.0
            ),
            adverse_selection_buffer_bps=float(
                getattr(
                    strategy,
                    "spread_adverse_selection_buffer_bps",
                    cls.adverse_selection_buffer_bps,
                )
                or 0.0
            ),
            expected_hold_ms=int(
                getattr(strategy, "spread_expected_hold_ms", cls.expected_hold_ms) or 0
            ),
            fair_price_max_venue_premium_bps=float(
                getattr(
                    strategy,
                    "spread_fair_price_max_venue_premium_bps",
                    cls.fair_price_max_venue_premium_bps,
                )
                or 0.0
            ),
            fair_price_min_venues=int(
                getattr(strategy, "spread_fair_price_min_venues", cls.fair_price_min_venues)
                or 0
            ),
            min_fair_price_confidence=float(
                getattr(
                    strategy,
                    "spread_min_fair_price_confidence",
                    cls.min_fair_price_confidence,
                )
                or 0.0
            ),
            single_venue_dislocation_enabled=bool(
                getattr(
                    strategy,
                    "spread_single_venue_dislocation_enabled",
                    cls.single_venue_dislocation_enabled,
                )
            ),
            single_venue_dislocation_min_anchor_venues=int(
                getattr(
                    strategy,
                    "spread_single_venue_dislocation_min_anchor_venues",
                    cls.single_venue_dislocation_min_anchor_venues,
                )
                or 0
            ),
            min_liquidity_capacity_ratio=float(
                getattr(
                    strategy,
                    "spread_min_liquidity_capacity_ratio",
                    cls.min_liquidity_capacity_ratio,
                )
                or 0.0
            ),
            min_history_ms=int(
                getattr(strategy, "spread_min_history_ms", cls.min_history_ms) or 0
            ),
            mean_reversion_min_std_bps=float(
                getattr(
                    strategy,
                    "spread_mean_reversion_min_std_bps",
                    cls.mean_reversion_min_std_bps,
                )
                or 0.0
            ),
            mean_reversion_max_half_life_ms=int(
                getattr(
                    strategy,
                    "spread_mean_reversion_max_half_life_ms",
                    cls.mean_reversion_max_half_life_ms,
                )
                or 0
            ),
            ranker_max_candidates=int(
                getattr(strategy, "spread_ranker_max_candidates", cls.ranker_max_candidates)
                or 0
            ),
            score_liquidity_weight=float(
                getattr(
                    strategy,
                    "spread_score_liquidity_weight",
                    cls.score_liquidity_weight,
                )
                or 0.0
            ),
            score_z_cap=float(
                getattr(strategy, "spread_score_z_cap", cls.score_z_cap) or 0.0
            ),
            liquidity_small_quote=float(
                getattr(
                    strategy,
                    "spread_liquidity_small_quote",
                    cls.liquidity_small_quote,
                )
                or 0.0
            ),
            liquidity_medium_quote=float(
                getattr(
                    strategy,
                    "spread_liquidity_medium_quote",
                    cls.liquidity_medium_quote,
                )
                or 0.0
            ),
            liquidity_large_quote=float(
                getattr(
                    strategy,
                    "spread_liquidity_large_quote",
                    cls.liquidity_large_quote,
                )
                or 0.0
            ),
            liquidity_small_penalty_bps=float(
                getattr(
                    strategy,
                    "spread_liquidity_small_penalty_bps",
                    cls.liquidity_small_penalty_bps,
                )
                or 0.0
            ),
            liquidity_medium_penalty_bps=float(
                getattr(
                    strategy,
                    "spread_liquidity_medium_penalty_bps",
                    cls.liquidity_medium_penalty_bps,
                )
                or 0.0
            ),
            liquidity_sublarge_penalty_bps=float(
                getattr(
                    strategy,
                    "spread_liquidity_sublarge_penalty_bps",
                    cls.liquidity_sublarge_penalty_bps,
                )
                or 0.0
            ),
        )

    @classmethod
    def from_strategy(
        cls,
        strategy: StrategyConfig,
        venues: list[VenueConfig] | None = None,
    ) -> "SpreadReversionConfig":
        app_config = AppConfig(strategy=strategy, venues=list(venues or []))
        return cls.from_app_config(app_config)


@dataclass(frozen=True)
class SpreadStatsSnapshot:
    sample_count: int
    mean_bps: float
    std_bps: float
    first_observed_ms: int = 0
    last_observed_ms: int = 0

    @property
    def history_age_ms(self) -> int:
        if self.first_observed_ms <= 0 or self.last_observed_ms <= 0:
            return 0
        return max(self.last_observed_ms - self.first_observed_ms, 0)


@dataclass
class _WelfordState:
    count: int = 0
    mean: float = 0.0
    m2: float = 0.0
    first_observed_ms: int = 0
    last_observed_ms: int = 0

    def update(self, value: float, *, observed_at_ms: int = 0) -> SpreadStatsSnapshot:
        observed_ms = int(observed_at_ms or 0)
        if observed_ms > 0:
            if self.first_observed_ms <= 0:
                self.first_observed_ms = observed_ms
            self.last_observed_ms = observed_ms
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.m2 += delta * delta2
        return self.snapshot()

    def snapshot(self) -> SpreadStatsSnapshot:
        if self.count < 2:
            std = 0.0
        else:
            std = math.sqrt(max(self.m2 / (self.count - 1), 0.0))
        return SpreadStatsSnapshot(
            sample_count=self.count,
            mean_bps=self.mean,
            std_bps=std,
            first_observed_ms=self.first_observed_ms,
            last_observed_ms=self.last_observed_ms,
        )


class SpreadStatsTracker:
    """Online per symbol/venue-pair spread statistics."""

    def __init__(self) -> None:
        self._states: dict[tuple[str, str, str], _WelfordState] = {}

    def update(
        self,
        symbol: str,
        long_venue: str,
        short_venue: str,
        spread_mid_bps: float,
        *,
        observed_at_ms: int = 0,
    ) -> SpreadStatsSnapshot:
        state = self._states.setdefault(
            _key(symbol, long_venue, short_venue),
            _WelfordState(),
        )
        return state.update(spread_mid_bps, observed_at_ms=observed_at_ms)

    def snapshot(
        self,
        symbol: str,
        long_venue: str,
        short_venue: str,
    ) -> SpreadStatsSnapshot | None:
        state = self._states.get(_key(symbol, long_venue, short_venue))
        return None if state is None else state.snapshot()


def build_spread_reversion_candidates(
    quotes: dict[str, QuoteSnapshot],
    symbols: list[str],
    *,
    tracker: SpreadStatsTracker,
    config: SpreadReversionConfig,
    now_ms: int,
) -> list[SpreadReversionCandidate]:
    candidates: list[SpreadReversionCandidate] = []
    source = CandidateSource()
    fair_price_model = FairPriceModel(
        max_venue_premium_bps=config.fair_price_max_venue_premium_bps,
        min_venues_for_filter=config.fair_price_min_venues,
    )
    zscore_model = ZScoreSignalModel()
    quality_model = MeanReversionQualityModel(
        min_std_bps=config.mean_reversion_min_std_bps,
        max_half_life_ms=config.mean_reversion_max_half_life_ms,
    )
    cost_model = CostModel()
    funding_model = FundingAwarenessModel(expected_hold_ms=config.expected_hold_ms)
    liquidity_gate = LiquidityAndVenueHealthGate()

    fair_price_by_symbol: dict[str, dict[str, FairPriceAssessment]] = {}
    for symbol in symbols:
        symbol_quotes = [
            q for q in quotes.values()
            if str(getattr(q, "symbol", "") or "").upper() == symbol.upper()
        ]
        fair_price_by_symbol[symbol.upper()] = fair_price_model.assess(symbol_quotes)
    for left, right in source.iter_pairs(quotes, symbols):
        symbol = str(getattr(left, "symbol", "") or "").upper()
        candidate = _candidate_for_pair(
            left,
            right,
            tracker,
            config,
            now_ms,
            fair_price=fair_price_by_symbol.get(symbol, {}),
            zscore_model=zscore_model,
            quality_model=quality_model,
            cost_model=cost_model,
            funding_model=funding_model,
            liquidity_gate=liquidity_gate,
        )
        if candidate is not None:
            candidates.append(candidate)
    return SpreadRanker(max_candidates=config.ranker_max_candidates).rank(candidates)


def _candidate_for_pair(
    left: QuoteSnapshot,
    right: QuoteSnapshot,
    tracker: SpreadStatsTracker,
    config: SpreadReversionConfig,
    now_ms: int,
    *,
    fair_price: dict[str, FairPriceAssessment],
    zscore_model: ZScoreSignalModel,
    quality_model: MeanReversionQualityModel,
    cost_model: CostModel,
    funding_model: FundingAwarenessModel,
    liquidity_gate: LiquidityAndVenueHealthGate,
) -> SpreadReversionCandidate | None:
    left_mid = _mid(left)
    right_mid = _mid(right)
    if left_mid <= 0.0 or right_mid <= 0.0:
        return None

    if left_mid <= right_mid:
        long_q, short_q = left, right
        long_mid, short_mid = left_mid, right_mid
    else:
        long_q, short_q = right, left
        long_mid, short_mid = right_mid, left_mid

    long_fair = fair_price.get(str(long_q.venue).lower())
    short_fair = fair_price.get(str(short_q.venue).lower())
    opportunity_label = "spread_reversion"
    screening_reasons: list[str] = []
    long_outlier = long_fair is not None and not long_fair.eligible
    short_outlier = short_fair is not None and not short_fair.eligible
    if long_outlier or short_outlier:
        if not _single_venue_dislocation_allowed(
            long_outlier=long_outlier,
            short_outlier=short_outlier,
            fair_price=fair_price,
            config=config,
        ):
            return None
        opportunity_label = "single_venue_dislocation"
        screening_reasons.append("fair_outlier_override")

    if not liquidity_gate.quote_fresh(
        long_q,
        short_q,
        now_ms=now_ms,
        signal_ttl_ms=config.signal_ttl_ms,
        quote_skew_ms=config.quote_skew_ms,
    ):
        return None

    reference_mid = (long_mid + short_mid) / 2.0
    spread_mid_bps = ((short_mid - long_mid) / reference_mid) * 10_000.0
    stats = tracker.update(
        str(long_q.symbol),
        str(long_q.venue),
        str(short_q.venue),
        spread_mid_bps,
        observed_at_ms=now_ms,
    )
    is_single_venue_dislocation = opportunity_label == "single_venue_dislocation"
    if not is_single_venue_dislocation:
        if stats.sample_count < max(config.min_samples, 1):
            return None
        if config.min_history_ms > 0 and stats.history_age_ms < config.min_history_ms:
            return None
    z_score = zscore_model.z_score(
        spread_mid_bps=spread_mid_bps,
        mean_bps=stats.mean_bps,
        std_bps=stats.std_bps,
    )
    if not is_single_venue_dislocation and z_score < config.entry_z:
        return None
    quality = quality_model.assess(
        z_score=z_score,
        sample_count=stats.sample_count,
        rolling_std_bps=stats.std_bps,
    )
    if not is_single_venue_dislocation and not quality.entry_allowed and config.entry_z > 0.0:
        return None

    executable_spread_bps = 0.0
    if short_q.bid > 0.0 and long_q.ask > 0.0:
        executable_spread_bps = ((short_q.bid - long_q.ask) / reference_mid) * 10_000.0
    fee_bps = _fee_bps(config, long_q.venue) + _fee_bps(config, short_q.venue)
    funding = funding_model.assess(
        long_funding_rate_bps=float(getattr(long_q, "funding_rate_bps", 0.0) or 0.0),
        short_funding_rate_bps=float(getattr(short_q, "funding_rate_bps", 0.0) or 0.0),
    )
    cost = cost_model.assess(
        executable_spread_bps=executable_spread_bps,
        fee_bps=fee_bps,
        slippage_reserve_bps=config.slippage_reserve_bps,
        adverse_selection_buffer_bps=config.adverse_selection_buffer_bps,
        funding_carry_bps=funding.carry_cost_bps,
    )
    if cost.net_edge_bps < config.min_net_edge_bps:
        return None

    quote_skew = abs(int(long_q.observed_at_ms or 0) - int(short_q.observed_at_ms or 0))
    entry_notional = min(config.live_notional_quote, config.max_gross_quote)
    if entry_notional <= 0.0:
        return None
    liquidity_evidence_status = liquidity_gate.liquidity_evidence_status(long_q, short_q)
    capacity_quote = liquidity_gate.capacity_quote(long_q, short_q, entry_notional)
    required_capacity = entry_notional * max(config.min_liquidity_capacity_ratio, 0.0)
    if required_capacity > 0.0 and capacity_quote < required_capacity:
        return None
    liquidity_score = liquidity_gate.liquidity_score(
        capacity_quote=capacity_quote,
        entry_notional_quote=entry_notional,
    )
    if long_fair is not None:
        fair_price_value = long_fair.fair_price
    elif short_fair is not None:
        fair_price_value = short_fair.fair_price
    else:
        fair_price_value = reference_mid
    venue_premium_bps = (
        (abs(long_fair.premium_bps) + abs(short_fair.premium_bps)) / 2.0
        if long_fair is not None and short_fair is not None
        else 0.0
    )
    fair_price_confidence = max(
        long_fair.confidence if long_fair is not None else 0.0,
        short_fair.confidence if short_fair is not None else 0.0,
    )
    if fair_price_confidence < config.min_fair_price_confidence:
        return None
    mean_quality = quality.quality if quality.entry_allowed else 0.0
    effective_z_score = _effective_z_score(z_score, config)
    liquidity_penalty_bps = _liquidity_rank_penalty_bps(capacity_quote, config)
    score = (
        cost.net_edge_bps
        + effective_z_score * 2.0
        + mean_quality * 5.0
        + funding.score_adjustment_bps
        + liquidity_score * config.score_liquidity_weight
        + fair_price_confidence
        - liquidity_penalty_bps
    )
    rank_reason = (
        f"score={score:.2f};net_edge_bps={cost.net_edge_bps:.2f};"
        f"z_score={z_score:.2f};effective_z_score={effective_z_score:.2f};"
        f"mean_reversion_quality={mean_quality:.2f};"
        f"liquidity_score={liquidity_score:.2f};"
        f"liquidity_penalty_bps={liquidity_penalty_bps:.2f}"
    )
    return SpreadReversionCandidate(
        candidate_id=_candidate_id(str(long_q.symbol), str(long_q.venue), str(short_q.venue)),
        symbol=str(long_q.symbol).upper(),
        long_venue=str(long_q.venue).lower(),
        short_venue=str(short_q.venue).lower(),
        spread_mid_bps=spread_mid_bps,
        executable_spread_bps=executable_spread_bps,
        rolling_mean_bps=stats.mean_bps,
        rolling_std_bps=stats.std_bps,
        z_score=z_score,
        net_edge_bps=cost.net_edge_bps,
        sample_count=stats.sample_count,
        signal_ts_ms=now_ms,
        long_quote_ts_ms=int(long_q.observed_at_ms or 0),
        short_quote_ts_ms=int(short_q.observed_at_ms or 0),
        entry_notional_quote=entry_notional,
        capacity_quote=capacity_quote,
        signal_status="entry_ready",
        fee_bps=fee_bps,
        slippage_reserve_bps=config.slippage_reserve_bps,
        adverse_selection_buffer_bps=config.adverse_selection_buffer_bps,
        funding_carry_cost_bps=funding.carry_cost_bps,
        quote_skew_ms=quote_skew,
        fair_price=fair_price_value,
        venue_premium_bps=venue_premium_bps,
        fair_price_confidence=fair_price_confidence,
        mean_reversion_quality=mean_quality,
        half_life_ms=quality.half_life_ms,
        hold_time_hint_ms=quality.hold_time_hint_ms,
        gross_edge_bps=cost.gross_edge_bps,
        funding_carry_bps=funding.carry_cost_bps,
        liquidity_score=liquidity_score,
        venue_health_score=1.0,
        score=score,
        rank_reason=rank_reason,
        degradation_state=DegradationState.HEALTHY.value,
        liquidity_evidence_status=liquidity_evidence_status,
        screening_reasons=screening_reasons,
        history_age_ms=stats.history_age_ms,
        opportunity_label=opportunity_label,
    )


def _fee_map(venues: list[VenueConfig]) -> dict[str, float]:
    result: dict[str, float] = {}
    for venue in venues:
        name = str(getattr(venue, "venue", "") or "").lower()
        if not name:
            continue
        result[name] = float(getattr(venue, "taker_fee_bps", 0.0) or 0.0)
    return result


def _single_venue_dislocation_allowed(
    *,
    long_outlier: bool,
    short_outlier: bool,
    fair_price: dict[str, FairPriceAssessment],
    config: SpreadReversionConfig,
) -> bool:
    if not config.single_venue_dislocation_enabled:
        return False
    if long_outlier == short_outlier:
        return False
    anchor_count = sum(1 for assessment in fair_price.values() if assessment.eligible)
    required = max(int(config.single_venue_dislocation_min_anchor_venues or 0), 1)
    return anchor_count >= required


def _fee_bps(config: SpreadReversionConfig, venue: str) -> float:
    return float(config.taker_fee_bps_by_venue.get(str(venue).lower(), 0.0) or 0.0)


def _effective_z_score(z_score: float, config: SpreadReversionConfig) -> float:
    score = max(float(z_score or 0.0), 0.0)
    cap = float(config.score_z_cap or 0.0)
    if cap <= 0.0:
        return score
    return min(score, cap)


def _liquidity_rank_penalty_bps(
    capacity_quote: float,
    config: SpreadReversionConfig,
) -> float:
    capacity = max(float(capacity_quote or 0.0), 0.0)
    small_quote = max(float(config.liquidity_small_quote or 0.0), 0.0)
    medium_quote = max(float(config.liquidity_medium_quote or 0.0), small_quote)
    large_quote = max(float(config.liquidity_large_quote or 0.0), medium_quote)
    if small_quote > 0.0 and capacity < small_quote:
        return max(float(config.liquidity_small_penalty_bps or 0.0), 0.0)
    if medium_quote > 0.0 and capacity < medium_quote:
        return max(float(config.liquidity_medium_penalty_bps or 0.0), 0.0)
    if large_quote > 0.0 and capacity < large_quote:
        return max(float(config.liquidity_sublarge_penalty_bps or 0.0), 0.0)
    return 0.0


def _candidate_id(symbol: str, long_venue: str, short_venue: str) -> str:
    return f"spread:{symbol.upper()}:{long_venue.lower()}->{short_venue.lower()}"


def _key(symbol: str, long_venue: str, short_venue: str) -> tuple[str, str, str]:
    return (symbol.upper(), long_venue.lower(), short_venue.lower())


def _mid(q: QuoteSnapshot) -> float:
    bid = float(getattr(q, "bid", 0.0) or 0.0)
    ask = float(getattr(q, "ask", 0.0) or 0.0)
    if bid <= 0.0 or ask <= 0.0:
        return 0.0
    return (bid + ask) / 2.0
