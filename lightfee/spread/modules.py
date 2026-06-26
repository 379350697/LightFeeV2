"""Composable spread-reversion screening modules."""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from enum import StrEnum
from typing import Iterable

from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.models import SpreadReversionCandidate


_FUNDING_INTERVAL_MS = 8 * 60 * 60 * 1000


class DegradationState(StrEnum):
    HEALTHY = "healthy"
    OBSERVE_DEGRADED = "observe_degraded"
    PROTECTIVE_EXIT_READY = "protective_exit_ready"
    FORCED_EXIT = "forced_exit"
    RECOVERY_REQUIRED = "recovery_required"


@dataclass(frozen=True)
class FairPriceAssessment:
    fair_price: float
    premium_bps: float
    confidence: float
    eligible: bool


@dataclass(frozen=True)
class MeanReversionAssessment:
    quality: float
    half_life_ms: int
    hold_time_hint_ms: int
    entry_allowed: bool


@dataclass(frozen=True)
class FundingAwarenessAssessment:
    carry_cost_bps: float
    score_adjustment_bps: float


@dataclass(frozen=True)
class CostAssessment:
    gross_edge_bps: float
    fee_bps: float
    funding_carry_bps: float
    net_edge_bps: float


class CandidateSource:
    """Build raw quote pairs without deciding whether they are tradeable."""

    def iter_pairs(
        self,
        quotes: dict[str, QuoteSnapshot],
        symbols: Iterable[str],
    ) -> list[tuple[QuoteSnapshot, QuoteSnapshot]]:
        pairs: list[tuple[QuoteSnapshot, QuoteSnapshot]] = []
        for symbol in symbols:
            symbol_quotes = [
                q
                for q in quotes.values()
                if str(getattr(q, "symbol", "") or "").upper() == str(symbol).upper()
            ]
            for i, left in enumerate(symbol_quotes):
                for right in symbol_quotes[i + 1:]:
                    pairs.append((left, right))
        return pairs


class FairPriceModel:
    """Median fair-price anchor used to filter isolated venue prices."""

    def __init__(
        self,
        *,
        max_venue_premium_bps: float = 150.0,
        min_venues_for_filter: int = 3,
    ) -> None:
        self.max_venue_premium_bps = max(float(max_venue_premium_bps or 0.0), 0.0)
        self.min_venues_for_filter = max(int(min_venues_for_filter or 0), 1)

    def assess(
        self,
        quotes: Iterable[QuoteSnapshot],
    ) -> dict[str, FairPriceAssessment]:
        quote_list = list(quotes)
        mids = [_mid(q) for q in quote_list]
        mids = [mid for mid in mids if mid > 0.0]
        if not mids:
            return {}
        fair_price = float(statistics.median(mids))
        confidence = 1.0 if len(mids) >= self.min_venues_for_filter else 0.5
        filtering_enabled = len(mids) >= self.min_venues_for_filter
        result: dict[str, FairPriceAssessment] = {}
        for quote in quote_list:
            mid = _mid(quote)
            venue = str(getattr(quote, "venue", "") or "").lower()
            if not venue or mid <= 0.0 or fair_price <= 0.0:
                continue
            premium_bps = ((mid - fair_price) / fair_price) * 10_000.0
            eligible = (
                not filtering_enabled
                or self.max_venue_premium_bps <= 0.0
                or abs(premium_bps) <= self.max_venue_premium_bps
            )
            result[venue] = FairPriceAssessment(
                fair_price=fair_price,
                premium_bps=premium_bps,
                confidence=confidence,
                eligible=eligible,
            )
        return result


class ZScoreSignalModel:
    """Primary spread abnormality signal."""

    def z_score(self, *, spread_mid_bps: float, mean_bps: float, std_bps: float) -> float:
        if std_bps <= 0.0:
            return 0.0
        return (spread_mid_bps - mean_bps) / std_bps


class MeanReversionQualityModel:
    """Simple OU-compatible quality gate that does not open trades by itself."""

    def __init__(
        self,
        *,
        min_std_bps: float = 0.05,
        max_half_life_ms: int = 30 * 60 * 1000,
    ) -> None:
        self.min_std_bps = max(float(min_std_bps or 0.0), 0.0)
        self.max_half_life_ms = max(int(max_half_life_ms or 0), 1)

    def assess(
        self,
        *,
        z_score: float,
        sample_count: int,
        rolling_std_bps: float,
    ) -> MeanReversionAssessment:
        if sample_count <= 1 or rolling_std_bps < self.min_std_bps:
            return MeanReversionAssessment(
                quality=0.0,
                half_life_ms=self.max_half_life_ms * 2,
                hold_time_hint_ms=self.max_half_life_ms,
                entry_allowed=False,
            )
        z_quality = min(max(abs(float(z_score or 0.0)) / 3.0, 0.0), 1.0)
        stability_quality = min(max(float(rolling_std_bps) / 2.0, 0.0), 1.0)
        quality = min(max((z_quality + stability_quality) / 2.0, 0.0), 1.0)
        half_life_ms = int(self.max_half_life_ms * (1.25 - quality))
        half_life_ms = max(min(half_life_ms, self.max_half_life_ms), 1)
        return MeanReversionAssessment(
            quality=quality,
            half_life_ms=half_life_ms,
            hold_time_hint_ms=half_life_ms,
            entry_allowed=quality > 0.0,
        )


class FundingAwarenessModel:
    """Funding carry score helper; it never flips spread legs."""

    def __init__(self, *, expected_hold_ms: int = 30 * 60 * 1000) -> None:
        self.expected_hold_ms = max(int(expected_hold_ms or 0), 0)

    def assess(
        self,
        *,
        long_funding_rate_bps: float,
        short_funding_rate_bps: float,
    ) -> FundingAwarenessAssessment:
        hold_ratio = float(self.expected_hold_ms) / _FUNDING_INTERVAL_MS
        raw_carry_bps = (
            float(long_funding_rate_bps or 0.0)
            - float(short_funding_rate_bps or 0.0)
        ) * hold_ratio
        carry_cost_bps = max(raw_carry_bps, 0.0)
        score_adjustment_bps = -raw_carry_bps
        return FundingAwarenessAssessment(
            carry_cost_bps=carry_cost_bps,
            score_adjustment_bps=score_adjustment_bps,
        )


class CostModel:
    def assess(
        self,
        *,
        executable_spread_bps: float,
        fee_bps: float,
        slippage_reserve_bps: float,
        adverse_selection_buffer_bps: float,
        funding_carry_bps: float,
    ) -> CostAssessment:
        gross_edge_bps = float(executable_spread_bps or 0.0)
        net_edge_bps = (
            gross_edge_bps
            - float(fee_bps or 0.0)
            - float(slippage_reserve_bps or 0.0)
            - float(adverse_selection_buffer_bps or 0.0)
            - float(funding_carry_bps or 0.0)
        )
        return CostAssessment(
            gross_edge_bps=gross_edge_bps,
            fee_bps=float(fee_bps or 0.0),
            funding_carry_bps=float(funding_carry_bps or 0.0),
            net_edge_bps=net_edge_bps,
        )


class LiquidityAndVenueHealthGate:
    def quote_fresh(
        self,
        long_q: QuoteSnapshot,
        short_q: QuoteSnapshot,
        *,
        now_ms: int,
        signal_ttl_ms: int,
        quote_skew_ms: int,
    ) -> bool:
        long_ts = int(getattr(long_q, "observed_at_ms", 0) or 0)
        short_ts = int(getattr(short_q, "observed_at_ms", 0) or 0)
        if long_ts <= 0 or short_ts <= 0:
            return False
        ttl = max(int(signal_ttl_ms or 0), 0)
        if ttl and (now_ms - long_ts > ttl or now_ms - short_ts > ttl):
            return False
        skew = max(int(quote_skew_ms or 0), 0)
        if skew and abs(long_ts - short_ts) > skew:
            return False
        return True

    def capacity_quote(
        self,
        long_q: QuoteSnapshot,
        short_q: QuoteSnapshot,
        fallback_notional: float,
    ) -> float:
        long_capacity = float(getattr(long_q, "ask_size", 0.0) or 0.0) * float(
            long_q.ask or 0.0
        )
        short_capacity = float(getattr(short_q, "bid_size", 0.0) or 0.0) * float(
            short_q.bid or 0.0
        )
        if long_capacity > 0.0 and short_capacity > 0.0:
            return min(long_capacity, short_capacity)
        return float(fallback_notional or 0.0)

    def liquidity_score(self, *, capacity_quote: float, entry_notional_quote: float) -> float:
        notional = float(entry_notional_quote or 0.0)
        if notional <= 0.0:
            return 0.0
        return min(max(float(capacity_quote or 0.0) / notional, 0.0), 1.0)


class SpreadRanker:
    def __init__(self, *, max_candidates: int = 0) -> None:
        self.max_candidates = max(int(max_candidates or 0), 0)

    def rank(
        self,
        candidates: Iterable[SpreadReversionCandidate],
    ) -> list[SpreadReversionCandidate]:
        ranked = sorted(
            candidates,
            key=lambda c: (
                float(c.score),
                float(c.net_edge_bps),
                float(c.z_score),
            ),
            reverse=True,
        )
        selected: list[SpreadReversionCandidate] = []
        used_symbols: set[str] = set()
        for candidate in ranked:
            symbol = str(candidate.symbol or "").upper()
            if self.max_candidates and symbol in used_symbols:
                continue
            used_symbols.add(symbol)
            if not candidate.rank_reason:
                candidate = candidate.__class__(
                    **{
                        **candidate.__dict__,
                        "rank_reason": (
                            f"score={candidate.score:.2f};"
                            f"net_edge_bps={candidate.net_edge_bps:.2f};"
                            f"z_score={candidate.z_score:.2f}"
                        ),
                    }
                )
            selected.append(candidate)
            if self.max_candidates and len(selected) >= self.max_candidates:
                break
        return selected


class ExecutionPolicy:
    TAKER_TAKER = "taker_taker"
    MAKER_TAKER = "maker_taker"


class ExitRiskClassifier:
    def __init__(self, *, observe_ticks: int = 2, protective_ticks: int = 5) -> None:
        self.observe_ticks = max(int(observe_ticks or 0), 0)
        self.protective_ticks = max(int(protective_ticks or 0), self.observe_ticks)

    def classify(
        self,
        *,
        signal_missing: bool = False,
        degraded_ticks: int = 0,
        venue_unavailable: bool = False,
        position_truth_confirmed: bool = True,
        hedge_delta_quote: float = 0.0,
    ) -> DegradationState:
        if not position_truth_confirmed or abs(float(hedge_delta_quote or 0.0)) > 0.0:
            return DegradationState.RECOVERY_REQUIRED
        if venue_unavailable and degraded_ticks >= self.protective_ticks:
            return DegradationState.FORCED_EXIT
        if signal_missing or degraded_ticks > 0 or venue_unavailable:
            if degraded_ticks > self.observe_ticks:
                return DegradationState.PROTECTIVE_EXIT_READY
            return DegradationState.OBSERVE_DEGRADED
        return DegradationState.HEALTHY


def _mid(q: QuoteSnapshot) -> float:
    bid = float(getattr(q, "bid", 0.0) or 0.0)
    ask = float(getattr(q, "ask", 0.0) or 0.0)
    if bid <= 0.0 or ask <= 0.0:
        return 0.0
    return (bid + ask) / 2.0
