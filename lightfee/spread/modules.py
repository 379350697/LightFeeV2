"""Composable spread-reversion screening modules."""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from enum import StrEnum
from math import isfinite
from typing import Iterable

from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.models import SpreadReversionCandidate


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
    funding_edge_bps: float
    carry_cost_bps: float
    score_adjustment_bps: float
    first_funding_timestamp_ms: int
    economics_complete: bool


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
        ar1_phi: float | None = None,
        half_life_ms: int | None = None,
    ) -> MeanReversionAssessment:
        if sample_count <= 1 or rolling_std_bps < self.min_std_bps:
            return MeanReversionAssessment(
                quality=0.0,
                half_life_ms=self.max_half_life_ms * 2,
                hold_time_hint_ms=self.max_half_life_ms,
                entry_allowed=False,
            )
        if ar1_phi is not None and not 0.0 < float(ar1_phi) < 1.0:
            return MeanReversionAssessment(
                quality=0.0,
                half_life_ms=self.max_half_life_ms * 2,
                hold_time_hint_ms=self.max_half_life_ms,
                entry_allowed=False,
            )
        resolved_half_life = int(half_life_ms or 0)
        if resolved_half_life <= 0:
            # Backward-compatible fallback for callers that do not yet provide
            # a residual AR(1) fit. The v2 signal engine always does.
            z_quality = min(max(abs(float(z_score or 0.0)) / 3.0, 0.0), 1.0)
            stability_quality = min(max(float(rolling_std_bps) / 2.0, 0.0), 1.0)
            quality = min(max((z_quality + stability_quality) / 2.0, 0.0), 1.0)
            resolved_half_life = int(self.max_half_life_ms * (1.25 - quality))
        else:
            z_quality = min(max(abs(float(z_score or 0.0)) / 3.0, 0.0), 1.0)
            speed_quality = min(
                max(1.0 - resolved_half_life / self.max_half_life_ms, 0.0), 1.0
            )
            quality = (z_quality + speed_quality) / 2.0
        if resolved_half_life > self.max_half_life_ms:
            return MeanReversionAssessment(
                quality=0.0,
                half_life_ms=resolved_half_life,
                hold_time_hint_ms=self.max_half_life_ms,
                entry_allowed=False,
            )
        resolved_half_life = max(resolved_half_life, 1)
        return MeanReversionAssessment(
            quality=quality,
            half_life_ms=resolved_half_life,
            hold_time_hint_ms=resolved_half_life,
            entry_allowed=quality > 0.0 and resolved_half_life <= self.max_half_life_ms,
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
        now_ms: int,
        long_funding_timestamp_ms: int,
        short_funding_timestamp_ms: int,
        long_funding_interval_ms: int,
        short_funding_interval_ms: int,
    ) -> FundingAwarenessAssessment:
        """Assess only funding settlements actually scheduled inside the hold.

        There is no fixed eight-hour assumption.  If a hold would cross a
        second settlement whose rate is not forecast, the economics are
        incomplete and the signal path must fail closed rather than extending
        today's rate into an unknown interval.
        """

        long_payment, long_complete = self._leg_payment(
            rate_bps=long_funding_rate_bps,
            next_timestamp_ms=long_funding_timestamp_ms,
            interval_ms=long_funding_interval_ms,
            now_ms=now_ms,
            side="long",
        )
        short_payment, short_complete = self._leg_payment(
            rate_bps=short_funding_rate_bps,
            next_timestamp_ms=short_funding_timestamp_ms,
            interval_ms=short_funding_interval_ms,
            now_ms=now_ms,
            side="short",
        )
        funding_edge_bps = long_payment + short_payment
        known_timestamps = [
            timestamp
            for timestamp in (
                max(int(long_funding_timestamp_ms or 0), 0),
                max(int(short_funding_timestamp_ms or 0), 0),
            )
            if timestamp > 0
        ]
        first_timestamp = min(known_timestamps) if long_complete and short_complete and known_timestamps else 0
        return FundingAwarenessAssessment(
            funding_edge_bps=funding_edge_bps,
            carry_cost_bps=max(-funding_edge_bps, 0.0),
            score_adjustment_bps=funding_edge_bps,
            first_funding_timestamp_ms=first_timestamp,
            economics_complete=long_complete and short_complete,
        )

    def _leg_payment(
        self,
        *,
        rate_bps: float,
        next_timestamp_ms: int,
        interval_ms: int,
        now_ms: int,
        side: str,
    ) -> tuple[float, bool]:
        if self.expected_hold_ms <= 0:
            return 0.0, True
        now = max(int(now_ms or 0), 0)
        next_timestamp = max(int(next_timestamp_ms or 0), 0)
        interval = max(int(interval_ms or 0), 0)
        if now <= 0 or next_timestamp <= now or interval <= 0:
            return 0.0, False
        hold = self.expected_hold_ms
        delay = next_timestamp - now
        if delay > hold:
            return 0.0, True
        settlement_count = 1 + max((hold - delay) // interval, 0)
        if settlement_count > 1:
            return 0.0, False
        try:
            rate = float(rate_bps or 0.0)
        except (TypeError, ValueError):
            return 0.0, False
        if not isfinite(rate):
            return 0.0, False
        return (-rate if side == "long" else rate), True


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
        for quote in (long_q, short_q):
            try:
                bid = float(getattr(quote, "bid", 0.0) or 0.0)
                ask = float(getattr(quote, "ask", 0.0) or 0.0)
                bid_size = float(getattr(quote, "bid_size", 0.0) or 0.0)
                ask_size = float(getattr(quote, "ask_size", 0.0) or 0.0)
            except (TypeError, ValueError):
                return False
            if (
                not all(isfinite(value) for value in (bid, ask, bid_size, ask_size))
                or bid <= 0.0
                or ask <= 0.0
                or bid_size < 0.0
                or ask_size < 0.0
            ):
                return False
        try:
            long_ts = int(getattr(long_q, "observed_at_ms", 0) or 0)
            short_ts = int(getattr(short_q, "observed_at_ms", 0) or 0)
        except (TypeError, ValueError, OverflowError):
            return False
        if long_ts <= 0 or short_ts <= 0:
            return False
        # A source clock ahead of the signal clock cannot prove quote
        # freshness.  Treat it as unknown instead of accepting a negative age.
        if long_ts > now_ms or short_ts > now_ms:
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
        return 0.0

    def liquidity_evidence_status(
        self,
        long_q: QuoteSnapshot,
        short_q: QuoteSnapshot,
    ) -> str:
        long_has_size = float(getattr(long_q, "ask_size", 0.0) or 0.0) > 0.0
        short_has_size = float(getattr(short_q, "bid_size", 0.0) or 0.0) > 0.0
        if not long_has_size or not short_has_size:
            return "missing_top_book_size"
        return "top_book_size_available"

    def liquidity_score(self, *, capacity_quote: float, entry_notional_quote: float) -> float:
        notional = float(entry_notional_quote or 0.0)
        if notional <= 0.0:
            return 0.0
        capacity = max(float(capacity_quote or 0.0), 0.0)
        ratio = capacity / notional
        tiers = (
            (0.0, 0.0),
            (1.25, 0.25),
            (2.5, 0.5),
            (5.0, 0.75),
            (25.0, 1.0),
        )
        for (lower_ratio, lower_score), (upper_ratio, upper_score) in zip(
            tiers,
            tiers[1:],
        ):
            if ratio <= upper_ratio:
                span = upper_ratio - lower_ratio
                if span <= 0.0:
                    return upper_score
                progress = (ratio - lower_ratio) / span
                return lower_score + progress * (upper_score - lower_score)
        return 1.0


class SpreadRanker:
    def __init__(
        self,
        *,
        max_candidates: int = 0,
        rank_by_capital_efficiency: bool = False,
    ) -> None:
        configured = int(max_candidates or 0)
        self.max_candidates = configured if configured > 0 else 10
        self.rank_by_capital_efficiency = rank_by_capital_efficiency is True

    def rank(
        self,
        candidates: Iterable[SpreadReversionCandidate],
    ) -> list[SpreadReversionCandidate]:
        # ``ranking_edge_bps`` is the single economics contract's conservative
        # decision edge.  Statistical quality and liquidity are valuable only
        # after two candidates have the same economic headroom; allowing an
        # arbitrary quality score to outrank it would reintroduce a second,
        # incompatible profit formula at the final selection boundary.
        primary_edge = (
            (
                lambda c: _finite_rank_value(
                    c.risk_adjusted_edge_per_capital_hour_bps
                )
            )
            if self.rank_by_capital_efficiency
            else (lambda c: _finite_rank_value(c.ranking_edge_bps))
        )
        ranked = sorted(
            candidates,
            key=lambda c: (
                primary_edge(c),
                _finite_rank_value(c.expected_net_edge_bps),
                _finite_rank_value(c.score),
                _finite_rank_value(abs(c.z_score)),
            ),
            reverse=True,
        )
        selected: list[SpreadReversionCandidate] = []
        used_symbols: set[str] = set()
        for candidate in ranked:
            symbol = str(candidate.symbol or "").upper()
            if symbol in used_symbols:
                continue
            used_symbols.add(symbol)
            if not candidate.rank_reason:
                candidate = candidate.__class__(
                    **{
                        **candidate.__dict__,
                        "rank_reason": (
                            f"ranking_edge_bps={candidate.ranking_edge_bps:.2f};"
                            f"expected_net_edge_bps={candidate.expected_net_edge_bps:.2f};"
                            f"score={candidate.score:.2f};"
                            f"z_score={candidate.z_score:.2f}"
                        ),
                    }
                )
            selected.append(candidate)
            if len(selected) >= self.max_candidates:
                break
        return selected


def _finite_rank_value(value: object) -> float:
    """Keep malformed research payloads from winning a ranking by accident."""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return float("-inf")
    return numeric if isfinite(numeric) else float("-inf")


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
    if not isfinite(bid) or not isfinite(ask) or bid <= 0.0 or ask <= 0.0:
        return 0.0
    return (bid + ask) / 2.0
