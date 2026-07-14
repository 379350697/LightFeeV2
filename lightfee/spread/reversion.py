"""Signed-basis, no-look-ahead spread-reversion signal generation."""

from __future__ import annotations

import math
import statistics
from collections import deque
from dataclasses import dataclass, field
from itertools import combinations
from typing import Deque

from lightfee.config.schema import AppConfig, StrategyConfig, VenueConfig
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.models import SpreadReversionCandidate
from lightfee.spread.modules import (
    DegradationState,
    FairPriceAssessment,
    FairPriceModel,
    FundingAwarenessModel,
    LiquidityAndVenueHealthGate,
    MeanReversionQualityModel,
    SpreadRanker,
    ZScoreSignalModel,
)
from lightfee.strategy.economics import build_edge_breakdown
from lightfee.strategy.fee_evidence import FeeEvidenceBook, effective_fee_maps


@dataclass(frozen=True)
class SpreadReversionConfig:
    min_samples: int = 120
    entry_z: float = 2.0
    exit_z: float = 0.5
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
    min_executable_spread_bps: float = 0.0
    max_executable_spread_bps: float = 0.0
    dynamic_net_edge_enabled: bool = False
    min_gross_profit_multiple: float = 1.0
    min_profit_buffer_bps: float = 0.0
    rank_by_capital_efficiency: bool = False
    volatility_high_std_bps: float = 10.0
    account_fee_evidence: FeeEvidenceBook | None = None
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
    stats_window_ms: int = 6 * 60 * 60 * 1000
    stats_max_samples: int = 7200
    stats_short_window_ms: int = 15 * 60 * 1000
    structural_break_sigma: float = 3.0
    structural_break_consecutive: int = 5
    structural_break_cooldown_ms: int = 30 * 60 * 1000
    model_epoch: str = "v2_signed_reversion"

    @classmethod
    def from_app_config(
        cls,
        config: AppConfig,
        *,
        fee_evidence: FeeEvidenceBook | None = None,
    ) -> "SpreadReversionConfig":
        s = config.strategy
        taker_fees, _ = effective_fee_maps(
            _fee_map(config.venues),
            _maker_fee_map(config.venues),
            fee_evidence,
        )
        return cls(
            min_samples=s.spread_min_samples,
            entry_z=s.spread_entry_z,
            exit_z=s.spread_exit_z,
            min_net_edge_bps=s.spread_min_net_edge_bps,
            signal_ttl_ms=s.spread_signal_ttl_ms,
            quote_skew_ms=s.spread_quote_skew_ms,
            live_notional_quote=s.spread_live_notional_quote,
            max_gross_quote=s.spread_max_gross_quote,
            taker_fee_bps_by_venue=taker_fees,
            slippage_reserve_bps=s.spread_slippage_reserve_bps,
            adverse_selection_buffer_bps=s.spread_adverse_selection_buffer_bps,
            expected_hold_ms=s.spread_expected_hold_ms,
            fair_price_max_venue_premium_bps=s.spread_fair_price_max_venue_premium_bps,
            fair_price_min_venues=s.spread_fair_price_min_venues,
            min_fair_price_confidence=s.spread_min_fair_price_confidence,
            single_venue_dislocation_enabled=s.spread_single_venue_dislocation_enabled,
            single_venue_dislocation_min_anchor_venues=s.spread_single_venue_dislocation_min_anchor_venues,
            min_liquidity_capacity_ratio=s.spread_min_liquidity_capacity_ratio,
            min_history_ms=s.spread_min_history_ms,
            min_executable_spread_bps=s.spread_min_executable_spread_bps,
            max_executable_spread_bps=s.spread_max_executable_spread_bps,
            dynamic_net_edge_enabled=s.spread_dynamic_net_edge_enabled,
            min_gross_profit_multiple=s.spread_min_gross_profit_multiple,
            min_profit_buffer_bps=s.spread_min_profit_buffer_bps,
            rank_by_capital_efficiency=s.spread_rank_by_capital_efficiency,
            volatility_high_std_bps=s.spread_volatility_high_std_bps,
            account_fee_evidence=fee_evidence,
            mean_reversion_min_std_bps=s.spread_mean_reversion_min_std_bps,
            mean_reversion_max_half_life_ms=s.spread_mean_reversion_max_half_life_ms,
            ranker_max_candidates=s.spread_ranker_max_candidates,
            score_liquidity_weight=s.spread_score_liquidity_weight,
            score_z_cap=s.spread_score_z_cap,
            liquidity_small_quote=s.spread_liquidity_small_quote,
            liquidity_medium_quote=s.spread_liquidity_medium_quote,
            liquidity_large_quote=s.spread_liquidity_large_quote,
            liquidity_small_penalty_bps=s.spread_liquidity_small_penalty_bps,
            liquidity_medium_penalty_bps=s.spread_liquidity_medium_penalty_bps,
            liquidity_sublarge_penalty_bps=s.spread_liquidity_sublarge_penalty_bps,
            stats_window_ms=s.spread_stats_window_ms,
            stats_max_samples=s.spread_stats_max_samples,
            stats_short_window_ms=s.spread_stats_short_window_ms,
            structural_break_sigma=s.spread_structural_break_sigma,
            structural_break_consecutive=s.spread_structural_break_consecutive,
            structural_break_cooldown_ms=s.spread_structural_break_cooldown_ms,
            model_epoch=s.spread_model_epoch,
        )

    @classmethod
    def from_strategy(
        cls, strategy: StrategyConfig, venues: list[VenueConfig] | None = None
    ) -> "SpreadReversionConfig":
        return cls.from_app_config(AppConfig(strategy=strategy, venues=list(venues or [])))


@dataclass(frozen=True)
class SpreadStatsSnapshot:
    sample_count: int
    mean_bps: float
    std_bps: float
    first_observed_ms: int = 0
    last_observed_ms: int = 0
    median_bps: float = 0.0
    robust_scale_bps: float = 0.0
    exit_half_spread_p75_bps: float = 0.0
    ar1_phi: float | None = None
    half_life_ms: int = 0
    cooldown_until_ms: int = 0
    structural_break: bool = False

    @property
    def history_age_ms(self) -> int:
        if self.first_observed_ms <= 0 or self.last_observed_ms <= 0:
            return 0
        return max(self.last_observed_ms - self.first_observed_ms, 0)


@dataclass(frozen=True)
class _Sample:
    observed_at_ms: int
    value_bps: float
    exit_half_spread_bps: float = 0.0


@dataclass
class _RollingState:
    samples: Deque[_Sample] = field(default_factory=deque)
    cooldown_until_ms: int = 0
    break_consecutive: int = 0


class SpreadStatsTracker:
    """Bounded robust rolling state keyed by a canonical venue pair."""

    def __init__(
        self,
        *,
        window_ms: int = 6 * 60 * 60 * 1000,
        max_samples: int = 7200,
        short_window_ms: int = 15 * 60 * 1000,
        structural_break_sigma: float = 3.0,
        structural_break_consecutive: int = 5,
        structural_break_cooldown_ms: int = 30 * 60 * 1000,
    ) -> None:
        self.window_ms = max(int(window_ms or 0), 1)
        self.max_samples = max(int(max_samples or 0), 1)
        self.short_window_ms = max(int(short_window_ms or 0), 1)
        self.structural_break_sigma = max(float(structural_break_sigma or 0.0), 0.0)
        self.structural_break_consecutive = max(int(structural_break_consecutive or 0), 1)
        self.structural_break_cooldown_ms = max(int(structural_break_cooldown_ms or 0), 0)
        self._states: dict[tuple[str, str, str], _RollingState] = {}

    def configure(self, config: SpreadReversionConfig) -> None:
        self.window_ms = max(int(config.stats_window_ms or 0), 1)
        self.max_samples = max(int(config.stats_max_samples or 0), 1)
        self.short_window_ms = max(int(config.stats_short_window_ms or 0), 1)
        self.structural_break_sigma = max(float(config.structural_break_sigma or 0.0), 0.0)
        self.structural_break_consecutive = max(int(config.structural_break_consecutive or 0), 1)
        self.structural_break_cooldown_ms = max(int(config.structural_break_cooldown_ms or 0), 0)

    def update(
        self,
        symbol: str,
        venue_a: str,
        venue_b: str,
        signed_basis_bps: float,
        *,
        observed_at_ms: int = 0,
        exit_half_spread_bps: float = 0.0,
    ) -> SpreadStatsSnapshot:
        key = _key(symbol, venue_a, venue_b)
        state = self._states.setdefault(key, _RollingState())
        observed_ms = max(int(observed_at_ms or 0), 0)
        if observed_ms <= 0:
            return self._snapshot(state, now_ms=0)
        try:
            value = float(signed_basis_bps)
            exit_half = float(exit_half_spread_bps or 0.0)
        except (TypeError, ValueError):
            return self._snapshot(state, now_ms=observed_ms)
        # This is both an online statistics boundary and a checkpoint source.
        # A NaN entered here would poison median/MAD/AR(1) and could be
        # serialized into the next process.  It is a bad market observation,
        # not a zero-basis observation, so preserve the old window unchanged.
        if not math.isfinite(value) or not math.isfinite(exit_half) or exit_half < 0.0:
            return self._snapshot(state, now_ms=observed_ms)
        # The deque is chronological by contract.  Appending a late source
        # sample would make both rolling eviction and AR(1) use future data.
        # Ignore it rather than retrospectively rewriting a live signal
        # window; the signal caller separately rejects the corresponding
        # out-of-order observation.
        if state.samples and observed_ms <= state.samples[-1].observed_at_ms:
            return self._snapshot(state, now_ms=state.samples[-1].observed_at_ms)
        self._evict(state, observed_ms)
        state.samples.append(
            _Sample(
                observed_ms,
                value,
                exit_half,
            )
        )
        self._evict(state, observed_ms)
        structural_break = self._detect_structural_break(state, observed_ms)
        if structural_break:
            state.samples.clear()
            state.cooldown_until_ms = observed_ms + self.structural_break_cooldown_ms
            state.break_consecutive = 0
        return self._snapshot(state, now_ms=observed_ms, structural_break=structural_break)

    def snapshot(
        self,
        symbol: str,
        venue_a: str,
        venue_b: str,
        *,
        now_ms: int = 0,
    ) -> SpreadStatsSnapshot | None:
        state = self._states.get(_key(symbol, venue_a, venue_b))
        if state is None:
            return None
        if now_ms > 0:
            self._evict(state, now_ms)
        return self._snapshot(state, now_ms=now_ms)

    def checkpoint(self, *, now_ms: int = 0) -> dict[str, dict]:
        """Bounded serialisable checkpoint; callers may persist it atomically."""
        payload: dict[str, dict] = {}
        for key, state in self._states.items():
            if now_ms > 0:
                self._evict(state, now_ms)
            payload["|".join(key)] = {
                "samples": [
                    [
                        sample.observed_at_ms,
                        sample.value_bps,
                        sample.exit_half_spread_bps,
                    ]
                    for sample in state.samples
                ],
                "cooldown_until_ms": state.cooldown_until_ms,
                "break_consecutive": state.break_consecutive,
            }
        return payload

    def restore(self, checkpoint: dict[str, dict], *, now_ms: int) -> bool:
        """Restore a complete valid checkpoint or cold-start.

        A partially decoded checkpoint is more dangerous than a missing one:
        it can blend an unknown old subset with new observations and defeat
        model-epoch comparability.  Invalid values therefore discard the
        whole checkpoint rather than skipping individual rows.
        """
        self._states.clear()
        if not isinstance(checkpoint, dict):
            return False
        restore_now = max(int(now_ms or 0), 0)
        for packed_key, raw in checkpoint.items():
            parts = str(packed_key).split("|")
            if len(parts) != 3 or not isinstance(raw, dict):
                self._states.clear()
                return False
            try:
                cooldown_until_ms = int(raw.get("cooldown_until_ms", 0) or 0)
                break_consecutive = int(raw.get("break_consecutive", 0) or 0)
            except (TypeError, ValueError, OverflowError):
                self._states.clear()
                return False
            if cooldown_until_ms < 0 or break_consecutive < 0:
                self._states.clear()
                return False
            state = _RollingState(
                cooldown_until_ms=cooldown_until_ms,
                break_consecutive=break_consecutive,
            )
            restored_samples: list[_Sample] = []
            rows = raw.get("samples", [])
            if not isinstance(rows, list):
                self._states.clear()
                return False
            for row in rows:
                if not isinstance(row, list) or len(row) not in {2, 3}:
                    self._states.clear()
                    return False
                try:
                    ts = int(row[0] or 0)
                    value = float(row[1])
                    exit_half = float(row[2]) if len(row) == 3 else 0.0
                except (TypeError, ValueError, OverflowError):
                    self._states.clear()
                    return False
                if (
                    ts <= 0
                    or ts > restore_now
                    or not math.isfinite(value)
                    or not math.isfinite(exit_half)
                    or exit_half < 0.0
                ):
                    self._states.clear()
                    return False
                if ts <= restore_now:
                    restored_samples.append(_Sample(ts, value, exit_half))
            # A checkpoint is an external boundary and can be reordered or
            # duplicated.  Re-establish a strict time sequence before using a
            # deque whose eviction logic intentionally pops from the left.
            last_ts = 0
            for sample in sorted(restored_samples, key=lambda item: item.observed_at_ms):
                if sample.observed_at_ms <= last_ts:
                    continue
                state.samples.append(sample)
                last_ts = sample.observed_at_ms
            self._evict(state, restore_now)
            if state.samples or state.cooldown_until_ms > restore_now:
                self._states[(parts[0].upper(), parts[1].lower(), parts[2].lower())] = state
        return True

    def _evict(self, state: _RollingState, now_ms: int) -> None:
        cutoff = max(int(now_ms or 0) - self.window_ms, 0)
        while state.samples and state.samples[0].observed_at_ms < cutoff:
            state.samples.popleft()
        while len(state.samples) > self.max_samples:
            state.samples.popleft()

    def _detect_structural_break(self, state: _RollingState, now_ms: int) -> bool:
        if len(state.samples) < 2 or self.structural_break_sigma <= 0.0:
            return False
        values = [sample.value_bps for sample in state.samples]
        center, scale = _robust_location_scale(values)
        short_values = [
            sample.value_bps
            for sample in state.samples
            if sample.observed_at_ms >= now_ms - self.short_window_ms
        ]
        short_center = statistics.median(short_values) if short_values else center
        severe = scale > 0.0 and abs(short_center - center) > self.structural_break_sigma * scale
        state.break_consecutive = state.break_consecutive + 1 if severe else 0
        return state.break_consecutive >= self.structural_break_consecutive

    def _snapshot(
        self, state: _RollingState, *, now_ms: int, structural_break: bool = False
    ) -> SpreadStatsSnapshot:
        samples = list(state.samples)
        if not samples:
            return SpreadStatsSnapshot(
                sample_count=0,
                mean_bps=0.0,
                std_bps=0.0,
                cooldown_until_ms=state.cooldown_until_ms,
                structural_break=structural_break,
            )
        values = [sample.value_bps for sample in samples]
        median, robust_scale = _robust_location_scale(values)
        phi, half_life = _ar1_residual_quality(samples, median)
        exit_half_spreads = [
            sample.exit_half_spread_bps
            for sample in samples
            if sample.exit_half_spread_bps >= 0.0
        ]
        return SpreadStatsSnapshot(
            sample_count=len(values),
            mean_bps=median,
            std_bps=robust_scale,
            first_observed_ms=samples[0].observed_at_ms,
            last_observed_ms=samples[-1].observed_at_ms,
            median_bps=median,
            robust_scale_bps=robust_scale,
            exit_half_spread_p75_bps=_percentile(exit_half_spreads, 0.75),
            ar1_phi=phi,
            half_life_ms=half_life,
            cooldown_until_ms=state.cooldown_until_ms,
            structural_break=structural_break,
        )


@dataclass(frozen=True)
class _SpreadSignalModels:
    """Configuration-bound models reused by the production signal service."""

    fair_price: FairPriceModel
    zscore: ZScoreSignalModel
    quality: MeanReversionQualityModel
    funding: FundingAwarenessModel
    liquidity: LiquidityAndVenueHealthGate
    ranker: SpreadRanker


def _build_signal_models(config: SpreadReversionConfig) -> _SpreadSignalModels:
    return _SpreadSignalModels(
        fair_price=FairPriceModel(
            max_venue_premium_bps=config.fair_price_max_venue_premium_bps,
            min_venues_for_filter=config.fair_price_min_venues,
        ),
        zscore=ZScoreSignalModel(),
        quality=MeanReversionQualityModel(
            min_std_bps=config.mean_reversion_min_std_bps,
            max_half_life_ms=config.mean_reversion_max_half_life_ms,
        ),
        funding=FundingAwarenessModel(expected_hold_ms=config.expected_hold_ms),
        liquidity=LiquidityAndVenueHealthGate(),
        ranker=SpreadRanker(
            max_candidates=config.ranker_max_candidates,
            rank_by_capital_efficiency=config.rank_by_capital_efficiency,
        ),
    )


class SpreadSignalEngine:
    """Long-lived spread signal service; refreshes reuse config-bound models."""

    def __init__(self, *, tracker: SpreadStatsTracker, config: SpreadReversionConfig) -> None:
        self.tracker = tracker
        self.reconfigure(config)

    def reconfigure(self, config: SpreadReversionConfig) -> None:
        """Refresh non-market evidence without resetting the rolling history."""
        self.config = config
        self.tracker.configure(config)
        self._models = _build_signal_models(config)

    def build(
        self,
        quotes: dict[str, QuoteSnapshot],
        symbols: list[str],
        *,
        now_ms: int,
        rejection_counts: dict[str, int] | None = None,
    ) -> list[SpreadReversionCandidate]:
        return build_spread_reversion_candidates(
            quotes,
            symbols,
            tracker=self.tracker,
            config=self.config,
            now_ms=now_ms,
            rejection_counts=rejection_counts,
            _models=self._models,
        )


def build_spread_reversion_candidates(
    quotes: dict[str, QuoteSnapshot],
    symbols: list[str],
    *,
    tracker: SpreadStatsTracker,
    config: SpreadReversionConfig,
    now_ms: int,
    rejection_counts: dict[str, int] | None = None,
    _models: _SpreadSignalModels | None = None,
) -> list[SpreadReversionCandidate]:
    """Score each canonical pair exactly once and update after signal lookup."""
    tracker.configure(config)
    models = _models or _build_signal_models(config)
    by_symbol: dict[str, list[QuoteSnapshot]] = {}
    wanted = {str(symbol).upper() for symbol in symbols}
    for quote in quotes.values():
        symbol = str(quote.symbol or "").upper()
        if symbol in wanted:
            by_symbol.setdefault(symbol, []).append(quote)

    candidates: list[SpreadReversionCandidate] = []
    for symbol in sorted(wanted):
        symbol_quotes = by_symbol.get(symbol, [])
        fair_price = models.fair_price.assess(symbol_quotes)
        sorted_quotes = sorted(symbol_quotes, key=lambda q: str(q.venue).lower())
        for venue_a, venue_b in combinations(sorted_quotes, 2):
            compatibility = _contract_compatibility(venue_a, venue_b)
            if not compatibility.compatible:
                if rejection_counts is not None:
                    rejection_counts[compatibility.reason] = (
                        rejection_counts.get(compatibility.reason, 0) + 1
                    )
                continue
            candidate = _candidate_for_pair(
                venue_a,
                venue_b,
                tracker,
                config,
                now_ms,
                fair_price=fair_price,
                zscore_model=models.zscore,
                quality_model=models.quality,
                funding_model=models.funding,
                liquidity_gate=models.liquidity,
                rejection_counts=rejection_counts,
            )
            if candidate is not None:
                candidates.append(candidate)
    return models.ranker.rank(candidates)


def _candidate_for_pair(
    venue_a: QuoteSnapshot,
    venue_b: QuoteSnapshot,
    tracker: SpreadStatsTracker,
    config: SpreadReversionConfig,
    now_ms: int,
    *,
    fair_price: dict[str, FairPriceAssessment],
    zscore_model: ZScoreSignalModel,
    quality_model: MeanReversionQualityModel,
    funding_model: FundingAwarenessModel,
    liquidity_gate: LiquidityAndVenueHealthGate,
    rejection_counts: dict[str, int] | None,
) -> SpreadReversionCandidate | None:
    a, b = _canonical_quotes(venue_a, venue_b)
    if not _contract_compatibility(a, b).compatible:
        return None
    a_mid, b_mid = _mid(a), _mid(b)
    if a_mid <= 0.0 or b_mid <= 0.0:
        return None
    if not liquidity_gate.quote_fresh(
        a, b, now_ms=now_ms, signal_ttl_ms=config.signal_ttl_ms, quote_skew_ms=config.quote_skew_ms
    ):
        return None
    reference_mid = (a_mid + b_mid) / 2.0
    signed_mid = ((a_mid - b_mid) / reference_mid) * 10_000.0
    observed_exit_half_spread = _two_leg_half_spread_bps(a, b, reference_mid)

    # No look-ahead: take a snapshot of the old window, then append the
    # current observation. The post-update snapshot only enforces a break.
    stats = tracker.snapshot(a.symbol, a.venue, b.venue, now_ms=now_ms)
    if stats is None:
        stats = SpreadStatsSnapshot(sample_count=0, mean_bps=0.0, std_bps=0.0)
    # A signal timestamp behind the restored/live rolling window is an
    # out-of-order market event.  Using its old price against newer samples is
    # look-ahead, so reject it and leave the state unchanged.
    if stats.last_observed_ms > now_ms:
        return None
    updated = tracker.update(
        a.symbol,
        a.venue,
        b.venue,
        signed_mid,
        observed_at_ms=now_ms,
        exit_half_spread_bps=observed_exit_half_spread,
    )
    if updated.structural_break or now_ms < updated.cooldown_until_ms:
        return None

    a_fair = fair_price.get(str(a.venue).lower())
    b_fair = fair_price.get(str(b.venue).lower())
    outliers = [assessment for assessment in (a_fair, b_fair) if assessment and not assessment.eligible]
    opportunity_label = "spread_reversion"
    screening_reasons: list[str] = []
    if outliers:
        if not _single_venue_dislocation_allowed(
            long_outlier=bool(a_fair and not a_fair.eligible),
            short_outlier=bool(b_fair and not b_fair.eligible),
            fair_price=fair_price,
            config=config,
        ):
            return None
        opportunity_label = "single_venue_dislocation"
        screening_reasons.append("fair_outlier_override")

    is_dislocation = opportunity_label == "single_venue_dislocation"
    if not is_dislocation:
        if stats.sample_count < max(int(config.min_samples or 0), 1):
            return None
        if stats.history_age_ms < max(int(config.min_history_ms or 0), 0):
            return None
        if stats.std_bps < max(float(config.mean_reversion_min_std_bps or 0.0), 0.0):
            return None
    z_score = zscore_model.z_score(
        spread_mid_bps=signed_mid, mean_bps=stats.mean_bps, std_bps=stats.std_bps
    )
    if not is_dislocation and abs(z_score) < abs(float(config.entry_z or 0.0)):
        return None
    quality = quality_model.assess(
        z_score=z_score,
        sample_count=stats.sample_count,
        rolling_std_bps=stats.std_bps,
        ar1_phi=stats.ar1_phi,
        half_life_ms=stats.half_life_ms,
    )
    if not is_dislocation and not quality.entry_allowed:
        return None

    if z_score >= 0.0:
        long_q, short_q = b, a
        current_executable = ((a.bid - b.ask) / reference_mid) * 10_000.0
    else:
        long_q, short_q = a, b
        current_executable = ((a.ask - b.bid) / reference_mid) * 10_000.0
    fee_evidence_complete = _taker_fee_evidence_complete(
        config.taker_fee_bps_by_venue,
        long_q.venue,
        short_q.venue,
    )
    if not fee_evidence_complete:
        if rejection_counts is not None:
            rejection_counts["missing_taker_fee_evidence"] = (
                rejection_counts.get("missing_taker_fee_evidence", 0) + 1
            )
        return None
    target_exit = stats.mean_bps + math.copysign(
        abs(float(config.exit_z or 0.0)) * stats.std_bps, z_score
    )
    gross_reversion = abs(current_executable - target_exit)
    if (z_score > 0.0 and current_executable <= target_exit) or (
        z_score < 0.0 and current_executable >= target_exit
    ):
        return None
    absolute_executable = abs(current_executable)
    if not is_dislocation and not config.dynamic_net_edge_enabled:
        minimum = max(float(config.min_executable_spread_bps or 0.0), 0.0)
        maximum = max(float(config.max_executable_spread_bps or 0.0), 0.0)
        if minimum > 0.0 and absolute_executable < minimum:
            return None
        if maximum > 0.0 and absolute_executable >= maximum:
            return None

    entry_fee = _fee_bps(config, long_q.venue) + _fee_bps(config, short_q.venue)
    fee_bps = entry_fee * 2.0
    slippage_bps = max(float(config.slippage_reserve_bps or 0.0), 0.0) * 4.0
    funding = funding_model.assess(
        long_funding_rate_bps=float(long_q.funding_rate_bps or 0.0),
        short_funding_rate_bps=float(short_q.funding_rate_bps or 0.0),
        now_ms=now_ms,
        long_funding_timestamp_ms=int(long_q.funding_timestamp_ms or 0),
        short_funding_timestamp_ms=int(short_q.funding_timestamp_ms or 0),
        long_funding_interval_ms=int(long_q.funding_interval_ms or 0),
        short_funding_interval_ms=int(short_q.funding_interval_ms or 0),
    )
    if not funding.economics_complete:
        return None
    edge = build_edge_breakdown(
        gross_signal_edge_bps=gross_reversion,
        funding_edge_bps=funding.funding_edge_bps,
        # `current_executable` already embeds entry bid/ask crossing.  The
        # target is a mid spread, so only the conservative historical p75
        # close cross belongs here as an additional signed cash-flow cost.
        expected_exit_cross_bps=-stats.exit_half_spread_p75_bps,
        entry_fee_bps=entry_fee,
        exit_fee_bps=entry_fee,
        entry_slippage_bps=slippage_bps / 2.0,
        exit_slippage_bps=slippage_bps / 2.0,
        adverse_selection_bps=config.adverse_selection_buffer_bps,
        execution_buffer_bps=config.adverse_selection_buffer_bps,
        calculation_version=(
            "spread_v3_cost_normalized_reversion"
            if config.dynamic_net_edge_enabled
            else "spread_v2_signed_reversion"
        ),
        model_epoch=config.model_epoch,
        observed_at_ms=now_ms,
        economics_complete=funding.economics_complete and fee_evidence_complete,
    )
    if edge.expected_net_edge_bps < config.min_net_edge_bps:
        return None
    # Absolute entry-spread floors are not portable across fee tiers or
    # symbols.  The v3 epoch instead requires reversion headroom to exceed
    # the complete adverse execution burden by a configured multiple, plus a
    # separately visible minimum profit buffer.  Positive funding/cross terms
    # are deliberately not allowed to subsidise this threshold.
    execution_burden_bps = (
        edge.entry_fee_bps
        + edge.exit_fee_bps
        + edge.entry_slippage_bps
        + edge.exit_slippage_bps
        + edge.adverse_selection_bps
        + edge.execution_buffer_bps
        + edge.capital_buffer_bps
        + edge.venue_risk_haircut_bps
        + max(-edge.funding_edge_bps, 0.0)
        + max(-edge.entry_cross_bps, 0.0)
        + max(-edge.expected_exit_cross_bps, 0.0)
    )
    dynamic_min_gross_edge_bps = 0.0
    if config.dynamic_net_edge_enabled:
        dynamic_min_gross_edge_bps = (
            execution_burden_bps * max(float(config.min_gross_profit_multiple), 1.0)
            + max(float(config.min_profit_buffer_bps), 0.0)
        )
        if gross_reversion + 1e-12 < dynamic_min_gross_edge_bps:
            if rejection_counts is not None:
                rejection_counts["dynamic_min_gross_edge"] = (
                    rejection_counts.get("dynamic_min_gross_edge", 0) + 1
                )
            return None

    # ``max_gross_quote`` is the sum of absolute notionals across both legs.
    # The candidate amount is therefore a *per-leg* quote cap.  Treating the
    # gross cap as a single-leg cap would let one neutral pair consume almost
    # twice the configured portfolio limit.
    gross_cap = float(config.max_gross_quote or 0.0)
    entry_notional = min(
        float(config.live_notional_quote or 0.0),
        gross_cap / 2.0,
    )
    if entry_notional <= 0.0:
        return None
    capacity_quote = liquidity_gate.capacity_quote(long_q, short_q, entry_notional)
    if capacity_quote < entry_notional * max(float(config.min_liquidity_capacity_ratio or 0.0), 0.0):
        return None
    fair_price_value = (a_fair.fair_price if a_fair else b_fair.fair_price if b_fair else reference_mid)
    confidence = max(a_fair.confidence if a_fair else 0.0, b_fair.confidence if b_fair else 0.0)
    if confidence < config.min_fair_price_confidence:
        return None
    liquidity_score = liquidity_gate.liquidity_score(capacity_quote=capacity_quote, entry_notional_quote=entry_notional)
    effective_z = min(abs(z_score), max(float(config.score_z_cap or 0.0), 0.0)) if config.score_z_cap > 0 else abs(z_score)
    penalty = _liquidity_rank_penalty_bps(capacity_quote, config)
    # Funding is already a signed component of `edge.ranking_edge_bps`.
    # Adding `score_adjustment_bps` here would rank a favourable funding leg
    # twice, while a headwind was already deducted by the same edge contract.
    hold_time_ms = max(int(quality.hold_time_hint_ms or 0), 60_000)
    net_edge_per_capital_hour_bps = (
        edge.expected_net_edge_bps * 3_600_000.0 / hold_time_ms
    )
    # A short observed half-life from a thin/noisy sample must not outrank a
    # slower but robust opportunity solely because it inflates expected edge
    # per capital-hour.  Scale holding-time confidence by both sample depth
    # and the independently computed reversion-quality score, then rank on
    # downside edge over the confidence-expanded holding-time estimate.
    sample_confidence = min(
        1.0,
        math.sqrt(
            max(float(stats.sample_count), 0.0)
            / max(float(config.min_samples) * 4.0, 1.0)
        ),
    )
    hold_time_confidence = max(
        0.25,
        min(1.0, sample_confidence * max(min(float(quality.quality), 1.0), 0.0)),
    )
    risk_adjusted_edge_per_capital_hour_bps = (
        max(edge.worst_case_edge_bps, 0.0)
        * 3_600_000.0
        / (hold_time_ms / hold_time_confidence)
    )
    statistical_score = edge.ranking_edge_bps + effective_z * 2.0 + quality.quality * 5.0 + liquidity_score * config.score_liquidity_weight - penalty
    score = (
        risk_adjusted_edge_per_capital_hour_bps
        if config.rank_by_capital_efficiency
        else statistical_score
    )
    account_fee_evidence_complete = bool(
        config.account_fee_evidence is not None
        and config.account_fee_evidence.complete_for(long_q.venue, short_q.venue)
    )
    calculation_version = edge.calculation_version

    return SpreadReversionCandidate(
        candidate_id=_candidate_id(a.symbol, a.venue, b.venue),
        symbol=str(a.symbol).upper(),
        long_venue=str(long_q.venue).lower(),
        short_venue=str(short_q.venue).lower(),
        spread_mid_bps=signed_mid,
        executable_spread_bps=current_executable,
        rolling_mean_bps=stats.mean_bps,
        rolling_std_bps=stats.std_bps,
        z_score=z_score,
        net_edge_bps=edge.expected_net_edge_bps,
        sample_count=stats.sample_count,
        signal_ts_ms=now_ms,
        long_quote_ts_ms=int(long_q.observed_at_ms or 0),
        short_quote_ts_ms=int(short_q.observed_at_ms or 0),
        entry_notional_quote=entry_notional,
        capacity_quote=capacity_quote,
        signal_status="entry_ready",
        fee_bps=fee_bps,
        slippage_reserve_bps=slippage_bps,
        adverse_selection_buffer_bps=config.adverse_selection_buffer_bps,
        funding_carry_cost_bps=funding.carry_cost_bps,
        quote_skew_ms=abs(int(a.observed_at_ms or 0) - int(b.observed_at_ms or 0)),
        funding_timestamp_ms=funding.first_funding_timestamp_ms,
        first_funding_timestamp_ms=funding.first_funding_timestamp_ms,
        fair_price=fair_price_value,
        venue_premium_bps=((abs(a_fair.premium_bps) if a_fair else 0.0) + (abs(b_fair.premium_bps) if b_fair else 0.0)) / 2.0,
        fair_price_confidence=confidence,
        mean_reversion_quality=quality.quality,
        half_life_ms=quality.half_life_ms,
        hold_time_hint_ms=quality.hold_time_hint_ms,
        gross_edge_bps=edge.gross_signal_edge_bps,
        funding_carry_bps=funding.carry_cost_bps,
        liquidity_score=liquidity_score,
        venue_health_score=1.0,
        score=score,
        rank_reason=(
            f"score={score:.2f};expected_net_edge_bps={edge.expected_net_edge_bps:.2f};"
            f"net_edge_per_capital_hour_bps={net_edge_per_capital_hour_bps:.2f};"
            f"risk_adjusted_edge_per_capital_hour_bps={risk_adjusted_edge_per_capital_hour_bps:.2f};"
            f"hold_time_confidence={hold_time_confidence:.3f};"
            f"z_score={z_score:.2f};phi={stats.ar1_phi}"
        ),
        degradation_state=DegradationState.HEALTHY.value,
        liquidity_evidence_status=liquidity_gate.liquidity_evidence_status(long_q, short_q),
        screening_reasons=screening_reasons,
        history_age_ms=stats.history_age_ms,
        opportunity_label=opportunity_label,
        canonical_venue_a=str(a.venue).lower(),
        canonical_venue_b=str(b.venue).lower(),
        current_signed_mid_spread_bps=signed_mid,
        current_executable_entry_spread_bps=current_executable,
        equilibrium_spread_bps=stats.mean_bps,
        target_exit_spread_bps=target_exit,
        gross_reversion_edge_bps=gross_reversion,
        gross_signal_edge_bps=edge.gross_signal_edge_bps,
        funding_edge_bps=edge.funding_edge_bps,
        entry_cross_bps=edge.entry_cross_bps,
        expected_exit_cross_bps=edge.expected_exit_cross_bps,
        entry_fee_bps=edge.entry_fee_bps,
        exit_fee_bps=edge.exit_fee_bps,
        entry_slippage_bps=edge.entry_slippage_bps,
        exit_slippage_bps=edge.exit_slippage_bps,
        adverse_selection_bps=edge.adverse_selection_bps,
        capital_buffer_bps=edge.capital_buffer_bps,
        execution_buffer_bps=edge.execution_buffer_bps,
        venue_risk_haircut_bps=edge.venue_risk_haircut_bps,
        transfer_or_inventory_bias_bps=edge.transfer_or_inventory_bias_bps,
        ranking_edge_bps=edge.ranking_edge_bps,
        economics_observed_at_ms=edge.observed_at_ms,
        expected_net_edge_bps=edge.expected_net_edge_bps,
        worst_case_edge_bps=edge.worst_case_edge_bps,
        calculation_version=calculation_version,
        model_epoch=config.model_epoch,
        economics_complete=edge.economics_complete,
        fee_evidence_complete=fee_evidence_complete,
        account_fee_evidence_complete=account_fee_evidence_complete,
        account_fee_evidence_observed_at_ms=(
            config.account_fee_evidence.observed_at_ms_for(long_q.venue, short_q.venue)
            if account_fee_evidence_complete and config.account_fee_evidence is not None
            else 0
        ),
        account_fee_evidence_source=(
            config.account_fee_evidence.source_for(long_q.venue, short_q.venue)
            if account_fee_evidence_complete and config.account_fee_evidence is not None
            else ""
        ),
        account_fee_evidence_fingerprint=(
            config.account_fee_evidence.fingerprint_for(long_q.venue, short_q.venue)
            if account_fee_evidence_complete and config.account_fee_evidence is not None
            else ""
        ),
        account_fee_evidence_provenance=(
            config.account_fee_evidence.provenance_for(long_q.venue, short_q.venue)
            if account_fee_evidence_complete and config.account_fee_evidence is not None
            else []
        ),
        volatility_regime=(
            "high" if stats.std_bps >= float(config.volatility_high_std_bps) else "low"
        ),
        net_edge_per_capital_hour_bps=net_edge_per_capital_hour_bps,
        risk_adjusted_edge_per_capital_hour_bps=(
            risk_adjusted_edge_per_capital_hour_bps
        ),
        hold_time_confidence=hold_time_confidence,
        dynamic_min_gross_edge_bps=dynamic_min_gross_edge_bps,
        contract_normalization_status="complete",
    )


def _robust_location_scale(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    center = float(statistics.median(values))
    deviations = [abs(value - center) for value in values]
    scale = 1.4826 * float(statistics.median(deviations))
    if scale <= 1e-12 and len(values) >= 2:
        scale = float(statistics.stdev(values))
    return center, scale


def _ar1_residual_quality(samples: list[_Sample], center: float) -> tuple[float | None, int]:
    if len(samples) < 3:
        return None, 0
    residuals = [sample.value_bps - center for sample in samples]
    denominator = sum(value * value for value in residuals[:-1])
    if denominator <= 1e-12:
        return None, 0
    phi = sum(current * previous for previous, current in zip(residuals, residuals[1:])) / denominator
    if not 0.0 < phi < 1.0:
        return phi, 0
    intervals = [
        current.observed_at_ms - previous.observed_at_ms
        for previous, current in zip(samples, samples[1:])
        if current.observed_at_ms > previous.observed_at_ms
    ]
    if not intervals:
        return phi, 0
    # An AR(1) coefficient is defined at one sampling interval.  Scaling it
    # by an average after a long quote outage makes a single stale gap look
    # like a slow but tradable reversion.  The signal engine has no
    # continuous-time fit, so fail closed until the bounded window contains a
    # reasonably regular sequence again.
    nominal_interval = float(statistics.median(intervals))
    if nominal_interval <= 0.0 or max(intervals) > nominal_interval * 2.0:
        return phi, 0
    half_life = -math.log(2.0) / math.log(phi) * nominal_interval
    return phi, max(int(half_life), 1)


@dataclass(frozen=True)
class ContractCompatibility:
    compatible: bool
    reason: str = ""


def _contract_compatibility(a: QuoteSnapshot, b: QuoteSnapshot) -> ContractCompatibility:
    if (
        a.contract_normalization_complete is not True
        or b.contract_normalization_complete is not True
    ):
        return ContractCompatibility(False, "contract_normalization_incomplete")
    if str(a.venue_status or "").lower() != "active" or str(b.venue_status or "").lower() != "active":
        return ContractCompatibility(False, "venue_not_active")
    for property_name in ("underlying", "quote_currency", "contract_type"):
        left = str(getattr(a, property_name, "") or "").strip().lower()
        right = str(getattr(b, property_name, "") or "").strip().lower()
        if not left or not right:
            return ContractCompatibility(False, f"missing_{property_name}")
        if left != right:
            return ContractCompatibility(False, f"mismatched_{property_name}")
    # ``QuoteSnapshot.bid_size``/``ask_size`` and every paper order quantity
    # are canonical *base* units.  That contract is currently proven only for
    # linear USDT-style perps (see ``FundingTicker``).  An equal multiplier is
    # not enough to make inverse or quanto contracts economically equivalent:
    # their order-book size, notional and settlement PnL units differ with
    # price/settle currency.  Admitting them here would make paper capacity and
    # PnL fictitious.  Keep those products fail-closed until a separate
    # contract-unit normalizer converts both book depth and cashflows to the
    # shared base/quote contract.
    if str(a.contract_type).lower() != "linear":
        return ContractCompatibility(False, "unsupported_contract_type_for_base_quantity_pnl")
    if not str(a.mark_index_source or "").strip() or not str(b.mark_index_source or "").strip():
        return ContractCompatibility(False, "missing_mark_index_source")
    if float(a.contract_multiplier or 0.0) <= 0.0 or float(b.contract_multiplier or 0.0) <= 0.0:
        return ContractCompatibility(False, "missing_contract_multiplier")
    if int(a.price_precision or 0) <= 0 or int(b.price_precision or 0) <= 0:
        return ContractCompatibility(False, "missing_price_precision")
    if int(a.quantity_precision or 0) <= 0 or int(b.quantity_precision or 0) <= 0:
        return ContractCompatibility(False, "missing_quantity_precision")
    if not math.isclose(float(a.contract_multiplier), float(b.contract_multiplier), rel_tol=1e-12):
        return ContractCompatibility(False, "mismatched_contract_multiplier")
    if int(a.funding_interval_ms or 0) <= 0 or int(b.funding_interval_ms or 0) <= 0:
        return ContractCompatibility(False, "missing_funding_interval")
    return ContractCompatibility(True)


def _two_leg_half_spread_bps(a: QuoteSnapshot, b: QuoteSnapshot, reference_mid: float) -> float:
    if reference_mid <= 0.0:
        return 0.0
    half_width = ((float(a.ask) - float(a.bid)) + (float(b.ask) - float(b.bid))) / 2.0
    return max(half_width / reference_mid * 10_000.0, 0.0)


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    index = (len(ordered) - 1) * min(max(float(quantile), 0.0), 1.0)
    lower = int(math.floor(index))
    upper = int(math.ceil(index))
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def _canonical_quotes(left: QuoteSnapshot, right: QuoteSnapshot) -> tuple[QuoteSnapshot, QuoteSnapshot]:
    return (left, right) if str(left.venue).lower() <= str(right.venue).lower() else (right, left)


def _fee_map(venues: list[VenueConfig]) -> dict[str, float]:
    return {str(venue.venue).lower(): float(venue.taker_fee_bps or 0.0) for venue in venues if str(venue.venue or "")}


def _maker_fee_map(venues: list[VenueConfig]) -> dict[str, float]:
    return {
        str(venue.venue).lower(): float(
            venue.maker_fee_bps
            if venue.maker_fee_bps is not None
            else venue.taker_fee_bps or 0.0
        )
        for venue in venues
        if str(venue.venue or "")
    }


def _single_venue_dislocation_allowed(*, long_outlier: bool, short_outlier: bool, fair_price: dict[str, FairPriceAssessment], config: SpreadReversionConfig) -> bool:
    return bool(config.single_venue_dislocation_enabled and long_outlier != short_outlier and sum(1 for value in fair_price.values() if value.eligible) >= max(int(config.single_venue_dislocation_min_anchor_venues or 0), 1))


def _fee_bps(config: SpreadReversionConfig, venue: str) -> float:
    try:
        fee_bps = float(config.taker_fee_bps_by_venue.get(str(venue).lower(), 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return fee_bps if math.isfinite(fee_bps) and fee_bps >= 0.0 else 0.0


def _taker_fee_evidence_complete(
    fee_by_venue: dict[str, float],
    long_venue: object,
    short_venue: object,
) -> bool:
    """Require explicit, finite, non-negative taker fees for both legs."""
    for venue in (long_venue, short_venue):
        key = str(venue or "").lower()
        if key not in fee_by_venue:
            return False
        try:
            fee_bps = float(fee_by_venue[key])
        except (TypeError, ValueError):
            return False
        if not math.isfinite(fee_bps) or fee_bps < 0.0:
            return False
    return True


def _liquidity_rank_penalty_bps(capacity_quote: float, config: SpreadReversionConfig) -> float:
    capacity = max(float(capacity_quote or 0.0), 0.0)
    if capacity < max(float(config.liquidity_small_quote or 0.0), 0.0):
        return max(float(config.liquidity_small_penalty_bps or 0.0), 0.0)
    if capacity < max(float(config.liquidity_medium_quote or 0.0), 0.0):
        return max(float(config.liquidity_medium_penalty_bps or 0.0), 0.0)
    if capacity < max(float(config.liquidity_large_quote or 0.0), 0.0):
        return max(float(config.liquidity_sublarge_penalty_bps or 0.0), 0.0)
    return 0.0


def _candidate_id(symbol: str, venue_a: str, venue_b: str) -> str:
    return f"spread:{str(symbol).upper()}:{str(venue_a).lower()}->{str(venue_b).lower()}"


def _key(symbol: str, venue_a: str, venue_b: str) -> tuple[str, str, str]:
    a, b = sorted((str(venue_a).lower(), str(venue_b).lower()))
    return str(symbol).upper(), a, b


def _mid(quote: QuoteSnapshot) -> float:
    bid, ask = float(quote.bid or 0.0), float(quote.ask or 0.0)
    return (
        (bid + ask) / 2.0
        if math.isfinite(bid) and math.isfinite(ask) and bid > 0.0 and ask > 0.0
        else 0.0
    )
