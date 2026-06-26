"""Continuous spread-reversion signal generation."""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from lightfee.config.schema import AppConfig, StrategyConfig, VenueConfig
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.models import SpreadReversionCandidate


_FUNDING_INTERVAL_MS = 8 * 60 * 60 * 1000


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


@dataclass
class _WelfordState:
    count: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def update(self, value: float) -> SpreadStatsSnapshot:
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
    ) -> SpreadStatsSnapshot:
        state = self._states.setdefault(
            _key(symbol, long_venue, short_venue),
            _WelfordState(),
        )
        return state.update(spread_mid_bps)

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
    for symbol in symbols:
        symbol_quotes = [
            q for q in quotes.values()
            if str(getattr(q, "symbol", "") or "").upper() == symbol.upper()
        ]
        if len(symbol_quotes) < 2:
            continue
        for i, left in enumerate(symbol_quotes):
            for right in symbol_quotes[i + 1:]:
                candidate = _candidate_for_pair(left, right, tracker, config, now_ms)
                if candidate is not None:
                    candidates.append(candidate)
    return sorted(candidates, key=lambda c: (c.net_edge_bps, c.z_score), reverse=True)


def _candidate_for_pair(
    left: QuoteSnapshot,
    right: QuoteSnapshot,
    tracker: SpreadStatsTracker,
    config: SpreadReversionConfig,
    now_ms: int,
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

    if not _quote_fresh(long_q, short_q, now_ms, config):
        return None

    reference_mid = (long_mid + short_mid) / 2.0
    spread_mid_bps = ((short_mid - long_mid) / reference_mid) * 10_000.0
    stats = tracker.update(
        str(long_q.symbol),
        str(long_q.venue),
        str(short_q.venue),
        spread_mid_bps,
    )
    if stats.sample_count < max(config.min_samples, 1):
        return None
    if stats.std_bps <= 0.0:
        z_score = 0.0
    else:
        z_score = (spread_mid_bps - stats.mean_bps) / stats.std_bps
    if z_score < config.entry_z:
        return None

    executable_spread_bps = 0.0
    if short_q.bid > 0.0 and long_q.ask > 0.0:
        executable_spread_bps = ((short_q.bid - long_q.ask) / reference_mid) * 10_000.0
    fee_bps = _fee_bps(config, long_q.venue) + _fee_bps(config, short_q.venue)
    funding_carry_cost_bps = _funding_carry_cost_bps(long_q, short_q, config)
    net_edge_bps = (
        executable_spread_bps
        - fee_bps
        - config.slippage_reserve_bps
        - config.adverse_selection_buffer_bps
        - funding_carry_cost_bps
    )
    if net_edge_bps < config.min_net_edge_bps:
        return None

    quote_skew = abs(int(long_q.observed_at_ms or 0) - int(short_q.observed_at_ms or 0))
    entry_notional = min(config.live_notional_quote, config.max_gross_quote)
    if entry_notional <= 0.0:
        return None
    capacity_quote = _capacity_quote(long_q, short_q, entry_notional)
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
        net_edge_bps=net_edge_bps,
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
        funding_carry_cost_bps=funding_carry_cost_bps,
        quote_skew_ms=quote_skew,
    )


def _fee_map(venues: list[VenueConfig]) -> dict[str, float]:
    result: dict[str, float] = {}
    for venue in venues:
        name = str(getattr(venue, "venue", "") or "").lower()
        if not name:
            continue
        result[name] = float(getattr(venue, "taker_fee_bps", 0.0) or 0.0)
    return result


def _fee_bps(config: SpreadReversionConfig, venue: str) -> float:
    return float(config.taker_fee_bps_by_venue.get(str(venue).lower(), 0.0) or 0.0)


def _funding_carry_cost_bps(
    long_q: QuoteSnapshot,
    short_q: QuoteSnapshot,
    config: SpreadReversionConfig,
) -> float:
    hold_ratio = max(float(config.expected_hold_ms or 0), 0.0) / _FUNDING_INTERVAL_MS
    long_rate = float(getattr(long_q, "funding_rate_bps", 0.0) or 0.0)
    short_rate = float(getattr(short_q, "funding_rate_bps", 0.0) or 0.0)
    carry_bps = (long_rate - short_rate) * hold_ratio
    return max(carry_bps, 0.0)


def _quote_fresh(
    long_q: QuoteSnapshot,
    short_q: QuoteSnapshot,
    now_ms: int,
    config: SpreadReversionConfig,
) -> bool:
    long_ts = int(getattr(long_q, "observed_at_ms", 0) or 0)
    short_ts = int(getattr(short_q, "observed_at_ms", 0) or 0)
    if long_ts <= 0 or short_ts <= 0:
        return False
    ttl = max(int(config.signal_ttl_ms or 0), 0)
    if ttl and (now_ms - long_ts > ttl or now_ms - short_ts > ttl):
        return False
    skew = max(int(config.quote_skew_ms or 0), 0)
    if skew and abs(long_ts - short_ts) > skew:
        return False
    return True


def _capacity_quote(
    long_q: QuoteSnapshot,
    short_q: QuoteSnapshot,
    fallback_notional: float,
) -> float:
    long_capacity = float(getattr(long_q, "ask_size", 0.0) or 0.0) * float(long_q.ask or 0.0)
    short_capacity = float(getattr(short_q, "bid_size", 0.0) or 0.0) * float(short_q.bid or 0.0)
    if long_capacity > 0.0 and short_capacity > 0.0:
        return min(long_capacity, short_capacity)
    return fallback_notional


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
