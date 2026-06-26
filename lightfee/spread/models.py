"""Spread-reversion domain models.

These models deliberately stay separate from funding `CandidateInput` so spread
reversion does not inherit funding-window or settlement lifecycle semantics.
"""

from __future__ import annotations

from dataclasses import dataclass, field


SPREAD_SNAPSHOT_SCHEMA_VERSION = 1
SPREAD_STRATEGY_BUCKET = "spread_reversion"


@dataclass(frozen=True)
class SpreadReversionCandidate:
    candidate_id: str
    symbol: str
    long_venue: str
    short_venue: str
    spread_mid_bps: float
    executable_spread_bps: float
    rolling_mean_bps: float
    rolling_std_bps: float
    z_score: float
    net_edge_bps: float
    sample_count: int
    signal_ts_ms: int
    long_quote_ts_ms: int
    short_quote_ts_ms: int
    entry_notional_quote: float
    capacity_quote: float
    signal_status: str
    strategy_bucket: str = SPREAD_STRATEGY_BUCKET
    fee_bps: float = 0.0
    slippage_reserve_bps: float = 0.0
    adverse_selection_buffer_bps: float = 0.0
    funding_carry_cost_bps: float = 0.0
    quote_skew_ms: int = 0
    funding_timestamp_ms: int = 0
    first_funding_timestamp_ms: int = 0
    fair_price: float = 0.0
    venue_premium_bps: float = 0.0
    fair_price_confidence: float = 0.0
    mean_reversion_quality: float = 0.0
    half_life_ms: int = 0
    hold_time_hint_ms: int = 0
    gross_edge_bps: float = 0.0
    funding_carry_bps: float = 0.0
    liquidity_score: float = 0.0
    venue_health_score: float = 1.0
    score: float = 0.0
    rank_reason: str = ""
    degradation_state: str = "healthy"
    liquidity_evidence_status: str = ""
    screening_reasons: list[str] = field(default_factory=list)
    history_age_ms: int = 0


@dataclass(frozen=True)
class SpreadSnapshot:
    schema_version: int = SPREAD_SNAPSHOT_SCHEMA_VERSION
    published_at_ms: int = 0
    market_observed_at_ms: int = 0
    snapshot_path: str = ""
    source_mode: str = ""
    degraded_venues: list[str] = field(default_factory=list)
    degraded_symbols: dict[str, list[str]] = field(default_factory=dict)
    candidates: list[SpreadReversionCandidate] = field(default_factory=list)


@dataclass(frozen=True)
class SpreadOrderIntent:
    candidate_id: str
    symbol: str
    long_venue: str
    short_venue: str
    entry_notional_quote: float
    reason: str
    strategy_bucket: str = SPREAD_STRATEGY_BUCKET


@dataclass(frozen=True)
class SpreadExitIntent:
    position_id: str
    symbol: str
    long_venue: str
    short_venue: str
    reason: str
    strategy_bucket: str = SPREAD_STRATEGY_BUCKET


@dataclass(frozen=True)
class SpreadDecision:
    allowed: bool
    reason: str
    intent: SpreadOrderIntent | SpreadExitIntent | None = None
    evidence: dict = field(default_factory=dict)


@dataclass(frozen=True)
class SpreadPosition:
    position_id: str
    symbol: str
    long_venue: str
    short_venue: str
    entry_spread_bps: float
    entry_z_score: float
    entry_notional_quote: float
    opened_at_ms: int
    strategy_bucket: str = SPREAD_STRATEGY_BUCKET


@dataclass(frozen=True)
class SpreadTradingState:
    open_positions: list[SpreadPosition] = field(default_factory=list)
    pending_entry_count: int = 0
    pending_close_count: int = 0
    global_gross_quote: float = 0.0
