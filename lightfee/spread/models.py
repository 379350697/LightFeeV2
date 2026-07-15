"""Spread-reversion domain models.

These models deliberately stay separate from funding `CandidateInput` so spread
reversion does not inherit funding-window or settlement lifecycle semantics.
"""

from __future__ import annotations

from dataclasses import dataclass, field


SPREAD_SNAPSHOT_SCHEMA_VERSION = 4
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
    opportunity_label: str = "spread_reversion"
    # v2 signed-basis economics. The original fields remain for old journals
    # and consumers, but no longer mean "the whole observed spread is profit".
    canonical_venue_a: str = ""
    canonical_venue_b: str = ""
    current_signed_mid_spread_bps: float = 0.0
    current_executable_entry_spread_bps: float = 0.0
    equilibrium_spread_bps: float = 0.0
    target_exit_spread_bps: float = 0.0
    gross_reversion_edge_bps: float = 0.0
    # The complete shared `EdgeBreakdown` is dual-written here.  The older
    # fields above remain readable by legacy paper journals and diagnostics.
    gross_signal_edge_bps: float = 0.0
    funding_edge_bps: float = 0.0
    entry_cross_bps: float = 0.0
    expected_exit_cross_bps: float = 0.0
    entry_fee_bps: float = 0.0
    exit_fee_bps: float = 0.0
    entry_slippage_bps: float = 0.0
    exit_slippage_bps: float = 0.0
    adverse_selection_bps: float = 0.0
    capital_buffer_bps: float = 0.0
    execution_buffer_bps: float = 0.0
    venue_risk_haircut_bps: float = 0.0
    transfer_or_inventory_bias_bps: float = 0.0
    ranking_edge_bps: float = 0.0
    economics_observed_at_ms: int = 0
    expected_net_edge_bps: float = 0.0
    worst_case_edge_bps: float = 0.0
    calculation_version: str = "spread_v2_signed_reversion"
    model_epoch: str = "v2_signed_reversion"
    # Construction outside the signed-basis builder must never manufacture
    # permission for paper registration or a future live adapter.
    economics_complete: bool = False
    # Four-leg economics must be backed by explicit fee inputs.  An explicit
    # zero is valid (for example a VIP tier); a missing key never is.
    fee_evidence_complete: bool = False
    # ``fee_evidence_complete`` means the four-leg formula had explicit fee
    # values.  The following fields distinguish a static configuration floor
    # from contemporaneous account-scoped evidence used for official paper
    # acceptance.
    account_fee_evidence_complete: bool = False
    account_fee_evidence_observed_at_ms: int = 0
    account_fee_evidence_source: str = ""
    account_fee_evidence_fingerprint: str = ""
    account_fee_evidence_provenance: list[dict[str, object]] = field(default_factory=list)
    # P1 research attribution: an immutable model epoch must not mix tuned
    # and holdout observations, and capital efficiency must be auditable.
    research_sample_split: str = "in_sample"
    volatility_regime: str = "unknown"
    net_edge_per_capital_hour_bps: float = 0.0
    # Capital efficiency is ranked from downside edge and confidence-adjusted
    # holding time; the legacy expected-edge rate stays diagnostic only.
    risk_adjusted_edge_per_capital_hour_bps: float = 0.0
    hold_time_confidence: float = 0.0
    dynamic_min_gross_edge_bps: float = 0.0
    contract_normalization_status: str = "unknown"
    contract_normalization_reason: str = ""


@dataclass(frozen=True)
class SpreadSnapshot:
    schema_version: int = SPREAD_SNAPSHOT_SCHEMA_VERSION
    decision_at_ms: int = 0
    published_at_ms: int = 0
    market_observed_at_ms: int = 0
    snapshot_path: str = ""
    source_mode: str = ""
    degraded_venues: list[str] = field(default_factory=list)
    degraded_symbols: dict[str, list[str]] = field(default_factory=dict)
    input_quote_count: int = 0
    valid_quote_count: int = 0
    evaluated_pair_count: int = 0
    accepted_pair_count: int = 0
    paper_configured_enabled: bool = False
    paper_admission_enabled: bool = False
    paper_tracked_count: int = 0
    paper_refresh_status: str = ""
    paper_event_count: int = 0
    paper_last_success_at_ms: int = 0
    # Aggregated stable reasons make a zero-candidate refresh diagnostically
    # useful without pretending blocked pairs are tradeable candidates.
    rejection_counts: dict[str, int] = field(default_factory=dict)
    # Paper admission is a separate funnel from signal construction.  Keep its
    # reasons separate so accepted signals that never become paper positions
    # cannot be misreported as a healthy no-event refresh.
    paper_admission_rejection_counts: dict[str, int] = field(default_factory=dict)
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
    # This is the actual matched base quantity, not a target quote amount.
    # A close planner must never reverse two independently re-derived notionals.
    base_quantity: float = 0.0


@dataclass(frozen=True)
class SpreadTradingState:
    open_positions: list[SpreadPosition] = field(default_factory=list)
    pending_entry_count: int = 0
    pending_close_count: int = 0
    global_gross_quote: float = 0.0
