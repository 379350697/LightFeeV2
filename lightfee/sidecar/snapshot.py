"""Sidecar snapshot schema matching Rust reference opportunity input shape."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


SNAPSHOT_SCHEMA_VERSION = 2


class SnapshotFreshness(Enum):
    """V1 snapshot freshness states (CONTRACT OPP-001).

    V1 anchor: src/opportunity_input/types.rs  OpportunityInputDomainState
    V1 semantics:
      - FRESH: usable directly
      - LAST_GOOD_FALLBACK: current stale/missing but recent valid snapshot exists
      - STALE: current exists but exceeds max_age_ms (warning)
      - MISSING: no snapshot available at all (blocks trading)
      - DEGRADED: one or more health domains degraded but snapshot is otherwise usable
    """

    FRESH = "fresh"
    LAST_GOOD_FALLBACK = "last_good_fallback"
    STALE = "stale"
    MISSING = "missing"
    DEGRADED = "degraded"


@dataclass
class FundingLifecycle:
    """Funding data freshness metadata — V1 domain-level lifecycle."""

    venue: str
    observed_at_ms: int
    symbol_count: int
    coverage_usable: int = 0
    degraded_reason: str = ""


@dataclass
class MarketLifecycle:
    """Market data freshness metadata — V1 domain-level lifecycle."""

    venue: str
    observed_at_ms: int
    symbol_count: int
    coverage_usable: int = 0
    degraded_reason: str = ""


@dataclass
class TransferLifecycle:
    """Transfer status freshness metadata — V1 domain-level lifecycle."""

    from_venue: str
    to_venue: str
    observed_at_ms: int
    coverage_usable: int = 0
    degraded_reason: str = ""


@dataclass
class LiquidityLifecycle:
    """Liquidity data freshness metadata — V1 domain-level lifecycle."""

    venue: str
    observed_at_ms: int
    symbol_count: int
    coverage_usable: int = 0
    degraded_reason: str = ""
    domain: str = "perp_liquidity"
    source: str = "sidecar_perp_liquidity"
    publish_interval_ms: int = 0
    published_at_ms: int = 0


@dataclass
class QuoteSnapshot:
    venue: str
    symbol: str
    bid: float
    ask: float
    bid_size: float = 0.0
    ask_size: float = 0.0
    funding_rate_bps: float = 0.0
    funding_timestamp_ms: int = 0
    mark_price: float = 0.0
    index_price: float = 0.0
    volume_24h_quote: float = 0.0
    open_interest: float = 0.0


@dataclass
class CandidateInput:
    """One directed pair candidate for the live engine."""

    long_venue: str
    short_venue: str
    symbol: str
    funding_diff_bps: float
    funding_edge_bps: float
    expected_edge_bps: float
    worst_case_edge_bps: float
    ranking_edge_bps: float
    transfer_bias_bps: float = 0.0
    opportunity_type: str = "aligned"
    blocked: bool = False
    blocked_reasons: list[str] = field(default_factory=list)
    long_venue_index: int = 0
    short_venue_index: int = 0
    entry_notional_quote: float = 0.0
    # V1 parity fields (CONTRACT OPP-002: candidate identity + prewarm)
    pair_id: str = ""
    funding_timestamp_ms: int = 0
    first_funding_timestamp_ms: int = 0
    long_funding_timestamp_ms: int = 0
    short_funding_timestamp_ms: int = 0
    second_funding_timestamp_ms: int = 0
    # V1: FundingLeg — which side's funding settles first
    first_funding_leg: str = ""  # "long" or "short"
    # V2: direction consistency and interval alignment (V1 fix)
    direction_consistent: bool = False
    interval_aligned: bool = False
    # Optional execution dependency marker. Empty means sizing/execution uses quote
    # and local-L2 gates rather than coarse sidecar perp liquidity.
    sizing_liquidity_source: str = ""


@dataclass
class SidecarSnapshot:
    """The opportunity-input-snapshot published by the sidecar."""

    schema_version: int = SNAPSHOT_SCHEMA_VERSION
    published_at_ms: int = 0
    market_observed_at_ms: int = 0
    funding_lifecycle: list[FundingLifecycle] = field(default_factory=list)
    market_lifecycle: list[MarketLifecycle] = field(default_factory=list)
    transfer_lifecycle: list[TransferLifecycle] = field(default_factory=list)
    liquidity_lifecycle: list[LiquidityLifecycle] = field(default_factory=list)
    degraded_venues: list[str] = field(default_factory=list)
    degraded_domains: list[str] = field(default_factory=list)
    degraded_symbols: dict[str, list[str]] = field(default_factory=dict)  # venue -> [symbol, ...]
    # V1 provider-depth semantics: provenance tracking
    source_mode: str = ""  # "direct_market" | "direct_market_enriched" | "coarse_sidecar" | "sidecar_scan"
    acquisition_mode: str = ""  # "fresh_sidecar" | "last_good_sidecar" | "direct_market_view" | "unavailable"
    quotes: dict[str, QuoteSnapshot] = field(default_factory=dict)
    candidates: list[CandidateInput] = field(default_factory=list)


def evaluate_snapshot_freshness(
    snapshot: SidecarSnapshot | None,
    max_age_ms: int,
    now_ms: int,
    last_good: SidecarSnapshot | None = None,
    last_good_max_age_ms: int | None = None,
    market_max_age_ms: int | None = None,
) -> SnapshotFreshness:
    """Evaluate snapshot freshness per V1 OpportunityInputDomainState semantics.

    V1 anchors: src/opportunity_input/types.rs  OpportunityInputDomainState
                 src/opportunity_input/sidecar_snapshot.rs  snapshot freshness evaluation

    Priority order:
    1. MISSING — no snapshot at all
    2. LAST_GOOD_FALLBACK — current is stale/missing but a recent valid one exists
    3. STALE — current snapshot exceeds max_age_ms
    4. DEGRADED — snapshot exists within max_age but has degraded venues/domains
    5. FRESH — snapshot exists within max_age and has no degradations
    """
    last_good_limit_ms = (
        last_good_max_age_ms if last_good_max_age_ms is not None else max_age_ms
    )
    market_limit_ms = market_max_age_ms if market_max_age_ms is not None else max_age_ms

    def _within_last_good_window(candidate: SidecarSnapshot | None) -> bool:
        if candidate is None:
            return False
        return now_ms - candidate.published_at_ms <= last_good_limit_ms

    if snapshot is None:
        if last_good is not None:
            if _within_last_good_window(last_good):
                return SnapshotFreshness.LAST_GOOD_FALLBACK
        return SnapshotFreshness.MISSING

    age_ms = now_ms - snapshot.published_at_ms

    if age_ms > max_age_ms:
        if _within_last_good_window(snapshot) or _within_last_good_window(last_good):
            return SnapshotFreshness.LAST_GOOD_FALLBACK
        return SnapshotFreshness.STALE

    market_age_ms = (
        now_ms - snapshot.market_observed_at_ms
        if snapshot.market_observed_at_ms > 0
        else 0
    )
    if market_age_ms > market_limit_ms:
        if _within_last_good_window(snapshot) or _within_last_good_window(last_good):
            return SnapshotFreshness.LAST_GOOD_FALLBACK
        return SnapshotFreshness.STALE

    if snapshot.degraded_venues or snapshot.degraded_domains or snapshot.degraded_symbols:
        return SnapshotFreshness.DEGRADED

    return SnapshotFreshness.FRESH
