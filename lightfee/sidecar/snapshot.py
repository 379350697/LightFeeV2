"""Sidecar snapshot schema matching Rust reference opportunity input shape."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


SNAPSHOT_SCHEMA_VERSION = 2


@dataclass
class FundingLifecycle:
    """Funding data freshness metadata."""

    venue: str
    observed_at_ms: int
    symbol_count: int


@dataclass
class MarketLifecycle:
    """Market data freshness metadata."""

    venue: str
    observed_at_ms: int
    symbol_count: int


@dataclass
class TransferLifecycle:
    """Transfer status freshness metadata."""

    from_venue: str
    to_venue: str
    observed_at_ms: int


@dataclass
class LiquidityLifecycle:
    """Liquidity data freshness metadata."""

    venue: str
    observed_at_ms: int
    symbol_count: int


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
    blocked_reason: str = ""
    long_venue_index: int = 0
    short_venue_index: int = 0
    entry_notional_quote: float = 0.0


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
    quotes: dict[str, QuoteSnapshot] = field(default_factory=dict)
    candidates: list[CandidateInput] = field(default_factory=list)
