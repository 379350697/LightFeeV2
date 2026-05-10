"""Market data freshness checks matching V1 behavior.

Rust references:
- src/execution_core/market_data.rs: MarketFreshness, degraded_venues, stale detection
"""

from __future__ import annotations

from dataclasses import dataclass, field


def is_market_data_fresh(observed_at_ms: int, max_age_ms: int, now_ms: int) -> bool:
    return (now_ms - observed_at_ms) <= max_age_ms


def allows_candidate(observed_at_ms: int, max_age_ms: int, now_ms: int) -> bool:
    """Check if market data is fresh enough for candidate evaluation."""
    return is_market_data_fresh(observed_at_ms, max_age_ms, now_ms)


@dataclass
class MarketFreshness:
    """V1 MarketFreshness: per-venue freshness state for entry/exit gating."""
    max_age_ms: int = 3_000
    now_ms: int = 0
    fresh_venues: list[str] = field(default_factory=list)
    stale_venues: list[str] = field(default_factory=list)
    degraded_venues: list[str] = field(default_factory=list)
    degraded_symbols: list[tuple[str, str]] = field(default_factory=list)  # (venue, symbol)

    def evaluate(self, venue_observed: dict[str, int]) -> None:
        """Classify venues as fresh, stale, or degraded."""
        self.fresh_venues.clear()
        self.stale_venues.clear()
        for venue, observed_at_ms in venue_observed.items():
            if is_market_data_fresh(observed_at_ms, self.max_age_ms, self.now_ms):
                self.fresh_venues.append(venue)
            else:
                self.stale_venues.append(venue)

    def any_stale(self) -> bool:
        return len(self.stale_venues) > 0

    def transfer_stale_venues(self) -> list[str]:
        """Transfer stale venues to degraded (V1: stale→degraded escalation)."""
        for v in self.stale_venues:
            if v not in self.degraded_venues:
                self.degraded_venues.append(v)
        self.stale_venues.clear()
        return list(self.degraded_venues)

    def is_venue_degraded(self, venue: str) -> bool:
        return venue in self.degraded_venues
