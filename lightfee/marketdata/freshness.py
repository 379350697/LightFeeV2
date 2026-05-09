"""Market data freshness checks."""

from __future__ import annotations


def is_market_data_fresh(observed_at_ms: int, max_age_ms: int, now_ms: int) -> bool:
    return (now_ms - observed_at_ms) <= max_age_ms


def allows_candidate(observed_at_ms: int, max_age_ms: int, now_ms: int) -> bool:
    """Check if market data is fresh enough for candidate evaluation."""
    return is_market_data_fresh(observed_at_ms, max_age_ms, now_ms)
