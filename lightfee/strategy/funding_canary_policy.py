"""Shared funding-canary policy resolution.

The canary is an entry-only release profile.  This module keeps venue scope,
pair-specific economics floors and fee-assurance sizing identical in discovery,
final dispatch and offline cohort identity.
"""

from __future__ import annotations

import math
from typing import Mapping


ALL_LIVE_FUNDING_VENUES = frozenset(
    {"aster", "binance", "bitget", "bybit", "gate", "hyperliquid", "okx"}
)


def canonical_venue_pair(long_venue: object, short_venue: object) -> str:
    venues = sorted(
        (
            str(getattr(long_venue, "value", long_venue) or "").strip().lower(),
            str(getattr(short_venue, "value", short_venue) or "").strip().lower(),
        )
    )
    return ":".join(venues) if all(venues) and venues[0] != venues[1] else ""


def normalized_pair_floor_map(raw: object) -> dict[str, float]:
    if not isinstance(raw, Mapping):
        return {}
    normalized: dict[str, float] = {}
    for raw_pair, raw_value in raw.items():
        parts = str(raw_pair or "").strip().lower().replace("|", ":").split(":")
        pair = canonical_venue_pair(*parts) if len(parts) == 2 else ""
        if not pair or isinstance(raw_value, bool):
            continue
        try:
            value = float(raw_value)
        except (TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(value) and value >= 0.0:
            normalized[pair] = value
    return normalized


def canary_edge_floors(
    strategy: object,
    long_venue: object,
    short_venue: object,
) -> tuple[float, float]:
    pair = canonical_venue_pair(long_venue, short_venue)
    expected_by_pair = normalized_pair_floor_map(
        getattr(strategy, "funding_canary_min_expected_net_edge_bps_by_venue_pair", {})
    )
    worst_by_pair = normalized_pair_floor_map(
        getattr(strategy, "funding_canary_min_worst_case_edge_bps_by_venue_pair", {})
    )
    return (
        expected_by_pair.get(
            pair,
            float(getattr(strategy, "funding_canary_min_expected_net_edge_bps", 0.0)),
        ),
        worst_by_pair.get(
            pair,
            float(getattr(strategy, "funding_canary_min_worst_case_edge_bps", 0.0)),
        ),
    )


def canary_fee_assurance_tier(candidate: object, strategy: object) -> str:
    if getattr(candidate, "account_fee_evidence_complete", False) is True:
        return "account"
    if (
        getattr(strategy, "funding_canary_require_account_fee_evidence", True)
        is not True
        and getattr(candidate, "taker_fee_evidence_complete", False) is True
    ):
        return "conservative"
    return "unavailable"


def canary_notional_cap(candidate: object, strategy: object) -> float:
    return canary_notional_cap_for_tier(
        canary_fee_assurance_tier(candidate, strategy), strategy
    )


def canary_notional_cap_for_tier(tier: str, strategy: object) -> float:
    configured = max(
        float(getattr(strategy, "funding_canary_max_entry_notional_quote", 0.0)),
        0.0,
    )
    if str(tier or "").lower() != "conservative":
        return configured
    fallback = max(float(
        getattr(
            strategy,
            "funding_canary_conservative_fee_max_entry_notional_quote",
            0.0,
        )
    ), 0.0)
    return min(configured, fallback)
