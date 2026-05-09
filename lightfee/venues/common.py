"""Venue common utilities: quantity normalization, sizing, reduce-only exemptions."""

from __future__ import annotations

from lightfee.core.domain import Venue
from lightfee.core.money import floor_to_step, normalize_order_quantity


def venue_reduce_only_close_exempts_min_notional(venue: Venue) -> bool:
    """Aster and Binance reduce-only closes are exempt from min notional checks."""
    return venue in (Venue.ASTER, Venue.BINANCE)


__all__ = [
    "floor_to_step",
    "normalize_order_quantity",
    "venue_reduce_only_close_exempts_min_notional",
]
