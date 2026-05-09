"""Venue registry: maps venue name to adapter and capabilities."""

from __future__ import annotations

from lightfee.core.domain import Venue
from lightfee.venues.base import VenueCapabilities


def get_capabilities(venue: Venue) -> VenueCapabilities:
    return VenueCapabilities.for_venue(venue)


def all_venues() -> list[Venue]:
    return list(Venue)


def all_live_perp_venues() -> list[Venue]:
    return [
        Venue.BINANCE,
        Venue.OKX,
        Venue.BYBIT,
        Venue.BITGET,
        Venue.GATE,
        Venue.ASTER,
        Venue.HYPERLIQUID,
    ]
