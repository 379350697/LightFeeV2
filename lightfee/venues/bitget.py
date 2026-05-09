"""Bitget Mix V2 adapter (detect classic vs UTA)."""

from __future__ import annotations

from lightfee.core.domain import (
    OrderFill,
    OrderRequest,
    PositionSnapshot,
    Venue,
    VenueMarketSnapshot,
)


class BitgetAdapter:
    @property
    def venue(self) -> Venue:
        return Venue.BITGET

    async def fetch_market_snapshot(self, symbols: list[str]) -> VenueMarketSnapshot:
        return VenueMarketSnapshot(venue=Venue.BITGET, observed_at_ms=0)

    async def place_order(self, request: OrderRequest) -> OrderFill:
        raise NotImplementedError("Bitget order placement requires live credentials")

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        raise NotImplementedError("Bitget position fetch requires live credentials")
