"""OKX V5 adapter (OkxV5 unified account)."""

from __future__ import annotations

from lightfee.core.domain import (
    OrderFill,
    OrderRequest,
    PositionSnapshot,
    Venue,
    VenueMarketSnapshot,
)


class OkxAdapter:
    @property
    def venue(self) -> Venue:
        return Venue.OKX

    async def fetch_market_snapshot(self, symbols: list[str]) -> VenueMarketSnapshot:
        return VenueMarketSnapshot(venue=Venue.OKX, observed_at_ms=0)

    async def place_order(self, request: OrderRequest) -> OrderFill:
        raise NotImplementedError("OKX order placement requires live credentials")

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        raise NotImplementedError("OKX position fetch requires live credentials")
