"""Gate Futures V4 adapter (dual position mode account)."""

from __future__ import annotations

from lightfee.core.domain import (
    OrderFill,
    OrderRequest,
    PositionSnapshot,
    Venue,
    VenueMarketSnapshot,
)


class GateAdapter:
    @property
    def venue(self) -> Venue:
        return Venue.GATE

    async def fetch_market_snapshot(self, symbols: list[str]) -> VenueMarketSnapshot:
        return VenueMarketSnapshot(venue=Venue.GATE, observed_at_ms=0)

    async def place_order(self, request: OrderRequest) -> OrderFill:
        raise NotImplementedError("Gate order placement requires live credentials")

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        raise NotImplementedError("Gate position fetch requires live credentials")
