"""Hyperliquid adapter (HyperliquidInfoApi / HyperliquidExchangeApi)."""

from __future__ import annotations

from lightfee.core.domain import (
    OrderFill,
    OrderRequest,
    PositionSnapshot,
    Venue,
    VenueMarketSnapshot,
)


class HyperliquidAdapter:
    @property
    def venue(self) -> Venue:
        return Venue.HYPERLIQUID

    async def fetch_market_snapshot(self, symbols: list[str]) -> VenueMarketSnapshot:
        return VenueMarketSnapshot(venue=Venue.HYPERLIQUID, observed_at_ms=0)

    async def place_order(self, request: OrderRequest) -> OrderFill:
        raise NotImplementedError("Hyperliquid order placement requires live credentials")

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        raise NotImplementedError("Hyperliquid position fetch requires live credentials")
