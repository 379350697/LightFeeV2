"""Binance USDM futures adapter (BinanceUsdmRest / BinanceUsdmPrivateV3)."""

from __future__ import annotations

from typing import Optional

from lightfee.core.domain import (
    OrderFill,
    OrderRequest,
    PositionSnapshot,
    Side,
    Venue,
    VenueMarketQuote,
    VenueMarketSnapshot,
)


class BinanceAdapter:
    """Binance USDⓈ-M futures adapter."""

    @property
    def venue(self) -> Venue:
        return Venue.BINANCE

    async def fetch_market_snapshot(self, symbols: list[str]) -> VenueMarketSnapshot:
        return VenueMarketSnapshot(venue=Venue.BINANCE, observed_at_ms=0)

    async def place_order(self, request: OrderRequest) -> OrderFill:
        raise NotImplementedError("Binance order placement requires live credentials")

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        raise NotImplementedError("Binance position fetch requires live credentials")

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        return quantity
