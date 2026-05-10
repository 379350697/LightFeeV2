"""Aster Perpetuals FAPI adapter (AsterBalanceV2 AccountV4 PositionV2)."""

from __future__ import annotations

from typing import Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import (
    OrderFill,
    OrderRequest,
    PositionSnapshot,
    Venue,
    VenueMarketSnapshot,
)
from lightfee.venues.specs import aster_spec
from lightfee.venues.transport import LiveCredential, VenueTransport


class AsterAdapter(VenueAdapter):
    """Aster Perpetuals FAPI adapter — separate balance and position surfaces."""

    def __init__(
        self,
        mode: str = "paper",
        credential: Optional[LiveCredential] = None,
    ) -> None:
        spec = aster_spec()
        self._transport = VenueTransport(spec=spec, mode=mode, credential=credential)

    @property
    def venue(self) -> Venue:
        return Venue.ASTER

    async def fetch_market_snapshot(self, symbols: list[str]) -> VenueMarketSnapshot:
        return await self._transport.fetch_market_snapshot(symbols)

    async def place_order(self, request: OrderRequest) -> OrderFill:
        return await self._transport.place_order(request)

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        return await self._transport.fetch_position(symbol)

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        return await self._transport.normalize_quantity(symbol, quantity)

    async def shutdown(self) -> None:
        await self._transport.close()
