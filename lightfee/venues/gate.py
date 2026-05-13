"""Gate Futures V4 adapter (dual position mode account)."""

from __future__ import annotations

from typing import Any, Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import (
    OrderFill,
    OrderRequest,
    PositionSnapshot,
    Venue,
    VenueMarketSnapshot,
)
from lightfee.venues.specs import gate_spec
from lightfee.venues.transport import LiveCredential, VenueTransport


class GateAdapter(VenueAdapter):
    """Gate Futures V4 adapter with dual-position mode and decimal contract sizes."""

    def __init__(
        self,
        mode: str = "paper",
        credential: Optional[LiveCredential] = None,
        exchange_http_timeout_ms: int = 10000,
        rate_limiter: Any = None,
    ) -> None:
        spec = gate_spec()
        self._transport = VenueTransport(spec=spec, mode=mode, credential=credential,
                                         exchange_http_timeout_ms=exchange_http_timeout_ms,
                                         rate_limiter=rate_limiter)
        self._mode = mode

    @property
    def venue(self) -> Venue:
        return Venue.GATE

    @property
    def supports_risk_health(self) -> bool:
        # V1 parity: Gate risk_health is UNSUPPORTED — the account endpoint
        # does not provide reliable margin/equity data for risk evaluation.
        return False

    async def fetch_market_snapshot(self, symbols: list[str]) -> VenueMarketSnapshot:
        return await self._transport.fetch_market_snapshot(symbols)

    async def place_order(self, request: OrderRequest) -> OrderFill:
        return await self._transport.place_order(request)

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        return await self._transport.fetch_position(symbol)

    async def fetch_account_risk_snapshot(self):
        return await self._transport.fetch_account_risk_snapshot()

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        return await self._transport.normalize_quantity(symbol, quantity)

    async def shutdown(self) -> None:
        await self._transport.close()
