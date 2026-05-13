"""Bybit V5 adapter (unified account)."""

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
from lightfee.venues.specs import bybit_spec
from lightfee.venues.transport import LiveCredential, VenueTransport


class BybitAdapter(VenueAdapter):
    """Bybit V5 unified account adapter."""

    def __init__(
        self,
        mode: str = "paper",
        credential: Optional[LiveCredential] = None,
        exchange_http_timeout_ms: int = 10000,
        rate_limiter: Any = None,
    ) -> None:
        spec = bybit_spec()
        self._transport = VenueTransport(spec=spec, mode=mode, credential=credential,
                                         exchange_http_timeout_ms=exchange_http_timeout_ms,
                                         rate_limiter=rate_limiter)

    @property
    def venue(self) -> Venue:
        return Venue.BYBIT

    @property
    def supports_risk_health(self) -> bool:
        return self._transport.mode == "live"

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

    async def fetch_order_fill_reconciliation(
        self,
        symbol: str,
        order_id: str,
        client_order_id: Optional[str] = None,
    ) -> Optional["OrderFillReconciliation"]:
        """V1: Bybit fetch_order_fill_reconciliation via /v5/order/realtime.

        Uses orderLinkId for client_order_id lookup (V1: bybit.rs:1522-1523).
        Falls through to transport.fetch_order_status then converts to
        OrderFillReconciliation.
        """
        from lightfee.core.domain import OrderFillReconciliation

        status = await self._transport.fetch_order_status(
            symbol, order_id=order_id, client_order_id=client_order_id or "",
        )
        if status is not None:
            return status
        return None

    async def shutdown(self) -> None:
        await self._transport.close()
