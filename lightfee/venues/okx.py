"""OKX V5 adapter (OkxV5 unified account)."""

from __future__ import annotations

from typing import Any, Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import (
    OrderFill,
    OrderFillReconciliation,
    OrderRequest,
    PositionSnapshot,
    Venue,
    VenueMarketSnapshot,
)
from lightfee.venues.specs import okx_spec
from lightfee.venues.transport import LiveCredential, VenueTransport


class OkxAdapter(VenueAdapter):
    """OKX V5 unified account adapter."""

    def __init__(
        self,
        mode: str = "paper",
        credential: Optional[LiveCredential] = None,
        exchange_http_timeout_ms: int = 10000,
        rate_limiter: Any = None,
    ) -> None:
        spec = okx_spec()
        self._transport = VenueTransport(spec=spec, mode=mode, credential=credential,
                                         exchange_http_timeout_ms=exchange_http_timeout_ms,
                                         rate_limiter=rate_limiter)

    @property
    def venue(self) -> Venue:
        return Venue.OKX

    @property
    def supports_risk_health(self) -> bool:
        return self._transport.mode == "live"

    @property
    def supports_private_health(self) -> bool:
        return self._transport.mode == "live"

    def supported_symbols(self) -> list[str]:
        """Return loaded OKX SWAP symbols, if the instrument catalog is available."""
        metadata = getattr(self._transport, "_symbol_metadata", {}) or {}
        symbols: set[str] = set()
        for symbol in metadata:
            symbol_text = str(symbol)
            if not symbol_text:
                continue
            if "-SWAP" in symbol_text:
                symbols.add(okx_spec().symbol_from_venue(symbol_text))
            elif symbol_text.endswith("USDT"):
                symbols.add(symbol_text)
        return sorted(symbols)

    async def ensure_supported_symbols_loaded(self) -> None:
        """Populate OKX SWAP instrument metadata for recovery catalog gating."""
        if self._transport._symbol_metadata:
            return
        await self._transport._ensure_okx_swap_instrument_metadata_loaded()

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
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> Optional[OrderFillReconciliation]:
        return await self._transport.fetch_order_status(
            symbol,
            order_id=order_id,
            client_order_id=client_order_id or "",
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )

    async def fetch_account_fill_reconciliations(
        self,
        symbol: str,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[OrderFillReconciliation]:
        return await self._transport.fetch_account_fill_reconciliations(
            symbol,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )

    async def shutdown(self) -> None:
        await self._transport.close()
