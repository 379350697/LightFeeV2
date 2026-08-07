"""Binance USDM futures adapter (BinanceUsdmRest / BinanceUsdmPrivateV3)."""

from __future__ import annotations

import asyncio
import time
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
from lightfee.venues.entry_tradability import (
    entry_tradability_blocked,
    entry_tradability_unavailable,
)
from lightfee.venues.specs import binance_spec
from lightfee.venues.transport import LiveCredential, VenueTransport


class BinanceAdapter(VenueAdapter):
    """Binance USDⓈ-M futures adapter."""

    _ENTRY_TRADABILITY_CATALOG_TTL_MS = 1_000

    def __init__(
        self,
        mode: str = "paper",
        credential: Optional[LiveCredential] = None,
        exchange_http_timeout_ms: int = 10000,
        rate_limiter: Any = None,
    ) -> None:
        spec = binance_spec()
        self._transport = VenueTransport(spec=spec, mode=mode, credential=credential,
                                         exchange_http_timeout_ms=exchange_http_timeout_ms,
                                         rate_limiter=rate_limiter)
        self._entry_tradability_catalog: dict[str, dict[str, Any]] = {}
        self._entry_tradability_catalog_at_ms = 0
        self._entry_tradability_catalog_lock = asyncio.Lock()

    @property
    def venue(self) -> Venue:
        return Venue.BINANCE

    @property
    def supports_risk_health(self) -> bool:
        return self._transport.mode == "live"

    @property
    def supports_private_health(self) -> bool:
        return self._transport.mode == "live"

    def supported_symbols(self) -> list[str]:
        """Return loaded Binance USD-M trading symbols, if available."""
        metadata = getattr(self._transport, "_symbol_metadata", {}) or {}
        return sorted(str(symbol) for symbol in metadata.keys())

    async def ensure_supported_symbols_loaded(self) -> None:
        """Populate the Binance contract catalog with actively trading symbols."""
        if self._transport._symbol_metadata:
            return
        raw = await self._transport._request("GET", "/fapi/v1/exchangeInfo")
        rows = raw.get("symbols", []) if isinstance(raw, dict) else []
        metadata: dict[str, dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol", ""))
            if not symbol:
                continue
            if str(row.get("status", "")).upper() != "TRADING":
                continue
            if str(row.get("contractType", "")).upper() != "PERPETUAL":
                continue
            metadata[symbol] = dict(row)
        self._transport.set_symbol_metadata(metadata)

    async def precheck_entry_tradability(self, symbol: str) -> dict[str, Any]:
        """Fail closed if Binance no longer accepts opening orders for ``symbol``.

        Binance documents ``GET /fapi/v1/exchangeInfo`` as an all-symbol
        catalog; it does not document a symbol filter. Keep a separate,
        one-second cache for this execution-time view so concurrent candidates
        do not repeatedly download the catalog, while never reusing the
        long-lived discovery catalog.
        """
        venue_symbol = self._transport._venue_symbol(symbol)
        now_ms = int(time.time() * 1000)
        if now_ms - self._entry_tradability_catalog_at_ms >= self._ENTRY_TRADABILITY_CATALOG_TTL_MS:
            async with self._entry_tradability_catalog_lock:
                now_ms = int(time.time() * 1000)
                if now_ms - self._entry_tradability_catalog_at_ms >= self._ENTRY_TRADABILITY_CATALOG_TTL_MS:
                    raw = await self._transport._request("GET", "/fapi/v1/exchangeInfo")
                    if not isinstance(raw, dict) or not isinstance(raw.get("symbols"), list):
                        raise entry_tradability_unavailable(
                            Venue.BINANCE.value,
                            venue_symbol,
                            "exchangeInfo_symbols_missing_or_malformed",
                        )
                    self._entry_tradability_catalog = {
                        str(item.get("symbol", "")).upper(): dict(item)
                        for item in raw["symbols"]
                        if isinstance(item, dict) and str(item.get("symbol", ""))
                    }
                    self._entry_tradability_catalog_at_ms = now_ms
        row = self._entry_tradability_catalog.get(venue_symbol.upper())
        if row is None:
            raise entry_tradability_blocked(
                Venue.BINANCE.value,
                venue_symbol,
                status="MISSING",
                contract_type="MISSING",
            )

        status = str(row.get("status", "")).upper()
        contract_type = str(row.get("contractType", "")).upper()
        delivery_date = str(row.get("deliveryDate", "") or "")
        if status != "TRADING" or contract_type != "PERPETUAL":
            raise entry_tradability_blocked(
                Venue.BINANCE.value,
                venue_symbol,
                status=status or "MISSING",
                contract_type=contract_type or "MISSING",
                delivery_date=delivery_date or "MISSING",
            )
        return {
            "venue": Venue.BINANCE.value,
            "symbol": venue_symbol,
            "status": "ok",
            "contract_status": status,
            "contract_type": contract_type,
            "delivery_date": delivery_date,
        }

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

    async def ensure_entry_leverage(
        self,
        symbol: str,
        leverage: int,
        *,
        notional_quote: float | None = None,
    ) -> None:
        await self._transport.ensure_entry_leverage(
            symbol,
            leverage,
            notional_quote=notional_quote,
        )

    async def fetch_order_fill_reconciliation(
        self,
        symbol: str,
        order_id: str,
        client_order_id: Optional[str] = None,
    ) -> Optional[OrderFillReconciliation]:
        return await self._transport.fetch_order_status(
            symbol,
            order_id=order_id,
            client_order_id=client_order_id or "",
        )

    async def shutdown(self) -> None:
        await self._transport.close()
