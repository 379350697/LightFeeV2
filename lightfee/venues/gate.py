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

    @property
    def supports_private_health(self) -> bool:
        return self._mode == "live"

    def supported_symbols(self) -> list[str]:
        """Return loaded Gate USDT futures symbols in canonical LightFee format."""
        metadata = getattr(self._transport, "_symbol_metadata", {}) or {}
        spec = gate_spec()
        symbols: set[str] = set()
        for symbol in metadata:
            symbol_text = str(symbol)
            if not symbol_text:
                continue
            symbols.add(spec.symbol_from_venue(symbol_text) if spec.symbol_from_venue else symbol_text)
        return sorted(symbols)

    async def ensure_supported_symbols_loaded(self) -> None:
        """Populate Gate futures contract catalog for recovery probe filtering."""
        if self._transport._symbol_metadata:
            return
        raw = await self._transport._request(
            "GET",
            "/api/v4/futures/usdt/contracts",
            private=False,
        )
        rows = raw.get("data", raw) if isinstance(raw, dict) else raw
        items = rows if isinstance(rows, list) else [rows]
        spec = gate_spec()
        metadata: dict[str, dict[str, Any]] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            venue_symbol = str(
                item.get("name")
                or item.get("contract")
                or item.get("symbol")
                or ""
            ).upper()
            if not venue_symbol:
                continue
            canonical = (
                spec.symbol_from_venue(venue_symbol)
                if spec.symbol_from_venue
                else venue_symbol
            )
            if not canonical.endswith("USDT"):
                continue
            status = str(
                item.get("status")
                or item.get("trade_status")
                or "trading"
            ).lower()
            if status not in ("trading", "tradable", "open"):
                continue
            if bool(item.get("in_delisting", False)):
                continue
            metadata[venue_symbol] = dict(item)
        self._transport.set_symbol_metadata(metadata)

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
