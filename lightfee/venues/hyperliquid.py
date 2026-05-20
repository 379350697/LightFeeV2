"""Hyperliquid adapter (HyperliquidInfoApi / HyperliquidExchangeApi)."""

from __future__ import annotations

from typing import Any, Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import (
    OrderFill,
    OrderFillReconciliation,
    OrderRequest,
    PositionSnapshot,
    Side,
    Venue,
    VenueMarketSnapshot,
)
from lightfee.venues.specs import hyperliquid_spec
from lightfee.venues.transport import LiveCredential, VenueTransport


class HyperliquidAdapter(VenueAdapter):
    """Hyperliquid adapter — native perp account, risk-health unsupported."""

    def __init__(
        self,
        mode: str = "paper",
        credential: Optional[LiveCredential] = None,
        exchange_http_timeout_ms: int = 10000,
        rate_limiter: Any = None,
    ) -> None:
        spec = hyperliquid_spec()
        self._transport = VenueTransport(spec=spec, mode=mode, credential=credential,
                                         exchange_http_timeout_ms=exchange_http_timeout_ms,
                                         rate_limiter=rate_limiter)
        self._credential = credential

    @property
    def venue(self) -> Venue:
        return Venue.HYPERLIQUID

    @property
    def supports_private_health(self) -> bool:
        return self._transport.mode == "live"

    def supported_symbols(self) -> list[str]:
        """Return loaded Hyperliquid perp asset names, if available."""
        metadata = getattr(self._transport, "_symbol_metadata", {}) or {}
        return sorted(str(symbol) for symbol in metadata.keys())

    async def ensure_supported_symbols_loaded(self) -> None:
        """Populate the Hyperliquid perp universe, excluding delisted assets."""
        if self._transport._symbol_metadata:
            return
        raw = await self._transport._request(
            "POST",
            "/info",
            body={"type": "meta"},
            private=False,
        )
        universe = raw.get("universe", []) if isinstance(raw, dict) else []
        metadata: dict[str, dict[str, Any]] = {}
        for row in universe:
            if not isinstance(row, dict):
                continue
            name = str(row.get("name", ""))
            if not name:
                continue
            if bool(row.get("isDelisted", False)):
                continue
            metadata[name] = dict(row)
        self._transport.set_symbol_metadata(metadata)

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

    # ------------------------------------------------------------------
    # Hyperliquid order reconciliation (V1 parity)
    # ------------------------------------------------------------------

    async def fetch_order_fill_reconciliation(
        self,
        symbol: str,
        order_id: str,
        client_order_id: Optional[str] = None,
    ) -> Optional[OrderFillReconciliation]:
        """Query Hyperliquid info endpoint for order status by oid or cloid.

        Hyperliquid info API endpoints:
        - "orderStatus": query by oid (integer) or cloid (16-byte hex)
        - "historicalOrders": fallback listing of recent user orders

        Returns OrderFillReconciliation with raw exchange status on success,
        or None if the order cannot be found.
        """
        return await self._fetch_order_status_hl(symbol, order_id, client_order_id)

    async def _fetch_order_status_hl(
        self,
        symbol: str,
        order_id: str,
        client_order_id: Optional[str] = None,
    ) -> Optional[OrderFillReconciliation]:
        transport = self._transport
        cred = self._credential
        if cred is None or not cred.account_address:
            return None

        user_addr = cred.account_address
        now_ms = int(__import__("time").time() * 1000)

        # --- Attempt 1: orderStatus by oid ---
        if order_id:
            try:
                oid = int(order_id)
                raw = await transport._request(
                    "POST", "/info",
                    body={"type": "orderStatus", "user": user_addr, "oid": oid},
                    private=False,
                )
                result = self._parse_hl_order_status(raw, symbol, now_ms)
                if result is not None:
                    return result
            except (ValueError, Exception):
                pass

        # --- Attempt 2: orderStatus by cloid ---
        if client_order_id:
            try:
                raw = await transport._request(
                    "POST", "/info",
                    body={"type": "orderStatus", "user": user_addr, "cloid": client_order_id},
                    private=False,
                )
                result = self._parse_hl_order_status(raw, symbol, now_ms)
                if result is not None:
                    return result
            except Exception:
                pass

        # --- Attempt 3: historicalOrders fallback ---
        try:
            raw = await transport._request(
                "POST", "/info",
                body={"type": "historicalOrders", "user": user_addr},
                private=False,
            )
            return self._parse_hl_historical_orders(
                raw, symbol, order_id, client_order_id, now_ms,
            )
        except Exception:
            pass

        return None

    @staticmethod
    def _parse_hl_order_status(
        raw: dict[str, Any], symbol: str, now_ms: int,
    ) -> Optional[OrderFillReconciliation]:
        """Parse Hyperliquid orderStatus response.

        Response shape:
          {"order": {"order": {oid, cloid, coin, side, status, sz, limitPx, avgPx, ...}, "status": "order"}}
        Status values: "filled", "open", "canceled", "rejected"
        """
        order_wrapper = raw.get("order", raw)
        if not isinstance(order_wrapper, dict):
            return None

        order = order_wrapper.get("order", order_wrapper)
        if not isinstance(order, dict):
            return None

        status = str(order.get("status", "")).lower()
        oid = str(order.get("oid", ""))
        cloid = str(order.get("cloid", ""))
        side_raw = str(order.get("side", "")).upper()
        side = Side.BUY if side_raw == "B" else Side.SELL
        orig_sz = float(order.get("origSz", 0))
        sz = float(order.get("sz", 0))
        total_sz = float(order.get("totalSz", sz))
        limit_px = float(order.get("limitPx", 0))
        avg_px = float(order.get("avgPx", 0))

        if status == "filled":
            return OrderFillReconciliation(
                venue=Venue.HYPERLIQUID,
                symbol=symbol,
                side=side,
                quantity=total_sz,
                average_price=avg_px if avg_px > 0 else limit_px,
                order_id=oid,
                client_order_id=cloid,
                filled_at_ms=now_ms,
                metadata={
                    "raw_exchange_status": status,
                    "orig_sz": orig_sz,
                    "response_type": "orderStatus",
                },
            )

        if status in ("open", "resting", "triggered"):
            # Order is resting — not yet filled, return None for now
            return None

        if status in ("canceled", "rejected"):
            # Terminal non-fill — record for evidence
            return OrderFillReconciliation(
                venue=Venue.HYPERLIQUID,
                symbol=symbol,
                side=side,
                quantity=0.0,
                average_price=0.0,
                order_id=oid,
                client_order_id=cloid,
                filled_at_ms=now_ms,
                metadata={
                    "raw_exchange_status": status,
                    "response_type": "orderStatus",
                    "terminal_non_fill": True,
                },
            )

        # Unknown status — record but don't claim terminal
        return OrderFillReconciliation(
            venue=Venue.HYPERLIQUID,
            symbol=symbol,
            side=side,
            quantity=0.0,
            average_price=0.0,
            order_id=oid,
            client_order_id=cloid,
            filled_at_ms=now_ms,
            metadata={
                "raw_exchange_status": status,
                "response_type": "orderStatus",
                "orig_sz": orig_sz,
            },
        )

    @staticmethod
    def _parse_hl_historical_orders(
        raw: Any, symbol: str, order_id: str,
        client_order_id: Optional[str], now_ms: int,
    ) -> Optional[OrderFillReconciliation]:
        """Parse Hyperliquid historicalOrders response for a matching order.

        Response is a list of order dicts (same shape as orderStatus.order).
        """
        orders = raw if isinstance(raw, list) else []
        if isinstance(raw, dict):
            orders = raw.get("historicalOrders", raw.get("orders", []))

        for entry in orders:
            if not isinstance(entry, dict):
                continue
            entry_oid = str(entry.get("oid", ""))
            entry_cloid = str(entry.get("cloid", ""))
            if order_id and entry_oid != order_id:
                continue
            if client_order_id and entry_cloid != client_order_id:
                continue

            status = str(entry.get("status", "")).lower()
            side_raw = str(entry.get("side", "")).upper()
            side = Side.BUY if side_raw == "B" else Side.SELL
            total_sz = float(entry.get("totalSz", entry.get("sz", 0)))
            avg_px = float(entry.get("avgPx", 0))
            limit_px = float(entry.get("limitPx", 0))

            if status == "filled" and total_sz > 0:
                return OrderFillReconciliation(
                    venue=Venue.HYPERLIQUID,
                    symbol=symbol,
                    side=side,
                    quantity=total_sz,
                    average_price=avg_px if avg_px > 0 else limit_px,
                    order_id=entry_oid,
                    client_order_id=entry_cloid,
                    filled_at_ms=now_ms,
                    metadata={
                        "raw_exchange_status": status,
                        "response_type": "historicalOrders",
                    },
                )

            # Record non-fill terminal states
            if status in ("canceled", "rejected"):
                return OrderFillReconciliation(
                    venue=Venue.HYPERLIQUID,
                    symbol=symbol,
                    side=side,
                    quantity=0.0,
                    average_price=0.0,
                    order_id=entry_oid,
                    client_order_id=entry_cloid,
                    filled_at_ms=now_ms,
                    metadata={
                        "raw_exchange_status": status,
                        "response_type": "historicalOrders",
                        "terminal_non_fill": True,
                    },
                )

        return None
