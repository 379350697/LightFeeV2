"""Aster adapter.

Public market data remains on Aster FAPI. Private account/order operations use
Aster Pro API V3 and do not share Binance HMAC signing.
"""

from __future__ import annotations

from typing import Any, Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import (
    OrderFill,
    OrderRequest,
    PassiveOrderAck,
    PassiveOrderProgress,
    PositionSnapshot,
    Venue,
    VenueMarketSnapshot,
)
from lightfee.venues.aster_v3 import AsterV3Client
from lightfee.venues.specs import aster_spec
from lightfee.venues.transport import (
    LiveCredential,
    TransportError,
    TransportErrorCategory,
    VenueTransport,
)


class AsterAdapter(VenueAdapter):
    """Aster public FAPI + private Pro API V3 adapter."""

    def __init__(
        self,
        mode: str = "paper",
        credential: Optional[LiveCredential] = None,
        exchange_http_timeout_ms: int = 10000,
        rate_limiter: Any = None,
    ) -> None:
        spec = aster_spec()
        self._transport = VenueTransport(
            spec=spec,
            mode=mode,
            credential=credential,
            exchange_http_timeout_ms=exchange_http_timeout_ms,
            rate_limiter=rate_limiter,
        )
        self._mode = mode
        self._private_disabled_reason = ""
        self._private: AsterV3Client | None = None
        if mode == "live" and credential is not None:
            from lightfee.venues.aster_v3 import credential_has_aster_v3_signer

            if credential_has_aster_v3_signer(credential):
                self._private = AsterV3Client(
                    credential=credential,
                    exchange_http_timeout_ms=exchange_http_timeout_ms,
                    rate_limiter=rate_limiter,
                )
            else:
                self._private_disabled_reason = (
                    "invalid_or_missing_aster_api_wallet_private_key"
                )

    @property
    def venue(self) -> Venue:
        return Venue.ASTER

    @property
    def supports_risk_health(self) -> bool:
        return self._transport.mode == "live"

    @property
    def supports_private_health(self) -> bool:
        return False

    def supported_symbols(self) -> list[str]:
        """Return loaded Aster trading symbols, if available."""
        metadata = getattr(self._transport, "_symbol_metadata", {}) or {}
        return sorted(str(symbol) for symbol in metadata.keys())

    def _private_unavailable(self) -> TransportError:
        reason = self._private_disabled_reason or "aster_v3_private_client_unavailable"
        return TransportError(
            TransportErrorCategory.AUTH_FAILURE,
            f"aster private API disabled: {reason}",
        )

    async def ensure_supported_symbols_loaded(self) -> None:
        """Populate the Aster contract catalog with actively trading symbols."""
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
            status = str(row.get("status", row.get("contractStatus", "TRADING"))).upper()
            if status != "TRADING":
                continue
            contract_type = str(row.get("contractType", "PERPETUAL")).upper()
            if contract_type != "PERPETUAL":
                continue
            metadata[symbol] = dict(row)
        self._transport.set_symbol_metadata(metadata)

    async def fetch_market_snapshot(self, symbols: list[str]) -> VenueMarketSnapshot:
        return await self._transport.fetch_market_snapshot(symbols)

    async def place_order(self, request: OrderRequest) -> OrderFill:
        if self._private is not None:
            return await self._private.place_order(request)
        if self._mode == "live":
            raise self._private_unavailable()
        return await self._transport.place_order(request)

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        if self._private is not None:
            return await self._private.fetch_position(symbol)
        if self._mode == "live":
            raise self._private_unavailable()
        return await self._transport.fetch_position(symbol)

    async def fetch_all_positions(self) -> list[PositionSnapshot]:
        if self._private is not None:
            return await self._private.fetch_all_positions()
        if self._mode == "live":
            raise self._private_unavailable()
        return await self._transport.fetch_all_positions()

    async def fetch_account_risk_snapshot(self):
        if self._private is not None:
            return await self._private.fetch_account_risk_snapshot()
        if self._mode == "live":
            raise self._private_unavailable()
        return await self._transport.fetch_account_risk_snapshot()

    async def ensure_entry_leverage(
        self,
        symbol: str,
        leverage: int,
        *,
        notional_quote: float | None = None,
    ) -> None:
        if self._private is not None:
            return await self._private.ensure_entry_leverage(
                symbol,
                leverage,
                notional_quote=notional_quote,
            )
        if self._mode == "live":
            raise self._private_unavailable()
        return await self._transport.ensure_entry_leverage(
            symbol,
            leverage,
            notional_quote=notional_quote,
        )

    async def fetch_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        if self._private is not None:
            return await self._private.fetch_open_orders(symbol)
        if self._mode == "live":
            raise self._private_unavailable()
        return []

    async def submit_passive_order(self, request: OrderRequest) -> PassiveOrderAck:
        if self._private is not None:
            return await self._private.submit_passive_order(request)
        if self._mode == "live":
            raise self._private_unavailable()
        return await self._transport.submit_passive_order(request)

    async def query_passive_order_progress(
        self,
        symbol: str,
        order_id: str,
        client_order_id: Optional[str] = None,
        side: Any = None,
    ) -> PassiveOrderProgress | None:
        if self._private is not None:
            return await self._private.query_passive_order_progress(
                symbol, order_id, client_order_id, side,
            )
        if self._mode == "live":
            raise self._private_unavailable()
        return await self._transport.query_passive_order_progress(
            symbol, order_id, client_order_id, side,
        )

    async def cancel_passive_order(
        self,
        symbol: str,
        order_id: str,
        client_order_id: Optional[str] = None,
    ) -> PassiveOrderAck:
        if self._private is not None:
            return await self._private.cancel_passive_order(
                symbol, order_id, client_order_id,
            )
        if self._mode == "live":
            raise self._private_unavailable()
        return await self._transport.cancel_passive_order(
            symbol, order_id, client_order_id,
        )

    async def fetch_order_status(
        self,
        symbol: str,
        order_id: str = "",
        client_order_id: Optional[str] = None,
    ) -> OrderFill | None:
        if self._private is not None:
            return await self._private.fetch_order_status(
                symbol, order_id, client_order_id,
            )
        if self._mode == "live":
            raise self._private_unavailable()
        return await self._transport.fetch_order_status(
            symbol, order_id, client_order_id,
        )

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        return await self._transport.normalize_quantity(symbol, quantity)

    async def shutdown(self) -> None:
        if self._private is not None:
            await self._private.close()
        await self._transport.close()
