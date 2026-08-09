"""Aster adapter.

Public market data remains on Aster FAPI. Private account/order operations use
Aster Pro API V3 and do not share Binance HMAC signing.
"""

from __future__ import annotations

import asyncio
import time
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
from lightfee.venues.entry_tradability import (
    entry_tradability_blocked,
    entry_tradability_unavailable,
)
from lightfee.venues.specs import aster_spec
from lightfee.venues.transport import (
    LiveCredential,
    TransportError,
    TransportErrorCategory,
    VenueTransport,
)


# Aster exchangeInfo catalog refresh TTL.  The directory is not allowed to be
# permanently valid after a single load: delisted/unsupported contracts must be
# re-discovered on refresh while remaining excluded via the negative cache.
_ASTER_EXCHANGE_INFO_TTL_MS = 300_000
# Entry admission requires a fresh server response, not a short-lived view
# reused from a prior candidate.
_ASTER_ENTRY_TRADABILITY_CATALOG_TTL_MS = 0

# How long an invalid-symbol (-1121) negative-cache entry stays effective before
# the symbol may be re-admitted on a future refresh.
_ASTER_UNSUPPORTED_SYMBOL_TTL_MS = 3_600_000


def _is_invalid_symbol_transport_error(exc: Exception) -> bool:
    """Return True if the exception (or its cause chain) is an Aster -1121.

    AsterV3Client._request marks TransportError with ``invalid_symbol=True`` at
    the raw-response choke point.  Order paths convert that TransportError into
    OrderSubmitError, so this walks the __cause__/__context__ chain and also
    accepts the generic "-1121"/"invalid symbol" body marker.
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if getattr(current, "invalid_symbol", False):
            return True
        text = f"{current} {getattr(current, 'body', '')}".lower()
        if "-1121" in text or "invalid symbol" in text:
            return True
        transport_error = getattr(current, "transport_error", None)
        if transport_error is not None and id(transport_error) not in seen:
            if getattr(transport_error, "invalid_symbol", False):
                return True
            inner = f"{transport_error} {getattr(transport_error, 'body', '')}".lower()
            if "-1121" in inner or "invalid symbol" in inner:
                return True
        current = current.__cause__ or current.__context__
    return False


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
        self._symbol_metadata_loaded_at_ms: int = 0
        self._entry_tradability_catalog: dict[str, dict[str, Any]] = {}
        self._entry_tradability_catalog_at_ms = 0
        self._entry_tradability_catalog_lock = asyncio.Lock()
        # negative cache: symbol -> marked_at_ms for exchange -1121 evidence
        self._unsupported_symbols: dict[str, int] = {}
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
        """Return loaded Aster trading symbols, excluding invalidated ones."""
        metadata = getattr(self._transport, "_symbol_metadata", {}) or {}
        return sorted(
            str(symbol)
            for symbol in metadata.keys()
            if not self._is_unsupported_symbol(str(symbol))
        )

    def _is_unsupported_symbol(self, symbol: str) -> bool:
        marked_at_ms = self._unsupported_symbols.get(symbol, 0)
        if marked_at_ms <= 0:
            return False
        now_ms = int(time.time() * 1000)
        if now_ms - marked_at_ms >= _ASTER_UNSUPPORTED_SYMBOL_TTL_MS:
            self._unsupported_symbols.pop(symbol, None)
            return False
        return True

    def mark_symbol_unsupported(
        self,
        symbol: str,
        *,
        endpoint: str,
        exchange_code: str,
        status_code: int = 0,
    ) -> None:
        """Invalidate a symbol locally on exchange -1121 evidence.

        The symbol is removed from the trading catalog (so it can never be
        admitted again) and recorded in a negative cache.  A structured
        diagnostic is buffered through the transport so the runtime flushes it
        into the journal without re-sending the rejected private request.  The
        diagnostic is emitted exactly once per symbol; subsequent -1121
        evidence for the same symbol stays in the negative cache silently.
        """
        symbol_text = str(symbol)
        already = self._is_unsupported_symbol(symbol_text)
        self._unsupported_symbols[symbol_text] = int(time.time() * 1000)
        metadata = getattr(self._transport, "_symbol_metadata", None)
        if isinstance(metadata, dict):
            metadata.pop(symbol_text, None)
        if already:
            return
        record = getattr(self._transport, "_record_order_diagnostic", None)
        if callable(record):
            record(
                "venues.aster.unsupported_symbol",
                {
                    "venue": self.venue.value,
                    "symbol": symbol_text,
                    "endpoint": endpoint,
                    "exchange_code": exchange_code,
                    "status_code": status_code,
                    "reason": "exchange_invalid_symbol",
                },
            )

    def _handle_private_invalid_symbol(
        self,
        exc: Exception,
        symbol: str,
        *,
        endpoint: str,
    ) -> None:
        if not _is_invalid_symbol_transport_error(exc):
            return
        status_code = int(getattr(exc, "status_code", 0) or 0)
        cause = exc.__cause__ or exc.__context__
        if status_code == 0 and cause is not None:
            status_code = int(getattr(cause, "status_code", 0) or 0)
        self.mark_symbol_unsupported(
            symbol,
            endpoint=endpoint,
            exchange_code="-1121",
            status_code=status_code,
        )

    def _private_unavailable(self) -> TransportError:
        reason = self._private_disabled_reason or "aster_v3_private_client_unavailable"
        return TransportError(
            TransportErrorCategory.AUTH_FAILURE,
            f"aster private API disabled: {reason}",
        )

    async def ensure_supported_symbols_loaded(self) -> None:
        """Populate the Aster contract catalog with actively trading symbols.

        The catalog is refreshed when empty or older than the TTL.  Symbols
        invalidated by -1121 stay excluded through the negative cache.
        """
        now_ms = int(time.time() * 1000)
        loaded_at = int(self._symbol_metadata_loaded_at_ms or 0)
        if (
            self._transport._symbol_metadata
            and loaded_at > 0
            and now_ms - loaded_at < _ASTER_EXCHANGE_INFO_TTL_MS
        ):
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
        self._symbol_metadata_loaded_at_ms = int(time.time() * 1000)

    async def precheck_entry_tradability(self, symbol: str) -> dict[str, Any]:
        """Check Aster's current FAPI contract state before opening a leg.

        Aster's endpoint is Binance-compatible and returns a full catalog. The
        execution-time catalog is refreshed per admission and is independent
        from discovery metadata.
        """
        venue_symbol = self._transport._venue_symbol(symbol)
        now_ms = int(time.time() * 1000)
        if now_ms - self._entry_tradability_catalog_at_ms >= _ASTER_ENTRY_TRADABILITY_CATALOG_TTL_MS:
            async with self._entry_tradability_catalog_lock:
                now_ms = int(time.time() * 1000)
                if now_ms - self._entry_tradability_catalog_at_ms >= _ASTER_ENTRY_TRADABILITY_CATALOG_TTL_MS:
                    raw = await self._transport._request("GET", "/fapi/v1/exchangeInfo")
                    if not isinstance(raw, dict) or not isinstance(raw.get("symbols"), list):
                        raise entry_tradability_unavailable(
                            Venue.ASTER.value,
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
                Venue.ASTER.value,
                venue_symbol,
                status="MISSING",
                contract_type="MISSING",
            )
        status = str(row.get("status", row.get("contractStatus", ""))).upper()
        contract_type = str(row.get("contractType", "")).upper()
        if status != "TRADING" or contract_type != "PERPETUAL":
            raise entry_tradability_blocked(
                Venue.ASTER.value,
                venue_symbol,
                status=status or "MISSING",
                contract_type=contract_type or "MISSING",
            )
        return {
            "venue": Venue.ASTER.value,
            "symbol": venue_symbol,
            "status": "ok",
            "contract_status": status,
            "contract_type": contract_type,
        }

    async def fetch_market_snapshot(self, symbols: list[str]) -> VenueMarketSnapshot:
        return await self._transport.fetch_market_snapshot(symbols)

    async def place_order(self, request: OrderRequest) -> OrderFill:
        if self._private is not None:
            try:
                prepared_request, _, _ = await self._transport.prepare_order_request(
                    request,
                    require_exchange_rules=self._mode == "live",
                )
                return await self._private.place_order(prepared_request)
            except Exception as exc:
                self._handle_private_invalid_symbol(
                    exc, request.symbol, endpoint="/fapi/v3/order"
                )
                raise
        if self._mode == "live":
            raise self._private_unavailable()
        return await self._transport.place_order(request)

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        if self._private is not None:
            try:
                return await self._private.fetch_position(symbol)
            except Exception as exc:
                self._handle_private_invalid_symbol(
                    exc, symbol, endpoint="/fapi/v3/positionRisk"
                )
                raise
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
            try:
                return await self._private.ensure_entry_leverage(
                    symbol,
                    leverage,
                    notional_quote=notional_quote,
                )
            except Exception as exc:
                self._handle_private_invalid_symbol(
                    exc, symbol, endpoint="/fapi/v3/leverage"
                )
                raise
        if self._mode == "live":
            raise self._private_unavailable()
        return await self._transport.ensure_entry_leverage(
            symbol,
            leverage,
            notional_quote=notional_quote,
        )

    async def fetch_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        if self._private is not None:
            if symbol:
                try:
                    return await self._private.fetch_open_orders(symbol)
                except Exception as exc:
                    self._handle_private_invalid_symbol(
                        exc, symbol, endpoint="/fapi/v3/openOrders"
                    )
                    raise
            return await self._private.fetch_open_orders(None)
        if self._mode == "live":
            raise self._private_unavailable()
        return []

    async def submit_passive_order(self, request: OrderRequest) -> PassiveOrderAck:
        if self._private is not None:
            try:
                prepared_request, _, _ = await self._transport.prepare_order_request(
                    request,
                    require_exchange_rules=self._mode == "live",
                )
                return await self._private.submit_passive_order(prepared_request)
            except Exception as exc:
                self._handle_private_invalid_symbol(
                    exc, request.symbol, endpoint="/fapi/v3/order"
                )
                raise
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
            try:
                return await self._private.query_passive_order_progress(
                    symbol, order_id, client_order_id, side,
                )
            except Exception as exc:
                self._handle_private_invalid_symbol(
                    exc, symbol, endpoint="/fapi/v3/order"
                )
                raise
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
            try:
                return await self._private.cancel_passive_order(
                    symbol, order_id, client_order_id,
                )
            except Exception as exc:
                self._handle_private_invalid_symbol(
                    exc, symbol, endpoint="/fapi/v3/order"
                )
                raise
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
            try:
                return await self._private.fetch_order_status(
                    symbol, order_id, client_order_id,
                )
            except Exception as exc:
                self._handle_private_invalid_symbol(
                    exc, symbol, endpoint="/fapi/v3/order"
                )
                raise
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
