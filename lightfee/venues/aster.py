"""Aster adapter.

Public market data remains on Aster FAPI. Private account/order operations use
Aster Pro API V3 and do not share Binance HMAC signing.
"""

from __future__ import annotations

import asyncio
import math
import time
from typing import Any, Iterable, Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import (
    AccountFeeSnapshot,
    HistoricalCloseEvidenceDiscovery,
    OrderFill,
    OrderFillReconciliation,
    OrderRequest,
    PassiveOrderAck,
    PassiveOrderProgress,
    PositionSnapshot,
    Side,
    Venue,
    VenueMarketSnapshot,
    close_order_side_for_position,
)
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.venues.account_fees import fee_rate_from_mapping
from lightfee.venues.aster_v3 import (
    ASTER_V3_ORDER_PATH,
    ASTER_V3_USER_TRADES_PATH,
    AsterV3Client,
    _extract_rows,
)
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
_ASTER_HISTORY_TIME_WINDOW_MS = 300_000
_ASTER_QUOTE_FEE_ASSETS = frozenset({"USDT", "USDC"})


def _aster_history_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0.0 else None


def _aster_history_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed > 0 else None


def _aster_history_row_closes_position_side(
    raw: dict[str, Any],
    expected_position_side: str,
) -> bool:
    """Keep one-way-mode history out unless it proves reduce-only ownership."""
    observed_position_side = str(raw.get("positionSide") or "").upper()
    if observed_position_side == str(expected_position_side or "").upper():
        return True
    reduce_only = raw.get("reduceOnly")
    return observed_position_side == "BOTH" and (
        reduce_only is True
        or (isinstance(reduce_only, str) and reduce_only.lower() == "true")
    )


def find_aster_v3_historical_close_order_candidates(
    trades: Iterable[dict[str, Any]],
    *,
    symbol: str,
    side: Side | str,
    position_side: str,
    quantity: float,
    closed_at_ms: int,
    time_window_ms: int = _ASTER_HISTORY_TIME_WINDOW_MS,
    quantity_relative_tolerance: float = 1e-9,
) -> list[dict[str, Any]]:
    """Group V3 user trades into every strictly matching close-order candidate."""
    expected_side = "BUY" if side == Side.BUY or str(side).upper() == "BUY" else "SELL"
    quantity_tolerance = max(quantity * quantity_relative_tolerance, 1e-12)
    grouped: dict[str, dict[str, Any]] = {}
    for raw in trades:
        if not isinstance(raw, dict):
            continue
        order_id = str(raw.get("orderId") or raw.get("order_id") or "").strip()
        trade_qty = _aster_history_float(raw.get("qty", raw.get("quantity")))
        trade_time_ms = _aster_history_int(raw.get("time", raw.get("timestamp")))
        if (
            not order_id
            or str(raw.get("symbol") or "").upper() != symbol.upper()
            or str(raw.get("side") or "").upper() != expected_side
            or not _aster_history_row_closes_position_side(raw, position_side)
            or trade_qty is None
            or trade_qty <= 1e-12
            or trade_time_ms is None
            or abs(trade_time_ms - closed_at_ms) > time_window_ms
        ):
            continue
        candidate = grouped.setdefault(
            order_id,
            {
                "order_id": order_id,
                "client_order_id": str(raw.get("clientOrderId") or ""),
                "quantity": 0.0,
                "updated_at_ms": 0,
                "trades": [],
            },
        )
        candidate["quantity"] += trade_qty
        candidate["updated_at_ms"] = max(candidate["updated_at_ms"], trade_time_ms)
        candidate["trades"].append(raw)
        client_order_id = str(raw.get("clientOrderId") or "")
        if candidate["client_order_id"] and client_order_id != candidate["client_order_id"]:
            candidate["client_order_id"] = ""
    return [
        candidate
        for candidate in grouped.values()
        if math.isclose(
            candidate["quantity"],
            quantity,
            rel_tol=quantity_relative_tolerance,
            abs_tol=quantity_tolerance,
        )
    ]


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


def _aster_transport_error_in_chain(exc: BaseException) -> TransportError | None:
    """Return the transport response carried by an Aster private-order error."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, TransportError):
            return current
        transport_error = getattr(current, "transport_error", None)
        if isinstance(transport_error, TransportError):
            return transport_error
        current = current.__cause__ or current.__context__
    return None


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

    async def fetch_account_fee_snapshot(
        self, reference_symbol: str = ""
    ) -> Optional[AccountFeeSnapshot]:
        venue_symbol = self._transport._venue_symbol(reference_symbol) if reference_symbol else ""
        if not venue_symbol or self._private is None:
            return None
        raw = await self._private._request(
            "GET",
            "/fapi/v3/commissionRate",
            params={"symbol": venue_symbol},
        )
        if not isinstance(raw, dict):
            raise ValueError("Aster commission-rate response is malformed")
        return AccountFeeSnapshot(
            venue=self.venue,
            maker_fee_bps=fee_rate_from_mapping(raw, "maker fee", "makerCommissionRate"),
            taker_fee_bps=fee_rate_from_mapping(raw, "taker fee", "takerCommissionRate"),
            observed_at_ms=int(time.time() * 1000),
            source="aster_v3_commission_rate",
        )

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

    def _record_private_order_submit_result(
        self,
        *,
        preflight: dict[str, Any],
        request: OrderRequest,
        operation: str,
        result: OrderFill | PassiveOrderAck | None = None,
        exc: BaseException | None = None,
    ) -> None:
        """Record Aster V3 order evidence through the shared transport buffer.

        Aster's private V3 client has a different signer/transport, so it
        cannot emit the generic transport submit record.  Keep both aggressive
        and passive paths on this single evidence contract: the live rule
        preflight, HTTP response status/body when supplied, and the final
        submit classification are all journaled together.
        """
        record = getattr(self._transport, "_record_order_diagnostic", None)
        if not callable(record):
            return
        payload = dict(preflight)
        payload.update({
            "venue": self.venue.value,
            "symbol": request.symbol,
            "operation": operation,
            "endpoint": "/fapi/v3/order",
            "private_api": "aster_v3",
        })
        if exc is None:
            payload["response_classification"] = (
                "ack_accepted" if operation == "submit_passive_order" else "filled"
            )
            if result is not None:
                payload["order_id"] = str(getattr(result, "order_id", "") or "")
                payload["client_order_id"] = str(
                    getattr(result, "client_order_id", "") or request.client_order_id or ""
                )
            record("order.private_submit_result", payload)
            return

        classification = getattr(getattr(exc, "class_", None), "value", "")
        payload["response_classification"] = str(classification or "uncertain")
        payload["response_msg"] = str(exc)[:500]
        transport_error = _aster_transport_error_in_chain(exc)
        if transport_error is not None:
            payload["status_code"] = int(transport_error.status_code or 0)
            payload["response_body"] = str(transport_error.body or "")[:500]
            payload["transport_category"] = transport_error.category.value
        record("order.private_submit_result", payload)

    def _record_private_admission_precheck_result(
        self,
        *,
        request: OrderRequest,
        result: dict[str, Any] | None = None,
        exc: BaseException | None = None,
        response_classification: str | None = None,
    ) -> None:
        """Persist Aster V3 capacity proof on the shared precheck contract."""
        record = getattr(self._transport, "_record_order_diagnostic", None)
        if not callable(record):
            return
        payload = {
            "venue": self.venue.value,
            "symbol": request.symbol,
            "endpoint": "/fapi/v3/positionRisk,/fapi/v3/openOrders",
            "private_api": "aster_v3",
        }
        if exc is not None:
            classification = getattr(getattr(exc, "class_", None), "value", "")
            payload["response_classification"] = str(
                response_classification or classification or "uncertain"
            )
            payload["response_msg"] = str(exc)[:500]
            transport_error = _aster_transport_error_in_chain(exc)
            if transport_error is not None:
                payload["status_code"] = int(transport_error.status_code or 0)
                payload["response_body"] = str(transport_error.body or "")[:500]
                payload["transport_category"] = transport_error.category.value
            record("order.precheck_result", payload)
            return

        for key in (
            "source",
            "requested_notional",
            "current_position_notional",
            "open_order_notional",
            "max_notional_value",
            "remaining_notional",
        ):
            if result is not None and key in result:
                payload[key] = result[key]
        payload["response_classification"] = "accepted"
        record("order.precheck_result", payload)

    async def _submit_private_order(
        self,
        request: OrderRequest,
        *,
        passive: bool,
    ) -> OrderFill | PassiveOrderAck:
        """Submit either Aster private order flavor through one evidence path."""
        operation = "submit_passive_order" if passive else "place_order"
        prepared_request, preflight, _ = await self._transport.prepare_order_request(
            request,
            require_exchange_rules=self._mode == "live",
        )
        try:
            await self.precheck_order_admission(prepared_request)
            result = (
                await self._private.submit_passive_order(prepared_request)
                if passive
                else await self._private.place_order(prepared_request)
            )
        except Exception as exc:
            self._record_private_order_submit_result(
                preflight=preflight,
                request=request,
                operation=operation,
                exc=exc,
            )
            self._handle_private_invalid_symbol(
                exc, request.symbol, endpoint="/fapi/v3/order"
            )
            raise
        self._record_private_order_submit_result(
            preflight=preflight,
            request=request,
            operation=operation,
            result=result,
        )
        return result

    async def precheck_order_admission(self, request: OrderRequest) -> dict[str, Any]:
        """Check V3 opening capacity without submitting an order."""
        if self._private is not None:
            try:
                result = await self._private.precheck_order_admission(request)
            except TransportError as exc:
                self._handle_private_invalid_symbol(
                    exc,
                    request.symbol,
                    endpoint="/fapi/v3/positionRisk",
                )
                if exc.category in (
                    TransportErrorCategory.AUTH_FAILURE,
                    TransportErrorCategory.AUTHORIZATION_FAILURE,
                    TransportErrorCategory.REQUEST_REJECTED,
                ):
                    mapped_error = OrderSubmitError(
                        SubmitFailureClass.REJECTED,
                        f"aster_v3 capacity precheck rejected: {exc}",
                    )
                    self._record_private_admission_precheck_result(
                        request=request,
                        exc=exc,
                        response_classification=SubmitFailureClass.REJECTED.value,
                    )
                    raise mapped_error from exc
                self._record_private_admission_precheck_result(request=request, exc=exc)
                raise
            except Exception as exc:
                self._record_private_admission_precheck_result(request=request, exc=exc)
                raise
            self._record_private_admission_precheck_result(
                request=request,
                result=result,
            )
            return result
        if self._mode == "live":
            raise self._private_unavailable()
        return {
            "venue": Venue.ASTER.value,
            "symbol": request.symbol,
            "status": "skipped",
            "reason": "paper_mode",
        }

    async def place_order(self, request: OrderRequest) -> OrderFill:
        if self._private is not None:
            result = await self._submit_private_order(request, passive=False)
            assert isinstance(result, OrderFill)
            return result
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

    @property
    def supports_entry_leverage_preparation(self) -> bool:
        return True

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
            result = await self._submit_private_order(request, passive=True)
            assert isinstance(result, PassiveOrderAck)
            return result
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

    async def discover_historical_close_fill_reconciliation(
        self,
        *,
        symbol: str,
        side: Side,
        position_side: str,
        quantity: float,
        closed_at_ms: int,
    ) -> HistoricalCloseEvidenceDiscovery:
        """Find one V3 execution group, then exactly recheck its order state.

        V3 userTrades supplies per-execution commissions but has no safe
        client-order filter. A bounded time window is therefore only candidate
        discovery; its grouped quantity must match the exact order response.
        """
        if side != close_order_side_for_position(position_side):
            raise ValueError("Aster historical close side contradicts position side")
        if not math.isfinite(quantity) or quantity <= 1e-12 or closed_at_ms <= 0:
            raise ValueError("Aster historical close query requires quantity and closed_at_ms")
        if self._private is None:
            return HistoricalCloseEvidenceDiscovery(
                classification="history_discovery_unsupported",
                candidate_count=0,
            )
        venue_symbol = self._transport._venue_symbol(symbol)
        try:
            raw_trades = await self._private._request(
                "GET",
                ASTER_V3_USER_TRADES_PATH,
                params={
                    "symbol": venue_symbol,
                    "startTime": max(0, closed_at_ms - _ASTER_HISTORY_TIME_WINDOW_MS),
                    "endTime": closed_at_ms + _ASTER_HISTORY_TIME_WINDOW_MS,
                    "limit": 1000,
                },
            )
        except Exception as exc:
            self._handle_private_invalid_symbol(
                exc, symbol, endpoint=ASTER_V3_USER_TRADES_PATH
            )
            raise
        rows = _extract_rows(raw_trades)
        if len(rows) >= 1000:
            return HistoricalCloseEvidenceDiscovery(
                classification="history_incomplete",
                candidate_count=0,
            )
        candidates = find_aster_v3_historical_close_order_candidates(
            rows,
            symbol=venue_symbol,
            side=side,
            position_side=position_side,
            quantity=quantity,
            closed_at_ms=closed_at_ms,
        )
        if len(candidates) != 1:
            return HistoricalCloseEvidenceDiscovery(
                classification="ambiguous_candidates" if candidates else "no_candidate",
                candidate_count=len(candidates),
            )
        candidate = candidates[0]
        try:
            raw_order = await self._private._request(
                "GET",
                ASTER_V3_ORDER_PATH,
                params={"symbol": venue_symbol, "orderId": candidate["order_id"]},
            )
        except Exception as exc:
            self._handle_private_invalid_symbol(exc, symbol, endpoint=ASTER_V3_ORDER_PATH)
            raise
        order_rows = _extract_rows(raw_order)
        order = order_rows[0] if order_rows else raw_order if isinstance(raw_order, dict) else {}
        if not isinstance(order, dict):
            return HistoricalCloseEvidenceDiscovery(
                classification="exact_recheck_unavailable",
                candidate_count=1,
            )
        order_qty = _aster_history_float(order.get("executedQty"))
        order_price = _aster_history_float(order.get("avgPrice", order.get("price")))
        order_status = str(order.get("status") or "").upper()
        order_identity_matches = str(order.get("orderId") or order.get("id") or "") == candidate["order_id"]
        order_position_matches = _aster_history_row_closes_position_side(order, position_side)
        if (
            str(order.get("symbol") or "").upper() != venue_symbol.upper()
            or str(order.get("side") or "").upper() != side.value.upper()
            or not order_position_matches
            or not order_identity_matches
            # Historical billing evidence must follow the V3 contract exactly.
            # The broader progress-state normalization is intentionally not
            # evidence that an Aster order is terminal and fully filled.
            or order_status != "FILLED"
            or order_qty is None
            or not math.isclose(order_qty, quantity, rel_tol=1e-9, abs_tol=1e-12)
            or not math.isclose(order_qty, candidate["quantity"], rel_tol=1e-9, abs_tol=1e-12)
            or order_price is None
            or order_price <= 0.0
        ):
            return HistoricalCloseEvidenceDiscovery(
                classification="exact_recheck_identity_mismatch",
                candidate_count=1,
            )
        fee_quote = 0.0
        for trade in candidate["trades"]:
            try:
                fee = float(trade.get("commission"))
            except (TypeError, ValueError, OverflowError):
                fee = math.nan
            fee_asset = str(trade.get("commissionAsset") or "").upper()
            if not math.isfinite(fee) or fee_asset not in _ASTER_QUOTE_FEE_ASSETS:
                return HistoricalCloseEvidenceDiscovery(
                    classification="exact_recheck_incomplete",
                    candidate_count=1,
                )
            fee_quote += abs(fee)
        return HistoricalCloseEvidenceDiscovery(
            classification="unique_candidate_exact_recheck",
            candidate_count=1,
            reconciliation=OrderFillReconciliation(
                venue=Venue.ASTER,
                symbol=venue_symbol,
                side=side,
                quantity=order_qty,
                average_price=order_price,
                order_id=candidate["order_id"],
                client_order_id=(
                    str(order.get("clientOrderId") or "")
                    or candidate["client_order_id"]
                    or None
                ),
                fee_quote=fee_quote,
                filled_at_ms=int(candidate["updated_at_ms"]),
                metadata={
                    "fee_evidence_complete": True,
                    "fee_evidence_source": "aster_v3_user_trades",
                    "historical_candidate_endpoint": ASTER_V3_USER_TRADES_PATH,
                    "historical_candidate_updated_at_ms": candidate["updated_at_ms"],
                    "historical_evidence_provenance": "aster_v3_user_trades_exact_order",
                },
            ),
        )

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        return await self._transport.normalize_quantity(symbol, quantity)

    async def shutdown(self) -> None:
        if self._private is not None:
            await self._private.close()
        await self._transport.close()
