"""Aster adapter.

Public market data remains on Aster FAPI. Private account/order operations use
Aster Pro API V3 and do not share Binance HMAC signing.
"""

from __future__ import annotations

from dataclasses import replace
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
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.venues.aster_v3 import AsterV3Client
from lightfee.venues.specs import aster_spec
from lightfee.venues.transport import (
    LiveCredential,
    TransportError,
    TransportErrorCategory,
    VenueTransport,
)
from lightfee.venues import transport as transport_mod

_ASTER_TRUSTED_SYMBOL_RULE_SOURCES = {"exchangeinfo"}


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

    def _remaining_openable_provider(self):
        private = self._private
        if private is None:
            return None
        provider = getattr(private, "fetch_remaining_openable_notional", None)
        if callable(provider):
            return provider
        return None

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

    async def precheck_order_admission(self, request: OrderRequest) -> dict[str, Any]:
        """Non-mutating Aster admission check used before paired entry submit."""
        if request.reduce_only:
            return {
                "venue": Venue.ASTER.value,
                "symbol": request.symbol,
                "status": "skipped",
                "reason": "reduce_only_exempt",
            }
        if self._private is None:
            if self._mode == "live":
                raise self._private_unavailable()
            return await self._transport.precheck_order_admission(request)

        venue_symbol = self._transport._venue_symbol(request.symbol)
        symbol_rule = None
        rule_source = "unavailable"
        try:
            symbol_rule = await transport_mod.get_symbol_rules_cache().get(
                self._transport,
                Venue.ASTER,
                venue_symbol,
            )
            rule_source = str(getattr(symbol_rule, "rule_source", "") or "unknown")
        except Exception:
            symbol_rule = None
        if rule_source.lower() not in _ASTER_TRUSTED_SYMBOL_RULE_SOURCES:
            payload = {
                "venue": Venue.ASTER.value,
                "symbol": request.symbol,
                "venue_symbol": venue_symbol,
                "endpoint": self._transport._spec.order_path,
                "product_type": self._transport._product_type(),
                "client_order_id": request.client_order_id or "",
                "order_id": request.order_id or "",
                "raw_price": request.price,
                "raw_qty": request.quantity,
                "rule_source": rule_source,
                "response_classification": "precision_rule_unavailable",
                "reason": "aster_trusted_symbol_rule_unavailable",
            }
            self._transport._record_order_diagnostic(
                "order_error.precision_rule_unavailable_before_submit",
                payload,
            )
            raise OrderSubmitError(
                SubmitFailureClass.REJECTED,
                "aster_trusted_symbol_rule_unavailable",
            )

        preflight = self._transport.preflight_order_request(
            request,
            symbol_rule=symbol_rule,
        )
        fallback_price = request.price if request.price is not None else request.price_hint
        headroom_price = preflight["quantized_price"]
        if headroom_price is None:
            headroom_price = fallback_price
        headroom_payload = (
            await self._transport._aster_reject_new_risk_without_headroom(
                request,
                venue_symbol,
                float(preflight["quantized_qty"]),
                headroom_price,
                order_role="maker" if request.post_only else "hedge",
                source="aster_headroom_pre_entry_precheck",
                remaining_openable_provider=self._remaining_openable_provider(),
                account_risk_provider=self._private.fetch_account_risk_snapshot,
                position_provider=self._private.fetch_position,
                open_orders_provider=self._private.fetch_open_orders,
            )
        )
        result = dict(preflight)
        result.update(headroom_payload)
        result["status"] = "ok"
        result["private_api"] = "aster_v3"
        return result

    async def _preflight_private_order_request(
        self,
        request: OrderRequest,
    ) -> OrderRequest:
        """Apply Binance-style Aster symbol filters before private V3 submit."""
        venue_symbol = self._transport._venue_symbol(request.symbol)
        symbol_rule = None
        rule_source = "unavailable"
        try:
            symbol_rule = await transport_mod.get_symbol_rules_cache().get(
                self._transport,
                Venue.ASTER,
                venue_symbol,
            )
            rule_source = str(getattr(symbol_rule, "rule_source", "") or "unknown")
        except Exception:
            symbol_rule = None
        if (
            not request.reduce_only
            and rule_source.lower() not in _ASTER_TRUSTED_SYMBOL_RULE_SOURCES
        ):
            payload = {
                "venue": Venue.ASTER.value,
                "symbol": request.symbol,
                "venue_symbol": venue_symbol,
                "endpoint": self._transport._spec.order_path,
                "product_type": self._transport._product_type(),
                "client_order_id": request.client_order_id or "",
                "order_id": request.order_id or "",
                "raw_price": request.price,
                "raw_qty": request.quantity,
                "rule_source": rule_source,
                "response_classification": "precision_rule_unavailable",
                "reason": "aster_trusted_symbol_rule_unavailable",
            }
            self._transport._record_order_diagnostic(
                "order_error.precision_rule_unavailable_before_submit",
                payload,
            )
            raise OrderSubmitError(
                SubmitFailureClass.REJECTED,
                "aster_trusted_symbol_rule_unavailable",
            )
        try:
            preflight = self._transport.preflight_order_request(
                request,
                symbol_rule=symbol_rule,
            )
        except OrderSubmitError:
            for event in reversed(self._transport.order_diagnostics):
                payload = event.get("payload", {})
                if (
                    isinstance(payload, dict)
                    and payload.get("response_classification") == "precision_rejected"
                ):
                    self._transport._record_order_diagnostic(
                        "order_error.precision_rejected_before_submit",
                        payload,
                    )
                    break
            raise
        quantized_request = replace(
            request,
            quantity=float(preflight["quantized_qty"]),
            price=(
                None
                if preflight["quantized_price"] is None
                else float(preflight["quantized_price"])
            ),
        )
        headroom_price = (
            preflight["quantized_price"]
            if preflight["quantized_price"] is not None
            else request.price
        )
        aster_headroom_payload = (
            await self._transport._aster_reject_new_risk_without_headroom(
                request,
                venue_symbol,
                float(preflight["quantized_qty"]),
                headroom_price,
                order_role="maker" if request.post_only else "hedge",
                source="aster_headroom_precheck",
                remaining_openable_provider=self._remaining_openable_provider(),
                account_risk_provider=self._private.fetch_account_risk_snapshot,
                position_provider=self._private.fetch_position,
                open_orders_provider=self._private.fetch_open_orders,
            )
        )
        attempt_payload = dict(preflight)
        attempt_payload["response_classification"] = "attempt"
        attempt_payload["private_api"] = "aster_v3"
        if aster_headroom_payload:
            attempt_payload.update(aster_headroom_payload)
        self._transport._record_order_diagnostic(
            "order.submit_attempt",
            attempt_payload,
        )
        return quantized_request

    async def place_order(self, request: OrderRequest) -> OrderFill:
        if self._private is not None:
            request = await self._preflight_private_order_request(request)
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
            request = await self._preflight_private_order_request(request)
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
