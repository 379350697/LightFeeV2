"""Hyperliquid adapter (HyperliquidInfoApi / HyperliquidExchangeApi)."""

from __future__ import annotations

import math
import time
from typing import Any, Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.core.domain import (
    AccountBalanceSnapshot,
    AccountFeeSnapshot,
    OrderFill,
    OrderFillReconciliation,
    OrderRequest,
    PositionSnapshot,
    Side,
    Venue,
    VenueMarketSnapshot,
)
from lightfee.venues.account_fees import fee_rate_from_mapping
from lightfee.venues.entry_tradability import (
    entry_tradability_blocked,
    entry_tradability_unavailable,
)
from lightfee.venues.specs import hyperliquid_spec
from lightfee.venues.transport import (
    LiveCredential,
    TransportError,
    TransportErrorCategory,
    VenueTransport,
)


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
        self._credential = self._transport._credential

    @property
    def venue(self) -> Venue:
        return Venue.HYPERLIQUID

    @property
    def supports_private_health(self) -> bool:
        return self._transport.mode == "live"

    @property
    def supports_entry_leverage_preparation(self) -> bool:
        return True

    async def fetch_account_fee_snapshot(
        self, reference_symbol: str = ""
    ) -> Optional[AccountFeeSnapshot]:
        del reference_symbol
        account_address = self._credential.account_address if self._credential else ""
        if not account_address:
            return None
        raw = await self._transport._request(
            "POST",
            "/info",
            body={"type": "userFees", "user": account_address},
            private=False,
        )
        if not isinstance(raw, dict):
            raise ValueError("Hyperliquid user-fees response is malformed")
        return AccountFeeSnapshot(
            venue=self.venue,
            maker_fee_bps=fee_rate_from_mapping(raw, "maker fee", "userAddRate"),
            taker_fee_bps=fee_rate_from_mapping(raw, "taker fee", "userCrossRate"),
            observed_at_ms=int(time.time() * 1000),
            source="hyperliquid_user_fees",
        )

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

    async def precheck_entry_tradability(self, symbol: str) -> dict[str, Any]:
        """Require the current Hyperliquid meta universe to retain the asset."""
        venue_symbol = self._transport._venue_symbol(symbol)
        raw = await self._transport._request(
            "POST",
            "/info",
            body={"type": "meta"},
            private=False,
        )
        if not isinstance(raw, dict) or not isinstance(raw.get("universe"), list):
            raise entry_tradability_unavailable(
                Venue.HYPERLIQUID.value,
                venue_symbol,
                "meta_universe_missing_or_malformed",
            )
        row = next(
            (
                item
                for item in raw["universe"]
                if isinstance(item, dict)
                and str(item.get("name", "")).upper() == venue_symbol.upper()
            ),
            None,
        )
        if row is None or bool(row.get("isDelisted", False)):
            raise entry_tradability_blocked(
                Venue.HYPERLIQUID.value,
                venue_symbol,
                state="DELISTED" if row is not None else "MISSING",
            )
        return {
            "venue": Venue.HYPERLIQUID.value,
            "symbol": venue_symbol,
            "status": "ok",
            "is_delisted": False,
        }

    async def fetch_market_snapshot(self, symbols: list[str]) -> VenueMarketSnapshot:
        return await self._transport.fetch_market_snapshot(symbols)

    async def place_order(self, request: OrderRequest) -> OrderFill:
        return await self._transport.place_order(request)

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        return await self._transport.fetch_position(symbol)

    async def fetch_account_balance_snapshot(self) -> Optional[AccountBalanceSnapshot]:
        return await self._transport.fetch_account_balance_snapshot()

    async def fetch_open_orders(self, symbol: str) -> list[dict[str, Any]]:
        """Fetch Hyperliquid open orders for the configured account.

        The raw /info openOrders response must be a recognized open-order
        collection.  A missing account, an unrecognized shape, or a non-list
        payload is NOT equivalent to "no open orders": it raises so the caller
        keeps the close pending instead of treating unknown truth as proven flat.
        """
        cred = self._credential
        if cred is None or not cred.account_address:
            raise TransportError(
                TransportErrorCategory.AUTH_FAILURE,
                "hyperliquid open orders require account_address",
            )

        raw = await self._transport._request(
            "POST",
            "/info",
            body={"type": "openOrders", "user": cred.account_address},
            private=False,
        )
        rows: Any = raw
        if isinstance(raw, dict):
            for key in ("openOrders", "orders", "data"):
                if key in raw:
                    rows = raw[key]
                    break
        if not isinstance(rows, list):
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                "hyperliquid openOrders response not a list",
                status_code=0,
                body=str(raw)[:500] if raw is not None else "",
            )

        venue_symbol = self._transport._venue_symbol(symbol).upper()
        canonical_symbol = symbol.upper()
        matched: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            coin = str(row.get("coin", "") or "").upper()
            if not coin:
                continue
            if coin == venue_symbol or f"{coin}USDT" == canonical_symbol:
                matched.append(dict(row))
        return matched

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        return await self._transport.normalize_quantity(symbol, quantity)

    def _hyperliquid_entry_max_leverage(self, venue_symbol: str) -> int | None:
        metadata = (getattr(self._transport, "_symbol_metadata", {}) or {}).get(
            venue_symbol
        )
        if not isinstance(metadata, dict):
            return None
        try:
            value = float(metadata.get("maxLeverage"))
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(value) or value <= 0 or not value.is_integer():
            return None
        return int(value)

    async def ensure_entry_leverage(
        self,
        symbol: str,
        leverage: int,
        *,
        notional_quote: float | None = None,
    ) -> None:
        """Submit Hyperliquid's signed updateLeverage action before entry.

        Hyperliquid exposes the applied configuration through the accepted
        signed action; an empty account need not expose a position row to read
        back, so requiring ``userState`` here would incorrectly block first
        entries.  The action acknowledgement is therefore the authoritative
        postcondition for this venue.
        """
        target = int(leverage or 0)
        if target <= 0 or self._transport.mode != "live":
            return

        from lightfee.venues.hyperliquid_signing import build_hyperliquid_exchange_payload

        venue_symbol = self._transport._venue_symbol(symbol)
        payload: dict[str, Any] = {
            "venue": Venue.HYPERLIQUID.value,
            "symbol": venue_symbol,
            "requested_leverage": target,
            "requested_notional_quote": float(notional_quote or 0.0),
            "set_leverage_endpoint": "POST /exchange updateLeverage",
        }
        try:
            credential = self._credential
            if credential is None or not credential.wallet_private_key:
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    "Hyperliquid entry leverage requires wallet_private_key",
                )
            asset_meta = await self._transport._hl_resolve_asset_meta(venue_symbol)
            asset_index = int(asset_meta["asset_index"])
            max_leverage = self._hyperliquid_entry_max_leverage(venue_symbol)
            effective = min(target, max_leverage) if max_leverage else target
            effective = max(int(effective), 1)
            payload.update(
                {
                    "asset_index": asset_index,
                    "catalog_max_leverage": max_leverage,
                    "effective_leverage": effective,
                }
            )
            body = build_hyperliquid_exchange_payload(
                action={
                    "type": "updateLeverage",
                    "asset": asset_index,
                    "isCross": True,
                    "leverage": effective,
                },
                private_key_hex=credential.wallet_private_key,
                vault_address=None,
                is_mainnet=True,
            )
            response = await self._transport._request(
                "POST", "/exchange", body=body, private=True
            )
            response_status = str(response.get("status", "") if isinstance(response, dict) else "")
            payload["response_status"] = response_status
            if response_status != "ok":
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    "Hyperliquid entry leverage update rejected "
                    f"symbol={venue_symbol} status={response_status or 'missing'}",
                )
            payload["outcome"] = "set_and_acknowledged"
            self._transport._record_order_diagnostic("order.entry_leverage_ready", payload)
        except OrderSubmitError:
            payload["outcome"] = "rejected"
            self._transport._record_order_diagnostic("order.entry_leverage_unavailable", payload)
            raise
        except Exception as exc:
            payload["outcome"] = "error"
            payload["error"] = str(exc)[:300]
            self._transport._record_order_diagnostic("order.entry_leverage_unavailable", payload)
            raise OrderSubmitError(
                SubmitFailureClass.REJECTED,
                f"Hyperliquid entry leverage prepare failed: {exc}",
            ) from exc

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
                result = self._parse_hl_order_status(
                    raw,
                    symbol,
                    now_ms,
                    configured_account_address=user_addr,
                )
                if result is not None:
                    return result
            except (ValueError, Exception):
                pass

        # --- Attempt 2: orderStatus by cloid ---
        if client_order_id:
            try:
                from lightfee.venues.hyperliquid_signing import (
                    hyperliquid_cloid_for_client_order,
                )
                wire_cloid = hyperliquid_cloid_for_client_order(client_order_id)
                raw = await transport._request(
                    "POST", "/info",
                    body={"type": "orderStatus", "user": user_addr, "cloid": wire_cloid},
                    private=False,
                )
                result = self._parse_hl_order_status(
                    raw,
                    symbol,
                    now_ms,
                    configured_account_address=user_addr,
                )
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
                raw,
                symbol,
                order_id,
                client_order_id,
                now_ms,
                configured_account_address=user_addr,
            )
        except Exception:
            pass

        return None

    @staticmethod
    def _parse_hl_order_status(
        raw: dict[str, Any],
        symbol: str,
        now_ms: int,
        *,
        configured_account_address: str = "",
    ) -> Optional[OrderFillReconciliation]:
        """Parse Hyperliquid orderStatus response.

        Response shape:
          {"status": "order", "order": {"order": {oid, cloid, coin, side, sz, ...}, "status": "filled"}}
        Status values: "filled", "open", "canceled", "rejected"
        """
        order_wrapper = raw.get("order", raw)
        if not isinstance(order_wrapper, dict):
            return None

        status = str(order_wrapper.get("status", raw.get("status", ""))).lower()
        order = order_wrapper.get("order", order_wrapper)
        if not isinstance(order, dict):
            return None

        if not status or status == "order":
            status = str(order.get("status", status)).lower()
        oid = str(order.get("oid", ""))
        cloid = str(order.get("cloid", ""))
        side_raw = str(order.get("side", "")).upper()
        side = Side.BUY if side_raw == "B" else Side.SELL
        orig_sz = float(order.get("origSz", order.get("totalSz", 0)))
        remaining_sz = float(order.get("sz", 0))
        total_sz = float(
            order.get("totalSz", max(orig_sz - remaining_sz, 0.0))
        )
        if status == "filled" and total_sz <= 0 < orig_sz:
            total_sz = orig_sz
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
                    "configured_account_address": configured_account_address,
                    "oid": oid,
                    "cloid": cloid,
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
                    "configured_account_address": configured_account_address,
                    "oid": oid,
                    "cloid": cloid,
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
                "configured_account_address": configured_account_address,
                "oid": oid,
                "cloid": cloid,
            },
        )

    @staticmethod
    def _parse_hl_historical_orders(
        raw: Any, symbol: str, order_id: str,
        client_order_id: Optional[str], now_ms: int,
        *,
        configured_account_address: str = "",
    ) -> Optional[OrderFillReconciliation]:
        """Parse Hyperliquid historicalOrders response for a matching order.

        Response is a list of order dicts (same shape as orderStatus.order).
        """
        orders = raw if isinstance(raw, list) else []
        if isinstance(raw, dict):
            orders = raw.get("historicalOrders", raw.get("orders", []))

        client_order_ids: set[str] = set()
        if client_order_id:
            client_order_ids.add(client_order_id)
            from lightfee.venues.hyperliquid_signing import (
                hyperliquid_cloid_for_client_order,
            )
            client_order_ids.add(hyperliquid_cloid_for_client_order(client_order_id))

        for entry in orders:
            if not isinstance(entry, dict):
                continue
            entry_oid = str(entry.get("oid", ""))
            entry_cloid = str(entry.get("cloid", ""))
            if order_id and entry_oid != order_id:
                continue
            if client_order_ids and entry_cloid not in client_order_ids:
                continue

            status = str(entry.get("status", "")).lower()
            side_raw = str(entry.get("side", "")).upper()
            side = Side.BUY if side_raw == "B" else Side.SELL
            orig_sz = float(entry.get("origSz", entry.get("totalSz", 0)))
            remaining_sz = float(entry.get("sz", 0))
            total_sz = float(
                entry.get("totalSz", max(orig_sz - remaining_sz, 0.0))
            )
            if status == "filled" and total_sz <= 0 < orig_sz:
                total_sz = orig_sz
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
                        "configured_account_address": configured_account_address,
                        "oid": entry_oid,
                        "cloid": entry_cloid,
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
                        "configured_account_address": configured_account_address,
                        "oid": entry_oid,
                        "cloid": entry_cloid,
                    },
                )

        return None
