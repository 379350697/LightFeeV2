"""Bybit V5 adapter (unified account)."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import (
    AccountFeeSnapshot,
    OrderFill,
    OrderRequest,
    PositionSnapshot,
    Venue,
    VenueMarketSnapshot,
)
from lightfee.venues.account_fees import fee_rate_from_mapping, first_mapping
from lightfee.venues.entry_tradability import (
    entry_tradability_blocked,
    entry_tradability_unavailable,
)
from lightfee.venues.specs import bybit_spec
from lightfee.venues.transport import LiveCredential, VenueTransport

if TYPE_CHECKING:
    from lightfee.core.domain import OrderFillReconciliation


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

    @property
    def supports_private_health(self) -> bool:
        return self._transport.mode == "live"

    async def fetch_account_fee_snapshot(
        self, reference_symbol: str = ""
    ) -> Optional[AccountFeeSnapshot]:
        venue_symbol = self._transport._venue_symbol(reference_symbol) if reference_symbol else ""
        if not venue_symbol:
            return None
        raw = await self._transport._request(
            "GET",
            "/v5/account/fee-rate",
            params={"category": "linear", "symbol": venue_symbol},
            private=True,
        )
        if not isinstance(raw, dict) or int(raw.get("retCode", 0)) != 0:
            raise ValueError("Bybit fee-rate request failed")
        result = raw.get("result")
        row = first_mapping(result.get("list") if isinstance(result, dict) else None, "Bybit fee-rate row")
        return AccountFeeSnapshot(
            venue=self.venue,
            maker_fee_bps=fee_rate_from_mapping(row, "maker fee", "makerFeeRate"),
            taker_fee_bps=fee_rate_from_mapping(row, "taker fee", "takerFeeRate"),
            observed_at_ms=int(time.time() * 1000),
            source=f"bybit_fee_rate:{venue_symbol}",
        )

    def supported_symbols(self) -> list[str]:
        """Return loaded Bybit linear USDT perpetual symbols, if available."""
        metadata = getattr(self._transport, "_symbol_metadata", {}) or {}
        return sorted(str(symbol) for symbol in metadata.keys())

    async def ensure_supported_symbols_loaded(self) -> None:
        """Populate Bybit's paginated linear contract catalog for recovery probes."""
        if self._transport._symbol_metadata:
            return
        metadata: dict[str, dict[str, Any]] = {}
        cursor = ""
        while True:
            params: dict[str, Any] = {"category": "linear", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            raw = await self._transport._request(
                "GET",
                "/v5/market/instruments-info",
                params=params,
                private=False,
            )
            result = raw.get("result", {}) if isinstance(raw, dict) else {}
            rows = result.get("list", []) if isinstance(result, dict) else []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                symbol = str(row.get("symbol", "")).upper()
                if not symbol.endswith("USDT"):
                    continue
                status = str(row.get("status", "Trading")).upper()
                if status != "TRADING":
                    continue
                contract_type = str(row.get("contractType", "LinearPerpetual")).upper()
                if "PERPETUAL" not in contract_type:
                    continue
                metadata[symbol] = dict(row)
            next_cursor = str(result.get("nextPageCursor", "") or "")
            if not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor
        self._transport.set_symbol_metadata(metadata)

    async def precheck_entry_tradability(self, symbol: str) -> dict[str, Any]:
        """Fail closed if Bybit no longer accepts a new position for ``symbol``.

        Contract status and delivery state are execution-time facts, so this
        deliberately bypasses the long-lived discovery catalog cache.
        """
        venue_symbol = self._transport._venue_symbol(symbol)
        raw = await self._transport._request(
            "GET",
            "/v5/market/instruments-info",
            params={"category": "linear", "symbol": venue_symbol},
            private=False,
        )
        if (
            not isinstance(raw, dict)
            or str(raw.get("retCode", 0)) != "0"
            or not isinstance(raw.get("result"), dict)
        ):
            raise entry_tradability_unavailable(
                Venue.BYBIT.value,
                venue_symbol,
                "instruments_info_response_missing_or_unsuccessful",
            )
        result = raw["result"]
        rows = result.get("list")
        if not isinstance(rows, list):
            raise entry_tradability_unavailable(
                Venue.BYBIT.value,
                venue_symbol,
                "instruments_info_list_missing_or_malformed",
            )
        row = next(
            (
                item
                for item in rows
                if isinstance(item, dict)
                and str(item.get("symbol", "")).upper() == venue_symbol.upper()
            ),
            None,
        )
        if row is None:
            raise entry_tradability_blocked(
                Venue.BYBIT.value,
                venue_symbol,
                status="MISSING",
                contract_type="MISSING",
            )

        status = str(row.get("status", "")).upper()
        contract_type = str(row.get("contractType", "")).upper()
        delivery_time = str(row.get("deliveryTime", "") or "")
        if status != "TRADING" or "PERPETUAL" not in contract_type:
            raise entry_tradability_blocked(
                Venue.BYBIT.value,
                venue_symbol,
                status=status or "MISSING",
                contract_type=contract_type or "MISSING",
                delivery_time=delivery_time or "MISSING",
            )
        return {
            "venue": Venue.BYBIT.value,
            "symbol": venue_symbol,
            "status": "ok",
            "contract_status": status,
            "contract_type": contract_type,
            "delivery_time": delivery_time,
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
