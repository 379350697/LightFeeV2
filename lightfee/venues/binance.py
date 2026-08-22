"""Binance USDM futures adapter (BinanceUsdmRest / BinanceUsdmPrivateV3)."""

from __future__ import annotations

import asyncio
from dataclasses import replace
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
    PositionSnapshot,
    Side,
    Venue,
    VenueMarketSnapshot,
    close_order_side_for_position,
)
from lightfee.venues.account_fees import fee_rate_from_mapping
from lightfee.venues.entry_tradability import (
    entry_tradability_blocked,
    entry_tradability_unavailable,
)
from lightfee.venues.specs import binance_spec
from lightfee.venues.transport import LiveCredential, VenueTransport


def _binance_history_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0.0 else None


def _binance_history_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed > 0 else None


def _binance_reduce_only(value: Any) -> bool:
    return value is True or (isinstance(value, str) and value.lower() == "true")


def _binance_history_row_closes_position_side(
    raw: dict[str, Any],
    expected_position_side: str,
) -> bool:
    """Apply Binance's Hedge Mode and One-way Mode close semantics.

    In Hedge Mode the opposite order side plus ``positionSide`` owns the close;
    Binance does not require (and its new-order contract forbids sending)
    ``reduceOnly`` there.  In One-way Mode ``BOTH`` is only safe history
    evidence when the exchange row explicitly marks the order reduce/close-only.
    """
    observed_position_side = str(raw.get("positionSide") or "").upper()
    if observed_position_side == expected_position_side:
        return True
    return observed_position_side == "BOTH" and (
        _binance_reduce_only(raw.get("reduceOnly"))
        or _binance_reduce_only(raw.get("closePosition"))
    )


def find_binance_historical_close_order_candidates(
    orders: Iterable[dict[str, Any]],
    *,
    symbol: str,
    side: Side | str,
    position_side: str,
    quantity: float,
    closed_at_ms: int,
    time_window_ms: int = 300_000,
    quantity_relative_tolerance: float = 1e-9,
) -> list[dict[str, Any]]:
    """Return every Binance order row satisfying the close identity contract."""
    expected_side = (
        side.value.upper() if isinstance(side, Side) else str(side).upper()
    )
    expected_position_side = str(position_side).upper()
    quantity_tolerance = max(quantity * quantity_relative_tolerance, 1e-12)
    candidates: list[dict[str, Any]] = []
    for raw in orders:
        if not isinstance(raw, dict):
            continue
        order_id = str(raw.get("orderId") or "").strip()
        client_order_id = str(raw.get("clientOrderId") or "").strip()
        executed_quantity = _binance_history_float(raw.get("executedQty"))
        updated_at_ms = _binance_history_int(raw.get("updateTime"))
        if (
            str(raw.get("symbol") or "").upper() != symbol.upper()
            or str(raw.get("side") or "").upper() != expected_side
            or not _binance_history_row_closes_position_side(
                raw,
                expected_position_side,
            )
            or str(raw.get("status") or "").upper() != "FILLED"
            or executed_quantity is None
            or executed_quantity <= 1e-12
            or updated_at_ms is None
            or not order_id
        ):
            continue
        quantity_delta = abs(executed_quantity - quantity)
        time_delta_ms = updated_at_ms - closed_at_ms
        if quantity_delta > quantity_tolerance or abs(time_delta_ms) > time_window_ms:
            continue
        candidates.append(
            {
                "order_id": order_id,
                "client_order_id": client_order_id,
                "system_client_order_id": client_order_id.startswith("lf"),
                "executed_quantity": executed_quantity,
                "average_price": _binance_history_float(raw.get("avgPrice")),
                "updated_at_ms": updated_at_ms,
                "time_delta_ms": time_delta_ms,
                "quantity_delta": quantity_delta,
            }
        )
    candidates.sort(
        key=lambda candidate: (
            abs(int(candidate["time_delta_ms"])),
            float(candidate["quantity_delta"]),
            str(candidate["order_id"]),
            str(candidate["client_order_id"]),
        )
    )
    return candidates


class BinanceAdapter(VenueAdapter):
    """Binance USDⓈ-M futures adapter."""

    # An entry admission decision must use a server response obtained for that
    # decision.  Reusing even a one-second catalog would reintroduce the
    # PRE_SETTLE transition window this guard exists to close.
    _ENTRY_TRADABILITY_CATALOG_TTL_MS = 0

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

    async def fetch_account_fee_snapshot(
        self, reference_symbol: str = ""
    ) -> Optional[AccountFeeSnapshot]:
        venue_symbol = self._transport._venue_symbol(reference_symbol) if reference_symbol else ""
        if not venue_symbol:
            return None
        raw = await self._transport._request(
            "GET",
            "/fapi/v1/commissionRate",
            params={"symbol": venue_symbol},
            private=True,
        )
        if not isinstance(raw, dict):
            raise ValueError("Binance commission-rate response is malformed")
        return AccountFeeSnapshot(
            venue=self.venue,
            maker_fee_bps=fee_rate_from_mapping(raw, "maker fee", "makerCommissionRate"),
            taker_fee_bps=fee_rate_from_mapping(raw, "taker fee", "takerCommissionRate"),
            observed_at_ms=int(time.time() * 1000),
            source="binance_fapi_commission_rate",
        )

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
        catalog; it does not document a symbol filter.  This execution-time
        view is refreshed for every admission decision, while remaining
        independent from the long-lived discovery catalog.
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

    async def discover_historical_close_fill_reconciliation(
        self,
        *,
        symbol: str,
        side: Side,
        position_side: str,
        quantity: float,
        closed_at_ms: int,
    ) -> HistoricalCloseEvidenceDiscovery:
        """Find a unique allOrders row, then re-read exact order trades/fees."""
        if side != close_order_side_for_position(position_side):
            raise ValueError("Binance historical close side contradicts position side")
        if not math.isfinite(quantity) or quantity <= 1e-12 or closed_at_ms <= 0:
            raise ValueError("Binance historical close query requires quantity and closed_at_ms")
        venue_symbol = self._transport._venue_symbol(symbol)
        time_window_ms = 300_000
        raw = await self._transport._request(
            "GET",
            "/fapi/v1/allOrders",
            params={
                "symbol": venue_symbol,
                "startTime": max(0, closed_at_ms - time_window_ms),
                "endTime": closed_at_ms + time_window_ms,
                "limit": 1000,
            },
            private=True,
        )
        if not isinstance(raw, list):
            raise ValueError("Binance allOrders response is malformed")
        # Binance does not return a continuation cursor for this request shape.
        # A full page cannot prove that the bounded interval was exhaustive.
        if len(raw) >= 1000:
            return HistoricalCloseEvidenceDiscovery(
                classification="history_incomplete",
                candidate_count=0,
            )
        candidates = find_binance_historical_close_order_candidates(
            raw,
            symbol=venue_symbol,
            side=side,
            position_side=position_side,
            quantity=quantity,
            closed_at_ms=closed_at_ms,
            time_window_ms=time_window_ms,
        )
        if len(candidates) != 1:
            return HistoricalCloseEvidenceDiscovery(
                classification=(
                    "ambiguous_candidates" if candidates else "no_candidate"
                ),
                candidate_count=len(candidates),
            )

        candidate = candidates[0]
        reconciliation = await self.fetch_order_fill_reconciliation(
            symbol,
            str(candidate["order_id"]),
            str(candidate["client_order_id"]),
        )
        if reconciliation is None:
            return HistoricalCloseEvidenceDiscovery(
                classification="exact_recheck_unavailable",
                candidate_count=1,
            )
        if (
            reconciliation.symbol.upper() != venue_symbol.upper()
            or reconciliation.order_id != str(candidate["order_id"])
        ):
            return HistoricalCloseEvidenceDiscovery(
                classification="exact_recheck_identity_mismatch",
                candidate_count=1,
            )
        metadata = dict(reconciliation.metadata or {})
        fee_quote = reconciliation.fee_quote
        if (
            metadata.get("fee_evidence_complete") is not True
            or fee_quote is None
            or not math.isfinite(fee_quote)
            or fee_quote < 0.0
        ):
            return HistoricalCloseEvidenceDiscovery(
                classification="exact_recheck_incomplete",
                candidate_count=1,
            )
        metadata.update(
            {
                "historical_candidate_endpoint": "/fapi/v1/allOrders",
                "historical_candidate_updated_at_ms": candidate["updated_at_ms"],
                "historical_evidence_provenance": (
                    "system_client_id_execution"
                    if candidate["system_client_order_id"]
                    else "exchange_execution_unattributed"
                ),
            }
        )
        return HistoricalCloseEvidenceDiscovery(
            classification="unique_candidate_exact_recheck",
            candidate_count=1,
            reconciliation=replace(reconciliation, metadata=metadata),
        )

    async def shutdown(self) -> None:
        await self._transport.close()
