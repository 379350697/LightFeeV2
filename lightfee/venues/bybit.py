"""Bybit V5 adapter (unified account)."""

from __future__ import annotations

from dataclasses import replace
import math
import time
from typing import TYPE_CHECKING, Any, Iterable, Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import (
    AccountFeeSnapshot,
    HistoricalCloseEvidenceDiscovery,
    OrderFill,
    OrderRequest,
    PositionSnapshot,
    Side,
    Venue,
    VenueMarketSnapshot,
    close_order_side_for_position,
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


def _bybit_history_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0.0 else None


def _bybit_history_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed > 0 else None


def _bybit_reduce_only(value: Any) -> bool:
    return value is True or (isinstance(value, str) and value.lower() == "true")


def find_bybit_historical_close_order_candidates(
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
    """Return every Bybit history row satisfying the close identity contract."""
    expected_side = "Buy" if (side == Side.BUY or str(side).upper() == "BUY") else "Sell"
    expected_position_idx = 1 if str(position_side).upper() == "LONG" else 2
    quantity_tolerance = max(quantity * quantity_relative_tolerance, 1e-12)
    candidates: list[dict[str, Any]] = []
    for raw in orders:
        if not isinstance(raw, dict):
            continue
        order_id = str(raw.get("orderId") or "").strip()
        client_order_id = str(raw.get("orderLinkId") or "").strip()
        executed_quantity = _bybit_history_float(raw.get("cumExecQty"))
        updated_at_ms = _bybit_history_int(raw.get("updatedTime"))
        try:
            position_idx = int(raw.get("positionIdx"))
        except (TypeError, ValueError, OverflowError):
            continue
        position_side_matches = position_idx == expected_position_idx or (
            position_idx == 0 and _bybit_reduce_only(raw.get("reduceOnly"))
        )
        if (
            str(raw.get("symbol") or "").upper() != symbol.upper()
            or str(raw.get("side") or "") != expected_side
            or not position_side_matches
            or not _bybit_reduce_only(raw.get("reduceOnly"))
            or str(raw.get("orderStatus") or "") != "Filled"
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
        )
    )
    return candidates


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

    async def discover_historical_close_fill_reconciliation(
        self,
        *,
        symbol: str,
        side: Side,
        position_side: str,
        quantity: float,
        closed_at_ms: int,
    ) -> HistoricalCloseEvidenceDiscovery:
        """Find one paginated order-history row, then re-read exact executions."""
        if side != close_order_side_for_position(position_side):
            raise ValueError("Bybit historical close side contradicts position side")
        if not math.isfinite(quantity) or quantity <= 1e-12 or closed_at_ms <= 0:
            raise ValueError("Bybit historical close query requires quantity and closed_at_ms")
        venue_symbol = self._transport._venue_symbol(symbol)
        time_window_ms = 300_000
        rows: list[dict[str, Any]] = []
        cursor = ""
        seen_cursors: set[str] = set()
        page_count = 0
        while True:
            params: dict[str, Any] = {
                "category": "linear",
                "symbol": venue_symbol,
                "startTime": max(0, closed_at_ms - time_window_ms),
                "endTime": closed_at_ms + time_window_ms,
                "limit": 50,
            }
            if cursor:
                params["cursor"] = cursor
            raw = await self._transport._request(
                "GET",
                "/v5/order/history",
                params=params,
                private=True,
            )
            page_count += 1
            if (
                not isinstance(raw, dict)
                or str(raw.get("retCode", 0)) != "0"
                or not isinstance(raw.get("result"), dict)
                or not isinstance(raw["result"].get("list"), list)
            ):
                raise ValueError("Bybit order history response is malformed")
            rows.extend(
                row for row in raw["result"]["list"] if isinstance(row, dict)
            )
            next_cursor = str(raw["result"].get("nextPageCursor") or "")
            if not next_cursor:
                break
            if page_count >= 20:
                return HistoricalCloseEvidenceDiscovery(
                    classification="history_incomplete",
                    candidate_count=0,
                )
            if next_cursor == cursor or next_cursor in seen_cursors:
                raise ValueError("Bybit order history cursor repeated")
            seen_cursors.add(next_cursor)
            cursor = next_cursor

        candidates = find_bybit_historical_close_order_candidates(
            rows,
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
        execution_types = {
            str(value) for value in metadata.get("execution_types", []) if str(value)
        }
        if "BustTrade" in execution_types:
            provenance = "exchange_takeover_execution"
        elif candidate["system_client_order_id"]:
            provenance = "system_client_id_execution"
        else:
            provenance = "exchange_execution_unattributed"
        metadata.update(
            {
                "historical_candidate_endpoint": "/v5/order/history",
                "historical_candidate_updated_at_ms": candidate["updated_at_ms"],
                "historical_evidence_provenance": provenance,
            }
        )
        return HistoricalCloseEvidenceDiscovery(
            classification="unique_candidate_exact_recheck",
            candidate_count=1,
            reconciliation=replace(reconciliation, metadata=metadata),
        )

    async def shutdown(self) -> None:
        await self._transport.close()
