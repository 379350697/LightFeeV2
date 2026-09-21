"""Gate Futures V4 adapter (dual position mode account)."""

from __future__ import annotations

import math
from dataclasses import replace
import time
from typing import Any, Iterable, Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
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
from lightfee.venues.account_fees import fee_rate_from_mapping, first_mapping
from lightfee.venues.entry_tradability import (
    entry_tradability_blocked,
    entry_tradability_unavailable,
)
from lightfee.venues.specs import gate_spec
from lightfee.venues.transport import (
    LiveCredential,
    TransportError,
    TransportErrorCategory,
    VenueTransport,
)


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

    @property
    def supports_entry_leverage_preparation(self) -> bool:
        return True

    async def fetch_account_fee_snapshot(
        self, reference_symbol: str = ""
    ) -> Optional[AccountFeeSnapshot]:
        del reference_symbol
        raw = await self._transport._request(
            "GET",
            "/api/v4/wallet/fee",
            params={"settle": "usdt"},
            private=True,
        )
        row = raw.get("data", raw) if isinstance(raw, dict) else raw
        return self._account_fee_snapshot_from_row(row, "gate_wallet_fee")

    def _account_fee_snapshot_from_row(
        self, row: Any, source: str
    ) -> AccountFeeSnapshot:
        values = first_mapping(row, "Gate fee response")
        return AccountFeeSnapshot(
            venue=self.venue,
            maker_fee_bps=fee_rate_from_mapping(
                values,
                "maker fee",
                "futures_maker_fee",
                "futuresMakerFee",
                "maker_fee_rate",
                "maker_fee",
                "makerFeeRate",
                "makerFee",
            ),
            taker_fee_bps=fee_rate_from_mapping(
                values,
                "taker fee",
                "futures_taker_fee",
                "futuresTakerFee",
                "taker_fee_rate",
                "taker_fee",
                "takerFeeRate",
                "takerFee",
            ),
            observed_at_ms=int(time.time() * 1000),
            source=source,
        )

    def l2_book_quantity_to_base_scale(self, symbol: str) -> float | None:
        """Convert Gate local-book contract counts with ``quanto_multiplier``."""
        metadata_by_symbol = getattr(self._transport, "_symbol_metadata", {}) or {}
        for key in (self._transport._venue_symbol(symbol), symbol):
            metadata = metadata_by_symbol.get(key)
            if not isinstance(metadata, dict):
                continue
            try:
                value = float(
                    metadata.get(
                        "quanto_multiplier",
                        metadata.get("quantoMultiplier", 0.0),
                    )
                    or 0.0
                )
            except (TypeError, ValueError):
                return None
            return value if value > 0.0 and math.isfinite(value) else None
        return None

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

    async def precheck_entry_tradability(self, symbol: str) -> dict[str, Any]:
        """Require Gate to report the exact USDT futures contract as trading."""
        venue_symbol = self._transport._venue_symbol(symbol)
        raw = await self._transport._request(
            "GET",
            f"/api/v4/futures/usdt/contracts/{venue_symbol}",
            private=False,
        )
        row = raw.get("data", raw) if isinstance(raw, dict) else None
        if not isinstance(row, dict):
            raise entry_tradability_unavailable(
                Venue.GATE.value,
                venue_symbol,
                "contract_response_missing_or_malformed",
            )
        returned_symbol = str(
            row.get("name") or row.get("contract") or row.get("symbol") or ""
        ).upper()
        if returned_symbol != venue_symbol.upper():
            raise entry_tradability_unavailable(
                Venue.GATE.value,
                venue_symbol,
                "contract_response_symbol_mismatch",
            )
        status = str(row.get("status", "")).lower()
        in_delisting = bool(row.get("in_delisting", False))
        if status != "trading" or in_delisting:
            raise entry_tradability_blocked(
                Venue.GATE.value,
                venue_symbol,
                status=status or "MISSING",
                in_delisting=in_delisting,
            )
        return {
            "venue": Venue.GATE.value,
            "symbol": venue_symbol,
            "status": "ok",
            "contract_status": status,
            "in_delisting": in_delisting,
        }

    async def fetch_market_snapshot(self, symbols: list[str]) -> VenueMarketSnapshot:
        return await self._transport.fetch_market_snapshot(symbols)

    async def place_order(self, request: OrderRequest) -> OrderFill:
        return await self._transport.place_order(request)

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        return await self._transport.fetch_position(symbol)

    async def fetch_account_risk_snapshot(self):
        return await self._transport.fetch_account_risk_snapshot()

    async def fetch_order_fill_reconciliation(
        self,
        symbol: str,
        order_id: str,
        client_order_id: Optional[str] = None,
    ) -> Optional[OrderFillReconciliation]:
        # Gate keeps no client order id on futures orders: identity is the
        # exchange order id, and the transport branch enriches the order row
        # with the order's own my_trades commission.
        return await self._transport.fetch_order_status(
            symbol, order_id=order_id, client_order_id=client_order_id or ""
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
        """Find a unique execution-window group, then re-read the exact order.

        Gate ``my_trades`` windows filter by execution time (seconds), so
        candidate groups come from executions; the order row then decides
        close-only ownership via ``is_reduce_only`` and confirms the full
        executed quantity before the fee-complete exact reconciliation.
        """
        if side != close_order_side_for_position(position_side):
            raise ValueError("Gate historical close side contradicts position side")
        if not math.isfinite(quantity) or quantity <= 1e-12 or closed_at_ms <= 0:
            raise ValueError("Gate historical close query requires quantity and closed_at_ms")
        venue_symbol = self._transport._venue_symbol(symbol)
        time_window_ms = 300_000
        window_sec = time_window_ms // 1000
        from_sec = max(0, int(closed_at_ms / 1000) - window_sec)
        to_sec = int(closed_at_ms / 1000) + window_sec
        raw_trades = await self._transport._request(
            "GET",
            "/api/v4/futures/usdt/my_trades",
            params={
                "contract": venue_symbol,
                "from": str(from_sec),
                "to": str(to_sec),
                "limit": "1000",
            },
            private=True,
        )
        if not isinstance(raw_trades, list):
            raise ValueError("Gate my_trades response is malformed")
        # Gate returns no continuation cursor for this request shape.  A full
        # page cannot prove that the bounded interval was exhaustive.
        if len(raw_trades) >= 1000:
            return HistoricalCloseEvidenceDiscovery(
                classification="history_incomplete",
                candidate_count=0,
            )
        candidates = find_gate_historical_close_execution_candidates(
            raw_trades,
            contract=venue_symbol,
            side=side,
            quantity=quantity,
            closed_at_ms=closed_at_ms,
            time_window_ms=time_window_ms,
        )
        if len(candidates) != 1:
            return HistoricalCloseEvidenceDiscovery(
                classification=(
                    "ambiguous_candidates"
                    if candidates
                    else "gate_my_trades_no_candidate"
                ),
                candidate_count=len(candidates),
            )

        candidate = candidates[0]
        raw_order = await self._transport._request(
            "GET",
            f"/api/v4/futures/usdt/orders/{candidate['order_id']}",
            private=True,
        )
        order_size = (
            _gate_history_float(raw_order.get("size"))
            if isinstance(raw_order, dict)
            else None
        )
        executed_quantity = (
            abs(_gate_history_float(raw_order.get("size", "0")) or 0.0)
            - abs(_gate_history_float(raw_order.get("left", "0")) or 0.0)
            if isinstance(raw_order, dict)
            else None
        )
        order_side = (
            Side.BUY
            if order_size is not None and order_size > 0
            else Side.SELL
            if order_size is not None
            else None
        )
        if (
            not isinstance(raw_order, dict)
            or str(raw_order.get("contract") or "").upper() != venue_symbol.upper()
            or order_side != side
            or raw_order.get("is_reduce_only") is not True
            or str(raw_order.get("finish_as") or "") != "filled"
            or executed_quantity is None
            or not math.isclose(
                executed_quantity,
                quantity,
                rel_tol=1e-9,
                abs_tol=1e-12,
            )
            or not math.isclose(
                executed_quantity,
                candidate["quantity"],
                rel_tol=1e-9,
                abs_tol=1e-12,
            )
            or str(raw_order.get("id") or "") != candidate["order_id"]
        ):
            return HistoricalCloseEvidenceDiscovery(
                classification="exact_recheck_identity_mismatch",
                candidate_count=1,
            )
        reconciliation = await self.fetch_order_fill_reconciliation(
            symbol,
            str(candidate["order_id"]),
            "",
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
                "historical_candidate_endpoint": "/api/v4/futures/usdt/my_trades",
                "historical_candidate_updated_at_ms": candidate["updated_at_ms"],
                "historical_evidence_provenance": "exchange_execution_unattributed",
            }
        )
        return HistoricalCloseEvidenceDiscovery(
            classification="unique_candidate_exact_recheck",
            candidate_count=1,
            reconciliation=replace(reconciliation, metadata=metadata),
        )

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        return await self._transport.normalize_quantity(symbol, quantity)

    def _gate_catalog_entry_leverage_limit(
        self, symbol: str, venue_symbol: str
    ) -> int | None:
        metadata_by_symbol = getattr(self._transport, "_symbol_metadata", {}) or {}
        for key in (venue_symbol, symbol):
            metadata = metadata_by_symbol.get(key)
            if not isinstance(metadata, dict):
                continue
            for field in ("leverage_max", "max_leverage", "cross_leverage_limit"):
                try:
                    value = float(metadata.get(field))
                except (TypeError, ValueError, OverflowError):
                    continue
                if math.isfinite(value) and value > 0 and value.is_integer():
                    return int(value)
        return None

    @staticmethod
    def _gate_set_leverage_response(raw: Any) -> list[int]:
        """Read the applied leverage from a set-leverage response.

        Gate answers with one Position object in single mode and a Position
        array for dual-mode accounts; dual rows carry the applied value in
        `lever` while the legacy `leverage` field can read "0" there."""
        rows = raw if isinstance(raw, list) else [raw]
        if not rows or not all(isinstance(row, dict) for row in rows):
            raise ValueError("Gate set-leverage response is malformed")
        applied: list[int] = []
        for row in rows:
            for field in ("lever", "leverage"):
                try:
                    value = float(row.get(field))
                except (TypeError, ValueError, OverflowError):
                    continue
                if math.isfinite(value) and value > 0 and value.is_integer():
                    applied.append(int(value))
                    break
            else:
                raise ValueError(
                    "Gate set-leverage response leverage is missing or invalid"
                )
        return applied

    @staticmethod
    def _gate_requires_dual_leverage_retry(error: Exception) -> bool:
        if not isinstance(error, TransportError):
            return False
        if error.category != TransportErrorCategory.REQUEST_REJECTED:
            return False
        text = f"{error} {error.body}".lower()
        return (
            "position_mode" in text
            or "position mode" in text
            or "dual mode" in text
            or "dual_comp" in text
            or "hedge mode" in text
            or "position_not_found" in text
            or "position not found" in text
        )

    async def ensure_entry_leverage(
        self,
        symbol: str,
        leverage: int,
        *,
        notional_quote: float | None = None,
    ) -> None:
        """Set Gate cross leverage, retrying only the V1 dual-mode mismatch path."""
        target = int(leverage or 0)
        if target <= 0 or self._mode != "live":
            return

        venue_symbol = self._transport._venue_symbol(symbol)
        catalog_limit = self._gate_catalog_entry_leverage_limit(symbol, venue_symbol)
        effective = min(target, catalog_limit) if catalog_limit else target
        effective = max(int(effective), 1)
        path = f"/api/v4/futures/usdt/positions/{venue_symbol}/set_leverage"
        payload: dict[str, Any] = {
            "venue": Venue.GATE.value,
            "symbol": venue_symbol,
            "requested_leverage": target,
            "effective_leverage": effective,
            "catalog_max_leverage": catalog_limit,
            "requested_notional_quote": float(notional_quote or 0.0),
            "set_leverage_endpoint": path,
        }
        try:
            async def set_leverage(dual_side: str | None = None) -> None:
                params = {"leverage": str(effective), "margin_mode": "cross"}
                if dual_side:
                    params["dual_side"] = dual_side
                response = await self._transport._request(
                    "POST", path, params=params, private=True
                )
                applied = self._gate_set_leverage_response(response)
                payload["position_mode"] = (
                    "dual" if isinstance(response, list) else "single"
                )
                if any(value != effective for value in applied):
                    raise OrderSubmitError(
                        SubmitFailureClass.REJECTED,
                        "Gate entry leverage response mismatch "
                        f"symbol={venue_symbol} expected={effective} actual={applied}",
                    )

            try:
                await set_leverage()
            except Exception as exc:
                if not self._gate_requires_dual_leverage_retry(exc):
                    raise
                await set_leverage("dual_long")
                await set_leverage("dual_short")

            payload["outcome"] = "set_and_verified"
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
                f"Gate entry leverage prepare failed: {exc}",
            ) from exc

    async def shutdown(self) -> None:
        await self._transport.close()


def _gate_history_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def find_gate_historical_close_execution_candidates(
    trades: Iterable[dict[str, Any]],
    *,
    contract: str,
    side: Side | str,
    quantity: float,
    closed_at_ms: int,
    time_window_ms: int = 300_000,
    quantity_relative_tolerance: float = 1e-9,
) -> list[dict[str, Any]]:
    """Group Gate my_trades rows into strictly matching close candidates.

    Gate ``my_trades`` rows carry a signed ``size`` (negative = sell) and no
    reduce-only marker, so sign-filtered rows are candidate selection only;
    the order row's ``is_reduce_only`` decides close ownership during the
    exact recheck.  Gate timestamps are seconds.
    """
    expected_side = (
        side.value.upper() if isinstance(side, Side) else str(side).upper()
    )
    expected_sign = -1.0 if expected_side == "SELL" else 1.0
    quantity_tolerance = max(quantity * quantity_relative_tolerance, 1e-12)
    grouped: dict[str, dict[str, Any]] = {}
    for raw in trades:
        if not isinstance(raw, dict):
            continue
        order_id = str(raw.get("order_id") or "").strip()
        trade_size = _gate_history_float(raw.get("size"))
        created_at_sec = _gate_history_float(raw.get("create_time"))
        if (
            not order_id
            or str(raw.get("contract") or "").upper() != contract.upper()
            or trade_size is None
            or trade_size == 0.0
            or (trade_size > 0) != (expected_sign > 0)
            or created_at_sec is None
            or created_at_sec <= 0
        ):
            continue
        traded_at_ms = int(created_at_sec * 1000)
        if abs(traded_at_ms - closed_at_ms) > time_window_ms:
            continue
        candidate = grouped.setdefault(
            order_id,
            {
                "order_id": order_id,
                "quantity": 0.0,
                "updated_at_ms": 0,
            },
        )
        candidate["quantity"] += abs(trade_size)
        candidate["updated_at_ms"] = max(candidate["updated_at_ms"], traded_at_ms)
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
