"""Order and position reconciliation service matching Rust V1 recovery behavior.

Rust references:
- src/engine/recovery.rs (reconcile_dust_residuals, reconcile_open_positions_internal,
  process_pending_close_reconciliations, process_pending_residual_repairs)
- src/engine/state.rs (PendingCloseReconciliation lifecycle)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Mapping, Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import OrderFill, PositionSnapshot, Side, Venue
from lightfee.engine.order_truth_ledger import ORDER_TRUTH_LEDGER, OrderTruthFillStatus

if TYPE_CHECKING:
    from lightfee.engine.residual import ResidualExposureTask
    from lightfee.engine.state import PendingCloseReconciliation


# ---------------------------------------------------------------------------
# Reconciliation result types
# ---------------------------------------------------------------------------

@dataclass
class ReconciliationResult:
    """Result of reconciling an unknown order via venue query."""

    status: str  # "filled", "uncertain", "not_found", "rejected"
    order_id: str = ""
    symbol: str = ""
    fill: Optional[OrderFill] = None
    reason: str = ""


@dataclass
class PositionReconciliationResult:
    """Result of reconciling a two-leg position."""

    position_id: str
    symbol: str
    long_status: str = "uncertain"
    short_status: str = "uncertain"
    long_fill: Optional[OrderFill] = None
    short_fill: Optional[OrderFill] = None
    long_position: Optional[PositionSnapshot] = None
    short_position: Optional[PositionSnapshot] = None
    # A terminal Bitget row with an explicit cumulative fill of zero.  This is
    # narrower than a generic ``not_found``: runtime may lower stale local
    # fill state only when the corresponding live venue position is also flat.
    long_terminal_zero_fill: bool = False
    short_terminal_zero_fill: bool = False
    # A terminal Bitget row whose execution fields are absent or malformed.
    # This is not zero-fill evidence; pending-entry recovery must retain and
    # retry rather than acting on any stale local fill high-water mark.
    long_terminal_fill_quantity_unavailable: bool = False
    short_terminal_fill_quantity_unavailable: bool = False
    matched_quantity: float = 0.0
    residual_long: float = 0.0
    residual_short: float = 0.0
    is_flat: bool = False
    reason: str = ""


# ---------------------------------------------------------------------------
# Fill object compatibility helpers
# ---------------------------------------------------------------------------


def _recon_fill_price(obj) -> float:
    """Return fill price from either OrderFill (has price) or OrderFillReconciliation (has average_price)."""
    if obj is None:
        return 0.0
    return getattr(obj, "average_price", getattr(obj, "price", 0.0))


def _recon_metadata(obj) -> Optional[dict]:
    """Return metadata from either OrderFillReconciliation or None for OrderFill."""
    if obj is None:
        return None
    return getattr(obj, "metadata", None)


def _recon_meta_get(obj, key: str, default: Any = "") -> Any:
    """Get a key from metadata, safely handling both OrderFill and OrderFillReconciliation."""
    meta = _recon_metadata(obj)
    if meta is None:
        return default
    return meta.get(key, default)


def _venue_id(venue: Optional[Venue]) -> str:
    if venue is None:
        return ""
    return venue.value if hasattr(venue, "value") else str(venue)


def _as_list(value: Any) -> list[Any]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _live_position_delta(position: Optional[PositionSnapshot]) -> dict[str, Any]:
    if position is None:
        return {
            "quantity": 0.0,
            "side": "",
            "observed_at_ms": 0,
            "source": "fetch_position_unavailable",
        }
    return {
        "quantity": abs(float(position.quantity)),
        "signed_quantity": float(position.quantity),
        "side": position.side.value if hasattr(position.side, "value") else str(position.side),
        "observed_at_ms": position.observed_at_ms,
        "source": "fetch_position",
    }


def _endpoint_evidence_proves_no_effect(subtype: str, response_classification: str) -> bool:
    text = f"{subtype} {response_classification}".lower()
    if "accepted" in text or "stale_accepted" in text:
        return False
    return (
        subtype in {"execution_not_found", "open_order_not_found", "closed_order_not_found"}
        or "execution_not_found" in text
        or "no_execution" in text
        or ("not_found" in text and "accepted" not in text)
    )


# ---------------------------------------------------------------------------
# Order reconciler
# ---------------------------------------------------------------------------

class OrderReconciler:
    """Reconciles pending/unknown orders by querying venue adapters.

    Rust V1 equivalent: engine queries venue adapters for order fills and
    position state during recovery. This service encapsulates the async
    adapter queries needed to resolve uncertainty.

    Constructor accepts only a dict[Venue, VenueAdapter] map. Both legs
    must be queried through the adapter map — single-adapter shortcuts
    are not permitted (V1 requires both-leg reconciliation).
    """

    def __init__(
        self,
        adapters: dict[Venue, VenueAdapter],
    ) -> None:
        self._adapters = dict(adapters)
        self._order_diagnostics: list[dict[str, Any]] = []

    def _adapter_for(self, venue: Venue) -> Optional[VenueAdapter]:
        return self._adapters.get(venue)

    @staticmethod
    def _query_proves_terminal_zero_fill(payload: Mapping[str, Any]) -> bool:
        """Return whether a queried order exactly proved terminal zero fill."""
        return (
            str(payload.get("response_classification", "")) == "terminal_zero_fill"
            and str(payload.get("uncertain_subtype", "")) == "execution_not_found"
        )

    @staticmethod
    def _query_has_terminal_fill_quantity_unavailable(
        payload: Mapping[str, Any],
    ) -> bool:
        """Return whether a terminal response lacked usable execution quantity."""
        return (
            str(payload.get("response_classification", ""))
            == "terminal_fill_quantity_unavailable"
            and str(payload.get("uncertain_subtype", ""))
            == "execution_fields_unavailable"
        )

    def drain_order_diagnostics(self) -> list[dict[str, Any]]:
        events = list(self._order_diagnostics)
        self._order_diagnostics.clear()
        return events

    def _record_reconcile_result(
        self,
        *,
        venue: Optional[Venue],
        symbol: str,
        order_id: str,
        client_order_id: str,
        status: str,
        reason: str = "",
        raw_exchange_status: str = "",
        fill_qty: float = 0.0,
        fill_price: float = 0.0,
        position_qty: float = 0.0,
        position_side: str = "",
        hedge_submitted: bool = False,
        uncertain_subtype: str = "",
        queried_endpoints: Optional[list[str]] = None,
        endpoint_responses: Optional[list[dict[str, Any]]] = None,
        live_position_delta: Optional[dict[str, Any]] = None,
        next_action: str = "",
    ) -> None:
        if venue is None:
            return
        endpoint_list = [str(e) for e in (queried_endpoints or []) if str(e)]
        response_summary = (
            status
            if not endpoint_list and status == "uncertain"
            else raw_exchange_status or status
        )
        payload = {
            "venue": _venue_id(venue),
            "symbol": symbol,
            "endpoint": endpoint_list[0] if endpoint_list else "fetch_order_status",
            "queried_endpoints": endpoint_list,
            "endpoint_responses": endpoint_responses or [
                {"endpoint": endpoint, "classification": response_summary}
                for endpoint in endpoint_list
            ],
            "product_type": "reconciliation",
            "category": "reconciliation",
            "order_id": order_id,
            "exchange_order_id": order_id,
            "client_order_id": client_order_id,
            "status": status,
            "reason": reason,
            "uncertain_subtype": uncertain_subtype,
            "raw_exchange_status": raw_exchange_status,
            "fill_qty": fill_qty,
            "fill_price": fill_price,
            "position_qty": position_qty,
            "position_side": position_side,
            "live_position_delta": live_position_delta or {
                "quantity": abs(float(position_qty)),
                "signed_quantity": float(position_qty),
                "side": position_side,
                "observed_at_ms": 0,
                "source": "fetch_position",
            },
            "next_action": next_action,
            "hedge_submitted": hedge_submitted,
            "raw_price": None,
            "raw_qty": None,
            "quantized_price": None,
            "quantized_qty": None,
            "tick_size": None,
            "quantity_step": None,
            "response_classification": response_summary,
        }
        self._order_diagnostics.append({
            "kind": "order.reconcile_result",
            "payload": payload,
        })
        if next_action == "clear_uncertain_state" and uncertain_subtype:
            self._order_diagnostics.append({
                "kind": "order.reconcile_resolution",
                "payload": {
                    "venue": payload["venue"],
                    "symbol": symbol,
                    "client_order_id": client_order_id,
                    "exchange_order_id": order_id,
                    "queried_endpoints": endpoint_list,
                    "response_classification": response_summary,
                    "live_position_delta": payload["live_position_delta"],
                    "resolution": uncertain_subtype,
                    "clears_uncertain_state": True,
                    "next_action": "clear_uncertain_state",
                },
            })

    @staticmethod
    def _drain_adapter_order_diagnostics(adapter: Any) -> list[dict[str, Any]]:
        drains = []
        drain = getattr(adapter, "drain_order_diagnostics", None)
        if callable(drain):
            drains.append(drain)
        transport = getattr(adapter, "_transport", None)
        transport_drain = getattr(transport, "drain_order_diagnostics", None)
        if callable(transport_drain):
            drains.append(transport_drain)

        events: list[dict[str, Any]] = []
        for drain_fn in drains:
            try:
                events.extend(drain_fn())
            except Exception:
                continue
        return events

    @staticmethod
    def _latest_query_payload(events: list[dict[str, Any]]) -> dict[str, Any]:
        for event in reversed(events):
            if event.get("kind") == "order.reconcile_query":
                payload = event.get("payload", {})
                return payload if isinstance(payload, dict) else {}
        return {}

    @staticmethod
    def _status_from_reconciliation(
        reconciliation: Any,
        position: Optional[PositionSnapshot],
        query_payload: dict[str, Any],
        *,
        venue: Optional[Venue] = None,
        symbol: str = "",
        order_id: str = "",
        client_order_id: str = "",
    ) -> tuple[str, str, str, str, list[str], list[dict[str, Any]], str]:
        raw_meta = _recon_metadata(reconciliation)
        meta = raw_meta if isinstance(raw_meta, Mapping) else {}
        merged_meta: dict[str, Any] = {}
        if isinstance(meta, Mapping):
            merged_meta.update(meta)
        if isinstance(query_payload, Mapping):
            for key, value in query_payload.items():
                merged_meta.setdefault(key, value)
        endpoints = _as_list(meta.get("queried_endpoints") or meta.get("reconcile_endpoints"))
        if not endpoints:
            endpoints = _as_list(query_payload.get("queried_endpoints"))
        endpoint_responses = _as_list(meta.get("endpoint_responses"))
        if not endpoint_responses:
            endpoint_responses = _as_list(query_payload.get("endpoint_responses"))
        response_classification = str(
            meta.get("response_classification")
            or query_payload.get("response_classification")
            or meta.get("raw_exchange_status")
            or ""
        )
        subtype = str(
            meta.get("uncertain_subtype")
            or query_payload.get("uncertain_subtype")
            or ""
        )
        next_action = str(meta.get("next_action") or query_payload.get("next_action") or "")

        if reconciliation is not None and getattr(reconciliation, "quantity", 0.0) > 0:
            truth_decision = ORDER_TRUTH_LEDGER.resolve_order_success(
                venue=venue or getattr(reconciliation, "venue", None),
                symbol=symbol or str(getattr(reconciliation, "symbol", "") or ""),
                order_id=order_id or str(getattr(reconciliation, "order_id", "") or ""),
                client_order_id=(
                    client_order_id
                    or str(getattr(reconciliation, "client_order_id", "") or "")
                ),
                target_qty=float(getattr(reconciliation, "quantity", 0.0) or 0.0),
                reconciliation=reconciliation,
                metadata=merged_meta,
            )
            raw = str(
                meta.get("raw_exchange_status")
                or response_classification
                or truth_decision.fill_status.value
            )
            if truth_decision.fill_status != OrderTruthFillStatus.CONFIRMED_FILL:
                return (
                    truth_decision.fill_status.value,
                    raw,
                    truth_decision.fill_status.value,
                    response_classification or truth_decision.fill_status.value,
                    [str(e) for e in endpoints],
                    [e for e in endpoint_responses if isinstance(e, dict)],
                    next_action or truth_decision.decision,
                )
            if subtype:
                next_action = "clear_uncertain_state"
            return (
                "filled",
                raw,
                subtype,
                response_classification or "filled",
                [str(e) for e in endpoints],
                [e for e in endpoint_responses if isinstance(e, dict)],
                next_action,
            )

        position_qty = abs(float(position.quantity)) if position is not None else 0.0
        has_endpoint_evidence = bool(endpoints or query_payload)
        explicit_live_position_match = (
            subtype == "live_position_confirmed"
            or response_classification == "live_position_confirmed"
            or bool(meta.get("live_position_confirmed"))
            or bool(query_payload.get("live_position_confirmed"))
        )
        if position_qty > 1e-12 and has_endpoint_evidence and explicit_live_position_match:
            return (
                "filled",
                response_classification or "live_position_confirmed",
                "live_position_confirmed",
                response_classification or "live_position_confirmed",
                [str(e) for e in endpoints],
                [e for e in endpoint_responses if isinstance(e, dict)],
                "clear_uncertain_state",
            )
        if (
            position is not None
            and position_qty <= 1e-12
            and has_endpoint_evidence
            and _endpoint_evidence_proves_no_effect(subtype, response_classification)
        ):
            return (
                "not_found",
                response_classification or "live_no_effect_confirmed",
                "live_no_effect_confirmed",
                response_classification or "live_no_effect_confirmed",
                [str(e) for e in endpoints],
                [e for e in endpoint_responses if isinstance(e, dict)],
                "clear_uncertain_state",
            )

        if not subtype:
            text = response_classification.lower()
            if "timeout" in text:
                subtype = "submit_timeout"
            elif "duplicate" in text:
                subtype = "duplicate_client_id"
            elif "open" in text and "not_found" in text:
                subtype = "open_order_not_found"
            elif "closed" in text and "not_found" in text:
                subtype = "closed_order_not_found"
            elif "accepted" in text or "new" in text or "open" in text:
                subtype = "stale_accepted_order"
            else:
                subtype = "execution_not_found"
        return (
            "uncertain",
            response_classification or subtype,
            subtype,
            response_classification or subtype,
            [str(e) for e in endpoints],
            [e for e in endpoint_responses if isinstance(e, dict)],
            next_action or "reconcile_again_after_backoff",
        )

    async def reconcile_position(
        self,
        position_id: str,
        symbol: str,
        long_venue: Optional[Venue] = None,
        short_venue: Optional[Venue] = None,
        long_order_id: str = "",
        short_order_id: str = "",
        long_client_order_id: str = "",
        short_client_order_id: str = "",
    ) -> PositionReconciliationResult:
        """Query both venue adapters for fill and position state.

        V1: prefers clientOrderId lookup when order_id is empty or unfound.
        Falls back to order_id lookup, then position-only check.
        """
        result = PositionReconciliationResult(
            position_id=position_id,
            symbol=symbol,
        )

        long_adapter = self._adapter_for(long_venue) if long_venue else None
        short_adapter = self._adapter_for(short_venue) if short_venue else None
        long_raw_status = ""
        short_raw_status = ""
        long_uncertain_subtype = ""
        short_uncertain_subtype = ""
        long_queried_endpoints: list[str] = []
        short_queried_endpoints: list[str] = []
        long_endpoint_responses: list[dict[str, Any]] = []
        short_endpoint_responses: list[dict[str, Any]] = []
        long_next_action = ""
        short_next_action = ""

        if long_adapter is not None:
            long_recon = None
            if long_order_id:
                long_recon = await long_adapter.fetch_order_fill_reconciliation(
                    symbol, long_order_id, long_client_order_id
                )
            elif long_client_order_id:
                long_recon = await long_adapter.fetch_order_fill_reconciliation(
                    symbol, "", long_client_order_id
                )
            pos = await long_adapter.fetch_position(symbol)
            result.long_position = pos
            long_query_diagnostics = self._drain_adapter_order_diagnostics(long_adapter)
            self._order_diagnostics.extend(long_query_diagnostics)
            long_query_payload = self._latest_query_payload(long_query_diagnostics)
            result.long_terminal_zero_fill = self._query_proves_terminal_zero_fill(
                long_query_payload
            )
            result.long_terminal_fill_quantity_unavailable = (
                self._query_has_terminal_fill_quantity_unavailable(
                    long_query_payload
                )
            )
            (
                result.long_status,
                long_raw_status,
                long_uncertain_subtype,
                _long_response_classification,
                long_queried_endpoints,
                long_endpoint_responses,
                long_next_action,
            ) = self._status_from_reconciliation(
                long_recon,
                pos,
                long_query_payload,
                venue=long_venue,
                symbol=symbol,
                order_id=long_order_id,
                client_order_id=long_client_order_id,
            )
            if result.long_status == "filled" and long_recon is not None:
                result.long_fill = long_recon

        if short_adapter is not None:
            short_recon = None
            if short_order_id:
                short_recon = await short_adapter.fetch_order_fill_reconciliation(
                    symbol, short_order_id, short_client_order_id
                )
            elif short_client_order_id:
                short_recon = await short_adapter.fetch_order_fill_reconciliation(
                    symbol, "", short_client_order_id
                )
            pos = await short_adapter.fetch_position(symbol)
            result.short_position = pos
            short_query_diagnostics = self._drain_adapter_order_diagnostics(short_adapter)
            self._order_diagnostics.extend(short_query_diagnostics)
            short_query_payload = self._latest_query_payload(short_query_diagnostics)
            result.short_terminal_zero_fill = self._query_proves_terminal_zero_fill(
                short_query_payload
            )
            result.short_terminal_fill_quantity_unavailable = (
                self._query_has_terminal_fill_quantity_unavailable(
                    short_query_payload
                )
            )
            (
                result.short_status,
                short_raw_status,
                short_uncertain_subtype,
                _short_response_classification,
                short_queried_endpoints,
                short_endpoint_responses,
                short_next_action,
            ) = self._status_from_reconciliation(
                short_recon,
                pos,
                short_query_payload,
                venue=short_venue,
                symbol=symbol,
                order_id=short_order_id,
                client_order_id=short_client_order_id,
            )
            if result.short_status == "filled" and short_recon is not None:
                result.short_fill = short_recon

        # Determine if flat
        long_qty = result.long_position.quantity if result.long_position else 0.0
        short_qty = abs(result.short_position.quantity) if result.short_position else 0.0
        result.is_flat = abs(long_qty) < 1e-12 and abs(short_qty) < 1e-12

        if not result.is_flat and result.long_position and result.short_position:
            result.matched_quantity = min(
                abs(result.long_position.quantity),
                abs(result.short_position.quantity),
            )

        self._record_reconcile_result(
            venue=long_venue,
            symbol=symbol,
            order_id=(
                result.long_fill.order_id
                if result.long_fill is not None and result.long_fill.order_id
                else long_order_id
            ),
            client_order_id=long_client_order_id,
            status=result.long_status,
            raw_exchange_status=long_raw_status,
            fill_qty=result.long_fill.quantity if result.long_fill else 0.0,
            fill_price=_recon_fill_price(result.long_fill),
            position_qty=result.long_position.quantity if result.long_position else 0.0,
            position_side=result.long_position.side.value if result.long_position else "",
            uncertain_subtype=long_uncertain_subtype,
            queried_endpoints=long_queried_endpoints,
            endpoint_responses=long_endpoint_responses,
            live_position_delta=_live_position_delta(result.long_position),
            next_action=long_next_action,
        )
        self._record_reconcile_result(
            venue=short_venue,
            symbol=symbol,
            order_id=(
                result.short_fill.order_id
                if result.short_fill is not None and result.short_fill.order_id
                else short_order_id
            ),
            client_order_id=short_client_order_id,
            status=result.short_status,
            raw_exchange_status=short_raw_status,
            fill_qty=result.short_fill.quantity if result.short_fill else 0.0,
            fill_price=_recon_fill_price(result.short_fill),
            position_qty=result.short_position.quantity if result.short_position else 0.0,
            position_side=result.short_position.side.value if result.short_position else "",
            uncertain_subtype=short_uncertain_subtype,
            queried_endpoints=short_queried_endpoints,
            endpoint_responses=short_endpoint_responses,
            live_position_delta=_live_position_delta(result.short_position),
            next_action=short_next_action,
        )
        return result


# ---------------------------------------------------------------------------
# Reconciliation helpers
# ---------------------------------------------------------------------------

async def reconcile_unknown_order(
    adapter: VenueAdapter,
    symbol: str,
    order_id: str,
    client_order_id: str = "",
) -> ReconciliationResult:
    """Query a single venue adapter to resolve an unknown order.

    Rust V1: recovery queries venue for order status when outcomes are uncertain.
    """
    try:
        fill = await adapter.fetch_order_fill_reconciliation(
            symbol, order_id, client_order_id
        )
        if fill is not None:
            truth_decision = ORDER_TRUTH_LEDGER.resolve_order_success(
                venue=getattr(fill, "venue", None) or getattr(adapter, "venue", None),
                symbol=symbol,
                order_id=order_id or str(getattr(fill, "order_id", "") or ""),
                client_order_id=(
                    client_order_id
                    or str(getattr(fill, "client_order_id", "") or "")
                ),
                target_qty=float(getattr(fill, "quantity", 0.0) or 0.0),
                reconciliation=fill,
                metadata=getattr(fill, "metadata", None),
            )
            if truth_decision.fill_status != OrderTruthFillStatus.CONFIRMED_FILL:
                return ReconciliationResult(
                    status=truth_decision.fill_status.value,
                    order_id=order_id,
                    symbol=symbol,
                    reason=truth_decision.decision,
                )
            return ReconciliationResult(
                status="filled",
                order_id=order_id,
                symbol=symbol,
                fill=fill,
            )
        return ReconciliationResult(
            status="uncertain",
            order_id=order_id,
            symbol=symbol,
            reason="adapter_returned_none",
        )
    except Exception as e:
        return ReconciliationResult(
            status="uncertain",
            order_id=order_id,
            symbol=symbol,
            reason=f"query_error:{e}",
        )


async def reconcile_pending_close(
    pending: "PendingCloseReconciliation",
    long_adapter: VenueAdapter,
    short_adapter: VenueAdapter,
    now_ms: int = 0,
) -> "PendingCloseReconciliation":
    """Process a pending close reconciliation entry.

    Rust V1: process_pending_close_reconciliations() queries adapters,
    updates attempt counts, and escalates backoff.

    Returns the updated PendingCloseReconciliation with incremented attempt
    and next_attempt_ms advanced according to exponential backoff.
    """
    CLOSE_RECONCILIATION_RETRY_BASE_MS = 30_000
    CLOSE_RECONCILIATION_RETRY_MAX_MS = 300_000

    pending.attempt_count += 1
    backoff = min(
        CLOSE_RECONCILIATION_RETRY_BASE_MS * (2 ** (pending.attempt_count - 1)),
        CLOSE_RECONCILIATION_RETRY_MAX_MS,
    )
    pending.next_attempt_ms = now_ms + backoff
    return pending


async def reconcile_residual_exposure(
    task: "ResidualExposureTask",
    adapter: VenueAdapter,
    now_ms: int = 0,
) -> str:
    """Reconcile a residual exposure task by querying the venue for current position.

    Rust V1: process_pending_residual_repairs() checks if the residual
    position has been naturally closed or still needs repair.

    Returns: "cleared", "retry", or "protect"
    """
    try:
        position = await adapter.fetch_position(task.symbol)
    except Exception:
        return "retry"

    if abs(position.quantity) < 1e-12:
        return "cleared"

    if task.deadline_ms > 0 and now_ms > task.deadline_ms:
        return "protect"

    return "retry"
