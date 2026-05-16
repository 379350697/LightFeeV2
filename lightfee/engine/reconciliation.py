"""Order and position reconciliation service matching Rust V1 recovery behavior.

Rust references:
- src/engine/recovery.rs (reconcile_dust_residuals, reconcile_open_positions_internal,
  process_pending_close_reconciliations, process_pending_residual_repairs)
- src/engine/state.rs (PendingCloseReconciliation lifecycle)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import OrderFill, PositionSnapshot, Side, Venue


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
    ) -> None:
        if venue is None:
            return
        self._order_diagnostics.append({
            "kind": "order.reconcile_result",
            "payload": {
                "venue": venue.value if hasattr(venue, "value") else str(venue),
                "symbol": symbol,
                "endpoint": "fetch_order_status",
                "product_type": "reconciliation",
                "category": "reconciliation",
                "order_id": order_id,
                "client_order_id": client_order_id,
                "status": status,
                "reason": reason,
                "raw_exchange_status": raw_exchange_status,
                "fill_qty": fill_qty,
                "fill_price": fill_price,
                "position_qty": position_qty,
                "position_side": position_side,
                "hedge_submitted": hedge_submitted,
                "raw_price": None,
                "raw_qty": None,
                "quantized_price": None,
                "quantized_qty": None,
                "tick_size": None,
                "quantity_step": None,
                "response_classification": status,
            },
        })

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
            if long_recon is not None and long_recon.quantity > 0:
                result.long_status = "filled"
                result.long_fill = long_recon
                long_raw_status = _recon_meta_get(long_recon, 'raw_exchange_status', '')
            else:
                result.long_status = "uncertain" if long_recon is None else _recon_meta_get(long_recon, 'raw_exchange_status', 'uncertain')
                long_raw_status = _recon_meta_get(long_recon, 'raw_exchange_status', '') if long_recon is not None else ''
            pos = await long_adapter.fetch_position(symbol)
            result.long_position = pos

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
            if short_recon is not None and short_recon.quantity > 0:
                result.short_status = "filled"
                result.short_fill = short_recon
                short_raw_status = _recon_meta_get(short_recon, 'raw_exchange_status', '')
            else:
                result.short_status = "uncertain" if short_recon is None else _recon_meta_get(short_recon, 'raw_exchange_status', 'uncertain')
                short_raw_status = _recon_meta_get(short_recon, 'raw_exchange_status', '') if short_recon is not None else ''
            pos = await short_adapter.fetch_position(symbol)
            result.short_position = pos

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
            order_id=long_order_id,
            client_order_id=long_client_order_id,
            status=result.long_status,
            raw_exchange_status=long_raw_status,
            fill_qty=result.long_fill.quantity if result.long_fill else 0.0,
            fill_price=_recon_fill_price(result.long_fill),
            position_qty=result.long_position.quantity if result.long_position else 0.0,
            position_side=result.long_position.side.value if result.long_position else "",
        )
        self._record_reconcile_result(
            venue=short_venue,
            symbol=symbol,
            order_id=short_order_id,
            client_order_id=short_client_order_id,
            status=result.short_status,
            raw_exchange_status=short_raw_status,
            fill_qty=result.short_fill.quantity if result.short_fill else 0.0,
            fill_price=_recon_fill_price(result.short_fill),
            position_qty=result.short_position.quantity if result.short_position else 0.0,
            position_side=result.short_position.side.value if result.short_position else "",
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
