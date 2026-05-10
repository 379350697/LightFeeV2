"""Order and position reconciliation service matching Rust V1 recovery behavior.

Rust references:
- src/engine/recovery.rs (reconcile_dust_residuals, reconcile_open_positions_internal,
  process_pending_close_reconciliations, process_pending_residual_repairs)
- src/engine/state.rs (PendingCloseReconciliation lifecycle)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

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

    def _adapter_for(self, venue: Venue) -> Optional[VenueAdapter]:
        return self._adapters.get(venue)

    async def reconcile_position(
        self,
        position_id: str,
        symbol: str,
        long_venue: Optional[Venue] = None,
        short_venue: Optional[Venue] = None,
        long_order_id: str = "",
        short_order_id: str = "",
    ) -> PositionReconciliationResult:
        """Query both venue adapters for fill and position state."""
        result = PositionReconciliationResult(
            position_id=position_id,
            symbol=symbol,
        )

        long_adapter = self._adapter_for(long_venue) if long_venue else None
        short_adapter = self._adapter_for(short_venue) if short_venue else None

        if long_adapter is not None:
            if long_order_id:
                fill = await long_adapter.fetch_order_fill_reconciliation(
                    symbol, long_order_id
                )
                if fill is not None:
                    result.long_status = "filled"
                    result.long_fill = fill
                else:
                    result.long_status = "uncertain"
            pos = await long_adapter.fetch_position(symbol)
            result.long_position = pos

        if short_adapter is not None:
            if short_order_id:
                fill = await short_adapter.fetch_order_fill_reconciliation(
                    symbol, short_order_id
                )
                if fill is not None:
                    result.short_status = "filled"
                    result.short_fill = fill
                else:
                    result.short_status = "uncertain"
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
