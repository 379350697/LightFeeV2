"""Entry execution state machine matching Rust V1 entry flow.

Rust references:
- src/execution_core/entry_sync.rs: PendingEntryHedge, state transitions
- src/engine/entry.rs: EntryAttemptOutcome, execute_entry_order_leg
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, replace
from enum import Enum
from typing import Optional

from lightfee.core.domain import OrderFill, OrderRequest, Side, Venue
from lightfee.engine.execution_planner import ExecutionRoute
from lightfee.engine.state import OpenPosition


def generate_review_id() -> str:
    """Generate a unique review id for observability tracing.

    V1: review_id is a short unique identifier that survives through the
    full position lifecycle — entry, journal, state snapshot, offline analysis.
    """
    return f"rev-{uuid.uuid4().hex[:12]}"


class EntryState(Enum):
    IDLE = "idle"
    SUBMITTING_MAKER = "submitting_maker"
    MAKER_RESTING = "maker_resting"
    SUBMITTING_HEDGE = "submitting_hedge"
    HEDGE_PENDING = "hedge_pending"
    COMPLETED = "completed"
    FAILED = "failed"
    # --- V1 passive fallback and residual states ---
    PASSIVE_FALLBACK = "passive_fallback"
    FAILED_WITH_RESIDUAL = "failed_with_residual"

    @property
    def is_terminal(self) -> bool:
        return self in (EntryState.COMPLETED, EntryState.FAILED, EntryState.FAILED_WITH_RESIDUAL)


class EntryType(Enum):
    STANDARD_DUAL_TAKER = "standard_dual_taker"
    PASSIVE_INCREMENTAL = "passive_incremental"
    PASSIVE_FALLBACK = "passive_fallback"


@dataclass
class EntryContext:
    entry_id: str
    symbol: str
    long_venue: Venue
    short_venue: Venue
    long_quantity: float
    short_quantity: float
    long_price_hint: float
    short_price_hint: float
    maker_leg: Side
    entry_type: EntryType
    state: EntryState = EntryState.IDLE
    maker_fill: Optional[OrderFill] = None
    hedge_fill: Optional[OrderFill] = None
    created_at_ms: int = 0
    # --- V1 maker-event lane repricing ---
    parent_entry_id: Optional[str] = None
    reprice_action: str = ""
    # --- V1 planner output ---
    planned_route: ExecutionRoute = ExecutionRoute.PASSIVE_INCREMENTAL


def advance_entry_state(ctx: EntryContext, next_state: EntryState) -> EntryContext:
    """Advance EntryContext to next_state, enforcing valid transitions.

    V1 transition rules:
    - COMPLETED, FAILED, FAILED_WITH_RESIDUAL are terminal
    - All other states valid for forward progress
    """
    if ctx.state.is_terminal:
        raise ValueError(
            f"Cannot advance from terminal state {ctx.state.value} to {next_state.value}"
        )
    return replace(ctx, state=next_state)


def build_entry_orders(
    ctx: EntryContext,
) -> tuple[OrderRequest, OrderRequest]:
    """Build maker and hedge order requests with V1 TIF/reduce-only/clientOrderId.

    V1 semantics:
    - Maker: GTC post-only with deterministic clientOrderId
    - Hedge: IOC reduce-only=False (hedge is opening, not closing)
    - Both legs carry clientOrderId for idempotency and reconciliation
    """
    from lightfee.core.domain import TimeInForce

    maker_cid = f"{ctx.entry_id}-maker"
    hedge_cid = f"{ctx.entry_id}-hedge"

    if ctx.maker_leg == Side.BUY:
        maker_req = OrderRequest(
            venue=ctx.long_venue,
            symbol=ctx.symbol,
            side=Side.BUY,
            quantity=ctx.long_quantity,
            price=ctx.long_price_hint,
            post_only=True,
            time_in_force=TimeInForce.GTC,
            client_order_id=maker_cid,
        )
        hedge_req = OrderRequest(
            venue=ctx.short_venue,
            symbol=ctx.symbol,
            side=Side.SELL,
            quantity=ctx.short_quantity,
            price=ctx.short_price_hint,
            reduce_only=False,
            time_in_force=TimeInForce.IOC,
            client_order_id=hedge_cid,
        )
    else:
        maker_req = OrderRequest(
            venue=ctx.short_venue,
            symbol=ctx.symbol,
            side=Side.SELL,
            quantity=ctx.short_quantity,
            price=ctx.short_price_hint,
            post_only=True,
            time_in_force=TimeInForce.GTC,
            client_order_id=maker_cid,
        )
        hedge_req = OrderRequest(
            venue=ctx.long_venue,
            symbol=ctx.symbol,
            side=Side.BUY,
            quantity=ctx.long_quantity,
            price=ctx.long_price_hint,
            reduce_only=False,
            time_in_force=TimeInForce.IOC,
            client_order_id=hedge_cid,
        )
    return maker_req, hedge_req


def build_open_position(
    ctx: EntryContext,
    maker_fill: OrderFill,
    hedge_fill: OrderFill,
    now_ms: int,
    review_id: str | None = None,
) -> OpenPosition:
    """Build an OpenPosition from completed entry fills."""
    maker_is_long = ctx.maker_leg == Side.BUY
    matched_qty = min(maker_fill.quantity, hedge_fill.quantity)

    if maker_is_long:
        long_fill, short_fill = maker_fill, hedge_fill
        long_qty = matched_qty
        short_qty = matched_qty
        long_entry_price = maker_fill.price
        short_entry_price = hedge_fill.price
    else:
        long_fill, short_fill = hedge_fill, maker_fill
        long_qty = matched_qty
        short_qty = matched_qty
        long_entry_price = hedge_fill.price
        short_entry_price = maker_fill.price

    return OpenPosition(
        position_id=ctx.entry_id,
        symbol=ctx.symbol,
        long_venue=ctx.long_venue,
        short_venue=ctx.short_venue,
        long_quantity=long_qty,
        short_quantity=short_qty,
        long_entry_price=long_entry_price,
        short_entry_price=short_entry_price,
        opened_at_ms=now_ms,
        review_id=review_id,
        long_fill=long_fill,
        short_fill=short_fill,
    )
