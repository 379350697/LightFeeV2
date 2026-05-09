"""Entry execution state machine matching Rust entry flow."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from lightfee.core.domain import OrderFill, OrderRequest, Side, Venue
from lightfee.engine.state import OpenPosition


class EntryState(Enum):
    IDLE = "idle"
    SUBMITTING_MAKER = "submitting_maker"
    MAKER_RESTING = "maker_resting"
    SUBMITTING_HEDGE = "submitting_hedge"
    HEDGE_PENDING = "hedge_pending"
    COMPLETED = "completed"
    FAILED = "failed"


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


def build_entry_orders(
    ctx: EntryContext,
) -> tuple[OrderRequest, OrderRequest]:
    """Build maker and hedge order requests."""
    if ctx.maker_leg == Side.BUY:
        maker_req = OrderRequest(
            venue=ctx.long_venue,
            symbol=ctx.symbol,
            side=Side.BUY,
            quantity=ctx.long_quantity,
            price=ctx.long_price_hint,
            post_only=True,
        )
        hedge_req = OrderRequest(
            venue=ctx.short_venue,
            symbol=ctx.symbol,
            side=Side.SELL,
            quantity=ctx.short_quantity,
            price=ctx.short_price_hint,
        )
    else:
        maker_req = OrderRequest(
            venue=ctx.short_venue,
            symbol=ctx.symbol,
            side=Side.SELL,
            quantity=ctx.short_quantity,
            price=ctx.short_price_hint,
            post_only=True,
        )
        hedge_req = OrderRequest(
            venue=ctx.long_venue,
            symbol=ctx.symbol,
            side=Side.BUY,
            quantity=ctx.long_quantity,
            price=ctx.long_price_hint,
        )
    return maker_req, hedge_req


def build_open_position(
    ctx: EntryContext,
    maker_fill: OrderFill,
    hedge_fill: OrderFill,
    now_ms: int,
) -> OpenPosition:
    """Build an OpenPosition from completed entry fills."""
    matched_qty = min(maker_fill.quantity, hedge_fill.quantity)
    return OpenPosition(
        position_id=ctx.entry_id,
        symbol=ctx.symbol,
        long_venue=ctx.long_venue,
        short_venue=ctx.short_venue,
        long_quantity=matched_qty if ctx.maker_leg == Side.BUY else matched_qty,
        short_quantity=matched_qty if ctx.maker_leg == Side.SELL else matched_qty,
        long_entry_price=maker_fill.price if ctx.maker_leg == Side.BUY else hedge_fill.price,
        short_entry_price=hedge_fill.price if ctx.maker_leg == Side.BUY else maker_fill.price,
        opened_at_ms=now_ms,
        long_fill=maker_fill if ctx.maker_leg == Side.BUY else hedge_fill,
        short_fill=hedge_fill if ctx.maker_leg == Side.BUY else maker_fill,
    )
