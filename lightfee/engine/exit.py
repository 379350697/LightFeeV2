"""Exit execution state machine matching Rust exit flow."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from lightfee.core.domain import OrderFill, OrderRequest, Side, Venue
from lightfee.engine.state import OpenPosition


class ExitReason(Enum):
    PROFIT_TAKE = "profit_take"
    NET_STOP_LOSS = "net_stop_loss"
    TRAILING_EXIT = "trailing_exit"
    FIRST_STAGE_CAPTURE = "first_stage_capture"
    SECOND_STAGE_CAPTURE = "second_stage_capture"
    FUNDING_CAPTURE = "funding_capture"
    SETTLEMENT_FORCE_CLOSE = "settlement_force_close"
    MARK_PRICE_HARD_STOP = "mark_price_hard_stop"
    RISK_DEATH = "risk_death"
    RISK_DELEVER = "risk_delever"


class CloseState(Enum):
    IDLE = "idle"
    CLOSING_LONG = "closing_long"
    CLOSING_SHORT = "closing_short"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class CloseExecution:
    position_id: str
    reason: str
    long_close_price: float
    short_close_price: float
    long_close_qty: float
    short_close_qty: float
    long_fee_quote: float = 0.0
    short_fee_quote: float = 0.0
    realized_price_pnl_quote: float = 0.0
    funding_pnl_quote: float = 0.0
    net_quote: float = 0.0
    # None means the benchmark is unavailable; zero is a verified
    # no-adverse-fill result.  Keep this separate from price PnL.
    implementation_shortfall_quote: float | None = None


def build_reduce_only_close_orders(
    position: OpenPosition,
    reason: ExitReason,
) -> tuple[OrderRequest, OrderRequest]:
    """Build reduce-only close orders for both legs."""
    long_close = OrderRequest(
        venue=position.long_venue,
        symbol=position.symbol,
        side=Side.SELL,
        quantity=abs(position.long_quantity),
        reduce_only=True,
    )
    short_close = OrderRequest(
        venue=position.short_venue,
        symbol=position.symbol,
        side=Side.BUY,
        quantity=abs(position.short_quantity),
        reduce_only=True,
    )
    return long_close, short_close


def compute_close_pnl(
    position: OpenPosition,
    long_fill: OrderFill,
    short_fill: OrderFill,
) -> CloseExecution:
    """Compute PnL attribution for a close execution."""
    matched_qty = min(long_fill.quantity, short_fill.quantity)
    realized_pnl = (
        (long_fill.price - position.long_entry_price) * matched_qty
        + (position.short_entry_price - short_fill.price) * matched_qty
    )
    long_fee = long_fill.fee_quote or 0.0
    short_fee = short_fill.fee_quote or 0.0
    return CloseExecution(
        position_id=position.position_id,
        reason="manual",
        long_close_price=long_fill.price,
        short_close_price=short_fill.price,
        long_close_qty=long_fill.quantity,
        short_close_qty=short_fill.quantity,
        long_fee_quote=long_fee,
        short_fee_quote=short_fee,
        realized_price_pnl_quote=realized_pnl,
        net_quote=realized_pnl - long_fee - short_fee,
    )
