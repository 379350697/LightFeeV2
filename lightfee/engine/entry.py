"""Entry execution state machine matching Rust V1 entry flow.

Rust references:
- src/execution_core/entry_sync.rs: PendingEntryHedge, state transitions
- src/engine/entry.rs: EntryAttemptOutcome, execute_entry_order_leg
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field, replace
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
    # V1 pair identity must survive into PendingEntry for L2-session ownership.
    pair_id: str = ""
    # --- V1 maker-event lane repricing ---
    parent_entry_id: Optional[str] = None
    reprice_action: str = ""
    # --- V1 planner output ---
    planned_route: ExecutionRoute = ExecutionRoute.PASSIVE_INCREMENTAL
    # --- V1 funding lifecycle semantics selected with the candidate ---
    opportunity_type: str = "aligned"
    funding_timestamp_ms: int = 0
    first_funding_timestamp_ms: int = 0
    long_funding_timestamp_ms: int = 0
    short_funding_timestamp_ms: int = 0
    second_funding_timestamp_ms: int = 0
    first_funding_leg: str = ""
    funding_edge_bps_entry: float = 0.0
    total_funding_edge_bps_entry: float = 0.0
    expected_edge_bps_entry: float = 0.0
    worst_case_edge_bps_entry: float = 0.0
    entry_maker_leg: str = ""
    exit_maker_leg: str = ""
    entry_cross_bps_entry: float = 0.0
    fee_bps_entry: float = 0.0
    entry_slippage_bps_entry: float = 0.0
    transfer_bias_bps_entry: float = 0.0
    transfer_state_at_entry: str | None = None
    entry_liquidity_source_at_entry: str | None = None
    long_volume_24h_quote_at_entry: float = 0.0
    short_volume_24h_quote_at_entry: float = 0.0
    long_open_interest_quote_at_entry: float = 0.0
    short_open_interest_quote_at_entry: float = 0.0
    long_entry_vwap: float | None = None
    short_entry_vwap: float | None = None
    entry_capacity_constrained: bool = False
    entry_target_quantity: float = 0.0
    long_max_executable_quantity: float = 0.0
    short_max_executable_quantity: float = 0.0
    entry_max_executable_quantity: float = 0.0
    entry_depth_shortfall_quantity: float = 0.0
    entry_max_executable_notional_quote: float = 0.0
    entry_depth_capped_at_entry: bool = False
    advisories: list[str] = field(default_factory=list)
    blocked_reasons: list[str] = field(default_factory=list)
    exit_after_first_stage: bool = False


def normalize_opportunity_type(value: str | None) -> str:
    """Map legacy/non-stage labels onto V1 close-stage labels."""
    return "staggered" if value == "staggered" else "aligned"


def _positive_int(value: object) -> int:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def _first_positive(values: list[object]) -> int:
    for value in values:
        parsed = _positive_int(value)
        if parsed > 0:
            return parsed
    return 0


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
    - Maker: GTC post-only for passive entries, IOC for taker entries
    - Hedge: IOC reduce_only=False (hedge is opening, not closing)
    - Both legs carry exchange-legal clientOrderId (decoupled from internal entry_id)
    - Maker reduce_only must be False (maker is the opening leg, not closing)
    """
    from lightfee.core.domain import TimeInForce
    from lightfee.venues.cid import generate_exchange_cid

    maker_venue = ctx.long_venue if ctx.maker_leg == Side.BUY else ctx.short_venue
    hedge_venue = ctx.short_venue if ctx.maker_leg == Side.BUY else ctx.long_venue

    maker_cid = generate_exchange_cid(ctx.entry_id, "m", maker_venue)
    hedge_cid = generate_exchange_cid(ctx.entry_id, "h", hedge_venue)
    is_passive = ctx.entry_type in (EntryType.PASSIVE_INCREMENTAL, EntryType.PASSIVE_FALLBACK)

    if ctx.maker_leg == Side.BUY:
        maker_req = OrderRequest(
            venue=ctx.long_venue,
            symbol=ctx.symbol,
            side=Side.BUY,
            quantity=ctx.long_quantity,
            price=ctx.long_price_hint,
            post_only=is_passive,
            time_in_force=TimeInForce.GTC if is_passive else TimeInForce.IOC,
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
            post_only=is_passive,
            time_in_force=TimeInForce.GTC if is_passive else TimeInForce.IOC,
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

    long_entry_fee_quote = (
        float(long_fill.fee_quote or 0.0) * (matched_qty / long_fill.quantity)
        if long_fill.quantity > 0.0
        else 0.0
    )
    short_entry_fee_quote = (
        float(short_fill.fee_quote or 0.0) * (matched_qty / short_fill.quantity)
        if short_fill.quantity > 0.0
        else 0.0
    )
    total_entry_fee_quote = long_entry_fee_quote + short_entry_fee_quote
    entry_notional_quote = (
        matched_qty * (long_entry_price + short_entry_price) * 0.5
        if matched_qty > 0.0 and long_entry_price > 0.0 and short_entry_price > 0.0
        else 0.0
    )

    long_funding_timestamp_ms = _positive_int(ctx.long_funding_timestamp_ms)
    short_funding_timestamp_ms = _positive_int(ctx.short_funding_timestamp_ms)
    inferred_first_funding_ms = _first_positive(
        [
            ctx.funding_timestamp_ms,
            ctx.first_funding_timestamp_ms,
            min(
                ts for ts in (long_funding_timestamp_ms, short_funding_timestamp_ms)
                if ts > 0
            ) if long_funding_timestamp_ms > 0 or short_funding_timestamp_ms > 0 else 0,
        ]
    )
    inferred_second_funding_ms = _positive_int(ctx.second_funding_timestamp_ms)
    if inferred_second_funding_ms <= 0 and long_funding_timestamp_ms > 0 and short_funding_timestamp_ms > 0:
        later_funding_ms = max(long_funding_timestamp_ms, short_funding_timestamp_ms)
        if later_funding_ms > inferred_first_funding_ms:
            inferred_second_funding_ms = later_funding_ms
    opportunity_type = normalize_opportunity_type(ctx.opportunity_type)
    second_stage_enabled = (
        opportunity_type == "staggered"
        and inferred_first_funding_ms > 0
        and inferred_second_funding_ms > inferred_first_funding_ms
    )

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
        entry_notional_quote=entry_notional_quote,
        matched_quantity=matched_qty,
        initial_quantity=matched_qty,
        entered_at_ms=max(maker_fill.filled_at_ms or 0, hedge_fill.filled_at_ms or 0),
        review_id=review_id,
        long_fill=long_fill,
        short_fill=short_fill,
        long_entry_fee_quote=long_entry_fee_quote,
        short_entry_fee_quote=short_entry_fee_quote,
        total_entry_fee_quote=total_entry_fee_quote,
        entry_fee_evidence_complete=True,
        current_net_quote=-total_entry_fee_quote,
        peak_net_quote=-total_entry_fee_quote,
        funding_timestamp_ms=inferred_first_funding_ms,
        long_funding_timestamp_ms=long_funding_timestamp_ms,
        short_funding_timestamp_ms=short_funding_timestamp_ms,
        second_funding_timestamp_ms=inferred_second_funding_ms,
        opportunity_type=opportunity_type,
        second_stage_enabled_at_entry=second_stage_enabled,
        exit_after_first_stage=bool(ctx.exit_after_first_stage),
        funding_edge_bps_entry=float(ctx.funding_edge_bps_entry or 0.0),
        total_funding_edge_bps_entry=float(
            ctx.total_funding_edge_bps_entry or ctx.funding_edge_bps_entry or 0.0
        ),
        expected_edge_bps_entry=float(ctx.expected_edge_bps_entry or 0.0),
        worst_case_edge_bps_entry=float(ctx.worst_case_edge_bps_entry or 0.0),
        first_funding_leg=str(ctx.first_funding_leg or ""),
        entry_maker_leg=str(ctx.entry_maker_leg or ""),
        exit_maker_leg=str(ctx.exit_maker_leg or ""),
        entry_cross_bps_entry=float(ctx.entry_cross_bps_entry or 0.0),
        fee_bps_entry=float(ctx.fee_bps_entry or 0.0),
        entry_slippage_bps_entry=float(ctx.entry_slippage_bps_entry or 0.0),
        transfer_bias_bps_entry=float(ctx.transfer_bias_bps_entry or 0.0),
        transfer_state_at_entry=ctx.transfer_state_at_entry,
        entry_liquidity_source_at_entry=ctx.entry_liquidity_source_at_entry,
        long_volume_24h_quote_at_entry=float(ctx.long_volume_24h_quote_at_entry or 0.0),
        short_volume_24h_quote_at_entry=float(ctx.short_volume_24h_quote_at_entry or 0.0),
        long_open_interest_quote_at_entry=float(ctx.long_open_interest_quote_at_entry or 0.0),
        short_open_interest_quote_at_entry=float(ctx.short_open_interest_quote_at_entry or 0.0),
        long_entry_vwap=ctx.long_entry_vwap,
        short_entry_vwap=ctx.short_entry_vwap,
        entry_capacity_constrained=bool(ctx.entry_capacity_constrained),
        entry_target_quantity=float(ctx.entry_target_quantity or 0.0),
        long_max_executable_quantity=float(ctx.long_max_executable_quantity or 0.0),
        short_max_executable_quantity=float(ctx.short_max_executable_quantity or 0.0),
        entry_max_executable_quantity=float(ctx.entry_max_executable_quantity or 0.0),
        entry_depth_shortfall_quantity=float(ctx.entry_depth_shortfall_quantity or 0.0),
        entry_max_executable_notional_quote=float(
            ctx.entry_max_executable_notional_quote or 0.0
        ),
        entry_depth_capped_at_entry=bool(ctx.entry_depth_capped_at_entry),
        advisories=list(ctx.advisories),
        blocked_reasons=list(ctx.blocked_reasons),
        entry_quality_completed_at_ms=0,
    )
