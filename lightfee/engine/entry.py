"""Entry execution state machine matching Rust V1 entry flow.

Rust references:
- src/execution_core/entry_sync.rs: PendingEntryHedge, state transitions
- src/engine/entry.rs: EntryAttemptOutcome, execute_entry_order_leg
"""

from __future__ import annotations

import uuid
from copy import deepcopy
from dataclasses import dataclass, field, replace
from enum import Enum
from math import isfinite
from typing import Optional

from lightfee.core.domain import OrderFill, OrderRequest, Side, Venue
from lightfee.engine.execution_planner import ExecutionRoute
from lightfee.engine.exit import (
    EXECUTION_BENCHMARK_MAX_OBSERVATION_TO_SUBMIT_MS,
    execution_benchmark_receipt_semantically_verified,
    seal_execution_benchmark_receipt,
)
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
    # Entry-time ES is retained for later portfolio admission.  Re-estimating
    # an existing position with a new candidate's volatility is unsafe.
    expected_shortfall_bps_entry: float = 0.0
    calculation_version: str = "v1_exact"
    model_epoch: str = "v1_exact"
    economics_observed_at_ms: int = 0
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
    # Optional raw L2 receipt captured immediately before paired entry order
    # submission.  Price hints are routing inputs, never execution evidence.
    entry_execution_benchmark_receipt: dict[str, object] | None = None
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


def _finite_positive_float(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if isfinite(parsed) and parsed > 0.0 else None


def _execution_fee_observed(value: object) -> bool:
    """Return whether a fill carries a finite quote-fee observation.

    A missing fee preserves V1's numeric zero fallback for lifecycle
    accounting, but it is not evidence that the fee was actually zero.
    """
    if value is None or isinstance(value, bool):
        return False
    try:
        return isfinite(float(value))
    except (TypeError, ValueError, OverflowError):
        return False


def _first_positive(values: list[object]) -> int:
    for value in values:
        parsed = _positive_int(value)
        if parsed > 0:
            return parsed
    return 0


def _receipt_positive_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed > 0 else None


def _receipt_nonnegative_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed >= 0 else None


def _receipt_finite_positive(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if isfinite(parsed) and parsed > 0.0 else None


def _receipt_finite(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if isfinite(parsed) else None


def finalize_entry_execution_benchmark_receipt(
    raw_receipt: object,
    *,
    position_id: str,
    symbol: str,
    long_venue: Venue,
    short_venue: Venue,
    long_fill: OrderFill,
    short_fill: OrderFill,
    long_client_order_id: str,
    short_client_order_id: str,
    long_submitted_at_ms: int,
    short_submitted_at_ms: int,
) -> dict[str, object] | None:
    """Seal a pre-submit L2 capture with the two actual entry fills.

    The dispatch runtime supplies only a raw, immutable book observation.  It
    cannot know fill IDs, prices, or times before placing either order.  This
    function is the sole bridge from that observation to promotion evidence:
    it rejects a delayed/partial/misrouted fill rather than fabricating a
    benchmark.  It has no order-routing effect; ``None`` means unavailable
    execution-quality attribution while the V1 position lifecycle continues.
    """
    if not isinstance(raw_receipt, dict):
        return None
    if (
        raw_receipt.get("source") != "local_l2_vwap"
        or raw_receipt.get("position_id") != position_id
        or raw_receipt.get("symbol") != symbol
        # A final receipt must never be used as a new raw observation.
        or "integrity" in raw_receipt
        or "receipt_digest" in raw_receipt
    ):
        return None

    captured_at_ms = _receipt_positive_int(raw_receipt.get("captured_at_ms"))
    max_delay_ms = _receipt_positive_int(
        raw_receipt.get("max_observation_to_submit_ms")
    )
    requested_quantity = _receipt_finite_positive(
        raw_receipt.get("requested_base_quantity")
    )
    if (
        captured_at_ms is None
        or max_delay_ms != EXECUTION_BENCHMARK_MAX_OBSERVATION_TO_SUBMIT_MS
        or requested_quantity is None
    ):
        return None

    quantity_tolerance = max(1e-10, requested_quantity * 1e-8)
    leg_inputs = {
        "long": (long_venue, Side.BUY, long_fill, long_client_order_id, long_submitted_at_ms),
        "short": (short_venue, Side.SELL, short_fill, short_client_order_id, short_submitted_at_ms),
    }
    completed: dict[str, dict[str, object]] = {}
    total_shortfall = 0.0

    for name, (venue, side, fill, client_order_id, submitted_at_ms) in leg_inputs.items():
        raw_leg = raw_receipt.get(name)
        if not isinstance(raw_leg, dict):
            return None
        vwap_price = _receipt_finite_positive(raw_leg.get("vwap_price"))
        available_quantity = _receipt_finite_positive(
            raw_leg.get("available_base_quantity")
        )
        observed_at_ms = _receipt_positive_int(raw_leg.get("observed_at_ms"))
        age_ms = _receipt_nonnegative_int(raw_leg.get("age_ms"))
        submitted = _receipt_positive_int(submitted_at_ms)
        filled_at_ms = _receipt_positive_int(fill.filled_at_ms)
        fill_quantity = _receipt_finite_positive(fill.quantity)
        fill_price = _receipt_finite_positive(fill.price)
        fee_quote = _receipt_finite(fill.fee_quote)
        if (
            raw_leg.get("venue") != venue.value
            or raw_leg.get("side") != side.value
            or vwap_price is None
            or available_quantity is None
            or available_quantity + quantity_tolerance < requested_quantity
            or observed_at_ms is None
            or age_ms is None
            or observed_at_ms > captured_at_ms
            or captured_at_ms - observed_at_ms != age_ms
            or submitted is None
            or filled_at_ms is None
            or captured_at_ms > submitted
            or submitted > filled_at_ms
            or submitted - observed_at_ms > max_delay_ms
            or fill.venue != venue
            or fill.symbol != symbol
            or fill.side != side
            or fill_quantity is None
            or abs(fill_quantity - requested_quantity) > quantity_tolerance
            or fill_price is None
            or not str(fill.order_id or "")
            or not str(client_order_id or "")
        ):
            return None
        adverse_move = (
            fill_price - vwap_price
            if side == Side.BUY
            else vwap_price - fill_price
        )
        leg_shortfall = max(adverse_move, 0.0) * fill_quantity
        total_shortfall += leg_shortfall
        completed[name] = {
            "venue": venue.value,
            "side": side.value,
            "vwap_price": vwap_price,
            "available_base_quantity": available_quantity,
            "observed_at_ms": observed_at_ms,
            "age_ms": age_ms,
            "filled_base_quantity": fill_quantity,
            "implementation_shortfall_quote": leg_shortfall,
            "fills": [
                {
                    "order_id": str(fill.order_id or ""),
                    "client_order_id": str(client_order_id),
                    "submitted_at_ms": submitted,
                    "filled_at_ms": filled_at_ms,
                    "quantity": fill_quantity,
                    "price": fill_price,
                    **({"fee_quote": fee_quote} if fee_quote is not None else {}),
                }
            ],
        }

    sealed = seal_execution_benchmark_receipt(
        {
            "source": "local_l2_vwap",
            "position_id": position_id,
            "symbol": symbol,
            "captured_at_ms": captured_at_ms,
            "max_observation_to_submit_ms": max_delay_ms,
            "requested_base_quantity": requested_quantity,
            "long": completed["long"],
            "short": completed["short"],
            "implementation_shortfall_quote": total_shortfall,
        }
    )
    if not execution_benchmark_receipt_semantically_verified(
        sealed,
        position_id=position_id,
        symbol=symbol,
        expected_legs={
            "long": (long_venue.value, Side.BUY.value),
            "short": (short_venue.value, Side.SELL.value),
        },
    ):
        return None
    return sealed


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
    execution_fee_complete = _execution_fee_observed(
        long_fill.fee_quote
    ) and _execution_fee_observed(short_fill.fee_quote)
    entry_notional_quote = (
        matched_qty * (long_entry_price + short_entry_price) * 0.5
        if matched_qty > 0.0 and long_entry_price > 0.0 and short_entry_price > 0.0
        else 0.0
    )
    entry_receipt = ctx.entry_execution_benchmark_receipt
    entry_benchmark_complete = execution_benchmark_receipt_semantically_verified(
        entry_receipt,
        position_id=ctx.entry_id,
        symbol=ctx.symbol,
        expected_legs={
            "long": (ctx.long_venue.value, "buy"),
            "short": (ctx.short_venue.value, "sell"),
        },
    )
    # A BBO/IOC price hint may be stale, capacity-unaware, or supplied by a
    # caller without any paired L2 capture.  It therefore cannot become an
    # entry benchmark or manufacture a zero shortfall.  Until the runtime
    # supplies a sealed receipt, the only correct value is unavailable.
    long_benchmark_price = 0.0
    short_benchmark_price = 0.0
    entry_implementation_shortfall_quote = 0.0
    if entry_benchmark_complete:
        assert isinstance(entry_receipt, dict)
        long_benchmark_price = float(entry_receipt["long"]["vwap_price"])
        short_benchmark_price = float(entry_receipt["short"]["vwap_price"])
        entry_implementation_shortfall_quote = float(
            entry_receipt["implementation_shortfall_quote"]
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
        # V1 only holds a staggered position through the second settlement
        # when that incremental carry is beneficial and the entry explicitly
        # selected evaluate-second-stage.  A timestamp alone is not consent to
        # hold a negative second leg.
        and not bool(ctx.exit_after_first_stage)
        and (
            float(ctx.total_funding_edge_bps_entry or 0.0)
            - float(ctx.funding_edge_bps_entry or 0.0)
        ) > 0.0
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
        entry_benchmark_long_price=(
            long_benchmark_price if entry_benchmark_complete else 0.0
        ),
        entry_benchmark_short_price=(
            short_benchmark_price if entry_benchmark_complete else 0.0
        ),
        entry_implementation_shortfall_quote=entry_implementation_shortfall_quote,
        entry_execution_benchmark_receipt=(
            deepcopy(entry_receipt)
            if entry_benchmark_complete and isinstance(entry_receipt, dict)
            else None
        ),
        execution_fee_complete=execution_fee_complete,
        execution_benchmark_complete=entry_benchmark_complete,
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
        expected_shortfall_bps_entry=float(ctx.expected_shortfall_bps_entry or 0.0),
        calculation_version=str(ctx.calculation_version or "v1_exact"),
        model_epoch=str(ctx.model_epoch or ctx.calculation_version or "v1_exact"),
        economics_observed_at_ms=_positive_int(ctx.economics_observed_at_ms),
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
