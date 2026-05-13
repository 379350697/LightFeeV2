"""V1 close executor: reduce-only chunked close execution.

Rust references:
- src/engine/exit.rs: execute_aggressive_close_orders (line 3335)
- src/engine/exit.rs: build_close_execution_from_legs (line 1155)
- src/engine/exit.rs: close_position_exchange_min_notional_violation (line 3067)
- src/engine/exit.rs: close_leg_exchange_min_notional_violation (line 3035)
- src/engine/exit.rs: finalize_close_position_execution (line 4896)
- src/execution_core/helpers.rs: close_balance_from_closed_quantities (line 181)
- src/execution_core/residual.rs: split_close_fill_residual (line 75)
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass, field
from typing import Any, Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import OrderFill, OrderRequest, Side, Venue
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.engine.exit import CloseExecution
from lightfee.engine.residual import ResidualExposureTask, ResidualOrigin, approx_eq
from lightfee.engine.state import CloseLegRecord, OpenPosition, PendingClose
from lightfee.persistence.journal import Journal
from lightfee.venues.common import venue_reduce_only_close_exempts_min_notional


# ---------------------------------------------------------------------------
# Close execution leg
# ---------------------------------------------------------------------------


@dataclass
class CloseExecutionLeg:
    """V1 CloseExecutionLeg: single filled close order."""
    fill: OrderFill
    client_order_id: str = ""
    submit_started_at_ms: int = 0
    latency_ms: int = 0


# ---------------------------------------------------------------------------
# Close balance (matched quantities after close)
# ---------------------------------------------------------------------------


@dataclass
class CloseBalance:
    """V1 CloseBalance (helpers.rs line 174): matched remaining after close."""
    matched_closed_quantity: float
    matched_remaining_quantity: float
    long_remaining_quantity: float
    short_remaining_quantity: float


def close_balance_from_closed_quantities(
    position_quantity: float,
    long_closed_quantity: float,
    short_closed_quantity: float,
) -> CloseBalance:
    """V1 close_balance_from_closed_quantities (helpers.rs line 181).

    matched_remaining = min(long_remaining, short_remaining)
    — the leg that closed LESS determines the matched remaining.
    """
    long_remaining = max(position_quantity - long_closed_quantity, 0.0)
    short_remaining = max(position_quantity - short_closed_quantity, 0.0)
    matched_remaining = min(long_remaining, short_remaining)
    matched_closed = max(position_quantity - matched_remaining, 0.0)
    return CloseBalance(
        matched_closed_quantity=matched_closed,
        matched_remaining_quantity=matched_remaining,
        long_remaining_quantity=long_remaining,
        short_remaining_quantity=short_remaining,
    )


# ---------------------------------------------------------------------------
# Build close execution from legs (PnL aggregation)
# ---------------------------------------------------------------------------


def build_close_execution_from_legs(
    position: OpenPosition,
    chunk_count: int,
    short_legs: list[CloseExecutionLeg],
    long_legs: list[CloseExecutionLeg],
) -> CloseExecution:
    """V1 build_close_execution_from_legs (exit.rs line 1155): aggregate PnL.

    Price PnL:
      long: (exit_price - entry_price) * quantity
      short: (entry_price - exit_price) * quantity
    Fee PnL: sum of all leg fees.
    """
    # Price PnL
    long_price_pnl = sum(
        (leg.fill.price - position.long_entry_price) * leg.fill.quantity
        for leg in long_legs
    )
    short_price_pnl = sum(
        (position.short_entry_price - leg.fill.price) * leg.fill.quantity
        for leg in short_legs
    )
    realized_price_pnl = long_price_pnl + short_price_pnl

    # Fees
    long_fee = sum(leg.fill.fee_quote or 0.0 for leg in long_legs)
    short_fee = sum(leg.fill.fee_quote or 0.0 for leg in short_legs)
    total_exit_fee = long_fee + short_fee

    # Quantities
    long_close_qty = sum(leg.fill.quantity for leg in long_legs)
    short_close_qty = sum(leg.fill.quantity for leg in short_legs)

    # Average prices
    long_avg = long_close_qty and sum(
        leg.fill.price * leg.fill.quantity for leg in long_legs
    ) / long_close_qty or 0.0
    short_avg = short_close_qty and sum(
        leg.fill.price * leg.fill.quantity for leg in short_legs
    ) / short_close_qty or 0.0

    return CloseExecution(
        position_id=position.position_id,
        reason="",  # set by caller
        long_close_price=long_avg,
        short_close_price=short_avg,
        long_close_qty=long_close_qty,
        short_close_qty=short_close_qty,
        long_fee_quote=long_fee,
        short_fee_quote=short_fee,
        realized_price_pnl_quote=realized_price_pnl,
        funding_pnl_quote=position.captured_funding_quote + position.second_stage_funding_quote,
        net_quote=(
            realized_price_pnl
            + position.captured_funding_quote
            + position.second_stage_funding_quote
            - total_exit_fee
        ),
    )


# ---------------------------------------------------------------------------
# Split close fill residual
# ---------------------------------------------------------------------------


def split_close_fill_residual(
    position: OpenPosition,
    long_closed_quantity: float,
    short_closed_quantity: float,
    now_ms: int,
    deadline_ms: int,
) -> Optional[ResidualExposureTask]:
    """V1 split_close_fill_residual (residual.rs line 75): detect asymmetric close.

    If long and short close quantities differ, create a ResidualExposureTask
    for the excess side.
    """
    balance = close_balance_from_closed_quantities(
        position.matched_quantity,
        long_closed_quantity,
        short_closed_quantity,
    )
    delta = balance.long_remaining_quantity - balance.short_remaining_quantity

    if approx_eq(delta, 0.0):
        return None

    if delta > 0:
        exposure_venue = position.long_venue
        exposure_side = Side.SELL
        exposure_qty = delta
    else:
        exposure_venue = position.short_venue
        exposure_side = Side.BUY
        exposure_qty = -delta

    return ResidualExposureTask(
        position_id=position.position_id,
        pair_id=f"{position.symbol.lower()}:{position.long_venue.value}->{position.short_venue.value}",
        symbol=position.symbol,
        long_venue=position.long_venue,
        short_venue=position.short_venue,
        origin=ResidualOrigin.CLOSE_RESIDUAL,
        exposure_venue=exposure_venue,
        exposure_side=exposure_side,
        exposure_quantity=exposure_qty,
        created_at_ms=now_ms,
        deadline_ms=deadline_ms,
    )


# ---------------------------------------------------------------------------
# Min notional dust check
# ---------------------------------------------------------------------------


def close_leg_exchange_min_notional_violation(
    venue: Venue,
    symbol: str,
    side: Side,
    quantity: float,
    reduce_only: bool,
    price_hint: float,
    min_notional_quote: float,
) -> Optional[tuple[Venue, float, float]]:
    """V1 close_leg_exchange_min_notional_violation (exit.rs line 3035).

    Returns None if quantity passes, or Some((venue, leg_notional, min_notional))
    if the leg notional is below min notional.
    """
    if quantity <= 0.0:
        return None
    if reduce_only and venue_reduce_only_close_exempts_min_notional(venue):
        return None

    leg_notional = quantity * price_hint
    if leg_notional + 1e-9 < min_notional_quote:
        return (venue, leg_notional, min_notional_quote)
    return None


def close_position_exchange_min_notional_violation(
    position: OpenPosition,
    quantity: float,
    long_price_hint: float,
    short_price_hint: float,
    min_long_notional: float,
    min_short_notional: float,
) -> Optional[tuple[Venue, float, float]]:
    """V1 close_position_exchange_min_notional_violation (exit.rs line 3067).

    Checks both legs (short-close = Buy at short_venue, long-close = Sell at long_venue).
    Returns the first violation found (short first, then long).
    """
    # Short close: Buy at short_venue
    violation = close_leg_exchange_min_notional_violation(
        position.short_venue, position.symbol, Side.BUY,
        quantity, reduce_only=True, price_hint=short_price_hint,
        min_notional_quote=min_short_notional,
    )
    if violation:
        return violation

    # Long close: Sell at long_venue
    violation = close_leg_exchange_min_notional_violation(
        position.long_venue, position.symbol, Side.SELL,
        quantity, reduce_only=True, price_hint=long_price_hint,
        min_notional_quote=min_long_notional,
    )
    return violation


# ---------------------------------------------------------------------------
# Close chunk planning (V1 semantic activation)
# ---------------------------------------------------------------------------


@dataclass
class ChunkPlan:
    chunk_quantities: list[float]
    total_chunks: int


def compute_close_chunks(
    total_quantity: float,
    long_price_hint: float,
    short_price_hint: float,
    max_notional_quote: float,
    min_long_notional: float = 0.0,
    min_short_notional: float = 0.0,
    venue_long: Venue | None = None,
    venue_short: Venue | None = None,
) -> list[float]:
    """V1 close chunk planning: split a large close into notional-capped chunks.

    Rust V1 reference: close_execution_chunks (risk.rs line 826) splits based
    on L2 liquidity depth. Python V2 uses a simpler notional-cap approach that
    achieves the same effect: no single chunk exceeds max_notional_quote on
    either leg.

    Returns empty list if total_quantity <= 0.
    Returns single-element list if max_notional_quote <= 0 or total notional
    is already below the cap.
    """
    if total_quantity <= 0.0:
        return []

    if max_notional_quote <= 0.0:
        return [total_quantity]

    # Use the more expensive leg to determine chunk count
    price_per_unit = max(long_price_hint, short_price_hint)
    if price_per_unit <= 0.0:
        return [total_quantity]

    total_notional = total_quantity * price_per_unit
    if total_notional <= max_notional_quote:
        return [total_quantity]

    # How many equal-sized chunks to stay under the cap
    num_chunks = int(math.ceil(total_notional / max_notional_quote))
    base_qty = total_quantity / num_chunks

    # Validate min notional if venue info provided
    if venue_long is not None and venue_short is not None:
        min_allowed = 0.0
        if min_long_notional > 0 and not venue_reduce_only_close_exempts_min_notional(venue_long):
            min_allowed = max(min_allowed, min_long_notional / long_price_hint if long_price_hint > 0 else 0.0)
        if min_short_notional > 0 and not venue_reduce_only_close_exempts_min_notional(venue_short):
            min_allowed = max(min_allowed, min_short_notional / short_price_hint if short_price_hint > 0 else 0.0)
        if min_allowed > 0 and base_qty < min_allowed:
            # Chunks would be too small — fall back to fewer, larger chunks
            max_chunk_qty = total_notional / max_notional_quote
            num_chunks = max(1, int(math.floor(total_quantity / min_allowed)))
            # But each chunk must still respect the notional cap
            max_qty_per_chunk = max_notional_quote / price_per_unit
            if num_chunks * max_qty_per_chunk < total_quantity:
                num_chunks = int(math.ceil(total_quantity / max_qty_per_chunk))
            base_qty = total_quantity / num_chunks

    chunks = []
    remaining = total_quantity
    for i in range(num_chunks):
        if i == num_chunks - 1:
            chunks.append(remaining)
        else:
            chunks.append(base_qty)
            remaining -= base_qty

    return chunks


# ---------------------------------------------------------------------------
# Close executor
# ---------------------------------------------------------------------------


@dataclass
class CloseExecConfig:
    deadline_ms: int = 30_000
    max_close_retries: int = 3
    post_funding_hold_ms: int = 30_000
    # V1 semantic alignment: large-close chunking (activated).
    # Splits closes above close_chunk_max_notional_quote into multiple chunks to
    # reduce market impact. Each chunk is submitted as an independent close leg
    # pair with its own clientOrderId and journal entries. Set to 0 to disable.
    close_chunk_max_notional_quote: float = 0.0  # 0 = no chunking
    close_chunk_min_interval_ms: int = 1_000


def _legs_to_records(legs: list[CloseExecutionLeg]) -> list[CloseLegRecord]:
    """Convert CloseExecutionLeg list to CloseLegRecord list for persistence.

    V1: close_leg_record() in exit.rs — maps filled leg data into
    serializable CloseLegRecord for PendingClose reconciliation.
    """
    records: list[CloseLegRecord] = []
    for leg in legs:
        ven_str = leg.fill.venue.value if hasattr(leg.fill.venue, 'value') else str(leg.fill.venue)
        records.append(CloseLegRecord(
            venue=ven_str,
            order_id=leg.fill.order_id,
            client_order_id=leg.client_order_id,
            quantity=leg.fill.quantity,
            average_price=leg.fill.average_price,
            fee_quote=leg.fill.fee_quote,
        ))
    return records


class CloseExecutor:
    """Async close executor using venue adapters.

    Submits reduce-only close orders (short=Bid, long=Ask), handles
    rejected/uncertain/fill outcomes, builds CloseExecution, and
    detects residual.
    """

    def __init__(
        self,
        adapters: dict[Venue, VenueAdapter],
        journal: Journal,
        config_overrides: dict[str, Any] | None = None,
    ):
        self.adapters = adapters
        self.journal = journal
        overrides = config_overrides or {}
        self.config = CloseExecConfig(
            deadline_ms=overrides.get("deadline_ms", 30_000),
            max_close_retries=overrides.get("max_close_retries", 3),
            post_funding_hold_ms=overrides.get("post_funding_hold_ms", 30_000),
            close_chunk_max_notional_quote=overrides.get("close_chunk_max_notional_quote", 0.0),
            close_chunk_min_interval_ms=overrides.get("close_chunk_min_interval_ms", 1_000),
        )

    async def execute_close(
        self,
        position: OpenPosition,
        reason: str,
        now_ms: int,
        long_price_hint: float = 0.0,
        short_price_hint: float = 0.0,
        total_quantity: float | None = None,
        state: Any | None = None,
    ) -> CloseExecution:
        """Execute a full close for an open position.

        Submits both legs (short=buy, long=sell) as reduce-only IOC taker orders.
        When *state* is provided, writes back PnL attribution, matched quantity
        updates, and manages PendingClose lifecycle for uncertain outcomes.

        V1 parity additions:
        - IOC time_in_force for close orders
        - Deterministic clientOrderId for idempotency
        - Retry throttling with exponential backoff on each leg
        """
        from lightfee.core.domain import TimeInForce
        from lightfee.engine.exit import CloseExecution as CE

        if total_quantity is None:
            total_quantity = position.matched_quantity
        if total_quantity <= 0:
            return CE(position_id=position.position_id, reason=reason,
                      long_close_price=0.0, short_close_price=0.0,
                      long_close_qty=0.0, short_close_qty=0.0)

        close_id = f"close-{position.position_id}-{now_ms}"

        # V1 close chunk planning: split large closes by notional cap
        chunk_quantities = compute_close_chunks(
            total_quantity=total_quantity,
            long_price_hint=long_price_hint,
            short_price_hint=short_price_hint,
            max_notional_quote=self.config.close_chunk_max_notional_quote,
            min_long_notional=0.0,
            min_short_notional=0.0,
            venue_long=position.long_venue,
            venue_short=position.short_venue,
        )
        total_chunks = len(chunk_quantities)
        if total_chunks == 0:
            return CE(position_id=position.position_id, reason=reason,
                      long_close_price=0.0, short_close_price=0.0,
                      long_close_qty=0.0, short_close_qty=0.0)

        short_legs: list[CloseExecutionLeg] = []
        long_legs: list[CloseExecutionLeg] = []
        chunk_short_cids: list[str] = []
        chunk_long_cids: list[str] = []
        chunk_short_order_ids: list[str] = []
        chunk_long_order_ids: list[str] = []
        any_short_uncertain = False
        any_long_uncertain = False

        for chunk_idx, chunk_qty in enumerate(chunk_quantities):
            chunk_suffix = f"_chunk_{chunk_idx + 1}" if total_chunks > 1 else ""
            chunk_start_ms = now_ms

            short_cid = f"{close_id}-short{chunk_suffix}"
            long_cid = f"{close_id}-long{chunk_suffix}"

            # Submit short close first (V1: short leg first per chunk)
            short_req = OrderRequest(
                venue=position.short_venue, symbol=position.symbol,
                side=Side.BUY, quantity=chunk_qty, reduce_only=True,
                price=short_price_hint or None,
                time_in_force=TimeInForce.IOC,
                client_order_id=short_cid,
            )
            short_result = await self._submit_close_leg_with_retry(
                short_req, position.position_id, "short", now_ms,
            )
            if short_result["outcome"] == "filled":
                short_legs.append(CloseExecutionLeg(
                    fill=short_result["fill"],
                    client_order_id=short_cid,
                    submit_started_at_ms=chunk_start_ms,
                ))
                chunk_short_order_ids.append(short_result["fill"].order_id)
            elif short_result["outcome"] == "uncertain":
                any_short_uncertain = True
                chunk_short_order_ids.append(short_result.get("order_id", ""))

            long_req = OrderRequest(
                venue=position.long_venue, symbol=position.symbol,
                side=Side.SELL, quantity=chunk_qty, reduce_only=True,
                price=long_price_hint or None,
                time_in_force=TimeInForce.IOC,
                client_order_id=long_cid,
            )
            long_result = await self._submit_close_leg_with_retry(
                long_req, position.position_id, "long", now_ms,
            )
            if long_result["outcome"] == "filled":
                long_legs.append(CloseExecutionLeg(
                    fill=long_result["fill"],
                    client_order_id=long_cid,
                    submit_started_at_ms=chunk_start_ms,
                ))
                chunk_long_order_ids.append(long_result["fill"].order_id)
            elif long_result["outcome"] == "uncertain":
                any_long_uncertain = True
                chunk_long_order_ids.append(long_result.get("order_id", ""))

            chunk_short_cids.append(short_cid)
            chunk_long_cids.append(long_cid)

            self.journal.append(
                "exit.close_chunk_submitted",
                {
                    "position_id": position.position_id,
                    "chunk_index": chunk_idx,
                    "total_chunks": total_chunks,
                    "chunk_quantity": chunk_qty,
                    "short_client_order_id": short_cid,
                    "long_client_order_id": long_cid,
                    "short_outcome": short_result["outcome"],
                    "long_outcome": long_result["outcome"],
                },
            )

            # Inter-chunk delay (skip after last chunk)
            if chunk_idx < total_chunks - 1 and self.config.close_chunk_min_interval_ms > 0:
                delay_s = self.config.close_chunk_min_interval_ms / 1000.0
                await asyncio.sleep(delay_s)

        # Aggregate PnL across all chunks
        close = build_close_execution_from_legs(
            position, total_chunks, short_legs, long_legs,
        )
        close.reason = reason

        # Detect residual from total closed quantities
        long_closed = sum(leg.fill.quantity for leg in long_legs)
        short_closed = sum(leg.fill.quantity for leg in short_legs)
        residual = split_close_fill_residual(
            position, long_closed, short_closed,
            now_ms, now_ms + self.config.deadline_ms,
        )
        if residual:
            self.journal.append(
                "exit.close_residual_detected",
                {
                    "position_id": position.position_id,
                    "exposure_quantity": residual.exposure_quantity,
                    "exposure_venue": residual.exposure_venue.value,
                    "close_id": close_id,
                    "chunk_count": total_chunks,
                },
            )

        # Write back to EngineState when available
        if state is not None:
            self._writeback_to_state(
                state, position, close, long_closed, short_closed,
                any_long_uncertain, any_short_uncertain, now_ms, reason,
                close_id=close_id,
                short_order_id=", ".join(chunk_short_order_ids),
                long_order_id=", ".join(chunk_long_order_ids),
                short_client_order_id=", ".join(chunk_short_cids),
                long_client_order_id=", ".join(chunk_long_cids),
                chunk_count=total_chunks,
                long_legs=long_legs,
                short_legs=short_legs,
            )

        pnl_attr = build_exit_pnl_attribution(position, close)

        self.journal.append(
            "exit.closed",
            {
                "position_id": position.position_id,
                "reason": reason,
                "long_closed_qty": long_closed,
                "short_closed_qty": short_closed,
                "long_uncertain": any_long_uncertain,
                "short_uncertain": any_short_uncertain,
                "price_pnl": pnl_attr["price_pnl_quote"],
                "funding_pnl_quote": pnl_attr["funding_quote"],
                "entry_fee_quote": pnl_attr["entry_fee_quote"],
                "exit_fee_quote": pnl_attr["exit_fee_quote"],
                "net_quote": pnl_attr["net_quote"],
                "close_id": close_id,
                "chunk_count": total_chunks,
                "long_client_order_id": ", ".join(chunk_long_cids),
                "short_client_order_id": ", ".join(chunk_short_cids),
            },
        )

        return close

    def _writeback_to_state(
        self,
        state: Any,
        position: OpenPosition,
        close: CloseExecution,
        long_closed: float,
        short_closed: float,
        long_uncertain: bool,
        short_uncertain: bool,
        now_ms: int,
        reason: str,
        close_id: str = "",
        short_order_id: str = "",
        long_order_id: str = "",
        short_client_order_id: str = "",
        long_client_order_id: str = "",
        chunk_count: int = 1,
        long_legs: list[CloseExecutionLeg] | None = None,
        short_legs: list[CloseExecutionLeg] | None = None,
    ) -> None:
        """Write close execution results back into EngineState.

        - Updates position PnL / quantity tracking
        - Creates PendingClose for uncertain legs with clientOrderId for idempotency
        - Removes fully-closed positions from open_positions
        - Tracks chunk_index / total_chunks for multi-chunk closes
        """
        # Track partial close: update quantities
        matched_closed = min(long_closed, short_closed)
        position.matched_quantity = max(position.matched_quantity - matched_closed, 0.0)
        position.long_quantity = max(position.long_quantity - long_closed, 0.0)
        position.short_quantity = max(position.short_quantity - short_closed, 0.0)
        position.realized_price_pnl_quote += close.realized_price_pnl_quote
        position.realized_exit_fee_quote += close.long_fee_quote + close.short_fee_quote
        position.current_net_quote += close.net_quote

        # If any leg is uncertain, register a PendingClose for reconciliation
        if long_uncertain or short_uncertain:
            if not close_id:
                close_id = f"pending-close-{position.position_id}-{now_ms}"
            state.pending_closes[close_id] = PendingClose(
                close_id=close_id,
                position_id=position.position_id,
                reason=reason,
                created_at_ms=now_ms,
                long_order_id=long_order_id,
                short_order_id=short_order_id,
                long_client_order_id=long_client_order_id,
                short_client_order_id=short_client_order_id,
                long_closed=long_closed,
                short_closed=short_closed,
                long_uncertain=long_uncertain,
                short_uncertain=short_uncertain,
                chunk_index=0,
                total_chunks=chunk_count,
                long_legs=_legs_to_records(long_legs or []),
                short_legs=_legs_to_records(short_legs or []),
            )
            self.journal.append(
                "exit.pending_close_registered",
                {
                    "close_id": close_id,
                    "position_id": position.position_id,
                    "long_uncertain": long_uncertain,
                    "short_uncertain": short_uncertain,
                    "long_client_order_id": long_client_order_id,
                    "short_client_order_id": short_client_order_id,
                    "chunk_count": chunk_count,
                },
            )

        # Emit partial close when position remains open
        if position.matched_quantity > 1e-12:
            self.journal.append(
                "exit.partial_closed",
                {
                    "position_id": position.position_id,
                    "quantity": position.matched_quantity,
                    "long_quantity": position.long_quantity,
                    "short_quantity": position.short_quantity,
                    "current_net_quote": position.current_net_quote,
                    "peak_net_quote": position.peak_net_quote,
                    "funding_captured": position.funding_captured,
                    "second_stage_funding_captured": position.second_stage_funding_captured,
                    "long_closed_qty": long_closed,
                    "short_closed_qty": short_closed,
                    "close_id": close_id,
                    "reason": reason,
                },
            )

        # Fully closed → remove from open positions
        if position.matched_quantity < 1e-12:
            state.open_positions.pop(position.position_id, None)
            self.journal.append(
                "exit.closed",
                {
                    "position_id": position.position_id,
                    "reason": reason,
                    "price_pnl": close.realized_price_pnl_quote,
                    "net_quote": close.net_quote,
                },
            )

    async def _submit_close_leg(
        self,
        request: OrderRequest,
        position_id: str,
        leg: str,
        now_ms: int,
    ) -> dict[str, Any]:
        """Submit a single close leg through the venue adapter."""
        adapter = self.adapters.get(request.venue)
        if adapter is None:
            self.journal.append(
                "order.rejected",
                {
                    "position_id": position_id,
                    "leg": leg,
                    "reason": f"no adapter for {request.venue.value}",
                    "client_order_id": request.client_order_id,
                },
            )
            return {"outcome": "rejected", "fill": None, "reason": "no adapter", "order_id": ""}

        try:
            fill = await adapter.place_order(request)
            if fill.quantity > 0:
                self.journal.append(
                    "order.filled",
                    {
                        "position_id": position_id,
                        "leg": leg,
                        "order_id": fill.order_id,
                        "client_order_id": request.client_order_id,
                        "quantity": fill.quantity,
                        "price": fill.price,
                        "fee_quote": fill.fee_quote,
                    },
                )
                return {"outcome": "filled", "fill": fill, "order_id": fill.order_id}
            else:
                self.journal.append(
                    "order.uncertain",
                    {
                        "position_id": position_id,
                        "leg": leg,
                        "reason": "zero fill",
                        "client_order_id": request.client_order_id,
                    },
                )
                return {"outcome": "uncertain", "fill": None, "order_id": ""}

        except OrderSubmitError as e:
            if e.is_rejected:
                self.journal.append(
                    "order.rejected",
                    {
                        "position_id": position_id,
                        "leg": leg,
                        "reason": str(e),
                        "client_order_id": request.client_order_id,
                    },
                )
                return {"outcome": "rejected", "fill": None, "reason": str(e), "order_id": ""}
            else:
                self.journal.append(
                    "order.uncertain",
                    {
                        "position_id": position_id,
                        "leg": leg,
                        "reason": str(e),
                        "client_order_id": request.client_order_id,
                    },
                )
                return {"outcome": "uncertain", "fill": None, "order_id": ""}

        except Exception as e:
            self.journal.append(
                "order.uncertain",
                {
                    "position_id": position_id,
                    "leg": leg,
                    "reason": str(e),
                    "client_order_id": request.client_order_id,
                },
            )
            return {"outcome": "uncertain", "fill": None, "order_id": ""}

    async def _submit_close_leg_with_retry(
        self,
        request: OrderRequest,
        position_id: str,
        leg: str,
        now_ms: int,
    ) -> dict[str, Any]:
        """Submit a close leg with V1 retry throttling.

        On UNCERTAIN outcomes, retries up to max_close_retries with
        exponential backoff. On REJECTED, returns immediately.
        Terminal success on empty-position reduce-only (venue-reported flat).
        """
        retry_base_ms = 1000
        retry_max_ms = 10_000

        for attempt in range(1, self.config.max_close_retries + 1):
            result = await self._submit_close_leg(request, position_id, leg, now_ms)

            if result["outcome"] == "filled":
                return result

            if result["outcome"] == "rejected":
                # Check for terminal reduce-only success (venue flat)
                reason = result.get("reason", "")
                if "position closed" in reason.lower() or "empty position" in reason.lower():
                    self.journal.append(
                        "order.filled",
                        {
                            "position_id": position_id,
                            "leg": leg,
                            "reason": "terminal_reduce_only",
                            "client_order_id": request.client_order_id,
                            "attempt": attempt,
                        },
                    )
                    return {"outcome": "filled", "fill": OrderFill(
                        venue=request.venue, symbol=request.symbol,
                        side=request.side, quantity=0.0, price=0.0,
                        order_id="terminal-flat",
                    ), "order_id": "terminal-flat"}
                return result

            # Uncertain — retry with backoff if attempts remain
            if attempt < self.config.max_close_retries:
                backoff_ms = min(retry_base_ms * (2 ** (attempt - 1)), retry_max_ms)
                self.journal.append(
                    "exit.retry_wait",
                    {
                        "position_id": position_id,
                        "leg": leg,
                        "attempt": attempt,
                        "backoff_ms": backoff_ms,
                        "client_order_id": request.client_order_id,
                    },
                )
                await asyncio.sleep(backoff_ms / 1000.0)

        return {"outcome": "uncertain", "fill": None, "order_id": ""}


# ---------------------------------------------------------------------------
# Build exit PnL attribution (V1 build_exit_pnl_attribution, line 5960)
# ---------------------------------------------------------------------------


def build_exit_pnl_attribution(
    position: OpenPosition,
    close: CloseExecution,
) -> dict[str, float]:
    """V1 build_exit_pnl_attribution (exit.rs line 5960): separate PnL components.

    Returns dict with funding, price_pnl, entry_fee, exit_fee, net_quote.
    """
    funding_quote = position.captured_funding_quote + position.second_stage_funding_quote
    entry_fee = position.long_entry_fee_quote + position.short_entry_fee_quote
    exit_fee = close.long_fee_quote + close.short_fee_quote
    net = close.realized_price_pnl_quote + funding_quote - entry_fee - exit_fee

    return {
        "funding_quote": funding_quote,
        "price_pnl_quote": close.realized_price_pnl_quote,
        "entry_fee_quote": entry_fee,
        "exit_fee_quote": exit_fee,
        "net_quote": net,
    }
