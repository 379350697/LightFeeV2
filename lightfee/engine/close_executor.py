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

from dataclasses import dataclass, field
from typing import Any, Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import OrderFill, OrderRequest, Side, Venue
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.engine.exit import CloseExecution
from lightfee.engine.residual import ResidualExposureTask, ResidualOrigin, approx_eq
from lightfee.engine.state import OpenPosition, PendingClose
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
# Close executor
# ---------------------------------------------------------------------------


@dataclass
class CloseExecConfig:
    deadline_ms: int = 30_000
    max_close_retries: int = 3
    post_funding_hold_ms: int = 30_000


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

        Submits both legs (short=buy, long=sell) as reduce-only taker orders.
        When *state* is provided, writes back PnL attribution, matched quantity
        updates, and manages PendingClose lifecycle for uncertain outcomes.
        """
        from lightfee.engine.exit import CloseExecution as CE

        if total_quantity is None:
            total_quantity = position.matched_quantity
        if total_quantity <= 0:
            return CE(position_id=position.position_id, reason=reason,
                      long_close_price=0.0, short_close_price=0.0,
                      long_close_qty=0.0, short_close_qty=0.0)

        short_req = OrderRequest(
            venue=position.short_venue, symbol=position.symbol,
            side=Side.BUY, quantity=total_quantity, reduce_only=True,
            price=short_price_hint or None,
        )
        long_req = OrderRequest(
            venue=position.long_venue, symbol=position.symbol,
            side=Side.SELL, quantity=total_quantity, reduce_only=True,
            price=long_price_hint or None,
        )

        # Submit short close first
        short_legs: list[CloseExecutionLeg] = []
        long_legs: list[CloseExecutionLeg] = []
        short_uncertain = False
        long_uncertain = False

        short_result = await self._submit_close_leg(
            short_req, position.position_id, "short", now_ms,
        )
        if short_result["outcome"] == "filled":
            short_legs.append(CloseExecutionLeg(
                fill=short_result["fill"],
                submit_started_at_ms=now_ms,
            ))
        elif short_result["outcome"] == "rejected":
            self.journal.append(
                "exit.short_rejected",
                {"position_id": position.position_id, "reason": short_result.get("reason", "")},
            )
        elif short_result["outcome"] == "uncertain":
            short_uncertain = True

        long_result = await self._submit_close_leg(
            long_req, position.position_id, "long", now_ms,
        )
        if long_result["outcome"] == "filled":
            long_legs.append(CloseExecutionLeg(
                fill=long_result["fill"],
                submit_started_at_ms=now_ms,
            ))
        elif long_result["outcome"] == "uncertain":
            long_uncertain = True

        close = build_close_execution_from_legs(
            position, 1, short_legs, long_legs,
        )
        close.reason = reason

        # Detect residual
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
                },
            )

        # Write back to EngineState when available
        if state is not None:
            self._writeback_to_state(
                state, position, close, long_closed, short_closed,
                long_uncertain, short_uncertain, now_ms, reason,
            )

        self.journal.append(
            "exit.completed",
            {
                "position_id": position.position_id,
                "reason": reason,
                "long_closed_qty": long_closed,
                "short_closed_qty": short_closed,
                "long_uncertain": long_uncertain,
                "short_uncertain": short_uncertain,
                "price_pnl": close.realized_price_pnl_quote,
                "net_quote": close.net_quote,
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
    ) -> None:
        """Write close execution results back into EngineState.

        - Updates position PnL / quantity tracking
        - Creates PendingClose for uncertain legs
        - Removes fully-closed positions from open_positions
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
            close_id = f"pending-close-{position.position_id}-{now_ms}"
            state.pending_closes[close_id] = PendingClose(
                close_id=close_id,
                position_id=position.position_id,
                reason=reason,
                created_at_ms=now_ms,
                long_order_id="",
                short_order_id="",
                long_closed=long_closed,
                short_closed=short_closed,
                long_uncertain=long_uncertain,
                short_uncertain=short_uncertain,
            )
            self.journal.append(
                "exit.pending_close_registered",
                {
                    "close_id": close_id,
                    "position_id": position.position_id,
                    "long_uncertain": long_uncertain,
                    "short_uncertain": short_uncertain,
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
                f"exit.{leg}_rejected",
                {"position_id": position_id, "reason": f"no adapter for {request.venue.value}"},
            )
            return {"outcome": "rejected", "fill": None, "reason": "no adapter"}

        try:
            fill = await adapter.place_order(request)
            if fill.quantity > 0:
                self.journal.append(
                    f"exit.{leg}_filled",
                    {
                        "position_id": position_id,
                        "order_id": fill.order_id,
                        "quantity": fill.quantity,
                        "price": fill.price,
                        "fee_quote": fill.fee_quote,
                    },
                )
                return {"outcome": "filled", "fill": fill}
            else:
                self.journal.append(
                    f"exit.{leg}_uncertain",
                    {"position_id": position_id, "reason": "zero fill"},
                )
                return {"outcome": "uncertain", "fill": None}

        except OrderSubmitError as e:
            if e.is_rejected:
                self.journal.append(
                    f"exit.{leg}_rejected",
                    {"position_id": position_id, "reason": str(e)},
                )
                return {"outcome": "rejected", "fill": None, "reason": str(e)}
            else:
                self.journal.append(
                    f"exit.{leg}_uncertain",
                    {"position_id": position_id, "reason": str(e)},
                )
                return {"outcome": "uncertain", "fill": None}

        except Exception as e:
            self.journal.append(
                f"exit.{leg}_uncertain",
                {"position_id": position_id, "reason": str(e)},
            )
            return {"outcome": "uncertain", "fill": None}


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
