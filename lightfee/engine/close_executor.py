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
from lightfee.core.exchange_errors import (
    RequestContext,
    build_evidence_from_order_submit_error,
    build_fallback_evidence,
)
from lightfee.engine.bootstrap import wall_clock_now_ms
from lightfee.engine.exit import CloseExecution
from lightfee.engine.lifecycle import enter_fail_closed
from lightfee.engine.residual import ResidualExposureTask, ResidualOrigin, approx_eq
from lightfee.engine.state import CloseLegRecord, OpenPosition, PendingClose
from lightfee.persistence.journal import Journal
from lightfee.venues.cid import compact_client_order_id
from lightfee.venues.common import venue_reduce_only_close_exempts_min_notional


# ---------------------------------------------------------------------------
# V1 compensation constants (entry.rs:23-24)
# ---------------------------------------------------------------------------

_MAX_EMERGENCY_FLATTEN_ATTEMPTS = 3
_COMPENSATION_HARD_STOP_DELAY_MS = 750


def order_error_may_have_created_exposure(error: Exception) -> bool:
    """V1 order_error_may_have_created_exposure (entry.rs:494).

    Returns True when the error's SubmitFailureClass is UNCERTAIN,
    meaning the order may have been partially or fully executed
    on the exchange before the error was observed.
    """
    if isinstance(error, OrderSubmitError):
        return error.is_uncertain
    return False


# ---------------------------------------------------------------------------
# V1 structured close-leg error classification (M-R12)
# ---------------------------------------------------------------------------


def _extract_gate_error_fields(error_str: str) -> dict[str, Optional[str]]:
    """V1 gate_http_error_details: extract label and message from Gate error string.

    Gate error format: "... label=REDUCE_EXCEEDED msg=empty position ..."
    """
    label = None
    message = None
    lower = error_str.lower()
    # Extract label=
    label_start = lower.find("label=")
    if label_start >= 0:
        label_start += 6
        label_end = lower.find(" ", label_start)
        if label_end < 0:
            label_end = len(lower)
        label = lower[label_start:label_end].strip()
    # Extract msg=
    msg_start = lower.find("msg=")
    if msg_start >= 0:
        msg_start += 4
        msg_end = lower.find(" ", msg_start)
        if msg_end < 0:
            msg_end = len(lower)
        message = lower[msg_start:msg_end].strip()
    return {"label": label, "message": message}


def _classify_close_leg_error(error_str: str) -> dict[str, Any]:
    """V1 structured error classification for close leg rejections.

    Returns dict with keys:
    - label: str (error label if extractable, else "")
    - message: str (error message if extractable, else "")
    - empty_position: bool (venue reports no position for reduce_only)
    - order_not_found: bool (order id not recognized)
    - pending_conflict: bool (reduce_only order conflicts with pending)
    - terminal_reduce_only: bool (reduce_only rejected because position flat)
    """
    fields = _extract_gate_error_fields(error_str)
    label = fields.get("label") or ""
    message = fields.get("message") or ""
    lower = error_str.lower()

    # --- Gate structured detection ---
    empty_position = (
        label == "reduce_exceeded" and "empty position" in message
    ) or (
        "reduce_exceeded" in lower and "empty position" in lower
    )

    order_not_found = (
        label == "order_not_found"
    ) or (
        "order not found" in lower
    ) or (
        "order_not_found" in lower
    )

    pending_conflict = (
        (label in ("reduce_only_fail", "reduce_exceeded"))
        and "pending order" in message
        and "reduce order" in message
    ) or (
        ("reduce_only_fail" in lower or "reduce_exceeded" in lower)
        and "pending order" in lower
        and "reduce order" in lower
    )

    # --- OKX structured detection (code-based) ---
    # V1: OKX error codes — "51000"=order not found, "51108"=position closed/no position
    if not empty_position:
        if _string_contains_any(lower, ("code=51000", "code 51000", "\"51000\"")):
            order_not_found = True
        if "position" in lower and _string_contains_any(lower, ("51000", "51108", "51109", "51110", "51112")):
            empty_position = True
        # OKX: "Order does not exist" with reduce_only → position already flat
        if "order does not exist" in lower:
            order_not_found = True
            empty_position = empty_position or "reduce" in lower

    # --- Bybit structured detection (code-based) ---
    # V1: Bybit error codes for reduce-only terminal conditions
    if not empty_position and not order_not_found:
        if "position" in lower and _string_contains_any(lower, ("110001", "110017", "110043", "20001", "20070")):
            empty_position = True
        # Bybit: "no position" / "position idx" error for reduce_only
        if "no position" in lower or "current position is zero" in lower:
            empty_position = True

    # --- Binance structured detection (code-based) ---
    # V1: Binance error codes — -2010=insufficient position, -2011=order not found
    if not order_not_found:
        if _string_contains_any(lower, ("-2011", "-2013", "-2015", "code=-201")):
            order_not_found = True
    if not empty_position:
        if _string_contains_any(lower, ("-2010", "-4069", "-4164")):
            empty_position = True
        if "-2022" in lower and _string_contains_any(lower, ("reduceonly", "reduce only", "reduce_only")):
            empty_position = True
        # Binance: "position is not enough" / "insufficient position"
        if "reduce" in lower and ("insufficient" in lower or "not enough" in lower):
            empty_position = True
        if "reduceonly order is rejected" in lower or "reduce only order is rejected" in lower:
            empty_position = True

    # --- Terminal: when empty_position or order_not_found confirmed, or generic pattern ---
    terminal_reduce_only = (
        empty_position
        or order_not_found
        or _string_contains_any(lower, (
            "position closed", "empty position", "position does not exist",
            "reduce_only", "no position", "insufficient position",
            "position not found", "order does not exist",
            "current position is zero", "reduceonly order is rejected",
            "reduce only order is rejected",
        ))
    )

    return {
        "label": label,
        "message": message,
        "empty_position": empty_position,
        "order_not_found": order_not_found,
        "pending_conflict": pending_conflict,
        "terminal_reduce_only": terminal_reduce_only,
    }


def _is_bybit_duplicate_order_link_id(reason: str) -> bool:
    lower = reason.lower()
    return "110072" in lower or ("orderlinkedid" in lower and "duplicate" in lower)


def _is_terminal_reduce_only(error_class: dict[str, Any], reason: str) -> bool:
    """V1: determine if a reduce-only rejection is truly terminal.

    Terminal when:
    - Structured empty_position detected (Gate label-based)
    - Generic terminal pattern matched AND NOT a pending conflict
    """
    if error_class["empty_position"]:
        return True
    if error_class["pending_conflict"]:
        return False  # Pending conflict is retryable, not terminal
    return error_class["terminal_reduce_only"]


def _string_contains_any(text: str, patterns: tuple[str, ...]) -> bool:
    return any(p in text for p in patterns)


# ---------------------------------------------------------------------------


class CompensationFailedError(Exception):
    """Raised when close compensation flatten fails on all venues.

    V1: enter_fail_closed + persist_state + return Err(...)
    """


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
            average_price=leg.fill.price,
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
        short_stage: str = "exit_short",
        long_stage: str = "exit_long",
    ) -> CloseExecution | None:
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

            short_cid = compact_client_order_id(
                position.position_id,
                f"{short_stage}{chunk_suffix}",
            )
            long_cid = compact_client_order_id(
                position.position_id,
                f"{long_stage}{chunk_suffix}",
            )

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
                # V1: compensate_failed_full_close for short leg uncertain errors
                if reason:
                    chunk_short_cids.append(short_cid)
                    self.journal.append(
                        "exit.close_chunk_submitted",
                        {
                            "position_id": position.position_id,
                            "chunk_index": chunk_idx,
                            "total_chunks": total_chunks,
                            "chunk_quantity": chunk_qty,
                            "short_client_order_id": short_cid,
                            "long_client_order_id": "",
                            "short_outcome": "uncertain_compensating",
                            "long_outcome": "not_submitted",
                        },
                    )
                    try:
                        await self.compensate_failed_full_close(
                            position, reason,
                            f"exit_short_chunk_{chunk_idx}",
                            position.short_venue,
                            OrderSubmitError(
                                SubmitFailureClass.UNCERTAIN,
                                short_result.get("reason", "close leg uncertain"),
                            ),
                            short_legs, long_legs, state,
                        )
                        break  # compensation succeeded, exit chunk loop
                    except CompensationFailedError:
                        return build_close_execution_from_legs(
                            position, total_chunks, short_legs, long_legs,
                        )
            elif short_result["outcome"] == "rejected":
                chunk_short_cids.append(short_cid)
                chunk_short_order_ids.append(short_result.get("order_id", ""))
                self.journal.append(
                    "exit.close_chunk_submitted",
                    {
                        "position_id": position.position_id,
                        "chunk_index": chunk_idx,
                        "total_chunks": total_chunks,
                        "chunk_quantity": chunk_qty,
                        "short_client_order_id": short_cid,
                        "long_client_order_id": "",
                        "short_outcome": "rejected",
                        "long_outcome": "not_submitted",
                        "reason": short_result.get("reason", ""),
                    },
                )
                self.journal.append(
                    "execution.close_failed",
                    {
                        "position_id": position.position_id,
                        "symbol": position.symbol,
                        "reason": reason,
                        "failed_leg": "short",
                        "error": short_result.get("reason", "close leg rejected"),
                    },
                )
                return None

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
                # V1: compensate for long leg uncertain errors
                if reason:
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
                            "long_outcome": "uncertain_compensating",
                        },
                    )
                    try:
                        await self.compensate_failed_full_close(
                            position, reason,
                            f"exit_long_chunk_{chunk_idx}",
                            position.long_venue,
                            OrderSubmitError(
                                SubmitFailureClass.UNCERTAIN,
                                long_result.get("reason", "close leg uncertain"),
                            ),
                            short_legs, long_legs, state,
                        )
                        break
                    except CompensationFailedError:
                        return build_close_execution_from_legs(
                            position, total_chunks, short_legs, long_legs,
                        )
            elif long_result["outcome"] == "rejected" and reason:
                # V1: long leg rejected after short may have filled → compensate
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
                        "long_outcome": "rejected_compensating",
                    },
                )
                try:
                    await self.compensate_failed_full_close(
                        position, reason,
                        f"exit_long_chunk_{chunk_idx}",
                        position.long_venue,
                        OrderSubmitError(
                            SubmitFailureClass.REJECTED,
                            long_result.get("reason", "close leg rejected"),
                        ),
                        short_legs, long_legs, state,
                    )
                    break
                except CompensationFailedError:
                    return build_close_execution_from_legs(
                        position, total_chunks, short_legs, long_legs,
                    )

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

        if not short_legs and not long_legs and not (any_short_uncertain or any_long_uncertain):
            self.journal.append(
                "execution.close_failed",
                {
                    "position_id": position.position_id,
                    "symbol": position.symbol,
                    "reason": reason,
                    "error": "close produced no confirmed fills",
                },
            )
            return None


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
            # V1: replace_pending_residual_repair_for_origin (exit.rs:5078-5084)
            # Write the residual repair task into state so it is periodically retried
            if state is not None:
                # Remove any existing task for the same (position_id, pair_id, origin)
                residual_dict = _residual_task_to_dict(residual)
                state.pending_residual_repairs = [
                    t for t in state.pending_residual_repairs
                    if not (isinstance(t, dict)
                            and t.get("position_id") == residual.position_id
                            and t.get("pair_id") == residual.pair_id
                            and t.get("origin") == residual_dict["origin"])
                ]
                state.pending_residual_repairs.append(residual_dict)

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

        if long_closed > 1e-12 or short_closed > 1e-12:
            # V1: exit.closed is a critical event — synchronous durability
            self.journal.append_critical(
                now_ms,
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
        # V1: exit.partial_closed is a critical event
        if matched_closed > 1e-12 and position.matched_quantity > 1e-12:
            self.journal.append_critical(
                now_ms,
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
        # V1: exit.closed is a critical event
        if position.matched_quantity < 1e-12:
            state.open_positions.pop(position.position_id, None)
            self.journal.append_critical(
                now_ms,
                "exit.closed",
                {
                    "position_id": position.position_id,
                    "reason": reason,
                    "price_pnl": close.realized_price_pnl_quote,
                    "net_quote": close.net_quote,
                },
            )
        else:
            # V1: dust pause — when remaining is below dust, mark last_risk_action
            # to prevent immediate re-close (exit.rs:3093-3171)
            _DUST_QUANTITY_THRESHOLD = 1e-8
            if position.matched_quantity < _DUST_QUANTITY_THRESHOLD:
                position.last_risk_action_at_ms = now_ms  # reuse as dust pause sentinel
                self.journal.append(
                    "exit.close_dust_paused",
                    {
                        "position_id": position.position_id,
                        "remaining_quantity": position.matched_quantity,
                        "ts_ms": now_ms,
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
                # V1 parity: execution.order_filled includes venue/symbol/side/filled_at_ms
                self.journal.append(
                    "order.filled",
                    {
                        "position_id": position_id,
                        "leg": leg,
                        "venue": fill.venue.value if hasattr(fill.venue, 'value') else str(fill.venue),
                        "symbol": fill.symbol,
                        "side": fill.side.value if hasattr(fill.side, 'value') else str(fill.side),
                        "order_id": fill.order_id,
                        "client_order_id": request.client_order_id,
                        "quantity": fill.quantity,
                        "price": fill.price,
                        "fee_quote": fill.fee_quote,
                        "filled_at_ms": fill.filled_at_ms,
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
            req_ctx = RequestContext.from_order_request(request)
            evidence = build_evidence_from_order_submit_error(
                e,
                venue=request.venue.value,
                operation="place_order",
                endpoint="",
                request_context=req_ctx,
            )
            if e.is_rejected:
                self.journal.append(
                    "order.rejected",
                    {
                        "position_id": position_id,
                        "leg": leg,
                        "reason": str(e),
                        "client_order_id": request.client_order_id,
                        "exchange_error": evidence.to_dict(),
                        "request_context": req_ctx.to_dict(),
                        "evidence_completeness": evidence.evidence_completeness,
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
                        "exchange_error": evidence.to_dict(),
                        "request_context": req_ctx.to_dict(),
                        "evidence_completeness": evidence.evidence_completeness,
                    },
                )
                return {"outcome": "uncertain", "fill": None, "order_id": ""}

        except Exception as e:
            req_ctx = RequestContext.from_order_request(request)
            evidence = build_fallback_evidence(
                e,
                venue=request.venue.value,
                operation="place_order",
                request_context=req_ctx,
            )
            self.journal.append(
                "order.uncertain",
                {
                    "position_id": position_id,
                    "leg": leg,
                    "reason": str(e),
                    "client_order_id": request.client_order_id,
                    "exchange_error": evidence.to_dict(),
                    "request_context": req_ctx.to_dict(),
                    "evidence_completeness": evidence.evidence_completeness,
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
                # V1: check for terminal reduce-only success (venue already flat)
                reason = result.get("reason", "")
                if request.venue == Venue.BYBIT and _is_bybit_duplicate_order_link_id(reason):
                    adapter = self.adapters.get(request.venue)
                    reconciliation = None
                    if adapter is not None:
                        try:
                            reconciliation = await adapter.fetch_order_fill_reconciliation(
                                request.symbol, "", request.client_order_id,
                            )
                        except Exception as exc:
                            self.journal.append(
                                "exit.close_duplicate_client_order_reconcile_failed",
                                {
                                    "position_id": position_id,
                                    "leg": leg,
                                    "client_order_id": request.client_order_id,
                                    "error": str(exc),
                                },
                            )
                    recon_qty = getattr(reconciliation, "quantity", 0.0) or 0.0
                    if recon_qty > 1e-12:
                        fill = OrderFill(
                            venue=request.venue,
                            symbol=request.symbol,
                            side=request.side,
                            quantity=float(recon_qty),
                            price=float(
                                getattr(
                                    reconciliation,
                                    "price",
                                    getattr(reconciliation, "average_price", request.price or 0.0),
                                )
                                or 0.0
                            ),
                            order_id=getattr(reconciliation, "order_id", "") or "",
                            client_order_id=(
                                getattr(reconciliation, "client_order_id", None)
                                or request.client_order_id
                            ),
                            fee_quote=getattr(reconciliation, "fee_quote", None),
                            filled_at_ms=getattr(reconciliation, "filled_at_ms", 0) or now_ms,
                        )
                        self.journal.append(
                            "order.filled",
                            {
                                "position_id": position_id,
                                "leg": leg,
                                "venue": request.venue.value,
                                "symbol": request.symbol,
                                "side": request.side.value,
                                "order_id": fill.order_id,
                                "client_order_id": fill.client_order_id,
                                "quantity": fill.quantity,
                                "price": fill.price,
                                "fee_quote": fill.fee_quote,
                                "filled_at_ms": fill.filled_at_ms,
                                "reason": "duplicate_client_order_reconciled",
                            },
                        )
                        return {"outcome": "filled", "fill": fill, "order_id": fill.order_id}

                    self.journal.append(
                        "exit.close_duplicate_client_order_pending_reconcile",
                        {
                            "position_id": position_id,
                            "leg": leg,
                            "client_order_id": request.client_order_id,
                            "reason": reason,
                        },
                    )
                    return {
                        "outcome": "uncertain",
                        "fill": None,
                        "reason": reason,
                        "order_id": "",
                    }

                # --- Structured classification (Gate first, then generic) ---
                error_class = _classify_close_leg_error(reason)
                is_terminal = _is_terminal_reduce_only(error_class, reason)
                is_pending_conflict = error_class.get("pending_conflict", False)
                is_empty_position = error_class.get("empty_position", False)
                is_order_not_found = error_class.get("order_not_found", False)

                if is_terminal or is_order_not_found:
                    # V1: verify by fetching exchange position
                    is_flat = False
                    try:
                        adapter = self.adapters.get(request.venue)
                        if adapter is not None:
                            current_pos = await adapter.fetch_position(request.symbol)
                            is_flat = current_pos is not None and abs(current_pos.quantity) <= 1e-9
                    except Exception:
                        pass
                    if is_flat or is_empty_position or is_order_not_found:
                        self.journal.append(
                            "order.filled",
                            {
                                "position_id": position_id,
                                "leg": leg,
                                "reason": "terminal_reduce_only",
                                "error_label": error_class.get("label", ""),
                                "exchange_verified_flat": is_flat,
                                "client_order_id": request.client_order_id,
                                "attempt": attempt,
                            },
                        )
                        return {"outcome": "filled", "fill": OrderFill(
                        venue=request.venue, symbol=request.symbol,
                        side=request.side, quantity=0.0, price=0.0,
                        order_id="terminal-flat",
                    ), "order_id": "terminal-flat"}

                if is_pending_conflict:
                    # V1: pending reduce-only conflict — not terminal, retry in next iteration
                    # V1 gate.rs:2070 — cancel conflicting orders, then retry.
                    self.journal.append(
                        "exit.close_reduce_only_pending_conflict",
                        {
                            "position_id": position_id,
                            "leg": leg,
                            "error_label": error_class.get("label", ""),
                            "reason": reason,
                            "client_order_id": request.client_order_id,
                            "attempt": attempt,
                        },
                    )
                    if attempt < self.config.max_close_retries:
                        backoff_ms = min(retry_base_ms * (2 ** (attempt - 1)), retry_max_ms)
                        self.journal.append(
                            "exit.retry_wait",
                            {
                                "position_id": position_id,
                                "leg": leg,
                                "attempt": attempt,
                                "backoff_ms": backoff_ms,
                                "reason": "reduce_only_pending_conflict",
                                "client_order_id": request.client_order_id,
                            },
                        )
                        await asyncio.sleep(backoff_ms / 1000.0)
                        continue
                    # Max retries exhausted — return as rejected
                    return result
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

    # ------------------------------------------------------------------
    # V1 compensate_failed_full_close (exit.rs:1482-1601)
    # ------------------------------------------------------------------

    async def compensate_failed_full_close(
        self,
        position: OpenPosition,
        close_reason: str,
        failed_stage: str,
        failed_venue: Venue,
        error: Exception,
        short_legs: list[CloseExecutionLeg],
        long_legs: list[CloseExecutionLeg],
        state: Any | None = None,
    ) -> None:
        """V1 compensate_failed_full_close (exit.rs:1482-1601).

        After a close leg fails with exposure-creating error, fetch exchange
        positions and flatten residual. Falls back to hard-stop flattening.
        If all venues fail, enters FAIL_CLOSED and raises CompensationFailedError.
        """
        residual_positions: list[dict[str, Any]] = []
        compensated_venues: list[str] = []

        for venue, compensate_stage in [
            (position.short_venue, "exit_compensate_short"),
            (position.long_venue, "exit_compensate_long"),
        ]:
            adapter = self.adapters.get(venue)
            if adapter is None:
                continue

            try:
                pos = await adapter.fetch_position(position.symbol)
            except Exception as fetch_err:
                self.journal.append(
                    "exit.compensation_fetch_failed",
                    {
                        "position_id": position.position_id,
                        "venue": venue.value,
                        "error": str(fetch_err),
                    },
                )
                continue

            if pos is None or abs(pos.quantity) <= 1e-9:
                continue

            residual_positions.append({
                "venue": venue.value,
                "size": pos.quantity,
                "observed_at_ms": pos.observed_at_ms,
            })

            # V1: side from signed position size
            # V2: PositionSnapshot.quantity is abs, side carries direction
            cleanup_side = pos.side.opposite()
            quantity = abs(pos.quantity)

            # Tier 1: flatten with retries
            fill_result = await self._flatten_close_leg_with_retries(
                venue, position.symbol, quantity, cleanup_side,
                position.position_id, compensate_stage,
            )

            if fill_result is None:
                # Tier 2: compensation hard stop
                fill_result = await self._compensation_hard_stop_close_leg(
                    venue, position.symbol, position.position_id,
                    compensate_stage,
                )

            if fill_result is None:
                # Both tiers failed — re-verify exchange position before FAIL_CLOSED.
                # V1: compensate_failed_full_close (exit.rs:1482-1601) only enters
                # fail_closed when there is confirmed residual exposure. If the
                # exchange reports flat (position already closed by another
                # mechanism), treat as success — do NOT enter fail_closed.
                exchange_flat = False
                try:
                    verify_pos = await adapter.fetch_position(position.symbol)
                    exchange_flat = verify_pos is None or abs(verify_pos.quantity) <= 1e-9
                except Exception:
                    pass

                if exchange_flat:
                    self.journal.append(
                        "exit.compensation_already_flat",
                        {
                            "position_id": position.position_id,
                            "symbol": position.symbol,
                            "venue": venue.value,
                            "reason": close_reason,
                            "failed_stage": failed_stage,
                        },
                    )
                    compensated_venues.append(venue.value)
                    continue

                # Confirmed residual exposure → FAIL_CLOSED
                if state is not None:
                    enter_fail_closed(state)
                    state.last_error = (
                        f"close compensation failed for {position.position_id}"
                        f" on {venue.value}"
                    )
                self.journal.append_critical(
                    wall_clock_now_ms(),
                    "execution.compensation_failed",
                    {
                        "position_id": position.position_id,
                        "symbol": position.symbol,
                        "phase": "close",
                        "reason": close_reason,
                        "failed_stage": failed_stage,
                        "failed_venue": failed_venue.value,
                        "error": str(error),
                        "compensation_venue": venue.value,
                    },
                )
                raise CompensationFailedError(
                    f"close compensation failed for {position.position_id}"
                    f" on {venue.value}"
                )

            # Compensation succeeded — record the fill leg
            fill, client_order_id, submit_ms = fill_result
            compensated_venues.append(venue.value)
            leg = CloseExecutionLeg(
                fill=fill,
                client_order_id=client_order_id,
                submit_started_at_ms=submit_ms,
            )
            if venue == position.short_venue:
                short_legs.append(leg)
            else:
                long_legs.append(leg)

        self.journal.append(
            "exit.compensated",
            {
                "position_id": position.position_id,
                "symbol": position.symbol,
                "reason": close_reason,
                "failed_stage": failed_stage,
                "failed_venue": failed_venue.value,
                "error": str(error),
                "residual_positions": residual_positions,
                "compensated_venues": compensated_venues,
            },
        )

    async def _compensate_close_leg_exposure(
        self, venue: Venue, symbol: str, quantity: float,
        side: Side, position_id: str, stage: str,
    ) -> tuple[OrderFill, str, int] | None:
        """Single-shot reduce-only close for compensation flatten.

        Returns (fill, client_order_id, submit_started_at_ms) or None.
        """

        adapter = self.adapters.get(venue)
        if adapter is None:
            return None

        client_order_id = compact_client_order_id(
            position_id, f"{stage}:{symbol}",
        )
        submit_ms = wall_clock_now_ms()

        req = OrderRequest(
            venue=venue, symbol=symbol, side=side,
            quantity=quantity, reduce_only=True, post_only=False,
            client_order_id=client_order_id,
        )

        try:
            fill = await adapter.place_order(req)
        except Exception:
            # Order may have created exposure — check if position is now flat
            try:
                verify_pos = await adapter.fetch_position(symbol)
                if verify_pos is None or abs(verify_pos.quantity) <= 1e-9:
                    return None  # Already flat, no fill to record
            except Exception:
                pass
            return None

        if fill.quantity >= quantity - 1e-9:
            return (fill, client_order_id, submit_ms)

        # Partial fill — re-verify flatness
        try:
            verify_pos = await adapter.fetch_position(symbol)
            if verify_pos is None or abs(verify_pos.quantity) <= 1e-9:
                return (fill, client_order_id, submit_ms)  # Flat despite partial fill
        except Exception:
            pass

        return None  # Position not flat after partial fill

    async def _flatten_close_leg_with_retries(
        self, venue: Venue, symbol: str, quantity: float,
        side: Side, position_id: str, stage: str,
    ) -> tuple[OrderFill, str, int] | None:
        """V1 flatten_single_leg_with_retries_collect (entry.rs:2711-2801).

        Retries flatten up to _MAX_EMERGENCY_FLATTEN_ATTEMPTS, re-fetching
        position between attempts.
        """
        adapter = self.adapters.get(venue)
        if adapter is None:
            return None

        retry_quantity = quantity
        retry_side = side

        for attempt in range(1, _MAX_EMERGENCY_FLATTEN_ATTEMPTS + 1):
            result = await self._compensate_close_leg_exposure(
                venue, symbol, retry_quantity, retry_side,
                position_id, f"{stage}_attempt_{attempt}",
            )
            if result is not None:
                return result

            if attempt < _MAX_EMERGENCY_FLATTEN_ATTEMPTS:
                # Re-fetch position for next retry
                try:
                    pos = await adapter.fetch_position(symbol)
                except Exception:
                    continue

                if pos is None or abs(pos.quantity) <= 1e-9:
                    return None  # Already flat

                retry_quantity = abs(pos.quantity)
                retry_side = pos.side.opposite()

        return None

    async def _compensation_hard_stop_close_leg(
        self, venue: Venue, symbol: str, position_id: str, stage: str,
    ) -> tuple[OrderFill, str, int] | None:
        """V1 compensation_hard_stop_failed_leg_exposure_collect (entry.rs:2828-2909).

        Delay, re-fetch position, then single-shot flatten.
        """
        await asyncio.sleep(_COMPENSATION_HARD_STOP_DELAY_MS / 1000.0)

        adapter = self.adapters.get(venue)
        if adapter is None:
            return None

        try:
            pos = await adapter.fetch_position(symbol)
        except Exception:
            return None

        if pos is None or abs(pos.quantity) <= 1e-9:
            return None  # Already flat

        cleanup_side = pos.side.opposite()
        quantity = abs(pos.quantity)

        return await self._compensate_close_leg_exposure(
            venue, symbol, quantity, cleanup_side,
            position_id, f"{stage}_hard_stop",
        )


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


def _residual_task_to_dict(task: ResidualExposureTask) -> dict[str, Any]:
    """V1: convert ResidualExposureTask to dict for state.pending_residual_repairs.

    The runtime expects dicts (runtime.py:4146), so we serialize the dataclass
    to a dict with V1-compatible field names.
    """
    return {
        "position_id": task.position_id,
        "pair_id": task.pair_id,
        "symbol": task.symbol,
        "origin": task.origin.value if hasattr(task.origin, 'value') else str(task.origin),
        "repair_venue": task.exposure_venue.value,
        "repair_side": task.exposure_side.value,
        "repair_quantity": task.exposure_quantity,
        "deadline_ms": task.deadline_ms,
        "created_at_ms": task.created_at_ms,
        "retry_count": task.retry_count,
        "last_attempt_at_ms": task.last_attempt_at_ms,
    }
