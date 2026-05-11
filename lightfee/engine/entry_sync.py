"""Async entry executor matching Rust V1 execute_incremental_entry and
submit_pending_entry_passive_cycle.

Rust references:
- src/execution_core/entry_sync.rs: execute_incremental_entry (line 3173)
- src/execution_core/entry_sync.rs: submit_pending_entry_passive_cycle (line 2486)
- src/execution_core/entry_sync.rs: reconcile_inflight_entry_hedge (line 4568)
- src/execution_core/entry_sync.rs: build_residual_task (line 749)
- src/engine/entry.rs: execute_order_leg (line 3854)
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import OrderFill, OrderRequest, Side, Venue
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.engine.entry import (
    EntryContext,
    EntryState,
    EntryType,
    advance_entry_state,
    build_entry_orders,
    build_open_position,
)
from lightfee.engine.execution_planner import (
    ExecutionRoute,
    plan_incremental_entry_execution,
)
from lightfee.engine.residual import (
    ResidualExposureTask,
    ResidualOrigin,
    split_entry_fill_residual,
)
from lightfee.engine.state import OpenPosition, PendingEntry
from lightfee.persistence.journal import Journal


@dataclass
class EntryExecutionResult:
    route: ExecutionRoute = ExecutionRoute.REJECTED
    state: EntryState = EntryState.FAILED
    open_position: Optional[OpenPosition] = None
    pending_entry: Optional[PendingEntry] = None
    residual_task: Optional[ResidualExposureTask] = None
    has_uncertainty: bool = False
    maker_fill: Optional[OrderFill] = None
    hedge_fill: Optional[OrderFill] = None
    journal_entries: list[dict[str, Any]] = field(default_factory=list)


class EntrySyncExecutor:
    """Async entry executor that submits maker→hedge orders via venue adapters.

    Mirrors V1 execute_incremental_entry: submits maker first, then hedge,
    handles reject/uncertain/fill outcomes, builds OpenPosition or
    ResidualExposureTask depending on fill symmetry.
    """

    def __init__(
        self,
        adapters: dict[Venue, VenueAdapter],
        journal: Journal,
        state: dict[str, Any] | None = None,
        config_overrides: dict[str, Any] | None = None,
    ):
        self.adapters = adapters
        self.journal = journal
        self.state: dict[str, Any] = state or {}
        overrides = config_overrides or {}
        self.deadline_ms: int = overrides.get("deadline_ms", 30_000)
        self.min_matched_ratio: float = overrides.get("min_matched_ratio", 0.95)

    # ------------------------------------------------------------------
    # Main execution entry point
    # ------------------------------------------------------------------

    async def execute(self, ctx: EntryContext) -> EntryExecutionResult:
        """Execute a full entry flow: maker → hedge, with outcome handling.

        V1 parity additions:
        - Creates PendingEntry on uncertain outcomes for reconciliation
        - Tracks clientOrderId in all journal events
        - Propagates TIF/reduce-only through order requests
        """
        now_ms = int(time.time() * 1000)
        result = EntryExecutionResult(
            route=ExecutionRoute.REJECTED,
            state=EntryState.IDLE,
        )

        # Build order requests with clientOrderId, TIF, reduce_only
        maker_req, hedge_req = build_entry_orders(ctx)

        # --- Phase 1: Submit maker ---
        maker_result = await self._submit_maker(ctx, maker_req, now_ms)
        result.journal_entries.extend(maker_result.get("journal", []))
        result.maker_fill = maker_result.get("fill")

        if maker_result["outcome"] == "rejected":
            result.state = EntryState.FAILED
            result.route = ExecutionRoute.REJECTED
            result.pending_entry = self._make_pending_entry(
                ctx, maker_req, hedge_req, now_ms,
                outcome="rejected",
                maker_order_id="",
                hedge_order_id="",
            )
            return result

        if maker_result["outcome"] == "uncertain":
            result.state = EntryState.FAILED_WITH_RESIDUAL
            result.has_uncertainty = True
            result.pending_entry = self._make_pending_entry(
                ctx, maker_req, hedge_req, now_ms,
                outcome="uncertain",
                maker_order_id=maker_result.get("order_id", ""),
                hedge_order_id="",
            )
            return result

        # Maker filled — advance to hedge
        maker_fill = maker_result["fill"]
        ctx = advance_entry_state(ctx, EntryState.MAKER_RESTING)
        ctx = advance_entry_state(ctx, EntryState.SUBMITTING_HEDGE)

        # --- Phase 2: Submit hedge ---
        hedge_result = await self._submit_hedge(ctx, hedge_req, now_ms)
        result.journal_entries.extend(hedge_result.get("journal", []))
        result.hedge_fill = hedge_result.get("fill")

        if hedge_result["outcome"] == "rejected":
            # Hedge reject after maker fill → residual
            if ctx.maker_leg == Side.BUY:
                residual = split_entry_fill_residual(
                    position_id=ctx.entry_id,
                    pair_id=f"{ctx.symbol.lower()}:{ctx.long_venue.value}->{ctx.short_venue.value}",
                    symbol=ctx.symbol,
                    long_venue=ctx.long_venue,
                    short_venue=ctx.short_venue,
                    long_fill=maker_fill,
                    short_fill=OrderFill(
                        venue=ctx.short_venue, symbol=ctx.symbol,
                        side=Side.SELL, quantity=0.0, price=0.0,
                    ),
                    created_cycle=0,
                    now_ms=now_ms,
                    deadline_ms=now_ms + self.deadline_ms,
                )
            else:
                residual = split_entry_fill_residual(
                    position_id=ctx.entry_id,
                    pair_id=f"{ctx.symbol.lower()}:{ctx.long_venue.value}->{ctx.short_venue.value}",
                    symbol=ctx.symbol,
                    long_venue=ctx.long_venue,
                    short_venue=ctx.short_venue,
                    long_fill=OrderFill(
                        venue=ctx.long_venue, symbol=ctx.symbol,
                        side=Side.BUY, quantity=0.0, price=0.0,
                    ),
                    short_fill=maker_fill,
                    created_cycle=0,
                    now_ms=now_ms,
                    deadline_ms=now_ms + self.deadline_ms,
                )

            result.state = EntryState.FAILED_WITH_RESIDUAL
            result.residual_task = residual
            result.pending_entry = self._make_pending_entry(
                ctx, maker_req, hedge_req, now_ms,
                outcome="hedge_rejected",
                maker_order_id=maker_fill.order_id,
                hedge_order_id="",
                maker_filled=maker_fill.quantity,
            )
            self.journal.append(
                "entry.hedge_rejected_residual",
                {
                    "position_id": ctx.entry_id,
                    "maker_filled": maker_fill.quantity,
                    "maker_client_order_id": maker_req.client_order_id,
                    "hedge_client_order_id": hedge_req.client_order_id,
                    "residual_quantity": residual.exposure_quantity if residual else 0,
                },
            )
            return result

        if hedge_result["outcome"] == "uncertain":
            result.state = EntryState.FAILED_WITH_RESIDUAL
            result.has_uncertainty = True
            result.pending_entry = self._make_pending_entry(
                ctx, maker_req, hedge_req, now_ms,
                outcome="hedge_uncertain",
                maker_order_id=maker_fill.order_id,
                hedge_order_id=hedge_result.get("order_id", ""),
                maker_filled=maker_fill.quantity,
            )
            return result

        # Both filled — check symmetry
        hedge_fill = hedge_result["fill"]

        # Detect residual
        residual = split_entry_fill_residual(
            position_id=ctx.entry_id,
            pair_id=f"{ctx.symbol.lower()}:{ctx.long_venue.value}->{ctx.short_venue.value}",
            symbol=ctx.symbol,
            long_venue=ctx.long_venue,
            short_venue=ctx.short_venue,
            long_fill=maker_fill if ctx.maker_leg == Side.BUY else hedge_fill,
            short_fill=hedge_fill if ctx.maker_leg == Side.BUY else maker_fill,
            created_cycle=0,
            now_ms=now_ms,
            deadline_ms=now_ms + self.deadline_ms,
        )

        if residual is not None:
            result.state = EntryState.FAILED_WITH_RESIDUAL
            result.residual_task = residual
            result.pending_entry = self._make_pending_entry(
                ctx, maker_req, hedge_req, now_ms,
                outcome="partial_fill_residual",
                maker_order_id=maker_fill.order_id,
                hedge_order_id=hedge_fill.order_id,
                maker_filled=maker_fill.quantity,
                hedge_filled=hedge_fill.quantity,
            )
            self.journal.append(
                "entry.residual_detected",
                {
                    "position_id": ctx.entry_id,
                    "exposure_quantity": residual.exposure_quantity,
                    "exposure_venue": residual.exposure_venue.value,
                    "maker_client_order_id": maker_req.client_order_id,
                    "hedge_client_order_id": hedge_req.client_order_id,
                },
            )
            return result

        # Check minimum matched ratio
        matched = min(maker_fill.quantity, hedge_fill.quantity)
        target = ctx.long_quantity
        if target > 0 and matched / target < self.min_matched_ratio:
            result.state = EntryState.FAILED_WITH_RESIDUAL
            result.residual_task = split_entry_fill_residual(
                position_id=ctx.entry_id,
                pair_id=f"{ctx.symbol.lower()}:{ctx.long_venue.value}->{ctx.short_venue.value}",
                symbol=ctx.symbol,
                long_venue=ctx.long_venue,
                short_venue=ctx.short_venue,
                long_fill=maker_fill if ctx.maker_leg == Side.BUY else hedge_fill,
                short_fill=hedge_fill if ctx.maker_leg == Side.BUY else maker_fill,
                created_cycle=0,
                now_ms=now_ms,
                deadline_ms=now_ms + self.deadline_ms,
            )
            result.pending_entry = self._make_pending_entry(
                ctx, maker_req, hedge_req, now_ms,
                outcome="below_min_matched_ratio",
                maker_order_id=maker_fill.order_id,
                hedge_order_id=hedge_fill.order_id,
                maker_filled=maker_fill.quantity,
                hedge_filled=hedge_fill.quantity,
            )
            return result

        # Success — build OpenPosition
        position = build_open_position(ctx, maker_fill, hedge_fill, now_ms)
        result.open_position = position
        result.state = EntryState.COMPLETED
        result.route = ExecutionRoute.PASSIVE_INCREMENTAL
        result.maker_fill = maker_fill
        result.hedge_fill = hedge_fill

        self.journal.append(
            "entry.opened",
            {
                "position_id": position.position_id,
                "symbol": position.symbol,
                "long_venue": position.long_venue.value,
                "short_venue": position.short_venue.value,
                "quantity": position.matched_quantity,
                "long_quantity": position.long_quantity,
                "short_quantity": position.short_quantity,
                "long_entry_price": position.long_entry_price,
                "short_entry_price": position.short_entry_price,
                "opened_at_ms": position.opened_at_ms,
                "matched_quantity": position.matched_quantity,
                "current_net_quote": position.current_net_quote,
                "peak_net_quote": position.peak_net_quote,
                "captured_funding_quote": position.captured_funding_quote,
                "second_stage_funding_quote": position.second_stage_funding_quote,
                "long_entry_fee_quote": position.long_entry_fee_quote,
                "short_entry_fee_quote": position.short_entry_fee_quote,
                "funding_captured": position.funding_captured,
                "second_stage_funding_captured": position.second_stage_funding_captured,
                "maker_order_id": maker_fill.order_id,
                "hedge_order_id": hedge_fill.order_id,
                "maker_client_order_id": maker_req.client_order_id,
                "hedge_client_order_id": hedge_req.client_order_id,
            },
        )
        return result

    # ------------------------------------------------------------------
    # PendingEntry factory (V1: creates reconcilable pending state)
    # ------------------------------------------------------------------

    def _make_pending_entry(
        self,
        ctx: EntryContext,
        maker_req: OrderRequest,
        hedge_req: OrderRequest,
        now_ms: int,
        outcome: str = "uncertain",
        maker_order_id: str = "",
        hedge_order_id: str = "",
        maker_filled: float = 0.0,
        hedge_filled: float = 0.0,
    ) -> PendingEntry:
        """Create a PendingEntry for reconciliation after uncertain outcomes.

        V1: after any non-terminal entry outcome, a PendingEntry is created
        so the reconciliation loop can resolve it via venue queries.
        """
        maker_is_long = ctx.maker_leg == Side.BUY
        return PendingEntry(
            pending_id=ctx.entry_id,
            symbol=ctx.symbol,
            long_venue=ctx.long_venue,
            short_venue=ctx.short_venue,
            target_quantity=ctx.long_quantity,
            long_side=Side.BUY if maker_is_long else Side.SELL,
            short_side=Side.SELL if maker_is_long else Side.BUY,
            created_at_ms=now_ms,
            maker_order_id=maker_order_id,
            hedge_order_id=hedge_order_id,
            maker_client_order_id=maker_req.client_order_id or "",
            hedge_client_order_id=hedge_req.client_order_id or "",
            maker_leg_filled=maker_filled,
            hedge_leg_filled=hedge_filled,
            uncertain_outcome=(outcome != "filled"),
            entry_type=ctx.entry_type.value,
            maker_price=maker_req.price or 0.0,
            long_quantity=ctx.long_quantity,
            short_quantity=ctx.short_quantity,
            deadline_ms=now_ms + self.deadline_ms,
            entry_route=ctx.planned_route.value,
            outcome=outcome,
        )

    # ------------------------------------------------------------------
    # Order submission helpers
    # ------------------------------------------------------------------

    async def _submit_maker(
        self, ctx: EntryContext, request: OrderRequest, now_ms: int
    ) -> dict[str, Any]:
        """Submit maker order and classify outcome."""
        self.journal.append(
            "entry.maker_submitted",
            {
                "position_id": ctx.entry_id,
                "symbol": request.symbol,
                "venue": request.venue.value,
                "side": request.side.value,
                "quantity": request.quantity,
                "price": request.price,
                "post_only": request.post_only,
                "client_order_id": request.client_order_id,
                "time_in_force": request.time_in_force.value if request.time_in_force else "",
            },
        )
        return await self._submit_order(request, ctx.entry_id, "maker")

    async def _submit_hedge(
        self, ctx: EntryContext, request: OrderRequest, now_ms: int
    ) -> dict[str, Any]:
        """Submit hedge order and classify outcome."""
        self.journal.append(
            "entry.hedge_submitted",
            {
                "position_id": ctx.entry_id,
                "symbol": request.symbol,
                "venue": request.venue.value,
                "side": request.side.value,
                "quantity": request.quantity,
                "price": request.price,
                "reduce_only": request.reduce_only,
                "client_order_id": request.client_order_id,
                "time_in_force": request.time_in_force.value if request.time_in_force else "",
            },
        )
        return await self._submit_order(request, ctx.entry_id, "hedge")

    async def _submit_order(
        self, request: OrderRequest, position_id: str, leg: str
    ) -> dict[str, Any]:
        """Submit an order through the venue adapter and classify outcome.

        Returns dict with outcome, fill, order_id, and client_order_id.
        """
        adapter = self.adapters.get(request.venue)
        if adapter is None:
            self.journal.append(
                f"entry.{leg}_rejected",
                {
                    "position_id": position_id,
                    "reason": f"no adapter for {request.venue.value}",
                    "client_order_id": request.client_order_id,
                },
            )
            return {"outcome": "rejected", "fill": None, "order_id": ""}

        try:
            fill = await adapter.place_order(request)
            if fill.quantity > 0:
                self.journal.append(
                    f"entry.{leg}_filled",
                    {
                        "position_id": position_id,
                        "order_id": fill.order_id,
                        "client_order_id": request.client_order_id,
                        "quantity": fill.quantity,
                        "price": fill.price,
                        "fee_quote": fill.fee_quote,
                    },
                )
                return {
                    "outcome": "filled",
                    "fill": fill,
                    "order_id": fill.order_id,
                    "journal": [],
                }
            else:
                # Ack-only or zero-fill
                self.journal.append(
                    f"entry.{leg}_uncertain",
                    {
                        "position_id": position_id,
                        "reason": "zero fill quantity",
                        "client_order_id": request.client_order_id,
                    },
                )
                return {
                    "outcome": "uncertain",
                    "fill": None,
                    "order_id": getattr(fill, "order_id", ""),
                }

        except OrderSubmitError as e:
            if e.is_rejected:
                self.journal.append(
                    f"entry.{leg}_rejected",
                    {
                        "position_id": position_id,
                        "reason": str(e),
                        "client_order_id": request.client_order_id,
                    },
                )
                return {"outcome": "rejected", "fill": None, "order_id": ""}
            else:
                self.journal.append(
                    f"entry.{leg}_uncertain",
                    {
                        "position_id": position_id,
                        "reason": str(e),
                        "client_order_id": request.client_order_id,
                    },
                )
                return {"outcome": "uncertain", "fill": None, "order_id": ""}

        except Exception as e:
            self.journal.append(
                f"entry.{leg}_uncertain",
                {
                    "position_id": position_id,
                    "reason": str(e),
                    "client_order_id": request.client_order_id,
                },
            )
            return {"outcome": "uncertain", "fill": None, "order_id": ""}


# ---------------------------------------------------------------------------
# Convenience entry point
# ---------------------------------------------------------------------------


async def execute_entry(
    entry_id: str,
    symbol: str,
    long_venue: Venue,
    short_venue: Venue,
    quantity: float,
    long_price_hint: float,
    short_price_hint: float,
    maker_leg: Side,
    adapters: dict[Venue, VenueAdapter],
    journal: Journal,
    entry_type: EntryType = EntryType.STANDARD_DUAL_TAKER,
    **kwargs,
) -> EntryExecutionResult:
    """Convenience wrapper: build EntryContext → execute."""
    ctx = EntryContext(
        entry_id=entry_id,
        symbol=symbol,
        long_venue=long_venue,
        short_venue=short_venue,
        long_quantity=quantity,
        short_quantity=quantity,
        long_price_hint=long_price_hint,
        short_price_hint=short_price_hint,
        maker_leg=maker_leg,
        entry_type=entry_type,
        created_at_ms=int(time.time() * 1000),
    )
    executor = EntrySyncExecutor(
        adapters=adapters,
        journal=journal,
        config_overrides=kwargs,
    )
    return await executor.execute(ctx)


# ---------------------------------------------------------------------------
# V1 pending entry hedge in-situ driver
# ---------------------------------------------------------------------------
# Rust reference: src/execution_core/entry_sync.rs:5459+ drive_pending_entry_hedge()
#
# This is the V1-equivalent hedge driver. Unlike execute() which creates a full
# maker→hedge flow, drive_pending_entry_hedge() amends or cancel-replaces the
# EXISTING maker order within a pending entry. It does NOT create a new entry
# flow, a new entry_id, or submit a new hedge.
#
# Valid actions:
#   "reprice"       → amend the maker order price
#   "cancel_replace" → cancel + re-submit the maker order at new price
# ---------------------------------------------------------------------------


@dataclass
class HedgeDriveResult:
    """Outcome of a pending hedge drive operation."""
    action: str  # "reprice", "cancel_replace", "noop"
    outcome: str  # "applied", "rejected", "uncertain", "noop"
    detail: str = ""
    order_id: str = ""
    new_price: float = 0.0


async def drive_pending_entry_hedge(
    entry_id: str,
    pending,
    new_price: float,
    old_price: float,
    action: str,
    now_ms: int,
    adapters: dict[Venue, VenueAdapter],
    journal: Journal,
    maker_leg: Side,
    symbol: str,
    long_venue: Venue,
    short_venue: Venue,
    quantity: float = 0.0,
) -> HedgeDriveResult:
    """Drive a pending entry hedge in-situ: amend or cancel-replace the maker order.

    V1 semantics:
    - "reprice": amend the existing maker order to new_price
    - "cancel_replace": cancel existing maker + submit new maker at new_price
    - Does NOT create a new entry flow or submit a hedge
    - Does NOT change the entry_id
    - Returns HedgeDriveResult with outcome classification

    This is called by the maker-event lane when local-L2 events trigger
    repricing of a pending passive maker order.
    """
    from lightfee.core.domain import OrderRequest

    if action == "noop":
        return HedgeDriveResult(action="noop", outcome="noop")

    maker_venue = long_venue if maker_leg == Side.BUY else short_venue
    adapter = adapters.get(maker_venue)
    if adapter is None:
        journal.append(
            "entry.hedge_drive_no_adapter",
            {"entry_id": entry_id, "venue": maker_venue.value, "action": action},
        )
        return HedgeDriveResult(action=action, outcome="rejected",
                               detail=f"no adapter for {maker_venue.value}")

    qty = quantity if quantity > 0 else getattr(pending, 'long_quantity', 0) or getattr(pending, 'target_quantity', 0)
    if qty <= 0:
        return HedgeDriveResult(action=action, outcome="rejected",
                               detail="zero quantity")

    # Build an amend or cancel-replace request for the existing maker order
    maker_order_id = getattr(pending, 'maker_order_id', '')
    post_only = True  # V1: passive maker orders are always post-only

    if action == "reprice" and maker_order_id:
        # Amend existing maker order price
        amend_req = OrderRequest(
            venue=maker_venue,
            symbol=symbol,
            side=maker_leg,
            quantity=qty,
            price=new_price,
            post_only=post_only,
            order_id=maker_order_id,
            reduce_only=False,
        )
        try:
            fill = await adapter.amend_order(amend_req)
            if fill.quantity > 0:
                journal.append(
                    "entry.hedge_drive_reprice",
                    {"entry_id": entry_id, "action": "reprice",
                     "old_price": old_price, "new_price": new_price,
                     "order_id": fill.order_id},
                )
                return HedgeDriveResult(action="reprice", outcome="applied",
                                       order_id=fill.order_id, new_price=new_price)
            else:
                return HedgeDriveResult(action="reprice", outcome="uncertain",
                                       detail="amend ack-only, fill not confirmed")
        except OrderSubmitError as e:
            if e.is_rejected:
                return HedgeDriveResult(action="reprice", outcome="rejected",
                                       detail=str(e))
            return HedgeDriveResult(action="reprice", outcome="uncertain",
                                   detail=str(e))
        except Exception as e:
            return HedgeDriveResult(action="reprice", outcome="uncertain",
                                   detail=str(e))

    elif action == "cancel_replace":
        # Cancel existing + submit new maker at new_price
        cancel_req = OrderRequest(
            venue=maker_venue,
            symbol=symbol,
            side=maker_leg,
            quantity=0.0,  # cancel
            price=0.0,
            post_only=False,
            order_id=maker_order_id,
            reduce_only=False,
        )
        try:
            await adapter.cancel_order(cancel_req)
        except Exception:
            pass  # Best-effort cancel; proceed to re-submit

        new_req = OrderRequest(
            venue=maker_venue,
            symbol=symbol,
            side=maker_leg,
            quantity=qty,
            price=new_price,
            post_only=post_only,
            reduce_only=False,
        )
        try:
            fill = await adapter.place_order(new_req)
            if fill.quantity > 0:
                journal.append(
                    "entry.hedge_drive_cancel_replace",
                    {"entry_id": entry_id, "action": "cancel_replace",
                     "old_price": old_price, "new_price": new_price,
                     "order_id": fill.order_id},
                )
                return HedgeDriveResult(action="cancel_replace", outcome="applied",
                                       order_id=fill.order_id, new_price=new_price)
            else:
                return HedgeDriveResult(action="cancel_replace", outcome="uncertain",
                                       detail="new order ack-only after cancel")
        except OrderSubmitError as e:
            if e.is_rejected:
                return HedgeDriveResult(action="cancel_replace", outcome="rejected",
                                       detail=str(e))
            return HedgeDriveResult(action="cancel_replace", outcome="uncertain",
                                   detail=str(e))
        except Exception as e:
            return HedgeDriveResult(action="cancel_replace", outcome="uncertain",
                                   detail=str(e))

    else:
        return HedgeDriveResult(action=action, outcome="noop",
                               detail=f"unknown action: {action}")
