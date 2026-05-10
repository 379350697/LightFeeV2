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
        """Execute a full entry flow: maker → hedge, with outcome handling."""
        now_ms = int(time.time() * 1000)
        result = EntryExecutionResult(
            route=ExecutionRoute.REJECTED,
            state=EntryState.IDLE,
        )

        # Build order requests
        maker_req, hedge_req = build_entry_orders(ctx)

        # --- Phase 1: Submit maker ---
        maker_result = await self._submit_maker(ctx, maker_req, now_ms)
        result.journal_entries.extend(maker_result.get("journal", []))
        result.maker_fill = maker_result.get("fill")

        if maker_result["outcome"] == "rejected":
            result.state = EntryState.FAILED
            result.route = ExecutionRoute.REJECTED
            return result

        if maker_result["outcome"] == "uncertain":
            result.state = EntryState.FAILED_WITH_RESIDUAL
            result.has_uncertainty = True
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
            return result

        if hedge_result["outcome"] == "uncertain":
            result.state = EntryState.FAILED_WITH_RESIDUAL
            result.has_uncertainty = True
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
            self.journal.append(
                "entry.residual_detected",
                {
                    "position_id": ctx.entry_id,
                    "exposure_quantity": residual.exposure_quantity,
                    "exposure_venue": residual.exposure_venue.value,
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
            return result

        # Success — build OpenPosition
        position = build_open_position(ctx, maker_fill, hedge_fill, now_ms)
        result.open_position = position
        result.state = EntryState.COMPLETED
        result.route = ExecutionRoute.PASSIVE_INCREMENTAL
        result.maker_fill = maker_fill
        result.hedge_fill = hedge_fill

        self.journal.append(
            "entry.completed",
            {
                "position_id": ctx.entry_id,
                "symbol": ctx.symbol,
                "long_quantity": position.long_quantity,
                "short_quantity": position.short_quantity,
                "matched_quantity": position.matched_quantity,
            },
        )
        return result

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
            },
        )
        return await self._submit_order(request, ctx.entry_id, "hedge")

    async def _submit_order(
        self, request: OrderRequest, position_id: str, leg: str
    ) -> dict[str, Any]:
        """Submit an order through the venue adapter and classify outcome."""
        adapter = self.adapters.get(request.venue)
        if adapter is None:
            self.journal.append(
                f"entry.{leg}_rejected",
                {"position_id": position_id, "reason": f"no adapter for {request.venue.value}"},
            )
            return {"outcome": "rejected", "fill": None}

        try:
            fill = await adapter.place_order(request)
            if fill.quantity > 0:
                self.journal.append(
                    f"entry.{leg}_filled",
                    {
                        "position_id": position_id,
                        "order_id": fill.order_id,
                        "quantity": fill.quantity,
                        "price": fill.price,
                        "fee_quote": fill.fee_quote,
                    },
                )
                return {"outcome": "filled", "fill": fill, "journal": []}
            else:
                # Ack-only or zero-fill
                self.journal.append(
                    f"entry.{leg}_uncertain",
                    {"position_id": position_id, "reason": "zero fill quantity"},
                )
                return {"outcome": "uncertain", "fill": None}

        except OrderSubmitError as e:
            if e.is_rejected:
                self.journal.append(
                    f"entry.{leg}_rejected",
                    {"position_id": position_id, "reason": str(e)},
                )
                return {"outcome": "rejected", "fill": None}
            else:
                self.journal.append(
                    f"entry.{leg}_uncertain",
                    {"position_id": position_id, "reason": str(e)},
                )
                return {"outcome": "uncertain", "fill": None}

        except Exception as e:
            self.journal.append(
                f"entry.{leg}_uncertain",
                {"position_id": position_id, "reason": str(e)},
            )
            return {"outcome": "uncertain", "fill": None}


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
