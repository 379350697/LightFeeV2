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
from lightfee.core.domain import OrderFill, OrderRequest, PassiveOrderState, Side, Venue
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.core.exchange_errors import (
    RequestContext,
    build_evidence_from_order_submit_error,
    build_fallback_evidence,
)
from lightfee.engine.entry import (
    EntryContext,
    EntryState,
    EntryType,
    advance_entry_state,
    build_entry_orders,
    build_open_position,
    generate_review_id,
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
from lightfee.engine.state import (
    OpenPosition,
    PendingEntry,
    PendingEntryPassivePhaseState,
    PendingPassiveOrder,
)
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
    reject_reason: str = ""
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
        # --- V1 active entry/passive-maker knobs ---
        self.maker_entry_max_reposts: int = overrides.get("maker_entry_max_reposts", 0)
        self.maker_entry_rest_timeout_ms: int = overrides.get(
            "maker_entry_rest_timeout_ms", 6000
        )
        self.pending_entry_zero_fill_terminal_cooldown_ms: int = overrides.get(
            "pending_entry_zero_fill_terminal_cooldown_ms", 0
        )
        # --- V1 review observability ---
        self.review_observability_enabled: bool = overrides.get(
            "review_observability_enabled", False
        )

    # ------------------------------------------------------------------
    # Main execution entry point
    # ------------------------------------------------------------------

    async def execute(self, ctx: EntryContext) -> EntryExecutionResult:
        """Execute a full entry flow: maker → hedge, with outcome handling.

        V1 parity additions:
        - Creates PendingEntry on uncertain outcomes for reconciliation
        - Tracks clientOrderId in all journal events
        - Propagates TIF/reduce-only through order requests
        - Enforces maker_entry_max_reposts limit
        - Generates and propagates review_id when review_observability_enabled
        """
        now_ms = int(time.time() * 1000)
        result = EntryExecutionResult(
            route=ExecutionRoute.REJECTED,
            state=EntryState.IDLE,
        )

        # --- V1: check repost limit ---
        current_repost_count = 0
        pending_entries = self.state.get("pending_entries", {})
        if ctx.entry_id in pending_entries:
            existing = pending_entries[ctx.entry_id]
            current_repost_count = getattr(existing, "repost_count", 0) if hasattr(existing, "repost_count") else existing.get("repost_count", 0) if isinstance(existing, dict) else 0

        if self.maker_entry_max_reposts > 0 and current_repost_count >= self.maker_entry_max_reposts:
            self.journal.append(
                "entry.aborted",
                {
                    "position_id": ctx.entry_id,
                    "internal_entry_id": ctx.entry_id,
                    "reason": f"max reposts reached ({current_repost_count}/{self.maker_entry_max_reposts})",
                },
            )
            result.state = EntryState.FAILED
            result.route = ExecutionRoute.REJECTED
            return result

        # --- V1: zero-fill terminal cooldown check ---
        if self.pending_entry_zero_fill_terminal_cooldown_ms > 0 and ctx.entry_id in pending_entries:
            existing = pending_entries[ctx.entry_id]
            zero_fill_since = (
                getattr(existing, "zero_fill_since_ms", 0)
                if hasattr(existing, "zero_fill_since_ms")
                else existing.get("zero_fill_since_ms", 0) if isinstance(existing, dict) else 0
            )
            if zero_fill_since > 0 and (now_ms - zero_fill_since) >= self.pending_entry_zero_fill_terminal_cooldown_ms:
                self.journal.append(
                    "entry.aborted",
                    {
                        "position_id": ctx.entry_id,
                        "internal_entry_id": ctx.entry_id,
                        "reason": f"zero-fill terminal cooldown expired ({now_ms - zero_fill_since}ms >= {self.pending_entry_zero_fill_terminal_cooldown_ms}ms)",
                    },
                )
                result.state = EntryState.FAILED
                result.route = ExecutionRoute.REJECTED
                return result

        # --- V1: review observability ---
        review_id: str | None = None
        if self.review_observability_enabled:
            review_id = generate_review_id()
            self.journal.append(
                "review.assigned",
                {
                    "position_id": ctx.entry_id,
                    "review_id": review_id,
                },
            )

        # Build order requests with clientOrderId, TIF, reduce_only
        maker_req, hedge_req = build_entry_orders(ctx)

        # --- Phase 1: Submit maker ---
        maker_result = await self._submit_maker(ctx, maker_req, now_ms)
        result.journal_entries.extend(maker_result.get("journal", []))
        result.maker_fill = maker_result.get("fill")

        # V2: post_only maker is resting — create pending entry, no hedge yet
        if maker_result["outcome"] == "resting":
            ack = maker_result.get("ack")
            result.state = EntryState.MAKER_RESTING
            result.route = ExecutionRoute.PASSIVE_INCREMENTAL
            result.pending_entry = self._make_pending_entry(
                ctx, maker_req, hedge_req, now_ms,
                outcome="maker_resting",
                maker_order_id=ack.order_id if ack else "",
                hedge_order_id="",
                passive_order_ack=ack,
            )
            return result

        if maker_result["outcome"] == "rejected":
            result.state = EntryState.FAILED
            result.route = ExecutionRoute.REJECTED
            result.reject_reason = maker_result.get("reason", "maker rejected")
            self.journal.append(
                "entry.aborted",
                {
                    "position_id": ctx.entry_id,
                    "internal_entry_id": ctx.entry_id,
                    "reason": maker_result.get("reason", "maker rejected"),
                    "maker_client_order_id": maker_req.client_order_id,
                    "hedge_client_order_id": hedge_req.client_order_id,
                },
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
        position = build_open_position(ctx, maker_fill, hedge_fill, now_ms, review_id=review_id)
        result.open_position = position
        result.state = EntryState.COMPLETED
        result.route = ExecutionRoute.PASSIVE_INCREMENTAL
        result.maker_fill = maker_fill
        result.hedge_fill = hedge_fill

        # V1: entry.opened is a critical event — synchronous durability
        self.journal.append_critical(
            now_ms, "entry.opened",
            {
                "position_id": position.position_id,
                "internal_entry_id": position.position_id,
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
                "funding_timestamp_ms": position.funding_timestamp_ms,
                "second_funding_timestamp_ms": position.second_funding_timestamp_ms,
                "opportunity_type": position.opportunity_type,
                "second_stage_enabled_at_entry": position.second_stage_enabled_at_entry,
                "exit_after_first_stage": position.exit_after_first_stage,
                "funding_edge_bps_entry": position.funding_edge_bps_entry,
                "total_funding_edge_bps_entry": position.total_funding_edge_bps_entry,
                "expected_edge_bps_entry": position.expected_edge_bps_entry,
                "long_entry_fee_quote": position.long_entry_fee_quote,
                "short_entry_fee_quote": position.short_entry_fee_quote,
                "funding_captured": position.funding_captured,
                "second_stage_funding_captured": position.second_stage_funding_captured,
                "maker_order_id": maker_fill.order_id,
                "hedge_order_id": hedge_fill.order_id,
                "maker_client_order_id": maker_req.client_order_id,
                "hedge_client_order_id": hedge_req.client_order_id,
                "review_id": review_id or "",
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
        repost_count: int = 0,
        passive_order_ack: Any = None,  # PassiveOrderAck from submit_passive_order
    ) -> PendingEntry:
        """Create a PendingEntry for reconciliation after uncertain outcomes.

        V1: after any non-terminal entry outcome, a PendingEntry is created
        so the reconciliation loop can resolve it via venue queries.
        Repost count is incremented from the previous pending entry if it exists.
        Zero-fill timing starts when both legs have zero cumulative fills.
        """
        # Determine repost count and zero-fill state from existing pending entry
        pending_entries = self.state.get("pending_entries", {})
        existing = pending_entries.get(ctx.entry_id)
        if existing is not None and repost_count == 0:
            repost_count = (
                getattr(existing, "repost_count", 0)
                if hasattr(existing, "repost_count")
                else existing.get("repost_count", 0) if isinstance(existing, dict) else 0
            )
        next_repost_count = repost_count + 1
        maker_filled_total = maker_filled
        hedge_filled_total = hedge_filled
        if existing is not None:
            prev_maker = (
                getattr(existing, "maker_leg_filled", 0)
                if hasattr(existing, "maker_leg_filled")
                else existing.get("maker_leg_filled", 0) if isinstance(existing, dict) else 0
            )
            prev_hedge = (
                getattr(existing, "hedge_leg_filled", 0)
                if hasattr(existing, "hedge_leg_filled")
                else existing.get("hedge_leg_filled", 0) if isinstance(existing, dict) else 0
            )
            maker_filled_total = maker_filled or prev_maker
            hedge_filled_total = hedge_filled or prev_hedge

        # Zero-fill detection: both legs still zero
        zero_fill_since_ms = 0
        if maker_filled_total == 0.0 and hedge_filled_total == 0.0:
            prev_zero = 0
            if existing is not None:
                prev_zero = (
                    getattr(existing, "zero_fill_since_ms", 0)
                    if hasattr(existing, "zero_fill_since_ms")
                    else existing.get("zero_fill_since_ms", 0) if isinstance(existing, dict) else 0
                )
            zero_fill_since_ms = prev_zero if prev_zero > 0 else now_ms

        maker_is_long = ctx.maker_leg == Side.BUY

        # --- V1: build PendingPassiveOrder when maker order is resting ---
        passive_order: Optional[PendingPassiveOrder] = None
        phase_state: PendingEntryPassivePhaseState | None = None
        passive_attempt_count = 0
        if passive_order_ack is not None:
            ack_accepted_at_ms = getattr(passive_order_ack, "accepted_at_ms", 0) or now_ms
            ack_order_id = getattr(passive_order_ack, "order_id", "") or maker_order_id
            ack_cid = getattr(passive_order_ack, "client_order_id", "") or maker_req.client_order_id or ""
            ack_price = getattr(passive_order_ack, "price", 0.0) or maker_req.price or 0.0
            ack_qty = getattr(passive_order_ack, "quantity", 0.0) or ctx.long_quantity
            ack_state = getattr(passive_order_ack, "state", None)
            rest_timeout_ms = self.maker_entry_rest_timeout_ms
            if rest_timeout_ms <= 0:
                rest_timeout_ms = 6000
            passive_order = PendingPassiveOrder(
                order_id=ack_order_id,
                client_order_id=ack_cid,
                limit_price=float(ack_price) if ack_price > 0 else None,
                target_quantity=float(ack_qty),
                accepted_at_ms=ack_accepted_at_ms,
                timeout_at_ms=ack_accepted_at_ms + rest_timeout_ms,
                cancel_requested_at_ms=0,
                last_progress_state=ack_state if ack_state is not None else PassiveOrderState.UNKNOWN,
            )
            maker_leg_label = "long" if maker_is_long else "short"
            phase_state = PendingEntryPassivePhaseState(
                execution_kind="entry",
                preferred_maker_leg=maker_leg_label,
                active_maker_leg=maker_leg_label,
                phase="high_slippage_maker",
                cycle_attempt=1,
                phase_started_at_ms=ack_accepted_at_ms,
                cycle_started_at_ms=ack_accepted_at_ms,
            )
            passive_attempt_count = 1

        return PendingEntry(
            pending_id=ctx.entry_id,
            symbol=ctx.symbol,
            long_venue=ctx.long_venue,
            short_venue=ctx.short_venue,
            target_quantity=ctx.long_quantity,
            # The arbitrage direction is an invariant: long leg always buys
            # and short leg always sells.  maker_is_long only selects which
            # canonical leg posts passively; it must never invert exposure.
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=now_ms,
            maker_order_id=maker_order_id,
            hedge_order_id=hedge_order_id,
            maker_client_order_id=maker_req.client_order_id or "",
            hedge_client_order_id=hedge_req.client_order_id or "",
            maker_leg_filled=maker_filled_total,
            hedge_leg_filled=hedge_filled_total,
            uncertain_outcome=(outcome not in {"filled", "rejected"}),
            entry_type=ctx.entry_type.value,
            maker_price=maker_req.price or 0.0,
            long_quantity=ctx.long_quantity,
            short_quantity=ctx.short_quantity,
            deadline_ms=now_ms + self.deadline_ms,
            entry_route=ctx.planned_route.value,
            outcome=outcome,
            opportunity_type=ctx.opportunity_type,
            funding_timestamp_ms=ctx.funding_timestamp_ms,
            first_funding_timestamp_ms=ctx.first_funding_timestamp_ms,
            long_funding_timestamp_ms=ctx.long_funding_timestamp_ms,
            short_funding_timestamp_ms=ctx.short_funding_timestamp_ms,
            second_funding_timestamp_ms=ctx.second_funding_timestamp_ms,
            first_funding_leg=ctx.first_funding_leg,
            funding_edge_bps_entry=ctx.funding_edge_bps_entry,
            total_funding_edge_bps_entry=ctx.total_funding_edge_bps_entry,
            expected_edge_bps_entry=ctx.expected_edge_bps_entry,
            worst_case_edge_bps_entry=ctx.worst_case_edge_bps_entry,
            entry_maker_leg=ctx.entry_maker_leg,
            exit_maker_leg=ctx.exit_maker_leg,
            entry_cross_bps_entry=ctx.entry_cross_bps_entry,
            fee_bps_entry=ctx.fee_bps_entry,
            entry_slippage_bps_entry=ctx.entry_slippage_bps_entry,
            transfer_bias_bps_entry=ctx.transfer_bias_bps_entry,
            transfer_state_at_entry=ctx.transfer_state_at_entry,
            entry_liquidity_source_at_entry=ctx.entry_liquidity_source_at_entry,
            long_volume_24h_quote_at_entry=ctx.long_volume_24h_quote_at_entry,
            short_volume_24h_quote_at_entry=ctx.short_volume_24h_quote_at_entry,
            long_open_interest_quote_at_entry=ctx.long_open_interest_quote_at_entry,
            short_open_interest_quote_at_entry=ctx.short_open_interest_quote_at_entry,
            long_entry_vwap=ctx.long_entry_vwap,
            short_entry_vwap=ctx.short_entry_vwap,
            entry_capacity_constrained=ctx.entry_capacity_constrained,
            entry_target_quantity=ctx.entry_target_quantity,
            long_max_executable_quantity=ctx.long_max_executable_quantity,
            short_max_executable_quantity=ctx.short_max_executable_quantity,
            entry_max_executable_quantity=ctx.entry_max_executable_quantity,
            entry_depth_shortfall_quantity=ctx.entry_depth_shortfall_quantity,
            entry_max_executable_notional_quote=ctx.entry_max_executable_notional_quote,
            entry_depth_capped_at_entry=ctx.entry_depth_capped_at_entry,
            advisories=list(ctx.advisories),
            blocked_reasons=list(ctx.blocked_reasons),
            exit_after_first_stage=ctx.exit_after_first_stage,
            phase_state=phase_state,
            passive_attempt_count=passive_attempt_count,
            repost_count=next_repost_count,
            repost_attempt_count=0 if passive_order_ack is not None else next_repost_count,
            zero_fill_since_ms=zero_fill_since_ms,
            maker_leg="long" if maker_is_long else "short",
            passive_order=passive_order,
        )

    # ------------------------------------------------------------------
    # Order submission helpers
    # ------------------------------------------------------------------

    async def _submit_maker(
        self, ctx: EntryContext, request: OrderRequest, now_ms: int
    ) -> dict[str, Any]:
        """Submit maker order and classify outcome."""
        self.journal.append(
            "order.submitted",
            {
                "position_id": ctx.entry_id,
                "internal_entry_id": ctx.entry_id,
                "leg": "maker",
                "symbol": request.symbol,
                "venue": request.venue.value,
                "side": request.side.value,
                "quantity": request.quantity,
                "price": request.price,
                "post_only": request.post_only,
                "is_maker": True,
                "client_order_id": request.client_order_id,
                "time_in_force": request.time_in_force.value if request.time_in_force else "",
            },
        )
        return await self._submit_order(request, ctx.entry_id, "maker", now_ms)

    async def _submit_hedge(
        self, ctx: EntryContext, request: OrderRequest, now_ms: int
    ) -> dict[str, Any]:
        """Submit hedge order and classify outcome."""
        self.journal.append(
            "order.submitted",
            {
                "position_id": ctx.entry_id,
                "internal_entry_id": ctx.entry_id,
                "leg": "hedge",
                "symbol": request.symbol,
                "venue": request.venue.value,
                "side": request.side.value,
                "quantity": request.quantity,
                "price": request.price,
                "reduce_only": request.reduce_only,
                "is_maker": False,
                "client_order_id": request.client_order_id,
                "time_in_force": request.time_in_force.value if request.time_in_force else "",
            },
        )
        return await self._submit_order(request, ctx.entry_id, "hedge", now_ms)

    async def _submit_order(
        self, request: OrderRequest, position_id: str, leg: str,
        submit_started_at_ms: int = 0,
    ) -> dict[str, Any]:
        """Submit an order through the venue adapter and classify outcome.

        V2 fix: post_only maker orders go through submit_passive_order.
        Hedge/taker orders continue to use place_order for IOC fills.

        Returns dict with outcome, fill/ack, order_id, and client_order_id.
        """
        import time as _time
        adapter = self.adapters.get(request.venue)
        is_maker = (leg == "maker")
        if adapter is None:
            self.journal.append(
                "order.rejected",
                {
                    "position_id": position_id,
                    "internal_entry_id": position_id,
                    "leg": leg,
                    "venue": request.venue.value,
                    "symbol": request.symbol,
                    "reason": f"no adapter for {request.venue.value}",
                    "client_order_id": request.client_order_id,
                    "is_maker": is_maker,
                },
            )
            return {
                "outcome": "rejected",
                "fill": None,
                "order_id": "",
                "reason": str(e),
            }

        # V2: post_only maker orders go through passive submit (ACK, not fill)
        if is_maker and request.post_only:
            return await self._submit_passive_order(request, position_id, leg, adapter)

        try:
            fill = await adapter.place_order(request)
            self._flush_adapter_order_diagnostics(adapter)
            latency_ms = 0
            if submit_started_at_ms > 0 and fill.filled_at_ms > 0:
                latency_ms = fill.filled_at_ms - submit_started_at_ms
            if fill.quantity > 0:
                # V1 parity: execution.order_filled includes venue/symbol/side/filled_at_ms
                self.journal.append(
                    "order.filled",
                    {
                        "position_id": position_id,
                        "internal_entry_id": position_id,
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
                        "latency_ms": latency_ms,
                        "is_maker": is_maker,
                    },
                )
                return {
                    "outcome": "filled",
                    "fill": fill,
                    "order_id": fill.order_id,
                    "journal": [],
                }
            else:
                self.journal.append(
                    "order.uncertain",
                    {
                        "position_id": position_id,
                        "internal_entry_id": position_id,
                        "leg": leg,
                        "venue": request.venue.value,
                        "symbol": request.symbol,
                        "reason": "zero fill quantity",
                        "client_order_id": request.client_order_id,
                        "is_maker": is_maker,
                        "latency_ms": latency_ms,
                    },
                )
                return {
                    "outcome": "uncertain",
                    "fill": None,
                    "order_id": getattr(fill, "order_id", ""),
                }

        except OrderSubmitError as e:
            self._flush_adapter_order_diagnostics(adapter)
            req_ctx = RequestContext.from_order_request(request)
            evidence = build_evidence_from_order_submit_error(
                e,
                venue=request.venue.value,
                operation="submit_passive_order" if is_maker else "place_order",
                endpoint="",
                request_context=req_ctx,
            )
            if e.is_rejected:
                self.journal.append(
                    "order.rejected",
                    {
                        "position_id": position_id,
                        "internal_entry_id": position_id,
                        "leg": leg,
                        "venue": request.venue.value,
                        "symbol": request.symbol,
                        "reason": str(e),
                        "client_order_id": request.client_order_id,
                        "is_maker": is_maker,
                        "exchange_error": evidence.to_dict(),
                        "request_context": req_ctx.to_dict(),
                        "evidence_completeness": evidence.evidence_completeness,
                    },
                )
                return {
                    "outcome": "rejected",
                    "fill": None,
                    "order_id": "",
                    "reason": str(e),
                    "exchange_error": evidence.to_dict(),
                }
            else:
                self.journal.append(
                    "order.uncertain",
                    {
                        "position_id": position_id,
                        "internal_entry_id": position_id,
                        "leg": leg,
                        "venue": request.venue.value,
                        "symbol": request.symbol,
                        "reason": str(e),
                        "client_order_id": request.client_order_id,
                        "is_maker": is_maker,
                        "exchange_error": evidence.to_dict(),
                        "request_context": req_ctx.to_dict(),
                        "evidence_completeness": evidence.evidence_completeness,
                    },
                )
                return {"outcome": "uncertain", "fill": None, "order_id": ""}

        except Exception as e:
            self._flush_adapter_order_diagnostics(adapter)
            req_ctx = RequestContext.from_order_request(request)
            evidence = build_fallback_evidence(
                e,
                venue=request.venue.value,
                operation="submit_passive_order" if is_maker else "place_order",
                request_context=req_ctx,
            )
            self.journal.append(
                "order.uncertain",
                {
                    "position_id": position_id,
                    "internal_entry_id": position_id,
                    "leg": leg,
                    "venue": request.venue.value,
                    "symbol": request.symbol,
                    "reason": str(e),
                    "client_order_id": request.client_order_id,
                    "is_maker": is_maker,
                    "exchange_error": evidence.to_dict(),
                    "request_context": req_ctx.to_dict(),
                    "evidence_completeness": evidence.evidence_completeness,
                },
            )
            return {"outcome": "uncertain", "fill": None, "order_id": ""}

    def _flush_adapter_order_diagnostics(self, adapter) -> None:
        transport = getattr(adapter, "_transport", adapter)
        drain = getattr(transport, "drain_order_diagnostics", None)
        if not callable(drain):
            return
        for event in drain():
            kind = event.get("kind", "")
            payload = event.get("payload", {})
            if isinstance(kind, str) and isinstance(payload, dict):
                self.journal.append(kind, payload)

    async def _submit_passive_order(
        self, request: OrderRequest, position_id: str, leg: str, adapter
    ) -> dict[str, Any]:
        """Submit a post_only maker order via submit_passive_order.

        Returns ack-based outcomes: resting (ACK received), rejected, or uncertain.

        V2 fix: flushes transport diagnostics so order.submit_attempt and
        order.submit_result (with normalization evidence) land in the journal.
        """
        try:
            ack = await adapter.submit_passive_order(request)
            self._flush_adapter_order_diagnostics(adapter)
            self.journal.append(
                "order.passive_submitted",
                {
                    "position_id": position_id,
                    "internal_entry_id": position_id,
                    "leg": leg,
                    "venue": request.venue.value,
                    "symbol": request.symbol,
                    "order_id": ack.order_id,
                    "client_order_id": ack.client_order_id,
                    "price": ack.price,
                    "quantity": ack.quantity,
                    "is_maker": True,
                },
            )
            return {"outcome": "resting", "ack": ack, "order_id": ack.order_id}

        except OrderSubmitError as e:
            self._flush_adapter_order_diagnostics(adapter)
            req_ctx = RequestContext.from_order_request(request)
            evidence = build_evidence_from_order_submit_error(
                e,
                venue=request.venue.value,
                operation="submit_passive_order",
                endpoint="",
                request_context=req_ctx,
            )
            if e.is_rejected:
                self.journal.append(
                    "order.rejected",
                    {
                        "position_id": position_id,
                        "internal_entry_id": position_id,
                        "leg": leg,
                        "venue": request.venue.value,
                        "symbol": request.symbol,
                        "reason": str(e),
                        "client_order_id": request.client_order_id,
                        "is_maker": True,
                        "exchange_error": evidence.to_dict(),
                        "request_context": req_ctx.to_dict(),
                        "evidence_completeness": evidence.evidence_completeness,
                    },
                )
                return {
                    "outcome": "rejected",
                    "fill": None,
                    "order_id": "",
                    "reason": str(e),
                    "exchange_error": evidence.to_dict(),
                }
            else:
                self.journal.append(
                    "order.uncertain",
                    {
                        "position_id": position_id,
                        "internal_entry_id": position_id,
                        "leg": leg,
                        "venue": request.venue.value,
                        "symbol": request.symbol,
                        "reason": str(e),
                        "client_order_id": request.client_order_id,
                        "is_maker": True,
                        "exchange_error": evidence.to_dict(),
                        "request_context": req_ctx.to_dict(),
                        "evidence_completeness": evidence.evidence_completeness,
                    },
                )
                return {"outcome": "uncertain", "fill": None, "order_id": ""}

        except Exception as e:
            self._flush_adapter_order_diagnostics(adapter)
            req_ctx = RequestContext.from_order_request(request)
            evidence = build_fallback_evidence(
                e,
                venue=request.venue.value,
                operation="submit_passive_order",
                request_context=req_ctx,
            )
            self.journal.append(
                "order.uncertain",
                {
                    "position_id": position_id,
                    "internal_entry_id": position_id,
                    "leg": leg,
                    "venue": request.venue.value,
                    "symbol": request.symbol,
                    "reason": str(e),
                    "client_order_id": request.client_order_id,
                    "is_maker": True,
                    "exchange_error": evidence.to_dict(),
                    "request_context": req_ctx.to_dict(),
                    "evidence_completeness": evidence.evidence_completeness,
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


def _adapter_supports_amend(adapter) -> bool:
    """V1 passive_order_supports_amend (entry_sync.rs:1534).

    Returns True if the adapter explicitly implements amend_order (the subclass
    overrides VenueAdapter.amend_order rather than inheriting the default
    NotImplementedError stub).
    """
    if adapter is None:
        return False
    # Is amend_order defined directly on this class (not just inherited)?
    cls = type(adapter)
    method = cls.__dict__.get('amend_order')
    if method is None:
        return False
    # V1: If the implementation only raises NotImplementedError (like the
    # VenueAdapter base), treat as unsupported.
    try:
        import inspect
        source = inspect.getsource(method)
        body = [l.strip() for l in source.split('\n')[1:]
                if l.strip() and not l.strip().startswith('"""')
                and not l.strip().startswith('#')]
        if len(body) == 1 and ('raise NotImplementedError' in body[0] or body[0] == '...'):
            return False
    except (OSError, TypeError):
        pass
    return True


def _flush_adapter_diagnostics(adapter, journal) -> None:
    """Flush transport order diagnostics into the journal.

    V1: after every adapter order operation, drain transport diagnostics
    so order.submit_attempt, order.submit_result, and normalization evidence
    land in the journal observability stream.
    """
    transport = getattr(adapter, "_transport", adapter)
    drain = getattr(transport, "drain_order_diagnostics", None)
    if not callable(drain):
        return
    for event in drain():
        kind = event.get("kind", "")
        payload = event.get("payload", {})
        if isinstance(kind, str) and isinstance(payload, dict):
            journal.append(kind, payload)


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
        # V1: passive_order_supports_amend check (entry_sync.rs:1534)
        if not _adapter_supports_amend(adapter):
            journal.append(
                "entry.hedge_drive_amend_unsupported",
                {"entry_id": entry_id, "venue": maker_venue.value,
                 "action": "reprice", "reason": "amend_unsupported_by_venue"},
            )
            return HedgeDriveResult(action="reprice", outcome="rejected",
                                   detail="amend not supported by venue, use cancel_replace instead")

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
            _flush_adapter_diagnostics(adapter, journal)
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
        # V1: cancel_order (entry_sync.rs:2401-2445) → cancel_pending_entry_passive_order
        # V2: uses cancel_passive_order which goes through transport.cancel_passive_order
        try:
            await adapter.cancel_passive_order(
                symbol=symbol,
                order_id=maker_order_id,
                client_order_id=getattr(pending, 'maker_client_order_id', None) or None,
            )
            _flush_adapter_diagnostics(adapter, journal)
        except NotImplementedError:
            # Adapter doesn't support cancel_passive_order → try legacy cancel_order
            cancel_req = OrderRequest(
                venue=maker_venue,
                symbol=symbol,
                side=maker_leg,
                quantity=0.0,
                price=0.0,
                post_only=False,
                order_id=maker_order_id,
                reduce_only=False,
            )
            try:
                await adapter.cancel_order(cancel_req)
                _flush_adapter_diagnostics(adapter, journal)
            except Exception as e2:
                journal.append(
                    "entry.hedge_drive_cancel_replace_cancel_failed",
                    {
                        "entry_id": entry_id,
                        "action": "cancel_replace",
                        "old_price": old_price,
                        "new_price": new_price,
                        "order_id": maker_order_id,
                        "error": str(e2),
                        "reason": "replacement_not_submitted_to_avoid_double_maker",
                    },
                )
                return HedgeDriveResult(
                    action="cancel_replace",
                    outcome="uncertain",
                    detail=f"cancel failed before replacement: {e2}",
                )
        except Exception as e:
            journal.append(
                "entry.hedge_drive_cancel_replace_cancel_failed",
                {
                    "entry_id": entry_id,
                    "action": "cancel_replace",
                    "old_price": old_price,
                    "new_price": new_price,
                    "order_id": maker_order_id,
                    "error": str(e),
                    "reason": "replacement_not_submitted_to_avoid_double_maker",
                },
            )
            return HedgeDriveResult(
                action="cancel_replace",
                outcome="uncertain",
                detail=f"cancel failed before replacement: {e}",
            )

        new_req = OrderRequest(
            venue=maker_venue,
            symbol=symbol,
            side=maker_leg,
            quantity=qty,
            price=new_price,
            post_only=post_only,
            reduce_only=False,
        )
        # V1: replacement maker is always post_only → submit_passive_order (ACK-based)
        try:
            ack = await adapter.submit_passive_order(new_req)
            _flush_adapter_diagnostics(adapter, journal)
            journal.append(
                "entry.hedge_drive_cancel_replace",
                {"entry_id": entry_id, "action": "cancel_replace",
                 "old_price": old_price, "new_price": new_price,
                 "order_id": ack.order_id},
            )
            return HedgeDriveResult(action="cancel_replace", outcome="applied",
                                   order_id=ack.order_id, new_price=new_price)
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
