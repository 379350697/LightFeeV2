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

import inspect
import math
import time
from dataclasses import dataclass, field, replace
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from typing import Any, Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import (
    OrderFill,
    OrderRequest,
    PassiveOrderState,
    Side,
    TimeInForce,
    Venue,
)
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
    finalize_entry_execution_benchmark_receipt,
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
from lightfee.venues.cid import generate_exchange_cid


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
    reject_evidence: dict[str, Any] = field(default_factory=dict)
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
        # The executor deliberately has no market-data dependency.  The live
        # runtime injects this callback so the irrevocable first-leg fill is
        # repriced from its newest executable book before we either finish the
        # hedge or flatten that first leg.  A missing callback remains
        # conservative: complete the hedge rather than leaving naked delta.
        self.post_first_fill_decider = overrides.get("post_first_fill_decider")

    @staticmethod
    def _align_quantity_down_on_frozen_grid(
        quantity: float,
        step: float,
    ) -> float:
        """Floor a base quantity on the immutable entry-time decimal grid."""

        try:
            raw = Decimal(str(quantity))
            grid = Decimal(str(step))
        except (InvalidOperation, TypeError, ValueError):
            return 0.0
        if (
            not raw.is_finite()
            or not grid.is_finite()
            or raw <= 0
            or grid <= 0
        ):
            return 0.0
        return float(
            (raw / grid).to_integral_value(rounding=ROUND_FLOOR) * grid
        )

    @staticmethod
    def _entry_context_frozen_symbol_rule_contract(
        ctx: EntryContext,
        venue: Venue,
    ) -> tuple[dict[str, Any], float, str]:
        """Validate one leg of the immutable entry-time symbol-rule contract."""

        if venue == ctx.long_venue:
            raw_rule = ctx.long_symbol_rule_at_entry
        elif venue == ctx.short_venue:
            raw_rule = ctx.short_symbol_rule_at_entry
        else:
            return {}, 0.0, "direct_hedge_route_invariant_breached"
        if not raw_rule:
            return {}, 0.0, "direct_hedge_symbol_rule_evidence_missing"
        if not isinstance(raw_rule, dict):
            return {}, 0.0, "direct_hedge_symbol_rule_evidence_invalid"
        rule = dict(raw_rule)
        try:
            common_step = float(
                ctx.common_base_quantity_step_at_entry or 0.0
            )
            quantity_step = float(
                rule.get("quantity_step_base", 0.0) or 0.0
            )
            min_quantity = float(rule.get("min_quantity_base", 0.0) or 0.0)
            min_notional = float(rule.get("min_notional_quote", -1.0))
        except (TypeError, ValueError):
            return rule, 0.0, "direct_hedge_symbol_rule_evidence_invalid"
        if (
            rule.get("evidence_complete") is not True
            or list(rule.get("missing_fields") or [])
            or str(rule.get("quantity_units") or "") != "base"
            or str(rule.get("venue") or "") != venue.value
            or str(rule.get("symbol") or "") != str(ctx.symbol)
            or not math.isfinite(common_step)
            or not math.isfinite(quantity_step)
            or not math.isfinite(min_quantity)
            or not math.isfinite(min_notional)
            or common_step <= 1e-12
            or quantity_step <= 1e-12
            or min_quantity <= 0.0
            or min_notional < 0.0
        ):
            return (
                rule,
                common_step,
                "direct_hedge_symbol_rule_evidence_invalid",
            )
        grid_ratio = common_step / quantity_step
        if abs(grid_ratio - round(grid_ratio)) > 1e-8:
            return rule, common_step, "direct_hedge_common_quantity_grid_invalid"
        return rule, common_step, ""

    def _plan_direct_hedge_quantity(
        self,
        *,
        ctx: EntryContext,
        maker_fill: OrderFill,
        hedge_request: OrderRequest,
    ) -> dict[str, Any]:
        """Plan a legal hedge from the actual first fill and frozen rules.

        Legacy callers that predate entry-time symbol-rule freezing keep their
        old behavior only when *all* three contract fields are absent.  A
        partially populated contract fails closed; new live dispatches always
        populate the two rules and common grid together.
        """

        maker_quantity = float(maker_fill.quantity or 0.0)
        contract_present = bool(
            ctx.long_symbol_rule_at_entry
            or ctx.short_symbol_rule_at_entry
            or ctx.common_base_quantity_step_at_entry
        )
        try:
            common_step_evidence = float(
                ctx.common_base_quantity_step_at_entry or 0.0
            )
        except (TypeError, ValueError):
            common_step_evidence = 0.0
        try:
            hedge_price_evidence = float(hedge_request.price or 0.0)
        except (TypeError, ValueError):
            hedge_price_evidence = 0.0
        evidence: dict[str, Any] = {
            "maker_fill_venue": maker_fill.venue.value,
            "maker_fill_side": maker_fill.side.value,
            "maker_filled_quantity": maker_quantity,
            "hedge_venue": hedge_request.venue.value,
            "hedge_side": hedge_request.side.value,
            "hedge_price": hedge_price_evidence,
            "common_base_quantity_step_at_entry": common_step_evidence,
            "contract_present": contract_present,
        }
        if not contract_present:
            evidence.update(
                {
                    "hedge_quantity": maker_quantity,
                    "off_grid_remainder_quantity": 0.0,
                    "contract_mode": "legacy_unfrozen",
                }
            )
            return {
                "hedge_quantity": maker_quantity,
                "remainder_quantity": 0.0,
                "blocked_reason": "",
                "evidence": evidence,
            }

        maker_is_long = ctx.maker_leg == Side.BUY
        expected_maker_venue = (
            ctx.long_venue if maker_is_long else ctx.short_venue
        )
        expected_maker_side = Side.BUY if maker_is_long else Side.SELL
        expected_hedge_venue = (
            ctx.short_venue if maker_is_long else ctx.long_venue
        )
        expected_hedge_side = Side.SELL if maker_is_long else Side.BUY
        if (
            maker_fill.venue != expected_maker_venue
            or maker_fill.side != expected_maker_side
            or hedge_request.venue != expected_hedge_venue
            or hedge_request.side != expected_hedge_side
        ):
            return {
                "hedge_quantity": 0.0,
                "remainder_quantity": maker_quantity,
                "blocked_reason": "direct_hedge_route_invariant_breached",
                "evidence": evidence,
            }

        rules: dict[str, dict[str, Any]] = {}
        common_step = 0.0
        for venue in (ctx.long_venue, ctx.short_venue):
            rule, venue_common_step, reason = (
                self._entry_context_frozen_symbol_rule_contract(ctx, venue)
            )
            rules[venue.value] = rule
            common_step = venue_common_step or common_step
            if reason:
                evidence["frozen_symbol_rules"] = rules
                return {
                    "hedge_quantity": 0.0,
                    "remainder_quantity": maker_quantity,
                    "blocked_reason": reason,
                    "evidence": evidence,
                }

        hedge_rule = rules[hedge_request.venue.value]
        aligned_quantity = self._align_quantity_down_on_frozen_grid(
            maker_quantity,
            common_step,
        )
        remainder_quantity = max(maker_quantity - aligned_quantity, 0.0)
        min_quantity = float(hedge_rule["min_quantity_base"])
        min_notional = float(hedge_rule["min_notional_quote"])
        hedge_price = float(hedge_request.price or 0.0)
        notional = aligned_quantity * hedge_price
        evidence.update(
            {
                "frozen_symbol_rules": rules,
                "common_base_quantity_step_at_entry": common_step,
                "hedge_quantity": aligned_quantity,
                "off_grid_remainder_quantity": remainder_quantity,
                "min_quantity_base": min_quantity,
                "min_notional_quote": min_notional,
                "hedge_notional_quote": notional,
                "contract_mode": "frozen_entry_rules",
            }
        )
        blocked_reason = ""
        if aligned_quantity <= 1e-9:
            blocked_reason = (
                "direct_hedge_quantity_below_frozen_common_grid"
            )
        elif aligned_quantity + 1e-12 < min_quantity:
            blocked_reason = "direct_hedge_quantity_below_frozen_minimum"
        elif hedge_price <= 0.0 and min_notional > 0.0:
            blocked_reason = (
                "direct_hedge_price_missing_for_frozen_min_notional"
            )
        elif notional + 1e-9 < min_notional:
            blocked_reason = "direct_hedge_notional_below_frozen_minimum"
        return {
            "hedge_quantity": aligned_quantity,
            "remainder_quantity": remainder_quantity,
            "blocked_reason": blocked_reason,
            "evidence": evidence,
        }

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
        maker_submitted_at_ms = int(time.time() * 1000)
        maker_result = await self._submit_maker(
            ctx,
            maker_req,
            now_ms,
            submit_started_at_ms=maker_submitted_at_ms,
        )
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
            exchange_error = maker_result.get("exchange_error")
            if isinstance(exchange_error, dict):
                result.reject_evidence = dict(exchange_error)
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

        # --- Phase 2: reprice after the irrevocable first fill ---
        # A partial first fill must never cause the original target quantity to
        # be hedged.  It creates over-hedge delta.  The callback also lets the
        # live runtime compare the current cost of completing the pair with an
        # immediate first-leg unwind, using fresh executable prices.
        post_fill = await self._decide_after_first_fill(
            ctx=ctx,
            maker_fill=maker_fill,
            hedge_request=hedge_req,
            now_ms=now_ms,
        )
        action = str(post_fill.get("action", "complete_hedge") or "complete_hedge")
        if action == "unwind_first_leg":
            return await self._unwind_first_leg_after_fill(
                result=result,
                ctx=ctx,
                maker_request=maker_req,
                maker_fill=maker_fill,
                decision=post_fill,
                now_ms=now_ms,
            )

        hedge_price = float(post_fill.get("hedge_price", 0.0) or 0.0)
        hedge_req = replace(
            hedge_req,
            price=hedge_price if hedge_price > 0.0 else hedge_req.price,
        )
        try:
            direct_hedge_plan = self._plan_direct_hedge_quantity(
                ctx=ctx,
                maker_fill=maker_fill,
                hedge_request=hedge_req,
            )
        except Exception as exc:
            # No validation/planning defect is allowed to escape after the
            # first leg has filled.  Fail closed into the V1 flatten path.
            direct_hedge_plan = {
                "hedge_quantity": 0.0,
                "remainder_quantity": float(maker_fill.quantity or 0.0),
                "blocked_reason": "direct_hedge_quantity_planner_error",
                "evidence": {
                    "maker_filled_quantity": float(
                        maker_fill.quantity or 0.0
                    ),
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:300],
                },
            }
        direct_hedge_blocked_reason = str(
            direct_hedge_plan.get("blocked_reason", "") or ""
        )
        direct_hedge_evidence = dict(
            direct_hedge_plan.get("evidence", {}) or {}
        )
        if direct_hedge_blocked_reason:
            result.reject_evidence = {
                "reason": direct_hedge_blocked_reason,
                **direct_hedge_evidence,
            }
            self.journal.append(
                "entry.direct_hedge_quantity_blocked",
                {
                    "position_id": ctx.entry_id,
                    "reason": direct_hedge_blocked_reason,
                    **direct_hedge_evidence,
                },
            )
            return await self._unwind_first_leg_after_fill(
                result=result,
                ctx=ctx,
                maker_request=maker_req,
                maker_fill=maker_fill,
                decision={
                    "action": "unwind_first_leg",
                    "reason": direct_hedge_blocked_reason,
                    "complete_hedge_loss_quote": float(
                        post_fill.get("complete_hedge_loss_quote", 0.0) or 0.0
                    ),
                    "unwind_first_leg_loss_quote": float(
                        post_fill.get("unwind_first_leg_loss_quote", 0.0) or 0.0
                    ),
                    "market_evidence": {
                        **dict(post_fill.get("market_evidence", {}) or {}),
                        "direct_hedge_quantity_plan": direct_hedge_evidence,
                    },
                },
                now_ms=now_ms,
            )
        hedge_req = replace(
            hedge_req,
            quantity=float(direct_hedge_plan["hedge_quantity"]),
        )
        if float(direct_hedge_plan.get("remainder_quantity", 0.0) or 0.0) > 1e-9:
            self.journal.append(
                "entry.direct_hedge_quantity_aligned_down",
                {
                    "position_id": ctx.entry_id,
                    **direct_hedge_evidence,
                },
            )
        self.journal.append(
            "entry.post_first_fill_decision",
            {
                "position_id": ctx.entry_id,
                "action": "complete_hedge",
                "reason": str(post_fill.get("reason", "complete_hedge_default")),
                "maker_filled_quantity": maker_fill.quantity,
                "hedge_quantity": hedge_req.quantity,
                "hedge_price": hedge_req.price,
                "complete_hedge_loss_quote": float(
                    post_fill.get("complete_hedge_loss_quote", 0.0) or 0.0
                ),
                "unwind_first_leg_loss_quote": float(
                    post_fill.get("unwind_first_leg_loss_quote", 0.0) or 0.0
                ),
                "market_evidence": dict(post_fill.get("market_evidence", {}) or {}),
                "quantity_rule_evidence": direct_hedge_evidence,
            },
        )

        # --- Phase 3: submit the sized, repriced hedge ---
        hedge_submitted_at_ms = int(time.time() * 1000)
        hedge_result = await self._submit_hedge(
            ctx,
            hedge_req,
            now_ms,
            submit_started_at_ms=hedge_submitted_at_ms,
        )
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

        # Bind the raw pre-submit L2 capture to the two exchange fills only
        # after a symmetric, successful entry.  In particular, never make a
        # partial/residual branch look like a complete execution observation.
        if ctx.entry_execution_benchmark_receipt is not None:
            if ctx.maker_leg == Side.BUY:
                long_fill, short_fill = maker_fill, hedge_fill
                long_client_order_id = maker_req.client_order_id or ""
                short_client_order_id = hedge_req.client_order_id or ""
                long_submitted_at_ms = maker_submitted_at_ms
                short_submitted_at_ms = hedge_submitted_at_ms
            else:
                long_fill, short_fill = hedge_fill, maker_fill
                long_client_order_id = hedge_req.client_order_id or ""
                short_client_order_id = maker_req.client_order_id or ""
                long_submitted_at_ms = hedge_submitted_at_ms
                short_submitted_at_ms = maker_submitted_at_ms
            ctx = replace(
                ctx,
                entry_execution_benchmark_receipt=(
                    finalize_entry_execution_benchmark_receipt(
                        ctx.entry_execution_benchmark_receipt,
                        position_id=ctx.entry_id,
                        symbol=ctx.symbol,
                        long_venue=ctx.long_venue,
                        short_venue=ctx.short_venue,
                        long_fill=long_fill,
                        short_fill=short_fill,
                        long_client_order_id=long_client_order_id,
                        short_client_order_id=short_client_order_id,
                        long_submitted_at_ms=long_submitted_at_ms,
                        short_submitted_at_ms=short_submitted_at_ms,
                    )
                ),
            )

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
                "entry_execution_benchmark_receipt": position.entry_execution_benchmark_receipt,
                "execution_benchmark_complete": position.execution_benchmark_complete,
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
            # These fields identify the economic legs, not maker/hedge order
            # sequence.  Recovery derives maker_side()/hedge_side() from
            # maker_leg; reversing them for a short maker reverses both orders.
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
            metadata={"entry_selected_at_ms": ctx.created_at_ms},
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
            expected_shortfall_bps_entry=ctx.expected_shortfall_bps_entry,
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
            candidate_revision_id=ctx.candidate_revision_id,
            entry_max_leg_notional_quote=ctx.entry_max_leg_notional_quote,
            funding_canary_enabled_at_entry=ctx.funding_canary_enabled_at_entry,
            funding_canary_fee_assurance_tier=ctx.funding_canary_fee_assurance_tier,
            funding_canary_hard_max_entry_notional_quote=(
                ctx.funding_canary_hard_max_entry_notional_quote
            ),
            funding_canary_size_constrained=ctx.funding_canary_size_constrained,
            long_symbol_rule_at_entry=dict(ctx.long_symbol_rule_at_entry or {}),
            short_symbol_rule_at_entry=dict(ctx.short_symbol_rule_at_entry or {}),
            common_base_quantity_step_at_entry=(
                ctx.common_base_quantity_step_at_entry
            ),
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
        self,
        ctx: EntryContext,
        request: OrderRequest,
        now_ms: int,
        *,
        submit_started_at_ms: int = 0,
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
        return await self._submit_order(
            request,
            ctx.entry_id,
            "maker",
            submit_started_at_ms or now_ms,
        )

    async def _submit_hedge(
        self,
        ctx: EntryContext,
        request: OrderRequest,
        now_ms: int,
        *,
        submit_started_at_ms: int = 0,
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
        return await self._submit_order(
            request,
            ctx.entry_id,
            "hedge",
            submit_started_at_ms or now_ms,
        )

    async def _decide_after_first_fill(
        self,
        *,
        ctx: EntryContext,
        maker_fill: OrderFill,
        hedge_request: OrderRequest,
        now_ms: int,
    ) -> dict[str, Any]:
        """Ask the runtime to choose a fully hedgeable closure after a fill.

        The absence of a fresh decision is not permission to abandon the
        exposure: it explicitly defaults to completing the hedge at the
        original executable limit.  This preserves the V1 no-naked-leg
        invariant during startup/recovery and in test harnesses that do not
        own a live market-data runtime.
        """
        default = {
            "action": "complete_hedge",
            "reason": "post_first_fill_market_data_unavailable_complete_hedge",
            "hedge_price": hedge_request.price,
            "complete_hedge_loss_quote": 0.0,
            "unwind_first_leg_loss_quote": 0.0,
            "market_evidence": {},
        }
        decider = self.post_first_fill_decider
        if not callable(decider):
            return default
        try:
            decision = decider(
                ctx=ctx,
                maker_fill=maker_fill,
                hedge_request=hedge_request,
                now_ms=now_ms,
            )
            if inspect.isawaitable(decision):
                decision = await decision
            if not isinstance(decision, dict):
                raise TypeError("post_first_fill_decider must return a mapping")
            action = str(decision.get("action", "") or "")
            if action not in {"complete_hedge", "unwind_first_leg"}:
                raise ValueError(f"unsupported post-first-fill action: {action!r}")
            merged = dict(default)
            merged.update(decision)
            return merged
        except Exception as exc:
            self.journal.append(
                "entry.post_first_fill_decision_unavailable",
                {
                    "position_id": ctx.entry_id,
                    "reason": "post_first_fill_decider_error_complete_hedge",
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:300],
                },
            )
            return default

    async def _unwind_first_leg_after_fill(
        self,
        *,
        result: EntryExecutionResult,
        ctx: EntryContext,
        maker_request: OrderRequest,
        maker_fill: OrderFill,
        decision: dict[str, Any],
        now_ms: int,
    ) -> EntryExecutionResult:
        """Flatten the filled first leg and queue any incomplete unwind.

        This is a terminal branch.  It intentionally never creates a pending
        *entry* that could later submit the rejected hedge direction; an
        incomplete flatten becomes the normal V1 residual-repair lifecycle.
        """
        unwind_price = float(decision.get("unwind_price", 0.0) or 0.0)
        unwind_request = replace(
            maker_request,
            side=maker_fill.side.opposite(),
            quantity=maker_fill.quantity,
            price=unwind_price if unwind_price > 0.0 else maker_request.price,
            reduce_only=True,
            post_only=False,
            time_in_force=TimeInForce.IOC,
            client_order_id=generate_exchange_cid(
                ctx.entry_id,
                "unwind_after_first_fill",
                maker_fill.venue,
            ),
        )
        self.journal.append(
            "entry.post_first_fill_decision",
            {
                "position_id": ctx.entry_id,
                "action": "unwind_first_leg",
                "reason": str(decision.get("reason", "lower_expected_loss")),
                "maker_filled_quantity": maker_fill.quantity,
                "unwind_quantity": unwind_request.quantity,
                "unwind_price": unwind_request.price,
                "complete_hedge_loss_quote": float(
                    decision.get("complete_hedge_loss_quote", 0.0) or 0.0
                ),
                "unwind_first_leg_loss_quote": float(
                    decision.get("unwind_first_leg_loss_quote", 0.0) or 0.0
                ),
                "market_evidence": dict(decision.get("market_evidence", {}) or {}),
            },
        )
        self.journal.append(
            "order.submitted",
            {
                "position_id": ctx.entry_id,
                "internal_entry_id": ctx.entry_id,
                "leg": "unwind",
                "symbol": unwind_request.symbol,
                "venue": unwind_request.venue.value,
                "side": unwind_request.side.value,
                "quantity": unwind_request.quantity,
                "price": unwind_request.price,
                "reduce_only": True,
                "is_maker": False,
                "client_order_id": unwind_request.client_order_id,
                "time_in_force": unwind_request.time_in_force.value,
            },
        )
        unwind = await self._submit_order(
            unwind_request,
            ctx.entry_id,
            "unwind",
            now_ms,
        )
        unwind_fill = unwind.get("fill")
        unwinded_quantity = max(
            min(float(getattr(unwind_fill, "quantity", 0.0) or 0.0), maker_fill.quantity),
            0.0,
        )
        remaining = max(maker_fill.quantity - unwinded_quantity, 0.0)
        result.reject_reason = "unwound_after_first_fill"
        result.route = ExecutionRoute.REJECTED
        result.has_uncertainty = unwind.get("outcome") == "uncertain"
        if remaining <= 1e-9:
            result.state = EntryState.FAILED
            self.journal.append(
                "entry.unwound_after_first_fill",
                {
                    "position_id": ctx.entry_id,
                    "maker_filled_quantity": maker_fill.quantity,
                    "unwind_filled_quantity": unwinded_quantity,
                    "reason": str(decision.get("reason", "lower_expected_loss")),
                    "terminal": True,
                },
            )
            return result

        result.state = EntryState.FAILED_WITH_RESIDUAL
        result.residual_task = ResidualExposureTask(
            position_id=ctx.entry_id,
            pair_id=f"{ctx.symbol.lower()}:{ctx.long_venue.value}->{ctx.short_venue.value}",
            symbol=ctx.symbol,
            long_venue=ctx.long_venue,
            short_venue=ctx.short_venue,
            origin=ResidualOrigin.ENTRY_OPEN,
            exposure_venue=maker_fill.venue,
            exposure_side=maker_fill.side.opposite(),
            exposure_quantity=remaining,
            created_cycle=0,
            created_at_ms=now_ms,
            deadline_ms=now_ms + self.deadline_ms,
        )
        self.journal.append(
            "entry.unwind_after_first_fill_residual",
            {
                "position_id": ctx.entry_id,
                "maker_filled_quantity": maker_fill.quantity,
                "unwind_filled_quantity": unwinded_quantity,
                "residual_quantity": remaining,
                "residual_venue": maker_fill.venue.value,
                "residual_side": maker_fill.side.opposite().value,
                "unwind_outcome": unwind.get("outcome", "unknown"),
                "reason": str(decision.get("reason", "lower_expected_loss")),
            },
        )
        return result

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
            reason = f"no adapter for {request.venue.value}"
            self.journal.append(
                "order.rejected",
                {
                    "position_id": position_id,
                    "internal_entry_id": position_id,
                    "leg": leg,
                    "venue": request.venue.value,
                    "symbol": request.symbol,
                    "reason": reason,
                    "client_order_id": request.client_order_id,
                    "is_maker": is_maker,
                },
            )
            return {
                "outcome": "rejected",
                "fill": None,
                "order_id": "",
                "reason": reason,
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
