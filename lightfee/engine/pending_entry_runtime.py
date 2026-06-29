"""Pending-entry runtime delegate.

This module owns pending-entry reconcile and terminal-removal behavior
mechanically moved from LiveRuntime. Keep journal events, payload keys,
pending-entry removal authority, and recovery-core refresh timing stable.
"""

from __future__ import annotations

import json
from typing import Any

from lightfee.core.domain import OrderFill, PositionSnapshot, Side, Venue
from lightfee.engine.bootstrap import wall_clock_now_ms
from lightfee.engine.pending_entry_lifecycle import apply_pending_entry_passive_progress
from lightfee.engine.pending_entry_terminalizer import (
    PendingEntryLiveTruth,
    PendingEntryTerminalDecision,
    PendingEntryTerminalizer,
)
from lightfee.engine.reconciliation import _recon_fill_price
from lightfee.engine.runtime_context import RuntimeContext
from lightfee.risk.modes import EngineLifecycle


def apply_pending_entry_hedge_progress(
    pending: Any,
    *,
    new_total_quantity: float,
    price: float = 0.0,
    order_id: str = "",
    observed_at_ms: int = 0,
    now_ms: int = 0,
    quality: str = "observed",
) -> float:
    """Apply confirmed hedge progress and retire maker remainder FIFO slices."""

    previous_quantity = float(getattr(pending, "hedge_leg_filled", 0.0) or 0.0)
    target_quantity = max(float(new_total_quantity or 0.0), previous_quantity)
    delta = max(target_quantity - previous_quantity, 0.0)
    if delta > 1e-12:
        pending.hedge_leg_filled = target_quantity
        pending.consume_hedge_quantity_fifo(delta)
        pending.note_hedge_fill_observed(
            int(observed_at_ms or now_ms or 0),
            quality=quality,
        )

    price_value = float(price or 0.0)
    if price_value > 0:
        pending.hedge_fill_price = price_value
    if order_id:
        pending.hedge_order_id = str(order_id)

    return delta


class PendingEntryRuntime:
    def __init__(self, ctx: RuntimeContext) -> None:
        self.ctx = ctx

    async def _reconcile_pending_state(self, now_ms: int) -> None:
        """Process pending closes and pending entries through venue adapters.

        Rust V1: recovery.rs process_pending_close_reconciliations() with
        exponential backoff (base 30s, max 300s) and hard deadline (10 min).

        V1 parity (live tick hedge drive):
        After reconciliation resolves maker fills, if the pending entry has
        a missing hedge quantity > 0 and no inflight hedge, submits the hedge
        IOC/taker order.  On hedge fill, finalizes the entry → OpenPosition,
        writes entry.opened/runtime.position_opened, removes pending entry.
        """
        if not self.ctx._venue_adapters:
            return
        if self.ctx.reconciler is None:
            await self.ctx._process_pending_close_reconciliations(now_ms)
            return

        # --- Process pending entries: reconcile + drive missing hedge ---
        resolved_entry_ids: list[str] = []
        for entry_id, pending in list(self.ctx.state.pending_entries.items()):
            if getattr(pending, "outcome", "") == "rejected":
                if not pending.has_any_fill():
                    self.ctx.journal.append(
                        "reconciliation.rejected_pending_cleared",
                        {
                            "entry_id": entry_id,
                            "symbol": pending.symbol,
                            "reason": "maker rejected is terminal in V1",
                        },
                    )
                    resolved_entry_ids.append(entry_id)
                    continue
                if await self.ctx._maybe_finalize_rejected_pending_with_fill(
                    pending,
                    entry_id,
                    now_ms,
                    source="reconciliation",
                ):
                    continue
                self.ctx.journal.append(
                    "reconciliation.rejected_pending_retained_with_fill",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "maker_leg_filled": pending.maker_leg_filled,
                        "hedge_leg_filled": pending.hedge_leg_filled,
                        "reason": "rejected pending contains fill evidence; manual recovery required",
                    },
                )
                self.ctx._apply_reconcile_backoff(pending, now_ms)
                continue

            if not pending.uncertain_outcome:
                resolved_entry_ids.append(entry_id)
                continue

            # Respect backoff window
            if pending.reconcile_next_attempt_ms > 0 and now_ms < pending.reconcile_next_attempt_ms:
                continue

            # V1: abandon via live-size probe, not hard deadline.
            if pending.reconcile_attempt >= 1:
                abandoned = await self.ctx._try_abandon_stale_entry(pending, entry_id)
                if abandoned:
                    await self.ctx._complete_pending_entry_terminal_removal(
                        entry_id,
                        reason="pending_entry_reconcile_abandoned_flat",
                        symbol=pending.symbol,
                        now_ms=now_ms,
                    )
                    continue

            pending.reconcile_attempt += 1
            try:
                # V1: prefer hedge_inflight CID for reconciliation queries
                hedge_lookup_cid = (
                    pending.hedge_inflight.client_order_id
                    if pending.hedge_inflight
                    else (
                        pending.hedge_client_order_id
                        if (
                            pending.hedge_order_id
                            or pending.hedge_leg_filled > 0
                        )
                        else ""
                    )
                )
                maker_order_id, maker_client_order_id = (
                    self.ctx._pending_entry_maker_order_identifiers(pending)
                )
                result = await self.ctx.reconciler.reconcile_position(
                    position_id=entry_id,
                    symbol=pending.symbol,
                    long_venue=pending.long_venue,
                    short_venue=pending.short_venue,
                    long_order_id=maker_order_id,
                    short_order_id=pending.hedge_order_id,
                    long_client_order_id=maker_client_order_id,
                    short_client_order_id=hedge_lookup_cid,
                )
                self.ctx._flush_reconciler_order_diagnostics()
            except Exception as e:
                self.ctx._flush_reconciler_order_diagnostics()
                self.ctx.journal.append(
                    "reconciliation.entry_reconcile_error",
                    {"entry_id": entry_id, "error": str(e)},
                )
                self.ctx._apply_reconcile_backoff(pending, now_ms)
                continue

            # --- V1: write back fill quantities from reconciliation ---
            prev_maker_filled = pending.maker_leg_filled
            prev_hedge_filled = pending.hedge_leg_filled
            maker_filled_updated = False
            hedge_filled_updated = False

            if result.long_fill is not None and result.long_fill.quantity > 0:
                if pending.maker_leg == "long":
                    if result.long_fill.quantity > pending.maker_leg_filled:
                        pending.maker_leg_filled = result.long_fill.quantity
                        pending.maker_fill_price = _recon_fill_price(result.long_fill)
                        pending.note_maker_fill_observed(
                            getattr(result.long_fill, "filled_at_ms", 0) or now_ms,
                            quality=(
                                "exchange_fill_exact"
                                if getattr(result.long_fill, "filled_at_ms", 0)
                                else "observed"
                            ),
                        )
                        maker_filled_updated = True
                else:
                    if result.long_fill.quantity > pending.hedge_leg_filled:
                        delta = apply_pending_entry_hedge_progress(
                            pending,
                            new_total_quantity=result.long_fill.quantity,
                            price=_recon_fill_price(result.long_fill),
                            order_id=result.long_fill.order_id,
                            observed_at_ms=getattr(result.long_fill, "filled_at_ms", 0),
                            now_ms=now_ms,
                            quality=(
                                "exchange_fill_exact"
                                if getattr(result.long_fill, "filled_at_ms", 0)
                                else "observed"
                            ),
                        )
                        hedge_filled_updated = delta > 0

            if result.short_fill is not None and result.short_fill.quantity > 0:
                if pending.maker_leg == "short":
                    if result.short_fill.quantity > pending.maker_leg_filled:
                        pending.maker_leg_filled = result.short_fill.quantity
                        pending.maker_fill_price = _recon_fill_price(result.short_fill)
                        pending.note_maker_fill_observed(
                            getattr(result.short_fill, "filled_at_ms", 0) or now_ms,
                            quality=(
                                "exchange_fill_exact"
                                if getattr(result.short_fill, "filled_at_ms", 0)
                                else "observed"
                            ),
                        )
                        maker_filled_updated = True
                else:
                    if result.short_fill.quantity > pending.hedge_leg_filled:
                        delta = apply_pending_entry_hedge_progress(
                            pending,
                            new_total_quantity=result.short_fill.quantity,
                            price=_recon_fill_price(result.short_fill),
                            order_id=result.short_fill.order_id,
                            observed_at_ms=getattr(result.short_fill, "filled_at_ms", 0),
                            now_ms=now_ms,
                            quality=(
                                "exchange_fill_exact"
                                if getattr(result.short_fill, "filled_at_ms", 0)
                                else "observed"
                            ),
                        )
                        hedge_filled_updated = delta > 0

            def _defer_live_position_progress(
                *,
                position_leg: str,
                status: str,
                position: PositionSnapshot,
            ) -> None:
                pos_qty = abs(float(getattr(position, "quantity", 0.0) or 0.0))
                pos_price = float(getattr(position, "entry_price", 0.0) or 0.0)
                if (
                    pos_qty <= pending.maker_leg_filled
                    and (pos_price <= 0 or pending.maker_fill_price > 0)
                    and pending.maker_order_id
                ):
                    return
                self.ctx.journal.append(
                    "pending_entry.live_position_progress_deferred",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "leg": "maker",
                        "position_leg": position_leg,
                        "venue": position.venue.value,
                        "status": status,
                        "position_quantity": pos_qty,
                        "position_entry_price": pos_price,
                        "maker_leg_filled": pending.maker_leg_filled,
                        "maker_fill_price": pending.maker_fill_price,
                        "reason": "order_terminality_not_confirmed",
                    },
                )

            # Also update from position snapshots if fill data wasn't available.
            # Passive-maker progress still requires terminal order/fill evidence;
            # live position truth alone is used as evidence, not maker terminality.
            if result.long_position is not None and abs(result.long_position.quantity) > 0:
                pos_qty = abs(result.long_position.quantity)
                pos_price = float(getattr(result.long_position, "entry_price", 0.0) or 0.0)
                if pending.maker_leg == "long":
                    if result.long_status == "filled":
                        if pos_qty > pending.maker_leg_filled:
                            pending.maker_leg_filled = pos_qty
                            pending.note_maker_fill_observed(
                                getattr(result.long_position, "observed_at_ms", 0) or now_ms,
                                quality="live_truth_observed",
                            )
                            maker_filled_updated = True
                        if pos_price > 0 and pending.maker_fill_price <= 0:
                            pending.maker_fill_price = pos_price
                            maker_filled_updated = True
                        if not pending.maker_order_id:
                            pending.maker_order_id = f"{entry_id}-recovery-long"
                            maker_filled_updated = True
                    else:
                        _defer_live_position_progress(
                            position_leg="long",
                            status=result.long_status,
                            position=result.long_position,
                        )
                elif pos_qty > pending.hedge_leg_filled:
                    delta = apply_pending_entry_hedge_progress(
                        pending,
                        new_total_quantity=pos_qty,
                        price=pos_price,
                        order_id=(
                            "" if pending.hedge_order_id
                            else f"{entry_id}-recovery-long"
                        ),
                        observed_at_ms=getattr(result.long_position, "observed_at_ms", 0),
                        now_ms=now_ms,
                        quality="live_truth_observed",
                    )
                    hedge_filled_updated = delta > 0
                if pending.maker_leg != "long":
                    if pos_price > 0 and pending.hedge_fill_price <= 0:
                        pending.hedge_fill_price = pos_price
                        hedge_filled_updated = True
                    if not pending.hedge_order_id:
                        pending.hedge_order_id = f"{entry_id}-recovery-long"
                        hedge_filled_updated = True

            if result.short_position is not None and abs(result.short_position.quantity) > 0:
                pos_qty = abs(result.short_position.quantity)
                pos_price = float(getattr(result.short_position, "entry_price", 0.0) or 0.0)
                if pending.maker_leg == "short":
                    if result.short_status == "filled":
                        if pos_qty > pending.maker_leg_filled:
                            pending.maker_leg_filled = pos_qty
                            pending.note_maker_fill_observed(
                                getattr(result.short_position, "observed_at_ms", 0) or now_ms,
                                quality="live_truth_observed",
                            )
                            maker_filled_updated = True
                        if pos_price > 0 and pending.maker_fill_price <= 0:
                            pending.maker_fill_price = pos_price
                            maker_filled_updated = True
                        if not pending.maker_order_id:
                            pending.maker_order_id = f"{entry_id}-recovery-short"
                            maker_filled_updated = True
                    else:
                        _defer_live_position_progress(
                            position_leg="short",
                            status=result.short_status,
                            position=result.short_position,
                        )
                elif pos_qty > pending.hedge_leg_filled:
                    delta = apply_pending_entry_hedge_progress(
                        pending,
                        new_total_quantity=pos_qty,
                        price=pos_price,
                        order_id=(
                            "" if pending.hedge_order_id
                            else f"{entry_id}-recovery-short"
                        ),
                        observed_at_ms=getattr(result.short_position, "observed_at_ms", 0),
                        now_ms=now_ms,
                        quality="live_truth_observed",
                    )
                    hedge_filled_updated = delta > 0
                if pending.maker_leg != "short":
                    if pos_price > 0 and pending.hedge_fill_price <= 0:
                        pending.hedge_fill_price = pos_price
                        hedge_filled_updated = True
                    if not pending.hedge_order_id:
                        pending.hedge_order_id = f"{entry_id}-recovery-short"
                        hedge_filled_updated = True

            if maker_filled_updated:
                self.ctx.journal.append(
                    "pending_entry.maker_progress_applied",
                    {
                        "entry_id": entry_id,
                        "prev_maker_filled": prev_maker_filled,
                        "new_maker_filled": pending.maker_leg_filled,
                        "maker_fill_price": pending.maker_fill_price,
                    },
                )

            if hedge_filled_updated:
                self.ctx.journal.append(
                    "pending_entry.hedge_progress_applied",
                    {
                        "entry_id": entry_id,
                        "prev_hedge_filled": prev_hedge_filled,
                        "new_hedge_filled": pending.hedge_leg_filled,
                        "hedge_fill_price": pending.hedge_fill_price,
                    },
                )

            # --- V1: check if both legs are now filled → finalize ---
            if pending.missing_hedge_quantity() <= 1e-9 and pending.maker_completed():
                if await self.ctx._finalize_pending_entry(pending, entry_id, now_ms):
                    resolved_entry_ids.append(entry_id)
                else:
                    self.ctx._apply_reconcile_backoff(pending, now_ms)
                continue

            if result.long_status == "filled" and result.short_status == "filled":
                if await self.ctx._finalize_pending_entry(pending, entry_id, now_ms):
                    resolved_entry_ids.append(entry_id)
                    self.ctx.journal.append(
                        "reconciliation.entry_resolved",
                        {"entry_id": entry_id, "long_status": result.long_status, "short_status": result.short_status},
                    )
                else:
                    self.ctx._apply_reconcile_backoff(pending, now_ms)
                continue

            # V1: force_terminalize_pending_entry_if_budget_exhausted()
            # runs before flat-position retention. Otherwise a zero-fill
            # maker_resting entry with both venues flat but missing maker
            # terminal evidence can be retained forever.
            if await self.ctx._force_terminalize_pending_entry_if_budget_exhausted(
                pending, entry_id, now_ms
            ):
                continue

            if result.is_flat:
                maker_status = self.ctx._pending_entry_reconcile_maker_status(
                    pending, result
                )
                if (
                    maker_status == "not_found"
                    and self.ctx._pending_entry_has_maker_order_reference(pending)
                    and float(getattr(pending, "maker_leg_filled", 0.0) or 0.0) <= 1e-9
                    and float(getattr(pending, "hedge_leg_filled", 0.0) or 0.0) <= 1e-9
                ):
                    finalized = await self.ctx._finalize_pending_entry(
                        pending,
                        entry_id,
                        now_ms,
                    )
                    if finalized:
                        resolved_entry_ids.append(entry_id)
                        self.ctx.journal.append(
                            "reconciliation.entry_flat_not_found_terminal_cleared",
                            {
                                "entry_id": entry_id,
                                "symbol": pending.symbol,
                                "maker_status": maker_status,
                                "reason": "flat_position_zero_fill_not_found_maker_verified",
                            },
                        )
                        continue

                if not self.ctx._pending_entry_flat_clear_has_terminal_maker_evidence(
                    pending, result
                ):
                    self.ctx.journal.append(
                        "reconciliation.entry_flat_unresolved_maker_retained",
                        {
                            "entry_id": entry_id,
                            "symbol": pending.symbol,
                            "maker_status": maker_status,
                            "reason": "flat_position_without_terminal_maker_order_evidence",
                            "missing_endpoint": "maker_order_terminal_or_live_truth",
                        },
                    )
                    self.ctx._apply_reconcile_backoff(pending, now_ms)
                    continue
                self.ctx.journal.append(
                    "reconciliation.entry_cleared_flat",
                    {"entry_id": entry_id},
                )
                await self.ctx._complete_pending_entry_terminal_removal(
                    entry_id,
                    reason="pending_entry_reconcile_terminal_flat",
                    symbol=pending.symbol,
                    now_ms=now_ms,
                )
                continue

            # --- Clear stale hedge inflight after negative evidence ---
            if pending.hedge_inflight is not None:
                self.ctx._try_clear_stale_hedge_inflight(pending, entry_id, result, now_ms)

            # --- V1: hedge deadline check ---
            # If inflight hedge has exceeded its hard deadline, abort fail-closed
            # before attempting another hedge submit.
            if pending.hedge_inflight is not None:
                deadline = self.ctx._pending_entry_hedge_deadline_decision(pending, now_ms)
                if deadline.get("hard_breached"):
                    self.ctx.journal.append(
                        "pending_entry.hedge_deadline_breached",
                        {
                            "entry_id": entry_id,
                            "symbol": pending.symbol,
                            "hedge_venue": pending.hedge_venue().value,
                            "hedge_elapsed_ms": pending.hedge_inflight.elapsed_ms(now_ms),
                            "deadline_ms": deadline["hard_deadline_ms"],
                            "attempt": pending.hedge_inflight.attempt,
                        },
                    )
                    removed = await self.ctx._abort_pending_entry_fail_closed(
                        pending, entry_id,
                        "entry hedge deadline breached during reconciliation",
                    )
                    if removed:
                        resolved_entry_ids.append(entry_id)
                    continue

            # --- V1: terminalization budget check ---
            if await self.ctx._force_terminalize_pending_entry_if_budget_exhausted(
                pending, entry_id, now_ms
            ):
                continue

            # --- V1: drive missing hedge on normal tick ---
            missing = pending.missing_hedge_quantity()
            if missing > 1e-9:
                self.ctx.journal.append(
                    "pending_entry.missing_hedge_detected",
                    {
                        "entry_id": entry_id,
                        "missing_hedge_quantity": missing,
                        "maker_leg_filled": pending.maker_leg_filled,
                        "hedge_leg_filled": pending.hedge_leg_filled,
                        "maker_venue": pending.maker_venue().value,
                        "hedge_venue": pending.hedge_venue().value,
                    },
                )
                if await self.ctx._maybe_finalize_pending_entry_terminal_hedge_dust(
                    pending,
                    entry_id,
                    now_ms,
                    source="reconciliation",
                ):
                    resolved_entry_ids.append(entry_id)
                    continue
                hedge_driven = await self.ctx._drive_missing_hedge_live(pending, entry_id, now_ms)
                if hedge_driven:
                    if pending.missing_hedge_quantity() <= 1e-9 and pending.maker_completed():
                        if await self.ctx._finalize_pending_entry(pending, entry_id, now_ms):
                            resolved_entry_ids.append(entry_id)
                        else:
                            self.ctx._apply_reconcile_backoff(pending, now_ms)
                        continue
                # Keep entry for next reconciliation cycle
                self.ctx._apply_reconcile_backoff(pending, now_ms)
            else:
                # No fill progress, no missing hedge — backoff & wait
                self.ctx._apply_reconcile_backoff(pending, now_ms)

        for eid in resolved_entry_ids:
            resolved_pending = self.ctx.state.pending_entries.get(eid)
            await self.ctx._complete_pending_entry_terminal_removal(
                eid,
                reason="pending_entry_reconcile_resolved",
                symbol=str(getattr(resolved_pending, "symbol", "") or ""),
                now_ms=now_ms,
            )

        # --- Process V1 background close reconciliations ---
        await self.ctx._process_pending_close_reconciliations(now_ms)

        # --- Process pending closes ---
        resolved_ids: list[str] = []
        for close_id, pending in list(self.ctx.state.pending_closes.items()):
            if pending.long_uncertain or pending.short_uncertain:
                # Respect backoff window
                if pending.reconcile_next_attempt_ms > 0 and now_ms < pending.reconcile_next_attempt_ms:
                    continue

                # Hard deadline check
                if pending.deadline_ms > 0 and now_ms > pending.deadline_ms:
                    self.ctx.journal.append(
                        "reconciliation.close_abandoned_deadline",
                        {"close_id": close_id, "deadline_ms": pending.deadline_ms},
                    )
                    resolved_ids.append(close_id)
                    continue

                pos = self.ctx.state.open_positions.get(pending.position_id)
                if pos is None:
                    resolved_ids.append(close_id)
                    self.ctx.journal.append(
                        "reconciliation.pending_close_orphaned",
                        {"close_id": close_id, "position_id": pending.position_id},
                    )
                    continue

                pending.reconcile_attempt += 1
                try:
                    result = await self.ctx.reconciler.reconcile_position(
                        position_id=pending.position_id,
                        symbol=pos.symbol,
                        long_venue=pos.long_venue,
                        short_venue=pos.short_venue,
                    )
                    self.ctx._flush_reconciler_order_diagnostics()
                except Exception as e:
                    self.ctx._flush_reconciler_order_diagnostics()
                    self.ctx.journal.append(
                        "reconciliation.reconcile_error",
                        {"close_id": close_id, "error": str(e)},
                    )
                    self.ctx._apply_reconcile_backoff(pending, now_ms)
                    continue

                if result.is_flat:
                    resolved_ids.append(close_id)
                    self.ctx.state.open_positions.pop(pending.position_id, None)
                    self.ctx.journal.append(
                        "reconciliation.close_resolved_flat",
                        {"close_id": close_id, "position_id": pending.position_id},
                    )
                elif not pending.long_uncertain and not pending.short_uncertain:
                    resolved_ids.append(close_id)
                    self.ctx.journal.append(
                        "reconciliation.close_resolved",
                        {"close_id": close_id, "position_id": pending.position_id},
                    )
                else:
                    self.ctx._apply_reconcile_backoff(pending, now_ms)
            else:
                resolved_ids.append(close_id)

        for cid in resolved_ids:
            self.ctx.state.pending_closes.pop(cid, None)

        # Transition out of RECONCILING if all work is done
        if (
            self.ctx.state.lifecycle == EngineLifecycle.RECONCILING
            and not self.ctx.state.pending_entries
            and not self.ctx.state.pending_closes
        ):
            from lightfee.engine.lifecycle import transition_to_running

            transition_to_running(self.ctx.state)
            self.ctx.journal.append(
                "runtime.reconciling_complete",
                {"reason": "all_pending_resolved", "ts_ms": now_ms},
            )

    @staticmethod
    def _pending_entry_reconcile_maker_status(pending, result) -> str:
        if getattr(pending, "maker_leg", "long") == "long":
            return str(getattr(result, "long_status", "") or "").lower()
        return str(getattr(result, "short_status", "") or "").lower()

    @staticmethod
    def _order_status_is_terminal_no_fill(status: str) -> bool:
        normalized = str(status or "").lower()
        return normalized in {
            "canceled",
            "cancelled",
            "expired",
            "rejected",
        }

    @staticmethod
    def _fill_reconciliation_terminal_no_fill(reconciliation: Any) -> bool:
        qty = float(getattr(reconciliation, "quantity", 0.0) or 0.0)
        if qty > 0.0:
            return False
        metadata = getattr(reconciliation, "metadata", None) or {}
        status = ""
        if isinstance(metadata, dict):
            for key in (
                "status",
                "raw_exchange_status",
                "order_status",
                "state",
                "response_classification",
            ):
                status = str(metadata.get(key) or "")
                if status:
                    break
        return PendingEntryRuntime._order_status_is_terminal_no_fill(status)

    @staticmethod
    def _pending_entry_has_terminal_maker_zero_fill_evidence(
        pending,
        maker_reconciliation: Any | None,
    ) -> bool:
        if (
            maker_reconciliation is not None
            and PendingEntryRuntime._fill_reconciliation_terminal_no_fill(maker_reconciliation)
        ):
            return True

        passive_order = getattr(pending, "passive_order", None)
        state = getattr(passive_order, "last_progress_state", None)
        if state is None:
            return False
        if hasattr(state, "is_terminal") and state.is_terminal():
            return True
        return PendingEntryRuntime._order_status_is_terminal_no_fill(
            getattr(state, "value", str(state or ""))
        )

    @staticmethod
    def _pending_entry_has_maker_order_reference(pending) -> bool:
        order_id, client_order_id = PendingEntryRuntime._pending_entry_maker_order_identifiers(
            pending
        )
        return bool(order_id or client_order_id)

    @staticmethod
    def _pending_entry_maker_order_identifiers(pending) -> tuple[str, str]:
        passive_order = getattr(pending, "passive_order", None)
        order_id = str(getattr(pending, "maker_order_id", "") or "")
        client_order_id = str(getattr(pending, "maker_client_order_id", "") or "")
        if passive_order is not None:
            order_id = order_id or str(getattr(passive_order, "order_id", "") or "")
            client_order_id = client_order_id or str(
                getattr(passive_order, "client_order_id", "") or ""
            )
        return order_id, client_order_id

    @staticmethod
    def _pending_entry_maker_cancel_requested(pending) -> bool:
        passive_order = getattr(pending, "passive_order", None)
        return bool(
            getattr(pending, "_cancel_requested", False)
            or (
                passive_order is not None
                and passive_order.cancel_requested()
            )
        )

    def _mark_pending_entry_maker_cancel_requested(self, pending, now_ms: int) -> None:
        passive_order = getattr(pending, "passive_order", None)
        if passive_order is not None and not passive_order.cancel_requested():
            passive_order.cancel_requested_at_ms = now_ms
        pending._cancel_requested = True
        pending.next_progress_poll_ms = (
            now_ms + self.ctx.config.strategy.maker_venue_budget_window_ms
        )

    @staticmethod
    def _pending_entry_open_order_matches(
        row: Any,
        *,
        symbol: str,
        order_id: str,
        client_order_id: str,
    ) -> bool:
        if not isinstance(row, dict):
            return False
        row_order_id = str(
            row.get("orderId")
            or row.get("ordId")
            or row.get("id")
            or row.get("oid")
            or row.get("order_id")
            or ""
        )
        row_client_order_id = str(
            row.get("clientOrderId")
            or row.get("clOrdId")
            or row.get("orderLinkId")
            or row.get("clientOid")
            or row.get("cloid")
            or row.get("client_order_id")
            or ""
        )
        id_matches = bool(order_id and row_order_id == order_id) or bool(
            client_order_id and row_client_order_id == client_order_id
        )
        if not id_matches:
            return False
        row_symbol = str(row.get("symbol") or row.get("instId") or row.get("coin") or "")
        if not row_symbol:
            return True
        target_symbol = symbol.replace("-", "").replace("_", "").replace("SWAP", "")
        compact_row_symbol = (
            row_symbol.replace("-", "").replace("_", "").replace("SWAP", "")
        )
        return (
            row_symbol == symbol
            or compact_row_symbol == target_symbol
            or f"{compact_row_symbol}USDT" == target_symbol
        )

    async def _pending_entry_maker_open_order_matches(
        self,
        pending,
        adapter,
        maker_venue: Venue,
    ) -> tuple[list[Any] | None, str]:
        order_id, client_order_id = self.ctx._pending_entry_maker_order_identifiers(pending)
        try:
            open_orders = await self.ctx._fetch_residual_repair_open_orders(
                adapter,
                maker_venue,
                pending.symbol,
            )
        except Exception as exc:
            return None, str(exc)
        matches = [
            row
            for row in open_orders
            if self.ctx._pending_entry_open_order_matches(
                row,
                symbol=pending.symbol,
                order_id=order_id,
                client_order_id=client_order_id,
            )
        ]
        return matches, "open_order_truth"

    def _pending_entry_maker_terminal_base_evidence(
        self,
        pending,
        entry_id: str,
        *,
        maker_venue: Venue,
        order_id: str,
        client_order_id: str,
        reason: str,
    ) -> dict[str, Any]:
        passive_order = getattr(pending, "passive_order", None)
        passive_state = getattr(passive_order, "last_progress_state", None)
        passive_state_value = getattr(passive_state, "value", str(passive_state or ""))
        passive_checkpoint = (
            float(getattr(passive_order, "fill_checkpoint_quantity", 0.0) or 0.0)
            if passive_order is not None
            else 0.0
        )
        return {
            "entry_id": entry_id,
            "symbol": pending.symbol,
            "venue": maker_venue.value,
            "maker_venue": maker_venue.value,
            "maker_order_id": order_id,
            "maker_client_order_id": client_order_id,
            "maker_leg_filled": float(getattr(pending, "maker_leg_filled", 0.0) or 0.0),
            "target_quantity": float(getattr(pending, "target_quantity", 0.0) or 0.0),
            "progress_state": passive_state_value,
            "cumulative_quantity": passive_checkpoint,
            "passive_fill_checkpoint_quantity": passive_checkpoint,
            "passive_fill_checkpoint_last_fill_at_ms": (
                int(getattr(passive_order, "fill_checkpoint_last_fill_at_ms", 0) or 0)
                if passive_order is not None
                else 0
            ),
            "source": reason,
            "reason": reason,
        }

    def _pending_entry_terminal_event_names(self, scope: str) -> dict[str, str]:
        if scope == "release":
            return {
                "truth_unavailable": "pending_entry.release_maker_order_truth_unavailable",
                "open_truth_unavailable": "pending_entry.release_maker_open_order_truth_unavailable",
                "open_order": "pending_entry.release_retained_maker_open_order",
                "cancel_requested": "pending_entry.release_maker_cancel_requested",
                "cancel_failed": "pending_entry.release_maker_cancel_failed",
                "positive_fill": "pending_entry.release_maker_positive_fill_truth_retained",
                "terminal": "pending_entry.release_maker_terminal_no_open_order",
            }
        return {
            "truth_unavailable": "entry.abort_maker_order_truth_unavailable",
            "open_truth_unavailable": "entry.abort_maker_order_truth_unavailable",
            "open_order": "entry.abort_retained_maker_open_order",
            "cancel_requested": "entry.abort_maker_cancel_requested",
            "cancel_failed": "entry.abort_maker_cancel_failed",
            "positive_fill": "entry.abort_maker_positive_fill_truth_retained",
            "terminal": "entry.abort_maker_terminal_no_open_order",
        }

    @staticmethod
    def _pending_entry_exception_evidence(exc: Exception) -> dict[str, Any]:
        body = str(getattr(exc, "body", "") or "")
        payload: dict[str, Any] = {"error": str(exc) or exc.__class__.__name__}
        status_code = int(getattr(exc, "status_code", 0) or 0)
        if status_code:
            payload["error_status_code"] = status_code
        if body:
            payload["error_body"] = body
            try:
                parsed = json.loads(body)
            except (TypeError, ValueError):
                parsed = None
            if isinstance(parsed, dict):
                if parsed.get("code") is not None:
                    payload["exchange_code"] = str(parsed.get("code"))
                if parsed.get("msg") is not None:
                    payload["exchange_msg"] = str(parsed.get("msg"))
        return payload

    async def _ensure_pending_entry_maker_terminal_proof(
        self,
        pending: Any,
        entry_id: str,
        *,
        reason: str,
        now_ms: int,
        scope: str,
    ) -> tuple[bool, dict[str, Any]]:
        maker_venue = pending.maker_venue()
        adapter = self.ctx.get_venue_adapter(maker_venue)
        order_id, client_order_id = self.ctx._pending_entry_maker_order_identifiers(pending)
        base_evidence = self._pending_entry_maker_terminal_base_evidence(
            pending,
            entry_id,
            maker_venue=maker_venue,
            order_id=order_id,
            client_order_id=client_order_id,
            reason=reason,
        )
        events = self._pending_entry_terminal_event_names(scope)

        def state_value(progress: Any) -> str:
            state = getattr(progress, "state", None) if progress is not None else None
            return getattr(state, "value", str(state or ""))

        def is_terminal_progress(progress: Any) -> bool:
            state = getattr(progress, "state", None) if progress is not None else None
            return bool(state is not None and hasattr(state, "is_terminal") and state.is_terminal())

        def terminal(extra: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
            payload = {
                **base_evidence,
                "result": "maker_terminal_before_pending_release"
                if scope == "release"
                else "maker_terminal_before_pending_abort",
                "open_order_truth": "absent",
                "has_live_open_order": False,
                **extra,
            }
            self.ctx.journal.append(events["terminal"], payload)
            return True, payload

        def retain(event_key: str, extra: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
            payload = {
                **base_evidence,
                "result": "maker_order_not_terminal_before_pending_release"
                if scope == "release"
                else "maker_order_not_terminal_before_pending_abort",
                **extra,
            }
            self.ctx.journal.append(events[event_key], payload)
            return False, payload

        if adapter is None:
            return retain("truth_unavailable", {"error": "maker_adapter_unavailable"})

        matches, open_order_error = await self.ctx._pending_entry_maker_open_order_matches(
            pending,
            adapter,
            maker_venue,
        )
        if matches is None:
            return retain(
                "open_truth_unavailable",
                {
                    "error": open_order_error or "open_order_truth_unavailable",
                    "has_live_open_order": False,
                },
            )
        if matches:
            return retain(
                "open_order",
                {
                    "open_order_count": len(matches),
                    "open_order_truth": "present",
                    "has_live_open_order": True,
                },
            )

        maker_completed = bool(getattr(pending, "maker_completed", lambda: False)())
        if maker_completed:
            return terminal({"reason": "maker_completed_and_open_order_absent"})

        maker_filled = float(getattr(pending, "maker_leg_filled", 0.0) or 0.0)
        target_quantity = float(getattr(pending, "target_quantity", 0.0) or 0.0)

        async def query_progress() -> tuple[Any, dict[str, Any]]:
            try:
                progress = await adapter.query_passive_order_progress(
                    symbol=pending.symbol,
                    order_id=order_id,
                    client_order_id=client_order_id or None,
                    side=pending.maker_side(),
                )
                return progress, {}
            except Exception as exc:
                return None, self._pending_entry_exception_evidence(exc)

        progress, progress_error = await query_progress()
        cumulative_quantity = (
            float(getattr(progress, "cumulative_quantity", 0.0) or 0.0)
            if progress is not None
            else 0.0
        )
        if cumulative_quantity > maker_filled + 1e-9:
            apply_pending_entry_passive_progress(pending, progress)
            return retain(
                "positive_fill",
                {
                    "open_order_truth": "absent",
                    "has_live_open_order": False,
                    "progress_state": state_value(progress),
                    "cumulative_quantity": cumulative_quantity,
                    "local_maker_quantity": maker_filled,
                    "target_quantity": target_quantity,
                    "decision": "retain_pending_for_recovery_truth",
                },
            )
        if is_terminal_progress(progress):
            return terminal(
                {
                    "reason": "maker_progress_terminal_and_open_order_absent",
                    "progress_state": state_value(progress),
                    "cumulative_quantity": cumulative_quantity,
                }
            )

        cancel_requested = self.ctx._pending_entry_maker_cancel_requested(pending)
        if cancel_requested:
            if scope == "abort" and not progress_error:
                return terminal(
                    {
                        "reason": "maker_cancel_requested_and_open_order_absent",
                        "progress_state": state_value(progress),
                        "cumulative_quantity": cumulative_quantity,
                    }
                )
            return retain(
                "truth_unavailable",
                {
                    **(
                        progress_error
                        or {"error": "maker_cancel_requested_without_terminal_progress"}
                    ),
                    "open_order_truth": "absent",
                    "has_live_open_order": False,
                    "progress_state": state_value(progress),
                    "cumulative_quantity": cumulative_quantity,
                },
            )

        try:
            await adapter.cancel_passive_order(
                symbol=pending.symbol,
                order_id=order_id,
                client_order_id=client_order_id or None,
            )
            self.ctx._mark_pending_entry_maker_cancel_requested(pending, now_ms)
            self.ctx.journal.append(
                events["cancel_requested"],
                {
                    **base_evidence,
                    "reason": "cancel_before_pending_release"
                    if scope == "release"
                    else reason,
                    "result": "maker_cancel_requested_before_pending_release"
                    if scope == "release"
                    else "maker_cancel_requested_before_pending_abort",
                    "open_order_truth": "absent",
                    "has_live_open_order": False,
                    "progress_state": state_value(progress),
                    "cumulative_quantity": cumulative_quantity,
                    **{
                        f"pre_cancel_{key}": value
                        for key, value in progress_error.items()
                    },
                },
            )
        except Exception as exc:
            cancel_error = self._pending_entry_exception_evidence(exc)
            self.ctx.journal.append(
                events["cancel_failed"],
                {
                    **base_evidence,
                    "result": "maker_cancel_failed_before_pending_release"
                    if scope == "release"
                    else "maker_cancel_failed_before_pending_abort",
                    **cancel_error,
                    "open_order_truth": "absent",
                    "has_live_open_order": False,
                    "progress_state": state_value(progress),
                    "cumulative_quantity": cumulative_quantity,
                    **{
                        f"pre_cancel_{key}": value
                        for key, value in progress_error.items()
                    },
                },
            )
            follow_matches, follow_open_error = await self.ctx._pending_entry_maker_open_order_matches(
                pending,
                adapter,
                maker_venue,
            )
            if follow_matches:
                return retain(
                    "open_order",
                    {
                        "open_order_count": len(follow_matches),
                        "open_order_truth": "present",
                        "has_live_open_order": True,
                        "cancel_error": cancel_error,
                    },
                )
            follow_progress, follow_progress_error = await query_progress()
            follow_cumulative = (
                float(getattr(follow_progress, "cumulative_quantity", 0.0) or 0.0)
                if follow_progress is not None
                else 0.0
            )
            if follow_cumulative > maker_filled + 1e-9:
                apply_pending_entry_passive_progress(pending, follow_progress)
                return retain(
                    "positive_fill",
                    {
                        "open_order_truth": "absent" if follow_matches is not None else "unknown",
                        "has_live_open_order": False,
                        "progress_state": state_value(follow_progress),
                        "cumulative_quantity": follow_cumulative,
                        "local_maker_quantity": maker_filled,
                        "target_quantity": target_quantity,
                        "decision": "retain_pending_for_recovery_truth",
                        **{f"cancel_{key}": value for key, value in cancel_error.items()},
                    },
                )
            if follow_matches is not None and is_terminal_progress(follow_progress):
                return terminal(
                    {
                        "reason": "maker_cancel_failed_but_followup_terminal",
                        "progress_state": state_value(follow_progress),
                        "cumulative_quantity": follow_cumulative,
                        **{f"cancel_{key}": value for key, value in cancel_error.items()},
                    }
                )
            fallback_error = (
                follow_progress_error
                or ({"error": follow_open_error} if follow_open_error else {})
                or cancel_error
                or {"error": "maker_cancel_failed_without_terminal_followup"}
            )
            return retain(
                "truth_unavailable",
                {
                    **fallback_error,
                    "open_order_truth": "absent" if follow_matches is not None else "unknown",
                    "has_live_open_order": False,
                    "progress_state": state_value(follow_progress),
                    "cumulative_quantity": follow_cumulative,
                    **{f"cancel_{key}": value for key, value in cancel_error.items()},
                },
            )

        follow_matches, follow_open_error = await self.ctx._pending_entry_maker_open_order_matches(
            pending,
            adapter,
            maker_venue,
        )
        if follow_matches is None:
            return retain(
                "open_truth_unavailable",
                {
                    "error": follow_open_error or "open_order_truth_unavailable_after_cancel",
                    "open_order_truth": "unknown",
                    "has_live_open_order": False,
                },
            )
        if follow_matches:
            return retain(
                "open_order",
                {
                    "open_order_count": len(follow_matches),
                    "open_order_truth": "present",
                    "has_live_open_order": True,
                },
            )
        follow_progress, follow_progress_error = await query_progress()
        follow_cumulative = (
            float(getattr(follow_progress, "cumulative_quantity", 0.0) or 0.0)
            if follow_progress is not None
            else 0.0
        )
        if follow_cumulative > maker_filled + 1e-9:
            apply_pending_entry_passive_progress(pending, follow_progress)
            return retain(
                "positive_fill",
                {
                    "open_order_truth": "absent",
                    "has_live_open_order": False,
                    "progress_state": state_value(follow_progress),
                    "cumulative_quantity": follow_cumulative,
                    "local_maker_quantity": maker_filled,
                    "target_quantity": target_quantity,
                    "decision": "retain_pending_for_recovery_truth",
                },
            )
        if is_terminal_progress(follow_progress) or scope == "abort":
            return terminal(
                {
                    "reason": "maker_cancel_terminal_and_open_order_absent",
                    "progress_state": state_value(follow_progress),
                    "cumulative_quantity": follow_cumulative,
                    **{
                        f"progress_{key}": value
                        for key, value in follow_progress_error.items()
                    },
                }
            )
        return retain(
            "truth_unavailable",
            {
                **(
                    follow_progress_error
                    or {"error": "maker_cancel_without_terminal_progress"}
                ),
                "open_order_truth": "absent",
                "has_live_open_order": False,
                "progress_state": state_value(follow_progress),
                "cumulative_quantity": follow_cumulative,
            },
        )

    async def _ensure_pending_entry_maker_not_open_before_abort(
        self,
        pending,
        entry_id: str,
        reason: str,
    ) -> bool:
        if not self.ctx._pending_entry_has_maker_order_reference(pending):
            return True
        ok, _payload = await self._ensure_pending_entry_maker_terminal_proof(
            pending,
            entry_id,
            reason=reason,
            now_ms=wall_clock_now_ms(),
            scope="abort",
        )
        return ok

    def _pending_entry_flat_clear_has_terminal_maker_evidence(self, pending, result) -> bool:
        if not self.ctx._pending_entry_has_maker_order_reference(pending):
            return self.ctx.config.runtime.mode != "live"
        maker_status = self.ctx._pending_entry_reconcile_maker_status(pending, result)
        return self.ctx._order_status_is_terminal_no_fill(maker_status)

    async def _pending_entry_has_unresolved_maker_order(
        self, pending, entry_id: str
    ) -> bool:
        if not self.ctx._pending_entry_has_maker_order_reference(pending):
            if self.ctx.config.runtime.mode == "live":
                maker_venue = pending.maker_venue()
                self.ctx.journal.append(
                    "pending_entry.maker_terminal_evidence_unavailable",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "maker_venue": maker_venue.value,
                        "reason": "maker_order_reference_unavailable",
                    },
                )
                return True
            return False
        maker_filled = float(getattr(pending, "maker_leg_filled", 0.0) or 0.0)
        target_quantity = float(getattr(pending, "target_quantity", 0.0) or 0.0)
        if pending.maker_completed() and maker_filled >= target_quantity - 1e-9:
            return False

        maker_venue = pending.maker_venue()
        adapter = self.ctx.get_venue_adapter(maker_venue)
        if adapter is None:
            return True

        try:
            maker_side = getattr(pending, 'maker_side', None)
            if callable(maker_side):
                maker_side = maker_side()
            order_id, client_order_id = self.ctx._pending_entry_maker_order_identifiers(
                pending
            )
            progress = await adapter.query_passive_order_progress(
                symbol=pending.symbol,
                order_id=order_id,
                client_order_id=client_order_id or None,
                side=maker_side if isinstance(maker_side, Side) else None,
            )
        except Exception as e:
            self.ctx.journal.append(
                "pending_entry.maker_terminal_evidence_unavailable",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "maker_venue": maker_venue.value,
                    "error": str(e),
                },
            )
            return True

        if progress is None:
            matches, open_order_error = await self.ctx._pending_entry_maker_open_order_matches(
                pending,
                adapter,
                maker_venue,
            )
            if matches is not None:
                if matches:
                    self.ctx.journal.append(
                        "pending_entry.maker_open_order_retained",
                        {
                            "entry_id": entry_id,
                            "symbol": pending.symbol,
                            "maker_venue": maker_venue.value,
                            "maker_order_id": order_id,
                            "maker_client_order_id": client_order_id,
                            "open_order_count": len(matches),
                            "reason": "passive_order_progress_none",
                        },
                    )
                    return True
                if not self.ctx._pending_entry_maker_cancel_requested(pending):
                    self.ctx.journal.append(
                        "pending_entry.maker_cancel_required_before_flat_abandon",
                        {
                            "entry_id": entry_id,
                            "symbol": pending.symbol,
                            "maker_venue": maker_venue.value,
                            "maker_order_id": order_id,
                            "maker_client_order_id": client_order_id,
                            "reason": "passive_order_progress_none_open_order_absent",
                        },
                    )
                    return True
                self.ctx.journal.append(
                    "pending_entry.maker_terminal_no_open_order",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "maker_venue": maker_venue.value,
                        "maker_order_id": order_id,
                        "maker_client_order_id": client_order_id,
                        "reason": "passive_order_progress_none_open_order_absent",
                    },
                )
                return False
            self.ctx.journal.append(
                "pending_entry.maker_terminal_evidence_unavailable",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "maker_venue": maker_venue.value,
                    "reason": "passive_order_progress_none",
                    "open_order_error": open_order_error,
                },
            )
            return True

        if getattr(progress, "cumulative_quantity", 0.0) > 1e-9:
            return True
        state = getattr(progress, "state", None)
        if state is not None and hasattr(state, "is_terminal"):
            state_value = str(getattr(state, "value", str(state or "")) or "").lower()
            if state_value == "filled":
                return True
            if state.is_terminal():
                matches, open_order_error = await self.ctx._pending_entry_maker_open_order_matches(
                    pending,
                    adapter,
                    maker_venue,
                )
                if matches is not None:
                    if matches:
                        self.ctx.journal.append(
                            "pending_entry.maker_open_order_retained",
                            {
                                "entry_id": entry_id,
                                "symbol": pending.symbol,
                                "maker_venue": maker_venue.value,
                                "maker_order_id": order_id,
                                "maker_client_order_id": client_order_id,
                                "open_order_count": len(matches),
                                "reason": "passive_order_terminal_no_fill_open_order_present",
                                "progress_state": state_value,
                            },
                        )
                        return True
                    self.ctx.journal.append(
                        "pending_entry.maker_terminal_no_open_order",
                        {
                            "entry_id": entry_id,
                            "symbol": pending.symbol,
                            "maker_venue": maker_venue.value,
                            "maker_order_id": order_id,
                            "maker_client_order_id": client_order_id,
                            "reason": "passive_order_terminal_no_fill_open_order_absent",
                            "progress_state": state_value,
                        },
                    )
                    return False
                self.ctx.journal.append(
                    "pending_entry.maker_terminal_evidence_unavailable",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "maker_venue": maker_venue.value,
                        "reason": "passive_order_terminal_no_fill",
                        "progress_state": state_value,
                        "open_order_error": open_order_error,
                    },
                )
                return True
            return True
        return True

    async def _pending_entry_zero_fill_has_live_maker_open_order(
        self,
        pending,
        entry_id: str,
        now_ms: int,
    ) -> PendingEntryLiveTruth:
        if not self.ctx._pending_entry_has_maker_order_reference(pending):
            if str(getattr(self.ctx.config.runtime, "mode", "") or "") == "live":
                pending.uncertain_outcome = True
                pending.reconcile_next_attempt_ms = max(
                    int(getattr(pending, "reconcile_next_attempt_ms", 0) or 0),
                    now_ms + 1_000,
                )
                self.ctx.journal.append(
                    "pending_entry.finalize_maker_order_reference_unavailable",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "maker_venue": pending.maker_venue().value,
                        "reason": "zero_fill_finalize_maker_order_reference_unavailable",
                    },
                )
                return PendingEntryLiveTruth(
                    available=False,
                    has_live_open_order=False,
                    has_live_position=False,
                    error="maker_order_reference_unavailable",
                )
            return PendingEntryLiveTruth(
                available=True,
                has_live_open_order=False,
                has_live_position=False,
            )

        maker_venue = pending.maker_venue()
        adapter = self.ctx.get_venue_adapter(maker_venue)
        order_id, client_order_id = self.ctx._pending_entry_maker_order_identifiers(pending)
        if adapter is None:
            if str(getattr(self.ctx.config.runtime, "mode", "") or "") != "live":
                return PendingEntryLiveTruth(
                    available=True,
                    has_live_open_order=False,
                    has_live_position=False,
                )
            pending.uncertain_outcome = True
            pending.reconcile_next_attempt_ms = max(
                int(getattr(pending, "reconcile_next_attempt_ms", 0) or 0),
                now_ms + 1_000,
            )
            return PendingEntryLiveTruth(
                available=False,
                has_live_open_order=False,
                has_live_position=False,
                error="maker_adapter_unavailable",
            )

        fetch_open_orders = getattr(adapter, "fetch_open_orders", None)
        transport = getattr(adapter, "_transport", None)
        if (
            str(getattr(self.ctx.config.runtime, "mode", "") or "") != "live"
            and not callable(fetch_open_orders)
            and (transport is None or not hasattr(transport, "_request"))
        ):
            return PendingEntryLiveTruth(
                available=True,
                has_live_open_order=False,
                has_live_position=False,
            )

        matches, open_order_error = await self.ctx._pending_entry_maker_open_order_matches(
            pending,
            adapter,
            maker_venue,
        )
        if matches is None:
            pending.uncertain_outcome = True
            pending.reconcile_next_attempt_ms = max(
                int(getattr(pending, "reconcile_next_attempt_ms", 0) or 0),
                now_ms + 1_000,
            )
            self.ctx.journal.append(
                "pending_entry.finalize_maker_open_order_truth_unavailable",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "maker_venue": maker_venue.value,
                    "maker_order_id": order_id,
                    "maker_client_order_id": client_order_id,
                    "error": open_order_error,
                    "reason": "zero_fill_finalize_open_order_truth_unavailable",
                },
            )
            return PendingEntryLiveTruth(
                available=False,
                has_live_open_order=False,
                has_live_position=False,
                error=open_order_error or "open_order_truth_unavailable",
            )
        if not matches:
            return PendingEntryLiveTruth(
                available=True,
                has_live_open_order=False,
                has_live_position=False,
            )

        pending.uncertain_outcome = True
        pending.reconcile_next_attempt_ms = max(
            int(getattr(pending, "reconcile_next_attempt_ms", 0) or 0),
            now_ms + 1_000,
        )
        self.ctx.journal.append(
            "pending_entry.finalize_deferred_maker_open_order",
            {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "maker_venue": maker_venue.value,
                "maker_order_id": order_id,
                "maker_client_order_id": client_order_id,
                "open_order_count": len(matches),
                "maker_leg_filled": pending.maker_leg_filled,
                "hedge_leg_filled": pending.hedge_leg_filled,
                "reason": "maker_open_order_truth_present",
            },
        )
        return PendingEntryLiveTruth(
            available=True,
            has_live_open_order=True,
            has_live_position=False,
        )

    async def _pending_entry_zero_fill_has_live_maker_position(
        self,
        pending,
        entry_id: str,
        now_ms: int,
    ) -> PendingEntryLiveTruth:
        maker_venue = pending.maker_venue()
        adapter = self.ctx.get_venue_adapter(maker_venue)
        if adapter is None:
            if str(getattr(self.ctx.config.runtime, "mode", "") or "") != "live":
                return PendingEntryLiveTruth(
                    available=True,
                    has_live_open_order=False,
                    has_live_position=False,
                )
            pending.uncertain_outcome = True
            pending.reconcile_next_attempt_ms = max(
                int(getattr(pending, "reconcile_next_attempt_ms", 0) or 0),
                now_ms + 1_000,
            )
            return PendingEntryLiveTruth(
                available=False,
                has_live_open_order=False,
                has_live_position=False,
                error="maker_adapter_unavailable",
            )

        fetch_position = getattr(adapter, "fetch_position", None)
        if (
            str(getattr(self.ctx.config.runtime, "mode", "") or "") != "live"
            and not callable(fetch_position)
        ):
            return PendingEntryLiveTruth(
                available=True,
                has_live_open_order=False,
                has_live_position=False,
            )

        try:
            position = await fetch_position(pending.symbol)
        except Exception as exc:
            pending.uncertain_outcome = True
            pending.reconcile_next_attempt_ms = max(
                int(getattr(pending, "reconcile_next_attempt_ms", 0) or 0),
                now_ms + 1_000,
            )
            self.ctx.journal.append(
                "pending_entry.finalize_maker_live_position_truth_unavailable",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "maker_venue": maker_venue.value,
                    "error": str(exc) or exc.__class__.__name__,
                    "reason": "zero_fill_finalize_live_position_truth_unavailable",
                },
            )
            return PendingEntryLiveTruth(
                available=False,
                has_live_open_order=False,
                has_live_position=False,
                error=str(exc) or exc.__class__.__name__,
            )

        live_qty = abs(float(getattr(position, "quantity", 0.0) or 0.0)) if position else 0.0
        if live_qty <= 1e-9:
            return PendingEntryLiveTruth(
                available=True,
                has_live_open_order=False,
                has_live_position=False,
            )

        position_side = getattr(position, "side", None)
        maker_leg = str(getattr(pending, "maker_leg", "") or "").lower()
        live_long_quantity = (
            live_qty
            if maker_leg == "long" and position_side == Side.BUY
            else 0.0
        )
        live_short_quantity = (
            live_qty
            if maker_leg == "short" and position_side == Side.SELL
            else 0.0
        )
        pending.uncertain_outcome = True
        pending.reconcile_next_attempt_ms = max(
            int(getattr(pending, "reconcile_next_attempt_ms", 0) or 0),
            now_ms + 1_000,
        )
        self.ctx.journal.append(
            "pending_entry.finalize_deferred_maker_live_position",
            {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "maker_venue": maker_venue.value,
                "live_position_quantity": live_qty,
                "live_position_side": getattr(
                    getattr(position, "side", None), "value", str(getattr(position, "side", ""))
                ),
                "live_position_entry_price": float(
                    getattr(position, "entry_price", 0.0) or 0.0
                ),
                "maker_leg_filled": pending.maker_leg_filled,
                "hedge_leg_filled": pending.hedge_leg_filled,
                "reason": "maker_live_position_truth_present",
            },
        )
        return PendingEntryLiveTruth(
            available=True,
            has_live_open_order=False,
            has_live_position=True,
            live_long_quantity=live_long_quantity,
            live_short_quantity=live_short_quantity,
            live_balanced_quantity=0.0,
        )

    async def _pending_entry_positive_fill_live_truth(
        self,
        pending,
        entry_id: str,
        now_ms: int,
    ) -> PendingEntryLiveTruth:
        if str(getattr(self.ctx.config.runtime, "mode", "") or "") != "live":
            return PendingEntryLiveTruth(
                available=True,
                has_live_open_order=False,
                has_live_position=True,
                positive_fill_requires_live_position=False,
            )

        open_order_truth = PendingEntryLiveTruth(available=True)
        if self.ctx._pending_entry_has_maker_order_reference(pending):
            open_order_truth = await self.ctx._pending_entry_zero_fill_has_live_maker_open_order(
                pending,
                entry_id,
                now_ms,
            )
            if open_order_truth.has_live_open_order:
                return PendingEntryLiveTruth(
                    available=True,
                    has_live_open_order=True,
                    has_live_position=False,
                    positive_fill_requires_live_position=True,
                )

        live_positions: dict[str, float] = {}
        live_position_details: dict[str, dict[str, Any]] = {}
        live_long_quantity = 0.0
        live_short_quantity = 0.0
        errors: list[str] = []
        for venue, expected_side, leg_name in (
            (pending.long_venue, Side.BUY, "long"),
            (pending.short_venue, Side.SELL, "short"),
        ):
            adapter = self.ctx.get_venue_adapter(venue)
            fetch_position = getattr(adapter, "fetch_position", None) if adapter else None
            venue_name = getattr(venue, "value", str(venue))
            if not callable(fetch_position):
                errors.append(f"{venue_name}:fetch_position_unavailable")
                continue
            try:
                position = await fetch_position(pending.symbol)
            except Exception as exc:
                errors.append(f"{venue_name}:{str(exc) or exc.__class__.__name__}")
                continue
            raw_quantity = abs(
                float(getattr(position, "quantity", 0.0) or 0.0)
            ) if position else 0.0
            position_side = getattr(position, "side", None) if position else None
            side_matches = position_side == expected_side
            matched_quantity = raw_quantity if side_matches else 0.0
            live_positions[venue_name] = raw_quantity
            live_position_details[venue_name] = {
                "leg": leg_name,
                "quantity": raw_quantity,
                "matched_quantity": matched_quantity,
                "side": getattr(position_side, "value", str(position_side)),
                "expected_side": expected_side.value,
            }
            if leg_name == "long":
                live_long_quantity = matched_quantity
            else:
                live_short_quantity = matched_quantity

        if errors:
            error_text = ";".join(
                [error for error in (open_order_truth.error, *errors) if error]
            )
            pending.uncertain_outcome = True
            pending.reconcile_next_attempt_ms = max(
                int(getattr(pending, "reconcile_next_attempt_ms", 0) or 0),
                now_ms + 1_000,
            )
            self.ctx.journal.append(
                "pending_entry.positive_fill_live_truth_unavailable",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "maker_leg_filled": pending.maker_leg_filled,
                    "hedge_leg_filled": pending.hedge_leg_filled,
                    "live_positions": live_positions,
                    "live_position_details": live_position_details,
                    "error": error_text or "positive_fill_live_truth_unavailable",
                    "reason": "positive_fill_requires_live_position_truth",
                },
            )
            return PendingEntryLiveTruth(
                available=False,
                has_live_open_order=open_order_truth.has_live_open_order,
                has_live_position=False,
                error=error_text or "positive_fill_live_truth_unavailable",
                positive_fill_requires_live_position=True,
                live_long_quantity=live_long_quantity,
                live_short_quantity=live_short_quantity,
                live_balanced_quantity=min(live_long_quantity, live_short_quantity),
                live_position_details=live_position_details,
            )

        has_live_position = any(qty > 1e-9 for qty in live_positions.values())
        live_balanced_quantity = min(live_long_quantity, live_short_quantity)
        return PendingEntryLiveTruth(
            available=True,
            has_live_open_order=False,
            has_live_position=has_live_position,
            positive_fill_requires_live_position=True,
            live_long_quantity=live_long_quantity,
            live_short_quantity=live_short_quantity,
            live_balanced_quantity=live_balanced_quantity,
            live_position_details=live_position_details,
        )

    async def _finalize_pending_entry(self, pending, entry_id: str, now_ms: int) -> bool:
        """Finalize a completed pending entry: build OpenPosition, write entry.opened.

        Returns True only after terminal open/residual/passive-unfilled evidence.
        Returns False when live truth or fill details defer finalization and the
        caller must retain pending work.

        V1 parity gate (entry_sync.rs:5338-5454):
        1. Compute residual_task BEFORE the balanced_quantity branch (line 5338).
        2. balanced_quantity > 0: create OpenPosition, emit entry.opened; if residual
           exists → persist as "incremental_entry_open_partially_matched".
        3. balanced_quantity == 0 with residual (has_any_fill): persist as
           "incremental_entry_open_unmatched_residual", no open position.
        4. balanced_quantity == 0 with no fill (zero-fill): retain unless
           terminal no-fill, open-order truth, and live-position truth prove no
           live maker artifact; only then emit entry.passive_unfilled.

        Zero-fill (maker=0, hedge=0) entries are not immediate terminality.
        One-sided fill (maker>0, hedge=0) creates an unmatched residual task for
        cleanup but does NOT create an open position or emit entry.opened.
        """
        from lightfee.engine.entry import build_open_position, EntryContext, EntryType
        from lightfee.engine.residual import (
            split_entry_fill_residual,
            residual_pair_id,
        )

        maker_is_long = pending.maker_leg == "long"
        maker_side = Side.BUY if maker_is_long else Side.SELL

        precheck_maker_filled = float(getattr(pending, "maker_leg_filled", 0.0) or 0.0)
        precheck_hedge_filled = float(getattr(pending, "hedge_leg_filled", 0.0) or 0.0)
        precheck_balanced = min(precheck_maker_filled, precheck_hedge_filled)
        precheck_missing_price = (
            precheck_balanced > 1e-9
            and (
                float(getattr(pending, "maker_fill_price", 0.0) or 0.0) <= 0.0
                or float(getattr(pending, "hedge_fill_price", 0.0) or 0.0) <= 0.0
            )
        )
        if precheck_missing_price:
            live_truth = await self.ctx._pending_entry_positive_fill_live_truth(
                pending,
                entry_id,
                now_ms,
            )
            decision = PendingEntryTerminalizer().decide(
                pending,
                live_truth=live_truth,
            )
            if decision.outcome == "positive_fill_live_truth_conflict":
                self.ctx.journal.append(
                    "pending_entry.terminalizer_decision",
                    self.ctx._pending_entry_terminalizer_decision_payload(
                        entry_id,
                        pending,
                        decision,
                        now_ms,
                    ),
                )
                if await self._handle_positive_fill_live_truth_conflict(
                    pending=pending,
                    entry_id=entry_id,
                    decision=decision,
                    live_truth=live_truth,
                    now_ms=now_ms,
                ):
                    return True
                return False

        if not await self.ctx._ensure_pending_entry_open_fill_details(
            pending,
            entry_id,
            now_ms,
        ):
            return False

        raw_maker_leg_filled = float(pending.maker_leg_filled or 0.0)
        raw_hedge_leg_filled = float(pending.hedge_leg_filled or 0.0)
        raw_long_fill_quantity = (
            raw_maker_leg_filled if maker_is_long else raw_hedge_leg_filled
        )
        raw_short_fill_quantity = (
            raw_hedge_leg_filled if maker_is_long else raw_maker_leg_filled
        )
        long_venue_metadata = self.ctx._venue_symbol_metadata_evidence(
            pending.long_venue,
            pending.symbol,
        )
        short_venue_metadata = self.ctx._venue_symbol_metadata_evidence(
            pending.short_venue,
            pending.symbol,
        )
        maker_order_id_for_fill, maker_client_order_id_for_fill = (
            self.ctx._pending_entry_maker_order_identifiers(pending)
        )
        maker_filled_at_ms = int(getattr(pending, "maker_leg_filled_at_ms", 0) or 0)
        hedge_filled_at_ms = int(getattr(pending, "hedge_leg_filled_at_ms", 0) or 0)
        maker_timestamp_quality = str(
            getattr(pending, "maker_fill_timestamp_quality", "") or ""
        )
        hedge_timestamp_quality = str(
            getattr(pending, "hedge_fill_timestamp_quality", "") or ""
        )
        if maker_filled_at_ms <= 0 and raw_maker_leg_filled > 0.0:
            maker_filled_at_ms = now_ms
            maker_timestamp_quality = "finalization_fallback"
        if hedge_filled_at_ms <= 0 and raw_hedge_leg_filled > 0.0:
            hedge_filled_at_ms = now_ms
            hedge_timestamp_quality = "finalization_fallback"
        entry_timestamp_quality = (
            "exchange_fill_exact"
            if (
                maker_timestamp_quality == "exchange_fill_exact"
                and hedge_timestamp_quality == "exchange_fill_exact"
            )
            else (
                "live_truth_observed"
                if "live_truth_observed" in {
                    maker_timestamp_quality,
                    hedge_timestamp_quality,
                }
                else (
                    "observed"
                    if "observed" in {
                        maker_timestamp_quality,
                        hedge_timestamp_quality,
                    }
                    else "finalization_fallback"
                )
            )
        )

        # V1: build_residual_task is computed before branching, but only after
        # order/fill reconciliation has made pending quantities authoritative.
        maker_fill = OrderFill(
            venue=pending.maker_venue(),
            symbol=pending.symbol,
            side=maker_side,
            quantity=pending.maker_leg_filled,
            price=pending.maker_fill_price if pending.maker_fill_price > 0 else pending.maker_price,
            order_id=maker_order_id_for_fill,
            filled_at_ms=maker_filled_at_ms,
        )
        hedge_fill = OrderFill(
            venue=pending.hedge_venue(),
            symbol=pending.symbol,
            side=pending.hedge_side(),
            quantity=pending.hedge_leg_filled,
            price=pending.hedge_fill_price if pending.hedge_fill_price > 0 else pending.maker_fill_price,
            order_id=pending.hedge_order_id,
            filled_at_ms=hedge_filled_at_ms,
        )

        pair_id = getattr(pending, "pair_id", "") or residual_pair_id(
            pending.symbol, pending.long_venue, pending.short_venue
        )
        residual_task = split_entry_fill_residual(
            position_id=entry_id,
            pair_id=pair_id,
            symbol=pending.symbol,
            long_venue=pending.long_venue,
            short_venue=pending.short_venue,
            long_fill=OrderFill(
                venue=pending.long_venue,
                symbol=pending.symbol,
                side=Side.BUY,
                quantity=pending.maker_leg_filled if maker_is_long else pending.hedge_leg_filled,
                price=pending.maker_fill_price if maker_is_long else pending.hedge_fill_price,
            ),
            short_fill=OrderFill(
                venue=pending.short_venue,
                symbol=pending.symbol,
                side=Side.SELL,
                quantity=pending.hedge_leg_filled if maker_is_long else pending.maker_leg_filled,
                price=pending.hedge_fill_price if maker_is_long else pending.maker_fill_price,
            ),
            created_cycle=getattr(self.ctx.state, "cycle", 0),
            now_ms=now_ms,
        )

        balanced_quantity = min(pending.maker_leg_filled, pending.hedge_leg_filled)
        balanced_quantity = max(balanced_quantity, 0.0)
        residual_evidence = {
            "raw_maker_leg_filled": raw_maker_leg_filled,
            "raw_hedge_leg_filled": raw_hedge_leg_filled,
            "raw_long_fill_quantity": raw_long_fill_quantity,
            "raw_short_fill_quantity": raw_short_fill_quantity,
            "matched_quantity": balanced_quantity,
            "maker_order_id": maker_order_id_for_fill,
            "hedge_order_id": pending.hedge_order_id,
            "maker_client_order_id": maker_client_order_id_for_fill,
            "hedge_client_order_id": pending.hedge_client_order_id,
            "quantity_source": "finalized_pending_entry_reconciled_fills",
            "long_venue_metadata": long_venue_metadata,
            "short_venue_metadata": short_venue_metadata,
        }

        if balanced_quantity <= 0.0:
            if not pending.has_any_fill():
                open_order_truth = await self.ctx._pending_entry_zero_fill_has_live_maker_open_order(
                    pending,
                    entry_id,
                    now_ms,
                )
                live_position_truth = await self.ctx._pending_entry_zero_fill_has_live_maker_position(
                    pending,
                    entry_id,
                    now_ms,
                )
                missing_truth_errors = [
                    truth.error
                    for truth in (open_order_truth, live_position_truth)
                    if not truth.available and truth.error
                ]
                live_truth = PendingEntryLiveTruth(
                    available=open_order_truth.available and live_position_truth.available,
                    has_live_open_order=open_order_truth.has_live_open_order,
                    has_live_position=live_position_truth.has_live_position,
                    error=";".join(missing_truth_errors),
                    live_long_quantity=live_position_truth.live_long_quantity,
                    live_short_quantity=live_position_truth.live_short_quantity,
                    live_balanced_quantity=live_position_truth.live_balanced_quantity,
                )
                decision = PendingEntryTerminalizer().decide(
                    pending,
                    live_truth=live_truth,
                )
                self.ctx.journal.append(
                    "pending_entry.terminalizer_decision",
                    self.ctx._pending_entry_terminalizer_decision_payload(
                        entry_id,
                        pending,
                        decision,
                        now_ms,
                    ),
                )
                if not decision.allows_pending_removal:
                    if (
                        decision.outcome == "deferred_live_position"
                        and await self._cleanup_zero_fill_live_truth_conflict(
                            pending=pending,
                            entry_id=entry_id,
                            live_truth=live_truth,
                            now_ms=now_ms,
                        )
                    ):
                        return True
                    return False
                # V1: zero-fill is removable only after terminal no-fill plus
                # available clear open-order and live-position truth.
                self.ctx.journal.append(
                    "entry.passive_unfilled",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "pair_id": pair_id,
                        "maker_leg_filled": pending.maker_leg_filled,
                        "hedge_leg_filled": pending.hedge_leg_filled,
                        "balanced_quantity": balanced_quantity,
                        "reason": "zero_fill_unfilled_removal",
                    },
                )
                self.ctx.journal.append(
                    "pending_entry.pending_entry_finalized",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "pair_id": pair_id,
                        "position_id": None,
                        "maker_leg_filled": pending.maker_leg_filled,
                        "hedge_leg_filled": pending.hedge_leg_filled,
                        "maker_fill_price": pending.maker_fill_price,
                        "hedge_fill_price": pending.hedge_fill_price,
                        "finalized_as": "unfilled_zero_balanced",
                    },
                )
                await self.ctx._complete_pending_entry_terminal_removal(
                    entry_id,
                    reason="zero_fill_unfilled_removal",
                    symbol=pending.symbol,
                    now_ms=now_ms,
                )
                return True

            # V1: balanced_quantity == 0 but has_any_fill → one-sided exposure.
            # No open position, no entry.opened. Persist residual task if asymmetric.
            # entry_sync.rs:5436-5443: if let Some(task) = residual_task {
            #   persist_pending_residual_repair(task, "incremental_entry_open_unmatched_residual")
            # }
            decision = PendingEntryTerminalizer().decide(
                pending,
                live_truth=PendingEntryLiveTruth(available=True),
            )
            self.ctx.journal.append(
                "pending_entry.terminalizer_decision",
                self.ctx._pending_entry_terminalizer_decision_payload(
                    entry_id,
                    pending,
                    decision,
                    now_ms,
                ),
            )
            if not decision.allows_pending_removal:
                return False
            if residual_task is not None:
                self.ctx._queue_pending_residual_repair(
                    residual_task,
                    "incremental_entry_open_unmatched_residual",
                    residual_evidence,
                )

            self.ctx.journal.append(
                "pending_entry.zero_balanced_with_fill_retained",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "pair_id": pair_id,
                    "maker_leg_filled": pending.maker_leg_filled,
                    "hedge_leg_filled": pending.hedge_leg_filled,
                    "balanced_quantity": balanced_quantity,
                    "reason": "one_sided_fill_retained_for_cleanup",
                },
            )
            self.ctx.journal.append(
                "pending_entry.pending_entry_finalized",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "pair_id": pair_id,
                    "position_id": None,
                    "maker_leg_filled": pending.maker_leg_filled,
                    "hedge_leg_filled": pending.hedge_leg_filled,
                    "maker_fill_price": pending.maker_fill_price,
                    "hedge_fill_price": pending.hedge_fill_price,
                    "balanced_quantity": balanced_quantity,
                    "finalized_as": "unmatched_residual",
                },
            )
            await self.ctx._complete_pending_entry_terminal_removal(
                entry_id,
                reason="unmatched_residual_terminalized",
                symbol=pending.symbol,
                now_ms=now_ms,
            )
            return True

        # --- balanced_quantity > 0: create OpenPosition and entry.opened ---
        live_truth = await self.ctx._pending_entry_positive_fill_live_truth(
            pending,
            entry_id,
            now_ms,
        )
        decision = PendingEntryTerminalizer().decide(
            pending,
            live_truth=live_truth,
        )
        self.ctx.journal.append(
            "pending_entry.terminalizer_decision",
            self.ctx._pending_entry_terminalizer_decision_payload(
                entry_id,
                pending,
                decision,
                now_ms,
            ),
        )
        if not decision.allows_pending_removal:
            if decision.outcome == "positive_fill_live_truth_conflict":
                if await self._handle_positive_fill_live_truth_conflict(
                    pending=pending,
                    entry_id=entry_id,
                    decision=decision,
                    live_truth=live_truth,
                    now_ms=now_ms,
                ):
                    return True
            return False

        open_maker_fill_quantity = min(
            float(pending.maker_leg_filled or 0.0),
            balanced_quantity,
        )
        open_hedge_fill_quantity = min(
            float(pending.hedge_leg_filled or 0.0),
            balanced_quantity,
        )
        maker_fill = OrderFill(
            venue=pending.maker_venue(),
            symbol=pending.symbol,
            side=maker_side,
            quantity=open_maker_fill_quantity,
            price=pending.maker_fill_price,
            order_id=maker_order_id_for_fill,
            filled_at_ms=maker_filled_at_ms,
        )
        hedge_fill = OrderFill(
            venue=pending.hedge_venue(),
            symbol=pending.symbol,
            side=pending.hedge_side(),
            quantity=open_hedge_fill_quantity,
            price=pending.hedge_fill_price,
            order_id=pending.hedge_order_id,
            filled_at_ms=hedge_filled_at_ms,
        )

        ctx = EntryContext(
            entry_id=entry_id,
            symbol=pending.symbol,
            long_venue=pending.long_venue,
            short_venue=pending.short_venue,
            long_quantity=pending.target_quantity,
            short_quantity=pending.target_quantity,
            long_price_hint=0.0,
            short_price_hint=0.0,
            maker_leg=maker_side,
            entry_type=EntryType(pending.entry_type) if pending.entry_type else EntryType.STANDARD_DUAL_TAKER,
            created_at_ms=pending.created_at_ms,
            opportunity_type=pending.opportunity_type,
            funding_timestamp_ms=pending.funding_timestamp_ms,
            first_funding_timestamp_ms=pending.first_funding_timestamp_ms,
            long_funding_timestamp_ms=pending.long_funding_timestamp_ms,
            short_funding_timestamp_ms=pending.short_funding_timestamp_ms,
            second_funding_timestamp_ms=pending.second_funding_timestamp_ms,
            first_funding_leg=pending.first_funding_leg,
            funding_edge_bps_entry=pending.funding_edge_bps_entry,
            total_funding_edge_bps_entry=pending.total_funding_edge_bps_entry,
            expected_edge_bps_entry=pending.expected_edge_bps_entry,
            worst_case_edge_bps_entry=pending.worst_case_edge_bps_entry,
            entry_maker_leg=pending.entry_maker_leg,
            exit_maker_leg=pending.exit_maker_leg,
            entry_cross_bps_entry=pending.entry_cross_bps_entry,
            fee_bps_entry=pending.fee_bps_entry,
            entry_slippage_bps_entry=pending.entry_slippage_bps_entry,
            transfer_bias_bps_entry=pending.transfer_bias_bps_entry,
            transfer_state_at_entry=pending.transfer_state_at_entry,
            entry_liquidity_source_at_entry=pending.entry_liquidity_source_at_entry,
            long_volume_24h_quote_at_entry=pending.long_volume_24h_quote_at_entry,
            short_volume_24h_quote_at_entry=pending.short_volume_24h_quote_at_entry,
            long_open_interest_quote_at_entry=pending.long_open_interest_quote_at_entry,
            short_open_interest_quote_at_entry=pending.short_open_interest_quote_at_entry,
            long_entry_vwap=pending.long_entry_vwap,
            short_entry_vwap=pending.short_entry_vwap,
            entry_capacity_constrained=pending.entry_capacity_constrained,
            entry_target_quantity=pending.entry_target_quantity,
            long_max_executable_quantity=pending.long_max_executable_quantity,
            short_max_executable_quantity=pending.short_max_executable_quantity,
            entry_max_executable_quantity=pending.entry_max_executable_quantity,
            entry_depth_shortfall_quantity=pending.entry_depth_shortfall_quantity,
            entry_max_executable_notional_quote=pending.entry_max_executable_notional_quote,
            entry_depth_capped_at_entry=pending.entry_depth_capped_at_entry,
            advisories=list(pending.advisories),
            blocked_reasons=list(pending.blocked_reasons),
            exit_after_first_stage=pending.exit_after_first_stage,
        )

        position = build_open_position(ctx, maker_fill, hedge_fill, now_ms)

        self.ctx.state.open_positions[position.position_id] = position

        self.ctx.journal.append_critical(
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
                "entered_at_ms": position.entered_at_ms,
                "maker_filled_at_ms": maker_filled_at_ms,
                "hedge_filled_at_ms": hedge_filled_at_ms,
                "maker_fill_timestamp_quality": maker_timestamp_quality,
                "hedge_fill_timestamp_quality": hedge_timestamp_quality,
                "entry_timestamp_quality": entry_timestamp_quality,
                "matched_quantity": position.matched_quantity,
                "balanced_quantity": balanced_quantity,
                "raw_maker_leg_filled": raw_maker_leg_filled,
                "raw_hedge_leg_filled": raw_hedge_leg_filled,
                "open_maker_fill_quantity": open_maker_fill_quantity,
                "open_hedge_fill_quantity": open_hedge_fill_quantity,
                "maker_order_id": maker_fill.order_id,
                "hedge_order_id": hedge_fill.order_id,
                "maker_client_order_id": pending.maker_client_order_id,
                "hedge_client_order_id": pending.hedge_client_order_id,
                "funding_timestamp_ms": position.funding_timestamp_ms,
                "second_funding_timestamp_ms": position.second_funding_timestamp_ms,
                "opportunity_type": position.opportunity_type,
                "second_stage_enabled_at_entry": position.second_stage_enabled_at_entry,
                "exit_after_first_stage": position.exit_after_first_stage,
                "funding_edge_bps_entry": position.funding_edge_bps_entry,
                "total_funding_edge_bps_entry": position.total_funding_edge_bps_entry,
                "expected_edge_bps_entry": position.expected_edge_bps_entry,
                "quantity_source": "matched_fill_open_position",
                "long_venue_metadata": long_venue_metadata,
                "short_venue_metadata": short_venue_metadata,
            },
        )

        self.ctx.journal.append(
            "pending_entry.pending_entry_finalized",
            {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "pair_id": pair_id,
                "position_id": position.position_id,
                "maker_leg_filled": pending.maker_leg_filled,
                "hedge_leg_filled": pending.hedge_leg_filled,
                "maker_fill_price": pending.maker_fill_price,
                "hedge_fill_price": pending.hedge_fill_price,
                "balanced_quantity": balanced_quantity,
                "raw_maker_leg_filled": raw_maker_leg_filled,
                "raw_hedge_leg_filled": raw_hedge_leg_filled,
                "open_maker_fill_quantity": open_maker_fill_quantity,
                "open_hedge_fill_quantity": open_hedge_fill_quantity,
                "funding_timestamp_ms": position.funding_timestamp_ms,
                "second_funding_timestamp_ms": position.second_funding_timestamp_ms,
                "opportunity_type": position.opportunity_type,
                "second_stage_enabled_at_entry": position.second_stage_enabled_at_entry,
                "exit_after_first_stage": position.exit_after_first_stage,
                "funding_edge_bps_entry": position.funding_edge_bps_entry,
                "total_funding_edge_bps_entry": position.total_funding_edge_bps_entry,
                "expected_edge_bps_entry": position.expected_edge_bps_entry,
                "finalized_as": "open_position",
                "quantity_source": "matched_fill_open_position",
                "long_venue_metadata": long_venue_metadata,
                "short_venue_metadata": short_venue_metadata,
            },
        )

        self.ctx.journal.append(
            "runtime.position_opened",
            {
                "position_id": position.position_id,
                "symbol": position.symbol,
            },
        )

        # V1: entry_sync.rs:5423-5430 — if residual exists for partially matched
        # fill (e.g. maker=10, hedge=8 → 8 balanced + 2 residual), persist it.
        if residual_task is not None:
            self.ctx._queue_pending_residual_repair(
                residual_task,
                "incremental_entry_open_partially_matched",
                residual_evidence,
            )
        return True

    async def _handle_positive_fill_live_truth_conflict(
        self,
        *,
        pending: Any,
        entry_id: str,
        decision: PendingEntryTerminalDecision,
        live_truth: PendingEntryLiveTruth,
        now_ms: int,
    ) -> bool:
        pending.uncertain_outcome = True
        pending.reconcile_next_attempt_ms = max(
            int(getattr(pending, "reconcile_next_attempt_ms", 0) or 0),
            now_ms + 1_000,
        )
        maker_order_id, maker_client_order_id = (
            self.ctx._pending_entry_maker_order_identifiers(pending)
        )
        maker_fill_truth = {
            "maker_venue": pending.maker_venue().value,
            "hedge_venue": pending.hedge_venue().value,
            "maker_side": pending.maker_side().value,
            "hedge_side": pending.hedge_side().value,
            "maker_order_id": maker_order_id,
            "maker_client_order_id": maker_client_order_id,
            "hedge_order_id": str(getattr(pending, "hedge_order_id", "") or ""),
            "hedge_client_order_id": str(
                getattr(pending, "hedge_client_order_id", "") or ""
            ),
            "maker_leg_filled": float(
                getattr(pending, "maker_leg_filled", 0.0) or 0.0
            ),
            "hedge_leg_filled": float(
                getattr(pending, "hedge_leg_filled", 0.0) or 0.0
            ),
            "maker_fill_price": float(
                getattr(pending, "maker_fill_price", 0.0) or 0.0
            ),
            "hedge_fill_price": float(
                getattr(pending, "hedge_fill_price", 0.0) or 0.0
            ),
            "maker_fill_timestamp_quality": str(
                getattr(pending, "maker_fill_timestamp_quality", "") or ""
            ),
            "hedge_fill_timestamp_quality": str(
                getattr(pending, "hedge_fill_timestamp_quality", "") or ""
            ),
            "truth_contract": "positive_fill_requires_direction_correct_live_balance",
        }
        self.ctx.journal.append(
            "pending_entry.positive_fill_live_truth_conflict",
            {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "maker_leg_filled": pending.maker_leg_filled,
                "hedge_leg_filled": pending.hedge_leg_filled,
                "matched_quantity": decision.matched_quantity,
                "residual_quantity": decision.residual_quantity,
                "live_long_quantity": decision.live_long_quantity,
                "live_short_quantity": decision.live_short_quantity,
                "live_balanced_quantity": decision.live_balanced_quantity,
                "reason": decision.reason,
                "maker_fill_truth": maker_fill_truth,
                "live_position_details": dict(
                    getattr(live_truth, "live_position_details", {}) or {}
                ),
            },
        )
        return await self._cleanup_positive_fill_live_truth_conflict(
            pending=pending,
            entry_id=entry_id,
            decision=decision,
            live_truth=live_truth,
            now_ms=now_ms,
        )

    def _remove_pending_entry_after_terminal_decision(
        self,
        entry_id: str,
        *,
        reason: str,
    ) -> None:
        """Single runtime authority for pending-entry removal.

        Callers must reach this only after a terminalizer, abort, cleanup, or
        reconciliation decision has proven that retaining the pending entry is
        no longer the V1-safe action.
        """
        pending = self.ctx.state.pending_entries.get(entry_id)
        owner_id = entry_id
        if isinstance(pending, dict):
            owner_id = str(
                pending.get("pending_id")
                or pending.get("position_id")
                or pending.get("entry_id")
                or entry_id
            )
        elif pending is not None:
            owner_id = str(
                getattr(pending, "pending_id", "")
                or getattr(pending, "position_id", "")
                or getattr(pending, "entry_id", "")
                or entry_id
            )
        closure_fields = self.ctx._v1_lifecycle_event_fields(
            phase="PENDING_ENTRY",
            owner_id=owner_id,
        )
        closure_phase = closure_fields.get("closure_phase", "PENDING_ENTRY")
        closure_row_key = closure_fields.get("closure_row_key", "")
        closure_decision_id = closure_fields.get("closure_decision_id", "")
        # _remove_pending_entry_after_terminal_decision is the only direct pop authority.
        removed = self.ctx.state.pending_entries.pop(entry_id, None)
        if removed is not None:
            self.ctx.journal.append(
                "pending_entry.removed_by_v1_lifecycle_closure",
                {
                    "entry_id": entry_id,
                    "owner_id": owner_id,
                    "reason": reason,
                    "closure_phase": closure_phase,
                    "closure_row_key": closure_row_key,
                    "closure_decision_id": closure_decision_id,
                },
            )

    async def _cleanup_positive_fill_live_truth_conflict(
        self,
        *,
        pending: Any,
        entry_id: str,
        decision: PendingEntryTerminalDecision,
        live_truth: PendingEntryLiveTruth,
        now_ms: int,
    ) -> bool:
        """Clean an owned single-leg live conflict before releasing pending."""

        if not live_truth.available or live_truth.has_live_open_order:
            return False

        return await self._cleanup_owned_single_leg_live_truth_conflict(
            pending=pending,
            entry_id=entry_id,
            live_long_quantity=decision.live_long_quantity,
            live_short_quantity=decision.live_short_quantity,
            matched_quantity=decision.matched_quantity,
            reason=decision.reason,
            now_ms=now_ms,
        )

    async def _cleanup_zero_fill_live_truth_conflict(
        self,
        *,
        pending: Any,
        entry_id: str,
        live_truth: PendingEntryLiveTruth,
        now_ms: int,
    ) -> bool:
        """Clean an owned zero-fill pending entry once live truth proves exposure."""

        if (
            not live_truth.available
            or live_truth.has_live_open_order
            or not live_truth.has_live_position
        ):
            return False

        return await self._cleanup_owned_single_leg_live_truth_conflict(
            pending=pending,
            entry_id=entry_id,
            live_long_quantity=live_truth.live_long_quantity,
            live_short_quantity=live_truth.live_short_quantity,
            matched_quantity=0.0,
            reason="zero_fill_live_position_truth_present",
            now_ms=now_ms,
        )

    async def _ensure_pending_entry_maker_terminal_before_single_leg_release(
        self,
        pending: Any,
        entry_id: str,
        *,
        reason: str,
        now_ms: int,
    ) -> tuple[bool, dict[str, Any]]:
        if not self.ctx._pending_entry_has_maker_order_reference(pending):
            evidence = {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "reason": "maker_order_reference_unavailable_before_pending_release",
                "source": reason,
                "result": "maker_order_not_terminal_before_pending_release",
                "has_live_open_order": False,
            }
            if str(getattr(self.ctx.config.runtime, "mode", "") or "") != "live":
                return True, evidence
            pending.uncertain_outcome = True
            pending.reconcile_next_attempt_ms = max(
                int(getattr(pending, "reconcile_next_attempt_ms", 0) or 0),
                now_ms + 10_000,
            )
            self.ctx.journal.append(
                "pending_entry.release_maker_order_reference_unavailable",
                evidence,
            )
            return False, evidence

        ok, payload = await self._ensure_pending_entry_maker_terminal_proof(
            pending,
            entry_id,
            reason=reason,
            now_ms=now_ms,
            scope="release",
        )
        if ok:
            return True, payload

        pending.uncertain_outcome = True
        pending.reconcile_next_attempt_ms = max(
            int(getattr(pending, "reconcile_next_attempt_ms", 0) or 0),
            now_ms + 10_000,
        )
        if "reason" not in payload:
            payload["reason"] = "maker_order_not_terminal_before_pending_release"
        return False, payload

    async def _cleanup_owned_single_leg_live_truth_conflict(
        self,
        *,
        pending: Any,
        entry_id: str,
        live_long_quantity: float,
        live_short_quantity: float,
        matched_quantity: float,
        reason: str,
        now_ms: int,
    ) -> bool:
        """Flatten one owned live leg and release pending only after fresh flat truth."""

        eps = 1e-9
        live_long_quantity = max(float(live_long_quantity or 0.0), 0.0)
        live_short_quantity = max(float(live_short_quantity or 0.0), 0.0)
        has_live_long = live_long_quantity > eps
        has_live_short = live_short_quantity > eps
        if has_live_long == has_live_short:
            self.ctx.journal.append(
                "pending_entry.owned_live_conflict_cleanup_skipped",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "live_long_quantity": live_long_quantity,
                    "live_short_quantity": live_short_quantity,
                    "reason": (
                        "ambiguous_live_position_shape"
                        if has_live_long
                        else "no_live_single_leg_to_cleanup"
                    ),
                },
            )
            return False

        cleanup_venue = pending.long_venue if has_live_long else pending.short_venue
        live_side = Side.BUY if has_live_long else Side.SELL
        live_quantity = live_long_quantity if has_live_long else live_short_quantity
        stage = (
            "owned_pending_entry_live_conflict_long"
            if has_live_long
            else "owned_pending_entry_live_conflict_short"
        )
        self.ctx.journal.append(
            "pending_entry.owned_live_conflict_cleanup_attempt",
            {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "venue": cleanup_venue.value,
                "stage": stage,
                "live_position_side": live_side.value,
                "live_position_quantity": live_quantity,
                "matched_quantity": matched_quantity,
                "live_long_quantity": live_long_quantity,
                "live_short_quantity": live_short_quantity,
                "reason": reason,
            },
        )
        self.ctx.journal.append(
            "pending_entry.single_leg_flatten_submitted",
            {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "venue": cleanup_venue.value,
                "stage": stage,
                "live_position_side": live_side.value,
                "cleanup_side": live_side.opposite().value,
                "live_position_quantity": live_quantity,
                "matched_quantity": matched_quantity,
                "live_long_quantity": live_long_quantity,
                "live_short_quantity": live_short_quantity,
                "reason": reason,
                "source": "owned_pending_entry_live_conflict",
                "ts_ms": now_ms,
            },
        )

        cleanup_result = await self.ctx._cleanup_failed_leg_exposure(
            cleanup_venue,
            pending.symbol,
            entry_id,
            stage,
        )
        if cleanup_result is not True:
            pending.reconcile_next_attempt_ms = max(
                int(getattr(pending, "reconcile_next_attempt_ms", 0) or 0),
                now_ms + 10_000,
            )
            self.ctx.journal.append(
                "pending_entry.owned_live_conflict_cleanup_failed",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "venue": cleanup_venue.value,
                    "stage": stage,
                    "live_position_side": live_side.value,
                    "live_position_quantity": live_quantity,
                    "result": "adapter_unavailable"
                    if cleanup_result is None
                    else "failed",
                    "reason": "reduce_only_cleanup_failed",
                },
            )
            self.ctx.journal.append(
                "pending_entry.single_leg_flatten_failed",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "venue": cleanup_venue.value,
                    "stage": stage,
                    "live_position_side": live_side.value,
                    "live_position_quantity": live_quantity,
                    "result": "adapter_unavailable"
                    if cleanup_result is None
                    else "failed",
                    "reason": "reduce_only_cleanup_failed",
                    "source": "owned_pending_entry_live_conflict",
                    "ts_ms": now_ms,
                },
            )
            return False

        fresh_truth = await self.ctx._pending_entry_positive_fill_live_truth(
            pending,
            entry_id,
            now_ms,
        )
        post_long = max(float(fresh_truth.live_long_quantity or 0.0), 0.0)
        post_short = max(float(fresh_truth.live_short_quantity or 0.0), 0.0)
        post_flat = (
            fresh_truth.available
            and not fresh_truth.has_live_open_order
            and not fresh_truth.has_live_position
            and post_long <= eps
            and post_short <= eps
        )
        if not post_flat:
            pending.reconcile_next_attempt_ms = max(
                int(getattr(pending, "reconcile_next_attempt_ms", 0) or 0),
                now_ms + 10_000,
            )
            self.ctx.journal.append(
                "pending_entry.owned_live_conflict_cleanup_failed",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "venue": cleanup_venue.value,
                    "stage": stage,
                    "live_position_side": live_side.value,
                    "live_position_quantity": live_quantity,
                    "result": "post_cleanup_truth_not_flat"
                    if fresh_truth.available
                    else "post_cleanup_truth_unavailable",
                    "post_cleanup_live_long_quantity": post_long,
                    "post_cleanup_live_short_quantity": post_short,
                    "post_cleanup_has_live_open_order": fresh_truth.has_live_open_order,
                    "post_cleanup_has_live_position": fresh_truth.has_live_position,
                    "post_cleanup_error": fresh_truth.error,
                    "reason": "fresh_live_truth_required_before_pending_release",
                },
            )
            self.ctx.journal.append(
                "pending_entry.single_leg_flatten_failed",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "venue": cleanup_venue.value,
                    "stage": stage,
                    "live_position_side": live_side.value,
                    "live_position_quantity": live_quantity,
                    "result": "post_cleanup_truth_not_flat"
                    if fresh_truth.available
                    else "post_cleanup_truth_unavailable",
                    "post_cleanup_live_long_quantity": post_long,
                    "post_cleanup_live_short_quantity": post_short,
                    "post_cleanup_has_live_open_order": fresh_truth.has_live_open_order,
                    "post_cleanup_has_live_position": fresh_truth.has_live_position,
                    "post_cleanup_error": fresh_truth.error,
                    "reason": "fresh_live_truth_required_before_pending_release",
                    "source": "owned_pending_entry_live_conflict",
                    "ts_ms": now_ms,
                },
            )
            return False

        maker_terminal, maker_release = (
            await self._ensure_pending_entry_maker_terminal_before_single_leg_release(
                pending,
                entry_id,
                reason=reason,
                now_ms=now_ms,
            )
        )
        if not maker_terminal:
            pending.reconcile_next_attempt_ms = max(
                int(getattr(pending, "reconcile_next_attempt_ms", 0) or 0),
                now_ms + 10_000,
            )
            failure_payload = {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "venue": cleanup_venue.value,
                "stage": stage,
                "live_position_side": live_side.value,
                "live_position_quantity": live_quantity,
                "result": "maker_order_not_terminal_before_pending_release",
                "post_cleanup_live_long_quantity": post_long,
                "post_cleanup_live_short_quantity": post_short,
                "post_cleanup_has_live_open_order": bool(
                    maker_release.get("has_live_open_order")
                ),
                "post_cleanup_has_live_position": fresh_truth.has_live_position,
                "post_cleanup_error": maker_release.get("error", fresh_truth.error),
                "maker_release_evidence": maker_release,
                "reason": "maker_order_not_terminal_before_pending_release",
            }
            self.ctx.journal.append(
                "pending_entry.owned_live_conflict_cleanup_failed",
                failure_payload,
            )
            self.ctx.journal.append(
                "pending_entry.single_leg_flatten_failed",
                {
                    **failure_payload,
                    "source": "owned_pending_entry_live_conflict",
                    "ts_ms": now_ms,
                },
            )
            return False

        self.ctx.journal.append(
            "pending_entry.owned_live_conflict_cleanup_succeeded",
            {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "venue": cleanup_venue.value,
                "stage": stage,
                "live_position_side": live_side.value,
                "live_position_quantity": live_quantity,
                "post_cleanup_live_long_quantity": post_long,
                "post_cleanup_live_short_quantity": post_short,
                "maker_release_evidence": maker_release,
                "reason": "owned_single_leg_flattened_and_fresh_truth_flat",
            },
        )
        self.ctx.journal.append(
            "pending_entry.single_leg_flatten_succeeded",
            {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "venue": cleanup_venue.value,
                "stage": stage,
                "live_position_side": live_side.value,
                "live_position_quantity": live_quantity,
                "post_cleanup_live_long_quantity": post_long,
                "post_cleanup_live_short_quantity": post_short,
                "maker_release_evidence": maker_release,
                "reason": "owned_single_leg_flattened_and_fresh_truth_flat",
                "source": "owned_pending_entry_live_conflict",
                "ts_ms": now_ms,
            },
        )
        await self.ctx._complete_pending_entry_terminal_removal(
            entry_id,
            reason="owned_live_conflict_cleanup_succeeded",
            symbol=pending.symbol,
            now_ms=now_ms,
        )
        self.ctx.journal.append(
            "pending_entry.terminalized_after_single_leg_recovery",
            {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "venue": cleanup_venue.value,
                "stage": stage,
                "reason": "owned_live_conflict_cleanup_succeeded",
                "source": "owned_pending_entry_live_conflict",
                "ts_ms": now_ms,
            },
        )
        return True

    async def _complete_pending_entry_terminal_removal(
        self,
        entry_id: str,
        *,
        reason: str,
        symbol: str = "",
        now_ms: int | None = None,
    ) -> None:
        """Remove a terminal pending entry and re-run recovery-core release.

        V1 terminalizes a pending entry only after order/fill/position truth
        proves the maker owner is gone. Once V2 removes that local owner, stale
        recovery ledger work must be rebuilt immediately so risk_only does not
        remain latched on a no-work/no-artifact state.
        """
        pending = self.ctx.state.pending_entries.get(entry_id)
        pending_symbol = str(symbol or getattr(pending, "symbol", "") or "").upper()
        if pending is None and not pending_symbol:
            return
        source_symbols = self.ctx._truth_required_recovery_probe_symbol_sources(
            [pending_symbol] if pending_symbol else []
        )
        self.ctx._remove_pending_entry_after_terminal_decision(entry_id, reason=reason)
        if not self.ctx._pending_entry_terminal_needs_recovery_core_refresh():
            return
        await self.ctx._refresh_recovery_core_after_pending_entry_terminal(
            reason=reason,
            symbol=pending_symbol,
            source_symbols=source_symbols,
            now_ms=now_ms if now_ms is not None else wall_clock_now_ms(),
        )

    def _pending_entry_terminal_needs_recovery_core_refresh(self) -> bool:
        if self.ctx.state.lifecycle == EngineLifecycle.RISK_ONLY:
            return True
        if self.ctx.state.recovery_blocked_reason:
            return True
        if getattr(self.ctx.recovery_ledger, "work_items", None):
            return True
        recovery_decision = getattr(self.ctx, "recovery_decision", None)
        if recovery_decision is not None and not recovery_decision.entry_allowed:
            return True
        return False

    @staticmethod
    def _pending_entry_terminalizer_decision_payload(
        entry_id: str,
        pending: Any,
        decision: PendingEntryTerminalDecision,
        now_ms: int,
    ) -> dict[str, Any]:
        return {
            "entry_id": entry_id,
            "symbol": getattr(pending, "symbol", ""),
            "outcome": decision.outcome,
            "reason": decision.reason,
            "terminal": decision.terminal,
            "allows_pending_removal": decision.allows_pending_removal,
            "healthy": decision.healthy,
            "operator_block_required": decision.operator_block_required,
            "matched_quantity": decision.matched_quantity,
            "residual_quantity": decision.residual_quantity,
            "live_long_quantity": decision.live_long_quantity,
            "live_short_quantity": decision.live_short_quantity,
            "live_balanced_quantity": decision.live_balanced_quantity,
            "contains_positive_fill_evidence": (
                decision.contains_positive_fill_evidence
            ),
            "truth_gate_decision": getattr(
                decision,
                "truth_gate_decision",
                "",
            ),
            "order_truth_refs": getattr(decision, "order_truth_refs", []),
            "trade_truth_refs": getattr(decision, "trade_truth_refs", []),
            "live_position_truth_refs": getattr(
                decision,
                "live_position_truth_refs",
                [],
            ),
            "owner_id": entry_id,
            "ts_ms": now_ms,
        }
