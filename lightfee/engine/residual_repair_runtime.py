"""Residual repair runtime delegate.

This module owns the mechanical implementation moved from LiveRuntime.
Do not change residual repair business conditions while extracting it.
"""

from __future__ import annotations

from typing import Any

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import (
    OrderFill,
    OrderRequest,
    PositionSnapshot,
    Side,
    TimeInForce,
    Venue,
)
from lightfee.core.errors import OrderSubmitError
from lightfee.engine.bybit_duplicate_reconcile import (
    BYBIT_DUPLICATE_RECONCILE_ENDPOINTS,
    build_order_reconcile_result_payload,
    reconcile_bybit_duplicate_client_order,
)
from lightfee.engine.close_executor import _is_bybit_duplicate_order_link_id
from lightfee.engine.lifecycle import clear_risk_mode_for_recovery, enter_fail_closed
from lightfee.engine.order_submit_uncertainty import (
    build_order_submit_uncertainty_payload,
    order_truth_probe_paths,
)
from lightfee.engine.reconciliation import _recon_fill_price
from lightfee.engine.recovery_decision_core import (
    CORE_CLEARABLE_BLOCK_REASONS,
    RecoveryEvidenceSnapshot,
    V1RecoveryDecisionCore,
)
from lightfee.engine.runtime_context import RuntimeContext
from lightfee.risk.modes import GlobalRiskMode


class ResidualRepairRuntime:
    def __init__(self, ctx: RuntimeContext) -> None:
        self.ctx = ctx

    @property
    def state(self):
        return self.ctx.state

    @property
    def config(self):
        return self.ctx.config

    @property
    def journal(self):
        return self.ctx.journal

    @property
    def snapshot_store(self):
        return self.ctx.snapshot_store

    def get_venue_adapter(self, venue: Venue) -> VenueAdapter | None:
        return self.ctx.get_venue_adapter(venue)

    def _flush_adapter_order_diagnostics(self, adapter: Any) -> None:
        return self.ctx._flush_adapter_order_diagnostics(adapter)

    def _recovery_state_collection(self, name: str) -> list[Any]:
        return self.ctx._recovery_state_collection(name)

    def _venue_min_notional(self, venue: Venue, symbol: str) -> float:
        return self.ctx._venue_min_notional(venue, symbol)

    def _safe_positive_float(self, value: Any) -> float:
        return self.ctx._safe_positive_float(value)

    def _close_reconciliation_fill_qty(self, fill: Any) -> float:
        return self.ctx._close_reconciliation_fill_qty(fill)

    def _v1_lifecycle_event_fields(
        self,
        *,
        phase: str,
        owner_id: str,
        now_ms: int,
    ) -> dict[str, str]:
        return self.ctx._v1_lifecycle_event_fields(
            phase=phase,
            owner_id=owner_id,
            now_ms=now_ms,
        )

    async def recover_residual_repairs(self, now_ms: int) -> None:
        """Process ready pending residual repair tasks during normal runtime."""
        if not self.state.pending_residual_repairs:
            return

        from lightfee.core.domain import OrderRequest
        from lightfee.venues.common import venue_reduce_only_close_exempts_min_notional

        repaired = 0
        for task in list(self.state.pending_residual_repairs):
            if not isinstance(task, dict):
                continue

            fields = self._pending_residual_repair_fields(task)
            if fields is None:
                self.journal.append(
                    "recovery.residual_repair_invalid_removed",
                    {"position_id": task.get("position_id", ""), "symbol": task.get("symbol", "")},
                )
                self.state.pending_residual_repairs.remove(task)
                continue

            repair_venue, repair_side, task_repair_quantity = fields
            position_id = task.get("position_id", "")
            pair_id = task.get("pair_id", "")
            symbol = task.get("symbol", "")

            is_locally_paused = bool(task.get("local_entry_paused", False))
            next_attempt_ms = int(task.get("next_attempt_ms", 0) or 0)
            if next_attempt_ms > 0 and now_ms < next_attempt_ms:
                continue

            adapter = self.get_venue_adapter(repair_venue)
            if adapter is None:
                if (
                    is_locally_paused
                    or self._residual_repair_deadline_or_attempts_exhausted(task, now_ms)
                ):
                    self._pause_pending_residual_repair(task, now_ms)
                    continue
                self._reschedule_pending_residual_repair_task(task, now_ms, "adapter_missing")
                self.journal.append(
                    "recovery.residual_repair_failed",
                    {
                        "position_id": position_id,
                        "pair_id": pair_id,
                        "symbol": symbol,
                        "repair_venue": repair_venue.value,
                        "repair_side": repair_side.value,
                        "repair_quantity": task_repair_quantity,
                        "error": "adapter_missing",
                    },
                )
                continue

            probe_venues = [repair_venue]
            if pair_id and "->" in pair_id:
                try:
                    venue_part = pair_id.split(":", 1)[1]
                except IndexError:
                    venue_part = ""
                for raw_venue in venue_part.split("->"):
                    try:
                        parsed = Venue.from_str(raw_venue)
                    except Exception:
                        continue
                    if parsed not in probe_venues and self.get_venue_adapter(parsed) is not None:
                        probe_venues.append(parsed)

            baseline = self._residual_repair_baseline_size(task, repair_venue)
            accepted_order_id = str(task.get("accepted_order_id", "") or "")
            accepted_client_order_id = str(
                task.get("accepted_client_order_id", "") or ""
            )
            if accepted_order_id or accepted_client_order_id:
                status, accepted_fill, accepted_payload = (
                    await self._resolve_residual_repair_accepted_order(
                        task=task,
                        adapter=adapter,
                        repair_venue=repair_venue,
                        repair_side=repair_side,
                        symbol=symbol,
                        baseline=baseline,
                        probe_venues=probe_venues,
                        accepted_order_id=accepted_order_id,
                        accepted_client_order_id=accepted_client_order_id,
                        now_ms=now_ms,
                    )
                )
                if status == "filled" and accepted_fill is not None:
                    live_excess_quantity = float(
                        task.get("repair_quantity", task_repair_quantity) or 0.0
                    )
                    remaining_quantity = max(
                        live_excess_quantity - float(accepted_fill.quantity or 0.0),
                        0.0,
                    )
                    self.state.pending_residual_repairs.remove(task)
                    self._clear_residual_repair_accepted_order_gap(task)
                    if remaining_quantity > 1e-9:
                        updated = dict(task)
                        updated["repair_venue"] = repair_venue.value
                        updated["repair_side"] = repair_side.value
                        updated["repair_quantity"] = remaining_quantity
                        updated["retry_count"] = 0
                        updated["last_attempt_at_ms"] = now_ms
                        updated["next_attempt_ms"] = now_ms
                        self.state.pending_residual_repairs.append(updated)
                    else:
                        self._release_residual_repair_pair_gate(pair_id, symbol)
                        repaired += 1
                    completed_payload = {
                        "position_id": position_id,
                        "pair_id": pair_id,
                        "symbol": symbol,
                        "origin": task.get("origin", ""),
                        "repair_venue": repair_venue.value,
                        "repair_side": repair_side.value,
                        "result": "accepted_order_reconciled",
                        "requested_quantity": live_excess_quantity,
                        "filled_quantity": float(accepted_fill.quantity or 0.0),
                        "remaining_quantity": remaining_quantity,
                        "fill_order_id": getattr(accepted_fill, "order_id", ""),
                        "fill_price": float(getattr(accepted_fill, "price", 0.0) or 0.0),
                    }
                    completed_payload.update(accepted_payload)
                    self.journal.append(
                        "execution.residual_repair_completed",
                        completed_payload,
                    )
                    continue
                if status == "live_flat":
                    self.state.pending_residual_repairs.remove(task)
                    self._clear_residual_repair_accepted_order_gap(task)
                    self._release_residual_repair_pair_gate(pair_id, symbol)
                    repaired += 1
                    completed_payload = {
                        "position_id": position_id,
                        "pair_id": pair_id,
                        "symbol": symbol,
                        "origin": task.get("origin", ""),
                        "repair_venue": repair_venue.value,
                        "repair_side": repair_side.value,
                        "result": "accepted_order_live_flat",
                    }
                    completed_payload.update(accepted_payload)
                    self.journal.append(
                        "execution.residual_repair_completed",
                        completed_payload,
                    )
                    continue

                self._retain_residual_repair_accepted_order_gap(
                    task,
                    now_ms,
                    status=status,
                    accepted_order_id=accepted_order_id,
                    accepted_client_order_id=accepted_client_order_id,
                )
                failed_payload = {
                    "position_id": position_id,
                    "pair_id": pair_id,
                    "symbol": symbol,
                    "repair_venue": repair_venue.value,
                    "repair_side": repair_side.value,
                    "repair_quantity": task_repair_quantity,
                    "error": task["last_error"],
                }
                failed_payload.update(accepted_payload)
                self.journal.append(
                    "recovery.residual_repair_failed",
                    failed_payload,
                )
                continue

            live_positions: dict[Venue, PositionSnapshot | None] = {}
            open_order_count = 0
            open_order_counts_by_venue: dict[str, int] = {}
            live_truth_error = ""
            for probe_venue in probe_venues:
                probe_adapter = self.get_venue_adapter(probe_venue)
                if probe_adapter is None:
                    continue
                try:
                    live_positions[probe_venue] = await probe_adapter.fetch_position(symbol)
                except Exception as e:
                    live_truth_error = str(e) or e.__class__.__name__
                    break

                try:
                    open_orders = await self._fetch_residual_repair_open_orders(
                        probe_adapter, probe_venue, symbol,
                    )
                except Exception as e:
                    live_truth_error = str(e) or e.__class__.__name__
                    break
                venue_open_order_count = len(open_orders)
                open_order_count += venue_open_order_count
                open_order_counts_by_venue[probe_venue.value] = venue_open_order_count

            if live_truth_error:
                error = f"residual_repair_live_truth_untrusted:{live_truth_error}"
                if (
                    is_locally_paused
                    or self._residual_repair_deadline_or_attempts_exhausted(task, now_ms)
                ):
                    self.journal.append(
                        "recovery.residual_repair_failed",
                        {
                            "position_id": position_id,
                            "pair_id": pair_id,
                            "symbol": symbol,
                            "repair_venue": repair_venue.value,
                            "repair_side": repair_side.value,
                            "repair_quantity": task_repair_quantity,
                            "error": error,
                        },
                    )
                    task["last_error"] = error
                    self._pause_pending_residual_repair(task, now_ms)
                    continue
                self._reschedule_pending_residual_repair_task(task, now_ms, error)
                self.journal.append(
                    "recovery.residual_repair_failed",
                    {
                        "position_id": position_id,
                        "pair_id": pair_id,
                        "symbol": symbol,
                        "repair_venue": repair_venue.value,
                        "repair_side": repair_side.value,
                        "repair_quantity": task_repair_quantity,
                        "error": error,
                    },
                )
                continue

            live_position = live_positions.get(repair_venue)
            live_size = self._signed_position_size(live_position)
            if repair_side == Side.SELL:
                live_excess_quantity = max(live_size - baseline, 0.0)
            else:
                live_excess_quantity = max(baseline - live_size, 0.0)

            if live_excess_quantity <= 1e-9:
                has_local_position = position_id in self.state.open_positions
                all_probed_positions_flat = all(
                    abs(self._signed_position_size(pos)) <= 1e-9
                    for pos in live_positions.values()
                )
                if open_order_count > 0:
                    error = "residual_repair_live_open_orders_present"
                    pause_evidence = {
                        "open_order_count": open_order_count,
                        "open_order_counts_by_venue": open_order_counts_by_venue,
                        "live_truth_venues": [venue.value for venue in probe_venues],
                        "live_excess_quantity": live_excess_quantity,
                        "baseline_quantity": baseline,
                        "live_size": live_size,
                    }
                    if (
                        is_locally_paused
                        or self._residual_repair_deadline_or_attempts_exhausted(task, now_ms)
                    ):
                        task["last_error"] = error
                        self._pause_pending_residual_repair(task, now_ms, pause_evidence)
                        task["last_error"] = error
                    else:
                        self._reschedule_pending_residual_repair_task(task, now_ms, error)
                    continue
                if (
                    not has_local_position
                    and abs(live_size) > 1e-9
                    and live_position is not None
                ):
                    original_repair_side = repair_side
                    repair_side = Side.SELL if live_size > 0.0 else Side.BUY
                    live_excess_quantity = abs(live_size)
                    task["repair_side"] = repair_side.value
                    task["repair_quantity"] = live_excess_quantity
                    self.journal.append(
                        "execution.residual_repair_side_rebuilt_from_live_truth",
                        {
                            "position_id": position_id,
                            "pair_id": pair_id,
                            "symbol": symbol,
                            "origin": task.get("origin", ""),
                            "repair_venue": repair_venue.value,
                            "original_repair_side": original_repair_side.value,
                            "repair_side": repair_side.value,
                            "live_size": live_size,
                            "live_excess_quantity": live_excess_quantity,
                            "baseline_quantity": baseline,
                        },
                    )
                elif not has_local_position and not all_probed_positions_flat:
                    error = "residual_repair_live_position_nonzero"
                    if (
                        is_locally_paused
                        or self._residual_repair_deadline_or_attempts_exhausted(task, now_ms)
                    ):
                        task["last_error"] = error
                        self._pause_pending_residual_repair(task, now_ms)
                        task["last_error"] = error
                    else:
                        self._reschedule_pending_residual_repair_task(task, now_ms, error)
                    continue
                else:
                    self.state.pending_residual_repairs.remove(task)
                    self._release_residual_repair_pair_gate(pair_id, symbol)
                    repaired += 1
                    self.journal.append(
                        "execution.residual_repair_completed",
                        {
                            "position_id": position_id,
                            "pair_id": pair_id,
                            "symbol": symbol,
                            "origin": task.get("origin", ""),
                            "repair_venue": repair_venue.value,
                            "repair_side": repair_side.value,
                            "result": "already_flat",
                            "open_order_count": open_order_count,
                            "open_order_counts_by_venue": open_order_counts_by_venue,
                            "live_truth_venues": [venue.value for venue in probe_venues],
                            "live_positions": self._live_positions_evidence(live_positions),
                            "live_excess_quantity": live_excess_quantity,
                            "baseline_quantity": baseline,
                            "live_size": live_size,
                        },
                    )
                    continue

            repair_quantity = live_excess_quantity
            if hasattr(adapter, "normalize_quantity"):
                try:
                    repair_quantity = await adapter.normalize_quantity(symbol, repair_quantity)
                except Exception as e:
                    self._reschedule_pending_residual_repair_task(task, now_ms, str(e))
                    self.journal.append(
                        "recovery.residual_repair_failed",
                        {
                            "position_id": position_id,
                            "pair_id": pair_id,
                            "symbol": symbol,
                            "repair_venue": repair_venue.value,
                            "repair_side": repair_side.value,
                            "repair_quantity": live_excess_quantity,
                            "error": str(e),
                        },
                    )
                    continue
            matched_quantity = 0.0
            residual_ratio = 0.0
            if task.get("origin") == "entry_open":
                open_position = self.state.open_positions.get(position_id)
                if open_position is not None:
                    matched_quantity = abs(
                        float(getattr(open_position, "matched_quantity", 0.0) or 0.0)
                    )
                if matched_quantity > 1e-9:
                    residual_ratio = (
                        abs(float(live_excess_quantity or 0.0)) / matched_quantity
                    )

            if repair_quantity <= 1e-9:
                if repair_venue == Venue.OKX:
                    live_price = abs(float(getattr(live_position, "entry_price", 0.0) or 0.0))
                    if (
                        task.get("origin") == "entry_open"
                        and matched_quantity > 1e-9
                        and residual_ratio > 0.02 + 1e-12
                    ):
                        task["last_error"] = "entry_residual_dust_over_tolerance"
                        self._pause_pending_residual_repair(
                            task,
                            now_ms,
                            evidence={
                                "terminal_reason": "exchange_min_quantity_dust",
                                "live_excess_quantity": live_excess_quantity,
                                "matched_quantity": matched_quantity,
                                "residual_ratio": residual_ratio,
                                "normalized_quantity": repair_quantity,
                            },
                        )
                        continue
                    self._terminalize_residual_repair_task(
                        task,
                        now_ms,
                        terminal_reason="exchange_min_quantity_dust",
                        repair_venue=repair_venue,
                        repair_side=repair_side,
                        repair_quantity=live_excess_quantity,
                        live_price=live_price,
                        min_notional=0.0,
                    )
                    continue
                self._reschedule_pending_residual_repair_task(
                    task, now_ms, "normalized_repair_quantity_zero"
                )
                self.journal.append(
                    "recovery.residual_repair_failed",
                    {
                        "position_id": position_id,
                        "pair_id": pair_id,
                        "symbol": symbol,
                        "repair_venue": repair_venue.value,
                        "repair_side": repair_side.value,
                        "repair_quantity": live_excess_quantity,
                        "error": "normalized_repair_quantity_zero",
                    },
                )
                continue

            min_notional = self._venue_min_notional(repair_venue, symbol)
            live_price = abs(float(getattr(live_position, "entry_price", 0.0) or 0.0))
            if (
                min_notional > 0
                and live_price > 0
                and repair_quantity * live_price + 1e-12 < min_notional
                and not venue_reduce_only_close_exempts_min_notional(repair_venue)
            ):
                if (
                    task.get("origin") == "entry_open"
                    and matched_quantity > 1e-9
                    and residual_ratio > 0.02 + 1e-12
                ):
                    task["last_error"] = "entry_residual_dust_over_tolerance"
                    self._pause_pending_residual_repair(
                        task,
                        now_ms,
                        evidence={
                            "terminal_reason": "exchange_min_notional_dust",
                            "live_excess_quantity": live_excess_quantity,
                            "repair_quantity": repair_quantity,
                            "live_price": live_price,
                            "min_notional": min_notional,
                            "matched_quantity": matched_quantity,
                            "residual_ratio": residual_ratio,
                        },
                    )
                    continue
                self._terminalize_residual_repair_task(
                    task,
                    now_ms,
                    terminal_reason="exchange_min_notional_dust",
                    repair_venue=repair_venue,
                    repair_side=repair_side,
                    repair_quantity=repair_quantity,
                    live_price=live_price,
                    min_notional=min_notional,
                )
                continue

            if (
                is_locally_paused
                or self._residual_repair_deadline_or_attempts_exhausted(task, now_ms)
            ):
                self.journal.append(
                    "execution.residual_repair_resumed",
                    {
                        "position_id": position_id,
                        "pair_id": pair_id,
                        "symbol": symbol,
                        "origin": task.get("origin", ""),
                        "repair_venue": repair_venue.value,
                        "repair_side": repair_side.value,
                        "live_excess_quantity": live_excess_quantity,
                        "repair_quantity": repair_quantity,
                        "baseline_quantity": baseline,
                        "live_size": live_size,
                        "open_order_count": open_order_count,
                        "open_order_counts_by_venue": open_order_counts_by_venue,
                        "previous_error": task.get("last_error", ""),
                        "retry_count": self._residual_repair_attempt_count(task),
                    },
                )

            next_client_order_id = str(task.pop("next_client_order_id", "") or "")
            current_duplicate_attempt = int(
                task.pop(
                    "next_duplicate_attempt",
                    self._residual_repair_attempt_count(task),
                )
                or 0
            )
            cleanup_client_order_id = (
                next_client_order_id
                or self._residual_repair_client_order_id(
                    position_id,
                    current_duplicate_attempt,
                )
            )
            req = OrderRequest(
                venue=repair_venue,
                symbol=symbol,
                side=repair_side,
                quantity=repair_quantity,
                price=None,
                post_only=False,
                reduce_only=True,
                time_in_force=TimeInForce.IOC,
                client_order_id=cleanup_client_order_id,
            )
            fill = None
            duplicate_live_nonzero_error = ""
            duplicate_live_nonzero_evidence: dict[str, Any] | None = None
            try:
                fill = await adapter.place_order(req)
                self._flush_adapter_order_diagnostics(adapter)
            except Exception as e:
                self._flush_adapter_order_diagnostics(adapter)
                if (
                    repair_venue == Venue.BYBIT
                    and _is_bybit_duplicate_order_link_id(str(e))
                ):
                    duplicate_reconcile = await reconcile_bybit_duplicate_client_order(
                        adapter=adapter,
                        symbol=symbol,
                        client_order_id=req.client_order_id or "",
                        target_qty=repair_quantity,
                        live_pos_before=live_position,
                    )
                    self.journal.append(
                        "order.reconcile_result",
                        build_order_reconcile_result_payload(
                            result=duplicate_reconcile,
                            symbol=symbol,
                            client_order_id=req.client_order_id or "",
                            reason="duplicate_client_id",
                        ),
                    )
                    duplicate_payload = {
                        "position_id": position_id,
                        "pair_id": pair_id,
                        "symbol": symbol,
                        "origin": task.get("origin", ""),
                        "repair_venue": repair_venue.value,
                        "repair_side": repair_side.value,
                        "client_order_id": req.client_order_id,
                        "reconcile_endpoints": list(BYBIT_DUPLICATE_RECONCILE_ENDPOINTS),
                        "classification": duplicate_reconcile.classification,
                        "decision": duplicate_reconcile.decision,
                        "target_qty": duplicate_reconcile.target_qty,
                        "reconciled_qty": duplicate_reconcile.reconciled_qty,
                        "live_qty": duplicate_reconcile.live_qty,
                        "remaining_qty": duplicate_reconcile.remaining_qty,
                        "retry_qty": duplicate_reconcile.retry_qty,
                        "order_id": duplicate_reconcile.order_id,
                        "original_error": str(e),
                    }
                    if duplicate_reconcile.reconcile_error:
                        duplicate_payload["reconcile_error"] = (
                            duplicate_reconcile.reconcile_error
                        )
                    if duplicate_reconcile.live_fetch_error:
                        duplicate_payload["live_fetch_error"] = (
                            duplicate_reconcile.live_fetch_error
                        )
                    self.journal.append(
                        "recovery.residual_repair_duplicate_client_order_reconcile_result",
                        duplicate_payload,
                    )
                    if duplicate_reconcile.clear_state:
                        self.state.pending_residual_repairs.remove(task)
                        self._release_residual_repair_pair_gate(pair_id, symbol)
                        repaired += 1
                        self.journal.append(
                            "execution.residual_repair_completed",
                            {
                                "position_id": position_id,
                                "pair_id": pair_id,
                                "symbol": symbol,
                                "origin": task.get("origin", ""),
                                "repair_venue": repair_venue.value,
                                "repair_side": repair_side.value,
                                "result": "duplicate_client_order_reconciled",
                                "client_order_id": req.client_order_id,
                                "order_id": duplicate_reconcile.order_id,
                                "reconciled_qty": duplicate_reconcile.reconciled_qty,
                                "live_qty": duplicate_reconcile.live_qty,
                            },
                        )
                        continue
                    if duplicate_reconcile.should_retry_with_new_client_id:
                        retry_quantity = duplicate_reconcile.retry_qty
                        if duplicate_reconcile.live_qty > 1e-9:
                            retry_quantity = min(retry_quantity, duplicate_reconcile.live_qty)
                        duplicate_attempt = max(
                            self._residual_repair_attempt_count(task) + 1,
                            current_duplicate_attempt + 1,
                        )
                        retry_client_order_id = self._residual_repair_client_order_id(
                            position_id,
                            duplicate_attempt,
                        )
                        if retry_client_order_id == req.client_order_id:
                            duplicate_attempt += 1
                            retry_client_order_id = (
                                self._residual_repair_client_order_id(
                                    position_id,
                                    duplicate_attempt,
                                )
                            )
                        retry_req = OrderRequest(
                            venue=repair_venue,
                            symbol=symbol,
                            side=repair_side,
                            quantity=retry_quantity,
                            price=None,
                            post_only=False,
                            reduce_only=True,
                            time_in_force=TimeInForce.IOC,
                            client_order_id=retry_client_order_id,
                        )
                        try:
                            fill = await adapter.place_order(retry_req)
                            self._flush_adapter_order_diagnostics(adapter)
                            repair_quantity = retry_quantity
                        except Exception as retry_error:
                            self._flush_adapter_order_diagnostics(adapter)
                            if (
                                repair_venue == Venue.BYBIT
                                and _is_bybit_duplicate_order_link_id(str(retry_error))
                                and duplicate_reconcile.live_qty > 1e-9
                            ):
                                next_retry_count = (
                                    self._residual_repair_attempt_count(task) + 1
                                )
                                duplicate_live_nonzero_error = (
                                    "residual_repair_duplicate_live_nonzero_blocked"
                                    if next_retry_count >= 3
                                    else "residual_repair_duplicate_live_nonzero_retry_failed"
                                )
                                task["next_client_order_id"] = (
                                    self._residual_repair_client_order_id(
                                        position_id,
                                        duplicate_attempt + 1,
                                    )
                                )
                                task["next_duplicate_attempt"] = duplicate_attempt + 1
                                duplicate_live_nonzero_evidence = {
                                    "position_id": position_id,
                                    "pair_id": pair_id,
                                    "symbol": symbol,
                                    "origin": task.get("origin", ""),
                                    "repair_venue": repair_venue.value,
                                    "repair_side": repair_side.value,
                                    "client_order_id": req.client_order_id,
                                    "retry_client_order_id": retry_req.client_order_id,
                                    "next_client_order_id": task["next_client_order_id"],
                                    "classification": duplicate_reconcile.classification,
                                    "decision": duplicate_reconcile.decision,
                                    "target_qty": duplicate_reconcile.target_qty,
                                    "reconciled_qty": duplicate_reconcile.reconciled_qty,
                                    "live_qty": duplicate_reconcile.live_qty,
                                    "remaining_qty": duplicate_reconcile.remaining_qty,
                                    "retry_qty": duplicate_reconcile.retry_qty,
                                    "order_id": duplicate_reconcile.order_id,
                                    "original_error": str(e),
                                    "retry_error": str(retry_error),
                                }
                                e = RuntimeError(duplicate_live_nonzero_error)
                            else:
                                e = retry_error

                if fill is not None:
                    pass
                else:
                    if isinstance(e, OrderSubmitError) and bool(
                        getattr(e, "order_ack_only", False)
                    ):
                        order_gap_evidence = self._order_submit_error_runtime_evidence(
                            e,
                            venue=repair_venue,
                            operation="place_order",
                            request=req,
                            default_client_order_id=req.client_order_id or "",
                        )
                        accepted_order_id = str(
                            getattr(e, "accepted_order_id", "") or ""
                        )
                        accepted_client_order_id = str(
                            getattr(e, "accepted_client_order_id", "")
                            or req.client_order_id
                            or ""
                        )
                        status, accepted_fill, accepted_payload = (
                            await self._resolve_residual_repair_accepted_order(
                                task=task,
                                adapter=adapter,
                                repair_venue=repair_venue,
                                repair_side=repair_side,
                                symbol=symbol,
                                baseline=baseline,
                                probe_venues=probe_venues,
                                accepted_order_id=accepted_order_id,
                                accepted_client_order_id=accepted_client_order_id,
                                now_ms=now_ms,
                            )
                        )
                        accepted_payload = {
                            **order_gap_evidence,
                            **accepted_payload,
                        }
                        if status == "filled" and accepted_fill is not None:
                            remaining_quantity = max(
                                live_excess_quantity
                                - float(accepted_fill.quantity or 0.0),
                                0.0,
                            )
                            self.state.pending_residual_repairs.remove(task)
                            self._clear_residual_repair_accepted_order_gap(task)
                            if remaining_quantity > 1e-9:
                                updated = dict(task)
                                updated["repair_venue"] = repair_venue.value
                                updated["repair_side"] = repair_side.value
                                updated["repair_quantity"] = remaining_quantity
                                updated.pop("exposure_venue", None)
                                updated.pop("exposure_side", None)
                                updated.pop("exposure_quantity", None)
                                updated["retry_count"] = 0
                                updated["last_attempt_at_ms"] = now_ms
                                updated["next_attempt_ms"] = now_ms
                                self.state.pending_residual_repairs.append(updated)
                            else:
                                self._release_residual_repair_pair_gate(pair_id, symbol)
                                repaired += 1
                            completed_payload = {
                                "position_id": position_id,
                                "pair_id": pair_id,
                                "symbol": symbol,
                                "origin": task.get("origin", ""),
                                "repair_venue": repair_venue.value,
                                "repair_side": repair_side.value,
                                "result": "accepted_order_reconciled",
                                "requested_quantity": repair_quantity,
                                "filled_quantity": float(accepted_fill.quantity or 0.0),
                                "remaining_quantity": remaining_quantity,
                                "open_order_count": open_order_count,
                                "open_order_counts_by_venue": open_order_counts_by_venue,
                                "live_truth_venues": [
                                    venue.value for venue in probe_venues
                                ],
                                "live_positions": self._live_positions_evidence(
                                    live_positions
                                ),
                                "live_excess_quantity": live_excess_quantity,
                                "baseline_quantity": baseline,
                                "live_size": live_size,
                                "fill_order_id": getattr(accepted_fill, "order_id", ""),
                                "fill_price": float(
                                    getattr(accepted_fill, "price", 0.0) or 0.0
                                ),
                            }
                            completed_payload.update(accepted_payload)
                            self.journal.append(
                                "execution.residual_repair_completed",
                                completed_payload,
                            )
                            continue
                        if status == "live_flat":
                            self.state.pending_residual_repairs.remove(task)
                            self._clear_residual_repair_accepted_order_gap(task)
                            self._release_residual_repair_pair_gate(pair_id, symbol)
                            repaired += 1
                            completed_payload = {
                                "position_id": position_id,
                                "pair_id": pair_id,
                                "symbol": symbol,
                                "origin": task.get("origin", ""),
                                "repair_venue": repair_venue.value,
                                "repair_side": repair_side.value,
                                "result": "accepted_order_live_flat",
                                "requested_quantity": repair_quantity,
                                "filled_quantity": 0.0,
                                "remaining_quantity": 0.0,
                            }
                            completed_payload.update(accepted_payload)
                            self.journal.append(
                                "execution.residual_repair_completed",
                                completed_payload,
                            )
                            continue
                        self._retain_residual_repair_accepted_order_gap(
                            task,
                            now_ms,
                            status=status,
                            accepted_order_id=accepted_order_id,
                            accepted_client_order_id=accepted_client_order_id,
                        )
                        failed_payload = {
                            "position_id": position_id,
                            "pair_id": pair_id,
                            "symbol": symbol,
                            "repair_venue": repair_venue.value,
                            "repair_side": repair_side.value,
                            "repair_quantity": repair_quantity,
                            "live_excess_quantity": live_excess_quantity,
                            "baseline_quantity": baseline,
                            "live_size": live_size,
                            "open_order_count": open_order_count,
                            "open_order_counts_by_venue": open_order_counts_by_venue,
                            "error": task["last_error"],
                        }
                        failed_payload.update(accepted_payload)
                        self.journal.append(
                            "recovery.residual_repair_failed",
                            failed_payload,
                        )
                        continue

                    self._reschedule_pending_residual_repair_task(task, now_ms, str(e))
                    order_gap_evidence = (
                        self._order_submit_error_runtime_evidence(
                            e,
                            venue=repair_venue,
                            operation="place_order",
                            request=req,
                            default_client_order_id=req.client_order_id or "",
                        )
                        if isinstance(e, OrderSubmitError)
                        else {}
                    )
                    if duplicate_live_nonzero_evidence is not None:
                        task["last_duplicate_cleanup"] = dict(
                            duplicate_live_nonzero_evidence
                        )
                        if (
                            duplicate_live_nonzero_error
                            == "residual_repair_duplicate_live_nonzero_blocked"
                        ):
                            enter_fail_closed(self.state)
                            self.state.recovery_blocked_reason = (
                                duplicate_live_nonzero_error
                            )
                            self.state.recovery_blocked_at_ms = now_ms
                            self.state.last_error = duplicate_live_nonzero_error
                            task["last_error"] = duplicate_live_nonzero_error
                            blocker_payload = dict(duplicate_live_nonzero_evidence)
                            blocker_payload.update({
                                "retry_count": self._residual_repair_attempt_count(task),
                                "blocked_new_entry": True,
                                "ts_ms": now_ms,
                            })
                            self.journal.append(
                                "recovery.residual_repair_duplicate_live_nonzero_blocked",
                                blocker_payload,
                            )
                    failed_payload = {
                            "position_id": position_id,
                            "pair_id": pair_id,
                            "symbol": symbol,
                            "repair_venue": repair_venue.value,
                            "repair_side": repair_side.value,
                            "repair_quantity": repair_quantity,
                            "live_excess_quantity": live_excess_quantity,
                            "baseline_quantity": baseline,
                            "live_size": live_size,
                            "open_order_count": open_order_count,
                            "open_order_counts_by_venue": open_order_counts_by_venue,
                            "error": str(e),
                    }
                    failed_payload.update(order_gap_evidence)
                    self.journal.append(
                        "recovery.residual_repair_failed",
                        failed_payload,
                    )
                    continue

            remaining_quantity = max(live_excess_quantity - float(fill.quantity or 0.0), 0.0)
            self.state.pending_residual_repairs.remove(task)
            if remaining_quantity > 1e-9:
                updated = dict(task)
                updated["repair_venue"] = repair_venue.value
                updated["repair_side"] = repair_side.value
                updated["repair_quantity"] = remaining_quantity
                updated.pop("exposure_venue", None)
                updated.pop("exposure_side", None)
                updated.pop("exposure_quantity", None)
                updated["retry_count"] = 0
                updated["last_attempt_at_ms"] = now_ms
                updated["next_attempt_ms"] = now_ms
                self.state.pending_residual_repairs.append(updated)
            else:
                self._release_residual_repair_pair_gate(pair_id, symbol)
                repaired += 1
            self.journal.append(
                "execution.residual_repair_completed",
                {
                    "position_id": position_id,
                    "pair_id": pair_id,
                    "symbol": symbol,
                    "origin": task.get("origin", ""),
                    "repair_venue": repair_venue.value,
                    "repair_side": repair_side.value,
                    "requested_quantity": repair_quantity,
                    "filled_quantity": float(fill.quantity or 0.0),
                    "remaining_quantity": remaining_quantity,
                    "open_order_count": open_order_count,
                    "open_order_counts_by_venue": open_order_counts_by_venue,
                    "live_truth_venues": [venue.value for venue in probe_venues],
                    "live_positions": self._live_positions_evidence(live_positions),
                    "live_excess_quantity": live_excess_quantity,
                    "baseline_quantity": baseline,
                    "live_size": live_size,
                    "fill_order_id": getattr(fill, "order_id", ""),
                    "fill_price": float(getattr(fill, "price", 0.0) or 0.0),
                },
            )

        if repaired > 0:
            core_decision = V1RecoveryDecisionCore().decide(
                RecoveryEvidenceSnapshot(
                    local_open_positions=tuple(
                        self._recovery_state_collection("open_positions")
                    ),
                    pending_entries=tuple(
                        self._recovery_state_collection("pending_entries")
                    ),
                    residual_repairs=tuple(
                        self._recovery_state_collection("pending_residual_repairs")
                    ),
                    passive_closes=tuple(
                        self._recovery_state_collection("pending_passive_closes")
                    ),
                    exchange_truth=None,
                    prior_recovery_block_reason=self.state.recovery_blocked_reason,
                    operator_fail_closed=(
                        self.state.operator.requested_mode
                        == GlobalRiskMode.FAIL_CLOSED
                    ),
                )
            )
            self.ctx.recovery_decision = core_decision
            if (
                core_decision.clear_previous_block
                and self.state.recovery_blocked_reason
                in CORE_CLEARABLE_BLOCK_REASONS
            ):
                clear_risk_mode_for_recovery(self.state, core_decision)
                self.journal.append(
                    "recovery.residual_repairs_core_clear",
                    {
                        "reason": core_decision.clear_reason,
                        "decision": core_decision.kind.value,
                        "ts_ms": now_ms,
                    },
                )
            self.journal.append(
                "recovery.residual_repairs_complete",
                {"repaired": repaired, "ts_ms": now_ms},
            )

    @staticmethod
    def _residual_repair_client_order_id(
        position_id: str,
        duplicate_attempt: int,
    ) -> str:
        from lightfee.venues.cid import compact_client_order_id

        suffix = (
            "residual_repair"
            if duplicate_attempt <= 0
            else f"residual_repair_duplicate_{duplicate_attempt}"
        )
        return compact_client_order_id(position_id, suffix)

    def _pending_residual_repair_fields(self, task: dict) -> tuple[Venue, Side, float] | None:
        venue_raw = task.get("repair_venue") or task.get("exposure_venue")
        side_raw = task.get("repair_side") or task.get("exposure_side")
        quantity_raw = task.get("repair_quantity", task.get("exposure_quantity", 0.0))
        if venue_raw is None or side_raw is None:
            return None
        try:
            repair_venue = Venue.from_str(str(venue_raw))
            repair_side = Side(str(side_raw).strip().lower())
            repair_quantity = float(quantity_raw or 0.0)
        except Exception:
            return None
        if repair_quantity <= 1e-9:
            return None
        return repair_venue, repair_side, repair_quantity

    def _signed_position_size(self, position: PositionSnapshot | None) -> float:
        if position is None:
            return 0.0
        quantity = abs(float(position.quantity or 0.0))
        return quantity if position.side == Side.BUY else -quantity

    @staticmethod
    def _position_snapshot_evidence(position: PositionSnapshot | None) -> dict[str, Any]:
        if position is None:
            return {
                "available": False,
                "quantity": 0.0,
            }
        return {
            "available": True,
            "venue": position.venue.value,
            "symbol": position.symbol,
            "side": position.side.value,
            "quantity": float(position.quantity or 0.0),
            "entry_price": float(position.entry_price or 0.0),
            "observed_at_ms": int(position.observed_at_ms or 0),
        }

    def _live_positions_evidence(
        self,
        live_positions: dict[Venue, PositionSnapshot | None],
    ) -> dict[str, dict[str, Any]]:
        return {
            venue.value: self._position_snapshot_evidence(position)
            for venue, position in live_positions.items()
        }

    def _venue_symbol_metadata_evidence(self, venue: Venue, symbol: str) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "available": False,
            "venue": venue.value,
            "symbol": symbol,
            "venue_symbol": symbol,
            "metadata_source": "unavailable",
        }
        adapter = self.get_venue_adapter(venue)
        if adapter is None:
            return payload

        transport = getattr(adapter, "_transport", adapter)
        venue_symbol = symbol
        to_venue_symbol = getattr(transport, "_venue_symbol", None)
        if not callable(to_venue_symbol):
            to_venue_symbol = getattr(adapter, "_venue_symbol", None)
        if callable(to_venue_symbol):
            try:
                venue_symbol = str(to_venue_symbol(symbol) or symbol)
            except Exception:
                venue_symbol = symbol
        payload["venue_symbol"] = venue_symbol

        metadata_map = getattr(transport, "_symbol_metadata", {}) or {}
        metadata = {}
        if isinstance(metadata_map, dict):
            for key in (symbol, venue_symbol):
                candidate = metadata_map.get(key)
                if isinstance(candidate, dict):
                    metadata = candidate
                    break
        if metadata:
            payload["available"] = True
            payload["metadata_source"] = "transport_symbol_metadata"
            payload["raw_metadata_keys"] = sorted(str(key) for key in metadata.keys())[:40]
            for output_key, source_keys in {
                "instrument_id": ("instId", "instrument_id", "id"),
                "ct_type": ("ct_type", "ctType"),
                "contract_type": ("contractType", "contract_type"),
                "status": ("status", "contractStatus", "state"),
            }.items():
                for source_key in source_keys:
                    value = metadata.get(source_key)
                    if value not in (None, ""):
                        payload[output_key] = str(value)
                        break
            for output_key, source_keys in {
                "ct_val": ("ct_val", "ctVal", "contract_size", "contractSize"),
                "contract_size": ("contract_size", "contractSize"),
                "lot_size": ("lotSz", "lot_size", "qtyStep", "stepSize"),
                "min_size": ("minSz", "min_size", "minOrderQty"),
                "quantity_step": ("qtyStep", "stepSize", "lotSz"),
                "min_notional": ("minNotionalValue", "min_notional", "minNotional"),
            }.items():
                for source_key in source_keys:
                    parsed = self._safe_positive_float(metadata.get(source_key))
                    if parsed > 0:
                        payload[output_key] = parsed
                        break
            filters = metadata.get("filters")
            if isinstance(filters, list):
                for item in filters:
                    if not isinstance(item, dict):
                        continue
                    filter_type = str(item.get("filterType", ""))
                    if filter_type in {"LOT_SIZE", "MARKET_LOT_SIZE"} and "quantity_step" not in payload:
                        step = self._safe_positive_float(item.get("stepSize"))
                        if step > 0:
                            payload["quantity_step"] = step
                    if filter_type in {"MIN_NOTIONAL", "NOTIONAL"} and "min_notional" not in payload:
                        notional = self._safe_positive_float(
                            item.get("notional") or item.get("minNotional")
                        )
                        if notional > 0:
                            payload["min_notional"] = notional

        passive_metadata = getattr(adapter, "passive_metadata", None)
        if callable(passive_metadata):
            try:
                passive = passive_metadata(symbol) or {}
            except Exception:
                passive = {}
            if isinstance(passive, dict) and passive:
                if not payload["available"]:
                    payload["available"] = True
                    payload["metadata_source"] = "adapter_passive_metadata"
                for output_key, source_keys in {
                    "min_notional": ("min_notional", "min_notional_quote"),
                    "quantity_step": ("quantity_step", "qty_step"),
                    "price_tick": ("price_tick", "tick_size"),
                    "max_quantity": ("max_quantity", "max_qty"),
                }.items():
                    if output_key in payload:
                        continue
                    for source_key in source_keys:
                        parsed = self._safe_positive_float(passive.get(source_key))
                        if parsed > 0:
                            payload[output_key] = parsed
                            break

        spec = getattr(transport, "_spec", None)
        if spec is not None:
            for output_key, attr in {
                "spec_contract_size": "contract_size",
                "spec_quantity_step": "quantity_step",
                "spec_min_notional": "min_notional",
            }.items():
                parsed = self._safe_positive_float(getattr(spec, attr, 0.0))
                if parsed > 0:
                    payload[output_key] = parsed
            if not payload["available"] and any(
                key in payload
                for key in ("spec_contract_size", "spec_quantity_step", "spec_min_notional")
            ):
                payload["available"] = True
                payload["metadata_source"] = "venue_spec"
        return payload

    def _order_submit_error_runtime_evidence(
        self,
        error: OrderSubmitError,
        *,
        venue: Venue | None = None,
        operation: str = "",
        request: Any = None,
        default_client_order_id: str = "",
    ) -> dict[str, Any]:
        try:
            return build_order_submit_uncertainty_payload(
                error,
                venue=venue,
                operation=operation,
                request=request,
                default_client_order_id=default_client_order_id,
            )
        except Exception:
            return {}

    @staticmethod
    def _order_truth_probe_paths(venue: Venue | None) -> dict[str, str]:
        return order_truth_probe_paths(venue)

    async def _resolve_residual_repair_accepted_order(
        self,
        *,
        task: dict,
        adapter: VenueAdapter,
        repair_venue: Venue,
        repair_side: Side,
        symbol: str,
        baseline: float,
        probe_venues: list[Venue],
        accepted_order_id: str,
        accepted_client_order_id: str,
        now_ms: int,
    ) -> tuple[str, OrderFill | None, dict[str, Any]]:
        payload: dict[str, Any] = {
            "accepted_order_id": accepted_order_id,
            "accepted_client_order_id": accepted_client_order_id,
            "accepted_order_truth_gap": True,
            "truth_required_by": "accepted_order_truth_gap",
            "terminal_without_truth": False,
            "next_action": "reconcile_accepted_order_or_probe_live_position",
            "order_truth_probe_paths": self._order_truth_probe_paths(repair_venue),
        }

        fetch_reconciliation = getattr(adapter, "fetch_order_fill_reconciliation", None)
        if callable(fetch_reconciliation) and (accepted_order_id or accepted_client_order_id):
            try:
                reconciliation = await fetch_reconciliation(
                    symbol,
                    accepted_order_id,
                    accepted_client_order_id or None,
                )
                self._flush_adapter_order_diagnostics(adapter)
            except Exception as e:
                payload["fill_reconciliation_result"] = "error"
                payload["fill_reconciliation_error"] = str(e) or e.__class__.__name__
                return "truth_unavailable", None, payload

            recon_qty = self._close_reconciliation_fill_qty(reconciliation)
            if recon_qty > 1e-12:
                payload["fill_reconciliation_result"] = "filled"
                fill = OrderFill(
                    venue=repair_venue,
                    symbol=symbol,
                    side=getattr(reconciliation, "side", repair_side) or repair_side,
                    quantity=recon_qty,
                    price=_recon_fill_price(reconciliation),
                    order_id=(
                        str(getattr(reconciliation, "order_id", "") or "")
                        or accepted_order_id
                    ),
                    client_order_id=(
                        str(getattr(reconciliation, "client_order_id", "") or "")
                        or accepted_client_order_id
                        or None
                    ),
                    fee_quote=getattr(reconciliation, "fee_quote", None),
                    filled_at_ms=int(
                        getattr(reconciliation, "filled_at_ms", 0) or now_ms
                    ),
                )
                return "filled", fill, payload
            payload["fill_reconciliation_result"] = "missing_or_zero_fill"
        else:
            payload["fill_reconciliation_result"] = "not_available"

        live_positions: dict[Venue, PositionSnapshot | None] = {}
        open_order_count = 0
        open_order_counts_by_venue: dict[str, int] = {}
        for probe_venue in probe_venues:
            probe_adapter = self.get_venue_adapter(probe_venue)
            if probe_adapter is None:
                continue
            try:
                live_positions[probe_venue] = await probe_adapter.fetch_position(symbol)
                open_orders = await self._fetch_residual_repair_open_orders(
                    probe_adapter,
                    probe_venue,
                    symbol,
                )
            except Exception as e:
                payload["live_truth_error"] = str(e) or e.__class__.__name__
                return "truth_unavailable", None, payload
            venue_open_order_count = len(open_orders)
            open_order_count += venue_open_order_count
            open_order_counts_by_venue[probe_venue.value] = venue_open_order_count

        live_position = live_positions.get(repair_venue)
        live_size = self._signed_position_size(live_position)
        if repair_side == Side.SELL:
            live_excess_quantity = max(live_size - baseline, 0.0)
        else:
            live_excess_quantity = max(baseline - live_size, 0.0)
        payload.update(
            {
                "open_order_count": open_order_count,
                "open_order_counts_by_venue": open_order_counts_by_venue,
                "live_truth_venues": [venue.value for venue in probe_venues],
                "live_positions": self._live_positions_evidence(live_positions),
                "live_excess_quantity": live_excess_quantity,
                "baseline_quantity": baseline,
                "live_size": live_size,
            }
        )
        if open_order_count > 0:
            return "open_order_present", None, payload
        if live_excess_quantity <= 1e-9:
            return "live_flat", None, payload
        return "truth_gap", None, payload

    def _retain_residual_repair_accepted_order_gap(
        self,
        task: dict,
        now_ms: int,
        *,
        status: str,
        accepted_order_id: str,
        accepted_client_order_id: str,
    ) -> None:
        retry_count = self._residual_repair_attempt_count(task) + 1
        task["retry_count"] = retry_count
        task["attempt_count"] = retry_count
        task["last_attempt_at_ms"] = now_ms
        task["next_attempt_ms"] = now_ms + self._residual_repair_retry_delay_ms(
            retry_count
        )
        task["accepted_order_truth_gap"] = True
        if accepted_order_id:
            task["accepted_order_id"] = accepted_order_id
        if accepted_client_order_id:
            task["accepted_client_order_id"] = accepted_client_order_id
        task["last_error"] = f"accepted_order_truth_gap_{status}"

    def _clear_residual_repair_accepted_order_gap(self, task: dict) -> None:
        for key in (
            "accepted_order_truth_gap",
            "accepted_order_id",
            "accepted_client_order_id",
        ):
            task.pop(key, None)

    async def _fetch_residual_repair_open_orders(
        self, adapter: VenueAdapter, venue: Venue, symbol: str,
    ) -> list[Any]:
        fetch_open_orders = getattr(adapter, "fetch_open_orders", None)
        if callable(fetch_open_orders):
            open_orders = await fetch_open_orders(symbol)
            if isinstance(open_orders, dict) and open_orders.get("error"):
                raise RuntimeError(str(open_orders.get("error")))
            return self._residual_repair_open_order_items(open_orders)

        transport = getattr(adapter, "_transport", None)
        if transport is None or not hasattr(transport, "_request"):
            raise RuntimeError("open_orders_truth_unavailable")

        venue_symbol = symbol
        to_venue_symbol = getattr(transport, "_venue_symbol", None)
        if callable(to_venue_symbol):
            venue_symbol = to_venue_symbol(symbol)

        if venue in (Venue.BINANCE, Venue.ASTER):
            raw = await transport._request(
                "GET", "/fapi/v1/openOrders",
                params={"symbol": venue_symbol},
                private=True,
            )
        elif venue == Venue.BYBIT:
            raw = await transport._request(
                "GET", "/v5/order/realtime",
                params={
                    "category": "linear",
                    "symbol": venue_symbol,
                    "settleCoin": "USDT",
                },
                private=True,
            )
        elif venue == Venue.OKX:
            raw = await transport._request(
                "GET", "/api/v5/trade/orders-pending",
                params={"instId": venue_symbol},
                private=True,
            )
        else:
            raise RuntimeError(f"open_orders_truth_unsupported:{venue.value}")

        return self._residual_repair_open_order_items(raw)

    @staticmethod
    def _residual_repair_open_order_items(raw: Any) -> list[Any]:
        if raw is None:
            return []
        if isinstance(raw, list):
            return raw
        if not isinstance(raw, dict):
            return [raw]
        if raw.get("error"):
            raise RuntimeError(str(raw.get("error")))
        result = raw.get("result")
        if isinstance(result, dict) and isinstance(result.get("list"), list):
            return result["list"]
        if isinstance(raw.get("data"), list):
            return raw["data"]
        if isinstance(raw.get("list"), list):
            return raw["list"]
        return []

    def _residual_repair_baseline_size(self, task: dict, repair_venue: Venue) -> float:
        position_id = task.get("position_id", "")
        position = self.state.open_positions.get(position_id)
        if position is None:
            return 0.0
        matched_quantity = float(
            position.matched_quantity
            or min(position.long_quantity, position.short_quantity)
            or 0.0
        )
        if repair_venue == position.long_venue:
            return matched_quantity
        if repair_venue == position.short_venue:
            return -matched_quantity
        return 0.0

    @staticmethod
    def _residual_repair_retry_delay_ms(attempt_count: int) -> int:
        attempt = max(int(attempt_count or 0), 1)
        return min(1_000 * (2 ** (attempt - 1)), 30_000)

    @staticmethod
    def _residual_repair_attempt_count(task: dict) -> int:
        return int(task.get("retry_count", task.get("attempt_count", 0)) or 0)

    def _residual_repair_deadline_or_attempts_exhausted(
        self, task: dict, now_ms: int,
    ) -> bool:
        deadline_ms = int(task.get("deadline_ms", 0) or 0)
        attempts = self._residual_repair_attempt_count(task)
        return (deadline_ms > 0 and now_ms >= deadline_ms) or attempts >= 3

    def _pause_pending_residual_repair(
        self, task: dict, now_ms: int, evidence: dict[str, Any] | None = None
    ) -> None:
        task["local_entry_paused"] = True
        task["last_attempt_at_ms"] = now_ms
        retry_count = self._residual_repair_attempt_count(task)
        task["next_attempt_ms"] = now_ms + self._residual_repair_retry_delay_ms(retry_count)
        current_error = str(task.get("last_error", "") or "")
        if current_error:
            task["last_error"] = current_error
        else:
            task["last_error"] = "residual_repair_deadline_or_attempts_exhausted"
        payload = {
            "position_id": task.get("position_id", ""),
            "pair_id": task.get("pair_id", ""),
            "symbol": task.get("symbol", ""),
            "repair_venue": task.get("repair_venue", task.get("exposure_venue", "")),
            "repair_side": task.get("repair_side", task.get("exposure_side", "")),
            "retry_count": self._residual_repair_attempt_count(task),
            "deadline_ms": int(task.get("deadline_ms", 0) or 0),
            "ts_ms": now_ms,
            "last_error": task["last_error"],
        }
        if evidence:
            payload.update(evidence)
        self.journal.append(
            "execution.residual_repair_paused",
            payload,
        )

    def _release_residual_repair_pair_gate(self, pair_id: str, symbol: str) -> None:
        if not getattr(self.state, "live_recovery_reduce_only_pairs", None):
            return
        kept = []
        for item in self.state.live_recovery_reduce_only_pairs:
            item_pair_id = ""
            item_symbol = ""
            if isinstance(item, dict):
                item_pair_id = str(item.get("pair_id", ""))
                item_symbol = str(item.get("symbol", ""))
            else:
                item_pair_id = str(getattr(item, "pair_id", ""))
                item_symbol = str(getattr(item, "symbol", ""))
            if pair_id and item_pair_id == pair_id:
                continue
            if not pair_id and symbol and item_symbol == symbol:
                continue
            kept.append(item)
        self.state.live_recovery_reduce_only_pairs = kept

    def _terminalize_residual_repair_task(
        self,
        task: dict,
        now_ms: int,
        *,
        terminal_reason: str,
        repair_venue: Venue,
        repair_side: Side,
        repair_quantity: float,
        live_price: float,
        min_notional: float,
    ) -> None:
        pair_id = str(task.get("pair_id", ""))
        symbol = str(task.get("symbol", ""))
        position_id = str(task.get("position_id", "") or "")
        position = self.state.open_positions.get(position_id)
        matched_quantity = float(
            getattr(position, "matched_quantity", 0.0) or 0.0
        ) if position is not None else 0.0
        residual_ratio = (
            abs(float(repair_quantity or 0.0)) / matched_quantity
            if matched_quantity > 1e-9
            else 0.0
        )
        task["terminal_reason"] = terminal_reason
        task["residual_ratio"] = residual_ratio
        residual_owner_id = str(
            task.get("repair_id")
            or task.get("task_id")
            or task.get("position_id")
            or symbol
            or "residual"
        )
        closure_fields = self._v1_lifecycle_event_fields(
            phase="RESIDUAL_REPAIR",
            owner_id=residual_owner_id,
            now_ms=now_ms,
        )
        closure_phase = closure_fields.get("closure_phase", "RESIDUAL_REPAIR")
        closure_row_key = closure_fields.get("closure_row_key", "")
        closure_decision_id = closure_fields.get("closure_decision_id", "")
        try:
            self.state.pending_residual_repairs.remove(task)
        except ValueError:
            pass
        self._release_residual_repair_pair_gate(pair_id, symbol)
        self.journal.append(
            "execution.residual_repair_terminal",
            {
                "position_id": position_id,
                "pair_id": pair_id,
                "symbol": symbol,
                "origin": task.get("origin", ""),
                "repair_venue": repair_venue.value,
                "repair_side": repair_side.value,
                "repair_quantity": repair_quantity,
                "live_price": live_price,
                "notional": repair_quantity * live_price,
                "min_notional": min_notional,
                "terminal_reason": terminal_reason,
                "repair_venue_metadata": self._venue_symbol_metadata_evidence(
                    repair_venue,
                    symbol,
                ),
                "ts_ms": now_ms,
                "closure_phase": closure_phase,
                "closure_row_key": closure_row_key,
                "closure_decision_id": closure_decision_id,
            },
        )
        if (
            str(task.get("origin", "") or "") == "entry_open"
            and terminal_reason in {
                "exchange_min_quantity_dust",
                "exchange_min_notional_dust",
            }
            and matched_quantity > 1e-9
            and residual_ratio <= 0.02 + 1e-12
        ):
            self.journal.append(
                "execution.entry_residual_dust_tolerated",
                {
                    "position_id": position_id,
                    "pair_id": pair_id,
                    "symbol": symbol,
                    "repair_venue": repair_venue.value,
                    "repair_side": repair_side.value,
                    "repair_quantity": repair_quantity,
                    "matched_quantity": matched_quantity,
                    "residual_ratio": residual_ratio,
                    "terminal_reason": terminal_reason,
                    "reason": "unrepairable_entry_residual_dust_within_tolerance",
                    "ts_ms": now_ms,
                    "closure_phase": closure_phase,
                    "closure_row_key": closure_row_key,
                    "closure_decision_id": closure_decision_id,
                },
            )

    def _reschedule_pending_residual_repair_task(
        self, task: dict, now_ms: int, error: str
    ) -> None:
        retry_count = self._residual_repair_attempt_count(task) + 1
        task["retry_count"] = retry_count
        task["attempt_count"] = retry_count
        task["last_attempt_at_ms"] = now_ms
        task["last_error"] = error
        if self._residual_repair_deadline_or_attempts_exhausted(task, now_ms):
            self._pause_pending_residual_repair(task, now_ms)
            return
        task["next_attempt_ms"] = now_ms + self._residual_repair_retry_delay_ms(retry_count)
