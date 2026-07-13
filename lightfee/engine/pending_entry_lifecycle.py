"""V1 pending-entry passive lifecycle source-port helpers.

Runtime code owns exchange IO. This module owns source-named state transitions
ported from V1 `execution_core/entry_sync.rs` for pending-entry passive opening.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lightfee.config.schema import StrategyConfig
from lightfee.core.domain import PassiveOrderState
from lightfee.engine.state import (
    PassiveOrderManagerRuntime,
    PendingEntryPassivePhaseState,
    PendingPassiveOrder,
)


@dataclass(frozen=True)
class PendingEntryLifecycleAction:
    kind: str
    reason: str = ""
    evidence: dict[str, Any] = field(default_factory=dict)


def ensure_pending_entry_phase_state(pending: Any, now_ms: int = 0) -> PendingEntryPassivePhaseState:
    """V1: `PendingEntryHedge::ensure_phase_state_mut` for entry cycles."""

    phase_state = getattr(pending, "phase_state", None)
    if isinstance(phase_state, PendingEntryPassivePhaseState):
        return phase_state
    if isinstance(phase_state, dict):
        restored = PendingEntryPassivePhaseState.from_dict(phase_state)
        if restored is not None:
            pending.phase_state = restored
            return restored

    maker_leg = str(getattr(pending, "maker_leg", "") or "long")
    passive_order = getattr(pending, "passive_order", None)
    started_at_ms = int(
        getattr(passive_order, "accepted_at_ms", 0)
        or getattr(pending, "created_at_ms", 0)
        or now_ms
        or 0
    )
    pending.phase_state = PendingEntryPassivePhaseState(
        execution_kind="entry",
        preferred_maker_leg=maker_leg,
        active_maker_leg=maker_leg,
        phase="high_slippage_maker",
        cycle_attempt=1,
        phase_started_at_ms=started_at_ms,
        cycle_started_at_ms=started_at_ms,
    )
    return pending.phase_state


def note_passive_operation(pending: Any) -> None:
    """V1: `PendingEntryHedge::note_passive_operation`."""

    pending.passive_ops_total = int(getattr(pending, "passive_ops_total", 0) or 0) + 1


def pending_entry_phase_zero_fill_budget(strategy: StrategyConfig) -> int:
    """V1: `pending_entry_phase_zero_fill_budget`."""

    budget = int(strategy.pending_entry_phase_zero_fill_budget or 0)
    if budget <= 0:
        budget = int(strategy.maker_phase_max_zero_fill_cycles or 0)
    return max(1, budget)


def _maker_cycle_retry_delay_ms(strategy: StrategyConfig, completed_cycles: int) -> int:
    delays = list(strategy.maker_cycle_retry_delays_ms or [])
    if not delays:
        return 0
    index = min(max(completed_cycles - 1, 0), len(delays) - 1)
    return max(0, int(delays[index] or 0))


def record_pending_entry_zero_fill_cycle(
    pending: Any, strategy: StrategyConfig, now_ms: int
) -> int:
    """V1: first half of `handle_pending_entry_zero_fill_completion`."""

    phase_state = ensure_pending_entry_phase_state(pending, now_ms)
    completed_cycles = int(phase_state.zero_fill_cycles_in_phase or 0) + 1
    delay_ms = _maker_cycle_retry_delay_ms(strategy, completed_cycles)

    phase_state.zero_fill_cycles_in_phase = completed_cycles
    phase_state.cycle_attempt = completed_cycles
    phase_state.next_cycle_delay_ms = delay_ms
    phase_state.hedge_deadline_at_ms = None
    pending.repost_attempt_count = completed_cycles
    pending.passive_attempt_count = 0
    pending.next_progress_poll_ms = int(now_ms) + delay_ms
    return delay_ms


def apply_pending_entry_passive_progress(pending: Any, progress: Any) -> bool:
    """V1: `apply_pending_entry_passive_progress` maker delta branch."""

    passive_order = getattr(pending, "passive_order", None)
    if passive_order is None or progress is None:
        return False

    previous_quantity = float(getattr(pending, "maker_leg_filled", 0.0) or 0.0)
    previous_state = getattr(passive_order, "last_progress_state", None)
    progress_state = getattr(progress, "state", None)
    if progress_state is not None:
        passive_order.last_progress_state = progress_state

    checkpoint_quantity = float(
        getattr(passive_order, "fill_checkpoint_quantity", 0.0) or 0.0
    )
    order_cumulative_quantity = max(
        0.0,
        float(getattr(progress, "cumulative_quantity", 0.0) or 0.0),
    )
    target_quantity = float(getattr(passive_order, "target_quantity", 0.0) or 0.0)
    if target_quantity > 0.0:
        order_cumulative_quantity = min(order_cumulative_quantity, target_quantity)
    updated_quantity = checkpoint_quantity + order_cumulative_quantity
    if updated_quantity > previous_quantity + 1e-9:
        delta_quantity = updated_quantity - previous_quantity
        pending.maker_leg_filled = updated_quantity
        average_price = float(getattr(progress, "average_price", 0.0) or 0.0)
        if average_price > 0.0:
            pending.maker_fill_price = average_price
        filled_at_ms = int(
            getattr(progress, "last_fill_time_ms", 0)
            or getattr(progress, "observed_at_ms", 0)
            or 0
        )
        quality = (
            "exchange_fill_exact"
            if int(getattr(progress, "last_fill_time_ms", 0) or 0) > 0
            else "observed"
        )
        if hasattr(pending, "note_maker_fill_observed"):
            pending.note_maker_fill_observed(filled_at_ms, quality=quality)
        pending.push_maker_remainder_slice(
            delta_quantity,
            average_price if average_price > 0.0 else None,
            getattr(progress, "observed_at_ms", 0) or None,
        )

    return (
        abs(previous_quantity - float(getattr(pending, "maker_leg_filled", 0.0) or 0.0))
        > 1e-9
        or previous_state != getattr(passive_order, "last_progress_state", None)
    )


def _candidate_blocked(candidate: Any) -> tuple[bool, list[str]]:
    if candidate is None:
        return True, []
    if isinstance(candidate, dict):
        blocked_reasons = list(candidate.get("blocked_reasons", []) or [])
        blocked = bool(candidate.get("blocked", False)) or bool(blocked_reasons)
        return blocked, blocked_reasons
    is_tradeable = getattr(candidate, "is_tradeable", None)
    if callable(is_tradeable):
        tradeable = bool(is_tradeable())
        blocked_reasons = list(getattr(candidate, "blocked_reasons", []) or [])
        return not tradeable, blocked_reasons
    blocked_reasons = list(getattr(candidate, "blocked_reasons", []) or [])
    blocked = bool(getattr(candidate, "blocked", False)) or bool(blocked_reasons)
    return blocked, blocked_reasons


def candidate_for_terminal_taker_fallback(pending: Any) -> Any:
    """V1: `current_tradeable_candidate_for_terminal_taker_fallback` source candidate."""

    frozen = getattr(pending, "frozen_candidate", None)
    if isinstance(frozen, dict):
        return dict(frozen)
    if frozen is not None:
        return frozen
    return None


def terminal_recheck_is_tradeable(candidate: Any) -> PendingEntryLifecycleAction:
    """V1: terminal recheck tradeability decision after runtime guards."""

    blocked, blocked_reasons = _candidate_blocked(candidate)
    if blocked:
        return PendingEntryLifecycleAction(
            kind="blocked",
            reason="candidate_not_tradeable_after_terminal_reprice",
            evidence={"blocked_reasons": blocked_reasons},
        )
    return PendingEntryLifecycleAction(kind="tradeable")


def _opposite_pending_entry_maker_leg(maker_leg: str) -> str:
    return "short" if str(maker_leg or "long") == "long" else "long"


def advance_pending_entry_zero_fill_phase(
    pending: Any,
    strategy: Any,
    now_ms: int,
    candidate: Any,
) -> PendingEntryLifecycleAction:
    """V1: phase branch of `handle_pending_entry_zero_fill_completion`."""

    blocked, blocked_reasons = _candidate_blocked(candidate)
    if blocked:
        return PendingEntryLifecycleAction(
            kind="finalized",
            reason="candidate_not_tradeable_after_zero_fill_reprice",
            evidence={"blocked_reasons": blocked_reasons},
        )

    phase_state = ensure_pending_entry_phase_state(pending, now_ms)
    if (
        int(phase_state.zero_fill_cycles_in_phase or 0)
        < pending_entry_phase_zero_fill_budget(strategy)
    ):
        return PendingEntryLifecycleAction(kind="submit_next_cycle")

    if phase_state.phase == "high_slippage_maker":
        next_maker_leg = _opposite_pending_entry_maker_leg(
            phase_state.preferred_maker_leg or getattr(pending, "maker_leg", "long")
        )
        phase_state.phase = "low_slippage_maker"
        phase_state.active_maker_leg = next_maker_leg
        phase_state.zero_fill_cycles_in_phase = 0
        phase_state.cycle_attempt = 0
        phase_state.next_cycle_delay_ms = None
        phase_state.hedge_deadline_at_ms = None
        phase_state.phase_started_at_ms = int(now_ms)
        phase_state.small_fill_min_notional_attempts = 0
        pending.maker_leg = next_maker_leg
        pending.repost_attempt_count = 0
        pending.passive_attempt_count = 0
        return PendingEntryLifecycleAction(
            kind="submit_next_cycle",
            reason="phase_switched_to_low_slippage_maker",
        )

    if phase_state.phase == "low_slippage_maker":
        phase_state.phase = "dual_taker"
        phase_state.next_cycle_delay_ms = None
        phase_state.hedge_deadline_at_ms = None
        return PendingEntryLifecycleAction(
            kind="trigger_dual_taker",
            reason="maker_entry_dual_taker_after_phase_exhaustion",
        )

    return PendingEntryLifecycleAction(
        kind="finalized",
        reason="dual_taker_phase_already_armed",
    )


def decide_terminal_taker_fallback(
    candidate: Any,
    terminal_reason: str,
) -> PendingEntryLifecycleAction:
    """V1: `try_terminal_taker_fallback` candidate decision branch."""

    blocked, blocked_reasons = _candidate_blocked(candidate)
    if blocked:
        return PendingEntryLifecycleAction(
            kind="skip_fallback",
            reason="candidate_not_tradeable_after_terminal_reprice",
            evidence={
                "terminal_reason": terminal_reason,
                "blocked_reasons": blocked_reasons,
            },
        )
    return PendingEntryLifecycleAction(
        kind="fallback_to_taker",
        reason="maker_entry_terminal_zero_fill",
        evidence={"terminal_reason": terminal_reason},
    )


def note_pending_entry_passive_submit(pending: Any, accepted_at_ms: int) -> None:
    """V1: `note_pending_entry_passive_submit` side effects."""

    runtime = getattr(pending, "passive_manager_runtime", None)
    if not isinstance(runtime, PassiveOrderManagerRuntime):
        runtime = PassiveOrderManagerRuntime.from_dict(runtime)
        pending.passive_manager_runtime = runtime
    runtime.last_attempt_ms = int(accepted_at_ms)
    runtime.last_operation_ms = int(accepted_at_ms)
    runtime.last_success_ms = int(accepted_at_ms)
    runtime.consecutive_failures = 0
    note_passive_operation(pending)
    passive_order = getattr(pending, "passive_order", None)
    if passive_order is not None:
        passive_order.last_resting_quality_sample_at_ms = int(accepted_at_ms)


def note_pending_entry_passive_cycle_accepted(
    pending: Any,
    *,
    order_id: str,
    client_order_id: str,
    accepted_at_ms: int,
    limit_price: float | None,
    target_quantity: float,
    passive_attempt_count: int,
    rest_timeout_ms: int,
) -> None:
    """V1: accepted branch of `submit_pending_entry_passive_cycle`."""

    accepted_at_ms = int(accepted_at_ms)
    checkpoint_quantity = float(getattr(pending, "maker_leg_filled", 0.0) or 0.0)
    checkpoint_price = float(
        getattr(pending, "maker_fill_price", 0.0)
        or getattr(pending, "maker_price", 0.0)
        or 0.0
    )
    pending.passive_order = PendingPassiveOrder(
        order_id=str(order_id or ""),
        client_order_id=str(client_order_id or ""),
        limit_price=limit_price,
        target_quantity=float(target_quantity or 0.0),
        accepted_at_ms=accepted_at_ms,
        timeout_at_ms=accepted_at_ms + max(0, int(rest_timeout_ms or 0)),
        cancel_requested_at_ms=0,
        last_progress_state=PassiveOrderState.OPEN,
        fill_checkpoint_quantity=checkpoint_quantity,
        fill_checkpoint_notional_quote=max(0.0, checkpoint_quantity * checkpoint_price),
    )
    note_pending_entry_passive_submit(pending, accepted_at_ms)
    pending.next_progress_poll_ms = accepted_at_ms
    pending.passive_attempt_count = int(passive_attempt_count or 0)
    phase_state = ensure_pending_entry_phase_state(pending, accepted_at_ms)
    pending.repost_attempt_count = int(phase_state.zero_fill_cycles_in_phase or 0)
    phase_state.cycle_attempt = int(phase_state.zero_fill_cycles_in_phase or 0) + 1
    phase_state.next_cycle_delay_ms = None
    phase_state.hedge_deadline_at_ms = None
    phase_state.cycle_started_at_ms = accepted_at_ms


def _remaining_target_quantity(pending: Any) -> float:
    target_quantity = float(getattr(pending, "target_quantity", 0.0) or 0.0)
    maker_filled = float(getattr(pending, "maker_leg_filled", 0.0) or 0.0)
    return max(0.0, target_quantity - maker_filled)


def prepare_pending_entry_passive_cycle(
    pending: Any,
    *,
    normalized_quantity: float,
) -> PendingEntryLifecycleAction:
    """V1: pre-submit decisions in `submit_pending_entry_passive_cycle`."""

    remaining_quantity = _remaining_target_quantity(pending)
    if remaining_quantity <= 1e-9:
        return PendingEntryLifecycleAction(
            kind="finalized",
            reason="remaining_quantity_depleted",
            evidence={"remaining_quantity": remaining_quantity},
        )
    normalized_quantity = float(normalized_quantity or 0.0)
    if normalized_quantity <= 1e-9:
        return PendingEntryLifecycleAction(
            kind="finalized",
            reason="remaining_quantity_below_minimum",
            evidence={
                "remaining_quantity": remaining_quantity,
                "normalized_quantity": normalized_quantity,
            },
        )
    phase_state = ensure_pending_entry_phase_state(pending)
    return PendingEntryLifecycleAction(
        kind="submit_passive_cycle",
        evidence={
            "remaining_quantity": remaining_quantity,
            "normalized_quantity": normalized_quantity,
            "cycle_attempt": int(phase_state.zero_fill_cycles_in_phase or 0) + 1,
        },
    )


def prepare_pending_entry_remainder_repost(
    pending: Any,
    strategy: StrategyConfig,
    *,
    normalized_quantity: float,
    passive_attempt_limit: int = 3,
) -> PendingEntryLifecycleAction:
    """V1: pre-submit decisions in `try_repost_pending_entry_remainder`."""

    remaining_quantity = _remaining_target_quantity(pending)
    if int(getattr(pending, "passive_attempt_count", 0) or 0) >= passive_attempt_limit:
        return PendingEntryLifecycleAction(
            kind="finalized",
            reason="max_passive_attempts_reached",
            evidence={
                "remaining_quantity": remaining_quantity,
                "normalized_quantity": None,
            },
        )
    max_reposts = int(strategy.maker_entry_max_reposts or 0)
    if int(getattr(pending, "repost_attempt_count", 0) or 0) >= max_reposts:
        return PendingEntryLifecycleAction(
            kind="finalized",
            reason="max_reposts_reached",
            evidence={
                "remaining_quantity": remaining_quantity,
                "normalized_quantity": None,
            },
        )
    if remaining_quantity <= 1e-9:
        return PendingEntryLifecycleAction(
            kind="finalized",
            reason="remaining_quantity_depleted",
            evidence={
                "remaining_quantity": remaining_quantity,
                "normalized_quantity": None,
            },
        )
    normalized_quantity = float(normalized_quantity or 0.0)
    if normalized_quantity <= 1e-9:
        return PendingEntryLifecycleAction(
            kind="finalized",
            reason="remaining_quantity_below_minimum",
            evidence={
                "remaining_quantity": remaining_quantity,
                "normalized_quantity": normalized_quantity,
            },
        )
    return PendingEntryLifecycleAction(
        kind="repost_remainder",
        evidence={
            "remaining_quantity": remaining_quantity,
            "normalized_quantity": normalized_quantity,
        },
    )


def note_pending_entry_remainder_repost_accepted(
    pending: Any,
    *,
    order_id: str,
    client_order_id: str,
    accepted_at_ms: int,
    limit_price: float | None,
    target_quantity: float,
    passive_attempt_count: int,
    rest_timeout_ms: int,
) -> None:
    """V1: accepted branch of `try_repost_pending_entry_remainder`."""

    repost_attempt_count = int(getattr(pending, "repost_attempt_count", 0) or 0) + 1
    note_pending_entry_passive_cycle_accepted(
        pending,
        order_id=order_id,
        client_order_id=client_order_id,
        accepted_at_ms=accepted_at_ms,
        limit_price=limit_price,
        target_quantity=target_quantity,
        passive_attempt_count=passive_attempt_count,
        rest_timeout_ms=rest_timeout_ms,
    )
    pending.repost_attempt_count = repost_attempt_count
