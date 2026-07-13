"""V1 pending-entry hedge delta source-port decisions.

Runtime owns adapter IO. This module owns the state-only decisions from V1
`hedge_pending_entry_delta` and the entry hedge adaptive deadline helper.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from lightfee.config.schema import StrategyConfig
from lightfee.engine.pending_entry_lifecycle import ensure_pending_entry_phase_state


EPSILON = 1e-9


class HedgeDeadlineStatus(Enum):
    HEALTHY = "healthy"
    SOFT_BREACHED = "soft_breached"
    HARD_BREACHED = "hard_breached"


@dataclass(frozen=True)
class PendingEntryHedgeabilityPlan:
    min_hedgeable_chunk: float
    aligned_target_quantity: float = 0.0
    blocked_reason: str = ""
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PendingEntryHedgeDeltaDecision:
    kind: str
    reason: str = ""
    releasable_quantity: float = 0.0
    normalized_quantity: float = 0.0
    next_progress_poll_ms: int | None = None
    event: str = ""
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AdaptiveEntryHedgeDeadlineDecision:
    effective_soft_deadline_ms: int
    effective_hard_deadline_ms: int
    status: HedgeDeadlineStatus
    extension_ms: int = 0


def _finite_positive(value: Any) -> float:
    try:
        number = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(number) or number <= EPSILON:
        return 0.0
    return number


def releasable_hedge_quantity(unmatched_quantity: float, min_hedgeable_chunk: float) -> float:
    """V1 `releasable_hedge_quantity` helper."""

    unmatched = _finite_positive(unmatched_quantity)
    if unmatched <= EPSILON:
        return 0.0
    try:
        chunk = float(min_hedgeable_chunk or 0.0)
    except (TypeError, ValueError):
        chunk = 0.0
    if not math.isfinite(chunk) or chunk <= EPSILON:
        return unmatched
    whole_chunks = math.floor(unmatched / chunk)
    if whole_chunks <= 0.0:
        return 0.0
    return whole_chunks * chunk


def _passive_order_terminal_or_canceling(pending: Any) -> bool:
    maker_completed = getattr(pending, "maker_completed", None)
    if callable(maker_completed) and maker_completed():
        return True
    passive_order = getattr(pending, "passive_order", None)
    if passive_order is None:
        return False
    cancel_requested = getattr(passive_order, "cancel_requested", None)
    if callable(cancel_requested) and cancel_requested():
        return True
    try:
        return int(getattr(passive_order, "cancel_requested_at_ms", 0) or 0) > 0
    except (TypeError, ValueError):
        return False


def _can_accumulate_small_fill(pending: Any) -> bool:
    helper = getattr(pending, "can_accumulate_small_fill", None)
    if callable(helper):
        return bool(helper())
    return not bool(getattr(pending, "repair_state", ""))


def _missing_hedge_quantity(pending: Any) -> float:
    helper = getattr(pending, "missing_hedge_quantity", None)
    if callable(helper):
        return _finite_positive(helper())
    maker = _finite_positive(getattr(pending, "maker_leg_filled", 0.0))
    hedge = _finite_positive(getattr(pending, "hedge_leg_filled", 0.0))
    return max(0.0, maker - hedge)


def _maker_remainder_notional_quote(pending: Any, quantity: float) -> float:
    total = 0.0
    for item in list(getattr(pending, "maker_remainder_slices", []) or []):
        item_quantity = _finite_positive(getattr(item, "quantity", 0.0))
        if item_quantity <= EPSILON:
            continue
        total += max(0.0, float(getattr(item, "notional_quote", 0.0) or 0.0))
    if total > EPSILON:
        return total
    price = 0.0
    weighted_average = getattr(pending, "unmatched_maker_weighted_average_price", None)
    if callable(weighted_average):
        price = _finite_positive(weighted_average())
    if price <= EPSILON:
        price = _finite_positive(getattr(pending, "maker_fill_price", 0.0))
    if price <= EPSILON:
        price = _finite_positive(getattr(pending, "maker_price", 0.0))
    return max(0.0, quantity) * max(0.0, price)


def _oldest_unmatched_maker_fill_at_ms(pending: Any, now_ms: int) -> int:
    oldest: int | None = None
    for item in list(getattr(pending, "maker_remainder_slices", []) or []):
        if _finite_positive(getattr(item, "quantity", 0.0)) <= EPSILON:
            continue
        try:
            fill_at_ms = int(getattr(item, "fill_at_ms", 0) or 0)
        except (TypeError, ValueError):
            fill_at_ms = 0
        if fill_at_ms <= 0:
            continue
        oldest = fill_at_ms if oldest is None else min(oldest, fill_at_ms)
    return oldest if oldest is not None else now_ms


def _min_notional_values(min_notional_violation: Any) -> tuple[float, float]:
    if isinstance(min_notional_violation, dict):
        return (
            float(
                min_notional_violation.get("leg_notional_quote")
                or min_notional_violation.get("leg_notional")
                or 0.0
            ),
            float(
                min_notional_violation.get("venue_min_notional_quote")
                or min_notional_violation.get("min_notional")
                or 0.0
            ),
        )
    if isinstance(min_notional_violation, tuple | list) and len(min_notional_violation) >= 2:
        return (float(min_notional_violation[0] or 0.0), float(min_notional_violation[1] or 0.0))
    return (0.0, 0.0)


def _base_evidence(
    pending: Any,
    delta: float,
    releasable_delta: float,
    normalized_quantity: float,
    terminal_or_canceling: bool,
) -> dict[str, Any]:
    return {
        "entry_id": getattr(pending, "pending_id", ""),
        "symbol": getattr(pending, "symbol", ""),
        "missing_hedge_quantity": delta,
        "releasable_quantity": releasable_delta,
        "normalized_quantity": normalized_quantity,
        "terminal_or_canceling": terminal_or_canceling,
        "adapter_calls": [],
    }


def decide_pending_entry_hedge_delta_pre_submit(
    pending: Any,
    *,
    strategy: StrategyConfig,
    hedgeability_plan: PendingEntryHedgeabilityPlan,
    normalized_quantity: float | None,
    min_notional_violation: Any,
    now_ms: int,
    maker_progress_updated: bool,
) -> PendingEntryHedgeDeltaDecision:
    """State-only V1 pre-submit decision for pending-entry hedge deltas."""

    now = int(now_ms or 0)
    delta = _missing_hedge_quantity(pending)
    terminal_or_canceling = _passive_order_terminal_or_canceling(pending)
    min_chunk = float(hedgeability_plan.min_hedgeable_chunk or 0.0)
    releasable_delta = releasable_hedge_quantity(delta, min_chunk)
    can_accumulate = _can_accumulate_small_fill(pending)

    if delta <= EPSILON:
        return PendingEntryHedgeDeltaDecision(
            kind="keep_pending",
            reason="no_missing_hedge_quantity",
            evidence=_base_evidence(pending, delta, releasable_delta, 0.0, terminal_or_canceling),
        )

    if releasable_delta <= EPSILON and can_accumulate and not terminal_or_canceling:
        poll_ms = now + max(1, int(strategy.maker_entry_progress_poll_ms or 1))
        pending.next_progress_poll_ms = poll_ms
        evidence = _base_evidence(pending, delta, releasable_delta, 0.0, terminal_or_canceling)
        evidence.update(
            {
                "min_hedgeable_chunk": min_chunk,
                "hedgeability": dict(hedgeability_plan.diagnostics),
            }
        )
        return PendingEntryHedgeDeltaDecision(
            kind="buffer_small_fill",
            reason="below_min_hedgeable_chunk",
            releasable_quantity=releasable_delta,
            next_progress_poll_ms=poll_ms,
            event="execution.pending_entry_hedge_chunk_buffering",
            evidence=evidence,
        )

    normalized = (
        _finite_positive(normalized_quantity)
        if normalized_quantity is not None
        else _finite_positive(releasable_delta)
    )
    notional_violation = min_notional_violation is not None
    if normalized <= EPSILON or notional_violation:
        phase_state = ensure_pending_entry_phase_state(pending, now)
        previous_attempt = int(phase_state.small_fill_min_notional_attempts or 0)
        should_count_small_fill = (
            bool(maker_progress_updated)
            or terminal_or_canceling
            or previous_attempt == 0
        )
        attempt = previous_attempt
        if should_count_small_fill:
            attempt += 1
            phase_state.small_fill_min_notional_attempts = attempt
            phase_state.hedge_deadline_at_ms = None
        leg_notional_quote, venue_min_notional_quote = _min_notional_values(
            min_notional_violation
        )
        evidence = _base_evidence(
            pending,
            delta,
            releasable_delta,
            normalized,
            terminal_or_canceling,
        )
        evidence.update(
            {
                "attempt": attempt,
                "max_attempts": int(
                    strategy.maker_min_notional_accumulation_attempts or 0
                ),
                "leg_notional_quote": leg_notional_quote,
                "venue_min_notional_quote": venue_min_notional_quote,
                "should_count_small_fill": should_count_small_fill,
            }
        )
        max_attempts = max(0, int(strategy.maker_min_notional_accumulation_attempts or 0))
        if can_accumulate and attempt < max_attempts:
            poll_ms = now + max(1, int(strategy.maker_entry_progress_poll_ms or 1))
            pending.next_progress_poll_ms = poll_ms
            return PendingEntryHedgeDeltaDecision(
                kind="wait_min_notional_accumulation",
                reason="hedge_leg_below_min_notional",
                releasable_quantity=releasable_delta,
                normalized_quantity=normalized,
                next_progress_poll_ms=poll_ms,
                event="execution.min_notional_accumulating",
                evidence=evidence,
            )
        return PendingEntryHedgeDeltaDecision(
            kind="abort_and_flatten",
            reason="entry_hedge_leg_below_minimum_notional",
            releasable_quantity=releasable_delta,
            normalized_quantity=normalized,
            event="execution.min_notional_abort_and_flatten",
            evidence=evidence,
        )

    buffered_notional_quote = _maker_remainder_notional_quote(pending, delta)
    buffer_notional_quote = max(
        0.0,
        float(strategy.passive_small_fill_buffer_notional_quote or 0.0),
    )
    buffer_wait_ms = max(
        1,
        int(strategy.passive_small_fill_buffer_max_wait_ms or 1),
    )
    oldest_fill_at_ms = max(0, _oldest_unmatched_maker_fill_at_ms(pending, now))
    buffered_elapsed_ms = max(0, now - oldest_fill_at_ms)
    can_buffer_small_fill = (
        buffer_notional_quote > EPSILON
        and buffered_notional_quote > EPSILON
        and buffered_notional_quote + EPSILON < buffer_notional_quote
        and can_accumulate
        and not terminal_or_canceling
        and getattr(pending, "hedge_inflight", None) is None
    )
    evidence = _base_evidence(
        pending,
        delta,
        releasable_delta,
        normalized,
        terminal_or_canceling,
    )
    evidence.update(
        {
            "buffered_notional_quote": buffered_notional_quote,
            "buffer_threshold_quote": buffer_notional_quote,
            "buffered_elapsed_ms": buffered_elapsed_ms,
            "buffer_wait_ms": buffer_wait_ms,
        }
    )
    if can_buffer_small_fill and buffered_elapsed_ms < buffer_wait_ms:
        remaining_wait_ms = max(1, buffer_wait_ms - buffered_elapsed_ms)
        poll_interval_ms = max(1, int(strategy.maker_entry_progress_poll_ms or 1))
        poll_ms = now + min(poll_interval_ms, remaining_wait_ms)
        pending.next_progress_poll_ms = poll_ms
        return PendingEntryHedgeDeltaDecision(
            kind="wait_passive_small_fill_buffer",
            reason="passive_small_fill_buffering",
            releasable_quantity=releasable_delta,
            normalized_quantity=normalized,
            next_progress_poll_ms=poll_ms,
            event="execution.passive_small_fill_buffering",
            evidence=evidence,
        )
    if can_buffer_small_fill:
        return PendingEntryHedgeDeltaDecision(
            kind="submit_hedge",
            reason="passive_small_fill_buffer_expired",
            releasable_quantity=releasable_delta,
            normalized_quantity=normalized,
            event="execution.passive_small_fill_buffer_expired",
            evidence=evidence,
        )
    return PendingEntryHedgeDeltaDecision(
        kind="submit_hedge",
        reason="",
        releasable_quantity=releasable_delta,
        normalized_quantity=normalized,
        event="",
        evidence=evidence,
    )


def _adaptive_hedge_deadline_extension_ms(
    *,
    hedge_notional_quote: float,
    quote_fresh: bool,
    has_execution_progress: bool,
    reconciled: bool,
    base_hard_deadline_ms: int,
) -> int:
    if not quote_fresh:
        return 0
    if reconciled and not has_execution_progress:
        return 0
    notional_quote = max(0.0, float(hedge_notional_quote or 0.0))
    if notional_quote <= 50.0:
        base_extension_ms = 800
    elif notional_quote <= 150.0:
        base_extension_ms = 400
    elif notional_quote <= 300.0:
        base_extension_ms = 200
    else:
        base_extension_ms = 0
    if base_extension_ms == 0:
        return 0
    progress_bonus_ms = 250 if has_execution_progress else 0
    reconciled_bonus_ms = 100 if reconciled and has_execution_progress else 0
    execution_cap_ms = max(0, int(base_hard_deadline_ms or 0)) // 2
    return min(base_extension_ms + progress_bonus_ms + reconciled_bonus_ms, execution_cap_ms)


def adaptive_entry_hedge_deadline_decision(
    *,
    hedge_elapsed_ms: int,
    base_soft_deadline_ms: int,
    base_hard_deadline_ms: int,
    hedge_notional_quote: float,
    quote_fresh: bool,
    has_execution_progress: bool,
    reconciled: bool,
) -> AdaptiveEntryHedgeDeadlineDecision:
    """V1 `adaptive_hedge_deadline_status` for entry hedges."""

    base_soft = max(0, int(base_soft_deadline_ms or 0))
    base_hard = max(0, int(base_hard_deadline_ms or 0))
    extension_ms = _adaptive_hedge_deadline_extension_ms(
        hedge_notional_quote=hedge_notional_quote,
        quote_fresh=quote_fresh,
        has_execution_progress=has_execution_progress,
        reconciled=reconciled,
        base_hard_deadline_ms=base_hard,
    )
    effective_hard = max(base_hard + extension_ms, max(base_soft, 1))
    effective_soft = min(base_soft + (extension_ms // 2), effective_hard)
    elapsed = max(0, int(hedge_elapsed_ms or 0))
    if elapsed > effective_hard:
        status = HedgeDeadlineStatus.HARD_BREACHED
    elif elapsed > effective_soft:
        status = HedgeDeadlineStatus.SOFT_BREACHED
    else:
        status = HedgeDeadlineStatus.HEALTHY
    return AdaptiveEntryHedgeDeadlineDecision(
        effective_soft_deadline_ms=effective_soft,
        effective_hard_deadline_ms=effective_hard,
        status=status,
        extension_ms=extension_ms,
    )


def note_pending_entry_hedge_submitted(
    pending: Any,
    *,
    submitted_at_ms: int,
    base_soft_deadline_ms: int,
    base_hard_deadline_ms: int,
    hedge_notional_quote: float,
    quote_fresh: bool,
) -> AdaptiveEntryHedgeDeadlineDecision:
    decision = adaptive_entry_hedge_deadline_decision(
        hedge_elapsed_ms=0,
        base_soft_deadline_ms=base_soft_deadline_ms,
        base_hard_deadline_ms=base_hard_deadline_ms,
        hedge_notional_quote=hedge_notional_quote,
        quote_fresh=quote_fresh,
        has_execution_progress=False,
        reconciled=False,
    )
    phase_state = ensure_pending_entry_phase_state(pending, int(submitted_at_ms or 0))
    phase_state.hedge_deadline_at_ms = int(submitted_at_ms or 0) + (
        decision.effective_hard_deadline_ms
    )
    return decision


def note_pending_entry_hedge_filled(pending: Any) -> None:
    phase_state = ensure_pending_entry_phase_state(pending)
    phase_state.hedge_deadline_at_ms = None
    phase_state.small_fill_min_notional_attempts = 0


def decide_pending_entry_hedge_submit_error(
    *,
    may_have_created_exposure: bool,
    deadline_decision: AdaptiveEntryHedgeDeadlineDecision,
) -> PendingEntryHedgeDeltaDecision:
    if deadline_decision.status == HedgeDeadlineStatus.HARD_BREACHED:
        return PendingEntryHedgeDeltaDecision(
            kind="fail_closed_abort" if may_have_created_exposure else "spread_timeout",
            reason="entry_hedge_deadline_breached",
            event="execution.hedge_deadline_breached",
            evidence={"adapter_calls": []},
        )
    return PendingEntryHedgeDeltaDecision(
        kind="abort_and_flatten",
        reason="entry_hedge_submit_error",
        evidence={"adapter_calls": []},
    )


def decide_pending_entry_hedge_deadline_timeout(
    pending: Any,
    *,
    deadline_decision: AdaptiveEntryHedgeDeadlineDecision,
) -> PendingEntryHedgeDeltaDecision:
    if deadline_decision.status != HedgeDeadlineStatus.HARD_BREACHED:
        return PendingEntryHedgeDeltaDecision(
            kind="keep_pending",
            reason="hedge_deadline_not_breached",
            evidence={"adapter_calls": []},
        )
    if _missing_hedge_quantity(pending) > EPSILON:
        return PendingEntryHedgeDeltaDecision(
            kind="spread_timeout",
            reason="entry_hedge_deadline_breached_with_unmatched_maker",
            event="execution.hedge_deadline_breached",
            evidence={"adapter_calls": []},
        )
    return PendingEntryHedgeDeltaDecision(
        kind="keep_pending",
        reason="entry_hedge_deadline_breached_without_unmatched_maker",
        event="execution.hedge_deadline_breached",
        evidence={"adapter_calls": []},
    )
