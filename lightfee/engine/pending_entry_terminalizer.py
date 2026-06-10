"""Pending-entry terminalization authority.

The terminalizer answers one narrow question: whether a pending entry may be
removed, retained, opened as matched exposure, or routed to residual cleanup.
Runtime code still performs the actual I/O and state mutation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, MutableMapping


@dataclass(frozen=True)
class PendingEntryLiveTruth:
    available: bool = True
    has_live_open_order: bool = False
    has_live_position: bool = False
    error: str = ""


@dataclass(frozen=True)
class PendingEntryTerminalDecision:
    outcome: str
    reason: str
    terminal: bool
    allows_pending_removal: bool
    healthy: bool
    operator_block_required: bool = False
    matched_quantity: float = 0.0
    residual_quantity: float = 0.0
    contains_positive_fill_evidence: bool = False


class PendingEntryTerminalizer:
    def decide(
        self,
        pending: Any,
        *,
        live_truth: PendingEntryLiveTruth | None = None,
    ) -> PendingEntryTerminalDecision:
        truth = live_truth or PendingEntryLiveTruth(available=True)
        maker_filled = _quantity(_get(pending, "maker_leg_filled", 0.0))
        hedge_filled = _quantity(_get(pending, "hedge_leg_filled", 0.0))
        matched = max(min(maker_filled, hedge_filled), 0.0)
        residual = abs(maker_filled - hedge_filled)
        has_positive_fill = maker_filled > 1e-9 or hedge_filled > 1e-9

        if not truth.available:
            return PendingEntryTerminalDecision(
                outcome="deferred_missing_live_truth",
                reason=truth.error or "exchange_truth_unavailable",
                terminal=False,
                allows_pending_removal=False,
                healthy=False,
                operator_block_required=True,
                matched_quantity=matched,
                residual_quantity=residual,
                contains_positive_fill_evidence=has_positive_fill,
            )

        if truth.has_live_open_order:
            return PendingEntryTerminalDecision(
                outcome="deferred_live_open_order",
                reason="live_maker_order_requires_owner_or_terminal_evidence",
                terminal=False,
                allows_pending_removal=False,
                healthy=False,
                operator_block_required=False,
                matched_quantity=matched,
                residual_quantity=residual,
                contains_positive_fill_evidence=has_positive_fill,
            )

        if truth.has_live_position and not has_positive_fill:
            return PendingEntryTerminalDecision(
                outcome="deferred_live_position",
                reason="live_position_truth_blocks_zero_fill_terminality",
                terminal=False,
                allows_pending_removal=False,
                healthy=False,
                operator_block_required=False,
                matched_quantity=matched,
                residual_quantity=residual,
                contains_positive_fill_evidence=False,
            )

        if not has_positive_fill:
            return PendingEntryTerminalDecision(
                outcome="passive_unfilled",
                reason="zero_fill_with_no_live_artifact",
                terminal=True,
                allows_pending_removal=True,
                healthy=True,
                matched_quantity=0.0,
                residual_quantity=0.0,
                contains_positive_fill_evidence=False,
            )

        if matched > 1e-9:
            outcome = (
                "open_position_with_residual"
                if residual > 1e-9
                else "open_position"
            )
            return PendingEntryTerminalDecision(
                outcome=outcome,
                reason="positive_fill_terminalized_with_matched_exposure",
                terminal=True,
                allows_pending_removal=True,
                healthy=True,
                matched_quantity=matched,
                residual_quantity=residual,
                contains_positive_fill_evidence=True,
            )

        return PendingEntryTerminalDecision(
            outcome="unmatched_residual_cleanup",
            reason="positive_fill_without_matched_hedge",
            terminal=True,
            allows_pending_removal=True,
            healthy=True,
            matched_quantity=0.0,
            residual_quantity=residual,
            contains_positive_fill_evidence=True,
        )

    def decide_supervision_stale_clear(
        self,
        pending: Any,
        *,
        live_truth: PendingEntryLiveTruth | None = None,
        passive_progress_found: bool = False,
    ) -> PendingEntryTerminalDecision:
        """V1 supervision stale-backlog clear authority.

        This path is intentionally narrower than the generic terminalizer: V1
        supervision may clear stale pending entries only after zero-fill,
        no live artifact, no inflight/cancel, a resting passive order, and a
        progress fetch that returned no order.
        """
        truth = live_truth or PendingEntryLiveTruth(available=True)
        maker_filled = _quantity(_get(pending, "maker_leg_filled", 0.0))
        hedge_filled = _quantity(_get(pending, "hedge_leg_filled", 0.0))
        has_positive_fill = maker_filled > 1e-9 or hedge_filled > 1e-9

        if not truth.available:
            return PendingEntryTerminalDecision(
                outcome="deferred_missing_live_truth",
                reason=truth.error or "exchange_truth_unavailable",
                terminal=False,
                allows_pending_removal=False,
                healthy=False,
                operator_block_required=True,
                contains_positive_fill_evidence=has_positive_fill,
            )

        if truth.has_live_open_order:
            return PendingEntryTerminalDecision(
                outcome="deferred_live_open_order",
                reason="live_maker_order_requires_owner_or_terminal_evidence",
                terminal=False,
                allows_pending_removal=False,
                healthy=False,
                contains_positive_fill_evidence=has_positive_fill,
            )

        if truth.has_live_position:
            return PendingEntryTerminalDecision(
                outcome="deferred_live_position",
                reason="live_position_truth_blocks_supervision_stale_clear",
                terminal=False,
                allows_pending_removal=False,
                healthy=False,
                contains_positive_fill_evidence=has_positive_fill,
            )

        if has_positive_fill:
            return PendingEntryTerminalDecision(
                outcome="deferred_positive_fill_evidence",
                reason="positive_fill_blocks_supervision_stale_clear",
                terminal=False,
                allows_pending_removal=False,
                healthy=False,
                matched_quantity=max(min(maker_filled, hedge_filled), 0.0),
                residual_quantity=abs(maker_filled - hedge_filled),
                contains_positive_fill_evidence=True,
            )

        if _get(pending, "hedge_inflight", None) is not None:
            return PendingEntryTerminalDecision(
                outcome="deferred_hedge_inflight",
                reason="hedge_inflight_blocks_supervision_stale_clear",
                terminal=False,
                allows_pending_removal=False,
                healthy=False,
                contains_positive_fill_evidence=False,
            )

        passive_order = _get(pending, "passive_order", None)
        if _passive_order_cancel_requested(passive_order):
            return PendingEntryTerminalDecision(
                outcome="deferred_cancel_requested",
                reason="cancel_requested_blocks_supervision_stale_clear",
                terminal=False,
                allows_pending_removal=False,
                healthy=False,
                contains_positive_fill_evidence=False,
            )

        state_value = _passive_order_state_value(passive_order)
        if state_value != "open":
            return PendingEntryTerminalDecision(
                outcome="deferred_passive_progress",
                reason="passive_order_not_resting",
                terminal=False,
                allows_pending_removal=False,
                healthy=False,
                contains_positive_fill_evidence=False,
            )

        if passive_progress_found:
            return PendingEntryTerminalDecision(
                outcome="deferred_passive_progress",
                reason="passive_progress_still_exists",
                terminal=False,
                allows_pending_removal=False,
                healthy=False,
                contains_positive_fill_evidence=False,
            )

        return PendingEntryTerminalDecision(
            outcome="supervision_passive_unfilled",
            reason="zero_fill_resting_progress_absent_no_live_artifact",
            terminal=True,
            allows_pending_removal=True,
            healthy=True,
            matched_quantity=0.0,
            residual_quantity=0.0,
            contains_positive_fill_evidence=False,
        )

    @staticmethod
    def remove_if_allowed(
        pending_entries: MutableMapping[str, Any],
        entry_id: str,
        decision: PendingEntryTerminalDecision,
    ) -> bool:
        if not decision.allows_pending_removal:
            return False
        pending_entries.pop(entry_id, None)
        return True


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _quantity(value: Any) -> float:
    try:
        result = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if result != result:
        return 0.0
    return max(result, 0.0)


def _passive_order_cancel_requested(passive_order: Any) -> bool:
    if passive_order is None:
        return False
    cancel_requested = _get(passive_order, "cancel_requested", None)
    if callable(cancel_requested):
        return bool(cancel_requested())
    return _quantity(_get(passive_order, "cancel_requested_at_ms", 0)) > 0.0


def _passive_order_state_value(passive_order: Any) -> str:
    if passive_order is None:
        return ""
    state = _get(passive_order, "last_progress_state", None)
    value = getattr(state, "value", state)
    return str(value or "").lower()
