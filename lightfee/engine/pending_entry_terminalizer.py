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
