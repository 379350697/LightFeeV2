"""Passive maker order management: timeouts, repricing, fallback rules."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class PassivePhase(Enum):
    HIGH_SLIPPAGE_MAKER = "high_slippage_maker"
    LOW_SLIPPAGE_MAKER = "low_slippage_maker"
    DUAL_TAKER = "dual_taker"


class MakerDecision(Enum):
    PLACE = "place"
    HOLD = "hold"
    AMEND = "amend"
    CANCEL_REPLACE = "cancel_replace"
    COOLDOWN = "cooldown"
    BUDGET_EXCEEDED = "budget_exceeded"


@dataclass
class PassiveMakerState:
    phase: PassivePhase = PassivePhase.HIGH_SLIPPAGE_MAKER
    zero_fill_cycles: int = 0
    max_zero_fill_cycles: int = 3
    consecutive_failures: int = 0
    ops_tokens: float = 8.0
    cooling_down_until_ms: int = 0


def decide_passive_action(
    current_price: float | None,
    target_price: float | None,
    reprice_threshold_ticks: int,
    cancel_replace_threshold_ticks: int,
    tick_size: float,
    min_amend_interval_ms: int,
    last_action_ms: int,
    now_ms: int,
    prefer_amend: bool,
    state: PassiveMakerState,
) -> tuple[MakerDecision, str]:
    """Decide next passive order management action.

    Returns (decision, reason).
    """
    # Cooldown check
    if state.cooling_down_until_ms > now_ms:
        return (MakerDecision.COOLDOWN, "cooling_down")

    # Budget check
    if state.ops_tokens <= 0:
        return (MakerDecision.BUDGET_EXCEEDED, "ops_budget_exhausted")

    # No market data
    if current_price is None or target_price is None or tick_size <= 0:
        return (MakerDecision.HOLD, "missing_book_data")

    distance_ticks = abs(current_price - target_price) / tick_size

    # Below reprice threshold
    if distance_ticks < reprice_threshold_ticks:
        return (MakerDecision.HOLD, "within_reprice_threshold")

    # Min amend interval not elapsed
    if now_ms - last_action_ms < min_amend_interval_ms:
        return (MakerDecision.HOLD, "min_amend_interval")

    # Within cancel-replace threshold + prefer amend
    if distance_ticks < cancel_replace_threshold_ticks and prefer_amend:
        return (MakerDecision.AMEND, "amend_within_cancel_replace_threshold")

    return (MakerDecision.CANCEL_REPLACE, "large_deviation")
