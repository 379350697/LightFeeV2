"""Entry execution planner matching Rust plan_incremental_entry_execution."""

from __future__ import annotations

from enum import Enum


class ExecutionRoute(Enum):
    PASSIVE_INCREMENTAL = "passive_incremental"
    FALLBACK_TO_STANDARD = "fallback_to_standard"
    REJECTED = "rejected"


def plan_entry_execution(
    target_quantity: float,
    price_hint: float,
    min_notional_quote: float,
    maker_min_clip: float,
    hedge_chunk: float,
    max_initial_clip_ratio: float = 0.8,
) -> tuple[ExecutionRoute, float, str]:
    """Plan entry execution route. Returns (route, maker_clip_quantity, reason).

    - target_quantity: total base quantity to open
    - price_hint: reference price for notional computation
    - min_notional_quote: venue minimum notional
    - maker_min_clip: minimum valid maker clip quantity (from venue min notional)
    - hedge_chunk: minimum hedgeable chunk after floor alignment
    """
    if target_quantity <= 0:
        return (ExecutionRoute.REJECTED, 0.0, "zero_target_quantity")

    # Max initial clip
    max_clip = target_quantity * max_initial_clip_ratio

    # If maker min clip > max clip → fallback
    if maker_min_clip > max_clip:
        return (ExecutionRoute.FALLBACK_TO_STANDARD, 0.0, "maker_min_clip_exceeds_max_initial_clip")

    # Clip is at least maker_min_clip
    clip = max(maker_min_clip, target_quantity * 0.5)

    # If clip is too close to full target → fallback
    if target_quantity - clip < maker_min_clip:
        return (ExecutionRoute.FALLBACK_TO_STANDARD, 0.0, "clip_too_close_to_full_target")

    # Validate hedge quantity
    hedge_remainder = target_quantity - clip
    hedge_aligned = max(hedge_chunk, hedge_remainder - (hedge_remainder % hedge_chunk))
    if hedge_aligned <= 0:
        return (ExecutionRoute.REJECTED, 0.0, "zero_hedgeable_quantity")

    # Check min notional for maker clip
    maker_notional = clip * price_hint
    if maker_notional < min_notional_quote:
        # Raise clip to min notional
        raised_clip = min_notional_quote / price_hint if price_hint > 0 else 0
        if raised_clip >= target_quantity * 0.95:
            return (ExecutionRoute.FALLBACK_TO_STANDARD, 0.0, "raised_clip_too_close_to_full_target")
        clip = raised_clip

    return (ExecutionRoute.PASSIVE_INCREMENTAL, clip, "")
