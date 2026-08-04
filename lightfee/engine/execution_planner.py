"""Entry execution planner matching Rust V1 plan_incremental_entry_execution.

Rust references:
- src/execution_core/entry_execution_planner.rs: plan_incremental_entry_execution (line 49)
- src/execution_core/entry_execution_planner.rs: bounded_maker_first_initial_target_quantity (line 33)
- src/execution_core/entry_execution_planner.rs: maker_min_valid_clip_quantity (line 210)
- src/engine/entry.rs: effective_entry_leg_notional_floor (line 430)
- src/engine/entry.rs: align_quantity_down_to_chunk (line 4558)
- src/engine/entry.rs: min_hedgeable_chunk_from_spec (line 4583)
- src/market_gateway/ports.rs: plan_venue_order_quantity (line 370)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from fractions import Fraction
from typing import Optional


class ExecutionRoute(Enum):
    PASSIVE_INCREMENTAL = "passive_incremental"
    FALLBACK_TO_STANDARD = "fallback_to_standard"
    REJECTED = "rejected"


@dataclass
class IncrementalEntryExecutionPlan:
    route: ExecutionRoute
    full_target_quantity: float = 0.0
    initial_maker_target_quantity: float = 0.0
    maker_min_valid_clip_quantity: Optional[float] = None
    reason: Optional[str] = None


# ---------------------------------------------------------------------------
# V1-equivalent helpers
# ---------------------------------------------------------------------------

_EPS = 1e-9


def quantities_match(left: float, right: float) -> bool:
    """V1 quantities_match (line 4527): epsilon comparison."""
    return abs(left - right) <= _EPS


def effective_entry_leg_notional_floor(
    global_min_entry_leg_notional_quote: float,
    exchange_min_notional_quote: Optional[float],
) -> float:
    """V1 effective_entry_leg_notional_floor (line 430)."""
    return max(
        global_min_entry_leg_notional_quote,
        0.0,
    ) if exchange_min_notional_quote is None else max(
        global_min_entry_leg_notional_quote,
        exchange_min_notional_quote,
        0.0,
    )


def align_quantity_down_to_chunk(quantity: float, chunk: float) -> float:
    """V1 align_quantity_down_to_chunk (line 4558)."""
    if not math.isfinite(quantity) or quantity <= 0.0:
        return 0.0
    if not math.isfinite(chunk) or chunk <= _EPS:
        return max(quantity, 0.0)
    whole_chunks = math.floor(quantity / chunk)
    if whole_chunks <= 0.0:
        return 0.0
    return whole_chunks * chunk


def align_quantity_up_to_step(quantity: float, step: float) -> float:
    """V1 align_quantity_up_to_step (line 4573)."""
    if not math.isfinite(quantity) or quantity <= 0.0:
        return 0.0
    if not math.isfinite(step) or step <= 1e-12:
        return quantity
    return math.ceil(quantity / step) * step


def common_executable_quantity_step(*steps: float) -> float:
    """Return the smallest base-quantity grid accepted by every leg.

    V1 plans hedgeability against the hedge venue's native step. V2 submits
    equal base quantities on both legs, so the planned quantity must also lie
    on the maker leg's grid before the maker order is submitted.
    """
    fractions: list[Fraction] = []
    for step in steps:
        if not math.isfinite(step) or step <= _EPS:
            return 0.0
        fractions.append(Fraction(str(step)).limit_denominator(1_000_000_000))
    if not fractions:
        return 0.0

    common_denominator = 1
    for step in fractions:
        common_denominator = math.lcm(common_denominator, step.denominator)

    common_numerator = 1
    for step in fractions:
        numerator = step.numerator * (common_denominator // step.denominator)
        common_numerator = math.lcm(common_numerator, numerator)
    return float(Fraction(common_numerator, common_denominator))


def min_hedgeable_chunk_from_notional(
    min_base_quantity: float,
    min_notional_quote: float,
    step_base_quantity: float,
    price_hint: Optional[float],
) -> float:
    """V1 min_hedgeable_chunk_from_spec (line 4583), simplified.

    Combines venue min base quantity and notional-derived quantity,
    aligned up to the venue step.
    """
    min_notional_quantity: float = 0.0
    if min_notional_quote > 0.0:
        if price_hint is None or not math.isfinite(price_hint) or price_hint <= 0.0:
            raise ValueError("price_hint required when min_notional_quote > 0")
        min_notional_quantity = min_notional_quote / price_hint

    raw_min_quantity = max(min_base_quantity, 0.0, min_notional_quantity)
    chunk = align_quantity_up_to_step(raw_min_quantity, max(step_base_quantity, 0.0))
    if chunk <= _EPS:
        raise ValueError("min hedgeable chunk rounded to zero")
    return chunk


def bounded_maker_first_initial_target_quantity(
    target_quantity: float,
    slice_ratio: float,
) -> float:
    """V1 bounded_maker_first_initial_target_quantity (line 33)."""
    target_quantity = max(target_quantity, 0.0)
    if target_quantity <= _EPS or slice_ratio >= 1.0 - _EPS:
        return target_quantity
    sliced_quantity = max(target_quantity * slice_ratio, 0.0)
    if sliced_quantity <= _EPS or sliced_quantity >= target_quantity - _EPS:
        return target_quantity
    return sliced_quantity


def maker_min_valid_clip_quantity(
    maker_min_notional_quote: float,
    maker_price_hint: Optional[float],
    min_hedgeable_chunk: float,
    full_target_quantity: float,
) -> Optional[float]:
    """V1 maker_min_valid_clip_quantity (line 210)."""
    if not math.isfinite(maker_min_notional_quote) or maker_min_notional_quote <= 0.0:
        return None
    if maker_price_hint is None or not math.isfinite(maker_price_hint) or maker_price_hint <= 0.0:
        return None
    raw_quantity = maker_min_notional_quote / maker_price_hint
    if not math.isfinite(raw_quantity) or raw_quantity <= _EPS:
        return None
    if math.isfinite(min_hedgeable_chunk) and min_hedgeable_chunk > _EPS:
        chunks = math.ceil(raw_quantity / min_hedgeable_chunk)
        return min(chunks * min_hedgeable_chunk, max(full_target_quantity, raw_quantity))
    return raw_quantity


def plan_incremental_entry_execution(
    target_quantity: float,
    slice_ratio: float,
    min_hedgeable_chunk: float,
    maker_min_notional_quote: float,
    maker_price_hint: Optional[float],
    max_initial_clip_ratio: float,
    hedge_min_notional_quote: float,
    hedge_price_hint: Optional[float],
) -> tuple[ExecutionRoute, IncrementalEntryExecutionPlan]:
    """V1 plan_incremental_entry_execution (line 49).

    Returns (route, plan) where plan carries full_target_quantity,
    initial_maker_target_quantity, maker_min_valid_clip_quantity, and reason.
    """
    _rejected = lambda reason: (
        ExecutionRoute.REJECTED,
        IncrementalEntryExecutionPlan(
            route=ExecutionRoute.REJECTED,
            reason=reason,
        ),
    )

    if not math.isfinite(target_quantity) or target_quantity <= _EPS:
        return _rejected("target_quantity_not_positive")

    # Compute aligned full target and initial maker target
    if not math.isfinite(min_hedgeable_chunk) or min_hedgeable_chunk <= _EPS:
        full_target_quantity = max(target_quantity, 0.0)
        initial_maker_target_quantity = bounded_maker_first_initial_target_quantity(
            target_quantity, slice_ratio
        )
    else:
        full_chunks = math.floor(target_quantity / min_hedgeable_chunk)
        if full_chunks <= 0.0:
            return _rejected("target_below_min_hedgeable_chunk")
        full_target_quantity = full_chunks * min_hedgeable_chunk
        raw_initial = bounded_maker_first_initial_target_quantity(
            full_target_quantity, slice_ratio
        )
        initial_chunks = math.floor(raw_initial / min_hedgeable_chunk)
        if initial_chunks <= 0.0:
            initial_maker_target_quantity = min(min_hedgeable_chunk, full_target_quantity)
        else:
            initial_maker_target_quantity = min(
                initial_chunks * min_hedgeable_chunk, full_target_quantity
            )

    maker_min_clip = maker_min_valid_clip_quantity(
        maker_min_notional_quote,
        maker_price_hint,
        min_hedgeable_chunk,
        full_target_quantity,
    )

    if maker_min_clip is not None:
        if maker_min_clip > full_target_quantity + _EPS:
            return (
                ExecutionRoute.REJECTED,
                IncrementalEntryExecutionPlan(
                    route=ExecutionRoute.REJECTED,
                    full_target_quantity=full_target_quantity,
                    maker_min_valid_clip_quantity=maker_min_clip,
                    reason="maker_min_clip_exceeds_full_target",
                ),
            )

        effective_max_ratio = (
            max_initial_clip_ratio
            if (math.isfinite(max_initial_clip_ratio) and max_initial_clip_ratio > 0.0)
            else 0.8
        )
        effective_max_ratio = min(effective_max_ratio, 1.0)

        if maker_min_clip / full_target_quantity > effective_max_ratio + _EPS:
            return (
                ExecutionRoute.FALLBACK_TO_STANDARD,
                IncrementalEntryExecutionPlan(
                    route=ExecutionRoute.FALLBACK_TO_STANDARD,
                    full_target_quantity=full_target_quantity,
                    initial_maker_target_quantity=maker_min_clip,
                    maker_min_valid_clip_quantity=maker_min_clip,
                    reason="maker_min_clip_too_close_to_full_target",
                ),
            )

        initial_maker_target_quantity = max(initial_maker_target_quantity, maker_min_clip)

    # Hedge-side validation
    hedge_min_clip = maker_min_valid_clip_quantity(
        hedge_min_notional_quote,
        hedge_price_hint,
        min_hedgeable_chunk,
        full_target_quantity,
    )
    if hedge_min_clip is not None:
        hedge_remainder = max(full_target_quantity - initial_maker_target_quantity, 0.0)
        maker_min_clip_value = maker_min_clip or 0.0

        if hedge_remainder <= _EPS:
            if full_target_quantity - hedge_min_clip >= _EPS:
                reduced_maker = (
                    math.floor((full_target_quantity - hedge_min_clip) / min_hedgeable_chunk)
                    * min_hedgeable_chunk
                )
                if reduced_maker >= maker_min_clip_value - _EPS:
                    initial_maker_target_quantity = max(reduced_maker, 0.0)
                else:
                    return (
                        ExecutionRoute.FALLBACK_TO_STANDARD,
                        IncrementalEntryExecutionPlan(
                            route=ExecutionRoute.FALLBACK_TO_STANDARD,
                            full_target_quantity=full_target_quantity,
                            initial_maker_target_quantity=initial_maker_target_quantity,
                            maker_min_valid_clip_quantity=maker_min_clip,
                            reason="hedge_remainder_below_min_notional",
                        ),
                    )
            else:
                return (
                    ExecutionRoute.FALLBACK_TO_STANDARD,
                    IncrementalEntryExecutionPlan(
                        route=ExecutionRoute.FALLBACK_TO_STANDARD,
                        full_target_quantity=full_target_quantity,
                        initial_maker_target_quantity=initial_maker_target_quantity,
                        maker_min_valid_clip_quantity=maker_min_clip,
                        reason="hedge_remainder_below_min_notional",
                    ),
                )
        elif hedge_remainder < hedge_min_clip - _EPS:
            if full_target_quantity - hedge_min_clip >= maker_min_clip_value - _EPS:
                reduced_maker = (
                    math.floor((full_target_quantity - hedge_min_clip) / min_hedgeable_chunk)
                    * min_hedgeable_chunk
                )
                if reduced_maker >= _EPS:
                    initial_maker_target_quantity = reduced_maker
                else:
                    return (
                        ExecutionRoute.FALLBACK_TO_STANDARD,
                        IncrementalEntryExecutionPlan(
                            route=ExecutionRoute.FALLBACK_TO_STANDARD,
                            full_target_quantity=full_target_quantity,
                            initial_maker_target_quantity=initial_maker_target_quantity,
                            maker_min_valid_clip_quantity=maker_min_clip,
                            reason="hedge_remainder_below_min_notional",
                        ),
                    )
            else:
                return (
                    ExecutionRoute.FALLBACK_TO_STANDARD,
                    IncrementalEntryExecutionPlan(
                        route=ExecutionRoute.FALLBACK_TO_STANDARD,
                        full_target_quantity=full_target_quantity,
                        initial_maker_target_quantity=initial_maker_target_quantity,
                        maker_min_valid_clip_quantity=maker_min_clip,
                        reason="hedge_remainder_below_min_notional",
                    ),
                )

    return (
        ExecutionRoute.PASSIVE_INCREMENTAL,
        IncrementalEntryExecutionPlan(
            route=ExecutionRoute.PASSIVE_INCREMENTAL,
            full_target_quantity=full_target_quantity,
            initial_maker_target_quantity=initial_maker_target_quantity,
            maker_min_valid_clip_quantity=maker_min_clip,
            reason=None,
        ),
    )


# ---------------------------------------------------------------------------
# Legacy adapter preserved for backward compatibility
# ---------------------------------------------------------------------------


def plan_entry_execution(
    target_quantity: float,
    price_hint: float,
    min_notional_quote: float,
    maker_min_clip: float,
    hedge_chunk: float,
    max_initial_clip_ratio: float = 0.8,
) -> tuple[ExecutionRoute, float, str]:
    """Legacy wrapper: delegates to plan_incremental_entry_execution.

    Backward-compatible interface for existing callers. New code should
    call plan_incremental_entry_execution directly.
    """
    price = price_hint if price_hint > 0 else None
    maker_notional_from_clip = maker_min_clip * price_hint if price_hint > 0 else 0.0
    effective_maker_notional = max(min_notional_quote, maker_notional_from_clip)

    route, plan = plan_incremental_entry_execution(
        target_quantity=target_quantity,
        slice_ratio=0.5,
        min_hedgeable_chunk=hedge_chunk,
        maker_min_notional_quote=effective_maker_notional,
        maker_price_hint=price,
        max_initial_clip_ratio=max_initial_clip_ratio,
        hedge_min_notional_quote=min_notional_quote,
        hedge_price_hint=price,
    )
    return (route, plan.initial_maker_target_quantity, plan.reason or "")
