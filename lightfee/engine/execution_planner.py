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
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class ExecutionRoute(Enum):
    PASSIVE_INCREMENTAL = "passive_incremental"
    FALLBACK_TO_STANDARD = "fallback_to_standard"
    REJECTED = "rejected"


@dataclass(frozen=True)
class IncrementalEntryExecutionPlan:
    route: ExecutionRoute
    full_target_quantity: float = 0.0
    initial_maker_target_quantity: float = 0.0
    maker_min_valid_clip_quantity: Optional[float] = None
    reason: Optional[str] = None


@dataclass(frozen=True)
class ExecutableEntryEnvelope:
    """Shared V1 entry sizing envelope for selection and dispatch."""

    plan: IncrementalEntryExecutionPlan
    maker_leg: str
    hedge_leg: str
    requested_quantity: float
    effective_dispatch_quantity: float
    min_hedgeable_chunk: float = 0.0
    blocker_reason: str = ""
    blocker_evidence: dict[str, object] = field(default_factory=dict)
    context_key: tuple[object, ...] = field(default_factory=tuple)


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


def entry_pair_minimum_reason(
    *,
    quantity: float,
    long_price: float,
    short_price: float,
    long_metadata: dict,
    short_metadata: dict,
    strategy_min_notional: float,
) -> tuple[str, dict[str, object]]:
    failures: list[dict[str, float | str]] = []
    for leg, price, metadata in (
        ("long", long_price, long_metadata),
        ("short", short_price, short_metadata),
    ):
        min_quantity = float(metadata.get("min_quantity", 0.0) or 0.0)
        min_notional = max(
            float(metadata.get("min_notional", 0.0) or 0.0),
            max(float(strategy_min_notional or 0.0), 0.0),
        )
        notional = max(float(quantity or 0.0), 0.0) * max(
            float(price or 0.0), 0.0
        )
        if quantity + 1e-12 < min_quantity or notional + 1e-9 < min_notional:
            failures.append(
                {
                    "leg": leg,
                    "quantity": quantity,
                    "price": price,
                    "notional_quote": notional,
                    "min_quantity": min_quantity,
                    "min_notional_quote": min_notional,
                }
            )
    if not failures:
        return "", {}
    return "entry_pair_minimum_not_met", {"pair_minimum_failures": failures}


def executable_entry_envelope_context_key(
    *,
    target_quantity: float,
    maker_leg: str,
    long_price: float,
    short_price: float,
    long_metadata: dict,
    short_metadata: dict,
    strategy_min_notional: float,
    common_base_quantity_step: float,
    slice_ratio: float,
    max_initial_clip_ratio: float,
) -> tuple[object, ...]:
    maker_leg_name = str(maker_leg or "").lower()
    if maker_leg_name not in {"long", "short"}:
        maker_leg_name = "long"

    def _float_value(value) -> float:
        try:
            numeric = float(value or 0.0)
        except (TypeError, ValueError, OverflowError):
            return 0.0
        return numeric if math.isfinite(numeric) else 0.0

    def _metadata_key(metadata: dict) -> tuple[float, float]:
        if not isinstance(metadata, dict):
            metadata = {}
        return (
            _float_value(metadata.get("min_quantity", 0.0)),
            _float_value(metadata.get("min_notional", 0.0)),
        )

    return (
        _float_value(target_quantity),
        maker_leg_name,
        _float_value(long_price),
        _float_value(short_price),
        _metadata_key(long_metadata),
        _metadata_key(short_metadata),
        _float_value(strategy_min_notional),
        _float_value(common_base_quantity_step),
        _float_value(slice_ratio),
        _float_value(max_initial_clip_ratio),
    )


def build_executable_entry_envelope(
    *,
    target_quantity: float,
    maker_leg: str,
    long_price: float,
    short_price: float,
    long_metadata: dict,
    short_metadata: dict,
    strategy_min_notional: float,
    common_base_quantity_step: float,
    slice_ratio: float,
    max_initial_clip_ratio: float,
) -> ExecutableEntryEnvelope:
    """Build the single executable entry envelope reused by live selection/dispatch."""

    context_key = executable_entry_envelope_context_key(
        target_quantity=target_quantity,
        maker_leg=maker_leg,
        long_price=long_price,
        short_price=short_price,
        long_metadata=long_metadata,
        short_metadata=short_metadata,
        strategy_min_notional=strategy_min_notional,
        common_base_quantity_step=common_base_quantity_step,
        slice_ratio=slice_ratio,
        max_initial_clip_ratio=max_initial_clip_ratio,
    )
    requested_quantity = max(float(target_quantity or 0.0), 0.0)
    maker_leg_name = str(maker_leg or "").lower()
    if maker_leg_name not in {"long", "short"}:
        maker_leg_name = "long"
    hedge_leg_name = "short" if maker_leg_name == "long" else "long"
    maker_metadata = long_metadata if maker_leg_name == "long" else short_metadata
    hedge_metadata = short_metadata if maker_leg_name == "long" else long_metadata
    maker_price_source = long_price if maker_leg_name == "long" else short_price
    hedge_price_source = short_price if maker_leg_name == "long" else long_price
    maker_price = float(maker_price_source or 0.0)
    hedge_price = float(hedge_price_source or 0.0)

    def _rejected(reason: str, evidence: dict[str, object]) -> ExecutableEntryEnvelope:
        return ExecutableEntryEnvelope(
            context_key=context_key,
            plan=IncrementalEntryExecutionPlan(
                route=ExecutionRoute.REJECTED,
                reason=reason,
            ),
            maker_leg=maker_leg_name,
            hedge_leg=hedge_leg_name,
            requested_quantity=requested_quantity,
            effective_dispatch_quantity=0.0,
            blocker_reason=reason,
            blocker_evidence=evidence,
        )

    minimum_reason, minimum_evidence = entry_pair_minimum_reason(
        quantity=requested_quantity,
        long_price=float(long_price or 0.0),
        short_price=float(short_price or 0.0),
        long_metadata=long_metadata,
        short_metadata=short_metadata,
        strategy_min_notional=strategy_min_notional,
    )
    if minimum_reason:
        return _rejected(minimum_reason, minimum_evidence)

    maker_min_notional = max(
        float(strategy_min_notional or 0.0),
        float(maker_metadata.get("min_notional", 0.0) or 0.0),
    )
    hedge_min_notional = max(
        float(strategy_min_notional or 0.0),
        float(hedge_metadata.get("min_notional", 0.0) or 0.0),
    )
    maker_min_quantity = max(
        float(maker_metadata.get("min_quantity", 0.0) or 0.0), 0.0
    )
    hedge_min_quantity = max(
        float(hedge_metadata.get("min_quantity", 0.0) or 0.0), 0.0
    )
    if maker_price > 0.0 and maker_min_quantity > 0.0:
        maker_min_notional = max(maker_min_notional, maker_min_quantity * maker_price)
    evidence = {
        "target_quantity": requested_quantity,
        "maker_leg": maker_leg_name,
        "hedge_leg": hedge_leg_name,
        "maker_price_hint": maker_price,
        "hedge_price_hint": hedge_price,
        "maker_min_notional_quote": maker_min_notional,
        "hedge_min_notional_quote": hedge_min_notional,
        "maker_min_quantity": maker_min_quantity,
        "hedge_min_quantity": hedge_min_quantity,
        "common_base_quantity_step": float(common_base_quantity_step or 0.0),
    }
    try:
        min_hedgeable_chunk = min_hedgeable_chunk_from_notional(
            min_base_quantity=hedge_min_quantity,
            min_notional_quote=hedge_min_notional,
            step_base_quantity=float(common_base_quantity_step or 0.0),
            price_hint=hedge_price if hedge_price > 0.0 else None,
        )
    except ValueError:
        return _rejected("min_hedgeable_chunk_invalid", evidence)
    route, plan = plan_incremental_entry_execution(
        target_quantity=requested_quantity,
        slice_ratio=slice_ratio,
        min_hedgeable_chunk=min_hedgeable_chunk,
        maker_min_notional_quote=maker_min_notional,
        maker_price_hint=maker_price if maker_price > 0.0 else None,
        max_initial_clip_ratio=max_initial_clip_ratio,
        hedge_min_notional_quote=hedge_min_notional,
        hedge_price_hint=hedge_price if hedge_price > 0.0 else None,
    )
    if route == ExecutionRoute.REJECTED:
        reason = str(plan.reason or "planner_rejected_entry")
        rejected_evidence = {
            **evidence,
            "min_hedgeable_chunk": min_hedgeable_chunk,
            "maker_min_valid_clip_quantity": plan.maker_min_valid_clip_quantity,
            "planner_route": route.value,
            "planner_reason": reason,
        }
        return _rejected(reason, rejected_evidence)
    effective_dispatch_quantity = (
        plan.initial_maker_target_quantity
        if route == ExecutionRoute.PASSIVE_INCREMENTAL
        else plan.full_target_quantity
    )
    return ExecutableEntryEnvelope(
        context_key=context_key,
        plan=plan,
        maker_leg=maker_leg_name,
        hedge_leg=hedge_leg_name,
        requested_quantity=requested_quantity,
        effective_dispatch_quantity=effective_dispatch_quantity,
        min_hedgeable_chunk=min_hedgeable_chunk,
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
