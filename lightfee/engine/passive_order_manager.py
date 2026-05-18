"""V1 PassiveOrderManager: token-bucket ops budget, cooldown, and repricing decisions.

Rust references:
- src/execution_core/passive_order_manager.rs (complete)
- src/execution_core/entry_sync.rs:1554-1851 (maintain_pending_entry_passive_order)

This module provides the full V1 state machine for passive (maker) order
management: ops budget via token bucket, consecutive-failure cooldown,
supports_amend capability check, and the full decide() decision tree
matching V1 PassiveOrderManager.decide().
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class PassiveSkipReason(Enum):
    BELOW_REPRICE_THRESHOLD = "below_reprice_threshold"
    MIN_AMEND_INTERVAL_NOT_ELAPSED = "min_amend_interval_not_elapsed"
    OPS_BUDGET_EXCEEDED = "ops_budget_exceeded"
    FOLLOW_MARKET_REPRICE_DISABLED = "follow_market_reprice_disabled"
    MISSING_BOOK_DATA = "missing_book_data"


class PassiveReplaceReason(Enum):
    TIMEOUT = "timeout"
    LARGE_DEVIATION = "large_deviation"
    AMEND_UNSUPPORTED = "amend_unsupported"
    AMEND_FAILED = "amend_failed"


class PassiveCooldownReason(Enum):
    CONSECUTIVE_FAILURES_EXCEEDED = "consecutive_failures_exceeded"
    ACTIVE_COOLDOWN = "active_cooldown"


class PassiveOrderManagerDecisionType(Enum):
    HOLD = "hold"
    PLACE = "place"
    AMEND = "amend"
    CANCEL_REPLACE = "cancel_replace"
    COOLDOWN = "cooldown"
    COMPLETE = "complete"


@dataclass
class PassiveOrderManagerDecision:
    """V1 PassiveOrderManagerDecision (passive_order_manager.rs:43-66)."""
    kind: PassiveOrderManagerDecisionType
    price: float = 0.0
    quantity: float = 0.0
    new_price: float = 0.0
    new_quantity: float | None = None
    until_ms: int = 0
    skip_reason: PassiveSkipReason | None = None
    replace_reason: PassiveReplaceReason | None = None
    cooldown_reason: PassiveCooldownReason | None = None


@dataclass
class PassiveOrderManagerProfile:
    """V1 PassiveOrderManagerProfile (passive_order_manager.rs:69-83)."""
    reprice_threshold_ticks: int = 2
    cancel_replace_threshold_ticks: int = 5
    min_amend_interval_ms: int = 300
    reprice_threshold_bps: float = 2.0
    cancel_replace_threshold_bps: float = 6.0
    ops_bucket_capacity: float = 8.0
    ops_bucket_refill_per_sec: float = 8.0
    working_timeout_ms: int = 3000
    max_ops_per_sec: int = 8
    max_consecutive_failures: int = 5
    failure_cooldown_ms: int = 5000
    follow_market_reprice_enabled: bool = True
    prefer_amend_over_cancel: bool = True


@dataclass
class PassiveOrderDecisionInput:
    """V1 PassiveOrderDecisionInput (passive_order_manager.rs:97-106)."""
    tick_size: float
    reference_mid_price: float | None = None
    target_price: float | None = None
    current_price: float | None = None
    resting_since_ms: int | None = None
    target_quantity: float = 0.0
    supports_amend: bool = True


def passive_price_distance_bps(
    current_price: float | None,
    target_price: float | None,
    reference_mid_price: float | None,
) -> float | None:
    """V1 passive_price_distance_bps (passive_order_manager.rs:121-131)."""
    if current_price is None or not (current_price > 0):
        return None
    if target_price is None or not (target_price > 0):
        return None
    if reference_mid_price is None or not (reference_mid_price > 0):
        return None
    return abs(current_price - target_price) / reference_mid_price * 10_000.0


def passive_tick_distance(
    current_price: float | None,
    target_price: float | None,
    tick_size: float,
) -> float | None:
    """V1 passive_tick_distance (passive_order_manager.rs:108-119)."""
    if tick_size <= 0:
        return None
    if current_price is None or not (current_price > 0):
        return None
    if target_price is None or not (target_price > 0):
        return None
    return abs(current_price - target_price) / tick_size


class PassiveOrderManager:
    """V1 PassiveOrderManager (passive_order_manager.rs:134-418).

    Provides token-bucket ops budget, consecutive-failure cooldown,
    and the full V1 decide() decision tree for passive order management.
    """

    def __init__(self, profile: PassiveOrderManagerProfile) -> None:
        self._profile = profile
        self._last_action_at_ms: int | None = None
        self._ops_bucket_last_refill_at_ms: int | None = None
        self._ops_bucket_tokens: float = self._effective_capacity()
        self._consecutive_failures: int = 0
        self._cooldown_until_ms: int | None = None

    # ---- public API ----

    def decide(
        self, input_: PassiveOrderDecisionInput, now_ms: int,
    ) -> PassiveOrderManagerDecision:
        """V1 PassiveOrderManager::decide (passive_order_manager.rs:272-400)."""
        # Cooldown gate
        if self._cooldown_until_ms is not None and now_ms < self._cooldown_until_ms:
            return PassiveOrderManagerDecision(
                kind=PassiveOrderManagerDecisionType.COOLDOWN,
                until_ms=self._cooldown_until_ms,
                cooldown_reason=PassiveCooldownReason.ACTIVE_COOLDOWN,
            )
        self._cooldown_until_ms = None

        # Ops budget gate
        self._refill_ops_bucket(now_ms)
        if self._ops_bucket_tokens < 1.0:
            return PassiveOrderManagerDecision(
                kind=PassiveOrderManagerDecisionType.HOLD,
                skip_reason=PassiveSkipReason.OPS_BUDGET_EXCEEDED,
            )

        target_price = input_.target_price
        if target_price is None or not (target_price > 0):
            return PassiveOrderManagerDecision(
                kind=PassiveOrderManagerDecisionType.HOLD,
                skip_reason=PassiveSkipReason.MISSING_BOOK_DATA,
            )

        if input_.tick_size <= 0:
            return PassiveOrderManagerDecision(
                kind=PassiveOrderManagerDecisionType.HOLD,
                skip_reason=PassiveSkipReason.MISSING_BOOK_DATA,
            )

        current_price = input_.current_price
        if current_price is None:
            return PassiveOrderManagerDecision(
                kind=PassiveOrderManagerDecisionType.PLACE,
                price=target_price,
                quantity=input_.target_quantity,
            )

        # Timeout → cancel-replace
        resting = input_.resting_since_ms
        if resting is not None and (now_ms - resting) >= self._profile.working_timeout_ms:
            return PassiveOrderManagerDecision(
                kind=PassiveOrderManagerDecisionType.CANCEL_REPLACE,
                new_price=target_price,
                new_quantity=input_.target_quantity,
                replace_reason=PassiveReplaceReason.TIMEOUT,
            )

        # Reprice threshold
        tick_dist = passive_tick_distance(current_price, target_price, input_.tick_size)
        bps_dist = passive_price_distance_bps(
            current_price, target_price, input_.reference_mid_price,
        )
        reprice_threshold_bps = self._threshold_bps(
            self._profile.reprice_threshold_bps,
            self._profile.reprice_threshold_ticks,
            input_.tick_size,
            input_.reference_mid_price,
        )
        below_reprice = (
            bps_dist is not None and reprice_threshold_bps is not None
            and bps_dist < reprice_threshold_bps
        ) or (
            tick_dist is not None
            and tick_dist < self._profile.reprice_threshold_ticks
        )
        if below_reprice:
            return PassiveOrderManagerDecision(
                kind=PassiveOrderManagerDecisionType.HOLD,
                skip_reason=PassiveSkipReason.BELOW_REPRICE_THRESHOLD,
            )

        if not self._profile.follow_market_reprice_enabled:
            return PassiveOrderManagerDecision(
                kind=PassiveOrderManagerDecisionType.HOLD,
                skip_reason=PassiveSkipReason.FOLLOW_MARKET_REPRICE_DISABLED,
            )

        # Min amend interval
        last_action = self._last_action_at_ms
        if last_action is not None and (now_ms - last_action) < self._profile.min_amend_interval_ms:
            return PassiveOrderManagerDecision(
                kind=PassiveOrderManagerDecisionType.HOLD,
                skip_reason=PassiveSkipReason.MIN_AMEND_INTERVAL_NOT_ELAPSED,
            )

        cancel_replace_threshold_bps = self._threshold_bps(
            self._profile.cancel_replace_threshold_bps,
            self._profile.cancel_replace_threshold_ticks,
            input_.tick_size,
            input_.reference_mid_price,
        )
        below_cancel_replace = (
            bps_dist is not None and cancel_replace_threshold_bps is not None
            and bps_dist < cancel_replace_threshold_bps
        ) or (
            tick_dist is not None
            and tick_dist < self._profile.cancel_replace_threshold_ticks
        )

        # Prefer amend when supported and deviation is below cancel-replace threshold
        if (
            input_.supports_amend
            and self._profile.prefer_amend_over_cancel
            and below_cancel_replace
        ):
            return PassiveOrderManagerDecision(
                kind=PassiveOrderManagerDecisionType.AMEND,
                new_price=target_price,
                new_quantity=input_.target_quantity,
            )

        # Cancel-replace needs 2 ops tokens (cancel + submit)
        if self._ops_bucket_tokens < 2.0:
            return PassiveOrderManagerDecision(
                kind=PassiveOrderManagerDecisionType.HOLD,
                skip_reason=PassiveSkipReason.OPS_BUDGET_EXCEEDED,
            )

        return PassiveOrderManagerDecision(
            kind=PassiveOrderManagerDecisionType.CANCEL_REPLACE,
            new_price=target_price,
            new_quantity=input_.target_quantity,
            replace_reason=(
                PassiveReplaceReason.LARGE_DEVIATION
                if input_.supports_amend
                else PassiveReplaceReason.AMEND_UNSUPPORTED
            ),
        )

    def note_operation(self, now_ms: int) -> None:
        """V1: consume one ops token. Call BEFORE submitting an order."""
        self._refill_ops_bucket(now_ms)
        if self._ops_bucket_tokens >= 1.0:
            self._ops_bucket_tokens -= 1.0
        else:
            self._ops_bucket_tokens = 0.0
        self._last_action_at_ms = now_ms

    def note_success(self, now_ms: int) -> None:
        """V1: record successful operation — resets consecutive failures."""
        self._refill_ops_bucket(now_ms)
        self._consecutive_failures = 0
        self._last_action_at_ms = now_ms
        self._cooldown_until_ms = None

    def note_failure(self, now_ms: int) -> None:
        """V1: record failed operation — may trigger cooldown."""
        self._refill_ops_bucket(now_ms)
        self._consecutive_failures += 1
        self._last_action_at_ms = now_ms
        if self._consecutive_failures >= self._profile.max_consecutive_failures:
            self._cooldown_until_ms = now_ms + self._profile.failure_cooldown_ms

    def is_in_cooldown(self, now_ms: int) -> bool:
        return self._cooldown_until_ms is not None and now_ms < self._cooldown_until_ms

    @property
    def ops_bucket_tokens(self) -> float:
        return self._ops_bucket_tokens

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    @property
    def cooldown_until_ms(self) -> int | None:
        return self._cooldown_until_ms

    def runtime_dict(self) -> dict:
        """Serialize to dict matching V1 PassiveOrderManagerRuntime fields."""
        return {
            "last_action_at_ms": self._last_action_at_ms,
            "ops_window_started_at_ms": self._ops_bucket_last_refill_at_ms,
            "ops_in_window": int(
                max(0.0, self._effective_capacity() - self._ops_bucket_tokens)
            ),
            "ops_bucket_last_refill_at_ms": self._ops_bucket_last_refill_at_ms,
            "ops_bucket_tokens": self._ops_bucket_tokens,
            "consecutive_failures": self._consecutive_failures,
            "cooldown_until_ms": self._cooldown_until_ms,
        }

    # ---- internal ----

    def _effective_capacity(self) -> float:
        return min(
            self._profile.ops_bucket_capacity,
            float(max(self._profile.max_ops_per_sec, 1)),
        )

    def _effective_refill_per_sec(self) -> float:
        return min(
            self._profile.ops_bucket_refill_per_sec,
            float(max(self._profile.max_ops_per_sec, 1)),
        )

    def _refill_ops_bucket(self, now_ms: int) -> None:
        capacity = self._effective_capacity()
        refill_per_sec = self._effective_refill_per_sec()
        last = self._ops_bucket_last_refill_at_ms
        if last is not None:
            elapsed_ms = max(0, now_ms - last)
            refill = elapsed_ms * refill_per_sec / 1000.0
            self._ops_bucket_tokens = min(capacity, self._ops_bucket_tokens + refill)
        else:
            self._ops_bucket_tokens = capacity
        self._ops_bucket_last_refill_at_ms = now_ms

    @staticmethod
    def _threshold_bps(
        configured_bps: float,
        threshold_ticks: int,
        tick_size: float,
        reference_mid_price: float | None,
    ) -> float | None:
        """V1 threshold_bps: max(configured_bps, tick_floor_bps)."""
        if reference_mid_price is not None and reference_mid_price > 0 and tick_size > 0:
            tick_floor_bps = threshold_ticks * tick_size / reference_mid_price * 10_000.0
            return max(configured_bps, tick_floor_bps)
        return configured_bps if configured_bps >= 0 else None
