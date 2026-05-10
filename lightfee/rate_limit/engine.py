"""Token-bucket rate-limit engine matching Rust RateLimitEngine behavior."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class RateLimitErrorReason(Enum):
    COOLDOWN = "cooldown"
    MIN_INTERVAL = "min_interval"
    BUDGET_EXCEEDED = "budget_exceeded"


@dataclass
class RateLimitError(Exception):
    """Returned when try_consume fails, with retry hint."""

    reason: RateLimitErrorReason
    retry_in_ms: int = 0

    def __str__(self) -> str:
        return f"RateLimitError({self.reason.value}, retry_in_ms={self.retry_in_ms})"


@dataclass
class BucketState:
    """Single token bucket with capacity, refill, and backoff state."""

    capacity: float
    refill_per_sec: float
    tokens: float
    cooldown_until_ms: int = 0
    backoff_until_ms: int = 0
    min_interval_ms: int = 0
    last_consume_ms: int = 0

    def refill(self, now_ms: int) -> None:
        """Top up tokens based on elapsed time since last refill."""
        elapsed_ms = max(0, now_ms - self.last_consume_ms)
        if elapsed_ms <= 0:
            return
        added = (elapsed_ms / 1000.0) * self.refill_per_sec
        self.tokens = min(self.capacity, self.tokens + added)

    def can_consume(self, weight: float, now_ms: int) -> Optional[RateLimitError]:
        """Check if weight can be consumed now. Returns None on success."""
        if self.cooldown_until_ms > now_ms:
            return RateLimitError(
                reason=RateLimitErrorReason.COOLDOWN,
                retry_in_ms=self.cooldown_until_ms - now_ms,
            )
        if self.backoff_until_ms > now_ms:
            return RateLimitError(
                reason=RateLimitErrorReason.COOLDOWN,
                retry_in_ms=self.backoff_until_ms - now_ms,
            )
        if self.min_interval_ms > 0 and self.last_consume_ms > 0:
            next_allowed = self.last_consume_ms + self.min_interval_ms
            if now_ms < next_allowed:
                return RateLimitError(
                    reason=RateLimitErrorReason.MIN_INTERVAL,
                    retry_in_ms=next_allowed - now_ms,
                )
        if self.tokens < weight:
            return RateLimitError(
                reason=RateLimitErrorReason.BUDGET_EXCEEDED,
                retry_in_ms=max(0, int((weight - self.tokens) / max(self.refill_per_sec, 1e-9) * 1000)),
            )
        return None

    def consume(self, weight: float, now_ms: int) -> None:
        """Deduct weight from the bucket and record timestamp."""
        self.tokens -= weight
        self.last_consume_ms = now_ms


class RateLimitEngine:
    """Token-bucket engine with scoped buckets, weights, and intervals."""

    def __init__(self, default_margin: float = 0.95) -> None:
        self._buckets: dict[str, BucketState] = {}
        self._weights: dict[str, dict[str, float]] = {}
        self._min_intervals: dict[str, dict[str, int]] = {}
        self._default_margin = default_margin
        self._last_scope_request_ms: dict[str, dict[str, int | None]] = {}

    # -- Registration --------------------------------------------------

    def register_bucket(
        self,
        bucket_id: str,
        capacity: float,
        refill_per_sec: float,
    ) -> None:
        """Register or update a token bucket."""
        margin = self._default_margin
        capped_capacity = capacity * margin
        if bucket_id in self._buckets:
            b = self._buckets[bucket_id]
            b.capacity = capped_capacity
            b.refill_per_sec = refill_per_sec
            b.tokens = min(b.tokens, capped_capacity)
        else:
            self._buckets[bucket_id] = BucketState(
                capacity=capped_capacity,
                refill_per_sec=refill_per_sec,
                tokens=capped_capacity,
            )

    def register_weight(self, bucket_id: str, scope: str, weight: float) -> None:
        """Set the cost weight for a scope within a bucket."""
        self._weights.setdefault(bucket_id, {})[scope] = weight

    def register_min_interval(self, bucket_id: str, scope: str, interval_ms: int) -> None:
        """Set the minimum interval between requests for a scope."""
        self._min_intervals.setdefault(bucket_id, {})[scope] = interval_ms

    # -- Consumption ---------------------------------------------------

    def try_consume(
        self, bucket_id: str, scopes: list[str], now_ms: int | None = None
    ) -> None:
        """Attempt to consume one unit from a bucket across scopes.

        Raises RateLimitError if any scope/bucket is blocked.
        """
        if now_ms is None:
            now_ms = int(time.time() * 1000)

        bucket = self._buckets.get(bucket_id)
        if bucket is None:
            return  # unregistered bucket → always allow

        bucket.refill(now_ms)

        # Compute total weight for requested scopes
        scope_weights = self._weights.get(bucket_id, {})
        total_weight = sum(scope_weights.get(s, 1.0) for s in scopes) if scopes else 1.0

        # Check each scope's min-interval (per-scope tracked)
        scope_intervals = self._min_intervals.get(bucket_id, {})
        scope_times = self._last_scope_request_ms.setdefault(bucket_id, {})
        max_min_interval_retry = 0
        for scope in scopes:
            interval = scope_intervals.get(scope, 0)
            if interval > 0:
                last_req = scope_times.get(scope)
                if last_req is not None:
                    elapsed = now_ms - last_req
                    if elapsed < interval:
                        retry = interval - elapsed
                        max_min_interval_retry = max(max_min_interval_retry, retry)
        if max_min_interval_retry > 0:
            raise RateLimitError(
                reason=RateLimitErrorReason.MIN_INTERVAL,
                retry_in_ms=max_min_interval_retry,
            )

        # Check bucket-level constraints (cooldown, backoff, budget)
        error = bucket.can_consume(total_weight, now_ms)
        if error is not None:
            raise error

        bucket.consume(total_weight, now_ms)
        # Record per-scope request timestamps for min-interval tracking
        for scope in scopes:
            scope_times[scope] = now_ms

    def try_consume_scopes(
        self, scopes: list[str], now_ms: int | None = None
    ) -> None:
        """Consume from every registered bucket for the given scopes."""
        for bucket_id in self._buckets:
            self.try_consume(bucket_id, scopes, now_ms)

    # -- Cooldown / backoff --------------------------------------------

    def apply_cooldown(self, bucket_id: str, duration_ms: int, now_ms: int | None = None) -> None:
        """Apply a cooldown to a specific bucket."""
        if now_ms is None:
            now_ms = int(time.time() * 1000)
        bucket = self._buckets.get(bucket_id)
        if bucket is not None:
            bucket.cooldown_until_ms = max(bucket.cooldown_until_ms, now_ms + duration_ms)

    def apply_backoff(self, bucket_id: str, duration_ms: int, now_ms: int | None = None) -> None:
        """Apply exponential backoff to a specific bucket."""
        if now_ms is None:
            now_ms = int(time.time() * 1000)
        bucket = self._buckets.get(bucket_id)
        if bucket is not None:
            bucket.backoff_until_ms = max(bucket.backoff_until_ms, now_ms + duration_ms)

    # -- Read ----------------------------------------------------------

    def bucket_snapshot(self, bucket_id: str) -> Optional[dict]:
        """Return a read-only snapshot of a bucket."""
        b = self._buckets.get(bucket_id)
        if b is None:
            return None
        return {
            "capacity": b.capacity,
            "refill_per_sec": b.refill_per_sec,
            "tokens": b.tokens,
            "cooldown_until_ms": b.cooldown_until_ms,
            "backoff_until_ms": b.backoff_until_ms,
            "last_consume_ms": b.last_consume_ms,
        }

    @property
    def bucket_ids(self) -> list[str]:
        return list(self._buckets.keys())


class RateLimitRuntime:
    """High-level rate-limit runtime wrapping engine, config, and recommendations."""

    def __init__(
        self,
        engine: RateLimitEngine | None = None,
        config_manager=None,
        recommendation_engine=None,
    ) -> None:
        self.engine = engine or RateLimitEngine()
        self.config_manager = config_manager
        self.recommendation_engine = recommendation_engine
        self._last_refresh_ms: int = 0

    async def refresh(self, now_ms: int | None = None) -> None:
        """Hot-reload rate-limit config from disk if changed."""
        if self.config_manager is None:
            return
        if now_ms is None:
            now_ms = int(time.time() * 1000)
        outcome = self.config_manager.refresh(now_ms)
        if outcome == "reloaded":
            self._apply_config(self.config_manager.config)

    def refresh_interval_secs(self) -> int:
        """Recommended interval between refresh attempts."""
        return 30

    def wait_until_ready_for_scopes(
        self, scopes: list[str], timeout_ms: int = 0
    ) -> bool:
        """Block until the given scopes are available (polling, not async)."""
        deadline = int(time.time() * 1000) + timeout_ms
        while True:
            try:
                self.engine.try_consume_scopes(scopes)
                return True
            except RateLimitError as e:
                if timeout_ms > 0 and int(time.time() * 1000) + e.retry_in_ms > deadline:
                    return False
                time.sleep(max(0.0, e.retry_in_ms / 1000.0))

    def record_rate_limit_for_scopes(
        self, scopes: list[str], retry_after_ms: int = 0
    ) -> None:
        """Record a 429 / rate-limit-hit for later recommendations."""
        if self.recommendation_engine is not None:
            for scope in scopes:
                self.recommendation_engine.record_limit_hit(scope, retry_after_ms)

    def drain_journal_events(self) -> list[dict]:
        """Return accumulated recommendation events as dicts."""
        if self.recommendation_engine is None:
            return []
        return self.recommendation_engine.drain_events()

    def flush_recommendations(self) -> None:
        """Force flush of recommendation engine."""
        if self.recommendation_engine is not None:
            self.recommendation_engine.flush()

    def _apply_config(self, config) -> None:
        """Populate engine from a rate-limit config object."""
        if config is None:
            return
        for host_id, host in getattr(config, "hosts", {}).items():
            self.engine.register_bucket(
                host_id,
                getattr(host, "capacity", 100.0),
                getattr(host, "refill_per_sec", 10.0),
            )
        for venue_id, venue in getattr(config, "venues", {}).items():
            self.engine.register_bucket(
                venue_id,
                getattr(venue, "capacity", 50.0),
                getattr(venue, "refill_per_sec", 5.0),
            )


# Global singleton helpers -----------------------------------------------

_global_runtime: RateLimitRuntime | None = None


def install_global_rate_limit_runtime(rt: RateLimitRuntime) -> None:
    global _global_runtime
    _global_runtime = rt


def global_rate_limit_runtime() -> RateLimitRuntime:
    global _global_runtime
    if _global_runtime is None:
        _global_runtime = RateLimitRuntime()
    return _global_runtime
