"""Token-bucket rate-limit engine matching V1 Rust RateLimitEngine behavior.

V1 architecture (src/rate_limit/mod.rs):
- Scopes are the primary key. Each scope has optional bucket, weight, min_interval.
- try_consume_scopes ONLY touches scopes passed in, never all registered buckets.
- Weight: resolve_weight picks endpoint scope weight with group fallback, or 1.0.
- Min interval: max of min intervals across the given scopes.
- Cooldown/backoff: per-scope, applied to bucket-bearing scopes only.
- Margin: capacity = budget_per_minute * margin (ONCE). refill_per_ms = capacity / window_ms.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
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
    refill_per_ms: float
    tokens: float
    last_refill_ms: int = 0
    cooldown_until_ms: int = 0

    def refill(self, now_ms: int) -> None:
        elapsed_ms = max(0, now_ms - self.last_refill_ms)
        if elapsed_ms <= 0:
            return
        replenished = float(elapsed_ms) * self.refill_per_ms
        self.tokens = min(self.capacity, self.tokens + replenished)
        self.last_refill_ms = now_ms


@dataclass
class ScopeState:
    """Per-scope state: optional bucket, weight, min_interval, last_request_at_ms."""

    bucket: Optional[BucketState] = None
    weight: Optional[float] = None
    min_interval_ms: Optional[int] = None
    last_request_at_ms: Optional[int] = None
    backoff_failures: int = 0


# V1 constants from mod.rs
DEFAULT_BACKOFF_INITIAL_MS = 1_000
DEFAULT_BACKOFF_MAX_MS = 8_000
DEFAULT_BUDGET_RETRY_MS = 50


class RateLimitEngine:
    """Token-bucket engine matching V1 RateLimitEngine semantics exactly.

    V1 reference: src/rate_limit/mod.rs RateLimitEngine (lines 84-297)
    """

    def __init__(
        self,
        window_secs: int = 60,
        margin: float = 0.95,
        *,
        default_margin: float | None = None,
    ) -> None:
        self._window_ms = float(max(window_secs, 1)) * 1_000.0
        self._margin = default_margin if default_margin is not None else margin
        self._scopes: dict[str, ScopeState] = {}
        self._backoff_initial_ms = DEFAULT_BACKOFF_INITIAL_MS
        self._backoff_max_ms = DEFAULT_BACKOFF_MAX_MS

    # ------------------------------------------------------------------
    # Registration (V1: register_bucket, register_weight, register_min_interval)
    # ------------------------------------------------------------------

    def register_bucket(
        self,
        scope: str,
        budget_per_minute: float | None = None,
        *,
        capacity: float | None = None,
        refill_per_sec: float | None = None,
    ) -> None:
        """V1: capacity = budget * margin. refill_per_ms = capacity / window_ms.

        Legacy compat: register_bucket(scope, capacity=X, refill_per_sec=Y)
        sets bucket capacity/refill directly without margin.
        """
        if budget_per_minute is not None:
            cap = max(budget_per_minute * self._margin, 0.0)
            rps = cap / self._window_ms if self._window_ms > 0.0 else 0.0
        elif capacity is not None:
            cap = float(capacity)
            rps = float(refill_per_sec or 0.0) / 1000.0
        else:
            cap = 0.0
            rps = 0.0

        bucket = BucketState(capacity=cap, refill_per_ms=rps, tokens=cap)
        self._scope_mut(scope).bucket = bucket

    def register_weight(self, scope: str, weight_or_scope: float | str | None = None, weight_val: float | None = None) -> None:
        """V1: register_weight(scope, weight).

        Legacy compat: register_weight(bucket_id, scope, weight) — bucket_id ignored.
        """
        if weight_val is not None:
            # Legacy: register_weight(bucket_id, scope_str, weight)
            self._scope_mut(str(weight_or_scope)).weight = float(weight_val)
        elif weight_or_scope is not None:
            if isinstance(weight_or_scope, (int, float)):
                # V1: register_weight(scope, weight_number)
                self._scope_mut(scope).weight = float(weight_or_scope)
            else:
                # Legacy: register_weight(bucket_id, scope_str) — weight defaults to 1.0
                self._scope_mut(str(weight_or_scope)).weight = 1.0

    def register_min_interval(
        self,
        scope_or_bucket: str,
        scope_or_interval: str | int | None = None,
        interval_ms: int | None = None,
    ) -> None:
        """V1: register_min_interval(scope, interval_ms).

        Legacy compat: register_min_interval(bucket_id, scope, ms) — bucket_id ignored.
        """
        if interval_ms is not None:
            self._scope_mut(str(scope_or_interval)).min_interval_ms = interval_ms
        elif isinstance(scope_or_interval, int):
            self._scope_mut(scope_or_bucket).min_interval_ms = scope_or_interval
        elif scope_or_interval is not None:
            self._scope_mut(str(scope_or_interval)).min_interval_ms = 0

    # ------------------------------------------------------------------
    # Weight / min_interval resolution (V1: resolve_weight, resolve_min_interval)
    # ------------------------------------------------------------------

    def resolve_weight(self, endpoint_scope: str, group_scope: Optional[str] = None) -> float:
        """V1: pick endpoint weight, fall back to group weight, default 1.0."""
        state = self._scopes.get(endpoint_scope)
        if state is not None and state.weight is not None:
            return state.weight
        if group_scope is not None:
            gs = self._scopes.get(group_scope)
            if gs is not None and gs.weight is not None:
                return gs.weight
        return 1.0

    def resolve_min_interval(self, scopes: list[str]) -> int:
        """V1: max of min intervals across given scopes."""
        max_interval = 0
        for scope in scopes:
            state = self._scopes.get(scope)
            if state is not None and state.min_interval_ms is not None:
                max_interval = max(max_interval, state.min_interval_ms)
        return max_interval

    # ------------------------------------------------------------------
    # Consumption (V1: try_consume, try_consume_scopes)
    # ------------------------------------------------------------------

    def try_consume(
        self,
        scope_or_bucket: str,
        scopes_or_weight: list[str] | float = 1.0,
        now_ms: int | None = None,
    ) -> None:
        """V1 try_consume(scope, weight, now_ms).

        Legacy compat: try_consume(bucket_id, [scopes], now_ms=X).
        When scopes list is passed, resolves weight from scopes using V1
        resolve_weight (endpoint with group fallback), rather than sum.
        """
        if isinstance(scopes_or_weight, list):
            # Legacy: try_consume(bucket_id, scopes_list, now_ms=X)
            # Build full scope list, bucket_id is also a scope (host/venue/etc)
            all_scopes = [scope_or_bucket] + [
                s for s in scopes_or_weight if s != scope_or_bucket
            ]
            # V1: resolve weight from scopes (endpoint with group fallback)
            weight = self._resolve_weight_from_scopes(all_scopes)
            if now_ms is None:
                now_ms = int(time.time() * 1000)
        else:
            # V1: try_consume(scope, weight, now_ms)
            all_scopes = [scope_or_bucket]
            weight = float(scopes_or_weight)
        self.try_consume_scopes(all_scopes, weight, now_ms)

    def _resolve_weight_from_scopes(self, scopes: list[str]) -> float:
        """V1: resolve_request_weight — endpoint scope with group fallback."""
        endpoint_scope = "request"
        for s in scopes:
            if not (s.startswith("host:") or s.startswith("venue:") or s.startswith("group:")):
                endpoint_scope = s
                break
        group_scope = None
        for s in scopes:
            if s.startswith("group:") and s.count(":") >= 2:
                group_scope = s
                break
        if group_scope is None:
            for s in scopes:
                if s.startswith("group:"):
                    group_scope = s
                    break
        return self.resolve_weight(endpoint_scope, group_scope)

    def try_consume_scopes(
        self,
        scopes: list[str],
        weight: float = 1.0,
        now_ms: int | None = None,
    ) -> None:
        """V1: check cooldown -> min_interval -> budget for GIVEN scopes only.

        Raises RateLimitError on failure.
        Scopes without registered buckets are skipped (pass-through).
        """
        if now_ms is None:
            now_ms = int(time.time() * 1000)

        # Phase 1: cooldown check
        retry_ms = self._cooldown_retry_ms(scopes, now_ms)
        if retry_ms is not None:
            raise RateLimitError(RateLimitErrorReason.COOLDOWN, retry_in_ms=retry_ms)

        # Phase 2: min_interval check
        retry_ms = self._min_interval_retry_ms(scopes, now_ms)
        if retry_ms is not None:
            raise RateLimitError(RateLimitErrorReason.MIN_INTERVAL, retry_in_ms=retry_ms)

        # Phase 3: refill + budget check for scopes with buckets
        for scope in scopes:
            state = self._scopes.get(scope)
            if state is None or state.bucket is None:
                continue
            bucket = state.bucket
            bucket.refill(now_ms)
            if bucket.tokens + 1e-9 < weight:
                raise RateLimitError(
                    RateLimitErrorReason.BUDGET_EXCEEDED,
                    retry_in_ms=self._budget_retry_ms(bucket, weight),
                )

        # Phase 4: deduct tokens + record timestamps
        for scope in scopes:
            state = self._scope_mut(scope)
            if state.bucket is not None:
                state.bucket.tokens = max(0.0, state.bucket.tokens - weight)
            if state.min_interval_ms is not None:
                state.last_request_at_ms = now_ms

    # ------------------------------------------------------------------
    # Cooldown / Backoff (V1: apply_cooldown, apply_backoff)
    # ------------------------------------------------------------------

    def apply_cooldown(
        self,
        scopes_or_bucket: str | list[str],
        retry_after_ms: int = 0,
        now_ms: int | None = None,
    ) -> None:
        """V1: set cooldown_until_ms = max(existing, now + retry_after) for scopes with buckets.

        Legacy compat: apply_cooldown(bucket_id, duration_ms, now_ms) for single bucket.
        """
        if isinstance(scopes_or_bucket, str):
            scopes = [scopes_or_bucket]
        else:
            scopes = scopes_or_bucket

        if now_ms is None:
            now_ms = int(time.time() * 1000)
        until_ms = now_ms + retry_after_ms
        for scope in scopes:
            state = self._scope_mut(scope)
            if state.bucket is not None:
                state.bucket.cooldown_until_ms = max(state.bucket.cooldown_until_ms, until_ms)

    def apply_backoff(
        self,
        scopes_or_bucket: str | list[str],
        maybe_now_ms: int | None = None,
        now_ms: int | None = None,
    ) -> int:
        """V1: exponential backoff per scope. Returns max delay_ms.

        Legacy compat: apply_backoff(bucket_id, duration_ms, now_ms) — duration_ms ignored.
        """
        if isinstance(scopes_or_bucket, str):
            scopes = [scopes_or_bucket]
        else:
            scopes = scopes_or_bucket

        if now_ms is None:
            now_ms = maybe_now_ms or int(time.time() * 1000)

        max_delay_ms = 0
        for scope in scopes:
            state = self._scope_mut(scope)
            if state.bucket is not None:
                delay_ms = self._failure_backoff_ms(state.backoff_failures)
                state.backoff_failures += 1
                state.bucket.cooldown_until_ms = max(
                    state.bucket.cooldown_until_ms, now_ms + delay_ms
                )
                max_delay_ms = max(max_delay_ms, delay_ms)
        return max_delay_ms

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def bucket(self, scope: str) -> Optional[dict]:
        """V1: bucket(scope) -> BucketSnapshot."""
        state = self._scopes.get(scope)
        if state is None or state.bucket is None:
            return None
        b = state.bucket
        return {
            "capacity": b.capacity,
            "refill_per_ms": b.refill_per_ms,
            "tokens": b.tokens,
            "cooldown_until_ms": b.cooldown_until_ms,
        }

    def bucket_snapshot(self, scope: str) -> Optional[dict]:
        """Alias for bucket()."""
        return self.bucket(scope)

    @property
    def bucket_ids(self) -> list[str]:
        return [s for s, st in self._scopes.items() if st.bucket is not None]

    @property
    def scope_count(self) -> int:
        return len(self._scopes)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _scope_mut(self, scope: str) -> ScopeState:
        if scope not in self._scopes:
            self._scopes[scope] = ScopeState()
        return self._scopes[scope]

    def _cooldown_retry_ms(self, scopes: list[str], now_ms: int) -> Optional[int]:
        max_remaining = 0
        for scope in scopes:
            state = self._scopes.get(scope)
            if state is None or state.bucket is None:
                continue
            remaining = state.bucket.cooldown_until_ms - now_ms
            if remaining > 0:
                max_remaining = max(max_remaining, remaining)
        return max_remaining if max_remaining > 0 else None

    def _min_interval_retry_ms(self, scopes: list[str], now_ms: int) -> Optional[int]:
        max_retry = 0
        for scope in scopes:
            state = self._scopes.get(scope)
            if state is None or state.min_interval_ms is None or state.last_request_at_ms is None:
                continue
            elapsed = max(0, now_ms - state.last_request_at_ms)
            if elapsed < state.min_interval_ms:
                retry = state.min_interval_ms - elapsed
                max_retry = max(max_retry, retry)
        return max_retry if max_retry > 0 else None

    def _budget_retry_ms(self, bucket: BucketState, weight: float) -> int:
        if bucket.refill_per_ms <= 0.0:
            return DEFAULT_BUDGET_RETRY_MS
        missing = max(weight - bucket.tokens, 0.0)
        if missing < 1e-9:
            return DEFAULT_BUDGET_RETRY_MS
        retry = int(missing / bucket.refill_per_ms)
        if missing % bucket.refill_per_ms > 1e-9:
            retry += 1
        return max(retry, 1)

    def _failure_backoff_ms(self, failures: int) -> int:
        shift = min(failures, 20)
        return min(self._backoff_initial_ms << shift, self._backoff_max_ms)

    # ------------------------------------------------------------------
    # V1 alias methods (for RED tests and explicit V1-parity code)
    # ------------------------------------------------------------------

    def register_bucket_v1(self, scope: str, budget_per_minute: float) -> None:
        """V1 alias: register_bucket(scope, budget_per_minute)."""
        self.register_bucket(scope, budget_per_minute=budget_per_minute)

    def register_weight_v1(self, scope: str, weight: float) -> None:
        """V1 alias: register_weight(scope, weight)."""
        self.register_weight(scope, weight)

    def register_min_interval_v1(self, scope: str, min_interval_ms: int) -> None:
        """V1 alias: register_min_interval(scope, min_interval_ms)."""
        self.register_min_interval(scope, min_interval_ms)

    def try_consume_scopes_v1(
        self, scopes: list[str], weight: float = 1.0, now_ms: int | None = None
    ) -> None:
        """V1 alias: try_consume_scopes(scopes, weight, now_ms)."""
        self.try_consume_scopes(scopes, weight, now_ms)

    def apply_cooldown_v1(
        self, scopes: list[str], retry_after_ms: int = 0, now_ms: int | None = None
    ) -> None:
        """V1 alias: apply_cooldown(scopes, retry_after_ms, now_ms)."""
        self.apply_cooldown(scopes, retry_after_ms, now_ms)

    def apply_backoff_v1(
        self, scopes: list[str], now_ms: int | None = None
    ) -> int:
        """V1 alias: apply_backoff(scopes, now_ms)."""
        return self.apply_backoff(scopes, now_ms)


# ============================================================================
# RateLimitRuntime — high-level wrapper matching V1 RateLimitRuntime
# ============================================================================


class RateLimitRuntime:
    """V1-aligned rate-limit runtime (src/rate_limit/mod.rs RateLimitRuntime).

    On construction: immediately builds engine from config (V1: RateLimitRuntime::new).
    record_rate_limit_for_scopes: calls engine.apply_cooldown or engine.apply_backoff.
    """

    def __init__(
        self,
        engine: RateLimitEngine | None = None,
        config_manager=None,
        recommendation_engine=None,
    ) -> None:
        self.config_manager = config_manager
        self.recommendation_engine = recommendation_engine

        if engine is not None:
            self.engine = engine
        else:
            # V1: RateLimitRuntime::new immediately builds engine from current config
            margin = 0.95
            if config_manager is not None and config_manager.config is not None:
                cfg = config_manager.config
                if hasattr(cfg, "global_config"):
                    margin = cfg.global_config.default_margin
                self.engine = self._build_engine_from_config(cfg)
            else:
                self.engine = RateLimitEngine(window_secs=60, margin=margin)

    async def refresh(self, now_ms: int | None = None) -> None:
        """Hot-reload rate-limit config from disk if changed (V1: RateLimitRuntime::refresh)."""
        if self.config_manager is None:
            return
        if now_ms is None:
            now_ms = int(time.time() * 1000)
        outcome = self.config_manager.refresh(now_ms)
        if outcome == "reloaded":
            self.engine = self._build_engine_from_config(self.config_manager.config)

    def refresh_interval_secs(self) -> int:
        if self.config_manager is not None and self.config_manager.config is not None:
            return self.config_manager.config.global_config.refresh_interval_secs
        return 30

    def wait_until_ready_for_scopes(
        self,
        scopes: list[str],
        timeout_ms: int = 0,
        *,
        weight_override: float | None = None,
    ) -> bool:
        """Block until scopes are available (V1: wait_until_ready_for_scopes)."""
        deadline = int(time.time() * 1000) + timeout_ms
        while True:
            now_ms = int(time.time() * 1000)
            try:
                weight = (
                    float(weight_override)
                    if weight_override is not None
                    else self._resolve_request_weight(scopes)
                )
                self.engine.try_consume_scopes(scopes, weight, now_ms)
                return True
            except RateLimitError as e:
                if timeout_ms > 0 and now_ms + e.retry_in_ms > deadline:
                    return False
                time.sleep(max(0.0, e.retry_in_ms / 1000.0))

    async def async_wait_until_ready_for_scopes(
        self,
        scopes: list[str],
        timeout_ms: int = 0,
        *,
        weight_override: float | None = None,
    ) -> bool:
        """Async version: block until scopes are available."""
        import asyncio
        now_ms = int(time.time() * 1000)
        deadline = now_ms + timeout_ms
        while True:
            now_ms = int(time.time() * 1000)
            try:
                weight = (
                    float(weight_override)
                    if weight_override is not None
                    else self._resolve_request_weight(scopes)
                )
                self.engine.try_consume_scopes(scopes, weight, now_ms)
                return True
            except RateLimitError as e:
                if timeout_ms > 0 and now_ms + e.retry_in_ms > deadline:
                    return False
                await asyncio.sleep(max(0.0, e.retry_in_ms / 1000.0))

    def record_rate_limit_for_scopes(
        self, scopes: list[str], retry_after_ms: int = 0
    ) -> int:
        """V1: apply cooldown or backoff on engine. Returns delay ms."""
        now_ms = int(time.time() * 1000)
        if retry_after_ms and retry_after_ms > 0:
            self.engine.apply_cooldown(scopes, retry_after_ms, now_ms)
            cooldown_ms = retry_after_ms
        else:
            cooldown_ms = self.engine.apply_backoff(scopes, now_ms)

        if self.recommendation_engine is not None:
            for scope in scopes:
                self.recommendation_engine.record_limit_hit(scope, retry_after_ms or cooldown_ms)

        return cooldown_ms

    def drain_journal_events(self) -> list[dict]:
        if self.recommendation_engine is None:
            return []
        return self.recommendation_engine.drain_events()

    def flush_recommendations(self) -> None:
        if self.recommendation_engine is not None:
            self.recommendation_engine.flush()

    # ------------------------------------------------------------------
    # Engine builder (V1: build_engine_from_config)
    # ------------------------------------------------------------------

    @staticmethod
    def _build_engine_from_config(config) -> RateLimitEngine:
        """V1: build_engine_from_config (rate_limit/mod.rs:529-580)."""
        if config is None:
            return RateLimitEngine()

        margin = getattr(config, "default_margin", 0.95)
        if hasattr(config, "global_config"):
            margin = config.global_config.default_margin

        engine = RateLimitEngine(window_secs=60, margin=margin)

        # Hosts
        for host_id, host in getattr(config, "hosts", {}).items():
            budget = getattr(host, "budget_per_minute", None)
            min_interval = getattr(host, "min_interval_ms", None)
            if budget is None:
                cap = getattr(host, "capacity", None)
                if cap is not None:
                    budget = int(cap)
                else:
                    continue
            host_scope = f"host:{host_id}"
            engine.register_bucket(host_scope, float(budget))
            if min_interval is not None:
                engine.register_min_interval(host_scope, min_interval)

        # Venues
        for venue_id, venue in getattr(config, "venues", {}).items():
            budget = getattr(venue, "budget_per_minute", None)
            docs = getattr(venue, "docs_fallback", None)
            docs_budget = docs.budget_per_minute if docs else None
            effective_budget = budget or docs_budget
            if effective_budget is None:
                continue

            venue_scope = f"venue:{venue_id}"
            engine.register_bucket(venue_scope, float(effective_budget))

            min_interval = getattr(venue, "min_interval_ms", None)
            docs_min = docs.min_interval_ms if docs else None
            effective_min = min_interval or docs_min
            if effective_min is not None:
                engine.register_min_interval(venue_scope, effective_min)

            # Endpoint weights
            for endpoint, weight in (getattr(venue, "endpoint_weights", {}) or {}).items():
                engine.register_weight(endpoint, float(weight))

            # Group weights: register as group:<group> AND group:<venue>:<group>
            for group, gw in (getattr(venue, "group_weights", {}) or {}).items():
                engine.register_weight(f"group:{group}", float(gw))
                engine.register_weight(f"group:{venue_id}:{group}", float(gw))

            # Endpoint min intervals
            for endpoint, em in (getattr(venue, "endpoint_min_interval_ms", {}) or {}).items():
                engine.register_min_interval(endpoint, em)

            # Group min intervals
            for group, gm in (getattr(venue, "group_min_interval_ms", {}) or {}).items():
                engine.register_min_interval(f"group:{group}", gm)
                engine.register_min_interval(f"group:{venue_id}:{group}", gm)

            # WS budget buckets
            ws_budget = getattr(venue, "ws_budget_per_minute", None)
            if ws_budget is not None and ws_budget > 0:
                for ws_group in ("ws_public", "ws_private"):
                    engine.register_bucket(f"group:{venue_id}:{ws_group}", float(ws_budget))

        return engine

    def _resolve_request_weight(self, scopes: list[str]) -> float:
        """V1: resolve_request_weight (rate_limit/mod.rs:676-692)."""
        # Find endpoint scope (first non-host, non-venue, non-group)
        endpoint_scope = "request"
        for s in scopes:
            if not (s.startswith("host:") or s.startswith("venue:") or s.startswith("group:")):
                endpoint_scope = s
                break

        # Find group scope (prefer venue-specific, then generic)
        group_scope = None
        for s in scopes:
            if s.startswith("group:") and s.count(":") >= 2:
                group_scope = s
                break
        if group_scope is None:
            for s in scopes:
                if s.startswith("group:"):
                    group_scope = s
                    break

        return self.engine.resolve_weight(endpoint_scope, group_scope)


# ---------------------------------------------------------------------------
# Global singleton helpers
# ---------------------------------------------------------------------------

_global_runtime: RateLimitRuntime | None = None


def install_global_rate_limit_runtime(rt: RateLimitRuntime) -> None:
    global _global_runtime
    _global_runtime = rt


def global_rate_limit_runtime() -> RateLimitRuntime | None:
    global _global_runtime
    return _global_runtime
