"""Recommendation engine: sliding-window rate-limit observation and suggestion."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class RateLimitRequestObserved:
    """Recorded when a request completes (success or failure)."""

    venue: str
    endpoint: str
    weight: float = 1.0
    observed_at_ms: int = 0


@dataclass
class RateLimitLimitHit:
    """Recorded when a 429 / rate-limit error is received."""

    venue: str
    endpoint: str
    status_code: int = 429
    retry_after_ms: int = 0
    weight: float = 1.0
    observed_at_ms: int = 0


@dataclass
class RateLimitRecommendation:
    """Periodically flushed suggestion derived from the observation window."""

    venue: str
    endpoint: str
    observed_rate_per_min: float = 0.0
    limit_hit_rate: float = 0.0
    suggested_budget_per_min: float = 0.0
    suggested_weight: float = 1.0
    window_start_ms: int = 0
    window_end_ms: int = 0


@dataclass
class _EndpointBucket:
    """Internal accumulator for one (venue, endpoint) pair."""

    requests: list[RateLimitRequestObserved] = field(default_factory=list)
    limit_hits: list[RateLimitLimitHit] = field(default_factory=list)


class RecommendationEngine:
    """Sliding-window recommendation engine.

    Accumulates per-endpoint request observations and limit-hit events,
    then periodically flushes RateLimitRecommendation records.
    """

    def __init__(self, window_ms: int = 6 * 3600 * 1000) -> None:
        self._window_ms = window_ms
        self._buckets: dict[tuple[str, str], _EndpointBucket] = {}
        self._events: list[dict] = []
        self._window_start_ms: int | None = None

    # -- Record --------------------------------------------------------

    def record_request(
        self, venue: str, endpoint: str, weight: float = 1.0
    ) -> None:
        """Record an observed request for the sliding window."""
        self._ensure_window_started()
        key = (venue, endpoint)
        bucket = self._buckets.setdefault(key, _EndpointBucket())
        bucket.requests.append(
            RateLimitRequestObserved(
                venue=venue,
                endpoint=endpoint,
                weight=weight,
                observed_at_ms=int(time.time() * 1000),
            )
        )
        self._prune(bucket, int(time.time() * 1000))

    def record_limit_hit(
        self, venue: str, endpoint: str, retry_after_ms: int = 0, weight: float = 1.0
    ) -> None:
        """Record a rate-limit hit (429 or similar)."""
        self._ensure_window_started()
        key = (venue, endpoint)
        bucket = self._buckets.setdefault(key, _EndpointBucket())
        bucket.limit_hits.append(
            RateLimitLimitHit(
                venue=venue,
                endpoint=endpoint,
                retry_after_ms=retry_after_ms,
                weight=weight,
                observed_at_ms=int(time.time() * 1000),
            )
        )

    # -- Flush ---------------------------------------------------------

    def _ensure_window_started(self) -> None:
        """Start the observation window on first record if not yet started."""
        if self._window_start_ms is None:
            self._window_start_ms = int(time.time() * 1000)

    def flush(self, now_ms: int | None = None) -> list[RateLimitRecommendation]:
        """Emit recommendations when the observation window has elapsed."""
        if now_ms is None:
            now_ms = int(time.time() * 1000)

        if self._window_start_ms is None:
            return []

        if now_ms - self._window_start_ms < self._window_ms:
            return []

        window_start = self._window_start_ms
        recs: list[RateLimitRecommendation] = []

        for (venue, endpoint), bucket in self._buckets.items():
            self._prune(bucket, now_ms)
            in_window = [
                r for r in bucket.requests
                if r.observed_at_ms >= window_start
            ]
            hits_in_window = [
                h for h in bucket.limit_hits
                if h.observed_at_ms >= window_start
            ]
            if not in_window:
                continue

            window_minutes = self._window_ms / 60000.0
            observed_rate = len(in_window) / window_minutes
            limit_hit_rate = len(hits_in_window) / max(len(in_window), 1)
            total_weight = sum(r.weight for r in in_window)

            rec = RateLimitRecommendation(
                venue=venue,
                endpoint=endpoint,
                observed_rate_per_min=round(observed_rate, 3),
                limit_hit_rate=round(limit_hit_rate, 4),
                suggested_budget_per_min=round(observed_rate * 1.2, 1),
                suggested_weight=round(total_weight / max(len(in_window), 1), 3),
                window_start_ms=window_start,
                window_end_ms=now_ms,
            )
            recs.append(rec)

        self._buckets.clear()
        self._window_start_ms = now_ms
        return recs

    def drain_events(self) -> list[dict]:
        """Return and clear accumulated journal-style events."""
        events = self._events
        self._events = []
        return events

    # -- Helpers -------------------------------------------------------

    def _prune(self, bucket: _EndpointBucket, now_ms: int) -> None:
        cutoff = now_ms - self._window_ms
        bucket.requests = [r for r in bucket.requests if r.observed_at_ms >= cutoff]
        bucket.limit_hits = [h for h in bucket.limit_hits if h.observed_at_ms >= cutoff]

    @property
    def endpoint_count(self) -> int:
        return len(self._buckets)
