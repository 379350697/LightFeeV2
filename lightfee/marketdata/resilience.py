"""V1 resilience: FailureBackoff, ConnectionHealth, and HTTP rate-limit helpers.

Rust references:
- src/resilience.rs: FailureBackoff (line 17), ConnectionHealth (line 326)
- src/resilience.rs: failure_backoff_delay_ms (line 371), jitter_delay_ms (line 359)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Failure backoff with jitter
# ---------------------------------------------------------------------------


@dataclass
class FailureBackoff:
    """V1 FailureBackoff (resilience.rs line 17): exponential backoff with jitter.

    Delay doubles each failure up to max_ms. on_success() resets counter.
    on_failure_with_jitter() adds ±20% jitter.
    """
    initial_ms: int
    max_ms: int
    failures: int = 0
    jitter_salt: int = 0x55AA

    def __post_init__(self) -> None:
        self.initial_ms = max(self.initial_ms, 1)
        self.max_ms = max(self.max_ms, self.initial_ms)

    def on_failure(self) -> int:
        shift = min(self.failures, 20)
        multiplier = 1 << shift
        delay = min(self.initial_ms * multiplier, self.max_ms)
        self.failures += 1
        return delay

    def on_failure_with_jitter(self) -> int:
        base = self.on_failure()
        return _jitter_delay(base, self.failures, self.jitter_salt)

    def on_success(self) -> None:
        self.failures = 0


def _jitter_delay(base_ms: int, failures: int, salt: int) -> int:
    if base_ms <= 1:
        return base_ms
    spread = max(base_ms // 5, 1)
    mix = (salt * 0x9E3779B97F4A7C15) + (failures * 0xA0761D6478BD642F)
    offset = mix % (spread * 2 + 1)
    return max(base_ms - spread + offset, 1)


def backoff_delay(initial_ms: int, max_ms: int, failures: int) -> int:
    shift = min(failures, 20)
    return min(initial_ms * (1 << shift), max_ms)


# ---------------------------------------------------------------------------
# Connection health
# ---------------------------------------------------------------------------


@dataclass
class ConnectionHealth:
    """V1 ConnectionHealth (resilience.rs line 326): track connection health.

    Becomes unhealthy after consecutive_failures >= unhealthy_after_failures.
    record_success() clears unhealthy state.
    """
    consecutive_failures: int = 0
    last_success_ms: Optional[int] = None
    last_failure_ms: Optional[int] = None
    unhealthy_since_ms: Optional[int] = None
    last_error: Optional[str] = None

    def record_success(self, now_ms: int) -> None:
        self.consecutive_failures = 0
        self.last_success_ms = now_ms
        self.unhealthy_since_ms = None
        self.last_error = None

    def record_failure(
        self, now_ms: int, unhealthy_after_failures: int, error: str
    ) -> None:
        self.consecutive_failures += 1
        self.last_failure_ms = now_ms
        self.last_error = error
        if (
            unhealthy_after_failures > 0
            and self.consecutive_failures >= unhealthy_after_failures
            and self.unhealthy_since_ms is None
        ):
            self.unhealthy_since_ms = now_ms

    def is_unhealthy(self) -> bool:
        return self.unhealthy_since_ms is not None


# ---------------------------------------------------------------------------
# HTTP rate-limit helpers
# ---------------------------------------------------------------------------


def is_rate_limited_status(http_status: int) -> bool:
    """V1 is_rate_limited_status (resilience.rs line 385): 429 or 418."""
    return http_status in (429, 418)
