"""Time utilities for LightFee runtime."""

from __future__ import annotations

import time


def now_ms() -> int:
    """Current wall-clock time in milliseconds since Unix epoch."""
    return int(time.time() * 1000)


def age_ms(timestamp_ms: int, wall_clock_now_ms: int | None = None) -> int:
    """Age of a timestamp in milliseconds."""
    ref = wall_clock_now_ms if wall_clock_now_ms is not None else now_ms()
    return max(0, ref - timestamp_ms)


def is_stale(timestamp_ms: int, max_age_ms: int, wall_clock_now_ms: int | None = None) -> bool:
    """Check if a timestamp is older than max_age_ms."""
    return age_ms(timestamp_ms, wall_clock_now_ms) > max_age_ms
