"""Startup bootstrap: symbol resolution, tick readiness, warmup helpers."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Optional

from lightfee.config.schema import AppConfig
from lightfee.risk.modes import EngineLifecycle


def wall_clock_now_ms() -> int:
    """Monotonic wall clock in milliseconds."""
    return int(time.time() * 1000)


def rate_limit_config_path(config_path: str) -> str:
    """Derive rate_limits.toml path sibling to the main config file."""
    p = Path(config_path)
    return str(p.parent / "rate_limits.toml")


def full_tick_ready(backoff_until_ms: Optional[int], now_ms: int) -> bool:
    """True if the full engine tick should fire past its backoff deadline."""
    if backoff_until_ms is None:
        return True
    return now_ms >= backoff_until_ms


def active_position_tick_ready(backoff_until_ms: Optional[int], now_ms: int) -> bool:
    """True if the active-position tick should fire past its backoff deadline."""
    return full_tick_ready(backoff_until_ms, now_ms)


def active_position_poll_interval_ms(
    lifecycle: EngineLifecycle, poll_interval_ms: int, active_position_count: int
) -> int:
    """Fast-poll interval (min 250ms) when live positions are open."""
    if lifecycle == EngineLifecycle.RUNNING and active_position_count > 0:
        return min(poll_interval_ms, 250)
    return poll_interval_ms


def active_position_poll_enabled(
    lifecycle: EngineLifecycle, poll_interval_ms: int, active_position_count: int
) -> bool:
    """True if the fast tick lane should fire (interval reduced from baseline)."""
    fast = active_position_poll_interval_ms(lifecycle, poll_interval_ms, active_position_count)
    return fast < poll_interval_ms


def startup_market_warmup_ms(
    lifecycle: EngineLifecycle,
    market_data_active: bool,
    active_position_count: int,
    poll_interval_ms: int,
) -> Optional[int]:
    """Optional warmup delay (3*poll, clamped [3000,10000]ms) before first tick."""
    if lifecycle != EngineLifecycle.RUNNING:
        return None
    if not market_data_active:
        return None
    if active_position_count > 0:
        return None
    warmup = poll_interval_ms * 3
    return max(3000, min(warmup, 10000))


async def prepare_runtime_symbols(config: AppConfig) -> Optional[dict]:
    """Resolve runtime trading symbols before adapter construction.

    When the daily-universe feature is enabled (Task 23), loads the persisted
    daily universe JSON and mutates config.symbols.  Today this is a passthrough
    that returns the static symbol list.
    """
    symbols = list(config.symbols)
    if not symbols:
        return None

    return {
        "daily_universe_enabled": False,
        "global_symbol_count": len(symbols),
        "resolved_symbol_count": len(symbols),
        "resolved_symbols": symbols,
    }
