"""Startup bootstrap: symbol resolution, tick readiness, warmup helpers."""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Optional

from lightfee.config.schema import AppConfig
from lightfee.config.universe import resolve_or_generate_universe_symbols
from lightfee.core.domain import Venue
from lightfee.risk.modes import EngineLifecycle
from lightfee.venues.market_data import MarketDataClient
from lightfee.venues.specs import get_spec
from lightfee.venues.transport import EndpointRateLimiter


logger = logging.getLogger(__name__)


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
    """Resolve runtime trading symbols before startup workers fan out.

    The resolver is the sole owner of daily-universe fallback and capping
    semantics.  Both live and sidecar startup call this boundary before a
    sidecar or runtime worker can fan out over ``config.symbols``.
    """
    async def fetch_liquidity(symbols: list[str]) -> dict[str, dict]:
        clients: dict[str, MarketDataClient] = {}
        for venue_config in config.venues:
            venue = Venue.from_str(venue_config.venue)
            clients[venue.value] = MarketDataClient(
                get_spec(venue),
                exchange_http_timeout_ms=config.runtime.exchange_http_timeout_ms,
                rate_limiter=EndpointRateLimiter(1000, 8000, 50),
            )

        timeout_s = max(float(config.runtime.sidecar_funding_timeout_s), 0.001)

        async def fetch_one(venue_name: str, client: MarketDataClient):
            try:
                rows = await asyncio.wait_for(
                    client.fetch_perp_liquidity(symbols), timeout=timeout_s
                )
                return venue_name, rows
            except Exception as exc:
                logger.warning(
                    "daily_universe: liquidity fetch failed venue=%s error=%s",
                    venue_name,
                    exc,
                )
                return venue_name, None

        try:
            results = await asyncio.gather(
                *(fetch_one(venue_name, client) for venue_name, client in clients.items())
            )
            return {
                venue_name: rows
                for venue_name, rows in results
                if rows is not None
            }
        finally:
            await asyncio.gather(
                *(client.close() for client in clients.values()),
                return_exceptions=True,
            )

    resolved = await resolve_or_generate_universe_symbols(config, fetch_liquidity)
    symbols = list(resolved["resolved_symbols"])
    config.symbols = symbols
    if not symbols:
        return None
    return resolved
