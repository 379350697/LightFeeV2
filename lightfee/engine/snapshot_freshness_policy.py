"""Shared sidecar freshness budgets used by entry and production health."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _field(value: Any, name: str, default: int = 0) -> int:
    raw = value.get(name, default) if isinstance(value, Mapping) else getattr(value, name, default)
    try:
        return int(raw or 0)
    except (TypeError, ValueError, OverflowError):
        return default


def snapshot_domain_budget_ms(config: Any, domain: str, row: Any = None) -> int:
    """Return the V1-compatible source-age limit for a snapshot domain.

    Keep this policy independent of runtime ownership so deployment health uses
    exactly the same configured limits as the entry path.
    """
    runtime = config.runtime
    strategy = config.strategy
    domain_s = str(domain or "").lower()
    if domain_s == "liquidity":
        configured_ms = _field(
            runtime,
            "sidecar_perp_liquidity_budget_ms",
            _field(strategy, "max_liquidity_snapshot_age_ms"),
        )
        refresh_ms = _field(runtime, "sidecar_refresh_ms")
        timeout_ms = int(
            float(getattr(runtime, "sidecar_liquidity_timeout_s", 10.0) or 0.0)
            * 1000.0
        )
        publish_interval_ms = _field(row, "publish_interval_ms")
        return max(
            configured_ms,
            _field(strategy, "max_liquidity_snapshot_age_ms"),
            refresh_ms * 3 if refresh_ms > 0 else 0,
            refresh_ms + timeout_ms * 2 if timeout_ms > 0 else 0,
            publish_interval_ms * 2 if publish_interval_ms > 0 else 0,
            30_000,
        )
    if domain_s == "quote":
        return (
            _field(runtime, "max_order_quote_age_ms")
            or _field(runtime, "max_market_age_ms")
            or _field(runtime, "sidecar_snapshot_max_age_ms")
        )
    return _field(runtime, "sidecar_snapshot_max_age_ms")
