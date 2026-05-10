"""Rate-limit config: per-venue defaults and TOML hot-reload manager."""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from enum import Enum


class RefreshOutcome(Enum):
    RELOADED = "reloaded"
    UNCHANGED = "unchanged"
    FAILED_BUT_KEPT_OLD = "failed_but_kept_old"


@dataclass
class RateLimitHostConfig:
    """Per-host (IP/domain) rate-limit budget."""

    capacity: float = 100.0
    refill_per_sec: float = 10.0


@dataclass
class RateLimitVenueConfig:
    """Per-venue rate-limit budget."""

    capacity: float = 50.0
    refill_per_sec: float = 5.0


@dataclass
class RateLimitConfig:
    """Top-level rate-limit configuration (mirrors rate_limits.toml)."""

    default_margin: float = 0.95
    refresh_interval_secs: int = 30
    hosts: dict[str, RateLimitHostConfig] = field(default_factory=dict)
    venues: dict[str, RateLimitVenueConfig] = field(default_factory=dict)


def built_in_defaults() -> RateLimitConfig:
    """Return per-venue built-in defaults for all 7 supported exchanges."""
    venue_defaults = {
        "binance": RateLimitVenueConfig(capacity=1200.0, refill_per_sec=20.0),
        "okx": RateLimitVenueConfig(capacity=400.0, refill_per_sec=10.0),
        "bybit": RateLimitVenueConfig(capacity=600.0, refill_per_sec=10.0),
        "bitget": RateLimitVenueConfig(capacity=200.0, refill_per_sec=5.0),
        "gate": RateLimitVenueConfig(capacity=200.0, refill_per_sec=4.0),
        "aster": RateLimitVenueConfig(capacity=100.0, refill_per_sec=3.0),
        "hyperliquid": RateLimitVenueConfig(capacity=1200.0, refill_per_sec=20.0),
    }
    hosts = {
        "binance.com": RateLimitHostConfig(capacity=1200.0, refill_per_sec=20.0),
        "okx.com": RateLimitHostConfig(capacity=400.0, refill_per_sec=10.0),
        "bybit.com": RateLimitHostConfig(capacity=600.0, refill_per_sec=10.0),
        "bitget.com": RateLimitHostConfig(capacity=200.0, refill_per_sec=5.0),
        "gate.io": RateLimitHostConfig(capacity=200.0, refill_per_sec=4.0),
        "hyperliquid.xyz": RateLimitHostConfig(capacity=1200.0, refill_per_sec=20.0),
    }
    return RateLimitConfig(hosts=hosts, venues=venue_defaults)


class RateLimitConfigManager:
    """Manages rate-limit config with disk-backed hot-reload.

    On refresh, reads rate_limits.toml, diffs against the last raw content,
    and rebuilds the config only when changed.
    """

    def __init__(self, config_path: str | None = None) -> None:
        self._path = config_path
        self._last_raw: str = ""
        self._last_hash: str = ""
        self.config: RateLimitConfig = built_in_defaults()

    def refresh(self, now_ms: int | None = None) -> str:
        """Read the file; return RELOADED, UNCHANGED, or FAILED_BUT_KEPT_OLD."""
        if self._path is None:
            return RefreshOutcome.UNCHANGED.value

        try:
            raw = _read_file(self._path)
        except OSError:
            return RefreshOutcome.FAILED_BUT_KEPT_OLD.value

        h = hashlib.sha256(raw.encode()).hexdigest()
        if h == self._last_hash:
            return RefreshOutcome.UNCHANGED.value

        try:
            self.config = _parse_toml_config(raw)
        except Exception:
            return RefreshOutcome.FAILED_BUT_KEPT_OLD.value

        self._last_raw = raw
        self._last_hash = h
        return RefreshOutcome.RELOADED.value

    @property
    def path(self) -> str | None:
        return self._path


def _read_file(path: str) -> str:
    with open(path, "r") as f:
        return f.read()


def _parse_toml_config(raw: str) -> RateLimitConfig:
    """Parse a rate_limits.toml string into RateLimitConfig.

    Tolerates missing sections; falls back to built-in defaults per venue.
    """
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib  # Python < 3.11 fallback

    data = tomllib.loads(raw)
    config = built_in_defaults()

    margin = data.get("global", {}).get("default_margin")
    if isinstance(margin, (int, float)):
        config.default_margin = float(margin)

    refresh = data.get("global", {}).get("refresh_interval_secs")
    if isinstance(refresh, int):
        config.refresh_interval_secs = refresh

    # Merge host overrides
    hosts_raw = data.get("hosts", {})
    if isinstance(hosts_raw, dict):
        for host_id, hdata in hosts_raw.items():
            if isinstance(hdata, dict):
                config.hosts[host_id] = RateLimitHostConfig(
                    capacity=float(hdata.get("capacity", 100.0)),
                    refill_per_sec=float(hdata.get("refill_per_sec", 10.0)),
                )

    # Merge venue overrides
    venues_raw = data.get("venues", {})
    if isinstance(venues_raw, dict):
        for venue_id, vdata in venues_raw.items():
            if isinstance(vdata, dict):
                config.venues[venue_id] = RateLimitVenueConfig(
                    capacity=float(vdata.get("capacity", 50.0)),
                    refill_per_sec=float(vdata.get("refill_per_sec", 5.0)),
                )

    return config
