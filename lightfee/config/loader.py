"""TOML configuration loader for Python LightFee."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any, Optional

from lightfee.config.defaults import (
    default_passive_maker,
    default_persistence,
    default_runtime,
    default_strategy,
    default_trade_credentials,
    default_venue_live,
)
from lightfee.config.schema import (
    AppConfig,
    PersistenceConfig,
    RuntimeConfig,
    StrategyConfig,
    TradeCredentials,
    VenueConfig,
    VenueLiveConfig,
    VenuePassiveMakerConfig,
)
from lightfee.config.validation import check_raw_toml_for_chillybot, validate_config
from lightfee.core.errors import ConfigError


def load_config(path: str | Path) -> AppConfig:
    """Load and validate a TOML config file. Raises ConfigError on failure."""
    raw = _read_toml(path)

    if "daily_universe" in raw:
        raise ConfigError(
            "daily_universe must be declared under [runtime.daily_universe]"
        )

    # Check for removed Chillybot fields first
    chillybot_errors = check_raw_toml_for_chillybot(raw)
    if chillybot_errors:
        raise ConfigError("\n".join(chillybot_errors))

    symbols = raw.get("symbols", [])
    if isinstance(symbols, str):
        symbols = [s.strip() for s in symbols.split(",") if s.strip()]
    elif not isinstance(symbols, list):
        symbols = []

    runtime = _load_runtime(raw.get("runtime", {}))
    strategy = _load_strategy(raw.get("strategy", {}), runtime=runtime)
    persistence = _load_persistence(raw.get("persistence", {}))
    venues = _load_venues(raw.get("venues", []))

    config = AppConfig(
        symbols=symbols,
        runtime=runtime,
        strategy=strategy,
        persistence=persistence,
        venues=venues,
    )

    issues = validate_config(config)
    if issues:
        raise ConfigError("config validation failed:\n" + "\n".join(f"  - {i}" for i in issues))

    return config


def _read_toml(path: str | Path) -> dict[str, Any]:
    with open(path, "rb") as f:
        return tomllib.load(f)


def _merge_defaults(base: Any, raw: dict[str, Any]) -> None:
    """Merge raw dict values into a dataclass instance in-place."""
    for key, value in raw.items():
        if hasattr(base, key):
            setattr(base, key, value)


def _load_runtime(raw: dict[str, Any]) -> RuntimeConfig:
    cfg = default_runtime()
    raw = dict(raw)
    daily_universe_raw = raw.pop("daily_universe", {})
    if not isinstance(daily_universe_raw, dict):
        raise ConfigError("runtime.daily_universe must be a TOML table")
    _merge_defaults(cfg, raw)
    _merge_defaults(cfg.daily_universe, daily_universe_raw)
    return cfg


def _load_strategy(
    raw: dict[str, Any],
    *,
    runtime: RuntimeConfig | None = None,
) -> StrategyConfig:
    cfg = default_strategy()
    raw = _normalize_entry_perp_liquidity_thresholds(raw)
    _merge_defaults(cfg, raw)
    return cfg


def _normalize_entry_perp_liquidity_thresholds(raw: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(raw)

    if (
        "entry_open_interest_floor_quote" in normalized
        and "entry_open_interest_floor_default_quote" not in normalized
    ):
        normalized["entry_open_interest_floor_default_quote"] = normalized[
            "entry_open_interest_floor_quote"
        ]
    normalized.pop("entry_open_interest_floor_quote", None)
    if (
        "entry_min_perp_open_interest_quote" in normalized
        and "entry_open_interest_floor_default_quote" not in normalized
    ):
        normalized["entry_open_interest_floor_default_quote"] = normalized[
            "entry_min_perp_open_interest_quote"
        ]
    normalized.pop("entry_min_perp_open_interest_quote", None)

    raw_volume_by_venue = normalized.get("entry_volume_floor_quote_by_venue", {})
    volume_by_venue = (
        dict(raw_volume_by_venue)
        if isinstance(raw_volume_by_venue, dict)
        else {}
    )
    for venue in (
        "gate",
        "aster",
        "hyperliquid",
        "bitget",
        "bybit",
        "binance",
        "okx",
    ):
        legacy_key = f"entry_min_perp_volume_24h_quote_{venue}"
        if legacy_key in normalized:
            volume_by_venue[venue] = normalized[legacy_key]
    if volume_by_venue:
        normalized["entry_volume_floor_quote_by_venue"] = volume_by_venue

    return normalized


def _load_persistence(raw: dict[str, Any]) -> PersistenceConfig:
    cfg = default_persistence()
    _merge_defaults(cfg, raw)
    return cfg


def _load_venues(raw: list[dict[str, Any]]) -> list[VenueConfig]:
    venues: list[VenueConfig] = []
    for entry in raw:
        vc = VenueConfig()
        _merge_defaults(vc, entry)

        live_raw = entry.get("live", {})
        if live_raw:
            vc.live = _load_venue_live(live_raw)

        venues.append(vc)
    return venues


def _load_venue_live(raw: dict[str, Any]) -> VenueLiveConfig:
    cfg = default_venue_live()

    creds_raw = raw.get("trade_credentials", {})
    if creds_raw:
        tc = default_trade_credentials()
        _merge_defaults(tc, creds_raw)
        cfg.trade_credentials = tc

    pm_raw = raw.get("passive_maker", {})
    if pm_raw:
        pm = default_passive_maker()
        _merge_defaults(pm, pm_raw)
        cfg.passive_maker = pm

    for key in ("is_testnet", "okx_passive_px_amend_type"):
        if key in raw:
            setattr(cfg, key, raw[key])

    return cfg
