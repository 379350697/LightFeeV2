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
from lightfee.config.paths import (
    remember_config_artifact_root,
    remember_hyperliquid_info_coordinator_dir,
)
from lightfee.config.validation import check_raw_toml_for_chillybot, validate_config
from lightfee.core.errors import ConfigError

_RETIRED_TRANSFER_BIAS_FIELDS = frozenset(
    {
        "transfer_healthy_bias_bps",
        "transfer_unknown_bias_bps",
        "transfer_degraded_bias_bps",
    }
)

_REMOVED_PRODUCTION_FIELDS = {
    "runtime": frozenset(
        {
            "opportunity_input_mode",
            "entry_open_interest_store_path",
            "entry_open_interest_background_refresh_ms",
            "fee_evidence_path",
            "fee_evidence_max_age_ms",
            "fee_evidence_integrity_key_env",
            "fee_evidence_account_identity_hashes",
            "funding_fee_evidence_path",
            "funding_fee_evidence_max_age_ms",
            "spread_sidecar_source_mode",
            "spread_sidecar_direct_fetch_enabled",
            "entry_account_truth_probe_timeout_ms",
        }
    ),
    "strategy": frozenset(
        {
            "funding_canary_enabled",
            "funding_canary_allowed_venues",
            "funding_canary_max_concurrent_positions",
            "funding_canary_max_entry_notional_quote",
            "funding_canary_min_expected_net_edge_bps",
            "funding_canary_min_worst_case_edge_bps",
            "funding_canary_min_expected_net_edge_bps_by_venue_pair",
            "funding_canary_min_worst_case_edge_bps_by_venue_pair",
            "funding_canary_require_account_fee_evidence",
            "funding_canary_conservative_fee_max_entry_notional_quote",
            "funding_canary_conservative_fee_buffer_bps",
            "spread_paper_research_manifest_path",
            "spread_paper_require_account_fee_evidence",
            "spread_allow_verified_maker_rebates",
            "spread_paper_oos_start_ms",
            "spread_paper_require_out_of_sample",
        }
    ),
    "persistence": frozenset({"spread_paper_rollback_anchor_path"}),
}


def load_config(path: str | Path) -> AppConfig:
    """Load and validate a TOML config file. Raises ConfigError on failure."""
    config_path = Path(path).expanduser()
    raw = _read_toml(config_path)

    # Check for removed Chillybot fields first
    chillybot_errors = check_raw_toml_for_chillybot(raw)
    if chillybot_errors:
        raise ConfigError("\n".join(chillybot_errors))
    removed_errors = _removed_production_field_errors(raw)
    if removed_errors:
        raise ConfigError("\n".join(removed_errors))

    symbols = raw.get("symbols", [])
    if isinstance(symbols, str):
        symbols = [s.strip() for s in symbols.split(",") if s.strip()]
    elif not isinstance(symbols, list):
        symbols = []

    runtime = _load_runtime(raw.get("runtime", {}))
    remember_config_artifact_root(runtime, config_path)
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

    remember_hyperliquid_info_coordinator_dir(config)
    return config


def _read_toml(path: str | Path) -> dict[str, Any]:
    with open(path, "rb") as f:
        return tomllib.load(f)


def _removed_production_field_errors(raw: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for section, removed_fields in _REMOVED_PRODUCTION_FIELDS.items():
        values = raw.get(section, {})
        if not isinstance(values, dict):
            continue
        for field_name in sorted(removed_fields.intersection(values)):
            errors.append(
                f"removed production field: {section}.{field_name}; "
                "delete it from the config"
            )
    return errors


def _merge_defaults(base: Any, raw: dict[str, Any]) -> None:
    """Merge raw dict values into a dataclass instance in-place."""
    for key, value in raw.items():
        if hasattr(base, key):
            setattr(base, key, value)


def _load_runtime(raw: dict[str, Any]) -> RuntimeConfig:
    cfg = default_runtime()
    _merge_defaults(cfg, raw)
    return cfg


def _load_strategy(
    raw: dict[str, Any],
    *,
    runtime: RuntimeConfig | None = None,
) -> StrategyConfig:
    cfg = default_strategy()
    provider_configured = "entry_readiness_provider" in raw
    provider_raw = raw.get("entry_readiness_provider", "")
    raw = _normalize_entry_perp_liquidity_thresholds(raw)
    _merge_defaults(cfg, raw)
    # Keep loader provenance out of the TOML schema.  Runtime diagnostics use
    # it to distinguish a defaulted composed mode from an explicit one.
    cfg._entry_readiness_provider_configured = provider_configured
    cfg._entry_readiness_provider_raw = provider_raw if provider_configured else ""
    return cfg


def _normalize_entry_perp_liquidity_thresholds(raw: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(raw)

    # These fields formerly ranked opportunities from a non-querying transfer
    # source. They are accepted only at the parser boundary so an old TOML file
    # cannot reactivate a fake inventory/transfer signal.
    for key in _RETIRED_TRANSFER_BIAS_FIELDS:
        normalized.pop(key, None)

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
