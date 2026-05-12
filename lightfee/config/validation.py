"""Configuration validation with Chillybot removal enforcement."""

from __future__ import annotations

from typing import Any

from lightfee.config.compatibility import REMOVED_FIELD_MESSAGES, VALID_OPPORTUNITY_INPUT_MODES
from lightfee.config.schema import (
    AppConfig,
    _is_valid_generate_time,
)
from lightfee.config.universe import validate_directed_pairs
from lightfee.core.domain import Venue
from lightfee.core.errors import ConfigError


def validate_config(config: AppConfig) -> list[str]:
    """Validate an AppConfig and return list of issues. Empty list means valid."""
    issues: list[str] = []

    if not config.symbols:
        issues.append("symbols list must not be empty")

    if config.runtime.mode not in ("paper", "live"):
        issues.append(f"runtime.mode must be 'paper' or 'live', got: {config.runtime.mode}")

    if config.runtime.opportunity_input_mode not in VALID_OPPORTUNITY_INPUT_MODES:
        issues.append(
            f"runtime.opportunity_input_mode must be one of "
            f"{sorted(VALID_OPPORTUNITY_INPUT_MODES)}, got: {config.runtime.opportunity_input_mode}"
        )

    if config.runtime.maker_event_lane_enabled and config.runtime.maker_event_lane_min_wake_interval_ms <= 0:
        issues.append(
            f"runtime.maker_event_lane_min_wake_interval_ms must be > 0 when maker_event_lane_enabled, "
            f"got: {config.runtime.maker_event_lane_min_wake_interval_ms}"
        )

    if config.strategy.max_concurrent_positions < 0:
        issues.append("strategy.max_concurrent_positions must be >= 0")

    if config.strategy.entry_notional_cap_quote <= 0:
        issues.append("strategy.entry_notional_cap_quote must be > 0")

    # V1 entry planner constraints (Rust: entry_execution_planner.rs:38,108)
    sr = config.strategy.maker_initial_slice_ratio
    if not (0.0 < sr <= 1.0):
        issues.append(
            f"strategy.maker_initial_slice_ratio must be within (0.0, 1.0], got: {sr}"
        )

    cr = config.strategy.entry_max_initial_clip_ratio
    if not (cr > 0.0 and cr == cr):
        issues.append(
            f"strategy.entry_max_initial_clip_ratio must be finite and > 0, got: {cr}"
        )

    if config.strategy.maker_leg_default not in ("buy", "sell"):
        issues.append(
            f"strategy.maker_leg_default must be 'buy' or 'sell', got: {config.strategy.maker_leg_default}"
        )

    # V1 local-L2 resource budget validation
    if config.strategy.local_l2_global_max_books <= 0:
        issues.append(
            f"strategy.local_l2_global_max_books must be > 0, got: {config.strategy.local_l2_global_max_books}"
        )
    if config.strategy.local_l2_max_books_per_venue > config.strategy.local_l2_global_max_books:
        issues.append(
            f"strategy.local_l2_max_books_per_venue ({config.strategy.local_l2_max_books_per_venue}) "
            f"must be <= strategy.local_l2_global_max_books ({config.strategy.local_l2_global_max_books})"
        )
    if config.strategy.local_l2_hot_exec_global_budget > config.strategy.local_l2_global_max_books:
        issues.append(
            f"strategy.local_l2_hot_exec_global_budget ({config.strategy.local_l2_hot_exec_global_budget}) "
            f"must be <= strategy.local_l2_global_max_books ({config.strategy.local_l2_global_max_books})"
        )

    for vc in config.venues:
        try:
            Venue.from_str(vc.venue)
        except ValueError:
            issues.append(f"unknown venue: {vc.venue}")

    # V1 directed_pairs validation (CONFIG-001, CONFIG-004)
    issues.extend(validate_directed_pairs(config.runtime.directed_pairs, config.symbols))

    # V1 daily_universe validation (CONFIG-004)
    du = config.runtime.daily_universe
    if du.enabled:
        if not du.path.strip():
            issues.append("daily_universe.path must not be empty when enabled")
        if not _is_valid_generate_time(du.generate_time_local):
            issues.append(
                f"daily_universe.generate_time_local must be HH:MM:SS (00:00:00–23:59:59), "
                f"got: {du.generate_time_local!r}"
            )
        if du.max_symbols == 0:
            issues.append("daily_universe.max_symbols must be > 0")

    return issues


def check_raw_toml_for_chillybot(raw: dict[str, Any]) -> list[str]:
    """Scan a parsed TOML dict for removed Chillybot fields. Returns migration errors."""
    errors: list[str] = []

    runtime = raw.get("runtime", {})
    if isinstance(runtime, dict):
        for field_name in ("chillybot_api_base", "chillybot_timeout_ms", "sidecar_chillybot_mode"):
            if field_name in runtime:
                msg = REMOVED_FIELD_MESSAGES.get(field_name, f"removed Chillybot config field: runtime.{field_name}")
                errors.append(msg)

        opp_source = runtime.get("opportunity_source", "")
        if isinstance(opp_source, str) and "chillybot" in opp_source.lower():
            errors.append(REMOVED_FIELD_MESSAGES["opportunity_source"])

    return errors
