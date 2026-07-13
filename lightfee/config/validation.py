"""Configuration validation with Chillybot removal enforcement."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from lightfee.config.compatibility import REMOVED_FIELD_MESSAGES, VALID_OPPORTUNITY_INPUT_MODES
from lightfee.config.schema import (
    AppConfig,
    ENTRY_READINESS_PROVIDERS,
    _is_valid_generate_time,
)
from lightfee.config.universe import validate_directed_pairs
from lightfee.core.domain import Venue


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

    if config.runtime.sidecar_snapshot_max_age_ms <= 0:
        issues.append(
            f"runtime.sidecar_snapshot_max_age_ms must be > 0, "
            f"got: {config.runtime.sidecar_snapshot_max_age_ms}"
        )
    if config.runtime.live_scan_last_good_max_age_ms <= 0:
        issues.append(
            f"runtime.live_scan_last_good_max_age_ms must be > 0, "
            f"got: {config.runtime.live_scan_last_good_max_age_ms}"
        )
    if config.runtime.live_scan_recovery_success_count <= 0:
        issues.append(
            f"runtime.live_scan_recovery_success_count must be > 0, "
            f"got: {config.runtime.live_scan_recovery_success_count}"
        )
    if config.runtime.live_startup_phase_timeout_ms <= 0:
        issues.append(
            f"runtime.live_startup_phase_timeout_ms must be > 0, "
            f"got: {config.runtime.live_startup_phase_timeout_ms}"
        )
    if config.runtime.max_market_age_ms <= 0:
        issues.append(
            f"runtime.max_market_age_ms must be > 0, "
            f"got: {config.runtime.max_market_age_ms}"
        )
    if config.runtime.max_order_quote_age_ms <= 0:
        issues.append(
            f"runtime.max_order_quote_age_ms must be > 0, "
            f"got: {config.runtime.max_order_quote_age_ms}"
        )
    if not str(config.runtime.funding_basis_risk_checkpoint_path or "").strip():
        issues.append("runtime.funding_basis_risk_checkpoint_path must be non-empty")
    if config.runtime.local_l2_depth_bridge_enabled:
        if not str(config.runtime.local_l2_depth_bridge_path or "").strip():
            issues.append("runtime.local_l2_depth_bridge_path must be set when enabled")
        if config.runtime.local_l2_depth_bridge_max_levels <= 0:
            issues.append("runtime.local_l2_depth_bridge_max_levels must be > 0")
        if config.runtime.local_l2_depth_bridge_publish_interval_ms <= 0:
            issues.append(
                "runtime.local_l2_depth_bridge_publish_interval_ms must be > 0"
            )

    if config.strategy.max_concurrent_positions < 0:
        issues.append("strategy.max_concurrent_positions must be >= 0")

    if config.strategy.entry_min_first_funding_remaining_secs < 0:
        issues.append("strategy.entry_min_first_funding_remaining_secs must be >= 0")

    if config.strategy.entry_notional_cap_quote <= 0:
        issues.append("strategy.entry_notional_cap_quote must be > 0")

    # Safety gates must be literal booleans.  Dataclass annotations do not
    # coerce programmatically assembled configs, and Python treats the string
    # ``"false"`` as truthy.  Rejecting it at the parser/validation boundary
    # keeps entry freeze, risk monitoring and paper-only intent fail-closed.
    for field_name in (
        "funding_new_entries_enabled",
        "funding_dynamic_expected_shortfall_enabled",
        "risk_monitor_enabled",
        "spread_live_enabled",
        "spread_paper_enabled",
    ):
        value = getattr(config.strategy, field_name)
        if value is not True and value is not False:
            issues.append(f"strategy.{field_name} must be a boolean")

    if (
        config.runtime.mode == "live"
        and config.strategy.risk_monitor_enabled is not True
    ):
        issues.append("strategy.risk_monitor_enabled must be true in live mode")

    if config.strategy.funding_forecast_mode not in {"shadow", "live"}:
        issues.append("strategy.funding_forecast_mode must be 'shadow' or 'live'")
    if not _is_finite_nonnegative(
        config.strategy.funding_forecast_uncertainty_haircut_bps
    ):
        issues.append("strategy.funding_forecast_uncertainty_haircut_bps must be >= 0")
    if not _is_nonnegative_int(config.strategy.funding_forecast_min_samples):
        issues.append("strategy.funding_forecast_min_samples must be >= 0")
    if not _is_nonnegative_int(config.strategy.funding_forecast_shadow_min_days):
        issues.append("strategy.funding_forecast_shadow_min_days must be >= 0")
    if not _is_finite_nonnegative(
        config.strategy.funding_forecast_stability_max_quantile_drift_bps
    ):
        issues.append(
            "strategy.funding_forecast_stability_max_quantile_drift_bps must be >= 0"
        )
    for field_name, value in {
        "entry_exit_reserve_bps": config.strategy.entry_exit_reserve_bps,
        "execution_buffer_bps": config.strategy.execution_buffer_bps,
        "capital_buffer_bps": config.strategy.capital_buffer_bps,
        "spread_slippage_reserve_bps": config.strategy.spread_slippage_reserve_bps,
        "spread_adverse_selection_buffer_bps": (
            config.strategy.spread_adverse_selection_buffer_bps
        ),
        "spread_paper_slippage_buffer_bps": (
            config.strategy.spread_paper_slippage_buffer_bps
        ),
        "exit_shadow_cost_buffer_bps": config.strategy.exit_shadow_cost_buffer_bps,
    }.items():
        if not _is_finite_nonnegative(value):
            issues.append(f"strategy.{field_name} must be finite and >= 0")
    for field_name, value in {
        "funding_missing_margin_fallback_notional_quote": (
            config.strategy.funding_missing_margin_fallback_notional_quote
        ),
        "funding_max_venue_pair_exposure_quote": config.strategy.funding_max_venue_pair_exposure_quote,
        "funding_max_global_gross_exposure_quote": config.strategy.funding_max_global_gross_exposure_quote,
        "funding_max_settlement_bucket_exposure_quote": config.strategy.funding_max_settlement_bucket_exposure_quote,
        "funding_max_correlation_group_exposure_quote": config.strategy.funding_max_correlation_group_exposure_quote,
        "funding_expected_shortfall_bps": config.strategy.funding_expected_shortfall_bps,
        "funding_expected_shortfall_budget_quote": config.strategy.funding_expected_shortfall_budget_quote,
    }.items():
        if not _is_finite_nonnegative(value):
            issues.append(f"strategy.{field_name} must be finite and >= 0")
    for field_name, value in {
        "funding_dynamic_expected_shortfall_window_ms": (
            config.strategy.funding_dynamic_expected_shortfall_window_ms
        ),
        "funding_dynamic_expected_shortfall_max_samples": (
            config.strategy.funding_dynamic_expected_shortfall_max_samples
        ),
        "funding_dynamic_expected_shortfall_max_pairs": (
            config.strategy.funding_dynamic_expected_shortfall_max_pairs
        ),
        "funding_dynamic_expected_shortfall_horizon_ms": (
            config.strategy.funding_dynamic_expected_shortfall_horizon_ms
        ),
        "funding_dynamic_expected_shortfall_min_samples": (
            config.strategy.funding_dynamic_expected_shortfall_min_samples
        ),
        "funding_dynamic_expected_shortfall_min_history_ms": (
            config.strategy.funding_dynamic_expected_shortfall_min_history_ms
        ),
        "funding_dynamic_expected_shortfall_quote_skew_ms": (
            config.strategy.funding_dynamic_expected_shortfall_quote_skew_ms
        ),
        "funding_dynamic_expected_shortfall_checkpoint_max_age_ms": (
            config.strategy.funding_dynamic_expected_shortfall_checkpoint_max_age_ms
        ),
        "funding_dynamic_expected_shortfall_checkpoint_publish_interval_ms": (
            config.strategy.funding_dynamic_expected_shortfall_checkpoint_publish_interval_ms
        ),
    }.items():
        if not _is_positive_int(value):
            issues.append(f"strategy.{field_name} must be a positive integer")
    dynamic_es_window_ms = config.strategy.funding_dynamic_expected_shortfall_window_ms
    dynamic_es_horizon_ms = config.strategy.funding_dynamic_expected_shortfall_horizon_ms
    dynamic_es_history_ms = config.strategy.funding_dynamic_expected_shortfall_min_history_ms
    if (
        _is_positive_int(dynamic_es_horizon_ms)
        and _is_positive_int(dynamic_es_window_ms)
        and dynamic_es_horizon_ms > dynamic_es_window_ms
    ):
        issues.append(
            "strategy.funding_dynamic_expected_shortfall_horizon_ms must not exceed window_ms"
        )
    if (
        _is_positive_int(dynamic_es_history_ms)
        and _is_positive_int(dynamic_es_window_ms)
        and dynamic_es_history_ms > dynamic_es_window_ms
    ):
        issues.append(
            "strategy.funding_dynamic_expected_shortfall_min_history_ms must not exceed window_ms"
        )
    try:
        dynamic_es_confidence = float(
            config.strategy.funding_dynamic_expected_shortfall_confidence
        )
    except (TypeError, ValueError):
        dynamic_es_confidence = 0.0
    if not math.isfinite(dynamic_es_confidence) or not 0.0 < dynamic_es_confidence < 1.0:
        issues.append(
            "strategy.funding_dynamic_expected_shortfall_confidence must be finite and within (0, 1)"
        )
    if (
        config.runtime.mode == "live"
        and config.strategy.funding_new_entries_enabled is True
    ):
        if config.strategy.funding_dynamic_expected_shortfall_enabled is not True:
            issues.append(
                "strategy.funding_dynamic_expected_shortfall_enabled must be true when live funding entries are enabled"
            )
        if not _is_positive_finite(
            config.strategy.funding_expected_shortfall_budget_quote
        ):
            issues.append(
                "strategy.funding_expected_shortfall_budget_quote must be > 0 when live funding entries are enabled"
            )
    if not _is_finite_ratio(config.strategy.funding_risk_health_buffer_ratio):
        issues.append(
            "strategy.funding_risk_health_buffer_ratio must be finite and within (0, 1]"
        )
    if not isinstance(config.strategy.funding_venue_risk_haircut_bps_by_venue, dict):
        issues.append("strategy.funding_venue_risk_haircut_bps_by_venue must be a mapping")
    else:
        for venue, haircut_bps in (
            config.strategy.funding_venue_risk_haircut_bps_by_venue.items()
        ):
            if not str(venue).strip() or not _is_finite_nonnegative(haircut_bps):
                issues.append(
                    "strategy.funding_venue_risk_haircut_bps_by_venue values "
                    "must be finite and >= 0"
                )
                break
    if not isinstance(config.strategy.funding_correlation_group_by_symbol, dict):
        issues.append("strategy.funding_correlation_group_by_symbol must be a mapping")
    else:
        for symbol, group in config.strategy.funding_correlation_group_by_symbol.items():
            if not str(symbol).strip() or not str(group).strip():
                issues.append(
                    "strategy.funding_correlation_group_by_symbol keys and values "
                    "must be non-empty"
                )
                break
    if not _is_positive_int(config.strategy.funding_settlement_crowding_bucket_ms):
        issues.append(
            "strategy.funding_settlement_crowding_bucket_ms must be > 0"
        )
    if config.strategy.funding_economics_mode not in {
        "v1_exact",
        "enhanced_shadow",
        "enhanced_live",
    }:
        issues.append(
            "strategy.funding_economics_mode must be v1_exact, enhanced_shadow, or enhanced_live"
        )
    if (
        config.strategy.funding_economics_mode == "enhanced_live"
        and config.strategy.funding_forecast_mode != "live"
    ):
        issues.append(
            "strategy.funding_forecast_mode must be live when funding_economics_mode is enhanced_live"
        )
    if (
        config.strategy.funding_economics_mode == "enhanced_live"
        and not _is_positive_int(config.strategy.funding_forecast_min_samples)
    ):
        issues.append(
            "strategy.funding_forecast_min_samples must be > 0 when funding_economics_mode is enhanced_live"
        )

    if config.strategy.shadow_entry_opportunity_count < 0:
        issues.append("strategy.shadow_entry_opportunity_count must be >= 0")
    if config.strategy.maker_entry_reconcile_backoff_ms <= 0:
        issues.append("strategy.maker_entry_reconcile_backoff_ms must be > 0")
    if config.strategy.maker_hedge_soft_deadline_ms <= 0:
        issues.append("strategy.maker_hedge_soft_deadline_ms must be > 0")
    if config.strategy.maker_hedge_deadline_ms <= 0:
        issues.append("strategy.maker_hedge_deadline_ms must be > 0")
    if (
        config.strategy.maker_hedge_soft_deadline_ms
        > config.strategy.maker_hedge_deadline_ms
    ):
        issues.append(
            "strategy.maker_hedge_soft_deadline_ms must be <= maker_hedge_deadline_ms"
        )

    if config.strategy.spread_live_enabled:
        issues.append("strategy.spread_live_enabled is not supported; spread remains paper-only")
    # These are the v2 signed-basis model's statistical safety boundaries,
    # not tuning suggestions.  Letting a config silently relax them turns a
    # cold/noisy sequence into a tradable signal and makes epochs incomparable.
    if not 0 < config.strategy.spread_stats_window_ms <= 6 * 60 * 60 * 1000:
        issues.append("strategy.spread_stats_window_ms must be within (0, 21600000]")
    if config.strategy.spread_stats_max_samples < 120:
        issues.append("strategy.spread_stats_max_samples must be >= 120")
    if config.strategy.spread_stats_max_samples > 7_200:
        issues.append("strategy.spread_stats_max_samples must be <= 7200")
    if config.strategy.spread_min_samples < 120:
        issues.append("strategy.spread_min_samples must be >= 120")
    if config.strategy.spread_min_history_ms < 5 * 60 * 1000:
        issues.append("strategy.spread_min_history_ms must be >= 300000")
    if not 0 < config.strategy.spread_mean_reversion_max_half_life_ms <= 30 * 60 * 1000:
        issues.append(
            "strategy.spread_mean_reversion_max_half_life_ms must be within (0, 1800000]"
        )
    if config.strategy.spread_signal_ttl_ms <= 0:
        issues.append("strategy.spread_signal_ttl_ms must be > 0")
    if config.strategy.spread_quote_skew_ms <= 0:
        issues.append("strategy.spread_quote_skew_ms must be > 0")
    if not 0 < config.strategy.spread_stats_short_window_ms <= config.strategy.spread_stats_window_ms:
        issues.append(
            "strategy.spread_stats_short_window_ms must be within (0, spread_stats_window_ms]"
        )
    if config.strategy.spread_structural_break_consecutive < 5:
        issues.append("strategy.spread_structural_break_consecutive must be >= 5")
    if config.strategy.spread_structural_break_sigma <= 0.0:
        issues.append("strategy.spread_structural_break_sigma must be > 0")
    if config.strategy.spread_structural_break_cooldown_ms < 30 * 60 * 1000:
        issues.append(
            "strategy.spread_structural_break_cooldown_ms must be >= 1800000"
        )
    if config.strategy.spread_paper_enabled:
        if (
            config.strategy.spread_paper_model_epoch
            != config.strategy.spread_model_epoch
        ):
            issues.append(
                "strategy.spread_paper_model_epoch must match strategy.spread_model_epoch"
            )
        if str(config.strategy.spread_paper_primary_fill_model or "").lower() != "taker_taker":
            issues.append(
                "strategy.spread_paper_primary_fill_model must be taker_taker"
            )
        if config.strategy.spread_paper_require_taker_taker is not True:
            issues.append(
                "strategy.spread_paper_require_taker_taker must be literal true"
            )
        if not _is_positive_int(config.strategy.spread_paper_finalist_limit):
            issues.append(
                "strategy.spread_paper_finalist_limit must be a positive integer"
            )
        if not _is_positive_int(
            config.strategy.spread_paper_min_decision_latency_ms
        ):
            issues.append(
                "strategy.spread_paper_min_decision_latency_ms must be a positive integer"
            )
        if not _is_positive_int(config.strategy.spread_paper_terminal_secs):
            issues.append(
                "strategy.spread_paper_terminal_secs must be a positive integer"
            )
        markout_secs = config.strategy.spread_paper_markout_secs
        if (
            not isinstance(markout_secs, list)
            or not markout_secs
            or not all(_is_positive_int(value) for value in markout_secs)
        ):
            issues.append(
                "strategy.spread_paper_markout_secs must be a non-empty list of positive integers"
            )
        manifest_path = str(
            config.strategy.spread_paper_research_manifest_path or ""
        ).strip()
        if not manifest_path:
            issues.append("strategy.spread_paper_research_manifest_path must be set")
        elif not Path(manifest_path).is_file():
            issues.append(
                "strategy.spread_paper_research_manifest_path must reference a file"
            )

    if (
        config.strategy.max_scan_minutes_before_funding > 0
        and config.strategy.min_scan_minutes_before_funding > 0
        and config.strategy.max_scan_minutes_before_funding
        < config.strategy.min_scan_minutes_before_funding
    ):
        issues.append(
            "strategy.max_scan_minutes_before_funding must be >= "
            "strategy.min_scan_minutes_before_funding"
        )

    min_before_secs = config.strategy.min_scan_minutes_before_funding * 60
    max_before_secs = config.strategy.max_scan_minutes_before_funding * 60
    if min_before_secs > 0 and config.strategy.entry_window_secs < min_before_secs:
        issues.append(
            "strategy.entry_window_secs must be >= "
            "strategy.min_scan_minutes_before_funding * 60"
        )
    if (
        min_before_secs > 0
        and config.strategy.entry_local_l2_prewarm_window_secs < min_before_secs
    ):
        issues.append(
            "strategy.entry_local_l2_prewarm_window_secs must be >= "
            "strategy.min_scan_minutes_before_funding * 60"
        )
    if (
        max_before_secs > 0
        and config.strategy.entry_local_l2_prewarm_window_secs > max_before_secs
    ):
        issues.append(
            "strategy.entry_local_l2_prewarm_window_secs must be <= "
            "strategy.max_scan_minutes_before_funding * 60"
        )

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

    provider = config.strategy.entry_readiness_provider.strip().lower()
    if provider not in ENTRY_READINESS_PROVIDERS:
        issues.append(
            "strategy.entry_readiness_provider must be one of "
            f"{list(ENTRY_READINESS_PROVIDERS)}, got: {provider}"
        )
    quote_lease_ttl_ms = config.strategy.entry_quote_lease_ttl_ms
    if (
        provider in {"quote_lease", "ws_top_book", "ws_bbo_quote_lease"}
        and quote_lease_ttl_ms <= 0
    ):
        issues.append("strategy.entry_quote_lease_ttl_ms must be > 0")
    ws_bbo_per_venue_budget = config.strategy.entry_ws_bbo_per_venue_budget
    if provider == "ws_bbo_quote_lease" and ws_bbo_per_venue_budget <= 0:
        issues.append("strategy.entry_ws_bbo_per_venue_budget must be > 0")

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


def _is_finite_nonnegative(value: object) -> bool:
    """Return true only for finite, non-negative numeric policy inputs."""
    if isinstance(value, bool):
        return False
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(numeric) and numeric >= 0.0


def _is_finite_ratio(value: object) -> bool:
    """A usable margin-health ratio preserves some collateral and is bounded."""
    if isinstance(value, bool):
        return False
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(numeric) and 0.0 < numeric <= 1.0


def _is_positive_int(value: object) -> bool:
    """Reject fractional/boolean/NaN scheduling horizons at config boundary."""
    if isinstance(value, bool):
        return False
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(numeric) and numeric > 0.0 and numeric.is_integer()


def _is_nonnegative_int(value: object) -> bool:
    """Reject booleans, fractions and non-finite sample-count policy values."""
    if isinstance(value, bool):
        return False
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(numeric) and numeric >= 0.0 and numeric.is_integer()


def _is_positive_finite(value: object) -> bool:
    """Strictly positive finite numeric policy value, never a boolean."""
    return _is_finite_nonnegative(value) and float(value) > 0.0


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
