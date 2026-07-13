"""Tests for config loading, validation, and Chillybot removal."""

import math

import pytest

from lightfee.config.validation import check_raw_toml_for_chillybot
from lightfee.config.loader import load_config
from lightfee.config.schema import AppConfig, StrategyConfig, VenueConfig
from lightfee.config.validation import validate_config


def test_strategy_config_defaults_first_funding_horizon_floor_to_60s():
    from lightfee.config.schema import StrategyConfig

    cfg = StrategyConfig()

    assert cfg.entry_min_first_funding_remaining_secs == 60


def test_strategy_config_forecast_default_is_reachable_in_a_seven_day_8h_window():
    """There are only 21 independent 8-hour settlements in seven days."""
    assert StrategyConfig().funding_forecast_min_samples <= 21


def test_enhanced_live_requires_a_positive_forecast_sample_threshold():
    cfg = AppConfig()
    cfg.strategy.funding_economics_mode = "enhanced_live"
    cfg.strategy.funding_forecast_mode = "live"
    cfg.strategy.funding_forecast_min_samples = 0

    issues = validate_config(cfg)

    assert any("funding_forecast_min_samples" in issue for issue in issues)


def test_strategy_config_rejects_invalid_forecast_distribution_drift_limit():
    cfg = AppConfig()
    cfg.strategy.funding_forecast_stability_max_quantile_drift_bps = -0.1

    issues = validate_config(cfg)

    assert any("funding_forecast_stability_max_quantile_drift_bps" in issue for issue in issues)


@pytest.mark.parametrize(
    "field_name",
    [
        "funding_forecast_uncertainty_haircut_bps",
        "funding_forecast_stability_max_quantile_drift_bps",
        "entry_exit_reserve_bps",
        "execution_buffer_bps",
        "capital_buffer_bps",
        "spread_slippage_reserve_bps",
        "spread_adverse_selection_buffer_bps",
        "spread_paper_slippage_buffer_bps",
        "exit_shadow_cost_buffer_bps",
    ],
)
@pytest.mark.parametrize("value", [-0.1, "not-a-number"])
def test_strategy_config_rejects_invalid_cost_and_haircut_values(
    field_name: str,
    value: object,
):
    cfg = AppConfig()
    setattr(cfg.strategy, field_name, value)

    issues = validate_config(cfg)

    assert any(field_name in issue for issue in issues)


@pytest.mark.parametrize(
    "field_name",
    [
        "funding_missing_margin_fallback_notional_quote",
        "funding_max_venue_pair_exposure_quote",
        "funding_max_global_gross_exposure_quote",
        "funding_max_settlement_bucket_exposure_quote",
        "funding_max_correlation_group_exposure_quote",
        "funding_expected_shortfall_bps",
        "funding_expected_shortfall_budget_quote",
    ],
)
def test_strategy_config_rejects_nonfinite_funding_risk_limits(field_name: str):
    cfg = AppConfig()
    setattr(cfg.strategy, field_name, float("nan"))

    issues = validate_config(cfg)

    assert f"strategy.{field_name} must be finite and >= 0" in issues


def test_strategy_config_rejects_nonfinite_or_invalid_funding_risk_contracts():
    cfg = AppConfig()
    cfg.strategy.funding_risk_health_buffer_ratio = float("inf")
    cfg.strategy.funding_settlement_crowding_bucket_ms = 1.5  # type: ignore[assignment]
    cfg.strategy.funding_venue_risk_haircut_bps_by_venue = {"binance": float("nan")}
    cfg.strategy.funding_correlation_group_by_symbol = ["BTCUSDT"]  # type: ignore[assignment]

    issues = validate_config(cfg)

    assert (
        "strategy.funding_risk_health_buffer_ratio must be finite and within (0, 1]"
        in issues
    )
    assert "strategy.funding_settlement_crowding_bucket_ms must be > 0" in issues
    assert (
        "strategy.funding_venue_risk_haircut_bps_by_venue values must be finite and >= 0"
        in issues
    )
    assert "strategy.funding_correlation_group_by_symbol must be a mapping" in issues


@pytest.mark.parametrize(
    "field_name",
    [
        "funding_new_entries_enabled",
        "funding_dynamic_expected_shortfall_enabled",
        "risk_monitor_enabled",
        "spread_live_enabled",
        "spread_paper_enabled",
    ],
)
def test_strategy_safety_switches_require_literal_booleans(field_name: str):
    cfg = AppConfig()
    setattr(cfg.strategy, field_name, "false")

    issues = validate_config(cfg)

    assert f"strategy.{field_name} must be a boolean" in issues


def test_live_config_does_not_accept_truthy_risk_monitor_value():
    cfg = AppConfig()
    cfg.runtime.mode = "live"
    cfg.strategy.risk_monitor_enabled = "false"  # type: ignore[assignment]

    issues = validate_config(cfg)

    assert "strategy.risk_monitor_enabled must be true in live mode" in issues


def test_runtime_basis_risk_checkpoint_path_must_be_non_empty():
    cfg = AppConfig()
    cfg.runtime.funding_basis_risk_checkpoint_path = " "

    issues = validate_config(cfg)

    assert "runtime.funding_basis_risk_checkpoint_path must be non-empty" in issues


def test_strategy_config_rejects_negative_first_funding_horizon():
    from lightfee.config.schema import AppConfig
    from lightfee.config.validation import validate_config

    cfg = AppConfig()
    cfg.strategy.entry_min_first_funding_remaining_secs = -1

    issues = validate_config(cfg)

    assert any("entry_min_first_funding_remaining_secs" in issue for issue in issues)


def test_strategy_config_requires_positive_maker_reconcile_backoff():
    cfg = AppConfig()
    cfg.strategy.maker_entry_reconcile_backoff_ms = 0

    issues = validate_config(cfg)

    assert any("maker_entry_reconcile_backoff_ms" in issue for issue in issues)


def test_strategy_config_requires_ordered_positive_maker_hedge_deadlines():
    cfg = AppConfig()
    cfg.strategy.maker_hedge_soft_deadline_ms = 0

    issues = validate_config(cfg)

    assert any("maker_hedge_soft_deadline_ms" in issue for issue in issues)

    cfg.strategy.maker_hedge_soft_deadline_ms = 900
    cfg.strategy.maker_hedge_deadline_ms = 800
    issues = validate_config(cfg)

    assert any("must be <= maker_hedge_deadline_ms" in issue for issue in issues)


class TestChillybotRejection:
    def test_rejects_chillybot_api_base_in_raw_toml(self):
        raw = {"runtime": {"chillybot_api_base": "https://api.chillybot.xyz"}}
        errors = check_raw_toml_for_chillybot(raw)
        assert len(errors) >= 1
        assert any("chillybot" in e.lower() for e in errors)

    def test_rejects_chillybot_timeout_ms(self):
        raw = {"runtime": {"chillybot_timeout_ms": 2000}}
        errors = check_raw_toml_for_chillybot(raw)
        assert len(errors) >= 1
        assert any("chillybot_timeout_ms" in e for e in errors)

    def test_rejects_sidecar_chillybot_mode(self):
        raw = {"runtime": {"sidecar_chillybot_mode": "assist_only"}}
        errors = check_raw_toml_for_chillybot(raw)
        assert len(errors) >= 1
        assert any("sidecar_chillybot_mode" in e for e in errors)

    def test_rejects_chillybot_opportunity_source(self):
        raw = {"runtime": {"opportunity_source": "chillybot_first"}}
        errors = check_raw_toml_for_chillybot(raw)
        assert len(errors) >= 1

    def test_passes_clean_config(self):
        raw = {"runtime": {"mode": "paper", "opportunity_input_mode": "coarse_sidecar"}}
        errors = check_raw_toml_for_chillybot(raw)
        assert len(errors) == 0


class TestConfigLoading:
    def test_loads_example_config(self):
        config = load_config("config/example.toml")
        assert len(config.symbols) >= 1
        assert config.runtime.mode == "paper"
        assert len(config.venues) >= 4

    def test_loads_live_example_config(self):
        config = load_config("config/live.example.toml")
        assert config.runtime.mode == "live"
        assert len(config.venues) == 7

    def test_live_missing_entry_provider_defaults_to_ws_bbo_even_with_legacy_local_l2_flag(
        self, tmp_path
    ):
        path = tmp_path / "live.toml"
        path.write_text(
            """
symbols = ["BTCUSDT"]

[runtime]
mode = "live"

[strategy]
risk_monitor_enabled = true
local_l2_enabled = true
local_l2_ws_enabled = true
""",
            encoding="utf-8",
        )

        config = load_config(path)

        assert config.strategy.entry_readiness_provider == "ws_bbo_quote_lease"
        assert config.strategy.local_l2_enabled is True
        assert config.strategy.local_l2_ws_enabled is True

    def test_live_explicit_local_l2_provider_keeps_v1_local_l2_mode(self, tmp_path):
        path = tmp_path / "live.toml"
        path.write_text(
            """
symbols = ["BTCUSDT"]

[runtime]
mode = "live"

[strategy]
risk_monitor_enabled = true
entry_readiness_provider = "local_l2"
local_l2_enabled = true
""",
            encoding="utf-8",
        )

        config = load_config(path)

        assert config.strategy.entry_readiness_provider == "local_l2"

    def test_paper_missing_entry_provider_keeps_schema_default(self, tmp_path):
        path = tmp_path / "paper.toml"
        path.write_text(
            """
symbols = ["BTCUSDT"]

[runtime]
mode = "paper"

[strategy]
local_l2_enabled = true
""",
            encoding="utf-8",
        )

        config = load_config(path)

        assert config.strategy.entry_readiness_provider == "local_l2"

    def test_retired_transfer_bias_keys_are_ignored_at_config_boundary(self, tmp_path):
        path = tmp_path / "paper.toml"
        path.write_text(
            """
symbols = ["BTCUSDT"]

[runtime]
mode = "paper"

[strategy]
transfer_healthy_bias_bps = 99.0
transfer_unknown_bias_bps = -99.0
transfer_degraded_bias_bps = -199.0
""",
            encoding="utf-8",
        )

        config = load_config(path)

        assert not hasattr(config.strategy, "transfer_healthy_bias_bps")
        assert not hasattr(config.strategy, "transfer_unknown_bias_bps")
        assert not hasattr(config.strategy, "transfer_degraded_bias_bps")

    def test_loads_v1_entry_perp_liquidity_thresholds(self, tmp_path):
        path = tmp_path / "paper.toml"
        path.write_text(
            """
symbols = ["BTCUSDT"]

[runtime]
mode = "paper"

[strategy]
entry_volume_floor_default_quote = 900000.0
entry_open_interest_floor_quote = 1200000.0

[strategy.entry_volume_floor_quote_by_venue]
binance = 4500000.0
okx = 4200000.0

[strategy.entry_open_interest_floor_quote_by_venue]
okx = 5000000.0
gate = 1000000.0
""",
            encoding="utf-8",
        )

        config = load_config(path)

        assert config.strategy.entry_volume_floor_quote("gate") == 900_000.0
        assert config.strategy.entry_volume_floor_quote("binance") == 4_500_000.0
        assert config.strategy.entry_volume_floor_quote("okx") == 4_200_000.0
        assert config.strategy.entry_open_interest_floor_quote("gate") == 1_000_000.0
        assert config.strategy.entry_open_interest_floor_quote("okx") == 5_000_000.0
        assert config.strategy.entry_open_interest_floor_quote("binance") == 1_200_000.0

    def test_loads_legacy_v1_entry_perp_liquidity_threshold_fields(self, tmp_path):
        path = tmp_path / "paper.toml"
        path.write_text(
            """
symbols = ["BTCUSDT"]

[runtime]
mode = "paper"

[strategy]
entry_min_perp_volume_24h_quote_gate = 1000000.0
entry_min_perp_volume_24h_quote_aster = 1000000.0
entry_min_perp_volume_24h_quote_hyperliquid = 1000000.0
entry_min_perp_volume_24h_quote_bitget = 2000000.0
entry_min_perp_volume_24h_quote_bybit = 2000000.0
entry_min_perp_volume_24h_quote_binance = 5000000.0
entry_min_perp_volume_24h_quote_okx = 5000000.0
entry_min_perp_open_interest_quote = 1100000.0
""",
            encoding="utf-8",
        )

        config = load_config(path)

        assert config.strategy.entry_volume_floor_quote("gate") == 1_000_000.0
        assert config.strategy.entry_volume_floor_quote("aster") == 1_000_000.0
        assert config.strategy.entry_volume_floor_quote("hyperliquid") == 1_000_000.0
        assert config.strategy.entry_volume_floor_quote("bitget") == 2_000_000.0
        assert config.strategy.entry_volume_floor_quote("bybit") == 2_000_000.0
        assert config.strategy.entry_volume_floor_quote("binance") == 5_000_000.0
        assert config.strategy.entry_volume_floor_quote("okx") == 5_000_000.0
        assert config.strategy.entry_open_interest_floor_quote("okx") == 1_100_000.0


class TestConfigValidation:
    def test_spread_v2_statistical_safety_contract_cannot_be_relaxed(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.spread_min_samples = 119
        config.strategy.spread_min_history_ms = 299_999
        config.strategy.spread_mean_reversion_max_half_life_ms = 1_800_001
        config.strategy.spread_signal_ttl_ms = 0
        config.strategy.spread_quote_skew_ms = 0
        config.strategy.spread_structural_break_consecutive = 4
        config.strategy.spread_structural_break_cooldown_ms = 1_799_999

        issues = validate_config(config)

        for field_name in (
            "spread_min_samples",
            "spread_min_history_ms",
            "spread_mean_reversion_max_half_life_ms",
            "spread_signal_ttl_ms",
            "spread_quote_skew_ms",
            "spread_structural_break_consecutive",
            "spread_structural_break_cooldown_ms",
        ):
            assert any(field_name in issue for issue in issues)

    def test_spread_paper_epoch_and_taker_baseline_are_startup_contracts(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.spread_paper_enabled = True
        config.strategy.spread_paper_model_epoch = "v2_other_epoch"
        config.strategy.spread_paper_primary_fill_model = "maker_taker"
        config.strategy.spread_paper_require_taker_taker = False
        config.strategy.spread_paper_finalist_limit = 0

        issues = validate_config(config)

        for field_name in (
            "spread_paper_model_epoch",
            "spread_paper_primary_fill_model",
            "spread_paper_require_taker_taker",
            "spread_paper_finalist_limit",
        ):
            assert any(field_name in issue for issue in issues)

    @pytest.mark.parametrize(
        ("finalist_limit", "terminal_secs", "markout_secs"),
        [
            (True, True, [True]),
            (1.5, 1.5, [1.5]),
            (math.nan, math.nan, [math.nan]),
        ],
    )
    def test_spread_paper_schedule_fields_reject_bool_fraction_and_nonfinite_values(
        self,
        finalist_limit: object,
        terminal_secs: object,
        markout_secs: object,
    ) -> None:
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.spread_paper_enabled = True
        config.strategy.spread_paper_finalist_limit = finalist_limit
        config.strategy.spread_paper_terminal_secs = terminal_secs
        config.strategy.spread_paper_markout_secs = markout_secs

        issues = validate_config(config)

        for field_name in (
            "spread_paper_finalist_limit",
            "spread_paper_terminal_secs",
            "spread_paper_markout_secs",
        ):
            assert any(field_name in issue for issue in issues)

    @pytest.mark.parametrize("value", ["true", "false", 1])
    def test_spread_paper_taker_baseline_requires_literal_true(
        self,
        value: object,
    ) -> None:
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.spread_paper_enabled = True
        config.strategy.spread_paper_require_taker_taker = value

        issues = validate_config(config)

        assert any("spread_paper_require_taker_taker" in issue for issue in issues)

    def test_strategy_defaults_keep_entry_window_valid(self):
        strategy = StrategyConfig()
        assert strategy.entry_window_secs >= strategy.min_scan_minutes_before_funding * 60
        assert strategy.entry_local_l2_prewarm_window_secs >= (
            strategy.min_scan_minutes_before_funding * 60
        )

    def test_v1_rejects_entry_window_shorter_than_min_scan_boundary(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.entry_window_secs = 120
        config.strategy.min_scan_minutes_before_funding = 3
        issues = validate_config(config)
        assert any("entry_window_secs" in i and "min_scan_minutes_before_funding" in i for i in issues)

    def test_v1_rejects_prewarm_window_shorter_than_min_scan_boundary(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.entry_window_secs = 480
        config.strategy.entry_local_l2_prewarm_window_secs = 120
        config.strategy.min_scan_minutes_before_funding = 3
        issues = validate_config(config)
        assert any(
            "entry_local_l2_prewarm_window_secs" in i
            and "min_scan_minutes_before_funding" in i
            for i in issues
        )

    def test_v1_rejects_prewarm_window_outside_scan_window(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.entry_window_secs = 300
        config.strategy.entry_local_l2_prewarm_window_secs = 900
        config.strategy.max_scan_minutes_before_funding = 10
        config.strategy.min_scan_minutes_before_funding = 3
        issues = validate_config(config)
        assert any(
            "entry_local_l2_prewarm_window_secs" in i
            and "max_scan_minutes_before_funding" in i
            for i in issues
        )

    def test_v1_rejects_scan_window_with_max_before_min(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.max_scan_minutes_before_funding = 2
        config.strategy.min_scan_minutes_before_funding = 3
        issues = validate_config(config)
        assert any("max_scan_minutes_before_funding" in i for i in issues)

    def test_runtime_last_good_and_startup_guards_must_be_positive(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.runtime.live_scan_last_good_max_age_ms = 0
        config.runtime.live_startup_phase_timeout_ms = 0
        config.runtime.max_market_age_ms = 0
        config.runtime.max_order_quote_age_ms = 0
        issues = validate_config(config)
        assert any("live_scan_last_good_max_age_ms" in i for i in issues)
        assert any("live_startup_phase_timeout_ms" in i for i in issues)
        assert any("max_market_age_ms" in i for i in issues)
        assert any("max_order_quote_age_ms" in i for i in issues)

    def test_entry_readiness_provider_must_be_known(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.entry_readiness_provider = "unknown_provider"
        issues = validate_config(config)
        assert any("entry_readiness_provider" in i for i in issues)

    def test_quote_lease_ttl_must_be_positive(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.entry_readiness_provider = "quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 0
        issues = validate_config(config)
        assert any("entry_quote_lease_ttl_ms" in i for i in issues)

    def test_shadow_entry_opportunity_count_default_is_v1_explicit(self):
        assert StrategyConfig().shadow_entry_opportunity_count == 2

    def test_accepts_sidecar_backed_opportunity_input_mode(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.runtime.opportunity_input_mode = "sidecar_backed"
        issues = validate_config(config)
        assert len(issues) == 0

    def test_accepts_ws_top_book_entry_readiness_provider(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.entry_readiness_provider = "ws_top_book"
        issues = validate_config(config)
        assert len(issues) == 0

    def test_ws_top_book_ttl_must_be_positive(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.entry_readiness_provider = "ws_top_book"
        config.strategy.entry_quote_lease_ttl_ms = 0
        issues = validate_config(config)
        assert any("entry_quote_lease_ttl_ms" in i for i in issues)

    def test_accepts_ws_bbo_quote_lease_entry_readiness_provider(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        issues = validate_config(config)
        assert len(issues) == 0

    def test_ws_bbo_quote_lease_ttl_must_be_positive(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 0
        issues = validate_config(config)
        assert any("entry_quote_lease_ttl_ms" in i for i in issues)

    def test_ws_bbo_per_venue_budget_must_be_positive(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_ws_bbo_per_venue_budget = 0
        issues = validate_config(config)
        assert any("entry_ws_bbo_per_venue_budget" in i for i in issues)

    def test_ws_bbo_per_venue_budget_default_is_ten(self):
        assert StrategyConfig().entry_ws_bbo_per_venue_budget == 10

    def test_entry_quote_prewarm_extra_candidate_count_default_is_twenty_four(self):
        assert StrategyConfig().entry_quote_prewarm_extra_candidate_count == 24

    def test_rejects_empty_symbols(self):
        config = AppConfig(symbols=[])
        issues = validate_config(config)
        assert any("symbols" in i for i in issues)

    def test_rejects_negative_max_concurrent(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.max_concurrent_positions = -1
        issues = validate_config(config)
        assert any("max_concurrent" in i for i in issues)

    def test_rejects_zero_entry_notional(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.entry_notional_cap_quote = 0
        issues = validate_config(config)
        assert any("entry_notional" in i for i in issues)

    def test_rejects_unknown_venue(self):
        config = AppConfig(symbols=["BTCUSDT"], venues=[VenueConfig(venue="unknown_x")])
        issues = validate_config(config)
        assert any("unknown venue" in i.lower() for i in issues)

    def test_rejects_invalid_maker_initial_slice_ratio(self):
        config = AppConfig(symbols=["BTCUSDT"])

        # 0.0 — invalid (must be > 0.0)
        config.strategy.maker_initial_slice_ratio = 0.0
        issues = validate_config(config)
        assert any("maker_initial_slice_ratio" in i for i in issues)

        # negative — invalid
        config.strategy.maker_initial_slice_ratio = -0.5
        issues = validate_config(config)
        assert any("maker_initial_slice_ratio" in i for i in issues)

        # > 1.0 — invalid
        config.strategy.maker_initial_slice_ratio = 1.5
        issues = validate_config(config)
        assert any("maker_initial_slice_ratio" in i for i in issues)

    def test_accepts_maker_initial_slice_ratio_eq_one(self):
        """Rust V1 allows maker_initial_slice_ratio = 1.0 (constraint: (0.0, 1.0])."""
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.maker_initial_slice_ratio = 1.0
        issues = validate_config(config)
        assert len(issues) == 0

    def test_rejects_invalid_entry_max_initial_clip_ratio(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.entry_max_initial_clip_ratio = 0.0
        issues = validate_config(config)
        assert any("entry_max_initial_clip_ratio" in i for i in issues)

        config.strategy.entry_max_initial_clip_ratio = float("nan")
        issues = validate_config(config)
        assert any("entry_max_initial_clip_ratio" in i for i in issues)

    def test_rejects_invalid_maker_leg_default(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.maker_leg_default = "SELL"
        issues = validate_config(config)
        assert any("maker_leg_default" in i for i in issues)

        config.strategy.maker_leg_default = "both"
        issues = validate_config(config)
        assert any("maker_leg_default" in i for i in issues)

    def test_accepts_valid_new_fields(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.maker_initial_slice_ratio = 0.5
        config.strategy.entry_max_initial_clip_ratio = 0.8
        config.strategy.maker_leg_default = "buy"
        issues = validate_config(config)
        assert len(issues) == 0

        config.strategy.maker_leg_default = "sell"
        issues = validate_config(config)
        assert len(issues) == 0
