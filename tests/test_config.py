"""Tests for config loading, validation, and Chillybot removal."""

import tempfile
from pathlib import Path

import pytest

from lightfee.config.validation import check_raw_toml_for_chillybot
from lightfee.config.loader import load_config
from lightfee.config.schema import AppConfig, StrategyConfig, VenueConfig
from lightfee.config.validation import validate_config
from lightfee.core.errors import ConfigError


def test_strategy_config_defaults_first_funding_horizon_floor_to_60s():
    from lightfee.config.schema import StrategyConfig

    cfg = StrategyConfig()

    assert cfg.entry_min_first_funding_remaining_secs == 60


def test_strategy_config_rejects_negative_first_funding_horizon():
    from lightfee.config.schema import AppConfig
    from lightfee.config.validation import validate_config

    cfg = AppConfig()
    cfg.strategy.entry_min_first_funding_remaining_secs = -1

    issues = validate_config(cfg)

    assert any("entry_min_first_funding_remaining_secs" in issue for issue in issues)


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
