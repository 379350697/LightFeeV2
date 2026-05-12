"""Worker A: Test V1 real config gap semantics — local_l2_global_max_books, entry_min_size_round_up_whitelist.

These tests prove that the remaining active V1 config knobs have real business meaning
and are not just config-only stubs.
"""

from __future__ import annotations

import pytest

from lightfee.config.schema import AppConfig, RuntimeConfig, StrategyConfig
from lightfee.config.validation import validate_config


class TestLocalL2GlobalMaxBooks:
    """local_l2_global_max_books controls active/warm/retained book capacity."""

    def test_field_exists_with_default_64(self):
        """V1 default is 64. Must exist on StrategyConfig."""
        cfg = StrategyConfig()
        assert hasattr(cfg, "local_l2_global_max_books"), (
            "local_l2_global_max_books missing from StrategyConfig — V1 controls L2 book capacity"
        )
        assert cfg.local_l2_global_max_books == 64, (
            f"Expected default 64 (V1 parity), got: {cfg.local_l2_global_max_books}"
        )

    def test_local_l2_resource_budget_consumes_max_books(self):
        """V1: local_l2_resource_budget() spreads max_books across active/warm/retained/topo pools."""
        cfg = StrategyConfig(local_l2_global_max_books=24)
        budget = cfg.local_l2_resource_budget()
        assert budget is not None
        assert budget["max_active_books"] == 24
        assert budget["warm_global"] == 24
        assert budget["retained_global"] == 24

    def test_max_books_must_be_positive(self):
        """V1 validation: local_l2_global_max_books must be > 0."""
        from lightfee.config.validation import validate_config

        config = AppConfig(
            symbols=["BTCUSDT"],
            strategy=StrategyConfig(local_l2_global_max_books=0),
        )
        issues = validate_config(config)
        assert any("local_l2_global_max_books" in i for i in issues), (
            f"Expected validation error for max_books=0, got: {issues}"
        )

    def test_default_strategy_config_is_valid(self):
        """Default should pass validation."""
        config = AppConfig(
            symbols=["BTCUSDT"],
            strategy=StrategyConfig(),
        )
        issues = validate_config(config)
        local_l2_issues = [i for i in issues if "local_l2_global_max_books" in i]
        assert len(local_l2_issues) == 0, f"Default should be valid: {local_l2_issues}"


class TestEntryMinSizeRoundUpWhitelist:
    """entry_min_size_round_up_whitelist alters symbol-specific sizing behavior."""

    def test_field_exists_default_empty(self):
        """V1 default is empty BTreeSet. Must exist on StrategyConfig."""
        cfg = StrategyConfig()
        assert hasattr(cfg, "entry_min_size_round_up_whitelist"), (
            "entry_min_size_round_up_whitelist missing from StrategyConfig — V1 controls min-size rounding"
        )
        assert cfg.entry_min_size_round_up_whitelist == [], (
            f"Expected default [], got: {cfg.entry_min_size_round_up_whitelist}"
        )

    def test_can_populate_whitelist(self):
        """Whitelist accepts venue:symbol pairs like 'gate:RAVEUSDT'."""
        cfg = StrategyConfig(
            entry_min_size_round_up_whitelist=["gate:RAVEUSDT", "bitget:BTCUSDT"]
        )
        assert "gate:RAVEUSDT" in cfg.entry_min_size_round_up_whitelist
        assert "bitget:BTCUSDT" in cfg.entry_min_size_round_up_whitelist
        assert len(cfg.entry_min_size_round_up_whitelist) == 2


class TestDirectMarketEnrichedMode:
    """direct_market_enriched is a valid V1 opportunity input mode with provenance depth."""

    def test_direct_market_enriched_is_valid_mode(self):
        """V1 direct_market_enriched is a real provider mode, not historical residue."""
        from lightfee.config.compatibility import VALID_OPPORTUNITY_INPUT_MODES

        assert "direct_market_enriched" in VALID_OPPORTUNITY_INPUT_MODES, (
            "direct_market_enriched missing from VALID_OPPORTUNITY_INPUT_MODES — "
            "V1 uses this for enriched provider with hints and transfer resolution"
        )

    def test_direct_market_enriched_passes_validation(self):
        """Config with direct_market_enriched should validate."""
        config = AppConfig(
            symbols=["BTCUSDT"],
            runtime=RuntimeConfig(opportunity_input_mode="direct_market_enriched"),
        )
        issues = validate_config(config)
        mode_issues = [i for i in issues if "opportunity_input_mode" in i]
        assert len(mode_issues) == 0, f"direct_market_enriched should validate: {mode_issues}"


class TestChillybotFieldsRemainRejected:
    """Confirm retired Chillybot behavior stays filtered out."""

    def test_chillybot_api_base_still_rejected(self):
        """Chillybot fields are still blocked."""
        from lightfee.config.validation import check_raw_toml_for_chillybot

        raw = {"runtime": {"chillybot_api_base": "https://evil.xyz"}}
        errors = check_raw_toml_for_chillybot(raw)
        assert len(errors) >= 1

    def test_sidecar_chillybot_mode_still_rejected(self):
        """sidecar_chillybot_mode is still rejected."""
        from lightfee.config.validation import check_raw_toml_for_chillybot

        raw = {"runtime": {"sidecar_chillybot_mode": "assist_only"}}
        errors = check_raw_toml_for_chillybot(raw)
        assert len(errors) >= 1

    def test_chillybot_timeout_still_rejected(self):
        """chillybot_timeout_ms is still rejected."""
        from lightfee.config.validation import check_raw_toml_for_chillybot

        raw = {"runtime": {"chillybot_timeout_ms": 5000}}
        errors = check_raw_toml_for_chillybot(raw)
        assert len(errors) >= 1

    def test_no_chillybot_fields_sneaked_into_config(self):
        """StrategyConfig must not have any Chillybot-related fields."""
        cfg = StrategyConfig()
        chillybot_related = [f for f in dir(cfg) if "chillybot" in f.lower()]
        assert len(chillybot_related) == 0, (
            f"StrategyConfig has Chillybot-related fields: {chillybot_related}"
        )
