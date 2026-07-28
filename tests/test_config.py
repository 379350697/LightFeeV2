"""Tests for config loading, validation, and Chillybot removal."""

import math

import pytest

from lightfee.config.compatibility import (
    ENTRY_READINESS_PROVIDER_ON_DEMAND,
    LEGACY_ENTRY_READINESS_PROVIDERS,
    resolve_entry_readiness_provider,
)
from lightfee.config.paths import (
    DEFAULT_HYPERLIQUID_INFO_COORDINATOR_DIR,
)
from lightfee.config.validation import check_raw_toml_for_chillybot
from lightfee.config.loader import load_config
from lightfee.config.schema import AppConfig, StrategyConfig, VenueConfig
from lightfee.config.validation import validate_config
from lightfee.core.errors import ConfigError

EXPECTED_PUBLIC_ENTRY_OPEN_INTEREST_RUNTIME_FIELDS = {
    "entry_open_interest_refresh_timeout_ms",
    "entry_open_interest_cache_fallback_max_age_ms",
}


def test_strategy_config_defaults_first_funding_horizon_floor_to_60s():
    from lightfee.config.schema import StrategyConfig

    cfg = StrategyConfig()

    assert cfg.entry_min_first_funding_remaining_secs == 60


def test_strategy_config_forecast_default_is_reachable_in_a_seven_day_8h_window():
    """There are only 21 independent 8-hour settlements in seven days."""
    assert StrategyConfig().funding_forecast_min_samples <= 21


def test_strategy_config_entry_defaults_use_the_composed_on_demand_mode():
    cfg = StrategyConfig()

    assert cfg.entry_readiness_provider == ENTRY_READINESS_PROVIDER_ON_DEMAND
    assert cfg.entry_local_l2_primary_count == 3
    assert cfg.shadow_entry_opportunity_count == 0
    assert cfg.entry_quote_prewarm_extra_candidate_count == 0
    assert cfg.maker_initial_slice_ratio == 0.25
    assert cfg.maker_entry_max_reposts == 1


@pytest.mark.parametrize(
    ("section", "field"),
    [
        ("runtime", "fee_evidence_path"),
        ("runtime", "funding_fee_evidence_path"),
        ("strategy", "funding_canary_enabled"),
        ("strategy", "funding_canary_max_entry_notional_quote"),
    ],
)
def test_removed_live_evidence_and_canary_fields_fail_config_loading(
    tmp_path, section, field
):
    path = tmp_path / "removed.toml"
    value = "true" if field.endswith("enabled") else '"retired"'
    path.write_text(
        f'symbols = ["BTCUSDT"]\n\n[{section}]\n{field} = {value}\n',
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match=rf"removed production field: {section}\.{field}"):
        load_config(path)


def test_runtime_config_validates_entry_open_interest_controls(tmp_path):
    cfg = AppConfig()

    assert cfg.runtime.entry_open_interest_refresh_timeout_ms == 750
    assert cfg.runtime.entry_open_interest_cache_fallback_max_age_ms == 30 * 60_000
    assert {
        name
        for name in cfg.runtime.__dataclass_fields__
        if name.startswith("entry_open_interest_")
    } == EXPECTED_PUBLIC_ENTRY_OPEN_INTEREST_RUNTIME_FIELDS
    assert not any("hot_cache" in name for name in cfg.runtime.__dataclass_fields__)
    assert not any("entry_open_interest" in issue for issue in validate_config(cfg))

    cfg.runtime.entry_open_interest_refresh_timeout_ms = 0
    assert (
        "runtime.entry_open_interest_refresh_timeout_ms must be a positive integer"
        in validate_config(cfg)
    )

    cfg.runtime.entry_open_interest_refresh_timeout_ms = 750
    cfg.runtime.entry_open_interest_cache_fallback_max_age_ms = 30 * 60_000 + 1
    assert (
        "runtime.entry_open_interest_cache_fallback_max_age_ms must be <= 1800000"
        in validate_config(cfg)
    )

    cfg.runtime.entry_open_interest_cache_fallback_max_age_ms = -1
    assert (
        "runtime.entry_open_interest_cache_fallback_max_age_ms must be a positive integer"
        in validate_config(cfg)
    )

    assert not hasattr(cfg.runtime, "entry_open_interest_store_path")
    assert not hasattr(cfg.runtime, "entry_open_interest_background_refresh_ms")


def test_runtime_config_validates_entry_account_truth_per_venue_timeout():
    cfg = AppConfig()

    assert cfg.runtime.entry_account_truth_per_venue_timeout_ms == 2000
    assert not hasattr(cfg.runtime, "entry_account_truth_probe_timeout_ms")
    assert cfg.runtime.live_recovery_rest_probe_timeout_ms == 2000
    assert (
        cfg.runtime.hyperliquid_info_coordinator_dir
        == DEFAULT_HYPERLIQUID_INFO_COORDINATOR_DIR
    )
    assert not any(
        "entry_account_truth_per_venue_timeout_ms" in issue
        for issue in validate_config(cfg)
    )
    assert not any(
        "hyperliquid_info_coordinator_dir" in issue
        for issue in validate_config(cfg)
    )

    cfg.runtime.entry_account_truth_per_venue_timeout_ms = 0
    assert (
        "runtime.entry_account_truth_per_venue_timeout_ms must be a positive integer"
        in validate_config(cfg)
    )
    cfg.runtime.entry_account_truth_per_venue_timeout_ms = 2000

    cfg.runtime.hyperliquid_info_coordinator_dir = ""
    assert (
        "runtime.hyperliquid_info_coordinator_dir must be non-empty"
        in validate_config(cfg)
    )


@pytest.mark.parametrize(
    ("field_name", "value", "expected"),
    [
        ("taker_fee_bps", -1.0, "taker_fee_bps must be finite and >= 0"),
        ("taker_fee_bps", float("nan"), "taker_fee_bps must be finite and >= 0"),
        (
            "maker_fee_bps",
            -0.1,
            "maker_fee_bps must be finite and >= 0; maker rebates require signed account fee evidence",
        ),
        ("max_notional", 0.0, "max_notional must be finite and > 0"),
    ],
)
def test_venue_cost_and_cap_configuration_fails_closed(
    field_name: str, value: float, expected: str
) -> None:
    venue = VenueConfig(venue="bybit")
    setattr(venue, field_name, value)
    cfg = AppConfig(symbols=["BTCUSDT"], venues=[venue])

    assert any(expected in issue for issue in validate_config(cfg))


def test_spread_paper_uses_configured_venue_fees_without_account_identity():
    cfg = AppConfig(
        symbols=["BTCUSDT"],
        venues=[VenueConfig(venue="binance", taker_fee_bps=5.0)],
    )
    cfg.strategy.spread_paper_enabled = True

    assert validate_config(cfg) == []
    assert not hasattr(cfg.runtime, "fee_evidence_account_identity_hashes")


def test_live_funding_entries_do_not_require_the_canary_profile():
    cfg = AppConfig()
    cfg.runtime.mode = "live"
    cfg.strategy.risk_monitor_enabled = True
    cfg.strategy.funding_new_entries_enabled = True

    assert not any(
        "funding_canary_enabled must be true" in issue
        for issue in validate_config(cfg)
    )


def test_live_funding_entries_do_not_require_retired_expected_shortfall_configuration():
    cfg = AppConfig()
    cfg.runtime.mode = "live"
    cfg.strategy.risk_monitor_enabled = True
    cfg.strategy.funding_new_entries_enabled = True

    issues = validate_config(cfg)

    assert not any("expected_shortfall" in issue for issue in issues)


def test_dynamic_spread_cost_gate_and_model_epoch_are_indivisible():
    cfg = AppConfig()
    cfg.strategy.spread_dynamic_net_edge_enabled = True

    assert (
        "strategy.spread_model_epoch must be a v3 epoch when dynamic net edge is enabled"
        in validate_config(cfg)
    )

    cfg.strategy.spread_model_epoch = "v3_cost_normalized_reversion"
    assert not any("spread_model_epoch" in issue for issue in validate_config(cfg))

    cfg.strategy.spread_dynamic_net_edge_enabled = False
    assert (
        "strategy.spread_dynamic_net_edge_enabled must be true for a v3 spread epoch"
        in validate_config(cfg)
    )


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
        assert config.strategy.entry_readiness_provider == ENTRY_READINESS_PROVIDER_ON_DEMAND

    def test_loads_live_example_config(self):
        config = load_config("config/live.example.toml")
        assert config.runtime.mode == "live"
        assert len(config.venues) == 7
        assert config.strategy.entry_readiness_provider == ENTRY_READINESS_PROVIDER_ON_DEMAND

    def test_config_relative_artifacts_resolve_from_project_root_without_mutating_literals(
        self,
        tmp_path,
        monkeypatch,
    ):
        project = tmp_path / "project"
        config_dir = project / "config"
        config_dir.mkdir(parents=True)
        other_cwd = tmp_path / "other"
        other_cwd.mkdir()
        path = config_dir / "live.toml"
        path.write_text(
            """
symbols = ["BTCUSDT"]

[runtime]
mode = "paper"
fee_evidence_path = "runtime/account-fee-evidence.json"
funding_fee_evidence_path = "runtime/funding-account-fee-evidence.json"

[strategy]
spread_paper_enabled = true
spread_paper_research_manifest_path = "config/research/spread_v2_signed_reversion.json"
""",
            encoding="utf-8",
        )
        monkeypatch.chdir(other_cwd)

        with pytest.raises(ConfigError, match="removed production field"):
            load_config(path)

    def test_config_artifact_duplicate_check_uses_loaded_project_root(
        self,
        tmp_path,
        monkeypatch,
    ):
        project = tmp_path / "project"
        config_dir = project / "config"
        config_dir.mkdir(parents=True)
        account_evidence = project / "runtime" / "account-fee-evidence.json"
        account_evidence.parent.mkdir()
        other_cwd = tmp_path / "other"
        other_cwd.mkdir()
        path = config_dir / "live.toml"
        path.write_text(
            f"""
symbols = ["BTCUSDT"]

[runtime]
mode = "paper"
fee_evidence_path = "runtime/account-fee-evidence.json"
funding_fee_evidence_path = "{account_evidence}"
""",
            encoding="utf-8",
        )
        monkeypatch.chdir(other_cwd)

        with pytest.raises(ConfigError, match="funding_fee_evidence_path"):
            load_config(path)

    def test_loaded_config_root_feeds_runtime_artifact_consumers_after_cwd_change(
        self,
        tmp_path,
        monkeypatch,
    ):
        project = tmp_path / "project"
        config_dir = project / "config"
        config_dir.mkdir(parents=True)
        path = config_dir / "live.toml"
        path.write_text(
            """
symbols = ["BTCUSDT"]

[runtime]
mode = "paper"

[[venues]]
venue = "binance"
taker_fee_bps = 5.0
maker_fee_bps = 2.0

[[venues]]
venue = "okx"
taker_fee_bps = 6.0
maker_fee_bps = 3.0
""",
            encoding="utf-8",
        )
        other_cwd = tmp_path / "other"
        other_cwd.mkdir()
        monkeypatch.chdir(other_cwd)

        config = load_config(path)
        assert [(row.venue, row.taker_fee_bps, row.maker_fee_bps) for row in config.venues] == [
            ("binance", 5.0, 2.0),
            ("okx", 6.0, 3.0),
        ]
        assert not hasattr(config.runtime, "fee_evidence_path")
        assert not hasattr(config.runtime, "funding_fee_evidence_path")

    def test_live_missing_entry_provider_defaults_to_composed_mode_even_with_local_l2_flag(
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

        assert config.strategy.entry_readiness_provider == ENTRY_READINESS_PROVIDER_ON_DEMAND
        resolution = resolve_entry_readiness_provider(
            config.strategy.entry_readiness_provider,
            configured=config.strategy._entry_readiness_provider_configured,
        )
        assert resolution.raw == ""
        assert resolution.effective == ENTRY_READINESS_PROVIDER_ON_DEMAND
        assert resolution.defaulted is True
        assert resolution.migrated is False
        assert config.strategy.local_l2_enabled is True
        assert config.strategy.local_l2_ws_enabled is True

    def test_loaded_live_defaulted_provider_preserves_ws_bbo_non_entry_helpers(
        self, tmp_path
    ):
        """Loader provenance keeps V1 non-entry routing while entry is composed."""
        from lightfee.core.domain import Venue
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.marketdata.ws_bbo import TopBookQuote

        path = tmp_path / "live.toml"
        path.write_text(
            """
symbols = ["BTCUSDT"]

[runtime]
mode = "live"

[strategy]
risk_monitor_enabled = true
local_l2_enabled = true
""",
            encoding="utf-8",
        )
        config = load_config(path)
        runtime = LiveRuntime(config)
        now_ms = 1_000_000
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=50000.0,
                ask=50010.0,
                observed_at_ms=now_ms,
                received_at_ms=now_ms,
                source="binance_bbo_ws",
            )
        )

        assert runtime._entry_effective_readiness_provider_uses_ws_bbo() is True
        assert runtime._entry_effective_readiness_provider_uses_local_l2() is True
        assert runtime._entry_readiness_provider_uses_ws_bbo() is True
        assert runtime._entry_readiness_provider_uses_local_l2() is False
        assert runtime.close_runtime._resolve_ws_bbo_close_quote(
            Venue.BINANCE,
            "BTCUSDT",
            now_ms=now_ms,
        ) == (50000.0, 50010.0)
        assert runtime.pending_entry_runtime._post_first_fill_executable_quote(
            Venue.BINANCE,
            "BTCUSDT",
            now_ms,
        ) == (
            50000.0,
            50010.0,
            {
                "venue": "binance",
                "bid": 50000.0,
                "ask": 50010.0,
                "observed_at_ms": now_ms,
                "max_age_ms": config.strategy.max_liquidity_snapshot_age_ms,
                "source": "ws_bbo_quote_lease",
            },
        )
        assert runtime.passive_maker_runtime._entry_readiness_provider_uses_ws_bbo()

    def test_programmatic_composed_default_keeps_local_l2_non_entry_compatibility(self):
        from lightfee.engine.runtime import LiveRuntime

        config = AppConfig()
        config.runtime.mode = "live"
        config.strategy.local_l2_enabled = True
        runtime = LiveRuntime(config)

        assert runtime._entry_effective_readiness_provider_uses_ws_bbo() is True
        assert runtime._entry_effective_readiness_provider_uses_local_l2() is True
        assert runtime._entry_readiness_provider_uses_ws_bbo() is False
        assert runtime._entry_readiness_provider_uses_local_l2() is True

    def test_live_explicit_legacy_provider_retains_raw_value_and_migrates_entry_mode(self, tmp_path):
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
        resolution = resolve_entry_readiness_provider(
            config.strategy.entry_readiness_provider,
            configured=config.strategy._entry_readiness_provider_configured,
        )
        assert resolution.effective == ENTRY_READINESS_PROVIDER_ON_DEMAND
        assert resolution.defaulted is False
        assert resolution.migrated is True

    def test_paper_missing_entry_provider_defaults_to_composed_mode(self, tmp_path):
        from lightfee.engine.runtime import LiveRuntime

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

        assert config.strategy.entry_readiness_provider == ENTRY_READINESS_PROVIDER_ON_DEMAND
        resolution = resolve_entry_readiness_provider(
            config.strategy.entry_readiness_provider,
            configured=config.strategy._entry_readiness_provider_configured,
        )
        assert resolution.raw == ""
        assert resolution.defaulted is True

        runtime = LiveRuntime(config)
        assert runtime._entry_readiness_provider_uses_ws_bbo() is False
        assert runtime._entry_readiness_provider_uses_local_l2() is True

    @pytest.mark.parametrize("provider", sorted(LEGACY_ENTRY_READINESS_PROVIDERS))
    def test_every_legacy_provider_resolves_to_composed_entry_mode(self, provider):
        resolution = resolve_entry_readiness_provider(provider)

        assert resolution.raw == provider
        assert resolution.effective == ENTRY_READINESS_PROVIDER_ON_DEMAND
        assert resolution.defaulted is False
        assert resolution.migrated is True

    def test_explicit_composed_entry_provider_is_not_migrated(self):
        resolution = resolve_entry_readiness_provider(ENTRY_READINESS_PROVIDER_ON_DEMAND)

        assert resolution.raw == ENTRY_READINESS_PROVIDER_ON_DEMAND
        assert resolution.effective == ENTRY_READINESS_PROVIDER_ON_DEMAND
        assert resolution.defaulted is False
        assert resolution.migrated is False

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
    def test_spread_quote_freshness_contract_cannot_be_relaxed(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.spread_signal_ttl_ms = 1_001
        config.strategy.spread_quote_skew_ms = 251

        issues = validate_config(config)

        assert any("spread_signal_ttl_ms must be <= 1000" in issue for issue in issues)
        assert any("spread_quote_skew_ms must be <= 250" in issue for issue in issues)

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

    def test_shadow_entry_opportunity_count_default_is_zero(self):
        assert StrategyConfig().shadow_entry_opportunity_count == 0

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

    @pytest.mark.parametrize(
        "provider",
        sorted(LEGACY_ENTRY_READINESS_PROVIDERS | {ENTRY_READINESS_PROVIDER_ON_DEMAND}),
    )
    def test_all_effective_entry_modes_require_positive_quote_lease_and_ws_bbo_budget(
        self,
        provider,
    ):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.entry_readiness_provider = provider
        config.strategy.entry_quote_lease_ttl_ms = 0
        config.strategy.entry_ws_bbo_per_venue_budget = 0

        issues = validate_config(config)

        assert any("entry_quote_lease_ttl_ms" in issue for issue in issues)
        assert any("entry_ws_bbo_per_venue_budget" in issue for issue in issues)

    def test_ws_bbo_per_venue_budget_default_is_ten(self):
        assert StrategyConfig().entry_ws_bbo_per_venue_budget == 10

    def test_entry_quote_prewarm_extra_candidate_count_default_is_zero(self):
        assert StrategyConfig().entry_quote_prewarm_extra_candidate_count == 0

    def test_zero_disables_optional_entry_prewarm_and_shadow_scopes(self):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.shadow_entry_opportunity_count = 0
        config.strategy.entry_quote_prewarm_extra_candidate_count = 0
        config.strategy.entry_local_l2_prewarm_window_secs = 0
        config.strategy.local_l2_short_prewarm_max_pairs = 0
        config.strategy.local_l2_short_prewarm_max_rank = 0

        issues = validate_config(config)

        assert not any("prewarm" in issue or "shadow_entry" in issue for issue in issues)

    @pytest.mark.parametrize(
        "field_name",
        (
            "shadow_entry_opportunity_count",
            "entry_quote_prewarm_extra_candidate_count",
            "entry_local_l2_prewarm_window_secs",
            "local_l2_short_prewarm_max_pairs",
            "local_l2_short_prewarm_max_rank",
        ),
    )
    def test_negative_optional_entry_scope_is_invalid(self, field_name):
        config = AppConfig(symbols=["BTCUSDT"])
        setattr(config.strategy, field_name, -1)

        assert any(field_name in issue for issue in validate_config(config))

    @pytest.mark.parametrize("value", (True, "3", -1))
    def test_entry_local_l2_primary_count_requires_nonnegative_integer(self, value):
        config = AppConfig(symbols=["BTCUSDT"])
        config.strategy.entry_local_l2_primary_count = value

        assert any(
            "entry_local_l2_primary_count" in issue
            for issue in validate_config(config)
        )

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
