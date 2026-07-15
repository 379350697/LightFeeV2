from __future__ import annotations

from lightfee.config.loader import load_config
from lightfee.config.schema import PersistenceConfig, RuntimeConfig, StrategyConfig
from lightfee.ops.production_health import analyze_systemd_unit
from scripts import check_process_singleton as singleton


def test_spread_reversion_config_defaults_are_disabled_and_small() -> None:
    runtime = RuntimeConfig()
    strategy = StrategyConfig()

    assert runtime.spread_sidecar_snapshot_path == "runtime/spread-opportunities-current.json"
    assert runtime.spread_sidecar_refresh_ms == 1000
    assert runtime.spread_sidecar_source_mode == "sidecar_snapshot"
    assert runtime.spread_sidecar_direct_fetch_enabled is False
    assert strategy.spread_reversion_enabled is False
    assert strategy.spread_live_notional_quote == 20.0
    assert strategy.spread_max_gross_quote == 50.0
    assert strategy.spread_max_concurrent_positions == 1
    assert strategy.spread_min_samples == 120
    assert strategy.spread_min_fair_price_confidence == 1.0
    assert strategy.spread_min_liquidity_capacity_ratio == 1.25
    assert strategy.spread_min_history_ms == 300_000
    assert strategy.spread_min_executable_spread_bps == 50.0
    assert strategy.spread_max_executable_spread_bps == 300.0
    assert strategy.spread_ranker_max_candidates == 10
    assert strategy.spread_single_venue_dislocation_enabled is False
    assert strategy.spread_single_venue_dislocation_min_anchor_venues == 3
    assert strategy.spread_paper_enabled is False
    assert strategy.spread_paper_finalist_limit == 10
    assert strategy.spread_paper_excluded_symbols == []
    assert strategy.spread_paper_allowed_opportunity_labels == ["spread_reversion"]
    assert strategy.spread_paper_episode_cooldown_ms == 1_800_000
    assert strategy.spread_live_enabled is False
    assert strategy.spread_model_epoch == "v2_signed_reversion"
    assert strategy.spread_stats_window_ms == 21_600_000
    assert strategy.spread_stats_max_samples == 7_200
    assert strategy.spread_paper_bot_ids == ["tt_conservative"]
    assert (
        strategy.spread_paper_research_manifest_path
        == "config/research/spread_v2_signed_reversion.json"
    )
    assert strategy.spread_paper_primary_fill_model == "taker_taker"
    assert strategy.spread_paper_require_taker_taker is True
    assert strategy.spread_paper_markout_secs == [60, 300, 900, 1800]
    assert strategy.spread_paper_terminal_secs == 1800
    assert strategy.spread_paper_slippage_buffer_bps == 0.0
    assert PersistenceConfig().spread_paper_event_log_path == "runtime/spread-paper-events.jsonl"
    assert PersistenceConfig().spread_paper_rollback_anchor_path == ""
    assert PersistenceConfig().spread_paper_event_log_hard_max_bytes == 67_108_864


def test_loads_spread_reversion_config_without_loader_changes(tmp_path) -> None:
    path = tmp_path / "live.toml"
    path.write_text(
        """
symbols = ["BTCUSDT"]

[runtime]
mode = "live"
spread_sidecar_snapshot_path = "runtime/spread.json"
spread_sidecar_refresh_ms = 750
spread_sidecar_source_mode = "direct_market"
spread_sidecar_direct_fetch_enabled = true

[strategy]
spread_reversion_enabled = true
risk_monitor_enabled = true
spread_live_enabled = false
spread_model_epoch = "v2_signed_reversion"
spread_live_notional_quote = 25.0
spread_entry_z = 2.25
spread_min_fair_price_confidence = 0.5
spread_min_liquidity_capacity_ratio = 1.5
spread_min_history_ms = 600000
spread_min_executable_spread_bps = 55.0
spread_max_executable_spread_bps = 250.0
spread_ranker_max_candidates = 3
spread_single_venue_dislocation_enabled = true
spread_single_venue_dislocation_min_anchor_venues = 4
spread_paper_enabled = true
spread_paper_finalist_limit = 2
spread_paper_excluded_symbols = ["BBUSDT", "QNTUSDT", "EDGEUSDT"]
spread_paper_allowed_opportunity_labels = ["spread_reversion", "single_venue_dislocation"]
spread_paper_episode_cooldown_ms = 900000
spread_paper_bot_ids = ["tt_conservative"]
spread_paper_primary_fill_model = "taker_taker"
spread_paper_require_taker_taker = true
spread_paper_markout_secs = [10, 20]
spread_paper_terminal_secs = 20
spread_paper_slippage_buffer_bps = 4.0

[persistence]
spread_paper_event_log_path = "runtime/custom-spread-paper.jsonl"
spread_paper_rollback_anchor_path = "/var/lib/lightfee-test/spread-paper.epoch"
spread_paper_event_log_hard_max_bytes = 12345678

[[venues]]
venue = "binance"
taker_fee_bps = 0.6
maker_fee_bps = 0.2

[[venues]]
venue = "okx"
""",
        encoding="utf-8",
    )

    config = load_config(path)

    assert config.runtime.spread_sidecar_snapshot_path == "runtime/spread.json"
    assert config.runtime.spread_sidecar_refresh_ms == 750
    assert config.runtime.spread_sidecar_source_mode == "direct_market"
    assert config.runtime.spread_sidecar_direct_fetch_enabled is True
    assert config.strategy.spread_reversion_enabled is True
    assert config.strategy.spread_live_notional_quote == 25.0
    assert config.strategy.spread_entry_z == 2.25
    assert config.strategy.spread_min_fair_price_confidence == 0.5
    assert config.strategy.spread_min_liquidity_capacity_ratio == 1.5
    assert config.strategy.spread_min_history_ms == 600_000
    assert config.strategy.spread_min_executable_spread_bps == 55.0
    assert config.strategy.spread_max_executable_spread_bps == 250.0
    assert config.strategy.spread_ranker_max_candidates == 3
    assert config.strategy.spread_single_venue_dislocation_enabled is True
    assert config.strategy.spread_single_venue_dislocation_min_anchor_venues == 4
    assert config.strategy.spread_paper_enabled is True
    assert config.strategy.spread_paper_finalist_limit == 2
    assert config.strategy.spread_paper_excluded_symbols == [
        "BBUSDT",
        "QNTUSDT",
        "EDGEUSDT",
    ]
    assert config.strategy.spread_paper_allowed_opportunity_labels == [
        "spread_reversion",
        "single_venue_dislocation",
    ]
    assert config.strategy.spread_paper_episode_cooldown_ms == 900_000
    assert config.strategy.spread_live_enabled is False
    assert config.strategy.spread_model_epoch == "v2_signed_reversion"
    assert config.strategy.spread_paper_bot_ids == ["tt_conservative"]
    assert (
        config.strategy.spread_paper_research_manifest_path
        == "config/research/spread_v2_signed_reversion.json"
    )
    assert config.strategy.spread_paper_primary_fill_model == "taker_taker"
    assert config.strategy.spread_paper_require_taker_taker is True
    assert config.strategy.spread_paper_markout_secs == [10, 20]
    assert config.strategy.spread_paper_terminal_secs == 20
    assert config.strategy.spread_paper_slippage_buffer_bps == 4.0
    assert config.persistence.spread_paper_event_log_path == "runtime/custom-spread-paper.jsonl"
    assert (
        config.persistence.spread_paper_rollback_anchor_path
        == "/var/lib/lightfee-test/spread-paper.epoch"
    )
    assert config.persistence.spread_paper_event_log_hard_max_bytes == 12_345_678
    assert config.venues[0].taker_fee_bps == 0.6
    assert config.venues[0].maker_fee_bps == 0.2


def test_spread_sidecar_systemd_unit_is_validated_like_sidecar() -> None:
    text = """
[Service]
EnvironmentFile=/etc/lightfee/lightfee.env
ExecStart=/opt/lightfee-v2/.venv/bin/lightfee-spread-sidecar --config /opt/lightfee-v2/config/live.toml
LimitNOFILE=65536
"""

    report = analyze_systemd_unit("lightfee-spread-sidecar.service", text)

    assert report.ok


def test_singleton_counts_spread_sidecar_separately() -> None:
    processes = [
        {"pid": 1, "command": "python -m lightfee.apps.sidecar --config live.toml"},
        {"pid": 2, "command": "python -m lightfee.apps.spread_sidecar --config live.toml"},
        {"pid": 3, "command": "python -m lightfee.apps.live --config live.toml"},
    ]

    sidecars = singleton.count_matching(processes, singleton.SIDECAR_PATTERNS)
    spread_sidecars = singleton.count_matching(processes, singleton.SPREAD_SIDECAR_PATTERNS)
    lives = singleton.count_matching(processes, singleton.LIVE_PATTERNS)

    assert len(sidecars) == 1
    assert len(spread_sidecars) == 1
    assert len(lives) == 1


def test_strict_singleton_requires_one_process(capsys) -> None:
    assert singleton.check_singleton("spread-bbo", [], min_required=1) is False
    assert (
        singleton.check_singleton(
            "spread-bbo",
            [{"pid": 7, "command": "python -m lightfee.apps.spread_bbo"}],
            min_required=1,
        )
        is True
    )
    assert "VIOLATION" in capsys.readouterr().out
