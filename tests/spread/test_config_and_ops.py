from __future__ import annotations

from lightfee.config.loader import load_config
from lightfee.config.schema import RuntimeConfig, StrategyConfig
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
    assert strategy.spread_ranker_max_candidates == 10


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
spread_live_notional_quote = 25.0
spread_entry_z = 2.25
spread_min_fair_price_confidence = 0.5
spread_min_liquidity_capacity_ratio = 1.5
spread_min_history_ms = 600000
spread_ranker_max_candidates = 3

[[venues]]
venue = "binance"

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
    assert config.strategy.spread_ranker_max_candidates == 3


def test_spread_sidecar_systemd_unit_is_validated_like_sidecar() -> None:
    text = """
[Service]
EnvironmentFile=/etc/lightfee/lightfee.env
ExecStart=/opt/lightfee-v2/.venv/bin/lightfee-spread-sidecar --config /opt/lightfee-v2/config/live.toml
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
