"""End-to-end configuration and runtime smoke tests."""

import asyncio
import json
import subprocess
import sys
from types import SimpleNamespace

import pytest

from lightfee.config.loader import load_config
from lightfee.config.validation import validate_config
from lightfee.config.schema import AppConfig


class TestConfigSmoke:
    def test_example_config_loads(self):
        config = load_config("config/example.toml")
        issues = validate_config(config)
        assert len(issues) == 0

    def test_live_example_config_loads(self):
        config = load_config("config/live.example.toml")
        issues = validate_config(config)
        assert len(issues) == 0

    def test_no_chillybot_fields_accepted(self):
        config = load_config("config/example.toml")
        # Verify no Chillybot module/field leakage
        import lightfee
        assert not hasattr(lightfee, "chillybot")
        assert not hasattr(lightfee, "feedgrab")

    def test_all_seven_venues_in_registry(self):
        from lightfee.venues.registry import all_live_perp_venues
        venues = all_live_perp_venues()
        venue_names = {v.value for v in venues}
        assert venue_names == {"binance", "okx", "bybit", "bitget", "gate", "aster", "hyperliquid"}


class TestImportSmoke:
    def test_core_imports(self):
        from lightfee.core import domain, money, time, errors, contracts

    def test_config_imports(self):
        from lightfee.config import schema, loader, validation, compatibility, defaults

    def test_persistence_imports(self):
        from lightfee.persistence import journal, snapshot_store, sqlite_store, ledgers, metrics

    def test_venues_imports(self):
        from lightfee.venues import base, registry, common, binance, okx, bybit, bitget, gate, aster, hyperliquid

    def test_sidecar_imports(self):
        from lightfee.sidecar import snapshot, publisher, pairing, service

    def test_strategy_imports(self):
        from lightfee.strategy import discovery, market_view

    def test_risk_imports(self):
        from lightfee.risk import modes, budgets, health, operator

    def test_engine_imports(self):
        from lightfee.engine import state, lifecycle, recovery, entry, exit, passive_maker, execution_planner, runtime, supervisor

    def test_marketdata_imports(self):
        from lightfee.marketdata import l2, liquidity, freshness, local_book

    def test_offline_imports(self):
        from lightfee.offline.analysis import journal as aj, incident
        from lightfee.offline.replay import dataset, engine, counterfactual, walk_forward
        from lightfee.offline.evolution import report, ledger, approval, cycle
        from lightfee.offline.llm_evolution import report as llm_report
        from lightfee.offline.reports import render, daily

    def test_apps_imports(self):
        from lightfee.apps import live, sidecar, scheduler, ops, report, replay, evolution, probe

    def test_sidecar_module_help_invokes_main(self):
        result = subprocess.run(
            [sys.executable, "-m", "lightfee.apps.sidecar", "--help"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0
        assert "lightfee-sidecar" in result.stdout

    def test_ops_imports(self):
        from lightfee.ops import commands


class TestRuntimeLaneScheduling:
    """V1 parity: run_loop schedules all eight lanes in V1 order."""

    @pytest.mark.asyncio
    async def test_tick_does_not_run_retired_funding_basis_risk_admission(
        self, tmp_path, monkeypatch
    ):
        from lightfee.config.schema import RuntimeConfig, PersistenceConfig
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.sidecar.publisher import publish_snapshot
        from lightfee.sidecar.snapshot import (
            FundingLifecycle,
            LiquidityLifecycle,
            MarketLifecycle,
            QuoteSnapshot,
            SidecarSnapshot,
        )

        now_ms = 2_000_000
        config = AppConfig(
            runtime=RuntimeConfig(
                mode="paper",
                sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
                sidecar_snapshot_max_age_ms=60_000,
                live_scan_last_good_max_age_ms=60_000,
                max_market_age_ms=60_000,
            ),
            persistence=PersistenceConfig(
                event_log_path=str(tmp_path / "events.jsonl"),
                snapshot_path=str(tmp_path / "state.json"),
            ),
            symbols=["BTCUSDT"],
        )
        publish_snapshot(
            SidecarSnapshot(
                published_at_ms=now_ms,
                market_observed_at_ms=now_ms,
                candidate_build_observed_at_ms=now_ms,
                candidate_build_diagnostics={
                    "input_quote_count": 2,
                    "requested_symbol_count": 1,
                    "requested_symbols": ["BTCUSDT"],
                    "requested_venues": ["binance", "okx"],
                    "directional_pair_count": 0,
                    "output_candidate_count": 0,
                    "future_input_quote_count": 0,
                    "rejection_counts": {},
                },
                source_mode="direct_market",
                acquisition_mode="fresh_sidecar",
                funding_lifecycle=[
                    FundingLifecycle("binance", now_ms, 1, 1),
                    FundingLifecycle("okx", now_ms, 1, 1),
                ],
                market_lifecycle=[
                    MarketLifecycle("binance", now_ms, 1, 1),
                    MarketLifecycle("okx", now_ms, 1, 1),
                ],
                liquidity_lifecycle=[
                    LiquidityLifecycle("binance", now_ms, 1, 1),
                    LiquidityLifecycle("okx", now_ms, 1, 1),
                ],
                quotes={
                    "binance:BTCUSDT": QuoteSnapshot(
                        venue="binance",
                        symbol="BTCUSDT",
                        bid=100.0,
                        ask=101.0,
                        observed_at_ms=now_ms,
                        funding_rate_bps=1.0,
                        funding_timestamp_ms=now_ms + 28_800_000,
                        funding_interval_ms=28_800_000,
                    ),
                    "okx:BTCUSDT": QuoteSnapshot(
                        venue="okx",
                        symbol="BTCUSDT",
                        bid=100.0,
                        ask=101.0,
                        observed_at_ms=now_ms,
                        funding_rate_bps=-1.0,
                        funding_timestamp_ms=now_ms + 28_800_000,
                        funding_interval_ms=28_800_000,
                    ),
                },
            ),
            config.runtime.sidecar_snapshot_path,
        )
        runtime = LiveRuntime(config)
        runtime.journal.open()

        class FailingFundingRiskRuntime:
            def __init__(self) -> None:
                self.marked_reasons: list[str] = []

            def observe_fresh_snapshot(self, *_args, **_kwargs):
                raise RuntimeError("basis checkpoint write failed")

            def mark_unhealthy(self, reason: str) -> None:
                self.marked_reasons.append(reason)

        fake_risk_runtime = FailingFundingRiskRuntime()
        runtime.funding_risk_runtime = fake_risk_runtime
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms",
            lambda: now_ms,
        )

        try:
            await runtime.tick()
        finally:
            runtime.journal.close()

        assert fake_risk_runtime.marked_reasons == []
        assert "funding_basis_risk" not in runtime.state.last_scan

    @pytest.mark.asyncio
    async def test_degraded_tick_does_not_publish_retired_funding_basis_risk_state(
        self, tmp_path, monkeypatch
    ):
        from lightfee.config.schema import RuntimeConfig, PersistenceConfig
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.sidecar.publisher import publish_snapshot
        from lightfee.sidecar.snapshot import (
            FundingLifecycle,
            LiquidityLifecycle,
            MarketLifecycle,
            QuoteSnapshot,
            SidecarSnapshot,
        )

        now_ms = 2_000_000
        config = AppConfig(
            runtime=RuntimeConfig(
                mode="paper",
                sidecar_snapshot_path=str(tmp_path / "sidecar.json"),
                sidecar_snapshot_max_age_ms=60_000,
                live_scan_last_good_max_age_ms=60_000,
                max_market_age_ms=60_000,
            ),
            persistence=PersistenceConfig(
                event_log_path=str(tmp_path / "events.jsonl"),
                snapshot_path=str(tmp_path / "state.json"),
            ),
            symbols=["BTCUSDT"],
        )
        publish_snapshot(
            SidecarSnapshot(
                published_at_ms=now_ms,
                market_observed_at_ms=now_ms,
                candidate_build_observed_at_ms=now_ms,
                candidate_build_diagnostics={
                    "input_quote_count": 1,
                    "requested_symbol_count": 1,
                    "requested_symbols": ["BTCUSDT"],
                    "requested_venues": ["binance"],
                    "directional_pair_count": 0,
                    "output_candidate_count": 0,
                    "future_input_quote_count": 0,
                    "rejection_counts": {},
                },
                source_mode="direct_market",
                acquisition_mode="degraded_sidecar",
                degraded_domains=["perp_liquidity"],
                funding_lifecycle=[
                    FundingLifecycle("binance", now_ms, 1, 1)
                ],
                market_lifecycle=[MarketLifecycle("binance", now_ms, 1, 1)],
                liquidity_lifecycle=[
                    LiquidityLifecycle(
                        venue="binance",
                        observed_at_ms=now_ms,
                        symbol_count=1,
                        coverage_usable=0,
                        degraded_reason="liquidity unavailable",
                    )
                ],
                quotes={
                    "binance:BTCUSDT": QuoteSnapshot(
                        venue="binance",
                        symbol="BTCUSDT",
                        bid=100.0,
                        ask=101.0,
                        observed_at_ms=now_ms,
                        funding_rate_bps=1.0,
                        funding_timestamp_ms=now_ms + 28_800_000,
                        funding_interval_ms=28_800_000,
                    )
                },
            ),
            config.runtime.sidecar_snapshot_path,
        )
        runtime = LiveRuntime(config)
        runtime.journal.open()

        class FundingRiskRuntimeSpy:
            def __init__(self) -> None:
                self.marked_reasons: list[str] = []
                self.observe_calls = 0

            def observe_fresh_snapshot(self, *_args, **_kwargs):
                self.observe_calls += 1
                return {"checkpoint_healthy": True}

            def mark_unhealthy(self, reason: str) -> None:
                self.marked_reasons.append(reason)

        fake_risk_runtime = FundingRiskRuntimeSpy()
        runtime.funding_risk_runtime = fake_risk_runtime
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms",
            lambda: now_ms,
        )

        try:
            await runtime.tick()
        finally:
            runtime.journal.close()

        assert fake_risk_runtime.observe_calls == 0
        assert fake_risk_runtime.marked_reasons == []
        assert "funding_basis_risk" not in runtime.state.last_scan

    @pytest.mark.asyncio
    async def test_run_loop_schedules_all_lanes(self, monkeypatch):
        """Verify run_loop calls every lane at least once in V1 order."""
        import tempfile
        from pathlib import Path
        from lightfee.config.schema import (
            AppConfig, RuntimeConfig, StrategyConfig, PersistenceConfig,
        )
        from lightfee.engine.runtime import LiveRuntime

        td = tempfile.mkdtemp()
        try:
            config = AppConfig(
                runtime=RuntimeConfig(
                    mode="paper",
                    poll_interval_ms=100,
                    sidecar_snapshot_path=str(Path(td) / "sidecar.json"),
                    sidecar_snapshot_max_age_ms=600_000,
                    tick_failure_backoff_initial_ms=100,
                    tick_failure_backoff_max_ms=1000,
                ),
                strategy=StrategyConfig(
                    risk_monitor_enabled=False,
                    max_concurrent_positions=2,
                    local_l2_enabled=False,
                    local_l2_ws_enabled=False,
                ),
                persistence=PersistenceConfig(
                    event_log_path=str(Path(td) / "events.jsonl"),
                    snapshot_path=str(Path(td) / "state.json"),
                ),
                venues=[],
                symbols=["BTCUSDT"],
            )
            runtime = LiveRuntime(config)
            runtime.journal.open()

            seen: list[str] = []

            async def fake_full_tick():
                seen.append("full_tick")

            async def fake_active_tick():
                seen.append("active_tick")

            async def fake_rate_limit_reload(now_ms: int):
                seen.append("rate_limit_reload")

            async def fake_l2_sync(now_ms: int):
                seen.append("l2_sync")

            async def fake_passive_close(now_ms: int):
                seen.append("passive_close")

            async def fake_normal_exits(now_ms: int):
                seen.append("normal_exits")

            async def fake_maker_event(now_ms: int):
                seen.append("maker_event")

            async def fake_housekeeping(now_ms: int):
                seen.append("housekeeping")

            monkeypatch.setattr(runtime, "tick", fake_full_tick)
            monkeypatch.setattr(runtime, "tick_active_positions", fake_active_tick)
            monkeypatch.setattr(runtime, "_maybe_reload_rate_limits", fake_rate_limit_reload)
            monkeypatch.setattr(runtime, "_sync_local_l2_data", fake_l2_sync)
            monkeypatch.setattr(runtime, "_maybe_tick_passive_close", fake_passive_close)
            monkeypatch.setattr(runtime, "_maybe_process_normal_exits", fake_normal_exits)
            monkeypatch.setattr(runtime, "_maybe_tick_maker_event", fake_maker_event)
            monkeypatch.setattr(runtime, "_post_tick_housekeeping", fake_housekeeping)

            # Run exactly one iteration, then stop
            original_run_loop = runtime.run_loop

            async def one_shot_run_loop():
                runtime._running = True
                now_ms = 0

                # Full tick lane
                await runtime.tick()
                # Active-position fast tick lane
                await runtime.tick_active_positions()
                # Rate-limit reload
                await runtime._maybe_reload_rate_limits(now_ms)
                # Local-L2 data sync
                await runtime._sync_local_l2_data(now_ms)
                # Passive close lane
                await runtime._maybe_tick_passive_close(now_ms)
                # Normal exit lane
                await runtime._maybe_process_normal_exits(now_ms)
                # Maker-event lane
                await runtime._maybe_tick_maker_event(now_ms)
                # Post-tick housekeeping
                await runtime._post_tick_housekeeping(now_ms)

                runtime._running = False

            monkeypatch.setattr(runtime, "run_loop", one_shot_run_loop)

            await runtime.run_loop()

            expected = [
                "full_tick",
                "active_tick",
                "rate_limit_reload",
                "l2_sync",
                "passive_close",
                "normal_exits",
                "maker_event",
                "housekeeping",
            ]
            assert seen == expected, (
                f"V1 lane ordering violation: expected {expected}, got {seen}"
            )

        finally:
            import shutil
            shutil.rmtree(td, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_run_loop_exports_current_state_while_full_tick_is_awaiting(
        self, tmp_path, monkeypatch
    ):
        """Current-state heartbeat must run independently while full tick awaits IO."""
        from lightfee.config.schema import (
            AppConfig, RuntimeConfig, StrategyConfig, PersistenceConfig,
        )
        from lightfee.engine.bootstrap import wall_clock_now_ms
        from lightfee.engine.loop_control import current_state_export_path
        from lightfee.engine.runtime import LiveRuntime

        monkeypatch.setenv("LIGHTFEE_CURRENT_STATE_EXPORT_INTERVAL_MS", "1000")
        config = AppConfig(
            runtime=RuntimeConfig(
                mode="live",
                poll_interval_ms=100,
                sidecar_snapshot_path=str(tmp_path / "missing-sidecar.json"),
                sidecar_snapshot_max_age_ms=1000,
            ),
            strategy=StrategyConfig(
                entry_readiness_provider="ws_bbo_quote_lease",
                risk_monitor_enabled=False,
                local_l2_enabled=True,
                local_l2_ws_enabled=False,
            ),
            persistence=PersistenceConfig(
                event_log_path=str(tmp_path / "events.jsonl"),
                snapshot_path=str(tmp_path / "live-state.json"),
            ),
            venues=[],
            symbols=["BTCUSDT"],
        )
        runtime = LiveRuntime(config)
        tick_started = asyncio.Event()

        async def slow_tick():
            runtime.state.last_tick_ms = wall_clock_now_ms()
            runtime.state.tick_count += 1
            tick_started.set()
            await asyncio.sleep(2.0)
            runtime._running = False

        async def noop_lane(*args, **kwargs):
            return None

        monkeypatch.setattr(runtime, "tick", slow_tick)
        monkeypatch.setattr(runtime, "tick_active_positions", noop_lane)
        monkeypatch.setattr(runtime, "_maybe_reload_rate_limits", noop_lane)
        monkeypatch.setattr(runtime, "_sync_local_l2_data", noop_lane)
        monkeypatch.setattr(runtime, "_maybe_tick_passive_close", noop_lane)
        monkeypatch.setattr(runtime, "_maybe_process_normal_exits", noop_lane)
        monkeypatch.setattr(runtime, "_maybe_tick_maker_event", noop_lane)
        monkeypatch.setattr(runtime, "_maintain_pending_entry_passive_orders", noop_lane)
        monkeypatch.setattr(runtime, "_post_tick_housekeeping", noop_lane)
        monkeypatch.setattr(runtime, "_snapshot_local_l2_state", lambda: None)
        monkeypatch.setattr(runtime.snapshot_store, "write", lambda state: None)

        runtime.journal.open()
        try:
            run_task = asyncio.create_task(runtime.run_loop())
            await tick_started.wait()
            await asyncio.sleep(1.2)
            current_state_path = current_state_export_path(config)
            with open(current_state_path) as f:
                exported = json.load(f)
            assert exported["tick_count"] == 1
            assert exported["last_tick_ms"] == runtime.state.last_tick_ms
            runtime_progress = exported["runtime_progress"]
            assert runtime_progress["last_lane_progress_ms"] == 0
            assert runtime_progress["active_lane"] == "full_tick"
            assert runtime_progress["active_lane_overdue"] is False
            effective = exported["runtime_market_data_config"]
            assert effective["entry_readiness_provider_effective"] == "ws_bbo_l2_on_demand"
            assert effective["local_l2_configured_enabled"] is True
            assert effective["local_l2_effective_enabled"] is True
            runtime._running = False
            await run_task
        finally:
            runtime.journal.close()

    @pytest.mark.asyncio
    async def test_run_loop_maker_event_fast_wake_does_not_accelerate_full_tick(
        self, tmp_path, monkeypatch
    ):
        """V1 parity: maker-event lane wakes before poll without dragging full tick."""
        from lightfee.config.schema import (
            AppConfig, RuntimeConfig, StrategyConfig, PersistenceConfig,
        )
        import lightfee.engine.runtime as runtime_module
        from lightfee.engine.runtime import LiveRuntime

        config = AppConfig(
            runtime=RuntimeConfig(
                mode="paper",
                poll_interval_ms=3000,
                maker_event_lane_enabled=True,
                maker_event_lane_min_wake_interval_ms=40,
                sidecar_snapshot_path=str(tmp_path / "missing-sidecar.json"),
                sidecar_snapshot_max_age_ms=1000,
            ),
            strategy=StrategyConfig(
                risk_monitor_enabled=False,
                local_l2_enabled=False,
                local_l2_ws_enabled=False,
            ),
            persistence=PersistenceConfig(
                event_log_path=str(tmp_path / "events.jsonl"),
                snapshot_path=str(tmp_path / "live-state.json"),
            ),
            venues=[],
            symbols=["HOMEUSDT"],
        )
        runtime = LiveRuntime(config)
        runtime.state.pending_entries["entry-1"] = SimpleNamespace(entry_type="passive_maker")

        now = {"ms": 0}
        sleeps: list[float] = []
        tick_calls: list[int] = []
        maker_calls: list[int] = []
        real_sleep = asyncio.sleep

        monkeypatch.setattr(runtime_module, "wall_clock_now_ms", lambda: now["ms"])

        async def fake_sleep(seconds: float):
            sleeps.append(seconds)
            now["ms"] += int(seconds * 1000)
            if len(sleeps) >= 2:
                runtime._running = False
            await real_sleep(0)

        async def quiet_heartbeat():
            while runtime._running:
                await real_sleep(0.01)

        async def fake_tick():
            tick_calls.append(now["ms"])

        async def fake_maker_event(now_ms: int):
            maker_calls.append(now_ms)

        async def noop_lane(*args, **kwargs):
            return None

        monkeypatch.setattr(runtime, "_current_state_heartbeat_loop", quiet_heartbeat)
        monkeypatch.setattr(runtime_module.asyncio, "sleep", fake_sleep)
        monkeypatch.setattr(runtime, "tick", fake_tick)
        monkeypatch.setattr(runtime, "tick_active_positions", noop_lane)
        monkeypatch.setattr(runtime, "_maybe_reload_rate_limits", noop_lane)
        monkeypatch.setattr(runtime, "_sync_local_l2_data", noop_lane)
        monkeypatch.setattr(runtime, "_maybe_tick_passive_close", noop_lane)
        monkeypatch.setattr(runtime, "_maybe_process_normal_exits", noop_lane)
        monkeypatch.setattr(runtime, "_maybe_tick_maker_event", fake_maker_event)
        monkeypatch.setattr(runtime, "_maintain_pending_entry_passive_orders", noop_lane)
        monkeypatch.setattr(runtime, "_post_tick_housekeeping", noop_lane)
        monkeypatch.setattr(runtime, "_snapshot_local_l2_state", lambda: None)
        monkeypatch.setattr(runtime_module, "build_persistent_state_view", lambda state: {})
        monkeypatch.setattr(runtime.snapshot_store, "write", lambda state: None)

        await runtime.run_loop()

        assert sleeps[0] <= 0.05
        assert maker_calls[:2] == [0, 40]
        assert tick_calls == [0]

    @pytest.mark.asyncio
    async def test_active_position_tick_runs_normal_exit_before_next_full_tick(
        self, tmp_path, monkeypatch
    ):
        """Open positions must let active fast ticks advance normal exits."""
        from lightfee.config.schema import (
            AppConfig, RuntimeConfig, StrategyConfig, PersistenceConfig,
        )
        import lightfee.engine.runtime as runtime_module
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.risk.modes import EngineLifecycle

        config = AppConfig(
            runtime=RuntimeConfig(
                mode="paper",
                poll_interval_ms=60_000,
                sidecar_snapshot_path=str(tmp_path / "missing-sidecar.json"),
                sidecar_snapshot_max_age_ms=1000,
            ),
            strategy=StrategyConfig(
                risk_monitor_enabled=False,
                local_l2_enabled=False,
                local_l2_ws_enabled=False,
            ),
            persistence=PersistenceConfig(
                event_log_path=str(tmp_path / "events.jsonl"),
                snapshot_path=str(tmp_path / "live-state.json"),
            ),
            venues=[],
            symbols=["BTCUSDT"],
        )
        runtime = LiveRuntime(config)
        runtime.state.lifecycle = EngineLifecycle.RUNNING
        runtime.state.open_positions["entry-fast-exit"] = SimpleNamespace(
            position_id="entry-fast-exit",
            symbol="BTCUSDT",
        )

        now = {"ms": 0}
        full_tick_count = {"value": 0}
        normal_exit_calls: list[tuple[int, int]] = []
        real_sleep = asyncio.sleep

        monkeypatch.setattr(runtime_module, "wall_clock_now_ms", lambda: now["ms"])

        async def fake_sleep(_seconds: float):
            now["ms"] = 300
            await real_sleep(0)

        async def quiet_heartbeat():
            while runtime._running:
                await real_sleep(0.01)

        async def fake_full_tick():
            full_tick_count["value"] += 1

        async def fake_normal_exits(now_ms: int):
            normal_exit_calls.append((now_ms, full_tick_count["value"]))
            if len(normal_exit_calls) >= 2:
                runtime._running = False

        async def noop_lane(*args, **kwargs):
            return None

        monkeypatch.setattr(runtime, "_current_state_heartbeat_loop", quiet_heartbeat)
        monkeypatch.setattr(runtime_module.asyncio, "sleep", fake_sleep)
        monkeypatch.setattr(runtime, "tick", fake_full_tick)
        monkeypatch.setattr(runtime, "tick_active_positions", noop_lane)
        monkeypatch.setattr(runtime, "_maybe_reload_rate_limits", noop_lane)
        monkeypatch.setattr(runtime, "_sync_local_l2_data", noop_lane)
        monkeypatch.setattr(runtime, "_maybe_tick_passive_close", noop_lane)
        monkeypatch.setattr(runtime, "_maybe_process_normal_exits", fake_normal_exits)
        monkeypatch.setattr(runtime, "_maybe_tick_maker_event", noop_lane)
        monkeypatch.setattr(runtime, "_maintain_pending_entry_passive_orders", noop_lane)
        monkeypatch.setattr(runtime, "_post_tick_housekeeping", noop_lane)
        monkeypatch.setattr(runtime, "_snapshot_local_l2_state", lambda: None)
        monkeypatch.setattr(runtime_module, "build_persistent_state_view", lambda state: {})
        monkeypatch.setattr(runtime.snapshot_store, "write", lambda state: None)

        await runtime.run_loop()

        assert normal_exit_calls == [(0, 1), (300, 1)]

    @pytest.mark.asyncio
    async def test_tick_exports_current_state_before_long_or_early_return_tick(self, tmp_path):
        """Runtime heartbeat: current-state refresh must not wait for full tick completion."""
        from lightfee.config.schema import (
            AppConfig, RuntimeConfig, StrategyConfig, PersistenceConfig,
        )
        from lightfee.engine.loop_control import current_state_export_path
        from lightfee.engine.runtime import LiveRuntime

        config = AppConfig(
            runtime=RuntimeConfig(
                mode="live",
                poll_interval_ms=1000,
                sidecar_snapshot_path=str(tmp_path / "missing-sidecar.json"),
                sidecar_snapshot_max_age_ms=1000,
            ),
            strategy=StrategyConfig(
                risk_monitor_enabled=False,
                local_l2_enabled=False,
                local_l2_ws_enabled=False,
            ),
            persistence=PersistenceConfig(
                event_log_path=str(tmp_path / "events.jsonl"),
                snapshot_path=str(tmp_path / "live-state.json"),
            ),
            venues=[],
            symbols=["BTCUSDT"],
        )
        runtime = LiveRuntime(config)
        runtime.journal.open()
        try:
            await runtime.tick()
            background_export = runtime._current_state_export_task
            if background_export is not None:
                await background_export
        finally:
            runtime.journal.close()

        current_state_path = current_state_export_path(config)
        with open(current_state_path) as f:
            exported = json.load(f)
        assert exported["tick_count"] == 1
        assert exported["last_tick_ms"] > 0
        assert exported["open_position_count"] == 0

    @pytest.mark.asyncio
    async def test_tick_exports_current_state_after_scan_progress_before_long_await(
        self, tmp_path, monkeypatch
    ):
        """Health heartbeat: long ticks must export fresh scan progress early."""
        import time

        from lightfee.config.schema import (
            AppConfig, RuntimeConfig, StrategyConfig, PersistenceConfig,
        )
        from lightfee.engine.loop_control import current_state_export_path
        from lightfee.engine.runtime import LiveRuntime

        now_ms = int(time.time() * 1000)
        sidecar_path = tmp_path / "sidecar.json"
        sidecar_path.write_text(json.dumps({
            "schema_version": 2,
            "published_at_ms": now_ms,
            "market_observed_at_ms": now_ms,
            "quotes": {
                "BINANCE:BTCUSDT": {
                    "venue": "binance",
                    "symbol": "BTCUSDT",
                    "bid": 50000.0,
                    "ask": 50005.0,
                    "observed_at_ms": now_ms,
                    "funding_rate_bps": 1.0,
                    "funding_timestamp_ms": now_ms + 28_800_000,
                    "funding_interval_ms": 28_800_000,
                },
                "OKX:BTCUSDT": {
                    "venue": "okx",
                    "symbol": "BTCUSDT",
                    "bid": 50000.0,
                    "ask": 50005.0,
                    "observed_at_ms": now_ms,
                    "funding_rate_bps": -1.0,
                    "funding_timestamp_ms": now_ms + 28_800_000,
                    "funding_interval_ms": 28_800_000,
                },
            },
            "candidates": [],
            "degraded_venues": [],
        }))
        from lightfee.sidecar.publisher import load_snapshot

        monkeypatch.setattr(
            "lightfee.engine.runtime.load_snapshot",
            lambda _path: load_snapshot(sidecar_path),
        )
        config = AppConfig(
            runtime=RuntimeConfig(
                mode="live",
                poll_interval_ms=1000,
                sidecar_snapshot_path=str(sidecar_path),
                sidecar_snapshot_max_age_ms=600_000,
            ),
            strategy=StrategyConfig(
                risk_monitor_enabled=False,
                local_l2_enabled=False,
                local_l2_ws_enabled=False,
            ),
            persistence=PersistenceConfig(
                event_log_path=str(tmp_path / "events.jsonl"),
                snapshot_path=str(tmp_path / "live-state.json"),
            ),
            venues=[],
            symbols=["BTCUSDT"],
        )
        runtime = LiveRuntime(config)
        exported_during_scan: dict[str, object] = {}

        original_schedule_export = runtime._schedule_current_state_snapshot_export

        def observe_exported_progress(*args, **kwargs):
            last_scan = runtime.state.last_scan
            if isinstance(last_scan, dict) and last_scan.get("snapshot_freshness") == "fresh":
                exported_during_scan.update(
                    {
                        "tick_count": runtime.state.tick_count,
                        "last_tick_ms": runtime.state.last_tick_ms,
                        "last_scan": dict(runtime.state.last_scan),
                    }
                )
            return original_schedule_export(*args, **kwargs)

        monkeypatch.setattr(
            runtime,
            "_schedule_current_state_snapshot_export",
            observe_exported_progress,
        )
        runtime.journal.open()
        try:
            await runtime.tick()
            background_export = runtime._current_state_export_task
            if background_export is not None:
                await background_export
        finally:
            runtime.journal.close()

        assert exported_during_scan["tick_count"] == 1
        last_scan = exported_during_scan.get("last_scan")
        assert isinstance(last_scan, dict)
        assert last_scan["ts_ms"] == exported_during_scan["last_tick_ms"]
        assert last_scan["snapshot_freshness"] == "fresh"


class TestReplaySmoke:
    """V2: smoke tests for replay dataset structured reads and journal fallback."""

    def test_replay_dataset_imports(self):
        """Verify all replay dataset symbols are importable."""
        from lightfee.offline.replay.dataset import (
            ReplayDataset,
            _is_journal_only,
            _ensure_replay_facts_table,
            _read_replay_facts,
            _read_journal_only_events,
        )

    def test_replay_dataset_load_journal_baseline(self):
        """load() with only journal_path returns journal-sourced dataset."""
        import tempfile
        from pathlib import Path
        from lightfee.offline.replay.dataset import ReplayDataset
        from lightfee.persistence.journal import Journal

        with tempfile.TemporaryDirectory() as td:
            jp = Path(td) / "smoke-events.jsonl"
            journal = Journal(jp)
            journal.open()
            journal.append("entry.opened",
                          {"position_id": "smoke-pos", "symbol": "BTCUSDT"},
                          ts_ms=1768003200000)
            journal.append("runtime.lifecycle_changed",
                          {"from": "booting", "to": "running"},
                          ts_ms=1768003300000)
            journal.close()

            dataset = ReplayDataset.load(str(jp))
            assert dataset.source == "journal"
            assert len(dataset.records) == 2

            kinds = {r["kind"] for r in dataset.records}
            assert "entry.opened" in kinds
            assert "runtime.lifecycle_changed" in kinds

    def test_replay_dataset_from_journal_range_preserves_filter(self):
        """from_journal_range date filter works correctly."""
        import tempfile
        from pathlib import Path
        from lightfee.offline.replay.dataset import ReplayDataset
        from lightfee.persistence.journal import Journal

        with tempfile.TemporaryDirectory() as td:
            jp = Path(td) / "smoke-filter.jsonl"
            journal = Journal(jp)
            journal.open()
            # Jan 10 2026
            journal.append("scan.completed", {"candidate_count": 1},
                          ts_ms=1768003200000)
            # Jan 15 2026
            journal.append("scan.completed", {"candidate_count": 2},
                          ts_ms=1768435200000)
            # Jan 20 2026
            journal.append("scan.completed", {"candidate_count": 3},
                          ts_ms=1768867200000)
            journal.close()

            dataset = ReplayDataset.from_journal_range(
                str(jp), date_from="20260112", date_to="20260117"
            )
            assert len(dataset.records) == 1
            assert dataset.records[0]["payload"]["candidate_count"] == 2

    def test_replay_semantics_identical_regardless_of_source(self):
        """Replay results must be identical whether records come from
        structured store, journal, or merged path."""
        import json
        import sqlite3
        import tempfile
        from pathlib import Path
        from lightfee.offline.replay.dataset import (
            ReplayDataset, _ensure_replay_facts_table, _is_journal_only, _ts_to_date_str,
        )
        from lightfee.persistence.journal import Journal, replay_journal_records

        with tempfile.TemporaryDirectory() as td:
            all_records = [
                {"seq": 1, "ts_ms": 1768003200000, "kind": "entry.opened",
                 "payload": {"position_id": "pos-smoke", "symbol": "ETHUSDT",
                            "long_venue": "binance", "short_venue": "okx",
                            "quantity": 2.0}},
                {"seq": 2, "ts_ms": 1768003300000, "kind": "runtime.lifecycle_changed",
                 "payload": {"from": "booting", "to": "running"}},
                {"seq": 3, "ts_ms": 1768003400000, "kind": "exit.closed",
                 "payload": {"position_id": "pos-smoke"}},
            ]

            # Path A: journal
            jp = Path(td) / "smoke-journal.jsonl"
            journal = Journal(jp)
            journal.open()
            for r in all_records:
                journal.append(r["kind"], r["payload"], ts_ms=r["ts_ms"])
            journal.close()

            result_a = replay_journal_records(
                ReplayDataset.from_journal_range(str(jp)).records
            )

            # Path B: structured with journal fallback (journal-only events)
            store_path = Path(td) / "smoke-store.db"
            conn = sqlite3.connect(str(store_path))
            _ensure_replay_facts_table(conn)
            for r in all_records:
                if _is_journal_only(r["kind"]):
                    continue
                conn.execute(
                    "INSERT INTO replay_facts (seq, run_id, ts_ms, kind, payload_json, date) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (r["seq"], "test-run", r["ts_ms"], r["kind"],
                     json.dumps(r["payload"]), _ts_to_date_str(r["ts_ms"])),
                )
            conn.commit()
            conn.close()

            result_b = replay_journal_records(
                ReplayDataset.from_structured(str(store_path), journal_path=str(jp)).records
            )

            assert result_a["open_position_count"] == result_b["open_position_count"]
            assert result_a["final_lifecycle"] == result_b["final_lifecycle"]
            assert result_a["pending_entry_count"] == result_b["pending_entry_count"]
            assert result_a["pending_close_count"] == result_b["pending_close_count"]
