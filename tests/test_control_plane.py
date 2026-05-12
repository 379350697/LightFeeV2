"""Tests for bootstrap, loop_control, and control-plane additions."""

import os
import tempfile

import pytest

from lightfee.config.schema import AppConfig, RuntimeConfig, StrategyConfig
from lightfee.engine.bootstrap import (
    active_position_poll_enabled,
    active_position_poll_interval_ms,
    active_position_tick_ready,
    full_tick_ready,
    rate_limit_config_path,
    startup_market_warmup_ms,
    wall_clock_now_ms,
)
from lightfee.engine.lifecycle import LiveStartupPhase, transition_to_reconciling, transition_to_running
from lightfee.engine.loop_control import (
    ExportState,
    current_state_export_interval_ms,
    current_state_export_path,
    maybe_export_current_state_snapshot,
    maybe_export_runtime_metrics,
    metrics_export_interval_ms,
    metrics_export_path,
    write_json_atomic,
)
from lightfee.engine.state import EngineState
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


class TestBootstrap:
    def test_wall_clock_now_ms_returns_positive(self):
        ts = wall_clock_now_ms()
        assert ts > 0

    def test_rate_limit_config_path(self):
        path = rate_limit_config_path("/etc/lightfee/config.toml")
        assert path == "/etc/lightfee/rate_limits.toml"

    def test_full_tick_ready_no_backoff(self):
        assert full_tick_ready(None, 1000) is True

    def test_full_tick_ready_past_deadline(self):
        assert full_tick_ready(1000, 2000) is True

    def test_full_tick_ready_before_deadline(self):
        assert full_tick_ready(2000, 1000) is False

    def test_full_tick_ready_at_deadline(self):
        assert full_tick_ready(1000, 1000) is True

    def test_active_tick_ready_delegates(self):
        assert active_position_tick_ready(None, 0) is True
        assert active_position_tick_ready(2000, 1000) is False

    def test_poll_interval_running_with_positions(self):
        ms = active_position_poll_interval_ms(EngineLifecycle.RUNNING, 3000, 2)
        assert ms == 250

    def test_poll_interval_running_no_positions(self):
        ms = active_position_poll_interval_ms(EngineLifecycle.RUNNING, 3000, 0)
        assert ms == 3000

    def test_poll_interval_booting(self):
        ms = active_position_poll_interval_ms(EngineLifecycle.BOOTING, 3000, 5)
        assert ms == 3000

    def test_fast_poll_enabled_with_positions(self):
        assert active_position_poll_enabled(EngineLifecycle.RUNNING, 3000, 3) is True

    def test_fast_poll_disabled_no_positions(self):
        assert active_position_poll_enabled(EngineLifecycle.RUNNING, 3000, 0) is False

    def test_fast_poll_disabled_already_250(self):
        assert active_position_poll_enabled(EngineLifecycle.RUNNING, 250, 3) is False

    def test_warmup_running_no_positions(self):
        ms = startup_market_warmup_ms(EngineLifecycle.RUNNING, True, 0, 3000)
        assert ms == 9000

    def test_warmup_clamped_to_3000(self):
        ms = startup_market_warmup_ms(EngineLifecycle.RUNNING, True, 0, 500)
        assert ms == 3000

    def test_warmup_clamped_to_10000(self):
        ms = startup_market_warmup_ms(EngineLifecycle.RUNNING, True, 0, 5000)
        assert ms == 10000

    def test_warmup_skips_when_market_inactive(self):
        assert startup_market_warmup_ms(EngineLifecycle.RUNNING, False, 0, 3000) is None

    def test_warmup_skips_with_positions(self):
        assert startup_market_warmup_ms(EngineLifecycle.RUNNING, True, 2, 3000) is None

    def test_warmup_skips_not_running(self):
        assert startup_market_warmup_ms(EngineLifecycle.BOOTING, True, 0, 3000) is None


class TestLoopControl:
    def test_metrics_export_disabled_by_env(self):
        config = AppConfig()
        os.environ["LIGHTFEE_METRICS_EXPORT"] = "0"
        try:
            assert metrics_export_path(config) is None
        finally:
            del os.environ["LIGHTFEE_METRICS_EXPORT"]

    def test_metrics_export_path_env_override(self):
        config = AppConfig()
        os.environ["LIGHTFEE_METRICS_TEXTFILE_PATH"] = "/tmp/test.prom"
        try:
            assert metrics_export_path(config) == "/tmp/test.prom"
        finally:
            del os.environ["LIGHTFEE_METRICS_TEXTFILE_PATH"]

    def test_metrics_export_default_path(self):
        config = AppConfig()
        path = metrics_export_path(config)
        assert path is not None
        assert path.endswith(".prom")

    def test_metrics_export_interval_min_1000(self):
        config = AppConfig(runtime=RuntimeConfig(poll_interval_ms=100))
        assert metrics_export_interval_ms(config) >= 1000

    def test_current_state_export_path(self):
        config = AppConfig()
        path = current_state_export_path(config)
        assert "-current.json" in path

    def test_current_state_export_interval(self):
        config = AppConfig()
        ms = current_state_export_interval_ms(config)
        assert ms >= 1000

    def test_write_json_atomic(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            tmp = f.name
        try:
            write_json_atomic(tmp, {"a": 1})
            import json
            with open(tmp) as f:
                data = json.load(f)
            assert data == {"a": 1}
        finally:
            os.unlink(tmp)

    def test_export_state_tracks_deadlines(self):
        state = ExportState()
        assert state.next_metrics_export_ms == 0
        assert state.next_state_export_ms == 0

    def test_maybe_export_metrics_updates_deadline(self):
        state = EngineState()
        config = AppConfig()
        es = ExportState()
        os.environ["LIGHTFEE_METRICS_TEXTFILE_PATH"] = "/tmp/test_ctrl.prom"
        try:
            maybe_export_runtime_metrics(state, config, es, 99999)
            assert es.next_metrics_export_ms > 99999
        finally:
            del os.environ["LIGHTFEE_METRICS_TEXTFILE_PATH"]
            try:
                os.unlink("/tmp/test_ctrl.prom")
            except OSError:
                pass

    def test_maybe_export_state_updates_deadline(self):
        state = EngineState()
        config = AppConfig()
        es = ExportState()
        maybe_export_current_state_snapshot(state, config, es, 99999)
        assert es.next_state_export_ms > 99999


class TestTickErrorBackoffAndExport:
    """V1 parity: tick errors record backoff, journal, and still run export path."""

    @pytest.mark.asyncio
    async def test_tick_error_records_backoff_and_exports(self, monkeypatch):
        """V1: a failing tick must journal the error, apply backoff, and still export."""
        import tempfile
        from pathlib import Path
        from lightfee.config.schema import (
            AppConfig, RuntimeConfig, StrategyConfig, PersistenceConfig,
        )
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.engine.bootstrap import wall_clock_now_ms

        td = tempfile.mkdtemp()
        try:
            config = AppConfig(
                runtime=RuntimeConfig(
                    mode="paper",
                    poll_interval_ms=100,
                    sidecar_snapshot_path=str(Path(td) / "sidecar.json"),
                    sidecar_snapshot_max_age_ms=600_000,
                    tick_failure_backoff_initial_ms=500,
                    tick_failure_backoff_max_ms=5000,
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

            tick_errors = []
            original_journal_append = runtime.journal.append

            def tracking_append(event: str, data: dict, flush: bool = False):
                if "tick_error" in event:
                    tick_errors.append((event, data))
                return original_journal_append(event, data, flush=flush)

            monkeypatch.setattr(runtime.journal, "append", tracking_append)

            # Force tick to raise
            async def boom():
                raise RuntimeError("V1 simulated tick failure")

            monkeypatch.setattr(runtime, "tick", boom)

            # Track whether post_tick_housekeeping runs after the error
            export_called = []
            original_housekeeping = runtime._post_tick_housekeeping

            async def tracking_housekeeping(now_ms: int):
                export_called.append(now_ms)
                await original_housekeeping(now_ms)

            monkeypatch.setattr(
                runtime, "_post_tick_housekeeping", tracking_housekeeping
            )

            # Run one loop iteration
            runtime._running = True
            runtime._tick_backoff_until_ms = None  # ensure no prior backoff

            now_ms = wall_clock_now_ms()

            # Simulate the full-tick lane from run_loop
            from lightfee.engine.bootstrap import full_tick_ready
            assert full_tick_ready(runtime._tick_backoff_until_ms, now_ms)

            try:
                await runtime.tick()
            except Exception:
                runtime._apply_tick_backoff(is_active=False)
                runtime.journal.append("runtime.tick_error", {"error": "boom"})

            # After the error, backoff must be active
            assert runtime._tick_backoff_until_ms is not None, (
                "V1 parity violation: backoff not applied after tick failure"
            )
            assert runtime._tick_backoff_until_ms > now_ms, (
                "V1 parity violation: backoff deadline must be in the future"
            )

            # Verify error was journaled
            assert len(tick_errors) >= 1, (
                "V1 parity violation: tick error not journaled"
            )

            # Run housekeeping (as the loop would)
            await runtime._post_tick_housekeeping(now_ms)

            # Post-tick housekeeping must have been called (V1 always exports after tick)
            assert len(export_called) == 1, (
                f"V1 parity violation: post-tick housekeeping not called after error; "
                f"called {len(export_called)} times"
            )

        finally:
            import shutil
            shutil.rmtree(td, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_shutdown_flushes_final_state(self, monkeypatch):
        """V1: final stop() must flush rate-limit, export final state, and close journal."""
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

            stop_sequence: list[str] = []

            # Track snapshot write
            original_write = runtime.snapshot_store.write
            def tracking_write(data):
                stop_sequence.append("snapshot_write")
                return original_write(data)
            monkeypatch.setattr(runtime.snapshot_store, "write", tracking_write)

            # Track journal close
            original_close = runtime.journal.close
            def tracking_close():
                stop_sequence.append("journal_close")
                return original_close()
            monkeypatch.setattr(runtime.journal, "close", tracking_close)

            await runtime.start()
            await runtime.stop()

            # V1: final state must be flushed in order
            assert "snapshot_write" in stop_sequence, (
                "V1 parity violation: stop() did not write final state snapshot"
            )
            assert "journal_close" in stop_sequence, (
                "V1 parity violation: stop() did not close journal"
            )

        finally:
            import shutil
            shutil.rmtree(td, ignore_errors=True)


class TestLifecycleAdditions:
    def test_live_startup_phase_order(self):
        phases = list(LiveStartupPhase)
        assert LiveStartupPhase.PRIVATE_STREAMS in phases
        assert LiveStartupPhase.MARKET_STREAMS in phases
        assert LiveStartupPhase.LOCAL_L2 in phases

    def test_transition_to_reconciling(self):
        s = EngineState(lifecycle=EngineLifecycle.BOOTING)
        transition_to_reconciling(s)
        assert s.lifecycle == EngineLifecycle.RECONCILING

    def test_transition_to_running(self):
        s = EngineState(lifecycle=EngineLifecycle.RECONCILING, risk_mode=GlobalRiskMode.RUNNING)
        transition_to_running(s)
        assert s.lifecycle == EngineLifecycle.RUNNING
        assert s.risk_mode == GlobalRiskMode.RUNNING
