"""Live startup preflight tests matching Rust V1 bootstrap validation.

Rust references:
- src/main.rs (main startup sequence)
- src/app_runtime/bootstrap.rs (symbol resolution, credential validation)
- src/app_runtime/services.rs (adapter construction)
"""

import asyncio
import tempfile
from pathlib import Path

import pytest

from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig, StrategyConfig
from lightfee.core.domain import Venue
from lightfee.engine.bootstrap import (
    active_position_poll_enabled,
    active_position_poll_interval_ms,
    full_tick_ready,
    prepare_runtime_symbols,
    startup_market_warmup_ms,
    wall_clock_now_ms,
)
from lightfee.engine.runtime import LiveRuntime
from lightfee.risk.modes import EngineLifecycle
from tests.fake_adapters import FakeVenueAdapter


def make_test_config(temp_dir: str) -> AppConfig:
    return AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            poll_interval_ms=200,
            sidecar_snapshot_path=str(Path(temp_dir) / "sidecar.json"),
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
            event_log_path=str(Path(temp_dir) / "events.jsonl"),
            snapshot_path=str(Path(temp_dir) / "state.json"),
        ),
        venues=[],
        symbols=["BTCUSDT"],
    )


class TestBootstrapHelpers:
    """Test bootstrap utility functions."""

    def test_wall_clock_now_ms_is_recent(self):
        now = wall_clock_now_ms()
        import time
        expected = int(time.time() * 1000)
        assert abs(now - expected) < 5000

    def test_full_tick_ready_no_backoff(self):
        assert full_tick_ready(None, 1000)

    def test_full_tick_ready_past_deadline(self):
        assert full_tick_ready(500, 1000)

    def test_full_tick_ready_before_deadline(self):
        assert not full_tick_ready(1500, 1000)

    def test_active_position_poll_interval_with_positions(self):
        interval = active_position_poll_interval_ms(EngineLifecycle.RUNNING, 3000, 1)
        assert interval <= 250

    def test_active_position_poll_interval_without_positions(self):
        interval = active_position_poll_interval_ms(EngineLifecycle.RUNNING, 3000, 0)
        assert interval == 3000

    def test_active_position_poll_enabled(self):
        # Fast poll enabled when lifecycle RUNNING + positions > 0 + fast < poll
        # poll_interval must be > 250 for fast poll to be faster
        assert active_position_poll_enabled(EngineLifecycle.RUNNING, 3000, 1)
        assert not active_position_poll_enabled(EngineLifecycle.RUNNING, 3000, 0)
        assert not active_position_poll_enabled(EngineLifecycle.BOOTING, 3000, 1)

    def test_startup_market_warmup_without_positions(self):
        warmup = startup_market_warmup_ms(
            EngineLifecycle.RUNNING, True, 0, 3000
        )
        assert warmup is not None
        assert 3000 <= warmup <= 10000

    def test_startup_market_warmup_skipped_with_positions(self):
        warmup = startup_market_warmup_ms(
            EngineLifecycle.RUNNING, True, 1, 3000
        )
        assert warmup is None

    @pytest.mark.asyncio
    async def test_prepare_runtime_symbols_returns_passthrough(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            result = await prepare_runtime_symbols(config)
            assert result is not None
            assert result["resolved_symbol_count"] == 1
            assert "BTCUSDT" in result["resolved_symbols"]


class TestRuntimePreflight:
    """Preflight checks before live trading starts."""

    @pytest.mark.asyncio
    async def test_startup_with_valid_config(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config)
            await runtime.start()
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING

    @pytest.mark.asyncio
    async def test_startup_journals_run_id(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config)
            await runtime.start()

            # Run ID is a non-empty timestamp string
            assert runtime.state.run_id
            assert len(runtime.state.run_id) > 0
            assert runtime.state.started_at_ms > 0

    @pytest.mark.asyncio
    async def test_shutdown_graceful(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config)
            await runtime.start()
            await runtime.stop()

            # Journal is closed after stop
            assert runtime.journal._file is None

    @pytest.mark.asyncio
    async def test_shutdown_calls_per_adapter_shutdown(self):
        """V1 parity: LiveRuntime.stop() calls shutdown() on each venue adapter."""
        shutdown_calls: list[str] = []

        class ShutdownTrackingAdapter(FakeVenueAdapter):
            async def shutdown(self) -> None:
                shutdown_calls.append(self._venue.value)

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            adapters = {
                Venue.BINANCE: ShutdownTrackingAdapter(Venue.BINANCE),
                Venue.OKX: ShutdownTrackingAdapter(Venue.OKX),
                Venue.HYPERLIQUID: ShutdownTrackingAdapter(Venue.HYPERLIQUID),
            }
            runtime = LiveRuntime(config, venue_adapters=adapters)
            await runtime.start()
            await runtime.stop()

            assert sorted(shutdown_calls) == ["binance", "hyperliquid", "okx"]

    @pytest.mark.asyncio
    async def test_shutdown_adapter_error_does_not_block(self):
        """V1 parity: adapter shutdown errors are journaled, not re-raised."""
        class FailingShutdownAdapter(FakeVenueAdapter):
            async def shutdown(self) -> None:
                raise RuntimeError("adapter shutdown failure")

        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            adapters = {Venue.BINANCE: FailingShutdownAdapter(Venue.BINANCE)}
            runtime = LiveRuntime(config, venue_adapters=adapters)
            await runtime.start()
            # Must not raise — error is journaled
            await runtime.stop()

            assert runtime.journal._file is None

    @pytest.mark.asyncio
    async def test_venue_adapters_accessible(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            adapters = {
                Venue.BINANCE: FakeVenueAdapter(Venue.BINANCE),
            }
            runtime = LiveRuntime(config, venue_adapters=adapters)
            await runtime.start()

            adapter = runtime.get_venue_adapter(Venue.BINANCE)
            assert adapter is not None

    @pytest.mark.asyncio
    async def test_missing_venue_adapter_returns_none(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config)
            await runtime.start()

            adapter = runtime.get_venue_adapter(Venue.HYPERLIQUID)
            assert adapter is None


class TestRateLimitConfigManagerStartup:
    """Verify live startup constructs RateLimitConfigManager with correct parameter name."""

    def test_rate_limit_config_manager_accepts_config_path_param(self):
        from lightfee.rate_limit.config import RateLimitConfigManager
        import tempfile, os

        with tempfile.TemporaryDirectory() as td:
            rl_path = os.path.join(td, "rate_limits.toml")
            with open(rl_path, "w") as f:
                f.write("[global]\ndefault_margin = 0.95\n")

            mgr = RateLimitConfigManager(config_path=rl_path)
            assert mgr.config.default_margin == 0.95
            outcome = mgr.refresh()
            assert outcome in ("reloaded", "unchanged")

    def test_rate_limit_config_manager_path_is_stored(self):
        from lightfee.rate_limit.config import RateLimitConfigManager

        mgr = RateLimitConfigManager(config_path="/tmp/test_limits.toml")
        assert mgr.path == "/tmp/test_limits.toml"

    def test_rate_limit_config_manager_no_path_uses_defaults(self):
        from lightfee.rate_limit.config import RateLimitConfigManager

        mgr = RateLimitConfigManager()
        assert mgr.path is None
        assert mgr.config.default_margin == 0.95
        # Refresh should be a no-op without path
        outcome = mgr.refresh()
        assert outcome == "unchanged"


class TestLiveMainStartupShutdownOrder:
    """V1 parity: startup always calls start before stop, stop always fires on exit."""

    @pytest.mark.asyncio
    async def test_live_main_calls_start_then_stop(self, monkeypatch):
        """V1: async_main calls LiveRuntime.start() then LiveRuntime.stop() in order."""
        calls: list[str] = []

        async def fake_start(self) -> None:
            calls.append("start")

        async def fake_stop(self) -> None:
            calls.append("stop")

        async def fake_run_loop(self) -> None:
            calls.append("run_loop")
            self._running = False

        monkeypatch.setattr(
            "lightfee.apps.live.LiveRuntime.start", fake_start
        )
        monkeypatch.setattr(
            "lightfee.apps.live.LiveRuntime.stop", fake_stop
        )
        monkeypatch.setattr(
            "lightfee.apps.live.LiveRuntime.run_loop", fake_run_loop
        )

        import tempfile
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            # Patch load_config to return the in-memory config directly
            monkeypatch.setattr(
                "lightfee.apps.live.load_config", lambda _path: config
            )
            from lightfee.apps.live import async_main
            await async_main("test.toml")

        assert calls == ["start", "run_loop", "stop"], (
            f"V1 parity violation: expected start→run_loop→stop, got {calls}"
        )

    @pytest.mark.asyncio
    async def test_live_main_stop_always_called_on_keyboard_interrupt(self, monkeypatch):
        """V1: async_main calls stop() even when KeyboardInterrupt fires during run_loop."""
        calls: list[str] = []

        async def fake_start(self) -> None:
            calls.append("start")

        async def fake_stop(self) -> None:
            calls.append("stop")

        async def fake_run_loop(self) -> None:
            calls.append("run_loop")
            raise KeyboardInterrupt()

        monkeypatch.setattr(
            "lightfee.apps.live.LiveRuntime.start", fake_start
        )
        monkeypatch.setattr(
            "lightfee.apps.live.LiveRuntime.stop", fake_stop
        )
        monkeypatch.setattr(
            "lightfee.apps.live.LiveRuntime.run_loop", fake_run_loop
        )

        import tempfile
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            monkeypatch.setattr(
                "lightfee.apps.live.load_config", lambda _path: config
            )
            from lightfee.apps.live import async_main
            await async_main("test.toml")

        assert "start" in calls, f"start was never called: {calls}"
        assert "stop" in calls, (
            f"V1 parity violation: stop() must be called even after KeyboardInterrupt, got {calls}"
        )
