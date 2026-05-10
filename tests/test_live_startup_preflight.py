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
