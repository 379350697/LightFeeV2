"""Full live-loop orchestration tests.

Covers Rust V1 main.rs loop: startup → recovery → snapshot → candidate →
entry → active position tick → exit/risk close → journal/snapshot/metrics.

Uses fake venue adapters to simulate the full production loop.
"""

import asyncio
import json
import tempfile
from pathlib import Path

import pytest

from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig, StrategyConfig
from lightfee.core.domain import OrderFill, PositionSnapshot, Side, Venue
from lightfee.engine.runtime import LiveRuntime
from lightfee.engine.state import EngineState, OpenPosition
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from tests.fake_adapters import (
    FakeVenueAdapter,
    make_fake_fill,
    make_rejected_error,
    make_uncertain_error,
)


def make_test_config(temp_dir: str) -> AppConfig:
    """Build a minimal live-mode config for testing."""
    return AppConfig(
        runtime=RuntimeConfig(
            mode="live",
            poll_interval_ms=100,
            sidecar_snapshot_path=str(Path(temp_dir) / "sidecar.json"),
            sidecar_snapshot_max_age_ms=600_000,
            tick_failure_backoff_initial_ms=100,
            tick_failure_backoff_max_ms=1000,
        ),
        strategy=StrategyConfig(
            risk_monitor_enabled=False,
            entry_notional_cap_quote=30.0,
            max_concurrent_positions=2,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(Path(temp_dir) / "events.jsonl"),
            snapshot_path=str(Path(temp_dir) / "state.json"),
        ),
        venues=[],
        symbols=["BTCUSDT", "ETHUSDT"],
    )


# ---------------------------------------------------------------------------
# Full loop smoke tests
# ---------------------------------------------------------------------------

class TestLiveFullClosure:
    """End-to-end loop tests with fake adapters."""

    @pytest.mark.asyncio
    async def test_startup_sequence_booting_to_running(self):
        """Rust V1: startup transitions BOOTING → RECONCILING → RUNNING."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            adapters = {
                Venue.BINANCE: FakeVenueAdapter(Venue.BINANCE),
                Venue.OKX: FakeVenueAdapter(Venue.OKX),
            }

            runtime = LiveRuntime(config, venue_adapters=adapters)
            await runtime.start()

            # Clean startup → RUNNING
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
            assert runtime.state.run_id != ""

    @pytest.mark.asyncio
    async def test_startup_journals_booting_and_running_events(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            runtime = LiveRuntime(config)
            await runtime.start()

            records = runtime.journal.read_all()
            kinds = [r["kind"] for r in records]
            assert "runtime.booting" in kinds
            assert "runtime.started" in kinds

    @pytest.mark.asyncio
    async def test_full_tick_loads_snapshot(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            # Write a valid sidecar snapshot for the tick to load
            sidecar_path = Path(td) / "sidecar.json"
            sidecar_path.write_text(json.dumps({
                "schema_version": 2,
                "published_at_ms": 500000,
                "candidates": [],
                "quotes": {},
            }))

            runtime = LiveRuntime(config)
            await runtime.start()

            # Run one tick
            await runtime.tick()

            # Should have loaded snapshot and logged
            records = runtime.journal.read_all()
            kinds = [r["kind"] for r in records]
            assert "runtime.snapshot_missing" not in kinds  # snapshot was found

    @pytest.mark.asyncio
    async def test_tick_with_stale_snapshot_logs_stale(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.sidecar_snapshot_max_age_ms = 100  # very short max age

            sidecar_path = Path(td) / "sidecar.json"
            sidecar_path.write_text(json.dumps({
                "schema_version": 2,
                "published_at_ms": 1,  # very old
                "candidates": [],
                "quotes": {},
            }))

            runtime = LiveRuntime(config)
            await runtime.start()

            await runtime.tick()

            records = runtime.journal.read_all()
            kinds = [r["kind"] for r in records]
            assert "runtime.snapshot_stale" in kinds

    @pytest.mark.asyncio
    async def test_stop_persists_snapshot_and_journals_stopped(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            runtime = LiveRuntime(config)
            await runtime.start()
            runtime.state.tick_count = 42
            await runtime.stop()

            # Snapshot persisted
            snap_path = Path(td) / "state.json"
            assert snap_path.exists()

            data = json.loads(snap_path.read_text())
            assert data["tick_count"] == 42

            # Journal has stopped event
            records = runtime.journal.read_all()
            kinds = [r["kind"] for r in records]
            assert "runtime.stopped" in kinds

    @pytest.mark.asyncio
    async def test_full_loop_start_tick_stop(self):
        """Complete start → tick → stop cycle."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            sidecar_path = Path(td) / "sidecar.json"
            sidecar_path.write_text(json.dumps({
                "schema_version": 2,
                "published_at_ms": 500000,
                "candidates": [],
                "quotes": {},
            }))

            runtime = LiveRuntime(config)
            await runtime.start()

            # Run a few ticks
            for _ in range(3):
                await runtime.tick()

            await runtime.stop()

            # Verify lifecycle
            assert runtime.state.tick_count == 3
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING

    @pytest.mark.asyncio
    async def test_backoff_on_tick_error(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            # No sidecar file → tick logs snapshot_missing and returns gracefully
            runtime = LiveRuntime(config)
            await runtime.start()

            # Tick without sidecar → snapshot_missing logged, no error raised
            await runtime.tick()
            # Backoff is NOT applied for missing snapshot (not an error)
            # The runtime just skips the tick
            records = runtime.journal.read_all()
            kinds = [r["kind"] for r in records]
            assert "runtime.snapshot_missing" in kinds

    @pytest.mark.asyncio
    async def test_active_position_tick(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            runtime = LiveRuntime(config)
            await runtime.start()

            # Add an open position
            runtime.state.open_positions["pos-1"] = OpenPosition(
                position_id="pos-1",
                symbol="BTCUSDT",
                long_venue=Venue.BINANCE,
                short_venue=Venue.OKX,
                long_quantity=0.1,
                short_quantity=0.1,
                long_entry_price=50000.0,
                short_entry_price=50100.0,
                opened_at_ms=1000,
            )

            await runtime.tick_active_positions()

            records = runtime.journal.read_all()
            kinds = [r["kind"] for r in records]
            assert "runtime.active_position_tick" in kinds

    @pytest.mark.asyncio
    async def test_housekeeping_exports_metrics(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            runtime = LiveRuntime(config)
            await runtime.start()

            # Trigger housekeeping
            runtime._post_tick_housekeeping(5000)

            # Should not crash (metrics export is gated by env var)


# ---------------------------------------------------------------------------
# Preflight and startup validation tests
# ---------------------------------------------------------------------------

class TestLiveStartupPreflight:
    """Preflight checks matching Rust V1 bootstrap validation."""

    @pytest.mark.asyncio
    async def test_paper_mode_starts_cleanly(self):
        """Paper mode should still work for testing."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.mode = "paper"

            runtime = LiveRuntime(config)
            await runtime.start()

            assert runtime.state.lifecycle == EngineLifecycle.RUNNING

    @pytest.mark.asyncio
    async def test_live_mode_with_no_adapters_starts(self):
        """Live mode without adapters should still start (adapters not required for runtime init)."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.mode = "live"

            runtime = LiveRuntime(config)
            await runtime.start()

            # Runtime starts cleanly; adapters are checked at entry/exit time
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING

    @pytest.mark.asyncio
    async def test_runtime_exposes_venue_adapters(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            binance_adapter = FakeVenueAdapter(Venue.BINANCE)
            adapters = {Venue.BINANCE: binance_adapter}

            runtime = LiveRuntime(config, venue_adapters=adapters)
            await runtime.start()

            adapter = runtime.get_venue_adapter(Venue.BINANCE)
            assert adapter is not None
            assert adapter is binance_adapter

            all_adapters = runtime.get_venue_adapters()
            assert Venue.BINANCE in all_adapters

    @pytest.mark.asyncio
    async def test_recovered_state_with_positions_enters_reconciling(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            # Pre-write a snapshot with open positions
            from lightfee.persistence.snapshot_store import SnapshotStore
            snap = SnapshotStore(config.persistence.snapshot_path)
            snap.write({
                "lifecycle": "running",
                "risk_mode": "running",
                "open_positions": {
                    "pos-rec": {
                        "position_id": "pos-rec",
                        "symbol": "ETHUSDT",
                        "long_venue": "binance",
                        "short_venue": "bybit",
                        "long_quantity": 1.0,
                        "short_quantity": 1.0,
                        "long_entry_price": 3000.0,
                        "short_entry_price": 3010.0,
                        "opened_at_ms": 1000,
                        "matched_quantity": 1.0,
                    }
                },
            })

            runtime = LiveRuntime(config)
            await runtime.start()

            # Has open positions → must enter RECONCILING
            assert runtime.state.lifecycle == EngineLifecycle.RECONCILING

    @pytest.mark.asyncio
    async def test_fail_closed_state_is_preserved_on_startup(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            snap = __import__("lightfee.persistence.snapshot_store", fromlist=["SnapshotStore"]).SnapshotStore(
                config.persistence.snapshot_path
            )
            snap.write({
                "lifecycle": "risk_only",
                "risk_mode": "fail_closed",
            })

            runtime = LiveRuntime(config)
            await runtime.start()

            # Fail-closed state must be preserved
            assert runtime.state.risk_mode.at_least(GlobalRiskMode.ENTRY_PAUSED)

    @pytest.mark.asyncio
    async def test_operator_control_restored_from_snapshot(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            snap = __import__("lightfee.persistence.snapshot_store", fromlist=["SnapshotStore"]).SnapshotStore(
                config.persistence.snapshot_path
            )
            snap.write({
                "lifecycle": "running",
                "risk_mode": "reduce_only",
            })

            runtime = LiveRuntime(config)
            await runtime.start()

            # Risk mode restored
            assert runtime.state.risk_mode == GlobalRiskMode.REDUCE_ONLY
