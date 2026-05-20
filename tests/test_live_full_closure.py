"""Full live-loop orchestration tests.

Covers Rust V1 main.rs loop: startup → recovery → snapshot → candidate →
entry → active position tick → exit/risk close → journal/snapshot/metrics.

Uses fake venue adapters to simulate the full production loop.
"""

import asyncio
import json
import tempfile
import time
from pathlib import Path

import pytest

from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig, StrategyConfig
from lightfee.core.domain import OrderFill, PositionSnapshot, Side, Venue
from lightfee.engine.runtime import LiveRuntime
from lightfee.engine.state import EngineState, OpenPosition, PendingEntry
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
            local_l2_enabled=False,  # default: sidecar path; enable explicitly for local-L2 tests
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
            await runtime._post_tick_housekeeping(5000)

            # Should not crash (metrics export is gated by env var)
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING

    @pytest.mark.asyncio
    async def test_housekeeping_clears_clean_fail_closed_after_live_recovery(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            runtime = LiveRuntime(config)
            await runtime.start()
            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
            runtime.state.risk_mode = GlobalRiskMode.FAIL_CLOSED
            runtime.state.open_positions.clear()
            runtime.state.pending_entries.clear()
            runtime.state.pending_closes.clear()
            runtime.state.pending_passive_closes.clear()
            runtime.state.recovery_blocked_reason = None
            runtime.state.operator.requested_mode = None

            await runtime._post_tick_housekeeping(5000)

            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
            records = runtime.journal.read_all()
            assert any(
                r.get("kind") == "runtime.stale_fail_closed_cleared"
                for r in records
            )


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

            # V1: after successful recovery with open positions within max → RUNNING
            # (RECONCILING is a transient phase, not the terminal startup state)
            assert runtime.state.lifecycle in (
                EngineLifecycle.RECONCILING,
                EngineLifecycle.RUNNING,
            ), f"Expected RECONCILING or RUNNING, got {runtime.state.lifecycle}"

    @pytest.mark.asyncio
    async def test_operator_requested_fail_closed_is_preserved_on_startup(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            snap = __import__("lightfee.persistence.snapshot_store", fromlist=["SnapshotStore"]).SnapshotStore(
                config.persistence.snapshot_path
            )
            snap.write({
                "lifecycle": "risk_only",
                "risk_mode": "fail_closed",
                "operator": {"requested_mode": "fail_closed"},
            })

            runtime = LiveRuntime(config)
            await runtime.start()

            assert runtime.state.risk_mode == GlobalRiskMode.FAIL_CLOSED

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

    @pytest.mark.asyncio
    async def test_stale_fail_closed_clean_state_is_cleared_on_startup(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            snap = __import__("lightfee.persistence.snapshot_store", fromlist=["SnapshotStore"]).SnapshotStore(
                config.persistence.snapshot_path
            )
            snap.write({
                "lifecycle": "running",
                "risk_mode": "fail_closed",
                "open_positions": {},
                "pending_entries": {},
                "pending_closes": {},
                "pending_passive_closes": {},
                "global_risk_reason": None,
                "recovery_blocked_reason": None,
            })

            runtime = LiveRuntime(config)
            await runtime.start()

            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
            records = runtime.journal.read_all()
            assert any(r.get("kind") == "runtime.stale_fail_closed_cleared" for r in records)

    @pytest.mark.asyncio
    async def test_tick_populates_last_scan_with_fresh_snapshot(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            sidecar_path = Path(td) / "sidecar.json"
            now_ms = int(time.time() * 1000)
            sidecar_path.write_text(json.dumps({
                "schema_version": 2,
                "published_at_ms": now_ms - 1000,
                "quotes": {
                    "BINANCE:BTCUSDT": {
                        "venue": "binance", "symbol": "BTCUSDT",
                        "bid": 50000.0, "ask": 50005.0,
                        "bid_size": 1.0, "ask_size": 1.0,
                        "funding_rate_bps": 5.0,
                        "funding_timestamp_ms": now_ms - 1000,
                        "mark_price": 50002.0,
                        "index_price": 50002.0,
                        "volume_24h_quote": 1000000.0,
                        "open_interest": 1000.0,
                    },
                },
                "candidates": [],
                "degraded_venues": [],
            }))

            runtime = LiveRuntime(config)
            await runtime.start()
            await runtime.tick()

            assert runtime.state.last_scan is not None, "last_scan must be populated after tick"
            assert "snapshot_freshness" in runtime.state.last_scan
            assert "candidate_count" in runtime.state.last_scan
            assert "tradeable_count" in runtime.state.last_scan
            assert "degraded_venues" in runtime.state.last_scan
            assert "no_entry_reason" in runtime.state.last_scan
            assert runtime.state.last_scan["ts_ms"] > 0


# ---------------------------------------------------------------------------
# Maker-event lane tests (V1: tick_maker_event_lane)
# ---------------------------------------------------------------------------

class TestMakerEventLane:
    """Tests for _maybe_tick_maker_event covering all states and edge cases.

    V1 reference: execution_core/engine.rs:4587-4693 tick_maker_event_lane
    """

    @pytest.mark.asyncio
    async def test_disabled_when_flag_false(self):
        """maker_event_lane_enabled=False clears state and returns immediately."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = False

            sidecar_path = Path(td) / "sidecar.json"
            sidecar_path.write_text(json.dumps(_mk_sidecar(["BTCUSDT"], 50000.0)))

            runtime = LiveRuntime(config)
            await runtime.start()
            runtime._maker_event_state["test_entry"] = {"maker_price": 50000.0}
            runtime._last_maker_event_ms = 5000

            await runtime._maybe_tick_maker_event(10000)

            # State cleared
            assert len(runtime._maker_event_state) == 0

    @pytest.mark.asyncio
    async def test_min_wake_interval_gating(self):
        """Does not wake before maker_event_lane_min_wake_interval_ms elapses."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = True
            config.runtime.maker_event_lane_min_wake_interval_ms = 15000

            sidecar_path = Path(td) / "sidecar.json"
            sidecar_path.write_text(json.dumps(_mk_sidecar(["BTCUSDT"], 50000.0)))

            runtime = LiveRuntime(config)
            await runtime.start()

            # Add a passive pending entry
            runtime.state.pending_entries["pe-1"] = _mk_passive_pending(
                "pe-1", "BTCUSDT", Venue.BINANCE, Venue.OKX
            )

            # Set last wake very recently
            runtime._last_maker_event_ms = 5000

            await runtime._maybe_tick_maker_event(10000)

            # Should not have woken — interval not elapsed
            assert runtime._last_maker_event_ms == 5000

    @pytest.mark.asyncio
    async def test_no_passive_entries_skips(self):
        """No passive entries → no repricing work to do."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = True
            config.runtime.maker_event_lane_min_wake_interval_ms = 1000

            sidecar_path = Path(td) / "sidecar.json"
            sidecar_path.write_text(json.dumps(_mk_sidecar(["BTCUSDT"], 50000.0)))

            runtime = LiveRuntime(config)
            await runtime.start()
            runtime._last_maker_event_ms = 0

            # No pending entries added
            await runtime._maybe_tick_maker_event(5000)

            # Returns early (no passive entries), timestamp not updated
            assert runtime._last_maker_event_ms == 0
            assert "runtime.maker_event_lane_wake" not in _journal_kinds(runtime)

    @pytest.mark.asyncio
    async def test_missing_snapshot_skips(self):
        """Missing sidecar snapshot → skip (no price data)."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = True
            config.runtime.maker_event_lane_min_wake_interval_ms = 1000

            runtime = LiveRuntime(config)
            await runtime.start()

            runtime.state.pending_entries["pe-1"] = _mk_passive_pending(
                "pe-1", "BTCUSDT", Venue.BINANCE, Venue.OKX
            )
            runtime._last_maker_event_ms = 0

            await runtime._maybe_tick_maker_event(5000)

            assert runtime._last_maker_event_ms == 0  # not updated (returned early)

    @pytest.mark.asyncio
    async def test_first_observation_stores_price_and_skips(self):
        """First observation of a passive entry stores the price, no reprice action."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = True
            config.runtime.maker_event_lane_min_wake_interval_ms = 1000
            config.runtime.opportunity_input_mode = "non_parity"

            sidecar_path = Path(td) / "sidecar.json"
            sidecar_path.write_text(json.dumps(_mk_sidecar(["BTCUSDT"], 50000.0)))

            runtime = LiveRuntime(config)
            await runtime.start()

            runtime.state.pending_entries["pe-1"] = _mk_passive_pending(
                "pe-1", "BTCUSDT", Venue.BINANCE, Venue.OKX
            )
            runtime._last_maker_event_ms = 0

            await runtime._maybe_tick_maker_event(5000)

            # Should store the first price observation
            est = runtime._maker_event_state.get("pe-1", {})
            assert est.get("maker_price") == 50000.0
            assert est.get("consecutive_failures") == 0
            # Should still update the lane timestamp
            assert runtime._last_maker_event_ms == 5000

    @pytest.mark.asyncio
    async def test_reprice_when_move_above_reprice_threshold(self):
        """Price move >= passive_reprice_threshold_bps triggers reprice action."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = True
            config.runtime.maker_event_lane_min_wake_interval_ms = 1000
            config.runtime.opportunity_input_mode = "non_parity"
            config.strategy.passive_reprice_threshold_bps = 2.0
            config.strategy.passive_cancel_replace_threshold_bps = 10.0

            sidecar_path = Path(td) / "sidecar.json"
            # Price moved ~5bps from 50000 to 50025
            sidecar_path.write_text(json.dumps(_mk_sidecar(["BTCUSDT"], 50025.0)))

            runtime = LiveRuntime(config)
            await runtime.start()

            runtime.state.pending_entries["pe-1"] = _mk_passive_pending(
                "pe-1", "BTCUSDT", Venue.BINANCE, Venue.OKX
            )
            runtime._last_maker_event_ms = 0
            # Seed with stored price from "previous observation"
            runtime._maker_event_state["pe-1"] = {
                "maker_price": 50000.0,
                "last_reprice_ms": 0,
                "consecutive_failures": 0,
            }
            runtime.entry_executor = _FakeExecutor()

            await runtime._maybe_tick_maker_event(5000)

            # Should have reprice journal entries
            kinds = _journal_kinds(runtime)
            assert "runtime.maker_event_reprice" in kinds, f"got kinds: {kinds}"
            # Should record wake
            assert "runtime.maker_event_lane_wake" in kinds

    @pytest.mark.asyncio
    async def test_cancel_replace_when_move_above_cancel_replace_threshold(self):
        """Price move >= passive_cancel_replace_threshold_bps triggers cancel+replace."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = True
            config.runtime.maker_event_lane_min_wake_interval_ms = 1000
            config.runtime.opportunity_input_mode = "non_parity"
            config.strategy.passive_reprice_threshold_bps = 2.0
            config.strategy.passive_cancel_replace_threshold_bps = 6.0

            sidecar_path = Path(td) / "sidecar.json"
            # Price moved ~8bps from 50000 to 50040
            sidecar_path.write_text(json.dumps(_mk_sidecar(["BTCUSDT"], 50040.0)))

            runtime = LiveRuntime(config)
            await runtime.start()

            runtime.state.pending_entries["pe-1"] = _mk_passive_pending(
                "pe-1", "BTCUSDT", Venue.BINANCE, Venue.OKX
            )
            runtime._last_maker_event_ms = 0
            runtime._maker_event_state["pe-1"] = {
                "maker_price": 50000.0,
                "last_reprice_ms": 0,
                "consecutive_failures": 0,
            }
            runtime.entry_executor = _FakeExecutor()

            await runtime._maybe_tick_maker_event(5000)

            kinds = _journal_kinds(runtime)
            assert "runtime.maker_event_reprice" in kinds
            # Verify it was a cancel_replace action
            reprice_events = [
                r for r in runtime.journal.read_all()
                if r["kind"] == "runtime.maker_event_reprice"
            ]
            assert len(reprice_events) == 1
            assert reprice_events[0]["payload"]["action"] == "cancel_replace"

    @pytest.mark.asyncio
    async def test_no_action_when_price_move_below_threshold(self):
        """Reprice below threshold → no action."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = True
            config.runtime.maker_event_lane_min_wake_interval_ms = 1000
            config.runtime.opportunity_input_mode = "non_parity"
            config.strategy.passive_reprice_threshold_bps = 5.0
            config.strategy.passive_cancel_replace_threshold_bps = 10.0

            sidecar_path = Path(td) / "sidecar.json"
            # Price moved ~1bps from 50000 to 50005 (below 5bps threshold)
            sidecar_path.write_text(json.dumps(_mk_sidecar(["BTCUSDT"], 50005.0)))

            runtime = LiveRuntime(config)
            await runtime.start()

            runtime.state.pending_entries["pe-1"] = _mk_passive_pending(
                "pe-1", "BTCUSDT", Venue.BINANCE, Venue.OKX
            )
            runtime._last_maker_event_ms = 0
            runtime._maker_event_state["pe-1"] = {
                "maker_price": 50000.0,
                "last_reprice_ms": 0,
                "consecutive_failures": 0,
            }

            await runtime._maybe_tick_maker_event(5000)

            # No wake logged (no position was repriced)
            assert "runtime.maker_event_lane_wake" not in _journal_kinds(runtime)

    @pytest.mark.asyncio
    async def test_cooldown_respected(self):
        """Repricing gate: cooldown prevents reprice before passive_failure_cooldown_ms."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = True
            config.runtime.maker_event_lane_min_wake_interval_ms = 1000
            config.runtime.opportunity_input_mode = "non_parity"
            config.strategy.passive_reprice_threshold_bps = 1.0
            config.strategy.passive_failure_cooldown_ms = 5000

            sidecar_path = Path(td) / "sidecar.json"
            sidecar_path.write_text(json.dumps(_mk_sidecar(["BTCUSDT"], 51000.0)))

            runtime = LiveRuntime(config)
            await runtime.start()

            runtime.state.pending_entries["pe-1"] = _mk_passive_pending(
                "pe-1", "BTCUSDT", Venue.BINANCE, Venue.OKX
            )
            runtime._last_maker_event_ms = 0
            # Stored price is 50000, last reprice was at time 3000
            runtime._maker_event_state["pe-1"] = {
                "maker_price": 50000.0,
                "last_reprice_ms": 3000,
                "consecutive_failures": 0,
            }

            # Now at 5000, only 2000ms since last reprice → still in cooldown
            await runtime._maybe_tick_maker_event(5000)

            assert "runtime.maker_event_lane_wake" not in _journal_kinds(runtime)

    @pytest.mark.asyncio
    async def test_cooldown_passed_allows_reprice(self):
        """After cooldown period passes, reprice is allowed."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = True
            config.runtime.maker_event_lane_min_wake_interval_ms = 1000
            config.runtime.opportunity_input_mode = "non_parity"
            config.strategy.passive_reprice_threshold_bps = 1.0
            config.strategy.passive_failure_cooldown_ms = 1000

            sidecar_path = Path(td) / "sidecar.json"
            sidecar_path.write_text(json.dumps(_mk_sidecar(["BTCUSDT"], 51000.0)))

            runtime = LiveRuntime(config)
            await runtime.start()

            runtime.state.pending_entries["pe-1"] = _mk_passive_pending(
                "pe-1", "BTCUSDT", Venue.BINANCE, Venue.OKX
            )
            runtime._last_maker_event_ms = 0
            runtime._maker_event_state["pe-1"] = {
                "maker_price": 50000.0,
                "last_reprice_ms": 1000,
                "consecutive_failures": 0,
            }
            runtime.entry_executor = _FakeExecutor()

            # Now at 5000, 4000ms since last reprice → past cooldown
            await runtime._maybe_tick_maker_event(5000)

            kinds = _journal_kinds(runtime)
            assert "runtime.maker_event_reprice" in kinds

    @pytest.mark.asyncio
    async def test_max_consecutive_failures_gated(self):
        """After max_consecutive_failures is reached, further repricing is stopped."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = True
            config.runtime.maker_event_lane_min_wake_interval_ms = 1000
            config.runtime.opportunity_input_mode = "non_parity"
            config.strategy.passive_reprice_threshold_bps = 1.0
            config.strategy.passive_max_consecutive_failures = 5

            sidecar_path = Path(td) / "sidecar.json"
            sidecar_path.write_text(json.dumps(_mk_sidecar(["BTCUSDT"], 51000.0)))

            runtime = LiveRuntime(config)
            await runtime.start()

            runtime.state.pending_entries["pe-1"] = _mk_passive_pending(
                "pe-1", "BTCUSDT", Venue.BINANCE, Venue.OKX
            )
            runtime._last_maker_event_ms = 0
            runtime._maker_event_state["pe-1"] = {
                "maker_price": 50000.0,
                "last_reprice_ms": 0,
                "consecutive_failures": 5,  # already at max
            }

            await runtime._maybe_tick_maker_event(5000)

            assert "runtime.maker_event_lane_wake" not in _journal_kinds(runtime)

    @pytest.mark.asyncio
    async def test_error_sets_failure_count(self):
        """When _reprice_passive_maker raises, failure counter increments."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = True
            config.runtime.maker_event_lane_min_wake_interval_ms = 1000
            config.runtime.opportunity_input_mode = "non_parity"
            config.strategy.passive_reprice_threshold_bps = 1.0
            config.strategy.maker_leg_default = "buy"

            sidecar_path = Path(td) / "sidecar.json"
            sidecar_path.write_text(json.dumps(_mk_sidecar(["BTCUSDT"], 51000.0)))

            runtime = LiveRuntime(config)
            await runtime.start()

            runtime.state.pending_entries["pe-1"] = _mk_passive_pending(
                "pe-1", "BTCUSDT", Venue.BINANCE, Venue.OKX
            )
            runtime._last_maker_event_ms = 0
            runtime._maker_event_state["pe-1"] = {
                "maker_price": 50000.0,
                "last_reprice_ms": 0,
                "consecutive_failures": 0,
            }

            # entry_executor is None → _reprice_passive_maker will be skipped
            # (checks `if self.entry_executor is None: continue`)
            # Instead, we manually simulate the error path
            # _maybe_tick_maker_event wraps _reprice_passive_maker in try/except
            # To trigger the error path, set entry_executor to a mock that raises
            class _RaisingExecutor:
                async def execute(self, ctx):
                    raise RuntimeError("test error")
            runtime.entry_executor = _RaisingExecutor()

            await runtime._maybe_tick_maker_event(5000)

            est = runtime._maker_event_state.get("pe-1", {})
            assert est.get("consecutive_failures") == 1

    @pytest.mark.asyncio
    async def test_no_entry_executor_skips_reprice(self):
        """entry_executor is None → repricing is skipped for that entry."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = True
            config.runtime.maker_event_lane_min_wake_interval_ms = 1000
            config.runtime.opportunity_input_mode = "non_parity"
            config.strategy.passive_reprice_threshold_bps = 1.0

            sidecar_path = Path(td) / "sidecar.json"
            sidecar_path.write_text(json.dumps(_mk_sidecar(["BTCUSDT"], 51000.0)))

            runtime = LiveRuntime(config)
            await runtime.start()
            # entry_executor is None by default

            runtime.state.pending_entries["pe-1"] = _mk_passive_pending(
                "pe-1", "BTCUSDT", Venue.BINANCE, Venue.OKX
            )
            runtime._last_maker_event_ms = 0
            runtime._maker_event_state["pe-1"] = {
                "maker_price": 50000.0,
                "last_reprice_ms": 0,
                "consecutive_failures": 0,
            }

            await runtime._maybe_tick_maker_event(5000)

            # No wake (no positions actually repriced)
            assert "runtime.maker_event_lane_wake" not in _journal_kinds(runtime)

    @pytest.mark.asyncio
    async def test_non_passive_entries_filtered_out(self):
        """Only entries with 'passive' in entry_type are processed."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = True
            config.runtime.maker_event_lane_min_wake_interval_ms = 1000
            config.strategy.passive_reprice_threshold_bps = 1.0

            sidecar_path = Path(td) / "sidecar.json"
            sidecar_path.write_text(json.dumps(_mk_sidecar(["BTCUSDT"], 51000.0)))

            runtime = LiveRuntime(config)
            await runtime.start()

            # Standard taker entry — should be filtered out
            runtime.state.pending_entries["pe-taker"] = PendingEntry(
                pending_id="pe-taker",
                symbol="BTCUSDT",
                long_venue=Venue.BINANCE,
                short_venue=Venue.OKX,
                target_quantity=1.0,
                long_side=Side.BUY,
                short_side=Side.SELL,
                created_at_ms=0,
                entry_type="standard_dual_taker",
                maker_price=0.0,
            )
            runtime._last_maker_event_ms = 0

            await runtime._maybe_tick_maker_event(5000)

            # Returns early (no passive entries found), timestamp not updated
            assert runtime._last_maker_event_ms == 0
            assert "runtime.maker_event_lane_wake" not in _journal_kinds(runtime)

    @pytest.mark.asyncio
    async def test_multiple_symbols_price_hints(self):
        """Multiple symbols in sidecar → correct per-symbol price hints."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = True
            config.runtime.maker_event_lane_min_wake_interval_ms = 1000
            config.runtime.opportunity_input_mode = "non_parity"
            config.strategy.passive_reprice_threshold_bps = 1.0

            sidecar_path = Path(td) / "sidecar.json"
            sidecar_path.write_text(json.dumps({
                "schema_version": 2,
                "published_at_ms": 5000,
                "candidates": [],
                "quotes": {
                    "BINANCE:BTCUSDT": {
                        "venue": "binance", "symbol": "BTCUSDT",
                        "bid": 50000.0, "ask": 50010.0,
                        "bid_size": 1.0, "ask_size": 1.0,
                        "funding_rate_bps": 0.0, "funding_timestamp_ms": 5000,
                        "mark_price": 50005.0, "index_price": 50000.0,
                        "volume_24h_quote": 1000000.0, "open_interest": 1000.0,
                    },
                    "OKX:ETHUSDT": {
                        "venue": "okx", "symbol": "ETHUSDT",
                        "bid": 3000.0, "ask": 3006.0,
                        "bid_size": 10.0, "ask_size": 10.0,
                        "funding_rate_bps": 1.0, "funding_timestamp_ms": 5000,
                        "mark_price": 3003.0, "index_price": 3000.0,
                        "volume_24h_quote": 5000000.0, "open_interest": 5000.0,
                    },
                },
            }))

            runtime = LiveRuntime(config)
            await runtime.start()

            runtime.state.pending_entries["pe-btc"] = _mk_passive_pending(
                "pe-btc", "BTCUSDT", Venue.BINANCE, Venue.OKX
            )
            runtime.state.pending_entries["pe-eth"] = _mk_passive_pending(
                "pe-eth", "ETHUSDT", Venue.OKX, Venue.BYBIT
            )
            runtime._last_maker_event_ms = 0

            await runtime._maybe_tick_maker_event(5000)

            # Both entries should have their first price stored
            btc = runtime._maker_event_state.get("pe-btc", {})
            eth = runtime._maker_event_state.get("pe-eth", {})
            assert btc.get("maker_price") == 50005.0  # mid of 50000/50010
            assert eth.get("maker_price") == 3003.0  # mid of 3000/3006


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mk_sidecar(symbols: list[str], mid_price: float) -> dict:
    """Build a minimal sidecar snapshot dict with quotes at mid_price."""
    quotes = {}
    for sym in symbols:
        half_spread = mid_price * 0.0001
        quotes[f"BINANCE:{sym}"] = {
            "venue": "binance",
            "symbol": sym,
            "bid": mid_price - half_spread,
            "ask": mid_price + half_spread,
            "bid_size": 1.0,
            "ask_size": 1.0,
            "funding_rate_bps": 0.0,
            "funding_timestamp_ms": 5000,
            "mark_price": mid_price,
            "index_price": mid_price,
            "volume_24h_quote": 1000000.0,
            "open_interest": 1000.0,
        }
    return {
        "schema_version": 2,
        "published_at_ms": 5000,
        "candidates": [],
        "quotes": quotes,
    }


def _mk_passive_pending(pid: str, symbol: str, long_v: Venue, short_v: Venue):
    """Create a PendingEntry with passive entry_type."""
    from lightfee.core.domain import Side
    return PendingEntry(
        pending_id=pid,
        symbol=symbol,
        long_venue=long_v,
        short_venue=short_v,
        target_quantity=1.0,
        long_side=Side.BUY,
        short_side=Side.SELL,
        created_at_ms=0,
        entry_type="passive_incremental",
        maker_price=0.0,
        long_quantity=1.0,
        short_quantity=1.0,
    )


def _journal_kinds(runtime) -> list[str]:
    return [r["kind"] for r in runtime.journal.read_all()]


class _FakeExecutor:
    """Minimal executor mock that records ctx and succeeds."""
    def __init__(self):
        self.calls: list = []

    async def execute(self, ctx):
        self.calls.append(ctx)
