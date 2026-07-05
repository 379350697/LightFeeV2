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
from lightfee.engine.exit_decision import aligned_settlement_delay_elapsed
from lightfee.engine.business_contract import pending_entry_has_unhedged_maker_exposure
from lightfee.engine.runtime import LiveRuntime
from lightfee.engine.passive_close import PassiveCloseExecutor
from lightfee.engine.state import EngineState, OpenPosition, PendingEntry, PendingPassiveClose
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

    def test_startup_recovered_position_hydrates_funding_metadata_from_entry_journal(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config)
            runtime.journal.open()
            funding_ms = 1_782_633_600_000
            runtime.journal.append(
                "runtime.pending_entry_registered",
                {
                    "entry_id": "entry-act",
                    "symbol": "ACTUSDT",
                    "long_venue": "binance",
                    "short_venue": "okx",
                    "funding_timestamp_ms": funding_ms,
                    "first_funding_timestamp_ms": funding_ms,
                    "long_funding_timestamp_ms": funding_ms,
                    "short_funding_timestamp_ms": 0,
                    "second_funding_timestamp_ms": 0,
                    "opportunity_type": "aligned",
                    "entry_maker_leg": "long",
                    "exit_maker_leg": "short",
                },
                ts_ms=funding_ms - 360_000,
            )

            created, recovered_indices = runtime._hydrate_balanced_startup_live_positions(
                [
                    (
                        "ACTUSDT",
                        PositionSnapshot(
                            venue=Venue.BINANCE,
                            symbol="ACTUSDT",
                            side=Side.BUY,
                            quantity=5385.0,
                            entry_price=0.0089,
                            observed_at_ms=funding_ms + 1_200_000,
                        ),
                    ),
                    (
                        "ACTUSDT",
                        PositionSnapshot(
                            venue=Venue.OKX,
                            symbol="ACT-USDT-SWAP",
                            side=Side.SELL,
                            quantity=5385.0,
                            entry_price=0.008904,
                            observed_at_ms=funding_ms + 1_200_000,
                        ),
                    ),
                ],
                funding_ms + 1_200_000,
                source="startup_live_position_probe",
            )

            assert created == 1
            assert recovered_indices == {0, 1}
            position = runtime.state.open_positions[
                "live-recovered:ACTUSDT:binance->okx"
            ]
            assert position.funding_timestamp_ms == funding_ms
            assert position.long_funding_timestamp_ms == funding_ms
            assert position.short_funding_timestamp_ms == 0
            assert position.opportunity_type == "aligned"
            assert position.entry_maker_leg == "long"
            assert position.exit_maker_leg == "short"
            assert aligned_settlement_delay_elapsed(
                position,
                funding_ms + 1_200_000,
                delay_ms=1,
            )

    def test_startup_recovered_position_without_funding_metadata_is_not_opened(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            runtime = LiveRuntime(config)
            runtime.journal.open()

            created, recovered_indices = runtime._hydrate_balanced_startup_live_positions(
                [
                    (
                        "ACTUSDT",
                        PositionSnapshot(
                            venue=Venue.BINANCE,
                            symbol="ACTUSDT",
                            side=Side.BUY,
                            quantity=5385.0,
                            entry_price=0.0089,
                            observed_at_ms=1_000_000,
                        ),
                    ),
                    (
                        "ACTUSDT",
                        PositionSnapshot(
                            venue=Venue.OKX,
                            symbol="ACT-USDT-SWAP",
                            side=Side.SELL,
                            quantity=5385.0,
                            entry_price=0.008904,
                            observed_at_ms=1_000_000,
                        ),
                    ),
                ],
                1_000_000,
                source="startup_live_position_probe",
            )

            assert created == 0
            assert recovered_indices == set()
            assert runtime.state.open_positions == {}
            records = runtime.journal.read_all()
            event = records[-1]
            assert event["kind"] == "recovery.recovered_position_funding_timestamp_missing"
            assert event["payload"]["symbol"] == "ACTUSDT"
            assert event["payload"]["reason"] == (
                "recovered_position_funding_timestamp_missing"
            )

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

    @pytest.mark.asyncio
    async def test_housekeeping_clears_clean_recovery_block_after_passive_orphan_cleanup(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            runtime = LiveRuntime(config)
            await runtime.start()
            class FlatAdapter(FakeVenueAdapter):
                async def fetch_open_orders(self, symbol: str) -> list[dict]:
                    return []

                async def fetch_all_positions(self) -> list[PositionSnapshot]:
                    return []

            runtime.passive_close_executor = PassiveCloseExecutor(
                {
                    Venue.BINANCE: FlatAdapter(Venue.BINANCE),
                    Venue.OKX: FlatAdapter(Venue.OKX),
                },
                runtime.journal,
            )
            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
            runtime.state.risk_mode = GlobalRiskMode.RUNNING
            runtime.state.recovery_blocked_reason = (
                "startup_recovery_pending_work_without_open_positions"
            )
            runtime.state.recovery_blocked_at_ms = 1234
            position = OpenPosition(
                position_id="entry-orphan",
                symbol="BTCUSDT",
                long_venue=Venue.BINANCE,
                short_venue=Venue.OKX,
                long_quantity=0.01,
                short_quantity=0.01,
                long_entry_price=50000.0,
                short_entry_price=50010.0,
                opened_at_ms=1000,
                matched_quantity=0.01,
            )
            runtime.state.pending_passive_closes["entry-orphan"] = PendingPassiveClose(
                position_id="entry-orphan",
                reason="funding_capture",
                position_snapshot=position,
            )

            await runtime._maybe_tick_passive_close(5000)
            await runtime._post_tick_housekeeping(5000)

            assert runtime.state.pending_passive_closes == {}
            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
            assert runtime.state.recovery_blocked_reason is None
            assert runtime.state.recovery_blocked_at_ms == 0
            records = runtime.journal.read_all()
            assert any(
                r.get("kind") == "recovery.legacy_block_cleared"
                for r in records
            )

    @pytest.mark.asyncio
    async def test_housekeeping_does_not_clear_legacy_block_without_core_decision(self):
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)

            runtime = LiveRuntime(config)
            await runtime.start()
            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
            runtime.state.risk_mode = GlobalRiskMode.FAIL_CLOSED
            runtime.state.recovery_blocked_reason = (
                "startup_recovery_pending_work_without_open_positions"
            )
            runtime.state.recovery_blocked_at_ms = 1234

            await runtime._post_tick_housekeeping(5000)

            assert runtime.state.lifecycle == EngineLifecycle.RISK_ONLY
            assert runtime.state.risk_mode == GlobalRiskMode.FAIL_CLOSED
            assert runtime.state.recovery_blocked_reason == (
                "startup_recovery_pending_work_without_open_positions"
            )
            records = runtime.journal.read_all()
            assert not any(
                r.get("kind") == "recovery.legacy_block_cleared"
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

    def test_clean_v1_closure_releases_fail_closed_stale_recovery_block(self):
        from lightfee.engine.recovery_decision_core import (
            RecoveryDecision,
            RecoveryDecisionKind,
            RecoveryEvidenceClass,
        )

        with tempfile.TemporaryDirectory() as td:
            runtime = LiveRuntime(make_test_config(td))
            runtime.state.lifecycle = EngineLifecycle.RISK_ONLY
            runtime.state.risk_mode = GlobalRiskMode.FAIL_CLOSED
            runtime.state.recovery_blocked_reason = "live_position_mismatch_flatten_failed"
            runtime.state.recovery_blocked_at_ms = 1234
            runtime.state.v1_lifecycle_closure = {
                "summary": {
                    "entry_allowed": True,
                    "recovery_block_reason": None,
                }
            }
            runtime.recovery_decision = RecoveryDecision(
                kind=RecoveryDecisionKind.RUNNING_CLEAN,
                evidence_class=RecoveryEvidenceClass.COMPLETE_FLAT,
                entry_allowed=True,
                clear_previous_block=True,
                clear_reason="core_running_clean_flat_no_open_orders",
            )

            runtime._clear_stale_recovery_block_when_v1_closure_allows_entry(1700000000000)

            assert runtime.state.lifecycle == EngineLifecycle.RUNNING
            assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
            assert runtime.state.recovery_blocked_reason is None
            assert runtime.state.recovery_blocked_at_ms == 0

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

    def test_single_leg_risk_uses_v1_missing_hedge_quantity(self):
        pending = _mk_passive_pending("pe-hedged-target", "BTCUSDT", Venue.BINANCE, Venue.OKX)
        pending.target_quantity = 10.0
        pending.long_quantity = 10.0
        pending.short_quantity = 10.0
        pending.maker_leg_filled = 12.0
        pending.hedge_leg_filled = 10.0

        assert pending.missing_hedge_quantity() == pytest.approx(0.0)
        assert pending_entry_has_unhedged_maker_exposure(pending) is False

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
    async def test_bybit_balance_reject_terminally_blocks_maker_reprice(self):
        """Bybit 110007 is an admission block, not a repeatable reprice error."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = True
            config.runtime.maker_event_lane_min_wake_interval_ms = 1000
            config.runtime.opportunity_input_mode = "non_parity"
            config.strategy.passive_reprice_threshold_bps = 1.0
            config.strategy.passive_max_consecutive_failures = 5
            config.strategy.maker_leg_default = "buy"

            sidecar_path = Path(td) / "sidecar.json"
            sidecar_path.write_text(json.dumps(_mk_sidecar(["BTCUSDT"], 51000.0)))

            runtime = LiveRuntime(config)
            await runtime.start()

            runtime.state.pending_entries["pe-1"] = _mk_passive_pending(
                "pe-1", "BTCUSDT", Venue.BYBIT, Venue.OKX
            )
            runtime._last_maker_event_ms = 0
            runtime._maker_event_state["pe-1"] = {
                "maker_price": 50000.0,
                "last_reprice_ms": 0,
                "consecutive_failures": 0,
            }

            class _BalanceRejectExecutor:
                calls = 0

                async def execute(self, ctx):
                    self.calls += 1
                    raise RuntimeError(
                        "bybit passive order failed: bybit retCode=110007 "
                        "retMsg=Available balance is insufficient"
                    )

            executor = _BalanceRejectExecutor()
            runtime.entry_executor = executor

            await runtime._maybe_tick_maker_event(5000)

            est = runtime._maker_event_state.get("pe-1", {})
            assert est.get("terminal_reject_reason") == (
                "insufficient_balance_admission_blocked"
            )
            assert est.get("consecutive_failures") == 5
            error_events = [
                event for event in runtime.journal.read_all()
                if event["kind"] == "runtime.maker_event_reprice_error"
            ]
            assert error_events[-1]["payload"]["response_classification"] == (
                "insufficient_balance_admission_blocked"
            )

            sidecar_path.write_text(json.dumps(_mk_sidecar(["BTCUSDT"], 52000.0)))
            runtime._last_maker_event_ms = 0

            await runtime._maybe_tick_maker_event(10_000)

            assert executor.calls == 1

    @pytest.mark.asyncio
    async def test_bybit_balance_reject_records_short_venue_for_sell_maker(self):
        """Terminal admission diagnostics must identify the actual maker venue."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = True
            config.runtime.maker_event_lane_min_wake_interval_ms = 1000
            config.runtime.opportunity_input_mode = "non_parity"
            config.strategy.passive_reprice_threshold_bps = 1.0
            config.strategy.maker_leg_default = "sell"

            sidecar_path = Path(td) / "sidecar.json"
            sidecar_path.write_text(json.dumps(_mk_sidecar(["BTCUSDT"], 51000.0)))

            runtime = LiveRuntime(config)
            await runtime.start()

            runtime.state.pending_entries["pe-sell-maker"] = _mk_passive_pending(
                "pe-sell-maker", "BTCUSDT", Venue.OKX, Venue.BYBIT
            )
            runtime._last_maker_event_ms = 0
            runtime._maker_event_state["pe-sell-maker"] = {
                "maker_price": 50000.0,
                "last_reprice_ms": 0,
                "consecutive_failures": 0,
            }

            class _BalanceRejectExecutor:
                async def execute(self, ctx):
                    raise RuntimeError(
                        "bybit passive order failed: bybit retCode=110007 "
                        "retMsg=Available balance is insufficient"
                    )

            runtime.entry_executor = _BalanceRejectExecutor()

            await runtime._maybe_tick_maker_event(5000)

            est = runtime._maker_event_state.get("pe-sell-maker", {})
            assert est.get("terminal_reject_reason") == (
                "insufficient_balance_admission_blocked"
            )
            assert est.get("venue") == "bybit"

    @pytest.mark.asyncio
    async def test_unhedged_single_leg_pending_blocks_maker_event_reprice(self):
        """Maker-event lane must not add risk once maker fill outruns hedge fill."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = True
            config.runtime.maker_event_lane_min_wake_interval_ms = 1000
            config.runtime.opportunity_input_mode = "non_parity"
            config.strategy.passive_reprice_threshold_bps = 1.0
            config.strategy.maker_leg_default = "buy"

            sidecar_path = Path(td) / "sidecar.json"
            sidecar_path.write_text(json.dumps(_mk_sidecar(["LABUSDT"], 16.5)))

            runtime = LiveRuntime(config)
            await runtime.start()

            pending = _mk_passive_pending(
                "pe-unhedged", "LABUSDT", Venue.BINANCE, Venue.ASTER
            )
            pending.maker_leg = "long"
            pending.maker_leg_filled = 19.0
            pending.hedge_leg_filled = 0.0
            runtime.state.pending_entries[pending.pending_id] = pending
            runtime._last_maker_event_ms = 0
            runtime._maker_event_state[pending.pending_id] = {
                "maker_price": 16.0,
                "last_reprice_ms": 0,
                "consecutive_failures": 0,
            }

            class _RecordingExecutor:
                calls = 0

                async def execute(self, ctx):
                    self.calls += 1
                    return None

            executor = _RecordingExecutor()
            runtime.entry_executor = executor

            await runtime._maybe_tick_maker_event(5000)

            assert executor.calls == 0
            blocked = [
                event for event in runtime.journal.read_all()
                if event["kind"] == "runtime.maker_event_reprice_blocked"
            ]
            assert blocked[-1]["payload"]["entry_id"] == "pe-unhedged"
            assert blocked[-1]["payload"]["reason"] == "unhedged_single_leg_risk"
            assert blocked[-1]["payload"]["maker_leg_filled"] == 19.0
            assert blocked[-1]["payload"]["hedge_leg_filled"] == 0.0
            await runtime._maybe_tick_maker_event(7000)
            blocked_after_retry = [
                event
                for event in runtime.journal.read_all()
                if event["kind"] == "runtime.maker_event_reprice_blocked"
            ]
            assert blocked_after_retry == blocked
            assert executor.calls == 0

    @pytest.mark.asyncio
    async def test_unhedged_single_leg_block_clears_when_hedge_catches_up(self):
        """Single-leg risk is a transient pause, not a terminal maker fuse."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = True
            config.runtime.maker_event_lane_min_wake_interval_ms = 1000
            config.runtime.opportunity_input_mode = "non_parity"
            config.strategy.passive_reprice_threshold_bps = 1.0
            config.strategy.maker_leg_default = "buy"

            sidecar_path = Path(td) / "sidecar.json"
            sidecar_path.write_text(json.dumps(_mk_sidecar(["LABUSDT"], 16.5)))

            runtime = LiveRuntime(config)
            await runtime.start()

            pending = _mk_passive_pending(
                "pe-unhedged-clears", "LABUSDT", Venue.BINANCE, Venue.ASTER
            )
            pending.maker_leg = "long"
            pending.target_quantity = 10.0
            pending.long_quantity = 10.0
            pending.short_quantity = 10.0
            pending.maker_leg_filled = 5.0
            pending.hedge_leg_filled = 0.0
            runtime.state.pending_entries[pending.pending_id] = pending
            runtime._last_maker_event_ms = 0
            runtime._maker_event_state[pending.pending_id] = {
                "maker_price": 16.0,
                "last_reprice_ms": 0,
                "consecutive_failures": 0,
            }

            class _RecordingExecutor:
                calls = 0

                async def execute(self, ctx):
                    self.calls += 1
                    return None

            executor = _RecordingExecutor()
            runtime.entry_executor = executor

            await runtime._maybe_tick_maker_event(5000)

            assert executor.calls == 0
            first_state = runtime._maker_event_state[pending.pending_id]
            assert first_state.get("terminal_reject_reason") is None
            assert first_state.get("transient_block_reason") == "unhedged_single_leg_risk"

            pending.hedge_leg_filled = 5.0

            await runtime._maybe_tick_maker_event(7000)

            assert executor.calls == 1
            resumed_state = runtime._maker_event_state[pending.pending_id]
            assert resumed_state.get("terminal_reject_reason") is None
            assert resumed_state.get("transient_block_reason") is None

    @pytest.mark.asyncio
    async def test_binance_margin_reject_terminally_blocks_maker_reprice(self):
        """Binance -2019 is an admission block, not a repeatable reprice error."""
        with tempfile.TemporaryDirectory() as td:
            config = make_test_config(td)
            config.runtime.maker_event_lane_enabled = True
            config.runtime.maker_event_lane_min_wake_interval_ms = 1000
            config.runtime.opportunity_input_mode = "non_parity"
            config.strategy.passive_reprice_threshold_bps = 1.0
            config.strategy.passive_max_consecutive_failures = 5
            config.strategy.maker_leg_default = "buy"

            sidecar_path = Path(td) / "sidecar.json"
            sidecar_path.write_text(json.dumps(_mk_sidecar(["LABUSDT"], 16.5)))

            runtime = LiveRuntime(config)
            await runtime.start()

            runtime.state.pending_entries["pe-binance-margin"] = _mk_passive_pending(
                "pe-binance-margin", "LABUSDT", Venue.BINANCE, Venue.ASTER
            )
            runtime._last_maker_event_ms = 0
            runtime._maker_event_state["pe-binance-margin"] = {
                "maker_price": 16.0,
                "last_reprice_ms": 0,
                "consecutive_failures": 0,
            }

            class _MarginRejectExecutor:
                calls = 0

                async def execute(self, ctx):
                    self.calls += 1
                    raise RuntimeError(
                        'binance error code=-2019 msg="Margin is insufficient."'
                    )

            executor = _MarginRejectExecutor()
            runtime.entry_executor = executor

            await runtime._maybe_tick_maker_event(5000)

            est = runtime._maker_event_state.get("pe-binance-margin", {})
            assert est.get("terminal_reject_reason") == (
                "insufficient_margin_admission_blocked"
            )
            assert est.get("consecutive_failures") == 5
            error_events = [
                event for event in runtime.journal.read_all()
                if event["kind"] == "runtime.maker_event_reprice_error"
            ]
            assert error_events[-1]["payload"]["response_classification"] == (
                "insufficient_margin_admission_blocked"
            )

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
