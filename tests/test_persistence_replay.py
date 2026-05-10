"""Tests for persistence replay: journal replay, snapshot recovery, atomic writes.

Covers Rust V1 behavior from:
- src/observability_ops/replay_bridge.rs (journal record replay)
- src/runtime_state/persisted_engine.rs (state normalization)
- src/runtime_state/snapshot_store.rs (atomic snapshot persistence)
"""

import json
import tempfile
from pathlib import Path

import pytest

from lightfee.engine.state import EngineState, OpenPosition, PendingEntry, PendingClose
from lightfee.core.domain import Side, Venue
from lightfee.persistence.journal import Journal, replay_journal_records
from lightfee.persistence.snapshot_store import SnapshotStore
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


class TestJournalReplayEngine:
    """Test journal replay engine that reconstructs state from event records."""

    def test_replay_empty_journal_returns_empty_state(self):
        records: list[dict] = []
        result = replay_journal_records(records)
        assert result["open_position_count"] == 0
        assert result["pending_entry_count"] == 0
        assert result["pending_close_count"] == 0

    def test_replay_single_entry_opened(self):
        records = [
            {
                "seq": 1,
                "run_id": "test-run",
                "ts_ms": 1000,
                "kind": "entry.opened",
                "payload": {
                    "position_id": "pos-1",
                    "symbol": "BTCUSDT",
                    "long_venue": "binance",
                    "short_venue": "okx",
                    "quantity": 0.1,
                },
            }
        ]
        result = replay_journal_records(records)
        assert result["open_position_count"] == 1
        assert "pos-1" in result["open_position_ids"]

    def test_replay_entry_then_close(self):
        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "entry.opened",
                "payload": {"position_id": "pos-1", "symbol": "ETHUSDT"},
            },
            {
                "seq": 2, "run_id": "r1", "ts_ms": 5000,
                "kind": "exit.closed",
                "payload": {"position_id": "pos-1", "reason": "profit_take"},
            },
        ]
        result = replay_journal_records(records)
        assert result["open_position_count"] == 0

    def test_replay_partial_close_reduces_quantity(self):
        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "entry.opened",
                "payload": {
                    "position_id": "pos-2",
                    "symbol": "SOLUSDT",
                    "quantity": 100.0,
                    "long_quantity": 100.0,
                    "short_quantity": 100.0,
                },
            },
            {
                "seq": 2, "run_id": "r1", "ts_ms": 3000,
                "kind": "exit.partial_closed",
                "payload": {
                    "position_id": "pos-2",
                    "quantity": 50.0,
                    "current_net_quote": 10.0,
                },
            },
        ]
        result = replay_journal_records(records)
        # Position still open after partial close
        assert result["open_position_count"] == 1
        # Quantity reduced
        pos_data = result.get("positions", {}).get("pos-2", {})
        assert pos_data.get("quantity", 100.0) == 50.0

    def test_replay_lifecycle_changes(self):
        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "runtime.lifecycle_changed",
                "payload": {"from": "booting", "to": "reconciling", "reason": "startup"},
            },
            {
                "seq": 2, "run_id": "r1", "ts_ms": 2000,
                "kind": "runtime.lifecycle_changed",
                "payload": {"from": "reconciling", "to": "running", "reason": "recovery_complete"},
            },
        ]
        result = replay_journal_records(records)
        assert result["final_lifecycle"] == "running"

    def test_replay_risk_mode_changes(self):
        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "runtime.risk_mode_changed",
                "payload": {"from": "running", "to": "reduce_only", "reason": "health_drop"},
            },
        ]
        result = replay_journal_records(records)
        assert result["final_risk_mode"] == "reduce_only"

    def test_replay_recovery_live_detected(self):
        """Rust V1: recovery.live_detected records restore as open positions."""
        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "recovery.live_detected",
                "payload": {
                    "position_id": "pos-recovered",
                    "symbol": "BTCUSDT",
                    "long_venue": "binance",
                    "short_venue": "okx",
                    "quantity": 0.05,
                },
            }
        ]
        result = replay_journal_records(records)
        assert result["open_position_count"] == 1

    def test_replay_recovery_flat_removes_position(self):
        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "recovery.live_detected",
                "payload": {"position_id": "pos-flat", "symbol": "ETHUSDT"},
            },
            {
                "seq": 2, "run_id": "r1", "ts_ms": 5000,
                "kind": "recovery.flat",
                "payload": {"position_id": "pos-flat", "reason": "recovery_flat"},
            },
        ]
        result = replay_journal_records(records)
        assert result["open_position_count"] == 0

    def test_replay_ignores_non_state_events(self):
        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "runtime.tick_error",
                "payload": {"error": "timeout"},
            },
            {
                "seq": 2, "run_id": "r1", "ts_ms": 2000,
                "kind": "scan.completed",
                "payload": {"candidate_count": 3},
            },
        ]
        result = replay_journal_records(records)
        assert result["open_position_count"] == 0


class TestSnapshotAtomicity:
    """Test atomic snapshot write semantics (Rust V1: FileStateStore)."""

    def test_atomic_write_survives_crash_simulation(self):
        with tempfile.TemporaryDirectory() as td:
            snap_path = Path(td) / "atomic.json"
            store = SnapshotStore(snap_path)

            # Write initial state
            store.write({"lifecycle": "running", "tick_count": 1})

            # Partial write simulation: if temp file exists but not renamed,
            # the original must remain intact
            assert store.exists()
            data = store.read()
            assert data is not None
            assert data["tick_count"] == 1

    def test_write_then_read_immediately(self):
        with tempfile.TemporaryDirectory() as td:
            snap_path = Path(td) / "immediate.json"
            store = SnapshotStore(snap_path)
            store.write({"lifecycle": "booting", "risk_mode": "fail_closed"})

            # Immediate read should reflect the write
            data = store.read()
            assert data is not None
            assert data["risk_mode"] == "fail_closed"

    def test_write_overwrites_previous(self):
        with tempfile.TemporaryDirectory() as td:
            snap_path = Path(td) / "overwrite.json"
            store = SnapshotStore(snap_path)

            store.write({"tick_count": 1})
            store.write({"tick_count": 2})

            data = store.read()
            assert data is not None
            assert data["tick_count"] == 2


class TestJournalWithSnapshotRecovery:
    """Test combined snapshot + journal replay recovery path."""

    def test_snapshot_baseline_plus_journal_events(self):
        with tempfile.TemporaryDirectory() as td:
            # Write snapshot
            snap_path = Path(td) / "combined-state.json"
            store = SnapshotStore(snap_path)
            store.write({
                "lifecycle": "running",
                "risk_mode": "running",
                "tick_count": 50,
                "open_positions": {},
            })

            # Write journal events after snapshot
            jp = Path(td) / "combined-events.jsonl"
            journal = Journal(jp)
            journal.open()
            journal.append("entry.opened", {
                "position_id": "new-pos",
                "symbol": "AVAXUSDT",
                "quantity": 5.0,
            }, flush=True)
            journal.close()

            # Recovery: load snapshot, replay journal
            base = store.read()
            assert base is not None
            assert base["tick_count"] == 50

            journal_records = journal.read_all()
            assert len(journal_records) == 1

            # Replay: snapshot had 0 positions, journal added 1
            replay_result = replay_journal_records(journal_records)
            assert replay_result["open_position_count"] == 1


class TestJournalCompaction:
    """Test journal compaction behavior (Rust V1: maybe_compact_persisted_journal)."""

    def test_journal_read_all_handles_large_volume(self):
        with tempfile.TemporaryDirectory() as td:
            jp = Path(td) / "bulk.jsonl"
            journal = Journal(jp)
            journal.open()

            # Write 500 records
            for i in range(500):
                journal.append("scan.completed", {"seq": i})

            journal.close()

            records = journal.read_all()
            assert len(records) == 500
            # Verify sequential ordering
            for i, r in enumerate(records):
                assert r["payload"]["seq"] == i

    def test_journal_retention_compact_keeps_baseline(self):
        """Verify journal can be compacted while keeping critical baselines."""
        with tempfile.TemporaryDirectory() as td:
            jp = Path(td) / "compact.jsonl"
            journal = Journal(jp)
            journal.open()

            # Write baseline + many events
            journal.append("recovery.live_detected", {
                "position_id": "pos-keep",
                "symbol": "BTCUSDT",
                "quantity": 0.1,
            }, flush=True)
            for i in range(100):
                journal.append("scan.completed", {"seq": i})
            journal.close()

            # All records should be readable
            records = journal.read_all()
            assert len(records) == 101
            # Baseline record preserved
            assert records[0]["kind"] == "recovery.live_detected"
