"""Tests for engine state, lifecycle, and recovery."""

import tempfile
from pathlib import Path

import pytest

from lightfee.engine.lifecycle import (
    can_enter_new_positions,
    enter_fail_closed,
    set_global_risk_mode,
    set_lifecycle,
)
from lightfee.engine.recovery import (
    build_recovery_snapshot,
    recover_from_snapshot,
)
from lightfee.engine.state import EngineState, OpenPosition
from lightfee.core.domain import Venue
from lightfee.persistence.journal import Journal
from lightfee.persistence.snapshot_store import SnapshotStore
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


class TestEngineState:
    def test_empty_state_starts_booting(self):
        state = EngineState()
        assert state.lifecycle == EngineLifecycle.BOOTING
        assert state.risk_mode == GlobalRiskMode.RUNNING
        assert len(state.open_positions) == 0

    def test_to_dict(self):
        state = EngineState(tick_count=5)
        d = state.to_dict()
        assert d["tick_count"] == 5
        assert d["lifecycle"] == "booting"


class TestLifecycle:
    def test_set_lifecycle(self):
        state = EngineState()
        set_lifecycle(state, EngineLifecycle.RUNNING)
        assert state.lifecycle == EngineLifecycle.RUNNING

    def test_global_risk_mode_max(self):
        state = EngineState()
        set_global_risk_mode(state, GlobalRiskMode.ENTRY_PAUSED)
        assert state.risk_mode == GlobalRiskMode.ENTRY_PAUSED
        set_global_risk_mode(state, GlobalRiskMode.REDUCE_ONLY)
        assert state.risk_mode == GlobalRiskMode.REDUCE_ONLY

    def test_fail_closed(self):
        state = EngineState()
        enter_fail_closed(state)
        assert state.lifecycle == EngineLifecycle.RISK_ONLY
        assert state.risk_mode == GlobalRiskMode.FAIL_CLOSED

    def test_can_enter_new_positions(self):
        state = EngineState()
        set_lifecycle(state, EngineLifecycle.RUNNING)
        state.risk_mode = GlobalRiskMode.RUNNING
        assert can_enter_new_positions(state)

        state.risk_mode = GlobalRiskMode.ENTRY_PAUSED
        assert not can_enter_new_positions(state)


class TestRecovery:
    def test_empty_snapshot_starts_running(self):
        """Rust V1: clean startup with no snapshot and no positions → RUNNING."""
        with tempfile.TemporaryDirectory() as td:
            journal_path = Path(td) / "events.jsonl"
            snap_path = Path(td) / "state.json"

            journal = Journal(journal_path)
            journal.open()
            journal.close()

            snap = SnapshotStore(snap_path)
            state = recover_from_snapshot(snap, journal)
            assert state.lifecycle == EngineLifecycle.RUNNING

    def test_recovery_snapshot_no_positions(self):
        state = EngineState()
        rs = build_recovery_snapshot(state)
        assert not rs.has_open_positions
        assert not rs.ambiguous_state

    def test_recovery_snapshot_with_positions_at_boot(self):
        state = EngineState(lifecycle=EngineLifecycle.BOOTING)
        state.open_positions["p1"] = OpenPosition(
            position_id="p1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=0.1, short_quantity=0.1,
            long_entry_price=50000, short_entry_price=50100,
            opened_at_ms=1000,
        )
        rs = build_recovery_snapshot(state)
        assert rs.has_open_positions
        assert rs.ambiguous_state

    def test_snapshot_load_restores_state(self):
        with tempfile.TemporaryDirectory() as td:
            journal_path = Path(td) / "events.jsonl"
            snap_path = Path(td) / "state.json"

            snap = SnapshotStore(snap_path)
            snap.write({"lifecycle": "running", "risk_mode": "reduce_only", "tick_count": 42})

            journal = Journal(journal_path)
            journal.open()
            journal.close()

            state = recover_from_snapshot(snap, journal)
            assert state.lifecycle == EngineLifecycle.RUNNING
            assert state.risk_mode == GlobalRiskMode.REDUCE_ONLY
            assert state.tick_count == 42
