"""Tests for engine state, lifecycle, and recovery."""

import tempfile
from pathlib import Path

import pytest

from lightfee.engine.lifecycle import (
    can_enter_new_positions,
    clear_risk_mode_for_recovery,
    enter_fail_closed,
    set_global_risk_mode,
    set_lifecycle,
)
from lightfee.engine.recovery import (
    build_recovery_snapshot,
    clear_stale_fail_closed_if_recovery_clean,
    clear_stale_recovery_block_if_recovery_clean,
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

    def test_clear_risk_mode_for_recovery_clears_blocked_reason(self):
        state = EngineState()
        enter_fail_closed(state)
        state.recovery_blocked_reason = "live_position_mismatch_flatten_failed"
        state.recovery_blocked_at_ms = 1234

        clear_risk_mode_for_recovery(state)

        assert state.lifecycle == EngineLifecycle.RUNNING
        assert state.risk_mode == GlobalRiskMode.RUNNING
        assert state.recovery_blocked_reason is None
        assert state.recovery_blocked_at_ms == 0

    def test_clean_live_mismatch_fail_closed_latch_auto_clears_like_v1(self):
        state = EngineState()
        enter_fail_closed(state)
        state.recovery_blocked_reason = "live_position_mismatch_flatten_failed"
        state.recovery_blocked_at_ms = 1234
        state.last_error = "live exchange position mismatch cleanup failed"

        block_cleared = clear_stale_recovery_block_if_recovery_clean(state, None)
        fail_closed_cleared = clear_stale_fail_closed_if_recovery_clean(state, None)

        assert block_cleared is True
        assert fail_closed_cleared is False
        assert state.lifecycle == EngineLifecycle.RUNNING
        assert state.risk_mode == GlobalRiskMode.RUNNING
        assert state.recovery_blocked_reason is None
        assert state.recovery_blocked_at_ms == 0
        assert state.last_error is None


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

    def test_recovery_snapshot_includes_pending_passive_closes(self):
        """Rust V1: recovery snapshot must count pending passive closes."""
        from lightfee.engine.state import (
            PendingPassiveClose, ActiveMakerLeg, PassiveExecutionPhase,
            PassivePhaseState, PendingPassiveLegFill,
        )
        state = EngineState(lifecycle=EngineLifecycle.RECONCILING)
        state.pending_passive_closes["ppc-1"] = PendingPassiveClose(
            position_id="pos-1",
            reason="trailing_drawdown",
            target_quantity=0.1,
            chunk_quantities=[0.05, 0.05],
            active_chunk_index=0,
        )
        rs = build_recovery_snapshot(state)
        assert rs.has_pending_passive_closes
        assert rs.has_open_positions is False

    def test_snapshot_restores_local_l2_state(self):
        """Rust V1: local-L2 retained books, books snapshot, session snapshot restore as resume-waiting."""
        from lightfee.engine.recovery import _restore_state_from_snapshot_dict
        snap = {
            "lifecycle": "running",
            "risk_mode": "running",
            "retained_local_l2_books": [
                {"venue": "binance", "symbol": "BTCUSDT", "generation": 3},
            ],
            "local_l2_books_snapshot": [
                {"venue": "okx", "symbol": "ETHUSDT", "bids": []},
            ],
            "local_l2_session_snapshot": [
                {"venue": "bybit", "symbol": "SOLUSDT"},
            ],
        }
        state = _restore_state_from_snapshot_dict(snap)
        assert len(state.retained_local_l2_books) == 1
        assert state.retained_local_l2_books[0]["venue"] == "binance"
        assert len(state.local_l2_books_snapshot) == 1
        assert state.local_l2_books_snapshot[0]["symbol"] == "ETHUSDT"
        assert len(state.local_l2_session_snapshot) == 1

    def test_recovery_blocked_preserves_diagnostic_open_positions(self):
        """Rust V1: recovery.blocked must include open_position_count for audit."""
        with tempfile.TemporaryDirectory() as td:
            journal_path = Path(td) / "events.jsonl"
            snap_path = Path(td) / "state.json"

            snap = SnapshotStore(snap_path)
            snap.write({
                "lifecycle": "booting",
                "risk_mode": "running",
                "open_position_count": 3,
                "open_positions": {
                    f"pos-{i}": {
                        "position_id": f"pos-{i}",
                        "symbol": "BTCUSDT",
                        "long_venue": "binance",
                        "short_venue": "okx",
                        "long_quantity": 0.1,
                        "short_quantity": 0.1,
                        "long_entry_price": 50000,
                        "short_entry_price": 50100,
                        "opened_at_ms": 1000 * i,
                        "matched_quantity": 0.1,
                    }
                    for i in range(3)
                },
            })

            journal = Journal(journal_path)
            journal.open()
            journal.close()

            state = recover_from_snapshot(snap, journal)
            # Has open positions with booting lifecycle → ambiguous → blocked + RECONCILING
            assert state.lifecycle == EngineLifecycle.RECONCILING
            assert len(state.open_positions) == 3

            # Read journal to check recovery.blocked event was emitted
            records = journal.read_all()
            blocked = [r for r in records if r.get("kind") == "recovery.blocked"]
            assert len(blocked) >= 1, "ambiguous recovery must emit recovery.blocked"
            assert blocked[0]["payload"]["open_position_count"] == 3

    def test_recovery_flat_emitted_when_position_closed_in_journal(self):
        """Rust V1: recovery.flat emitted when snapshot position is closed by journal events."""
        with tempfile.TemporaryDirectory() as td:
            journal_path = Path(td) / "events.jsonl"
            snap_path = Path(td) / "state.json"

            snap = SnapshotStore(snap_path)
            snap.write({
                "lifecycle": "running",
                "risk_mode": "running",
                "open_positions": {
                    "pos-close-me": {
                        "position_id": "pos-close-me",
                        "symbol": "ETHUSDT",
                        "long_venue": "binance",
                        "short_venue": "okx",
                        "long_quantity": 1.0,
                        "short_quantity": 1.0,
                        "long_entry_price": 3000,
                        "short_entry_price": 3010,
                        "opened_at_ms": 1000,
                        "matched_quantity": 1.0,
                    },
                },
            })

            journal = Journal(journal_path)
            journal.open()
            journal.append("exit.closed", {
                "position_id": "pos-close-me",
                "reason": "profit_take",
            }, flush=True)
            journal.close()

            state = recover_from_snapshot(snap, journal)
            assert "pos-close-me" not in state.open_positions

            # recovery.flat should be emitted
            records = journal.read_all()
            flat_events = [r for r in records if r.get("kind") == "recovery.flat"]
            assert len(flat_events) >= 1, "position closed in journal must emit recovery.flat"

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
