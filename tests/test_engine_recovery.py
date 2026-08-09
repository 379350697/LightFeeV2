"""Tests for engine state, lifecycle, and recovery."""

import json
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
    clear_legacy_recovery_block_via_core,
    normalize_engine_state,
    recover_from_snapshot,
)
from lightfee.engine.recovery_decision_core import (
    RecoveryDecision,
    RecoveryDecisionKind,
    RecoveryEvidenceClass,
    RecoveryEvidenceSnapshot,
    V1RecoveryDecisionCore,
)
from lightfee.engine.state import EngineState, OpenPosition, PendingEntry
from lightfee.core.domain import Side, Venue
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

    def test_clear_risk_mode_for_recovery_requires_core_clear_decision(self):
        state = EngineState()
        enter_fail_closed(state)
        state.recovery_blocked_reason = "exchange_truth_recovery_ledger_blocked"
        state.recovery_blocked_at_ms = 1234

        assert clear_risk_mode_for_recovery(state) is False
        assert state.lifecycle == EngineLifecycle.RISK_ONLY
        assert state.risk_mode == GlobalRiskMode.FAIL_CLOSED
        assert state.recovery_blocked_reason == "exchange_truth_recovery_ledger_blocked"

        core_decision = RecoveryDecision(
            kind=RecoveryDecisionKind.RUNNING_CLEAN,
            evidence_class=RecoveryEvidenceClass.COMPLETE_FLAT,
            entry_allowed=True,
            clear_previous_block=True,
            clear_reason="core_running_clean",
        )
        assert clear_risk_mode_for_recovery(state, core_decision) is True
        assert state.lifecycle == EngineLifecycle.RUNNING
        assert state.risk_mode == GlobalRiskMode.RUNNING
        assert state.recovery_blocked_reason is None
        assert state.recovery_blocked_at_ms == 0

    def test_live_mismatch_fail_closed_latch_is_not_stale_cleanable(self):
        state = EngineState()
        enter_fail_closed(state)
        state.recovery_blocked_reason = "live_position_mismatch_flatten_failed"
        state.recovery_blocked_at_ms = 1234
        state.last_error = "live exchange position mismatch cleanup failed"

        core_decision = RecoveryDecision(
            kind=RecoveryDecisionKind.RUNNING_WITH_EVIDENCE_GAP,
            evidence_class=RecoveryEvidenceClass.PARTIAL_EVIDENCE_GAP,
            entry_allowed=True,
            clear_previous_block=False,
        )
        block_cleared = clear_legacy_recovery_block_via_core(
            state,
            core_decision,
            None,
        )
        fail_closed_cleared = clear_stale_fail_closed_if_recovery_clean(state, None)

        assert block_cleared is False
        assert fail_closed_cleared is False
        assert state.lifecycle == EngineLifecycle.RISK_ONLY
        assert state.risk_mode == GlobalRiskMode.FAIL_CLOSED
        assert state.recovery_blocked_reason == "live_position_mismatch_flatten_failed"
        assert state.recovery_blocked_at_ms == 1234
        assert state.last_error == "live exchange position mismatch cleanup failed"

    def test_legacy_recovery_clearer_does_not_clear_core_owned_ledger_block(self):
        state = EngineState()
        enter_fail_closed(state)
        state.recovery_blocked_reason = "exchange_truth_recovery_ledger_blocked"
        state.recovery_blocked_at_ms = 1234
        state.last_error = "exchange truth recovery ledger blocked"
        core_decision = RecoveryDecision(
            kind=RecoveryDecisionKind.RUNNING_WITH_EVIDENCE_GAP,
            evidence_class=RecoveryEvidenceClass.PARTIAL_EVIDENCE_GAP,
            entry_allowed=True,
            clear_previous_block=True,
        )

        block_cleared = clear_legacy_recovery_block_via_core(
            state,
            core_decision,
            None,
        )

        assert block_cleared is False
        assert state.recovery_blocked_reason == "exchange_truth_recovery_ledger_blocked"
        assert state.recovery_blocked_at_ms == 1234

    def test_legacy_recovery_clearer_requires_core_running_decision(self):
        state = EngineState()
        enter_fail_closed(state)
        state.recovery_blocked_reason = "startup_recovery_pending_work_without_open_positions"
        state.recovery_blocked_at_ms = 1234
        blocked_decision = RecoveryDecision(
            kind=RecoveryDecisionKind.BLOCK_OR_FLATTEN_LIVE_ARTIFACT,
            evidence_class=RecoveryEvidenceClass.ORPHAN_LIVE_ARTIFACT,
            entry_allowed=False,
            block_reason="unpaired_live_position",
        )

        block_cleared = clear_legacy_recovery_block_via_core(
            state,
            blocked_decision,
            None,
        )

        assert block_cleared is False
        assert state.lifecycle == EngineLifecycle.RISK_ONLY
        assert state.risk_mode == GlobalRiskMode.FAIL_CLOSED
        assert state.recovery_blocked_reason == (
            "startup_recovery_pending_work_without_open_positions"
        )

    def test_legacy_recovery_clearer_clears_obsolete_block_without_last_error(self):
        state = EngineState()
        enter_fail_closed(state)
        state.recovery_blocked_reason = "startup_recovery_pending_work_without_open_positions"
        state.recovery_blocked_at_ms = 1234
        state.last_error = "old pending work"
        core_decision = RecoveryDecision(
            kind=RecoveryDecisionKind.RUNNING_WITH_EVIDENCE_GAP,
            evidence_class=RecoveryEvidenceClass.PARTIAL_EVIDENCE_GAP,
            entry_allowed=True,
            clear_previous_block=True,
        )

        block_cleared = clear_legacy_recovery_block_via_core(
            state,
            core_decision,
            None,
        )

        assert block_cleared is True
        assert state.lifecycle == EngineLifecycle.RUNNING
        assert state.risk_mode == GlobalRiskMode.RUNNING
        assert state.recovery_blocked_reason is None
        assert state.recovery_blocked_at_ms == 0
        assert state.last_error == "old pending work"

    def test_legacy_recovery_clearer_requires_core_clear_decision(self):
        state = EngineState()
        enter_fail_closed(state)
        state.recovery_blocked_reason = "startup_recovery_pending_work_without_open_positions"
        state.recovery_blocked_at_ms = 1234
        core_decision = RecoveryDecision(
            kind=RecoveryDecisionKind.RUNNING_WITH_EVIDENCE_GAP,
            evidence_class=RecoveryEvidenceClass.PARTIAL_EVIDENCE_GAP,
            entry_allowed=True,
            clear_previous_block=False,
        )

        block_cleared = clear_legacy_recovery_block_via_core(
            state,
            core_decision,
            None,
        )

        assert block_cleared is False
        assert state.lifecycle == EngineLifecycle.RISK_ONLY
        assert state.risk_mode == GlobalRiskMode.FAIL_CLOSED
        assert state.recovery_blocked_reason == (
            "startup_recovery_pending_work_without_open_positions"
        )

    def test_legacy_recovery_clearer_requires_explicit_core_decision(self):
        state = EngineState()
        enter_fail_closed(state)
        state.recovery_blocked_reason = "startup_recovery_pending_work_without_open_positions"
        state.recovery_blocked_at_ms = 1234

        block_cleared = clear_legacy_recovery_block_via_core(state, None, None)

        assert block_cleared is False
        assert state.lifecycle == EngineLifecycle.RISK_ONLY
        assert state.risk_mode == GlobalRiskMode.FAIL_CLOSED
        assert state.recovery_blocked_reason == (
            "startup_recovery_pending_work_without_open_positions"
        )

    def test_core_marks_clear_for_legacy_migration_block_with_no_local_work(self):
        decision = V1RecoveryDecisionCore().decide(
            RecoveryEvidenceSnapshot(
                exchange_truth=None,
                prior_recovery_block_reason=(
                    "startup_recovery_pending_work_without_open_positions"
                ),
            )
        )

        assert decision.kind == RecoveryDecisionKind.RUNNING_WITH_EVIDENCE_GAP
        assert decision.entry_allowed is True
        assert decision.clear_previous_block is True

    def test_core_keeps_close_reconciliation_visible_without_global_entry_block(self):
        decision = V1RecoveryDecisionCore().decide(
            RecoveryEvidenceSnapshot(
                exchange_truth={"available": True, "positions": [], "open_orders": []},
                recovery_work_items=(
                    {
                        "kind": "pending_close_reconciliation",
                        "blocking": False,
                    },
                ),
            )
        )

        assert decision.kind == RecoveryDecisionKind.RUNNING_WITH_EVIDENCE_GAP
        assert decision.evidence_class == RecoveryEvidenceClass.BACKGROUND_CLOSE_RECONCILIATION
        assert decision.evidence_quality == "background_close_reconciliation"
        assert decision.entry_allowed is True
        assert decision.block_reason is None
        assert decision.clear_reason == "core_background_close_reconciliation"


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

    def test_recovery_snapshot_counts_pending_residual_repairs_as_work(self):
        """Live exchange exposure retained as residual repair is recovery work."""
        state = EngineState(lifecycle=EngineLifecycle.RECONCILING)
        state.pending_residual_repairs.append({
            "position_id": "entry-biousdt",
            "pair_id": "biousdt:bybit->okx",
            "symbol": "BIOUSDT",
            "repair_venue": "bybit",
            "repair_side": "sell",
            "repair_quantity": 1444.0,
            "origin": "entry_open",
        })

        rs = build_recovery_snapshot(state)

        assert rs.ambiguous_state
        assert getattr(rs, "has_pending_residual_repairs") is True

    def test_normalize_retains_invalid_pending_entry_with_exchange_evidence(self):
        """Bad local shape with fill/order evidence must not bypass live-truth recovery."""
        state = EngineState(lifecycle=EngineLifecycle.RECONCILING)
        state.pending_entries["bad-with-evidence"] = PendingEntry(
            pending_id="bad-with-evidence",
            symbol="",
            long_venue=Venue.BYBIT,
            short_venue=Venue.HYPERLIQUID,
            target_quantity=0.0,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=1778787000000,
            maker_order_id="maker-order",
            maker_client_order_id="maker-client",
            maker_leg_filled=1.0,
        )

        normalize_engine_state(state)

        assert "bad-with-evidence" in state.pending_entries
        assert state.recovery_blocked_reason == "invalid_pending_entry_with_exchange_evidence"
        assert state.lifecycle == EngineLifecycle.RISK_ONLY

    def test_current_state_export_exposes_pending_residual_repairs(self, tmp_path):
        from lightfee.config.schema import AppConfig
        from lightfee.engine.loop_control import _export_current_state_snapshot

        state = EngineState(lifecycle=EngineLifecycle.RUNNING)
        state.run_id = "run-test"
        state.last_tick_ms = 1778786999000
        state.pending_residual_repairs.append({
            "position_id": "entry-biousdt",
            "pair_id": "biousdt:bybit->okx",
            "symbol": "BIOUSDT",
            "repair_venue": "bybit",
            "repair_side": "sell",
            "repair_quantity": 1444.0,
            "client_order_id": "cleanup-cid",
            "origin": "entry_open",
        })

        path = tmp_path / "current.json"
        _export_current_state_snapshot(state, str(path), AppConfig())
        data = json.loads(path.read_text())

        assert data["open_position_count"] == 0
        assert data["pending_entry_count"] == 0
        assert data["pending_residual_repair_count"] == 1
        assert data["pending_residual_repairs"][0]["symbol"] == "BIOUSDT"
        assert data["pending_residual_repairs"][0]["client_order_id"] == "cleanup-cid"

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

    def test_ambiguous_open_positions_are_core_evidence_gap_not_recovery_block(self):
        """Ambiguous replay truth is evidence quality, not an independent block."""
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
            assert state.lifecycle == EngineLifecycle.RECONCILING
            assert len(state.open_positions) == 3

            records = journal.read_all()
            blocked = [
                r
                for r in records
                if r.get("kind") == "recovery.blocked"
                and r.get("payload", {}).get("reason") == "ambiguous_live_truth"
            ]
            assert blocked == []
            core_events = [
                r
                for r in records
                if r.get("kind") == "recovery.core.running_with_evidence_gap"
            ]
            assert len(core_events) >= 1
            assert core_events[0]["payload"]["open_position_count"] == 3

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
