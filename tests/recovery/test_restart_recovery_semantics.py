"""Semantic parity tests for restart recovery (REC-001).

Validates that V2 recovery blocks/resumes with V1-equivalent semantics:
- Block when unresolved positions, uncertain entries, or pending closes exist.
- Resume only when all positions reconciled and no uncertain states.
- Journal recovery events.
- Reduce-only mode enforced during recovery.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from lightfee.engine.recovery import (
    RecoveredState,
    RecoveryBlockedState,
    build_recovery_dedup_index,
    build_recovery_snapshot,
    classify_startup_recovery_state,
    clear_legacy_recovery_block_via_core,
    has_lifecycle_blocking_work,
    is_ambiguous_live_truth,
    is_safe_to_resume,
    needs_reconciliation,
    normalize_engine_state,
    recover_from_snapshot,
)
from lightfee.engine.recovery_decision_core import (
    RecoveryDecision,
    RecoveryDecisionKind,
    RecoveryEvidenceClass,
)
from lightfee.engine.state import (
    EngineState,
    HedgeInflight,
    OpenPosition,
    PendingClose,
    PendingEntry,
    PendingPassiveClose,
    RecoveryWorkSnapshot,
)
from lightfee.persistence.journal import Journal
from lightfee.persistence.snapshot_store import SnapshotStore
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_state(**kwargs) -> EngineState:
    s = EngineState(**kwargs)
    return s


def _add_open_position(state: EngineState, pid: str = "p1", symbol: str = "BTC-USDT"):
    from lightfee.core.domain import Venue
    state.open_positions[pid] = OpenPosition(
        position_id=pid, symbol=symbol,
        long_venue=Venue.BINANCE, short_venue=Venue.OKX,
        long_quantity=1.0, short_quantity=1.0,
        long_entry_price=50000.0, short_entry_price=50000.0,
        opened_at_ms=1000,
    )


def _add_pending_entry(state: EngineState, pid: str = "pe1"):
    from lightfee.core.domain import Side, Venue
    state.pending_entries[pid] = PendingEntry(
        pending_id=pid, symbol="BTC-USDT",
        long_venue=Venue.BINANCE, short_venue=Venue.OKX,
        target_quantity=1.0, long_side=Side.BUY, short_side=Side.SELL,
        created_at_ms=1000,
    )


def _add_pending_close(state: EngineState, cid: str = "c1", position_id: str = "p1"):
    state.pending_closes[cid] = PendingClose(
        close_id=cid, position_id=position_id, reason="manual",
        created_at_ms=1000,
    )


# ---------------------------------------------------------------------------
# REC-001: Recovery block/resume semantics
# ---------------------------------------------------------------------------

class TestRecoveryBlockResume:
    """REC-001: Recovery blocks live trading when V1 would block."""

    def test_clean_state_safe_to_resume(self):
        state = EngineState(lifecycle=EngineLifecycle.RUNNING)
        assert is_safe_to_resume(state)
        assert not needs_reconciliation(state)

    def test_open_positions_need_reconciliation(self):
        state = EngineState(lifecycle=EngineLifecycle.RECONCILING)
        _add_open_position(state, "p1")
        assert needs_reconciliation(state)
        assert not is_safe_to_resume(state)

    def test_pending_entries_need_reconciliation(self):
        state = EngineState(lifecycle=EngineLifecycle.RECONCILING)
        _add_pending_entry(state, "pe1")
        assert needs_reconciliation(state)
        assert not is_safe_to_resume(state)

    def test_pending_closes_need_reconciliation(self):
        state = EngineState(lifecycle=EngineLifecycle.RECONCILING)
        _add_open_position(state, "p1")
        _add_pending_close(state, "c1", "p1")
        assert needs_reconciliation(state)
        assert not is_safe_to_resume(state)

    def test_booting_with_positions_is_ambiguous(self):
        state = EngineState(lifecycle=EngineLifecycle.BOOTING)
        _add_open_position(state, "p1")
        assert is_ambiguous_live_truth(state)

    def test_reconciling_without_positions_is_not_ambiguous(self):
        state = EngineState(lifecycle=EngineLifecycle.RECONCILING)
        assert not is_ambiguous_live_truth(state)

    def test_fail_closed_not_safe(self):
        state = EngineState(
            lifecycle=EngineLifecycle.RISK_ONLY,
            risk_mode=GlobalRiskMode.FAIL_CLOSED,
        )
        assert not is_safe_to_resume(state)

    def test_fail_closed_operator_override_not_safe(self):
        state = EngineState(
            lifecycle=EngineLifecycle.RISK_ONLY,
            risk_mode=GlobalRiskMode.FAIL_CLOSED,
        )
        state.operator.requested_mode = GlobalRiskMode.FAIL_CLOSED
        assert not is_safe_to_resume(state)

    def test_clear_legacy_recovery_block_does_not_create_last_error_attribute(self):
        state = EngineState(
            lifecycle=EngineLifecycle.RISK_ONLY,
            risk_mode=GlobalRiskMode.FAIL_CLOSED,
            recovery_blocked_reason="startup_recovery_pending_work_without_open_positions",
            recovery_blocked_at_ms=1234,
        )
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
        assert state.global_risk_reason is None
        assert hasattr(state, "last_error") is False


class TestRecoverySnapshot:
    """REC-001: Recovery snapshot reflects V1-visible recovery state."""

    def test_empty_state_produces_clean_snapshot(self):
        state = EngineState()
        snap = build_recovery_snapshot(state)
        assert not snap.has_open_positions
        assert not snap.has_pending_entries
        assert not snap.has_pending_closes
        assert not snap.ambiguous_state

    def test_state_with_positions_produces_work_snapshot(self):
        state = EngineState()
        _add_open_position(state, "p1")
        snap = build_recovery_snapshot(state)
        assert snap.has_open_positions

    def test_state_with_entries_produces_work_snapshot(self):
        state = EngineState()
        _add_pending_entry(state, "pe1")
        snap = build_recovery_snapshot(state)
        assert snap.has_pending_entries

    def test_booting_with_positions_is_ambiguous(self):
        state = EngineState(lifecycle=EngineLifecycle.BOOTING)
        _add_open_position(state, "p1")
        snap = build_recovery_snapshot(state)
        assert snap.ambiguous_state


class TestRecoveryClassification:
    """REC-001: Startup state classification matches V1."""

    def test_clean_state_classified_clean(self):
        state = EngineState(lifecycle=EngineLifecycle.RUNNING)
        assert classify_startup_recovery_state(state) == "clean"

    def test_state_with_positions_needs_recovery(self):
        state = EngineState()
        _add_open_position(state, "p1")
        assert classify_startup_recovery_state(state) == "recovery_needed"

    def test_state_with_pending_entries_needs_recovery(self):
        state = EngineState()
        _add_pending_entry(state, "pe1")
        assert classify_startup_recovery_state(state) == "recovery_needed"

    def test_ambiguous_state_needs_recovery(self):
        state = EngineState(lifecycle=EngineLifecycle.BOOTING)
        _add_open_position(state, "p1")
        assert classify_startup_recovery_state(state) == "recovery_needed"


class TestLifecycleBlocking:
    """REC-001: Lifecycle-blocking work detection."""

    def test_open_positions_block_lifecycle(self):
        state = EngineState()
        _add_open_position(state, "p1")
        assert has_lifecycle_blocking_work(state)

    def test_pending_entries_block_lifecycle(self):
        state = EngineState()
        _add_pending_entry(state, "pe1")
        assert has_lifecycle_blocking_work(state)

    def test_clean_state_no_blocking(self):
        state = EngineState()
        assert not has_lifecycle_blocking_work(state)


class TestRecoveryDedup:
    """REC-001: Recovery dedup index prevents duplicate orders after restart."""

    def test_empty_state_produces_empty_index(self):
        state = EngineState()
        idx = build_recovery_dedup_index(state)
        assert idx == {}

    def test_pending_entry_client_order_ids_in_index(self):
        state = EngineState()
        pe = PendingEntry(
            pending_id="pe1", symbol="BTC-USDT",
            long_venue="binance", short_venue="okx",
            target_quantity=1.0, long_side="buy", short_side="sell",
            created_at_ms=1000,
            maker_client_order_id="mco-1",
            hedge_client_order_id="hco-1",
        )
        state.pending_entries["pe1"] = pe
        idx = build_recovery_dedup_index(state)
        assert "mco-1" in idx
        assert "hco-1" in idx

    def test_pending_close_client_order_ids_in_index(self):
        state = EngineState()
        pc = PendingClose(
            close_id="c1", position_id="p1", reason="manual",
            created_at_ms=1000,
            long_client_order_id="lco-1",
            short_client_order_id="sco-1",
        )
        state.pending_closes["c1"] = pc
        idx = build_recovery_dedup_index(state)
        assert "lco-1" in idx
        assert "sco-1" in idx

    def test_duplicate_client_order_id_detected(self):
        from lightfee.engine.recovery import is_client_order_id_duplicate
        idx = {"existing-co-id": "pe-1"}
        assert is_client_order_id_duplicate("existing-co-id", idx)
        assert not is_client_order_id_duplicate("new-co-id", idx)
        assert not is_client_order_id_duplicate("", idx)


class TestNormalizeEngineState:
    """REC-001: State normalization after recovery load."""

    def test_dust_positions_removed(self):
        state = EngineState()
        from lightfee.core.domain import Venue
        state.open_positions["dust"] = OpenPosition(
            position_id="dust", symbol="BTC-USDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=0.0, short_quantity=0.0,
            long_entry_price=50000.0, short_entry_price=50000.0,
            opened_at_ms=1000,
        )
        normalize_engine_state(state)
        assert "dust" not in state.open_positions

    def test_matched_quantity_set_if_zero(self):
        state = EngineState()
        from lightfee.core.domain import Venue
        state.open_positions["p1"] = OpenPosition(
            position_id="p1", symbol="BTC-USDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=3.0, short_quantity=2.0,
            long_entry_price=50000.0, short_entry_price=50000.0,
            opened_at_ms=1000,
            matched_quantity=0.0,
        )
        normalize_engine_state(state)
        assert state.open_positions["p1"].matched_quantity == 2.0

    def test_invalid_pending_entry_with_hedge_inflight_blocks_instead_of_crashing(self):
        state = EngineState()
        from lightfee.core.domain import Side, Venue
        state.pending_entries["pe1"] = PendingEntry(
            pending_id="pe1",
            symbol="",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            target_quantity=1.0,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=1000,
            hedge_inflight=HedgeInflight(
                client_order_id="hedge-cid-1",
                venue=Venue.OKX,
                side=Side.SELL,
                quantity=1.0,
                attempt=1,
                submitted_at_ms=1100,
            ),
        )

        normalize_engine_state(state)

        assert "pe1" in state.pending_entries
        assert state.recovery_blocked_reason == "invalid_pending_entry_with_exchange_evidence"
        assert state.lifecycle == EngineLifecycle.RISK_ONLY

    def test_pending_close_without_symbol_field_is_retained(self):
        state = EngineState()
        _add_pending_close(state, "c1", "p1")

        normalize_engine_state(state)

        assert "c1" in state.pending_closes
        assert state.pending_closes["c1"].reconcile_next_attempt_ms == 1000


class TestSnapshotRecovery:
    """REC-001: Recovery from snapshot + journal replay."""

    def test_recover_from_empty_snapshot(self, tmp_path):
        snap = SnapshotStore(tmp_path / "snapshot.json")
        journal = Journal(tmp_path / "journal.jsonl")
        state = recover_from_snapshot(snap, journal)
        # V1: empty snapshot + no recovery work = safe to run
        assert state.lifecycle == EngineLifecycle.RUNNING

    def test_recover_from_snapshot_with_positions(self, tmp_path):
        snap = SnapshotStore(tmp_path / "snapshot.json")
        journal_path = tmp_path / "journal.jsonl"
        # Pre-write a snapshot with an open position
        snap.write({
            "lifecycle": "reconciling",
            "run_id": "test-run",
            "open_positions": {
                "p1": {
                    "position_id": "p1",
                    "symbol": "ETH-USDT",
                    "long_venue": "binance",
                    "short_venue": "okx",
                    "long_quantity": 1.0,
                    "short_quantity": 1.0,
                    "long_entry_price": 3000.0,
                    "short_entry_price": 3000.0,
                    "opened_at_ms": 1000,
                }
            },
        })

        # Need an empty journal file
        journal_path.write_text("")

        journal = Journal(journal_path)
        state = recover_from_snapshot(snap, journal)
        assert len(state.open_positions) == 1
        assert "p1" in state.open_positions

    def test_recover_with_journal_replay_closes_position(self, tmp_path):
        snap = SnapshotStore(tmp_path / "snapshot.json")
        journal_path = tmp_path / "journal.jsonl"

        snap.write({
            "lifecycle": "running",
            "run_id": "test-run",
            "open_positions": {
                "p1": {
                    "position_id": "p1",
                    "symbol": "ETH-USDT",
                    "long_venue": "binance",
                    "short_venue": "okx",
                    "long_quantity": 1.0,
                    "short_quantity": 1.0,
                    "opened_at_ms": 1000,
                }
            },
        })

        # Journal closes p1 after the snapshot
        journal_path.write_text(
            '{"seq":1,"run_id":"test-run","ts_ms":2000,"kind":"exit.closed","payload":{"position_id":"p1"}}\n'
        )

        journal = Journal(journal_path)
        state = recover_from_snapshot(snap, journal)
        assert "p1" not in state.open_positions

    def test_journal_replay_restores_pending_entry_registered_event(self, tmp_path):
        snap = SnapshotStore(tmp_path / "snapshot.json")
        journal_path = tmp_path / "journal.jsonl"

        snap.write({
            "lifecycle": "running",
            "run_id": "test-run",
        })
        journal = Journal(journal_path)
        journal.open()
        try:
            journal.append(
                "entry.pending_registered",
                {
                    "position_id": "entry-1",
                    "symbol": "BTC-USDT",
                    "long_venue": "binance",
                    "short_venue": "okx",
                    "target_quantity": 2.5,
                    "long_side": "buy",
                    "short_side": "sell",
                    "created_at_ms": 1234,
                    "maker_order_id": "maker-order-1",
                    "maker_client_order_id": "maker-client-1",
                    "hedge_order_id": "hedge-order-1",
                    "hedge_client_order_id": "hedge-client-1",
                    "maker_leg_filled": 0.4,
                    "hedge_leg_filled": 0.1,
                },
                flush=True,
                ts_ms=1300,
            )
        finally:
            journal.close()

        state = recover_from_snapshot(snap, Journal(journal_path))

        assert "entry-1" in state.pending_entries
        pending = state.pending_entries["entry-1"]
        assert pending.pending_id == "entry-1"
        assert pending.symbol == "BTC-USDT"
        assert pending.target_quantity == 2.5
        assert pending.maker_order_id == "maker-order-1"
        assert pending.hedge_client_order_id == "hedge-client-1"
        assert pending.maker_leg_filled == 0.4
        assert pending.hedge_leg_filled == 0.1
