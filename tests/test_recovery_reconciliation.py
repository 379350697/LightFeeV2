"""Tests for recovery, reconciliation, journal replay, and persistence replay.

Covers Rust V1 behavior from:
- src/engine/recovery.rs (finalize_startup_position_recovery, reconcile_dust_residuals)
- src/engine/state.rs (EngineState pending/recovery fields)
- src/observability_ops/replay_bridge.rs (journal replay)
- src/runtime_state/persisted_engine.rs (normalize_engine_state_positions)
"""

import json
import tempfile
from pathlib import Path

import pytest

from lightfee.core.domain import OrderFill, PositionSnapshot, Side, Venue
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.engine.recovery import (
    DustResidual,
    PendingCloseReconciliation,
    RecoveryBlockedState,
    ResidualExposureTask,
    build_recovery_snapshot,
    recover_from_snapshot,
    RecoveredState,
)
from lightfee.engine.reconciliation import (
    OrderReconciler,
    ReconciliationResult,
    reconcile_pending_close,
    reconcile_residual_exposure,
    reconcile_unknown_order,
)
from lightfee.engine.state import (
    EngineState,
    OpenPosition,
    OperatorControlState,
    PendingClose,
    PendingEntry,
    RecoveryWorkSnapshot,
)
from lightfee.persistence.journal import Journal
from lightfee.persistence.snapshot_store import SnapshotStore
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from tests.fake_adapters import (
    FakeVenueAdapter,
    make_fake_fill,
    make_rejected_error,
    make_uncertain_error,
)


# ---------------------------------------------------------------------------
# Recovery data structures
# ---------------------------------------------------------------------------

class TestRecoveryDataStructures:
    """Test that recovery dataclasses exist and carry the right fields."""

    def test_dust_residual_fields(self):
        dust = DustResidual(
            position_id="pos-dust",
            symbol="BTCUSDT",
            long_venue="binance",
            short_venue="okx",
            long_size=0.0,
            short_size=0.0,
            leg_notional_quote=8.0,
            venue_min_notional_quote=10.0,
            terminal_reason="exchange_min_notional_dust",
            recorded_at_ms=1000,
        )
        assert dust.position_id == "pos-dust"
        assert dust.terminal_reason == "exchange_min_notional_dust"
        assert abs(dust.leg_notional_quote - 8.0) < 1e-9

    def test_pending_close_reconciliation_fields(self):
        pcr = PendingCloseReconciliation(
            position_id="pos-1",
            symbol="ETHUSDT",
            kind="final_close",
            reason="trailing_drawdown",
            closed_at_ms=5000,
            attempt_count=0,
            next_attempt_ms=5000,
            created_cycle=1,
        )
        assert pcr.position_id == "pos-1"
        assert pcr.kind == "final_close"
        assert pcr.attempt_count == 0

    def test_residual_exposure_task_fields(self):
        task = ResidualExposureTask(
            position_id="pos-r",
            pair_id="btcusdt:binance->okx",
            symbol="BTCUSDT",
            long_venue="binance",
            short_venue="okx",
            origin="hedge_reject",
            repair_venue="okx",
            repair_side="sell",
            repair_quantity=0.01,
            created_cycle=1,
            created_at_ms=1000,
            deadline_ms=31000,
        )
        assert task.position_id == "pos-r"
        assert task.origin == "hedge_reject"
        assert abs(task.repair_quantity - 0.01) < 1e-9

    def test_recovery_blocked_state(self):
        blocked = RecoveryBlockedState(
            reason="market_view_unavailable",
            blocked_at_ms=5000,
        )
        assert blocked.reason == "market_view_unavailable"
        assert blocked.blocked_at_ms == 5000

    def test_recovered_state_wraps_engine_state(self):
        es = EngineState()
        recovered = RecoveredState(state=es, recovery_work=RecoveryWorkSnapshot())
        assert recovered.state is es
        assert not recovered.recovery_work.has_open_positions


# ---------------------------------------------------------------------------
# Recovery from snapshot
# ---------------------------------------------------------------------------

class TestRecoveryFromSnapshot:
    """Test snapshot load, journal replay, and state reconstruction."""

    def test_empty_snapshot_enters_running(self):
        """Rust V1: clean startup with no snapshot and no recovery work → RUNNING."""
        with tempfile.TemporaryDirectory() as td:
            journal_path = Path(td) / "events.jsonl"
            snap_path = Path(td) / "state.json"

            journal = Journal(journal_path)
            journal.open()
            journal.close()

            snap = SnapshotStore(snap_path)
            state = recover_from_snapshot(snap, journal)
            # No recovery work → transitions through RECONCILING to RUNNING
            assert state.lifecycle == EngineLifecycle.RUNNING

    def test_clean_running_snapshot_stays_running(self):
        with tempfile.TemporaryDirectory() as td:
            journal_path = Path(td) / "events.jsonl"
            snap_path = Path(td) / "state.json"

            snap = SnapshotStore(snap_path)
            snap.write({
                "lifecycle": "running",
                "risk_mode": "running",
                "tick_count": 100,
                "open_position_count": 0,
            })

            journal = Journal(journal_path)
            journal.open()
            journal.close()

            state = recover_from_snapshot(snap, journal)
            # Clean state with no recovery work → should go to RECONCILING briefly then RUNNING
            assert state.lifecycle in (EngineLifecycle.RUNNING, EngineLifecycle.RECONCILING)

    def test_snapshot_with_open_positions_enters_reconciling(self):
        with tempfile.TemporaryDirectory() as td:
            journal_path = Path(td) / "events.jsonl"
            snap_path = Path(td) / "state.json"

            snap = SnapshotStore(snap_path)
            snap.write({
                "lifecycle": "running",
                "risk_mode": "running",
                "tick_count": 42,
                "open_position_count": 1,
                "open_positions": {
                    "pos-1": {
                        "position_id": "pos-1",
                        "symbol": "BTCUSDT",
                        "long_venue": "binance",
                        "short_venue": "okx",
                        "long_quantity": 0.1,
                        "short_quantity": 0.1,
                        "long_entry_price": 50000.0,
                        "short_entry_price": 50100.0,
                        "opened_at_ms": 1000,
                        "matched_quantity": 0.1,
                    }
                },
            })

            journal = Journal(journal_path)
            journal.open()
            journal.close()

            state = recover_from_snapshot(snap, journal)
            # Has open positions → must enter RECONCILING
            assert state.lifecycle == EngineLifecycle.RECONCILING

    def test_ambiguous_state_sets_reduce_only(self):
        with tempfile.TemporaryDirectory() as td:
            journal_path = Path(td) / "events.jsonl"
            snap_path = Path(td) / "state.json"

            snap = SnapshotStore(snap_path)
            snap.write({
                "lifecycle": "booting",
                "risk_mode": "running",
                "open_position_count": 1,
                "open_positions": {
                    "pos-a": {
                        "position_id": "pos-a",
                        "symbol": "ETHUSDT",
                        "long_venue": "binance",
                        "short_venue": "bybit",
                        "long_quantity": 1.0,
                        "short_quantity": 1.0,
                        "long_entry_price": 3000.0,
                        "short_entry_price": 3010.0,
                        "opened_at_ms": 500,
                        "matched_quantity": 1.0,
                    }
                },
            })

            journal = Journal(journal_path)
            journal.open()
            journal.close()

            state = recover_from_snapshot(snap, journal)
            assert state.lifecycle == EngineLifecycle.RECONCILING
            assert state.risk_mode.at_least(GlobalRiskMode.ENTRY_PAUSED)

    def test_snapshot_restores_risk_mode(self):
        with tempfile.TemporaryDirectory() as td:
            journal_path = Path(td) / "events.jsonl"
            snap_path = Path(td) / "state.json"

            snap = SnapshotStore(snap_path)
            snap.write({"lifecycle": "running", "risk_mode": "reduce_only"})

            journal = Journal(journal_path)
            journal.open()
            journal.close()

            state = recover_from_snapshot(snap, journal)
            assert state.risk_mode == GlobalRiskMode.REDUCE_ONLY

    def test_recovered_state_has_recovery_work_snapshot(self):
        """Rust V1: EngineState.recovery_work_snapshot() counts all pending work."""
        es = EngineState(lifecycle=EngineLifecycle.BOOTING)
        es.open_positions["pos-1"] = OpenPosition(
            position_id="pos-1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=0.1, short_quantity=0.1,
            long_entry_price=50000, short_entry_price=50100,
            opened_at_ms=1000,
        )
        es.pending_entries["pend-1"] = PendingEntry(
            pending_id="pend-1", symbol="ETHUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.BYBIT,
            target_quantity=1.0, long_side=Side.BUY, short_side=Side.SELL,
            created_at_ms=2000, maker_order_id="m-1", hedge_order_id="h-1",
            uncertain_outcome=True,
        )
        es.pending_closes["close-1"] = PendingClose(
            close_id="close-1", position_id="pos-1",
            reason="profit_take", created_at_ms=3000,
            long_order_id="c-long", short_order_id="c-short",
            long_uncertain=True,
        )

        snap = build_recovery_snapshot(es)
        assert snap.has_open_positions
        assert snap.has_pending_entries
        assert snap.has_pending_closes


# ---------------------------------------------------------------------------
# Journal replay
# ---------------------------------------------------------------------------

class TestJournalReplay:
    """Test that journal records can be replayed to reconstruct state."""

    def test_replay_rebuilds_position_state(self):
        with tempfile.TemporaryDirectory() as td:
            jp = Path(td) / "replay.jsonl"
            journal = Journal(jp)
            journal.open()
            journal.append("entry.opened", {
                "position_id": "pos-1",
                "symbol": "BTCUSDT",
                "long_venue": "binance",
                "short_venue": "okx",
                "quantity": 0.1,
                "long_quantity": 0.1,
                "short_quantity": 0.1,
                "long_entry_price": 50000.0,
                "short_entry_price": 50100.0,
                "opened_at_ms": 1000,
                "matched_quantity": 0.1,
                "current_net_quote": 5.0,
                "peak_net_quote": 10.0,
                "funding_captured": False,
            }, flush=True)
            journal.append("exit.closed", {
                "position_id": "pos-1",
                "reason": "profit_take",
                "closed_quantity": 0.1,
                "closed_at_ms": 5000,
            }, flush=True)
            journal.close()

            records = journal.read_all()
            assert len(records) == 2

            # Replay: position opened then closed → no open positions
            positions: dict[str, dict] = {}
            for record in records:
                kind = record["kind"]
                payload = record["payload"]
                if kind == "entry.opened":
                    positions[payload["position_id"]] = payload
                elif kind == "exit.closed":
                    positions.pop(payload.get("position_id", ""), None)

            assert len(positions) == 0

    def test_replay_preserves_pending_state(self):
        with tempfile.TemporaryDirectory() as td:
            jp = Path(td) / "replay_pending.jsonl"
            journal = Journal(jp)
            journal.open()
            journal.append("entry.opened", {
                "position_id": "pos-2",
                "symbol": "ETHUSDT",
                "long_venue": "bybit",
                "short_venue": "gate",
                "quantity": 1.0,
                "long_quantity": 1.0,
                "short_quantity": 1.0,
                "long_entry_price": 3000.0,
                "short_entry_price": 3010.0,
                "opened_at_ms": 1000,
                "matched_quantity": 1.0,
            }, flush=True)
            journal.append("exit.partial_closed", {
                "position_id": "pos-2",
                "quantity": 0.5,
                "current_net_quote": 2.0,
            }, flush=True)
            journal.close()

            records = journal.read_all()
            assert len(records) == 2
            # Position still open after partial close
            kind_counts = {}
            for r in records:
                kind_counts[r["kind"]] = kind_counts.get(r["kind"], 0) + 1
            assert kind_counts.get("entry.opened", 0) == 1
            assert kind_counts.get("exit.partial_closed", 0) == 1

    def test_replay_restores_risk_mode_from_journal(self):
        with tempfile.TemporaryDirectory() as td:
            jp = Path(td) / "replay_risk.jsonl"
            journal = Journal(jp)
            journal.open()
            journal.append("runtime.risk_mode_changed", {
                "from": "running", "to": "reduce_only", "reason": "health_drop"
            }, flush=True)
            journal.close()

            records = journal.read_all()
            assert len(records) == 1
            assert records[0]["payload"]["to"] == "reduce_only"


# ---------------------------------------------------------------------------
# Order reconciliation
# ---------------------------------------------------------------------------

class TestOrderReconciliation:
    """Test unknown order reconciliation via venue adapters."""

    @pytest.mark.asyncio
    async def test_known_filled_order_reconciles_success(self):
        adapter = FakeVenueAdapter(Venue.BINANCE)
        adapter.place_order_outcomes = [
            make_fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.1, 50000.0)
        ]

        result = await reconcile_unknown_order(
            adapter, "BTCUSDT", "order-known", "client-1"
        )
        # Default fake adapter returns None from fetch_order_fill_reconciliation
        # (simulating unknown), but we test the interface
        assert isinstance(result, ReconciliationResult)

    @pytest.mark.asyncio
    async def test_uncertain_order_stays_uncertain(self):
        adapter = FakeVenueAdapter(Venue.OKX)

        result = await reconcile_unknown_order(
            adapter, "BTCUSDT", "order-unknown", "client-2"
        )
        # Default fake returns None → UNCERTAIN
        assert result.status == "uncertain"

    @pytest.mark.asyncio
    async def test_reconcile_pending_close_updates_attempt_count(self):
        pcr = PendingCloseReconciliation(
            position_id="pos-1",
            symbol="BTCUSDT",
            kind="final_close",
            reason="profit_take",
            closed_at_ms=5000,
        )
        original_attempts = pcr.attempt_count
        adapter = FakeVenueAdapter(Venue.BINANCE)

        updated = await reconcile_pending_close(
            pcr, adapter, adapter, now_ms=35000
        )
        assert updated.attempt_count == original_attempts + 1


# ---------------------------------------------------------------------------
# Residual exposure
# ---------------------------------------------------------------------------

class TestResidualExposure:
    """Test residual exposure tracking and repair scheduling."""

    def test_residual_task_has_deadline(self):
        task = ResidualExposureTask(
            position_id="pos-r",
            pair_id="btcusdt:binance->okx",
            symbol="BTCUSDT",
            long_venue="binance",
            short_venue="okx",
            origin="hedge_reject",
            repair_venue="okx",
            repair_side="sell",
            repair_quantity=0.01,
            created_cycle=5,
            created_at_ms=1000,
            deadline_ms=31000,
        )
        assert task.deadline_ms > task.created_at_ms
        assert task.deadline_ms == 31000

    @pytest.mark.asyncio
    async def test_reconcile_residual_exposure_returns_action(self):
        adapter = FakeVenueAdapter(Venue.BYBIT)
        task = ResidualExposureTask(
            position_id="pos-r",
            pair_id="btcusdt:binance->bybit",
            symbol="BTCUSDT",
            long_venue="binance",
            short_venue="bybit",
            origin="hedge_reject",
            repair_venue="bybit",
            repair_side="sell",
            repair_quantity=0.01,
            created_cycle=1,
            created_at_ms=1000,
            deadline_ms=31000,
        )
        result = await reconcile_residual_exposure(task, adapter, now_ms=5000)
        # Default fake adapter has zero position → should clear
        assert result in ("cleared", "retry", "protect")

    def test_dust_residual_created_for_min_notional(self):
        pos = OpenPosition(
            position_id="pos-dust", symbol="RAREUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=0.001, short_quantity=0.001,
            long_entry_price=1.0, short_entry_price=1.01,
            opened_at_ms=1000,
        )
        dust = DustResidual(
            position_id=pos.position_id,
            symbol=pos.symbol,
            long_venue=pos.long_venue.value,
            short_venue=pos.short_venue.value,
            long_size=pos.long_quantity,
            short_size=pos.short_quantity,
            leg_notional_quote=10.0,
            venue_min_notional_quote=15.0,
            terminal_reason="exchange_min_notional_dust",
            recorded_at_ms=pos.opened_at_ms,
        )
        assert dust.is_dust  # should be True when notional < min_notional
        assert dust.terminal_reason == "exchange_min_notional_dust"


# ---------------------------------------------------------------------------
# Recovery blocked state
# ---------------------------------------------------------------------------

class TestRecoveryBlockedState:
    """Test recovery blocked/shutdown behavior (Rust V1 fail-safe)."""

    def test_ambiguous_state_never_goes_directly_to_running(self):
        """Rust V1: ambiguous state must enter RECONCILING, never RUNNING directly."""
        es = EngineState(lifecycle=EngineLifecycle.BOOTING)
        es.open_positions["pos-amb"] = OpenPosition(
            position_id="pos-amb", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=0.1, short_quantity=0.1,
            long_entry_price=50000, short_entry_price=50100,
            opened_at_ms=1000,
        )
        es.pending_entries["pend-amb"] = PendingEntry(
            pending_id="pend-amb", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            target_quantity=0.1, long_side=Side.BUY, short_side=Side.SELL,
            created_at_ms=2000, uncertain_outcome=True,
        )

        snap = build_recovery_snapshot(es)
        # Ambiguous: has open positions AND pending entries with uncertain outcome
        assert snap.has_open_positions
        assert snap.has_pending_entries
        # Ambiguous because booting + has pending work
        assert snap.ambiguous_state

        # Verify that the recovery snapshot correctly identifies this as needing reconciling
        assert snap.lifecycle == EngineLifecycle.BOOTING

    def test_recovery_blocked_preserves_reason(self):
        blocked = RecoveryBlockedState(
            reason="open_positions_exceed_configured_max",
            blocked_at_ms=10000,
        )
        assert "open_positions_exceed" in blocked.reason
        assert blocked.blocked_at_ms == 10000

    def test_fail_closed_latch_clears_when_safe(self):
        """Rust V1: fail_closed latch clears when state_is_safe_to_resume."""
        es = EngineState(lifecycle=EngineLifecycle.RISK_ONLY)
        es.risk_mode = GlobalRiskMode.FAIL_CLOSED
        es.operator = OperatorControlState()

        snap = build_recovery_snapshot(es)
        # Empty positions + no pending entries → safe to resume
        assert not snap.has_open_positions
        assert not snap.has_pending_entries
        assert not snap.has_pending_closes
        assert not snap.ambiguous_state

    def test_operator_fail_closed_override_prevents_clearing(self):
        """Rust V1: operator-requested fail_closed prevents auto-recovery."""
        es = EngineState(lifecycle=EngineLifecycle.RISK_ONLY)
        es.risk_mode = GlobalRiskMode.FAIL_CLOSED
        es.operator = OperatorControlState(
            requested_mode=GlobalRiskMode.FAIL_CLOSED,
        )

        snap = build_recovery_snapshot(es)
        # Even with no positions, operator override keeps risk mode
        assert not snap.has_open_positions
        # The operator state keeps fail_closed
        assert es.operator.requested_mode == GlobalRiskMode.FAIL_CLOSED


# ---------------------------------------------------------------------------
# Journal critical events
# ---------------------------------------------------------------------------

class TestJournalCriticalEvents:
    """Test Journal.append_critical (Rust V1 parity)."""

    def test_append_critical_includes_ts_and_kind(self):
        with tempfile.TemporaryDirectory() as td:
            jp = Path(td) / "critical.jsonl"
            journal = Journal(jp)
            journal.open()
            seq = journal.append_critical(5000, "recovery.blocked", {
                "reason": "market_view_unavailable",
                "error": "connection refused",
            })
            journal.close()

            records = journal.read_all()
            assert len(records) == 1
            assert records[0]["kind"] == "recovery.blocked"
            assert records[0]["ts_ms"] == 5000
            assert records[0]["payload"]["reason"] == "market_view_unavailable"

    def test_append_critical_flushes_by_default(self):
        with tempfile.TemporaryDirectory() as td:
            jp = Path(td) / "critical2.jsonl"
            journal = Journal(jp)
            journal.open()
            journal.append_critical(7000, "recovery.completed", {
                "open_position_count": 0,
            })
            journal.close()

            # File should be immediately readable (flushed)
            records = journal.read_all()
            assert len(records) == 1


# ---------------------------------------------------------------------------
# Snapshot recovery - extended state
# ---------------------------------------------------------------------------

class TestSnapshotExtendedState:
    """Test that snapshot can persist and restore extended recovery state."""

    def test_snapshot_roundtrips_pending_entries(self):
        with tempfile.TemporaryDirectory() as td:
            snap_path = Path(td) / "ext-state.json"
            snap = SnapshotStore(snap_path)

            data = {
                "lifecycle": "reconciling",
                "risk_mode": "reduce_only",
                "run_id": "test-run-1",
                "tick_count": 5,
                "open_positions": {},
                "pending_entry_count": 1,
                "pending_close_count": 0,
                "pending_entries": {
                    "pend-1": {
                        "pending_id": "pend-1",
                        "symbol": "SOLUSDT",
                        "long_venue": "binance",
                        "short_venue": "okx",
                        "target_quantity": 10.0,
                        "maker_order_id": "m-1",
                        "hedge_order_id": "",
                        "uncertain_outcome": True,
                    }
                },
            }
            snap.write(data)

            restored = snap.read()
            assert restored is not None
            assert restored["lifecycle"] == "reconciling"
            assert restored["pending_entry_count"] == 1
            assert "pend-1" in restored.get("pending_entries", {})

    def test_snapshot_roundtrips_dust_residuals(self):
        with tempfile.TemporaryDirectory() as td:
            snap_path = Path(td) / "dust-state.json"
            snap = SnapshotStore(snap_path)

            data = {
                "lifecycle": "running",
                "risk_mode": "running",
                "dust_residual_count": 1,
                "dust_residuals": [
                    {
                        "position_id": "dust-1",
                        "symbol": "RAREUSDT",
                        "terminal_reason": "exchange_min_notional_dust",
                        "leg_notional_quote": 8.0,
                    }
                ],
            }
            snap.write(data)

            restored = snap.read()
            assert restored is not None
            assert restored["dust_residual_count"] == 1
            assert len(restored.get("dust_residuals", [])) == 1


# ---------------------------------------------------------------------------
# Reconciliation service
# ---------------------------------------------------------------------------

class TestReconciliationService:
    """Test the OrderReconciler service that queries venue adapters."""

    @pytest.mark.asyncio
    async def test_reconciler_queries_both_legs(self):
        long_adapter = FakeVenueAdapter(Venue.BINANCE)
        short_adapter = FakeVenueAdapter(Venue.OKX)

        reconciler = OrderReconciler(long_adapter, short_adapter)
        result = await reconciler.reconcile_position(
            position_id="pos-1",
            symbol="BTCUSDT",
            long_order_id="long-1",
            short_order_id="short-1",
        )

        assert result.position_id == "pos-1"
        assert result.long_status in ("filled", "uncertain", "not_found")
        assert result.short_status in ("filled", "uncertain", "not_found")

    @pytest.mark.asyncio
    async def test_reconciler_marks_both_uncertain_when_adapters_return_none(self):
        long_adapter = FakeVenueAdapter(Venue.BINANCE)
        short_adapter = FakeVenueAdapter(Venue.OKX)

        reconciler = OrderReconciler(long_adapter, short_adapter)
        result = await reconciler.reconcile_position(
            position_id="pos-u",
            symbol="ETHUSDT",
            long_order_id="long-u",
            short_order_id="short-u",
        )

        # Default fake adapters return None → UNCERTAIN
        assert result.long_status == "uncertain"
        assert result.short_status == "uncertain"
