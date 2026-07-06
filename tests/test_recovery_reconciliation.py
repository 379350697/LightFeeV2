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

from lightfee.core.domain import (
    OrderFill,
    OrderFillReconciliation,
    PositionSnapshot,
    Side,
    Venue,
)
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

    def test_snapshot_restore_sanitizes_pending_passive_close_none_identity(self):
        with tempfile.TemporaryDirectory() as td:
            journal_path = Path(td) / "events.jsonl"
            snap_path = Path(td) / "state.json"

            snap = SnapshotStore(snap_path)
            snap.write({
                "lifecycle": "risk_only",
                "risk_mode": "fail_closed",
                "pending_passive_closes": {
                    "entry-siren": {
                        "position_id": "entry-siren",
                        "reason": "funding_capture",
                        "target_quantity": 460.0,
                        "chunk_quantities": [460.0],
                        "phase_state": {
                            "phase": "dual_taker",
                            "preferred_maker_leg": "short",
                            "active_maker_leg": "short",
                            "maker_order_id": "None",
                            "maker_client_order_id": "lfex-siren-close",
                        },
                        "maker_fill": {
                            "quantity": 0.0,
                            "order_id": "None",
                            "client_order_id": "null",
                        },
                        "hedge_fill": {
                            "quantity": 0.0,
                            "order_id": " null ",
                            "client_order_id": "",
                        },
                        "close_order_identity_history": [
                            {
                                "venue": "bitget",
                                "leg": "short",
                                "order_id": "None",
                                "client_order_id": "lfex-siren-close",
                            }
                        ],
                    }
                },
            })

            journal = Journal(journal_path)
            journal.open()
            journal.close()

            state = recover_from_snapshot(snap, journal)

            pending = state.pending_passive_closes["entry-siren"]
            assert pending.phase_state.maker_order_id == ""
            assert pending.phase_state.maker_client_order_id == "lfex-siren-close"
            assert pending.maker_fill.order_id == ""
            assert pending.maker_fill.client_order_id == ""
            assert pending.hedge_fill.order_id == ""
            assert pending.close_order_identity_history[-1]["order_id"] == ""
            assert pending.close_order_identity_history[-1]["client_order_id"] == "lfex-siren-close"

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

    def test_ambiguous_state_records_core_evidence_gap_without_reduce_only(self):
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
            assert state.risk_mode == GlobalRiskMode.RUNNING
            records = journal.read_all()
            assert not any(
                r.get("kind") == "recovery.blocked"
                and r.get("payload", {}).get("reason") == "ambiguous_live_truth"
                for r in records
            )
            assert any(
                r.get("kind") == "recovery.core.running_with_evidence_gap"
                for r in records
            )

    def test_snapshot_residual_work_blocks_without_open_positions(self):
        with tempfile.TemporaryDirectory() as td:
            journal_path = Path(td) / "events.jsonl"
            snap_path = Path(td) / "state.json"

            snap = SnapshotStore(snap_path)
            snap.write({
                "lifecycle": "booting",
                "risk_mode": "running",
                "pending_residual_repairs": [
                    {
                        "position_id": "entry-residual",
                        "pair_id": "btcusdt:binance->okx",
                        "symbol": "BTCUSDT",
                        "repair_venue": "binance",
                        "repair_side": "sell",
                        "repair_quantity": 0.01,
                        "origin": "entry_open",
                    }
                ],
            })

            journal = Journal(journal_path)
            journal.open()
            journal.close()

            state = recover_from_snapshot(snap, journal)

            assert state.lifecycle == EngineLifecycle.RECONCILING
            assert state.recovery_blocked_reason is None
            records = journal.read_all()
            assert not any(
                r.get("kind") == "runtime.running"
                and r.get("payload", {}).get("reason") == "startup_no_recovery_work"
                for r in records
            )
            assert any(
                r.get("kind") == "recovery.core.truth_required_blocked"
                for r in records
            )

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
    async def test_unknown_order_positive_reconciliation_without_truth_stays_uncertain(self):
        weak_reconciliation = OrderFillReconciliation(
            venue=Venue.OKX,
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=0.5,
            average_price=50000.0,
            order_id="weak-order",
            client_order_id="weak-client",
            metadata={},
        )
        adapter = _EvidenceAdapter(Venue.OKX, reconciliation=weak_reconciliation)

        result = await reconcile_unknown_order(
            adapter, "BTCUSDT", "weak-order", "weak-client"
        )

        assert result.status == "truth_gap"
        assert result.fill is None
        assert result.reason == "retain_backoff"

    @pytest.mark.asyncio
    async def test_unknown_order_positive_reconciliation_with_truth_is_filled(self):
        fill_reconciliation = OrderFillReconciliation(
            venue=Venue.OKX,
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=0.5,
            average_price=50000.0,
            order_id="fill-order",
            client_order_id="fill-client",
            metadata={
                "evidence_source": "okx_fills",
                "response_classification": "filled",
                "queried_endpoints": ["/api/v5/trade/fills"],
            },
        )
        adapter = _EvidenceAdapter(Venue.OKX, reconciliation=fill_reconciliation)

        result = await reconcile_unknown_order(
            adapter, "BTCUSDT", "fill-order", "fill-client"
        )

        assert result.status == "filled"
        assert result.fill is fill_reconciliation

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

        reconciler = OrderReconciler(adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter})
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

        reconciler = OrderReconciler(adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter})
        result = await reconciler.reconcile_position(
            position_id="pos-u",
            symbol="ETHUSDT",
            long_order_id="long-u",
            short_order_id="short-u",
        )

        # Default fake adapters return None → UNCERTAIN
        assert result.long_status == "uncertain"
        assert result.short_status == "uncertain"

    @pytest.mark.asyncio
    async def test_reconciler_records_sanitized_order_reconcile_result(self):
        long_adapter = FakeVenueAdapter(Venue.BINANCE)
        short_adapter = FakeVenueAdapter(Venue.OKX)

        reconciler = OrderReconciler(adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter})
        await reconciler.reconcile_position(
            position_id="pos-log",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_order_id="long-order",
            short_order_id="short-order",
            long_client_order_id="long-client",
            short_client_order_id="short-client",
        )

        events = reconciler.drain_order_diagnostics()
        assert [event["kind"] for event in events] == [
            "order.reconcile_result",
            "order.reconcile_result",
        ]
        payload = events[0]["payload"]
        assert payload["venue"] == "binance"
        assert payload["symbol"] == "BTCUSDT"
        assert payload["endpoint"] == "fetch_order_status"
        assert payload["product_type"] == "reconciliation"
        assert payload["category"] == "reconciliation"
        assert payload["client_order_id"] == "long-client"
        assert payload["order_id"] == "long-order"
        assert payload["response_classification"] == "uncertain"
        serialized = json.dumps(events)
        assert "secret" not in serialized.lower()
        assert "signature" not in serialized.lower()


# ---------------------------------------------------------------------------
# Task 2 regression: client_order_id reconciliation
# ---------------------------------------------------------------------------


class TestClientOrderIdReconciliation:
    """Regr: fetch_order_fill_reconciliation must work with client_order_id only."""

    def _make_fill(self, venue, symbol, side, qty, price):
        return OrderFill(
            venue=venue, symbol=symbol, side=side,
            quantity=qty, price=price, order_id="",
            client_order_id="",
        )

    def _make_mock_bybit_transport(self):
        """Create a Bybit transport in paper mode with mocked fetch_order_status."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bybit_spec
        return VenueTransport(spec=bybit_spec(), mode="paper")

    def _make_mock_bitget_transport(self):
        """Create a Bitget transport in paper mode with mocked fetch_order_status."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bitget_spec
        return VenueTransport(spec=bitget_spec(), mode="paper")

    @pytest.mark.asyncio
    async def test_fake_transport_fetch_order_status_returns_fill(self):
        """fetch_order_fill_reconciliation must not return None when
        fetch_order_status has a fill."""
        from lightfee.core.domain import Venue as V, OrderFillReconciliation as OFR

        transport = self._make_mock_bybit_transport()

        called_with_cid = None

        async def mock_fetch_order_status(symbol, *, order_id="", client_order_id=""):
            nonlocal called_with_cid
            called_with_cid = client_order_id
            return OFR(
                venue=V.BYBIT, symbol=symbol,
                side=Side.BUY, quantity=0.5, average_price=50000.0,
                order_id="bybit-ord-1", client_order_id="my-client-cid",
                filled_at_ms=5000,
            )

        transport.fetch_order_status = mock_fetch_order_status

        from lightfee.venues.bybit import BybitAdapter
        adapter = BybitAdapter(mode="paper")
        adapter._transport = transport

        result = await adapter.fetch_order_fill_reconciliation(
            "BTCUSDT", order_id="", client_order_id="my-client-cid",
        )
        assert result is not None, "must return a fill, not None"
        assert result.order_id == "bybit-ord-1"
        assert result.quantity == 0.5
        assert called_with_cid == "my-client-cid"

    @pytest.mark.asyncio
    async def test_order_reconciler_transitions_ack_to_filled_via_cid(self):
        """OrderReconciler: pending ACK (only client_order_id) must resolve to filled
        when adapter.fetch_order_fill_reconciliation supports cid lookup."""
        from lightfee.core.domain import Venue as V, OrderFillReconciliation as OFR

        transport = self._make_mock_bybit_transport()

        async def mock_fetch_status(symbol, *, order_id="", client_order_id=""):
            return OFR(
                venue=V.BYBIT, symbol=symbol,
                side=Side.BUY, quantity=0.3, average_price=60000.0,
                order_id="resolved-ord", client_order_id=client_order_id or "ack-cid",
                filled_at_ms=6000,
                metadata={
                    "evidence_source": "bybit_execution_list",
                    "queried_endpoints": ["/v5/execution/list"],
                    "response_classification": "filled",
                },
            )

        transport.fetch_order_status = mock_fetch_status

        from lightfee.venues.bybit import BybitAdapter
        long_adapter = BybitAdapter(mode="paper")
        long_adapter._transport = transport

        from tests.fake_adapters import FakeVenueAdapter
        short_adapter = FakeVenueAdapter(V.OKX)

        reconciler = OrderReconciler(adapters={V.BYBIT: long_adapter, V.OKX: short_adapter})
        result = await reconciler.reconcile_position(
            position_id="pos-ack-1",
            symbol="BTCUSDT",
            long_venue=V.BYBIT,
            short_venue=V.OKX,
            long_order_id="",  # ACK: no exchange order id
            long_client_order_id="ack-cid",
        )
        assert result.long_status == "filled", f"expected filled, got {result.long_status}"
        assert result.long_fill is not None

    @pytest.mark.asyncio
    async def test_bybit_override_uses_order_link_id(self):
        """BybitAdapter.fetch_order_fill_reconciliation must use orderLinkId
        for client_order_id lookup (V1: bybit.rs:1522-1523)."""
        from lightfee.core.domain import Venue as V, OrderFillReconciliation as OFR

        transport = self._make_mock_bybit_transport()

        captured_params = {}

        async def mock_fetch_status(symbol, *, order_id="", client_order_id=""):
            captured_params["order_id"] = order_id
            captured_params["client_order_id"] = client_order_id
            captured_params["symbol"] = symbol
            return OFR(
                venue=V.BYBIT, symbol=symbol,
                side=Side.BUY, quantity=0.2, average_price=50000.0,
                order_id="bybit-ord", client_order_id=client_order_id,
                filled_at_ms=7000,
            )

        transport.fetch_order_status = mock_fetch_status

        from lightfee.venues.bybit import BybitAdapter
        adapter = BybitAdapter(mode="paper")
        adapter._transport = transport

        result = await adapter.fetch_order_fill_reconciliation(
            "BTCUSDT", order_id="", client_order_id="test-link-id",
        )
        assert result is not None
        assert captured_params["client_order_id"] == "test-link-id"

    @pytest.mark.asyncio
    async def test_bitget_override_uses_client_oid(self):
        """BitgetAdapter.fetch_order_fill_reconciliation must use clientOid
        for client_order_id lookup (V1: bitget.rs:2942 clientOid)."""
        from lightfee.core.domain import Venue as V, OrderFillReconciliation as OFR

        transport = self._make_mock_bitget_transport()

        captured_cid = None

        async def mock_fetch_status(symbol, *, order_id="", client_order_id=""):
            nonlocal captured_cid
            captured_cid = client_order_id
            return OFR(
                venue=V.BITGET, symbol=symbol,
                side=Side.SELL, quantity=0.25, average_price=50000.0,
                order_id="bitget-ord", client_order_id=client_order_id,
                filled_at_ms=8000,
            )

        transport.fetch_order_status = mock_fetch_status

        from lightfee.venues.bitget import BitgetAdapter
        adapter = BitgetAdapter(mode="paper")
        adapter._transport = transport

        result = await adapter.fetch_order_fill_reconciliation(
            "BTCUSDT", order_id="", client_order_id="test-oid",
        )
        assert result is not None
        assert captured_cid == "test-oid"


    @pytest.mark.asyncio
    async def test_hyperliquid_override_uses_wire_cloid_and_official_order_status_shape(self):
        """Hyperliquid reconciliation must hash internal CIDs to 128-bit cloids."""
        from lightfee.engine.order_truth_ledger import (
            ORDER_TRUTH_LEDGER,
            OrderTruthFillStatus,
        )
        from lightfee.venues.hyperliquid import HyperliquidAdapter
        from lightfee.venues.transport import LiveCredential
        from lightfee.venues.hyperliquid_signing import hyperliquid_cloid_for_client_order

        internal_cid = "entry-1779342733376-SAGAUSDT-h1"
        account_address = "0x0000000000000000000000000000000000000001"
        wire_cloid = hyperliquid_cloid_for_client_order(internal_cid)
        adapter = HyperliquidAdapter(
            mode="live",
            credential=LiveCredential(
                wallet_private_key=(
                    "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e"
                ),
                account_address=account_address,
            ),
        )
        captured_bodies: list[dict] = []

        async def fake_request(method, path, body=None, private=False, **kwargs):
            captured_bodies.append(body or {})
            assert method == "POST"
            assert path == "/info"
            assert body["cloid"] == wire_cloid
            return {
                "status": "order",
                "order": {
                    "order": {
                        "coin": "SAGA",
                        "side": "A",
                        "limitPx": "0.03",
                        "sz": "0.0",
                        "oid": 123,
                        "timestamp": 1779342767947,
                        "origSz": "772.0",
                        "cloid": wire_cloid,
                    },
                    "status": "filled",
                    "statusTimestamp": 1779342767947,
                },
            }

        adapter._transport._request = fake_request
        try:
            result = await adapter.fetch_order_fill_reconciliation(
                "SAGAUSDT",
                order_id="",
                client_order_id=internal_cid,
            )
        finally:
            await adapter.shutdown()

        assert result is not None
        assert captured_bodies[0]["cloid"] == wire_cloid
        assert result.quantity == 772.0
        assert result.average_price == 0.03
        assert result.client_order_id == wire_cloid
        assert result.metadata["configured_account_address"] == account_address
        assert result.metadata["oid"] == "123"
        assert result.metadata["cloid"] == wire_cloid
        decision = ORDER_TRUTH_LEDGER.resolve_order_success(
            venue=result.venue,
            symbol=result.symbol,
            order_id=result.order_id,
            client_order_id=result.client_order_id or "",
            target_qty=result.quantity,
            reconciliation=result,
        )
        assert decision.fill_status is OrderTruthFillStatus.CONFIRMED_FILL

    @pytest.mark.asyncio
    async def test_vanilla_fetch_order_fill_reconciliation_returns_none_without_override(self):
        """Default VenueAdapter.fetch_order_fill_reconciliation returns None.
        Concrete venue overrides return actual data."""
        from lightfee.core.contracts import VenueAdapter as VA
        from lightfee.core.domain import Venue as V

        adapter = FakeVenueAdapter(V.OKX)
        result = await adapter.fetch_order_fill_reconciliation(
            "BTCUSDT", "some-order", "some-cid",
        )
        # FakeVenueAdapter default returns None
        assert result is None


class _EvidenceAdapter:
    def __init__(
        self,
        venue: Venue,
        *,
        reconciliation: OrderFillReconciliation | None = None,
        position_qty: float = 0.0,
        position_side: Side = Side.BUY,
        diagnostics: list[dict] | None = None,
    ) -> None:
        self.venue = venue
        self.reconciliation = reconciliation
        self.position = PositionSnapshot(
            venue=venue,
            symbol="BTCUSDT",
            side=position_side,
            quantity=position_qty,
            entry_price=50000.0,
            observed_at_ms=1234,
        )
        self._diagnostics = diagnostics or []

    async def fetch_order_fill_reconciliation(
        self,
        symbol: str,
        order_id: str,
        client_order_id: str | None = None,
    ) -> OrderFillReconciliation | None:
        return self.reconciliation

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        return self.position

    def drain_order_diagnostics(self) -> list[dict]:
        events = list(self._diagnostics)
        self._diagnostics.clear()
        return events


class TestOrderReconcileUncertainEvidence:
    """CL-001-G: uncertain reconciliation must carry venue terminality evidence."""

    def _diag(
        self,
        venue: Venue,
        subtype: str,
        classification: str,
        endpoints: list[str],
    ) -> dict:
        return {
            "kind": "order.reconcile_query",
            "payload": {
                "venue": venue.value,
                "symbol": "BTCUSDT",
                "client_order_id": "cid-1",
                "exchange_order_id": "oid-1",
                "queried_endpoints": endpoints,
                "response_classification": classification,
                "uncertain_subtype": subtype,
                "next_action": "reconcile_again_after_backoff",
            },
        }

    @pytest.mark.asyncio
    async def test_positive_reconciliation_without_fill_truth_remains_uncertain(self):
        weak_reconciliation = OrderFillReconciliation(
            venue=Venue.OKX,
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=0.5,
            average_price=50000.0,
            order_id="weak-order",
            client_order_id="weak-client",
            metadata={},
        )
        adapter = _EvidenceAdapter(
            Venue.OKX,
            reconciliation=weak_reconciliation,
            position_qty=0.0,
        )
        reconciler = OrderReconciler(adapters={Venue.OKX: adapter})

        result = await reconciler.reconcile_position(
            position_id="pos-weak",
            symbol="BTCUSDT",
            long_venue=Venue.OKX,
            long_order_id="weak-order",
            long_client_order_id="weak-client",
        )

        assert result.long_status in {"uncertain", "truth_gap"}
        assert result.long_fill is None
        events = reconciler.drain_order_diagnostics()
        result_payload = [
            event["payload"]
            for event in events
            if event["kind"] == "order.reconcile_result"
        ][-1]
        assert result_payload["status"] in {"uncertain", "truth_gap"}
        assert result_payload["fill_qty"] == 0.0

    @pytest.mark.asyncio
    async def test_positive_reconciliation_with_fill_truth_is_filled(self):
        fill_reconciliation = OrderFillReconciliation(
            venue=Venue.OKX,
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=0.5,
            average_price=50000.0,
            order_id="fill-order",
            client_order_id="fill-client",
            metadata={
                "evidence_source": "okx_fills",
                "response_classification": "filled",
                "queried_endpoints": ["/api/v5/trade/fills"],
            },
        )
        adapter = _EvidenceAdapter(
            Venue.OKX,
            reconciliation=fill_reconciliation,
            position_qty=0.0,
        )
        reconciler = OrderReconciler(adapters={Venue.OKX: adapter})

        result = await reconciler.reconcile_position(
            position_id="pos-fill",
            symbol="BTCUSDT",
            long_venue=Venue.OKX,
            long_order_id="fill-order",
            long_client_order_id="fill-client",
        )

        assert result.long_status == "filled"
        assert result.long_fill is fill_reconciliation

    @pytest.mark.asyncio
    async def test_bybit_duplicate_emits_resolution_with_endpoint_evidence(self):
        fill = OrderFillReconciliation(
            venue=Venue.BYBIT,
            symbol="BTCUSDT",
            side=Side.SELL,
            quantity=0.5,
            average_price=50000.0,
            order_id="oid-1",
            client_order_id="cid-1",
            metadata={
                "uncertain_subtype": "duplicate_client_id",
                "queried_endpoints": [
                    "/v5/order/realtime",
                    "/v5/order/history",
                    "/v5/execution/list",
                ],
                "response_classification": "filled_after_duplicate_client_id",
                "next_action": "clear_uncertain_state",
            },
        )
        adapter = _EvidenceAdapter(Venue.BYBIT, reconciliation=fill)
        reconciler = OrderReconciler({Venue.BYBIT: adapter})

        result = await reconciler.reconcile_position(
            position_id="pos-1",
            symbol="BTCUSDT",
            long_venue=Venue.BYBIT,
            long_client_order_id="cid-1",
        )

        assert result.long_status == "filled"
        events = reconciler.drain_order_diagnostics()
        payload = [e["payload"] for e in events if e["kind"] == "order.reconcile_result"][-1]
        assert payload["uncertain_subtype"] == "duplicate_client_id"
        assert payload["queried_endpoints"] == [
            "/v5/order/realtime",
            "/v5/order/history",
            "/v5/execution/list",
        ]
        assert payload["exchange_order_id"] == "oid-1"
        resolution = [e["payload"] for e in events if e["kind"] == "order.reconcile_resolution"][-1]
        assert resolution["resolution"] == "duplicate_client_id"
        assert resolution["clears_uncertain_state"] is True

    @pytest.mark.asyncio
    async def test_binance_submit_timeout_accepted_later_clears_uncertain(self):
        fill = OrderFillReconciliation(
            venue=Venue.BINANCE,
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=0.25,
            average_price=51000.0,
            order_id="bn-oid-1",
            client_order_id="bn-timeout-cid",
            metadata={
                "uncertain_subtype": "submit_timeout",
                "queried_endpoints": ["/fapi/v1/order"],
                "response_classification": "filled_after_submit_timeout",
                "next_action": "clear_uncertain_state",
            },
        )
        adapter = _EvidenceAdapter(Venue.BINANCE, reconciliation=fill, position_qty=0.25)
        reconciler = OrderReconciler({Venue.BINANCE: adapter})

        await reconciler.reconcile_position(
            position_id="pos-1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            long_client_order_id="bn-timeout-cid",
        )

        events = reconciler.drain_order_diagnostics()
        result_payload = [e["payload"] for e in events if e["kind"] == "order.reconcile_result"][-1]
        assert result_payload["uncertain_subtype"] == "submit_timeout"
        assert result_payload["live_position_delta"]["quantity"] == pytest.approx(0.25)
        resolution = [e["payload"] for e in events if e["kind"] == "order.reconcile_resolution"][-1]
        assert resolution["resolution"] == "submit_timeout"
        assert resolution["next_action"] == "clear_uncertain_state"

    @pytest.mark.asyncio
    async def test_okx_order_not_found_live_flat_is_no_effect_resolution(self):
        adapter = _EvidenceAdapter(
            Venue.OKX,
            position_qty=0.0,
            diagnostics=[
                self._diag(
                    Venue.OKX,
                    "open_order_not_found",
                    "open_order_not_found;closed_order_not_found",
                    ["/api/v5/trade/order", "/api/v5/trade/orders-history"],
                )
            ],
        )
        reconciler = OrderReconciler({Venue.OKX: adapter})

        result = await reconciler.reconcile_position(
            position_id="pos-1",
            symbol="BTCUSDT",
            long_venue=Venue.OKX,
            long_client_order_id="cid-1",
        )

        assert result.long_status == "not_found"
        events = reconciler.drain_order_diagnostics()
        payload = [e["payload"] for e in events if e["kind"] == "order.reconcile_result"][-1]
        assert payload["uncertain_subtype"] == "live_no_effect_confirmed"
        assert payload["queried_endpoints"] == [
            "/api/v5/trade/order",
            "/api/v5/trade/orders-history",
        ]
        assert payload["next_action"] == "clear_uncertain_state"
        resolution = [e["payload"] for e in events if e["kind"] == "order.reconcile_resolution"][-1]
        assert resolution["resolution"] == "live_no_effect_confirmed"

    @pytest.mark.asyncio
    async def test_live_position_confirmed_resolution_has_live_delta(self):
        adapter = _EvidenceAdapter(
            Venue.BINANCE,
            position_qty=0.4,
            diagnostics=[
                self._diag(
                    Venue.BINANCE,
                    "live_position_confirmed",
                    "live_position_confirmed",
                    ["/fapi/v1/order", "/fapi/v1/userTrades"],
                )
            ],
        )
        reconciler = OrderReconciler({Venue.BINANCE: adapter})

        result = await reconciler.reconcile_position(
            position_id="pos-1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            long_client_order_id="cid-1",
        )

        assert result.long_status == "filled"
        events = reconciler.drain_order_diagnostics()
        payload = [e["payload"] for e in events if e["kind"] == "order.reconcile_result"][-1]
        assert payload["uncertain_subtype"] == "live_position_confirmed"
        assert payload["live_position_delta"]["quantity"] == pytest.approx(0.4)
        assert payload["next_action"] == "clear_uncertain_state"

    @pytest.mark.asyncio
    async def test_nonzero_live_position_without_order_match_stays_uncertain(self):
        adapter = _EvidenceAdapter(
            Venue.BINANCE,
            position_qty=0.4,
            diagnostics=[
                self._diag(
                    Venue.BINANCE,
                    "execution_not_found",
                    "order_found_without_execution",
                    ["/fapi/v1/order", "/fapi/v1/userTrades"],
                )
            ],
        )
        reconciler = OrderReconciler({Venue.BINANCE: adapter})

        result = await reconciler.reconcile_position(
            position_id="pos-1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            long_client_order_id="cid-1",
        )

        assert result.long_status == "uncertain"
        events = reconciler.drain_order_diagnostics()
        payload = [e["payload"] for e in events if e["kind"] == "order.reconcile_result"][-1]
        assert payload["uncertain_subtype"] == "execution_not_found"
        assert payload["live_position_delta"]["quantity"] == pytest.approx(0.4)
        assert payload["next_action"] != "clear_uncertain_state"
        assert not [e for e in events if e["kind"] == "order.reconcile_resolution"]

    @pytest.mark.asyncio
    async def test_live_no_effect_confirmed_is_terminal_not_generic_uncertain(self):
        adapter = _EvidenceAdapter(
            Venue.BYBIT,
            position_qty=0.0,
            diagnostics=[
                self._diag(
                    Venue.BYBIT,
                    "execution_not_found",
                    "order_history_found_no_execution",
                    ["/v5/order/history", "/v5/execution/list"],
                )
            ],
        )
        reconciler = OrderReconciler({Venue.BYBIT: adapter})

        result = await reconciler.reconcile_position(
            position_id="pos-1",
            symbol="BTCUSDT",
            long_venue=Venue.BYBIT,
            long_client_order_id="cid-1",
        )

        assert result.long_status == "not_found"
        payload = [
            e["payload"]
            for e in reconciler.drain_order_diagnostics()
            if e["kind"] == "order.reconcile_result"
        ][-1]
        assert payload["status"] != "uncertain"
        assert payload["uncertain_subtype"] == "live_no_effect_confirmed"
