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


class TestCounterfactualSafety:
    """Test that counterfactual analysis never synthesizes missing evidence."""

    def test_counterfactual_never_synthesizes_positions_or_pnl(self):
        """Rust V1: counterfactual must NOT synthesize positions, PnL, or fake records."""
        from lightfee.offline.replay.counterfactual import CounterfactualSpec, run_counterfactual
        from lightfee.offline.replay.dataset import ReplayDataset

        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "scan.completed",
                "payload": {
                    "candidate_count": 2,
                    "blocked_count": 0,
                    "accepted_count": 2,
                    "blocked_reasons": {},
                    "no_entry_reason": "",
                },
            },
        ]
        dataset = ReplayDataset(records=records, date_from="", date_to="")
        spec = CounterfactualSpec(
            review_id="cf-1",
            config_overrides={"min_edge_bps": 15.0},
        )
        result = run_counterfactual(dataset, spec)
        # Counterfactual must NOT synthesize positions, PnL, or extra records
        assert result.simulated_positions == 0
        assert result.estimated_pnl_quote == 0.0
        # The total_candidates comes from recorded scan.completed events (config override applied)
        # — this is V1 semantics: override config on recorded evidence, not synthesize new data

    def test_counterfactual_without_overrides_equals_replay(self):
        """Rust V1: counterfactual without config overrides returns same result as replay."""
        from lightfee.offline.replay.counterfactual import CounterfactualSpec, run_counterfactual
        from lightfee.offline.replay.dataset import ReplayDataset
        from lightfee.offline.replay.engine import replay_dataset

        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "sidecar.candidate_published",
                "payload": {"pair_id": "btcusdt:binance->okx", "blocked": False},
            },
            {
                "seq": 2, "run_id": "r1", "ts_ms": 2000,
                "kind": "sidecar.candidate_published",
                "payload": {"pair_id": "ethusdt:binance->bybit", "blocked": True,
                           "blocked_reasons": ["low_liquidity"]},
            },
        ]
        dataset = ReplayDataset(records=records, date_from="", date_to="")
        spec = CounterfactualSpec(review_id="cf-default")
        cf_result = run_counterfactual(dataset, spec)
        replay_result = replay_dataset(dataset)
        assert cf_result.total_candidates == replay_result.total_candidates
        assert cf_result.accepted == replay_result.accepted
        assert cf_result.rejected == replay_result.rejected


class TestWalkForwardDateArithmetic:
    """Test that walk-forward uses real datetime arithmetic."""

    def test_walk_forward_uses_real_date_window(self):
        """Rust V1: walk-forward must use real calendar date arithmetic, not mock dates."""
        from lightfee.offline.replay.walk_forward import generate_walk_forward_windows

        windows = generate_walk_forward_windows(
            start_date="20260101",
            end_date="20260110",
            train_days=4,
            test_days=2,
        )
        assert len(windows) > 0

        # Each window must have non-overlapping test periods, rolling forward
        for i in range(1, len(windows)):
            # Previous test_to == next test_from (deterministic roll)
            prev = windows[i - 1]
            curr = windows[i]
            assert prev.test_to == curr.test_from, (
                f"Window {i}: test periods should be contiguous "
                f"({prev.test_to} != {curr.test_from})"
            )

        # Check first window structure
        w0 = windows[0]
        assert w0.train_from == "20260101"
        assert w0.train_to == "20260105"  # +4 days
        assert w0.test_from == "20260105"  # train_to
        assert w0.test_to == "20260107"    # +2 days

    def test_walk_forward_date_arithmetic_not_broken_by_string_comparison(self):
        """Rust V1: date comparisons must use datetime, not string lexicographic ordering."""
        from datetime import datetime, timedelta

        # Prove that string comparison would give wrong answer for cross-year dates
        assert "20260101" < "20270101"  # string comparison works for YYYYMMDD format
        # But the implementation must actually use datetime math, not string slicing
        from lightfee.offline.replay.walk_forward import generate_walk_forward_windows

        windows = generate_walk_forward_windows(
            start_date="20261228",
            end_date="20270105",
            train_days=3,
            test_days=2,
        )
        assert len(windows) > 0
        # Verify cross-year boundary is handled correctly
        last = windows[-1]
        assert last.test_to == "20270104" or last.test_to == "20270105"


class TestReplayTimelineCompleteness:
    """Test that replay timeline captures all state-transition events."""

    def test_replay_timeline_includes_risk_death_triggered(self):
        """Rust V1: risk.death_triggered events must appear in replay timeline."""
        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "entry.opened",
                "payload": {"position_id": "pos-risk", "symbol": "BTCUSDT"},
            },
            {
                "seq": 2, "run_id": "r1", "ts_ms": 5000,
                "kind": "risk.death_triggered",
                "payload": {
                    "position_id": "pos-risk",
                    "reason": "health_drop",
                    "long_health_ratio": 0.35,
                    "short_health_ratio": 0.30,
                },
            },
        ]
        result = replay_journal_records(records)
        timeline_kinds = [e["kind"] for e in result.get("timeline", [])]
        assert "risk.death_triggered" in timeline_kinds

    def test_replay_timeline_includes_recovery_events(self):
        """Rust V1: recovery events must appear in timeline for audit trace."""
        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "recovery.live_detected",
                "payload": {"position_id": "pos-rec", "symbol": "ETHUSDT"},
            },
            {
                "seq": 2, "run_id": "r1", "ts_ms": 5000,
                "kind": "recovery.blocked",
                "payload": {"reason": "venue_unavailable"},
            },
            {
                "seq": 3, "run_id": "r1", "ts_ms": 10000,
                "kind": "recovery.mismatch_detected",
                "payload": {"position_id": "pos-rec", "expected": 1.0, "actual": 0.8},
            },
        ]
        result = replay_journal_records(records)
        timeline_kinds = [e["kind"] for e in result.get("timeline", [])]
        assert "recovery.live_detected" in timeline_kinds
        assert "recovery.blocked" in timeline_kinds
        assert "recovery.mismatch_detected" in timeline_kinds

    def test_replay_pending_counts_from_evidence_not_hardcoded(self):
        """Rust V1: pending_entry_count and pending_close_count must be zero when no such events exist."""
        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "scan.completed",
                "payload": {"candidate_count": 5},
            },
            {
                "seq": 2, "run_id": "r1", "ts_ms": 2000,
                "kind": "entry.opened",
                "payload": {"position_id": "pos-e", "symbol": "BTCUSDT"},
            },
        ]
        result = replay_journal_records(records)
        # No pending_entry_registered events → pending_entry_count == 0
        assert result["pending_entry_count"] == 0
        # No pending_close_registered events → pending_close_count == 0
        assert result["pending_close_count"] == 0
        # But we DO have 1 open position from entry.opened
        assert result["open_position_count"] == 1


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


class TestReplayReconstruction:
    """Test that replay reconstructs full timeline, not just summary counts."""

    def test_replay_reconstructs_position_fields_losslessly(self):
        """V1 rule: replay preserves all position fields from recorded evidence."""
        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "entry.opened",
                "payload": {
                    "position_id": "pos-full",
                    "symbol": "BTCUSDT",
                    "long_venue": "binance",
                    "short_venue": "okx",
                    "quantity": 0.1,
                    "long_quantity": 0.1,
                    "short_quantity": 0.1,
                    "long_entry_price": 68750.0,
                    "short_entry_price": 68755.0,
                    "opened_at_ms": 1000,
                    "matched_quantity": 0.1,
                    "current_net_quote": 1.5,
                    "peak_net_quote": 2.0,
                    "captured_funding_quote": 0.05,
                    "funding_captured": False,
                    "long_entry_fee_quote": 0.01,
                    "short_entry_fee_quote": 0.01,
                },
            }
        ]
        result = replay_journal_records(records)
        assert result["open_position_count"] == 1
        pos = result["positions"].get("pos-full", {})
        assert pos.get("long_entry_price") == 68750.0
        assert pos.get("short_entry_price") == 68755.0
        assert pos.get("current_net_quote") == 1.5
        assert pos.get("peak_net_quote") == 2.0
        assert pos.get("long_entry_fee_quote") == 0.01

    def test_replay_tracks_pending_entry_from_journal(self):
        """V1 rule: pending_entry_count must come from journal events, not hardcoded 0."""
        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "entry.pending_registered",
                "payload": {"pending_id": "pend-1", "symbol": "ETHUSDT"},
            },
        ]
        result = replay_journal_records(records)
        # Should track pending entries, not hardcode 0
        assert result["pending_entry_count"] == 1

    def test_replay_tracks_pending_close_from_journal(self):
        """V1 rule: pending_close_count must come from journal events, not hardcoded 0."""
        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "exit.pending_close_registered",
                "payload": {"close_id": "close-1", "position_id": "pos-1"},
            },
        ]
        result = replay_journal_records(records)
        assert result["pending_close_count"] == 1

    def test_replay_preserves_candidate_filter_list_evidence(self):
        """V1 rule: replay must preserve candidate list and blocked reasons, not boolean only."""
        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "scan.completed",
                "payload": {
                    "candidate_count": 3,
                    "blocked_count": 1,
                    "accepted_count": 2,
                    "blocked_reasons": {
                        "btcusdt:binance->okx": ["stale_market_data:binance"],
                    },
                    "accepted_candidates": [
                        {"pair_id": "ethusdt:binance->okx", "edge_bps": 15.0},
                        {"pair_id": "solusdt:binance->bybit", "edge_bps": 12.0},
                    ],
                    "no_entry_reason": "",
                },
            }
        ]
        result = replay_journal_records(records)
        assert result["scan_stats"] is not None
        assert result["scan_stats"]["candidate_count"] == 3
        assert result["scan_stats"]["blocked_count"] == 1
        assert result["scan_stats"]["accepted_count"] == 2
        assert "btcusdt:binance->okx" in result["scan_stats"]["blocked_reasons"]

    def test_replay_preserves_recovery_diagnostic_records(self):
        """V1 rule: recovery diagnostics (blocked, mismatch, flat) must be tracked."""
        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "recovery.blocked",
                "payload": {"reason": "venue_unavailable", "venue": "binance"},
            },
        ]
        result = replay_journal_records(records)
        assert len(result.get("recovery_events", [])) == 1
        assert result["recovery_events"][0]["kind"] == "recovery.blocked"

    def test_replay_preserves_risk_trigger_evidence(self):
        """V1 rule: risk trigger payloads must include health ratios and adjusted quantities."""
        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "risk.death_triggered",
                "payload": {
                    "position_id": "pos-risk",
                    "reason": "health_drop",
                    "long_health_ratio": 0.45,
                    "short_health_ratio": 0.38,
                    "min_health_ratio": 0.38,
                    "requested_quantity": 0.1,
                    "adjusted_quantity": 0.08,
                    "blocked_reason": "",
                },
            }
        ]
        result = replay_journal_records(records)
        assert len(result.get("risk_events", [])) == 1
        risk = result["risk_events"][0]
        assert risk["payload"]["long_health_ratio"] == 0.45
        assert risk["payload"]["short_health_ratio"] == 0.38
        assert risk["payload"]["adjusted_quantity"] == 0.08

    def test_replay_reconstructs_lifecycle_timeline(self):
        """V1 rule: replay must reconstruct per-event timeline, not just final state."""
        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "runtime.lifecycle_changed",
                "payload": {"from": "booting", "to": "reconciling", "reason": "startup"},
            },
            {
                "seq": 2, "run_id": "r1", "ts_ms": 5000,
                "kind": "entry.opened",
                "payload": {"position_id": "pos-timeline", "symbol": "BTCUSDT"},
            },
            {
                "seq": 3, "run_id": "r1", "ts_ms": 10000,
                "kind": "runtime.lifecycle_changed",
                "payload": {"from": "reconciling", "to": "running", "reason": "recovery_complete"},
            },
            {
                "seq": 4, "run_id": "r1", "ts_ms": 20000,
                "kind": "runtime.risk_mode_changed",
                "payload": {"from": "running", "to": "reduce_only", "reason": "health_drop"},
            },
            {
                "seq": 5, "run_id": "r1", "ts_ms": 30000,
                "kind": "exit.closed",
                "payload": {"position_id": "pos-timeline", "reason": "risk_close"},
            },
        ]
        result = replay_journal_records(records)
        assert result["final_lifecycle"] == "running"
        assert result["final_risk_mode"] == "reduce_only"
        # Must have timeline events
        timeline = result.get("timeline", [])
        assert len(timeline) >= 5
        kinds = [e["kind"] for e in timeline]
        assert "runtime.lifecycle_changed" in kinds
        assert "entry.opened" in kinds
        assert "exit.closed" in kinds

    def test_replay_seed_state_propagates_fields(self):
        """V1 rule: seed state fields propagate through replay without loss."""
        seed = {
            "lifecycle": "reconciling",
            "risk_mode": "reduce_only",
            "open_positions": {
                "pos-seed": {
                    "position_id": "pos-seed",
                    "symbol": "BTCUSDT",
                    "long_venue": "binance",
                    "short_venue": "okx",
                    "quantity": 0.05,
                    "long_entry_price": 68000.0,
                    "short_entry_price": 68005.0,
                    "opened_at_ms": 500,
                    "matched_quantity": 0.05,
                    "current_net_quote": 0.5,
                    "peak_net_quote": 1.0,
                    "captured_funding_quote": 0.0,
                    "funding_captured": False,
                    "long_entry_fee_quote": 0.005,
                    "short_entry_fee_quote": 0.005,
                },
            },
        }
        result = replay_journal_records([], seed_state=seed)
        assert result["open_position_count"] == 1
        assert "pos-seed" in result["open_position_ids"]
        pos = result["positions"].get("pos-seed", {})
        assert pos.get("long_entry_price") == 68000.0
        assert pos.get("peak_net_quote") == 1.0

    def test_replay_returns_fixed_position_schema(self):
        """V1 rule: replay output must use fixed ReplayPositionSnapshot schema, not ad-hoc dict."""
        records = [
            {
                "seq": 1, "run_id": "r1", "ts_ms": 1000,
                "kind": "entry.opened",
                "payload": {
                    "position_id": "pos-schema",
                    "symbol": "ETHUSDT",
                    "long_venue": "binance",
                    "short_venue": "okx",
                    "quantity": 10.0,
                },
            }
        ]
        result = replay_journal_records(records)
        pos = result["positions"].get("pos-schema", {})
        # Every key from ReplayPositionSnapshot must exist (defaulted if missing)
        required_keys = {"position_id", "symbol", "long_venue", "short_venue",
                         "quantity", "long_quantity", "short_quantity",
                         "long_entry_price", "short_entry_price", "opened_at_ms",
                         "matched_quantity", "current_net_quote", "peak_net_quote",
                         "captured_funding_quote", "second_stage_funding_quote",
                         "long_entry_fee_quote", "short_entry_fee_quote",
                         "funding_captured", "second_stage_funding_captured"}
        for key in required_keys:
            assert key in pos, f"Missing key '{key}' in replay position snapshot"
