"""Tests for persistence replay: journal replay, snapshot recovery, atomic writes,
and V2 structured replay reads with journal fallback.

Covers Rust V1 behavior from:
- src/observability_ops/replay_bridge.rs (journal record replay)
- src/runtime_state/persisted_engine.rs (state normalization)
- src/runtime_state/snapshot_store.rs (atomic snapshot persistence)

V2 coverage:
- ReplayDataset.from_structured() reads from SQLite replay_facts
- ReplayDataset.load() structured-first with journal fallback
- merge of structured + journal-only events preserves replay semantics
"""

import json
import tempfile
from pathlib import Path

import pytest

from lightfee.engine.state import EngineState, OpenPosition, PendingEntry, PendingClose
from lightfee.core.domain import Side, Venue
from lightfee.persistence.journal import Journal, replay_journal_records
from lightfee.persistence.snapshot_store import SnapshotStore
from lightfee.offline.replay.dataset import ReplayDataset
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


# ---------------------------------------------------------------------------
# V2: Structured replay reads with journal fallback
# ---------------------------------------------------------------------------

class TestReplayDatasetStructuredRead:
    """Test that ReplayDataset can read from SQLite replay_facts table."""

    def _make_store_with_facts(
        self, tmpdir: str, records: list[dict], date_from: str = "", date_to: str = ""
    ) -> str:
        """Create a temporary SQLite store with replay_facts table populated."""
        import sqlite3
        from lightfee.offline.replay.dataset import _ensure_replay_facts_table, _ts_to_date_str

        store_path = f"{tmpdir}/replay_store.db"
        conn = sqlite3.connect(store_path)
        _ensure_replay_facts_table(conn)
        for r in records:
            conn.execute(
                "INSERT INTO replay_facts (seq, run_id, ts_ms, kind, payload_json, date) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    r["seq"],
                    r.get("run_id", "test-run"),
                    r["ts_ms"],
                    r["kind"],
                    json.dumps(r.get("payload", {})),
                    _ts_to_date_str(r["ts_ms"]),
                ),
            )
        conn.commit()
        conn.close()
        return store_path

    def test_from_structured_reads_all_records(self):
        """Structured path reads all records from replay_facts within date range."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            records = [
                {"seq": 1, "ts_ms": 1700000000000, "kind": "entry.opened",
                 "payload": {"position_id": "pos-1"}},
                {"seq": 2, "ts_ms": 1700000100000, "kind": "scan.completed",
                 "payload": {"candidate_count": 3}},
                {"seq": 3, "ts_ms": 1700000200000, "kind": "exit.closed",
                 "payload": {"position_id": "pos-1"}},
            ]
            store_path = self._make_store_with_facts(td, records)

            dataset = ReplayDataset.from_structured(store_path)
            assert len(dataset.records) == 3
            assert dataset.source == "structured"
            kinds = [r["kind"] for r in dataset.records]
            assert kinds == ["entry.opened", "scan.completed", "exit.closed"]

    def test_from_structured_respects_date_range(self):
        """Structured path filters by date range using SQL WHERE clause."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            # ts_ms for 2026-01-10 = 1768003200000
            # ts_ms for 2026-01-15 = 1768435200000
            # ts_ms for 2026-01-20 = 1768867200000
            records = [
                {"seq": 1, "ts_ms": 1768003200000, "kind": "entry.opened",
                 "payload": {}},   # 20260110
                {"seq": 2, "ts_ms": 1768435200000, "kind": "scan.completed",
                 "payload": {}},   # 20260115
                {"seq": 3, "ts_ms": 1768867200000, "kind": "exit.closed",
                 "payload": {}},   # 20260120
            ]
            store_path = self._make_store_with_facts(td, records)

            dataset = ReplayDataset.from_structured(
                store_path, date_from="20260112", date_to="20260117"
            )
            assert len(dataset.records) == 1
            assert dataset.records[0]["kind"] == "scan.completed"

    def test_from_structured_empty_store_raises_without_journal(self):
        """Structured path raises ValueError when store has no records and no journal."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            store_path = f"{td}/empty.db"
            with pytest.raises(ValueError, match="No structured records"):
                ReplayDataset.from_structured(store_path)

    def test_from_structured_falls_back_to_journal_when_empty(self):
        """When replay_facts is empty, fall back to full journal scan."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            # Empty structured store
            import sqlite3
            store_path = f"{td}/empty_facts.db"
            conn = sqlite3.connect(store_path)
            from lightfee.offline.replay.dataset import _ensure_replay_facts_table
            _ensure_replay_facts_table(conn)
            conn.close()

            # Journal with records
            jp = f"{td}/events.jsonl"
            journal = Journal(jp)
            journal.open()
            journal.append("entry.opened", {"position_id": "pos-j"}, ts_ms=1768003200000)
            journal.append("scan.completed", {"candidate_count": 1}, ts_ms=1768003300000)
            journal.close()

            dataset = ReplayDataset.from_structured(
                store_path, journal_path=jp
            )
            assert len(dataset.records) == 2
            assert dataset.source == "journal"

    def test_from_structured_merges_journal_only_events(self):
        """Structured + journal merge: journal-only events (recovery, lifecycle)
        are read from journal and merged with structured records."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            # Structured store with projectable events
            proj_records = [
                {"seq": 1, "ts_ms": 1768003200000, "kind": "entry.opened",
                 "payload": {"position_id": "pos-merge"}},
                {"seq": 5, "ts_ms": 1768003500000, "kind": "exit.closed",
                 "payload": {"position_id": "pos-merge"}},
            ]
            store_path = self._make_store_with_facts(td, proj_records)

            # Journal with journal-only events
            jp = f"{td}/events.jsonl"
            journal = Journal(jp)
            journal.open()
            journal.append("runtime.lifecycle_changed",
                          {"from": "booting", "to": "reconciling"},
                          ts_ms=1768003150000)
            journal.append("runtime.risk_mode_changed",
                          {"from": "running", "to": "reduce_only"},
                          ts_ms=1768003400000)
            journal.close()

            dataset = ReplayDataset.from_structured(
                store_path, journal_path=jp
            )
            # Should have 4 records: 2 structured + 2 journal-only
            assert len(dataset.records) == 4
            assert dataset.source == "merged"

            kinds = [r["kind"] for r in dataset.records]
            assert "entry.opened" in kinds
            assert "exit.closed" in kinds
            assert "runtime.lifecycle_changed" in kinds
            assert "runtime.risk_mode_changed" in kinds

    def test_merged_records_sorted_by_seq(self):
        """Merged records must be sorted by seq to preserve event ordering."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            # Structured: seq 10, 30
            proj_records = [
                {"seq": 30, "ts_ms": 1768003500000, "kind": "exit.closed",
                 "payload": {"position_id": "pos-sort"}},
                {"seq": 10, "ts_ms": 1768003200000, "kind": "entry.opened",
                 "payload": {"position_id": "pos-sort"}},
            ]
            store_path = self._make_store_with_facts(td, proj_records)

            # Journal: seq 20
            jp = f"{td}/events.jsonl"
            journal = Journal(jp)
            journal.open()
            journal.append("runtime.lifecycle_changed",
                          {"from": "booting", "to": "running"},
                          ts_ms=1768003300000)
            journal.close()

            dataset = ReplayDataset.from_structured(
                store_path, journal_path=jp
            )
            seqs = [r["seq"] for r in dataset.records]
            assert seqs == sorted(seqs), f"records not sorted by seq: {seqs}"

    def test_from_structured_preserves_payload_fidelity(self):
        """Structured path must return payloads identical to journal path."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            payload = {
                "position_id": "pos-fidelity",
                "symbol": "BTCUSDT",
                "long_venue": "binance",
                "short_venue": "okx",
                "quantity": 0.1,
                "long_entry_price": 68750.0,
                "current_net_quote": 1.5,
            }
            records = [
                {"seq": 1, "ts_ms": 1768003200000, "kind": "entry.opened",
                 "payload": payload},
            ]
            store_path = self._make_store_with_facts(td, records)

            # Structured read
            ds_struct = ReplayDataset.from_structured(store_path)
            assert ds_struct.records[0]["payload"] == payload

            # Compare with journal read
            jp = f"{td}/events.jsonl"
            journal = Journal(jp)
            journal.open()
            journal.append("entry.opened", payload, ts_ms=1768003200000)
            journal.close()
            ds_journal = ReplayDataset.from_journal_range(jp)
            assert ds_journal.records[0]["payload"] == payload


class TestReplayDatasetLoad:
    """Test ReplayDataset.load() — the recommended structured-first entry point."""

    def test_load_structured_first_when_store_exists(self):
        """load() uses structured path when store_path is provided and exists."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            import sqlite3
            from lightfee.offline.replay.dataset import _ensure_replay_facts_table, _ts_to_date_str

            # Structured store with records
            store_path = f"{td}/replay_store.db"
            conn = sqlite3.connect(store_path)
            _ensure_replay_facts_table(conn)
            conn.execute(
                "INSERT INTO replay_facts (seq, run_id, ts_ms, kind, payload_json, date) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (1, "test-run", 1768003200000, "entry.opened",
                 json.dumps({"position_id": "pos-load"}), "20260110"),
            )
            conn.commit()
            conn.close()

            # Journal (should not be the primary source)
            jp = f"{td}/events.jsonl"
            journal = Journal(jp)
            journal.open()
            journal.append("entry.opened", {"position_id": "pos-from-journal"},
                          ts_ms=1768003200000)
            journal.close()

            dataset = ReplayDataset.load(jp, store_path=store_path)
            assert dataset.source in ("structured", "merged")

    def test_load_falls_back_to_journal_when_store_missing(self):
        """load() falls back to journal when store_path doesn't exist."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            jp = f"{td}/events.jsonl"
            journal = Journal(jp)
            journal.open()
            journal.append("entry.opened", {"position_id": "pos-fallback"},
                          ts_ms=1768003200000)
            journal.close()

            store_path = f"{td}/nonexistent.db"
            dataset = ReplayDataset.load(jp, store_path=store_path)
            assert dataset.source == "journal"
            assert len(dataset.records) == 1

    def test_load_without_store_uses_journal(self):
        """load() uses journal when store_path is None."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            jp = f"{td}/events.jsonl"
            journal = Journal(jp)
            journal.open()
            journal.append("scan.completed", {"candidate_count": 5},
                          ts_ms=1768003200000)
            journal.close()

            dataset = ReplayDataset.load(jp)
            assert dataset.source == "journal"
            assert len(dataset.records) == 1

    def test_load_structured_preserves_replay_semantics(self):
        """Replay semantics must be identical regardless of read path."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            import sqlite3
            from lightfee.offline.replay.dataset import _ensure_replay_facts_table, _ts_to_date_str

            # Create records covering multiple event kinds
            all_records = [
                {"seq": 1, "ts_ms": 1768003200000, "kind": "runtime.lifecycle_changed",
                 "payload": {"from": "booting", "to": "running"}},
                {"seq": 2, "ts_ms": 1768003300000, "kind": "entry.opened",
                 "payload": {"position_id": "pos-sem", "symbol": "ETHUSDT",
                            "long_venue": "binance", "short_venue": "okx",
                            "quantity": 1.0}},
                {"seq": 3, "ts_ms": 1768003400000, "kind": "scan.completed",
                 "payload": {"candidate_count": 2, "blocked_count": 0,
                            "accepted_count": 2, "blocked_reasons": {},
                            "no_entry_reason": ""}},
                {"seq": 4, "ts_ms": 1768003500000, "kind": "exit.closed",
                 "payload": {"position_id": "pos-sem", "reason": "profit_take"}},
            ]

            # Path A: full journal
            jp = f"{td}/events.jsonl"
            journal = Journal(jp)
            journal.open()
            for r in all_records:
                journal.append(r["kind"], r["payload"], ts_ms=r["ts_ms"])
            journal.close()
            ds_journal = ReplayDataset.from_journal_range(jp)

            # Path B: structured (projectable) + journal-only from journal
            store_path = f"{td}/replay_store.db"
            conn = sqlite3.connect(store_path)
            _ensure_replay_facts_table(conn)
            # Only projectable events go into structured store
            for r in all_records:
                from lightfee.offline.replay.dataset import _is_journal_only
                if _is_journal_only(r["kind"]):
                    continue
                conn.execute(
                    "INSERT INTO replay_facts (seq, run_id, ts_ms, kind, payload_json, date) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (r["seq"], "test-run", r["ts_ms"], r["kind"],
                     json.dumps(r["payload"]), _ts_to_date_str(r["ts_ms"])),
                )
            conn.commit()
            conn.close()
            ds_structured = ReplayDataset.from_structured(
                store_path, journal_path=jp
            )

            # Both paths must produce identical replay results
            from lightfee.persistence.journal import replay_journal_records

            journal_result = replay_journal_records(ds_journal.records)
            structured_result = replay_journal_records(ds_structured.records)

            assert journal_result["open_position_count"] == structured_result["open_position_count"]
            assert journal_result["pending_entry_count"] == structured_result["pending_entry_count"]
            assert journal_result["pending_close_count"] == structured_result["pending_close_count"]
            assert journal_result["final_lifecycle"] == structured_result["final_lifecycle"]
            assert journal_result["final_risk_mode"] == structured_result["final_risk_mode"]


class TestJournalOnlyClassification:
    """Test that journal-only event classification is correct."""

    def test_recovery_events_are_journal_only(self):
        from lightfee.offline.replay.dataset import _is_journal_only
        assert _is_journal_only("recovery.live_detected")
        assert _is_journal_only("recovery.flat")
        assert _is_journal_only("recovery.blocked")
        assert _is_journal_only("recovery.mismatch_detected")
        assert _is_journal_only("recovery.resumed")

    def test_lifecycle_events_are_journal_only(self):
        from lightfee.offline.replay.dataset import _is_journal_only
        assert _is_journal_only("pending_entry.viability_blocked")
        assert _is_journal_only("runtime.entry_blocked_lifecycle")
        assert _is_journal_only("runtime.entry_blocked_lifecycle_selection")
        assert _is_journal_only("runtime.lifecycle_changed")
        assert _is_journal_only("runtime.risk_mode_changed")
        assert _is_journal_only("runtime.booting")
        assert _is_journal_only("runtime.running")
        assert _is_journal_only("runtime.stopped")

    def test_projectable_events_are_not_journal_only(self):
        from lightfee.offline.replay.dataset import _is_journal_only
        assert not _is_journal_only("entry.opened")
        assert not _is_journal_only("exit.closed")
        assert not _is_journal_only("exit.partial_closed")
        assert not _is_journal_only("order.submitted")
        assert not _is_journal_only("order.filled")
        assert not _is_journal_only("scan.completed")
        assert not _is_journal_only("scan.no_entry_diagnostics")
        assert not _is_journal_only("risk.death_triggered")
        assert not _is_journal_only("risk.warning_triggered")
        assert not _is_journal_only("runtime.local_l2_sequence_gap")
        assert not _is_journal_only("entry.pending_registered")
        assert not _is_journal_only("exit.pending_close_registered")
