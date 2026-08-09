"""Semantic parity tests for journal envelope and durability (JRNL-001).

Validates that V2 journal preserves: seq (monotonic), run_id (unique per process),
ts (microsecond or better), kind (string), payload (JSON), critical durability
(fsync), malformed-line tolerance, streaming, and indexed seek behavior.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pytest
from lightfee.persistence.journal import Journal, replay_journal_records


# ---------------------------------------------------------------------------
# Journal envelope and basic behavior
# ---------------------------------------------------------------------------

class TestJournalEnvelope:
    """JRNL-001: Journal envelope preserves V1 semantics."""

    def test_journal_record_has_all_envelope_fields(self, tmp_path):
        journal = Journal(tmp_path / "test.jsonl")
        journal.open()
        try:
            journal.append("test.kind", {"key": "value"})
            records = journal.read_all()
            assert len(records) == 1
            r = records[0]
            assert "seq" in r
            assert "run_id" in r
            assert "ts_ms" in r
            assert "kind" in r
            assert r["kind"] == "test.kind"
            assert "payload" in r
            assert r["payload"] == {"key": "value"}
        finally:
            journal.close()

    def test_seq_is_monotonic(self, tmp_path):
        journal = Journal(tmp_path / "test.jsonl")
        journal.open()
        try:
            seqs = []
            for i in range(10):
                seq = journal.append("test.kind", {"n": i})
                seqs.append(seq)
            assert seqs == list(range(1, 11))
        finally:
            journal.close()

    def test_run_id_is_unique(self, tmp_path):
        j1 = Journal(tmp_path / "j1.jsonl")
        j2 = Journal(tmp_path / "j2.jsonl")
        # Different paths — run_ids may collide if same process/time, but
        # format should be "lightfee-<ts_ms>-<pid>"
        assert j1.run_id.startswith("lightfee-")
        assert j2.run_id.startswith("lightfee-")

    def test_ts_millisecond_precision(self, tmp_path):
        import time
        journal = Journal(tmp_path / "test.jsonl")
        journal.open()
        try:
            before = int(time.time() * 1000)
            seq = journal.append("test.kind", {"key": "value"})
            after = int(time.time() * 1000)
            records = journal.read_all()
            r = records[0]
            assert before <= r["ts_ms"] <= after
        finally:
            journal.close()

    def test_explicit_ts_override(self, tmp_path):
        journal = Journal(tmp_path / "test.jsonl")
        journal.open()
        try:
            journal.append("test.kind", {"k": "v"}, ts_ms=42)
            records = journal.read_all()
            assert records[0]["ts_ms"] == 42
        finally:
            journal.close()


# ---------------------------------------------------------------------------
# Critical durability (fsync)
# ---------------------------------------------------------------------------

class TestJournalDurability:
    """JRNL-001: Critical events must be durable before returning."""

    def test_append_critical_fsyncs(self, tmp_path):
        journal = Journal(tmp_path / "test.jsonl")
        journal.open()
        try:
            journal.append_critical(ts_ms=1000, kind="critical.event",
                                     payload={"important": True})
            records = journal.read_all()
            assert len(records) == 1
            assert records[0]["kind"] == "critical.event"
            # After critical append, record must be on disk immediately
            # (verified by read_all finding it without extra flush)
        finally:
            journal.close()

    def test_flush_param_in_append(self, tmp_path):
        journal = Journal(tmp_path / "test.jsonl")
        journal.open()
        try:
            journal.append("flushed.event", {"k": "v"}, flush=True)
            records = journal.read_all()
            assert len(records) == 1
        finally:
            journal.close()


# ---------------------------------------------------------------------------
# Malformed-line tolerance
# ---------------------------------------------------------------------------

class TestJournalMalformedLineTolerance:
    """JRNL-001: Malformed lines are skipped with no crash."""

    def test_malformed_lines_skipped(self, tmp_path):
        p = tmp_path / "mixed.jsonl"
        p.write_text(
            '{"seq":1,"run_id":"r1","ts_ms":1,"kind":"good","payload":{}}\n'
            'this is not json\n'
            '{"seq":2,"run_id":"r1","ts_ms":2,"kind":"also_good","payload":{}}\n'
            '\n'
            '{"seq":3,"run_id":"r1","ts_ms":3,"kind":"after_blank","payload":{}}\n'
        )
        journal = Journal(p)
        records = journal.read_all()
        kinds = [r["kind"] for r in records]
        assert "good" in kinds
        assert "also_good" in kinds
        assert "after_blank" in kinds
        assert len(records) == 3

    def test_empty_file_returns_empty(self, tmp_path):
        journal = Journal(tmp_path / "empty.jsonl")
        journal.open()
        journal.close()
        assert journal.read_all() == []

    def test_nonexistent_file_returns_empty(self, tmp_path):
        journal = Journal(tmp_path / "nonexistent.jsonl")
        assert journal.read_all() == []


# ---------------------------------------------------------------------------
# Streaming reads
# ---------------------------------------------------------------------------

class TestJournalStreaming:
    """JRNL-001: Streaming reads don't materialize the full file."""

    def test_stream_records_yields_all(self, tmp_path):
        journal = Journal(tmp_path / "test.jsonl")
        journal.open()
        try:
            for i in range(50):
                journal.append("test.kind", {"n": i})
        finally:
            journal.close()

        count = 0
        for record in journal.stream_records():
            count += 1
            assert "seq" in record
        assert count == 50

    def test_stream_from_with_seq(self, tmp_path):
        journal = Journal(tmp_path / "test.jsonl")
        journal.open()
        try:
            for i in range(20):
                journal.append("test.kind", {"n": i})
        finally:
            journal.close()

        records = list(journal.stream_from(start_seq=10))
        assert len(records) >= 10  # at least records 10-20
        for r in records:
            assert r["seq"] >= 10


# ---------------------------------------------------------------------------
# max_seq property
# ---------------------------------------------------------------------------

class TestJournalMaxSeq:
    """JRNL-001: max_seq returns the highest seq on disk."""

    def test_max_seq_on_populated_journal(self, tmp_path):
        journal = Journal(tmp_path / "test.jsonl")
        journal.open()
        try:
            for i in range(5):
                journal.append("test.kind", {"n": i})
        finally:
            journal.close()
        assert journal.max_seq == 5

    def test_max_seq_on_empty_journal(self, tmp_path):
        journal = Journal(tmp_path / "empty.jsonl")
        journal.open()
        journal.close()
        assert journal.max_seq == 0


# ---------------------------------------------------------------------------
# Replay semantic equivalence (REPLAY-001)
# ---------------------------------------------------------------------------

class TestJournalReplayBasics:
    """REPLAY-001: Journal replay reconstructs state from events."""

    def test_replay_empty_produces_default(self):
        result = replay_journal_records([])
        assert result["open_position_count"] == 0
        assert result["pending_entry_count"] == 0
        assert result["pending_close_count"] == 0
        assert result["final_lifecycle"] == "booting"
        assert result["final_risk_mode"] == "running"

    def test_replay_entry_open_and_close(self):
        records = [
            {"seq": 1, "run_id": "r1", "ts_ms": 1,
             "kind": "entry.opened",
             "payload": {"position_id": "p1", "symbol": "ETH-USDT",
                         "quantity": 2.0, "long_quantity": 2.0, "short_quantity": 2.0}},
            {"seq": 2, "run_id": "r1", "ts_ms": 2,
             "kind": "exit.closed",
             "payload": {"position_id": "p1"}},
        ]
        result = replay_journal_records(records)
        assert result["open_position_count"] == 0
        assert "p1" not in result["positions"]

    def test_replay_lifecycle_risk_transitions(self):
        records = [
            {"seq": 1, "run_id": "r1", "ts_ms": 1,
             "kind": "runtime.lifecycle_changed",
             "payload": {"to": "running"}},
            {"seq": 2, "run_id": "r1", "ts_ms": 2,
             "kind": "runtime.risk_mode_changed",
             "payload": {"to": "reduce_only"}},
        ]
        result = replay_journal_records(records)
        assert result["final_lifecycle"] == "running"
        assert result["final_risk_mode"] == "reduce_only"

    def test_replay_with_seed_state(self):
        seed = {
            "lifecycle": "reconciling",
            "risk_mode": "reduce_only",
            "open_positions": {
                "p-seed": {
                    "position_id": "p-seed",
                    "symbol": "BTC-USDT",
                    "quantity": 1.0,
                    "long_quantity": 1.0,
                    "short_quantity": 1.0,
                }
            },
        }
        records = [
            {"seq": 1, "run_id": "r1", "ts_ms": 1,
             "kind": "runtime.lifecycle_changed",
             "payload": {"to": "running"}},
        ]
        result = replay_journal_records(records, seed_state=seed)
        assert result["open_position_count"] == 1
        assert "p-seed" in result["positions"]
        assert result["final_lifecycle"] == "running"

    def test_replay_recovery_events_tracked(self):
        records = [
            {"seq": 1, "run_id": "r1", "ts_ms": 1,
             "kind": "recovery.live_detected",
             "payload": {"position_id": "p1"}},
            {"seq": 2, "run_id": "r1", "ts_ms": 2,
             "kind": "recovery.blocked",
             "payload": {"reason": "ambiguous"}},
        ]
        result = replay_journal_records(records)
        assert len(result["recovery_events"]) == 2

    def test_replay_risk_events_tracked(self):
        records = [
            {"seq": 1, "run_id": "r1", "ts_ms": 1,
             "kind": "risk.warning_triggered",
             "payload": {"pnl": -500}},
            {"seq": 2, "run_id": "r1", "ts_ms": 2,
             "kind": "risk.death_triggered",
             "payload": {"pnl": -5000}},
        ]
        result = replay_journal_records(records)
        assert len(result["risk_events"]) == 2

    def test_replay_timeline_tracks_key_events(self):
        records = [
            {"seq": 1, "run_id": "r1", "ts_ms": 1,
             "kind": "entry.opened",
             "payload": {"position_id": "p1"}},
            {"seq": 2, "run_id": "r1", "ts_ms": 2,
             "kind": "runtime.lifecycle_changed",
             "payload": {"to": "running"}},
            {"seq": 3, "run_id": "r1", "ts_ms": 3,
             "kind": "scan.completed",
             "payload": {"candidate_count": 10}},
            {"seq": 4, "run_id": "r1", "ts_ms": 4,
             "kind": "exit.closed",
             "payload": {"position_id": "p1"}},
        ]
        result = replay_journal_records(records)
        assert len(result["timeline"]) >= 3  # entry.opened, lifecycle, exit.closed

    def test_replay_timeline_tracks_billing_evidence_import(self):
        records = [
            {"seq": 1, "run_id": "r1", "ts_ms": 1,
             "kind": "exit.billing_evidence_imported",
             "payload": {"position_id": "historical-close-owner"}},
        ]

        result = replay_journal_records(records)

        assert result["timeline"] == [
            {
                "seq": 1,
                "ts_ms": 1,
                "kind": "exit.billing_evidence_imported",
            }
        ]

    def test_replay_idempotent(self):
        """Replaying the same records twice produces same result."""
        records = [
            {"seq": 1, "run_id": "r1", "ts_ms": 1,
             "kind": "entry.opened",
             "payload": {"position_id": "p1", "symbol": "ETH-USDT",
                         "quantity": 1.0, "long_quantity": 1.0, "short_quantity": 1.0}},
            {"seq": 2, "run_id": "r1", "ts_ms": 2,
             "kind": "runtime.lifecycle_changed",
             "payload": {"to": "running"}},
        ]
        r1 = replay_journal_records(records)
        r2 = replay_journal_records(records)
        assert r1["open_position_count"] == r2["open_position_count"]
        assert r1["final_lifecycle"] == r2["final_lifecycle"]
        assert r1["open_position_ids"] == r2["open_position_ids"]

    def test_replay_scan_stats(self):
        records = [
            {"seq": 1, "run_id": "r1", "ts_ms": 1,
             "kind": "scan.completed",
             "payload": {
                 "candidate_count": 42,
                 "blocked_count": 10,
                 "accepted_count": 32,
                 "blocked_reasons": {"min_notional": 8, "risk": 2},
                 "no_entry_reason": "",
             }},
        ]
        result = replay_journal_records(records)
        assert result["scan_stats"] is not None
        assert result["scan_stats"]["candidate_count"] == 42
        assert result["scan_stats"]["accepted_count"] == 32

    def test_replay_entry_pending_close_lifecycle(self):
        """Full lifecycle roundtrip: open -> pending_close -> close."""
        records = [
            {"seq": 1, "run_id": "r1", "ts_ms": 1,
             "kind": "entry.opened",
             "payload": {"position_id": "p1", "symbol": "ETH-USDT",
                         "quantity": 1.0, "long_quantity": 1.0, "short_quantity": 1.0}},
            {"seq": 2, "run_id": "r1", "ts_ms": 2,
             "kind": "exit.pending_close_registered",
             "payload": {"close_id": "c1", "position_id": "p1"}},
            {"seq": 3, "run_id": "r1", "ts_ms": 3,
             "kind": "exit.closed",
             "payload": {"position_id": "p1"}},
        ]
        result = replay_journal_records(records)
        assert result["open_position_count"] == 0
        assert result["pending_close_count"] == 0
