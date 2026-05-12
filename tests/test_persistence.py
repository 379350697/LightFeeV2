"""Tests for persistence: journal, snapshot, SQLite."""

import json
import tempfile
from pathlib import Path

import pytest

from lightfee.persistence.journal import Journal
from lightfee.persistence.journal_index import JournalIndex
from lightfee.persistence.metrics import PersistenceMetrics
from lightfee.persistence.projection_backfill import ProjectionBackfill
from lightfee.persistence.snapshot_store import SnapshotStore
from lightfee.persistence.projection_contracts import (
    ALL_JOURNAL_ONLY_KINDS,
    ALL_PROJECTED_KINDS,
    PROJECTED_ENTRY_EXIT_KINDS,
    PROJECTED_EXECUTION_KINDS,
    PROJECTED_L2_HEALTH_KINDS,
    PROJECTED_ORDER_KINDS,
    PROJECTED_RISK_KINDS,
    PROJECTED_SCAN_KINDS,
    classify_kind,
    fact_table_for_kind,
    is_journal_only_kind,
    is_projected_kind,
)
from lightfee.persistence.sqlite_store import SqliteStore


class TestJournal:
    def test_appends_jsonl_records(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "test.jsonl"
            j = Journal(path)
            j.open()
            seq = j.append("test.event", {"value": 42}, flush=True)
            assert seq == 1
            j.close()

            records = j.read_all()
            assert len(records) == 1
            assert records[0]["kind"] == "test.event"
            assert records[0]["seq"] == 1
            assert records[0]["payload"]["value"] == 42

    def test_has_run_id(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "test.jsonl"
            j = Journal(path)
            assert j.run_id
            assert len(j.run_id) > 0

    def test_run_id_matches_v1_shape(self):
        """Rust V1: run_id = lightfee-{timestamp_ms}-{pid}."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "test.jsonl"
            j = Journal(path)
            parts = j.run_id.split("-")
            # lightfee-{ts_ms}-{pid}
            assert len(parts) >= 3, f"run_id '{j.run_id}' should be lightfee-{{ts_ms}}-{{pid}}"
            assert parts[0] == "lightfee"
            assert parts[1].isdigit(), f"timestamp part of run_id should be digits, got '{parts[1]}'"
            assert parts[2].isdigit(), f"pid part of run_id should be digits, got '{parts[2]}'"

    def test_seq_starts_at_1(self):
        """Rust V1: next_seq starts at 1 (AtomicU64::new(1))."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "test.jsonl"
            j = Journal(path)
            j.open()
            seq = j.append("test", {"x": 1}, flush=True)
            assert seq == 1
            j.close()
            records = j.read_all()
            assert records[0]["seq"] == 1

    def test_scan_records_matching_kinds(self):
        """Rust V1: scan_records_matching_kinds filters by kind during read."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "scan_filter.jsonl"
            j = Journal(path)
            j.open()
            j.append("scan.completed", {"cycle": 1}, flush=True)
            j.append("entry.opened", {"position_id": "pos-1"}, flush=True)
            j.append("scan.completed", {"cycle": 2}, flush=True)
            j.append("exit.closed", {"position_id": "pos-1"}, flush=True)
            j.close()

            # Only scan.completed records
            records = j.scan_records_matching_kinds(["scan.completed"])
            kinds = [r["kind"] for r in records]
            assert kinds == ["scan.completed", "scan.completed"]

            # Multiple kinds
            records = j.scan_records_matching_kinds(["entry.opened", "exit.closed"])
            kinds = [r["kind"] for r in records]
            assert "entry.opened" in kinds
            assert "exit.closed" in kinds
            assert "scan.completed" not in kinds

            # No match
            records = j.scan_records_matching_kinds(["nonexistent"])
            assert records == []

    def test_read_all_handles_missing_file(self):
        j = Journal("/tmp/nonexistent/test_journal.jsonl")
        assert j.read_all() == []

    def test_raises_when_not_open(self):
        with tempfile.TemporaryDirectory() as td:
            j = Journal(Path(td) / "test.jsonl")
            with pytest.raises(RuntimeError, match="not open"):
                j.append("test", {})


class TestSnapshotStore:
    def test_atomic_write_and_read(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "state.json"
            store = SnapshotStore(path)
            store.write({"lifecycle": "running", "tick_count": 42})
            assert store.exists()

            data = store.read()
            assert data is not None
            assert data["lifecycle"] == "running"
            assert data["tick_count"] == 42

    def test_read_missing_returns_none(self):
        store = SnapshotStore("/tmp/nonexistent/snap.json")
        assert store.read() is None
        assert not store.exists()


class TestSqliteStore:
    def test_creates_tables_on_open(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "test.sqlite"
            store = SqliteStore(path)
            conn = store.open()

            # Verify tables exist
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
            table_names = {t[0] for t in tables}
            assert "daily_snapshots" in table_names
            assert "scan_facts" in table_names
            assert "proposal_catalog" in table_names
            assert "approval_queue" in table_names
            assert "experiment_ledger" in table_names
            assert "operator_commands" in table_names
            # V2 projection tables
            assert "projected_facts" in table_names
            assert "order_facts" in table_names
            assert "entry_exit_facts" in table_names
            assert "risk_counter_facts" in table_names
            assert "local_l2_health_facts" in table_names
            assert "diagnostic_facts" in table_names
            assert "projection_cursor" in table_names
            conn.close()

    def test_insert_daily_snapshot(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "test.sqlite"
            store = SqliteStore(path)
            conn = store.open()
            store.insert_daily_snapshot(conn, "2026-05-10", "binance", "BTCUSDT", 100.0, 5.0, 3, 3, 1000)
            rows = conn.execute("SELECT * FROM daily_snapshots").fetchall()
            assert len(rows) == 1
            conn.close()

    def test_insert_operator_command(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "test.sqlite"
            store = SqliteStore(path)
            conn = store.open()
            store.insert_operator_command(conn, 1000, "pause-entry")
            rows = conn.execute("SELECT command FROM operator_commands").fetchall()
            assert rows[0][0] == "pause-entry"
            conn.close()


class TestMetrics:
    def test_tracks_appends_and_writes(self):
        m = PersistenceMetrics()
        m.record_journal_append(1000)
        m.record_journal_append(2000)
        m.record_snapshot_write(3000)
        assert m.journal_appends == 2
        assert m.snapshot_writes == 1
        assert m.last_journal_append_ms == 2000
        assert m.last_snapshot_write_ms == 3000

    def test_tracks_async_and_critical_appends(self):
        """Rust V1: JournalRuntimeMetrics tracks async/critical/sync_fallback/dropped."""
        m = PersistenceMetrics()
        m.record_journal_append(1000, critical=False)
        m.record_journal_append(2000, critical=True)
        m.record_journal_append(3000, critical=False)
        assert m.journal_appends == 3
        assert m.critical_appends == 1
        assert m.async_appends == 2

    def test_tracks_writer_and_flush_counters(self):
        """Rust V1: writer_flushes, writer_failures, flush_requests counters."""
        m = PersistenceMetrics()
        m.record_journal_flush()
        m.record_journal_flush()
        m.record_writer_failure()
        assert m.journal_flushes == 2
        assert m.writer_failures == 1

    def test_tracks_health_and_event_counters(self):
        """Rust V1: risk/health/event counters in persistence metrics."""
        m = PersistenceMetrics()
        m.record_risk_warning_trigger()
        m.record_risk_death_trigger()
        m.record_order_timeout()
        m.record_ws_disconnect()
        m.record_rest_failure()
        m.record_reconcile_drift()
        assert m.risk_warning_trigger_count == 1
        assert m.risk_death_trigger_count == 1
        assert m.order_timeout_count == 1
        assert m.ws_disconnect_count == 1
        assert m.rest_failure_count == 1
        assert m.reconcile_drift_count == 1

    def test_runtime_health_metrics_snapshot(self):
        """Rust V1: set_runtime_health_metrics updates snapshot counters."""
        m = PersistenceMetrics()
        m.set_runtime_health(
            open_position_count=2,
            global_risk_mode="reduce_only",
            net_exposure_milli_quote=1250,
            venue_health_normal_count=1,
            venue_health_pause_entry_count=2,
            venue_health_reduce_only_count=3,
            venue_health_fail_closed_count=4,
        )
        assert m.open_position_count == 2
        assert m.global_risk_mode == "reduce_only"
        assert m.net_exposure_milli_quote == 1250
        assert m.venue_health_normal_count == 1
        assert m.venue_health_pause_entry_count == 2
        assert m.venue_health_reduce_only_count == 3
        assert m.venue_health_fail_closed_count == 4

    def test_flush_requests_property_matches_v1(self):
        """Rust V1: JournalRuntimeMetricsSnapshot exposes flush_requests."""
        m = PersistenceMetrics()
        m.record_journal_flush()
        m.record_journal_flush()
        m.record_journal_flush()
        assert m.flush_requests == 3
        # flush_requests and journal_flushes must be semantically equivalent
        assert m.flush_requests == m.journal_flushes

    def test_all_v1_runtime_counters_are_present(self):
        """Rust V1: all 19 JournalRuntimeMetrics counters must exist."""
        m = PersistenceMetrics()
        v1_counter_fields = {
            "async_appends", "critical_appends", "sync_fallback_appends",
            "dropped_async_appends", "flush_requests", "writer_flushes",
            "writer_failures", "queue_disconnects",
            "open_position_count", "net_exposure_milli_quote",
            "venue_health_normal_count", "venue_health_pause_entry_count",
            "venue_health_reduce_only_count", "venue_health_fail_closed_count",
            "risk_warning_trigger_count", "risk_delever_trigger_count",
            "risk_death_trigger_count", "order_timeout_count",
            "ws_disconnect_count", "rest_failure_count",
            "reconcile_drift_count",
        }
        for field in v1_counter_fields:
            assert hasattr(m, field), f"Missing V1 counter: {field}"


class TestJournalPayloadPreservation:
    """Test that Journal preserves all payload fields without loss."""

    def test_preserves_nested_payload_fields(self):
        """V1 rule: payload must preserve arbitrary nested keys without dropping unknown keys."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "nest.jsonl"
            j = Journal(path)
            j.open()
            payload = {
                "nested": {"pair_id": "btcusdt:binance->okx", "depth": {"a": 1, "b": 2}},
                "blocked_reasons": ["stale_market_data:binance", "low_liquidity:okx"],
                "candidates": [
                    {"symbol": "BTCUSDT", "score": 0.95},
                    {"symbol": "ETHUSDT", "score": 0.82},
                ],
                "metadata": {"source": "live", "version": 2},
            }
            j.append("scan.completed", payload, flush=True)
            j.close()

            records = j.read_all()
            assert len(records) == 1
            r = records[0]
            assert r["payload"]["nested"]["pair_id"] == "btcusdt:binance->okx"
            assert r["payload"]["nested"]["depth"]["a"] == 1
            assert r["payload"]["blocked_reasons"] == ["stale_market_data:binance", "low_liquidity:okx"]
            assert len(r["payload"]["candidates"]) == 2
            assert r["payload"]["metadata"]["source"] == "live"

    def test_roundtrip_full_envelope_fields(self):
        """V1 rule: all envelope fields (seq, run_id, ts_ms, kind, payload) survive round-trip."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "envelope.jsonl"
            j = Journal(path)
            j.open()
            ts = 1715000000000
            payload = {"order_id": "ord-123", "venue": "binance", "quantity": 0.15, "price": 68750.5,
                       "reduce_only": True, "post_only": False, "client_order_id": "cl-456"}
            seq = j.append("order.submitted", payload, ts_ms=ts, flush=True)
            j.close()

            records = j.read_all()
            assert len(records) == 1
            r = records[0]
            assert r["seq"] == seq
            assert r["run_id"] == j.run_id
            assert r["ts_ms"] == ts
            assert r["kind"] == "order.submitted"
            assert r["payload"]["order_id"] == "ord-123"
            assert r["payload"]["price"] == 68750.5
            assert r["payload"]["reduce_only"] is True
            assert r["payload"]["client_order_id"] == "cl-456"

    def test_unicode_payload_roundtrip(self):
        """Ensure non-ASCII payload values round-trip correctly."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "unicode.jsonl"
            j = Journal(path)
            j.open()
            payload = {"note": "привет", "symbol": "BTCUSDT", "reason": "市场数据过期"}
            j.append("scan.no_entry_diagnostics", payload, flush=True)
            j.close()

            records = j.read_all()
            assert len(records) == 1
            assert records[0]["payload"]["note"] == "привет"
            assert records[0]["payload"]["reason"] == "市场数据过期"


class TestJournalCriticalAppendDurability:
    """Test that append_critical forces durability while preserving same envelope shape."""

    def test_critical_append_same_envelope_as_append(self):
        """V1 rule: append_critical preserves the same envelope fields as append()."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "critical.jsonl"
            j = Journal(path)
            j.open()

            ts = 1715000000100
            # Regular append
            j.append("scan.completed", {"cycle": 1}, ts_ms=ts)
            # Critical append
            j.append_critical(ts_ms=ts + 1, kind="runtime.lifecycle_changed",
                              payload={"from": "booting", "to": "reconciling", "reason": "startup"})
            j.close()

            records = j.read_all()
            assert len(records) == 2
            # Both have the same envelope shape
            for r in records:
                assert "seq" in r
                assert "run_id" in r
                assert "ts_ms" in r
                assert "kind" in r
                assert "payload" in r

            assert records[0]["kind"] == "scan.completed"
            assert records[1]["kind"] == "runtime.lifecycle_changed"
            assert records[1]["payload"]["to"] == "reconciling"

    def test_critical_append_writes_then_reads_immediately(self):
        """Critical appends must be durable (fsync before return)."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "critical2.jsonl"
            j = Journal(path)
            j.open()

            j.append_critical(ts_ms=1715000000200, kind="runtime.risk_mode_changed",
                              payload={"from": "running", "to": "fail_closed"})
            j.close()

            # Read back from a fresh Journal instance (same path)
            j2 = Journal(path)
            records = j2.read_all()
            assert len(records) == 1
            assert records[0]["kind"] == "runtime.risk_mode_changed"
            assert records[0]["payload"]["to"] == "fail_closed"


# ---------------------------------------------------------------------------
# V2: Streaming reads
# ---------------------------------------------------------------------------


class TestJournalStreaming:
    """Streaming read primitives — avoid read_all() memory pressure."""

    def test_stream_records_yields_all(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "stream.jsonl"
            j = Journal(path)
            j.open()
            j.append("scan.completed", {"cycle": 1}, flush=True)
            j.append("entry.opened", {"position_id": "pos-1"}, flush=True)
            j.append("exit.closed", {"position_id": "pos-1"}, flush=True)
            j.close()

            records = list(j.stream_records())
            assert len(records) == 3
            assert records[0]["kind"] == "scan.completed"
            assert records[1]["kind"] == "entry.opened"
            assert records[2]["kind"] == "exit.closed"

    def test_stream_from_starts_at_given_seq(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "stream_from.jsonl"
            j = Journal(path)
            j.open()
            j.append("scan.completed", {"cycle": 1}, flush=True)   # seq=1
            j.append("scan.completed", {"cycle": 2}, flush=True)   # seq=2
            j.append("entry.opened", {"position_id": "P1"}, flush=True)  # seq=3
            j.append("exit.closed", {"position_id": "P1"}, flush=True)   # seq=4
            j.close()

            records = list(j.stream_from(start_seq=3))
            assert len(records) == 2
            assert records[0]["seq"] == 3
            assert records[0]["kind"] == "entry.opened"
            assert records[1]["seq"] == 4
            assert records[1]["kind"] == "exit.closed"

    def test_stream_from_beyond_max_seq_returns_empty(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "stream_beyond.jsonl"
            j = Journal(path)
            j.open()
            j.append("test", {"x": 1}, flush=True)
            j.close()

            records = list(j.stream_from(start_seq=99))
            assert records == []

    def test_stream_records_empty_file(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "empty.jsonl"
            j = Journal(path)
            j.open()
            j.close()
            records = list(j.stream_records())
            assert records == []

    def test_stream_records_missing_file(self):
        j = Journal("/tmp/nonexistent/stream_test.jsonl")
        records = list(j.stream_records())
        assert records == []

    def test_max_seq_returns_highest(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "maxseq.jsonl"
            j = Journal(path)
            j.open()
            j.append("first", {"x": 1}, flush=True)
            j.append("second", {"x": 2}, flush=True)
            j.append("third", {"x": 3}, flush=True)
            j.close()

            assert j.max_seq == 3

    def test_max_seq_missing_file_returns_0(self):
        j = Journal("/tmp/nonexistent/maxseq_test.jsonl")
        assert j.max_seq == 0

    def test_max_seq_empty_file_returns_0(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "empty_maxseq.jsonl"
            j = Journal(path)
            j.open()
            j.close()
            assert j.max_seq == 0


# ---------------------------------------------------------------------------
# V2: JournalIndex
# ---------------------------------------------------------------------------


class TestJournalIndex:
    """Lightweight seq→byte_offset index for sub-linear seeks."""

    def test_build_and_query(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "idx_journal.jsonl"
            j = Journal(path)
            j.open()
            j.append("scan.completed", {"cycle": 1}, flush=True)
            j.append("entry.opened", {"position_id": "pos-1"}, flush=True)
            j.append("exit.closed", {"position_id": "pos-1"}, flush=True)
            j.close()

            idx = JournalIndex(path)
            assert idx.build() == 3
            assert idx.record_count == 3
            assert idx.max_seq == 3
            for seq in (1, 2, 3):
                assert idx.offset_for(seq) is not None

    def test_load_persisted_index(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "idx_persist.jsonl"
            j = Journal(path)
            j.open()
            j.append("test", {"x": 1}, flush=True)
            j.close()

            idx1 = JournalIndex(path)
            idx1.build()
            assert idx1.record_count == 1

            # Fresh instance loads persisted sidecar
            idx2 = JournalIndex(path)
            assert idx2.load()
            assert idx2.record_count == 1
            assert idx2.max_seq == 1
            assert idx2.offset_for(1) is not None

    def test_build_empty_file(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "idx_empty.jsonl"
            j = Journal(path)
            j.open()
            j.close()

            idx = JournalIndex(path)
            assert idx.build() == 0
            assert idx.record_count == 0
            assert idx.max_seq == 0

    def test_load_missing_returns_false(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "idx_no_file.jsonl"
            idx = JournalIndex(path)
            assert idx.load() is False
            assert idx.record_count == 0

    def test_stream_from_with_index_uses_seek(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "idx_stream.jsonl"
            j = Journal(path)
            j.open()
            for i in range(20):
                j.append(f"kind.{i}", {"n": i}, flush=True)
            j.close()

            idx = JournalIndex(path)
            idx.build()

            records = list(j.stream_from(start_seq=15, index=idx))
            assert len(records) == 6  # seq 15..20
            assert records[0]["seq"] == 15

    def test_seeks_are_valid_across_builds(self):
        """Index built once must remain valid when journal does not change."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "idx_stable.jsonl"
            journal = Journal(path)
            journal.open()
            journal.append("a", {"n": 1}, flush=True)
            journal.append("b", {"n": 2}, flush=True)
            journal.append("c", {"n": 3}, flush=True)
            journal.close()

            idx = JournalIndex(path)
            idx.build()

            # Use the index with a fresh Journal instance
            j2 = Journal(path)
            records = list(j2.stream_from(start_seq=2, index=idx))
            assert len(records) == 2
            assert records[0]["seq"] == 2
            assert records[1]["seq"] == 3


# ---------------------------------------------------------------------------
# V2: ProjectionBackfill
# ---------------------------------------------------------------------------


class TestProjectionBackfill:
    """Reentrant, idempotent journal-to-projection backfill."""

    @staticmethod
    def _collecting_writer():
        """Return (writer, store) where store is a list collecting records."""
        store: list[dict] = []

        def writer(record: dict) -> bool:
            store.append(dict(record))
            return True

        return writer, store

    @staticmethod
    def _failing_writer(fail_on_seq: int):
        """Return a writer that fails the first time it sees fail_on_seq."""
        failed: set[int] = set()

        def writer(record: dict) -> bool:
            seq = record.get("seq", 0)
            if seq == fail_on_seq and seq not in failed:
                failed.add(seq)
                raise RuntimeError("simulated transient failure")
            return True

        return writer

    @staticmethod
    def _idempotent_writer():
        """Return (writer, seen_seqs) that tolerates replays but tracks uniques."""
        seen: set[int] = set()

        def writer(record: dict) -> bool:
            seq = record.get("seq", 0)
            seen.add(seq)
            return True

        return writer, seen

    # -- Basic backfill --------------------------------------------------

    def test_backfill_writes_all_records(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "bf_journal.jsonl"
            j = Journal(path)
            j.open()
            for i in range(5):
                j.append(f"kind.{i}", {"n": i}, flush=True)
            j.close()

            idx = JournalIndex(path)
            idx.build()

            writer, store = self._collecting_writer()
            cursor_path = Path(td) / "bf_cursor.json"
            bf = ProjectionBackfill(j, idx, cursor_path, writer)

            result = bf.backfill()
            assert result.records_processed == 5
            assert result.errors == 0
            assert len(store) == 5
            assert bf.cursor_seq == 5

    def test_backfill_empty_journal(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "bf_empty.jsonl"
            j = Journal(path)
            j.open()
            j.close()

            idx = JournalIndex(path)
            idx.build()

            writer, store = self._collecting_writer()
            cursor_path = Path(td) / "bf_cursor.json"
            bf = ProjectionBackfill(j, idx, cursor_path, writer)

            result = bf.backfill()
            assert result.records_processed == 0
            assert len(store) == 0
            assert bf.cursor_seq == 0

    # -- Reentrancy / resume ---------------------------------------------

    def test_backfill_resumes_from_cursor(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "bf_resume.jsonl"
            j = Journal(path)
            j.open()
            for i in range(10):
                j.append(f"kind.{i}", {"n": i}, flush=True)
            j.close()

            idx = JournalIndex(path)
            idx.build()

            cursor_path = Path(td) / "bf_cursor.json"
            writer, store = self._collecting_writer()

            # First run writes 5 then "crashes" (simulated by target_seq=5)
            bf1 = ProjectionBackfill(j, idx, cursor_path, writer)
            r1 = bf1.backfill(target_seq=5)
            assert r1.records_processed == 5

            # Second run picks up from cursor
            writer2, store2 = self._collecting_writer()
            bf2 = ProjectionBackfill(j, idx, cursor_path, writer2)
            r2 = bf2.backfill()
            assert r2.records_processed == 5  # seqs 6-10
            assert bf2.cursor_seq == 10

    def test_cursor_advanced_after_each_record(self):
        """Cursor must be durable after every record so crash never loses progress."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "bf_cursor_durable.jsonl"
            j = Journal(path)
            j.open()
            j.append("a", {"n": 1}, flush=True)
            j.append("b", {"n": 2}, flush=True)
            j.append("c", {"n": 3}, flush=True)
            j.close()

            idx = JournalIndex(path)
            idx.build()

            cursor_path = Path(td) / "bf_cursor.json"
            fail_writer = self._failing_writer(fail_on_seq=2)
            bf = ProjectionBackfill(j, idx, cursor_path, fail_writer)
            result = bf.backfill()

            # Should have stopped at seq 2, cursor at 1 (last successful)
            assert result.errors == 1
            assert result.records_processed == 1
            assert bf.cursor_seq == 1

    # -- Idempotency -----------------------------------------------------

    def test_repeat_backfill_does_not_double_write(self):
        """Projection row-writer is idempotent — re-run produces same result."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "bf_idempotent.jsonl"
            j = Journal(path)
            j.open()
            for i in range(5):
                j.append(f"kind.{i}", {"n": i}, flush=True)
            j.close()

            idx = JournalIndex(path)
            idx.build()

            writer, seen = self._idempotent_writer()
            cursor_path1 = Path(td) / "bf_cursor1.json"

            bf1 = ProjectionBackfill(j, idx, cursor_path1, writer)
            r1 = bf1.backfill()
            assert r1.records_processed == 5
            assert len(seen) == 5

            # Fresh cursor: re-process same journal range, idempotent writer
            # must not produce duplicate rows
            cursor_path2 = Path(td) / "bf_cursor2.json"
            bf2 = ProjectionBackfill(j, idx, cursor_path2, writer)
            r2 = bf2.backfill()
            assert r2.records_processed == 5
            assert len(seen) == 5

    def test_backfill_with_no_index_falls_back_to_linear_scan(self):
        """stream_from works correctly even without an index."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "bf_noindex.jsonl"
            j = Journal(path)
            j.open()
            for i in range(5):
                j.append(f"kind.{i}", {"n": i}, flush=True)
            j.close()

            # Build index but don't pass it to backfill — verify
            # cursor still works (cursor tracks seq, not byte offset)
            idx = JournalIndex(path)
            idx.build()
            assert idx.record_count == 5

            writer, store = self._collecting_writer()
            cursor_path = Path(td) / "bf_cursor.json"
            bf = ProjectionBackfill(j, idx, cursor_path, writer)

            result = bf.backfill()
            assert result.records_processed == 5
            assert bf.cursor_seq == 5

    # -- Cursor persistence ----------------------------------------------

    def test_cursor_file_atomic_write(self):
        """Cursor is written atomically (via rename) so corruption is impossible."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "bf_atomic_cursor.jsonl"
            j = Journal(path)
            j.open()
            j.append("test", {"n": 1}, flush=True)
            j.close()

            idx = JournalIndex(path)
            idx.build()

            writer, store = self._collecting_writer()
            cursor_path = Path(td) / "bf_cursor.json"
            bf = ProjectionBackfill(j, idx, cursor_path, writer)

            bf.backfill()
            assert bf.cursor_seq == 1

            # Cursor file should exist and be valid JSON
            with open(cursor_path) as f:
                data = json.load(f)
            assert data["last_projected_seq"] == 1
            assert data["backfill_version"] == 1

            # No .tmp file left behind
            tmp_path = cursor_path.with_suffix(".tmp")
            assert not tmp_path.exists()


# ---------------------------------------------------------------------------
# V2: Projection contracts — classification boundaries
# ---------------------------------------------------------------------------


class TestProjectionContractsClassification:
    """Each journal kind classified as projected, journal_only, or unclassified."""

    def test_classify_projected_order_kinds(self):
        for kind in PROJECTED_ORDER_KINDS:
            assert classify_kind(kind) == "projected", f"{kind} should be projected"
            assert is_projected_kind(kind) is True
            assert is_journal_only_kind(kind) is False

    def test_classify_projected_entry_exit_kinds(self):
        for kind in PROJECTED_ENTRY_EXIT_KINDS:
            assert classify_kind(kind) == "projected", f"{kind} should be projected"
            assert is_projected_kind(kind) is True

    def test_classify_projected_scan_kinds(self):
        for kind in PROJECTED_SCAN_KINDS:
            assert classify_kind(kind) == "projected", f"{kind} should be projected"

    def test_classify_projected_risk_kinds(self):
        for kind in PROJECTED_RISK_KINDS:
            assert classify_kind(kind) == "projected", f"{kind} should be projected"

    def test_classify_projected_l2_health_kinds(self):
        for kind in PROJECTED_L2_HEALTH_KINDS:
            assert classify_kind(kind) == "projected", f"{kind} should be projected"

    def test_classify_projected_execution_kinds(self):
        for kind in PROJECTED_EXECUTION_KINDS:
            assert classify_kind(kind) == "projected", f"{kind} should be projected"

    def test_classify_journal_only_kinds(self):
        for kind in ALL_JOURNAL_ONLY_KINDS:
            assert classify_kind(kind) == "journal_only", f"{kind} should be journal_only"
            assert is_journal_only_kind(kind) is True
            assert is_projected_kind(kind) is False

    def test_no_overlap_between_projected_and_journal_only(self):
        overlap = ALL_PROJECTED_KINDS & ALL_JOURNAL_ONLY_KINDS
        assert overlap == set(), f"Overlap found: {overlap}"

    def test_unclassified_kind_returns_unclassified(self):
        assert classify_kind("nonexistent.fake_event") == "unclassified"
        assert is_projected_kind("nonexistent.fake_event") is False
        assert is_journal_only_kind("nonexistent.fake_event") is False


class TestProjectionContractsFactTableMapping:
    """Each projected kind maps to its concrete fact table."""

    def test_order_kinds_map_to_order_facts(self):
        for kind in PROJECTED_ORDER_KINDS:
            assert fact_table_for_kind(kind) == "order_facts"

    def test_entry_exit_kinds_map_to_entry_exit_facts(self):
        for kind in PROJECTED_ENTRY_EXIT_KINDS:
            assert fact_table_for_kind(kind) == "entry_exit_facts"

    def test_scan_kinds_map_to_scan_facts(self):
        for kind in PROJECTED_SCAN_KINDS:
            assert fact_table_for_kind(kind) == "scan_facts"

    def test_risk_kinds_map_to_risk_counter_facts(self):
        for kind in PROJECTED_RISK_KINDS:
            assert fact_table_for_kind(kind) == "risk_counter_facts"

    def test_l2_health_kinds_map_to_local_l2_health_facts(self):
        for kind in PROJECTED_L2_HEALTH_KINDS:
            assert fact_table_for_kind(kind) == "local_l2_health_facts"

    def test_execution_kinds_map_to_diagnostic_facts(self):
        for kind in PROJECTED_EXECUTION_KINDS:
            assert fact_table_for_kind(kind) == "diagnostic_facts"

    def test_journal_only_kinds_return_none_table(self):
        for kind in list(ALL_JOURNAL_ONLY_KINDS)[:5]:
            assert fact_table_for_kind(kind) is None


class TestProjectionSqliteStore:
    """Projected_facts table with idempotent insert and query."""

    def test_projected_facts_table_exists(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "proj.sqlite"
            store = SqliteStore(path)
            conn = store.open()
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
            table_names = {t[0] for t in tables}
            assert "projected_facts" in table_names
            conn.close()

    def test_insert_projected_fact_succeeds(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "proj.sqlite"
            store = SqliteStore(path)
            conn = store.open()
            inserted = store.insert_projected_fact(
                conn,
                seq=1,
                ts_ms=1715000000000,
                kind="order.submitted",
                venue="binance",
                symbol="BTCUSDT",
                payload_json='{"order_id":"ord-1","price":68750.5}',
            )
            assert inserted is True
            rows = store.query_projected_facts(conn)
            assert len(rows) == 1
            assert rows[0]["kind"] == "order.submitted"
            assert rows[0]["venue"] == "binance"
            assert rows[0]["symbol"] == "BTCUSDT"
            conn.close()

    def test_insert_projected_fact_idempotent(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "proj.sqlite"
            store = SqliteStore(path)
            conn = store.open()
            args = dict(
                conn=conn, seq=5, ts_ms=1715000000100, kind="scan.completed",
                venue="bybit", symbol="ETHUSDT", payload_json='{"cycle":1}',
            )
            first = store.insert_projected_fact(**args)
            assert first is True
            second = store.insert_projected_fact(**args)
            assert second is False  # duplicate (seq, kind) — ignored
            rows = store.query_projected_facts(conn)
            assert len(rows) == 1
            conn.close()

    def test_query_projected_facts_filtered_by_kind(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "proj.sqlite"
            store = SqliteStore(path)
            conn = store.open()
            store.insert_projected_fact(
                conn, seq=1, ts_ms=1000, kind="order.submitted",
                venue="binance", symbol="BTCUSDT", payload_json="{}",
            )
            store.insert_projected_fact(
                conn, seq=2, ts_ms=2000, kind="scan.completed",
                venue="binance", symbol="BTCUSDT", payload_json="{}",
            )
            store.insert_projected_fact(
                conn, seq=3, ts_ms=3000, kind="order.filled",
                venue="binance", symbol="ETHUSDT", payload_json="{}",
            )

            orders = store.query_projected_facts(conn, kind="order.submitted")
            assert len(orders) == 1
            assert orders[0]["kind"] == "order.submitted"

            scans = store.query_projected_facts(conn, kind="scan.completed")
            assert len(scans) == 1
            assert scans[0]["kind"] == "scan.completed"
            conn.close()

    def test_projection_cursor_defaults_to_zero(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "proj.sqlite"
            store = SqliteStore(path)
            conn = store.open()
            cursor = store.get_projection_cursor(conn)
            assert cursor["last_projected_seq"] == 0
            assert cursor["last_projected_at_ms"] == 0
            assert cursor["total_facts_written"] == 0
            assert cursor["total_failures"] == 0
            conn.close()

    def test_has_projection_data_returns_false_when_empty(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "proj.sqlite"
            store = SqliteStore(path)
            conn = store.open()
            assert store.has_projection_data(conn) is False
            conn.close()
