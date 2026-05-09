"""Tests for persistence: journal, snapshot, SQLite."""

import json
import tempfile
from pathlib import Path

import pytest

from lightfee.persistence.journal import Journal
from lightfee.persistence.metrics import PersistenceMetrics
from lightfee.persistence.snapshot_store import SnapshotStore
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
