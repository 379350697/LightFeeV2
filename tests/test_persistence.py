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
