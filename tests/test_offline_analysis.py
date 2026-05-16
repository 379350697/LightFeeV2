"""Tests for offline analysis: journal stats, PnL summary, incident reports, diagnostics,
projection writer, and structured store read paths."""

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from lightfee.offline.analysis.journal import (
    JournalAnalysisReport,
    analyze_from_store,
    analyze_journal_or_store,
    analyze_journal_records,
)
from lightfee.offline.analysis.incident import build_incident_report
from lightfee.offline.reports.daily import generate_daily_snapshot
from lightfee.offline.reports.render import render_json, render_text
from lightfee.persistence.journal import Journal
from lightfee.persistence.metrics import PersistenceMetrics
from lightfee.persistence.projection_writer import (
    ProjectionWriter,
    is_projected_kind,
    is_journal_only_kind,
)
from lightfee.persistence.sqlite_store import SqliteStore


class TestJournalAnalysis:
    def test_analyzes_entry_and_exit(self):
        records = [
            {"kind": "entry.opened", "payload": {"entry_fee_quote": 5.0, "symbol": "BTCUSDT"}},
            {"kind": "exit.closed", "payload": {"net_quote": 50.0, "exit_fee_quote": 3.0}},
            {"kind": "order.submitted", "payload": {"venue": "binance"}},
            {"kind": "order.filled", "payload": {"venue": "binance", "latency_ms": 150}},
            {"kind": "order.rejected", "payload": {"venue": "binance"}},
        ]
        report = analyze_journal_records(records)
        assert report.daily.entry_count == 1
        assert report.daily.exit_count == 1
        assert report.daily.total_pnl_quote == 50.0
        assert report.daily.total_fee_quote == 8.0  # 5.0 entry + 3.0 exit

        binance = report.venue_stats["binance"]
        assert binance.order_count == 1
        assert binance.fill_count == 1
        assert binance.failure_count == 1
        assert binance.max_latency_ms == 150
        assert binance.min_latency_ms == 150

    def test_consumes_recovery_records(self):
        records = [
            {"kind": "recovery.live_detected", "payload": {"position_id": "pos1"}},
            {"kind": "recovery.live_detected", "payload": {"position_id": "pos2"}},
            {"kind": "recovery.flat", "payload": {"position_id": "pos3"}},
            {"kind": "recovery.blocked", "payload": {"position_id": "pos4"}},
            {"kind": "recovery.mismatch_detected", "payload": {"position_id": "pos5"}},
            {"kind": "recovery.resumed", "payload": {"position_id": "pos6"}},
        ]
        report = analyze_journal_records(records)
        assert report.recovery_counts["recovery.live_detected"] == 2
        assert report.recovery_counts["recovery.flat"] == 1
        assert report.recovery_counts["recovery.blocked"] == 1
        assert report.recovery_counts["recovery.mismatch_detected"] == 1
        assert report.recovery_counts["recovery.resumed"] == 1

    def test_consumes_risk_records(self):
        records = [
            {"kind": "risk.warning_triggered", "payload": {"health_ratio": 0.5}},
            {"kind": "risk.warning_triggered", "payload": {"health_ratio": 0.4}},
            {"kind": "risk.warning_cleared", "payload": {}},
            {"kind": "risk.death_triggered", "payload": {"reason": "equity_drawdown"}},
            {"kind": "risk.single_side_protection_triggered", "payload": {"venue": "binance"}},
        ]
        report = analyze_journal_records(records)
        assert report.risk_counts["risk.warning_triggered"] == 2
        assert report.risk_counts["risk.warning_cleared"] == 1
        assert report.risk_counts["risk.death_triggered"] == 1
        assert report.risk_counts["risk.single_side_protection_triggered"] == 1

    def test_consumes_scan_diagnostics(self):
        records = [
            {
                "kind": "scan.no_entry_diagnostics",
                "payload": {"reason": "no_candidates", "symbol_count": 0},
            },
            {
                "kind": "scan.no_entry_diagnostics",
                "payload": {"reason": "all_blocked", "symbol_count": 8},
            },
            {
                "kind": "scan.runtime_gate_blocked",
                "payload": {"reason": "global_risk_mode", "mode": "risk_only"},
            },
            {
                "kind": "scan.runtime_gate_blocked",
                "payload": {"reason": "insufficient_balance", "available": 100.0},
            },
        ]
        report = analyze_journal_records(records)
        assert report.scan_no_entry_diagnostics_count == 2
        assert report.scan_runtime_gate_blocked_count == 2

    def test_consumes_execution_diagnostics(self):
        records = [
            {
                "kind": "execution.entry_liquidity_blocked",
                "payload": {"position_id": "p1", "reason": "spread_too_wide"},
            },
            {
                "kind": "execution.entry_liquidity_blocked",
                "payload": {"position_id": "p2", "reason": "insufficient_depth"},
            },
            {
                "kind": "execution.entry_liquidity_blocked",
                "payload": {"position_id": "p3", "reason": "venue_throttled"},
            },
        ]
        report = analyze_journal_records(records)
        assert report.execution_liquidity_blocked_count == 3


class TestProductionBlockerAnalyzer:
    def test_synthetic_20260515_fixture_counts_entry_l2_snapshot_and_orders(self):
        fixture = Path("tests/fixtures/journals/production_entry_l2_blockers_20260515.jsonl")
        result = subprocess.run(
            [
                sys.executable,
                "scripts/analyze_production_blockers.py",
                "--since",
                "2026-05-15T00:00:00+08:00",
                "--json",
                str(fixture),
            ],
            check=True,
            text=True,
            capture_output=True,
        )
        report = json.loads(result.stdout)

        assert report["entry_l2_blocker_counts"]["entry_local_l2_waiting_for_prewarm_window"] == 1
        assert report["entry_l2_blocker_counts"]["entry_local_l2_waiting_for_dual_ready"] == 1
        assert report["entry_l2_not_ready_reason_counts"]["book_missing"] == 1
        assert report["entry_l2_not_ready_reason_counts"]["waiting_for_dual_ready"] == 1
        assert report["snapshot_degraded_counts"]["liquidity"] == 1
        assert report["snapshot_stale_counts"]["snapshot_publish_stale"] == 1
        assert report["order_event_counts"]["order.submit_attempt"] == 1
        assert report["order_event_counts"]["order.submit_result"] == 1
        assert report["exchange_error_counts"]["precision_rejected"] == 1
        assert report["top_pairs"][0]["pair_id"] == "polyxusdt:binance->hyperliquid"
        assert report["top_symbols"][0]["symbol"] == "POLYXUSDT"
        assert report["first_ts_ms"] == 1778784001000
        assert report["last_ts_ms"] == 1778784009000

    def test_consumes_local_l2_diagnostics(self):
        records = [
            {
                "kind": "runtime.local_l2_sequence_gap",
                "payload": {"continuity_reason": "ws_disconnect", "gap_ms": 5000},
            },
            {
                "kind": "runtime.local_l2_sequence_gap",
                "payload": {"continuity_reason": "rest_fallback", "gap_ms": 2000},
            },
            {
                "kind": "runtime.local_l2_sync_failed",
                "payload": {"failure_category": "timeout", "venue": "binance"},
            },
            {
                "kind": "runtime.local_l2_sync_failed",
                "payload": {"failure_category": "auth_error", "venue": "bybit"},
            },
            {
                "kind": "runtime.local_l2_sync_failed",
                "payload": {"failure_category": "timeout", "venue": "okx"},
            },
        ]
        report = analyze_journal_records(records)
        assert report.local_l2_sequence_gap_count == 2
        assert report.local_l2_sync_failed_count == 3

    def test_empty_records_returns_defaults(self):
        report = analyze_journal_records([])
        assert report.total_records == 0
        assert report.venue_stats == {}
        assert report.recovery_counts == {}
        assert report.risk_counts == {}
        assert report.scan_no_entry_diagnostics_count == 0
        assert report.scan_runtime_gate_blocked_count == 0
        assert report.execution_liquidity_blocked_count == 0
        assert report.local_l2_sequence_gap_count == 0
        assert report.local_l2_sync_failed_count == 0
        assert report.daily.entry_count == 0

    def test_full_pipeline(self):
        """Records spanning multiple categories produce a complete report."""
        records = [
            {"kind": "entry.opened", "payload": {"entry_fee_quote": 3.0, "symbol": "ETHUSDT"}},
            {"kind": "entry.opened", "payload": {"entry_fee_quote": 4.0, "symbol": "BTCUSDT"}},
            {"kind": "exit.closed", "payload": {"net_quote": 120.0, "exit_fee_quote": 5.0}},
            {"kind": "order.submitted", "payload": {"venue": "binance"}},
            {"kind": "order.filled", "payload": {"venue": "binance", "latency_ms": 200}},
            {"kind": "order.rejected", "payload": {"venue": "bybit"}},
            {"kind": "order.uncertain", "payload": {"venue": "bybit"}},
            {"kind": "recovery.live_detected", "payload": {"position_id": "p1"}},
            {"kind": "risk.warning_triggered", "payload": {"health_ratio": 0.3}},
            {"kind": "scan.no_entry_diagnostics", "payload": {"reason": "no_candidates"}},
            {"kind": "execution.entry_liquidity_blocked", "payload": {"position_id": "p2"}},
            {"kind": "runtime.local_l2_sequence_gap", "payload": {"continuity_reason": "ws"}},
            {"kind": "runtime.local_l2_sync_failed", "payload": {"failure_category": "timeout"}},
        ]
        report = analyze_journal_records(records)
        assert report.total_records == 13
        assert report.daily.entry_count == 2
        assert report.daily.exit_count == 1
        assert report.daily.total_pnl_quote == 120.0
        assert report.daily.total_fee_quote == 12.0
        assert report.venue_stats["binance"].fill_count == 1
        assert report.venue_stats["bybit"].failure_count == 2
        assert report.recovery_counts["recovery.live_detected"] == 1
        assert report.risk_counts["risk.warning_triggered"] == 1
        assert report.scan_no_entry_diagnostics_count == 1
        assert report.execution_liquidity_blocked_count == 1
        assert report.local_l2_sequence_gap_count == 1
        assert report.local_l2_sync_failed_count == 1


class TestIncidentReport:
    def test_no_errors_no_incident(self):
        records = [{"kind": "entry.opened", "payload": {}}]
        report = build_incident_report(records, None, 1000)
        assert report is None

    def test_errors_produce_incident(self):
        records = [{"kind": "runtime.tick_error", "payload": {"error": "test error"}}]
        report = build_incident_report(records, None, 1000)
        assert report is not None
        assert "error" in report.summary.lower()


class TestDailyReport:
    def test_report_summary_includes_all_sections(self):
        """Daily snapshot report must include PnL, fee, venue, recovery, risk,
        scan diagnostics, and local-L2 health."""
        records = [
            {"kind": "entry.opened", "payload": {"entry_fee_quote": 5.0, "symbol": "BTCUSDT"}},
            {"kind": "exit.closed", "payload": {"net_quote": 50.0, "exit_fee_quote": 3.0}},
            {"kind": "order.submitted", "payload": {"venue": "binance"}},
            {"kind": "order.filled", "payload": {"venue": "binance", "latency_ms": 80}},
            {"kind": "recovery.live_detected", "payload": {"position_id": "p1"}},
            {"kind": "risk.warning_triggered", "payload": {"health_ratio": 0.5}},
            {"kind": "scan.no_entry_diagnostics", "payload": {"reason": "no_candidates"}},
            {"kind": "scan.runtime_gate_blocked", "payload": {"reason": "risk_only"}},
            {"kind": "execution.entry_liquidity_blocked", "payload": {"position_id": "p2", "reason": "spread"}},
            {"kind": "runtime.local_l2_sequence_gap", "payload": {"continuity_reason": "ws"}},
            {"kind": "runtime.local_l2_sync_failed", "payload": {"failure_category": "timeout"}},
        ]
        with tempfile.TemporaryDirectory() as td:
            journal_path = Path(td) / "test.jsonl"
            j = Journal(journal_path)
            j.open()
            for r in records:
                j.append(r["kind"], r.get("payload", {}))
            j.close()

            sqlite_path = Path(td) / "test.sqlite"
            summary = generate_daily_snapshot(
                journal_path=journal_path,
                sqlite_path=sqlite_path,
                date="2026-05-12",
            )

        # Core PnL
        assert summary["date"] == "2026-05-12"
        assert summary["total_pnl_quote"] == 50.0
        assert summary["total_fee_quote"] == 8.0
        assert summary["entry_count"] == 1
        assert summary["exit_count"] == 1

        # Venue stats
        assert "binance" in summary["venue_stats"]
        assert summary["venue_stats"]["binance"]["fill_count"] == 1
        assert summary["venue_stats"]["binance"]["order_count"] == 1

        # Recovery
        assert summary["recovery_counts"]["recovery.live_detected"] == 1

        # Risk
        assert summary["risk_counts"]["risk.warning_triggered"] == 1

        # Scan diagnostics
        assert summary["scan_no_entry_diagnostics"] == 1
        assert summary["scan_runtime_gate_blocked"] == 1

        # Execution
        assert summary["execution_liquidity_blocked"] == 1

        # Local-L2
        assert summary["local_l2_sequence_gap_count"] == 1
        assert summary["local_l2_sync_failed_count"] == 1

    def test_report_renders_json_deterministically(self):
        """Rendered JSON output is stable and includes all report sections."""
        records = [
            {"kind": "entry.opened", "payload": {"entry_fee_quote": 1.0}},
            {"kind": "recovery.flat", "payload": {}},
            {"kind": "risk.warning_cleared", "payload": {}},
        ]
        report = analyze_journal_records(records)
        report.daily.date = "2026-05-12"
        result = render_json({
            "date": report.daily.date,
            "total_pnl_quote": report.daily.total_pnl_quote,
            "total_fee_quote": report.daily.total_fee_quote,
            "entry_count": report.daily.entry_count,
            "recovery_counts": report.recovery_counts,
            "risk_counts": report.risk_counts,
        })
        assert "2026-05-12" in result
        assert "recovery.flat" in result
        assert "risk.warning_cleared" in result
        # Second render produces identical output
        result2 = render_json({
            "date": report.daily.date,
            "total_pnl_quote": report.daily.total_pnl_quote,
            "total_fee_quote": report.daily.total_fee_quote,
            "entry_count": report.daily.entry_count,
            "recovery_counts": report.recovery_counts,
            "risk_counts": report.risk_counts,
        })
        assert result == result2


class TestReportRendering:
    def test_render_json(self):
        data = {"key": "value", "num": 42}
        result = render_json(data)
        assert "key" in result
        assert "42" in result

    def test_render_text(self):
        data = {"key": "value"}
        result = render_text(data)
        assert "key" in result
        assert "value" in result


# ---------------------------------------------------------------------------
# Projection writer tests
# ---------------------------------------------------------------------------

def _make_record(seq: int, kind: str, ts_ms: int = 1000, **payload):
    return {"seq": seq, "kind": kind, "ts_ms": ts_ms, "payload": payload}


class TestProjectionWriter:
    """Tests for idempotent journal-to-structured-store projection."""

    @staticmethod
    def _open_store():
        td = tempfile.mkdtemp()
        store = SqliteStore(Path(td) / "test.sqlite")
        conn = store.open()
        return td, store, conn

    def test_projects_order_facts(self):
        _td, store, conn = self._open_store()
        records = [
            _make_record(1, "order.submitted", venue="binance", symbol="BTCUSDT"),
            _make_record(2, "order.filled", venue="binance", symbol="BTCUSDT", latency_ms=150, fee_quote=0.5),
            _make_record(3, "order.rejected", venue="bybit", symbol="ETHUSDT"),
            _make_record(4, "order.uncertain", venue="bybit", symbol="ETHUSDT"),
        ]
        writer = ProjectionWriter(store)
        result = writer.project_records(conn, records)
        assert result["appended"] == 4
        assert result["skipped"] == 0
        assert result["failed"] == 0

        rows = store.query_order_facts(conn)
        assert len(rows) == 4
        kinds = {r["kind"] for r in rows}
        assert kinds == {"order.submitted", "order.filled", "order.rejected", "order.uncertain"}

        # Verify filled row
        filled = [r for r in rows if r["kind"] == "order.filled"][0]
        assert filled["filled"] == 1
        assert filled["latency_ms"] == 150
        assert filled["fee_quote"] == 0.5

    def test_projects_entry_exit_facts(self):
        _td, store, conn = self._open_store()
        records = [
            _make_record(1, "entry.opened", symbol="BTCUSDT", entry_fee_quote=5.0),
            _make_record(2, "exit.closed", symbol="BTCUSDT", net_quote=50.0, exit_fee_quote=3.0),
        ]
        writer = ProjectionWriter(store)
        result = writer.project_records(conn, records)
        assert result["appended"] == 2

        rows = store.query_entry_exit_facts(conn)
        assert len(rows) == 2
        entry = [r for r in rows if r["kind"] == "entry.opened"][0]
        assert entry["entry_fee_quote"] == 5.0
        exit_ = [r for r in rows if r["kind"] == "exit.closed"][0]
        assert exit_["net_quote"] == 50.0

    def test_projects_risk_counter_facts(self):
        _td, store, conn = self._open_store()
        records = [
            _make_record(1, "risk.warning_triggered", health_ratio=0.3),
            _make_record(2, "risk.warning_cleared"),
            _make_record(3, "risk.death_triggered", reason="equity_drawdown"),
            _make_record(4, "risk.single_side_protection_triggered", venue="binance"),
        ]
        writer = ProjectionWriter(store)
        result = writer.project_records(conn, records)
        assert result["appended"] == 4

        rows = store.query_risk_counter_facts(conn)
        assert len(rows) == 4
        kinds = {r["kind"] for r in rows}
        assert "risk.warning_triggered" in kinds
        assert "risk.death_triggered" in kinds

    def test_projects_local_l2_health_facts(self):
        _td, store, conn = self._open_store()
        records = [
            _make_record(1, "runtime.local_l2_sequence_gap", continuity_reason="ws_disconnect", gap_ms=5000),
            _make_record(2, "runtime.local_l2_sync_failed", failure_category="timeout", venue="binance"),
        ]
        writer = ProjectionWriter(store)
        result = writer.project_records(conn, records)
        assert result["appended"] == 2

        rows = store.query_local_l2_health_facts(conn)
        assert len(rows) == 2
        gap = [r for r in rows if r["kind"] == "runtime.local_l2_sequence_gap"][0]
        assert gap["reason"] == "ws_disconnect"
        assert gap["category"] == "sequence_gap"
        fail = [r for r in rows if r["kind"] == "runtime.local_l2_sync_failed"][0]
        assert fail["reason"] == "timeout"
        assert fail["category"] == "sync_failed"
        assert fail["venue"] == "binance"

    def test_projects_diagnostic_facts(self):
        _td, store, conn = self._open_store()
        records = [
            _make_record(1, "scan.no_entry_diagnostics", reason="no_candidates"),
            _make_record(2, "scan.runtime_gate_blocked", reason="risk_only"),
            _make_record(3, "execution.entry_liquidity_blocked", reason="spread_too_wide", eligibility_class="class_a"),
            _make_record(4, "runtime.fail_closed", reason="venue_disconnect"),
        ]
        writer = ProjectionWriter(store)
        result = writer.project_records(conn, records)
        assert result["appended"] == 4

        rows = store.query_diagnostic_facts(conn)
        assert len(rows) == 4
        kinds = {r["kind"] for r in rows}
        assert "scan.no_entry_diagnostics" in kinds
        assert "runtime.fail_closed" in kinds

        exec_row = [r for r in rows if r["kind"] == "execution.entry_liquidity_blocked"][0]
        assert exec_row["reason"] == "spread_too_wide"
        assert exec_row["classification"] == "class_a"

    def test_idempotent_reprojection(self):
        """Reprojecting the same records must not duplicate facts."""
        _td, store, conn = self._open_store()
        records = [
            _make_record(1, "order.submitted", venue="binance"),
            _make_record(2, "entry.opened", entry_fee_quote=5.0),
            _make_record(3, "risk.warning_triggered"),
        ]
        writer = ProjectionWriter(store)

        r1 = writer.project_records(conn, records)
        assert r1["appended"] == 3

        r2 = writer.project_records(conn, records)
        assert r2["appended"] == 0
        assert r2["skipped"] == 3

        assert len(store.query_order_facts(conn)) == 1
        assert len(store.query_entry_exit_facts(conn)) == 1
        assert len(store.query_risk_counter_facts(conn)) == 1

    def test_skips_journal_only_kinds(self):
        """Recovery and lifecycle records must stay in journal, never projected."""
        _td, store, conn = self._open_store()
        records = [
            _make_record(1, "recovery.live_detected", position_id="p1"),
            _make_record(2, "runtime.lifecycle_changed", to="running"),
            _make_record(3, "runtime.risk_mode_changed", to="risk_only"),
            _make_record(4, "recovery.resumed", position_id="p2"),
            _make_record(5, "runtime.booting"),
            _make_record(6, "runtime.stopped"),
        ]
        writer = ProjectionWriter(store)
        result = writer.project_records(conn, records)
        assert result["appended"] == 0
        assert result["skipped"] == 0
        assert result["failed"] == 0

        assert not store.has_projection_data(conn)

    def test_cursor_tracks_projection_progress(self):
        _td, store, conn = self._open_store()
        records = [
            _make_record(1, "entry.opened", entry_fee_quote=5.0),
            _make_record(2, "exit.closed", net_quote=100.0),
        ]
        writer = ProjectionWriter(store)
        writer.project_records(conn, records)

        cursor = store.get_projection_cursor(conn)
        assert cursor["last_projected_seq"] == 2
        assert cursor["total_facts_written"] == 2
        assert cursor["total_failures"] == 0

    def test_metrics_tracked_during_projection(self):
        _td, store, conn = self._open_store()
        metrics = PersistenceMetrics()
        records = [
            _make_record(1, "order.submitted", venue="binance"),
            _make_record(2, "order.filled", venue="binance", latency_ms=100),
        ]
        writer = ProjectionWriter(store, metrics=metrics)
        writer.project_records(conn, records)

        assert metrics.projection_appends == 2
        assert metrics.projection_skips == 0
        assert metrics.projection_failures == 0
        assert metrics.last_projection_seq == 2


class TestStoreBackedAnalysis:
    """Tests for structured-store-backed analysis with journal fallback."""

    @staticmethod
    def _setup_store_with_data(records: list[dict]):
        td = tempfile.mkdtemp()
        store = SqliteStore(Path(td) / "test.sqlite")
        conn = store.open()
        writer = ProjectionWriter(store)
        writer.project_records(conn, records)
        return td, store, conn

    def test_analyze_from_store_matches_journal_scan(self):
        records = [
            _make_record(1, "entry.opened", entry_fee_quote=5.0, symbol="BTCUSDT"),
            _make_record(2, "exit.closed", net_quote=50.0, exit_fee_quote=3.0),
            _make_record(3, "order.submitted", venue="binance"),
            _make_record(4, "order.filled", venue="binance", latency_ms=150, fee_quote=0.5),
            _make_record(5, "order.rejected", venue="bybit"),
            _make_record(6, "risk.warning_triggered", health_ratio=0.3),
            _make_record(7, "scan.no_entry_diagnostics", reason="no_candidates"),
            _make_record(8, "runtime.local_l2_sequence_gap", continuity_reason="ws"),
            _make_record(9, "runtime.local_l2_sync_failed", failure_category="timeout"),
            _make_record(10, "execution.entry_liquidity_blocked", reason="spread", eligibility_class="A"),
        ]
        _td, store, conn = self._setup_store_with_data(records)

        store_report = analyze_from_store(conn)
        journal_report = analyze_journal_records(records)

        # Core PnL should match
        assert store_report.daily.entry_count == journal_report.daily.entry_count
        assert store_report.daily.exit_count == journal_report.daily.exit_count
        assert store_report.daily.total_pnl_quote == journal_report.daily.total_pnl_quote
        assert store_report.daily.total_fee_quote == journal_report.daily.total_fee_quote

        # Venue stats
        assert store_report.venue_stats["binance"].fill_count == journal_report.venue_stats["binance"].fill_count
        assert store_report.venue_stats["bybit"].failure_count == journal_report.venue_stats["bybit"].failure_count

        # Risk
        assert store_report.risk_counts == journal_report.risk_counts

        # Diagnostics
        assert store_report.scan_no_entry_diagnostics_count == journal_report.scan_no_entry_diagnostics_count
        assert store_report.local_l2_sequence_gap_count == journal_report.local_l2_sequence_gap_count
        assert store_report.local_l2_sync_failed_count == journal_report.local_l2_sync_failed_count
        assert store_report.execution_liquidity_blocked_count == journal_report.execution_liquidity_blocked_count

    def test_analyze_journal_or_store_prefers_store(self):
        records = [
            _make_record(1, "entry.opened", entry_fee_quote=5.0, symbol="BTCUSDT"),
            _make_record(2, "risk.warning_triggered"),
        ]
        _td, store, conn = self._setup_store_with_data(records)

        report = analyze_journal_or_store(conn=conn, records=records)
        assert report.daily.entry_count == 1
        assert report.risk_counts["risk.warning_triggered"] == 1

    def test_analyze_journal_or_store_falls_back_when_store_empty(self):
        records = [
            _make_record(1, "entry.opened", entry_fee_quote=5.0, symbol="BTCUSDT"),
        ]
        td = tempfile.mkdtemp()
        store = SqliteStore(Path(td) / "test.sqlite")
        conn = store.open()
        # No projection — store is empty

        report = analyze_journal_or_store(conn=conn, records=records)
        assert report.daily.entry_count == 1

    def test_analyze_journal_or_store_handles_missing_conn(self):
        records = [
            _make_record(1, "entry.opened", entry_fee_quote=5.0, symbol="BTCUSDT"),
        ]
        report = analyze_journal_or_store(conn=None, records=records)
        assert report.daily.entry_count == 1

    def test_analyze_journal_or_store_returns_empty_when_no_data(self):
        report = analyze_journal_or_store(conn=None, records=None)
        assert report.total_records == 0
        assert report.daily.entry_count == 0

    def test_store_has_no_recovery_or_lifecycle_data(self):
        """Recovery and lifecycle events are journal-only — store analysis returns 0 for them."""
        records = [
            _make_record(1, "entry.opened", entry_fee_quote=5.0),
            _make_record(2, "recovery.live_detected", position_id="p1"),
            _make_record(3, "runtime.lifecycle_changed", to="running"),
        ]
        _td, store, conn = self._setup_store_with_data(records)

        store_report = analyze_from_store(conn)
        journal_report = analyze_journal_records(records)

        # Store: recovery not projected
        assert store_report.recovery_counts == {}
        # Journal: has recovery
        assert journal_report.recovery_counts["recovery.live_detected"] == 1

        # Store: PnL still captured
        assert store_report.daily.entry_count == 1


class TestProjectionClassification:
    """Tests for event kind classification boundaries."""

    def test_projected_kinds_are_identified(self):
        assert is_projected_kind("order.submitted")
        assert is_projected_kind("order.filled")
        assert is_projected_kind("entry.opened")
        assert is_projected_kind("exit.closed")
        assert is_projected_kind("risk.warning_triggered")
        assert is_projected_kind("runtime.local_l2_sequence_gap")
        assert is_projected_kind("scan.no_entry_diagnostics")
        assert is_projected_kind("execution.entry_liquidity_blocked")
        assert is_projected_kind("runtime.fail_closed")
        assert is_projected_kind("runtime.fail_closed_venue_disconnect")

    def test_journal_only_kinds_are_identified(self):
        assert is_journal_only_kind("recovery.live_detected")
        assert is_journal_only_kind("recovery.flat")
        assert is_journal_only_kind("recovery.blocked")
        assert is_journal_only_kind("recovery.mismatch_detected")
        assert is_journal_only_kind("recovery.mismatch_flattened")
        assert is_journal_only_kind("recovery.resumed")
        assert is_journal_only_kind("runtime.lifecycle_changed")
        assert is_journal_only_kind("runtime.risk_mode_changed")
        assert is_journal_only_kind("runtime.booting")
        assert is_journal_only_kind("runtime.running")
        assert is_journal_only_kind("runtime.stopped")

    def test_journal_only_is_not_projected(self):
        for kind in ["recovery.live_detected", "runtime.lifecycle_changed", "runtime.stopped"]:
            assert is_journal_only_kind(kind)
            assert not is_projected_kind(kind)

    def test_unknown_kind_is_neither(self):
        assert not is_projected_kind("some.unknown.kind")
        assert not is_journal_only_kind("some.unknown.kind")


class TestDailyReportWithProjection:
    """Tests for daily report generation with structured store path."""

    def test_daily_snapshot_generates_via_journal_fallback(self):
        """When store has no projection data, daily snapshot falls back to journal scan."""
        records = [
            {"kind": "entry.opened", "payload": {"entry_fee_quote": 5.0, "symbol": "BTCUSDT"}},
            {"kind": "exit.closed", "payload": {"net_quote": 50.0, "exit_fee_quote": 3.0}},
            {"kind": "order.submitted", "payload": {"venue": "binance"}},
        ]
        with tempfile.TemporaryDirectory() as td:
            journal_path = Path(td) / "test.jsonl"
            j = Journal(journal_path)
            j.open()
            for r in records:
                j.append(r["kind"], r.get("payload", {}))
            j.close()

            sqlite_path = Path(td) / "test.sqlite"
            summary = generate_daily_snapshot(
                journal_path=journal_path,
                sqlite_path=sqlite_path,
                date="2026-05-12",
            )

            assert summary["total_pnl_quote"] == 50.0
            assert summary["total_fee_quote"] == 8.0
            assert summary["entry_count"] == 1
            assert summary["exit_count"] == 1

            # After fallback, store should have projection data
            store = SqliteStore(sqlite_path)
            conn2 = store.open()
            assert store.has_projection_data(conn2)
            conn2.close()

    def test_daily_snapshot_with_all_diagnostic_kinds(self):
        """Report includes all diagnostic breakdowns from structured store when available."""
        records = [
            _make_record(1, "entry.opened", entry_fee_quote=5.0),
            _make_record(2, "exit.closed", net_quote=50.0, exit_fee_quote=3.0),
            _make_record(3, "scan.no_entry_diagnostics", reason="no_candidates"),
            _make_record(4, "scan.runtime_gate_blocked", reason="risk_only"),
            _make_record(5, "execution.entry_liquidity_blocked", reason="spread", eligibility_class="A"),
            _make_record(6, "runtime.local_l2_sequence_gap", continuity_reason="ws"),
            _make_record(7, "runtime.local_l2_sync_failed", failure_category="timeout"),
            _make_record(8, "runtime.fail_closed", reason="venue_error"),
            _make_record(9, "risk.warning_triggered", health_ratio=0.3),
        ]
        with tempfile.TemporaryDirectory() as td:
            journal_path = Path(td) / "test.jsonl"
            j = Journal(journal_path)
            j.open()
            for r in records:
                j.append(r["kind"], r.get("payload", {}))
            j.close()

            sqlite_path = Path(td) / "test.sqlite"
            summary = generate_daily_snapshot(
                journal_path=journal_path,
                sqlite_path=sqlite_path,
                date="2026-05-12",
            )

            assert summary["scan_no_entry_diagnostics"] == 1
            assert summary["scan_runtime_gate_blocked"] == 1
            assert summary["execution_liquidity_blocked"] == 1
            assert summary["local_l2_sequence_gap_count"] == 1
            assert summary["local_l2_sync_failed_count"] == 1
            assert summary["entry_liquidity_blocked_by_reason"]["spread"] == 1
            assert summary["local_l2_sequence_gap_by_reason"]["ws"] == 1
            assert summary["local_l2_sync_failed_by_category"]["timeout"] == 1
            assert summary["fail_closed_reason_counts"]["venue_error"] == 1
            assert summary["risk_counts"]["risk.warning_triggered"] == 1


# ===========================================================================
# RED-LIGHT: dry-run audit script (V2 journal format + ts_ms filtering)
# ===========================================================================


class TestDryRunAuditRedLight:
    """RED-LIGHT: scripts/lightfee_v2_live_dryrun_audit.py must handle V2 journal format.

    Current audit script bugs:
      1. Reads `event` field — V2 journal uses `kind`
      2. Prioritizes `data` over `payload` — V2 uses `payload`
      3. No ts_ms window filtering — `minutes` param unused
      4. Script is untracked in git

    These tests MUST fail on current V2 to prove the bugs exist.
    """

    @staticmethod
    def _write_journal(path: str, lines: list[dict]) -> None:
        with open(path, "w") as f:
            for line in lines:
                f.write(json.dumps(line) + "\n")

    @staticmethod
    def _import_audit():
        import sys
        from pathlib import Path as _Path
        scripts_dir = str(_Path(__file__).parent.parent / "scripts")
        if scripts_dir not in sys.path:
            sys.path.insert(0, scripts_dir)
        # Force reimport to avoid stale cached module
        import importlib
        mod_name = "lightfee_v2_live_dryrun_audit"
        if mod_name in sys.modules:
            del sys.modules[mod_name]
        mod = importlib.import_module(mod_name)
        return mod.audit

    def test_audit_reads_kind_field(self, tmp_path):
        """WAS RED-LIGHT, NOW GREEN: V2 journal 'kind' field is read."""
        import time
        audit = self._import_audit()
        now_ms = int(time.time() * 1000)

        journal = tmp_path / "test.jsonl"
        self._write_journal(str(journal), [
            {
                "ts_ms": now_ms,
                "kind": "runtime.entry_blocked_local_l2_selection",
                "payload": {"reason": "entry_local_l2_waiting_for_prewarm_window",
                            "pair_id": "btcusdt:binance->bybit"},
            },
        ])

        # Use huge minutes value to ensure event is within window
        result = audit(str(journal), minutes=52560000)  # 100 years
        counts = result.get("counts", {})
        blocked_count = counts.get("entry_blocked_local_l2_selection", 0)
        assert blocked_count >= 1, (
            f"audit should count entry_blocked_local_l2_selection from 'kind', "
            f"found {blocked_count}"
        )

    def test_audit_reads_payload_field(self, tmp_path):
        """WAS RED-LIGHT, NOW GREEN: V2 journal 'payload' field is read."""
        import time
        audit = self._import_audit()
        now_ms = int(time.time() * 1000)

        journal = tmp_path / "test2.jsonl"
        self._write_journal(str(journal), [
            {
                "ts_ms": now_ms,
                "kind": "runtime.entry_blocked_local_l2_selection",
                "payload": {"reason": "entry_local_l2_waiting_for_prewarm_window"},
            },
        ])

        result = audit(str(journal), minutes=52560000)
        l2_reasons = result.get("l2_selection_reasons", {})
        assert "entry_local_l2_waiting_for_prewarm_window" in l2_reasons, (
            f"audit should extract reason from 'payload', got: {l2_reasons}"
        )

    def test_audit_ts_ms_window_filtering(self, tmp_path):
        """WAS RED-LIGHT, NOW GREEN: ts_ms window filtering excludes old events."""
        import time

        audit = self._import_audit()
        now_ms = int(time.time() * 1000)
        old_ms = now_ms - 3 * 3600 * 1000  # 3 hours ago

        journal = tmp_path / "test3.jsonl"
        self._write_journal(str(journal), [
            {
                "ts_ms": old_ms,
                "kind": "runtime.entry_blocked_local_l2_selection",
                "payload": {"reason": "stale_event"},
            },
            {
                "ts_ms": now_ms,
                "kind": "runtime.entry_blocked_local_l2_selection",
                "payload": {"reason": "recent_event"},
            },
        ])

        result = audit(str(journal), minutes=120)
        l2_reasons = result.get("l2_selection_reasons", {})

        # old event (3h ago) should be excluded by ts_ms window
        stale_count = l2_reasons.get("stale_event", 0)
        assert stale_count == 0, (
            f"stale event from 3h ago should not be counted with minutes=120, "
            f"but got stale_event={stale_count}"
        )
        # Recent event should be counted
        assert l2_reasons.get("recent_event", 0) >= 1, "recent event should be counted"

    def test_script_is_tracked_by_git(self):
        """Verify the audit script is tracked by git.

        RED-LIGHT: script was untracked (?? in git status).
        """
        import subprocess
        script_path = Path(__file__).parent.parent / "scripts" / "lightfee_v2_live_dryrun_audit.py"
        result = subprocess.run(
            ["git", "ls-files", "--error-unmatch", str(script_path)],
            capture_output=True, cwd=str(script_path.parent.parent),
        )
        tracked = result.returncode == 0
        assert tracked, (
            "RED-LIGHT FAIL: audit script must be tracked by git "
            "(currently untracked/new file)"
        )
