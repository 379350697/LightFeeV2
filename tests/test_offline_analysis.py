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
from lightfee.offline.reports.render import (
    render_daily_report_markdown,
    render_json,
    render_text,
)
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
            {
                "kind": "exit_shadow.strategy_decision",
                "payload": {
                    "bot_id": "top_book_imbalance",
                    "direction": "bullish",
                    "confidence": 0.75,
                },
            },
            {
                "kind": "exit_shadow.path_markout",
                "payload": {
                    "bot_id": "top_book_imbalance",
                    "path": "short_first_then_long",
                    "horizon_ms": 1000,
                },
            },
            {
                "kind": "exit_shadow.strategy_summary",
                "payload": {
                    "bot_id": "top_book_imbalance",
                    "direction_correct": True,
                    "incremental_net_bps": 11.0,
                    "max_adverse_bps": 2.0,
                    "excluded": False,
                },
            },
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

        # Exit shadow
        assert summary["exit_shadow_decision_count"] == 1
        assert summary["exit_shadow_path_markout_count"] == 1
        assert summary["exit_shadow_summary_count"] == 1
        shadow = summary["exit_shadow_by_bot"]["top_book_imbalance"]
        assert shadow["sample_count"] == 1
        assert shadow["direction_accuracy"] == 1.0

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

    def test_render_daily_markdown_includes_exit_shadow(self):
        report = {
            "date": "2026-05-12",
            "exit_shadow_decision_count": 1,
            "exit_shadow_path_markout_count": 1,
            "exit_shadow_by_bot": {
                "top_book_imbalance": {
                    "sample_count": 2,
                    "direction_accuracy": 0.5,
                    "win_rate": 0.5,
                    "avg_incremental_net_bps": 4.25,
                }
            },
        }

        result = render_daily_report_markdown(report)

        assert "Exit Shadow Decisions: 1" in result
        assert "top_book_imbalance" in result
        assert "Avg Incremental Net Bps: 4.2500" in result


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

    def test_v1_ledger_backfill_counts_new_ledger_rows_as_appended(self):
        """Upgrade replay writes the new ledger row even when old fact row exists."""
        _td, store, conn = self._open_store()
        records = [
            _make_record(
                1,
                "entry.opened",
                ts_ms=1000,
                position_id="pos-backfill",
                symbol="LABUSDT",
                entry_fee_quote=1.0,
            ),
        ]
        writer = ProjectionWriter(store)

        legacy = writer._project_entry_exit(conn, 1, 1000, "entry.opened", records[0]["payload"])
        assert legacy is True
        result = writer.project_records(conn, records)

        assert result == {"appended": 1, "skipped": 0, "failed": 0}
        assert len(store.query_entry_exit_facts(conn)) == 1
        events = store.query_trade_ledger_events(conn)
        assert len(events) == 1
        assert events[0]["event_kind"] == "entry.opened"
        assert events[0]["entity_id"] == "pos-backfill"

    def test_journal_only_kinds_only_enter_v1_lifecycle_ledger_when_supported(self):
        """Recovery stays journal-first, but V1 live recovery gets a ledger view."""
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
        assert result["appended"] == 1
        assert result["skipped"] == 0
        assert result["failed"] == 0

        assert store.has_projection_data(conn)
        assert not store.query_entry_exit_facts(conn)
        assert not store.query_diagnostic_facts(conn)
        events = store.query_trade_ledger_events(conn)
        assert len(events) == 1
        assert events[0]["event_kind"] == "recovery.live_detected"
        assert events[0]["entity_id"] == "p1"

        duplicate = writer.project_records(conn, records)
        assert duplicate["appended"] == 0
        assert duplicate["skipped"] == 1
        assert duplicate["failed"] == 0

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

    def test_v1_ledger_bridge_records_full_position_order_fill_chain(self):
        """V1 ledger bridge: position, order, fill, and event rows join by ids."""
        _td, store, conn = self._open_store()
        records = [
            _make_record(
                1,
                "entry.opened",
                ts_ms=1000,
                position_id="pos-ledger-1",
                review_id="rvw-ledger-1",
                symbol="LABUSDT",
                long_venue="binance",
                short_venue="bybit",
                quantity=12.0,
                entry_notional_quote=120.0,
                entered_at_ms=900,
            ),
            _make_record(
                2,
                "order.submitted",
                ts_ms=1100,
                position_id="pos-ledger-1",
                review_id="rvw-ledger-1",
                venue="binance",
                symbol="LABUSDT",
                side="buy",
                stage="entry_maker",
                client_order_id="entry-maker-cid",
                order_id="entry-maker-oid",
                requested_quantity=12.0,
            ),
            _make_record(
                3,
                "order.filled",
                ts_ms=1200,
                position_id="pos-ledger-1",
                review_id="rvw-ledger-1",
                venue="binance",
                symbol="LABUSDT",
                side="buy",
                stage="entry_maker",
                client_order_id="entry-maker-cid",
                order_id="entry-maker-oid",
                trade_id="trade-1",
                executed_quantity=12.0,
                average_price=10.0,
                fee_quote=0.1,
                filled_at_ms=1190,
            ),
            _make_record(
                4,
                "exit.closed",
                ts_ms=2000,
                position_id="pos-ledger-1",
                review_id="rvw-ledger-1",
                symbol="LABUSDT",
                long_venue="binance",
                short_venue="bybit",
                reason="funding_capture",
                net_quote=1.23,
                realized_price_pnl_quote=1.56,
                total_entry_fee_quote=0.1,
                total_exit_fee_quote=0.2,
                long_exit_order_id="long-exit-oid",
                short_exit_order_id="short-exit-oid",
                closed_at_ms=2000,
                entry_notional_quote=120.0,
            ),
        ]

        result = ProjectionWriter(store).project_records(conn, records)

        assert result["failed"] == 0
        positions = conn.execute("SELECT * FROM position_ledger").fetchall()
        assert len(positions) == 1
        assert positions[0]["position_id"] == "pos-ledger-1"
        assert positions[0]["state"] == "closed"
        assert positions[0]["truth_level"] == "venue_fill_confirmed"
        assert positions[0]["closed_at_ms"] == 2000

        orders = conn.execute("SELECT * FROM order_ledger").fetchall()
        assert len(orders) == 1
        assert orders[0]["position_id"] == "pos-ledger-1"
        assert orders[0]["client_order_id"] == "entry-maker-cid"
        assert orders[0]["exchange_order_id"] == "entry-maker-oid"
        assert orders[0]["status"] == "filled"
        assert orders[0]["truth_level"] == "venue_fill_confirmed"

        fills = conn.execute("SELECT * FROM fill_ledger").fetchall()
        assert len(fills) == 1
        assert fills[0]["position_id"] == "pos-ledger-1"
        assert fills[0]["exchange_trade_id"] == "trade-1"

        events = conn.execute(
            "SELECT event_kind, entity_type, entity_id, truth_level "
            "FROM trade_ledger_events ORDER BY seq"
        ).fetchall()
        assert [row["event_kind"] for row in events] == [
            "entry.opened",
            "order.submitted",
            "order.filled",
            "exit.closed",
        ]
        assert {row["entity_id"] for row in events} >= {
            "pos-ledger-1",
            "binance:entry-maker-cid",
        }

    def test_v1_ledger_bridge_records_compensation_recovery_and_terminal_problem(self):
        """Compensation/recovery/terminal events must not remain attribution blind spots."""
        _td, store, conn = self._open_store()
        records = [
            _make_record(
                1,
                "recovery.live_detected",
                ts_ms=1000,
                position_id="live-recovered:LABUSDT:binance->bybit",
                symbol="LABUSDT",
                long_venue="binance",
                short_venue="bybit",
                quantity=12.0,
                source="runtime_live_position_probe",
            ),
            _make_record(
                2,
                "exit.compensated",
                ts_ms=1100,
                position_id="live-recovered:LABUSDT:binance->bybit",
                symbol="LABUSDT",
                reason="passive_close_live_one_sided_force_close_problem",
                failed_stage="exit_short",
                failed_venue="bybit",
                compensated_venues=["bybit"],
            ),
            _make_record(
                3,
                "execution.compensation_failed",
                ts_ms=1200,
                position_id="live-recovered:LABUSDT:binance->bybit",
                symbol="LABUSDT",
                phase="close",
                compensation_venue="bybit",
                hard_stop_error="simulated failure",
            ),
            _make_record(
                4,
                "runtime.position_lifecycle_terminal",
                ts_ms=1300,
                position_id="live-recovered:LABUSDT:binance->bybit",
                symbol="LABUSDT",
                long_venue="binance",
                short_venue="bybit",
                terminal_state="flat",
                terminal_reason="passive_close_live_one_sided_force_close_problem",
                problem=True,
                problem_reason="normal_one_sided_flatten_failed_force_close",
                client_order_ids=["force-close-cid"],
                order_ids=["force-close-oid"],
            ),
            _make_record(
                5,
                "recovery.flat",
                ts_ms=1400,
                position_id="live-recovered:LABUSDT:binance->bybit",
                symbol="LABUSDT",
                long_venue="binance",
                short_venue="bybit",
                reason="exchange_flat_local_open",
            ),
        ]

        result = ProjectionWriter(store).project_records(conn, records)

        assert result["failed"] == 0
        events = conn.execute(
            "SELECT event_kind, entity_type, entity_id, truth_level "
            "FROM trade_ledger_events ORDER BY seq"
        ).fetchall()
        assert [row["event_kind"] for row in events] == [
            "recovery.live_detected",
            "exit.compensated",
            "execution.compensation_failed",
            "runtime.position_lifecycle_terminal",
            "recovery.flat",
        ]
        assert all(row["entity_id"] == "live-recovered:LABUSDT:binance->bybit" for row in events)
        assert all(row["truth_level"] == "runtime_estimated" for row in events)

        positions = conn.execute("SELECT * FROM position_ledger").fetchall()
        assert len(positions) == 1
        assert positions[0]["position_id"] == "live-recovered:LABUSDT:binance->bybit"
        assert positions[0]["state"] == "closed"
        assert positions[0]["closed_at_ms"] == 1400
        assert positions[0]["terminal_reason"] == "passive_close_live_one_sided_force_close_problem"
        assert positions[0]["problem"] == 1
        assert positions[0]["problem_reason"] == "normal_one_sided_flatten_failed_force_close"


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

    def test_analyze_from_store_includes_exit_shadow_summary(self):
        records = [
            _make_record(
                1,
                "exit_shadow.strategy_decision",
                shadow_id="shadow-1",
                bot_id="top_book_imbalance",
                direction="bullish",
                recommended_path="short_first_then_long",
                confidence=0.8,
            ),
            _make_record(
                2,
                "exit_shadow.path_markout",
                shadow_id="shadow-1",
                path="short_first_then_long",
                horizon_ms=1000,
                net_bps=12.0,
                max_adverse_bps=1.5,
            ),
            _make_record(
                3,
                "exit_shadow.strategy_summary",
                shadow_id="shadow-1",
                bot_id="top_book_imbalance",
                direction="bullish",
                recommended_path="short_first_then_long",
                horizon_ms=1000,
                direction_correct=True,
                incremental_net_bps=12.0,
                max_adverse_bps=1.5,
                excluded=False,
            ),
        ]
        _td, store, conn = self._setup_store_with_data(records)

        report = analyze_from_store(conn)

        assert report.exit_shadow_decision_count == 1
        assert report.exit_shadow_path_markout_count == 1
        assert report.exit_shadow_summary_count == 1
        summary = report.exit_shadow_by_bot["top_book_imbalance"]
        assert summary["sample_count"] == 1
        assert summary["direction_accuracy"] == 1.0
        assert summary["avg_incremental_net_bps"] == 12.0
        assert summary["max_adverse_bps"] == 1.5

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
        assert is_projected_kind("entry.compensated")
        assert is_projected_kind("exit.compensated")
        assert is_projected_kind("execution.compensation_failed")
        assert is_projected_kind("runtime.position_lifecycle_terminal")
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
        assert is_journal_only_kind("pending_entry.viability_blocked")
        assert is_journal_only_kind("runtime.entry_blocked_lifecycle_selection")
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
            _make_record(
                10,
                "exit_shadow.strategy_decision",
                bot_id="top_book_imbalance",
                direction="bullish",
                confidence=0.75,
            ),
            _make_record(
                11,
                "exit_shadow.path_markout",
                bot_id="top_book_imbalance",
                path="short_first_then_long",
                horizon_ms=1000,
            ),
            _make_record(
                12,
                "exit_shadow.strategy_summary",
                bot_id="top_book_imbalance",
                direction_correct=True,
                incremental_net_bps=9.0,
                max_adverse_bps=1.5,
                excluded=False,
            ),
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
            assert summary["exit_shadow_decision_count"] == 1
            assert summary["exit_shadow_path_markout_count"] == 1
            assert summary["exit_shadow_summary_count"] == 1
            shadow = summary["exit_shadow_by_bot"]["top_book_imbalance"]
            assert shadow["sample_count"] == 1
            assert shadow["avg_incremental_net_bps"] == 9.0


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

    def test_audit_classifies_legacy_ws_bbo_event_as_ws_bbo_not_local_l2(self, tmp_path):
        import time
        audit = self._import_audit()
        now_ms = int(time.time() * 1000)

        journal = tmp_path / "legacy_ws_bbo.jsonl"
        self._write_journal(str(journal), [
            {
                "ts_ms": now_ms,
                "kind": "runtime.entry_blocked_local_l2_selection",
                "payload": {
                    "reason": "entry_ws_bbo_quote_lease_stale_quote",
                    "provider": "ws_bbo_quote_lease",
                    "readiness_evidence": {
                        "provider": "ws_bbo_quote_lease",
                        "source": "ws_bbo_quote_lease",
                    },
                },
            },
            {
                "ts_ms": now_ms,
                "kind": "runtime.entry_blocked_ws_bbo_selection",
                "payload": {
                    "reason": "entry_ws_bbo_quote_lease_missing_quote",
                    "provider": "ws_bbo_quote_lease",
                },
            },
        ])

        result = audit(str(journal), minutes=52560000)

        assert result.get("l2_selection_reasons", {}) == {}
        assert result.get("ws_bbo_selection_reasons", {}) == {
            "entry_ws_bbo_quote_lease_missing_quote": 1,
            "entry_ws_bbo_quote_lease_stale_quote": 1,
        }
        assert result.get("counts", {}).get("entry_blocked_local_l2_selection", 0) == 0
        assert result.get("counts", {}).get("entry_blocked_ws_bbo_selection", 0) == 2

    def test_dry_run_text_output_prints_ws_bbo_selection_summary(
        self, tmp_path, monkeypatch, capsys,
    ):
        import importlib
        import sys
        import time
        from pathlib import Path as _Path

        scripts_dir = str(_Path(__file__).parent.parent / "scripts")
        if scripts_dir not in sys.path:
            sys.path.insert(0, scripts_dir)
        mod_name = "lightfee_v2_live_dryrun_audit"
        if mod_name in sys.modules:
            del sys.modules[mod_name]
        mod = importlib.import_module(mod_name)

        now_ms = int(time.time() * 1000)
        journal = tmp_path / "ws_bbo_text.jsonl"
        self._write_journal(str(journal), [
            {
                "ts_ms": now_ms,
                "kind": "runtime.entry_blocked_ws_bbo_selection",
                "payload": {
                    "reason": "entry_ws_bbo_quote_lease_missing_quote",
                    "provider": "ws_bbo_quote_lease",
                },
            },
        ])

        monkeypatch.setattr(
            sys,
            "argv",
            [
                "lightfee_v2_live_dryrun_audit.py",
                "--minutes",
                "52560000",
                "--log",
                str(journal),
            ],
        )
        mod.main()

        output = capsys.readouterr().out
        assert "Top WS BBO Selection Blockers:" in output
        assert "entry_ws_bbo_quote_lease_missing_quote: 1" in output
        assert "entry_blocked_ws_bbo_selection (1)" in output

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


# ===========================================================================
# Production blocker analyzer: windowed classification and fixture tests
# ===========================================================================


class TestProductionBlockerAnalyzer:
    """Tests for the production blocker analyzer with windowed classification."""

    @staticmethod
    def _fixture_path():
        return Path(__file__).parent / "fixtures" / "journals" / "production_entry_l2_pending_reconcile_20260517.jsonl"

    def test_analyzer_returns_all_windows(self):
        from scripts.analyze_production_blockers import analyze_event_file

        result = analyze_event_file(
            self._fixture_path(),
            now_ms=1778989200000,
            windows=["last_2h", "last_24h", "run_window"],
        )

        assert "windows" in result
        assert "last_2h" in result["windows"]
        assert "last_24h" in result["windows"]
        assert "run_window" in result["windows"]
        win_2h = result["windows"]["last_2h"]
        assert win_2h["entry_l2_blocker_counts"]["entry_local_l2_waiting_for_primary_tracking"] == 1
        assert win_2h["entry_l2_blocker_counts"]["entry_local_l2_waiting_for_dual_ready"] == 1

    def test_analyzer_classifies_blockers(self):
        from scripts.analyze_production_blockers import analyze_event_file

        result = analyze_event_file(
            self._fixture_path(),
            now_ms=1778989200000,
            windows=["last_2h", "last_24h", "run_window"],
        )

        classification = result["classification"]
        assert classification["entry_local_l2_waiting_for_primary_tracking"] == "current_new_high_frequency"
        assert classification["entry_local_l2_waiting_for_dual_ready"] == "current_new_high_frequency"

    def test_analyzer_classifies_legacy_ws_bbo_events_separately(self, tmp_path):
        from scripts.analyze_production_blockers import analyze_event_file

        journal = tmp_path / "ws_bbo_legacy.jsonl"
        records = [
            {
                "ts_ms": 1778985600000,
                "kind": "runtime.entry_blocked_local_l2_selection",
                "payload": {
                    "reason": "entry_ws_bbo_quote_lease_stale_quote",
                    "pair_id": "btcusdt:binance->bybit",
                    "symbol": "BTCUSDT",
                    "provider": "ws_bbo_quote_lease",
                    "readiness_evidence": {"provider": "ws_bbo_quote_lease"},
                },
            },
            {
                "ts_ms": 1778985601000,
                "kind": "runtime.entry_blocked_local_l2_selection",
                "payload": {
                    "reason": "entry_local_l2_waiting_for_dual_ready",
                    "pair_id": "ethusdt:binance->okx",
                    "symbol": "ETHUSDT",
                    "provider": "local_l2",
                },
            },
        ]
        journal.write_text(
            "\n".join(json.dumps(record) for record in records) + "\n"
        )

        result = analyze_event_file(
            journal,
            now_ms=1778989200000,
            windows=["last_2h"],
        )
        win_2h = result["windows"]["last_2h"]

        assert win_2h["entry_ws_bbo_blocker_counts"] == {
            "entry_ws_bbo_quote_lease_stale_quote": 1,
        }
        assert win_2h["entry_l2_blocker_counts"] == {
            "entry_local_l2_waiting_for_dual_ready": 1,
        }

    def test_analyzer_rebuckets_legacy_ws_bbo_and_admission_events(
        self,
        tmp_path,
    ):
        from scripts.analyze_production_blockers import analyze_event_file

        journal = tmp_path / "ws_bbo_admission_legacy.jsonl"
        records = [
            {
                "ts_ms": 1778985600000,
                "kind": "runtime.entry_blocked_local_l2_selection",
                "payload": {
                    "reason": "entry_ws_bbo_quote_lease_stale_quote",
                    "provider": "ws_bbo_quote_lease",
                    "symbol": "SEIUSDT",
                    "pair_id": "seiusdt:bybit->hyperliquid",
                },
            },
            {
                "ts_ms": 1778985600100,
                "kind": "runtime.entry_blocked_local_l2_selection",
                "payload": {
                    "reason": "insufficient_margin_admission_blocked",
                    "provider": "ws_bbo_quote_lease",
                    "source": "entry_admission",
                    "symbol": "SEIUSDT",
                    "pair_id": "seiusdt:bybit->hyperliquid",
                },
            },
            {
                "ts_ms": 1778985600200,
                "kind": "runtime.entry_blocked_local_l2_selection",
                "payload": {
                    "reason": "entry_local_l2_waiting_for_dual_ready",
                    "provider": "local_l2",
                    "symbol": "POLYXUSDT",
                    "pair_id": "polyxusdt:binance->bybit",
                },
            },
            {
                "ts_ms": 1778985600300,
                "kind": "scan.no_entry_diagnostics",
                "payload": {
                    "entry_ws_bbo_blocker_counts": {
                        "entry_ws_bbo_quote_lease_stale_quote": 1,
                    },
                    "entry_admission_blocker_counts": {
                        "insufficient_margin_admission_blocked": 1,
                    },
                    "entry_local_l2_primary_not_ready_reason_totals": {
                        "entry_local_l2_waiting_for_dual_ready": 1,
                    },
                },
            },
        ]
        journal.write_text(
            "\n".join(json.dumps(record) for record in records) + "\n"
        )

        result = analyze_event_file(
            journal,
            now_ms=1778985601000,
            windows=["run_window"],
        )
        window = result["windows"]["run_window"]

        assert window["entry_l2_blocker_counts"] == {
            "entry_local_l2_waiting_for_dual_ready": 1,
        }
        assert window["entry_ws_bbo_blocker_counts"] == {
            "entry_ws_bbo_quote_lease_stale_quote": 2,
        }
        assert window["entry_admission_blocker_counts"] == {
            "insufficient_margin_admission_blocked": 2,
        }
        assert (
            window["blocker_reason_counts"]["insufficient_margin_admission_blocked"]
            == 2
        )

    def test_analyzer_detects_min_notional_residual(self):
        from scripts.analyze_production_blockers import analyze_event_file

        result = analyze_event_file(
            self._fixture_path(),
            now_ms=1778989200000,
            windows=["last_2h", "last_24h", "run_window"],
        )

        win_2h = result["windows"]["last_2h"]
        pending = win_2h.get("pending_entry_counts", {})
        assert "pending_entry.hedge_submit_result:min_notional_rejected" in pending
        assert pending["pending_entry.hedge_submit_result:min_notional_rejected"] == 1

    def test_analyzer_has_classification_for_exchange_residual(self):
        from scripts.analyze_production_blockers import analyze_event_file

        result = analyze_event_file(
            self._fixture_path(),
            now_ms=1778989200000,
            windows=["last_2h", "last_24h", "run_window"],
        )

        classification = result["classification"]
        assert classification["pending_entry.hedge_submit_result:min_notional_rejected"] == "exchange_rule_residual"

    def test_production_blocker_analyzer_classifies_l2_and_pending_residuals(self):
        """Plan-specified test: verify the exact expected classification."""
        from scripts.analyze_production_blockers import analyze_event_file

        result = analyze_event_file(
            self._fixture_path(),
            now_ms=1778989200000,
        )

        assert result["windows"]["last_2h"]["entry_l2_blocker_counts"]["entry_local_l2_waiting_for_primary_tracking"] == 1
        assert result["windows"]["last_2h"]["entry_l2_blocker_counts"]["entry_local_l2_waiting_for_dual_ready"] == 1
        assert result["windows"]["last_2h"]["pending_entry_counts"]["pending_entry.hedge_submit_result:min_notional_rejected"] == 1
        assert result["classification"]["entry_local_l2_waiting_for_primary_tracking"] == "current_new_high_frequency"
        assert result["classification"]["entry_local_l2_waiting_for_dual_ready"] == "current_new_high_frequency"
        assert result["classification"]["pending_entry.hedge_submit_result:min_notional_rejected"] == "exchange_rule_residual"

    def test_code_side_view_filters_strategy_liquidity_and_oi_from_incident_window(self, tmp_path):
        from scripts.analyze_production_blockers import analyze_event_file

        journal = tmp_path / "code_side_blockers.jsonl"
        records = _code_side_blocker_incident_records()
        journal.write_text("\n".join(json.dumps(record) for record in records) + "\n")

        result = analyze_event_file(
            journal,
            now_ms=1781111910000,
            windows=["run_window"],
            exclude_strategy=True,
            exclude_liquidity=True,
        )

        view = result["windows"]["run_window"]["code_side_blocker_view"]
        assert view["filtered_out_counts"] == {
            "liquidity": 68,
            "open_interest": 76,
            "strategy": 85,
        }
        assert view["category_counts"] == {
            "code_data_freshness": 56,
            "exchange_truth_probe": 1,
            "order_truth_gap": 1,
            "ws_bbo_budget": 8,
        }
        assert view["reason_counts"] == {
            "accepted_order_truth_gap": 1,
            "bulk_position_probe_timeout": 1,
            "entry_ws_bbo_quote_lease_budget_exhausted": 8,
            "invalid_quote": 50,
            "last_good_sidecar_revalidate_required": 2,
            "quote_revalidate_unavailable": 1,
            "rest_invalid_quote": 3,
        }
        assert view["resolution_counts"] == {
            "last_good_revalidated": 1,
            "quote_truth_failed": 3,
            "quote_truth_must_resolve": 10,
            "quote_truth_resolved": 7,
            "quote_truth_rest_resolved": 5,
            "quote_truth_ws_resolved": 2,
            "quote_revalidate_failed": 1,
            "quote_revalidate_resolved": 1,
            "quote_revalidate_source:bybit_bbo_ws": 1,
        }

    def test_code_side_view_cli_flags_are_read_only_report_filters(self, tmp_path):
        journal = tmp_path / "code_side_blockers.jsonl"
        journal.write_text(
            "\n".join(json.dumps(record) for record in _code_side_blocker_incident_records())
            + "\n"
        )

        result = subprocess.run(
            [
                sys.executable,
                "scripts/analyze_production_blockers.py",
                "--events",
                str(journal),
                "--windows",
                "run_window",
                "--exclude-strategy",
                "--exclude-liquidity",
            ],
            check=True,
            text=True,
            capture_output=True,
        )
        report = json.loads(result.stdout)

        view = report["windows"]["run_window"]["code_side_blocker_view"]
        assert view["excluded_filters"] == ["strategy", "liquidity", "open_interest"]
        assert view["category_counts"]["code_data_freshness"] == 56
        assert view["resolution_counts"]["quote_revalidate_resolved"] == 1
        assert view["resolution_counts"]["quote_truth_rest_resolved"] == 5
        assert view["filtered_out_counts"] == {
            "liquidity": 68,
            "open_interest": 76,
            "strategy": 85,
        }

    def test_code_side_view_default_is_compatibility_only(self, tmp_path):
        from scripts.analyze_production_blockers import analyze_event_file

        journal = tmp_path / "code_side_blockers.jsonl"
        journal.write_text(
            "\n".join(json.dumps(record) for record in _code_side_blocker_incident_records())
            + "\n"
        )

        result = analyze_event_file(journal, windows=["run_window"])

        view = result["windows"]["run_window"]["code_side_blocker_view"]
        assert view["enabled"] is False
        assert view["excluded_filters"] == []
        assert view["category_counts"] == {}
        assert view["reason_counts"] == {}
        assert view["resolution_counts"] == {}
        assert view["oi_evidence_health_summary"] == {}
        assert view["filtered_out_counts"] == {}
        assert result["windows"]["run_window"]["entry_ws_bbo_blocker_counts"] == {
            "entry_ws_bbo_quote_lease_budget_exhausted": 8,
        }

    def test_code_side_view_does_not_double_count_total_and_breakdown_fields(self):
        from scripts.analyze_production_blockers import build_code_side_blocker_view

        view = build_code_side_blocker_view(
            [
                {
                    "kind": "scan.no_entry_diagnostics",
                    "payload": {
                        "strategy_blocker_counts": {
                            "funding_edge_below_floor": 3,
                        },
                        "strategy_blocked_count": 3,
                        "liquidity_blocker_counts": {
                            "depth_too_low": 4,
                        },
                        "liquidity_blocked_count": 4,
                        "open_interest_blocker_counts": {
                            "oi_below_floor": 5,
                        },
                        "open_interest_blocked_count": 5,
                    },
                },
            ],
            exclude_strategy=True,
            exclude_liquidity=True,
        )

        assert view["filtered_out_counts"] == {
            "liquidity": 4,
            "open_interest": 5,
            "strategy": 3,
        }

    def test_code_side_view_exposes_open_interest_evidence_status(self):
        from scripts.analyze_production_blockers import build_code_side_blocker_view

        view = build_code_side_blocker_view(
            [
                {
                    "kind": "execution.entry_liquidity_blocked",
                    "payload": {
                        "reason": "open_interest_unavailable",
                        "open_interest_evidence_status": "deferred_by_cap",
                        "open_interest_evidence_reason": "refresh_cap_exceeded",
                        "oi_cache_miss_count": 8,
                        "oi_refresh_attempt_count": 3,
                        "oi_deferred_count": 5,
                    },
                },
                {
                    "kind": "execution.entry_liquidity_blocked",
                    "payload": {
                        "reason": "open_interest_unavailable",
                        "open_interest_evidence_status": "rate_limited",
                        "open_interest_evidence_reason": "http_429",
                    },
                },
                {
                    "kind": "runtime.entry_oi_targeted_refresh_failed",
                    "payload": {
                        "open_interest_evidence_status": "timeout",
                        "open_interest_evidence_reason": "timeout_waiting_for_oi",
                        "elapsed_ms": 101,
                    },
                },
            ],
            enabled=True,
        )

        assert view["reason_counts"]["oi_evidence_status:deferred_by_cap"] == 1
        assert view["reason_counts"]["oi_evidence_status:rate_limited"] == 1
        assert view["reason_counts"]["oi_targeted_status:timeout"] == 1
        assert view["reason_counts"]["oi_evidence_reason:refresh_cap_exceeded"] == 1
        assert view["reason_counts"]["oi_evidence_reason:http_429"] == 1
        assert view["reason_counts"]["oi_targeted_reason:timeout_waiting_for_oi"] == 1
        assert view["oi_evidence_health_summary"] == {
            "oi_cache_miss_count": 8,
            "oi_deferred_count": 5,
            "oi_refresh_attempt_count": 3,
            "oi_targeted_failed_count": 1,
            "oi_targeted_max_elapsed_ms": 101,
        }

    def test_strategy_candidate_funnel_audit_groups_blockers_without_config_advice(self):
        from scripts.analyze_production_blockers import build_strategy_candidate_funnel_audit

        audit = build_strategy_candidate_funnel_audit([
            {
                "kind": "scan.no_entry_diagnostics",
                "payload": {
                    "top_quote_blocker_buckets": {"quote_stale": 3},
                    "open_interest_blocker_counts": {"oi_below_floor": 2},
                    "execution_liquidity_blocked_counts": {"insufficient_depth": 1},
                    "entry_admission_blocker_counts": {
                        "maker_fill_unit_truth_unavailable": 1,
                        "position_capacity_full": 1,
                        "entry_waiting_for_finalization_window_too_early": 1,
                    },
                },
            },
            {
                "kind": "execution.entry_liquidity_blocked",
                "payload": {
                    "reason": "open_interest_unavailable",
                    "open_interest_evidence_status": "deferred_by_cap",
                },
            },
            {
                "kind": "startup.trading_preflight",
                "payload": {
                    "venues": {
                        "hyperliquid": {
                            "status": "failed",
                            "reason": "account_wallet_signer_mismatch",
                        }
                    }
                },
            },
        ])

        assert audit["category_counts"] == {
            "admission": 1,
            "capacity": 1,
            "finalization_window": 1,
            "oi_liquidity": 4,
            "quote": 3,
            "venue_readiness": 1,
        }
        assert audit["top_reasons"][0]["reason"] == "quote_stale"
        rendered = json.dumps(audit, sort_keys=True)
        assert "lower" not in rendered
        assert "raise" not in rendered
        assert "config" not in rendered


def _code_side_blocker_incident_records():
    return [
            {
                "ts_ms": 1781111900000,
                "kind": "runtime.snapshot_freshness_decision",
                "payload": {
                    "symbol": "SAHARAUSDT",
                    "venue": "bybit",
                    "reason": "invalid_quote",
                    "invalid_quote_fields": ["bid", "ask"],
                },
            },
            {
                "ts_ms": 1781111900100,
                "kind": "scan.no_entry_diagnostics",
                "payload": {
                    "snapshot_freshness_blocked_counts": {"invalid_quote": 49},
                    "entry_ws_bbo_blocker_counts": {
                        "entry_ws_bbo_quote_lease_budget_exhausted": 8,
                    },
                    "quote_truth_must_resolve_count": 10,
                    "quote_truth_resolved_count": 7,
                    "quote_truth_failed_count": 3,
                    "quote_truth_ws_resolved_count": 2,
                    "quote_truth_rest_resolved_count": 5,
                    "budget_excluded_without_rest_count": 0,
                    "top_quote_blocker_buckets": {
                        "rest_invalid_quote": 3,
                    },
                    "strategy_blocker_counts": {
                        "funding_edge_below_floor": 80,
                    },
                    "open_interest_blocker_counts": {
                        "oi_below_floor": 70,
                    },
                    "liquidity_blocker_counts": {
                        "depth_too_low": 60,
                    },
                    "entry_admission_blocker_counts": {
                        "insufficient_margin_admission_prefiltered": 2,
                    },
                    "entry_admission_venue_degraded_samples": [
                        {
                            "venue": "hyperliquid",
                            "reason": "insufficient_margin_admission_prefiltered",
                            "available_balance_quote": 0.0,
                            "required_initial_margin_quote": 12.5,
                            "balance_classification": "unified_collateral_available",
                            "spot_usdc_available": 145.863168,
                            "user_abstraction": "unifiedAccount",
                        },
                        {
                            "venue": "hyperliquid",
                            "reason": "insufficient_margin_admission_prefiltered",
                            "available_balance_quote": 0.0,
                            "required_initial_margin_quote": 12.5,
                            "balance_classification": "unified_collateral_available",
                            "spot_usdc_available": 145.863168,
                            "user_abstraction": "unifiedAccount",
                        },
                    ],
                },
            },
            {
                "ts_ms": 1781111900150,
                "kind": "scan.no_entry_diagnostics",
                "payload": {
                    "blocked_reason_counts": {
                        "funding_window_passed": 5,
                        "perp_open_interest_below_floor": 6,
                        "execution_liquidity_depth_too_low": 7,
                    },
                    "execution_liquidity_blocked_counts": {},
                },
            },
            {
                "ts_ms": 1781111900200,
                "kind": "runtime.snapshot_fallback_last_good",
                "payload": {
                    "symbol": "CLUSDT",
                    "candidate_freshness_scope": [
                        {
                            "candidate_symbol": "CLUSDT",
                            "candidate_pair_id": "clus:okx->bybit",
                            "domain": "market_observed",
                            "venue": "global",
                            "blocked": True,
                            "block_reason": "last_good_sidecar",
                        },
                    ],
                },
            },
            {
                "ts_ms": 1781111900300,
                "kind": "runtime.live_scan_revalidate_required",
                "payload": {
                    "symbol": "CLUSDT",
                    "fallback_source": "last_good_sidecar",
                    "targeted_revalidate_required": True,
                },
            },
            {
                "ts_ms": 1781111900320,
                "kind": "runtime.entry_quote_revalidate_resolved",
                "payload": {
                    "venue": "okx",
                    "symbol": "CLUSDT",
                    "source": "bybit_bbo_ws",
                    "outcome": "resolved",
                },
            },
            {
                "ts_ms": 1781111900330,
                "kind": "runtime.last_good_revalidated_by_entry_quote_truth",
                "payload": {
                    "venue": "okx",
                    "symbol": "CLUSDT",
                    "source": "entry_quote_truth",
                },
            },
            {
                "ts_ms": 1781111900340,
                "kind": "runtime.entry_quote_revalidate_failed",
                "payload": {
                    "venue": "gate",
                    "symbol": "CLUSDT",
                    "outcome": "quote_revalidate_unavailable",
                },
            },
            {
                "ts_ms": 1781111900400,
                "kind": "recovery.live_position_bulk_diagnostic_error",
                "payload": {
                    "venue": "okx",
                    "classification": "timeout",
                    "diagnostic_scope": "best_effort_bulk_positions",
                    "truth_required_by": [],
                    "blocking": False,
                },
            },
            {
                "ts_ms": 1781111900500,
                "kind": "exit.passive_close_hedge_ack_pending_reconcile",
                "payload": {
                    "venue": "bybit",
                    "symbol": "KATUSDT",
                    "accepted_order_truth_gap": True,
                    "accepted_order_id": "oid-1",
                    "accepted_client_order_id": "cid-1",
                },
            },
            {
                "ts_ms": 1781111900600,
                "kind": "execution.entry_liquidity_blocked",
                "payload": {"reason": "depth_too_low"},
            },
    ]
