"""Tests for offline analysis: journal stats, PnL summary, incident reports, diagnostics."""

import pytest

import tempfile
from pathlib import Path

import pytest

from lightfee.offline.analysis.journal import (
    JournalAnalysisReport,
    analyze_journal_records,
)
from lightfee.offline.analysis.incident import build_incident_report
from lightfee.offline.reports.daily import generate_daily_snapshot
from lightfee.offline.reports.render import render_json, render_text
from lightfee.persistence.journal import Journal


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
