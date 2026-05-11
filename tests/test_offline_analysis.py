"""Tests for offline analysis: journal stats, PnL summary, incident reports."""

import pytest

from lightfee.offline.analysis.journal import analyze_journal_records
from lightfee.offline.analysis.incident import build_incident_report
from lightfee.offline.reports.render import render_json, render_text


class TestJournalAnalysis:
    def test_analyzes_entry_and_exit(self):
        records = [
            {"kind": "entry.opened", "payload": {"entry_fee_quote": 5.0, "symbol": "BTCUSDT"}},
            {"kind": "exit.closed", "payload": {"net_quote": 50.0, "exit_fee_quote": 3.0}},
            {"kind": "order.submitted", "payload": {"venue": "binance"}},
            {"kind": "order.filled", "payload": {"venue": "binance", "latency_ms": 150}},
            {"kind": "order.rejected", "payload": {"venue": "binance"}},
        ]
        venue_stats, daily = analyze_journal_records(records)
        assert daily.entry_count == 1
        assert daily.exit_count == 1
        assert daily.total_pnl_quote == 50.0
        assert daily.total_fee_quote == 8.0  # 5.0 entry + 3.0 exit

        binance = venue_stats["binance"]
        assert binance.order_count == 1
        assert binance.fill_count == 1
        assert binance.failure_count == 1
        assert binance.max_latency_ms == 150
        assert binance.min_latency_ms == 150


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
