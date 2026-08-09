"""Semantic parity tests for journal analysis against V1 business contract ANAL-001.

Verifies that V2 journal analysis counts match V1 semantics:
order lifecycle, entry/exit PnL, recovery, risk, scan diagnostics,
execution liquidity blocks, local-L2 sequence gaps, local-L2 sync failures,
fail-closed reasons, and classification breakdowns.
"""

import json
from pathlib import Path

from lightfee.offline.analysis.journal import (
    JournalAnalysisReport,
    analyze_journal_records,
    analyze_from_store,
    analyze_journal_or_store,
    summarize_quick_flat_events,
    VenueOrderStats,
    DailyPnLSummary,
)


def make_record(kind: str, payload: dict | None = None) -> dict:
    return {"kind": kind, "payload": payload or {}}


# ── Order Lifecycle ─────────────────────────────────────────────────────────


class TestOrderLifecycleCounts:
    def test_submitted_order_increments_venue_order_count(self):
        records = [
            make_record("order.submitted", {"venue": "binance", "symbol": "BTCUSDT"}),
        ]
        report = analyze_journal_records(records)
        assert report.venue_stats["binance"].order_count == 1

    def test_filled_order_increments_fill_and_latency(self):
        records = [
            make_record("order.filled", {
                "venue": "binance", "latency_ms": 150, "fee_quote": 0.01
            }),
        ]
        report = analyze_journal_records(records)
        stats = report.venue_stats["binance"]
        assert stats.fill_count == 1
        assert stats.total_latency_ms == 150
        assert stats.max_latency_ms == 150
        assert stats.min_latency_ms == 150
        assert stats.total_fee_quote == 0.01

    def test_rejected_order_increments_failure(self):
        records = [
            make_record("order.rejected", {"venue": "bybit", "symbol": "ETHUSDT"}),
        ]
        report = analyze_journal_records(records)
        assert report.venue_stats["bybit"].failure_count == 1
        assert report.venue_stats["bybit"].fill_count == 0

    def test_uncertain_order_increments_failure(self):
        records = [
            make_record("order.uncertain", {"venue": "okx", "symbol": "SOLUSDT"}),
        ]
        report = analyze_journal_records(records)
        assert report.venue_stats["okx"].failure_count == 1

    def test_venue_stats_aggregate_across_multiple_orders(self):
        records = [
            make_record("order.submitted", {"venue": "binance"}),
            make_record("order.filled", {"venue": "binance", "latency_ms": 100, "fee_quote": 0.0}),
            make_record("order.filled", {"venue": "binance", "latency_ms": 300, "fee_quote": 0.02}),
            make_record("order.rejected", {"venue": "binance"}),
        ]
        report = analyze_journal_records(records)
        stats = report.venue_stats["binance"]
        assert stats.order_count == 1
        assert stats.fill_count == 2
        assert stats.failure_count == 1
        assert stats.min_latency_ms == 100
        assert stats.max_latency_ms == 300


# ── Entry/Exit PnL ──────────────────────────────────────────────────────────


class TestEntryExitPnL:
    def test_entry_opened_increments_entry_count_and_fee(self):
        records = [
            make_record("entry.opened", {"entry_fee_quote": 0.5}),
        ]
        report = analyze_journal_records(records)
        assert report.daily.entry_count == 1
        assert report.daily.total_fee_quote == 0.5
        assert report.daily.exit_count == 0

    def test_exit_closed_increments_pnl_and_fee(self):
        records = [
            make_record("exit.closed", {"net_quote": 12.5, "exit_fee_quote": 0.3}),
        ]
        report = analyze_journal_records(records)
        assert report.daily.exit_count == 1
        assert report.daily.total_pnl_quote == 12.5
        assert report.daily.total_fee_quote == 0.3
        assert report.daily.entry_count == 0

    def test_provisional_billing_terminal_never_realizes_pnl(self):
        records = [
            make_record(
                "exit.billing_evidence_unavailable",
                {"net_quote": 12.5, "exit_fee_quote": 0.3},
            ),
        ]

        report = analyze_journal_records(records)

        assert report.daily.exit_count == 0
        assert report.daily.total_pnl_quote == 0.0
        assert report.daily.total_fee_quote == 0.0


# ── Quick-flat observability ───────────────────────────────────────────────


class TestQuickFlatObservability:
    def test_home_recovery_flat_terminal_chain_counts_as_quick_flat(self):
        records = [
            json.loads(line)
            for line in Path(
                "tests/fixtures/live_incidents/2026-06-13/"
                "homeusdt_recovery_quick_flat_chain.jsonl"
            ).read_text().splitlines()
            if line.strip()
        ]

        summary = summarize_quick_flat_events(
            records,
            quick_flat_window_ms=60_000,
        )

        assert summary["quick_flat_count"] == 2
        assert summary["quick_flat_terminal_kind_counts"] == {
            "recovery.flat": 1,
            "runtime.position_lifecycle_terminal": 1,
        }

    def test_quick_flat_close_count_deduplicates_double_exit_closed_projection(self):
        records = [
            {
                "ts_ms": 1000,
                "kind": "entry.opened",
                "payload": {"position_id": "p1", "symbol": "BTCUSDT"},
            },
            {
                "ts_ms": 1500,
                "kind": "exit.closed",
                "payload": {
                    "position_id": "p1",
                    "reason": "funding_capture",
                    "close_id": "c1",
                },
            },
            {
                "ts_ms": 1500,
                "kind": "exit.closed",
                "payload": {
                    "position_id": "p1",
                    "reason": "funding_capture",
                    "close_id": "c1",
                },
            },
        ]

        summary = summarize_quick_flat_events(
            records,
            quick_flat_window_ms=60_000,
        )

        assert summary["quick_flat_count"] == 1
        assert summary["duplicate_event_count"] == 1

    def test_exit_reconciled_is_a_quick_flat_even_when_its_billing_is_historical_unverified(self):
        records = [
            {
                "ts_ms": 1000,
                "kind": "entry.opened",
                "payload": {"position_id": "p1", "symbol": "BTCUSDT"},
            },
            {
                "ts_ms": 1500,
                "kind": "exit.reconciled",
                "payload": {
                    "position_id": "p1",
                    "reason": "funding_capture",
                    "venue_statement_reconciled": False,
                },
            },
        ]

        summary = summarize_quick_flat_events(records, quick_flat_window_ms=60_000)

        assert summary["quick_flat_count"] == 1
        assert summary["quick_flat_terminal_kind_counts"] == {"exit.reconciled": 1}
        assert summary["quick_flat_unreconciled_billing_count"] == 1

    def test_billing_unreconciled_is_counted_without_becoming_a_terminal_close(self):
        records = [
            {
                "ts_ms": 1000,
                "kind": "entry.opened",
                "payload": {"position_id": "p1", "symbol": "BTCUSDT"},
            },
            {
                "ts_ms": 1500,
                "kind": "exit.billing_unreconciled",
                "payload": {"position_id": "p1"},
            },
            {
                "ts_ms": 2500,
                "kind": "exit.billing_unreconciled",
                "payload": {"position_id": "p1"},
            },
        ]

        summary = summarize_quick_flat_events(records, quick_flat_window_ms=60_000)

        assert summary["quick_flat_count"] == 0
        assert summary["quick_flat_unreconciled_billing_count"] == 1

    def test_billing_evidence_debt_is_counted_without_becoming_a_terminal_close(self):
        records = [
            {
                "ts_ms": 1000,
                "kind": "entry.opened",
                "payload": {"position_id": "p1", "symbol": "BTCUSDT"},
            },
            {
                "ts_ms": 1500,
                "kind": "exit.billing_evidence_debt_registered",
                "payload": {"position_id": "p1"},
            },
            {
                "ts_ms": 2500,
                "kind": "exit.billing_evidence_debt_registered",
                "payload": {"position_id": "p1"},
            },
        ]

        summary = summarize_quick_flat_events(records, quick_flat_window_ms=60_000)

        assert summary["quick_flat_count"] == 0
        assert summary["quick_flat_unreconciled_billing_count"] == 1

    def test_terminal_billing_evidence_gap_is_physical_terminal_but_not_financially_reconciled(self):
        records = [
            {
                "ts_ms": 1000,
                "kind": "entry.opened",
                "payload": {"position_id": "p1", "symbol": "BTCUSDT"},
            },
            {
                "ts_ms": 1500,
                "kind": "exit.billing_evidence_unavailable",
                "payload": {
                    "position_id": "p1",
                    "terminal_accounting_status": "provisional_entry_fee_evidence_unavailable",
                },
            },
        ]

        summary = summarize_quick_flat_events(records, quick_flat_window_ms=60_000)

        assert summary["quick_flat_count"] == 1
        assert summary["quick_flat_terminal_kind_counts"] == {
            "exit.billing_evidence_unavailable": 1,
        }
        assert summary["quick_flat_unreconciled_billing_count"] == 1

    def test_journal_report_exposes_deduplicated_quick_flat_counts(self):
        records = [
            {
                "ts_ms": 1000,
                "kind": "entry.opened",
                "payload": {"position_id": "p1", "symbol": "BTCUSDT"},
            },
            {
                "ts_ms": 1500,
                "kind": "exit.closed",
                "payload": {
                    "position_id": "p1",
                    "reason": "funding_capture",
                    "close_id": "c1",
                },
            },
            {
                "ts_ms": 1500,
                "kind": "exit.closed",
                "payload": {
                    "position_id": "p1",
                    "reason": "funding_capture",
                    "close_id": "c1",
                },
            },
        ]

        report = analyze_journal_records(records)

        assert report.quick_flat_count == 1
        assert report.quick_flat_duplicate_event_count == 1

    def test_quick_flat_close_without_close_id_uses_lower_confidence_key(self):
        records = [
            {
                "ts_ms": 1000,
                "kind": "entry.opened",
                "payload": {"position_id": "p1", "symbol": "BTCUSDT"},
            },
            {
                "ts_ms": 1500,
                "kind": "exit.closed",
                "payload": {"position_id": "p1", "reason": "funding_capture"},
            },
            {
                "ts_ms": 1500,
                "kind": "exit.closed",
                "payload": {"position_id": "p1", "reason": "funding_capture"},
            },
        ]

        summary = summarize_quick_flat_events(
            records,
            quick_flat_window_ms=60_000,
        )

        assert summary["quick_flat_count"] == 1
        assert summary["duplicate_event_count"] == 1
        assert summary["close_identity_confidence"] == "lower"


# ── Recovery Evidence ──────────────────────────────────────────────────────


class TestRecoveryCounts:
    def test_recovery_kinds_are_counted(self):
        records = [
            make_record("recovery.live_detected", {}),
            make_record("recovery.blocked", {}),
            make_record("recovery.resumed", {}),
        ]
        report = analyze_journal_records(records)
        assert report.recovery_counts.get("recovery.live_detected") == 1
        assert report.recovery_counts.get("recovery.blocked") == 1
        assert report.recovery_counts.get("recovery.resumed") == 1

    def test_recovery_flat_counted(self):
        records = [
            make_record("recovery.flat", {}),
            make_record("recovery.mismatch_detected", {}),
            make_record("recovery.mismatch_flattened", {}),
        ]
        report = analyze_journal_records(records)
        assert report.recovery_counts.get("recovery.flat") == 1
        assert report.recovery_counts.get("recovery.mismatch_detected") == 1
        assert report.recovery_counts.get("recovery.mismatch_flattened") == 1


# ── Risk Triggers ──────────────────────────────────────────────────────────


class TestRiskCounts:
    def test_risk_kinds_are_counted(self):
        records = [
            make_record("risk.warning_triggered", {}),
            make_record("risk.warning_cleared", {}),
            make_record("risk.death_triggered", {}),
            make_record("risk.single_side_protection_triggered", {}),
            make_record("risk.single_side_protection_failed", {}),
            make_record("risk.single_side_protection_unavailable", {}),
        ]
        report = analyze_journal_records(records)
        assert report.risk_counts.get("risk.warning_triggered") == 1
        assert report.risk_counts.get("risk.warning_cleared") == 1
        assert report.risk_counts.get("risk.death_triggered") == 1
        assert report.risk_counts.get("risk.single_side_protection_triggered") == 1
        assert report.risk_counts.get("risk.single_side_protection_failed") == 1
        assert report.risk_counts.get("risk.single_side_protection_unavailable") == 1


# ── Scan Diagnostics ────────────────────────────────────────────────────────


class TestScanDiagnostics:
    def test_scan_no_entry_diagnostics_counted(self):
        records = [
            make_record("scan.no_entry_diagnostics", {}),
            make_record("scan.no_entry_diagnostics", {}),
            make_record("scan.no_entry_diagnostics", {}),
        ]
        report = analyze_journal_records(records)
        assert report.scan_no_entry_diagnostics_count == 3

    def test_scan_runtime_gate_blocked_counted(self):
        records = [
            make_record("scan.runtime_gate_blocked", {}),
            make_record("scan.runtime_gate_blocked", {}),
        ]
        report = analyze_journal_records(records)
        assert report.scan_runtime_gate_blocked_count == 2


# ── Execution Diagnostics ──────────────────────────────────────────────────


class TestExecutionDiagnostics:
    def test_entry_liquidity_blocked_counted(self):
        records = [
            make_record("execution.entry_liquidity_blocked", {
                "reason": "insufficient_depth",
                "eligibility_class": "passive",
            }),
        ]
        report = analyze_journal_records(records)
        assert report.execution_liquidity_blocked_count == 1
        assert report.entry_liquidity_blocked_by_reason["insufficient_depth"] == 1
        assert report.execution_liquidity_blocked_by_class["passive"] == 1

    def test_entry_liquidity_blocked_by_reason_aggregates(self):
        records = [
            make_record("execution.entry_liquidity_blocked", {"reason": "spread_too_wide"}),
            make_record("execution.entry_liquidity_blocked", {"reason": "spread_too_wide"}),
            make_record("execution.entry_liquidity_blocked", {"reason": "no_quotes"}),
        ]
        report = analyze_journal_records(records)
        assert report.entry_liquidity_blocked_by_reason["spread_too_wide"] == 2
        assert report.entry_liquidity_blocked_by_reason["no_quotes"] == 1

    def test_entry_liquidity_blocked_by_class(self):
        records = [
            make_record("execution.entry_liquidity_blocked", {
                "reason": "thin_book", "eligibility_class": "aggressive",
            }),
            make_record("execution.entry_liquidity_blocked", {
                "reason": "thin_book", "eligibility_class": "aggressive",
            }),
        ]
        report = analyze_journal_records(records)
        assert report.execution_liquidity_blocked_by_class["aggressive"] == 2

    def test_entry_liquidity_blocked_by_open_interest_evidence_status(self):
        records = [
            make_record(
                "execution.entry_liquidity_blocked",
                {
                    "reason": "open_interest_unavailable",
                    "open_interest_evidence_status": "deferred_by_cap",
                },
            ),
            make_record(
                "execution.entry_liquidity_blocked",
                {
                    "reason": "open_interest_unavailable",
                    "open_interest_evidence_status": "rate_limited",
                },
            ),
            make_record(
                "execution.entry_liquidity_blocked",
                {
                    "reason": "open_interest_unavailable",
                    "open_interest_evidence_status": "rate_limited",
                },
            ),
        ]
        report = analyze_journal_records(records)
        assert report.entry_liquidity_blocked_by_open_interest_evidence_status == {
            "deferred_by_cap": 1,
            "rate_limited": 2,
        }


# ── Local-L2 Health ─────────────────────────────────────────────────────────


class TestLocalL2Health:
    def test_local_l2_sequence_gap_counted(self):
        records = [
            make_record("runtime.local_l2_sequence_gap", {
                "continuity_reason": "ws_disconnect",
            }),
        ]
        report = analyze_journal_records(records)
        assert report.local_l2_sequence_gap_count == 1
        assert report.local_l2_sequence_gap_by_reason["ws_disconnect"] == 1

    def test_local_l2_sync_failed_counted(self):
        records = [
            make_record("runtime.local_l2_sync_failed", {
                "failure_category": "checksum_mismatch",
            }),
        ]
        report = analyze_journal_records(records)
        assert report.local_l2_sync_failed_count == 1
        assert report.local_l2_sync_failed_by_category["checksum_mismatch"] == 1

    def test_local_l2_sequence_gap_by_reason_aggregates(self):
        records = [
            make_record("runtime.local_l2_sequence_gap", {"continuity_reason": "ws_disconnect"}),
            make_record("runtime.local_l2_sequence_gap", {"continuity_reason": "ws_disconnect"}),
            make_record("runtime.local_l2_sequence_gap", {"continuity_reason": "gap_too_large"}),
        ]
        report = analyze_journal_records(records)
        assert report.local_l2_sequence_gap_by_reason["ws_disconnect"] == 2
        assert report.local_l2_sequence_gap_by_reason["gap_too_large"] == 1


# ── Fail-Closed Reasons ────────────────────────────────────────────────────


class TestFailClosedReasons:
    def test_fail_closed_reason_counted(self):
        records = [
            make_record("runtime.fail_closed", {"reason": "venue_disconnected"}),
            make_record("runtime.fail_closed", {"reason": "venue_disconnected"}),
            make_record("runtime.fail_closed", {"reason": "margin_insufficient"}),
        ]
        report = analyze_journal_records(records)
        assert report.fail_closed_reason_counts["venue_disconnected"] == 2
        assert report.fail_closed_reason_counts["margin_insufficient"] == 1

    def test_fail_closed_subkind_also_counted(self):
        records = [
            make_record("runtime.fail_closed.venue_error", {"reason": "timeout"}),
        ]
        report = analyze_journal_records(records)
        assert report.fail_closed_reason_counts["timeout"] == 1


# ── Analysis mode selection ────────────────────────────────────────────────


class TestAnalyzeJournalOrStore:
    def test_analyze_journal_or_store_without_conn_uses_records(self):
        records = [
            make_record("order.submitted", {"venue": "binance"}),
        ]
        report = analyze_journal_or_store(None, records)
        assert report.venue_stats["binance"].order_count == 1

    def test_analyze_journal_or_store_empty_returns_default(self):
        report = analyze_journal_or_store(None, None)
        assert report.total_records == 0
        assert len(report.venue_stats) == 0

    def test_total_records_tracks_count(self):
        records = [make_record("entry.opened", {}) for _ in range(5)]
        report = analyze_journal_records(records)
        assert report.total_records == 5


# ── Report field completeness (V1 semantic coverage) ───────────────────────


class TestReportFieldCompleteness:
    """Verify that JournalAnalysisReport exposes all V1-visible fields."""

    def test_report_has_all_v1_top_level_fields(self):
        report = JournalAnalysisReport()
        # Core V1 fields
        assert hasattr(report, "total_records")
        assert hasattr(report, "venue_stats")
        assert hasattr(report, "daily")
        assert hasattr(report, "recovery_counts")
        assert hasattr(report, "risk_counts")
        assert hasattr(report, "scan_no_entry_diagnostics_count")
        assert hasattr(report, "scan_runtime_gate_blocked_count")
        assert hasattr(report, "execution_liquidity_blocked_count")
        assert hasattr(report, "local_l2_sequence_gap_count")
        assert hasattr(report, "local_l2_sync_failed_count")
        assert hasattr(report, "local_l2_sequence_gap_by_reason")
        assert hasattr(report, "local_l2_sync_failed_by_category")
        assert hasattr(report, "entry_liquidity_blocked_by_reason")
        assert hasattr(report, "execution_liquidity_blocked_by_class")
        assert hasattr(report, "fail_closed_reason_counts")

    def test_venue_stats_has_all_v1_fields(self):
        stats = VenueOrderStats(venue="test")
        assert hasattr(stats, "venue")
        assert hasattr(stats, "order_count")
        assert hasattr(stats, "fill_count")
        assert hasattr(stats, "failure_count")
        assert hasattr(stats, "total_latency_ms")
        assert hasattr(stats, "max_latency_ms")
        assert hasattr(stats, "min_latency_ms")
        assert hasattr(stats, "total_fee_quote")

    def test_daily_pnl_summary_has_all_v1_fields(self):
        daily = DailyPnLSummary()
        assert hasattr(daily, "date")
        assert hasattr(daily, "total_pnl_quote")
        assert hasattr(daily, "total_fee_quote")
        assert hasattr(daily, "entry_count")
        assert hasattr(daily, "exit_count")
        assert hasattr(daily, "by_venue")
        assert hasattr(daily, "by_symbol")
