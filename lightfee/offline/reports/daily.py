"""Daily snapshot report generation."""

from __future__ import annotations

import time
from pathlib import Path

from lightfee.offline.analysis.journal import analyze_journal_records
from lightfee.persistence.journal import Journal
from lightfee.persistence.sqlite_store import SqliteStore


def generate_daily_snapshot(
    journal_path: str | Path,
    sqlite_path: str | Path,
    date: str,
) -> dict:
    """Generate daily snapshot from journal and write to SQLite.

    Returns a summary dict suitable for rendering.
    """
    journal = Journal(journal_path)
    records = journal.read_all()

    report = analyze_journal_records(records)
    report.daily.date = date

    store = SqliteStore(sqlite_path)
    conn = store.open()
    now_ms = int(time.time() * 1000)

    for venue in report.venue_stats:
        store.insert_daily_snapshot(
            conn,
            date=date,
            venue=venue,
            symbol="ALL",
            total_pnl_quote=report.daily.total_pnl_quote,
            total_fee_quote=report.daily.total_fee_quote,
            entry_count=report.daily.entry_count,
            exit_count=report.daily.exit_count,
            created_at_ms=now_ms,
        )

    conn.close()

    return {
        "date": date,
        "total_pnl_quote": report.daily.total_pnl_quote,
        "total_fee_quote": report.daily.total_fee_quote,
        "entry_count": report.daily.entry_count,
        "exit_count": report.daily.exit_count,
        "venue_stats": {
            v: {
                "order_count": s.order_count,
                "fill_count": s.fill_count,
                "failure_count": s.failure_count,
                "total_fee_quote": s.total_fee_quote,
            }
            for v, s in report.venue_stats.items()
        },
        "recovery_counts": dict(report.recovery_counts),
        "risk_counts": dict(report.risk_counts),
        "scan_no_entry_diagnostics": report.scan_no_entry_diagnostics_count,
        "scan_runtime_gate_blocked": report.scan_runtime_gate_blocked_count,
        "execution_liquidity_blocked": report.execution_liquidity_blocked_count,
        "local_l2_sequence_gap_count": report.local_l2_sequence_gap_count,
        "local_l2_sync_failed_count": report.local_l2_sync_failed_count,
        "local_l2_sequence_gap_by_reason": dict(report.local_l2_sequence_gap_by_reason),
        "local_l2_sync_failed_by_category": dict(report.local_l2_sync_failed_by_category),
        "entry_liquidity_blocked_by_reason": dict(report.entry_liquidity_blocked_by_reason),
        "fail_closed_reason_counts": dict(report.fail_closed_reason_counts),
    }
