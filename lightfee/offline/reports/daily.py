"""Daily snapshot report generation.

Read path: structured store first, journal fallback second.
After a journal fallback read, backfill the projection store so the next
read can use the fast path.
"""

from __future__ import annotations

import time
from pathlib import Path

from lightfee.offline.analysis.journal import (
    analyze_from_store,
    analyze_journal_records,
)
from lightfee.persistence.journal import Journal
from lightfee.persistence.projection_writer import ProjectionWriter
from lightfee.persistence.sqlite_store import SqliteStore


def generate_daily_snapshot(
    journal_path: str | Path,
    sqlite_path: str | Path,
    date: str,
) -> dict:
    """Generate daily snapshot preferring structured store, falling back to journal.

    Returns a summary dict suitable for rendering.
    """
    journal = Journal(journal_path)
    store = SqliteStore(sqlite_path)
    conn = store.open()
    now_ms = int(time.time() * 1000)

    # Prefer structured store when projection data exists
    if store.has_projection_data(conn):
        report = analyze_from_store(conn)

    # Fall back to journal scan when store has no data or returned empty
    if not store.has_projection_data(conn) or _is_empty_report(report):
        records = journal.read_all()
        report = analyze_journal_records(records)
        # Backfill: project records into store for future fast reads
        _backfill_projection(store, conn, records)

    report.daily.date = date

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
        "entry_liquidity_blocked_by_open_interest_evidence_status": dict(
            report.entry_liquidity_blocked_by_open_interest_evidence_status
        ),
        "fail_closed_reason_counts": dict(report.fail_closed_reason_counts),
        "exit_shadow_decision_count": report.exit_shadow_decision_count,
        "exit_shadow_path_markout_count": report.exit_shadow_path_markout_count,
        "exit_shadow_summary_count": report.exit_shadow_summary_count,
        "exit_shadow_by_bot": dict(report.exit_shadow_by_bot),
    }


def _backfill_projection(
    store: SqliteStore,
    conn: object,
    records: list[dict],
) -> dict[str, int]:
    """Project journal records into the store so future reads use the fast path."""
    writer = ProjectionWriter(store)
    return writer.project_records(conn, records)


def _is_empty_report(report: object) -> bool:
    """Check whether a report has no data at all (needs journal fallback)."""
    return (
        report.total_records == 0
        and report.daily.entry_count == 0
        and report.daily.exit_count == 0
        and not report.venue_stats
        and not report.recovery_counts
        and not report.risk_counts
    )
