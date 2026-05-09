"""Daily snapshot report generation."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

from lightfee.offline.analysis.journal import analyze_journal_records
from lightfee.persistence.journal import Journal
from lightfee.persistence.sqlite_store import SqliteStore


def generate_daily_snapshot(
    journal_path: str | Path,
    sqlite_path: str | Path,
    date: str,
) -> None:
    """Generate daily snapshot from journal and write to SQLite."""
    journal = Journal(journal_path)
    records = journal.read_all()

    venue_stats, daily = analyze_journal_records(records)
    daily.date = date

    store = SqliteStore(sqlite_path)
    conn = store.open()
    now_ms = int(time.time() * 1000)

    for venue, stats in venue_stats.items():
        for symbol in set():  # symbols would come from position data
            store.insert_daily_snapshot(
                conn,
                date=date,
                venue=venue,
                symbol=symbol or "ALL",
                total_pnl_quote=daily.total_pnl_quote,
                total_fee_quote=daily.total_fee_quote,
                entry_count=daily.entry_count,
                exit_count=daily.exit_count,
                created_at_ms=now_ms,
            )

    conn.close()
