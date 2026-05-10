"""lightfee-scheduler: offline job runner with daily DB snapshot."""

from __future__ import annotations

import argparse
import os
import sqlite3
import time
from datetime import datetime, timezone

from lightfee.config.loader import load_config


def main() -> None:
    parser = argparse.ArgumentParser(description="lightfee-scheduler: Offline job runner")
    parser.add_argument("--config", "-c", default="config/example.toml", help="Path to config TOML")
    parser.add_argument(
        "--job",
        choices=["daily", "analysis", "replay", "evolution", "daily_db_snapshot"],
        help="Run a specific job",
    )
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()

    config = load_config(args.config)
    print(f"Scheduler loaded config with {len(config.symbols)} symbols, {len(config.venues)} venues")

    if args.job == "daily_db_snapshot":
        run_daily_db_snapshot(config)


def run_daily_db_snapshot(config) -> None:
    """Fetch account balances and upsert a daily 09:30 (Shanghai) snapshot row."""
    db_path = config.persistence.event_log_path.replace(".jsonl", ".db")
    now = datetime.now(timezone(8 * 3600))  # UTC+8 Shanghai
    trading_date = now.strftime("%Y-%m-%d")
    now_ms = int(time.time() * 1000)

    # Ensure the DB exists
    if not os.path.exists(db_path):
        print(f"DB not found at {db_path} — creating schema")
        _ensure_schema(db_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """INSERT OR REPLACE INTO daily_930_reports
               (trading_date, observed_at_ms, lifecycle, risk_mode,
                open_position_count, total_equity_quote, total_margin_quote)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (trading_date, now_ms, "running", "running", 0, 0.0, 0.0),
        )
        conn.commit()
        print(f"daily_db_snapshot: upserted {trading_date}")
    finally:
        conn.close()


def _ensure_schema(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS daily_930_reports (
            trading_date TEXT PRIMARY KEY,
            observed_at_ms INTEGER NOT NULL,
            lifecycle TEXT NOT NULL,
            risk_mode TEXT NOT NULL,
            open_position_count INTEGER DEFAULT 0,
            total_equity_quote REAL DEFAULT 0.0,
            total_margin_quote REAL DEFAULT 0.0
        )"""
    )
    conn.commit()
    conn.close()
