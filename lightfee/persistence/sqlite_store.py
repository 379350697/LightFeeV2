"""SQLite store for structured facts, daily snapshots, and ledgers."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS daily_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    total_pnl_quote REAL NOT NULL DEFAULT 0,
    total_fee_quote REAL NOT NULL DEFAULT 0,
    entry_count INTEGER NOT NULL DEFAULT 0,
    exit_count INTEGER NOT NULL DEFAULT 0,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS scan_facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms INTEGER NOT NULL,
    venue TEXT NOT NULL,
    symbol_count INTEGER NOT NULL DEFAULT 0,
    latency_ms INTEGER NOT NULL DEFAULT 0,
    degraded INTEGER NOT NULL DEFAULT 0,
    error TEXT
);

CREATE TABLE IF NOT EXISTS proposal_catalog (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposal_id TEXT NOT NULL UNIQUE,
    version INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'proposed',
    title TEXT NOT NULL,
    body_json TEXT NOT NULL,
    proposed_at_ms INTEGER NOT NULL,
    approved_at_ms INTEGER,
    rejected_at_ms INTEGER
);

CREATE TABLE IF NOT EXISTS approval_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposal_id TEXT NOT NULL,
    reviewer TEXT NOT NULL,
    decision TEXT,
    notes TEXT,
    decided_at_ms INTEGER,
    FOREIGN KEY (proposal_id) REFERENCES proposal_catalog(proposal_id)
);

CREATE TABLE IF NOT EXISTS experiment_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposal_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    outcome_json TEXT NOT NULL,
    started_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER,
    FOREIGN KEY (proposal_id) REFERENCES proposal_catalog(proposal_id)
);

CREATE TABLE IF NOT EXISTS operator_commands (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms INTEGER NOT NULL,
    command TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'manual',
    applied INTEGER NOT NULL DEFAULT 0
);
"""


class SqliteStore:
    """SQLite persistence for structured facts and ledgers."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def open(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.path))
        conn.executescript(SCHEMA_SQL)
        conn.commit()
        return conn

    def insert_daily_snapshot(
        self,
        conn: sqlite3.Connection,
        date: str,
        venue: str,
        symbol: str,
        total_pnl_quote: float,
        total_fee_quote: float,
        entry_count: int,
        exit_count: int,
        created_at_ms: int,
    ) -> None:
        conn.execute(
            "INSERT INTO daily_snapshots (date, venue, symbol, total_pnl_quote, "
            "total_fee_quote, entry_count, exit_count, created_at_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (date, venue, symbol, total_pnl_quote, total_fee_quote, entry_count, exit_count, created_at_ms),
        )
        conn.commit()

    def insert_scan_fact(
        self,
        conn: sqlite3.Connection,
        ts_ms: int,
        venue: str,
        symbol_count: int,
        latency_ms: int,
        degraded: int = 0,
        error: str | None = None,
    ) -> None:
        conn.execute(
            "INSERT INTO scan_facts (ts_ms, venue, symbol_count, latency_ms, degraded, error) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (ts_ms, venue, symbol_count, latency_ms, degraded, error),
        )
        conn.commit()

    def insert_operator_command(
        self, conn: sqlite3.Connection, ts_ms: int, command: str, source: str = "manual"
    ) -> None:
        conn.execute(
            "INSERT INTO operator_commands (ts_ms, command, source) VALUES (?, ?, ?)",
            (ts_ms, command, source),
        )
        conn.commit()
