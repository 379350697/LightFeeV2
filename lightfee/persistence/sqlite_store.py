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

-- Projection fact tables (Tier 2: queryable facts)

CREATE TABLE IF NOT EXISTS projected_facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    seq INTEGER NOT NULL,
    ts_ms INTEGER NOT NULL,
    kind TEXT NOT NULL,
    venue TEXT NOT NULL DEFAULT '',
    symbol TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(seq, kind)
);

CREATE TABLE IF NOT EXISTS order_facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    seq INTEGER NOT NULL,
    ts_ms INTEGER NOT NULL,
    kind TEXT NOT NULL,
    venue TEXT NOT NULL DEFAULT 'unknown',
    symbol TEXT NOT NULL DEFAULT '',
    filled INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    latency_ms INTEGER NOT NULL DEFAULT 0,
    fee_quote REAL NOT NULL DEFAULT 0.0,
    payload_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(seq, kind)
);

CREATE TABLE IF NOT EXISTS entry_exit_facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    seq INTEGER NOT NULL,
    ts_ms INTEGER NOT NULL,
    kind TEXT NOT NULL,
    symbol TEXT NOT NULL DEFAULT '',
    entry_fee_quote REAL NOT NULL DEFAULT 0.0,
    exit_fee_quote REAL NOT NULL DEFAULT 0.0,
    net_quote REAL NOT NULL DEFAULT 0.0,
    payload_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(seq, kind)
);

CREATE TABLE IF NOT EXISTS risk_counter_facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    seq INTEGER NOT NULL,
    ts_ms INTEGER NOT NULL,
    kind TEXT NOT NULL,
    counter_value INTEGER NOT NULL DEFAULT 1,
    payload_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(seq, kind)
);

CREATE TABLE IF NOT EXISTS local_l2_health_facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    seq INTEGER NOT NULL,
    ts_ms INTEGER NOT NULL,
    kind TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT 'unspecified',
    category TEXT NOT NULL DEFAULT 'unspecified',
    venue TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(seq, kind)
);

CREATE TABLE IF NOT EXISTS diagnostic_facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    seq INTEGER NOT NULL,
    ts_ms INTEGER NOT NULL,
    kind TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT 'unspecified',
    classification TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(seq, kind)
);

CREATE TABLE IF NOT EXISTS projection_cursor (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_projected_seq INTEGER NOT NULL DEFAULT 0,
    last_projected_at_ms INTEGER NOT NULL DEFAULT 0,
    total_facts_written INTEGER NOT NULL DEFAULT 0,
    total_failures INTEGER NOT NULL DEFAULT 0
);
"""


class SqliteStore:
    """SQLite persistence for structured facts and ledgers."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def open(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.path))
        conn.row_factory = sqlite3.Row
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

    # ---------- Projection fact inserters (idempotent via ON CONFLICT) ----------

    def insert_projected_fact(
        self,
        conn: sqlite3.Connection,
        *,
        seq: int,
        ts_ms: int,
        kind: str,
        venue: str = "",
        symbol: str = "",
        payload_json: str = "{}",
    ) -> bool:
        """Insert a generic projected fact row. Returns True if inserted, False if skipped (dup)."""
        cur = conn.execute(
            "INSERT OR IGNORE INTO projected_facts "
            "(seq, ts_ms, kind, venue, symbol, payload_json) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (seq, ts_ms, kind, venue, symbol, payload_json),
        )
        conn.commit()
        return cur.rowcount > 0

    def insert_order_fact(
        self,
        conn: sqlite3.Connection,
        *,
        seq: int,
        ts_ms: int,
        kind: str,
        venue: str = "unknown",
        symbol: str = "",
        filled: bool = False,
        failed: bool = False,
        latency_ms: int = 0,
        fee_quote: float = 0.0,
        payload_json: str = "{}",
    ) -> bool:
        """Insert an order fact row. Returns True if inserted, False if skipped (dup)."""
        cur = conn.execute(
            "INSERT OR IGNORE INTO order_facts "
            "(seq, ts_ms, kind, venue, symbol, filled, failed, latency_ms, fee_quote, payload_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (seq, ts_ms, kind, venue, symbol, int(filled), int(failed), latency_ms, fee_quote, payload_json),
        )
        conn.commit()
        return cur.rowcount > 0

    def insert_entry_exit_fact(
        self,
        conn: sqlite3.Connection,
        *,
        seq: int,
        ts_ms: int,
        kind: str,
        symbol: str = "",
        entry_fee_quote: float = 0.0,
        exit_fee_quote: float = 0.0,
        net_quote: float = 0.0,
        payload_json: str = "{}",
    ) -> bool:
        cur = conn.execute(
            "INSERT OR IGNORE INTO entry_exit_facts "
            "(seq, ts_ms, kind, symbol, entry_fee_quote, exit_fee_quote, net_quote, payload_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (seq, ts_ms, kind, symbol, entry_fee_quote, exit_fee_quote, net_quote, payload_json),
        )
        conn.commit()
        return cur.rowcount > 0

    def insert_risk_counter_fact(
        self,
        conn: sqlite3.Connection,
        *,
        seq: int,
        ts_ms: int,
        kind: str,
        counter_value: int = 1,
        payload_json: str = "{}",
    ) -> bool:
        cur = conn.execute(
            "INSERT OR IGNORE INTO risk_counter_facts "
            "(seq, ts_ms, kind, counter_value, payload_json) "
            "VALUES (?, ?, ?, ?, ?)",
            (seq, ts_ms, kind, counter_value, payload_json),
        )
        conn.commit()
        return cur.rowcount > 0

    def insert_local_l2_health_fact(
        self,
        conn: sqlite3.Connection,
        *,
        seq: int,
        ts_ms: int,
        kind: str,
        reason: str = "unspecified",
        category: str = "unspecified",
        venue: str = "",
        payload_json: str = "{}",
    ) -> bool:
        cur = conn.execute(
            "INSERT OR IGNORE INTO local_l2_health_facts "
            "(seq, ts_ms, kind, reason, category, venue, payload_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (seq, ts_ms, kind, reason, category, venue, payload_json),
        )
        conn.commit()
        return cur.rowcount > 0

    def insert_diagnostic_fact(
        self,
        conn: sqlite3.Connection,
        *,
        seq: int,
        ts_ms: int,
        kind: str,
        reason: str = "unspecified",
        classification: str = "",
        payload_json: str = "{}",
    ) -> bool:
        cur = conn.execute(
            "INSERT OR IGNORE INTO diagnostic_facts "
            "(seq, ts_ms, kind, reason, classification, payload_json) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (seq, ts_ms, kind, reason, classification, payload_json),
        )
        conn.commit()
        return cur.rowcount > 0

    # ---------- Projection cursor ----------

    def get_projection_cursor(self, conn: sqlite3.Connection) -> dict:
        row = conn.execute(
            "SELECT last_projected_seq, last_projected_at_ms, total_facts_written, total_failures "
            "FROM projection_cursor WHERE id = 1"
        ).fetchone()
        if row is None:
            return {"last_projected_seq": 0, "last_projected_at_ms": 0,
                    "total_facts_written": 0, "total_failures": 0}
        return dict(row)

    def upsert_projection_cursor(
        self,
        conn: sqlite3.Connection,
        *,
        last_projected_seq: int,
        last_projected_at_ms: int,
        total_facts_written: int,
        total_failures: int,
    ) -> None:
        conn.execute(
            "INSERT INTO projection_cursor (id, last_projected_seq, last_projected_at_ms, "
            "total_facts_written, total_failures) "
            "VALUES (1, ?, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET "
            "last_projected_seq=excluded.last_projected_seq, "
            "last_projected_at_ms=excluded.last_projected_at_ms, "
            "total_facts_written=excluded.total_facts_written, "
            "total_failures=excluded.total_failures",
            (last_projected_seq, last_projected_at_ms, total_facts_written, total_failures),
        )
        conn.commit()

    # ---------- Structured store queries for analytical consumers ----------

    def query_projected_facts(
        self, conn: sqlite3.Connection, *, kind: str | None = None
    ) -> list[dict]:
        if kind:
            rows = conn.execute(
                "SELECT seq, ts_ms, kind, venue, symbol, payload_json "
                "FROM projected_facts WHERE kind = ? ORDER BY seq",
                (kind,),
            )
        else:
            rows = conn.execute(
                "SELECT seq, ts_ms, kind, venue, symbol, payload_json "
                "FROM projected_facts ORDER BY seq"
            )
        return [dict(r) for r in rows.fetchall()]

    def query_order_facts(
        self, conn: sqlite3.Connection, *, venue: str | None = None
    ) -> list[dict]:
        if venue:
            rows = conn.execute(
                "SELECT seq, ts_ms, kind, venue, symbol, filled, failed, latency_ms, fee_quote "
                "FROM order_facts WHERE venue = ? ORDER BY seq",
                (venue,),
            )
        else:
            rows = conn.execute(
                "SELECT seq, ts_ms, kind, venue, symbol, filled, failed, latency_ms, fee_quote "
                "FROM order_facts ORDER BY seq"
            )
        return [dict(r) for r in rows.fetchall()]

    def query_entry_exit_facts(self, conn: sqlite3.Connection) -> list[dict]:
        rows = conn.execute(
            "SELECT seq, ts_ms, kind, symbol, entry_fee_quote, exit_fee_quote, net_quote "
            "FROM entry_exit_facts ORDER BY seq"
        )
        return [dict(r) for r in rows.fetchall()]

    def query_risk_counter_facts(self, conn: sqlite3.Connection) -> list[dict]:
        rows = conn.execute(
            "SELECT seq, ts_ms, kind, counter_value "
            "FROM risk_counter_facts ORDER BY seq"
        )
        return [dict(r) for r in rows.fetchall()]

    def query_local_l2_health_facts(self, conn: sqlite3.Connection) -> list[dict]:
        rows = conn.execute(
            "SELECT seq, ts_ms, kind, reason, category, venue "
            "FROM local_l2_health_facts ORDER BY seq"
        )
        return [dict(r) for r in rows.fetchall()]

    def query_diagnostic_facts(self, conn: sqlite3.Connection) -> list[dict]:
        rows = conn.execute(
            "SELECT seq, ts_ms, kind, reason, classification "
            "FROM diagnostic_facts ORDER BY seq"
        )
        return [dict(r) for r in rows.fetchall()]

    def count_facts_by_kind(self, conn: sqlite3.Connection, kind: str) -> int:
        """Count projected facts matching a journal event kind across all fact tables."""
        total = 0
        for table in ["projected_facts", "order_facts", "entry_exit_facts",
                       "risk_counter_facts", "local_l2_health_facts",
                       "diagnostic_facts"]:
            row = conn.execute(
                f"SELECT COUNT(*) as cnt FROM {table} WHERE kind = ?", (kind,)
            ).fetchone()
            total += row["cnt"] if row else 0
        return total

    def has_projection_data(self, conn: sqlite3.Connection) -> bool:
        """Check whether any projection facts exist (used for fallback decisions)."""
        cursor = self.get_projection_cursor(conn)
        return cursor["total_facts_written"] > 0
