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

-- V1-compatible lifecycle ledgers. These are rebuildable projections from the
-- journal and must not be used for runtime recovery decisions.

CREATE TABLE IF NOT EXISTS trade_ledger_events (
    event_id TEXT PRIMARY KEY,
    seq INTEGER NOT NULL,
    ts_ms INTEGER NOT NULL,
    run_id TEXT,
    instance_id TEXT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    event_kind TEXT NOT NULL,
    truth_level TEXT NOT NULL,
    source_kind TEXT NOT NULL DEFAULT 'journal_payload',
    source_ref TEXT,
    schema_version INTEGER NOT NULL DEFAULT 1,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS trade_ledger_entity_idx
    ON trade_ledger_events(entity_type, entity_id, ts_ms);
CREATE INDEX IF NOT EXISTS trade_ledger_kind_idx
    ON trade_ledger_events(event_kind, ts_ms);

CREATE TABLE IF NOT EXISTS position_ledger (
    position_id TEXT PRIMARY KEY,
    candidate_id TEXT,
    review_id TEXT,
    strategy_id TEXT,
    run_id TEXT,
    instance_id TEXT,
    symbol TEXT NOT NULL DEFAULT '',
    long_venue TEXT NOT NULL DEFAULT '',
    short_venue TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL,
    opened_at_ms INTEGER NOT NULL,
    closed_at_ms INTEGER,
    entry_qty REAL,
    exit_qty REAL,
    entry_notional_quote REAL,
    exit_notional_quote REAL,
    owner_instance_id TEXT,
    terminal_reason TEXT,
    problem INTEGER NOT NULL DEFAULT 0,
    problem_reason TEXT,
    reconciliation_status TEXT NOT NULL,
    truth_level TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS position_ledger_time_idx
    ON position_ledger(opened_at_ms, closed_at_ms);
CREATE INDEX IF NOT EXISTS position_ledger_symbol_idx
    ON position_ledger(symbol, opened_at_ms);

CREATE TABLE IF NOT EXISTS position_pnl_facts (
    pnl_key TEXT PRIMARY KEY,
    position_id TEXT NOT NULL,
    realized_price_pnl_quote REAL,
    funding_pnl_quote REAL,
    entry_fee_quote REAL,
    exit_fee_quote REAL,
    slippage_quote REAL,
    net_pnl_quote REAL,
    net_bps REAL,
    exit_reason TEXT,
    truth_level TEXT NOT NULL,
    reconciled_at_ms INTEGER,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS position_pnl_facts_position_idx
    ON position_pnl_facts(position_id, truth_level);

CREATE TABLE IF NOT EXISTS order_ledger (
    order_key TEXT PRIMARY KEY,
    position_id TEXT,
    candidate_id TEXT,
    venue TEXT NOT NULL DEFAULT '',
    symbol TEXT NOT NULL DEFAULT '',
    side TEXT NOT NULL DEFAULT '',
    stage TEXT NOT NULL DEFAULT '',
    reduce_only INTEGER NOT NULL DEFAULT 0,
    client_order_id TEXT,
    exchange_order_id TEXT,
    status TEXT NOT NULL,
    requested_qty REAL,
    filled_qty REAL,
    avg_fill_price REAL,
    fee_quote REAL,
    submitted_at_ms INTEGER,
    updated_at_ms INTEGER NOT NULL,
    truth_level TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS order_ledger_position_idx
    ON order_ledger(position_id, updated_at_ms);
CREATE INDEX IF NOT EXISTS order_ledger_client_idx
    ON order_ledger(venue, client_order_id);
CREATE INDEX IF NOT EXISTS order_ledger_exchange_idx
    ON order_ledger(venue, exchange_order_id);

CREATE TABLE IF NOT EXISTS fill_ledger (
    fill_key TEXT PRIMARY KEY,
    order_key TEXT,
    position_id TEXT,
    venue TEXT NOT NULL DEFAULT '',
    symbol TEXT NOT NULL DEFAULT '',
    side TEXT NOT NULL DEFAULT '',
    price REAL,
    qty REAL,
    fee_quote REAL,
    liquidity TEXT,
    exchange_trade_id TEXT,
    filled_at_ms INTEGER NOT NULL,
    truth_level TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS fill_ledger_position_idx
    ON fill_ledger(position_id, filled_at_ms);
CREATE INDEX IF NOT EXISTS fill_ledger_exchange_trade_idx
    ON fill_ledger(venue, exchange_trade_id);
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

    # ---------- V1-compatible lifecycle ledger inserters ----------

    def insert_trade_ledger_event(
        self,
        conn: sqlite3.Connection,
        *,
        event_id: str,
        seq: int,
        ts_ms: int,
        entity_type: str,
        entity_id: str,
        event_kind: str,
        truth_level: str,
        created_at_ms: int,
        run_id: str | None = None,
        instance_id: str | None = None,
        source_kind: str = "journal_payload",
        source_ref: str | None = None,
        schema_version: int = 1,
        payload_json: str = "{}",
    ) -> bool:
        cur = conn.execute(
            "INSERT OR IGNORE INTO trade_ledger_events "
            "(event_id, seq, ts_ms, run_id, instance_id, entity_type, entity_id, "
            "event_kind, truth_level, source_kind, source_ref, schema_version, "
            "payload_json, created_at_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                event_id,
                seq,
                ts_ms,
                run_id,
                instance_id,
                entity_type,
                entity_id,
                event_kind,
                truth_level,
                source_kind,
                source_ref,
                schema_version,
                payload_json,
                created_at_ms,
            ),
        )
        conn.commit()
        return cur.rowcount > 0

    def upsert_position_ledger(
        self,
        conn: sqlite3.Connection,
        *,
        position_id: str,
        state: str,
        opened_at_ms: int,
        updated_at_ms: int,
        candidate_id: str | None = None,
        review_id: str | None = None,
        strategy_id: str | None = None,
        run_id: str | None = None,
        instance_id: str | None = None,
        symbol: str = "",
        long_venue: str = "",
        short_venue: str = "",
        closed_at_ms: int | None = None,
        entry_qty: float | None = None,
        exit_qty: float | None = None,
        entry_notional_quote: float | None = None,
        exit_notional_quote: float | None = None,
        owner_instance_id: str | None = None,
        terminal_reason: str | None = None,
        problem: bool = False,
        problem_reason: str | None = None,
        reconciliation_status: str = "runtime_estimated",
        truth_level: str = "runtime_estimated",
        payload_json: str = "{}",
        created_at_ms: int,
    ) -> bool:
        cur = conn.execute(
            "INSERT INTO position_ledger ("
            "position_id, candidate_id, review_id, strategy_id, run_id, instance_id, "
            "symbol, long_venue, short_venue, state, opened_at_ms, closed_at_ms, "
            "entry_qty, exit_qty, entry_notional_quote, exit_notional_quote, "
            "owner_instance_id, terminal_reason, problem, problem_reason, "
            "reconciliation_status, truth_level, payload_json, created_at_ms, updated_at_ms"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(position_id) DO UPDATE SET "
            "candidate_id=COALESCE(excluded.candidate_id, position_ledger.candidate_id), "
            "review_id=COALESCE(excluded.review_id, position_ledger.review_id), "
            "strategy_id=COALESCE(excluded.strategy_id, position_ledger.strategy_id), "
            "run_id=COALESCE(excluded.run_id, position_ledger.run_id), "
            "instance_id=COALESCE(excluded.instance_id, position_ledger.instance_id), "
            "symbol=COALESCE(NULLIF(excluded.symbol, ''), position_ledger.symbol), "
            "long_venue=COALESCE(NULLIF(excluded.long_venue, ''), position_ledger.long_venue), "
            "short_venue=COALESCE(NULLIF(excluded.short_venue, ''), position_ledger.short_venue), "
            "state=excluded.state, "
            "opened_at_ms=MIN(position_ledger.opened_at_ms, excluded.opened_at_ms), "
            "closed_at_ms=COALESCE(excluded.closed_at_ms, position_ledger.closed_at_ms), "
            "entry_qty=COALESCE(excluded.entry_qty, position_ledger.entry_qty), "
            "exit_qty=COALESCE(excluded.exit_qty, position_ledger.exit_qty), "
            "entry_notional_quote=COALESCE(excluded.entry_notional_quote, position_ledger.entry_notional_quote), "
            "exit_notional_quote=COALESCE(excluded.exit_notional_quote, position_ledger.exit_notional_quote), "
            "owner_instance_id=COALESCE(excluded.owner_instance_id, position_ledger.owner_instance_id), "
            "terminal_reason=COALESCE(excluded.terminal_reason, position_ledger.terminal_reason), "
            "problem=MAX(position_ledger.problem, excluded.problem), "
            "problem_reason=COALESCE(excluded.problem_reason, position_ledger.problem_reason), "
            "reconciliation_status=excluded.reconciliation_status, "
            "truth_level=excluded.truth_level, "
            "payload_json=excluded.payload_json, "
            "updated_at_ms=excluded.updated_at_ms",
            (
                position_id,
                candidate_id,
                review_id,
                strategy_id,
                run_id,
                instance_id,
                symbol,
                long_venue,
                short_venue,
                state,
                opened_at_ms,
                closed_at_ms,
                entry_qty,
                exit_qty,
                entry_notional_quote,
                exit_notional_quote,
                owner_instance_id,
                terminal_reason,
                int(problem),
                problem_reason,
                reconciliation_status,
                truth_level,
                payload_json,
                created_at_ms,
                updated_at_ms,
            ),
        )
        conn.commit()
        return cur.rowcount > 0

    def upsert_position_pnl_fact(
        self,
        conn: sqlite3.Connection,
        *,
        pnl_key: str,
        position_id: str,
        truth_level: str,
        created_at_ms: int,
        realized_price_pnl_quote: float | None = None,
        funding_pnl_quote: float | None = None,
        entry_fee_quote: float | None = None,
        exit_fee_quote: float | None = None,
        slippage_quote: float | None = None,
        net_pnl_quote: float | None = None,
        net_bps: float | None = None,
        exit_reason: str | None = None,
        reconciled_at_ms: int | None = None,
        payload_json: str = "{}",
    ) -> bool:
        cur = conn.execute(
            "INSERT OR REPLACE INTO position_pnl_facts "
            "(pnl_key, position_id, realized_price_pnl_quote, funding_pnl_quote, "
            "entry_fee_quote, exit_fee_quote, slippage_quote, net_pnl_quote, "
            "net_bps, exit_reason, truth_level, reconciled_at_ms, payload_json, created_at_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                pnl_key,
                position_id,
                realized_price_pnl_quote,
                funding_pnl_quote,
                entry_fee_quote,
                exit_fee_quote,
                slippage_quote,
                net_pnl_quote,
                net_bps,
                exit_reason,
                truth_level,
                reconciled_at_ms,
                payload_json,
                created_at_ms,
            ),
        )
        conn.commit()
        return cur.rowcount > 0

    def upsert_order_ledger(
        self,
        conn: sqlite3.Connection,
        *,
        order_key: str,
        status: str,
        updated_at_ms: int,
        truth_level: str,
        position_id: str | None = None,
        candidate_id: str | None = None,
        venue: str = "",
        symbol: str = "",
        side: str = "",
        stage: str = "",
        reduce_only: bool = False,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
        requested_qty: float | None = None,
        filled_qty: float | None = None,
        avg_fill_price: float | None = None,
        fee_quote: float | None = None,
        submitted_at_ms: int | None = None,
        payload_json: str = "{}",
        created_at_ms: int,
    ) -> bool:
        cur = conn.execute(
            "INSERT INTO order_ledger ("
            "order_key, position_id, candidate_id, venue, symbol, side, stage, "
            "reduce_only, client_order_id, exchange_order_id, status, requested_qty, "
            "filled_qty, avg_fill_price, fee_quote, submitted_at_ms, updated_at_ms, "
            "truth_level, payload_json, created_at_ms"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(order_key) DO UPDATE SET "
            "position_id=COALESCE(excluded.position_id, order_ledger.position_id), "
            "candidate_id=COALESCE(excluded.candidate_id, order_ledger.candidate_id), "
            "venue=COALESCE(NULLIF(excluded.venue, ''), order_ledger.venue), "
            "symbol=COALESCE(NULLIF(excluded.symbol, ''), order_ledger.symbol), "
            "side=COALESCE(NULLIF(excluded.side, ''), order_ledger.side), "
            "stage=COALESCE(NULLIF(excluded.stage, ''), order_ledger.stage), "
            "reduce_only=excluded.reduce_only, "
            "client_order_id=COALESCE(excluded.client_order_id, order_ledger.client_order_id), "
            "exchange_order_id=COALESCE(excluded.exchange_order_id, order_ledger.exchange_order_id), "
            "status=excluded.status, "
            "requested_qty=COALESCE(excluded.requested_qty, order_ledger.requested_qty), "
            "filled_qty=COALESCE(excluded.filled_qty, order_ledger.filled_qty), "
            "avg_fill_price=COALESCE(excluded.avg_fill_price, order_ledger.avg_fill_price), "
            "fee_quote=COALESCE(excluded.fee_quote, order_ledger.fee_quote), "
            "submitted_at_ms=COALESCE(excluded.submitted_at_ms, order_ledger.submitted_at_ms), "
            "updated_at_ms=excluded.updated_at_ms, "
            "truth_level=excluded.truth_level, "
            "payload_json=excluded.payload_json",
            (
                order_key,
                position_id,
                candidate_id,
                venue,
                symbol,
                side,
                stage,
                int(reduce_only),
                client_order_id,
                exchange_order_id,
                status,
                requested_qty,
                filled_qty,
                avg_fill_price,
                fee_quote,
                submitted_at_ms,
                updated_at_ms,
                truth_level,
                payload_json,
                created_at_ms,
            ),
        )
        conn.commit()
        return cur.rowcount > 0

    def upsert_fill_ledger(
        self,
        conn: sqlite3.Connection,
        *,
        fill_key: str,
        filled_at_ms: int,
        truth_level: str,
        created_at_ms: int,
        order_key: str | None = None,
        position_id: str | None = None,
        venue: str = "",
        symbol: str = "",
        side: str = "",
        price: float | None = None,
        qty: float | None = None,
        fee_quote: float | None = None,
        liquidity: str | None = None,
        exchange_trade_id: str | None = None,
        payload_json: str = "{}",
    ) -> bool:
        cur = conn.execute(
            "INSERT OR REPLACE INTO fill_ledger "
            "(fill_key, order_key, position_id, venue, symbol, side, price, qty, "
            "fee_quote, liquidity, exchange_trade_id, filled_at_ms, truth_level, payload_json, created_at_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                fill_key,
                order_key,
                position_id,
                venue,
                symbol,
                side,
                price,
                qty,
                fee_quote,
                liquidity,
                exchange_trade_id,
                filled_at_ms,
                truth_level,
                payload_json,
                created_at_ms,
            ),
        )
        conn.commit()
        return cur.rowcount > 0

    # ---------- V1-compatible lifecycle ledger queries ----------

    def query_trade_ledger_events(
        self, conn: sqlite3.Connection, *, entity_type: str | None = None,
        entity_id: str | None = None,
    ) -> list[dict]:
        if entity_type and entity_id:
            rows = conn.execute(
                "SELECT * FROM trade_ledger_events "
                "WHERE entity_type = ? AND entity_id = ? ORDER BY seq",
                (entity_type, entity_id),
            )
        else:
            rows = conn.execute("SELECT * FROM trade_ledger_events ORDER BY seq")
        return [dict(r) for r in rows.fetchall()]

    def query_position_ledger(self, conn: sqlite3.Connection) -> list[dict]:
        rows = conn.execute("SELECT * FROM position_ledger ORDER BY opened_at_ms")
        return [dict(r) for r in rows.fetchall()]

    def query_order_ledger(self, conn: sqlite3.Connection) -> list[dict]:
        rows = conn.execute("SELECT * FROM order_ledger ORDER BY updated_at_ms")
        return [dict(r) for r in rows.fetchall()]

    def query_fill_ledger(self, conn: sqlite3.Connection) -> list[dict]:
        rows = conn.execute("SELECT * FROM fill_ledger ORDER BY filled_at_ms")
        return [dict(r) for r in rows.fetchall()]
