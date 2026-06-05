"""Replay dataset: loads journal range for offline replay using recorded evidence.

V2: structured-first reads from SQLite replay_facts when available,
with journal fallback for exact evidence and ordering-dependent events.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lightfee.persistence.journal import Journal

logger = logging.getLogger(__name__)


def _ts_to_date_str(ts_ms: int) -> str:
    """Convert a journal timestamp in ms to YYYYMMDD string."""
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y%m%d")
    except (OSError, ValueError, OverflowError):
        return ""


# Event kinds that MUST come from the journal because their meaning
# depends on exact sequence ordering or they participate in recovery.
_JOURNAL_ONLY_PREFIXES = (
    "recovery.",
    "runtime.booting",
    "runtime.running",
    "runtime.stopped",
)

_JOURNAL_ONLY_KINDS = frozenset({
    "pending_entry.viability_blocked",
    "runtime.entry_blocked_lifecycle",
    "runtime.entry_blocked_lifecycle_selection",
    "runtime.lifecycle_changed",
    "runtime.risk_mode_changed",
})


def _is_journal_only(kind: str) -> bool:
    """Return True if this event kind must always be read from the journal."""
    if kind in _JOURNAL_ONLY_KINDS:
        return True
    return kind.startswith(_JOURNAL_ONLY_PREFIXES)


# SQL for the expected replay_facts projection table.
# Tasks 1-3 create and populate this table; replay reads it.
_REPLAY_FACTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS replay_facts (
    seq INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    ts_ms INTEGER NOT NULL,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    date TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_replay_facts_date ON replay_facts(date);
CREATE INDEX IF NOT EXISTS idx_replay_facts_kind ON replay_facts(kind);
"""


def _ensure_replay_facts_table(conn: sqlite3.Connection) -> None:
    """Ensure the replay_facts table exists (idempotent)."""
    conn.executescript(_REPLAY_FACTS_TABLE_SQL)
    conn.commit()


def _read_replay_facts(
    store_path: Path,
    date_from: str,
    date_to: str,
) -> list[dict[str, Any]]:
    """Read projected replay records from the SQLite replay_facts table.

    Returns an empty list if the table doesn't exist or has no matching rows.
    """
    if not store_path.exists():
        return []

    conn = sqlite3.connect(str(store_path))
    try:
        # Check if the table exists
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='replay_facts'"
        )
        if cur.fetchone() is None:
            return []

        where: list[str] = []
        params: list[str] = []
        if date_from:
            where.append("date >= ?")
            params.append(date_from)
        if date_to:
            where.append("date <= ?")
            params.append(date_to)

        sql = "SELECT seq, run_id, ts_ms, kind, payload_json FROM replay_facts"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY seq ASC"

        records: list[dict[str, Any]] = []
        for row in conn.execute(sql, params):
            seq, run_id, ts_ms, kind, payload_json_str = row
            try:
                payload = json.loads(payload_json_str)
            except json.JSONDecodeError:
                payload = {}
            records.append({
                "seq": seq,
                "run_id": run_id,
                "ts_ms": ts_ms,
                "kind": kind,
                "payload": payload,
            })
        return records
    finally:
        conn.close()


def _read_journal_only_events(
    journal: Journal,
    date_from: str,
    date_to: str,
) -> list[dict[str, Any]]:
    """Read journal-only events (recovery, lifecycle, risk mode) from journal.

    These events must always come from the journal because their ordering
    and exact content are critical for replay correctness.
    """
    all_records = journal.read_all()
    filtered: list[dict[str, Any]] = []

    for r in all_records:
        kind = r.get("kind", "")
        if not _is_journal_only(kind):
            continue
        ts = r.get("ts_ms", 0)
        if date_from and _ts_to_date_str(ts) < date_from:
            continue
        if date_to and _ts_to_date_str(ts) > date_to:
            continue
        filtered.append(r)

    return filtered


@dataclass
class ReplayDataset:
    records: list[dict] = field(default_factory=list)
    date_from: str = ""
    date_to: str = ""
    source: str = "unknown"

    @classmethod
    def from_journal_range(
        cls,
        journal_path: str | Path,
        date_from: str = "",
        date_to: str = "",
    ) -> ReplayDataset:
        """Full journal scan with date filter — authoritative exact-evidence path."""
        journal = Journal(journal_path)
        records = journal.read_all()

        filtered = records
        if date_from:
            filtered = [
                r for r in filtered
                if _ts_to_date_str(r.get("ts_ms", 0)) >= date_from
            ]
        if date_to:
            filtered = [
                r for r in filtered
                if _ts_to_date_str(r.get("ts_ms", 0)) <= date_to
            ]

        return cls(records=filtered, date_from=date_from, date_to=date_to,
                   source="journal")

    @classmethod
    def from_structured(
        cls,
        store_path: str | Path,
        date_from: str = "",
        date_to: str = "",
        *,
        journal_path: str | Path | None = None,
    ) -> ReplayDataset:
        """Structured-first read with journal fallback for ordering-dependent events.

        Reads projectable events (entry, exit, order, scan, risk counters,
        local-L2 health) from the SQLite replay_facts table. When
        journal_path is provided, also reads journal-only events (recovery,
        lifecycle, risk mode transitions) and merges them in seq order.

        Falls back to a full journal scan when the structured store has no
        matching records.
        """
        store_path = Path(store_path)
        structured_records = _read_replay_facts(store_path, date_from, date_to)

        if journal_path is None:
            if not structured_records:
                raise ValueError(
                    "No structured records found in replay_facts and no "
                    "journal_path provided for fallback"
                )
            return cls(records=structured_records, date_from=date_from,
                       date_to=date_to, source="structured")

        journal = Journal(journal_path)
        journal_only = _read_journal_only_events(journal, date_from, date_to)

        if structured_records:
            merged = list(structured_records) + journal_only
            merged.sort(key=lambda r: r.get("seq", 0))
            return cls(records=merged, date_from=date_from, date_to=date_to,
                       source="merged")
        else:
            logger.info(
                "replay_facts table empty or missing, falling back to journal scan"
            )
            return cls.from_journal_range(journal_path, date_from, date_to)

    @classmethod
    def load(
        cls,
        journal_path: str | Path,
        store_path: str | Path | None = None,
        date_from: str = "",
        date_to: str = "",
    ) -> ReplayDataset:
        """Load replay dataset: structured-first with journal fallback.

        This is the recommended entry point for offline replay consumers.
        Tries the structured projection store first when store_path is
        provided and exists. Falls back to the journal scan when structured
        data is unavailable or incomplete.

        Returns a ReplayDataset with source="structured", "merged", or
        "journal" so callers can inspect which path was used.
        """
        if store_path is not None and Path(store_path).exists():
            try:
                return cls.from_structured(
                    store_path, date_from, date_to, journal_path=journal_path,
                )
            except Exception:
                logger.warning(
                    "structured read failed, falling back to journal",
                    exc_info=True,
                )
        return cls.from_journal_range(journal_path, date_from, date_to)
