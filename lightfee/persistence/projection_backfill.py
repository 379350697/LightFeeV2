"""Reentrant, idempotent journal-to-projection backfill.

Reads the canonical journal via streaming primitives and dispatches
records to a row-writer callback. Tracks progress in a cursor sidecar
so interrupted backfills can resume without re-materialising the whole
journal.

Idempotency contract
--------------------
The backfill may replay records after a restart. The row-writer MUST
be idempotent (e.g. INSERT OR IGNORE keyed on journal seq). The cursor
is only advanced after a successful write, so the invariants are:

- Every projected row corresponds to exactly one journal record.
- Re-running the backfill with the same journal + same row-writer
  produces the same projected rows.
- No duplicate rows are created regardless of restart count.

Recovery safety
---------------
Recovery must NEVER depend on the projection store. This module lives
downstream of the journal — it reads the journal but never writes to
it, and recovery/replay paths do not import it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Protocol

from lightfee.persistence.journal import Journal
from lightfee.persistence.journal_index import JournalIndex


# ---------------------------------------------------------------------------
# Row-writer protocol
# ---------------------------------------------------------------------------


class RowWriter(Protocol):
    """Callback that materialises one journal record into projection storage.

    Must be idempotent: calling it twice with the same record must not
    produce duplicate rows (e.g. INSERT OR IGNORE on journal seq).
    """

    def __call__(self, record: dict[str, Any]) -> bool:
        """Write the record. Return True on success, False on transient failure."""
        ...


# ---------------------------------------------------------------------------
# Backfill result
# ---------------------------------------------------------------------------


@dataclass
class BackfillResult:
    records_processed: int = 0
    records_skipped: int = 0
    errors: int = 0
    started_seq: int = 0
    ended_seq: int = 0


# ---------------------------------------------------------------------------
# Cursor persistence
# ---------------------------------------------------------------------------


@dataclass
class _CursorState:
    last_projected_seq: int = 0
    backfill_version: int = 1


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------


class ProjectionBackfill:
    """Streaming journal → projection backfill with cursor-based resume."""

    def __init__(
        self,
        journal: Journal,
        index: JournalIndex,
        cursor_path: str | Path,
        row_writer: RowWriter,
    ) -> None:
        self._journal = journal
        self._index = index
        self._cursor_path = Path(cursor_path)
        self._writer = row_writer

    # ------------------------------------------------------------------
    # Cursor management
    # ------------------------------------------------------------------

    def _read_cursor(self) -> _CursorState:
        try:
            if self._cursor_path.exists():
                with open(self._cursor_path) as f:
                    raw = json.load(f)
                return _CursorState(
                    last_projected_seq=int(raw.get("last_projected_seq", 0)),
                    backfill_version=int(raw.get("backfill_version", 1)),
                )
        except (OSError, json.JSONDecodeError, ValueError):
            pass
        return _CursorState()

    def _write_cursor(self, state: _CursorState) -> None:
        self._cursor_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._cursor_path.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(
                {
                    "last_projected_seq": state.last_projected_seq,
                    "backfill_version": state.backfill_version,
                },
                f,
            )
        tmp.rename(self._cursor_path)

    @property
    def cursor_seq(self) -> int:
        """Last successfully projected seq (0 if nothing has been projected)."""
        return self._read_cursor().last_projected_seq

    # ------------------------------------------------------------------
    # Backfill
    # ------------------------------------------------------------------

    def backfill(self, *, target_seq: int | None = None) -> BackfillResult:
        """Run the backfill from cursor to target_seq (or end of journal).

        Idempotent: safe to call repeatedly. Records between cursor and
        target_seq are dispatched to the row-writer. The cursor advances
        after every successful write so partial progress is durable.

        Returns a BackfillResult summarising what happened.
        """
        cursor = self._read_cursor()
        start_seq = cursor.last_projected_seq + 1
        end_seq = target_seq if target_seq is not None else self._journal.max_seq

        result = BackfillResult(started_seq=start_seq, ended_seq=end_seq)

        if start_seq > end_seq:
            return result

        for record in self._journal.stream_from(start_seq, index=self._index):
            seq = record.get("seq", 0)
            if seq > end_seq:
                break

            try:
                ok = self._writer(record)
            except Exception:
                result.errors += 1
                # Stop on first failure so the cursor stays at the last
                # known-good position — retry will resume from here.
                break

            if ok:
                cursor.last_projected_seq = seq
                self._write_cursor(cursor)
                result.records_processed += 1
            else:
                result.records_skipped += 1

        return result
