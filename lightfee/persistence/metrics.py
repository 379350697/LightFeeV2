"""Persistence metrics and diagnostics."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class PersistenceMetrics:
    journal_appends: int = 0
    journal_flushes: int = 0
    snapshot_writes: int = 0
    snapshot_reads: int = 0
    sqlite_writes: int = 0
    last_journal_append_ms: int = 0
    last_snapshot_write_ms: int = 0
    errors: list[str] = field(default_factory=list)

    def record_journal_append(self, ts_ms: int) -> None:
        self.journal_appends += 1
        self.last_journal_append_ms = ts_ms

    def record_journal_flush(self) -> None:
        self.journal_flushes += 1

    def record_snapshot_write(self, ts_ms: int) -> None:
        self.snapshot_writes += 1
        self.last_snapshot_write_ms = ts_ms

    def record_snapshot_read(self) -> None:
        self.snapshot_reads += 1

    def record_error(self, error: str) -> None:
        self.errors.append(error)
