"""Replay dataset: loads journal range for offline replay."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from lightfee.persistence.journal import Journal


@dataclass
class ReplayDataset:
    records: list[dict] = field(default_factory=list)
    date_from: str = ""
    date_to: str = ""

    @classmethod
    def from_journal_range(
        cls,
        journal_path: str | Path,
        date_from: str = "",
        date_to: str = "",
    ) -> ReplayDataset:
        journal = Journal(journal_path)
        records = journal.read_all()

        filtered = records
        if date_from:
            filtered = [r for r in filtered if str(r.get("ts_ms", 0))[:8] >= date_from]
        if date_to:
            filtered = [r for r in filtered if str(r.get("ts_ms", 0))[:8] <= date_to]

        return cls(records=filtered, date_from=date_from, date_to=date_to)
