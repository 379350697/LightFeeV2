"""Approval queue for evolution proposals."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from lightfee.persistence.ledgers import ApprovalRecord


class ApprovalQueue:
    """Tracks proposal approval/rejection decisions."""

    def __init__(self, sqlite_path: str | Path) -> None:
        self.sqlite_path = Path(sqlite_path)

    def record_decision(self, conn: sqlite3.Connection, approval: ApprovalRecord) -> None:
        conn.execute(
            "INSERT INTO approval_queue (proposal_id, reviewer, decision, notes, decided_at_ms) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                approval.proposal_id,
                approval.reviewer,
                approval.decision,
                approval.notes,
                approval.decided_at_ms,
            ),
        )
        conn.commit()
