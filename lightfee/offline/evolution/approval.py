"""Approval queue for evolution proposals with deterministic overlay.

V1: approval overlay in evolution/cycle.rs — accepts and rejects proposals
deterministically based on reviewer decision.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any

from lightfee.persistence.ledgers import ApprovalRecord, ProposalRecord


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


def apply_approval_overlay(
    proposal: ProposalRecord,
    reviewer: str,
    *,
    approved: bool,
) -> dict[str, Any]:
    """Deterministic approval overlay for a proposal.

    V1: approval overlay in evolution cycle.
    Returns a dict with decision, proposal_id, reviewer, and decided_at_ms.
    """
    decision = "approved" if approved else "rejected"
    return {
        "proposal_id": proposal.proposal_id,
        "reviewer": reviewer,
        "decision": decision,
        "decided_at_ms": int(time.time() * 1000),
        "title": proposal.title,
    }


def reject_proposal(
    proposal: ProposalRecord,
    reviewer: str,
    reason: str = "",
) -> dict[str, Any]:
    """Deterministic rejection of a proposal with reason.

    V1: rejection path in evolution cycle.
    """
    return {
        "proposal_id": proposal.proposal_id,
        "reviewer": reviewer,
        "decision": "rejected",
        "reason": reason,
        "decided_at_ms": int(time.time() * 1000),
        "title": proposal.title,
    }
