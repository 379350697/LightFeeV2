"""Experiment ledger: tracks outcomes of applied proposals."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from lightfee.persistence.ledgers import ExperimentRecord, ProposalRecord


class EvolutionLedger:
    """Tracks evolution proposals, approvals, and experiment outcomes."""

    def __init__(self, sqlite_path: str | Path) -> None:
        self.sqlite_path = Path(sqlite_path)

    def record_proposal(self, conn: sqlite3.Connection, proposal: ProposalRecord) -> None:
        import json
        conn.execute(
            "INSERT OR REPLACE INTO proposal_catalog "
            "(proposal_id, version, status, title, body_json, proposed_at_ms) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                proposal.proposal_id,
                proposal.version,
                proposal.status,
                proposal.title,
                json.dumps(proposal.body),
                proposal.proposed_at_ms,
            ),
        )
        conn.commit()

    def record_experiment(self, conn: sqlite3.Connection, experiment: ExperimentRecord) -> None:
        import json
        conn.execute(
            "INSERT INTO experiment_ledger "
            "(proposal_id, run_id, outcome_json, started_at_ms, completed_at_ms) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                experiment.proposal_id,
                experiment.run_id,
                json.dumps(experiment.outcome),
                experiment.started_at_ms,
                experiment.completed_at_ms,
            ),
        )
        conn.commit()
