"""Experiment ledger and proposal catalog: tracks outcomes of applied proposals.

V1: evolution/ledger.rs — experiment ledger with outcomes, facts, insights.
V1: evolution/catalog.rs — proposal catalog with versioned snapshots.

Provides both SQLite (for runtime integration) and JSON (for offline/audit) backends.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from lightfee.persistence.ledgers import ExperimentRecord, ProposalRecord


class EvolutionLedger:
    """Tracks evolution proposals, approvals, and experiment outcomes.

    V1: EvolutionLedger in evolution/ledger.rs and evolution/catalog.rs.
    """

    def __init__(self, sqlite_path: str | Path) -> None:
        self.sqlite_path = Path(sqlite_path)

    def record_proposal(self, conn: sqlite3.Connection, proposal: ProposalRecord) -> None:
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


# ── JSON-based Proposal Catalog (V1 parity) ────────────────────────────────


def proposal_catalog_path(output_dir: str | Path) -> Path:
    """Path to proposal-catalog.json.

    V1: proposal_catalog_path() in evolution/catalog.rs.
    """
    return Path(output_dir) / "proposal-catalog.json"


def load_proposal_catalog(output_dir: str | Path) -> list[dict[str, Any]]:
    """Load the proposal catalog from JSON.

    V1: load_proposal_catalog() in evolution/catalog.rs.
    Returns empty list if the catalog does not exist.
    """
    path = proposal_catalog_path(output_dir)
    if not path.exists():
        return []
    raw = path.read_text()
    return json.loads(raw)


def persist_proposal_catalog(
    output_dir: str | Path,
    proposals: list[dict[str, Any]],
) -> Path:
    """Persist the proposal catalog to JSON.

    V1: persist_report_proposals() in evolution/catalog.rs.
    """
    path = proposal_catalog_path(output_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(proposals, indent=2, ensure_ascii=False))
    return path


def add_to_proposal_catalog(
    output_dir: str | Path,
    proposal: dict[str, Any],
) -> list[dict[str, Any]]:
    """Add a proposal to the catalog and persist.

    V1: persist_parameter_snapshot / persist_system_snapshot in catalog.rs.
    """
    catalog = load_proposal_catalog(output_dir)
    catalog.append(proposal)
    persist_proposal_catalog(output_dir, catalog)
    return catalog


def latest_proposal_records(
    output_dir: str | Path,
    proposal_id: str,
) -> list[dict[str, Any]]:
    """Find the latest version(s) of a proposal by proposal_id.

    V1: latest_proposal_record() in evolution/catalog.rs.
    """
    catalog = load_proposal_catalog(output_dir)
    matching = [p for p in catalog if p.get("proposal_id") == proposal_id]
    matching.sort(key=lambda p: p.get("stored_at_ms", 0), reverse=True)
    return matching


# ── JSON-based Experiment Ledger (V1 parity) ───────────────────────────────


def experiment_ledger_path(output_dir: str | Path) -> Path:
    """Path to experiment-ledger.json.

    V1: experiment_ledger_path() in evolution/ledger.rs.
    """
    return Path(output_dir) / "experiment-ledger.json"


def load_experiment_ledger(output_dir: str | Path) -> dict[str, Any]:
    """Load the experiment ledger from JSON.

    V1: load_experiment_ledger() in evolution/ledger.rs.
    Returns default structure if the ledger does not exist.
    """
    path = experiment_ledger_path(output_dir)
    if not path.exists():
        return {"outcomes": [], "insights": [], "facts": []}
    raw = path.read_text()
    return json.loads(raw)


def persist_experiment_ledger(
    output_dir: str | Path,
    ledger: dict[str, Any],
) -> Path:
    """Persist the experiment ledger to JSON.

    V1: persist in evolution/ledger.rs.
    """
    path = experiment_ledger_path(output_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(ledger, indent=2, ensure_ascii=False))
    return path


def record_experiment_outcome(
    output_dir: str | Path,
    proposal_id: str,
    run_id: str,
    outcome: dict[str, Any],
    *,
    started_at_ms: int = 0,
    completed_at_ms: int = 0,
) -> dict[str, Any]:
    """Record an experiment outcome in the ledger.

    V1: apply_experiment_overlay in evolution/ledger.rs.
    """
    ledger = load_experiment_ledger(output_dir)
    record = {
        "proposal_id": proposal_id,
        "run_id": run_id,
        "outcome": outcome,
        "started_at_ms": started_at_ms,
        "completed_at_ms": completed_at_ms,
    }
    ledger.setdefault("outcomes", []).append(record)
    persist_experiment_ledger(output_dir, ledger)
    return record
