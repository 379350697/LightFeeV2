"""Ledger abstractions for evolution proposals and approval tracking."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ProposalRecord:
    proposal_id: str
    version: int = 1
    status: str = "proposed"
    title: str = ""
    body: dict[str, Any] = field(default_factory=dict)
    proposed_at_ms: int = 0
    approved_at_ms: int | None = None
    rejected_at_ms: int | None = None


@dataclass
class ApprovalRecord:
    proposal_id: str
    reviewer: str = ""
    decision: str | None = None
    notes: str | None = None
    decided_at_ms: int | None = None


@dataclass
class ExperimentRecord:
    proposal_id: str
    run_id: str
    outcome: dict[str, Any] = field(default_factory=dict)
    started_at_ms: int = 0
    completed_at_ms: int | None = None
