"""LLM evolution proposal schema and validation.

V1: evolution/model.rs — ProposalKind, ParameterProposalDraft, SystemProposalDraft.
V1: llm_evolution/engine.rs — RootCauseFinding with prompt contract validation.

Preserves V1's strict prompt contract: every proposal must have findings,
affected parameters, and pass validation before being added to the catalog.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class LlmProposal:
    """LLM-generated evolution proposal.

    V1: ParameterProposalDraft and SystemProposalDraft from model.rs.
    """

    proposal_id: str
    findings: list[str] = field(default_factory=list)
    affected_parameters: list[str] = field(default_factory=list)
    rationale: str = ""
    proposed_value: float | None = None
    current_value: float | None = None
    confidence: str = "medium"
    coverage_grade: str = "bounded"
    uncertainties: list[str] = field(default_factory=list)
    evidence_refs: list[str] = field(default_factory=list)
    source: str = "llm_evolution"


@dataclass
class LlmProposalValidation:
    """Validation result for an LLM proposal.

    V1: ensure_root_cause_contract() and validation in engine.rs.
    """

    proposal_id: str
    valid: bool = False
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


# ── Prompt contract constants ──────────────────────────────────────────────

PROMPT_CONTRACT_VERSION = "1.0"

REQUIRED_PROPOSAL_FIELDS = frozenset({
    "proposal_id",
    "findings",
    "affected_parameters",
    "rationale",
    "evidence_refs",
    "uncertainties",
})


def validate_proposal(proposal: LlmProposal) -> LlmProposalValidation:
    """Validate an LLM proposal against the prompt contract.

    V1: ensure_root_cause_contract() in llm_evolution/engine.rs.
    A valid proposal must have:
    - At least one finding
    - At least one affected parameter
    - Non-empty rationale
    - At least one evidence reference
    - At least one uncertainty
    """
    errors: list[str] = []
    warnings: list[str] = []

    if not proposal.findings:
        errors.append("proposal must have at least one finding")

    if not proposal.affected_parameters:
        errors.append("proposal must have at least one affected parameter")

    if not proposal.rationale.strip():
        errors.append("proposal must have a non-empty rationale")

    if not proposal.evidence_refs:
        errors.append("proposal must have at least one evidence reference")

    if not proposal.uncertainties:
        errors.append("proposal must have at least one uncertainty")

    if proposal.confidence not in ("low", "medium", "high", "insufficient_evidence"):
        warnings.append(
            f"unexpected confidence level: {proposal.confidence}"
        )

    if proposal.coverage_grade not in ("none", "bounded", "comprehensive"):
        warnings.append(
            f"unexpected coverage grade: {proposal.coverage_grade}"
        )

    return LlmProposalValidation(
        proposal_id=proposal.proposal_id,
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
    )


def validate_proposals(proposals: list[LlmProposal]) -> list[LlmProposalValidation]:
    """Validate multiple proposals. V1: batch validation in engine.rs."""
    return [validate_proposal(p) for p in proposals]
