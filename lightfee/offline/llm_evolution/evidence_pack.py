"""LLM evolution evidence pack — structured evidence for LLM-driven analysis.

V1: llm_evolution/evidence_pack.rs — EvidencePack with phase1 data,
counterfactual summary, walk-forward summary, deterministic diagnostics,
and raw snippets. Disabled mode produces auditable no-op output.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class EvidencePack:
    """Structured evidence bundle for LLM-driven evolution analysis.

    V1: EvidencePack in llm_evolution/evidence_pack.rs.
    Contains phase1 report data, counterfactual replay summaries,
    deterministic diagnostics, and raw journal snippets.
    """

    cycle_id: str = ""
    status: str = "disabled"
    generated_at_ms: int = 0

    # Phase1 report evidence (V1: phase1 report sections)
    phase1_evidence: dict[str, Any] = field(default_factory=dict)

    # Counterfactual backfill summary (V1: counterfactual_scenarios)
    counterfactual_summary: dict[str, Any] = field(default_factory=dict)

    # Walk-forward summary (V1: walk_forward window results)
    walk_forward_summary: dict[str, Any] | None = None

    # Deterministic diagnostics (V1: run_deterministic_diagnostics output)
    diagnostics: list[dict[str, Any]] = field(default_factory=list)

    # Raw journal snippets for LLM context (V1: raw_snippets)
    raw_snippets: list[dict[str, Any]] = field(default_factory=list)

    # Review observability (V1: review_observability_summary)
    review_observability: dict[str, Any] | None = None

    # Opportunity cost summary (V1: opportunity_cost)
    opportunity_cost: dict[str, Any] | None = None

    # Shadow basket summary (V1: shadow_basket)
    shadow_basket: dict[str, Any] | None = None


def build_evidence_pack_disabled(
    cycle_id: str = "",
    generated_at_ms: int = 0,
) -> EvidencePack:
    """Build an auditable disabled evidence pack.

    V1: disabled mode emits structured no-op output, never silent absence.
    """
    return EvidencePack(
        cycle_id=cycle_id,
        status="disabled",
        generated_at_ms=generated_at_ms,
        diagnostics=[{
            "kind": "llm_evolution_disabled",
            "note": "LLM evolution is disabled — no network call made",
            "cycle_id": cycle_id,
        }],
    )


def build_evidence_pack_from_cycle(
    cycle_id: str,
    phase1_evidence: dict[str, Any],
    *,
    generated_at_ms: int = 0,
    counterfactual_summary: dict[str, Any] | None = None,
    walk_forward_summary: dict[str, Any] | None = None,
    diagnostics: list[dict[str, Any]] | None = None,
    raw_snippets: list[dict[str, Any]] | None = None,
    review_observability: dict[str, Any] | None = None,
    opportunity_cost: dict[str, Any] | None = None,
    shadow_basket: dict[str, Any] | None = None,
) -> EvidencePack:
    """Build an evidence pack from a completed evolution cycle.

    V1: build_llm_evidence_pack() in llm_evolution/evidence_pack.rs.
    """
    return EvidencePack(
        cycle_id=cycle_id,
        status="ready",
        generated_at_ms=generated_at_ms,
        phase1_evidence=phase1_evidence,
        counterfactual_summary=counterfactual_summary or {},
        walk_forward_summary=walk_forward_summary,
        diagnostics=diagnostics or [],
        raw_snippets=raw_snippets or [],
        review_observability=review_observability,
        opportunity_cost=opportunity_cost,
        shadow_basket=shadow_basket,
    )


def evidence_pack_summary(pack: EvidencePack) -> dict[str, Any]:
    """Produce a structured summary of the evidence pack.

    V1: serialized evidence pack summary for LLM prompt context.
    """
    return {
        "cycle_id": pack.cycle_id,
        "status": pack.status,
        "generated_at_ms": pack.generated_at_ms,
        "phase1_evidence_keys": list(pack.phase1_evidence.keys()),
        "counterfactual_has_data": bool(pack.counterfactual_summary),
        "walk_forward_available": pack.walk_forward_summary is not None,
        "diagnostic_count": len(pack.diagnostics),
        "raw_snippet_count": len(pack.raw_snippets),
        "review_observability_available": pack.review_observability is not None,
        "opportunity_cost_available": pack.opportunity_cost is not None,
        "shadow_basket_available": pack.shadow_basket is not None,
    }
