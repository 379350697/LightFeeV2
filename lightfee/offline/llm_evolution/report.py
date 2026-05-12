"""LLM-assisted evolution reports. Disabled by default, explicit env prefix required.

V1: llm_evolution/engine.rs — LlmStageMode::Disabled by default.
Http mode requires explicit environment configuration and provider/model metadata.
Disabled mode must never make network calls.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any


@dataclass
class LLMEvolutionReport:
    report_id: str
    llm_enabled: bool = False
    llm_model: str = ""
    llm_provider: str = ""
    evidence: dict[str, Any] = field(default_factory=dict)
    analysis: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create_if_enabled(
        cls, report_id: str, evidence: dict[str, Any]
    ) -> LLMEvolutionReport | None:
        """Create an LLM evolution report only if explicitly enabled.

        V1: disabled by default (LlmStageMode::Disabled).
        Requires LIGHTFEE_LLM_EVOLUTION_ENABLED=1.
        Optionally records LIGHTFEE_LLM_MODEL and LIGHTFEE_LLM_PROVIDER metadata.
        """
        enabled = os.environ.get("LIGHTFEE_LLM_EVOLUTION_ENABLED", "0") == "1"
        if not enabled:
            return None
        model = os.environ.get("LIGHTFEE_LLM_MODEL", "")
        provider = os.environ.get("LIGHTFEE_LLM_PROVIDER", "")
        return cls(
            report_id=report_id,
            llm_enabled=True,
            llm_model=model,
            llm_provider=provider,
            evidence=evidence,
        )

    def generate(self) -> None:
        """Generate analysis.

        V1: disabled mode emits structured pending/disabled report.
        Enabled mode records provider/model metadata.
        No network call is attempted in disabled mode.
        """
        if not self.llm_enabled:
            self.analysis = {
                "status": "disabled",
                "note": "LLM evolution is disabled — no network call made",
            }
            return
        self.analysis = {
            "status": "pending",
            "note": "LLM evolution requires network",
            "provider": self.llm_provider if self.llm_provider else "unspecified",
            "model": self.llm_model if self.llm_model else "unspecified",
        }


# ── Prompt Contract ────────────────────────────────────────────────────────

def get_prompt_contract() -> dict[str, Any]:
    """Return the LLM evolution prompt contract definition.

    V1: prompt.rs — explicit contract with required sections, proposal schema,
    and validation rules. Used by LLM stage to enforce structured output.
    """
    return {
        "version": "1.0",
        "required_sections": [
            "findings",
            "evidence_refs",
            "affected_parameters",
            "rationale",
            "uncertainties",
        ],
        "proposal_schema": {
            "proposal_id": "string",
            "findings": "list[string]",
            "affected_parameters": "list[string]",
            "rationale": "string",
            "confidence": "string (low|medium|high|insufficient_evidence)",
            "coverage_grade": "string (none|bounded|comprehensive)",
            "uncertainties": "list[string]",
            "evidence_refs": "list[string]",
        },
        "validation_rules": [
            "findings must not be empty",
            "affected_parameters must not be empty",
            "rationale must not be empty",
            "evidence_refs must not be empty",
            "uncertainties must not be empty",
        ],
    }


# ── Root-Cause Summary ─────────────────────────────────────────────────────


def build_root_cause_summary_disabled() -> dict[str, Any]:
    """Build a root-cause summary when LLM evolution is disabled.

    V1: disabled_root_cause_findings() in llm_evolution/engine.rs.
    Disabled mode must produce auditable no-op output, not silent absence.
    """
    return {
        "status": "disabled",
        "note": "LLM root-cause stage is disabled — no network call made",
        "findings": [],
        "validation_passed": True,
    }

