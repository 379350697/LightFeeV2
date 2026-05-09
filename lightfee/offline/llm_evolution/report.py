"""LLM-assisted evolution reports. Disabled by default, explicit env prefix required."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any


@dataclass
class LLMEvolutionReport:
    report_id: str
    llm_enabled: bool = False
    llm_model: str = ""
    evidence: dict[str, Any] = field(default_factory=dict)
    analysis: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create_if_enabled(cls, report_id: str, evidence: dict[str, Any]) -> LLMEvolutionReport | None:
        enabled = os.environ.get("LIGHTFEE_LLM_EVOLUTION_ENABLED", "0") == "1"
        if not enabled:
            return None
        model = os.environ.get("LIGHTFEE_LLM_MODEL", "")
        return cls(report_id=report_id, llm_enabled=True, llm_model=model, evidence=evidence)

    def generate(self) -> None:
        """LLM-assisted analysis (disabled in no-network mode)."""
        if not self.llm_enabled:
            return
        self.analysis = {"status": "pending", "note": "LLM evolution requires network"}
