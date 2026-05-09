"""Evolution report: deterministic evidence-based parameter proposals."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class EvolutionReport:
    report_id: str
    generated_at_ms: int
    evidence: dict[str, Any] = field(default_factory=dict)
    proposals: list[dict[str, Any]] = field(default_factory=list)
    markdown: str = ""
    json_path: str = ""

    def write_markdown(self, path: str | Path) -> None:
        p = Path(path)
        p.write_text(self.markdown or "# Evolution Report\n\nNo proposals generated.")

    def write_json(self, path: str | Path) -> None:
        import json
        p = Path(path)
        p.write_text(json.dumps({"report_id": self.report_id, "proposals": self.proposals}, indent=2))
