"""Report rendering: JSON and text output formats."""

from __future__ import annotations

import json
from typing import Any


def render_text(data: Any, indent: int = 0) -> str:
    if isinstance(data, dict):
        lines: list[str] = []
        for k, v in data.items():
            lines.append(f"{'  ' * indent}{k}: {render_text(v, indent + 1)}")
        return "\n".join(lines)
    elif isinstance(data, list):
        return ", ".join(str(x) for x in data)
    return str(data)


def render_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, default=str)
