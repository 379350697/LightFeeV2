"""Incident report from state + journal."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class IncidentReport:
    incident_id: str
    ts_ms: int
    kind: str
    summary: str
    affected_positions: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)


def build_incident_report(
    records: list[dict],
    state: dict | None,
    now_ms: int,
) -> IncidentReport | None:
    """Build an incident report from journal and state."""
    errors = [r for r in records if r.get("kind", "").startswith("runtime.") and "error" in r.get("payload", {})]

    if not errors:
        return None

    return IncidentReport(
        incident_id=f"incident-{now_ms}",
        ts_ms=now_ms,
        kind="runtime_errors",
        summary=f"{len(errors)} runtime errors detected",
        recommendations=["Review error details in journal", "Check venue connectivity"],
    )
