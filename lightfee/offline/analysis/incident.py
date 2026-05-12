"""Incident report from state + journal.

V1: src/bin/incident_report.rs — builds incident reports with
affected positions, venue health, risk state, and recommendations.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class IncidentReport:
    incident_id: str
    ts_ms: int
    kind: str
    summary: str
    affected_positions: list[str] = field(default_factory=list)
    venue_health: dict[str, str] = field(default_factory=dict)
    risk_state: dict[str, str] = field(default_factory=dict)
    recommendations: list[str] = field(default_factory=list)
    severity: str = "info"


_INCIDENT_KINDS: dict[str, str] = {
    "runtime.error": "runtime_error",
    "runtime.fail_closed": "fail_closed",
    "runtime.fail_closed.venue_error": "venue_error",
    "runtime.fail_closed.margin_insufficient": "margin_insufficient",
    "risk.warning_triggered": "risk_warning",
    "risk.death_triggered": "risk_death",
    "runtime.local_l2_sync_failed": "local_l2_sync_failure",
    "recovery.blocked": "recovery_blocked",
}

_SEVERITY_MAP: dict[str, str] = {
    "runtime_error": "warning",
    "fail_closed": "critical",
    "venue_error": "critical",
    "margin_insufficient": "critical",
    "risk_warning": "warning",
    "risk_death": "critical",
    "local_l2_sync_failure": "warning",
    "recovery_blocked": "critical",
}


def classify_incident_kind(kind: str) -> str:
    """Map a journal event kind to a canonical incident kind."""
    for prefix, canonical in sorted(_INCIDENT_KINDS.items(), key=lambda x: -len(x[0])):
        if kind == prefix or kind.startswith(prefix + "."):
            return canonical
    return "unknown"


def incident_severity(incident_kind: str) -> str:
    """Return severity for an incident kind."""
    return _SEVERITY_MAP.get(incident_kind, "info")


def build_incident_report(
    records: list[dict],
    state: dict | None = None,
    now_ms: int = 0,
) -> IncidentReport | None:
    """Build an incident report from journal records and optional state.

    V1: build_incident_report in analysis.rs / incident_report.rs.
    Scans for runtime errors, fail-closed events, risk triggers,
    and recovery blocking events.
    """
    incidents: list[dict] = []
    venue_health: dict[str, str] = {}
    risk_state: dict[str, str] = {}
    affected_positions: list[str] = []

    for record in records:
        kind: str = record.get("kind", "")
        payload: dict = record.get("payload", {})

        incident_kind = classify_incident_kind(kind)
        if incident_kind != "unknown":
            incidents.append({
                "kind": incident_kind,
                "record_kind": kind,
                "payload": payload,
            })

        # Collect venue health from fail-closed events
        if kind.startswith("runtime.fail_closed"):
            venue = payload.get("venue", "")
            reason = payload.get("reason", "unknown")
            if venue:
                venue_health[venue] = f"fail_closed: {reason}"

        # Collect risk state
        if kind == "risk.warning_triggered":
            risk_state["warning"] = "active"
        elif kind == "risk.death_triggered":
            risk_state["death"] = "active"

        # Collect affected positions
        pos_id = payload.get("position_id", "")
        if pos_id and pos_id not in affected_positions:
            affected_positions.append(pos_id)

    if not incidents:
        return None

    # Determine dominant severity
    severities = [incident_severity(inc["kind"]) for inc in incidents]
    severity = "critical" if "critical" in severities else (
        "warning" if "warning" in severities else "info"
    )

    primary_kind = incidents[0]["kind"]

    return IncidentReport(
        incident_id=f"incident-{now_ms}",
        ts_ms=now_ms,
        kind=primary_kind,
        severity=severity,
        summary=(
            f"{len(incidents)} incident(s) detected: "
            f"{', '.join(inc['kind'] for inc in incidents[:5])}"
        ),
        affected_positions=affected_positions,
        venue_health=venue_health,
        risk_state=risk_state,
        recommendations=_build_recommendations(incidents),
    )


def _build_recommendations(incidents: list[dict]) -> list[str]:
    recommendations: list[str] = []
    kinds = {inc["kind"] for inc in incidents}

    if "fail_closed" in kinds or "venue_error" in kinds:
        recommendations.append("Check venue connectivity and API status")
    if "risk_warning" in kinds or "risk_death" in kinds:
        recommendations.append("Review risk envelope and position exposure")
    if "local_l2_sync_failure" in kinds:
        recommendations.append("Verify local-L2 stream health and re-subscribe if needed")
    if "recovery_blocked" in kinds:
        recommendations.append("Investigate recovery blocked state and clear pending reconciliations")
    if "runtime_error" in kinds:
        recommendations.append("Review error details in journal")

    if not recommendations:
        recommendations.append("Review incident details in journal")

    return recommendations
