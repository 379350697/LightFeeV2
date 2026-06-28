"""Report rendering: JSON, text, and markdown output formats.

V1: evolution/render.rs — renders reports in operator-facing formats.
"""

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


def render_daily_report_markdown(report: dict[str, Any]) -> str:
    """Render a daily report as operator-facing markdown.

    V1: daily report rendering in reports module.
    """
    lines = [
        f"# Daily Report: {report.get('date', 'unknown')}",
        "",
        "## Summary",
        f"- Total PnL (quote): {report.get('total_pnl_quote', 0):.4f}",
        f"- Total Fees (quote): {report.get('total_fee_quote', 0):.4f}",
        f"- Entry Count: {report.get('entry_count', 0)}",
        f"- Exit Count: {report.get('exit_count', 0)}",
    ]

    # Venue stats
    venue_stats = report.get("venue_stats", {})
    if venue_stats:
        lines.append("")
        lines.append("## Venue Statistics")
        for venue, stats in sorted(venue_stats.items()):
            lines.append(f"### {venue}")
            lines.append(f"- Orders: {stats.get('order_count', 0)}")
            lines.append(f"- Fills: {stats.get('fill_count', 0)}")
            lines.append(f"- Failures: {stats.get('failure_count', 0)}")
            lines.append(f"- Total Fee: {stats.get('total_fee_quote', 0):.4f}")

    # Recovery
    recovery = report.get("recovery_counts", {})
    if recovery:
        lines.append("")
        lines.append("## Recovery")
        for k, v in sorted(recovery.items()):
            lines.append(f"- {k}: {v}")

    # Risk
    risk = report.get("risk_counts", {})
    if risk:
        lines.append("")
        lines.append("## Risk")
        for k, v in sorted(risk.items()):
            lines.append(f"- {k}: {v}")

    # Diagnostics
    lines.append("")
    lines.append("## Diagnostics")
    lines.append(f"- Scan No-Entry: {report.get('scan_no_entry_diagnostics', 0)}")
    lines.append(f"- Scan Gate Blocked: {report.get('scan_runtime_gate_blocked', 0)}")
    lines.append(f"- Entry Liquidity Blocked: {report.get('execution_liquidity_blocked', 0)}")
    lines.append(f"- Local-L2 Sequence Gaps: {report.get('local_l2_sequence_gap_count', 0)}")
    lines.append(f"- Local-L2 Sync Failures: {report.get('local_l2_sync_failed_count', 0)}")
    lines.append(f"- Exit Shadow Decisions: {report.get('exit_shadow_decision_count', 0)}")
    lines.append(f"- Exit Shadow Path Markouts: {report.get('exit_shadow_path_markout_count', 0)}")

    exit_shadow = report.get("exit_shadow_by_bot", {})
    if exit_shadow:
        lines.append("")
        lines.append("## Exit Shadow")
        for bot_id, stats in sorted(exit_shadow.items()):
            lines.append(f"### {bot_id}")
            lines.append(f"- Samples: {stats.get('sample_count', 0)}")
            lines.append(f"- Direction Accuracy: {stats.get('direction_accuracy', 0):.4f}")
            lines.append(f"- Win Rate: {stats.get('win_rate', 0):.4f}")
            lines.append(
                f"- Avg Incremental Net Bps: {stats.get('avg_incremental_net_bps', 0):.4f}"
            )

    # Fail-closed
    fail_closed = report.get("fail_closed_reason_counts", {})
    if fail_closed:
        lines.append("")
        lines.append("## Fail-Closed Reasons")
        for reason, count in sorted(fail_closed.items()):
            lines.append(f"- {reason}: {count}")

    return "\n".join(lines)


def render_daily_report_json(report: dict[str, Any]) -> str:
    """Render a daily report as operator-facing JSON.

    V1: JSON report export in reports module.
    """
    return json.dumps(report, ensure_ascii=False, indent=2, default=str)


def render_incident_report_markdown(report: dict[str, Any]) -> str:
    """Render an incident report as operator-facing markdown.

    V1: incident report rendering in reports module.
    """
    lines = [
        f"# Incident Report: {report.get('incident_id', 'unknown')}",
        "",
        f"**Severity:** {report.get('severity', 'info')}",
        f"**Kind:** {report.get('kind', 'unknown')}",
        f"**Timestamp:** {report.get('ts_ms', 0)}",
        "",
        report.get("summary", "No summary"),
    ]

    affected = report.get("affected_positions", [])
    if affected:
        lines.append("")
        lines.append("## Affected Positions")
        for pos_id in affected:
            lines.append(f"- {pos_id}")

    venue_health = report.get("venue_health", {})
    if venue_health:
        lines.append("")
        lines.append("## Venue Health")
        for venue, status in sorted(venue_health.items()):
            lines.append(f"- {venue}: {status}")

    risk_state = report.get("risk_state", {})
    if risk_state:
        lines.append("")
        lines.append("## Risk State")
        for key, value in sorted(risk_state.items()):
            lines.append(f"- {key}: {value}")

    recommendations = report.get("recommendations", [])
    if recommendations:
        lines.append("")
        lines.append("## Recommendations")
        for rec in recommendations:
            lines.append(f"- {rec}")

    return "\n".join(lines)
