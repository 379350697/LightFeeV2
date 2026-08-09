"""lightfee-explain: runtime posture diagnostic tool."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, field
from typing import Optional

from lightfee.engine.recovery_decision_core import pending_close_owner_counts


@dataclass
class RuntimePostureReport:
    """Summary of the engine's current lifecycle, risk mode, and evidence."""

    lifecycle: str = ""
    risk_mode: str = ""
    run_id: str = ""
    tick_count: int = 0
    open_positions: int = 0
    pending_entries: int = 0
    pending_closes: int = 0
    pending_passive_closes: int = 0
    pending_close_reconciliations: int = 0
    pending_close_owners: int = 0
    recent_errors: list[str] = field(default_factory=list)


def load_runtime_posture_report(snapshot_path: str) -> Optional[RuntimePostureReport]:
    """Load EngineState snapshot and produce a posture report."""
    if not os.path.exists(snapshot_path):
        return None

    with open(snapshot_path) as f:
        data = json.load(f)

    pending_close_owners = pending_close_owner_counts(data)
    return RuntimePostureReport(
        lifecycle=data.get("lifecycle", "unknown"),
        risk_mode=data.get("risk_mode", "unknown"),
        run_id=data.get("run_id", ""),
        tick_count=data.get("tick_count", 0),
        open_positions=data.get("open_position_count", 0),
        pending_entries=data.get("pending_entry_count", 0),
        pending_closes=pending_close_owners.pending_close_count,
        pending_passive_closes=(
            pending_close_owners.pending_passive_close_count
        ),
        pending_close_reconciliations=(
            pending_close_owners.pending_close_reconciliation_count
        ),
        pending_close_owners=pending_close_owners.pending_close_owner_count,
    )


def render_runtime_posture_text(report: RuntimePostureReport) -> str:
    """Render a human-readable posture summary."""
    lines = [
        "=" * 54,
        "  LightFee Runtime Posture",
        "=" * 54,
        f"  lifecycle     : {report.lifecycle}",
        f"  risk mode     : {report.risk_mode}",
        f"  run id        : {report.run_id}",
        f"  tick count    : {report.tick_count}",
        f"  open positions: {report.open_positions}",
        f"  pending entries: {report.pending_entries}",
        f"  pending closes : {report.pending_closes}",
        f"  pending passive closes: {report.pending_passive_closes}",
        f"  pending close reconciliations: {report.pending_close_reconciliations}",
        f"  pending close owners : {report.pending_close_owners}",
        "=" * 54,
    ]
    if report.recent_errors:
        lines.append("  recent errors:")
        for err in report.recent_errors[-5:]:
            lines.append(f"    - {err}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="lightfee-explain: runtime posture diagnostic"
    )
    parser.add_argument(
        "--snapshot",
        default="runtime/state.json",
        help="Path to engine state snapshot JSON",
    )
    parser.add_argument(
        "--json", action="store_true", help="Output as JSON instead of text"
    )
    args = parser.parse_args()

    report = load_runtime_posture_report(args.snapshot)
    if report is None:
        print(f"No snapshot found at {args.snapshot}")
        return

    if args.json:
        print(json.dumps(report.__dict__, indent=2))
    else:
        print(render_runtime_posture_text(report))
