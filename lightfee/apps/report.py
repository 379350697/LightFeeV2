"""lightfee-report: journal/incident/runtime posture report CLI."""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from lightfee.config.loader import load_config
from lightfee.offline.analysis.journal import analyze_journal_records
from lightfee.offline.analysis.incident import build_incident_report
from lightfee.offline.reports.render import render_json, render_text
from lightfee.persistence.journal import Journal


def main() -> None:
    parser = argparse.ArgumentParser(description="lightfee-report: Analysis and reporting")
    parser.add_argument("--config", "-c", default="config/example.toml", help="Path to config TOML")
    parser.add_argument("--format", choices=["json", "text"], default="text", help="Output format")
    parser.add_argument("--journal", "-j", help="Path to journal file")
    parser.add_argument("--state", "-s", help="Path to state snapshot (for incident report)")
    args = parser.parse_args()

    config = load_config(args.config)

    if not args.journal:
        print(f"Report loaded config: {config.runtime.mode} mode, {len(config.symbols)} symbols")
        print("No journal path provided; pass --journal to generate a report.")
        return

    journal_path = Path(args.journal)
    if not journal_path.exists():
        print(f"Journal not found: {args.journal}")
        return

    journal = Journal(journal_path)
    records = journal.read_all()
    report = analyze_journal_records(records)

    state = None
    if args.state:
        state_path = Path(args.state)
        if state_path.exists():
            import json
            state = json.loads(state_path.read_text())

    incident = build_incident_report(records, state, int(time.time() * 1000))

    output = {
        "date": report.daily.date or "unknown",
        "total_pnl_quote": report.daily.total_pnl_quote,
        "total_fee_quote": report.daily.total_fee_quote,
        "entry_count": report.daily.entry_count,
        "exit_count": report.daily.exit_count,
        "venue_stats": {
            v: {
                "order_count": s.order_count,
                "fill_count": s.fill_count,
                "failure_count": s.failure_count,
                "max_latency_ms": s.max_latency_ms if s.max_latency_ms != 9223372036854775807 else 0,
                "min_latency_ms": s.min_latency_ms if s.min_latency_ms != 9223372036854775807 else 0,
                "total_fee_quote": s.total_fee_quote,
            }
            for v, s in report.venue_stats.items()
        },
        "recovery_counts": report.recovery_counts,
        "risk_counts": report.risk_counts,
        "scan_no_entry_diagnostics": report.scan_no_entry_diagnostics_count,
        "scan_runtime_gate_blocked": report.scan_runtime_gate_blocked_count,
        "execution_liquidity_blocked": report.execution_liquidity_blocked_count,
        "local_l2_sequence_gap_count": report.local_l2_sequence_gap_count,
        "local_l2_sync_failed_count": report.local_l2_sync_failed_count,
    }

    if incident:
        output["incident"] = {
            "incident_id": incident.incident_id,
            "kind": incident.kind,
            "summary": incident.summary,
        }

    render = render_json if args.format == "json" else render_text
    print(render(output))
