#!/usr/bin/env python3
"""V2 live dry-run audit — reads journal/logs, reports root-fix metrics.

Does NOT import live credentials or submit orders.
Usage: python3 scripts/lightfee_v2_live_dryrun_audit.py --minutes 120 --log /var/log/lightfee-v2/live.log
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict


def audit(journal_path: str, minutes: int) -> dict:
    """Parse journal lines, count key events."""
    import time
    counts: dict[str, int] = defaultdict(int)
    venue_reasons: dict[str, int] = defaultdict(int)
    l2_selection_reasons: dict[str, int] = defaultdict(int)
    ws_bbo_selection_reasons: dict[str, int] = defaultdict(int)
    l2_not_ready_reasons: dict[str, int] = defaultdict(int)

    if not os.path.exists(journal_path):
        print(f"Journal not found: {journal_path}", file=sys.stderr)
        return {}

    now_ms = int(time.time() * 1000)
    window_ms = minutes * 60 * 1000
    cutoff_ms = now_ms - window_ms

    with open(journal_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            # ts_ms window filter
            ts_ms = entry.get("ts_ms", 0)
            if ts_ms > 0 and ts_ms < cutoff_ms:
                continue
            # V2 journal uses 'kind'; V1 uses 'event'. Support both.
            event = entry.get("kind", entry.get("event", ""))
            # V2 journal uses 'payload'; V1 uses 'data'. Support both.
            payload = entry.get("payload", entry.get("data", {}))

            readiness = payload.get("readiness_evidence", {})
            if not isinstance(readiness, dict):
                readiness = {}
            provider = str(
                payload.get("provider")
                or readiness.get("provider")
                or ""
            )

            if event == "entry.opened":
                counts["open_position_count"] += 1
            elif event == "order.passive_submitted":
                counts["order.passive_submitted"] += 1
            elif event == "order.uncertain":
                counts["order.uncertain"] += 1
                venue = payload.get("venue", "unknown")
                venue_reasons[f"uncertain:{venue}"] += 1
            elif event in {
                "runtime.entry_blocked_local_l2_selection",
                "runtime.entry_blocked_ws_bbo_selection",
            }:
                reason = payload.get("reason", "unknown")
                if (
                    event == "runtime.entry_blocked_ws_bbo_selection"
                    or provider == "ws_bbo_quote_lease"
                    or str(reason).startswith("entry_ws_bbo_quote_lease_")
                ):
                    counts["entry_blocked_ws_bbo_selection"] += 1
                    ws_bbo_selection_reasons[reason] += 1
                else:
                    counts["entry_blocked_local_l2_selection"] += 1
                    l2_selection_reasons[reason] += 1
            elif event == "runtime.entry_blocked_local_l2_not_ready":
                counts["entry_blocked_local_l2_not_ready"] += 1
                reasons = payload.get("reasons", [])
                if isinstance(reasons, str):
                    reasons = [reasons]
                for r in reasons:
                    l2_not_ready_reasons[r[:80]] += 1
            elif event == "runtime.local_l2_snapshot_error":
                counts["runtime.local_l2_snapshot_error"] += 1
                venue = payload.get("venue", "unknown")
                reason = payload.get("reason", "unknown")
                venue_reasons[f"snapshot_error:{venue}:{reason}"] += 1
            elif event == "entry.aborted":
                counts["entry.aborted"] += 1
            elif event == "pending_entry_count" or event == "state.pending_entries":
                pe_list = payload.get("pending_entries", payload.get("pendingEntries", []))
                if isinstance(pe_list, list):
                    counts["pending_entry_count"] = max(counts["pending_entry_count"], len(pe_list))
                elif isinstance(pe_list, dict):
                    counts["pending_entry_count"] = max(counts["pending_entry_count"], len(pe_list))

    return {
        "counts": dict(counts),
        "venue_reasons": dict(venue_reasons),
        "l2_selection_reasons": dict(l2_selection_reasons),
        "ws_bbo_selection_reasons": dict(ws_bbo_selection_reasons),
        "l2_not_ready_reasons": dict(l2_not_ready_reasons),
    }


def main():
    parser = argparse.ArgumentParser(description="V2 live dry-run audit")
    parser.add_argument("--minutes", type=int, default=120, help="Minutes of history to audit")
    parser.add_argument("--log", type=str, default="/var/log/lightfee-v2/live.log",
                        help="Path to journal/log file")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    result = audit(args.log, args.minutes)

    if not result:
        print("No data to audit.", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print("=== V2 Dry-Run Audit ===")
        print(f"Source: {args.log}")
        print()
        counts = result.get("counts", {})
        for key in sorted(counts):
            print(f"  {key}: {counts[key]}")
        print()
        print("Top Local L2 Selection Blockers:")
        for reason, count in sorted(result.get("l2_selection_reasons", {}).items(),
                                     key=lambda x: -x[1])[:10]:
            print(f"  {reason}: {count}")
        print()
        print("Top WS BBO Selection Blockers:")
        for reason, count in sorted(result.get("ws_bbo_selection_reasons", {}).items(),
                                     key=lambda x: -x[1])[:10]:
            print(f"  {reason}: {count}")
        print()
        print("Top Not-Ready Reasons:")
        for reason, count in sorted(result.get("l2_not_ready_reasons", {}).items(),
                                     key=lambda x: -x[1])[:10]:
            print(f"  {reason[:100]}: {count}")
        print()
        print("=== Acceptance ===")
        uncertain = counts.get("order.uncertain", 0)
        if uncertain == 0:
            print("  PASS: order.uncertain caused by ACK-only maker responses is zero")
        else:
            print(f"  WARN: order.uncertain count = {uncertain}")

        ws_bbo_selection = counts.get("entry_blocked_ws_bbo_selection", 0)
        if ws_bbo_selection:
            print(f"  NOTE: entry_blocked_ws_bbo_selection ({ws_bbo_selection})")

        l2_selection = counts.get("entry_blocked_local_l2_selection", 0)
        l2_not_ready = counts.get("entry_blocked_local_l2_not_ready", 0)
        if l2_not_ready < l2_selection:
            print(f"  PASS: entry_blocked_local_l2_not_ready ({l2_not_ready}) < selection blocks ({l2_selection})")
        else:
            print(f"  NOTE: l2_not_ready={l2_not_ready}, l2_selection={l2_selection}")

        snapshot_errors = counts.get("runtime.local_l2_snapshot_error", 0)
        if snapshot_errors == 0:
            print("  PASS: no L2 snapshot errors")
        else:
            print(f"  WARN: {snapshot_errors} L2 snapshot errors")


if __name__ == "__main__":
    main()
