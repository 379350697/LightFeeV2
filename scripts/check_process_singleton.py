#!/usr/bin/env python3
"""Process singleton enforcement for LightFeeV2 sidecar and live runtime.

Cloud incident: 8 Rust opportunity_input_sidecar processes were simultaneously
writing to runtime/opportunity-input-snapshot.json, causing snapshot corruption.

This script:
1. Checks exactly one funding sidecar, spread sidecar, and live process
2. Fails when the retired spread-BBO process is still running
2. Can be used as a pre-start check in deployment scripts
3. Verifies no zombie/stale processes are writing to the snapshot file

Usage:
  python scripts/check_process_singleton.py [--strict]
  python scripts/check_process_singleton.py --kill-extra  # DANGER: kills extras
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time


# Process name patterns to count
SIDECAR_PATTERNS = [
    "opportunity_input_sidecar",
    "lightfee.apps.sidecar",
    "lightfee-sidecar",
    "lightfee_sidecar",
]

SPREAD_SIDECAR_PATTERNS = [
    "lightfee.apps.spread_sidecar",
    "lightfee-spread-sidecar",
    "lightfee_spread_sidecar",
    "spread_sidecar",
]

RETIRED_SPREAD_BBO_PATTERNS = [
    "lightfee.apps.spread_bbo",
    "lightfee-spread-bbo",
    "lightfee_spread_bbo",
    "spread_bbo",
]

LIVE_PATTERNS = [
    "lightfee.apps.live",
    "lightfee-live",
    "lightfee_live",
    "lightfee run",
]


def get_process_list() -> list[dict]:
    """Get process list with PID, command, and snapshot file access info."""
    try:
        # Use ps to get all processes
        result = subprocess.run(
            ["ps", "aux"],
            capture_output=True,
            text=True,
        )
        lines = result.stdout.strip().split("\n")
        if len(lines) < 2:
            return []

        processes = []
        for line in lines[1:]:
            parts = line.split(None, 10)
            if len(parts) < 11:
                continue
            processes.append(
                {
                    "user": parts[0],
                    "pid": int(parts[1]),
                    "cpu": parts[2],
                    "mem": parts[3],
                    "command": parts[10],
                }
            )
        return processes
    except Exception as e:
        print(f"ERROR: ps aux failed: {e}", file=sys.stderr)
        return []


def check_lsof_snapshot_writers() -> list[int]:
    """Find PIDs with open file handles to opportunity-input-snapshot.json."""
    writers = []
    snapshot_paths = [
        "runtime/opportunity-input-snapshot.json",
        "/opt/lightfee-v2/runtime/opportunity-input-snapshot.json",
        "runtime/spread-opportunities-current.json",
        "/opt/lightfee-v2/runtime/spread-opportunities-current.json",
    ]
    for sp in snapshot_paths:
        try:
            result = subprocess.run(
                ["lsof", "-t", sp],
                capture_output=True,
                text=True,
            )
            for line in result.stdout.strip().split("\n"):
                if line.strip().isdigit():
                    writers.append(int(line.strip()))
        except Exception:
            pass
    return list(set(writers))


def count_matching(processes: list[dict], patterns: list[str]) -> list[dict]:
    """Count processes whose command line matches any pattern."""
    matches = []
    for proc in processes:
        cmd = proc["command"].lower()
        for pat in patterns:
            if pat.lower() in cmd:
                matches.append(proc)
                break
    return matches


def check_singleton(
    label: str,
    matches: list[dict],
    max_allowed: int = 1,
    min_required: int = 0,
) -> bool:
    """Check a bounded process count, optionally requiring the service alive."""
    count = len(matches)
    if count < min_required:
        print(f"  {label}: {count} processes (VIOLATION — min {min_required})")
        return False
    if count == 0:
        print(f"  {label}: 0 processes (not running)")
        return True
    if count <= max_allowed:
        proc = matches[0]
        print(f"  {label}: 1 process (PID={proc['pid']}, cmd={proc['command'][:80]})")
        return True
    print(f"  {label}: {count} processes (VIOLATION — max {max_allowed}):")
    for proc in matches:
        print(f"    PID={proc['pid']}  {proc['command'][:100]}")
    return False


def kill_extras(label: str, matches: list[dict], keep_count: int = 1) -> int:
    """Kill extra processes beyond keep_count. Returns number killed."""
    if len(matches) <= keep_count:
        return 0

    killed = 0
    # Sort by PID (newest first = highest PID, keep oldest)
    matches_sorted = sorted(matches, key=lambda p: p["pid"], reverse=True)
    for proc in matches_sorted[keep_count:]:
        try:
            os.kill(proc["pid"], 15)  # SIGTERM
            print(f"  KILLED PID={proc['pid']} ({label})")
            killed += 1
        except Exception as e:
            print(f"  FAILED to kill PID={proc['pid']}: {e}")

    return killed


def main() -> None:
    parser = argparse.ArgumentParser(description="Process singleton enforcement for LightFeeV2")
    parser.add_argument(
        "--strict", action="store_true", help="Exit with error if any violation found"
    )
    parser.add_argument(
        "--kill-extra",
        action="store_true",
        help="Kill extra processes beyond the singleton limit (DANGEROUS)",
    )
    parser.add_argument(
        "--snapshot-path",
        type=str,
        default="runtime/opportunity-input-snapshot.json",
        help="Path to check for snapshot file writers",
    )
    args = parser.parse_args()

    print("=== LightFeeV2 Process Singleton Check ===")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    processes = get_process_list()
    print(f"Total processes: {len(processes)}")

    # Check sidecar processes
    print("\n--- Sidecar Processes ---")
    sidecars = count_matching(processes, SIDECAR_PATTERNS)
    required_min = 1 if args.strict else 0
    sidecar_ok = check_singleton("sidecar", sidecars, min_required=required_min)

    # Check spread sidecar processes
    print("\n--- Spread Sidecar Processes ---")
    spread_sidecars = count_matching(processes, SPREAD_SIDECAR_PATTERNS)
    spread_sidecar_ok = check_singleton(
        "spread-sidecar", spread_sidecars, min_required=required_min
    )

    # The BBO process was retired when sampling returned to the funding
    # sidecar.  It is an unexpected fourth process, not a second owner.
    retired_spread_bbos = count_matching(processes, RETIRED_SPREAD_BBO_PATTERNS)
    retired_spread_bbo_ok = check_singleton(
        "retired-spread-bbo", retired_spread_bbos, max_allowed=0
    )

    # Check live runtime processes
    print("\n--- Live Runtime Processes ---")
    lives = count_matching(processes, LIVE_PATTERNS)
    live_ok = check_singleton("live", lives, min_required=required_min)

    # Check snapshot writers
    print("\n--- Snapshot Writers ---")
    writers = check_lsof_snapshot_writers()
    if writers:
        writer_procs = [p for p in processes if p["pid"] in writers]
        print(f"  Processes writing to snapshot: {len(writers)}")
        for wp in writer_procs:
            print(f"    PID={wp['pid']}  {wp['command'][:100]}")
        if len(writers) > 2:  # 1 sidecar write + 1 live read = 2 max
            print(f"  WARNING: {len(writers)} processes have snapshot file open")
    else:
        print("  No snapshot file writers detected")

    # Summary
    print("\n=== Summary ===")
    all_ok = sidecar_ok and spread_sidecar_ok and retired_spread_bbo_ok and live_ok
    status = "PASS" if all_ok else "FAIL"
    print(f"Status: {status}")
    print(f"  sidecar processes:        {len(sidecars)} (limit: 1)")
    print(f"  spread-sidecar processes: {len(spread_sidecars)} (limit: 1)")
    print(f"  retired spread-bbo processes: {len(retired_spread_bbos)} (limit: 0)")
    print(f"  live processes:           {len(lives)} (limit: 1)")
    print(f"  snapshot writers:         {len(writers)}")

    # Handle --kill-extra
    if args.kill_extra and not all_ok:
        print("\n=== Killing Extra Processes ===")
        if len(sidecars) > 1:
            kill_extras("sidecar", sidecars)
        if len(spread_sidecars) > 1:
            kill_extras("spread-sidecar", spread_sidecars)
        if retired_spread_bbos:
            kill_extras("retired-spread-bbo", retired_spread_bbos, keep_count=0)
        if len(lives) > 1:
            kill_extras("live", lives)

        # Re-check
        time.sleep(1)
        processes2 = get_process_list()
        sidecars2 = count_matching(processes2, SIDECAR_PATTERNS)
        spread_sidecars2 = count_matching(processes2, SPREAD_SIDECAR_PATTERNS)
        retired_spread_bbos2 = count_matching(
            processes2, RETIRED_SPREAD_BBO_PATTERNS
        )
        lives2 = count_matching(processes2, LIVE_PATTERNS)
        required_counts = {1} if args.strict else {0, 1}
        all_ok = all(
            count in required_counts
            for count in (
                len(sidecars2),
                len(spread_sidecars2),
                len(retired_spread_bbos2),
                len(lives2),
            )
        )
        print(
            f"\nAfter cleanup: sidecar={len(sidecars2)}, "
            f"spread-sidecar={len(spread_sidecars2)}, "
            f"retired-spread-bbo={len(retired_spread_bbos2)}, "
            f"live={len(lives2)}"
        )
        print(f"Status: {'PASS' if all_ok else 'FAIL (manual intervention needed)'}")

    if args.strict and not all_ok:
        sys.exit(1)

    sys.exit(0 if all_ok else 0)  # Always exit 0 unless --strict


if __name__ == "__main__":
    main()
