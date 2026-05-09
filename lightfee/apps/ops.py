"""lightfee-ops: operator control CLI entrypoint."""

from __future__ import annotations

import argparse
import sys

from lightfee.risk.operator import OperatorCommand


def main() -> None:
    parser = argparse.ArgumentParser(description="lightfee-ops: Operator controls")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("pause-entry", help="Pause new entries")
    sub.add_parser("reduce-only", help="Enter reduce-only mode")
    sub.add_parser("fail-closed", help="Enter fail-closed mode")
    sub.add_parser("reconcile-now", help="Trigger immediate reconciliation")
    sub.add_parser("resume-if-safe", help="Resume if no blocking recovery work")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        "pause-entry": OperatorCommand.PAUSE_ENTRY,
        "reduce-only": OperatorCommand.REDUCE_ONLY,
        "fail-closed": OperatorCommand.FAIL_CLOSED,
        "reconcile-now": OperatorCommand.RECONCILE_NOW,
        "resume-if-safe": OperatorCommand.RESUME_IF_SAFE,
    }
    cmd = commands[args.command]
    print(f"Operator command: {cmd.value}")
