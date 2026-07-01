#!/usr/bin/env python3
"""Context-safe LightFeeV2 diagnostics entrypoint."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lightfee.ops.diagnostics.reporting import render_budgeted_json
from scripts import diagnose_live


def _parse_venues(value: str) -> list[str] | None:
    venues = [venue.strip().lower() for venue in value.split(",") if venue.strip()]
    return venues or None


def _add_diagnose_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--profile",
        choices=["operator", "agent", "gate", "full"],
        default="agent",
        help="Output profile; defaults to context-safe agent JSON",
    )
    parser.add_argument("--symbol", type=str, default="", help="Filter by symbol")
    parser.add_argument(
        "--venues",
        type=str,
        default="",
        help="Comma-separated venues for exchange-truth checks",
    )
    parser.add_argument(
        "--since-deploy",
        action="store_true",
        default=False,
        help="Limit to events since last deploy",
    )
    parser.add_argument(
        "--runtime-dir",
        type=str,
        default=diagnose_live.DEFAULT_RUNTIME_DIR,
        help=f"Runtime directory (default: {diagnose_live.DEFAULT_RUNTIME_DIR})",
    )
    parser.add_argument(
        "--current-state",
        type=str,
        default="",
        help="Path to live-state-current.json (overrides default)",
    )
    parser.add_argument(
        "--events",
        type=str,
        nargs="*",
        default=None,
        help="Specific event file(s); auto-discovered if omitted",
    )
    parser.add_argument(
        "--snapshot",
        type=str,
        default="",
        help="Path to opportunity-input-snapshot.json",
    )
    parser.add_argument(
        "--unit-dir",
        type=str,
        default=diagnose_live.DEFAULT_UNIT_DIR,
        help=f"Systemd unit directory (default: {diagnose_live.DEFAULT_UNIT_DIR})",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=diagnose_live.DEFAULT_MAX_EVENTS,
        help=f"Max events to parse (default: {diagnose_live.DEFAULT_MAX_EVENTS})",
    )
    parser.add_argument("--now-ms", type=int, default=0, help="Override current time in ms")
    parser.add_argument(
        "--artifact-dir",
        type=str,
        default="",
        help="Optional directory for full artifacts when profile output exceeds budget",
    )


def _run_diagnose(args: argparse.Namespace) -> int:
    result = diagnose_live.run_diagnose(
        runtime_dir=args.runtime_dir,
        unit_dir=args.unit_dir,
        current_state_path=args.current_state,
        event_paths=args.events,
        snapshot_path=args.snapshot,
        symbol=args.symbol,
        max_events=args.max_events,
        now_ms=args.now_ms,
        since_deploy=args.since_deploy,
        venues=_parse_venues(args.venues),
    )
    sys.stdout.write(
        render_budgeted_json(
            result,
            profile=args.profile,
            artifact_dir=args.artifact_dir or None,
            artifact_name=f"lfdiag-diagnose-{args.profile}.json",
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    diagnose_parser = subparsers.add_parser(
        "diagnose",
        help="Run read-only live diagnostics through a budgeted output profile",
    )
    _add_diagnose_args(diagnose_parser)
    diagnose_parser.set_defaults(handler=_run_diagnose)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
