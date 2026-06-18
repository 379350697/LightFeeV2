"""lightfee-ops: operator control CLI entrypoint.

V1 parity: reads persisted state/snapshot → applies command →
writes journal (append_critical) → persists snapshot → reports result.

Rust references:
- src/engine/state.rs:769-868 (apply_operator_command with atomic persistence)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from lightfee.risk.operator import OperatorCommand


def _resolve_paths(
    *,
    journal_path: str | None = None,
    snapshot_path: str | None = None,
) -> tuple[Path, Path]:
    """Resolve journal and snapshot paths from environment or defaults."""
    data_dir = os.environ.get("LIGHTFEE_DATA_DIR", "data")
    base = Path(data_dir)
    resolved_journal_path = Path(journal_path) if journal_path else base / "journal.jsonl"
    resolved_snapshot_path = Path(snapshot_path) if snapshot_path else base / "snapshot.json"
    return resolved_journal_path, resolved_snapshot_path


def _load_json_file(path: str | None) -> dict | None:
    if not path:
        return None
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"expected JSON object at {path}")
    return data


def main() -> None:
    parser = argparse.ArgumentParser(description="lightfee-ops: Operator controls")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("pause-entry", help="Pause new entries")
    sub.add_parser("reduce-only", help="Enter reduce-only mode")
    sub.add_parser("fail-closed", help="Enter fail-closed mode")
    sub.add_parser("reconcile-now", help="Trigger immediate reconciliation")
    sub.add_parser("resume-if-safe", help="Resume if no blocking recovery work")
    repair = sub.add_parser(
        "repair-auto-fail-closed-latch",
        help="Dry-run or repair a stale auto fail-closed operator latch",
    )
    repair.add_argument(
        "--apply",
        action="store_true",
        help="Apply only when classified as safe_to_repair_auto_latch",
    )
    repair.add_argument(
        "--exchange-truth",
        help="Path to high-confidence exchange truth JSON; defaults to snapshot exchange_truth",
    )
    repair.add_argument(
        "--journal-path",
        help="Explicit journal path for production repairs; defaults to LIGHTFEE_DATA_DIR/journal.jsonl",
    )
    repair.add_argument(
        "--snapshot-path",
        help="Explicit snapshot path for production repairs; defaults to LIGHTFEE_DATA_DIR/snapshot.json",
    )
    align = sub.add_parser(
        "repair-stale-risk-state",
        help="Dry-run or align stale risk_only lifecycle from clean exchange truth",
    )
    align.add_argument(
        "--apply",
        action="store_true",
        help="Apply only when classified as safe_to_align_stale_risk_state",
    )
    align.add_argument(
        "--exchange-truth",
        help="Path to high-confidence exchange truth JSON; defaults to snapshot exchange_truth",
    )
    align.add_argument(
        "--journal-path",
        help="Explicit journal path for production repairs; defaults to LIGHTFEE_DATA_DIR/journal.jsonl",
    )
    align.add_argument(
        "--snapshot-path",
        help="Explicit snapshot path for production repairs; defaults to LIGHTFEE_DATA_DIR/snapshot.json",
    )

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    journal_path, snapshot_path = _resolve_paths(
        journal_path=getattr(args, "journal_path", None),
        snapshot_path=getattr(args, "snapshot_path", None),
    )

    # Load persisted state from snapshot for both operator commands and repairs.
    from lightfee.engine.recovery import _restore_state_from_snapshot_dict
    from lightfee.persistence.snapshot_store import SnapshotStore

    store = SnapshotStore(snapshot_path)
    snap = store.read()
    if snap is not None:
        state = _restore_state_from_snapshot_dict(snap)
    else:
        from lightfee.engine.state import EngineState
        state = EngineState()

    if args.command == "repair-auto-fail-closed-latch":
        from lightfee.engine.bootstrap import wall_clock_now_ms
        from lightfee.engine.recovery import build_persistent_state_view
        from lightfee.ops.auto_fail_closed_repair import repair_auto_fail_closed_latch
        from lightfee.persistence.journal import Journal

        exchange_truth = _load_json_file(args.exchange_truth) if args.exchange_truth else None
        if exchange_truth is None and isinstance(snap, dict):
            maybe_truth = snap.get("exchange_truth")
            exchange_truth = maybe_truth if isinstance(maybe_truth, dict) else None

        journal_reader = Journal(journal_path)
        events = journal_reader.read_all()
        journal = Journal(journal_path)
        try:
            if args.apply:
                journal.open()
            result = repair_auto_fail_closed_latch(
                state,
                journal_events=events,
                exchange_truth=exchange_truth,
                apply=bool(args.apply),
                journal=journal if args.apply else None,
                ts_ms=wall_clock_now_ms() if args.apply else None,
            )
            if result.get("applied"):
                store.write(build_persistent_state_view(state))
            print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
            sys.exit(0 if (not args.apply or result.get("applied")) else 2)
        except Exception as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(1)
        finally:
            journal.close()

    if args.command == "repair-stale-risk-state":
        from lightfee.engine.bootstrap import wall_clock_now_ms
        from lightfee.engine.recovery import build_persistent_state_view
        from lightfee.ops.auto_fail_closed_repair import repair_stale_risk_state_alignment
        from lightfee.persistence.journal import Journal

        exchange_truth = _load_json_file(args.exchange_truth) if args.exchange_truth else None
        if exchange_truth is None and isinstance(snap, dict):
            maybe_truth = snap.get("exchange_truth")
            exchange_truth = maybe_truth if isinstance(maybe_truth, dict) else None

        journal_reader = Journal(journal_path)
        events = journal_reader.read_all()
        journal = Journal(journal_path)
        try:
            if args.apply:
                journal.open()
            result = repair_stale_risk_state_alignment(
                state,
                journal_events=events,
                exchange_truth=exchange_truth,
                apply=bool(args.apply),
                journal=journal if args.apply else None,
                ts_ms=wall_clock_now_ms() if args.apply else None,
            )
            if result.get("applied"):
                store.write(build_persistent_state_view(state))
            print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
            sys.exit(0 if (not args.apply or result.get("applied")) else 2)
        except Exception as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(1)
        finally:
            journal.close()

    commands = {
        "pause-entry": OperatorCommand.PAUSE_ENTRY,
        "reduce-only": OperatorCommand.REDUCE_ONLY,
        "fail-closed": OperatorCommand.FAIL_CLOSED,
        "reconcile-now": OperatorCommand.RECONCILE_NOW,
        "resume-if-safe": OperatorCommand.RESUME_IF_SAFE,
    }
    cmd = commands[args.command]

    # 2. Open journal for critical append
    from lightfee.persistence.journal import Journal

    journal = Journal(journal_path)
    journal.open()

    try:
        # 3. Execute operator command with real state + journal
        from lightfee.ops.commands import execute_operator_command

        has_blocking = bool(state.recovery_blocked_reason)
        new_risk, new_lifecycle, msg = execute_operator_command(
            command=cmd,
            current_risk=state.risk_mode,
            current_lifecycle=state.lifecycle,
            has_blocking_recovery=has_blocking,
            journal=journal,
            state=state,
        )

        # 4. Persist updated state to snapshot
        from lightfee.engine.recovery import build_persistent_state_view
        view = build_persistent_state_view(state)
        store.write(view)

        print(msg)
        sys.exit(0)

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    finally:
        journal.close()


if __name__ == "__main__":
    main()
