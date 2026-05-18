"""lightfee-ops: operator control CLI entrypoint.

V1 parity: reads persisted state/snapshot → applies command →
writes journal (append_critical) → persists snapshot → reports result.

Rust references:
- src/engine/state.rs:769-868 (apply_operator_command with atomic persistence)
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from lightfee.risk.operator import OperatorCommand


def _resolve_paths() -> tuple[Path, Path]:
    """Resolve journal and snapshot paths from environment or defaults."""
    data_dir = os.environ.get("LIGHTFEE_DATA_DIR", "data")
    base = Path(data_dir)
    journal_path = base / "journal.jsonl"
    snapshot_path = base / "snapshot.json"
    return journal_path, snapshot_path


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

    journal_path, snapshot_path = _resolve_paths()

    # 1. Load persisted state from snapshot (lifecycle + risk_mode)
    from lightfee.engine.recovery import _restore_state_from_snapshot_dict
    from lightfee.persistence.snapshot_store import SnapshotStore

    store = SnapshotStore(snapshot_path)
    snap = store.read()
    if snap is not None:
        state = _restore_state_from_snapshot_dict(snap)
    else:
        from lightfee.engine.state import EngineState
        state = EngineState()

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
