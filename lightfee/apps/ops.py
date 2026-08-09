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


def _resolve_paths(
    *,
    event_log_path: Path | None = None,
    snapshot_path: Path | None = None,
) -> tuple[Path, Path]:
    """Resolve a matching event-log/snapshot pair from flags or defaults."""
    if (event_log_path is None) != (snapshot_path is None):
        raise ValueError("--event-log-path and --snapshot-path must be supplied together")
    if event_log_path is not None and snapshot_path is not None:
        return event_log_path, snapshot_path
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
    billing_import = sub.add_parser(
        "import-billing-evidence",
        help="Import one typed close-accounting evidence pack for exchange reconciliation",
    )
    billing_import.add_argument("--file", required=True, type=Path)
    billing_import.add_argument(
        "--apply",
        action="store_true",
        help="Acknowledge the durable journal and snapshot mutation",
    )
    billing_import.add_argument(
        "--event-log-path",
        type=Path,
        help="Configured live persistence event_log_path (requires --snapshot-path)",
    )
    billing_import.add_argument(
        "--snapshot-path",
        type=Path,
        help="Configured live persistence snapshot_path (requires --event-log-path)",
    )

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
    if args.command == "import-billing-evidence" and not args.apply:
        parser.error("import-billing-evidence requires --apply")
    if args.command == "import-billing-evidence" and (
        getattr(args, "event_log_path", None) is None
    ) != (getattr(args, "snapshot_path", None) is None):
        parser.error("--event-log-path and --snapshot-path must be supplied together")

    journal_path, snapshot_path = _resolve_paths(
        event_log_path=getattr(args, "event_log_path", None),
        snapshot_path=getattr(args, "snapshot_path", None),
    )
    from lightfee.persistence.writer_lease import (
        PersistenceWriterLease,
        PersistenceWriterLeaseError,
    )

    # All commands below append a critical journal event and rewrite the
    # snapshot.  Keep one writer boundary for the entire control plane, not
    # only for the billing-evidence command.
    writer_lease = PersistenceWriterLease(journal_path)
    try:
        writer_lease.acquire()
    except PersistenceWriterLeaseError as exc:
        writer_lease.release()
        parser.error(str(exc))

    journal = None
    try:
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

        # 3. Apply one durable control-plane mutation to real state + journal.
        if args.command == "import-billing-evidence":
            from lightfee.engine.bootstrap import wall_clock_now_ms
            from lightfee.ops.commands import (
                execute_billing_evidence_import,
                load_billing_evidence_import,
            )

            evidence = load_billing_evidence_import(args.file)
            msg = execute_billing_evidence_import(
                evidence,
                journal=journal,
                state=state,
                now_ms=wall_clock_now_ms(),
            )
        else:
            from lightfee.ops.commands import execute_operator_command

            has_blocking = bool(state.recovery_blocked_reason)
            _new_risk, _new_lifecycle, msg = execute_operator_command(
                command=commands[args.command],
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
        if journal is not None:
            journal.close()
        writer_lease.release()


if __name__ == "__main__":
    main()
