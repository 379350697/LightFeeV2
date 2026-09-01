"""lightfee-ops: operator control CLI entrypoint.

V1 parity: reads persisted state/snapshot → applies command →
writes journal (append_critical) → persists snapshot → reports result.

Rust references:
- src/engine/state.rs:769-868 (apply_operator_command with atomic persistence)
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from lightfee.risk.operator import OperatorCommand


def _resolve_paths(
    *,
    event_log_path: Path | None = None,
    snapshot_path: Path | None = None,
) -> tuple[Path, Path]:
    """Require the exact event-log/snapshot pair for a durable mutation."""
    if (event_log_path is None) != (snapshot_path is None):
        raise ValueError("--event-log-path and --snapshot-path must be supplied together")
    if event_log_path is not None and snapshot_path is not None:
        return event_log_path, snapshot_path
    raise ValueError(
        "state-mutating commands require --event-log-path and --snapshot-path"
    )


def _add_persistence_path_args(parser: argparse.ArgumentParser) -> None:
    """Require the deployed persistence pair instead of deriving unsafe defaults."""
    parser.add_argument(
        "--event-log-path",
        required=True,
        type=Path,
        help="Configured live persistence event_log_path",
    )
    parser.add_argument(
        "--snapshot-path",
        required=True,
        type=Path,
        help="Configured live persistence snapshot_path",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="lightfee-ops: Operator controls")
    sub = parser.add_subparsers(dest="command")

    for command, help_text in (
        ("pause-entry", "Pause new entries"),
        ("reduce-only", "Enter reduce-only mode"),
        ("fail-closed", "Enter fail-closed mode"),
        ("reconcile-now", "Trigger immediate reconciliation"),
        ("resume-if-safe", "Resume if no blocking recovery work"),
    ):
        command_parser = sub.add_parser(command, help=help_text)
        _add_persistence_path_args(command_parser)
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
    _add_persistence_path_args(billing_import)
    candidate_discovery = sub.add_parser(
        "discover-binance-close-evidence",
        help="Read-only candidate discovery for a Binance close-identity evidence debt",
    )
    candidate_discovery.add_argument(
        "--snapshot-path",
        required=True,
        type=Path,
        help="Persisted snapshot containing the exact evidence-debt owner",
    )
    candidate_discovery.add_argument(
        "--orders-file",
        required=True,
        type=Path,
        help="Read-only Binance allOrders JSON export (a list or {orders: [...]})",
    )
    candidate_discovery.add_argument("--position-id", required=True)
    candidate_discovery.add_argument("--kind", choices=("final", "partial"), required=True)
    candidate_discovery.add_argument("--closed-at-ms", required=True, type=int)
    candidate_discovery.add_argument("--time-window-ms", default=300_000, type=int)
    candidate_discovery.add_argument(
        "--quantity-relative-tolerance",
        default=1e-9,
        type=float,
    )

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    # This branch deliberately exits before path-pair resolution, writer-lease
    # acquisition, journal opening, or snapshot writing.  Candidate discovery
    # can narrow an offline investigation, but it must never mutate live state
    # or manufacture an accounting fact from a fuzzy historical match.
    if args.command == "discover-binance-close-evidence":
        from lightfee.engine.recovery import _restore_state_from_snapshot_dict
        from lightfee.ops.commands import (
            discover_binance_close_evidence_candidates,
            load_binance_order_history_export,
        )
        from lightfee.persistence.snapshot_store import SnapshotStore

        snapshot = SnapshotStore(args.snapshot_path).read()
        if snapshot is None:
            parser.error("snapshot does not exist or is empty")
        state = _restore_state_from_snapshot_dict(snapshot)
        candidates = [
            reconciliation
            for reconciliation in state.pending_close_reconciliations
            if (
                str(reconciliation.get("position_id") or "") == args.position_id
                and str(reconciliation.get("kind") or "") == args.kind
                and str(reconciliation.get("closed_at_ms") or "") == str(args.closed_at_ms)
                and reconciliation.get("reconciliation_status") == "evidence_debt"
            )
        ]
        if len(candidates) != 1:
            parser.error(
                "expected exactly one evidence_debt reconciliation matching "
                "--position-id/--kind/--closed-at-ms"
            )
        try:
            orders = load_binance_order_history_export(args.orders_file)
            result = discover_binance_close_evidence_candidates(
                candidates[0],
                orders,
                time_window_ms=args.time_window_ms,
                quantity_relative_tolerance=args.quantity_relative_tolerance,
            )
        except ValueError as exc:
            parser.error(str(exc))
        print(json.dumps(result, sort_keys=True))
        sys.exit(0)

    commands = {
        "pause-entry": OperatorCommand.PAUSE_ENTRY,
        "reduce-only": OperatorCommand.REDUCE_ONLY,
        "fail-closed": OperatorCommand.FAIL_CLOSED,
        "reconcile-now": OperatorCommand.RECONCILE_NOW,
        "resume-if-safe": OperatorCommand.RESUME_IF_SAFE,
    }
    if args.command == "import-billing-evidence" and not args.apply:
        parser.error("import-billing-evidence requires --apply")
    journal_path, snapshot_path = _resolve_paths(
        event_log_path=args.event_log_path,
        snapshot_path=args.snapshot_path,
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
        from lightfee.engine.recovery import (
            _restore_state_from_snapshot_dict,
            has_lifecycle_blocking_work,
        )
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

            has_blocking = has_lifecycle_blocking_work(state)
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
        view = build_persistent_state_view(
            state,
            journal_checkpoint=journal.snapshot_checkpoint(),
        )
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
