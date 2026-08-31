# Bug Card: Recovery Snapshot / Journal Boundary

## Stable Fingerprints

- `recovery.snapshot_full_journal_replay_stale_owner_resurrection`
- `pending_entry.removed_by_v1_lifecycle_closure`
- A clean snapshot becomes `reconciling` solely after retained historical
  `entry.pending_registered` or `entry.opened` records are replayed.
- Startup recovery probes symbols absent from the current snapshot because an
  old owner CID is retained in the journal.

## Current Effective Rule

- A durable persistent snapshot is the state-recovery boundary.
- Every production snapshot writer, including the live runtime and
  operator-control CLI, records journal device, inode, and byte offset
  immediately before its atomic write.
- Restart recovery reads only the contiguous retained journal tail after that
  boundary. It supports normal journal rotation while the exact chain remains
  available.
- A legacy, missing, malformed, replaced, or discontinuous marker reads no
  journal history. The snapshot is still restored and existing exchange truth
  reconciliation remains the live-position/order safety boundary.
- If no snapshot exists, full journal replay remains the emergency recovery
  path.
- The V1 pending-entry terminal closure removes its matching pending owner
  evidence before the startup owner index can make recovery queries from it.
  Terminal close order evidence stays available for accounting reconciliation.

## V1 / Exchange Semantics

V1 makes durable current state—not retained audit history—the ordinary restart
authority. V2 must preserve the no-snapshot emergency recovery fallback but
must not turn an audit-retention log into a second, competing state store.
Exchange account truth outranks both local snapshot and journal for physical
positions and open orders; a missing journal tail must therefore reconcile or
fail closed, never invent historical work.

## Attempts Ledger

| Date | Attempt | Status | Why |
|---|---|---|---|
| 2026-08-31 | Replay every retained record after snapshot restore | ineffective | Retained audit history recreated 29 stale pending owners and startup symbol probes. |
| 2026-08-31 | Use journal sequence as restart cursor | rejected | Production sequences reset across process runs; it is not durable. |
| 2026-08-31 | Suppress every terminal owner event | rejected | A flat position can still require exact order/fee evidence for accounting. |
| 2026-08-31 | File identity/byte-offset checkpoint plus contiguous tail | local-green | Covers the real snapshot boundary and rotation; impossible continuity fails safe to snapshot/account truth. |
| 2026-08-31 | Let the operator CLI write an unmarked snapshot | rejected | A later crash before the next runtime snapshot would silently skip its journal tail as legacy. |

## Recurrences

| Date | Symbols / Venues | Commit / Fix | Result | Detail |
|---|---|---|---|---|
| 2026-08-31 | historical all-venue audit events | working tree | local-green | [CL-137](../daily/2026-08-31.md#cl-137--snapshot-recovery-replayed-retained-audit-history-as-live-work) |

## Regression Harness

- `tests/persistence/test_journal_event_semantics.py`: exact tail, UTF-8
  offset, rotation continuity, and unsafe-checkpoint rejection.
- `tests/recovery/test_restart_recovery_semantics.py`: no-snapshot emergency
  replay, legacy snapshot authority, post-snapshot tail, and lifecycle closure.
- `tests/ops/test_billing_evidence_import.py`: operator CLI snapshot followed
  by a real journal tail and restart recovery.
- `tests/engine/test_recovery_owner_index.py` and
  `tests/test_live_startup_preflight.py`: terminal pending-owner evidence
  cannot expand startup recovery probes while terminal close order evidence
  remains available for accounting.
- `tests/test_pending_entry_v1_semantic_drift.py`,
  `tests/test_live_entry_hedge_root_fix.py`, and `tests/test_passive_close.py`:
  crash-window tails remain recoverable.

## Next Recurrence Checklist

1. Read snapshot metadata and the current/rotated journal identity before
   changing recovery code.
2. Determine whether the reported event is before or after the snapshot
   boundary; do not start with a full journal scan.
3. Confirm whether a terminal event clears the same owner identifiers that its
   registration used.
4. Compare V1 snapshot recovery behavior before changing recovery semantics.
5. Re-run no-snapshot, legacy-snapshot, tail-register, tail-clear, rotation,
   discontinuity, and exchange-truth counterexamples.
6. After deployment, use one read-only restart probe; never create orders or
   manually delete history to force acceptance.
