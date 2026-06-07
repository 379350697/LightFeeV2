# V1 Pending Close Reconciliation Queue Closure Design

Date: 2026-06-07
Status: Implemented locally; deployment/read-only production verification pending
Scope: CL-051 post-`bff33ec` production recurrence covering passive-close
live-flat cleanup, pending-close reconciliation queue shape, and same-window
unpaired live-artifact recovery coverage.

## Goal

Close the post-deploy recurrence as one V1-compatible contract gap, not as a
symbol patch. The implementation must restore the V1 pending-close
reconciliation queue boundary in Python, prevent observable half-complete
passive-close terminal cleanup, and prove that same-window unpaired exchange
live artifacts remain owned/recovered/fail-safe.

## Implementation Status

Local implementation now covers PC-06, PC-07, and PC-08 with RED/GREEN tests:

- PC-06: queue restore/serialization/enqueue/remove normalize keyed maps,
  single-task dicts, and invalid non-dict evidence into a canonical list.
- PC-07: live-flat cleanup journals terminal/drift/recovery success only after
  registration, managed-state removal, last-error cleanup, and core clear
  complete; failures retain managed state and emit cleanup-failed evidence.
- PC-08: reconciliation processor, entry gate, and supervisor venue collection
  normalize queue data and preserve recoverable symbol/venue evidence.

Remaining caveat: this pass did not deploy, mutate production state, submit or
cancel orders, or run service-env read-only production verification. The
post-deploy acceptance criteria remain pending until a separate deploy/check
pass is explicitly requested.

## Source Of Truth

- V1 typed queue:
  `/Users/wl/projects/LightFee/src/engine/state.rs`
  - `pending_close_reconciliations: Vec<PendingCloseReconciliation>`
  - `enqueue_pending_close_reconciliation`
  - `remove_pending_close_reconciliation`
- V1 reconciliation processor:
  `/Users/wl/projects/LightFee/src/engine/recovery.rs`
  - `process_pending_close_reconciliations`
  - `try_abandon_stale_pending_close_reconciliation`
- V1 close exit flow:
  `/Users/wl/projects/LightFee/src/engine/exit.rs`
  - `finalize_close_position_execution`
  - close accounting registration after managed-state mutation
- V2 current boundaries:
  - `lightfee/engine/state.py::EngineState`
  - `lightfee/engine/recovery.py::_restore_state_from_snapshot_dict`
  - `lightfee/engine/passive_close.py::_clear_live_flat_state`
  - `lightfee/engine/passive_close.py::_register_close_reconciliation_after_live_flat`
  - `lightfee/engine/runtime.py::_process_pending_close_reconciliations`
  - `lightfee/engine/supervisor.py::supervised_venues`
  - `lightfee/engine/runtime.py::_gate_pending_close_reconciliation`
- Contract ledger:
  - `docs/bugs/contracts/pending-entry-live-truth-contract.md`
  - `docs/bugs/daily/2026-06-07.md`
  - `docs/bugs/cards/passive-close-terminal-flatness.md`

## Production Evidence Covered

| Evidence | Covered by this spec? | Contract row / action |
|---|---:|---|
| deploy marker and services are healthy but runtime health is red | yes | pre-flight and post-deploy verification, not a code fix |
| `BABYUSDT` local open/passive state while OKX/Bybit exchange truth is flat | yes | PC-06, PC-07 |
| repeated `runtime.passive_close_tick_error` with `"'dict' object has no attribute 'append'"` | yes | PC-06 restore/persist queue shape |
| repeated terminal-flat/drift events without local state removal | yes | PC-07 cleanup atomicity |
| Bybit close order accepted but no fill fields | yes | pending-close reconciliation must retain until fill history or terminal live-flat evidence |
| `exit.passive_close_hedge_deadline_fail_closed` with unhedged gap | yes | passive-close cleanup must not claim terminality until state/core clear succeeds |
| Bybit unpaired live positions on `MORPHOUSDT`, `MONUSDT`, `SEIUSDT` | yes | live-artifact owner/recovery fixture coverage in this spec, shared with recovery-ledger contract |
| `runtime.snapshot_degraded`, `runtime.snapshot_fallback_last_good`, quote stale skips | adjacent only | evidence watch; not root of queue-shape crash or unpaired live artifact |

This spec does not claim that market-data degradation is solved. It requires the
implementation to keep those warnings visible and to prove they are not used as
flat/live-artifact evidence.

## Problem Statement

V1's `pending_close_reconciliations` cannot become a map at runtime because it
is restored and held as `Vec<PendingCloseReconciliation>`. V2 has the same
semantic field, but it is a Python raw list and restore currently assigns the
snapshot value directly. A persisted dict-shaped value can therefore reach
passive-close cleanup.

The current V2 live-flat cleanup path can then:

```text
prove BABYUSDT close venues flat
  -> journal terminal-flat / drift events
  -> call state.pending_close_reconciliations.append(...)
  -> throw AttributeError because the field is dict-shaped
  -> skip pending_passive_closes/open_positions removal
  -> skip V1RecoveryDecisionCore clear
  -> repeat on the next tick
```

That is a pre-core closure hole. The V1 recovery decision core may be correct
after evidence reaches it, but this failure prevents evidence from reaching it.

## Required Contract

### PC-06: Canonical Queue Shape

`EngineState.pending_close_reconciliations` must always be a canonical list of
dict-like reconciliation tasks at runtime and at serialization boundaries.

Allowed restore behavior:

- missing field -> empty list.
- list of dict tasks -> normalized list.
- dict/map legacy shape -> deterministic migration into a list when values are
  valid tasks, otherwise explicit invalid/migration evidence and fail-safe
  retention policy.
- non-list/non-dict garbage -> explicit invalid evidence, no raw `.append()`
  crash, no false clean state.

`EngineState.to_dict()` must not persist a poisoned non-list shape.

### PC-07: Observable Cleanup Atomicity

Passive-close live-flat cleanup must not emit terminal lifecycle/drift success
unless these operations can complete as one V1-equivalent success path:

1. close-reconciliation task registration or explicit no-task-needed decision;
2. `pending_passive_closes` removal;
3. `open_positions` removal;
4. `last_error` cleanup when it matches the stale passive-close error;
5. `recovery.flat` / `runtime.position_drift_corrected`;
6. `V1RecoveryDecisionCore` clear;
7. persistence / runtime state projection.

If any precondition fails, emit explicit cleanup-failed evidence and retain
state; do not journal terminal-flat success first.

### PC-08: Queue Consumer Boundaries

Every queue consumer must receive a normalized queue or fail safe:

- pending-close reconciliation processor;
- supervisor venue coverage;
- entry conflict gate;
- diagnostics/state projection consumers.

Malformed tasks must not silently skip supervised venues, bypass entry conflict
protection, or remain forever without explicit invalid evidence.

### Live Artifact Coverage

Same-window Bybit live positions that are not owned by local `BABYUSDT` state
must stay classified as unpaired/orphan live artifacts. Fixing the BABY queue
bug must not accidentally declare production clean while `MORPHOUSDT`,
`MONUSDT`, or `SEIUSDT` live positions remain on exchange.

## Non-Goals

- Do not add a duplicate field or new config to mirror V1. V2 already has the
  semantic field; the missing part is the V1-equivalent type/queue boundary.
- Do not rewrite V2 into V1 Rust file layout.
- Do not manually edit production runtime state as part of the code fix.
- Do not suppress repeated `runtime.passive_close_tick_error` by log filtering.
- Do not treat Bybit `retCode=0` as fill evidence without fill history or live
  terminal-flat proof.

## Acceptance Criteria

- RED tests reproduce the dict-shaped queue recurrence before implementation.
- RED tests prove terminal-flat events are not emitted before cleanup can
  complete.
- RED tests prove malformed queue consumers cannot silently lose supervision or
  entry-conflict evidence.
- RED tests prove unpaired Bybit live positions remain blocking recovery work
  until owned, flattened, or fail-closed with evidence.
- Focused passive-close, recovery-ledger, diagnose, production-health, and
  supervisor tests pass.
- `python3 -m compileall -q lightfee scripts tests` passes.
- `git diff --check` passes.
- GitNexus index is fresh and `detect_changes` scope matches this contract.
- Post-deploy read-only verification shows no local-open/exchange-flat
  mismatch, no unowned live positions for the focused symbols, and no recurring
  passive-close tick error.
