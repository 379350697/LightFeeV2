# V1 Pending Entry Hedge Delta Closure Design

Date: 2026-06-07

Status: design draft for a focused V1 source-level port.

## Source Verdict

This is the remaining pending-entry area where V2 is still too local and V1 has
a complete closed loop that should be ported as a unit. The existing V2 code
already preserves useful pieces: weighted maker price, remainder FIFO consume,
normalized quantity, inflight client id, and recovery retry hooks. Those pieces
are not enough to claim V1 parity because the hedge delta driver is still split
across runtime branches and does not own the hedgeability, small-fill,
deadline, timeout, and abort/flatten chain as one decision surface.

## Primary Sources

- Audit source: `/Users/wl/Downloads/lightfeev2_v1_audit_report-anti.md`
- V1 hedge delta driver:
  `/Users/wl/projects/LightFee/src/execution_core/entry_sync.rs:4710-5448`
- V1 releasable quantity helper:
  `/Users/wl/projects/LightFee/src/execution_core/entry_sync.rs:127-140`
- V1 hedgeability planner:
  `/Users/wl/projects/LightFee/src/engine/entry.rs:3804-3839`
- V1 adaptive deadline helper:
  `/Users/wl/projects/LightFee/src/execution_core/passive_phase.rs:133-190`
- V1 spread-aware timeout branch:
  `/Users/wl/projects/LightFee/src/execution_core/entry_sync.rs:3869-4145`
- V2 current runtime drivers:
  `lightfee/engine/runtime.py::_drive_missing_hedge_live`,
  `lightfee/engine/runtime.py::_recover_drive_missing_hedge`,
  `lightfee/engine/runtime.py::_recover_pending_entry_hedges`
- V2 current lifecycle boundary:
  `lightfee/engine/pending_entry_lifecycle.py`
- Current parity matrix:
  `docs/parity/2026-06-05-v1-pending-entry-source-port-matrix.md`

## Control Range

In scope:

- Source-port the V1 `hedge_pending_entry_delta` business decisions as a
  single pending-entry hedge delta closure.
- Keep Python runtime as the adapter IO boundary for normalization, min
  notional hints, order submit, reconciliation, cancel, abort, and persist.
- Preserve V1 branch ordering:
  missing delta -> hedgeability plan -> releasable chunk -> small-fill buffer ->
  normalized/min-notional accumulation -> passive small-fill buffer ->
  deadline start -> submit -> success/reconcile/error -> deadline timeout ->
  abort/flatten or continue.
- Use one shared decision path for normal tick and startup recovery.
- Add RED tests before implementation and keep existing tests green.
- Update the parity matrix and approved deviation ledger after implementation.

Out of scope:

- Recreating V1 `MarketView` candidate rediscovery for no-frozen terminal
  fallback. That remains the approved DEV-003 boundary adaptation.
- Rewriting Python adapters or changing venue transport APIs.
- Live production probing, deploy, order submit, cancel, runtime state mutation,
  or operator intervention.
- Passive close hedge deadline behavior, except for reusable helper comparison.
- Funding lifecycle and unrelated entry selection changes.

## Problem Statement

The current V2 hedge path can submit a missing hedge, but it does not yet model
the whole V1 closure:

- sub-minimum deltas are treated as simple below-min-notional outcomes instead
  of V1 releasable chunk and accumulation decisions;
- `hedge_deadline_at_ms` is written and cleared in lifecycle paths, but the
  timeout path is not tested as a source-owned entry hedge decision;
- startup recovery and normal reconciliation can diverge because they call
  different helper paths;
- terminal-or-canceling maker state does not force the same small-fill and
  min-notional escalation as V1;
- order submit errors do not consistently flow through V1's reconciled,
  uncertain exposure, confirmed absent, spread-aware timeout, and fail-closed
  branches.

This is a source-port candidate because V1 already solved the full lifecycle in
one driver, while V2 currently has scattered partial semantics.

## Target Shape

Add a pure or near-pure decision boundary, preferably
`lightfee/engine/pending_entry_hedge_delta.py`, and keep adapter operations in
`LiveRuntime`.

The new boundary owns:

- `releasable_hedge_quantity`
- hedgeability result normalization into a Python dataclass
- small-fill chunk buffering decisions
- min-notional accumulation attempt accounting
- passive small-fill buffer wait/expired decisions
- adaptive entry hedge deadline calculation
- timeout decision classification
- event names and payload fields expected by runtime

The runtime owns:

- adapter lookup;
- `normalize_quantity`;
- min-notional floor hints;
- price and mark-price hints;
- `OrderRequest` construction;
- `place_order` and reconciliation calls;
- cancel, abort, fail-closed, flatten, persistence, and journal flushing.

The runtime should call one common hedge delta closure from both:

- `_drive_missing_hedge_live`
- `_recover_drive_missing_hedge`

Any remaining wrapper differences must be explicit policy flags, not separate
business logic forks.

## Required Behavior

### Delta and hedgeability

- `delta <= 0` keeps the pending entry without submitting a hedge.
- Non-finite or non-positive delta releases zero quantity.
- Non-positive `min_hedgeable_chunk` releases the full positive delta.
- Positive `min_hedgeable_chunk` releases only whole chunks.
- Below-chunk delta buffers only when `pending.can_accumulate_small_fill()` is
  true and the maker is not terminal-or-canceling.

### Small-fill and min-notional accumulation

- V1's `should_count_small_fill` rule must be preserved:
  maker progress updated, terminal-or-canceling, or first attempt counts an
  accumulation attempt.
- Counting an attempt clears `phase_state.hedge_deadline_at_ms`.
- Accumulation below max attempts schedules `next_progress_poll_ms` and keeps
  the pending entry.
- Exhausted accumulation logs abort/flatten intent and routes through cancel
  plus abort/flatten, not silent retention.

### Passive small-fill buffer

- Buffered notional below `passive_small_fill_buffer_notional_quote` waits until
  `passive_small_fill_buffer_max_wait_ms` expires.
- Wait state must not submit a hedge or increment attempts.
- Expired wait logs the expiry and proceeds to hedge submission.
- Terminal-or-canceling entries skip the wait and continue to terminal handling.

### Deadline lifecycle

- Submission sets `phase_state.hedge_deadline_at_ms` from the effective V1 hard
  deadline.
- Soft and hard status use V1 adaptive logic:
  quote freshness, hedge notional, execution progress, and reconciliation all
  affect the effective deadline.
- Successful fill or reconciled fill clears `hedge_deadline_at_ms` and resets
  `small_fill_min_notional_attempts`.
- Hard breach with remaining unmatched maker quantity enters spread-aware hedge
  timeout handling.
- Hard breach with no remaining unmatched quantity logs deadline breach and
  keeps or finalizes through the normal terminalizer path.

### Submit, reconcile, and abort

- Hedge submit creates a deterministic client id per attempt and persists
  `hedge_inflight` before adapter IO.
- Filled submit consumes maker remainder FIFO and updates hedge fill fields.
- Submit error first tries order fill reconciliation by client id.
- Reconciled positive fill follows the same fill path and marks `reconciled`.
- Error that may have created exposure uses uncertain-exposure handling.
- Confirmed absent hard breach routes through spread-aware timeout or
  fail-closed abort.
- Non-terminal submit error aborts through the same pending-entry abort/flatten
  authority used by V1-compatible recovery.

## Test Matrix

Add focused tests before implementation.

Pure source-port tests:

- `releasable_hedge_quantity` blocks sub-chunk deltas.
- `releasable_hedge_quantity` rounds down to whole chunks.
- small-fill below chunk buffers when accumulation is allowed and maker is not
  terminal-or-canceling.
- terminal-or-canceling below chunk counts an attempt immediately.
- min-notional accumulation below max keeps pending and clears hedge deadline.
- min-notional accumulation at max returns abort/flatten intent.
- passive small-fill buffer waits before the configured deadline.
- passive small-fill buffer expires and releases the hedge path.
- adaptive deadline extends for fresh small progressing entry hedges.
- reconciled-without-progress does not extend indefinitely.

Runtime integration tests:

- `_drive_missing_hedge_live` uses the shared decision boundary before submit.
- startup recovery and normal tick produce the same decision for the same
  pending entry.
- `hedge_deadline_at_ms` hard breach with unmatched maker emits deadline
  evidence and routes to timeout/abort rather than retrying a second hedge.
- partial maker fill followed by high-to-low phase switching preserves maker
  remainder slices and still hedges only releasable quantity.
- uncertain submit error keeps or clears `hedge_inflight` according to
  reconciliation evidence.

## Parallelization Boundary

Can be parallel:

- Pure helper RED tests and helper implementation.
- Runtime fixture construction for normal tick and startup recovery.
- Documentation update after the exact source-port rows are known.

Must be serial:

- Edits to `_drive_missing_hedge_live`, `_recover_drive_missing_hedge`, and
  `_recover_pending_entry_hedges`, because these paths share pending-entry
  terminality and recovery state.
- Any change that touches pending-entry removal, abort, or fail-closed state.

## Acceptance Criteria

- The current parity matrix no longer marks the full hedge deadline and terminal
  driver as out of scope; it is either `ported` or explicitly
  `boundary-adapted` with the adapter-only differences listed.
- Startup recovery and normal tick use the same hedge delta closure.
- No code path submits a second hedge while `hedge_inflight` is active.
- Small-fill accumulation and passive buffer decisions are covered by RED-first
  tests.
- Deadline hard breach is covered by at least one runtime test and one pure
  decision test.
- Existing recovery, pending-entry lifecycle, live-entry hedge, and persistence
  suites remain green.
- No production state, deploy artifact, or venue order side effect is touched.
