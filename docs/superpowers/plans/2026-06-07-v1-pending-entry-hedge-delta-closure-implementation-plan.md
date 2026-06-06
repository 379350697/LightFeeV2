# Implementation Plan: V1 Pending Entry Hedge Delta Closure

Date: 2026-06-07

Spec:
`docs/superpowers/specs/2026-06-07-v1-pending-entry-hedge-delta-closure-design.md`

## Goal

Port the V1 pending-entry hedge delta closure as one controlled source-level
unit while keeping Python adapter IO in `LiveRuntime`.

## Constraints

- Start read-only and preserve unrelated dirty worktree changes.
- Write RED tests before implementation.
- Do not deploy, probe production, submit orders, cancel orders, or mutate
  runtime state.
- Do not implement the DEV-003 no-frozen candidate rediscovery branch.
- Do not rewrite passive close or funding lifecycle.
- Before modifying functions/classes, run GitNexus upstream impact commands.

## Files

Expected production targets:

- `lightfee/engine/pending_entry_hedge_delta.py` (new pure decision boundary)
- `lightfee/engine/pending_entry_lifecycle.py`
- `lightfee/engine/runtime.py`
- `lightfee/engine/state.py` only if a missing persisted field is required

Expected tests:

- `tests/engine/test_v1_pending_entry_hedge_delta_closure.py`
- `tests/engine/test_v1_pending_entry_lifecycle_parity.py`
- `tests/test_live_entry_hedge_root_fix.py`
- `tests/live_harness/test_passive_maker_zero_fill_incident.py`

Expected docs after implementation:

- `docs/parity/2026-06-05-v1-pending-entry-source-port-matrix.md`
- `docs/parity/approved_deviations.md`

## Pre-Flight

Run:

```bash
git status --short
git rev-parse HEAD
npx gitnexus status
```

If GitNexus is stale:

```bash
npx gitnexus analyze --index-only --name LightFeeV2 --drop-embeddings .
npx gitnexus status
```

Before editing, run upstream impact:

```bash
npx gitnexus impact --repo LightFeeV2 PendingEntry --include-tests
npx gitnexus impact --repo LightFeeV2 _drive_missing_hedge_live --include-tests
npx gitnexus impact --repo LightFeeV2 _recover_drive_missing_hedge --include-tests
npx gitnexus impact --repo LightFeeV2 _recover_pending_entry_hedges --include-tests
npx gitnexus impact --repo LightFeeV2 ensure_pending_entry_phase_state --include-tests
```

If any impact is `HIGH` or `CRITICAL`, stop and report the direct callers,
affected processes, and risk before editing.

## Step 1: Re-Read V1 Source Blocks

Read these exact ranges immediately before implementation:

```bash
sed -n '120,145p' /Users/wl/projects/LightFee/src/execution_core/entry_sync.rs
sed -n '3860,4145p' /Users/wl/projects/LightFee/src/execution_core/entry_sync.rs
sed -n '4710,5448p' /Users/wl/projects/LightFee/src/execution_core/entry_sync.rs
sed -n '3800,3845p' /Users/wl/projects/LightFee/src/engine/entry.rs
sed -n '133,190p' /Users/wl/projects/LightFee/src/execution_core/passive_phase.rs
```

Expected result: implementation notes identify branch order, event names,
state mutations, and adapter-only operations.

## Step 2: Add RED Pure Contract Tests

Create `tests/engine/test_v1_pending_entry_hedge_delta_closure.py`.

The first test set should not construct `LiveRuntime`. It should use simple
pending-entry objects and call the new decision boundary.

Required test names:

- `test_v1_releasable_hedge_quantity_blocks_sub_chunk_delta`
- `test_v1_releasable_hedge_quantity_rounds_down_to_whole_chunks`
- `test_v1_small_fill_below_chunk_buffers_when_not_terminal_or_canceling`
- `test_v1_terminal_or_canceling_small_fill_counts_attempt_immediately`
- `test_v1_min_notional_accumulation_clears_hedge_deadline_and_keeps_pending`
- `test_v1_min_notional_attempt_exhaustion_returns_abort_and_flatten_action`
- `test_v1_passive_small_fill_buffer_waits_until_deadline`
- `test_v1_passive_small_fill_buffer_expiry_releases_submit_action`
- `test_v1_entry_hedge_deadline_extends_for_fresh_progressing_small_hedge`
- `test_v1_reconciled_without_progress_does_not_extend_deadline`

Each test must assert the decision kind, mutated pending fields, emitted event
name, and absence of adapter calls when the expected V1 branch is buffering or
waiting.

Run the RED slice:

```bash
.venv/bin/pytest -q tests/engine/test_v1_pending_entry_hedge_delta_closure.py
```

Expected result before implementation: failures show missing module/functions,
not unrelated runtime failures.

## Step 3: Add the Pure Decision Boundary

Create `lightfee/engine/pending_entry_hedge_delta.py`.

Use dataclasses for decision shape. Keep it free of adapter calls:

```python
@dataclass(frozen=True)
class PendingEntryHedgeabilityPlan:
    min_hedgeable_chunk: float
    aligned_target_quantity: float
    blocked_reason: str = ""
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PendingEntryHedgeDeltaDecision:
    kind: str
    reason: str = ""
    releasable_quantity: float = 0.0
    normalized_quantity: float = 0.0
    next_progress_poll_ms: int | None = None
    event: str = ""
    evidence: dict[str, Any] = field(default_factory=dict)
```

Implement helpers in this order:

- `releasable_hedge_quantity`
- `adaptive_entry_hedge_deadline_decision`
- `decide_pending_entry_hedge_delta_pre_submit`
- `note_pending_entry_hedge_submitted`
- `note_pending_entry_hedge_filled`
- `decide_pending_entry_hedge_submit_error`
- `decide_pending_entry_hedge_deadline_timeout`

Run:

```bash
.venv/bin/pytest -q tests/engine/test_v1_pending_entry_hedge_delta_closure.py
```

Expected result: pure tests pass without touching runtime.

## Step 4: Wire Normal Tick Runtime

Modify `_drive_missing_hedge_live` to call the shared decision boundary before
building `OrderRequest`.

Required behavior:

- no submit when the decision is `buffer_small_fill`;
- no submit when the decision is `wait_min_notional_accumulation`;
- no submit while `pending.hedge_inflight` exists;
- submit path sets `hedge_inflight` and `phase_state.hedge_deadline_at_ms`
  before adapter IO;
- filled path uses `note_pending_entry_hedge_filled` and consumes FIFO;
- zero-fill and rejected paths return through explicit decisions.

Add or update runtime tests:

- `test_live_drive_missing_hedge_buffers_sub_chunk_delta_without_submit`
- `test_live_drive_missing_hedge_sets_hedge_deadline_before_submit`
- `test_live_drive_missing_hedge_hard_deadline_routes_to_timeout_abort`

The fake adapter must count normalization, min-notional lookup, submit, and
reconciliation calls so the tests prove the runtime followed the intended V1
branch.

Run:

```bash
.venv/bin/pytest -q tests/test_live_entry_hedge_root_fix.py -k "hedge or deadline or min_notional"
```

Expected result: targeted runtime hedge tests pass.

## Step 5: Wire Startup Recovery Runtime

Modify `_recover_drive_missing_hedge` to share the same hedge delta closure. It
may pass a recovery policy flag for fail-closed gating, but it must not keep a
separate small-fill or deadline algorithm.

Add tests proving normal tick and startup recovery agree:

- `test_startup_recovery_and_normal_tick_share_small_fill_decision`
- `test_startup_recovery_and_normal_tick_share_deadline_timeout_decision`

Both tests must construct the same pending-entry facts for each path and assert
matching decision kinds, journal event names, and pending-entry field changes.

Run:

```bash
.venv/bin/pytest -q tests/test_live_entry_hedge_root_fix.py tests/live_harness/test_passive_maker_zero_fill_incident.py -k "recovery or hedge or deadline or partial"
```

Expected result: startup and normal tick no longer diverge on the same pending
entry facts.

## Step 6: Cover Partial Fill Plus Phase Switching

Add focused coverage in
`tests/engine/test_v1_pending_entry_lifecycle_parity.py` or the new hedge-delta
test file.

Required cases:

- partial maker fill records a remainder slice;
- high-to-low phase switch does not clear the slice;
- the next hedge delta uses FIFO weighted price and only releases whole chunks;
- `hedge_deadline_at_ms` is not accidentally preserved across phase switch.

Run:

```bash
.venv/bin/pytest -q tests/engine/test_v1_pending_entry_lifecycle_parity.py tests/engine/test_v1_pending_entry_hedge_delta_closure.py
```

Expected result: lifecycle and hedge delta source-port tests pass together.

## Step 7: Update Parity Docs

Update:

- `docs/parity/2026-06-05-v1-pending-entry-source-port-matrix.md`
- `docs/parity/approved_deviations.md`

Required doc outcome:

- The `hedge_pending_entry_delta` full deadline and terminal driver row is no
  longer `explicitly-out-of-scope`.
- Any remaining differences are adapter-only boundary adaptations.
- DEV-003 keeps no-frozen candidate rediscovery out of scope.

## Step 8: Verification Gate

Run focused tests:

```bash
.venv/bin/pytest -q tests/engine/test_v1_pending_entry_hedge_delta_closure.py
.venv/bin/pytest -q tests/engine/test_v1_pending_entry_lifecycle_parity.py tests/test_live_entry_hedge_root_fix.py tests/live_harness/test_passive_maker_zero_fill_incident.py
```

Run broad pending-entry and recovery tests:

```bash
.venv/bin/pytest -q tests/recovery tests/test_engine_recovery.py tests/test_recovery_reconciliation.py tests/engine/test_recovery_decision_core.py tests/engine/test_recovery_ledger.py tests/engine/test_recovery_owner_index.py tests/engine/test_pending_entry_terminalizer.py tests/test_v1_parity_pending_entry_recovery_red.py tests/test_persistence.py tests/persistence/test_journal_event_semantics.py tests/persistence/test_v1_state_snapshot_semantics.py
```

Run static checks:

```bash
git diff --check
npx gitnexus status
npx gitnexus detect_changes --repo LightFeeV2
```

Expected result:

- all tests pass;
- GitNexus is fresh;
- `detect_changes` scope matches pending-entry hedge delta work;
- no unrelated files are modified except planned docs/tests/engine files.

## Parallel Execution Notes

Safe to run in parallel:

- Step 2 pure RED tests and Step 6 fixture design.
- Step 7 doc drafting after the expected decision names are fixed.

Keep serial:

- Step 4 and Step 5 runtime wiring.
- Any code that changes pending-entry removal, abort, fail-closed, or
  reconciliation state.
