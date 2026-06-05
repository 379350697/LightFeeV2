# V1 Pending Entry Source Port Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace V2's bug-driven pending-entry passive opening patches with a source-function-level port of the V1 Rust pending-entry lifecycle.

**Architecture:** Build a V1-to-V2 migration matrix first, then move lifecycle decisions into `lightfee/engine/pending_entry_lifecycle.py`. `LiveRuntime` remains an IO orchestrator and must stop owning phase, zero-fill, remainder, and terminal fallback semantics directly.

**Tech Stack:** Python, pytest, dataclasses, existing LightFeeV2 runtime/adapters/journal, V1 Rust source at `/Users/wl/projects/LightFee`, GitNexus.

---

## Global Rules

- Do not mutate production state, submit/cancel orders, or deploy.
- Do not use `git reset` or broad revert commands.
- Before modifying code functions/classes/methods, run GitNexus impact. If
  private runtime methods are not found, record manual hot-path risk.
- Every behavior change starts with a RED test.
- Use V1 source functions as work units. Do not implement by incident symptom.
- Keep incident tests, but do not let them define target behavior.
- Existing dirty worktree hunks are not automatically accepted. First classify
  each hunk as `keep`, `replace`, `revert`, or `defer`, then implement from
  V1 source.
- "Looks like V1" is not enough. A helper survives only if it maps to a V1
  function/source block or to a documented boundary adaptation.

## Target Files

Create:

- `docs/parity/2026-06-05-v1-pending-entry-source-port-matrix.md`
- `lightfee/engine/pending_entry_lifecycle.py`
- `tests/engine/test_v1_pending_entry_lifecycle_parity.py`

Modify:

- `lightfee/engine/state.py`
- `lightfee/engine/entry.py`
- `lightfee/engine/entry_sync.py`
- `lightfee/engine/runtime.py`
- `lightfee/engine/recovery.py`
- `tests/live_harness/test_passive_maker_zero_fill_incident.py`
- `tests/live_harness/test_pending_entry_v1_semantic_drift_incident.py`
- `tests/persistence/test_v1_state_snapshot_semantics.py`
- `tests/test_runtime_entry_flow.py`

## Task 0: Preflight And No-Code Audit Gate

**Files:**

- Read-only: current working tree and V1 source files

- [ ] **Step 1: Confirm repository state**

Run:

```bash
git status --short
git rev-parse HEAD
```

Expected: note the dirty files. Do not revert or stage anything.

- [ ] **Step 2: Confirm GitNexus freshness before code edits**

Follow `AGENTS.md` for LightFeeV2:

```bash
npx gitnexus list
npx gitnexus status
```

If the index is stale, rebuild it as instructed by `AGENTS.md` before running
impact analysis for code edits.

- [ ] **Step 3: Establish source-copy stance**

Write in the matrix preamble:

```markdown
Implementation rule: V1 source behavior wins over current V2 behavior. Current
V2 hunks are preserved only when they are source-copied or boundary-adapted.
Bug-driven shortcuts must be replaced or reverted by targeted patches.
```

Do not edit Python code until Task 1 and Task 2 are complete.

## Task 1: Create The Source-Port Matrix

**Files:**

- Create: `docs/parity/2026-06-05-v1-pending-entry-source-port-matrix.md`

- [ ] **Step 1: Read V1 source blocks**

Run:

```bash
sed -n '64,105p' /Users/wl/projects/LightFee/src/execution_core/entry_sync.rs
sed -n '424,535p' /Users/wl/projects/LightFee/src/execution_core/entry_sync.rs
sed -n '1554,2048p' /Users/wl/projects/LightFee/src/execution_core/entry_sync.rs
sed -n '2500,2925p' /Users/wl/projects/LightFee/src/execution_core/entry_sync.rs
sed -n '4290,4565p' /Users/wl/projects/LightFee/src/execution_core/entry_sync.rs
```

Expected: source snippets for `PendingEntryHedge`, remainder helpers,
maintenance, zero-fill phase lifecycle, terminal fallback, and remainder repost.

- [ ] **Step 2: Write the matrix**

Create `docs/parity/2026-06-05-v1-pending-entry-source-port-matrix.md`:

```markdown
# V1 Pending Entry Source Port Matrix

Date: 2026-06-05

| V1 item | V1 source | V2 target | Status | Notes |
|---|---|---|---|---|
| PendingEntryHedge | src/execution_core/entry_sync.rs:64-101 | lightfee/engine/state.py::PendingEntry | pending-audit | Field map required before code changes. |
| PendingEntryRemainderSlice helpers | src/execution_core/entry_sync.rs:424-535 | lightfee/engine/state.py | pending-audit | Verify exact FIFO behavior. |
| maintain_pending_entry_passive_order | src/execution_core/entry_sync.rs:1554-1830 | pending_entry_lifecycle.py | missing | Source-port target. |
| current_tradeable_candidate_for_terminal_taker_fallback | src/execution_core/entry_sync.rs:1831-1874 | pending_entry_lifecycle.py plus runtime boundary | missing | Requires frozen candidate and guard recheck. |
| try_terminal_taker_fallback | src/execution_core/entry_sync.rs:1875-2048 | pending_entry_lifecycle.py plus runtime boundary | replace-current | Current V2 helper is bug-driven. |
| submit_pending_entry_passive_cycle | src/execution_core/entry_sync.rs:2505-2689 | pending_entry_lifecycle.py | missing | Source-port target. |
| handle_pending_entry_zero_fill_completion | src/execution_core/entry_sync.rs:2698-2925 | pending_entry_lifecycle.py | replace-current | Current V2 branch is partial. |
| try_repost_pending_entry_remainder | src/execution_core/entry_sync.rs:4346-4565 | pending_entry_lifecycle.py | missing | Source-port target. |
```

- [ ] **Step 3: Review checkpoint**

Do not continue implementation until the matrix has no `pending-audit` rows
without an owner decision.

## Task 2: Audit Current Bug-Driven Patches

**Files:**

- Modify: `docs/parity/2026-06-05-v1-pending-entry-source-port-matrix.md`

- [ ] **Step 1: Inspect current diff by file**

Run:

```bash
git diff -- lightfee/engine/runtime.py
git diff -- lightfee/engine/state.py
git diff -- lightfee/engine/entry_sync.py
git diff -- lightfee/engine/entry.py
git diff -- tests/live_harness/test_passive_maker_zero_fill_incident.py
```

- [ ] **Step 2: Mark each hunk**

For each hunk, add one of these labels to the matrix notes:

- `keep`: direct V1 field/function semantics;
- `replace`: useful behavior but wrong boundary;
- `revert`: encodes bug-driven shortcut;
- `defer`: out of this source-port scope.

- [ ] **Step 3: Required decisions**

The matrix must explicitly classify these current helpers:

```text
LiveRuntime._try_pending_entry_terminal_taker_fallback
LiveRuntime._ensure_pending_entry_phase_state
LiveRuntime._pending_entry_phase_zero_fill_budget
LiveRuntime._note_pending_entry_passive_operation
PendingEntry.push_maker_remainder_slice
PendingEntry.consume_hedge_quantity_fifo
```

Expected decisions:

- `PendingEntry` remainder helpers likely `keep`;
- runtime terminal fallback helper likely `replace`;
- runtime phase helpers likely `replace` or move into source-port module.

- [ ] **Step 4: Record restore/replacement path**

For every `replace` or `revert` hunk, add a `Replacement source` note pointing
to the V1 function/source block that will replace it. If no V1 source exists,
mark it `boundary-adapted` and explain why exact copy is impossible.

## Task 3: Add RED Source-Parity Tests

**Files:**

- Create: `tests/engine/test_v1_pending_entry_lifecycle_parity.py`

- [ ] **Step 1: Add state-construction and FIFO tests**

Add tests that reference V1 functions:

```python
def test_v1_pending_entry_hedge_remainder_fifo_consumes_slices():
    from lightfee.engine.state import PendingEntry, PendingEntryRemainderSlice
    from lightfee.core.domain import Side, Venue

    pending = PendingEntry(
        pending_id="entry-v1-fifo",
        symbol="BTCUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.OKX,
        target_quantity=3.0,
        long_side=Side.BUY,
        short_side=Side.SELL,
        created_at_ms=1000,
        maker_leg_filled=3.0,
        hedge_leg_filled=0.0,
        maker_remainder_slices=[
            PendingEntryRemainderSlice(quantity=1.0, notional_quote=10.0, fill_at_ms=1001),
            PendingEntryRemainderSlice(quantity=2.0, notional_quote=40.0, fill_at_ms=1002),
        ],
    )

    assert pending.missing_hedge_quantity() == 3.0
    assert pending.consume_hedge_quantity_fifo(1.5) == 1.5
    assert pending.missing_hedge_quantity() == 1.5
    assert len(pending.maker_remainder_slices) == 1
```

- [ ] **Step 2: Add zero-fill phase tests**

Add test placeholders with exact expected behavior, then implement helpers in
Task 4:

```python
def test_v1_handle_zero_fill_records_delay_before_repost():
    from lightfee.engine.pending_entry_lifecycle import record_pending_entry_zero_fill_cycle

    # Build minimal pending with phase_state high_slippage_maker.
    # Expect zero_fill_cycles_in_phase=1, passive_attempt_count=0,
    # repost_attempt_count=1, next_cycle_delay_ms from maker_cycle_retry_delays_ms.
```

This test must fail before `pending_entry_lifecycle.py` exists.

- [ ] **Step 3: Run RED**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_v1_pending_entry_lifecycle_parity.py
```

Expected: fail due to missing `lightfee.engine.pending_entry_lifecycle`.

## Task 4: Create Source-Port Module Skeleton

**Files:**

- Create: `lightfee/engine/pending_entry_lifecycle.py`

- [ ] **Step 1: Implement only pure data helpers**

Create:

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PendingEntryLifecycleAction:
    kind: str
    reason: str = ""
    evidence: dict[str, Any] | None = None
```

- [ ] **Step 2: Move or wrap V1 source-equivalent helpers**

Add wrappers named after V1 source intent:

```python
def note_passive_operation(pending) -> None:
    pending.passive_ops_total = int(getattr(pending, "passive_ops_total", 0) or 0) + 1


def pending_entry_phase_zero_fill_budget(strategy) -> int:
    budget = int(getattr(strategy, "pending_entry_phase_zero_fill_budget", 0) or 0)
    if budget <= 0:
        budget = int(getattr(strategy, "maker_phase_max_zero_fill_cycles", 0) or 0)
    return max(1, budget)
```

- [ ] **Step 3: Run parity tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_v1_pending_entry_lifecycle_parity.py
```

Expected: FIFO passes if state helpers already exist; zero-fill tests still fail
until Task 5.

## Task 5: Source-Port Zero-Fill Phase Lifecycle

**Files:**

- Modify: `lightfee/engine/pending_entry_lifecycle.py`
- Modify: `lightfee/engine/runtime.py`
- Test: `tests/engine/test_v1_pending_entry_lifecycle_parity.py`
- Test: `tests/live_harness/test_passive_maker_zero_fill_incident.py`

- [ ] **Step 1: Run impact analysis**

Run:

```bash
npx gitnexus impact --repo LightFeeV2 PendingEntry
npx gitnexus impact --repo LightFeeV2 LiveRuntime
```

If private methods are missing, record manual risk: runtime pending-entry hot
path.

- [ ] **Step 2: Implement source-named pure helpers**

Implement:

```python
def record_pending_entry_zero_fill_cycle(pending, strategy, now_ms: int) -> int:
    phase_state = pending.phase_state
    completed_cycles = int(phase_state.zero_fill_cycles_in_phase or 0) + 1
    delays = list(getattr(strategy, "maker_cycle_retry_delays_ms", []) or [])
    delay_ms = int(delays[min(completed_cycles - 1, len(delays) - 1)]) if delays else 0
    phase_state.zero_fill_cycles_in_phase = completed_cycles
    phase_state.cycle_attempt = completed_cycles
    phase_state.next_cycle_delay_ms = delay_ms
    phase_state.hedge_deadline_at_ms = None
    pending.repost_attempt_count = completed_cycles
    pending.passive_attempt_count = 0
    pending.next_progress_poll_ms = now_ms + delay_ms
    return delay_ms
```

Add high-to-low and low-to-dual helpers by translating
`handle_pending_entry_zero_fill_completion` from V1, preserving branch order.

- [ ] **Step 3: Replace runtime-local branch mutations**

In `runtime.py`, replace direct phase mutation with calls to
`pending_entry_lifecycle.py`. Runtime may still perform adapter submits and
journals.

- [ ] **Step 4: Run focused tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_v1_pending_entry_lifecycle_parity.py tests/live_harness/test_passive_maker_zero_fill_incident.py
```

Expected: pass.

## Task 6: Source-Port Terminal Taker Fallback Boundary

**Files:**

- Modify: `lightfee/engine/pending_entry_lifecycle.py`
- Modify: `lightfee/engine/runtime.py`
- Test: `tests/engine/test_v1_pending_entry_lifecycle_parity.py`
- Test: `tests/live_harness/test_passive_maker_zero_fill_incident.py`

- [ ] **Step 1: RED for frozen-candidate recheck**

Add a test proving fallback is skipped when frozen candidate has
`blocked=True` or non-empty `blocked_reasons` after runtime guard recheck:

```python
def test_v1_terminal_taker_fallback_skips_blocked_frozen_candidate():
    # Build pending.frozen_candidate with blocked=True.
    # Call the source-port fallback decision helper.
    # Expect action kind "skip_fallback" and reason
    # "candidate_not_tradeable_after_terminal_reprice".
```

- [ ] **Step 2: Replace current runtime helper**

Replace `LiveRuntime._try_pending_entry_terminal_taker_fallback` with a
boundary wrapper that:

1. asks source-port helper for candidate/fallback decision;
2. applies V2 runtime guard adapter exactly once;
3. calls existing ForceStandard/open implementation;
4. materializes open position or defers pending exactly like V1.

The current helper that builds `EntryContext` directly from pending fields must
be removed or downgraded to a boundary adapter after frozen-candidate recheck.

- [ ] **Step 3: Run tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_v1_pending_entry_lifecycle_parity.py tests/live_harness/test_passive_maker_zero_fill_incident.py tests/test_runtime_entry_flow.py
```

Expected: pass.

## Task 7: Source-Port Passive Cycle Submit And Remainder Repost

**Files:**

- Modify: `lightfee/engine/pending_entry_lifecycle.py`
- Modify: `lightfee/engine/runtime.py`
- Test: `tests/engine/test_v1_pending_entry_lifecycle_parity.py`

- [ ] **Step 1: RED for submit_pending_entry_passive_cycle**

Add tests for:

- remaining quantity depleted;
- normalized quantity below minimum;
- accepted ack resets `next_cycle_delay_ms` and `hedge_deadline_at_ms`;
- `cycle_attempt = zero_fill_cycles_in_phase + 1`;
- passive submit increments `passive_ops_total`.

- [ ] **Step 2: RED for try_repost_pending_entry_remainder**

Add tests for:

- passive attempt limit;
- max reposts;
- remaining quantity;
- accepted ack increments `repost_attempt_count`;
- partial balanced remainder does not use zero-fill phase counters.

- [ ] **Step 3: Implement source-port helpers**

Translate V1 logic into pure action functions. Runtime handles actual adapter
normalization/submission, then calls source-port state mutation helpers.

- [ ] **Step 4: Run tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_v1_pending_entry_lifecycle_parity.py tests/live_harness/test_passive_maker_zero_fill_incident.py tests/test_live_entry_hedge_root_fix.py
```

Expected: pass.

## Task 8: Remove Or Replace Bug-Driven Runtime Helpers

**Files:**

- Modify: `lightfee/engine/runtime.py`
- Modify: `docs/parity/2026-06-05-v1-pending-entry-source-port-matrix.md`

- [ ] **Step 1: Search for non-source helpers**

Run:

```bash
rg -n "_try_pending_entry_terminal_taker_fallback|_pending_entry_phase_zero_fill_budget|_ensure_pending_entry_phase_state|_note_pending_entry_passive_operation" lightfee/engine/runtime.py
```

- [ ] **Step 2: Remove or delegate**

Every match must either be deleted or be a one-line delegation to
`pending_entry_lifecycle.py` with a matrix row explaining the boundary.
Do not leave lifecycle decisions in `runtime.py` behind new names.

- [ ] **Step 3: Run focused tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_v1_pending_entry_lifecycle_parity.py tests/live_harness/test_passive_maker_zero_fill_incident.py tests/test_runtime_entry_flow.py
```

Expected: pass.

## Task 9: Regression And Hygiene Gate

**Files:**

- Modify: bug docs if behavior classification changes.

- [ ] **Step 1: Run broad regression**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_entry_semantic_parity.py tests/test_entry_sync.py tests/test_runtime_entry_flow.py tests/live_harness/test_pending_entry_v1_semantic_drift_incident.py tests/live_harness/test_passive_maker_zero_fill_incident.py tests/persistence/test_v1_state_snapshot_semantics.py tests/test_exit_decisions.py tests/engine/test_funding_lifecycle.py tests/engine/test_v1_trading_lifecycle.py tests/engine/test_v1_real_config_gap_semantics.py tests/engine/test_close_semantic_parity.py tests/test_close_pnl.py tests/test_close_execution.py tests/test_sidecar_snapshot.py tests/sidecar/test_sidecar_service.py tests/test_strategy_discovery.py tests/test_live_entry_hedge_root_fix.py
```

Expected: pass.

- [ ] **Step 2: Run hygiene**

Run:

```bash
.venv/bin/python -m compileall -q lightfee
git diff --check
npx gitnexus detect-changes --repo LightFeeV2
```

Expected: compileall and diff-check pass; detect-changes risk reviewed and
limited to pending-entry lifecycle/runtime/recovery/entry scope.

- [ ] **Step 3: Update bug docs**

Update:

- `docs/bugs/BUG_INDEX.md`
- `docs/bugs/daily/2026-06-05.md`

State whether the final patch is source-ported, boundary-adapted, or still
contains known non-source deviations.

## Task 10: Final Source-Port Closure Review

**Files:**

- `docs/parity/2026-06-05-v1-pending-entry-source-port-matrix.md`
- `docs/bugs/BUG_INDEX.md`
- `docs/bugs/daily/2026-06-05.md`

- [ ] **Step 1: Search for leftover bug-driven seams**

Run:

```bash
rg -n "quick_flat|quick-flat|incident|fallback|zero_fill|phase_state|force_standard|EntryContext\\(" lightfee/engine/runtime.py lightfee/engine/pending_entry_lifecycle.py tests
```

Expected: each remaining lifecycle hit is either a source-port function,
runtime IO wrapper, or regression test. No incident-only runtime behavior
remains.

- [ ] **Step 2: Close the matrix**

Every matrix row must be one of:

- `ported`;
- `boundary-adapted`;
- `explicitly-out-of-scope`.

No `missing`, `pending-audit`, or `replace-current` rows may remain in final
acceptance.

- [ ] **Step 3: Final report**

Report:

- copied V1 functions and V2 targets;
- current patches kept;
- current patches replaced/restored;
- approved deviations, if any;
- verification commands and exact results.
