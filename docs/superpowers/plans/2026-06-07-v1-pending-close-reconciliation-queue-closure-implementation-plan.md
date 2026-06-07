# V1 Pending Close Reconciliation Queue Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close CL-051 by restoring the V1-equivalent pending-close reconciliation queue boundary, making passive-close live-flat cleanup atomic, and proving same-window unpaired live artifacts remain fail-safe.

**Architecture:** Keep V2's Python runtime shape, but add a small canonical queue boundary equivalent to V1's typed `Vec<PendingCloseReconciliation>` plus state-owned enqueue/remove helpers. Runtime cleanup, reconciliation processing, supervisor, entry gate, diagnose, and persistence must consume the same normalized queue contract.

**Tech Stack:** Python dataclasses, pytest, existing LightFeeV2 engine modules, GitNexus CLI, V1 Rust source as semantic reference.

---

Spec:
`docs/superpowers/specs/2026-06-07-v1-pending-close-reconciliation-queue-closure-design.md`

Bug docs:

- `docs/bugs/daily/2026-06-07.md`
- `docs/bugs/contracts/pending-entry-live-truth-contract.md`
- `docs/bugs/cards/passive-close-terminal-flatness.md`

## Coverage Verdict

This plan covers every critical post-`bff33ec` production red signal that was
identified:

- `BABYUSDT` local-open/exchange-flat mismatch;
- repeated `runtime.passive_close_tick_error` from dict-shaped queue;
- repeated terminal-flat/drift journals without state removal;
- accepted Bybit close ack without fill evidence;
- hedge deadline fail-closed cleanup path;
- unpaired Bybit live positions on `MORPHOUSDT`, `MONUSDT`, and `SEIUSDT`.

Market-data degradation and quote-stale warnings are intentionally treated as
adjacent evidence, not the root fix. This plan keeps them visible and verifies
they are not used as false flat/live-artifact evidence.

## File Map

Expected production targets:

- Modify: `lightfee/engine/state.py`
  - add canonical queue normalize/enqueue/remove helpers;
  - guard `to_dict()` from persisting poisoned non-list shapes.
- Modify: `lightfee/engine/recovery.py`
  - normalize `pending_close_reconciliations` during snapshot restore.
- Modify: `lightfee/engine/passive_close.py`
  - replace raw `.append()` with state helper;
  - reorder live-flat cleanup so success journals follow safe state mutation.
- Modify: `lightfee/engine/runtime.py`
  - harden `_process_pending_close_reconciliations`;
  - harden `_gate_pending_close_reconciliation`;
  - preserve core clear as the single authority.
- Modify: `lightfee/engine/supervisor.py`
  - use normalized queue data for supervised venues or emit invalid evidence.

Expected tests:

- Modify: `tests/persistence/test_v1_state_snapshot_semantics.py`
- Modify: `tests/test_passive_close.py`
- Modify: `tests/test_pending_entry_v1_semantic_drift.py`
- Modify: `tests/test_supervisor_execution.py`
- Modify: `tests/engine/test_recovery_ledger.py`
- Modify: `tests/engine/test_recovery_decision_core.py`
- Modify: `tests/test_diagnose_live.py`
- Modify: `tests/ops/test_production_health.py`

## Pre-Flight

- [x] **Step 1: Confirm working tree and GitNexus freshness**

Run:

```bash
git status --short
git rev-parse HEAD
npx gitnexus status
```

Expected: index commit equals current `HEAD`. If stale, run:

```bash
npx gitnexus analyze --index-only --name LightFeeV2 --drop-embeddings .
npx gitnexus status
```

- [x] **Step 2: Run upstream impact before touching functions**

Run:

```bash
npx gitnexus impact EngineState --repo LightFeeV2 --direction upstream --depth 2 --include-tests
npx gitnexus impact _restore_state_from_snapshot_dict --repo LightFeeV2 --direction upstream --depth 2 --include-tests
npx gitnexus impact _clear_live_flat_state --repo LightFeeV2 --direction upstream --depth 2 --include-tests
npx gitnexus impact _register_close_reconciliation_after_live_flat --repo LightFeeV2 --direction upstream --depth 2 --include-tests
```

Expected: report direct callers and risk. If any impact is `HIGH` or
`CRITICAL`, summarize affected flows before editing.

## Task 1: RED Restore/Persist Queue Shape Tests

**Files:**

- Modify: `tests/persistence/test_v1_state_snapshot_semantics.py`
- Later modify: `lightfee/engine/state.py`
- Later modify: `lightfee/engine/recovery.py`

- [x] **Step 1: Add failing restore test**

Add a test that calls `_restore_state_from_snapshot_dict()` with:

```python
snapshot = {
    "lifecycle": "risk_only",
    "risk_mode": "fail_closed",
    "pending_close_reconciliations": {
        "entry-1780771924982-BABYUSDT": {
            "position_id": "entry-1780771924982-BABYUSDT",
            "symbol": "BABYUSDT",
            "kind": "final",
            "reason": "pending_passive_close_flat_probe",
            "closed_at_ms": 1780771929000,
            "created_cycle": 42,
            "position_snapshot": {
                "position_id": "entry-1780771924982-BABYUSDT",
                "symbol": "BABYUSDT",
                "long_venue": "okx",
                "short_venue": "bybit",
            },
            "long_legs": [],
            "short_legs": [],
            "attempt_count": 0,
            "next_attempt_ms": 1780771929000,
        }
    },
}
```

Expected assertions:

```python
state = _restore_state_from_snapshot_dict(snapshot)
assert isinstance(state.pending_close_reconciliations, list)
assert state.pending_close_reconciliations[0]["position_id"] == "entry-1780771924982-BABYUSDT"
assert isinstance(state.to_dict()["pending_close_reconciliations"], list)
```

- [x] **Step 2: Run test to verify RED**

Run:

```bash
.venv/bin/pytest -q tests/persistence/test_v1_state_snapshot_semantics.py -k "pending_close_reconciliations"
```

Expected before implementation: the new test fails because restore keeps a dict
or `to_dict()` can preserve the poisoned shape.

## Task 2: Implement Canonical Queue Boundary

**Files:**

- Modify: `lightfee/engine/state.py`
- Modify: `lightfee/engine/recovery.py`
- Test: `tests/persistence/test_v1_state_snapshot_semantics.py`

- [x] **Step 1: Add queue normalization helpers**

Add helpers on or near `EngineState`:

```python
MAX_PENDING_CLOSE_RECONCILIATIONS = 256

def normalize_pending_close_reconciliations(raw: Any) -> list[dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        items = [raw] if raw is a single reconciliation task else list(raw.values())
    else:
        return [invalid evidence task]
    return [dict(item) or invalid evidence task for item in items]
```

Add an instance helper:

```python
def set_pending_close_reconciliations(self, raw: Any) -> None:
    self.pending_close_reconciliations = normalize_pending_close_reconciliations(raw)[
        -MAX_PENDING_CLOSE_RECONCILIATIONS:
    ]
```

- [x] **Step 2: Use helper in restore and serialization**

In `_restore_state_from_snapshot_dict()`:

```python
state.set_pending_close_reconciliations(
    snap.get("pending_close_reconciliations", [])
)
```

In `EngineState.to_dict()`:

```python
"pending_close_reconciliations": normalize_pending_close_reconciliations(
    self.pending_close_reconciliations
),
```

- [x] **Step 3: Run focused tests**

Run:

```bash
.venv/bin/pytest -q tests/persistence/test_v1_state_snapshot_semantics.py -k "pending_close_reconciliations"
```

Expected: RED from Task 1 turns GREEN and existing queue persistence tests pass.

## Task 3: RED State-Owned Enqueue/Remove Tests

**Files:**

- Modify: `tests/persistence/test_v1_state_snapshot_semantics.py`
- Later modify: `lightfee/engine/state.py`
- Later modify: `lightfee/engine/passive_close.py`

- [x] **Step 1: Add helper behavior tests**

Add tests proving:

- duplicate `position_id + kind` is ignored;
- queue caps at 256 by dropping oldest;
- remove matches `position_id + kind + closed_at_ms`;
- helper accepts dict-shaped existing queue by normalizing first.
- single-task dict shape is retained as one task;
- non-dict items become explicit invalid evidence tasks instead of disappearing.

Use the V1 sample behavior from `/Users/wl/projects/LightFee/src/engine/state.rs`.

- [x] **Step 2: Run tests to verify RED**

Run:

```bash
.venv/bin/pytest -q tests/persistence/test_v1_state_snapshot_semantics.py -k "pending_close_reconciliation and enqueue"
```

Expected before implementation: helper tests fail because the helper does not
exist.

## Task 4: Implement State-Owned Enqueue/Remove

**Files:**

- Modify: `lightfee/engine/state.py`
- Modify: `lightfee/engine/passive_close.py`
- Test: `tests/persistence/test_v1_state_snapshot_semantics.py`

- [x] **Step 1: Add enqueue/remove helpers**

Implement:

```python
def enqueue_pending_close_reconciliation(self, item: dict[str, Any]) -> None:
    self.set_pending_close_reconciliations(self.pending_close_reconciliations)
    position_id = str(item.get("position_id") or "")
    kind = str(item.get("kind") or "final")
    for existing in self.pending_close_reconciliations:
        if (
            str(existing.get("position_id") or "") == position_id
            and str(existing.get("kind") or "final") == kind
        ):
            return
    self.pending_close_reconciliations.append(dict(item))
    if len(self.pending_close_reconciliations) > MAX_PENDING_CLOSE_RECONCILIATIONS:
        self.pending_close_reconciliations = self.pending_close_reconciliations[
            -MAX_PENDING_CLOSE_RECONCILIATIONS:
        ]
```

Implement:

```python
def remove_pending_close_reconciliation(self, task: dict[str, Any]) -> bool:
    self.set_pending_close_reconciliations(self.pending_close_reconciliations)
    before = len(self.pending_close_reconciliations)
    target = (
        str(task.get("position_id") or ""),
        str(task.get("kind") or "final"),
        int(task.get("closed_at_ms") or 0),
    )
    self.pending_close_reconciliations = [
        item for item in self.pending_close_reconciliations
        if (
            str(item.get("position_id") or ""),
            str(item.get("kind") or "final"),
            int(item.get("closed_at_ms") or 0),
        ) != target
    ]
    return len(self.pending_close_reconciliations) != before
```

- [x] **Step 2: Replace raw append in passive close**

In `_register_close_reconciliation_after_live_flat()`, replace:

```python
state.pending_close_reconciliations.append(reconciliation)
```

with:

```python
state.enqueue_pending_close_reconciliation(reconciliation)
```

- [x] **Step 3: Run focused tests**

Run:

```bash
.venv/bin/pytest -q tests/persistence/test_v1_state_snapshot_semantics.py -k "pending_close_reconciliation"
```

Expected: helper behavior passes.

## Task 5: RED Live-Flat Cleanup Atomicity Tests

**Files:**

- Modify: `tests/test_passive_close.py`
- Later modify: `lightfee/engine/passive_close.py`

- [x] **Step 1: Add BABYUSDT dict-shape recurrence test**

Create a passive-close executor state with:

- `state.pending_close_reconciliations` set to a dict;
- one `BABYUSDT` `pending_passive_closes` item;
- one matching local `open_positions` item;
- fake live truth returning OKX/Bybit flat.

Expected assertions:

```python
assert "entry-1780771924982-BABYUSDT" not in state.pending_passive_closes
assert "entry-1780771924982-BABYUSDT" not in state.open_positions
assert isinstance(state.pending_close_reconciliations, list)
assert any(e["event"] == "recovery.flat" for e in journal_events)
assert not any(e["event"] == "runtime.passive_close_tick_error" for e in journal_events)
```

- [x] **Step 2: Add terminal-event-before-failure test**

Monkeypatch queue registration to raise before state mutation.
Also monkeypatch core clear to raise after queue registration/state mutation
would otherwise have happened.

Expected assertions:

```python
assert "entry-1780771924982-BABYUSDT" in state.pending_passive_closes
assert "entry-1780771924982-BABYUSDT" in state.open_positions
assert not any(e["event"] == "runtime.position_lifecycle_terminal" for e in journal_events)
assert any(e["event"] == "exit.passive_close_live_flat_cleanup_failed" for e in journal_events)
```

- [x] **Step 3: Run tests to verify RED**

Run:

```bash
.venv/bin/pytest -q tests/test_passive_close.py -k "live_flat_cleanup"
```

Expected before implementation: one or both new tests fail.

## Task 6: Implement Live-Flat Cleanup Atomicity

**Files:**

- Modify: `lightfee/engine/passive_close.py`
- Test: `tests/test_passive_close.py`

- [x] **Step 1: Reorder cleanup**

Build the payload first, then perform safe queue registration / no-task-needed
decision before terminal success journals. If registration fails, emit:

```python
self._journal.append(
    "exit.passive_close_live_flat_cleanup_failed",
    {
        "position_id": pending.position_id,
        "symbol": position.symbol,
        "source": source,
        "reason": "pending_close_reconciliation_registration_failed",
        "error": str(error),
    },
)
```

Then return without removing state or emitting terminal success.

- [x] **Step 2: Emit success journals only after safe mutation**

After registration succeeds, remove `pending_passive_closes` and
`open_positions`, clear matching `last_error`, then run
`V1RecoveryDecisionCore` / `clear_legacy_recovery_block_via_core`. Emit:

- `runtime.position_drift_detected`
- `exit.passive_close_fallback_terminal_flat`
- `runtime.position_lifecycle_terminal`
- `recovery.flat`
- `runtime.position_drift_corrected`

If any pre-success step fails, restore managed state/queue/last_error and emit
`exit.passive_close_live_flat_cleanup_failed`.

- [x] **Step 3: Run focused tests**

Run:

```bash
.venv/bin/pytest -q tests/test_passive_close.py -k "live_flat_cleanup or pending_close_reconciliation"
```

Expected: atomicity tests pass and existing passive-close tests remain green.

## Task 7: RED Malformed Queue Consumer Tests

**Files:**

- Modify: `tests/test_pending_entry_v1_semantic_drift.py`
- Modify: `tests/test_supervisor_execution.py`
- Later modify: `lightfee/engine/runtime.py`
- Later modify: `lightfee/engine/supervisor.py`

- [x] **Step 1: Add reconciliation processor malformed queue test**

Set `runtime.state.pending_close_reconciliations` to a dict-shaped queue and run
`_process_pending_close_reconciliations(now_ms)`.

Expected: queue becomes canonical list and invalid/migration evidence is
journaled or retained as explicit fail-safe work; no silent infinite retained
string/key entries.

- [x] **Step 2: Add supervisor venue test**

Set queue with a valid task stored under a dict key. Expected supervised venues
include the task snapshot venues (`okx`, `bybit`).

- [x] **Step 3: Add entry gate conflict test**

Set queue with a valid dict-shaped task for `BABYUSDT` OKX/Bybit. Expected a
candidate on the same symbol/venues is blocked with
`pending_close_reconciliation_conflict`.

- [x] **Step 4: Run tests to verify RED**

Run:

```bash
.venv/bin/pytest -q tests/test_pending_entry_v1_semantic_drift.py tests/test_supervisor_execution.py -k "pending_close_reconciliation"
```

Expected before implementation: at least one test fails because consumers skip
or retain malformed queue data.

## Task 8: Harden Queue Consumers

**Files:**

- Modify: `lightfee/engine/runtime.py`
- Modify: `lightfee/engine/supervisor.py`
- Test: `tests/test_pending_entry_v1_semantic_drift.py`
- Test: `tests/test_supervisor_execution.py`

- [x] **Step 1: Normalize queue at consumer entry**

At the start of `_process_pending_close_reconciliations()`:

```python
self.state.set_pending_close_reconciliations(
    getattr(self.state, "pending_close_reconciliations", [])
)
pending_reconciliations = self.state.pending_close_reconciliations
```

Use the same helper before supervisor venue iteration and entry-gate iteration.

- [x] **Step 2: Preserve invalid evidence**

When a task lacks required fields such as `position_snapshot` venues or order
identity, journal the existing invalid event and apply backoff. Do not silently
drop symbol/venue evidence if it can be recovered from the task.

- [x] **Step 3: Run focused tests**

Run:

```bash
.venv/bin/pytest -q tests/test_pending_entry_v1_semantic_drift.py tests/test_supervisor_execution.py -k "pending_close_reconciliation"
```

Expected: consumer boundary tests pass.

## Task 9: RED Same-Window Live Artifact Tests

Status for strict CL-051 review-fix pass: covered by a recovery-ledger
regression fixture for `MORPHOUSDT`, `MONUSDT`, and `SEIUSDT` with only local
`BABYUSDT` state present. The required recovery-decision, recovery-ledger,
diagnose, and production-health suites were rerun. No production code outside
the CL-051 allowed file map was modified for this task.

**Files:**

- Modify: `tests/engine/test_recovery_ledger.py`
- Modify: `tests/engine/test_recovery_decision_core.py`
- Modify: `tests/test_diagnose_live.py`
- Modify: `tests/ops/test_production_health.py`

- [x] **Step 1: Add unpaired live position fixtures**

Create sanitized exchange truth with local state containing only `BABYUSDT`,
while exchange truth contains Bybit live positions:

```python
[
    {"symbol": "MORPHOUSDT", "venue": "bybit", "side": "buy", "size": 14},
    {"symbol": "MONUSDT", "venue": "bybit", "side": "buy", "size": 1150},
    {"symbol": "SEIUSDT", "venue": "bybit", "side": "buy", "size": 341},
]
```

Expected:

- ledger work item kind is `unpaired_live_position`;
- decision is not `RUNNING_CLEAN`;
- diagnose/prod health are not green;
- no ownership is inferred from `BABYUSDT` local state.

- [x] **Step 2: Run tests to verify RED or coverage**

Run:

```bash
.venv/bin/pytest -q tests/engine/test_recovery_ledger.py tests/engine/test_recovery_decision_core.py tests/test_diagnose_live.py tests/ops/test_production_health.py -k "unpaired_live_position or exchange_truth"
```

Expected: if current code already covers this, tests pass and become regression
coverage. If any fail, fix only the owner/ledger/diagnose boundary.

## Task 10: Final Verification

**Files:** all changed files.

- [x] **Step 1: Run focused suites**

Run:

```bash
.venv/bin/pytest -q tests/persistence/test_v1_state_snapshot_semantics.py tests/test_passive_close.py tests/test_pending_entry_v1_semantic_drift.py tests/test_supervisor_execution.py tests/engine/test_recovery_ledger.py tests/engine/test_recovery_decision_core.py tests/test_diagnose_live.py tests/ops/test_production_health.py
```

Expected: all focused tests pass.

- [x] **Step 2: Run static and smoke checks**

Run:

```bash
python3 -m compileall -q lightfee scripts tests
git diff --check
npx gitnexus status
npx gitnexus detect_changes --repo LightFeeV2
```

Expected:

- compileall passes;
- diff-check passes;
- GitNexus is fresh;
- detect-changes risk is limited to state/recovery/passive-close/runtime/
  supervisor/diagnostic queue and live-artifact coverage.

- [ ] **Step 3: Post-deploy read-only verification checklist**

After deployment only, run from the service credential environment:

```bash
PYTHONPATH=/opt/lightfee-v2 /opt/lightfee-v2/.venv/bin/python3 scripts/verify_production_services.py --json
PYTHONPATH=/opt/lightfee-v2 /opt/lightfee-v2/.venv/bin/python3 scripts/diagnose_live.py --json --since-deploy
```

Expected:

- deploy version matches git head;
- services are active/running singletons;
- no recurring `runtime.passive_close_tick_error`;
- no `local_open_exchange_flat` for `BABYUSDT`;
- focused `BABYUSDT`, `MORPHOUSDT`, `MONUSDT`, `SEIUSDT` truth is flat/no open
  orders or explicitly managed as recovery work;
- market-data warnings remain visible if present, but do not produce false
  clean exchange-truth decisions.

## Self-Review

- Spec coverage:
  - PC-06 maps to Tasks 1-4.
  - PC-07 maps to Tasks 5-6.
  - PC-08 maps to Tasks 7-8.
  - unpaired live artifacts map to Task 9.
  - deployment/service evidence and post-deploy acceptance map to Task 10.
- Placeholder scan: no `TBD`, `TODO`, or "write tests" placeholders remain.
- Type consistency:
  `pending_close_reconciliations`, `_restore_state_from_snapshot_dict`,
  `_clear_live_flat_state`, `_register_close_reconciliation_after_live_flat`,
  `_process_pending_close_reconciliations`, and
  `_gate_pending_close_reconciliation` names match current V2 source.
