# V1 Recovery Decision Core Closed Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development to execute this plan.

**Goal:** Implement one V1-compatible pure decision core that closes the
`ambiguous_exchange_truth -> stale_recovery_block_cleared` bug family without
making normal entry harder or scattering new runtime conditions.

**Architecture:** Add a pure `V1RecoveryDecisionCore` that consumes normalized
local state, exchange truth, owner evidence, prior block state, and operator
policy. Runtime, recovery ledger, entry gate, diagnose, and production health
consume its typed decision. Existing modules remain responsible for I/O,
normalization, owner indexing, terminalization, and execution.

**Tech Stack:** Python, pytest, existing LightFeeV2 engine modules, GitNexus
impact workflow before code edits, V1 Rust source as semantic reference.

---

## Global Rules

- Start with RED tests. Do not edit runtime behavior before the oscillation and
  entry-availability cases fail for the right reason.
- Before modifying any function/class/method, refresh GitNexus for `LightFeeV2`
  and run upstream impact analysis for the target symbol.
- Do not submit orders, cancel orders, edit production runtime state, or deploy
  while collecting evidence.
- Do not add symbol-specific branches for `TRXUSDT`, `SEIUSDT`, or any incident
  symbol.
- Do not copy V1 Rust file layout wholesale. Copy semantics into one Python
  boundary.
- Runtime may orchestrate side effects; the decision core must remain pure.
- Entry must remain available for flat/no-local-work evidence-gap states.
- `ambiguous_exchange_truth` is an evidence quality label, not standalone
  recovery work.

## Task 1: Add RED Decision-Core Tests

**Files:**

- `tests/engine/test_recovery_decision_core.py`

**Tests to add first:**

```python
def test_flat_no_local_work_truth_gap_runs_with_evidence_gap_and_allows_entry():
    snapshot = RecoveryEvidenceSnapshot(
        local_open_positions=[],
        pending_entries=[],
        residual_repairs=[],
        passive_closes=[],
        exchange_truth=ExchangeTruthSnapshot(available=False, unsupported=True),
        prior_recovery_block_reason=None,
    )

    decision = V1RecoveryDecisionCore().decide(snapshot)

    assert decision.kind == RecoveryDecisionKind.RUNNING_WITH_EVIDENCE_GAP
    assert decision.entry_allowed is True
    assert decision.block_reason is None
    assert decision.clear_previous_block is True
```

```python
def test_local_recovery_work_plus_unavailable_truth_blocks_new_entry():
    snapshot = snapshot_with_pending_entry_and_unavailable_truth()

    decision = V1RecoveryDecisionCore().decide(snapshot)

    assert decision.kind == RecoveryDecisionKind.RISK_ONLY_WAIT_FOR_TRUTH
    assert decision.entry_allowed is False
    assert decision.block_reason == "truth_unavailable_for_required_recovery"
```

```python
def test_orphan_live_open_order_blocks_as_live_artifact():
    snapshot = snapshot_with_live_non_reduce_order_without_owner()

    decision = V1RecoveryDecisionCore().decide(snapshot)

    assert decision.kind == RecoveryDecisionKind.BLOCK_OR_FLATTEN_LIVE_ARTIFACT
    assert decision.block_reason == "orphan_maker_order"
```

```python
def test_previous_ambiguous_block_clears_only_through_core():
    snapshot = clean_snapshot_with_prior_block("exchange_truth_recovery_ledger_blocked")

    decision = V1RecoveryDecisionCore().decide(snapshot)

    assert decision.clear_previous_block is True
    assert decision.clear_reason == "core_running_clean"
```

**Acceptance:**

- RED fails because `V1RecoveryDecisionCore` does not exist or current semantics
  still treat the evidence gap as global blocking work.

## Task 2: Implement The Pure Core

**Files:**

- `lightfee/engine/recovery_decision_core.py`

**Required shapes:**

- `RecoveryEvidenceSnapshot`
- `RecoveryEvidenceClass`
- `RecoveryDecisionKind`
- `RecoveryDecision`
- `V1RecoveryDecisionCore`

**Boundary requirements:**

- no adapter calls.
- no order submit/cancel/flatten.
- no journal writes.
- no runtime state mutation.
- deterministic output for deterministic input.

**Core decision order:**

1. preserve explicit operator fail-closed.
2. classify concrete live artifacts.
3. resolve owner evidence.
4. classify local recovery work.
5. classify truth availability only after knowing whether truth is required.
6. choose lifecycle and entry policy.
7. choose block/clear action.

Add a short comment near the evidence-gap branch:

```python
# A probe gap is not recovery work by itself. It becomes blocking only when
# existing local work or a concrete live artifact requires truth to proceed.
```

**Acceptance:**

- Task 1 tests pass.
- Core has no imports from runtime executors or venue adapters.

## Task 3: Make RecoveryLedger Consume Core Semantics

**Files:**

- `lightfee/engine/recovery_ledger.py`
- `tests/engine/test_recovery_ledger.py`
- `tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py`

**Work:**

- Keep `RecoveryLedger` as a work-item builder.
- Remove or downgrade any rule that directly turns unavailable truth into
  blocking `ambiguous_exchange_truth` when no local recovery work or live
  artifact exists.
- Feed ledger evidence into `V1RecoveryDecisionCore` for final block/clear
  semantics.
- Keep orphan maker order and unpaired live position work items.

**Acceptance:**

- Existing `TRXUSDT` and `SEIUSDT` fixture protections still pass.
- New evidence-gap tests prove no false global recovery block.

## Task 4: Integrate Runtime Lifecycle Block/Clear Through Core

**Files:**

- `lightfee/engine/runtime.py`
- targeted runtime tests in `tests/test_runtime_entry_flow.py` or a smaller
  adjacent suite matching existing patterns.

**Work:**

- Runtime refresh builds `RecoveryEvidenceSnapshot`.
- Runtime applies only the core result for core-owned recovery block state.
- Existing `clear_stale_recovery_block_if_recovery_clean()` is either retired or
  constrained to migration fallback:
  - it may clear obsolete pre-core reasons only when the core says
    `RUNNING_CLEAN` or `RUNNING_WITH_EVIDENCE_GAP`.
  - it must not independently decide that ledger/core-blocking work is clean.
- Add a short comment at integration:

```python
# Block and clear are both driven by V1RecoveryDecisionCore so evidence-gap
# states cannot oscillate between ledger block and stale-block cleanup.
```

**Acceptance:**

- A prior `exchange_truth_recovery_ledger_blocked` no longer oscillates through a
  separate stale-clean authority.
- Operator fail-closed remains preserved.

## Task 5: Route Entry Gate Through Core Entry Policy

**Files:**

- `lightfee/engine/runtime.py`
- `tests/test_runtime_entry_flow.py`

**Work:**

- Replace direct coarse `RecoveryLedger.has_blocking_work()` entry decisions with
  `RecoveryDecision.entry_allowed` and `entry_block_reason`.
- Ensure candidate-scoped probe errors do not mutate global recovery state.
- Preserve existing blockers:
  - live artifact.
  - local recovery work.
  - unresolved pending/passive/residual/open work.
  - same-symbol venue-overlap pending protection.
  - first-funding horizon.
  - deterministic admission reject.
  - operator fail-closed.

**Acceptance:**

- Flat/no-local-work truth gap does not block normal entry.
- Same-symbol/venue-overlap pending protection still blocks.
- Orphan live order and unpaired live position still block.

## Task 6: Align Diagnose And Production Health

**Files:**

- `scripts/diagnose_live.py`
- `scripts/verify_production_services.py`
- `tests/test_diagnose_live.py`
- `tests/ops/test_production_health.py`

**Work:**

- Consume or mirror the same core classification for:
  - local-flat/live-position critical mismatch.
  - local-flat/non-reduce open-order critical mismatch.
  - evidence-gap warning.
  - production high-confidence truth requirement.
- Keep the distinction between runtime entry availability and production
  high-confidence acceptance.

**Acceptance:**

- Evidence gap can be warning/incomplete evidence without being runtime recovery
  work.
- Concrete live artifacts remain unhealthy/high risk.

## Task 7: Clean Up Old Semantic Authorities

**Files:**

- `lightfee/engine/recovery_ledger.py`
- `lightfee/engine/runtime.py`
- diagnose/health scripts as needed.
- docs touched in this plan.

**Static checks:**

```bash
rg "ambiguous_exchange_truth" lightfee tests scripts docs
rg "clear_stale_recovery_block_if_recovery_clean|stale_recovery_block_cleared" lightfee tests scripts docs
rg "has_blocking_work" lightfee/engine
```

**Work:**

- Every remaining `ambiguous_exchange_truth` path must be either:
  - evidence gap,
  - truth unavailable for required recovery,
  - historical docs/tests, or
  - a compatibility alias with core-owned semantics.
- Every stale-block clearer must say why it cannot override the core.
- Entry gate direct ledger reads must be gone or only used as core input.

**Acceptance:**

- No old path can create a global recovery block from probe availability alone.
- No old path can clear a core-owned blocker without a core clear decision.

## Task 8: Verification Matrix

Run focused suites first:

```bash
.venv/bin/pytest tests/engine/test_recovery_decision_core.py -q
.venv/bin/pytest tests/engine/test_recovery_ledger.py tests/engine/test_recovery_owner_index.py tests/engine/test_exchange_truth_runtime.py -q
.venv/bin/pytest tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py -q
.venv/bin/pytest tests/test_runtime_entry_flow.py -q
.venv/bin/pytest tests/test_diagnose_live.py tests/ops/test_production_health.py -q
```

Then run adjacent pending/recovery suites selected by GitNexus impact. If the
change touches shared runtime paths, run full pytest.

Always run:

```bash
python -m compileall lightfee scripts tests
git diff --check
```

Production verification, when deploy is explicitly requested later, must be
read-only first:

```bash
scripts/verify_production_services.py --json
scripts/diagnose_live.py --json --since-deploy
scripts/verify_deploy_manifest.py --check /opt/lightfee-v2
```

## Done Criteria

- The new pure core owns block and clear.
- `ambiguous_exchange_truth` no longer means global blocker by default.
- Entry remains available for flat/no-local-work evidence-gap states.
- Real live artifacts still block/manage/flatten.
- Runtime, ledger, entry gate, diagnose, and production health agree on the same
  evidence classification.
- Existing CL-048/049/050 protections remain covered.
- Docs and comments make the authority boundary obvious for future maintainers.
