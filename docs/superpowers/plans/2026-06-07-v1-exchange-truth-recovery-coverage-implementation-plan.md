# Implementation Plan: V1 Exchange Truth Recovery Coverage

Date: 2026-06-07

Spec:
`docs/superpowers/specs/2026-06-07-v1-exchange-truth-recovery-coverage-design.md`

## Goal

Prove and tighten the V2 lightweight exchange-truth recovery closure without
rewriting it into Rust V1's runtime layout.

## Constraints

- Start with tests and fixture-only changes.
- Keep runtime collector changes narrow and evidence-preserving.
- Do not deploy, call live venues, submit orders, cancel orders, or mutate
  runtime state.
- Do not combine this work with pending-entry hedge delta runtime wiring.
- Before modifying functions/classes, run GitNexus upstream impact commands.

## Files

Expected production targets:

- `lightfee/engine/exchange_truth.py`
- `lightfee/engine/recovery_ledger.py`
- `lightfee/engine/recovery_owner_index.py`
- `lightfee/engine/recovery_decision_core.py`
- `lightfee/engine/runtime.py`
- `scripts/diagnose_live.py`
- `scripts/verify_production_services.py`

Expected tests:

- `tests/engine/test_exchange_truth_runtime.py`
- `tests/engine/test_recovery_ledger.py`
- `tests/engine/test_recovery_owner_index.py`
- `tests/engine/test_recovery_decision_core.py`
- `tests/test_diagnose_live.py`
- `tests/ops/test_production_health.py`
- `tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py`

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

Before editing, run:

```bash
npx gitnexus impact --repo LightFeeV2 ExchangeTruthSnapshot --include-tests
npx gitnexus impact --repo LightFeeV2 normalize_exchange_truth_payload --include-tests
npx gitnexus impact --repo LightFeeV2 snapshot_from_legacy_payload --include-tests
npx gitnexus impact --repo LightFeeV2 RecoveryLedger --include-tests
npx gitnexus impact --repo LightFeeV2 RecoveryOwnerIndex --include-tests
npx gitnexus impact --repo LightFeeV2 V1RecoveryDecisionCore --include-tests
npx gitnexus impact --repo LightFeeV2 _collect_recovery_ledger_exchange_truth --include-tests
npx gitnexus impact --repo LightFeeV2 _refresh_recovery_ledger_from_exchange_truth --include-tests
```

Report and pause before code edits if impact is `HIGH` or `CRITICAL`.

## Step 1: Add RED Exchange Truth Payload Tests

Expand `tests/engine/test_exchange_truth_runtime.py`.

Required test names:

- `test_multi_venue_positions_and_open_orders_roundtrip_through_legacy_payload`
- `test_partial_venue_timeout_preserves_available_venues_and_missing_evidence`
- `test_unsupported_symbol_evidence_is_not_treated_as_successful_flat_truth`
- `test_retryable_probe_error_survives_fetch_status_errors_and_probe_evidence`
- `test_exchange_truth_schema_version_survives_normalization_when_present`

Each test must assert both the normalized dictionary and the
`ExchangeTruthSnapshot` roundtrip when a snapshot object is involved.

Run:

```bash
.venv/bin/pytest -q tests/engine/test_exchange_truth_runtime.py
```

Expected result before implementation: only the newly added contract cases fail
if current normalization drops fields or merges evidence classes.

## Step 2: Fix Exchange Truth Normalization Narrowly

Modify `lightfee/engine/exchange_truth.py` only as required by Step 1.

Rules:

- Preserve input schema/version fields when present.
- Do not convert unsupported-symbol evidence into successful flat evidence.
- Keep timeout evidence queryable through `probe_evidence`.
- Keep backward-compatible `positions` and `open_orders` nesting.

Run:

```bash
.venv/bin/pytest -q tests/engine/test_exchange_truth_runtime.py
```

Expected result: payload tests pass.

## Step 3: Add RED Multi-Symbol Ledger Tests

Expand `tests/engine/test_recovery_ledger.py` and
`tests/engine/test_recovery_owner_index.py`.

Required test names:

- `test_multi_symbol_owned_work_blocks_same_symbol_without_global_block`
- `test_multi_symbol_orphan_maker_order_globally_blocks_while_owned_work_remains_visible`
- `test_partial_truth_with_local_pending_entry_records_owned_work_and_truth_gap`
- `test_journal_owned_order_is_owned_pending_entry_when_local_pending_is_absent`

Each test must assert work item kind, owner confidence, blocking scope, and
entry-gate result for same-symbol and unrelated-symbol candidates.

Run:

```bash
.venv/bin/pytest -q tests/engine/test_recovery_ledger.py tests/engine/test_recovery_owner_index.py
```

Expected result before implementation: failures identify missing or ambiguous
ledger evidence handling.

## Step 4: Tighten Ledger and Owner Classification

Modify `recovery_ledger.py` or `recovery_owner_index.py` only if Step 3 exposes
a real gap.

Rules:

- Concrete orphan maker order and unpaired live position remain global blocks.
- `ambiguous_exchange_truth` alone remains non-global and non-blocking when no
  local work exists.
- Local pending work plus unavailable/partial truth remains managed work plus
  evidence gap, not running clean.
- Owner matches require order id, client id, position id, or journal evidence;
  do not infer ownership from symbol alone for live orders.

Run:

```bash
.venv/bin/pytest -q tests/engine/test_recovery_ledger.py tests/engine/test_recovery_owner_index.py tests/engine/test_recovery_decision_core.py
```

Expected result: pure recovery classification remains green.

## Step 5: Add Runtime Collector Tests

Add focused runtime tests in
`tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py` or a new
runtime test file if the existing harness is too broad.

Required test names:

- `test_runtime_collects_position_and_open_order_truth_for_each_requested_symbol`
- `test_runtime_collects_partial_venue_error_without_dropping_successful_venue`
- `test_runtime_refresh_ledger_preserves_multi_symbol_work_items`

Use fake adapters. Do not call live exchange clients.

Run:

```bash
.venv/bin/pytest -q tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py -k "truth or ledger or multi_symbol"
```

Expected result: failures point to runtime collector or refresh behavior only.

## Step 6: Fix Runtime Collector if Needed

Modify `lightfee/engine/runtime.py` only around:

- `_collect_recovery_ledger_exchange_truth`
- `_refresh_recovery_ledger_for_symbols`
- `_refresh_recovery_ledger_from_exchange_truth`

Rules:

- Preserve per-venue and per-symbol probe evidence.
- Do not drop successful venue truth because another venue failed.
- Do not mark truth clean when open-order evidence is missing for a requested
  symbol.
- Keep `V1RecoveryDecisionCore` as the single block/clear authority.

Run:

```bash
.venv/bin/pytest -q tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py tests/engine/test_recovery_decision_core.py
```

Expected result: runtime collector tests and decision core tests pass together.

## Step 7: Add Diagnostic and Verifier Agreement Tests

Expand `tests/test_diagnose_live.py` and `tests/ops/test_production_health.py`.

Required test names:

- `test_diagnose_and_runtime_recovery_decision_agree_on_partial_truth_payload`
- `test_verify_production_services_preserves_exchange_truth_probe_evidence`
- `test_production_gate_does_not_report_clean_when_open_orders_present`

Each diagnostic test must use a sanitized payload, not live adapters, and assert
the same `RecoveryDecisionKind` or production blocker fingerprint.

Run:

```bash
.venv/bin/pytest -q tests/test_diagnose_live.py tests/ops/test_production_health.py -k "exchange_truth or recovery_decision or production_gate"
```

Expected result: diagnose/verifier consumers use the shared normalized truth
fields.

## Step 8: Verification Gate

Run focused suites:

```bash
.venv/bin/pytest -q tests/engine/test_exchange_truth_runtime.py tests/engine/test_recovery_ledger.py tests/engine/test_recovery_owner_index.py tests/engine/test_recovery_decision_core.py
.venv/bin/pytest -q tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py
.venv/bin/pytest -q tests/test_diagnose_live.py tests/ops/test_production_health.py
```

Run broad recovery suites:

```bash
.venv/bin/pytest -q tests/recovery tests/test_engine_recovery.py tests/test_recovery_reconciliation.py tests/engine/test_pending_entry_terminalizer.py tests/test_v1_parity_pending_entry_recovery_red.py tests/test_persistence.py tests/persistence/test_journal_event_semantics.py tests/persistence/test_v1_state_snapshot_semantics.py
```

Run static checks:

```bash
git diff --check
npx gitnexus status
npx gitnexus detect_changes --repo LightFeeV2
```

Expected result:

- exchange truth payload tests cover the audit gaps;
- multi-symbol recovery coverage exists;
- diagnose and verifier agree with runtime normalized truth;
- no unrelated files are changed.

## Parallel Execution Notes

Safe to parallelize:

- Step 1 payload tests and Step 3 ledger tests.
- Step 7 diagnostic/verifier tests once the normalized payload contract is
  written.

Keep serial:

- Step 6 runtime changes.
- Any decision-core change that alters entry admission or block clearing.
- Any work overlapping with pending-entry hedge delta runtime edits.
