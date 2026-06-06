# Implementation Plan: V1 State Contract Cleanup

Date: 2026-06-07

Spec:
`docs/superpowers/specs/2026-06-07-v1-state-contract-cleanup-design.md`

## Goal

Remove low-impact state contract drift without changing recovery behavior.

## Constraints

- This is intentionally small and independent.
- Do not add `EngineState.last_error`.
- Do not modify runtime, exchange truth, ledger, pending-entry lifecycle, or
  production scripts.
- Before modifying functions/classes, run GitNexus upstream impact commands.

## Files

Expected production targets:

- `lightfee/engine/state.py`
- `lightfee/engine/recovery.py`

Expected tests:

- `tests/persistence/test_v1_state_snapshot_semantics.py`
- `tests/recovery/test_restart_recovery_semantics.py`

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
npx gitnexus impact --repo LightFeeV2 EngineState --include-tests
npx gitnexus impact --repo LightFeeV2 clear_legacy_recovery_block_via_core --include-tests
```

If impact is `HIGH` or `CRITICAL`, report direct callers and risk before
editing.

## Step 1: Add RED Tests

Add to `tests/persistence/test_v1_state_snapshot_semantics.py`:

- `test_engine_state_declares_hyperliquid_disabled_reason_once`
- `test_engine_state_to_dict_emits_hyperliquid_disabled_reason_once`

The field test should inspect `dataclasses.fields(EngineState)`. The
serialization test should inspect the returned keys or the source mapping so a
future duplicate cannot silently overwrite itself.

Add to `tests/recovery/test_restart_recovery_semantics.py`:

- `test_clear_legacy_recovery_block_does_not_create_last_error_attribute`

The test should call `clear_legacy_recovery_block_via_core()` with an allowlisted
legacy block and a core decision that permits clearing, then assert
`hasattr(state, "last_error") is False`.

Run:

```bash
.venv/bin/pytest -q tests/persistence/test_v1_state_snapshot_semantics.py -k "hyperliquid_disabled_reason"
.venv/bin/pytest -q tests/recovery/test_restart_recovery_semantics.py -k "last_error"
```

Expected result before implementation: the new tests expose the duplicate field
or dynamic attribute drift.

## Step 2: Clean `EngineState`

Edit `lightfee/engine/state.py`.

Required changes:

- Keep one `hyperliquid_trading_disabled_reason: str | None = None` field.
- Keep one `to_dict()` key for `hyperliquid_trading_disabled_reason`.
- Do not move unrelated state fields.

Run:

```bash
.venv/bin/pytest -q tests/persistence/test_v1_state_snapshot_semantics.py -k "hyperliquid_disabled_reason"
```

Expected result: state contract tests pass.

## Step 3: Clean Legacy Recovery Bridge

Edit `lightfee/engine/recovery.py`.

Required change:

- Remove `state.last_error = None` from
  `clear_legacy_recovery_block_via_core()`.
- Do not change the allowlist, core-decision conditions, journal event, or
  lifecycle/risk-mode assignments.

Run:

```bash
.venv/bin/pytest -q tests/recovery/test_restart_recovery_semantics.py -k "last_error or legacy_recovery_block"
```

Expected result: legacy cleanup tests pass and behavior remains unchanged except
for avoiding the dynamic attribute.

## Step 4: Verification Gate

Run focused tests:

```bash
.venv/bin/pytest -q tests/persistence/test_v1_state_snapshot_semantics.py tests/recovery/test_restart_recovery_semantics.py
```

Run broader recovery/state checks:

```bash
.venv/bin/pytest -q tests/recovery tests/test_persistence.py tests/persistence/test_journal_event_semantics.py tests/persistence/test_v1_state_snapshot_semantics.py
```

Run static checks:

```bash
git diff --check
npx gitnexus status
npx gitnexus detect_changes --repo LightFeeV2
```

Expected result:

- tests pass;
- GitNexus is fresh;
- `detect_changes` shows only the small state/recovery cleanup scope.

## Parallel Execution Notes

This spec can be implemented in parallel with pure test-writing for the two
larger specs because it touches only `state.py`, `recovery.py`, and focused
state/recovery tests. Keep it out of the same branch or commit as runtime hedge
delta wiring to preserve a small review surface.
