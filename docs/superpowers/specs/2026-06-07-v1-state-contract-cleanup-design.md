# V1 State Contract Cleanup Design

Date: 2026-06-07

Status: design draft for a small independent cleanup.

## Source Verdict

This is not a source-port project. It is a scoped cleanup of low-impact
contract drift found by the audit:

- `EngineState.hyperliquid_trading_disabled_reason` is declared twice.
- `EngineState.to_dict()` emits `hyperliquid_trading_disabled_reason` twice.
- `clear_legacy_recovery_block_via_core()` assigns `state.last_error = None`
  even though `EngineState` has no `last_error` dataclass field.

These issues do not require copying more V1 complexity. They should be fixed
because V2's state contract should be explicit and testable.

## Primary Sources

- Audit source: `/Users/wl/Downloads/lightfeev2_v1_audit_report-anti.md`
- V2 state contract: `lightfee/engine/state.py::EngineState`
- V2 recovery cleanup bridge:
  `lightfee/engine/recovery.py::clear_legacy_recovery_block_via_core`
- Current tests:
  `tests/persistence/test_v1_state_snapshot_semantics.py`,
  `tests/recovery/test_restart_recovery_semantics.py`

## Control Range

In scope:

- Remove the duplicate `hyperliquid_trading_disabled_reason` dataclass field.
- Remove the duplicate `hyperliquid_trading_disabled_reason` key from
  `EngineState.to_dict()`.
- Remove the dynamic assignment to nonexistent `state.last_error`.
- Add tests proving the state schema is single-source and the cleanup bridge
  does not create dynamic attributes.

Out of scope:

- Adding a new `EngineState.last_error` field.
- Changing recovery decision semantics.
- Changing risk mode, lifecycle, exchange truth, ledger, or pending-entry
  behavior.
- Editing deploy scripts or production state.

## Required Behavior

- `EngineState` has exactly one `hyperliquid_trading_disabled_reason` dataclass
  field.
- `EngineState.to_dict()` produces exactly one
  `hyperliquid_trading_disabled_reason` key.
- `clear_legacy_recovery_block_via_core()` may clear:
  `lifecycle`, `risk_mode`, `recovery_blocked_reason`,
  `recovery_blocked_at_ms`, and `global_risk_reason` according to the existing
  core decision rules.
- `clear_legacy_recovery_block_via_core()` must not create `last_error` on an
  `EngineState` instance.

## Test Matrix

Add tests before code changes:

- dataclass fields contain `hyperliquid_trading_disabled_reason` exactly once;
- `EngineState.to_dict()` includes that key exactly once by inspecting source or
  using a custom mapping guard;
- legacy block clearing does not create `last_error`;
- existing recovery clear allowlist behavior remains unchanged.

## Parallelization Boundary

This cleanup is independent of the two larger specs and can be done in parallel
with pure test-writing for those specs. It should not be combined in the same
commit as runtime hedge delta or exchange truth behavior changes, because its
risk profile is much smaller.

## Acceptance Criteria

- The tests above fail before the cleanup and pass after it.
- No behavior changes occur outside state serialization and the dynamic
  attribute removal.
- Broad recovery/state persistence tests remain green.
- `git diff --check` is clean.
