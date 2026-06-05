# V1 Pending Entry Source Port Design

Date: 2026-06-05

Status: source-authoritative reset after bug-driven patch review.

Supersedes, for pending-entry passive opening scope only, the looser
"V1-compatible lifecycle core" direction in:

- `docs/superpowers/specs/2026-06-05-v1-trading-lifecycle-core-design.md`
- `docs/superpowers/plans/2026-06-05-v1-trading-lifecycle-core-implementation-plan.md`

Those documents remain useful background for funding and recovery semantics,
but they are not sufficient for this scope because they allow V1-like Python
semantic design. This spec requires source-function-level migration from V1:
copy the V1 control flow and state transitions first, then adapt only the
language/runtime boundary that cannot be copied literally.

## Goal

Replace V2's scattered pending-entry passive opening behavior with a
source-level port of the V1 Rust pending-entry lifecycle. The implementation
must be organized by V1 source functions and state structures, not by observed
production bug symptoms. The default implementation strategy is source-copy:
read the V1 function, create the V2 target with the same branch order and state
mutation meaning, and only then wire it into the Python runtime.

## Non-Goals

- Do not continue adding "next bug segment" fixes directly in `runtime.py`.
- Do not invent a new V2 lifecycle design that only looks like V1.
- Do not keep a V2 helper because it "passes the incident" unless it maps to a
  named V1 function or an approved boundary deviation.
- Do not relax V1 close semantics to reduce quick-flat counts.
- Do not submit live orders, cancel orders, mutate production runtime state, or
  deploy while performing the port.
- Do not port unrelated V1 modules unless directly called by pending-entry
  passive opening lifecycle functions.

## V1 Source Of Truth

Use V1 at `/Users/wl/projects/LightFee` as authoritative.

Primary files:

- `/Users/wl/projects/LightFee/src/execution_core/entry_sync.rs`
- `/Users/wl/projects/LightFee/crates/lightfee-engine/src/lib.rs`

Direct dependencies allowed only when referenced by the primary files:

- `/Users/wl/projects/LightFee/src/execution_core/passive_phase.rs`
- `/Users/wl/projects/LightFee/src/execution_core/maker_profile.rs`
- `/Users/wl/projects/LightFee/src/runtime_state/config.rs`
- V1 candidate/market/guard functions only for
  `current_tradeable_candidate_for_terminal_taker_fallback`.

## Source-Port Standard

A V2 function is source-level acceptable only if all of these are true:

1. It maps to a named V1 function or a documented contiguous V1 source block.
2. Its branch order matches V1 unless a Python boundary makes that impossible.
3. Its state mutations match V1 field-by-field.
4. Its journal/event reasons preserve V1 decision meaning.
5. Any deviation is explicitly documented with:
   - V1 source line/function;
   - V2 boundary reason;
   - why exact copy is impossible;
   - regression test that pins the accepted deviation.

If a function cannot satisfy this standard, it must be marked
`boundary-adapted` instead of `ported`, and the deviation must be recorded in
`docs/parity/2026-06-05-v1-pending-entry-source-port-matrix.md`. If the
deviation is meant to survive beyond the implementation branch, also record it
in `docs/parity/approved_deviations.md` or create that file if it does not
exist.

## Source-Copy Priority

When there is a conflict between current V2 behavior and V1 source behavior,
choose in this order:

1. direct V1 source copy;
2. boundary adaptation with the same V1 branch order and state semantics;
3. explicit out-of-scope/deviation record with a reason;
4. no implementation.

Do not choose an incident-specific V2 shortcut over a copyable V1 function.

## Required V1 Function Map

The implementation must create and maintain a migration matrix with these rows
before changing runtime behavior.

| V1 source item | Required V2 target | Status requirement |
|---|---|---|
| `PendingEntryHedge` | `PendingEntry` plus source-port state dataclasses | Must be field-complete or explicitly abandoned with reason. |
| `PendingEntryRemainderSlice` | `PendingEntryRemainderSlice` | Must be source-ported. |
| `PassivePhaseState` for entry | `PendingEntryPassivePhaseState` | Must be source-ported for pending entry, not reused from passive close. |
| `PendingPassiveOrder` | `PendingPassiveOrder` | Must be field-complete enough for lifecycle. |
| `PendingPassiveManagerRuntime` | `PassiveOrderManagerRuntime` or pending-entry equivalent | Must not be just persistence-only if V1 manager decisions are in scope. |
| `maintain_pending_entry_passive_order` | pending-entry lifecycle driver | Must be source-ported control flow. |
| `note_pending_entry_passive_submit` | passive submit side-effect helper | Must be source-ported. |
| `current_tradeable_candidate_for_terminal_taker_fallback` | frozen-candidate recheck boundary | Must be source-ported or boundary-adapted with guard parity. |
| `try_terminal_taker_fallback` | terminal taker fallback | Must use frozen candidate, runtime guards, and ForceStandard semantics. |
| `submit_pending_entry_passive_cycle` | passive cycle submit | Must be source-ported. |
| `handle_pending_entry_zero_fill_completion` | zero-fill phase lifecycle | Must be source-ported. |
| `try_repost_pending_entry_remainder` | balanced partial remainder repost | Must be source-ported or explicitly out-of-scope with reason. |
| `apply_pending_entry_passive_progress` | maker progress application | Must be source-ported. |
| `hedge_pending_entry_delta` | missing hedge driver | Must source-port quantity, price, FIFO, inflight, deadlines, and terminal handling. |

## Current Patch Audit Requirement

The current worktree contains a mix of useful source ports and bug-driven
semantic patches. Before implementing this spec, audit each changed hunk and
classify it:

### Keep

- `OpenPosition` and `PendingEntry` missing fields that directly exist in V1.
- `PendingEntryRemainderSlice` and FIFO remainder helpers where line-by-line
  semantics match V1.
- `build_open_position` matched quantity, entry notional, fee prorating, and
  entry net initialization.
- Funding capture based on V1 entry notional.
- Persistence roundtrip for V1 fields.

### Replace

- Runtime-local phase mutation helpers if they do not map to V1 function
  boundaries.
- `_try_pending_entry_terminal_taker_fallback` if it bypasses frozen-candidate
  recheck and V1 ForceStandard candidate flow.
- Any direct `EntryContext` reconstruction in runtime that exists only to patch
  an incident.
- Any `phase_state` update that does not occur inside a V1-mapped lifecycle
  function.

### Revert Or Restore

- Tests whose expectations encode an accepted bug-driven shortcut rather than
  V1 source behavior.
- Runtime branches that suppress quick flats by terminalizing or skipping work
  without the corresponding V1 source branch.

Do not use `git reset` or revert unrelated user changes. Revert by targeted
patches only after the audit matrix says which hunk is being replaced. When a
current hunk contains both useful field additions and wrong lifecycle logic,
split it: keep the source-copied field/state part and restore the behavior
through the V1-mapped lifecycle function.

## Target Architecture

Create a pending-entry source-port module that owns V1 lifecycle decisions:

- `lightfee/engine/pending_entry_lifecycle.py`

This module should expose functions named after V1 source functions where
practical. `LiveRuntime` should orchestrate IO only:

1. query/cancel/submit through adapters;
2. pass state and observations to source-ported lifecycle functions;
3. execute returned intents;
4. persist/journal outcomes.

Runtime must not be the place where phase counters, zero-fill budgets, frozen
candidate fallback, and remainder consumption are independently reinvented.
It may keep adapter-specific wrappers, but those wrappers must delegate to the
source-port module for lifecycle decisions.

## Boundary Adaptation Rules

Python/V2 boundaries may differ from Rust/V1 in these places only:

- adapter API names and return objects;
- journal API shape;
- snapshot/cache access;
- enum representation;
- dataclass serialization;
- absence of a V1 `MarketView` object, provided the replacement carries the
  same candidate/quote/guard facts.

Every boundary adaptation must have a regression test and a note in the
migration matrix.

## Required Tests

Add source parity tests that are not incident-first:

- `tests/engine/test_v1_pending_entry_lifecycle_parity.py`

Each test must name the V1 function and branch it covers. Incident tests can
remain, but they are secondary regression evidence.

Required test groups:

1. pending entry state construction and recovery;
2. passive progress apply and remainder slice accounting;
3. zero-fill cycle recording and retry delay;
4. same-phase passive cycle submit;
5. high-slippage to low-slippage phase switch;
6. low-slippage to dual-taker transition;
7. terminal taker fallback with frozen-candidate recheck;
8. fallback deferred when ForceStandard open does not materialize;
9. partial maker fill hedge FIFO and remainder repost;
10. old incident fixtures continue to pass without symbol-specific branches.

## Acceptance Criteria

- A V1-to-V2 function map exists and every row is `ported`,
  `boundary-adapted`, or `explicitly-out-of-scope`.
- Runtime bug-driven helpers are removed or replaced by mapped source-port
  functions.
- Focused pending-entry lifecycle parity tests pass.
- Existing incident tests pass as regression tests, not as implementation
  drivers.
- Existing broad entry/runtime/recovery/close/persistence regression suite
  passes.
- `compileall`, `git diff --check`, and GitNexus `detect-changes` pass.
