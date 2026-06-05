# V1 Trading Lifecycle Core Design

Date: 2026-06-05

Status: design approved by product direction; implementation plan follows in
`docs/superpowers/plans/2026-06-05-v1-trading-lifecycle-core-implementation-plan.md`.

## Goal

Reduce quick open-then-close behavior and long-term maintenance risk by
collapsing V2's scattered entry, pending-entry, close, residual, recovery, and
exchange-truth decisions into one V1-compatible trading lifecycle semantic core.

This is not a request to port Rust files line by line. V1 remains the
authoritative trading contract. V2 should keep Pythonic execution boundaries,
but all compatibility-sensitive lifecycle decisions must come from shared pure
semantic components instead of ad hoc runtime branches.

## Problem Statement

V1 behaved well because entry, pending-entry terminality, funding stage
progression, close intent, residual repair, recovery, and live truth formed one
state machine. V2 reimplemented these concepts across separate modules and
runtime branches:

- candidate discovery and final entry window;
- Local-L2 / quote-lease readiness;
- dispatch and passive maker pending entry;
- pending-entry reconciliation and finalization;
- open-position funding capture and close reason;
- passive close and residual repair;
- startup/runtime recovery;
- diagnose and production health exchange truth.

The result is repeated "V1 semantic fragment" fixes: one bug closes a symbol
shape, another production window exposes the same contract gap at a different
boundary. Quick flats are one visible symptom: entry may treat a candidate or
pending fill as valid while the close lane sees the same lifecycle state as
immediately ready to close.

## Design Principles

1. Entry and close share lifecycle facts, not execution details.
2. Entry must never call a full close-reason simulator.
3. Close must keep V1 close semantics and must not be weakened to hide quick
   flats.
4. Runtime orchestrates decisions and side effects; it does not own V1 trading
   semantics.
5. Pure semantic decisions return `allowed`, `reason`, and `evidence`; they do
   not submit orders, cancel orders, mutate state, or write journals.
6. Exchange truth is a runtime input. Local flat is not sufficient unless the
   lifecycle core proves ledger flatness.
7. Positive fill or live exposure is never discarded to avoid a quick flat.
   Already-created exposure must be owned, residualized, closed, or fail-closed.
8. New production recurrences map to this lifecycle contract before code
   changes. Covered rows get evidence; uncovered rows get RED tests.

## Existing Components To Reuse

The lifecycle core must reuse and elevate the already-created V1 parity pieces:

- `RecoveryLedger` in `lightfee/engine/recovery_ledger.py`
- `ExchangeTruthSnapshot` normalization in `lightfee/engine/exchange_truth.py`
- `RecoveryOwnerIndex` in `lightfee/engine/recovery_owner_index.py`
- `PendingEntryTerminalizer` in `lightfee/engine/pending_entry_terminalizer.py`
- V1 close helpers in `lightfee/engine/exit_decision.py`
- V1 discovery window logic in `lightfee/strategy/discovery.py`
- The contract matrix in
  `docs/bugs/contracts/pending-entry-live-truth-contract.md`

The root fix should connect these into one lifecycle boundary instead of adding
a parallel framework.

## Target Architecture

### `FundingLifecycle`

Create a pure shared funding semantic surface, for example
`lightfee/engine/funding_lifecycle.py`.

Responsibilities:

- normalize candidate, pending-entry, and open-position funding timestamps;
- derive first and second funding milestones;
- compute remaining time to first and second funding;
- compute entry horizon admissibility;
- compute funding capture stage transitions used by close semantics;
- expose the same timestamp evidence to entry, pending finalization, normal
  close, passive close, recovery, and diagnostics.

Required entry horizon rule:

```text
effective_min_before_first_funding_ms =
    max(
        strategy.min_scan_minutes_before_funding * 60_000,
        strategy.entry_min_first_funding_remaining_secs * 1_000
    )
```

The new strategy config `entry_min_first_funding_remaining_secs` defaults to
`60`. If remaining time to first funding is below the effective minimum, a new
entry must not be submitted as normal entry risk.

This is a lifecycle rule, not a standalone patch. It must be applied wherever
V2 can turn candidate or pending-entry work into new risk:

- final candidate selection;
- dispatch pre-submit;
- passive/pending entry viability before repost or continuing maker work;
- pending finalization when the system decides whether a filled state is a
  normal open position, recovery-owned exposure, residual repair, or fail-closed
  work.

If positive fill or live exposure already exists, the system must not discard it
because it is too close to funding. It must move through owner recovery and
close/residual handling rather than pretending no exposure exists.

### `V1TradingLifecycle`

Create a pure facade, for example `lightfee/engine/v1_lifecycle.py`.

Responsibilities:

- gather state required for one lifecycle decision;
- call `FundingLifecycle`, `RecoveryLedger`, `RecoveryOwnerIndex`, and
  `PendingEntryTerminalizer`;
- return a decision object for the runtime to execute.

Decision surfaces:

```text
entry_admissibility(candidate, state, exchange_truth, now_ms, config)
pending_entry_viability(pending, state, exchange_truth, now_ms, config)
pending_terminality(pending, live_truth, now_ms, config)
open_position_funding_update(position, now_ms, config)
close_intent(position, now_ms, config)
ledger_flatness(state, exchange_truth, now_ms, config)
```

Entry-specific decisions must not depend on `CloseExecutor`,
`PassiveCloseExecutor`, or venue order-submission code.

Close-specific decisions may use the same funding lifecycle facts, but they
remain close decisions. Entry does not need to know whether a future close would
be passive or aggressive.

### Runtime Integration

`LiveRuntime` should become thinner:

1. Build or refresh exchange truth when supported.
2. Build owner evidence and recovery ledger.
3. Ask `V1TradingLifecycle` for entry or pending decisions.
4. Execute the returned intent through existing executors.
5. Write journal evidence using stable lifecycle reasons.

Runtime should not duplicate funding horizon, pending terminality, owner, or
ledger-flatness decisions across local branches.

### Lifecycle State Flow

```text
Candidate
  -> EntryIntent
  -> PendingEntry
  -> OpenPosition
  -> CloseIntent / ResidualRepair / RecoveryWork
  -> LedgerProvenFlat
```

Allowed transitions:

- Candidate can become `EntryIntent` only when entry admissibility passes.
- EntryIntent can create `PendingEntry` or `OpenPosition` through existing
  executors.
- PendingEntry can become OpenPosition only through terminalizer-approved
  matched fill evidence.
- PendingEntry can become ResidualRepair or fail-closed work when exposure is
  one-sided or incomplete.
- OpenPosition can become CloseIntent only through V1 close/funding semantics.
- Local empty state can become LedgerProvenFlat only after exchange truth and
  owner evidence agree.

Disallowed transitions:

- Candidate directly bypasses recovery-ledger blockers.
- PendingEntry is popped after deferred terminality.
- Zero-fill becomes terminal while maker order or live position truth is open,
  unavailable, or ambiguous.
- Positive fill becomes local flat.
- Entry opens normal risk when first funding is inside the effective minimum
  horizon.
- Close semantics are relaxed to reduce the appearance of quick flats.

## Stable Reasons

Use stable reasons in journal and diagnostics:

- `entry_blocked_first_funding_too_close`
- `entry_blocked_first_funding_missing`
- `entry_blocked_recovery_ledger`
- `entry_blocked_pending_entry_protection`
- `pending_entry_viability_first_funding_too_close`
- `pending_entry_terminality_deferred_live_truth`
- `pending_entry_terminality_positive_fill_recovery`
- `ledger_flatness_unproven_exchange_truth`
- `ledger_flatness_proven`

Existing reasons such as `entry_finalization_window_expired`,
`funding_capture`, `first_stage_capture`, `second_stage_capture`, and
`settlement_force_close` remain valid. Do not rename V1 close reasons.

## Quick-Flat Reduction Strategy

The goal is not to suppress close events. The goal is to reduce avoidable quick
flats by refusing normal new risk when the shared lifecycle state says the
position would immediately enter a funding/terminal boundary.

Quick-flat categories:

1. **Bug quick flats:** missing funding timestamp, stale zero terminality,
   local-flat/live-open-order mismatch, deferred finalizer pop, positive fill
   discarded. These must be driven to zero by V1 lifecycle correctness.
2. **Avoidable timing quick flats:** candidate or pending maker work reaches
   entry execution too close to first funding. These are blocked by the shared
   entry horizon rule before order submit or before continuing normal entry
   work.
3. **Unavoidable recovery quick flats:** exposure already exists by the time V2
   observes it. These are not normal entries; they must be owned and closed or
   residualized safely, then excluded from quick-flat reduction success metrics.
4. **Observation duplicates:** journal/reporting may double-count a close event.
   Reduction metrics must deduplicate by position and close identity.

## Observability

Add lifecycle evidence without increasing event noise:

- emit first occurrence and state changes explicitly;
- compact repeated lifecycle blockers with `compact=true` and
  `suppressed_count`;
- include `remaining_to_first_funding_ms`, `effective_min_before_ms`,
  `first_funding_timestamp_ms`, and `source` for every funding horizon block;
- count prevented quick-flat risk separately from completed close events;
- deduplicate quick-flat reports by `position_id`, `reason`, and `close_id`
  where available.

Required metrics:

- `entry_first_funding_too_close_blocked_count`
- `pending_entry_too_close_normal_entry_suppressed_count`
- `quick_flat_avoidable_count`
- `quick_flat_unavoidable_recovery_count`
- `quick_flat_duplicate_event_count`

## Testing Strategy

Tests must be RED-first and follow this order:

1. Pure `FundingLifecycle` tests for timestamp normalization, missing funding,
   60-second default horizon, existing `min_scan_minutes_before_funding`, and
   funding capture stage transitions.
2. Pure `V1TradingLifecycle` tests that show entry and close share funding facts
   without entry calling close reason.
3. Runtime selection tests that block too-close candidates.
4. Runtime dispatch tests that recheck the horizon after selection delay.
5. Pending-entry viability tests that stop normal maker continuation/repost when
   the horizon is crossed and no positive exposure is proven.
6. Pending-entry finalization tests that route already-positive exposure through
   managed open/residual/recovery rather than dropping it.
7. Normal close regression tests proving V1 close reasons are unchanged.
8. Diagnose/offline quick-flat report tests that deduplicate double
   `exit.closed` projections.
9. Production incident replay tests using sanitized quick-flat and TRX/SEI
   fixtures.

## Acceptance Criteria

The implementation is accepted only when all are true:

- Entry selection, dispatch, and pending-entry continuation use one shared
  lifecycle entry admissibility decision.
- Normal close still uses V1 close semantics and does not call entry-specific
  blockers.
- Candidate inside the effective first-funding minimum horizon is blocked before
  order submission.
- Existing `min_scan_minutes_before_funding` still dominates when it is larger
  than the 60-second default.
- Positive fill or live exposure already created near funding is recovered and
  closed/residualized safely, not discarded.
- Runtime no longer contains independent copies of funding horizon checks in
  entry, pending, and close branches.
- Production quick-flat analysis can distinguish bug, avoidable timing,
  unavoidable recovery, and duplicate-observation categories.
- Focused entry/exit/recovery tests pass.
- Full local pytest, compileall, diff-check, GitNexus detect-changes, and
  production read-only diagnose pass before deployment.

## Non-Goals

- Full Rust line-by-line port.
- Tuning profit thresholds, PnL thresholds, or close slippage.
- Making close slower to hide quick flats.
- Entry calling `standard_close_reason()` as a simulator.
- Manual production order cancellation as a root fix.
- Adding symbol-specific guards for current TRX/SEI/MEU/MAGMA examples.
