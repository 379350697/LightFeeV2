# Pending Entry Live Truth Contract

Purpose: define the unified V1 contract for pending-entry recovery,
live-position truth, and residual / reduce-only cleanup. This is the gate for
future CL-048-family fixes: do not add a symbol-specific patch until the
relevant row in this matrix is covered by a failing test.

## V1 Sources

- `/Users/wl/projects/LightFee/src/execution_core/entry_sync.rs`
  - `PendingEntryTerminalizationInput`
  - `pending_entry_terminalization_budget_from_input`
  - `PendingEntryHedge::build_residual_task`
  - pending-entry finalization around balanced quantity and residual creation
- `/Users/wl/projects/LightFee/src/engine/supervision.rs`
  - `try_clear_stale_pending_entry_after_terminal_evidence`
- `/Users/wl/projects/LightFee/src/engine/state.rs`
  - `recovery_work_snapshot`
  - lifecycle/risk-mode recompute from recovery work
- `/Users/wl/projects/LightFee/README.md`
  - unattended recovery: live position discovery when local state is gone

## Unified Invariants

1. Exchange truth dominates local truth.
   Local `open_positions=[]` or `pending_entries=[]` is not healthy if a
   credentialed venue probe reports a nonzero position or live open order.

2. Zero-fill is terminal only with terminal no-fill evidence.
   A local zero fill, stale accepted order, missing fill record, or uncertain
   reconciliation is not enough. A live maker open order or live position
   progress keeps the pending entry unresolved.

3. Positive fill evidence is never discarded.
   Any maker or hedge fill quantity greater than zero must become one of:
   managed matched open state, residual / reduce-only cleanup work, or an
   explicit fail-closed state with evidence and no new-entry risk.

4. Live position progress is safety evidence before it is terminality evidence.
   If order terminality is uncertain but live position truth is nonzero, V2
   must not finalize as `unfilled_zero_balanced`. It must retain, hydrate, or
   cleanup/fail-closed.

5. Balanced quantity opens only the matched portion.
   `min(maker_fill, hedge_fill) > 0` creates a managed open position for that
   matched quantity. Any excess is residual repair or deterministic cleanup.

6. One-sided fill is not local flat.
   `maker_fill > 0, hedge_fill == 0` or the inverse creates unmatched residual
   cleanup/fail-closed work. It does not create a matched open position, but it
   also must not clear local state as healthy.

7. Residual repair is driven from live truth.
   Repair quantity, side, and terminal dust decisions use trusted live position
   and open-order truth, not stale local deltas.

8. Duplicate client id evidence is idempotency evidence, not completion.
   Bybit `110072` must be reconciled by order id/client id/execution history
   and rechecked against live position. An old filled order is completion only
   when live truth is flat or at target.

9. Risk-only can be correct, but it is not green.
   `risk_only` with unresolved exchange truth mismatch is a safe operating
   posture, not production acceptance.

10. Production health gates must agree with exchange truth.
    `verify_production_services.py` and `diagnose_live.py` must not create a
    false-green split for local-flat/exchange-nonzero or local-flat/open-order
    states.

## Decision Matrix

| ID | Evidence shape | Required decision | Must not happen |
|---|---|---|---|
| PE-01 | maker=0, hedge=0, maker order terminal no-fill, no live order, no live position | finalize `passive_unfilled`, remove pending | retain forever |
| PE-02 | maker=0, hedge=0, maker order open or open-order truth unavailable | retain pending with `uncertain_outcome` and backoff | emit `entry.passive_unfilled` |
| PE-03 | maker=0, hedge=0, order terminality uncertain, live maker position nonzero | retain/hydrate/cleanup/fail-closed from live truth | finalize `unfilled_zero_balanced` |
| PE-04 | stale zero reconciliation after previous positive fill | keep positive fill evidence | overwrite fill quantities to zero |
| PE-05 | rejected pending, maker or hedge positive fill | route through V1 finalization / residual cleanup | loop `rejected_pending_retained_with_fill` forever |
| PE-06 | startup block says pending work without open positions, but pending has live exposure | retry recovery against live truth | make recovery unreachable |
| PE-07 | balanced positive fills with prices | create managed open position for matched quantity | leave pending or create zero/ghost position |
| PE-08 | balanced positive fills missing local price/order but trusted live position has price | hydrate missing details then create managed open position | require manual recovery only |
| PE-09 | imbalanced positive fills on both legs | open matched quantity, queue residual for excess | treat excess as missing hedge demand |
| PE-10 | one-sided positive fill | queue unmatched residual cleanup or fail-closed | local flat / healthy |
| PE-11 | deterministic hedge admission reject after maker exposure | abort through cleanup path; retain fail-closed if cleanup unproven | retry same hedge indefinitely |
| RC-01 | residual task, live excess tradeable, open orders empty | submit one reduce-only IOC and complete/backoff from fill truth | use stale local repair quantity blindly |
| RC-02 | residual task, live excess zero, open orders empty | complete as already flat and release pair gate | keep pair gate forever |
| RC-03 | residual task, live excess zero, open orders present | pause/backoff with open-order evidence | clear task as already flat |
| RC-04 | no local open position, repair venue live position nonzero | rebuild repair side from signed live truth | drop residual because local position is missing |
| RC-05 | official min quantity/notional dust | terminalize with exchange-rule evidence | retry forever |
| RC-06 | Bybit duplicate client id, reconciled fill, live flat/target | complete idempotently | resubmit unnecessary cleanup |
| RC-07 | Bybit duplicate client id, old fill, live still nonzero | classify stale/partial, retry with fresh bounded CID or fail closed | clear state from old fill |
| RC-08 | duplicate cleanup keeps failing and live remains nonzero | stop new-entry risk and expose deterministic blocker | unbounded retry loop with repeated CIDs |
| DG-01 | local flat, exchange position nonzero | unhealthy / high risk / gate failed | service-health green |
| DG-02 | local flat, exchange open order present | unhealthy / high risk / gate failed | service-health green |
| DG-03 | exchange truth unavailable | missing evidence, not green | assume flat |

## Current Test Coverage

| Matrix IDs | Current coverage | Status |
|---|---|---|
| PE-01 | `tests/test_live_entry_hedge_root_fix.py::TestZeroFillFinalizeV1ParityGate` | covered |
| PE-02 | `tests/test_pending_entry_v1_semantic_drift.py::test_finalize_zero_fill_retains_pending_when_maker_open_order_truth_exists` | covered for open-order truth |
| PE-03 | `tests/test_pending_entry_v1_semantic_drift.py::test_uncertain_maker_order_live_position_does_not_apply_maker_progress`; `test_zero_fill_finalize_retains_when_live_position_truth_is_nonzero` | covered locally; zero-fill finalize now defers when maker live position is nonzero |
| PE-04 | `tests/test_live_entry_hedge_root_fix.py::test_stale_zero_reconciliation_does_not_erase_known_hedge_fill` | covered |
| PE-05 | `tests/test_pending_entry_v1_semantic_drift.py::test_startup_rejected_positive_fill_finalizes_open_and_residual`, `test_reconcile_rejected_positive_fill_does_not_retained_loop` | covered for SEIUSDT shape |
| PE-06 | `tests/test_pending_entry_v1_semantic_drift.py::test_startup_blocked_pending_entry_retries_live_truth_recovery` | covered |
| PE-07, PE-08 | `tests/test_pending_entry_v1_semantic_drift.py::test_live_position_hydrates_balanced_pending_entry_and_finalizes_like_v1` | covered |
| PE-09 | `tests/test_pending_entry_v1_semantic_drift.py::test_startup_recovery_imbalanced_live_truth_finalizes_balanced_position` | covered |
| PE-10 | `tests/test_live_entry_hedge_root_fix.py::TestUnmatchedResidualV1Parity` | covered locally |
| PE-11 | `tests/live_harness/test_exchange_admission_incidents.py`, `tests/test_live_startup_preflight.py` | covered for known deterministic families |
| RC-01, RC-02, RC-05 | `tests/test_live_entry_hedge_root_fix.py::TestResidualRepairExecutionV1Parity` | covered |
| RC-03 | `tests/live_harness/test_residual_repair_incident_replay.py::test_hmstr_open_orders_present_pause_records_truth_evidence` | covered |
| RC-04 | `tests/live_harness/test_residual_repair_incident_replay.py::test_exhausted_residual_repair_live_nonzero_repairs_but_untrusted_stays_fail_closed` | covered |
| RC-06 | `tests/test_live_entry_hedge_root_fix.py::test_bybit_duplicate_residual_repair_reconciles_full_live_flat` | covered |
| RC-07 | `tests/live_harness/test_recovered_close_and_duplicate_incidents.py::test_biousdt_bybit_duplicate_old_fill_live_nonzero_retries_fresh_cid`, cleanup duplicate tests in `tests/test_live_entry_hedge_root_fix.py` | covered for stale-fill/live-nonzero retry |
| RC-08 | `tests/test_live_entry_hedge_root_fix.py::TestResidualRepairExecutionV1Parity::test_bybit_duplicate_residual_repair_live_nonzero_blocks_after_bounded_retries` | covered locally; repeated duplicate/live-nonzero residual repair now fail-closes with blocker evidence and non-reused CIDs |
| DG-01, DG-02 | `tests/ops/test_production_health.py`, `tests/test_diagnose_live.py`, `tests/probes/test_false_green_exchange_truth_gate.py` | covered locally; production service verifier now requires exchange-truth evidence and diagnose already rejects live positions/open orders |
| DG-03 | `tests/test_diagnose_live.py::test_run_diagnose_gate_fails_when_exchange_truth_unavailable` | covered for diagnose |

## Required RED Tests Before Runtime Fixes

1. `PE-03` / BIOUSDT pending terminality:
   accepted maker order, zero order fill reconciliation, no live open order,
   but trusted live maker position is nonzero. Expected: no
   `unfilled_zero_balanced`; pending is retained or routed to recovery
   cleanup/hydration with explicit live-position evidence. RED added in
   `tests/test_pending_entry_v1_semantic_drift.py`; local GREEN now passes.

2. `RC-08` / repeated Bybit duplicate cleanup:
   live position remains nonzero, duplicate reconciliation alternates
   stale-full and partial evidence, and retry CIDs repeat or exhaust. Expected:
   bounded retry/fail-closed blocker with no new-entry risk; no state clear
   from stale fill evidence. RED/GREEN added in
   `tests/test_live_entry_hedge_root_fix.py`.

3. `DG-01` service gate alignment:
   local runtime state is running and empty, but exchange truth has one Bybit
   live position. Expected: the production service verifier reports critical
   or requires diagnose gate evidence; it cannot be the only green gate.
   RED/GREEN added in `tests/ops/test_production_health.py`.

4. Incident fixture replay:
   add a sanitized BIOUSDT journal fixture covering accepted maker orders,
   `pending_entry.live_position_progress_deferred`, zero finalization,
   duplicate cleanup retries, and final exchange truth mismatch.

## Bug Mapping

| Bug / family | Contract rows | Coverage judgement | Next action |
|---|---:|---|---|
| CL-048 SEIUSDT retained rejected positive fill | PE-05, PE-06, PE-07, PE-09, RC-01 | covered for that shape | keep as regression |
| CL-049 SEIUSDT open maker order terminality | PE-02, PE-11, DG-02 | covered for open-order shape | keep as regression |
| Current BIOUSDT local false-flat with live position | PE-03, RC-07, RC-08, DG-01 | covered locally by terminality, duplicate-cleanup blocker, and service-gate tests; cloud deploy/diagnose still pending | deploy only through manifest gate, then credentialed all-venue flat/no-open-orders verification |
| Bybit duplicate `110072` stale-fill/live-nonzero family | RC-06, RC-07, RC-08 | covered locally | keep repeated-loop/fail-closed regression |
| Residual repair live truth card | RC-01 through RC-05 | mostly covered | add any BIOUSDT residual variant if evidence proves residual path |
| Hyperliquid insufficient margin admission | PE-11 | covered for symbol-level containment | evaluate account/venue cooldown only if recurrence evidence supports it |
| Diagnose false green | DG-01 through DG-03 | diagnose and production service verifier covered locally | keep production acceptance tied to exchange truth |

## Implementation Direction After RED Tests

1. Introduce a single pending-entry terminality decision boundary in
   `lightfee/engine/runtime.py` instead of scattering zero-fill, live-position,
   rejected-pending, and startup recovery decisions across unrelated branches.
   PE-03 is now guarded locally by live maker position truth before zero-fill
   removal; the broader unification remains follow-up work.

2. Feed live-position progress into that decision boundary. A live nonzero
   position with uncertain order terminality must block zero-fill finalization
   and either hydrate, residualize, cleanup, or fail closed.

3. Make cleanup duplicate reconciliation persist attempt evidence and stop at a
   deterministic blocker instead of cycling through a small deterministic CID
   set while live truth remains nonzero. RC-08 is now guarded locally for
   residual repair duplicate/live-nonzero retries.

4. Align production acceptance so the operator cannot see services as green
   when credentialed exchange truth is red. DG-01 is now guarded locally by
   requiring exchange-truth evidence in `verify_production_services.py`.

Before editing any production function, run GitNexus freshness and impact for
the target symbols named in the RED tests.
