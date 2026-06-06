# Pending Entry Live Truth Contract

Purpose: define the unified V1 contract for pending-entry recovery,
live-position truth, residual / reduce-only cleanup, and pending-close
reconciliation lifecycle. This is the gate for CL-048, CL-049, CL-050, and
future CL-048-family recurrences: do not add a symbol-specific patch until the
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
- `/Users/wl/projects/LightFee/src/engine/recovery.rs`
  - `process_pending_close_reconciliations`
  - final close reconciliation abandon after terminal flat live truth
- `/Users/wl/projects/LightFee/src/execution_core/engine.rs`
  - private position confirmation for background reconciliation lifecycle
- `/Users/wl/projects/LightFee/README.md`
  - unattended recovery: live position discovery when local state is gone

## Contract Governance

This file is the only active contract for the CL-048 / CL-049 / CL-050
pending-entry live-truth family. The old cluster ids remain useful chronology,
but they are not separate root-fix tracks.

V1 is the coverage floor for this contract. V2 may keep a different
implementation shape, but the V2 behavior must not cover fewer terminality,
live-truth, residual-cleanup, fallback, supervision, or lifecycle safety cases
than V1 covers for pending-entry recovery.

Every new production recurrence must be mapped to one or more `PE-*`, `RC-*`,
or `DG-*` rows before code changes. If the mapped rows are already covered by
regression tests and current credentialed cloud truth is high-confidence
flat/no-open-orders, do not change trading code; update evidence or bug docs
only. If the recurrence does not map to this matrix, or maps to an uncovered
row, add a RED test for the missing contract row before changing runtime code.

Code audit rule: callers of `_finalize_pending_entry()` must treat deferred
finalization as retained pending work. A direct `pending_entries.pop()` after a
finalize call is a potential contract bypass unless the caller has proved that
the finalize path actually emitted terminal open/residual/unfilled evidence.
Runtime pending-entry removal must route through the pending-entry terminality
authority or the shared post-terminal removal helper; a direct pop outside that
boundary is a contract bypass.

Recovery block and clear governance: the V1 recovery decision core is the
single active authority for core-owned recovery block, clear, lifecycle, entry,
diagnose, and production-health semantics. `ambiguous_exchange_truth` is an
evidence quality label, not standalone global recovery work. It becomes blocking
only when local recovery work, unresolved pending/passive/residual work, owner
evidence, a concrete live artifact, or operator fail-closed policy requires
truth. A flat/no-local-work probe gap may warn or keep production acceptance
incomplete, but it must not create `exchange_truth_recovery_ledger_blocked` or
block normal entry by itself.

## V1 Coverage Floor

The minimum semantic surface from V1 is:

- terminalization budget: inflight hedge blocks force terminalization; hard
  ceiling and force-terminal windows have different outcomes for zero fill,
  balanced fill, and missing hedge.
- terminal maker state: zero-fill may repost, trigger configured taker fallback,
  finalize, or enter a cooldown; it is not a blind pending removal.
- live balanced hydration: startup/recovery probes can hydrate balanced live
  long/short exposure into pending fills, with prices/order placeholders, before
  terminalization.
- residual task ordering: residual repair is computed before final branching;
  partial matched fills open the matched quantity plus residual, and one-sided
  fills persist unmatched residual cleanup.
- abort/fail-closed cleanup: pending entries with unresolved exposure are
  cleaned with reduce-only/hard-stop evidence or retained fail-closed.
- supervision clear: stale pending backlog clears only after terminal no-fill
  evidence; any fill, inflight hedge, cancel, or non-resting progress retains.
- recovery/lifecycle: recovery work snapshots drive risk mode. Local flat is
  not enough, and ambiguous or untrusted live exposure must fail closed instead
  of being guessed into healthy state.
- pending-entry protection: V1 blocks a new entry on the same pair, and also
  blocks the same symbol when either venue overlaps with an unresolved pending
  entry. A live-truth-deferred pending entry must not be bypassed by swapping
  only one venue.
- pending-close reconciliation: after a close has already flattened the live
  legs and removed managed open state, final fill/PnL accounting
  reconciliation is background work unless live truth shows remaining exposure.
  Unavailable final close-fill reconciliation may be abandoned only after both
  close venues prove terminal flat; nonzero terminal live size remains
  fail-safe blocking.

## Unified Invariants

1. Exchange truth dominates local truth.
   Local `open_positions=[]` or `pending_entries=[]` is not healthy if a
   credentialed venue probe reports a nonzero position or live open order.

2. Local flat must be core-proven flat or explicitly evidence-gapped.
   A state is flat only when the recovery ledger and V1 recovery decision core
   have classified live positions, live open orders, pending entries, residual
   repairs, passive closes, owner evidence, and evidence quality. Local empty
   collections are inputs, not the conclusion. A pure probe gap with no local
   recovery work and no live artifact is not recovery work by itself.

3. Zero-fill is terminal only with terminal no-fill evidence.
   A local zero fill, stale accepted order, missing fill record, or uncertain
   reconciliation is not enough. A live maker open order or live position
   progress keeps the pending entry unresolved.

4. Positive fill evidence is never discarded.
   Any maker or hedge fill quantity greater than zero must become one of:
   managed matched open state, residual / reduce-only cleanup work, or an
   explicit fail-closed state with evidence and no new-entry risk.

5. Live position progress is safety evidence before it is terminality evidence.
   If order terminality is uncertain but live position truth is nonzero, V2
   must not finalize as `unfilled_zero_balanced`. It must retain, hydrate, or
   cleanup/fail-closed.

6. Balanced quantity opens only the matched portion.
   `min(maker_fill, hedge_fill) > 0` creates a managed open position for that
   matched quantity. Any excess is residual repair or deterministic cleanup.

7. One-sided fill is not local flat.
   `maker_fill > 0, hedge_fill == 0` or the inverse creates unmatched residual
   cleanup/fail-closed work. It does not create a matched open position, but it
   also must not clear local state as healthy.

8. Residual repair is driven from live truth.
   Repair quantity, side, and terminal dust decisions use trusted live position
   and open-order truth, not stale local deltas.

9. Duplicate client id evidence is idempotency evidence, not completion.
   Bybit `110072` must be reconciled by order id/client id/execution history
   and rechecked against live position. An old filled order is completion only
   when live truth is flat or at target.

10. Risk-only can be correct, but it is not green.
   `risk_only` with unresolved exchange truth mismatch is a safe operating
   posture, not production acceptance.

11. Production health gates must agree with exchange truth.
    `verify_production_services.py` and `diagnose_live.py` must not create a
    false-green split for local-flat/exchange-nonzero or local-flat/open-order
    states.

## Lifecycle Core Rows

| ID | Evidence shape | Required decision | Must not happen |
|---|---|---|---|
| LC-01 | candidate or pending normal-entry work is inside the effective first-funding minimum horizon with no positive exposure | block normal entry risk with stable lifecycle evidence | submit new maker/hedge risk that close lane will immediately capture |
| LC-02 | positive fill or live exposure already exists inside the funding horizon | own, recover, residualize, close, or fail-closed with evidence | discard exposure or call local flat |
| LC-03 | quick-flat report sees duplicate `exit.closed` projections for one close identity | count one real quick flat and one duplicate observation | inflate quick-flat frequency |

## Decision Matrix

| ID | Evidence shape | Required decision | Must not happen |
|---|---|---|---|
| LC-01 | candidate or pending normal-entry work is inside the effective first-funding minimum horizon with no positive exposure | block normal entry risk with `entry_blocked_first_funding_too_close` or `pending_entry_viability_first_funding_too_close` | submit new maker/hedge risk that close lane will immediately capture |
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
| PE-11 | deterministic hedge admission reject after maker exposure, including account/venue-scoped margin rejects | abort through cleanup path; retain fail-closed if cleanup unproven; arm symbol/venue admission cooldown when the reject proves account/venue capacity is exhausted | retry same hedge indefinitely or keep submitting new entries through the same exhausted venue |
| PE-12 | terminalization budget reached while hedge is inflight | retain/reconcile until hedge terminality or deadline fail-closed | force-finalize or pop pending |
| PE-13 | zero-fill terminal maker state with configured taker fallback/repost/cooldown | try V1 fallback/repost path or record zero-fill cooldown before accepting terminal no-entry | clear and immediately churn the same pair |
| PE-14 | stale pending backlog, zero fills, no inflight/cancel, resting order fetch returns none | supervision may clear as terminal no-fill | clear when any fill/inflight/cancel/progress exists |
| PE-15 | live balanced exposure exists while local fill/order details are incomplete | hydrate from live truth and then finalize/open/residualize | require manual recovery or discard live truth |
| PE-16 | live exposure is ambiguous, untrusted, or cannot be mapped to the pending contract | retain/fail-closed with explicit evidence and no new-entry risk | guess healthy/open/flat |
| PE-17 | unresolved pending entry exists for the same symbol and the new candidate shares either venue | block the new entry with `pending_entry_protection` until the pending entry terminalizes or cleanup proves flat | open a second overlapping pending entry by changing only one venue |
| PE-18 | `_finalize_pending_entry()` defers because fill details, open-order truth, or live-position truth are incomplete | caller retains pending work, applies backoff, and does not add the entry to resolved/pop paths | treat a deferred finalizer call as terminal completion |
| RC-01 | residual task, live excess tradeable, open orders empty | submit one reduce-only IOC and complete/backoff from fill truth | use stale local repair quantity blindly |
| RC-02 | residual task, live excess zero, open orders empty | complete as already flat and release pair gate | keep pair gate forever |
| RC-03 | residual task, live excess zero, open orders present | pause/backoff with open-order evidence | clear task as already flat |
| RC-04 | no local open position, repair venue live position nonzero | rebuild repair side from signed live truth | drop residual because local position is missing |
| RC-05 | official min quantity/notional dust | terminalize with exchange-rule evidence | retry forever |
| RC-06 | Bybit duplicate client id, reconciled fill, live flat/target | complete idempotently | resubmit unnecessary cleanup |
| RC-07 | Bybit duplicate client id, old fill, live still nonzero | classify stale/partial, retry with fresh bounded CID or fail closed | clear state from old fill |
| RC-08 | duplicate cleanup keeps failing and live remains nonzero | stop new-entry risk and expose deterministic blocker | unbounded retry loop with repeated CIDs |
| RC-09 | local open/pending/residual/passive work exists and exchange truth is unavailable or partial | risk-only / no new entry until required truth or deterministic cleanup path is available | treat missing truth as clean or open new entry risk |
| RC-10 | previous core-owned `exchange_truth_recovery_ledger_blocked` exists, but the core classifies the current snapshot as `RUNNING_CLEAN` or `RUNNING_WITH_EVIDENCE_GAP` | clear through the core with explicit clear reason | clear through an independent stale-clean helper that can contradict the core |
| DG-01 | local flat, exchange position nonzero | unhealthy / high risk / gate failed | service-health green |
| DG-02 | local flat, exchange open order present | unhealthy / high risk / gate failed | service-health green |
| DG-03 | exchange truth unavailable for production high-confidence acceptance | missing evidence / not high-confidence green | assume production flat |
| DG-04 | local flat, no local recovery work, no live artifact, and only a timeout/unsupported/partial probe gap | runtime `RUNNING_WITH_EVIDENCE_GAP`; normal entry remains governed by normal candidate gates; production acceptance may still be incomplete | create `exchange_truth_recovery_ledger_blocked` or block all normal entry |
| PC-01 | live passive close flattened both legs and final close-leg fill reconciliation is available from stored order/client ids | rebuild final close accounting from the stored leg snapshot and remove the pending reconciliation | depend on entry reconciliation or require a still-managed open position |
| PC-02 | final pending-close reconciliation, no managed open position, fill reconciliation unavailable, both close venues prove terminal live size zero | abandon stale final reconciliation and release lifecycle/gate with explicit terminal-flat evidence | retain risk-only forever solely because fill/PnL accounting is unavailable |
| PC-03 | final pending-close reconciliation, fill reconciliation unavailable, either close venue reports nonzero terminal live size | retain reconciliation as fail-safe risk-only/backoff | abandon or mark healthy while live exposure remains |
| PC-04 | managed local open positions remain and venue private-position truth is confirmed while close accounting reconciliation is pending | allow reconciliation to continue in background without forcing normal lifecycle to risk-only, unless another explicit risk policy is active | make normal trading harder solely due to background accounting work |
| PC-05 | pending-close reconciliation exists only as a stored position snapshot after the managed position was removed | supervisor/risk venue coverage includes the snapshot venues | drop venues from supervision because `open_positions` is empty |

## Current Test Coverage

| Matrix IDs | Current coverage | Status |
|---|---|---|
| PE-02, PE-05, PE-10, RC-01 through RC-04, DG-01 through DG-03 | `tests/engine/test_recovery_ledger.py`; `tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py` | covered for pure recovery-ledger classification of local-flat/live-open-order, local-flat/live-position, positive-fill false-flat, residual repair, ambiguous truth, and proven-flat states |
| RC-09, RC-10, DG-04 | `tests/engine/test_recovery_decision_core.py`; `tests/engine/test_recovery_ledger.py`; `tests/test_live_startup_preflight.py`; `tests/test_runtime_entry_flow.py`; `tests/test_diagnose_live.py`; `tests/ops/test_production_health.py` | covered for flat/no-local-work evidence gap, local work plus unavailable truth, core-owned clear, runtime block/clear authority, diagnose/health core classification, and review closure for same-symbol-but-unowned live artifacts |
| PC-01 through PC-05 | `tests/test_pending_entry_v1_semantic_drift.py`; `tests/test_passive_close.py`; `tests/test_supervisor_execution.py` | covered for live-only registration, V1 close-leg snapshots, fill-based terminal accounting, unavailable-fill terminal-flat abandon, nonzero terminal-live retention, background lifecycle with private confirmation, risk-mode preservation, and supervisor venue coverage |
| PE-01 through PE-03, PE-07 through PE-10, PE-16, PE-18 | `tests/engine/test_pending_entry_terminalizer.py` | covered for pure terminal decision outcomes; runtime finalizer remains the integration authority |
| PE-01 | `tests/test_live_entry_hedge_root_fix.py::TestZeroFillFinalizeV1ParityGate` | covered |
| PE-02 | `tests/test_pending_entry_v1_semantic_drift.py::test_finalize_zero_fill_retains_pending_when_maker_open_order_truth_exists` | covered for open-order truth |
| PE-03 | `tests/test_pending_entry_v1_semantic_drift.py::test_uncertain_maker_order_live_position_does_not_apply_maker_progress`; `test_zero_fill_finalize_retains_when_live_position_truth_is_nonzero` | covered and cloud-verified in CL-050; zero-fill finalize now defers when maker live position is nonzero |
| PE-04 | `tests/test_live_entry_hedge_root_fix.py::test_stale_zero_reconciliation_does_not_erase_known_hedge_fill` | covered |
| PE-05 | `tests/test_pending_entry_v1_semantic_drift.py::test_startup_rejected_positive_fill_finalizes_open_and_residual`, `test_reconcile_rejected_positive_fill_does_not_retained_loop` | covered for SEIUSDT shape |
| PE-06 | `tests/test_pending_entry_v1_semantic_drift.py::test_startup_blocked_pending_entry_retries_live_truth_recovery` | covered |
| PE-07, PE-08 | `tests/test_pending_entry_v1_semantic_drift.py::test_live_position_hydrates_balanced_pending_entry_and_finalizes_like_v1` | covered |
| PE-09 | `tests/test_pending_entry_v1_semantic_drift.py::test_startup_recovery_imbalanced_live_truth_finalizes_balanced_position` | covered |
| PE-10 | `tests/test_live_entry_hedge_root_fix.py::TestUnmatchedResidualV1Parity` | covered locally |
| PE-11 | `tests/live_harness/test_exchange_admission_incidents.py::test_pending_hedge_hyperliquid_insufficient_margin_reject_aborts_without_retry`, `tests/test_live_startup_preflight.py` | covered for known deterministic families; Hyperliquid insufficient margin now arms both symbol and venue cooldowns |
| PE-12 | `tests/test_v1_parity_pending_entry_recovery_red.py`; `_pending_entry_terminalization_budget()` runtime path | covered for budget table and inflight/force-terminal decision shape |
| PE-13 | `tests/engine/test_v1_real_config_gap_semantics.py`; `tests/test_v1_config_defaults_parity_red.py`; `tests/test_runtime_entry_flow.py::TestPlannerDispatchIntegration::test_force_terminal_zero_fill_uses_finalizer_not_blind_pop` | covered for zero-fill terminal completion through the V1 finalizer; add incident REDs before changing fallback/repost routing if a configured fallback recurrence appears |
| PE-14 | V1 source audited; V2 has stale-entry abandon/terminal evidence checks but no dedicated supervision-backlog matrix test found | uncovered matrix row; add RED before any supervision clear change |
| PE-15 | `tests/test_pending_entry_v1_semantic_drift.py::test_live_position_hydrates_balanced_pending_entry_and_finalizes_like_v1` | covered with PE-07/PE-08 |
| PE-16 | `tests/live_harness/test_residual_repair_incident_replay.py::test_exhausted_residual_repair_live_nonzero_repairs_but_untrusted_stays_fail_closed`; pending-entry ambiguous-live coverage still needs targeted RED if production evidence appears | partially covered |
| PE-17 | `tests/test_runtime_entry_flow.py::TestPendingEntryTracking::test_pending_entry_dedup_blocks_same_symbol_venue_overlap_like_v1`; `tests/test_v1_record_layer_parity.py -k has_pending_entry_for_symbol` | covered for V1 same-symbol venue-overlap pending protection |
| PE-18 | `tests/test_runtime_entry_flow.py::TestPlannerDispatchIntegration::test_reconcile_retains_pending_when_finalize_defers_missing_fill_details`; runtime caller audit grep for `_finalize_pending_entry()` | covered for normal reconciliation incomplete-fill defer and caller-side pop/resolve guards |
| RC-01, RC-02, RC-05 | `tests/test_live_entry_hedge_root_fix.py::TestResidualRepairExecutionV1Parity` | covered |
| RC-03 | `tests/live_harness/test_residual_repair_incident_replay.py::test_hmstr_open_orders_present_pause_records_truth_evidence` | covered |
| RC-04 | `tests/live_harness/test_residual_repair_incident_replay.py::test_exhausted_residual_repair_live_nonzero_repairs_but_untrusted_stays_fail_closed` | covered |
| RC-06 | `tests/test_live_entry_hedge_root_fix.py::test_bybit_duplicate_residual_repair_reconciles_full_live_flat` | covered |
| RC-07 | `tests/live_harness/test_recovered_close_and_duplicate_incidents.py::test_biousdt_bybit_duplicate_old_fill_live_nonzero_retries_fresh_cid`, cleanup duplicate tests in `tests/test_live_entry_hedge_root_fix.py` | covered for stale-fill/live-nonzero retry |
| RC-08 | `tests/test_live_entry_hedge_root_fix.py::TestResidualRepairExecutionV1Parity::test_bybit_duplicate_residual_repair_live_nonzero_blocks_after_bounded_retries` | covered and cloud-verified in CL-050; repeated duplicate/live-nonzero residual repair now fail-closes with blocker evidence and non-reused CIDs |
| DG-01, DG-02 | `tests/ops/test_production_health.py`, `tests/test_diagnose_live.py`, `tests/probes/test_false_green_exchange_truth_gate.py` | covered and cloud-verified; production service verifier requires exchange-truth evidence and diagnose rejects live positions/open orders |
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

5. `PE-18` / caller-side deferred finalize:
   create one runtime-path RED per finalize-and-pop caller where
   `_finalize_pending_entry()` can defer because fill details or live truth are
   incomplete. Expected: pending remains and no caller-level pop occurs.
   RED/GREEN added for the normal reconciliation path in
   `tests/test_runtime_entry_flow.py`; runtime call sites now consume the
   finalizer boolean instead of assuming completion.

6. `PE-13` / terminal fallback and cooldown:
   force-terminal zero-fill must route through the finalizer rather than direct
   pending removal. RED/GREEN added in `tests/test_runtime_entry_flow.py`. If a
   future production recurrence reaches configured fallback/repost evidence, add
   RED tests covering frozen-candidate fallback materialization, fallback
   deferred retention, and cooldown before changing runtime code.

7. `PE-14` / supervision terminal backlog clear:
   add RED tests before any supervision clear change proving that stale zero-fill
   resting backlog clears only when the maker order fetch returns no progress,
   and retains when any fill/inflight/cancel/non-resting evidence exists.

8. `PE-16` / ambiguous live exposure:
   if production evidence shows live truth that cannot be mapped to balanced,
   residual, or deterministic cleanup semantics, add RED tests requiring
   fail-closed retention rather than guessed open/flat state.

9. `PE-17` / same-symbol venue-overlap pending protection:
   RED/GREEN added in `tests/test_runtime_entry_flow.py` and record-layer parity
   coverage. Future overlapping-pending recurrences should update evidence/docs
   if this row still passes.

10. Exchange-truth recovery ledger fixtures:
    RED/GREEN added in
    `tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py`.
    The `TRXUSDT` fixture proves local-flat plus a Bybit non-reduce open maker
    order becomes blocking `orphan_maker_order`; the `SEIUSDT` fixture proves
    positive maker-fill evidence prevents local false-flat/proven-flat.

11. V1 recovery decision core closed-loop:
    RED/GREEN coverage now proves flat/no-local-work plus unavailable or
    partial truth returns `RUNNING_WITH_EVIDENCE_GAP` with normal entry allowed,
    while local recovery work plus unavailable truth returns risk-only/blocking.
    Prior `exchange_truth_recovery_ledger_blocked` clears through the core, not
    an independent stale-clean helper. Review closure adds RED/GREEN coverage
    for same-symbol but unmatched live positions, same-symbol unrelated maker
    orders, and diagnose count-only pending work.

12. `PC-01` through `PC-05` / pending-close reconciliation lifecycle:
    RED/GREEN coverage must prove close accounting work is registered only in
    live runtime, uses stored close-leg identities, abandons stale final work
    only after terminal flat live truth, retains nonzero terminal live exposure,
    preserves explicit reduce-only/fail-closed policy, and keeps snapshot venues
    supervised after managed open state is removed.

## Bug Mapping

| Bug / family | Contract rows | Coverage judgement | Next action |
|---|---:|---|---|
| CL-048 SEIUSDT retained rejected positive fill | PE-05, PE-06, PE-07, PE-09, RC-01 | covered for that shape; historical cluster now maps into this contract | keep as regression |
| CL-049 SEIUSDT open maker order terminality | PE-02, PE-11, DG-02 | covered for open-order shape; historical cluster now maps into this contract | keep as regression |
| CL-050 BIOUSDT local false-flat with live position | PE-03, RC-07, RC-08, DG-01 | covered and cloud-verified at `68a979b`; production verifier and service-env diagnose were high-confidence flat/no-open-orders across all seven venues | keep as regression; if it recurs, map evidence first and only add code after an uncovered RED row |
| 2026-06-05 V1 recovery ledger architecture | PE-02, PE-05, PE-10, PE-16, RC-01 through RC-04, DG-01 through DG-03 | locally covered by pure ledger, shared exchange-truth normalizer, owner index, terminalizer, startup ledger blocker, entry ledger gate, and production-health open-order mismatch tests | keep as the common runtime boundary for future CL-048-family recurrences; do not add CL-specific branches |
| 2026-06-06 V1 recovery decision core closed-loop | RC-09, RC-10, DG-04, plus DG-01/DG-02 live-artifact preservation | final root-closure deployed and cloud diagnosed healthy; default 15s tick verifier threshold remains a separate ops follow-up | keep one pure core as the shared authority for block/clear/lifecycle/entry/diagnose/health; no-work probe gaps are evidence warnings, local work plus missing truth remains risk-only, and live artifacts remain blockers unless exact owner evidence exists. The post-deploy `risk_only` recurrence showed that successful live mismatch flatten must immediately recollect truth and route to the same core for `recovery.ledger_clear`; RED/GREEN coverage now protects startup and runtime clean-live-position recovery paths. Final local closure removes stale recovery-block cleanup as an authority and routes legacy migration clear, including passive-close live-flat cleanup, through `V1RecoveryDecisionCore` decisions. Final review also narrowed evidence-gap clearability so `RUNNING_WITH_EVIDENCE_GAP` cannot clear `orphan_maker_order`/`unpaired_live_position`, and clarified that managed local open positions require explicit truth-required/recovery-required evidence before becoming recovery work. Cloud diagnose after redeploy showed high-confidence flat/no-open-orders, `RUNNING_CLEAN`, `gate_passed=true`, and `recovery.ledger_clear=1` after the startup recovery block |
| 2026-06-06 pending-close reconciliation lifecycle gap | PC-01 through PC-05 | locally covered; pending cloud redeploy | keep close accounting reconciliation as V1 background work after live-flat lifecycle closure; abandon stale final work only with terminal flat live truth, retain nonzero terminal live exposure, and keep supervision/risk venues from the stored snapshot |
| Bybit duplicate `110072` stale-fill/live-nonzero family | RC-06, RC-07, RC-08 | covered locally | keep repeated-loop/fail-closed regression |
| Residual repair live truth card | RC-01 through RC-05 | mostly covered | add any BIOUSDT residual variant if evidence proves residual path |
| Hyperliquid insufficient margin admission | PE-11 | covered for symbol and venue-level containment | keep admission regression; if it recurs with same evidence, update docs/evidence only |
| Post-contract WLD/XLM/MON/MOVE Hyperliquid margin rejects and XLM overlapping pending churn | PE-11, PE-17, PE-18 | covered locally by full-loop parity follow-up | deploy when requested; future recurrences map to these rows before code |
| Diagnose false green | DG-01 through DG-03 | diagnose and production service verifier covered locally | keep production acceptance tied to exchange truth |

## Implementation Direction After RED Tests

1. Keep `_finalize_pending_entry()` as the effective pending-entry terminality
   decision boundary in `lightfee/engine/runtime.py` and do not add
   symbol-specific branches outside the matrix. PE-03 is guarded by live maker
   position truth before zero-fill removal.

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

5. Treat the boolean result of `_finalize_pending_entry()` as part of the
   contract. `False` means retained pending work; callers must back off or keep
   recovery reachable, not resolve/pop.

6. Apply V1 pending-entry protection before dispatch. Same symbol plus either
   venue overlap is enough to block a new candidate while the old pending entry
   is unresolved.

7. Route recovery block, clear, lifecycle, entry, diagnose, and production
   health through the V1 recovery decision core. RecoveryLedger may build work
   items, but it must not independently decide that a probe gap is global
   recovery work. Legacy stale-block cleanup may remain only as migration
   fallback when the core already chose clean/evidence-gap.

8. Keep pending-close reconciliation separate from pending-entry/order-entry
   reconciliation. It is close accounting work backed by stored close-leg
   identities and terminal live-position truth; it must not force risk-only
   when V1 would run it in the background, and it must not clear when terminal
   live size remains nonzero.

Before editing any production function, run GitNexus freshness and impact for
the target symbols named in the RED tests.

## Current Code Audit Notes

Source audit on 2026-06-04 found the expected core contract checks inside
`_finalize_pending_entry()`: incomplete fill details defer, zero-fill checks
maker open-order truth, zero-fill checks maker live-position truth, balanced
fills create managed open positions, and one-sided fills queue residual cleanup.

Follow-up audit on 2026-06-05 closed the caller-side bypass risk. Runtime
callers now consume the boolean result of `_finalize_pending_entry()`: true paths
may resolve/pop, false paths retain pending work and back off. The RED/GREEN
normal-reconciliation test covers incomplete fill-detail deferral, and a code
grep confirms no bare `await self._finalize_pending_entry(...)` call remains.

The same follow-up added V1 same-symbol venue-overlap pending protection and
Hyperliquid insufficient-margin venue cooldown coverage. Remaining open matrix
rows are evidence-driven, not active production bugs: `PE-14` needs supervision
backlog RED tests before any stale-backlog clear change, and pending-entry
`PE-16` needs targeted RED only if future live truth is ambiguous or untrusted.

Implementation follow-up on 2026-06-05 added the first unified V1-style
recovery ledger boundary:

- `lightfee/engine/recovery_ledger.py` classifies live exchange artifacts,
  local pending/open/residual/passive-close work, ambiguous truth, and
  proven-flat states into recovery work items.
- `lightfee/engine/exchange_truth.py` normalizes the exchange-truth payload
  used by runtime-adjacent health gates, production verifier, and
  `diagnose_live.py`.
- `lightfee/engine/recovery_owner_index.py` maps live orders/positions to
  proven or probable owners and leaves insufficient evidence as orphan work.
- `lightfee/engine/pending_entry_terminalizer.py` records the pure terminality
  decision surface so pending-entry removal stays behind one authority.
- `LiveRuntime` now has a recovery-ledger refresh helper and an entry gate that
  blocks new risk on orphan maker orders, unpaired live positions, ambiguous
  truth, and same-symbol/venue overlap with unresolved work.
- Production health now treats local-flat plus live non-reduce open orders as a
  critical exchange-truth mismatch, not green service health.

Review-closure follow-up on 2026-06-05 closed the remaining implementation
drift in that boundary:

- Startup and tick runtime paths now refresh the ledger before startup complete
  / entry dispatch when supported private truth probes are available.
- Unsupported private-truth probes are skipped rather than converted into
  ambiguous exchange-truth blockers.
- Startup live-position mismatch cleanup and
  `live_position_mismatch_flatten_failed` blockers remain the specific V1
  authority and are not overwritten by generic ledger blockers.
- Journal order evidence feeds `RecoveryOwnerIndex`, allowing live local-flat
  orders to map to probable owned pending work.
- `PendingEntryTerminalizer` is the removal authority for terminal pending
  entries, including recovery normalization drops.
- Invalid pending entries with order/fill/inflight evidence are retained and
  risk-only blocked instead of silently dropped.
- Ledger candidate gating blocks V1 same-symbol venue overlap but no longer
  blocks unrelated same-venue symbols.
- Legacy `available=False` exchange-truth payloads are treated as unavailable
  truth.

Pending-close follow-up on 2026-06-06 closed the related lifecycle gap:

- `PassiveCloseExecutor` registers live-only final close reconciliation records
  after live-flat cleanup using stored close-leg order/client ids and the
  removed position snapshot.
- `LiveRuntime` processes those records through close-leg fill reconciliation,
  and when fills are unavailable it probes terminal live sizes before deciding
  abandon versus fail-safe retention.
- Lifecycle drain preserves explicit reduce-only/fail-closed state and allows
  background reconciliation while private position truth for managed open
  positions is confirmed.
- `Supervisor` includes pending-close snapshot venues in supervised venues after
  the managed open position is gone.
