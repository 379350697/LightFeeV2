# Bug Card: Pending Entry Terminality And Live Truth

Purpose: keep the reusable memory for pending-entry false flat, stale accepted
orders, planned hedge CID misuse, and balanced live-position hydration.

Unified contract and coverage matrix:
[pending-entry-live-truth-contract](../contracts/pending-entry-live-truth-contract.md).
This contract is the only active intake for CL-048 / CL-049 / CL-050 and future
pending-entry live-truth recurrences. V1 is the coverage floor; new production
evidence must map to the matrix before runtime code changes.

## Stable Fingerprints

- Local state shows no open/pending work while exchange truth has nonzero positions.
- `stale_accepted_order`
- planned hedge client order id queried before submit.
- `pending_entry` cleared from momentary flat position snapshots.
- Balanced live-position evidence has quantity but missing local order/price details.
- `pending_entry.finalize_deferred_unresolved_maker_zero_fill`
- `pending_entry.finalize_fill_reconciliation_ignored_stale_zero`
- `reconciliation.rejected_pending_retained_with_fill` repeats while exchange
  truth has a nonzero position and local state has no managed open position.
- `startup_recovery_pending_work_without_open_positions` blocks recovery even
  though credentialed exchange truth proves a pending entry has live exposure.
- Hyperliquid `Insufficient margin to place order.` repeats after maker
  exposure, or new candidates continue to route through Hyperliquid while an
  account/venue margin block is active.
- A same-symbol candidate changes one venue and bypasses an unresolved pending
  entry that already shares the other venue.
- `_finalize_pending_entry()` emits deferred evidence, but the caller still
  resolves or removes the pending entry.
- `exchange_truth_recovery_ledger_blocked`
- `orphan_maker_order`
- `unpaired_live_position`
- `ambiguous_exchange_truth`
- local state is flat while exchange truth has a non-reduce-only open maker
  order.

## Current Effective Rule

Pending entry terminality must be decided by real terminal exchange evidence,
not by stale accepted maker state, planned hedge IDs, stale recovery blocks, or
momentary flat probes. Live exchange truth dominates local false-flat state.
Balanced live-position evidence can hydrate pending fills with quantity and
price and finalize by V1 quantity+price semantics.

Zero-fill reconciliation is terminal evidence only when the maker/order status
is terminal no-fill. A nonterminal maker order with zero fill keeps the pending
entry unresolved, and a later stale zero reconciliation must not erase a
previously confirmed positive fill.

A rejected/retained pending entry with positive fill evidence must not remain
as a local false-flat loop. Startup/runtime recovery must either hydrate and
finalize the live quantity into managed state, create deterministic residual or
reduce-only cleanup work, or stay fail-closed/risk-only with explicit exchange
truth evidence and no new-entry risk.

The finalizer return value is terminality evidence. If
`_finalize_pending_entry()` defers because fill details, open-order truth, or
live-position truth are incomplete, callers must retain pending work and back
off. A deferred finalizer call is not a resolved entry.

While a pending entry is unresolved, V1 protects the same pair and the same
symbol with either venue overlapping. A new route cannot bypass live-truth
recovery by swapping only one venue.

The V1-style recovery ledger is now the shared intake for this family. Local
empty `open_positions`, `pending_entries`, and `pending_residual_repairs` are
not enough to prove flat. Runtime must classify exchange positions, exchange
open orders, pending work, residual repair, passive close work, owner evidence,
and ambiguous probe evidence before normal entry risk is allowed. A live
non-reduce order without a proven owner becomes blocking `orphan_maker_order`;
a live position without a proven owner becomes blocking `unpaired_live_position`.

## V1 / Exchange Semantics

- V1 keeps accepted maker evidence uncertain until terminal no-fill/fill evidence exists.
- V1 does not query a planned hedge CID before that hedge is submitted.
- V1 can finalize balanced entries from live-position quantity and price evidence, even if local order ids are missing after recovery.
- Duplicate client-id cleanup must be reconciled against live position truth before treating an old filled order as full cleanup.
- A balanced pending entry with only an untradeable hedge dust residual must
  terminalize the dust and finalize the balanced open position; otherwise
  live-truth drift repair cannot act on exchange excess.
- If startup live truth is imbalanced but both legs have trusted positions and
  no open orders, hydrate/finalize the balanced quantity and let live-truth
  drift repair close the excess side. Do not treat the excess as new missing
  hedge demand.
- Planned hedge client IDs are not submitted-order evidence. They must not be
  queried during finalize unless the hedge has an order id, inflight record,
  attempt count, or fill evidence.
- A stale fail-closed recovery block must not make existing pending-entry
  recovery unreachable on restart. Retry startup recovery for old blocks when
  recovery work still exists, while preserving operator-requested fail-closed
  and blocks created by the current startup probe.
- Drift cleanup submit/fill evidence is not enough to declare correction
  failure when live truth already proves both legs are back to the target
  balanced quantity.
- Synthetic recovery order ids such as `entry-...-recovery-short` are local
  placeholders, not exchange order ids. Venue reconciliation must use the
  submitted client id field when an exchange requires a distinct client-order
  lookup parameter.
- A stale `startup_recovery_pending_work_without_open_positions` block must be
  released after runtime live-position recovery creates a balanced managed open
  position and no pending entry/close/passive-close work remains. Open
  positions are startup recovery work before finalization, but normal managed
  state after V1 recovery has recovered them.
- V1 terminalization budget, terminal taker fallback/repost/cooldown,
  supervision backlog clear, and ambiguous-live fail-closed semantics are now
  explicit matrix rows (`PE-12` through `PE-16`). They must not be treated as
  optional just because CL-048 / CL-049 / CL-050 did not exercise all of them.
- V1 `pending_entry_gate_reason_excluding_position` blocks same-pair pending
  entries and same-symbol candidates with any venue overlap. The block reason is
  `pending_entry_protection`.
- Hyperliquid insufficient margin is deterministic admission evidence. Once it
  proves account/venue capacity is exhausted, V2 must block new entries through
  that venue before maker submit, not discover the same condition after another
  maker exposure.

## Attempts Ledger

| Date | Attempt | Status | Why |
|---|---|---|---|
| 2026-05-17 | Pending hedge inflight metadata / deadline / cleanup parity | effective locally | Fixed direct-pop and cleanup semantics but later live truth exposed additional false-flat cases. |
| 2026-05-27 | Stale accepted / planned-CID / false-flat root fix | effective | Remote RED/GREEN and credentialed truth passed; known live mismatches flattened. |
| 2026-05-27 | PRL balanced live-position hydration | effective | Closed quantity-without-price/order evidence gap; pending finalized by V1 quantity+price semantics. |
| 2026-05-30 | ORCA/NOM/RAVE post-deploy live-truth watch | deployed/probe verified | Current probes are flat/no-open-orders; no local false-flat state found. Future recurrence still needs fixture classification instead of widening local heuristics. |
| 2026-06-01 | ARIA under-min hedge dust, imbalanced live hydration, stale recovery-block retry, drift false-negative verification, and planned hedge CID finalize query | deployed/cloud verified | Balanced `619` pending entry no longer stays pending when only untradeable hedge dust remains; imbalanced live truth finalizes the balanced quantity instead of treating excess as missing hedge; stale fail-closed recovery blocks no longer make pending-entry recovery unreachable; post-cleanup live truth can verify drift correction when synchronous fill evidence is incomplete; finalize no longer queries a planned-only hedge CID. |
| 2026-06-03 | Binance recovery placeholder reconciliation | deployed, short-window clean | Binance USD-M `orderId` is exchange numeric id; V2 recovery placeholders now query with `origClientOrderId` when a client id is available. |
| 2026-06-03 | Runtime live recovery stale startup block release | deployed/cloud verified | After a missing hedge is recovered and live truth proves a balanced managed open position, V2 must reuse V1 startup finalization instead of leaving `risk_only` latched. |
| 2026-06-04 | Bybit nonterminal zero-fill and stale zero reconciliation | deployed through contract family | Maker zero-fill with nonterminal evidence no longer finalizes as passive unfilled, and later zero reconciliation no longer erases known positive hedge fill. Final pending-entry contract acceptance was closed at `68a979b`. |
| 2026-06-04 | SEIUSDT post-deploy retained pending/live-truth mismatch | deployed/cloud verified | Rejected pending entries with positive fill evidence now route through V1 `_finalize_pending_entry()` from startup force reconcile, startup recovery, and normal reconciliation. The SEIUSDT fixture finalizes maker `455.0` / hedge `68.0` into matched open `68.0` plus Bybit residual repair `387.0`; incomplete evidence is retained with explicit deferred events instead of local false-flat looping. Full pytest reached `3434 passed`, `9 skipped`, `1 warning`; cloud final health is running/flat with no open orders. |
| 2026-06-04 | SEIUSDT open maker order truth before zero-fill finalize | deployed/cloud verified | `_finalize_pending_entry()` now queries live maker open-order truth before removing a zero-fill pending entry. If a matching maker order is still open, pending is retained with `uncertain_outcome`, reconcile backoff, and `pending_entry.finalize_deferred_maker_open_order`; diagnose also treats acceptance-gate open-order blockers as unhealthy. Cloud final truth is high-confidence flat/no-open-orders on all venues. |
| 2026-06-04 | BIOUSDT live-position truth before zero-fill finalize | deployed/cloud verified | `_finalize_pending_entry()` now checks maker live-position truth before removing a zero-fill pending entry. If maker live position is nonzero, pending is retained with `uncertain_outcome`, reconcile backoff, and `pending_entry.finalize_deferred_maker_live_position`; residual duplicate cleanup and production service gating are also covered. Cloud `68a979b` final verifier/diagnose proved all seven venues flat/no-open-orders. |
| 2026-06-05 | Full-loop V1 parity follow-up after post-deploy pending churn | deployed; production acceptance blocked by live open order | Hyperliquid insufficient-margin now creates both symbol and venue admission cooldowns; same-symbol venue-overlap pending entries block with `pending_entry_protection`; `_finalize_pending_entry()` returns terminality status and runtime callers retain/backoff on deferred finalization instead of resolving/popping; force-terminal zero-fill routes through the finalizer so positive/live evidence cannot be discarded. Cloud deployed `3af002d` and services stayed active, but diagnose still found one Bybit `TRXUSDT` non-reduce-only open maker order from pre-deploy state, so acceptance remains blocked by `DG-02`. |
| 2026-06-05 | Exchange-truth recovery ledger V1 parity | locally implemented, not deployed in this pass | Added sanitized `TRXUSDT` and `SEIUSDT` incident fixtures, pure `RecoveryLedger`, shared exchange-truth normalizer, recovery owner index, pending-entry terminalizer, runtime recovery-ledger refresh/entry gate, shared pending-entry post-terminal removal helper, and production-health local-flat/live-open-order critical classification. Focused tests passed: core ledger/owner/truth/terminalizer `27 passed`, startup/runtime/passive-close `359 passed`, diagnose/health `66 passed`, pending-entry parity `35 passed`; full pytest `3479 passed`, `9 skipped`, `1 warning`; compileall and diff-check passed; GitNexus staged detect-changes reported medium risk, 23 files, 47 symbols, and 3 affected verifier flows. |
| 2026-06-05 | Exchange-truth recovery ledger review closure | locally implemented, not deployed in this pass | Closed post-review drift without CL-specific branches: startup/tick now refresh the ledger from supported private truth probes; metadata-only adapters do not create ambiguous ledger blockers; startup mismatch cleanup outcomes are not overwritten by generic ledger blockers; journal owner evidence feeds the owner index; terminal pending removal stays behind `PendingEntryTerminalizer`; invalid pending entries with exchange evidence are retained and risk-only blocked; same-symbol venue-overlap still blocks while unrelated same-venue symbols do not; legacy `available=False` truth is treated as unavailable. Focused and adjacent suites passed, and full pytest now reports `3487 passed`, `9 skipped`, `1 warning`. |

## Recurrences

| Date | Symbols / Venues | Commit / Fix | Result | Detail |
|---|---|---|---|---|
| 2026-05-27 | `MUBARAKUSDT`, `EDENUSDT`, `INUSDT`, `BEATUSDT`, `PRLUSDT` | remote hot patch family | closed | [daily/2026-05-27.md#cluster-cl-013-pending-entry-v1-terminality-drift-live-single-sided](../daily/2026-05-27.md#cluster-cl-013-pending-entry-v1-terminality-drift-live-single-sided) |
| 2026-05-30 | `ORCAUSDT`, `NOMUSDT`, `RAVEUSDT` | `0fd9a74`; no semantic code change selected for this family | final targeted probes flat/no-open-orders | [daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission](../daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission) |
| 2026-06-01 | `ARIAUSDT` Bybit/Binance | `f1727c1`; cloud verified | pending-entry live truth mismatch reproduced from production evidence; first deploy showed stale recovery block kept the fix unreachable; second deploy converted pending to open and flattened excess, then exposed drift-correction false-negative latching; final deploy reached running/flat/no-open-orders | [daily/2026-06-01.md#cluster-cl-027-pending-entry-live-truth-under-min-hedge-dust](../daily/2026-06-01.md#cluster-cl-027-pending-entry-live-truth-under-min-hedge-dust) |
| 2026-06-03 | `CLOUSDT`, `TRIAUSDT`, `PEOPLEUSDT` Binance | `a272b6b` / `cb1abbe` | deployed, short-window clean; green health blocked by separate CL037 | [daily/2026-06-03.md#cluster-cl-035-post-e087513-long-window-follow-up](../daily/2026-06-03.md#cluster-cl-035-post-e087513-long-window-follow-up) |
| 2026-06-03 | `HIVEUSDT` Binance/Bybit | `5625361` | cloud verified: lifecycle running, local flat, credentialed exchange truth flat/no-open-orders; close-path retry noise remains a separate watch | [daily/2026-06-03.md#cluster-cl-037-runtime-live-recovery-stale-startup-block](../daily/2026-06-03.md#cluster-cl-037-runtime-live-recovery-stale-startup-block) |
| 2026-06-04 | `MEUUSDT`, `LDOUSDT` Bybit-related entry/fill samples | deployed; later contract acceptance closed | full pytest `3432 passed`, `9 skipped`, `1 warning`; later pending-entry contract acceptance at `68a979b` proved production flat/no-open-orders | [daily/2026-06-04.md#cluster-cl-046-bybit-entry-passive-close-v1-deadline-loop](../daily/2026-06-04.md#cluster-cl-046-bybit-entry-passive-close-v1-deadline-loop) |
| 2026-06-04 | `SEIUSDT` Bybit/Hyperliquid | `8be067e`; deployed via `30aba89` | local RED/GREEN fixed retained rejected positive-fill recovery; cloud emitted `recovery.rejected_pending_positive_fill_finalized`, completed residual repair and drift correction, then settled to `lifecycle=running`, local open/pending/residual `0/0/0`, all-venue exchange truth flat/no open orders | [daily/2026-06-04.md#cluster-cl-048-post-deploy-seiusdt-pending-entry-live-truth-mismatch](../daily/2026-06-04.md#cluster-cl-048-post-deploy-seiusdt-pending-entry-live-truth-mismatch) |
| 2026-06-04 | `SEIUSDT` Bybit/Hyperliquid open maker order | `1e082d9` | local RED/GREEN prevents zero-fill finalization when Bybit live open-order truth still has the maker order; cloud final diagnose is healthy/high-confidence flat/no-open-orders on all venues, and no manual order/state mutation was used | [daily/2026-06-04.md#cluster-cl-049-post-cl048-seiusdt-open-maker-order-terminality](../daily/2026-06-04.md#cluster-cl-049-post-cl048-seiusdt-open-maker-order-terminality) |
| 2026-06-04 | `BIOUSDT` Bybit live maker position | `68a979b` | deployed/cloud verified: local RED/GREEN prevents zero-fill finalization when Bybit maker live-position truth is nonzero; RC-08 duplicate cleanup convergence and DG-01 production service gate are covered; final verifier/diagnose proved all seven venues flat/no-open-orders. CL-048/049/050 now map to the contract matrix instead of separate active bug tracks. | [daily/2026-06-04.md#cluster-cl-050-biousdt-live-position-zero-fill-terminality](../daily/2026-06-04.md#cluster-cl-050-biousdt-live-position-zero-fill-terminality) |
| 2026-06-05 | `WLDUSDT`, `XLMUSDT`, `MONUSDT`, `MOVEUSDT` Hyperliquid margin rejects; `XLMUSDT` overlapping pending route; post-deploy `TRXUSDT` Bybit open maker order | deployed `3af002d`; acceptance blocked | Mapped to `PE-11`, `PE-17`, `PE-18`, plus `PE-02`/`DG-02` for the remaining Bybit open order. Local RED/GREEN covers venue-level margin cooldown, same-symbol venue-overlap pending protection, deferred-finalizer retention, and force-terminal zero-fill finalizer routing. Cloud deploy passed manifest/services, but diagnose found Bybit open order `a84df707-efb3-4e40-bab1-641a4eb0f3d4` for `72.0` `TRXUSDT`; no manual order/state mutation was performed. | [daily/2026-06-05.md#contract-follow-up-pending-entry-v1-full-loop-parity](../daily/2026-06-05.md#contract-follow-up-pending-entry-v1-full-loop-parity) |
| 2026-06-05 | `TRXUSDT` Bybit live open maker order local-flat; `SEIUSDT` Bybit positive-fill local false-flat | local implementation pending deploy | Mapped to the unified exchange-truth recovery ledger. `TRXUSDT` local-flat/live-open-order becomes blocking `orphan_maker_order`; `SEIUSDT` positive-fill evidence prevents proven-flat and routes into blocking recovery work. Runtime entry gating now asks the ledger before dispatch, and production health flags live non-reduce open orders as critical exchange-truth mismatches. | [daily/2026-06-05.md#contract-follow-up-exchange-truth-recovery-ledger-v1-parity](../daily/2026-06-05.md#contract-follow-up-exchange-truth-recovery-ledger-v1-parity) |
| 2026-06-05 | Review closure for exchange-truth recovery ledger wiring and terminalizer authority | local implementation pending deploy | Startup and tick wiring, clean-blocker release, journal owner mapping, unsupported-probe handling, V1 same-symbol venue-overlap scope, and terminalizer-only pending removal are now covered by RED/GREEN regression tests. This is a closure of the ledger contract, not a new symbol-specific bug. | [daily/2026-06-05.md#contract-follow-up-exchange-truth-recovery-ledger-v1-parity](../daily/2026-06-05.md#contract-follow-up-exchange-truth-recovery-ledger-v1-parity) |

## Regression Harness

- `tests/test_pending_entry_v1_semantic_drift.py`
- `tests/test_live_entry_hedge_root_fix.py`
- `tests/engine/test_recovery_ledger.py`
- `tests/engine/test_recovery_owner_index.py`
- `tests/engine/test_exchange_truth_runtime.py`
- `tests/engine/test_pending_entry_terminalizer.py`
- `tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py`
- `tests/live_harness`
- `scripts/diagnose_live.py --venues ...`

## Next Recurrence Checklist

1. Compare local `open_positions` / `pending_entries` against explicit exchange truth on all possible venues.
2. Search for stale accepted maker evidence and planned hedge CID lookups.
3. Verify whether any live position proof contains enough quantity and price evidence to hydrate pending fills.
4. Check whether `recovery_blocked_reason` is preventing startup recovery from running against retained pending work.
5. After reduce-only cleanup failure/uncertain evidence, re-probe live truth before accepting a fail-closed drift latch.
6. If local is flat and exchange nonzero, treat as critical false-green until reduce-only cleanup or fail-closed retention proves safe.
7. For Binance reconciliation, verify a local recovery placeholder is not sent
   as `orderId`; use `origClientOrderId` unless the id is numeric exchange
   truth.
8. If a runtime live probe recovers balanced exchange positions while an old
   `startup_recovery_pending_work_without_open_positions` block is latched,
   confirm the lifecycle re-finalizes to `running` when no pending work remains.
9. If maker reconciliation reports quantity `0`, confirm the order status is
   terminal no-fill before finalizing as unfilled; otherwise keep pending
   unresolved.
10. If a leg already has positive fill progress, reject later stale zero
   reconciliation unless exchange truth proves a terminal correction.
11. Closure requires remote or cloud harness plus credentialed all-venue flat/no-open-orders probe.
12. For retained rejected pending entries with positive fill evidence, verify
   startup recovery can use credentialed live truth to hydrate/finalize,
   residualize, or issue deterministic reduce-only cleanup instead of looping
   in local false-flat/risk-only.
13. If Hyperliquid insufficient margin appears, confirm whether the block is
   account/venue scoped. If yes, future candidates through Hyperliquid must be
   blocked before maker submit.
14. If a pending entry remains unresolved, block same-symbol candidates that
   share either venue until finalization, residual cleanup, or fail-closed
   evidence releases the risk.
15. If finalization emits a deferred event, check that the caller retained the
   pending entry and did not append it to a resolved list or pop it afterward.
16. If local state is flat, build or inspect the recovery ledger before calling
   the state safe. A live position, non-reduce open order, unavailable exchange
   truth, or positive fill evidence must map to ledger work before any
   production-health conclusion or entry-risk decision.
17. For quick-flat reports, join `execution.entry_selected`, `entry.opened`,
    `runtime.funding_capture_state_updated`, `runtime.normal_close_routing_*`,
    and `exit.closed` by `position_id`.
18. Deduplicate duplicate `exit.closed` projections before judging frequency.
19. Classify each quick flat as bug, avoidable timing, unavoidable recovery, or
    duplicate observation.
