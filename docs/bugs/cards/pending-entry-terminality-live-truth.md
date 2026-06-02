# Bug Card: Pending Entry Terminality And Live Truth

Purpose: keep the reusable memory for pending-entry false flat, stale accepted
orders, planned hedge CID misuse, and balanced live-position hydration.

## Stable Fingerprints

- Local state shows no open/pending work while exchange truth has nonzero positions.
- `stale_accepted_order`
- planned hedge client order id queried before submit.
- `pending_entry` cleared from momentary flat position snapshots.
- Balanced live-position evidence has quantity but missing local order/price details.

## Current Effective Rule

Pending entry terminality must be decided by real terminal exchange evidence,
not by stale accepted maker state, planned hedge IDs, stale recovery blocks, or
momentary flat probes. Live exchange truth dominates local false-flat state.
Balanced live-position evidence can hydrate pending fills with quantity and
price and finalize by V1 quantity+price semantics.

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

## Attempts Ledger

| Date | Attempt | Status | Why |
|---|---|---|---|
| 2026-05-17 | Pending hedge inflight metadata / deadline / cleanup parity | effective locally | Fixed direct-pop and cleanup semantics but later live truth exposed additional false-flat cases. |
| 2026-05-27 | Stale accepted / planned-CID / false-flat root fix | effective | Remote RED/GREEN and credentialed truth passed; known live mismatches flattened. |
| 2026-05-27 | PRL balanced live-position hydration | effective | Closed quantity-without-price/order evidence gap; pending finalized by V1 quantity+price semantics. |
| 2026-05-30 | ORCA/NOM/RAVE post-deploy live-truth watch | deployed/probe verified | Current probes are flat/no-open-orders; no local false-flat state found. Future recurrence still needs fixture classification instead of widening local heuristics. |
| 2026-06-01 | ARIA under-min hedge dust, imbalanced live hydration, stale recovery-block retry, drift false-negative verification, and planned hedge CID finalize query | deployed/cloud verified | Balanced `619` pending entry no longer stays pending when only untradeable hedge dust remains; imbalanced live truth finalizes the balanced quantity instead of treating excess as missing hedge; stale fail-closed recovery blocks no longer make pending-entry recovery unreachable; post-cleanup live truth can verify drift correction when synchronous fill evidence is incomplete; finalize no longer queries a planned-only hedge CID. |
| 2026-06-03 | Binance recovery placeholder reconciliation | local green, deploy pending | Binance USD-M `orderId` is exchange numeric id; V2 recovery placeholders now query with `origClientOrderId` when a client id is available. |

## Recurrences

| Date | Symbols / Venues | Commit / Fix | Result | Detail |
|---|---|---|---|---|
| 2026-05-27 | `MUBARAKUSDT`, `EDENUSDT`, `INUSDT`, `BEATUSDT`, `PRLUSDT` | remote hot patch family | closed | [daily/2026-05-27.md#cluster-cl-013-pending-entry-v1-terminality-drift-live-single-sided](../daily/2026-05-27.md#cluster-cl-013-pending-entry-v1-terminality-drift-live-single-sided) |
| 2026-05-30 | `ORCAUSDT`, `NOMUSDT`, `RAVEUSDT` | `0fd9a74`; no semantic code change selected for this family | final targeted probes flat/no-open-orders | [daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission](../daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission) |
| 2026-06-01 | `ARIAUSDT` Bybit/Binance | `f1727c1`; cloud verified | pending-entry live truth mismatch reproduced from production evidence; first deploy showed stale recovery block kept the fix unreachable; second deploy converted pending to open and flattened excess, then exposed drift-correction false-negative latching; final deploy reached running/flat/no-open-orders | [daily/2026-06-01.md#cluster-cl-027-pending-entry-live-truth-under-min-hedge-dust](../daily/2026-06-01.md#cluster-cl-027-pending-entry-live-truth-under-min-hedge-dust) |
| 2026-06-03 | `CLOUSDT`, `TRIAUSDT`, `PEOPLEUSDT` Binance | working tree | local green, deploy pending | [daily/2026-06-03.md#cluster-cl-035-post-e087513-long-window-follow-up](../daily/2026-06-03.md#cluster-cl-035-post-e087513-long-window-follow-up) |

## Regression Harness

- `tests/test_pending_entry_v1_semantic_drift.py`
- `tests/test_live_entry_hedge_root_fix.py`
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
8. Closure requires remote or cloud harness plus credentialed all-venue flat/no-open-orders probe.
