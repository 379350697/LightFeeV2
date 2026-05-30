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

Pending entry terminality must be decided by real terminal exchange evidence, not by stale accepted maker state, planned hedge IDs, or momentary flat probes. Live exchange truth dominates local false-flat state. Balanced live-position evidence can hydrate pending fills with quantity and price and finalize by V1 quantity+price semantics.

## V1 / Exchange Semantics

- V1 keeps accepted maker evidence uncertain until terminal no-fill/fill evidence exists.
- V1 does not query a planned hedge CID before that hedge is submitted.
- V1 can finalize balanced entries from live-position quantity and price evidence, even if local order ids are missing after recovery.
- Duplicate client-id cleanup must be reconciled against live position truth before treating an old filled order as full cleanup.

## Attempts Ledger

| Date | Attempt | Status | Why |
|---|---|---|---|
| 2026-05-17 | Pending hedge inflight metadata / deadline / cleanup parity | effective locally | Fixed direct-pop and cleanup semantics but later live truth exposed additional false-flat cases. |
| 2026-05-27 | Stale accepted / planned-CID / false-flat root fix | effective | Remote RED/GREEN and credentialed truth passed; known live mismatches flattened. |
| 2026-05-27 | PRL balanced live-position hydration | effective | Closed quantity-without-price/order evidence gap; pending finalized by V1 quantity+price semantics. |
| 2026-05-30 | ORCA/NOM/RAVE post-deploy live-truth watch | effective so far | Current probes are flat/no-open-orders; no local false-flat state found, but RAVE/ORCA need fixture classification if repeated. |

## Recurrences

| Date | Symbols / Venues | Commit / Fix | Result | Detail |
|---|---|---|---|---|
| 2026-05-27 | `MUBARAKUSDT`, `EDENUSDT`, `INUSDT`, `BEATUSDT`, `PRLUSDT` | remote hot patch family | closed | [daily/2026-05-27.md#cluster-cl-013-pending-entry-v1-terminality-drift-live-single-sided](../daily/2026-05-27.md#cluster-cl-013-pending-entry-v1-terminality-drift-live-single-sided) |
| 2026-05-30 | `ORCAUSDT`, `NOMUSDT`, `RAVEUSDT` | deployed `bbcd7b9`; no code fix selected | open watch; current exchange truth flat | [daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission](../daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission) |

## Regression Harness

- `tests/test_pending_entry_v1_semantic_drift.py`
- `tests/test_live_entry_hedge_root_fix.py`
- `tests/live_harness`
- `scripts/diagnose_live.py --venues ...`

## Next Recurrence Checklist

1. Compare local `open_positions` / `pending_entries` against explicit exchange truth on all possible venues.
2. Search for stale accepted maker evidence and planned hedge CID lookups.
3. Verify whether any live position proof contains enough quantity and price evidence to hydrate pending fills.
4. If local is flat and exchange nonzero, treat as critical false-green until reduce-only cleanup or fail-closed retention proves safe.
5. Closure requires remote or cloud harness plus credentialed all-venue flat/no-open-orders probe.
