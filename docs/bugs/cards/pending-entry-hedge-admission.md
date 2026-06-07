# Bug Card: Pending Entry Hedge Admission

Purpose: keep the reusable memory for deterministic hedge-leg admission rejects.
Daily ledgers keep the full incident evidence; this card keeps the next-debug
decision path short.

## Stable Fingerprints

- `pending_entry.hedge_submit_result` with `outcome=error` and deterministic exchange reject text.
- `order.submit_result` rejected with `response_classification`.
- Bybit trading-terms family: `110126`, `110125`, `110123`, `must sign required agreement`.
- Aster max-notional family: `-5018`, `maximum notional value limit`, `max_notional_admission_blocked`.
- Hyperliquid insufficient-margin family: `Insufficient margin to place order.`
- Recurrence shape: maker leg has fill/exposure, hedge venue rejects deterministically, same pending keeps retrying until max lifetime or cleanup.

## Current Effective Rule

Deterministic hedge admission reject must:

1. Reuse the same admission classifier as initial entry dispatch.
2. Record `runtime.entry_admission_blocked`.
3. Emit `pending_entry.hedge_admission_blocked`.
4. Clear `hedge_inflight`.
5. Abort through the existing pending-entry cleanup path so maker exposure is flattened or retained fail-closed if cleanup cannot prove flat.
6. Prevent repeated same-pending hedge attempts for the same deterministic admission blocker.
7. Carry the same evidence shape on initial-entry, shortlist/cooldown, and pending-hedge paths: `venue`, `symbol`, `block_scope`, `blocked_until_ms`, `source=initial_entry|pending_hedge`, `candidate_pair_id`/`pair_id`, `official_doc_url`, and `evidence_gap=false` when the reject family is exchange-documented.
8. Venue-scope admission cooldowns must prune new-entry candidates before
   shortlist tracking or maker submit. This is a new-entry admission downgrade
   only; exchange truth, close, cancel, and residual repair must remain usable.

## V1 / Exchange Semantics

- Aster `-5018`: V1 detects max-notional submit reject and starts venue entry cooldown with reason `aster_max_notional_limit`. V2 should keep symbol evidence and also create venue-scope cooldown for this family.
- Bybit trading-terms rejects: no matching V1 definition found. Treat as exchange-documented admission/permission block, not as V1 copy work.
- Hyperliquid insufficient-margin rejects: no matching V1 exchange family found. Hyperliquid's official error response documents `Insufficient margin to place order.` under the perp margin family; V2 treats it as deterministic admission evidence with symbol cooldown, venue cooldown, shortlist/dispatch admission blocking, and pending hedge abort. Official doc: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses>.
- Transport-level classification alone is insufficient unless runtime consumes it in the pending hedge branch.

## Attempts Ledger

| Date | Attempt | Status | Why |
|---|---|---|---|
| 2026-05-20/26 | Transport `response_classification` for Bybit/Aster admission rejects | partial | Correctly classified reject payloads but did not stop pending-hedge runtime retries. |
| 2026-05-29 | Pending hedge admission consumer + cleanup/abort | effective | Cloud harness/probe passed; BZUSDT/LABUSDT flat/no-open-orders; post-deploy reject counts empty. |
| 2026-05-30 | Binance `-2027` leverage-cap classification | fixed, deployed, probe verified | Binance USD-M official `-2027 MAX_LEVERAGE_RATIO` now blocks initial entry and pending hedge like the existing Aster family, using Binance's official error-code doc URL. Cloud `HEIUSDT` reproduced the family and aborted cleanly. |
| 2026-06-04 | Hyperliquid insufficient-margin admission classification | deployed/cloud verified | Hyperliquid `Insufficient margin to place order.` now creates deterministic admission evidence for initial entry dispatch and pending hedge recovery, emits `pending_entry.hedge_admission_blocked`, and aborts through the existing cleanup path instead of retrying. |
| 2026-06-07 | Post-`21e5d44` Hyperliquid evidence-shape closure | local implementation pending deploy | The existing admission classifier was present, but the whole chain lacked uniform `source`, `block_scope`, `blocked_until_ms`, pair id, official doc URL, and `evidence_gap=false` evidence on pending hedge and entry cooldown events. RED/GREEN now pins those fields and keeps the Aster-specific venue cooldown reason from leaking into Hyperliquid. |
| 2026-06-07 | Hyperliquid venue-scope pre-shortlist admission downgrade | local implementation pending deploy | Active `venue_entry_cooldowns["hyperliquid:*"]` now prune Hyperliquid new-entry candidates before Local-L2/quote tracking. Selection and dispatch keep their existing admission blocks as bypass safety. |

## Recurrences

| Date | Symbols / Venues | Commit / Fix | Result | Detail |
|---|---|---|---|---|
| 2026-05-29 | `BZUSDT` Aster maker / Bybit hedge, `LABUSDT` Binance maker / Aster hedge | `6987fc8`; deployed through `bbcd7b9` docs sync | closed | [daily/2026-05-29.md#cluster-cl-017-post-deploy-pending-hedge-admission-and-okx-l2-evidence](../daily/2026-05-29.md#cluster-cl-017-post-deploy-pending-hedge-admission-and-okx-l2-evidence) |
| 2026-05-30 | `LITEUSDT`, `AVGOUSDT`, `HMSTRUSDT`, `HEIUSDT`, `GENIUSUSDT` | `0fd9a74` | admission/transport harness green; cloud targeted probes flat/no-open-orders | [daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission](../daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission) |
| 2026-05-31 | `AVGOUSDT` Bybit `110126`; attempted `STGUSDT`/`LABUSDT` Aster `-2027`/`-5018` admission blocks | existing admission classification | contained; no stuck pending entry or live exposure after read-only probes | [daily/2026-05-31.md#cluster-cl-025-post-ae4bd9c-passive-close-maker-leg-live-flat-precheck](../daily/2026-05-31.md#cluster-cl-025-post-ae4bd9c-passive-close-maker-leg-live-flat-precheck) |
| 2026-06-04 | `SEIUSDT` Bybit maker / Hyperliquid hedge | `1e082d9` | local RED/GREEN and cloud focused tests verify Hyperliquid insufficient margin now blocks initial entry and pending hedge retry; final cloud diagnose is flat/no-open-orders | [daily/2026-06-04.md#cluster-cl-049-post-cl048-seiusdt-open-maker-order-terminality](../daily/2026-06-04.md#cluster-cl-049-post-cl048-seiusdt-open-maker-order-terminality) |

## Regression Harness

- `tests/live_harness/test_exchange_admission_incidents.py`
- `tests/test_live_startup_preflight.py`
- `tests/test_venues_transport.py -k 'aster or bybit_110126 or bybit_trading_terms or max_notional'`
- `tests/test_pending_entry_v1_semantic_drift.py`

## Next Recurrence Checklist

1. Count `order.submit_result` rejected events by venue, symbol, and `response_classification`.
2. Check whether `pending_entry.hedge_admission_blocked` appears after the hedge reject.
3. Check `runtime.entry_admission_blocked` and `state.venue_entry_cooldowns`.
4. For Aster `-5018`, check `runtime.venue_cooldown_started` reason `aster_max_notional_limit`.
5. For Hyperliquid insufficient margin, check the symbol and venue cooldown events carry `source`, `block_scope`, `blocked_until_ms`, pair id, official doc URL, and `evidence_gap=false`.
6. If `hyperliquid:*` venue cooldown is active, confirm
   `runtime.entry_admission_venue_degraded` prunes Hyperliquid candidates before
   shortlist/Local-L2 tracking while close/cancel/recovery truth still runs.
7. Run `scripts/diagnose_live.py --json --symbol <symbol> --venues <maker,hedge> --since-deploy`.
8. Closure requires cloud harness plus high-confidence exchange truth flat/no-open-orders.
