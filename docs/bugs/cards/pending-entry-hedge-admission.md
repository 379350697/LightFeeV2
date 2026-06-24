# Bug Card: Pending Entry Hedge Admission

Purpose: keep the reusable memory for deterministic hedge-leg admission rejects.
Daily ledgers keep the full incident evidence; this card keeps the next-debug
decision path short.

## Stable Fingerprints

- `pending_entry.hedge_submit_result` with `outcome=error` and deterministic exchange reject text.
- `order.submit_result` rejected with `response_classification`.
- `order.precheck_result` rejected before maker dispatch for venues with official non-mutating order pre-check support.
- Bybit trading-terms family: `110126`, `110125`, `110123`, `must sign required agreement`.
- Bybit opening balance family: `110007`, `ab not enough for new order`, `Available balance is insufficient`.
- Aster max-notional family: `-5018`, `maximum notional value limit`, `max_notional_admission_blocked`.
- Hyperliquid insufficient-margin family: `Insufficient margin to place order.`
- Gate futures contract sizing family: `INSUFFICIENT_AVAILABLE` with
  `quantity_units=base_to_gate_contracts`, `contract_qty`, and
  `contract_multiplier`.
- `execution.entry_quantity_plan` with `quantity_contract_status` and
  `unhedgeable_residual_quantity`.
- `execution.entry_quantity_plan` with `quantity_plan_reason=planner_quantity_adjustment`
  followed by unopened terminal evidence such as `runtime.entry_admission_blocked`.
- `runtime.entry_admission_venue_degraded` with `aggregation_key` and
  `suppressed_count`.
- `runtime.entry_admission_symbol_cooldown_armed`
- `runtime.venue_cooldown_started`
- Recurrence shape: maker leg has fill/exposure, hedge venue rejects deterministically, same pending keeps retrying until max lifetime or cleanup.

## Current Effective Rule

Deterministic hedge admission reject must:

1. For Bybit non-reduce-only entry exposure, run the official
   `/v5/order/pre-check-order` before maker dispatch when the live adapter
   supports it.
2. Reuse the same admission classifier as initial entry dispatch.
3. Record `runtime.entry_admission_blocked`.
4. Emit `pending_entry.hedge_admission_blocked` when the reject is discovered
   in the pending-hedge fallback branch.
5. Clear `hedge_inflight` on pending-hedge discovery.
6. Abort through the existing pending-entry cleanup path so maker exposure is flattened or retained fail-closed if cleanup cannot prove flat.
7. Prevent repeated same-pending hedge attempts for the same deterministic admission blocker.
8. Carry the same evidence shape on pre-entry, initial-entry, shortlist/cooldown, and pending-hedge paths: `venue`, `symbol`, `block_scope`, `blocked_until_ms`, `source=pre_entry_bybit_precheck|initial_entry|pending_hedge`, `candidate_pair_id`/`pair_id`, `official_doc_url`, and `evidence_gap=false` when the reject family is exchange-documented.
9. Venue-scope admission cooldowns must prune new-entry candidates before
   shortlist tracking or maker submit. This is a new-entry admission downgrade
   only; exchange truth, close, cancel, and residual repair must remain usable.
10. For paired entry, both legs must pass admission/headroom immediately before
    maker submit. If the hedge leg deterministically cannot accept new risk,
    runtime must block selection before any maker `order.passive_submitted`.
11. If a maker fill already created a single leg before a deterministic hedge
    admission block is discovered, cleanup is the final loss-control path. After
    cleanup, the route must arm hard symbol/venue cooldown and diagnostics must
    flag any maker submit on the same guarded route as repeated single-leg
    fee-drag risk.
12. Bybit `110007` is an opening admission block. It must arm
    `bybit:SYMBOL` opening cooldown and be consumed by candidate/selection
    filters before another maker submit. The cooldown does not block
    reduce-only close, passive close maintenance, cancel, or recovery truth.
13. Candidate and quote-rewarm quality is part of admission closure. A
    deterministic admission/cooldown route should be pruned before selection;
    a quote rewarm that reaches hard stale must emit
    `runtime.entry_quote_rewarm_terminal_stale`, write cooldown, and stop
    repeated same venue/symbol scheduling until TTL expires.
14. Entry sizing must be hedgeable before maker submit. If venue step/contract
    size converts raw size into a smaller common quantity, journal it as
    `quantity_contract_status=hedgeable_adjusted` with the
    `unhedgeable_residual_quantity`. If the route cannot produce a hedgeable
    quantity, block before maker submit; residual repair remains a fallback,
    not the normal sizing path.
15. For new recurrence fixes, check `lightfee/engine/business_contract.py`
    first. Admission block reasons, quantity hedgeability, and diagnostic
    process counters should be extended there before adding another local
    predicate in entry runtime or diagnose.
16. `execution.dual_taker_armed` is a pending-entry terminal fallback signal.
    It must map through `classify_business_event_kind` to V1 `PENDING_ENTRY`
    unless the payload explicitly says `execution_kind=exit`; diagnose and
    lifecycle should not each carry a separate allowlist.
17. Candidate quantity plans that never reached actual opened exposure must not
    become current entry quantity warnings. If a planner adjustment is followed
    by `entry.aborted`, `entry.passive_unfilled`,
    `runtime.entry_admission_blocked`, or
    `pending_entry.hedge_admission_blocked` without balanced opened evidence,
    classify it as unopened terminal candidate evidence.
18. Gate futures order `size` is contract count, while engine
    `OrderRequest.quantity` is base quantity. Live Gate order sizing must use
    official contract metadata (`quanto_multiplier`, `order_size_round`,
    `order_size_min`) and fail closed when that metadata is missing.

## V1 / Exchange Semantics

- Aster `-5018`: V1 detects max-notional submit reject and starts venue entry cooldown with reason `aster_max_notional_limit`. V2 should keep symbol evidence and also create venue-scope cooldown for this family.
- Aster V2 private V3 must now do better than V1's post-submit detection:
  non-`reduce_only` new-risk orders precheck
  `remainingOpenableNotionalValue` before submitting. If requested notional is
  above the remaining headroom, block the whole candidate with
  `max_notional_admission_blocked`; do not shrink one leg and submit a smaller
  one-sided order. Reduce-only close/flatten remains allowed even if headroom
  truth is unavailable. The V3 adapter must source this precheck through the
  Aster V3 signer client, not generic Binance-HMAC private transport; if truth
  is unavailable, runtime must still arm symbol + venue admission cooldown with
  `evidence_gap=true`.
- Aster Pro API V3 credentials are API-wallet/Web3 credentials, not Binance
  HMAC credentials. Aster public market data can keep Binance-compatible FAPI
  paths, but private account/order/open-order probes must use Aster V3
  `https://fapi3.asterdex.com/fapi/v3/*` with `nonce`, `signer`, and EIP-712
  `signature`. Aster private failures on old `/fapi/v1|v2|v4` HMAC paths are
  transport-integration drift, not admission rejects.
- Aster account-risk truth specifically uses Pro API V3
  `GET /fapi/v3/accountWithJoinMargin`. Capability metadata must not advertise
  Binance-style private WS/listen-key health for Aster V3; passive progress and
  accepted-order uncertainty truth rely on REST V3 polling/probes.
- Bybit trading-terms rejects: no matching V1 definition found. Treat as exchange-documented admission/permission block, not as V1 copy work. Use Bybit's official non-mutating order pre-check endpoint as the maker-before-hedge protection; keep the pending-hedge branch as defense-in-depth.
- Bybit `110007` balance rejects are deterministic for non-reduce-only
  opening risk and should be blocked at candidate/selection once observed.
  They are not reduce-only close admission rejects.
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
| 2026-06-08 | Aster Pro API V3 private transport isolation | fixed, deployed through main | Aster private account/order/open-order probes are no longer routed through shared Binance HMAC transport. V2 now keeps public Aster FAPI market data separate from a dedicated V3 Web3 signer client, so V3 API-wallet credentials do not produce false `Signature for this request is not valid` private-truth failures. Review closure also pins `accountWithJoinMargin`, Aster V3 capability truth, and REST V3 order-truth probe paths. Follow-up startup safety and host fixes are in main (`9d037f5`, `6054c47`) and the deployed manifest line. |
| 2026-06-08 | Production issues 3-7 admission evidence audit | fixed, deployed/cloud verified | Existing admission classifier and venue-scope downgrade coverage stayed authoritative. CL-052 did not add another admission branch; it records that deployment review should verify the already-covered `runtime.entry_admission_blocked`, `runtime.entry_admission_venue_degraded`, `pending_entry.hedge_admission_blocked`, and `entry.aborted` payload fields across the same source/scope/venue/symbol/pair/action/cooldown contract. |
| 2026-06-09 | Bybit trading-terms maker-before-hedge precheck | local green, deploy pending | Bybit non-reduce-only entry exposure now uses `/v5/order/pre-check-order` before maker dispatch when the adapter supports it. `110125/110126/110123` records `runtime.entry_admission_blocked` with `source=pre_entry_bybit_precheck` and prevents maker submit; pending hedge admission handling remains the fallback. |
| 2026-06-18 | Aster `-5018` remaining-openable-notional pre-submit gate | deployed through `039d52c` | Aster V3 and transport submits now share the admission helper, but V3 gets headroom through the dedicated V3 signer client rather than generic HMAC transport. Insufficient or unavailable headroom blocks the candidate before HTTP order submit, emits `runtime.entry_admission_blocked`, arms symbol + venue cooldown, keeps reduce-only cleanup allowed, and no longer retries by shrinking quantity after exchange `-5018`. |
| 2026-06-19 | Two-leg admission selection and single-leg fee-drag guard | deployed through `039d52c` | Entry dispatch now verifies hedge venue/symbol admission after selection and before maker submit. Aster zero headroom, max-notional, and error-only `max_notional_admission_blocked` shapes block pre-submit when truth is available. If a deterministic hedge block is only discovered after maker fill, cleanup still runs through the single-leg fallback, but hard cooldown plus `business_progression_quality_summary.repeated_single_leg_guarded` makes a repeated maker submit on the same route a production issue instead of silent fee churn. |
| 2026-06-19 | Bybit 110007 and quote-rewarm process-quality closure | deployed through `039d52c`; `106f47e` keeps diagnostics aligned | Bybit `110007` is treated as opening-only deterministic admission cooldown, while reduce-only close remains allowed. Diagnose consumes production-gate truth so historical quote/admission hard-over-budget artifacts remain counted as process issues without becoming current active stuck when exchange truth and lifecycle blockers are clean. `106f47e` keeps that distinction while adding passive-close waiting-event truth payloads, so admission/quote recovered artifacts do not hide live close blockers or create false active stuck. |
| 2026-06-19 | Centralized business contract for entry sizing/admission diagnostics | `8eadb8e` deployed as clean baseline; unified-contract follow-up local, deploy pending | CL-102 adds `lightfee/engine/business_contract.py` and wires entry quantity plans, admission degraded aggregation keys, business-progression process counters, quote-rewarm handoff evidence, and dual-taker lifecycle mapping through it. HOMEUSDT-style `1856 -> 1800` contract-size adjustment is now explicit evidence, not inferred from later residual repair or terminal state. Unhedgeable quantity must block before maker submit rather than relying on residual repair as the normal path. |
| 2026-06-21 | Unopened and terminal-flat quantity plans plus cooldown/min-notional lifecycle mapping | `9dee9f3` deployed/cloud verified; terminal quantity-warning residual fixed locally, deploy pending | CL-105 keeps actual unproven opened-entry quantity warnings strict, but moves planner adjustments for unopened admission-blocked candidates and terminal-flat accounting-gap owners into resolved terminal evidence. The local residual follow-up also covers `reconciliation.entry_abandoned_flat` and entry-plan `exchange_step_rounding` when terminal/unopened, residual-repaired, or current clean exchange truth proves no active exposure. `runtime.entry_admission_symbol_cooldown_armed`, `runtime.venue_cooldown_started`, `execution.min_notional_accumulating`, `execution.min_notional_abort_and_flatten`, and `execution.pending_entry_hedge_chunk_buffering` no longer pollute V1 lifecycle acceptance. Entry `execution.dual_taker_armed` remains `PENDING_ENTRY`; exit payloads map separately to `PASSIVE_CLOSE`. |
| 2026-06-23 | Aster max-notional admission headroom diagnostics | local green, deploy pending | CL-110 keeps Aster `max_notional_admission_blocked` fail-closed and adds actionable diagnosis only: `requested_notional`, `remaining_openable_notional`, computed `notional_gap`, leverage, top blocked symbols, and account/leverage/position-limit/capital advice. It does not auto-shrink quantity or bypass `remainingOpenableNotionalValue`. |
| 2026-06-24 | Aster max-notional cooldown before entry prewarm | `653b21a` deployed/cloud verified | CL-115 keeps Aster `max_notional_admission_blocked` as a venue-scope new-entry cooldown and verifies it prunes candidates before quote/OI prewarm or maker submit. This reduces empty prewarm churn after Gate/Bitget candidate recovery without weakening the fail-closed admission guard. Cloud log scan after the new service start had no `max_notional_admission_blocked` repeats in the deploy window. |

## Recurrences

| Date | Symbols / Venues | Commit / Fix | Result | Detail |
|---|---|---|---|---|
| 2026-05-29 | `BZUSDT` Aster maker / Bybit hedge, `LABUSDT` Binance maker / Aster hedge | `6987fc8`; deployed through `bbcd7b9` docs sync | closed | [daily/2026-05-29.md#cluster-cl-017-post-deploy-pending-hedge-admission-and-okx-l2-evidence](../daily/2026-05-29.md#cluster-cl-017-post-deploy-pending-hedge-admission-and-okx-l2-evidence) |
| 2026-05-30 | `LITEUSDT`, `AVGOUSDT`, `HMSTRUSDT`, `HEIUSDT`, `GENIUSUSDT` | `0fd9a74` | admission/transport harness green; cloud targeted probes flat/no-open-orders | [daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission](../daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission) |
| 2026-05-31 | `AVGOUSDT` Bybit `110126`; attempted `STGUSDT`/`LABUSDT` Aster `-2027`/`-5018` admission blocks | existing admission classification | contained; no stuck pending entry or live exposure after read-only probes | [daily/2026-05-31.md#cluster-cl-025-post-ae4bd9c-passive-close-maker-leg-live-flat-precheck](../daily/2026-05-31.md#cluster-cl-025-post-ae4bd9c-passive-close-maker-leg-live-flat-precheck) |
| 2026-06-04 | `SEIUSDT` Bybit maker / Hyperliquid hedge | `1e082d9` | local RED/GREEN and cloud focused tests verify Hyperliquid insufficient margin now blocks initial entry and pending hedge retry; final cloud diagnose is flat/no-open-orders | [daily/2026-06-04.md#cluster-cl-049-post-cl048-seiusdt-open-maker-order-terminality](../daily/2026-06-04.md#cluster-cl-049-post-cl048-seiusdt-open-maker-order-terminality) |
| 2026-06-08 | issue 3-7 admission / pending hedge closure review | `89e2b93` | deployed/cloud verified | [daily/2026-06-08.md#cluster-cl-052-production-issues-3-11-root-closure-evidence-hardening](../daily/2026-06-08.md#cluster-cl-052-production-issues-3-11-root-closure-evidence-hardening) |
| 2026-06-09 | `CLUSDT` OKX maker / Bybit hedge, Bybit `110125` crude-oil terms | working tree | local precheck regression added; deploy pending | Bybit endpoint/signature was healthy in the same window; failure is symbol trading-terms admission, now blocked before maker dispatch. |
| 2026-06-19 | `HUSDT` Binance maker / Aster hedge, Aster headroom exhausted | `039d52c` | deployed RED/GREEN covers pre-submit block and post-fill fee-drag guard; current cloud recovered | [daily/2026-06-19.md#cluster-cl-099---aster-headroom-pre-submit-single-leg-fee-drag-guard-and-active-handoff-quality](../daily/2026-06-19.md#cluster-cl-099---aster-headroom-pre-submit-single-leg-fee-drag-guard-and-active-handoff-quality) |
| 2026-06-21 | `ESPORTSUSDT` unopened Aster admission-blocked quantity plan plus terminal-flat quantity warning residues | `cfd0644`, deployed baseline `9dee9f3`, terminal quantity-warning residual fixed locally | cloud baseline flat/no-open-orders and no exact `-5022`/`-2022`; residual RED/GREEN keeps unproven active mismatches warning while terminal/unopened/repair/clean-truth quantity adjustments resolve | [daily/2026-06-21.md#cluster-cl-105---latest-deploy-2-7-root-fix-semantic-closure](../daily/2026-06-21.md#cluster-cl-105---latest-deploy-2-7-root-fix-semantic-closure) |
| 2026-06-23 | Aster max-notional admission diagnostics | working tree | local green; deploy pending | [daily/2026-06-23.md#cluster-cl-110---aster-admission-close-artifact-noise-lifecycle-and-stale-quote-diagnostics](../daily/2026-06-23.md#cluster-cl-110---aster-admission-close-artifact-noise-lifecycle-and-stale-quote-diagnostics) |
| 2026-06-24 | Gate contract-size quantity drift after Gate/Bitget candidate recovery | `4ddbd07` | deployed/cloud verified; exchange flat/no-open-orders; no post-deploy Gate size reject recurrence | [daily/2026-06-24.md#cluster-cl-116---gate-contract-size-quantity-drift-and-bitget-duplicate-clientoid-truth-closure](../daily/2026-06-24.md#cluster-cl-116---gate-contract-size-quantity-drift-and-bitget-duplicate-clientoid-truth-closure) |

## Regression Harness

- `tests/live_harness/test_exchange_admission_incidents.py`
- `tests/test_live_startup_preflight.py`
- `tests/test_venues_transport.py -k 'aster or bybit_110126 or bybit_trading_terms or bybit_order_admission_precheck or max_notional'`
- `tests/test_pending_entry_v1_semantic_drift.py`

## Next Recurrence Checklist

1. Count `order.submit_result` rejected events by venue, symbol, and `response_classification`.
2. For Bybit terms, check whether `order.precheck_result` and
   `runtime.entry_admission_blocked source=pre_entry_bybit_precheck` appear
   before any maker `order.submit_attempt`.
3. If the reject was only discovered after maker fill, check whether `pending_entry.hedge_admission_blocked` appears after the hedge reject.
4. Check `runtime.entry_admission_blocked` and `state.venue_entry_cooldowns`.
5. For Aster `-5018`, first check whether `runtime.entry_admission_blocked`
   appeared before HTTP order submit with
   `reason=max_notional_admission_blocked`,
   `source=aster_headroom_precheck`, and
   `cooldown_scope=symbol_and_venue`. If the exchange returned `-5018` first,
   check fallback source `exchange_5018_fallback`; it must not retry with a
   shrunken one-sided quantity.
6. If a maker fill happened before a deterministic hedge admission block,
   confirm cleanup emits single-leg recovery evidence and immediately arms hard
   cooldown for the route. Then check `business_progression_quality_summary`
   for `cleanup_after_admission_block` and
   `repeated_single_leg_guarded`.
7. For Hyperliquid insufficient margin, check the symbol and venue cooldown events carry `source`, `block_scope`, `blocked_until_ms`, pair id, official doc URL, and `evidence_gap=false`.
8. If `hyperliquid:*` venue cooldown is active, confirm
   `runtime.entry_admission_venue_degraded` prunes Hyperliquid candidates before
   shortlist/Local-L2 tracking while close/cancel/recovery truth still runs.
9. Run `scripts/diagnose_live.py --json --symbol <symbol> --venues <maker,hedge> --since-deploy`.
10. For quote stale recurrences, confirm terminal stale cooldown exists and
    `quote_rewarm_terminalized_count` / `active_stuck_count` distinguish
    terminalized stale work from active stuck work.
11. For sizing recurrences, inspect `execution.entry_quantity_plan` before
    pending-entry residual evidence. `quantity_contract_status` should explain
    whether the maker quantity was already hedgeable, adjusted to a hedgeable
    quantity, or blocked.
12. Closure requires cloud harness plus high-confidence exchange truth flat/no-open-orders.
13. If a quantity mismatch appears for an entry that never opened, inspect
    admission-block/abort evidence before treating it as a current quantity
    warning.
