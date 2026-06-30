# Bug Card: Passive Close Terminal Flatness

Purpose: keep the reusable memory for passive close terminal states, under-min
residuals, and live-flat cleanup.

## Stable Fingerprints

- `exit.passive_close_fallback_terminal_flat`
- `exit.passive_close_terminal_zero_qty_reduce_only_evidence`
- `runtime.passive_close_deadline_fallback_armed`
- `execution.hedge_deadline_breached`
- `execution.close_deadline_breached`
- `exit.passive_close_hedge_deadline_fail_closed`
- `pending_passive_close_flat_probe`
- `exit.passive_close_waiting_exchange_flat_truth`
- `exit.passive_close_live_one_sided_flatten`
- `passive_close_final_truth_actions.flatten_remaining_live_leg`
- `passive_close_actionable_single_leg_wait_count`
- `risk_only_live_single_leg_exposure_count`
- `runtime.passive_close_recovery_result`
- `runtime.stale_fail_closed_cleared`
- `passive_close_resolved_without_terminal_truth_count`
- `close_reconciliation_evidence_gap_count`
- `execution.dual_taker_armed` with `execution_kind=exit`
- `exit.passive_close_post_only_maker_rotated`
- Binance close `-5022 GTX_ORDER_REJECT` post-only maker reject
- Binance close `-2022 ReduceOnly Order is rejected`
- Bitget close `40786 Duplicate clientOid`
- Bybit close `110017 current position is zero, cannot fix reduce-only order qty`
- `exit.reconciled` with `evidence_gap_reason`,
  `statement_probe_status`, and `trade_probe_status`
- `price_unavailable_for_min_notional`
- `passive_close_maker_filled_under_chunk`
- `entry.cleanup_leg_exposure`
- `runtime.passive_close_tick_error` with
  `"'dict' object has no attribute 'append'"`
- Recurrence shape: local pending passive close/open state keeps retrying while exchange truth is already flat, while terminal maker/under-min branches need V1 compensation, or while Bybit reduce-only close admission reports `110017/orderQty will be truncated to zero`.

## Current Effective Rule

Terminal reduce-only, already-flat, under-min, price-unavailable, or
maker+hedge-fill-complete close branches can clear only after live exchange
truth proves both legs flat and both venues have no live open orders. If live
truth shows residual exposure and the residual is tradeable, route through
V1-style compensation/flattening. If truth is incomplete or cleanup cannot
prove flat, retain/fail-closed with structured evidence.

`risk_only` and `fail_closed` are not business terminal states. They block new
risk and preserve evidence while cleanup is still incomplete. For an owned
passive close, the only clean terminal state is exchange position flat plus no
open orders, followed by removal of the local pending close/open-position owner.
When final exchange truth is trusted, open orders are flat, and exactly one live
leg remains, `passive_close_final_truth_contract` must return
`flatten_remaining_live_leg`; runtime must execute the owned one-sided
reduce-only IOC flatten path and then prove fresh exchange flat/no-open-orders
before clearing local state. Errors stay as historical evidence after cleanup;
they do not replace cleanup.

After the last owned passive-close/open-position record is removed behind
fresh exchange flat/no-open-orders truth, stale automatic `fail_closed` must be
cleared through `clear_stale_fail_closed_if_recovery_clean(...)`. Operator
fail-closed remains authoritative, and any remaining open/pending/unpaired
recovery work still blocks the clear. Current risk-only exposure counters must
come from current unpaired recovery records, not from historical since-deploy
events that have already reached terminal flat.

Local maker+hedge fill equality is not live terminal proof in production mode.
If passive close execution is locally complete but exchange position/open-order
truth has not jointly proven flat, retain `pending_passive_close`, keep the
close owner, emit `exit.passive_close_waiting_exchange_flat_truth`, and let
passive close maintenance probe again. Paper/backtest may keep local execution
completion as terminal because no live exchange truth exists.

Bybit reduce-only close `110017/orderQty will be truncated to zero` is terminal
zero-qty evidence for that reduce-only request, not a confirmed close fill and
not an ordinary maker retry. It must immediately route to live-truth closure:
maker-flat plus other-leg-live goes to one-sided flatten, both-flat goes to
flat cleanup, and untrusted truth retains pending close.

If Bybit `110017` or a passive-close terminal/no-fill branch occurs while live
position truth is still nonzero and open-order truth shows a matching
reduce-only close order, the order is covered by existing exchange work rather
than terminal-flat. Adopt the order by order id/client id when available, or by
venue+symbol+side+reduce-only+quantity coverage when that evidence is
consistent, then continue passive maintenance. Do not submit a duplicate
reduce-only maker or taker-flatten while the owned close order is live. If the
open order cannot be proven as a close owner, fail closed and block new risk.

Duplicate client-order ids are idempotency conflicts, not terminal success by
themselves. Bybit `110072` and Bitget `40786 Duplicate clientOid` must query
order/fill truth by client id and current live position truth. They may clear
only when reconciliation proves filled/flat or current exchange truth is flat;
truth gaps remain fail-closed.

Passive close retry/backoff is also bounded by the V1 exit hedge/fallback hard
deadline. Once the deadline is hard-breached, V2 must stop passive retry
backoff, enter fail-closed, compensate any unhedged gap when possible, and
probe live flat truth before clearing local state.

The pending-close reconciliation queue is part of the terminal-flat contract.
It must be normalized before passive-close cleanup emits terminal lifecycle or
drift-correction events. A cleanup path must not journal terminal-flat evidence
and then throw before `open_positions` / `pending_passive_closes` are removed
or before the V1 recovery decision core sees the clear evidence.
The same normalized queue boundary applies to pending-close reconciliation
processing, supervisor venue coverage, and entry-conflict gating.

Diagnostics must separate current risk from recovered process quality. An
`exit.passive_close_resolved` payload without explicit exchange position-flat
and open-order-flat proof counts as
`passive_close_resolved_without_terminal_truth_count`. If a later production
gate is green with exchange flat/no-open-orders and no V1 lifecycle blockers,
the artifact is a recovered process issue, not current `active_stuck_count`.

Close reconciliation evidence gaps are process-quality issues, not terminal
truth. `exit.reconciled evidence_gap=true` must explain the missing proof via
`evidence_gap_reason`, `statement_probe_status`, and per-side
`trade_probe_status`. Gate green remains driven by exchange position truth plus
open-order truth; missing statement/trade evidence is retained as historical
process evidence so future reviews do not confuse "currently safe" with "the
close path had perfect evidence".
`close_reconciliation_evidence_contract(...)` is the shared classifier:
exchange flat plus no open orders produces `terminal_flat_accounting_gap`;
unclean or unavailable exchange truth produces
`unresolved_close_accounting_gap`. Binance `userTrades` and Bybit
`/v5/execution/list` are accounting evidence APIs, not substitutes for live
position/open-order truth. Statement query windows, ordering, or multi-fill
gaps must not re-open a cleared business owner or create `risk_only` /
`fail_closed` after terminal exchange truth.

For new recurrences, start from `lightfee/engine/business_contract.py` before
adding another passive-close or diagnose-local terminality predicate.

Diagnostic visibility is part of the same terminal-truth contract. Resolved
close/order artifacts such as Binance close `-2022`, Binance close post-only
`-5022`, Bybit duplicate `110072`, ACK-only, and zero-fill may be
`historical_terminal_evidence` only after current exchange truth proves
flat/no-open-orders. Reduce-only and zero-fill artifacts also need a strong
order/client identity match to terminal close evidence before diagnostics may
downgrade them; a same-position terminal event without order identity is not
enough. The explicit exception is Bybit `110017 current position is zero` /
`cannot fix reduce-only order qty`: this is a no-order terminal reject and may
resolve by position terminal truth only when current exchange truth is
flat/no-open-orders. If that truth is unavailable, dirty, or identity cannot be
matched for all other reduce-only artifacts, the artifact stays a current
blocker. Quote stale and quote rewarm terminal stale are
`current_admission_blocker` records, not current exposure. Unsupported-symbol
position/open-order probe records from venue catalog scope are
`catalog_diagnostic`; they stay visible for audit but do not block the gate.

For Binance post-only close maker `-5022`, run the BBO guard before submit.
If the high-slippage chunk has zero maker/hedge fill progress, V2 may rotate to
the opposite maker leg and reset the zero-fill containers. If the chunk already
has any maker or hedge fill progress, do not rotate maker legs; preserve the
fill ledger and arm `DUAL_TAKER` for the remaining quantity.

For Binance reduce-only close `-2022`, first check current open orders. A
matching reduce-only close order that covers the quantity should be adopted as
the passive-close owner. If the position is already flat, terminal-flat handling
still requires exchange position truth and no-open-order truth; the error code
alone is not success evidence.

## V1 / Exchange Semantics

- V1 lets live exchange truth dominate stale recovered local close state.
- V1 terminal-maker and under-min branches do not spin forever; they either prove flat, compensate, or fail-closed.
- Unsupported or failed open-order truth is not flat evidence.
- Exchange min-notional / reduce-only terminal rejects must be interpreted with live position truth, not used blindly as success.
- Missing close price evidence must explain whether Local-L2 was stale, WS BBO
  fallback was stale, or WS BBO fallback had no cache/quote/budget. A bare
  `price_hint=0.0` is not enough to choose a semantic fix.
- An accepted taker hedge ACK without fill confirmation is not a terminal close
  fill. It must carry order-truth probe paths and stay in reconcile/live-truth
  flow before passive close can advance or clear.
- For Aster and any venue that returns an accepted close order identity with
  zero immediate fill, the ACK is accepted-order truth work, not ordinary
  `zero_fill`. Register `accepted_order_truth_gap`, keep the passive-close
  owner, and resolve only through confirmed fill or live-flat truth that carries
  the accepted order/client identity. Later Aster `-2022 ReduceOnly Order is
  rejected` is historical terminal evidence only when that identity chain
  matches; a same-position terminal flat event without order identity is still
  unresolved.
- OKX amend `50115 Invalid request type` from the amend-order endpoint is an
  amend-path capability failure for that call. Route through cancel-replace and
  keep the existing double-order guard.

## Attempts Ledger

| Date | Attempt | Status | Why |
|---|---|---|---|
| 2026-05-25 | Live-flat sweep for recovered passive closes | effective | UBUSDT/XCNUSDT style stale recovered closes clear only after live flat proof. |
| 2026-05-28 | Conservative open-order truth + DUAL_TAKER fallback | effective | OPGUSDT maker-under-chunk path closes through real flat clearing harness. |
| 2026-05-29 | Terminal-maker price-unavailable compensation | effective | JCTUSDT terminal maker branch no longer loops; cloud probe flat/no-open-orders. |
| 2026-05-30 | Post-`bbcd7b9` RAVE/NOM/ORCA flatness watch plus post-fix POWER/HOME closes | deployed/probe verified | Current probes are flat. POWER/HOME opened and auto-closed after `0fd9a74`; POWER had duplicate/reduce-only already-flat noise after live-matched close, then terminal-flat recovery cleared. |
| 2026-05-31 | Live-truth precheck before first passive maker submit | fixed, deployed, cloud verified | ID/HOME reproduced Bybit `110017` because the chosen short maker leg was already live-flat before first maker submit. V2 now probes both legs first and routes maker-flat truth to existing one-sided flatten or both-flat recovery. Cloud `70a1a8c` targeted HOME/ID probes are flat/no-open-orders. |
| 2026-06-03 | WS BBO close fallback missing-quote evidence | local green, deploy pending | Post-`e087513` had two passive close missing-price events after current state recovered flat. Runtime now emits `runtime.close_price_evidence_missing` for active WS BBO fallback missing cache/quote/budget branches without changing fail-closed behavior. |
| 2026-06-04 | V1 passive close hedge/fallback hard deadline | local full gate green, cloud deploy pending | Bybit abnormal close samples exposed passive close retry/fallback paths that could continue after the V1 deadline. V2 now arms overdue passive closes into DUAL_TAKER, converts hedge/fallback hard breaches into fail-closed plus compensation/live-flat probing, and avoids same-cycle maker submit after live truth has already driven one-sided flatten. |
| 2026-06-07 | Post-`bff33ec` live-flat cleanup re-entry | local `PC-06`/`PC-07`/`PC-08` green, deploy pending | `BABYUSDT` exchange truth is flat while local open/passive state remains. The local fix restores the canonical pending-close reconciliation queue boundary, keeps cleanup success journals behind queue/state/core clear, and hardens malformed queue consumers. |
| 2026-06-08 | ACK-only hedge evidence and OKX amend fallback | fixed, deployed/cloud verified | Passive-close hedge errors now share pending-entry order-truth-gap evidence (`order_ack_only`, accepted ids, missing fill fields, probe paths, next action, reconciliation result). OKX amend endpoint HTTP 405 / code `50115` / `Invalid request type` now routes to existing cancel-replace without changing signing, CID, sizing, or close execution. |
| 2026-06-08 | Pending-entry passive timeout ACK truth-gap evidence | fixed, deployed/cloud verified | CL-052 adds order id/client id, venue, `cancel_ack_terminal=false`, `truth_required_by=pending_entry_passive_reconciliation`, and `next_truth_probe=query_passive_order_progress` to passive maker rest-timeout cancel evidence. This does not change cancel behavior; it preserves the ACK-not-terminal review trail. |
| 2026-06-08 | Passive close terminality proof gate | fixed, deployed/cloud verified | Maker progress query timeout now keeps zero-fill cycle state; one-sided ACK-only flatten now registers `accepted_order_truth_gap` and waits for exchange truth before terminal lifecycle; clear path now passes full exchange positions/open-orders truth to recovery core. Merged as `74475c5` and included in the latest deployed main line. |
| 2026-06-10 | MOVEUSDT ACK-only duplicate-client diagnose closure | fixed locally, deploy pending | CL-066 closes the deployed MOVEUSDT evidence-consumption gap where Bybit ACK-only accepted ids plus same-client-id `110072` stayed in active diagnose errors after reconciliation/terminal/current exchange truth proved flat. Diagnose now treats ACK-only `order.uncertain` as implicit truth-gap registration when accepted ids and no-fill evidence are present, binds nested request identities, and resolves matching duplicate-client artifacts only behind flat/no-open-orders truth. Close execution now records duplicate-client live-flat as `exit.close_duplicate_client_order_resolved_live_flat`, not zero-quantity `order.filled`. |
| 2026-06-15 | Bybit 110017 submit-time terminal zero-qty evidence | fixed, deployed/cloud verified | HOMEUSDT closed flat/no-open-orders, but Bybit returned `110017 orderQty will be truncated to zero` after passive maker submit rather than before submit precheck. CL-083 preserves raw Bybit retCode body, emits terminal-zero evidence, immediately reuses V1 live-truth closure, and keeps diagnose from treating resolved terminal-zero cases as unresolved order errors. Cloud verification under `fd1579d` is flat/no-open-orders with no active order errors and no unmapped lifecycle events. |
| 2026-06-19 | Existing reduce-only close order covers quantity | deployed in `106f47e`; waiting-event evidence payload is local to the event | GENIUSUSDT-style close drift is not terminal-flat when Bybit still has nonzero position plus a matching reduce-only close order. CL-100 adopts the existing close order into pending passive close state, adds journal-based close-owner recovery, and keeps diagnose/gate red until exchange truth is flat/no-open-orders or owned close work is resolved. Waiting passive-close evidence now carries local `exchange_truth_attempt` payload instead of requiring a join with sibling diagnostics. |
| 2026-06-19 | Passive close resolved before terminal exchange truth | deployed in `106f47e`; cloud gate green; non-blocking diagnose mapping follow-up remains | ESPORTSUSDT-style truth lag showed current production could recover flat/no-open-orders while diagnostics still counted old passive-close artifacts as active stuck. CL-101 keeps live pending passive close owner in `waiting_exchange_flat_truth` until position/open-order truth jointly proves flat, maps that owner as `owned_pending_passive_close`, moves gate-green historical hard-over-budget artifacts into recovered process counters, and records `exchange_truth_attempt` directly on every waiting event, including missing-position-snapshot cases. Post-deploy `106f47e` cloud truth is flat/no-open-orders and gate green, while `diagnose_live.py --since-deploy` still exposes `runtime.passive_close_recovery_result` as a non-blocking V1 lifecycle mapping follow-up. |
| 2026-06-19 | Reconciliation evidence gap and close-error terminal truth classification | `8eadb8e` deployed as clean baseline; unified-contract follow-up local, deploy pending | CL-102 centralizes terminal-truth and evidence-gap vocabulary in `lightfee/engine/business_contract.py`. `exit.reconciled` now classifies missing long/short trade-statement evidence with `evidence_gap_reason`, `statement_probe_status`, and `trade_probe_status`; diagnose counts it as `close_reconciliation_evidence_gap_count` without changing the exchange-truth gate. SAHARAUSDT-style `-2022` reduce-only close errors resolve only through `close_order_error_resolution_contract` when current exchange truth is flat and has no open orders, with matching terminal position/order evidence. The contract must read nested `exchange_error`/`request_context` journal payloads and must not treat a bare `-2022` as resolved without reduce-only close context. |
| 2026-06-20 | Owned passive-close single-leg final truth cleanup | runtime fix `fb1c3a0` deployed; cloud gate green | CL-103 makes `passive_close_final_truth_contract` the final truth action source. Trusted one-sided residual plus no open orders now routes to owned `exit.passive_close_live_one_sided_flatten` instead of repeating `retry_exchange_position_open_order_truth`; untrusted truth and unknown/open orders still retain/fail closed. Follow-up after first deploy showed actual exchange truth/local owners cleared, but stale `risk_mode=fail_closed`, unmapped `runtime.passive_close_recovery_result`, and historical risk-only event counting still made diagnose non-green. The same contract now clears stale automatic fail-closed after terminal owner cleanup, maps the passive-close recovery result through `classify_business_event_kind`, and reports `risk_only_live_single_leg_exposure_count` from current recovery records only. Cloud verification after `fb1c3a0` showed `risk_mode=running`, `lifecycle=running`, no local open/pending work, exchange flat/no-open-orders, `gate_passed=true`, no unmapped lifecycle events, and no current risk-only single-leg exposure. |
| 2026-06-20 | Reconciliation statement gap cannot override terminal flat truth | fixed in `447218a`, deployed/cloud verified | CL-104 adds `close_reconciliation_evidence_contract(...)` and root `close_reconciliation_evidence_gap_summary`. HUSDT-style `missing_short_close_trade_statement` is `terminal_flat_accounting_gap` when current exchange truth is flat/no-open-orders; it stays visible as accounting evidence but cannot create current exposure, `risk_only`, or `fail_closed`. If exchange truth is not clean, the same gap is `unresolved_close_accounting_gap` and remains a blocker. Cloud `447218a` showed `close_reconciliation_evidence_gap_summary.blocking_count=0`, exchange flat/no-open-orders, no pending close, and `risk_mode=running`. |
| 2026-06-20 | Resolved close artifacts and terminal unpaired cleanup become historical diagnostic evidence | fixed in `091ce2c`, deployed/cloud verified | `classify_noise_visibility(...)` and root `diagnostic_noise_summary` separate resolved ACK-only / duplicate-client / reduce-only / post-only close artifacts from current blockers after exchange flat + no open orders. Final review also fixed historical `recovery.unpaired_live_position_cleanup_skipped` events so they become `historical_terminal_evidence` when current exchange truth is flat/no-open-orders and current recovery exposure count is zero. `short_window_warning_details.passive_close_truth_gap` now counts only unresolved/current truth gaps. Current single-leg exposure, ownerless open orders, untrusted truth, and unresolved reconciliation gaps remain blocking and are not downgraded. Cloud `091ce2c` showed `diagnostic_noise_summary.current_blocker_count=0`, exchange flat/no-open-orders, and no local open/pending work. |
| 2026-06-20 | Compensation already-flat lifecycle mapping | local green, deploy pending | Post-deploy strict review found `exit.compensation_already_flat` could remain a V1 lifecycle unmapped tail even though it means the passive-close compensation branch already has flat truth. The follow-up maps it through `classify_business_event_kind(...)` to V1 `PASSIVE_CLOSE`, `terminal_flat_already_proven`, action `record_compensation_terminal_flat`, so terminal-flat evidence is preserved without creating current risk or suppressing true non-flat blockers. |
| 2026-06-21 | Binance close 5022/2022 and exit dual-taker semantic split | `9dee9f3` deployed/cloud verified; terminal quantity-warning residual fixed locally, deploy pending | CL-105 adds BBO guard + post-submit 5022 classification for passive close. Zero-progress high-slippage chunks may rotate to the opposite maker leg; partial-progress chunks preserve fill accounting and arm `DUAL_TAKER`. Binance `-2022` close rejects now adopt an existing matching reduce-only close order before retry/fail-closed. Passive-close `execution.dual_taker_armed` carries `execution_kind=exit` and maps to V1 `PASSIVE_CLOSE`, while entry fallback remains `PENDING_ENTRY`. Cloud baseline `9dee9f3` verified flat/no-open-orders, services green, `gate_passed=true`, and no exact `-5022`/`-2022` recurrence. |
| 2026-06-21 | Terminal-zero truth-probe retain-pending split from maker submit errors | local green, deploy pending | Latest cloud showed one `exit.passive_close_maker_submit_error` whose payload was not an exchange submit failure: `reason=terminal_zero_qty_live_truth_not_flat`, `decision=retain_pending`, followed by terminal close resolution and clean exchange truth. Diagnose now splits this shape into `truth_probe_retain_pending_*` terminal-zero summary fields and filters it from `order_error_evidence` only after same-position terminal evidence; unproven maker submit errors remain active. |
| 2026-06-23 | Close artifact diagnostic downgrade requires trusted identity | local green, deploy pending | CL-110 tightens Binance/Aster `-2022` and zero-fill diagnostic downgrade: clean exchange truth plus same-position terminal flat is not enough without same-order/client identity. `diagnostic_noise_summary` now trusts only resolved truth-gap, terminal-zero-qty, or close-terminal summaries; unmatched artifacts remain `current_blocker/unresolved_close_artifact`. `reconciliation.pending_close_exchange_truth_refreshed` maps to V1 `PASSIVE_CLOSE`. |
| 2026-06-28 | ACT recovered funding close lifecycle | fixed in `904bb23`, deployed/cloud verified | CL-131 restores V1 close semantics for recovered live positions: startup recovery must hydrate funding timestamps and maker-leg hints from owner/pending journal or quote truth before creating a managed open position, and missing funding truth emits `recovery.recovered_position_funding_timestamp_missing` instead of a silent never-close state. Recovery ownership now canonicalizes OKX raw symbols such as `ACT-USDT-SWAP` to `ACTUSDT` across ledger, owner index, unpaired recovery, and V1 closure. Cloud ACT closed through normal passive/fallback close; final truth is running, exchange flat/no-open-orders, no pending close, and V1 `RUNNING_CLEAN`. |
| 2026-06-29 | Aster accepted close ACK truth-gap identity closure | fixed in `617adb8`, deployed/cloud verified | CL-133 turns accepted 0-fill Aster close ACKs into `accepted_order_truth_gap` work rather than ordinary `zero_fill`; diagnose now treats `exit.accepted_order_truth_gap_resolved` as terminal only for `filled/live_flat` and requires matching order/client identity before downgrading Aster `-2022` reduce-only artifacts. Cloud verification showed running/flat/no-open-orders/no pending truth gaps and V1 `RUNNING_CLEAN`. |
| 2026-06-29 | LABUSDT accepted truth-gap to single-leg cleanup handoff | local verified; deploy pending | CL-136 keeps exit-shadow accounting at one strategy sample group per normal close trigger and separates passive-close retry counts in diagnose. Existing accepted-order truth gaps no longer block live one-sided cleanup when follow-up truth proves open orders empty and live exposure still exists; runtime emits `exit.accepted_order_truth_gap_cleanup_handoff`, submits reduce-only IOC cleanup, then removes the old gap only after fresh live-flat/no-open-orders proof and emits `exit.accepted_order_truth_gap_resolved(live_flat_after_single_leg_cleanup)`. |
| 2026-06-30 | POWRUSDT post-maker-fill live-zero hedge closure | local verified; deploy pending | CL-137 adds a terminal-maker-fill live-position/open-order precheck before hedge catch-up. Both legs already flat clears through live-flat proof instead of submitting a Bybit reduce-only order that would return `110017 current position is zero`; dirty open-order truth retains. Diagnose/business-contract close only this Bybit no-order zero-position reject by position terminal truth when current exchange truth is clean. |
| 2026-06-30 | LABUSDT Aster direct reduce-only no-order reject diagnosis | local verified; deploy pending | CL-141 keeps CL-110 identity tightening for generic Binance/Aster `-2022`, but adds the narrower Aster V3 direct reject shape: reduce-only, non-post-only request, request client/order identity, same-position terminal flat, and current flat/no-open-orders truth. That no-order close artifact can resolve by position terminal truth because no accepted order exists to match; missing identity or dirty truth remains a current blocker. |
| 2026-06-30 | one-sided cleanup pre-submit live truth refresh | local verified; deploy pending | CL-142 reduces avoidable Aster/Bybit already-flat reduce-only artifacts at the source: one-sided passive-close cleanup refreshes live position after open-order proof and before IOC construction. If latest truth is flat it skips IOC only through full live-flat/open-order terminal proof; if quantity/side changed it rebuilds the request from latest truth; unavailable truth retains fail-closed. |

## Recurrences

| Date | Symbols / Venues | Commit / Fix | Result | Detail |
|---|---|---|---|---|
| 2026-05-25 | `UBUSDT`, `XCNUSDT` | `6845d9b` family | closed | [daily/2026-05-25.md](../daily/2026-05-25.md) |
| 2026-05-28 | `OPGUSDT` | local CL-014 | closed by harness | [daily/2026-05-28.md#cluster-cl-014-opgusdt-passive-close-stuck-under-chunk-live-flatness-and-precision](../daily/2026-05-28.md#cluster-cl-014-opgusdt-passive-close-stuck-under-chunk-live-flatness-and-precision) |
| 2026-05-29 | `JCTUSDT` | `9cdb9df` | closed by cloud probe | [daily/2026-05-29.md#cluster-cl-015-jctusdt-partiusdt-v1-terminality-regression](../daily/2026-05-29.md#cluster-cl-015-jctusdt-partiusdt-v1-terminality-regression) |
| 2026-05-30 | `RAVEUSDT`, `NOMUSDT`, `ORCAUSDT`, `HOMEUSDT`, `POWERUSDT` | `0fd9a74` | final targeted probes flat/no-open-orders; no pending passive close remains | [daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission](../daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission) |
| 2026-05-31 | `IDUSDT`, `HOMEUSDT` Binance/Bybit | `70a1a8c` | deployed; post-restart window has no opens or maker-submit errors; targeted probes flat/no-open-orders | [daily/2026-05-31.md#cluster-cl-025-post-ae4bd9c-passive-close-maker-leg-live-flat-precheck](../daily/2026-05-31.md#cluster-cl-025-post-ae4bd9c-passive-close-maker-leg-live-flat-precheck) |
| 2026-06-03 | `STEEMUSDT`, `TRIAUSDT` | working tree | local green, deploy pending | [daily/2026-06-03.md#cluster-cl-035-post-e087513-long-window-follow-up](../daily/2026-06-03.md#cluster-cl-035-post-e087513-long-window-follow-up) |
| 2026-06-04 | `MEUUSDT`, `LDOUSDT`, `SEIUSDT`, `ICPUSDT`, `BSBUSDT` Bybit-related close samples | main push in this session | full pytest `3432 passed`, `9 skipped`, `1 warning`; cloud deploy pending | [daily/2026-06-04.md#cluster-cl-046-bybit-entry-passive-close-v1-deadline-loop](../daily/2026-06-04.md#cluster-cl-046-bybit-entry-passive-close-v1-deadline-loop) |
| 2026-06-07 | `BABYUSDT` OKX/Bybit plus unowned Bybit `MORPHOUSDT`, `MONUSDT`, `SEIUSDT` live artifacts | working tree | local RED/GREEN coverage complete for `PC-06`/`PC-07`/`PC-08`; deploy and production verification pending | [daily/2026-06-07.md#cluster-cl-051-post-bff33ec-passive-close-live-flat-cleanup-re-entry](../daily/2026-06-07.md#cluster-cl-051-post-bff33ec-passive-close-live-flat-cleanup-re-entry) |
| 2026-06-08 | ACK-only timeout/order-truth evidence for production issue 10 | `89e2b93` / `74475c5` | deployed/cloud verified | [daily/2026-06-08.md#cluster-cl-052-production-issues-3-11-root-closure-evidence-hardening](../daily/2026-06-08.md#cluster-cl-052-production-issues-3-11-root-closure-evidence-hardening) |
| 2026-06-15 | `HOMEUSDT` OKX/Bybit | `2eb14b7`, verified again under `fd1579d` | closed: local RED/GREEN targeted regressions passed; cloud manifest, singleton, production verifier, and since-deploy diagnose passed with flat/no-open-orders truth | [daily/2026-06-15.md#cluster-cl-083-bybit-110017-submit-time-terminal-zero-qty-close-drift](../daily/2026-06-15.md#cluster-cl-083-bybit-110017-submit-time-terminal-zero-qty-close-drift) |
| 2026-06-19 | `GENIUSUSDT` Bybit reduce-only close owner | `106f47e` | deployed terminal-truth gates pass; waiting-event evidence payload now local to the event | [daily/2026-06-19.md#cluster-cl-100---openclose-lifecycle-owner-truth-and-reduce-only-adoption](../daily/2026-06-19.md#cluster-cl-100---openclose-lifecycle-owner-truth-and-reduce-only-adoption) |
| 2026-06-21 | `HOMEUSDT`, `ESPORTSUSDT`, Binance close `-5022`/`-2022` families | `cfd0644`, deployed baseline `9dee9f3`, terminal quantity-warning residual fixed locally | cloud baseline flat/no-open-orders, no exact `-5022`/`-2022`, and passive close artifacts resolved; residual root fix closes min-notional lifecycle and terminal/unopened quantity warning diagnostics | [daily/2026-06-21.md#cluster-cl-105---latest-deploy-2-7-root-fix-semantic-closure](../daily/2026-06-21.md#cluster-cl-105---latest-deploy-2-7-root-fix-semantic-closure) |
| 2026-06-23 | Binance/Aster reduce-only diagnostic noise and pending-close exchange-truth refresh mapping | working tree | local green; deploy pending | [daily/2026-06-23.md#cluster-cl-110---aster-admission-close-artifact-noise-lifecycle-and-stale-quote-diagnostics](../daily/2026-06-23.md#cluster-cl-110---aster-admission-close-artifact-noise-lifecycle-and-stale-quote-diagnostics) |
| 2026-06-24 | Bitget `40786 Duplicate clientOid` close/recovery truth closure | `4ddbd07` | deployed/cloud verified; duplicate-client truth closure no longer leaves unresolved close residue when exchange truth is clean | [daily/2026-06-24.md#cluster-cl-116---gate-contract-size-quantity-drift-and-bitget-duplicate-clientoid-truth-closure](../daily/2026-06-24.md#cluster-cl-116---gate-contract-size-quantity-drift-and-bitget-duplicate-clientoid-truth-closure) |
| 2026-06-29 | `LABUSDT` Aster accepted close ACK / Aster `-2022` reduce-only artifact family | `617adb8` | deployed/cloud verified; gate passed, exchange flat/no-open-orders, unresolved order-truth gap 0, order error evidence 0 | [daily/2026-06-29.md#cluster-cl-133---aster-accepted-close-ack-truth-gap-and-reduce-only-identity-closure](../daily/2026-06-29.md#cluster-cl-133---aster-accepted-close-ack-truth-gap-and-reduce-only-identity-closure) |
| 2026-06-29 | `LABUSDT` Bitget live long / OKX flat accepted close truth-gap retry loop | working tree | local verified; exit-shadow expected count stays one normal close trigger, existing truth gap hands off to reduce-only IOC cleanup only with open-orders-empty + live one-sided proof | [daily/2026-06-29.md#cluster-cl-136---labusdt-exit-shadow-accounting-and-single-leg-cleanup-handoff](../daily/2026-06-29.md#cluster-cl-136---labusdt-exit-shadow-accounting-and-single-leg-cleanup-handoff) |
| 2026-06-30 | `POWRUSDT` Binance maker filled / Bybit hedge leg already flat | working tree | local verified; terminal maker fill probes live flat before hedge catch-up, and Bybit `110017 current position is zero` is closed as historical terminal evidence only behind clean exchange truth plus position terminal proof | [daily/2026-06-30.md#cluster-cl-137---powrusdt-post-maker-fill-live-zero-hedge-closure](../daily/2026-06-30.md#cluster-cl-137---powrusdt-post-maker-fill-live-zero-hedge-closure) |

## Regression Harness

- `tests/test_passive_close.py`
- `tests/test_exit_decisions.py::TestPassiveCloseFallbackDue`
- `tests/test_runtime_entry_flow.py::TestPlannerDispatchIntegration::test_pending_passive_close_overdue_arms_dual_taker_despite_future_retry`
- `tests/test_passive_close.py::TestPassiveCloseMakerLegLiveTruthPrecheck`
- `tests/live_harness/test_opgusdt_passive_close_stuck_incident.py`
- `tests/live_harness/test_20260529_jct_parti_regressions.py`
- `tests/live_harness/test_historical_passive_close_incidents.py`
- `tests/persistence/test_v1_state_snapshot_semantics.py -k "pending_close_reconciliation"`
- `tests/test_passive_close.py -k "live_flat_cleanup"`
- `tests/test_pending_entry_v1_semantic_drift.py tests/test_supervisor_execution.py -k "pending_close_reconciliation"`

## Next Recurrence Checklist

1. Inspect `pending_passive_closes`, `open_positions`, and `last_error` in current state.
2. Run `diagnose_live.py --symbol <symbol> --venues <long,short>` for high-confidence live truth.
3. Check whether open-order truth is supported and successful on both venues.
4. Search for terminal flat, under-min, price-unavailable, and fallback events.
5. When `price_hint=0.0`, search adjacent `runtime.close_price_evidence_fallback`,
   `runtime.close_price_evidence_stale`, and
   `runtime.close_price_evidence_missing` before changing close semantics.
6. Check whether `runtime.passive_close_deadline_fallback_armed`,
   `execution.hedge_deadline_breached`, or
   `execution.close_deadline_breached` fired before any retry backoff.
7. If live truth is flat, bug is stale local terminality. If live truth is nonzero, bug is compensation/repair path.
8. Inspect the restored `pending_close_reconciliations` container type before
   assuming a passive-close semantic decision failure.
9. Check whether terminal-flat events were emitted before state removal or core
   clear evidence.
10. Check whether malformed pending-close reconciliation items are being
    silently retained, skipped by supervisor venue coverage, or skipped by entry
    conflict gating.
11. Closure requires harness replay plus credentialed flat/no-open-orders probe.
12. If a reduce-only close open order still exists, decide owner first: adopt
    matching pending/journal passive-close owner, or fail closed as ownerless.
13. If `exit.reconciled evidence_gap=true`, classify whether the missing proof
    is long side, short side, both sides, or duplicate suppression before
    treating it as a resolved process-quality artifact.
14. If final truth is trusted, open orders are flat, and exactly one live leg
    remains, expect `passive_close_final_truth_action=flatten_remaining_live_leg`
    and an owned reduce-only flatten attempt. A repeated
    `retry_exchange_position_open_order_truth` in that shape is a recurrence.
15. Treat `risk_only`/`fail_closed` as temporary containment only. Closure still
    requires exchange flat, no open orders, and local owner cleanup.
16. If current truth is flat/no-open-orders but diagnose still shows
    `risk_mode_fail_closed`, verify whether terminal owner cleanup emitted
    `runtime.stale_fail_closed_cleared` or whether a real operator/requested
    fail-closed or remaining recovery record is blocking the clear.
17. If `risk_only_live_single_leg_exposure_count` is nonzero, verify it against
    current `unpaired_live_position_recoveries`, not only since-deploy event
    history.
18. If Binance close returns `-5022`, check BBO and chunk fill progress before
    maker-leg rotation. Rotation is zero-progress only; partial-progress close
    chunks must preserve fill accounting and fall back to terminal taker close.
19. If Binance close returns `-2022`, inspect existing reduce-only close orders
    and position truth before deciding retry, adopt, or terminal-flat.
20. If Aster close returns `-2022 ReduceOnly Order is rejected`, distinguish
    accepted-order truth gaps from direct HTTP rejects. Direct rejects create no
    accepted order to match later; only downgrade them after request identity,
    same-position terminal flat, and current flat/no-open-orders truth are all
    present.
21. For repeated Aster/Bybit already-flat reduce-only artifacts, inspect whether
    `exit.passive_close_live_one_sided_pre_submit_flat` or
    `exit.passive_close_live_one_sided_pre_submit_quantity_refreshed` preceded
    the cleanup submit. Missing pre-submit refresh is a runtime-quality issue;
    present refresh plus clean terminal proof is a resolved close artifact.
