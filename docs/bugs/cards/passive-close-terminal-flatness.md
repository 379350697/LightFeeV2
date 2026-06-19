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
- `passive_close_resolved_without_terminal_truth_count`
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
| 2026-06-19 | Existing reduce-only close order covers quantity | deployed through `039d52c`; waiting-event evidence contract fixed in current release | GENIUSUSDT-style close drift is not terminal-flat when Bybit still has nonzero position plus a matching reduce-only close order. CL-100 adopts the existing close order into pending passive close state, adds journal-based close-owner recovery, and keeps diagnose/gate red until exchange truth is flat/no-open-orders or owned close work is resolved. Waiting passive-close evidence now carries local `exchange_truth_attempt` payload instead of requiring a join with sibling diagnostics. |
| 2026-06-19 | Passive close resolved before terminal exchange truth | deployed through `039d52c`; payload contract fixed in current release | ESPORTSUSDT-style truth lag showed current production could recover flat/no-open-orders while diagnostics still counted old passive-close artifacts as active stuck. CL-101 keeps live pending passive close owner in `waiting_exchange_flat_truth` until position/open-order truth jointly proves flat, maps that owner as `owned_pending_passive_close`, moves gate-green historical hard-over-budget artifacts into recovered process counters, and records `exchange_truth_attempt` directly on every waiting event, including missing-position-snapshot cases. |

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
| 2026-06-19 | `GENIUSUSDT` Bybit reduce-only close owner | `039d52c` + current release payload follow-up | deployed terminal-truth gates pass; waiting-event evidence payload now local to the event | [daily/2026-06-19.md#cluster-cl-100---openclose-lifecycle-owner-truth-and-reduce-only-adoption](../daily/2026-06-19.md#cluster-cl-100---openclose-lifecycle-owner-truth-and-reduce-only-adoption) |

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
