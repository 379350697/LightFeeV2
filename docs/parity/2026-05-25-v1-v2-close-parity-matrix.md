# V1/V2 Close Parity Matrix - 2026-05-25

This matrix is the close-specific audit artifact for prompt 2. It is separate from the UBUSDT bug cluster ledger. The ledger records one production branch; this file records the V1/V2 close semantic comparison by function, branch, state field, venue call, event, and test evidence.

Status values:

- `PASS`: V2 implementation and existing tests match the listed V1 branch.
- `FAIL -> GREEN`: baseline V2 drift was reproduced with a RED test and is fixed in the current worktree.
- `REVIEW GAP`: code-read evidence exists, but this matrix does not claim a full branch-exhaustive root fix.
- `APPROVED RUNTIME BOUNDARY`: V2 has no executor-local equivalent to the V1 call site; the matrix documents the runtime boundary and requires a harness proving equivalent persisted/raw view behavior.

## Executive Matrix

| Area | V1 reference | V2 reference | Current status | RED / evidence test |
|---|---|---|---|---|
| aggressive close order sequencing | `LightFee/src/engine/exit.rs:3335-3527` | `lightfee/engine/close_executor.py:595-952` | PASS | `tests/test_close_execution.py`, `tests/engine/test_close_semantic_parity.py` |
| aggressive close min-notional/dust precheck | `exit.rs:3035-3171`, `3335-3380` | `close_executor.py:396-449`, `595-640` | PASS | `TestMinNotionalDustHandling`, `tests/test_close_execution.py::TestMinNotionalDust` |
| aggressive close uncertain/compensation/fail-closed | `exit.rs:3450-3516`, compensation path around `1482-1601` | `close_executor.py:687-837`, `1440-1500` | PASS | `tests/test_close_execution.py` compensation cases |
| terminal reduce-only flat | V1 close reducer and venue error handling, plus recovery flat checks | `close_executor.py:90-207`, `1291-1325`; `passive_close.py:2185-2230` | PASS | `tests/engine/test_close_semantic_parity.py:261-470`, passive terminal-flat tests |
| duplicate cid reconciliation | `live/bybit.rs:2255-2266`, `2820-2894` | `close_executor.py:1215-1289`; `passive_close.py` duplicate hedge branch | PASS | `tests/test_close_execution.py::test_bybit_duplicate_close_id_registers_pending_reconciliation`, `tests/test_passive_close.py::test_fallback_unhedged_bybit_duplicate_reconciles_fill_and_clears` |
| passive close creation/persistence | `exit.rs:1603-1646` | `passive_close.py:264-340` start path, state dataclasses | PASS | `tests/test_passive_close.py::test_start_rejects_duplicate`, `tests/engine/test_passive_close_semantic_parity.py:41-67` |
| passive close maker progress and persisted close legs | `exit.rs:2190-2197`, persisted-leg use in `1677-1687` | `passive_close.py:1216-1226`, `1374-1396`, finalization path | PASS | `tests/test_passive_close.py::test_terminal_filled_persists_full_maker_leg`, close semantic PnL tests |
| passive close terminal maker `FILLED` hedge guard | `exit.rs:2197-2313`, `2343-2414` | `passive_close.py:445-502`, `1232-1338` | FAIL -> GREEN | `tests/test_passive_close.py::TestTerminalMakerFillHedgeFail::test_terminal_maker_filled_bybit_dust_gap_uses_guard_and_live_flat_cleanup` |
| passive close generic delta hedge guard | `exit.rs:2197-2313` | `passive_close.py:532-658`, `1232-1338` | FAIL -> GREEN | `tests/test_passive_close.py::TestPartialMakerFillGradualCatchUp::test_generic_delta_hedge_bybit_dust_gap_uses_same_guard` |
| small-fill buffer | `exit.rs:2218-2296`, `3173-3194` | `passive_close.py:532-652`, `_small_fill_buffer_decision` | PASS after UBUSDT fix | `tests/engine/test_passive_close_semantic_parity.py:606-847`; UBUSDT RED tests above |
| min-notional / dust normalized quantity ordering | `exit.rs:2297-2313` | `passive_close.py:1269-1314`, `transport.py:1536-1639`, `5694-5708` | FAIL -> GREEN | UBUSDT hedge guard tests; `tests/test_venues_transport.py::TestPassivePreflight::test_bybit_normalize_quantity_uses_dynamic_symbol_rules` |
| fallback dual taker | `exit.rs:2980-2984`, aggressive fallback semantics | `passive_close.py:2232-2395` | PASS | `tests/test_passive_close.py::test_fallback_paired_residual_total_quantity`, `test_fallback_unhedged_fails_blocks_aggressive_close`, terminal-flat fallback tests |
| chunk advance invariant | `exit.rs:1648-1708` | `passive_close.py:1867+` `_advance_chunk` and `drive_pending_passive_close` guards | PASS | `tests/test_passive_close.py::TestTerminalMakerFillHedgeFail::*advance*`, `tests/engine/test_passive_close_semantic_parity.py:772-822` |
| retry/cooldown/deadline | `exit.rs:2987-3017`, passive retry windows, close retry loops | `passive_close.py:362-365`, `821-850`; `close_executor.py:1190-1335`; runtime cooldown tests | PASS for covered branches | `tests/engine/test_passive_close_semantic_parity.py:323-587`, `tests/test_live_full_closure.py::test_cooldown_respected` |
| pending passive close processing | `exit.rs:2987-3017` | `passive_close.py:2143-2175`; runtime call `runtime.py:7579-7596` | FAIL -> GREEN for live-flat sweep | `tests/test_passive_close.py::TestProcessPendingPassiveCloseLiveFlatReconcile::test_process_pending_passive_closes_clears_live_flat_state_before_hedge` |
| pending passive close live-flat recovery | `recovery.rs:2785-2901` | `passive_close.py:2185-2230`, `2143-2175`; runtime recovery `runtime.py:7549-7596` | FAIL -> GREEN, with payload gap noted below | same live-flat RED/GREEN test |
| live-flat reconcile payload and last_error clearing | `recovery.rs:2818-2900` | `passive_close.py:2201-2259` | FAIL -> GREEN | `tests/test_passive_close.py::TestProcessPendingPassiveCloseLiveFlatReconcile::test_live_flat_cleanup_records_v1_recovery_payload_fields`, `test_live_flat_cleanup_clears_matching_last_error` |
| recovered pending close non-flat/probe-failure diagnostics | `recovery.rs:2785-2901` conservative continue branches | `passive_close.py:_probe_live_flatness`, `scripts/diagnose_live.py` | FAIL -> GREEN locally; production deploy pending | `test_xcnusdt_recovered_one_side_live_nonzero_records_diagnostic_event`, `test_xcnusdt_recovered_live_fetch_partial_failure_records_retry_diagnostic`, diagnose-live venue tests |
| position drift correction | `recovery.rs:2822-2861` | `passive_close.py:2212-2229`; runtime housekeeping | PASS for local event/state cleanup | `tests/test_passive_close.py::test_fallback_clears_when_live_positions_are_flat`, live-flat RED/GREEN test |
| current-state mirror / runtime view sync | `recovery.rs:2864-2885` | `passive_close.py:2218-2259`; `loop_control.py:_export_current_state_snapshot`; `EngineState.to_dict()` | FAIL -> GREEN | `tests/test_passive_close.py::TestProcessPendingPassiveCloseLiveFlatReconcile::test_live_flat_cleanup_syncs_current_state_view_without_position` |
| state persistence / pending close legs | `exit.rs:1645`, `1677-1687`, `1703-1706`, `2397-2413`, `2900` | `passive_close.py` pending mutation, `close_executor.py:990-1023`, `runtime.py:2828-2842`, snapshot layer | APPROVED RUNTIME BOUNDARY -> GREEN harness | `tests/test_passive_close.py::TestProcessPendingPassiveCloseLiveFlatReconcile::test_live_flat_cleanup_persistent_state_view_drops_pending_and_open` |
| Bybit order quantity normalization | `live/bybit.rs:2099-2110`, `2896-2899` | `transport.py:3147-3155`, `5694-5708` | FAIL -> GREEN | `tests/test_venues_transport.py::TestPassivePreflight::test_bybit_place_order_reduce_only_rejects_dynamic_min_qty_without_http`, `test_bybit_normalize_quantity_uses_dynamic_symbol_rules` |

## Function-Level Detail

### 1. Aggressive Close

| Required field | V1 | V2 | Status |
|---|---|---|---|
| V1 line | `exit.rs:3335-3527` | `close_executor.py:595-952` | PASS |
| V1 condition ordering | Build chunks, then for each chunk check `close_position_exchange_min_notional_violation` before submitting either leg; submit short close first, then long close. | `compute_close_chunks()` at `631-640`; short IOC request at `669-679`; long IOC request at `751-760`. | PASS |
| V1 state changes | Accumulates `short_legs` / `long_legs`; compensation may add legs; final close execution is built from all legs. | Accumulates `short_legs` / `long_legs`; writes `PendingClose` on uncertainty at `990-1023`; updates/removes `open_positions` at `981-1060`. | PASS |
| V1 venue calls | `execute_order_leg(short_venue, Side::Buy)` then `execute_order_leg(long_venue, Side::Sell)`. | `adapter.place_order(OrderRequest(..., reduce_only=True, TimeInForce.IOC))` via `_submit_close_leg`. | PASS |
| V1 journal/event | `execution.order_blocked`, close leg fill events, `exit.closed`, compensation events. Payload keys include `position_id`, `symbol`, `chunk_index`, `client_order_id`, PnL fields. | `order.filled`, `order.rejected`, `order.uncertain`, `exit.close_chunk_submitted`, `exit.closed`, `exit.pending_close_registered`, `exit.close_residual_detected`. | PASS, event names are V2 equivalents not byte-identical |
| Minimal RED if drift | Make short leg fill and long leg rejected/uncertain; assert compensation or pending close rather than dropping one-sided exposure. | Existing coverage in `tests/test_close_execution.py`. | PASS |

### 2. Passive Close Creation And Chunk Advance

| Required field | V1 | V2 | Status |
|---|---|---|---|
| V1 line | `exit.rs:1603-1646`, `1648-1708` | `passive_close.py:start_pending_passive_close`, `_advance_chunk`, `drive_pending_passive_close` | PASS |
| V1 condition ordering | Reject duplicate pending close first; compute chunk quantities; initialize phase state; persist. On chunk advance, increment index, finalize if complete, otherwise reset phase/fill/small-fill fields then persist. | Duplicate guard in start path; chunk reset semantics tested via state fields and `_advance_chunk`. | PASS |
| V1 state changes | `pending_passive_closes += PendingPassiveClose`; resets `maker_fill`, `hedge_fill`, `small_fill_min_notional_attempts`, `last_small_fill_missing_quantity`, `small_fill_buffer_started_at_ms`, `next_retry_at_ms`. | Same state fields exist and tests assert reset. | PASS |
| V1 venue calls | None on creation; no venue call during pure chunk advance. | Same. | PASS |
| V1 journal/event | `pending_passive_close_created`, `execution.multi_phase_latency_summary`, `pending_passive_close_advanced_chunk`, `pending_passive_close_resolved`. | `exit.passive_close_started`, `exit.passive_close_chunk_filled`, finalize/cleanup events. | PASS, names differ |
| Minimal RED if drift | Maker/hedge not caught up must not advance; chunk reset must clear M-R14 state. | `test_advance_blocked_when_maker_under_chunk`, `test_field_reset_on_chunk_advance`. | PASS |

### 3. Passive Maker Progress And Hedge Delta

| Required field | V1 | V2 | Status |
|---|---|---|---|
| V1 line | `exit.rs:2190-2414` | `passive_close.py:437-658`, `1232-1399` | FAIL -> GREEN for normalized dust branches |
| V1 condition ordering | Apply maker cumulative progress, compute `outstanding_hedge_quantity = maker_fill - hedged_close_quantity`, then small-fill buffer, then `adapter.normalize_quantity`, then min-notional/dust decision, then hedge submit only if valid. Terminal maker states do not bypass this branch. | Terminal maker `FILLED` routes through `_submit_hedge_for_delta(..., maker_terminal=True)` at `455-463`; generic delta routes through `_submit_hedge_for_delta` at `654-658`; `_submit_hedge_for_delta` normalizes before min-notional at `1269-1314`. | GREEN locally |
| V1 state changes | Updates `pending.maker_fill`; increments `small_fill_min_notional_attempts` only when missing quantity grows; updates `last_small_fill_missing_quantity`; on abort/fallback enters compensation or dual-taker; persists pending. | Updates maker and hedge cumulative fills; records dust abort; sets `DUAL_TAKER` on terminal dust; clears live-flat pending/open if both venues flat. | GREEN locally |
| V1 venue calls | Maker progress from passive order query; hedge calls `normalize_quantity` before IOC reduce-only submit; cancel maker when aborting non-terminal accumulation. | `_poll_maker_progress`; `adapter.normalize_quantity`; `adapter.place_order`; `_clear_if_live_flat`; maker cancel/maintenance paths. | GREEN locally |
| V1 journal/event | `execution.passive_close_small_fill_buffering`, `execution.passive_close_small_fill_buffer_expired`, `execution.min_notional_accumulating`, `execution.min_notional_abort_and_flatten`. Payload keys include `missing_hedge_quantity`, `normalized_quantity`, `leg_notional_quote`, `venue_min_notional_quote`, `attempt`. | `exit.passive_close_small_fill_buffering`, `exit.passive_close_small_fill_buffer_expired`, `exit.passive_close_min_notional_accumulating`, `exit.passive_close_min_notional_abort`, `exit.passive_close_hedge_dust_aborted`, `exit.passive_close_hedge_filled`. | GREEN locally; event names/payloads are V2 equivalents |
| Minimal RED if drift | Terminal `FILLED` with UBUSDT maker fill `1.0` must await `normalize_quantity`, avoid HTTP hedge, and clear if live flat. Generic non-terminal delta must use same guard. | `test_terminal_maker_filled_bybit_dust_gap_uses_guard_and_live_flat_cleanup`, `test_generic_delta_hedge_bybit_dust_gap_uses_same_guard`. | GREEN locally |

### 4. Fallback Dual Taker

| Required field | V1 | V2 | Status |
|---|---|---|---|
| V1 line | passive fallback arming around `exit.rs:2980-2984`; aggressive close `3335-3527` | `passive_close.py:2232-2395`; `close_executor.py:595-952` | PASS |
| V1 condition ordering | If passive phases exhausted or terminal hedge cannot safely continue, arm dual taker. Catch up unhedged one-sided residual before paired residual; then use aggressive close for only the current chunk remainder. | `_fallback_to_aggressive_close` computes `unhedged_residual` and `paired_residual` at `2247-2252`; probes live flat; hedges unhedged first at `2296-2329`; closes paired residual at `2330-2342`. | PASS |
| V1 state changes | Pending stays until fallback succeeds or is reconciled flat; final cleanup removes pending passive close. | Pending retained on failed catch-up/zero fill, removed on completion or live-flat clear. | PASS |
| V1 venue calls | Hedge residual via hedge venue; paired residual through close executor. | `_submit_hedge_for_delta`; `CloseExecutor.execute_close`. | PASS |
| V1 journal/event | `pending_passive_close_dual_taker_armed`, close events, compensation/fallback events. | `exit.passive_close_fallback_aggressive`, `exit.passive_close_fallback_unhedged_failed`, `exit.passive_close_fallback_complete`, terminal-flat events. | PASS |
| Minimal RED if drift | If maker=0.4, hedge=0.2, paired=0.6, aggressive close must not run until hedge catch-up succeeds. | `test_fallback_unhedged_fails_blocks_aggressive_close`, `test_fallback_paired_residual_total_quantity`. | PASS |

### 5. Pending Passive Close Recovery And Live-Flat Reconcile

| Required field | V1 | V2 | Status |
|---|---|---|---|
| V1 line | `recovery.rs:2785-2901`; pending driver `exit.rs:2987-3017` | `passive_close.py:2143-2175`, `2185-2230`; runtime call sites `runtime.py:7549-7596` | FAIL -> GREEN for sweep, REVIEW GAP for exact payload/persistence |
| V1 condition ordering | Only live mode and pending passive closes; fetch long and short live positions; if either fetch fails, continue; if both approx zero, mark drift/flat, remove open and pending, clear matching `last_error`, sync mirrors, persist. | `process_pending_passive_closes` probes each ready pending before driving; `_clear_if_live_flat` fetches via adapters, records V1 payload fields, removes pending/open, clears matching `last_error`, and logs flat/drift correction. Runtime persists at the end of the same `run_loop` pass after the passive close lane (`runtime.py:2828-2842`). | GREEN locally for cleanup, payload, `last_error`, mirror/current-state view, and raw persistent view |
| V1 state changes | Removes `open_positions`, removes `pending_passive_close`, clears selected `last_error`, syncs mirrors, persists. | Removes `state.pending_passive_closes` and `state.open_positions`; clears matching dynamic `state.last_error`; `EngineState.to_dict()`, current-state export, and persistent view are derived from the same mutated state. | GREEN locally; V2 has no separate open-position mirror collection beyond derived current-state/runtime views |
| V1 venue calls | `adapter(long_venue).fetch_position(symbol)` and `adapter(short_venue).fetch_position(symbol)`. | `_probe_live_flatness` through adapters. | PASS |
| V1 journal/event | `runtime.position_drift_detected`, `recovery.flat`, `runtime.position_drift_corrected`, or `runtime.pending_passive_close_flat_reconciled`; payload keys include position/venues/symbol/sizes/source. | `runtime.position_drift_detected`, `exit.passive_close_fallback_terminal_flat`, `recovery.flat`, `runtime.position_drift_corrected`; drift payload includes `position_id`, `symbol`, `long_venue`, `short_venue`, `expected_size`, `old_quantity`, `actual_long_size`, `actual_short_size`, `new_quantity=0.0`, and `source=pending_passive_close_flat_probe`. | FAIL -> GREEN |
| Minimal RED if drift | Pending passive close with both venues flat must be cleared before another hedge/fallback attempt. | `test_process_pending_passive_closes_clears_live_flat_state_before_hedge`. | GREEN locally |

### 5A. XCNUSDT Recovered Pending Evidence Boundary

This section records the follow-up evidence for `live-recovered:XCNUSDT:bybit->aster`. It is not counted as a new global close-parity completion; it is a symbol-specific production read-only finding plus local diagnostic hardening.

| Required evidence | Result |
|---|---|
| Local state chain | Initial deployed `diagnose_live.py --json --symbol XCNUSDT --since-deploy` saw `open_position_count=1`, `position_id=live-recovered:XCNUSDT:bybit->aster`, `quantity=5070.0`, `matched_quantity=0`, `long_venue=bybit`, `short_venue=aster`, and no proof that the exchange still held exposure. |
| Production exchange truth | Read-only probe under service env showed Bybit XCNUSDT position `0.0`, Bybit open orders `0`, Aster XCNUSDT position `0.0`, Aster open orders `0`. |
| Production current state after V1 live-flat sweep | `/opt/lightfee-v2/runtime/live-state.json` and `live-state-current.json` showed `open_position_count=0`, `pending_passive_close_count=0`, `last_error=null`. |
| Event chain | `exit.passive_close_recovery_probe_flat` -> `runtime.position_drift_detected` -> `exit.passive_close_fallback_terminal_flat` -> `recovery.flat` -> `runtime.position_drift_corrected`, with `actual_long_size=0.0`, `actual_short_size=0.0`, `new_quantity=0.0`, `source=pending_passive_close_flat_probe`. |
| Root-cause classification | Evidence points to stale recovered local pending/open state that was correctly removed by the deployed V1 live-flat sweep. It is not attributed to the UBUSDT Bybit `quantity=1.0` min-quantity bug. |
| New local diagnostic behavior | If both sides are not flat or either live fetch fails, V2 now keeps the pending close, emits `exit.passive_close_recovery_probe_diagnostic`, and records `position_id`, venues, local and live sizes, pending phase, maker/hedge fill, client order ids, source, decision, and next action. |
| Diagnose script behavior | `scripts/diagnose_live.py` now derives venues from local positions and can query Aster and OKX read-only adapters; symbol filtering also matches `position_id` for recovered events without a top-level `symbol`. |
| Acceptance boundary | Production XCNUSDT is read-only verified flat and has no open/pending state. The new diagnostic hardening is local GREEN and still needs a deployment/read-only acceptance run before being called cloud verified. |

### 6. Bybit Venue Normalization And Reconciliation

| Required field | V1 | V2 | Status |
|---|---|---|---|
| V1 line | `live/bybit.rs:2099-2110`, `2212-2266`, `2360-2540`, `2820-2899` | `transport.py:1536-1639`, `3147-3155`, `5694-5708`; passive/close duplicate reconciliation paths | FAIL -> GREEN for dynamic min quantity |
| V1 condition ordering | Resolve `symbol_meta`, floor quantity to `meta.qty_step`, validate min quantity/notional, then submit. Duplicate passive orderLinkId attempts recover via order lookup. Normalize quantity uses live metadata. | Bybit `place_order` loads `SymbolRulesCache` before preflight; preflight rejects below dynamic `min_qty`; `normalize_quantity` uses dynamic `qty_step/min_qty`. Duplicate cid paths reconcile fills by client id. | GREEN locally |
| V1 state changes | None inside venue except fill metadata; callers update pending/open. | Same boundary. | PASS |
| V1 venue calls | `/v5/order/create`, `/v5/order/realtime`, `/v5/execution/list`, cancellation endpoint, metadata lookup. | Real `VenueTransport` preflight before `_request`; reconciliation methods tested in venue/passive paths. | GREEN locally |
| V1 journal/event | V1 adapter returns fills/errors; engine logs duplicate reconciliation and progress. | Transport diagnostics plus engine events `exit.passive_close_hedge_duplicate_client_order_reconciled`, `exit.close_duplicate_client_order_*`. | PASS |
| Minimal RED if drift | UBUSDT `quantity=1.0` with dynamic `min_qty=10`, `qty_step=10` must normalize to `0.0` and must not send HTTP. | `test_bybit_normalize_quantity_uses_dynamic_symbol_rules`, `test_bybit_place_order_reduce_only_rejects_dynamic_min_qty_without_http`. | GREEN locally |

## Prompt-2 Coverage Checklist

| Required coverage | Status | Evidence |
|---|---|---|
| aggressive close | PASS | `close_executor.py:595-952`; close execution/parity tests |
| passive close maker | PASS | `passive_close.py:417-524`; maker progress tests |
| passive close hedge | FAIL -> GREEN for UBUSDT dust branches | `_submit_hedge_for_delta` RED/GREEN tests |
| fallback dual taker | PASS | `passive_close.py:2232-2395`; fallback tests |
| small-fill buffer | PASS after fix | M-R14 tests and UBUSDT RED/GREEN |
| min-notional/dust | FAIL -> GREEN for Bybit dynamic min quantity; REVIEW GAP for exact event-name parity | venue transport RED/GREEN and M-R14 tests |
| duplicate cid reconciliation | PASS | Bybit duplicate close/passive tests |
| terminal reduce-only flat | PASS | close semantic M-R12 tests and passive terminal-flat tests |
| pending passive close recovery | FAIL -> GREEN for live-flat sweep, exact V1 payload fields, matching `last_error` clearing, and persistent raw-state view | live-flat RED/GREEN plus `test_live_flat_cleanup_records_v1_recovery_payload_fields`, `test_live_flat_cleanup_clears_matching_last_error`, `test_live_flat_cleanup_persistent_state_view_drops_pending_and_open` |
| recovered pending close non-flat/probe-failure diagnostics | FAIL -> GREEN locally; production deploy pending | XCNUSDT recovered tests and diagnose-live Aster/position-id tests |
| live-flat reconcile | GREEN for cleanup and current-state/runtime view sync; approved difference: V2 has no separate open-position mirror collection beyond derived state/current-state views | `test_live_flat_cleanup_syncs_current_state_view_without_position` |
| position drift correction | PASS for event/state cleanup | `runtime.position_drift_corrected` asserted in events |
| persisted close legs | PASS for leg accumulation/finalization tests | passive maker/hedge persisted leg tests; close PnL tests |
| chunk advance | PASS | `_advance_chunk` tests and semantic tests |
| retry/cooldown/deadline | PASS for covered branches, not newly root-fixed here | retry/cooldown semantic tests |
| state persistence | APPROVED RUNTIME BOUNDARY -> GREEN harness | executor mutates state; `Runtime.run_loop` persists via `snapshot_store.write(build_persistent_state_view(self.state))` after `_maybe_tick_passive_close`; raw view harness proves no pending/open remains |

## Known Drift Items And Test Mapping

| Risk | Drift | Baseline RED | Current status |
|---|---|---|---|
| Critical | Terminal maker `FILLED` directly submitted hedge for UBUSDT `1.0`, bypassing normalized min-quantity/dust guard. | `test_terminal_maker_filled_bybit_dust_gap_uses_guard_and_live_flat_cleanup` failed on temp HEAD because `normalize_quantity` was never awaited. | GREEN |
| Critical | Generic delta hedge branch did not prove all hedge submit paths used normalized dust guard. | `test_generic_delta_hedge_bybit_dust_gap_uses_same_guard` failed on temp HEAD because `normalize_quantity` was never awaited. | GREEN |
| Critical | Bybit `place_order` / `normalize_quantity` used static spec rather than dynamic `SymbolRulesCache`; under-min UBUSDT was sent to exchange. | `test_bybit_normalize_quantity_uses_dynamic_symbol_rules` returned `1.0`; `test_bybit_place_order_reduce_only_rejects_dynamic_min_qty_without_http` reached HTTP mock. | GREEN |
| High | Pending passive close driver did not continuously perform V1 live-flat reconcile before retrying hedge/fallback. | `test_process_pending_passive_closes_clears_live_flat_state_before_hedge` retained pending id on temp HEAD. | GREEN |
| Medium | Exact V1 recovery event payload, `last_error` clearing, mirror/current-state sync, and persist raw-state view were narrower or unproven in V2 than in `recovery.rs:2818-2900`. | REVIEW GAP closure tests failed before the local fix for payload, `last_error`, and current-state count; persistent raw-state view harness documents the V2 runtime boundary. | GREEN locally; production acceptance pending deploy |
| Medium | Recovered pending close was observable in production without enough structured evidence when it was not yet proven flat/non-flat, especially for Aster pairs and events that only carried the recovered symbol in `position_id`. | XCNUSDT diagnostics tests first failed because no diagnostic event was emitted for one-side nonzero/fetch failure, `diagnose_live` did not target Aster, and symbol filter missed position-id-only events. | GREEN locally; diagnostic deploy acceptance pending |

## Event And Payload Parity

| Branch | V1 payload fields | V2 payload fields | Status |
|---|---|---|---|
| live-flat recovery drift | `position_id`, `symbol`, venues, expected/local size, live long/short size, source, corrected quantity | `runtime.position_drift_detected` includes `position_id`, `symbol`, `long_venue`, `short_venue`, `expected_size`, `old_quantity`, `actual_long_size`, `actual_short_size`, `new_quantity`, `source` | FAIL -> GREEN |
| live-flat recovery completion | flat/recovery event with position id, symbol, venues, live sizes and recovery source | `recovery.flat`, `runtime.position_drift_corrected`, and terminal-flat event carry position/symbol/venue/source fields | FAIL -> GREEN; V2 event names approved as equivalent because local consumers assert fields through journal payload rather than V1 event names |
| terminal flat reduce-only reconciliation | terminal/flat close branch records position, symbol, venue, local size, live size and reason | `exit.passive_close_fallback_terminal_flat` plus drift/corrected events | PASS for tested passive and close branches |
| min-notional / dust abort | missing hedge quantity, normalized quantity, quote notional, venue min notional, attempt/source | `exit.passive_close_min_notional_accumulating`, `exit.passive_close_min_notional_abort`, `exit.passive_close_hedge_dust_aborted` include pending id, symbol, venue, missing/normalized quantities and guard decision fields where tested | FAIL -> GREEN for UBUSDT/Bybit dynamic guard; exact V1 names are approved difference |
| duplicate cid reconciliation | client id, venue order id/fill, symbol, side, quantity, terminal/duplicate reason | V2 duplicate close/passive events include client id, venue, symbol and reconciled fill context | PASS for covered Bybit branches |
| fallback complete | position/pending id, symbol, unhedged residual, paired residual, final state | `exit.passive_close_fallback_complete` and related unhedged/paired events carry pending id, symbol and residual quantities in tests | PASS |
| recovered pending non-flat/probe-failure diagnostic | V1 conservative branch continues pending when live evidence is incomplete or non-flat; no clear is performed | `exit.passive_close_recovery_probe_diagnostic` includes `position_id`, `symbol`, `long_venue`, `short_venue`, `local_quantity`, `matched_quantity`, `maker_fill`, `hedge_fill`, `pending_phase`, `live_long_size`, `live_short_size`, `live_long_open_orders`, `live_short_open_orders`, `client_order_ids`, `source`, `decision`, `next_action`, and live errors | FAIL -> GREEN locally; evidence-only, not a cleanup root fix |

## Verification Evidence

Last local verification for this matrix:

- `pytest -q tests/test_passive_close.py tests/test_venues_transport.py tests/test_diagnose_live.py tests/engine/test_close_semantic_parity.py tests/engine/test_passive_close_semantic_parity.py tests/test_close_execution.py` = `596 passed`
- Hyperliquid IOC/L2/reconciliation targeted confirmation = `9 passed`
- OKX contract-unit/residual targeted confirmation = `13 passed`
- Bybit/Aster dynamic-rule targeted confirmation = `6 passed`
- `python3 -m compileall -q lightfee tests scripts` = passed
- `git diff --check` = clean
- `gitnexus_detect_changes(scope=all)` = MEDIUM risk, changed files `10`, affected processes `2`, no HIGH/CRITICAL risk

REVIEW GAP closure evidence added after the initial matrix:

- `tests/test_passive_close.py::TestProcessPendingPassiveCloseLiveFlatReconcile::test_live_flat_cleanup_records_v1_recovery_payload_fields` = RED then GREEN locally
- `tests/test_passive_close.py::TestProcessPendingPassiveCloseLiveFlatReconcile::test_live_flat_cleanup_clears_matching_last_error` = RED then GREEN locally
- `tests/test_passive_close.py::TestProcessPendingPassiveCloseLiveFlatReconcile::test_live_flat_cleanup_syncs_current_state_view_without_position` = RED then GREEN locally
- `tests/test_passive_close.py::TestProcessPendingPassiveCloseLiveFlatReconcile::test_live_flat_cleanup_persistent_state_view_drops_pending_and_open` = GREEN harness for the approved runtime persistence boundary
- `tests/test_passive_close.py::TestProcessPendingPassiveCloseLiveFlatReconcile::test_xcnusdt_recovered_live_flat_cleanup_records_diagnostic_payload` = RED then GREEN locally
- `tests/test_passive_close.py::TestProcessPendingPassiveCloseLiveFlatReconcile::test_xcnusdt_recovered_one_side_live_nonzero_records_diagnostic_event` = RED then GREEN locally
- `tests/test_passive_close.py::TestProcessPendingPassiveCloseLiveFlatReconcile::test_xcnusdt_recovered_live_fetch_partial_failure_records_retry_diagnostic` = RED then GREEN locally
- `tests/test_diagnose_live.py::test_symbol_filter_matches_position_id_when_symbol_field_missing` = RED then GREEN locally
- `tests/test_diagnose_live.py::test_exchange_truth_targets_aster_for_xcnusdt_pair` = RED then GREEN locally
- `tests/test_diagnose_live.py::test_run_diagnose_derives_exchange_truth_venues_from_xcnusdt_position` = RED then GREEN locally

Production acceptance is closed for the UBUSDT branch on deployed local patch `local_patch_73d5428-reviewgap-20260525`. Post-deploy read-only checks showed the services active as singletons, remote compileall passed, deployed file checksums matched local files, UBUSDT local pending/open state was cleared, `diagnose_live.py --json --symbol UBUSDT --since-deploy` emitted `exit.passive_close_recovery_probe_flat`, `runtime.position_drift_detected`, `recovery.flat`, and `runtime.position_drift_corrected`, Bybit and OKX UBUSDT positions were `0.0`, and Bybit/OKX UBUSDT open order counts were `0`. No post-restart UBUSDT Bybit `retCode=10001` / minimum-contract / `quantity=1.0` hedge loop was observed.

Follow-up production read-only acceptance for `live-recovered:XCNUSDT:bybit->aster` showed the deployed live-flat sweep cleared the stale local recovered state: both `live-state.json` and `live-state-current.json` had `open_position_count=0`, `pending_passive_close_count=0`, and `last_error=null`; Bybit and Aster XCNUSDT positions were `0.0`; Bybit and Aster XCNUSDT open order counts were `0`. This is a symbol-specific evidence closure for XCNUSDT, not proof that every old close-parity ledger item is cloud verified. The new diagnostic enhancements in this worktree are local GREEN and remain deploy/read-only acceptance pending.
