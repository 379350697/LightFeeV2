# BUG-20260514-v2-v1-parity-root-fix-loop

Status: fixed; all P1/P2 residual closed (index-detail now consistent)
Severity: high
Component: `entry-local-l2`, `sidecar-candidate-contract`, `order-reconciliation`, `bitget-l2-metadata`, `dryrun-audit`, `test-coverage`, `bybit-execution-side`, `bitget-quantity-fallback`
Fingerprint: `v2.v1-parity.surface-copy.not-data-contract + fake-tests.green.real-path-red`
First Seen: 2026-05-14 +08:00 during V2 cloud/runtime log review and follow-up parity validation
First Seen Commit: `72ae905` was the first reviewed "root-fix" commit that still showed real-path gaps
Related Refactor: `V1-to-V2 execution and venue parity replication`
Fixed In: `eb9f793` (partial); `c378352` (claimed closure but missing Bitget fillQty/size and Bybit fail-closed side); working tree 2026-05-14 (full closure)
Verified In: local RED→GREEN tests and full suite on 2026-05-14 against working tree

## Summary

The V2 root-fix loop repeatedly produced green tests while real runtime paths still diverged from V1. The core problem was not that V1 behavior was wrong; the repeated failures came from copying visible V1 concepts without fully copying the V1 data contracts, adapter call chains, and failure semantics.

The chain is now closed in the working tree after `c378352`: schema-v2 candidate ingestion derives required identity/timing fields or fails closed, Bybit reconciliation side/retCode semantics are fail-closed, Bitget reconciliation covers the V1 quantity fallback fields, and the bug ledger index/detail status is consistent.

## Symptoms

- Local L2 entry selection initially did not block at the intended stage because V2 candidates lacked the fields the blocker expected.
- Later local L2 fixes moved from no-op/bypass to permanent prewarm blocking for real `CandidateInput` objects.
- ACK-only maker/order reconciliation remained `uncertain` because the engine called `fetch_order_fill_reconciliation`, while a previous fix added `fetch_order_status` as a parallel unused path.
- Bybit/Bitget HTTP status probes returned `None` even when mock exchange responses contained fills.
- Bitget unsupported L2 symbols could reach `/api/v3/market/orderbook` and rely on exchange error `400172` instead of local metadata rejection.
- Dry-run audit returned empty counts for real V2 journal records because it read `event`/`data` while V2 journal writes `kind`/`payload`.
- Tests were green while real paths failed because some tests used fake candidates, monkeypatched target methods, or asserted only field existence rather than field value/behavior.

## Impact

- V2 could report no open entries or repeated local-L2 blockers for reasons unrelated to real market readiness.
- Recovery/reconciliation could leave ACK-only orders in `uncertain` state even when exchange order detail was available by client id.
- Bitget local-L2 bootstrap could waste requests on unsupported symbols and surface noisy exchange-side `400172` instead of deterministic local rejection.
- Operator audit could hide the real reason entries did not open.
- The team spent multiple repair cycles because passing tests did not prove V1 parity on real runtime paths.

## V1 Behavior Proof

| Area | V1 Behavior | V1 Source | V2 Gap Found |
|---|---|---|---|
| Candidate identity | Candidate has stable `pair_id` used across tracked opportunities, sessions, and final gate. | `src/execution_core/entry_local_l2.rs`, `src/execution_core/market_data.rs` | Closed: `CandidateInput.pair_id` exists and schema-v2 ingestion derives it from `symbol/long_venue/short_venue` when missing. |
| Local-L2 prewarm | Prewarm uses `remaining_ms = first_funding_timestamp_ms - now_ms`; candidate must be inside configured prewarm window. | `src/execution_core/market_data.rs:1518` | Closed: runtime uses `first_funding_timestamp_ms`; schema-v2 ingestion derives it from quotes or blocks the candidate fail-closed. |
| Primary/dual-ready gate | Candidate must be primary tracked and have dual-ready entry-local-L2 session. | `src/execution_core/market_data.rs:1681`, `src/execution_core/entry_local_l2_sessions.rs` | Closed: tracked primary ids and entry-local-L2 session runtime are exercised by real `CandidateInput` tests. |
| Bybit reconciliation | Resolve order by `orderLinkId` when needed, then query `/v5/execution/list`, aggregate actual executions, and return None if total quantity is zero. | `src/live/bybit.rs:2820` | Closed: V2 resolves order id, checks retCode, aggregates executions, accepts only `Buy/Sell`, and raises `REQUEST_REJECTED` on invalid/missing side with positive quantity. |
| Bitget reconciliation | Query order detail by `orderId` or `clientOid`; return reconciliation only when filled quantity is positive. | `src/live/bitget.rs:2912` | Closed: V2 returns only positive fills and covers `cumExecQty/baseVolume/filledQty/fillQty/filled_amount/size/fillSz`. |
| Bitget L2 metadata guard | Load symbol metadata before orderbook; reject locally if metadata missing. | `src/live/bitget.rs` `symbol_meta`, `bitget_fetch_execution_liquidity_snapshot` | Closed: unsupported symbols are rejected before orderbook requests. |
| Journal/audit format | V2 journal canonical fields are `kind` and `payload`. | `lightfee/persistence/journal.py` in V2 | Closed: audit reader supports `kind/event`, `payload/data`, and time-window filtering. |

## V1 to V2 Mapping

| V1 Concept | V2 Mapping | Current Status | Notes |
|---|---|---|---|
| `CandidateOpportunity.pair_id` | `CandidateInput.pair_id`, `make_candidate_pair_id()`, `_dict_to_snapshot()` enrichment | fixed | Pairing/V1 compat preserve it; schema-v2 ingestion derives stable `symbol:long->short` when absent. |
| `CandidateOpportunity.first_funding_timestamp_ms` | `CandidateInput.first_funding_timestamp_ms`, `_dict_to_snapshot()` quote-derived enrichment | fixed | Pairing/V1 compat fill it; schema-v2 ingestion derives min positive quote funding timestamp or blocks fail-closed. |
| `candidate_in_entry_local_l2_prewarm_window()` | `LiveRuntime._entry_local_l2_selection_blocker()` | fixed | Runtime uses V1-style `remaining_ms = first_funding_timestamp_ms - now_ms`. |
| tracked primary opportunities | `LiveRuntime._tracked_primary_pair_ids` and `EntryLocalL2SessionRuntime` | fixed | Selection/session path exists and is covered by real `CandidateInput` tests. |
| Bybit `fetch_order_fill_reconciliation()` | `BybitAdapter.fetch_order_fill_reconciliation()` → `VenueTransport.fetch_order_status()` → `_fetch_order_status_bybit()` | fixed | Real HTTP path returns fills, checks retCode, maps 110001 to None, raises other nonzero retCodes, and side parsing is fail-closed. |
| Bitget `fetch_order_fill_reconciliation()` | `BitgetAdapter.fetch_order_fill_reconciliation()` → `VenueTransport.fetch_order_status()` → `_parse_order_status_bitget()` | fixed | Positive-quantity guard and all V1/V2 quantity fallback fields are covered by single-field regression tests. |
| Bitget symbol metadata | `BitgetAdapter.fetch_l2_snapshot()` + transport metadata guard | fixed locally | Verified unsupported symbols do not call orderbook. |
| V2 audit journal reader | `scripts/lightfee_v2_live_dryrun_audit.py` | fixed locally | Supports `kind/event`, `payload/data`, and `ts_ms` window filtering. |

## Root Cause

### Confirmed Root Cause A: V2 copied V1 gate names without copying V1 candidate data contract

Confirmed.

The early local-L2 selection blocker expected fields that the real V2 candidate object did not provide. The first implementation depended on `pair_id` and `created_at_ms`. The next implementation depended on `funding_timestamp_ms`. Real `CandidateInput` still lacked those fields, so the fix either did nothing or blocked forever.

### Confirmed Root Cause B: Reconciliation introduced a parallel API instead of wiring the V1 trait path

Confirmed.

The engine calls `fetch_order_fill_reconciliation`. A previous repair added `fetch_order_status`, but that did not change the engine path. Later adapter overrides called `fetch_order_status`, but real parser exceptions were swallowed and returned `None`.

### Confirmed Root Cause C: Tests validated substitutes instead of production paths

Confirmed.

Early tests used fake candidate classes or monkeypatched the target method (`fetch_order_status`) that needed validation. Later tests improved this, but still left gaps by testing parser helpers instead of the full adapter HTTP path and by asserting only that fields exist rather than that values are populated and usable.

### Confirmed Root Cause D: Audit tooling was outside tracked, tested code

Confirmed.

The dry-run audit script existed as an untracked script and used the wrong field names for V2 journal records. It therefore could not reliably explain why entries did not open.

## Evidence

| Time | Host | Source | Evidence |
|---|---|---|---|
| 2026-05-14 | local | Probe before `eb9f793` | Real `CandidateInput` had no `funding_timestamp_ms`; blocker returned `entry_local_l2_waiting_for_prewarm_window` even with primary tracking and dual-ready session. |
| 2026-05-14 | local | Probe before `eb9f793` | Bybit `/v5/order/realtime` and Bitget `/api/v3/trade/order-info` mock responses returned `None` because parser could not construct `OrderFillReconciliation`. |
| 2026-05-14 | local | Probe before `eb9f793` | Audit with `kind=runtime.entry_blocked_local_l2_selection` returned empty counts. |
| 2026-05-14 | local | Probe after `eb9f793` | `build_same_symbol_pairs()` candidate produced `pair_id='btcusdt:binance->bybit'`, `first_funding_timestamp_ms=15000`, and blocker returned `None` when primary tracked and dual-ready. |
| 2026-05-14 | local | Probe after `eb9f793` | Bybit and Bitget adapter HTTP paths returned `OrderFillReconciliation` instead of `None`. |
| 2026-05-14 | local | Probe after `eb9f793` | Bybit execution with `side=Sell` still returned reconciliation `side=buy`. |
| 2026-05-14 | local | Probe after `eb9f793` | Bybit `retCode=10001` returned `None` from parser instead of surfacing a business error. |
| 2026-05-14 | local | Probe after `eb9f793` | V2 schema-2 snapshot candidate without new fields loaded with `pair_id=''` and `first_funding_timestamp_ms=0`, then prewarm-blocked. |
| 2026-05-14 | local | RED tests post-`c378352` | Bitget `fillQty`-only response returned `None` (not in fallback chain). |
| 2026-05-14 | local | RED tests post-`c378352` | Bitget `size`-only response returned `None` (not in fallback chain). |
| 2026-05-14 | local | RED tests post-`c378352` | Bitget `filled_amount`-only response returned `None` (not in fallback chain). |
| 2026-05-14 | local | RED tests post-`c378352` | Bybit execution `side=Hold` did not raise; silently treated as SELL. |
| 2026-05-14 | local | RED tests post-`c378352` | Bybit execution `side=buy` (lowercase) did not raise; silently treated as SELL. |
| 2026-05-14 | local | RED tests post-`c378352` | Bybit order status `side` missing did not raise; defaulted to "Buy". |
| 2026-05-14 | local | RED tests post-`c378352` | Bybit order status `side=Hold` did not raise; silently treated as SELL. |
| 2026-05-14 | local | Manual audit post-`c378352` | BUG_INDEX: "partially fixed; residual open" vs BUG doc: "fixed; all closed". |
| 2026-05-14 | local | GREEN tests post-fix | All 7 RED counterexamples → GREEN. Full suite 2080 passed. |

## Occurrence History

| Cycle ID | Hosts | Occurrence Count | First Seen | Last Seen | Notes |
|---|---|---:|---|---|---|
| prompt-1/root-fix | local/cloud review | 1 | 2026-05-14 | 2026-05-14 | Prompt allowed "copy V1" without forcing V1 data-contract proof and real red tests. |
| commit-72ae905 | local | 4 | 2026-05-14 | 2026-05-14 | Local-L2, reconciliation, Bitget guard, audit were reviewed; three remained incomplete, Bitget guard mostly passed. |
| commit-eb9f793 | local | 3 residual | 2026-05-14 | 2026-05-14 | Most root fixes advanced; residual Bybit side, Bybit retCode, old/external V2 snapshot enrichment. |

## Failed / Ineffective Attempts

| Date | Attempt | Commit/Config | Result | Why Ineffective |
|---|---|---|---|---|
| 2026-05-14 | First local-L2 blocker fix using `pair_id`/`created_at_ms` | pre-`72ae905` working state | no root closure | Real `CandidateInput` had neither field, so production path did not match the test path. |
| 2026-05-14 | Add `fetch_order_status` | pre-`72ae905` working state | no effect on engine reconciliation | Engine calls `fetch_order_fill_reconciliation`; new method was a parallel API. |
| 2026-05-14 | Adapter override to call transport status | `72ae905` | half-effective | Request paths were closer, but transport parser lacked `OrderFillReconciliation` import and swallowed parser exceptions as `None`. |
| 2026-05-14 | Tests with fake candidate / monkeypatched status | `72ae905` | false green | Tests bypassed the real objects and real parser being validated. |
| 2026-05-14 | Add CandidateInput fields and parser tests | `eb9f793` | mostly effective | Still misses old V2 snapshot enrichment and real Bybit execution side/retCode semantics. |
| 2026-05-14 | Commit message claimed "Bybit execution side/retCode, Bitget UTA field completeness" | `c378352` | incomplete | Claimed closure but: (a) Bitget quantity fallback still missing fillQty/size/filled_amount from V1 chain; (b) Bybit execution/order-status side still treated invalid values as SELL (fail-open); (c) BUG_INDEX said "residual open" while BUG doc said "fixed". |

## New Counterexamples (this cycle, post-`c378352`)

| Fingerprint | V1 Source | V2 Gap | Test Name | RED→GREEN |
|---|---|---|---|---|
| `bitget.quantity_fallback.fillQty.single_field` | bitget.rs:2519 | fillQty not in V2 fallback chain | `test_bitget_quantity_fallback...[fillQty]` | RED → GREEN |
| `bitget.quantity_fallback.size.single_field` | bitget.rs:2521 | size not in V2 fallback chain | `test_bitget_quantity_fallback...[size]` | RED → GREEN |
| `bitget.quantity_fallback.filled_amount.single_field` | bitget.rs:2520 | filled_amount not in V2 fallback chain | `test_bitget_quantity_fallback...[filled_amount]` | RED → GREEN |
| `bybit.execution.side.invalid.fail_closed` | bybit.rs:3973-3979 | Invalid side (Hold) silently treated as SELL | `test_redlight_execution_side_invalid_hold_raises` | RED → GREEN |
| `bybit.execution.side.lowercase.fail_closed` | bybit.rs:3973-3979 | Lowercase "buy" silently treated as SELL | `test_redlight_execution_side_lowercase_buy_raises` | RED → GREEN |
| `bybit.order_status.side.missing.fail_closed` | bybit.rs:3973-3979 | Missing side defaulted to "Buy" | `test_redlight_order_status_side_missing_with_qty_raises` | RED → GREEN |
| `bybit.order_status.side.invalid.fail_closed` | bybit.rs:3973-3979 | Invalid side (Hold) silently treated as SELL | `test_redlight_order_status_side_invalid_hold_raises` | RED → GREEN |
| `bug-ledger.status.index-detail-consistency` | — | BUG_INDEX vs BUG doc contradictory status | Manual audit | Inconsistent → Consistent |

## Fix Plan

### Completed in `eb9f793`

- Add `CandidateInput.pair_id`, `funding_timestamp_ms`, and `first_funding_timestamp_ms`.
- Fill candidate fields in `build_same_symbol_pairs()`.
- Preserve fields in V1 compatibility conversion.
- Use V1-style local-L2 prewarm calculation in runtime: `remaining_ms = first_funding_timestamp_ms - now_ms`.
- Compute non-empty stable `pair_id` in local-L2 blocked journal payloads.
- Add `OrderFillReconciliation` import and stop swallowing arbitrary parser exceptions.
- Implement Bybit two-step reconciliation: orderLinkId to orderId, then execution list aggregation.
- Implement Bitget positive-quantity reconciliation with multi-key price/fee fallback.
- Track and fix the dry-run audit script.

### Residual Required Follow-Up (all closed in working tree post-`c378352`)

- ~~Fix Bybit execution-list side parsing instead of hardcoding `Side.BUY`.~~ Closed: `_parse_bybit_execution_list` and `_parse_order_status_bybit` now only accept "Buy"/"Sell" case-sensitively (V1 parity). Invalid/missing side with qty>0 raises TransportError REQUEST_REJECTED.
- ~~Call `_require_bybit_success` on Bybit order realtime and execution-list payloads.~~ Closed: `_fetch_order_status_bybit` already calls `_require_bybit_reconciliation_success` (added in `eb9f793` / `c378352`). retCode=110001 returns None; other nonzero raises TransportError.
- ~~Fix Bitget quantity fallback: add `fillQty`, `filled_amount`, `size` to chain.~~ Closed: all V1 fields now in fallback (cumExecQty → baseVolume → filledQty → fillQty → filled_amount → size → fillSz).
- ~~Decide fail-closed vs enrichment behavior for schema-version-2 snapshots.~~ Closed: `_dict_to_snapshot()` derives `pair_id` and `first_funding_timestamp_ms` from candidate fields plus quote timestamps; if timestamp evidence is unavailable, the candidate is marked blocked with `missing_candidate_identity_or_funding_timestamp`.
- ~~Strengthen red tests to use full `BybitAdapter.fetch_order_fill_reconciliation()` HTTP mock path.~~ Done: `TestBybitExecutionSideRedLight` + `TestBybitOrderStatusSideRedLight` + existing `TestBybitAdapterHttpRedLight` cover execution-side, order-status-side, and adapter HTTP path.
- ~~Fix BUG_INDEX/BUG doc inconsistency.~~ Closed: both now read "fixed".

## Implemented Fix

- Commit: `72ae905`
- Summary: First root-fix attempt added stable candidate pair id helper, local-L2 selection gate, adapter-level reconciliation overrides, and Bitget metadata guard. It was only partially effective.

- Commit: `eb9f793`
- Summary: Added candidate parity fields, V1 compatibility preservation, V1-style local-L2 prewarm, Bybit execution-list aggregation, Bitget quantity-positive reconciliation, and tracked audit script with `kind/payload` support.

- Commit: `c378352`
- Summary: Added snapshot candidate enrichment, Bybit execution side/retCode guard, Bitget UTA field completeness. Claimed closure but residual gaps remained in Bitget quantity fallback fields and Bybit fail-closed side validation.

- Working tree (post-`c378352`, 2026-05-14)
- Summary: Added Bitget quantity fallback fields fillQty/filled_amount/size (V1 parity); made Bybit execution-list and order-status side parsing fail-closed (only "Buy"/"Sell" accepted, invalid→TransportError); fixed BUG_INDEX/BUG doc status inconsistency; added 18 regression test cases (7 parameterized Bitget quantity + 11 Bybit side validation) covering all counterexamples.

## Verification

| Date | Environment | Command / Evidence | Result |
|---|---|---|---|
| 2026-05-14 | local | `pytest tests/test_entry_local_l2.py::TestEntryLocalL2SelectionBlockerRealCandidateInput tests/test_venues_transport.py::TestBybitParseOrderStatusRedLight tests/test_venues_transport.py::TestBitgetParseOrderStatusRedLight tests/test_offline_analysis.py::TestDryRunAuditRedLight -q` | `19 passed` |
| 2026-05-14 | local | `pytest tests/test_venues_transport.py tests/test_venues_contract.py tests/test_entry_sync.py tests/test_runtime_entry_flow.py tests/test_entry_local_l2.py tests/test_runtime_maker_event_local_l2.py tests/test_recovery_reconciliation.py -q` | `439 passed` |
| 2026-05-14 | local | `pytest -q` | `2054 passed, 2 skipped, 1 warning` |
| 2026-05-14 | local | `pytest tests/test_venues_transport.py::TestBitgetAdapterL2MetadataGuard tests/test_venues_transport.py::TestBitgetL2Guard -q` | `7 passed` |
| 2026-05-14 | local | GitNexus `detect_changes` compare `HEAD~1` | risk `medium`; 32 changed symbols; 4 affected processes |
| 2026-05-14 | local | Bybit adapter HTTP mock with positive executions | returned reconciliation, but sell execution still produced `side=buy` |
| 2026-05-14 | local | RED-LIGHT only: `TestBitgetQuantityFallbackRedLight + TestBybitExecutionSideRedLight + TestBybitOrderStatusSideRedLight` | `11 passed, 7 failed` (fillQty, size, filled_amount, Hold x2, lowercase buy, missing side) |
| 2026-05-14 | local | Same tests after fixes | `18 passed` (all 7 failures turned GREEN) |
| 2026-05-14 | local | Full regression: `pytest tests/test_venues_transport.py tests/test_entry_local_l2.py::TestEntryLocalL2SelectionBlockerRealCandidateInput tests/test_venues_transport.py::TestBybitAdapterHttpRedLight tests/test_venues_transport.py::TestBitgetAdapterHttpRedLight -q` | `215 passed` |
| 2026-05-14 | local | `pytest -q` (post-fixes) | `2080 passed` (+18 test cases since `c378352`) |
| 2026-05-14 | local | GitNexus `detect_changes` on unstaged docs/code fix | risk low; changed files only |
| 2026-05-14 | local | BUG_INDEX vs BUG doc consistency audit | Both now read "fixed" — consistent |

## Regression Watch

- `runtime.entry_blocked_local_l2_selection` with reason `entry_local_l2_waiting_for_prewarm_window` on candidates that should be inside prewarm window.
- Blocked journal events with empty `pair_id`.
- `order.uncertain` or reconciliation `long_status=uncertain` after ACK-only maker responses that have client order ids.
- Bybit reconciliation fills where expected sell-side execution is recorded as buy.
- Bybit nonzero `retCode` appearing as `None` rather than a transport/business error.
- Bitget `runtime.local_l2_snapshot_error` containing exchange `400172`, which would indicate local metadata guard regression.
- Dry-run audit counts empty while journal contains matching `kind` records.

## Similar Bugs

- V1: [BUG-20260509-live-entry-finalist-churn-and-readiness](/media/wl/新加卷/codex/LightFee/docs/bugs/BUG-20260509-live-entry-finalist-churn-and-readiness.md)
- V1: [BUG-20260502-aster-headroom-review-observability-local-l2](/media/wl/新加卷/codex/LightFee/docs/bugs/BUG-20260502-aster-headroom-review-observability-local-l2.md)

## Related Commits

- `72ae905` `fix: V1 root-fix — entry local L2 selection gate, order cid reconciliation, Bitget L2 metadata guard`
- `eb9f793` `fix: V1 parity — local L2 prewarm, reconciliation, audit closure`
- `c378352` `fix: V1 parity — snapshot candidate enrichment, Bybit execution side/retCode, Bitget UTA field completeness`
- Working tree (next commit) `fix: V1 parity — Bitget quantity fallback V1 fields, Bybit fail-closed side, ledger consistency`
