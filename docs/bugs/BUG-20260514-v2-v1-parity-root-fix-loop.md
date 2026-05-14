# BUG-20260514-v2-v1-parity-root-fix-loop

Status: fixed; all P1/P2 residual closed
Severity: high
Component: `entry-local-l2`, `sidecar-candidate-contract`, `order-reconciliation`, `bitget-l2-metadata`, `dryrun-audit`, `test-coverage`
Fingerprint: `v2.v1-parity.surface-copy.not-data-contract + fake-tests.green.real-path-red`
First Seen: 2026-05-14 +08:00 during V2 cloud/runtime log review and follow-up parity validation
First Seen Commit: `72ae905` was the first reviewed "root-fix" commit that still showed real-path gaps
Related Refactor: `V1-to-V2 execution and venue parity replication`
Fixed In: `eb9f793` (partial); working tree 2026-05-14 (full closure)
Verified In: local probes and tests on 2026-05-14 against working tree

## Summary

The V2 root-fix loop repeatedly produced green tests while real runtime paths still diverged from V1. The core problem was not that V1 behavior was wrong; the repeated failures came from copying visible V1 concepts without fully copying the V1 data contracts, adapter call chains, and failure semantics.

The latest commit `eb9f793` closes several major gaps, but the record remains open because Bybit reconciliation still has P1 semantic gaps and old/external V2 snapshots can still produce candidates without usable local-L2 prewarm fields.

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
| Candidate identity | Candidate has stable `pair_id` used across tracked opportunities, sessions, and final gate. | `src/execution_core/entry_local_l2.rs`, `src/execution_core/market_data.rs` | V2 `CandidateInput` originally had no `pair_id`; blocker used missing fields. |
| Local-L2 prewarm | Prewarm uses `remaining_ms = first_funding_timestamp_ms - now_ms`; candidate must be inside configured prewarm window. | `src/execution_core/market_data.rs:1518` | V2 used `created_at_ms`, then later only checked a timestamp field that real candidates did not have. |
| Primary/dual-ready gate | Candidate must be primary tracked and have dual-ready entry-local-L2 session. | `src/execution_core/market_data.rs:1681`, `src/execution_core/entry_local_l2_sessions.rs` | V2 first bypassed early selection; later added primary/session logic but data contract was incomplete. |
| Bybit reconciliation | Resolve order by `orderLinkId` when needed, then query `/v5/execution/list`, aggregate actual executions, and return None if total quantity is zero. | `src/live/bybit.rs:2820` | V2 first added unused status API; then parser lacked import and swallowed exceptions; latest fix aggregates but still hardcodes side and ignores Bybit nonzero retCode. |
| Bitget reconciliation | Query order detail by `orderId` or `clientOid`; return reconciliation only when filled quantity is positive. | `src/live/bitget.rs:2912` | V2 parser originally returned None due missing import/swallowed exception; latest fix mostly closes this. |
| Bitget L2 metadata guard | Load symbol metadata before orderbook; reject locally if metadata missing. | `src/live/bitget.rs` `symbol_meta`, `bitget_fetch_execution_liquidity_snapshot` | V2 transport guard only worked after metadata was already populated; adapter did not own the catalog guard. |
| Journal/audit format | V2 journal canonical fields are `kind` and `payload`. | `lightfee/persistence/journal.py` in V2 | Audit script read only `event` and `data`. |

## V1 to V2 Mapping

| V1 Concept | V2 Mapping | Current Status | Notes |
|---|---|---|---|
| `CandidateOpportunity.pair_id` | `CandidateInput.pair_id`, `make_candidate_pair_id()` | partially fixed | Pairing and V1 compat now preserve/fill; old V2 snapshots missing the field remain weak. |
| `CandidateOpportunity.first_funding_timestamp_ms` | `CandidateInput.first_funding_timestamp_ms` | partially fixed | Pairing and V1 compat now fill; `_dict_to_snapshot()` does not derive it from quotes if absent. |
| `candidate_in_entry_local_l2_prewarm_window()` | `LiveRuntime._entry_local_l2_selection_blocker()` | partially fixed | Runtime uses remaining-ms window now. |
| tracked primary opportunities | `LiveRuntime._tracked_primary_pair_ids` and `EntryLocalL2SessionRuntime` | mostly fixed | Selection/session path now exists. |
| Bybit `fetch_order_fill_reconciliation()` | `BybitAdapter.fetch_order_fill_reconciliation()` → `VenueTransport.fetch_order_status()` → `_fetch_order_status_bybit()` | partially fixed | Real HTTP path now returns fills; side and retCode semantics still open. |
| Bitget `fetch_order_fill_reconciliation()` | `BitgetAdapter.fetch_order_fill_reconciliation()` → `VenueTransport.fetch_order_status()` → `_parse_order_status_bitget()` | mostly fixed | Quantity-positive guard and multi-key fallback added. |
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

### Residual Required Follow-Up

- Fix Bybit execution-list side parsing instead of hardcoding `Side.BUY`.
- Call `_require_bybit_success` on Bybit order realtime and execution-list payloads; nonzero `retCode` should not silently become `None`.
- Decide fail-closed vs enrichment behavior for schema-version-2 snapshots whose candidates lack `pair_id` or `first_funding_timestamp_ms`.
- Strengthen red tests to use full `BybitAdapter.fetch_order_fill_reconciliation()` HTTP mock path, including sell executions and nonzero retCode.
- Strengthen snapshot tests to assert non-empty `pair_id` and positive `first_funding_timestamp_ms`, not just field existence.

## Implemented Fix

- Commit: `72ae905`
- Summary: First root-fix attempt added stable candidate pair id helper, local-L2 selection gate, adapter-level reconciliation overrides, and Bitget metadata guard. It was only partially effective.

- Commit: `eb9f793`
- Summary: Added candidate parity fields, V1 compatibility preservation, V1-style local-L2 prewarm, Bybit execution-list aggregation, Bitget quantity-positive reconciliation, and tracked audit script with `kind/payload` support.

## Verification

| Date | Environment | Command / Evidence | Result |
|---|---|---|---|
| 2026-05-14 | local | `pytest tests/test_entry_local_l2.py::TestEntryLocalL2SelectionBlockerRealCandidateInput tests/test_venues_transport.py::TestBybitParseOrderStatusRedLight tests/test_venues_transport.py::TestBitgetParseOrderStatusRedLight tests/test_offline_analysis.py::TestDryRunAuditRedLight -q` | `19 passed` |
| 2026-05-14 | local | `pytest tests/test_venues_transport.py tests/test_venues_contract.py tests/test_entry_sync.py tests/test_runtime_entry_flow.py tests/test_entry_local_l2.py tests/test_runtime_maker_event_local_l2.py tests/test_recovery_reconciliation.py -q` | `439 passed` |
| 2026-05-14 | local | `pytest -q` | `2054 passed, 2 skipped, 1 warning` |
| 2026-05-14 | local | `pytest tests/test_venues_transport.py::TestBitgetAdapterL2MetadataGuard tests/test_venues_transport.py::TestBitgetL2Guard -q` | `7 passed` |
| 2026-05-14 | local | GitNexus `detect_changes` compare `HEAD~1` | risk `medium`; 32 changed symbols; 4 affected processes |
| 2026-05-14 | local | Bybit adapter HTTP mock with positive executions | returned reconciliation, but sell execution still produced `side=buy` |
| 2026-05-14 | local | Audit probe with old and recent `kind/payload` events | counted only recent event |

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
