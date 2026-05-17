# Production Entry Local-L2 Pending/Reconcile Root Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Root-fix the current production no-entry condition by separating primary-tracking admission from true local-L2 readiness, stabilizing dual-ready book state, and closing pending/reconcile residuals without unsafe duplicate hedges.

**Architecture:** Keep strategy admission in `LiveRuntime`, entry local-L2 readiness in `entry_local_l2.py`, exchange evidence in venue adapters, and pending-entry state transitions in runtime/reconciliation. V1 parity is used for state-machine semantics; exchange-specific order, fill, min-notional, and local-order-book rules are verified against official venue docs.

**Deployment boundary:** 本轮默认只做到本地实现、测试、GitNexus detect_changes、文档更新和生产只读验证。不得部署、重启服务或下单，除非用户明确授权。

**Tech Stack:** Python 3.11/3.12, pytest, pytest-asyncio, GitNexus MCP, LightFeeV2 journal/current-state, production read-only probes, V1 Rust references under `/media/wl/新加卷/codex/LightFee`.

---

## Required Context

- Spec: `docs/superpowers/specs/2026-05-17-production-entry-local-l2-pending-reconcile-root-fix-design.md`
- Bug ledger: `docs/bugs/daily/2026-05-17.md`
- Prior L2 bug ledger: `docs/bugs/daily/2026-05-16.md`
- V1/V2 parity loop: `docs/bugs/BUG-20260514-v2-v1-parity-root-fix-loop.md`
- Current blockers:
  - `entry_local_l2_waiting_for_primary_tracking`
  - `entry_local_l2_waiting_for_dual_ready`
  - `pending_entry.missing_hedge_detected`
  - `pending_entry.hedge_submit_result:min_notional_rejected`
  - `order.reconcile_result:uncertain`
- Current production evidence paths:
  - `/opt/lightfee-v2/runtime/live-events.jsonl`
  - `/opt/lightfee-v2/runtime/live-state.json`
  - `/opt/lightfee-v2/runtime/opportunity-input-snapshot.json`

Before editing any function, class, or method, follow `AGENTS.md`: run GitNexus impact analysis for the exact target symbol, report risk, then write failing tests first.

## File Structure

| Path | Responsibility |
|---|---|
| `lightfee/engine/runtime.py` | Selection order, primary/shadow tracking classification, pending-entry reconciliation state transitions, no-entry diagnostics |
| `lightfee/engine/entry_local_l2.py` | Per-leg book readiness mapping, session readiness snapshots, stable not-ready reason taxonomy |
| `lightfee/engine/reconciliation.py` | Order/fill/position evidence merge and terminal/uncertain classification |
| `lightfee/venues/hyperliquid.py` | Hyperliquid `cloid`/order-status/historical-order/fill evidence and min-notional metadata |
| `lightfee/venues/transport.py` | Venue-agnostic order submit diagnostics, min-notional/precision preflight, OKX/Bybit evidence helpers if adapter-local hooks are insufficient |
| `scripts/analyze_production_blockers.py` | Current-vs-history blocker summarization and pending/reconcile decision tree |
| `tests/test_entry_local_l2.py` | Unit tests for primary-tracking classification and readiness reason taxonomy |
| `tests/test_runtime_maker_event_local_l2.py` | Runtime tests for tracked HOT books passing the existing gate and untracked candidates not masking ready pairs |
| `tests/test_live_entry_hedge_root_fix.py` | Pending-entry stale-inflight, min-notional residual, and finalization regression tests |
| `tests/test_offline_analysis.py` | Analyzer regression tests for `last_2h`, `last_24h`, `run_window`, and issue classification |
| `docs/bugs/daily/2026-05-17.md` | Ledger updates after implementation and production verification |

## Task 1: Preserve the Current Failure as Tests and Analyzer Evidence

**Files:**
- Modify: `scripts/analyze_production_blockers.py`
- Modify: `tests/test_offline_analysis.py`
- Create or modify fixture: `tests/fixtures/journals/production_entry_l2_pending_reconcile_20260517.jsonl`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "analyze_production_blockers", direction: "upstream", repo: "LightFeeV2"})
```

Expected: risk is LOW or MEDIUM because this is offline tooling. If GitNexus cannot resolve the script symbol, use `gitnexus_query({query: "production blocker analyzer", repo: "LightFeeV2"})` and record the result in the implementation notes.

- [ ] **Step 2: Add a compact fixture**

Create a small fixture with only sanitized event kinds and payload fields. It must include these exact facts:

```json
{"ts_ms":1778985600000,"kind":"runtime.entry_blocked_local_l2_selection","payload":{"reason":"entry_local_l2_waiting_for_primary_tracking","pair_id":"polyxusdt:bybit->hyperliquid","symbol":"POLYXUSDT"}}
{"ts_ms":1778985601000,"kind":"runtime.entry_blocked_local_l2_selection","payload":{"reason":"entry_local_l2_waiting_for_dual_ready","pair_id":"chipusdt:binance->aster","symbol":"CHIPUSDT"}}
{"ts_ms":1778985602000,"kind":"runtime.entry_local_l2_readiness_diagnostics","payload":{"entry_local_l2_primary_not_ready_reason_totals":{"book_bootstrapping":13,"book_rebuilding":1},"detail_samples":[{"pair_id":"chipusdt:binance->aster","venue":"aster","symbol":"CHIPUSDT","reason":"book_bootstrapping","book_status":"BOOTSTRAPPING"}]}}
{"ts_ms":1778985603000,"kind":"pending_entry.missing_hedge_detected","payload":{"entry_id":"entry-sample-STABLEUSDT","symbol":"STABLEUSDT","hedge_venue":"hyperliquid","missing_quantity":650.0}}
{"ts_ms":1778985604000,"kind":"pending_entry.hedge_submit_result","payload":{"entry_id":"entry-sample-STABLEUSDT","symbol":"STABLEUSDT","venue":"hyperliquid","outcome":"rejected","reason":"min_notional_rejected","notional":3.12,"min_notional":10.0}}
{"ts_ms":1778985605000,"kind":"order.reconcile_result","payload":{"entry_id":"entry-sample-POLYXUSDT","symbol":"POLYXUSDT","venue":"hyperliquid","outcome":"uncertain"}}
```

- [ ] **Step 3: Add analyzer test expectations**

Add a test that asserts the analyzer returns all three windows and the current classification:

```python
def test_production_blocker_analyzer_classifies_l2_and_pending_residuals():
    from scripts.analyze_production_blockers import analyze_event_file

    result = analyze_event_file(
        "tests/fixtures/journals/production_entry_l2_pending_reconcile_20260517.jsonl",
        now_ms=1778989200000,
    )

    assert result["windows"]["last_2h"]["entry_local_l2_waiting_for_primary_tracking"] == 1
    assert result["windows"]["last_2h"]["entry_local_l2_waiting_for_dual_ready"] == 1
    assert result["windows"]["last_2h"]["pending_entry.hedge_submit_result:min_notional_rejected"] == 1
    assert result["classification"]["entry_local_l2_waiting_for_primary_tracking"] == "current_new_high_frequency"
    assert result["classification"]["entry_local_l2_waiting_for_dual_ready"] == "old_issue_recurred_with_book_reason"
    assert result["classification"]["min_notional_rejected"] == "exchange_rule_residual"
```

- [ ] **Step 4: Run the analyzer tests**

Run:

```bash
pytest tests/test_offline_analysis.py -k "production_blocker_analyzer_classifies_l2_and_pending_residuals" -q
```

Expected before implementation: FAIL because the analyzer does not yet expose the classification fields.

## Task 2: Separate Primary Tracking Admission from Local-L2 Readiness Failure

**Files:**
- Modify: `lightfee/engine/runtime.py`
- Modify: `tests/test_entry_local_l2.py`
- Modify: `tests/test_runtime_maker_event_local_l2.py`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "LiveRuntime._select_entry_candidates", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "LiveRuntime._entry_local_l2_selection_blocker", direction: "upstream", repo: "LightFeeV2"})
```

Expected: risk is MEDIUM or higher because this affects live candidate dispatch. Report the risk before editing.

- [ ] **Step 2: Add a failing runtime test for untracked noise**

Add a test proving one untracked tradeable candidate does not hide one tracked and ready candidate:

```python
@pytest.mark.asyncio
async def test_primary_tracking_admission_does_not_mask_ready_primary_candidate(live_runtime_factory):
    runtime = live_runtime_factory()
    now_ms = 1778985600000

    ready = runtime._test_candidate(
        symbol="POLYXUSDT",
        long_venue="bybit",
        short_venue="hyperliquid",
        pair_id="polyxusdt:bybit->hyperliquid",
        first_funding_timestamp_ms=now_ms + 180_000,
    )
    untracked = runtime._test_candidate(
        symbol="BANANAUSDT",
        long_venue="bybit",
        short_venue="hyperliquid",
        pair_id="bananausdt:bybit->hyperliquid",
        first_funding_timestamp_ms=now_ms + 180_000,
    )

    runtime._tracked_primary_pair_ids = {ready.pair_id}
    runtime._mark_test_books_hot(ready, now_ms=now_ms)
    runtime._refresh_entry_l2_session_readiness(now_ms=now_ms)

    selected = runtime._select_entry_candidates([untracked, ready], now_ms=now_ms)

    assert [candidate.pair_id for candidate in selected] == [ready.pair_id]
    assert runtime._last_no_entry_diagnostics["selection_bucket_counts"]["not_primary_tracked"] == 1
```

If the repository uses different test helpers, define local fake candidates in the test file and keep the assertion semantics identical.

- [ ] **Step 3: Implement the classification**

Change selection diagnostics so that:

```python
if pair_id not in self._tracked_primary_pair_ids:
    return EntrySelectionBlocker(
        code="not_primary_tracked",
        public_reason="entry_local_l2_waiting_for_primary_tracking",
        counts_as_l2_not_ready=False,
    )
```

The exact data type can be a dataclass or existing dict style, but the behavior must satisfy:

- `not_primary_tracked` is an admission bucket.
- `entry_local_l2_waiting_for_dual_ready` remains a true readiness bucket.
- no-entry diagnostics report both counts separately.

- [ ] **Step 4: Run targeted tests**

Run:

```bash
pytest tests/test_entry_local_l2.py tests/test_runtime_maker_event_local_l2.py -k "primary_tracking or local_l2_selection" -q
```

Expected after implementation: PASS.

## Task 3: Stabilize Dual-Ready Book State and Reason Taxonomy

**Files:**
- Modify: `lightfee/engine/entry_local_l2.py`
- Modify: `lightfee/engine/runtime.py`
- Modify: `tests/test_entry_local_l2.py`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "apply_book_readiness_to_leg", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "EntryLocalL2Session.both_legs_ready", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Add reason-taxonomy tests**

Add table-driven assertions:

```python
@pytest.mark.parametrize(
    ("book_status", "bid", "ask", "observed_at_ms", "expected_reason"),
    [
        ("BOOTSTRAPPING", 1.0, 1.1, 1778985600000, "book_bootstrapping"),
        ("REBUILDING", 1.0, 1.1, 1778985600000, "book_rebuilding"),
        ("HOT", 1.0, 1.1, 1778985000000, "stale_book"),
        ("HOT", 1.1, 1.0, 1778985600000, "crossed_or_locked_book"),
    ],
)
def test_entry_l2_not_ready_reasons_are_specific(
    book_status,
    bid,
    ask,
    observed_at_ms,
    expected_reason,
):
    leg = EntryLocalL2LegSession(venue="binance", symbol="CHIPUSDT")
    book = make_test_l2_book(
        status=book_status,
        bid=bid,
        ask=ask,
        observed_at_ms=observed_at_ms,
    )

    result = apply_book_readiness_to_leg(
        leg,
        book,
        now_ms=1778985600000,
        stale_after_ms=300_000,
    )

    assert result.ready is False
    assert result.reason == expected_reason
    assert result.reason != "book_hot"
```

- [ ] **Step 3: Keep HOT/fresh as ready only**

Update readiness mapping so a HOT book with fresh timestamp, non-empty bid/ask, and uncrossed top of book returns:

```python
BookReadinessResult(
    ready=True,
    reason="ready",
    detail="local_l2_book_hot_fresh",
    book_status="HOT",
)
```

Any HOT book that is not actually usable must map to the precise failing reason: `stale_book`, `book_empty_side`, `crossed_or_locked_book`, or `book_timestamp_missing`.

- [ ] **Step 4: Run targeted tests**

Run:

```bash
pytest tests/test_entry_local_l2.py -k "not_ready_reasons_are_specific or hot_fresh" -q
```

Expected after implementation: PASS and no not-ready diagnostic sample has `reason="book_hot"`.

## Task 4: Add Pending/Reconcile Terminal Policy for Stale Inflight and Min-Notional Residuals

**Files:**
- Modify: `lightfee/engine/runtime.py`
- Modify: `lightfee/engine/reconciliation.py`
- Modify: `lightfee/venues/hyperliquid.py`
- Modify: `tests/test_live_entry_hedge_root_fix.py`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "LiveRuntime._reconcile_pending_state", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "LiveRuntime._drive_missing_hedge_live", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "OrderReconciler.reconcile_position", direction: "upstream", repo: "LightFeeV2"})
```

Expected: likely HIGH because this touches live order repair. Report the risk before editing and keep the patch narrow.

- [ ] **Step 2: Add stale-inflight test**

Add a test where:

- pending maker fill exists;
- `hedge_inflight` is set;
- Hyperliquid order query says missing;
- Hyperliquid fills query says no fill;
- Hyperliquid position query says zero;
- runtime clears stale inflight once and submits one legal hedge.

Assertion skeleton:

```python
@pytest.mark.asyncio
async def test_pending_reconcile_clears_stale_hedge_inflight_after_negative_evidence():
    runtime = make_pending_entry_runtime()
    pending = runtime._add_test_pending_entry(
        entry_id="entry-sample-POLYXUSDT",
        symbol="POLYXUSDT",
        maker_venue="bybit",
        hedge_venue="hyperliquid",
        maker_leg_filled=425.0,
        hedge_leg_filled=0.0,
        hedge_inflight="0x11111111111111111111111111111111",
    )
    runtime.fake_reconciler.set_hedge_evidence(
        order_status="missing",
        fills_quantity=0.0,
        position_quantity=0.0,
    )

    await runtime._reconcile_pending_state(now_ms=1778985600000)

    assert pending.hedge_inflight != "0x11111111111111111111111111111111"
    assert runtime.fake_hedge_adapter.submit_count == 1
    assert runtime.journal_contains("pending_entry.hedge_inflight_cleared")
```

- [ ] **Step 3: Add min-notional residual test**

Add a test for STABLE-like residual:

```python
@pytest.mark.asyncio
async def test_pending_reconcile_does_not_retry_hedge_below_hyperliquid_min_notional():
    runtime = make_pending_entry_runtime()
    pending = runtime._add_test_pending_entry(
        entry_id="entry-sample-STABLEUSDT",
        symbol="STABLEUSDT",
        maker_venue="okx",
        hedge_venue="hyperliquid",
        maker_leg_filled=78.0,
        hedge_leg_filled=0.0,
        hedge_reference_price=0.004,
    )
    runtime.fake_hedge_adapter.set_min_notional("STABLEUSDT", 10.0)

    await runtime._reconcile_pending_state(now_ms=1778985600000)

    assert runtime.fake_hedge_adapter.submit_count == 0
    assert pending.repair_state == "hedge_residual_below_min_notional"
    assert runtime.journal_contains("pending_entry.hedge_residual_below_min_notional")
```

- [ ] **Step 4: Implement terminal policy**

Add a small explicit policy:

```python
if hedge_notional < hedge_min_notional:
    pending.repair_state = "hedge_residual_below_min_notional"
    journal(
        "pending_entry.hedge_residual_below_min_notional",
        entry_id=entry_id,
        venue=hedge_venue,
        symbol=pending.symbol,
        hedge_notional=hedge_notional,
        hedge_min_notional=hedge_min_notional,
    )
    return False
```

The policy must not mark the entry opened. It must keep enough state for a later aggregate/flatten/manual repair path.

- [ ] **Step 5: Run targeted tests**

Run:

```bash
pytest tests/test_live_entry_hedge_root_fix.py -k "stale_hedge_inflight or min_notional" -q
```

Expected after implementation: PASS.

## Task 5: Verify Venue-Document Contracts in Adapter/Reconciler Tests

**Files:**
- Modify: `tests/test_live_entry_hedge_root_fix.py`
- Modify: `tests/test_venues_transport.py`
- Modify: `tests/test_recovery_reconciliation.py`
- Modify: `lightfee/venues/hyperliquid.py`
- Modify: `lightfee/venues/transport.py`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "HyperliquidAdapter.fetch_order_fill_reconciliation", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "VenueTransport.place_order", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Add Hyperliquid min-notional and cloid tests**

The test must prove:

- `MinTradeNtl` maps to `min_notional_rejected`;
- `cloid` is preserved in order query evidence;
- missing order plus zero position is not treated as filled.

Run:

```bash
pytest tests/test_venues_transport.py tests/test_live_entry_hedge_root_fix.py -k "hyperliquid and (min_notional or cloid or missing_order)" -q
```

- [ ] **Step 3: Add OKX/Bybit async ACK tests**

The tests must prove:

- Bybit place order ACK is accepted/resting, not final fill.
- Bybit order history delay does not override fresher websocket/open-order evidence.
- OKX `clOrdId` lookup uses `ordId` first when both are available and falls back to fills/history.

Run:

```bash
pytest tests/test_recovery_reconciliation.py tests/test_venues_transport.py -k "bybit_ack or okx_clordid or order_history" -q
```

- [ ] **Step 4: Record doc citations in code comments only where helpful**

Acceptable short comments:

```python
# Hyperliquid returns MinTradeNtl for perp orders below $10; treat it as deterministic.
```

Do not paste long documentation excerpts into code or tests.

## Task 6: Production Read-Only Verification and Bug Ledger Closeout

**Files:**
- Modify: `docs/bugs/daily/2026-05-17.md`
- Modify: `docs/bugs/BUG_INDEX.md`

- [ ] **Step 1: Run local test suite slice**

Run:

```bash
pytest tests/test_entry_local_l2.py tests/test_runtime_maker_event_local_l2.py tests/test_live_entry_hedge_root_fix.py tests/test_recovery_reconciliation.py tests/test_venues_transport.py tests/test_offline_analysis.py -q
```

Expected: all targeted tests PASS.

- [ ] **Step 2: Run GitNexus change detection**

Run:

```text
gitnexus_detect_changes({repo: "LightFeeV2", scope: "all"})
```

Expected: changed symbols match the planned runtime, entry-local-l2, reconciliation, venue, analyzer, and docs scope.

- [ ] **Step 3: Run production read-only probe**

Use absolute paths only:

```bash
python3 scripts/analyze_production_blockers.py \
  --events /opt/lightfee-v2/runtime/live-events.jsonl \
  --state /opt/lightfee-v2/runtime/live-state.json \
  --snapshot /opt/lightfee-v2/runtime/opportunity-input-snapshot.json \
  --windows last_2h,last_24h,run_window \
  --no-secrets
```

Expected output summary:

- sidecar snapshot fresh with 7 venues;
- live state running;
- no raw credentials;
- `entry_local_l2_waiting_for_primary_tracking`, `entry_local_l2_waiting_for_dual_ready`, pending/reconcile counts separated;
- current pending entries have a deterministic decision: hedge exists, hedge absent and repairable, or residual below min notional.

- [ ] **Step 4: Update bug ledger**

Update `docs/bugs/daily/2026-05-17.md` with:

- implementation commit hash;
- test result summary;
- production read-only verification row;
- whether CL-002 is fixed, partially fixed, or still open.

Update `docs/bugs/BUG_INDEX.md` Latest Outcome with the same final status.

## Self-Review Checklist

- [ ] The plan never asks an engineer to bypass local-L2 safety.
- [ ] The plan never treats REST ACK as final fill.
- [ ] `primary_tracking` and `dual_ready` are separate buckets.
- [ ] Hyperliquid `$10` min-notional residual has a terminal state.
- [ ] Current-vs-historical frequencies are preserved in docs and analyzer output.
- [ ] No cloud raw logs, account identifiers, SSH credentials, API keys, or secrets are committed.

