# V1/V2 Module Parity Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Audit and close remaining Rust V1 to Python V2 live-path drift with every gap assigned to its proper LightFeeV2 module.

**Architecture:** Keep V2's Python module boundaries. The plan first creates a parity matrix, then audits and fixes one module owner at a time. Runtime remains orchestration; business formulas stay in planner/risk/exit modules; venue quirks stay in venue adapters; persistence owns replayable state.

**Tech Stack:** Python 3.12, asyncio, dataclasses, pytest, pytest-asyncio, GitNexus MCP/CLI, existing LightFeeV2 modules, Rust V1 source under `/media/wl/新加卷/codex/LightFee`.

---

## Reference Documents

- Spec: `docs/superpowers/specs/2026-05-10-v1-v2-module-parity-closure-design.md`
- Production closure spec: `docs/superpowers/specs/2026-05-10-v1-production-closure-replication-design.md`
- Execution prompt: `docs/superpowers/prompts/2026-05-10-v1-production-closure-replication-execution-prompt.md`
- Rust V1 source: `/media/wl/新加卷/codex/LightFee`
- Python V2 target: `/media/wl/新加卷/codex/LightFeeV2`

## Mandatory Rules

- Do not edit a Python function/class/method before running GitNexus impact analysis for that symbol.
- Do not fix drift in `runtime.py` if the owning rule belongs in planner, venue, risk, close, recovery, or market-data modules.
- Do not mark a parity row fixed unless a focused test and a production caller/integration test prove it.
- Do not treat fake-adapter behavior as live behavior.
- Do not change thresholds, retry counts, stale windows, precision, or funding/risk semantics unless Rust V1 proves the value.
- Do not mechanically copy Rust structure when Python can express the same live behavior more cleanly.
- Do preserve every Rust live-path production patch semantically, even if the Python code is reorganized.
- Do not remove defensive Rust branches as "cleanup" unless the row is proven `deferred_non_live` or explicitly `approved_deviation`.
- For every cleaned-up Python abstraction, tests must cover the Rust branches it replaces.

## Global Implementation Standard

Every task follows this rule:

```text
Rust live behavior: 1:1 semantic parity
Python implementation: optimized structure and clearer module ownership
```

This applies to all modules, not only local L2. Entry residual protection, close reconciliation, venue error special cases, risk snapshot fallback, rate-limit reload, journal replay, and startup activation may all include Rust production patches. The worker may refactor those patches into cleaner Python helpers, but may not drop their behavior.

When a Rust section is large or patch-heavy:

1. Extract each live-path behavior into parity matrix rows.
2. Group related rows under the correct V2 owner module.
3. Write one focused test per behavior or per equivalent abstraction.
4. Implement the Python abstraction.
5. Add one integration test proving the production caller consumes the abstraction.

## Verification Commands

Run focused commands per task. Before reporting completion of the whole plan, run:

```bash
rtk pytest -q
rtk pytest tests/test_live_full_closure.py::TestLiveFullClosure::test_housekeeping_exports_metrics -q -W error
rtk python3 -m compileall lightfee tests
```

If a task changes a module with async tests, also run the touched test file with:

```bash
rtk pytest <test-file> -q -W error
```

---

### Task 1: Create And Seed The Module Parity Matrix

**Files:**
- Create: `docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md`
- Read: `docs/superpowers/specs/2026-05-10-v1-v2-module-parity-closure-design.md`
- Read: `/media/wl/新加卷/codex/LightFee/src/main.rs`
- Read: `lightfee/engine/runtime.py`

- [ ] **Step 1: Refresh repository intelligence**

Run:

```bash
rtk npx gitnexus analyze
```

Expected: analyzer reports up to date or successfully indexes the repository.

- [ ] **Step 2: Create the parity matrix file**

Create `docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md` with this exact structure:

```markdown
# V1/V2 Module Parity Matrix

**Rust source:** `/media/wl/新加卷/codex/LightFee`

**Python target:** `/media/wl/新加卷/codex/LightFeeV2`

**Status values:** `open`, `in_progress`, `fixed`, `deferred_non_live`, `approved_deviation`

| ID | Owner Module | Rust Source | Python Source | Live Caller | Category | Observed Drift | Required Parity | Test File | Priority | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
```

- [ ] **Step 3: Seed known rows**

Add at least these rows:

```markdown
| RT-001 | `lightfee/engine/runtime.py` | `/media/wl/新加卷/codex/LightFee/src/main.rs:285-410` | `lightfee/engine/runtime.py:279-323` | `lightfee/apps/live.py` | loop lanes | V2 lacks Rust maker-event lane, rate-limit reload interval, SIGHUP reload, jittered evidence-backed error recording | Runtime must schedule or explicitly defer each Rust live lane with module-owned service boundaries | `tests/test_live_full_closure.py` | P1 | open |
| RT-002 | `lightfee/engine/runtime.py` | `/media/wl/新加卷/codex/LightFee/src/main.rs:499-713` | `lightfee/apps/live.py`, `lightfee/engine/runtime.py:start` | `lightfee/apps/live.py` | live startup | V2 wires executors but does not run V1 prewarm and phased private/market/local-L2 activation | Live startup must prewarm/activate adapters with timeout/budget semantics or document approved deviation | `tests/test_live_startup_preflight.py` | P1 | open |
| EN-001 | `lightfee/engine/execution_planner.py` | `/media/wl/新加卷/codex/LightFee/src/execution_core/entry_execution_planner.rs:49-208` | `lightfee/engine/runtime.py:_dispatch_entry` | `LiveRuntime.tick` | entry route | Planner exists but runtime fixes `maker_leg` and `EntryType` | Runtime must call planner and route through entry executor with V1 route/maker semantics | `tests/test_runtime_entry_flow.py`, `tests/test_entry_planner.py` | P0 | open |
| RK-001 | `lightfee/venues/` + `lightfee/engine/risk_actions.py` | `/media/wl/新加卷/codex/LightFee/src/health.rs`, `/media/wl/新加卷/codex/LightFee/src/live/*.rs` | `lightfee/core/contracts.py`, `lightfee/venues/*.py`, `lightfee/engine/runtime.py:231-263` | `LiveRuntime.tick_active_positions` | risk snapshot | Capability truth and real account risk snapshot implementation are not aligned | Venue adapters must expose real risk snapshots where V1 supports them and unsupported policy must be handled in risk_actions | `tests/test_risk_actions.py`, `tests/test_venues_contract.py` | P0 | open |
| RC-001 | `lightfee/engine/reconciliation.py` | `/media/wl/新加卷/codex/LightFee/src/engine/recovery.rs` | `lightfee/engine/reconciliation.py:64-135` | `LiveRuntime._reconcile_pending_state` | reconciliation | Old positional constructor can hide tests that do not query both legs | Constructor and tests must prove adapter-map and fixed-leg reconciliation query both legs correctly | `tests/test_recovery_reconciliation.py` | P1 | open |
| TS-001 | `tests/test_live_full_closure.py` | `/media/wl/新加卷/codex/LightFee/src/main.rs:316-354` | `tests/test_live_full_closure.py:test_housekeeping_exports_metrics` | test caller | async test | Test calls async `_post_tick_housekeeping` without await; `-W error` fails | Test must await async housekeeping and assert reconciliation/export side effects | `tests/test_live_full_closure.py` | P1 | open |
| VN-001 | `lightfee/venues/transport.py` and per-venue modules | `/media/wl/新加卷/codex/LightFee/src/live/gate.rs`, `/media/wl/新加卷/codex/LightFee/src/live/hyperliquid.rs` | `lightfee/venues/transport.py`, `lightfee/venues/*.py` | `EntrySyncExecutor`, `CloseExecutor`, `OrderReconciler` | venue errors | Reduce-only empty-position success, pending-conflict retry, and asset-index placeholders require V1 parity review | Venue adapters must classify terminal success/retry/uncertain/rejected exactly as V1 | `tests/test_venues_contract.py`, `tests/test_venues_transport.py` | P0 | open |
| QL-001 | all live modules | Rust live-path production patches across engine, venues, market data, recovery, rate limit, persistence | all owning Python modules | live runtime callers | code quality | Rust patch-heavy code may tempt mechanical copy or accidental deletion | Python may reorganize and simplify code, but each Rust live-path patch behavior must be preserved by module-owned tests | module-specific tests | P0 | open |
```

- [ ] **Step 4: Verify matrix formatting**

Run:

```bash
rtk python3 -m compileall lightfee tests
```

Expected: compileall succeeds. This does not validate markdown, but confirms no accidental code change was introduced.

---

### Task 2: Fix Async Housekeeping Test Coverage

**Files:**
- Modify: `tests/test_live_full_closure.py`
- Read: `lightfee/engine/runtime.py`

**Rust references:**
- `/media/wl/新加卷/codex/LightFee/src/main.rs:316-354`

- [ ] **Step 1: Run impact analysis**

No production symbol edit is expected. If editing `LiveRuntime._post_tick_housekeeping`, first run:

```text
gitnexus_impact({target: "_post_tick_housekeeping", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Reproduce current warning failure**

Run:

```bash
rtk pytest tests/test_live_full_closure.py::TestLiveFullClosure::test_housekeeping_exports_metrics -q -W error
```

Expected before fix: fails with `RuntimeWarning: coroutine 'LiveRuntime._post_tick_housekeeping' was never awaited`.

- [ ] **Step 3: Fix the test**

Change the test body from:

```python
runtime._post_tick_housekeeping(5000)
```

to:

```python
await runtime._post_tick_housekeeping(5000)
```

Also assert at least one observable effect that proves the coroutine ran. For example, if no pending state exists:

```python
assert runtime.state.lifecycle == EngineLifecycle.RUNNING
```

- [ ] **Step 4: Verify warning-clean behavior**

Run:

```bash
rtk pytest tests/test_live_full_closure.py::TestLiveFullClosure::test_housekeeping_exports_metrics -q -W error
```

Expected: `1 passed`.

- [ ] **Step 5: Update matrix**

Mark `TS-001` as `fixed`, with the test file listed.

---

### Task 3: Move Entry Route And Maker-Leg Decisions Out Of Runtime

**Files:**
- Modify: `lightfee/engine/runtime.py`
- Modify: `lightfee/engine/execution_planner.py` only if missing fields or return data are needed
- Modify: `lightfee/engine/entry_sync.py` only if route execution cannot consume planner output
- Modify: `tests/test_runtime_entry_flow.py`
- Modify: `tests/test_entry_planner.py`

**Rust references:**
- `/media/wl/新加卷/codex/LightFee/src/execution_core/entry_execution_planner.rs:49-208`
- `/media/wl/新加卷/codex/LightFee/src/execution_core/entry_sync.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/entry.rs`

- [ ] **Step 1: Run impact analysis**

Run before editing:

```text
gitnexus_impact({target: "_dispatch_entry", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "plan_incremental_entry_execution", direction: "upstream", repo: "LightFeeV2"})
```

If impact is HIGH or CRITICAL, report it before editing.

- [ ] **Step 2: Add a failing runtime test for planner usage**

Add a test to `tests/test_runtime_entry_flow.py` that monkeypatches or injects planner output and asserts `_dispatch_entry()` uses the planned route instead of fixed `STANDARD_DUAL_TAKER`. The test must prove:

```python
assert captured_ctx.entry_type.value == "passive_incremental"
assert captured_ctx.maker_leg in (Side.BUY, Side.SELL)
```

Use a fake entry executor whose `execute(ctx)` stores the context and returns a completed result compatible with existing tests.

- [ ] **Step 3: Add a no-valid-quote test**

Add a test proving `_dispatch_entry()` does not construct an entry context when `price_hint <= 0`, and journals `runtime.entry_skipped_no_quote`.

- [ ] **Step 4: Implement runtime-to-planner call**

In `_dispatch_entry()`, derive planner inputs from candidate, quote, config, and venue metadata. Runtime may assemble inputs, but route decisions must come from `lightfee/engine/execution_planner.py`.

Keep this rule:

```python
# Runtime dispatches the V1 planner result; it does not choose route or maker side itself.
```

If Rust has several patch branches for passive fallback, hedge remainder, min-notional, or residual prevention, implement them as smaller Python helpers in `execution_planner.py` or `entry_sync.py`. Do not flatten them into a generic "fallback" path unless tests cover each original Rust reason.

- [ ] **Step 5: Verify**

Run:

```bash
rtk pytest tests/test_runtime_entry_flow.py tests/test_entry_planner.py -q
rtk pytest tests/test_runtime_entry_flow.py -q -W error
```

Expected: tests pass with no warnings.

- [ ] **Step 6: Update matrix**

Mark `EN-001` fixed only if runtime integration and planner helper tests both pass.

---

### Task 4: Align Risk Snapshot Capability And Adapter Implementations

**Files:**
- Modify: `lightfee/core/contracts.py`
- Modify: `lightfee/venues/base.py`
- Modify: `lightfee/venues/binance.py`
- Modify: `lightfee/venues/okx.py`
- Modify: `lightfee/venues/bybit.py`
- Modify: `lightfee/venues/bitget.py`
- Modify: `lightfee/venues/gate.py`
- Modify: `lightfee/venues/aster.py`
- Modify: `lightfee/venues/hyperliquid.py`
- Modify: `lightfee/venues/transport.py`
- Modify: `tests/test_risk_actions.py`
- Modify: `tests/test_venues_contract.py`
- Modify: `tests/test_venues_transport.py`

**Rust references:**
- `/media/wl/新加卷/codex/LightFee/src/health.rs`
- `/media/wl/新加卷/codex/LightFee/src/risk.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/binance.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/okx.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/bybit.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/bitget.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/gate.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/aster.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/hyperliquid.rs`

- [ ] **Step 1: Run impact analysis**

Run before editing:

```text
gitnexus_impact({target: "VenueAdapter", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "evaluate_position_risk", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Document V1 support per venue**

Add rows to the parity matrix for each venue risk snapshot support decision:

```text
binance, okx, bybit, bitget, gate, aster, hyperliquid
```

Each row must cite the Rust function or test proving supported, unsupported, or conditionally unsupported.

- [ ] **Step 3: Add failing capability truth tests**

In `tests/test_venues_contract.py`, add tests that assert:

```python
adapter.supports_risk_health == expected_from_v1
snapshot = await adapter.fetch_account_risk_snapshot()
```

For supported venues, use fixture-backed transport data and assert `snapshot.supported is True` plus a positive `health_ratio`.

For unsupported venues, assert `supports_risk_health is False` and snapshot is `None`.

- [ ] **Step 4: Implement adapter snapshot methods**

Implement per-venue `supports_risk_health` and `fetch_account_risk_snapshot()` by delegating to `transport.py` parsers where possible. Keep venue-specific parsing in `venues/`, not in `risk_actions.py` or `runtime.py`.

If Rust has venue-specific risk snapshot patches, keep the Python code cleaner by using parser helpers or capability objects, but preserve the exact supported/unsupported/stale behavior per venue.

- [ ] **Step 5: Verify runtime policy integration**

Add or update `tests/test_risk_actions.py` so stale/missing/unsupported snapshots trigger V1 policy:

```text
death_line -> fail closed
warning_only -> pause entry
ignore -> no additional action
```

- [ ] **Step 6: Verify**

Run:

```bash
rtk pytest tests/test_risk_actions.py tests/test_venues_contract.py tests/test_venues_transport.py -q
rtk pytest tests/test_risk_actions.py tests/test_venues_contract.py -q -W error
```

Expected: tests pass.

- [ ] **Step 7: Update matrix**

Mark `RK-001` fixed only when capability truth and runtime risk policy are both tested.

---

### Task 5: Harden Reconciliation Constructor And Both-Leg Query Tests

**Files:**
- Modify: `lightfee/engine/reconciliation.py`
- Modify: `tests/test_recovery_reconciliation.py`
- Modify: `lightfee/engine/runtime.py` only if runtime call signatures need adjustment

**Rust references:**
- `/media/wl/新加卷/codex/LightFee/src/engine/recovery.rs`
- `/media/wl/新加卷/codex/LightFee/src/runtime_state/persisted_engine.rs`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "OrderReconciler", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "_reconcile_pending_state", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Add strict constructor tests**

In `tests/test_recovery_reconciliation.py`, add tests for:

```python
OrderReconciler(adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter})
OrderReconciler(long_adapter=long_adapter, short_adapter=short_adapter)
```

The test must assert `fetch_position_call_count == 1` on both adapters after `reconcile_position()`.

- [ ] **Step 3: Reject ambiguous positional construction or preserve it intentionally**

Choose one behavior and test it:

Preferred behavior:

```python
with pytest.raises(TypeError):
    OrderReconciler(long_adapter, short_adapter)
```

Alternative behavior is allowed only if implemented explicitly:

```python
OrderReconciler(long_adapter, short_adapter)
```

must set fixed long/short adapters, not treat the first argument as an adapter map.

- [ ] **Step 4: Implement constructor behavior**

Keep the constructor unambiguous. If backward compatibility is kept, detect whether the first positional argument is a dict or a `VenueAdapter` instance.

- [ ] **Step 5: Verify runtime reconciliation**

Add a runtime-level test with pending close or pending entry and adapter map. Assert reconciliation queries both venues and updates pending state.

- [ ] **Step 6: Verify**

Run:

```bash
rtk pytest tests/test_recovery_reconciliation.py tests/test_persistence_replay.py -q
rtk pytest tests/test_recovery_reconciliation.py -q -W error
```

Expected: tests pass with both-leg query counts asserted.

- [ ] **Step 7: Update matrix**

Mark `RC-001` fixed only when both constructor modes and runtime reconciliation path are tested.

---

### Task 6: Venue Reduce-Only, Error Classification, And Precision Parity Audit

**Files:**
- Modify: `lightfee/venues/transport.py`
- Modify: `lightfee/venues/common.py`
- Modify: per-venue files under `lightfee/venues/`
- Modify: `lightfee/core/errors.py`
- Modify: `tests/test_venues_contract.py`
- Modify: `tests/test_venues_transport.py`
- Modify: `tests/test_close_execution.py`

**Rust references:**
- `/media/wl/新加卷/codex/LightFee/src/live/binance.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/okx.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/bybit.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/bitget.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/gate.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/aster.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/hyperliquid.rs`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "VenueTransport", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "normalize_order_quantity", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Add venue parity rows**

For each venue, add rows covering:

```text
order quantity precision
min notional
reduce-only close behavior
order rejection vs uncertain classification
known special errors
```

- [ ] **Step 3: Add failing tests for known V1 edge cases**

At minimum:

- Gate reduce-only empty position is terminal success when venue reports flat.
- Gate reduce-only pending conflict cancels conflicting order and retries.
- Binance/Aster reduce-only min-notional exemption matches V1.
- Hyperliquid live path resolves real asset index and never uses placeholder index.
- Network timeout after submit is `UNCERTAIN`, not `REJECTED`.
- Exchange explicit insufficient balance or invalid size is `REJECTED`.

- [ ] **Step 4: Implement venue-owned fixes**

Put venue-specific error branches in per-venue/transport code. Engine code must receive normalized `OrderFill` or `OrderSubmitError`.

Rust venue files are patch-heavy. Do not copy their shape blindly. Prefer small Python helpers per venue for:

```text
classify_order_error
parse_order_fill
detect_terminal_reduce_only_success
detect_retryable_reduce_only_conflict
normalize_symbol_or_contract
resolve_live_asset_id
```

Each helper must have fixture-backed tests for the Rust patch cases it replaces.

- [ ] **Step 5: Verify**

Run:

```bash
rtk pytest tests/test_venues_transport.py tests/test_venues_contract.py tests/test_close_execution.py -q
rtk pytest tests/test_venues_transport.py tests/test_venues_contract.py -q -W error
```

Expected: tests pass.

- [ ] **Step 6: Update matrix**

Mark `VN-001` fixed only after all listed special cases are tested.

---

### Task 7: Runtime Loop Lane And Startup Activation Parity Decision

**Files:**
- Modify: `lightfee/apps/live.py`
- Modify: `lightfee/engine/runtime.py`
- Modify: `lightfee/engine/bootstrap.py`
- Modify: `lightfee/rate_limit/engine.py` only if reload integration is missing
- Modify: `tests/test_live_startup_preflight.py`
- Modify: `tests/test_live_full_closure.py`

**Rust references:**
- `/media/wl/新加卷/codex/LightFee/src/main.rs:243-410`
- `/media/wl/新加卷/codex/LightFee/src/main.rs:499-713`
- `/media/wl/新加卷/codex/LightFee/src/app_runtime/bootstrap.rs`
- `/media/wl/新加卷/codex/LightFee/src/app_runtime/loop_control.rs`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "LiveRuntime", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "main", file_path: "lightfee/apps/live.py", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Decide lane ownership**

For each Rust lane, update the matrix with implementation or approved deviation:

```text
full tick
active-position tick
maker-event lane
rate-limit reload interval
SIGHUP reload
post-tick housekeeping
runtime error evidence recording
```

- [ ] **Step 3: Add tests for implemented lanes**

Add focused tests that prove:

- full tick failure records V1-style error event and applies backoff
- active tick failure records V1-style error event and applies backoff
- rate-limit reload is called on interval or SIGHUP handler path
- startup prewarm is skipped in non-live mode and attempted in live mode
- activation timeout/failure logs and continues when V1 continues

- [ ] **Step 4: Implement missing orchestration**

Keep orchestration thin. If maker-event lane or startup activation needs substantial behavior, put feed/readiness code in `marketdata/` or `bootstrap.py`, then call it from runtime.

Do not reproduce `tokio::select!` structure mechanically. Python may use asyncio tasks, services, and small lane runners. The required parity is lane behavior: wake conditions, skip behavior, backoff, evidence/journal events, and startup continuation/failure semantics.

- [ ] **Step 5: Verify**

Run:

```bash
rtk pytest tests/test_live_startup_preflight.py tests/test_live_full_closure.py -q
rtk pytest tests/test_live_startup_preflight.py tests/test_live_full_closure.py -q -W error
```

Expected: tests pass.

- [ ] **Step 6: Update matrix**

Mark `RT-001` and `RT-002` fixed, deferred, or approved deviation with evidence.

---

### Task 8: Recovery, Journal, Snapshot, And Replay Parity Audit

**Files:**
- Modify: `lightfee/engine/recovery.py`
- Modify: `lightfee/persistence/journal.py`
- Modify: `lightfee/persistence/snapshot_store.py`
- Modify: `lightfee/engine/state.py`
- Modify: `tests/test_recovery_reconciliation.py`
- Modify: `tests/test_persistence_replay.py`
- Modify: `tests/test_engine_recovery.py`

**Rust references:**
- `/media/wl/新加卷/codex/LightFee/src/engine/recovery.rs`
- `/media/wl/新加卷/codex/LightFee/src/runtime_state/persisted_engine.rs`
- `/media/wl/新加卷/codex/LightFee/src/runtime_state/snapshot_store.rs`
- `/media/wl/新加卷/codex/LightFee/src/observability_ops/replay_bridge.rs`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "recover_from_snapshot", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "replay_journal_records", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "EngineState", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Add matrix rows for replay-critical events**

Cover at least:

```text
entry opened
entry pending/uncertain
exit closed
exit partial closed
pending close
risk mode changed
lifecycle changed
recovery flat
residual exposure
operator command
```

- [ ] **Step 3: Add roundtrip tests**

Tests must prove:

- `EngineState.to_dict()` then recovery reconstructs open positions with fees/funding/risk fields.
- Journal replay removes closed positions.
- Journal replay reduces partial closed positions.
- Pending entry/close survive snapshot and enter `RECONCILING`.
- Fail-closed state is preserved on startup.

- [ ] **Step 4: Implement missing replay fields**

Add only fields needed for V1 live replay. Keep unknown future fields ignored for forward compatibility.

If Rust replay logic is branch-heavy, split Python into decode, apply-event, classify-startup, and normalize-state helpers. The final replay result must match Rust semantics for every event row in the matrix.

- [ ] **Step 5: Verify**

Run:

```bash
rtk pytest tests/test_recovery_reconciliation.py tests/test_persistence_replay.py tests/test_engine_recovery.py -q
rtk pytest tests/test_recovery_reconciliation.py tests/test_persistence_replay.py -q -W error
```

Expected: tests pass.

---

### Task 9: Observability, Evidence, And Rate-Limit Reload Parity

**Files:**
- Modify: `lightfee/persistence/journal.py`
- Modify: `lightfee/persistence/metrics.py`
- Modify: `lightfee/engine/loop_control.py`
- Modify: `lightfee/rate_limit/engine.py`
- Modify: `lightfee/apps/live.py`
- Modify: `tests/test_rate_limit.py`
- Modify: `tests/test_control_plane.py`
- Modify: `tests/test_live_full_closure.py`

**Rust references:**
- `/media/wl/新加卷/codex/LightFee/src/main.rs:388-420`
- `/media/wl/新加卷/codex/LightFee/src/main.rs:809-1525`
- `/media/wl/新加卷/codex/LightFee/src/rate_limit/`
- `/media/wl/新加卷/codex/LightFee/src/observability_ops/`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "Journal", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "RateLimitEngine", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Add matrix rows**

Add rows for:

```text
runtime error evidence
rate-limit reload interval
SIGHUP reload
recommendation flush
journal critical append
metrics names needed by live operations
```

- [ ] **Step 3: Add tests**

Tests must prove failed reload records a structured event, successful reload updates runtime state, and recommendation events drain exactly once.

- [ ] **Step 4: Implement parity**

Keep rate-limit logic in `rate_limit/`; runtime only schedules reload and records outcome.

- [ ] **Step 5: Verify**

Run:

```bash
rtk pytest tests/test_rate_limit.py tests/test_control_plane.py tests/test_live_full_closure.py -q
```

Expected: tests pass.

---

### Task 10: Final Parity Gate And No-Drift Report

**Files:**
- Modify: `docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md`
- Create: `docs/superpowers/reports/2026-05-10-v1-v2-module-parity-closure-report.md`

- [ ] **Step 1: Run full verification**

Run:

```bash
rtk pytest -q
rtk pytest -q -W error
rtk python3 -m compileall lightfee tests
```

Expected: all commands pass. If `pytest -q -W error` exposes unrelated existing warnings, document each and fix touched-test warnings before claiming closure.

- [ ] **Step 2: Run GitNexus changed-flow detection**

Run:

```text
gitnexus_detect_changes({scope: "all", repo: "LightFeeV2"})
```

Expected: affected symbols and flows match the modules touched by the plan.

- [ ] **Step 3: Write closure report**

Create `docs/superpowers/reports/2026-05-10-v1-v2-module-parity-closure-report.md` with:

```markdown
# V1/V2 Module Parity Closure Report

## Verification

- `rtk pytest -q`: result
- `rtk pytest -q -W error`: result
- `rtk python3 -m compileall lightfee tests`: result
- `gitnexus_detect_changes(scope=all)`: result

## Fixed P0/P1 Drift

| ID | Module | Summary | Tests |
| --- | --- | --- | --- |

## Structural Optimizations With Preserved Semantics

| ID | Rust Patch Area | Python Owner | Cleaner Python Shape | Tests Covering Original Behavior |
| --- | --- | --- | --- | --- |

## Approved Deviations

| ID | Reason | Approval |
| --- | --- | --- |

## Remaining Open Items

| ID | Priority | Reason Not Closed |
| --- | --- | --- |
```

- [ ] **Step 4: Final matrix check**

Verify every P0/P1 row is `fixed` or `approved_deviation`. P2/P3 rows may remain `open` only if they are not live-trading blockers.
