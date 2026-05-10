# V1 Production Closure Replication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port the verified Rust V1 live trading closure into LightFeeV2 so Python can run the complete production loop: entry, exit, risk actions, market data, venue certainty, recovery, persistence, and observability.

**Architecture:** Keep V2's Python module boundaries, but use Rust V1 as the business-logic authority. Each implementation slice starts with Rust alignment, GitNexus impact analysis, focused tests, then Python code changes. The plan is organized by production closure subsystem so each task can be reviewed and accepted independently.

**Tech Stack:** Python 3.12, asyncio, dataclasses, Decimal, httpx, pytest, pytest-asyncio, GitNexus MCP/CLI, existing LightFeeV2 domain and adapter contracts.

---

## Reference Documents

- Design: `docs/superpowers/specs/2026-05-10-v1-production-closure-replication-design.md`
- V1/V2 comparison: `/home/wl/下载/v1_v2_comparison_part1.md`
- V1/V2 comparison: `/home/wl/下载/v1_v2_comparison_part2.md`
- Rust source of truth: `/media/wl/新加卷/codex/LightFee`
- Python target repo: `/media/wl/新加卷/codex/LightFeeV2`

## Mandatory Workflow For Every Task

- [ ] **Step 1: Refresh intelligence context**

Run from V2 repo:

```bash
npx gitnexus analyze
```

Expected: repository indexed successfully. If MCP still reports stale data, use the terminal result as confirmation and restart/reload the MCP server before relying on GitNexus graph results.

- [ ] **Step 2: Locate exact Rust source**

Use GitNexus and local source reads to identify the V1 functions, tests, and live-path callers. Record the source paths in the task note before editing Python.

- [ ] **Step 3: Run impact analysis before editing symbols**

For every Python function/class/method to edit, run:

```text
gitnexus_impact({target: "<symbol>", direction: "upstream", repo: "LightFeeV2"})
```

Expected: risk understood and reported. If risk is HIGH or CRITICAL, warn before editing.

- [ ] **Step 4: Write behavior tests first**

Tests must encode Rust-equivalent behavior: state transitions, precision, rejection/uncertainty, partial fills, stale data, and journal effects.

- [ ] **Step 5: Implement the Python port**

Use V2 structure, but keep Rust business behavior. Use comments only on formulas and branch conditions where Rust provenance matters.

- [ ] **Step 6: Verify focused and full scope**

Run the focused task tests. For production closure milestones, also run:

```bash
pytest -q
python -m compileall lightfee tests
```

- [ ] **Step 7: Detect changed flow impact before commit**

Run:

```text
gitnexus_detect_changes({scope: "all", repo: "LightFeeV2"})
```

Expected: changed symbols and affected flows match the task scope.

## Execution Order

1. Task 1: domain, state, precision, and error contracts
2. Task 2: entry planner and pending-entry model
3. Task 3: synchronized entry executor and residual protection
4. Task 4: exit decision engine and close execution model
5. Task 5: reduce-only close executor and PnL attribution
6. Task 6: risk action closure
7. Task 7: market data, local L2, and WS resilience
8. Task 8: venue production parity
9. Task 9: recovery and reconciliation
10. Task 10: full live-loop orchestration and acceptance harness

Tasks 2 and 4 can be developed in parallel after Task 1. Tasks 7 and 8 can be developed in parallel after the execution request/response contracts from Task 1 stabilize. Task 10 must run last.

---

### Task 1: Domain, State, Precision, And Error Contracts

**Files:**
- Modify: `lightfee/core/domain.py`
- Modify: `lightfee/core/errors.py`
- Modify: `lightfee/core/money.py`
- Modify: `lightfee/engine/state.py`
- Modify: `lightfee/core/contracts.py`
- Test: `tests/test_domain_contracts.py`
- Test: `tests/test_engine_state.py`

**Rust references:**
- `/media/wl/新加卷/codex/LightFee/src/models.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/state.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/reliability_contract.rs`
- `/media/wl/新加卷/codex/LightFee/src/market_gateway/ports.rs`

- [ ] **Step 1: Align Rust domain fields**

Document which V1 live fields are missing from V2. At minimum, check order request timing hints, mark-price hints, observed timestamps, fill timing, order status, and uncertainty metadata.

- [ ] **Step 2: Add failing tests for production state shape**

Create tests that assert:

- `OpenPosition` can persist matched quantities, long/short fills, fees, funding accrual, peak edge, latest edge, and close deadlines.
- `PendingEntry` can persist maker order id, hedge order id, fill quantities, fallback state, deadline, and uncertain outcome flags.
- `PendingClose` can persist long/short order ids, close quantities, reason, deadline, and uncertain outcome flags.
- `OrderSubmitError` preserves `REJECTED` vs `UNCERTAIN`.
- quantity normalization helpers use floor behavior.

Run:

```bash
pytest tests/test_domain_contracts.py tests/test_engine_state.py -v
```

Expected: failures describe missing fields or helpers.

- [ ] **Step 3: Expand domain and state dataclasses**

Add only fields required by live Rust behavior. Prefer optional fields with explicit defaults for recovery compatibility. Use `Decimal` for values that affect trading decisions.

- [ ] **Step 4: Update serialization**

Extend `EngineState.to_dict()` and recovery-compatible decoding so pending/open state can survive restart. Preserve existing top-level keys used by current-state exports.

- [ ] **Step 5: Verify**

Run:

```bash
pytest tests/test_domain_contracts.py tests/test_engine_state.py -v
python -m compileall lightfee/core lightfee/engine tests/test_domain_contracts.py tests/test_engine_state.py
```

Expected: tests pass and package compiles.

---

### Task 2: Entry Planner And Pending-Entry Model

**Files:**
- Modify: `lightfee/engine/execution_planner.py`
- Modify: `lightfee/engine/entry.py`
- Modify: `lightfee/engine/state.py`
- Modify: `lightfee/risk/budgets.py`
- Test: `tests/test_entry_planner.py`
- Test: `tests/test_entry_state_machine.py`

**Rust references:**
- `/media/wl/新加卷/codex/LightFee/src/engine/entry.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/entry_sync.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/market_data.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/helpers.rs`

- [ ] **Step 1: Align planner formulas**

Extract V1 formulas for target size, maker clip, max initial clip, hedge chunk, min-notional, passive fallback, and rejected reasons. Record exact Rust function names in the test file header.

- [ ] **Step 2: Add failing planner tests**

Test cases must cover:

- zero or negative target is rejected
- maker min clip above max initial clip falls back
- clip too close to full target falls back
- zero hedgeable quantity is rejected
- min notional raises clip only when V1 allows it
- floor-to-step never rounds quantity upward
- budget failure blocks entry before order creation

Run:

```bash
pytest tests/test_entry_planner.py -v
```

Expected: failures identify planner deviations from V1.

- [ ] **Step 3: Port planner logic**

Implement V1-equivalent planner branches in `plan_entry_execution()` or split into smaller helpers if needed. Do not change returned route semantics without updating all callers.

- [ ] **Step 4: Add pending-entry transition tests**

Test transitions:

```text
IDLE -> SUBMITTING_MAKER -> MAKER_RESTING -> SUBMITTING_HEDGE -> HEDGE_PENDING -> COMPLETED
IDLE -> SUBMITTING_MAKER -> FAILED
MAKER_RESTING -> PASSIVE_FALLBACK
SUBMITTING_HEDGE -> FAILED_WITH_RESIDUAL
```

If V2 lacks a needed state value, add it to `EntryState` and document the Rust source.

- [ ] **Step 5: Verify**

Run:

```bash
pytest tests/test_entry_planner.py tests/test_entry_state_machine.py -v
```

Expected: planner and state-machine tests pass.

---

### Task 3: Synchronized Entry Executor And Residual Protection

**Files:**
- Create: `lightfee/engine/entry_sync.py`
- Create: `lightfee/engine/residual.py`
- Modify: `lightfee/engine/entry.py`
- Modify: `lightfee/engine/runtime.py`
- Modify: `lightfee/persistence/journal.py` if journal metadata is insufficient
- Test: `tests/test_entry_sync.py`
- Test: `tests/test_entry_residual.py`
- Test: `tests/test_runtime_entry_flow.py`

**Rust references:**
- `/media/wl/新加卷/codex/LightFee/src/engine/entry_sync.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/entry.rs`
- `/media/wl/新加卷/codex/LightFee/src/execution_core/residual.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/reliability_contract.rs`

- [ ] **Step 1: Add fake venue adapters**

Create test-local fake adapters that can return full fills, partial fills, rejected submit, uncertain submit, delayed reconciliation, and failed reconciliation.

- [ ] **Step 2: Add failing synchronized-entry tests**

Cover:

- standard dual-taker opens one matched `OpenPosition`
- passive maker fill followed by hedge fill opens one matched `OpenPosition`
- maker reject fails entry without hedge submit
- hedge reject after maker fill creates residual protection work
- order timeout becomes uncertain and enters reconciliation
- partial maker fill below V1 threshold does not open a full position
- partial hedge fill leaves residual state

Run:

```bash
pytest tests/test_entry_sync.py tests/test_entry_residual.py -v
```

Expected: tests fail before executor exists.

- [ ] **Step 3: Implement entry executor**

Implement an async entry executor that receives an entry context, venue adapters, config, state, and journal. It must submit orders through `VenueAdapter.place_order()`, classify submit failures, update pending state at each transition, and append journal events before and after live actions.

- [ ] **Step 4: Implement residual protection**

Port V1 residual close/protect behavior for unhedged maker fills. Residual action must never be silently ignored; it must produce pending close/protective action or fail closed.

- [ ] **Step 5: Wire runtime candidate flow**

Update `LiveRuntime.tick()` so tradeable candidates are transformed into entry contexts and passed to the entry executor when lifecycle/risk/budgets permit.

- [ ] **Step 6: Verify**

Run:

```bash
pytest tests/test_entry_sync.py tests/test_entry_residual.py tests/test_runtime_entry_flow.py -v
```

Expected: synchronized entry, residual protection, and runtime entry wiring pass with fake adapters.

---

### Task 4: Exit Decision Engine And Close Execution Model

**Files:**
- Modify: `lightfee/engine/exit.py`
- Create: `lightfee/engine/exit_decision.py`
- Modify: `lightfee/engine/state.py`
- Test: `tests/test_exit_decisions.py`

**Rust references:**
- `/media/wl/新加卷/codex/LightFee/src/engine/exit.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/market_data.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/state.rs`

- [ ] **Step 1: Align V1 exit reasons**

For every V2 `ExitReason`, identify the Rust condition, threshold/config field, required market data, and resulting close behavior.

- [ ] **Step 2: Add failing decision tests**

Cover:

- profit take
- net stop loss
- trailing exit after peak edge drawdown
- first-stage funding capture
- second-stage funding capture
- funding capture outside stage windows
- settlement force close deadline
- mark price hard stop
- risk death close
- risk delever close

Run:

```bash
pytest tests/test_exit_decisions.py -v
```

Expected: failures identify missing decision engine.

- [ ] **Step 3: Implement decision engine**

Create pure decision helpers that take position state, market/funding/risk views, config, and `now_ms`, then return an explicit close intent or no-action result. Keep order submission out of this module.

- [ ] **Step 4: Verify**

Run:

```bash
pytest tests/test_exit_decisions.py -v
```

Expected: all exit decisions match Rust-equivalent cases.

---

### Task 5: Reduce-Only Close Executor And PnL Attribution

**Files:**
- Create: `lightfee/engine/close_executor.py`
- Modify: `lightfee/engine/exit.py`
- Modify: `lightfee/engine/runtime.py`
- Modify: `lightfee/venues/common.py`
- Test: `tests/test_close_execution.py`
- Test: `tests/test_close_pnl.py`

**Rust references:**
- `/media/wl/新加卷/codex/LightFee/src/engine/exit.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/market_data.rs`
- GitNexus V1 processes named `Close_execution_chunks_*`

- [ ] **Step 1: Add failing close-chunk tests**

Cover:

- reduce-only close orders use opposite sides
- long and short close quantities are absolute
- close chunks respect venue step/min-notional rules
- reduce-only exemptions match V1 for supported venues
- close plan that would leave min-notional dust is rejected
- cached true-L2 is used before live depth fallback where V1 does so
- protect-only mode uses allowed fallback
- suspend mode requires hot local-L2

Run:

```bash
pytest tests/test_close_execution.py -v
```

Expected: failures describe missing close executor and chunking rules.

- [ ] **Step 2: Implement close executor**

Create an async close executor that builds close chunks, submits reduce-only orders, handles rejected/uncertain outcomes, updates `PendingClose`, and reconciles order fills before removing `OpenPosition`.

- [ ] **Step 3: Add failing PnL attribution tests**

Cover:

- long price PnL
- short price PnL
- fees on both legs
- funding PnL attribution
- net quote result
- matched close quantity by `min(long_fill.quantity, short_fill.quantity)`

- [ ] **Step 4: Implement PnL attribution**

Extend `compute_close_pnl()` so it matches V1 accounting instead of price-only PnL.

- [ ] **Step 5: Wire active-position tick**

Update `LiveRuntime.tick_active_positions()` to evaluate exit decisions and call the close executor for actionable positions.

- [ ] **Step 6: Verify**

Run:

```bash
pytest tests/test_close_execution.py tests/test_close_pnl.py -v
```

Expected: close execution and PnL attribution pass with fake adapters.

---

### Task 6: Risk Action Closure

**Files:**
- Modify: `lightfee/engine/supervisor.py`
- Create: `lightfee/engine/risk_actions.py`
- Modify: `lightfee/risk/health.py`
- Modify: `lightfee/risk/budgets.py`
- Modify: `lightfee/risk/modes.py`
- Test: `tests/test_risk_actions.py`
- Test: `tests/test_supervisor_execution.py`

**Rust references:**
- `/media/wl/新加卷/codex/LightFee/src/risk.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/risk.rs`
- `/media/wl/新加卷/codex/LightFee/src/health.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/supervision.rs`

- [ ] **Step 1: Add failing risk-action tests**

Cover:

- warning line sets entry pause when enabled
- delever line creates synchronized delever close intent
- death line creates protective close intent
- stale risk snapshot applies configured fail-closed/protect/suspend behavior
- unsupported risk snapshot behavior follows V1 config
- supervisor journal includes pre-action and post-action events

Run:

```bash
pytest tests/test_risk_actions.py tests/test_supervisor_execution.py -v
```

Expected: failures show supervisor currently logs only.

- [ ] **Step 2: Implement action planner**

Create `risk_actions.py` with pure helpers that convert risk health, current positions, venue support, and config into explicit actions: pause entries, delever, death close, fail closed, or no action.

- [ ] **Step 3: Make supervisor execute actions**

Change supervisor from synchronous log-only behavior to an execution-capable service. If async execution is required, update runtime call sites so `_post_tick_housekeeping()` can await risk actions safely.

- [ ] **Step 4: Verify**

Run:

```bash
pytest tests/test_risk_actions.py tests/test_supervisor_execution.py -v
```

Expected: risk lines trigger real close/delever intents and state changes.

---

### Task 7: Market Data, Local L2, And WS Resilience

**Files:**
- Modify: `lightfee/marketdata/l2.py`
- Modify: `lightfee/marketdata/local_book.py`
- Modify: `lightfee/marketdata/freshness.py`
- Modify: `lightfee/marketdata/liquidity.py`
- Create: `lightfee/marketdata/ws.py`
- Create: `lightfee/marketdata/private_ws.py`
- Create: `lightfee/marketdata/resilience.py`
- Test: `tests/test_marketdata_l2.py`
- Test: `tests/test_marketdata_freshness.py`
- Test: `tests/test_ws_resilience.py`

**Rust references:**
- `/media/wl/新加卷/codex/LightFee/src/engine/market_data.rs`
- `/media/wl/新加卷/codex/LightFee/src/ws.rs`
- `/media/wl/新加卷/codex/LightFee/src/private_ws.rs`
- `/media/wl/新加卷/codex/LightFee/src/resilience.rs`

- [ ] **Step 1: Add failing local-L2 state tests**

Cover:

- cold to bootstrapping
- bootstrapping to hot
- hot to degraded
- degraded to rebuilding
- repeated degradation to suspended
- stale book detection
- warm-to-hot promotion
- local-L2 protect/suspend behavior for entry and close

- [ ] **Step 2: Add failing WS resilience tests**

Cover:

- reconnect backoff
- heartbeat timeout
- stale stream degradation
- REST bootstrap before incremental updates
- private fill event updates pending entry/close reconciliation

Run:

```bash
pytest tests/test_marketdata_l2.py tests/test_marketdata_freshness.py tests/test_ws_resilience.py -v
```

Expected: failures identify missing WS/feed management.

- [ ] **Step 3: Implement market data services**

Implement local book update application, freshness decisions, execution liquidity selection, and websocket lifecycle services using asyncio tasks. Keep venue-specific WS topic details behind venue specs or profiles.

- [ ] **Step 4: Verify**

Run:

```bash
pytest tests/test_marketdata_l2.py tests/test_marketdata_freshness.py tests/test_ws_resilience.py -v
```

Expected: market data behavior matches V1 state-machine cases.

---

### Task 8: Venue Production Parity

**Files:**
- Modify: `lightfee/venues/transport.py`
- Modify: `lightfee/venues/specs.py`
- Modify: `lightfee/venues/common.py`
- Modify: `lightfee/venues/binance.py`
- Modify: `lightfee/venues/okx.py`
- Modify: `lightfee/venues/bybit.py`
- Modify: `lightfee/venues/bitget.py`
- Modify: `lightfee/venues/gate.py`
- Modify: `lightfee/venues/aster.py`
- Modify: `lightfee/venues/hyperliquid.py`
- Test: `tests/test_venues_transport.py`
- Test: `tests/test_venues_contract.py`
- Test: `tests/test_venue_precision.py`

**Rust references:**
- `/media/wl/新加卷/codex/LightFee/src/live/binance.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/okx.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/bybit.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/bitget.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/gate.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/aster.rs`
- `/media/wl/新加卷/codex/LightFee/src/live/hyperliquid.rs`

- [ ] **Step 1: Build venue deviation matrix**

For each venue, record V1 behavior for signing, timestamp, endpoint, position mode, leverage setup, order type, reduce-only, post-only, cancel, amend, fill reconciliation, L2 support, private WS support, precision, and special error handling.

- [ ] **Step 2: Add failing venue parity tests**

Fixture tests must cover:

- live request signing payloads
- private GET query signing where applicable
- order payload mapping
- position parsing
- fill reconciliation parsing
- rejected vs uncertain error classification
- quantity and price precision
- reduce-only min-notional exceptions
- leverage and position mode preflight calls

Run:

```bash
pytest tests/test_venues_transport.py tests/test_venues_contract.py tests/test_venue_precision.py -v
```

Expected: failures identify missing venue details.

- [ ] **Step 3: Implement venue parity**

Keep shared HTTP mechanics in `transport.py`, but put venue-specific behavior in specs/profile helpers or adapter modules. Do not move exchange-specific branches into engine or risk modules.

- [ ] **Step 4: Verify**

Run:

```bash
pytest tests/test_venues_transport.py tests/test_venues_contract.py tests/test_venue_precision.py -v
```

Expected: all seven venue parity tests pass without live network calls.

---

### Task 9: Recovery And Reconciliation

**Files:**
- Modify: `lightfee/engine/recovery.py`
- Create: `lightfee/engine/reconciliation.py`
- Modify: `lightfee/persistence/journal.py`
- Modify: `lightfee/persistence/snapshot_store.py`
- Modify: `lightfee/persistence/sqlite_store.py`
- Modify: `lightfee/persistence/ledgers.py`
- Test: `tests/test_recovery_reconciliation.py`
- Test: `tests/test_persistence_replay.py`

**Rust references:**
- `/media/wl/新加卷/codex/LightFee/src/engine/recovery.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/state.rs`
- `/media/wl/新加卷/codex/LightFee/src/runtime_state/`
- `/media/wl/新加卷/codex/LightFee/src/observability_ops/`

- [ ] **Step 1: Add failing recovery tests**

Cover:

- clean open position recovery
- pending entry recovery with known maker order
- pending entry recovery with uncertain hedge order
- pending close recovery
- ambiguous state enters fail-closed/reconciling mode
- journal replay restores latest risk mode
- unknown order reconciliation queries venue adapter
- residual exposure recovery schedules protective action

Run:

```bash
pytest tests/test_recovery_reconciliation.py tests/test_persistence_replay.py -v
```

Expected: failures show current recovery is too shallow.

- [ ] **Step 2: Implement reconciliation service**

Create a service that can query adapters for order fills, positions, and account risk, then resolve pending entry/close uncertainty according to V1 policy.

- [ ] **Step 3: Extend persistence replay**

Persist enough event payload to rebuild pending/open state and risk mode after restart. Keep atomic snapshot writes.

- [ ] **Step 4: Verify**

Run:

```bash
pytest tests/test_recovery_reconciliation.py tests/test_persistence_replay.py -v
```

Expected: recovery produces safe states for clean, pending, and ambiguous cases.

---

### Task 10: Full Live-Loop Orchestration And Acceptance Harness

**Files:**
- Modify: `lightfee/engine/runtime.py`
- Modify: `lightfee/apps/live.py`
- Modify: `lightfee/apps/probe.py`
- Modify: `lightfee/config/schema.py`
- Modify: `lightfee/config/validation.py`
- Create: `tests/test_live_full_closure.py`
- Create: `tests/test_live_startup_preflight.py`

**Rust references:**
- `/media/wl/新加卷/codex/LightFee/src/main.rs`
- `/media/wl/新加卷/codex/LightFee/src/app_runtime/loop_control.rs`
- `/media/wl/新加卷/codex/LightFee/src/app_runtime/bootstrap.rs`
- `/media/wl/新加卷/codex/LightFee/src/engine/supervision.rs`

- [ ] **Step 1: Add full-loop fake-live tests**

Build a fixture that drives:

```text
startup
  -> recovery
  -> sidecar snapshot load
  -> candidate discovery
  -> entry execution
  -> active position tick
  -> exit decision
  -> reduce-only close
  -> PnL journal
  -> final state snapshot
```

Run:

```bash
pytest tests/test_live_full_closure.py -v
```

Expected: fails until runtime orchestration is connected.

- [ ] **Step 2: Add live startup preflight tests**

Cover:

- missing credentials fail closed in live mode
- unsupported required venue capability fails closed
- leverage setup failure fails closed or follows V1 configured fallback
- ambiguous recovered state enters reconciling/fail-closed
- configured paper/test fake mode remains available for tests only

- [ ] **Step 3: Wire orchestrator services**

Update `LiveRuntime` construction so entry executor, close executor, risk supervisor, market data service, and reconciliation service are explicit dependencies. Keep default construction ergonomic for `apps/live.py`.

- [ ] **Step 4: Verify full acceptance**

Run:

```bash
pytest -q
python -m compileall lightfee tests
```

Expected:

- full test suite passes
- compileall succeeds
- no live-path required method raises `NotImplementedError`

- [ ] **Step 5: Run GitNexus change detection**

Run:

```text
gitnexus_detect_changes({scope: "all", repo: "LightFeeV2"})
```

Expected: affected flows are entry, exit, risk, market data, venue, recovery, and runtime closure only.

---

## Acceptance Gates

The production replication project is complete only when all gates pass:

- [ ] V2 can open a two-leg position through the live execution path using fake live adapters.
- [ ] V2 can close a two-leg position through reduce-only close execution.
- [ ] V2 risk death and delever conditions trigger live close/delever actions.
- [ ] V2 restart recovery can resolve pending entries, pending closes, and unknown order outcomes.
- [ ] V2 local L2 and WS services provide the same protect/suspend/fallback decisions as V1.
- [ ] All seven venue adapters preserve V1 precision and error-classification behavior.
- [ ] Current-state snapshot and journal contain enough data to replay live decisions.
- [ ] `pytest -q` passes.
- [ ] `python -m compileall lightfee tests` passes.
- [ ] `gitnexus_detect_changes()` shows expected scope before commit.

## Deviation Policy

Allowed without approval:

- splitting large Rust functions into smaller Python helpers
- replacing Rust `Arc`, `Mutex`, `Rc`, channels, and threads with asyncio-compatible services
- improving names where tests and comments preserve Rust provenance
- replacing duplicate venue HTTP mechanics with shared transport code

Requires explicit approval:

- changing thresholds, window sizes, retry counts, stale-data behavior, funding-stage logic, or risk action semantics
- changing order submission order, fallback behavior, or uncertain outcome handling
- using float arithmetic where the value affects order size, price, notional, fee, funding PnL, or a trading threshold
- treating an unsupported venue capability as success

Forbidden:

- hardcoding missing skeleton context instead of expanding the skeleton
- swallowing partial fills or uncertain submits
- logging risk triggers without executing required risk actions
- removing live-path behavior because it is hard to port
- accepting paper-only behavior as production closure
