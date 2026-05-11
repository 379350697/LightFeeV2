# V1 Full Parity Gap Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the remaining Rust V1 live-path parity gaps by first making the missing architecture explicit, then assigning each gap to the correct Python owner module, then proving closure with fresh verification evidence.

**Architecture:** Keep LightFeeV2's Pythonic module structure. Do not continue single-gap patching. Treat local-L2, startup, recovery, maker-event, and venue capability truth as separate layers that communicate through explicit contracts. When current code lacks a proper owner boundary, introduce one instead of stuffing more logic into `runtime.py`.

**Tech Stack:** Python 3.12, asyncio, dataclasses, pytest, pytest-asyncio, GitNexus MCP/CLI, existing LightFeeV2 modules, Rust V1 source under `/media/wl/新加卷/codex/LightFee`.

---

## Reference Documents

- Spec: `docs/superpowers/specs/2026-05-11-v1-full-parity-gap-closure-design.md`
- Existing parity closure spec: `docs/superpowers/specs/2026-05-10-v1-v2-module-parity-closure-design.md`
- Existing parity matrix: `docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md`
- Existing closure report: `docs/superpowers/reports/2026-05-10-v1-v2-module-parity-closure-report.md`
- Rust V1 source: `/media/wl/新加卷/codex/LightFee`
- Python V2 target: `/media/wl/新加卷/codex/LightFeeV2`

## Mandatory Rules

- Do not claim full closure until fresh verification commands pass.
- Do not keep patching `runtime.py` when the missing behavior belongs in a dedicated worker, contract, or data-plane owner.
- Do not mark any new parity row `fixed` without a focused test and a production caller/integration proof.
- Do not use fake adapters, sidecars, or shadow-mode shortcuts to claim live-path closure.
- Do not change thresholds, retry budgets, timing windows, or precision rules unless Rust V1 evidence proves that exact behavior.
- Do not flatten venue-specific support into a generic path when Rust V1 treats the venues differently.

## Working Rule

Every task in this plan must preserve the following:

```text
Rust live behavior: 1:1 semantic parity
Python implementation: cleaner ownership, smaller services, fewer historical patches
```

If a behavior is currently spread across several Python modules, the fix is to define a clearer boundary, not to sprinkle one more helper.

## Task 1: Build The Missing Gap Registry

**Files:**
- Create: `docs/superpowers/parity/2026-05-11-v1-full-parity-gap-closure-matrix.md`
- Read: `docs/superpowers/specs/2026-05-11-v1-full-parity-gap-closure-design.md`
- Read: `docs/superpowers/reports/2026-05-10-v1-v2-module-parity-closure-report.md`
- Read: `lightfee/engine/runtime.py`
- Read: `lightfee/marketdata/local_l2_data_plane.py`
- Read: `lightfee/marketdata/local_l2_runtime.py`
- Read: `lightfee/marketdata/local_l2_ws.py`
- Read: `lightfee/venues/transport.py`

- [ ] **Step 1: Seed the matrix with missing architectural rows**

Create a parity matrix that separates:

- worker ownership gaps
- startup and shutdown gaps
- canonical symbol authority gaps
- local-L2 runtime/state-machine gaps
- venue capability truth gaps
- docs/verification gaps

Use exact Rust and Python file paths for each row. Mark rows `open` until a focused test and the production caller path prove the fix.

- [ ] **Step 2: Require a module owner for every gap**

Every row must have exactly one Python owner module. If a gap currently spans two modules, name the module that owns the business rule and list the other module as a consumer only.

- [ ] **Step 3: Add a verification column**

Each row must include the focused test file and the production caller path that proves the row is live-path relevant.

- [ ] **Step 4: Prevent fake closure**

Do not allow `fixed` unless the latest acceptance commands were run fresh in this session or the implementation session that produced the change.

## Task 2: Introduce Explicit Local-L2 Worker Ownership

**Files:**
- Create or modify: `lightfee/marketdata/local_l2_worker.py` or the smallest equivalent session-manager module
- Modify: `lightfee/marketdata/local_l2_data_plane.py`
- Modify: `lightfee/engine/runtime.py`
- Modify: `lightfee/marketdata/local_l2_ws.py`
- Modify: `tests/test_local_l2_ws.py`
- Modify: `tests/test_live_startup_preflight.py`
- Modify: `tests/test_live_full_closure.py`

- [ ] **Step 1: Write a failing lifecycle test**

Add a test that starts runtime, activates local-L2, and then stops runtime cleanly without leaving pending worker tasks.

The test must prove:

- `start()` returns
- worker tasks are registered under an explicit owner
- `stop()` cancels them
- no timeout is needed to make the test exit

- [ ] **Step 2: Make worker ownership explicit**

Move local-L2 worker/session management out of implicit runtime task spawning.

The worker owner must expose clear operations such as:

- register desired venue/symbol pairs
- start workers
- stop workers
- inspect worker diagnostics

If a new helper is needed, keep it in the market-data layer, not in planner or venue contract code.

- [ ] **Step 3: Verify the preflight no longer hangs**

Run:

```bash
rtk timeout 25s pytest tests/test_live_startup_preflight.py -q -W error
```

Expected: exit `0`.

- [ ] **Step 4: Update the matrix**

Mark the worker ownership rows as `fixed` only after the lifecycle tests pass and the worker owner has a clear production caller.

## Task 3: Normalize Canonical Symbol Authority End-To-End

**Files:**
- Modify: `lightfee/venues/specs.py`
- Modify: `lightfee/venues/transport.py`
- Modify: `lightfee/marketdata/local_l2_ws.py`
- Modify: `lightfee/marketdata/local_l2_data_plane.py`
- Modify: `lightfee/marketdata/l2.py` if the update model needs an explicit canonical/wire field split
- Modify: `tests/test_local_l2_ws.py`
- Modify: `tests/test_venues_transport.py`
- Modify: `tests/test_marketdata_l2.py`

- [ ] **Step 1: Add failing tests for canonical-only runtime keys**

Add tests that prove:

- OKX wire symbol may be `BTC-USDT-SWAP`, but the runtime book key is canonical `BTCUSDT`
- Gate wire symbol may be `BTC_USDT`, but the runtime book key is canonical `BTCUSDT`
- Hyperliquid wire symbol may be `BTC`, but the runtime book key is canonical `BTCUSDT`

- [ ] **Step 2: Keep wire symbols at the boundary**

The transport and WS client layers may derive venue wire symbols, but they must convert back to canonical symbols before `LocalL2Runtime.record_update()` sees them.

- [ ] **Step 3: Eliminate symbol split books**

Make sure one venue/symbol pair maps to exactly one runtime book key.

- [ ] **Step 4: Re-run focused parity tests**

Run the WS and transport tests that cover all venues and confirm no split-book behavior remains.

## Task 4: Close Local-L2 Runtime, Recovery, And Maker-Event Boundaries

**Files:**
- Modify: `lightfee/engine/runtime.py`
- Modify: `lightfee/engine/recovery.py`
- Modify: `lightfee/engine/entry.py`
- Modify: `lightfee/engine/entry_sync.py`
- Modify: `lightfee/marketdata/local_l2_runtime.py`
- Modify: `tests/test_runtime_entry_flow.py`
- Modify: `tests/test_local_l2_runtime.py`
- Modify: `tests/test_runtime_maker_event_local_l2.py`
- Modify: `tests/test_live_full_closure.py`

- [ ] **Step 1: Prove the current runtime boundaries with tests**

Write tests that show which behavior belongs to:

- startup recovery
- local-L2 runtime state
- maker-event lane wakeup
- pending-entry reconciliation

- [ ] **Step 2: Keep runtime as orchestration only**

Runtime should assemble inputs, call owner modules, and record journals. It should not own business formulas that belong in planner, recovery, or data-plane services.

- [ ] **Step 3: Align recovery and reconciliation behavior**

If pending entries, retained books, or recovery snapshots need a specialized helper, place it in `recovery.py` or `entry_sync.py`, not in the startup loop.

- [ ] **Step 4: Prove maker-event wake semantics**

The maker-event lane must wake from real local-L2 updates and consume canonical books only.

## Task 5: Normalize Venue Capability Truth And Risk Snapshot Behavior

**Files:**
- Modify: `lightfee/core/contracts.py`
- Modify: `lightfee/venues/transport.py`
- Modify: `lightfee/venues/specs.py`
- Modify: `lightfee/venues/binance.py`
- Modify: `lightfee/venues/okx.py`
- Modify: `lightfee/venues/bybit.py`
- Modify: `lightfee/venues/bitget.py`
- Modify: `lightfee/venues/gate.py`
- Modify: `lightfee/venues/aster.py`
- Modify: `lightfee/venues/hyperliquid.py`
- Modify: `tests/test_venues_base.py`
- Modify: `tests/test_venues_transport.py`
- Modify: `tests/test_risk_actions.py`

- [ ] **Step 1: Write capability-truth tests per venue**

Add or update tests so that supported and unsupported capability combinations are explicit, especially for risk health.

- [ ] **Step 2: Keep unsupported behavior honest**

If a venue is unsupported for a capability, the code must say so directly. Do not hide that behind silent fallback paths.

- [ ] **Step 3: Align risk-health and account-risk behavior**

Where Rust V1 has a real account-risk query, Python must call the correct endpoint and shape. Where Rust V1 marks a venue unsupported, Python must match that truth rather than approximating it.

- [ ] **Step 4: Verify venue contract tests**

Run the transport and venue-base tests that cover capabilities, signing, account risk, and unsupported behavior.

## Task 6: Make Closure Documentation Match Fresh Evidence

**Files:**
- Modify: `docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md`
- Modify: `docs/superpowers/reports/2026-05-10-v1-v2-module-parity-closure-report.md`
- Modify: `docs/superpowers/parity/2026-05-11-v1-full-parity-gap-closure-matrix.md`
- Modify: `docs/superpowers/reports/*` only after code/tests prove the state

- [ ] **Step 1: Remove premature fixed claims**

If the latest acceptance commands have not passed, the docs must say `open` or `in_progress`, not `fixed`.

- [ ] **Step 2: Update the closure report with fresh evidence**

Use exact commands, exit codes, and observed pass counts from the current session.

- [ ] **Step 3: Keep parity matrix and closure report consistent**

The matrix and report must never disagree on whether a gap is closed.

## Verification Commands

Before reporting completion of the whole plan, run:

```bash
rtk timeout 25s pytest tests/test_live_startup_preflight.py -q -W error
rtk pytest tests/test_local_l2_ws.py tests/test_local_l2_runtime.py tests/test_live_startup_preflight.py tests/test_runtime_maker_event_local_l2.py tests/test_marketdata_l2.py -q -W error
rtk pytest tests/test_venues_transport.py tests/test_venues_base.py tests/test_risk_actions.py -q -W error
rtk python3 -m compileall lightfee tests
rtk pytest -q -W error
```

If a task changes production symbols, run GitNexus impact first and `gitnexus_detect_changes()` before claiming completion.

## Final Acceptance

This plan is complete only when all of the following are true:

- worker ownership is explicit
- startup preflight returns cleanly
- canonical symbols are enforced end-to-end
- local-L2 runtime and worker lifecycle are separated cleanly
- venue capability truth matches Rust V1
- maker-event lane consumes real local-L2 state
- docs only say `fixed` after fresh acceptance evidence

