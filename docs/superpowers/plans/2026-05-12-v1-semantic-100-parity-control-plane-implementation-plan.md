# V1 Semantic 100% Parity Control Plane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. This plan owns orchestration only. Keep the live business semantics in their owning modules.

**Goal:** Make LightFeeV2's startup, shutdown, tick scheduling, backoff, export, and signal handling match V1 live runtime semantics.

**Architecture:** `apps/live.py` stays thin and starts the runtime. `engine/runtime.py` owns orchestration of the loop, but not venue parsing, order routing, or record semantics. The control plane should schedule the same lanes V1 runs, preserve backoff behavior, and shut down in an order that does not leak workers or skip exports.

**Tech Stack:** Python 3.12, asyncio, signal handlers, pytest, existing LightFeeV2 runtime modules, GitNexus MCP, Rust V1 source under `/media/wl/新加卷/codex/LightFee`.

---

## Reference Docs

- Master spec: `docs/superpowers/specs/2026-05-12-v1-semantic-100-parity-design.md`
- Current gap baseline: `docs/superpowers/parity/2026-05-11-v1-full-parity-gap-closure-matrix.md`
- Rust V1 control plane anchors:
  - `/media/wl/新加卷/codex/LightFee/src/main.rs`
  - `/media/wl/新加卷/codex/LightFee/src/execution_core/engine.rs`

## File Ownership

- Modify: `lightfee/apps/live.py`
- Modify: `lightfee/engine/runtime.py`
- Modify: `lightfee/engine/bootstrap.py`
- Modify: `lightfee/engine/loop_control.py`
- Modify: `tests/test_live_startup_preflight.py`
- Modify: `tests/test_runtime_smoke.py`
- Optional test support: `tests/test_control_plane.py`

Do not edit venue, execution, persistence, or offline modules in this plan.

## Task 1: Prove Startup And Shutdown Order

**Files:**
- Modify: `tests/test_live_startup_preflight.py`
- Modify: `lightfee/apps/live.py`

- [ ] **Step 1: Write the failing test**

Add a test that patches `LiveRuntime.start()` and `LiveRuntime.stop()` and asserts `lightfee-live` calls them in a bounded order and always stops on exit.

```python
async def test_live_main_calls_start_then_stop(monkeypatch):
    calls = []

    async def fake_start(self):
        calls.append("start")

    async def fake_stop(self):
        calls.append("stop")

    monkeypatch.setattr(LiveRuntime, "start", fake_start)
    monkeypatch.setattr(LiveRuntime, "stop", fake_stop)
```

- [ ] **Step 2: Run the test and confirm it fails**

Run:

```bash
rtk pytest tests/test_live_startup_preflight.py -q -W error
```

Expected before fix: the test should fail if shutdown ordering or signal wiring is incomplete.

- [ ] **Step 3: Implement the minimal control-plane fix**

Keep `apps/live.py` responsible for signal wiring and final stop. Keep `runtime.py` responsible for the loop mechanics only.

- [ ] **Step 4: Re-run the focused test**

Run:

```bash
rtk pytest tests/test_live_startup_preflight.py -q -W error
```

Expected: pass with no warnings.

## Task 2: Match V1 Lane Scheduling And Backoff

**Files:**
- Modify: `lightfee/engine/runtime.py`
- Modify: `tests/test_runtime_smoke.py`

- [ ] **Step 1: Write the failing test**

Add a test that proves the runtime loop schedules full tick, active-position tick, rate-limit reload, local-L2 sync, passive close, normal exit, maker-event, and post-tick housekeeping as separate lanes.

```python
async def test_run_loop_schedules_all_lanes(monkeypatch, runtime):
    seen = []
    monkeypatch.setattr(runtime, "tick", lambda: seen.append("tick"))
    monkeypatch.setattr(runtime, "tick_active_positions", lambda: seen.append("active"))
```

- [ ] **Step 2: Run the test**

Run:

```bash
rtk pytest tests/test_runtime_smoke.py -q -W error
```

- [ ] **Step 3: Implement the lane wiring**

Preserve the V1 order:

1. full tick
2. active-position fast tick
3. rate-limit reload
4. local-L2 sync
5. passive close lane
6. normal exit lane
7. maker-event lane
8. housekeeping and snapshot persistence

- [ ] **Step 4: Verify warning-clean execution**

Run:

```bash
rtk pytest tests/test_runtime_smoke.py -q -W error
```

## Task 3: Preserve Export And Error Semantics

**Files:**
- Modify: `lightfee/engine/loop_control.py`
- Modify: `lightfee/engine/runtime.py`
- Modify: `tests/test_control_plane.py`

- [ ] **Step 1: Write the failing test**

Add a test that forces a tick error and asserts the runtime records the error, applies backoff, and still performs the export path that V1 performs after a successful tick.

```python
async def test_tick_error_records_backoff_and_exports(monkeypatch, runtime):
    async def boom():
        raise RuntimeError("boom")

    monkeypatch.setattr(runtime, "tick", boom)
```

- [ ] **Step 2: Run the test**

Run:

```bash
rtk pytest tests/test_control_plane.py -q -W error
```

- [ ] **Step 3: Implement the export and backoff semantics**

Match V1's intent:

- failed tick is journaled
- backoff deadline is updated
- export paths stay explicit and bounded
- shutdown still flushes the final state

- [ ] **Step 4: Final verification**

Run:

```bash
rtk pytest tests/test_live_startup_preflight.py tests/test_runtime_smoke.py tests/test_control_plane.py -q -W error
```

## Task 4: Detect Unexpected Blast Radius

**Files:**
- No code edits

- [ ] **Step 1: Check changed symbols**

Run:

```bash
gitnexus_detect_changes({scope: "unstaged", repo: "LightFeeV2"})
```

- [ ] **Step 2: Confirm only orchestration symbols moved**

Expected: changed symbols should stay inside the control-plane surface.

