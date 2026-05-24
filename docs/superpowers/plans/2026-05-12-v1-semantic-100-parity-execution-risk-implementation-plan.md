# V1 Semantic 100% Parity Execution and Risk Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. This plan owns entry, close, passive close, residual repair, and risk actions.

**Goal:** Make LightFeeV2 entry/exit/risk behavior semantically equivalent to V1 live execution.

**Architecture:** Keep the runtime as the orchestrator. Entry planning, entry sync, close execution, passive close, residual repair, and risk decisions stay in dedicated modules. All order lifecycle journal payloads must remain recoverable by the persistence workstream.

**Tech Stack:** Python 3.12, asyncio, dataclasses, pytest, existing venue adapters and local-L2 runtime, GitNexus MCP, Rust V1 source under `/media/wl/新加卷/codex/LightFee`.

---

## Reference Docs

- Master spec: `docs/superpowers/specs/2026-05-12-v1-semantic-100-parity-design.md`
- Existing passive close plan: `docs/superpowers/plans/2026-05-11-v1-passive-close-gap-closure-implementation-plan.md`
- Rust V1 anchors:
  - `/media/wl/新加卷/codex/LightFee/src/execution_core/entry_sync.rs`
  - `/media/wl/新加卷/codex/LightFee/src/execution_core/entry_execution_planner.rs`
  - `/media/wl/新加卷/codex/LightFee/src/engine/entry.rs`
  - `/media/wl/新加卷/codex/LightFee/src/engine/exit.rs`
  - `/media/wl/新加卷/codex/LightFee/src/engine/risk.rs`
  - `/media/wl/新加卷/codex/LightFee/src/engine/supervision.rs`

## File Ownership

- Modify: `lightfee/engine/entry.py`
- Modify: `lightfee/engine/entry_sync.py`
- Modify: `lightfee/engine/execution_planner.py`
- Modify: `lightfee/engine/entry_local_l2.py`
- Modify: `lightfee/engine/exit.py`
- Modify: `lightfee/engine/exit_decision.py`
- Modify: `lightfee/engine/close_executor.py`
- Modify: `lightfee/engine/passive_close.py`
- Modify: `lightfee/engine/reconciliation.py`
- Modify: `lightfee/engine/residual.py`
- Modify: `lightfee/engine/risk_actions.py`
- Modify: `lightfee/engine/supervisor.py`
- Modify: `tests/test_entry_sync.py`
- Modify: `tests/test_runtime_entry_flow.py`
- Modify: `tests/test_entry_planner.py`
- Modify: `tests/test_close_execution.py`
- Modify: `tests/test_passive_close.py`
- Modify: `tests/test_exit_decisions.py`
- Modify: `tests/test_risk_actions.py`
- Modify: `tests/test_supervisor_execution.py`

Coordinate before changing `lightfee/core/domain.py` or `lightfee/engine/state.py`.

## Task 1: Align Entry Planning And Order Lifecycle

**Files:**
- Modify: `lightfee/engine/execution_planner.py`
- Modify: `lightfee/engine/entry.py`
- Modify: `lightfee/engine/entry_sync.py`
- Modify: `tests/test_entry_planner.py`
- Modify: `tests/test_entry_sync.py`
- Modify: `tests/test_runtime_entry_flow.py`

- [ ] **Step 1: Run impact analysis**

Before editing:

```text
gitnexus_impact({target: "plan_incremental_entry_execution", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "EntrySyncExecutor", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Write failing tests**

Add tests for:

- planner route is consumed by entry dispatch
- maker leg is not hardcoded unless the V1 config says so
- no pseudo-price entry is created when quote is invalid
- post-only maker and IOC hedge flags match V1
- partial fill creates `PendingEntry`
- hedge rejection creates residual repair evidence

- [ ] **Step 3: Implement minimal entry alignment**

Keep route computation in `execution_planner.py`. Keep order construction in `entry.py`. Keep live submission and outcome classification in `entry_sync.py`.

- [ ] **Step 4: Verify**

Run:

```bash
rtk pytest tests/test_entry_planner.py tests/test_entry_sync.py tests/test_runtime_entry_flow.py -q -W error
```

## Task 2: Align Close And Passive Close Semantics

**Files:**
- Modify: `lightfee/engine/close_executor.py`
- Modify: `lightfee/engine/passive_close.py`
- Modify: `lightfee/engine/exit.py`
- Modify: `lightfee/engine/exit_decision.py`
- Modify: `tests/test_close_execution.py`
- Modify: `tests/test_passive_close.py`
- Modify: `tests/test_exit_decisions.py`

- [ ] **Step 1: Run impact analysis**

Before editing:

```text
gitnexus_impact({target: "CloseExecutor", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "PassiveCloseExecutor", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Write failing tests**

Add or extend tests for:

- normal close reasons route passive maker+taker when V1 does
- hard stop, risk delever, and protection remain aggressive
- passive close maker leg is reduce-only post-only GTC
- passive close hedge leg is reduce-only IOC
- maker fill deltas, not full chunk quantities, drive hedges
- chunk advance preserves cumulative fill state
- passive close falls back to dual taker only when V1 would

- [ ] **Step 3: Implement passive close and close routing alignment**

Keep `PendingClose` and `PendingPassiveClose` distinct. Do not represent passive close as a regular aggressive close with a different reason string.

- [ ] **Step 4: Verify**

Run:

```bash
rtk pytest tests/test_close_execution.py tests/test_passive_close.py tests/test_exit_decisions.py -q -W error
```

## Task 3: Align Risk And Supervision Actions

**Files:**
- Modify: `lightfee/engine/risk_actions.py`
- Modify: `lightfee/engine/supervisor.py`
- Modify: `tests/test_risk_actions.py`
- Modify: `tests/test_supervisor_execution.py`

- [ ] **Step 1: Run impact analysis**

Before editing:

```text
gitnexus_impact({target: "Supervisor", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "plan_risk_action", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Write failing tests**

Add tests for:

- warning trigger and clear payloads preserve health ratios
- death-line single-side protection submits a real protective close
- partial delever obeys cooldown and max step count
- unsupported venue risk health becomes snapshot unavailable, not capability unsupported

- [ ] **Step 3: Implement risk action alignment**

Keep decision logic in `risk_actions.py` and side-effect execution in `supervisor.py`.

- [ ] **Step 4: Verify**

Run:

```bash
rtk pytest tests/test_risk_actions.py tests/test_supervisor_execution.py -q -W error
```

## Task 4: Validate Execution Workstream Scope

**Files:**
- No code edits

- [ ] **Step 1: Run execution suite**

Run:

```bash
rtk pytest tests/test_entry_sync.py tests/test_runtime_entry_flow.py tests/test_close_execution.py tests/test_passive_close.py tests/test_risk_actions.py tests/test_supervisor_execution.py -q -W error
```

- [ ] **Step 2: Run GitNexus change detection**

Run:

```bash
gitnexus_detect_changes({scope: "unstaged", repo: "LightFeeV2"})
```

Expected: changed symbols should stay inside execution/risk modules and tests, except for explicitly coordinated shared contracts.

