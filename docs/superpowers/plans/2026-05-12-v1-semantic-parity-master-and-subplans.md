# V1 Semantic Parity Master Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement each child plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make LightFeeV2 behaviorally equivalent to LightFee V1 across production business semantics, excluding code-level replication.

**Architecture:** Treat V1 as the business contract, not as source code to copy. For every V1 behavior, define observable inputs, state transitions, journal records, recovery behavior, and operator-visible outputs; then implement the cleanest V2-native design that preserves those semantics. Approved improvements are allowed when they keep the same business meaning and produce equal or stronger invariants.

**Tech Stack:** Python, pytest, asyncio, dataclasses, JSONL journal, SQLite projections, GitNexus, existing LightFeeV2 modules.

---

## Parity Principles

- [ ] Preserve V1 business semantics, not V1 incidental structure.
- [ ] Prefer explicit contracts over implicit V1 coupling.
- [ ] Prefer smaller V2-native services over large V1-style monolith files.
- [ ] Every parity claim must have an executable test or a named approved deviation.
- [ ] Every lossy mapping from V1 state, config, event, or journal payload must be either fixed or documented as an approved deviation.
- [ ] If V1 behavior is unsafe or overly complex, keep the same observable business outcome with a stricter implementation: stronger validation, clearer state machines, idempotent persistence, deterministic scheduling, and better failure classification.

## Program Acceptance Gates

- [ ] A V1 semantic contract catalog exists for config, runtime lanes, venue capabilities, state, journal, recovery, execution, risk, offline reports, and evolution.
- [ ] V2 has a parity test suite grouped by child plan, with every previously identified gap covered by at least one test.
- [ ] No V2 fallback is labeled "non-parity" in a production default path unless it is explicitly configured and documented as a deviation.
- [ ] V2 startup, shutdown, tick scheduling, recovery, and journal replay are deterministic in tests.
- [ ] Current-state export, metrics, and journal facts preserve operator-facing V1 meanings.
- [ ] V1 fixture journals and V2 fixture journals replay to equivalent semantic summaries.
- [ ] GitNexus detect-changes is clean after each implementation batch.

## Child Plan Overview

1. Contract Catalog and Parity Harness
2. Config, Symbol Universe, and Opportunity Input
3. Runtime Control Plane, Scheduling, Observability, and Ops
4. Venue Contract, Market Data, and Local L2
5. Execution, Close, Passive Close, and Risk
6. State Model, Persistence, Recovery, and Replay
7. Offline Analysis, Evolution, and LLM Evolution
8. Final Conformance, Deviation Ledger, and Rollout

---

## Child Plan 1: Contract Catalog and Parity Harness

**Goal:** Build the semantic inventory and test harness that every other plan uses.

**Primary files:**
- Create: `docs/parity/v1_semantic_contract_catalog.md`
- Create: `docs/parity/approved_deviations.md`
- Create: `tests/parity/conftest.py`
- Create: `tests/parity/fixtures/v1_contract_cases.py`
- Create: `tests/parity/test_contract_catalog_coverage.py`

**Work items:**

- [ ] Define contract categories: config, startup, runtime lanes, opportunity input, venue capabilities, market data, local L2, entry, close, passive close, risk, state, journal, recovery, replay, offline analysis, evolution, ops.
- [ ] For each category, list V1 source anchor, V2 source anchor, observable behavior, required journal kinds, required state fields, and allowed implementation improvements.
- [ ] Create `approved_deviations.md` with a strict schema: `id`, `area`, `v1_behavior`, `v2_behavior`, `reason`, `risk`, `operator_impact`, `test_coverage`.
- [ ] Add a pytest guard that fails when a contract category has no V2 coverage test or no approved deviation.
- [ ] Add fixture builders for synthetic V1 events and V2 events so later plans can compare semantic summaries instead of raw line-by-line code.

**Acceptance:**
- [ ] `pytest tests/parity/test_contract_catalog_coverage.py -v` passes.
- [ ] Every child plan references catalog entries instead of rediscovering V1 behavior ad hoc.

---

## Child Plan 2: Config, Symbol Universe, and Opportunity Input

**Goal:** Restore V1 production configuration and opportunity-input semantics with cleaner V2 parsing and validation.

**Primary files:**
- Modify: `lightfee/config/schema.py`
- Modify: `lightfee/config/loaders.py`
- Modify: `lightfee/engine/bootstrap.py`
- Modify: `lightfee/sidecar/service.py`
- Modify: `lightfee/sidecar/snapshot.py`
- Modify: `lightfee/sidecar/pairing.py`
- Create: `lightfee/config/universe.py`
- Create: `tests/config/test_v1_config_semantics.py`
- Create: `tests/sidecar/test_opportunity_input_semantics.py`

**Required V1 semantics:**

- [ ] `directed_pairs` must restrict pair direction independently from global symbols.
- [ ] `daily_universe` must support enablement, generation time, max symbols, fallback-to-last-good, and path resolution.
- [ ] Runtime opportunity modes must distinguish direct market, coarse sidecar, sidecar scan, and disabled/explicit non-parity modes.
- [ ] Sidecar snapshot freshness must preserve V1 meanings: fresh, last-good fallback, stale, missing, and degraded domains.
- [ ] Market, transfer, hint, and perp-liquidity health domains must survive into runtime diagnostics.
- [ ] Chillybot-era inputs that are intentionally removed must become approved deviations, not silent omissions.

**Better V2 implementation direction:**

- [ ] Use typed config dataclasses with validation functions that return structured error objects.
- [ ] Keep symbol selection in `lightfee/config/universe.py` instead of scattering pair filtering in runtime code.
- [ ] Make non-parity sidecar-mid fallback opt-in and visible in config.

**Acceptance:**
- [ ] V1 config fixtures load into equivalent V2 semantic config objects.
- [ ] Missing or contradictory config fails before runtime starts.
- [ ] Runtime cannot silently trade symbols outside `directed_pairs` or `daily_universe`.

---

## Child Plan 3: Runtime Control Plane, Scheduling, Observability, and Ops

**Goal:** Match V1 production orchestration semantics while keeping V2's asyncio architecture.

**Primary files:**
- Modify: `lightfee/apps/live.py`
- Modify: `lightfee/engine/runtime.py`
- Modify: `lightfee/engine/loop_control.py`
- Modify: `lightfee/engine/bootstrap.py`
- Modify: `lightfee/ops/commands.py`
- Create: `lightfee/ops/metrics.py`
- Create: `tests/engine/test_runtime_lane_scheduling.py`
- Create: `tests/engine/test_startup_activation_semantics.py`
- Create: `tests/ops/test_current_state_and_metrics_export.py`

**Required V1 semantics:**

- [ ] Startup phases must include config validation, symbol preparation, adapter construction, adapter prewarm, opportunity provider construction, local-L2 activation, recovery, and started journal event.
- [ ] Full tick, active tick, maker-event tick, rate-limit reload, SIGHUP reload, and shutdown must be independently scheduled.
- [ ] Per-lane failure backoff must not block unrelated lanes.
- [ ] Post-tick housekeeping must export snapshot, current state, metrics, and journal diagnostics with V1-visible meanings.
- [ ] Shutdown must persist final state and export final current-state snapshot.
- [ ] Operator commands must preserve V1 risk-mode and pending-reconcile semantics.

**Better V2 implementation direction:**

- [ ] Use one explicit `RuntimeLane` abstraction per lane instead of copying V1's `tokio::select!` shape.
- [ ] Use deterministic fake clocks in tests.
- [ ] Separate scheduling from business tick handlers to make lane fairness testable.

**Acceptance:**
- [ ] A slow or failing full tick does not prevent active or maker-event lane eligibility.
- [ ] SIGHUP and periodic reload share the same idempotent reload path.
- [ ] Current-state and metrics output contains every V1 operator-visible field or an approved deviation.

---

## Child Plan 4: Venue Contract, Market Data, and Local L2

**Goal:** Restore the full venue-facing semantic contract and local-L2 behavior, using V2-native adapter composition.

**Primary files:**
- Modify: `lightfee/core/contracts.py`
- Modify: `lightfee/core/domain.py`
- Modify: `lightfee/venues/base.py`
- Modify: `lightfee/venues/specs.py`
- Modify: `lightfee/venues/transport.py`
- Modify: `lightfee/marketdata/local_l2.py`
- Modify: `lightfee/marketdata/local_l2_ws.py`
- Create: `lightfee/venues/capabilities.py`
- Create: `tests/venues/test_v1_capability_matrix.py`
- Create: `tests/venues/test_order_sizing_and_headroom.py`
- Create: `tests/marketdata/test_local_l2_runtime_targets.py`

**Required V1 semantics:**

- [ ] Capability matrix must match V1 unless an approved deviation exists, including Bitget/Gate risk-health support.
- [ ] Venue adapter contract must include private health, private passive progress, private wakeups, passive metadata, order sizing spec, entry open-notional headroom, transfer status, supported symbols, market-data activity control, live startup activation, local-L2 reconcile targets, worker status, cached private health, prewarm, and shutdown.
- [ ] Passive order ack/progress types must be semantically compatible with V1, including optional fields and unknown states.
- [ ] Market snapshot diagnostics must preserve missing, stale, partial, degraded, and returned-symbol semantics.
- [ ] Local-L2 must preserve bootstrap, sequence gap, checksum failure, stale book, retained snapshot, resume metadata, runtime targets, and active-book budgets.

**Better V2 implementation direction:**

- [ ] Split venue adapters into public REST, private REST, private stream, local-L2, sizing, and transfer facets.
- [ ] Keep `VenueAdapter` as a facade so engine code remains simple.
- [ ] Use capability objects to drive behavior instead of `hasattr` checks.

**Acceptance:**
- [ ] Venue capability tests fail on undocumented V1/V2 drift.
- [ ] Local-L2 tests cover restart/resume and degradation recovery.
- [ ] No production engine path assumes a venue capability that the adapter has not declared.

---

## Child Plan 5: Execution, Close, Passive Close, and Risk

**Goal:** Complete core trading semantics without copying V1's large execution files.

**Primary files:**
- Modify: `lightfee/engine/execution_planner.py`
- Modify: `lightfee/engine/entry.py`
- Modify: `lightfee/engine/entry_sync.py`
- Modify: `lightfee/engine/passive_maker.py`
- Modify: `lightfee/engine/close_executor.py`
- Modify: `lightfee/engine/passive_close.py`
- Modify: `lightfee/engine/risk_actions.py`
- Modify: `lightfee/engine/supervisor.py`
- Create: `tests/engine/test_entry_semantic_parity.py`
- Create: `tests/engine/test_close_semantic_parity.py`
- Create: `tests/engine/test_passive_close_semantic_parity.py`
- Create: `tests/engine/test_risk_semantic_parity.py`

**Required V1 semantics:**

- [ ] Entry planning must preserve incremental entry, min-notional, remainder, matched ratio, and reason strings.
- [ ] Entry execution must preserve maker/hedge ordering, client-order idempotency, uncertain outcomes, reject classification, residual task creation, pending entry registration, and journal payloads.
- [ ] Passive maker must preserve reprice, cancel-replace, amend budget, small-fill buffer, deadline, fallback, and maker-event semantics.
- [ ] Close execution must preserve reduce-only, chunking, min-notional dust, residual split, partial close, final close, and PnL attribution.
- [ ] Passive close must preserve high-slippage phase, low-slippage phase, zero-fill counters, delta hedge, cumulative hedge catch-up, fallback to dual taker, and terminal cleanup.
- [ ] Risk must preserve unsupported snapshot policy, warning/delever/death lines, cooldowns, max steps, single-side protection, fail-closed, and venue-health aggregation.

**Better V2 implementation direction:**

- [ ] Model entry/close/passive close as explicit state machines with transition tables.
- [ ] Keep venue side effects at edges; state transitions should be pure and testable.
- [ ] Use structured failure classes instead of stringly typed exceptions where possible.

**Acceptance:**
- [ ] Every V1 execution outcome class has a V2 test: filled, rejected, uncertain, partial, residual, dust, fallback, protection, and fail-closed.
- [ ] Journal events generated by V2 map to V1 semantic summaries.
- [ ] Risk actions cannot execute without a durable journal transition.

---

## Child Plan 6: State Model, Persistence, Recovery, and Replay

**Goal:** Make V2 state, journal, snapshot, and replay lossless at the business-semantic level.

**Primary files:**
- Modify: `lightfee/engine/state.py`
- Modify: `lightfee/engine/recovery.py`
- Modify: `lightfee/engine/reconciliation.py`
- Modify: `lightfee/persistence/journal.py`
- Modify: `lightfee/persistence/snapshot_store.py`
- Modify: `lightfee/persistence/sqlite_store.py`
- Modify: `lightfee/persistence/projection_writer.py`
- Create: `tests/persistence/test_v1_state_snapshot_semantics.py`
- Create: `tests/persistence/test_journal_event_semantics.py`
- Create: `tests/recovery/test_restart_recovery_semantics.py`
- Create: `tests/offline/replay/test_replay_semantic_equivalence.py`

**Required V1 semantics:**

- [ ] Open position state must preserve review id, origin tags, opportunity source, funding legs, edge breakdowns, transfer state, liquidity source, VWAP, capacity constraints, advisories, blocked reasons, quality markouts, risk/protection PnL, and settlement fields.
- [ ] Pending entry state must preserve metadata, client ids, leg fills, deadlines, retry state, route, outcome, and recovery dedup.
- [ ] Engine state must preserve lifecycle, risk mode, operator control, venue health, pending close reconciliation, recovery blocked state, residual repairs, live recovery reduce-only pairs, venue cooldowns, market-data degradations, transfer truth, retained local-L2 books, and entry liquidity qualification records.
- [ ] Journal envelope must preserve seq, run id, ts, kind, payload, critical durability, malformed-line tolerance, streaming, and indexed seek behavior.
- [ ] Replay must reconstruct open positions, pending entries, pending closes, lifecycle, risk mode, scan stats, recovery events, risk events, local-L2 events, and timeline.
- [ ] SQLite projections must preserve V1-visible facts and cursor idempotency.

**Better V2 implementation direction:**

- [ ] Use versioned snapshot schemas with forward/backward migration.
- [ ] Use pure replay reducers that are tested independently from file I/O.
- [ ] Keep projection tables normalized but preserve raw payload JSON for auditability.

**Acceptance:**
- [ ] V1-style fixture snapshots round-trip through V2 without semantic loss.
- [ ] Replaying the same journal twice is idempotent.
- [ ] Recovery blocks live trading when V1 would block, and resumes only when V1 would resume.

---

## Child Plan 7: Offline Analysis, Evolution, and LLM Evolution

**Goal:** Restore V1 offline business outputs and evolution governance with a simpler V2 pipeline.

**Primary files:**
- Modify: `lightfee/offline/analysis/journal.py`
- Modify: `lightfee/offline/analysis/incident.py`
- Modify: `lightfee/offline/reports/daily.py`
- Modify: `lightfee/offline/reports/render.py`
- Modify: `lightfee/offline/evolution/cycle.py`
- Modify: `lightfee/offline/evolution/ledger.py`
- Modify: `lightfee/offline/evolution/approval.py`
- Modify: `lightfee/offline/evolution/report.py`
- Modify: `lightfee/offline/llm_evolution/report.py`
- Create: `lightfee/offline/llm_evolution/evidence_pack.py`
- Create: `lightfee/offline/llm_evolution/proposal.py`
- Create: `tests/offline/test_journal_analysis_semantics.py`
- Create: `tests/offline/test_evolution_governance_semantics.py`
- Create: `tests/offline/test_llm_evolution_contracts.py`

**Required V1 semantics:**

- [ ] Journal analysis must count order lifecycle, entry/exit PnL, recovery, risk, scan diagnostics, execution liquidity blocks, local-L2 sequence gaps, local-L2 sync failures, fail-closed reasons, and classification breakdowns.
- [ ] Daily and incident reports must preserve V1 operator-facing sections and numeric meanings.
- [ ] Evolution ledger must preserve proposal catalog, approval queue, experiment ledger, diagnostics, rendered report, and deterministic cycle results.
- [ ] LLM evolution must preserve evidence pack, prompt contract, proposal schema, review, validation, root-cause summary, and disabled/enabled behavior.

**Better V2 implementation direction:**

- [ ] Build reports from projection facts when available and fall back to JSONL scan.
- [ ] Make LLM evolution deterministic by separating evidence generation from provider calls.
- [ ] Use explicit governance states instead of scattered booleans.

**Acceptance:**
- [ ] V1 fixture journals produce equivalent V2 daily and incident summaries.
- [ ] Evolution proposals cannot be executed without approval state.
- [ ] LLM evolution disabled mode produces auditable no-op output, not silent absence.

---

## Child Plan 8: Final Conformance, Deviation Ledger, and Rollout

**Goal:** Prove the program is complete, identify approved deviations, and make rollout safe.

**Primary files:**
- Modify: `docs/parity/v1_semantic_contract_catalog.md`
- Modify: `docs/parity/approved_deviations.md`
- Create: `tests/parity/test_end_to_end_v1_semantic_conformance.py`
- Create: `tests/parity/test_approved_deviation_ledger.py`
- Create: `docs/parity/rollout_checklist.md`

**Work items:**

- [ ] Add an end-to-end fixture: config load, startup, sidecar snapshot, entry, passive close, risk event, recovery, replay, report.
- [ ] Compare V2 output to V1 semantic summary, not raw code structure.
- [ ] Require every mismatch to link to an approved deviation.
- [ ] Add rollout checklist: paper mode, shadow mode, small notional, production guarded mode, rollback.
- [ ] Run full test suite and GitNexus detect-changes.

**Acceptance:**
- [ ] `pytest tests/parity -v` passes.
- [ ] Full relevant suite passes.
- [ ] `docs/parity/approved_deviations.md` contains only intentional deviations with tests.
- [ ] Operators can understand what changed from V1 without reading code.

---

## Recommended Execution Order

1. Child Plan 1, because it prevents future subjective parity claims.
2. Child Plan 2, because config and opportunity input determine what the runtime is allowed to trade.
3. Child Plan 4, because execution correctness depends on venue and market-data contracts.
4. Child Plan 6, because state and recovery must be lossless before expanding execution behavior.
5. Child Plan 5, because execution/risk can then rely on stable contracts and persistence.
6. Child Plan 3, because orchestration should wire already-correct services.
7. Child Plan 7, because offline analysis/evolution consumes journal and projection semantics.
8. Child Plan 8, because it is the final conformance and rollout gate.

## Parallel Execution Split

The safest split is **six implementation work packages**, executed in **two parallel waves**, with one coordinator-owned gate before and after them.

Do not run all eight child plans in parallel. Child Plan 1 and Child Plan 8 are coordinator gates, not normal implementation tracks. Runtime wiring also depends on config, venue, state, and execution interfaces, so it should not be mixed into the first wave.

### Coordinator-Owned Gate 0: Contract Harness

**Owner:** Final reviewer/coordinator.

**Scope:**
- Child Plan 1: Contract Catalog and Parity Harness.
- Define the contract catalog, approved deviation schema, parity fixture shape, and coverage guard.

**Reason:**
- All workers need the same definition of "same semantics".
- This prevents six workers from inventing six incompatible parity standards.

**Exit gate:**
- `docs/parity/v1_semantic_contract_catalog.md` exists.
- `docs/parity/approved_deviations.md` exists.
- `tests/parity/test_contract_catalog_coverage.py -v` passes.

### Wave 1: Foundation Tracks, Run in Parallel

#### Worker A: Config, Universe, and Opportunity Input

**Child plan:** Child Plan 2.

**Owns:**
- `lightfee/config/**`
- `lightfee/sidecar/**`
- `tests/config/**`
- `tests/sidecar/**`

**Avoids:**
- `lightfee/engine/runtime.py`
- `lightfee/core/contracts.py`
- `lightfee/engine/state.py`

**Delivers:**
- Typed config schema and validation.
- Directed-pair and daily-universe service.
- Opportunity-input snapshot and health-domain semantics.
- Explicit approved deviations for removed Chillybot behavior.

#### Worker B: Venue Contract, Market Data, and Local L2

**Child plan:** Child Plan 4.

**Owns:**
- `lightfee/core/contracts.py`
- `lightfee/core/domain.py`
- `lightfee/venues/**`
- `lightfee/marketdata/**`
- `tests/venues/**`
- `tests/marketdata/**`

**Avoids:**
- `lightfee/engine/entry*.py`
- `lightfee/engine/passive_close.py`
- `lightfee/engine/runtime.py`

**Delivers:**
- V1-compatible capability matrix or approved deviations.
- Venue adapter facade plus facets/capability objects.
- Passive order ack/progress semantic compatibility.
- Local-L2 bootstrap, resume, degradation, and runtime target contracts.

#### Worker C: State, Persistence, Recovery, and Replay

**Child plan:** Child Plan 6.

**Owns:**
- `lightfee/engine/state.py`
- `lightfee/engine/recovery.py`
- `lightfee/engine/reconciliation.py`
- `lightfee/persistence/**`
- `tests/persistence/**`
- `tests/recovery/**`
- `tests/offline/replay/**`

**Avoids:**
- `lightfee/engine/entry*.py`
- `lightfee/engine/close_executor.py`
- `lightfee/engine/passive_close.py`
- `lightfee/engine/runtime.py`

**Delivers:**
- Versioned V1-compatible state model.
- Snapshot migration and replay reducers.
- Journal envelope, critical durability, streaming, indexed seek, and SQLite projection semantics.
- Recovery block/resume semantics.

### Wave 1 Merge Gate

**Owner:** Final reviewer/coordinator.

**Checks:**
- Workers A, B, and C do not edit each other's owned files.
- Config service, venue contract, and state schema are stable enough for Wave 2.
- Focused suites pass:
  - `pytest tests/config tests/sidecar -v`
  - `pytest tests/venues tests/marketdata -v`
  - `pytest tests/persistence tests/recovery tests/offline/replay -v`

### Wave 2: Consumer Tracks, Run in Parallel

#### Worker D: Execution, Close, Passive Close, and Risk

**Child plan:** Child Plan 5.

**Owns:**
- `lightfee/engine/execution_planner.py`
- `lightfee/engine/entry.py`
- `lightfee/engine/entry_sync.py`
- `lightfee/engine/passive_maker.py`
- `lightfee/engine/close_executor.py`
- `lightfee/engine/passive_close.py`
- `lightfee/engine/risk_actions.py`
- `lightfee/engine/supervisor.py`
- `tests/engine/test_entry_semantic_parity.py`
- `tests/engine/test_close_semantic_parity.py`
- `tests/engine/test_passive_close_semantic_parity.py`
- `tests/engine/test_risk_semantic_parity.py`

**Avoids:**
- `lightfee/core/contracts.py`
- `lightfee/core/domain.py`
- `lightfee/engine/state.py`
- `lightfee/engine/runtime.py`

**Delivers:**
- Pure state-machine style execution transitions.
- Entry, close, passive-close, and risk parity tests.
- Journal payload compatibility for execution outcomes.

#### Worker E: Runtime Control Plane, Scheduling, Observability, and Ops

**Child plan:** Child Plan 3.

**Owns:**
- `lightfee/apps/live.py`
- `lightfee/engine/runtime.py`
- `lightfee/engine/loop_control.py`
- `lightfee/ops/**`
- `tests/engine/test_runtime_lane_scheduling.py`
- `tests/engine/test_startup_activation_semantics.py`
- `tests/ops/**`

**Avoids:**
- `lightfee/config/schema.py`
- `lightfee/core/contracts.py`
- `lightfee/engine/state.py`
- `lightfee/engine/entry*.py`
- `lightfee/persistence/**`

**Delivers:**
- Deterministic asyncio lane scheduler.
- Startup and shutdown parity.
- SIGHUP/rate-limit reload parity.
- Current-state and metrics export parity.

#### Worker F: Offline Analysis, Evolution, and LLM Evolution

**Child plan:** Child Plan 7.

**Owns:**
- `lightfee/offline/analysis/**`
- `lightfee/offline/reports/**`
- `lightfee/offline/evolution/**`
- `lightfee/offline/llm_evolution/**`
- `tests/offline/**` except `tests/offline/replay/**`

**Avoids:**
- `lightfee/persistence/**`
- `lightfee/engine/state.py`
- `lightfee/engine/runtime.py`

**Delivers:**
- Journal analysis and report parity.
- Evolution governance state machine.
- LLM evidence/proposal/review/validation contracts.
- Disabled-mode auditable no-op behavior.

### Wave 2 Merge Gate

**Owner:** Final reviewer/coordinator.

**Checks:**
- Worker D did not mutate foundational contracts from Worker B or state schema from Worker C.
- Worker E only wires stable public interfaces.
- Worker F consumes journal/projection contracts without changing them.
- Focused suites pass:
  - `pytest tests/engine/test_entry_semantic_parity.py tests/engine/test_close_semantic_parity.py tests/engine/test_passive_close_semantic_parity.py tests/engine/test_risk_semantic_parity.py -v`
  - `pytest tests/engine/test_runtime_lane_scheduling.py tests/engine/test_startup_activation_semantics.py tests/ops -v`
  - `pytest tests/offline -v`

### Coordinator-Owned Gate 3: Total Acceptance

**Owner:** Final reviewer/coordinator.

**Scope:**
- Child Plan 8: Final Conformance, Deviation Ledger, and Rollout.

**Checks:**
- Run focused parity suites.
- Run full relevant suite.
- Run GitNexus detect-changes.
- Audit `docs/parity/approved_deviations.md`.
- Confirm no production default path still carries a hidden "non-parity" fallback.
- Confirm V1 fixture journals and V2 fixture journals replay to equivalent semantic summaries.

### Why Six Workers

- Fewer than six leaves too much surface area per worker and slows the program.
- More than six creates write conflicts in `runtime.py`, `state.py`, `contracts.py`, and journal/projection files.
- Six keeps each worker bounded by business domain while still allowing real parallelism.
- Two waves avoid the main dependency trap: execution and runtime should consume stable config, venue, and state contracts instead of changing them at the same time.

## Implementation Policy

- [ ] Do not merge a child plan unless its focused tests pass.
- [ ] Do not claim parity for a V1 behavior until the catalog entry links to a passing test.
- [ ] Do not remove a V1 behavior because it looks obsolete; either implement the same semantic result or add an approved deviation.
- [ ] Prefer V2-native design when V1's implementation is overly coupled, but keep V1-visible behavior stable.
- [ ] Keep commits per child plan small enough to review independently.
