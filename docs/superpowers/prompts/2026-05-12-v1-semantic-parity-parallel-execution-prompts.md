# V1 Semantic Parity Parallel Execution Prompts

Use these prompts with separate implementation agents. Do not give every agent every prompt. Give each agent only the section assigned to it plus the shared rules.

Repository:
- V2: `/media/wl/新加卷/codex/LightFeeV2`
- V1 reference: `/media/wl/新加卷/codex/LightFee`

Primary plan:
- `/media/wl/新加卷/codex/LightFeeV2/docs/superpowers/plans/2026-05-12-v1-semantic-parity-master-and-subplans.md`

Important:
- This is semantic parity, not code-level replication.
- V1 is the business contract, not source code to copy.
- If V1 implementation is awkward, coupled, or unsafe, implement a cleaner V2-native design that preserves the same observable business meaning.
- All parity claims need tests or an approved deviation.

---

## Shared Rules For Every Implementation Agent

Paste this at the top of every worker prompt.

```text
You are implementing part of LightFeeV2 semantic parity with LightFee V1.

Working directory:
/media/wl/新加卷/codex/LightFeeV2

V1 reference repository:
/media/wl/新加卷/codex/LightFee

Read first:
1. docs/superpowers/plans/2026-05-12-v1-semantic-parity-master-and-subplans.md
2. docs/parity/v1_semantic_contract_catalog.md if it exists
3. docs/parity/approved_deviations.md if it exists

Core rule:
Do NOT do code-level replication. Preserve V1 business semantics: observable inputs, state transitions, venue contracts, journal records, recovery behavior, replay results, reports, and operator-visible outputs. If V1's implementation is poor, use a better V2-native implementation while preserving the same semantic outcome.

Git/worktree safety:
- Do not revert or overwrite unrelated user changes.
- Stay inside your assigned write scope.
- If you discover that another required file outside your scope must change, stop and report the exact dependency instead of editing it.
- Use `rtk` before shell commands.
- Prefer `rg`/`rg --files` for searching.
- Use `apply_patch` for manual edits.

GitNexus/project rule:
- Before editing a function/class/method, run GitNexus impact analysis when available.
- If GitNexus is unavailable or stale, say so in your final summary and continue with source inspection.

Testing rule:
- Write failing tests first for each semantic gap.
- Run the focused tests and report exact commands.
- Do not claim parity for behavior without a passing test or an approved deviation entry.

Final response must include:
1. Files changed.
2. V1 semantic anchors inspected.
3. Tests added/changed.
4. Commands run and results.
5. Any approved deviations added or requested.
6. Any blocked dependencies outside your scope.
```

---

## Prompt 0: Coordinator Gate 0 - Contract Harness

Run this first. Do not start Worker A-F until this is complete.

```text
Use the shared rules above.

Your role:
Coordinator Gate 0. Build the semantic contract harness that all later workers must use.

You own:
- docs/parity/v1_semantic_contract_catalog.md
- docs/parity/approved_deviations.md
- tests/parity/conftest.py
- tests/parity/fixtures/v1_contract_cases.py
- tests/parity/test_contract_catalog_coverage.py

You must not edit production code.

Goal:
Create a catalog and pytest guard for semantic parity coverage. This is not implementation of parity behavior yet. It is the contract inventory and deviation ledger.

Required work:
1. Read the master plan:
   docs/superpowers/plans/2026-05-12-v1-semantic-parity-master-and-subplans.md
2. Inspect V1 and V2 source anchors enough to define categories:
   - config
   - startup
   - runtime lanes
   - opportunity input
   - venue capabilities
   - market data
   - local L2
   - entry
   - close
   - passive close
   - risk
   - state
   - journal
   - recovery
   - replay
   - offline analysis
   - evolution
   - ops
3. Create docs/parity/v1_semantic_contract_catalog.md with one entry per category. Each entry must include:
   - contract id
   - area
   - V1 source anchors
   - V2 source anchors
   - observable behavior
   - required state fields
   - required journal/event kinds
   - focused test path that must cover it
   - deviation id if not implemented yet
4. Create docs/parity/approved_deviations.md with a strict table schema:
   - id
   - area
   - v1_behavior
   - v2_behavior
   - reason
   - risk
   - operator_impact
   - test_coverage
5. Create tests/parity/test_contract_catalog_coverage.py that fails if:
   - a required area is missing from the catalog
   - a catalog entry has neither a test path nor a deviation id
   - a deviation id referenced in the catalog does not exist in approved_deviations.md
6. Add small fixture helpers in tests/parity/fixtures/v1_contract_cases.py for later workers to reuse:
   - make_v1_journal_record(kind, payload)
   - make_v2_journal_record(kind, payload)
   - semantic_summary(records)

Acceptance command:
rtk pytest tests/parity/test_contract_catalog_coverage.py -v

Expected:
Passes after the catalog and deviation ledger are internally consistent.

Final response:
Return changed files, command output summary, and a list of contract ids created.
```

---

## Prompt A: Worker A - Config, Universe, and Opportunity Input

Run in Wave 1 after Prompt 0 is complete. Can run in parallel with Workers B and C.

```text
Use the shared rules above.

Your role:
Worker A: Config, Symbol Universe, and Opportunity Input.

You own:
- lightfee/config/**
- lightfee/sidecar/**
- tests/config/**
- tests/sidecar/**

You may create:
- lightfee/config/universe.py
- tests/config/test_v1_config_semantics.py
- tests/sidecar/test_opportunity_input_semantics.py

You must not edit:
- lightfee/engine/runtime.py
- lightfee/core/contracts.py
- lightfee/core/domain.py
- lightfee/engine/state.py
- lightfee/persistence/**
- lightfee/engine/entry*.py

Goal:
Restore V1 production configuration and opportunity-input semantics in a clean V2-native way.

V1 source anchors to inspect:
- /media/wl/新加卷/codex/LightFee/src/runtime_state/config.rs
- /media/wl/新加卷/codex/LightFee/src/opportunity_input/**
- /media/wl/新加卷/codex/LightFee/src/main.rs around build_opportunity_input_provider

V2 source anchors to inspect:
- lightfee/config/schema.py
- lightfee/config/loaders.py
- lightfee/engine/bootstrap.py
- lightfee/sidecar/**

Required semantics:
1. directed_pairs restrict pair direction independently from global symbols.
2. daily_universe supports enablement, generation time, max symbols, fallback-to-last-good, and path resolution.
3. Runtime opportunity modes distinguish direct market, coarse sidecar, sidecar scan, disabled, and explicit non-parity fallback modes.
4. Sidecar snapshot freshness preserves fresh, last-good fallback, stale, missing, and degraded-domain semantics.
5. Market, transfer, hint, and perp-liquidity health domains survive into runtime diagnostics.
6. Removed Chillybot behavior must be listed as approved deviations, not silently omitted.

Implementation guidance:
- Prefer typed dataclasses plus validation helpers.
- Keep symbol selection in lightfee/config/universe.py.
- Do not wire runtime.py directly. Expose functions/classes for Worker E to consume later.
- Make non-parity sidecar-mid fallback opt-in and visible in config.

Tests to write first:
- tests/config/test_v1_config_semantics.py
  Cover directed_pairs, daily_universe, invalid generation time, max_symbols validation, path resolution, fallback-to-last-good.
- tests/sidecar/test_opportunity_input_semantics.py
  Cover fresh snapshot, stale snapshot, missing snapshot, last-good fallback, degraded market/transfer/hint/liquidity health.

Acceptance commands:
rtk pytest tests/config/test_v1_config_semantics.py -v
rtk pytest tests/sidecar/test_opportunity_input_semantics.py -v
rtk pytest tests/config tests/sidecar -v

Final response:
Return files changed, tests run, V1 anchors inspected, and any deviations added/requested.
```

---

## Prompt B: Worker B - Venue Contract, Market Data, and Local L2

Run in Wave 1 after Prompt 0 is complete. Can run in parallel with Workers A and C.

```text
Use the shared rules above.

Your role:
Worker B: Venue Contract, Market Data, and Local L2.

You own:
- lightfee/core/contracts.py
- lightfee/core/domain.py
- lightfee/venues/**
- lightfee/marketdata/**
- tests/venues/**
- tests/marketdata/**

You may create:
- lightfee/venues/capabilities.py
- tests/venues/test_v1_capability_matrix.py
- tests/venues/test_order_sizing_and_headroom.py
- tests/marketdata/test_local_l2_runtime_targets.py

You must not edit:
- lightfee/engine/entry*.py
- lightfee/engine/passive_close.py
- lightfee/engine/close_executor.py
- lightfee/engine/runtime.py
- lightfee/engine/state.py
- lightfee/persistence/**

Goal:
Restore the full venue-facing semantic contract and local-L2 behavior while using clean V2 adapter composition.

V1 source anchors to inspect:
- /media/wl/新加卷/codex/LightFee/src/market_gateway/ports.rs
- /media/wl/新加卷/codex/LightFee/crates/lightfee-engine/src/lib.rs passive order types
- /media/wl/新加卷/codex/LightFee/src/market_gateway/**
- /media/wl/新加卷/codex/LightFee/src/live/**

V2 source anchors to inspect:
- lightfee/core/contracts.py
- lightfee/core/domain.py
- lightfee/venues/base.py
- lightfee/venues/specs.py
- lightfee/venues/transport.py
- lightfee/marketdata/local_l2.py
- lightfee/marketdata/local_l2_ws.py

Required semantics:
1. Capability matrix matches V1 unless an approved deviation exists.
   Important known drift: V1 marks Bitget/Gate risk_health unsupported; V2 currently marks them supported. Fix or create explicit approved deviation with tests.
2. VenueAdapter contract includes:
   - private health
   - private passive progress
   - private passive progress wakeups
   - passive metadata
   - order sizing spec
   - entry open-notional headroom
   - transfer status
   - supported symbols
   - market-data activity control
   - live startup activation
   - local-L2 reconcile targets
   - worker status
   - cached private health
   - prewarm
   - shutdown
3. Passive order ack/progress types are semantically compatible with V1:
   - optional client_order_id/progress fields
   - Unknown state
   - resting quantity semantics
4. Market snapshot diagnostics preserve missing, stale, partial, degraded, and returned-symbol semantics.
5. Local-L2 preserves bootstrap, sequence gap, checksum failure, stale book, retained snapshot, resume metadata, runtime targets, and active-book budgets.

Implementation guidance:
- Split adapter behavior into facets if useful: public REST, private REST, private stream, local L2, sizing, transfer.
- Keep VenueAdapter as a simple facade for engine consumers.
- Use explicit capability objects instead of `hasattr` checks.
- Do not implement engine behavior here. Expose contracts for Workers D and E.

Tests to write first:
- tests/venues/test_v1_capability_matrix.py
- tests/venues/test_order_sizing_and_headroom.py
- tests/marketdata/test_local_l2_runtime_targets.py

Acceptance commands:
rtk pytest tests/venues/test_v1_capability_matrix.py -v
rtk pytest tests/venues/test_order_sizing_and_headroom.py -v
rtk pytest tests/marketdata/test_local_l2_runtime_targets.py -v
rtk pytest tests/venues tests/marketdata -v

Final response:
Return files changed, tests run, V1 anchors inspected, and any deviations added/requested.
```

---

## Prompt C: Worker C - State, Persistence, Recovery, and Replay

Run in Wave 1 after Prompt 0 is complete. Can run in parallel with Workers A and B.

```text
Use the shared rules above.

Your role:
Worker C: State Model, Persistence, Recovery, and Replay.

You own:
- lightfee/engine/state.py
- lightfee/engine/recovery.py
- lightfee/engine/reconciliation.py
- lightfee/persistence/**
- tests/persistence/**
- tests/recovery/**
- tests/offline/replay/**

You may create:
- tests/persistence/test_v1_state_snapshot_semantics.py
- tests/persistence/test_journal_event_semantics.py
- tests/recovery/test_restart_recovery_semantics.py
- tests/offline/replay/test_replay_semantic_equivalence.py

You must not edit:
- lightfee/engine/entry*.py
- lightfee/engine/close_executor.py
- lightfee/engine/passive_close.py
- lightfee/engine/risk_actions.py
- lightfee/engine/runtime.py
- lightfee/core/contracts.py
- lightfee/core/domain.py

Goal:
Make V2 state, journal, snapshot, recovery, and replay lossless at the business-semantic level.

V1 source anchors to inspect:
- /media/wl/新加卷/codex/LightFee/crates/lightfee-engine/src/lib.rs
- /media/wl/新加卷/codex/LightFee/src/engine/state.rs
- /media/wl/新加卷/codex/LightFee/src/runtime_state/persisted_engine.rs
- /media/wl/新加卷/codex/LightFee/src/runtime_state/sqlite_store.rs
- /media/wl/新加卷/codex/LightFee/src/observability_ops/journal_bridge.rs
- /media/wl/新加卷/codex/LightFee/src/observability_ops/replay_bridge.rs
- /media/wl/新加卷/codex/LightFee/src/engine/recovery.rs

V2 source anchors to inspect:
- lightfee/engine/state.py
- lightfee/engine/recovery.py
- lightfee/engine/reconciliation.py
- lightfee/persistence/**
- lightfee/offline/replay/**

Required semantics:
1. OpenPosition preserves V1 semantic fields:
   review id, origin tags, opportunity source, funding legs, edge breakdowns, transfer state, liquidity source, VWAP, capacity constraints, advisories, blocked reasons, quality markouts, risk/protection PnL, settlement fields.
2. PendingEntry preserves metadata, client ids, leg fills, deadlines, retry state, route, outcome, recovery dedup.
3. EngineState preserves lifecycle, risk mode, operator control, venue health, pending close reconciliation, recovery blocked state, residual repairs, live recovery reduce-only pairs, venue cooldowns, market-data degradations, transfer truth, retained local-L2 books, entry liquidity qualification records.
4. Journal envelope preserves seq, run id, ts, kind, payload, critical durability, malformed-line tolerance, streaming, indexed seek behavior.
5. Replay reconstructs open positions, pending entries, pending closes, lifecycle, risk mode, scan stats, recovery events, risk events, local-L2 events, and timeline.
6. SQLite projections preserve V1-visible facts and cursor idempotency.

Implementation guidance:
- Use versioned snapshot schemas and migrations.
- Keep replay reducers pure and testable.
- Preserve raw payload JSON in projections for auditability.
- Do not wire runtime behavior here. Worker E will consume your public APIs later.

Tests to write first:
- tests/persistence/test_v1_state_snapshot_semantics.py
- tests/persistence/test_journal_event_semantics.py
- tests/recovery/test_restart_recovery_semantics.py
- tests/offline/replay/test_replay_semantic_equivalence.py

Acceptance commands:
rtk pytest tests/persistence/test_v1_state_snapshot_semantics.py -v
rtk pytest tests/persistence/test_journal_event_semantics.py -v
rtk pytest tests/recovery/test_restart_recovery_semantics.py -v
rtk pytest tests/offline/replay/test_replay_semantic_equivalence.py -v
rtk pytest tests/persistence tests/recovery tests/offline/replay -v

Final response:
Return files changed, tests run, V1 anchors inspected, and any deviations added/requested.
```

---

## Prompt D: Worker D - Execution, Close, Passive Close, and Risk

Run in Wave 2 only after Workers A, B, C are merged and Wave 1 Merge Gate passes. Can run in parallel with Workers E and F.

```text
Use the shared rules above.

Your role:
Worker D: Execution, Close, Passive Close, and Risk.

You own:
- lightfee/engine/execution_planner.py
- lightfee/engine/entry.py
- lightfee/engine/entry_sync.py
- lightfee/engine/passive_maker.py
- lightfee/engine/close_executor.py
- lightfee/engine/passive_close.py
- lightfee/engine/risk_actions.py
- lightfee/engine/supervisor.py
- tests/engine/test_entry_semantic_parity.py
- tests/engine/test_close_semantic_parity.py
- tests/engine/test_passive_close_semantic_parity.py
- tests/engine/test_risk_semantic_parity.py

You must not edit:
- lightfee/core/contracts.py
- lightfee/core/domain.py
- lightfee/venues/**
- lightfee/marketdata/**
- lightfee/engine/state.py
- lightfee/persistence/**
- lightfee/engine/runtime.py
- lightfee/apps/live.py

Goal:
Complete core trading semantics while using V2-native explicit state machines where that is cleaner than V1's large execution files.

V1 source anchors to inspect:
- /media/wl/新加卷/codex/LightFee/src/execution_core/entry_execution_planner.rs
- /media/wl/新加卷/codex/LightFee/src/execution_core/entry_sync.rs
- /media/wl/新加卷/codex/LightFee/src/engine/entry.rs
- /media/wl/新加卷/codex/LightFee/src/engine/exit.rs
- /media/wl/新加卷/codex/LightFee/src/engine/risk.rs
- /media/wl/新加卷/codex/LightFee/src/risk.rs
- /media/wl/新加卷/codex/LightFee/src/health.rs

V2 source anchors to inspect:
- lightfee/engine/execution_planner.py
- lightfee/engine/entry.py
- lightfee/engine/entry_sync.py
- lightfee/engine/passive_maker.py
- lightfee/engine/close_executor.py
- lightfee/engine/passive_close.py
- lightfee/engine/risk_actions.py
- lightfee/engine/supervisor.py

Required semantics:
1. Entry planning preserves incremental entry, min-notional, remainder, matched ratio, and reason strings.
2. Entry execution preserves maker/hedge ordering, client-order idempotency, uncertain outcomes, reject classification, residual task creation, pending entry registration, and journal payloads.
3. Passive maker preserves reprice, cancel-replace, amend budget, small-fill buffer, deadline, fallback, and maker-event semantics.
4. Close execution preserves reduce-only, chunking, min-notional dust, residual split, partial close, final close, and PnL attribution.
5. Passive close preserves high-slippage phase, low-slippage phase, zero-fill counters, delta hedge, cumulative hedge catch-up, fallback to dual taker, and terminal cleanup.
6. Risk preserves unsupported snapshot policy, warning/delever/death lines, cooldowns, max steps, single-side protection, fail-closed, and venue-health aggregation.

Implementation guidance:
- Prefer pure transition functions plus thin async side-effect wrappers.
- Keep venue calls at the edges.
- Use structured failure classes where possible.
- Consume Worker B contracts and Worker C state models. Do not change them.

Tests to write first:
- tests/engine/test_entry_semantic_parity.py
- tests/engine/test_close_semantic_parity.py
- tests/engine/test_passive_close_semantic_parity.py
- tests/engine/test_risk_semantic_parity.py

Acceptance commands:
rtk pytest tests/engine/test_entry_semantic_parity.py -v
rtk pytest tests/engine/test_close_semantic_parity.py -v
rtk pytest tests/engine/test_passive_close_semantic_parity.py -v
rtk pytest tests/engine/test_risk_semantic_parity.py -v

Final response:
Return files changed, tests run, V1 anchors inspected, and any deviations added/requested.
```

---

## Prompt E: Worker E - Runtime Control Plane, Scheduling, Observability, and Ops

Run in Wave 2 only after Workers A, B, C are merged and Wave 1 Merge Gate passes. Can run in parallel with Workers D and F.

```text
Use the shared rules above.

Your role:
Worker E: Runtime Control Plane, Scheduling, Observability, and Ops.

You own:
- lightfee/apps/live.py
- lightfee/engine/runtime.py
- lightfee/engine/loop_control.py
- lightfee/ops/**
- tests/engine/test_runtime_lane_scheduling.py
- tests/engine/test_startup_activation_semantics.py
- tests/ops/**

You may create:
- lightfee/ops/metrics.py
- tests/ops/test_current_state_and_metrics_export.py

You must not edit:
- lightfee/config/schema.py
- lightfee/config/loaders.py
- lightfee/core/contracts.py
- lightfee/core/domain.py
- lightfee/venues/**
- lightfee/marketdata/**
- lightfee/engine/state.py
- lightfee/engine/entry*.py
- lightfee/engine/close_executor.py
- lightfee/engine/passive_close.py
- lightfee/persistence/**
- lightfee/offline/**

Goal:
Match V1 production orchestration semantics while keeping V2's asyncio architecture clean and deterministic.

V1 source anchors to inspect:
- /media/wl/新加卷/codex/LightFee/src/main.rs
- /media/wl/新加卷/codex/LightFee/src/app_runtime/**
- /media/wl/新加卷/codex/LightFee/src/runtime_state/**
- /media/wl/新加卷/codex/LightFee/src/observability_ops/**

V2 source anchors to inspect:
- lightfee/apps/live.py
- lightfee/engine/runtime.py
- lightfee/engine/loop_control.py
- lightfee/engine/bootstrap.py
- lightfee/ops/**

Required semantics:
1. Startup phases include config validation, symbol preparation, adapter construction, adapter prewarm, opportunity provider construction, local-L2 activation, recovery, and started journal event.
2. Full tick, active tick, maker-event tick, rate-limit reload, SIGHUP reload, and shutdown are independently scheduled.
3. Per-lane failure backoff does not block unrelated lanes.
4. Post-tick housekeeping exports snapshot, current state, metrics, and journal diagnostics with V1-visible meanings.
5. Shutdown persists final state and exports final current-state snapshot.
6. Operator commands preserve V1 risk-mode and pending-reconcile semantics.

Implementation guidance:
- Use an explicit RuntimeLane abstraction rather than copying V1 tokio select structure.
- Use deterministic fake clocks in tests.
- Separate scheduling from tick handlers.
- Consume Worker A config APIs, Worker B venue APIs, and Worker C state/persistence APIs. Do not change their files.

Tests to write first:
- tests/engine/test_runtime_lane_scheduling.py
- tests/engine/test_startup_activation_semantics.py
- tests/ops/test_current_state_and_metrics_export.py

Acceptance commands:
rtk pytest tests/engine/test_runtime_lane_scheduling.py -v
rtk pytest tests/engine/test_startup_activation_semantics.py -v
rtk pytest tests/ops -v

Final response:
Return files changed, tests run, V1 anchors inspected, and any deviations added/requested.
```

---

## Prompt F: Worker F - Offline Analysis, Evolution, and LLM Evolution

Run in Wave 2 only after Workers A, B, C are merged and Wave 1 Merge Gate passes. Can run in parallel with Workers D and E.

```text
Use the shared rules above.

Your role:
Worker F: Offline Analysis, Evolution, and LLM Evolution.

You own:
- lightfee/offline/analysis/**
- lightfee/offline/reports/**
- lightfee/offline/evolution/**
- lightfee/offline/llm_evolution/**
- tests/offline/** except tests/offline/replay/**

You may create:
- lightfee/offline/llm_evolution/evidence_pack.py
- lightfee/offline/llm_evolution/proposal.py
- tests/offline/test_journal_analysis_semantics.py
- tests/offline/test_evolution_governance_semantics.py
- tests/offline/test_llm_evolution_contracts.py

You must not edit:
- lightfee/persistence/**
- lightfee/engine/state.py
- lightfee/engine/runtime.py
- lightfee/engine/entry*.py
- lightfee/core/**
- lightfee/venues/**
- lightfee/marketdata/**

Goal:
Restore V1 offline business outputs and evolution governance with a simpler V2 pipeline.

V1 source anchors to inspect:
- /media/wl/新加卷/codex/LightFee/src/analysis.rs
- /media/wl/新加卷/codex/LightFee/src/evolution/**
- /media/wl/新加卷/codex/LightFee/src/llm_evolution/**
- /media/wl/新加卷/codex/LightFee/src/offline_replay/**
- /media/wl/新加卷/codex/LightFee/src/observability_ops/**

V2 source anchors to inspect:
- lightfee/offline/analysis/**
- lightfee/offline/reports/**
- lightfee/offline/evolution/**
- lightfee/offline/llm_evolution/**

Required semantics:
1. Journal analysis counts order lifecycle, entry/exit PnL, recovery, risk, scan diagnostics, execution liquidity blocks, local-L2 sequence gaps, local-L2 sync failures, fail-closed reasons, and classification breakdowns.
2. Daily and incident reports preserve V1 operator-facing sections and numeric meanings.
3. Evolution ledger preserves proposal catalog, approval queue, experiment ledger, diagnostics, rendered report, and deterministic cycle results.
4. LLM evolution preserves evidence pack, prompt contract, proposal schema, review, validation, root-cause summary, and disabled/enabled behavior.

Implementation guidance:
- Build reports from projection facts when available and fall back to JSONL scan.
- Separate evidence generation from provider calls.
- Use explicit governance states instead of scattered booleans.
- Consume Worker C's journal/projection contracts. Do not change persistence files.

Tests to write first:
- tests/offline/test_journal_analysis_semantics.py
- tests/offline/test_evolution_governance_semantics.py
- tests/offline/test_llm_evolution_contracts.py

Acceptance commands:
rtk pytest tests/offline/test_journal_analysis_semantics.py -v
rtk pytest tests/offline/test_evolution_governance_semantics.py -v
rtk pytest tests/offline/test_llm_evolution_contracts.py -v
rtk pytest tests/offline -v

Final response:
Return files changed, tests run, V1 anchors inspected, and any deviations added/requested.
```

---

## Optional Prompt: Wave 1 Merge Gate

Use this if a separate integration agent merges Workers A-C before Wave 2 starts.

```text
Use the shared rules above.

Your role:
Wave 1 Merge Gate. Do not implement new feature behavior unless required to resolve integration breakage.

Scope:
Review and integrate outputs from:
- Worker A: Config, Universe, Opportunity Input
- Worker B: Venue Contract, Market Data, Local L2
- Worker C: State, Persistence, Recovery, Replay

Tasks:
1. Inspect git diff and ensure each worker stayed inside its assigned write scope.
2. Resolve import/type/test integration issues only.
3. Do not redesign worker implementations.
4. Run focused suites:
   rtk pytest tests/config tests/sidecar -v
   rtk pytest tests/venues tests/marketdata -v
   rtk pytest tests/persistence tests/recovery tests/offline/replay -v
5. Update docs/parity/approved_deviations.md only if a worker supplied a justified deviation.
6. Report whether Wave 2 may start.

Final response:
Return changed files, test results, conflicts found, and Wave 2 readiness.
```

---

## Optional Prompt: Wave 2 Merge Gate

Use this if a separate integration agent merges Workers D-F before final review.

```text
Use the shared rules above.

Your role:
Wave 2 Merge Gate. Do not implement new feature behavior unless required to resolve integration breakage.

Scope:
Review and integrate outputs from:
- Worker D: Execution, Close, Passive Close, Risk
- Worker E: Runtime Control Plane, Scheduling, Observability, Ops
- Worker F: Offline Analysis, Evolution, LLM Evolution

Tasks:
1. Inspect git diff and ensure each worker stayed inside its assigned write scope.
2. Confirm Worker D did not mutate venue contracts or state schema.
3. Confirm Worker E only wired public interfaces from Workers A-C and did not mutate their owned files.
4. Confirm Worker F consumes journal/projection contracts without changing persistence files.
5. Resolve import/type/test integration issues only.
6. Run focused suites:
   rtk pytest tests/engine/test_entry_semantic_parity.py tests/engine/test_close_semantic_parity.py tests/engine/test_passive_close_semantic_parity.py tests/engine/test_risk_semantic_parity.py -v
   rtk pytest tests/engine/test_runtime_lane_scheduling.py tests/engine/test_startup_activation_semantics.py tests/ops -v
   rtk pytest tests/offline -v
7. Report readiness for final acceptance.

Final response:
Return changed files, test results, conflicts found, and final-review readiness.
```

---

## Prompt For Final Acceptance

Do not give this to implementation workers. This is for the final reviewer.

```text
Use the shared rules above.

Your role:
Final acceptance reviewer for V1 semantic parity. Do not implement new feature behavior unless a tiny integration fix is necessary to run verification.

Scope:
Validate that the merged work achieves semantic parity goals from:
docs/superpowers/plans/2026-05-12-v1-semantic-parity-master-and-subplans.md

Tasks:
1. Inspect the full diff.
2. Confirm every contract entry in docs/parity/v1_semantic_contract_catalog.md has passing test coverage or an approved deviation.
3. Confirm docs/parity/approved_deviations.md contains only intentional deviations with operator impact and test coverage.
4. Search for hidden production default non-parity paths:
   rtk rg -n "non-parity|fallback|TODO|not implemented|pass" lightfee tests docs/parity
5. Run parity suites:
   rtk pytest tests/parity -v
   rtk pytest tests/config tests/sidecar tests/venues tests/marketdata tests/persistence tests/recovery tests/offline tests/engine tests/ops -v
6. Run GitNexus detect changes if available.
7. Produce final verdict:
   - PASS
   - PASS with approved deviations
   - FAIL with blocking gaps

Final response:
Lead with the verdict. Then list blocking issues by file/line, test evidence, approved deviations, and residual risk.
```
