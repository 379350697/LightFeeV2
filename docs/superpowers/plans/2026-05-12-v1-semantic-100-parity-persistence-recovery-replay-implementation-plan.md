# V1 Semantic 100% Parity Persistence, Recovery, and Replay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. This plan owns journal fidelity, persisted state, recovery, metrics, replay, and recovery diagnostics.

**Goal:** Make LightFeeV2 persistence, recovery, and replay semantics lossless against V1.

**Architecture:** Journal is the event source, snapshot is the fast recovery state, and replay is the audit/reconstruction consumer. Recovery must preserve V1's diagnostic evidence instead of compressing ambiguous states into generic errors.

**Tech Stack:** Python 3.12, JSONL, SQLite, dataclasses, pytest, GitNexus MCP, Rust V1 source under `/media/wl/新加卷/codex/LightFee`.

---

## Reference Docs

- Master spec: `docs/superpowers/specs/2026-05-12-v1-semantic-100-parity-design.md`
- Existing record-layer design: `docs/superpowers/specs/2026-05-11-v1-record-layer-full-parity-and-semantic-alignment-design.md`
- Rust V1 anchors:
  - `/media/wl/新加卷/codex/LightFee/src/observability_ops/journal_bridge.rs`
  - `/media/wl/新加卷/codex/LightFee/src/observability_ops/replay_bridge.rs`
  - `/media/wl/新加卷/codex/LightFee/src/runtime_state/*`
  - `/media/wl/新加卷/codex/LightFee/src/engine/recovery.rs`

## File Ownership

- Modify: `lightfee/persistence/journal.py`
- Modify: `lightfee/persistence/metrics.py`
- Modify: `lightfee/persistence/snapshot_store.py`
- Modify: `lightfee/persistence/sqlite_store.py`
- Modify: `lightfee/engine/state.py`
- Modify: `lightfee/engine/recovery.py`
- Modify: `lightfee/engine/reconciliation.py`
- Modify: `lightfee/offline/replay/dataset.py`
- Modify: `lightfee/offline/replay/engine.py`
- Modify: `lightfee/offline/replay/counterfactual.py`
- Modify: `lightfee/offline/replay/walk_forward.py`
- Modify: `tests/test_persistence.py`
- Modify: `tests/test_persistence_replay.py`
- Modify: `tests/test_recovery_reconciliation.py`
- Modify: `tests/test_engine_recovery.py`

Coordinate before changing journal event names used by execution or offline analysis.

## Task 1: Lock Journal Envelope And Runtime Metrics

**Files:**
- Modify: `lightfee/persistence/journal.py`
- Modify: `lightfee/persistence/metrics.py`
- Modify: `tests/test_persistence.py`

- [ ] **Step 1: Run impact analysis**

Before editing:

```text
gitnexus_impact({target: "Journal", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "PersistenceMetrics", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Write failing tests**

Add tests proving:

- `run_id` matches V1 shape
- `seq` increments monotonically
- critical append fsyncs before return
- Unicode payloads round-trip
- runtime health counters and typed event counters match V1 fields

- [ ] **Step 3: Implement envelope and metrics parity**

Do not rename journal keys unless every producer and consumer is updated in the same workstream.

- [ ] **Step 4: Verify**

Run:

```bash
rtk pytest tests/test_persistence.py -q -W error
```

## Task 2: Align Snapshot And Recovery State

**Files:**
- Modify: `lightfee/engine/state.py`
- Modify: `lightfee/engine/recovery.py`
- Modify: `lightfee/persistence/snapshot_store.py`
- Modify: `tests/test_engine_recovery.py`
- Modify: `tests/test_recovery_reconciliation.py`

- [ ] **Step 1: Run impact analysis**

Before editing:

```text
gitnexus_impact({target: "EngineState", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "recover_from_snapshot", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] **Step 2: Write failing tests**

Add tests for:

- open positions restore into reconciling state
- pending entries/closes/passive closes count as recovery work
- local-L2 retained books restore as resume-waiting
- ambiguous state emits `recovery.blocked`
- flat live truth emits `recovery.flat`

- [ ] **Step 3: Implement recovery semantics**

Keep recovery classification, journal replay, dedup index, and live reconciliation separate.

- [ ] **Step 4: Verify**

Run:

```bash
rtk pytest tests/test_engine_recovery.py tests/test_recovery_reconciliation.py -q -W error
```

## Task 3: Align Replay, Counterfactual, And Walk-Forward

**Files:**
- Modify: `lightfee/offline/replay/dataset.py`
- Modify: `lightfee/offline/replay/engine.py`
- Modify: `lightfee/offline/replay/counterfactual.py`
- Modify: `lightfee/offline/replay/walk_forward.py`
- Modify: `tests/test_persistence_replay.py`

- [ ] **Step 1: Write failing tests**

Add tests for:

- replay returns event-level timeline
- replay normalizes positions to a fixed V1-visible schema
- pending entry and pending close counts come from journal evidence
- counterfactual applies config overrides to recorded evidence
- walk-forward uses real date arithmetic

- [ ] **Step 2: Run the replay suite**

Run:

```bash
rtk pytest tests/test_persistence_replay.py -q -W error
```

- [ ] **Step 3: Implement replay parity**

Never synthesize missing evidence. If V1 uses raw journal payloads, V2 must preserve those payloads or explicitly report that the evidence is unavailable.

- [ ] **Step 4: Verify**

Run:

```bash
rtk pytest tests/test_persistence_replay.py -q -W error
```

## Task 4: Validate Persistence Workstream Scope

**Files:**
- No code edits

- [ ] **Step 1: Run focused suite**

Run:

```bash
rtk pytest tests/test_persistence.py tests/test_persistence_replay.py tests/test_engine_recovery.py tests/test_recovery_reconciliation.py -q -W error
```

- [ ] **Step 2: Run GitNexus change detection**

Run:

```bash
gitnexus_detect_changes({scope: "unstaged", repo: "LightFeeV2"})
```

Expected: changed symbols should stay inside persistence/recovery/replay modules and tests, except for explicitly coordinated shared state models.

