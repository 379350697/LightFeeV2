# V2 Journal Fact Store and Projection Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. This plan owns the V2 journal-to-fact projection layer, structured query storage, and consumer migration.

**Goal:** Add a Qlib-style derived data layer to LightFeeV2 so historical and analytical reads can use structured storage while the journal remains the source of truth.

**Architecture:** The runtime keeps appending to the JSONL journal. A new projection layer classifies selected records into facts, writes them into SQLite-backed tables, and exposes backfill/rebuild paths. Offline analysis and reporting should switch to the structured layer when possible, with journal fallback only for gaps or rebuilds.

**Tech Stack:** Python 3.12, JSONL, SQLite, dataclasses, pytest, GitNexus MCP.

---

## Reference Docs

- Master spec: `docs/superpowers/specs/2026-05-12-v2-journal-fact-store-projection-design.md`
- Current journal implementation: `lightfee/persistence/journal.py`
- Current structured store: `lightfee/persistence/sqlite_store.py`
- Current snapshot path: `lightfee/persistence/snapshot_store.py`
- Current offline consumers: `lightfee/offline/analysis/journal.py`, `lightfee/offline/reports/daily.py`, `lightfee/offline/replay/dataset.py`

## File Ownership

- Modify: `lightfee/persistence/journal.py`
- Modify: `lightfee/persistence/sqlite_store.py`
- Modify: `lightfee/persistence/metrics.py`
- Modify: `lightfee/offline/analysis/journal.py`
- Modify: `lightfee/offline/reports/daily.py`
- Modify: `lightfee/offline/replay/dataset.py`
- Create: `lightfee/persistence/projection_contracts.py`
- Create: `lightfee/persistence/projection_writer.py`
- Create: `lightfee/persistence/journal_index.py`
- Create: `lightfee/persistence/projection_backfill.py`
- Modify: `tests/test_persistence.py`
- Modify: `tests/test_persistence_replay.py`
- Modify: `tests/test_offline_analysis.py`
- Modify: `tests/test_runtime_smoke.py`

Keep recovery and replay semantics untouched unless a test proves the projection layer is not leaking into them.

## Task 1: Define Projection Boundaries And Facts

**Files:**
- Create: `lightfee/persistence/projection_contracts.py`
- Modify: `lightfee/persistence/sqlite_store.py`
- Modify: `tests/test_persistence.py`

- [ ] **Step 1: Lock the event classification**

Define which journal kinds are projected into tables and which kinds stay journal-only.

Minimum projected groups:

- order facts
- entry/exit facts
- scan facts
- risk counters
- local-L2 health facts
- daily summary facts

Minimum journal-only groups:

- recovery evidence
- lifecycle transitions
- state reconciliation evidence

- [ ] **Step 2: Add failing tests for classification**

Add tests proving that projected kinds and journal-only kinds are separated intentionally, not by accident.

- [ ] **Step 3: Add projection-friendly schemas**

Extend the SQLite schema with tables that can store facts by `seq`, `ts_ms`, `kind`, `venue`, `symbol`, and a JSON payload or normalized scalar columns where appropriate.

- [ ] **Step 4: Verify**

Run:

```bash
rtk pytest tests/test_persistence.py -q -W error
```

## Task 2: Add Journal Streaming And Backfill Support

**Files:**
- Modify: `lightfee/persistence/journal.py`
- Create: `lightfee/persistence/journal_index.py`
- Create: `lightfee/persistence/projection_backfill.py`
- Modify: `tests/test_persistence.py`

- [ ] **Step 1: Write failing tests**

Add tests proving:

- records can be streamed without materializing the whole file
- filtered reads can avoid full post-hoc Python scans where possible
- backfill can resume from a seq cursor
- duplicate projection writes remain idempotent

- [ ] **Step 2: Implement streaming read primitives**

Add iterator-based accessors and a lightweight index/cursor model so projection jobs do not need `read_all()` for every rebuild.

- [ ] **Step 3: Implement backfill entry points**

Create a rebuild path that reads the journal in order and writes idempotent projection rows.

- [ ] **Step 4: Verify**

Run:

```bash
rtk pytest tests/test_persistence.py -q -W error
```

## Task 3: Materialize Analytic Facts

**Files:**
- Create: `lightfee/persistence/projection_writer.py`
- Modify: `lightfee/persistence/sqlite_store.py`
- Modify: `lightfee/persistence/metrics.py`
- Modify: `lightfee/offline/reports/daily.py`
- Modify: `lightfee/offline/analysis/journal.py`
- Modify: `tests/test_offline_analysis.py`

- [ ] **Step 1: Write failing tests**

Add tests proving:

- daily summary rows are written from projected facts
- venue and symbol summaries can be rebuilt from the structured store
- projection metrics track lag, failures, and backfill state

- [ ] **Step 2: Implement the projection writer**

Write an idempotent consumer that maps journal records into structured rows and keeps a small amount of cursor state.

- [ ] **Step 3: Move analytical consumers to the structured layer**

Prefer the store for reporting and offline analysis, falling back to journal scans only when the store is missing or stale.

- [ ] **Step 4: Verify**

Run:

```bash
rtk pytest tests/test_offline_analysis.py -q -W error
```

## Task 4: Switch Replay Consumers To Structured Inputs Where Safe

**Files:**
- Modify: `lightfee/offline/replay/dataset.py`
- Modify: `tests/test_persistence_replay.py`
- Modify: `tests/test_runtime_smoke.py`

- [ ] **Step 1: Write failing tests**

Add tests proving replay dataset construction can read range-filtered structured data when available, while preserving journal fallback for exact evidence.

- [ ] **Step 2: Implement safe consumer migration**

Keep replay semantics unchanged, but stop forcing every read through a full journal scan when a structured source already exists.

- [ ] **Step 3: Verify**

Run:

```bash
rtk pytest tests/test_persistence_replay.py tests/test_runtime_smoke.py -q -W error
```

## Task 5: Validate Scope And Rollout Safety

**Files:**
- No code edits

- [ ] **Step 1: Run the focused suite**

Run:

```bash
rtk pytest tests/test_persistence.py tests/test_persistence_replay.py tests/test_offline_analysis.py tests/test_runtime_smoke.py -q -W error
```

- [ ] **Step 2: Run GitNexus change detection**

Run:

```text
gitnexus_detect_changes({scope: "unstaged", repo: "LightFeeV2"})
```

Expected: changes should stay within persistence, offline analysis, replay, reporting, and their tests.

