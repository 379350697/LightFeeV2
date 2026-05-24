# V2 Journal Fact Store and Projection Layer Design

**Goal:** Keep the journal as LightFeeV2's canonical event source while projecting selected high-value records into queryable tables for faster reads, richer analysis, and Qlib-style data reuse.

**Architecture:** The append-only journal remains the system of record for recovery, replay, and audit. A derived projection layer consumes journal records and writes normalized facts, snapshots, and aggregates into structured storage such as SQLite, with optional columnar storage for high-volume history later. Consumers that need scan-friendly reads should query the projection layer; consumers that need exact restart semantics should continue to read the journal.

**Target:** `/media/wl/新加卷/codex/LightFeeV2`

**Date:** 2026-05-12

---

## Problem

LightFeeV2 currently stores most operational evidence in `runtime/events.jsonl` and reads it back with whole-file scans. That is correct for durability, but it is not ideal for:

- repeated analysis of the same historical records
- range queries by date, venue, symbol, or event kind
- daily reporting and metrics aggregation
- cross-run comparisons and backfills

The codebase already hints at a split:

- `lightfee/persistence/journal.py` keeps the canonical event stream
- `lightfee/persistence/snapshot_store.py` keeps restart state
- `lightfee/persistence/sqlite_store.py` already stores structured facts and ledgers
- `lightfee/offline/reports/daily.py` materializes journal-derived summaries into SQLite

This design makes that split explicit and durable.

## Core Principle

Journal first, projection second.

The journal is the truth source. Projections are rebuildable views of that truth. A projection may lag, fail, or be rebuilt without changing the meaning of the engine state. Recovery must never depend on the projection store.

## Storage Tiers

### Tier 1: Canonical journal

Keep all semantically important runtime evidence in JSONL journal form, including:

- startup and shutdown lifecycle
- recovery evidence
- risk transitions and fail-closed transitions
- order lifecycle and execution evidence
- local-L2 health and synchronization evidence

This tier must remain append-only and replayable.

### Tier 2: Queryable facts

Project selected journal records into normalized tables that are optimized for queries and reporting. Good candidates are:

- `entry.opened`
- `exit.closed`
- `order.submitted`
- `order.filled`
- `order.rejected`
- `order.uncertain`
- `scan.completed`
- `scan.no_entry_diagnostics`
- `scan.runtime_gate_blocked`
- `runtime.local_l2_sequence_gap`
- `runtime.local_l2_sync_failed`
- aggregated `risk.*` counters
- daily venue and symbol summaries

These rows are cheap to query, easy to aggregate, and do not need full replay semantics.

### Tier 3: Derived snapshots and caches

Keep daily summaries, recovery snapshots, and reporting caches as derived outputs. They are disposable and rebuildable from Tier 1 + Tier 2.

## Data Classification

Not every log line should leave the journal.

### Stay in the journal

These records are primarily about order, causality, or exact replay:

- `recovery.*`
- `runtime.lifecycle_changed`
- `runtime.risk_mode_changed`
- `runtime.booting`
- `runtime.running`
- `runtime.stopped`
- any event whose meaning depends on sequence ordering

### Project into facts

These records are primarily about queryable business evidence:

- order and fill events
- entry and exit events
- risk counters and incident counts
- scan outcomes
- venue health and local-L2 health events
- daily aggregate outputs

### Persist as structured state

These are not event history and should stay in the state/snapshot layer:

- current runtime snapshot
- retained books snapshot
- open positions snapshot
- pending entries and pending closes

## Read Paths

### Operational reads

Runtime code continues to read the journal and snapshots when it needs exact recovery or live state reconstruction.

### Analytical reads

Offline reports, dashboards, and historical summaries should read from the projection layer first. If a projection is missing, they may fall back to the journal and then backfill the missing projection.

### Backfill reads

Any projection must be rebuildable from the journal with deterministic output for a given journal range and projection version.

## Write Paths

1. The runtime appends to the journal.
2. A projection worker or backfill job consumes new journal records in seq order.
3. The worker writes idempotent rows into SQLite or the selected structured store.
4. Reporting code reads the structured store.

The projection writer must tolerate restarts and duplicate reads. `seq` should be treated as the deduplication anchor.

## Qlib-Style Borrowing

The useful part of Qlib here is not its backtesting story. It is the separation of:

- raw source data
- normalized/queryable data
- derived caches

LightFeeV2 should adopt that layering for history-heavy reads, while keeping its own semantics for trading recovery.

## Non-Goals

- replacing the journal with a database
- making recovery depend on the projection layer
- forcing all event kinds into relational tables
- changing live trading behavior to satisfy storage convenience
- copying Qlib's domain model literally

## Acceptance Criteria

- the journal remains the canonical source of truth
- query-heavy consumers can read projected tables instead of scanning JSONL
- projected data can be rebuilt from journal evidence
- daily summaries and offline analysis become range-aware and table-backed
- recovery and replay behavior do not change
- the separation between raw evidence and derived facts is explicit in code and docs

