# Local-L2 Data Plane Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` for parallel execution, or `superpowers:executing-plans` if working serially. Follow this plan task-by-task and keep checkboxes updated.

**Goal:** Build the missing Rust V1-equivalent local-L2 live data plane in Python V2 so RT-001/RT-002/P2-L2 can be closed honestly.

**Architecture:** Keep Python V2 module boundaries. `runtime.py` orchestrates lanes; `marketdata/` owns local-L2 data structures and runtime; `venues/` owns exchange feed quirks; `engine/entry_local_l2.py` owns candidate/session readiness; `entry_sync.py` owns pending-entry hedge progression.

**Tech Stack:** Python 3.12, asyncio, dataclasses, pytest, pytest-asyncio, GitNexus MCP, existing LightFeeV2 config/runtime/journal/persistence modules, Rust V1 source under `/media/wl/新加卷/codex/LightFee`.

---

## Required Reading

- Spec: `docs/superpowers/specs/2026-05-10-local-l2-data-plane-closure-design.md`
- Parity matrix: `docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md`
- Closure report: `docs/superpowers/reports/2026-05-10-v1-v2-module-parity-closure-report.md`
- Rust maker-event lane: `/media/wl/新加卷/codex/LightFee/src/execution_core/engine.rs:4587-4693`
- Rust pending hedge driver: `/media/wl/新加卷/codex/LightFee/src/execution_core/entry_sync.rs:5459+`
- Rust local-L2 runtime: `/media/wl/新加卷/codex/LightFee/src/execution_core/local_l2_runtime.rs`
- Rust entry local-L2 sessions: `/media/wl/新加卷/codex/LightFee/src/execution_core/entry_local_l2.rs`, `/media/wl/新加卷/codex/LightFee/src/execution_core/entry_local_l2_sessions.rs`
- Current Python L2 model: `lightfee/marketdata/l2.py`, `lightfee/marketdata/local_book.py`
- Current Python maker-event partial: `lightfee/engine/runtime.py:_maybe_tick_maker_event`

## Mandatory Rules

- Before editing any production function/class/method, run GitNexus impact analysis for that symbol and record risk in the work note.
- Do not use sidecar snapshot as the V1 parity local-L2 execution source.
- Do not change strategy thresholds, risk thresholds, retry windows, precision, or venue policy without Rust evidence.
- Do not create fake L2 readiness, fake order IDs, fake prices, or fake fills in production paths.
- Do not move venue-specific sequence/checksum/depth rules into `runtime.py`.
- Do not mark parity fixed until focused tests and a production caller test pass.
- If a Rust branch is replaced by a cleaner Python helper, add tests for each live behavior that the Rust branch protected.

## Task 0: Baseline And Drift Lock

**Files:**

- Read: `docs/superpowers/specs/2026-05-10-local-l2-data-plane-closure-design.md`
- Read/modify as needed: `docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md`
- Create as tests fail first: `tests/test_runtime_maker_event_local_l2.py`, `tests/test_local_l2_runtime.py`, `tests/test_entry_local_l2.py`

- [ ] Run repository status:

```bash
rtk git status --short
```

- [ ] Run current verification to separate inherited failures from new work:

```bash
rtk pytest -q -W error
rtk python3 -m compileall lightfee tests
```

- [ ] Add parity rows if missing:

```text
P2-L2-001 local-L2 book core
P2-L2-002 venue L2 normalization
P2-L2-003 local-L2 runtime assignment/events
P2-L2-004 entry local-L2 sessions/readiness
P2-L2-005 execution liquidity source from local-L2
P2-L2-006 maker-event lane consumes local-L2 events
P2-L2-007 phased live startup local-L2 activation
P2-L2-008 local-L2 persistence/recovery/metrics
```

- [ ] Add failing drift-lock tests that prove current sidecar-mid lane is not parity:
  - A local-L2 event matching a pending entry must wake maker-event lane without reading sidecar snapshot.
  - A sidecar mid-price move alone must not be considered V1 parity when local-L2 parity mode is enabled.
  - Reprice/cancel-replace must target the existing pending entry hedge state, not create `entry_id="reprice-..."`.

## Task 1: Upgrade Local L2 Book Core

**Files:**

- Modify: `lightfee/marketdata/l2.py`
- Modify: `lightfee/marketdata/local_book.py`
- Modify/add tests: `tests/test_marketdata_l2.py`

**Impact first:**

```text
gitnexus_impact({target: "LocalL2Book", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] Extend `LocalL2Book` with Rust-required fields:
  - `last_update_id`
  - `sequence`
  - `checksum`
  - `last_snapshot_ms`
  - `last_delta_ms`
  - `resume_waiting_until_ms`
  - `runtime_suspended_until_ms`
  - `source`
  - `fault_reason`

- [ ] Add pure dataclasses/enums:
  - `LocalL2BookKey(venue, symbol)`
  - `LocalL2Update(venue, symbol, bids, asks, sequence, previous_sequence, checksum, event_time_ms, received_at_ms, update_kind)`
  - `LocalL2Event(venue, symbol, event_kind, wake_reason, observed_at_ms, sequence, detail)`
  - `LocalL2UpdateResult(applied, events, fault_reason, rebuild_required)`

- [ ] Implement pure book operations:
  - snapshot replace
  - delta merge
  - zero quantity delete
  - bid descending / ask ascending sorting
  - max depth trimming
  - sequence gap detection
  - optional checksum verification hook
  - age and readiness checks

- [ ] Preserve existing tests and add focused cases:
  - snapshot creates sorted book
  - delta updates price level
  - delta deletes zero quantity
  - stale book is not ready
  - sequence gap returns `rebuild_required=True`
  - checksum mismatch returns a checksum event and degrades book
  - repeated degradation suspends according to existing threshold semantics

## Task 2: Add Venue L2 Normalization Layer

**Files:**

- Create: `lightfee/marketdata/local_l2_venues.py`
- Modify if needed: `lightfee/venues/base.py`, `lightfee/venues/transport.py`, venue adapter files
- Create: `tests/test_local_l2_venue_rules.py`

**Impact first when editing adapter methods:**

```text
gitnexus_impact({target: "VenueAdapter", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "VenueTransport", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] Define per-venue rule object:
  - venue name
  - default local-L2 depth
  - sequence mode
  - checksum mode
  - symbol normalization
  - snapshot bootstrap requirement
  - reconnect/rebuild trigger policy

- [ ] Normalize venue payloads into `LocalL2Update`; keep raw payload parsing out of `runtime.py`.

- [ ] Add fixture tests for at least Binance, OKX, Bybit, Bitget, Gate, Aster, Hyperliquid:
  - normal delta parses into expected levels
  - venue sequence field maps correctly
  - missing/invalid sequence produces deterministic fault classification
  - venue unsupported depth/checksum behavior is explicit, not silently ignored

## Task 3: Build Local-L2 Runtime Service

**Files:**

- Create: `lightfee/marketdata/local_l2_runtime.py`
- Modify: `lightfee/config/schema.py`
- Modify: `lightfee/config/validation.py`
- Create: `tests/test_local_l2_runtime.py`

**Impact first:**

```text
gitnexus_impact({target: "RuntimeConfig", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "StrategyConfig", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] Implement `LocalL2Runtime` with:
  - `books`
  - `assignments`
  - `assignment_leases`
  - `pending_events`
  - `metrics`
  - `sync(now_ms, include_scan_promoted)`
  - `record_update(update, now_ms)`
  - `drain_events(limit)`
  - `apply_fallback(venue, symbol, reason)`
  - `handle_runtime_failure(venue, symbol, error, now_ms)`

- [ ] Implement assignment semantics:
  - hot execution pool
  - warm pool
  - retained pool
  - dropped pool
  - lease preserve
  - lease expiry
  - assignment empty event

- [ ] Implement runtime fault semantics:
  - rate-limited
  - transport failure
  - checksum mismatch
  - sequence gap
  - quote age triggered
  - resume expired
  - runtime suspended
  - budget suspended

- [ ] Add metrics counters matching Rust naming where current metrics exporter can consume them:
  - `local_l2_rebuild_total`
  - `local_l2_resume_expired_total`
  - `local_l2_fallback_total`
  - `local_l2_budget_suspended_total`
  - `local_l2_runtime_suspended_total`
  - `local_l2_runtime_rate_limited_total`
  - `local_l2_runtime_transport_failure_total`
  - `local_l2_assignment_empty_total`
  - `local_l2_assignment_lease_preserved_total`
  - `local_l2_assignment_lease_expired_total`
  - `maker_event_lane_wake_total`

- [ ] Tests must prove event queue bounded behavior and event age fields.

## Task 4: Implement Entry Local-L2 Sessions

**Files:**

- Create: `lightfee/engine/entry_local_l2.py`
- Modify if needed: `lightfee/engine/state.py`
- Create: `tests/test_entry_local_l2.py`

**Impact first:**

```text
gitnexus_impact({target: "EngineState", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] Implement tracked candidate/session dataclasses:
  - opportunity id
  - long venue/symbol leg
  - short venue/symbol leg
  - primary/shadow role
  - leg state: arming, ready, stale, degraded, suspended, unavailable
  - session state
  - not-ready reasons
  - last readiness update

- [ ] Implement refresh behavior:
  - primary count from config
  - shadow candidates
  - prewarm window
  - quiet book grace without relaxing invalid books
  - readiness downgrade
  - shadow promotion after hold expiry
  - primary demotion
  - lease preserve/expire

- [ ] Add diagnostics snapshot and journal payload shape compatible with existing reporting style.

- [ ] Tests must cover Rust edge cases:
  - rebuilding leg stays arming
  - suspended leg stays arming/not-ready
  - resume waiting does not count as ready
  - dual-leg freshness threshold
  - not-ready pair reasons are visible

## Task 5: Connect Execution Liquidity To Local L2

**Files:**

- Modify: `lightfee/marketdata/liquidity.py`
- Modify relevant entry planner/execution files after impact analysis
- Add/modify tests: `tests/test_runtime_entry_flow.py`, `tests/test_entry_planner.py`, `tests/test_marketdata_l2.py`

**Impact first for any edited production symbol:**

```text
gitnexus_impact({target: "ExecutionLiquiditySnapshot", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] Add conversion from `LocalL2Book` to `ExecutionLiquiditySnapshot`.
- [ ] Ensure buy uses asks and sell uses bids with correct sorting and quantity.
- [ ] Enforce max snapshot age and readiness.
- [ ] Return explicit `ExecutionLiquiditySource.TRUE_L2`, `TOP_BOOK`, `CACHED`, or `NONE`.
- [ ] In parity mode, block entry when Rust V1 would block due to local-L2 unavailable; do not silently top-book fallback.
- [ ] Tests must verify fallback reason strings and journal evidence.

## Task 6: Rewrite Maker-Event Lane To Consume Local-L2 Events

**Files:**

- Modify: `lightfee/engine/runtime.py`
- Modify: `lightfee/engine/entry_sync.py`
- Add: `tests/test_runtime_maker_event_local_l2.py`
- Update existing sidecar-mid tests in `tests/test_live_full_closure.py` so they are labeled non-parity fallback if retained.

**Impact first:**

```text
gitnexus_impact({target: "_maybe_tick_maker_event", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "EntrySyncExecutor", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] Replace parity-mode trigger source:
  - call local-L2 runtime `sync(now_ms, include_scan_promoted=False)`
  - finalize entry-local-L2 readiness
  - return early if no pending entry hedges
  - drain bounded local-L2 events
  - filter events matching pending entries
  - enforce `maker_event_lane_min_wake_interval_ms`

- [ ] Implement or expose pending-entry hedge driver in Python:
  - accepts existing pending entry id
  - reads current market/local-L2 view
  - amends or cancel-replaces maker order according to existing passive maker config
  - preserves pending entry state
  - records uncertain/retryable/terminal outcomes explicitly

- [ ] Remove parity-mode behavior that creates a fresh `EntryContext(entry_id="reprice-...")`.

- [ ] Journal `execution.maker_event_lane_wake` with:
  - `event_count`
  - `position_count`
  - `symbols`
  - `event_kinds`
  - `wake_reasons`
  - `min_event_age_ms`
  - `max_event_age_ms`
  - `venues`

- [ ] Tests must prove:
  - no events -> no wake
  - unrelated local-L2 event -> no wake
  - matching event -> pending hedge driver called once
  - min wake interval suppresses repeated wake
  - event age min/max recorded
  - sidecar file missing does not affect local-L2 parity lane

## Task 7: Add Live Startup Local-L2 Phase

**Files:**

- Modify: `lightfee/apps/live.py`
- Modify: `lightfee/engine/runtime.py`
- Modify: `lightfee/engine/lifecycle.py` if needed
- Add/modify: `tests/test_live_startup_preflight.py`

**Impact first:**

```text
gitnexus_impact({target: "LiveStartupPhase", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "LiveRuntime", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] Implement startup phases:
  - private stream activation
  - market stream activation
  - local-L2 activation

- [ ] Local-L2 phase must:
  - restore retained books
  - subscribe/bootstrap configured hot/warm/retained symbols
  - respect `live_startup_phase_timeout_ms`
  - emit startup local-L2 mode note
  - decide fail-closed/degraded behavior based on config and Rust semantics

- [ ] Tests must verify order of phases and timeout journal payload.

## Task 8: Persist And Recover Local-L2 State

**Files:**

- Modify: `lightfee/engine/state.py`
- Modify: `lightfee/engine/recovery.py`
- Modify: persistence modules if local-L2 snapshot belongs there
- Add tests in recovery/persistence test files

**Impact first:**

```text
gitnexus_impact({target: "EngineState", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "recover_state", direction: "upstream", repo: "LightFeeV2"})
```

- [ ] Snapshot enough local-L2 state to preserve Rust V1 live behavior:
  - retained books metadata
  - assignment leases
  - resume waiting expiry
  - session readiness state
  - metrics counters required by operator reports

- [ ] Recovery rules:
  - restored book is never automatically `HOT` unless freshness/sequence proves it
  - resume-waiting book blocks readiness until resumed
  - unknown or invalid state degrades explicitly
  - pending events are either replayable or dropped with journal reason; do not replay stale events blindly

- [ ] Tests must roundtrip snapshot and prove no false-ready after recovery.

## Task 9: Metrics, Docs, And Final Closure

**Files:**

- Modify metrics exporter modules already used by runtime
- Update: `docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md`
- Update: `docs/superpowers/reports/2026-05-10-v1-v2-module-parity-closure-report.md`

- [ ] Export Rust-equivalent local-L2 and entry-local-L2 metrics where the current metrics stack supports them.
- [ ] Update parity matrix rows from `open`/`partial` to `fixed` only when tests prove production caller wiring.
- [ ] Update closure report honestly:
  - If sidecar-mid fallback remains, label it fallback/non-parity.
  - RT-001 can be fixed only after maker-event lane consumes local-L2 events and pending hedge driver.
  - RT-002 can be fixed only after phased local-L2 startup activation is wired.

- [ ] Run full verification:

```bash
rtk pytest -q
rtk pytest -q -W error
rtk python3 -m compileall lightfee tests
```

- [ ] Run GitNexus change detection before final report:

```text
gitnexus_detect_changes({scope: "all", repo: "LightFeeV2"})
```

## Parallelization Guidance

Safe parallel lanes:

- Worker A: Task 1 book core and tests.
- Worker B: Task 2 venue normalization fixtures.
- Worker C: Task 3 local-L2 runtime service.
- Worker D: Task 4 entry local-L2 sessions.

Integration order:

1. Merge Task 1 before Task 3.
2. Merge Task 3 before Task 6.
3. Merge Task 4 and Task 5 before Task 6 final parity tests.
4. Merge Task 7 and Task 8 after Task 3 service API is stable.

Avoid parallel edits to `lightfee/engine/runtime.py` and `lightfee/engine/entry_sync.py`; those should be owned by the maker-event integration worker after the data-plane services are ready.

## Completion Statement Template

Use this exact shape in the final report:

```text
结论：local-L2 实盘数据平面已闭合 / 未闭合

已闭合：
- P2-L2-001 ...

仍未闭合：
- ...

Rust 对齐证据：
- ...

验证：
- rtk pytest -q
- rtk pytest -q -W error
- rtk python3 -m compileall lightfee tests

风险：
- ...
```
