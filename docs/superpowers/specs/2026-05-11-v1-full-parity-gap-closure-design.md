# V1 Full Parity Gap Closure Design

**Goal:** Identify and close every remaining Rust V1 live-path parity gap in LightFeeV2, with special focus on worker ownership, startup/recovery boundaries, local-L2 lifecycle, canonical symbol authority, venue capability truth, and verification discipline.

**Primary source of truth:** `/media/wl/新加卷/codex/LightFee`

**Target repository:** `/media/wl/新加卷/codex/LightFeeV2`

**Date:** 2026-05-11

---

## Decision

LightFeeV2 should not continue as a stream of isolated drift patches. The remaining work must be treated as a full live-path parity closure effort.

Rust V1 is not just a collection of feature checks. It has a control plane, a worker ownership model, a startup/recovery lifecycle, a local-L2 state machine, and venue-specific capability truth. Python already reproduces many behaviors, but the ownership boundaries are still too loose:

- startup owns both preflight and long-lived worker activation
- the local-L2 data plane owns both orchestration and worker lifecycle too indirectly
- canonical symbol authority is split across transport, parser, factory, runtime, and tests
- docs can still claim `fixed` ahead of fresh acceptance evidence

The design rule for this phase is:

```text
Rust live behavior: 1:1 semantic parity
Python implementation: clearer ownership, smaller services, fewer historical patches
```

If a Rust behavior exists only because of production patches, it still counts. If Python can express it more cleanly, that is allowed, but only if the live-path semantics remain unchanged and are covered by focused tests.

## Scope

In scope:

- worker/session ownership for local-L2 and other long-lived live workers
- startup and shutdown boundaries
- recovery and reconciliation boundaries
- local-L2 runtime/data-plane separation
- canonical symbol and venue wire symbol conversion
- venue capability truth, including risk-health support
- maker-event wake semantics and local-L2 readiness gating
- closure documentation and verification discipline

Out of scope:

- changing strategy thresholds
- changing business formulas or venue precision rules without Rust evidence
- replacing live semantics with paper/shadow shortcuts
- mechanical Rust file-layout reproduction
- non-live research/evolution/report code

## Current Gaps

### 1. Worker Ownership Is Not Explicit Enough

Rust V1 venues own their local-L2 workers. They can start, abort, clear, restart, and report worker categories. Python currently has a `LocalL2DataPlane` and `LocalL2WsClient`, but worker ownership is still effectively centralized through `LiveRuntime.start()`.

Why this matters:

- startup preflight can accidentally leave live background tasks running
- tests can hang if runtime start is treated as a one-shot call
- there is no dedicated session generation or worker topology state

Required direction:

- introduce or formalize an explicit local-L2 worker/session manager boundary
- treat startup as registration/activation, not as anonymous task spawning
- make stop/abort/restart ownership visible in diagnostics

### 2. Startup / Recovery Boundaries Are Still Blurred

Rust separates symbol resolution, startup recovery, local-L2 activation, and runtime loop phases. Python has these pieces, but `LiveRuntime.start()` still mixes preflight, recovery, and worker activation tightly enough that it can hang or leak background tasks.

Why this matters:

- preflight should return even when live WS cannot connect
- shutdown must fully cancel worker tasks
- restart and recovery should not depend on test cleanup habits

Required direction:

- keep startup phase work bounded
- make worker activation explicitly stoppable and cancelable
- keep recovery and reconciliation separated from worker lifetime management

### 3. Canonical Symbol Authority Is Still Split

Rust local-L2 flows use venue wire symbols on the wire but canonical symbols internally. Python now has some canonical conversion, but it still spreads authority across `VenueSpec`, `VenueTransport`, `LocalL2WsClient`, parser paths, and some tests.

Why this matters:

- split books can appear if wire symbols leak into runtime keys
- factory/parser tests can pass while runtime behavior is still ambiguous
- venue-specific symbol conversion logic can drift if each layer implements its own rule

Required direction:

- one internal canonical symbol policy
- venue wire symbols only at request/subscribe/parse boundaries
- runtime books, pending entries, and maker-event matching must use canonical keys

### 4. Local-L2 Runtime Is Still Missing a Stronger State-Owner Boundary

`LocalL2Runtime` already owns books, assignments, leases, events, and metrics, but it is still not the whole Rust state machine. Rust V1 also binds in supervision, fallback action, retention/rebuild state, venue topology, and worker categories.

Why this matters:

- a book store is not yet a full runtime state machine
- the same status may need to mean different things at book, venue, and worker levels
- recovery/reconcile can be correct per book but still wrong per venue topology

Required direction:

- keep `LocalL2Runtime` as the canonical book/state owner
- move worker/session orchestration outside it
- make supervision/recovery consume runtime state rather than mix with worker control

### 5. Venue Capability Truth Is Not Yet Treated as a First-Class Contract

Rust V1 has explicit capability truth per venue. Python has capability fields and some support matrices, but the remaining implementation still needs to prove unsupported behavior, partial support, and special-case risk-health handling per venue.

Why this matters:

- unsupported must be explicit, not just absent
- live behavior differs across Binance / OKX / Bybit / Bitget / Gate / Aster / Hyperliquid
- risk-health and reduce-only behavior need exact support truth, not generic fallback

Required direction:

- keep capability truth in venue contract/transport layers
- let runtime consume that truth rather than infer it
- ensure unsupported paths are journaled and tested

### 6. Closure Evidence Still Lags the Code

The existing closure docs can get ahead of fresh acceptance results. That is a process gap, not just a documentation gap.

Why this matters:

- a green subset of tests is not the same as full closure
- `fixed` means fresh verification, not historical confidence
- docs must not lead code

Required direction:

- parity matrix rows must stay honest
- closure report wording must match the latest verification commands
- no module can be marked `fixed` without fresh evidence

## Proposed Architecture

The next phase should be organized into four clear layers:

### Control Plane

Owns startup, shutdown, recovery classification, and worker activation timing.

Candidate owners:

- `lightfee/apps/live.py`
- `lightfee/engine/runtime.py`

### Worker Plane

Owns local-L2 worker/session lifecycle, start/stop/restart, and worker diagnostics.

Candidate owners:

- a dedicated `lightfee/marketdata/local_l2_worker.py` or equivalent session manager
- `lightfee/marketdata/local_l2_data_plane.py` as the data-plane facade

### Data Plane

Owns canonical update ingestion, book state, assignment, lease, event queue, and local-L2 runtime metrics.

Candidate owners:

- `lightfee/marketdata/local_l2_runtime.py`
- `lightfee/marketdata/local_l2_data_plane.py`
- `lightfee/marketdata/local_l2_ws.py`

### Contract Plane

Owns venue payloads, symbol conversion, precision, risk-health truth, and capability support.

Candidate owners:

- `lightfee/core/contracts.py`
- `lightfee/venues/specs.py`
- `lightfee/venues/transport.py`
- `lightfee/venues/*.py`

## Success Criteria

This design is only complete when all of the following are true:

- startup preflight does not hang because of live WS lifecycle
- worker/session ownership is explicit and cancelable
- canonical symbol use is consistent from transport through runtime
- local-L2 runtime and worker ownership are separated cleanly
- venue capability truth matches Rust V1 live-path behavior
- maker-event lane consumes real local-L2 state only
- parity matrix and closure report stay consistent with fresh verification

## Non-Goals

- rewriting Rust file layout in Python
- reintroducing sidecar or shadow modes as closure shortcuts
- changing live trading logic to make tests easier
- collapsing venue-specific behavior into one generic abstraction when Rust proves the behaviors are distinct

