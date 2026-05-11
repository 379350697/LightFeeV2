# V1 Passive Close Gap Closure Design

**Goal:** Restore Rust V1 passive close semantics in LightFeeV2 so normal close reasons can execute as maker+taker flows with chunking, maker repricing, and recovery-aware pending state, while leaving aggressive close behavior unchanged.

**Primary source of truth:** `/media/wl/新加卷/codex/LightFee`

**Target repository:** `/media/wl/新加卷/codex/LightFeeV2`

**Date:** 2026-05-11

---

## Decision

Passive close is a distinct live-path state machine, not a flag inside aggressive close execution.

LightFeeV2 already has the close-reason predicate that says which normal exits should use maker+taker semantics, but it does not yet have the execution path that follows that predicate. The missing behavior must be added as a dedicated passive close service that owns:

- chunked passive close lifecycle
- maker resting order management
- repricing and cancel/replace decisions
- delta hedge submission after maker fills
- pending-state persistence and recovery
- venue op-budget gating for passive maintenance

This feature is live-only. No fake, paper, or shadow execution path should be introduced to satisfy it.

If passive close cannot be armed or safely maintained for a chunk, the remaining quantity must fall back to the existing aggressive taker close path or the existing fail-closed/protection logic, depending on the reason and state. The system must never silently skip a close just because passive maintenance is unavailable.

## Scope

In scope:

- route normal close reasons into passive close when V1 says they should use maker+taker
- add a `PendingPassiveClose` state model and persistence/recovery support
- add a dedicated passive close executor/service
- add venue contract support for resting passive orders, progress checks, amend, and cancel
- add canonical tick-size / price-step metadata for passive repricing
- wire passive close into live runtime and reconciliation flows
- preserve chunking, small-fill buffering, budget gating, and deadline behavior from V1
- add journal events and tests for the passive close lifecycle

Out of scope:

- entry-side behavior
- aggressive close economics or chunking rules, except for shared helpers
- any fake/paper/shadow-mode implementation
- changing thresholds, fees, budgets, or timeouts without V1 evidence
- redesigning the existing runtime loop beyond what passive close needs

## Current Gaps

1. `lightfee/engine/close_executor.py` only implements aggressive reduce-only IOC close flow.
2. `lightfee/engine/state.py` has `PendingClose`, but no passive-close-specific pending state.
3. `lightfee/engine/exit_decision.py` can classify passive close reasons, but nothing consumes that route.
4. `lightfee/core/contracts.py` has no contract for a resting passive order's cumulative fill/progress.
5. `lightfee/marketdata/l2.py` exposes `mid_price()`, but the execution stack has no canonical price tick / tick-size source for passive repricing.
6. `lightfee/engine/runtime.py` only drives aggressive close and maker-entry lanes; it does not own a passive close loop.
7. Recovery can restore aggressive pending closes, but it cannot resume passive close maintenance.

## Proposed Architecture

### Routing And Ownership

- `lightfee/engine/exit_decision.py` remains the pure route predicate.
- `lightfee/engine/close_executor.py` remains the aggressive taker close executor and the owner of shared close aggregation helpers.
- `lightfee/engine/passive_close.py` (new) owns passive close lifecycle, chunk progression, maintenance, and finalization.
- `lightfee/engine/state.py` owns `PendingPassiveClose` and its persistence shape.
- `lightfee/engine/runtime.py` orchestrates when passive close is started, advanced, or recovered.
- `lightfee/core/contracts.py` and `lightfee/core/domain.py` carry passive order progress and maintenance contracts.
- `lightfee/venues/specs.py`, `lightfee/venues/transport.py`, and venue adapters supply price tick / precision metadata and passive order operations.
- `lightfee/marketdata/local_l2_runtime.py` remains the canonical owner of local-L2 book state and freshness.

### Passive Close Execution Model

Each passive close position is tracked as one live `PendingPassiveClose` record keyed by `position_id`.

Each record owns:

- the original position snapshot
- the close reason
- the target close quantity
- the chunk plan
- the current chunk index
- the active maker leg
- the maker order identity and client order id
- cumulative maker and hedge fills
- the per-venue passive order-manager runtime
- the next retry deadline
- the small-fill buffer state

Per chunk, the flow is:

1. choose maker leg using V1 slippage/venue logic
2. submit a reduce-only GTC post-only maker order
3. observe cumulative maker progress
4. hedge only the delta with an IOC reduce-only taker order
5. maintain the maker order by repricing against local-L2 mid and tick size
6. advance to the next chunk when both legs are complete

The passive close service must preserve the V1 behaviors for:

- chunking large closes
- maker-leg repricing
- cancel/replace versus amend decisions
- small-fill buffering when the hedge remainder is temporarily too small
- venue-specific op budget gating
- deadline escalation when maker/hedge progress stalls

### Recovery And Persistence

`PendingPassiveClose` must be serialized into the engine snapshot and restored on startup.

Recovery must:

- detect whether the position is already flat before resuming maintenance
- resume an in-flight passive close if the position is still open
- clear or reconcile stale passive close state when the close has already completed externally

Passive pending state and aggressive pending close state must remain separate. The aggressive uncertain-close reconciliation model is not enough to represent passive close maintenance.

### Venue Contract And Market Data

Passive repricing needs a canonical price tick / tick-size source. That source must live in the venue contract or venue metadata layer, not be inferred from ad hoc spread math or quantity step size.

The passive order contract must support resting-order semantics, not just terminal fills. In practice, the adapter layer needs to expose:

- submit passive order and receive an acknowledgement with order id, client order id, and accepted state
- query cumulative progress for a resting order
- amend a resting passive order
- cancel a resting passive order

The existing order-fill reconciliation API remains relevant for fallback and uncertainty resolution, but it is not enough to run passive maintenance by itself.

### Runtime Integration

Live runtime should treat passive close as a first-class lane:

- close reasons that use maker+taker semantics start passive close instead of aggressive close
- the passive close lane runs after local-L2 sync so repricing can use fresh book state
- passive close maintenance participates in recovery and reconciliation
- maker-entry repricing remains a separate lane

The passive close lane must not depend on sidecar fallback or any paper-only path.

## Success Criteria

- Close reasons listed by `normal_close_reason_uses_passive_maker_taker()` execute through a live passive close path.
- A passive close chunk can be started, repriced, hedged, advanced, persisted, recovered, and finalized.
- Local-L2 mid price and venue tick size are both required inputs for repricing decisions.
- Maker fills are hedged by delta, not by replaying the entire chunk.
- Aggressive close behavior remains unchanged.
- Recovery can resume or clear an in-flight passive close after restart.
- No fake/paper/shadow execution path is introduced.

## Non-Goals

- rewriting entry flow
- changing the aggressive close strategy
- changing venue fee schedules or minimums
- replacing live adapter semantics with mocked or shadow execution
- collapsing passive and aggressive close into one blended code path

