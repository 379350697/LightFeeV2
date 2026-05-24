# V1 Passive Close Gap Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. This is a semantic execution plan, not a prewritten code patch. Keep each implementation step live-path only.

**Goal:** Restore Rust V1 passive close semantics in LightFeeV2 so normal closes can run through maker+taker passive close with chunking, repricing, delta hedge, persistence, and recovery.

**Architecture:** Add passive close as a separate live state machine beside the existing aggressive close executor. Keep aggressive close unchanged for risk closes and fallback. Use Rust V1 as the behavioral source of truth, and wire V2 through explicit venue contracts, `PendingPassiveClose`, local-L2 based repricing, runtime routing, and recovery.

**Tech Stack:** Python 3.12, asyncio, dataclasses, pytest, existing LightFeeV2 venue adapters, existing local-L2 runtime, GitNexus MCP, Rust V1 source at `/media/wl/新加卷/codex/LightFee`.

---

## Ready-To-Run Execution Prompt

Use this prompt to execute the spec and this plan:

```text
You are working in /media/wl/新加卷/codex/LightFeeV2.

Implement the V1 passive close gap closure using these documents:
- Spec: docs/superpowers/specs/2026-05-11-v1-passive-close-gap-closure-design.md
- Plan: docs/superpowers/plans/2026-05-11-v1-passive-close-gap-closure-implementation-plan.md

Primary source of truth is the Rust V1 repository:
- /media/wl/新加卷/codex/LightFee
- Key Rust anchors:
  - src/engine/exit.rs: PendingPassiveClose
  - src/engine/exit.rs: start_pending_passive_close
  - src/engine/exit.rs: drive_pending_passive_close
  - src/engine/exit.rs: maintain_passive_close_order
  - src/engine/exit.rs: process_pending_passive_closes
  - src/engine/entry.rs: post-only price hint, tick alignment, passive retry/backoff helpers

Hard requirements:
- Do not introduce fake, paper, or shadow execution paths.
- Do not claim passive close parity by changing aggressive close only.
- Do not merge passive close into PendingClose; add separate PendingPassiveClose semantics.
- Do not infer price tick from quantity step.
- Do not silently skip closes when passive maintenance is unavailable.
- Preserve existing aggressive close behavior for hard stop, risk delever, protection, and fallback.
- Before editing any function/class/method, run GitNexus impact analysis as required by AGENTS.md.
- Before committing, run GitNexus detect_changes.

Implementation order:
1. Confirm V1/V2 gap and current dirty worktree.
2. Add live resting-order contract support: submit passive order ack, query cumulative passive progress, amend, cancel, and canonical tick size.
3. Add PendingPassiveClose state, snapshot serialization, recovery deserialization, dedup indexing, and recovery work counting.
4. Add a dedicated passive close service/state machine that starts, drives, maintains, hedges maker-fill deltas, advances chunks, finalizes, and falls back to aggressive close when V1 would.
5. Wire runtime routing so normal close reasons using maker+taker start passive close; risk/protection closes remain aggressive.
6. Wire recovery so restored passive close state probes live flatness and either clears or resumes maintenance.
7. Add journal events and verification for start, progress, reprice, amend/cancel-replace, small-fill buffering, chunk advance, fallback, recovery, and finalization.
8. Run focused tests first, then the close/runtime/recovery/venue contract suite.

Acceptance:
- normal_close_reason_uses_passive_maker_taker() is consumed by production runtime routing.
- PendingPassiveClose can roundtrip through snapshot/recovery.
- Maker leg is GTC post-only reduce-only; hedge leg is IOC reduce-only.
- Maker fill deltas, not whole chunk quantities, drive taker hedges.
- Local L2 mid price and venue tick size are required for passive repricing.
- Chunked passive close advances independently per chunk.
- Existing aggressive close tests still pass.
- No product fake/paper/shadow path is added.
```

## References

- Spec: `docs/superpowers/specs/2026-05-11-v1-passive-close-gap-closure-design.md`
- V2 aggressive close owner: `lightfee/engine/close_executor.py`
- V2 state owner: `lightfee/engine/state.py`
- V2 close reason predicate: `lightfee/engine/exit_decision.py`
- V2 venue contract: `lightfee/core/contracts.py`
- V2 domain models: `lightfee/core/domain.py`
- V2 venue metadata: `lightfee/venues/specs.py`
- V2 venue transport: `lightfee/venues/transport.py`
- V2 local L2: `lightfee/marketdata/l2.py` and `lightfee/marketdata/local_l2_runtime.py`
- V2 runtime orchestration: `lightfee/engine/runtime.py`
- V2 recovery: `lightfee/engine/recovery.py`
- Rust V1 source: `/media/wl/新加卷/codex/LightFee`

## Non-Negotiable Semantics

Passive close is a separate close path:

- aggressive close: both legs IOC taker, tracked by `PendingClose`
- passive close: maker leg GTC post-only, taker hedge leg IOC, tracked by `PendingPassiveClose`

Passive close must preserve these V1 behaviors:

- per-position passive pending record
- per-chunk lifecycle
- maker leg selection
- post-only maker order submission
- cumulative maker progress polling
- hedge only the newly filled maker delta
- local-L2 mid/tick based repricing
- PassiveOrderManager hold/amend/cancel-replace/cooldown/budget decisions
- small-fill buffering and min-notional accumulation
- fallback to aggressive close when V1 would use dual taker or when passive cannot safely continue
- recovery after restart

## Execution Plan

### Phase 0: Safety And Baseline

- Read the spec and this plan.
- Check current dirty worktree and preserve unrelated changes.
- Re-run GitNexus analysis if the index is stale.
- For each symbol about to be edited, run upstream impact analysis and record the risk.
- Confirm V1 source anchors before porting any behavior.

Exit criteria:

- Current V2 gaps are still the same: no `PendingPassiveClose`, no passive close service, no runtime consumer for passive close routing.
- Existing aggressive close behavior is understood and left as the baseline.

### Phase 1: Live Venue Contract Prerequisites

Add the contract surface that passive close needs before writing the state machine.

Required semantics:

- passive maker submit returns an acknowledgement, not a terminal fill assumption
- passive progress query returns cumulative quantity, average price, fee, last fill time, and state
- amend and cancel are explicit resting-order operations
- venue metadata exposes price tick independently from quantity step

Affected areas:

- `lightfee/core/domain.py`
- `lightfee/core/contracts.py`
- `lightfee/venues/specs.py`
- `lightfee/venues/transport.py`
- venue adapter wrappers if they currently hide transport operations

Exit criteria:

- A live adapter can express a reduce-only post-only maker order without pretending it filled immediately.
- Repricing has a real tick-size source.
- Existing IOC close placement still behaves as before.

### Phase 2: PendingPassiveClose State

Add state that can represent the V1 passive close lifecycle.

Required fields:

- position id and close reason
- position snapshot
- target close quantity
- chunk quantities and active chunk index
- active maker leg and phase
- maker order id and client order id
- resting maker price and resting timestamp
- maker cumulative fill
- hedge cumulative fill
- persisted long and short close legs
- passive manager runtime
- small-fill buffer fields
- next retry timestamp
- created run/cycle metadata for dedup and recovery

Affected areas:

- `lightfee/engine/state.py`
- `lightfee/engine/recovery.py`
- snapshot write/read paths
- recovery work snapshot
- dedup index construction

Exit criteria:

- Passive pending state is not represented by `PendingClose`.
- Snapshot and recovery can roundtrip a pending passive close.
- Recovery sees passive pending close as pending recovery work.

### Phase 3: Passive Close Service

Create the owner for passive close lifecycle.

Expected owner:

- `lightfee/engine/passive_close.py`

Required operations:

- start pending passive close
- drive pending passive close
- maintain passive maker order
- process all ready pending passive closes
- append maker and hedge close legs
- finalize completed passive close
- clear passive pending state

Core semantics:

- choose maker leg using V1 logic
- submit maker as reduce-only GTC post-only
- poll cumulative maker progress
- compute maker fill delta versus prior cumulative fill
- submit taker hedge for the delta only
- advance chunk only when maker and hedge are aligned
- keep per-chunk state independent
- fall back to aggressive close for remaining quantity when V1 would use dual taker or post-only exhaustion

Exit criteria:

- One chunk can move from maker submit to maker progress to taker hedge to chunk completion.
- Multi-chunk passive close advances without losing cumulative fill state.
- The service finalizes through the same PnL aggregation semantics as close execution.

### Phase 4: Repricing And Budget Management

Wire passive maintenance to local L2 and PassiveOrderManager semantics.

Required inputs:

- fresh local L2 book
- best bid and best ask
- mid price
- venue tick size
- current resting maker price
- remaining maker quantity
- venue passive operation budget

Required decisions:

- hold when the order is close enough
- amend when supported and within amend threshold
- cancel-replace when deviation is too large or amend is unsupported
- cooldown after repeated failures
- budget-limited retry without losing pending state

Exit criteria:

- Repricing cannot run without local L2 and tick size.
- A repriced maker order preserves post-only side safety.
- Budget denial delays maintenance instead of dropping the close.

### Phase 5: Runtime Routing

Make the existing close reason predicate production-relevant.

Routing semantics:

- `funding_capture`, `trailing_exit`, `first_stage_capture`, `second_stage_capture`, `settlement_half_close`, and `settlement_force_close` start passive close.
- hard stop, death-line, risk delever, and protection close remain aggressive.
- passive close maintenance runs after local L2 sync.
- passive close maintenance has close priority over maker-entry repricing.
- existing aggressive close executor remains the fallback path.

Affected areas:

- `lightfee/engine/runtime.py`
- `lightfee/engine/exit_decision.py`
- runtime tests that cover active position close routing

Exit criteria:

- `normal_close_reason_uses_passive_maker_taker()` is consumed by runtime, not only unit-tested.
- Normal close can create `PendingPassiveClose`.
- Risk close still creates/uses aggressive close behavior.

### Phase 6: Recovery And Reconciliation

Resume passive close safely after restart.

Recovery semantics:

- restore `PendingPassiveClose`
- probe live position flatness first
- if flat, clear passive pending and emit recovery resolved event
- if still open, resume passive maintenance from the persisted chunk/order state
- if adapter state is ambiguous, use existing fail-closed/reconciliation protection

Exit criteria:

- Restart with pending passive close does not duplicate the maker order blindly.
- Restart with already-flat live position clears stale passive pending state.
- Restart with still-open position continues close instead of falling back silently.

### Phase 7: Verification

Run focused verification in this order:

1. route predicate and runtime routing
2. state snapshot/recovery roundtrip
3. passive close single chunk
4. passive close multi chunk
5. maker progress delta hedge
6. repricing with local L2 and tick size
7. small-fill buffer/min-notional behavior
8. passive recovery resume/clear
9. aggressive close regression
10. venue contract/transport codec coverage

Suggested command groups:

```bash
rtk pytest tests/test_exit_decisions.py tests/test_engine_recovery.py tests/test_recovery_reconciliation.py -q
rtk pytest tests/test_close_execution.py tests/test_live_full_closure.py -q
rtk pytest tests/test_venues_contract.py tests/test_venues_transport.py tests/test_marketdata_l2.py tests/test_local_l2_runtime.py -q
```

Exit criteria:

- Focused tests pass.
- Existing aggressive close tests pass.
- GitNexus `detect_changes` shows only expected passive-close-related impact.

## Completion Criteria

The implementation is complete only when all of these are true:

- V2 contains a real passive close path.
- Passive close is live-path, not fake/paper/shadow.
- Normal close routing reaches passive close.
- Passive close can be started, maintained, hedged, chunk-advanced, finalized, persisted, and recovered.
- Aggressive close behavior remains unchanged except where it is explicitly used as V1 fallback.
- Docs and tests reflect the final behavior.

