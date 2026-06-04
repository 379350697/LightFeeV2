# Exchange Truth Recovery Ledger V1 Parity Design

Date: 2026-06-05

Status: approved design draft for implementation planning. This document turns
the current CL-048 / CL-049 / CL-050 family into one runtime architecture
change instead of another symbol-specific patch.

## Goal

Fully replicate the Rust V1 live-trading recovery semantics for pending-entry
recovery, live-position truth, residual repair, reduce-only cleanup, and
lifecycle gating. The V2 implementation may stay Pythonic, but it must not
cover fewer business cases than V1 and it must remove the extra local branches
created by previous surface fixes.

## Primary Sources

- V1 repository: `/Users/wl/projects/LightFee`
- V1 pending-entry source: `/Users/wl/projects/LightFee/src/execution_core/entry_sync.rs`
- V1 supervision source: `/Users/wl/projects/LightFee/src/engine/supervision.rs`
- V1 state source: `/Users/wl/projects/LightFee/src/engine/state.rs`
- V2 active contract: `docs/bugs/contracts/pending-entry-live-truth-contract.md`
- Current production recurrence: local flat while Bybit has a live non-reduce
  open order for `TRXUSDT`.

## Problem Statement

V2 currently has many correct-looking local patches, but it does not have the
same structural ownership model as V1. Pending-entry recovery, startup live
truth, residual repair, open-order cleanup, entry gating, and production
diagnostics are split across runtime branches. That split allows this unsafe
shape:

```text
exchange truth: live position or non-reduce open order exists
local runtime: open_positions=[], pending_entries=[]
service state: active/running
diagnose: unhealthy after the fact
```

This is not a deployment problem and not a Python performance problem. It is a
runtime ownership problem: exchange truth is still discovered by diagnostics
after runtime has already treated local state as sufficient.

## Non-Negotiable Invariants

1. Exchange truth is runtime input, not only a diagnostic output.
2. Every live exchange artifact has exactly one runtime owner or becomes
   blocking recovery work.
3. Local flat is not state. Ledger-proven flat is state.
4. Positive fill evidence is never discarded or overwritten by later zero-fill
   evidence.
5. Pending-entry terminality has one authority. Direct pending removal outside
   that authority is a contract bypass.
6. Residual repair decisions are driven by live position and open-order truth,
   not stale local deltas.
7. Runtime cannot enter normal entry scan while the ledger contains blocking
   work.
8. `risk_only` can be safe, but it is not production acceptance when exchange
   truth is unresolved.
9. New production recurrences must first map to the contract matrix. Covered
   rows get evidence and docs; uncovered rows get RED tests before code.

## Target Architecture

### Recovery Ledger

Create `lightfee/engine/recovery_ledger.py`.

Responsibilities:

- accept normalized exchange truth snapshots;
- accept local runtime state summaries;
- accept owner evidence from pending entries, open positions, residual repair
  tasks, passive closes, and journal events;
- classify every live artifact into a recovery work item;
- expose a recovery work snapshot that drives lifecycle and entry gating.

Core concepts:

```text
ExchangeArtifact
RecoveryOwner
RecoveryWorkItem
RecoveryDecision
RecoveryLedger
```

Required work kinds:

- `owned_pending_entry`
- `owned_open_position`
- `pending_residual_repair`
- `pending_passive_close`
- `orphan_maker_order`
- `orphan_reduce_only_order`
- `unpaired_live_position`
- `ambiguous_exchange_truth`

Required outcomes:

- `managed_open_position`
- `residual_repair_queued`
- `reduce_only_cleanup_submitted`
- `owned_order_cancel_requested`
- `proven_flat`
- `fail_closed_operator_block`

### Exchange Truth Runtime Adapter

Create or formalize `lightfee/engine/exchange_truth.py`.

Responsibilities:

- collect all-venue position truth;
- collect all-venue open-order truth;
- preserve probe evidence, timeout budget, venue, endpoint, raw classification,
  and unsupported-symbol evidence;
- expose the same semantic shape to runtime, production verifier, and
  `diagnose_live.py`.

Diagnostics may render this truth differently, but they must not own a second
business interpretation.

### Recovery Owner Index

Create `lightfee/engine/recovery_owner_index.py`.

Responsibilities:

- map live orders and positions back to pending entries, open positions,
  residual repairs, passive closes, journal events, order ids, and client ids;
- distinguish proven owner, probable owner, and orphan;
- never guess an owner when the evidence is insufficient.

The current `TRXUSDT` open maker order must resolve to one of two states:

- owned pending-entry recovery when local or journal evidence proves ownership;
- `orphan_maker_order` fail-closed blocker when ownership cannot be proven.

### Pending Entry Terminalizer

Create `lightfee/engine/pending_entry_terminalizer.py`.

Responsibilities:

- own all pending-entry finalization;
- hydrate from live balanced exposure;
- open matched managed positions;
- queue residual repair for imbalance;
- retain unresolved positive-fill evidence;
- terminalize zero-fill only with terminal no-fill evidence;
- return an explicit boolean or decision object that callers must respect.

No caller may remove a pending entry unless this component returns a terminal
decision.

### LiveRuntime Role

`lightfee/engine/runtime.py` remains the orchestrator, not the truth owner.

Startup flow:

1. collect exchange truth;
2. build owner index;
3. build recovery ledger;
4. drive recoverable work;
5. block normal entry scan if blocking work remains.

Normal tick flow:

1. refresh scoped truth for active recovery work;
2. periodically refresh all-venue truth;
3. build/update the ledger;
4. process recovery work before entry selection;
5. allow entry scan only when the ledger proves it is safe.

Entry admission:

- ask the ledger whether a candidate overlaps unresolved work;
- block same pair or same symbol with venue overlap like V1;
- block all new entry risk when the ledger has an orphan maker order,
  unpaired live position, or ambiguous exchange truth.

## Surface Complexity To Remove

These are not independent feature areas. They are symptoms of the missing
ledger boundary and should be removed after tests prove the new path:

- direct `state.pending_entries.pop()` after partial or deferred finalization;
- scattered startup positive-fill recovery branches;
- rejected-pending-with-fill loops that retain forever;
- local-flat acceptance before exchange open-order truth is known;
- diagnosis-only false-green compensation;
- duplicated same-symbol pending-entry dedup outside a shared ledger gate;
- separate CL-048, CL-049, and CL-050 recovery paths;
- open-order truth that only appears in `diagnose_live.py`;
- fallback cleanup branches that do not record owner evidence.

## Required Behavior For Current Production Shape

Given:

```text
local open_positions: empty
local pending_entries: empty or incomplete
exchange open order: Bybit TRXUSDT Buy 72.0, reduce_only=false
exchange positions: flat
```

Runtime must do one of the following:

1. reconstruct the owner and route through owned pending-entry recovery;
2. prove it is a safe owned open order and drive V1 cleanup;
3. fail closed with `orphan_maker_order` and block new entries.

Runtime must not:

- report production healthy;
- enter normal entry scan;
- silently clear evidence;
- rely on local flat as acceptance.

## Test Strategy

Tests must be added in this order:

1. pure ledger matrix tests;
2. owner reconstruction tests with sanitized incident fixtures;
3. pending-entry terminalizer tests;
4. runtime startup integration tests;
5. runtime tick and entry gate tests;
6. diagnose/verifier agreement tests;
7. cleanup audit tests that reject bypasses.

## Acceptance Criteria

The implementation is accepted only when all are true:

- `TRXUSDT` open-order/local-flat recurrence is reproduced by a RED test.
- `SEIUSDT` positive-fill/local-false-flat recurrence stays covered.
- Runtime and diagnose agree on exchange truth health.
- No new entry can be selected while blocking ledger work exists.
- Positive fill evidence cannot be cleared by any runtime path.
- Direct pending-entry removal outside the terminalizer is removed or covered by
  a static bypass test.
- `docs/bugs/contracts/pending-entry-live-truth-contract.md` maps CL-048,
  CL-049, CL-050, and future recurrences to ledger rows.
- Full verification passes before deployment.

## Non-Goals

- Rewriting Python to match Rust file layout line by line.
- Manual production order cancellation as a root fix.
- Expanding passive-close behavior to hide pending-entry recovery gaps.
- Treating exchange-rule rejects as system bugs when official venue evidence
  proves they are admission blocks.
- Adding another CL-specific runtime branch without routing through the ledger.
