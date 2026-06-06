# V1 Recovery Decision Core Closed Loop Design

Date: 2026-06-06
Status: Proposed for implementation
Scope: LightFeeV2 recovery, lifecycle gate, entry gate, diagnose, and production
health semantics for the `ambiguous_exchange_truth` / stale recovery-block bug
family.

## Goal

Close the whole `ambiguous_exchange_truth -> stale_recovery_block_cleared` bug
family by replacing competing runtime decisions with one V1-compatible recovery
decision core.

The fix must not be another symbol branch or a single `if`. It must make the
same pure decision decide:

- whether exchange/live evidence is recovery work, risk-only evidence, or a
  non-blocking evidence gap.
- whether lifecycle should block, manage existing work, or run clean.
- whether the entry gate may dispatch new risk.
- whether a prior recovery block should remain, change reason, or clear.
- what diagnose and production health should report.

## Source Of Truth

- V1 is authoritative for compatibility-sensitive trading semantics:
  `/Users/wl/projects/LightFee`.
- Existing V2 building blocks remain useful implementation inputs:
  - `lightfee/engine/recovery_ledger.py`
  - `lightfee/engine/exchange_truth.py`
  - `lightfee/engine/recovery_owner_index.py`
  - `lightfee/engine/pending_entry_terminalizer.py`
  - `lightfee/engine/runtime.py`
- Existing design baseline:
  - `docs/superpowers/specs/2026-06-05-exchange-truth-recovery-ledger-v1-parity-design.md`
  - `docs/superpowers/plans/2026-06-05-exchange-truth-recovery-ledger-v1-parity-implementation-plan.md`
  - `docs/bugs/contracts/pending-entry-live-truth-contract.md`
  - `docs/bugs/cards/pending-entry-terminality-live-truth.md`

## Problem Statement

V2 currently has the right ingredients but still allows two authorities to fight:

1. `RecoveryLedger` can classify unavailable or partial exchange truth as
   `ambiguous_exchange_truth` and create
   `exchange_truth_recovery_ledger_blocked`.
2. A later local-clean helper such as
   `clear_stale_recovery_block_if_recovery_clean()` can clear that block because
   local recovery collections are empty.

That oscillation is the bug. It proves that block and clear are not controlled
by one closed-loop semantic authority.

The core drift is not that V2 lacks a condition. The drift is that probe
availability, owner evidence, recovery work, lifecycle state, entry eligibility,
and stale-block clearing can each reach a conclusion through separate paths.

## Non-Negotiable Constraints

- Do not make opening positions harder for normal clean operation.
- Do not lower stability by suppressing real live-artifact blockers.
- Do not copy the V1 Rust file layout wholesale into Python.
- Do not create a second runtime.
- Do not let runtime, diagnose, production health, and entry gate each carry
  independent recovery semantics.
- Keep business semantics centralized and side effects loosely coupled.
- Keep execution capabilities where they are: runtime/executors may submit,
  cancel, flatten, journal, or mutate state; the decision core may not.
- Candidate-scoped truth probe errors must not become global recovery blocks by
  themselves.
- A concrete live artifact or local recovery work still fails safe.

## Boundary Model

`V1RecoveryDecisionCore` is a pure semantic boundary. It accepts snapshots and
returns a decision. It does not fetch exchange data, submit orders, cancel
orders, write journals, sleep, mutate runtime state, or inspect files.

Runtime remains the orchestrator:

1. collect local state and exchange truth.
2. build a `RecoveryEvidenceSnapshot`.
3. call `V1RecoveryDecisionCore.decide(snapshot)`.
4. execute the returned lifecycle, recovery, entry, clear, and diagnostic
   policy through existing side-effect paths.

The core owns the business meaning. Existing modules own data collection,
normalization, owner lookup, terminalization, and execution.

## Evidence Classes

The core must classify evidence before producing a lifecycle result.

| Evidence class | Meaning | Blocking rule |
|---|---|---|
| `complete_flat` | Local work is empty and required exchange truth proves no live position and no non-reduce open order. | No block. |
| `complete_live_artifact` | Exchange truth reports live position or non-reduce open order. | Block/manage/flatten depending on owner. |
| `owned_recovery_work` | Pending entry, residual repair, passive close, open position, or owner-mapped live artifact exists. | Manage existing work; block new entry risk as needed. |
| `orphan_live_artifact` | Live order/position exists but owner evidence is absent or insufficient. | Block and route to explicit recovery/flatten/fail-closed policy. |
| `partial_evidence_gap` | Probe timeout/error/unsupported venue/partial venue coverage with no local recovery work and no concrete live artifact. | Warn/diagnose only; no global recovery block. |
| `truth_unavailable_for_required_recovery` | Truth is unavailable while local recovery work, owned artifact, or unresolved pending/passive/residual work requires truth. | Risk-only wait or fail-closed; block new entry risk. |
| `operator_fail_closed` | Operator or existing lifecycle state intentionally blocks trading. | Preserve until operator/runtime rule releases it. |

`ambiguous_exchange_truth` is not a standalone global blocker. It is an evidence
quality label that can become blocking only when the snapshot also contains
required recovery work, a concrete live artifact, owner evidence, unresolved
pending/passive/residual work, or an explicit fail-closed operator policy.

## Decision Results

The core should return a typed result equivalent to:

| Decision | Lifecycle policy | Entry policy | Recovery/block policy |
|---|---|---|---|
| `RUNNING_CLEAN` | normal running | allow normal entry | clear previous core-owned recovery block |
| `RUNNING_WITH_EVIDENCE_GAP` | normal running with warning | allow normal entry unless candidate-specific gate blocks | clear previous `ambiguous_exchange_truth` core block; report evidence gap |
| `RISK_ONLY_WAIT_FOR_TRUTH` | risk-only / no new risk | block new entry | keep or create blocker because existing work needs truth |
| `MANAGE_OWNED_RECOVERY_WORK` | recovery management | block conflicting new risk | keep/create specific recovery work reason |
| `BLOCK_OR_FLATTEN_LIVE_ARTIFACT` | fail-safe management | block new entry | orphan or unpaired live artifact must be handled |
| `OPERATOR_FAIL_CLOSED_PRESERVED` | preserve fail-closed | block per operator policy | do not auto-clear |

Every result must include:

- `decision`
- `block_reason` or `clear_reason`
- `entry_allowed`
- `entry_block_reason`
- `recovery_work_items`
- `diagnostic_severity`
- `evidence_quality`
- `journal_event_name`
- `maintenance_note` for future auditors when the result looks non-obvious

## Closed-Loop State Transition

Only the core may decide a core-owned recovery block state.

```text
collect snapshots
  -> classify evidence
  -> resolve owner
  -> plan recovery work
  -> decide lifecycle and entry
  -> decide block/clear
  -> emit one semantic result
```

Runtime may still have migration or legacy cleanup helpers, but they must not
contradict the core. A stale-clean helper may remove only obsolete pre-core
blocks when the core says `RUNNING_CLEAN` or `RUNNING_WITH_EVIDENCE_GAP`.

## Old Semantics To Retire

Retire these as decision authorities:

1. `truth_available=False -> ambiguous_exchange_truth -> global recovery block`
   without checking local work or live artifacts.
2. `clear_stale_recovery_block_if_recovery_clean()` as the primary corrective
   mechanism against ledger decisions.
3. Entry gate reading a coarse `RecoveryLedger.has_blocking_work()` result
   directly.
4. Diagnose, production health, and runtime each applying separate truth
   semantics.
5. Candidate scan truth errors mutating or latching global runtime recovery
   blocks.

Keep these capabilities:

1. orphan maker order detection.
2. unpaired live position detection.
3. owned pending entry / owner index mapping.
4. pending-entry terminalizer.
5. residual repair and passive-close execution.
6. false-green exchange-truth diagnose and production health gates.
7. all-venue flat truth diagnostics.

## Entry Availability Rule

The upgrade must not make opening positions harder under clean conditions.

Entry can be blocked only by:

- concrete live artifact.
- local recovery work.
- unresolved pending/passive/residual/open work.
- same-symbol/venue-overlap pending protection.
- operator fail-closed or configured risk policy.
- candidate-specific gate such as first-funding horizon, deterministic admission
  reject, local-L2 readiness, or venue eligibility.

Flat local state plus a candidate-scoped truth probe timeout is
`RUNNING_WITH_EVIDENCE_GAP`, not a global entry block.

## Diagnose And Production Health

Runtime acceptance and production acceptance are related but not identical.

- Runtime may continue as `RUNNING_WITH_EVIDENCE_GAP` when no local recovery work
  or live artifact exists.
- Production health may still report incomplete evidence if the operator asks
  for all-venue high-confidence proof.
- Neither path may report healthy if exchange truth proves a live position or
  non-reduce open order while local state is flat.

## Maintenance Notes

The implementation should include short comments only at the authority boundary:

- in the pure core, explain why evidence gap is not recovery work by itself.
- in runtime integration, explain that block and clear both come from the core.
- in legacy cleanup, explain that it is migration fallback only and cannot decide
  against the core.

Avoid duplicating the full business matrix in comments. The source of truth is
this spec plus `docs/bugs/contracts/pending-entry-live-truth-contract.md`.

## Acceptance Criteria

1. Flat local state, no pending/open/residual/passive work, and unsupported or
   timed-out exchange truth returns `RUNNING_WITH_EVIDENCE_GAP`, does not create
   `exchange_truth_recovery_ledger_blocked`, and does not block normal entry.
2. Existing local recovery work plus unavailable truth returns
   `RISK_ONLY_WAIT_FOR_TRUTH` or a stricter specific recovery decision.
3. A live non-reduce open maker order with no owner returns
   `BLOCK_OR_FLATTEN_LIVE_ARTIFACT` with `orphan_maker_order`.
4. A live position with no owner returns `BLOCK_OR_FLATTEN_LIVE_ARTIFACT` with
   `unpaired_live_position`.
5. A previous core-owned `exchange_truth_recovery_ledger_blocked` state clears
   only through a core decision.
6. Runtime entry gating, diagnose, and production health consume the same core
   classification instead of local re-implementations.
7. Static/code audit finds no new symbol-specific branch for `TRXUSDT`,
   `SEIUSDT`, or any other incident symbol.
8. Tests prove the oscillation
   `ambiguous_exchange_truth -> stale_recovery_block_cleared` cannot recur for
   the no-local-work/evidence-gap shape.

## Non-Goals

- Broad Rust-to-Python source copy.
- Replacing venue adapters.
- Rewriting pending-entry terminalization.
- Changing order submit/cancel/flatten mechanics.
- Weakening fail-safe behavior for real live exposure.
- Marking production high-confidence green when required credentialed truth was
  not collected.
