# V1 Semantic 100% Parity Design

**Goal:** Make LightFeeV2 behaviorally indistinguishable from LightFee V1 across live execution, record-layer, recovery, offline analysis, and evolution semantics. Any remaining mismatch must be explicitly named as an approved deviation, not left as implicit drift.

**Primary source of truth:** `/media/wl/新加卷/codex/LightFee`

**Target repository:** `/media/wl/新加卷/codex/LightFeeV2`

**Date:** 2026-05-12

---

## Decision

This is a semantic parity program, not a feature port.

V2 is already beyond a skeleton. It has real live execution paths, local-L2, venue adapters, journal replay, and a growing parity test suite. But the remaining work is not "more patches until it feels close". The target is stricter:

- every V1 user-visible behavior must exist in V2
- every V1 failure/fallback behavior must exist in V2
- every journal and replay semantic must be lossless or explicitly mapped
- every offline consumer that V1 relies on must produce equivalent outputs
- every difference must be documented, testable, and either closed or approved

If a V1 behavior only exists because production patches accumulated over time, it still counts. If V2 can implement it more cleanly, that is allowed, but only if the live behavior does not change.

## Semantic 100% Means

1. Live runtime control flow matches V1 by meaning, not by syntax.
2. Venue behavior matches V1 for supported and unsupported cases.
3. Journal events, payloads, ordering, and recovery semantics match V1.
4. Replay, analysis, counterfactual, and walk-forward outputs match V1 inputs and contracts.
5. Any deviation is named, tested, and recorded as an approved deviation.

This is stricter than "same happy path". It includes:

- startup and shutdown boundaries
- backoff, retry, and fail-closed behavior
- canonical symbol authority
- order lifecycle and close routing
- recovery and dedup
- local-L2 freshness and worker lifecycle
- PnL, journal, and replay fidelity
- offline analysis and evolution outputs

## Workstreams

The implementation should be split into independently owned workstreams so multiple agents can move in parallel.

| ID | Workstream | Primary Ownership | Main Output |
| --- | --- | --- | --- |
| WS-1 | Control plane and lifecycle | `lightfee/apps/live.py`, `lightfee/engine/runtime.py` | Startup, shutdown, loop scheduling, backoff, exports |
| WS-2 | Market data and venue contracts | `lightfee/core/contracts.py`, `lightfee/venues/*`, `lightfee/marketdata/*` | Canonical symbols, WS/bootstrap, venue truth, local-L2 behavior |
| WS-3 | Execution and risk semantics | `lightfee/engine/entry*.py`, `close_executor.py`, `passive_close.py`, `supervisor.py`, `risk_actions.py` | Entry, close, passive close, risk actions, record payloads |
| WS-4 | Persistence, recovery, replay | `lightfee/persistence/*`, `lightfee/engine/state.py`, `lightfee/engine/recovery.py`, `lightfee/offline/replay/*` | Journal, metrics, snapshot, recovery, replay fidelity |
| WS-5 | Offline analysis and evolution | `lightfee/offline/analysis/*`, `lightfee/offline/evolution/*`, `lightfee/offline/llm_evolution/*` | Journal analysis, evolution cycle, reports, counterfactuals |

## Coordination Model

The workstreams are intended to run in parallel after the shared contracts are understood.

Shared contract surfaces that require coordination:

- `lightfee/core/contracts.py`
- `lightfee/core/domain.py`
- `lightfee/engine/state.py`
- `lightfee/persistence/journal.py`
- `lightfee/persistence/metrics.py`

If one workstream needs to expand one of those shared contracts, it must record the change in a plan or parity note before dependent workstreams finalize their own changes.

## Source Baseline

Use the current parity documents as audit input, not as proof of completion:

- `docs/superpowers/parity/2026-05-11-v1-full-parity-gap-closure-matrix.md`
- `docs/superpowers/parity/2026-05-11-v1-record-layer-parity-matrix.md`
- `docs/superpowers/parity/2026-05-10-v1-v2-module-parity-matrix.md`

Those documents are useful because they already identify the remaining risk surface, but they are not the final authority. The final authority is the live behavior in V1 and the focused tests in V2.

## Acceptance Criteria

The parity program is only complete when all of the following are true:

- every live-path V1 behavior has a V2 equivalent or an approved deviation
- every journal and replay consumer is either aligned or explicitly marked partial
- every workstream plan passes its focused tests
- the full repo test sweep is clean enough to support the parity claim
- `detect_changes` shows only the symbols expected by the workstream
- no doc claims `fixed` without fresh evidence

## Non-Goals

- copying Rust file layout mechanically
- using fake, paper, or shadow behavior as a substitute for live semantics
- "close enough" drift tolerance
- changing business meaning just to make tests easier
- hiding unresolved gaps behind documentation wording

