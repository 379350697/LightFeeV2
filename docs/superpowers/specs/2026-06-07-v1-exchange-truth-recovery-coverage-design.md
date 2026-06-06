# V1 Exchange Truth Recovery Coverage Design

Date: 2026-06-07

Status: design draft for coverage and boundary closure.

## Source Verdict

This is not a request to line-copy the Rust recovery implementation. V2's
`ExchangeTruthSnapshot`, `RecoveryLedger`, `RecoveryOwnerIndex`, and
`V1RecoveryDecisionCore` are a valid lightweight closed-loop boundary: they make
V1's scattered startup/recovery ownership semantics explicit without copying the
whole Rust runtime shape.

The gap is coverage and a few contract edges, not architecture. The spec should
prove the whole truth -> owner -> ledger -> decision -> gate chain, especially
when only part of venue truth is available or multiple symbols are recovering at
once.

## Primary Sources

- Audit source: `/Users/wl/Downloads/lightfeev2_v1_audit_report-anti.md`
- V2 exchange truth shape: `lightfee/engine/exchange_truth.py`
- V2 ledger: `lightfee/engine/recovery_ledger.py`
- V2 owner index: `lightfee/engine/recovery_owner_index.py`
- V2 decision core: `lightfee/engine/recovery_decision_core.py`
- V2 runtime wiring:
  `lightfee/engine/runtime.py::_refresh_recovery_ledger_from_exchange_truth`,
  `lightfee/engine/runtime.py::_collect_recovery_ledger_exchange_truth`
- Production/diagnostic consumers:
  `scripts/diagnose_live.py`, `scripts/verify_production_services.py`

## Control Range

In scope:

- Add tests for multi-venue truth aggregation.
- Add tests for partial venue availability, unsupported symbol evidence, retry
  classification, and timeout classification.
- Add tests for snapshot version/shape consistency across runtime, diagnose,
  and production verifier payloads.
- Add tests for multi-symbol recovery interaction and entry gate scope.
- Fix small contract issues discovered by those tests if they are within the
  exchange truth, ledger, owner index, decision core, or runtime collection
  boundary.

Out of scope:

- Rewriting `RecoveryLedger` into Rust V1's exact file layout.
- Expanding probe scope to every venue for every possible market when the active
  recovery/candidate scope is narrower.
- Live exchange calls, production mutation, deploy, order submit, or cancel.
- Pending-entry hedge delta source-port. That belongs to
  `2026-06-07-v1-pending-entry-hedge-delta-closure-design.md`.
- State/recovery code-smell cleanup. That belongs to
  `2026-06-07-v1-state-contract-cleanup-design.md`.

## Problem Statement

The current code has a good boundary, but the audit found that
`tests/engine/test_exchange_truth_runtime.py` only covers payload serialization
and unsupported-symbol preservation. That leaves the most important production
inputs weakly specified:

- one venue succeeds while another times out;
- positions and open orders are discovered on different venues;
- unsupported symbols are evidence gaps, not live-artifact absence;
- retryable probe errors must stay visible to recovery decisions;
- snapshot payloads must stay schema-compatible for runtime, diagnose, and
  verifier consumers;
- multiple symbols can carry different recovery states at the same time.

## Target Contract

### Exchange truth shape

`ExchangeTruthSnapshot.to_legacy_payload()` and
`normalize_exchange_truth_payload()` must preserve:

- `available`
- `truth_available`
- `available_venues`
- `confidence`
- `positions`
- `open_orders`
- `probe_evidence`
- `position_probe_evidence`
- `open_order_probe_evidence`
- `fetch_status`
- `errors`
- `missing_evidence`
- `has_nonzero_position`
- `has_open_order`
- schema/version marker when present in producer payloads

Unsupported symbols and timeouts must remain separate evidence classes.

### Ledger classification

`RecoveryLedger.from_local_and_exchange_truth()` must classify:

- local flat plus live non-reduce order as `orphan_maker_order`;
- local flat plus live position as `unpaired_live_position`;
- owned open order as `owned_pending_entry`;
- reduce-only orphan as cleanup work, not ignored;
- unavailable truth plus no local work as non-blocking evidence gap;
- unavailable truth plus local work as blocking owned work plus truth gap;
- multiple symbols independently, with global blocking only for concrete orphan
  live artifacts.

### Decision core

`V1RecoveryDecisionCore` must preserve priority:

1. operator fail-closed;
2. orphan live artifact;
3. truth required for local recovery work;
4. owned recovery work;
5. non-blocking evidence gap;
6. running clean.

No test should accept local flat as production-safe when a concrete exchange
artifact exists.

### Runtime and diagnostic agreement

Runtime, `diagnose_live.py`, and `verify_production_services.py` must use the
same normalized truth fields. A consumer may render the fields differently, but
it must not invent a separate business interpretation.

## Required Test Matrix

Exchange truth payload tests:

- multi-venue positions and open orders survive roundtrip normalization;
- partial venue success plus timeout keeps `available=True` only when policy
  says the snapshot is usable and records the timed-out venue as evidence gap;
- unsupported symbol evidence is not counted as a successful flat probe;
- retryable probe errors remain in `errors`, `fetch_status`, and flattened
  `probe_evidence`;
- schema/version marker survives normalization.

Ledger and owner tests:

- two symbols with independent pending entries block only their same-symbol
  scope unless a global orphan live artifact exists;
- one symbol has owned pending work while another has orphan maker order; the
  orphan globally blocks entry and owned work remains visible;
- partial venue truth with local pending work returns owned recovery plus truth
  gap, not running clean;
- owned order from journal/client id remains `owned_pending_entry` even when
  local `pending_entries` is empty.

Runtime/diagnostic tests:

- `_collect_recovery_ledger_exchange_truth` records per-venue position and
  open-order evidence for every requested symbol;
- runtime ledger refresh and diagnose recovery decision agree for the same
  sanitized exchange truth payload;
- production verifier treats missing exchange truth as blocking only when its
  selected policy requires truth or local recovery work exists.

## Parallelization Boundary

Can be parallel:

- Exchange truth payload tests.
- Pure ledger/owner-index tests.
- Diagnostic/verifier payload tests.

Must be serial:

- Runtime collector changes in `lightfee/engine/runtime.py`.
- Decision-core changes that alter entry admission or block clearing.
- Any change that affects `clear_legacy_recovery_block_via_core` or risk mode.

## Acceptance Criteria

- `test_exchange_truth_runtime.py` covers multi-venue, partial availability,
  retry/timeout classification, unsupported symbol evidence, and schema
  consistency.
- Ledger tests cover at least one multi-symbol recovery scenario.
- Runtime and diagnostic consumers agree on the same normalized exchange truth
  payload.
- No concrete live exchange artifact can be classified as running clean.
- Evidence gaps without local work remain non-blocking, matching the current V2
  lightweight boundary.
- Existing recovery decision and production verifier tests remain green.
