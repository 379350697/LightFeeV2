# V1 Semantic Alignment Gap Triage Design

**Goal:** Distinguish between historical residue and real business-semantic gaps between LightFee V1 and LightFeeV2, then define the remaining parity work that is still worth implementing.

## Problem Statement

LightFeeV2 now passes its parity-oriented test suite and broad repository verification, but that does not mean every V1 concept should be copied forward.

There are still visible differences across:

- config surface area
- opportunity-input provider depth
- runtime/control-plane orchestration shape
- state model richness
- review/evidence and paper-outcome paths

The next step is not blind porting. The next step is semantic triage:

1. Which V1 parameters and fields are real production behavior?
2. Which ones are dead or retired historical residue?
3. Which V1 provider layers still encode meaningful business semantics?
4. Which differences are implementation-only, and which still affect business behavior?

This design defines that triage and names the remaining implementation-worthy gaps.

---

## Current High-Level Assessment

### Already Good Enough To Treat As Implementation Difference

- `tokio` in V1 vs `asyncio` in V2 is now mostly an implementation difference, not a major business-semantic gap.
- Lane separation, rate-limit reload behavior, current-state export, maker-event scheduling, and failure isolation are now close enough that the concurrency model no longer needs to be made structurally identical.
- V2 should not attempt code-structure parity here.

### Still Real Semantic Depth Gaps

- opportunity-input provider layering and provenance
- local-L2 resource budgeting knobs
- passive maker repost/cooldown policy knobs
- review observability / review-id generation chain
- paper outcome tracking and settlement/markout semantics
- some high-detail sizing / whitelist entry behavior

### Explicitly Retired / Approved-Deviation Candidates

- Chillybot-specific config and provider branches
- Chillybot hint-assist / feed integration
- other directly Chillybot-coupled fields whose only purpose was integrating that retired source

---

## Decision Framework

Every unmatched V1 concept must be classified into exactly one of these buckets.

### Bucket A: Retired Historical Residue

Definition:
- V1 field or branch exists, but it is tied to a retired subsystem, removed data source, or no-longer-valid operating model.

Required action:
- Do not implement behavior in V2.
- Keep a clear compatibility error or approved deviation entry.

Examples:
- `sidecar_chillybot_mode`
- `chillybot_api_base`
- `chillybot_timeout_ms`
- Chillybot-specific sidecar hint assist branches

### Bucket B: Implementation-Only Difference

Definition:
- V1 and V2 behave the same from the business perspective, but scheduling model, file structure, or internal orchestration differs.

Required action:
- Do not port structure.
- Keep V2-native design.
- Add tests only if business behavior is still ambiguous.

Examples:
- `tokio::select!` vs `asyncio` lane orchestration
- helper decomposition and module boundaries

### Bucket C: Real Business Gap Requiring V2 Feature Alignment

Definition:
- V1 field, branch, or provider layer is still actively consumed by runtime behavior, recovery, reporting, observability, sizing, or operator workflows.

Required action:
- Add parameter and corresponding behavior in V2.
- Do not add config-only stubs with no engine effect.

### Bucket D: Needs Proof Before Deciding

Definition:
- Surface exists in V1 and V2 partially, but it is unclear whether V2 already preserves enough semantics.

Required action:
- Build source-backed evidence and fixture tests before deciding whether to implement more.

---

## Triage Conclusions

## 1. Config Surface

### Retired Historical Residue

- Chillybot fields are retired and should remain rejected in V2.
- V2 should not reintroduce Chillybot provider branches or config keys.

### Real Business Gaps

These V1 config items still drive production behavior and should be treated as real semantic gaps if not fully implemented in V2:

- `local_l2_global_max_books`
- `maker_entry_max_reposts`
- `pending_entry_zero_fill_terminal_cooldown_ms`
- `entry_min_size_round_up_whitelist`
- `review_observability_enabled`
- `paper_outcome_tracking_enabled`
- `paper_outcome_finalist_limit`
- `paper_outcome_markout_secs`
- `paper_outcome_settlement_grace_secs`

Why they are real:

- `local_l2_global_max_books` controls active/warm/retained book capacity and therefore affects market-data resource behavior.
- `maker_entry_max_reposts` changes passive maker retry/repost semantics in entry sync.
- `pending_entry_zero_fill_terminal_cooldown_ms` changes pending-entry terminalization timing.
- `entry_min_size_round_up_whitelist` affects sizing/normalization behavior for specific symbols.
- `review_observability_enabled` drives review-id creation and downstream evidence/diagnostics chains.
- `paper_outcome_tracking_*` drives paper-outcome finalist tracking, markouts, and settlement grace behavior.

These are not “carry-over clutter”; they alter operator-visible or trading-visible semantics.

## 2. Runtime / Control Plane

### Implementation-Only Difference

- V1’s `tokio::select!` orchestration and V2’s `asyncio` control loop are now acceptable as different implementations.
- The remaining question is not structure, but whether per-lane timing, wakeup, error isolation, shutdown, and export semantics remain equivalent.

### Current Conclusion

- No major new structural parity project is needed here.
- Only incremental follow-up is justified if a business behavior mismatch is found in live fixture or operator export evidence.

## 3. Opportunity Input Provider Depth

### Real Business Gap

V1 provider layering is not just extra ceremony.

V1 still contains meaningful semantic structure across:

- `direct_market`
- `coarse_sidecar`
- `sidecar_scan`
- `direct_market_enriched`
- richer provenance / acquisition mode / diagnostics / domain lifecycle handling

The V2 runtime now recognizes the high-level modes, but it does not yet clearly expose all of V1’s provider-depth semantics.

### Specific Conclusion

- `direct_market_enriched` should not be dismissed as historical residue.
- It appears to add real enrichment/provenance semantics on top of direct market collection.
- This should be evaluated as an alignment target, not auto-filtered out.

### What Needs Alignment

- acquisition mode / provenance representation
- direct-market-enriched behavior or an equivalent V2-native enrichment layer
- richer domain lifecycle and diagnostic propagation if V2 is still shallower

## 4. State Model

### Improved Status

V2 already carries many state fields that were previously missing:

- `review_id`
- `opportunity_origin_tags`
- `opportunity_hint_source`
- `risk_delever_realized_*`
- `protection_realized_*`
- `entry_quality_markout_*`
- `settlement_half_closed_*`
- `live_recovery_reduce_only_pairs`
- `venue_market_data_degradations`
- `transfer_truth`
- `entry_liquidity_qualification_records`

### Remaining Conclusion

- There is no longer evidence of a broad structural state deficit.
- Remaining risk is now about full losslessness and edge-field fidelity, not obvious missing blocks.
- This should be treated as a verification-depth problem, not an architectural rewrite problem.

## 5. Review / Evidence / Paper Outcome Paths

### Real Business Gap

Even though many state fields now exist in V2, the semantic chain behind them is not yet proven complete.

In V1:

- `review_observability_enabled` gates review-id generation and review-linked diagnostics/evidence behavior.
- `paper_outcome_tracking_*` drives paper finalists, markouts, and settlement grace semantics.

If V2 lacks the behavioral chain behind those knobs, then parameter parity has not been achieved.

This area is still a real semantic gap until behavior, not just fields, is demonstrated.

---

## Recommended Remaining Workstreams

### Workstream 1: Config Knobs With Real Engine Effect

Add and wire the remaining active V1 parameters only where they still change runtime behavior:

- `local_l2_global_max_books`
- `maker_entry_max_reposts`
- `pending_entry_zero_fill_terminal_cooldown_ms`
- `entry_min_size_round_up_whitelist`

### Workstream 2: Review Observability Chain

Prove or implement the end-to-end semantic behavior behind:

- `review_observability_enabled`
- review-id assignment
- review-linked diagnostics / evidence propagation

### Workstream 3: Paper Outcome Tracking

Add or verify V1-equivalent behavior for:

- `paper_outcome_tracking_enabled`
- `paper_outcome_finalist_limit`
- `paper_outcome_markout_secs`
- `paper_outcome_settlement_grace_secs`

### Workstream 4: Opportunity Provider Depth

Evaluate and implement V2-native equivalents for:

- `direct_market_enriched`
- richer provenance / acquisition mode semantics
- domain lifecycle / diagnostic propagation that still exists in V1 but is shallow in V2

### Workstream 5: State Fidelity Verification

Do not start a broad state rewrite.

Instead:

- use fixture snapshots and journal replays
- prove which remaining V1 state fields are still semantically missing
- only then add missing state semantics

---

## Non-Goals

- Reintroducing Chillybot behavior
- Reproducing V1 module boundaries or concurrency structure
- Adding V1 config keys without wiring real behavior behind them
- Rewriting already-validated asyncio control flow to mimic `tokio`

---

## Acceptance Criteria For This Triage

This design is successful if follow-on implementation work:

- does not waste time porting retired Chillybot behavior
- does not reopen the `tokio` vs `asyncio` structure question unnecessarily
- does prioritize the still-active config knobs and provider-depth semantics
- does treat state parity as verification-first, not rewrite-first
- does separate “field exists” from “real business chain is implemented”
