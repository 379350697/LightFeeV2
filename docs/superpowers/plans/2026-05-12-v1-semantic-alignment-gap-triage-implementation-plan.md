# V1 Semantic Alignment Gap Triage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the remaining business-semantic gaps between LightFeeV2 and LightFee V1 without reintroducing retired behavior or chasing code-level replication.

**Architecture:** Use V1 as the behavioral contract and the gap-triage spec as the scope filter. Implement only the remaining active semantics: real config knobs with engine effect, review/evidence behavior, paper-outcome behavior, opportunity-input provider depth, and any state fidelity gaps proven by fixtures. Keep V2-native structure and avoid re-creating V1’s concurrency or Chillybot-era topology.

**Tech Stack:** Python, pytest, asyncio, dataclasses, JSONL journal, SQLite, GitNexus, existing LightFeeV2 parity harness.

---

## Reference Spec

This plan implements:

- `docs/superpowers/specs/2026-05-12-v1-semantic-alignment-gap-triage-design.md`

Read that spec first. It defines what should be ignored, what should be implemented, and what must be proven before implementation.

## Scope Split

This plan is intentionally split into five independent workstreams:

1. Active config knobs with real engine effect
2. Review observability chain
3. Paper outcome tracking
4. Opportunity-input provider depth
5. State fidelity verification

These can be assigned to separate workers if write scopes are respected.

---

### Task 1: Active Config Knobs With Real Engine Effect

**Files:**
- Modify: `lightfee/config/schema.py`
- Modify: `lightfee/config/validation.py`
- Modify: `lightfee/engine/entry.py`
- Modify: `lightfee/engine/entry_sync.py`
- Modify: `lightfee/marketdata/*` as needed
- Test: `tests/config/test_v1_real_config_gap_semantics.py`
- Test: `tests/engine/test_v1_real_config_gap_semantics.py`

- [ ] **Step 1: Write the failing config-surface tests**

Create tests for the remaining active V1 knobs:

```python
def test_local_l2_global_max_books_affects_capacity():
    ...

def test_maker_entry_max_reposts_limits_repost_attempts():
    ...

def test_pending_entry_zero_fill_terminal_cooldown_changes_terminal_timing():
    ...

def test_entry_min_size_round_up_whitelist_changes_sizing_behavior():
    ...
```

- [ ] **Step 2: Run the focused failing tests**

Run:

```bash
rtk pytest tests/config/test_v1_real_config_gap_semantics.py -v
rtk pytest tests/engine/test_v1_real_config_gap_semantics.py -v
```

Expected:
- At least one test fails because the parameter exists only partially or has no engine effect.

- [ ] **Step 3: Implement minimal parameter-to-behavior wiring**

Implement only the active semantics:

- `local_l2_global_max_books` must influence local-L2 capacity or scheduling limits.
- `maker_entry_max_reposts` must cap passive maker repost attempts in entry sync.
- `pending_entry_zero_fill_terminal_cooldown_ms` must influence pending-entry terminal timing.
- `entry_min_size_round_up_whitelist` must alter symbol-specific sizing behavior.

- [ ] **Step 4: Re-run focused tests**

Run:

```bash
rtk pytest tests/config/test_v1_real_config_gap_semantics.py -v
rtk pytest tests/engine/test_v1_real_config_gap_semantics.py -v
```

Expected:
- PASS

- [ ] **Step 5: Commit**

```bash
git add lightfee/config/schema.py lightfee/config/validation.py lightfee/engine/entry.py lightfee/engine/entry_sync.py tests/config/test_v1_real_config_gap_semantics.py tests/engine/test_v1_real_config_gap_semantics.py
git commit -m "feat: align active V1 config knobs with engine semantics"
```

---

### Task 2: Review Observability Chain

**Files:**
- Modify: `lightfee/config/schema.py`
- Modify: `lightfee/engine/entry.py`
- Modify: `lightfee/engine/exit.py`
- Modify: `lightfee/engine/risk_actions.py`
- Modify: `lightfee/persistence/journal.py`
- Test: `tests/engine/test_review_observability_semantics.py`
- Test: `tests/persistence/test_review_observability_semantics.py`

- [ ] **Step 1: Write the failing review-observability tests**

```python
def test_review_observability_enabled_assigns_review_id():
    ...

def test_review_observability_disabled_does_not_assign_review_id():
    ...

def test_review_id_propagates_into_persisted_state_and_journal():
    ...
```

- [ ] **Step 2: Run the focused failing tests**

Run:

```bash
rtk pytest tests/engine/test_review_observability_semantics.py -v
rtk pytest tests/persistence/test_review_observability_semantics.py -v
```

Expected:
- FAIL where V2 has field presence but not full behavior.

- [ ] **Step 3: Implement the minimal end-to-end observability chain**

Implement:

- `review_observability_enabled` config surface if missing.
- review-id generation at entry time.
- persistence and replay propagation of review id.
- visibility in relevant diagnostics/journal paths.

- [ ] **Step 4: Re-run focused tests**

Run:

```bash
rtk pytest tests/engine/test_review_observability_semantics.py -v
rtk pytest tests/persistence/test_review_observability_semantics.py -v
```

Expected:
- PASS

- [ ] **Step 5: Commit**

```bash
git add lightfee/config/schema.py lightfee/engine/entry.py lightfee/engine/exit.py lightfee/engine/risk_actions.py lightfee/persistence/journal.py tests/engine/test_review_observability_semantics.py tests/persistence/test_review_observability_semantics.py
git commit -m "feat: restore review observability semantics"
```

---

### Task 3: Paper Outcome Tracking

**Files:**
- Modify: `lightfee/config/schema.py`
- Modify: `lightfee/offline/analysis/journal.py`
- Modify: `lightfee/offline/reports/daily.py`
- Modify: `lightfee/persistence/sqlite_store.py`
- Create or modify runtime/offline helper modules as needed
- Test: `tests/offline/test_paper_outcome_tracking_semantics.py`

- [ ] **Step 1: Write the failing paper-outcome tests**

```python
def test_paper_outcome_tracking_disabled_is_noop():
    ...

def test_paper_outcome_finalist_limit_is_enforced():
    ...

def test_paper_outcome_markouts_use_configured_windows():
    ...

def test_paper_outcome_settlement_grace_changes_finalization():
    ...
```

- [ ] **Step 2: Run the focused failing tests**

Run:

```bash
rtk pytest tests/offline/test_paper_outcome_tracking_semantics.py -v
```

Expected:
- FAIL where V2 lacks behavior behind the config.

- [ ] **Step 3: Implement minimal tracking semantics**

Implement only active behavior:

- `paper_outcome_tracking_enabled`
- `paper_outcome_finalist_limit`
- `paper_outcome_markout_secs`
- `paper_outcome_settlement_grace_secs`

Do not invent extra product surface beyond parity needs.

- [ ] **Step 4: Re-run focused tests**

Run:

```bash
rtk pytest tests/offline/test_paper_outcome_tracking_semantics.py -v
```

Expected:
- PASS

- [ ] **Step 5: Commit**

```bash
git add lightfee/config/schema.py lightfee/offline/analysis/journal.py lightfee/offline/reports/daily.py lightfee/persistence/sqlite_store.py tests/offline/test_paper_outcome_tracking_semantics.py
git commit -m "feat: restore paper outcome tracking semantics"
```

---

### Task 4: Opportunity-Input Provider Depth

**Files:**
- Modify: `lightfee/config/schema.py`
- Modify: `lightfee/sidecar/snapshot.py`
- Modify: `lightfee/sidecar/service.py`
- Modify: `lightfee/engine/runtime.py`
- Create: `lightfee/sidecar/providers.py`
- Test: `tests/sidecar/test_provider_depth_semantics.py`

- [ ] **Step 1: Write the failing provider-depth tests**

```python
def test_direct_market_enriched_has_distinct_provenance():
    ...

def test_provider_source_mode_survives_into_diagnostics():
    ...

def test_domain_lifecycle_depth_matches_expected_semantics():
    ...
```

- [ ] **Step 2: Run the focused failing tests**

Run:

```bash
rtk pytest tests/sidecar/test_provider_depth_semantics.py -v
```

Expected:
- FAIL because V2 currently collapses some provider-depth semantics.

- [ ] **Step 3: Implement V2-native provider enrichment**

Implement:

- a V2-native equivalent for `direct_market_enriched` where it still adds meaningful provenance/diagnostics
- richer source mode and acquisition mode propagation
- domain lifecycle propagation only where it changes business diagnostics

Do not reintroduce Chillybot.

- [ ] **Step 4: Re-run focused tests**

Run:

```bash
rtk pytest tests/sidecar/test_provider_depth_semantics.py -v
```

Expected:
- PASS

- [ ] **Step 5: Commit**

```bash
git add lightfee/config/schema.py lightfee/sidecar/snapshot.py lightfee/sidecar/service.py lightfee/engine/runtime.py lightfee/sidecar/providers.py tests/sidecar/test_provider_depth_semantics.py
git commit -m "feat: align opportunity-input provider depth semantics"
```

---

### Task 5: State Fidelity Verification Before Further State Expansion

**Files:**
- Modify: `tests/persistence/test_v1_state_snapshot_semantics.py`
- Modify: `tests/offline/replay/test_replay_semantic_equivalence.py`
- Create: `tests/parity/test_remaining_state_fidelity_gaps.py`
- Modify production state/recovery files only if a proven missing field or semantic loss is found

- [ ] **Step 1: Add explicit fidelity-gap tests**

```python
def test_all_known_v1_business_fields_round_trip():
    ...

def test_replay_preserves_remaining_high_value_state_semantics():
    ...
```

- [ ] **Step 2: Run the fidelity tests and observe actual gaps**

Run:

```bash
rtk pytest tests/persistence/test_v1_state_snapshot_semantics.py -v
rtk pytest tests/offline/replay/test_replay_semantic_equivalence.py -v
rtk pytest tests/parity/test_remaining_state_fidelity_gaps.py -v
```

Expected:
- Either PASS, proving current V2 is sufficient, or a small number of specific missing fields/behaviors fail.

- [ ] **Step 3: Implement only proven missing state semantics**

If tests reveal real gaps:

- add only the missing fields/serialization/replay logic
- do not rewrite the entire state model

- [ ] **Step 4: Re-run the fidelity tests**

Run:

```bash
rtk pytest tests/persistence/test_v1_state_snapshot_semantics.py -v
rtk pytest tests/offline/replay/test_replay_semantic_equivalence.py -v
rtk pytest tests/parity/test_remaining_state_fidelity_gaps.py -v
```

Expected:
- PASS

- [ ] **Step 5: Commit**

```bash
git add tests/persistence/test_v1_state_snapshot_semantics.py tests/offline/replay/test_replay_semantic_equivalence.py tests/parity/test_remaining_state_fidelity_gaps.py lightfee/engine/state.py lightfee/engine/recovery.py
git commit -m "feat: prove and close remaining state fidelity gaps"
```

---

## Final Verification

- [ ] **Step 1: Run the remaining-gap suite**

Run:

```bash
rtk pytest tests/config tests/sidecar tests/offline tests/persistence tests/parity tests/engine -q
```

Expected:
- PASS

- [ ] **Step 2: Run broad repository verification**

Run:

```bash
rtk pytest -q
```

Expected:
- PASS

- [ ] **Step 3: Review deviations**

Confirm:

- no new Chillybot behavior was reintroduced
- any retained deviation remains explicit in `docs/parity/approved_deviations.md`

- [ ] **Step 4: Commit final integration**

```bash
git add docs/parity/approved_deviations.md
git commit -m "feat: close remaining V1 semantic alignment gaps"
```

---

## Execution Notes

- Do not reopen the `tokio` vs `asyncio` design question unless a business-semantic mismatch is proven.
- Do not add config flags without wiring behavior.
- Do not reintroduce retired Chillybot dependencies.
- Prefer proof-first for state fidelity gaps.
