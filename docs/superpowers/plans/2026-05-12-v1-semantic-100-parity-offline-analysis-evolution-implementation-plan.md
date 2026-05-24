# V1 Semantic 100% Parity Offline Analysis and Evolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. This plan owns offline analysis, reporting, evolution, and LLM-evolution parity.

**Goal:** Make LightFeeV2 offline analysis and evolution outputs semantically equivalent to V1 where those surfaces are part of the product behavior.

**Architecture:** Offline analysis consumes journal/replay evidence but must not mutate live runtime state. Evolution consumes analysis outputs, proposal ledgers, parameter registries, and approval records. LLM evolution remains disabled unless explicitly configured, but its disabled/enabled behavior should match V1 semantics.

**Tech Stack:** Python 3.12, dataclasses, JSON, markdown rendering, pytest, GitNexus MCP, Rust V1 source under `/media/wl/新加卷/codex/LightFee`.

---

## Reference Docs

- Master spec: `docs/superpowers/specs/2026-05-12-v1-semantic-100-parity-design.md`
- Record-layer parity matrix: `docs/superpowers/parity/2026-05-11-v1-record-layer-parity-matrix.md`
- Rust V1 anchors:
  - `/media/wl/新加卷/codex/LightFee/src/analysis.rs`
  - `/media/wl/新加卷/codex/LightFee/src/analysis/review_samples.rs`
  - `/media/wl/新加卷/codex/LightFee/src/evolution/*`
  - `/media/wl/新加卷/codex/LightFee/src/llm_evolution/*`
  - `/media/wl/新加卷/codex/LightFee/src/offline_replay/*`

## File Ownership

- Modify: `lightfee/offline/analysis/journal.py`
- Modify: `lightfee/offline/analysis/incident.py`
- Modify: `lightfee/offline/reports/daily.py`
- Modify: `lightfee/offline/reports/render.py`
- Modify: `lightfee/offline/evolution/approval.py`
- Modify: `lightfee/offline/evolution/cycle.py`
- Modify: `lightfee/offline/evolution/ledger.py`
- Modify: `lightfee/offline/evolution/report.py`
- Modify: `lightfee/offline/llm_evolution/report.py`
- Modify: `lightfee/apps/report.py`
- Modify: `lightfee/apps/evolution.py`
- Modify: `tests/test_offline_analysis.py`
- Modify: `tests/test_evolution.py`

Do not edit live execution modules in this plan.

## Task 1: Expand Journal Analysis Consumers

**Files:**
- Modify: `lightfee/offline/analysis/journal.py`
- Modify: `tests/test_offline_analysis.py`

- [ ] **Step 1: Write failing tests**

Add tests proving `analyze_journal_records()` consumes more than order and PnL basics:

- `recovery.*` counts
- `risk.*` trigger counts
- `scan.no_entry_diagnostics`
- `scan.runtime_gate_blocked`
- `execution.entry_liquidity_blocked`
- `runtime.local_l2_sequence_gap`
- `runtime.local_l2_sync_failed`

- [ ] **Step 2: Run the test**

Run:

```bash
rtk pytest tests/test_offline_analysis.py -q -W error
```

- [ ] **Step 3: Implement analysis parity**

Add explicit dataclasses for the V1-visible summaries rather than returning unstructured nested dicts.

- [ ] **Step 4: Verify**

Run:

```bash
rtk pytest tests/test_offline_analysis.py -q -W error
```

## Task 2: Align Daily Report And Render Outputs

**Files:**
- Modify: `lightfee/offline/reports/daily.py`
- Modify: `lightfee/offline/reports/render.py`
- Modify: `lightfee/apps/report.py`
- Modify: `tests/test_offline_analysis.py`

- [ ] **Step 1: Write failing tests**

Add tests that feed representative journal records and assert report output includes:

- daily PnL
- fee attribution
- venue stats
- recovery counts
- risk counts
- no-entry diagnostics
- local-L2 health/fault summaries

- [ ] **Step 2: Run report tests**

Run:

```bash
rtk pytest tests/test_offline_analysis.py -q -W error
```

- [ ] **Step 3: Implement report parity**

Keep rendering deterministic. Do not include wall-clock-only text that makes tests unstable.

- [ ] **Step 4: Verify**

Run:

```bash
rtk pytest tests/test_offline_analysis.py -q -W error
```

## Task 3: Replace Evolution Stubs With V1 Semantics

**Files:**
- Modify: `lightfee/offline/evolution/cycle.py`
- Modify: `lightfee/offline/evolution/approval.py`
- Modify: `lightfee/offline/evolution/ledger.py`
- Modify: `lightfee/offline/evolution/report.py`
- Modify: `lightfee/apps/evolution.py`
- Modify: `tests/test_evolution.py`

- [ ] **Step 1: Write failing tests**

Add tests for:

- parameter registry has bounded parameter rules
- cycle observation validates sample size, window, and regime fingerprint
- previous action evaluation returns improved/regressed/constrained/inconclusive
- cycle run persists and sorts by generated timestamp
- approval overlay accepts and rejects proposals deterministically

- [ ] **Step 2: Run evolution tests**

Run:

```bash
rtk pytest tests/test_evolution.py -q -W error
```

- [ ] **Step 3: Implement deterministic evolution semantics**

Mirror V1's deterministic cycle behavior first. Do not add network-dependent LLM behavior here.

- [ ] **Step 4: Verify**

Run:

```bash
rtk pytest tests/test_evolution.py -q -W error
```

## Task 4: Align LLM Evolution Disabled/Enabled Contract

**Files:**
- Modify: `lightfee/offline/llm_evolution/report.py`
- Modify: `tests/test_evolution.py`

- [ ] **Step 1: Write failing tests**

Add tests for:

- LLM evolution is disabled by default
- enabling requires explicit environment configuration
- enabled mode records provider/model metadata
- no network call is attempted in disabled mode

- [ ] **Step 2: Run tests**

Run:

```bash
rtk pytest tests/test_evolution.py -q -W error
```

- [ ] **Step 3: Implement disabled/enabled contract**

Keep enabled behavior explicit and testable. If network execution is out of scope, emit a structured pending/disabled report matching V1's operator-facing meaning.

- [ ] **Step 4: Verify**

Run:

```bash
rtk pytest tests/test_evolution.py tests/test_offline_analysis.py -q -W error
```

## Task 5: Validate Offline Workstream Scope

**Files:**
- No code edits

- [ ] **Step 1: Run focused suite**

Run:

```bash
rtk pytest tests/test_offline_analysis.py tests/test_evolution.py -q -W error
```

- [ ] **Step 2: Run GitNexus change detection**

Run:

```bash
gitnexus_detect_changes({scope: "unstaged", repo: "LightFeeV2"})
```

Expected: changed symbols should stay inside offline analysis, reports, evolution, app entrypoints, and tests.

