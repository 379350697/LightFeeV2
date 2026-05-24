# V1 Semantic Alignment Gap Triage Parallel Execution Prompts

Use this file to dispatch the remaining semantic-alignment work to multiple implementation agents.

Repository:
- V2: `/media/wl/新加卷/codex/LightFeeV2`
- V1 reference: `/media/wl/新加卷/codex/LightFee`

Read-first documents:
- `/media/wl/新加卷/codex/LightFeeV2/docs/superpowers/specs/2026-05-12-v1-semantic-alignment-gap-triage-design.md`
- `/media/wl/新加卷/codex/LightFeeV2/docs/superpowers/plans/2026-05-12-v1-semantic-alignment-gap-triage-implementation-plan.md`

## Recommended Split

Use **3 implementation agents in parallel** plus **1 integration/review agent** after they finish.

Why this split is the best fit:

- `schema.py`, `validation.py`, `compatibility.py`, `runtime.py`, and sidecar/provider files form one tight config/provider surface and should stay with one owner.
- `entry.py`, `entry_sync.py`, `exit.py`, `risk_actions.py`, and journal/review wiring form one tight engine/observability surface and should stay with one owner.
- `sqlite_store.py`, offline analysis/reports, state/recovery, and fidelity tests form one persistence/offline surface and should stay with one owner.
- Splitting more finely would create avoidable conflicts on `schema.py`, `runtime.py`, `entry.py`, and persistence modules.

## Shared Rules

Paste this at the top of every worker prompt.

```text
You are implementing part of the remaining V1 semantic-alignment gap closure for LightFeeV2.

Working directory:
/media/wl/新加卷/codex/LightFeeV2

V1 reference repository:
/media/wl/新加卷/codex/LightFee

Read first:
1. docs/superpowers/specs/2026-05-12-v1-semantic-alignment-gap-triage-design.md
2. docs/superpowers/plans/2026-05-12-v1-semantic-alignment-gap-triage-implementation-plan.md
3. docs/parity/v1_semantic_contract_catalog.md
4. docs/parity/approved_deviations.md

Core rule:
Do NOT chase code-level replication. Preserve V1 business semantics where they are still active. Do NOT reintroduce retired Chillybot behavior. If V1 and V2 differ only by implementation shape, keep the cleaner V2-native shape.

Worktree safety:
- Do not revert unrelated user changes.
- Stay strictly inside your assigned write scope.
- If you discover a required change outside your write scope, stop and report the exact dependency.
- Use `rtk` before shell commands.
- Prefer `rg` / `rg --files`.
- Use `apply_patch` for manual edits.

Testing rule:
- Write focused failing tests first where meaningful.
- Run the exact focused commands listed in your prompt.
- Do not claim a semantic gap is closed without fresh green test output.

Final response must include:
1. Files changed.
2. Which V1 semantics were proven active vs filtered as retired.
3. Tests added/updated.
4. Commands run and results.
5. Any deviations added or updated.
6. Any blocked dependencies outside your scope.
```

---

## Prompt A: Config, Provider Depth, and Runtime Mode Surface

```text
Use the shared rules above.

Your role:
Worker A owns the config/provider/runtime-mode surface.

You own:
- lightfee/config/**
- lightfee/sidecar/**
- lightfee/engine/runtime.py
- lightfee/marketdata/**
- tests/config/**
- tests/sidecar/**
- tests/engine/** only where the test is directly about runtime mode/provider semantics

You must not edit:
- lightfee/engine/entry.py
- lightfee/engine/entry_sync.py
- lightfee/engine/exit.py
- lightfee/engine/risk_actions.py
- lightfee/persistence/**
- lightfee/offline/**
- lightfee/engine/state.py
- lightfee/engine/recovery.py

Goal:
Close the remaining active config and opportunity-input/provider depth gaps, and explicitly keep retired Chillybot behavior filtered out.

What to implement:
1. Add and wire only the V1 config knobs that still have real business meaning on this surface:
   - local_l2_global_max_books
   - entry_min_size_round_up_whitelist
   - any provider-depth or mode fields needed for direct_market / coarse_sidecar / sidecar_scan / direct_market_enriched-equivalent behavior
2. Do NOT reintroduce:
   - sidecar_chillybot_mode
   - chillybot_api_base
   - chillybot_timeout_ms
   - any Chillybot provider branch
3. Evaluate and implement the remaining provider-depth semantics:
   - provenance / source-mode propagation
   - acquisition-mode depth where it still matters
   - a V2-native equivalent of direct_market_enriched if V1 still uses it for real business diagnostics
4. Keep non-parity fallback explicit opt-in only.

What to prove in your summary:
- Which V1 config/provider items were retired historical residue and intentionally not implemented
- Which active V1 semantics you actually wired into runtime/provider behavior

Primary V1 anchors to inspect:
- src/runtime_state/config.rs
- src/opportunity_input/**
- src/main.rs around opportunity provider construction

Primary V2 anchors to inspect:
- lightfee/config/schema.py
- lightfee/config/validation.py
- lightfee/config/compatibility.py
- lightfee/sidecar/**
- lightfee/engine/runtime.py
- lightfee/marketdata/**

Focused tests to add or update:
- tests/config/test_v1_real_config_gap_semantics.py
- tests/sidecar/test_provider_depth_semantics.py
- tests/config/test_v1_config_semantics.py
- tests/sidecar/test_opportunity_input_semantics.py

Required commands:
- rtk pytest tests/config/test_v1_real_config_gap_semantics.py -v
- rtk pytest tests/sidecar/test_provider_depth_semantics.py -v
- rtk pytest tests/config tests/sidecar -q

Nice-to-have verification:
- rtk pytest tests/engine/test_runtime_lane_scheduling.py -q
```

---

## Prompt B: Engine Semantics, Review Observability, and Active Entry Controls

```text
Use the shared rules above.

Your role:
Worker B owns engine-side active semantics: entry controls, repost/cooldown behavior, and review observability chain.

You own:
- lightfee/engine/entry.py
- lightfee/engine/entry_sync.py
- lightfee/engine/exit.py
- lightfee/engine/risk_actions.py
- lightfee/persistence/journal.py
- tests/engine/**
- tests/persistence/** where review/journal behavior is involved

You must not edit:
- lightfee/config/**
- lightfee/sidecar/**
- lightfee/engine/runtime.py
- lightfee/persistence/sqlite_store.py
- lightfee/offline/**
- lightfee/engine/state.py
- lightfee/engine/recovery.py

Goal:
Implement the engine-side semantics behind still-active V1 controls and restore the end-to-end review observability chain.

What to implement:
1. Active entry/passive-maker controls that are still real V1 semantics:
   - maker_entry_max_reposts
   - pending_entry_zero_fill_terminal_cooldown_ms
2. Review observability semantics:
   - review_observability_enabled behavior
   - review-id assignment
   - propagation into journal and relevant engine state transitions
3. Do not add config-only stubs. If a knob exists, it must alter engine behavior.

What to prove in your summary:
- Which knobs changed actual entry/repost/terminal behavior
- Whether review_id is now generated and persisted only under the intended V1 conditions

Primary V1 anchors to inspect:
- src/execution_core/entry_sync.rs
- src/engine/entry.rs
- src/engine/exit.rs
- src/engine/risk.rs
- src/execution_core/engine.rs

Primary V2 anchors to inspect:
- lightfee/engine/entry.py
- lightfee/engine/entry_sync.py
- lightfee/engine/exit.py
- lightfee/engine/risk_actions.py
- lightfee/persistence/journal.py

Focused tests to add or update:
- tests/engine/test_v1_real_config_gap_semantics.py
- tests/engine/test_review_observability_semantics.py
- tests/persistence/test_review_observability_semantics.py

Required commands:
- rtk pytest tests/engine/test_v1_real_config_gap_semantics.py -v
- rtk pytest tests/engine/test_review_observability_semantics.py -v
- rtk pytest tests/persistence/test_review_observability_semantics.py -v
- rtk pytest tests/engine -q
```

---

## Prompt C: Paper Outcome Tracking, State Fidelity, and Offline Semantics

```text
Use the shared rules above.

Your role:
Worker C owns persistence/offline/state-fidelity work.

You own:
- lightfee/persistence/sqlite_store.py
- lightfee/offline/**
- lightfee/engine/state.py
- lightfee/engine/recovery.py
- tests/offline/**
- tests/persistence/**
- tests/parity/** only where state-fidelity verification is involved

You must not edit:
- lightfee/config/**
- lightfee/sidecar/**
- lightfee/engine/runtime.py
- lightfee/engine/entry.py
- lightfee/engine/entry_sync.py
- lightfee/engine/exit.py
- lightfee/engine/risk_actions.py
- lightfee/persistence/journal.py

Goal:
Implement the remaining paper-outcome semantics and prove any still-missing state fidelity gaps before adding more state surface.

What to implement:
1. Paper outcome tracking behavior:
   - paper_outcome_tracking_enabled
   - paper_outcome_finalist_limit
   - paper_outcome_markout_secs
   - paper_outcome_settlement_grace_secs
2. State fidelity verification:
   - prove whether current V2 state/recovery/replay still misses any active V1 semantics
   - only add state/recovery fields or replay behavior if a focused test proves a real loss
3. Do not start a broad state rewrite.

What to prove in your summary:
- Which paper-outcome behaviors are now actually enforced
- Which state fields were already sufficient
- Which state/recovery semantics, if any, needed real code changes

Primary V1 anchors to inspect:
- src/execution_core/engine.rs
- src/runtime_state/sqlite_store.rs
- crates/lightfee-engine/src/lib.rs
- src/engine/state.rs
- src/analysis.rs
- src/bin/incident_report.rs

Primary V2 anchors to inspect:
- lightfee/persistence/sqlite_store.py
- lightfee/offline/**
- lightfee/engine/state.py
- lightfee/engine/recovery.py

Focused tests to add or update:
- tests/offline/test_paper_outcome_tracking_semantics.py
- tests/parity/test_remaining_state_fidelity_gaps.py
- tests/persistence/test_v1_state_snapshot_semantics.py
- tests/offline/replay/test_replay_semantic_equivalence.py

Required commands:
- rtk pytest tests/offline/test_paper_outcome_tracking_semantics.py -v
- rtk pytest tests/parity/test_remaining_state_fidelity_gaps.py -v
- rtk pytest tests/persistence/test_v1_state_snapshot_semantics.py -v
- rtk pytest tests/offline tests/persistence -q
```

---

## Prompt D: Integration and Final Gap Verification

Dispatch this after Workers A, B, and C finish.

```text
Use the shared rules above.

Your role:
Integration / verification agent for the remaining semantic-alignment gap closure.

You may edit:
- any conflicting imports, test fixtures, or glue code required to integrate Workers A/B/C
- docs/parity/approved_deviations.md if a truly intentional retained deviation must be documented

You should not redesign subsystem logic unless integration makes it unavoidable.

Tasks:
1. Review the diffs from Workers A/B/C and confirm they stayed inside scope.
2. Resolve integration issues.
3. Run targeted verification:
   - rtk pytest tests/config tests/sidecar tests/offline tests/persistence tests/parity tests/engine -q
4. Run full verification:
   - rtk pytest -q
5. Summarize:
   - which V1 gaps are now closed
   - which items were intentionally filtered as retired
   - whether any new deviation entry was necessary

Final response must lead with one of:
- PASS
- PASS with approved deviations
- FAIL with remaining blockers
```

---

## Minimal Dispatch Messages

You can hand work to other agents with these short messages:

### Worker A

```text
请打开并执行这个提示词文件里的「Shared Rules」和「Prompt A: Config, Provider Depth, and Runtime Mode Surface」：

/media/wl/新加卷/codex/LightFeeV2/docs/superpowers/prompts/2026-05-12-v1-semantic-alignment-gap-triage-parallel-prompts.md
```

### Worker B

```text
请打开并执行这个提示词文件里的「Shared Rules」和「Prompt B: Engine Semantics, Review Observability, and Active Entry Controls」：

/media/wl/新加卷/codex/LightFeeV2/docs/superpowers/prompts/2026-05-12-v1-semantic-alignment-gap-triage-parallel-prompts.md
```

### Worker C

```text
请打开并执行这个提示词文件里的「Shared Rules」和「Prompt C: Paper Outcome Tracking, State Fidelity, and Offline Semantics」：

/media/wl/新加卷/codex/LightFeeV2/docs/superpowers/prompts/2026-05-12-v1-semantic-alignment-gap-triage-parallel-prompts.md
```

### Integration

```text
请打开并执行这个提示词文件里的「Shared Rules」和「Prompt D: Integration and Final Gap Verification」：

/media/wl/新加卷/codex/LightFeeV2/docs/superpowers/prompts/2026-05-12-v1-semantic-alignment-gap-triage-parallel-prompts.md
```
