# Execution Prompt: V1 Pending Entry Source Port

Work in `/Users/wl/projects/LightFeeV2`. Execute these two docs exactly:

- Spec: `docs/superpowers/specs/2026-06-05-v1-pending-entry-source-port-design.md`
- Plan: `docs/superpowers/plans/2026-06-05-v1-pending-entry-source-port-implementation-plan.md`

Background: current pending-entry passive opening work mixed valid V1 field/function ports with bug-driven runtime patches. The user wants source-level copying from V1, not a V1-looking redesign and not more "patch the next missing segment" fixes.

V1 authority:

- `/Users/wl/projects/LightFee/src/execution_core/entry_sync.rs`
- `/Users/wl/projects/LightFee/crates/lightfee-engine/src/lib.rs`

Rules:

- Start with Plan Task 0-2. Do not edit Python code until the source-port matrix exists and every current hunk is classified as `keep`, `replace`, `revert`, or `defer`.
- Preserve user/current worktree changes. Do not use `git reset` or broad revert commands.
- Implement by V1 source function/source block. Direct source copy wins; only unavoidable Python/runtime boundaries may be `boundary-adapted`.
- Put lifecycle semantics in `lightfee/engine/pending_entry_lifecycle.py`; keep `runtime.py` as IO/orchestration.
- Add RED parity tests in `tests/engine/test_v1_pending_entry_lifecycle_parity.py` before each behavior change.
- Do not submit/cancel live orders, mutate production state, deploy, or claim production acceptance.

Finish only when the matrix has no `missing`, `pending-audit`, or `replace-current` rows; bug-driven runtime helpers are removed/delegated; focused parity, incident regressions, broad regression, `compileall`, `git diff --check`, and `npx gitnexus detect-changes --repo LightFeeV2` have been run and reported with exact results.
