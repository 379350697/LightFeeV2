# V1 Semantic Parity Blockers Fix Prompt

Use this prompt with a single implementation agent to fix the remaining final-acceptance blockers in LightFeeV2.

Repository:
- V2: `/media/wl/新加卷/codex/LightFeeV2`
- V1 reference: `/media/wl/新加卷/codex/LightFee`

Primary context:
- `/media/wl/新加卷/codex/LightFeeV2/docs/superpowers/plans/2026-05-12-v1-semantic-parity-master-and-subplans.md`
- `/media/wl/新加卷/codex/LightFeeV2/docs/parity/v1_semantic_contract_catalog.md`
- `/media/wl/新加卷/codex/LightFeeV2/docs/parity/approved_deviations.md`

## Prompt

```text
You are fixing the remaining final-acceptance blockers for LightFeeV2 V1 semantic parity.

Working directory:
/media/wl/新加卷/codex/LightFeeV2

V1 reference repository:
/media/wl/新加卷/codex/LightFee

Core rule:
Do NOT do code-level replication. Preserve V1 business semantics: observable inputs, state transitions, journal records, recovery behavior, replay results, reports, and operator-visible outputs. If V1 implementation is awkward, use a cleaner V2-native implementation that preserves the same semantic outcome.

Git/worktree safety:
- Do not revert or overwrite unrelated user changes.
- Use `rtk` before shell commands.
- Prefer `rg` / `rg --files`.
- Use `apply_patch` for manual edits.

Testing rule:
- Write or adjust focused tests first if coverage is missing.
- Run the exact focused commands listed below.
- Do not claim completion without fresh green test output.

Your scope is ONLY these blocker areas:
1. Incident analysis does not recognize runtime tick-error events.
2. Venue shutdown contract test is unstable in full-suite execution because it assumes a current asyncio event loop.
3. Maker-event non-parity sidecar fallback is not explicit opt-in; it is still implicitly selected by `local_l2_enabled=False`.

You must inspect these files first:
- lightfee/offline/analysis/incident.py
- lightfee/engine/runtime.py
- lightfee/config/schema.py
- lightfee/config/compatibility.py
- lightfee/config/validation.py
- tests/test_offline_analysis.py
- tests/venues/test_v1_capability_matrix.py
- tests/config/test_v1_config_semantics.py
- tests/sidecar/test_opportunity_input_semantics.py
- tests/engine/test_runtime_lane_scheduling.py
- tests/engine/test_startup_activation_semantics.py
- docs/parity/v1_semantic_contract_catalog.md
- docs/parity/approved_deviations.md

Allowed file changes:
- lightfee/offline/analysis/incident.py
- lightfee/engine/runtime.py
- lightfee/config/schema.py
- lightfee/config/compatibility.py
- lightfee/config/validation.py
- tests/test_offline_analysis.py
- tests/venues/test_v1_capability_matrix.py
- tests/config/test_v1_config_semantics.py
- tests/sidecar/test_opportunity_input_semantics.py
- tests/engine/test_runtime_lane_scheduling.py
- tests/engine/test_startup_activation_semantics.py
- If truly necessary: docs/parity/approved_deviations.md

Do NOT change:
- venue capability business semantics outside the shutdown-test fix
- persistence/replay code
- execution/risk business logic unrelated to the three blockers
- broad runtime orchestration outside the non-parity opt-in fix

## Blocker 1: Incident Analysis Compatibility

Problem:
- `build_incident_report()` recognizes `runtime.error` but not the runtime events actually emitted now:
  - `runtime.tick_error`
  - `runtime.active_tick_error`
  - `runtime.maker_event_tick_error`
  - `runtime.passive_close_tick_error`
- Final acceptance fails because `tests/test_offline_analysis.py::TestIncidentReport::test_errors_produce_incident` expects these to produce an incident report.

Required behavior:
- Runtime tick-related error events must be recognized as runtime incidents.
- Existing handling for `runtime.error`, `runtime.fail_closed*`, `risk.*`, and `recovery.blocked` must remain intact.
- Summary and recommendations must still preserve V1-style runtime error semantics.

Preferred fix:
- Add compatible classification for the tick-error event family rather than renaming the runtime event stream.

## Blocker 2: Venue Shutdown Test Stability

Problem:
- `tests/venues/test_v1_capability_matrix.py::TestAdapterContractCompleteness::test_all_adapters_respond_to_shutdown` may fail in full-suite execution with:
  `RuntimeError: There is no current event loop in thread ...`
- The same test passes in isolation, so this is a stability / invocation issue.

Required behavior:
- Adapter shutdown contract test must pass both in isolation and in larger suite runs.
- Do not weaken the test by skipping it.

Preferred fix:
- Fix the test invocation style using `asyncio.run(...)` or explicit loop creation/teardown per adapter.
- Only touch production adapter shutdown code if you confirm an actual contract defect.

## Blocker 3: Non-Parity Fallback Must Be Explicit

Problem:
- `lightfee/engine/runtime.py` still routes maker-event logic into the non-parity sidecar-mid fallback whenever `local_l2_enabled` is false.
- This is semantically wrong because tests and contract docs say non-parity fallback must be explicit opt-in, not an implicit side effect of disabling local L2.

Required behavior:
- The non-parity sidecar maker-event path must only run when explicitly configured as a non-parity mode.
- `local_l2_enabled=False` alone must not activate it.
- Parity mode must keep the "no matching local-L2 events => no sidecar fallback" behavior.

Preferred fix:
- Drive this from an explicit runtime mode check, for example `runtime.opportunity_input_mode == "non_parity"` or a similarly explicit config rule.
- Keep comments, journal behavior, and implementation aligned.

## Required commands

Run these focused commands during the fix:
- `rtk pytest tests/test_offline_analysis.py::TestIncidentReport::test_errors_produce_incident -vv`
- `rtk pytest tests/test_offline_analysis.py -q`
- `rtk pytest tests/venues/test_v1_capability_matrix.py::TestAdapterContractCompleteness::test_all_adapters_respond_to_shutdown -vv`
- `rtk pytest tests/venues/test_v1_capability_matrix.py -q`
- `rtk pytest tests/config/test_v1_config_semantics.py -q`
- `rtk pytest tests/sidecar/test_opportunity_input_semantics.py -q`
- `rtk pytest tests/engine/test_runtime_lane_scheduling.py -q`
- `rtk pytest tests/engine/test_startup_activation_semantics.py -q`

Before finishing, run this combined verification:
- `rtk pytest tests/test_offline_analysis.py tests/venues/test_v1_capability_matrix.py tests/config/test_v1_config_semantics.py tests/sidecar/test_opportunity_input_semantics.py tests/engine/test_runtime_lane_scheduling.py tests/engine/test_startup_activation_semantics.py -q`

If that passes, run this broader confidence check too:
- `rtk pytest tests/parity tests/offline tests/venues tests/config tests/sidecar tests/engine -q`

## Final response format

Return:
1. Files changed.
2. For blocker 1: which runtime error events are now recognized by incident analysis.
3. For blocker 2: whether the failure was a test issue or production contract issue.
4. For blocker 3: exactly what condition now enables the non-parity fallback.
5. Commands run and results.
6. Any deviation entry added or updated.
7. Any remaining risk.
```
