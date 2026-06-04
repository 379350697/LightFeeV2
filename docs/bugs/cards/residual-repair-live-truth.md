# Bug Card: Residual Repair Live Truth

Purpose: keep the reusable memory for residual repair tasks, pair-gate release,
live-position truth, open-order truth, and dust terminality.

Unified contract and coverage matrix:
[pending-entry-live-truth-contract](../contracts/pending-entry-live-truth-contract.md).
Residual repair rows are part of the same V1 coverage floor as pending-entry
terminality. CL-048 / CL-049 / CL-050 are mapped through that contract, not
tracked as separate active root-fix branches.

## Stable Fingerprints

- `execution.residual_repair_queued`
- `execution.residual_repair_paused`
- `execution.residual_repair_resumed`
- `execution.residual_repair_completed`
- `execution.residual_repair_terminal`
- `recovery.residual_repair_failed`
- `recovery.residual_repairs_complete`
- `residual_repair_live_open_orders_present`
- `residual_repair_live_position_nonzero`
- `residual_repair_live_truth_untrusted`
- Recurrence shape: pending residual repair or pair gate remains after local entry state is gone, or repeated pauses appear even though later exchange truth proves flat.

## Current Effective Rule

Residual repair is normal runtime housekeeping. Fetch trusted live position on
the repair venue and trusted open-order truth on all relevant venues. Repair
tradeable live excess with one reduce-only IOC. If live excess is zero and
trusted open-order truth is clear, complete the task and release the pair gate.
If open-order or live-position truth is unavailable, keep the task fail-closed
with structured evidence. If live excess is official dust, terminalize it and
release the gate with rule-source evidence.

## V1 / Exchange Semantics

- V1 computes residual repair from live exchange truth, not stale local deltas.
- Paused or attempt-exhausted tasks may be resumed only when live position and open-order truth are trusted.
- Open orders matter: zero live excess plus non-empty trusted open orders is not safe to clear.
- If no local open position remains but a trusted repair-venue live position exists, rebuild the repair side from signed live truth before reduce-only IOC.
- Exchange quantity/notional rules decide dust terminality. Do not retry official below-min residuals forever.

## Attempts Ledger

| Date | Attempt | Status | Why |
|---|---|---|---|
| 2026-05-22 | V1 residual contract and excess-only repair | effective locally | One-sided and partial-match residuals repair only live excess instead of full-closing matched positions. |
| 2026-05-25/26 | Exhausted residual already-flat cleanup and open-order truth | effective | LYN/OPG style tasks clear only after trusted live flat plus open-order empty truth. |
| 2026-05-29 | Paused/exhausted live-nonzero resume, side rebuild, dust terminality | effective | PARTI/JCT production closure showed tradeable live residuals resume and complete; untrusted truth stays fail-closed. |
| 2026-05-30 | HMSTR open-order-present pause evidence | fixed, deployed, probe verified | RED/GREEN harness now requires paused-event payload to include aggregate and per-venue open-order truth evidence; `0fd9a74` deployed and affected-symbol probes are flat/no-open-orders. |
| 2026-06-04 | BIOUSDT Bybit duplicate cleanup convergence | deployed/cloud verified | Repeated Bybit `110072` residual cleanup with stale-full/live-nonzero evidence now persists bounded CID attempt evidence, stops at `residual_repair_duplicate_live_nonzero_blocked`, enters fail-closed/risk-only, and retains the residual pair gate. Cloud `68a979b` final verifier/diagnose proved all seven venues flat/no-open-orders. |

## Recurrences

| Date | Symbols / Venues | Commit / Fix | Result | Detail |
|---|---|---|---|---|
| 2026-05-22 | `PROVEUSDT`, `XCNUSDT` residual contract family | local CL-005 | closed locally | [daily/2026-05-22.md#cluster-cl-005-zero-fill-ghost-open-position-v1-parity-drift](../daily/2026-05-22.md#cluster-cl-005-zero-fill-ghost-open-position-v1-parity-drift) |
| 2026-05-25/26 | `LYNUSDT`, `OPGUSDT` exhausted residuals | `30a8ddc` family | closed by harness/probe | [daily/2026-05-26.md#cluster-cl-007-biousdt-exchange-truth-false-green-root-fix](../daily/2026-05-26.md#cluster-cl-007-biousdt-exchange-truth-false-green-root-fix) |
| 2026-05-29 | `PARTIUSDT` OKX/Binance | `9cdb9df` | closed by cloud probe | [daily/2026-05-29.md#cluster-cl-015-jctusdt-partiusdt-v1-terminality-regression](../daily/2026-05-29.md#cluster-cl-015-jctusdt-partiusdt-v1-terminality-regression) |
| 2026-05-30 | `HMSTRUSDT` Bybit/OKX, `RAVEUSDT` Bybit/OKX, post-fix `HOMEUSDT`/`POWERUSDT` residual completions | `0fd9a74` | fixed, deployed, final probes flat/no-open-orders | [daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission](../daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission) |
| 2026-05-31 | `HOMEUSDT` Binance residual repair before passive close | existing residual-repair live-truth path | completed; final local state and read-only exchange truth flat/no-open-orders | [daily/2026-05-31.md#cluster-cl-025-post-ae4bd9c-passive-close-maker-leg-live-flat-precheck](../daily/2026-05-31.md#cluster-cl-025-post-ae4bd9c-passive-close-maker-leg-live-flat-precheck) |
| 2026-06-04 | `BIOUSDT` Bybit live mismatch cleanup | `68a979b` | deployed/cloud verified: `PE-03` keeps live maker position truth from becoming false-flat zero-fill; `RC-08` makes repeated duplicate/live-nonzero cleanup fail-closed with explicit blocker evidence and non-reused CIDs; production verifier/diagnose ended flat/no-open-orders across all seven venues. | [daily/2026-06-04.md#cluster-cl-050-biousdt-live-position-zero-fill-terminality](../daily/2026-06-04.md#cluster-cl-050-biousdt-live-position-zero-fill-terminality) |

## Regression Harness

- `tests/test_live_entry_hedge_root_fix.py::TestResidualRepairExecutionV1Parity`
- `tests/live_harness/test_residual_repair_incident_replay.py`
- `tests/live_harness/test_residual_repair_incident_replay.py::test_hmstr_open_orders_present_pause_records_truth_evidence`
- `tests/live_harness/test_20260529_jct_parti_regressions.py`
- `tests/live_harness/test_20260530_post_deploy_watch.py` when the 2026-05-30 fixture is added.
- `scripts/diagnose_live.py --json --symbol <symbol> --venues <repair,counter>`

## Next Recurrence Checklist

1. Inspect `pending_residual_repairs`, `live_recovery_reduce_only_pairs`, local open positions, and `last_error`.
2. Run explicit read-only `diagnose_live.py --symbol <symbol> --venues <repair,counter>` with the same EnvironmentFile used by systemd.
3. Confirm live position truth and open-order truth are both available. Missing truth is fail-closed, not green.
4. If live excess is tradeable, expect exactly one reduce-only IOC repair and then completion or backoff after submit failure.
5. If live excess is zero and open orders are empty, expect `execution.residual_repair_completed(result=already_flat)` and pair-gate release.
6. If `residual_repair_live_open_orders_present` recurs, preserve the raw open-order count/source in the fixture. Without that evidence, do not claim the pause was exchange-real or stale.
7. Closure requires fixture replay plus credentialed flat/no-open-orders probe.
