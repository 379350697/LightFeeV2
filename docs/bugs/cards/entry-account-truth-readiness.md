# Bug Card: Entry Account-Truth Readiness

Purpose: prevent a safe account-truth prerequisite from becoming a global
entry-liveness outage, while retaining V1 live-truth and fail-closed semantics.

## Stable Fingerprints

- `runtime.entry_account_truth_pending`
- `runtime.entry_account_truth_not_ready_before_dispatch`
- `runtime.entry_account_truth_timeout`
- `runtime.entry_account_truth_incomplete`
- Candidate frontier is nonempty, but `selected_candidate_count=0` and
  `dispatched_candidate_count=0` because account truth is not ready.
- `live_recovery_rest_probe_timeout_ms` is lower than a healthy all-venue
  position-plus-open-order sweep.
- `entry_account_truth_per_venue_timeout_ms` is missing, non-positive, or
  coupled back to recovery admission timing. The prior
  `entry_account_truth_probe_timeout_ms` spelling is compatibility-only.
- A non-participating venue truth failure prevents a candidate whose two
  execution venues have fresh complete private truth.

## Current Effective Rule

No order may be submitted unless both selected execution venues have fresh,
complete position and open-order truth. A missing, stale, or failed selected
leg is a candidate-specific no-submit result, not an empty/flat inference.

Full-account truth remains mandatory for startup recovery and for any detected
live artifact. A real position, non-reduce-only order, pending owner, residual,
or passive-close artifact must continue to block or enter recovery under the
existing V1 decision core. The rule is not permission to ignore a venue; it is
a scope boundary between global recovery and ordinary candidate admission.

For normal entry, retain per-venue private truth receipts with completion time,
duration, error, and last-good timestamp. Probe the two selected venues
concurrently with independent deadlines. A non-participating venue with no
known live artifact may expose a health/evidence warning, but may not make
every unrelated candidate wait for a full all-venue REST sweep. A completed
global sweep can be reused as supporting evidence, never treated as the sole
freshness path if it is older than selected-leg requirements.

The runtime must journal which required venue is missing and why. A generic
`pending` state is insufficient when its actual cause is deadline exhaustion.

## V1 / Exchange Semantics

- V1 recovery prioritizes fresh private state and probes missing venues with
  concurrent, per-venue bounded work; it does not serialize all venue
  position-plus-order reads inside one normal-entry global deadline.
- V1 treats exchange position and open-order truth as stronger than local
  recovery state. V2 must retain this ordering for target legs and all detected
  recovery artifacts.
- Exchange APIs can be slow, rate-limited, or temporarily incomplete. Timeout
  is evidence uncertainty, never flat evidence and never authority to submit
  an order on the affected leg.

## Attempts Ledger

| Date | Attempt | Status | Why |
|---|---|---|---|
| 2026-07-25 | Reuse full recovery-ledger account truth before each dispatch under `live_recovery_rest_probe_timeout_ms=2000` | ineffective in production | A healthy read of all seven venues completed in about 6.367 seconds, so the aggregate deadline repeatedly returned unready and starved every candidate before order submission. |
| 2026-07-25 | Target-only entry account truth with `entry_account_truth_per_venue_timeout_ms`, venue singleflight/short negative no-submit cache, and sanitized block summaries | local verified; production lifecycle pending | Focused tests prove a background all-venue sweep can remain pending while a healthy target pair proceeds, a target timeout emits no-submit evidence, and a detected target-leg artifact enters global recovery. Full startup/recovery truth remains complete/fail-closed and is no longer cut by the ordinary-entry 2-second aggregate deadline. |

## Recurrences

| Date | Symbols / Venues | Commit / Fix | Result | Detail |
|---|---|---|---|---|
| 2026-07-25 | All configured venues; current candidates | local fix pending deploy | local verified / current cloud watch | [CL-159](../daily/2026-07-25.md#issue-cl-159-a-global-account-truth-deadline-starves-every-entry) found no abnormal exposure; local tests now prove target-only truth, candidate progress during a pending global sweep, per-venue singleflight, and no-submit target failure, but production has not completed a controlled open-to-close lifecycle. |

## Required Regression Harness

1. Seven venue receipts together exceed two seconds while selected two venues
   complete within their own budgets: normal candidate preparation proceeds.
2. Either selected venue lacks fresh positions or open-order truth: no submit,
   explicit venue-scoped incomplete/timeout event.
3. A slow non-participating venue does not suppress a target pair with fresh
   two-leg truth and no recovery artifact.
4. A hidden unpaired position or non-reduce-only order from any venue still
   enters global recovery and blocks new risk.
5. Restart/recovery remains fail-closed until its required all-account truth is
   complete or an explicit operator decision resolves the evidence gap.

## Next Recurrence Checklist

1. Read `candidate_count`, `selected_candidate_count`,
   `dispatched_candidate_count`, and `no_entry_reason` from current state.
2. Measure a read-only all-venue account sweep and record per-venue position,
   open-order, and total duration without logging credentials or raw account
   payloads.
3. Compare the result to `live_recovery_rest_probe_timeout_ms` and selected-leg
   receipt ages.
4. Confirm that any proposed liveness fix still refuses submit when either
   target venue's position or open-order evidence is missing.
5. Do not call a fix production-verified until a controlled, authorized real
   open-to-close lifecycle has passed and post-close exchange truth is flat.
