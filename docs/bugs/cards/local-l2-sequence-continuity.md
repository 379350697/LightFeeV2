# Bug Card: Local-L2 Sequence Continuity

Purpose: keep the reusable memory for local order-book rebuilds, sequence gaps,
and official exchange continuity evidence.

## Stable Fingerprints

- `runtime.local_l2_sequence_gap_rebuild`
- `runtime.local_l2_snapshot_error` with `category=buffered_replay_failed`
- Binance/Aster payload fields: `raw_U`, `raw_u`, `raw_pu`, `expected_previous_sequence`, `snapshot_last_update_id`
- OKX payload fields: `seqId`, `prevSeqId`, checksum mismatch evidence
- Recurrence shape: high rebuild volume, entry-local-L2 blockers, or insufficient evidence classification hiding whether the rebuild is official exchange behavior or V2 drift.

## Current Effective Rule

Never relax official continuity to make books ready. A local book may be trusted only when the venue-documented sequence bridge is intact. A rebuild is classified as official behavior only when payload evidence proves it.

Classify as official when:

- Binance `pu` does not match previous `u` after snapshot bridge evidence.
- Aster `pu` does not match previous `u` **and** its `U..u` range does not
  cover the next expected local sequence.
- Binance/Aster update range proves a real skipped update.
- REST snapshot boundary is not bridged by buffered updates.
- For a `REST_SNAPSHOT_BUFFERED_REPLAY` venue, the registered bridge stream
  must be ready to receive before REST starts.  A direct or periodic snapshot
  must defer while that stream is not ready, and a response spanning stream
  registration or a reconnect generation must be discarded and retried.
- OKX `prevSeqId/seqId` proves previous-link mismatch or sequence reset.
- OKX checksum mismatch is present while checksum is still meaningful for the wire channel.

Last-good / quote fallback rules:

- `last_good_sidecar` can keep coarse shortlist diagnostics alive, but live entry still requires targeted revalidate or an explicit block before dispatch.
- Quote lease and ws-bbo blockers must distinguish `budget_exhausted`, `waiting_for_subscription`, `stale_quote`, and `invalid_quote`; a blocked top candidate must not turn the whole scan into an unowned incident if another candidate satisfies admission, L2, and quote gates.
- Whole-snapshot stale quote volume is health evidence only. Only
  admission-filtered selected/tracked/recovery-owned candidate-leg stale quotes
  may emit entry blockers such as `runtime.order_quote_stale_skipped` or
  `runtime.quote_stale`.
- Diagnostic payloads must include candidate universe/counts, selected/tracked pair, blocker-family counts, quote age, fallback source, and targeted revalidate outcome. These are evidence fields only; they do not relax Local-L2 sequence continuity.
- New-entry admission venue downgrades run before Local-L2/quote tracking, so a
  degraded venue should not consume hot Local-L2 or ws-bbo budget for fresh
  entry candidates during the cooldown TTL.

## V1 / Exchange Semantics

- Binance: V1 and exchange docs require `pu == previous u`; mismatch means
  reinitialize the local book.
- Aster: V1 accepts a stale `pu` only when `U <= previous_u + 1 <= u`; all
  other previous-link mismatches remain rebuilds.  This exception is limited
  to the shared Aster policy and must not be generalized to Binance or Gate.
- Aster public Local-L2 semantics remain Binance-compatible FAPI. This does not
  imply Aster private account/order APIs are Binance-HMAC compatible; private
  Aster V3 account/order/open-order paths use a separate Web3 signer client.
- OKX: older V1/V2 parity included checksum support, but OKX later documented JSON checksum deprecation and sequence validation using `seqId/prevSeqId`. Use exchange docs for OKX classification instead of forcing Binance/Aster `pu` wording.
- Do not accept broken deltas as a "fix"; that is semantic drift and can create a false-ready book.

## Attempts Ledger

| Date | Attempt | Status | Why |
|---|---|---|---|
| 2026-05-18 | Preserve fault reason through rebuild/bootstrapping | effective | Fixed V2 state-machine evidence loss for readiness arming. |
| 2026-05-29 | Shared strict Binance/Aster classifier | effective for Binance/Aster | Closed Aster evidence gap and prevented over-classifying field-complete non-breaks. |
| 2026-05-29 | OKX `seqId/prevSeqId` and checksum classification | effective for evidence classification | Cloud post-deploy Local-L2 classification had `L2_INSUFFICIENT=[]`. |
| 2026-05-30 | Post-`bbcd7b9` high rebuild/snapshot watch | deployed/probe verified | Current evidence remains official continuity/rebuild behavior; no Local-L2 readiness relaxation is allowed without new docs or V1 proof. |
| 2026-06-07 | Post-`21e5d44` fallback/quote evidence closure | local implementation pending deploy | Added RED/GREEN contracts that `runtime.live_scan_revalidate_required` marks `fallback_source=last_good_sidecar` and `targeted_revalidate_required=true`; quote readiness evidence carries blocker families and quote ages; static recovery-probe skip evidence is bounded summary rather than noisy per-universe spam. No sequence continuity relaxation was made. |
| 2026-06-07 | Entry admission prefilter and no-entry stage breakdown | local implementation pending deploy | `runtime.entry_admission_venue_degraded` now summarizes venue-scope admission pruning before Local-L2/quote tracking. `scan.no_entry_diagnostics` includes stage counts for candidate universe, unsupported symbol, entry-admission venue downgrade, snapshot/quote freshness, liquidity, and entry selection. |
| 2026-06-08 | Structural open-interest degraded evidence | local RED/GREEN, deploy pending | CL-052 adds endpoint, source, floor/current value, fallback source, and targeted revalidate scope to `perp_open_interest_structural` payloads. This is diagnostic evidence only and does not relax Local-L2 sequence continuity or entry dispatch truth requirements. |
| 2026-06-08 | Whole-snapshot stale quote noise split | local RED/GREEN, deploy pending | CL-056 limits stale quote entry blockers to admission-filtered candidate-leg quote keys and emits non-candidate or admission-blocked stale quote volume as rate-limited `runtime.order_quote_stale_health_summary` with `blocking=false`. |
| 2026-08-14 | Decision-time/Aster-overlap/primary-owner repair | locally validated; cloud pending | CL-105 restores V1's Aster covered-range acceptance, uses decision-time for Local-L2 readiness, and prevents normal rank churn from resetting an existing primary's hold/session owner. |
| 2026-08-15 | WS-ready REST bridge boundary | locally validated; cloud pending | CL-110 makes the per-symbol pre-snapshot WS receiver readiness explicit, rejects snapshots across registration/reconnect boundaries, and wakes/cleans waiters on worker stop or prune.  It changes no sequence acceptance rule, strategy threshold, or unrelated venue policy. |

## Recurrences

| Date | Symbols / Venues | Commit / Fix | Result | Detail |
|---|---|---|---|---|
| 2026-05-29 | Binance/Aster `LABUSDT`, Binance `IOUSDT` | `7ee4c72` | closed for Binance/Aster | [daily/2026-05-29.md#cluster-cl-016-post-deploy-official-local-l2-and-terminal-order-reject-watch](../daily/2026-05-29.md#cluster-cl-016-post-deploy-official-local-l2-and-terminal-order-reject-watch) |
| 2026-05-29 | OKX `LABUSDT` evidence gap | `6987fc8` | closed for classification | [daily/2026-05-29.md#cluster-cl-017-post-deploy-pending-hedge-admission-and-okx-l2-evidence](../daily/2026-05-29.md#cluster-cl-017-post-deploy-pending-hedge-admission-and-okx-l2-evidence) |
| 2026-05-30 | Binance `LABUSDT`, `ALLOUSDT`, `IOUSDT`, `HEMIUSDT`; Aster `HMSTRUSDT`; post-fix `HEIUSDT` snapshot errors | `0fd9a74`; no Local-L2 code change selected | no dirty trading state; final affected-symbol probes flat/no-open-orders | [daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission](../daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission) |
| 2026-05-31 | current run high-volume rebuild/snapshot family across active candidates | no Local-L2 semantic change selected | production state and exchange truth stayed flat/no-open-orders; evidence is insufficient to relax official continuity | [daily/2026-05-31.md#cluster-cl-025-post-ae4bd9c-passive-close-maker-leg-live-flat-precheck](../daily/2026-05-31.md#cluster-cl-025-post-ae4bd9c-passive-close-maker-leg-live-flat-precheck) |
| 2026-06-08 | production issue 11 snapshot/OI degraded evidence | working tree | local RED/GREEN and full pytest green; deploy pending | [daily/2026-06-08.md#cluster-cl-052-production-issues-3-11-root-closure-evidence-hardening](../daily/2026-06-08.md#cluster-cl-052-production-issues-3-11-root-closure-evidence-hardening) |
| 2026-08-14 | Aster overlap, stale tick clock, rank-churn primary sessions | working tree | deterministic runtime/DataPlane harness and Local-L2 profile green; cloud pending | [daily/2026-08-14.md#cluster-cl-105-local-l2-decision-time-aster-overlap-primary-ownership](../daily/2026-08-14.md#cluster-cl-105-local-l2-decision-time-aster-overlap-primary-ownership) |
| 2026-08-15 | bridge WS/REST startup race, reconnect during REST, and prune during wait | working tree | deterministic real DataPlane+WS harness and Local-L2 profile green; cloud pending | [daily/2026-08-15.md#cluster-cl-110-local-l2-ws-rest-bootstrap-bridge-readiness](../daily/2026-08-15.md#cluster-cl-110-local-l2-ws-rest-bootstrap-bridge-readiness) |

## Regression Harness

- `tests/live_harness/test_local_l2_incident_replay.py`
- `tests/test_local_l2_replay_harness.py`
- `tests/test_local_l2_ws.py::TestLocalL2WsFreshnessEvidence`
- `tests/test_diagnose_live.py`
- `scripts/probe_local_l2_rebuilds.py --venue <venue> --symbol <symbol>`

## Next Recurrence Checklist

1. Count `runtime.local_l2_sequence_gap_rebuild` and `runtime.local_l2_snapshot_error` since deploy.
2. Split samples by venue and official classification reason.
3. If `insufficient` appears, inspect whether required raw fields are missing or whether a new exchange semantic is needed.
4. Run public Local-L2 probe for the affected venue/symbol.
5. Do not change data-plane readiness until official docs or V1 behavior proves the current rule wrong.
6. If last-good fallback appears, confirm it only feeds coarse shortlist evidence and that entry dispatch still has targeted revalidate or blocks.
7. Split quote blockers by blocker family and quote age before treating them as incidents.
8. If venue-scope admission cooldown is active, confirm degraded-venue candidates
   are pruned before Local-L2/ws-bbo budget allocation.
9. Closure requires local harness plus cloud classification showing no unexplained entry-blocking Local-L2 samples.
