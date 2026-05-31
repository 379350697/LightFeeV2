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

- Binance/Aster `pu` does not match previous `u` after snapshot bridge evidence.
- Binance/Aster update range proves a real skipped update.
- REST snapshot boundary is not bridged by buffered updates.
- OKX `prevSeqId/seqId` proves previous-link mismatch or sequence reset.
- OKX checksum mismatch is present while checksum is still meaningful for the wire channel.

## V1 / Exchange Semantics

- Binance/Aster: V1 and exchange docs require `pu == previous u`; mismatch means reinitialize local book.
- OKX: older V1/V2 parity included checksum support, but OKX later documented JSON checksum deprecation and sequence validation using `seqId/prevSeqId`. Use exchange docs for OKX classification instead of forcing Binance/Aster `pu` wording.
- Do not accept broken deltas as a "fix"; that is semantic drift and can create a false-ready book.

## Attempts Ledger

| Date | Attempt | Status | Why |
|---|---|---|---|
| 2026-05-18 | Preserve fault reason through rebuild/bootstrapping | effective | Fixed V2 state-machine evidence loss for readiness arming. |
| 2026-05-29 | Shared strict Binance/Aster classifier | effective for Binance/Aster | Closed Aster evidence gap and prevented over-classifying field-complete non-breaks. |
| 2026-05-29 | OKX `seqId/prevSeqId` and checksum classification | effective for evidence classification | Cloud post-deploy Local-L2 classification had `L2_INSUFFICIENT=[]`. |
| 2026-05-30 | Post-`bbcd7b9` high rebuild/snapshot watch | deployed/probe verified | Current evidence remains official continuity/rebuild behavior; no Local-L2 readiness relaxation is allowed without new docs or V1 proof. |

## Recurrences

| Date | Symbols / Venues | Commit / Fix | Result | Detail |
|---|---|---|---|---|
| 2026-05-29 | Binance/Aster `LABUSDT`, Binance `IOUSDT` | `7ee4c72` | closed for Binance/Aster | [daily/2026-05-29.md#cluster-cl-016-post-deploy-official-local-l2-and-terminal-order-reject-watch](../daily/2026-05-29.md#cluster-cl-016-post-deploy-official-local-l2-and-terminal-order-reject-watch) |
| 2026-05-29 | OKX `LABUSDT` evidence gap | `6987fc8` | closed for classification | [daily/2026-05-29.md#cluster-cl-017-post-deploy-pending-hedge-admission-and-okx-l2-evidence](../daily/2026-05-29.md#cluster-cl-017-post-deploy-pending-hedge-admission-and-okx-l2-evidence) |
| 2026-05-30 | Binance `LABUSDT`, `ALLOUSDT`, `IOUSDT`, `HEMIUSDT`; Aster `HMSTRUSDT`; post-fix `HEIUSDT` snapshot errors | `0fd9a74`; no Local-L2 code change selected | no dirty trading state; final affected-symbol probes flat/no-open-orders | [daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission](../daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission) |
| 2026-05-31 | current run high-volume rebuild/snapshot family across active candidates | no Local-L2 semantic change selected | production state and exchange truth stayed flat/no-open-orders; evidence is insufficient to relax official continuity | [daily/2026-05-31.md#cluster-cl-025-post-ae4bd9c-passive-close-maker-leg-live-flat-precheck](../daily/2026-05-31.md#cluster-cl-025-post-ae4bd9c-passive-close-maker-leg-live-flat-precheck) |

## Regression Harness

- `tests/live_harness/test_local_l2_incident_replay.py`
- `tests/test_local_l2_replay_harness.py`
- `tests/test_diagnose_live.py`
- `scripts/probe_local_l2_rebuilds.py --venue <venue> --symbol <symbol>`

## Next Recurrence Checklist

1. Count `runtime.local_l2_sequence_gap_rebuild` and `runtime.local_l2_snapshot_error` since deploy.
2. Split samples by venue and official classification reason.
3. If `insufficient` appears, inspect whether required raw fields are missing or whether a new exchange semantic is needed.
4. Run public Local-L2 probe for the affected venue/symbol.
5. Do not change data-plane readiness until official docs or V1 behavior proves the current rule wrong.
6. Closure requires local harness plus cloud classification showing no unexplained entry-blocking Local-L2 samples.
