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
- Binance/Aster first bridge does not cover the REST snapshot boundary in its
  `U/u` range. After that bridge, `U > previous_u + 1` alone is not a gap when
  `pu == previous_u`.
- REST snapshot boundary is not bridged by buffered updates.
- REST snapshot, Gate rebase snapshot, or replay application attempts are not
  bound to the request-time stream generation and snapshot attempt. A stale
  response must be discarded before it can mutate the local book.
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

- Binance/Aster: the first event bridges the REST snapshot by `U/u`; every
  established WS event then requires `pu == previous u`. A mismatch means
  reinitialize the local book; repeating the bridge range test after exact
  `pu` continuity is semantic drift.
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

## Recurrences

| Date | Symbols / Venues | Commit / Fix | Result | Detail |
|---|---|---|---|---|
| 2026-05-29 | Binance/Aster `LABUSDT`, Binance `IOUSDT` | `7ee4c72` | closed for Binance/Aster | [daily/2026-05-29.md#cluster-cl-016-post-deploy-official-local-l2-and-terminal-order-reject-watch](../daily/2026-05-29.md#cluster-cl-016-post-deploy-official-local-l2-and-terminal-order-reject-watch) |
| 2026-05-29 | OKX `LABUSDT` evidence gap | `6987fc8` | closed for classification | [daily/2026-05-29.md#cluster-cl-017-post-deploy-pending-hedge-admission-and-okx-l2-evidence](../daily/2026-05-29.md#cluster-cl-017-post-deploy-pending-hedge-admission-and-okx-l2-evidence) |
| 2026-05-30 | Binance `LABUSDT`, `ALLOUSDT`, `IOUSDT`, `HEMIUSDT`; Aster `HMSTRUSDT`; post-fix `HEIUSDT` snapshot errors | `0fd9a74`; no Local-L2 code change selected | no dirty trading state; final affected-symbol probes flat/no-open-orders | [daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission](../daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission) |
| 2026-05-31 | current run high-volume rebuild/snapshot family across active candidates | no Local-L2 semantic change selected | production state and exchange truth stayed flat/no-open-orders; evidence is insufficient to relax official continuity | [daily/2026-05-31.md#cluster-cl-025-post-ae4bd9c-passive-close-maker-leg-live-flat-precheck](../daily/2026-05-31.md#cluster-cl-025-post-ae4bd9c-passive-close-maker-leg-live-flat-precheck) |
| 2026-06-08 | production issue 11 snapshot/OI degraded evidence | working tree | local RED/GREEN and full pytest green; deploy pending | [daily/2026-06-08.md#cluster-cl-052-production-issues-3-11-root-closure-evidence-hardening](../daily/2026-06-08.md#cluster-cl-052-production-issues-3-11-root-closure-evidence-hardening) |
| 2026-08-01 | Binance `BANKUSDT` exact-`pu` / advanced-`U` false gaps plus completion-audit generation/HOT/dispatch evidence gaps | corrective fix after `0606ab47` and audit remediation | local regression green; deployment proof pending | [daily/2026-08-01.md#cluster-cl-172---post-bridge-binanceaster-range-check-reintroduced-false-gaps](../daily/2026-08-01.md#cluster-cl-172---post-bridge-binanceaster-range-check-reintroduced-false-gaps) |
| 2026-08-01 | Gate HOT refresh lifecycle showed error-free `degraded` duplicates and negative age while the real REST request succeeded | CL-174 atomic dispatch claim and observation-lead evidence | local regression green; deployment proof pending | [daily/2026-08-01.md#cluster-cl-174---concurrent-hot-refresh-lanes-broke-same-book-singleflight-evidence](../daily/2026-08-01.md#cluster-cl-174---concurrent-hot-refresh-lanes-broke-same-book-singleflight-evidence) |

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
6. If last-good fallback appears, confirm it only feeds coarse shortlist evidence and that entry dispatch still has targeted revalidate or blocks.
7. Split quote blockers by blocker family and quote age before treating them as incidents.
8. If venue-scope admission cooldown is active, confirm degraded-venue candidates
   are pruned before Local-L2/ws-bbo budget allocation.
9. Closure requires local harness plus cloud classification showing no unexplained entry-blocking Local-L2 samples.

## CL-163 Evidence Boundary

Aster continues to rebuild only from its documented Binance-compatible `pu/u`
continuity rule.  The additional instrumentation is limited to subscription
ACK, snapshot synchronization, buffered replay, rebuild reason, and
venue-symbol backoff counters.  It does not widen acceptable sequence gaps or
turn incomplete sequence evidence into readiness.

The final entry gate preserves `missing_book`, true stale-book, sequence-gap,
and true clock-skew causes.  The runtime obtains time after awaited L2 work to
avoid falsely classifying scheduling delay as clock skew.  CL-163 is local
verified; deployment pending.

## CL-165 Ownership Boundary

V1 primary/shadow selection is not sufficient by itself: when Local-L2 is
enabled, each selected **primary** must be handed into
`EntryLocalL2SessionRuntime` and the primary pair set before Local-L2 warming.
Shadows stay in the bounded warm pool without session ownership. Every pass
applies V1 `close_missing(active_primaries)` and retains at most 64 sticky
pairs through brief rank churn. A fresh primary without this handoff is an
implementation defect, not a reason to weaken Local-L2; after the handoff, an
unavailable book correctly reports `entry_local_l2_waiting_for_dual_ready`.

The ownership decision itself is stateful and lives in the ranked-scope
selector: an in-scope primary is retained for V1's configured hold window,
then the best shadow can replace the worst eligible primary only when its
direct Local-L2 HOT/WARM book evidence is fresh and valid and its score clears
the configured delta. Shadows must use the same book-readiness predicate as a
primary, but must not own a Local-L2 session. An unassigned timestamp is an
initial-assignment condition, not a permanent promotion block. The policy is
explicit in `StrategyConfig` (two shadows, 15 seconds, 3.0 bps by V1 default)
and rejects invalid values during config validation.

The on-demand readiness decision consumes this Local-L2 lifecycle together
with WS-BBO lease/subscription evidence. BBO activation is scoped to the same
bounded V1 primary+shadow frontier, capped at 100 ms, and is not re-reconciled
to one final candidate after successful prewarm. A prewarm timeout is explicit
and falls back to the existing bounded final activation. Sequence, stale-book,
missing-book, and clock-skew rules remain fail-closed. CL-165 is local
verified; deployment and controlled live proof remain pending.
