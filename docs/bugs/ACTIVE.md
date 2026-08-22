# Current Bug Status

This is the **single status source** for active LightFeeV2 bug work. Daily
ledgers and `BUG_INDEX.md` preserve investigation history and evidence, but
their historical status text must not be used to decide whether a fix is
deployed or closed.

Scope: current operational status begins with the post-regression batch
`CL-093` through `CL-117`. The explicit pre-CL-093 historical boundary below
prevents older nonterminal prose from being mistaken for current work. New
production/parity bugs must be added here when they are discovered.

## Status Rules

Use exactly one status per row:

- `detected` — incident is recorded, but the shared root cause is not yet
  confirmed.
- `root-cause-confirmed` — contract, cause, and affected family are known;
  implementation or a production-path regression is still missing.
- `local-green` — code and production-path regression are green locally, but
  the fixing commit is not known to be on production.
- `deployed-awaiting-verification` — fixing commit is on the recorded
  production version; the row must state the remaining scenario-level proof.
- `closed` — a real production-path regression, deployed version, and
  production probe or explicit observation window are recorded.
- `superseded` — this is not an open defect; its replacement is named in the
  evidence cell.

`closed` is not a synonym for “tests passed.” Its row needs nonempty
**Regression evidence** and **Production evidence** cells, as well as a
deployed SHA. For a non-production fixture, say so explicitly in the
production-evidence cell.

## Deployment Record

- Last production SHA checked: `1e66dc140b92ec08fa60f39f4acd0f5091dbfc65`
- Checked on: `2026-08-22` after the CL-118 deployment: remote compileall and
  the 426-file manifest passed; all 14 critical hashes matched; one live and
  one sidecar restarted active with zero restarts; singleton, verifier, and
  diagnostic acceptance gate were green with high-confidence flat/no-order
  exchange truth.
- Check command: `python scripts/check_bug_ledger.py --deployed-sha <value-from-production-.deploy_version>`

The command fails when the recorded SHA differs from the supplied production
SHA, or when a row marked deployed/closed names a fix that is absent from the
recorded deployment. Update this file after a deployment or production check;
do not rewrite the daily evidence just to change status.

## Current Batch

| ID | Status | Fix commit | Regression evidence | Production evidence / next condition | History |
|---|---|---|---|---|---|
| CL-093 | closed | `3c42aea` | startup/normal positive-fill replay matrix | manifest and production health verified; replay observation remains watch-only | [2026-08-04](daily/2026-08-04.md#cluster-cl-093-confirmed-replay-fill-owner-terminalization) |
| CL-094 | deployed-awaiting-verification | `319896bf` | lifecycle/state/passive regression | needs Hyperliquid recovery-fail-closed → recovered event proof | [2026-08-04](daily/2026-08-04.md#cluster-cl-094-post-deploy-bankusdt-lifecycle-closure-and-hyperliquid-recovery-observability) |
| CL-095 | deployed-awaiting-verification | `319896bf` | restart persistence regression | needs production restart/recovery preserving close CID and escalation budget | [2026-08-04](daily/2026-08-04.md#cluster-cl-095-passive-close-billing-recovery-symbol-and-joint-entry-grid) |
| CL-096 | deployed-awaiting-verification | `ba4e9b66` | ACK/uncertain-close reconciliation regression | needs target reconciliation event proving repaired owner path | [2026-08-04](daily/2026-08-04.md#cluster-cl-096-pending-close-reconciliation-ack-truth-gap-billing-loop) |
| CL-097 | deployed-awaiting-verification | `7fa873bf` | Aster order/owner regressions | needs rejected zero-fill passive-close truth proof | [2026-08-09](daily/2026-08-09.md#cluster-cl-097-live-order-preparation-passive-close-terminality-and-owner-count) |
| CL-098 | deployed-awaiting-verification | `7fa873bf` | canonical owner/health regression | needs historical recovery/accounting acceptance review | [2026-08-09](daily/2026-08-09.md#cluster-cl-098-close-reconciliation-owner-contract-and-aster-v3-evidence) |
| CL-099 | deployed-awaiting-verification | `67b6ae2b` | stale-risk-only recovery regression | needs restart/recovery proof that account truth releases latch | [2026-08-09](daily/2026-08-09.md#cluster-cl-099-kaito-v3-capacity-admission-and-live-artifact-release) |
| CL-100 | closed | `c48f59a` | target recovery regression and incident replay | CLUSDT/OKX/Bitget exchange truth production-verified | [2026-08-12](daily/2026-08-12.md#cluster-cl-100-bitget-success-null-open-order-truth) |
| CL-101 | deployed-awaiting-verification | `dd04106d` | recovered-close accounting regression | needs legacy-V1 isolation and external recovered-shape revalidation | [2026-08-12](daily/2026-08-12.md#cluster-cl-101-unattributed-recovered-close-accounting-boundary) |
| CL-102 | closed | `03b53855` | Binance close-fee import/reconciliation regression | four COTI records imported/reconciled and target truth verified | [2026-08-13](daily/2026-08-13.md#cluster-cl-102-binance-close-fee-evidence) |
| CL-103 | superseded | `1077e3a9` | n/a; replaced design | replaced by CL-104 because old BBO/Local-L2 split broke V1 ownership | [2026-08-13](daily/2026-08-13.md#cluster-cl-103-ws-bbo-candidate-l2-final-cost-path) |
| CL-104 | deployed-awaiting-verification | `1077e3a9` | full Local-L2 ownership regression | needs active-candidate ownership observation | [2026-08-13](daily/2026-08-13.md#cluster-cl-104-local-l2-ownership-degraded-snapshot-parity) |
| CL-105 | deployed-awaiting-verification | `e2cd56ac` | runtime/DataPlane decision-boundary regression | needs rank-churn or Aster covered-range observation | [2026-08-14](daily/2026-08-14.md#cluster-cl-105-local-l2-decision-time-aster-overlap-primary-ownership) |
| CL-106 | deployed-awaiting-verification | `7dbf4e14` | Local-L2 stream-ownership regression | needs Binance WS bridge observation | [2026-08-14](daily/2026-08-14.md#cluster-cl-106-local-l2-rest-bridge-and-clock-domain) |
| CL-107 | deployed-awaiting-verification | `6b24e3a7` | Aster MARKET wire regression | needs real Aster MARKET/IOC close request evidence | [2026-08-14](daily/2026-08-14.md#cluster-cl-107-aster-v3-market-ioc-wire-contract-and-trigger-causality) |
| CL-108 | deployed-awaiting-verification | `6b24e3a7` | Aster Hedge/MARKET result regression | needs V3 Hedge-position and MARKET RESULT observation | [2026-08-15](daily/2026-08-15.md#cluster-cl-108-aster-v3-position-mode-and-market-result-contract) |
| CL-109 | deployed-awaiting-verification | `51323aa2` | deadline/CID/zero-fill boundary regressions | needs target live evidence or Binance dynamic-rule path | [2026-08-15](daily/2026-08-15.md#cluster-cl-109-close-entry-boundary-contracts) |
| CL-110 | deployed-awaiting-verification | `36d2e168` | Bybit WS-before-REST race regression (95 passed) | needs dynamic bridge activation observation | [2026-08-15](daily/2026-08-15.md#cluster-cl-110-local-l2-ws-rest-bootstrap-bridge-readiness) |
| CL-111 | closed | `51323aa2` | Binance fixture contract regression (135 passed) | non-production fixture correction only | [2026-08-15](daily/2026-08-15.md#cluster-cl-111-binance-live-order-fixture-dynamic-rule-contract) |
| CL-112 | closed | `cf31c16` | execution-accounting regression suite | manifest, singleton, verifier, and seven-venue probe production-green | [2026-08-16](daily/2026-08-16.md#cluster-cl-112-execution-evidence-accounting-contract) |
| CL-113 | deployed-awaiting-verification | `047b22c` | short-maker/Bybit recovery regression | needs lifecycle that finalizes or retains explicit ownership | [2026-08-17](daily/2026-08-17.md#cluster-cl-113-short-maker-reconciliation-and-bybit-recovery-identity) |
| CL-114 | deployed-awaiting-verification | `e06d58c` | staged close/import/recovery regression | COW needs dual-fill refetch; BICO needs Binance identity | [2026-08-19](daily/2026-08-19.md#cluster-cl-114-staged-close-reconciliation-and-terminal-fill-price) |
| CL-115 | deployed-awaiting-verification | `79db9b4` | direct reconciliation, passive reconciliation removal, direct passive completion, and passive-to-accounting handoff matrix (`3 passed`); recovery/closure suite (`90 passed`); targeted suite (`141 passed`) | 2026-08-21 post-deploy check is high-confidence flat with no pending passive-close owner. Needs another lifecycle/restart observation; the separate COTI bill still requires exact Binance evidence. | [2026-08-20](daily/2026-08-20.md#cluster-cl-115-post-terminal-recovery-ledger-staleness) |
| CL-116 | closed | `f174bce` | shared health/diagnosis-gate RED/GREEN (`7 passed`), related suite (`220 passed`), full suite (`4359 passed, 9 skipped`) | `76e6f914` production probe: manifest/singleton passed; verifier green and `diagnose_live --since-deploy` reported a passing acceptance gate with no blockers. The one visible COTI bill remains a separate evidence-retrieval task. | [2026-08-21](daily/2026-08-21.md#cluster-cl-116-background-accounting-debt-deploy-gate) |
| CL-117 | deployed-awaiting-verification | `9e7c982` | live `CloseRuntime` handoff/replay and duplicate-query regression (`21 passed`); diagnosis/lifecycle matrix (`133 passed`); full suite (`4367 passed, 9 skipped`) | `9e7c982` production probe passed manifest, singleton, verifier, and acceptance gate with flat/no-order exchange truth. Needs a real future complete partial-to-final handoff observation; the separate COTI evidence debt still needs exact Binance import evidence. | [2026-08-21](daily/2026-08-21.md#cluster-cl-117-close-evidence-lineage-and-residual-classification) |
| CL-118 | deployed-awaiting-verification | `1e66dc1` | producer-to-consumer live-flat regression (`22 passed`); billing-evidence import suite (`35 passed`); full validation profile (`1,568 passed`); cloud compile/manifest/singleton/health/diagnose checks | `1e66dc1` is live: all 14 critical deployment hashes, singleton, health, and high-confidence all-venue flat/no-order truth passed. Production must still observe a V2-owned close with one attributed leg and one unknown exchange leg, retaining an `unattributed_exchange_execution` debt with no automatic attribution. | [2026-08-22](daily/2026-08-22.md#cluster-cl-118-unattributed-exchange-close-provenance) |
| CL-119 | local-green | worktree on `a5ef64a` (uncommitted) | exact/reversed/different venue-pair matrix; active/unknown/truth-gap vs terminal proven-flat debt; ledger-build owner add/transition/remove invalidation; full runtime dispatch; complete suite `4386 passed, 9 skipped`; full profile `1571 passed` | Not deployed. Needs post-deploy proof that terminal proven-flat debt remains visible but does not permanently block its pair, while active/unknown close work still blocks. Historical COTI/ONG debts remain unsettled. | [2026-08-22](daily/2026-08-22.md#cluster-cl-119-terminal-close-debt-entry-gate-semantics) |
| CL-120 | local-green | worktree on `a5ef64a` (uncommitted) | default health non-green plus exact explicit deployment-acceptance RED/GREEN; generated deploy command regression; complete suite `4386 passed, 9 skipped`; full profile `1571 passed` | Not deployed. Needs ordinary health to retain the accounting warning and explicit deploy admission to accept only the all-evidence-debt proven-flat sole-warning case. | [2026-08-22](daily/2026-08-22.md#cluster-cl-120-background-debt-health-and-deploy-admission-split) |
| CL-121 | local-green | worktree on `a5ef64a` (uncommitted) | direct per-symbol OI request, snapshot mark-price reuse, shared typed 429/retry-after classifier, stale-counter reset, runtime fail-closed regression; complete suite `4386 passed, 9 skipped`; full profile `1571 passed` | Not deployed. Needs a production targeted-OI success or 429 sample showing exact causality; no forced request/order is required. | [2026-08-22](daily/2026-08-22.md#cluster-cl-121-targeted-oi-refresh-rate-limit-causality) |
| CL-122 | local-green | worktree on `a5ef64a` (uncommitted) | all `_dispatch_entry` false-return boundaries feed one Counter; initial-ledger, downstream no-quote, selected-dispatch, and `scan.no_entry_diagnostics` regressions; complete suite `4386 passed, 9 skipped`; full profile `1571 passed` | Not deployed. Needs a future zero-dispatch scan to expose the final dispatch blocker breakdown without changing the admission decision. | [2026-08-22](daily/2026-08-22.md#cluster-cl-122-final-entry-dispatch-blocker-observability) |

## Pre-CL-093 Historical Boundary

The historical index contains 23 entries whose old prose still says local,
pending, or otherwise nonterminal. They were audited as an explicit boundary,
not silently treated as closed. They are **not current operating work** and
must be re-triaged into `Current Batch` before their old status can justify a
code change, a release decision, or a production conclusion.

- Already represented by the current batch: `CL-097-live-order-preparation-passive-close-terminality-and-owner-count`.
- Historical status must be re-triaged before reuse: `CL-077-home-lifecycle-quick-flat-recurrence`, `CL-076-v1-lifecycle-closure-table-runtime-gate-release-unification`, `CL-068-code-side-no-entry-blocker-attribution-closure`, `CL-072-entry-admission-prefilter-and-v1-state-machine-evidence`, `CL-038-single-leg-force-close-and-ledger-attribution`, `CL-036-startup-live-position-probe-static-universe-fanout`, `CL-035-post-e087513-long-window-follow-up`, `CL-014-opgusdt-passive-close-stuck-under-chunk-live-flatness-and-precision`, `CL-013-pending-entry-v1-terminality-drift-live-single-sided`, `CL-004-hyperliquid-ioc-finalize-rounding-v1-drift`, `CL-001-passive-close-bybit-tick-v1-drift`, `CL-002-passive-close-terminal-flat-probe-snapshot-fallback-drift`, `CL-004-bybit-hedge-resting-limit-and-aster-buffer-v1-drift`, `CL-002-live-entry-l2-and-exchange-residual-watch`, `CL-003-pending-entry-hedge-submit-reconciliation-v1-drift`, `CL-002-fail-closed-latch-v1-parity-drift`, `CL-003-production-pending-hedge-inflight-v1-parity-drift`, `CL-002-D-maker-rejected-pending-v1-parity-drift`, `CL-002-E-local-l2-bootstrap-structure-v1-parity-drift`, `CL-002-C-no-entry-reason-v1-aggregate-drift`, `BUG-20260514-v2-v1-parity-root-fix-loop`, and `CL-005-zero-fill-ghost-open-position-v1-parity-drift`.

## Lightweight Update Routine

1. Add the incident evidence to its daily ledger and add/update the reusable
   card only for a recurring family.
2. Add one row here immediately with `detected`; move it through the statuses
   above rather than writing a second narrative status. Add regression and
   production cells before declaring it local-green, deployed, or closed.
3. Before marking `deployed-awaiting-verification` or `closed`, run the
   ledger check with the real `.deploy_version` (the argument is mandatory)
   and record the result in the row's linked daily entry.
4. If a claimed fix misses a complementary production path, return the row to
   `root-cause-confirmed` or `local-green`; record the failed attempt in the
   daily ledger/card instead of silently replacing it.
