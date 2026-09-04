# Current Bug Status

This is the **single status source** for active LightFeeV2 bug work. Daily
ledgers and `BUG_INDEX.md` preserve investigation history and evidence, but
their historical status text must not be used to decide whether a fix is
deployed or closed.

Scope: current operational status begins with the post-regression batch
`CL-093` through `CL-124`. The explicit pre-CL-093 historical boundary below
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

- Last production SHA checked: `d230f8df`
- Checked on: `2026-09-05`. Runtime code `d230f8df` was fast-forwarded from
  `47c94b24` over Tailscale SSH and both services restarted active; the
  local `main` ledger subsequently gained docs-only commits, while the cloud
  runtime checkout remains pinned to `d230f8df` because docs are excluded from
  the runtime sync; the deployment marker remains `d230f8df`. All critical
  manifest hashes
  matched; systemd cgroups showed exactly one sidecar and one live main process;
  all seven source domains were fresh; the initial two resource samples showed
  zero `CLOSE_WAIT`, and the later sample showed one live `CLOSE_WAIT` with
  delta `0` (sidecar `0`), so there is no observed growth. The six enabled
  private-WS venues each started exactly once in the post-deploy window, and the
  Binance listenKey was newly created with a valid expiry. Fresh high-confidence
  exchange truth was complete-flat/no-order and state-consistent. Acceptance is
  warning-only, not green, because seven historical close-reconciliation owners
  remain (six evidence debts, one partial); lifecycle/risk are nevertheless
  `running` and `entry_allowed=true`. No order or cancel was submitted by
  deployment checks.
- Prior checked deployment: `2026-09-02`. `dbb591b` was fast-forwarded and deployed; all 16
  critical manifest hashes matched, both services restarted active, singleton
  passed, all seven source domains were fresh, and both processes had zero
  `CLOSE_WAIT`. Fresh high-confidence exchange truth was complete-flat/no-order.
  With the live writer stopped and the authoritative pair plus the earlier
  non-authoritative pair backed up, one explicitly paired `resume-if-safe`
  appended `ops.command_applied` to `runtime/live-events.jsonl` and changed
  `runtime/live-state.json` from `risk_only/fail_closed` to `running/running`.
  The post-start verifier is fully green: no open local work or exchange orders,
  exactly one private stream per enabled venue, and no orders were submitted or
  cancelled by recovery.
- Check command: `python scripts/check_bug_ledger.py --deployed-sha <value-from-production-.deploy_version>`

The command fails when the recorded SHA differs from the supplied production
SHA, or when a row marked deployed/closed names a fix that is absent from the
recorded deployment. Update this file after a deployment or production check;
do not rewrite the daily evidence just to change status.

## Historical Working-Tree Repairs

The following bullets preserve investigation-time wording for older repairs;
the current deployment state is maintained in the table above. Items without
a corresponding deployed row remain local-green only, not production facts:

- exclusion of terminal automatic-history audit debt from recovery probe symbol
  projections;
- source-health inspection in `diagnose_live`, using the same config budgets as
  the deployment verifier; and
- exact Tailscale Quad100 resolver recognition.

- shared HTTP transport typed failure evidence and delayed failed-client
  retirement. This keeps the current request fail-closed, records phase/type/
  root cause/client generation, and prevents a public failure from closing a
  client still borrowed by private REST or listenKey work. It adds no retry or
  venue-specific reset.

- pending-entry delayed maker-fill ownership and all-venue entry-leverage
  verification. These retain a known maker owner until exact fill evidence can
  drive the existing normal hedge, and require every live entry leg to prove
  its exchange-specific leverage postcondition before a new order. They do not
  change the existing terminalization-budget cleanup or submit an order during
  verification.

- 2026-09-03 repairs (CYS terminal-maker live-flatness, Gate `U..u` range
  continuity, Aster structured rejection evidence, immutable terminal debt
  ownership, and snapshot-health clock selection) are included in CL-145 at
  `47c94b24` and are deployed-awaiting-verification. Their natural scenario
  observations remain pending; details are in [2026-09-03](daily/2026-09-03.md).

- 2026-09-04 repairs (matched live-pair account-truth refresh, Bitget
  contracts-backed precision/decimal wire formatting, and literal Local-L2
  heartbeat parsing) are included in CL-145 at `47c94b24` and are
  deployed-awaiting-verification. Aster/OI, Local-L2 readiness, sidecar
  resource, and rotated-log roots remain unconfirmed; details and required
  production evidence are in [2026-09-04](daily/2026-09-04.md).

Their contract, rejected prior repair patterns, counterexamples, and local
evidence are recorded in [2026-08-29](daily/2026-08-29.md),
[2026-08-30](daily/2026-08-30.md), and
[2026-08-31](daily/2026-08-31.md), and
[2026-09-02](daily/2026-09-02.md). They must not be called deployed or closed
until this table receives real fixing commits and a production observation.

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
| CL-119 | closed | `0474ca28` | three production REDs drove the diagnostics/runtime schema, startup owner-symbol reachability, and production-adapter order-contract counterexamples; persisted `runtime.start()` exercises official Bybit/Binance private order endpoints through the shared operation contract/parser; metadata-only adapters remain unsupported; focused `2 passed`; adjacent recovery/startup/closure `343 passed`; complete `4398 passed, 9 skipped`; full profile 10/10, `1571 passed` | `0474ca28` manifest/singleton/services passed; all seven venues high-confidence flat/no-order; COTI/ONG rows visible with `blocking=false`, `allow_new_risk_background_work`, and closure `blocking_row_count=0`; post-deploy scans show no recovery-ledger/final-dispatch blocker. Historical accounting debts remain separately unsettled. | [2026-08-22](daily/2026-08-22.md#cluster-cl-119-terminal-close-debt-entry-gate-semantics) |
| CL-120 | closed | `2377193c` | default production health retained one warning and `ok=false`; explicit deployment admission independently returned `deployment_acceptable=true`; prior full suite/profile green | Required production split observed with no hidden extra warning. Accounting evidence debt is not settled by this closure. | [2026-08-22](daily/2026-08-22.md#cluster-cl-120-background-debt-health-and-deploy-admission-split) |
| CL-121 | deployed-awaiting-verification | `2377193c` | direct per-symbol OI request, snapshot mark-price reuse, shared typed 429/retry-after classifier, stale-counter reset, runtime fail-closed regression; prior full suite/profile green | Needs a natural production targeted-OI success or 429 sample showing exact causality; no forced request/order is required. | [2026-08-22](daily/2026-08-22.md#cluster-cl-121-targeted-oi-refresh-rate-limit-causality) |
| CL-122 | closed | `2377193c` | all `_dispatch_entry` false-return boundaries feed one Counter; initial-ledger, downstream no-quote, selected-dispatch, and `scan.no_entry_diagnostics` regressions; full suite/profile green | Production run `lightfee-1787395032184-4057959` naturally selected ONG, dispatched zero, and emitted `entry_dispatch_blocked_counts={recovery_ledger_blocked:1}`, the same final-gate count, and `candidate_stage_blocked_counts.entry_dispatch=1`; later CL-119 closure removed the false blocker without changing diagnostics. | [2026-08-22](daily/2026-08-22.md#cluster-cl-122-final-entry-dispatch-blocker-observability) |
| CL-123 | closed | `194861c` | real `CloseRuntime` → Binance/Bybit history → exact order/execution → billing regressions cover COTI/ONG, stale CID fallback, Hedge/One-way semantics, pagination, ambiguity, incomplete fees, mismatched/unrelated execution identity, and live-truth blockers (dedicated `11 passed`; related selection `191 passed, 338 deselected`); complete suite `4411 passed, 9 skipped`; full profile 10/10 with `1571` tests | Deployed in `62474b39` and retained in `67e5c66b`. COTI reconciled Binance order `7918051356`, fee `0.01213439`; ONG reconciled Binance `2926675711`, fee `0.00473472`, and Bybit `9cec8c96-dc21-4c31-baf9-ec43f6184195`, fee `0.0144743`. Both events have `venue_statement_reconciled=true`, `evidence_gap=false`; owners/debts are zero and all seven venues remain flat/no-order. | [2026-08-22](daily/2026-08-22.md#cluster-cl-123-automatic-historical-close-evidence-exact-recheck) |
| CL-124 | closed | `67e5c66b` | production-shaped `diagnose_live` RED/GREEN covers account-fee success/unavailable, exact Local-L2 phase start/complete, and the shared `runtime.local_l2_*` / `runtime.entry_local_l2_*` diagnostic families; lifecycle/diagnose suite `135 passed`; complete suite `4412 passed, 9 skipped` | Initial exact-only attempt `ebecc081` left nine natural Local-L2 kinds unmapped and was not accepted as closed. `67e5c66b` is deployed: the same natural startup/Local-L2 family now yields `unmapped_event_kinds=[]`; health/gate are green, services have zero restarts, and seven-venue truth is flat/no-order. | [2026-08-22](daily/2026-08-22.md#cluster-cl-124-startup-local-l2-lifecycle-diagnostic-mapping) |
| CL-125 | deployed-awaiting-verification | `ac9d536` | real `fetch_funding_tickers` slow-transport RED/GREEN: V1 four-request ceiling, miss rotation, normal completion/no aggregate cancellation, parent-cancellation child cleanup, partial-failure quote preservation; full profile 10/10 green | first sample: all source domains fresh and sidecar `14` FD/`0` CLOSE_WAIT; still need a second no-growth sample after SSH recovers | [2026-08-26](daily/2026-08-26.md#cluster-cl-125-okx-funding-fanout-aggregate-cancellation) |
| CL-126 | deployed-awaiting-verification | `ac9d536` | ACK/retry/compensation identity persistence, strict-history complete/temporary failure, and crash replay matrix (`349 passed` in close scope; full profile 10/10 green) | existing BICO passive-close owner prevented full acceptance; after SSH recovery, re-read exchange truth and classify existing COTI×2/BTR evidence without order mutation | [2026-08-27](daily/2026-08-27.md#cl-126-passive-close-ack-identity-loss-and-stranded-evidence-debt) |
| CL-127 | deployed-awaiting-verification | `ac9d536` | fresh writer + stale/degraded quote/funding/liquidity source matrix; live-config budget wiring (`223 passed` health/sidecar/diagnose selection; full profile 10/10 green) | first production sample is green; rerun complete acceptance after state recovery reaches `running` | [2026-08-27](daily/2026-08-27.md#cl-127-sidecar-health-false-green-on-stale-source-evidence) |
| CL-128 | deployed-awaiting-verification | `ac9d536` | rotated-only event, duplicate rotation, global cap, high-confidence `since-deploy`, and large-tail counterexamples (`117 passed`; full profile 10/10 green) | after SSH recovery, run deployed diagnosis and record discovered rotations/scope metadata | [2026-08-27](daily/2026-08-27.md#cl-128-diagnosis-omitted-rotated-journals-and-drifted-ledger) |
| CL-134 | deployed-awaiting-verification | `2111e36` | production-path RED/GREEN for terminal normal pair plus unrelated final close debt; exact-pair/one-leg/wrong-side/wrong-quantity/open-order core matrix; focused `477 passed`; full `4491 passed, 9 skipped` | `fe821142` is live: services active/0 restarts, current state `running` with background debt and high-confidence flat/no-order truth. Observe a natural matched open hedge beside debt remain `running`; non-normal/incomplete truth must stay blocked. | [2026-08-30](daily/2026-08-30.md#cl-134--a-normal-matched-hedge-was-globally-blocked-by-unrelated-close-accounting-debt) |
| CL-135 | deployed-awaiting-verification | `0428fd1` | COTI-shaped delayed-fill → normal-hedge production-path regression; seven-venue prepare/verify capability matrix, malformed/mismatch/dual-mode/non-ack blockers, and same-venue de-duplication; deployment acceptance high-confidence clean | Natural observation still required: a delayed owned maker fill must hedge normally rather than reduce-only flatten; every entry leg must log its exchange-specific leverage proof or block before order submission. | [2026-08-30](daily/2026-08-30.md#cl-135--delayed-owned-maker-fill-was-cleaned-before-hedge-and-entry-leverage-was-not-proved-for-every-live-leg) |
| CL-136 | deployed-awaiting-verification | `079601f` | Aster one-way V3 `userTrades` missing-`reduceOnly` production-shape RED/GREEN; exact order `reduceOnly=false` rejects; legacy generic miss retries once then corrected miss remains terminal; close profile `481 passed`, Aster profile `19 passed` | `f443c10` is live and exchange truth is flat/no-order. Confirm the named ONG debt resolves only when its exact Aster order still proves `FILLED`, `reduceOnly=true`, identity/quantity/price, and quote fee; otherwise it must remain terminal debt. | [2026-08-31](daily/2026-08-31.md#cl-136--aster-v3-one-way-history-row-was-misclassified-as-no-candidate) |
| CL-137 | deployed-awaiting-verification | `0e0cc7a` | snapshot/tail/rotation/no-snapshot/terminal-owner matrix, operator CLI crash-window path, and terminalizer guard (`778 passed`) | `0e0cc7a` is live: manifest, singleton, deployment acceptance, and post-restart checkpoint passed; exchange truth is high-confidence flat/no-order. Await one ordinary later restart or rotated-tail observation; do not force orders or mutate history. | [2026-08-31](daily/2026-08-31.md#cl-137--snapshot-recovery-replayed-retained-audit-history-as-live-work) |
| CL-138 | deployed-awaiting-verification | `d18d016` | Gate/Bitget documented-payload parser, metadata-error fail-closed, source→pairing→V1 discovery, cache, and rate-limit matrix; adjacent `638 passed` | `12d8e038` is live: manifest/singleton/acceptance green; Bitget 93/93 and Gate 90/90 usable rows have nonzero quotes and exchange-future funding, with zero `CLOSE_WAIT`. Await only a natural eligible candidate; do not force an order. A separate post-restart Local-L2 diagnostic signal is tracked independently and is not evidence against this parser repair. | [2026-09-01](daily/2026-09-01.md#cl-138--gatebitget-sidecar-market-data-contract-drift) |
| CL-139 | closed | `49de86a` | real HTTP-400 detail → bounded exact-history → zero-fill → lone-hedge cleanup matrix (`56` focused; `1,601` full) | Production observed exact Bitget `40109` → one Classic history row with zero fill → stale 2,496 maker high-water cleared → existing Bybit reduce-only cleanup verified 2,400 short flat; high-confidence exchange truth has no order/pending owner. | [2026-09-01](daily/2026-09-01.md#cl-139--bitget-canceled-maker-misread-as-fill-cancel-and-reconciliation-contract-gaps) |
| CL-140 | deployed-awaiting-verification | `b682891` | direct deadline-abort → complete-flat recovery release, explicit-operator retain, and incomplete-truth retain matrix (`4` direct/adjacent checks; close profile `481 passed`) | The legacy latch was released by the CL-142/143 audited recovery after complete-flat truth. Still await one natural automatic hedge-deadline event returning to `running`; do not manufacture it. | [2026-09-02](daily/2026-09-02.md#cl-140--automatic-hedge-deadline-stop-was-persisted-as-an-operator-stop) |
| CL-141 | closed | `216dc258` | numeric, display-only-property, malformed fallback, and resource-collector current-generation matrix (`55` health tests; close profile `481 passed`) | `216dc258` manifest/singleton probe passed with all sources fresh and zero `CLOSE_WAIT`; after its ordinary restart the verifier counted exactly six starts (one per enabled venue), used the current activation time as journal lower bound, and saw `NRestarts=0`. | [2026-09-02](daily/2026-09-02.md#cl-141--private-ws-churn-health-counted-a-prior-service-generation) |
| CL-142 | closed | `82e859c` | real maintenance CLI clean-release/real-position-retain matrix (`6 passed`); adjacent ops/risk/recovery (`222 passed`); pending-entry/recovery (`237 passed`); close profile (`481 passed`); full profile (`1,604 passed`) | `dbb591b` is deployed. After fresh complete-flat/no-order truth, the single authoritative resume event (`seq=1`, `run_id=lightfee-1788286119229-2211467`) changed the real state to `running/running`; post-start acceptance is fully green and recovery submitted/cancelled no orders. | [2026-09-02](daily/2026-09-02.md#cl-142--audited-resume-if-safe-could-not-release-a-clean-risk-only-latch) |
| CL-143 | closed | `5d64b5a` | production-shaped CLI RED/GREEN: five controls formerly accepted generic defaults; all six mutating forms now require an explicit pair, retain/clear and lease/discovery complements pass (`46` ops tests); close profile `481 passed`; full release profile `1,604 passed` before final test-only amend | `dbb591b` is deployed. The remote command without a pair rejects with exit `2` before mutation; the one explicit pair wrote the matching authoritative event/state. The earlier generic pair was preserved in the recovery backup, and post-start acceptance is fully green. | [2026-09-02](daily/2026-09-02.md#cl-143--state-mutating-lightfee-ops-could-target-a-non-authoritative-default-pair) |
| CL-144 | deployed-awaiting-verification | `b33aa6c` | Bitget identity/HTTP detail, Local-L2 protocol, pending-entry hedge, immutable close owner including strict partial→final promotion, close/replay, and entry-runtime suites; full validation profile `1,612 passed` plus ACK-state `32 passed`; compile and whitespace checks passed | Deploy `b33aa6c`, then observe naturally: Bitget null-order-ID ACK, owned-hedge retry, Gate/Bitget Local-L2 bridge, and uncertain-submit full-fill close. No order is to be forced for verification. | [2026-09-02](daily/2026-09-02.md#cl-144--bitget-order-identity-owned-hedge-close-owner-and-local-l2-contract-drift) |
| CL-145 | deployed-awaiting-verification | `47c94b24` | 2026-09-03/04 shared recovery/live-truth, Gate/Aster/Bitget contract, diagnosis, Local-L2, Bitget dynamic precision/heartbeat, and typed-transport production-path regressions; full validation profile 10/10; changed regression suite 364 passed | Deployment manifest, singleton, source freshness, FD/CLOSE_WAIT, private WS, listenKey, high-confidence flat/no-order truth, and `diagnose_live --since-deploy` passed. Await only natural matched-pair release, eligible Bitget order, and terminal evidence-debt observations; no order is to be forced. | [2026-09-04](daily/2026-09-04.md) |
| CL-146 | deployed-awaiting-verification | `d230f8df` | Bitget duplicate-client-id production-path regression; inherited close-evidence owner/restart/terminal-debt end-to-end verification; ledger drift correction; Aster/OI request identity, Local-L2 sequence/generation, source-age, and two-sample resource evidence tests; shared Bitget diagnosis predicate and persisted resource baseline | Deployed and observed `2026-09-05`: manifest and systemd service checks passed; seven venue sources fresh; exchange truth high-confidence flat/no-order; six private WS workers started once; resource baseline showed no FD/CLOSE_WAIT growth (latest live `CLOSE_WAIT=1`, delta `0`; sidecar `0`). Acceptance remains warning-only for seven existing close-reconciliation owners, so natural Bitget duplicate-id and evidence-debt completion observations remain pending; no order was forced. | [2026-09-04](daily/2026-09-04.md#current-working-tree-follow-up--wt-20260904-5) |
| CL-147 | local-green | `working-tree` | Bitget Classic/UTA unified historical-close discovery; executed-vs-submitted quantity, close ownership, official trade-side variants, explicit zero execution/fee, cursor pagination, exact order-status recheck, and real `CloseRuntime → BitgetAdapter → VenueTransport` regression; close profile `492 passed`; full validation profile `10/10` passed | Not deployed. After a fixing commit is recorded, observe a natural Bitget evidence-debt reconciliation; no order or cancel should be forced for verification. | [2026-09-05](daily/2026-09-05.md#bitget-historical-close-discovery-is-now-wired-to-the-unified-adapter-contract) |

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
