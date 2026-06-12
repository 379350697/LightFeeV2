# Bug Ledger Index

This index tracks LightFeeV2 production/parity bugs with stable fingerprints. It follows the V1 bug-ledger style so regressions can be tied back to prior fixes, failed attempts, and verification evidence.

## Bug Cards

Use these quick-read cards first when a familiar failure family recurs. Daily
ledgers keep full incident evidence; cards keep reusable root-cause memory.

| Card | Failure Family | Start Here When |
|---|---|---|
| [residual-repair-live-truth](cards/residual-repair-live-truth.md) | residual repair live truth, pair-gate release, open-order/dust terminality | `execution.residual_repair_paused`, `residual_repair_live_open_orders_present`, `residual_repair_live_position_nonzero`, or stale `pending_residual_repairs` appears. |
| [pending-entry-hedge-admission](cards/pending-entry-hedge-admission.md) | pending hedge deterministic admission rejects | Bybit `110125`/`110126`, Aster `-5018`, Hyperliquid `Insufficient margin`, or repeated `pending_entry.hedge_submit_result:error` appears. |
| [local-l2-sequence-continuity](cards/local-l2-sequence-continuity.md) | Local-L2 rebuilds and official sequence evidence | `runtime.local_l2_sequence_gap_rebuild`, `runtime.local_l2_snapshot_error`, or Local-L2 insufficient evidence recurs. |
| [passive-close-terminal-flatness](cards/passive-close-terminal-flatness.md) | passive close terminal flatness / under-min / price unavailable | Pending passive close loops, terminal flat, under-min, or price-unavailable close branches recur. |
| [pending-entry-terminality-live-truth](cards/pending-entry-terminality-live-truth.md) | pending entry false flat / live truth mismatch | Local state is flat but exchange truth has nonzero positions, or pending entries clear on stale/uncertain evidence. |

## Recent Closures

Latest V1 lifecycle closure-table runtime takeover, 2026-06-13: recent
WS-BBO, pending-entry, passive-close, residual, and recovery fixes are now
projected through one pure `V1LifecycleClosureTable` instead of being
reinterpreted by separate runtime, diagnose, and production-health paths. The
table maps `ENTRY_QUOTE_LEASE`, `PENDING_ENTRY`, `OPEN_POSITION`,
`PASSIVE_CLOSE`, `RESIDUAL_REPAIR`, `RECOVERY_TRUTH`, and `RUNTIME_PROGRESS`
rows, each with a stable `closure_decision_id`. Runtime entry admission now
reads `closure.summary.entry_allowed`; recovery block/clear, pending removal,
passive close terminal events, residual terminal/dust release, diagnose,
production health, and current-state export all consume or emit closure-table
identity. The implementation deliberately reuses the runtime-owned
`RecoveryLedger` and `V1RecoveryDecisionCore` result when available, after
local regression caught the exact drift that would otherwise misclassify an
owned live order as orphan during a second owner-unaware projection. Local
verification passed the closure table suite (`16 passed`), the related
diagnose/production-health/snapshot/startup/entry/passive regression set
(`551 passed`), compileall, diff-check, and GitNexus detect-changes reported
low risk / affected processes `0` with the known caveat that `runtime.py` is
too large for method-level GitNexus indexing. No order submit/cancel,
runtime-state mutation, strategy, sizing, Local-L2 opt-in, WS-BBO budget/TTL,
close executor, residual-repair submit, or recovery flatten policy changed.
Cloud deployment is pending. Full evidence is recorded in
[`daily/2026-06-13.md#cluster-cl-076-v1-lifecycle-closure-table-runtime-gaterelease-unification`](daily/2026-06-13.md#cluster-cl-076-v1-lifecycle-closure-table-runtime-gaterelease-unification).

Latest WS-BBO tracked-scope data-plane root fix, 2026-06-12: post-cutover
production evidence showed `live_tick_stale` was recurring after settled checks,
not just during warmup. Exchange truth stayed flat/no-open-orders. The first
root was a V1 scope drift introduced by the WS-BBO data-plane switch: V2
revalidated quote truth across the whole tradeable shortlist, while V1 assigns
execution-grade market data only to primary plus shadow tracked opportunities.
The first fix narrowed WS-BBO prewarm/revalidate to primary+shadow and kept
CL-071 REST fallback for tracked budget-excluded top candidates. After that
deployed as `26d4ff1`, post-deploy no-entry diagnostics showed
`tradeable_count=0` and all `quote_truth_*` counters at `0`, but
`live_tick_stale` recurred again with the live process CPU-bound around 96%.
The remaining root was the same scope drift in snapshot-freshness
observability: `_snapshot_freshness_observability()` expanded the full snapshot
candidate universe, including transfer lifecycle status, before tradeable or
tracked narrowing. The local follow-up now keeps initial snapshot freshness
observability at global snapshot/quote scope and refreshes candidate-scoped
status only for V1 primary+shadow tracked candidates when they exist. After
that follow-up was deployed as `13ea417`, quote fanout stayed at tracked-scope
scale (`target_count=16`, `quote_truth_rest_resolved_count=4`), but
`live_tick_stale` recurred once more: the final snapshot freshness filter still
evaluated the full post-discovery tradeable set and emitted hundreds of
`runtime.snapshot_freshness_decision` records for untracked candidates. The
next fix deployed as `a87c249`, but `live_tick_stale` recurred again during
`last_good_sidecar` warmup. That recurrence had no quote revalidate fanout yet;
instead `runtime.snapshot_fallback_last_good` still built its
`candidate_freshness_scope` by running per-candidate freshness decisions across
all 4689 snapshot candidates before primary+shadow tracking existed. The
deployed fix at `6cffb2b` now feeds final freshness filtering and fallback
health candidate-scope sampling only from V1 primary+shadow in
WS-BBO/Local-L2 effective modes, so only tracked candidates can obtain fresh
execution-grade quote/freshness evidence and reach selection, and warmup
diagnostics no longer walk the full candidate universe. Diagnostics now
include `quote_revalidate_candidate_scope`,
`quote_revalidate_candidate_count`, `quote_revalidate_all_target_count`,
`quote_revalidate_skipped_untracked_count`,
`snapshot_freshness_candidate_scope`,
`snapshot_freshness_candidate_count`, and
`snapshot_freshness_skipped_untracked_count`, plus the filter-scope counters
`snapshot_freshness_filter_candidate_scope`,
`snapshot_freshness_filter_candidate_count`, and
`snapshot_freshness_filter_skipped_untracked_count`, plus fallback-health
counters `candidate_freshness_candidate_scope`,
`candidate_freshness_candidate_count`, and
`candidate_freshness_skipped_untracked_count`. RED/GREEN reproduced all
failure layers: 50 stale candidates shrank to 3 tracked candidates / 6 quote
legs with 94 untracked quote targets skipped, 64 no-tradeable snapshot
candidates no longer enter freshness observability, and 64 tradeable candidates
now feed only 8 primary+shadow candidates into the final freshness filter, while
64 fallback-health candidates now call per-candidate freshness only for 8
tracked candidates. CL-071 fallback and the Local-L2 V1 primary+shadow
activation test stayed green. Cloud fast-forwarded by
`git pull --ff-only origin main` to `6cffb2b`; `.deploy_version` matched;
manifest, compileall, and the focused remote suite passed; sidecar/live
restarted active with `NRestarts=0`. Consecutive settled verifiers passed
without `live_tick_stale` recurrence (`ok=true`, `critical_count=0`,
`warning_count=0`, `runtime_progress.active_lane_overdue=false`, full tick
within the 60000ms budget). Since-deploy events showed max
`runtime.entry_quote_revalidate_targeted.target_count=16`, max candidate rank
`10`, recent REST resolutions only `0-2`, and fallback health
`candidate_freshness_candidate_scope=v1_primary_shadow` with only `10` sampled
tracked candidates out of about `4634-4642`. Acceptance stayed flat:
`entry_opened_count=0`, `position_opened_count=0`, no open orders, pending
entry, pending close, residual repair, abnormal or single-leg position, and
`local_l2_residual_runtime_enabled_count=0`. The only since-deploy error-like
event was a nonblocking Bitget best-effort bulk-position timeout
(`blocking=false`, `running_with_nonblocking_health_diagnostic`); diagnose
still reports Hyperliquid API-wallet account/signer mismatch metadata
(`account_matches_signer=false`), which is a preflight/env identity issue and
not a WS-BBO/Local-L2 or SAHARA open/close root. No health budget, WS-BBO
budget/TTL, REST concurrency, strategy/OI/liquidity, admission, sizing, order,
close, recovery, residual-repair, or production config semantics changed.
Full evidence is recorded in
[`daily/2026-06-12.md#cluster-cl-075-ws-bbo-tracked-scope-data-plane-fanout`](daily/2026-06-12.md#cluster-cl-075-ws-bbo-tracked-scope-data-plane-fanout).

Latest runtime heartbeat/health-gate root fix, 2026-06-12: the
`live_tick_stale` line now separates current-state exporter freshness from real
runtime business progress. `EngineState` and current-state exports carry
`runtime_progress`; `LiveRuntime.run_loop()` records lane boundaries for
`full_tick`, `active_positions`, `rate_limit_reload`, `local_l2_sync`,
`passive_close`, `normal_exit`, `maker_event`, `pending_entry_maintenance`, and
`housekeeping`, while `_current_state_heartbeat_loop()` only writes snapshots
and does not advance lane progress. `analyze_current_state()` no longer uses
fresh `current_state_age_ms` to suppress `live_tick_stale`; suppression now
requires clean local state, flat exchange truth, and recent `last_scan`, recent
runtime lane progress, or a non-overdue bounded active lane. Fresh
exporter-only snapshots keep `live_tick_stale` and add
`exporter_only_progress`, with `progress_source` and normalized
`runtime_progress` exposed through production health and diagnose. Follow-up
review found the remaining cloud red was also caused by production's untracked
`config/live.toml` retaining `local_l2_enabled=true` without an explicit WS BBO
provider, which pull-only deploys cannot change. Live config loading now
defaults missing live `entry_readiness_provider` to `ws_bbo_quote_lease`;
explicit `entry_readiness_provider="local_l2"` remains the only Local-L2 opt-in.
Runtime exports `runtime_market_data_config`, effective-disables Local-L2
startup/dynamic/snapshot/session gates when WS BBO is active, drives pending
passive maker events from fresh WS BBO quotes through the in-situ hedge driver,
and diagnose blocks `local_l2_residual_runtime_enabled` if any Local-L2 runtime
event appears in WS BBO effective mode. Deploy follow-up found the hand-written
current-state JSON exporter omitted the new `runtime_market_data_config` even
though `EngineState.to_dict()` had it, and recovered runtime state could export
before refreshing the effective config; runtime now refreshes that config before
current-state export, the exporter includes the field, and diagnose preserves it
through the local-state/state-consistency/acceptance-gate views. A final
follow-up found active Local-L2 worker/event paths were off, but persisted
`runtime/live-state.json` could still retain old `local_l2_books_snapshot`
entries until the runtime wrote a new snapshot. `7180ab8` closes that drift by
clearing Local-L2 runtime containers and persisted retained/book/session
snapshots whenever WS BBO is the effective profile.
Focused local verification passed (`125 passed` for the heartbeat suite,
`555 passed` for the WS BBO/Local-L2 follow-up suite, `156 passed` for the
current-state/health/diagnose export follow-up, and `96 passed` for the final
startup-preflight snapshot regression), compileall and diff-check passed. Cloud
was fast-forwarded to `7180ab8` by `git pull --ff-only origin main`,
`.deploy_version` matched, sidecar/live were restarted active, both
`runtime/live-state-current.json` and `runtime/live-state.json` reported
retained/book/session Local-L2 snapshots `0/0/0`, diagnose passed with
`local_l2_residual_runtime_enabled_count=0`, no opens/pending/residual/open
orders, and the final settled production verifier reported `ok=true`,
`critical_count=0`. A later docs-only fast-forward did not restart services;
one verifier sample briefly observed `risk_only` with `block_reason=orphan_maker_order`
and `pending_entry_count=1` during a Bybit `TRUMPUSDT` maker abort/cleanup
cycle. The settled follow-up diagnose/verifier returned to `RUNNING_CLEAN`,
`running/running`, no open positions, no open orders, no pending entry/close or
residual work, and diagnose still reported `entry_opened_count=0` and
`position_opened_count=0`; track any recurrence as pending-entry lifecycle
churn, separate from the WS BBO/Local-L2 data-plane closure. Full evidence is
recorded in
[`daily/2026-06-12.md#cluster-cl-074-runtime-heartbeat-health-gate-exporter-only-false-progress`](daily/2026-06-12.md#cluster-cl-074-runtime-heartbeat-health-gate-exporter-only-false-progress).

Latest SAHARA entry residual dust warning split, 2026-06-12: post-deploy
review of `6b92cd1` found current production flat/no-open-orders and one
`SAHARAUSDT` lifecycle that opened, repaired residual, and closed. The
short-window quantity evidence was Bybit maker `2871.070969...`, OKX hedge
`2860`, residual `11`, approximately `0.38%` of the matched `2860`. The local
fix tightens entry residual dust tolerance from `<=5%` to `<=2%` and still
requires exchange-min terminal evidence (`exchange_min_quantity_dust` or
`exchange_min_notional_dust`) before emitting
`execution.entry_residual_dust_tolerated`; exchange-min entry residuals above
`2%` pause the residual repair instead of releasing the pair gate. Diagnose now keeps raw
`hedge_quantity_undercut` / `common_quantity_mismatch` counters while adding
warning counters that exclude tolerated `<=2%` exchange-min dust entries, and
the production acceptance gate exposes short-window warning families separately
from final flat health. The fix was deployed as `deec77f`; cloud read-only
verification showed matching remote `git_head` / `.deploy_version`, flat
exchange truth, no open orders, no pending work, and zero entry quantity
warnings in the deploy window. One HOME passive-close order-truth gap remains
classified separately as closed by current exchange truth.
Hyperliquid `account_wallet_signer_mismatch` remains a separate preflight/env
identity issue, not part of the SAHARA residual family. Full evidence is
recorded in
[`daily/2026-06-12.md#cluster-cl-073-sahara-entry-residual-dust-2-warning-split`](daily/2026-06-12.md#cluster-cl-073-sahara-entry-residual-dust-2-warning-split).

Latest quote-truth budget-excluded REST closure, 2026-06-11: post-deploy
evidence showed the quote stale/last-good pre-filter revalidation path was active
but incomplete. `runtime.entry_quote_revalidate_resolved` and
`runtime.last_good_revalidated_by_entry_quote_truth` increased, while
`runtime.entry_quote_revalidate_failed` and
`runtime.entry_ws_bbo_top_candidate_rewarm_budget_exhausted` remained high.
The deterministic gap was that must-resolve top candidate legs excluded from
the WS-BBO subscription budget were treated as final failures and never reached
REST/top-book truth. The follow-up fix keeps WS-BBO budget and quote TTL
unchanged, but reclassifies budget exhaustion as a WS prewarm outcome only:
budget-excluded must-resolve legs now continue into bounded REST/top-book
fallback, successful REST quotes update the entry quote overlay/cache, and
`budget_excluded_without_rest_count` is exposed as the hard code-side closure
metric. Invalid, zero, crossed, and missing-observed quotes still fail closed,
and no strategy, OI, liquidity, margin/admission, order submit, close, or
production config semantics changed. Full evidence is recorded in
[`daily/2026-06-11.md#cluster-cl-071-quote-stale--last-good-entry-truth-revalidate-closure`](daily/2026-06-11.md#cluster-cl-071-quote-stale--last-good-entry-truth-revalidate-closure).

Latest 8-concurrent-position capacity closure, 2026-06-11:
production concurrency target is 8, and a normal balanced active position must
consume one slot rather than turning diagnose or entry admission into global
single-position mode. The local closure keeps V1/historical semantics: only
over-capacity, single-leg exposure, exchange/local mismatch, unresolved open
orders, pending truth gaps, reduce-only, or fail-closed states block new entry
globally. Diagnose now distinguishes balanced active lifecycle with remaining
capacity from true high-risk abnormal state, normalizes enum-style
`Side.BUY`/`Side.SELL`, and runtime no-entry/current-state diagnostics expose
`max_concurrent_positions`, `open_position_count`, and `remaining_slots`. New
runtime entry tests prove that 1 healthy active position with
`max_concurrent_positions=8` can still select and dispatch another symbol, and
that pending close reconciliation/passive close gates block only the matching
symbol/venue pair. No strategy, OI, liquidity, margin/admission, WS-BBO budget,
TTL, or production config was loosened.

Latest Hyperliquid unified-collateral diagnostic closure, 2026-06-11:
production blocker attribution could still report Hyperliquid
`insufficient_margin_admission_prefiltered` as an account/admission blocker
when the account was actually a unified account with available spot USDC
collateral. The transport admission path already uses the configured
Hyperliquid account address, checks `userAbstraction`, and only falls back to
`spotClearinghouseState` for `unifiedAccount`; a new guard test freezes the
non-unified fail-closed behavior and the snapshot now carries
`balance_classification`, `user_abstraction`, and `spot_usdc_available` so
runtime-generated prefilter samples can be analyzed with the same resolver
truth. The local fix synchronizes
`scripts/diagnose_live.py` and `scripts/analyze_production_blockers.py` with
that resolver: diagnose now queries `userAbstraction`, classifies
`withdrawable=0 + unifiedAccount + spot USDC available` as
`unified_collateral_available`, removes the old transfer-advice direction for
that case, and the exclude-strategy/exclude-liquidity offline view no
longer counts those samples as `account/admission` blockers. Runtime admission
reject classification now also recognizes official Hyperliquid final reject
statuses `perpMarginRejected` and `insufficientSpotBalanceRejected`, recording
them as exchange-truth insufficient-margin admission blocks instead of an
unknown error. V1 comparison
preserves the account-truth boundary: exchange/user state is queried with the
configured actual account address, while signer/API wallet identity is not
treated as account truth. Hyperliquid official docs confirm that info queries
must use the actual master/sub-account address and that `orderStatus` exposes
final reject truths such as `perpMarginRejected`.
Full evidence is recorded in
[`daily/2026-06-11.md#cluster-cl-069-hyperliquid-unified-collateral-diagnostic-closure`](daily/2026-06-11.md#cluster-cl-069-hyperliquid-unified-collateral-diagnostic-closure).

Latest code-side no-entry blocker attribution closure, 2026-06-11: the
previous deployment window had only one real open over roughly six to seven
hours, but after filtering strategy plus OI/liquidity blockers the remaining
code-side evidence was a mixed diagnostic surface rather than one decisive
open-blocking bug. The local fix adds a dedicated `code_side_blocker_view` to
`scripts/analyze_production_blockers.py` and optional diagnose output, grouping
only `code_data_freshness`, `ws_bbo_budget`, `exchange_truth_probe`, and
`order_truth_gap` evidence plus a separate `account/admission` bucket, while
separately reporting filtered strategy, liquidity, and open-interest counts.
It preserves V1/exchange semantics:
Bybit ACK-only is not a fill, invalid quotes remain fail-closed, stale quote
evidence still needs fresh WS-BBO or refresh proof, and best-effort bulk probe
timeouts do not poison the production acceptance gate. No strategy threshold,
OI/liquidity filter, WS-BBO budget/TTL, production config, order, restart, or
deployment was changed. Focused local verification passed the production
blocker analyzer slice (`11 passed`), diagnose code-side/nonblocking health
slice (`2 passed`), and passive-close ACK-only guard slice (`2 passed`).
Review hardening keeps disabled default views empty, counts
`runtime.snapshot_fallback_last_good`, and prevents total/breakdown double
counting.
`compileall` and `git diff --check` passed. GitNexus `detect-changes` reports
HIGH risk because analyzer/diagnose `main` flows are affected; no trading
runtime/order path was edited. The two planned broad regression suites were run
and still show existing runtime dispatch/quantity-metadata failures outside the
edited analyzer/diagnose/passive-close scope, so the closure is local and
deploy remains pending explicit authorization.
Full evidence is recorded in
[`daily/2026-06-11.md#cluster-cl-068-code-side-no-entry-blocker-attribution-closure`](daily/2026-06-11.md#cluster-cl-068-code-side-no-entry-blocker-attribution-closure).

Latest Hyperliquid exchange-truth account identity false-green, 2026-06-10:
production read-only evidence showed the configured Hyperliquid account still
had 18 nonzero positions, but V2 diagnose queried the signer/API-wallet address
and saw empty `assetPositions`, so deployment checks repeatedly reported a
false flat state. Root cause was credential identity drift: Hyperliquid account
data queries must use the actual master/sub-account address, while API/agent
wallets are only signers. V2 `_normalize_hyperliquid_credential()` overwrote an
explicit `account_address` with the signer-derived address outside the
`api_wallet` branch, and `diagnose_live.py` did not load
`LIGHTFEE_HYPERLIQUID_WALLET_MODE`, so service-env diagnostics could silently
drop the configured account identity. The fix preserves explicit Hyperliquid
account addresses, derives from the wallet only when no account is configured,
loads and normalizes wallet mode in diagnose, fails closed on account-wallet
signer/account mismatch, and exposes sanitized `credential_identity` with
masked/hash account and signer plus `account_matches_signer`. Local RED/GREEN
proved configured-account exchange truth, account-wallet mismatch fail-closed,
private-key alias wallet mode, and sanitized identity; related diagnose/health
gates report `104 passed`, full venue transport reports `401 passed`,
`git diff --check` passed, and GitNexus detect-changes reported low risk with
no affected processes. Cloud deployed through `c1381b3` with `7890949`
included: remote `git_head` and `.deploy_version` both read `c1381b3`,
services were active with `NRestarts=0`, the production verifier returned
`ok=true`, and `diagnose_live.py --json --since-deploy` returned `healthy` /
`risk=low` / `gate_passed=true`. The deployed diagnose now loads
`/etc/lightfee/lightfee.env`, reports
`credential_identity.wallet_mode=api_wallet`, account/signer identities
present, and `account_matches_signer=false`; current configured-account
Hyperliquid truth is flat/no-open-orders, so the previous 18-position exposure
is no longer present in the deployed acceptance window and is not hidden behind
signer-address false flatness.
Full evidence is recorded in
[`daily/2026-06-10.md#cluster-cl-065-hyperliquid-exchange-truth-account-identity-false-green`](daily/2026-06-10.md#cluster-cl-065-hyperliquid-exchange-truth-account-identity-false-green).

Latest pending-entry terminal no-fill maker-order owner retention, 2026-06-10:
production evidence showed local flat/no pending work while Bybit still had
non-reduce-only maker open orders for `SUSHIUSDT` and `MEUSDT`. The runtime had
first emitted deferred maker-open-order evidence, then later stale-abandoned
the same pending entries with `reason=both_venues_zero` and let recovery behave
as if only an evidence gap remained. Root cause was a remaining bypass in the
pending-entry terminality path: terminal/no-fill passive progress, such as
Bybit execution history `execution_not_found` represented as `canceled`, could
short-circuit before realtime open-order truth; abort cleanup had the same
`pending.maker_completed()` fast path. The local fix requires actual maker fill
for the maker-completed fast path, checks realtime open-order truth for
terminal no-fill progress, retains pending when a matching maker order exists
or open-order truth is unavailable, and allows flat abandon only when
open-order truth explicitly has no match. It also latches operator
fail-closed during abort fail-closed cleanup. Local verification passed
focused terminal no-fill branches (`6 passed`), recovery core/ledger
(`34 passed`), target regression (`609 passed`), compileall, and full pytest
`3778 passed`, `9 skipped`, `1 warning`. Follow-up PE-14 coverage now routes
V1 supervision stale-backlog clear through `PendingEntryTerminalizer`: only
zero-fill/resting/progress-absent/no-live-artifact may clear, while fills,
inflight hedge, cancel requests, non-resting/progress evidence, live artifacts,
or unavailable truth retain pending. Follow-up RED/GREEN reports
`4 passed`, the terminalizer suite reports `11 passed`, and the final full
suite reports `3782 passed`, `9 skipped`, `1 warning`. Runtime commit
`66a3688` was deployed and cloud verified: manifest critical files passed,
`verify_production_services.py --json` returned `ok=true` with no
critical/warning issues, `diagnose_live.py --json --since-deploy` returned
`healthy` / `risk=low` with `gate_passed=true`, all configured venues were
flat/no-open-orders, sidecar/live were active with `NRestarts=0`, and warning
logs were empty. No manual order submission, order cancellation, or
runtime-state edit was used.
Full evidence is recorded in
[`daily/2026-06-10.md#cluster-cl-064-pending-entry-terminal-no-fill-maker-open-order-owner-retention`](daily/2026-06-10.md#cluster-cl-064-pending-entry-terminal-no-fill-maker-open-order-owner-retention).

Latest WS BBO quote-evidence / snapshot-diagnose split, 2026-06-10: the
post-deploy 3-6 events were not service crashes and not proof that production
had switched back to Local L2. The root was twofold: entry candidate freshness
still let stale sidecar quote evidence block before consuming fresher WS BBO
evidence from the active `ws_bbo_quote_lease` provider, and
`diagnose_live.py` still counted `runtime.snapshot_stale` /
`runtime.snapshot_degraded` as `l2_evidence.stale_rebuild_count`. The local
fix adds source-aware WS BBO entry quote resolution, emits
`runtime.entry_quote_evidence_resolved_by_ws_bbo` for fresh same-venue/symbol
WS BBO overrides, keeps new entry fail-closed when sidecar quote is missing or
invalid, keeps stale sidecar quote fail-closed when WS BBO is missing/stale or
invalid, reprices one fresh WS BBO post-only would-cross maker quote before
blocking, records resolved quote metrics as fresh WS BBO evidence, and adds
separate `snapshot_evidence` so snapshot degraded/stale no longer appears as
Local L2 recurrence. Code commit `9e92718` was deployed and cloud verified:
critical runtime hashes matched local, sidecar/live were active, singleton
check passed, production services returned `ok=true`, since-deploy diagnose
returned `healthy` / `risk=low`, all exchange truth was flat/no-open-orders,
and the deploy-window journal warning/error scan had no entries. Full evidence
is recorded in
[`daily/2026-06-10.md#cluster-cl-063-ws-bbo-entry-quote-evidence-and-snapshot-diagnose-split`](daily/2026-06-10.md#cluster-cl-063-ws-bbo-entry-quote-evidence-and-snapshot-diagnose-split).

Latest post-deploy WS BBO admission, ACK-only diagnose, and active probe
follow-up, 2026-06-09: deployment-window events were not a service crash, not a
WS BBO quote-path trading failure, and not a fresh Local L2 recurrence. The
root was diagnostic/event ownership drift after the WS BBO mainline switch:
admission blockers such as Hyperliquid insufficient margin could still be
represented by or rebucketed through the old Local L2 / WS BBO selection
surfaces, and `diagnose_live.py` did not consume already-closed Bybit ACK-only
passive-close truth gaps when `accepted_order_truth_gap` registration,
reconciliation or terminal-flat evidence, and current exchange
flat/no-open-orders truth were all present. The original Bybit trading-terms
incident remains an account/symbol permission problem, but the new admission
precheck implementation also had an endpoint follow-up: cloud dry-run
reproduced HTTP 404 on the older `/v5/order/pre-check-order` path, and the
local fix uses current Bybit V5 `/v5/order/pre-check`. The local follow-up adds
`runtime.entry_blocked_admission_selection`,
`entry_admission_blocker_counts`, admission-first analyzer/audit rebucketing,
`resolved_order_truth_gap_summary`, resolved ACK-only filtering from active
`top_exchange_errors`, and lifecycle closure for mixed `entry.opened` /
`runtime.position_opened` evidence by exact key, symbol terminal evidence, or
high-confidence exchange flat/no-open-orders truth after residual/recovery
completion. Opened evidence without terminal/recovery proof still fails the
gate. Local verification passed focused RED/GREEN plus entry/WS-BBO,
diagnose/offline (`103 passed`), snapshot/passive-close, venues/startup,
active Bybit probe harness (`7 passed`), Bybit precheck/ACK-only focused
transport tests (`3 passed`), live-harness business-line, compile, and
GitNexus change-scope gates. Cloud read-only probes on the current deployed
`af40809` reproduced the old unhealthy diagnose interpretation, while the
patched `/tmp` diagnose replay against the same production event/state files
cleared `top_exchange_errors`, resolved the ACK-only truth gap, closed legacy
opened lifecycle keys by current exchange truth, and returned
`gate_passed=true` / `conclusion.status=healthy` without deploying or
restarting services. An independent DOGEUSDT active probe capped at 10 USDT
then reproduced Bybit ACK-only open and passive-close terminal flatness:
precheck accepted, open IOC returned ACK-only with accepted id and no fill
fields, live position truth proved the temporary long, passive reduce-only
close resolved to final flat, and post-probe production verification showed all
venues flat/no-open-orders with no pending-entry/passive-close/residual work.
Deployment accepted on `1894412`: remote `git_head` and `.deploy_version`
matched, `verify_production_services.py --json` returned `ok=true` with zero
critical/warning reports, and `diagnose_live.py --json --since-deploy` returned
`health.ok=true`, `top_exchange_errors=[]`, `gate_passed=true`,
`conclusion.status=healthy`, and all venue positions/open orders empty. Full
evidence is recorded in
[`daily/2026-06-09.md#cluster-cl-062-post-deploy-ws-bbo-admission-and-ack-only-truth-consumption`](daily/2026-06-09.md#cluster-cl-062-post-deploy-ws-bbo-admission-and-ack-only-truth-consumption).

Latest WS BBO mainline / Local L2 isolation closure, 2026-06-09: production
configuration already used `entry_readiness_provider="ws_bbo_quote_lease"`,
but WS BBO selection blockers still reused the historical
`runtime.entry_blocked_local_l2_selection` event name, and close/passive price
hints could still prefer Local L2 before WS BBO. The local fix keeps Local L2
runtime/data-plane/session code intact for explicit `local_l2` / `ws_top_book`
legacy modes and diagnostics, while moving the active WS BBO business chain to
`runtime.entry_blocked_ws_bbo_selection`, WS BBO-first close price evidence,
source-aware passive close labels, WS BBO-specific passive maker-leg
quote-evidence gap logs, non-blocking WS BBO quote-resolver missing/stale
evidence, and separate `entry_ws_bbo_*` no-entry diagnostics. Historical
old-name records carrying
`provider=ws_bbo_quote_lease` are now classified as WS BBO, not Local L2
recurrence, and dry-run audit text output prints WS BBO selection blockers
separately from Local L2. Local verification passed entry/WS BBO,
close/passive, diagnose/offline, entry execution gate, and live-harness
business-line slices.
Deployment accepted on `1894412`; the post-deploy diagnose window showed
admission blockers under `runtime.entry_blocked_admission_selection`, no
current Local L2 recurrence, a passing production acceptance gate, and all
venues flat/no-open-orders. Full evidence is recorded in
[`daily/2026-06-09.md#cluster-cl-061-ws-bbo-mainline-separates-from-local-l2-legacy-diagnostics`](daily/2026-06-09.md#cluster-cl-061-ws-bbo-mainline-separates-from-local-l2-legacy-diagnostics).

Latest live-artifact account-truth recovery release, 2026-06-09: post-`b2d0706`
deploy proved the previous fail-closed guard fix was still not enough. Current
production account truth was flat/no-open-orders on every venue, but exported
state still carried the old `recovery_blocked_reason=unpaired_live_position`.
Runtime was attempting to release that live-artifact blocker through a dirty
candidate-symbol sweep, so historical unsupported symbols created evidence gaps
even though diagnose had already proved account-level truth clean. The deployed
follow-up in `78a0feb` adds account-truth recovery ledger refresh using
`fetch_all_positions` plus unfiltered open-order truth, and routes old
`unpaired_live_position` cleanup through that evidence source. Live open orders
and open-order truth errors still block release. Cloud acceptance on `78a0feb`
passed verifier and diagnose with `lifecycle=running`, `risk_mode=running`,
`recovery_blocked_reason=null`, all venues flat/no-open-orders, and
`production_acceptance_gate.gate_passed=true`. Full evidence is recorded in
[`daily/2026-06-09.md#cluster-cl-060-live-artifact-recovery-release-uses-account-truth`](daily/2026-06-09.md#cluster-cl-060-live-artifact-recovery-release-uses-account-truth).

Latest recovery fail-closed release latch follow-up, 2026-06-09: post-`3a59a53`
deploy verification showed the business exchange-truth gate clean
(`gate_passed=true`, all venues flat/no-open-orders), but top-level health still
failed with `lifecycle=risk_only`, `risk_mode=fail_closed` while
`V1RecoveryDecisionCore=RUNNING_CLEAN`. This is distinct from the prior CL-057
`risk_only/running` stale lifecycle case: the fail-closed risk latch itself was
still present after the recovery block reason had gone. The deployed follow-up
extends the same core-clean + flat-position + empty-open-order release helper to
allow stale `fail_closed` through `clear_risk_mode_for_recovery()`, while still
preserving operator fail-closed, reduce-only/entry-paused risk modes, local
recovery work, live position truth, and live open-order blockers. Cloud
acceptance on `78a0feb` proved the stale fail-closed latch cleared. Full
evidence is recorded in
[`daily/2026-06-09.md#cluster-cl-059-recovery-fail-closed-release-latch-after-core-clean-truth`](daily/2026-06-09.md#cluster-cl-059-recovery-fail-closed-release-latch-after-core-clean-truth).

Latest Bybit trading-terms pre-entry guard, 2026-06-09: production `CLUSDT`
showed OKX maker / Bybit hedge failing on Bybit `110125` crude-oil trading
terms. The same deploy window had healthy Bybit order-path behavior on other
symbols, so the root is deterministic symbol/account trading-terms permission,
not main order endpoint, timestamp, signature, or category drift. A later active
probe dry-run found the new admission guard's endpoint path was stale:
`/v5/order/pre-check-order` returned HTTP 404, while current Bybit V5 precheck
uses `/v5/order/pre-check`. The local follow-up corrects that endpoint before
non-reduce-only Bybit entry exposure when the live adapter supports it,
classifies `110125/110126/110123` through the existing admission contract, emits
`runtime.entry_admission_blocked source=pre_entry_bybit_precheck`, and prevents
maker dispatch before a known terms-blocked hedge. Pending-hedge admission
handling remains as defense in depth, and reduce-only close/cancel/passive/
residual paths are unchanged. This deployment was accepted as a full
business-line gate, not a Bybit-only gate: entry admission, pending-entry
terminality, passive-close ACK-only terminality, residual ACK-only closure,
recovery lifecycle release, and Aster V3 private-truth startup safety stayed
green in local tests and post-deploy read-only production diagnostics.
Full evidence is recorded in
[`daily/2026-06-09.md#cluster-cl-058-bybit-trading-terms-pre-entry-admission-precheck`](daily/2026-06-09.md#cluster-cl-058-bybit-trading-terms-pre-entry-admission-precheck).

Latest recovery current-state release latch closure, 2026-06-09: after the
prior deploy, production exchange truth was high-confidence flat/no-open-orders
and `V1RecoveryDecisionCore` evaluated `RUNNING_CLEAN`, but runtime
`lifecycle=risk_only` remained latched while `risk_mode=running`. The root
cause was a missing V1 current-state release loop after pending-entry/passive
maker work had already terminalized, plus an older `unpaired_live_position`
path that synthesized `open_orders=[]` from position-flat evidence. The fix in
`62dee61`, deployed through the latest main manifest line, adds a narrow
stale-lifecycle release helper that only clears on current
`RUNNING_CLEAN` plus available flat position truth and empty open-order truth,
routes pending-entry terminal removal/startup/tick recovery back through the
recovery ledger/core, and makes `unpaired_live_position` use bounded symbol
open-order truth instead of synthetic empties. `diagnose_live.py` now reports
`lifecycle_release_not_applied` when current core/exchange truth is clean but
runtime lifecycle remains `risk_only`. Local verification passed focused
RED/GREEN and recovery/pending/residual regression scope (`311 passed`), and
the deployed main line reached running/flat/no-open-orders acceptance before
the Bybit precheck follow-up. Full evidence is recorded in
[`daily/2026-06-09.md#cluster-cl-057-recovery-current-state-release-latch-and-open-order-truth-gap`](daily/2026-06-09.md#cluster-cl-057-recovery-current-state-release-latch-and-open-order-truth-gap).

Latest production evidence contract split, 2026-06-08: CL-055 already closed
the core trading risk for issue 2/3; this follow-up separates the diagnostic
surface so healthy deploy windows are not misread as unclosed recovery risk.
No-work bulk position timeout now emits
`recovery.live_position_bulk_diagnostic_error` as nonblocking health evidence.
Required recovery bulk timeout with bounded fallback now emits
`recovery.required_position_bulk_fallback_planned`; only missing fallback or
fallback failure emits the blocking
`recovery.required_position_truth_unavailable`. Entry quote stale blockers are
now admission-filtered candidate-leg scoped, while whole-snapshot stale quote
noise becomes a rate-limited nonblocking health summary.
`diagnose_live.py --since-deploy` classifies `blocking_required_truth`,
`contained_admission`, and
`nonblocking_health_diagnostic` separately so Hyperliquid admission cooldowns
and full-snapshot health noise do not mask required recovery blockers. Local
verification passed focused RED/GREEN and related recovery/residual/admission/
snapshot/entry/diagnose/order-uncertainty regression scope. Full evidence is
recorded in
[`daily/2026-06-08.md#cluster-cl-056-production-error-evidence-contract-split`](daily/2026-06-08.md#cluster-cl-056-production-error-evidence-contract-split).
Cloud deploy of `c44be73` fast-forwarded `/opt/lightfee-v2`, wrote
`.deploy_version=c44be73`, passed deploy-manifest and remote compileall checks,
restarted sidecar/live active/running with `NRestarts=0`, and passed singleton,
`verify_production_services.py --json`, and
`diagnose_live.py --json --since-deploy` with healthy/running state,
`gate_passed=true`, required truth/residual/open/pending counts all zero, and
high-confidence flat/no-open-orders exchange truth on all venues.

Latest recovery truth-probe and residual-repair ACK-only closure, 2026-06-08:
production issue 2 (`recovery.live_position_bulk_probe_error`) and issue 3
(`recovery.residual_repair_failed` ACK-only) were closed as one recovery-truth
chain, not as venue-specific one-off patches. Startup/recovery live-position
truth now treats full-position bulk fetch as diagnostic/supplemental: whenever
pending residual/passive/pending-entry work requires truth, a bulk timeout must
fall through to bounded symbol fallback for only recovery-owned/truth-required
symbols, including OKX, and failed fallback becomes
`truth_unavailable_for_required_recovery` / risk-only evidence rather than
false flat. Residual repair now shares the accepted-order uncertainty resolver:
ACK-only reduce-only responses immediately reconcile execution/order/open-order/
position truth, complete on confirmed fill or live-flat/no-open-orders, and
retain accepted ids for the next tick when an open order or truth gap remains
instead of submitting another reduce-only blindly. Local verification passed
focused RED/GREEN recovery and residual harnesses, related regression suites,
full pytest (`3726 passed`, `9 skipped`, `1 warning`), diff-check, and
GitNexus detect-changes at low indexed risk; `runtime.py` remains a GitNexus
large-file skipped path, so runtime call impact was manually reviewed. Cloud
deploy of `4b02ec2` fast-forwarded `/opt/lightfee-v2`, passed deploy-manifest
and compileall checks, restarted sidecar/live active/running with
`NRestarts=0`, and post-deploy verification reported singleton PASS,
`verify_production_services.py --json` `ok=true` with zero critical/warnings,
and `diagnose_live.py --json --since-deploy` healthy/running with
high-confidence flat/no-open-orders exchange truth on all venues. Full evidence
is recorded in
[`daily/2026-06-08.md#cluster-cl-055-recovery-bulk-timeout-and-residual-ack-only-truth-closure`](daily/2026-06-08.md#cluster-cl-055-recovery-bulk-timeout-and-residual-ack-only-truth-closure).

Latest Aster V3 startup safety fix, 2026-06-08: cloud deploy to `e63d70e`
passed manifest/compileall and wrote `.deploy_version=e63d70e`, but
`lightfee-live.service` entered `activating/auto-restart` because Aster V3
startup attempted to derive a signer from the configured legacy secret and
raised `ValueError: failed to derive aster signer from private key`. The root
fix in `9d037f5` makes `credential_has_aster_v3_signer()` deterministic,
allows Aster public live transport to start when private signing is invalid,
and makes Aster private truth/order methods fail closed with explicit
auth-failure evidence instead of crashing startup or returning false empty
truth. Cloud deploy of `9d037f5` passed manifest/compileall, restarted sidecar
and live active/running with `NRestarts=0`, and settled
`diagnose_live.py --json --since-deploy` reported `health.ok=true`,
`lifecycle=running`, `risk_mode=running`, `version_mismatch=false`, and
`production_acceptance_gate.gate_passed=true`. Full evidence is recorded in
[`daily/2026-06-08.md#cluster-cl-053-aster-v3-invalid-signer-startup-crash`](daily/2026-06-08.md#cluster-cl-053-aster-v3-invalid-signer-startup-crash).

Latest production issues 3-11 root-closure evidence hardening, 2026-06-08:
issues 1-2 remain explicitly out of scope. The existing V1/exchange-semantics
fixes for admission, pending hedge, recovery ledger, pending close, shared
ACK-only order uncertainty, OKX amend fallback, and snapshot degraded handling
were not duplicated. This pass only closed the remaining deterministic evidence
gaps needed for deployment review: live mismatch flatten success/failure now
records cleanup intent and post-cleanup read-only position truth; pending-entry
passive maker rest-timeout cancel records order identity and that cancel ACK is
not terminal; structural perp open-interest degraded events record endpoint,
floor/current value, fallback source, and targeted revalidate scope. Local
verification passed focused RED/GREEN, relevant suites, affected file suites,
compileall, diff-check, full pytest (`3711 passed`, `9 skipped`, `1 warning`),
and GitNexus detect-changes (`risk=low`, affected processes `0`). This was then
deployed on `89e2b93` and verified `ok=true` by both
`scripts/verify_production_services.py --json` and
`scripts/diagnose_live.py --json --since-deploy`:
`health.ok=true`, `version_mismatch=false`, `lifecycle=running`,
`risk_mode=running`, all counts zero, and no exchange-truth mismatches.
Full evidence is recorded in
[`daily/2026-06-08.md#cluster-cl-052-production-issues-3-11-root-closure-evidence-hardening`](daily/2026-06-08.md#cluster-cl-052-production-issues-3-11-root-closure-evidence-hardening).

Latest post-deploy issue 1/4-9 closure, 2026-06-08: follow-up review excluded
issue 3 and issue 11, kept existing entry admission coverage for Bybit `110007`
and Hyperliquid venue-scope blockers, and closed the remaining health plus
ACK-only close/repair evidence gaps without widening trading behavior. Runtime
health now suppresses only the `live_tick_stale` fingerprint when local state is
clean, exchange truth is available and flat/no-open-orders, and recent scan
progress proves the loop is still running; non-high exchange truth remains
visible as `exchange_truth_confidence_not_high`. Passive-close hedge submit
errors and residual repair submit errors now share accepted-order/no-fill
order-truth-gap evidence with pending entry (`order_ack_only`, accepted ids,
missing fill fields, probe paths, and next action). OKX passive amend endpoint
HTTP 405 / code `50115` / `Invalid request type` routes through existing
cancel-replace while retaining the double-order guard. The local closure is
recorded in
[`daily/2026-06-07.md#2026-06-08-local-follow-up-health-and-ack-only-closerepair-evidence`](daily/2026-06-07.md#2026-06-08-local-follow-up-health-and-ack-only-closerepair-evidence),
[`cards/passive-close-terminal-flatness.md`](cards/passive-close-terminal-flatness.md),
[`cards/residual-repair-live-truth.md`](cards/residual-repair-live-truth.md),
and
[`cards/pending-entry-terminality-live-truth.md`](cards/pending-entry-terminality-live-truth.md).
Boundaries stayed closed: no signing, CID, sizing, strategy threshold, close
executor, cancel, residual clear, or recovery flatten execution logic changed.

Latest passive-close live-flat recurrence, 2026-06-07: post-`bff33ec`
production is not failing because the deploy is mismatched or services failed
to restart. The live red path is a connected recovery-contract recurrence:
`BABYUSDT` is locally open/pending passive close while OKX/Bybit exchange truth
is flat, and passive-close cleanup repeats
`runtime.passive_close_tick_error` with the error string
`"'dict' object has no attribute 'append'"`. Source tracing maps this to a restored
`pending_close_reconciliations` shape hole plus a cleanup atomicity hole:
terminal-flat/drift events are journaled before queue registration and managed
state removal can succeed, so the V1 recovery decision core never receives the
successful clear evidence. Focused probes also found unowned Bybit live
positions on `MORPHOUSDT`, `MONUSDT`, and `SEIUSDT`; those are the same
live-artifact ownership contract family, not a separate deploy issue. Root plan
and required RED coverage are recorded in
[`daily/2026-06-07.md#cluster-cl-051-post-bff33ec-passive-close-live-flat-cleanup-re-entry`](daily/2026-06-07.md#cluster-cl-051-post-bff33ec-passive-close-live-flat-cleanup-re-entry),
[`cards/passive-close-terminal-flatness.md`](cards/passive-close-terminal-flatness.md),
and
[`contracts/pending-entry-live-truth-contract.md`](contracts/pending-entry-live-truth-contract.md).
Formal execution artifacts are
[`../superpowers/specs/2026-06-07-v1-pending-close-reconciliation-queue-closure-design.md`](../superpowers/specs/2026-06-07-v1-pending-close-reconciliation-queue-closure-design.md)
and
[`../superpowers/plans/2026-06-07-v1-pending-close-reconciliation-queue-closure-implementation-plan.md`](../superpowers/plans/2026-06-07-v1-pending-close-reconciliation-queue-closure-implementation-plan.md).
This update is local implementation and local tests only: no deploy, no
production verification, no production state mutation, and no order/cancel is
claimed.

Latest V1 recovery decision core closed-loop implementation, 2026-06-06: the
`ambiguous_exchange_truth -> runtime.stale_recovery_block_cleared` pattern is
now handled as a single-authority recovery semantics gap, not as another
single-condition bug. A pure `V1RecoveryDecisionCore` owns evidence
classification, ownership resolution, recovery work planning, lifecycle, entry
policy, diagnostic severity, and block/clear decisions. The boundary keeps
normal entry available for flat/no-local-work evidence gaps, while still
blocking/managing concrete live artifacts, local recovery work, operator
fail-closed, and unresolved pending/passive/residual/open work. Post-review
closure tightened live-artifact ownership so same-symbol evidence is not enough
to own an unmatched live position or unrelated maker order; diagnose also treats
count-only pending/residual/passive state as required recovery work when truth is
unavailable. The spec and execution plan are recorded in
[`../superpowers/specs/2026-06-06-v1-recovery-decision-core-closed-loop-design.md`](../superpowers/specs/2026-06-06-v1-recovery-decision-core-closed-loop-design.md)
and
[`../superpowers/plans/2026-06-06-v1-recovery-decision-core-closed-loop-implementation-plan.md`](../superpowers/plans/2026-06-06-v1-recovery-decision-core-closed-loop-implementation-plan.md).
Local closure commit `a89d64d` passed focused suites, compileall, diff-check, and
full pytest; deploy-manifest commit `41ab11e` was pushed and deployed to
`/opt/lightfee-v2`. Remote manifest verification and compileall passed, both
services restarted active/running with `NRestarts=0`, and `.deploy_version`
matches `41ab11e`. Production acceptance is still blocked: verification from the
shell lacks credentialed exchange truth, the core reports
`RUNNING_WITH_EVIDENCE_GAP` with `entry_allowed=true` and no block reason, and a
later post-deploy check observed `lifecycle=risk_only` after the restarted live
runtime submitted and then recovery-flattened live Bybit legs. No manual order,
cancel, or runtime-state edit was performed outside the deploy/restart.

Follow-up closure after the post-deploy `risk_only`, 2026-06-06: the remaining
gap was not a bad core decision, but a missing return edge into the core after
side effects. Production showed `exchange_truth_recovery_ledger_blocked` for an
unpaired Bybit `WLDUSDT` live position, then successful
`recovery.live_mismatch_flattened` events, but no `recovery.ledger_clear`.
Runtime now refreshes recovery-ledger exchange truth for the live mismatch
symbols immediately after successful startup/runtime mismatch flatten, so the
same `V1RecoveryDecisionCore` clears the core-owned blocker. RED/GREEN coverage
protects both startup and runtime clean-live-position paths; focused suites,
compileall, diff-check, and full pytest (`3592 passed`, `9 skipped`, `1
warning`) passed before deployment.

Final root-closure review before the next main/cloud deploy, 2026-06-06:
unconditional stale cleanup is removed as an authority, passive-close live-flat
legacy cleanup routes through `V1RecoveryDecisionCore`, and evidence-gap clear
is narrowed so `RUNNING_WITH_EVIDENCE_GAP` cannot clear live-artifact blockers
such as `orphan_maker_order` or `unpaired_live_position`. Managed local open
positions continue normal V1 trading under a probe gap unless explicit
truth-required/recovery-required evidence marks them as recovery work. Latest
local verification passed focused core/pending-entry checks (`14 passed`), full
pytest (`3602 passed`, `9 skipped`, `1 warning`), compileall, diff-check, and a
fresh GitNexus detect-changes audit in the intended recovery/runtime scope.
Cloud redeploy then fast-forwarded `/opt/lightfee-v2` to the final main closure:
manifest verification and compileall passed, sidecar/live restarted as
singletons with `NRestarts=0`, credentialed `.venv` diagnose reported
high-confidence flat/no-open-orders on all seven venues, local open/pending
counts zero, `RUNNING_CLEAN`, `gate_passed=true`, and deploy-window
`recovery.ledger_clear=1`. The only remaining non-green signal is the default
15s verifier tick threshold, while observed live-state ticks advance about every
25s and the 60s verifier is green; track that as production-health cadence, not
as recovery-core semantics.

Latest pending-entry passive opening source-port closure, 2026-06-05: the
post-quick-flat pending-entry work has been re-centered on V1 source functions
rather than incident patches. The required matrix now classifies current hunks
and has no `missing`, `pending-audit`, or `replace-current` rows. Source-named
helpers in `lightfee/engine/pending_entry_lifecycle.py` own phase-state
construction, passive progress, zero-fill cycle recording, high-to-low and
low-to-dual transitions, terminal fallback decisions, passive-cycle acceptance,
and remainder-repost decisions. `LiveRuntime` keeps Python adapter IO only.
The post-review drift fixes also make terminal fallback derive ForceStandard
sizing/price hints from the rechecked frozen candidate, persist and apply V1
passive-order `fill_checkpoint` for repost progress, and run passive submit /
repost through the V1 post-only loop: retryable rejects now wait, freeze maker
venue request budget on rate-limit evidence, refresh the passive market
snapshot, reprice with best-quote/tick/edge/inventory semantics, and exhaust
instead of submitting a stale maker price when price evidence is missing.
Accepted boundary adaptations are recorded as
[`DEV-003`](../parity/approved_deviations.md#dev-003-pending-entry-python-runtime-boundaries).
Local verification for this closure passed focused pending-entry parity,
incident regressions, the broad plan regression suite, `compileall`,
full pytest (`3575 passed`, `9 skipped`, `1 warning`), `git diff --check`,
and GitNexus `detect-changes` at medium risk. This is local source-port
acceptance only: no orders, cancels, production state mutation, deploy, or
production acceptance is claimed.

Latest V1 trading lifecycle core local follow-up, 2026-06-05: entry,
pending-entry, recovery, close-facing funding facts, and quick-flat
observability now route through a shared lifecycle semantic surface instead of
symbol-specific branches. The local implementation added shared
`FundingLifecycle` facts, a pure `V1TradingLifecycle` facade, selection and
dispatch lifecycle gates, pending-entry zero-fill viability gating, sanitized
quick-flat replay coverage, and duplicate `exit.closed` de-duplication in
offline/diagnose summaries. Task 10 found no duplicate runtime first-funding
horizon branch to remove; the remaining `60_000` literals were unrelated
diagnostic/rate-limit or V1 finalization/prewarm timing, so that task was
accepted as a no-op/mismatch inventory rather than an unsafe broad cleanup. New
contract rows `LC-01` through `LC-03` document the shared lifecycle decisions.
This pass is local evidence only: no orders, cancels, runtime-state edits,
commit, deploy, or production acceptance claim.

Latest exchange-truth recovery ledger V1 parity follow-up, 2026-06-05: the
post-contract `TRXUSDT` and `SEIUSDT` evidence is now handled as one V1-style
runtime recovery-ledger boundary, not another CL-specific patch. V2 now has a
pure `RecoveryLedger`, shared exchange-truth normalizer, recovery owner index,
pending-entry terminalizer, runtime recovery-ledger refresh helper, runtime
entry gate, shared post-terminal pending-entry removal helper, and production
health classification for local-flat/live-open-order mismatches. Sanitized
fixtures prove the current `TRXUSDT` Bybit non-reduce live open maker order
maps to blocking `orphan_maker_order`, and positive-fill local false-flat
evidence maps to blocking recovery work rather than proven-flat. Focused tests
passed: core ledger/owner/truth/terminalizer `27 passed`,
startup/runtime/passive-close `359 passed`, diagnose/health `66 passed`, and
pending-entry parity `35 passed`. Full pytest passed with `3479 passed`,
`9 skipped`, `1 warning`; compileall and `git diff --check` passed. GitNexus
staged detect-changes reported medium risk across 23 files, 47 symbols, and 3
affected verifier flows. This pass did not submit orders, cancel orders, edit runtime
state, or deploy to cloud. It is documented in
[`daily/2026-06-05.md#contract-follow-up-exchange-truth-recovery-ledger-v1-parity`](daily/2026-06-05.md#contract-follow-up-exchange-truth-recovery-ledger-v1-parity).

Latest pending-entry V1 full-loop parity follow-up, 2026-06-05: post-contract
review after the latest deployment did not create a new independent CL bug. The
observed pending churn maps to the unified
[`pending-entry-live-truth-contract`](contracts/pending-entry-live-truth-contract.md):
Hyperliquid insufficient-margin retries are `PE-11`, same-symbol venue-overlap
pending protection is `PE-17`, deferred-finalizer caller retention is `PE-18`,
and force-terminal zero-fill finalizer routing is `PE-13`. V2 now blocks
Hyperliquid account/venue margin recurrences with a venue cooldown before maker
submit, matches V1 `pending_entry_protection` for same-symbol candidates sharing
either venue, makes `_finalize_pending_entry()` return terminality status, and
requires callers to retain/backoff when finalization defers. Focused local
verification passed: runtime/admission `51 passed`, pending-entry parity
`35 passed`, and record-layer pending protection `1 passed`, `27 deselected`.
Adjacent gates passed (`355 passed`; `49 passed`), compileall and diff-check
passed, and full pytest passed with `3447 passed`, `9 skipped`, `1 warning`.
Cloud fast-forwarded to `3af002d`, wrote `.deploy_version=3af002d`, passed the
manifest gate and remote compileall, and restarted sidecar/live active/running
with `NRestarts=0`. Production acceptance is not closed: service-env
`diagnose_live.py --json --since-deploy` returned `status=unhealthy`,
`risk=high`, and blocker `exchange_truth_open_orders_present` because Bybit has
one non-reduce-only `TRXUSDT` open maker order
`a84df707-efb3-4e40-bab1-641a4eb0f3d4` for `72.0` at `0.33044` while local
open/pending state is flat. No manual order/cancel/runtime-state mutation was
performed.
This follow-up is documented in
[`daily/2026-06-05.md#contract-follow-up-pending-entry-v1-full-loop-parity`](daily/2026-06-05.md#contract-follow-up-pending-entry-v1-full-loop-parity).

Latest recovery bulk-probe timeout evidence follow-up, 2026-06-04:
`recovery.live_position_bulk_probe_error` timeout is not root-fixed because the
available evidence is insufficient to distinguish exchange/network transport
noise from a local budget/concurrency defect. The runtime now records endpoint,
timeout budget/source, per-venue vs global timeout trigger, global-budget
applied/triggered state, concurrency limit, batch index/count, requested symbol
scope, and probe queued/start/finish/elapsed timings. This is intentionally an
observability-only change: future occurrences only become code work if the new
fields prove exchange truth became unavailable or pending/residual cleanup
could not close; otherwise update evidence/docs without changing runtime
behavior.

Latest pending-entry live-truth contract closure, 2026-06-04: CL-048, CL-049,
and CL-050 are now one contract family governed by
[`pending-entry-live-truth-contract`](contracts/pending-entry-live-truth-contract.md),
not three independent root-fix tracks. Each recurrence maps into the matrix:
CL-048 covers retained rejected positive fill recovery (`PE-05`, `PE-06`,
`PE-07`, `PE-09`, `RC-01`), CL-049 covers live open-order terminality and
diagnose gate alignment (`PE-02`, `PE-11`, `DG-02`), and CL-050 covers live
maker position zero-fill terminality plus duplicate cleanup convergence
(`PE-03`, `RC-07`, `RC-08`, `DG-01`). The contract closure is deployed through
`68a979b`; production `verify_production_services.py --json` returned `ok=true`
with zero local open/pending/close/residual work, and service-env
`diagnose_live.py --json --since-deploy` returned healthy/high-confidence
flat/no-open-orders truth across all seven venues. Future production issues in
this family must be mapped to the matrix first. If the row is already covered
and cloud truth is flat, update evidence/docs only; if not, add a RED test for
the uncovered row before changing runtime code. The same contract now records
V1 as the coverage floor, including terminalization budget, terminal
taker-fallback/repost/cooldown, supervision backlog clear, live balanced
hydration, ambiguous-live fail-closed behavior, and recovery-work lifecycle
semantics. Rows `PE-12` through `PE-16` are the current V1 completeness audit
surface; any future production evidence must map there before runtime edits.

Historical CL-049 contract recurrence, 2026-06-04: after the CL-048 deployment
was healthy, read-only diagnose found local state flat/running while Bybit still
had non-reduce-only maker order
`d792a623-d9e4-4c20-905f-f76a8f2efaeb` open for `451.0` `SEIUSDT` at
`0.05315`. This maps to matrix rows `PE-02`, `PE-11`, and `DG-02`. The deployed
fix queries live maker open-order truth before zero-fill pending finalization,
retains/backoffs pending when a matching maker order exists, classifies
Hyperliquid insufficient margin as deterministic admission evidence, and keeps
diagnose from reporting healthy when production acceptance has blockers. Cloud
fast-forwarded to `1e082d9`, manifest and focused tests passed, services were
active/running with `NRestarts=0`, and final service-env diagnose was
healthy/high-confidence flat/no-open-orders across all seven venues.

Historical CL-048 contract recurrence, 2026-06-04: cloud fast-forwarded to
`9e93c90` and restarted sidecar/live active with `NRestarts=0`, but production
acceptance failed because credentialed exchange truth found a Bybit `SEIUSDT`
long position (`455.0`) while local runtime had `open_positions=[]`,
`pending_entry_count=1`, and `lifecycle=risk_only`. This maps to matrix rows
`PE-05`, `PE-06`, `PE-07`, `PE-09`, and `RC-01`: retained rejected positive
fill recovery must finalize matched state plus residual cleanup instead of
looping in local false-flat/risk-only.

Latest diagnostic/Bybit time-window follow-up, 2026-06-04: two post-deploy
concerns were split away from the order/close main-loop fight. First,
`snapshot_fallback_blocking=insufficient_evidence` was a diagnose/acceptance
classification bug: candidate-scoped fallback evidence was counted as
insufficient global evidence unless `v1_parity_evidence` was explicitly
present. CL047 now treats candidate/domain/venue/age scoped fallback as
`v1_parity`, while global fallback without scope still blocks as insufficient
evidence. Second, Bybit `10002` is an official timestamp/`recv_window` window
failure. V2 already used server time and the V1 1500ms Bybit auth backoff, but
it parsed second-level `timeSecond` before millisecond `time` / `timeNano` and
did not retry the 200-envelope `retCode=10002`. CL047 now preserves millisecond
server-time precision, clears cached offset and re-signs once on Bybit 10002,
and emits `server_time_path`, `auth_timestamp_ms`, `recv_window_ms`, status,
path, and body evidence if the refreshed retry still fails. Local verification:
diagnose suite `41 passed`, transport suite `392 passed`, full pytest
`3432 passed`, `9 skipped`, `1 warning`, compileall/diff-check clean, and
GitNexus detect-changes critical as expected because private REST transport is
a shared entry point. Cloud code deployment completed, and the later
pending-entry live-truth contract closure at `68a979b` closed the production
acceptance blocker.

Latest Bybit entry/passive-close deadline follow-up, 2026-06-04: phone
screenshots showed Bybit-related abnormal close samples including `MEUUSDT`
opened around `2026-06-03 20:12`, seconds-long closes, and one position with
more than eight hours of exposure. The root was not a single Bybit-only leg:
entry and close are dual-leg workflows, so Bybit maker/fill evidence must be
matched against the opposite venue before V2 can finalize or close. CL046
copies V1 semantics in two places. Pending-entry finalize now defers
zero-balanced finalization unless maker/order evidence is terminal no-fill, and
later stale zero reconciliation no longer erases a previously confirmed
positive fill. Passive close now has the V1 settlement-force fallback guard:
overdue pending passive closes are armed into DUAL_TAKER, retry delay is
cleared, and hedge/fallback hard breaches enter fail-closed with compensation
and live-flat probing instead of continuing retry backoff. The follow-up
mutual-fight fix makes maker-leg live-flat truth terminal for the current
passive-close drive cycle and makes runtime drift correction stand down while
passive-close live action is settling. Local verification is green: passive
close suite `111 passed`, startup preflight `67 passed`, runtime entry /
passive-close / live incident focused gates passed, and full pytest
`3432 passed`, `9 skipped`, `1 warning`; compile, diff-check, and GitNexus
detect-changes also ran. Cloud deployment and credentialed flat/no-open-orders
acceptance for the pending-entry contract family were later closed at
`68a979b`. Watch
`pending_entry.finalize_deferred_unresolved_maker_zero_fill`,
`pending_entry.finalize_fill_reconciliation_ignored_stale_zero`,
`runtime.passive_close_deadline_fallback_armed`,
`exit.passive_close_live_truth_one_sided_flatten_submitted`,
`exit.passive_close_live_truth_settling_preserved`,
`execution.hedge_deadline_breached`, and
`execution.close_deadline_breached`.

Latest pending-entry maker terminality follow-up, 2026-06-03: post-`bd12acd`
production is currently green and flat/no-open-orders across all configured
venues, but the deploy window showed a CL039 recurrence variant. Two `MEUSDT`
Bybit non-reduce-only PostOnly maker orders remained open after local pending
state was absent, then the runtime later self-cleaned through
`entry.cleanup_leg_exposure`, duplicate-client-id reconciliation, and
`recovery.live_mismatch_flattened`. The root split is: a stubborn maker
terminality bug where V2 applied live-position quantity as maker progress while
order status was uncertain/execution-not-found; a V2/V1 semantic drift where
live-position truth was treated as passive-maker terminality instead of
requiring order/fill evidence; an OKX exchange-parameter variant where a
synthetic recovery id was sent as `ordId` and returned `51000 Parameter ordId
error`; and remaining evidence needs around Bybit execution/open-order history.
Local and short-window cloud verification are green: maker-leg position-derived
pending progress is now gated behind filled/terminal reconciliation evidence,
runtime emits deferred-progress evidence when maker terminality is unproven,
and OKX synthetic/client ids route through `clOrdId`. Full local pytest passed
(`3392 passed`, `9 skipped`, `1 warning`), compileall and diff-check passed.
Cloud fast-forwarded to `9d580d5`, wrote `.deploy_version=9d580d5`, passed
remote manifest, compileall, focused remote suite (`86 passed`), restarted both
services active/running with `NRestarts=0`, and post-deploy health is green.
Credentialed all-account truth showed all 7 venues have nonzero positions `0`
and open orders `0`. Remaining evidence is historical only: collect Bybit
order/execution-history around the two pre-fix MEUSDT order ids/client ids.

Latest entry perp-liquidity qualification follow-up, 2026-06-03: the V1/V2
entry audit found a persisted-only V2 drift. V1 has
`EntryLiquidityQualificationState`: per-leg open interest below the configured
floor blocks entry, low volume is advisory, three repeated OI failures become
`structural_ineligibility`, structural rows suppress for 30 minutes, and probes
are rate-limited to 60 seconds. V2 already carried
`entry_liquidity_qualification_records` and sidecar quote fields
`volume_24h_quote` / `open_interest`, but runtime never consumed those records
or applied the V1 hard gate. CL042 adds V1 threshold defaults and venue overrides
to `StrategyConfig`, supports map-based and legacy `entry_min_perp_*` config
fields, adds a V1-compatible qualification state machine, and wires the live
candidate filter to record qualification results only in the real filtering
path. Diagnostic/scope calls remain read-only so observability cannot increment
failure counters. RED/GREEN proved missing defaults, ignored config aliases,
missing state module, fresh quote with low OI being incorrectly dispatched, and
persisted structural rows being ignored. Focused and adjacent gates passed:
config/defaults/state/runtime RED set `9 passed`, runtime snapshot freshness
`22 passed`, snapshot fallback harness `3 passed`, runtime entry flow
`32 passed`, entry-local-L2 `138 passed`, startup preflight `66 passed`,
exchange admission harness `12 passed`, sidecar schema/source/lifecycle/candidate
identity `42 passed`, venue market-data/transport `415 passed`, full pytest
`3390 passed`, `9 skipped`, `1 warning`, compileall, and diff-check. Cloud
`/opt/lightfee-v2` then fast-forwarded `c63ec7d..0c8356e`, wrote
`.deploy_version=0c8356e`, regenerated the manifest, passed the remote focused
entry/config/snapshot/startup/admission suite (`321 passed`), compileall, and
all 14 critical manifest checks. Both services restarted active/running with
`NRestarts=0`; post-deploy diagnose showed `health.ok=true`, local
open/pending/close `0/0/0`, Local-L2 missing/stale/sequence-gap `0/0/0`,
`execution.entry_liquidity_blocked=300`, no entry/open-position events, and
`version_mismatch=false`. Exchange truth was unavailable in that invocation, so
consistency confidence stayed low. No WS BBO, Local-L2 lifecycle/data-plane,
order submit, sizing, close, recovery, or residual-repair behavior changed.

Latest entry final gate follow-up, 2026-06-03: V1 has a final execution skew
gate for dual HOT Local-L2 books: when both execution books are ready but their
`observed_at_ms` values diverge beyond `entry_final_gate_max_skew_ms`, V1 blocks
entry with `ExecutionSkew`. V2 had the config value but did not apply the final
gate in `_dispatch_entry`, so a candidate could dispatch with two individually
fresh but mutually stale-skewed books. CL041 adds a narrow local-L2-only final
gate in `LiveRuntime`: after both execution-L2 books pass readiness, it compares
the two observed timestamps and emits `runtime.entry_blocked_final_gate` plus
`review.candidate_rejected` when skew exceeds the configured threshold. No
Local-L2 data-plane, WS BBO quote lease, sizing, order submit, close, recovery,
or residual-repair behavior changed. GitNexus method-level impact could not
resolve runtime.py private methods even after `npx gitnexus analyze`, so the
blast radius was manually bounded with GitNexus query and direct caller grep:
`runtime.tick -> _dispatch_entry` plus `_dispatch_entry` test surfaces. Local
RED/GREEN proved current V2 dispatched through a 200ms skew with a 100ms budget
before the fix, then blocked it after the fix. Focused and adjacent gates passed:
runtime entry flow `32 passed`, entry-local-L2 `138 passed`, runtime snapshot
freshness `20 passed`, exchange admission harness `12 passed`, startup preflight
`66 passed`, compileall, and diff-check. GitNexus detect-changes returned LOW
with zero affected processes, while including pre-existing `AGENTS.md` and
`CLAUDE.md` changes that are not part of CL041.

Latest lifecycle attribution follow-up, 2026-06-03: CL038 and CL039 closed the
runtime safety surface, but open-to-close attribution still had a V2/V1 drift:
V2 projected only simple order/entry/exit facts, while compensation, recovery,
and terminal-problem events such as `exit.compensated`,
`entry.compensated`, `execution.compensation_failed`, and
`runtime.position_lifecycle_terminal` were not part of a structured lifecycle
ledger. V1's answer is `runtime_state/ledger_bridge.rs`: a rebuildable
journal-derived bridge into `trade_ledger_events`, `position_ledger`,
`order_ledger`, `fill_ledger`, and `position_pnl_facts` with `truth_level`
labels. The local CL040 fix replicates that boundary in V2's persistence layer:
`ProjectionWriter` now writes V1-compatible lifecycle ledger rows for
entry/open, recovered live positions, order submit/fill/failure, exit close,
recovery flat, compensation success/failure, and terminal problem events.
Recovery remains journal-first for replay, but no longer lacks a queryable
attribution row. `execution.compensation_failed` is also mapped to
`trade_ledger_events` rather than `diagnostic_facts`, matching the implemented
ledger bridge. Focused RED/GREEN proved the old missing-table blind spot and
the new full position/order/fill/exit plus recovery/compensation/terminal
chains; adjacent persistence/offline and close/replay suites passed. This
fix then passed the full local gate (`3381 passed`, `9 skipped`, `1 warning`),
compileall, diff-check, and GitNexus detect-changes LOW with zero affected
processes. It was committed as `09292c1` and deployed to `/opt/lightfee-v2`
with `.deploy_version=09292c1`; remote manifest check, compileall, and focused
persistence/offline tests passed (`114 passed`). Services were left running
without restart because the changed persistence/projection modules are only
imported by offline report/analysis code, not by live/sidecar execution. Current
production health is green with local open/pending/close/residual `0/0/0/0`,
Local-L2 missing/stale/sequence-gap `0/0/0`, and no entry/open-position events
since the marker. The read-only diagnose invocation did not load exchange
credentials, so exchange-truth consistency remains a separate credentialed
probe if needed. The fix does not change live order submit, close executor,
recovery policy, Local-L2, WS BBO, sizing, or residual repair. Historical
unallocated Binance/Bybit/Aster rows still require exchange order-history
replay; future rows after CL040 have the V1-style join surface.

Latest pending-entry maker-order follow-up, 2026-06-03: after cloud deploy
`338caec`, production health was green but an all-venue exchange-truth probe
found a separate pending-entry lifecycle issue: local state was flat while 9
non-reduce-only PostOnly/GTX maker entry orders remained open on Binance/Bybit.
OKX current positions and open orders were empty, so this was not the OKX
copy/follow account path. The 9 exact order id/client id targets were canceled
through `VenueTransport.cancel_passive_order()`, and follow-up all-venue truth
showed nonzero positions `0` and open orders `0`; `verify_production_services.py`
stayed green. V1 comparison showed the structural guard: V1 always carries
`PendingEntryHedge.passive_order`, cancels through that passive-order lifecycle,
persists `cancel_requested_at_ms`, and polls passive progress by order id/client
id. The local CL039 fix fully mirrors that boundary: recovery cancel/progress,
abort-open-order truth, maintenance, reconciliation/finalize maker id lookup,
snapshot `passive_order`/`next_progress_poll_ms` roundtrip, and durable cancel
state now all use the passive-order identity. It was committed as `4b6a6ee`
and deployed to `/opt/lightfee-v2`: remote focused maker/passive lifecycle tests
passed (`117 passed`), compileall and deploy-manifest checks passed, sidecar
and live restarted active with `NRestarts=0`, `.deploy_version=4b6a6ee`, and
`diagnose_live.py --since-deploy --json` was healthy with local
open/pending/close `0/0/0` and Local-L2 missing/stale/sequence-gap counts
`0/0/0`. A credentialed all-venue truth probe over Binance, Aster, Bybit, OKX,
Bitget, Gate, and Hyperliquid returned `nonzero_position_count=0` and
`open_order_count=0`, and warning-level journal logs since restart were empty.
CL039 is closed; keep `entry.abort_retained_maker_open_order`,
`entry.abort_maker_order_truth_unavailable`, and
`recovery.maker_cancel_requested` as watch keys for future maker terminality
samples.

Latest startup recovery lifecycle follow-up, 2026-06-03: cloud first deployed
`cb1abbed4516ff016b78be6cc1e0f588d40853c7`; remote focused tests passed
(`60 passed`), services restarted active with `NRestarts=0`, and the prior
CL036 Binance startup probe fanout no longer reproduced (`live_tick_stale` /
Binance `429` gone, Local-L2 rebuild and sequence-gap counts `0`). Production
health still stayed red for a separate V2/V1 lifecycle-release drift:
`lifecycle=risk_only` with stale
`recovery_blocked_reason=startup_recovery_pending_work_without_open_positions`
after runtime recovery had submitted the missing Bybit hedge and then detected
a balanced live `HIVEUSDT` position (`Binance long 394`, `Bybit short 394`,
no open orders). The local CL037 fix is narrow: when
`LiveRuntime._maybe_recover_clean_live_positions()` creates a new balanced open
position from the runtime live-position probe and no pending entry/close work
remains, it reuses `_finalize_startup_recovery()` to apply the V1 rule that
recovered open positions are managed running state. It does not change order
submit, Local-L2, WS BBO, close, residual repair, mismatch flattening, or
generic recovery-block clearing behavior. RED/GREEN reproduced the stale block
(`risk_only` before fix), then focused and adjacent local gates passed
(`1`, `48`, `11`, `4`, and `15` tests); full pytest passed
(`3369 passed`, `9 skipped`, `1 warning`), compileall/diff-check passed, and
GitNexus detect-changes was LOW with zero affected processes. Cloud deployed
`5625361a99d8f5efd0a589446d809353674c7b5e`, remote focused regression passed
(`80 passed`), compileall and manifest checks passed, and services restarted
active. Settled production verification is green (`ok=true`,
`critical_count=0`, `risk_mode=running`, `lifecycle=running`, local
open/pending/close/residual `0/0/0/0`). Credentialed HIVE-only exchange truth
is flat/no-open-orders on Binance and Bybit with high confidence. The close
window did emit WS-BBO/close-price missing evidence and reduce-only retry
noise, but terminal events reached `exit.closed`, `recovery.flat`, and
`runtime.position_drift_corrected`; treat that as a separate close-path watch,
not old WS BBO subscription or Local-L2 recurrence.

Latest OKX contract-fill unit follow-up, 2026-06-02: cloud remained on
`a0b8b84` while the WS BBO classification fix `a8ee0ec` was pushed but not
deployed. Current remote health later recovered green (`ok=true`,
`risk_mode=running`, `lifecycle=running`, local open/pending `0/0`,
`exchange_truth_mismatches=[]`), but the deploy window contained a separate
OKX SWAP quantity-unit drift. Examples: `SIGNUSDT` local expected OKX long
`21` while live OKX long was `2100`; `HOMEUSDT` local expected OKX long `3`
while live OKX long was `300`; `LABUSDT` local expected OKX long `0.1` while
live OKX long was `1.0`. Official OKX API docs define `fillSz`/`accFillSz`
and derivative order sizes as contract units for `FUTURES`/`SWAP`/`OPTION`.
V2 already converted OKX order request size base-to-contracts and OKX live
positions contracts-to-base through `ctVal`, but direct order fill parsing and
order-status reconciliation still treated `fillSz`/`accFillSz` as base
quantity. The local fix converts OKX fill and order-status quantities back to
base quantity with trusted `ctVal`, records `quantity_units`,
`contract_qty`, and `ct_val` evidence on OKX reconciliation, and does not
change order request sizing, Local-L2, WS BBO, close, residual repair, or
recovery policy. RED/GREEN proved direct `fillSz=3, ctVal=100` returns base
quantity `300`, and order-status `accFillSz=3, ctVal=100` reconciles as
`300`; `tests/test_venues_transport.py` passed (`380 passed`), adjacent
live-entry/startup/pending/OKX-ctVal/WS-BBO suites passed (`466 passed`),
compileall/diff-check passed, full pytest passed
(`3365 passed, 9 skipped, 1 warning`), and GitNexus detect-changes was HIGH
because the hunk touches broad `place_order`/`fetch_order_status` transport
flows; manual hunk audit kept the code scope to OKX contract-to-base
fill/reconciliation conversion plus tests/docs. Cloud fast-forwarded to
`e087513d4b7e799227c14181341b9313f88cd74a`, remote compileall passed, the
focused remote regression suite passed (`846 passed`), the manifest check
passed, sidecar/live restarted active with `NRestarts=0`, singleton check
passed, `verify_production_services.py --json` stayed green, and the final
7-minute deploy-window diagnose had zero Local-L2 rebuilds, zero WS BBO
provider blocks, zero quote-lease blocks, zero entries opened, zero positions
opened, zero OKX drift correction failures, and zero pending-entry finalize
reconciliation errors.

Latest WS BBO provider follow-up, 2026-06-02: after CL032 acceptance on cloud
deploy `a0b8b84`, Local-L2 remained clean, production health was green, and
current local open/pending state later returned to `0/0`. A new WS BBO
provider split appeared in `runtime.entry_blocked_local_l2_selection`:
`entry_ws_bbo_quote_lease_waiting_for_subscription=92`,
`entry_ws_bbo_quote_lease_stale_quote=22`, and
`entry_ws_bbo_quote_lease_missing_quote=1`. This was not the old Local-L2
stale rebuild and not dirty state. Root cause was two precision gaps: budget-out
WS BBO candidates were still reported as generic
`entry_ws_bbo_quote_lease_waiting_for_subscription`, and tracked stale/missing
REST refresh failures did not carry refresh attempt/outcome evidence. The local
fix records per-venue WS BBO budgeted and budget-excluded keys during
candidate activation, emits
`entry_ws_bbo_quote_lease_budget_exhausted` with
`coverage_reason=subscription_budget_exhausted` for budget-out candidates, and
adds per-leg `readiness_evidence.rest_refresh` outcomes for tracked stale or
missing quote blocks. Real untracked missing coverage remains
`waiting_for_subscription`; tracked invalid/no-quote REST remains fail-closed.
No budget increase, TTL widening, Local-L2, order submit, sizing, close,
residual repair, or recovery semantics changed. Focused RED/GREEN passed
(`3 passed`), adjacent provider/WS/dispatch suites passed (`21 passed`,
`15 passed`, `22 passed`), impacted runtime/entry suites passed (`138 passed`,
`31 passed`, `32 passed`, `19 passed`), compileall and diff-check passed, and
full pytest passed (`3363 passed, 9 skipped, 1 warning`). GitNexus resolved
provider and WS BBO data-plane impact as LOW; runtime private helpers remained
a GitNexus lookup limitation and were covered by manual hunk audit plus tests.
Final GitNexus detect-changes was MEDIUM, limited to `_validate_quotes`
quote-refresh/provider evidence flows plus docs/tests. It was deployed
together with the OKX unit fix in `e087513`; the final 7-minute deploy-window
diagnose had `runtime.entry_blocked_local_l2_selection=0`,
`runtime.entry_blocked_quote_lease=0`, `runtime.local_l2_hot_stale_rebuild=0`,
and no entry/open-position events.

Latest Local-L2 WS lifecycle follow-up, 2026-06-02: post-`087d73a` production
health stayed green and the WS BBO provider issues did not recur, but
`runtime.local_l2_hot_stale_rebuild` still showed `subscription_missing` for
active HOT execution books, mostly Bybit `LABUSDT/HUSDT`. Root cause was in the
Local-L2 activation boundary: `_ensure_l2_active_for_candidates` skipped all
activation work when a book was already HOT, fresh, and non-crossed, so
WS-authoritative venues could keep a snapshot-built HOT book without ever
registering/connecting the Local-L2 WS stream that must maintain it. A second
lifecycle gap left already-registered but disconnected streams unconnected
because duplicate `start_ws_streams()` returns `0`. The local fix adds
`LocalL2DataPlane.ws_stream_state()` and makes dynamic Local-L2 activation
register/connect WS for HOT WS-authoritative/stream-only books that lack stream
lifecycle, plus reconnect registered-but-disconnected streams even when no new
stream was registered. The first deploy attempt `2583b11` still reproduced two
Bybit `subscription_missing` rebuilds and proved a second startup-restore
branch: `_restore_local_l2_state()` could resurrect old
`local_l2_books_snapshot` entries with `pool=hot_exec` after startup activation
had already decided which retained/open-position books deserved WS streams. The
follow-up local fix skips transient `hot_exec`/`warm` full-book snapshots unless
they have a retained/open/pending/passive-close owner. Stale/crossed rebuild
behavior, REST-buffered venue behavior, quote provider, order submit, sizing,
close, residual repair, and recovery semantics are unchanged. RED/GREEN covered
missing-stream, duplicate-disconnected, and unowned restored-HOT branches;
adjacent entry/local-L2/startup suites passed. The follow-up was deployed in
`a0b8b84`; remote related regression tests passed (`344 passed`), the settled
five-minute production window did not reproduce
`runtime.local_l2_hot_stale_rebuild`, and `diagnose_live.py --since-deploy`
reported `health.ok=true`, `l2_evidence.stale_rebuild_count=0`,
`missing_l2_or_tick_count=0`, and `sequence_gap_count=0`. Final
`verify_production_services.py --json` returned `ok=true`,
`critical_count=0`, `warning_count=0`, `risk_mode=running`,
`lifecycle=running`, and `exchange_truth_mismatches=[]`; the nonzero
`open_position_count=2` / `pending_entry_count=10` is current trading runtime
state, not evidence of Local-L2 recurrence.

Latest WS BBO close-price follow-up, 2026-06-02: after the provider was active
on `e6cc67f`, entry still selected/dispatched/opened, but passive close showed
a separate close-price evidence family: `exit.passive_close_missing_l2_or_tick`
samples had positive venue `tick_size` and `price_hint=0.0`, then escalated to
aggressive close after three missing-price failures. Root cause was that the
entry provider had been decoupled from Local-L2, while normal close price hints
still only used `LiveRuntime._resolve_local_l2_mid`. The local fix keeps
Local-L2 as the preferred source, but when active provider is
`ws_bbo_quote_lease` and Local-L2 cannot provide a valid mid, it uses a fresh,
positive, non-crossed `ws_bbo_cache` quote inside the quote-lease age budget as
the close price hint. Stale/invalid BBO fallback remains rejected with
structured evidence. No passive executor, post-only, tick-size, order submit,
sizing, residual repair, or recovery semantics changed. Focused RED/GREEN and
adjacent runtime/passive/provider tests passed; full pytest passed
(`3356 passed, 9 skipped, 1 warning`). Cloud deployed `087d73a`: remote
focused regression tests passed (`74 passed`), `.deploy_version` is
`087d73ab28ac649af9728528e2334472b6074c68`, settled health is `ok=true`, and
post-deploy scan showed the fallback live and active
(`runtime.close_price_evidence_fallback=49`, fresh samples inside `1500ms`,
`exit.passive_close_missing_l2_or_tick=0`). Stale WS BBO fallback samples were
rejected fail-closed (`runtime.close_price_evidence_stale=6`).
`entry_ws_bbo_quote_lease_waiting_for_subscription` and
`runtime.local_l2_hot_stale_rebuild` remain separate evidence tracks, not
claimed closed by this fix.

Supplemental evidence before push, 2026-06-02: read-only cloud probe on
pre-fix deploy `e6cc67f` parsed 66,949 event rows through
`2026-06-02 18:50:53 +0800`. The provider waiting track did not reproduce in
the refreshed window (`entry_ws_bbo_quote_lease_waiting_for_subscription=0`,
`missing_quote=0`, `stale_quote=0`), while quote-lease execution blocks
remained (`runtime.entry_blocked_quote_lease=209`) and close price evidence
remained (`exit.passive_close_missing_l2_or_tick=132`,
`exit.passive_close_missing_l2_tick_escalated=43`) with positive tick sizes and
`price_hint=0.0`. Local-L2 stale rebuilds also remained as a separate track
(`runtime.local_l2_hot_stale_rebuild=592`, mostly
`reason=subscription_missing`, `pool=hot_exec`, Bybit `LABUSDT/HUSDT`). Current
production health was green: sidecar/live active, `running/running`, local
open/pending/residual counts `0`, and `exchange_truth_mismatches=[]`. The
Local-L2 track is not part of CL031; its active HOT WS-lifecycle branch is
handled separately as CL032.

Post-deploy residual split for `087d73a`: current production health stayed
green and the only active unresolved family was Local-L2 stale rebuild noise.
CL031 close-price fallback is deployed and triggered
(`runtime.close_price_evidence_fallback=49`,
`exit.passive_close_missing_l2_or_tick=0`). CL030 is deployed and no
quote-lease block recurred in the immediate window, but the stale/expired
refresh branch still needs a real trigger sample. WS BBO subscription waiting
did not recur. Local-L2 `subscription_missing` rebuilds were the active
unresolved family before CL032. The local CL032 fix targets the active HOT
WS-lifecycle branch; target-prune semantics and connected-but-idle venue
timestamp/depth behavior remain separate watch tracks after deployment.

Latest WS BBO quote-lease execution follow-up, 2026-06-02: post-CL029 cloud
evidence on `e6cc67f` showed the provider-stage missing/stale bug stayed
closed (`entry_ws_bbo_quote_lease_missing_quote=0`,
`entry_ws_bbo_quote_lease_stale_quote=0`,
`tracked_false_missing_stream_refs=0`) while entries still selected,
dispatched, and opened (`execution.entry_selected=24`,
`runtime.entry_dispatched=24`, `entry.opened=8`). The remaining live blocker
was later in the final execution gate: `runtime.entry_blocked_quote_lease` with
`stale_quote_lease=12`. Payloads showed the lease TTL was `1500ms`, but the
provider still accepted WS BBO quotes against `max_market_age_ms=30000`; some
leases were created from leg quotes already older than the execution budget or
expired before dispatch. The local fix keeps the same TTL and fail-closed gate:
`ws_bbo_quote_lease` now treats `entry_quote_lease_ttl_ms` as the quote
freshness budget before creating a lease, refreshing tracked stale legs through
the existing REST top-book path, and `_dispatch_entry` retries the current
provider exactly once when the final gate returns `expired_quote_lease` or
`stale_quote_lease`. The refreshed lease must pass the same execution check;
otherwise the original block remains with structured refresh evidence. No
Local-L2, order submit, sizing, close, residual repair, or recovery semantics
changed. RED/GREEN passed for provider TTL refresh and dispatch just-in-time
lease refresh; adjacent provider/dispatch/WS/runtime suites passed; full pytest
passed (`3354 passed, 9 skipped, 1 warning`). Cloud deployed `087d73a`, remote
focused regression tests passed (`74 passed`), and settled health is `ok=true`.
The immediate post-deploy scan had `execution.entry_selected=14` and no
`runtime.entry_blocked_quote_lease`, no provider `missing_quote`/`stale_quote`,
and no `waiting_for_subscription`; longer acceptance still needs a window that
actually exercises the stale/expired execution-refresh branch.

Latest WS BBO stale-quote follow-up, 2026-06-02: after CL-028 deployed, the old
tracked-false missing-quote branch stayed closed (`tracked_false_missing_stream_refs=0`),
but the live provider window showed `entry_ws_bbo_quote_lease_stale_quote=218`
and `stale_quote_lease=102`. The stale split was Aster-heavy (`211/218`), with
`GUNUSDT`, `ARIAUSDT`, and `COSUSDT` at the top. Read-only production probes
showed the distinction needed for a root fix: Aster `GUNUSDT` received WS and
REST top-book; Aster `ARIAUSDT` had no WS BBO update in a 35s probe but REST
returned valid bid/ask; Aster `COSUSDT` had no WS update and REST returned a
one-sided top book (`bid=0`, `ask>0`). The local fix adds a short-timeout public
REST top-book refresher for Binance-compatible venues, Bybit linear tickers,
and OKX ticker, used only by `ws_bbo_quote_lease` when a venue/symbol stream is
already tracked and the WS quote is missing or stale. Refreshed REST quotes must
pass the same positive bid/ask and non-crossed checks before updating
`ws_bbo_cache` and creating the normal quote lease; unsupported, invalid,
one-sided, timed-out, or errored REST responses keep the old fail-closed path.
No Local-L2, order placement, sizing, close, residual repair, or recovery
semantics changed. Focused RED/GREEN passed (`3 passed` provider tests and
`4 passed` REST refresher parser tests), adjacent provider/WS/dispatch suites
passed (`17 passed`, `15 passed`, `21 passed`), and impacted entry/runtime
suites passed (`132 passed`, `32 passed`, `17 passed`). Cloud deployed
`3c70b01`: remote focused tests passed (`53 passed`), deploy marker was
`3c70b01fb0310da1b996320992b4db221065d816`, sidecar/live restarted active
with `NRestarts=0`, and health was `ok=true` with running lifecycle and zero
local open/pending/residual state. The short post-deploy scan had no provider
selection sample, so long-window provider acceptance remains open.

Latest WS BBO provider follow-up, 2026-06-02: after CL-027 cleared the older
ARIA pending-entry blocker, production finally provided a clean provider window.
The CL-026 fixes were active, but the window still showed
`entry_ws_bbo_quote_lease_missing_quote=512`, `entry_ws_bbo_quote_lease_stale_quote=5`,
and `stale_quote_lease=29` with one real open. The split showed the remaining
missing quote family was mostly `tracked=false` stream state, not subscribed
streams with no data. Root cause was coverage consistency: the default
`entry_ws_bbo_per_venue_budget=4` was too small for production candidate sets,
and budget-out candidates could still reach provider quote validation. The
local fix raises the default per-venue WS BBO budget to `10` and adds a
pre-provider subscription coverage guard so candidates with no cached quote and
no tracked stream are classified as
`entry_ws_bbo_quote_lease_waiting_for_subscription` instead of inflating generic
`missing_quote`. Focused RED/GREEN passed, provider/config/dispatch/WS BBO
adjacent suites passed, impacted entry/runtime/snapshot suites passed, and full
pytest passed (`3345 passed, 9 skipped, 1 warning`). Cloud deployed `457d16e`:
remote focused tests passed (`47 passed`), live config loaded
`entry_ws_bbo_per_venue_budget=10`, sidecar/live restarted active, production
health was `ok=true` with `running/running` and zero local open/pending/residual
state, and a short post-restart scan saw only
`runtime.ws_bbo_dynamic_ws_started=4` with no new missing-quote or tracked-false
missing stream refs. This changes only WS BBO provider coverage and config
defaults; Local-L2, order placement, sizing, close, residual repair, and
recovery semantics are unchanged. Longer production-window verification should
continue because the short window had no entry selection attempt.

Latest pending-entry live-truth follow-up, 2026-06-01: after CL-026 deploy,
production stayed `risk_only` / `fail_closed` because an older `ARIAUSDT`
pending entry remained from before the deploy. Credentialed read-only truth
showed Bybit long `1238`, Binance short `619`, and no open orders while local
state had `open_position_count=0` and `pending_entry_count=1`. This was not a
Local-L2 or WS BBO provider regression: the mismatch arose after entry dispatch
from pending-entry reconciliation. Root cause was a balanced pending entry with
only an untradeable hedge dust residual staying parked as
`hedge_quantity_below_min_notional`, startup live hydration interpreting the
Bybit excess as missing Binance hedge, a stale
`position_drift_correction_failed` block keeping the deployed recovery fix
unreachable, runtime drift flatten latching fail-closed from incomplete
synchronous cleanup fill evidence even after live truth became balanced, and
finalize still treating planned hedge CIDs as queryable evidence before hedge
submission. The local fix terminalizes under-min hedge dust only when balanced
fills already exist, finalizes the balanced open position, clamps trusted
imbalanced startup live truth to the balanced quantity so drift repair can
flatten the excess side, retries old fail-closed recovery blocks when pending
work still exists, verifies live truth after incomplete drift-cleanup evidence,
and keeps planned hedge CIDs out of finalize reconciliation unless there is
submitted-hedge evidence. The fix is deployed on `f1727c1`: cloud focused tests
passed (`140 passed`), sidecar/live restarted active, final
`verify_production_services.py --json` returned `ok=true`, and credentialed
ARIA Bybit/Binance truth is flat/no-open-orders with local open/pending/residual
counts `0`. Focused RED/GREEN passed (`2 passed`), pending-entry
suite passed (`12 passed`), startup/drift guard passed (`4 passed`), adjacent
hedge root-fix suite passed (`127 passed`), broader focused runtime/diagnose
suite passed (`191 passed`), compileall passed, and full pytest passed
(`3343 passed, 9 skipped, 1 warning`).

Latest WS BBO provider closure, 2026-06-01: production after switching to
`entry_readiness_provider=ws_bbo_quote_lease` proved the new provider was in the
real entry path (`execution.entry_selected=8`, `runtime.entry_dispatched=8`,
`runtime.position_opened=2` for `ALLOUSDT` and `BASUSDT`) but still had
coverage/lifecycle misses (`entry_ws_bbo_quote_lease_missing_quote=179`,
`stale_quote_lease=9`). Official exchange docs confirmed the chosen
top-of-book feeds are valid for quote leases: Binance/Aster `bookTicker`, OKX
`tickers`, Bybit linear `tickers.{symbol}`, Bitget futures `ticker`, Gate
`futures.book_ticker`, and Hyperliquid `bbo`. Root cause was V2 provider
lifecycle, not Local-L2 sequence reconstruction: WS BBO activation used the
post-freshness final tradeable list, per-venue budget spent slots by
alphabetical symbol order, and subscription/control errors were not surfaced.
The local fix prewarms from the catalog-supported discovery shortlist,
preserves candidate ranking order for per-venue stream budgets, aligns Binance
to the current routed public WS endpoint, and records WS control errors in
stream state evidence. Focused RED/GREEN and adjacent provider/dispatch/config
suites passed (`4 passed`, `10 passed`, `13 passed`, `21 passed`, `3 passed`).
Read-only public WS probes received quotes for all seven configured venues
(local `NO_PROXY=*`: Binance, Bybit, Hyperliquid; production-cloud network:
OKX, Bitget, Gate, Aster). No Local-L2, order placement, sizing, close,
residual repair, or recovery semantics changed. The closure was deployed in
`7745e86`; remote focused provider/dispatch tests passed (`43 passed`) and both
services restarted active. Post-deploy live stayed `risk_only` / `fail_closed`
because an older `ARIAUSDT` pending-entry/live-truth mismatch remained from
before this deployment, so fresh production entry counters are blocked until
that separate pending-entry family is resolved.

Latest targeted local closure, 2026-05-31: post-`ae4bd9c` production stayed
healthy with zero local open/pending/residual state, empty warning logs, and
account-level read-only exchange truth flat/no-open-orders across configured
venues. Real `IDUSDT` and `HOMEUSDT` opens closed, but both reproduced the
passive-close terminal-flatness family: V2 submitted a first reduce-only maker
on Bybit after live truth had already made the short maker leg flat, producing
`110017`. The local root fix adds a live-truth precheck before first passive
maker submit; if the active maker leg is flat, V2 now either clears both-flat
state through existing live-flat recovery or directly flattens the other live
leg through the existing reduce-only IOC one-sided path. Focused RED failed as
expected, then the new test passed, and adjacent passive-close terminality
tests passed (`14 passed`), the full passive-close suite passed (`106 passed`),
historical passive harnesses passed (`8 passed`, `4 passed`, `4 passed`),
and probe-catalog evidence was strengthened so catalog-load failures,
`supported_symbols()` errors, and empty catalogs are distinguishable from
private position API failures. Focused probe RED/GREEN passed (`2 passed`) and
adjacent probe suites passed (`7 passed`, `11 passed`). Wider focused suites
passed (`62 passed`, `106 passed`), compileall and diff-check passed, and
GitNexus detect-changes reported medium risk with the two affected flows still
limited to passive-close drive paths. Cloud deployed `70a1a8c`; remote manifest,
compileall, focused regressions, service restart, health, warning-log, current
state, deploy-window scan, and targeted HOME/ID exchange-truth probes passed.
Entry-admission rejects, residual repair, and Local-L2 rebuilds were documented
as separate families; only the passive-close maker-flat branch changed trading
control flow, while the probe work is evidence-only.

Latest high-frequency diagnostic closure, 2026-05-31: the CL-023 log rotation
fix left several safe-to-compact Local-L2/catalog diagnostics as the next
largest remaining source of avoidable event volume. The follow-up compacts
repeated `runtime.local_l2_freshness_state`,
`runtime.local_l2_rest_bootstrap_deferred_for_ws_snapshot`,
`runtime.local_l2_snapshot_ok`, `runtime.entry_blocked_local_l2_selection`, and
`runtime.candidate_symbol_skipped` records with `compact=true` and
`suppressed_count`, while keeping first, state-change, error, and recovery
success evidence. No Local-L2 readiness, candidate filtering result, order
submission, close timing, or recovery semantic changed. Focused RED/GREEN
passed (`3 passed` + `2 passed`), the adjacent Local-L2/entry/snapshot suite
passed (`221 passed`), compileall and diff-check passed, and GitNexus
detect-changes reported high risk only because the event helpers sit in the
main tick path. Cloud deployed code commit `0c1e620` with manifest commit
`59ac6c1`; remote manifest, compileall, focused regressions (`5 passed`),
service restart, health, warning-log, and scoped deploy-window event checks
passed. The short deploy-window scan had no opens and no error-like records;
target event families were already low-volume, so repeated same-key compact
samples may appear only after longer runtime.

Latest observability closure, 2026-05-31: production logs after `90fa2bc`
showed no warning-level service errors and no local open/pending/residual work,
but the active JSONL event log had grown to about 577 MiB because the existing
retention/compaction config was not wired into `Journal`, while repeated
`scan.no_entry_diagnostics` full payloads and
`runtime.snapshot_freshness_decision` invalid-quote decisions dominated byte
volume. The local fix adds JSONL rotation using the existing persistence
config, compact repeated snapshot-freshness events with `suppressed_count`, and
compact repeated no-entry blocker-family diagnostics with
`suppressed_full_payload_count`. This is an observability-only closure: no
strategy thresholds, order submission, close timing, admission rules, or
recovery semantics changed. Focused RED/GREEN passed (`2 passed` + `2 passed`),
adjacent persistence/snapshot/local-L2/degraded-snapshot suite passed
(`90 passed`), compileall passed, `git diff --check` passed, and GitNexus
detect-changes reported medium risk limited to Journal/runtime evidence paths.
Cloud deployed code commit `42dd100` with manifest commit `6681321`; remote
manifest, compileall, focused regressions, service restart, health, and
warning-log checks passed. The active event log rotated to a fresh small file
and the prior 585 MiB file moved to `.1`. During deploy startup, recovery found
and flattened a stale Bybit `BRUSDT` live position; follow-up read-only truth
then found two unmanaged non-reduce-only Bybit `BRUSDT` buy open orders, both
were canceled after confirming position was zero, and final scoped exchange
truth was high-confidence flat/no-open-orders.

Latest passive-close terminal normalization closure, 2026-06-10: post-deploy
review found 8 opened positions with 6 abnormal passive-close close paths, plus
one-sided fast-flatten ambiguity and passive-close/runtime-drift ownership risk.
The local fix emits `exit.passive_close_resolved` from live-flat passive-close
cleanup, retains OKX original order identity after amend-failure truth shows
live/partial, makes runtime drift stand down while passive close owns a
position, requires open-order-flat proof before one-sided reduce-only flatten,
and adds passive-close terminal counters to diagnose. Local gates passed
passive terminal/OKX amend/runtime-owner RED/GREEN (`3 passed`), diagnose
summary (`1 passed`), passive close (`129 passed`), close execution
(`50 passed`), diagnose (`59 passed`), recovery core (`110 passed`), recovery
restart (`33 passed`), live startup preflight (`94 passed`), and compileall.
Cloud deployed through `c1381b3` with `f8c2b36` included: remote `git_head` and
`.deploy_version` matched `c1381b3`, sidecar/live were active with
`NRestarts=0`, verifier returned `ok=true` with zero critical/warning reports,
and since-deploy diagnose returned `healthy` / `risk=low` / `gate_passed=true`.
Current exchange truth is high-confidence flat/no-open-orders and
`passive_close_terminal_summary` has zero stale/fast-flatten/problem residue.

Latest MOVEUSDT ACK-only duplicate-client closure, 2026-06-10: deployment-window
automatic handling produced a Bybit ACK-only close artifact with accepted ids
but no fill confirmation, followed by a same-client-id Bybit `110072
OrderLinkedID is duplicate`. Later reconciliation, terminal lifecycle, residual
repair, and current exchange truth proved flat/no-open-orders, but diagnose kept
the ACK-only/duplicate pair in active order errors and the close executor
projected duplicate live-flat as zero-quantity `order.filled`. The local fix
treats ACK-only `order.uncertain` as implicit truth-gap registration when
accepted ids/no-fill evidence is present, binds nested exchange-error and
request-context identities, resolves same-client-id duplicate artifacts only
behind terminal/current flat truth, and emits
`exit.close_duplicate_client_order_resolved_live_flat` instead of a zero fill.
Focused MOVEUSDT/ACK-only/duplicate tests passed (`5 passed`), full
diagnose+close executor files passed (`108 passed`), related passive-close /
venue-transport / recovery-core regression passed (`540 passed`), and compileall
passed. Cloud deployed through `c1381b3` with `f5541fe` included: remote
`git_head` and `.deploy_version` matched `c1381b3`, verifier returned `ok=true`
with zero critical/warning reports, and since-deploy diagnose returned
`healthy` / `risk=low` / `gate_passed=true`. The current window has
`top_exchange_errors=[]`, `order_error_evidence=[]`, and
`resolved_order_truth_gap_summary.count=0`, so there is no active MOVEUSDT
ACK-only/110072 diagnostic residue after deployment.

Latest evidence-gap closure, 2026-05-31: post-`88a1948` production watch found
no recurrence of the CL-020 early funding close or CL-021 first-stage hold.
Remaining issues were observability/probe gaps: invalid quote records lacked
raw sanitized quote values, pending-entry finalization records lacked stable
symbol/pair/outcome context in all branches, and `diagnose_live.py` treated OKX
venue-symbol conversion failures or unsupported symbols as generic read-only
probe failures. The local fix adds evidence fields and structured read-only
probe classifications only; no strategy thresholds, order submit behavior,
close timing, or residual repair semantics changed. Focused RED/GREEN passed
(`6 passed`), adjacent evidence/admission/local-L2 suite passed (`79 passed`),
the production-feedback OKX `classification=instrument_missing` regression
passed, and compile/diff hygiene passed.

Latest pending-entry V1 semantic-drift closure, 2026-05-27: production read-only truth found local V2 false-flat state while exchanges held live single-sided positions (`MUBARAKUSDT`, `EDENUSDT`, `INUSDT`, `BEATUSDT`). Root cause was V2 clearing pending entries from `stale_accepted_order + momentary flat position` and querying a planned hedge CID before submit. Remote RED tests reproduced all three branches; after patching `lightfee/engine/runtime.py` and `lightfee/engine/reconciliation.py`, remote tests passed: `tests/test_pending_entry_v1_semantic_drift.py` (`3 passed`), focused adjacent reconciliation/runtime tests (`16 passed, 172 deselected`), full live-harness profile (`47 passed`), and explicit offline false-green probe (`1 passed`). `lightfee-live` was restarted cleanly at 13:51:17 CST and runtime recovery flattened the known live mismatches by reduce-only orders. PRLUSDT then exposed a second V1-drift layer: balanced live-position evidence hydrated quantity but not price/order evidence, so V2 deferred pending finalize and relied on live recovery. The PRL branch now hydrates live position quantity/entry price into pending fills, uses V1-style synthetic recovery order ids, and finalizes balanced entries based on V1 quantity+price semantics rather than requiring order ids. Remote PRL RED/GREEN, focused adjacent tests (`20 passed, 172 deselected`), full live-harness (`48 passed`), explicit probe (`1 passed`), and credentialed read-only all-venue account truth passed. The legacy PRL pending entry was later auto-cleaned under a reduce-only cleanup window without manual state deletion. Normal deploy initially hit an operational OOM because `runtime/live-events.jsonl` had grown to 1.7G; the old log was archived, a fresh event log was created, and live was restarted. Final production health passed with sidecar/live active, `risk_mode=running`, `lifecycle=running`, local open/pending/residual counts `0`, all venues `nonzero_positions=[]`, and Binance/Bybit/Aster/OKX open-order counts `0`.

Latest Task-7 closure evidence, 2026-05-27: local full gate is green on code closure commit `30a8ddc` (`python3 scripts/validate_change.py --profile full --keep-going`, 10/10 steps passed; `test_diagnose_live.py` `18 passed`, `test_venues_contract.py` `133 passed`). GitNexus `detect_changes(scope="all", repo="LightFeeV2")` reported low risk for the final diagnose-tool delta (`changed_count=11`, `changed_files=2`, no affected execution flows). Cloud `/opt/lightfee-v2` was fast-forwarded, restarted, and then synced again with this documentation closure; `verify_production_services.py --json` passed with `risk_mode=running`, `lifecycle=running`, `open_position_count=0`, `pending_entry_count=0`, `pending_close_count=0`, and `pending_residual_repair_count=0`. Credentialed read-only probes with explicit venues are high-confidence flat/no-open-orders for `LYNUSDT` (`bybit,aster`), `OPGUSDT` (`binance,okx`), `BEATUSDT` (`okx,bybit`), and `BIOUSDT` (`bybit`). Fresh local harness/probe recheck also passed the independent offline incident gate (`python3 scripts/validate_change.py --profile live-harness --keep-going`, `31 passed` in `tests/live_harness`, now including fixture-driven older GMT/LYN/XCN/UBUSDT passive-close replays from `tests/fixtures/live_incidents/2026-05-23` and `2026-05-25`), confirmed default live probes remain explicit-only (`6 skipped` without `LIGHTFEE_RUN_LIVE_PROBES=1`), passed the public Bybit/Binance/OKX Local-L2 read-only probe (`6 passed`), passed the 2026-05-20 public REST exchange-semantics probe (`ok=true`), and confirmed the OKX position probe is still read-only but credential-gated locally (`1 passed, 1 skipped`). Earlier history was also rechecked where the old incident surface is reproducible without inventing state: 2026-05-14 rate-limit/signing semantics (`4 passed`), 2026-05-15/16 sanitized production-blocker analyzer replay, 2026-05-17 pending/L2 analyzer fixture (`5 passed`), 2026-05-18 maker-reject/no-entry/local-L2 targeted surfaces (`3 passed`, `2 passed`, `43 passed`), and 2026-05-22 zero-fill/Bybit empty-string transport path (`33 passed`). Current-run log scan found no new `entry.opened`, `runtime.position_opened`, residual, reject, fail, or error fingerprints.

| Bug ID | Status | Severity | Component | Fingerprint | First Seen | First Seen Commit | Fixed In | Last Verified | Related Refactor | Latest Outcome |
|---|---|---:|---|---|---|---|---|---|---|---|
| [CL-076-v1-lifecycle-closure-table-runtime-gate-release-unification](daily/2026-06-13.md#cluster-cl-076-v1-lifecycle-closure-table-runtime-gaterelease-unification) | local fixed; deploy pending | critical | `live-runtime`, `v1-parity`, `pending-entry`, `passive-close`, `residual-repair`, `recovery-core`, `current-state`, `diagnose-live`, `production-health` | `v1-lifecycle-closure-table-runtime-gate-release-unification + scattered-pending-passive-residual-recovery-semantics` | 2026-06-13 | local audit after recent WS-BBO/pending/passive/residual/recovery fixes | working tree | 2026-06-13 local: closure-table suite `16 passed`; related diagnose/production-health/snapshot/startup/entry/passive regression set `551 passed`; compileall passed; diff-check passed; GitNexus detect-changes low risk with affected processes `0`, with runtime large-file indexing caveat. | `V1LifecycleClosureTable as runtime/diagnose/health semantic authority` | V2 now maps entry quote lease, pending entry, open position, passive close, residual repair, recovery truth, and runtime progress into one V1 semantic closure surface. Runtime entry gate reads the table summary, recovery block/clear events include closure identity, and pending/passive/residual release paths emit `closure_decision_id`. The fix reuses runtime-owned recovery ledger/core results to avoid owner drift and does not change trading aggressiveness, strategy thresholds, WS-BBO budget/TTL, Local-L2 opt-in, order submit/cancel, close execution, residual repair submission, or recovery flatten policy. |
| [CL-075-ws-bbo-tracked-scope-data-plane-fanout](daily/2026-06-12.md#cluster-cl-075-ws-bbo-tracked-scope-data-plane-fanout) | fixed, deployed, cloud verified | high | `live-runtime`, `ws-bbo`, `snapshot-freshness`, `v1-parity`, `production-health`, `production-observability` | `ws-bbo-entry-quote-revalidate-full-shortlist-rest-fanout-live_tick_stale + snapshot-freshness-full-candidate-observability-cpu-fanout + final-snapshot-freshness-full-tradeable-filter-cpu-fanout + snapshot-fallback-health-full-candidate-scope-cpu-fanout` | 2026-06-12 | post-WS-BBO production cutover recurring `live_tick_stale` after settled checks | `6cffb2b` | 2026-06-12 local: focused WS-BBO/snapshot/Local-L2 scope suite `43 passed`; production-health/runtime-smoke/diagnose suite `127 passed`; compileall passed; diff-check passed; GitNexus detect-changes low with 0 affected processes. Cloud: `/opt/lightfee-v2` fast-forwarded by `git pull --ff-only origin main` to `6cffb2b`, `.deploy_version` matched, manifest/compile/focused suite `7 passed`, sidecar/live active with `NRestarts=0`, consecutive settled verifier `ok=true`/`critical_count=0`/`warning_count=0` with no `live_tick_stale` recurrence, diagnose `gate_passed=true`, exchange truth high-confidence flat/no-open-orders, `entry_opened_count=0`, `position_opened_count=0`, pending/open/residual counts all `0`, `local_l2_residual_runtime_enabled_count=0`, max quote revalidate target count `16`, and fallback health sampling `10` V1 tracked candidates out of about `4634-4642`. | `V1 primary+shadow execution data-plane scope for WS-BBO and snapshot freshness` | V2 now preserves V1's boundary between large scan candidate pools and execution-grade market data: WS-BBO quote revalidation, snapshot-freshness observability, final freshness filtering, and fallback health candidate sampling all use primary+shadow tracked scope in WS-BBO/Local-L2 effective modes. CL-071 tracked REST fallback remains intact, and the fix does not change health budgets, WS-BBO budget/TTL, REST concurrency, strategy/OI/liquidity, admission, sizing, order, close, recovery, residual repair, or production config semantics. |
| [CL-074-runtime-heartbeat-health-gate-exporter-only-false-progress](daily/2026-06-12.md#cluster-cl-074-runtime-heartbeat-health-gate-exporter-only-false-progress) | fixed, deployed, cloud verified | high | `live-runtime`, `current-state`, `production-health`, `diagnose-live`, `production-observability`, `entry-readiness-provider`, `market-data-runtime` | `runtime-heartbeat-health-gate-exporter-only-false-progress + ws-bbo-effective-provider-local-l2-residual-runtime` | 2026-06-12 | post-heartbeat/health-gate review after repeated `live_tick_stale` surface fixes and follow-up cloud Local-L2 residual runtime evidence | `7180ab8` | 2026-06-12 local: heartbeat focused suite `125 passed`; WS BBO/Local-L2 follow-up focused suite `555 passed`; current-state/health/diagnose export follow-up `156 passed`; final WS BBO snapshot restore/persistence RED/GREEN plus startup-preflight `96 passed`; compileall passed; diff-check passed; GitNexus detect-changes low for the final snapshot guard. Cloud: `/opt/lightfee-v2` fast-forwarded by `git pull --ff-only origin main` to `7180ab8`, `.deploy_version=7180ab8f152a1f3167ef193adefbb9d1937d0010`, sidecar/live restarted active, persistent and current Local-L2 snapshots `0/0/0`, diagnose `gate_passed=true`, `local_l2_residual_runtime_enabled_count=0`, exchange truth flat/no-open-orders, no opened/pending/residual work in the deploy window, and final settled verifier `ok=true`, `critical_count=0`. | `Runtime lane progress schema plus WS BBO effective market-data profile` | Current-state exporter freshness no longer proves runtime progress. Pull-only live deployments now default missing live provider to `ws_bbo_quote_lease`, keep explicit `local_l2` as the V1 opt-in, refresh and export configured/effective market-data config through EngineState, current-state JSON, and diagnose local/state-consistency/gate views, suppress Local-L2 data-plane/session gates under WS BBO effective mode, drive pending passive maker-event maintenance from WS BBO via the in-situ hedge driver, clear Local-L2 persisted snapshots under WS BBO effective mode, and diagnose blocks `local_l2_residual_runtime_enabled` on any Local-L2 runtime residue. |
| [CL-070-8-concurrent-active-position-capacity-closure](daily/2026-06-11.md#cluster-cl-070-8-concurrent-active-position-capacity-and-scoped-pending-close-gate-closure) | fixed, deployed, cloud verified | high | `live-runtime`, `entry-selection`, `entry-dispatch`, `diagnose-live`, `production-observability`, `v1-parity` | `eight-concurrent-active-position-capacity-and-scoped-pending-close-gates` | 2026-06-11 | post-deploy normal active position reported as high-risk/capacity blocker | `ec2af59` | 2026-06-11 local: new targeted guard `3 passed`; related diagnose/runtime/freshness/local-L2/production-health regression `326 passed`; compileall and diff-check passed. Cloud: manifest and singleton passed; `verify_production_services.py --json` `ok=true`; since-deploy diagnose `healthy`, `risk=low`, `gate_passed=true`, `version_mismatch=false`, all venues flat/no-open-orders, `max_concurrent_positions=8`, `remaining_position_slots=8`. | `8 concurrent active-position capacity plus scoped pending-close gates` | Normal balanced active positions within `max_concurrent_positions=8` are active lifecycle/capacity evidence, not high-risk abnormal state. Runtime selection/dispatch tests now prove spare capacity still dispatches another symbol and pending close/reconciliation gates block only the matching symbol/venue pair. No strategy/OI/liquidity/admission/budget/config relaxation. |
| [CL-068-code-side-no-entry-blocker-attribution-closure](daily/2026-06-11.md#cluster-cl-068-code-side-no-entry-blocker-attribution-closure) | local fixed; deploy pending | high | `production-observability`, `diagnose-live`, `offline-analysis`, `ws-bbo`, `snapshot-freshness`, `recovery-probe`, `passive-close`, `v1-parity`, `exchange-docs` | `post-deploy-one-open-code-side-blocker-attribution-gap + strategy-oi-liquidity-filtered-view` | 2026-06-11 | read-only review of previous deployment one-open window | working tree | 2026-06-11 local: production blocker analyzer slice `11 passed`; diagnose code-side/nonblocking health slice `2 passed`; passive-close ACK-only guard slice `2 passed`; broader regression pending. | `Code-side no-entry blocker view without trading semantic relaxation` | Analyzer and diagnose now expose `code_side_blocker_view` with code/data-path buckets after filtering strategy, liquidity, and OI. The fix does not change strategy thresholds, OI/liquidity rules, WS-BBO budgets, Bybit ACK-only semantics, production config, order flow, restarts, or deployment. Nonblocking bulk probe timeouts remain health evidence rather than acceptance-gate failures. |
| [CL-067-passive-close-terminal-normalization-and-one-sided-risk-reduction](daily/2026-06-10.md#cluster-cl-067-passive-close-terminal-normalization-and-one-sided-risk-reduction) | fixed, deployed, cloud verified | critical | `passive-close`, `live-runtime`, `diagnose-live`, `venue-okx`, `venue-bybit`, `recovery-core`, `v1-parity`, `production-observability` | `post-deploy-8-opens-6-passive-close-abnormal-terminal + one-sided-fast-flatten-risk + drift-owner-race` | 2026-06-10 | post-deploy 8 opens / 6 abnormal passive close terminal review | `f8c2b36`, deployed via `c1381b3` | 2026-06-10 local: RED/GREEN passive terminal/OKX amend/runtime owner `3 passed`; diagnose summary `1 passed`; passive close `129 passed`; close execution `50 passed`; diagnose `59 passed`; recovery core `110 passed`; recovery restart `33 passed`; live startup preflight `94 passed`; compileall passed. Cloud: remote `git_head=c1381b3`, `.deploy_version=c1381b3`, services active with `NRestarts=0`, verifier `ok=true`, diagnose `healthy`/`risk=low`/`gate_passed=true`, all venues flat/no-open-orders, and passive-close terminal counters show no current residue. | `V1-style passive close terminal projection plus owner-aware drift skip` | Live-flat passive cleanup now emits `exit.passive_close_resolved`; OKX amend failures reconcile and retain live original order when truth says open/partial; runtime drift skips passive close owner; one-sided reduce-only flatten requires open-order-flat proof; diagnose exposes terminal and fast-flatten counters. Current deployed window has no stale fail-closed-after-flat, fast-flatten, problem, open/pending, or recovery-blocked residue. |
| [CL-066-moveusdt-bybit-ack-only-duplicate-cid-diagnose-closure](daily/2026-06-10.md#cluster-cl-066-moveusdt-bybit-ack-only-duplicate-cid-diagnose-closure) | fixed, deployed, cloud verified | high | `close-executor`, `passive-close`, `diagnose-live`, `venue-bybit`, `order-reconciliation`, `production-observability`, `v1-parity` | `moveusdt-bybit-ack-only-duplicate-client-id-diagnose-leak + zero-quantity-filled-projection` | 2026-06-10 | deployment-window MOVEUSDT automatic close evidence: Bybit ACK-only accepted id without fill confirmation, then same client id `110072 OrderLinkedID is duplicate` | `f5541fe`, deployed via `c1381b3` | 2026-06-10 local: RED/GREEN covered MOVEUSDT ACK-only plus same-client-id duplicate diagnose closure and duplicate-client live-flat close projection; focused regression `5 passed`; full diagnose+close executor files `108 passed`; passive-close/venue-transport/recovery-core suite `540 passed`; compileall passed. Cloud: remote `git_head=c1381b3`, `.deploy_version=c1381b3`, verifier `ok=true`, diagnose `healthy`/`risk=low`/`gate_passed=true`, `top_exchange_errors=[]`, `order_error_evidence=[]`, `resolved_order_truth_gap_summary.count=0`. | `Bybit ACK-only truth-gap identity binding plus duplicate-client live-flat projection without zero-fill` | Diagnose now treats ACK-only `order.uncertain` with accepted ids/no fill confirmation as implicit truth-gap registration, binds nested exchange-error/request-context ids, and filters matching Bybit `110072` artifacts only behind reconciliation/terminal/current flat truth. `CloseExecutor._submit_close_leg_with_retry()` emits `exit.close_duplicate_client_order_resolved_live_flat` and returns `terminal_flat` instead of writing `order.filled quantity=0` when duplicate-client reconciliation has no fill and live size is zero. The current deployed window has no active MOVEUSDT ACK-only/110072 diagnostic residue. |
| [CL-065-hyperliquid-exchange-truth-account-identity-false-green](daily/2026-06-10.md#cluster-cl-065-hyperliquid-exchange-truth-account-identity-false-green) | fixed, deployed, cloud verified | critical | `venue-hyperliquid`, `diagnose-live`, `exchange-truth`, `production-health`, `v1-parity`, `exchange-docs` | `hyperliquid-clearinghouseState-signer-false-empty + configured-account-overwritten + diagnose-false-green-flat` | 2026-06-10 | production read-only evidence: configured Hyperliquid account had 18 nonzero positions while signer/API-wallet address returned empty `assetPositions` | `7890949`, deployed via `c1381b3` | 2026-06-10 local: RED/GREEN covered configured-account exchange truth, account-wallet signer/account mismatch fail-closed, diagnose wallet-mode loading, and sanitized credential identity; related diagnose/health suite `104 passed`; full venue transport `401 passed`; `git diff --check` passed; GitNexus detect-changes low risk with affected processes `0`. Cloud: remote `git_head=c1381b3`, `.deploy_version=c1381b3`, verifier `ok=true`, diagnose `healthy`/`risk=low`/`gate_passed=true`, Hyperliquid credential identity shows `wallet_mode=api_wallet`, account/signer present, `account_matches_signer=false`, and current configured-account truth flat/no-open-orders. | `Hyperliquid configured account identity preserved for exchange truth; API/agent wallet signer separated from account state` | Hyperliquid exchange truth now preserves explicit `account_address` in both API/agent-wallet and account-wallet modes, derives from `wallet_private_key` only when no account is configured, and diagnose loads `LIGHTFEE_HYPERLIQUID_WALLET_MODE`. Account-wallet signer/account mismatch fails closed for trading preflight. Diagnose emits masked/hash credential identity plus `account_matches_signer`, making future signer/account drift visible without leaking credentials. The prior 18-position exposure is no longer present in the current deployed configured-account truth window. |
| [CL-064-pending-entry-terminal-no-fill-maker-open-order-owner-retention](daily/2026-06-10.md#cluster-cl-064-pending-entry-terminal-no-fill-maker-open-order-owner-retention) | fixed, deployed, cloud verified | critical | `live-runtime`, `pending-entry`, `entry-reconciliation`, `recovery-ledger`, `bybit`, `hyperliquid`, `v1-parity`, `production-risk` | `pending-entry-terminal-no-fill-open-maker-order-owner-drop + both_venues_zero_abandon + recovery-release-with-live-open-order` | 2026-06-10 | production read-only evidence after latest pending-entry recurrence review | `66a3688` | 2026-06-10 local: focused terminal no-fill branches `6 passed`; recovery core/ledger `34 passed`; target regression `609 passed`; compileall passed; PE-14 RED/GREEN `4 passed`; terminalizer suite `11 passed`; full pytest `3782 passed`, `9 skipped`, `1 warning`; GitNexus freshness and impact were run before edits with HIGH target-symbol risk and detect-changes low after implementation. Cloud: runtime commit `66a3688` deployed, manifest critical files passed, `.deploy_version=66a3688`, verifier `ok=true` with `0` critical and `0` warning, since-deploy diagnose `healthy`/`risk=low`/`gate_passed=true`, all configured exchange truth flat/no-open-orders, live/sidecar active with `NRestarts=0`, warning logs empty. | `V1 pending-entry terminal no-fill open-order truth before stale abandon/recovery release` | Terminal/no-fill passive progress no longer proves the maker owner is gone while realtime open-order truth is unavailable or still matches the maker order id/client id. `_pending_entry_has_unresolved_maker_order()` and abort cleanup now use an actual-fill-only maker-completed fast path, retain pending on matching open orders or truth gaps, and only allow flat abandon when open-order truth explicitly has no match. Matching live maker orders keep recovery risk-only blocked and prevent `recovery.ledger_clear`; PE-14 supervision stale-backlog clear now routes through the same terminalizer authority. No manual order/cancel/runtime-state mutation was used in production closure. |
| [CL-063-ws-bbo-entry-quote-evidence-and-snapshot-diagnose-split](daily/2026-06-10.md#cluster-cl-063-ws-bbo-entry-quote-evidence-and-snapshot-diagnose-split) | fixed, deployed, cloud verified | high | `live-runtime`, `entry-readiness`, `snapshot-freshness`, `post-only-bbo`, `diagnose-live`, `local-l2-legacy` | `ws-bbo-entry-quote-evidence-post-only-reprice-snapshot-diagnose-split` | 2026-06-10 | post-deploy review of issue 3-6 plus misleading Local L2 evidence count | `9e92718` | 2026-06-10 local: focused RED/GREEN passed; entry/WS BBO `183 passed`; entry execution slice `8 passed, 47 deselected`; diagnose/offline `104 passed`; live harness slice `85 passed, 14 deselected`; compile passed; GitNexus detect-changes high with stale-index caveat. Cloud: `.deploy_version=9e92718`, critical runtime hashes matched local, sidecar/live active, singleton PASS, `verify_production_services.py --json` `ok=true`, since-deploy diagnose `healthy`/`risk=low`, exchange truth flat/no-open-orders, `l2_evidence=0`, `snapshot_evidence=0`, journal warning/error scan empty. | `Fresh WS BBO entry quote evidence plus post-only repricing and snapshot evidence split` | Active `ws_bbo_quote_lease` provider now resolves only stale sidecar entry quote evidence with fresh same-venue/symbol WS BBO instead of blocking, while sidecar missing/invalid quote and stale sidecar quote without fresh WS BBO remain fail-closed. Resolved quote metrics are recorded as fresh WS BBO evidence instead of stale sidecar evidence. Fresh post-only would-cross maker prices are repriced once to maker-side BBO before blocking. Diagnose separates `snapshot_evidence` from true Local L2 evidence, so `snapshot_degraded` no longer creates a false Local L2 recurrence. |
| [CL-072-entry-admission-prefilter-and-v1-state-machine-evidence](daily/2026-06-07.md#cluster-cl-072---entry-admission-prefilter-and-v1-state-machine-evidence) | local fixed; deploy pending | critical | `live-runtime`, `entry-admission`, `production-health`, `passive-close`, `residual-repair`, `venue-okx`, `exchange-truth`, `v1-parity`, `production-observability` | `post-deploy.issue-1-4-9.live_tick_stale_false_critical + passive_close_ack_only_no_fill_gap + residual_repair_ack_only_no_fill_gap + okx_amend_50115_cancel_replace` | 2026-06-08 | post-deploy issue review after latest main deployment; issue 3 and issue 11 explicitly excluded | working tree | 2026-06-08 local: compileall passed; targeted regression suite `31 passed`; full pytest `3703 passed, 9 skipped, 1 warning`; diff-check passed. | `Shared order-submit uncertainty evidence plus narrow health tick-stale suppression and OKX amend cancel-replace fallback` | Accepted-order/no-fill submit uncertainty is now shared across pending entry, passive close, and residual repair. `live_tick_stale` no longer becomes a standalone blocker when clean flat exchange truth plus recent runtime progress proves the loop is alive, while non-high exchange truth still reports separately. OKX amend endpoint `405/50115 Invalid request type` routes to existing cancel-replace without changing order signing, CID, sizing, close executor, cancel semantics, residual clear, or recovery flatten execution. |
| [CL-049-post-cl048-seiusdt-open-maker-order-terminality](daily/2026-06-04.md#cluster-cl-049-post-cl048-seiusdt-open-maker-order-terminality) | fixed, deployed, cloud verified | critical | `live-runtime`, `pending-entry`, `entry-reconciliation`, `exchange-truth`, `diagnose-live`, `bybit`, `hyperliquid`, `v1-parity`, `production-risk` | `post-cl048.bybit-seiusdt-open-maker-order-local-flat + zero-fill-finalize-cleared-pending + diagnose-gate-failed-conclusion-healthy + hyperliquid-insufficient-margin-no-admission-block` | 2026-06-04 | post-CL048 production diagnose found Bybit `SEIUSDT` maker order open while local pending/open state was empty; events showed zero-fill finalize removed the owner | `1e082d9` | 2026-06-04 local: RED/GREEN covered maker open-order truth blocking zero-fill finalize, Hyperliquid insufficient-margin admission block, and diagnose gate false-green; focused tests `1 passed`, `9 passed`, `1 passed`; file suites `16 passed`, `14 passed`, `42 passed`; adjacent startup/entry/runtime/passive suite `350 passed`; full pytest `3438 passed`, `9 skipped`, `1 warning`; compileall/diff-check/manifest passed; GitNexus detect-changes `medium`. Cloud: fast-forwarded to `1e082d9`, `.deploy_version=1e082d9`, manifest PASS, focused tests `11 passed`, sidecar/live active with `NRestarts=0`, final `verify_production_services.py --json` `ok=true`, and service-env diagnose `healthy`/`risk=low` with all-venue flat/no-open-orders truth. | `V1 pending-entry terminality live open-order truth plus deterministic Hyperliquid admission evidence` | V2 now checks maker live open-order truth before zero-fill `passive_unfilled`, retains/backoffs pending entries with matching open maker orders, classifies Hyperliquid insufficient-margin as a deterministic admission block for initial and pending hedge paths, and prevents diagnose from reporting healthy when the production acceptance gate has blockers. The pre-existing SEIUSDT orphan open order was absent from final read-only truth; no manual order or runtime-state mutation was used. |
| [CL-048-post-deploy-seiusdt-pending-entry-live-truth-mismatch](daily/2026-06-04.md#cluster-cl-048-post-deploy-seiusdt-pending-entry-live-truth-mismatch) | fixed, deployed, cloud verified | critical | `live-runtime`, `pending-entry`, `startup-recovery`, `entry-reconciliation`, `exchange-truth`, `bybit`, `hyperliquid`, `v1-parity`, `production-risk` | `post-deploy.bybit-seiusdt-live-position-local-flat + reconciliation.rejected_pending_retained_with_fill + startup_recovery_pending_work_without_open_positions` | 2026-06-04 | post-`9e93c90` deploy verification found Bybit `SEIUSDT` live long while local state had no open position and retained one pending entry | `8be067e` | 2026-06-04 local: GitNexus index refreshed to `772614d`; private method impact unresolved by symbol, scoped by query/direct callers and treated HIGH; RED/GREEN SEIUSDT startup and reconciliation fixtures passed; pending-entry file `15 passed`; adjacent startup/entry/runtime/passive suite `350 passed`; V1 recovery `18 passed`; live harness `3 passed`; full pytest `3434 passed`, `9 skipped`, `1 warning`; compileall and diff-check passed. Cloud: fast-forwarded to `30aba89`, manifest PASS, remote focused `365 passed`, services active with `NRestarts=0`, final `verify_production_services.py --json` `ok=true`, and service-env diagnose `healthy`/`risk=low` with all-venue flat/no-open-orders truth. | `Retained rejected pending positive-fill recovery finalizes matched state plus residual repair` | V2 now routes rejected pending entries with positive fill evidence through the existing V1 `_finalize_pending_entry()` path from startup force reconcile, startup recovery, and normal reconciliation. The deployed SEIUSDT recovery emitted `recovery.rejected_pending_positive_fill_finalized`, opened matched state, queued/completed residual repair, verified drift flat, and returned local state to running with open/pending/residual `0/0/0`. |
| [CL-047-diagnose-snapshot-fallback-and-bybit-time-window-evidence](daily/2026-06-04.md#cluster-cl-047-diagnose-snapshot-fallback-and-bybit-time-window-evidence) | code deployed; later contract acceptance closed | high | `production-diagnostics`, `acceptance-gate`, `venue-transport`, `venue-bybit`, `server-time`, `exchange-docs`, `v1-parity` | `diagnose.snapshot_fallback_scoped_blocking_misclassified_insufficient + bybit.10002.timestamp_recv_window_retry_missing + bybit.server_time_seconds_precision_loss` | 2026-06-04 | post-deploy review separated snapshot fallback insufficient-evidence and Bybit 10002 from passive-close mutual fighting | `9e93c90` | 2026-06-04 local: GitNexus impact LOW for diagnose snapshot helper, CRITICAL for private REST transport helpers; RED/GREEN covered scoped vs global snapshot fallback, Bybit millisecond server-time parse, Bybit 10002 retry, and success envelope no false retry; diagnose suite `41 passed`; transport suite `392 passed`; full pytest `3432 passed, 9 skipped, 1 warning`; remote manifest PASS and focused suite `612 passed`; services restarted active/running; the later pending-entry live-truth contract closure at `68a979b` closed the production acceptance blocker | `Candidate-scoped snapshot fallback classification plus Bybit official time-window retry/evidence` | Candidate-scoped snapshot fallback with candidate/domain/venue/age evidence now classifies as `v1_parity`, while global fallback without scope still blocks as insufficient evidence. Bybit server time now uses millisecond precision, 200-envelope `retCode=10002` clears cached offset and re-signs once, and repeated failure emits server-time/header/body evidence instead of leaving an unrooted timestamp-window guess. |
| [CL-046-bybit-entry-passive-close-v1-deadline-loop](daily/2026-06-04.md#cluster-cl-046-bybit-entry-passive-close-v1-deadline-loop) | code deployed; later contract acceptance closed | critical | `live-runtime`, `pending-entry`, `entry-reconciliation`, `exit-decision`, `passive-close`, `close-executor`, `bybit`, `v1-parity`, `production-risk` | `bybit.non-settlement-entry + pending-entry-nonterminal-zero-fill-finalized + stale-zero-reconciliation-erases-fill + passive-close-overdue-backoff-loop + passive-close-fallback-zero-fill-no-fail-closed` | 2026-06-04 | phone screenshots showed Bybit entries/closes outside expected settlement behavior, seconds-long closes, and >8h exposure; V1 comparison required dual-leg terminality, hard passive-close deadline, and no close-loop mutual fighting | `9e93c90` | 2026-06-04 local: GitNexus impact HIGH/CRITICAL for passive close and transport-adjacent flows; RED/GREEN covered pending-entry zero terminality, stale zero fill preservation, overdue passive fallback, no-executor/zero-fill hard breach, hedge hard breach, one-sided live flatten settling, and drift-correction ownership. Passive close `111 passed`; startup preflight `67 passed`; runtime/passive/incident focused gates passed; full pytest `3432 passed, 9 skipped, 1 warning`; remote manifest PASS and focused suite `612 passed`; services restarted active/running; the later pending-entry live-truth contract closure at `68a979b` closed the production acceptance blocker | `V1 pending-entry terminality plus passive-close deadline/fail-closed ownership` | V2 no longer finalizes maker zero-fill without terminal order/fill evidence, no longer lets stale zero reconciliation erase known positive fills, arms overdue passive closes into DUAL_TAKER, converts hard breaches into fail-closed with compensation/live-flat probing, and prevents passive close from submitting maker work in the same tick after live truth has already driven one-sided flatten. Runtime drift correction now stands down while passive-close live action is settling. |
| [CL-044-close-risk-force-v1-parity-gaps](daily/2026-06-04.md#cluster-cl-044-close-risk-force-v1-parity-gaps) | fixed, deployed, cloud verified; full risk-delever L2 sizing follow-up open | critical | `live-runtime`, `risk-supervisor`, `close-executor`, `residual-repair`, `bybit-duplicate-reconcile`, `v1-parity` | `v2.force-close-helper-unwired + death-line-single-side-protection-dual-close + residual-repair-bybit-110072-loop + compensation-duplicate-no-reconcile + risk-protection-pnl-bucket-missing` | 2026-06-04 | V1/V2 source audit after production Bybit residual-repair duplicate loop and force-close question | `6466eb0` | 2026-06-04 local/cloud: GitNexus impact HIGH for `build_risk_execution_plan`, LOW for close/risk helpers; RED/GREEN focused passed; adjacent suite reached `189 passed`; remote compile/manifest/focused tests passed; services active with `NRestarts=0`; post-deploy all-account truth high-confidence flat/no-open-orders across seven venues | `V1 close/risk/force-close semantic parity on active runtime paths` | Runtime now routes due aligned positions as `settlement_force_close`; death-line protection carries a V1 venue/side/stage target and submits exactly one reduce-only IOC leg; residual repair and close compensation reconcile Bybit `110072` before rescheduling/failing; risk/protection close PnL buckets are written. Full V1 risk-delever L2 sizing remains explicitly open because it needs a separate HIGH-impact change. |
| [CL-045-diagnose-empty-symbol-exchange-truth-gap](daily/2026-06-04.md#cluster-cl-045-diagnose-empty-symbol-exchange-truth-gap) | fixed, deployed, cloud verified | high | `production-diagnostics`, `exchange-truth`, `read-only-probes`, `deployment-acceptance`, `exchange-docs` | `diagnose-live.empty-local-symbols-skips-exchange-truth + exchange-truth-unavailable-after-healthy-deploy` | 2026-06-04 | post-`37d7b6f` deploy health was green, but `diagnose_live --since-deploy` could not prove exchange truth because local symbols were empty and no private probe was issued | `6138003` | 2026-06-04 local/cloud: impact LOW/MEDIUM limited to diagnose report flow; RED/GREEN `3 passed`; full diagnose suite `38 passed`; adjacent deploy gate `189 passed`; remote compile/manifest/focused tests passed; post-deploy diagnose reports seven venue `status=ok`, zero positions/orders, `gate_passed=true`, and evidence complete | `Read-only all-account exchange-truth probes when local state has no symbols` | `diagnose_live` now uses all-position and venue-documented all-open-order read-only probes when symbols are empty, creates readonly adapters for all seven live perp venues, defaults truth venues to all seven, loads Hyperliquid wallet/account envs including the production `LIGHTFEE_HYPERLIQUID_PRIVATE_KEY` alias, avoids the cloud `asyncio.get_event_loop()` DeprecationWarning, and marks high-confidence no-error healthy windows as complete evidence. No trading-state mutation or order submission path changed. |
| [CL-043-pending-entry-maker-progress-open-order-truth](daily/2026-06-03.md#cluster-cl-043-pending-entry-maker-progress-open-order-truth) | fixed, deployed, short-window cloud verified; historical Bybit chronology evidence pending | critical | `live-runtime`, `pending-entry-reconciliation`, `venue-transport`, `venue-bybit`, `venue-okx`, `production-observability`, `v1-parity`, `exchange-docs` | `post-bd12acd.pending-entry.live-position-progress-with-uncertain-maker-order + bybit-open-maker-orders-after-local-flat + okx-synthetic-recovery-ordid-51000` | 2026-06-03 | post-`bd12acd` deploy-window read-only audit found two `MEUSDT` Bybit non-reduce-only PostOnly maker orders open after local pending state was absent, plus OKX `51000 Parameter ordId error` for a synthetic recovery order id; final all-venue truth later returned flat/no-open-orders | `9d580d5` | 2026-06-03 local/cloud: production read-only pre-fix state was green/flat but deploy window had `pending_entry.maker_progress_applied` after Bybit `execution_not_found` / `live_position_delta.quantity=608`, two transient Bybit open maker orders, later cleanup/recovery flattening, and OKX `51000` for synthetic `entry-...-recovery-short`. GitNexus index refreshed to `bd12acd`; method-level impact returned UNKNOWN/not found, so blast radius was bounded by query/direct callers. RED/GREEN proved OKX synthetic ids no longer go to `ordId` and uncertain maker live positions no longer apply maker progress; focused `2 passed`; pending-entry `13 passed`; live-harness incident `3 passed`; runtime/abort cleanup `65 passed`; venue evidence `5 passed`; reconciliation/V1 client-id `45 passed`; full transport `382 passed`; live-entry root-fix `134 passed`; full pytest `3392 passed, 9 skipped, 1 warning`; compileall/diff-check passed; detect-changes HIGH with `8 files`, `24 symbols`, `6` OKX transport affected flows and pre-existing `AGENTS.md`/`CLAUDE.md` noise. Cloud fast-forwarded `bd12acd..9d580d5`, wrote `.deploy_version=9d580d5`, manifest `370` files / 14 critical checks passed, remote compileall passed, focused remote suite `86 passed`, services restarted active/running with `NRestarts=0`, post-deploy health green with local open/pending/close/residual `0/0/0/0`, warning logs empty, diagnose Local-L2 missing/stale/sequence-gap `0/0/0`, entry/open-position `0/0`, and credentialed all-account truth showed all 7 venues nonzero positions `0` and open orders `0`. | `V1 passive-maker terminality semantics plus exchange-documented OKX order query ids` | V2 now refuses to apply live-position quantity/price/order-id progress to the passive maker leg unless that leg is reconciled as `filled`; it emits `pending_entry.live_position_progress_deferred` when live position truth exists but maker order terminality is unproven. OKX order status now sends numeric exchange ids as `ordId` and synthetic/client ids as `clOrdId`. Hedge live-position hydration remains unchanged for the PRL-style V1 recovery path. No Bybit orphan maker or OKX synthetic `ordId` recurrence appeared in the short post-deploy window. Remaining evidence is historical: credentialed Bybit order/execution-history around the two pre-fix MEUSDT order ids/client ids. |
| [CL-042-entry-perp-liquidity-qualification-v1-parity](daily/2026-06-03.md#cluster-cl-042-entry-perp-liquidity-qualification-v1-parity) | fixed, deployed, short-window cloud verified | high | `live-runtime`, `config`, `sidecar-snapshot`, `entry-liquidity-qualification`, `v1-parity`, `production-observability` | `v2.entry-perp-liquidity-qualification.persisted-only-no-runtime-gate + v1-oi-floor-structural-memory-missing` | 2026-06-03 | V1/V2 static parity audit found V1 `EntryLiquidityQualificationState` and live OI floor/structural suppression semantics while V2 only persisted `entry_liquidity_qualification_records` and never consumed them at runtime | `0c8356e` | 2026-06-03 local/cloud: GitNexus impact CRITICAL for `StrategyConfig` and `_load_strategy`, UNKNOWN/not found for runtime private method; RED/GREEN proved missing threshold defaults, ignored map/legacy config, missing V1 state module, fresh low-OI quote incorrectly dispatched, and persisted structural rows ignored. Focused RED set `9 passed`; runtime snapshot freshness `22 passed`; snapshot fallback harness `3 passed`; runtime entry flow `32 passed`; entry-local-L2 `138 passed`; startup preflight `66 passed`; exchange admission `12 passed`; sidecar schema/source/lifecycle/candidate identity `42 passed`; venue market-data/transport `415 passed`; full pytest `3390 passed, 9 skipped, 1 warning`; compileall/diff-check passed; GitNexus detect-changes HIGH with `13 files`, `89 symbols`, and `7` affected `Main -> Default_strategy` flows via `_load_strategy`; cloud fast-forwarded `c63ec7d..0c8356e`, remote focused suite `321 passed`, manifest `370` files and 14 critical checks passed, services active/running with `NRestarts=0`, post-deploy health green with local open/pending/close `0/0/0`, Local-L2 missing/stale/sequence-gap `0/0/0`, `execution.entry_liquidity_blocked=300`, entry/open-position events `0/0`, and `version_mismatch=false`; exchange truth unavailable so consistency confidence low. | `V1 entry perp-liquidity qualification state and OI hard gate replicated in V2 live candidate filtering` | V2 now applies V1 entry perp-liquidity defaults and config aliases, blocks live entries when per-leg open interest is below floor, emits volume-below-floor as advisory, persists qualification records only from the real filtering path, and honors structural suppression/probe cadence. No WS BBO, Local-L2 lifecycle/data-plane, order submit, sizing, close, recovery, or residual repair behavior changed. |
| [CL-041-entry-final-gate-local-l2-skew-parity](daily/2026-06-03.md#cluster-cl-041-entry-final-gate-local-l2-skew-parity) | fixed, deployed, short-window cloud verified | high | `live-runtime`, `entry-final-gate`, `local-l2`, `v1-parity`, `production-observability` | `v2.entry-final-gate.local-l2-observed-at-skew-not-blocked + v1-execution-skew-missing` | 2026-06-03 | V1/V2 static parity audit found V1 `entry_final_gate_skew_result` but V2 only had `entry_final_gate_max_skew_ms` config and no dispatch-time skew gate | `0c8356e` | 2026-06-03 local/cloud: GitNexus index refreshed; runtime.py private methods were not resolvable by method-level impact, so GitNexus query plus direct caller grep bounded blast radius to `runtime.tick -> _dispatch_entry` and dispatch tests. RED reproduced V2 dispatching with 200ms dual-book skew over a 100ms budget; GREEN focused `1 passed`; runtime entry flow `32 passed`; entry-local-L2 `138 passed`; runtime snapshot freshness `20 passed`; exchange admission harness `12 passed`; startup preflight `66 passed`; compileall/diff-check passed; initial detect LOW with zero affected processes; cloud fast-forwarded `c63ec7d..0c8356e`, remote focused suite `321 passed`, manifest `370` files and 14 critical checks passed, services active/running with `NRestarts=0`, post-deploy health green with Local-L2 missing/stale/sequence-gap `0/0/0`, `runtime.entry_blocked_final_gate=0`, entry/open-position events `0/0`, and `version_mismatch=false`; exchange truth unavailable so consistency confidence low. | `V1 entry_final_gate_skew_result replicated in V2 local-L2 dispatch gate` | V2 now blocks live local-L2 entries when both leg books are individually ready but mutually skewed beyond `entry_final_gate_max_skew_ms`, emitting `runtime.entry_blocked_final_gate` and `review.candidate_rejected`. No Local-L2 data-plane, WS BBO quote lease, sizing, order submit, close, recovery, or residual repair behavior changed. |
| [CL-040-v1-ledger-bridge-lifecycle-attribution](daily/2026-06-03.md#cluster-cl-040-v1-ledger-bridge-lifecycle-attribution) | fixed, deployed, cloud focused verified | high | `persistence`, `projection-writer`, `lifecycle-ledger`, `ledger-attribution`, `offline-analysis` | `entry-to-exit.lifecycle-ledger-missing + compensation-terminal-unclassified + recovery-flat-ledger-blind-spot` | 2026-06-03 | post-CL038/CL039 runtime safety was green, but V2 still lacked V1's rebuildable lifecycle ledger bridge for compensation, recovery, terminal-problem, order, fill, and position attribution | `09292c1` | 2026-06-03 local/cloud: GitNexus context limited blast radius to offline reports/projection/tests; GitNexus impact CLI hung, so class/function context was used as manual impact evidence; RED/GREEN proved missing ledger tables, backfill count correctness, and `execution.compensation_failed` fact-table mapping; focused `6 passed`; persistence/offline `114 passed`; close/replay adjacent `179 passed`; full pytest `3381 passed, 9 skipped, 1 warning`; compileall/diff-check passed; GitNexus detect-changes LOW with 0 affected processes; cloud manifest/compileall/focused `114 passed`; production health green with local open/pending/close/residual `0/0/0/0`. | `V1-compatible journal-derived lifecycle ledger bridge without changing live execution` | V2 now has V1-style `trade_ledger_events`, `position_ledger`, `position_pnl_facts`, `order_ledger`, and `fill_ledger` projections. `ProjectionWriter` records entry/open, recovery live, order submit/fill/failure, exit close, recovery flat, compensation success/failure, and terminal-problem events with `truth_level`, while recovery remains journal-first for replay. No live order submit, close executor, recovery policy, Local-L2, WS BBO, sizing, or residual repair behavior changed. Historical unallocated rows still need credentialed exchange order-history replay. |
| [CL-038-single-leg-force-close-and-ledger-attribution](daily/2026-06-03.md#cluster-cl-038-single-leg-force-close-and-ledger-attribution) | local full gate green, deploy pending | critical | `passive-close`, `close-executor`, `position-lifecycle`, `production-observability`, `ledger-attribution`, `exchange-truth` | `single-leg.normal-flatten-fails-no-force-close + ledger-row-outside-position-window + terminal-position-evidence-missing + okx-copy-trade-not-main-loss` | 2026-06-03 | recent 72-hour exchange-truth PnL split showed single-leg quick flatten loss, long/unclosed single-leg samples, and `-14.213442883` unallocated ledger rows; cloud OKX attribution showed current OKX positions empty and only 8 external/copy-style OKX bill rows, net `+0.07306395`, so OKX follow trading is not the large unallocated-loss source | working tree | 2026-06-03 local/cloud read-only: GitNexus impact LOW for `_flatten_live_one_sided_position`, HIGH for `_clear_live_flat_state`, LOW for `_compensate_close_leg_exposure`; focused RED/GREEN one-sided IOC failure force-close/lifecycle terminal test `1 passed`; passive fallback class `14 passed`; close compensation class `6 passed`; compileall/diff-check passed; full pytest `3370 passed, 9 skipped, 1 warning`; GitNexus detect-changes HIGH with `8` affected close/compensation flows, expected for `_clear_live_flat_state`; cloud verify current health green with local open/pending/close/residual `0/0/0/0` and exchange truth mismatches `[]`; OKX current open positions `[]`. | `V1-compatible one-sided force close with problem marking and terminal ledger anchors` | If normal live one-sided reduce-only IOC flattening fails, passive close now routes through existing close compensation hard-stop, records `exit.passive_close_live_one_sided_force_close_problem`, and clears only after live truth proves flat. Flat cleanup emits `runtime.position_lifecycle_terminal` with terminal reason, problem flag/reason, and client/order ids so future ledger rows can be attributed by durable anchors instead of broad time windows. Compensation close-leg requests now explicitly use IOC. No entry selection, Local-L2, WS BBO, sizing, residual-repair scheduling, or startup recovery behavior changed. |
| [CL-036-startup-live-position-probe-static-universe-fanout](daily/2026-06-03.md#cluster-cl-036-startup-live-position-probe-static-universe-fanout) | local focused green, deploy pending | critical | `live-runtime`, `startup-recovery`, `venue-binance`, `production-health`, `production-observability` | `startup-live-position-probe.empty-requested-symbols + static-config-universe-fanout + binance-positionrisk-429 + live_tick_stale` | 2026-06-03 | first `a272b6b` cloud deploy: services active with `NRestarts=0`, but health stayed red with `live_tick_stale`; logs showed startup recovery fanning out Binance `GET /fapi/v2/positionRisk` until `HTTP 429 Too Many Requests` | working tree | 2026-06-03 local: GitNexus impact HIGH for `LiveRuntime._fetch_startup_live_position_snapshots`; RED reproduced clean startup with empty requested symbols and two static config symbols calling per-symbol `fetch_position`; GREEN focused `1 passed`; startup preflight `47 passed`; recovery probe harnesses `11 passed` | `Bound startup static-config live-position fallback without weakening requested-symbol recovery` | Clean startup no longer falls back from empty requested/recovery symbols to a large configured trading universe. Static config fallback is kept only for a single explicit symbol; larger universes emit `recovery.live_position_static_config_probe_skipped` and skip per-symbol private REST fanout. Requested recovery symbols, bulk position APIs, mismatch flattening, balanced hydration, and trading execution paths are unchanged. |
| [CL-035-post-e087513-long-window-follow-up](daily/2026-06-03.md#cluster-cl-035-post-e087513-long-window-follow-up) | local full gate green, deploy pending | high | `venue-transport`, `pending-entry-reconciliation`, `live-runtime`, `passive-close`, `entry-readiness-provider`, `production-observability`, `exchange-docs` | `post-e087513.binance-recovery-placeholder-orderid + ws-bbo-close-missing-quote-evidence + ws-bbo-budget-exhausted-not-subscription-regression` | 2026-06-03 | post-`e087513` long-window review: current service health green and local open/pending/residual counts `0`, but deploy-window events showed Binance `-1102` finalize errors on `CLOUSDT/TRIAUSDT/PEOPLEUSDT`, passive close missing-price events on `STEEMUSDT/TRIAUSDT`, and WS BBO blocks split into budget-exhausted/stale-quote rather than old waiting-for-subscription | working tree | 2026-06-03 local: GitNexus index refreshed with `runtime.py` included; impact LOW for `VenueTransport._fetch_order_status_binance`, LOW for `LiveRuntime._resolve_ws_bbo_close_mid`, LOW for reviewed-but-unchanged `PassiveCloseExecutor._submit_maker_order`; RED/GREEN proved Binance recovery placeholders use `origClientOrderId` and WS BBO close fallback missing quote emits structured evidence; focused `2 passed`; adjacent venue/close/client-order-id suites `14 passed`; impacted files `381 passed` and `20 passed`; compileall/diff-check passed; full pytest `3367 passed, 9 skipped, 1 warning`; GitNexus detect-changes MEDIUM with 2 intended affected flows | `Binance client-order reconciliation + close-price evidence gap closure without changing WS BBO budget/TTL semantics` | Binance USD-M reconciliation now sends `orderId` only for numeric exchange ids; synthetic V2 recovery placeholders use `origClientOrderId` when client id truth exists. Active WS BBO close fallback now emits `runtime.close_price_evidence_missing` for no cache/quote/budget/error branches while preserving fail-closed `0.0`. WS BBO `budget_exhausted` is the intended CL033 classification, not old subscription-waiting recurrence; stale quote remains an exchange-doc/probe track if it blocks trading. |
| [CL-034-okx-contract-fill-quantity-unit-drift](daily/2026-06-02.md#cluster-cl-034-okx-contract-fill-quantity-unit-drift) | fixed, deployed, short-window cloud verified | high | `venue-transport`, `pending-entry-reconciliation`, `residual-repair`, `production-observability`, `exchange-docs` | `okx-swap-fill-contracts-treated-as-base + local-open-position-unit-drift + position-drift-correction-fail` | 2026-06-02 | cloud deploy `a0b8b84`: current state later recovered green, but deploy-window events showed OKX local/live unit drift (`SIGN` 21 vs 2100, `HOME` 3 vs 300, `LAB` 0.1 vs 1.0), `runtime.position_drift_correction_failed=2`, and `pending_entry.finalize_fill_reconciliation_error=2` | e087513 | 2026-06-02 local: GitNexus impact LOW for `_parse_order_fill`, `_parse_order_status_okx`, `_okx_contract_size_for_venue_symbol`, and `VenueTransport.place_order` UID. RED/GREEN proved OKX direct `fillSz` and order-status `accFillSz` are converted from contracts to base quantity via trusted `ctVal`; focused `2 passed`; `tests/test_venues_transport.py` `380 passed`; adjacent live-entry/startup/pending-entry/OKX private WS ctVal/entry-local-L2/runtime-entry/WS-BBO suites `466 passed`; compileall and diff-check passed; full pytest `3365 passed, 9 skipped, 1 warning`; GitNexus detect-changes HIGH (`4 files`, `27 symbols`, `12` affected `place_order`/`fetch_order_status` flows), with manual hunk audit limited to OKX contract-to-base fill/reconciliation conversion plus tests/docs. | `OKX SWAP order fill and reconciliation quantity contract-to-base conversion` | OKX order request sizing remains base-to-contracts. OKX direct fill parsing now converts `fillSz`/filled quantity back to base using preflight `ct_val`, and OKX order-status reconciliation lazily fetches official contract size only when a positive `accFillSz`/`fillSz` exists, then records base quantity plus `quantity_units=contracts_to_base`, `contract_qty`, and `ct_val`. No Local-L2, WS BBO, order request sizing, post-only, close, residual repair, or recovery policy changed. |
| [CL-033-ws-bbo-budget-exhaustion-and-refresh-evidence](daily/2026-06-02.md#cluster-cl-033-ws-bbo-budget-exhaustion-and-refresh-evidence) | fixed, deployed, short-window cloud verified | high | `entry-readiness-provider`, `live-runtime`, `production-observability` | `ws-bbo-quote-lease.budget-out-waiting-for-subscription + tracked-refresh-outcome-evidence-gap + provider-follow-up` | 2026-06-02 | post-CL032 cloud deploy `a0b8b84`: Local-L2 stayed clean and health was green, but WS BBO provider selection showed `waiting_for_subscription=92`, `stale_quote=22`, `missing_quote=1`; current local open/pending later returned `0/0` | e087513 | 2026-06-02 local: GitNexus impact LOW for `WsBboQuoteLeaseEntryReadinessProvider`, LOW for `_validate_quotes` UID, and LOW for `VenueBboDataPlane`; GitNexus could not resolve `LiveRuntime` private helpers, so runtime blast radius was manually limited to WS BBO candidate activation and the pre-provider blocker. RED/GREEN proved tracked stale no-quote REST evidence, tracked missing no-quote REST evidence, and budget-excluded candidate classification; focused `3 passed`; provider factory `21 passed`; WS BBO/probe `15 passed`; planner dispatch `22 passed`; entry-local-L2 `138 passed`; runtime entry flow `31 passed`; runtime smoke/lane scheduling `32 passed`; snapshot freshness `19 passed`; compileall and diff-check passed; full pytest `3363 passed, 9 skipped, 1 warning`; GitNexus detect-changes MEDIUM, with affected flows limited to `_validate_quotes` quote-refresh/provider evidence flows plus docs/tests. | `WS BBO budget-exhausted classification + tracked REST refresh outcome evidence without changing trading semantics` | Runtime now records per-venue WS BBO budgeted/excluded keys and classifies budget-out no-quote candidates as `entry_ws_bbo_quote_lease_budget_exhausted` with per-leg budget evidence. Real untracked missing coverage remains `entry_ws_bbo_quote_lease_waiting_for_subscription`. Provider blocks for tracked missing/stale quotes now include per-leg REST refresh attempt/outcome evidence, so future production windows can separate `no_quote`, invalid, error, cache rejection, and successful cache refresh. No budget increase, TTL widening, Local-L2, order submit, sizing, close, residual repair, or recovery semantics changed. |
| [CL-032-local-l2-hot-ws-lifecycle-coverage](daily/2026-06-02.md#cluster-cl-032-local-l2-hot-ws-lifecycle-coverage) | fixed, deployed, short-window cloud verified; longer watch continues | high | `live-runtime`, `local-l2-data-plane`, `production-observability` | `local-l2.hot-fresh-book-skips-ws-registration + duplicate-ws-stream-no-reconnect + restored-transient-hot-exec-no-owner + bybit-subscription-missing-rebuild` | 2026-06-02 | post-`087d73a` deploy scan: `runtime.local_l2_hot_stale_rebuild=30`, `subscription_missing=26`, mostly Bybit `LABUSDT/HUSDT`, `pool=hot_exec`, `policy_bridge_mode=ws_snapshot_authoritative`; first `2583b11` deploy attempt still had 2 Bybit `subscription_missing` rebuilds from restored transient HOT_EXEC books | `a0b8b84` | 2026-06-02 local: GitNexus impact HIGH for `_ensure_l2_active_for_candidates`, LOW for `LocalL2DataPlane`, LOW for `_restore_local_l2_state`; RED missing HOT WS lifecycle, registered-disconnected duplicate stream, and unowned restored HOT_EXEC tests failed before fixes and passed after; entry-local-L2 suite `135 passed`; local-L2 WS/runtime suites `145 passed`; startup preflight `64 passed`; related Local-L2/entry/startup suite `280 passed`; compileall and diff-check passed; full pytest `3360 passed, 9 skipped, 1 warning`; GitNexus detect-changes LOW after restore follow-up (`affected processes=0`), with manual hunk audit limited to Local-L2 activation/data-plane/restore lifecycle plus docs/tests. Cloud: deployed `.deploy_version=a0b8b846d952586414a52c2edfe5c9fcfb9bd837`, remote related tests `344 passed`; post-restart five-minute diagnose window had `health.ok=true`, no `runtime.local_l2_hot_stale_rebuild`, `l2_evidence.stale_rebuild_count=0`, `missing_l2_or_tick_count=0`, `sequence_gap_count=0`; final verify returned `ok=true`, `critical_count=0`, `warning_count=0`, `risk_mode=running`, `lifecycle=running`, and `exchange_truth_mismatches=[]`. Nonzero `open_position_count=2` / `pending_entry_count=10` is current trading runtime state, not CL032 recurrence evidence. Targeted ruff still reports pre-existing repo lint debt, left untouched. | `Local-L2 HOT WS-authoritative lifecycle coverage and transient snapshot restore pruning without changing trading semantics` | Dynamic Local-L2 activation now checks WS lifecycle for HOT/fresh/non-crossed WS-authoritative or stream-only books and registers/connects missing streams without re-bootstrapping. Needed symbols with already-registered but disconnected streams now trigger `connect_ws_streams()` even when duplicate registration returns `0`. Startup restore skips transient `hot_exec`/`warm` full-book snapshots with no retained/open/pending/passive-close owner, preventing old entry books from surviving restart without WS lifecycle. Stale/crossed rebuild behavior and REST-buffered venue behavior remain unchanged; no quote provider, order submit, sizing, close, residual repair, or recovery semantics changed. |
| [CL-031-ws-bbo-close-price-hint-fallback](daily/2026-06-02.md#cluster-cl-031-ws-bbo-close-price-hint-fallback) | fixed, deployed, cloud triggered/verified | high | `live-runtime`, `passive-close`, `entry-readiness-provider`, `production-observability` | `ws-bbo-quote-lease.close-price-hint-local-l2-coupling + passive-close-price-hint-zero + fresh-bbo-fallback` | 2026-06-02 | post-CL029/030 investigation on `e6cc67f`: `exit.passive_close_missing_l2_or_tick=37`, with samples showing positive venue `tick_size` but `price_hint=0.0`, then `exit.passive_close_missing_l2_tick_escalated=9` | `087d73a` | 2026-06-02 local: GitNexus impact LOW for `LiveRuntime._resolve_local_l2_mid`; RED/GREEN fresh/stale WS BBO close price fallback tests `2 passed`; snapshot freshness `19 passed`; passive resolver/provider/dispatch adjacent set `45 passed`; impacted entry/runtime/snapshot files `183 passed`; related provider/dispatch/WS/runtime suites `106 passed`; compileall and diff-check passed; full pytest `3356 passed, 9 skipped, 1 warning`. Cloud: remote focused tests `74 passed`; `.deploy_version=087d73ab28ac649af9728528e2334472b6074c68`; settled health `ok=true`; post-deploy scan had `runtime.close_price_evidence_fallback=49`, `runtime.close_price_evidence_stale=6` for over-budget WS BBO fallback, and `exit.passive_close_missing_l2_or_tick=0`. | `Fresh WS BBO close price hint fallback when Local-L2 is unavailable` | Close price hint resolution still prefers valid HOT Local-L2. When active provider is `ws_bbo_quote_lease` and Local-L2 cannot provide a positive non-stale mid, runtime can use a fresh positive non-crossed `ws_bbo_cache` quote inside the quote-lease age budget as the close price hint. Stale/invalid WS BBO fallback is rejected with structured evidence. No passive executor, post-only, tick-size, order submit, sizing, residual repair, or recovery semantics changed. |
| [CL-030-ws-bbo-quote-lease-execution-refresh](daily/2026-06-02.md#cluster-cl-030-ws-bbo-quote-lease-execution-refresh) | fixed, deployed, cloud health verified; stale/expired branch trigger pending | high | `entry-readiness-provider`, `live-runtime`, `production-observability` | `ws-bbo-quote-lease.execution-stale-quote-lease + lease-ttl-market-age-budget-mismatch + just-in-time-refresh` | 2026-06-02 | post-CL029 deploy window on `e6cc67f`: provider-stage `missing_quote=0`, `stale_quote=0`, `tracked_false_missing_stream_refs=0`, but execution-stage `runtime.entry_blocked_quote_lease/stale_quote_lease=12` while `entry.opened=8` | `087d73a` | 2026-06-02 local: GitNexus impact HIGH for `_entry_quote_lease_execution_check` and `_dispatch_entry`, LOW for `WsBboQuoteLeaseEntryReadinessProvider` and `_validate_quotes`; RED/GREEN provider TTL refresh and dispatch refresh tests `2 passed`; provider factory `18 passed`; planner dispatch `22 passed`; WS BBO/probe `15 passed`; entry-local-L2 `133 passed`; runtime entry flow `31 passed`; runtime smoke/lane scheduling `32 passed`; snapshot freshness `17 passed`; compileall and diff-check passed; full pytest `3354 passed, 9 skipped, 1 warning`. Cloud: remote focused tests `74 passed`; `.deploy_version=087d73ab28ac649af9728528e2334472b6074c68`; settled health `ok=true`; post-deploy scan had `execution.entry_selected=14` and no `runtime.entry_blocked_quote_lease`, no provider missing/stale quote, and no waiting-for-subscription. | `WS BBO quote lease TTL-aligned provider refresh + just-in-time execution refresh` | `ws_bbo_quote_lease` now uses `entry_quote_lease_ttl_ms` as the quote freshness budget before creating a lease and refreshes tracked stale legs through the existing REST top-book path. `_dispatch_entry` retries the active `ws_bbo_quote_lease` provider exactly once when the final quote-lease gate reports `expired_quote_lease` or `stale_quote_lease`, then reruns the same execution check. Failed refresh remains fail-closed with structured evidence. No Local-L2, order submit, sizing, close, residual repair, or recovery semantics changed. |
| [CL-029-ws-bbo-tracked-stale-quote-rest-top-book-refresh](daily/2026-06-02.md#cluster-cl-029-ws-bbo-tracked-stale-quote-rest-top-book-refresh) | fixed, deployed, cloud health verified; long-window provider acceptance pending | high | `entry-readiness-provider`, `ws-bbo-data-plane`, `production-observability`, `exchange-docs` | `ws-bbo-quote-lease.tracked-stale-quote + aster-event-driven-bookticker + rest-top-book-refresh` | 2026-06-02 | post-CL028 provider window: `tracked_false_missing_stream_refs=0`, `entry_ws_bbo_quote_lease_stale_quote=218`, `stale_quote_lease=102`, Aster `211/218`; read-only probes showed Aster REST can prove valid top-book for quiet WS streams and one-sided top-book for invalid symbols | `3c70b01` | 2026-06-02 local: GitNexus impact LOW for provider class and `_validate_quotes`, MEDIUM for `VenueBboDataPlane`; RED/GREEN provider tests `3 passed`; RED/GREEN REST refresher parser tests `4 passed`; provider factory `17 passed`; WS BBO/probe `15 passed`; planner dispatch `21 passed`; entry-local-L2 `132 passed`; runtime smoke/lane scheduling `32 passed`; snapshot freshness `17 passed`; compileall and diff-check passed; full pytest `3352 passed, 9 skipped, 1 warning`; GitNexus detect-changes reported MEDIUM with affected flows limited to WS BBO start/parse helper flows. Cloud: fast-forwarded to `3c70b01`, remote compileall and focused provider/WS/dispatch tests `53 passed`, deploy marker `3c70b01fb0310da1b996320992b4db221065d816`, sidecar/live active with `NRestarts=0`, health `ok=true`, running/running, local open/pending/residual `0`, exchange truth mismatches `[]`; short scan had `tracked_false_missing_stream_refs=0` and no provider selection sample. | `Tracked WS BBO stale/missing quote REST top-book refresh without relaxing quote freshness` | `ws_bbo_quote_lease` now attempts short-timeout public REST top-book refresh only when the stream is already tracked and the WS quote is missing or stale. Refreshed quotes must pass the same positive bid/ask and non-crossed checks before updating `ws_bbo_cache` and normal quote-lease creation. Untracked candidates still use `entry_ws_bbo_quote_lease_waiting_for_subscription`; unsupported/invalid/one-sided/timed-out REST remains fail-closed. No Local-L2, order placement, sizing, close, residual repair, or recovery semantics changed. |
| [CL-028-ws-bbo-quote-lease-budget-coverage-consistency](daily/2026-06-02.md#cluster-cl-028-ws-bbo-quote-lease-budget-coverage-consistency) | fixed, deployed, short-window cloud verified | high | `entry-readiness-provider`, `ws-bbo-data-plane`, `live-runtime`, `config`, `production-observability` | `ws-bbo-quote-lease.budget-coverage-mismatch + tracked-false-missing-quote + per-venue-budget-default-too-small` | 2026-06-02 | post-CL027 clean provider window: `missing_quote=512`, `stale_quote=5`, `stale_quote_lease=29`, and missing quote stream states dominated by `tracked=false` | `457d16e` | 2026-06-02 local: RED/GREEN for default budget and untracked-candidate guard passed; provider factory `14 passed`; config `32 passed`; WS BBO/probe `11 passed`; dispatch `21 passed`; entry-local-L2 `129 passed`; runtime smoke/lane scheduling `32 passed`; snapshot freshness `17 passed`; full pytest `3345 passed, 9 skipped, 1 warning`; compileall and diff-check passed. Cloud: focused tests `47 passed`; live config reported `entry_ws_bbo_per_venue_budget=10`; sidecar/live active; health `ok=true`, `running/running`, local open/pending/residual `0`; 3-minute scan saw `runtime.ws_bbo_dynamic_ws_started=4`, no new missing-quote, and `tracked_false_missing_stream_refs=0`. GitNexus impact was HIGH for `_ensure_entry_bbo_active_for_candidates` and `_select_entry_candidates`, CRITICAL for `StrategyConfig` import fan-out. | `WS BBO provider budget increase + pre-provider subscription coverage guard` | Default per-venue WS BBO subscription budget is now `10`. Budget-out candidates with no cached quote and no tracked stream are blocked as `entry_ws_bbo_quote_lease_waiting_for_subscription` before provider quote validation, so generic `missing_quote` is reserved for tracked streams/cached quote validation failures. No Local-L2, order placement, sizing, close, residual repair, or recovery semantics changed. Longer production watch remains useful because the short window had no entry selection attempt. |
| [CL-027-pending-entry-live-truth-under-min-hedge-dust](daily/2026-06-01.md#cluster-cl-027-pending-entry-live-truth-under-min-hedge-dust) | fixed, deployed, cloud verified | critical | `pending-entry`, `entry-reconciliation`, `startup-recovery`, `live-truth`, `v1-parity` | `pending-entry.live-truth-mismatch + under-min-hedge-residual-loop + planned-hedge-cid-finalize-query + stale-recovery-block-suppresses-startup-recovery + drift-cleanup-fill-false-negative` | 2026-06-01 | production after CL-026 deploy: old `ARIAUSDT` pending entry blocked live with Bybit long `1238`, Binance short `619`, open orders `0`, local open `0`, pending entry `1`; second deploy converted pending to open and live truth balanced `619/619` but left fail-closed latch | `f1727c1` | 2026-06-01 local: RED/GREEN `2 passed`; pending-entry suite `12 passed`; startup/drift guard `4 passed`; adjacent hedge root-fix `127 passed`; broader focused runtime/diagnose `191 passed`; compileall passed; full pytest `3343 passed, 9 skipped, 1 warning`. Cloud: focused tests `140 passed`, compile/install/restart ok, final health `ok=true`, sidecar/live active, `running/running`, local open/pending/residual `0`, ARIA Bybit/Binance flat/no-open-orders. GitNexus impact was LOW for `_reconcile_pending_state`, `_recover_pending_entry_hedges`, `_maintain_pending_entry_passive_orders`; HIGH for `_ensure_pending_entry_open_fill_details`, `_recover_hydrate_from_live_positions`, and `_maybe_check_active_position_drift`; CRITICAL for `LiveRuntime.start` because it is the live startup entrypoint. | `Pending-entry terminal hedge dust + imbalanced live-truth hydration + stale recovery-block retry + drift cleanup live-truth verification + submitted-vs-planned hedge CID evidence boundary` | Balanced pending entries no longer remain pending forever when only an untradeable hedge dust residual remains. Trusted imbalanced startup live truth now finalizes the balanced quantity and leaves the excess side for existing live-truth drift repair, instead of treating the excess as new hedge demand. Old fail-closed recovery blocks no longer make retained pending-entry recovery unreachable; operator-requested fail-closed and blocks produced by the current startup probe remain preserved. Drift correction now re-probes live truth after incomplete cleanup fill evidence and accepts already-balanced legs instead of latching fail-closed. Finalize no longer queries a planned-only hedge CID; hedge CID lookup now requires submitted-hedge evidence. No Local-L2, WS BBO provider, entry selection, sizing, or order submit semantics changed. |
| [CL-026-ws-bbo-quote-lease-coverage-prewarm-lifecycle](daily/2026-06-01.md#cluster-cl-026-ws-bbo-quote-lease-coverage-prewarm-lifecycle) | fixed, deployed; live window blocked by older pending-entry state | high | `entry-readiness-provider`, `ws-bbo-data-plane`, `live-runtime`, `production-observability`, `exchange-docs` | `ws-bbo-quote-lease.missing-quote-after-provider-switch + ranked-subscription-budget-drift + prewarm-after-freshness-filter + subscription-error-evidence-gap` | 2026-06-01 | production `2bc2096` after provider switch: real opens happened but `entry_ws_bbo_quote_lease_missing_quote=179` and `stale_quote_lease=9` showed coverage/warmup misses | `7745e86` | 2026-06-01 local: focused RED/GREEN `4 passed`; WS BBO/probe `10 passed`; provider factory `13 passed`; planner dispatch `21 passed`; config provider validation `3 passed`; full pytest `3337 passed, 9 skipped, 1 warning`; public read-only WS probes got quotes on all seven venues; cloud deploy focused tests `43 passed` and both services active. Post-deploy production stayed `risk_only` / `fail_closed` because an older `ARIAUSDT` pending-entry/live-truth mismatch remained before this deploy. | `WS BBO quote provider lifecycle, ranked budget coverage, and exchange-doc endpoint alignment` | New provider remains fail-closed, but stream activation now starts from the catalog-supported discovery shortlist before final snapshot quote filtering, per-venue BBO budget preserves candidate ranking order, Binance uses the current official `/public/ws` route for bookTicker, and WS subscription/control errors populate stream-state evidence. No Local-L2, order placement, sizing, close, residual repair, or recovery semantics changed. Fresh production entry-path counters require resolving the separate ARIA pending-entry/live-truth blocker first. |
| [CL-025-post-ae4bd9c-passive-close-maker-leg-live-flat-precheck](daily/2026-05-31.md#cluster-cl-025-post-ae4bd9c-passive-close-maker-leg-live-flat-precheck) | fixed, deployed, cloud verified | high | `passive-close`, `entry-admission`, `residual-repair`, `local-l2`, `probe-diagnostics`, `exchange-docs`, `v1-parity` | `post-ae4bd9c.passive-close-maker-leg-live-flat-before-submit + bybit-110017 + contained-entry-admission-rejects + residual-repair-contained + local-l2/probe-noise` | 2026-05-31 | production `ae4bd9c` watch: `IDUSDT` and `HOMEUSDT` closed but both hit Bybit `110017` because the maker short leg was already flat before first passive maker submit; probe errors still lacked catalog-unavailable reasons | `70a1a8c` | 2026-06-01 local: passive focused RED/GREEN `1 passed`, adjacent passive terminality `14 passed`, full passive suite `106 passed`, historical harnesses `8 passed`/`4 passed`/`4 passed`; probe evidence RED/GREEN `2 passed`, adjacent probe suites `7 passed`/`11 passed`, wider focused suites `62 passed`/`106 passed`; compileall/diff-check passed; GitNexus detect-changes medium. Cloud: remote manifest, compileall, focused regressions `4 passed`, restart, health, warning-log, deploy-window event scan, and HOME/ID read-only probes passed. | `V1 live-truth precheck before first passive-close maker submit + probe catalog-unavailable evidence` | Passive close now probes both live legs before the first maker order. Trusted live-flat maker leg routes to existing one-sided reduce-only IOC flattening for the other leg or existing live-flat recovery if both legs are flat. Untrusted live probes leave the old fail-closed path unchanged. Probe errors now include catalog availability, unavailable reason, adapter capability flags, and catalog error text; catalog-unavailable aggregate events preserve existing unfiltered fallback probing. Production current state has no local open/pending/passive/residual work; deploy-window scan had `entry_opened=0`, `position_opened=0`, `maker_submit_error=0`, and targeted HOME/ID probes are high-confidence flat/no-open-orders. |
| [CL-024-high-frequency-local-l2-catalog-diagnostics](daily/2026-05-31.md#cluster-cl-024-high-frequency-local-l2-catalog-diagnostics) | fixed, deployed, cloud verified | medium | `local-l2-data-plane`, `live-runtime`, `production-observability` | `post-cl023.high-frequency-local-l2-catalog-diagnostics + freshness-state-heartbeat + local-l2-snapshot-ok-success-spam + candidate-skip-repeat` | 2026-05-31 | post-CL-023 optimization review: Local-L2 freshness/bootstrap/success and catalog/selection skip diagnostics remained high-volume but non-trading-semantic | `0c1e620`, manifest `59ac6c1` | 2026-05-31 local: focused RED/GREEN `3 passed` + `2 passed`; adjacent Local-L2/entry/snapshot suite `221 passed`; compileall and diff-check passed; GitNexus code-delta detect was high only because runtime event helpers sit in the main tick path. Cloud: remote manifest, compileall, focused regressions `5 passed`, service restart, health, warning-log, and deploy-window event checks passed with zero opens and zero error-like records. | `Compact repeated Local-L2/catalog diagnostics while preserving first/error/recovery evidence` | Repeated Local-L2 freshness state, WS-authoritative REST deferral, snapshot success, entry Local-L2 selection block, and candidate catalog skip events now emit compact summaries with `suppressed_count`; first events, errors, and recovery successes stay explicit. No readiness, filtering, order, close, or recovery behavior changed. |
| [CL-023-production-observability-log-volume](daily/2026-05-31.md#cluster-cl-023-production-observability-log-volume) | fixed, deployed, cloud verified | medium | `journal`, `live-runtime`, `production-observability` | `post-90fa2bc.production-observability.event-log-unbounded + no-entry-large-payload + snapshot-freshness-invalid-quote-repeat` | 2026-05-31 | production `90fa2bc` watch: services healthy but `runtime/live-events.jsonl` about 577 MiB; no-entry full payloads and repeated snapshot invalid-quote decisions dominated bytes | `42dd100`, manifest `6681321` | 2026-05-31 local: focused RED/GREEN `2 passed` + `2 passed`; adjacent persistence/snapshot/local-L2/degraded-snapshot suite `90 passed`; compileall, diff-check, and GitNexus medium-scope detect passed. Cloud: remote manifest, compileall, focused regressions `4 passed`, service restart, health, warning-log, and log-rotation checks passed. Scoped BRUSDT truth ended high-confidence flat/no-open-orders after startup recovery and stale-order cancellation. | `Event-log rotation + compact repeated runtime diagnostics without trading-path changes` | Existing persistence retention config is now wired into `Journal` rotation. Repeated snapshot freshness source/domain/reason decisions are compacted with `suppressed_count`; repeated no-entry blocker-family diagnostics keep periodic compact counts and omit large candidate arrays. No strategy, order, close, admission, or recovery behavior changed. |
| [CL-022-post-c66f470-evidence-gap-closure](daily/2026-05-31.md#cluster-cl-022-post-c66f470-evidence-gap-closure) | fixed, deployed, cloud verified | high | `live-runtime`, `snapshot-freshness`, `pending-entry`, `diagnose-live`, `production-observability` | `post-c66f470.watch.evidence-gap.invalid-quote-values + pending-entry-finalized-missing-context + diagnose-live-unsupported-symbol-medium-confidence` | 2026-05-31 | production `88a1948`: CL-020/CL-021 did not recur, but invalid-quote/degraded snapshot/finalized-entry/diagnose evidence was insufficient to localize future incidents | `4403ec4`, `90fa2bc` | 2026-05-31 local: focused RED/GREEN `6 passed`; adjacent snapshot/diagnose/pending/admission/local-L2 suite `79 passed`; production-feedback OKX instrument-missing regression `3 passed`; `python3 -m compileall -q lightfee scripts tests` and `git diff --check` passed. GitNexus detect-changes first reported `high` from broad runtime-file flow grouping; manual hunk audit confirmed evidence-only scope. OKX classifier delta detect-changes was low risk with `0` affected processes. Cloud: fast-forwarded to `90fa2bc`, manifest/compileall/focused regressions passed, sidecar/live restarted active with `NRestarts=0`, health ok, warning log empty, and PRL scoped exchange truth high-confidence flat/no-open-orders with OKX `instrument_missing` classified as `unsupported_symbol_flat`. | `Evidence-only closure: quote diagnostics, pending-finalized context, and read-only probe classification without trading-path changes` | Runtime snapshot freshness evidence now carries sanitized quote values and invalid field names into decision/no-entry/degraded samples. Pending-entry finalization records now include `symbol`, `pair_id`, and `finalized_as` across zero-unfilled, unmatched-residual, and open-position branches. `diagnose_live.py` converts venue symbols for private read-only probes and classifies unsupported/non-listed symbols, including OKX `classification=instrument_missing` metadata responses, as structured flat/no-open-order evidence instead of generic failures. No strategy threshold, order submission, close timing, or residual repair behavior changed. Final deploy window has no opens; no-entry is currently `entry_waiting_for_finalization_window_too_early`, not an error. |
| [CL-021-staggered-first-stage-capture-runtime-drift](daily/2026-05-31.md#cluster-cl-021-staggered-first-stage-capture-runtime-drift) | fixed, deployed, cloud verified | critical | `live-runtime`, `entry-dispatch`, `normal-exit`, `funding-capture-state`, `v1-parity` | `v2.staggered-first-stage-funding-captured-not-advanced + exit-after-first-stage-config-not-persisted + recovered-prl-open-held-past-first-funding` | 2026-05-31 | production `7704238`: `PRLUSDT` remained open after first funding despite `staggered_exit_mode=after_first_stage`; position had `funding_captured=false` and `exit_after_first_stage=false` | `c66f470` | 2026-05-31 local: focused RED/GREEN `3 passed`; adjacent entry/exit/runtime/persistence suite `132 passed`; runtime/full-closure/replay suite `109 passed`; passive close suite `105 passed`; recovery suite `50 passed`; compileall and diff-check passed; GitNexus detect-changes low risk with `0` affected processes. Cloud: fast-forwarded `7704238..c66f470`, manifest/compileall/focused regression passed, services restarted active with `NRestarts=0`, health ok, and real `PRLUSDT` routed `first_stage_capture` at `2026-05-31 03:30:00 CST` then cleared flat by `2026-05-31 03:30:27 CST`; final state at `2026-05-31 03:31:17 CST` was `open=0`, `pending_entry=0`, `pending_close=0`, `passive=0`, `residual=0`. Read-only exchange truth after docs sync: `PRLUSDT` Binance/Aster high-confidence no position/no open orders, and account-level `fetch_all_positions()` on Binance/Bybit/Aster/OKX/Bitget/Gate/Hyperliquid all returned `nonzero_count=0`. Full pytest: `3247 passed, 9 skipped, 32 failed`; failures are unrelated known Hyperliquid `Crypto` dependency and shutdown monkeypatch-signature issues. | `V1 runtime funding-capture state update before standard close decision + config-derived first-stage staggered exit persisted/backfilled` | Runtime now advances funding stage state with V1 `update_position_funding_capture_state()` before `standard_close_reason()`, records stage-change evidence, persists `exit_after_first_stage=true` for new staggered entries when config says `after_first_stage`, and backfills recovered stale open positions before close evaluation. Cloud evidence shows the recovered PRL position backfilled, captured first-stage funding, routed `first_stage_capture`, escalated from missing L2 to aggressive close, terminally verified flat with no pending close/residual work, and read-only exchange truth confirmed no residual live positions. |
| [CL-020-funding-capture-before-settlement-timestamp-drift](daily/2026-05-31.md#cluster-cl-020-funding-capture-before-settlement-timestamp-drift) | fixed, deployed, cloud verified | critical | `live-runtime`, `entry-sync`, `exit-decision`, `state-persistence`, `v1-parity` | `v2.entry-open-position.funding-timestamp-zero + funding-capture-before-settlement + candidate-opportunity-type-dropped` | 2026-05-31 | production `d3e8d12`: `MAGMAUSDT` opened `2026-05-31 01:58:28 CST` and closed `2026-05-31 01:58:40 CST` as `funding_capture` before funding settlement | `053cc8a` | 2026-05-31 local: focused RED/GREEN `9 passed`; related entry/exit/runtime/persistence suite `129 passed`; adjacent recovery/persistence suite `50 passed`; persistence/replay suite `94 passed`; compileall and diff-check passed. Cloud: fast-forwarded `d3e8d12..053cc8a`, manifest/compileall/focused regression passed, services restarted active with `NRestarts=0`, health ok, and real post-deploy `PRLUSDT` open preserved first/second funding timestamps with no immediate `funding_capture`. Latest CL-020 watch at `2026-05-31 03:05:24 CST`: `/opt/lightfee-v2` and `.deploy_version` were `8cf2b9f`, services still active, `verify_production_services.py` passed, no journal warnings, full state had one `PRLUSDT` open with `funding_captured=false`, `second_stage_funding_captured=false`, and zero pending entry/close/residual work. Full pytest still has unrelated Hyperliquid `Crypto` dependency and shutdown monkeypatch failures. | `V1 funding lifecycle semantics preserved from candidate to pending/open state + missing funding timestamp fail-closed` | Runtime entry dispatch, pending entry persistence/finalization, open-position construction, state snapshot restore, and close timing now preserve candidate funding timestamps and opportunity type. `funding_timestamp_ms=0` no longer triggers aligned funding capture, force close, or funding-captured state updates. At CL-020 acceptance, `PRLUSDT` was legitimately held for staggered funding timing instead of closing early; the later first-stage close-path drift for that same position is closed as CL-021. |
| [CL-019-post-94e6257-evidence-gap-closure](daily/2026-05-30.md#cluster-cl-019-post-94e6257-evidence-gap-closure) | fixed, deployed, harness/probe verified | high | `venue-transport`, `exchange-error-evidence`, `live-runtime`, `passive-close`, `probe-diagnostics`, `v1-parity` | `v2.post-94e6257.ack-only-fill-gap + unsupported-probe-catalog-evidence-gap + okx-dust-rule-source-gap` | 2026-05-30 | post-`94e6257` deployment watch | `e6b7645` | 2026-05-30 local: RED/GREEN focused evidence harness failed before and passed after (`6 passed`); passive/diagnose/startup/recovery harness group passed `244 passed`; relevant transport groups passed `49 passed`. Cloud: fast-forwarded to `e6b7645`, deploy manifest and compileall passed, focused cloud harness passed `6 passed`, sidecar/live restarted cleanly `NRestarts=0`, production health passed with zero open/pending/residual and no exchange-truth mismatches, singleton passed, and targeted credentialed probes for `POWER`, `HOME`, `GENIUS`, `SPACE`, `RAVE`, `FORM`, `LAB`, `BSB`, `BAS`, `SNDK`, `HMSTR`, `ORCA`, `NOM` were flat/no-open-orders. Full `tests/test_venues_transport.py` still has unrelated Hyperliquid optional dependency failures (`Crypto`, `eth_account`). | `V1 exchange-truth priority preserved + exchange-rule/probe evidence completion without trading behavior changes` | Accepted-but-unfilled order acks now preserve raw ack, accepted IDs, missing fill fields, and partial evidence confidence. Unsupported/timeout probes carry catalog/cooldown diagnostics. OKX dust terminal events preserve cached official instrument rule source and quantity-contract metadata. Current run has no opens/pending/passive-dust/residual-pause; remaining post-deploy events are old-family Local-L2/snapshot/probe noise. |
| [CL-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission](daily/2026-05-30.md#cluster-cl-018-post-bbcd7b9-production-watch-residual-live-truth-and-exchange-admission) | fixed, deployed, harness/probe verified | high | `live-runtime`, `residual-repair`, `passive-close`, `pending-entry`, `entry-admission`, `local-l2`, `diagnose-live`, `exchange-docs` | `v2.post-bbcd7b9.hmstr-residual-open-orders-pause + binance-2027-leverage-admission-gap + nomusdt-drift-failed-then-cleared + orca-live-recovered-mismatch + rave-reduceonly-flat + l2/probe-noise` | 2026-05-30 | post-`bbcd7b9` deployment watch | `0fd9a74` | 2026-05-30 local: RED/GREEN HMSTR pause evidence harness passed; Binance `-2027` admission/transport harness passed `9 passed`; focused residual/passive/admission/Local-L2 harness passed `39 passed`; live-harness profile passed compileall, diff-check, and `66 passed`. Cloud: fast-forwarded to `0fd9a74`, `.deploy_version=0fd9a74`, manifest and compileall passed, sidecar/live restarted cleanly `NRestarts=0`, production health ok with zero open/pending/residual, and targeted credentialed probes for old and new event symbols (`HMSTR`, `ORCA`, `NOM`, `RAVE`, `LITE`, `AVGO`, `FHE`, `HOME`, `POWER`, `HEI`, `GENIUS`) were flat/no-open-orders on real venues. | `V1 residual repair live-truth/open-order semantics + Binance USD-M official -2027 admission classification + exchange-documented rate-limit/local-L2 rules` | HMSTR open-order-present pause diagnosability is root-fixed without changing trading semantics; Binance `-2027` is root-fixed as deterministic admission block and reproduced/closed on cloud via `HEIUSDT`; post-fix HOME/POWER real opens auto-closed; current production is flat and unblocked. |
| [CL-017-post-deploy-pending-hedge-admission-and-okx-l2-evidence](daily/2026-05-29.md#cluster-cl-017-post-deploy-pending-hedge-admission-and-okx-l2-evidence) | fixed, deployed, cloud harness/probe verified | high | `live-runtime`, `pending-entry`, `entry-admission`, `venue-aster`, `venue-bybit`, `local-l2`, `diagnose-live`, `live-harness`, `exchange-docs` | `v2.pending-entry.hedge-admission-reject-retries-until-lifetime + aster-max-notional-no-v1-venue-cooldown + okx-local-l2-official-evidence-gap` | 2026-05-29 | post-deploy watch after `1120868` | `6987fc8`; final deployed HEAD `145686b` after deploy-manifest sync | 2026-05-29 local: admission/Local-L2 harness `84 passed`; focused related runtime/transport suite `35 passed, 461 deselected`; wider related run had unrelated Hyperliquid missing dependency failures after 601 passes. Cloud: manifest check passed, focused harness `84 passed` + `26 passed`, BZUSDT/LABUSDT credentialed probes high-confidence flat/no-open-orders, services active/running `NRestarts=0`, no post-deploy reject counts, and Local-L2 insufficient classifications `[]`. | `V1 Aster max-notional venue cooldown + exchange-documented Bybit trading-terms admission + OKX seqId/prevSeqId evidence classification` | Pending hedge deterministic admission rejects now block and abort through the existing cleanup path instead of retrying until lifetime. Aster `-5018` also creates V1-style venue cooldown. OKX official `prevSeqId/seqId` and checksum evidence is classified as exchange-doc behavior. Cloud harness/probe closed the loop. |
| [CL-016-post-deploy-official-local-l2-and-terminal-order-reject-watch](daily/2026-05-29.md#cluster-cl-016-post-deploy-official-local-l2-and-terminal-order-reject-watch) | fixed, deployed, production-probe verified | medium | `local-l2-data-plane`, `diagnose-live`, `production-blocker-analysis`, `passive-close`, `entry-order-submit`, `exchange-docs`, `live-harness`, `probe` | `v2.post-9cdb9df.local-l2-official-continuity-rebuild + terminal-order-reject-handled-flat + post-only-maker-reject-no-open-loop` | 2026-05-29 | post-deploy watch after `9cdb9df`/`89cf90a` | `7ee4c72`; final deployed HEAD `4afa8e6` after docs/manifest sync | 2026-05-29: RED proved Aster official mismatch was still an evidence gap in the harness and field-complete non-breaks were over-classified; GREEN focused RED tests `2 passed`; focused related suite `59 passed`; cloud fast-forwarded to `7ee4c72`, compile/focused nodes `3 passed`, deploy marker and manifest verified, JCT/PARTI credentialed probes gate-passed high-confidence flat/no-open-orders, Local-L2 since commit classified 58/58 rebuilds as `previous_link_mismatch` with zero Local-L2 evidence gaps, real public Binance/Aster `LABUSDT` probes passed, final cloud HEAD `4afa8e6` with clean worktree and sidecar/live active `NRestarts=0`. | `Shared strict Binance/Aster Local-L2 continuity classifier + V1 terminal-flat verification + no trading-path semantic relaxation` | Diagnostic classification now uses one shared helper for `diagnose_live.py`, `analyze_production_blockers.py`, and the Local-L2 harness. It classifies only proven previous-link mismatch, real skipped update, or unbridged snapshot boundary, so official Aster resets close while non-broken field-complete events remain insufficient evidence. Trading code was not changed, and production remains flat/running. |
| [CL-015-jctusdt-partiusdt-v1-terminality-regression](daily/2026-05-29.md#cluster-cl-015-jctusdt-partiusdt-v1-terminality-regression) | fixed, deployed, production-probe verified | critical | `passive-close`, `residual-repair`, `live-runtime`, `v1-parity`, `exchange-docs`, `live-harness` | `v2.jctusdt.live-one-sided-price-unavailable-dust-loop + v2.partiusdt.exhausted-residual-repair-live-nonzero-pause-loop` | 2026-05-29 | production deploy `7d4a8b2` | `9cdb9df` | 2026-05-29: RED/GREEN JCT/PARTI harness added; missed terminal-maker JCT branch reproduced after first deploy; final harness `4 passed`; focused incident harness `16 passed`; live-harness profile `60 passed`; close profile `280 passed`; probe guard/static read-only checks passed; cloud fast-forwarded to `9cdb9df`, services active, final credentialed probes show JCT/PARTI flat/no-open-orders with high-confidence consistent state. | `V1 terminal under-min compensation + terminal-maker price-unavailable branch closure + V2 live-truth residual side rebuild/backoff + explicit read-only probe gate` | V2 no longer retries confirmed live one-sided or terminal-maker `price_unavailable_for_min_notional` forever; both route through V1 live-truth compensation. Exhausted/paused residual repairs now resume tradeable reduce-only IOC repair when live truth is trusted, rebuild stale no-local repair side from signed live position, preserve submit-failure backoff, and keep untrusted truth fail-closed with structured evidence. Production `JCTUSDT` and `PARTIUSDT` are flat locally and on exchange truth. |
| [CL-014-opgusdt-passive-close-stuck-under-chunk-live-flatness-and-precision](daily/2026-05-28.md#cluster-cl-014-opgusdt-passive-close-stuck-under-chunk-live-flatness-and-precision) | fixed, verified | critical | `passive-close`, `live-runtime`, `venue-transport`, `v1-parity` | `exit.passive_close_maker_filled_under_chunk + exit.passive_close_recovery_probe_diagnostic + pending_entry.hard_ceiling_reconcile_before_abort + normalize_quantity_aster_highusdt` | 2026-05-28 | local audit / incident report | working tree / local commit | 2026-05-28: OPGUSDT harness verified full closed-loop flat clearing; hard ceiling reconcile extension tests passed; Aster/Binance dynamic rules NameError resolved and HIGHUSDT precision verified. | `Conservative open-order truth checks + DUAL_TAKER fallback + NameError-free dynamic precision + reconcile extension tests` | V2 no longer treats unsupported open orders check as flat, correctly escalates maker under-filled terminal order to DUAL_TAKER to trigger aggressive close fallback and flat clearing, resolves NameError on Binance/Aster live pathways, and validates reconcile extension and precision behaviors. |
| [CL-013-pending-entry-v1-terminality-drift-live-single-sided](daily/2026-05-27.md#cluster-cl-013-pending-entry-v1-terminality-drift-live-single-sided) | fixed, deployed, auto-cleaned, production-health verified | critical | `live-runtime`, `pending-entry`, `order-reconciliation`, `diagnose-live`, `live-harness`, `v1-parity` | `v2.pending-entry.stale-accepted-flat-cleared + planned-hedge-cid-pre-submit-query + exchange-truth-nonzero-local-flat + pending-finalize-live-position-hydration-gap` | 2026-05-27 | production after deploy: local open/pending zero while exchange truth showed Binance `MUBARAKUSDT`/`EDENUSDT`, Bybit `INUSDT`, and Aster `BEATUSDT` live single-sided positions; post-fix restart exposed PRLUSDT balanced live-position evidence falling through to live recovery | working tree / remote hot patch | 2026-05-27: remote RED reproduced stale-accepted/planned-CID failures and PRL pending-finalize failure; remote GREEN passed targeted drift tests, adjacent reconciliation/runtime tests (`20 passed, 172 deselected`), full live-harness (`48 passed`), explicit false-green probe (`1 passed`), automatic PRL pending cleanup, event-log archive after OOM, final production health, and flat account read-only check. | `Exact V1 pending-entry terminality semantics: accepted maker evidence stays uncertain until real terminal no-fill/fill evidence; planned hedge CID is not queried before submit; balanced live-position evidence hydrates pending fills and finalizes by quantity+price, not order-id presence` | V2 no longer clears pending entries on stale accepted maker + momentary flat snapshots, no longer uses planned hedge CID as exchange lookup evidence, and no longer defers a balanced pending entry solely because order ids are missing after live-position evidence supplies fill prices. Legacy PRL pending was auto-cleaned, old 1.7G event log was archived, and `lightfee-live` is active/running with zero local open/pending/residual state and flat exchange truth. |
| [CL-007-biousdt-exchange-truth-false-green-root-fix](daily/2026-05-26.md#cluster-cl-007-biousdt-exchange-truth-false-green-root-fix) | fixed; deployed and production-health verified | critical | `live-runtime`, `startup-recovery`, `current-state`, `production-health`, `diagnose-live`, `venue-bybit`, `venue-aster`, `venue-okx`, `v1-parity`, `exchange-docs` | `v2.bybit-biousdt-live-recovery-cleanup-duplicate-cid-full-misclassified + local-state-false-green-exchange-nonzero` | 2026-05-26 | production read-only truth: Bybit `BIOUSDT` long `1444.0` while local state open/pending zero; duplicate cleanup returned Bybit `110072` and old filled order evidence while live qty remained nonzero | `30a8ddc` | 2026-05-27: full local validation passed; cloud deployed the code closure and later synced this documentation closure; production health passed; explicit credentialed read-only diagnose probes for LYN/OPG/BEAT/BIO were high-confidence flat/no-open-orders; current-run log scan found no new open/reject/residual/error fingerprints. | `V1 exchange-truth priority + duplicate-CID live-position verification + exchange-documented admission blocks + residual terminality acceptance gate` | Closed. BIOUSDT/BEATUSDT local false-green, LYN/OPG stale residual terminality, and diagnose open-order evidence gaps are covered by independent harness/probe paths. No manual production state edit was performed. |
| [CL-005-live-shutdown-systemd-stop-timeout-root-fix](daily/2026-05-26.md#cluster-cl-005-live-shutdown-systemd-stop-timeout-root-fix) | fixed locally; main push pending production restart verification | high | `live-runtime`, `systemd`, `shutdown`, `background-tasks`, `state-persistence`, `network-close` | `v2.lightfee-live.systemd-stop-timeout.status-9-kill + sigterm-no-explicit-shutdown-path + background-task-unbounded-join` | 2026-05-26 | deployment restart evidence: `Main process exited status=9/KILL`, `Failed with result timeout` | this workstream | 2026-05-26 local: focused shutdown suite `11 passed`, full `tests/test_live_startup_preflight.py` `52 passed`, compileall passed, no hardcoded second wait remains. | `Explicit SIGTERM shutdown ownership + bounded background-task cancel/join + staged shutdown logging` | `lightfee-live` now handles SIGTERM via an explicit shutdown event/path, cancels runtime background tasks with configured timeout, bounds WS/listen-key/sidecar/adapter shutdown, flushes state, names stuck tasks, and does not rely on increasing `TimeoutStopSec`. Production restart acceptance remains the next watch item after deploy. |
| [CL-006-snapshot-fallback-degraded-domain-entry-impact](daily/2026-05-26.md#cluster-cl-006-snapshot-fallback-degraded-domain-entry-impact) | fixed in main push; production no-entry watch pending | critical | `live-runtime`, `sidecar`, `snapshot-freshness`, `last-good-fallback`, `entry-gate`, `observability`, `v1-parity` | `v2.runtime.snapshot_fallback_last_good + snapshot_degraded.market_observed_stale.global-no-tradeable-candidates` | 2026-05-26 | production evidence: `runtime.snapshot_fallback_last_good≈255`, `runtime.snapshot_degraded≈37`, degraded domain included `market_observed_stale`, and strict entry gate produced long no-entry streaks | this main push | 2026-05-26 local: original global `market_observed_stale` regression failed before fix, then focused snapshot/sidecar suite `90 passed`; py_compile passed. | `V1 domain-scoped degraded freshness + quote provenance round-trip + non-blocking last-good fallback diagnostics` | V2 no longer converts global `market_observed_stale` or last-good fallback into a blanket no-entry gate. Snapshot freshness status is scoped by domain/venue/symbol/source; quote `observed_at_ms/source` is produced by the real sidecar, persisted, and loaded by runtime; last-good fallback carries duration/age diagnostics; no-entry diagnostics distinguish domain stale, edge insufficient, and window mismatch. Production watch should confirm `market_observed_stale` does not produce global `no_tradeable_candidates` when candidate quote/local-L2 evidence is fresh. |
| [CL-002-recovery-live-position-probe-evidence-quality](daily/2026-05-26.md#cluster-cl-002-recovery-live-position-probe-evidence-quality) | fixed; deployed and Task-7 accepted | high | `startup-recovery`, `live-runtime`, `venue-catalog`, `venue-okx`, `observability`, `exchange-docs` | `v2.recovery.live_position_probe.unsupported-symbol-noise + okx-contract-metadata-missing-ctval-empty-error` | 2026-05-26 | production logs showed high-volume `recovery.live_position_probe_error`, many `error=""`, unsupported-symbol noise, and OKX `okx_contract_metadata_missing_ct_val` without classification | `30a8ddc` | 2026-05-27: full local validation passed; OKX fixture drift was corrected to include official `ctType`; explicit `diagnose_live --venues` was added and verified; cloud health passed after deploy. | `V1 scan-symbol-cache recovery probe parity + OKX official instruments metadata ctVal/ctType classification` | Closed for Task-7. Recovery probes use venue catalogs, OKX contract sizing requires official `ctVal`/`ctType`, and the read-only diagnose tool can force explicit venue truth when local state has no open position to infer venues. |
| [CL-003-no-entry-stale-liquidity-domain-root-fix](daily/2026-05-26.md#cluster-cl-003-no-entry-stale-liquidity-domain-root-fix) | fixed locally; main push pending production observation | critical | `live-runtime`, `sidecar`, `snapshot-freshness`, `candidate-filtering`, `local-l2`, `observability`, `v1-parity` | `v2.lightfeev2.no-entry.opened-zero + stale_liquidity_global_skip_entry + sidecar_perp_liquidity_budget_3000_vs_20s_publish` | 2026-05-26 | deployment evidence: `entry.opened=0`, `runtime.position_opened=0`, `scan.no_entry_diagnostics.reason=no_tradeable_candidates`, `stale_liquidity age_ms~=19971 budget_ms=3000 decision=skip_entry` | this workstream | 2026-05-26 local: sidecar lifecycle `25 passed`; runtime snapshot freshness `11 passed`; sidecar snapshot/schema `18 passed`; RED confirmed mixed-venue publish interval and other-symbol degradation failures before fix. | `V1 domain-scoped snapshot freshness + per-venue sidecar liquidity publish cadence + execution-required liquidity blocking only` | V2 no longer expands stale coarse sidecar perp liquidity into a global candidate skip. Runtime now separates quote stale, execution L2 stale, advisory perp-liquidity stale, and blocking perp-liquidity stale; sidecar publishes liquidity observed/published timing per venue/source/domain; a degraded row for another symbol does not block the current candidate. Production watch should confirm advisory liquidity staleness no longer produces `no_tradeable_candidates` by itself. |
| [CL-004-local-l2-hot-stale-freshness-root-fix](daily/2026-05-26.md#cluster-cl-004-local-l2-hot-stale-freshness-root-fix) | fixed in main workstream; production verification required | high | `local-l2-data-plane`, `local-l2-ws`, `v1-parity`, `production-observability` | `v2.local-l2.hot-stale-rebuild.ws-freshness-evidence-missing + rest-proactive-refresh-late + reason-spam` | 2026-05-26 | post-deploy evidence: about 1200 `runtime.local_l2_hot_stale_rebuild`, concentrated in Binance/Bybit/Aster, with `runtime.local_l2_snapshot_error=0` | this workstream | 2026-05-26 local: two RED/GREEN edge tests pass; local-L2 runtime/WS/V1 parity suite `150 passed`; py_compile and diff-check passed | `V1 local-L2 HOT freshness evidence + REST pre-stale refresh + bounded state-event observability` | HOT freshness now records valid WS delta, heartbeat, book confirmation, and subscription confirmation even when price levels do not change. REST buffered replay HOT books refresh before `stale_after_ms`. Rebuild logs include specific reasons (`no_ws_delta`, `no_keepalive`, `rest_refresh_late`, `subscription_missing`, `clock_skew`, `unknown`) and per-symbol/reason rate limiting with `suppressed_count`. Production log verification remains required after deploy. |
| [CL-001-post-deploy-risk-only-regressions-root-fix-plan](daily/2026-05-26.md#cluster-cl-001-post-deploy-risk-only-regressions-root-fix-plan) | partially fixed locally; Bybit duplicate cleanup closed; production verification pending | critical | `passive-close`, `recovery-cleanup`, `venue-bybit`, `venue-aster`, `local-l2-data-plane`, `snapshot-freshness`, `order-reconciliation`, `v1-parity`, `exchange-docs` | `v2.post-6845d9b.beatusdt-passive-close-dust-fallback-loop + recovery-cleanup-bybit-duplicate-cid + aster-listenkey-invalid-rotation + local-l2-sequence-rebuild-evidence + snapshot-last-good-stale` | 2026-05-26 | production deployed `30d89e8` audit after services active since `2026-05-25 21:25:45 CST` | this Bybit duplicate workstream commit | 2026-05-26 local: Bybit duplicate state-machine regressions pass in `tests/test_live_entry_hedge_root_fix.py`, `tests/test_passive_close.py`, and `tests/test_close_execution.py`; combined suite `271 passed`; `compileall` and `git diff --check` passed. | `V1 passive close abort-and-compensate parity + duplicate CID reconciliation reuse + listenKey terminal rotation + exchange-documented L2 continuity` | BEATUSDT passive-close semantic drift and UBUSDT Bybit duplicate cleanup now have local root fixes. Bybit `110072 OrderLinkedID is duplicate` is treated as idempotent evidence across runtime cleanup, passive close, and close executor paths: realtime/history/execution reconciliation classifies full/partial/none/unknown, emits unified `order.reconcile_result`, retries partial residuals with a fresh reduce-only cid, clears full/live-flat, and backs off no-evidence branches. Aster/listenKey and production acceptance remain separate watch items. |
| [CL-002-xcnusdt-recovered-passive-close-evidence-and-diagnostics](daily/2026-05-25.md#cluster-cl-002-xcnusdt-recovered-passive-close-evidence-and-diagnostics) | fixed; deployed and cloud read-only verified | high | `passive-close`, `recovery`, `diagnose-live`, `venue-aster`, `venue-bybit`, `production-observability` | `v2.recovered-passive-close.xcnusdt-live-flat-eventual-cleanup + diagnose-live-missing-aster-truth + nonflat-probe-no-structured-evidence` | 2026-05-25 | post-UBUSDT production acceptance still showed `live-recovered:XCNUSDT:bybit->aster` local open/pending passive close | `6845d9b` | 2026-05-25 local RED/GREEN: XCNUSDT both-flat cleanup, one-side nonzero diagnostic, partial live-fetch failure diagnostic, diagnose symbol-position-id filter, and Aster/Bybit exchange-truth routing tests pass; targeted close/venue/diagnose suite `596 passed`; compileall and diff-check clean; GitNexus detect_changes MEDIUM, no HIGH/CRITICAL. Cloud deployed by git pull/reset to `6845d9b`; remote compileall passed; live/sidecar restarted active; post-restart state has open/pending passive counts `0`, `last_error=null`, and no UBUSDT/XCNUSDT/min-quantity/error log hits in the restart window. | `V1 pending passive live-flat cleanup evidence + read-only multi-venue diagnose parity` | XCNUSDT was not UBUSDT same-root min-quantity drift. It was recovered as 5070/5070, repeatedly attempted passive/fallback while live exchange truth was already flat, then the deployed V1 live-flat sweep cleared it via `pending_passive_close_flat_probe`. The deployed `6845d9b` code now adds structured probe diagnostics for future one-sided/nonfetchable cases and extends `diagnose_live.py` to derive Bybit/Aster venue truth from local positions instead of only checking Binance/Bybit. Remote `.venv` lacks pytest, so cloud verification is compileall plus read-only runtime acceptance, not remote pytest. |
| [CL-001-passive-close-bybit-hedge-min-quantity-terminal-maker-v1-drift](daily/2026-05-25.md#cluster-cl-001-passive-close-bybit-hedge-min-quantity-terminal-maker-v1-drift) | fixed; deployed and cloud verified | critical | `passive-close`, `venue-bybit`, `venue-transport`, `symbol-rules`, `recovery`, `state-persistence`, `v1-parity` | `v2.passive-close.bybit-terminal-maker-filled-hedge-min-quantity-bypass + bybit-dynamic-symbol-rules-missing + pending-passive-close-live-flat-sweep-missing` | 2026-05-25 | production read-only current run on deployed `73d5428` showed UBUSDT pending passive close repeatedly submitting Bybit hedge `quantity=1.0` and receiving `retCode=10001 The number of contracts exceeds minimum limit allowed` | `local_patch_73d5428-reviewgap-20260525` | 2026-05-25 local RED/GREEN: 5 targeted UBUSDT drift tests plus REVIEW GAP closure tests pass in worktree; full targeted close/transport suite `270 passed`; compileall and diff-check clean; GitNexus detect_changes MEDIUM, no HIGH/CRITICAL. Production post-deploy read-only acceptance passed: UBUSDT pending/open was cleared, `recovery.flat` and `runtime.position_drift_corrected` emitted, Bybit/OKX UBUSDT positions were `0.0`, Bybit/OKX open orders were `0`, and no post-restart Bybit min-contract hedge loop was observed. | `V1 passive close hedge normalized min-quantity guard + dynamic Bybit symbol rules + pending passive live-flat sweep parity` | V2 fixes the new UBUSDT branch and closes the explicit REVIEW GAP: terminal maker `FILLED` and generic delta hedge both normalize before min-quantity/min-notional decisions, real Bybit transport uses dynamic SymbolRulesCache before HTTP submit, pending passive close processing probes live-flat state before retrying, exact V1 recovery payload fields are logged, matching `last_error` is cleared, and current-state/raw persistent views no longer retain flat pending/open state. |
| [CL-001-passive-close-bybit-duplicate-hedge-reconciliation-v1-drift](daily/2026-05-24.md#cluster-cl-001-passive-close-bybit-duplicate-hedge-reconciliation-v1-drift) | fixed; deployed and cloud verified | critical | `passive-close`, `venue-bybit`, `order-reconciliation`, `state-persistence`, `v1-parity`, `live-runtime` | `v2.passive-close.hedge.bybit-110072-duplicate-orderLinkId-no-cid-reconciliation + missing-persisted-leg-open-position-stale` | 2026-05-24 | cloud run `lightfee-1779546833786-1438044` repeatedly emitted Bybit `110072 OrderLinkedID is duplicate` for LYNUSDT passive close hedge while exchange truth was flat | `e1997a1` | 2026-05-24 local: targeted RED/GREEN, `tests/test_passive_close.py` `78 passed`, `tests/test_close_execution.py` `45 passed`, compileall and diff-check clean; GitNexus impact/detect_changes LOW. Cloud run `lightfee-1779557082898-1444166`: `exit.passive_close_hedge_duplicate_client_order_reconciled=1`, `exit.passive_close_hedge_error=0`, `open_positions=0`, `pending_passive_closes=0`, Binance/Bybit LYNUSDT positions/open orders zero. | `V1 passive close hedge submit-error client-id reconciliation + recovered cumulative-fill finalization parity` | V2 passive close hedge now matches V1: Bybit duplicate `orderLinkId` is reconciled by client order id before being treated as failure. Existing recovered pending passive closes with cumulative fills but missing persisted legs can finalize or clear by live-flat probe without leaving stale local open state. After CL-002, latest-audit strict unresolved root-fix count is 0; ALTUSDT remains acceptance-watch only because the scenario did not recur. |
| [CL-002-local-l2-dropped-hot-rebuild-v1-parity-drift](daily/2026-05-24.md#cluster-cl-002-local-l2-dropped-hot-rebuild-v1-parity-drift) | fixed; deployed and short-window cloud verified | high | `local-l2-runtime`, `local-l2-data-plane`, `entry-local-l2`, `live-runtime`, `v1-parity`, `production-observability` | `v2.local-l2.dropped-books-rebuilt-by-scan-promoted-hot-stale + dynamic-activation-missing-pool-assignment + sequence-gap-evidence-after-transition` | 2026-05-24 | cloud run after `e1997a1` showed dropped-pool local-L2 books retained in runtime state and eligible for stale HOT rebuilds; sequence-gap rebuild evidence logged post-transition status | `6d6ef17` | 2026-05-24 local: targeted RED/GREEN tests for dropped stale HOT skip/prune, dynamic HOT/WARM assignment, and sequence-gap evidence; local-L2/entry/startup suite `261 passed`; WS/V1 data-plane/maker-event suite `84 passed`; full `pytest` `2990 passed, 2 skipped, 1 warning`; GitNexus impact/detect_changes LOW. Cloud deploy: `.deploy_version=6d6ef17`, compileall passed, file checksums matched, run `lightfee-1779559680893-1446127` is `running/running` with open/pending zero and initial local-L2 watch counts zero. | `V1 local-L2 tracked assignment + prune dropped books + evidence logging parity` | V2 now assigns tracked primary books to `HOT_EXEC` and shadow books to `WARM`, prunes untracked `DROPPED` and stale/over-budget `RETAINED` books, skips dropped books in snapshot sync, and logs sequence-gap evidence from the pre-transition state. Short-window cloud verification is clean; keep normal longer production watch. |
| [CL-003-okx-contract-units-and-residual-repair-v1-drift](daily/2026-05-24.md#cluster-cl-003-okx-contract-units-and-residual-repair-v1-drift) | fixed; Task-7 OKX gate and cloud watch accepted | critical | `venue-okx`, `venue-transport`, `private-ws`, `live-runtime`, `residual-repair`, `entry-sizing`, `v1-parity`, `exchange-docs` | `v2.okx-net-short-sign-flipped + okx-sz-zero-contract-submit + okx-private-ws-ctval-one-fallback + okx-cancel-delete-405 + residual-repair-startup-only + okx-entry-unaligned-contract-base-step` | 2026-05-24 | audit found OKX net short sign drift, OKX zero-contract submit risk, private WS `ctVal=1.0` fallback risk, OKX cancel wrong endpoint, runtime residual repairs not continuously driven, and OKX entry quantities not aligned to contract base step | `30a8ddc` | 2026-05-27: full gate passed, including `tests/test_venues_contract.py` `133 passed`; OKX fixture metadata includes official `ctType`; explicit OPG OKX read-only probe returned high-confidence flat/no-open-orders after deploy. | `V1 OKX contract-unit semantics + V1 residual repair runtime state machine parity` | V2 preserves OKX net position sign, converts base quantity to derivative contracts using official metadata evidence, rejects invalid contract quantities locally, refuses untrusted OKX SWAP private WS ctVal fallback, cancels via official OKX POST endpoint, drives residual repairs in normal housekeeping, and aligns OKX SWAP entry quantity to `ctVal * lotSz`. Task-7 cloud watch is closed. |
| [CL-004-hyperliquid-ioc-finalize-rounding-v1-drift](daily/2026-05-24.md#cluster-cl-004-hyperliquid-ioc-finalize-rounding-v1-drift) | fixed locally; acceptance blocked by fixture contract tests | high | `venue-hyperliquid`, `venue-transport`, `exchange-signing`, `entry-finalize`, `residual-repair`, `api-wallet`, `tests`, `v1-parity`, `exchange-docs` | `v2.hyperliquid-ioc-price-none-local-reject + finalize-pending-entry-stale-residual-after-reconciliation + hyperliquid-rounding-bankers-drift + api-wallet-preflight-boundary + hl-contract-fixture-missing-l2book` | 2026-05-24 | review found Hyperliquid price-less IOC cleanup local rejects, stale residual computation after fill reconciliation, rounding drift against Rust V1 f64 vectors, and ambiguous API-wallet acceptance boundary | working tree | 2026-05-24 local: rounding vectors match Rust V1 helper; targeted `tests/test_venues_transport.py tests/test_live_entry_hedge_root_fix.py tests/test_runtime_entry_flow.py tests/test_passive_close.py` = `552 passed`; `git diff --check` clean; GitNexus detect_changes low. Broad venue/runtime/recovery/residual/passive-close/close suite = `1113 passed, 2 skipped, 3 failed`, all three failures in old Hyperliquid contract fixture tests lacking price or `l2Book` fallback mock. | `V1 Hyperliquid IOC quote fallback + post-reconciliation residual computation + f64 rounding parity` | Core code semantics are fixed locally: price-less Hyperliquid IOC uses L2 fallback or rejects before `/exchange`, finalize residuals use post-reconciliation fill quantities, Hyperliquid wire rounding matches Rust V1 boundary vectors, and unverified API wallet mode fails closed. Acceptance remains open until `tests/test_venues_contract.py` Hyperliquid fixture tests are updated to supply either price evidence or a two-response `l2Book -> exchange` mock. |
| [CL-001-passive-close-bybit-tick-v1-drift](daily/2026-05-23.md#cluster-cl-001-passive-close-bybit-tick-v1-drift) | deployed; acceptance pending | critical | `passive-close`, `venue-bybit`, `symbol-rules`, `local-l2`, `live-runtime`, `v1-parity` | `v2.passive-close.bybit-altusdt-static-price-tick-zero-aligned + v1-passive-order-tick-metadata-quote-fallback-drift` | 2026-05-23 | cloud latest deployed run showed `ALTUSDT` passive close stuck with Bybit `tick_size=0.01`, `price_hint~=0.0078`, `aligned_price=0.0` | working tree deployed to `/opt/lightfee-v2` | 2026-05-23 local: dynamic Bybit instruments-info tick RED/GREEN, metadata-missing local-L2 quote fallback RED/GREEN, non-dynamic/spec-fallback venue RED/GREEN, and no-metadata/no-quote zero-tick RED/GREEN pass; expanded passive/venue/runtime suite `640 passed`; compileall and diff-check clean; GitNexus impact/detect_changes LOW, 0 affected processes | `V1 passive_order_tick_size metadata + quote fallback parity` | Passive close no longer aligns sub-cent maker close prices with static `0.01`. It resolves true dynamic symbol-rule tick first, ignores `rule_source=spec_fallback` as metadata, then infers from hot local-L2 bid/ask precision. If neither exists, passive tick is `0.0` and the close remains fail-closed, matching V1. Deployed with the 2026-05-23 CL-002 rollout; production acceptance remains pending because the current run has no ALTUSDT passive close to exercise this path. |
| [CL-002-passive-close-terminal-flat-probe-snapshot-fallback-drift](daily/2026-05-23.md#cluster-cl-002-passive-close-terminal-flat-probe-snapshot-fallback-drift) | fixed; deployed and cloud verified | critical | `passive-close`, `close-executor`, `recovery`, `state-persistence`, `v1-parity`, `bybit`, `aster` | `v2.pending-passive-close.position-snapshot-missing-flat-probe-false + terminal-reduce-only-loop-bybit-110017-aster-2022` | 2026-05-23 | cloud latest deployed run recovered stale `GMTUSDT`/`LYNUSDT` pending passive closes while exchange truth was flat, then repeated Bybit `110017` and Aster `-2022` reduce-only 400s | working tree deployed to `/opt/lightfee-v2` | 2026-05-23 local: recovered pending snapshot omitted RED/GREEN tests pass; related suite `160 passed`; full `python3 -m pytest` `2985 passed, 2 skipped, 1 warning`; compileall and diff-check clean; GitNexus detect_changes LOW, 0 affected processes. Cloud run `lightfee-1779546833786-1438044`: terminal-flat clear emitted for both stale records; raw and current state open/pending counts are zero; Bybit/Aster read-only positions and open orders for `GMTUSDT`/`LYNUSDT` are zero. | `V1 pending passive close snapshot ownership + recovered pending snapshot fallback parity` | V2 recovery can reconstruct a position from `state.open_positions` when `PendingPassiveClose.position_snapshot` is absent. Passive fallback flatness now uses that recovered position, matching the fallback class already documented in the 2026-05-18 private-symbol parity fix, so terminal flat Bybit/Aster reduce-only rejects clear stale pending passive close state instead of looping. Cloud acceptance confirmed no new-run Bybit `110017`, Aster `-2022`, or `Bad Request` loop after terminal-flat clear. |
| [CL-006-hyperliquid-trading-preflight-v1-wallet-root-fix](daily/2026-05-26.md#cluster-cl-006-hyperliquid-trading-preflight-v1-wallet-root-fix) | fixed; production verification pending | critical | `venue-hyperliquid`, `exchange-signing`, `startup-preflight`, `current-state`, `v1-parity`, `exchange-docs` | `v2.hyperliquid-trading-preflight.api-wallet-authorization-unverified + v1-wallet-account-env-drift + direct-order-bypass` | 2026-05-26 | logs showed `wallet_matches_account=false`, `signer_matches_account=false`, `clearinghouse_state_readable=true`, and `reason=api_wallet_authorization_unverified` on a deployment intended to use V1 direct-wallet semantics | this workstream | 2026-05-26 local: Hyperliquid transport suite `30 passed`; startup preflight current-state export test `1 passed`; py_compile and diff-check passed | `V1 Hyperliquid direct-wallet account canonicalization + official API-wallet fail-closed proof` | Hyperliquid default mode is now V1 account-wallet semantics: `wallet_private_key` derives the account address and stale `account_address_env` cannot create signer/account drift. Explicit `api_wallet`/`agent_wallet` mode remains supported but must prove authorization with official `/exchange` `noop`; `clearinghouseState` readability alone does not enable trading. Direct live order paths fail closed until startup preflight trusts the transport, and `current_state.hyperliquid_trading_disabled_reason` exposes the disabled reason. |
| [CL-004-bybit-hedge-resting-limit-and-aster-buffer-v1-drift](daily/2026-05-21.md#cluster-cl-004-bybit-hedge-resting-limit-and-aster-buffer-v1-drift) | fixed locally; not deployed | critical | `venue-transport`, `live-runtime`, `pending-entry-hedge`, `bybit`, `binance`, `aster`, `okx`, `gate`, `local-l2-data-plane`, `v1-parity` | `v2.bybit-hedge-price-hint-resting-limit-orphan-orders + aster-pre-snapshot-buffer-overflow-rebuild + buffered-replay-previous-link-anchor-missing` | 2026-05-21 | cloud `/opt/lightfee-v2` current Bybit open-order probe found 12 resting orders; live events showed Bybit hedge submits accepted as `Limit` but fill-unconfirmed | working tree | 2026-05-21 local: RED/GREEN targeted tests for Bybit IOC hedge, runtime missing-hedge IOC, Aster overflow/replay; targeted venue/pending/L2 suites passed; `python3 -m compileall -q lightfee tests scripts`; `git diff --check`; full `pytest -q` = 2869 passed, 2 skipped, 1 warning; GitNexus `detect_changes(scope=all)` low risk, affected_processes=0 | `V1 taker hedge order mode parity + V1 Aster/Binance buffered replay parity` | Hedge/taker orders now preserve IOC/market semantics even when carrying a price hint; runtime missing-hedge repair emits IOC requests. Aster/Binance-style pre-snapshot buffer overflow keeps the newest 4096 updates instead of immediate rebuild, and buffered replay accepts V1 previous-link anchor bridging. |
| [CL-001-live-order-mode-and-recovery-latch-root-fix](daily/2026-05-21.md#cluster-cl-001-live-order-mode-and-recovery-latch-root-fix) | fixed; deployed and short-window cloud verified | critical | `venue-transport`, `binance`, `aster`, `bybit`, `live-runtime`, `risk-mode`, `pending-entry-recovery`, `v1-parity`, `exchange-docs` | `v2.live-order-mode.binance-4061 + bybit-cancel-wrong-endpoint + clean-fail-closed-post-entry-sticky` | 2026-05-21 | cloud run `lightfee-1779296116362-1228299` after `ad9fd72` showed clean stuck fail-closed, Binance `-4061`, and Bybit cancel `HTTP 404` | `f0e5b39` | 2026-05-21 local: full `pytest -q` 2823 passed, 2 skipped; compileall and diff check clean; GitNexus detect_changes low risk; cloud `/opt/lightfee-v2` on `f0e5b39` run `lightfee-1779300740095-1232768` is `running/running` with no short-window old error terms | `V1 clean fail-closed latch parity + exchange-documented Binance/Aster position-mode and Bybit cancel semantics` | Binance/Aster now query `/fapi/v1/positionSide/dual` and send Hedge Mode `positionSide` only when required; Bybit passive cancel uses `POST /v5/order/cancel`; live housekeeping clears clean fail-closed states after recovery cleanup, not only on startup. |
| [CL-002-live-entry-l2-and-exchange-residual-watch](daily/2026-05-21.md#cluster-cl-002-live-entry-l2-and-exchange-residual-watch) | partial fixed locally; book_bootstrapping/rebuild evidence still open | high | `entry-local-l2`, `local-l2-runtime`, `candidate-tracking`, `aster`, `bybit`, `gate`, `recovery-terminality`, `exchange-docs` | `v2.entry-local-l2.readiness-invalid-catalog-pair-residual + local-l2-buffer-overflow-rebuild + aster-cancel-405-terminality + bybit-rest-ws-depth-sequence-domain-drift + okx-replay-classification-drift + gate-order-book-invalid-accuracy` | 2026-05-21 | cloud run `lightfee-1779296116362-1228299` after local-L2 catalog fixes | working tree (`f0e5b39` + closure fixes + review corrections + candidate catalog gate) | 2026-05-21 local: unsupported tradeable-candidate leak test passed; related venue/recovery/L2 suite `628 passed`; full `pytest -q` `2857 passed, 2 skipped, 1 warning`. Cloud run `lightfee-1779339979926-1296893` still has `book_bootstrapping`/hot-stale evidence, especially Hyperliquid sequence 0 and Bybit/OKX/Aster bootstrapping, so data-plane root closure remains evidence-gated. | `V1 catalog-supported pair gate before shortlist/tracking; data-plane rebuild requires exchange docs/log proof` | Unsupported venue-symbol pairs are now filtered before shortlist, primary tracking, and entry-local-L2 selection, matching V1 scan-symbol-cache admission. This roots the “unsupported symbol leaked into candidate/readiness” branch without deleting symbols to hide true `book_bootstrapping/rebuild`. Remaining Local-L2 bootstrapping/rebuild issues stay open until V1 state-machine and exchange WS/REST evidence prove the exact root. |
| [CL-003-pending-entry-hedge-submit-reconciliation-v1-drift](daily/2026-05-21.md#cluster-cl-003-pending-entry-hedge-submit-reconciliation-v1-drift) | fixed locally; not deployed | critical | `live-runtime`, `pending-entry`, `hedge-reconciliation`, `venue-hyperliquid`, `v1-parity`, `exchange-docs` | `v2.pending-entry.hedge-submit-error-no-immediate-cid-reconciliation + hyperliquid-invalid-cloid-top-level-body + hedge-retry-stable-cid + hyperliquid-account-as-vault-address` | 2026-05-21 | cloud run `lightfee-1779339979926-1296893` showed pending entries clearing only by hard ceiling and Hyperliquid `/exchange` 422 bodies with top-level `cloid`, non-128-bit order `c`, hedge `Gtc`, and account/vault drift risk | working tree | 2026-05-21 local: immediate reconciliation RED/GREEN, Hyperliquid schema/cloid/orderStatus tests, retry-CID test, wallet-account fallback and no-vault red/green; venue/private-WS targeted suites passed (`427 passed`, `112 passed`); `python3 -m compileall -q lightfee tests scripts`; `git diff --check`; full `pytest -q` = 2869 passed, 2 skipped, 1 warning; GitNexus `detect_changes(scope=all)` low risk, affected_processes=0 | `V1 hedge submit error immediate CID reconciliation + V1 Hyperliquid wire cloid/IOC/account/vault schema parity` | V2 now reconciles accepted-but-uncertain hedge submits immediately by CID, persists hedge attempt counts and uses attempt-scoped retry CIDs, sends Hyperliquid `/exchange` with no top-level `cloid`, order `c` as official 128-bit wire cloid, IOC hedge orders, account address derived from wallet when unset, and no `vaultAddress` unless a future explicit vault field is added. |
| [CL-003-local-l2-exchange-semantics-root-fix](daily/2026-05-20.md#cluster-cl-003-local-l2-exchange-semantics-root-fix) | fixed; deployed and cloud-probe verified; 2817 tests pass | high | `local-l2-data-plane`, `local-l2-ws`, `venue-parsers`, `startup-recovery`, `bitget`, `bybit`, `binance`, `okx`, `aster`, `hyperliquid`, `exchange-semantics` | `v2.local-l2.exchange-semantics.rest-snapshot-as-delta + snapshot-buffered + bitget-checksum-as-sequence + unsupported-position-probe + unsupported-local-l2-symbol` | 2026-05-20 | cloud `1029089`/`bda5505`/`ba067ff` plus first symbol-catalog deploy showed local-L2 snapshot errors, Bitget unsupported-symbol probe noise, Binance `SYSUSDT`, Aster `RLSUSDT`, and Hyperliquid `MAVUSDT` empty/unsupported book errors after Hyperliquid/fail-closed fixes were clean | `bda5505`, `ba067ff`, `05424e0`, `fd4ee51`, `6b33192` | 2026-05-20 local: direct root suite 208 passed; targeted catalog/root suite 105 passed; adjacent runtime/recovery suite 404 passed; venue suite 512 passed; full `pytest -q` 2817 passed; cloud public probe ok; current event run `lightfee-1779296116362-1228299` has `snapshot_errors=[]`, no signing terms, no probe errors | `Exchange-documented local-L2 snapshot/sequence/probe/symbol-catalog semantics` | V2 now applies authoritative snapshots during bootstrap/rebuild, parses Binance `/fapi/v1/depth`, OKX `/api/v5/market/books`, and Bybit `/v5/market/orderbook` REST payloads as snapshots, uses Bitget `seq/pseq` instead of checksum for sequence continuity, filters fallback position probes through loaded venue catalogs, and filters local-L2 startup/candidate/retained/full-snapshot-restore symbols through Binance/Aster trading catalogs and Hyperliquid non-delisted meta universe. Cloud current-run verification confirms unsupported Binance/Aster/Hyperliquid symbols are skipped instead of producing snapshot errors. |
| [CL-002-fail-closed-latch-v1-parity-drift](daily/2026-05-20.md#cluster-cl-002-fail-closed-latch-v1-parity-drift) | fixed; deployed in `1029089`; cloud-verified | critical | `supervisor`, `risk-mode`, `recovery`, `pending-entry`, `v1-parity` | `v2.fail-closed-latch-clears-with-pending-work-or-sticks-in-clean-disabled-monitor` | 2026-05-20 | working tree after Hyperliquid signer production failure investigation | `1029089` | 2026-05-20 cloud `/opt/lightfee-v2`: `risk_mode=running`, `lifecycle=running`, zero open/pending work; local full `pytest -q` 2803 passed, 2 skipped before deploy | `V1 fail_closed_latch_can_clear parity` | V2 now blocks fail-closed auto-resume while operator fail-closed, recovery block, or lifecycle-blocking recovery work exists via `needs_reconciliation()`. Clean fail-closed state auto-resumes even when `risk_monitor_enabled=false`, matching V1 recompute semantics. |
| [CL-001-hyperliquid-wallet-key-v1-parity-drift](daily/2026-05-20.md#cluster-cl-001-hyperliquid-wallet-key-v1-parity-drift) | fixed; deployed in `1029089`; cloud-verified | critical | `venue-hyperliquid`, `exchange-signing`, `live-order`, `v1-parity` | `v2.hyperliquid.exchange-signing.api-secret-used-as-wallet-private-key` | 2026-05-20 | production hedge submit error: private key length 0 bytes | `1029089` | 2026-05-20 cloud `/opt/lightfee-v2`: current-run Hyperliquid `/exchange` preflight ok; no current-run private-key signing errors; local full `pytest -q` 2803 passed, 2 skipped before deploy | `V1 Hyperliquid wallet-key signing parity` | Hyperliquid live `/exchange` signing now uses `LiveCredential.wallet_private_key`, matching V1 `resolve_wallet()`. Tests were corrected so valid signing keys live in `wallet_private_key`, not `api_secret`. |
| [CL-003-production-pending-hedge-inflight-v1-parity-drift](daily/2026-05-17.md#cluster-cl-003-production-pending-hedge-inflight-v1-parity-drift) | fixed; third-return root-fix complete, 2455 tests pass, 2 skipped | critical | `live-runtime`, `pending-entry`, `hedge-inflight-metadata`, `deadline-decision`, `fail-closed-cleanup`, `state-recovery-backward-compat`, `v1-parity` | `production.pending-entry.v1-parity-drift.hedge-inflight-string-no-metadata + deadline-breach-no-fail-closed + direct-pop-without-exposure-cleanup` | 2026-05-17 | working tree (post-CL-001 `7792db5`) | working tree (not yet committed) | 2026-05-17 local: 2455 passed, 2 skipped; 112 tests (83 round-1 + 16 round-2 + 13 real-path) covering enter_fail_closed import, pos.quantity/side/reduce_only cleanup, _abort_pending_entry bool return, resolved_entry_ids conditional, legacy hard ceiling fallback, terminalization budget in normal tick | `V1 parity drift root fix: hedge inflight metadata, deadline, fail-closed cleanup (round 1) + cleanup direction, partial-fill, adapter-missing, min-notional terminal, startup direct-pop fix (round 2) + NameError, pos.size→quantity, return bool, legacy deadline, hard ceiling cleanup-before-pop (round 3)` | Round-1: `hedge_inflight` upgraded from `str` to `HedgeInflight` dataclass. Round-2: cleanup direction by side, partial-fill re-verify, adapter-missing, min-notional hard-ceiling, startup zero-fill abort. Round-3: `enter_fail_closed` module-level import, `pos.size`→`pos.quantity`+`reduce_only=True`, `_abort_pending_entry` returns `bool`, legacy `submitted_at_ms=0` falls back to entry lifetime at hard ceiling, normal tick has terminalization budget check that forces abort(cleanup) before pop. All 5 original acceptance failures closed. Residual: min-notional below hard ceiling requires manual aggregate/flatten; extreme illiquid markets cannot guarantee market-order flatten. |
| [CL-002-D-maker-rejected-pending-v1-parity-drift](daily/2026-05-18.md#cluster-cl-002-d-maker-rejected-pending-v1-parity-drift) | fixed locally; not deployed; 2489 tests pass, 2 skipped | critical | `entry-sync`, `live-runtime`, `order-reconciliation`, `pending-entry`, `v1-parity` | `v2.entry-sync.maker-rejected-created-pending.reconcile-position-hydrated-false-maker-progress` | 2026-05-18 | production after `95a91d9` deploy showed maker reject followed by rejected pending and reconcile loop | working tree (not yet committed) | 2026-05-18 local: 2489 passed, 2 skipped; targeted maker reject/no pending and rejected-pending reconcile tests pass | `V1 deterministic maker reject terminal behavior` | V2 no longer creates or registers `PendingEntry(outcome="rejected")` after deterministic maker reject. Runtime suppresses rejected pending defensively, and reconcile clears zero-fill rejected pending before position snapshot hydration. Existing rejected pending with fill evidence is retained and logged for manual recovery instead of being auto-cleared. |
| [CL-002-E-local-l2-bootstrap-structure-v1-parity-drift](daily/2026-05-18.md#cluster-cl-002-e-local-l2-bootstrap-structure-v1-parity-drift) | fixed locally; not deployed; 2492 tests pass, 2 skipped | critical | `local-l2-runtime`, `local-l2-data-plane`, `entry-local-l2`, `live-runtime`, `v1-parity` | `v2.local-l2.bootstrap-structure-v1-parity-drift.empty-side-accepted + replay-failure-completes-hot + finalization-window-per-candidate-log-spam` | 2026-05-18 | production after `4bd6e59` deploy showed finalization-window log spam plus HOT books with `observed_at_ms=0` and `book_empty_side` readiness samples | working tree (not yet committed) | 2026-05-18 local: RED/GREEN targeted tests pass; related L2/entry suite 293 passed; full `pytest -q` 2492 passed, 2 skipped, 1 warning; `git diff --check` clean | `V1 local-L2 book validation and bootstrap completion parity` | V2 now rejects empty bid/ask snapshots and deltas like V1, does not complete bootstrap HOT when snapshot apply or buffered replay fails, and suppresses per-candidate `runtime.entry_blocked_local_l2_selection` for finalization-window blockers while retaining aggregate no-entry diagnostics. Production deployment and log verification still pending. |
| [CL-002-C-no-entry-reason-v1-aggregate-drift](daily/2026-05-18.md#cluster-cl-002-c-no-entry-reason-v1-aggregate-drift) | fixed locally; deployed and production-observed; primary_tracking confirmed as V1 admission gate not data-plane bug | high | `entry-local-l2`, `live-runtime`, `production-observability`, `v1-parity` | `v2.entry-no-entry-reason.v1-aggregate-drift.finalization-window-hidden-by-entry-local-l2-selection-blocked` | 2026-05-18 | production after `95a91d9` deploy showed coarse no-entry reason | working tree deployed to cloud `/opt/lightfee-v2` | 2026-05-22 local: 2895 passed, 2 skipped. Production confirmed: `primary_tracking=78` is an admission bucket matching V1, not a data-plane bug. | `V1 no-entry aggregate reason parity for finalization-window and local-L2 blockers` | V2 maps blocker distributions to V1 aggregate labels. `primary_tracking` is confirmed as V1 admission gate, not a bug. Production deployment verified. |
| [CL-002-production-entry-local-l2-pending-reconcile-residuals](daily/2026-05-17.md#cluster-cl-002-production-entry-local-l2-pending-reconcile-residuals) | **2026-05-18: book_hot/stale_hot_book root-fixed (state-machine); data-plane root-fixed (arming_reason, fault_reason, rebuild_trigger, evidence_logging). Phase B acceptance review fixed transition_to_bootstrapping clearing fault_reason. 2486 tests pass, 27 new V1-parity tests (including 7 real-path closed-loop).** | critical | `entry-local-l2`, `local-l2-runtime`, `live-runtime`, `pending-entry`, `order-reconciliation`, `venue-hyperliquid`, `venue-okx`, `venue-bybit`, `production-observability`, `v1-parity` | `production.entry-local-l2.primary-tracking-selection-mismatch + dual-ready-book-state-not-hot + pending-entry.reconcile-uncertain-and-hedge-residual + book-hot-stale-fault-reappeared` | 2026-05-17 | production `7792db5`; `6c954d6` post-deploy | 2026-05-18: data-plane V1 parity fixed in working tree. Phase A: DP-1 arming_reason derivation, DP-2 handle_runtime_failure fault_reason, DP-3 rebuild trigger fault_reason, DP-4 evidence logging, DP-5 observed_at_ms verified. Phase B: transition_to_bootstrapping no longer clears fault_reason — preserves context through REBUILDING→BOOTSTRAPPING→readiness path. | 2026-05-18: 2486 tests pass; 27 new V1 parity tests; 7 real-path closed-loop tests covering all fault types through full REBUILDING→BOOTSTRAPPING→arming chain. | `Production entry local-L2 pending/reconcile root fix + book_hot V2 state-machine drift root fix + data-plane V1 parity (Phase A + Phase B)` | Phase A: DP-1 `_derive_arming_reason_from_book` maps book.fault_reason keywords → SessionArmingReason. DP-2 `handle_runtime_failure` sets `book.fault_reason` for all fault types. DP-3 all 5 rebuild sites set fault_reason before `transition_to_rebuilding()`. DP-4 `_rebuild_evidence()` structured logging. DP-5 observed_at_ms preservation verified. Phase B (acceptance review): `transition_to_bootstrapping()` no longer clears `fault_reason` (only `transition_to_hot()` clears it). This fixes the real production path: REBUILDING(fault="sequence_gap:...") → BOOTSTRAPPING → `apply_book_readiness_to_leg` now sees the fault and derives correct `arming_reason=SEQUENCE_GAP` instead of `BOOK_STATUS_TRANSITION`. 7 real-path tests verify: sequence_gap→SEQUENCE_GAP, checksum→SEQUENCE_GAP, transport→TRANSPORT_FAULT_RECOVERY, stale→STALE_BOOK_RECOVERY, buffer_overflow→BOOK_STATUS_TRANSITION, HOT clears fault, cold no false arming. Residual: production deployment needed; `book_bootstrapping` may persist if bootstrap worker never succeeds (connectivity). |
| [CL-001-production-pending-entry-hedge-closure](daily/2026-05-17.md#cluster-cl-001-production-pending-entry-hedge-closure) | fixed; deployed and production-health verified | critical | `live-runtime`, `pending-entry`, `order-reconciliation`, `venue-hyperliquid`, `venue-okx`, `state-recovery`, `production-deploy` | `production.pending-entry.maker-fill-without-hedge-drive + hedge-inflight-cid-drift + pending-entry-recovery-field-loss` | 2026-05-17 | `021178e` production showed maker-fill/pending-entry closure gap | `7792db5` | 2026-05-17 local: full pytest passed; production: one sidecar/live on `7792db5`, health ok with configured 30s snapshot age | `Production pending-entry hedge closure and venue reconciliation parity` | Fully fixed and deployed. Normal tick now drives missing hedge after maker fill, finalizes pending entry into `entry.opened` / `runtime.position_opened`, uncertain hedge uses stable venue-legal inflight CID, persistent-state view preserves hedge/fill fields, Hyperliquid/OKX evidence paths are diagnostic-ready, and post-deploy sidecar/live health was verified. |
| [CL-001-production-entry-local-l2-no-entry](daily/2026-05-16.md#cluster-cl-001-production-entry-local-l2-no-entry) | partial; local root-fix implemented, production deploy/dry-run evidence pending | critical | `entry-local-l2`, `local-l2-runtime`, `live-runtime`, `sidecar-snapshot`, `order-reconciliation`, `venue-transport`, `production-observability` | `production.entry-local-l2.dual-ready-never-driven + no-entry-diagnostics-too-coarse + exchange-ack-fill-shared-risk` | 2026-05-15 | production cloud observed V2 HEAD `de0fc02`, deploy marker `8a72df4`; local repo HEAD observed `c27e33c` during planning | local working tree | 2026-05-16 local: required pytest suites passed; analyzer fixture passed; production SSH dry-run failed before evidence collection | `Production entry local-L2 root fix` | Partial. Local code now implements the entry local-L2 readiness bridge, V1-style no-entry and snapshot diagnostics, standalone production blocker analyzer, sanitized order submit/reconcile diagnostics, precision quantization guards, and startup order-path preflight. Not closed because production dry-run evidence could not be collected and the account/margin/leverage admission precheck remains an explicit residual. |
| [CL-001-production-sidecar-live-health-drift](daily/2026-05-15.md#cluster-cl-001-production-sidecar-live-health-drift) | mitigated; permanent fix planned | critical | `production-sidecar`, `systemd`, `dns`, `live-runtime`, `restart-recovery`, `current-state-export` | `production.sidecar.example-config.false-green + okx.dns.system-resolver + live.fail_closed.sticky-clean-state` | 2026-05-15 | `cd7b584` observed repo HEAD during incident documentation | production hotfix only; repo fix planned | 2026-05-15 production probes: sidecar 7 venues, live `risk_mode=running`; permanent tests pending | `Production sidecar/live health hardening` | False-green service liveness was mitigated: sidecar now uses live config and live stale fail-closed was safely cleared. Permanent guardrails are specified in `docs/superpowers/specs/2026-05-15-production-sidecar-live-health-hardening-design.md` and planned in `docs/superpowers/plans/2026-05-15-production-sidecar-live-health-hardening-implementation-plan.md`. |
| [CL-002-sidecar-funding-coverage-gap](daily/2026-05-15.md#cluster-cl-002-sidecar-funding-coverage-gap) | fixed | high | `production-sidecar`, `venue-market-data`, `okx`, `hyperliquid`, `candidate-ranking`, `v1-parity` | `sidecar.direct-market.funding-coverage.zero-okx-hyperliquid + funding_timestamp.zero-all-venues` | 2026-05-15 | `8a72df4` after live sidecar hardening deploy | `631dc5e` → `647da1e` → `5859f7f` | 2026-05-15 production cloud: all 7 venues 99-100% coverage, degraded_venues empty, snapshot_degraded resolved | `V2 sidecar V1 funding semantic parity` | Fully fixed in 3 commits: (1) 631dc5e — field-name parity across all venues, (2) 647da1e — OKX 10min funding cache + 1000 prefix stripping + skip non-listed symbols, (3) 5859f7f — Binance/Aster skip non-perpetual symbols. No residuals remain. |
| [BUG-20260514-v2-v1-parity-root-fix-loop](BUG-20260514-v2-v1-parity-root-fix-loop.md) | fixed | high | `entry-local-l2`, `sidecar-candidate-contract`, `order-reconciliation`, `bitget-l2-metadata`, `dryrun-audit`, `test-coverage`, `bybit-execution-side`, `bitget-quantity-fallback` | `v2.v1-parity.surface-copy.not-data-contract + fake-tests.green.real-path-red` | 2026-05-14 | `72ae905` review target, with prior issues visible in cloud/runtime audit loop | working tree (post-`c378352`) | 2026-05-14 local: `pytest -q` 2080 passed; targeted probes passed | `V1-to-V2 execution and venue parity replication` | All known P1/P2 residuals closed: schema-v2 candidate enrichment derives-or-fails-closed, Bitget quantity fallback covers V1 fields (`fillQty`, `filled_amount`, `size`) plus V2 extras, Bybit execution/order-status side is fail-closed, and ledger index/detail status is consistent. |
| [BUG-20260514-exchange-signing-time-rate-limit-v1-parity](BUG-20260514-exchange-signing-time-rate-limit-v1-parity.md) | fixed | critical | `venue-transport`, `exchange-signing`, `server-time-sync`, `rate-limit-runtime`, `v1-parity` | `v2.exchange-v1-parity.tests-green.runtime-semantics-red` | 2026-05-14 | `94a1d5e` | 2026-05-14 code-level V1 parity fixes | 2026-05-14 local: 2119 passed; 4 behavioral tests added | `V1 exchange REST signing, server-time, recvWindow, and rate-limit parity` | Fixed: server-time 429/418 records cooldown/backoff on local + global limiter; Binance/Aster query params follow V1 order (recvWindow before timestamp); behavioral tests verify both fixes with mocked 429 + Retry-After and param order/signature probes. |
| [CL-005-zero-fill-ghost-open-position-v1-parity-drift](daily/2026-05-22.md#cluster-cl-005-zero-fill-ghost-open-position-v1-parity-drift) | fixed locally; not deployed | critical | `live-runtime`, `pending-entry`, `venue-transport`, `bybit`, `v1-parity`, `residual` | `v2.finalize_pending_entry.zero_balanced_quantity_creates_ghost_position + v2.bybit.risk_snapshot_empty_string_parse_failure + v2.finalize_pending_entry.missing_residual_task_branches` | 2026-05-22 | cloud `cdf80c1` run `lightfee-1779422875621-1388521` showed 2 ghost open positions (PROVEUSDT, XCNUSDT) with 0/0 fill and `could not convert string to float: ''` Bybit risk snapshot error | working tree | 2026-05-22 local (v2): P0-A rev4: `_finalize_pending_entry` now has V1 `build_residual_task` parity — computes residual before branching; asymmetric positive fills persist `incremental_entry_open_partially_matched`; one-sided zero-balanced fills persist `incremental_entry_open_unmatched_residual` with V1 `repair_venue/repair_side/repair_quantity` fields and terminalize without fail-closed by themselves. `_recover_residual_repairs` no longer drops unmatched residuals, no longer full-closes matched positions, and submits only the live excess as a reduce-only repair leg. P0-B, P0-C unchanged. Full `pytest -q` 2904 passed; residual V1 parity and execution tests pass; `compileall` clean; `git diff --check` clean. | `V1 build_residual_task parity + repair_* residual contract + excess-only residual repair + V1 parse_optional_f64_field parity + real transport method tests` | V2 `_finalize_pending_entry` routes by V1 `build_residual_task + balanced_quantity` branching: zero-fill (0/0) → `passive_unfilled`; one-sided fill → `incremental_entry_open_unmatched_residual`; partial match (e.g. 10/8) → open position + `incremental_entry_open_partially_matched` residual. `_recover_residual_repairs` queries the repair venue and repairs only live excess with reduce-only IOC instead of dropping residuals or full-closing matched positions. `_parse_optional_float` matches V1 `eq_ignore_ascii_case`. Bybit risk snapshot tests exercise real transport path. |

## Query Hints

- Find local-L2 parity bugs: `rg "entry-local-l2|local_l2|prewarm" docs/bugs`
- Find ineffective attempts: `rg "Ineffective|Why Ineffective|No Effect|half-effective" docs/bugs`
- Find unresolved items: `rg "Residual|Open|not closed|follow-up" docs/bugs`
- Start from cards for recurring families: `ls docs/bugs/cards`
