# Phase 5 live gate evidence

Status: local recovery integration complete; deployment is authorized for the
normal dynamic funding flow under the 50-quote test profile.

This document records the phase-5 readiness gate for the V1 recovery branch.
The historical observations below remain evidence, while the 2026-07-29
operator decision is recorded separately so it is not confused with a hard
runtime requirement.

## 2026-07-29 authorized deployment profile

- Integration base: `7164ec3821453a83d3d1b717974f3555e421679b` (`main`), not
  the old baseline by itself.
- Recovery commit: `7a1e6ec8`; it carries only the verified scoped-truth,
  private-WS, Gate V1 contract/dual-ACK, OKX ACK, Bitget documented product
  subscription, and logging changes on top of that base.
- The temporary canary gate is deliberately not reintroduced. V1 and current
  `main` select candidates dynamically from the configured universe; imposing
  a one-symbol/one-venue canary would change that business flow.
- Operator authorization: strategy chooses the opportunity, candidate, venue,
  and symbol through the normal funding pipeline. New-entry safety is enabled
  only after the recovery deployment and a fresh exchange-truth snapshot.
- Operational cap: 50 quote per leg and one concurrent position for this
  supervised test. Exchange truth, private-WS health, final economics, hedge,
  close, residual-repair, and fail-closed reconciliation gates remain required.
- Local verification: `97` private-WS tests and `385` recovery/business/close/
  runtime/economics tests pass; production lint and whitespace checks pass.

## Scope and local baseline

- Local branch: `codex/recovery-v1-baseline`
- Local baseline commit: `4bfd856607b452b6690bd45439515227d7b50146`
- Recovery implementation scope: close reconciliation scoped truth, private-WS
  lifecycle, Bybit position truth, OKX exact `instId`, Gate V1 contract-list
  subscription plus dynamic dual-ACK activation, and log-noise suppression.
- Excluded scope remains excluded: broad BBO/spread/OI/economics/runtime
  refactors from post-baseline commits are not imported by this gate.

## Fixture-path provenance decision

The planned phase-5 suite referenced:

- `tests/test_v1_execution_lifecycle_fixture.py`
- `tests/test_v1_live_contract_fixture.py`

`git log --all --name-status -- tests/test_v1_execution_lifecycle_fixture.py tests/test_v1_live_contract_fixture.py`
shows both files were added only by commit
`dc38c1b831dcecb5449bb5b290372905234257fa` (`refactor: simplify live funding
entry path`, 2026-07-28 12:29:32 +0800). That commit is in the excluded broad
post-baseline refactor set for this recovery path. The files are therefore not
restored and no placeholder tests are created.

The corrected phase-5 suite for this branch is the available V1/recovery
coverage:

```bash
.venv/bin/pytest -q \
  tests/test_v1_private_ws_parity.py \
  tests/test_close_execution.py \
  tests/test_runtime_entry_flow.py \
  tests/test_v1_recovery_phase23_contracts.py
```

Latest result (2026-07-29, split only to keep the slow close executor within
the command runner limit): `276 passed` total.

- `tests/test_v1_private_ws_parity.py`: `92 passed`.
- `tests/test_close_execution.py`: `50 + 17 + 7 passed` (all 74 collected
  cases, including close chunk execution and failed-close compensation).
- `tests/test_runtime_entry_flow.py`: `96 passed`.
- `tests/test_v1_recovery_phase23_contracts.py`: `14 passed`.

The Gate subset passed three consecutive runs (`12 passed` each): V1 initial
contract-list messages, no-id legacy ACK fallback, dynamic single-contract
subscriptions, dual-channel activation, rejected/malformed ACK retention, and
ACK timeout failure all have explicit coverage.

The live canary configuration/startup contracts also passed (`10 passed`): a
live funding entry requires an enabled canary, statement-reconcilable venues,
one concurrent position, a hard cap at or below 30 quote, account-fee evidence,
and the immutable economics floors.

## Scoped accounting-only terminal-flat fix

Two previous failures were caused by accounting-only terminal-flat fixtures using
unscoped recovered/global flags:

- `test_terminal_flat_accounting_gap_is_retained_as_accounting_only_backfill`
- `test_accounting_only_backfill_does_not_block_entry_gate`

The fix does not trust `positions_flat`, `open_orders_flat`, or any recovered
local flags by themselves. The accounting-only terminal-flat path now feeds the
same pair-scoped clean-truth resolver used by close reconciliation:

- scoped symbol;
- scoped long/short venues;
- flat position rows for the pair;
- empty open-order proof for the pair;
- successful position and open-order probe evidence for the pair.

Focused verification:

```bash
.venv/bin/pytest -q \
  tests/test_close_execution.py::test_terminal_flat_accounting_gap_is_retained_as_accounting_only_backfill \
  tests/test_close_execution.py::test_accounting_only_backfill_does_not_block_entry_gate \
  tests/test_v1_recovery_phase23_contracts.py
```

Latest result: `12 passed in 0.23s`.

## GitNexus and local audit

Freshness evidence:

- `git rev-parse HEAD`: `4bfd856607b452b6690bd45439515227d7b50146`
- `npx gitnexus status`: indexed/current commit `4bfd856`, status up to date.

Pre-edit impact evidence:

- `CloseRuntime._archive_terminal_flat_pending_close_reconciliation`: low risk,
  1 impacted symbol, affected process `_process_pending_close_reconciliations`.
- `CloseRuntime._current_exchange_truth_for_close_reconciliation`: low risk,
  3 impacted symbols, affected process `_process_pending_close_reconciliations`.
- `CloseRuntime._process_pending_close_reconciliations`: low risk, no upstream
  dependants reported.
- `EntryGateRuntime._gate_pending_close_reconciliation`: low risk, no upstream
  dependants reported.

Scope audit:

- `detect_changes` (2026-07-29) reports 17 tracked files, 138 changed symbols,
  16 affected processes, and CRITICAL aggregate risk. This is expected for the
  accumulated recovery/private-stream work and includes no broad BBO, spread,
  OI, or economics runtime import.
- Existing tracked recovery diff remains limited to recovery/logging/venue-truth
  files plus the close-runtime scoped-truth tightening and private-stream
  workers.
- `git diff --check`: pass.
- Ruff passes for the modified Gate worker; it compiles successfully.
- Narrow high-signal secret scan over changed production source files: no
  credential assignment was added.

## Read-only deployment-state evidence collected

Cloud inspection was read-only. Credentials and signed URLs are intentionally not
recorded here.

- Cloud repository: `/opt/lightfee-v2`
- Cloud branch: `main`
- Deployed commit observed: `7164ec3821453a83d3d1b717974f3555e421679b`
- This is not the local recovery branch commit
  `4bfd856607b452b6690bd45439515227d7b50146`; no recovery deployment occurred.

Service lifecycle observed:

- `lightfee-live.service`: active/running
- `lightfee-sidecar.service`: active/running
- `lightfee-spread-bbo.service`: active/running
- `lightfee-spread-sidecar.service`: active/running
- Legacy `lightfee.service`: failed

Runtime file evidence observed:

- `/opt/lightfee-v2/runtime/live-state-current.json`
- `/opt/lightfee-v2/runtime/live-state.json`
- `/opt/lightfee-v2/runtime/live-events.jsonl`
- stale legacy `/opt/lightfee-v2/runtime/state-current.json`
- missing legacy `/opt/lightfee-v2/runtime/state.json`

Runtime lifecycle/risk snapshot:

- lifecycle: `running`
- risk mode: `running`
- global risk mode: `running`
- recovery blocked reason: absent
- open positions: `0`
- pending entries: `0`
- pending closes: `0`
- pending passive closes: `0`
- pending residual repairs: `0`
- pending close reconciliations: `26`
- blocking pending close reconciliations: `0`
- terminal-flat accounting-only reconciliations: `26`
- pending close reconciliation symbols observed:
  `EVAAUSDT`, `VANRYUSDT`, `USUSDT`, `BSBUSDT`, `SKLUSDT`, `RESOLVUSDT`,
  `TUSDT`, `TRXUSDT`, `YFIUSDT`, `GWEIUSDT`, `SIRENUSDT`, `BLURUSDT`,
  `SAHARAUSDT`
- unpaired live-position recovery records: at least 20 stale records were
  visible in the snapshot sample.

Missing proof:

- The live-state payload does not persist per-venue private-WS health or Gate
  subscription ACK state.
- The 26 local terminal-flat reconciliation records were not altered or
  cleared; runtime-local evidence is never used as authority to clear them.

### 2026-07-29 read-only exchange refresh

At `2026-07-29T03:48:14Z`, the deployed service's read-only diagnostic was run
with its existing credential environment against Binance, Bybit, OKX, Gate,
Bitget, and Aster. It reported:

- `gate_passed=true`, `status=healthy`, and `risk=low`;
- exchange truth available with `confidence=high`;
- no non-zero position, no open order, and no missing required venue;
- local runtime: zero open positions, pending entries, and pending closes.

This is fresh exchange evidence for the currently deployed service only. It is
not a recovery deployment proof, does not prove the new Gate ACK lifecycle is
running, and does not authorize archival of the 26 local reconciliation rows.
The last 5,000 structured events contained no entry/open/close submission kind;
they contained scan/revalidation and reconciliation-query activity only.

## Actual safety-switch and cap evidence

Actual live config observed at `/opt/lightfee-v2/config/live.toml`:

- `runtime.mode = "live"`
- `strategy.funding_new_entries_enabled = true`
- `strategy.funding_canary_*`: absent
- `strategy.symbols`: 620 contracts
- `strategy.live_entry_notional_cap_quote = 50.0`
- `strategy.small_test_max_entry_notional_quote = 60.0`

Local examples/schema provide defaults such as canary venues
`["binance", "bybit", "okx", "gate"]` and canary notional cap `30.0`, but those
defaults do not prove the actual deployed operator-approved allowlist.

Phase-6 allowlist/cap status:

- Explicit venue allowlist: not proven from actual live config.
- Explicit symbol allowlist: absent in deployed old main. Its `symbols`
  universe contains 620 contracts, so it cannot be treated as a supervised
  single-pair canary allowlist. In the recovery branch, the final live-canary
  admission boundary now requires the configured global `symbols` set to
  normalize to exactly one contract and requires the candidate to match it.
  This guard applies only to a new live funding entry; it does not gate an
  existing position, pending hedge, residual repair, recovery, or close.
- Canary notional cap source: not proven from actual live config.
- General live-entry notional cap: observed as `50.0`, but that is not a canary
  allowlist/cap proof.
- The deployed `7164ec38` implementation does not expose
  `StrategyConfig.funding_canary_enabled`; its entry record explicitly writes
  `funding_canary_enabled_at_entry=False`. Thus it cannot supply the recovery
  branch's canary enforcement, regardless of configuration defaults.
- Safety switch: not proven fail-closed. `funding_new_entries_enabled=true`
  remains present in the deployed configuration while the recovery's explicit
  canary policy is not deployed.
- A dedicated `funding_canary_allowed_symbols` field remains absent. Adding it
  would change `StrategyConfig`; fresh GitNexus impact reports CRITICAL
  aggregate risk (157 impacted symbols and 75 direct imports). It is not needed
  for the current release: the lower-risk operational path is to deploy with
  `symbols` reduced to the operator-selected single canary contract, which the
  recovery runtime now independently enforces.

Additional local canary-boundary evidence: `12 passed` across config, startup,
and economics contracts, and the full entry/economics regression subset passed
`172` tests. The test suite also exposed a missing local definition of
`_LIVE_CANARY_FEE_EVIDENCE_MAX_AGE_MS`; live admission would previously raise
`NameError` instead of returning a fail-closed rejection. It now owns the same
24-hour upper bound as configuration validation and has a regression proving an
overage value returns `funding_canary_fee_evidence_age_invalid`.

## Actual account-fee evidence status

Fresh read-only cloud inspection on 2026-07-29 found:

- `LIGHTFEE_FEE_EVIDENCE_HMAC_KEY` is present and non-empty in the service
  environment; its value was neither displayed nor changed.
- `runtime/account-fee-evidence.json` exists (747 bytes).
- The deployed validator rejects that file: `valid=false`,
  `integrity_verified=false`, `covered_venues=[]`, with
  `reason=fee_evidence_stale:bybit`.
- The deployed TOML has no `runtime.fee_evidence_*` overrides and no
  `runtime.fee_evidence_account_identity_hashes`; recovery defaults do not
  supply the required account bindings.

Therefore the existing file is not authority for a direct live canary. Before
deployment, the operator must supply one exact symbol and two
statement-reconcilable venues, then create fresh schema-v3 signed private fee
evidence for those venues and set only their SHA256 account-identity bindings
in the canary configuration. The offline validator must pass with
`--require-integrity` and both selected `--require-venue` values. No public
fee page or static fee configuration can substitute for this evidence.

### Read-only fee-source feasibility refresh

On 2026-07-29, two credentialed, read-only endpoint probes were run from the
deployed host. They created no evidence artifact, modified no configuration,
and did not submit/cancel any exchange order:

- Bybit `GET /v5/account/fee-rate?category=linear&symbol=BTCUSDT` returned
  `retCode=0` with exactly one row containing maker and taker fee fields.
- The deployed V2 transport successfully read OKX account configuration and
  `GET /api/v5/account/trade-fee?instType=SWAP&instFamily=BTC-USDT`; both
  responses returned `code=0`, with an account identity field and maker/taker
  fee fields respectively. This uses the production signing path rather than
  a hand-built HTTP client.

The probes establish that the two existing evidence venues can supply fresh
private-source material. They do **not** refresh or validate the stale
schema-v3 artifact, bind any identity hash in live configuration, select the
operator's final canary pair, or unlock deployment/phase 6.

## Read-only current canary-candidate check

The current opportunity snapshot was inspected read-only on 2026-07-29 at
`04:16:08Z`. It was fresh (about six seconds old), contained 470 funding
candidates, and had no degraded domains, symbols, or venues. It nevertheless
contained **zero** candidates eligible for a new live entry under the existing
hard filters:

- all 470 candidates were blocked as `outside_scan_window`;
- 430 were also below the worst-case edge floor and 428 below the expected
  edge floor;
- no candidate was simultaneously unblocked and not entry-capacity-constrained.

The earliest displayed funding bucket was `05:00:00Z` (27 candidates), followed
by `08:00:00Z` (443 candidates). A candidate must be re-evaluated from a fresh
snapshot inside its actual scan window; the present output cannot justify a
symbol/venue selection or a live entry.

## Private-WS health

Evidence available:

- Cloud private/live service processes were active/running.
- Local private-WS parity/recovery tests passed in the corrected phase-5 suite.

Missing proof:

- No per-venue runtime private-WS worker health, subscription count, reconnect
  counter, or credentialed private-stream freshness proof exists in the
  deployed state payload. In particular, the new Gate dual-ACK state cannot be
  observed until the recovery code is deployed.

Phase 6 must not start until private-WS health is proven per venue for the
operator-approved canary pair.

## Backup commands for a future supervised cutover

These commands are documentation only. They were not executed by this phase-5
revision.

```bash
backup_root="/opt/lightfee-v2/runtime/backups/$(date -u +%Y%m%dT%H%M%SZ)-phase5"
mkdir -p "$backup_root"
cp -p /opt/lightfee-v2/runtime/live-events.jsonl "$backup_root/live-events.jsonl.before"
cp -p /opt/lightfee-v2/runtime/live-state.json "$backup_root/live-state.json.before"
cp -p /opt/lightfee-v2/runtime/live-state-current.json "$backup_root/live-state-current.json.before"
cp -p /opt/lightfee-v2/config/live.toml "$backup_root/live.toml.before"
shasum -a 256 "$backup_root"/*
```

## Rollback commands for a future supervised cutover

These commands are documentation only. They were not executed by this phase-5
revision.

```bash
systemctl stop lightfee-live.service
cp -p "$backup_root/live-state.json.before" /opt/lightfee-v2/runtime/live-state.json
cp -p "$backup_root/live-state-current.json.before" /opt/lightfee-v2/runtime/live-state-current.json
cp -p "$backup_root/live.toml.before" /opt/lightfee-v2/config/live.toml
systemctl start lightfee-live.service
systemctl status lightfee-live.service --no-pager
```

If any order, position, or reconciliation truth is uncertain, do not roll forward
by editing runtime state. Keep the service stopped or safety-off and collect
fresh exchange truth first.

## Close/reconciliation pass criteria

Before phase 6:

1. `pending_close_count == 0`
2. `pending_passive_close_count == 0`
3. `pending_residual_repair_count == 0`
4. `pending_close_reconciliation_blocking_count == 0`
5. Every accounting-only terminal-flat reconciliation is backed by pair-scoped
   exchange truth for its exact symbol and both venues.
6. Every unpaired live-position recovery record is either:
   - proven flat/no-open-order by fresh pair-scoped exchange truth; or
   - retained as a blocking item and excluded from new-entry admission.
7. No recovered/local flags may clear or archive a record without matching
   exchange truth.

## Hard stop conditions

Stop and do not start phase 6 if any of these are true:

- corrected phase-5 pytest suite is not green;
- GitNexus index is stale or `detect_changes` shows unexpected scope;
- deployed commit is not the reviewed recovery commit;
- `funding_new_entries_enabled` is true without explicit canary allowlist/cap;
- safety switch is not fail-closed before supervised enablement;
- explicit venue/symbol allowlist is missing;
- small-notional canary cap source is missing;
- credentialed pair-scoped exchange position/open-order truth is missing;
- any unexpected exchange position or open order exists;
- any residual, passive close, pending close, or blocking reconciliation exists;
- private-WS per-venue health is missing or stale;
- legacy service/process ownership conflicts with the intended supervisor;
- any deployment, state migration, or rollback command differs from the reviewed
  path.
