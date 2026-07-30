# 2026-07-28 Runtime State Migration and Deployment Safeguards

This is the phase-4 runtime-state procedure for the V1 recovery branch. It is
written for a supervised phase-5/phase-6 deployment; it is not an instruction to
submit orders, cancel orders, edit exchange state, or clean production state
blindly.

## Scope

- Preserve V1 live-trading semantics for recovery, passive close, residual
  repair, and pending-close reconciliation.
- Treat credentialed exchange position/open-order truth as authoritative over
  recovered local JSON, replayed journal rows, sidecar snapshots, diagnostics,
  or operator memory.
- Make all runtime-state actions auditable: exact paths, exact hashes,
  pair-scoped exchange truth, append-only operator decisions, and hard stop
  rules before any restore, archive, cleanup, or deployment restart.

## Evidence anchors

- V1 recovery floor:
  - `/Users/wl/projects/LightFee/src/engine/state.rs` records recovery work
    counts from open positions, pending close reconciliations, pending entry
    hedges, pending passive closes, pending residual repairs, and live recovery
    reduce-only pairs.
  - `/Users/wl/projects/LightFee/src/engine/recovery.rs` processes pending
    close reconciliations only in live mode, removes terminal rows only after
    reconciliation or pair terminal flat live-size proof, and keeps lifecycle
    risk-only while active work remains.
  - `/Users/wl/projects/LightFee/src/execution_core/residual.rs` drives residual
    repair from pending repair tasks and persists state after changed repair
    work.
- V2 runtime surfaces:
  - `lightfee/config/schema.py` defaults runtime/persistence paths.
  - `lightfee/persistence/journal.py` is append-only JSONL with optional
    rotation; it must not be truncated as a migration shortcut.
  - `lightfee/persistence/snapshot_store.py` writes snapshots atomically via
    temp-file plus replace.
  - `lightfee/engine/state.py` persists `pending_closes`,
    `pending_passive_closes`, `pending_residual_repairs`, and normalized
    `pending_close_reconciliations`.
  - `lightfee/engine/business_contract.py` requires scoped exchange-truth
    coverage before close reconciliation can be terminal-clean.
  - `tests/test_v1_recovery_phase23_contracts.py` pins the phase-3 boundary:
    stale unscoped `positions_flat/open_orders_flat` flags cannot authorize
    close archive; unrelated venue probe errors do not block a pair, but relevant
    venue probe errors do.

## Runtime path map

First resolve the exact config file used by the deployed service. If the service
uses a `config/live.toml` or systemd `EnvironmentFile` override, the override
wins. Do not infer paths from defaults when the running config disagrees.

Default paths from `config/example.toml`, `config/live.example.toml`, and
`lightfee/config/schema.py`:

| Config field / derived surface | Default path | Authority |
| --- | --- | --- |
| `persistence.event_log_path` | `runtime/events.jsonl` | Append-only event journal; audit evidence, not cleanup by deletion. |
| `persistence.snapshot_path` | `runtime/state.json` | Restart recovery snapshot written atomically. |
| Derived current-state export | `runtime/state-current.json` | Health/export view derived from `snapshot_path`; not state authority by itself. |
| Scheduler SQLite mirror | `runtime/events.db` | Derived from `event_log_path.replace(".jsonl", ".db")`; diagnostic/index surface. |
| Prometheus metrics export | `runtime/events.prom` | Derived diagnostic surface. |
| `persistence.spread_paper_event_log_path` | `runtime/spread-paper-events.jsonl` | Spread-paper/research journal; never close/recovery truth for funding live. |
| `runtime.sidecar_snapshot_path` | `runtime/opportunity-input-snapshot.json` | Candidate discovery input; never position/open-order truth. |
| `runtime.spread_sidecar_snapshot_path` | `runtime/spread-opportunities-current.json` | Spread-paper/research snapshot; never funding close truth. |
| `runtime.spread_stats_checkpoint_path` | `runtime/spread-stats-v2-checkpoint.json` | Spread research/stat checkpoint; not funding live state authority. |
| `runtime.funding_basis_risk_checkpoint_path` | `runtime/funding-basis-risk-v1-checkpoint.json` | Funding-basis admission evidence; not close/recovery truth. |
| `runtime.fee_evidence_path` | `runtime/account-fee-evidence.json` | Account fee evidence; not exposure truth. |
| `runtime.local_l2_depth_bridge_path` | `runtime/local-l2-depth-current.json` | Local-L2 quote bridge; not position/open-order truth. |

## Runtime-state record map

- `pending_closes`: owner-managed active close work keyed by position id. Close
  execution registers it and journals `exit.pending_close_registered`.
- `pending_passive_closes`: passive close lane state. It remains current
  recovery work until terminal exchange position-flat and open-order-empty truth
  permits removal.
- `pending_close_reconciliations`: normalized append/merge queue for close-fill,
  statement, and accounting gaps. Dedupe key is `(position_id, kind)`, while
  removal additionally matches `closed_at_ms`. Terminal exchange-flat accounting
  gaps may become nonblocking, but unresolved or untrusted truth remains
  recovery work.
- `pending_residual_repairs`: residual/reduce-only cleanup tasks. Residual
  repair is live-truth driven; stale local deltas do not decide repair quantity
  or terminality.
- `live_recovery_reduce_only_pairs` and `unpaired_live_position_recoveries`:
  recovery gates for live mismatch or ownerless live exposure. They are blockers
  until current exchange truth and V1 recovery decision logic classify them.
- `v1_lifecycle_closure`, recovery ledger, owner index, current-state summary,
  and production-health output are evidence/classification surfaces. They must
  not be treated as authority to erase state without the exchange-truth checks
  below.

## Exchange-truth boundary for archive or cleanup

A pending close, passive close, residual repair, or recovered-live record may be
archived or cleared only when all checks below are true for the exact business
scope being modified:

1. Scope is explicit: `symbol`, `long_venue`, `short_venue`, `position_id` or
   residual owner id, and close/residual kind are known.
2. Both relevant venues have fresh successful credentialed position probes for
   the target symbol, either from per-symbol `fetch_position` or scoped
   `fetch_all_positions` evidence.
3. Both relevant venues have fresh successful credentialed open-order probes for
   the target symbol.
4. There is no relevant probe error, timeout, rate-limit, missing evidence, or
   low-confidence truth for either scoped venue.
5. Position truth is flat within the code epsilon for both scoped venues.
6. Open-order truth is empty for both scoped venues.
7. If residual repair is involved, official exchange quantity/notional rules
   justify either repair completion or dust terminality; otherwise retain the
   task fail-closed.

What is explicitly not enough:

- Local `open_positions`, `pending_*`, or recovered snapshot collections being
  empty.
- Unscoped account flags such as `positions_flat=true` and
  `open_orders_flat=true` without records/probe evidence covering the exact
  pair.
- Sidecar, spread, BBO, OI, funding-basis, fee, Local-L2, or research snapshots.
- Historical terminal-looking events without current position and open-order
  truth.
- A diagnostic saying risk-only/fail-closed is safe. Those are blocking modes,
  not green deployment states.

## Backup procedure before pull, restart, restore, or state migration

Use exact deployed paths after resolving config. The default example below
assumes the deployed repo root is `/opt/lightfee-v2`; replace it with the
verified live root before use.

Create one timestamped backup directory:

```bash
backup_root="/opt/lightfee-v2/runtime/backups/YYYYMMDDTHHMMSSZ-v1-recovery"
mkdir -p "$backup_root"
```

Copy only exact files that exist; do not use globs and do not recursively copy
the whole runtime directory:

```bash
cp -p /opt/lightfee-v2/runtime/events.jsonl "$backup_root/events.jsonl.before"
cp -p /opt/lightfee-v2/runtime/state.json "$backup_root/state.json.before"
cp -p /opt/lightfee-v2/runtime/state-current.json "$backup_root/state-current.json.before"
cp -p /opt/lightfee-v2/runtime/events.db "$backup_root/events.db.before"
cp -p /opt/lightfee-v2/runtime/events.prom "$backup_root/events.prom.before"
cp -p /opt/lightfee-v2/runtime/spread-paper-events.jsonl "$backup_root/spread-paper-events.jsonl.before"
cp -p /opt/lightfee-v2/runtime/opportunity-input-snapshot.json "$backup_root/opportunity-input-snapshot.json.before"
cp -p /opt/lightfee-v2/runtime/spread-opportunities-current.json "$backup_root/spread-opportunities-current.json.before"
cp -p /opt/lightfee-v2/runtime/spread-stats-v2-checkpoint.json "$backup_root/spread-stats-v2-checkpoint.json.before"
cp -p /opt/lightfee-v2/runtime/funding-basis-risk-v1-checkpoint.json "$backup_root/funding-basis-risk-v1-checkpoint.json.before"
cp -p /opt/lightfee-v2/runtime/account-fee-evidence.json "$backup_root/account-fee-evidence.json.before"
cp -p /opt/lightfee-v2/runtime/local-l2-depth-current.json "$backup_root/local-l2-depth-current.json.before"
```

If any optional file is missing, record the absence in the manifest instead of
creating a placeholder. Required files for a live funding runtime are
`events.jsonl` and `state.json`; if either is missing when the service is
expected to be running, stop the migration and investigate.

Record a manifest next to the copies:

```bash
cd /opt/lightfee-v2
git rev-parse HEAD
shasum -a 256 /opt/lightfee-v2/runtime/events.jsonl
shasum -a 256 /opt/lightfee-v2/runtime/state.json
wc -c /opt/lightfee-v2/runtime/events.jsonl
wc -c /opt/lightfee-v2/runtime/state.json
tail -n 1 /opt/lightfee-v2/runtime/events.jsonl
```

The manifest must include:

- deployed git SHA and branch;
- config file path and runtime path overrides;
- source path, backup path, size, and SHA256 for every copied file;
- last event `seq`, `run_id`, `ts_ms`, and `kind`;
- snapshot `run_id`, `tick_count`, lifecycle/risk mode, and all pending counts;
- operator name/time/reason.

## Append-only migration decisions

Every decision that changes local runtime state must be appended to
`runtime/recovery-migration-decisions.jsonl` before and after the action. The
decision record is evidence; it does not replace the normal journal.

Minimum record shape:

```json
{
  "schema": "lightfee.recovery_migration_decision.v1",
  "ts_ms": 0,
  "operator": "",
  "deployed_sha": "",
  "decision": "backup|restore_snapshot|retain_pending_work|archive_pending_close_reconciliation|release_residual_gate|abort",
  "reason": "",
  "source_paths": [],
  "backup_paths": [],
  "source_hashes": {},
  "pair_scope": {
    "symbol": "",
    "long_venue": "",
    "short_venue": "",
    "position_id": "",
    "kind": ""
  },
  "exchange_truth_hash": "",
  "exchange_truth_summary": {
    "confidence": "",
    "position_probe_venues": [],
    "open_order_probe_venues": [],
    "position_flat": false,
    "open_orders_empty": false,
    "errors": [],
    "missing_evidence": []
  },
  "result": "planned|applied|aborted"
}
```

Do not edit old decision rows. If a decision was wrong, append a new corrective
row with the old row hash and the reason for superseding it.

## Restore and cleanup rules

- Snapshot restore is allowed only while the service is stopped and only from an
  exact backup path whose hash matches the manifest.
- Preserve the current `events.jsonl` before restoring a snapshot. Do not
  truncate or roll back the append-only journal; journal history is evidence.
- If a restored snapshot conflicts with current exchange truth, the exchange
  truth wins and the runtime stays fail-closed/risk-only until the state owner is
  reconciled.
- Do not remove a `pending_close_reconciliation` row by hand unless the
  append-only decision row includes pair-scoped exchange flat/no-open-orders
  truth and the row identity `(position_id, kind, closed_at_ms)`.
- Do not remove a `pending_residual_repairs` row by hand unless the decision row
  includes repair-venue live position truth, scoped open-order truth, and either
  terminal dust evidence from exchange filters or fresh live-flat proof.
- Do not treat diagnostic DB rebuilds, spread-paper cleanup, or sidecar snapshot
  refreshes as state migration.

## Hard stop rules

Stop immediately and do not deploy, restart, archive, clear, or restore when any
condition below is true:

- Required exchange truth is missing, stale, low-confidence, or unavailable.
- A relevant scoped venue has a probe error, timeout, auth failure, rate-limit,
  or missing open-order/position evidence.
- Current exchange truth shows any nonzero position or live open order in the
  pair scope being modified.
- Local state and exchange truth disagree in a way that would require guessing
  ownership, side, quantity, or order identity.
- `events.jsonl` or `state.json` is missing, unparsable, hash-mismatched, or has
  an unexpected last event compared with the backup manifest.
- The deployed SHA/config path does not match the planned phase-5 artifact.
- Runtime contains pending passive close, pending close reconciliation, residual
  repair, unpaired live recovery, or reduce-only pair gates that are not covered
  by an explicit decision row and scoped exchange truth.
- Operator fail-closed is active and has not been explicitly lifted by the
  operator.
- Any proposed action depends on sidecar/spread/BBO/OI/research/fee snapshots as
  closure truth.

## Phase-5 readiness checklist

Before phase 6, collect read-only evidence and attach it to the recovery
artifact:

1. Git SHA, branch, `git status --short`, and config path used by the service.
2. Focused recovery tests, including the phase-3 stale-local-state guard.
3. Current-state summary: lifecycle, risk mode, pending counts, and last tick.
4. Pair-scoped exchange truth for every pending close/passive close/residual
   repair/unpaired live recovery record.
5. Backup manifest with hashes and last journal event.
6. Dry-run migration decision rows for every retained/archived runtime record.
7. Explicit small-notional allowlist, safety switch state, and rollback plan.

If any checklist item is incomplete, phase 6 remains blocked.
