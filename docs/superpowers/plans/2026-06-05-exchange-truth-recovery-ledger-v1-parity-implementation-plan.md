# Exchange Truth Recovery Ledger V1 Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild pending-entry recovery, live-truth ownership, residual cleanup, and entry gating around one V1-parity recovery ledger, then delete the earlier surface-fix branches that bypass that contract.

**Architecture:** Add a pure recovery ledger, a shared exchange-truth snapshot adapter, an owner index, and a pending-entry terminalizer. Wire `LiveRuntime` to build the ledger at startup and before normal entry scan, so exchange truth and recovery work drive lifecycle instead of post-hoc diagnostics.

**Tech Stack:** Python 3.12, pytest, existing LightFeeV2 runtime and venue adapter contracts, existing journal/snapshot persistence, GitNexus impact analysis, existing deploy and production verification scripts.

---

## Global Rules

- Start read-only for production evidence. Do not submit orders, cancel orders,
  or edit runtime state while gathering evidence.
- Before editing any function, class, or method, run GitNexus impact analysis
  for the exact target symbol and record the risk in the task notes.
- Docs-only edits do not require GitNexus impact analysis.
- Each behavior change starts with a RED test that fails for the current V2
  shape.
- Do not add a new CL-specific branch when the case can be mapped to the
  recovery ledger.
- Do not remove old logic until the new ledger path has equivalent or stronger
  coverage.

## Target Files

Create:

- `lightfee/engine/recovery_ledger.py`
- `lightfee/engine/exchange_truth.py`
- `lightfee/engine/recovery_owner_index.py`
- `lightfee/engine/pending_entry_terminalizer.py`
- `tests/engine/test_recovery_ledger.py`
- `tests/engine/test_recovery_owner_index.py`
- `tests/engine/test_exchange_truth_runtime.py`
- `tests/engine/test_pending_entry_terminalizer.py`
- `tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py`

Modify:

- `lightfee/engine/runtime.py`
- `lightfee/engine/state.py`
- `scripts/diagnose_live.py`
- `scripts/verify_production_services.py`
- `tests/test_pending_entry_v1_semantic_drift.py`
- `tests/test_runtime_entry_flow.py`
- `tests/test_live_startup_preflight.py`
- `tests/test_live_entry_hedge_root_fix.py`
- `tests/test_diagnose_live.py`
- `tests/ops/test_production_health.py`
- `docs/bugs/contracts/pending-entry-live-truth-contract.md`
- `docs/bugs/cards/pending-entry-terminality-live-truth.md`
- `docs/bugs/BUG_INDEX.md`
- `docs/bugs/daily/2026-06-05.md`

## Task 1: Capture Current Incident Shapes As Fixtures

**Files:**

- Create: `tests/fixtures/live_incidents/2026-06-05/README.md`
- Create: `tests/fixtures/live_incidents/2026-06-05/trxusdt_open_order_local_flat.json`
- Create: `tests/fixtures/live_incidents/2026-06-05/seiusdt_positive_fill_local_false_flat.json`
- Test: `tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py`

- [ ] **Step 1: Record sanitized fixture contract**

Write fixture JSON with these non-secret fields:

```json
{
  "incident_id": "trxusdt-bybit-open-order-local-flat",
  "symbol": "TRXUSDT",
  "local": {
    "open_positions": [],
    "pending_entries": [],
    "pending_residual_repairs": []
  },
  "exchange_truth": {
    "positions": [],
    "open_orders": [
      {
        "venue": "bybit",
        "symbol": "TRXUSDT",
        "side": "buy",
        "quantity": 72.0,
        "price": 0.33044,
        "reduce_only": false,
        "order_id": "a84df707-efb3-4e40-bab1-641a4eb0f3d4"
      }
    ]
  },
  "expected_work_kind": "orphan_maker_order"
}
```

The `SEIUSDT` fixture must contain maker positive fill `455.0`, hedge partial
or uncertain fill evidence, and local open positions empty.

- [ ] **Step 2: Write RED fixture tests**

Add tests that assert the future ledger classifies:

```python
def test_trxusdt_open_maker_order_local_flat_is_blocking_recovery_work():
    fixture = load_incident("trxusdt_open_order_local_flat.json")
    ledger = RecoveryLedger.from_incident_fixture(fixture)

    assert ledger.has_blocking_work()
    assert ledger.work_items[0].kind == "orphan_maker_order"
    assert ledger.allows_new_entries is False


def test_seiusdt_positive_fill_local_false_flat_is_not_proven_flat():
    fixture = load_incident("seiusdt_positive_fill_local_false_flat.json")
    ledger = RecoveryLedger.from_incident_fixture(fixture)

    assert ledger.has_blocking_work()
    assert ledger.contains_positive_fill_evidence("SEIUSDT")
    assert ledger.is_proven_flat("SEIUSDT") is False
```

- [ ] **Step 3: Run the RED tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py
```

Expected before implementation: import or assertion failure for missing
`RecoveryLedger`.

## Task 2: Build The Pure Recovery Ledger

**Files:**

- Create: `lightfee/engine/recovery_ledger.py`
- Test: `tests/engine/test_recovery_ledger.py`
- Test: `tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py`

- [ ] **Step 1: Run impact analysis**

Run:

```bash
npx gitnexus impact --name LightFeeV2 --symbol RecoveryLedger
```

Expected: no existing symbol. If GitNexus does not support the new symbol yet,
record that this is a new-file impact.

- [ ] **Step 2: Add pure model tests**

Cover these cases:

- local empty plus live non-reduce open order creates blocking
  `orphan_maker_order`;
- local empty plus live nonzero position creates blocking
  `unpaired_live_position`;
- pending entry with maker positive fill creates blocking
  `owned_pending_entry`;
- residual task with live flat creates `pending_residual_repair` and can
  resolve to `proven_flat`;
- exchange truth unavailable creates `ambiguous_exchange_truth`;
- no live position, no open order, no local work creates `proven_flat`.

- [ ] **Step 3: Implement dataclasses and classifiers**

Implement:

- `ExchangeArtifact`
- `RecoveryOwner`
- `RecoveryWorkItem`
- `RecoveryDecision`
- `RecoveryLedger`
- `RecoveryLedger.from_local_and_exchange_truth(...)`
- `RecoveryLedger.from_incident_fixture(...)`
- `RecoveryLedger.has_blocking_work()`
- `RecoveryLedger.allows_new_entry(candidate)`
- `RecoveryLedger.is_proven_flat(symbol)`
- `RecoveryLedger.contains_positive_fill_evidence(symbol)`

- [ ] **Step 4: Run ledger tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_recovery_ledger.py tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py
```

Expected after implementation: pass.

## Task 3: Build Runtime Exchange Truth Snapshots

**Files:**

- Create: `lightfee/engine/exchange_truth.py`
- Test: `tests/engine/test_exchange_truth_runtime.py`
- Modify: `scripts/diagnose_live.py`
- Modify: `scripts/verify_production_services.py`
- Test: `tests/test_diagnose_live.py`
- Test: `tests/ops/test_production_health.py`

- [ ] **Step 1: Run impact analysis**

Run impact for the diagnose and verifier collection functions currently used
for exchange truth probes.

- [ ] **Step 2: Add tests for shared truth shape**

Assert snapshots include:

- venue;
- symbol;
- endpoint or method name;
- timeout budget;
- started and finished timestamps;
- positions;
- open orders;
- unsupported-symbol evidence;
- timeout evidence;
- `truth_available` boolean.

- [ ] **Step 3: Implement `ExchangeTruthSnapshot` and collection helpers**

The runtime helper must not call the CLI script. Move shared business parsing
into importable functions and keep CLI rendering in the scripts.

- [ ] **Step 4: Keep diagnose and verifier behavior identical**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_diagnose_live.py tests/ops/test_production_health.py tests/engine/test_exchange_truth_runtime.py
```

Expected: existing diagnose/verifier gates still fail on local-flat plus live
position or live open order.

## Task 4: Build Recovery Owner Index

**Files:**

- Create: `lightfee/engine/recovery_owner_index.py`
- Test: `tests/engine/test_recovery_owner_index.py`
- Test: `tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py`

- [ ] **Step 1: Run impact analysis**

Run impact for the state classes used as owner inputs:

```bash
npx gitnexus impact --name LightFeeV2 --symbol PendingEntry
npx gitnexus impact --name LightFeeV2 --symbol OpenPosition
```

- [ ] **Step 2: Add owner tests**

Cover:

- order id matches pending entry;
- client order id matches pending entry;
- live position matches open position;
- residual repair matches symbol and repair venue;
- journal event reconstructs missing pending owner;
- `TRXUSDT` order with no owner remains orphan.

- [ ] **Step 3: Implement owner index**

Expose:

- `RecoveryOwnerIndex.from_state(...)`
- `RecoveryOwnerIndex.from_state_and_journal(...)`
- `owner_for_order(...)`
- `owner_for_position(...)`
- owner confidence values `proven`, `probable`, `orphan`.

- [ ] **Step 4: Run owner tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_recovery_owner_index.py tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py
```

Expected: fixture cases classify as owned or orphan without guessing.

## Task 5: Extract Pending Entry Terminalizer

**Files:**

- Create: `lightfee/engine/pending_entry_terminalizer.py`
- Modify: `lightfee/engine/runtime.py`
- Test: `tests/engine/test_pending_entry_terminalizer.py`
- Test: `tests/test_pending_entry_v1_semantic_drift.py`
- Test: `tests/test_live_entry_hedge_root_fix.py`

- [ ] **Step 1: Run impact analysis**

Run:

```bash
npx gitnexus impact --name LightFeeV2 --symbol _finalize_pending_entry
npx gitnexus impact --name LightFeeV2 --symbol _recover_pending_entry_hedges
```

Pause and report if impact is HIGH or CRITICAL.

- [ ] **Step 2: Add RED tests for terminal decisions**

Cover:

- terminal zero-fill with no live order and no live position returns
  `passive_unfilled`;
- terminal zero-fill with live open order returns deferred;
- maker positive fill plus hedge partial returns matched open plus residual;
- maker positive fill plus no hedge returns residual cleanup or fail-closed;
- missing live truth returns fail-closed or retained pending, not terminal
  healthy;
- caller cannot pop pending when the terminalizer returns deferred.

- [ ] **Step 3: Move terminality logic into the new module**

Keep a compatibility wrapper on `LiveRuntime._finalize_pending_entry()` while
callers are migrated. The wrapper must return the terminalizer decision and the
old boolean only where tests prove callers still require it.

- [ ] **Step 4: Run focused pending tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_pending_entry_terminalizer.py tests/test_pending_entry_v1_semantic_drift.py tests/test_live_entry_hedge_root_fix.py
```

Expected: pass without losing positive fill evidence.

## Task 6: Wire Ledger Into Startup Recovery

**Files:**

- Modify: `lightfee/engine/runtime.py`
- Modify: `lightfee/engine/state.py`
- Test: `tests/test_live_startup_preflight.py`
- Test: `tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py`

- [ ] **Step 1: Run impact analysis**

Run impact for:

- `_recover_startup_live_positions`
- `_recover_pending_entry_hedges`
- startup lifecycle recompute functions in `LiveRuntime`.

- [ ] **Step 2: Add RED startup tests**

Cover:

- local flat plus `TRXUSDT` open maker order enters risk-only/fail-closed;
- local flat plus Bybit `SEIUSDT` live position hydrates or blocks recovery,
  never healthy;
- startup pending work without open positions retries live truth recovery;
- startup recovery work snapshot includes ledger blockers.

- [ ] **Step 3: Implement startup ledger build**

Startup must:

1. collect exchange truth;
2. build owner index;
3. build ledger;
4. persist or expose recovery work snapshot;
5. block normal startup activation when ledger has blocking work.

- [ ] **Step 4: Run startup tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_live_startup_preflight.py tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py
```

Expected: local-flat/exchange-truth mismatch is visible before normal entry
scan can run.

## Task 7: Wire Ledger Into Normal Tick And Entry Gate

**Files:**

- Modify: `lightfee/engine/runtime.py`
- Test: `tests/test_runtime_entry_flow.py`
- Test: `tests/test_v1_parity_pending_entry_recovery_red.py`

- [ ] **Step 1: Run impact analysis**

Run:

```bash
npx gitnexus impact --name LightFeeV2 --symbol _gate_pending_entry_dedup
npx gitnexus impact --name LightFeeV2 --symbol _reconcile_pending_state
```

- [ ] **Step 2: Add RED gate tests**

Cover:

- unresolved ledger item blocks same symbol and venue overlap;
- orphan maker order blocks every new entry risk;
- ambiguous exchange truth blocks new entries;
- clean ledger allows existing candidate path.

- [ ] **Step 3: Replace local-only gates**

Route candidate admission through `RecoveryLedger.allows_new_entry(candidate)`.
Keep V1 same-symbol venue-overlap protection.

- [ ] **Step 4: Run entry flow tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_runtime_entry_flow.py tests/test_v1_parity_pending_entry_recovery_red.py
```

Expected: normal candidate selection is unchanged when ledger is clean and
blocked when ledger has recovery work.

## Task 8: Drive Owned And Orphan Open Orders

**Files:**

- Modify: `lightfee/engine/runtime.py`
- Modify: `lightfee/engine/recovery_ledger.py`
- Test: `tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py`
- Test: `tests/engine/test_recovery_ledger.py`

- [ ] **Step 1: Add RED recovery decision tests**

Cover:

- owned non-reduce maker order queries progress before cleanup;
- owned stale order can request bounded cancel or terminal no-fill only after
  exchange evidence;
- orphan non-reduce maker order fail-closes and requires operator evidence;
- reduce-only orphan order is lower risk but still not ignored.

- [ ] **Step 2: Implement recovery decisions**

Implement ledger decisions that return:

- `owned_order_cancel_requested` for proven owned stale maker orders;
- `fail_closed_operator_block` for orphan maker orders;
- `ambiguous_exchange_truth` when open-order truth cannot be fetched.

Do not auto-cancel an orphan order without proven owner evidence.

- [ ] **Step 3: Run incident tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_recovery_ledger.py tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py
```

Expected: `TRXUSDT` is either owned by fixture evidence or blocks as orphan.

## Task 9: Remove Bypass Complexity

**Files:**

- Modify: `lightfee/engine/runtime.py`
- Test: `tests/engine/test_pending_entry_terminalizer.py`
- Test: `tests/test_runtime_entry_flow.py`

- [ ] **Step 1: Add static bypass test**

Write a test or repository scan helper that fails on direct pending-entry
removal outside the terminalizer allowlist:

```bash
rg -n "pending_entries\\.pop|del .*pending_entries" lightfee/engine/runtime.py lightfee/engine
```

Allowed removals must be documented next to terminalizer decisions.

- [ ] **Step 2: Delete obsolete branches**

Remove or collapse:

- direct post-finalizer `pending_entries.pop()`;
- rejected-positive-fill retain loops that do not call the terminalizer;
- startup pending recovery branches that duplicate ledger classification;
- local-flat healthy branches that do not consult exchange truth;
- duplicate same-symbol gate logic outside the ledger.

- [ ] **Step 3: Run focused cleanup tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_pending_entry_terminalizer.py tests/test_runtime_entry_flow.py tests/test_pending_entry_v1_semantic_drift.py
```

Expected: no bypass removal remains and existing pending-entry behavior still
passes.

## Task 10: Align Diagnostics And Production Gates

**Files:**

- Modify: `scripts/diagnose_live.py`
- Modify: `scripts/verify_production_services.py`
- Test: `tests/test_diagnose_live.py`
- Test: `tests/ops/test_production_health.py`
- Test: `tests/engine/test_exchange_truth_runtime.py`

- [ ] **Step 1: Add agreement tests**

Cover:

- runtime ledger local-flat/open-order blocker renders as diagnose unhealthy;
- verifier cannot report green without exchange truth evidence;
- unavailable truth is missing evidence, not flat;
- all seven venues are represented in the truth collection path.

- [ ] **Step 2: Reuse exchange truth helpers**

Import shared exchange-truth parsing and classification from the runtime module
instead of duplicating business interpretation in scripts.

- [ ] **Step 3: Run diagnostics tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/test_diagnose_live.py tests/ops/test_production_health.py tests/engine/test_exchange_truth_runtime.py
```

Expected: diagnose and verifier agree with runtime ledger semantics.

## Task 11: Update Bug Contract And Ledger Docs

**Files:**

- Modify: `docs/bugs/contracts/pending-entry-live-truth-contract.md`
- Modify: `docs/bugs/cards/pending-entry-terminality-live-truth.md`
- Modify: `docs/bugs/BUG_INDEX.md`
- Modify: `docs/bugs/daily/2026-06-05.md`

- [ ] **Step 1: Update contract rows**

Add explicit rows for:

- local flat plus exchange open maker order;
- orphan maker order owner unavailable;
- shared runtime/diagnose exchange truth;
- terminalizer as the only pending-entry removal authority.

- [ ] **Step 2: Collapse CL status**

Document CL-048, CL-049, CL-050, and the `TRXUSDT` recurrence as one
exchange-truth recovery ledger family. Keep old IDs for chronology only.

- [ ] **Step 3: Record verification commands**

Document the exact focused tests, full pytest, compileall, diff check,
GitNexus detect-changes, and cloud read-only verification outputs.

## Task 12: Full Verification And Deployment Handoff

**Files:**

- No planned source edits.

- [ ] **Step 1: Run focused tests**

Run:

```bash
.venv/bin/python -m pytest -q tests/engine/test_recovery_ledger.py tests/engine/test_recovery_owner_index.py tests/engine/test_exchange_truth_runtime.py tests/engine/test_pending_entry_terminalizer.py
.venv/bin/python -m pytest -q tests/live_harness/test_exchange_truth_recovery_ledger_incidents.py
.venv/bin/python -m pytest -q tests/test_live_startup_preflight.py tests/test_live_entry_hedge_root_fix.py tests/test_runtime_entry_flow.py tests/test_passive_close.py
.venv/bin/python -m pytest -q tests/test_diagnose_live.py tests/ops/test_production_health.py
```

- [ ] **Step 2: Run repository verification**

Run:

```bash
.venv/bin/python -m pytest -q
.venv/bin/python -m compileall lightfee scripts tests
git diff --check
npx gitnexus detect-changes --name LightFeeV2
```

- [ ] **Step 3: Commit and push**

Run:

```bash
git status --short
git add lightfee tests scripts docs
git commit -m "fix: enforce v1 exchange truth recovery ledger"
git push origin main
```

- [ ] **Step 4: Cloud fast-forward deploy**

Run on cloud:

```bash
cd /opt/lightfee-v2
git fetch origin main
git status --short
git pull --ff-only origin main
git rev-parse --short HEAD
git rev-parse --short HEAD > .deploy_version
.venv/bin/python scripts/verify_deploy_manifest.py --check /opt/lightfee-v2
systemctl is-active lightfee-sidecar.service lightfee-live.service
systemctl show lightfee-sidecar.service -p ActiveState -p SubState -p NRestarts --no-pager
systemctl show lightfee-live.service -p ActiveState -p SubState -p NRestarts --no-pager
.venv/bin/python scripts/verify_production_services.py --json
.venv/bin/python scripts/diagnose_live.py --json --since-deploy
```

Expected acceptance:

- services active/running;
- `NRestarts=0`;
- verifier and diagnose agree;
- no local-flat/exchange-open-order false green;
- no seven-venue live positions unless intentionally managed by ledger;
- no non-reduce open orders ignored by runtime;
- no new entry scan while blocking ledger work exists.
