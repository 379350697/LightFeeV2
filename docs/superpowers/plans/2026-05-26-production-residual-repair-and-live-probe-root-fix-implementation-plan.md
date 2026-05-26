# Production Residual Repair and Live-Probe Root Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Root-fix the production `risk_only/fail_closed` state caused by stale historical residual/recovery work, while separating V1-parity fixes from exchange-documented admission rules and proving every P0 through independent harness/probe tests.

**Architecture:** Add an isolated incident replay layer first, then execute independent workstreams for residual terminality, recovered passive-close/duplicate-CID semantics, recovery probe evidence quality, exchange admission rules, and Local-L2 evidence gating. Runtime fixes must be driven by V1 parity when V2 drift is proven, or by official exchange documentation when V1 and V2 share the same external rule.

**Tech Stack:** Python 3.12, pytest, existing `scripts/validate_change.py` profiles, LightFeeV2 runtime/venue adapters, sanitized JSONL fixtures, read-only exchange probes gated by environment variables.

---

## Global Rules for Every Task

- Before editing any function/class/method, run GitNexus impact analysis on the exact target symbol and record the risk in the task notes.
- If impact is HIGH or CRITICAL, pause and report before editing.
- Do not manually edit production state as a root fix.
- Do not use real exchange order submission in tests or probes.
- Do not mark a bug root-fixed unless its failing incident fixture/harness fails before the implementation and passes after it.
- Keep read-only live probes out of the default test path.

## Task 1: Incident Fixture and Independent Test Profiles

**Files:**
- Create: `tests/live_harness/__init__.py`
- Create: `tests/live_harness/conftest.py`
- Create: `tests/live_harness/test_incident_fixture_contract.py`
- Create: `tests/probes/__init__.py`
- Create: `tests/probes/conftest.py`
- Create: `tests/fixtures/live_incidents/2026-05-26/README.md`
- Create: `tests/fixtures/live_incidents/2026-05-26/current_state.json`
- Create: `tests/fixtures/live_incidents/2026-05-26/events_sample.jsonl`
- Modify: `pyproject.toml`
- Modify: `scripts/validate_change.py`
- Modify: `tests/test_validate_change.py`

- [ ] **Step 1: Add pytest markers**

Modify `[tool.pytest.ini_options]` in `pyproject.toml` to include:

```toml
markers = [
    "live_harness: offline deterministic incident replay tests using fake adapters",
    "live_probe: read-only live exchange probes, skipped unless LIGHTFEE_RUN_LIVE_PROBES=1",
]
```

- [ ] **Step 2: Add live probe skip guard**

Create `tests/probes/conftest.py`:

```python
from __future__ import annotations

import os

import pytest


def pytest_collection_modifyitems(config, items):
    if os.environ.get("LIGHTFEE_RUN_LIVE_PROBES") == "1":
        return
    skip = pytest.mark.skip(reason="LIGHTFEE_RUN_LIVE_PROBES=1 required")
    for item in items:
        if "live_probe" in item.keywords:
            item.add_marker(skip)
```

- [ ] **Step 3: Add harness marker**

Create `tests/live_harness/conftest.py`:

```python
from __future__ import annotations

import pytest


pytestmark = pytest.mark.live_harness
```

- [ ] **Step 4: Add fixture contract smoke test**

Create `tests/live_harness/test_incident_fixture_contract.py`:

```python
from __future__ import annotations

import json
from pathlib import Path


FIXTURE_ROOT = Path("tests/fixtures/live_incidents/2026-05-26")


def test_20260526_current_state_fixture_contains_residual_blockers():
    data = json.loads((FIXTURE_ROOT / "current_state.json").read_text())

    assert data["lifecycle"] == "risk_only"
    assert data["risk_mode"] == "fail_closed"
    assert data["open_position_count"] == 0
    residuals = data["pending_residual_repairs"]
    assert {task["symbol"] for task in residuals} == {"LYNUSDT", "OPGUSDT"}
    assert all(
        task["last_error"] == "residual_repair_deadline_or_attempts_exhausted"
        for task in residuals
    )


def test_20260526_event_sample_has_required_incident_families():
    kinds = []
    symbols = set()
    for line in (FIXTURE_ROOT / "events_sample.jsonl").read_text().splitlines():
        event = json.loads(line)
        kinds.append(event["kind"])
        payload = event.get("payload", {})
        if payload.get("symbol"):
            symbols.add(payload["symbol"])

    assert "entry.cleanup_duplicate_client_order_reconcile_result" in kinds
    assert "exit.passive_close_recovery_probe_diagnostic" in kinds
    assert "runtime.position_drift_corrected" in kinds
    assert {"BIOUSDT", "BEATUSDT"} <= symbols
```

- [ ] **Step 5: Add validation profiles**

Modify `scripts/validate_change.py` `PROFILES`:

```python
    "live-harness": (
        *BASE_STEPS,
        pytest("tests/live_harness", timeout_s=300),
    ),
    "live-probe": (
        pytest("tests/probes", timeout_s=300),
    ),
```

Do not include `live-probe` in `full`.

- [ ] **Step 6: Update validation runner tests**

Add to `tests/test_validate_change.py`:

```python
def test_validate_change_live_harness_profile_is_independent():
    result = subprocess.run(
        [sys.executable, SCRIPT, "--profile", "live-harness", "--dry-run"],
        text=True,
        capture_output=True,
        check=True,
    )

    assert "tests/live_harness" in result.stdout
    assert "tests/probes" not in result.stdout


def test_validate_change_live_probe_profile_is_explicit_only():
    result = subprocess.run(
        [sys.executable, SCRIPT, "--profile", "full", "--dry-run"],
        text=True,
        capture_output=True,
        check=True,
    )

    assert "tests/probes" not in result.stdout
```

- [ ] **Step 7: Populate initial sanitized fixtures**

Write `current_state.json` from the known current blocker state:

```json
{
  "run_id": "lightfee-1779803978233-1531592",
  "lifecycle": "risk_only",
  "risk_mode": "fail_closed",
  "open_position_count": 0,
  "pending_entry_count": 0,
  "pending_close_count": 0,
  "pending_passive_close_count": 0,
  "pending_residual_repair_count": 2,
  "pending_residual_repairs": [
    {
      "position_id": "entry-1779569524920-LYNUSDT",
      "pair_id": "lynusdt:aster->bybit",
      "symbol": "LYNUSDT",
      "origin": "close_residual",
      "repair_venue": "aster",
      "repair_side": "sell",
      "repair_quantity": 532.0,
      "local_entry_paused": true,
      "last_error": "residual_repair_deadline_or_attempts_exhausted"
    },
    {
      "position_id": "entry-1779594732734-OPGUSDT",
      "pair_id": "opgusdt:binance->okx",
      "symbol": "OPGUSDT",
      "origin": "entry_open",
      "repair_venue": "okx",
      "repair_side": "buy",
      "repair_quantity": 9.0,
      "local_entry_paused": true,
      "last_error": "residual_repair_deadline_or_attempts_exhausted"
    }
  ]
}
```

For `events_sample.jsonl`, include sanitized event records for:

- one BIOUSDT Bybit duplicate CID reconcile result;
- one BEATUSDT passive-close diagnostic;
- one BEATUSDT `runtime.position_drift_corrected`;
- one recovery probe error with non-secret OKX classification sample.

- [ ] **Step 8: Run profile checks**

Run:

```bash
python3 scripts/validate_change.py --profile live-harness --dry-run
python3 scripts/validate_change.py --profile full --dry-run
python3 -m pytest -q tests/test_validate_change.py tests/live_harness/test_incident_fixture_contract.py
```

Expected:

- `live-harness` lists `tests/live_harness`.
- `full` does not list `tests/probes`.
- pytest passes.

- [ ] **Step 9: Commit Task 1**

```bash
git add pyproject.toml scripts/validate_change.py tests/test_validate_change.py tests/live_harness tests/probes tests/fixtures/live_incidents/2026-05-26
git commit -m "test: add independent live incident harness profiles"
```

## Task 2: P0 Residual Repair Terminality for LYN/OPG

**Files:**
- Modify: `lightfee/engine/runtime.py`
- Test: `tests/live_harness/test_residual_repair_incident_replay.py`
- Test: `tests/test_live_entry_hedge_root_fix.py`
- Docs: `docs/bugs/daily/2026-05-26.md`

- [ ] **Step 1: Run impact analysis before edits**

Run GitNexus impact:

```text
impact(target="_recover_residual_repairs", direction="upstream", repo="LightFeeV2")
impact(target="_post_tick_housekeeping", direction="upstream", repo="LightFeeV2")
```

Proceed only if no HIGH/CRITICAL risk is returned, or after reporting the risk.

- [ ] **Step 2: Write failing live-flat exhausted residual test**

Create `tests/live_harness/test_residual_repair_incident_replay.py` with a fake adapter where Aster/Bybit or Binance/OKX positions and open orders are flat. The expected behavior is:

```python
assert runtime.state.pending_residual_repairs == []
assert "execution.residual_repair_completed" in kinds
assert completed_payload["result"] == "already_flat"
assert runtime.state.local_entry_pauses == {}
```

Run:

```bash
python3 -m pytest -q tests/live_harness/test_residual_repair_incident_replay.py::test_exhausted_residual_repair_already_flat_clears_lyn_opg
```

Expected before implementation: FAIL because exhausted tasks remain paused.

- [ ] **Step 3: Write failing live-nonzero safety test**

Add a test where the repair venue still reports nonzero live quantity. Expected behavior:

```python
assert len(runtime.state.pending_residual_repairs) == 1
assert runtime.state.pending_residual_repairs[0]["last_error"]
assert "execution.residual_repair_paused" in kinds
assert "execution.residual_repair_completed" not in kinds
```

Run the test and confirm it fails or proves existing behavior explicitly.

- [ ] **Step 4: Implement V1 terminality ordering**

In `LiveRuntime._recover_residual_repairs`:

- before deadline/attempt exhaustion becomes terminal pause, fetch live position and open order truth;
- if both venues are flat and no open orders exist, remove the task and emit `execution.residual_repair_completed(result=already_flat)`;
- if quantity is below venue contract/min quantity, terminalize only with symbol rules / instrument metadata evidence;
- if live truth fetch fails, keep fail-closed with structured non-empty reason;
- keep the existing safety behavior for live nonzero residuals.

- [ ] **Step 5: Run focused residual tests**

```bash
python3 -m pytest -q tests/live_harness/test_residual_repair_incident_replay.py tests/test_live_entry_hedge_root_fix.py -k "residual or LYNUSDT or OPGUSDT"
python3 scripts/validate_change.py --profile venue-okx --keep-going
```

Expected: all pass.

- [ ] **Step 6: Update bug docs**

Update `docs/bugs/daily/2026-05-26.md`:

- mark LYN/OPG as P0 old residual terminality regression;
- record V1 parity rule;
- record harness command and result;
- do not mark cloud verified until production current state confirms stale tasks are gone or intentionally retained with live nonzero evidence.

- [ ] **Step 7: Commit Task 2**

```bash
git add lightfee/engine/runtime.py tests/live_harness/test_residual_repair_incident_replay.py tests/test_live_entry_hedge_root_fix.py docs/bugs/daily/2026-05-26.md
git commit -m "fix: terminalize already-flat residual repairs with live proof"
```

## Task 3: P0 BEATUSDT/BIOUSDT Incident Replay Acceptance

**Files:**
- Modify if failing: `lightfee/engine/passive_close.py`
- Modify if failing: `lightfee/engine/runtime.py`
- Modify if failing: `lightfee/engine/bybit_duplicate_reconcile.py`
- Test: `tests/live_harness/test_recovered_close_and_duplicate_incidents.py`
- Test: `tests/test_passive_close.py`
- Test: `tests/test_live_entry_hedge_root_fix.py`

- [ ] **Step 1: Run impact analysis before edits**

```text
impact(target="_clear_if_live_flat", direction="upstream", repo="LightFeeV2")
impact(target="_cleanup_failed_leg_exposure", direction="upstream", repo="LightFeeV2")
impact(target="reconcile_bybit_duplicate_client_order", direction="upstream", repo="LightFeeV2")
```

- [ ] **Step 2: Add BEATUSDT recovered-flat harness**

The harness must start with recovered local open/pending state and fake adapters returning flat on both legs. Assert:

```python
assert no_order_submit_calls
assert "recovery.flat" in kinds
assert "runtime.position_drift_corrected" in kinds
assert runtime.state.open_positions == {}
assert runtime.state.pending_passive_closes == []
```

Run:

```bash
python3 -m pytest -q tests/live_harness/test_recovered_close_and_duplicate_incidents.py::test_beatusdt_recovered_state_clears_before_close_submission
```

Expected before implementation: FAIL if any order path runs before live-flat proof.

- [ ] **Step 3: Add BIOUSDT duplicate CID live-nonzero harness**

The harness must simulate:

- Bybit `110072 OrderLinkedID is duplicate`;
- original cid has historical full fill;
- live position remains nonzero.

Assert:

```python
assert "entry.cleanup_duplicate_client_order_reconcile_result" in kinds
assert payload["classification"] in {"full", "partial"}
assert "recovery.live_mismatch_flattened" not in kinds
assert retry_or_fail_closed_with_live_nonzero_evidence
```

- [ ] **Step 4: Implement only if harness fails**

If BEAT fails, enforce live-flat probe before close chunk creation in recovered passive close paths.

If BIO fails, make duplicate reconcile result subordinate to live position truth in cleanup paths.

- [ ] **Step 5: Run focused close/duplicate validation**

```bash
python3 -m pytest -q tests/live_harness/test_recovered_close_and_duplicate_incidents.py
python3 scripts/validate_change.py --profile close --keep-going
python3 scripts/validate_change.py --profile venue-bybit --keep-going
```

- [ ] **Step 6: Commit Task 3**

```bash
git add lightfee/engine/passive_close.py lightfee/engine/runtime.py lightfee/engine/bybit_duplicate_reconcile.py tests/live_harness/test_recovered_close_and_duplicate_incidents.py tests/test_passive_close.py tests/test_live_entry_hedge_root_fix.py
git commit -m "fix: prove recovered flat and duplicate cid semantics by incident replay"
```

## Task 4: P0 Recovery Probe Evidence Quality

**Files:**
- Modify: `lightfee/engine/runtime.py`
- Modify: `lightfee/venues/transport.py`
- Test: `tests/live_harness/test_recovery_probe_evidence_incidents.py`
- Test: `tests/test_live_startup_preflight.py`
- Test: `tests/test_venues_transport.py`
- Probe: `tests/probes/test_readonly_position_probe_evidence.py`

- [ ] **Step 1: Run impact analysis before edits**

```text
impact(target="_fetch_startup_live_position_snapshots", direction="upstream", repo="LightFeeV2")
impact(target="_position_probe_exception_payload", direction="upstream", repo="LightFeeV2")
impact(target="_ensure_okx_swap_instrument_metadata_loaded", direction="upstream", repo="LightFeeV2")
```

- [ ] **Step 2: Write failing probe evidence tests**

Add tests that assert:

```python
assert payload["error"] != ""
assert payload["exception_class"]
assert payload["venue"]
assert payload["symbol"]
assert payload["classification"] in {"timeout", "rate_limited", "instrument_missing", "metadata_missing", "unsupported_symbol"}
```

For unsupported symbols:

```python
assert "recovery.live_position_probe_unsupported_symbols" in kinds
assert "recovery.live_position_probe_error" not in unsupported_symbol_error_kinds
```

- [ ] **Step 3: Implement classification**

Make OKX timeout, HTTP 429 / code `50011`, missing `ctVal`, and unsupported symbols produce structured non-empty payloads. Preserve fail-closed behavior when exchange truth cannot be trusted.

- [ ] **Step 4: Add read-only live probe**

Create `tests/probes/test_readonly_position_probe_evidence.py` with `pytest.mark.live_probe`. It may call only read-only position/open-order/instrument endpoints and must assert payload shape, not account balances.

- [ ] **Step 5: Run focused validation**

```bash
python3 -m pytest -q tests/live_harness/test_recovery_probe_evidence_incidents.py tests/test_live_startup_preflight.py -k "probe or unsupported or metadata"
python3 -m pytest -q tests/test_venues_transport.py -k "okx and metadata"
LIGHTFEE_RUN_LIVE_PROBES=1 python3 -m pytest -q tests/probes/test_readonly_position_probe_evidence.py
```

If credentials are unavailable locally, record this and run the probe on cloud with production env in read-only mode before cloud verification.

- [ ] **Step 6: Commit Task 4**

```bash
git add lightfee/engine/runtime.py lightfee/venues/transport.py tests/live_harness/test_recovery_probe_evidence_incidents.py tests/probes/test_readonly_position_probe_evidence.py tests/test_live_startup_preflight.py tests/test_venues_transport.py
git commit -m "fix: structure recovery probe evidence and isolate live probes"
```

## Task 5: P1 Exchange Admission Blocks

**Files:**
- Modify: `lightfee/engine/runtime.py`
- Modify: `lightfee/venues/transport.py`
- Test: `tests/live_harness/test_exchange_admission_incidents.py`
- Test: `tests/test_live_startup_preflight.py`
- Test: `tests/test_venues_transport.py`
- Docs: `docs/bugs/daily/2026-05-26.md`

- [ ] **Step 1: Run impact analysis before edits**

```text
impact(target="_entry_admission_reject_reason", direction="upstream", repo="LightFeeV2")
impact(target="_record_symbol_admission_block", direction="upstream", repo="LightFeeV2")
impact(target="submit_passive_order", direction="upstream", repo="LightFeeV2")
```

- [ ] **Step 2: Add fixture-driven admission tests**

Cover:

- Bybit `110007` -> `insufficient_balance_admission_blocked`
- Bybit `110126` -> `bybit_trading_terms_required`, with `evidence_gap=true` until direct official doc evidence is captured
- Binance `-2019` -> `insufficient_margin_admission_blocked`
- Binance `-5022` -> `post_only_would_take`, cooldown not outage
- Aster `-2027` -> `leverage_admission_blocked`
- Aster `-5018` -> `max_notional_admission_blocked`, with `evidence_gap=true` unless official source is linked

- [ ] **Step 3: Implement admission payloads**

Every admission block payload must include:

```python
{
    "venue": venue.value,
    "symbol": symbol,
    "reason": reason,
    "blocked_until_ms": until_ms,
    "ttl_ms": self._SYMBOL_ADMISSION_BLOCK_TTL_MS,
    "raw_error": raw_error[:500],
    "official_doc_url": doc_url_or_empty,
    "evidence_gap": evidence_gap,
}
```

- [ ] **Step 4: Run focused validation**

```bash
python3 -m pytest -q tests/live_harness/test_exchange_admission_incidents.py
python3 -m pytest -q tests/test_live_startup_preflight.py -k "admission or trading_terms or leverage"
python3 -m pytest -q tests/test_venues_transport.py -k "5022 or 5018 or margin or balance"
```

- [ ] **Step 5: Commit Task 5**

```bash
git add lightfee/engine/runtime.py lightfee/venues/transport.py tests/live_harness/test_exchange_admission_incidents.py tests/test_live_startup_preflight.py tests/test_venues_transport.py docs/bugs/daily/2026-05-26.md
git commit -m "fix: classify exchange admission rejects with doc evidence"
```

## Task 6: P1 Local-L2 and Snapshot Evidence Gate

**Files:**
- Modify only if fixture proves drift: `lightfee/marketdata/local_l2_data_plane.py`
- Modify only if fixture proves drift: `lightfee/marketdata/local_l2_ws.py`
- Test: `tests/live_harness/test_local_l2_incident_replay.py`
- Test: `tests/test_local_l2_replay_harness.py`
- Probe: `tests/probes/test_readonly_local_l2_probe.py`

- [ ] **Step 1: Run impact analysis before any data-plane edit**

```text
impact(target="LocalL2DataPlane", direction="upstream", repo="LightFeeV2")
impact(target="LocalL2WsClient", direction="upstream", repo="LightFeeV2")
```

- [ ] **Step 2: Add replay harness before changing code**

Build fixtures for representative `runtime.local_l2_sequence_gap_rebuild`, `runtime.local_l2_snapshot_error`, and `runtime.snapshot_fallback_last_good` samples. The test must classify each sample as:

- V1 parity drift;
- official-doc exchange sequence/reset behavior;
- expected real gap;
- insufficient evidence.

- [ ] **Step 3: Add read-only public probe**

`tests/probes/test_readonly_local_l2_probe.py` may subscribe/fetch public orderbook data only. It must not depend on private credentials.

- [ ] **Step 4: Implement only proven drifts**

If replay proves drift, implement venue-specific V1/doc behavior. Do not expand freshness thresholds as a root fix.

- [ ] **Step 5: Run focused validation**

```bash
python3 -m pytest -q tests/live_harness/test_local_l2_incident_replay.py tests/test_local_l2_replay_harness.py
python3 scripts/validate_change.py --profile local-l2 --keep-going
LIGHTFEE_RUN_LIVE_PROBES=1 python3 -m pytest -q tests/probes/test_readonly_local_l2_probe.py
```

- [ ] **Step 6: Commit Task 6**

```bash
git add lightfee/marketdata/local_l2_data_plane.py lightfee/marketdata/local_l2_ws.py tests/live_harness/test_local_l2_incident_replay.py tests/probes/test_readonly_local_l2_probe.py tests/test_local_l2_replay_harness.py
git commit -m "fix: gate local l2 changes behind incident replay evidence"
```

## Task 7: Production Acceptance and Bug Ledger Closure

**Files:**
- Modify: `docs/bugs/daily/2026-05-26.md`
- Modify: `docs/bugs/BUG_INDEX.md`
- Optional: `docs/ops/production-health-runbook.md`

- [ ] **Step 1: Run combined local validation**

```bash
python3 scripts/validate_change.py --profile live-harness --profile close --profile venue-okx --profile venue-bybit --keep-going
python3 scripts/validate_change.py --profile smoke
```

Expected: all selected steps pass.

- [ ] **Step 2: Run read-only cloud predeploy evidence**

On cloud, before restart/deploy:

```bash
cd /opt/lightfee-v2
python3 scripts/verify_production_services.py --json
python3 scripts/diagnose_live.py --json --symbol LYNUSDT
python3 scripts/diagnose_live.py --json --symbol OPGUSDT
python3 scripts/diagnose_live.py --json --symbol BEATUSDT
python3 scripts/diagnose_live.py --json --symbol BIOUSDT
```

Record whether exchange truth is flat and whether any open orders exist.

- [ ] **Step 3: Deploy only after local and read-only evidence pass**

Use the existing deploy process. Do not manually edit `runtime/live-state*.json`.

- [ ] **Step 4: Run postdeploy acceptance**

On cloud after restart:

```bash
cd /opt/lightfee-v2
python3 scripts/verify_production_services.py --json
python3 scripts/diagnose_live.py --json --symbol LYNUSDT
python3 scripts/diagnose_live.py --json --symbol OPGUSDT
python3 scripts/diagnose_live.py --json --symbol BEATUSDT
python3 scripts/diagnose_live.py --json --symbol BIOUSDT
```

Acceptance:

- no stale `pending_residual_repairs` for flat/dust-proven tasks;
- no local open state when exchange truth is flat;
- no false green if exchange truth is nonzero;
- `risk_only/fail_closed` remains only when live nonzero or untrusted exchange evidence justifies it.

- [ ] **Step 5: Update bug ledger**

For each cluster, list:

- old vs new;
- V1 parity fix or exchange-doc fix;
- fixture/harness command;
- read-only probe command;
- production postdeploy result;
- remaining evidence gap, if any.

- [ ] **Step 6: Final GitNexus detect changes**

Run:

```text
detect_changes(scope="all", repo="LightFeeV2")
```

Report changed symbols and affected flows before final commit/push/deploy handoff.

- [ ] **Step 7: Commit docs and closure evidence**

```bash
git add docs/bugs/daily/2026-05-26.md docs/bugs/BUG_INDEX.md docs/ops/production-health-runbook.md
git commit -m "docs: record residual repair root-fix acceptance evidence"
```
