# Production Sidecar/Live Health Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make production sidecar/live health self-verifying, prevent example-config sidecar regressions, persist DNS hardening, and safely clear stale fail-closed state only when recovery is clean.

**Architecture:** Keep production checks in a pure ops library, expose them through one CLI, and keep live recovery semantics inside recovery/lifecycle code. Deployment files are versioned templates; runtime code never stores secrets.

**Tech Stack:** Python 3.11, pytest, systemd unit text, NetworkManager resolver config, LightFeeV2 `LiveRuntime`, GitNexus MCP.

---

## Required Context

- Spec: `docs/superpowers/specs/2026-05-15-production-sidecar-live-health-hardening-design.md`
- Bug ledger: `docs/bugs/daily/2026-05-15.md`
- Existing helpers:
  - `scripts/check_process_singleton.py`
  - `scripts/verify_deploy_manifest.py`
  - `lightfee/apps/sidecar.py`
  - `lightfee/engine/runtime.py`
  - `lightfee/engine/recovery.py`
  - `lightfee/engine/state.py`
  - `lightfee/engine/loop_control.py`

Before editing any symbol, follow `AGENTS.md`: run GitNexus impact for the symbol, report risk, then write tests first.

## File Structure

| Path | Responsibility |
|---|---|
| `lightfee/ops/production_health.py` | Pure production health analyzers for systemd unit text, resolver config, sidecar snapshot, and current-state JSON |
| `scripts/verify_production_services.py` | CLI wrapper around `production_health.py`; reads files/commands and exits nonzero on critical failures |
| `tests/ops/test_production_health.py` | Unit tests for all production health analyzers and CLI JSON shape |
| `deploy/systemd/lightfee-live.service` | Versioned V2 live systemd template |
| `deploy/systemd/lightfee-sidecar-rust-v1.service` | Versioned current production sidecar template using Rust V1 binary + live config |
| `deploy/network/NetworkManager-lightfee-dns.conf` | Persistent DNS template |
| `docs/ops/production-health-runbook.md` | Operator runbook for health verification and remediation |
| `lightfee/engine/state.py` | Add explicit `last_scan` field to `EngineState` |
| `lightfee/engine/runtime.py` | Populate `last_scan`; call safe stale fail-closed cleanup during clean startup |
| `lightfee/engine/recovery.py` | Add safe stale fail-closed cleanup helper |
| `tests/test_live_full_closure.py` | Split fail-closed startup tests into stale-clean, operator-requested, and pending-work cases |
| `tests/ops/test_current_state_and_metrics_export.py` | Assert current-state exports populated `last_scan` |
| `scripts/verify_deploy_manifest.py` | Add production health files to critical deploy checks |

### Task 1: Production Health Analyzer

**Files:**
- Create: `lightfee/ops/production_health.py`
- Create: `tests/ops/test_production_health.py`

- [ ] **Step 1: Run GitNexus context**

Run:

```text
gitnexus_context({name: "SidecarSnapshot", repo: "LightFeeV2"})
gitnexus_context({name: "EngineState", repo: "LightFeeV2"})
```

Expected: context identifies snapshot/current-state consumers. If GitNexus reports stale index, run `npx gitnexus analyze` first.

- [ ] **Step 2: Write failing analyzer tests**

Add to `tests/ops/test_production_health.py`:

```python
from lightfee.ops.production_health import (
    analyze_current_state,
    analyze_resolver_config,
    analyze_sidecar_snapshot,
    analyze_systemd_unit,
    summarize_reports,
)


def test_sidecar_unit_rejects_missing_config():
    text = """
[Service]
WorkingDirectory=/root/projects/LightFee
ExecStart=/root/projects/LightFee/target/release/opportunity_input_sidecar
"""
    report = analyze_systemd_unit("lightfee-sidecar.service", text)
    assert not report.ok
    assert "missing_explicit_config" in report.fingerprints


def test_sidecar_unit_rejects_example_config():
    text = """
[Service]
EnvironmentFile=/etc/lightfee/lightfee.env
ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.sidecar --config config/example.toml
"""
    report = analyze_systemd_unit("lightfee-sidecar.service", text)
    assert not report.ok
    assert "example_config_in_production" in report.fingerprints


def test_sidecar_unit_accepts_current_rust_v1_live_contract():
    text = """
[Service]
WorkingDirectory=/root/projects/LightFee
EnvironmentFile=/etc/lightfee/lightfee.env
ExecStart=/root/projects/LightFee/target/release/opportunity_input_sidecar --config /root/projects/LightFee/config/live.auto.toml
Restart=always
"""
    report = analyze_systemd_unit("lightfee-sidecar.service", text)
    assert report.ok


def test_snapshot_rejects_fixture_four_venue_shape():
    snapshot = {
        "market_observed_at_ms": 1710000075000,
        "quotes": {
            "binance:BTCUSDT": {"venue": "binance", "symbol": "BTCUSDT", "bid": 100.0, "ask": 100.0},
            "okx:BTCUSDT": {"venue": "okx", "symbol": "BTCUSDT", "bid": 100.0, "ask": 100.0},
            "bybit:BTCUSDT": {"venue": "bybit", "symbol": "BTCUSDT", "bid": 100.0, "ask": 100.0},
            "hyperliquid:BTCUSDT": {"venue": "hyperliquid", "symbol": "BTCUSDT", "bid": 100.0, "ask": 100.0},
        },
        "degraded_venues": [],
    }
    report = analyze_sidecar_snapshot(snapshot, now_ms=1778787000000, max_age_ms=10_000)
    assert not report.ok
    assert "fixture_timestamp" in report.fingerprints
    assert "quote_venue_count_lt_7" in report.fingerprints


def test_snapshot_accepts_fresh_seven_venue_shape():
    venues = ["aster", "binance", "bitget", "bybit", "gate", "hyperliquid", "okx"]
    snapshot = {
        "market_observed_at_ms": 1778786998000,
        "quotes": {
            f"{venue}:BTCUSDT": {"venue": venue, "symbol": "BTCUSDT", "bid": 65000.0, "ask": 65001.0}
            for venue in venues
        },
        "degraded_venues": [],
    }
    report = analyze_sidecar_snapshot(snapshot, now_ms=1778787000000, max_age_ms=10_000)
    assert report.ok


def test_current_state_flags_stale_fail_closed_clean_state():
    state = {
        "lifecycle": "running",
        "risk_mode": "fail_closed",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "recovery_blocked_reason": None,
    }
    report = analyze_current_state(state, now_ms=1778787000000, max_tick_age_ms=10_000)
    assert not report.ok
    assert "stale_fail_closed_clean_state" in report.fingerprints


def test_resolver_requires_okx_capable_priority():
    text = "nameserver 42.116.255.180\nnameserver 8.8.8.8\nnameserver 1.1.1.1\n"
    report = analyze_resolver_config(text)
    assert not report.ok
    assert "unverified_resolver_first" in report.fingerprints


def test_summary_is_failed_when_any_report_critical():
    bad = analyze_systemd_unit("lightfee-sidecar.service", "[Service]\nExecStart=lightfee-sidecar\n")
    summary = summarize_reports([bad])
    assert not summary.ok
    assert summary.critical_count == 1
```

Run:

```bash
rtk pytest tests/ops/test_production_health.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'lightfee.ops.production_health'`.

- [ ] **Step 3: Implement analyzer**

Create `lightfee/ops/production_health.py`:

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable


EXPECTED_VENUES = {"aster", "binance", "bitget", "bybit", "gate", "hyperliquid", "okx"}
KNOWN_GOOD_RESOLVERS = {"1.1.1.1", "1.0.0.1", "8.8.8.8", "8.8.4.4", "9.9.9.9"}
FIXTURE_MARKET_OBSERVED_AT_MS = 1710000075000


@dataclass(frozen=True)
class HealthReport:
    name: str
    ok: bool
    severity: str = "info"
    fingerprints: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class HealthSummary:
    ok: bool
    critical_count: int
    warning_count: int
    reports: list[HealthReport]


def _execstart_lines(unit_text: str) -> list[str]:
    return [
        line.split("=", 1)[1].strip()
        for line in unit_text.splitlines()
        if line.strip().startswith("ExecStart=")
    ]


def _has_environment_file(unit_text: str) -> bool:
    return any(
        line.strip().startswith("EnvironmentFile=")
        for line in unit_text.splitlines()
    )


def analyze_systemd_unit(name: str, text: str) -> HealthReport:
    fingerprints: list[str] = []
    details: dict[str, Any] = {"service": name}
    execs = _execstart_lines(text)
    details["execstart"] = execs
    is_sidecar = "sidecar" in name
    is_live = name.endswith("lightfee-live.service") or "live" in name

    if not execs:
        fingerprints.append("missing_execstart")
    command = " ".join(execs)
    if (is_sidecar or is_live) and "--config" not in command:
        fingerprints.append("missing_explicit_config")
    if "config/example.toml" in command or command.endswith("example.toml"):
        fingerprints.append("example_config_in_production")
    if is_sidecar and not _has_environment_file(text):
        fingerprints.append("missing_environment_file")
    if is_sidecar and "opportunity_input_sidecar" in command and "live.auto.toml" not in command:
        fingerprints.append("rust_sidecar_without_live_auto_config")

    return HealthReport(
        name=f"systemd:{name}",
        ok=not fingerprints,
        severity="critical" if fingerprints else "info",
        fingerprints=fingerprints,
        details=details,
    )


def _quote_venues(snapshot: dict[str, Any]) -> set[str]:
    venues: set[str] = set()
    quotes = snapshot.get("quotes", {})
    if isinstance(quotes, dict):
        for key, raw in quotes.items():
            if isinstance(raw, dict) and raw.get("venue"):
                venues.add(str(raw["venue"]).lower())
            elif isinstance(key, str) and ":" in key:
                venues.add(key.split(":", 1)[0].lower())
    return venues


def _fixture_quote_count(snapshot: dict[str, Any]) -> int:
    count = 0
    quotes = snapshot.get("quotes", {})
    if isinstance(quotes, dict):
        for raw in quotes.values():
            if not isinstance(raw, dict):
                continue
            try:
                if float(raw.get("bid", 0)) == 100.0 and float(raw.get("ask", 0)) == 100.0:
                    count += 1
            except (TypeError, ValueError):
                continue
    return count


def analyze_sidecar_snapshot(
    snapshot: dict[str, Any],
    *,
    now_ms: int,
    max_age_ms: int,
) -> HealthReport:
    fingerprints: list[str] = []
    observed = int(snapshot.get("market_observed_at_ms") or snapshot.get("published_at_ms") or 0)
    age_ms = now_ms - observed if observed else None
    venues = _quote_venues(snapshot)
    missing = sorted(EXPECTED_VENUES - venues)
    fixture_quotes = _fixture_quote_count(snapshot)

    if observed == FIXTURE_MARKET_OBSERVED_AT_MS:
        fingerprints.append("fixture_timestamp")
    if age_ms is None or age_ms < 0 or age_ms > max_age_ms:
        fingerprints.append("snapshot_stale_or_missing_timestamp")
    if len(venues) < len(EXPECTED_VENUES):
        fingerprints.append("quote_venue_count_lt_7")
    if fixture_quotes >= 2:
        fingerprints.append("fixture_100_quotes")

    return HealthReport(
        name="sidecar_snapshot",
        ok=not fingerprints,
        severity="critical" if fingerprints else "info",
        fingerprints=fingerprints,
        details={
            "observed_at_ms": observed,
            "age_ms": age_ms,
            "quote_venues": sorted(venues),
            "missing_venues": missing,
            "fixture_quote_count": fixture_quotes,
            "degraded_venues": list(snapshot.get("degraded_venues", [])),
        },
    )


def analyze_current_state(
    state: dict[str, Any],
    *,
    now_ms: int,
    max_tick_age_ms: int,
) -> HealthReport:
    fingerprints: list[str] = []
    last_tick_ms = int(state.get("last_tick_ms") or 0)
    tick_age_ms = now_ms - last_tick_ms if last_tick_ms else None
    open_count = int(state.get("open_position_count") or 0)
    pending_entries = int(state.get("pending_entry_count") or 0)
    pending_closes = int(state.get("pending_close_count") or 0)
    clean = open_count == 0 and pending_entries == 0 and pending_closes == 0

    if state.get("lifecycle") != "running":
        fingerprints.append("live_lifecycle_not_running")
    if tick_age_ms is None or tick_age_ms < 0 or tick_age_ms > max_tick_age_ms:
        fingerprints.append("live_tick_stale")
    if state.get("risk_mode") == "fail_closed" and clean and not state.get("recovery_blocked_reason"):
        fingerprints.append("stale_fail_closed_clean_state")
    if state.get("last_scan") is None:
        fingerprints.append("last_scan_missing")

    severity = "critical" if any(fp != "last_scan_missing" for fp in fingerprints) else "warning"
    return HealthReport(
        name="current_state",
        ok=not fingerprints,
        severity=severity if fingerprints else "info",
        fingerprints=fingerprints,
        details={
            "tick_age_ms": tick_age_ms,
            "risk_mode": state.get("risk_mode"),
            "lifecycle": state.get("lifecycle"),
            "open_position_count": open_count,
            "pending_entry_count": pending_entries,
            "pending_close_count": pending_closes,
        },
    )


def analyze_resolver_config(text: str) -> HealthReport:
    nameservers: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("nameserver ") and len(line.split()) >= 2:
            nameservers.append(line.split()[1])
        elif line.startswith("servers="):
            nameservers.extend(
                item.strip()
                for item in line.split("=", 1)[1].split(",")
                if item.strip()
            )
    fingerprints: list[str] = []
    if not nameservers:
        fingerprints.append("resolver_missing_nameserver")
    elif nameservers[0] not in KNOWN_GOOD_RESOLVERS:
        fingerprints.append("unverified_resolver_first")
    return HealthReport(
        name="resolver_config",
        ok=not fingerprints,
        severity="critical" if fingerprints else "info",
        fingerprints=fingerprints,
        details={"nameservers": nameservers},
    )


def summarize_reports(reports: Iterable[HealthReport]) -> HealthSummary:
    items = list(reports)
    critical = sum(1 for r in items if not r.ok and r.severity == "critical")
    warning = sum(1 for r in items if not r.ok and r.severity == "warning")
    return HealthSummary(
        ok=critical == 0 and warning == 0,
        critical_count=critical,
        warning_count=warning,
        reports=items,
    )
```

- [ ] **Step 4: Verify analyzer tests**

Run:

```bash
rtk pytest tests/ops/test_production_health.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lightfee/ops/production_health.py tests/ops/test_production_health.py
git commit -m "ops: add production health analyzers"
```

### Task 2: Production Verification CLI

**Files:**
- Create: `scripts/verify_production_services.py`
- Modify: `tests/ops/test_production_health.py`

- [ ] **Step 1: Add CLI JSON tests**

Add to `tests/ops/test_production_health.py`:

```python
import json
import subprocess
import sys
from pathlib import Path


def test_verify_production_services_cli_json_success(tmp_path):
    unit_dir = tmp_path / "systemd"
    unit_dir.mkdir()
    (unit_dir / "lightfee-sidecar.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/root/projects/LightFee/target/release/opportunity_input_sidecar --config /root/projects/LightFee/config/live.auto.toml\n"
    )
    (unit_dir / "lightfee-live.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.live --config /opt/lightfee-v2/config/live.toml\n"
    )
    snapshot = tmp_path / "snapshot.json"
    venues = ["aster", "binance", "bitget", "bybit", "gate", "hyperliquid", "okx"]
    snapshot.write_text(json.dumps({
        "market_observed_at_ms": 1778786998000,
        "quotes": {f"{v}:BTCUSDT": {"venue": v, "symbol": "BTCUSDT", "bid": 65000, "ask": 65001} for v in venues},
        "degraded_venues": [],
    }))
    current = tmp_path / "current.json"
    current.write_text(json.dumps({
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "last_scan": {"candidate_count": 10, "tradeable_count": 2},
    }))
    resolv = tmp_path / "resolv.conf"
    resolv.write_text("nameserver 1.1.1.1\nnameserver 8.8.8.8\n")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/verify_production_services.py",
            "--unit-dir", str(unit_dir),
            "--snapshot", str(snapshot),
            "--current-state", str(current),
            "--resolv-conf", str(resolv),
            "--now-ms", "1778787000000",
            "--json",
        ],
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr + result.stdout
    payload = json.loads(result.stdout)
    assert payload["ok"] is True


def test_verify_production_services_cli_json_failure(tmp_path):
    unit_dir = tmp_path / "systemd"
    unit_dir.mkdir()
    (unit_dir / "lightfee-sidecar.service").write_text("[Service]\nExecStart=lightfee-sidecar\n")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/verify_production_services.py",
            "--unit-dir", str(unit_dir),
            "--now-ms", "1778787000000",
            "--json",
        ],
        text=True,
        capture_output=True,
    )
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["critical_count"] >= 1
```

Run:

```bash
rtk pytest tests/ops/test_production_health.py -q
```

Expected: FAIL because `scripts/verify_production_services.py` does not exist.

- [ ] **Step 2: Implement CLI**

Create `scripts/verify_production_services.py`:

```python
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict
from pathlib import Path

from lightfee.ops.production_health import (
    analyze_current_state,
    analyze_resolver_config,
    analyze_sidecar_snapshot,
    analyze_systemd_unit,
    summarize_reports,
)


def _read_json(path: str) -> dict:
    with open(path) as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"JSON root must be object: {path}")
    return data


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify LightFee production service health")
    parser.add_argument("--unit-dir", default="/etc/systemd/system")
    parser.add_argument("--snapshot", default="/opt/lightfee-v2/runtime/opportunity-input-snapshot.json")
    parser.add_argument("--current-state", default="/opt/lightfee-v2/runtime/live-state-current.json")
    parser.add_argument("--resolv-conf", default="/etc/resolv.conf")
    parser.add_argument("--now-ms", type=int, default=0)
    parser.add_argument("--snapshot-max-age-ms", type=int, default=10_000)
    parser.add_argument("--max-tick-age-ms", type=int, default=15_000)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    now_ms = args.now_ms or int(time.time() * 1000)
    reports = []
    unit_dir = Path(args.unit_dir)
    for name in ("lightfee-sidecar.service", "lightfee-live.service"):
        path = unit_dir / name
        if path.exists():
            reports.append(analyze_systemd_unit(name, path.read_text()))
        else:
            reports.append(analyze_systemd_unit(name, ""))

    if Path(args.snapshot).exists():
        reports.append(analyze_sidecar_snapshot(_read_json(args.snapshot), now_ms=now_ms, max_age_ms=args.snapshot_max_age_ms))
    if Path(args.current_state).exists():
        reports.append(analyze_current_state(_read_json(args.current_state), now_ms=now_ms, max_tick_age_ms=args.max_tick_age_ms))
    if Path(args.resolv_conf).exists():
        reports.append(analyze_resolver_config(Path(args.resolv_conf).read_text()))

    summary = summarize_reports(reports)
    payload = asdict(summary)
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"ok={summary.ok} critical={summary.critical_count} warning={summary.warning_count}")
        for report in summary.reports:
            status = "PASS" if report.ok else report.severity.upper()
            print(f"{status} {report.name}: {','.join(report.fingerprints) or 'ok'}")
    sys.exit(0 if summary.ok else 1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Verify CLI tests**

Run:

```bash
rtk pytest tests/ops/test_production_health.py -q
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add scripts/verify_production_services.py tests/ops/test_production_health.py
git commit -m "ops: add production service verification cli"
```

### Task 3: Safe Stale Fail-Closed Recovery

**Files:**
- Modify: `lightfee/engine/recovery.py`
- Modify: `lightfee/engine/runtime.py`
- Modify: `tests/test_live_full_closure.py`
- Modify: `tests/recovery/test_restart_recovery_semantics.py`

- [ ] **Step 1: Run GitNexus impact**

Run:

```text
gitnexus_impact({target: "recover_from_snapshot", direction: "upstream", repo: "LightFeeV2"})
gitnexus_impact({target: "LiveRuntime.start", direction: "upstream", repo: "LightFeeV2"})
```

Expected: report any HIGH/CRITICAL risk before editing.

- [ ] **Step 2: Write failing recovery tests**

Update existing fail-closed startup coverage:

```python
@pytest.mark.asyncio
async def test_stale_fail_closed_clean_state_is_cleared_on_startup(self):
    with tempfile.TemporaryDirectory() as td:
        config = make_test_config(td)
        snap = __import__("lightfee.persistence.snapshot_store", fromlist=["SnapshotStore"]).SnapshotStore(
            config.persistence.snapshot_path
        )
        snap.write({
            "lifecycle": "running",
            "risk_mode": "fail_closed",
            "open_positions": {},
            "pending_entries": {},
            "pending_closes": {},
            "pending_passive_closes": {},
            "global_risk_reason": None,
            "recovery_blocked_reason": None,
        })

        runtime = LiveRuntime(config)
        await runtime.start()

        assert runtime.state.lifecycle == EngineLifecycle.RUNNING
        assert runtime.state.risk_mode == GlobalRiskMode.RUNNING
        records = runtime.journal.read_all()
        assert any(r.get("kind") == "runtime.stale_fail_closed_cleared" for r in records)


@pytest.mark.asyncio
async def test_operator_requested_fail_closed_is_preserved_on_startup(self):
    with tempfile.TemporaryDirectory() as td:
        config = make_test_config(td)
        snap = __import__("lightfee.persistence.snapshot_store", fromlist=["SnapshotStore"]).SnapshotStore(
            config.persistence.snapshot_path
        )
        snap.write({
            "lifecycle": "risk_only",
            "risk_mode": "fail_closed",
            "operator": {"requested_mode": "fail_closed"},
        })

        runtime = LiveRuntime(config)
        await runtime.start()

        assert runtime.state.risk_mode == GlobalRiskMode.FAIL_CLOSED
```

Run:

```bash
rtk pytest tests/test_live_full_closure.py -k "fail_closed" -q
```

Expected: first new test FAILS because stale clean fail-closed remains sticky.

- [ ] **Step 3: Implement helper in recovery**

Add to `lightfee/engine/recovery.py` near `is_safe_to_resume`:

```python
def clear_stale_fail_closed_if_recovery_clean(state: EngineState, journal: Journal | None = None) -> bool:
    """Clear persisted fail_closed only when there is no recovery or operator block.

    This is deliberately narrower than RESUME_IF_SAFE. It handles stale persisted
    state from prior incidents after open/pending work is already gone.
    """
    if state.risk_mode != GlobalRiskMode.FAIL_CLOSED:
        return False
    if state.operator.requested_mode == GlobalRiskMode.FAIL_CLOSED:
        return False
    if needs_reconciliation(state):
        return False
    if state.recovery_blocked_reason:
        return False

    previous = state.risk_mode.value
    state.risk_mode = GlobalRiskMode.RUNNING
    state.lifecycle = EngineLifecycle.RUNNING
    state.global_risk_reason = None
    state.recovery_blocked_at_ms = 0
    if journal is not None:
        _try_emit_recovery(journal, "runtime.risk_mode_changed", {
            "from": previous,
            "to": state.risk_mode.value,
            "reason": "startup_clean_stale_fail_closed_cleared",
        })
        _try_emit_recovery(journal, "runtime.stale_fail_closed_cleared", {
            "reason": "startup_clean_no_recovery_work",
        })
    return True
```

- [ ] **Step 4: Call helper in clean startup**

In `lightfee/engine/runtime.py`, inside the `if recovery_class == "clean":` branch, call the helper after `set_lifecycle`:

```python
from lightfee.engine.recovery import clear_stale_fail_closed_if_recovery_clean

set_lifecycle(self.state, EngineLifecycle.RUNNING)
clear_stale_fail_closed_if_recovery_clean(self.state, self.journal)
```

- [ ] **Step 5: Verify targeted recovery tests**

Run:

```bash
rtk pytest tests/test_live_full_closure.py -k "fail_closed" -q
rtk pytest tests/recovery/test_restart_recovery_semantics.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add lightfee/engine/recovery.py lightfee/engine/runtime.py tests/test_live_full_closure.py tests/recovery/test_restart_recovery_semantics.py
git commit -m "fix: clear stale fail-closed after clean recovery"
```

### Task 4: Populate Current-State `last_scan`

**Files:**
- Modify: `lightfee/engine/state.py`
- Modify: `lightfee/engine/runtime.py`
- Modify: `tests/ops/test_current_state_and_metrics_export.py`
- Modify: `tests/test_live_full_closure.py`

- [ ] **Step 1: Run GitNexus impact**

Run:

```text
gitnexus_impact({target: "EngineState", direction: "upstream", relationTypes: ["IMPORTS", "HAS_PROPERTY", "ACCESSES"], repo: "LightFeeV2"})
gitnexus_impact({target: "LiveRuntime.tick", direction: "upstream", repo: "LightFeeV2"})
```

Expected: report risk before editing.

- [ ] **Step 2: Write failing tests**

Add to current-state export tests:

```python
def test_export_includes_populated_last_scan_when_present(self):
    state = EngineState(lifecycle=EngineLifecycle.RUNNING, risk_mode=GlobalRiskMode.RUNNING)
    state.last_scan = {
        "ts_ms": 1778787000000,
        "snapshot_freshness": "fresh",
        "candidate_count": 12,
        "tradeable_count": 3,
        "degraded_venues": [],
        "no_entry_reason": None,
    }

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        path = f.name
    try:
        _export_current_state_snapshot(state, path)
        with open(path) as f:
            data = json.load(f)
        assert data["last_scan"]["candidate_count"] == 12
        assert data["last_scan"]["tradeable_count"] == 3
    finally:
        os.unlink(path)
```

Add a live tick test that writes a sidecar snapshot and asserts `runtime.state.last_scan` is non-null after `await runtime.tick()`.

Run:

```bash
rtk pytest tests/ops/test_current_state_and_metrics_export.py -k "last_scan" -q
rtk pytest tests/test_live_full_closure.py -k "last_scan or candidates_tradeable" -q
```

Expected: live tick test FAILS because `last_scan` is not populated.

- [ ] **Step 3: Add explicit field**

In `lightfee/engine/state.py`, add to `EngineState`:

```python
last_scan: dict | None = None
```

And in `to_dict()` add:

```python
"last_scan": self.last_scan,
```

- [ ] **Step 4: Populate in `LiveRuntime.tick`**

In `lightfee/engine/runtime.py`, after snapshot freshness is resolved and before early returns, set:

```python
self.state.last_scan = {
    "ts_ms": now_ms,
    "snapshot_freshness": freshness.value if hasattr(freshness, "value") else str(freshness),
    "candidate_count": len(snapshot.candidates) if snapshot is not None else 0,
    "tradeable_count": 0,
    "degraded_venues": list(getattr(snapshot, "degraded_venues", [])) if snapshot is not None else [],
    "no_entry_reason": None,
}
```

After `tradeable = discover_tradeable_candidates(...)`, update:

```python
self.state.last_scan["tradeable_count"] = len(tradeable)
if not tradeable:
    self.state.last_scan["no_entry_reason"] = "no_tradeable_candidates"
```

When returning for warmup or risk mode, set a specific `no_entry_reason`:

```python
self.state.last_scan["no_entry_reason"] = "live_scan_recovery_warmup"
```

- [ ] **Step 5: Verify tests**

Run:

```bash
rtk pytest tests/ops/test_current_state_and_metrics_export.py -k "last_scan" -q
rtk pytest tests/test_live_full_closure.py -k "last_scan or candidates_tradeable" -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add lightfee/engine/state.py lightfee/engine/runtime.py tests/ops/test_current_state_and_metrics_export.py tests/test_live_full_closure.py
git commit -m "ops: export live scan health in current state"
```

### Task 5: Version Production Deployment Assets

**Files:**
- Create: `deploy/systemd/lightfee-live.service`
- Create: `deploy/systemd/lightfee-sidecar-rust-v1.service`
- Create: `deploy/network/NetworkManager-lightfee-dns.conf`
- Create: `docs/ops/production-health-runbook.md`
- Modify: `scripts/verify_deploy_manifest.py`
- Modify: `tests/ops/test_production_health.py`

- [ ] **Step 1: Add deployment asset tests**

Add:

```python
from pathlib import Path


def test_deploy_systemd_templates_pass_contract():
    sidecar = Path("deploy/systemd/lightfee-sidecar-rust-v1.service").read_text()
    live = Path("deploy/systemd/lightfee-live.service").read_text()
    assert analyze_systemd_unit("lightfee-sidecar.service", sidecar).ok
    assert analyze_systemd_unit("lightfee-live.service", live).ok


def test_deploy_dns_template_prefers_verified_resolver():
    text = Path("deploy/network/NetworkManager-lightfee-dns.conf").read_text()
    assert analyze_resolver_config(text).ok
```

Run:

```bash
rtk pytest tests/ops/test_production_health.py -q
```

Expected: FAIL because deploy files do not exist.

- [ ] **Step 2: Add systemd templates**

Create `deploy/systemd/lightfee-live.service`:

```ini
[Unit]
Description=LightFee V2 live trading runtime
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/lightfee-v2
EnvironmentFile=/etc/lightfee/lightfee.env
Environment=LIGHTFEE_PRODUCTION_GUARD=1
ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.live --config /opt/lightfee-v2/config/live.toml
Restart=always
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=45

[Install]
WantedBy=multi-user.target
```

Create `deploy/systemd/lightfee-sidecar-rust-v1.service`:

```ini
[Unit]
Description=LightFee opportunity input sidecar (Rust V1 bridge for V2 live)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/root/projects/LightFee
EnvironmentFile=/etc/lightfee/lightfee.env
Environment=LIGHTFEE_PRODUCTION_GUARD=1
ExecStart=/root/projects/LightFee/target/release/opportunity_input_sidecar --config /root/projects/LightFee/config/live.auto.toml
Restart=always
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=45

[Install]
WantedBy=multi-user.target
```

Create `deploy/network/NetworkManager-lightfee-dns.conf`:

```ini
[main]
dns=default

[global-dns-domain-*]
servers=1.1.1.1,8.8.8.8,42.116.255.180
```

- [ ] **Step 3: Add runbook**

Create `docs/ops/production-health-runbook.md`:

````md
# Production Health Runbook

## Verify

Run on the production host:

```bash
cd /opt/lightfee-v2
python3 scripts/verify_production_services.py --json
python3 scripts/check_process_singleton.py --strict
```

Expected:

- `ok: true`
- one live process
- one sidecar process
- sidecar snapshot with 7 venues
- current state `lifecycle=running`, `risk_mode=running`

## Remediate Sidecar Config Drift

Install the versioned sidecar service template, then reload systemd:

```bash
sudo cp deploy/systemd/lightfee-sidecar-rust-v1.service /etc/systemd/system/lightfee-sidecar.service
sudo systemctl daemon-reload
sudo systemctl restart lightfee-sidecar.service
```

## Remediate DNS Drift

Install the NetworkManager DNS template or apply equivalent persistent resolver configuration:

```bash
sudo cp deploy/network/NetworkManager-lightfee-dns.conf /etc/NetworkManager/conf.d/99-lightfee-dns.conf
sudo systemctl reload NetworkManager || sudo systemctl restart NetworkManager
getent hosts www.okx.com
```

## Resume Live Only If Safe

Only resume from stale fail-closed when open/pending/recovery work is zero. Prefer the code-level clean-start recovery. Manual state edits require a backup and must be followed by `verify_production_services.py`.
````

- [ ] **Step 4: Extend deploy manifest critical files**

In `scripts/verify_deploy_manifest.py`, append:

```python
    "lightfee/ops/production_health.py",
    "scripts/verify_production_services.py",
    "deploy/systemd/lightfee-live.service",
    "deploy/systemd/lightfee-sidecar-rust-v1.service",
    "deploy/network/NetworkManager-lightfee-dns.conf",
```

- [ ] **Step 5: Verify deploy asset tests**

Run:

```bash
rtk pytest tests/ops/test_production_health.py -q
python scripts/verify_deploy_manifest.py
```

Expected: tests PASS and manifest lists the new critical files.

- [ ] **Step 6: Commit**

```bash
git add deploy docs/ops scripts/verify_deploy_manifest.py tests/ops/test_production_health.py
git commit -m "ops: version production service and dns templates"
```

### Task 6: Final Verification

**Files:**
- No new files unless prior tasks require fixes.

- [ ] **Step 1: Run targeted tests**

```bash
rtk pytest tests/ops/test_production_health.py -q
rtk pytest tests/test_live_full_closure.py -k "fail_closed or last_scan or candidates_tradeable" -q
rtk pytest tests/recovery/test_restart_recovery_semantics.py -q
rtk pytest tests/ops/test_current_state_and_metrics_export.py -q
```

Expected: PASS.

- [ ] **Step 2: Run broader related tests**

```bash
rtk pytest tests/config tests/sidecar tests/ops tests/recovery -q
```

Expected: PASS.

- [ ] **Step 3: Run GitNexus change detection**

```text
gitnexus_detect_changes({scope: "all", repo: "LightFeeV2"})
```

Expected: changed symbols match ops production health, recovery clean startup, current-state export, and tests.

- [ ] **Step 4: Final commit**

```bash
git add lightfee scripts deploy docs tests
git commit -m "fix: harden production sidecar live health checks"
```

## Rollout Checklist

- [ ] Apply systemd templates on production or confirm current units match them.
- [ ] Apply persistent DNS config through NetworkManager/systemd-resolved.
- [ ] Run `python3 scripts/verify_production_services.py --json` on production.
- [ ] Run `python3 scripts/check_process_singleton.py --strict` on production.
- [ ] Restart `lightfee-sidecar.service`, wait for 2 snapshots, verify 7 venue coverage.
- [ ] Restart `lightfee-live.service`, verify `risk_mode=running` and `last_scan` populated.
