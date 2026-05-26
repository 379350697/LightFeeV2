from __future__ import annotations

import json
from pathlib import Path

import pytest


pytestmark = pytest.mark.live_harness

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
