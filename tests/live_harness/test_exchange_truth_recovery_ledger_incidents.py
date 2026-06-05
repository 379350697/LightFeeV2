from __future__ import annotations

import json
from pathlib import Path

import pytest

from lightfee.engine.recovery_ledger import RecoveryLedger


pytestmark = pytest.mark.live_harness

FIXTURE_ROOT = Path("tests/fixtures/live_incidents/2026-06-05")


def load_incident(name: str) -> dict:
    return json.loads((FIXTURE_ROOT / name).read_text())


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
