from __future__ import annotations

import json
from pathlib import Path

from lightfee.marketdata.l2 import L2BookStatus
from lightfee.marketdata.local_l2_policy import BridgeMode, policy_for_venue
from lightfee.marketdata.local_l2_runtime import LocalL2Runtime
from lightfee.marketdata.local_l2_venues import parse_l2_update
from lightfee.marketdata.local_l2_incident_classification import (
    OKX_LOCAL_BOOK_DOC,
    official_local_book_doc_url,
    official_sequence_rebuild_reason,
)


FIXTURES = Path(__file__).parent / "fixtures" / "local_l2"

ALLOWED_INCIDENT_CLASSIFICATIONS = {
    "V1 parity drift",
    "official-doc exchange reset/sequence behavior",
    "expected real gap",
    "insufficient evidence",
}


def test_bybit_rest_snapshot_sequence_is_not_comparable_to_ws_depth50_book():
    fixture = json.loads((FIXTURES / "bybit_irysusdt_rest_ws_sequence_domain.json").read_text())
    policy = policy_for_venue("bybit")
    rt = LocalL2Runtime()
    book = rt.ensure_book("bybit", "IRYSUSDT")
    book.status = L2BookStatus.BOOTSTRAPPING
    book.sequence = fixture["current_book"]["sequence"]
    book.last_update_id = fixture["current_book"]["last_update_id"]
    book.observed_at_ms = 0

    update = parse_l2_update(
        "bybit",
        fixture["rest_snapshot"],
        symbol="IRYSUSDT",
        now_ms=1779302500002,
    )

    old_decision = update.sequence < book.last_update_id

    assert old_decision is True
    assert policy.bridge_mode is BridgeMode.WS_SNAPSHOT_AUTHORITATIVE
    assert policy.rest_snapshot_sequence_comparable is False
    assert policy.replay_rest_snapshot_with_ws_deltas is False


def test_binance_previous_link_mismatch_fixture_matches_production_error():
    fixture = json.loads((FIXTURES / "binance_jtousdt_buffered_replay_previous_link_mismatch.json").read_text())
    previous = fixture["snapshot_last_update_id"]
    first = fixture["buffered_updates"][0]
    second = fixture["buffered_updates"][1]

    assert first["pu"] == previous
    previous = first["u"]
    assert second["pu"] != previous
    assert fixture["expected"]["expected_previous"] == previous
    assert fixture["expected"]["observed_previous"] == second["pu"]


def test_local_l2_incident_replay_gate_uses_closed_classification_set():
    assert ALLOWED_INCIDENT_CLASSIFICATIONS == {
        "V1 parity drift",
        "official-doc exchange reset/sequence behavior",
        "expected real gap",
        "insufficient evidence",
    }


def test_okx_previous_link_mismatch_uses_official_sequence_classification():
    payload = {
        "venue": "okx",
        "symbol": "LAB-USDT-SWAP",
        "reason": "buffered_replay_invalid_link: expected 12345 got 12340->12350",
        "previous_sequence_present": True,
        "expected_previous_sequence": 12345,
        "raw_u": 12350,
        "raw_pu": 12340,
    }

    assert official_local_book_doc_url("okx") == OKX_LOCAL_BOOK_DOC
    assert official_sequence_rebuild_reason(payload) == "previous_link_mismatch"


def test_okx_checksum_mismatch_uses_official_data_integrity_classification():
    payload = {
        "venue": "okx",
        "symbol": "LAB-USDT-SWAP",
        "reason": "checksum_mismatch expected=-1546922990 actual=-687325722",
    }

    assert official_sequence_rebuild_reason(payload) == "checksum_mismatch"


def test_aster_overlap_is_not_official_rebuild_evidence():
    payload = {
        "venue": "aster",
        "symbol": "ASTERUSDT",
        "previous_sequence_present": True,
        "expected_previous_sequence": 100,
        "raw_U": 101,
        "raw_u": 103,
        "raw_pu": 99,
    }

    assert official_sequence_rebuild_reason(payload) == ""


def test_existing_replay_fixtures_do_not_require_stale_threshold_relaxation():
    bybit_fixture = json.loads(
        (FIXTURES / "bybit_irysusdt_rest_ws_sequence_domain.json").read_text()
    )
    binance_fixture = json.loads(
        (FIXTURES / "binance_jtousdt_buffered_replay_previous_link_mismatch.json").read_text()
    )
    bybit_policy = policy_for_venue("bybit")

    bybit_old_stale = (
        bybit_fixture["rest_snapshot"]["result"]["u"]
        < bybit_fixture["current_book"]["last_update_id"]
    )
    assert bybit_old_stale is True
    assert bybit_policy.rest_snapshot_sequence_comparable is False
    assert bybit_policy.replay_rest_snapshot_with_ws_deltas is False

    expected_previous = binance_fixture["expected"]["expected_previous"]
    observed_previous = binance_fixture["expected"]["observed_previous"]
    assert observed_previous != expected_previous
    assert binance_fixture["buffered_updates"][1]["pu"] == observed_previous
