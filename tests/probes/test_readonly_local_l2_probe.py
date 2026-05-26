from __future__ import annotations

import asyncio
import importlib.util
import os
from pathlib import Path
from types import SimpleNamespace

import pytest


pytestmark = pytest.mark.live_probe

DEFAULT_PUBLIC_PROBE_SYMBOLS = {
    "bybit": "BTCUSDT",
    "binance": "BTCUSDT",
    "okx": "BTCUSDT",
}

REQUIRED_EVIDENCE_KEYS = {
    "source",
    "depth",
    "action_or_kind",
    "sequence_fields",
    "bridge_decision",
    "readiness_effect",
    "old_behavior_result",
    "new_behavior_result",
}

FORBIDDEN_PROBE_SOURCE_TOKENS = (
    "place_order",
    "submit_order",
    "submit_passive_order",
    "cancel_order",
    "cancel_all",
    "set_leverage",
    "change_leverage",
    "set_margin",
    "margin_mode",
)


def _probe_evidence_contract(raw: dict) -> dict:
    classifications = raw.get("classifications")
    first_classification = (
        classifications[0]
        if isinstance(classifications, list)
        and classifications
        and isinstance(classifications[0], dict)
        else {}
    )
    sequence_fields = {
        key: raw[key]
        for key in (
            "ws_u",
            "ws_seq",
            "rest_u",
            "rest_seq",
            "rest_lastUpdateId",
            "first_U",
            "first_u",
            "first_pu",
            "last_u",
            "first_seq",
            "first_pseq",
            "update_id",
            "first_depth_id",
        )
        if raw.get(key) is not None
    }
    for key in ("seq_id", "prev_seq_id", "checksum"):
        if first_classification.get(key) is not None:
            sequence_fields.setdefault(key, first_classification[key])

    bridge_decision = {
        key: raw[key]
        for key in (
            "bridge_mode",
            "sequence_comparable",
            "replay_rest_snapshot_with_ws_deltas",
            "bridge_ok",
            "cap_512_would_overflow",
            "cap_4096_would_preserve_bridge",
            "subscribe_failed",
        )
        if key in raw
    }
    old_behavior_result = {}
    if "old_stale_decision" in raw:
        old_behavior_result["stale"] = raw["old_stale_decision"]
    if "cap_512_would_overflow" in raw:
        old_behavior_result["cap_512_would_overflow"] = raw["cap_512_would_overflow"]
    if first_classification.get("link_kind") is not None:
        old_behavior_result["link_kind"] = first_classification["link_kind"]

    new_behavior_result = {}
    if "sequence_comparable" in raw:
        new_behavior_result["rest_sequence_comparable"] = raw["sequence_comparable"]
    if "replay_rest_snapshot_with_ws_deltas" in raw:
        new_behavior_result["replay_rest_snapshot_with_ws_deltas"] = (
            raw["replay_rest_snapshot_with_ws_deltas"]
        )
    if "cap_4096_would_preserve_bridge" in raw:
        new_behavior_result["cap_4096_would_preserve_bridge"] = (
            raw["cap_4096_would_preserve_bridge"]
        )
    if "bridge_ok" in raw:
        new_behavior_result["bridge_ok"] = raw["bridge_ok"]
    if first_classification.get("link_kind") is not None:
        new_behavior_result["link_kind"] = first_classification["link_kind"]

    return {
        **raw,
        "source": "public_orderbook",
        "depth": (
            raw.get("ws_depth")
            or raw.get("rest_depth")
            or raw.get("depth")
            or raw.get("channel")
            or "stream"
        ),
        "action_or_kind": first_classification.get("action") or "orderbook",
        "sequence_fields": sequence_fields,
        "bridge_decision": bridge_decision,
        "readiness_effect": "evidence_only_no_runtime_state",
        "old_behavior_result": old_behavior_result,
        "new_behavior_result": new_behavior_result,
    }


def _load_probe_module():
    path = Path(__file__).resolve().parents[2] / "scripts" / "probe_local_l2_rebuilds.py"
    spec = importlib.util.spec_from_file_location("probe_local_l2_rebuilds", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _probe_script_source() -> str:
    path = Path(__file__).resolve().parents[2] / "scripts" / "probe_local_l2_rebuilds.py"
    return path.read_text()


def _probe_venues() -> list[str]:
    raw = os.environ.get("LIGHTFEE_LOCAL_L2_PROBE_VENUES", "bybit")
    return [venue.strip().lower() for venue in raw.split(",") if venue.strip()]


def test_readonly_local_l2_public_probe_uses_no_private_credentials():
    for key in os.environ:
        upper = key.upper()
        assert not (
            upper.startswith("LIGHTFEE_LOCAL_L2_PROBE_")
            and any(secret in upper for secret in ("KEY", "SECRET", "TOKEN", "PASSPHRASE"))
        )


def test_readonly_local_l2_probe_script_has_no_mutating_exchange_calls():
    source = _probe_script_source().lower()

    assert all(token not in source for token in FORBIDDEN_PROBE_SOURCE_TOKENS)


@pytest.mark.parametrize("venue", _probe_venues())
def test_readonly_local_l2_public_probe_captures_orderbook_evidence(venue: str):
    probe = _load_probe_module()
    symbol = DEFAULT_PUBLIC_PROBE_SYMBOLS.get(venue, "BTCUSDT")
    duration_s = float(os.environ.get("LIGHTFEE_LOCAL_L2_PROBE_DURATION_S", "3.0"))

    raw = asyncio.run(asyncio.wait_for(
        probe.probe(SimpleNamespace(
            venue=venue,
            symbol=symbol,
            duration_s=duration_s,
        )),
        timeout=duration_s + 20.0,
    ))
    result = _probe_evidence_contract(raw)

    assert result["venue"] == venue
    assert result["symbol"] == symbol
    assert result.get("ok") is True
    assert REQUIRED_EVIDENCE_KEYS <= result.keys()
    assert result["source"] == "public_orderbook"
    assert result["action_or_kind"]
    assert isinstance(result["sequence_fields"], dict)
    assert result["sequence_fields"]
    assert isinstance(result["bridge_decision"], dict)
    assert result["bridge_decision"]
    assert result["readiness_effect"] == "evidence_only_no_runtime_state"
    assert isinstance(result["old_behavior_result"], dict)
    assert result["old_behavior_result"]
    assert isinstance(result["new_behavior_result"], dict)
    assert result["new_behavior_result"]
    assert "submit" not in result
    assert "cancel" not in result
    assert "leverage" not in result
