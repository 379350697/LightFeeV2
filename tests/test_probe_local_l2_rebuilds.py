from __future__ import annotations

import asyncio
import importlib.util
import json
from types import SimpleNamespace
from pathlib import Path
import urllib.request


def _load_probe_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "probe_local_l2_rebuilds.py"
    spec = importlib.util.spec_from_file_location("probe_local_l2_rebuilds", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_probe_wire_symbol_uses_venue_specs():
    probe = _load_probe_module()

    assert probe._wire_symbol("okx", "INJUSDT") == "INJ-USDT-SWAP"
    assert probe._wire_symbol("gate", "BTCUSDT") == "BTC_USDT"
    assert probe._wire_symbol("hyperliquid", "MAVUSDT") == "MAV"


def test_hyperliquid_level_counts_parse_two_sided_levels():
    probe = _load_probe_module()

    bid_count, ask_count = probe._hyperliquid_level_counts({
        "levels": [
            [{"px": "1.0", "sz": "2.0"}],
            [{"px": "1.1", "sz": "3.0"}],
        ],
    })

    assert bid_count == 1
    assert ask_count == 1


def test_gate_subscribe_message_uses_legacy_zero_interval():
    probe = _load_probe_module()

    message = probe._gate_subscribe_message("BTC_USDT", now_s=123456)

    assert message == {
        "time": 123456,
        "channel": "futures.order_book",
        "event": "subscribe",
        "payload": ["BTC_USDT", "20", "0"],
    }


def test_hyperliquid_probe_reports_total_levels_from_two_sided_response(monkeypatch):
    probe = _load_probe_module()

    class FakeResponse:
        def read(self):
            return json.dumps({
                "coin": "BTC",
                "levels": [
                    [{"px": "1.0", "sz": "2.0"}],
                    [{"px": "1.1", "sz": "3.0"}],
                ],
            }).encode()

    def fake_urlopen(req, timeout):
        return FakeResponse()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    result = asyncio.run(probe._probe_hyperliquid(
        SimpleNamespace(symbol="BTCUSDT", duration_s=1.0)
    ))

    assert result["ok"] is True
    assert result["wire_symbol"] == "BTC"
    assert result["bid_levels"] == 1
    assert result["ask_levels"] == 1
    assert result["total_levels"] == 2
    assert result["empty_side"] is False
