from __future__ import annotations

import asyncio
import importlib.util
import json
from types import SimpleNamespace
from pathlib import Path


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


def test_gate_subscribe_message_uses_v1_obu_channel():
    probe = _load_probe_module()

    message = probe._gate_subscribe_message("BTC_USDT", now_s=123456)

    assert message == {
        "time": 123456,
        "channel": "futures.obu",
        "event": "subscribe",
        "payload": ["ob.BTC_USDT.400"],
    }


def test_hyperliquid_probe_reports_total_levels_from_two_sided_response(monkeypatch):
    probe = _load_probe_module()

    class FakeWs:
        async def send(self, message):
            self.subscribe_message = json.loads(message)

        async def recv(self):
            return json.dumps({
                "channel": "l2Book",
                "data": {
                "coin": "BTC",
                "levels": [
                    [{"px": "1.0", "sz": "2.0"}],
                    [{"px": "1.1", "sz": "3.0"}],
                ],
                },
            })

    class FakeConnect:
        async def __aenter__(self):
            self.ws = FakeWs()
            return self.ws

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def fake_connect(*args, **kwargs):
        return FakeConnect()

    monkeypatch.setattr(probe.websockets, "connect", fake_connect)

    result = asyncio.run(probe._probe_hyperliquid(
        SimpleNamespace(symbol="BTCUSDT", duration_s=1.0)
    ))

    assert result["ok"] is True
    assert result["wire_symbol"] == "BTC"
    assert result["bridge_mode"] == "stream_only"
    assert result["bid_levels"] == 1
    assert result["ask_levels"] == 1
    assert result["total_levels"] == 2
    assert result["empty_side"] is False
    assert result["ws_event_count"] == 1


def test_aster_probe_reports_binance_compatible_depth_update(monkeypatch):
    probe = _load_probe_module()

    class FakeWs:
        def __init__(self):
            self.calls = 0

        async def recv(self):
            self.calls += 1
            if self.calls > 1:
                raise asyncio.TimeoutError
            return json.dumps({
                "e": "depthUpdate",
                "s": "BTCUSDT",
                "U": 10,
                "u": 12,
                "pu": 9,
                "b": [["100.0", "1.0"]],
                "a": [["101.0", "1.0"]],
            })

    class FakeConnect:
        async def __aenter__(self):
            self.ws = FakeWs()
            return self.ws

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def fake_connect(*args, **kwargs):
        return FakeConnect()

    monkeypatch.setattr(probe.websockets, "connect", fake_connect)

    result = asyncio.run(probe._probe_aster(
        SimpleNamespace(symbol="BTCUSDT", duration_s=0.01)
    ))

    assert result["ok"] is True
    assert result["venue"] == "aster"
    assert result["bridge_mode"] == "rest_snapshot_buffered_replay"
    assert result["first_U"] == 10
    assert result["first_u"] == 12
    assert result["first_pu"] == 9
    assert result["previous_sequence_present_count"] == 1
