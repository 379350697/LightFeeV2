from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
from types import SimpleNamespace


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
    "private_key",
    "api_secret",
)


def _load_probe_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "probe_ws_bbo_quote_provider.py"
    spec = importlib.util.spec_from_file_location("probe_ws_bbo_quote_provider", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _probe_script_source() -> str:
    path = Path(__file__).resolve().parents[1] / "scripts" / "probe_ws_bbo_quote_provider.py"
    return path.read_text()


def test_ws_bbo_probe_script_is_public_read_only():
    source = _probe_script_source().lower()

    assert all(token not in source for token in FORBIDDEN_PROBE_SOURCE_TOKENS)


def test_ws_bbo_probe_uses_existing_provider_and_returns_quote_evidence(monkeypatch):
    probe = _load_probe_module()

    class FakeDataPlane:
        def __init__(self, cache):
            self.cache = cache

        def start_ws_streams(self, venue, symbols):
            from lightfee.marketdata.ws_bbo import TopBookQuote

            self.cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol=symbols[0],
                    bid=50000.0,
                    ask=50001.0,
                    bid_size=2.0,
                    ask_size=3.0,
                    observed_at_ms=1000,
                    received_at_ms=1001,
                    source="fake_ws_bbo",
                )
            )
            return 1

        async def connect_ws_streams(self):
            return 1

        def stream_state(self, venue, symbol):
            return {
                "venue": venue,
                "symbol": symbol,
                "tracked": True,
                "connected": True,
                "message_count": 1,
                "last_error": "",
            }

        async def stop_ws_streams(self, *, per_client_timeout_s=1.0):
            return None

    monkeypatch.setattr(probe, "VenueBboDataPlane", FakeDataPlane)

    result = asyncio.run(probe.probe(SimpleNamespace(
        venue="binance",
        symbol="BTCUSDT",
        duration_s=0.5,
    )))

    assert result["ok"] is True
    assert result["source"] == "ws_bbo_quote_provider"
    assert result["classification"] == "quote_received"
    assert result["bid"] == 50000.0
    assert result["ask"] == 50001.0
    assert result["readiness_effect"] == "evidence_only_no_runtime_state"
