from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from lightfee.config.schema import StrategyConfig
from lightfee.engine.v1_lifecycle import V1TradingLifecycle


FIXTURE_ROOT = Path("tests/fixtures/live_incidents/2026-06-05")


def load_jsonl_fixture(name: str) -> list[dict]:
    path = FIXTURE_ROOT / name
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def test_quick_flat_entry_chain_would_have_been_blocked_by_lifecycle_horizon():
    events = load_jsonl_fixture("quick_flat_entry_close_chain.jsonl")
    selected = next(event for event in events if event["kind"] == "execution.entry_selected")
    payload = selected["payload"]
    cfg = StrategyConfig()
    cfg.min_scan_minutes_before_funding = 0
    cfg.entry_min_first_funding_remaining_secs = 60
    candidate = SimpleNamespace(
        symbol=payload["symbol"],
        first_funding_timestamp_ms=payload["first_funding_timestamp_ms"],
        funding_timestamp_ms=payload["funding_timestamp_ms"],
        long_venue="binance",
        short_venue="bybit",
    )

    decision = V1TradingLifecycle.entry_admissibility(
        candidate,
        now_ms=selected["ts_ms"],
        strategy=cfg,
        recovery_ledger=None,
    )

    assert decision.allowed is False
    assert decision.reason == "entry_blocked_first_funding_too_close"
