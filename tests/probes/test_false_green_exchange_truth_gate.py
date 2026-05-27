from __future__ import annotations

import pytest

from lightfee.ops.production_health import analyze_current_state


pytestmark = pytest.mark.live_probe


def test_probe_rejects_local_flat_when_exchange_truth_has_live_position():
    state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1779845200000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "last_scan": {"candidate_count": 12, "tradeable_count": 1},
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": True,
            "positions": {
                "binance": {
                    "MUBARAKUSDT": {
                        "venue": "binance",
                        "symbol": "MUBARAKUSDT",
                        "side": "buy",
                        "quantity": 1758.0,
                        "entry_price": 0.01234,
                    }
                },
                "aster": {
                    "BEATUSDT": {
                        "venue": "aster",
                        "symbol": "BEATUSDT",
                        "side": "buy",
                        "quantity": 23.0,
                        "entry_price": 1.0192,
                    }
                },
            },
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1779845205000,
        max_tick_age_ms=15_000,
    )

    assert report.ok is False
    assert report.severity == "critical"
    assert "exchange_truth_mismatch" in report.fingerprints
    assert "nonzero_live_position" in report.fingerprints
    mismatches = report.details["exchange_truth_mismatches"]
    assert {item["symbol"] for item in mismatches} == {"MUBARAKUSDT", "BEATUSDT"}
