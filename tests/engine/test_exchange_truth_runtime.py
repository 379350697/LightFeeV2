from __future__ import annotations

from lightfee.engine.exchange_truth import (
    ExchangeTruthOpenOrder,
    ExchangeTruthPosition,
    ExchangeTruthProbeEvidence,
    ExchangeTruthSnapshot,
    normalize_exchange_truth_payload,
)


def test_snapshot_payload_includes_shared_probe_metadata_and_truth_available():
    snapshot = ExchangeTruthSnapshot(
        available=True,
        confidence="high",
        venues=("bybit",),
        positions=(
            ExchangeTruthPosition(
                venue="bybit",
                symbol="SEIUSDT",
                side="buy",
                quantity=455.0,
                entry_price=0.1887,
            ),
        ),
        open_orders=(
            ExchangeTruthOpenOrder(
                venue="bybit",
                symbol="TRXUSDT",
                side="buy",
                quantity=72.0,
                price=0.33044,
                reduce_only=False,
                order_id="live-order",
            ),
        ),
        probe_evidence=(
            ExchangeTruthProbeEvidence(
                venue="bybit",
                symbol="TRXUSDT",
                endpoint="/v5/order/realtime",
                method="GET",
                timeout_budget_s=30.0,
                started_at_ms=1778787000000,
                finished_at_ms=1778787000100,
                classification="open_order_probe_succeeded",
            ),
        ),
    )

    payload = snapshot.to_legacy_payload()

    assert payload["available"] is True
    assert payload["truth_available"] is True
    assert payload["has_nonzero_position"] is True
    assert payload["has_open_order"] is True
    assert payload["positions"]["bybit"]["SEIUSDT"]["quantity"] == 455.0
    assert payload["open_orders"]["bybit"]["TRXUSDT"][0]["order_id"] == "live-order"
    evidence = payload["probe_evidence"][0]
    assert evidence["endpoint"] == "/v5/order/realtime"
    assert evidence["method"] == "GET"
    assert evidence["timeout_budget_s"] == 30.0
    assert evidence["started_at_ms"] == 1778787000000
    assert evidence["finished_at_ms"] == 1778787000100


def test_normalize_payload_preserves_unsupported_symbol_and_timeout_evidence():
    payload = normalize_exchange_truth_payload(
        {
            "available": False,
            "confidence": "low",
            "positions": {},
            "open_orders": {},
            "position_probe_evidence": {
                "okx": {
                    "CROSSUSDT": {
                        "classification": "unsupported_symbol_flat",
                        "endpoint": "fetch_position",
                    }
                }
            },
            "open_order_probe_evidence": {
                "bybit": {
                    "TRXUSDT": {
                        "classification": "open_order_probe_failed",
                        "endpoint": "/v5/order/realtime",
                        "timeout_budget_s": 1.0,
                        "error": "exchange truth probe timed out after 1s",
                    }
                }
            },
        }
    )

    assert payload["truth_available"] is False
    assert payload["available"] is False
    assert payload["probe_evidence"][0]["unsupported_symbol"] is True
    assert payload["probe_evidence"][1]["timed_out"] is True
    assert payload["probe_evidence"][1]["timeout_budget_s"] == 1.0
