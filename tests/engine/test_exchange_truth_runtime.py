from __future__ import annotations

from lightfee.engine.exchange_truth import (
    ExchangeTruthOpenOrder,
    ExchangeTruthPosition,
    ExchangeTruthProbeEvidence,
    ExchangeTruthSnapshot,
    normalize_exchange_truth_payload,
    snapshot_from_legacy_payload,
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


def test_multi_venue_positions_and_open_orders_roundtrip_through_legacy_payload():
    snapshot = ExchangeTruthSnapshot(
        available=True,
        confidence="high",
        venues=("bybit", "okx"),
        positions=(
            ExchangeTruthPosition(
                venue="bybit",
                symbol="SEIUSDT",
                side="buy",
                quantity=455.0,
                entry_price=0.1887,
            ),
            ExchangeTruthPosition(
                venue="okx",
                symbol="TRXUSDT",
                side="sell",
                quantity=72.0,
                entry_price=0.33044,
            ),
        ),
        open_orders=(
            ExchangeTruthOpenOrder(
                venue="bybit",
                symbol="SEIUSDT",
                side="buy",
                quantity=455.0,
                price=0.1888,
                reduce_only=False,
                order_id="bybit-maker",
            ),
            ExchangeTruthOpenOrder(
                venue="okx",
                symbol="TRXUSDT",
                side="sell",
                quantity=72.0,
                price=0.3303,
                reduce_only=True,
                client_order_id="okx-reduce-client",
            ),
        ),
    )

    payload = normalize_exchange_truth_payload(snapshot.to_legacy_payload())
    roundtrip = snapshot_from_legacy_payload(payload)

    assert payload["positions"]["bybit"]["SEIUSDT"]["quantity"] == 455.0
    assert payload["positions"]["okx"]["TRXUSDT"]["quantity"] == 72.0
    assert payload["open_orders"]["bybit"]["SEIUSDT"][0]["order_id"] == "bybit-maker"
    assert (
        payload["open_orders"]["okx"]["TRXUSDT"][0]["client_order_id"]
        == "okx-reduce-client"
    )
    assert {position.venue for position in roundtrip.positions} == {"bybit", "okx"}
    assert {order.venue for order in roundtrip.open_orders} == {"bybit", "okx"}
    assert roundtrip.truth_available is True


def test_partial_venue_timeout_preserves_available_venues_and_missing_evidence():
    payload = normalize_exchange_truth_payload(
        {
            "available": True,
            "truth_available": True,
            "confidence": "partial",
            "fetch_status": {
                "bybit": {"status": "ok"},
                "okx": {
                    "status": "timeout",
                    "error": "exchange truth probe timed out after 2s",
                },
            },
            "positions": {
                "bybit": {
                    "SEIUSDT": {
                        "venue": "bybit",
                        "symbol": "SEIUSDT",
                        "side": "buy",
                        "quantity": 455.0,
                    }
                }
            },
            "open_orders": {},
            "position_probe_evidence": {
                "bybit": {
                    "SEIUSDT": {
                        "classification": "position_probe_succeeded",
                        "endpoint": "fetch_position",
                    }
                },
                "okx": {
                    "SEIUSDT": {
                        "classification": "position_probe_timeout",
                        "endpoint": "fetch_position",
                        "timeout_budget_s": 2.0,
                        "error": "exchange truth probe timed out after 2s",
                    }
                },
            },
            "missing_evidence": ["okx:SEIUSDT:position_probe_timeout"],
        }
    )
    roundtrip = snapshot_from_legacy_payload(payload).to_legacy_payload()

    assert payload["available"] is True
    assert payload["truth_available"] is True
    assert payload["available_venues"] == ["bybit"]
    assert payload["missing_evidence"] == ["okx:SEIUSDT:position_probe_timeout"]
    assert any(
        item["venue"] == "okx" and item["timed_out"] is True
        for item in payload["probe_evidence"]
    )
    assert roundtrip["available_venues"] == ["bybit"]
    assert roundtrip["missing_evidence"] == ["okx:SEIUSDT:position_probe_timeout"]


def test_unsupported_symbol_evidence_is_not_treated_as_successful_flat_truth():
    payload = normalize_exchange_truth_payload(
        {
            "available": False,
            "truth_available": False,
            "confidence": "low",
            "positions": {},
            "open_orders": {},
            "fetch_status": {
                "okx": {
                    "status": "unsupported_symbol",
                    "symbol": "CROSSUSDT",
                    "error": "instrument not found",
                }
            },
            "position_probe_evidence": {
                "okx": {
                    "CROSSUSDT": {
                        "classification": "unsupported_symbol_flat",
                        "endpoint": "fetch_position",
                        "error": "instrument not found",
                    }
                }
            },
            "missing_evidence": ["okx:CROSSUSDT:unsupported_symbol"],
        }
    )
    roundtrip = snapshot_from_legacy_payload(payload).to_legacy_payload()

    assert payload["truth_available"] is False
    assert payload["available_venues"] == []
    assert payload["has_nonzero_position"] is False
    assert payload["probe_evidence"][0]["unsupported_symbol"] is True
    assert payload["missing_evidence"] == ["okx:CROSSUSDT:unsupported_symbol"]
    assert roundtrip["truth_available"] is False
    assert roundtrip["missing_evidence"] == ["okx:CROSSUSDT:unsupported_symbol"]


def test_retryable_probe_error_survives_fetch_status_errors_and_probe_evidence():
    payload = normalize_exchange_truth_payload(
        {
            "available": True,
            "truth_available": True,
            "confidence": "partial",
            "fetch_status": {
                "bybit": {"status": "ok"},
                "okx": {
                    "status": "retryable_error",
                    "error": "HTTP 429 rate limit; retry after 1s",
                },
            },
            "positions": {},
            "open_orders": {},
            "open_order_probe_evidence": {
                "okx": {
                    "TRXUSDT": {
                        "classification": "open_order_probe_retryable_error",
                        "endpoint": "/api/v5/trade/orders-pending",
                        "method": "GET",
                        "error": "HTTP 429 rate limit; retry after 1s",
                    }
                }
            },
        }
    )
    roundtrip = snapshot_from_legacy_payload(payload).to_legacy_payload()

    assert payload["fetch_status"]["okx"]["status"] == "retryable_error"
    assert payload["errors"] == ["okx: HTTP 429 rate limit; retry after 1s"]
    assert payload["probe_evidence"][0]["classification"] == (
        "open_order_probe_retryable_error"
    )
    assert payload["probe_evidence"][0]["error"] == (
        "HTTP 429 rate limit; retry after 1s"
    )
    assert roundtrip["errors"] == ["okx: HTTP 429 rate limit; retry after 1s"]


def test_exchange_truth_schema_version_survives_normalization_when_present():
    payload = normalize_exchange_truth_payload(
        {
            "schema_version": "exchange_truth.v2",
            "snapshot_version": 7,
            "available": True,
            "truth_available": True,
            "positions": {},
            "open_orders": {},
        }
    )
    roundtrip = snapshot_from_legacy_payload(payload).to_legacy_payload()

    assert payload["schema_version"] == "exchange_truth.v2"
    assert payload["snapshot_version"] == 7
    assert roundtrip["schema_version"] == "exchange_truth.v2"
    assert roundtrip["snapshot_version"] == 7
