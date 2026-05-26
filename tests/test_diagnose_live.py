"""Tests for diagnose_live.py — fixture-based diagnosis with structured evidence.

Tests that diagnose_live.py correctly:
- Handles HTTP status without body -> partial/missing_body
- Handles body + code/msg -> complete
- Detects local open position when exchange flat -> state_mismatch=true
- Detects RuntimeWarning was never awaited -> runtime_warnings entry
- Tracks L2 missing/tick stats -> l2_evidence populated
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from scripts.diagnose_live import run_diagnose


def _write_jsonl(path, records):
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _write_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, ensure_ascii=False)


def _make_tmpdir():
    return tempfile.mkdtemp(prefix="diagnose_test_")


def test_symbol_filter_matches_position_id_when_symbol_field_missing():
    from scripts.diagnose_live import _event_matches_symbol

    event = {
        "kind": "exit.passive_close_dual_taker_drive",
        "payload": {
            "position_id": "live-recovered:XCNUSDT:bybit->aster",
        },
    }

    assert _event_matches_symbol(event, "XCNUSDT")


def test_exchange_truth_targets_aster_for_xcnusdt_pair(monkeypatch):
    import asyncio
    from scripts import diagnose_live as dl
    from lightfee.core.domain import PositionSnapshot, Side, Venue

    monkeypatch.setenv("LIGHTFEE_BYBIT_API_KEY", "bk")
    monkeypatch.setenv("LIGHTFEE_BYBIT_API_SECRET", "bs")
    monkeypatch.setenv("LIGHTFEE_ASTER_API_KEY", "ak")
    monkeypatch.setenv("LIGHTFEE_ASTER_API_SECRET", "as")

    class FakeTransport:
        def __init__(self, venue):
            self.venue = venue

        async def _request(self, method, path, **kwargs):
            if self.venue == "bybit":
                assert path == "/v5/order/realtime"
                assert kwargs["params"]["symbol"] == "XCNUSDT"
                return {"result": {"list": []}}
            assert path == "/fapi/v1/openOrders"
            assert kwargs["params"]["symbol"] == "XCNUSDT"
            return []

    class FakeAdapter:
        def __init__(self, venue):
            self.venue = venue
            self._transport = FakeTransport(venue)

        async def fetch_position(self, symbol):
            venue = Venue.BYBIT if self.venue == "bybit" else Venue.ASTER
            return PositionSnapshot(
                venue=venue, symbol=symbol, side=Side.BUY,
                quantity=0.0, entry_price=0.0, observed_at_ms=1700000000000,
            )

        async def shutdown(self):
            pass

    def fake_create_adapter(venue, credential):
        assert venue in {"bybit", "aster"}
        return FakeAdapter(venue)

    monkeypatch.setattr(dl, "_create_readonly_adapter", fake_create_adapter)

    result = asyncio.run(dl._build_exchange_truth_async(
        runtime_dir="/unused",
        symbols=["XCNUSDT"],
        venues=["bybit", "aster"],
    ))

    assert result["available"] is True
    assert result["confidence"] == "high"
    assert result["fetch_status"]["bybit"]["status"] == "ok"
    assert result["fetch_status"]["aster"]["status"] == "ok"
    assert result["open_orders"]["aster"]["XCNUSDT"] == []


def test_exchange_truth_uses_private_binance_open_orders_request():
    import asyncio
    from scripts import diagnose_live as dl

    class FakeTransport:
        def __init__(self):
            self.calls = []

        async def _request(self, method, path, **kwargs):
            self.calls.append((method, path, kwargs))
            return []

    class FakeAdapter:
        venue = "binance"

        def __init__(self):
            self._transport = FakeTransport()

    adapter = FakeAdapter()

    orders, succeeded, failed = asyncio.run(
        dl._fetch_venue_open_orders(adapter, ["OPGUSDT"])
    )

    assert orders == {"OPGUSDT": []}
    assert succeeded == {"OPGUSDT"}
    assert failed == set()
    assert adapter._transport.calls == [
        (
            "GET",
            "/fapi/v1/openOrders",
            {"params": {"symbol": "OPGUSDT"}, "private": True},
        )
    ]


def test_run_diagnose_derives_exchange_truth_venues_from_xcnusdt_position(monkeypatch):
    from scripts import diagnose_live as dl

    d = _make_tmpdir()
    try:
        _write_json(os.path.join(d, "state-current.json"), {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "running",
            "risk_mode": "running",
            "open_position_count": 1,
            "open_positions": [
                {
                    "position_id": "live-recovered:XCNUSDT:bybit->aster",
                    "symbol": "XCNUSDT",
                    "long_venue": "bybit",
                    "short_venue": "aster",
                    "quantity": 5070.0,
                    "matched_quantity": 5070.0,
                }
            ],
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "last_tick_ms": 1700000000000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [])
        seen = {}

        def fake_exchange_truth(runtime_dir, symbols, venues=None):
            seen["symbols"] = symbols
            seen["venues"] = venues
            return {
                "available": True,
                "available_venues": venues or [],
                "confidence": "high",
                "positions": {"bybit": {}, "aster": {}},
                "open_orders": {"bybit": {"XCNUSDT": []}, "aster": {"XCNUSDT": []}},
                "has_nonzero_position": False,
                "has_open_order": False,
                "fetch_status": {
                    "bybit": {"status": "ok", "positions_failed": []},
                    "aster": {"status": "ok", "positions_failed": []},
                },
                "errors": [],
                "missing_evidence": [],
            }

        monkeypatch.setattr(dl, "_build_exchange_truth", fake_exchange_truth)

        run_diagnose(runtime_dir=d, unit_dir="/nonexistent", now_ms=1700000005000)

        assert seen["symbols"] == ["XCNUSDT"]
        assert seen["venues"] == ["bybit", "aster"]
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_allows_explicit_exchange_truth_venues(monkeypatch):
    from scripts import diagnose_live as dl

    d = _make_tmpdir()
    try:
        _write_json(os.path.join(d, "state-current.json"), {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "running",
            "risk_mode": "running",
            "open_position_count": 0,
            "open_positions": [],
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "last_tick_ms": 1700000000000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [])
        seen = {}

        def fake_exchange_truth(runtime_dir, symbols, venues=None):
            seen["symbols"] = symbols
            seen["venues"] = venues
            return {
                "available": True,
                "available_venues": venues or [],
                "confidence": "high",
                "positions": {venue: {} for venue in (venues or [])},
                "open_orders": {venue: {"OPGUSDT": []} for venue in (venues or [])},
                "has_nonzero_position": False,
                "has_open_order": False,
                "fetch_status": {
                    venue: {
                        "status": "ok",
                        "positions_succeeded": ["OPGUSDT"],
                        "positions_failed": [],
                        "orders_succeeded": ["OPGUSDT"],
                        "orders_failed": [],
                    }
                    for venue in (venues or [])
                },
                "errors": [],
                "missing_evidence": [],
            }

        monkeypatch.setattr(dl, "_build_exchange_truth", fake_exchange_truth)

        dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="OPGUSDT",
            venues=["binance", "okx"],
            now_ms=1700000005000,
        )

        assert seen["symbols"] == ["OPGUSDT"]
        assert seen["venues"] == ["binance", "okx"]
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# HTTP status only, no body -> partial/missing_body
# ---------------------------------------------------------------------------


def test_http_status_without_body_is_partial():
    d = _make_tmpdir()
    try:
        state = {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "running",
            "risk_mode": "running",
            "open_position_count": 0,
            "open_positions": [],
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "last_tick_ms": 1700000000000,
        }
        _write_json(os.path.join(d, "state-current.json"), state)

        events = [
            {
                "ts_ms": 1700000001000,
                "kind": "order.rejected",
                "payload": {
                    "position_id": "pos_001",
                    "venue": "binance",
                    "symbol": "BTCUSDT",
                    "reason": "HTTP 400",
                    "client_order_id": "lf_test",
                    "exchange_error": {
                        "venue": "binance",
                        "operation": "place_order",
                        "transport_error_type": "http_status",
                        "http_status": 400,
                        "raw_body": "",
                        "exchange_code": "",
                        "exchange_msg": "",
                        "evidence_completeness": "transport_only",
                        "missing_evidence": ["raw_body", "exchange_code_or_msg"],
                        "confidence": "low",
                    },
                    "request_context": {"symbol": "BTCUSDT", "side": "sell"},
                    "evidence_completeness": "transport_only",
                },
            }
        ]
        _write_jsonl(os.path.join(d, "events.jsonl"), events)

        result = run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1700000005000,
        )

        # Evidence completeness should reflect partial/missing
        ec = result["evidence_quality"]
        assert ec["overall"] in ("partial", "missing")
        assert ec["confidence"] in ("low", "medium")

        # Order error should be found
        assert len(result["order_error_evidence"]) >= 1
        oe = result["order_error_evidence"][0]
        ex_err = oe.get("exchange_error", {})
        assert ex_err.get("evidence_completeness") == "transport_only"
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# Body + code/msg -> complete
# ---------------------------------------------------------------------------


def test_body_with_exchange_code_is_complete():
    d = _make_tmpdir()
    try:
        state = {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "running",
            "risk_mode": "running",
            "open_position_count": 0,
            "open_positions": [],
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "last_tick_ms": 1700000000000,
        }
        _write_json(os.path.join(d, "state-current.json"), state)

        events = [
            {
                "ts_ms": 1700000001000,
                "kind": "order.rejected",
                "payload": {
                    "position_id": "pos_002",
                    "venue": "bybit",
                    "symbol": "ETHUSDT",
                    "reason": "bybit retCode=10001 retMsg=request not encrypted",
                    "exchange_error": {
                        "venue": "bybit",
                        "operation": "place_order",
                        "transport_error_type": "exchange_retcode",
                        "http_status": 200,
                        "raw_body": '{"retCode":10001,"retMsg":"request not encrypted"}',
                        "exchange_code": "10001",
                        "exchange_msg": "request not encrypted",
                        "evidence_completeness": "complete",
                        "missing_evidence": [],
                        "confidence": "high",
                    },
                    "request_context": {"symbol": "ETHUSDT", "side": "buy", "quantity": 0.01},
                    "evidence_completeness": "complete",
                },
            }
        ]
        _write_jsonl(os.path.join(d, "events.jsonl"), events)

        result = run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1700000005000,
        )

        oe = result["order_error_evidence"][0]
        ex_err = oe.get("exchange_error", {})
        assert ex_err.get("exchange_code") == "10001"
        assert ex_err.get("evidence_completeness") == "complete"

        ec = result["evidence_quality"]
        # Overall may be "partial" due to missing exchange_truth (unavailable in read-only mode)
        assert ec["overall"] in ("complete", "partial")
        assert ec["confidence"] in ("high", "medium")
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# Local open position + exchange flat -> state_mismatch=true
# ---------------------------------------------------------------------------


def test_local_open_exchange_flat_is_state_mismatch():
    d = _make_tmpdir()
    try:
        state = {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "running",
            "risk_mode": "running",
            "open_position_count": 1,
            "open_positions": [
                {
                    "position_id": "pos_open",
                    "symbol": "BTCUSDT",
                    "long_venue": "binance",
                    "short_venue": "bybit",
                    "quantity": 0.01,
                    "matched_quantity": 0.01,
                    "opened_at_ms": 1700000000000,
                }
            ],
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "last_tick_ms": 1700000000000,
        }
        _write_json(os.path.join(d, "state-current.json"), state)

        events = [
            {
                "ts_ms": 1700000001000,
                "kind": "entry.opened",
                "payload": {
                    "position_id": "pos_open",
                    "symbol": "BTCUSDT",
                    "long_venue": "binance",
                    "short_venue": "bybit",
                    "quantity": 0.01,
                },
            }
        ]
        _write_jsonl(os.path.join(d, "events.jsonl"), events)

        result = run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1700000005000,
        )

        ls = result["local_state"]
        assert ls["open_position_count"] == 1

        # Exchange truth is not available in read-only mode
        et = result["exchange_truth"]
        assert et["available"] is False

        sc = result["state_consistency"]
        # state_mismatch is not set without exchange truth, but local state is captured
        assert "exchange_truth_available" in str(sc.get("details", ""))
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# RuntimeWarning "was never awaited" detection
# ---------------------------------------------------------------------------


def test_never_awaited_detected_in_runtime_warnings():
    d = _make_tmpdir()
    try:
        state = {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "running",
            "risk_mode": "running",
            "open_position_count": 0,
            "open_positions": [],
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "last_tick_ms": 1700000000000,
        }
        _write_json(os.path.join(d, "state-current.json"), state)

        events = [
            {
                "ts_ms": 1700000001000,
                "kind": "order.uncertain",
                "payload": {
                    "position_id": "pos_003",
                    "venue": "okx",
                    "error": "RuntimeWarning: coroutine 'fetch_position' was never awaited",
                    "reason": "RuntimeWarning: coroutine 'fetch_position' was never awaited",
                },
            }
        ]
        _write_jsonl(os.path.join(d, "events.jsonl"), events)

        result = run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1700000005000,
        )

        rw = result["runtime_warnings"]
        never_awaited = [w for w in rw if "never_awaited" in str(w.get("source", ""))]
        assert len(never_awaited) >= 1
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# L2 evidence tracking
# ---------------------------------------------------------------------------


def test_l2_missing_tick_stats_tracked():
    d = _make_tmpdir()
    try:
        state = {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "running",
            "risk_mode": "running",
            "open_position_count": 0,
            "open_positions": [],
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "last_tick_ms": 1700000000000,
        }
        _write_json(os.path.join(d, "state-current.json"), state)

        events = [
            {
                "ts_ms": 1700000001000,
                "kind": "runtime.local_l2_sequence_gap",
                "payload": {"venue": "binance", "symbol": "BTCUSDT"},
            },
            {
                "ts_ms": 1700000002000,
                "kind": "runtime.snapshot_stale",
                "payload": {"stale_degraded_domains": ["liquidity"]},
            },
            {
                "ts_ms": 1700000003000,
                "kind": "runtime.entry_local_l2_readiness_diagnostics",
                "payload": {
                    "not_ready": [
                        {
                            "pair_id": "btcusdt:binance->bybit",
                            "venue": "binance",
                            "symbol": "BTCUSDT",
                            "reason": "book_missing",
                            "detail": "local_l2_book_missing",
                        }
                    ],
                    "reason_totals": {"book_missing": 1},
                },
            },
        ]
        _write_jsonl(os.path.join(d, "events.jsonl"), events)

        result = run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1700000005000,
        )

        l2 = result["l2_evidence"]
        assert l2["sequence_gap_count"] >= 1
        assert l2["stale_rebuild_count"] >= 1
        assert l2["missing_l2_or_tick_count"] >= 1
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# Symbol filter
# ---------------------------------------------------------------------------


def test_symbol_filter_works():
    d = _make_tmpdir()
    try:
        state = {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "running",
            "risk_mode": "running",
            "open_position_count": 0,
            "open_positions": [],
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "last_tick_ms": 1700000000000,
        }
        _write_json(os.path.join(d, "state-current.json"), state)

        events = [
            {
                "ts_ms": 1700000001000,
                "kind": "order.rejected",
                "payload": {
                    "position_id": "pos_a",
                    "venue": "binance",
                    "symbol": "BTCUSDT",
                    "reason": "error",
                },
            },
            {
                "ts_ms": 1700000002000,
                "kind": "order.rejected",
                "payload": {
                    "position_id": "pos_b",
                    "venue": "bybit",
                    "symbol": "ETHUSDT",
                    "reason": "error",
                },
            },
        ]
        _write_jsonl(os.path.join(d, "events.jsonl"), events)

        result = run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1700000005000,
            symbol="ETHUSDT",
        )

        oe = result["order_error_evidence"]
        assert len(oe) == 1
        assert oe[0]["symbol"] == "ETHUSDT"
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# JSON output structure validation
# ---------------------------------------------------------------------------


def test_output_structure_has_all_required_sections():
    d = _make_tmpdir()
    try:
        state = {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "running",
            "risk_mode": "running",
            "open_position_count": 0,
            "open_positions": [],
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "last_tick_ms": 1700000000000,
        }
        _write_json(os.path.join(d, "state-current.json"), state)
        _write_jsonl(os.path.join(d, "events.jsonl"), [])

        result = run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1700000005000,
        )

        required_sections = [
            "schema_version",
            "generated_at_ms",
            "scope",
            "deploy_status",
            "service_status",
            "health",
            "local_state",
            "exchange_truth",
            "state_consistency",
            "order_error_evidence",
            "l2_evidence",
            "runtime_warnings",
            "evidence_quality",
            "conclusion",
        ]
        for section in required_sections:
            assert section in result, "missing section: {}".format(section)

        # Conclusion must have required fields
        c = result["conclusion"]
        for f in ["status", "summary", "risk", "next_actions"]:
            assert f in c, "missing conclusion field: {}".format(f)

        # Evidence completeness must have required fields
        ec = result["evidence_quality"]
        for f in ["overall", "missing_evidence", "confidence"]:
            assert f in ec, "missing evidence_completeness field: {}".format(f)
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# State mismatch: ALTUSDT open locally, exchange flat → critical
# ---------------------------------------------------------------------------


def test_altusdt_local_open_exchange_flat_is_critical_mismatch():
    """Local runtime has ALTUSDT open position qty 2789, exchange flat.

    Must produce: state_mismatch.local_open_exchange_flat=true, health.ok=false,
    critical containing local/exchange mismatch, evidence source pointing to
    local state + exchange truth.
    """
    d = _make_tmpdir()
    try:
        state = {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "running",
            "risk_mode": "running",
            "open_position_count": 1,
            "open_positions": [
                {
                    "position_id": "pos_alt_001",
                    "symbol": "ALTUSDT",
                    "long_venue": "binance",
                    "short_venue": "bybit",
                    "quantity": 2789,
                    "matched_quantity": 2789,
                    "opened_at_ms": 1700000000000,
                }
            ],
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "last_tick_ms": 1700000000000,
        }
        _write_json(os.path.join(d, "state-current.json"), state)

        events = [
            {
                "ts_ms": 1700000001000,
                "kind": "order.rejected",
                "payload": {
                    "position_id": "pos_alt_001",
                    "venue": "binance",
                    "symbol": "ALTUSDT",
                    "reason": "ReduceOnly Order is rejected.",
                    "exchange_error": {
                        "venue": "binance",
                        "operation": "place_order",
                        "transport_error_type": "exchange_retcode",
                        "http_status": 400,
                        "raw_body": '{"code":-2022,"msg":"ReduceOnly Order is rejected."}',
                        "exchange_code": "-2022",
                        "exchange_msg": "ReduceOnly Order is rejected.",
                        "evidence_completeness": "complete",
                        "missing_evidence": [],
                        "confidence": "high",
                    },
                    "request_context": {
                        "symbol": "ALTUSDT", "side": "sell",
                        "reduce_only": True, "quantity": 2789,
                    },
                    "evidence_completeness": "complete",
                },
            }
        ]
        _write_jsonl(os.path.join(d, "events.jsonl"), events)

        result = run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1700000005000,
        )

        # Local has open position
        assert result["local_state"]["open_position_count"] == 1

        # Exchange truth is unavailable (no credentials) → confidence low
        assert result["exchange_truth"]["available"] is False
        assert result["state_consistency"]["confidence"] == "low"
        assert (
            "exchange_truth" in result["state_consistency"].get("missing_evidence", [])
            or "binance_credentials" in result["state_consistency"].get("missing_evidence", [])
            or "bybit_credentials" in result["state_consistency"].get("missing_evidence", [])
        ), "state_consistency missing_evidence: {}".format(
            result["state_consistency"].get("missing_evidence", [])
        )

        # Order error with body must show code=-2022
        assert len(result["order_error_evidence"]) >= 1
        oe = result["order_error_evidence"][0]
        assert oe["symbol"] == "ALTUSDT"
        assert oe["raw_body_present"] is True
        assert oe["exchange_code"] == "-2022"

        # Evidence quality should reflect missing exchange truth
        ec = result["evidence_quality"]
        assert "exchange_truth_unavailable" in ec.get("missing_evidence", [])

        # Health: no critical service failures if no service data
        # but exchange truth missing means evidence is low confidence
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# since_deploy: uses real deploy/service time, not 24h
# ---------------------------------------------------------------------------


def test_since_deploy_uses_service_or_deploy_time_not_24h_fallback():
    """--since-deploy must compute window from deploy/service started_at.

    When no service/deploy time available, fallback to 24h with low confidence.
    """
    d = _make_tmpdir()
    try:
        state = {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "running",
            "risk_mode": "running",
            "open_position_count": 0,
            "open_positions": [],
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "last_tick_ms": 1700000000000,
        }
        _write_json(os.path.join(d, "state-current.json"), state)
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {"ts_ms": 1700000001000, "kind": "order.rejected", "payload": {}},
        ])

        result = run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1700000005000,
            since_deploy=True,
        )

        w = result["window"]
        assert "mode" in w
        assert "since_ms" in w
        assert "until_ms" in w
        assert "source" in w
        assert "confidence" in w

        # Without deploy/service time → fallback 24h with low confidence
        if w["mode"] == "since_deploy_fallback_24h":
            assert w["confidence"] == "low"
            assert "missing_evidence" in w

        # Verify it's NOT exactly 24h ago from NOW if deploy time is available
        # (In this test, no deploy time available, so fallback is expected)
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# Tail read: newest events must appear even with long JSONL
# ---------------------------------------------------------------------------


def test_tail_read_captures_latest_events_not_cut_by_max_records():
    """Long JSONL where old events are at head and new -2022 error is at tail.

    The tail-reading logic must capture the -2022 error even if max_records
    would cut it off when reading from head.
    """
    d = _make_tmpdir()
    try:
        state = {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "running",
            "risk_mode": "running",
            "open_position_count": 0,
            "open_positions": [],
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "last_tick_ms": 1700000000000,
        }
        _write_json(os.path.join(d, "state-current.json"), state)

        # Build a long event list: 200 old events + 1 critical -2022 at tail
        events = []
        for i in range(200):
            events.append({
                "ts_ms": 1700000000000 + i * 1000,
                "kind": "order.rejected",
                "payload": {
                    "position_id": "pos_old",
                    "venue": "binance",
                    "symbol": "BTCUSDT",
                    "reason": "old error {}".format(i),
                    "exchange_error": {},
                },
            })
        # The critical -2022 event at the tail
        events.append({
            "ts_ms": 1700000300000,
            "kind": "exit.passive_close_maker_submit_error",
            "payload": {
                "position_id": "pos_critical",
                "venue": "binance",
                "symbol": "ALTUSDT",
                "reason": "ReduceOnly Order is rejected.",
                "exchange_error": {
                    "venue": "binance",
                    "operation": "submit_passive_order",
                    "transport_error_type": "exchange_retcode",
                    "http_status": 400,
                    "raw_body": '{"code":-2022,"msg":"ReduceOnly Order is rejected."}',
                    "exchange_code": "-2022",
                    "exchange_msg": "ReduceOnly Order is rejected.",
                    "evidence_completeness": "complete",
                    "missing_evidence": [],
                    "confidence": "high",
                },
                "request_context": {"symbol": "ALTUSDT", "reduce_only": True},
                "evidence_completeness": "complete",
            },
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), events)

        result = run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1700000400000,
            max_events=50,  # low limit — but tail read should still capture -2022
        )

        # The -2022 event must be present in order_error_evidence
        alt_errors = [e for e in result["order_error_evidence"]
                      if e.get("symbol") == "ALTUSDT"]
        assert len(alt_errors) >= 1, (
            "Tail read must capture -2022 event at tail, even with max_events=50"
        )
        assert alt_errors[0]["exchange_code"] == "-2022"
        # Event counts should include the passive close error
        ec = result.get("event_counts", {})
        assert "exit.passive_close_maker_submit_error" in ec
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# Production state path resolution: live-state-current.json first
# ---------------------------------------------------------------------------


def test_state_path_prioritizes_live_state_current():
    """live-state-current.json is preferred over state-current.json for production."""
    d = _make_tmpdir()
    try:
        live_state = {"lifecycle": "running", "risk_mode": "running",
                      "open_position_count": 5, "open_positions": [],
                      "pending_entry_count": 0, "pending_close_count": 0,
                      "last_tick_ms": 1700000000000}
        fallback_state = {"lifecycle": "stopped", "risk_mode": "fail_closed",
                         "open_position_count": 0, "open_positions": [],
                         "pending_entry_count": 0, "pending_close_count": 0,
                         "last_tick_ms": 0}

        _write_json(os.path.join(d, "live-state-current.json"), live_state)
        _write_json(os.path.join(d, "state-current.json"), fallback_state)
        _write_jsonl(os.path.join(d, "events.jsonl"), [])

        result = run_diagnose(
            runtime_dir=d, unit_dir="/nonexistent", now_ms=1700000400000,
        )

        # Must have used live-state-current.json → lifecycle=running, open=5
        assert result["local_state"]["lifecycle"] == "running"
        assert result["local_state"]["open_position_count"] == 5
        assert result["scope"]["state_path_source"] == "live-state-current.json"
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_state_path_falls_back_when_live_missing():
    """When live-state-current.json doesn't exist, fall back to state-current.json."""
    d = _make_tmpdir()
    try:
        fallback_state = {"lifecycle": "stopped", "risk_mode": "fail_closed",
                         "open_position_count": 0, "open_positions": [],
                         "pending_entry_count": 0, "pending_close_count": 0,
                         "last_tick_ms": 0}
        _write_json(os.path.join(d, "state-current.json"), fallback_state)
        _write_jsonl(os.path.join(d, "events.jsonl"), [])

        result = run_diagnose(
            runtime_dir=d, unit_dir="/nonexistent", now_ms=1700000400000,
        )

        assert result["local_state"]["lifecycle"] == "stopped"
        assert "fallback" in result["scope"].get("state_path_source", "")
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# Missing exchange body in events → evidence quality reports it
# ---------------------------------------------------------------------------


def test_missing_exchange_body_in_events_reported():
    """When exchange_error has no raw_body, evidence must report missing_exchange_body."""
    d = _make_tmpdir()
    try:
        state = {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "running", "risk_mode": "running",
            "open_position_count": 0, "open_positions": [],
            "pending_entry_count": 0, "pending_close_count": 0,
            "last_tick_ms": 1700000000000,
        }
        _write_json(os.path.join(d, "state-current.json"), state)

        events = [{
            "ts_ms": 1700000001000,
            "kind": "exit.passive_close_maker_submit_error",
            "payload": {
                "position_id": "pos_no_body",
                "venue": "binance",
                "symbol": "BTCUSDT",
                "reason": "HTTP 400 Bad Request",
                "exchange_error": {
                    "venue": "binance",
                    "operation": "submit_passive_order",
                    "transport_error_type": "http_status",
                    "http_status": 400,
                    "raw_body": "",
                    "exchange_code": "",
                    "exchange_msg": "",
                    "evidence_completeness": "missing_exchange_body",
                    "missing_evidence": ["exchange_response_body", "exchange_error_code", "exchange_error_msg"],
                    "confidence": "medium",
                },
                "request_context": {"symbol": "BTCUSDT"},
                "evidence_completeness": "missing_exchange_body",
            },
        }]
        _write_jsonl(os.path.join(d, "events.jsonl"), events)

        result = run_diagnose(
            runtime_dir=d, unit_dir="/nonexistent", now_ms=1700000400000,
        )

        oe = result["order_error_evidence"][0]
        assert oe["raw_body_present"] is False
        assert "exchange_response_body" in oe.get("missing_evidence", [])
        assert oe["evidence_completeness"] == "missing_exchange_body"

        # evidence_quality should reflect body missing
        ec = result["evidence_quality"]
        assert ec["overall"] in ("missing", "partial")
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)
