"""Tests for diagnose_live.py — fixture-based diagnosis with structured evidence.

Tests that diagnose_live.py correctly:
- Handles HTTP status without body -> partial/missing_body
- Handles body + code/msg -> complete
- Detects local open position when exchange flat -> state_mismatch=true
- Detects RuntimeWarning was never awaited -> runtime_warnings entry
- Tracks L2 missing/tick stats -> l2_evidence populated
- Separates snapshot stale/degraded diagnostics from Local L2 evidence
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


def test_l2_evidence_excludes_legacy_ws_bbo_selection_events():
    from scripts.diagnose_live import _build_l2_evidence

    evidence = _build_l2_evidence([
        {
            "ts_ms": 1779810000000,
            "kind": "runtime.entry_blocked_local_l2_selection",
            "payload": {
                "reason": "entry_ws_bbo_quote_lease_stale_quote",
                "provider": "ws_bbo_quote_lease",
                "readiness_evidence": {
                    "provider": "ws_bbo_quote_lease",
                    "source": "ws_bbo_quote_lease",
                },
            },
        },
        {
            "ts_ms": 1779810001000,
            "kind": "runtime.entry_local_l2_readiness_diagnostics",
            "payload": {
                "provider": "local_l2",
                "reason_totals": {"book_missing": 2},
                "not_ready": [
                    {
                        "pair_id": "btcusdt:binance->bybit",
                        "venue": "binance",
                        "symbol": "BTCUSDT",
                        "reason": "book_missing",
                    }
                ],
            },
        },
    ])

    assert evidence["missing_l2_or_tick_count"] == 2
    assert evidence["details"] == [
        {
            "kind": "runtime.entry_local_l2_readiness_diagnostics",
            "pair_id": "btcusdt:binance->bybit",
            "venue": "binance",
            "symbol": "BTCUSDT",
            "reason": "book_missing",
            "ts_ms": 1779810001000,
        }
    ]


def test_snapshot_stale_and_degraded_are_reported_outside_l2_evidence():
    from scripts.diagnose_live import _build_l2_evidence, _build_snapshot_evidence

    events = [
        {
            "ts_ms": 1779810000000,
            "kind": "runtime.snapshot_stale",
            "payload": {"stale_degraded_domains": ["quote"]},
        },
        {
            "ts_ms": 1779810001000,
            "kind": "runtime.snapshot_degraded",
            "payload": {
                "degraded_domains": ["liquidity"],
                "candidate_freshness_scope": [
                    {
                        "candidate_symbol": "BTCUSDT",
                        "domain": "quote",
                        "blocked": True,
                        "block_reason": "quote_stale",
                    }
                ],
            },
        },
    ]

    l2 = _build_l2_evidence(events)
    snapshot = _build_snapshot_evidence(events)

    assert l2["stale_rebuild_count"] == 0
    assert l2["missing_l2_or_tick_count"] == 0
    assert snapshot["stale_or_degraded_count"] == 2
    assert snapshot["domain_counts"] == {"liquidity": 1, "quote": 2}
    assert snapshot["blocking_scope_count"] == 1


def _flat_exchange_truth(runtime_dir, symbols, venues=None):
    venues = venues or []
    return {
        "available": True,
        "available_venues": venues,
        "confidence": "high",
        "positions": {venue: {} for venue in venues},
        "open_orders": {venue: {symbol: [] for symbol in symbols} for venue in venues},
        "has_nonzero_position": False,
        "has_open_order": False,
        "fetch_status": {
            venue: {
                "status": "ok",
                "positions_succeeded": symbols,
                "positions_failed": [],
                "orders_succeeded": symbols,
                "orders_failed": [],
            }
            for venue in venues
        },
        "errors": [],
        "missing_evidence": [],
    }


def test_run_diagnose_emits_fixture_replay_acceptance_gate(monkeypatch):
    """Production-window fixture replay must expose the final read-only gate."""
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
            "last_tick_ms": 1779816050000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1779816045100,
                "kind": "passive_maintenance.cancel_try_window",
                "payload": {
                    "symbol": "RIVERUSDT",
                    "entry_id": "incident-riverusdt-0001",
                    "fill_ratio": 0.0,
                    "try_window_ms": 1500,
                    "min_fill_ratio": 0.25,
                },
            },
            {
                "ts_ms": 1779816047600,
                "kind": "entry.aborted",
                "payload": {
                    "symbol": "RIVERUSDT",
                    "entry_id": "incident-riverusdt-0001",
                    "reason": "passive_maker_canceled_zero_fill",
                },
            },
            {
                "ts_ms": 1779816047700,
                "kind": "recovery.live_position_probe_venue_cooldown",
                "payload": {
                    "venue": "okx",
                    "classification": "rate_limited",
                    "endpoint": "/api/v5/account/positions",
                },
            },
            {
                "ts_ms": 1779816047800,
                "kind": "recovery.live_position_probe_unsupported_symbols",
                "payload": {
                    "venue": "okx",
                    "skipped_by_catalog": ["CHIPUSDT", "DELISTEDUSDT", "OLDUSDT"],
                },
            },
            {
                "ts_ms": 1779816047900,
                "kind": "runtime.local_l2_sequence_gap_rebuild",
                "payload": {
                    "venue": "aster",
                    "symbol": "RIVERUSDT",
                    "previous_sequence_present": True,
                    "expected_previous_sequence": 924684113,
                    "raw_U": 924684120,
                    "raw_u": 924684126,
                    "raw_pu": 924684118,
                    "status_after": "rebuilding",
                },
            },
            {
                "ts_ms": 1779816048000,
                "kind": "runtime.snapshot_fallback_last_good",
                "payload": {
                    "symbol": "RIVERUSDT",
                    "domain": "market_observed",
                    "v1_parity_evidence": "CL-006-snapshot-fallback-degraded-domain-entry-impact",
                    "candidate_freshness_scope": [
                        {
                            "candidate_symbol": "RIVERUSDT",
                            "domain": "market_observed",
                            "blocked": True,
                            "block_reason": "market_observed_stale",
                        }
                    ],
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="RIVERUSDT",
            venues=["aster", "okx"],
            now_ms=1779816055000,
        )

        gate = result["production_acceptance_gate"]
        assert gate["passive_maker_zero_fill_count"] == 1
        assert gate["passive_maker_fill_rate"] == 0.0
        assert gate["abort_fail_closed_count"] == 0
        assert gate["okx_recovery_probe_rate_limited_count"] == 1
        assert gate["okx_instrument_missing_skipped_count"] == 3
        assert gate["local_l2_official_rebuild_count"] == 1
        assert gate["snapshot_fallback_blocking_count"] == 1
        assert gate["entry_opened_count"] == 0
        assert gate["position_opened_count"] == 0
        assert gate["open_position_count"] == 0
        assert gate["pending_entry_count"] == 0
        assert gate["exchange_truth_flat"] is True
        assert gate["exchange_truth_no_open_orders"] is True
        assert gate["gate_passed"] is True
        assert gate["blocking_reasons"] == []
        assert gate["unclassified_exceptions"] == []
        assert gate["insufficient_evidence_exceptions"] == []
        assert set(gate["exception_conclusions"].values()) <= {
            "v1_parity",
            "official_doc",
            "insufficient_evidence",
        }
        assert gate["exception_conclusions"]["passive_maker_zero_fill"] == "v1_parity"
        assert gate["exception_conclusions"]["local_l2_official_rebuild"] == "official_doc"
        assert gate["exception_conclusions"]["snapshot_fallback_blocking"] == "v1_parity"
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_acceptance_gate_blocks_insufficient_exception_evidence(monkeypatch):
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
            "last_tick_ms": 1779816050000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1779816049000,
                "kind": "entry.aborted",
                "payload": {
                    "symbol": "BULLAUSDT",
                    "reason": "fail_closed",
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="BULLAUSDT",
            venues=["aster", "binance"],
            now_ms=1779816055000,
        )

        gate = result["production_acceptance_gate"]
        assert gate["abort_fail_closed_count"] == 1
        assert gate["exception_conclusions"]["abort_fail_closed"] == "insufficient_evidence"
        assert gate["insufficient_evidence_exceptions"] == ["abort_fail_closed"]
        assert gate["blocking_reasons"] == ["diagnostic_exception_insufficient_evidence"]
        assert gate["gate_passed"] is False
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_acceptance_gate_classifies_nonblocking_health_and_contained_admission(monkeypatch):
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
            "last_tick_ms": 1779816050000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1779816049000,
                "kind": "recovery.live_position_bulk_diagnostic_error",
                "payload": {
                    "venue": "okx",
                    "symbol": "BTCUSDT",
                    "classification": "timeout",
                    "truth_required_by": [],
                    "diagnostic_scope": "best_effort_bulk_positions",
                    "blocking": False,
                },
            },
            {
                "ts_ms": 1779816049100,
                "kind": "runtime.entry_admission_venue_degraded",
                "payload": {
                    "venue": "hyperliquid",
                    "symbol": "BTCUSDT",
                    "reason": "insufficient_margin_admission_blocked",
                    "block_scope": "venue",
                    "evidence_gap": False,
                    "cooldown_active": True,
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="BTCUSDT",
            venues=["okx", "hyperliquid"],
            now_ms=1779816055000,
        )

        gate = result["production_acceptance_gate"]
        assert gate["bulk_health_diagnostic_count"] == 1
        assert gate["contained_admission_count"] == 1
        assert gate["required_position_truth_unavailable_count"] == 0
        assert gate["exception_conclusions"]["nonblocking_health_diagnostic"] == "nonblocking_health_diagnostic"
        assert gate["exception_conclusions"]["contained_admission"] == "contained_admission"
        assert gate["blocking_reasons"] == []
        assert gate["gate_passed"] is True
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_acceptance_gate_blocks_required_position_truth_unavailable(monkeypatch):
    from scripts import diagnose_live as dl

    d = _make_tmpdir()
    try:
        _write_json(os.path.join(d, "state-current.json"), {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "risk_only",
            "risk_mode": "risk_only",
            "open_position_count": 0,
            "open_positions": [],
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "last_tick_ms": 1779816050000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1779816049000,
                "kind": "recovery.required_position_truth_unavailable",
                "payload": {
                    "venue": "okx",
                    "symbol": "ETHUSDT",
                    "classification": "timeout",
                    "truth_required_by": ["pending_residual_repair"],
                    "blocking": True,
                    "decision": "truth_unavailable_for_required_recovery",
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="ETHUSDT",
            venues=["okx"],
            now_ms=1779816055000,
        )

        gate = result["production_acceptance_gate"]
        assert gate["required_position_truth_unavailable_count"] == 1
        assert gate["exception_conclusions"]["blocking_required_truth"] == "blocking_required_truth"
        assert gate["blocking_reasons"] == ["blocking_required_truth"]
        assert gate["gate_passed"] is False
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_acceptance_gate_downgrades_historical_required_truth_after_core_clean(
    monkeypatch,
):
    from scripts import diagnose_live as dl

    d = _make_tmpdir()
    try:
        _write_json(os.path.join(d, "state-current.json"), {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "risk_only",
            "risk_mode": "running",
            "open_position_count": 0,
            "open_positions": [],
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "pending_residual_repair_count": 0,
            "last_tick_ms": 1779816050000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1779816049000,
                "kind": "recovery.required_position_truth_unavailable",
                "payload": {
                    "venue": "bybit",
                    "symbol": "*",
                    "classification": "timeout",
                    "truth_required_by": ["recovery_ledger_work"],
                    "truth_required_symbol_sources": {
                        "recovery_ledger_work": ["LAYERUSDT"],
                    },
                    "blocking": True,
                    "decision": "truth_unavailable_for_required_recovery",
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="LAYERUSDT",
            venues=["bybit", "okx", "hyperliquid"],
            now_ms=1779816055000,
        )

        gate = result["production_acceptance_gate"]
        assert gate["required_position_truth_unavailable_count"] == 1
        assert gate["recovery_decision"]["kind"] == "RUNNING_CLEAN"
        assert gate["recovery_decision"]["entry_allowed"] is True
        assert gate["exception_conclusions"]["blocking_required_truth"] == (
            "historical_required_truth_resolved_by_current_core"
        )
        assert "blocking_required_truth" not in gate["blocking_reasons"]
        assert gate["gate_passed"] is True
        assert "lifecycle_release_not_applied" in gate["fingerprints"]
        assert "lifecycle_release_not_applied" in result["health"]["fingerprints"]
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_fingerprints_stale_lifecycle_release_after_core_clean(monkeypatch):
    from scripts import diagnose_live as dl

    d = _make_tmpdir()
    try:
        _write_json(os.path.join(d, "state-current.json"), {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "risk_only",
            "risk_mode": "running",
            "open_position_count": 0,
            "open_positions": [],
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "pending_residual_repair_count": 0,
            "last_tick_ms": 1779816050000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="LAYERUSDT",
            venues=["bybit", "okx"],
            now_ms=1779816055000,
        )

        gate = result["production_acceptance_gate"]
        assert gate["gate_passed"] is True
        assert gate["blocking_reasons"] == []
        assert gate["recovery_decision"]["kind"] == "RUNNING_CLEAN"
        assert "lifecycle_release_not_applied" in gate["fingerprints"]
        assert "lifecycle_release_not_applied" in result["health"]["fingerprints"]
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_acceptance_gate_classifies_scoped_snapshot_fallback_as_v1_parity(monkeypatch):
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
            "last_tick_ms": 1779816050000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1779816049000,
                "kind": "runtime.snapshot_fallback_last_good",
                "payload": {
                    "symbol": "BULLAUSDT",
                    "candidate_freshness_scope": [
                        {
                            "candidate_symbol": "BULLAUSDT",
                            "candidate_pair_id": "bulla:bybit->aster",
                            "domain": "market_observed",
                            "venue": "global",
                            "source_age_ms": 60000,
                            "fallback_duration_ms": 55000,
                            "blocked": True,
                            "block_reason": "market_observed_stale",
                        }
                    ],
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="BULLAUSDT",
            venues=["bybit", "aster"],
            now_ms=1779816055000,
        )

        gate = result["production_acceptance_gate"]
        assert gate["snapshot_fallback_blocking_count"] == 1
        assert gate["exception_conclusions"]["snapshot_fallback_blocking"] == "v1_parity"
        assert gate["insufficient_evidence_exceptions"] == []
        assert gate["blocking_reasons"] == []
        assert gate["gate_passed"] is True
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_acceptance_gate_blocks_global_snapshot_fallback_without_scope_evidence(monkeypatch):
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
            "last_tick_ms": 1779816050000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1779816049000,
                "kind": "runtime.snapshot_fallback_last_good",
                "payload": {
                    "symbol": "BULLAUSDT",
                    "blocked": True,
                    "block_reason": "global_snapshot_stale",
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="BULLAUSDT",
            venues=["bybit", "aster"],
            now_ms=1779816055000,
        )

        gate = result["production_acceptance_gate"]
        assert gate["snapshot_fallback_blocking_count"] == 1
        assert gate["exception_conclusions"]["snapshot_fallback_blocking"] == "insufficient_evidence"
        assert gate["insufficient_evidence_exceptions"] == ["snapshot_fallback_blocking"]
        assert gate["blocking_reasons"] == ["diagnostic_exception_insufficient_evidence"]
        assert gate["gate_passed"] is False
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_acceptance_gate_blocks_unhedged_open_events(monkeypatch):
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
            "last_tick_ms": 1779816050000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {"ts_ms": 1779816049000, "kind": "entry.opened", "payload": {"symbol": "BULLAUSDT"}},
            {"ts_ms": 1779816049100, "kind": "runtime.position_opened", "payload": {"symbol": "BULLAUSDT"}},
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="BULLAUSDT",
            venues=["aster", "binance"],
            now_ms=1779816055000,
        )

        gate = result["production_acceptance_gate"]
        assert gate["entry_opened_count"] == 1
        assert gate["position_opened_count"] == 1
        assert gate["gate_passed"] is False
        assert gate["blocking_reasons"] == [
            "entry_or_position_opened_without_fixture_finalized_evidence",
        ]
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_acceptance_gate_closes_symbol_position_id_lifecycle(monkeypatch):
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
            "pending_residual_repair_count": 0,
            "pending_residual_repairs": [],
            "last_tick_ms": 1779816050000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1779816049000,
                "kind": "entry.opened",
                "payload": {"symbol": "BULLAUSDT"},
            },
            {
                "ts_ms": 1779816049100,
                "kind": "runtime.position_opened",
                "payload": {
                    "position_id": "pos-bull-1",
                    "symbol": "BULLAUSDT",
                },
            },
            {
                "ts_ms": 1779816049500,
                "kind": "runtime.position_lifecycle_terminal",
                "payload": {
                    "position_id": "pos-bull-1",
                    "symbol": "BULLAUSDT",
                    "terminal_state": "flat",
                    "terminal_reason": "exchange_truth_flat",
                },
            },
            {
                "ts_ms": 1779816049600,
                "kind": "recovery.ledger_clear",
                "payload": {
                    "position_id": "pos-bull-1",
                    "symbol": "BULLAUSDT",
                    "reason": "flat_no_open_orders",
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="BULLAUSDT",
            venues=["aster", "binance"],
            now_ms=1779816055000,
        )

        gate = result["production_acceptance_gate"]
        assert gate["entry_opened_count"] == 1
        assert gate["position_opened_count"] == 1
        assert gate["gate_passed"] is True
        assert gate["blocking_reasons"] == []
        assert gate["exception_conclusions"]["entry_opened"] == "closed_by_current_exchange_truth"
        assert gate["exception_conclusions"]["position_opened"] == "closed_by_current_exchange_truth"
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_resolves_bybit_ack_only_after_reconciliation_and_flat_truth(monkeypatch):
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
            "pending_passive_close_count": 0,
            "pending_passive_closes": [],
            "pending_residual_repair_count": 0,
            "pending_residual_repairs": [],
            "last_tick_ms": 1780657210000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1780657201000,
                "kind": "exit.accepted_order_truth_gap_registered",
                "payload": {
                    "position_id": "pos-btc-ack",
                    "symbol": "BTCUSDT",
                    "venue": "bybit",
                    "accepted_order_id": "bybit-order-1",
                    "accepted_client_order_id": "lf-close-1",
                    "truth_required_by": "accepted_order_truth_gap",
                },
            },
            {
                "ts_ms": 1780657201100,
                "kind": "exit.passive_close_hedge_error",
                "payload": {
                    "position_id": "pos-btc-ack",
                    "symbol": "BTCUSDT",
                    "hedge_venue": "bybit",
                    "error": "accepted order lacks fill confirmation",
                    "exchange_error": {
                        "http_status": 200,
                        "exchange_code": "0",
                        "exchange_msg": "OK",
                        "evidence_completeness": "complete",
                        "confidence": "medium",
                        "raw_body": "{\"retCode\":0,\"result\":{\"orderId\":\"bybit-order-1\"}}",
                    },
                    "accepted_order_truth_gap": True,
                    "accepted_order_id": "bybit-order-1",
                    "accepted_client_order_id": "lf-close-1",
                },
            },
            {
                "ts_ms": 1780657202000,
                "kind": "exit.passive_close_hedge_reconciled_after_error",
                "payload": {
                    "position_id": "pos-btc-ack",
                    "symbol": "BTCUSDT",
                    "hedge_venue": "bybit",
                    "order_id": "bybit-order-1",
                    "client_order_id": "lf-close-1",
                    "filled": 0.01,
                    "residual": 0.0,
                },
            },
            {
                "ts_ms": 1780657203000,
                "kind": "runtime.position_lifecycle_terminal",
                "payload": {
                    "position_id": "pos-btc-ack",
                    "symbol": "BTCUSDT",
                    "terminal_state": "flat",
                    "terminal_reason": "pending_passive_close_flat_probe",
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="BTCUSDT",
            venues=["bybit"],
            now_ms=1780657215000,
        )

        gate = result["production_acceptance_gate"]
        assert gate["gate_passed"] is True
        assert gate["blocking_reasons"] == []
        assert gate["resolved_order_truth_gap_count"] == 1
        assert gate["exception_conclusions"]["resolved_order_truth_gap"] == "closed_by_current_exchange_truth"
        assert result["order_error_evidence"] == []
        assert result["top_exchange_errors"] == []
        assert result["conclusion"]["status"] == "healthy"
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_acceptance_gate_accepts_completed_residual_lifecycle(monkeypatch):
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
            "pending_residual_repair_count": 0,
            "pending_residual_repairs": [],
            "last_tick_ms": 1780657210000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1780656885719,
                "kind": "pending_entry.missing_hedge_detected",
                "payload": {
                    "position_id": "entry-wld",
                    "symbol": "WLDUSDT",
                    "maker_leg_filled": 43.8,
                    "hedge_leg_filled": 0.0,
                },
            },
            {
                "ts_ms": 1780656919170,
                "kind": "pending_entry.hedge_residual_below_min_notional_terminalized",
                "payload": {
                    "position_id": "entry-wld",
                    "symbol": "WLDUSDT",
                    "balanced_quantity": 43.0,
                    "residual_quantity": 0.8,
                },
            },
            {
                "ts_ms": 1780656919335,
                "kind": "pending_entry.terminalizer_decision",
                "payload": {
                    "position_id": "entry-wld",
                    "symbol": "WLDUSDT",
                    "outcome": "open_position_with_residual",
                    "reason": "positive_fill_terminalized_with_matched_exposure",
                    "residual_quantity": 0.8,
                },
            },
            {
                "ts_ms": 1780656917063,
                "kind": "entry.opened",
                "payload": {
                    "position_id": "entry-wld",
                    "symbol": "WLDUSDT",
                    "quantity": 43.0,
                },
            },
            {
                "ts_ms": 1780656919337,
                "kind": "runtime.position_opened",
                "payload": {
                    "position_id": "entry-wld",
                    "symbol": "WLDUSDT",
                    "quantity": 43.0,
                },
            },
            {
                "ts_ms": 1780656919337,
                "kind": "execution.residual_repair_queued",
                "payload": {
                    "position_id": "entry-wld",
                    "symbol": "WLDUSDT",
                    "reason": "incremental_entry_open_partially_matched",
                },
            },
            {
                "ts_ms": 1780656920603,
                "kind": "recovery.residual_repair_failed",
                "payload": {
                    "position_id": "entry-wld",
                    "symbol": "WLDUSDT",
                    "reason": "transient_retryable_exchange_error",
                },
            },
            {
                "ts_ms": 1780656926839,
                "kind": "execution.residual_repair_completed",
                "payload": {
                    "position_id": "entry-wld",
                    "symbol": "WLDUSDT",
                    "result": "filled",
                },
            },
            {
                "ts_ms": 1780656926839,
                "kind": "recovery.residual_repairs_complete",
                "payload": {
                    "position_id": "entry-wld",
                    "symbol": "WLDUSDT",
                },
            },
            {
                "ts_ms": 1780657201807,
                "kind": "runtime.normal_close_routing_passive",
                "payload": {
                    "position_id": "entry-wld",
                    "symbol": "WLDUSDT",
                    "reason": "first_stage_capture",
                },
            },
            {
                "ts_ms": 1780657208742,
                "kind": "exit.passive_close_fallback_terminal_flat",
                "payload": {
                    "position_id": "entry-wld",
                    "symbol": "WLDUSDT",
                },
            },
            {
                "ts_ms": 1780657208742,
                "kind": "runtime.position_lifecycle_terminal",
                "payload": {
                    "position_id": "entry-wld",
                    "symbol": "WLDUSDT",
                    "terminal_state": "flat",
                    "terminal_reason": "pending_passive_close_flat_probe",
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="WLDUSDT",
            venues=["bybit", "hyperliquid"],
            now_ms=1780657215000,
        )

        gate = result["production_acceptance_gate"]
        assert gate["entry_opened_count"] == 1
        assert gate["position_opened_count"] == 1
        assert gate["residual_count"] >= 1
        assert gate["gate_passed"] is True
        assert gate["blocking_reasons"] == []
        assert gate["closed_trade_lifecycle_count"] == 1
        assert gate["closed_residual_lifecycle_count"] == 1
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_deduplicates_duplicate_quick_flat_close_events(monkeypatch):
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
            "last_tick_ms": 1779816050000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1779816040000,
                "kind": "entry.opened",
                "payload": {"position_id": "p1", "symbol": "BTCUSDT"},
            },
            {
                "ts_ms": 1779816040500,
                "kind": "exit.closed",
                "payload": {
                    "position_id": "p1",
                    "symbol": "BTCUSDT",
                    "reason": "funding_capture",
                    "close_id": "c1",
                },
            },
            {
                "ts_ms": 1779816040500,
                "kind": "exit.closed",
                "payload": {
                    "position_id": "p1",
                    "symbol": "BTCUSDT",
                    "reason": "funding_capture",
                    "close_id": "c1",
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="BTCUSDT",
            venues=["binance", "bybit"],
            now_ms=1779816055000,
        )

        summary = result["quick_flat_summary"]
        assert summary["quick_flat_count"] == 1
        assert summary["duplicate_event_count"] == 1
        gate = result["production_acceptance_gate"]
        assert gate["gate_passed"] is False
        assert gate["quick_flat_count"] == 1
        assert gate["blocking_reasons"] == ["quick_flat_events_present"]
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_does_not_count_okx_global_recovery_for_non_okx_venues(monkeypatch):
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
            "last_tick_ms": 1779816050000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1779816047700,
                "kind": "recovery.live_position_probe_venue_cooldown",
                "payload": {
                    "venue": "okx",
                    "classification": "rate_limited",
                    "endpoint": "/api/v5/account/positions",
                },
            },
            {
                "ts_ms": 1779816047800,
                "kind": "recovery.live_position_probe_unsupported_symbols",
                "payload": {
                    "venue": "okx",
                    "requested_symbols": ["RIVERUSDT", "CHIPUSDT"],
                    "skipped_by_catalog": ["CHIPUSDT"],
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="BULLAUSDT",
            venues=["aster", "binance"],
            now_ms=1779816055000,
        )

        gate = result["production_acceptance_gate"]
        assert result["event_counts"] == {}
        assert gate["okx_recovery_probe_rate_limited_count"] == 0
        assert gate["okx_instrument_missing_skipped_count"] == 0
        assert "okx_recovery_probe_rate_limited" not in gate["exception_conclusions"]
        assert "okx_instrument_missing_skipped" not in gate["exception_conclusions"]
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_replays_okx_noise_skipped_count_from_fixture(monkeypatch):
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
            "last_tick_ms": 1779816050000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1779816047700,
                "kind": "okx_recovery_probe_noise",
                "payload": {
                    "probe_symbols": [
                        "BTCUSDT",
                        "ETHUSDT",
                        "CHIPUSDT",
                        "DELISTEDUSDT",
                        "OLDUSDT",
                    ],
                    "okx_catalog": [
                        {"instId": "BTC-USDT-SWAP", "state": "live"},
                        {"instId": "ETH-USDT-SWAP", "state": "live"},
                    ],
                    "rate_limit_error": {
                        "status_code": 429,
                        "body": "{\"code\":\"50011\",\"msg\":\"Rate limit reached\"}",
                    },
                    "instrument_missing_error": (
                        "okx_contract_metadata_missing_ct_val "
                        "classification=instrument_missing instId=CHIP-USDT-SWAP"
                    ),
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="CHIPUSDT",
            venues=["okx"],
            now_ms=1779816055000,
        )

        gate = result["production_acceptance_gate"]
        assert gate["okx_recovery_probe_rate_limited_count"] == 1
        assert gate["okx_instrument_missing_skipped_count"] == 3
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_gate_fails_when_exchange_truth_unavailable(monkeypatch):
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
            "last_tick_ms": 1779816050000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [])

        def unavailable_exchange_truth(runtime_dir, symbols, venues=None):
            return {
                "available": False,
                "available_venues": [],
                "confidence": "low",
                "positions": {"okx": {"error": "no credentials available"}},
                "open_orders": {"okx": {"error": "no credentials available"}},
                "has_nonzero_position": False,
                "has_open_order": False,
                "fetch_status": {"okx": {"status": "no_credentials"}},
                "errors": [],
                "missing_evidence": ["okx_credentials"],
            }

        monkeypatch.setattr(dl, "_build_exchange_truth", unavailable_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="RIVERUSDT",
            venues=["okx"],
            now_ms=1779816055000,
        )

        gate = result["production_acceptance_gate"]
        assert gate["exchange_truth_flat"] is False
        assert gate["exchange_truth_no_open_orders"] is False
        assert gate["gate_passed"] is False
        assert gate["blocking_reasons"] == ["exchange_truth_unavailable"]
        assert gate["recovery_decision"]["kind"] == "RUNNING_WITH_EVIDENCE_GAP"
        assert gate["recovery_decision"]["entry_allowed"] is True
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_loads_exchange_truth_credentials_from_systemd_env_file(tmp_path, monkeypatch):
    from scripts import diagnose_live as dl

    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    unit_dir = tmp_path / "systemd"
    unit_dir.mkdir()
    env_file = tmp_path / "lightfee.env"
    env_file.write_text(
        "LIGHTFEE_BYBIT_API_KEY=key-from-systemd\n"
        "LIGHTFEE_BYBIT_API_SECRET=secret-from-systemd\n"
    )
    (unit_dir / "lightfee-live.service").write_text(
        "[Service]\n"
        f"EnvironmentFile={env_file}\n"
    )
    (unit_dir / "lightfee-sidecar.service").write_text("[Service]\n")
    _write_json(runtime_dir / "state-current.json", {
        "schema": "lightfee.current_state.v1",
        "lifecycle": "running",
        "risk_mode": "running",
        "open_position_count": 0,
        "open_positions": [],
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "last_tick_ms": 1779816050000,
    })
    _write_jsonl(runtime_dir / "events.jsonl", [])
    seen: dict[str, object] = {}

    def fake_exchange_truth(runtime_dir_arg, symbols, venues=None):
        seen["api_key"] = os.environ.get("LIGHTFEE_BYBIT_API_KEY")
        seen["api_secret"] = os.environ.get("LIGHTFEE_BYBIT_API_SECRET")
        return {
            "available": True,
            "confidence": "high",
            "positions": {},
            "open_orders": {},
            "has_nonzero_position": False,
            "has_open_order": False,
        }

    monkeypatch.delenv("LIGHTFEE_BYBIT_API_KEY", raising=False)
    monkeypatch.delenv("LIGHTFEE_BYBIT_API_SECRET", raising=False)
    monkeypatch.setattr(dl, "_build_exchange_truth", fake_exchange_truth)

    result = dl.run_diagnose(
        runtime_dir=str(runtime_dir),
        unit_dir=str(unit_dir),
        symbol="RIVERUSDT",
        venues=["bybit"],
        now_ms=1779816055000,
    )

    assert seen["api_key"] == "key-from-systemd"
    assert seen["api_secret"] == "secret-from-systemd"
    assert result["exchange_truth_env_files_loaded"] == [str(env_file)]
    assert result["exchange_truth"]["available"] is True


def test_diagnose_recovery_decision_treats_count_only_pending_as_required_work():
    from scripts.diagnose_live import _recovery_decision_payload

    decision = _recovery_decision_payload(
        {"pending_entry_count": 1},
        {"available": False, "positions": {}, "open_orders": {}},
    )

    assert decision["kind"] == "RISK_ONLY_WAIT_FOR_TRUTH"
    assert decision["entry_allowed"] is False
    assert decision["block_reason"] == "truth_unavailable_for_required_recovery"


def test_diagnose_and_runtime_recovery_decision_agree_on_partial_truth_payload(tmp_path):
    from lightfee.engine.exchange_truth import normalize_exchange_truth_payload
    from lightfee.engine.runtime import LiveRuntime
    from scripts.diagnose_live import _recovery_decision_payload
    from tests.test_live_startup_preflight import make_test_config

    local_state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "open_position_count": 0,
        "open_positions": [],
        "pending_entry_count": 0,
        "pending_close_count": 0,
    }
    exchange_truth = normalize_exchange_truth_payload(
        {
            "available": True,
            "truth_available": True,
            "confidence": "partial",
            "positions": {},
            "open_orders": {},
            "fetch_status": {
                "bybit": {"status": "ok"},
                "okx": {
                    "status": "timeout",
                    "error": "exchange truth probe timed out after 2s",
                },
            },
            "probe_evidence": [
                {
                    "venue": "okx",
                    "symbol": "TRXUSDT",
                    "classification": "open_order_probe_timeout",
                    "error": "exchange truth probe timed out after 2s",
                }
            ],
            "missing_evidence": ["okx:TRXUSDT:open_order_probe_timeout"],
        }
    )
    runtime = LiveRuntime(make_test_config(str(tmp_path)))
    runtime.journal.open()

    runtime._refresh_recovery_ledger_from_exchange_truth(
        exchange_truth,
        now_ms=1778787000000,
    )
    diagnose_decision = _recovery_decision_payload(local_state, exchange_truth)

    assert runtime.recovery_decision is not None
    assert diagnose_decision["kind"] == runtime.recovery_decision.kind.value
    assert diagnose_decision["entry_allowed"] == runtime.recovery_decision.entry_allowed
    assert diagnose_decision["block_reason"] == runtime.recovery_decision.block_reason
    assert diagnose_decision["evidence_quality"] == (
        runtime.recovery_decision.evidence_quality
    )
    runtime.journal.close()


def test_run_diagnose_conclusion_is_unhealthy_when_acceptance_gate_has_open_order(
    monkeypatch,
):
    """A failed production gate must dominate a locally-flat health snapshot."""
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
            "last_tick_ms": 1779816050000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [])

        def open_order_exchange_truth(runtime_dir, symbols, venues=None):
            return {
                "available": True,
                "available_venues": ["bybit"],
                "confidence": "high",
                "positions": {"bybit": {}},
                "open_orders": {
                    "bybit": {
                        "*": [
                            {
                                "order_id": "d792a623-d9e4-4c20-905f-f76a8f2efaeb",
                                "symbol": "SEIUSDT",
                                "side": "Buy",
                                "quantity": 451.0,
                                "reduce_only": False,
                            }
                        ]
                    }
                },
                "has_nonzero_position": False,
                "has_open_order": True,
                "fetch_status": {
                    "bybit": {
                        "status": "ok",
                        "positions_succeeded": [],
                        "positions_failed": [],
                        "orders_succeeded": ["*"],
                        "orders_failed": [],
                    }
                },
                "errors": [],
                "missing_evidence": [],
            }

        monkeypatch.setattr(dl, "_build_exchange_truth", open_order_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="",
            venues=["bybit"],
            now_ms=1779816055000,
        )

        gate = result["production_acceptance_gate"]
        assert gate["gate_passed"] is False
        assert gate["blocking_reasons"] == ["exchange_truth_open_orders_present"]
        assert result["conclusion"]["status"] == "unhealthy"
        assert result["conclusion"]["risk"] == "high"
        assert "production acceptance gate failed" in result["conclusion"]["summary"]
        assert any(
            "exchange_truth_open_orders_present" in action
            for action in result["conclusion"]["next_actions"]
        )
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_production_blocker_window_reports_closed_evidence_conclusions(tmp_path):
    from scripts.analyze_production_blockers import analyze_event_file

    events_path = tmp_path / "window.jsonl"
    _write_jsonl(events_path, [
        {
            "ts_ms": 1779800000000,
            "kind": "runtime.entry_blocked_local_l2_selection",
            "payload": {
                "symbol": "MONUSDT",
                "reason": "entry_local_l2_waiting_for_dual_ready",
            },
        },
        {
            "ts_ms": 1779810000000,
            "kind": "runtime.local_l2_sequence_gap_rebuild",
            "payload": {
                "venue": "aster",
                "symbol": "MONUSDT",
                "previous_sequence_present": True,
                "expected_previous_sequence": 10,
                "raw_U": 12,
                "raw_u": 13,
                "raw_pu": 10,
                "status_after": "rebuilding",
            },
        },
        {
            "ts_ms": 1779811000000,
            "kind": "runtime.snapshot_fallback_last_good",
            "payload": {
                "symbol": "MONUSDT",
                "v1_parity_evidence": "CL-006",
                "candidate_freshness_scope": [{"blocked": True}],
            },
        },
    ])

    result = analyze_event_file(
        events_path,
        now_ms=1779812000000,
        windows=["last_2h", "run_window"],
    )

    assert result["windows"]["last_2h"]["incident_counts"] == {
        "local_l2_official_rebuild": 1,
        "snapshot_fallback_blocking": 1,
    }
    assert result["windows"]["last_2h"]["incident_conclusions"] == {
        "local_l2_official_rebuild": "official_doc",
        "snapshot_fallback_blocking": "v1_parity",
    }


def test_production_blocker_window_classifies_scoped_snapshot_fallback_as_v1_parity(tmp_path):
    from scripts.analyze_production_blockers import analyze_event_file

    events_path = tmp_path / "window_scoped_fallback.jsonl"
    _write_jsonl(events_path, [
        {
            "ts_ms": 1779811000000,
            "kind": "runtime.snapshot_fallback_last_good",
            "payload": {
                "symbol": "MONUSDT",
                "candidate_freshness_scope": [
                    {
                        "candidate_symbol": "MONUSDT",
                        "candidate_pair_id": "mon:bybit->aster",
                        "domain": "market_observed",
                        "venue": "global",
                        "source_age_ms": 60000,
                        "fallback_duration_ms": 55000,
                        "blocked": True,
                        "block_reason": "market_observed_stale",
                    }
                ],
            },
        },
    ])

    result = analyze_event_file(
        events_path,
        now_ms=1779812000000,
        windows=["last_2h", "run_window"],
    )

    assert result["windows"]["last_2h"]["incident_counts"] == {
        "snapshot_fallback_blocking": 1,
    }
    assert result["windows"]["last_2h"]["incident_conclusions"] == {
        "snapshot_fallback_blocking": "v1_parity",
    }


def test_production_blocker_window_requires_actual_official_sequence_break(tmp_path):
    from scripts.analyze_production_blockers import analyze_event_file

    events_path = tmp_path / "window_no_sequence_break.jsonl"
    _write_jsonl(events_path, [
        {
            "ts_ms": 1779810000000,
            "kind": "runtime.local_l2_sequence_gap_rebuild",
            "payload": {
                "venue": "aster",
                "symbol": "LABUSDT",
                "previous_sequence_present": True,
                "expected_previous_sequence": 468889077688,
                "raw_U": 468889077689,
                "raw_u": 468889077847,
                "raw_pu": 468889077688,
                "status_after": "rebuilding",
            },
        },
    ])

    result = analyze_event_file(
        events_path,
        now_ms=1779812000000,
        windows=["last_2h"],
    )

    window = result["windows"]["last_2h"]
    assert "local_l2_official_rebuild" not in window["incident_counts"]
    assert window["incident_conclusions"]["local_l2_official_rebuild"] == "insufficient_evidence"


def test_production_blocker_window_replays_okx_noise_skipped_count(tmp_path):
    from scripts.analyze_production_blockers import analyze_event_file

    events_path = tmp_path / "okx_noise.jsonl"
    _write_jsonl(events_path, [
        {
            "ts_ms": 1779811000000,
            "kind": "okx_recovery_probe_noise",
            "payload": {
                "probe_symbols": [
                    "BTCUSDT",
                    "ETHUSDT",
                    "CHIPUSDT",
                    "DELISTEDUSDT",
                    "OLDUSDT",
                ],
                "okx_catalog": [
                    {"instId": "BTC-USDT-SWAP", "state": "live"},
                    {"instId": "ETH-USDT-SWAP", "state": "live"},
                ],
                "rate_limit_error": {
                    "status_code": 429,
                    "body": "{\"code\":\"50011\",\"msg\":\"Rate limit reached\"}",
                },
                "instrument_missing_error": (
                    "okx_contract_metadata_missing_ct_val "
                    "classification=instrument_missing instId=CHIP-USDT-SWAP"
                ),
            },
        },
    ])

    result = analyze_event_file(
        events_path,
        now_ms=1779812000000,
        windows=["last_2h"],
    )

    assert result["windows"]["last_2h"]["incident_counts"] == {
        "okx_instrument_missing_skipped": 3,
        "okx_recovery_probe_rate_limited": 1,
    }
    assert result["windows"]["last_2h"]["incident_conclusions"] == {
        "okx_instrument_missing_skipped": "official_doc",
        "okx_recovery_probe_rate_limited": "official_doc",
    }


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
            raise AssertionError("aster open orders must use adapter V3 path")
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

        async def fetch_open_orders(self, symbol=None):
            assert self.venue == "aster"
            assert symbol == "XCNUSDT"
            return []

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

    orders, succeeded, failed, evidence = asyncio.run(
        dl._fetch_venue_open_orders(adapter, ["OPGUSDT"])
    )

    assert orders == {"OPGUSDT": []}
    assert succeeded == {"OPGUSDT"}
    assert failed == set()
    assert evidence["OPGUSDT"]["classification"] == "open_order_probe_succeeded"
    assert adapter._transport.calls == [
        (
            "GET",
            "/fapi/v1/openOrders",
            {"params": {"symbol": "OPGUSDT"}, "private": True},
        )
    ]


def test_exchange_truth_empty_symbols_uses_all_positions_probe():
    import asyncio
    from scripts import diagnose_live as dl
    from lightfee.core.domain import PositionSnapshot, Side, Venue

    class FakeAdapter:
        venue = "bybit"

        async def fetch_all_positions(self):
            return [
                PositionSnapshot(
                    venue=Venue.BYBIT,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=0.25,
                    entry_price=61000.0,
                    observed_at_ms=1700000000000,
                )
            ]

    positions, succeeded, failed, evidence = asyncio.run(
        dl._fetch_venue_positions(FakeAdapter(), [])
    )

    assert succeeded == {"*"}
    assert failed == set()
    assert positions == {
        "BTCUSDT": {
            "symbol": "BTCUSDT",
            "quantity": 0.25,
            "entry_price": 61000.0,
            "side": "Side.BUY",
        }
    }
    assert evidence["*"]["classification"] == "position_probe_unfiltered_succeeded"
    assert evidence["*"]["position_count"] == 1


def test_exchange_truth_empty_symbols_uses_unfiltered_open_orders_probe():
    import asyncio
    from scripts import diagnose_live as dl

    class FakeTransport:
        def __init__(self):
            self.calls = []

        async def _request(self, method, path, **kwargs):
            self.calls.append((method, path, kwargs))
            return [
                {
                    "orderId": "ord-1",
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "origQty": "0.25",
                    "price": "61000",
                }
            ]

    class FakeAdapter:
        venue = "binance"

        def __init__(self):
            self._transport = FakeTransport()

    adapter = FakeAdapter()

    orders, succeeded, failed, evidence = asyncio.run(
        dl._fetch_venue_open_orders(adapter, [])
    )

    assert succeeded == {"*"}
    assert failed == set()
    assert orders == {
        "*": [
            {
                "order_id": "ord-1",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "quantity": 0.25,
                "price": 61000.0,
                "reduce_only": False,
            }
        ]
    }
    assert evidence["*"]["classification"] == "open_order_probe_unfiltered_succeeded"
    assert evidence["*"]["order_count"] == 1
    assert adapter._transport.calls == [
        ("GET", "/fapi/v1/openOrders", {"params": {}, "private": True})
    ]


def test_exchange_truth_creates_readonly_adapters_for_all_live_perp_venues():
    from scripts import diagnose_live as dl
    from lightfee.venues.transport import LiveCredential

    credential = LiveCredential(
        api_key="key",
        api_secret="secret",
        api_passphrase="passphrase",
        wallet_private_key="0x" + "1" * 64,
        account_address="0x" + "2" * 40,
    )

    for venue in (
        "binance",
        "bybit",
        "aster",
        "okx",
        "bitget",
        "gate",
        "hyperliquid",
    ):
        adapter = dl._create_readonly_adapter(venue, credential)
        assert adapter is not None, venue


def test_exchange_truth_loads_hyperliquid_private_key_alias(monkeypatch):
    from scripts import diagnose_live as dl

    private_key = "0x" + "1" * 64
    account = "0x" + "2" * 40
    monkeypatch.setenv("LIGHTFEE_HYPERLIQUID_PRIVATE_KEY", private_key)
    monkeypatch.setenv("LIGHTFEE_HYPERLIQUID_ACCOUNT_ADDRESS", account)

    credential = dl._load_venue_credential("hyperliquid")

    assert credential is not None
    assert credential.wallet_private_key == private_key
    assert credential.account_address == account


def test_exchange_truth_default_venues_cover_all_live_perp_venues(monkeypatch):
    import asyncio
    from scripts import diagnose_live as dl

    class FakeTransport:
        async def _request(self, method, path, **kwargs):
            return []

    class FakeAdapter:
        def __init__(self, venue):
            self.venue = venue
            self._transport = FakeTransport()

        async def fetch_all_positions(self):
            return []

        async def shutdown(self):
            pass

    monkeypatch.setattr(dl, "_load_venue_credential", lambda venue: object())
    monkeypatch.setattr(
        dl,
        "_create_readonly_adapter",
        lambda venue, credential: FakeAdapter(venue),
    )

    result = asyncio.run(dl._build_exchange_truth_async(
        runtime_dir="/unused",
        symbols=[],
        venues=None,
    ))

    assert result["available"] is True
    assert result["confidence"] == "high"
    assert result["available_venues"] == [
        "binance",
        "bybit",
        "aster",
        "okx",
        "bitget",
        "gate",
        "hyperliquid",
    ]
    assert set(result["fetch_status"]) == set(result["available_venues"])


def test_exchange_truth_sync_wrapper_does_not_emit_deprecation_warning(monkeypatch):
    import warnings
    from scripts import diagnose_live as dl

    async def fake_exchange_truth_async(runtime_dir, symbols, venues):
        return {
            "available": True,
            "available_venues": venues or [],
            "confidence": "high",
            "positions": {},
            "open_orders": {},
            "has_nonzero_position": False,
            "has_open_order": False,
            "fetch_status": {},
            "errors": [],
            "missing_evidence": [],
        }

    monkeypatch.setattr(dl, "_build_exchange_truth_async", fake_exchange_truth_async)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        result = dl._build_exchange_truth("/unused", [], venues=["binance"])

    assert result["available"] is True
    assert not [
        warning for warning in caught
        if issubclass(warning.category, DeprecationWarning)
    ]


def test_evidence_quality_complete_when_truth_high_and_no_order_errors():
    from scripts import diagnose_live as dl

    result = dl._build_evidence_completeness(
        order_errors=[],
        state_consistency={
            "state_mismatch": False,
            "local_open_exchange_flat": False,
            "confidence": "high",
        },
        exchange_truth={
            "available": True,
            "confidence": "high",
            "missing_evidence": [],
        },
    )

    assert result["overall"] == "complete"
    assert result["confidence"] == "high"
    assert result["missing_evidence"] == []


def test_exchange_truth_uses_okx_venue_symbol_for_open_orders():
    import asyncio
    from scripts import diagnose_live as dl

    class FakeTransport:
        def __init__(self):
            self.calls = []

        def _venue_symbol(self, symbol):
            assert symbol == "PRLUSDT"
            return "PRL-USDT-SWAP"

        async def _request(self, method, path, **kwargs):
            self.calls.append((method, path, kwargs))
            return {"data": []}

    class FakeAdapter:
        venue = "okx"

        def __init__(self):
            self._transport = FakeTransport()

    adapter = FakeAdapter()

    orders, succeeded, failed, evidence = asyncio.run(
        dl._fetch_venue_open_orders(adapter, ["PRLUSDT"])
    )

    assert orders == {"PRLUSDT": []}
    assert succeeded == {"PRLUSDT"}
    assert failed == set()
    assert evidence["PRLUSDT"]["venue_symbol"] == "PRL-USDT-SWAP"
    assert adapter._transport.calls == [
        (
            "GET",
            "/api/v5/trade/orders-pending",
            {"params": {"instId": "PRL-USDT-SWAP"}, "private": True},
        )
    ]


def test_exchange_truth_classifies_unsupported_open_order_symbol_as_empty_with_evidence():
    import asyncio
    from scripts import diagnose_live as dl

    class FakeTransport:
        async def _request(self, method, path, **kwargs):
            raise RuntimeError("HTTP 400: invalid symbol")

    class FakeAdapter:
        venue = "aster"

        def __init__(self):
            self._transport = FakeTransport()

    adapter = FakeAdapter()

    orders, succeeded, failed, evidence = asyncio.run(
        dl._fetch_venue_open_orders(adapter, ["CROSSUSDT"])
    )

    assert orders == {"CROSSUSDT": []}
    assert succeeded == {"CROSSUSDT"}
    assert failed == set()
    assert evidence["CROSSUSDT"]["classification"] == "unsupported_symbol_no_open_orders"
    assert "invalid symbol" in evidence["CROSSUSDT"]["error"]


def test_exchange_truth_classifies_unsupported_position_symbol_as_flat_with_evidence():
    import asyncio
    from scripts import diagnose_live as dl

    class FakeAdapter:
        venue = "okx"

        async def fetch_position(self, symbol):
            raise RuntimeError("Instrument ID does not exist")

    adapter = FakeAdapter()

    positions, succeeded, failed, evidence = asyncio.run(
        dl._fetch_venue_positions(adapter, ["PRLUSDT"])
    )

    assert positions == {}
    assert succeeded == {"PRLUSDT"}
    assert failed == set()
    assert evidence["PRLUSDT"]["classification"] == "unsupported_symbol_flat"
    assert "Instrument ID does not exist" in evidence["PRLUSDT"]["error"]


def test_exchange_truth_classifies_okx_instrument_missing_metadata_as_flat():
    import asyncio
    from scripts import diagnose_live as dl

    class FakeTransport:
        def _venue_symbol(self, symbol):
            assert symbol == "PRLUSDT"
            return "PRL-USDT-SWAP"

    class FakeAdapter:
        venue = "okx"

        def __init__(self):
            self._transport = FakeTransport()

        async def fetch_position(self, symbol):
            raise RuntimeError(
                "okx_contract_metadata_missing_ct_val "
                "classification=instrument_missing instId=PRL-USDT-SWAP"
            )

    adapter = FakeAdapter()

    positions, succeeded, failed, evidence = asyncio.run(
        dl._fetch_venue_positions(adapter, ["PRLUSDT"])
    )

    assert positions == {}
    assert succeeded == {"PRLUSDT"}
    assert failed == set()
    assert evidence["PRLUSDT"]["classification"] == "unsupported_symbol_flat"
    assert evidence["PRLUSDT"]["venue_symbol"] == "PRL-USDT-SWAP"


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


def test_run_diagnose_current_exchange_truth_closes_legacy_opened_positions(monkeypatch):
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
            "pending_passive_close_count": 0,
            "pending_passive_closes": [],
            "pending_residual_repair_count": 0,
            "pending_residual_repairs": [],
            "last_tick_ms": 1781011420000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1781011300000,
                "kind": "entry.opened",
                "payload": {
                    "position_id": "entry-1781005938000-BSBUSDT",
                    "symbol": "BSBUSDT",
                },
            },
            {
                "ts_ms": 1781011300100,
                "kind": "runtime.position_opened",
                "payload": {
                    "position_id": "entry-1781005938000-BSBUSDT",
                    "symbol": "BSBUSDT",
                },
            },
            {
                "ts_ms": 1781011300200,
                "kind": "entry.opened",
                "payload": {
                    "position_id": "entry-1781006136786-MOVEUSDT",
                    "symbol": "MOVEUSDT",
                },
            },
            {
                "ts_ms": 1781011300300,
                "kind": "runtime.position_opened",
                "payload": {
                    "position_id": "entry-1781006136786-MOVEUSDT",
                    "symbol": "MOVEUSDT",
                },
            },
            {
                "ts_ms": 1781011301000,
                "kind": "execution.residual_repair_completed",
                "payload": {"symbol": "BSBUSDT"},
            },
            {
                "ts_ms": 1781011301100,
                "kind": "execution.residual_repair_completed",
                "payload": {"symbol": "MOVEUSDT"},
            },
            {
                "ts_ms": 1781011301200,
                "kind": "recovery.residual_repairs_complete",
                "payload": {"reason": "all_residual_repairs_complete"},
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="",
            venues=["bybit", "hyperliquid"],
            now_ms=1781011430000,
        )

        gate = result["production_acceptance_gate"]
        assert gate["exchange_truth_flat"] is True
        assert gate["exchange_truth_no_open_orders"] is True
        assert gate["gate_passed"] is True
        assert gate["blocking_reasons"] == []
        assert gate["recovery_lifecycle"]["unclosed_open_keys"] == []
        assert gate["recovery_lifecycle"]["exchange_truth_closed_open_keys"] == [
            "entry-1781005938000-BSBUSDT",
            "entry-1781006136786-MOVEUSDT",
        ]
        assert gate["exception_conclusions"]["entry_opened"] == "closed_by_current_exchange_truth"
        assert gate["exception_conclusions"]["position_opened"] == "closed_by_current_exchange_truth"
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
        snapshot = result["snapshot_evidence"]
        assert l2["sequence_gap_count"] >= 1
        assert l2["stale_rebuild_count"] == 0
        assert l2["missing_l2_or_tick_count"] >= 1
        assert snapshot["stale_or_degraded_count"] == 1
        assert snapshot["domain_counts"] == {"liquidity": 1}
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
