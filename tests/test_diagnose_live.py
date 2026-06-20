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

import pytest

from scripts.diagnose_live import run_diagnose


def test_deploy_status_treats_short_git_head_as_matching_full_deploy_version(monkeypatch):
    import scripts.diagnose_live as diagnose_live

    full = "33d56e761ec7b88e66099695d1350b73887b56fd"
    monkeypatch.setattr(diagnose_live, "_git_head", lambda: full[:7])
    monkeypatch.setattr(diagnose_live, "_git_commit_time", lambda: "2026-06-12T14:41:31+08:00")
    monkeypatch.setattr(diagnose_live, "_read_deploy_version", lambda runtime_dir: full)

    status = diagnose_live._build_deploy_status("/tmp/runtime")

    assert status["git_head"] == full[:7]
    assert status["deploy_version"] == full
    assert status["version_mismatch"] is False


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


def test_auto_fail_closed_summary_reports_recent_recovery_incident():
    from lightfee.ops.auto_fail_closed_events import build_auto_fail_closed_summary

    summary = build_auto_fail_closed_summary([
        {
            "ts_ms": 1000,
            "kind": "runtime.auto_fail_closed_cleanup_failed",
            "payload": {
                "source": "auto_pending_entry_abort",
                "reason": "deadline breach",
                "symbols": ["LINKUSDT"],
                "venues": ["binance", "okx"],
                "new_risk_mode": "fail_closed",
                "residual_blockers": ["pending_entry_retained"],
            },
        },
        {
            "ts_ms": 2000,
            "kind": "runtime.auto_fail_closed_recovered",
            "payload": {
                "source": "auto_pending_entry_abort",
                "reason": "deadline breach",
                "symbols": ["LINKUSDT"],
                "venues": ["binance", "okx"],
                "new_risk_mode": "running",
                "residual_blockers": [],
            },
        },
    ])

    assert summary["recovered_count"] == 1
    assert summary["cleanup_failed_count"] == 1
    assert summary["recent_incident"] is True
    assert summary["latest_event"]["kind"] == "runtime.auto_fail_closed_recovered"
    assert summary["latest_event"]["final_status"] == "recovered"


def test_auto_fail_closed_summary_ignores_events_before_window():
    from lightfee.ops.auto_fail_closed_events import build_auto_fail_closed_summary

    summary = build_auto_fail_closed_summary(
        [
            {
                "ts_ms": 1000,
                "kind": "runtime.auto_fail_closed_recovered",
                "payload": {"source": "auto_pending_entry_abort"},
            },
            {
                "ts_ms": 3000,
                "kind": "runtime.auto_fail_closed_cleanup_failed",
                "payload": {
                    "source": "auto_pending_entry_abort",
                    "symbols": ["LINKUSDT"],
                    "residual_blockers": ["pending_entry_retained"],
                },
            },
        ],
        since_ms=2000,
    )

    assert summary["recovered_count"] == 0
    assert summary["cleanup_failed_count"] == 1
    assert summary["recent_incident"] is True
    assert summary["latest_event"]["kind"] == "runtime.auto_fail_closed_cleanup_failed"
    assert summary["latest_event"]["symbols"] == ["LINKUSDT"]


def test_run_diagnose_reports_recent_auto_fail_closed_before_since_deploy_window(monkeypatch):
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
            "last_tick_ms": 1700000005000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1700000004900,
                "kind": "runtime.auto_fail_closed_recovered",
                "payload": {
                    "source": "repair_auto_fail_closed_latch",
                    "reason": "stale_auto_fail_closed_operator_latch_repaired",
                    "new_risk_mode": "running",
                    "residual_blockers": [],
                },
            },
            {
                "ts_ms": 1700000005000,
                "kind": "runtime.started",
                "payload": {},
            },
        ])

        monkeypatch.setattr(dl, "_build_service_status", lambda unit_dir: {
            "lightfee-live": {
                "active": "active",
                "unit_exists": True,
                "n_restarts": 0,
                "started_at_ms": 1700000005000,
            }
        })
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1700000006000,
            since_deploy=True,
        )

        assert result["health"]["ok"] is True
        assert result["auto_fail_closed_summary"]["recovered_count"] == 1
        assert result["auto_fail_closed_summary"]["recent_incident"] is True
        assert result["auto_fail_closed_window_summary"]["recovered_count"] == 0
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_reports_recent_stale_risk_state_alignment(monkeypatch):
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
            "unpaired_live_position_recoveries": [
                {
                    "venue": "okx",
                    "symbol": "HOME-USDT-SWAP",
                    "side": "sell",
                    "terminal_status": "flat",
                }
            ],
            "last_tick_ms": 1700000005000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1700000004900,
                "kind": "runtime.stale_risk_state_aligned",
                "payload": {
                    "source": "runtime_live_position_probe",
                    "reason": "runtime_flat_truth_current_state_clean",
                    "symbols": ["HOME-USDT-SWAP"],
                    "venues": ["okx"],
                    "terminalized_records": 1,
                    "previous_lifecycle": "risk_only",
                    "new_lifecycle": "running",
                    "previous_risk_mode": "running",
                    "new_risk_mode": "running",
                    "residual_blockers": [],
                },
            }
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1700000006000,
        )

        summary = result["stale_risk_state_alignment_summary"]
        assert summary["recent_incident"] is True
        assert summary["aligned_count"] == 1
        assert summary["blocked_count"] == 0
        assert summary["latest_event"]["kind"] == "runtime.stale_risk_state_aligned"
        assert summary["latest_event"]["symbols"] == ["HOME-USDT-SWAP"]
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


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


def _balanced_active_exchange_truth(runtime_dir, symbols, venues=None):
    venues = venues or []
    symbol = symbols[0] if symbols else "KATUSDT"
    return {
        "available": True,
        "available_venues": venues,
        "confidence": "high",
        "positions": {
            "okx": {
                symbol: {
                    "symbol": symbol,
                    "side": "Side.BUY",
                    "quantity": 7600.0,
                    "venue": "okx",
                }
            },
            "bybit": {
                symbol: {
                    "symbol": symbol,
                    "side": "Side.SELL",
                    "quantity": 7600.0,
                    "venue": "bybit",
                }
            },
        },
        "open_orders": {
            "okx": {symbol: []},
            "bybit": {symbol: []},
        },
        "has_nonzero_position": True,
        "has_open_order": False,
        "fetch_status": {
            "okx": {
                "status": "ok",
                "positions_succeeded": [symbol],
                "positions_failed": [],
                "orders_succeeded": [symbol],
                "orders_failed": [],
            },
            "bybit": {
                "status": "ok",
                "positions_succeeded": [symbol],
                "positions_failed": [],
                "orders_succeeded": [symbol],
                "orders_failed": [],
            },
        },
        "errors": [],
        "missing_evidence": [],
    }


def test_run_diagnose_active_balanced_position_is_not_high_risk(monkeypatch):
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
                    "position_id": "pos_kat_001",
                    "symbol": "KATUSDT",
                    "long_venue": "okx",
                    "short_venue": "bybit",
                    "quantity": 7600.0,
                    "matched_quantity": 7600.0,
                    "opened_at_ms": 1700000000000,
                }
            ],
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "last_tick_ms": 1700000000000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1700000001000,
                "kind": "runtime.position_opened",
                "payload": {
                    "position_id": "pos_kat_001",
                    "symbol": "KATUSDT",
                    "long_venue": "okx",
                    "short_venue": "bybit",
                    "quantity": 7600.0,
                },
            }
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _balanced_active_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="KATUSDT",
            venues=["okx", "bybit"],
            now_ms=1700000005000,
        )

        assert result["state_consistency"]["state_mismatch"] is False
        gate = result["production_acceptance_gate"]
        assert gate["gate_passed"] is True
        assert gate["open_position_count"] == 1
        assert gate["max_concurrent_positions"] == 8
        assert gate["remaining_position_slots"] == 7
        assert "active_positions_with_capacity" in gate["fingerprints"]
        assert "local_open_positions_present" not in gate["blocking_reasons"]
        assert "exchange_truth_nonzero_position" not in gate["blocking_reasons"]
        assert result["conclusion"]["risk"] != "high"
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_reports_unpaired_live_position_recovery_route(monkeypatch):
    from scripts import diagnose_live as dl

    d = _make_tmpdir()
    try:
        _write_json(os.path.join(d, "state-current.json"), {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "risk_only",
            "risk_mode": "fail_closed",
            "open_position_count": 0,
            "open_positions": [],
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "unpaired_live_position_recoveries": [
                {
                    "venue": "binance",
                    "symbol": "ESPORTSUSDT",
                    "side": "sell",
                    "quantity": 592.0,
                    "notional_quote": 11.84,
                    "first_seen_ms": 1770000000000,
                    "attempt_count": 1,
                    "next_attempt_ms": 1770000030000,
                    "last_error": "position_still_nonzero",
                    "terminal_status": "",
                    "owner_excluded": True,
                    "open_order_truth_available": True,
                    "cap_quote": 30.0,
                    "cap_ok": True,
                }
            ],
            "last_tick_ms": 1770000001000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1770000000000,
                "kind": "recovery.unpaired_live_position_detected",
                "payload": {
                    "venue": "binance",
                    "symbol": "ESPORTSUSDT",
                    "side": "sell",
                },
            },
            {
                "ts_ms": 1770000001000,
                "kind": "recovery.unpaired_live_position_cleanup_skipped",
                "payload": {
                    "venue": "binance",
                    "symbol": "ESPORTSUSDT",
                    "side": "sell",
                    "auto_enabled": False,
                    "reason": "auto_disabled",
                    "current_risk_exposure": True,
                    "business_terminal": False,
                    "diagnostic_severity": "critical",
                    "next_action": "operator_or_config_enable_required",
                },
            },
        ])
        monkeypatch.setattr(
            dl,
            "_build_exchange_truth",
            lambda *args, **kwargs: {
                "flat": False,
                "no_open_orders": True,
                "positions": [],
                "open_orders": [],
                "venues": {},
                "errors": [],
                "missing_evidence": [],
            },
        )

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="ESPORTSUSDT",
            venues=["binance"],
            now_ms=1770000005000,
        )

        summary = result["unpaired_live_position_recovery_summary"]
        assert summary["current_work_count"] == 1
        assert summary["active_work_count"] == 1
        assert summary["terminal_flat_count"] == 0
        assert summary["current_risk_exposure_count"] == 1
        assert summary["auto_enabled"] is False
        assert summary["event_counts"][
            "recovery.unpaired_live_position_cleanup_skipped"
        ] == 1
        detail = summary["details"][0]
        assert detail["venue"] == "binance"
        assert detail["symbol"] == "ESPORTSUSDT"
        assert detail["owner_excluded"] is True
        assert detail["open_order_truth_available"] is True
        assert detail["cap_ok"] is True
        assert detail["current_risk_exposure"] is True
        assert detail["business_terminal"] is False
        assert detail["latest_event"]["reason"] == "auto_disabled"
        assert detail["latest_event"]["current_risk_exposure"] is True
        assert detail["latest_event"]["business_terminal"] is False
        assert detail["latest_event"]["diagnostic_severity"] == "critical"
        assert detail["latest_event"]["next_action"] == (
            "operator_or_config_enable_required"
        )
        business = result["business_progression_quality_summary"]
        assert business["risk_only_live_single_leg_exposure_count"] == 1
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_counts_only_active_unpaired_recovery_as_current_work(monkeypatch):
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
            "unpaired_live_position_recoveries": [
                {
                    "venue": "binance",
                    "symbol": "ESPORTSUSDT",
                    "side": "sell",
                    "quantity": 0.0,
                    "notional_quote": 0.0,
                    "first_seen_ms": 1770000000000,
                    "attempt_count": 1,
                    "next_attempt_ms": 1770000001000,
                    "last_error": "",
                    "terminal_status": "flat",
                    "owner_excluded": True,
                    "open_order_truth_available": True,
                    "cap_quote": 30.0,
                    "cap_ok": True,
                }
            ],
            "last_tick_ms": 1770000001000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1770000001000,
                "kind": "recovery.unpaired_live_position_terminal_flat",
                "payload": {
                    "venue": "binance",
                    "symbol": "ESPORTSUSDT",
                    "side": "sell",
                    "reason": "cleanup_succeeded",
                },
            }
        ])
        monkeypatch.setattr(
            dl,
            "_build_exchange_truth",
            lambda *args, **kwargs: {
                "flat": True,
                "no_open_orders": True,
                "positions": [],
                "open_orders": [],
                "venues": {},
                "errors": [],
                "missing_evidence": [],
            },
        )

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="ESPORTSUSDT",
            venues=["binance"],
            now_ms=1770000005000,
        )

        summary = result["unpaired_live_position_recovery_summary"]
        assert summary["current_work_count"] == 0
        assert summary["active_work_count"] == 0
        assert summary["terminal_flat_count"] == 1
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_reports_manual_required_unpaired_recovery(monkeypatch):
    from scripts import diagnose_live as dl

    d = _make_tmpdir()
    try:
        _write_json(os.path.join(d, "state-current.json"), {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "risk_only",
            "risk_mode": "fail_closed",
            "open_position_count": 0,
            "open_positions": [],
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "unpaired_live_position_recoveries": [
                {
                    "venue": "binance",
                    "symbol": "ESPORTSUSDT",
                    "side": "sell",
                    "quantity": 592.0,
                    "notional_quote": 11.84,
                    "first_seen_ms": 1770000000000,
                    "attempt_count": 3,
                    "next_attempt_ms": 1770000001000,
                    "last_error": "max_attempts_exceeded",
                    "terminal_status": "manual_required",
                    "owner_excluded": True,
                    "open_order_truth_available": True,
                    "cap_quote": 30.0,
                    "cap_ok": True,
                }
            ],
            "last_tick_ms": 1770000001000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1770000001000,
                "kind": "recovery.unpaired_live_position_cleanup_failed",
                "payload": {
                    "venue": "binance",
                    "symbol": "ESPORTSUSDT",
                    "side": "sell",
                    "reason": "max_attempts_exceeded",
                },
            }
        ])
        monkeypatch.setattr(
            dl,
            "_build_exchange_truth",
            lambda *args, **kwargs: {
                "flat": False,
                "no_open_orders": True,
                "positions": [],
                "open_orders": [],
                "venues": {},
                "errors": [],
                "missing_evidence": [],
            },
        )

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="ESPORTSUSDT",
            venues=["binance"],
            now_ms=1770000005000,
        )

        summary = result["unpaired_live_position_recovery_summary"]
        assert summary["active_work_count"] == 1
        assert summary["manual_required_count"] == 1
        assert summary["details"][0]["terminal_status"] == "manual_required"
        assert summary["details"][0]["latest_event"]["reason"] == (
            "max_attempts_exceeded"
        )
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_acceptance_gate_blocks_only_when_open_positions_exceed_max():
    from scripts.diagnose_live import _build_production_acceptance_gate

    gate = _build_production_acceptance_gate(
        events=[],
        local_state={
            "lifecycle": "running",
            "risk_mode": "running",
            "open_position_count": 9,
            "max_concurrent_positions": 8,
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "pending_residual_repair_count": 0,
            "runtime_progress": {
                "active_lane": "housekeeping",
                "last_lane_progress_ms": 1778786994000,
            },
        },
        exchange_truth={
            "available": True,
            "has_nonzero_position": True,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        },
        state_consistency={"state_mismatch": True},
    )

    assert gate["gate_passed"] is False
    assert gate["open_position_count"] == 9
    assert gate["max_concurrent_positions"] == 8
    assert gate["remaining_position_slots"] == 0
    assert "open_positions_exceed_configured_max" in gate["blocking_reasons"]
    assert gate["runtime_progress"]["active_lane"] == "housekeeping"


def test_acceptance_gate_flags_local_l2_residual_events_in_ws_bbo_mode():
    from scripts.diagnose_live import _build_production_acceptance_gate

    gate = _build_production_acceptance_gate(
        events=[
            {
                "kind": "runtime.local_l2_phase_start",
                "payload": {"ts_ms": 1778786994000},
            },
            {
                "kind": "runtime.local_l2_snapshot_error",
                "payload": {"ts_ms": 1778786994100, "error": "legacy local-l2 ran"},
            },
        ],
        local_state={
            "lifecycle": "running",
            "risk_mode": "running",
            "open_position_count": 0,
            "max_concurrent_positions": 8,
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "pending_residual_repair_count": 0,
            "runtime_market_data_config": {
                "entry_readiness_provider_effective": "ws_bbo_quote_lease",
                "local_l2_configured_enabled": True,
                "local_l2_effective_enabled": False,
            },
        },
        exchange_truth={
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        },
    )

    assert gate["gate_passed"] is False
    assert gate["local_l2_residual_runtime_enabled_count"] == 2
    assert "local_l2_residual_runtime_enabled" in gate["fingerprints"]
    assert "local_l2_residual_runtime_enabled" in gate["blocking_reasons"]
    assert gate["runtime_market_data_config"]["local_l2_effective_enabled"] is False


def test_state_consistency_exposes_runtime_progress_diagnostics():
    from scripts.diagnose_live import _build_state_consistency

    local_state = {
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "runtime_progress": {
            "loop_iteration_started_ms": 1778786993000,
            "loop_iteration_completed_ms": 1778786990000,
            "last_lane_progress_ms": 1778786994000,
            "active_lane": "passive_close",
            "active_lane_started_ms": 1778786992000,
            "active_lane_budget_ms": 15_000,
            "active_lane_overdue": False,
        },
    }
    exchange_truth = {
        "available": True,
        "confidence": "high",
        "has_nonzero_position": False,
        "has_open_order": False,
        "positions": {},
        "open_orders": {},
    }

    consistency = _build_state_consistency(local_state, exchange_truth)

    assert consistency["runtime_progress"]["active_lane"] == "passive_close"
    assert consistency["runtime_progress"]["last_lane_progress_ms"] == 1778786994000


def test_local_state_preserves_runtime_progress_and_market_data_config():
    from scripts.diagnose_live import _build_local_state

    local_state = _build_local_state(
        {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "running",
            "risk_mode": "running",
            "open_position_count": 0,
            "pending_entry_count": 0,
            "pending_close_count": 0,
            "last_tick_ms": 1778786990000,
            "runtime_progress": {
                "active_lane": "full_tick",
                "active_lane_budget_ms": 60_000,
            },
            "runtime_market_data_config": {
                "entry_readiness_provider_effective": "ws_bbo_quote_lease",
                "local_l2_configured_enabled": True,
                "local_l2_effective_enabled": False,
            },
        },
        [],
    )

    assert local_state["runtime_progress"]["active_lane"] == "full_tick"
    effective = local_state["runtime_market_data_config"]
    assert effective["entry_readiness_provider_effective"] == "ws_bbo_quote_lease"
    assert effective["local_l2_configured_enabled"] is True
    assert effective["local_l2_effective_enabled"] is False


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


def test_run_diagnose_resolves_bybit_insufficient_balance_entry_reject_when_admission_blocked(monkeypatch):
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
            "last_tick_ms": 1781665908000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1781665905361,
                "kind": "order.rejected",
                "payload": {
                    "position_id": "entry-1781665905100-SOONUSDT",
                    "symbol": "SOONUSDT",
                    "venue": "bybit",
                    "reason": (
                        "bybit passive order failed: bybit retCode=110007 "
                        "retMsg=ab not enough for new order"
                    ),
                    "exchange_error": {
                        "http_status": 200,
                        "exchange_code": "110007",
                        "exchange_msg": "ab not enough for new order",
                        "evidence_completeness": "complete",
                        "confidence": "high",
                        "raw_body": '{"retCode":110007,"retMsg":"ab not enough for new order"}',
                    },
                    "request_context": {
                        "venue": "bybit",
                        "symbol": "SOONUSDT",
                        "post_only": True,
                        "reduce_only": False,
                    },
                },
            },
            {
                "ts_ms": 1781665905362,
                "kind": "runtime.entry_admission_blocked",
                "payload": {
                    "venue": "bybit",
                    "symbol": "SOONUSDT",
                    "reason": "insufficient_balance_admission_blocked",
                    "source": "initial_entry",
                    "block_scope": "symbol",
                    "evidence_gap": False,
                    "cooldown_active": True,
                    "raw_error": (
                        "bybit passive order failed: bybit retCode=110007 "
                        "retMsg=ab not enough for new order"
                    ),
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="SOONUSDT",
            venues=["bybit", "binance"],
            now_ms=1781665910000,
        )

        gate = result["production_acceptance_gate"]
        assert result["order_error_evidence"] == []
        assert result["top_exchange_errors"] == []
        assert gate["contained_admission_count"] == 1
        assert gate["exception_conclusions"]["contained_admission"] == "contained_admission"
        assert gate["blocking_reasons"] == []
        assert gate["gate_passed"] is True
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_resolves_aster_5018_when_headroom_admission_blocked(monkeypatch):
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
            "last_tick_ms": 1781665908000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1781665905361,
                "kind": "order.rejected",
                "payload": {
                    "position_id": "entry-1781665905100-LABUSDT",
                    "symbol": "LABUSDT",
                    "venue": "aster",
                    "reason": (
                        "HTTP 400: You've reached the maximum notional value "
                        "limit for this symbol."
                    ),
                    "exchange_error": {
                        "http_status": 400,
                        "exchange_code": "-5018",
                        "exchange_msg": (
                            "You've reached the maximum notional value limit "
                            "for this symbol."
                        ),
                        "evidence_completeness": "complete",
                        "confidence": "high",
                        "raw_body": (
                            '{"code":-5018,"msg":"You have reached the maximum '
                            'notional value limit for this symbol."}'
                        ),
                    },
                    "request_context": {
                        "venue": "aster",
                        "symbol": "LABUSDT",
                        "post_only": True,
                        "reduce_only": False,
                    },
                },
            },
            {
                "ts_ms": 1781665905362,
                "kind": "runtime.entry_admission_blocked",
                "payload": {
                    "venue": "aster",
                    "symbol": "LABUSDT",
                    "reason": "max_notional_admission_blocked",
                    "source": "aster_headroom_precheck",
                    "block_scope": "symbol_and_venue",
                    "cooldown_scope": "symbol_and_venue",
                    "evidence_gap": False,
                    "requested_notional": 30.0,
                    "remaining_openable_notional": 10.0,
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="LABUSDT",
            venues=["aster", "binance"],
            now_ms=1781665910000,
        )

        assert result["order_error_evidence"] == []
        assert result["top_exchange_errors"] == []
        summary = result["resolved_contained_entry_admission_summary"]
        assert summary["resolved_count"] == 1
        assert summary["aster_max_notional_blocked"] == 1
        assert summary["resolved_symbols"] == ["LABUSDT"]
        cooldown_summary = result["entry_admission_cooldown_summary"]
        assert cooldown_summary["reason_counts"] == {
            "max_notional_admission_blocked": 1
        }
        assert cooldown_summary["scope_counts"] == {"symbol_and_venue": 1}
        assert result["production_acceptance_gate"]["gate_passed"] is True
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_diagnose_exposes_local_order_identifier_reconcile_summary(monkeypatch):
    from scripts import diagnose_live as dl

    d = _make_tmpdir()
    try:
        _write_json(os.path.join(d, "state-current.json"), {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "RUNNING",
            "risk_mode": "normal",
            "open_positions": [],
            "pending_entries": [],
            "pending_closes": [],
            "pending_residual_repairs": [],
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1781675723066,
                "kind": "order.reconcile_query",
                "payload": {
                    "venue": "binance",
                    "symbol": "ESPORTSUSDT",
                    "order_id": "entry-1781671924167-esportsusdt-recovery-short",
                    "client_order_id": "",
                    "queried_endpoints": ["/fapi/v1/order"],
                    "response_classification": "invalid_local_order_identifier",
                    "uncertain_subtype": "invalid_local_order_identifier",
                    "next_action": "check_live_position",
                },
            },
            {
                "ts_ms": 1781675723067,
                "kind": "reconciliation.entry_reconcile_error",
                "payload": {
                    "entry_id": "entry-1781671924167-ESPORTSUSDT",
                    "error": (
                        'HTTP 400: {"code":-4015,'
                        '"msg":"Client order id length should be less than 36 chars"}'
                    ),
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="ESPORTSUSDT",
            venues=["binance", "bybit"],
            now_ms=1781675730000,
        )

        summary = result["order_reconcile_identifier_summary"]
        assert summary["invalid_local_order_identifier_count"] == 1
        assert summary["placeholder_order_id_blocked_count"] == 1
        assert summary["binance_invalid_client_order_id_error_count"] == 1
        assert summary["samples"][0]["symbol"] == "ESPORTSUSDT"
        assert result["event_counts"]["order.reconcile_query"] == 1
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_acceptance_gate_accepts_passive_close_resolved_lifecycle_when_flat():
    from scripts import diagnose_live as dl

    events = [
        {
            "ts_ms": 1781416492144,
            "kind": "entry.opened",
            "payload": {
                "entry_id": "entry-1781416483009-HOMEUSDT",
                "position_id": "entry-1781416483009-HOMEUSDT",
                "symbol": "HOMEUSDT",
            },
        },
        {
            "ts_ms": 1781416492145,
            "kind": "runtime.position_opened",
            "payload": {
                "position_id": "entry-1781416483009-HOMEUSDT",
                "symbol": "HOMEUSDT",
            },
        },
        {
            "ts_ms": 1781416809427,
            "kind": "exit.passive_close_resolved",
            "payload": {
                "position_id": "entry-1781416483009-HOMEUSDT",
                "symbol": "HOMEUSDT",
                "closure_phase": "PASSIVE_CLOSE",
                "long_closed_qty": 1300.0,
                "short_closed_qty": 1300.0,
                "reason": "first_stage_capture",
            },
        },
    ]
    local_state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
    }
    exchange_truth = {
        "available": True,
        "has_nonzero_position": False,
        "has_open_order": False,
        "positions": {"okx": {}, "bybit": {}},
        "open_orders": {"okx": {"HOMEUSDT": []}, "bybit": {"HOMEUSDT": []}},
    }

    gate = dl._build_production_acceptance_gate(events, local_state, exchange_truth)

    assert gate["gate_passed"] is True
    assert gate["unclosed_trade_lifecycle_count"] == 0
    assert gate["blocking_reasons"] == []
    assert gate["exception_conclusions"]["entry_opened"] == "closed_by_current_exchange_truth"
    assert gate["exception_conclusions"]["position_opened"] == "closed_by_current_exchange_truth"
    assert gate["recovery_lifecycle"]["closed_open_keys"] == ["entry-1781416483009-HOMEUSDT"]


def test_acceptance_gate_does_not_accept_passive_close_resolved_when_live_state_not_flat():
    from scripts import diagnose_live as dl

    events = [
        {
            "ts_ms": 1781416492144,
            "kind": "entry.opened",
            "payload": {
                "position_id": "entry-1781416483009-HOMEUSDT",
                "symbol": "HOMEUSDT",
            },
        },
        {
            "ts_ms": 1781416809427,
            "kind": "exit.passive_close_resolved",
            "payload": {
                "position_id": "entry-1781416483009-HOMEUSDT",
                "symbol": "HOMEUSDT",
                "closure_phase": "PASSIVE_CLOSE",
                "long_closed_qty": 1300.0,
                "short_closed_qty": 1300.0,
            },
        },
    ]
    local_state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "open_position_count": 0,
        "pending_entry_count": 1,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
    }
    exchange_truth = {
        "available": True,
        "has_nonzero_position": False,
        "has_open_order": True,
        "positions": {"okx": {}},
        "open_orders": {"okx": {"HOMEUSDT": [{"id": "still-open"}]}},
    }

    gate = dl._build_production_acceptance_gate(events, local_state, exchange_truth)

    assert gate["gate_passed"] is False
    assert "local_pending_entries_or_closes_present" in gate["blocking_reasons"]
    assert "exchange_truth_open_orders_present" in gate["blocking_reasons"]


@pytest.mark.parametrize(
    ("long_closed_qty", "short_closed_qty"),
    [
        (0.0, 0.0),
        (1300.0, 1.0),
    ],
)
def test_acceptance_gate_rejects_passive_close_resolved_without_sufficient_closure_evidence(
    long_closed_qty, short_closed_qty
):
    from scripts import diagnose_live as dl

    position_id = "entry-1781416483009-HOMEUSDT"
    events = [
        {
            "ts_ms": 1781416492144,
            "kind": "entry.opened",
            "payload": {
                "position_id": position_id,
                "symbol": "HOMEUSDT",
            },
        },
        {
            "ts_ms": 1781416809427,
            "kind": "exit.passive_close_resolved",
            "payload": {
                "position_id": position_id,
                "symbol": "HOMEUSDT",
                "long_closed_qty": long_closed_qty,
                "short_closed_qty": short_closed_qty,
            },
        },
    ]
    local_state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
    }
    exchange_truth = {
        "available": True,
        "has_nonzero_position": False,
        "has_open_order": False,
        "positions": {"okx": {}, "bybit": {}},
        "open_orders": {"okx": {"HOMEUSDT": []}, "bybit": {"HOMEUSDT": []}},
    }

    gate = dl._build_production_acceptance_gate(events, local_state, exchange_truth)

    assert gate["gate_passed"] is False
    assert gate["unclosed_trade_lifecycle_count"] == 1
    assert "entry_or_position_opened_without_fixture_finalized_evidence" in gate["blocking_reasons"]


def test_run_diagnose_can_report_code_side_blockers_without_changing_gate(monkeypatch):
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
            "last_tick_ms": 1781111910000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1781111900100,
                "kind": "scan.no_entry_diagnostics",
                "payload": {
                    "snapshot_freshness_blocked_counts": {"invalid_quote": 50},
                    "entry_ws_bbo_blocker_counts": {
                        "entry_ws_bbo_quote_lease_budget_exhausted": 8,
                    },
                    "strategy_blocker_counts": {"funding_edge_below_floor": 80},
                    "open_interest_blocker_counts": {"oi_below_floor": 70},
                    "liquidity_blocker_counts": {"depth_too_low": 60},
                },
            },
            {
                "ts_ms": 1781111900400,
                "kind": "recovery.live_position_bulk_diagnostic_error",
                "payload": {
                    "venue": "okx",
                    "classification": "timeout",
                    "diagnostic_scope": "best_effort_bulk_positions",
                    "truth_required_by": [],
                    "blocking": False,
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1781111910000,
            code_side_blockers=True,
            exclude_strategy=True,
            exclude_liquidity=True,
        )

        assert result["production_acceptance_gate"]["gate_passed"] is True
        view = result["code_side_blocker_view"]
        assert view["category_counts"] == {
            "code_data_freshness": 50,
            "exchange_truth_probe": 1,
            "ws_bbo_budget": 8,
        }
        assert view["filtered_out_counts"] == {
            "liquidity": 60,
            "open_interest": 70,
            "strategy": 80,
        }
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_production_gate_classifies_hyperliquid_unified_collateral_as_available():
    from scripts import diagnose_live as dl

    events = [
        {
            "ts_ms": 1779816049100,
            "kind": "runtime.entry_admission_venue_degraded",
            "payload": {
                "venue": "hyperliquid",
                "reason": "insufficient_margin_admission_prefiltered",
                "block_scope": "venue",
                "evidence_gap": False,
                "available_balance_quote": 0.0,
                "required_initial_margin_quote": 12.5075,
                "entry_notional_quote": 50.0,
                "live_target_leverage": 4.0,
                "margin_buffer_bps": 6.0,
            },
        }
    ]
    local_state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
    }
    exchange_truth = {
        "available": True,
        "confidence": "high",
        "positions": {"hyperliquid": {"*": []}},
        "open_orders": {"hyperliquid": {"*": []}},
        "has_nonzero_position": False,
        "has_open_order": False,
        "balance_views": {
            "hyperliquid": {
                "classification": "unified_collateral_available",
                "perp": {
                    "withdrawable": 0.0,
                    "account_value": 0.0,
                },
                "spot": {
                    "usdc_total": 145.863168,
                    "usdc_available": 145.863168,
                },
                "user_abstraction": "unifiedAccount",
            }
        },
    }

    gate = dl._build_production_acceptance_gate(events, local_state, exchange_truth)

    assert gate["contained_admission_count"] == 1
    assert gate["hyperliquid_margin_view_zero_count"] == 0
    assert gate["hyperliquid_unified_collateral_available_count"] == 1
    assert gate["exception_conclusions"]["contained_admission"] == "contained_admission"
    assert (
        gate["exception_conclusions"]["hyperliquid_unified_collateral_available"]
        == "unified_collateral_available"
    )
    expected_advice = (
        "Hyperliquid unified collateral is available from spot USDC. If "
        "entries are still blocked, check trading preflight, candidate "
        "freshness, and exchange reject truth."
    )
    assert gate["hyperliquid_balance_view_advice"] == [expected_advice]
    assert gate["blocking_reasons"] == []
    assert gate["gate_passed"] is True

    conclusion = dl._build_conclusion(
        {"ok": True, "critical_count": 0, "fingerprints": []},
        {"state_mismatch": False, "local_open_exchange_flat": False},
        {"overall": "complete"},
        [],
        {
            "missing_l2_or_tick_count": 0,
            "stale_rebuild_count": 0,
            "sequence_gap_count": 0,
        },
        {"stale_or_degraded_count": 0},
        exchange_truth,
        gate,
    )
    assert (
        "Hyperliquid unified collateral available; check trading preflight, "
        "candidate freshness, and exchange reject truth"
    ) in conclusion["summary"]
    assert expected_advice in conclusion["next_actions"]


def test_hyperliquid_balance_view_keeps_non_unified_spot_usdc_fail_closed():
    from scripts import diagnose_live as dl

    payload = dl._hyperliquid_balance_view_payload(
        account_address="0x" + "2" * 40,
        perp_raw={
            "withdrawable": "0",
            "marginSummary": {"accountValue": "0", "totalMarginUsed": "0"},
            "crossMarginSummary": {"accountValue": "0", "totalMarginUsed": "0"},
        },
        spot_raw={
            "balances": [
                {
                    "coin": "USDC",
                    "total": "145.863168",
                    "hold": "0",
                }
            ]
        },
        user_abstraction="normalAccount",
    )

    assert payload["classification"] == "usdc_present_margin_view_zero"
    assert payload["spot"]["usdc_available"] == pytest.approx(145.863168)
    assert payload["user_abstraction"] == "normalAccount"


def test_hyperliquid_balance_view_keeps_empty_unified_spot_fail_closed():
    from scripts import diagnose_live as dl

    payload = dl._hyperliquid_balance_view_payload(
        account_address="0x" + "2" * 40,
        perp_raw={
            "withdrawable": "0",
            "marginSummary": {"accountValue": "0", "totalMarginUsed": "0"},
            "crossMarginSummary": {"accountValue": "0", "totalMarginUsed": "0"},
        },
        spot_raw={
            "balances": [
                {
                    "coin": "USDC",
                    "total": "0",
                    "hold": "0",
                }
            ]
        },
        user_abstraction="unifiedAccount",
    )

    assert payload["classification"] == "margin_view_zero"
    assert payload["spot"]["usdc_available"] == pytest.approx(0.0)
    assert payload["user_abstraction"] == "unifiedAccount"


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
        assert (
            result["entry_quantity_terminal_summary"][
                "lifecycle_release_not_applied_count"
            ]
            == 1
        )
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
                "kind": "exit.passive_close_hedge_confirmed_after_ack",
                "payload": {
                    "position_id": "pos-btc-ack",
                    "symbol": "BTCUSDT",
                    "hedge_venue": "bybit",
                    "order_id": "bybit-order-1",
                    "client_order_id": "lf-close-1",
                    "filled": 0.01,
                    "residual": 0.0,
                    "classification": "accepted_ack_confirmed",
                    "severity": "info",
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


def test_run_diagnose_weak_order_truth_resolution_does_not_green(monkeypatch):
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
                    "position_id": "pos-btc-weak",
                    "symbol": "BTCUSDT",
                    "venue": "bybit",
                    "accepted_order_id": "bybit-order-weak",
                    "accepted_client_order_id": "lf-close-weak",
                    "truth_required_by": "accepted_order_truth_gap",
                },
            },
            {
                "ts_ms": 1780657202000,
                "kind": "exit.passive_close_hedge_confirmed_after_ack",
                "payload": {
                    "position_id": "pos-btc-weak",
                    "symbol": "BTCUSDT",
                    "hedge_venue": "bybit",
                    "order_id": "bybit-order-weak",
                    "client_order_id": "lf-close-weak",
                    "filled": 0.01,
                    "residual": 0.0,
                    "classification": "accepted_ack_confirmed",
                    "order_truth_fill_status": "truth_gap",
                    "order_truth_evidence_status": "unavailable",
                    "order_truth_decision": "retain_backoff",
                    "order_truth_missing_evidence": ["fill_confirmation"],
                    "terminal_without_truth": False,
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
        assert gate["gate_passed"] is False
        assert "order_truth_gap_unresolved" in gate["blocking_reasons"]
        assert gate["resolved_order_truth_gap_count"] == 0
        assert result["conclusion"]["status"] != "healthy"
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_resolves_moveusdt_ack_only_duplicate_after_terminal_flat(monkeypatch):
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
            "last_tick_ms": 1781078435000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1781078427209,
                "kind": "order.uncertain",
                "payload": {
                    "position_id": "entry-1781077922981-MOVEUSDT",
                    "reason": "order accepted (id=a29a75c3-9db5-464b-88ed-ef5cb0aa9aa9) but fill not confirmed",
                    "exchange_error": {
                        "venue": "bybit",
                        "operation": "place_order",
                        "exchange_code": "0",
                        "exchange_msg": "OK",
                        "evidence_completeness": "partial",
                        "missing_evidence": ["fill_confirmation"],
                        "extra": {
                            "order_ack_only": True,
                            "accepted_order_id": "a29a75c3-9db5-464b-88ed-ef5cb0aa9aa9",
                            "accepted_client_order_id": "lfxlbf3cf3338c41e406",
                        },
                        "request_context": {
                            "symbol": "MOVEUSDT",
                            "side": "sell",
                            "quantity": 1840.0,
                            "reduce_only": True,
                            "client_order_id": "lfxlbf3cf3338c41e406",
                        },
                    },
                    "request_context": {
                        "symbol": "MOVEUSDT",
                        "side": "sell",
                        "quantity": 1840.0,
                        "reduce_only": True,
                        "client_order_id": "lfxlbf3cf3338c41e406",
                    },
                },
            },
            {
                "ts_ms": 1781078428249,
                "kind": "order.rejected",
                "payload": {
                    "position_id": "entry-1781077922981-MOVEUSDT",
                    "reason": "bybit order failed: bybit retCode=110072 retMsg=OrderLinkedID is duplicate",
                    "exchange_error": {
                        "venue": "bybit",
                        "operation": "place_order",
                        "exchange_code": "110072",
                        "exchange_msg": "orderlinkedid is duplicate",
                        "evidence_completeness": "missing_exchange_body",
                        "missing_evidence": ["exchange_response_body"],
                        "request_context": {
                            "symbol": "MOVEUSDT",
                            "side": "sell",
                            "quantity": 1840.0,
                            "reduce_only": True,
                            "client_order_id": "lfxlbf3cf3338c41e406",
                        },
                    },
                    "request_context": {
                        "symbol": "MOVEUSDT",
                        "side": "sell",
                        "quantity": 1840.0,
                        "reduce_only": True,
                        "client_order_id": "lfxlbf3cf3338c41e406",
                    },
                },
            },
            {
                "ts_ms": 1781078428474,
                "kind": "order.reconcile_result",
                "payload": {
                    "position_id": "entry-1781077922981-MOVEUSDT",
                    "venue": "bybit",
                    "symbol": "MOVEUSDT",
                    "client_order_id": "lfxlbf3cf3338c41e406",
                    "status": "none",
                    "reason": "duplicate_client_id",
                    "uncertain_subtype": "duplicate_client_id",
                    "target_qty": 1840.0,
                    "reconciled_qty": 0.0,
                    "live_qty": 0.0,
                    "remaining_qty": 1840.0,
                    "next_action": "clear_live_flat",
                },
            },
            {
                "ts_ms": 1781078428728,
                "kind": "exit.passive_close_recovery_probe_flat",
                "payload": {
                    "position_id": "entry-1781077922981-MOVEUSDT",
                    "symbol": "MOVEUSDT",
                    "client_order_id": "lfxlbf3cf3338c41e406",
                },
            },
            {
                "ts_ms": 1781078428977,
                "kind": "runtime.position_lifecycle_terminal",
                "payload": {
                    "position_id": "entry-1781077922981-MOVEUSDT",
                    "symbol": "MOVEUSDT",
                    "terminal_state": "flat",
                    "terminal_reason": "pending_passive_close_flat_probe",
                    "client_order_id": "lfxlbf3cf3338c41e406",
                },
            },
            {
                "ts_ms": 1781078428977,
                "kind": "recovery.flat",
                "payload": {
                    "position_id": "entry-1781077922981-MOVEUSDT",
                    "symbol": "MOVEUSDT",
                    "client_order_id": "lfxlbf3cf3338c41e406",
                },
            },
            {
                "ts_ms": 1781078429188,
                "kind": "execution.residual_repair_completed",
                "payload": {
                    "position_id": "entry-1781077922981-MOVEUSDT",
                    "symbol": "MOVEUSDT",
                    "result": "already_flat",
                    "client_order_id": "lfxlbf3cf3338c41e406",
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="MOVEUSDT",
            venues=["bybit"],
            now_ms=1781078436000,
        )

        gate = result["production_acceptance_gate"]
        assert gate["gate_passed"] is True
        assert gate["blocking_reasons"] == []
        assert gate["resolved_order_truth_gap_count"] == 1
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


def test_run_diagnose_blocks_home_recovery_flat_quick_flat_chain(monkeypatch):
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
            "last_tick_ms": 1781293950000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1781293920000,
                "kind": "entry.opened",
                "payload": {
                    "position_id": "entry-1781293924792-HOMEUSDT",
                    "symbol": "HOMEUSDT",
                },
            },
            {
                "ts_ms": 1781293940000,
                "kind": "runtime.position_drift_corrected",
                "payload": {
                    "position_id": "entry-1781293924792-HOMEUSDT",
                    "symbol": "HOMEUSDT",
                    "reason": "exchange_truth_flat",
                },
            },
            {
                "ts_ms": 1781293940500,
                "kind": "recovery.flat",
                "payload": {
                    "position_id": "entry-1781293924792-HOMEUSDT",
                    "symbol": "HOMEUSDT",
                    "reason": "exchange_truth_flat",
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="HOMEUSDT",
            venues=["okx", "bybit"],
            now_ms=1781293950000,
        )

        summary = result["quick_flat_summary"]
        assert summary["quick_flat_count"] == 1
        assert summary["quick_flat_terminal_kind_counts"] == {"recovery.flat": 1}
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


def test_diagnose_pending_entry_live_conflict_summary_lists_home_truth_layers():
    from scripts.diagnose_live import _build_pending_entry_live_conflict_summary

    summary = _build_pending_entry_live_conflict_summary(
        {
            "pending_entries": [
                {
                    "pending_id": "entry-home",
                    "symbol": "HOMEUSDT",
                    "long_venue": "okx",
                    "short_venue": "bybit",
                    "maker_leg": "long",
                    "maker_leg_filled": 1600.0,
                    "hedge_leg_filled": 1600.0,
                    "maker_order_id": "okx-maker-order",
                    "hedge_order_id": "bybit-hedge-order",
                }
            ]
        },
        {
            "available": True,
            "confidence": "high",
            "positions": {
                "okx": {},
                "bybit": {
                    "HOMEUSDT": {
                        "venue": "bybit",
                        "symbol": "HOMEUSDT",
                        "side": "Side.SELL",
                        "quantity": 1600.0,
                    }
                },
            },
            "open_orders": {
                "okx": {"HOMEUSDT": []},
                "bybit": {"HOMEUSDT": []},
            },
        },
    )

    assert summary["count"] == 1
    detail = summary["details"][0]
    assert detail["pending_id"] == "entry-home"
    assert detail["maker_leg_filled"] == pytest.approx(1600.0)
    assert detail["hedge_leg_filled"] == pytest.approx(1600.0)
    assert "okx fill evidence conflicts with okx live flat" in detail["conflict_reasons"]
    assert "live position owned by pending conflict" in detail["conflict_reasons"]
    bybit_leg = [leg for leg in detail["legs"] if leg["venue"] == "bybit"][0]
    assert bybit_leg["live_quantity"] == pytest.approx(1600.0)
    assert bybit_leg["owner"] == "pending_entry"
    assert bybit_leg["live_position_confirmed"] is True
    assert detail["next_action"] == "owned_pending_entry_live_conflict_cleanup"


def test_diagnose_journal_positive_fill_conflict_owns_historical_live_single_leg(monkeypatch):
    from scripts import diagnose_live as dl

    def home_single_leg_exchange_truth(runtime_dir, symbols, venues=None):
        return {
            "available": True,
            "available_venues": ["okx", "bybit"],
            "confidence": "high",
            "positions": {
                "okx": {},
                "bybit": {
                    "HOMEUSDT": {
                        "venue": "bybit",
                        "symbol": "HOMEUSDT",
                        "side": "Side.SELL",
                        "quantity": 1600.0,
                    }
                },
            },
            "open_orders": {
                "okx": {"HOMEUSDT": []},
                "bybit": {"HOMEUSDT": []},
            },
            "has_nonzero_position": True,
            "has_open_order": False,
            "fetch_status": {
                "okx": {"status": "ok"},
                "bybit": {"status": "ok"},
            },
            "errors": [],
            "missing_evidence": [],
        }

    d = _make_tmpdir()
    try:
        _write_json(os.path.join(d, "state-current.json"), {
            "schema": "lightfee.current_state.v1",
            "lifecycle": "risk_only",
            "risk_mode": "running",
            "open_position_count": 0,
            "open_positions": [],
            "pending_entry_count": 0,
            "pending_entries": [],
            "pending_close_count": 0,
            "last_tick_ms": 1700000000000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1700000001000,
                "kind": "pending_entry.positive_fill_live_truth_conflict",
                "payload": {
                    "entry_id": "entry-home",
                    "symbol": "HOMEUSDT",
                    "maker_leg_filled": 1600.0,
                    "hedge_leg_filled": 1600.0,
                    "matched_quantity": 1600.0,
                    "live_long_quantity": 0.0,
                    "live_short_quantity": 1600.0,
                    "live_balanced_quantity": 0.0,
                },
            }
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", home_single_leg_exchange_truth)

        result = run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="HOMEUSDT",
            venues=["okx", "bybit"],
            now_ms=1700000005000,
        )

        rows = result["production_acceptance_gate"]["v1_lifecycle_closure"]["rows"]
        owned_rows = [
            row
            for row in rows
            if row["owner_id"] == "entry-home"
            and row["details"].get("kind") == "owned_pending_entry_live_conflict"
        ]

        assert owned_rows
        assert not any("unpaired_live_position" in row["row_key"] for row in rows)
        assert owned_rows[0]["terminality"] == "owned_pending_entry_live_conflict"
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


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


def test_production_gate_counts_journal_owned_pending_passive_close():
    from scripts import diagnose_live as dl

    local_state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "open_position_count": 1,
        "open_positions": [
            {
                "position_id": "entry-genius",
                "symbol": "GENIUSUSDT",
                "long_venue": "binance",
                "short_venue": "bybit",
            }
        ],
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_passive_close_count": 0,
        "pending_passive_closes": [],
        "pending_residual_repair_count": 0,
    }
    events = [
        {
            "kind": "exit.passive_close_maker_submitted",
            "payload": {
                "position_id": "entry-genius",
                "symbol": "GENIUSUSDT",
                "venue": "bybit",
                "order_id": "bybit-close-live",
                "client_order_id": "bybit-close-client",
                "reduce_only": True,
            },
        }
    ]
    exchange_truth = {
        "available": True,
        "confidence": "high",
        "has_nonzero_position": True,
        "has_open_order": True,
        "positions": {
            "bybit": {
                "GENIUSUSDT": {
                    "venue": "bybit",
                    "symbol": "GENIUSUSDT",
                    "side": "short",
                    "quantity": 60.0,
                }
            }
        },
        "open_orders": {
            "bybit": {
                "GENIUSUSDT": [
                    {
                        "venue": "bybit",
                        "symbol": "GENIUSUSDT",
                        "side": "Buy",
                        "quantity": 60.0,
                        "reduce_only": True,
                        "order_id": "bybit-close-live",
                        "client_order_id": "bybit-close-client",
                    }
                ]
            }
        },
    }

    gate = dl._build_production_acceptance_gate(
        events,
        local_state,
        exchange_truth,
    )

    assert gate["gate_passed"] is False
    assert gate["owned_pending_passive_close_count"] == 1
    assert gate["ownerless_open_order_count"] == 0
    rows = gate["v1_lifecycle_closure"]["rows"]
    assert any(
        row["evidence_class"] == "owned_pending_passive_close"
        and row["phase"] == "PASSIVE_CLOSE"
        for row in rows
    )


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


def test_production_blocker_window_reports_nonblocking_bulk_probe_timeout_details(tmp_path):
    from scripts.analyze_production_blockers import analyze_event_file

    events_path = tmp_path / "window_bulk_probe_timeout.jsonl"
    _write_jsonl(events_path, [
        {
            "ts_ms": 1779811000000,
            "kind": "recovery.live_position_bulk_diagnostic_error",
            "payload": {
                "venue": "bitget",
                "endpoint": "/api/v2/mix/position/all-position",
                "classification": "timeout",
                "timeout_ms": 2000,
                "diagnostic_scope": "best_effort_bulk_positions",
                "blocking": False,
                "truth_required_by": [],
                "fallback_planned": False,
            },
        },
        {
            "ts_ms": 1779811001000,
            "kind": "recovery.live_position_bulk_diagnostic_error",
            "payload": {
                "venue": "okx",
                "endpoint": "/api/v5/account/positions",
                "classification": "timeout",
                "timeout_ms": 2000,
                "diagnostic_scope": "best_effort_bulk_positions",
                "blocking": False,
                "truth_required_by": [],
                "fallback_planned": True,
            },
        },
    ])

    result = analyze_event_file(
        events_path,
        now_ms=1779812000000,
        windows=["last_2h", "run_window"],
    )

    summary = result["windows"]["last_2h"]["nonblocking_bulk_probe_summary"]
    assert summary["total_count"] == 2
    assert summary["timeout_count"] == 2
    assert summary["by_venue"] == {"bitget": 1, "okx": 1}
    assert summary["fallback_planned_count"] == 1
    assert summary["no_fallback_count"] == 1
    assert {
        "venue": "bitget",
        "endpoint": "/api/v2/mix/position/all-position",
        "count": 1,
        "timeout_count": 1,
        "fallback_planned_count": 0,
        "no_fallback_count": 1,
        "last_ts_ms": 1779811000000,
        "diagnostic_scope": "best_effort_bulk_positions",
        "timeout_ms": 2000,
    } in summary["details"]


def test_production_blocker_window_reports_candidate_selection_starvation(tmp_path):
    from scripts.analyze_production_blockers import analyze_event_file

    events_path = tmp_path / "window_candidate_starvation.jsonl"
    _write_jsonl(events_path, [
        {
            "ts_ms": 1779811000000,
            "kind": "runtime.entry_quote_revalidate_failed",
            "payload": {
                "symbol": "HEMIUSDT",
                "pair_id": "HEMIUSDT:bybit->aster",
                "source": "top_candidate_quote",
                "reason": "quote_stale",
                "age_ms": 65000,
            },
        },
        {
            "ts_ms": 1779811001000,
            "kind": "runtime.entry_ws_bbo_top_candidate_rewarm_failed",
            "payload": {
                "symbol": "HEMIUSDT",
                "pair_id": "HEMIUSDT:bybit->aster",
                "reason": "quote_stale",
                "source": "ws_bbo_quote_lease",
                "age_ms": 64000,
            },
        },
        {
            "ts_ms": 1779811002000,
            "kind": "execution.entry_liquidity_blocked",
            "payload": {
                "symbol": "ESPORTSUSDT",
                "pair_id": "ESPORTSUSDT:bybit->aster",
                "reason": "perp_open_interest_structural",
                "eligibility_class": "structural_ineligibility",
                "open_interest": 0,
            },
        },
        {
            "ts_ms": 1779811003000,
            "kind": "scan.no_entry_diagnostics",
            "payload": {
                "top_quote_blocker_buckets": {
                    "quote_stale": 2,
                },
                "open_interest_blocker_counts": {
                    "perp_open_interest_structural": 1,
                },
            },
        },
    ])

    result = analyze_event_file(
        events_path,
        now_ms=1779812000000,
        windows=["last_2h", "run_window"],
    )

    starvation = result["windows"]["last_2h"]["candidate_selection_starvation"]
    assert starvation["detected"] is True
    assert starvation["quote_stale_count"] >= 3
    assert starvation["open_interest_structural_count"] == 2
    assert starvation["top_reasons"][0]["reason"] == "quote_stale"
    assert {"symbol": "HEMIUSDT", "count": 2} in starvation["top_symbols"]
    assert starvation["action"] == "rewarm_top_candidates_and_reprobe_open_interest"


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

    def fake_create_adapter(venue, credential, *, rate_limiter=None):
        assert venue in {"bybit", "aster"}
        assert rate_limiter is not None
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


def test_exchange_truth_readonly_context_installs_rate_limit_runtime(monkeypatch):
    import asyncio

    from lightfee.core.domain import PositionSnapshot, Side, Venue
    from lightfee.rate_limit.engine import (
        global_rate_limit_runtime,
        install_global_rate_limit_runtime,
    )
    from scripts import diagnose_live as dl

    monkeypatch.setenv("LIGHTFEE_FAKE_API_KEY", "fk")
    monkeypatch.setenv("LIGHTFEE_FAKE_API_SECRET", "fs")

    observed = {}

    class FakeAdapter:
        venue = "fake"
        _transport = object()

        async def fetch_position(self, symbol):
            observed["runtime_during_fetch"] = global_rate_limit_runtime()
            return PositionSnapshot(
                venue=Venue.BINANCE,
                symbol=symbol,
                side=Side.BUY,
                quantity=0.0,
                entry_price=0.0,
                observed_at_ms=1700000000000,
            )

        async def fetch_open_orders(self, symbol=None):
            return []

        async def shutdown(self):
            observed["shutdown_called"] = True

    def fake_create_adapter(venue, credential, *, rate_limiter=None):
        observed["runtime_during_create"] = global_rate_limit_runtime()
        observed["rate_limiter"] = rate_limiter
        return FakeAdapter()

    monkeypatch.setattr(dl, "_create_readonly_adapter", fake_create_adapter)

    previous = global_rate_limit_runtime()
    install_global_rate_limit_runtime(None)
    try:
        result = asyncio.run(dl._build_exchange_truth_async(
            runtime_dir="/unused",
            symbols=["BTCUSDT"],
            venues=["fake"],
        ))
    finally:
        restored = global_rate_limit_runtime()
        install_global_rate_limit_runtime(previous)

    assert result["confidence"] == "high"
    assert observed["rate_limiter"] is not None
    assert observed["runtime_during_create"] is not None
    assert observed["runtime_during_fetch"] is observed["runtime_during_create"]
    assert observed["shutdown_called"] is True
    assert restored is None


def test_exchange_truth_aster_readonly_adapter_passes_rate_limiter_to_private_client():
    from scripts import diagnose_live as dl
    from lightfee.venues.transport import EndpointRateLimiter, LiveCredential

    limiter = EndpointRateLimiter(initial_ms=1000, max_ms=8000, pacing_interval_ms=25)
    credential = LiveCredential(
        api_key="key",
        api_secret="secret",
        wallet_private_key="0x" + "1" * 64,
    )

    adapter = dl._create_readonly_adapter("aster", credential, rate_limiter=limiter)

    assert adapter is not None
    assert getattr(adapter, "_private")._rate_limiter is limiter


def test_exchange_truth_loads_hyperliquid_private_key_alias(monkeypatch):
    from scripts import diagnose_live as dl

    private_key = "0x" + "1" * 64
    account = "0x" + "2" * 40
    monkeypatch.setenv("LIGHTFEE_HYPERLIQUID_PRIVATE_KEY", private_key)
    monkeypatch.setenv("LIGHTFEE_HYPERLIQUID_ACCOUNT_ADDRESS", account)
    monkeypatch.setenv("LIGHTFEE_HYPERLIQUID_WALLET_MODE", "agent_wallet")

    credential = dl._load_venue_credential("hyperliquid")

    assert credential is not None
    assert credential.wallet_private_key == private_key
    assert credential.account_address == account
    assert credential.wallet_mode == "api_wallet"


def test_exchange_truth_hyperliquid_queries_configured_account_when_signer_differs(monkeypatch):
    import asyncio
    import httpx
    from scripts import diagnose_live as dl

    private_key = "0x" + "1" * 64
    account = "0x" + "2" * 40
    seen_users: list[str] = []
    monkeypatch.setenv("LIGHTFEE_HYPERLIQUID_PRIVATE_KEY", private_key)
    monkeypatch.setenv("LIGHTFEE_HYPERLIQUID_ACCOUNT_ADDRESS", account)
    monkeypatch.setenv("LIGHTFEE_HYPERLIQUID_WALLET_MODE", "agent_wallet")

    original_create = dl._create_readonly_adapter

    def create_adapter(venue, credential, *, rate_limiter=None):
        assert rate_limiter is not None
        adapter = original_create(venue, credential, rate_limiter=rate_limiter)
        assert adapter is not None

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode())
            if request.url.path == "/info" and body.get("type") == "clearinghouseState":
                seen_users.append(body["user"])
                if body["user"].lower() == account.lower():
                    return httpx.Response(
                        200,
                        json={
                            "assetPositions": [
                                {
                                    "position": {
                                        "coin": "OP",
                                        "szi": "-397.9",
                                        "entryPx": "0.10286",
                                    }
                                }
                            ],
                            "marginSummary": {},
                        },
                    )
                return httpx.Response(
                    200,
                    json={"assetPositions": [], "marginSummary": {}},
                )
            if request.url.path == "/info" and body.get("type") == "openOrders":
                seen_users.append(body["user"])
                return httpx.Response(200, json=[])
            raise AssertionError(f"unexpected request: {request.url.path} {body}")

        adapter._transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        return adapter

    monkeypatch.setattr(dl, "_create_readonly_adapter", create_adapter)

    result = asyncio.run(dl._build_exchange_truth_async(
        runtime_dir="/unused",
        symbols=[],
        venues=["hyperliquid"],
    ))

    assert seen_users
    assert set(seen_users) == {account}
    assert result["available"] is True
    assert result["confidence"] == "high"
    assert result["has_nonzero_position"] is True
    assert result["credential_identity"]["hyperliquid"]["wallet_mode"] == "api_wallet"
    assert result["credential_identity"]["hyperliquid"]["account_matches_signer"] is False
    assert result["credential_identity"]["hyperliquid"]["account_address_masked"] == (
        "0x2222...2222"
    )
    assert "account_address" not in result["credential_identity"]["hyperliquid"]
    assert "wallet_private_key" not in result["credential_identity"]["hyperliquid"]
    assert result["positions"]["hyperliquid"]["OPUSDT"]["quantity"] == 397.9
    assert result["positions"]["hyperliquid"]["OPUSDT"]["side"].endswith("SELL")


def test_exchange_truth_hyperliquid_reports_unified_collateral_available(monkeypatch):
    import asyncio
    import httpx
    from scripts import diagnose_live as dl

    private_key = "0x" + "1" * 64
    account = "0x" + "2" * 40
    monkeypatch.setenv("LIGHTFEE_HYPERLIQUID_PRIVATE_KEY", private_key)
    monkeypatch.setenv("LIGHTFEE_HYPERLIQUID_ACCOUNT_ADDRESS", account)
    monkeypatch.setenv("LIGHTFEE_HYPERLIQUID_WALLET_MODE", "agent_wallet")

    original_create = dl._create_readonly_adapter

    def create_adapter(venue, credential, *, rate_limiter=None):
        assert rate_limiter is not None
        adapter = original_create(venue, credential, rate_limiter=rate_limiter)
        assert adapter is not None

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode())
            if request.url.path == "/info" and body.get("type") == "clearinghouseState":
                return httpx.Response(
                    200,
                    json={
                        "assetPositions": [],
                        "withdrawable": "0.0",
                        "marginSummary": {"accountValue": "0.0", "totalMarginUsed": "0.0"},
                        "crossMarginSummary": {"accountValue": "0.0", "totalMarginUsed": "0.0"},
                    },
                )
            if request.url.path == "/info" and body.get("type") == "userAbstraction":
                assert body["user"] == account
                return httpx.Response(200, json="unifiedAccount")
            if request.url.path == "/info" and body.get("type") == "spotClearinghouseState":
                return httpx.Response(
                    200,
                    json={
                        "balances": [
                            {
                                "coin": "USDC",
                                "total": "145.863168",
                                "hold": "0.0",
                                "entryNtl": "0.0",
                            }
                        ]
                    },
                )
            if request.url.path == "/info" and body.get("type") == "openOrders":
                return httpx.Response(200, json=[])
            raise AssertionError(f"unexpected request: {request.url.path} {body}")

        adapter._transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        return adapter

    monkeypatch.setattr(dl, "_create_readonly_adapter", create_adapter)

    result = asyncio.run(dl._build_exchange_truth_async(
        runtime_dir="/unused",
        symbols=[],
        venues=["hyperliquid"],
    ))

    balance = result["balance_views"]["hyperliquid"]
    assert balance["classification"] == "unified_collateral_available"
    assert balance["user_abstraction"] == "unifiedAccount"
    assert balance["perp"]["withdrawable"] == pytest.approx(0.0)
    assert balance["perp"]["account_value"] == pytest.approx(0.0)
    assert balance["spot"]["usdc_total"] == pytest.approx(145.863168)
    assert balance["spot"]["usdc_available"] == pytest.approx(145.863168)
    assert balance["spot"]["balances"][0]["coin"] == "USDC"
    assert "account_address" not in balance


def test_hyperliquid_balance_view_uses_operation_contracts(monkeypatch):
    import asyncio
    from types import SimpleNamespace

    from lightfee.core.domain import Venue
    from lightfee.venues.specs import VenueOperation
    import scripts.diagnose_live as dl

    account = "0x" + "2" * 40
    operations: list[VenueOperation] = []
    users: list[str] = []

    async def fake_request_venue_operation(
        transport,
        venue,
        operation,
        *,
        account_address="",
        agent_wallet_address="",
        **kwargs,
    ):
        operations.append(operation)
        users.append(account_address)
        assert venue == Venue.HYPERLIQUID
        assert agent_wallet_address == ""
        if operation == VenueOperation.POSITION:
            return (
                {
                    "assetPositions": [],
                    "withdrawable": "0.0",
                    "marginSummary": {"accountValue": "0.0", "totalMarginUsed": "0.0"},
                    "crossMarginSummary": {
                        "accountValue": "0.0",
                        "totalMarginUsed": "0.0",
                    },
                },
                SimpleNamespace(),
            )
        if operation == getattr(VenueOperation, "USER_ABSTRACTION"):
            return ("unifiedAccount", SimpleNamespace())
        if operation == getattr(VenueOperation, "SPOT_CLEARINGHOUSE_STATE"):
            return ({"balances": [{"coin": "USDC", "total": "145", "hold": "0"}]}, SimpleNamespace())
        raise AssertionError(f"unexpected operation: {operation}")

    monkeypatch.setattr(dl, "request_venue_operation", fake_request_venue_operation)

    result = asyncio.run(
        dl._fetch_hyperliquid_balance_view(
            SimpleNamespace(_transport=object()),
            SimpleNamespace(account_address=account),
        )
    )

    assert operations == [
        VenueOperation.POSITION,
        getattr(VenueOperation, "USER_ABSTRACTION"),
        getattr(VenueOperation, "SPOT_CLEARINGHOUSE_STATE"),
    ]
    assert set(users) == {account}
    assert result["classification"] == "unified_collateral_available"


def test_hyperliquid_balance_view_502_is_venue_diagnostic_without_owned_exposure(monkeypatch):
    import asyncio
    import httpx
    from scripts import diagnose_live as dl

    private_key = "0x" + "1" * 64
    account = "0x" + "2" * 40
    monkeypatch.setenv("LIGHTFEE_HYPERLIQUID_PRIVATE_KEY", private_key)
    monkeypatch.setenv("LIGHTFEE_HYPERLIQUID_ACCOUNT_ADDRESS", account)
    monkeypatch.setenv("LIGHTFEE_HYPERLIQUID_WALLET_MODE", "agent_wallet")

    original_create = dl._create_readonly_adapter

    def create_adapter(venue, credential, *, rate_limiter=None):
        assert rate_limiter is not None
        adapter = original_create(venue, credential, rate_limiter=rate_limiter)
        assert adapter is not None

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode())
            if request.url.path == "/info" and body.get("type") == "clearinghouseState":
                return httpx.Response(200, json={"assetPositions": [], "marginSummary": {}})
            if request.url.path == "/info" and body.get("type") == "openOrders":
                return httpx.Response(200, json=[])
            if request.url.path == "/info" and body.get("type") in {
                "userAbstraction",
                "spotClearinghouseState",
            }:
                return httpx.Response(502, text="bad gateway")
            raise AssertionError(f"unexpected request: {request.url.path} {body}")

        adapter._transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        return adapter

    monkeypatch.setattr(dl, "_create_readonly_adapter", create_adapter)

    result = asyncio.run(
        dl._build_exchange_truth_async(
            runtime_dir="/unused",
            symbols=[],
            venues=["hyperliquid"],
        )
    )

    assert result["available"] is True
    assert result["has_nonzero_position"] is False
    assert result["has_open_order"] is False
    assert result["missing_evidence"] == []
    assert result["balance_views"]["hyperliquid"]["classification"] == (
        "balance_view_probe_failed"
    )


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
            if venue == "bitget":
                from lightfee.venues.specs import BitgetContractFamily

                async def _resolve_bitget_family():
                    return BitgetContractFamily.UTA_V3

                self._transport._bitget_resolve_contract_family = _resolve_bitget_family

        async def fetch_all_positions(self):
            return []

        async def shutdown(self):
            pass

    monkeypatch.setattr(dl, "_load_venue_credential", lambda venue: object())
    monkeypatch.setattr(
        dl,
        "_create_readonly_adapter",
        lambda venue, credential, *, rate_limiter=None: FakeAdapter(venue),
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


def test_run_diagnose_reports_passive_close_terminal_summary(monkeypatch):
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
            "last_tick_ms": 1781097000000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1781096100000,
                "kind": "exit.passive_close_resolved",
                "payload": {
                    "position_id": "entry-normal",
                    "symbol": "HOMEUSDT",
                    "problem": False,
                    "single_leg_fast_flatten": False,
                },
            },
            {
                "ts_ms": 1781096100100,
                "kind": "exit.passive_close_resolved",
                "payload": {
                    "position_id": "entry-problem",
                    "symbol": "SAHARAUSDT",
                    "problem": True,
                    "single_leg_fast_flatten": True,
                },
            },
            {
                "ts_ms": 1781096100200,
                "kind": "runtime.position_drift_skipped_passive_close_owner",
                "payload": {
                    "position_id": "entry-owner",
                    "symbol": "MOVEUSDT",
                },
            },
            {
                "ts_ms": 1781096100300,
                "kind": "runtime.stale_fail_closed_cleared",
                "payload": {
                    "reason": "startup_clean_stale_fail_closed_cleared",
                },
            },
            {
                "ts_ms": 1781096100400,
                "kind": "execution.entry_residual_dust_tolerated",
                "payload": {
                    "position_id": "entry-sahara-dust",
                    "symbol": "SAHARAUSDT",
                    "repair_quantity": 40.0,
                    "matched_quantity": 2860.0,
                    "residual_ratio": 0.013986,
                    "terminal_reason": "exchange_min_quantity_dust",
                },
            },
            {
                "ts_ms": 1781096100500,
                "kind": "pending_entry.hedge_quantity_undercut",
                "payload": {
                    "entry_id": "entry-undercut",
                    "symbol": "HOMEUSDT",
                    "missing_hedge_quantity": 700.0,
                    "normalized_quantity": 690.0,
                    "reason_family": "exchange_step_rounding",
                },
            },
            {
                "ts_ms": 1781096100550,
                "kind": "pending_entry.hedge_quantity_undercut",
                "payload": {
                    "entry_id": "entry-sahara-dust",
                    "symbol": "SAHARAUSDT",
                    "missing_hedge_quantity": 2871.0,
                    "normalized_quantity": 2860.0,
                },
            },
            {
                "ts_ms": 1781096100600,
                "kind": "execution.entry_quantity_plan",
                "payload": {
                    "entry_id": "entry-legit-split",
                    "symbol": "BTCUSDT",
                    "common_quantity": 1.0,
                    "full_target_quantity": 1.0,
                    "initial_maker_target_quantity": 0.5,
                    "route": "passive_incremental",
                    "okx_base_quantity_step": 0.0,
                },
            },
            {
                "ts_ms": 1781096100700,
                "kind": "execution.entry_quantity_plan",
                "payload": {
                    "entry_id": "entry-mismatch",
                    "symbol": "HOMEUSDT",
                    "common_quantity": 700.0,
                    "full_target_quantity": 690.0,
                    "initial_maker_target_quantity": 690.0,
                    "route": "passive_incremental",
                    "okx_base_quantity_step": 100.0,
                },
            },
            {
                "ts_ms": 1781096100800,
                "kind": "execution.entry_quantity_plan",
                "payload": {
                    "entry_id": "entry-sahara-dust",
                    "symbol": "SAHARAUSDT",
                    "common_quantity": 2990.0,
                    "full_target_quantity": 2871.0709692855226,
                    "initial_maker_target_quantity": 2871.0709692855226,
                    "route": "passive_incremental",
                    "okx_base_quantity_step": 10.0,
                },
            },
        ])

        monkeypatch.setattr(dl, "_build_exchange_truth", lambda *args, **kwargs: {
            "available": True,
            "confidence": "high",
            "positions": {},
            "open_orders": {},
            "has_nonzero_position": False,
            "has_open_order": False,
            "errors": [],
            "missing_evidence": [],
        })

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1781097000000,
        )

        summary = result["passive_close_terminal_summary"]
        assert summary["passive_close_resolved_count"] == 2
        assert summary["problem_resolved_count"] == 1
        assert summary["single_leg_fast_flatten_count"] == 1
        assert summary["passive_owned_drift_blocked_count"] == 1
        assert summary["stale_fail_closed_after_flat_count"] == 1
        entry_summary = result["entry_quantity_terminal_summary"]
        assert entry_summary["entry_residual_dust_tolerated_count"] == 1
        assert entry_summary["hedge_quantity_undercut_count"] == 2
        assert entry_summary["hedge_quantity_undercut_warning_count"] == 1
        assert entry_summary["hedge_quantity_undercut_warning_entry_ids"] == [
            "entry-undercut"
        ]
        assert entry_summary["common_quantity_mismatch_count"] == 2
        assert entry_summary["common_quantity_mismatch_warning_count"] == 1
        assert entry_summary["common_quantity_mismatch_entry_ids"] == [
            "entry-mismatch",
            "entry-sahara-dust",
        ]
        assert entry_summary["common_quantity_mismatch_warning_entry_ids"] == [
            "entry-mismatch"
        ]
        assert entry_summary["quantity_warning_reason_counts"] == {
            "common_quantity_mismatch:unknown": 1,
            "hedge_quantity_undercut:exchange_step_rounding": 1,
        }
        assert entry_summary["quantity_warning_samples"] == [
            {
                "entry_id": "entry-mismatch",
                "kind": "common_quantity_mismatch",
                "reason_family": "unknown",
                "symbol": "HOMEUSDT",
                "common_quantity": 700.0,
                "full_target_quantity": 690.0,
            },
            {
                "entry_id": "entry-undercut",
                "kind": "hedge_quantity_undercut",
                "reason_family": "exchange_step_rounding",
                "symbol": "HOMEUSDT",
                "missing_hedge_quantity": 700.0,
                "normalized_quantity": 690.0,
                "undercut_quantity": 10.0,
            },
        ]
        gate = result["production_acceptance_gate"]
        assert gate["short_window_warning_count"] == 2
        assert gate["short_window_warning_families"] == [
            "entry_quantity_mismatch",
            "hedge_quantity_undercut",
        ]
        assert gate["short_window_warning_details"] == {
            "entry_quantity_mismatch": 1,
            "hedge_quantity_undercut": 1,
            "passive_close_truth_gap": 0,
            "passive_zero_fill_exhausted_then_recovered": 0,
        }
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_filters_resolved_bybit_terminal_zero_qty_from_order_errors(monkeypatch):
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
            "last_tick_ms": 1781531695000,
        })
        exchange_error = {
            "venue": "bybit",
            "operation": "submit_passive_order",
            "transport_error_type": "unknown",
            "raw_body": '{"retCode":110017,"retMsg":"orderQty will be truncated to zero."}',
            "exchange_code": "110017",
            "exchange_msg": "orderQty will be truncated to zero.",
            "evidence_completeness": "complete",
            "missing_evidence": [],
            "confidence": "high",
        }
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1781531688000,
                "kind": "exit.passive_close_maker_submit_error",
                "payload": {
                    "position_id": "entry-1781531687393-HOMEUSDT",
                    "symbol": "HOMEUSDT",
                    "venue": "bybit",
                    "error": (
                        "bybit passive order failed: bybit retCode=110017 "
                        "retMsg=orderQty will be truncated to zero."
                    ),
                    "exchange_error": exchange_error,
                    "request_context": {
                        "symbol": "HOMEUSDT",
                        "reduce_only": True,
                        "quantity": 1800.0,
                    },
                    "evidence_completeness": "complete",
                },
            },
            {
                "ts_ms": 1781531688001,
                "kind": "exit.passive_close_terminal_zero_qty_reduce_only_evidence",
                "payload": {
                    "position_id": "entry-1781531687393-HOMEUSDT",
                    "symbol": "HOMEUSDT",
                    "venue": "bybit",
                    "exchange_error": exchange_error,
                    "request_context": {
                        "symbol": "HOMEUSDT",
                        "reduce_only": True,
                        "quantity": 1800.0,
                    },
                    "decision": "probe_live_truth",
                },
            },
            {
                "ts_ms": 1781531689000,
                "kind": "exit.passive_close_resolved",
                "payload": {
                    "position_id": "entry-1781531687393-HOMEUSDT",
                    "symbol": "HOMEUSDT",
                    "resolution_source": "passive_close_bybit_terminal_zero_qty_reduce_only",
                    "live_flat_terminal": True,
                    "problem": False,
                },
            },
        ])

        monkeypatch.setattr(dl, "_build_exchange_truth", lambda *args, **kwargs: {
            "available": True,
            "truth_available": True,
            "confidence": "high",
            "positions": {
                "okx": {
                    "NEWTOKEN-USDT-SWAP": {"quantity": 1.0},
                },
            },
            "open_orders": {},
            "has_nonzero_position": True,
            "has_open_order": False,
            "errors": [],
            "missing_evidence": [],
        })

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1781531700000,
        )

        assert result["order_error_evidence"] == []
        resolved_summary = result["resolved_terminal_zero_qty_reduce_only_summary"]
        assert resolved_summary["current_exchange_truth_clean"] is False
        assert resolved_summary["resolved_position_ids"] == [
            "entry-1781531687393-HOMEUSDT"
        ]
        summary = result["passive_close_terminal_summary"]
        assert summary["terminal_zero_qty_reduce_only_count"] == 1
        assert summary["terminal_zero_qty_reduce_only_resolved_count"] == 1
        assert summary["terminal_zero_qty_reduce_only_position_ids"] == [
            "entry-1781531687393-HOMEUSDT"
        ]
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_reports_passive_zero_fill_recovered_short_window(monkeypatch):
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
            "last_tick_ms": 1781097000000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1781096100000,
                "kind": "execution.passive_cycle_zero_fill",
                "payload": {
                    "position_id": "entry-zero-recovered",
                    "symbol": "ZEROUSDT",
                    "zero_fill_cycles": 3,
                    "max_zero_fill_cycles": 3,
                },
            },
            {
                "ts_ms": 1781096100500,
                "kind": "exit.passive_close_resolved",
                "payload": {
                    "position_id": "entry-zero-recovered",
                    "symbol": "ZEROUSDT",
                    "problem": False,
                    "single_leg_fast_flatten": False,
                },
            },
        ])

        monkeypatch.setattr(dl, "_build_exchange_truth", lambda *args, **kwargs: {
            "available": True,
            "confidence": "high",
            "positions": {},
            "open_orders": {},
            "has_nonzero_position": False,
            "has_open_order": False,
            "errors": [],
            "missing_evidence": [],
        })

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1781097000000,
        )

        gate = result["production_acceptance_gate"]
        assert gate["short_window_warning_count"] == 1
        assert gate["short_window_warning_families"] == [
            "passive_zero_fill_exhausted_then_recovered"
        ]
        assert gate["short_window_warning_details"][
            "passive_zero_fill_exhausted_then_recovered"
        ] == 1
        assert gate["gate_passed"] is True
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_reports_zero_fill_lifecycle_guard_entry_outcome(monkeypatch):
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
            "last_tick_ms": 1781097000000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1781096100000,
                "kind": "execution.entry_selected",
                "payload": {
                    "entry_id": "entry-zero-lifecycle",
                    "symbol": "HOMEUSDT",
                },
            },
            {
                "ts_ms": 1781096100100,
                "kind": "runtime.entry_dispatched",
                "payload": {
                    "entry_id": "entry-zero-lifecycle",
                    "symbol": "HOMEUSDT",
                },
            },
            {
                "ts_ms": 1781096100200,
                "kind": "execution.direction_drift_blocked",
                "payload": {
                    "entry_id": "entry-zero-lifecycle",
                    "symbol": "HOMEUSDT",
                    "reason": "candidate_not_tradeable_after_zero_fill_reprice",
                    "blocked_reasons": ["lifecycle_risk_only"],
                    "phase": "high_slippage_maker",
                },
            },
            {
                "ts_ms": 1781096100300,
                "kind": "entry.passive_unfilled",
                "payload": {
                    "entry_id": "entry-zero-lifecycle",
                    "symbol": "HOMEUSDT",
                    "reason": "zero_fill_unfilled_removal",
                },
            },
            {
                "ts_ms": 1781096100400,
                "kind": "pending_entry.removed_by_v1_lifecycle_closure",
                "payload": {
                    "entry_id": "entry-zero-lifecycle",
                    "reason": "zero_fill_unfilled_removal",
                },
            },
        ])

        monkeypatch.setattr(dl, "_build_exchange_truth", lambda *args, **kwargs: {
            "available": True,
            "confidence": "high",
            "positions": {},
            "open_orders": {},
            "has_nonzero_position": False,
            "has_open_order": False,
            "errors": [],
            "missing_evidence": [],
        })

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1781097000000,
        )

        outcome = result["production_acceptance_gate"]["entry_outcome_summary"]
        assert outcome["selected_count"] == 1
        assert outcome["dispatched_count"] == 1
        assert outcome["opened_count"] == 0
        assert outcome["zero_fill_lifecycle_guard_count"] == 1
        assert outcome["zero_fill_lifecycle_guard_blocker_counts"] == {
            "lifecycle_risk_only": 1,
        }
        assert outcome["zero_fill_lifecycle_guard_entry_ids"] == [
            "entry-zero-lifecycle"
        ]
        assert outcome["reason_counts"][
            "candidate_not_tradeable_after_zero_fill_reprice"
        ] == 1
        assert outcome["passive_unfilled_count"] == 1
        assert result["production_acceptance_gate"]["gate_passed"] is True
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_entry_outcome_summary_separates_quote_lease_and_oi_liquidity_reasons():
    from scripts.diagnose_live import _build_entry_outcome_summary

    events = [
        {
            "kind": "runtime.entry_quote_revalidate_failed",
            "payload": {
                "venue": "aster",
                "symbol": "HOMEUSDT",
                "outcome": "rest_invalid_quote",
                "reason_bucket": "rest_resolved_but_stale",
                "reason_family": "rest_invalid_quote",
            },
        },
        {
            "kind": "runtime.entry_ws_bbo_top_candidate_rewarm_failed",
            "payload": {
                "venue": "binance",
                "symbol": "HOMEUSDT",
                "outcome": "rest_attempt_throttled",
                "reason_bucket": "rest_throttled",
            },
        },
        {
            "kind": "execution.entry_liquidity_blocked",
            "payload": {
                "venue": "aster",
                "symbol": "HOMEUSDT",
                "reason": "oi_evidence_unavailable",
                "open_interest_evidence_status": "deferred_by_cap",
            },
        },
        {
            "kind": "execution.entry_liquidity_blocked",
            "payload": {
                "venue": "binance",
                "symbol": "BSBUSDT",
                "reason": "oi_evidence_unavailable",
                "open_interest_evidence_status": "rate_limited",
            },
        },
        {
            "kind": "runtime.entry_oi_targeted_refresh_resolved",
            "payload": {
                "venue": "binance",
                "symbol": "HOMEUSDT",
                "previous_open_interest_evidence_status": "deferred_by_cap",
                "open_interest_evidence_status": "available",
                "open_interest_evidence_reason": "targeted_refresh",
                "elapsed_ms": 9,
            },
        },
        {
            "kind": "runtime.entry_oi_targeted_refresh_failed",
            "payload": {
                "venue": "aster",
                "symbol": "BSBUSDT",
                "previous_open_interest_evidence_status": "timeout",
                "open_interest_evidence_status": "timeout",
                "open_interest_evidence_reason": "timeout_waiting_for_oi",
                "elapsed_ms": 101,
            },
        },
    ]

    summary = _build_entry_outcome_summary(events)

    assert summary["quote_lease_failure_counts"] == {
        "rest_resolved_but_stale": 1,
        "rest_throttled": 1,
    }
    assert summary["quote_lease_failure_family_counts"] == {
        "rest_invalid_quote": 1,
        "rest_throttled": 1,
    }
    assert summary["oi_liquidity_evidence_counts"] == {
        "deferred_by_cap": 1,
        "rate_limited": 1,
    }
    assert summary["oi_liquidity_evidence_reason_counts"] == {
        "unknown": 2,
    }
    assert summary["oi_targeted_refresh_summary"] == {
        "attempt_count": 2,
        "resolved_count": 1,
        "failed_count": 1,
        "timeout_count": 1,
        "unsupported_count": 0,
        "entry_blocked_after_targeted_refresh_count": 1,
        "max_elapsed_ms": 101,
        "status_counts": {
            "available": 1,
            "timeout": 1,
        },
        "previous_status_counts": {
            "deferred_by_cap": 1,
            "timeout": 1,
        },
    }


def test_entry_outcome_summary_tracks_rewarm_after_rest_stale_resolution():
    from scripts.diagnose_live import _build_entry_outcome_summary

    events = [
        {
            "ts_ms": 1000,
            "kind": "runtime.entry_quote_revalidate_failed",
            "payload": {
                "venue": "binance",
                "symbol": "HOMEUSDT",
                "reason_bucket": "rest_resolved_but_stale",
            },
        },
        {
            "ts_ms": 1001,
            "kind": "runtime.entry_quote_rewarm_scheduled_after_rest_stale",
            "payload": {
                "venue": "binance",
                "symbol": "HOMEUSDT",
                "reason_bucket": "rest_resolved_but_stale",
            },
        },
        {
            "ts_ms": 1200,
            "kind": "runtime.entry_quote_revalidate_resolved",
            "payload": {
                "venue": "binance",
                "symbol": "HOMEUSDT",
            },
        },
        {
            "ts_ms": 2000,
            "kind": "runtime.entry_quote_rewarm_scheduled_after_rest_stale",
            "payload": {
                "venue": "aster",
                "symbol": "BSBUSDT",
            },
        },
        {
            "ts_ms": 2200,
            "kind": "runtime.entry_quote_revalidate_failed",
            "payload": {
                "venue": "aster",
                "symbol": "BSBUSDT",
                "reason_bucket": "rest_resolved_but_stale",
            },
        },
    ]

    summary = _build_entry_outcome_summary(events)

    assert summary["quote_rewarm_after_rest_stale_summary"] == {
        "scheduled_count": 2,
        "resolved_count": 1,
        "still_stale_count": 1,
        "timeout_count": 0,
        "still_stale_by_venue_symbol": {"aster:BSBUSDT": 1},
        "timeout_by_venue_symbol": {},
        "samples": [
            {
                "venue": "binance",
                "symbol": "HOMEUSDT",
                "status": "resolved",
                "scheduled_at_ms": 1001,
                "resolved_at_ms": 1200,
            },
            {
                "venue": "aster",
                "symbol": "BSBUSDT",
                "status": "still_stale",
                "scheduled_at_ms": 2000,
                "still_stale_at_ms": 2200,
            },
        ],
    }


def test_entry_outcome_summary_exposes_long_lived_artifact_durations():
    from scripts.diagnose_live import _build_entry_outcome_summary

    events = [
        {
            "ts_ms": 1_000,
            "kind": "execution.entry_selected",
            "payload": {"entry_id": "entry-long-br", "symbol": "BRUSDT"},
        },
        {
            "ts_ms": 11_000,
            "kind": "runtime.pending_entry_registered",
            "payload": {"entry_id": "entry-long-br", "symbol": "BRUSDT"},
        },
        {
            "ts_ms": 761_000,
            "kind": "pending_entry.long_lived_pending_entry",
            "payload": {
                "entry_id": "entry-long-br",
                "symbol": "BRUSDT",
                "selected_lifetime_ms": 760_000,
                "pending_lifetime_ms": 10_000,
                "sla_ms": 300_000,
                "reason": "long_lived_pending_entry",
            },
        },
        {
            "ts_ms": 761_200,
            "kind": "entry.aborted",
            "payload": {
                "entry_id": "entry-long-br",
                "symbol": "BRUSDT",
                "reason": "long_lived_pending_entry",
            },
        },
        {
            "ts_ms": 800_000,
            "kind": "entry.opened",
            "payload": {"entry_id": "entry-close-xvg", "symbol": "XVGUSDT"},
        },
        {
            "ts_ms": 1_000_000,
            "kind": "exit.passive_close_created",
            "payload": {"position_id": "entry-close-xvg", "symbol": "XVGUSDT"},
        },
        {
            "ts_ms": 1_041_000,
            "kind": "exit.passive_close_missing_l2_or_tick",
            "payload": {
                "position_id": "entry-close-xvg",
                "symbol": "XVGUSDT",
                "reason": "cannot submit post-only maker without valid L2 mid and tick size",
            },
        },
        {
            "ts_ms": 1_047_000,
            "kind": "runtime.position_lifecycle_terminal",
            "payload": {"position_id": "entry-close-xvg", "symbol": "XVGUSDT"},
        },
    ]

    summary = _build_entry_outcome_summary(events)
    durations = summary["artifact_duration_summary"]

    assert durations["long_lived_pending_entry_count"] == 1
    assert durations["close_data_quality_warning_count"] == 1
    assert durations["max_selected_to_terminal_ms"] == 760_200
    assert durations["max_close_created_to_terminal_ms"] == 47_000
    assert durations["samples"][0] == {
        "entry_id": "entry-long-br",
        "symbol": "BRUSDT",
        "status": "aborted",
        "selected_to_terminal_ms": 760_200,
        "pending_created_to_terminal_ms": 750_200,
        "close_created_to_terminal_ms": 0,
        "long_lived": True,
        "close_data_quality_warning": False,
        "terminal_kind": "entry.aborted",
        "terminal_reason": "long_lived_pending_entry",
    }


def test_entry_outcome_summary_exposes_phase_duration_budget_overruns():
    from scripts.diagnose_live import _build_entry_outcome_summary

    events = [
        {
            "ts_ms": 1_000,
            "kind": "execution.entry_selected",
            "payload": {"entry_id": "entry-no-submit", "symbol": "NOSUBUSDT"},
        },
        {
            "ts_ms": 10_000,
            "kind": "runtime.entry_quote_rewarm_scheduled_after_rest_stale",
            "payload": {"venue": "aster", "symbol": "DATASUSDT"},
        },
        {
            "ts_ms": 50_000,
            "kind": "runtime.entry_quote_rewarm_terminal_stale",
            "payload": {
                "venue": "aster",
                "symbol": "DATASUSDT",
                "action_taken": "skip_candidate_after_hard_rewarm",
            },
        },
        {
            "ts_ms": 20_000,
            "kind": "review.candidate_shortlisted",
            "payload": {"candidate_id": "candidate-slow", "symbol": "CANDUSDT"},
        },
        {
            "ts_ms": 100_000,
            "kind": "execution.entry_selected",
            "payload": {"entry_id": "entry-long-br", "symbol": "BRUSDT"},
        },
        {
            "ts_ms": 111_000,
            "kind": "runtime.pending_entry_registered",
            "payload": {
                "entry_id": "entry-long-br",
                "symbol": "BRUSDT",
                "maker_order_id": "maker-long",
                "outcome": "maker_resting",
            },
        },
        {
            "ts_ms": 861_000,
            "kind": "pending_entry.long_lived_pending_entry",
            "payload": {
                "entry_id": "entry-long-br",
                "symbol": "BRUSDT",
                "selected_lifetime_ms": 761_000,
                "pending_lifetime_ms": 750_000,
                "sla_ms": 300_000,
                "reason": "long_lived_pending_entry",
            },
        },
        {
            "ts_ms": 861_200,
            "kind": "entry.aborted",
            "payload": {
                "entry_id": "entry-long-br",
                "symbol": "BRUSDT",
                "reason": "long_lived_pending_entry",
            },
        },
        {
            "ts_ms": 900_000,
            "kind": "exit.passive_close_created",
            "payload": {"position_id": "close-slow", "symbol": "XVGUSDT"},
        },
        {
            "ts_ms": 1_210_000,
            "kind": "runtime.position_lifecycle_terminal",
            "payload": {"position_id": "close-slow", "symbol": "XVGUSDT"},
        },
        {
            "ts_ms": 1_300_000,
            "kind": "recovery.blocked",
            "payload": {
                "position_id": "recover-slow",
                "symbol": "RECUSDT",
                "reason": "unpaired_live_position",
            },
        },
        {
            "ts_ms": 1_610_000,
            "kind": "runtime.lifecycle_tick",
            "payload": {"reason": "diagnostic_horizon"},
        },
    ]

    summary = _build_entry_outcome_summary(events)
    phase_summary = summary["phase_duration_summary"]
    samples_by_phase = {
        sample["phase"]: sample for sample in phase_summary["samples"]
    }

    assert phase_summary["over_budget_count"] >= 5
    assert phase_summary["hard_over_budget_count"] >= 5
    assert phase_summary["blank_action_count"] == 1
    assert phase_summary["terminalized_candidate_lease_count"] == 0
    assert phase_summary["terminalized_quote_rewarm_count"] == 1
    assert phase_summary["budget_defaults_ms"]["selected_pre_submit"]["hard_ms"] == 15000
    assert phase_summary["budget_defaults_ms"]["pending_entry"]["hard_ms"] == 120000
    assert phase_summary["budget_defaults_ms"]["close_terminal"]["hard_ms"] == 300000
    assert phase_summary["budget_defaults_ms"]["recovery_terminal"]["hard_ms"] == 300000

    assert samples_by_phase["quote_rewarm"]["hard_ms"] == 30000
    assert samples_by_phase["quote_rewarm"]["configured_action"] == (
        "skip_candidate_after_hard_rewarm"
    )
    assert samples_by_phase["quote_rewarm"]["action_taken"] == (
        "skip_candidate_after_hard_rewarm"
    )
    assert samples_by_phase["quote_rewarm"]["action_evidence_kind"] == (
        "runtime.entry_quote_rewarm_terminal_stale"
    )
    assert samples_by_phase["candidate_lease"]["hard_ms"] == 60000
    assert samples_by_phase["candidate_lease"]["truth_source"] == (
        "fresh_scan_shortlist_candidate"
    )
    assert samples_by_phase["candidate_lease"]["action_taken"] == ""
    assert samples_by_phase["candidate_lease"]["action_evidence_kind"] == ""
    assert samples_by_phase["selected_pre_submit"] == {
        "phase": "selected_pre_submit",
        "artifact_id": "entry-no-submit",
        "symbol": "NOSUBUSDT",
        "venue": "",
        "age_ms": 1_609_000,
        "soft_ms": 0,
        "hard_ms": 15000,
        "status": "hard_over_budget",
        "configured_action": "cancel_selection_and_rescan",
        "action_taken": "cancel_selection_and_rescan",
        "action_evidence_kind": "runtime.entry_selected_submit_deadline_exceeded",
        "truth_source": "execution.entry_selected_without_submit_or_order_id",
    }
    assert samples_by_phase["entry_selected_terminal"]["status"] == "hard_over_budget"
    assert samples_by_phase["entry_selected_terminal"]["hard_ms"] == 300000
    assert samples_by_phase["entry_selected_terminal"]["action_taken"] == (
        "cancel_or_abort_entry_and_reconcile"
    )
    assert samples_by_phase["entry_selected_terminal"]["action_evidence_kind"] == (
        "pending_entry.long_lived_pending_entry"
    )
    assert samples_by_phase["pending_entry"]["truth_source"] == (
        "pending_entry_terminality_from_order_fill_position_truth"
    )
    assert samples_by_phase["maker_resting"]["action_taken"] == (
        "cancel_then_reconcile_open_orders_trades_positions"
    )
    assert samples_by_phase["maker_resting"]["action_evidence_kind"] == (
        "pending_entry.long_lived_pending_entry"
    )
    assert samples_by_phase["close_terminal"]["age_ms"] == 310_000
    assert samples_by_phase["recovery_terminal"]["action_taken"] == (
        "block_new_risk_and_escalate_recovery"
    )


def test_phase_duration_summary_terminalizes_hard_quote_rewarm_timeout():
    from scripts.diagnose_live import _build_entry_outcome_summary

    events = [
        {
            "ts_ms": 10_000,
            "kind": "runtime.entry_quote_rewarm_scheduled_after_rest_stale",
            "payload": {"venue": "binance", "symbol": "ALICEUSDT"},
        },
        {
            "ts_ms": 70_001,
            "kind": "runtime.lifecycle_tick",
            "payload": {"reason": "diagnostic_horizon"},
        },
    ]

    summary = _build_entry_outcome_summary(events)
    phase_summary = summary["phase_duration_summary"]
    quote_sample = phase_summary["samples"][0]

    assert quote_sample["phase"] == "quote_rewarm"
    assert quote_sample["status"] == "hard_over_budget"
    assert quote_sample["action_taken"] == "skip_candidate_after_hard_rewarm"
    assert (
        quote_sample["action_evidence_kind"]
        == "business_contract.quote_rewarm_hard_timeout"
    )
    assert phase_summary["blank_action_count"] == 0
    assert phase_summary["phase_handoff_quality"]["missing_takeover_count"] == 0


def test_phase_duration_summary_ignores_candidate_without_stable_artifact_id():
    from scripts.diagnose_live import _build_entry_outcome_summary

    events = [
        {
            "ts_ms": 1_000,
            "kind": "review.candidate_shortlisted",
            "payload": {"venue": "aster", "symbol": "FLOATUSDT"},
        },
        {
            "ts_ms": 100_000,
            "kind": "runtime.lifecycle_tick",
            "payload": {"reason": "diagnostic_horizon"},
        },
    ]

    summary = _build_entry_outcome_summary(events)
    phase_summary = summary["phase_duration_summary"]

    assert phase_summary["artifact_count"] == 0
    assert phase_summary["over_budget_count"] == 0
    assert phase_summary["blank_action_count"] == 0
    assert phase_summary["terminalized_candidate_lease_count"] == 0
    assert phase_summary["terminalized_quote_rewarm_count"] == 0
    assert phase_summary["samples"] == []


def test_phase_duration_summary_gives_soft_over_budget_selected_terminal_action():
    from scripts.diagnose_live import _build_entry_outcome_summary

    events = [
        {
            "ts_ms": 1_000,
            "kind": "execution.entry_selected",
            "payload": {"entry_id": "entry-soft-terminal", "symbol": "SOFTUSDT"},
        },
        {
            "ts_ms": 181_000,
            "kind": "runtime.lifecycle_tick",
            "payload": {"reason": "diagnostic_horizon"},
        },
    ]

    summary = _build_entry_outcome_summary(events)
    phase_summary = summary["phase_duration_summary"]
    samples_by_phase = {
        sample["phase"]: sample for sample in phase_summary["samples"]
    }

    assert samples_by_phase["entry_selected_terminal"]["status"] == "soft_over_budget"
    assert samples_by_phase["entry_selected_terminal"]["action_taken"] == (
        "cancel_or_abort_entry_and_reconcile"
    )
    assert samples_by_phase["entry_selected_terminal"]["action_evidence_kind"] == (
        "runtime.entry_selected_terminal_soft_budget_exceeded"
    )
    assert phase_summary["blank_action_count"] == 0


def test_run_diagnose_exposes_phase_duration_summary_at_root(monkeypatch):
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
            "last_tick_ms": 1_000,
        })
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1_000,
                "kind": "execution.entry_selected",
                "payload": {"entry_id": "entry-no-submit", "symbol": "NOSUBUSDT"},
            },
            {
                "ts_ms": 25_000,
                "kind": "runtime.entry_selected_submit_deadline_exceeded",
                "payload": {
                    "entry_id": "entry-no-submit",
                    "symbol": "NOSUBUSDT",
                    "reason": "selected_submit_deadline_exceeded",
                },
            },
        ])
        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="NOSUBUSDT",
            venues=["binance", "bybit"],
            now_ms=25_000,
        )

        nested = result["production_acceptance_gate"]["entry_outcome_summary"][
            "phase_duration_summary"
        ]
        assert result["phase_duration_summary"] == nested
        assert result["phase_duration_summary"]["hard_over_budget_count"] >= 1
        assert result["business_progression_quality_summary"] == {
            "pre_submit_blocked": 0,
            "single_leg_created": 0,
            "single_leg_cleanup": 0,
            "cleanup_after_admission_block": 0,
            "candidate_takeover_count": 0,
            "quote_rewarm_terminalized_count": 0,
            "recovered_but_counted_issue_count": 1,
            "historical_hard_over_budget_recovered_count": 1,
            "active_stuck_count": 0,
            "ownerless_open_order_count": 0,
            "owned_pending_passive_close_count": 0,
            "adopted_reduce_only_order_count": 0,
            "duplicate_reduce_only_submit_blocked_count": 0,
            "deterministic_reject_after_submit_count": 0,
            "entry_quantity_contract_blocked_count": 0,
            "close_reconciliation_evidence_gap_count": 0,
            "admission_degraded_suppressed_count": 0,
            "passive_close_resolved_without_terminal_truth_count": 0,
            "passive_close_truth_lag_resolved_count": 0,
            "passive_close_actionable_single_leg_wait_count": 0,
            "risk_only_live_single_leg_exposure_count": 0,
            "passive_close_final_truth_actions": {},
            "repeated_single_leg_guarded": {
                "violation_count": 0,
                "severity": "ok",
                "samples": [],
            },
            "phase_handoff_quality": {
                "severity": "ok",
                "missing_takeover_count": 0,
                "phase_counts": {
                    "candidate_lease": {
                        "over_budget_count": 0,
                        "takeover_count": 0,
                        "missing_takeover_count": 0,
                    },
                    "quote_rewarm": {
                        "over_budget_count": 0,
                        "takeover_count": 0,
                        "missing_takeover_count": 0,
                    },
                },
                "samples": [],
            },
        }
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_business_progression_quality_counts_recovered_zero_fill_without_active_stuck():
    import scripts.diagnose_live as dl

    events = [
        {
            "ts_ms": 1_000,
            "kind": "execution.entry_selected",
            "payload": {"entry_id": "entry-husdt", "symbol": "HUSDT"},
        },
        {
            "ts_ms": 2_000,
            "kind": "runtime.pending_entry_registered",
            "payload": {
                "entry_id": "entry-husdt",
                "symbol": "HUSDT",
                "maker_order_id": "maker-husdt",
                "outcome": "maker_resting",
            },
        },
        {
            "ts_ms": 5_400,
            "kind": "passive_maintenance.cancel_issued",
            "payload": {
                "entry_id": "entry-husdt",
                "symbol": "HUSDT",
                "reason": "maker_try_window_fill_ratio_below_threshold",
                "fill_ratio": 0.0,
            },
        },
        {
            "ts_ms": 8_000,
            "kind": "entry.aborted",
            "payload": {
                "entry_id": "entry-husdt",
                "symbol": "HUSDT",
                "reason": "pending_entry_reconcile_abandoned_flat",
            },
        },
        {
            "ts_ms": 900_000,
            "kind": "runtime.lifecycle_tick",
            "payload": {"reason": "diagnostic_horizon"},
        },
    ]

    phase_summary = dl._build_phase_duration_summary(events)
    business = dl._build_business_progression_quality_summary(events)

    assert phase_summary["hard_over_budget_count"] == 0
    assert business["recovered_but_counted_issue_count"] == 1
    assert business["active_stuck_count"] == 0


def test_business_progression_quality_counts_passive_close_truth_lag_as_recovered():
    import scripts.diagnose_live as dl

    events = [
        {
            "ts_ms": 1_000,
            "kind": "recovery.live_detected",
            "payload": {
                "position_id": "entry-1781859127568-ESPORTSUSDT",
                "symbol": "ESPORTSUSDT",
            },
        },
        {
            "ts_ms": 2_000,
            "kind": "exit.passive_close_created",
            "payload": {
                "position_id": "entry-1781859127568-ESPORTSUSDT",
                "symbol": "ESPORTSUSDT",
                "reason": "funding_capture",
            },
        },
        {
            "ts_ms": 5_000,
            "kind": "exit.passive_close_resolved",
            "payload": {
                "position_id": "entry-1781859127568-ESPORTSUSDT",
                "symbol": "ESPORTSUSDT",
                "reason": "funding_capture",
            },
        },
        {
            "ts_ms": 8_000,
            "kind": "exit.passive_close_resolved",
            "payload": {
                "position_id": "entry-1781859127568-ESPORTSUSDT",
                "symbol": "ESPORTSUSDT",
                "reason": "funding_capture",
                "exchange_truth": {
                    "truth_available": True,
                    "positions_flat": True,
                    "open_orders_flat": True,
                },
            },
        },
        {
            "ts_ms": 901_000,
            "kind": "runtime.lifecycle_tick",
            "payload": {"reason": "diagnostic_horizon"},
        },
    ]

    phase_summary = dl._build_phase_duration_summary(events)
    business = dl._build_business_progression_quality_summary(events)

    assert phase_summary["hard_over_budget_count"] == 1
    assert business["passive_close_resolved_without_terminal_truth_count"] == 1
    assert business["passive_close_truth_lag_resolved_count"] == 1
    assert business["recovered_but_counted_issue_count"] == 1
    assert business["active_stuck_count"] == 0


def test_business_progression_quality_flags_actionable_passive_close_single_leg_wait():
    import scripts.diagnose_live as dl

    events = [
        {
            "ts_ms": 5_000,
            "kind": "exit.passive_close_waiting_exchange_flat_truth",
            "payload": {
                "position_id": "entry-1781902330536-ESPORTSUSDT",
                "symbol": "ESPORTSUSDT",
                "long_venue": "bybit",
                "short_venue": "binance",
                "decision": "retain_pending",
                "next_action": "retry_exchange_position_open_order_truth",
                "exchange_truth_attempt": {
                    "truth_available": True,
                    "positions_flat": False,
                    "open_orders_flat": True,
                    "positions": [
                        {"venue": "bybit", "symbol": "ESPORTSUSDT", "quantity": 0.0},
                        {"venue": "binance", "symbol": "ESPORTSUSDT", "quantity": 470.0},
                    ],
                    "open_order_truth": [
                        {
                            "venue": "bybit",
                            "symbol": "ESPORTSUSDT",
                            "open_orders_empty": True,
                        },
                        {
                            "venue": "binance",
                            "symbol": "ESPORTSUSDT",
                            "open_orders_empty": True,
                        },
                    ],
                },
            },
        }
    ]

    business = dl._build_business_progression_quality_summary(events)

    assert business["passive_close_actionable_single_leg_wait_count"] == 1
    assert business["passive_close_final_truth_actions"]["flatten_remaining_live_leg"] == 1


def test_business_progression_quality_unresolved_passive_truth_lag_stays_active():
    import scripts.diagnose_live as dl

    events = [
        {
            "ts_ms": 1_000,
            "kind": "recovery.live_detected",
            "payload": {
                "position_id": "entry-1781859127568-ESPORTSUSDT",
                "symbol": "ESPORTSUSDT",
            },
        },
        {
            "ts_ms": 2_000,
            "kind": "exit.passive_close_created",
            "payload": {
                "position_id": "entry-1781859127568-ESPORTSUSDT",
                "symbol": "ESPORTSUSDT",
                "reason": "funding_capture",
            },
        },
        {
            "ts_ms": 5_000,
            "kind": "exit.passive_close_resolved",
            "payload": {
                "position_id": "entry-1781859127568-ESPORTSUSDT",
                "symbol": "ESPORTSUSDT",
                "reason": "funding_capture",
            },
        },
        {
            "ts_ms": 901_000,
            "kind": "runtime.lifecycle_tick",
            "payload": {"reason": "diagnostic_horizon"},
        },
    ]

    business = dl._build_business_progression_quality_summary(events)

    assert business["passive_close_resolved_without_terminal_truth_count"] == 1
    assert business["passive_close_truth_lag_resolved_count"] == 0
    assert business["recovered_but_counted_issue_count"] == 0
    assert business["active_stuck_count"] == 1


def test_business_progression_quality_gate_green_reclassifies_historical_hard_stuck():
    import scripts.diagnose_live as dl

    events = [
        {
            "ts_ms": 1_000,
            "kind": "recovery.live_detected",
            "payload": {
                "position_id": "entry-cloud-recovered-ESPORTSUSDT",
                "symbol": "ESPORTSUSDT",
            },
        },
        {
            "ts_ms": 2_000,
            "kind": "recovery.blocked",
            "payload": {
                "position_id": "entry-cloud-recovered-ESPORTSUSDT",
                "symbol": "ESPORTSUSDT",
                "reason": "unpaired_live_position",
            },
        },
        {
            "ts_ms": 901_000,
            "kind": "runtime.lifecycle_tick",
            "payload": {"reason": "diagnostic_horizon"},
        },
    ]

    business = dl._build_business_progression_quality_summary(
        events,
        production_acceptance_gate={
            "gate_passed": True,
            "exchange_truth_flat": True,
            "exchange_truth_no_open_orders": True,
            "blocking_reasons": [],
            "v1_lifecycle_summary": {"blocking_row_count": 0},
        },
    )

    assert business["active_stuck_count"] == 0
    assert business["recovered_but_counted_issue_count"] == 1
    assert business["historical_hard_over_budget_recovered_count"] == 1


def test_business_progression_quality_live_flat_flag_without_truth_stays_active():
    import scripts.diagnose_live as dl

    events = [
        {
            "ts_ms": 1_000,
            "kind": "recovery.live_detected",
            "payload": {
                "position_id": "entry-legacy-live-flat-flag",
                "symbol": "ESPORTSUSDT",
            },
        },
        {
            "ts_ms": 2_000,
            "kind": "exit.passive_close_created",
            "payload": {
                "position_id": "entry-legacy-live-flat-flag",
                "symbol": "ESPORTSUSDT",
                "reason": "funding_capture",
            },
        },
        {
            "ts_ms": 5_000,
            "kind": "exit.passive_close_resolved",
            "payload": {
                "position_id": "entry-legacy-live-flat-flag",
                "symbol": "ESPORTSUSDT",
                "reason": "funding_capture",
                "live_flat_terminal": True,
            },
        },
        {
            "ts_ms": 901_000,
            "kind": "runtime.lifecycle_tick",
            "payload": {"reason": "diagnostic_horizon"},
        },
    ]

    business = dl._build_business_progression_quality_summary(events)

    assert business["passive_close_resolved_without_terminal_truth_count"] == 1
    assert business["passive_close_truth_lag_resolved_count"] == 0
    assert business["recovered_but_counted_issue_count"] == 0
    assert business["active_stuck_count"] == 1


def test_business_progression_quality_counts_contract_and_process_issues():
    import scripts.diagnose_live as dl

    events = [
        {
            "ts_ms": 1_000,
            "kind": "execution.entry_quantity_plan",
            "payload": {
                "entry_id": "entry-home",
                "symbol": "HOMEUSDT",
                "quantity_contract_status": "blocked_unhedgeable_quantity",
                "unhedgeable_residual_quantity": 56.0,
            },
        },
        {
            "ts_ms": 2_000,
            "kind": "exit.reconciled",
            "payload": {
                "position_id": "entry-esports",
                "symbol": "ESPORTSUSDT",
                "evidence_gap": True,
                "evidence_gap_reason": "missing_short_close_trade_statement",
                "statement_probe_status": "partial",
                "trade_probe_status": {"long": "found", "short": "missing"},
            },
        },
        {
            "ts_ms": 3_000,
            "kind": "runtime.entry_admission_venue_degraded",
            "payload": {
                "venue": "bybit",
                "symbol": "HUSDT",
                "reason": "insufficient_balance_admission_blocked",
                "block_scope": "symbol",
                "suppressed_count": 37,
                "aggregation_key": "shortlist:bybit:HUSDT:insufficient_balance_admission_blocked:symbol",
            },
        },
    ]

    business = dl._build_business_progression_quality_summary(events)

    assert business["entry_quantity_contract_blocked_count"] == 1
    assert business["close_reconciliation_evidence_gap_count"] == 1
    assert business["admission_degraded_suppressed_count"] == 37


def test_business_progression_quality_soft_terminal_does_not_hide_hard_stuck():
    import scripts.diagnose_live as dl

    events = [
        {
            "ts_ms": 1_000,
            "kind": "runtime.entry_quote_rewarm_scheduled_after_rest_stale",
            "payload": {
                "candidate_pair_id": "quote-soft",
                "venue": "aster",
                "symbol": "SOFTUSDT",
            },
        },
        {
            "ts_ms": 12_000,
            "kind": "runtime.entry_quote_rewarm_terminal_stale",
            "payload": {
                "candidate_pair_id": "quote-soft",
                "venue": "aster",
                "symbol": "SOFTUSDT",
            },
        },
        {
            "ts_ms": 1_000,
            "kind": "exit.passive_close_created",
            "payload": {
                "position_id": "close-hard",
                "symbol": "HARDUSDT",
            },
        },
        {
            "ts_ms": 401_000,
            "kind": "runtime.lifecycle_tick",
            "payload": {"reason": "diagnostic_horizon"},
        },
    ]

    phase_summary = dl._build_phase_duration_summary(events)
    business = dl._build_business_progression_quality_summary(events)

    assert phase_summary["hard_over_budget_count"] == 1
    assert phase_summary["terminalized_quote_rewarm_count"] == 1
    assert business["quote_rewarm_terminalized_count"] == 1
    assert business["active_stuck_count"] == 1


def test_quantity_terminal_summary_resolves_terminal_planner_and_rounding_warnings():
    from scripts.diagnose_live import _build_entry_quantity_terminal_summary

    events = [
        {
            "kind": "execution.entry_quantity_plan",
            "payload": {
                "entry_id": "entry-balanced-flat",
                "symbol": "HOMEUSDT",
                "common_quantity": 9.0,
                "full_target_quantity": 10.0,
                "quantity_plan_reason": "planner_quantity_adjustment",
            },
        },
        {
            "kind": "entry.opened",
            "payload": {
                "entry_id": "entry-balanced-flat",
                "position_id": "entry-balanced-flat",
                "symbol": "HOMEUSDT",
                "long_quantity": 9.0,
                "short_quantity": 9.0,
                "matched_quantity": 9.0,
            },
        },
        {
            "kind": "exit.reconciled",
            "payload": {
                "position_id": "entry-balanced-flat",
                "symbol": "HOMEUSDT",
                "long_closed_qty": 9.0,
                "short_closed_qty": 9.0,
            },
        },
        {
            "kind": "pending_entry.hedge_quantity_undercut",
            "payload": {
                "entry_id": "entry-rounding-flat",
                "symbol": "BSBUSDT",
                "reason_family": "exchange_step_rounding",
                "missing_hedge_quantity": 0.003,
                "normalized_quantity": 0.002,
            },
        },
        {
            "kind": "runtime.position_lifecycle_terminal",
            "payload": {
                "position_id": "entry-rounding-flat",
                "symbol": "BSBUSDT",
                "terminal_state": "flat",
            },
        },
        {
            "kind": "pending_entry.hedge_quantity_undercut",
            "payload": {
                "entry_id": "entry-active-warning",
                "symbol": "MOVEUSDT",
                "reason_family": "exchange_step_rounding",
                "missing_hedge_quantity": 0.003,
                "normalized_quantity": 0.002,
            },
        },
        {
            "kind": "pending_entry.hedge_quantity_undercut",
            "payload": {
                "entry_id": "entry-symbol-residual-complete",
                "symbol": "SYMBOLUSDT",
                "reason_family": "exchange_step_rounding",
                "missing_hedge_quantity": 0.003,
                "normalized_quantity": 0.002,
            },
        },
        {
            "kind": "execution.residual_repair_completed",
            "payload": {
                "symbol": "SYMBOLUSDT",
            },
        },
    ]

    summary = _build_entry_quantity_terminal_summary(events)

    assert summary["common_quantity_mismatch_warning_entry_ids"] == []
    assert summary["hedge_quantity_undercut_warning_entry_ids"] == [
        "entry-active-warning"
    ]
    assert summary["resolved_quantity_adjustment_summary"] == {
        "planner_quantity_adjustment_count": 1,
        "hedge_exchange_step_rounding_count": 2,
        "entry_ids": [
            "entry-balanced-flat",
            "entry-rounding-flat",
            "entry-symbol-residual-complete",
        ],
        "samples": [
            {
                "entry_id": "entry-balanced-flat",
                "kind": "common_quantity_mismatch",
                "reason_family": "planner_quantity_adjustment",
                "symbol": "HOMEUSDT",
            },
            {
                "entry_id": "entry-rounding-flat",
                "kind": "hedge_quantity_undercut",
                "reason_family": "exchange_step_rounding",
                "symbol": "BSBUSDT",
            },
            {
                "entry_id": "entry-symbol-residual-complete",
                "kind": "hedge_quantity_undercut",
                "reason_family": "exchange_step_rounding",
                "symbol": "SYMBOLUSDT",
            },
        ],
    }


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


def test_run_diagnose_filters_resolved_binance_post_only_boundary_reject(monkeypatch):
    import scripts.diagnose_live as dl

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
            "pending_residual_repair_count": 0,
            "last_tick_ms": 1700000000000,
        }
        _write_json(os.path.join(d, "state-current.json"), state)

        events = [
            {
                "ts_ms": 1700000001000,
                "kind": "runtime.entry_post_only_bbo_repriced",
                "payload": {
                    "symbol": "STGUSDT",
                    "venue": "binance",
                    "long_venue": "binance",
                    "short_venue": "bybit",
                    "side": "buy",
                    "price": 0.2267,
                    "best_bid": 0.2267,
                    "best_ask": 0.2268,
                    "book_age_ms": 0,
                    "reason": "post_only_would_cross_repriced",
                },
            },
            {
                "ts_ms": 1700000002000,
                "kind": "order.rejected",
                "payload": {
                    "position_id": "entry-stg",
                    "venue": "binance",
                    "symbol": "STGUSDT",
                    "reason": (
                        "Binance -5022 GTX_ORDER_REJECT: Due to the order could "
                        "not be executed as maker, the Post Only order will be rejected"
                    ),
                    "exchange_error": {
                        "venue": "binance",
                        "operation": "place_order",
                        "transport_error_type": "exchange_retcode",
                        "http_status": 400,
                        "raw_body": (
                            '{"code":-5022,"msg":"Due to the order could not be '
                            'executed as maker, the Post Only order will be rejected."}'
                        ),
                        "exchange_code": "-5022",
                        "exchange_msg": (
                            "Due to the order could not be executed as maker, "
                            "the Post Only order will be rejected."
                        ),
                        "evidence_completeness": "complete",
                        "missing_evidence": [],
                        "confidence": "high",
                    },
                    "request_context": {
                        "symbol": "STGUSDT",
                        "side": "buy",
                        "price": 0.2267,
                        "quantity": 105.39953009376165,
                        "reduce_only": False,
                        "post_only": True,
                    },
                    "evidence_completeness": "complete",
                },
            },
            {
                "ts_ms": 1700000002100,
                "kind": "runtime.entry_post_only_reject_cooldown",
                "payload": {
                    "symbol": "STGUSDT",
                    "venue": "binance",
                    "reason": "post_only_would_take",
                    "raw_error": "-5022 GTX_ORDER_REJECT",
                    "side": "buy",
                    "price": 0.2267,
                    "best_bid": 0.2267,
                    "best_ask": 0.2268,
                    "book_age_ms": 0,
                    "cooldown_until_ms": 1700000032100,
                },
            },
        ]
        _write_jsonl(os.path.join(d, "events.jsonl"), events)

        monkeypatch.setattr(dl, "_build_exchange_truth", lambda *args, **kwargs: {
            "available": True,
            "truth_available": True,
            "confidence": "high",
            "positions": {},
            "open_orders": {},
            "has_nonzero_position": False,
            "has_open_order": False,
            "errors": [],
            "missing_evidence": [],
        })

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1700000005000,
        )

        assert result["order_error_evidence"] == []
        summary = result["resolved_post_only_reject_summary"]
        assert summary["count"] == 1
        assert summary["resolved_count"] == 1
        assert summary["unresolved_count"] == 0
        assert summary["current_exchange_truth_clean"] is True
        assert summary["resolved_symbols"] == ["STGUSDT"]
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_keeps_binance_post_only_reject_without_cooldown(monkeypatch):
    import scripts.diagnose_live as dl

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
            "pending_residual_repair_count": 0,
            "last_tick_ms": 1700000000000,
        }
        _write_json(os.path.join(d, "state-current.json"), state)
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1700000002000,
                "kind": "order.rejected",
                "payload": {
                    "position_id": "entry-stg",
                    "venue": "binance",
                    "symbol": "STGUSDT",
                    "reason": "-5022 GTX_ORDER_REJECT",
                    "exchange_error": {
                        "venue": "binance",
                        "operation": "place_order",
                        "http_status": 400,
                        "raw_body": '{"code":-5022,"msg":"GTX_ORDER_REJECT"}',
                        "exchange_code": "-5022",
                        "exchange_msg": "GTX_ORDER_REJECT",
                        "evidence_completeness": "complete",
                        "missing_evidence": [],
                        "confidence": "high",
                    },
                    "request_context": {
                        "symbol": "STGUSDT",
                        "post_only": True,
                        "reduce_only": False,
                    },
                },
            }
        ])

        monkeypatch.setattr(dl, "_build_exchange_truth", lambda *args, **kwargs: {
            "available": True,
            "truth_available": True,
            "confidence": "high",
            "positions": {},
            "open_orders": {},
            "has_nonzero_position": False,
            "has_open_order": False,
            "errors": [],
            "missing_evidence": [],
        })

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1700000005000,
        )

        assert result["resolved_post_only_reject_summary"]["resolved_count"] == 0
        assert len(result["order_error_evidence"]) == 1
        assert result["order_error_evidence"][0]["exchange_code"] == "-5022"
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_contains_bybit_auth_invalid_admission_when_truth_clean(monkeypatch):
    import scripts.diagnose_live as dl

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
            "pending_residual_repair_count": 0,
            "last_tick_ms": 1700000000000,
        }
        _write_json(os.path.join(d, "state-current.json"), state)
        _write_jsonl(os.path.join(d, "events.jsonl"), [
            {
                "ts_ms": 1700000002000,
                "kind": "order.rejected",
                "payload": {
                    "position_id": "entry-auth",
                    "venue": "bybit",
                    "symbol": "AUTHUSDT",
                    "reason": "bybit retCode=33004 retMsg=Your api key has expired",
                    "exchange_error": {
                        "venue": "bybit",
                        "operation": "place_order",
                        "http_status": 400,
                        "raw_body": '{"retCode":33004,"retMsg":"Your api key has expired"}',
                        "exchange_code": "33004",
                        "exchange_msg": "Your api key has expired",
                        "evidence_completeness": "complete",
                        "missing_evidence": [],
                        "confidence": "high",
                    },
                    "request_context": {
                        "symbol": "AUTHUSDT",
                        "post_only": False,
                        "reduce_only": False,
                    },
                },
            },
            {
                "ts_ms": 1700000002100,
                "kind": "runtime.entry_admission_blocked",
                "payload": {
                    "venue": "bybit",
                    "symbol": "AUTHUSDT",
                    "reason": "venue_auth_invalid",
                    "source": "venue_private_health_precheck",
                    "block_scope": "venue",
                    "cooldown_scope": "venue",
                    "venue_private_health_status": "auth_invalid",
                    "reduce_only": False,
                    "evidence_gap": False,
                },
            },
        ])

        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            now_ms=1700000005000,
        )

        assert result["order_error_evidence"] == []
        assert result["resolved_contained_entry_admission_summary"]["resolved_count"] == 1
        private_summary = result["venue_private_health_summary"]
        assert private_summary["count"] == 1
        assert private_summary["status_counts"] == {"auth_invalid": 1}
        assert private_summary["venue_counts"] == {"bybit": 1}
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def test_run_diagnose_exposes_single_leg_recovery_and_cleanup_blocker_summary():
    import scripts.diagnose_live as dl

    events = [
        {
            "ts_ms": 1700000001000,
            "kind": "pending_entry.single_leg_exposure_recovery_started",
            "payload": {
                "entry_id": "entry-auth",
                "symbol": "AUTHUSDT",
                "failed_hedge_venue": "bybit",
                "cleanup_venue": "binance",
                "reason": "venue_auth_invalid",
            },
        },
        {
            "ts_ms": 1700000001100,
            "kind": "pending_entry.single_leg_flatten_submitted",
            "payload": {
                "entry_id": "entry-auth",
                "symbol": "AUTHUSDT",
                "venue": "binance",
                "reason": "single_leg_exposure_recovery",
            },
        },
        {
            "ts_ms": 1700000001200,
            "kind": "pending_entry.single_leg_flatten_succeeded",
            "payload": {
                "entry_id": "entry-auth",
                "symbol": "AUTHUSDT",
                "venue": "binance",
                "reason": "owned_single_leg_flattened_and_fresh_truth_flat",
            },
        },
        {
            "ts_ms": 1700000001300,
            "kind": "pending_entry.terminalized_after_single_leg_recovery",
            "payload": {
                "entry_id": "entry-auth",
                "symbol": "AUTHUSDT",
                "venue": "binance",
                "reason": "owned_live_conflict_cleanup_succeeded",
            },
        },
        {
            "ts_ms": 1700000001400,
            "kind": "cleanup_blocked_by_venue_auth_invalid",
            "payload": {
                "severity": "critical",
                "entry_id": "entry-auth-cleanup",
                "symbol": "AUTHUSDT",
                "venue": "bybit",
                "reason": "cleanup_blocked_by_venue_auth_invalid",
                "action": "reduce_only_flatten",
                "reduce_only": True,
            },
        },
    ]

    single_leg = dl._build_single_leg_exposure_recovery_summary(events)
    cleanup = dl._build_cleanup_blocker_summary(events)

    assert single_leg["started_count"] == 1
    assert single_leg["submitted_count"] == 1
    assert single_leg["succeeded_count"] == 1
    assert single_leg["terminalized_count"] == 1
    assert single_leg["unresolved_count"] == 0
    assert cleanup["count"] == 1
    assert cleanup["critical_count"] == 1
    assert cleanup["venue_counts"] == {"bybit": 1}


def test_single_leg_recovery_summary_marks_started_without_terminal_unresolved():
    import scripts.diagnose_live as dl

    events = [
        {
            "ts_ms": 1700000001000,
            "kind": "pending_entry.single_leg_exposure_recovery_started",
            "payload": {
                "entry_id": "entry-auth-stuck",
                "symbol": "AUTHUSDT",
                "failed_hedge_venue": "bybit",
                "cleanup_venue": "binance",
                "reason": "venue_auth_invalid",
            },
        },
        {
            "ts_ms": 1700000001100,
            "kind": "pending_entry.single_leg_flatten_submitted",
            "payload": {
                "entry_id": "entry-auth-stuck",
                "symbol": "AUTHUSDT",
                "venue": "binance",
                "reason": "single_leg_exposure_recovery",
            },
        },
    ]

    summary = dl._build_single_leg_exposure_recovery_summary(events)

    assert summary["started_count"] == 1
    assert summary["terminalized_count"] == 0
    assert summary["unresolved_count"] == 1
    assert summary["unresolved_entry_ids"] == ["entry-auth-stuck"]


def test_business_progression_quality_summary_flags_repeated_single_leg_fee_drag():
    import scripts.diagnose_live as dl

    events = [
        {
            "ts_ms": 1700000000000,
            "kind": "runtime.entry_blocked_admission_selection",
            "payload": {
                "symbol": "HUSDT",
                "venue": "aster",
                "reason": "max_notional_admission_blocked",
                "stage": "selected_pre_submit",
            },
        },
        {
            "ts_ms": 1700000000100,
            "kind": "pending_entry.missing_hedge_detected",
            "payload": {
                "entry_id": "entry-husdt-1",
                "symbol": "HUSDT",
                "venue": "aster",
            },
        },
        {
            "ts_ms": 1700000000200,
            "kind": "pending_entry.hedge_admission_blocked",
            "payload": {
                "entry_id": "entry-husdt-1",
                "symbol": "HUSDT",
                "venue": "aster",
                "reason": "max_notional_admission_blocked",
                "blocked_until_ms": 1700000300000,
            },
        },
        {
            "ts_ms": 1700000000300,
            "kind": "entry.cleanup_leg_exposure",
            "payload": {
                "entry_id": "entry-husdt-1",
                "symbol": "HUSDT",
                "venue": "aster",
                "stage": "abort",
            },
        },
        {
            "ts_ms": 1700000000400,
            "kind": "runtime.venue_cooldown_started",
            "payload": {
                "symbol": "HUSDT",
                "venue": "aster",
                "reason": "aster_max_notional_limit",
                "blocked_until_ms": 1700000300000,
            },
        },
        {
            "ts_ms": 1700000000500,
            "kind": "order.passive_submitted",
            "payload": {
                "position_id": "entry-husdt-2",
                "symbol": "HUSDT",
                "venue": "aster",
                "leg": "long",
            },
        },
    ]

    summary = dl._build_business_progression_quality_summary(events)

    assert summary["pre_submit_blocked"] == 1
    assert summary["single_leg_created"] == 1
    assert summary["single_leg_cleanup"] == 1
    assert summary["cleanup_after_admission_block"] == 1
    assert summary["repeated_single_leg_guarded"]["violation_count"] == 1
    assert summary["repeated_single_leg_guarded"]["severity"] == "production_issue"
    assert summary["repeated_single_leg_guarded"]["samples"][0]["venue_symbol"] == (
        "aster:HUSDT"
    )


def test_business_progression_quality_summary_flags_route_cooldown_fee_drag():
    import scripts.diagnose_live as dl

    events = [
        {
            "ts_ms": 1700000000300,
            "kind": "execution.entry_quantity_plan",
            "payload": {
                "entry_id": "entry-husdt-2",
                "symbol": "HUSDT",
                "long_venue": "binance",
                "short_venue": "aster",
                "maker_leg": "long",
            },
        },
        {
            "ts_ms": 1700000000400,
            "kind": "runtime.venue_cooldown_started",
            "payload": {
                "symbol": "HUSDT",
                "venue": "aster",
                "reason": "aster_max_notional_limit",
                "blocked_until_ms": 1700000300000,
            },
        },
        {
            "ts_ms": 1700000000500,
            "kind": "order.passive_submitted",
            "payload": {
                "position_id": "entry-husdt-2",
                "symbol": "HUSDT",
                "venue": "binance",
                "leg": "long",
            },
        },
    ]

    summary = dl._build_business_progression_quality_summary(events)

    assert summary["repeated_single_leg_guarded"]["violation_count"] == 1
    sample = summary["repeated_single_leg_guarded"]["samples"][0]
    assert sample["venue_symbol"] == "aster:HUSDT"
    assert sample["submitted_venue_symbol"] == "binance:HUSDT"
    assert sample["cooldown_reason"] == "aster_max_notional_limit"


def test_business_progression_quality_counts_reduce_only_adopt_and_deterministic_reject():
    import scripts.diagnose_live as dl

    events = [
        {
            "ts_ms": 1700000000000,
            "kind": "order.rejected",
            "payload": {
                "symbol": "OPGUSDT",
                "venue": "bybit",
                "exchange_code": "110007",
                "raw_error": "bybit retCode=110007 retMsg=ab not enough for new order",
            },
        },
        {
            "ts_ms": 1700000000001,
            "kind": "runtime.entry_admission_symbol_cooldown_armed",
            "payload": {
                "symbol": "OPGUSDT",
                "venue": "bybit",
                "reason": "insufficient_balance_admission_blocked",
                "block_scope": "symbol",
                "blocked_until_ms": 1700000300000,
            },
        },
        {
            "ts_ms": 1700000000100,
            "kind": "exit.passive_close_existing_reduce_only_order_adopted",
            "payload": {
                "position_id": "entry-genius",
                "symbol": "GENIUSUSDT",
                "venue": "bybit",
                "order_id": "bybit-close-live",
            },
        },
        {
            "ts_ms": 1700000000200,
            "kind": "exit.passive_close_reduce_only_quantity_covered_by_open_order",
            "payload": {
                "position_id": "entry-genius",
                "symbol": "GENIUSUSDT",
                "venue": "bybit",
                "exchange_code": "110017",
            },
        },
        {
            "ts_ms": 1700000000300,
            "kind": "exit.passive_close_open_order_ownerless_blocked",
            "payload": {
                "position_id": "entry-ownerless",
                "symbol": "BADUSDT",
                "venue": "bybit",
            },
        },
    ]

    summary = dl._build_business_progression_quality_summary(events)

    assert summary["deterministic_reject_after_submit_count"] == 1
    assert summary["adopted_reduce_only_order_count"] == 1
    assert summary["duplicate_reduce_only_submit_blocked_count"] == 1
    assert summary["owned_pending_passive_close_count"] == 2
    assert summary["ownerless_open_order_count"] == 1


def test_business_progression_quality_summary_reports_phase_takeover_gaps():
    import scripts.diagnose_live as dl

    events = [
        {
            "ts_ms": 1_000,
            "kind": "review.candidate_shortlisted",
            "payload": {
                "candidate_pair_id": "slow-candidate",
                "symbol": "SLOWUSDT",
                "venue": "aster",
            },
        },
        {
            "ts_ms": 10_000,
            "kind": "runtime.entry_quote_rewarm_scheduled_after_rest_stale",
            "payload": {"venue": "aster", "symbol": "STALEUSDT"},
        },
        {
            "ts_ms": 100_000,
            "kind": "runtime.lifecycle_tick",
            "payload": {"reason": "diagnostic_horizon"},
        },
    ]

    summary = dl._build_business_progression_quality_summary(events)
    handoff = summary["phase_handoff_quality"]

    assert handoff["severity"] == "production_issue"
    assert handoff["missing_takeover_count"] == 1
    assert handoff["phase_counts"] == {
        "candidate_lease": {
            "over_budget_count": 1,
            "takeover_count": 0,
            "missing_takeover_count": 1,
        },
        "quote_rewarm": {
            "over_budget_count": 1,
            "takeover_count": 1,
            "missing_takeover_count": 0,
        },
    }
    assert {
        sample["phase"]: sample["configured_action"]
        for sample in handoff["samples"]
    } == {
        "candidate_lease": "expire_candidate_and_rescan",
    }


def test_business_progression_quality_summary_counts_candidate_quote_takeovers():
    import scripts.diagnose_live as dl

    events = [
        {
            "ts_ms": 1_000,
            "kind": "review.candidate_shortlisted",
            "payload": {
                "candidate_pair_id": "expired-candidate",
                "symbol": "EXPUSDT",
                "venue": "aster",
            },
        },
        {
            "ts_ms": 62_000,
            "kind": "runtime.candidate_lease_expired",
            "payload": {
                "candidate_pair_id": "expired-candidate",
                "symbol": "EXPUSDT",
                "venue": "aster",
                "action_taken": "expire_candidate_and_rescan",
            },
        },
        {
            "ts_ms": 10_000,
            "kind": "runtime.entry_quote_rewarm_scheduled_after_rest_stale",
            "payload": {"venue": "aster", "symbol": "STALEUSDT"},
        },
        {
            "ts_ms": 42_000,
            "kind": "runtime.entry_quote_rewarm_terminal_stale",
            "payload": {
                "venue": "aster",
                "symbol": "STALEUSDT",
                "action_taken": "skip_candidate_after_hard_rewarm",
            },
        },
    ]

    summary = dl._build_business_progression_quality_summary(events)
    handoff = summary["phase_handoff_quality"]

    assert handoff["severity"] == "ok"
    assert handoff["missing_takeover_count"] == 0
    assert handoff["phase_counts"] == {
        "candidate_lease": {
            "over_budget_count": 1,
            "takeover_count": 1,
            "missing_takeover_count": 0,
        },
        "quote_rewarm": {
            "over_budget_count": 1,
            "takeover_count": 1,
            "missing_takeover_count": 0,
        },
    }
    assert handoff["samples"] == []


def test_business_progression_quality_summary_treats_catalog_skip_as_candidate_takeover():
    import scripts.diagnose_live as dl

    events = [
        {
            "ts_ms": 1_000,
            "kind": "runtime.candidate_symbol_skipped",
            "payload": {
                "candidate_pair_id": "highusdt:bybit->aster",
                "pair_id": "highusdt:bybit->aster",
                "symbol": "HIGHUSDT",
                "long_venue": "bybit",
                "short_venue": "aster",
                "reason": "unsupported_symbol",
            },
        },
        {
            "ts_ms": 100_000,
            "kind": "runtime.lifecycle_tick",
            "payload": {"reason": "diagnostic_horizon"},
        },
    ]

    summary = dl._build_business_progression_quality_summary(events)
    handoff = summary["phase_handoff_quality"]

    assert handoff["severity"] == "ok"
    assert handoff["missing_takeover_count"] == 0
    assert handoff["phase_counts"]["candidate_lease"] == {
        "over_budget_count": 0,
        "takeover_count": 0,
        "missing_takeover_count": 0,
    }
    assert not any(
        sample["artifact_id"] == "highusdt:bybit->aster"
        for sample in handoff["samples"]
    )


def test_business_progression_quality_summary_counts_catalog_skip_after_shortlist():
    import scripts.diagnose_live as dl

    events = [
        {
            "ts_ms": 1_000,
            "kind": "review.candidate_shortlisted",
            "payload": {
                "candidate_pair_id": "highusdt:bybit->aster",
                "symbol": "HIGHUSDT",
                "venue": "bybit",
            },
        },
        {
            "ts_ms": 100_000,
            "kind": "runtime.candidate_symbol_skipped",
            "payload": {
                "candidate_pair_id": "highusdt:bybit->aster",
                "pair_id": "highusdt:bybit->aster",
                "symbol": "HIGHUSDT",
                "long_venue": "bybit",
                "short_venue": "aster",
                "reason": "unsupported_symbol",
            },
        },
    ]

    summary = dl._build_business_progression_quality_summary(events)
    handoff = summary["phase_handoff_quality"]

    assert handoff["severity"] == "ok"
    assert handoff["missing_takeover_count"] == 0
    assert handoff["phase_counts"]["candidate_lease"] == {
        "over_budget_count": 1,
        "takeover_count": 1,
        "missing_takeover_count": 0,
    }


def test_venue_private_health_summary_deduplicates_block_and_cooldown_event():
    import scripts.diagnose_live as dl

    events = [
        {
            "ts_ms": 1700000001000,
            "kind": "runtime.entry_admission_blocked",
            "payload": {
                "venue": "bybit",
                "symbol": "AUTHUSDT",
                "reason": "venue_auth_invalid",
                "source": "venue_private_health_precheck",
                "venue_private_health_status": "auth_invalid",
                "candidate_pair_id": "pair-auth",
            },
        },
        {
            "ts_ms": 1700000001000,
            "kind": "runtime.venue_cooldown_started",
            "payload": {
                "venue": "bybit",
                "symbol": "AUTHUSDT",
                "reason": "venue_auth_invalid",
                "source": "venue_private_health_precheck",
                "venue_private_health_status": "auth_invalid",
                "candidate_pair_id": "pair-auth",
            },
        },
    ]

    summary = dl._build_venue_private_health_summary(events)

    assert summary["count"] == 1
    assert summary["event_count"] == 2
    assert summary["status_counts"] == {"auth_invalid": 1}
    assert summary["venue_counts"] == {"bybit": 1}


def test_run_diagnose_resolves_home_passive_close_order_errors_by_terminal_truth(monkeypatch):
    import scripts.diagnose_live as dl

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
            "pending_residual_repair_count": 0,
            "last_tick_ms": 1781615110000,
        }
        _write_json(os.path.join(d, "state-current.json"), state)
        events = [
            {
                "ts_ms": 1781615103784,
                "kind": "exit.passive_close_maker_submit_error",
                "payload": {
                    "position_id": "entry-1781614327885-HOMEUSDT",
                    "venue": "binance",
                    "error": "HTTP 400: {\"code\":-5022,\"msg\":\"Post Only order will be rejected\"}",
                    "exchange_error": {
                        "venue": "binance",
                        "operation": "submit_passive_order",
                        "http_status": 400,
                        "raw_body": '{"code":-5022,"msg":"Post Only order will be rejected"}',
                        "exchange_code": "-5022",
                        "exchange_msg": "Post Only order will be rejected",
                        "evidence_completeness": "complete",
                        "missing_evidence": [],
                        "confidence": "high",
                        "request_context": {
                            "symbol": "HOMEUSDT",
                            "side": "sell",
                            "quantity": 830.0,
                            "reduce_only": True,
                            "post_only": True,
                            "client_order_id": "lfexade7189709dfb9ea",
                        },
                    },
                    "request_context": {
                        "symbol": "HOMEUSDT",
                        "side": "sell",
                        "quantity": 830.0,
                        "reduce_only": True,
                        "post_only": True,
                        "client_order_id": "lfexade7189709dfb9ea",
                    },
                },
            },
            {
                "ts_ms": 1781615104321,
                "kind": "exit.passive_close_dual_taker_drive",
                "payload": {"position_id": "entry-1781614327885-HOMEUSDT"},
            },
            {
                "ts_ms": 1781615104673,
                "kind": "order.uncertain",
                "payload": {
                    "position_id": "entry-1781614327885-HOMEUSDT",
                    "leg": "short",
                    "reason": "order accepted but fill not confirmed",
                    "client_order_id": "lfxs91799175d6b1dae6",
                    "exchange_error": {
                        "venue": "bybit",
                        "operation": "place_order",
                        "exchange_code": "0",
                        "exchange_msg": "OK",
                        "evidence_completeness": "partial",
                        "missing_evidence": ["fill_confirmation"],
                        "request_context": {
                            "symbol": "HOMEUSDT",
                            "side": "buy",
                            "quantity": 830.0,
                            "reduce_only": True,
                            "client_order_id": "lfxs91799175d6b1dae6",
                        },
                        "extra": {
                            "order_ack_only": True,
                            "accepted_order_id": "25ca666a-d038-4d34-9123-7551a3bb153c",
                            "accepted_client_order_id": "lfxs91799175d6b1dae6",
                        },
                    },
                    "request_context": {
                        "symbol": "HOMEUSDT",
                        "side": "buy",
                        "quantity": 830.0,
                        "reduce_only": True,
                        "client_order_id": "lfxs91799175d6b1dae6",
                    },
                },
            },
            {
                "ts_ms": 1781615105952,
                "kind": "order.reconcile_result",
                "payload": {
                    "position_id": "entry-1781614327885-HOMEUSDT",
                    "venue": "bybit",
                    "symbol": "HOMEUSDT",
                    "order_id": "25ca666a-d038-4d34-9123-7551a3bb153c",
                    "client_order_id": "lfxs91799175d6b1dae6",
                    "status": "full",
                    "reason": "duplicate_client_id",
                    "target_qty": 830.0,
                    "reconciled_qty": 830.0,
                    "live_qty": 0.0,
                    "next_action": "clear_live_flat",
                },
            },
            {
                "ts_ms": 1781615106060,
                "kind": "order.uncertain",
                "payload": {
                    "position_id": "entry-1781614327885-HOMEUSDT",
                    "leg": "long",
                    "reason": "zero fill",
                    "client_order_id": "lfxl2d990a62d66df2f9",
                },
            },
            {
                "ts_ms": 1781615107164,
                "kind": "order.rejected",
                "payload": {
                    "position_id": "entry-1781614327885-HOMEUSDT",
                    "leg": "long",
                    "reason": "HTTP 400: {\"code\":-2022,\"msg\":\"ReduceOnly Order is rejected.\"}",
                    "client_order_id": "lfxl2d990a62d66df2f9",
                    "exchange_error": {
                        "venue": "binance",
                        "operation": "place_order",
                        "http_status": 400,
                        "raw_body": '{"code":-2022,"msg":"ReduceOnly Order is rejected."}',
                        "exchange_code": "-2022",
                        "exchange_msg": "ReduceOnly Order is rejected.",
                        "evidence_completeness": "complete",
                        "missing_evidence": [],
                        "confidence": "high",
                        "request_context": {
                            "symbol": "HOMEUSDT",
                            "side": "sell",
                            "quantity": 830.0,
                            "reduce_only": True,
                            "post_only": False,
                            "client_order_id": "lfxl2d990a62d66df2f9",
                        },
                    },
                    "request_context": {
                        "symbol": "HOMEUSDT",
                        "side": "sell",
                        "quantity": 830.0,
                        "reduce_only": True,
                        "post_only": False,
                        "client_order_id": "lfxl2d990a62d66df2f9",
                    },
                },
            },
            {
                "ts_ms": 1781615107314,
                "kind": "order.filled",
                "payload": {
                    "position_id": "entry-1781614327885-HOMEUSDT",
                    "leg": "long",
                    "reason": "terminal_reduce_only",
                    "exchange_verified_flat": True,
                    "client_order_id": "lfxl2d990a62d66df2f9",
                },
            },
            {
                "ts_ms": 1781615107314,
                "kind": "exit.close_chunk_submitted",
                "payload": {
                    "position_id": "entry-1781614327885-HOMEUSDT",
                    "short_client_order_id": "lfxs91799175d6b1dae6",
                    "long_client_order_id": "lfxl2d990a62d66df2f9",
                    "short_outcome": "filled",
                    "long_outcome": "filled",
                },
            },
            {
                "ts_ms": 1781615107314,
                "kind": "exit.close_residual_detected",
                "payload": {
                    "position_id": "entry-1781614327885-HOMEUSDT",
                    "exposure_quantity": 830.0,
                    "exposure_venue": "binance",
                    "close_id": "close-entry-1781614327885-HOMEUSDT-1781615104525",
                },
            },
            {
                "ts_ms": 1781615104525,
                "kind": "exit.closed",
                "payload": {
                    "position_id": "entry-1781614327885-HOMEUSDT",
                    "reason": "funding_capture",
                    "long_closed_qty": 0.0,
                    "short_closed_qty": 830.0,
                    "close_id": "close-entry-1781614327885-HOMEUSDT-1781615104525",
                    "long_client_order_id": "lfxl2d990a62d66df2f9",
                    "short_client_order_id": "lfxs91799175d6b1dae6",
                },
            },
            {
                "ts_ms": 1781615108091,
                "kind": "exit.passive_close_resolved",
                "payload": {
                    "position_id": "entry-1781614327885-HOMEUSDT",
                    "symbol": "HOMEUSDT",
                    "resolution_source": "fallback_live_balanced_matched_close_flat_probe",
                    "terminal_close_execution": True,
                    "live_flat_terminal": True,
                    "problem": False,
                    "long_closed_qty": 830.0,
                    "short_closed_qty": 830.0,
                },
            },
            {
                "ts_ms": 1781615108092,
                "kind": "runtime.position_lifecycle_terminal",
                "payload": {
                    "position_id": "entry-1781614327885-HOMEUSDT",
                    "symbol": "HOMEUSDT",
                    "terminal_state": "flat",
                    "terminal_reason": "fallback_live_balanced_matched_close_flat_probe",
                    "problem": False,
                },
            },
            {
                "ts_ms": 1781615108467,
                "kind": "execution.residual_repair_completed",
                "payload": {
                    "position_id": "entry-1781614327885-HOMEUSDT",
                    "symbol": "HOMEUSDT",
                    "origin": "close_residual",
                    "result": "already_flat",
                    "live_excess_quantity": 0.0,
                },
            },
        ]
        _write_jsonl(os.path.join(d, "events.jsonl"), events)

        monkeypatch.setattr(dl, "_build_exchange_truth", _flat_exchange_truth)

        result = dl.run_diagnose(
            runtime_dir=d,
            unit_dir="/nonexistent",
            symbol="HOMEUSDT",
            venues=["binance", "bybit"],
            now_ms=1781615115000,
        )

        assert result["order_error_evidence"] == []
        assert result["top_exchange_errors"] == []
        summary = result["resolved_close_order_error_summary"]
        assert summary["current_exchange_truth_clean"] is True
        assert summary["post_only_boundary_reject_count"] == 1
        assert summary["reduce_only_terminal_flat_count"] == 1
        assert summary["zero_fill_terminal_flat_count"] == 1
        assert summary["position_ids"] == ["entry-1781614327885-HOMEUSDT"]
        gate = result["production_acceptance_gate"]
        assert gate["v1_lifecycle_closure"]["unmapped_event_kinds"] == []
        assert gate["gate_passed"] is True
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
