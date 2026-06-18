import json
import os
import subprocess
import sys
import time
from pathlib import Path

from lightfee.ops.production_health import (
    analyze_current_state,
    analyze_resolver_config,
    analyze_sidecar_snapshot,
    analyze_systemd_unit,
    summarize_reports,
)
from scripts.diagnose_live import _build_state_consistency
from scripts import verify_production_services as vps


def test_sidecar_unit_rejects_missing_config():
    text = """
[Service]
WorkingDirectory=/root/projects/LightFee
ExecStart=/root/projects/LightFee/target/release/opportunity_input_sidecar
"""
    report = analyze_systemd_unit("lightfee-sidecar.service", text)
    assert not report.ok
    assert "missing_explicit_config" in report.fingerprints


def test_sidecar_unit_rejects_example_config():
    text = """
[Service]
EnvironmentFile=/etc/lightfee/lightfee.env
ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.sidecar --config config/example.toml
"""
    report = analyze_systemd_unit("lightfee-sidecar.service", text)
    assert not report.ok
    assert "example_config_in_production" in report.fingerprints


def test_sidecar_unit_accepts_current_rust_v1_live_contract():
    text = """
[Service]
WorkingDirectory=/root/projects/LightFee
EnvironmentFile=/etc/lightfee/lightfee.env
ExecStart=/root/projects/LightFee/target/release/opportunity_input_sidecar --config /root/projects/LightFee/config/live.auto.toml
Restart=always
"""
    report = analyze_systemd_unit("lightfee-sidecar.service", text)
    assert report.ok


def test_snapshot_rejects_fixture_four_venue_shape():
    snapshot = {
        "market_observed_at_ms": 1710000075000,
        "quotes": {
            "binance:BTCUSDT": {"venue": "binance", "symbol": "BTCUSDT", "bid": 100.0, "ask": 100.0},
            "okx:BTCUSDT": {"venue": "okx", "symbol": "BTCUSDT", "bid": 100.0, "ask": 100.0},
            "bybit:BTCUSDT": {"venue": "bybit", "symbol": "BTCUSDT", "bid": 100.0, "ask": 100.0},
            "hyperliquid:BTCUSDT": {"venue": "hyperliquid", "symbol": "BTCUSDT", "bid": 100.0, "ask": 100.0},
        },
        "degraded_venues": [],
    }
    report = analyze_sidecar_snapshot(snapshot, now_ms=1778787000000, max_age_ms=10_000)
    assert not report.ok
    assert "fixture_timestamp" in report.fingerprints
    assert "quote_venue_count_lt_7" in report.fingerprints


def test_snapshot_accepts_fresh_seven_venue_shape():
    venues = ["aster", "binance", "bitget", "bybit", "gate", "hyperliquid", "okx"]
    snapshot = {
        "market_observed_at_ms": 1778786998000,
        "quotes": {
            f"{venue}:BTCUSDT": {"venue": venue, "symbol": "BTCUSDT", "bid": 65000.0, "ask": 65001.0}
            for venue in venues
        },
        "degraded_venues": [],
    }
    report = analyze_sidecar_snapshot(snapshot, now_ms=1778787000000, max_age_ms=10_000)
    assert report.ok


def test_current_state_flags_stale_fail_closed_clean_state():
    state = {
        "lifecycle": "running",
        "risk_mode": "fail_closed",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "recovery_blocked_reason": None,
    }
    report = analyze_current_state(state, now_ms=1778787000000, max_tick_age_ms=10_000)
    assert not report.ok
    assert "stale_fail_closed_clean_state" in report.fingerprints


def test_current_state_preserves_recent_auto_fail_closed_recovery_as_detail_only():
    state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "last_scan": {"candidate_count": 10, "tradeable_count": 2},
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
        },
        "auto_fail_closed_summary": {
            "recent_incident": True,
            "recovered_count": 1,
            "cleanup_failed_count": 0,
            "latest_event": {
                "kind": "runtime.auto_fail_closed_recovered",
                "final_status": "recovered",
                "symbols": ["LINKUSDT"],
                "venues": ["bybit"],
            },
        },
    }

    report = analyze_current_state(state, now_ms=1778787000000, max_tick_age_ms=10_000)

    assert report.ok
    assert report.fingerprints == []
    assert report.details["auto_fail_closed_summary"]["recent_incident"] is True
    assert report.details["auto_fail_closed_summary"]["latest_event"]["final_status"] == "recovered"


def test_current_state_preserves_recent_stale_risk_alignment_as_detail_only():
    state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "last_scan": {"candidate_count": 10, "tradeable_count": 2},
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
        },
        "stale_risk_state_alignment_summary": {
            "recent_incident": True,
            "aligned_count": 1,
            "blocked_count": 0,
            "latest_event": {
                "kind": "runtime.stale_risk_state_aligned",
                "symbols": ["HOMEUSDT"],
                "venues": ["aster"],
                "new_lifecycle": "running",
                "new_risk_mode": "running",
            },
        },
    }

    report = analyze_current_state(state, now_ms=1778787000000, max_tick_age_ms=10_000)

    assert report.ok
    assert report.fingerprints == []
    summary = report.details["stale_risk_state_alignment_summary"]
    assert summary["recent_incident"] is True
    assert summary["latest_event"]["new_lifecycle"] == "running"


def test_current_state_clean_local_exchange_nonzero_is_critical():
    state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "last_scan": {"candidate_count": 10, "tradeable_count": 2},
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": True,
            "positions": {
                "bybit": {
                    "BIOUSDT": {
                        "venue": "bybit",
                        "symbol": "BIOUSDT",
                        "side": "buy",
                        "quantity": 1444.0,
                        "entry_price": 0.03321,
                    }
                }
            },
        },
    }

    report = analyze_current_state(state, now_ms=1778787000000, max_tick_age_ms=10_000)

    assert not report.ok
    assert report.severity == "critical"
    assert "exchange_truth_mismatch" in report.fingerprints
    assert "nonzero_live_position" in report.fingerprints
    assert report.details["exchange_truth_mismatches"][0]["symbol"] == "BIOUSDT"


def test_current_state_pending_entry_live_conflict_lists_conflict_reasons():
    state = {
        "lifecycle": "risk_only",
        "risk_mode": "fail_closed",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 1,
        "pending_close_count": 0,
        "pending_entries": [
            {
                "pending_id": "entry-home",
                "symbol": "HOMEUSDT",
                "long_venue": "okx",
                "short_venue": "bybit",
                "maker_leg": "long",
                "maker_leg_filled": 1600.0,
                "hedge_leg_filled": 1600.0,
            }
        ],
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": True,
            "positions": {
                "okx": {"HOMEUSDT": {"venue": "okx", "symbol": "HOMEUSDT", "quantity": 0.0}},
                "bybit": {
                    "HOMEUSDT": {
                        "venue": "bybit",
                        "symbol": "HOMEUSDT",
                        "side": "sell",
                        "quantity": 1600.0,
                    }
                },
            },
            "open_orders": {"okx": {"HOMEUSDT": []}, "bybit": {"HOMEUSDT": []}},
        },
    }

    report = analyze_current_state(state, now_ms=1778787000000, max_tick_age_ms=10_000)

    detail = report.details["pending_entry_live_conflicts"]["details"][0]
    assert "okx fill evidence conflicts with okx live flat" in detail["conflict_reasons"]
    assert "live position owned by pending conflict" in detail["conflict_reasons"]


def test_current_state_clean_local_exchange_open_order_is_critical():
    state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "last_scan": {"candidate_count": 10, "tradeable_count": 2},
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": True,
            "positions": {"bybit": {}},
            "open_orders": {
                "bybit": {
                    "*": [
                        {
                            "venue": "bybit",
                            "symbol": "TRXUSDT",
                            "side": "buy",
                            "quantity": 72.0,
                            "reduce_only": False,
                            "order_id": "live-maker",
                        }
                    ]
                }
            },
        },
    }

    report = analyze_current_state(state, now_ms=1778787000000, max_tick_age_ms=10_000)

    assert not report.ok
    assert report.severity == "critical"
    assert "exchange_truth_mismatch" in report.fingerprints
    assert "live_open_order" in report.fingerprints
    mismatch = report.details["exchange_truth_mismatches"][0]
    assert mismatch["check"] == "unexpected_live_open_order"
    assert mismatch["symbol"] == "TRXUSDT"


def test_current_state_local_open_exchange_leg_quantity_mismatch_is_critical():
    state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 1,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "open_positions": [{
            "position_id": "pos-beat",
            "symbol": "BEATUSDT",
            "long_venue": "aster",
            "short_venue": "bybit",
            "quantity": 24.0,
        }],
        "last_scan": {"candidate_count": 10, "tradeable_count": 2},
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": True,
            "positions": {
                "aster": {
                    "BEATUSDT": {
                        "venue": "aster",
                        "symbol": "BEATUSDT",
                        "side": "buy",
                        "quantity": 0.0,
                    }
                },
                "bybit": {
                    "BEATUSDT": {
                        "venue": "bybit",
                        "symbol": "BEATUSDT",
                        "side": "sell",
                        "quantity": 9.0,
                    },
                    "BIOUSDT": {
                        "venue": "bybit",
                        "symbol": "BIOUSDT",
                        "side": "buy",
                        "quantity": 1444.0,
                    },
                },
            },
        },
    }

    report = analyze_current_state(state, now_ms=1778787000000, max_tick_age_ms=10_000)

    assert not report.ok
    assert report.severity == "critical"
    assert "exchange_truth_mismatch" in report.fingerprints
    assert "local_exchange_position_mismatch" in report.fingerprints
    checks = {m["check"] for m in report.details["exchange_truth_mismatches"]}
    assert "local_live_leg_missing_or_quantity_mismatch" in checks
    assert "unexpected_live_position" in checks


def test_diagnose_state_consistency_names_exchange_nonzero_local_flat():
    local_state = {
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "positions": [],
    }
    exchange_truth = {
        "available": True,
        "confidence": "high",
        "has_nonzero_position": True,
        "positions": {
            "bybit": {
                "BIOUSDT": {
                    "venue": "bybit",
                    "symbol": "BIOUSDT",
                    "side": "buy",
                    "quantity": 1444.0,
                    "entry_price": 0.03321,
                }
            }
        },
        "fetch_status": {"bybit": {"status": "ok", "positions_failed": []}},
    }

    consistency = _build_state_consistency(local_state, exchange_truth)

    assert consistency["state_mismatch"] is True
    assert consistency["state_verdict"] == "exchange_truth_mismatch"
    assert "exchange_truth_mismatch" in consistency["fingerprints"]
    assert "nonzero_live_position" in consistency["fingerprints"]
    detail = consistency["details"][0]
    assert detail["check"] == "nonzero_live_position"
    assert detail["live_positions"][0]["symbol"] == "BIOUSDT"


def test_diagnose_state_consistency_flags_local_open_live_leg_quantity_mismatch():
    local_state = {
        "open_position_count": 1,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "positions": [{
            "position_id": "pos-beat",
            "symbol": "BEATUSDT",
            "long_venue": "aster",
            "short_venue": "bybit",
            "quantity": 24.0,
        }],
    }
    exchange_truth = {
        "available": True,
        "confidence": "high",
        "has_nonzero_position": True,
        "positions": {
            "aster": {
                "BEATUSDT": {
                    "venue": "aster",
                    "symbol": "BEATUSDT",
                    "side": "buy",
                    "quantity": 0.0,
                }
            },
            "bybit": {
                "BEATUSDT": {
                    "venue": "bybit",
                    "symbol": "BEATUSDT",
                    "side": "sell",
                    "quantity": 9.0,
                },
                "BIOUSDT": {
                    "venue": "bybit",
                    "symbol": "BIOUSDT",
                    "side": "buy",
                    "quantity": 1444.0,
                },
            },
        },
        "fetch_status": {
            "aster": {"status": "ok", "positions_failed": []},
            "bybit": {"status": "ok", "positions_failed": []},
        },
    }

    consistency = _build_state_consistency(local_state, exchange_truth)

    assert consistency["state_mismatch"] is True
    assert consistency["state_verdict"] == "exchange_truth_mismatch"
    assert "exchange_truth_mismatch" in consistency["fingerprints"]
    assert "local_exchange_position_mismatch" in consistency["fingerprints"]
    checks = {d["check"] for d in consistency["details"]}
    assert "local_live_leg_missing_or_quantity_mismatch" in checks
    assert "unexpected_live_position" in checks


def test_state_consistency_accepts_exchange_side_enum_labels_for_local_open_legs():
    local_state = {
        "open_position_count": 1,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "positions": [{
            "position_id": "pos-home",
            "symbol": "HOMEUSDT",
            "long_venue": "binance",
            "short_venue": "bybit",
            "quantity": 12.0,
        }],
    }
    exchange_truth = {
        "available": True,
        "confidence": "high",
        "has_nonzero_position": True,
        "positions": {
            "binance": {
                "HOMEUSDT": {
                    "venue": "binance",
                    "symbol": "HOMEUSDT",
                    "side": "Side.BUY",
                    "quantity": 12.0,
                },
            },
            "bybit": {
                "HOMEUSDT": {
                    "venue": "bybit",
                    "symbol": "HOMEUSDT",
                    "side": "Side.SELL",
                    "quantity": 12.0,
                },
            },
        },
        "fetch_status": {
            "binance": {"status": "ok", "positions_failed": []},
            "bybit": {"status": "ok", "positions_failed": []},
        },
    }

    consistency = _build_state_consistency(local_state, exchange_truth)
    report = analyze_current_state(
        {
            **local_state,
            "exchange_truth": exchange_truth,
            "last_tick_ms": 1781531700000,
        },
        now_ms=1781531700100,
        max_tick_age_ms=1_000,
    )

    assert "local_exchange_position_mismatch" not in consistency["fingerprints"]
    assert "local_exchange_position_mismatch" not in report.fingerprints


def test_resolver_requires_okx_capable_priority():
    text = "nameserver 42.116.255.180\nnameserver 8.8.8.8\nnameserver 1.1.1.1\n"
    report = analyze_resolver_config(text)
    assert not report.ok
    assert "unverified_resolver_first" in report.fingerprints


def test_summary_is_failed_when_any_report_critical():
    bad = analyze_systemd_unit("lightfee-sidecar.service", "[Service]\nExecStart=lightfee-sidecar\n")
    summary = summarize_reports([bad])
    assert not summary.ok
    assert summary.critical_count == 1


def test_verify_production_services_cli_json_success(tmp_path):
    unit_dir = tmp_path / "systemd"
    unit_dir.mkdir()
    (unit_dir / "lightfee-sidecar.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/root/projects/LightFee/target/release/opportunity_input_sidecar --config /root/projects/LightFee/config/live.auto.toml\n"
    )
    (unit_dir / "lightfee-live.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.live --config /opt/lightfee-v2/config/live.toml\n"
    )
    snapshot = tmp_path / "snapshot.json"
    venues = ["aster", "binance", "bitget", "bybit", "gate", "hyperliquid", "okx"]
    snapshot.write_text(json.dumps({
        "market_observed_at_ms": 1778786998000,
        "quotes": {f"{v}:BTCUSDT": {"venue": v, "symbol": "BTCUSDT", "bid": 65000, "ask": 65001} for v in venues},
        "degraded_venues": [],
    }))
    current = tmp_path / "current.json"
    current.write_text(json.dumps({
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "last_scan": {"candidate_count": 10, "tradeable_count": 2},
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        },
    }))
    resolv = tmp_path / "resolv.conf"
    resolv.write_text("nameserver 1.1.1.1\nnameserver 8.8.8.8\n")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/verify_production_services.py",
            "--unit-dir", str(unit_dir),
            "--snapshot", str(snapshot),
            "--current-state", str(current),
            "--resolv-conf", str(resolv),
            "--now-ms", "1778787000000",
            "--json",
        ],
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr + result.stdout
    payload = json.loads(result.stdout)
    assert payload["ok"] is True


def test_verify_production_services_cli_default_allows_production_scan_gap(tmp_path):
    unit_dir = tmp_path / "systemd"
    unit_dir.mkdir()
    (unit_dir / "lightfee-sidecar.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/lightfee-sidecar --config /opt/lightfee-v2/config/live.toml\n"
    )
    (unit_dir / "lightfee-live.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.live --config /opt/lightfee-v2/config/live.toml\n"
    )
    snapshot = tmp_path / "snapshot.json"
    venues = ["aster", "binance", "bitget", "bybit", "gate", "hyperliquid", "okx"]
    snapshot.write_text(json.dumps({
        "market_observed_at_ms": 1778786955000,
        "quotes": {f"{v}:BTCUSDT": {"venue": v, "symbol": "BTCUSDT", "bid": 65000, "ask": 65001} for v in venues},
        "degraded_venues": [],
    }))
    current = tmp_path / "current.json"
    current.write_text(json.dumps({
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "last_scan": {"candidate_count": 10, "tradeable_count": 2},
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        },
    }))
    resolv = tmp_path / "resolv.conf"
    resolv.write_text("nameserver 1.1.1.1\nnameserver 8.8.8.8\n")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/verify_production_services.py",
            "--unit-dir", str(unit_dir),
            "--snapshot", str(snapshot),
            "--current-state", str(current),
            "--resolv-conf", str(resolv),
            "--now-ms", "1778787000000",
            "--json",
        ],
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr + result.stdout
    payload = json.loads(result.stdout)
    assert payload["ok"] is True


def test_verify_production_services_cli_requires_exchange_truth_evidence(tmp_path):
    unit_dir = tmp_path / "systemd"
    unit_dir.mkdir()
    (unit_dir / "lightfee-sidecar.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/lightfee-sidecar --config /opt/lightfee-v2/config/live.toml\n"
    )
    (unit_dir / "lightfee-live.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.live --config /opt/lightfee-v2/config/live.toml\n"
    )
    snapshot = tmp_path / "snapshot.json"
    venues = ["aster", "binance", "bitget", "bybit", "gate", "hyperliquid", "okx"]
    snapshot.write_text(json.dumps({
        "market_observed_at_ms": 1778786998000,
        "quotes": {f"{v}:BTCUSDT": {"venue": v, "symbol": "BTCUSDT", "bid": 65000, "ask": 65001} for v in venues},
        "degraded_venues": [],
    }))
    current = tmp_path / "current.json"
    current.write_text(json.dumps({
        "schema": "lightfee.current_state.v1",
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "last_scan": {"candidate_count": 10, "tradeable_count": 2},
    }))
    resolv = tmp_path / "resolv.conf"
    resolv.write_text("nameserver 1.1.1.1\nnameserver 8.8.8.8\n")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/verify_production_services.py",
            "--unit-dir", str(unit_dir),
            "--snapshot", str(snapshot),
            "--current-state", str(current),
            "--resolv-conf", str(resolv),
            "--now-ms", "1778787000000",
            "--json",
        ],
        text=True,
        capture_output=True,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    current_report = [
        report for report in payload["reports"]
        if report["name"] == "current_state"
    ][0]
    assert "exchange_truth_missing" in current_report["fingerprints"]
    assert current_report["details"]["exchange_truth_required"] is True
    assert (
        current_report["details"]["recovery_decision"]["kind"]
        == "RUNNING_WITH_EVIDENCE_GAP"
    )


def test_verify_production_services_attaches_exchange_truth_from_systemd_env_file(
    tmp_path, monkeypatch,
):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    current_state = runtime_dir / "live-state-current.json"
    env_file = tmp_path / "lightfee.env"
    env_file.write_text(
        "LIGHTFEE_BYBIT_API_KEY=key-from-file\n"
        "LIGHTFEE_BYBIT_API_SECRET=secret-from-file\n"
    )
    state = {
        "schema": "lightfee.current_state.v1",
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
    }
    seen: dict[str, object] = {}

    def fake_exchange_truth(runtime_dir_arg, symbols, venues=None):
        seen["runtime_dir"] = runtime_dir_arg
        seen["symbols"] = list(symbols)
        seen["venues"] = venues
        seen["api_key"] = os.environ.get("LIGHTFEE_BYBIT_API_KEY")
        return {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        }

    monkeypatch.delenv("LIGHTFEE_BYBIT_API_KEY", raising=False)
    enriched = vps._attach_exchange_truth_if_missing(
        state,
        current_state_path=current_state,
        unit_texts={
            "lightfee-live.service": (
                "[Service]\n"
                f"EnvironmentFile={env_file}\n"
            ),
        },
        exchange_truth_builder=fake_exchange_truth,
    )

    assert enriched["exchange_truth"]["available"] is True
    assert enriched["exchange_truth_source"] == "verify_production_services_probe"
    assert seen["runtime_dir"] == str(runtime_dir)
    assert seen["symbols"] == []
    assert seen["venues"] is None
    assert seen["api_key"] == "key-from-file"


def test_verify_production_services_attaches_auto_fail_closed_summary(tmp_path):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    current_state = runtime_dir / "live-state-current.json"
    state = {"generated_at_ms": 1778787000000}
    current_state.write_text(json.dumps(state))
    (runtime_dir / "events.jsonl").write_text(
        json.dumps(
            {
                "ts_ms": 1778786998000,
                "kind": "runtime.auto_fail_closed_recovered",
                "payload": {
                    "source": "auto_pending_entry_abort",
                    "reason": "deadline breach",
                    "symbols": ["LINKUSDT"],
                    "venues": ["bybit"],
                    "new_risk_mode": "running",
                    "residual_blockers": [],
                },
            }
        )
        + "\n"
    )

    enriched = vps._attach_auto_fail_closed_summary_if_missing(
        state,
        current_state_path=current_state,
    )

    summary = enriched["auto_fail_closed_summary"]
    assert summary["recent_incident"] is True
    assert summary["recovered_count"] == 1
    assert summary["latest_event"]["symbols"] == ["LINKUSDT"]


def test_verify_production_services_attaches_stale_risk_state_alignment_summary(tmp_path):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    current_state = runtime_dir / "live-state-current.json"
    state = {"generated_at_ms": 1778787000000}
    current_state.write_text(json.dumps(state))
    (runtime_dir / "events.jsonl").write_text(
        json.dumps(
            {
                "ts_ms": 1778786998000,
                "kind": "runtime.stale_risk_state_aligned",
                "payload": {
                    "source": "repair_stale_risk_state",
                    "symbols": ["HOMEUSDT"],
                    "venues": ["aster"],
                    "previous_lifecycle": "risk_only",
                    "previous_risk_mode": "running",
                    "new_lifecycle": "running",
                    "new_risk_mode": "running",
                    "terminalized_records": ["rec-1"],
                },
            }
        )
        + "\n"
    )

    enriched = vps._attach_stale_risk_state_alignment_summary_if_missing(
        state,
        current_state_path=current_state,
    )

    summary = enriched["stale_risk_state_alignment_summary"]
    assert summary["recent_incident"] is True
    assert summary["aligned_count"] == 1
    assert summary["latest_event"]["symbols"] == ["HOMEUSDT"]


def test_verify_production_services_ignores_old_or_unrelated_jsonl_events(tmp_path):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    current_state = runtime_dir / "live-state-current.json"
    state = {"generated_at_ms": 1778787000000}
    current_state.write_text(json.dumps(state))
    old_ts = 1778787000000 - (25 * 3600 * 1000)
    (runtime_dir / "events.jsonl").write_text(
        json.dumps(
            {
                "ts_ms": old_ts,
                "kind": "runtime.auto_fail_closed_recovered",
                "payload": {
                    "source": "auto_pending_entry_abort",
                    "symbols": ["OLDUSDT"],
                    "new_risk_mode": "running",
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (runtime_dir / "unrelated.jsonl").write_text(
        json.dumps(
            {
                "ts_ms": 1778786999000,
                "kind": "runtime.auto_fail_closed_recovered",
                "payload": {
                    "source": "auto_pending_entry_abort",
                    "symbols": ["NOISEUSDT"],
                    "new_risk_mode": "running",
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    enriched = vps._attach_auto_fail_closed_summary_if_missing(
        state,
        current_state_path=current_state,
    )

    assert "auto_fail_closed_summary" not in enriched


def test_verify_production_services_exchange_truth_probe_times_out(
    tmp_path, monkeypatch,
):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    current_state = runtime_dir / "live-state-current.json"
    state = {
        "schema": "lightfee.current_state.v1",
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
    }

    def stuck_exchange_truth(_runtime_dir_arg, _symbols, _venues=None):
        time.sleep(0.2)
        return {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        }

    monkeypatch.setattr(vps, "EXCHANGE_TRUTH_PROBE_TIMEOUT_S", 0.01, raising=False)
    started_at = time.monotonic()

    enriched = vps._attach_exchange_truth_if_missing(
        state,
        current_state_path=current_state,
        unit_texts={},
        exchange_truth_builder=stuck_exchange_truth,
    )

    elapsed_s = time.monotonic() - started_at
    assert elapsed_s < 0.15
    assert enriched["exchange_truth"]["available"] is False
    assert "exchange_truth_fetch_failed" in enriched["exchange_truth"]["missing_evidence"]
    assert "timed out" in enriched["exchange_truth"]["errors"][0].lower()


def test_verify_production_services_preserves_exchange_truth_probe_evidence(tmp_path):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    current_state = runtime_dir / "live-state-current.json"
    state = {
        "schema": "lightfee.current_state.v1",
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
    }

    def fake_exchange_truth(_runtime_dir_arg, _symbols, _venues=None):
        return {
            "available": True,
            "truth_available": True,
            "confidence": "partial",
            "positions": {},
            "open_orders": {},
            "fetch_status": {
                "bybit": {"status": "ok"},
                "okx": {
                    "status": "retryable_error",
                    "error": "HTTP 429 rate limit; retry after 1s",
                },
            },
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

    enriched = vps._attach_exchange_truth_if_missing(
        state,
        current_state_path=current_state,
        unit_texts={},
        exchange_truth_builder=fake_exchange_truth,
    )

    exchange_truth = enriched["exchange_truth"]
    assert exchange_truth["fetch_status"]["okx"]["status"] == "retryable_error"
    assert exchange_truth["errors"] == ["okx: HTTP 429 rate limit; retry after 1s"]
    assert exchange_truth["probe_evidence"][0]["classification"] == (
        "open_order_probe_retryable_error"
    )
    assert exchange_truth["probe_evidence"][0]["error"] == (
        "HTTP 429 rate limit; retry after 1s"
    )


def test_production_gate_does_not_report_clean_when_open_orders_present():
    state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "last_scan": {"candidate_count": 1, "tradeable_count": 1},
        "exchange_truth": {
            "available": True,
            "truth_available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": True,
            "positions": {"bybit": {}},
            "open_orders": {
                "bybit": {
                    "TRXUSDT": [
                        {
                            "venue": "bybit",
                            "symbol": "TRXUSDT",
                            "side": "buy",
                            "quantity": 72.0,
                            "reduce_only": False,
                            "order_id": "live-maker",
                        }
                    ]
                }
            },
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=10_000,
        require_exchange_truth=True,
    )

    assert report.ok is False
    assert report.severity == "critical"
    assert "exchange_truth_mismatch" in report.fingerprints
    assert "live_open_order" in report.fingerprints
    assert report.details["recovery_decision"]["kind"] == (
        "BLOCK_OR_FLATTEN_LIVE_ARTIFACT"
    )
    assert report.details["exchange_truth_mismatches"][0]["check"] == (
        "unexpected_live_open_order"
    )


def test_current_state_weak_order_truth_gap_is_not_green_even_when_flat():
    state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "last_scan": {"candidate_count": 1, "tradeable_count": 1},
        "recent_events": [
            {
                "kind": "exit.passive_close_hedge_confirmed_after_ack",
                "payload": {
                    "position_id": "pos-weak-truth",
                    "symbol": "BTCUSDT",
                    "order_truth_fill_status": "truth_gap",
                    "order_truth_evidence_status": "unavailable",
                    "order_truth_decision": "retain_backoff",
                    "order_truth_missing_evidence": ["fill_confirmation"],
                    "terminal_without_truth": False,
                },
            }
        ],
        "exchange_truth": {
            "available": True,
            "truth_available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {"bybit": {}},
            "open_orders": {"bybit": {}},
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=10_000,
        require_exchange_truth=True,
    )

    assert report.ok is False
    assert report.severity == "critical"
    assert "order_truth_gap_unresolved" in report.fingerprints
    assert report.details["weak_order_truth_events"][0]["symbol"] == "BTCUSDT"


def test_verify_production_services_cli_json_failure(tmp_path):
    unit_dir = tmp_path / "systemd"
    unit_dir.mkdir()
    (unit_dir / "lightfee-sidecar.service").write_text("[Service]\nExecStart=lightfee-sidecar\n")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/verify_production_services.py",
            "--unit-dir", str(unit_dir),
            "--now-ms", "1778787000000",
            "--json",
        ],
        text=True,
        capture_output=True,
    )
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["critical_count"] >= 1


def test_deploy_systemd_templates_pass_contract():
    sidecar = Path("deploy/systemd/lightfee-sidecar.service").read_text()
    live = Path("deploy/systemd/lightfee-live.service").read_text()
    assert analyze_systemd_unit("lightfee-sidecar.service", sidecar).ok
    assert analyze_systemd_unit("lightfee-live.service", live).ok


def test_deploy_dns_template_prefers_verified_resolver():
    text = Path("deploy/network/NetworkManager-lightfee-dns.conf").read_text()
    assert analyze_resolver_config(text).ok


def test_current_state_tick_stale_is_not_critical_when_flat_with_recent_scan_progress():
    state = {
        "schema": "lightfee.current_state.v1",
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786960000,
        "last_scan": {
            "ts_ms": 1778786995000,
            "candidate_count": 12,
            "selected_candidate_count": 0,
            "dispatched_candidate_count": 0,
        },
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=15_000,
        require_exchange_truth=True,
    )

    assert report.ok is True
    assert "live_tick_stale" not in report.fingerprints
    assert report.details["tick_stale_suppressed_by_runtime_progress"] is True


def test_current_state_tick_stale_remains_critical_with_exporter_only_heartbeat():
    state = {
        "schema": "lightfee.current_state.v1",
        "generated_at_ms": 1778786995000,
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786960000,
        "last_scan": {
            "ts_ms": 1778786800000,
            "candidate_count": 12,
            "selected_candidate_count": 0,
            "dispatched_candidate_count": 0,
        },
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=15_000,
        require_exchange_truth=True,
    )

    assert report.ok is False
    assert report.severity == "critical"
    assert "live_tick_stale" in report.fingerprints
    assert "exporter_only_progress" in report.fingerprints
    assert report.details["tick_stale_suppressed_by_runtime_progress"] is False
    assert report.details["current_state_age_ms"] == 5000
    assert report.details["progress_source"] == "exporter_only"
    assert report.details["exporter_only_progress"] is True


def test_current_state_tick_stale_is_not_critical_with_recent_runtime_lane_progress():
    state = {
        "schema": "lightfee.current_state.v1",
        "generated_at_ms": 1778786995000,
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786960000,
        "last_scan": {
            "ts_ms": 1778786800000,
            "candidate_count": 12,
        },
        "runtime_progress": {
            "loop_iteration_started_ms": 1778786993000,
            "loop_iteration_completed_ms": 1778786990000,
            "last_lane_progress_ms": 1778786994000,
            "active_lane": "",
            "active_lane_started_ms": 0,
            "active_lane_budget_ms": 0,
            "active_lane_overdue": False,
        },
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=15_000,
        require_exchange_truth=True,
    )

    assert report.ok is True
    assert "live_tick_stale" not in report.fingerprints
    assert report.details["tick_stale_suppressed_by_runtime_progress"] is True
    assert report.details["progress_source"] == "runtime_lane"
    assert report.details["exporter_only_progress"] is False


def test_current_state_tick_stale_is_not_critical_with_bounded_active_lane():
    state = {
        "schema": "lightfee.current_state.v1",
        "generated_at_ms": 1778786995000,
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786960000,
        "last_scan": {
            "ts_ms": 1778786800000,
            "candidate_count": 12,
        },
        "runtime_progress": {
            "loop_iteration_started_ms": 1778786993000,
            "loop_iteration_completed_ms": 1778786990000,
            "last_lane_progress_ms": 1778786900000,
            "active_lane": "full_tick",
            "active_lane_started_ms": 1778786992000,
            "active_lane_budget_ms": 15_000,
            "active_lane_overdue": False,
        },
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=15_000,
        require_exchange_truth=True,
    )

    assert report.ok is True
    assert "live_tick_stale" not in report.fingerprints
    assert report.details["tick_stale_suppressed_by_runtime_progress"] is True
    assert report.details["progress_source"] == "active_bounded_lane"


def test_current_state_journal_positive_fill_conflict_owns_historical_live_single_leg():
    state = {
        "schema": "lightfee.current_state.v1",
        "generated_at_ms": 1778786995000,
        "mode": "live",
        "lifecycle": "risk_only",
        "risk_mode": "running",
        "last_tick_ms": 1778786990000,
        "last_scan": {"ts_ms": 1778786990000},
        "open_position_count": 0,
        "open_positions": [],
        "pending_entry_count": 0,
        "pending_entries": [],
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "journal_events": [
            {
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
        ],
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": True,
            "has_open_order": False,
            "positions": {
                "bybit": {
                    "HOMEUSDT": {
                        "venue": "bybit",
                        "symbol": "HOMEUSDT",
                        "side": "Side.SELL",
                        "quantity": 1600.0,
                    }
                },
                "okx": {},
            },
            "open_orders": {
                "bybit": {"HOMEUSDT": []},
                "okx": {"HOMEUSDT": []},
            },
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=15_000,
        require_exchange_truth=True,
    )

    rows = report.details["v1_lifecycle_closure"]["rows"]
    assert "nonzero_live_position" in report.fingerprints
    assert any(
        row["owner_id"] == "entry-home"
        and row["terminality"] == "owned_pending_entry_live_conflict"
        and row["details"].get("kind") == "owned_pending_entry_live_conflict"
        for row in rows
    )
    assert not any("unpaired_live_position" in row["row_key"] for row in rows)


def test_current_state_tick_stale_is_critical_when_active_lane_overdue():
    state = {
        "schema": "lightfee.current_state.v1",
        "generated_at_ms": 1778786995000,
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786960000,
        "last_scan": {
            "ts_ms": 1778786800000,
            "candidate_count": 12,
        },
        "runtime_progress": {
            "loop_iteration_started_ms": 1778786965000,
            "loop_iteration_completed_ms": 1778786900000,
            "last_lane_progress_ms": 1778786900000,
            "active_lane": "full_tick",
            "active_lane_started_ms": 1778786965000,
            "active_lane_budget_ms": 15_000,
            "active_lane_overdue": True,
        },
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=15_000,
        require_exchange_truth=True,
    )

    assert report.ok is False
    assert report.severity == "critical"
    assert "live_tick_stale" in report.fingerprints
    assert report.details["tick_stale_suppressed_by_runtime_progress"] is False
    assert report.details["progress_source"] == "exporter_only"


def test_current_state_tick_stale_is_not_critical_when_flat_with_recent_progress_and_medium_truth():
    state = {
        "schema": "lightfee.current_state.v1",
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786960000,
        "last_scan": {
            "ts_ms": 1778786995000,
            "candidate_count": 12,
            "selected_candidate_count": 0,
            "dispatched_candidate_count": 0,
        },
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "exchange_truth": {
            "available": True,
            "confidence": "medium",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=15_000,
        require_exchange_truth=True,
    )

    assert "live_tick_stale" not in report.fingerprints
    assert "exchange_truth_confidence_not_high" in report.fingerprints
    assert report.severity == "critical"
    assert report.details["tick_stale_suppressed_by_runtime_progress"] is True


def test_current_state_tick_stale_remains_critical_without_runtime_progress():
    state = {
        "schema": "lightfee.current_state.v1",
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786960000,
        "last_scan": {
            "ts_ms": 1778786900000,
            "candidate_count": 12,
        },
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=15_000,
        require_exchange_truth=True,
    )

    assert report.ok is False
    assert report.severity == "critical"
    assert "live_tick_stale" in report.fingerprints
    assert report.details["tick_stale_suppressed_by_runtime_progress"] is False


def test_cli_reports_missing_snapshot_and_current_state(tmp_path):
    unit_dir = tmp_path / "systemd"
    unit_dir.mkdir()
    (unit_dir / "lightfee-sidecar.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/root/projects/LightFee/target/release/opportunity_input_sidecar --config /root/projects/LightFee/config/live.auto.toml\n"
    )
    (unit_dir / "lightfee-live.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.live --config /opt/lightfee-v2/config/live.toml\n"
    )
    resolv = tmp_path / "resolv.conf"
    resolv.write_text("nameserver 1.1.1.1\nnameserver 8.8.8.8\n")

    # snapshot and current-state paths point to non-existent files
    snapshot = tmp_path / "no-such-snapshot.json"
    current = tmp_path / "no-such-current.json"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/verify_production_services.py",
            "--unit-dir", str(unit_dir),
            "--snapshot", str(snapshot),
            "--current-state", str(current),
            "--resolv-conf", str(resolv),
            "--now-ms", "1778787000000",
            "--json",
        ],
        text=True,
        capture_output=True,
    )
    assert result.returncode == 1, result.stderr + result.stdout
    payload = json.loads(result.stdout)
    assert payload["critical_count"] >= 2  # snapshot + current-state both critical
    fingerprints = [fp for r in payload["reports"] for fp in r.get("fingerprints", [])]
    assert "snapshot_file_missing" in fingerprints
    assert "current_state_file_missing" in fingerprints
