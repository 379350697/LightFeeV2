import json
import subprocess
import sys
from pathlib import Path

from lightfee.ops.production_health import (
    analyze_current_state,
    analyze_resolver_config,
    analyze_sidecar_snapshot,
    analyze_systemd_unit,
    summarize_reports,
)


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
    sidecar = Path("deploy/systemd/lightfee-sidecar-rust-v1.service").read_text()
    live = Path("deploy/systemd/lightfee-live.service").read_text()
    assert analyze_systemd_unit("lightfee-sidecar.service", sidecar).ok
    assert analyze_systemd_unit("lightfee-live.service", live).ok


def test_deploy_dns_template_prefers_verified_resolver():
    text = Path("deploy/network/NetworkManager-lightfee-dns.conf").read_text()
    assert analyze_resolver_config(text).ok


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
