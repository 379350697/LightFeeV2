from __future__ import annotations

import json

import scripts.lfdiag as lfdiag


def test_lfdiag_diagnose_defaults_to_agent_profile(monkeypatch, capsys):
    def fake_run_diagnose(**kwargs):
        return {
            "conclusion": {
                "status": "healthy",
                "risk": "low",
                "summary": "no issues detected",
                "next_actions": [],
            },
            "scope": {
                "symbol": kwargs["symbol"] or "*",
                "venues": kwargs["venues"] or [],
                "since_deploy": kwargs["since_deploy"],
                "max_events": kwargs["max_events"],
                "event_files": ["/runtime/live-events.jsonl"],
                "events_parsed": 1,
                "state_path_source": "default",
            },
            "window": {},
            "health": {"ok": True, "critical_count": 0, "warning_count": 0},
            "local_state": {
                "lifecycle": "RUNNING",
                "risk_mode": "running",
                "open_position_count": 0,
                "pending_entry_count": 0,
                "pending_close_count": 0,
                "positions": [{"symbol": "SHOULD_NOT_LEAK"}],
            },
            "exchange_truth": {
                "available": True,
                "confidence": "high",
                "has_nonzero_position": False,
                "has_open_order": False,
                "state_verdict": "consistent",
            },
            "state_consistency": {},
            "production_acceptance_gate": {"gate_passed": True},
            "diagnostic_noise_summary": {},
        }

    monkeypatch.setattr(lfdiag.diagnose_live, "run_diagnose", fake_run_diagnose)

    rc = lfdiag.main(["diagnose", "--since-deploy", "--symbol", "BTCUSDT"])

    assert rc == 0
    output = json.loads(capsys.readouterr().out)
    assert output["profile"] == "agent"
    assert output["scope"]["symbol"] == "BTCUSDT"
    assert output["scope"]["since_deploy"] is True
    assert "positions" not in output["local_state"]


def test_lfdiag_diagnose_gate_profile(monkeypatch, capsys):
    def fake_run_diagnose(**kwargs):
        return {
            "conclusion": {
                "status": "healthy",
                "risk": "low",
                "summary": "no issues detected",
                "next_actions": [],
            },
            "local_state": {
                "lifecycle": "RUNNING",
                "risk_mode": "running",
                "open_position_count": 0,
                "pending_entry_count": 0,
                "pending_close_count": 0,
            },
            "exchange_truth": {
                "available": True,
                "confidence": "high",
                "has_nonzero_position": False,
                "has_open_order": False,
                "state_verdict": "consistent",
            },
            "production_acceptance_gate": {"gate_passed": True},
        }

    monkeypatch.setattr(lfdiag.diagnose_live, "run_diagnose", fake_run_diagnose)

    rc = lfdiag.main(["diagnose", "--profile", "gate"])

    assert rc == 0
    output = json.loads(capsys.readouterr().out)
    assert output["profile"] == "gate"
    assert output["gate_passed"] is True
