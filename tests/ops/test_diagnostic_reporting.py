from __future__ import annotations

import json

from lightfee.ops.diagnostics.reporting import (
    build_diagnose_report,
    render_budgeted_json,
)


def _sample_diagnose_result() -> dict:
    return {
        "schema_version": 2,
        "generated_at_ms": 123456,
        "scope": {
            "symbol": "*",
            "venues": [],
            "since_deploy": True,
            "max_events": 50_000,
            "event_files": ["/runtime/live-events.jsonl"],
            "events_parsed": 42,
            "event_scan_truncated": False,
            "events_dropped_by_cap": 0,
            "since_deploy_time_filtered": True,
            "state_path": "/runtime/live-state-current.json",
            "state_path_source": "explicit",
        },
        "window": {
            "mode": "since_deploy",
            "since_ms": 1000,
            "until_ms": 123456,
            "confidence": "high",
        },
        "conclusion": {
            "status": "healthy",
            "risk": "low",
            "summary": "no issues detected",
            "next_actions": [],
        },
        "health": {
            "ok": True,
            "critical_count": 0,
            "warning_count": 0,
            "fingerprints": [],
        },
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
            "available_venues": ["binance"],
            "has_nonzero_position": False,
            "has_open_order": False,
            "state_verdict": "consistent",
            "required_venues": ["binance"],
            "missing_required_venues": [],
            "missing_evidence": [],
            "errors": [],
        },
        "state_consistency": {
            "state_mismatch": False,
            "local_open_exchange_flat": False,
            "details": [],
        },
        "production_acceptance_gate": {
            "gate_passed": True,
            "blocking_reasons": [],
            "exchange_truth_missing_required_venues": [],
            "fingerprints": [],
            "next_actions": [],
        },
        "diagnostic_noise_summary": {
            "current_blocker_count": 0,
            "visibility_counts": {},
        },
        "top_exchange_errors": [],
        "order_error_evidence": [{"error": "SHOULD_NOT_LEAK"}],
    }


def test_agent_profile_keeps_context_safe_fields_only():
    report = build_diagnose_report(_sample_diagnose_result(), profile="agent")

    assert report["profile"] == "agent"
    assert report["scope"] == {
        "symbol": "*",
        "venues": [],
        "since_deploy": True,
        "max_events": 50_000,
        "events_parsed": 42,
        "event_file_count": 1,
        "event_scan_truncated": False,
        "events_dropped_by_cap": 0,
        "event_coverage": {
            "complete": True,
            "events_before_cap": 0,
            "events_dropped_by_cap": 0,
        },
        "since_deploy_time_filtered": True,
        "state_path_source": "explicit",
    }
    assert report["local_state"] == {
        "lifecycle": "RUNNING",
        "risk_mode": "running",
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
    }
    assert "positions" not in report["local_state"]
    assert "order_error_evidence" not in report


def test_gate_profile_is_smaller_than_agent_profile():
    result = _sample_diagnose_result()

    gate = build_diagnose_report(result, profile="gate")
    agent = build_diagnose_report(result, profile="agent")

    assert gate["profile"] == "gate"
    assert gate["gate_passed"] is True
    assert gate["exchange_truth"]["has_open_order"] is False
    assert len(json.dumps(gate)) < len(json.dumps(agent))


def test_render_budgeted_json_returns_manifest_when_profile_exceeds_budget(tmp_path):
    result = _sample_diagnose_result()
    result["conclusion"]["summary"] = "x" * 500

    rendered = render_budgeted_json(
        result,
        profile="agent",
        budget_bytes=1_500,
        artifact_dir=tmp_path,
        artifact_name="diagnose-agent-over-budget.json",
    )
    manifest = json.loads(rendered)

    assert manifest["status"] == "budget_exceeded"
    assert manifest["profile"] == "agent"
    assert manifest["budget_bytes"] == 1_500
    assert manifest["artifact"]["path"].endswith("diagnose-agent-over-budget.json")
    assert (tmp_path / "diagnose-agent-over-budget.json").exists()
    assert "summary" in manifest


def test_render_budgeted_json_manifest_respects_budget_when_summary_is_huge():
    result = _sample_diagnose_result()
    result["conclusion"]["summary"] = "x" * 5_000
    result["conclusion"]["next_actions"] = ["y" * 5_000]

    rendered = render_budgeted_json(
        result,
        profile="agent",
        budget_bytes=500,
    )
    manifest = json.loads(rendered)

    assert manifest["status"] == "budget_exceeded"
    assert len(rendered.encode("utf-8")) <= 500
    assert "exceeded context budget" in manifest["summary"]


def test_render_budgeted_json_manifest_truncates_summary_before_minimal_fallback():
    result = _sample_diagnose_result()
    result["conclusion"]["summary"] = "x" * 5_000

    rendered = render_budgeted_json(
        result,
        profile="agent",
        budget_bytes=1_500,
    )
    manifest = json.loads(rendered)

    assert manifest["status"] == "budget_exceeded"
    assert len(rendered.encode("utf-8")) <= 1_500
    assert manifest["summary"]["conclusion"]["summary"].endswith("...")
    assert len(manifest["summary"]["conclusion"]["summary"]) <= 120
