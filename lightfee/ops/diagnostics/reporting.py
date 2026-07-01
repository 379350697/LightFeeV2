"""Budgeted diagnostic report rendering.

Raw diagnostic evidence can be large. This module keeps stdout-safe reports
separate from full evidence artifacts so agent-facing commands do not
accidentally flood conversation context.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Literal

ReportProfile = Literal["full", "operator", "agent", "gate"]

DEFAULT_PROFILE_BUDGET_BYTES: dict[str, int] = {
    "operator": 16_000,
    "agent": 12_000,
    "gate": 6_000,
}


def _limit_list(value: Any, limit: int = 10) -> list[Any]:
    if not isinstance(value, list):
        return []
    return value[:limit]


def _as_bool(mapping: dict[str, Any], key: str, default: bool = False) -> bool:
    return bool(mapping.get(key, default))


def _as_int(mapping: dict[str, Any], key: str, default: int = 0) -> int:
    return int(mapping.get(key, default) or 0)


def _compact_scope(scope: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": scope.get("symbol", "*"),
        "venues": scope.get("venues", []),
        "since_deploy": _as_bool(scope, "since_deploy"),
        "max_events": _as_int(scope, "max_events"),
        "events_parsed": _as_int(scope, "events_parsed"),
        "event_file_count": len(scope.get("event_files", []) or []),
        "state_path_source": scope.get("state_path_source", ""),
    }


def _compact_local_state(local_state: dict[str, Any]) -> dict[str, Any]:
    return {
        "lifecycle": local_state.get("lifecycle", "unknown"),
        "risk_mode": local_state.get("risk_mode", "unknown"),
        "open_position_count": _as_int(local_state, "open_position_count"),
        "pending_entry_count": _as_int(local_state, "pending_entry_count"),
        "pending_close_count": _as_int(local_state, "pending_close_count"),
    }


def _compact_exchange_truth(exchange_truth: dict[str, Any]) -> dict[str, Any]:
    return {
        "available": _as_bool(exchange_truth, "available"),
        "confidence": exchange_truth.get("confidence", "unknown"),
        "available_venues": exchange_truth.get("available_venues", []),
        "has_nonzero_position": _as_bool(exchange_truth, "has_nonzero_position"),
        "has_open_order": _as_bool(exchange_truth, "has_open_order"),
        "state_verdict": exchange_truth.get("state_verdict", "unknown"),
        "errors": _limit_list(exchange_truth.get("errors", []), 5),
    }


def _compact_health(health: dict[str, Any]) -> dict[str, Any]:
    return {
        "ok": _as_bool(health, "ok"),
        "critical_count": _as_int(health, "critical_count"),
        "warning_count": _as_int(health, "warning_count"),
        "fingerprints": _limit_list(health.get("fingerprints", []), 10),
    }


def _compact_window(window: dict[str, Any]) -> dict[str, Any]:
    return {
        "mode": window.get("mode", "?"),
        "since_ms": window.get("since_ms", 0),
        "until_ms": window.get("until_ms", 0),
        "confidence": window.get("confidence", "?"),
    }


def _compact_state_consistency(state_consistency: dict[str, Any]) -> dict[str, Any]:
    return {
        "state_mismatch": _as_bool(state_consistency, "state_mismatch"),
        "local_open_exchange_flat": _as_bool(
            state_consistency,
            "local_open_exchange_flat",
        ),
        "details": _limit_list(state_consistency.get("details", []), 5),
    }


def _compact_gate(gate: dict[str, Any]) -> dict[str, Any]:
    return {
        "gate_passed": _as_bool(gate, "gate_passed"),
        "fingerprints": _limit_list(gate.get("fingerprints", []), 10),
        "next_actions": _limit_list(gate.get("next_actions", []), 10),
    }


def _compact_noise(noise: dict[str, Any]) -> dict[str, Any]:
    return {
        "current_blocker_count": _as_int(noise, "current_blocker_count"),
        "visibility_counts": noise.get("visibility_counts", {}),
    }


def _agent_diagnose_report(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "profile": "agent",
        "schema_version": result.get("schema_version", 0),
        "generated_at_ms": result.get("generated_at_ms", 0),
        "scope": _compact_scope(result.get("scope", {})),
        "window": _compact_window(result.get("window", {})),
        "conclusion": result.get("conclusion", {}),
        "health": _compact_health(result.get("health", {})),
        "local_state": _compact_local_state(result.get("local_state", {})),
        "exchange_truth": _compact_exchange_truth(result.get("exchange_truth", {})),
        "state_consistency": _compact_state_consistency(
            result.get("state_consistency", {})
        ),
        "production_acceptance_gate": _compact_gate(
            result.get("production_acceptance_gate", {})
        ),
        "diagnostic_noise_summary": _compact_noise(
            result.get("diagnostic_noise_summary", {})
        ),
        "top_exchange_errors": _limit_list(result.get("top_exchange_errors", []), 5),
    }


def _gate_diagnose_report(result: dict[str, Any]) -> dict[str, Any]:
    gate = result.get("production_acceptance_gate", {})
    local_state = result.get("local_state", {})
    exchange_truth = result.get("exchange_truth", {})
    conclusion = result.get("conclusion", {})
    return {
        "profile": "gate",
        "status": conclusion.get("status", "unknown"),
        "risk": conclusion.get("risk", "unknown"),
        "gate_passed": _as_bool(gate, "gate_passed"),
        "summary": conclusion.get("summary", ""),
        "next_actions": _limit_list(
            gate.get("next_actions") or conclusion.get("next_actions", []),
            10,
        ),
        "local_state": _compact_local_state(local_state),
        "exchange_truth": {
            "available": _as_bool(exchange_truth, "available"),
            "confidence": exchange_truth.get("confidence", "unknown"),
            "has_nonzero_position": _as_bool(
                exchange_truth,
                "has_nonzero_position",
            ),
            "has_open_order": _as_bool(exchange_truth, "has_open_order"),
            "state_verdict": exchange_truth.get("state_verdict", "unknown"),
        },
        "fingerprints": _limit_list(gate.get("fingerprints", []), 10),
    }


def build_diagnose_report(
    result: dict[str, Any],
    *,
    profile: ReportProfile = "full",
) -> dict[str, Any]:
    if profile == "full":
        return result
    if profile == "gate":
        return _gate_diagnose_report(result)
    if profile in {"agent", "operator"}:
        report = _agent_diagnose_report(result)
        report["profile"] = profile
        return report
    raise ValueError(f"unknown diagnostic report profile: {profile}")


def _json_bytes(value: dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8")


def _truncate_string(value: Any, limit: int) -> str:
    text = str(value or "")
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def _compact_conclusion(conclusion: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": conclusion.get("status", "unknown"),
        "risk": conclusion.get("risk", "unknown"),
        "summary": _truncate_string(conclusion.get("summary", ""), 120),
        "next_actions": [
            _truncate_string(action, 120)
            for action in _limit_list(conclusion.get("next_actions", []), 3)
        ],
    }


def _manifest_summary(report: dict[str, Any]) -> dict[str, Any]:
    conclusion = report.get("conclusion")
    if not isinstance(conclusion, dict):
        conclusion = {
            "status": report.get("status", "unknown"),
            "risk": report.get("risk", "unknown"),
            "summary": report.get("summary", ""),
            "next_actions": report.get("next_actions", []),
        }
    return {
        "conclusion": _compact_conclusion(conclusion),
        "local_state": report.get("local_state", {}),
        "exchange_truth": report.get("exchange_truth", {}),
        "gate_passed": report.get("gate_passed")
        if "gate_passed" in report
        else report.get("production_acceptance_gate", {}).get("gate_passed"),
    }


def _artifact_manifest(
    *,
    report: dict[str, Any],
    profile: str,
    budget_bytes: int,
    rendered_bytes: bytes,
    artifact_path: Path | None,
) -> dict[str, Any]:
    artifact = None
    if artifact_path is not None:
        artifact = {
            "path": str(artifact_path),
            "bytes": artifact_path.stat().st_size,
            "sha256": hashlib.sha256(artifact_path.read_bytes()).hexdigest(),
        }
    return {
        "status": "budget_exceeded",
        "profile": profile,
        "budget_bytes": budget_bytes,
        "rendered_bytes": len(rendered_bytes),
        "artifact": artifact,
        "summary": _manifest_summary(report),
    }


def _minimal_budget_exceeded_manifest(
    *,
    profile: str,
    budget_bytes: int,
    rendered_bytes: int,
    artifact_path: Path | None,
) -> dict[str, Any]:
    return {
        "status": "budget_exceeded",
        "profile": profile,
        "budget_bytes": budget_bytes,
        "rendered_bytes": rendered_bytes,
        "artifact_path": str(artifact_path) if artifact_path is not None else None,
        "summary": "diagnostic output exceeded context budget; inspect artifact or rerun with a narrower scope",
    }


def render_budgeted_json(
    result: dict[str, Any],
    *,
    profile: ReportProfile = "agent",
    budget_bytes: int | None = None,
    artifact_dir: str | Path | None = None,
    artifact_name: str = "diagnose-full.json",
) -> str:
    report = build_diagnose_report(result, profile=profile)
    rendered = _json_bytes(report)
    if profile == "full":
        return rendered.decode("utf-8") + "\n"

    effective_budget = (
        budget_bytes
        if budget_bytes is not None
        else DEFAULT_PROFILE_BUDGET_BYTES.get(profile, DEFAULT_PROFILE_BUDGET_BYTES["agent"])
    )
    if len(rendered) <= effective_budget:
        return rendered.decode("utf-8") + "\n"

    artifact_path = None
    if artifact_dir is not None:
        artifact_root = Path(artifact_dir)
        artifact_root.mkdir(parents=True, exist_ok=True)
        artifact_path = artifact_root / artifact_name
        artifact_path.write_bytes(_json_bytes(result))

    manifest = _artifact_manifest(
        report=report,
        profile=profile,
        budget_bytes=effective_budget,
        rendered_bytes=rendered,
        artifact_path=artifact_path,
    )
    manifest_bytes = _json_bytes(manifest)
    if len(manifest_bytes) > effective_budget:
        manifest = _minimal_budget_exceeded_manifest(
            profile=profile,
            budget_bytes=effective_budget,
            rendered_bytes=len(rendered),
            artifact_path=artifact_path,
        )
        manifest_bytes = _json_bytes(manifest)
    return manifest_bytes.decode("utf-8") + "\n"
