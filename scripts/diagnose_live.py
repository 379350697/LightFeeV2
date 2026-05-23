#!/usr/bin/env python3
"""Read-only production diagnostics for LightFeeV2 live.

Consumes structured journal events + live state + exchange snapshots
to produce a stable JSON diagnose artifact.  This artifact is consumed
by both local operators and the wlcodex Telegram cockpit — same facts,
same conclusion.

Usage:
  python scripts/diagnose_live.py --json                     # full diagnose
  python scripts/diagnose_live.py --json --symbol BTCUSDT     # filter
  python scripts/diagnose_live.py --json --runtime-dir ./tests/fixtures
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCHEMA_VERSION = 1
DEFAULT_RUNTIME_DIR = "/opt/lightfee-v2/runtime"
DEFAULT_UNIT_DIR = "/etc/systemd/system"
DEFAULT_MAX_EVENTS = 50_000
SERVICE_NAMES = ["lightfee-live.service", "lightfee-sidecar.service"]

ORDER_ERROR_KINDS = frozenset({
    "order.rejected",
    "order.uncertain",
    "exit.passive_close_maker_submit_error",
    "exit.passive_close_hedge_error",
})

L2_WARNING_KINDS = frozenset({
    "runtime.local_l2_sequence_gap",
    "runtime.local_l2_sync_failed",
    "runtime.snapshot_stale",
    "runtime.snapshot_degraded",
    "runtime.entry_blocked_local_l2_selection",
    "runtime.entry_local_l2_readiness_diagnostics",
    "scan.no_entry_diagnostics",
})

RUNTIME_WARNING_KINDS = frozenset({
    "runtime.lifecycle_changed",
    "runtime.risk_mode_changed",
    "runtime.fail_closed",
    "risk.warning_triggered",
    "risk.death_triggered",
})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_ms() -> int:
    return int(time.time() * 1000)


def _read_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    with open(p) as f:
        return json.load(f)


def _read_jsonl(path: str | Path, max_records: int = DEFAULT_MAX_EVENTS) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    records: list[dict[str, Any]] = []
    with open(p) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass
            if len(records) >= max_records:
                break
    return records


def _find_event_files(runtime_dir: str) -> list[Path]:
    base = Path(runtime_dir)
    if not base.exists():
        return []
    candidates: list[Path] = []
    for pattern in ["live-events*.jsonl", "events*.jsonl", "*.jsonl"]:
        for p in sorted(base.glob(pattern)):
            if p not in candidates:
                candidates.append(p)
    return candidates


def _try_read_unit(unit_dir: str, name: str) -> str:
    p = Path(unit_dir) / name
    try:
        return p.read_text()
    except (OSError, PermissionError):
        return ""


def _git_head(project_dir: str = "/opt/lightfee-v2") -> str:
    try:
        result = subprocess.run(
            ["git", "-C", project_dir, "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout.strip()
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# deploy status
# ---------------------------------------------------------------------------


def _read_deploy_version(runtime_dir: str) -> str:
    p = Path(runtime_dir) / "deploy_version.txt"
    try:
        return p.read_text().strip()
    except (OSError, PermissionError):
        return ""


def _build_deploy_status(runtime_dir: str) -> dict[str, Any]:
    git_head = _git_head()
    deploy_version = _read_deploy_version(runtime_dir)
    mismatch = bool(git_head and deploy_version and git_head != deploy_version)
    return {
        "git_head": git_head,
        "deploy_version": deploy_version,
        "version_mismatch": mismatch,
    }


# ---------------------------------------------------------------------------
# service status
# ---------------------------------------------------------------------------


def _build_service_status(unit_dir: str) -> dict[str, Any]:
    status: dict[str, Any] = {}
    for name in SERVICE_NAMES:
        unit_text = _try_read_unit(unit_dir, name)
        active = "unknown"
        try:
            result = subprocess.run(
                ["systemctl", "is-active", name],
                capture_output=True, text=True, timeout=5,
            )
            active = result.stdout.strip()
        except Exception:
            pass
        status[name.replace(".service", "")] = {
            "active": active,
            "unit_exists": bool(unit_text),
        }
    return status


# ---------------------------------------------------------------------------
# health
# ---------------------------------------------------------------------------


def _build_health(state: dict[str, Any], service_status: dict[str, Any]) -> dict[str, Any]:
    fingerprints: list[str] = []
    critical = 0
    warning = 0

    lifecycle = str(state.get("lifecycle", ""))
    risk_mode = str(state.get("risk_mode", ""))
    if lifecycle not in ("running", ""):
        fingerprints.append("lifecycle_{}".format(lifecycle))
        critical += 1
    if risk_mode in ("fail_closed", "risk_only"):
        fingerprints.append("risk_mode_{}".format(risk_mode))
        critical += 1
    if risk_mode == "warning":
        warning += 1

    for svc_name, svc in service_status.items():
        if svc.get("active") not in ("active", "unknown"):
            fingerprints.append("service_{}_{}".format(svc_name, svc.get("active")))
            critical += 1

    last_tick = int(state.get("last_tick_ms", 0) or 0)
    if last_tick and _now_ms() - last_tick > 300_000:
        fingerprints.append("tick_stale_5min")
        warning += 1

    return {
        "ok": critical == 0,
        "critical_count": critical,
        "warning_count": warning,
        "fingerprints": fingerprints,
    }


# ---------------------------------------------------------------------------
# local state
# ---------------------------------------------------------------------------


def _build_local_state(
    state: dict[str, Any], events: list[dict[str, Any]]
) -> dict[str, Any]:
    positions: list[dict[str, Any]] = []
    for pos in state.get("open_positions", []) or []:
        if isinstance(pos, dict):
            positions.append({
                "position_id": pos.get("position_id", ""),
                "symbol": pos.get("symbol", ""),
                "long_venue": pos.get("long_venue", ""),
                "short_venue": pos.get("short_venue", ""),
                "quantity": pos.get("quantity", 0),
                "matched_quantity": pos.get("matched_quantity", 0),
                "opened_at_ms": pos.get("opened_at_ms", 0),
            })

    return {
        "lifecycle": str(state.get("lifecycle", "unknown")),
        "risk_mode": str(state.get("risk_mode", "unknown")),
        "open_position_count": int(state.get("open_position_count", 0) or 0),
        "pending_entry_count": int(state.get("pending_entry_count", 0) or 0),
        "pending_close_count": int(state.get("pending_close_count", 0) or 0),
        "positions": positions,
    }


# ---------------------------------------------------------------------------
# exchange truth (placeholder — requires live API access)
# ---------------------------------------------------------------------------


def _build_exchange_truth(
    events: list[dict[str, Any]], symbol: str = "",
) -> dict[str, Any]:
    return {
        "available": False,
        "note": "exchange truth requires live API credentials; not available in read-only script",
        "positions": {},
        "open_orders": {},
        "errors": [],
    }


# ---------------------------------------------------------------------------
# state consistency
# ---------------------------------------------------------------------------


def _build_state_consistency(
    local_state: dict[str, Any], exchange_truth: dict[str, Any]
) -> dict[str, Any]:
    details: list[dict[str, Any]] = []
    local_open = local_state.get("open_position_count", 0)
    exchange_available = exchange_truth.get("available", False)

    if not exchange_available:
        details.append({
            "check": "exchange_truth_available",
            "ok": False,
            "detail": "exchange truth not available — cannot verify consistency",
        })
        return {
            "state_mismatch": False,
            "local_open_exchange_flat": False,
            "details": details,
        }

    exchange_positions = exchange_truth.get("positions", {})
    exchange_has_positions = bool(exchange_positions)
    local_has_positions = local_open > 0

    state_mismatch = local_has_positions != exchange_has_positions
    local_open_exchange_flat = local_has_positions and not exchange_has_positions

    if local_open_exchange_flat:
        details.append({
            "check": "local_open_exchange_flat",
            "ok": False,
            "detail": "local has {} open position(s) but exchange reports no positions".format(local_open),
        })
    if state_mismatch:
        details.append({
            "check": "state_mismatch",
            "ok": False,
            "detail": "local and exchange state diverge",
        })

    if not details:
        details.append({"check": "consistency", "ok": True, "detail": "local and exchange state consistent"})

    return {
        "state_mismatch": state_mismatch,
        "local_open_exchange_flat": local_open_exchange_flat,
        "details": details,
    }


# ---------------------------------------------------------------------------
# order error evidence
# ---------------------------------------------------------------------------


def _build_order_error_evidence(
    events: list[dict[str, Any]], symbol: str = "",
) -> list[dict[str, Any]]:
    groups: dict[tuple, dict[str, Any]] = {}

    for rec in events:
        kind = str(rec.get("kind", ""))
        if kind not in ORDER_ERROR_KINDS:
            continue
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue

        event_symbol = str(payload.get("symbol", ""))
        venue = str(payload.get("venue", payload.get("hedge_venue", "")))
        position_id = str(payload.get("position_id", ""))
        error_msg = str(payload.get("error", payload.get("reason", "")))

        if symbol and event_symbol and event_symbol != symbol:
            continue

        exchange_error = payload.get("exchange_error", {})
        if isinstance(exchange_error, dict):
            ex_code = str(exchange_error.get("exchange_code", ""))
            ex_msg = str(exchange_error.get("exchange_msg", ""))
            completeness = str(exchange_error.get("evidence_completeness", ""))
        else:
            ex_code = ""
            ex_msg = ""
            completeness = ""

        # Determine operation from kind
        if "passive_close" in kind:
            operation = "submit_passive_order" if "maker" in kind else "place_order"
        else:
            operation = "place_order"

        key = (kind, position_id, venue, event_symbol, ex_code, error_msg[:100])
        if key not in groups:
            groups[key] = {
                "kind": kind,
                "position_id": position_id,
                "symbol": event_symbol,
                "venue": venue,
                "operation": operation,
                "error": error_msg[:500],
                "exchange_error": exchange_error if isinstance(exchange_error, dict) else {},
                "request_context": payload.get("request_context", {}),
                "count": 0,
                "first_ts_ms": rec.get("ts_ms", 0),
                "last_ts_ms": rec.get("ts_ms", 0),
            }
        g = groups[key]
        g["count"] += 1
        ts = rec.get("ts_ms", 0)
        if ts:
            g["first_ts_ms"] = min(g["first_ts_ms"], ts) if g["first_ts_ms"] else ts
            g["last_ts_ms"] = max(g["last_ts_ms"], ts)

    return sorted(groups.values(), key=lambda g: g["first_ts_ms"] or 0)


# ---------------------------------------------------------------------------
# L2 evidence
# ---------------------------------------------------------------------------


def _build_l2_evidence(events: list[dict[str, Any]]) -> dict[str, Any]:
    missing_l2_count = 0
    stale_rebuild_count = 0
    sequence_gap_count = 0
    details: list[dict[str, Any]] = []

    for rec in events:
        kind = str(rec.get("kind", ""))
        if kind not in L2_WARNING_KINDS:
            continue

        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue

        if kind == "runtime.local_l2_sequence_gap":
            sequence_gap_count += 1
        elif kind == "runtime.local_l2_sync_failed":
            stale_rebuild_count += 1
        elif kind in ("runtime.snapshot_stale", "runtime.snapshot_degraded"):
            stale_rebuild_count += 1
        elif kind in ("runtime.entry_blocked_local_l2_selection", "runtime.entry_local_l2_readiness_diagnostics"):
            not_ready = payload.get("not_ready", []) or []
            reason_totals = payload.get("reason_totals", {})
            if reason_totals:
                missing_l2_count += sum(
                    int(v) for v in reason_totals.values()
                    if isinstance(v, (int, float))
                )
            if not_ready:
                for item in not_ready[:3]:
                    if isinstance(item, dict):
                        details.append({
                            "kind": kind,
                            "pair_id": item.get("pair_id", ""),
                            "venue": item.get("venue", ""),
                            "symbol": item.get("symbol", ""),
                            "reason": item.get("reason", ""),
                            "ts_ms": rec.get("ts_ms", 0),
                        })

        elif kind == "scan.no_entry_diagnostics":
            reason_totals = payload.get("entry_local_l2_primary_not_ready_reason_totals", {})
            if reason_totals:
                missing_l2_count += sum(
                    int(v) for v in reason_totals.values()
                    if isinstance(v, (int, float))
                )

    return {
        "missing_l2_or_tick_count": missing_l2_count,
        "stale_rebuild_count": stale_rebuild_count,
        "sequence_gap_count": sequence_gap_count,
        "details": details[:20],
    }


# ---------------------------------------------------------------------------
# runtime warnings
# ---------------------------------------------------------------------------


def _build_runtime_warnings(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    seen: set[tuple] = set()

    for rec in events:
        kind = str(rec.get("kind", ""))
        if kind not in RUNTIME_WARNING_KINDS:
            # Also catch "was never awaited" patterns in any error payload
            payload = rec.get("payload", {})
            if not isinstance(payload, dict):
                continue
            error_str = str(payload.get("error", payload.get("reason", "")))
            if "was never awaited" in error_str:
                key = (kind, error_str[:100])
                if key not in seen:
                    seen.add(key)
                    warnings.append({
                        "kind": kind,
                        "source": "never_awaited",
                        "error": error_str[:500],
                        "ts_ms": rec.get("ts_ms", 0),
                    })
            continue

        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            continue

        msg = str(payload.get("to", payload.get("reason", "")))
        key = (kind, msg[:100])
        if key not in seen:
            seen.add(key)
            warnings.append({
                "kind": kind,
                "ts_ms": rec.get("ts_ms", 0),
                "detail": msg[:500],
            })

    return sorted(warnings, key=lambda w: w.get("ts_ms", 0))


# ---------------------------------------------------------------------------
# evidence completeness
# ---------------------------------------------------------------------------


def _build_evidence_completeness(
    order_errors: list[dict[str, Any]],
    state_consistency: dict[str, Any],
    exchange_truth: dict[str, Any],
) -> dict[str, Any]:
    missing: list[str] = []
    completions: set[str] = set()

    for err in order_errors:
        ex_err = err.get("exchange_error", {})
        if isinstance(ex_err, dict):
            comp = str(ex_err.get("evidence_completeness", ""))
            if comp:
                completions.add(comp)
            for m in ex_err.get("missing_evidence", []) or []:
                if m and m not in missing:
                    missing.append(m)

    if not exchange_truth.get("available", False):
        missing.append("exchange_truth_unavailable")

    if state_consistency.get("local_open_exchange_flat"):
        missing.append("state_consistency_breach")

    if not completions:
        overall = "missing"
        confidence = "low"
    elif "transport_only" in completions or "missing" in completions:
        overall = "missing"
        confidence = "low"
    elif "missing_body" in completions or "partial" in completions:
        overall = "partial"
        confidence = "medium"
    else:
        overall = "complete"
        confidence = "high"

    if missing and overall == "complete":
        overall = "partial"
        confidence = "medium"

    return {
        "overall": overall,
        "missing_evidence": missing,
        "confidence": confidence,
    }


# ---------------------------------------------------------------------------
# conclusion
# ---------------------------------------------------------------------------


def _build_conclusion(
    health: dict[str, Any],
    state_consistency: dict[str, Any],
    evidence_completeness: dict[str, Any],
    order_errors: list[dict[str, Any]],
    l2_evidence: dict[str, Any],
) -> dict[str, Any]:
    if health["ok"] and not state_consistency["state_mismatch"] and not order_errors:
        status = "healthy"
        risk = "low"
    elif health["critical_count"] > 0:
        status = "unhealthy"
        risk = "high"
    elif state_consistency["state_mismatch"]:
        status = "degraded"
        risk = "high"
    elif order_errors:
        status = "degraded"
        has_rejected = any(e["kind"] == "order.rejected" for e in order_errors)
        risk = "medium" if has_rejected else "low"
    else:
        status = "degraded"
        risk = "medium"

    summary_parts: list[str] = []
    if health.get("fingerprints"):
        summary_parts.append("health issues: {}".format(", ".join(health["fingerprints"][:3])))
    if state_consistency.get("state_mismatch"):
        summary_parts.append("state mismatch detected")
    if l2_evidence["missing_l2_or_tick_count"] > 0:
        summary_parts.append("L2/tick gaps: {}".format(l2_evidence["missing_l2_or_tick_count"]))
    if order_errors:
        summary_parts.append("{} order error group(s)".format(len(order_errors)))
    if evidence_completeness["overall"] != "complete":
        summary_parts.append("evidence: {}".format(evidence_completeness["overall"]))

    if not summary_parts:
        summary = "no issues detected"
    else:
        summary = "; ".join(summary_parts)

    next_actions: list[str] = []
    if state_consistency.get("local_open_exchange_flat"):
        next_actions.append("verify position on exchange — local reports open but exchange flat")
    if evidence_completeness["overall"] in ("partial", "missing"):
        next_actions.append("collect full exchange error bodies (raw_body, exchange_code)")
    if order_errors:
        next_actions.append("review order_error_evidence for root cause")
    if l2_evidence["stale_rebuild_count"] > 0:
        next_actions.append("investigate L2 stale/rebuild")
    if not next_actions:
        next_actions.append("no immediate action required")

    return {
        "status": status,
        "summary": summary,
        "risk": risk,
        "next_actions": next_actions,
    }


# ---------------------------------------------------------------------------
# Main diagnose
# ---------------------------------------------------------------------------


def run_diagnose(
    *,
    runtime_dir: str = DEFAULT_RUNTIME_DIR,
    unit_dir: str = DEFAULT_UNIT_DIR,
    current_state_path: str = "",
    event_paths: list[str] | None = None,
    snapshot_path: str = "",
    symbol: str = "",
    max_events: int = DEFAULT_MAX_EVENTS,
    now_ms: int = 0,
    since_deploy: bool = False,
) -> dict[str, Any]:
    generated_at_ms = now_ms or _now_ms()

    # State
    state_path = Path(current_state_path) if current_state_path else Path(runtime_dir) / "state-current.json"
    state = _read_json(state_path)

    # Events
    if event_paths:
        event_files = [Path(p) for p in event_paths]
    else:
        event_files = _find_event_files(runtime_dir)
    all_events: list[dict[str, Any]] = []
    for ef in event_files:
        all_events.extend(_read_jsonl(ef, max_events))
        if len(all_events) >= max_events:
            break

    # Filter by since_deploy
    if since_deploy:
        cutoff_ms = generated_at_ms - 24 * 3600 * 1000
        all_events = [e for e in all_events if int(e.get("ts_ms", 0) or 0) >= cutoff_ms]

    if symbol:
        all_events = [e for e in all_events if _event_matches_symbol(e, symbol)]

    # Sub-reports
    deploy_status = _build_deploy_status(runtime_dir)
    service_status = _build_service_status(unit_dir)
    health = _build_health(state, service_status)
    local_state = _build_local_state(state, all_events)
    exchange_truth = _build_exchange_truth(all_events, symbol)
    state_consistency = _build_state_consistency(local_state, exchange_truth)
    order_errors = _build_order_error_evidence(all_events, symbol)
    l2_evidence = _build_l2_evidence(all_events)
    runtime_warnings = _build_runtime_warnings(all_events)
    evidence_completeness = _build_evidence_completeness(
        order_errors, state_consistency, exchange_truth,
    )
    conclusion = _build_conclusion(
        health, state_consistency, evidence_completeness, order_errors, l2_evidence,
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_ms": generated_at_ms,
        "scope": {
            "symbol": symbol or "*",
            "since_deploy": since_deploy,
            "max_events": max_events,
            "event_files": [str(ef) for ef in event_files],
            "events_parsed": len(all_events),
        },
        "deploy_status": deploy_status,
        "service_status": service_status,
        "health": health,
        "local_state": local_state,
        "exchange_truth": exchange_truth,
        "state_consistency": state_consistency,
        "order_error_evidence": order_errors,
        "l2_evidence": l2_evidence,
        "runtime_warnings": runtime_warnings,
        "evidence_completeness": evidence_completeness,
        "conclusion": conclusion,
    }


def _event_matches_symbol(event: dict[str, Any], symbol: str) -> bool:
    payload = event.get("payload", {})
    if not isinstance(payload, dict):
        return False
    event_symbol = str(payload.get("symbol", "")).upper()
    return event_symbol == symbol.upper()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="LightFeeV2 read-only production diagnostics")
    parser.add_argument("--json", action="store_true", default=True,
                       help="Output JSON (default)")
    parser.add_argument("--symbol", type=str, default="",
                       help="Filter by symbol")
    parser.add_argument("--since-deploy", action="store_true", default=False,
                       help="Limit to events since last deploy")
    parser.add_argument("--runtime-dir", type=str, default=DEFAULT_RUNTIME_DIR,
                       help="Runtime directory (default: {})".format(DEFAULT_RUNTIME_DIR))
    parser.add_argument("--current-state", type=str, default="",
                       help="Path to live-state-current.json")
    parser.add_argument("--events", type=str, nargs="*", default=None,
                       help="Specific event file(s); auto-discovered if omitted")
    parser.add_argument("--snapshot", type=str, default="",
                       help="Path to opportunity-input-snapshot.json")
    parser.add_argument("--unit-dir", type=str, default=DEFAULT_UNIT_DIR,
                       help="Systemd unit directory (default: {})".format(DEFAULT_UNIT_DIR))
    parser.add_argument("--max-events", type=int, default=DEFAULT_MAX_EVENTS,
                       help="Max events to parse (default: {})".format(DEFAULT_MAX_EVENTS))
    parser.add_argument("--now-ms", type=int, default=0,
                       help="Override current time in ms (testing)")
    args = parser.parse_args()

    result = run_diagnose(
        runtime_dir=args.runtime_dir,
        unit_dir=args.unit_dir,
        current_state_path=args.current_state,
        event_paths=args.events,
        snapshot_path=args.snapshot,
        symbol=args.symbol,
        max_events=args.max_events,
        now_ms=args.now_ms,
        since_deploy=args.since_deploy,
    )

    if args.json:
        json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
        sys.stdout.write("\n")
    else:
        # Human-readable summary
        c = result["conclusion"]
        h = result["health"]
        sc = result["state_consistency"]
        ec = result["evidence_completeness"]
        ls = result["local_state"]
        print("Status: {}  Risk: {}  Confidence: {}".format(c["status"], c["risk"], ec["confidence"]))
        health_str = "OK" if h["ok"] else "CRITICAL={} WARN={}".format(h["critical_count"], h["warning_count"])
        print("Health: {}".format(health_str))
        print("Local: {}/{} open={} pending_entry={} pending_close={}".format(
            ls["lifecycle"], ls["risk_mode"],
            ls["open_position_count"], ls["pending_entry_count"], ls["pending_close_count"],
        ))
        if sc["state_mismatch"]:
            print("STATE MISMATCH: {}".format(sc.get("details", [])))
        if ec["missing_evidence"]:
            print("Missing evidence: {}".format(ec["missing_evidence"]))
        if result["order_error_evidence"]:
            print("Order errors: {} groups".format(len(result["order_error_evidence"])))
            for e in result["order_error_evidence"][:5]:
                ex = e.get("exchange_error", {})
                print("  {} {} {}: {}".format(e["kind"], e["venue"], e["symbol"], e["error"][:100]))
                if ex.get("exchange_code"):
                    print("    code={} msg={}".format(ex["exchange_code"], ex.get("exchange_msg", "")[:80]))
        print("Summary: {}".format(c["summary"]))
        if c["next_actions"]:
            for a in c["next_actions"]:
                print("  -> {}".format(a))


if __name__ == "__main__":
    main()
