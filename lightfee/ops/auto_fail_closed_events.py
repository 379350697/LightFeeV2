"""Shared diagnostics helpers for automatic fail-closed incidents."""

from __future__ import annotations

from typing import Any


AUTO_FAIL_CLOSED_EVENT_KINDS = frozenset({
    "runtime.auto_fail_closed_recovered",
    "runtime.auto_fail_closed_cleanup_failed",
})


def _string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item or "")]
    if isinstance(value, str) and value:
        return [value]
    return []


def build_auto_fail_closed_summary(
    events: list[dict[str, Any]],
    *,
    since_ms: int = 0,
) -> dict[str, Any]:
    recovered_count = 0
    cleanup_failed_count = 0
    latest_event: dict[str, Any] | None = None

    for rec in events:
        kind = str(rec.get("kind") or "")
        if kind not in AUTO_FAIL_CLOSED_EVENT_KINDS:
            continue
        payload = rec.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        ts_ms = int(rec.get("ts_ms") or payload.get("ts_ms") or 0)
        if since_ms and ts_ms < since_ms:
            continue
        if kind == "runtime.auto_fail_closed_recovered":
            recovered_count += 1
            final_status = "recovered"
        else:
            cleanup_failed_count += 1
            final_status = "cleanup_failed"
        symbols = _string_list(payload.get("symbols"))
        if not symbols and payload.get("symbol"):
            symbols = [str(payload.get("symbol"))]
        event = {
            "kind": kind,
            "ts_ms": ts_ms,
            "final_status": final_status,
            "source": str(payload.get("source") or ""),
            "reason": str(payload.get("reason") or ""),
            "symbols": symbols,
            "venues": _string_list(payload.get("venues")),
            "new_risk_mode": str(payload.get("new_risk_mode") or ""),
            "residual_blockers": _string_list(payload.get("residual_blockers")),
        }
        if latest_event is None or event["ts_ms"] >= int(latest_event.get("ts_ms") or 0):
            latest_event = event

    return {
        "recovered_count": recovered_count,
        "cleanup_failed_count": cleanup_failed_count,
        "recent_incident": bool(recovered_count or cleanup_failed_count),
        "latest_event": latest_event or {},
        "window": {
            "since_ms": since_ms,
        },
    }
