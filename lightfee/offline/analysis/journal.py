"""Journal analysis: venue stats, failure rates, latency, PnL, diagnostics.

Read paths:
- analyze_journal_records() — canonical journal scan (always works, used as fallback)
- analyze_from_store() — structured store query (fast, requires prior projection)
- analyze_journal_or_store() — store-first with automatic journal fallback
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from statistics import median


@dataclass
class VenueOrderStats:
    venue: str
    order_count: int = 0
    fill_count: int = 0
    failure_count: int = 0
    total_latency_ms: int = 0
    max_latency_ms: int = 0
    min_latency_ms: int = 9223372036854775807  # i64 max
    total_fee_quote: float = 0.0


@dataclass
class DailyPnLSummary:
    date: str = ""
    total_pnl_quote: float = 0.0
    total_fee_quote: float = 0.0
    entry_count: int = 0
    exit_count: int = 0
    by_venue: dict[str, float] = field(default_factory=dict)
    by_symbol: dict[str, float] = field(default_factory=dict)


@dataclass
class JournalAnalysisReport:
    """Semantically equivalent to V1 JournalAnalysisReport.

    Consumes all record-layer event kinds that V1 offline analysis processes,
    including recovery, risk, scan diagnostics, execution diagnostics, and
    local-L2 health events.
    """
    total_records: int = 0
    venue_stats: dict[str, VenueOrderStats] = field(default_factory=dict)
    daily: DailyPnLSummary = field(default_factory=DailyPnLSummary)

    # Recovery evidence counts (V1: JournalAnalysisReport.recovery_counts)
    recovery_counts: dict[str, int] = field(default_factory=dict)

    # Risk trigger counts (V1: risk.warning_triggered/cleared, death, protection)
    risk_counts: dict[str, int] = field(default_factory=dict)

    # Scan diagnostics (V1: scan.no_entry_diagnostics, scan.runtime_gate_blocked)
    scan_no_entry_diagnostics_count: int = 0
    scan_runtime_gate_blocked_count: int = 0

    # Execution diagnostics (V1: execution.entry_liquidity_blocked)
    execution_liquidity_blocked_count: int = 0

    # Local-L2 health (V1: local_l2_sequence_gap, local_l2_sync_failed)
    local_l2_sequence_gap_count: int = 0
    local_l2_sync_failed_count: int = 0

    # Additional V1-visible breakdowns
    local_l2_sequence_gap_by_reason: dict[str, int] = field(default_factory=dict)
    local_l2_sync_failed_by_category: dict[str, int] = field(default_factory=dict)
    entry_liquidity_blocked_by_reason: dict[str, int] = field(default_factory=dict)
    entry_liquidity_blocked_by_open_interest_evidence_status: dict[str, int] = (
        field(default_factory=dict)
    )

    # Execution diagnostic classifications (V1: entry_liquidity_blocked_by_eligibility_class)
    execution_liquidity_blocked_by_class: dict[str, int] = field(default_factory=dict)

    # Fail-closed reason counts (V1: fail_closed_reason_counts)
    fail_closed_reason_counts: dict[str, int] = field(default_factory=dict)

    # Paper outcome tracking (V1: paper_outcome event kinds)
    paper_outcome_markout_count: int = 0
    paper_outcome_closed_count: int = 0
    paper_outcome_joined_count: int = 0
    paper_outcome_by_label: dict[str, int] = field(default_factory=dict)

    # Quick-flat observability
    quick_flat_count: int = 0
    quick_flat_duplicate_event_count: int = 0
    quick_flat_low_confidence_event_count: int = 0
    quick_flat_close_identity_confidence: str = "high"

    # Exit shadow advisor diagnostics
    exit_shadow_decision_count: int = 0
    exit_shadow_path_markout_count: int = 0
    exit_shadow_summary_count: int = 0
    exit_shadow_by_bot: dict[str, dict] = field(default_factory=dict)


_RECOVERY_KINDS = frozenset({
    "recovery.live_detected",
    "recovery.flat",
    "recovery.blocked",
    "recovery.mismatch_detected",
    "recovery.mismatch_flattened",
    "recovery.resumed",
})

_RISK_KINDS = frozenset({
    "risk.warning_triggered",
    "risk.warning_cleared",
    "risk.death_triggered",
    "risk.single_side_protection_triggered",
    "risk.single_side_protection_failed",
    "risk.single_side_protection_unavailable",
})

_SCAN_NO_ENTRY = "scan.no_entry_diagnostics"
_SCAN_GATE_BLOCKED = "scan.runtime_gate_blocked"
_EXEC_LIQUIDITY_BLOCKED = "execution.entry_liquidity_blocked"
_LOCAL_L2_SEQUENCE_GAP = "runtime.local_l2_sequence_gap"
_LOCAL_L2_SYNC_FAILED = "runtime.local_l2_sync_failed"

_PAPER_OUTCOME_KINDS = frozenset({
    "opportunity.paper_markout",
    "opportunity.paper_closed",
    "opportunity.real_vs_paper_joined",
})

_QUICK_FLAT_TERMINAL_KIND_PRIORITY = {
    "exit.closed": 20,
    "runtime.position_lifecycle_terminal": 30,
    "recovery.flat": 40,
}

_ENTRY_OVERHEDGE_DRIFT_CORRECTION_KINDS = frozenset({
    "runtime.position_drift_corrected",
})


def _event_payload(event: dict) -> dict:
    payload = event.get("payload", {})
    return payload if isinstance(payload, dict) else {}


def _event_ts_ms(event: dict) -> int:
    return int(event.get("ts_ms", 0) or 0)


def quick_flat_event_key(event: dict) -> tuple:
    """Return the best available duplicate key for a quick-flat terminal event."""
    payload = _event_payload(event)
    position_id = str(payload.get("position_id", "") or "")
    reason = str(payload.get("reason", "") or "")
    ts_ms = _event_ts_ms(event)
    close_id = payload.get("close_id")
    if close_id:
        return (position_id, reason, str(close_id), ts_ms)
    return (str(event.get("kind", "") or ""), position_id, reason, ts_ms)


_ENTRY_START_KIND_PRIORITY = {
    "review.candidate_shortlisted": 10,
    "execution.entry_selected": 20,
    "order.submitted": 30,
    "runtime.entry_dispatched": 40,
    "runtime.pending_entry_registered": 50,
}


def _entry_position_id(payload: dict) -> str:
    return str(
        payload.get("position_id")
        or payload.get("entry_id")
        or payload.get("internal_entry_id")
        or payload.get("pending_id")
        or ""
    )


def _positive_payload_ts(payload: dict, key: str) -> int:
    try:
        value = int(payload.get(key, 0) or 0)
    except (TypeError, ValueError):
        return 0
    return value if value > 0 else 0


def _exit_shadow_bot_summary(report: JournalAnalysisReport, bot_id: str) -> dict:
    return report.exit_shadow_by_bot.setdefault(
        bot_id,
        {
            "sample_count": 0,
            "excluded_count": 0,
            "direction_correct_count": 0,
            "win_count": 0,
            "incremental_net_bps_values": [],
            "max_adverse_bps_values": [],
            "exclude_reasons": {},
            "direction_accuracy": 0.0,
            "win_rate": 0.0,
            "avg_incremental_net_bps": 0.0,
            "median_incremental_net_bps": 0.0,
            "max_adverse_bps": 0.0,
        },
    )


def _bool_payload(payload: dict, key: str) -> bool:
    value = payload.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def _float_payload(payload: dict, key: str) -> float:
    try:
        return float(payload.get(key, 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _record_exit_shadow_summary(report: JournalAnalysisReport, payload: dict) -> None:
    bot_id = str(payload.get("bot_id", "") or "unknown")
    summary = _exit_shadow_bot_summary(report, bot_id)
    excluded = _bool_payload(payload, "excluded")
    if excluded:
        summary["excluded_count"] += 1
        reason = str(payload.get("exclude_reason", "") or "unspecified")
        reasons = summary["exclude_reasons"]
        reasons[reason] = reasons.get(reason, 0) + 1
        return

    incremental_net_bps = _float_payload(payload, "incremental_net_bps")
    max_adverse_bps = _float_payload(payload, "max_adverse_bps")
    summary["sample_count"] += 1
    if _bool_payload(payload, "direction_correct"):
        summary["direction_correct_count"] += 1
    if incremental_net_bps > 0.0:
        summary["win_count"] += 1
    summary["incremental_net_bps_values"].append(incremental_net_bps)
    summary["max_adverse_bps_values"].append(max_adverse_bps)


def _finalize_exit_shadow_summaries(report: JournalAnalysisReport) -> None:
    for summary in report.exit_shadow_by_bot.values():
        sample_count = int(summary.get("sample_count", 0) or 0)
        incremental = list(summary.get("incremental_net_bps_values", []) or [])
        adverse = list(summary.get("max_adverse_bps_values", []) or [])
        if sample_count > 0:
            summary["direction_accuracy"] = (
                float(summary.get("direction_correct_count", 0) or 0) / sample_count
            )
            summary["win_rate"] = float(summary.get("win_count", 0) or 0) / sample_count
        if incremental:
            summary["avg_incremental_net_bps"] = sum(incremental) / len(incremental)
            summary["median_incremental_net_bps"] = float(median(incremental))
        if adverse:
            summary["max_adverse_bps"] = max(adverse)
        summary.pop("incremental_net_bps_values", None)
        summary.pop("max_adverse_bps_values", None)


def _entry_time_info(records: list[dict]) -> dict[str, dict]:
    info_by_position: dict[str, dict] = {}

    def info_for(position_id: str) -> dict:
        return info_by_position.setdefault(
            position_id,
            {
                "position_id": position_id,
                "symbol": "",
                "entered_at_ms": 0,
                "opened_at_ms": 0,
                "opened_payload_at_ms": 0,
                "opened_event_ts_ms": 0,
                "opened_quality": "",
                "started_at_ms": 0,
                "started_kind": "",
                "started_priority": 0,
            },
        )

    for record in records:
        kind = str(record.get("kind", "") or "")
        payload = _event_payload(record)
        position_id = _entry_position_id(payload)
        if not position_id:
            continue
        ts_ms = _event_ts_ms(record)
        info = info_for(position_id)
        symbol = str(payload.get("symbol", "") or "")
        if symbol and not info["symbol"]:
            info["symbol"] = symbol

        if kind == "entry.opened":
            entered_at_ms = _positive_payload_ts(payload, "entered_at_ms")
            opened_payload_at_ms = _positive_payload_ts(payload, "opened_at_ms")
            opened_at_ms = opened_payload_at_ms or ts_ms
            if entered_at_ms and (
                not info["entered_at_ms"] or entered_at_ms < info["entered_at_ms"]
            ):
                info["entered_at_ms"] = entered_at_ms
            if opened_payload_at_ms and (
                not info["opened_payload_at_ms"]
                or opened_payload_at_ms < info["opened_payload_at_ms"]
            ):
                info["opened_payload_at_ms"] = opened_payload_at_ms
            if opened_at_ms and (
                not info["opened_at_ms"] or opened_at_ms < info["opened_at_ms"]
            ):
                info["opened_at_ms"] = opened_at_ms
            if ts_ms and (
                not info["opened_event_ts_ms"] or ts_ms < info["opened_event_ts_ms"]
            ):
                info["opened_event_ts_ms"] = ts_ms
            quality = str(payload.get("entry_timestamp_quality", "") or "")
            if quality and not info["opened_quality"]:
                info["opened_quality"] = quality
            continue

        priority = _ENTRY_START_KIND_PRIORITY.get(kind)
        if priority is None or ts_ms <= 0:
            continue
        current_started = int(info["started_at_ms"] or 0)
        current_priority = int(info["started_priority"] or 0)
        if (
            current_started <= 0
            or ts_ms < current_started
            or (ts_ms == current_started and priority < current_priority)
        ):
            info["started_at_ms"] = ts_ms
            info["started_kind"] = kind
            info["started_priority"] = priority

    return info_by_position


def _select_quick_flat_entry_time(info: dict, terminal_ts_ms: int) -> tuple[int, str, str]:
    entered_at_ms = int(info.get("entered_at_ms") or 0)
    entered_quality = str(info.get("opened_quality") or "exchange_fill_exact")
    if entered_at_ms > 0 and entered_quality != "finalization_fallback":
        return (
            entered_at_ms,
            "entry.opened.entered_at_ms",
            entered_quality,
        )

    started_at_ms = int(info.get("started_at_ms") or 0)
    opened_at_ms = int(info.get("opened_at_ms") or 0)
    opened_event_ts_ms = int(info.get("opened_event_ts_ms") or 0)
    if started_at_ms > 0:
        opened_reference = entered_at_ms or opened_at_ms or opened_event_ts_ms
        if (
            terminal_ts_ms > 0
            and opened_reference > 0
            and started_at_ms < opened_reference
        ):
            return (
                started_at_ms,
                str(info.get("started_kind") or "entry_lifecycle_start"),
                "lifecycle_start",
            )

    if entered_at_ms > 0:
        return (
            entered_at_ms,
            "entry.opened.entered_at_ms",
            entered_quality,
        )

    opened_payload_at_ms = int(info.get("opened_payload_at_ms") or 0)
    if opened_at_ms > 0:
        if opened_payload_at_ms > 0:
            return (
                opened_at_ms,
                "entry.opened.opened_at_ms",
                str(info.get("opened_quality") or "opened_at_ms"),
            )
        return (
            opened_at_ms,
            "entry.opened.event_ts_ms",
            "legacy_event_time",
        )
    if opened_event_ts_ms > 0:
        return (
            opened_event_ts_ms,
            "entry.opened.event_ts_ms",
            "legacy_event_time",
        )
    if started_at_ms > 0:
        return (
            started_at_ms,
            str(info.get("started_kind") or "entry_lifecycle_start"),
            "lifecycle_start",
        )
    return (0, "", "")


def summarize_quick_flat_events(
    records: list[dict],
    *,
    quick_flat_window_ms: int = 60_000,
) -> dict[str, int | str | list[dict[str, int | str]]]:
    """Summarize quick-flat terminal observations without double-counting exits."""
    entry_times = _entry_time_info(records)

    seen_exit_keys: set[tuple] = set()
    seen_entry_overhedge_drift_keys: set[tuple] = set()
    quick_flat_positions: dict[str, dict] = {}
    entry_overhedge_drift_positions: dict[str, dict] = {}
    resolved_positions: dict[str, dict] = {}
    duplicate_event_count = 0
    low_confidence_event_count = 0
    window_ms = int(quick_flat_window_ms or 0)

    for record in records:
        kind = str(record.get("kind", "") or "")
        if kind in _ENTRY_OVERHEDGE_DRIFT_CORRECTION_KINDS:
            payload = _event_payload(record)
            position_id = str(payload.get("position_id", "") or "")
            if not position_id:
                continue
            key = quick_flat_event_key(record)
            if key in seen_entry_overhedge_drift_keys:
                continue
            seen_entry_overhedge_drift_keys.add(key)
            entry_info = entry_times.get(position_id)
            if not entry_info:
                continue
            corrected_at_ms = _event_ts_ms(record)
            opened_at_ms, time_source, timestamp_quality = _select_quick_flat_entry_time(
                entry_info,
                corrected_at_ms,
            )
            if opened_at_ms <= 0:
                continue
            elapsed_ms = corrected_at_ms - opened_at_ms
            terminal = {
                "entry_started_ts_ms": int(entry_info.get("started_at_ms") or opened_at_ms),
                "entry_entered_ts_ms": int(entry_info.get("entered_at_ms") or 0),
                "entry_opened_ts_ms": int(entry_info.get("opened_at_ms") or opened_at_ms),
                "entry_opened_event_ts_ms": int(
                    entry_info.get("opened_event_ts_ms")
                    or entry_info.get("opened_at_ms")
                    or opened_at_ms
                ),
                "kind": kind,
                "position_id": position_id,
                "symbol": str(payload.get("symbol") or entry_info.get("symbol") or ""),
                "ts_ms": corrected_at_ms,
                "elapsed_ms": elapsed_ms,
                "time_source": time_source,
                "timestamp_quality": timestamp_quality,
            }
            previous = entry_overhedge_drift_positions.get(position_id)
            if previous is None or corrected_at_ms >= int(previous["ts_ms"]):
                entry_overhedge_drift_positions[position_id] = terminal
            continue

        if kind not in _QUICK_FLAT_TERMINAL_KIND_PRIORITY:
            continue
        payload = _event_payload(record)
        position_id = str(payload.get("position_id", "") or "")
        if not position_id:
            continue

        key = quick_flat_event_key(record)
        has_close_id = bool(payload.get("close_id"))
        if kind == "exit.closed" and not has_close_id:
            low_confidence_event_count += 1
        if key in seen_exit_keys:
            duplicate_event_count += 1
            continue
        seen_exit_keys.add(key)

        entry_info = entry_times.get(position_id)
        if not entry_info:
            continue
        closed_at_ms = _event_ts_ms(record)
        opened_at_ms, time_source, timestamp_quality = _select_quick_flat_entry_time(
            entry_info,
            closed_at_ms,
        )
        if opened_at_ms <= 0:
            continue
        elapsed_ms = closed_at_ms - opened_at_ms
        terminal = {
            "entry_started_ts_ms": int(entry_info.get("started_at_ms") or opened_at_ms),
            "entry_entered_ts_ms": int(entry_info.get("entered_at_ms") or 0),
            "entry_opened_ts_ms": int(entry_info.get("opened_at_ms") or opened_at_ms),
            "entry_opened_event_ts_ms": int(
                entry_info.get("opened_event_ts_ms") or entry_info.get("opened_at_ms") or opened_at_ms
            ),
            "kind": kind,
            "position_id": position_id,
            "symbol": str(payload.get("symbol") or entry_info.get("symbol") or ""),
            "ts_ms": closed_at_ms,
            "elapsed_ms": elapsed_ms,
            "time_source": time_source,
            "timestamp_quality": timestamp_quality,
        }
        if 0 <= elapsed_ms <= window_ms:
            previous = quick_flat_positions.get(position_id)
            priority = _QUICK_FLAT_TERMINAL_KIND_PRIORITY[kind]
            if previous is None or (
                priority,
                closed_at_ms,
            ) >= (
                int(previous["priority"]),
                int(previous["ts_ms"]),
            ):
                terminal["priority"] = priority
                quick_flat_positions[position_id] = terminal
            continue
        opened_reference = int(
            entry_info.get("opened_at_ms")
            or entry_info.get("opened_event_ts_ms")
            or 0
        )
        if (
            opened_reference > 0
            and 0 <= closed_at_ms - opened_reference <= window_ms
        ):
            previous = resolved_positions.get(position_id)
            priority = _QUICK_FLAT_TERMINAL_KIND_PRIORITY[kind]
            if previous is None or (
                priority,
                closed_at_ms,
            ) >= (
                int(previous["priority"]),
                int(previous["ts_ms"]),
            ):
                terminal["priority"] = priority
                resolved_positions[position_id] = terminal

    terminal_kind_counts: dict[str, int] = {}
    for terminal in quick_flat_positions.values():
        kind = str(terminal["kind"])
        terminal_kind_counts[kind] = terminal_kind_counts.get(kind, 0) + 1

    close_identity_confidence = "lower" if low_confidence_event_count else "high"
    return {
        "quick_flat_count": len(quick_flat_positions),
        "terminal_quick_flat_count": len(quick_flat_positions),
        "entry_overhedge_drift_corrected_count": len(
            entry_overhedge_drift_positions
        ),
        "duplicate_event_count": duplicate_event_count,
        "quick_flat_duplicate_event_count": duplicate_event_count,
        "resolved_long_pending_fast_close_count": len(resolved_positions),
        "low_confidence_event_count": low_confidence_event_count,
        "close_identity_confidence": close_identity_confidence,
        "quick_flat_terminal_kind_counts": terminal_kind_counts,
        "samples": [
            {
                "position_id": str(terminal.get("position_id") or ""),
                "symbol": str(terminal.get("symbol") or ""),
                "entry_opened_ts_ms": int(terminal.get("entry_opened_ts_ms") or 0),
                "entry_started_ts_ms": int(terminal.get("entry_started_ts_ms") or 0),
                "entry_entered_ts_ms": int(terminal.get("entry_entered_ts_ms") or 0),
                "entry_opened_event_ts_ms": int(
                    terminal.get("entry_opened_event_ts_ms") or 0
                ),
                "terminal_ts_ms": int(terminal.get("ts_ms") or 0),
                "terminal_kind": str(terminal.get("kind") or ""),
                "elapsed_ms": int(terminal.get("elapsed_ms") or 0),
                "time_source": str(terminal.get("time_source") or ""),
                "timestamp_quality": str(terminal.get("timestamp_quality") or ""),
            }
            for terminal in sorted(
                quick_flat_positions.values(),
                key=lambda item: (
                    int(item.get("ts_ms") or 0),
                    str(item.get("position_id") or ""),
                ),
            )[:12]
        ],
        "entry_overhedge_drift_corrected_samples": [
            {
                "position_id": str(terminal.get("position_id") or ""),
                "symbol": str(terminal.get("symbol") or ""),
                "entry_opened_ts_ms": int(terminal.get("entry_opened_ts_ms") or 0),
                "entry_started_ts_ms": int(terminal.get("entry_started_ts_ms") or 0),
                "entry_entered_ts_ms": int(terminal.get("entry_entered_ts_ms") or 0),
                "entry_opened_event_ts_ms": int(
                    terminal.get("entry_opened_event_ts_ms") or 0
                ),
                "terminal_ts_ms": int(terminal.get("ts_ms") or 0),
                "terminal_kind": str(terminal.get("kind") or ""),
                "elapsed_ms": int(terminal.get("elapsed_ms") or 0),
                "time_source": str(terminal.get("time_source") or ""),
                "timestamp_quality": str(terminal.get("timestamp_quality") or ""),
            }
            for terminal in sorted(
                entry_overhedge_drift_positions.values(),
                key=lambda item: (
                    int(item.get("ts_ms") or 0),
                    str(item.get("position_id") or ""),
                ),
            )[:12]
        ],
        "resolved_samples": [
            {
                "position_id": str(terminal.get("position_id") or ""),
                "symbol": str(terminal.get("symbol") or ""),
                "entry_started_ts_ms": int(terminal.get("entry_started_ts_ms") or 0),
                "entry_entered_ts_ms": int(terminal.get("entry_entered_ts_ms") or 0),
                "entry_opened_ts_ms": int(terminal.get("entry_opened_ts_ms") or 0),
                "entry_opened_event_ts_ms": int(
                    terminal.get("entry_opened_event_ts_ms") or 0
                ),
                "terminal_ts_ms": int(terminal.get("ts_ms") or 0),
                "terminal_kind": str(terminal.get("kind") or ""),
                "elapsed_ms": int(terminal.get("elapsed_ms") or 0),
                "time_source": str(terminal.get("time_source") or ""),
                "timestamp_quality": str(terminal.get("timestamp_quality") or ""),
            }
            for terminal in sorted(
                resolved_positions.values(),
                key=lambda item: (
                    int(item.get("ts_ms") or 0),
                    str(item.get("position_id") or ""),
                ),
            )[:12]
        ],
    }


def analyze_journal_records(
    records: list[dict],
) -> JournalAnalysisReport:
    """Analyze journal records for order stats, PnL, and diagnostics.

    Covers all V1 record-layer event kinds processed by offline analysis:
    entry/exit/order lifecycle, recovery evidence, risk triggers, scan
    diagnostics, execution diagnostics, and local-L2 health events.
    """
    report = JournalAnalysisReport(total_records=len(records))

    for record in records:
        kind: str = record.get("kind", "")
        payload: dict = record.get("payload", {})

        if kind == "entry.opened":
            report.daily.entry_count += 1
            report.daily.total_fee_quote += payload.get("entry_fee_quote", 0.0)

        elif kind == "exit.closed":
            report.daily.exit_count += 1
            report.daily.total_pnl_quote += payload.get("net_quote", 0.0)
            report.daily.total_fee_quote += payload.get("exit_fee_quote", 0.0)

        elif kind == "order.submitted":
            _record_order(report, payload, filled=False, failed=False)

        elif kind == "order.filled":
            _record_order(report, payload, filled=True, failed=False)

        elif kind == "order.rejected":
            _record_order(report, payload, filled=False, failed=True)

        elif kind == "order.uncertain":
            _record_order(report, payload, filled=False, failed=True)

        elif kind in _RECOVERY_KINDS:
            report.recovery_counts[kind] = report.recovery_counts.get(kind, 0) + 1

        elif kind in _RISK_KINDS:
            report.risk_counts[kind] = report.risk_counts.get(kind, 0) + 1

        elif kind == _SCAN_NO_ENTRY:
            report.scan_no_entry_diagnostics_count += 1

        elif kind == _SCAN_GATE_BLOCKED:
            report.scan_runtime_gate_blocked_count += 1

        elif kind == _EXEC_LIQUIDITY_BLOCKED:
            report.execution_liquidity_blocked_count += 1
            reason = payload.get("reason", "unspecified")
            report.entry_liquidity_blocked_by_reason[reason] = (
                report.entry_liquidity_blocked_by_reason.get(reason, 0) + 1
            )
            oi_status = str(
                payload.get("open_interest_evidence_status") or ""
            )
            if oi_status:
                counts = report.entry_liquidity_blocked_by_open_interest_evidence_status
                counts[oi_status] = counts.get(oi_status, 0) + 1
            eligibility = payload.get("eligibility_class", "")
            if eligibility:
                report.execution_liquidity_blocked_by_class[eligibility] = (
                    report.execution_liquidity_blocked_by_class.get(eligibility, 0) + 1
                )

        elif kind == _LOCAL_L2_SEQUENCE_GAP:
            report.local_l2_sequence_gap_count += 1
            reason = payload.get("continuity_reason", "unspecified")
            report.local_l2_sequence_gap_by_reason[reason] = (
                report.local_l2_sequence_gap_by_reason.get(reason, 0) + 1
            )

        elif kind == _LOCAL_L2_SYNC_FAILED:
            report.local_l2_sync_failed_count += 1
            category = payload.get("failure_category", "unspecified")
            report.local_l2_sync_failed_by_category[category] = (
                report.local_l2_sync_failed_by_category.get(category, 0) + 1
            )

        elif kind.startswith("runtime.fail_closed"):
            reason = payload.get("reason", "unspecified")
            report.fail_closed_reason_counts[reason] = (
                report.fail_closed_reason_counts.get(reason, 0) + 1
            )

        elif kind == "opportunity.paper_markout":
            report.paper_outcome_markout_count += 1
            label = payload.get("opportunity_label", "unknown")
            report.paper_outcome_by_label[label] = (
                report.paper_outcome_by_label.get(label, 0) + 1
            )

        elif kind == "opportunity.paper_closed":
            report.paper_outcome_closed_count += 1
            label = payload.get("opportunity_label", "unknown")
            report.paper_outcome_by_label[label] = (
                report.paper_outcome_by_label.get(label, 0) + 1
            )

        elif kind == "opportunity.real_vs_paper_joined":
            report.paper_outcome_joined_count += 1
            label = payload.get("opportunity_label", "unknown")
            report.paper_outcome_by_label[label] = (
                report.paper_outcome_by_label.get(label, 0) + 1
            )

        elif kind == "exit_shadow.strategy_decision":
            report.exit_shadow_decision_count += 1

        elif kind == "exit_shadow.path_markout":
            report.exit_shadow_path_markout_count += 1

        elif kind == "exit_shadow.strategy_summary":
            report.exit_shadow_summary_count += 1
            _record_exit_shadow_summary(report, payload)

    quick_flat_summary = summarize_quick_flat_events(records)
    report.quick_flat_count = int(quick_flat_summary["quick_flat_count"])
    report.quick_flat_duplicate_event_count = int(
        quick_flat_summary["duplicate_event_count"]
    )
    report.quick_flat_low_confidence_event_count = int(
        quick_flat_summary["low_confidence_event_count"]
    )
    report.quick_flat_close_identity_confidence = str(
        quick_flat_summary["close_identity_confidence"]
    )
    _finalize_exit_shadow_summaries(report)

    return report


def _record_order(
    report: JournalAnalysisReport,
    payload: dict,
    *,
    filled: bool,
    failed: bool,
) -> None:
    venue = payload.get("venue", "unknown")
    stats = report.venue_stats.setdefault(venue, VenueOrderStats(venue=venue))
    if filled:
        stats.fill_count += 1
        latency = payload.get("latency_ms", 0)
        stats.total_latency_ms += latency
        stats.max_latency_ms = max(stats.max_latency_ms, latency)
        stats.min_latency_ms = min(stats.min_latency_ms, latency)
        stats.total_fee_quote += payload.get("fee_quote", 0.0)
    elif failed:
        stats.failure_count += 1
    else:
        stats.order_count += 1


# ---------------------------------------------------------------------------
# Structured store read path (preferred for analytical consumers)
# ---------------------------------------------------------------------------

def analyze_from_store(conn: sqlite3.Connection) -> JournalAnalysisReport:
    """Build a JournalAnalysisReport from projection fact tables.

    This is the fast path — it queries normalized SQLite tables instead of
    scanning raw JSONL. Requires prior projection (see projection_writer.py).
    Does NOT include journal-only evidence (recovery, lifecycle) since those
    are intentionally not projected.
    """
    report = JournalAnalysisReport()

    # Entry / exit facts
    for row in conn.execute(
        "SELECT kind, symbol, entry_fee_quote, exit_fee_quote, net_quote "
        "FROM entry_exit_facts ORDER BY seq"
    ):
        r = dict(row)
        kind = r["kind"]
        if kind == "entry.opened":
            report.daily.entry_count += 1
            report.daily.total_fee_quote += r["entry_fee_quote"]
        elif kind == "exit.closed":
            report.daily.exit_count += 1
            report.daily.total_pnl_quote += r["net_quote"]
            report.daily.total_fee_quote += r["exit_fee_quote"]

    # Order facts
    for row in conn.execute(
        "SELECT kind, venue, symbol, filled, failed, latency_ms, fee_quote "
        "FROM order_facts ORDER BY seq"
    ):
        r = dict(row)
        venue = r["venue"]
        stats = report.venue_stats.setdefault(venue, VenueOrderStats(venue=venue))
        if r["filled"]:
            stats.fill_count += 1
            lat = r["latency_ms"]
            stats.total_latency_ms += lat
            stats.max_latency_ms = max(stats.max_latency_ms, lat)
            stats.min_latency_ms = min(stats.min_latency_ms, lat)
            stats.total_fee_quote += r["fee_quote"]
        elif r["failed"]:
            stats.failure_count += 1
        else:
            stats.order_count += 1

    # Risk counter facts
    for row in conn.execute(
        "SELECT kind, counter_value FROM risk_counter_facts ORDER BY seq"
    ):
        r = dict(row)
        kind = r["kind"]
        report.risk_counts[kind] = report.risk_counts.get(kind, 0) + r["counter_value"]

    # Local-L2 health facts
    for row in conn.execute(
        "SELECT kind, reason, category, venue FROM local_l2_health_facts ORDER BY seq"
    ):
        r = dict(row)
        kind = r["kind"]
        if kind == "runtime.local_l2_sequence_gap":
            report.local_l2_sequence_gap_count += 1
            report.local_l2_sequence_gap_by_reason[r["reason"]] = (
                report.local_l2_sequence_gap_by_reason.get(r["reason"], 0) + 1
            )
        elif kind == "runtime.local_l2_sync_failed":
            report.local_l2_sync_failed_count += 1
            report.local_l2_sync_failed_by_category[r["category"]] = (
                report.local_l2_sync_failed_by_category.get(r["category"], 0) + 1
            )

    # Diagnostic facts (scan, execution, fail-closed, exit-shadow)
    for row in conn.execute(
        "SELECT kind, reason, classification, payload_json FROM diagnostic_facts ORDER BY seq"
    ):
        r = dict(row)
        kind = r["kind"]
        if kind == "scan.no_entry_diagnostics":
            report.scan_no_entry_diagnostics_count += 1
        elif kind == "scan.runtime_gate_blocked":
            report.scan_runtime_gate_blocked_count += 1
        elif kind == "execution.entry_liquidity_blocked":
            report.execution_liquidity_blocked_count += 1
            reason = r["reason"]
            report.entry_liquidity_blocked_by_reason[reason] = (
                report.entry_liquidity_blocked_by_reason.get(reason, 0) + 1
            )
            cls = r["classification"]
            if cls:
                report.execution_liquidity_blocked_by_class[cls] = (
                    report.execution_liquidity_blocked_by_class.get(cls, 0) + 1
                )
        elif kind.startswith("runtime.fail_closed"):
            reason = r["reason"]
            report.fail_closed_reason_counts[reason] = (
                report.fail_closed_reason_counts.get(reason, 0) + 1
            )
        elif kind == "exit_shadow.strategy_decision":
            report.exit_shadow_decision_count += 1
        elif kind == "exit_shadow.path_markout":
            report.exit_shadow_path_markout_count += 1
        elif kind == "exit_shadow.strategy_summary":
            report.exit_shadow_summary_count += 1
            try:
                payload = json.loads(r["payload_json"]) if r["payload_json"] else {}
            except (TypeError, ValueError):
                payload = {}
            _record_exit_shadow_summary(report, payload)
        elif kind in ("opportunity.paper_markout", "opportunity.paper_closed",
                      "opportunity.real_vs_paper_joined"):
            import json as _json
            try:
                pl = _json.loads(row["payload_json"]) if row["payload_json"] else {}
            except Exception:
                pl = {}
            label = pl.get("opportunity_label", "unknown")
            report.paper_outcome_by_label[label] = (
                report.paper_outcome_by_label.get(label, 0) + 1
            )
            if kind == "opportunity.paper_markout":
                report.paper_outcome_markout_count += 1
            elif kind == "opportunity.paper_closed":
                report.paper_outcome_closed_count += 1
            elif kind == "opportunity.real_vs_paper_joined":
                report.paper_outcome_joined_count += 1

    # Total records = sum of all projected facts (approximate but sufficient for reporting)
    total = 0
    for table in ["order_facts", "entry_exit_facts", "risk_counter_facts",
                   "local_l2_health_facts", "diagnostic_facts"]:
        row = conn.execute(f"SELECT COUNT(*) as cnt FROM {table}").fetchone()
        total += row["cnt"] if row else 0
    report.total_records = total

    _finalize_exit_shadow_summaries(report)

    return report


def analyze_journal_or_store(
    conn: sqlite3.Connection | None,
    records: list[dict] | None = None,
) -> JournalAnalysisReport:
    """Store-first analysis with journal fallback.

    If the store has projection data, query it directly.
    Otherwise fall back to journal record scan.
    Recovery/lifecycle evidence is only available via journal scan.
    """
    if conn is not None:
        try:
            row = conn.execute(
                "SELECT total_facts_written FROM projection_cursor WHERE id = 1"
            ).fetchone()
            if row and row["total_facts_written"] > 0:
                return analyze_from_store(conn)
        except Exception:
            pass  # Fall through to journal scan

    if records is not None:
        return analyze_journal_records(records)

    return JournalAnalysisReport()
