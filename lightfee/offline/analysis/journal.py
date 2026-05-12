"""Journal analysis: venue stats, failure rates, latency, PnL, diagnostics."""

from __future__ import annotations

from dataclasses import dataclass, field


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

    # Execution diagnostic classifications (V1: entry_liquidity_blocked_by_eligibility_class)
    execution_liquidity_blocked_by_class: dict[str, int] = field(default_factory=dict)

    # Fail-closed reason counts (V1: fail_closed_reason_counts)
    fail_closed_reason_counts: dict[str, int] = field(default_factory=dict)


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
