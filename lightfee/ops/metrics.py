"""Journal runtime metrics snapshot and V1-prometheus metric building.

V1 anchors: src/observability_ops/journal_bridge.rs (JournalRuntimeMetrics,
JournalRuntimeMetricsSnapshot), src/app_runtime/loop_control.rs
(build_prometheus_metric_samples, build_prometheus_metric_samples_from_parts)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from lightfee.engine.state import EngineState
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


@dataclass
class JournalRuntimeMetricsSnapshot:
    """Mirrors Rust V1 JournalRuntimeMetricsSnapshot — all operator-visible counters.

    These counters are incremented by the journal writer, risk evaluator, venue
    adapters, and reconciliation loop. A zero value means "none since last export".
    """

    async_appends: int = 0
    critical_appends: int = 0
    sync_fallback_appends: int = 0
    dropped_async_appends: int = 0
    flush_requests: int = 0
    writer_flushes: int = 0
    writer_failures: int = 0
    queue_disconnects: int = 0
    open_position_count: int = 0
    global_risk_mode: str = "running"
    net_exposure_milli_quote: int = 0
    venue_health_normal_count: int = 0
    venue_health_pause_entry_count: int = 0
    venue_health_reduce_only_count: int = 0
    venue_health_fail_closed_count: int = 0
    risk_warning_trigger_count: int = 0
    risk_delever_trigger_count: int = 0
    risk_death_trigger_count: int = 0
    order_timeout_count: int = 0
    ws_disconnect_count: int = 0
    rest_failure_count: int = 0
    reconcile_drift_count: int = 0

    def to_dict(self) -> dict:
        return {
            "async_appends": self.async_appends,
            "critical_appends": self.critical_appends,
            "sync_fallback_appends": self.sync_fallback_appends,
            "dropped_async_appends": self.dropped_async_appends,
            "flush_requests": self.flush_requests,
            "writer_flushes": self.writer_flushes,
            "writer_failures": self.writer_failures,
            "queue_disconnects": self.queue_disconnects,
            "open_position_count": self.open_position_count,
            "global_risk_mode": self.global_risk_mode,
            "net_exposure_milli_quote": self.net_exposure_milli_quote,
            "venue_health_normal_count": self.venue_health_normal_count,
            "venue_health_pause_entry_count": self.venue_health_pause_entry_count,
            "venue_health_reduce_only_count": self.venue_health_reduce_only_count,
            "venue_health_fail_closed_count": self.venue_health_fail_closed_count,
            "risk_warning_trigger_count": self.risk_warning_trigger_count,
            "risk_delever_trigger_count": self.risk_delever_trigger_count,
            "risk_death_trigger_count": self.risk_death_trigger_count,
            "order_timeout_count": self.order_timeout_count,
            "ws_disconnect_count": self.ws_disconnect_count,
            "rest_failure_count": self.rest_failure_count,
            "reconcile_drift_count": self.reconcile_drift_count,
        }


@dataclass
class RuntimeHealthMetricsUpdate:
    """V1: per-tick health snapshot pushed into journal metrics."""

    open_position_count: int = 0
    global_risk_mode_code: int = 0
    net_exposure_milli_quote: int = 0
    venue_health_normal_count: int = 0
    venue_health_pause_entry_count: int = 0
    venue_health_reduce_only_count: int = 0
    venue_health_fail_closed_count: int = 0


def build_prometheus_metric_samples(
    state: EngineState,
    journal_metrics: Optional[JournalRuntimeMetricsSnapshot] = None,
) -> list[str]:
    """Build Prometheus textfile metric lines matching V1 output.

    V1: build_prometheus_metric_samples() + build_prometheus_metric_samples_from_parts().
    Includes journal diagnostics, risk counters, venue health breakdown,
    connection counters, position groups, lifecycle, and risk mode.
    """
    if journal_metrics is None:
        journal_metrics = JournalRuntimeMetricsSnapshot()

    samples: list[str] = []

    # --- Journal diagnostics (V1: journal_bridge.rs metrics) ---
    samples.extend([
        "# HELP lightfee_journal_async_appends_total Asynchronous journal appends accepted.",
        "# TYPE lightfee_journal_async_appends_total counter",
        f"lightfee_journal_async_appends_total {journal_metrics.async_appends}",
        "# HELP lightfee_journal_critical_appends_total Critical journal appends (fsync).",
        "# TYPE lightfee_journal_critical_appends_total counter",
        f"lightfee_journal_critical_appends_total {journal_metrics.critical_appends}",
        "# HELP lightfee_journal_sync_fallback_appends_total Journal appends that fell back to sync writer.",
        "# TYPE lightfee_journal_sync_fallback_appends_total counter",
        f"lightfee_journal_sync_fallback_appends_total {journal_metrics.sync_fallback_appends}",
        "# HELP lightfee_journal_dropped_async_appends_total Journal appends dropped (queue saturated).",
        "# TYPE lightfee_journal_dropped_async_appends_total counter",
        f"lightfee_journal_dropped_async_appends_total {journal_metrics.dropped_async_appends}",
        "# HELP lightfee_journal_flush_requests_total Flush requests received.",
        "# TYPE lightfee_journal_flush_requests_total counter",
        f"lightfee_journal_flush_requests_total {journal_metrics.flush_requests}",
        "# HELP lightfee_journal_writer_flushes_total Writer-side fsync calls.",
        "# TYPE lightfee_journal_writer_flushes_total counter",
        f"lightfee_journal_writer_flushes_total {journal_metrics.writer_flushes}",
        "# HELP lightfee_journal_writer_failures_total Writer I/O failures.",
        "# TYPE lightfee_journal_writer_failures_total counter",
        f"lightfee_journal_writer_failures_total {journal_metrics.writer_failures}",
        "# HELP lightfee_journal_queue_disconnects_total Writer queue disconnect events.",
        "# TYPE lightfee_journal_queue_disconnects_total counter",
        f"lightfee_journal_queue_disconnects_total {journal_metrics.queue_disconnects}",
    ])

    # --- Runtime health (V1: risk triggers, timeouts, connection failures) ---
    samples.extend([
        "# HELP lightfee_runtime_risk_warning_trigger_total Warning-line risk triggers.",
        "# TYPE lightfee_runtime_risk_warning_trigger_total counter",
        f"lightfee_runtime_risk_warning_trigger_total {journal_metrics.risk_warning_trigger_count}",
        "# HELP lightfee_runtime_risk_delever_trigger_total Auto-delever risk triggers.",
        "# TYPE lightfee_runtime_risk_delever_trigger_total counter",
        f"lightfee_runtime_risk_delever_trigger_total {journal_metrics.risk_delever_trigger_count}",
        "# HELP lightfee_runtime_risk_death_trigger_total Death-line risk triggers.",
        "# TYPE lightfee_runtime_risk_death_trigger_total counter",
        f"lightfee_runtime_risk_death_trigger_total {journal_metrics.risk_death_trigger_count}",
        "# HELP lightfee_runtime_order_timeout_total Order timeout events.",
        "# TYPE lightfee_runtime_order_timeout_total counter",
        f"lightfee_runtime_order_timeout_total {journal_metrics.order_timeout_count}",
        "# HELP lightfee_runtime_ws_disconnect_total WebSocket disconnect events.",
        "# TYPE lightfee_runtime_ws_disconnect_total counter",
        f"lightfee_runtime_ws_disconnect_total {journal_metrics.ws_disconnect_count}",
        "# HELP lightfee_runtime_rest_failure_total REST failure events.",
        "# TYPE lightfee_runtime_rest_failure_total counter",
        f"lightfee_runtime_rest_failure_total {journal_metrics.rest_failure_count}",
        "# HELP lightfee_runtime_reconcile_drift_total Reconciliation drift events.",
        "# TYPE lightfee_runtime_reconcile_drift_total counter",
        f"lightfee_runtime_reconcile_drift_total {journal_metrics.reconcile_drift_count}",
    ])

    # --- Venue health breakdown (V1: per-level venue health counts) ---
    samples.extend([
        "# HELP lightfee_venue_health_normal Venues in normal health.",
        "# TYPE lightfee_venue_health_normal gauge",
        f"lightfee_venue_health_normal {journal_metrics.venue_health_normal_count}",
        "# HELP lightfee_venue_health_pause_entry Venues with entry paused.",
        "# TYPE lightfee_venue_health_pause_entry gauge",
        f"lightfee_venue_health_pause_entry {journal_metrics.venue_health_pause_entry_count}",
        "# HELP lightfee_venue_health_reduce_only Venues in reduce-only.",
        "# TYPE lightfee_venue_health_reduce_only gauge",
        f"lightfee_venue_health_reduce_only {journal_metrics.venue_health_reduce_only_count}",
        "# HELP lightfee_venue_health_fail_closed Venues in fail-closed.",
        "# TYPE lightfee_venue_health_fail_closed gauge",
        f"lightfee_venue_health_fail_closed {journal_metrics.venue_health_fail_closed_count}",
    ])

    # --- Core engine gauges ---
    lifecycle_code = _lifecycle_code(state.lifecycle.value)
    risk_code = _risk_mode_code(state.risk_mode.value)

    samples.extend([
        "# HELP lightfee_tick_count Total engine ticks.",
        "# TYPE lightfee_tick_count counter",
        f"lightfee_tick_count {state.tick_count}",
        "# HELP lightfee_open_positions Current open position count.",
        "# TYPE lightfee_open_positions gauge",
        f"lightfee_open_positions {len(state.open_positions)}",
        "# HELP lightfee_lifecycle Engine lifecycle 0=booting 3=running 4=fail_closed.",
        "# TYPE lightfee_lifecycle gauge",
        f"lightfee_lifecycle{{state=\"{state.lifecycle.value}\"}} {lifecycle_code}",
        "# HELP lightfee_risk_mode Global risk mode 0=running 3=fail_closed.",
        "# TYPE lightfee_risk_mode gauge",
        f"lightfee_risk_mode{{mode=\"{state.risk_mode.value}\"}} {risk_code}",
    ])

    # --- Position groups (V1: aggregate by symbol+venue pair) ---
    position_groups: dict[tuple[str, str, str], tuple[int, float]] = {}
    for pos in state.open_positions.values():
        key = (pos.symbol, pos.long_venue.value, pos.short_venue.value)
        count, qty = position_groups.get(key, (0, 0.0))
        position_groups[key] = (count + 1, qty + pos.matched_quantity)

    for (symbol, long_v, short_v), (count, qty) in sorted(position_groups.items()):
        samples.append(
            f"lightfee_position_group_count{{symbol=\"{symbol}\","
            f"long_venue=\"{long_v}\",short_venue=\"{short_v}\"}} {count}"
        )
        samples.append(
            f"lightfee_position_group_quantity{{symbol=\"{symbol}\","
            f"long_venue=\"{long_v}\",short_venue=\"{short_v}\"}} {qty}"
        )

    # --- Per-position presence ---
    for pos_id, pos in state.open_positions.items():
        samples.append(
            f"lightfee_position{{id=\"{pos_id}\",symbol=\"{pos.symbol}\"}} 1"
        )

    return samples


def collect_runtime_health_update(state: EngineState) -> RuntimeHealthMetricsUpdate:
    """Build a RuntimeHealthMetricsUpdate from current engine state.

    V1: set_runtime_health_metrics() — called each tick to push current
    health counters into the journal metrics snapshot.
    """
    risk_code_map = {
        "running": 0,
        "entry_paused": 1,
        "reduce_only": 2,
        "fail_closed": 3,
    }

    venue_counts = _count_venue_health(state)

    return RuntimeHealthMetricsUpdate(
        open_position_count=len(state.open_positions),
        global_risk_mode_code=risk_code_map.get(state.risk_mode.value, 0),
        net_exposure_milli_quote=0,  # computed by risk evaluator
        venue_health_normal_count=venue_counts["normal"],
        venue_health_pause_entry_count=venue_counts["pause_entry"],
        venue_health_reduce_only_count=venue_counts["reduce_only"],
        venue_health_fail_closed_count=venue_counts["fail_closed"],
    )


def _count_venue_health(state: EngineState) -> dict[str, int]:
    """Count venues by health level from EngineState.venue_health."""
    counts = {"normal": 0, "pause_entry": 0, "reduce_only": 0, "fail_closed": 0}
    if not state.venue_health:
        return counts
    for health in state.venue_health.values():
        if hasattr(health, "value"):
            v = health.value
        else:
            v = str(health)
        if v in counts:
            counts[v] += 1
        elif v in ("running", "normal"):
            counts["normal"] += 1
    return counts


def _lifecycle_code(value: str) -> int:
    codes = {
        "booting": 0, "reconciling": 1, "risk_only": 2,
        "running": 3, "fail_closed": 4,
    }
    return codes.get(value, -1)


def _risk_mode_code(value: str) -> int:
    codes = {
        "running": 0, "entry_paused": 1, "reduce_only": 2, "fail_closed": 3,
    }
    return codes.get(value, -1)
