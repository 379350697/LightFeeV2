"""Persistence metrics and diagnostics matching Rust V1 JournalRuntimeMetrics.

Rust references:
- src/observability_ops/journal_bridge.rs (JournalRuntimeMetrics, JournalRuntimeMetricsSnapshot)
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class PersistenceMetrics:
    """Journal and persistence quality metrics matching Rust V1 JournalRuntimeMetrics.

    V1 has 19 counters split into:
    - Journal quality: async_appends, critical_appends, sync_fallback_appends,
      dropped_async_appends, flush_requests, writer_flushes, writer_failures,
      queue_disconnects
    - Runtime health: open_position_count, global_risk_mode_code,
      net_exposure_milli_quote, venue_health_{normal,pause_entry,reduce_only,fail_closed}_count
    - Risk triggers: risk_{warning,delever,death}_trigger_count
    - Event counters: order_timeout_count, ws_disconnect_count, rest_failure_count,
      reconcile_drift_count
    """

    # Journal quality counters
    journal_appends: int = 0
    async_appends: int = 0
    critical_appends: int = 0
    sync_fallback_appends: int = 0
    dropped_async_appends: int = 0
    journal_flushes: int = 0
    writer_flushes: int = 0
    writer_failures: int = 0
    queue_disconnects: int = 0

    # Snapshot / SQLite counters
    snapshot_writes: int = 0
    snapshot_reads: int = 0
    sqlite_writes: int = 0

    # Projection counters
    projection_appends: int = 0
    projection_skips: int = 0
    projection_failures: int = 0
    last_projection_seq: int = 0
    last_projection_at_ms: int = 0

    # Timestamps
    last_journal_append_ms: int = 0
    last_snapshot_write_ms: int = 0

    # Runtime health counters
    open_position_count: int = 0
    global_risk_mode: str = "running"
    net_exposure_milli_quote: int = 0
    venue_health_normal_count: int = 0
    venue_health_pause_entry_count: int = 0
    venue_health_reduce_only_count: int = 0
    venue_health_fail_closed_count: int = 0

    # Risk trigger counters
    risk_warning_trigger_count: int = 0
    risk_delever_trigger_count: int = 0
    risk_death_trigger_count: int = 0

    # Event counters
    order_timeout_count: int = 0
    ws_disconnect_count: int = 0
    rest_failure_count: int = 0
    reconcile_drift_count: int = 0

    # Errors
    errors: list[str] = field(default_factory=list)

    # -------------- Journal quality methods --------------

    def record_journal_append(self, ts_ms: int, *, critical: bool = False) -> None:
        self.journal_appends += 1
        self.last_journal_append_ms = ts_ms
        if critical:
            self.critical_appends += 1
        else:
            self.async_appends += 1

    def record_sync_fallback_append(self) -> None:
        self.sync_fallback_appends += 1

    def record_dropped_async_append(self) -> None:
        self.dropped_async_appends += 1

    def record_journal_flush(self) -> None:
        self.journal_flushes += 1

    @property
    def flush_requests(self) -> int:
        return self.journal_flushes

    def record_writer_flush(self) -> None:
        self.writer_flushes += 1

    def record_writer_failure(self) -> None:
        self.writer_failures += 1

    def record_queue_disconnect(self) -> None:
        self.queue_disconnects += 1

    # -------------- Snapshot / SQLite methods --------------

    def record_snapshot_write(self, ts_ms: int) -> None:
        self.snapshot_writes += 1
        self.last_snapshot_write_ms = ts_ms

    def record_snapshot_read(self) -> None:
        self.snapshot_reads += 1

    # -------------- Projection methods --------------

    def record_projection_append(self, seq: int, ts_ms: int) -> None:
        self.projection_appends += 1
        self.sqlite_writes += 1
        self.last_projection_seq = seq
        self.last_projection_at_ms = ts_ms

    def record_projection_skip(self) -> None:
        self.projection_skips += 1

    def record_projection_failure(self) -> None:
        self.projection_failures += 1

    @property
    def projection_lag(self) -> int:
        """How many journal appends behind the projection cursor is."""
        return max(0, self.journal_appends - self.last_projection_seq)

    # -------------- Runtime health methods --------------

    def set_runtime_health(
        self,
        *,
        open_position_count: int | None = None,
        global_risk_mode: str | None = None,
        net_exposure_milli_quote: int | None = None,
        venue_health_normal_count: int | None = None,
        venue_health_pause_entry_count: int | None = None,
        venue_health_reduce_only_count: int | None = None,
        venue_health_fail_closed_count: int | None = None,
    ) -> None:
        """Update runtime health counters (Rust V1: set_runtime_health_metrics)."""
        if open_position_count is not None:
            self.open_position_count = open_position_count
        if global_risk_mode is not None:
            self.global_risk_mode = global_risk_mode
        if net_exposure_milli_quote is not None:
            self.net_exposure_milli_quote = net_exposure_milli_quote
        if venue_health_normal_count is not None:
            self.venue_health_normal_count = venue_health_normal_count
        if venue_health_pause_entry_count is not None:
            self.venue_health_pause_entry_count = venue_health_pause_entry_count
        if venue_health_reduce_only_count is not None:
            self.venue_health_reduce_only_count = venue_health_reduce_only_count
        if venue_health_fail_closed_count is not None:
            self.venue_health_fail_closed_count = venue_health_fail_closed_count

    # -------------- Risk trigger methods --------------

    def record_risk_warning_trigger(self) -> None:
        self.risk_warning_trigger_count += 1

    def record_risk_delever_trigger(self) -> None:
        self.risk_delever_trigger_count += 1

    def record_risk_death_trigger(self) -> None:
        self.risk_death_trigger_count += 1

    # -------------- Event counter methods --------------

    def record_order_timeout(self) -> None:
        self.order_timeout_count += 1

    def record_ws_disconnect(self) -> None:
        self.ws_disconnect_count += 1

    def record_rest_failure(self) -> None:
        self.rest_failure_count += 1

    def record_reconcile_drift(self) -> None:
        self.reconcile_drift_count += 1

    # -------------- Error tracking --------------

    def record_error(self, error: str) -> None:
        self.errors.append(error)
