"""V1 semantic parity: current-state export and metrics export.

Contract: OPS-001 — Current State and Metrics Export
V1 anchors: src/app_runtime/loop_control.rs (metrics export, state snapshots),
            src/observability_ops/ (observability bridge)
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pytest

from lightfee.config.schema import (
    AppConfig,
    PersistenceConfig,
    RuntimeConfig,
    StrategyConfig,
)
from lightfee.engine.bootstrap import wall_clock_now_ms
from lightfee.engine.loop_control import (
    ExportState,
    _build_prometheus_metric_samples,
    _export_current_state_snapshot,
    _export_runtime_metrics,
    current_state_export_path,
    maybe_export_current_state_snapshot,
    maybe_export_runtime_metrics,
    metrics_export_path,
    write_json_atomic,
)
from lightfee.engine.state import EngineState
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


class TestCurrentStateExportV1Semantics:
    """V1: current-state snapshot preserves V1-visible fields."""

    def test_export_includes_v1_required_fields(self):
        """V1 parity: current-state export includes schema, lifecycle, risk_mode, mode,
        generated_at_ms, expires_at_ms, stale, global_risk_reason, open_positions, last_scan."""
        state = EngineState(
            lifecycle=EngineLifecycle.RUNNING,
            risk_mode=GlobalRiskMode.RUNNING,
        )
        state.run_id = "test-run-001"
        state.tick_count = 42
        state.last_tick_ms = wall_clock_now_ms()

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name

        try:
            _export_current_state_snapshot(state, path)
            with open(path) as f:
                data = json.load(f)

            # V1-required top-level fields
            required_fields = [
                "schema",
                "lifecycle",
                "risk_mode",
                "mode",
                "generated_at_ms",
                "expires_at_ms",
                "stale",
                "open_position_count",
                "open_positions",
            ]
            for field in required_fields:
                assert field in data, (
                    f"V1 parity violation: current-state export missing field '{field}'. "
                    f"Got keys: {sorted(data.keys())}"
                )

            # Schema must identify as lightfee current state
            assert "lightfee.current_state" in data["schema"], (
                f"V1: schema must identify as current_state, got {data['schema']}"
            )

            # Stale must be a boolean
            assert isinstance(data["stale"], bool), "V1: stale must be boolean"

            # generated_at_ms and expires_at_ms must be timestamps
            assert data["generated_at_ms"] > 0, "V1: generated_at_ms must be positive"
            assert data["expires_at_ms"] > data["generated_at_ms"], (
                "V1: expires_at_ms must be after generated_at_ms"
            )

            # open_positions must be a list
            assert isinstance(data["open_positions"], list), (
                "V1: open_positions must be a list"
            )

        finally:
            os.unlink(path)

    def test_export_includes_global_risk_reason_when_present(self):
        """V1: global_risk_reason is included when set (nullable)."""
        state = EngineState(
            lifecycle=EngineLifecycle.RUNNING,
            risk_mode=GlobalRiskMode.REDUCE_ONLY,
        )

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name

        try:
            _export_current_state_snapshot(state, path)
            with open(path) as f:
                data = json.load(f)

            # global_risk_reason should be present (nullable in V1)
            assert "global_risk_reason" in data, (
                "V1 parity violation: current-state export missing global_risk_reason"
            )

        finally:
            os.unlink(path)

    def test_export_includes_reconciliation_close_owner_contract(self):
        state = EngineState(
            lifecycle=EngineLifecycle.RUNNING,
            risk_mode=GlobalRiskMode.RUNNING,
        )
        state.pending_close_reconciliations = [{
            "position_id": "entry-reconciliation-owner",
            "symbol": "BTCUSDT",
            "kind": "final",
            "position_snapshot": {
                "position_id": "entry-reconciliation-owner",
                "symbol": "BTCUSDT",
                "long_venue": "bybit",
                "short_venue": "okx",
                "matched_quantity": 1.0,
            },
            "long_legs": [],
            "short_legs": [],
            "reconciliation_status": "evidence_debt",
            "evidence_debt_reason": "missing_close_order_identity",
        }]
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name

        try:
            _export_current_state_snapshot(state, path)
            with open(path) as f:
                data = json.load(f)
            assert data["pending_close_reconciliation_count"] == 1
            assert data["pending_close_reconciliation_summary"]["total_count"] == 1
            assert data["pending_close_reconciliation_summary"]["evidence_debt_count"] == 1
        finally:
            os.unlink(path)

    def test_export_includes_recovery_blocked_reason_when_present(self):
        """Current-state must expose active recovery blocks for ops health."""
        state = EngineState(
            lifecycle=EngineLifecycle.RISK_ONLY,
            risk_mode=GlobalRiskMode.FAIL_CLOSED,
        )
        state.recovery_blocked_reason = "unpaired_live_positions_detected"

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name

        try:
            _export_current_state_snapshot(state, path)
            with open(path) as f:
                data = json.load(f)

            assert data["recovery_blocked_reason"] == "unpaired_live_positions_detected"

        finally:
            os.unlink(path)

    def test_export_includes_runtime_market_data_config(self):
        """Current-state exposes effective market-data provider diagnostics."""
        state = EngineState(
            lifecycle=EngineLifecycle.RUNNING,
            risk_mode=GlobalRiskMode.RUNNING,
        )
        state.runtime_market_data_config = {
            "entry_readiness_provider_effective": "ws_bbo_quote_lease",
            "local_l2_configured_enabled": True,
            "local_l2_effective_enabled": False,
            "local_l2_effective_disabled_reason": (
                "ws_bbo_quote_lease_overrides_legacy_local_l2_flag"
            ),
        }

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name

        try:
            _export_current_state_snapshot(state, path)
            with open(path) as f:
                data = json.load(f)

            effective = data["runtime_market_data_config"]
            assert (
                effective["entry_readiness_provider_effective"]
                == "ws_bbo_quote_lease"
            )
            assert effective["local_l2_configured_enabled"] is True
            assert effective["local_l2_effective_enabled"] is False
        finally:
            os.unlink(path)

    def test_export_includes_open_position_details(self):
        """V1: each open position exports position_id, symbol, long_venue, short_venue, quantity."""
        from lightfee.core.domain import Venue
        from lightfee.engine.state import OpenPosition

        state = EngineState(
            lifecycle=EngineLifecycle.RUNNING,
            risk_mode=GlobalRiskMode.RUNNING,
        )
        pos = OpenPosition(
            position_id="pos-001",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=0.01,
            short_quantity=0.01,
            long_entry_price=50000.0,
            short_entry_price=50000.0,
            opened_at_ms=wall_clock_now_ms(),
        )
        state.open_positions["pos-001"] = pos

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name

        try:
            _export_current_state_snapshot(state, path)
            with open(path) as f:
                data = json.load(f)

            assert data["open_position_count"] == 1
            assert len(data["open_positions"]) == 1

            pos_data = data["open_positions"][0]
            assert pos_data["position_id"] == "pos-001"
            assert pos_data["symbol"] == "BTCUSDT"
            # Venue should be serialized as string value
            assert "long_venue" in pos_data
            assert "short_venue" in pos_data
            assert pos_data["quantity"] == 0.01

        finally:
            os.unlink(path)

    def test_export_includes_last_scan_when_present(self):
        """V1: last_scan is included when scan data exists (nullable)."""
        state = EngineState(
            lifecycle=EngineLifecycle.RUNNING,
            risk_mode=GlobalRiskMode.RUNNING,
        )

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name

        try:
            _export_current_state_snapshot(state, path)
            with open(path) as f:
                data = json.load(f)

            # last_scan should be present (possibly null)
            assert "last_scan" in data, (
                "V1 parity violation: current-state export missing last_scan field"
            )

        finally:
            os.unlink(path)

    def test_export_includes_populated_last_scan_when_scan_ran(self):
        state = EngineState(lifecycle=EngineLifecycle.RUNNING, risk_mode=GlobalRiskMode.RUNNING)
        state.last_scan = {
            "ts_ms": 1778787000000,
            "snapshot_freshness": "fresh",
            "candidate_count": 12,
            "tradeable_count": 3,
            "degraded_venues": [],
            "no_entry_reason": None,
        }

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            _export_current_state_snapshot(state, path)
            with open(path) as f:
                data = json.load(f)
            assert data["last_scan"]["candidate_count"] == 12
            assert data["last_scan"]["tradeable_count"] == 3
        finally:
            os.unlink(path)

    def test_export_atomic_write(self):
        """V1: current-state is written atomically (temp file + rename)."""
        state = EngineState(
            lifecycle=EngineLifecycle.RUNNING,
            risk_mode=GlobalRiskMode.RUNNING,
        )

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name

        try:
            # Write initial content
            with open(path, "w") as f:
                f.write('{"old": "data"}')

            _export_current_state_snapshot(state, path)

            with open(path) as f:
                data = json.load(f)

            # File should contain new data, not old
            assert "old" not in data, "V1: atomic write should replace old content"
            assert "schema" in data, "V1: atomic write should contain new content"

        finally:
            os.unlink(path)


class TestMetricsExportV1Semantics:
    """V1: Prometheus metrics include journal diagnostics and runtime health counters."""

    def test_metrics_include_journal_diagnostics(self):
        """V1 parity: metrics include journal async/critical/dropped append counters."""
        state = EngineState(
            lifecycle=EngineLifecycle.RUNNING,
            risk_mode=GlobalRiskMode.RUNNING,
        )

        samples = _build_prometheus_metric_samples(state)

        # V1-required journal diagnostic metrics
        required_metrics = [
            "lightfee_journal_async_appends_total",
            "lightfee_journal_critical_appends_total",
            "lightfee_journal_dropped_async_appends_total",
            "lightfee_journal_sync_fallback_appends_total",
        ]
        sample_text = "\n".join(samples)
        for metric in required_metrics:
            assert metric in sample_text, (
                f"V1 parity violation: metrics missing '{metric}'.\nGot:\n{sample_text[:500]}"
            )

    def test_metrics_include_risk_counters(self):
        """V1 parity: metrics include risk warning/delever/death trigger counters."""
        state = EngineState(
            lifecycle=EngineLifecycle.RUNNING,
            risk_mode=GlobalRiskMode.RUNNING,
        )

        samples = _build_prometheus_metric_samples(state)

        required_metrics = [
            "lightfee_runtime_risk_warning_trigger_total",
            "lightfee_runtime_risk_delever_trigger_total",
            "lightfee_runtime_risk_death_trigger_total",
        ]
        sample_text = "\n".join(samples)
        for metric in required_metrics:
            assert metric in sample_text, (
                f"V1 parity violation: metrics missing '{metric}'"
            )

    def test_metrics_include_venue_health_breakdown(self):
        """V1 parity: metrics include per-venue-health-level counts."""
        state = EngineState(
            lifecycle=EngineLifecycle.RUNNING,
            risk_mode=GlobalRiskMode.RUNNING,
        )

        samples = _build_prometheus_metric_samples(state)

        required_metrics = [
            "lightfee_venue_health_normal",
            "lightfee_venue_health_pause_entry",
            "lightfee_venue_health_reduce_only",
            "lightfee_venue_health_fail_closed",
        ]
        sample_text = "\n".join(samples)
        for metric in required_metrics:
            assert metric in sample_text, (
                f"V1 parity violation: metrics missing '{metric}'"
            )

    def test_metrics_include_connection_counters(self):
        """V1 parity: metrics include ws_disconnect, rest_failure, reconcile_drift, order_timeout."""
        state = EngineState(
            lifecycle=EngineLifecycle.RUNNING,
            risk_mode=GlobalRiskMode.RUNNING,
        )

        samples = _build_prometheus_metric_samples(state)

        required_metrics = [
            "lightfee_runtime_ws_disconnect_total",
            "lightfee_runtime_rest_failure_total",
            "lightfee_runtime_reconcile_drift_total",
            "lightfee_runtime_order_timeout_total",
        ]
        sample_text = "\n".join(samples)
        for metric in required_metrics:
            assert metric in sample_text, (
                f"V1 parity violation: metrics missing '{metric}'"
            )

    def test_metrics_include_position_groups(self):
        """V1 parity: metrics aggregate positions by (symbol, long_venue, short_venue) group."""
        from lightfee.core.domain import Venue
        from lightfee.engine.state import OpenPosition

        state = EngineState(
            lifecycle=EngineLifecycle.RUNNING,
            risk_mode=GlobalRiskMode.RUNNING,
        )
        pos = OpenPosition(
            position_id="pos-001",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=0.01,
            short_quantity=0.01,
            long_entry_price=50000.0,
            short_entry_price=50000.0,
            opened_at_ms=wall_clock_now_ms(),
        )
        state.open_positions["pos-001"] = pos

        samples = _build_prometheus_metric_samples(state)
        sample_text = "\n".join(samples)

        assert "lightfee_position_group_count" in sample_text, (
            "V1 parity violation: metrics missing position group aggregation"
        )
        assert "lightfee_position_group_quantity" in sample_text, (
            "V1 parity violation: metrics missing position group quantity"
        )

    def test_metrics_include_snapshot_freshness_by_venue_symbol_domain(self):
        state = EngineState(
            lifecycle=EngineLifecycle.RUNNING,
            risk_mode=GlobalRiskMode.RUNNING,
        )
        state.last_scan = {
            "snapshot_freshness_metrics": {
                "okx|BTCUSDT|liquidity": {"fresh": 0, "stale": 2},
                "bybit|BTCUSDT|quote": {"fresh": 1, "stale": 0},
            }
        }

        samples = _build_prometheus_metric_samples(state)

        assert (
            'lightfee_snapshot_freshness_observed_total{venue="okx",symbol="BTCUSDT",domain="liquidity",decision="stale"} 2'
            in samples
        )
        assert (
            'lightfee_snapshot_freshness_observed_total{venue="bybit",symbol="BTCUSDT",domain="quote",decision="fresh"} 1'
            in samples
        )

    def test_metrics_include_lifecycle_and_risk_mode_gauges(self):
        """V1 parity: lifecycle and risk_mode are exported as gauge metrics."""
        state = EngineState(
            lifecycle=EngineLifecycle.RUNNING,
            risk_mode=GlobalRiskMode.RUNNING,
        )

        samples = _build_prometheus_metric_samples(state)
        sample_text = "\n".join(samples)

        assert "lightfee_lifecycle" in sample_text, "V1: missing lifecycle gauge"
        assert "lightfee_risk_mode" in sample_text, "V1: missing risk_mode gauge"
        assert "lightfee_tick_count" in sample_text, "V1: missing tick_count counter"
        assert "lightfee_open_positions" in sample_text, "V1: missing open_positions gauge"

    def test_metrics_disabled_when_env_export_is_zero(self):
        """V1: LIGHTFEE_METRICS_EXPORT=0 disables metric export."""
        config = AppConfig()
        os.environ["LIGHTFEE_METRICS_EXPORT"] = "0"
        try:
            path = metrics_export_path(config)
            assert path is None, "V1: metrics export should be disabled when env=0"
        finally:
            del os.environ["LIGHTFEE_METRICS_EXPORT"]

    def test_maybe_export_respects_interval(self):
        """V1: metrics and state export are interval-gated."""
        state = EngineState(
            lifecycle=EngineLifecycle.RUNNING,
            risk_mode=GlobalRiskMode.RUNNING,
        )
        config = AppConfig()
        es = ExportState()
        now_ms = wall_clock_now_ms()

        # First call: should export (deadline is 0)
        os.environ["LIGHTFEE_METRICS_TEXTFILE_PATH"] = "/tmp/test_v1_metrics_export.prom"
        try:
            maybe_export_runtime_metrics(state, config, es, now_ms)
            assert es.next_metrics_export_ms > now_ms, "V1: export should set next deadline"

            # Second call within interval: should not update deadline further
            old_deadline = es.next_metrics_export_ms
            maybe_export_runtime_metrics(state, config, es, now_ms + 1000)
            assert es.next_metrics_export_ms == old_deadline, (
                "V1: export should be gated by interval"
            )
        finally:
            del os.environ["LIGHTFEE_METRICS_TEXTFILE_PATH"]
            try:
                os.unlink("/tmp/test_v1_metrics_export.prom")
            except OSError:
                pass


class TestJournalDiagnosticsExport:
    """V1: journal diagnostics are exported as part of post-tick housekeeping."""

    def test_journal_metrics_snapshot_structure(self):
        """V1: JournalRuntimeMetricsSnapshot has all V1-visible fields."""
        from lightfee.ops.metrics import JournalRuntimeMetricsSnapshot

        snap = JournalRuntimeMetricsSnapshot()

        # All fields should be accessible with defaults
        assert snap.async_appends == 0
        assert snap.critical_appends == 0
        assert snap.dropped_async_appends == 0
        assert snap.sync_fallback_appends == 0
        assert snap.flush_requests == 0
        assert snap.writer_flushes == 0
        assert snap.writer_failures == 0
        assert snap.queue_disconnects == 0
        assert snap.open_position_count == 0
        assert snap.global_risk_mode == "running"
        assert snap.net_exposure_milli_quote == 0
        assert snap.venue_health_normal_count == 0
        assert snap.venue_health_pause_entry_count == 0
        assert snap.venue_health_reduce_only_count == 0
        assert snap.venue_health_fail_closed_count == 0
        assert snap.risk_warning_trigger_count == 0
        assert snap.risk_delever_trigger_count == 0
        assert snap.risk_death_trigger_count == 0
        assert snap.order_timeout_count == 0
        assert snap.ws_disconnect_count == 0
        assert snap.rest_failure_count == 0
        assert snap.reconcile_drift_count == 0


class TestOperatorCommandsV1Semantics:
    """V1: operator commands preserve risk-mode and pending-reconcile semantics."""

    def test_pause_entry_escalates_risk(self):
        """V1: PAUSE_ENTRY escalates risk to at least ENTRY_PAUSED."""
        from lightfee.ops.commands import execute_operator_command
        from lightfee.risk.operator import OperatorCommand

        new_risk, new_lifecycle, msg = execute_operator_command(
            OperatorCommand.PAUSE_ENTRY,
            GlobalRiskMode.RUNNING,
            EngineLifecycle.RUNNING,
        )
        assert new_risk == GlobalRiskMode.ENTRY_PAUSED, (
            f"V1: PAUSE_ENTRY should set risk to ENTRY_PAUSED, got {new_risk}"
        )
        assert "paused" in msg.lower()

    def test_reduce_only_escalates_risk(self):
        """V1: REDUCE_ONLY escalates risk to at least REDUCE_ONLY."""
        from lightfee.ops.commands import execute_operator_command
        from lightfee.risk.operator import OperatorCommand

        new_risk, new_lifecycle, msg = execute_operator_command(
            OperatorCommand.REDUCE_ONLY,
            GlobalRiskMode.ENTRY_PAUSED,
            EngineLifecycle.RUNNING,
        )
        assert new_risk == GlobalRiskMode.REDUCE_ONLY, (
            f"V1: REDUCE_ONLY should escalate from ENTRY_PAUSED to REDUCE_ONLY, got {new_risk}"
        )

    def test_fail_closed_sets_lifecycle_risk_only(self):
        """V1: FAIL_CLOSED sets FAIL_CLOSED risk and RISK_ONLY lifecycle."""
        from lightfee.ops.commands import execute_operator_command
        from lightfee.risk.operator import OperatorCommand

        new_risk, new_lifecycle, msg = execute_operator_command(
            OperatorCommand.FAIL_CLOSED,
            GlobalRiskMode.RUNNING,
            EngineLifecycle.RUNNING,
        )
        assert new_risk == GlobalRiskMode.FAIL_CLOSED, (
            f"V1: FAIL_CLOSED should set risk to FAIL_CLOSED, got {new_risk}"
        )
        assert new_lifecycle == EngineLifecycle.RISK_ONLY, (
            f"V1: FAIL_CLOSED should set lifecycle to RISK_ONLY, got {new_lifecycle}"
        )

    def test_resume_blocked_by_recovery(self):
        """V1: RESUME_IF_SAFE is blocked when recovery is in progress."""
        from lightfee.ops.commands import execute_operator_command
        from lightfee.risk.operator import OperatorCommand

        new_risk, new_lifecycle, msg = execute_operator_command(
            OperatorCommand.RESUME_IF_SAFE,
            GlobalRiskMode.REDUCE_ONLY,
            EngineLifecycle.RECONCILING,
            has_blocking_recovery=True,
        )
        assert new_risk == GlobalRiskMode.REDUCE_ONLY, (
            "V1: RESUME_IF_SAFE must not change risk mode when recovery is blocking"
        )
        assert "Cannot resume" in msg or "unsafe" in msg.lower()

    def test_resume_succeeds_when_safe(self):
        """V1: RESUME_IF_SAFE restores RUNNING when no blocking conditions exist."""
        from lightfee.ops.commands import execute_operator_command
        from lightfee.risk.operator import OperatorCommand

        new_risk, new_lifecycle, msg = execute_operator_command(
            OperatorCommand.RESUME_IF_SAFE,
            GlobalRiskMode.ENTRY_PAUSED,
            EngineLifecycle.RUNNING,
            has_blocking_recovery=False,
        )
        assert new_risk == GlobalRiskMode.RUNNING, (
            f"V1: RESUME_IF_SAFE should restore RUNNING, got {new_risk}"
        )

    def test_fail_closed_persists_and_restart_shows_risk_only(self):
        """C-R3: Operator fail-closed persists to snapshot and journal,
        and restart shows RISK_ONLY + FAIL_CLOSED (not FAIL_CLOSED lifecycle)."""
        from lightfee.engine.recovery import _restore_state_from_snapshot_dict
        from lightfee.engine.state import EngineState
        from lightfee.ops.commands import execute_operator_command
        from lightfee.persistence.journal import Journal
        from lightfee.persistence.snapshot_store import SnapshotStore
        from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
        from lightfee.risk.operator import OperatorCommand

        tmp = tempfile.mkdtemp()
        snap_path = Path(tmp) / "snapshot.json"
        journal_path = Path(tmp) / "journal.jsonl"

        # 1. Write initial empty snapshot
        store = SnapshotStore(snap_path)
        state = EngineState()
        state.lifecycle = EngineLifecycle.RUNNING
        state.risk_mode = GlobalRiskMode.RUNNING
        from lightfee.engine.recovery import build_persistent_state_view
        store.write(build_persistent_state_view(state))

        # 2. Apply fail-closed operator command with real journal + state
        journal = Journal(journal_path)
        journal.open()
        new_risk, new_lifecycle, msg = execute_operator_command(
            OperatorCommand.FAIL_CLOSED,
            state.risk_mode, state.lifecycle,
            journal=journal, state=state,
        )
        store.write(build_persistent_state_view(state))
        journal.close()

        # Verify in-memory state
        assert new_risk == GlobalRiskMode.FAIL_CLOSED
        assert new_lifecycle == EngineLifecycle.RISK_ONLY  # V1: NOT fail_closed lifecycle

        # 3. Restart: reload from snapshot + journal → should keep RISK_ONLY + FAIL_CLOSED
        store2 = SnapshotStore(snap_path)
        snap2 = store2.read()
        assert snap2 is not None
        state2 = _restore_state_from_snapshot_dict(snap2)
        journal2 = Journal(journal_path)
        journal2.open()
        from lightfee.engine.recovery import recover_from_snapshot
        state2 = recover_from_snapshot(store2, journal2)
        journal2.close()

        assert state2.risk_mode == GlobalRiskMode.FAIL_CLOSED, (
            f"Restart must retain FAIL_CLOSED risk mode, got {state2.risk_mode}"
        )
        assert state2.lifecycle == EngineLifecycle.RISK_ONLY, (
            f"Restart must show RISK_ONLY lifecycle (V1: never fail_closed), got {state2.lifecycle}"
        )

        # 4. Verify journal has the ops.command_applied critical event
        records = journal2.read_all()
        ops_records = [r for r in records if r.get("kind") == "ops.command_applied"]
        assert len(ops_records) >= 1, "Must have ops.command_applied in journal"

    def test_fail_closed_operator_latch_persists(self):
        """C-R3: Operator FAIL_CLOSED persists its requested-mode latch."""
        from lightfee.engine.state import EngineState
        from lightfee.ops.commands import execute_operator_command
        from lightfee.persistence.journal import Journal
        from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
        from lightfee.risk.operator import OperatorCommand

        tmp = tempfile.mkdtemp()
        journal_path = Path(tmp) / "journal.jsonl"
        journal = Journal(journal_path)
        journal.open()

        state = EngineState()
        state.lifecycle = EngineLifecycle.RUNNING
        state.risk_mode = GlobalRiskMode.RUNNING

        new_risk, new_lifecycle, msg = execute_operator_command(
            OperatorCommand.FAIL_CLOSED,
            state.risk_mode, state.lifecycle,
            journal=journal, state=state,
        )
        journal.close()

        # After command: state must have operator.requested_mode == FAIL_CLOSED
        assert state.risk_mode == GlobalRiskMode.FAIL_CLOSED
        assert state.lifecycle == EngineLifecycle.RISK_ONLY
        assert state.operator.requested_mode == GlobalRiskMode.FAIL_CLOSED, (
            "C-R3: operator FAIL_CLOSED must set requested_mode latch"
        )

        assert state.risk_mode == GlobalRiskMode.FAIL_CLOSED, (
            "C-R3: risk_mode must stay FAIL_CLOSED until an explicit operator resume"
        )

    def test_resume_if_safe_clears_operator_latch(self):
        """C-R3: Successful RESUME_IF_SAFE clears operator.requested_mode latch.

        V1 RESUME_IF_SAFE only transitions to RUNNING when lifecycle is
        already RUNNING (clears ENTRY_PAUSED/REDUCE_ONLY risk). When it
        succeeds, the operator latch must also be cleared."""
        from lightfee.engine.state import EngineState
        from lightfee.ops.commands import execute_operator_command
        from lightfee.persistence.journal import Journal
        from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
        from lightfee.risk.operator import OperatorCommand

        tmp = tempfile.mkdtemp()
        journal_path = Path(tmp) / "journal.jsonl"
        journal = Journal(journal_path)
        journal.open()

        # Scenario: operator previously fail-closed, then manually returned
        # lifecycle to RUNNING (e.g. via recovery). RESUME_IF_SAFE should
        # clear the stale requested_mode latch.
        state = EngineState()
        state.lifecycle = EngineLifecycle.RUNNING
        state.risk_mode = GlobalRiskMode.ENTRY_PAUSED
        state.operator.requested_mode = GlobalRiskMode.FAIL_CLOSED

        new_risk, new_lifecycle, msg = execute_operator_command(
            OperatorCommand.RESUME_IF_SAFE,
            state.risk_mode, state.lifecycle,
            has_blocking_recovery=False,
            journal=journal, state=state,
        )
        journal.close()

        assert new_risk == GlobalRiskMode.RUNNING, (
            f"RESUME_IF_SAFE should restore RUNNING, got {new_risk}"
        )
        assert state.operator.requested_mode is None, (
            "C-R3: successful RESUME_IF_SAFE must clear operator.requested_mode"
        )

    def test_old_snapshot_fail_closed_lifecycle_migrates(self):
        """C-R1: Old snapshot with lifecycle='fail_closed' is migrated to RISK_ONLY.
        Runtime never produces lifecycle='fail_closed'."""
        from lightfee.engine.recovery import _restore_state_from_snapshot_dict
        from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode

        # Simulate an old snapshot with the removed FAIL_CLOSED lifecycle string
        snap = {
            "lifecycle": "fail_closed",
            "risk_mode": "fail_closed",
            "run_id": "test",
            "started_at_ms": 1000,
        }
        state = _restore_state_from_snapshot_dict(snap)

        assert state.lifecycle == EngineLifecycle.RISK_ONLY, (
            f"Old 'fail_closed' lifecycle must migrate to RISK_ONLY, got {state.lifecycle}"
        )
        assert state.risk_mode == GlobalRiskMode.FAIL_CLOSED, (
            "risk_mode should stay FAIL_CLOSED"
        )

        # Verify that Runtime never generates fail_closed lifecycle
        from lightfee.risk.modes import EngineMode, derive_engine_mode
        mode = derive_engine_mode(state.lifecycle, state.risk_mode)
        assert mode == EngineMode.FAIL_CLOSED

    def test_supports_amend_override_detection(self):
        """C-R8: supports_amend checks __dict__ for override, not hasattr.
        Base class amend_order that raises NotImplementedError must return False."""
        from lightfee.engine.entry_sync import _adapter_supports_amend

        class BaseAdapter:
            def amend_order(self, req):
                raise NotImplementedError

        class RealAdapter(BaseAdapter):
            def amend_order(self, req):
                return req  # Actually overrides

        base = BaseAdapter()
        real = RealAdapter()

        assert not _adapter_supports_amend(base), (
            "Base class with NotImplementedError must NOT report supports_amend"
        )
        assert _adapter_supports_amend(real), (
            "Real override must report supports_amend"
        )
        assert not _adapter_supports_amend(None), "None adapter must return False"


class TestCurrentStateExportPath:
    """V1: current-state path derivation from snapshot path."""

    def test_path_derivation(self):
        config = AppConfig(
            persistence=PersistenceConfig(
                event_log_path="/tmp/events.jsonl",
                snapshot_path="/tmp/state.json",
            )
        )
        path = current_state_export_path(config)
        assert "current" in path, f"V1: current-state path must contain 'current', got {path}"
        assert path.endswith(".json"), f"V1: current-state path must end with .json, got {path}"
