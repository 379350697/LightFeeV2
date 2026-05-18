"""Periodic metrics and state export matching Rust loop_control."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from lightfee.config.schema import AppConfig
from lightfee.engine.state import EngineState


def metrics_export_path(config: AppConfig) -> Optional[str]:
    """Prometheus textfile export path.  None when LIGHTFEE_METRICS_EXPORT=0."""
    env_disable = os.environ.get("LIGHTFEE_METRICS_EXPORT", "")
    if env_disable.lower() in ("0", "false", "no", "off"):
        return None
    env_path = os.environ.get("LIGHTFEE_METRICS_TEXTFILE_PATH", "")
    if env_path:
        return env_path
    return str(Path(config.persistence.event_log_path).with_suffix(".prom"))


def metrics_export_interval_ms(config: AppConfig) -> int:
    """Interval between Prometheus metric exports (min 1000ms)."""
    env_val = os.environ.get("LIGHTFEE_METRICS_EXPORT_INTERVAL_MS", "")
    if env_val:
        try:
            return max(int(env_val), 1000)
        except ValueError:
            pass
    return max(config.runtime.poll_interval_ms, 5000)


def current_state_export_path(config: AppConfig) -> str:
    """Path for the periodic current-state JSON snapshot."""
    base = Path(config.persistence.snapshot_path)
    return str(base.with_name(base.stem + "-current.json"))


def current_state_export_interval_ms(config: AppConfig) -> int:
    """Interval between current-state snapshot exports (min 1000ms)."""
    env_val = os.environ.get("LIGHTFEE_CURRENT_STATE_EXPORT_INTERVAL_MS", "")
    if env_val:
        try:
            return max(int(env_val), 1000)
        except ValueError:
            pass
    return max(config.runtime.poll_interval_ms, 1000)


def write_json_atomic(path: str, data: dict) -> None:
    """Atomically write JSON via temp-file + rename."""
    dirname = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=dirname, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


@dataclass
class ExportState:
    """Tracks next-export deadlines for throttled periodic exporters."""

    next_metrics_export_ms: int = 0
    next_state_export_ms: int = 0


def maybe_export_runtime_metrics(
    state: EngineState,
    config: AppConfig,
    export_state: ExportState,
    now_ms: int,
) -> None:
    """Interval-gated Prometheus textfile export."""
    path = metrics_export_path(config)
    if path is None:
        return
    if now_ms < export_state.next_metrics_export_ms:
        return
    export_state.next_metrics_export_ms = now_ms + metrics_export_interval_ms(config)
    _export_runtime_metrics(state, path)


def maybe_export_current_state_snapshot(
    state: EngineState,
    config: AppConfig,
    export_state: ExportState,
    now_ms: int,
) -> None:
    """Interval-gated current-state JSON snapshot export."""
    path = current_state_export_path(config)
    if now_ms < export_state.next_state_export_ms:
        return
    export_state.next_state_export_ms = now_ms + current_state_export_interval_ms(config)
    _export_current_state_snapshot(state, path, config)


def _export_runtime_metrics(state: EngineState, path: str) -> None:
    """Write Prometheus textfile metric samples (V1-compatible)."""
    from lightfee.ops.metrics import build_prometheus_metric_samples as _v1_build

    samples = _v1_build(state)
    with open(path, "w") as f:
        for sample in samples:
            f.write(f"{sample}\n")


def _export_current_state_snapshot(state: EngineState, path: str, config: Optional[AppConfig] = None) -> None:
    """Write current-state JSON snapshot with all V1-visible fields.

    V1: CurrentStateSnapshot — schema, generated_at_ms, expires_at_ms, stale,
    mode, lifecycle, global_risk_mode, global_risk_reason, open_position_count,
    open_positions (detailed), last_scan.
    """
    import time

    now_ms = int(time.time() * 1000)
    if config is None:
        config = AppConfig()
    stale_after_ms = current_state_export_interval_ms(config) * 3

    open_positions = []
    for pos in state.open_positions.values():
        open_positions.append({
            "position_id": pos.position_id,
            "symbol": pos.symbol,
            "long_venue": pos.long_venue.value if hasattr(pos.long_venue, "value") else str(pos.long_venue),
            "short_venue": pos.short_venue.value if hasattr(pos.short_venue, "value") else str(pos.short_venue),
            "quantity": pos.matched_quantity,
        })

    mode = config.runtime.mode

    data = {
        "schema": "lightfee.current_state.v1",
        "generated_at_ms": now_ms,
        "expires_at_ms": now_ms + stale_after_ms,
        "stale": False,
        "mode": mode,
        "lifecycle": state.lifecycle.value,
        "risk_mode": state.risk_mode.value,
        "global_risk_mode": state.risk_mode.value,
        "global_risk_reason": getattr(state, "global_risk_reason", None),
        "recovery_blocked_reason": getattr(state, "recovery_blocked_reason", None),
        "recovery_blocked_at_ms": getattr(state, "recovery_blocked_at_ms", 0),
        "run_id": state.run_id,
        "tick_count": state.tick_count,
        "last_tick_ms": state.last_tick_ms,
        "open_position_count": len(state.open_positions),
        "open_positions": open_positions,
        "pending_entry_count": len(state.pending_entries),
        "pending_close_count": len(state.pending_closes),
        "last_scan": getattr(state, "last_scan", None),
    }
    write_json_atomic(path, data)


def _build_prometheus_metric_samples(state: EngineState) -> list[str]:
    """Assemble Prometheus metric lines via V1-compatible builder."""
    from lightfee.ops.metrics import build_prometheus_metric_samples as _v1_build

    return _v1_build(state)
