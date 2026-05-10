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
    _export_current_state_snapshot(state, path)


def _export_runtime_metrics(state: EngineState, path: str) -> None:
    """Write Prometheus textfile metric samples."""
    samples = _build_prometheus_metric_samples(state)
    with open(path, "w") as f:
        for sample in samples:
            f.write(f"{sample}\n")


def _export_current_state_snapshot(state: EngineState, path: str) -> None:
    """Write the current-state JSON snapshot atomically."""
    data = {
        "schema": "lightfee.current_state.v1",
        "lifecycle": state.lifecycle.value,
        "risk_mode": state.risk_mode.value,
        "run_id": state.run_id,
        "tick_count": state.tick_count,
        "last_tick_ms": state.last_tick_ms,
        "open_position_count": len(state.open_positions),
        "pending_entry_count": len(state.pending_entries),
        "pending_close_count": len(state.pending_closes),
    }
    write_json_atomic(path, data)


def _build_prometheus_metric_samples(state: EngineState) -> list[str]:
    """Assemble Prometheus gauge / counter metric lines from engine state."""
    samples = [
        "# HELP lightfee_tick_count Total engine ticks.",
        "# TYPE lightfee_tick_count counter",
        f"lightfee_tick_count {state.tick_count}",
        "# HELP lightfee_open_positions Current open position count.",
        "# TYPE lightfee_open_positions gauge",
        f"lightfee_open_positions {len(state.open_positions)}",
        "# HELP lightfee_lifecycle Engine lifecycle 0=booting 3=running 4=fail_closed.",
        "# TYPE lightfee_lifecycle gauge",
        f"lightfee_lifecycle{{state=\"{state.lifecycle.value}\"}} {_lifecycle_code(state.lifecycle.value)}",
        "# HELP lightfee_risk_mode Global risk mode 0=running 3=fail_closed.",
        "# TYPE lightfee_risk_mode gauge",
        f"lightfee_risk_mode{{mode=\"{state.risk_mode.value}\"}} {_risk_mode_code(state.risk_mode.value)}",
    ]
    for pos_id, pos in state.open_positions.items():
        samples.append(
            f"lightfee_position{{id=\"{pos_id}\",symbol=\"{pos.symbol}\"}} 1"
        )
    return samples


def _lifecycle_code(value: str) -> int:
    codes = {"booting": 0, "reconciling": 1, "risk_only": 2, "running": 3, "fail_closed": 4}
    return codes.get(value, -1)


def _risk_mode_code(value: str) -> int:
    codes = {"running": 0, "entry_paused": 1, "reduce_only": 2, "fail_closed": 3}
    return codes.get(value, -1)
