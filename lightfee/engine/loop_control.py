"""Periodic metrics and state export matching Rust loop_control."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from lightfee.config.schema import AppConfig
from lightfee.engine.business_contract import (
    classify_close_reconciliation_state,
    close_reconciliation_exchange_truth_clean,
)
from lightfee.engine.state import EngineState, normalize_pending_close_reconciliations


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


def _effective_recovery_export_state(
    state: EngineState,
    v1_lifecycle_closure: dict[str, Any],
) -> tuple[str, Any, int]:
    lifecycle = state.lifecycle.value
    recovery_blocked_reason = getattr(state, "recovery_blocked_reason", None)
    recovery_blocked_at_ms = getattr(state, "recovery_blocked_at_ms", 0)
    summary = dict(v1_lifecycle_closure.get("summary") or {})
    closure_allows_entry = summary.get("entry_allowed") is True
    closure_block_reason = summary.get("recovery_block_reason")
    no_current_work = (
        len(getattr(state, "pending_entries", []) or []) == 0
        and len(getattr(state, "pending_closes", []) or []) == 0
        and len(getattr(state, "pending_passive_closes", []) or []) == 0
        and len(getattr(state, "pending_residual_repairs", []) or []) == 0
    )
    if closure_allows_entry and not closure_block_reason and no_current_work:
        if state.risk_mode.value == "running":
            lifecycle = "running"
            recovery_blocked_reason = None
            recovery_blocked_at_ms = 0
    return lifecycle, recovery_blocked_reason, recovery_blocked_at_ms


@dataclass
class ExportState:
    """Tracks next-export deadlines for throttled periodic exporters."""

    next_metrics_export_ms: int = 0
    next_state_export_ms: int = 0


def _pending_close_reconciliation_summary(raw: Any) -> dict[str, Any]:
    items = normalize_pending_close_reconciliations(raw)
    blocking_count = 0
    terminal_flat_count = 0
    symbols: list[str] = []
    seen_symbols: set[str] = set()

    for item in items:
        snapshot = item.get("position_snapshot") if isinstance(item, dict) else {}
        if not isinstance(snapshot, dict):
            snapshot = {}
        symbol = str(item.get("symbol") or snapshot.get("symbol") or "").upper()
        if symbol and symbol not in seen_symbols:
            symbols.append(symbol)
            seen_symbols.add(symbol)

        if (
            item.get("archived") is True
            and str(item.get("archive_reason") or "")
            == "terminal_flat_accounting_gap"
        ):
            terminal_flat_count += 1
            continue
        if (
            item.get("accounting_only_backfill") is True
            and item.get("blocking_trading") is False
            and str(item.get("close_reconciliation_state") or "")
            == "terminal_flat_accounting_gap"
        ):
            terminal_flat_count += 1
            continue

        contract = classify_close_reconciliation_state(
            item,
            current_exchange_truth_clean=close_reconciliation_exchange_truth_clean(
                item
            ),
        )
        state = str(contract.get("state") or "")
        if state == "terminal_flat_accounting_gap":
            terminal_flat_count += 1
        if contract.get("blocks_entry") is True:
            blocking_count += 1

    return {
        "pending_close_reconciliation_count": len(items),
        "pending_close_reconciliation_blocking_count": blocking_count,
        "pending_close_reconciliation_terminal_flat_count": terminal_flat_count,
        "pending_close_reconciliation_symbols": symbols,
    }


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
    data = build_current_state_snapshot_payload(state, config)
    write_json_atomic(path, data)


def _freeze_current_state_value(
    value: Any,
    *,
    budget: list[int],
    depth: int = 0,
) -> Any:
    """Bound and detach diagnostic state while the event loop owns mutation."""
    if budget[0] <= 0:
        return {"truncated": True, "reason": "current_state_node_budget"}
    budget[0] -= 1
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value[:4_096]
    if depth >= 12:
        return str(value)[:512]
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        original_count = len(value)
        for index, (key, item) in enumerate(value.items()):
            if index >= 1_024 or budget[0] <= 0:
                break
            result[str(key)] = _freeze_current_state_value(
                item,
                budget=budget,
                depth=depth + 1,
            )
        if len(result) < original_count:
            result["__truncated_items__"] = original_count - len(result)
        return result
    if isinstance(value, (list, tuple, set, frozenset)):
        source = list(value)
        result = [
            _freeze_current_state_value(
                item,
                budget=budget,
                depth=depth + 1,
            )
            for item in source[:256]
            if budget[0] > 0
        ]
        if len(result) < len(source):
            result.append(
                {"truncated_items": len(source) - len(result)}
            )
        return result
    if hasattr(value, "value"):
        return _freeze_current_state_value(
            value.value,
            budget=budget,
            depth=depth + 1,
        )
    return str(value)[:512]


def build_current_state_snapshot_payload(
    state: EngineState,
    config: Optional[AppConfig] = None,
    *,
    generated_at_ms: int | None = None,
) -> dict[str, Any]:
    """Build one bounded immutable current-state DTO on the owner thread."""
    import time

    now_ms = int(generated_at_ms or time.time() * 1000)
    if config is None:
        config = AppConfig()
    stale_after_ms = current_state_export_interval_ms(config) * 3
    # One global budget prevents a large last_scan/audit map from turning the
    # health export back into a megabyte-scale GIL workload.
    freeze_budget = [20_000]

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

    runtime_progress = _freeze_current_state_value(
        getattr(state, "runtime_progress", {}) or {},
        budget=freeze_budget,
    )
    if not isinstance(runtime_progress, dict):
        runtime_progress = {}
    active_lane = str(runtime_progress.get("active_lane") or "")
    try:
        active_lane_started_ms = int(runtime_progress.get("active_lane_started_ms") or 0)
    except (TypeError, ValueError):
        active_lane_started_ms = 0
    try:
        active_lane_budget_ms = int(runtime_progress.get("active_lane_budget_ms") or 0)
    except (TypeError, ValueError):
        active_lane_budget_ms = 0
    if active_lane and active_lane_started_ms > 0 and active_lane_budget_ms > 0:
        runtime_progress["active_lane_overdue"] = (
            now_ms - active_lane_started_ms > active_lane_budget_ms
        )
    v1_lifecycle_closure = _freeze_current_state_value(
        getattr(state, "v1_lifecycle_closure", {}) or {},
        budget=freeze_budget,
    )
    if not isinstance(v1_lifecycle_closure, dict):
        v1_lifecycle_closure = {}
    if not v1_lifecycle_closure:
        from lightfee.engine.v1_lifecycle_closure import (
            build_v1_lifecycle_closure_table,
        )

        v1_lifecycle_closure = build_v1_lifecycle_closure_table(
            local_state=state,
            exchange_truth=None,
            generated_at_ms=now_ms,
        ).to_dict()
    pending_close_reconciliation = _pending_close_reconciliation_summary(
        getattr(state, "pending_close_reconciliations", [])
    )
    (
        effective_lifecycle,
        effective_recovery_blocked_reason,
        effective_recovery_blocked_at_ms,
    ) = _effective_recovery_export_state(state, v1_lifecycle_closure)

    data = {
        "schema": "lightfee.current_state.v1",
        "generated_at_ms": now_ms,
        "expires_at_ms": now_ms + stale_after_ms,
        "stale": False,
        "mode": mode,
        "lifecycle": effective_lifecycle,
        "risk_mode": state.risk_mode.value,
        "global_risk_mode": state.risk_mode.value,
        "global_risk_reason": getattr(state, "global_risk_reason", None),
        "hyperliquid_trading_disabled_reason": getattr(
            state, "hyperliquid_trading_disabled_reason", None
        ),
        "recovery_blocked_reason": effective_recovery_blocked_reason,
        "recovery_blocked_at_ms": effective_recovery_blocked_at_ms,
        "run_id": state.run_id,
        "tick_count": state.tick_count,
        "last_tick_ms": state.last_tick_ms,
        "open_position_count": len(state.open_positions),
        "max_concurrent_positions": max(
            config.strategy.max_concurrent_positions,
            1,
        ),
        "open_positions": open_positions,
        "pending_entry_count": len(state.pending_entries),
        "pending_close_count": len(state.pending_closes),
        "pending_passive_close_count": len(state.pending_passive_closes),
        **pending_close_reconciliation,
        "pending_residual_repair_count": len(getattr(state, "pending_residual_repairs", []) or []),
        "pending_residual_repairs": _freeze_current_state_value(
            getattr(state, "pending_residual_repairs", []) or [],
            budget=freeze_budget,
        ),
        "live_recovery_reduce_only_pairs": _freeze_current_state_value(
            getattr(state, "live_recovery_reduce_only_pairs", []) or [],
            budget=freeze_budget,
        ),
        "venue_entry_cooldowns": _freeze_current_state_value(
            getattr(state, "venue_entry_cooldowns", {}) or {},
            budget=freeze_budget,
        ),
        "last_scan": _freeze_current_state_value(
            getattr(state, "last_scan", None),
            budget=freeze_budget,
        ),
        "runtime_progress": runtime_progress,
        "runtime_market_data_config": _freeze_current_state_value(
            getattr(state, "runtime_market_data_config", {}) or {},
            budget=freeze_budget,
        ),
        "v1_lifecycle_closure": v1_lifecycle_closure,
    }
    return data


def _build_prometheus_metric_samples(state: EngineState) -> list[str]:
    """Assemble Prometheus metric lines via V1-compatible builder."""
    from lightfee.ops.metrics import build_prometheus_metric_samples as _v1_build

    return _v1_build(state)
