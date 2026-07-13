"""Durable, epoch-scoped checkpoints for bounded spread statistics."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from lightfee.spread.reversion import SpreadStatsTracker


SPREAD_STATS_CHECKPOINT_SCHEMA_VERSION = 1


def restore_spread_stats_checkpoint(
    tracker: SpreadStatsTracker,
    path: str | Path,
    *,
    model_epoch: str,
    now_ms: int,
) -> bool:
    """Restore only a matching, bounded checkpoint; otherwise cold-start.

    This is intentionally a boolean rather than an exception path: corrupted
    local state must not take down a public sidecar, and it must never result
    in silently retained stale observations.
    """

    source = Path(path)
    try:
        raw = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if not isinstance(raw, dict):
        return False
    if int(raw.get("schema_version", 0) or 0) != SPREAD_STATS_CHECKPOINT_SCHEMA_VERSION:
        return False
    if str(raw.get("model_epoch", "") or "") != str(model_epoch or ""):
        return False
    states = raw.get("states")
    if not isinstance(states, dict):
        return False
    try:
        restored_at_ms = max(int(now_ms or 0), 0)
        saved_at_ms = int(raw.get("saved_at_ms", 0) or 0)
    except (TypeError, ValueError, OverflowError):
        return False
    # A checkpoint older than the full rolling window cannot provide a
    # continuous state.  Cold-start instead of silently rebuilding a signal
    # from an arbitrary surviving subset after a long outage.
    if (
        saved_at_ms <= 0
        or saved_at_ms > restored_at_ms
        or restored_at_ms - saved_at_ms > tracker.window_ms
    ):
        return False
    try:
        if not tracker.restore(states, now_ms=restored_at_ms):
            return False
    except (TypeError, ValueError, OverflowError):
        # `restore` is defensive itself, but keep this boundary fail-closed if
        # a future serialization change introduces a malformed scalar.
        return False
    return True


def publish_spread_stats_checkpoint(
    tracker: SpreadStatsTracker,
    path: str | Path,
    *,
    model_epoch: str,
    now_ms: int,
) -> None:
    """Atomically publish the already-bounded tracker state."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": SPREAD_STATS_CHECKPOINT_SCHEMA_VERSION,
        "model_epoch": str(model_epoch or ""),
        "saved_at_ms": max(int(now_ms or 0), 0),
        "states": tracker.checkpoint(now_ms=max(int(now_ms or 0), 0)),
    }
    fd, tmp_name = tempfile.mkstemp(dir=str(target.parent), prefix=".spread-stats-")
    os.close(fd)
    temporary = Path(tmp_name)
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(
                payload,
                handle,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            )
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(target)
    except Exception:
        if temporary.exists():
            temporary.unlink()
        raise
