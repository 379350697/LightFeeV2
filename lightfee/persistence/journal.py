"""JSONL journal: append-only event log matching Rust reference behavior.

Rust references:
- src/observability_ops/journal_bridge.rs (JsonlJournal)
- src/observability_ops/replay_bridge.rs (journal replay)
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any


class Journal:
    """Append-only JSONL journal for event persistence."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._seq = 0
        self._run_id = str(int(time.time() * 1000))
        self._file = None

    def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.path, "a")

    def close(self) -> None:
        if self._file:
            self._file.close()
            self._file = None

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def append(
        self,
        kind: str,
        payload: dict[str, Any],
        *,
        flush: bool = False,
        ts_ms: int | None = None,
    ) -> int:
        if self._file is None:
            raise RuntimeError("journal not open")
        self._seq += 1
        record = {
            "seq": self._seq,
            "run_id": self._run_id,
            "ts_ms": ts_ms if ts_ms is not None else int(time.time() * 1000),
            "kind": kind,
            "payload": payload,
        }
        self._file.write(json.dumps(record, ensure_ascii=False) + "\n")
        self._file.flush()
        if flush:
            os.fsync(self._file.fileno())
        return self._seq

    def append_critical(
        self,
        ts_ms: int,
        kind: str,
        payload: dict[str, Any],
    ) -> int:
        """Append a critical event with forced fsync (Rust V1: append_critical).

        Critical events (recovery state changes, risk mode transitions,
        operator commands) must be durable before the caller proceeds.
        """
        return self.append(kind, payload, flush=True, ts_ms=ts_ms)

    def read_all(self) -> list[dict[str, Any]]:
        """Read all journal records. Returns list of parsed dicts."""
        if not self.path.exists():
            return []
        records: list[dict[str, Any]] = []
        with open(self.path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        return records

    @property
    def seq(self) -> int:
        return self._seq

    @property
    def run_id(self) -> str:
        return self._run_id


# ---------------------------------------------------------------------------
# Journal replay (Rust V1: replay_bridge.rs)
# ---------------------------------------------------------------------------

def replay_journal_records(
    records: list[dict[str, Any]],
    seed_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Replay journal records to reconstruct engine state.

    Rust V1: replay_journal_records() in observability_ops/replay_bridge.rs
    processes each journal record and tracks:
    - Open positions (entry.opened → add, exit.closed/recovery.flat → remove)
    - Partial closes (exit.partial_closed → reduce quantity)
    - Lifecycle transitions (runtime.lifecycle_changed)
    - Risk mode transitions (runtime.risk_mode_changed)

    Returns a dict with:
    - open_position_count, open_position_ids
    - pending_entry_count, pending_close_count
    - final_lifecycle, final_risk_mode
    - positions (detail dict)
    """
    positions: dict[str, dict[str, Any]] = {}
    lifecycle = "booting"
    risk_mode = "running"
    open_ids: set[str] = set()

    # Apply seed state if provided
    if seed_state:
        lifecycle = seed_state.get("lifecycle", lifecycle)
        risk_mode = seed_state.get("risk_mode", risk_mode)
        seed_positions = seed_state.get("open_positions", {})
        if isinstance(seed_positions, dict):
            for pid, pdata in seed_positions.items():
                positions[pid] = dict(pdata) if isinstance(pdata, dict) else {}
                open_ids.add(pid)

    for record in records:
        kind = record.get("kind", "")
        payload = record.get("payload", {})

        if kind in ("entry.opened", "recovery.live_detected"):
            pid = payload.get("position_id", "")
            if pid:
                positions[pid] = dict(payload)
                open_ids.add(pid)

        elif kind in ("exit.closed", "exit.reconciled", "recovery.flat"):
            pid = payload.get("position_id", "")
            if pid and pid in positions:
                del positions[pid]
                open_ids.discard(pid)

        elif kind == "exit.partial_closed":
            pid = payload.get("position_id", "")
            if pid and pid in positions:
                if "quantity" in payload:
                    positions[pid]["quantity"] = payload["quantity"]
                if "current_net_quote" in payload:
                    positions[pid]["current_net_quote"] = payload["current_net_quote"]

        elif kind == "runtime.lifecycle_changed":
            to_val = payload.get("to")
            if to_val:
                lifecycle = str(to_val)

        elif kind == "runtime.risk_mode_changed":
            to_val = payload.get("to")
            if to_val:
                risk_mode = str(to_val)

    return {
        "open_position_count": len(open_ids),
        "open_position_ids": sorted(open_ids),
        "pending_entry_count": 0,  # journal alone can't distinguish
        "pending_close_count": 0,
        "final_lifecycle": lifecycle,
        "final_risk_mode": risk_mode,
        "positions": positions,
    }
