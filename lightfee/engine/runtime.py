"""Live runtime: main tick loop, snapshot consumption, supervision."""

from __future__ import annotations

import asyncio
import time

from lightfee.config.schema import AppConfig
from lightfee.engine.lifecycle import can_enter_new_positions, set_lifecycle
from lightfee.engine.recovery import recover_from_snapshot
from lightfee.engine.state import EngineState
from lightfee.persistence.journal import Journal
from lightfee.persistence.snapshot_store import SnapshotStore
from lightfee.risk.modes import EngineLifecycle
from lightfee.sidecar.pairing import check_stale_snapshot
from lightfee.sidecar.publisher import load_snapshot
from lightfee.sidecar.snapshot import CandidateInput
from lightfee.strategy.discovery import discover_tradeable_candidates


class LiveRuntime:
    """Live trading runtime: consumes sidecar snapshots, manages positions."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.state = EngineState()
        self.journal = Journal(config.persistence.event_log_path)
        self.snapshot_store = SnapshotStore(config.persistence.snapshot_path)
        self._running = False

    async def start(self) -> None:
        """Initialize runtime: open journal, recover state."""
        self.journal.open()
        self.state = recover_from_snapshot(self.snapshot_store, self.journal)
        self.state.run_id = self.journal.run_id
        self.state.started_at_ms = int(time.time() * 1000)

        self.journal.append(
            "runtime.started",
            {
                "run_id": self.state.run_id,
                "lifecycle": self.state.lifecycle.value,
                "risk_mode": self.state.risk_mode.value,
            },
            flush=True,
        )

        # Transition from booting to reconciling to running
        if self.state.lifecycle == EngineLifecycle.RECONCILING:
            self.journal.append("runtime.reconciling", {"reason": "startup_recovery"})
            # After successful reconciliation
            set_lifecycle(self.state, EngineLifecycle.RUNNING)
            self.journal.append("runtime.running", {})

    async def stop(self) -> None:
        self._running = False
        if self.state:
            self.snapshot_store.write(self.state.to_dict())
        self.journal.append("runtime.stopped", {})
        self.journal.close()

    async def tick(self) -> None:
        """Single tick: consume snapshot, scan, manage positions."""
        now_ms = int(time.time() * 1000)
        self.state.last_tick_ms = now_ms
        self.state.tick_count += 1

        # Load sidecar snapshot
        snapshot = load_snapshot(self.config.runtime.sidecar_snapshot_path)
        max_age = self.config.runtime.sidecar_snapshot_max_age_ms

        if snapshot is None:
            self.journal.append("runtime.snapshot_missing", {"ts_ms": now_ms})
            return

        if check_stale_snapshot(snapshot.published_at_ms, max_age, now_ms):
            self.journal.append(
                "runtime.snapshot_stale",
                {"published_at_ms": snapshot.published_at_ms, "max_age_ms": max_age},
            )
            return

        # Discover tradeable candidates
        if can_enter_new_positions(self.state):
            tradeable = discover_tradeable_candidates(
                snapshot.candidates, self.config.strategy, now_ms
            )
            if tradeable:
                self.journal.append(
                    "runtime.candidates_tradeable",
                    {"count": len(tradeable), "ts_ms": now_ms},
                )

    async def run_loop(self) -> None:
        """Main tick loop with configurable poll interval."""
        self._running = True
        while self._running:
            try:
                await self.tick()
                self.snapshot_store.write(self.state.to_dict())
            except Exception as e:
                self.journal.append("runtime.tick_error", {"error": str(e)})
            await asyncio.sleep(self.config.runtime.poll_interval_ms / 1000.0)
