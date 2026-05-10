"""Live runtime: multi-lane tick loop, snapshot consumption, supervision, export."""

from __future__ import annotations

import asyncio
from typing import Optional

from lightfee.config.schema import AppConfig
from lightfee.engine.bootstrap import (
    active_position_poll_enabled,
    active_position_poll_interval_ms,
    active_position_tick_ready,
    full_tick_ready,
    prepare_runtime_symbols,
    wall_clock_now_ms,
)
from lightfee.engine.lifecycle import (
    can_enter_new_positions,
    set_lifecycle,
    transition_to_reconciling,
    transition_to_running,
)
from lightfee.engine.loop_control import (
    ExportState,
    maybe_export_current_state_snapshot,
    maybe_export_runtime_metrics,
)
from lightfee.engine.recovery import recover_from_snapshot
from lightfee.engine.state import EngineState
from lightfee.engine.supervisor import Supervisor
from lightfee.persistence.journal import Journal
from lightfee.persistence.snapshot_store import SnapshotStore
from lightfee.risk.modes import EngineLifecycle
from lightfee.sidecar.pairing import check_stale_snapshot
from lightfee.sidecar.publisher import load_snapshot
from lightfee.strategy.discovery import discover_tradeable_candidates


class LiveRuntime:
    """Live trading runtime with multi-lane ticks and control-plane exports."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.state = EngineState()
        self.journal = Journal(config.persistence.event_log_path)
        self.snapshot_store = SnapshotStore(config.persistence.snapshot_path)
        self.supervisor = Supervisor(config, self.state, self.journal)
        self._running = False
        self._export_state = ExportState()

        # Tick-failure backoff deadlines (ms since epoch). None = no backoff active.
        self._tick_backoff_until_ms: Optional[int] = None
        self._active_tick_backoff_until_ms: Optional[int] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Booting sequence: resolve symbols, recover state, reconcile, run."""
        self.journal.open()

        # Phase 1 – BOOTING
        set_lifecycle(self.state, EngineLifecycle.BOOTING)
        self.state.run_id = self.journal.run_id
        self.state.started_at_ms = wall_clock_now_ms()

        self.journal.append(
            "runtime.booting",
            {"run_id": self.state.run_id, "ts_ms": self.state.started_at_ms},
            flush=True,
        )

        # Phase 2 – Resolve runtime symbols (daily-universe integration point)
        await prepare_runtime_symbols(self.config)

        # Phase 3 – Recover or start fresh
        self.state = recover_from_snapshot(self.snapshot_store, self.journal)
        self.state.run_id = self.journal.run_id
        if self.state.started_at_ms == 0:
            self.state.started_at_ms = wall_clock_now_ms()

        # Phase 4 – RECONCILING → RUNNING
        if self.state.lifecycle in (EngineLifecycle.BOOTING, EngineLifecycle.RECONCILING):
            transition_to_reconciling(self.state)
            self.journal.append(
                "runtime.reconciling",
                {"reason": "startup_recovery", "ts_ms": wall_clock_now_ms()},
            )
            transition_to_running(self.state)
            self.journal.append(
                "runtime.running",
                {"ts_ms": wall_clock_now_ms()},
            )

        self.journal.append(
            "runtime.started",
            {
                "run_id": self.state.run_id,
                "lifecycle": self.state.lifecycle.value,
                "risk_mode": self.state.risk_mode.value,
            },
            flush=True,
        )

    async def stop(self) -> None:
        """Graceful shutdown: stop loop, export final state, flush journal."""
        self._running = False

        # Final state snapshot
        if self.state:
            self.snapshot_store.write(self.state.to_dict())

        # Final current-state export
        now_ms = wall_clock_now_ms()
        path = self.config.persistence.snapshot_path.replace(".json", "-current.json")
        maybe_export_current_state_snapshot(
            self.state, self.config, self._export_state, now_ms
        )

        self.journal.append("runtime.stopped", {"ts_ms": wall_clock_now_ms()})
        self.journal.close()

    # ------------------------------------------------------------------
    # Tick lanes
    # ------------------------------------------------------------------

    async def tick(self) -> None:
        """Full engine tick: consume snapshot, scan, supervise, manage positions."""
        now_ms = wall_clock_now_ms()
        self.state.last_tick_ms = now_ms
        self.state.tick_count += 1

        # --- Load sidecar snapshot ---
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

        # --- Discover tradeable candidates ---
        if can_enter_new_positions(self.state):
            tradeable = discover_tradeable_candidates(
                snapshot.candidates, self.config.strategy, now_ms
            )
            if tradeable:
                self.journal.append(
                    "runtime.candidates_tradeable",
                    {"count": len(tradeable), "ts_ms": now_ms},
                )

    async def tick_active_positions(self) -> None:
        """Fast tick lane: active position monitoring (lighter than full tick)."""
        now_ms = wall_clock_now_ms()
        self.state.last_tick_ms = now_ms
        self.state.tick_count += 1

        if not self.state.open_positions:
            return

        self.journal.append(
            "runtime.active_position_tick",
            {"position_count": len(self.state.open_positions), "ts_ms": now_ms},
        )

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run_loop(self) -> None:
        """Multi-lane tick loop with backoff, housekeeping, and periodic export."""
        self._running = True
        poll_ms = self.config.runtime.poll_interval_ms

        while self._running:
            now_ms = wall_clock_now_ms()
            active_count = len(self.state.open_positions)

            # --- Full tick lane (backoff-gated) ---
            if full_tick_ready(self._tick_backoff_until_ms, now_ms):
                try:
                    await self.tick()
                    self._tick_backoff_until_ms = None
                except Exception as e:
                    self._apply_tick_backoff(is_active=False)
                    self.journal.append("runtime.tick_error", {"error": str(e)})

            # --- Active-position fast tick lane ---
            if active_position_poll_enabled(
                self.state.lifecycle, poll_ms, active_count
            ):
                if active_position_tick_ready(
                    self._active_tick_backoff_until_ms, now_ms
                ):
                    try:
                        await self.tick_active_positions()
                        self._active_tick_backoff_until_ms = None
                    except Exception as e:
                        self._apply_tick_backoff(is_active=True)
                        self.journal.append(
                            "runtime.active_tick_error", {"error": str(e)}
                        )

            # --- Post-tick housekeeping ---
            self._post_tick_housekeeping(now_ms)

            # --- Persist state snapshot ---
            self.snapshot_store.write(self.state.to_dict())

            # --- Sleep until next poll ---
            active_poll_ms = active_position_poll_interval_ms(
                self.state.lifecycle, poll_ms, active_count
            )
            await asyncio.sleep(min(poll_ms, active_poll_ms) / 1000.0)

    # ------------------------------------------------------------------
    # Housekeeping
    # ------------------------------------------------------------------

    def _post_tick_housekeeping(self, now_ms: int) -> None:
        """Run after every tick cycle: supervisor, periodic exports."""
        # Risk-line supervision
        self.supervisor.supervise(now_ms, self.state.venue_health)

        # Periodic Prometheus & state exports
        maybe_export_runtime_metrics(
            self.state, self.config, self._export_state, now_ms
        )
        maybe_export_current_state_snapshot(
            self.state, self.config, self._export_state, now_ms
        )

    # ------------------------------------------------------------------
    # Backoff
    # ------------------------------------------------------------------

    def _apply_tick_backoff(self, is_active: bool) -> None:
        """Apply incremental tick-failure backoff from config floors / caps."""
        init_ms = self.config.runtime.tick_failure_backoff_initial_ms
        max_ms = self.config.runtime.tick_failure_backoff_max_ms

        if is_active:
            current = self._active_tick_backoff_until_ms
        else:
            current = self._tick_backoff_until_ms

        now_ms = wall_clock_now_ms()
        base_backoff = max(init_ms, (current - now_ms) * 2 if current and current > now_ms else init_ms)
        deadline_ms = now_ms + min(base_backoff, max_ms)

        if is_active:
            self._active_tick_backoff_until_ms = deadline_ms
        else:
            self._tick_backoff_until_ms = deadline_ms
