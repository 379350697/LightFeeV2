"""Live runtime: multi-lane tick loop, snapshot consumption, supervision, export."""

from __future__ import annotations

import asyncio
from typing import Optional

from lightfee.config.schema import AppConfig
from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import Venue
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

    def __init__(self, config: AppConfig, venue_adapters: Optional[dict[Venue, VenueAdapter]] = None) -> None:
        self.config = config
        self.state = EngineState()
        self.journal = Journal(config.persistence.event_log_path)
        self.snapshot_store = SnapshotStore(config.persistence.snapshot_path)
        self.supervisor = Supervisor(config, self.state, self.journal)
        self._running = False
        self._export_state = ExportState()
        self._venue_adapters = venue_adapters or {}

        # Tick-failure backoff deadlines (ms since epoch). None = no backoff active.
        self._tick_backoff_until_ms: Optional[int] = None
        self._active_tick_backoff_until_ms: Optional[int] = None

        # V1 entry executor — set after construction or defaults to None
        self.entry_executor: Optional[object] = None
        # V1 close executor — set after construction or defaults to None
        self.close_executor: Optional[object] = None
        # V1 reconciliation service — set after construction or defaults to None
        self.reconciler: Optional[object] = None

    def get_venue_adapter(self, venue: Venue) -> Optional[VenueAdapter]:
        return self._venue_adapters.get(venue)

    def get_venue_adapters(self) -> dict[Venue, VenueAdapter]:
        return dict(self._venue_adapters)

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

        # Phase 4 – Recovery-aware startup (Rust V1: finalize_startup_position_recovery)
        from lightfee.engine.recovery import needs_reconciliation, classify_startup_recovery_state

        recovery_class = classify_startup_recovery_state(self.state)

        if recovery_class == "clean":
            # No recovery work → safe to run immediately
            set_lifecycle(self.state, EngineLifecycle.RUNNING)
            self.journal.append(
                "runtime.running",
                {"reason": "startup_no_recovery_work", "ts_ms": wall_clock_now_ms()},
            )
        elif recovery_class == "recovery_needed":
            # Has open positions or pending work → stay in RECONCILING
            transition_to_reconciling(self.state)
            self.journal.append(
                "runtime.reconciling",
                {
                    "reason": "startup_recovery_required",
                    "open_positions": len(self.state.open_positions),
                    "pending_entries": len(self.state.pending_entries),
                    "pending_closes": len(self.state.pending_closes),
                    "ts_ms": wall_clock_now_ms(),
                },
            )
        else:
            # fail_closed — preserve the recovered state
            self.journal.append(
                "runtime.recovery_blocked",
                {
                    "reason": "startup_fail_closed",
                    "lifecycle": self.state.lifecycle.value,
                    "risk_mode": self.state.risk_mode.value,
                    "ts_ms": wall_clock_now_ms(),
                },
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

        # --- Build price lookup from snapshot quotes ---
        price_hints: dict[str, float] = {}
        for quote in snapshot.quotes:
            price_hints[quote.symbol] = (quote.bid + quote.ask) / 2.0 if quote.bid > 0 and quote.ask > 0 else 0.0

        # --- Discover tradeable candidates ---
        if can_enter_new_positions(self.state) and self.entry_executor is not None:
            tradeable = discover_tradeable_candidates(
                snapshot.candidates, self.config.strategy, now_ms
            )
            if tradeable:
                self.journal.append(
                    "runtime.candidates_tradeable",
                    {"count": len(tradeable), "ts_ms": now_ms},
                )
                # Dispatch first tradeable candidate to entry executor
                # (V1 policy: one new entry per tick to avoid correlated fills)
                mid_price = price_hints.get(tradeable[0].symbol, 0.0)
                await self._dispatch_entry(tradeable[0], now_ms, price_hint=mid_price)

    async def tick_active_positions(self) -> None:
        """Fast tick lane: active position monitoring with risk supervision.

        Evaluates risk for every open position and executes delever / protection
        plans when conditions are met. This is the primary close-driving path.
        """
        now_ms = wall_clock_now_ms()
        self.state.last_tick_ms = now_ms
        self.state.tick_count += 1

        if not self.state.open_positions:
            return

        self.journal.append(
            "runtime.active_position_tick",
            {"position_count": len(self.state.open_positions), "ts_ms": now_ms},
        )

        # --- Per-position risk supervision ---
        for position in list(self.state.open_positions.values()):
            plan = self.supervisor.supervise_position(
                position, now_ms,
                long_supports_risk_health=False,
                short_supports_risk_health=False,
            )
            if plan is not None:
                self.journal.append(
                    "runtime.risk_plan_generated",
                    {
                        "position_id": position.position_id,
                        "kind": plan.kind.value,
                        "reason": plan.reason,
                    },
                )
                await self.supervisor.execute_risk_plan(position, plan, now_ms)

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

    # ------------------------------------------------------------------
    # Entry dispatch
    # ------------------------------------------------------------------

    async def _dispatch_entry(self, candidate, now_ms: int, price_hint: float = 0.0) -> None:
        """Transform a tradeable candidate into an entry context and execute via entry_executor."""
        from lightfee.core.domain import Side, Venue
        from lightfee.engine.entry import EntryContext, EntryType

        # Resolve venue enums from candidate string fields
        long_venue = Venue.from_str(candidate.long_venue)
        short_venue = Venue.from_str(candidate.short_venue)
        # Derive base quantity from notional and price; use price_hint from snapshot if available
        effective_price = price_hint if price_hint > 0 else 1.0
        quantity = candidate.entry_notional_quote / effective_price if candidate.entry_notional_quote > 0 else 0.0

        ctx = EntryContext(
            entry_id=f"entry-{now_ms}-{candidate.symbol}",
            symbol=candidate.symbol,
            long_venue=long_venue,
            short_venue=short_venue,
            long_quantity=quantity,
            short_quantity=quantity,
            long_price_hint=effective_price,
            short_price_hint=effective_price,
            maker_leg=Side.BUY,
            entry_type=EntryType.STANDARD_DUAL_TAKER,
            created_at_ms=now_ms,
        )

        try:
            result = await self.entry_executor.execute(ctx)
            self.journal.append(
                "runtime.entry_dispatched",
                {
                    "entry_id": ctx.entry_id,
                    "symbol": candidate.symbol,
                    "route": result.route.value,
                    "state": result.state.value,
                },
            )
            if result.open_position is not None:
                self.state.open_positions[result.open_position.position_id] = result.open_position
                self.journal.append(
                    "runtime.position_opened",
                    {"position_id": result.open_position.position_id},
                )
        except Exception as e:
            self.journal.append(
                "runtime.entry_dispatch_error",
                {"entry_id": ctx.entry_id, "error": str(e)},
            )

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
