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

        V1: queries venue adapters for account risk snapshots, passes real
        supports_risk_health flags instead of hardcoded False (Fix 4).
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
            # Determine risk health support from venue adapters (Fix 4)
            long_adapter = self.get_venue_adapter(position.long_venue)
            short_adapter = self.get_venue_adapter(position.short_venue)

            long_supports = (
                long_adapter is not None and long_adapter.supports_risk_health
            )
            short_supports = (
                short_adapter is not None and short_adapter.supports_risk_health
            )

            # Try to fetch real risk snapshots
            import asyncio as _asyncio

            long_snapshot = None
            short_snapshot = None
            try:
                if long_supports and long_adapter is not None:
                    long_snapshot = await long_adapter.fetch_account_risk_snapshot()
                if short_supports and short_adapter is not None:
                    short_snapshot = await short_adapter.fetch_account_risk_snapshot()
            except Exception:
                # Risk snapshot fetch failure — treat as unsupported (V1: fail-closed by config)
                pass

            plan = self.supervisor.supervise_position(
                position, now_ms,
                long_supports_risk_health=long_supports,
                short_supports_risk_health=short_supports,
                long_snapshot=long_snapshot,
                short_snapshot=short_snapshot,
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
            await self._post_tick_housekeeping(now_ms)

            # --- Persist state snapshot ---
            self.snapshot_store.write(self.state.to_dict())

            # --- Sleep until next poll ---
            active_poll_ms = active_position_poll_interval_ms(
                self.state.lifecycle, poll_ms, active_count
            )
            await asyncio.sleep(min(poll_ms, active_poll_ms) / 1000.0)

    # ------------------------------------------------------------------
    # Reconciliation (V1 recovery/reconciliation live path — Fix 3)
    # ------------------------------------------------------------------

    async def _reconcile_pending_state(self, now_ms: int) -> None:
        """Process pending closes and pending entries through venue adapters.

        Rust V1: recovery.rs process_pending_close_reconciliations() and
        runtime_state pending reconciliation tick.
        """
        if self.reconciler is None or not self._venue_adapters:
            return

        # --- Process pending entries (uncertain maker/hedge orders) ---
        resolved_entry_ids: list[str] = []
        for entry_id, pending in list(self.state.pending_entries.items()):
            if not pending.uncertain_outcome:
                resolved_entry_ids.append(entry_id)
                continue

            try:
                result = await self.reconciler.reconcile_position(
                    position_id=entry_id,
                    symbol=pending.symbol,
                    long_venue=pending.long_venue,
                    short_venue=pending.short_venue,
                    long_order_id=pending.maker_order_id,
                    short_order_id=pending.hedge_order_id,
                )
            except Exception as e:
                self.journal.append(
                    "reconciliation.entry_reconcile_error",
                    {"entry_id": entry_id, "error": str(e)},
                )
                continue

            if result.long_status == "filled" and result.short_status == "filled":
                resolved_entry_ids.append(entry_id)
                self.journal.append(
                    "reconciliation.entry_resolved",
                    {"entry_id": entry_id, "long_status": result.long_status, "short_status": result.short_status},
                )
            elif result.is_flat:
                # Both sides flat — entry was likely never placed, clear it
                resolved_entry_ids.append(entry_id)
                self.journal.append(
                    "reconciliation.entry_cleared_flat",
                    {"entry_id": entry_id},
                )

        for eid in resolved_entry_ids:
            self.state.pending_entries.pop(eid, None)

        # --- Process pending closes ---
        resolved_ids: list[str] = []
        for close_id, pending in list(self.state.pending_closes.items()):
            if pending.long_uncertain or pending.short_uncertain:
                # Find the associated position for venue info
                pos = self.state.open_positions.get(pending.position_id)
                if pos is None:
                    # Position already gone — clear the pending close
                    resolved_ids.append(close_id)
                    self.journal.append(
                        "reconciliation.pending_close_orphaned",
                        {"close_id": close_id, "position_id": pending.position_id},
                    )
                    continue

                try:
                    result = await self.reconciler.reconcile_position(
                        position_id=pending.position_id,
                        symbol=pos.symbol,
                        long_venue=pos.long_venue,
                        short_venue=pos.short_venue,
                    )
                except Exception as e:
                    self.journal.append(
                        "reconciliation.reconcile_error",
                        {"close_id": close_id, "error": str(e)},
                    )
                    continue

                # If both legs are confirmed, resolve
                if result.is_flat:
                    resolved_ids.append(close_id)
                    self.state.open_positions.pop(pending.position_id, None)
                    self.journal.append(
                        "reconciliation.close_resolved_flat",
                        {"close_id": close_id, "position_id": pending.position_id},
                    )
                elif not pending.long_uncertain and not pending.short_uncertain:
                    resolved_ids.append(close_id)
                    self.journal.append(
                        "reconciliation.close_resolved",
                        {"close_id": close_id, "position_id": pending.position_id},
                    )
                # else: leave pending for next tick

        for cid in resolved_ids:
            self.state.pending_closes.pop(cid, None)

        # --- Transition out of RECONCILING if all work is done ---
        if (
            self.state.lifecycle == EngineLifecycle.RECONCILING
            and not self.state.pending_entries
            and not self.state.pending_closes
        ):
            from lightfee.engine.lifecycle import transition_to_running

            transition_to_running(self.state)
            self.journal.append(
                "runtime.reconciling_complete",
                {"reason": "all_pending_resolved", "ts_ms": now_ms},
            )

    # ------------------------------------------------------------------
    # Housekeeping
    # ------------------------------------------------------------------

    async def _post_tick_housekeeping(self, now_ms: int) -> None:
        """Run after every tick cycle: supervisor, reconciliation, periodic exports."""
        # Risk-line supervision
        self.supervisor.supervise(now_ms, self.state.venue_health)

        # Reconciliation of pending/uncertain outcomes
        await self._reconcile_pending_state(now_ms)

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
        """Transform a tradeable candidate into an entry context and execute via entry_executor.

        V1: entry route/maker-leg/price gate from config and execution planner.
        Fix 5: no 1.0 pseudo-price — reject entries without valid quote.
        Fix EN-001: route and maker leg driven by planner, not hardcoded in runtime.
        """
        from lightfee.core.domain import Side, Venue
        from lightfee.engine.entry import EntryContext, EntryType
        from lightfee.engine.execution_planner import (
            ExecutionRoute,
            plan_incremental_entry_execution,
        )

        # V1 price gate: require valid quote before constructing entry context
        if price_hint <= 0 or candidate.entry_notional_quote <= 0:
            self.journal.append(
                "runtime.entry_skipped_no_quote",
                {
                    "symbol": candidate.symbol,
                    "price_hint": price_hint,
                    "notional": candidate.entry_notional_quote,
                    "reason": "no valid quote to construct entry — V1 rejects",
                },
            )
            return

        # Resolve venue enums from candidate string fields
        long_venue = Venue.from_str(candidate.long_venue)
        short_venue = Venue.from_str(candidate.short_venue)
        quantity = candidate.entry_notional_quote / price_hint

        # V1 entry route planning: derive route and maker leg from execution planner.
        # Strategy config provides min-notional; venue-specific chunk/min-notional
        # are resolved from the adapter or spec when available.
        strategy = self.config.strategy
        min_notional = strategy.min_entry_leg_notional_quote
        min_hedgeable_chunk = min_notional / price_hint if price_hint > 0 else 0.0

        route, plan = plan_incremental_entry_execution(
            target_quantity=quantity,
            slice_ratio=0.5,
            min_hedgeable_chunk=min_hedgeable_chunk,
            maker_min_notional_quote=min_notional,
            maker_price_hint=price_hint if price_hint > 0 else None,
            max_initial_clip_ratio=0.8,
            hedge_min_notional_quote=min_notional,
            hedge_price_hint=price_hint if price_hint > 0 else None,
        )

        if route == ExecutionRoute.REJECTED:
            self.journal.append(
                "runtime.entry_skipped_planner_rejected",
                {
                    "symbol": candidate.symbol,
                    "target_quantity": quantity,
                    "reason": plan.reason or "planner rejected entry",
                },
            )
            return

        # Map planner route to EntryType
        if route == ExecutionRoute.PASSIVE_INCREMENTAL:
            entry_type = EntryType.PASSIVE_INCREMENTAL
            effective_quantity = plan.initial_maker_target_quantity
        elif route == ExecutionRoute.FALLBACK_TO_STANDARD:
            entry_type = EntryType.STANDARD_DUAL_TAKER
            effective_quantity = quantity
        else:
            entry_type = EntryType.STANDARD_DUAL_TAKER
            effective_quantity = quantity

        # maker_leg defaults to BUY (funding arb: long side is typically maker)
        maker_leg = Side.BUY

        ctx = EntryContext(
            entry_id=f"entry-{now_ms}-{candidate.symbol}",
            symbol=candidate.symbol,
            long_venue=long_venue,
            short_venue=short_venue,
            long_quantity=effective_quantity,
            short_quantity=effective_quantity,
            long_price_hint=price_hint,
            short_price_hint=price_hint,
            maker_leg=maker_leg,
            entry_type=entry_type,
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
