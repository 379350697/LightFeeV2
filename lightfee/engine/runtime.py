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
from lightfee.engine.recovery import (
    recover_from_snapshot,
    build_recovery_dedup_index,
    is_client_order_id_duplicate,
    has_pending_entry_for_symbol,
)
from lightfee.engine.state import EngineState
from lightfee.engine.supervisor import Supervisor
from lightfee.persistence.journal import Journal
from lightfee.persistence.snapshot_store import SnapshotStore
from lightfee.risk.modes import EngineLifecycle
from lightfee.marketdata.l2 import L2BookStatus, L2PoolAssignment
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
        # V1 passive close executor — set after construction or defaults to None
        self.passive_close_executor: Optional[object] = None
        # V1 reconciliation service — set after construction or defaults to None
        self.reconciler: Optional[object] = None
        # V1 rate-limit runtime for periodic reload
        self._rate_limit_runtime: Optional[object] = None
        # V1 rate-limit reload tracking
        self._last_rate_limit_reload_ms: int = 0

        # V1 per-venue risk snapshot runtime cache
        #   key: venue → {fetched_at_ms, result: OK(Optional[ARS]) | Err(str)}
        self._risk_snapshot_cache: dict[Venue, dict] = {}

        # V1 maker-event lane state
        #   Tracks pending passive maker entries with last known price for repricing
        self._maker_event_state: dict[str, dict] = {}  # entry_id -> {maker_price, last_reprice_ms, consecutive_failures}
        self._last_maker_event_ms: int = 0

        # V1 local-L2 runtime (data-plane: book, assignment, events, metrics)
        from lightfee.marketdata.local_l2_runtime import LocalL2Runtime
        self.local_l2_runtime = LocalL2Runtime()

        # V1 local-L2 data plane (REST snapshot bootstrap + WS streaming)
        from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
        self.l2_data_plane = LocalL2DataPlane(
            l2_runtime=self.local_l2_runtime,
            journal=self.journal,
        )

        # V1 entry-local-L2 session runtime (tracked opportunities, readiness)
        from lightfee.engine.entry_local_l2 import EntryLocalL2SessionRuntime
        self.entry_l2_sessions = EntryLocalL2SessionRuntime()

        # V1 recovery dedup index: prevents duplicate orders after restart
        self._recovery_dedup_index: dict[str, str] = {}

    # V1 risk snapshot TTL constants (Rust: execution_core/engine.rs:127, risk.rs:12)
    _RISK_SNAPSHOT_TTL_MS_DEFAULT = 1_000
    _RISK_SNAPSHOT_TTL_MS_ASTER = 30_000  # Aster lacks WS, avoid REST polling

    @staticmethod
    def _risk_snapshot_ttl_ms(venue: Venue) -> int:
        if venue == Venue.ASTER:
            return LiveRuntime._RISK_SNAPSHOT_TTL_MS_ASTER
        return LiveRuntime._RISK_SNAPSHOT_TTL_MS_DEFAULT

    def get_venue_adapter(self, venue: Venue) -> Optional[VenueAdapter]:
        return self._venue_adapters.get(venue)

    def get_venue_adapters(self) -> dict[Venue, VenueAdapter]:
        return dict(self._venue_adapters)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Booting sequence: phased private→market→local-L2 startup (V1 parity)."""
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

        # Build recovery dedup index from recovered pending state
        self._recovery_dedup_index = build_recovery_dedup_index(self.state)

        # Phase 4 – Recovery-aware startup (Rust V1: finalize_startup_position_recovery)
        from lightfee.engine.recovery import needs_reconciliation, classify_startup_recovery_state

        recovery_class = classify_startup_recovery_state(self.state)

        if recovery_class == "clean":
            set_lifecycle(self.state, EngineLifecycle.RUNNING)
            self.journal.append(
                "runtime.running",
                {"reason": "startup_no_recovery_work", "ts_ms": wall_clock_now_ms()},
            )
        elif recovery_class == "recovery_needed":
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
            self.journal.append(
                "runtime.recovery_blocked",
                {
                    "reason": "startup_fail_closed",
                    "lifecycle": self.state.lifecycle.value,
                    "risk_mode": self.state.risk_mode.value,
                    "ts_ms": wall_clock_now_ms(),
                },
            )

        # Phase 5 – Local-L2 startup activation (V1: local-L2 phased activation)
        await self._activate_local_l2_phase(wall_clock_now_ms())

        # Phase 6 – Recover retained local-L2 state
        await self._restore_local_l2_state()

        # Phase 7 – Instantiate passive close executor
        if self._venue_adapters:
            from lightfee.engine.passive_close import PassiveCloseExecutor
            self.passive_close_executor = PassiveCloseExecutor(
                adapters=self._venue_adapters,
                journal=self.journal,
            )
            # Inject the L2 mid resolver so repricing has live book data
            self.passive_close_executor.set_l2_mid_resolver(self._resolve_local_l2_mid)
            # Inject close executor for DUAL_TAKER fallback
            if self.close_executor is not None:
                self.passive_close_executor.set_close_executor(self.close_executor)

        # Phase 8 – Recover pending passive closes
        await self._recover_passive_closes()

        self.journal.append(
            "runtime.started",
            {
                "run_id": self.state.run_id,
                "lifecycle": self.state.lifecycle.value,
                "risk_mode": self.state.risk_mode.value,
            },
            flush=True,
        )

    async def _activate_local_l2_phase(self, now_ms: int) -> None:
        """Phase 5: Bootstrap local-L2 books with real REST snapshot data.

        Mirrors Rust V1 local-L2 phased startup:
        - Derives target (venue, symbol) set from config.venues × config.symbols
        - Creates a LocalL2Book for each target pair with venue rules
        - Fetches real REST snapshots via the data plane for each book
        - Transitions to HOT on successful bootstrap, DEGRADED on failure
        - Respects live_startup_phase_timeout_ms
        - Degraded/fail-closed on timeout
        """
        self.journal.append(
            "runtime.local_l2_phase_start",
            {"ts_ms": now_ms},
        )

        timeout_ms = self.config.runtime.live_startup_phase_timeout_ms
        books_bootstrapped = 0
        books_failed = 0
        books_skipped = 0
        deadline_ms = now_ms + timeout_ms

        # Build target (venue, symbol) set from config
        target_pairs: set[tuple[str, str]] = set()
        if self.config.strategy.local_l2_enabled:
            from lightfee.core.domain import Venue as VenueEnum
            # Use configured venues from the venue adapters
            active_venues = list(self._venue_adapters.keys())
            for venue in active_venues:
                venue_str = venue.value if hasattr(venue, 'value') else str(venue)
                for symbol in self.config.symbols:
                    target_pairs.add((venue_str, symbol))

        if not target_pairs:
            self.journal.append(
                "runtime.local_l2_phase_complete",
                {
                    "books_bootstrapped": 0,
                    "reason": "no target pairs — local_l2 disabled or no venues/symbols",
                    "phase_ms": wall_clock_now_ms() - now_ms,
                },
            )
            return

        from lightfee.marketdata.local_l2_venues import get_venue_rules
        from lightfee.core.domain import Venue as VenueEnum

        for venue_str, symbol in sorted(target_pairs):
            if wall_clock_now_ms() > deadline_ms:
                self.journal.append(
                    "runtime.local_l2_phase_timeout",
                    {"ts_ms": wall_clock_now_ms(), "deadline_ms": deadline_ms,
                     "remaining_pairs": len(target_pairs) - (books_bootstrapped + books_failed + books_skipped)},
                )
                break

            try:
                rules = get_venue_rules(venue_str)
                book = self.local_l2_runtime.ensure_book(venue_str, symbol)
                book.max_depth = rules.default_depth
                book.max_sequence_gap = rules.max_sequence_gap

                if book.status == L2BookStatus.COLD:
                    book.transition_to_bootstrapping(now_ms)
                    ven = VenueEnum.from_str(venue_str)
                    adapter = self.get_venue_adapter(ven)
                    if adapter is None:
                        book.transition_to_degraded("no adapter available during startup")
                        books_failed += 1
                        continue

                    # Attempt real REST snapshot bootstrap via adapter's public interface
                    if adapter is None or not hasattr(adapter, 'fetch_l2_snapshot'):
                        book.transition_to_degraded("no adapter fetch_l2_snapshot during startup")
                        books_failed += 1
                        continue

                    if self.config.runtime.mode == "paper":
                        book.transition_to_hot()
                        books_bootstrapped += 1
                        continue

                    success = await self.l2_data_plane.bootstrap_book(
                        venue=venue_str,
                        symbol=symbol,
                        adapter=adapter,
                        depth=rules.default_depth,
                        now_ms=wall_clock_now_ms(),
                    )
                    if success:
                        book.transition_to_hot()
                        books_bootstrapped += 1
                    else:
                        book.transition_to_degraded("REST snapshot bootstrap failed")
                        books_failed += 1
                elif book.status == L2BookStatus.RESUME_WAITING:
                    books_skipped += 1
                elif book.status == L2BookStatus.HOT:
                    books_bootstrapped += 1
            except Exception as e:
                self.journal.append(
                    "runtime.local_l2_book_error",
                    {"venue": venue_str, "symbol": symbol, "error": str(e)},
                )
                books_failed += 1

        # Restore retained books from previous state
        if hasattr(self.state, "retained_local_l2_books"):
            for entry in getattr(self.state, "retained_local_l2_books", []):
                venue = entry.get("venue", "")
                sym = entry.get("symbol", "")
                if venue and sym:
                    book = self.local_l2_runtime.ensure_book(venue, sym)
                    if book.status == L2BookStatus.COLD:
                        book.pool = L2PoolAssignment.RETAINED
                        book.transition_to_bootstrapping(now_ms)
                        books_skipped += 1

        within_deadline = wall_clock_now_ms() <= deadline_ms
        self.journal.append(
            "runtime.local_l2_phase_complete",
            {
                "books_bootstrapped": books_bootstrapped,
                "books_failed": books_failed,
                "books_skipped": books_skipped,
                "target_pairs": len(target_pairs),
                "phase_ms": wall_clock_now_ms() - now_ms,
                "timeout_ms": timeout_ms,
                "within_deadline": within_deadline,
                "fail_closed": not within_deadline and books_bootstrapped == 0,
            },
        )

        # Start WebSocket L2 delta streams for HOT books (V1: WS sessions post-bootstrap)
        if (
            self.config.strategy.local_l2_enabled
            and getattr(self.config.strategy, 'local_l2_ws_enabled', False)
            and books_bootstrapped > 0
        ):
            await self._start_local_l2_ws_streams()

    async def _start_local_l2_ws_streams(self) -> None:
        """Start WebSocket L2 delta streams for books that are HOT after bootstrap.

        Groups symbols by venue and starts WS clients per venue/symbol pair.
        Passes venue adapter for poller-based venues (Hyperliquid).
        Books with active WS streaming get reduced REST refresh frequency.
        """
        from lightfee.core.domain import Venue as VenueEnum

        venue_symbols: dict[str, list[str]] = {}
        for book in self.local_l2_runtime.books.values():
            if book.status == L2BookStatus.HOT:
                venue_symbols.setdefault(book.venue, []).append(book.symbol)

        total_started = 0
        for venue_str, symbols in venue_symbols.items():
            # Resolve adapter for poller-based venues (Hyperliquid)
            adapter = None
            try:
                ven = VenueEnum.from_str(venue_str)
                adapter = self.get_venue_adapter(ven)
            except (ValueError, KeyError):
                pass

            registered = self.l2_data_plane.start_ws_streams(
                venue_str, symbols, adapter=adapter,
            )
            total_started += registered

        # Actually connect the registered WS clients from async context
        if total_started > 0:
            connected = await self.l2_data_plane.connect_ws_streams()
            self.journal.append(
                "runtime.local_l2_ws_started",
                {
                    "stream_count": total_started,
                    "connected": connected,
                    "venues": sorted(venue_symbols.keys()),
                    "ts_ms": wall_clock_now_ms(),
                },
            )

    async def _restore_local_l2_state(self) -> None:
        """Phase 6: Restore retained local-L2 books and session state from snapshot."""
        if not hasattr(self.state, "local_l2_books_snapshot"):
            return
        snap = getattr(self.state, "local_l2_books_snapshot", None)
        if not snap:
            return
        for entry in snap:
            venue = entry.get("venue", "")
            symbol = entry.get("symbol", "")
            if not venue or not symbol:
                continue
            book = self.local_l2_runtime.ensure_book(venue, symbol)
            book.last_update_id = entry.get("last_update_id", 0)
            book.sequence = entry.get("sequence", 0)
            book.last_snapshot_ms = entry.get("last_snapshot_ms", 0)
            book.last_delta_ms = entry.get("last_delta_ms", 0)
            book.pool = L2PoolAssignment.RETAINED
            # Restored book is never automatically HOT — must prove freshness
            book.status = L2BookStatus.RESUME_WAITING

    async def stop(self) -> None:
        """Graceful shutdown: stop loop, WS clients, adapter shutdown, export final state, flush journal."""
        self._running = False

        # Stop WebSocket L2 streams (V1: abort workers before adapter shutdown)
        await self.l2_data_plane.stop_ws_streams()

        # V1 parity: per-adapter shutdown (cancels workers, flushes state)
        for venue, adapter in list(self._venue_adapters.items()):
            try:
                await adapter.shutdown()
            except Exception as e:
                self.journal.append(
                    "runtime.adapter_shutdown_error",
                    {"venue": venue.value, "error": str(e)},
                )

        # Rate-limit runtime flush
        if self._rate_limit_runtime is not None:
            try:
                self._rate_limit_runtime.flush_recommendations()
            except Exception:
                pass

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
        for quote in snapshot.quotes.values():
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

    # ------------------------------------------------------------------
    # Risk snapshot runtime cache (V1: fetch_account_risk_with_runtime_cache)
    # ------------------------------------------------------------------

    def _cached_risk_snapshot(self, venue: Venue, now_ms: int):
        """Return cached (result, was_cached) or (None, False) if stale/missing.

        V1: cached_runtime_risk_snapshot() — checks freshness against
        venue-specific TTL (1s default, 30s for Aster to avoid REST polling).
        """
        entry = self._risk_snapshot_cache.get(venue)
        if entry is None:
            return None, False
        fetched_at = entry.get("fetched_at_ms", 0)
        ttl = self._risk_snapshot_ttl_ms(venue)
        if ttl <= 0 or (now_ms - fetched_at) > ttl:
            return None, False
        return entry.get("result"), True

    def _store_risk_snapshot(self, venue: Venue, now_ms: int, result) -> None:
        """Store a risk snapshot fetch result in the per-venue cache.

        V1: store_runtime_risk_snapshot() — stores Ok(snapshot), Ok(None),
        or Err(error_string) with fetched_at_ms.
        """
        self._risk_snapshot_cache[venue] = {
            "fetched_at_ms": now_ms,
            "result": result,
        }

    async def _fetch_venue_risk_snapshot(
        self, venue: Venue, adapter, supports: bool, now_ms: int,
    ):
        """Fetch venue risk snapshot with runtime cache.

        V1: fetch_account_risk_with_runtime_cache().
        Returns (snapshot_or_none, supports_still_valid).

        Cache stores: Ok(snapshot), Ok(None=unsupported/missing), or Err(str).
        Failed fetches are cached to avoid retry storms; same-tick same-venue
        calls share the cached result.
        """
        if not supports or adapter is None:
            return None, supports

        # Check cache first
        cached_result, was_cached = self._cached_risk_snapshot(venue, now_ms)
        if was_cached:
            if isinstance(cached_result, tuple) and len(cached_result) == 2:
                # (ok=True, snapshot) or (ok=False, error_string)
                ok, val = cached_result
                if ok:
                    return val, True
                else:
                    # Error was already journaled on original fetch.
                    # Keep supports=True — a fetch error means snapshot_unavailable,
                    # NOT capability_unsupported (V1: venue capability unchanged).
                    return None, True
            # Legacy: direct snapshot stored
            return cached_result, True

        # Cache miss — fetch from adapter
        try:
            snapshot = await adapter.fetch_account_risk_snapshot()
            self._store_risk_snapshot(venue, now_ms, (True, snapshot))
            return snapshot, True
        except Exception as e:
            error_str = str(e)
            self.journal.append(
                "runtime.risk_snapshot_fetch_error",
                {"venue": venue.value, "error": error_str},
            )
            self._store_risk_snapshot(venue, now_ms, (False, error_str))
            # Fetch error → snapshot unavailable, but capability (supports) unchanged.
            # V1: venue supports_risk_health is independent of transient fetch errors.
            return None, True

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

            # Fetch real risk snapshots with runtime cache (V1: per-venue TTL)
            long_snapshot, long_supports = await self._fetch_venue_risk_snapshot(
                position.long_venue, long_adapter, long_supports, now_ms,
            )
            short_snapshot, short_supports = await self._fetch_venue_risk_snapshot(
                position.short_venue, short_adapter, short_supports, now_ms,
            )

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
    # Rate-limit reload (V1: rate_limit_reload_interval)
    # ------------------------------------------------------------------

    _RATE_LIMIT_RELOAD_INTERVAL_MS = 30_000

    async def _maybe_reload_rate_limits(self, now_ms: int) -> None:
        """Periodic rate-limit config reload (V1: rate_limit_reload_interval).

        Reloads rate_limits.toml every _RATE_LIMIT_RELOAD_INTERVAL_MS if the
        config file has changed. Also flushes pending recommendation events.
        """
        if self._rate_limit_runtime is None:
            return
        if now_ms < self._last_rate_limit_reload_ms + self._RATE_LIMIT_RELOAD_INTERVAL_MS:
            return
        self._last_rate_limit_reload_ms = now_ms
        try:
            await self._rate_limit_runtime.refresh(now_ms)
            self._rate_limit_runtime.flush_recommendations()
        except Exception as e:
            self.journal.append(
                "runtime.rate_limit_reload_error", {"error": str(e)}
            )

    # ------------------------------------------------------------------
    # Local-L2 data sync (V1: periodic snapshot refresh per book)
    # ------------------------------------------------------------------

    async def _sync_local_l2_data(self, now_ms: int) -> None:
        """Periodic snapshot refresh for local-L2 books without WS streaming.

        Called each tick. Delegates to the data plane which respects per-book
        cooldown intervals and only refreshes books that need it (COLD,
        BOOTSTRAPPING, REBUILDING with priority; HOT at slower interval).
        """
        if not self.config.strategy.local_l2_enabled:
            return

        try:
            dispatched = await self.l2_data_plane.sync_snapshots(
                adapters=self._venue_adapters,
                now_ms=now_ms,
            )
            if dispatched > 0:
                self.local_l2_runtime.sync(now_ms)
        except Exception as e:
            self.journal.append(
                "runtime.local_l2_sync_error",
                {"error": str(e), "ts_ms": now_ms},
            )

    # ------------------------------------------------------------------
    # Maker-event lane (V1: maker_event_interval)
    # ------------------------------------------------------------------

    async def _maybe_tick_maker_event(self, now_ms: int) -> None:
        """V1 maker-event lane: repricing and cancel-replace for passive maker orders.

        V1 (Rust: engine.rs tick_maker_event_lane):
        - Syncs local-L2 runtime (expire leases, refresh metrics, drain events)
        - Filters events to those matching pending entry hedges
        - Calls drive_pending_entry_hedge() for repricing/cancel-replace

        Two modes:
        1. local-L2 mode (parity): driven by local-L2 book events
        2. sidecar-mid fallback (non-parity): driven by snapshot mid-price moves
        """
        if not self.config.runtime.maker_event_lane_enabled:
            self._maker_event_state.clear()
            return

        # Min wake interval gating
        min_interval = self.config.runtime.maker_event_lane_min_wake_interval_ms
        if self._last_maker_event_ms > 0 and (now_ms - self._last_maker_event_ms) < min_interval:
            return

        # Only process when there are pending entries with passive maker legs
        pending_passive = [
            (eid, pe) for eid, pe in self.state.pending_entries.items()
            if pe.entry_type and "passive" in str(pe.entry_type).lower()
        ]
        if not pending_passive:
            return

        local_l2_enabled = self.config.strategy.local_l2_enabled

        if local_l2_enabled:
            # --- Parity mode: local-L2 event-driven ---
            await self._maybe_tick_maker_event_local_l2(now_ms, pending_passive)
        else:
            # --- Non-parity fallback: sidecar mid-price ---
            await self._maybe_tick_maker_event_sidecar(now_ms, pending_passive)

    async def _maybe_tick_maker_event_local_l2(
        self, now_ms: int, pending_passive: list,
    ) -> None:
        """Local-L2 parity maker-event lane: sync runtime, drain events, drive hedges."""
        # Sync local-L2 runtime
        events = self.local_l2_runtime.sync(now_ms)

        # Build set of (venue, symbol) that matter to pending entries
        pending_venues_symbols: set[tuple[str, str]] = set()
        for entry_id, pending in pending_passive:
            pending_venues_symbols.add((pending.long_venue.value, pending.symbol))
            pending_venues_symbols.add((pending.short_venue.value, pending.symbol))

        # Filter events to those matching pending entries
        matching_events = [
            e for e in events
            if (e.venue, e.symbol) in pending_venues_symbols
        ]

        if not matching_events:
            # V1 parity mode: no auto sidecar fallback when local_l2_enabled=True.
            # When no matching local-L2 events exist, journal the reason and return.
            # Sidecar-mid is only reachable via explicit sidecar mode (local_l2_enabled=False).
            self.journal.append(
                "runtime.maker_event_no_local_l2_events",
                {
                    "ts_ms": now_ms,
                    "pending_venues_symbols": sorted(
                        f"{v}:{s}" for v, s in pending_venues_symbols
                    ),
                    "event_count": len(events),
                    "reason": "no matching local-L2 events for pending entries",
                },
            )
            return

        strategy = self.config.strategy
        reprice_threshold_bps = strategy.passive_reprice_threshold_bps
        cancel_replace_threshold_bps = strategy.passive_cancel_replace_threshold_bps

        woke_positions = 0
        event_kinds: set[str] = set()
        wake_reasons: set[str] = set()
        min_event_age_ms = 1_000_000_000
        max_event_age_ms = 0
        venues: set[str] = set()

        for entry_id, pending in pending_passive:
            # Check if any matching event involves this entry's venues
            entry_venues = {(pending.long_venue.value, pending.symbol),
                          (pending.short_venue.value, pending.symbol)}
            relevant = [e for e in matching_events if (e.venue, e.symbol) in entry_venues]
            if not relevant:
                continue

            # Get current mid price from local-L2 books
            long_book = self.local_l2_runtime.get_book(pending.long_venue.value, pending.symbol)
            short_book = self.local_l2_runtime.get_book(pending.short_venue.value, pending.symbol)

            long_mid = long_book.mid_price() if long_book else 0.0
            short_mid = short_book.mid_price() if short_book else 0.0
            mid = long_mid if long_mid > 0 else short_mid
            if mid <= 0:
                continue

            # Cooldown check
            est = self._maker_event_state.get(entry_id, {})
            last_reprice_ms = est.get("last_reprice_ms", 0)
            cooldown_ms = strategy.passive_failure_cooldown_ms
            if last_reprice_ms > 0 and (now_ms - last_reprice_ms) < cooldown_ms:
                continue

            # Consecutive failures check
            failures = est.get("consecutive_failures", 0)
            if failures >= strategy.passive_max_consecutive_failures:
                continue

            stored_price = est.get("maker_price", 0.0)
            if stored_price <= 0:
                self._maker_event_state[entry_id] = {
                    "maker_price": mid,
                    "last_reprice_ms": now_ms,
                    "consecutive_failures": 0,
                }
                continue

            price_move_bps = abs(mid - stored_price) / stored_price * 10000
            if price_move_bps >= cancel_replace_threshold_bps:
                action = "cancel_replace"
            elif price_move_bps >= reprice_threshold_bps:
                action = "reprice"
            else:
                continue

            if self.entry_executor is None:
                continue

            # Collect event metadata
            for e in relevant:
                event_kinds.add(e.event_kind.value)
                age = now_ms - e.observed_at_ms
                min_event_age_ms = min(min_event_age_ms, age)
                max_event_age_ms = max(max_event_age_ms, age)
                venues.add(e.venue)
                if e.wake_reason:
                    wake_reasons.add(e.wake_reason)

            try:
                result = await self._reprice_passive_maker_l2(
                    pending, mid, stored_price, action, now_ms, entry_id,
                )
                # Update runtime tracker
                self._maker_event_state[entry_id] = {
                    "maker_price": mid,
                    "last_reprice_ms": now_ms,
                    "consecutive_failures": 0,
                }
                # Write back to authoritative PendingEntry state
                pe = self.state.pending_entries.get(entry_id)
                if pe is not None:
                    pe.maker_price = mid
                    if result.order_id:
                        pe.maker_order_id = result.order_id
                woke_positions += 1
            except Exception as e:
                self._maker_event_state[entry_id] = {
                    "maker_price": stored_price,
                    "last_reprice_ms": now_ms,
                    "consecutive_failures": failures + 1,
                }
                self.journal.append(
                    "runtime.maker_event_reprice_error",
                    {"entry_id": entry_id, "action": action, "error": str(e)},
                )

        self._last_maker_event_ms = now_ms
        self.local_l2_runtime.metrics.maker_event_lane_wake_total += 1
        self.journal.append(
            "execution.maker_event_lane_wake",
            {
                "event_count": len(matching_events),
                "position_count": woke_positions,
                "symbols": list({p[1].symbol for p in pending_passive}),
                "event_kinds": sorted(event_kinds),
                "wake_reasons": sorted(wake_reasons) if wake_reasons else ["local_l2_event"],
                "min_event_age_ms": min_event_age_ms if min_event_age_ms < 1_000_000_000 else 0,
                "max_event_age_ms": max_event_age_ms,
                "venues": sorted(venues),
                "ts_ms": now_ms,
            },
        )

    async def _maybe_tick_maker_event_sidecar(
        self, now_ms: int, pending_passive: list,
    ) -> None:
        """Non-parity fallback: sidecar mid-price driven maker repricing."""
        from lightfee.sidecar.publisher import load_snapshot as _load_snap

        snapshot = _load_snap(self.config.runtime.sidecar_snapshot_path)
        if snapshot is None:
            return

        price_hints: dict[str, float] = {}
        for quote in snapshot.quotes.values():
            if quote.bid > 0 and quote.ask > 0:
                price_hints[quote.symbol] = (quote.bid + quote.ask) / 2.0

        strategy = self.config.strategy
        reprice_threshold_bps = strategy.passive_reprice_threshold_bps
        cancel_replace_threshold_bps = strategy.passive_cancel_replace_threshold_bps
        cooldown_ms = strategy.passive_failure_cooldown_ms
        max_failures = strategy.passive_max_consecutive_failures

        woke_positions = 0
        for entry_id, pending in pending_passive:
            mid = price_hints.get(pending.symbol, 0.0)
            if mid <= 0:
                continue

            est = self._maker_event_state.get(entry_id, {})
            last_reprice_ms = est.get("last_reprice_ms", 0)
            if last_reprice_ms > 0 and (now_ms - last_reprice_ms) < cooldown_ms:
                continue

            failures = est.get("consecutive_failures", 0)
            if failures >= max_failures:
                continue

            stored_price = est.get("maker_price", 0.0)
            if stored_price <= 0:
                self._maker_event_state[entry_id] = {
                    "maker_price": mid,
                    "last_reprice_ms": now_ms,
                    "consecutive_failures": 0,
                }
                continue

            price_move_bps = abs(mid - stored_price) / stored_price * 10000

            if price_move_bps >= cancel_replace_threshold_bps:
                action = "cancel_replace"
            elif price_move_bps >= reprice_threshold_bps:
                action = "reprice"
            else:
                continue

            if self.entry_executor is None:
                continue

            try:
                await self._reprice_passive_maker(
                    pending, mid, stored_price, action, now_ms, entry_id,
                )
                self._maker_event_state[entry_id] = {
                    "maker_price": mid,
                    "last_reprice_ms": now_ms,
                    "consecutive_failures": 0,
                }
                woke_positions += 1
            except Exception as e:
                self._maker_event_state[entry_id] = {
                    "maker_price": stored_price,
                    "last_reprice_ms": now_ms,
                    "consecutive_failures": failures + 1,
                }
                self.journal.append(
                    "runtime.maker_event_reprice_error",
                    {"entry_id": entry_id, "action": action, "error": str(e)},
                )

        self._last_maker_event_ms = now_ms
        if woke_positions > 0:
            self.journal.append(
                "runtime.maker_event_lane_wake",
                {
                    "position_count": woke_positions,
                    "pending_passive_total": len(pending_passive),
                    "source": "sidecar_mid",
                    "ts_ms": now_ms,
                },
            )

    async def _reprice_passive_maker(
        self, pending, new_price: float, old_price: float,
        action: str, now_ms: int, entry_id: str,
    ) -> None:
        """Reprice a passive maker order — sidecar path (non-parity fallback).

        Uses entry_executor.execute() for the non-parity sidecar-mid path.
        Local-L2 parity mode uses _reprice_passive_maker_l2() instead.
        """
        from lightfee.core.domain import Side
        from lightfee.engine.entry import EntryContext, EntryType

        maker_leg = Side.BUY if self.config.strategy.maker_leg_default == "buy" else Side.SELL

        ctx = EntryContext(
            entry_id=entry_id,
            symbol=pending.symbol,
            long_venue=pending.long_venue,
            short_venue=pending.short_venue,
            long_quantity=pending.long_quantity,
            short_quantity=pending.short_quantity,
            long_price_hint=new_price,
            short_price_hint=new_price,
            maker_leg=maker_leg,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
            created_at_ms=now_ms,
            parent_entry_id=entry_id,
            reprice_action=action,
        )
        await self.entry_executor.execute(ctx)
        self.journal.append(
            "runtime.maker_event_reprice",
            {
                "entry_id": entry_id,
                "action": action,
                "old_price": old_price,
                "new_price": new_price,
            },
        )

    async def _reprice_passive_maker_l2(
        self, pending, new_price: float, old_price: float,
        action: str, now_ms: int, entry_id: str,
    ) -> HedgeDriveResult:
        """Reprice a passive maker order using the V1 in-situ hedge driver.

        Calls drive_pending_entry_hedge() which amends or cancel-replaces
        the EXISTING maker order. Does NOT call entry_executor.execute()
        and does NOT create a new entry flow or submit a new hedge.

        V1: drive_pending_entry_hedge() — in-situ driver for pending entry hedge.
        Only used in local-L2 parity mode (local_l2_enabled=True).

        Returns HedgeDriveResult so the caller can write back to PendingEntry state.
        """
        from lightfee.core.domain import Side
        from lightfee.engine.entry_sync import drive_pending_entry_hedge, HedgeDriveResult

        maker_leg = Side.BUY if self.config.strategy.maker_leg_default == "buy" else Side.SELL

        result = await drive_pending_entry_hedge(
            entry_id=entry_id,
            pending=pending,
            new_price=new_price,
            old_price=old_price,
            action=action,
            now_ms=now_ms,
            adapters=self._venue_adapters,
            journal=self.journal,
            maker_leg=maker_leg,
            symbol=pending.symbol,
            long_venue=pending.long_venue,
            short_venue=pending.short_venue,
        )

        if result.outcome in ("applied", "uncertain"):
            self.journal.append(
                "runtime.maker_event_reprice",
                {
                    "entry_id": entry_id,
                    "action": action,
                    "old_price": old_price,
                    "new_price": new_price,
                    "outcome": result.outcome,
                    "order_id": result.order_id,
                },
            )

        if result.outcome == "rejected":
            raise RuntimeError(f"hedge drive rejected: {result.detail}")

        return result

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

            # --- Rate-limit periodic reload (V1: rate_limit_reload_interval) ---
            await self._maybe_reload_rate_limits(now_ms)

            # --- Local-L2 snapshot refresh (periodic REST bootstrap for books) ---
            await self._sync_local_l2_data(now_ms)

            # --- Passive close lane (V1: process_pending_passive_closes) ---
            await self._maybe_tick_passive_close(now_ms)

            # --- Normal exit lane (V1: standard_close_reason → passive/aggressive routing) ---
            await self._maybe_process_normal_exits(now_ms)

            # --- Maker-event lane (V1: maker_event_interval, optional) ---
            await self._maybe_tick_maker_event(now_ms)

            # --- Post-tick housekeeping ---
            await self._post_tick_housekeeping(now_ms)

            # --- Snapshot local-L2 state for persistence ---
            self._snapshot_local_l2_state()

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

    # V1 reconciliation retry constants (Rust V1 recovery.rs)
    _RECONCILE_RETRY_BASE_MS = 30_000
    _RECONCILE_RETRY_MAX_MS = 300_000
    _RECONCILE_HARD_DEADLINE_MS = 600_000  # 10 min hard deadline

    async def _reconcile_pending_state(self, now_ms: int) -> None:
        """Process pending closes and pending entries through venue adapters.

        Rust V1: recovery.rs process_pending_close_reconciliations() with
        exponential backoff (base 30s, max 300s) and hard deadline (10 min).
        """
        if self.reconciler is None or not self._venue_adapters:
            return

        # --- Process pending entries (uncertain maker/hedge orders) ---
        resolved_entry_ids: list[str] = []
        for entry_id, pending in list(self.state.pending_entries.items()):
            if not pending.uncertain_outcome:
                resolved_entry_ids.append(entry_id)
                continue

            # Respect backoff window
            if pending.reconcile_next_attempt_ms > 0 and now_ms < pending.reconcile_next_attempt_ms:
                continue

            # Hard deadline check
            if pending.deadline_ms > 0 and now_ms > pending.deadline_ms:
                self.journal.append(
                    "reconciliation.entry_abandoned_deadline",
                    {"entry_id": entry_id, "deadline_ms": pending.deadline_ms},
                )
                resolved_entry_ids.append(entry_id)
                continue

            pending.reconcile_attempt += 1
            try:
                result = await self.reconciler.reconcile_position(
                    position_id=entry_id,
                    symbol=pending.symbol,
                    long_venue=pending.long_venue,
                    short_venue=pending.short_venue,
                    long_order_id=pending.maker_order_id,
                    short_order_id=pending.hedge_order_id,
                    long_client_order_id=pending.maker_client_order_id,
                    short_client_order_id=pending.hedge_client_order_id,
                )
            except Exception as e:
                self.journal.append(
                    "reconciliation.entry_reconcile_error",
                    {"entry_id": entry_id, "error": str(e)},
                )
                self._apply_reconcile_backoff(pending, now_ms)
                continue

            if result.long_status == "filled" and result.short_status == "filled":
                resolved_entry_ids.append(entry_id)
                self.journal.append(
                    "reconciliation.entry_resolved",
                    {"entry_id": entry_id, "long_status": result.long_status, "short_status": result.short_status},
                )
            elif result.is_flat:
                resolved_entry_ids.append(entry_id)
                self.journal.append(
                    "reconciliation.entry_cleared_flat",
                    {"entry_id": entry_id},
                )
            else:
                self._apply_reconcile_backoff(pending, now_ms)

        for eid in resolved_entry_ids:
            self.state.pending_entries.pop(eid, None)

        # --- Process pending closes ---
        resolved_ids: list[str] = []
        for close_id, pending in list(self.state.pending_closes.items()):
            if pending.long_uncertain or pending.short_uncertain:
                # Respect backoff window
                if pending.reconcile_next_attempt_ms > 0 and now_ms < pending.reconcile_next_attempt_ms:
                    continue

                # Hard deadline check
                if pending.deadline_ms > 0 and now_ms > pending.deadline_ms:
                    self.journal.append(
                        "reconciliation.close_abandoned_deadline",
                        {"close_id": close_id, "deadline_ms": pending.deadline_ms},
                    )
                    resolved_ids.append(close_id)
                    continue

                pos = self.state.open_positions.get(pending.position_id)
                if pos is None:
                    resolved_ids.append(close_id)
                    self.journal.append(
                        "reconciliation.pending_close_orphaned",
                        {"close_id": close_id, "position_id": pending.position_id},
                    )
                    continue

                pending.reconcile_attempt += 1
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
                    self._apply_reconcile_backoff(pending, now_ms)
                    continue

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
                else:
                    self._apply_reconcile_backoff(pending, now_ms)
            else:
                resolved_ids.append(close_id)

        for cid in resolved_ids:
            self.state.pending_closes.pop(cid, None)

    @staticmethod
    def _apply_reconcile_backoff(pending, now_ms: int) -> None:
        """Apply exponential backoff to a PendingEntry or PendingClose.

        V1: CLOSE_RECONCILIATION_RETRY_BASE_MS=30s, max=300s.
        """
        backoff = min(
            LiveRuntime._RECONCILE_RETRY_BASE_MS * (2 ** max(pending.reconcile_attempt - 1, 0)),
            LiveRuntime._RECONCILE_RETRY_MAX_MS,
        )
        pending.reconcile_next_attempt_ms = now_ms + backoff

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

    def _snapshot_local_l2_state(self) -> None:
        """Snapshot local-L2 runtime state into EngineState for persistence/recovery."""
        diag = self.local_l2_runtime.diagnostics_snapshot()
        # Retained books metadata
        self.state.retained_local_l2_books = [
            {
                "venue": b.venue,
                "symbol": b.symbol,
                "status": b.status.value,
                "pool": b.pool.value,
                "sequence": b.sequence,
                "last_snapshot_ms": b.last_snapshot_ms,
                "last_delta_ms": b.last_delta_ms,
            }
            for b in self.local_l2_runtime.books.values()
            if b.pool == L2PoolAssignment.RETAINED
        ]
        # Full books snapshot for recovery
        self.state.local_l2_books_snapshot = [
            {
                "venue": b.venue,
                "symbol": b.symbol,
                "status": b.status.value,
                "pool": b.pool.value,
                "last_update_id": b.last_update_id,
                "sequence": b.sequence,
                "last_snapshot_ms": b.last_snapshot_ms,
                "last_delta_ms": b.last_delta_ms,
                "observed_at_ms": b.observed_at_ms,
            }
            for b in self.local_l2_runtime.books.values()
        ]
        # Session snapshot
        self.state.local_l2_session_snapshot = [
            s.diagnostics_snapshot(now_ms=wall_clock_now_ms(), stale_after_ms=5000)
            for s in self.entry_l2_sessions.sessions.values()
        ]

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

        # V1 local-L2 entry readiness gate: block entry when local-L2 enabled
        # but either leg's book is not ready (stale, degraded, cold, etc.)
        if self.config.strategy.local_l2_enabled:
            from lightfee.marketdata.liquidity import execution_liquidity_from_local_l2

            long_book = self.local_l2_runtime.get_book(long_venue.value, candidate.symbol)
            short_book = self.local_l2_runtime.get_book(short_venue.value, candidate.symbol)

            not_ready_reasons: list[str] = []
            if long_book is None:
                not_ready_reasons.append(f"long book missing: {long_venue.value}:{candidate.symbol}")
            else:
                liq = execution_liquidity_from_local_l2(
                    long_book, max_age_ms=self.config.strategy.max_liquidity_snapshot_age_ms,
                    now_ms=now_ms, require_ready=True,
                )
                if not liq.book_ready:
                    not_ready_reasons.append(
                        f"long leg not ready: {long_venue.value}:{candidate.symbol} "
                        f"status={long_book.status.value} age={long_book.age_ms(now_ms)}ms"
                    )

            if short_book is None:
                not_ready_reasons.append(f"short book missing: {short_venue.value}:{candidate.symbol}")
            else:
                liq = execution_liquidity_from_local_l2(
                    short_book, max_age_ms=self.config.strategy.max_liquidity_snapshot_age_ms,
                    now_ms=now_ms, require_ready=True,
                )
                if not liq.book_ready:
                    not_ready_reasons.append(
                        f"short leg not ready: {short_venue.value}:{candidate.symbol} "
                        f"status={short_book.status.value} age={short_book.age_ms(now_ms)}ms"
                    )

            if not_ready_reasons:
                self.journal.append(
                    "runtime.entry_blocked_local_l2_not_ready",
                    {
                        "symbol": candidate.symbol,
                        "long_venue": long_venue.value,
                        "short_venue": short_venue.value,
                        "reasons": not_ready_reasons,
                        "ts_ms": now_ms,
                    },
                )
                return

        # V1 entry route planning: derive route and maker leg from execution planner.
        # Strategy config provides min-notional; venue-specific chunk/min-notional
        # are resolved from the adapter or spec when available.
        strategy = self.config.strategy
        min_notional = strategy.min_entry_leg_notional_quote

        # V1: min_hedgeable_chunk aligns to venue step and notional floor
        min_hedgeable_chunk = min_notional / price_hint if price_hint > 0 else 0.0

        route, plan = plan_incremental_entry_execution(
            target_quantity=quantity,
            slice_ratio=strategy.maker_initial_slice_ratio,
            min_hedgeable_chunk=min_hedgeable_chunk,
            maker_min_notional_quote=min_notional,
            maker_price_hint=price_hint if price_hint > 0 else None,
            max_initial_clip_ratio=strategy.entry_max_initial_clip_ratio,
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

        # V1: maker leg from strategy config (funding arb: long side is typically maker)
        maker_leg = Side.BUY if strategy.maker_leg_default == "buy" else Side.SELL

        entry_id = f"entry-{now_ms}-{candidate.symbol}"

        # --- V1 recovery dedup: check for duplicate entries after restart ---
        maker_cid = f"{entry_id}-maker"
        hedge_cid = f"{entry_id}-hedge"

        if is_client_order_id_duplicate(maker_cid, self._recovery_dedup_index):
            self.journal.append(
                "runtime.entry_skipped_duplicate_client_order_id",
                {
                    "entry_id": entry_id,
                    "client_order_id": maker_cid,
                    "reason": "duplicate maker clientOrderId in recovery dedup index",
                },
            )
            return

        if is_client_order_id_duplicate(hedge_cid, self._recovery_dedup_index):
            self.journal.append(
                "runtime.entry_skipped_duplicate_client_order_id",
                {
                    "entry_id": entry_id,
                    "client_order_id": hedge_cid,
                    "reason": "duplicate hedge clientOrderId in recovery dedup index",
                },
            )
            return

        # Check for existing pending entry on same symbol pair
        if has_pending_entry_for_symbol(
            self.state, candidate.symbol,
            long_venue.value, short_venue.value,
        ):
            self.journal.append(
                "runtime.entry_skipped_existing_pending",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "reason": "pending entry already exists for this symbol pair",
                },
            )
            return

        ctx = EntryContext(
            entry_id=entry_id,
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
                    "has_uncertainty": result.has_uncertainty,
                },
            )
            if result.open_position is not None:
                self.state.open_positions[result.open_position.position_id] = result.open_position
                self.journal.append(
                    "runtime.position_opened",
                    {"position_id": result.open_position.position_id},
                )
            if result.pending_entry is not None:
                # Track pending entry for reconciliation
                self.state.pending_entries[result.pending_entry.pending_id] = result.pending_entry
                self._recovery_dedup_index[result.pending_entry.maker_client_order_id] = result.pending_entry.pending_id
                self._recovery_dedup_index[result.pending_entry.hedge_client_order_id] = result.pending_entry.pending_id
                self.journal.append(
                    "runtime.pending_entry_registered",
                    {
                        "pending_id": result.pending_entry.pending_id,
                        "symbol": result.pending_entry.symbol,
                        "outcome": result.pending_entry.outcome,
                        "maker_client_order_id": result.pending_entry.maker_client_order_id,
                        "hedge_client_order_id": result.pending_entry.hedge_client_order_id,
                    },
                )
        except Exception as e:
            self.journal.append(
                "runtime.entry_dispatch_error",
                {"entry_id": ctx.entry_id, "error": str(e)},
            )

        # ------------------------------------------------------------------
    # Passive close recovery (V1: recovery after restart)
    # ------------------------------------------------------------------

    async def _recover_passive_closes(self) -> None:
        """Probe and recover pending passive closes after restart.

        V1: On recovery, restored PendingPassiveClose records are probed
        for live flatness. Flat positions are cleared; still-open positions
        resume passive maintenance.
        """
        if self.passive_close_executor is None:
            return
        if not self.state.pending_passive_closes:
            return

        for position_id in list(self.state.pending_passive_closes.keys()):
            result = await self.passive_close_executor.recover_passive_close(
                self.state,
                position_id,
                self._venue_adapters,
            )
            self.journal.append(
                "runtime.passive_close_recovery_result",
                {
                    "position_id": position_id,
                    "result": result,
                },
            )

    # ------------------------------------------------------------------
    # Passive close lane (V1: process_pending_passive_closes)
    # ------------------------------------------------------------------

    async def _maybe_tick_passive_close(self, now_ms: int) -> None:
        """Drive pending passive closes each tick.

        V1: process_pending_passive_closes() in exit.rs line 2987.
        Runs after local-L2 sync so repricing has fresh book state.
        """
        if self.passive_close_executor is None:
            return
        if not self.state.pending_passive_closes:
            return

        try:
            await self.passive_close_executor.process_pending_passive_closes(
                self.state, now_ms,
            )
        except Exception as e:
            self.journal.append(
                "runtime.passive_close_tick_error",
                {"error": str(e), "ts_ms": now_ms},
            )

    # ------------------------------------------------------------------
    # Normal exit lane (V1: standard_close_reason → passive/aggressive)
    # ------------------------------------------------------------------

    async def _maybe_process_normal_exits(self, now_ms: int) -> None:
        """Evaluate normal exit reasons for open positions and route to close path.

        V1: standard_close_reason() identifies which positions should close.
        normal_close_reason_uses_passive_maker_taker() determines the close path:
        - passive close: funding_capture, trailing_exit, first_stage_capture,
          second_stage_capture, settlement_half_close, settlement_force_close
        - aggressive close: hard_stop, risk_delever, protection

        This method CONSUMES the predicate that was previously only unit-tested.
        """
        from lightfee.engine.exit_decision import (
            normal_close_reason_uses_passive_maker_taker,
            standard_close_reason,
        )

        if not self.state.open_positions:
            return

        for position in list(self.state.open_positions.values()):
            # Skip positions already in passive close
            if position.position_id in self.state.pending_passive_closes:
                continue

            reason = standard_close_reason(position, self.config.strategy, now_ms)
            if reason is None:
                continue

            reason_str = reason.value if hasattr(reason, 'value') else str(reason)

            if normal_close_reason_uses_passive_maker_taker(reason_str):
                # Route to passive close
                if self.passive_close_executor is not None:
                    self.journal.append(
                        "runtime.normal_close_routing_passive",
                        {
                            "position_id": position.position_id,
                            "reason": reason_str,
                            "matched_quantity": position.matched_quantity,
                        },
                    )
                    pending = await self.passive_close_executor.start_pending_passive_close(
                        self.state,
                        position,
                        reason_str,
                        long_price_hint=self._resolve_local_l2_mid(position.long_venue, position.symbol),
                        short_price_hint=self._resolve_local_l2_mid(position.short_venue, position.symbol),
                    )
                    if pending is not None:
                        # Immediately drive one cycle
                        await self.passive_close_executor.drive_pending_passive_close(
                            self.state, position.position_id, wait_until_terminal=False,
                        )
            else:
                # Route to aggressive close (hard_stop, risk, etc.)
                if self.close_executor is not None:
                    self.journal.append(
                        "runtime.normal_close_routing_aggressive",
                        {
                            "position_id": position.position_id,
                            "reason": reason_str,
                            "matched_quantity": position.matched_quantity,
                        },
                    )
                    await self.close_executor.execute_close(
                        position, reason_str, now_ms,
                        long_price_hint=self._resolve_local_l2_mid(position.long_venue, position.symbol),
                        short_price_hint=self._resolve_local_l2_mid(position.short_venue, position.symbol),
                        state=self.state,
                    )

    def _resolve_local_l2_mid(self, venue, symbol: str) -> float:
        """Get mid price from local L2 book or sidecar for the given venue+symbol."""
        try:
            book = self.local_l2_runtime.get_book(venue.value if hasattr(venue, 'value') else str(venue), symbol)
            if book is not None and book.status.value == "hot":
                mid = book.mid_price()
                if mid and mid > 0:
                    return mid
        except Exception:
            pass
        return 0.0

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
