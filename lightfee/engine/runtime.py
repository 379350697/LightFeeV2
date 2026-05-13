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
from lightfee.sidecar.snapshot import evaluate_snapshot_freshness, SnapshotFreshness
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
        self._maker_tick_backoff_until_ms: Optional[int] = None

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

        budget = config.strategy.local_l2_resource_budget()
        self.local_l2_runtime = LocalL2Runtime(
            max_hot_exec=budget["reserved_hot_global"],
            max_warm=budget["warm_global"],
        )

        # V1 local-L2 data plane (REST snapshot bootstrap + WS streaming)
        from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
        self.l2_data_plane = LocalL2DataPlane(
            l2_runtime=self.local_l2_runtime,
            journal=self.journal,
        )

        # V1 entry-local-L2 session runtime (tracked opportunities, readiness)
        from lightfee.engine.entry_local_l2 import EntryLocalL2SessionRuntime
        self.entry_l2_sessions = EntryLocalL2SessionRuntime()
        self._tracked_primary_pair_ids: set[str] = set()  # V1: primary_opportunities

        # V1 recovery dedup index: prevents duplicate orders after restart
        self._recovery_dedup_index: dict[str, str] = {}

        # V1 entry gate cooldown state
        self._venue_cooldown_until_ms: dict[str, int] = {}
        self._zero_fill_cooldown_until_ms: dict[tuple, int] = {}

        # V1 live scan recovery state (B2)
        self._live_scan_success_streak: int = 0
        self._last_good_snapshot = None

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

            # V1: finalize_startup_position_recovery — ordered recovery sequence
            now_ms = wall_clock_now_ms()

            # 1. reconcile_open_positions (force_reconcile — ignore backoff)
            await self._reconcile_pending_entries_force(now_ms)

            # 2. process pending_entry_hedges — re-drive any uncertain maker orders
            await self._recover_pending_entry_hedges(now_ms)

            # 3. process pending_passive_closes — resume passive close cycles
            await self._maybe_tick_passive_close(now_ms)

            # 4. process pending_close_reconciliations
            # (already handled by _reconcile_pending_state in housekeeping)

            # 5. residual repairs
            await self._recover_residual_repairs(now_ms)

            # 6. manage_open_positions — if still over max, enter fail_closed
            max_positions = self.config.strategy.max_concurrent_positions
            if len(self.state.open_positions) > max_positions:
                from lightfee.engine.lifecycle import enter_fail_closed
                enter_fail_closed(self.state)
                self.journal.append(
                    "runtime.recovery_fail_closed",
                    {
                        "reason": "open_positions_exceed_max_after_recovery",
                        "open_positions": len(self.state.open_positions),
                        "max": max_positions,
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
        """Phase 5: Activate local-L2 books — WS streams first, then background bootstrap.

        V1 parity with live_startup_activate_local_l2():
        1. Derive target pairs from retained state (retained_local_l2_books) and
           hot positions — NOT all config.symbols (V1: startup_local_l2_symbols)
        2. Create LocalL2Book for each target pair
        3. Start WS depth streams FIRST (deltas buffered during bootstrap gap)
        4. Start per-venue background bootstrap workers (REST snapshots)
        5. Return immediately — bootstrap completes asynchronously in background

        WS updates received while a book is BOOTSTRAPPING are buffered and
        replayed after the REST snapshot completes (V1 pre-snapshot buffer pattern).

        Runtime L2 activation for new entry symbols is handled separately by
        _ensure_l2_active_for_candidates() on each tick.
        """
        self.journal.append(
            "runtime.local_l2_phase_start",
            {"ts_ms": now_ms},
        )

        # V1: startup_local_l2_symbols() → retained + hot symbols only
        # NOT all config.symbols — L2 is only bootstrapped for symbols with activity
        target_pairs: set[tuple[str, str]] = set()
        if self.config.strategy.local_l2_enabled:
            active_venues = list(self._venue_adapters.keys())
            venue_set = {
                v.value if hasattr(v, 'value') else str(v)
                for v in active_venues
            }

            # 1. Retained books from previous run (V1: retained_local_l2_books)
            for book in (self.state.retained_local_l2_books or []):
                ven = book.get("venue", "")
                sym = book.get("symbol", "")
                if ven in venue_set and sym:
                    target_pairs.add((ven, sym))

            # 2. Hot symbols from active positions (V1: hot_local_l2_symbols)
            hot_budget = max(
                getattr(self.config.strategy, 'local_l2_hot_exec_per_venue_budget', 20), 1,
            )
            hot_global_budget = max(
                getattr(self.config.strategy, 'local_l2_hot_exec_global_budget', 0), 0,
            )
            hot_count = 0
            hot_global_count = 0
            for pos in getattr(self.state, 'open_positions', []) or []:
                if hot_count >= hot_budget:
                    break
                if hot_global_budget > 0 and hot_global_count >= hot_global_budget:
                    break
                ven = getattr(pos, 'venue', '')
                sym = getattr(pos, 'symbol', '')
                if isinstance(ven, str) and ven in venue_set and sym:
                    target_pairs.add((ven, sym))
                    hot_count += 1
                    hot_global_count += 1
                elif hasattr(ven, 'value'):
                    ven_str = ven.value
                    if ven_str in venue_set and sym:
                        target_pairs.add((ven_str, sym))
                        hot_count += 1
                        hot_global_count += 1

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

        # Step 1: Create books for all target pairs (V1: mark_binance_local_l2_bootstrapping)
        books_created = 0
        for venue_str, symbol in sorted(target_pairs):
            rules = get_venue_rules(venue_str)
            book = self.local_l2_runtime.ensure_book(venue_str, symbol)
            book.max_depth = rules.default_depth
            book.max_sequence_gap = rules.max_sequence_gap
            if book.status == L2BookStatus.COLD:
                if self.config.runtime.mode == "paper":
                    book.transition_to_hot()
                else:
                    book.transition_to_bootstrapping(now_ms)
                books_created += 1

        # Step 2: Start WS streams FIRST for all venues (V1: start_local_l2_ws)
        # This ensures delta updates are captured (buffered) during bootstrap gap
        if (
            self.config.strategy.local_l2_enabled
            and getattr(self.config.strategy, 'local_l2_ws_enabled', False)
            and self.config.runtime.mode != "paper"
        ):
            ws_started = 0
            venue_symbols: dict[str, list[str]] = {}
            for venue_str, symbol in target_pairs:
                venue_symbols.setdefault(venue_str, []).append(symbol)

            for venue_str, symbols in venue_symbols.items():
                try:
                    from lightfee.core.domain import Venue as VenueEnum
                    ven = VenueEnum.from_str(venue_str)
                    adapter = self.get_venue_adapter(ven) if ven in self._venue_adapters else None
                except (ValueError, KeyError):
                    adapter = None

                registered = self.l2_data_plane.start_ws_streams(
                    venue_str, symbols, adapter=adapter,
                )
                if registered > 0:
                    ws_started += registered

            if ws_started > 0:
                connected = await self.l2_data_plane.connect_ws_streams()
                ws_started = connected
                self.journal.append(
                    "runtime.local_l2_ws_started",
                    {
                        "stream_count": ws_started,
                        "venues": sorted(venue_symbols.keys()),
                        "ts_ms": wall_clock_now_ms(),
                    },
                )

        # Step 3: Start per-venue background bootstrap workers (V1: start_local_l2_bootstrap)
        # Each worker fetches REST snapshots with concurrency control and retry
        if self.config.runtime.mode != "paper":
            bs_total = 0
            bs_batch = getattr(self.config.strategy, 'local_l2_bootstrap_batch_size', 4)
            bs_jitter = getattr(self.config.strategy, 'local_l2_bootstrap_jitter_ms', 250)
            bs_retry = getattr(self.config.strategy, 'local_l2_bootstrap_retry_backoff_ms', 5000)

            for venue_str, symbols in venue_symbols.items():
                try:
                    from lightfee.core.domain import Venue as VenueEnum
                    ven = VenueEnum.from_str(venue_str)
                    adapter = self.get_venue_adapter(ven) if ven in self._venue_adapters else None
                except (ValueError, KeyError):
                    adapter = None

                if adapter is None or not hasattr(adapter, 'fetch_l2_snapshot'):
                    continue

                self.l2_data_plane.start_background_bootstrap(
                    venue=venue_str,
                    symbols=symbols,
                    adapter=adapter,
                    batch_size=bs_batch,
                    jitter_ms=bs_jitter,
                    retry_backoff_ms=bs_retry,
                )
                bs_total += len(symbols)

            self.journal.append(
                "runtime.local_l2_bootstrap_started",
                {
                    "venues": sorted(venue_symbols.keys()),
                    "total_symbols": bs_total,
                    "ts_ms": wall_clock_now_ms(),
                },
            )

        # Restore retained books from previous state
        books_retained = 0
        if hasattr(self.state, "retained_local_l2_books"):
            for entry in getattr(self.state, "retained_local_l2_books", []):
                venue = entry.get("venue", "")
                sym = entry.get("symbol", "")
                if venue and sym:
                    book = self.local_l2_runtime.ensure_book(venue, sym)
                    if book.status == L2BookStatus.COLD:
                        book.pool = L2PoolAssignment.RETAINED
                        book.transition_to_bootstrapping(now_ms)
                        books_retained += 1

        self.journal.append(
            "runtime.local_l2_phase_complete",
            {
                "books_created": books_created,
                "books_retained": books_retained,
                "target_pairs": len(target_pairs),
                "phase_ms": wall_clock_now_ms() - now_ms,
                "bootstrap_mode": "background_per_venue",
            },
        )

    async def _ensure_l2_active_for_candidates(self, candidates, now_ms: int) -> None:
        """Ensure L2 books are active for candidate entry symbols.

        V1 parity: activity_local_l2_symbols() → live_startup_activate_local_l2().

        Called on each tick when tradeable candidates are discovered.  For each
        candidate's long/short venue+symbol pair that does NOT already have an
        active L2 book, create the book, start a WS stream, and spawn a
        background bootstrap worker.

        Respects local_l2_hot_exec_per_venue_budget (V1).
        """
        if not self.config.strategy.local_l2_enabled:
            return
        if self.config.runtime.mode == "paper":
            return

        # Collect (venue, symbol) pairs from candidates that need L2
        # CandidateInput has long_venue/short_venue as str fields (not leg objects)
        needed: dict[str, set[str]] = {}  # venue -> {symbols}
        for c in candidates:
            sym = getattr(c, 'symbol', '')
            for ven_str in (getattr(c, 'long_venue', ''), getattr(c, 'short_venue', '')):
                if not ven_str or not sym:
                    continue
                # Skip if already active
                book = self.local_l2_runtime.get_book(ven_str, sym)
                if book is not None and book.status in (
                    L2BookStatus.HOT, L2BookStatus.BOOTSTRAPPING, L2BookStatus.DEGRADED,
                ):
                    continue
                needed.setdefault(ven_str, set()).add(sym)

        if not needed:
            return

        per_venue_budget = max(
            getattr(self.config.strategy, 'local_l2_hot_exec_per_venue_budget', 20), 1,
        )
        from lightfee.marketdata.local_l2_venues import get_venue_rules

        for ven_str, symbols in needed.items():
            # Limit per venue budget (V1: take(per_venue_budget))
            symbols_list = sorted(symbols)[:per_venue_budget]
            if not symbols_list:
                continue

            try:
                from lightfee.core.domain import Venue as VenueEnum
                ven = VenueEnum.from_str(ven_str)
                adapter = self.get_venue_adapter(ven) if ven in self._venue_adapters else None
            except (ValueError, KeyError):
                adapter = None
            if adapter is None or not hasattr(adapter, 'fetch_l2_snapshot'):
                continue

            # Ensure books exist
            for sym in symbols_list:
                rules = get_venue_rules(ven_str)
                book = self.local_l2_runtime.ensure_book(ven_str, sym)
                book.max_depth = rules.default_depth
                book.max_sequence_gap = rules.max_sequence_gap
                if book.status == L2BookStatus.COLD:
                    book.transition_to_bootstrapping(now_ms)

            # Start WS stream for this venue's new symbols
            self.l2_data_plane.start_ws_streams(ven_str, symbols_list, adapter=adapter)

            # Start background bootstrap worker
            bs_batch = getattr(self.config.strategy, 'local_l2_bootstrap_batch_size', 4)
            bs_jitter = getattr(self.config.strategy, 'local_l2_bootstrap_jitter_ms', 250)
            bs_retry = getattr(self.config.strategy, 'local_l2_bootstrap_retry_backoff_ms', 5000)
            self.l2_data_plane.start_background_bootstrap(
                venue=ven_str,
                symbols=symbols_list,
                adapter=adapter,
                batch_size=bs_batch,
                jitter_ms=bs_jitter,
                retry_backoff_ms=bs_retry,
            )

    async def _restore_local_l2_state(self) -> None:
        """Phase 6: Restore retained local-L2 books and session state from snapshot.

        V1: Restores PersistedRetainedLocalL2Book including bids/asks book data
        and generation tracking for stale-snapshot detection.
        """
        from lightfee.marketdata.l2 import PriceLevel

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
            # V1: restore generation for stale-snapshot gating
            if hasattr(book, 'generation'):
                book.generation = entry.get("generation", 1)
            # V1: restore book data (bids/asks) if available
            if entry.get("bids"):
                book.bids = [PriceLevel(price=l["price"], quantity=l["quantity"]) for l in entry["bids"]]
            if entry.get("asks"):
                book.asks = [PriceLevel(price=l["price"], quantity=l["quantity"]) for l in entry["asks"]]
            # Restore the persisted pool — only RETAINED books should be
            # re-bootstrapped at startup (V1: retained_local_l2_books).
            pool_str = entry.get("pool", "dropped")
            try:
                book.pool = L2PoolAssignment(pool_str)
            except ValueError:
                book.pool = L2PoolAssignment.DROPPED
            # V1: retained books bootstrap directly (retained_local_l2_books)
            if book.pool == L2PoolAssignment.RETAINED:
                if book.status in (L2BookStatus.COLD, L2BookStatus.RESUME_WAITING):
                    book.transition_to_bootstrapping(0)
            # Restored book is never automatically HOT — must prove freshness.
            # But don't overwrite a book that is already being bootstrapped
            # (set by _activate_local_l2_phase for retained/hot symbols).
            elif book.status in (L2BookStatus.COLD,):
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

        # V1: evaluate_snapshot_freshness — multi-state freshness evaluation
        freshness = evaluate_snapshot_freshness(
            snapshot=snapshot,
            max_age_ms=max_age,
            now_ms=now_ms,
            last_good=self._last_good_snapshot,
        )
        if freshness == SnapshotFreshness.MISSING:
            self._live_scan_success_streak = 0
            self.journal.append("runtime.snapshot_missing", {"ts_ms": now_ms})
            return
        if freshness == SnapshotFreshness.STALE:
            self._live_scan_success_streak = 0
            if self._last_good_snapshot is not None:
                snapshot = self._last_good_snapshot
                self.journal.append("runtime.snapshot_fallback_last_good", {"ts_ms": now_ms})
            else:
                self.journal.append("runtime.snapshot_stale", {"ts_ms": now_ms})
                return
        if freshness == SnapshotFreshness.DEGRADED:
            # Some venues degraded but can still trade on healthy ones
            self._live_scan_success_streak += 1
            self._last_good_snapshot = snapshot
            self.journal.append("runtime.snapshot_degraded",
                {"venues": snapshot.degraded_venues, "ts_ms": now_ms})
        if freshness == SnapshotFreshness.LAST_GOOD_FALLBACK:
            # Current snapshot is stale/missing; fall back to last good
            snapshot = self._last_good_snapshot
            self._live_scan_success_streak += 1
            self.journal.append("runtime.snapshot_fallback_last_good", {"ts_ms": now_ms})
        if freshness == SnapshotFreshness.FRESH:
            self._live_scan_success_streak += 1
            self._last_good_snapshot = snapshot

        # V1 pre-scan L2 sync: refresh execution-owned books only (scan_promoted=False)
        await self._sync_local_l2_data(now_ms, scan_promoted=False)

        # --- Build price lookup from snapshot quotes ---
        price_hints: dict[str, float] = {}
        for quote in snapshot.quotes.values():
            price_hints[quote.symbol] = (quote.bid + quote.ask) / 2.0 if quote.bid > 0 and quote.ask > 0 else 0.0

        # --- Discover tradeable candidates ---
        # V1 live scan recovery gate: require consecutive fresh snapshots before entry
        live_scan_recovery_count = getattr(self.config.strategy, 'live_scan_recovery_success_count', 3)
        if self._live_scan_success_streak < live_scan_recovery_count:
            self.journal.append(
                "runtime.live_scan_recovery_warmup",
                {"success_streak": self._live_scan_success_streak,
                 "required": live_scan_recovery_count, "ts_ms": now_ms},
            )
            return

        if can_enter_new_positions(self.state) and self.entry_executor is not None:
            tradeable = discover_tradeable_candidates(
                snapshot.candidates, self.config.strategy, now_ms
            )
            if tradeable:
                # V1: activity_local_l2_symbols() → ensure L2 active for candidate symbols
                # Only bootstrap L2 for symbols that have actual activity (shortlist/finalist)
                await self._ensure_l2_active_for_candidates(tradeable, now_ms)

                # V1 post-shortlist L2 sync: allows scan-promoted books (scan_promoted=True)
                await self._sync_local_l2_data(now_ms, scan_promoted=True)

                # V1 market data warmup: funding coverage must meet threshold before entry
                if hasattr(snapshot, 'funding_lifecycle') and snapshot.funding_lifecycle:
                    funding_warmup_required = getattr(
                        self.config.strategy, 'funding_warmup_min_coverage_ratio', 0.5,
                    )
                    total_count = sum(
                        fl.symbol_count for fl in snapshot.funding_lifecycle
                    )
                    venue_count = len(snapshot.funding_lifecycle)
                    funding_warmup_ok = (
                        venue_count >= 1 and total_count > 0
                    )
                    if not funding_warmup_ok and not self.state.open_positions:
                        self.journal.append(
                            "runtime.funding_warmup_insufficient",
                            {
                                "funding_venue_count": venue_count,
                                "funding_symbol_count": total_count,
                                "warmup_ratio_required": funding_warmup_required,
                                "ts_ms": now_ms,
                            },
                        )
                        return

                self.journal.append(
                    "runtime.candidates_tradeable",
                    {"count": len(tradeable), "ts_ms": now_ms},
                )
                # V1: refresh tracked entry local L2 opportunities per tick
                # select_tracked_entry_local_l2_opportunities → primary + shadow
                if self.config.strategy.local_l2_enabled:
                    primary_count = getattr(
                        self.config.strategy, "entry_local_l2_primary_count", 3,
                    )
                    shadow_count = getattr(
                        self.config.strategy, "entry_local_l2_shadow_count", 1,
                    )
                    from lightfee.engine.entry_local_l2 import (
                        select_tracked_opportunities,
                        make_candidate_pair_id,
                    )
                    tracked = select_tracked_opportunities(
                        tradeable, primary_count, shadow_count,
                    )
                    self._tracked_primary_pair_ids = {
                        t.pair_id for t in tracked
                        if t.class_.value == "primary_tracked"
                    }
                    # Refresh session state for all tracked opportunities
                    for t in tracked:
                        self.entry_l2_sessions.track_opportunity(t, now_ms)
                # V1: iterate entire shortlist until slot budget exhausted
                max_slots = self.config.strategy.max_concurrent_positions
                dispatched = 0
                for candidate in tradeable:
                    if len(self.state.open_positions) >= max_slots:
                        break
                    # V2 Task 9: entry local L2 selection blocker (prewarm/dual-ready)
                    l2_blocker = self._entry_local_l2_selection_blocker(candidate, now_ms)
                    if l2_blocker:
                        self.journal.append(
                            "runtime.entry_blocked_local_l2_selection",
                            {
                                "symbol": candidate.symbol,
                                "pair_id": getattr(candidate, "pair_id", ""),
                                "reason": l2_blocker,
                                "ts_ms": now_ms,
                            },
                        )
                        continue
                    mid_price = price_hints.get(candidate.symbol, 0.0)
                    await self._dispatch_entry(candidate, now_ms, price_hint=mid_price)
                    dispatched += 1

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

    async def _sync_local_l2_data(self, now_ms: int, *, scan_promoted: bool = False) -> None:
        """Periodic snapshot refresh for local-L2 books without WS streaming.

        Called at two points per tick (V1 dual-phase):
        1. Pre-scan (scan_promoted=False): execution-owned books only
        2. Post-shortlist (scan_promoted=True): allows scan-promoted books

        Delegates to the data plane which respects per-book cooldown intervals.
        """
        if not self.config.strategy.local_l2_enabled:
            return

        try:
            dispatched = await self.l2_data_plane.sync_snapshots(
                adapters=self._venue_adapters,
                now_ms=now_ms,
                scan_promoted=scan_promoted,
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
        non_parity_mode = self.config.runtime.opportunity_input_mode == "non_parity"

        if local_l2_enabled:
            # --- Parity mode: local-L2 event-driven ---
            await self._maybe_tick_maker_event_local_l2(now_ms, pending_passive)
        elif non_parity_mode:
            # --- Explicit non-parity fallback: sidecar mid-price ---
            await self._maybe_tick_maker_event_sidecar(now_ms, pending_passive)
        else:
            # Neither parity nor non-parity — sidecar fallback must be explicit opt-in.
            # local_l2_enabled=False alone does NOT activate the sidecar path.
            self.journal.append(
                "runtime.maker_event_no_eligible_mode",
                {
                    "ts_ms": now_ms,
                    "local_l2_enabled": local_l2_enabled,
                    "opportunity_input_mode": self.config.runtime.opportunity_input_mode,
                    "reason": "non-parity fallback requires explicit opportunity_input_mode='non_parity'",
                },
            )

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

            # --- Maker-event lane (V1: maker_event_interval, optional, with backoff) ---
            if full_tick_ready(self._maker_tick_backoff_until_ms, now_ms):
                try:
                    await self._maybe_tick_maker_event(now_ms)
                    self._maker_tick_backoff_until_ms = None
                except Exception as e:
                    self._apply_tick_backoff(is_maker=True)
                    self.journal.append(
                        "runtime.maker_event_tick_error", {"error": str(e)}
                    )

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

            # V1: abandon via live-size probe, not hard deadline.
            # After 1+ failed attempts, if the entry no longer references an active
            # position and both venues report ~zero live size → abandon immediately.
            if pending.reconcile_attempt >= 1:
                abandoned = await self._try_abandon_stale_entry(pending, entry_id)
                if abandoned:
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

        # Transition out of RECONCILING if all work is done
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

    async def _try_abandon_stale_entry(self, pending, entry_id: str) -> bool:
        """V1-style stale entry abandonment via live-size probe.

        V1: try_abandon_stale_pending_close_reconciliation() — after 1 failed
        reconciliation, if the entry no longer references an active position AND
        both venues report ~zero live size, the entry is abandoned immediately.
        No hard deadline — real evidence only.
        """
        # Entry must reference a position_id that is no longer active
        pos_id = pending.position_id if hasattr(pending, 'position_id') else pending.pending_id
        if self.state.open_positions.get(pos_id) is not None:
            return False  # still active, don't abandon

        # Probe both venues for live position size
        try:
            from lightfee.core.domain import Venue as VenueEnum
            long_ven = VenueEnum.from_str(pending.long_venue) if isinstance(pending.long_venue, str) else pending.long_venue
            short_ven = VenueEnum.from_str(pending.short_venue) if isinstance(pending.short_venue, str) else pending.short_venue
            long_adapter = self._venue_adapters.get(long_ven)
            short_adapter = self._venue_adapters.get(short_ven)
        except (ValueError, KeyError):
            long_adapter = None
            short_adapter = None

        long_zero = True
        short_zero = True
        try:
            if long_adapter is not None:
                pos = await long_adapter.fetch_position(pending.symbol)
                long_zero = pos.quantity <= 0.0
        except Exception:
            long_zero = False  # can't probe → assume not zero

        try:
            if short_adapter is not None:
                pos = await short_adapter.fetch_position(pending.symbol)
                short_zero = pos.quantity <= 0.0
        except Exception:
            short_zero = False

        if long_zero and short_zero:
            self.journal.append(
                "reconciliation.entry_abandoned_flat",
                {"entry_id": entry_id, "reason": "both_venues_zero"},
            )
            return True

        return False

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

    async def _reconcile_pending_entries_force(self, now_ms: int) -> None:
        """Force-reconcile pending entries ignoring backoff windows.

        V1: reconcile_open_positions() with force_reconcile=true — used at
        startup recovery to immediately resolve any uncertain outcomes before
        resuming normal operations.
        """
        if self.reconciler is None or not self._venue_adapters:
            return

        resolved_ids: list[str] = []
        for entry_id, pending in list(self.state.pending_entries.items()):
            if not pending.uncertain_outcome:
                resolved_ids.append(entry_id)
                continue

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
                    "recovery.force_reconcile_entry_error",
                    {"entry_id": entry_id, "error": str(e)},
                )
                continue

            if result.long_status == "filled" and result.short_status == "filled":
                resolved_ids.append(entry_id)
            elif result.is_flat:
                resolved_ids.append(entry_id)

        for eid in resolved_ids:
            self.state.pending_entries.pop(eid, None)

        self.journal.append(
            "recovery.force_reconcile_complete",
            {"resolved_entries": len(resolved_ids), "ts_ms": now_ms},
        )

    async def _recover_pending_entry_hedges(self, now_ms: int) -> None:
        """Re-drive pending entry hedges with uncertain maker orders after restart.

        V1: process_pending_entry_hedges() in finalize_startup_position_recovery —
        re-polls the maker order status for any pending entry hedge that survived
        a restart with an uncertain outcome.
        """
        if not self._venue_adapters:
            return

        for entry_id, pending in list(self.state.pending_entries.items()):
            if not pending.uncertain_outcome:
                continue
            if not pending.maker_order_id and not pending.hedge_order_id:
                continue

            # Check if either venue adapter can report on the order
            for ven in (pending.long_venue, pending.short_venue):
                adapter = self.get_venue_adapter(ven)
                if adapter is None:
                    continue
                try:
                    if hasattr(adapter, 'get_order_status'):
                        status = await adapter.get_order_status(
                            symbol=pending.symbol,
                            order_id=pending.maker_order_id or pending.hedge_order_id,
                        )
                        if status and getattr(status, 'status', '') == "filled":
                            pending.uncertain_outcome = False
                            pending.outcome = "filled"
                            self.journal.append(
                                "recovery.entry_hedge_resolved",
                                {"entry_id": entry_id, "venue": str(ven), "status": status.status},
                            )
                            break
                except Exception:
                    continue

    async def _recover_residual_repairs(self, now_ms: int) -> None:
        """Process pending residual repair tasks after startup recovery.

        V1: process_pending_residual_repairs() — iterates over
        pending_residual_repairs and attempts to repair one-sided exposure
        by submitting reduce-only orders on the over-exposed venue.
        """
        if not self.state.pending_residual_repairs:
            return

        if self.close_executor is None:
            self.journal.append(
                "recovery.residual_repairs_skipped_no_executor",
                {"count": len(self.state.pending_residual_repairs), "ts_ms": now_ms},
            )
            return

        repaired = 0
        for task in list(self.state.pending_residual_repairs):
            if not isinstance(task, dict):
                continue
            position_id = task.get("position_id", "")
            pos = self.state.open_positions.get(position_id)
            if pos is None:
                self.state.pending_residual_repairs.remove(task)
                continue

            try:
                await self.close_executor.execute_close(
                    pos, "residual_repair", now_ms,
                    long_price_hint=self._resolve_local_l2_mid(pos.long_venue, pos.symbol),
                    short_price_hint=self._resolve_local_l2_mid(pos.short_venue, pos.symbol),
                    state=self.state,
                )
                self.state.pending_residual_repairs.remove(task)
                repaired += 1
            except Exception as e:
                self.journal.append(
                    "recovery.residual_repair_error",
                    {"position_id": position_id, "error": str(e)},
                )

        if repaired > 0:
            self.journal.append(
                "recovery.residual_repairs_complete",
                {"repaired": repaired, "ts_ms": now_ms},
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
        """Snapshot local-L2 runtime state into EngineState for persistence/recovery.

        V1: PersistedRetainedLocalL2Book with bids/asks + generation tracking.
        """
        diag = self.local_l2_runtime.diagnostics_snapshot()
        # Retained books metadata (V1: persisted with full book data)
        self.state.retained_local_l2_books = [
            {
                "venue": b.venue,
                "symbol": b.symbol,
                "status": b.status.value,
                "pool": b.pool.value,
                "sequence": b.sequence,
                "last_snapshot_ms": b.last_snapshot_ms,
                "last_delta_ms": b.last_delta_ms,
                "last_update_id": b.last_update_id,
                "generation": getattr(b, 'generation', 1),
                "bids": [{"price": l.price, "quantity": l.quantity} for l in b.bids] if hasattr(b, 'bids') else [],
                "asks": [{"price": l.price, "quantity": l.quantity} for l in b.asks] if hasattr(b, 'asks') else [],
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
                "generation": getattr(b, 'generation', 1),
                "bids": [{"price": l.price, "quantity": l.quantity} for l in b.bids] if hasattr(b, 'bids') else [],
                "asks": [{"price": l.price, "quantity": l.quantity} for l in b.asks] if hasattr(b, 'asks') else [],
            }
            for b in self.local_l2_runtime.books.values()
        ]
        # Session snapshot
        self.state.local_l2_session_snapshot = [
            s.diagnostics_snapshot(now_ms=wall_clock_now_ms(), stale_after_ms=5000)
            for s in self.entry_l2_sessions.sessions.values()
        ]

    # ------------------------------------------------------------------
    # Entry guards (V1: apply_runtime_entry_guards)
    # ------------------------------------------------------------------

    def _gate_pending_close_reconciliation(self, candidate) -> tuple[bool, str]:
        """Block entry if a pending close reconciliation exists for same symbol+venues."""
        sym = getattr(candidate, 'symbol', '')
        long_v = getattr(candidate, 'long_venue', '')
        short_v = getattr(candidate, 'short_venue', '')
        for pc in self.state.pending_closes.values():
            if getattr(pc, 'symbol', '') != sym:
                continue
            pc_long = getattr(pc, 'long_venue', None)
            pc_short = getattr(pc, 'short_venue', None)
            pc_long_s = pc_long.value if hasattr(pc_long, 'value') else str(pc_long)
            pc_short_s = pc_short.value if hasattr(pc_short, 'value') else str(pc_short)
            if (pc_long_s == long_v and pc_short_s == short_v) or \
               (pc_long_s == short_v and pc_short_s == long_v):
                return False, "pending_close_reconciliation_conflict"
        return True, ""

    def _gate_passive_close_pending(self, candidate) -> tuple[bool, str]:
        """Block entry if a passive close is in-flight for the same symbol pair."""
        sym = getattr(candidate, 'symbol', '')
        long_v = getattr(candidate, 'long_venue', '')
        short_v = getattr(candidate, 'short_venue', '')
        for pos_id in list(self.state.pending_passive_closes.keys()):
            pos = self.state.open_positions.get(pos_id)
            if pos is None:
                continue
            if getattr(pos, 'symbol', '') != sym:
                continue
            pos_long = getattr(pos, 'long_venue', None)
            pos_short = getattr(pos, 'short_venue', None)
            pos_long_s = pos_long.value if hasattr(pos_long, 'value') else str(pos_long)
            pos_short_s = pos_short.value if hasattr(pos_short, 'value') else str(pos_short)
            if (pos_long_s == long_v and pos_short_s == short_v) or \
               (pos_long_s == short_v and pos_short_s == long_v):
                return False, "passive_close_in_flight"
        return True, ""

    def _gate_reduce_only(self, candidate) -> tuple[bool, str]:
        """Block new entry when lifecycle/risk mode is reduce-only or fail-closed."""
        if self.state.lifecycle in (EngineLifecycle.RISK_ONLY, EngineLifecycle.FAIL_CLOSED):
            return False, f"lifecycle_{self.state.lifecycle.value}"
        if self.state.risk_mode.value in ("reduce_only", "fail_closed"):
            return False, f"risk_mode_{self.state.risk_mode.value}"
        return True, ""

    def _gate_venue_cooldown(self, candidate, now_ms: int) -> tuple[bool, str]:
        """Block entry if either venue is in cooldown."""
        for ven_str in (getattr(candidate, 'long_venue', ''), getattr(candidate, 'short_venue', '')):
            if not ven_str:
                continue
            until = self._venue_cooldown_until_ms.get(ven_str, 0)
            if until > 0 and now_ms < until:
                return False, f"venue_cooldown_{ven_str}"
        return True, ""

    def _gate_zero_fill_cooldown(self, candidate, now_ms: int) -> tuple[bool, str]:
        """Block entry if a zero-fill terminal event is in cooldown for the same pair.

        Zero-fill means a recent entry attempt on this pair produced no fills,
        indicating the venue may be rejecting orders or the spread is too wide.
        """
        pair_key = (getattr(candidate, 'symbol', ''), getattr(candidate, 'long_venue', ''), getattr(candidate, 'short_venue', ''))
        until = self._zero_fill_cooldown_until_ms.get(pair_key, 0)
        if until > 0 and now_ms < until:
            return False, "zero_fill_cooldown"
        return True, ""

    def _gate_pending_entry_dedup(self, candidate) -> tuple[bool, str]:
        """Block entry if a pending entry already exists for same symbol+venue pair."""
        from lightfee.engine.recovery import has_pending_entry_for_symbol
        sym = getattr(candidate, 'symbol', '')
        long_v = getattr(candidate, 'long_venue', '')
        short_v = getattr(candidate, 'short_venue', '')
        if has_pending_entry_for_symbol(self.state, sym, long_v, short_v):
            return False, "pending_entry_duplicate"
        return True, ""

    def _gate_entry_sizing(self, candidate) -> tuple[bool, str]:
        """Block entry if notional quote is zero or negative."""
        if getattr(candidate, 'entry_notional_quote', 0.0) <= 0:
            return False, "entry_notional_zero_or_negative"
        return True, ""

    # ------------------------------------------------------------------
    # Entry dispatch
    # ------------------------------------------------------------------

    def _entry_local_l2_selection_blocker(self, candidate, now_ms: int) -> str | None:
        """V1 entry local L2 selection gate: check prewarm, primary tracking, dual-ready.

        Returns a reason string if blocked, or None if ready to proceed.

        V1 (Rust: final_gate.rs entry_final_gate_result_from_candidate_local_l2):
        - Live + local_l2_enabled → gate applies
        - Candidate must be in primary tracked set
        - Session must exist for pair_id
        - Both legs must be ready (dual-ready)
        - Book must be exportable as valid ExecutionLiquiditySnapshot

        Blocker reasons (V1 stable labels):
        - entry_local_l2_waiting_for_prewarm_window
        - entry_local_l2_waiting_for_primary_tracking
        - entry_local_l2_waiting_for_dual_ready
        """
        if not self.config.strategy.local_l2_enabled:
            return None
        if self.config.runtime.mode not in ("live", "paper"):
            return None

        from lightfee.engine.entry_local_l2 import make_candidate_pair_id

        symbol = getattr(candidate, "symbol", "")
        long_ven = str(getattr(candidate, "long_venue", ""))
        short_ven = str(getattr(candidate, "short_venue", ""))
        pair_id = getattr(candidate, "pair_id", None)
        if not pair_id:
            pair_id = make_candidate_pair_id(symbol, long_ven, short_ven)

        # Funding prewarm: candidate must have funding timestamp evidence
        funding_ts = getattr(candidate, "funding_timestamp_ms", 0)
        if funding_ts <= 0:
            return "entry_local_l2_waiting_for_prewarm_window"

        # Primary tracking: candidate must be in primary tracked set
        if pair_id not in self._tracked_primary_pair_ids:
            return "entry_local_l2_waiting_for_primary_tracking"

        # Session dual-ready check
        session = self.entry_l2_sessions.sessions.get(pair_id)
        if session is None:
            return "entry_local_l2_waiting_for_dual_ready"

        if not session.both_legs_ready(now_ms, stale_after_ms=300_000):
            return "entry_local_l2_waiting_for_dual_ready"

        return None

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

        # V1: apply_runtime_entry_guards — 8+ gate checks before entry
        gates = [
            ("reduce_only", self._gate_reduce_only, ()),
            ("pending_close_reconciliation", self._gate_pending_close_reconciliation, ()),
            ("passive_close_in_flight", self._gate_passive_close_pending, ()),
            ("pending_entry_duplicate", self._gate_pending_entry_dedup, ()),
            ("entry_sizing", self._gate_entry_sizing, ()),
            ("venue_cooldown", self._gate_venue_cooldown, (now_ms,)),
            ("zero_fill_cooldown", self._gate_zero_fill_cooldown, (now_ms,)),
        ]
        for gate_name, gate_fn, gate_args in gates:
            allowed, reason = gate_fn(candidate, *gate_args)
            if not allowed:
                self.journal.append(
                    "runtime.entry_blocked_gate",
                    {"symbol": candidate.symbol, "gate": gate_name, "reason": reason, "ts_ms": now_ms},
                )
                # V1: review.candidate_rejected — per-candidate rejection logging
                self.journal.append(
                    "review.candidate_rejected",
                    {
                        "symbol": candidate.symbol,
                        "long_venue": candidate.long_venue,
                        "short_venue": candidate.short_venue,
                        "rejected_stage": "runtime_entry_gate",
                        "rejected_reason": f"{gate_name}: {reason}",
                        "ranking_edge_bps": candidate.ranking_edge_bps,
                        "expected_edge_bps": candidate.expected_edge_bps,
                        "funding_edge_bps": candidate.funding_edge_bps,
                        "ts_ms": now_ms,
                    },
                )
                return

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
            self.journal.append(
                "review.candidate_rejected",
                {
                    "symbol": candidate.symbol,
                    "long_venue": candidate.long_venue,
                    "short_venue": candidate.short_venue,
                    "rejected_stage": "price_gate",
                    "rejected_reason": "no valid quote",
                    "ranking_edge_bps": candidate.ranking_edge_bps,
                    "ts_ms": now_ms,
                },
            )
            return

        # Resolve venue enums from candidate string fields
        long_venue = Venue.from_str(candidate.long_venue)
        short_venue = Venue.from_str(candidate.short_venue)
        quantity = candidate.entry_notional_quote / price_hint

        # V1 runtime entry guards (apply_runtime_entry_guards)
        gate_checks = [
            ("pending_close_reconciliation", self._gate_pending_close_reconciliation),
            ("passive_close_pending", self._gate_passive_close_pending),
            ("reduce_only", self._gate_reduce_only),
            ("venue_cooldown", self._gate_venue_cooldown),
            ("zero_fill_cooldown", self._gate_zero_fill_cooldown),
        ]
        for gate_name, gate_fn in gate_checks:
            allowed, reason = gate_fn(candidate, now_ms) if gate_name in ("venue_cooldown", "zero_fill_cooldown") else gate_fn(candidate)
            if not allowed:
                self.journal.append(
                    "runtime.entry_blocked_gate",
                    {"symbol": candidate.symbol, "gate": gate_name, "reason": reason, "ts_ms": now_ms},
                )
                return

        # V1 local-L2 entry readiness gate: block entry when local-L2 enabled
        # but either leg's book is not ready (stale, degraded, cold, etc.)
        if self.config.strategy.local_l2_enabled:
            from lightfee.marketdata.liquidity import execution_liquidity_from_local_l2

            long_book = self.local_l2_runtime.get_book(long_venue.value, candidate.symbol)
            short_book = self.local_l2_runtime.get_book(short_venue.value, candidate.symbol)

            not_ready_reasons: list[str] = []
            max_age_ms = self.config.strategy.max_liquidity_snapshot_age_ms
            if long_book is None:
                not_ready_reasons.append(
                    f"long book missing: {long_venue.value}:{candidate.symbol} "
                    f"max_age_ms={max_age_ms}"
                )
            else:
                liq = execution_liquidity_from_local_l2(
                    long_book, max_age_ms=max_age_ms,
                    now_ms=now_ms, require_ready=True,
                )
                if not liq.book_ready:
                    not_ready_reasons.append(
                        f"long leg not ready: {long_venue.value}:{candidate.symbol} "
                        f"status={long_book.status.value} pool={long_book.pool.value if hasattr(long_book, 'pool') else 'unknown'} "
                        f"age={long_book.age_ms(now_ms)}ms max_age_ms={max_age_ms}"
                    )

            if short_book is None:
                not_ready_reasons.append(
                    f"short book missing: {short_venue.value}:{candidate.symbol} "
                    f"max_age_ms={max_age_ms}"
                )
            else:
                liq = execution_liquidity_from_local_l2(
                    short_book, max_age_ms=max_age_ms,
                    now_ms=now_ms, require_ready=True,
                )
                if not liq.book_ready:
                    not_ready_reasons.append(
                        f"short leg not ready: {short_venue.value}:{candidate.symbol} "
                        f"status={short_book.status.value} pool={short_book.pool.value if hasattr(short_book, 'pool') else 'unknown'} "
                        f"age={short_book.age_ms(now_ms)}ms max_age_ms={max_age_ms}"
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

        # V1: review.candidate_shortlisted — candidate passed all gates, entered shortlist
        self.journal.append(
            "review.candidate_shortlisted",
            {
                "symbol": candidate.symbol,
                "long_venue": long_venue.value,
                "short_venue": short_venue.value,
                "ranking_edge_bps": candidate.ranking_edge_bps,
                "expected_edge_bps": candidate.expected_edge_bps,
                "funding_edge_bps": candidate.funding_edge_bps,
                "worst_case_edge_bps": candidate.worst_case_edge_bps,
                "entry_notional_quote": candidate.entry_notional_quote,
                "route": route.value,
                "maker_leg": maker_leg.value if hasattr(maker_leg, 'value') else str(maker_leg),
                "ts_ms": now_ms,
            },
        )

        try:
            # V1: execution.entry_selected — engine decided to open this candidate
            self.journal.append(
                "execution.entry_selected",
                {
                    "symbol": candidate.symbol,
                    "entry_id": entry_id,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "quantity": effective_quantity,
                    "route": route.value,
                    "maker_leg": maker_leg.value if hasattr(maker_leg, 'value') else str(maker_leg),
                    "price_hint": price_hint,
                    "ts_ms": now_ms,
                },
            )
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

    def _apply_tick_backoff(self, is_active: bool = False, is_maker: bool = False) -> None:
        """Apply incremental tick-failure backoff from config floors / caps.

        V1: separate FailureBackoff per lane with unique jitter seeds:
        - full tick: seed 0x1F7A_11FE
        - active tick: seed 0x1F7A_11FF
        - maker tick: seed 0x1F7A_1200
        """
        init_ms = self.config.runtime.tick_failure_backoff_initial_ms
        max_ms = self.config.runtime.tick_failure_backoff_max_ms

        if is_maker:
            current = self._maker_tick_backoff_until_ms
        elif is_active:
            current = self._active_tick_backoff_until_ms
        else:
            current = self._tick_backoff_until_ms

        now_ms = wall_clock_now_ms()
        base_backoff = max(init_ms, (current - now_ms) * 2 if current and current > now_ms else init_ms)
        deadline_ms = now_ms + min(base_backoff, max_ms)

        if is_maker:
            self._maker_tick_backoff_until_ms = deadline_ms
        elif is_active:
            self._active_tick_backoff_until_ms = deadline_ms
        else:
            self._tick_backoff_until_ms = deadline_ms
