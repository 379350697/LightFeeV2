"""Live runtime: multi-lane tick loop, snapshot consumption, supervision, export."""

from __future__ import annotations

import asyncio
from collections import Counter
from typing import Optional

from lightfee.config.schema import AppConfig
from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import OrderFill, PositionSnapshot, Side, Venue
from lightfee.core.errors import OrderSubmitError
from lightfee.engine.reconciliation import _recon_fill_price
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
    enter_fail_closed,
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
    clear_stale_fail_closed_if_recovery_clean,
)
from lightfee.engine.state import EngineState, HedgeInflight, OpenPosition
from lightfee.engine.supervisor import Supervisor
from lightfee.persistence.journal import Journal
from lightfee.persistence.snapshot_store import SnapshotStore
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
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
        self.l2_data_plane.hot_stale_after_ms = self._configured_entry_l2_stale_after_ms(config)

        # V1 entry-local-L2 session runtime (tracked opportunities, readiness)
        from lightfee.engine.entry_local_l2 import EntryLocalL2SessionRuntime
        self.entry_l2_sessions = EntryLocalL2SessionRuntime()
        self._tracked_primary_pair_ids: set[str] = set()  # V1: primary_opportunities
        self._entry_l2_last_leg_diagnostics: dict[tuple[str, str], dict] = {}
        self._last_entry_l2_readiness_diag_fingerprint: str = ""
        self._last_entry_l2_readiness_diag_ts_ms: int = 0
        self._last_no_entry_diag_fingerprint: str = ""
        self._last_no_entry_diag_ts_ms: int = 0
        self._last_no_entry_diagnostics: dict | None = None
        self._last_private_position_probe_ms: int = 0
        self._last_position_drift_check_ms: int = 0

        # V1 recovery dedup index: prevents duplicate orders after restart
        self._recovery_dedup_index: dict[str, str] = {}

        # V1 entry gate cooldown state
        self._venue_cooldown_until_ms: dict[str, int] = {}
        self._zero_fill_cooldown_until_ms: dict[tuple, int] = {}

        # V1 live scan recovery state (B2)
        self._live_scan_success_streak: int = 0
        self._last_good_snapshot = None

        # V1 maker venue request budget tracker (CONTRACT RECOVERY-005)
        # Per-venue sliding window of operation timestamps for cancel/submit
        # rate limiting. V1: try_consume_maker_venue_request_budget
        self._maker_venue_op_history: dict[str, list[int]] = {}

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

    def _venue_min_notional(self, venue: Venue, symbol: str) -> float:
        """Return the minimum notional value for a venue/symbol pair.

        Used to prevent infinite retry of hedge orders that are below the
        venue's minimum trade notional (e.g., Hyperliquid $10 MinTradeNtl).
        """
        adapter = self.get_venue_adapter(venue)
        if adapter is None:
            return 0.0
        transport = getattr(adapter, "_transport", adapter)
        spec = getattr(transport, "_spec", None)
        if spec is not None:
            return float(getattr(spec, "min_notional", 0.0) or 0.0)
        return 0.0

    def _emit_startup_order_path_preflight(self) -> None:
        """Emit sanitized startup visibility for order signing/dependency readiness."""
        blocked = {"api_key", "api_secret", "secret", "signature", "private_key", "headers", "auth"}
        for venue, adapter in sorted(
            self._venue_adapters.items(),
            key=lambda item: item[0].value if hasattr(item[0], "value") else str(item[0]),
        ):
            transport = getattr(adapter, "_transport", adapter)
            preflight_fn = getattr(transport, "startup_preflight", None)
            if not callable(preflight_fn):
                continue
            try:
                raw_payload = preflight_fn()
            except Exception as exc:
                raw_payload = {
                    "venue": venue.value if hasattr(venue, "value") else str(venue),
                    "status": "failed",
                    "reason": str(exc),
                }
            payload = {}
            for key, value in dict(raw_payload or {}).items():
                key_s = str(key)
                if any(token in key_s.lower() for token in blocked):
                    continue
                payload[key_s] = value
            payload.setdefault("venue", venue.value if hasattr(venue, "value") else str(venue))
            payload.setdefault("status", "ok")
            self.journal.append("startup.order_path_preflight", payload)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _run_startup_phase_with_timeout(self, phase: str, coro) -> None:
        timeout_ms = max(self.config.runtime.live_startup_phase_timeout_ms, 1)
        try:
            await asyncio.wait_for(coro, timeout=timeout_ms / 1000.0)
        except asyncio.TimeoutError:
            self.journal.append(
                "runtime.startup_phase_timeout",
                {
                    "phase": phase,
                    "timeout_ms": timeout_ms,
                    "ts_ms": wall_clock_now_ms(),
                },
            )

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
        self._emit_startup_order_path_preflight()

        # Phase 2 – Resolve runtime symbols (daily-universe integration point)
        symbol_info = await prepare_runtime_symbols(self.config)

        # Phase 3 – Recover or start fresh
        self.state = recover_from_snapshot(self.snapshot_store, self.journal)
        self.state.run_id = self.journal.run_id
        if self.state.started_at_ms == 0:
            self.state.started_at_ms = wall_clock_now_ms()

        # Build recovery dedup index from recovered pending state
        self._recovery_dedup_index = build_recovery_dedup_index(self.state)
        await self._recover_startup_live_positions(
            self._startup_position_probe_symbols(symbol_info),
            wall_clock_now_ms(),
        )

        # Phase 4 – Recovery-aware startup (Rust V1: finalize_startup_position_recovery)
        from lightfee.engine.recovery import needs_reconciliation, classify_startup_recovery_state

        recovery_class = (
            "blocked"
            if self.state.recovery_blocked_reason
            else classify_startup_recovery_state(self.state)
        )

        if recovery_class == "clean":
            set_lifecycle(self.state, EngineLifecycle.RUNNING)
            clear_stale_fail_closed_if_recovery_clean(self.state, self.journal)
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
            elif self.state.lifecycle == EngineLifecycle.RECONCILING:
                # Safety net: if _recover_pending_entry_hedges returned early
                # (e.g. no venue adapters) without finalizing, do it now.
                self._finalize_startup_recovery()
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
        await self._run_startup_phase_with_timeout(
            "local_l2_activation",
            self._activate_local_l2_phase(wall_clock_now_ms()),
        )

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

    def _startup_position_probe_symbols(self, symbol_info: object) -> list[str]:
        """Symbols to probe for live startup position recovery."""
        symbols: list[str] = []
        if isinstance(symbol_info, dict):
            raw = symbol_info.get("resolved_symbols") or []
            symbols.extend(str(s) for s in raw if str(s))
        symbols.extend(str(s) for s in getattr(self.config, "symbols", []) if str(s))

        for pos in self.state.open_positions.values():
            if pos.symbol:
                symbols.append(pos.symbol)
        for pending in self.state.pending_entries.values():
            if pending.symbol:
                symbols.append(pending.symbol)

        seen: set[str] = set()
        result: list[str] = []
        for symbol in symbols:
            if symbol not in seen:
                seen.add(symbol)
                result.append(symbol)
        return result

    async def _recover_startup_live_positions(
        self,
        symbols: list[str],
        now_ms: int,
        *,
        source: str = "startup_live_position_probe",
    ) -> None:
        """Detect balanced exchange positions that local snapshot/journal missed."""
        if str(getattr(self.config.runtime, "mode", "")).lower() != "live":
            return
        if not symbols or not self._venue_adapters:
            return
        if (
            self.state.open_positions
            or self.state.pending_entries
            or self.state.pending_closes
            or self.state.pending_passive_closes
        ):
            return

        snapshots = await self._fetch_startup_live_position_snapshots(symbols)
        if not snapshots:
            return

        created, recovered_indices = self._hydrate_balanced_startup_live_positions(
            snapshots, now_ms, source=source
        )
        mismatches = [
            item for idx, item in enumerate(snapshots)
            if idx not in recovered_indices
        ]
        if mismatches:
            flattened = await self._flatten_startup_live_position_mismatches(
                mismatches, now_ms, source=source
            )
            if not flattened:
                self._block_unpaired_startup_live_positions(
                    mismatches,
                    now_ms,
                    source=source,
                    recovered_open_positions=created,
                    reason="live_position_mismatch_flatten_failed",
                )
                return
        if created or mismatches:
            self.journal.append(
                "recovery.live_position_probe_complete",
                {
                    "detected_positions": len(snapshots),
                    "recovered_open_positions": created,
                    "mismatch_positions": len(mismatches),
                    "ts_ms": now_ms,
                },
            )

    def _block_unpaired_startup_live_positions(
        self,
        snapshots: list[tuple[str, PositionSnapshot]],
        now_ms: int,
        *,
        source: str,
        recovered_open_positions: int,
        reason: str = "unpaired_live_positions_detected",
    ) -> None:
        enter_fail_closed(self.state)
        self.state.recovery_blocked_reason = reason
        self.state.recovery_blocked_at_ms = now_ms
        self.state.last_error = "live exchange position mismatch cleanup failed"
        self.journal.append(
            "recovery.blocked",
            {
                "reason": self.state.recovery_blocked_reason,
                "source": source,
                "detected_positions": len(snapshots),
                "recovered_open_positions": recovered_open_positions,
                "positions": [
                    {
                        "requested_symbol": requested_symbol,
                        "venue": pos.venue.value,
                        "symbol": pos.symbol,
                        "side": pos.side.value,
                        "quantity": pos.quantity,
                        "entry_price": pos.entry_price,
                    }
                    for requested_symbol, pos in snapshots
                ],
                "ts_ms": now_ms,
            },
        )

    async def _flatten_startup_live_position_mismatches(
        self,
        snapshots: list[tuple[str, PositionSnapshot]],
        now_ms: int,
        *,
        source: str,
    ) -> bool:
        flattened: list[dict[str, object]] = []
        failed: list[dict[str, object]] = []
        for requested_symbol, pos in snapshots:
            if abs(pos.quantity) <= 1e-9:
                continue
            ok = await self._cleanup_failed_leg_exposure(
                pos.venue,
                requested_symbol,
                f"live-recovery:{source}:{requested_symbol}:{pos.venue.value}",
                "live_recovery_mismatch",
            )
            payload = {
                "requested_symbol": requested_symbol,
                "venue": pos.venue.value,
                "symbol": pos.symbol,
                "side": pos.side.value,
                "quantity": pos.quantity,
                "entry_price": pos.entry_price,
            }
            if ok is True:
                flattened.append(payload)
            else:
                payload["cleanup_result"] = ok
                failed.append(payload)

        if failed:
            self.journal.append(
                "recovery.live_mismatch_flatten_failed",
                {
                    "source": source,
                    "flattened_positions": flattened,
                    "failed_positions": failed,
                    "ts_ms": now_ms,
                },
            )
            return False

        self.journal.append(
            "recovery.live_mismatch_flattened",
            {
                "source": source,
                "positions": flattened,
                "ts_ms": now_ms,
            },
        )
        return True

    async def _maybe_recover_clean_live_positions(self, now_ms: int) -> None:
        """Probe private positions when the runtime would otherwise look clean."""
        if str(getattr(self.config.runtime, "mode", "")).lower() != "live":
            return
        if (
            self.state.open_positions
            or self.state.pending_entries
            or self.state.pending_closes
        ):
            return

        interval_ms = max(self.config.runtime.private_position_max_age_ms, 1)
        if (
            self._last_private_position_probe_ms > 0
            and now_ms < self._last_private_position_probe_ms + interval_ms
        ):
            return

        self._last_private_position_probe_ms = now_ms
        await self._recover_startup_live_positions(
            self._startup_position_probe_symbols({}),
            now_ms,
            source="runtime_live_position_probe",
        )

    async def _fetch_startup_live_position_snapshots(
        self, symbols: list[str]
    ) -> list[tuple[str, PositionSnapshot]]:
        timeout_s = max(self.config.runtime.live_recovery_rest_probe_timeout_ms, 1) / 1000.0
        semaphore = asyncio.Semaphore(8)
        probe_symbols = {str(symbol) for symbol in symbols}

        def is_active_probe_position(pos: PositionSnapshot) -> bool:
            return (
                abs(getattr(pos, "quantity", 0.0)) > 1e-9
                and str(getattr(pos, "symbol", "")) in probe_symbols
            )

        async def fetch_all_for_venue(venue: Venue, adapter: VenueAdapter):
            async with semaphore:
                try:
                    positions = await asyncio.wait_for(
                        adapter.fetch_all_positions(),
                        timeout=timeout_s,
                    )
                except Exception as e:
                    self.journal.append(
                        "recovery.live_position_bulk_probe_error",
                        {
                            "venue": venue.value,
                            "error": str(e),
                        },
                    )
                    return (venue, None)
                if positions is None:
                    return (venue, None)
                return (
                    venue,
                    [
                        (pos.symbol, pos)
                        for pos in positions
                        if is_active_probe_position(pos)
                    ],
                )

        async def fetch_one(venue: Venue, adapter: VenueAdapter, symbol: str):
            async with semaphore:
                try:
                    pos = await asyncio.wait_for(
                        adapter.fetch_position(symbol),
                        timeout=timeout_s,
                    )
                    return (symbol, pos)
                except Exception as e:
                    self.journal.append(
                        "recovery.live_position_probe_error",
                        {
                            "venue": venue.value,
                            "symbol": symbol,
                            "error": str(e),
                        },
                    )
                    return None

        bulk_results = await asyncio.gather(
            *[
                fetch_all_for_venue(venue, adapter)
                for venue, adapter in self._venue_adapters.items()
            ]
        )
        snapshots: list[tuple[str, PositionSnapshot]] = []
        fallback_venues: set[Venue] = set()
        for venue, positions in bulk_results:
            if positions is None:
                fallback_venues.add(venue)
            else:
                snapshots.extend(positions)

        tasks = [
            fetch_one(venue, adapter, symbol)
            for symbol in symbols
            for venue, adapter in self._venue_adapters.items()
            if venue in fallback_venues
        ]
        results = await asyncio.gather(*tasks) if tasks else []
        snapshots.extend(
            item for item in results
            if item is not None and abs(getattr(item[1], "quantity", 0.0)) > 1e-9
        )
        return snapshots

    def _hydrate_balanced_startup_live_positions(
        self,
        snapshots: list[tuple[str, PositionSnapshot]],
        now_ms: int,
        *,
        source: str,
    ) -> tuple[int, set[int]]:
        by_symbol: dict[str, list[tuple[int, PositionSnapshot]]] = {}
        for idx, (requested_symbol, pos) in enumerate(snapshots):
            by_symbol.setdefault(requested_symbol, []).append((idx, pos))

        created = 0
        recovered_indices: set[int] = set()
        for symbol, indexed_positions in by_symbol.items():
            active = [
                (idx, p) for idx, p in indexed_positions
                if abs(p.quantity) > 1e-9
            ]
            if len(active) != 2:
                continue

            (idx_a, pos_a), (idx_b, pos_b) = active
            if pos_a.venue == pos_b.venue or pos_a.side == pos_b.side:
                continue
            if abs(abs(pos_a.quantity) - abs(pos_b.quantity)) > 1e-9:
                continue

            if pos_a.side == Side.BUY:
                long_idx, long_pos = idx_a, pos_a
                short_idx, short_pos = idx_b, pos_b
            else:
                long_idx, long_pos = idx_b, pos_b
                short_idx, short_pos = idx_a, pos_a

            if self._has_open_position_pair(symbol, long_pos.venue, short_pos.venue):
                recovered_indices.update({long_idx, short_idx})
                continue

            position_id = (
                f"live-recovered:{symbol}:"
                f"{long_pos.venue.value}->{short_pos.venue.value}"
            )
            matched_quantity = abs(long_pos.quantity)
            position = OpenPosition(
                position_id=position_id,
                symbol=symbol,
                long_venue=long_pos.venue,
                short_venue=short_pos.venue,
                long_quantity=abs(long_pos.quantity),
                short_quantity=abs(short_pos.quantity),
                long_entry_price=long_pos.entry_price,
                short_entry_price=short_pos.entry_price,
                opened_at_ms=now_ms,
                matched_quantity=matched_quantity,
                opportunity_hint_source=source,
            )
            self.state.open_positions[position_id] = position
            recovered_indices.update({long_idx, short_idx})
            self.journal.append(
                "recovery.live_detected",
                {
                    "position_id": position.position_id,
                    "symbol": position.symbol,
                    "long_venue": position.long_venue.value,
                    "short_venue": position.short_venue.value,
                    "quantity": position.matched_quantity,
                    "long_quantity": position.long_quantity,
                    "short_quantity": position.short_quantity,
                    "long_entry_price": position.long_entry_price,
                    "short_entry_price": position.short_entry_price,
                    "opened_at_ms": position.opened_at_ms,
                    "matched_quantity": position.matched_quantity,
                    "opportunity_hint_source": position.opportunity_hint_source,
                    "source": source,
                    "ts_ms": now_ms,
                },
            )
            created += 1

        return created, recovered_indices

    def _has_open_position_pair(
        self, symbol: str, long_venue: Venue, short_venue: Venue
    ) -> bool:
        return any(
            pos.symbol == symbol
            and pos.long_venue == long_venue
            and pos.short_venue == short_venue
            for pos in self.state.open_positions.values()
        )

    async def _maybe_check_active_position_drift(self, now_ms: int) -> None:
        if str(getattr(self.config.runtime, "mode", "")).lower() != "live":
            return
        if not self.state.open_positions:
            return

        interval_ms = max(self.config.runtime.private_position_max_age_ms, 1)
        if (
            self._last_position_drift_check_ms > 0
            and now_ms < self._last_position_drift_check_ms + interval_ms
        ):
            return
        self._last_position_drift_check_ms = now_ms

        for position in list(self.state.open_positions.values()):
            if position.position_id in self.state.pending_passive_closes:
                continue
            if any(
                pending.position_id == position.position_id
                for pending in self.state.pending_closes.values()
            ):
                continue

            long_adapter = self.get_venue_adapter(position.long_venue)
            short_adapter = self.get_venue_adapter(position.short_venue)
            if long_adapter is None or short_adapter is None:
                continue

            try:
                long_pos = await long_adapter.fetch_position(position.symbol)
                short_pos = await short_adapter.fetch_position(position.symbol)
            except Exception as e:
                self.journal.append(
                    "runtime.position_drift_probe_error",
                    {
                        "position_id": position.position_id,
                        "symbol": position.symbol,
                        "error": str(e),
                    },
                )
                continue

            expected_long = abs(position.long_quantity or position.matched_quantity)
            expected_short = abs(position.short_quantity or position.matched_quantity)
            long_valid_qty = (
                abs(long_pos.quantity)
                if long_pos.side == Side.BUY and abs(long_pos.quantity) > 1e-9
                else 0.0
            )
            short_valid_qty = (
                abs(short_pos.quantity)
                if short_pos.side == Side.SELL and abs(short_pos.quantity) > 1e-9
                else 0.0
            )

            if (
                abs(long_valid_qty - expected_long) <= 1e-9
                and abs(short_valid_qty - expected_short) <= 1e-9
            ):
                continue

            balanced_quantity = min(long_valid_qty, short_valid_qty)
            self.journal.append(
                "runtime.position_drift_detected",
                {
                    "position_id": position.position_id,
                    "symbol": position.symbol,
                    "long_venue": position.long_venue.value,
                    "short_venue": position.short_venue.value,
                    "expected_long_quantity": expected_long,
                    "expected_short_quantity": expected_short,
                    "actual_long_side": long_pos.side.value,
                    "actual_long_quantity": long_pos.quantity,
                    "actual_short_side": short_pos.side.value,
                    "actual_short_quantity": short_pos.quantity,
                    "balanced_quantity": balanced_quantity,
                    "ts_ms": now_ms,
                },
            )

            long_excess = (
                abs(long_pos.quantity) - balanced_quantity
                if abs(long_pos.quantity) > 1e-9
                else 0.0
            )
            short_excess = (
                abs(short_pos.quantity) - balanced_quantity
                if abs(short_pos.quantity) > 1e-9
                else 0.0
            )
            long_ok = True
            short_ok = True
            if long_excess > 1e-9:
                long_ok = await self._flatten_live_position_leg_quantity(
                    position.long_venue,
                    position.symbol,
                    long_pos,
                    long_excess,
                    position.position_id,
                    "runtime_drift_flatten_long",
                )
            if short_excess > 1e-9:
                short_ok = await self._flatten_live_position_leg_quantity(
                    position.short_venue,
                    position.symbol,
                    short_pos,
                    short_excess,
                    position.position_id,
                    "runtime_drift_flatten_short",
                )

            if long_ok is not True or short_ok is not True:
                enter_fail_closed(self.state)
                self.state.recovery_blocked_reason = "position_drift_correction_failed"
                self.state.recovery_blocked_at_ms = now_ms
                self.state.last_error = "position drift correction failed"
                self.journal.append(
                    "runtime.position_drift_correction_failed",
                    {
                        "position_id": position.position_id,
                        "symbol": position.symbol,
                        "long_flatten_result": long_ok,
                        "short_flatten_result": short_ok,
                        "ts_ms": now_ms,
                    },
                )
                continue

            if balanced_quantity <= 1e-9:
                self.state.open_positions.pop(position.position_id, None)
                self.journal.append(
                    "recovery.flat",
                    {
                        "position_id": position.position_id,
                        "symbol": position.symbol,
                        "source": "runtime_position_drift",
                        "ts_ms": now_ms,
                    },
                )
            else:
                current = self.state.open_positions.get(position.position_id)
                if current is not None:
                    current.long_quantity = balanced_quantity
                    current.short_quantity = balanced_quantity
                    current.matched_quantity = balanced_quantity
                self.journal.append(
                    "runtime.position_drift_corrected",
                    {
                        "position_id": position.position_id,
                        "symbol": position.symbol,
                        "new_quantity": balanced_quantity,
                        "ts_ms": now_ms,
                    },
                )
            self.snapshot_store.write(self.state.to_dict())

    async def _flatten_live_position_leg_quantity(
        self,
        venue: Venue,
        symbol: str,
        live_position: PositionSnapshot,
        quantity: float,
        position_id: str,
        stage: str,
    ) -> bool | None:
        adapter = self.get_venue_adapter(venue)
        if adapter is None:
            return None
        if quantity <= 1e-9:
            return True

        cleanup_side = live_position.side.opposite()
        from lightfee.venues.cid import generate_exchange_cid
        cleanup_client_order_id = generate_exchange_cid(
            f"{position_id}:{stage}:{symbol}", "c", venue
        )
        self.journal.append(
            "runtime.position_drift_flatten_leg",
            {
                "position_id": position_id,
                "stage": stage,
                "venue": venue.value,
                "symbol": symbol,
                "live_side": live_position.side.value,
                "quantity": quantity,
                "cleanup_side": cleanup_side.value,
                "cleanup_client_order_id": cleanup_client_order_id,
            },
        )

        try:
            from lightfee.core.domain import OrderRequest

            req = OrderRequest(
                venue=venue,
                symbol=symbol,
                side=cleanup_side,
                quantity=abs(quantity),
                price=None,
                post_only=False,
                reduce_only=True,
                client_order_id=cleanup_client_order_id,
            )
            fill = await adapter.place_order(req)
            self._flush_adapter_order_diagnostics(adapter)
            return fill.quantity >= abs(quantity) - 1e-9
        except Exception:
            self._flush_adapter_order_diagnostics(adapter)
            return False

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
        stale_after_ms = self._entry_local_l2_stale_after_ms()
        for c in candidates:
            sym = getattr(c, 'symbol', '')
            for ven_str in (getattr(c, 'long_venue', ''), getattr(c, 'short_venue', '')):
                if not ven_str or not sym:
                    continue
                # Skip if already active
                book = self.local_l2_runtime.get_book(ven_str, sym)
                if book is not None:
                    if book.status == L2BookStatus.HOT:
                        stale = book.is_stale(stale_after_ms, now_ms)
                        crossed = book.has_crossed_book()
                        if not stale and not crossed:
                            continue
                        book.transition_to_rebuilding(now_ms)
                        book.fault_reason = (
                            "crossed_or_locked_book"
                            if crossed and not stale
                            else "stale_hot_book"
                        )
                    elif book.status == L2BookStatus.BOOTSTRAPPING:
                        continue
                needed.setdefault(ven_str, set()).add(sym)

        if not needed:
            return

        per_venue_budget = max(
            getattr(self.config.strategy, 'local_l2_hot_exec_per_venue_budget', 20), 1,
        )
        from lightfee.marketdata.local_l2_venues import get_venue_rules

        registered_total = 0
        registered_venues: set[str] = set()
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

            if getattr(self.config.strategy, 'local_l2_ws_enabled', False):
                registered = self.l2_data_plane.start_ws_streams(
                    ven_str, symbols_list, adapter=adapter,
                )
                if registered > 0:
                    registered_total += registered
                    registered_venues.add(ven_str)

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

        if registered_total > 0:
            connected = await self.l2_data_plane.connect_ws_streams()
            self.journal.append(
                "runtime.local_l2_dynamic_ws_started",
                {
                    "registered_stream_count": registered_total,
                    "connected_stream_count": connected,
                    "venues": sorted(registered_venues),
                    "ts_ms": wall_clock_now_ms(),
                },
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
        last_good_max_age = self.config.runtime.live_scan_last_good_max_age_ms

        # V1: evaluate_snapshot_freshness — multi-state freshness evaluation
        freshness = evaluate_snapshot_freshness(
            snapshot=snapshot,
            max_age_ms=max_age,
            now_ms=now_ms,
            last_good=self._last_good_snapshot,
            last_good_max_age_ms=last_good_max_age,
            market_max_age_ms=self.config.runtime.max_market_age_ms,
        )
        if freshness == SnapshotFreshness.MISSING:
            self._live_scan_success_streak = 0
            self.journal.append("runtime.snapshot_missing", {"ts_ms": now_ms})
            return
        if freshness == SnapshotFreshness.STALE:
            self._live_scan_success_streak = 0
            self.journal.append(
                "runtime.snapshot_stale",
                self._snapshot_health_payload(
                    snapshot=snapshot,
                    now_ms=now_ms,
                    max_age_ms=max_age,
                    freshness="stale",
                ),
            )
            return
        if freshness == SnapshotFreshness.DEGRADED:
            # Some venues degraded but can still trade on healthy ones
            self._live_scan_success_streak += 1
            self._last_good_snapshot = snapshot
            self.journal.append(
                "runtime.snapshot_degraded",
                self._snapshot_health_payload(
                    snapshot=snapshot,
                    now_ms=now_ms,
                    max_age_ms=max_age,
                    freshness="degraded",
                ),
            )
        if freshness == SnapshotFreshness.LAST_GOOD_FALLBACK:
            # Current snapshot is stale/missing; fall back to last good
            snapshot = snapshot if snapshot is not None else self._last_good_snapshot
            if snapshot is None:
                self._live_scan_success_streak = 0
                self.journal.append("runtime.snapshot_missing", {"ts_ms": now_ms})
                return
            self._last_good_snapshot = snapshot
            self._live_scan_success_streak += 1
            self.journal.append(
                "runtime.snapshot_fallback_last_good",
                self._snapshot_health_payload(
                    snapshot=snapshot,
                    now_ms=now_ms,
                    max_age_ms=max_age,
                    freshness="last_good_fallback",
                ),
            )
        if freshness == SnapshotFreshness.FRESH:
            self._live_scan_success_streak += 1
            self._last_good_snapshot = snapshot

        self.state.last_scan = {
            "ts_ms": now_ms,
            "snapshot_freshness": freshness.value if hasattr(freshness, "value") else str(freshness),
            "candidate_count": len(snapshot.candidates) if snapshot is not None else 0,
            "tradeable_count": 0,
            "selected_candidate_count": 0,
            "dispatched_candidate_count": 0,
            "degraded_venues": list(getattr(snapshot, "degraded_venues", [])) if snapshot is not None else [],
            "no_entry_reason": None,
        }
        if freshness == SnapshotFreshness.LAST_GOOD_FALLBACK:
            reason = "live_scan_revalidate_required:last_good_sidecar"
            self.state.last_scan["no_entry_reason"] = reason
            self.journal.append(
                "runtime.live_scan_revalidate_required",
                {
                    "reason": reason,
                    "candidate_count": len(snapshot.candidates) if snapshot is not None else 0,
                    "edge_buffer_bps": self.config.runtime.live_scan_revalidate_edge_buffer_bps,
                    "ts_ms": now_ms,
                },
            )
            return

        # V1 pre-scan L2 sync: refresh execution-owned books only (scan_promoted=False)
        await self._sync_local_l2_data(now_ms, scan_promoted=False)

        # --- Build price lookup from snapshot quotes ---
        price_hints: dict[str, float] = {}
        stale_order_quote_count = 0
        for quote in snapshot.quotes.values():
            quote_observed_at_ms = (
                int(getattr(quote, "observed_at_ms", 0) or 0)
                or int(getattr(snapshot, "market_observed_at_ms", 0) or 0)
            )
            if (
                quote_observed_at_ms > 0
                and now_ms - quote_observed_at_ms
                > self.config.runtime.max_order_quote_age_ms
            ):
                stale_order_quote_count += 1
                continue
            price_hints[quote.symbol] = (quote.bid + quote.ask) / 2.0 if quote.bid > 0 and quote.ask > 0 else 0.0
        if stale_order_quote_count > 0:
            self.journal.append(
                "runtime.order_quote_stale_skipped",
                {
                    "count": stale_order_quote_count,
                    "max_age_ms": self.config.runtime.max_order_quote_age_ms,
                    "ts_ms": now_ms,
                },
            )

        # --- Discover tradeable candidates ---
        # V1 live scan recovery gate: require consecutive fresh snapshots before entry
        live_scan_recovery_count = getattr(
            self.config.runtime,
            'live_scan_recovery_success_count',
            getattr(self.config.strategy, 'live_scan_recovery_success_count', 3),
        )
        if self._live_scan_success_streak < live_scan_recovery_count:
            self.state.last_scan["no_entry_reason"] = "live_scan_recovery_warmup"
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
            self.state.last_scan["tradeable_count"] = len(tradeable)
            self.state.last_scan["selected_candidate_count"] = 0
            self.state.last_scan["dispatched_candidate_count"] = 0
            if not tradeable:
                self.state.last_scan["no_entry_reason"] = "no_tradeable_candidates"
            if tradeable:
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
                        self.config.strategy,
                        "shadow_entry_opportunity_count",
                        getattr(self.config.strategy, "entry_local_l2_shadow_count", 2),
                    )
                    from lightfee.engine.entry_local_l2 import select_tracked_opportunities

                    tracked = select_tracked_opportunities(
                        tradeable, primary_count, shadow_count,
                    )
                    tracked_pair_ids = {t.pair_id for t in tracked}
                    tracked_candidates = [
                        candidate for candidate in tradeable
                        if self._candidate_pair_id(candidate) in tracked_pair_ids
                    ]
                    # V1: activity_local_l2_symbols() follows the tracked
                    # primary+shadow scope, not the whole tradeable shortlist.
                    await self._ensure_l2_active_for_candidates(
                        tracked_candidates,
                        now_ms,
                    )
                    self._tracked_primary_pair_ids = {
                        t.pair_id for t in tracked
                        if t.class_.value == "primary_tracked"
                    }
                    # Refresh session state for all tracked opportunities
                    for t in tracked:
                        self.entry_l2_sessions.track_opportunity(t, now_ms)
                    # V1 post-shortlist L2 sync after tracking: local books
                    # drive session readiness before the selection blocker.
                    await self._sync_local_l2_data(now_ms, scan_promoted=True)
                    self._refresh_entry_l2_session_readiness(now_ms)
                # V1: selected_candidates is a final-entry list, not the raw
                # shortlist. It excludes candidates still waiting on the final
                # entry window, primary L2 tracking, or dual-ready books.
                max_slots = max(self.config.strategy.max_concurrent_positions, 1)
                remaining_slots = max(max_slots - len(self.state.open_positions), 0)
                admission_blocker_counts: Counter[str] = Counter()
                selection_blocker_counts: Counter[str] = Counter()
                candidate_blockers: dict[str, str] = {}
                finalists = self._select_entry_candidates(
                    tradeable,
                    now_ms=now_ms,
                    remaining_slots=remaining_slots,
                    selection_blocker_counts=selection_blocker_counts,
                    candidate_blockers=candidate_blockers,
                    market_quotes=snapshot.quotes,
                    admission_blocker_counts=admission_blocker_counts,
                )
                self.state.last_scan["selected_candidate_count"] = len(finalists)
                dispatched = 0
                for candidate in finalists:
                    if len(self.state.open_positions) >= max_slots:
                        break
                    mid_price = price_hints.get(candidate.symbol, 0.0)
                    if await self._dispatch_entry(candidate, now_ms, price_hint=mid_price):
                        dispatched += 1
                self.state.last_scan["dispatched_candidate_count"] = dispatched
                if dispatched == 0:
                    reason = (
                        self._v1_tradeable_no_entry_reason(
                            selection_blocker_counts,
                            admission_blocker_counts,
                        )
                        or "no_entry_dispatched"
                    )
                    self._emit_scan_no_entry_diagnostics(
                        reason=reason,
                        snapshot=snapshot,
                        tradeable=tradeable,
                        selected_candidate_count=len(finalists),
                        dispatched_candidate_count=dispatched,
                        remaining_slots=remaining_slots,
                        tradeable_selection_blocker_counts=selection_blocker_counts,
                        candidate_blockers=candidate_blockers,
                        now_ms=now_ms,
                        admission_blocker_counts=admission_blocker_counts,
                    )
            elif can_enter_new_positions(self.state) and self.entry_executor is not None:
                self._emit_scan_no_entry_diagnostics(
                    reason="no_tradeable_candidates",
                    snapshot=snapshot,
                    tradeable=[],
                    selected_candidate_count=0,
                    dispatched_candidate_count=0,
                    remaining_slots=max(
                        self.config.strategy.max_concurrent_positions,
                        1,
                    ) - len(self.state.open_positions),
                    tradeable_selection_blocker_counts=Counter(),
                    candidate_blockers={},
                    now_ms=now_ms,
                )

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

        await self._maybe_check_active_position_drift(now_ms)
        if not self.state.open_positions:
            return

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

        V1 parity (live tick hedge drive):
        After reconciliation resolves maker fills, if the pending entry has
        a missing hedge quantity > 0 and no inflight hedge, submits the hedge
        IOC/taker order.  On hedge fill, finalizes the entry → OpenPosition,
        writes entry.opened/runtime.position_opened, removes pending entry.
        """
        if self.reconciler is None or not self._venue_adapters:
            return

        # --- Process pending entries: reconcile + drive missing hedge ---
        resolved_entry_ids: list[str] = []
        for entry_id, pending in list(self.state.pending_entries.items()):
            if getattr(pending, "outcome", "") == "rejected":
                if not pending.has_any_fill():
                    self.journal.append(
                        "reconciliation.rejected_pending_cleared",
                        {
                            "entry_id": entry_id,
                            "symbol": pending.symbol,
                            "reason": "maker rejected is terminal in V1",
                        },
                    )
                    resolved_entry_ids.append(entry_id)
                    continue
                self.journal.append(
                    "reconciliation.rejected_pending_retained_with_fill",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "maker_leg_filled": pending.maker_leg_filled,
                        "hedge_leg_filled": pending.hedge_leg_filled,
                        "reason": "rejected pending contains fill evidence; manual recovery required",
                    },
                )
                self._apply_reconcile_backoff(pending, now_ms)
                continue

            if not pending.uncertain_outcome:
                resolved_entry_ids.append(entry_id)
                continue

            # Respect backoff window
            if pending.reconcile_next_attempt_ms > 0 and now_ms < pending.reconcile_next_attempt_ms:
                continue

            # V1: abandon via live-size probe, not hard deadline.
            if pending.reconcile_attempt >= 1:
                abandoned = await self._try_abandon_stale_entry(pending, entry_id)
                if abandoned:
                    resolved_entry_ids.append(entry_id)
                    continue

            pending.reconcile_attempt += 1
            try:
                # V1: prefer hedge_inflight CID for reconciliation queries
                hedge_lookup_cid = pending.hedge_inflight.client_order_id if pending.hedge_inflight else pending.hedge_client_order_id
                result = await self.reconciler.reconcile_position(
                    position_id=entry_id,
                    symbol=pending.symbol,
                    long_venue=pending.long_venue,
                    short_venue=pending.short_venue,
                    long_order_id=pending.maker_order_id,
                    short_order_id=pending.hedge_order_id,
                    long_client_order_id=pending.maker_client_order_id,
                    short_client_order_id=hedge_lookup_cid,
                )
                self._flush_reconciler_order_diagnostics()
            except Exception as e:
                self._flush_reconciler_order_diagnostics()
                self.journal.append(
                    "reconciliation.entry_reconcile_error",
                    {"entry_id": entry_id, "error": str(e)},
                )
                self._apply_reconcile_backoff(pending, now_ms)
                continue

            # --- V1: write back fill quantities from reconciliation ---
            prev_maker_filled = pending.maker_leg_filled
            prev_hedge_filled = pending.hedge_leg_filled
            maker_filled_updated = False
            hedge_filled_updated = False

            if result.long_fill is not None and result.long_fill.quantity > 0:
                if pending.maker_leg == "long":
                    if result.long_fill.quantity > pending.maker_leg_filled:
                        pending.maker_leg_filled = result.long_fill.quantity
                        pending.maker_fill_price = _recon_fill_price(result.long_fill)
                        maker_filled_updated = True
                else:
                    if result.long_fill.quantity > pending.hedge_leg_filled:
                        pending.hedge_leg_filled = result.long_fill.quantity
                        pending.hedge_fill_price = _recon_fill_price(result.long_fill)
                        pending.hedge_order_id = result.long_fill.order_id
                        hedge_filled_updated = True

            if result.short_fill is not None and result.short_fill.quantity > 0:
                if pending.maker_leg == "short":
                    if result.short_fill.quantity > pending.maker_leg_filled:
                        pending.maker_leg_filled = result.short_fill.quantity
                        pending.maker_fill_price = _recon_fill_price(result.short_fill)
                        maker_filled_updated = True
                else:
                    if result.short_fill.quantity > pending.hedge_leg_filled:
                        pending.hedge_leg_filled = result.short_fill.quantity
                        pending.hedge_fill_price = _recon_fill_price(result.short_fill)
                        pending.hedge_order_id = result.short_fill.order_id
                        hedge_filled_updated = True

            # Also update from position snapshots if fill data wasn't available
            if result.long_position is not None and abs(result.long_position.quantity) > 0:
                pos_qty = abs(result.long_position.quantity)
                if pending.maker_leg == "long" and pos_qty > pending.maker_leg_filled:
                    pending.maker_leg_filled = pos_qty
                    maker_filled_updated = True
                elif pending.maker_leg == "short" and pos_qty > pending.hedge_leg_filled:
                    pending.hedge_leg_filled = pos_qty
                    hedge_filled_updated = True

            if result.short_position is not None and abs(result.short_position.quantity) > 0:
                pos_qty = abs(result.short_position.quantity)
                if pending.maker_leg == "short" and pos_qty > pending.maker_leg_filled:
                    pending.maker_leg_filled = pos_qty
                    maker_filled_updated = True
                elif pending.maker_leg == "long" and pos_qty > pending.hedge_leg_filled:
                    pending.hedge_leg_filled = pos_qty
                    hedge_filled_updated = True

            if maker_filled_updated:
                self.journal.append(
                    "pending_entry.maker_progress_applied",
                    {
                        "entry_id": entry_id,
                        "prev_maker_filled": prev_maker_filled,
                        "new_maker_filled": pending.maker_leg_filled,
                        "maker_fill_price": pending.maker_fill_price,
                    },
                )

            if hedge_filled_updated:
                self.journal.append(
                    "pending_entry.hedge_progress_applied",
                    {
                        "entry_id": entry_id,
                        "prev_hedge_filled": prev_hedge_filled,
                        "new_hedge_filled": pending.hedge_leg_filled,
                        "hedge_fill_price": pending.hedge_fill_price,
                    },
                )

            # --- V1: check if both legs are now filled → finalize ---
            if pending.missing_hedge_quantity() <= 1e-9 and pending.maker_completed():
                await self._finalize_pending_entry(pending, entry_id, now_ms)
                resolved_entry_ids.append(entry_id)
                continue

            if result.long_status == "filled" and result.short_status == "filled":
                await self._finalize_pending_entry(pending, entry_id, now_ms)
                resolved_entry_ids.append(entry_id)
                self.journal.append(
                    "reconciliation.entry_resolved",
                    {"entry_id": entry_id, "long_status": result.long_status, "short_status": result.short_status},
                )
                continue
            elif result.is_flat:
                resolved_entry_ids.append(entry_id)
                self.journal.append(
                    "reconciliation.entry_cleared_flat",
                    {"entry_id": entry_id},
                )
                continue

            # --- Clear stale hedge inflight after negative evidence ---
            if pending.hedge_inflight is not None:
                self._try_clear_stale_hedge_inflight(pending, entry_id, result, now_ms)

            # --- V1: hedge deadline check ---
            # If inflight hedge has exceeded its hard deadline, abort fail-closed
            # before attempting another hedge submit.
            if pending.hedge_inflight is not None:
                deadline = self._pending_entry_hedge_deadline_decision(pending, now_ms)
                if deadline.get("hard_breached"):
                    self.journal.append(
                        "pending_entry.hedge_deadline_breached",
                        {
                            "entry_id": entry_id,
                            "symbol": pending.symbol,
                            "hedge_venue": pending.hedge_venue().value,
                            "hedge_elapsed_ms": pending.hedge_inflight.elapsed_ms(now_ms),
                            "deadline_ms": deadline["hard_deadline_ms"],
                            "attempt": pending.hedge_inflight.attempt,
                        },
                    )
                    removed = await self._abort_pending_entry_fail_closed(
                        pending, entry_id,
                        "entry hedge deadline breached during reconciliation",
                    )
                    if removed:
                        resolved_entry_ids.append(entry_id)
                    continue

            # --- V1: terminalization budget check ---
            # Entries past their hard ceiling must go through cleanup/abort,
            # never direct pop.  min-notional residuals (repair_state set) past
            # hard ceiling also go through cleanup — they must not hang forever.
            budget = self._pending_entry_terminalization_budget(pending, now_ms)
            if budget is not None and budget.get("hard_ceiling_reached"):
                if pending.repair_state:
                    self.journal.append(
                        "pending_entry.min_notional_hard_ceiling_cleanup",
                        {
                            "entry_id": entry_id,
                            "symbol": pending.symbol,
                            "repair_state": pending.repair_state,
                            "final_reason": budget["final_reason"],
                            "lifetime_ms": budget["lifetime_ms"],
                        },
                    )
                if not pending.has_any_fill():
                    # Zero-fill entry past hard ceiling → live-size probe
                    # before popping to verify no residual exists.
                    abandoned = await self._try_abandon_stale_entry(pending, entry_id)
                    if abandoned:
                        resolved_entry_ids.append(entry_id)
                        continue
                    # Probe failed → abort (with cleanup) per V1
                removed = await self._abort_pending_entry(
                    pending, entry_id, budget["final_reason"]
                )
                if removed:
                    resolved_entry_ids.append(entry_id)
                continue
            if budget is not None and budget.get("force_terminal_reached"):
                # Zero-fill entry past force_terminal threshold → safe to
                # pop directly (no real exposure on either leg).
                self.journal.append(
                    "pending_entry.force_terminalized",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "reason": budget["final_reason"],
                        "lifetime_ms": budget["lifetime_ms"],
                    },
                )
                resolved_entry_ids.append(entry_id)
                continue

            # --- V1: drive missing hedge on normal tick ---
            missing = pending.missing_hedge_quantity()
            if missing > 1e-9:
                self.journal.append(
                    "pending_entry.missing_hedge_detected",
                    {
                        "entry_id": entry_id,
                        "missing_hedge_quantity": missing,
                        "maker_leg_filled": pending.maker_leg_filled,
                        "hedge_leg_filled": pending.hedge_leg_filled,
                        "maker_venue": pending.maker_venue().value,
                        "hedge_venue": pending.hedge_venue().value,
                    },
                )
                hedge_driven = await self._drive_missing_hedge_live(pending, entry_id, now_ms)
                if hedge_driven:
                    if pending.missing_hedge_quantity() <= 1e-9 and pending.maker_completed():
                        await self._finalize_pending_entry(pending, entry_id, now_ms)
                        resolved_entry_ids.append(entry_id)
                        continue
                # Keep entry for next reconciliation cycle
                self._apply_reconcile_backoff(pending, now_ms)
            else:
                # No fill progress, no missing hedge — backoff & wait
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
                    self._flush_reconciler_order_diagnostics()
                except Exception as e:
                    self._flush_reconciler_order_diagnostics()
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
                long_zero = pos is None or abs(pos.quantity) <= 1e-9
        except Exception:
            long_zero = False  # can't probe → assume not zero

        try:
            if short_adapter is not None:
                pos = await short_adapter.fetch_position(pending.symbol)
                short_zero = pos is None or abs(pos.quantity) <= 1e-9
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

    def _try_clear_stale_hedge_inflight(self, pending, entry_id: str, result, now_ms: int) -> None:
        """Clear hedge_inflight when order/fills/position all prove no hedge.

        Safety: only clears inflight after ALL three evidence sources
        (order status, fills, position) confirm the hedge order does not
        exist on the exchange. This prevents duplicate hedge exposure.
        """
        hedge_venue = pending.hedge_venue()
        is_long_hedge = pending.maker_leg != "long"
        is_short_hedge = pending.maker_leg != "short"

        hedge_status = result.short_status if is_short_hedge else result.long_status
        hedge_fill_obj = result.short_fill if is_short_hedge else result.long_fill
        hedge_pos_obj = result.short_position if is_short_hedge else result.long_position

        hedge_fill_qty = hedge_fill_obj.quantity if hedge_fill_obj is not None else 0.0
        hedge_pos_qty = abs(hedge_pos_obj.quantity) if hedge_pos_obj is not None else 0.0

        order_absent = hedge_status in ("missing", "canceled", "rejected", "unknown", "not_found")
        fills_zero = hedge_fill_qty <= 1e-9
        position_zero = hedge_pos_qty <= 1e-9

        if order_absent and fills_zero and position_zero:
            old_inflight = pending.hedge_inflight
            pending.hedge_inflight = None
            self.journal.append(
                "pending_entry.hedge_inflight_cleared",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "hedge_venue": hedge_venue.value,
                    "old_hedge_inflight": old_inflight.client_order_id if old_inflight else "",
                    "hedge_status": hedge_status,
                    "hedge_fill_quantity": hedge_fill_qty,
                    "hedge_position_quantity": hedge_pos_qty,
                    "ts_ms": now_ms,
                },
            )

    # ------------------------------------------------------------------
    # V1 parity: hedge deadline, terminalization budget, abort/cleanup
    # ------------------------------------------------------------------

    def _pending_entry_hedge_deadline_decision(
        self, pending, now_ms: int
    ) -> dict:
        """V1: pending_entry_hedge_deadline_decision + adaptive_hedge_deadline_status.

        Returns dict with:
          - hard_breached: bool — elapsed >= hard_deadline_ms
          - soft_breached: bool — elapsed >= soft_deadline_ms
          - hard_deadline_ms: int — effective hard deadline
          - soft_deadline_ms: int — effective soft deadline
          - hedge_elapsed_ms: int — time since hedge submission
        """
        if pending.hedge_inflight is None:
            return {
                "hard_breached": False,
                "soft_breached": False,
                "hard_deadline_ms": 0,
                "soft_deadline_ms": 0,
                "hedge_elapsed_ms": 0,
            }

        hedge_elapsed_ms = pending.hedge_inflight.elapsed_ms(now_ms)
        strategy = self.config.strategy
        base_deadline_ms = getattr(strategy, "maker_hedge_deadline_ms", 800)
        soft_deadline_ms = base_deadline_ms // 2
        hard_deadline_ms = base_deadline_ms

        # V1: legacy inflight (submitted_at_ms=0) has no timestamp — fall back
        # to entry lifetime as a conservative proxy so old production pending
        # entries eventually get a deadline decision instead of blocking
        # hedge drive indefinitely.
        if pending.hedge_inflight.submitted_at_ms <= 0:
            entry_lifetime = pending.compute_lifetime_ms(now_ms)
            if entry_lifetime >= getattr(strategy, "pending_entry_hard_ceiling_ms", 120000):
                hedge_elapsed_ms = entry_lifetime

        # Adaptive: if hedge has execution progress (partial fill) or quote is
        # not fresh, extend deadlines.  V1 uses adaptive_hedge_deadline_status().
        has_progress = pending.hedge_leg_filled > 1e-9

        if has_progress:
            hard_deadline_ms = base_deadline_ms * 2

        hard_breached = hedge_elapsed_ms >= hard_deadline_ms
        soft_breached = hedge_elapsed_ms >= soft_deadline_ms

        return {
            "hard_breached": hard_breached,
            "soft_breached": soft_breached,
            "hard_deadline_ms": hard_deadline_ms,
            "soft_deadline_ms": soft_deadline_ms,
            "hedge_elapsed_ms": hedge_elapsed_ms,
        }

    def _pending_entry_terminalization_budget(
        self, pending, now_ms: int
    ) -> dict | None:
        """V1: pending_entry_terminalization_budget_from_input.

        Returns None if no budget is active, else dict with:
          - hard_ceiling_reached: bool
          - force_terminal_reached: bool
          - final_reason: str
          - lifetime_ms: int
        """
        strategy = self.config.strategy
        hard_ceiling_ms = getattr(strategy, "pending_entry_hard_ceiling_ms", 120000)
        force_terminal_after_ms = getattr(strategy, "pending_entry_force_terminal_after_ms", 60000)

        lifetime_ms = pending.compute_lifetime_ms(now_ms)

        hard_ceiling_reached = lifetime_ms >= hard_ceiling_ms
        force_terminal_reached = (
            lifetime_ms >= force_terminal_after_ms
            and (not pending.has_any_fill() or pending.missing_hedge_quantity() <= 1e-9)
        )

        has_inflight = pending.hedge_inflight is not None
        if has_inflight and not hard_ceiling_reached:
            # V1: inflight hedge blocks terminalization until hard ceiling
            return None

        if not hard_ceiling_reached and not force_terminal_reached:
            return None

        final_reason = (
            "pending_entry_max_lifetime_exhausted"
            if hard_ceiling_reached
            else "pending_entry_zero_fill_lifetime_exhausted"
        )

        return {
            "hard_ceiling_reached": hard_ceiling_reached,
            "force_terminal_reached": force_terminal_reached,
            "final_reason": final_reason,
            "lifetime_ms": lifetime_ms,
        }

    async def _abort_pending_entry_fail_closed(
        self, pending, entry_id: str, reason: str
    ) -> bool:
        """V1: abort_pending_entry_fail_closed — enter fail_closed, then abort.

        entry_sync.rs:2448-2456

        Returns True if pending was removed, False if retained (cleanup failed).
        """
        enter_fail_closed(self.state)
        return await self._abort_pending_entry(pending, entry_id, reason)

    async def _abort_pending_entry(
        self, pending, entry_id: str, reason: str
    ) -> bool:
        """V1: abort_pending_entry — cleanup maker & hedge exposure, then remove.

        entry_sync.rs:4612-4708

        Two-tier exposure cleanup:
        1. cleanup_failed_leg_exposure for both maker and hedge legs
        2. If cleanup fails, compensation_hard_stop for both legs
        3. If hard stop also fails, enter fail_closed and retain pending
        4. On success, remove pending and emit entry.aborted

        Returns True if pending was removed, False if retained (cleanup failed).
        """
        maker_venue = pending.maker_venue()
        hedge_venue = pending.hedge_venue()
        symbol = pending.symbol

        # Tier 1: cleanup/flatten residual exposure on both legs
        maker_cleaned = await self._cleanup_failed_leg_exposure(
            maker_venue, symbol, entry_id, "maker"
        )
        hedge_cleaned = await self._cleanup_failed_leg_exposure(
            hedge_venue, symbol, entry_id, "hedge"
        )

        # V1: None (adapter missing) means uncertain — treat as failure
        if maker_cleaned is not True or hedge_cleaned is not True:
            # Tier 2: compensation hard stop (market order to flatten at any price)
            maker_stopped = await self._cleanup_failed_leg_exposure(
                maker_venue, symbol, entry_id, "maker_hard_stop"
            )
            hedge_stopped = await self._cleanup_failed_leg_exposure(
                hedge_venue, symbol, entry_id, "hedge_hard_stop"
            )

            if maker_stopped is not True or hedge_stopped is not True:
                # Tier 3: cleanup failed → fail_closed, retain pending
                enter_fail_closed(self.state)
                self.state.last_error = reason
                self.journal.append(
                    "entry.abort_failed_pending_retained",
                    {
                        "entry_id": entry_id,
                        "symbol": symbol,
                        "reason": reason,
                        "maker_cleaned": maker_cleaned,
                        "hedge_cleaned": hedge_cleaned,
                        "maker_hard_stop": maker_stopped,
                        "hedge_hard_stop": hedge_stopped,
                    },
                )
                return False

        # Success: remove pending entry
        self.state.pending_entries.pop(entry_id, None)
        self.state.last_error = reason
        self.journal.append(
            "entry.aborted",
            {
                "entry_id": entry_id,
                "symbol": symbol,
                "reason": reason,
                "maker_quantity": pending.maker_leg_filled,
                "hedge_quantity": pending.hedge_leg_filled,
            },
        )
        return True

    async def _cleanup_failed_leg_exposure(
        self, venue, symbol: str, entry_id: str, stage: str
    ) -> bool | None:
        """V1: cleanup_failed_leg_exposure — flatten residual position on one venue.

        entry.rs:4101-4144

        Returns:
          True: position was flattened (or was already zero)
          False: cleanup failed (position remains or can't verify)
          None: no adapter available (caller treats as uncertain — not success)
        """
        adapter = self.get_venue_adapter(venue)
        if adapter is None:
            return None

        try:
            pos = await adapter.fetch_position(symbol)
        except Exception:
            return False  # can't verify — assume position exists

        if pos is None or abs(pos.quantity) <= 1e-9:
            return True  # Already flat

        # V1: direction is based on position.side, NOT signed quantity.
        # V2 PositionSnapshot.quantity is always abs(size); side carries direction.
        # side=BUY (long) → cleanup SELL; side=SELL (short) → cleanup BUY
        cleanup_side = pos.side.opposite()
        from lightfee.venues.cid import generate_exchange_cid
        cleanup_client_order_id = generate_exchange_cid(
            f"{entry_id}:{stage}:{symbol}", "c", venue
        )

        self.journal.append(
            "entry.cleanup_leg_exposure",
            {
                "entry_id": entry_id,
                "stage": stage,
                "venue": venue.value,
                "symbol": symbol,
                "size": pos.quantity,
                "side": pos.side.value,
                "cleanup_side": cleanup_side.value,
                "cleanup_client_order_id": cleanup_client_order_id,
            },
        )

        try:
            from lightfee.core.domain import OrderRequest

            req = OrderRequest(
                venue=venue,
                symbol=symbol,
                side=cleanup_side,
                quantity=abs(pos.quantity),
                price=None,
                post_only=False,
                reduce_only=True,  # V1: cleanup always reduce-only
                client_order_id=cleanup_client_order_id,
            )
            fill = await adapter.place_order(req)
            self._flush_adapter_order_diagnostics(adapter)

            # V1: cleanup success needs EITHER fill covering target qty
            # OR verified-flat position after partial fill.
            target_qty = abs(pos.quantity)
            if fill.quantity >= target_qty - 1e-9:
                return True

            # Partial fill — re-fetch position to verify true flatness
            try:
                verify_pos = await adapter.fetch_position(symbol)
                if verify_pos is None or abs(verify_pos.quantity) <= 1e-9:
                    return True  # Position flat despite partial fill
            except Exception:
                pass

            return False  # Position not flat after cleanup
        except Exception:
            self._flush_adapter_order_diagnostics(adapter)
            try:
                verify_pos = await adapter.fetch_position(symbol)
                if verify_pos is None or abs(verify_pos.quantity) <= 1e-9:
                    return True
            except Exception:
                pass
            return False

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
                hedge_lookup_cid = pending.hedge_inflight.client_order_id if pending.hedge_inflight else pending.hedge_client_order_id
                result = await self.reconciler.reconcile_position(
                    position_id=entry_id,
                    symbol=pending.symbol,
                    long_venue=pending.long_venue,
                    short_venue=pending.short_venue,
                    long_order_id=pending.maker_order_id,
                    short_order_id=pending.hedge_order_id,
                    long_client_order_id=pending.maker_client_order_id,
                    short_client_order_id=hedge_lookup_cid,
                )
                self._flush_reconciler_order_diagnostics()
            except Exception as e:
                self._flush_reconciler_order_diagnostics()
                self.journal.append(
                    "recovery.force_reconcile_entry_error",
                    {"entry_id": entry_id, "error": str(e)},
                )
                continue

            if result.long_status == "filled" and result.short_status == "filled":
                pending.maker_leg_filled = result.long_fill.quantity if result.long_fill else pending.maker_leg_filled
                pending.hedge_leg_filled = result.short_fill.quantity if result.short_fill else pending.hedge_leg_filled
                if result.long_fill and _recon_fill_price(result.long_fill) > 0:
                    pending.maker_fill_price = _recon_fill_price(result.long_fill)
                if result.short_fill and _recon_fill_price(result.short_fill) > 0:
                    pending.hedge_fill_price = _recon_fill_price(result.short_fill)
                await self._finalize_pending_entry(pending, entry_id, now_ms)
                resolved_ids.append(entry_id)
            elif result.is_flat:
                resolved_ids.append(entry_id)

        for eid in resolved_ids:
            self.state.pending_entries.pop(eid, None)

        self.journal.append(
            "recovery.force_reconcile_complete",
            {"resolved_entries": len(resolved_ids), "ts_ms": now_ms},
        )

    def _flush_reconciler_order_diagnostics(self) -> None:
        if self.reconciler is None:
            return
        drain = getattr(self.reconciler, "drain_order_diagnostics", None)
        if not callable(drain):
            return
        for event in drain():
            kind = event.get("kind", "")
            payload = event.get("payload", {})
            if isinstance(kind, str) and isinstance(payload, dict):
                self.journal.append(kind, payload)

    def _flush_adapter_order_diagnostics(self, adapter) -> None:
        """Drain order diagnostics from a venue adapter's transport into the journal."""
        transport = getattr(adapter, "_transport", adapter)
        drain = getattr(transport, "drain_order_diagnostics", None)
        if not callable(drain):
            return
        for event in drain():
            kind = event.get("kind", "")
            payload = event.get("payload", {})
            if isinstance(kind, str) and isinstance(payload, dict):
                self.journal.append(kind, payload)

    async def _recover_pending_entry_hedges(self, now_ms: int) -> None:
        """Re-drive pending entry hedges with full V1 startup recovery semantics.

        V1: process_pending_entry_hedges() + drive_pending_entry_hedge() +
            force_terminalize_pending_entry_if_budget_exhausted() +
            hydrate_pending_entry_from_live_balanced_exposure()

        For each pending entry that is startup_recovery_ready:
        1. Query venue order status to resolve uncertain outcomes
        2. Try to hydrate from live balanced exposure (reconcile from exchange positions)
        3. Compute terminalization budget (lifetime vs hard_ceiling/force_terminal)
        4. If budget exhausted → abort or finalize per V1 rules
        5. If maker filled but hedge missing → attempt to drive hedge
        """
        if not self._venue_adapters:
            return

        strategy = self.config.strategy
        hard_ceiling_ms = strategy.pending_entry_hard_ceiling_ms
        force_terminal_after_ms = strategy.pending_entry_force_terminal_after_ms

        for entry_id, pending in list(self.state.pending_entries.items()):
            # V1: startup_recovery_ready gate — skip entries that don't need recovery yet
            if not pending.startup_recovery_ready():
                continue

            lifetime_ms = pending.compute_lifetime_ms(now_ms)

            # --- Step 1: Query venue for order status (resolve uncertain outcomes) ---
            if pending.uncertain_outcome:
                await self._recover_poll_order_status(entry_id, pending)

            # Re-check after order status poll: if no longer startup_recovery_ready, skip
            if not pending.startup_recovery_ready():
                continue

            # --- Step 2: Try hydrate from live balanced exposure ---
            # V1: hydrate_pending_entry_from_live_balanced_exposure —
            # fetches live positions from both venues; if there's already balanced
            # exposure, applies fills and may finalize.
            hydrated = await self._recover_hydrate_from_live_positions(pending)
            if hydrated:
                self.journal.append(
                    "recovery.pending_entry_live_balance_hydrated",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "maker_filled": pending.maker_leg_filled,
                        "hedge_filled": pending.hedge_leg_filled,
                    },
                )

            # Re-check after hydration: if no longer needs recovery, skip
            if not pending.startup_recovery_ready():
                continue

            # --- Step 3: Terminalization budget (shared helper) ---
            budget = self._pending_entry_terminalization_budget(pending, now_ms)
            if budget is None:
                # Below terminalization thresholds — re-poll, don't force
                pending.reconcile_attempt += 1
                self._apply_reconcile_backoff(pending, now_ms)
                continue

            hard_ceiling_reached = budget["hard_ceiling_reached"]
            final_reason = budget["final_reason"]

            # --- Step 4: Handle terminalization ---
            # V1: force_terminalize_pending_entry_if_budget_exhausted
            # Two main paths: maker not completed (cancel first) vs maker completed

            # --- 4a: Maker not completed → cancel maker order first (V1 cancel-before-abort) ---
            if not pending.maker_completed() and pending.maker_order_id:
                cancel_issued = await self._recover_cancel_maker_order(
                    pending, entry_id, final_reason
                )
                if hard_ceiling_reached and not cancel_issued:
                    # V1: hard ceiling + cancel failed → abort (with cleanup)
                    await self._abort_pending_entry(pending, entry_id, final_reason)
                    continue
                if cancel_issued:
                    # V1: cancel was issued
                    if hard_ceiling_reached:
                        if pending.has_any_fill() and pending.missing_hedge_quantity() <= 1e-9:
                            # Balanced fill → finalize even on hard ceiling
                            await self._finalize_pending_entry(pending, entry_id, now_ms)
                            self.state.pending_entries.pop(entry_id, None)
                            self.journal.append(
                                "recovery.pending_entry_finalized",
                                {"entry_id": entry_id, "symbol": pending.symbol,
                                 "reason": "cancel_completed_entry_balanced"},
                            )
                        else:
                            # Has fills with missing hedge or no fills → abort (with cleanup)
                            await self._abort_pending_entry(pending, entry_id, final_reason)
                        continue
                    # Cancel issued but below hard ceiling → keep for progress poll
                    pending.reconcile_attempt += 1
                    self._apply_reconcile_backoff(pending, now_ms)
                    continue
                # Cancel not issued (e.g. budget delayed) and below hard ceiling → keep
                if not hard_ceiling_reached:
                    pending.reconcile_attempt += 1
                    self._apply_reconcile_backoff(pending, now_ms)
                    continue

            # --- 4b: Zero fills → abort or try taker fallback ---
            if not pending.has_any_fill():
                # V1: try taker fallback when tradeable (config gated)
                if getattr(strategy, "pending_entry_force_fallback_when_tradeable", False):
                    fallback_ok = await self._recover_try_taker_fallback(
                        pending, entry_id, final_reason
                    )
                    if fallback_ok:
                        continue
                # Zero fills — live-size probe first to verify no exchange residual.
                # V1: zero local fills doesn't guarantee zero exchange exposure.
                # Must attempt cleanup/flatten before removing pending.
                abandoned = await self._try_abandon_stale_entry(pending, entry_id)
                if abandoned:
                    self.state.pending_entries.pop(entry_id, None)
                    continue  # Both venues flat → safe to clear
                removed = await self._abort_pending_entry(pending, entry_id, final_reason)
                if removed:
                    continue  # Cleanup succeeded → pending removed
                # Cleanup failed → fail_closed, pending retained
                continue

            # --- 4c: Has fills + missing hedge → try to drive hedge ---
            if pending.missing_hedge_quantity() > 1e-9:
                # V1: check if tradeable before hedging (config gated)
                if not getattr(strategy, "pending_entry_force_fallback_when_tradeable", False):
                    # When fallback_when_tradeable is false (default), skip tradeability
                    # check and go straight to abort on hard ceiling
                    if hard_ceiling_reached:
                        await self._abort_pending_entry(pending, entry_id, final_reason)
                        continue

                hedge_driven = await self._recover_drive_missing_hedge(
                    pending, final_reason
                )
                if hedge_driven:
                    # V1: if hedge completes the entry → finalize immediately
                    if pending.missing_hedge_quantity() <= 1e-9 and pending.maker_completed():
                        await self._finalize_pending_entry(pending, entry_id, now_ms)
                        self.state.pending_entries.pop(entry_id, None)
                        self.journal.append(
                            "recovery.pending_entry_finalized",
                            {
                                "entry_id": entry_id,
                                "symbol": pending.symbol,
                                "reason": "recovery_hedge_completed_entry",
                            },
                        )
                        continue

                    # Hedge submitted but entry not yet complete — keep for reconciliation
                    self.journal.append(
                        "recovery.pending_entry_hedge_driven",
                        {
                            "entry_id": entry_id,
                            "symbol": pending.symbol,
                            "reason": final_reason,
                            "missing_hedge": pending.missing_hedge_quantity(),
                        },
                    )
                    pending.reconcile_attempt += 1
                    self._apply_reconcile_backoff(pending, now_ms)
                    continue

                if hard_ceiling_reached:
                    # Hard ceiling with unresolved hedge → abort (with cleanup)
                    await self._abort_pending_entry(pending, entry_id, final_reason)
                    continue

            # --- 4d: Fully filled → finalize ---
            if pending.missing_hedge_quantity() <= 1e-9 and pending.maker_completed():
                await self._finalize_pending_entry(pending, entry_id, now_ms)
                self.state.pending_entries.pop(entry_id, None)
                self.journal.append(
                    "recovery.pending_entry_finalized",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "reason": final_reason,
                    },
                )
                continue

            # --- 4e: Fallback — still pending and hard ceiling reached → abort (with cleanup) ---
            if hard_ceiling_reached:
                await self._abort_pending_entry(pending, entry_id, final_reason)
                continue

            pending.reconcile_attempt += 1
            self._apply_reconcile_backoff(pending, now_ms)

        # --- Post-recovery lifecycle transition ---
        self._finalize_startup_recovery()

    async def _recover_poll_order_status(self, entry_id: str, pending) -> None:
        """Query each venue for its respective order status.

        V1: queries maker venue with maker_order_id and hedge venue with
        hedge_order_id independently, rather than using a fallback chain
        that shadows the hedge order when a maker order exists.
        """
        # Query maker venue with maker order
        if pending.maker_order_id:
            maker_ven = pending.maker_venue()
            maker_adapter = self.get_venue_adapter(maker_ven)
            if maker_adapter is not None and hasattr(maker_adapter, "get_order_status"):
                try:
                    status = await maker_adapter.get_order_status(
                        symbol=pending.symbol,
                        order_id=pending.maker_order_id,
                    )
                    if status and getattr(status, "status", "") == "filled":
                        pending.uncertain_outcome = False
                        pending.outcome = "filled"
                        filled_qty = getattr(status, "filled_quantity", 0.0) or getattr(status, "executed_qty", 0.0)
                        if filled_qty and filled_qty > 0:
                            pending.maker_leg_filled = max(pending.maker_leg_filled, float(filled_qty))
                        self.journal.append(
                            "recovery.maker_order_status_resolved",
                            {"entry_id": entry_id, "venue": str(maker_ven), "status": status.status},
                        )
                        return
                    elif status and getattr(status, "status", "") == "canceled":
                        pending.uncertain_outcome = False
                        pending.outcome = "canceled"
                        self.journal.append(
                            "recovery.maker_order_canceled",
                            {"entry_id": entry_id, "venue": str(maker_ven)},
                        )
                        return
                except Exception:
                    pass

        # Query hedge venue with hedge order (independent of maker query)
        if pending.hedge_order_id:
            hedge_ven = pending.hedge_venue()
            hedge_adapter = self.get_venue_adapter(hedge_ven)
            if hedge_adapter is not None and hasattr(hedge_adapter, "get_order_status"):
                try:
                    status = await hedge_adapter.get_order_status(
                        symbol=pending.symbol,
                        order_id=pending.hedge_order_id,
                    )
                    if status and getattr(status, "status", "") == "filled":
                        pending.uncertain_outcome = False
                        pending.outcome = "filled"
                        filled_qty = getattr(status, "filled_quantity", 0.0) or getattr(status, "executed_qty", 0.0)
                        if filled_qty and filled_qty > 0:
                            pending.hedge_leg_filled = max(pending.hedge_leg_filled, float(filled_qty))
                        self.journal.append(
                            "recovery.hedge_order_status_resolved",
                            {"entry_id": entry_id, "venue": str(hedge_ven), "status": status.status},
                        )
                        return
                    elif status and getattr(status, "status", "") == "canceled":
                        self.journal.append(
                            "recovery.hedge_order_canceled",
                            {"entry_id": entry_id, "venue": str(hedge_ven)},
                        )
                except Exception:
                    pass

    async def _recover_hydrate_from_live_positions(self, pending) -> bool:
        """Try to hydrate pending entry from live exchange positions.

        V1: hydrate_pending_entry_from_live_balanced_exposure() —
        If both venues have position size > 0 (long) and < 0 (short),
        and the balanced quantity exceeds current fill, apply fills.

        V1 skips hydration when inflight_hedge is active (the hedge may
        still fill). We approximate this by checking for an active hedge
        order id with uncertain outcome.
        """
        # V1: skip hydration while hedge is inflight
        if pending.uncertain_outcome and pending.hedge_order_id:
            return False

        try:
            long_adapter = self.get_venue_adapter(pending.long_venue)
            short_adapter = self.get_venue_adapter(pending.short_venue)
            if long_adapter is None or short_adapter is None:
                return False

            long_pos = await long_adapter.fetch_position(pending.symbol)
            short_pos = await short_adapter.fetch_position(pending.symbol)

            # V1: need long position (BUY side, qty > 0) and short (SELL side, qty > 0)
            # V2 transport returns side=SELL with quantity=abs(net) for shorts,
            # so checking quantity >= 0 is wrong — must check side field.
            from lightfee.core.domain import Side
            long_has_position = (
                long_pos.side == Side.BUY and long_pos.quantity > 1e-9
            )
            short_has_position = (
                short_pos.side == Side.SELL and short_pos.quantity > 1e-9
            )
            if not long_has_position or not short_has_position:
                return False

            live_balanced = min(long_pos.quantity, short_pos.quantity)
            current_balanced = min(pending.maker_leg_filled, pending.hedge_leg_filled)
            if live_balanced <= current_balanced + 1e-9:
                return False

            # Apply recovered fills (quantities already positive per side check above)
            long_delta = min(live_balanced, long_pos.quantity) - pending.maker_leg_filled
            short_delta = min(live_balanced, short_pos.quantity) - pending.hedge_leg_filled
            if long_delta > 1e-9:
                pending.maker_leg_filled += long_delta
            if short_delta > 1e-9:
                pending.hedge_leg_filled += short_delta

            # If both legs now filled, mark as resolved
            if pending.maker_completed() and pending.missing_hedge_quantity() <= 1e-9:
                pending.uncertain_outcome = False
                pending.outcome = "filled"

            return True
        except Exception:
            return False

    def _try_consume_maker_venue_budget(
        self, venue, now_ms: int
    ) -> bool:
        """Check and consume maker venue request budget for a cancel/submit op.

        V1: try_consume_maker_venue_request_budget (entry_sync.rs:2410-2431)
        Uses sliding-window budget: max_ops per window_ms, submit costs 2.
        Returns True if the operation is allowed (budget consumed).

        During recovery, operations are rare (one cancel per stuck entry),
        but the budget prevents accidental tight-loop retries.
        """
        strategy = self.config.strategy
        window_ms = strategy.maker_venue_budget_window_ms
        max_ops = strategy.maker_venue_budget_max_ops
        cost = strategy.maker_venue_submit_cost  # cancel uses submit cost

        venue_key = str(venue) if hasattr(venue, "value") else str(venue)
        history = self._maker_venue_op_history.setdefault(venue_key, [])

        # Prune expired timestamps
        cutoff = now_ms - window_ms
        history[:] = [ts for ts in history if ts > cutoff]

        # V1: check if budget remaining allows this operation
        current_ops = sum(1 for _ in history)
        if current_ops + cost > max_ops:
            return False

        # Consume budget: record this operation
        history.append(now_ms)
        return True

    async def _recover_cancel_maker_order(
        self, pending, entry_id: str, reason: str
    ) -> bool:
        """Attempt to cancel the maker order before abort.

        V1: cancel_pending_entry_passive_order (entry_sync.rs:2401-2445) —
        1. Returns false if maker already completed or cancel already requested
        2. Checks make_venue_request_budget (rate-limit gate)
        3. If budget exhausted → sets backoff, returns false
        4. Issues cancel_order on the maker venue adapter
        5. Returns true if cancel was successfully issued
        """
        if pending.maker_completed():
            return False

        maker_venue = pending.maker_venue()
        adapter = self.get_venue_adapter(maker_venue)
        if adapter is None:
            return False

        if not pending.maker_order_id:
            return False

        if getattr(pending, "_cancel_requested", False):
            return False

        # V1: check maker venue request budget before issuing cancel
        now_ms = wall_clock_now_ms()
        if not self._try_consume_maker_venue_budget(maker_venue, now_ms):
            # Budget exhausted — delay and retry later
            pending.next_progress_poll_ms = (
                now_ms + self.config.strategy.maker_venue_budget_window_ms
            )
            self.journal.append(
                "recovery.maker_cancel_budget_delayed",
                {"entry_id": entry_id, "venue": str(maker_venue),
                 "reason": reason, "next_poll_ms": pending.next_progress_poll_ms},
            )
            return False

        try:
            from lightfee.core.domain import OrderRequest
            cancel_req = OrderRequest(
                venue=maker_venue,
                symbol=pending.symbol,
                side=pending.maker_side(),
                quantity=pending.target_quantity,
                price=0.0,
                order_id=pending.maker_order_id,
                client_order_id=pending.maker_client_order_id or "",
            )
            await adapter.cancel_order(cancel_req)
            pending._cancel_requested = True
            pending.reconcile_next_attempt_ms = (
                now_ms + self._RECONCILE_RETRY_BASE_MS
            )
            self.journal.append(
                "recovery.maker_cancel_requested",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "maker_venue": str(maker_venue),
                    "maker_order_id": pending.maker_order_id,
                    "reason": reason,
                },
            )
            return True
        except Exception as e:
            if not pending.has_any_fill():
                self.journal.append(
                    "recovery.maker_cancel_failed_assumed_terminal",
                    {"entry_id": entry_id, "symbol": pending.symbol,
                     "error": str(e), "action": "abort_without_fail_closed"},
                )
                return False
            from lightfee.engine.lifecycle import enter_fail_closed
            enter_fail_closed(self.state)
            self.state.last_error = (
                f"pending_entry_lifetime_cancel_failed:{entry_id}: {e}"
            )
            self.journal.append(
                "recovery.maker_cancel_failed_fail_closed",
                {"entry_id": entry_id, "symbol": pending.symbol,
                 "error": str(e), "reason": reason},
            )
            return False

    async def _recover_try_taker_fallback(
        self, pending, entry_id: str, reason: str
    ) -> bool:
        """V1: try_terminal_taker_fallback() — taker order for zero-fill entries.

        Requires MarketView for tradeability check. Not available during
        recovery without a snapshot. Returns False → caller proceeds to abort.
        """
        return False

    async def _recover_drive_missing_hedge(self, pending, reason: str) -> bool:
        """Submit a hedge order for the missing quantity.

        V1: hedge_pending_entry_delta() —
        1. Normalize quantity via adapter.normalize_quantity (exchange lot size)
        2. Use maker fill price as hedge price hint (better than pure market)
        3. Submit order; gate only on FAIL_CLOSED (recovery lifecycle is RECONCILING)
        """
        hedge_venue = pending.hedge_venue()
        adapter = self.get_venue_adapter(hedge_venue)
        if adapter is None:
            return False

        missing = pending.missing_hedge_quantity()
        if missing <= 1e-9:
            return False

        if self.state.risk_mode == GlobalRiskMode.FAIL_CLOSED:
            self._try_journal(
                "recovery.hedge_blocked_fail_closed",
                {"entry_id": pending.pending_id, "reason": reason},
            )
            return False

        try:
            from lightfee.core.domain import OrderRequest

            # V1: normalize to exchange lot size
            normalized = missing
            if hasattr(adapter, "normalize_quantity"):
                normalized = await adapter.normalize_quantity(pending.symbol, missing)

            if normalized <= 1e-9:
                self.journal.append(
                    "recovery.hedge_quantity_below_min_notional",
                    {"entry_id": pending.pending_id, "symbol": pending.symbol,
                     "raw_quantity": missing, "normalized_quantity": normalized},
                )
                return False

            # V1: use maker fill price as hedge price hint (live fill preferred)
            hedge_price = pending.maker_fill_price if pending.maker_fill_price > 0 else pending.maker_price

            from lightfee.venues.cid import generate_exchange_cid
            recovery_cid = generate_exchange_cid(pending.pending_id, "h", hedge_venue)
            pending.hedge_client_order_id = pending.hedge_client_order_id or recovery_cid

            req = OrderRequest(
                venue=hedge_venue,
                symbol=pending.symbol,
                side=pending.hedge_side(),
                quantity=normalized,
                price=hedge_price,
                post_only=False,
                reduce_only=False,
                client_order_id=recovery_cid,
            )
            fill = await adapter.place_order(req)
            if fill.quantity > 0:
                pending.hedge_leg_filled += fill.quantity
                pending.hedge_order_id = fill.order_id
                pending.hedge_fill_price = fill.price
                if pending.missing_hedge_quantity() <= 1e-9:
                    pending.uncertain_outcome = False
                    pending.outcome = "filled"
                return True
            return False
        except Exception as e:
            self.journal.append(
                "recovery.hedge_submit_error",
                {"entry_id": pending.pending_id, "symbol": pending.symbol,
                 "error": str(e), "reason": reason},
            )
            return False

    async def _drive_missing_hedge_live(self, pending, entry_id: str, now_ms: int) -> bool:
        """Submit a hedge IOC/taker order for the missing quantity during normal tick.

        V1: hedge_pending_entry_delta() — called from the normal live tick after
        reconciliation detects a maker fill but the hedge leg is still missing.

        Idempotency: sets pending.hedge_inflight to the client_order_id before
        submitting; skips if already inflight.  On success updates
        hedge_leg_filled and clears inflight.

        Unlike _recover_drive_missing_hedge(), this does NOT gate on FAIL_CLOSED
        — normal ticks always attempt to complete the entry.
        """
        hedge_venue = pending.hedge_venue()
        adapter = self.get_venue_adapter(hedge_venue)
        if adapter is None:
            return False

        missing = pending.missing_hedge_quantity()
        if missing <= 1e-9:
            return False

        # Idempotency: skip if a hedge is already inflight
        if pending.hedge_inflight is not None:
            # Do not retry while inflight; reconciliation will clear it
            # after order/fills/position prove no hedge exists.
            return False

        # Terminal: do not drive hedge from a residual repair state
        if pending.repair_state:
            return False

        try:
            from lightfee.core.domain import OrderRequest

            normalized = missing
            if hasattr(adapter, "normalize_quantity"):
                normalized = await adapter.normalize_quantity(pending.symbol, missing)

            if normalized <= 1e-9:
                self.journal.append(
                    "pending_entry.hedge_quantity_below_min_notional",
                    {"entry_id": entry_id, "symbol": pending.symbol,
                     "raw_quantity": missing, "normalized_quantity": normalized},
                )
                return False

            hedge_price = pending.maker_fill_price if pending.maker_fill_price > 0 else pending.maker_price

            # Terminal policy: compute notional and check against venue min_notional
            hedge_notional = abs(normalized * hedge_price)
            min_notional = self._venue_min_notional(hedge_venue, pending.symbol)
            if min_notional > 0 and hedge_notional < min_notional:
                pending.repair_state = "hedge_residual_below_min_notional"
                self.journal.append(
                    "pending_entry.hedge_residual_below_min_notional",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "hedge_venue": hedge_venue.value,
                        "hedge_notional": hedge_notional,
                        "hedge_min_notional": min_notional,
                        "missing_quantity": missing,
                        "normalized_quantity": normalized,
                        "hedge_price": hedge_price,
                    },
                )
                return False

            from lightfee.venues.cid import generate_exchange_cid
            hedge_cloid = generate_exchange_cid(entry_id, "h", hedge_venue)
            pending.hedge_client_order_id = hedge_cloid
            pending.hedge_inflight = HedgeInflight(
                client_order_id=hedge_cloid,
                venue=hedge_venue,
                side=pending.hedge_side(),
                quantity=normalized,
                attempt=0,
                submitted_at_ms=now_ms,
            )

            self.journal.append(
                "pending_entry.hedge_submit_attempt",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "hedge_venue": hedge_venue.value,
                    "hedge_side": pending.hedge_side().value,
                    "hedge_quantity": normalized,
                    "hedge_price_hint": hedge_price,
                    "hedge_client_order_id": hedge_cloid,
                    "maker_leg_filled": pending.maker_leg_filled,
                    "hedge_leg_filled": pending.hedge_leg_filled,
                },
            )

            req = OrderRequest(
                venue=hedge_venue,
                symbol=pending.symbol,
                side=pending.hedge_side(),
                quantity=normalized,
                price=hedge_price,
                post_only=False,
                reduce_only=False,
                client_order_id=hedge_cloid,
            )
            fill = await adapter.place_order(req)

            self._flush_adapter_order_diagnostics(adapter)

            if fill.quantity > 0:
                pending.hedge_leg_filled += fill.quantity
                pending.hedge_order_id = fill.order_id
                pending.hedge_fill_price = fill.price
                pending.hedge_inflight = None

                self.journal.append(
                    "pending_entry.hedge_submit_result",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "outcome": "filled",
                        "hedge_fill_quantity": fill.quantity,
                        "hedge_fill_price": fill.price,
                        "hedge_order_id": fill.order_id,
                        "hedge_client_order_id": hedge_cloid,
                        "hedge_leg_filled": pending.hedge_leg_filled,
                        "missing_hedge_remaining": pending.missing_hedge_quantity(),
                    },
                )

                if pending.missing_hedge_quantity() <= 1e-9:
                    pending.uncertain_outcome = False
                    pending.outcome = "filled"
                return True

            # Zero fill — hedge order was placed but didn't fill (IOC/taker)
            pending.hedge_inflight = None
            self.journal.append(
                "pending_entry.hedge_submit_result",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "outcome": "zero_fill",
                    "hedge_client_order_id": hedge_cloid,
                    "order_id": getattr(fill, "order_id", ""),
                },
            )
            return False

        except OrderSubmitError as e:
            # V1: retain inflight on UNCERTAIN so reconciliation can query it;
            # only clear on REJECTED where we know the order never reached the exchange.
            submitted_inflight = pending.hedge_inflight
            if e.is_rejected:
                pending.hedge_inflight = None
            self._flush_adapter_order_diagnostics(adapter)
            self.journal.append(
                "pending_entry.hedge_submit_result",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "outcome": "error",
                    "error": str(e),
                    "is_rejected": e.is_rejected,
                    "hedge_client_order_id": submitted_inflight.client_order_id if submitted_inflight else "",
                },
            )
            return False
        except Exception as e:
            pending.hedge_inflight = None
            self._flush_adapter_order_diagnostics(adapter)
            self.journal.append(
                "pending_entry.hedge_submit_result",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "outcome": "error",
                    "error": str(e),
                },
            )
            return False

    async def _finalize_pending_entry(self, pending, entry_id: str, now_ms: int) -> None:
        """Finalize a completed pending entry: build OpenPosition, write entry.opened.

        V1: When both maker and hedge legs are filled, the pending entry is
        converted into an OpenPosition and recorded durably.
        """
        from lightfee.engine.entry import build_open_position, EntryContext, EntryType

        maker_is_long = pending.maker_leg == "long"
        maker_side = Side.BUY if maker_is_long else Side.SELL

        maker_fill = OrderFill(
            venue=pending.maker_venue(),
            symbol=pending.symbol,
            side=maker_side,
            quantity=pending.maker_leg_filled,
            price=pending.maker_fill_price if pending.maker_fill_price > 0 else pending.maker_price,
            order_id=pending.maker_order_id,
            filled_at_ms=now_ms,
        )
        hedge_fill = OrderFill(
            venue=pending.hedge_venue(),
            symbol=pending.symbol,
            side=pending.hedge_side(),
            quantity=pending.hedge_leg_filled,
            price=pending.hedge_fill_price if pending.hedge_fill_price > 0 else pending.maker_fill_price,
            order_id=pending.hedge_order_id,
            filled_at_ms=now_ms,
        )

        ctx = EntryContext(
            entry_id=entry_id,
            symbol=pending.symbol,
            long_venue=pending.long_venue,
            short_venue=pending.short_venue,
            long_quantity=pending.target_quantity,
            short_quantity=pending.target_quantity,
            long_price_hint=0.0,
            short_price_hint=0.0,
            maker_leg=maker_side,
            entry_type=EntryType(pending.entry_type) if pending.entry_type else EntryType.STANDARD_DUAL_TAKER,
            created_at_ms=pending.created_at_ms,
        )

        position = build_open_position(ctx, maker_fill, hedge_fill, now_ms)

        self.state.open_positions[position.position_id] = position

        self.journal.append_critical(
            now_ms, "entry.opened",
            {
                "position_id": position.position_id,
                "internal_entry_id": position.position_id,
                "symbol": position.symbol,
                "long_venue": position.long_venue.value,
                "short_venue": position.short_venue.value,
                "quantity": position.matched_quantity,
                "long_quantity": position.long_quantity,
                "short_quantity": position.short_quantity,
                "long_entry_price": position.long_entry_price,
                "short_entry_price": position.short_entry_price,
                "opened_at_ms": position.opened_at_ms,
                "matched_quantity": position.matched_quantity,
                "maker_order_id": maker_fill.order_id,
                "hedge_order_id": hedge_fill.order_id,
                "maker_client_order_id": pending.maker_client_order_id,
                "hedge_client_order_id": pending.hedge_client_order_id,
            },
        )

        self.journal.append(
            "pending_entry.pending_entry_finalized",
            {
                "entry_id": entry_id,
                "position_id": position.position_id,
                "maker_leg_filled": pending.maker_leg_filled,
                "hedge_leg_filled": pending.hedge_leg_filled,
                "maker_fill_price": pending.maker_fill_price,
                "hedge_fill_price": pending.hedge_fill_price,
            },
        )

        self.journal.append(
            "runtime.position_opened",
            {
                "position_id": position.position_id,
                "symbol": position.symbol,
            },
        )

    def _finalize_startup_recovery(self) -> None:
        """Transition lifecycle after startup recovery per V1 semantics.

        V1: finalize_startup_position_recovery() lifecycle transitions:
        - No open positions, no pending entries, no pending work → RUNNING
        - Has pending entries but no open positions → RISK_ONLY with blocked reason
        - Has open positions → RUNNING (normal, positions are managed)
        """
        from lightfee.engine.lifecycle import enter_fail_closed

        has_opens = len(self.state.open_positions) > 0
        has_pending = len(self.state.pending_entries) > 0
        has_pending_closes = len(self.state.pending_closes) > 0
        has_passive_closes = len(self.state.pending_passive_closes) > 0

        if not has_opens and not has_pending and not has_pending_closes and not has_passive_closes:
            # All clear — transition to RUNNING
            from lightfee.engine.lifecycle import clear_risk_mode_for_recovery
            clear_risk_mode_for_recovery(self.state)
            self.state.last_error = None
            self._try_journal("runtime.running",
                {"reason": "startup_recovery_completed", "ts_ms": wall_clock_now_ms()})
            return

        if has_opens:
            # Has open positions — normal operation
            max_positions = self.config.strategy.max_concurrent_positions
            if len(self.state.open_positions) > max_positions:
                enter_fail_closed(self.state)
                self.state.last_error = "open_positions_exceed_configured_max"
                self._try_journal("recovery.blocked", {
                    "reason": "open_positions_exceed_configured_max",
                    "open_positions": len(self.state.open_positions),
                    "max": max_positions,
                })
            else:
                from lightfee.engine.lifecycle import clear_risk_mode_for_recovery
                clear_risk_mode_for_recovery(self.state)
                self.state.last_error = None
                self._try_journal("runtime.running", {
                    "reason": "startup_recovery_completed_with_positions",
                    "open_positions": len(self.state.open_positions),
                    "ts_ms": wall_clock_now_ms(),
                })
            return

        # No open positions but has pending work → RISK_ONLY
        if has_pending or has_pending_closes or has_passive_closes:
            blocked_reason = (
                "startup_recovery_pending_work_without_open_positions"
            )
            self.state.recovery_blocked_reason = blocked_reason
            self.state.recovery_blocked_at_ms = wall_clock_now_ms()
            self.state.last_error = (
                f"startup recovery blocked: pending_entries={len(self.state.pending_entries)}, "
                f"pending_closes={len(self.state.pending_closes)}, "
                f"pending_passive_closes={len(self.state.pending_passive_closes)}"
            )
            set_lifecycle(self.state, EngineLifecycle.RISK_ONLY)
            self._try_journal("recovery.blocked", {
                "reason": blocked_reason,
                "pending_entries": list(self.state.pending_entries.keys()),
                "ts_ms": wall_clock_now_ms(),
            })

    def _try_journal(self, kind: str, payload: dict) -> None:
        """Append to journal if open; temporarily open if not (for recovery diagnostics)."""
        try:
            self.journal.append(kind, payload)
        except RuntimeError:
            # Journal not open — temporarily open for this event
            try:
                self.journal.open()
                self.journal.append(kind, payload)
            except Exception:
                pass
            finally:
                try:
                    self.journal.close()
                except Exception:
                    pass

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

        # Detect false-clean state where exchanges hold positions but V2 missed them.
        await self._maybe_recover_clean_live_positions(now_ms)

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

    def _entry_local_l2_stale_after_ms(self) -> int:
        return self._configured_entry_l2_stale_after_ms(self.config)

    @staticmethod
    def _configured_entry_l2_stale_after_ms(config) -> int:
        for field_name in (
            "entry_local_l2_book_stale_after_ms",
            "local_l2_quiet_book_grace_ms",
            "local_l2_max_age_ms",
        ):
            value = int(getattr(config.strategy, field_name, 0) or 0)
            if value > 0:
                return value
        return 300_000

    def _snapshot_health_payload(
        self,
        *,
        snapshot,
        now_ms: int,
        max_age_ms: int,
        freshness: str,
    ) -> dict:
        from collections import Counter as _Counter
        import hashlib

        per_venue_quote_count: _Counter[str] = _Counter()
        per_venue_candidate_count: _Counter[str] = _Counter()
        for quote in getattr(snapshot, "quotes", {}).values():
            venue = str(getattr(quote, "venue", "") or "")
            if venue:
                per_venue_quote_count[venue] += 1
        for candidate in getattr(snapshot, "candidates", []) or []:
            for venue_attr in ("long_venue", "short_venue"):
                venue = str(getattr(candidate, venue_attr, "") or "")
                if venue:
                    per_venue_candidate_count[venue] += 1

        published_at_ms = int(getattr(snapshot, "published_at_ms", 0) or 0)
        market_observed_at_ms = int(getattr(snapshot, "market_observed_at_ms", 0) or 0)
        snapshot_publish_age_ms = now_ms - published_at_ms if published_at_ms > 0 else 0
        market_observed_age_ms = (
            now_ms - market_observed_at_ms if market_observed_at_ms > 0 else 0
        )
        degraded_domains = [str(v) for v in getattr(snapshot, "degraded_domains", []) or []]
        degraded_venues = [str(v) for v in getattr(snapshot, "degraded_venues", []) or []]
        degraded_symbols = getattr(snapshot, "degraded_symbols", {}) or {}
        top_degraded_symbols: list[str] = []
        if isinstance(degraded_symbols, dict):
            for symbols in degraded_symbols.values():
                for symbol in symbols:
                    symbol_s = str(symbol)
                    if symbol_s and symbol_s not in top_degraded_symbols:
                        top_degraded_symbols.append(symbol_s)
                    if len(top_degraded_symbols) >= 24:
                        break
                if len(top_degraded_symbols) >= 24:
                    break

        domains = list(degraded_domains)
        if snapshot_publish_age_ms > max_age_ms:
            domains.append("snapshot_publish_stale")
        if market_observed_age_ms > max_age_ms:
            domains.append("market_observed_stale")
        for lifecycle_name, rows in (
            ("market", getattr(snapshot, "market_lifecycle", []) or []),
            ("funding", getattr(snapshot, "funding_lifecycle", []) or []),
            ("liquidity", getattr(snapshot, "liquidity_lifecycle", []) or []),
            ("transfer", getattr(snapshot, "transfer_lifecycle", []) or []),
        ):
            for row in rows:
                reason = str(getattr(row, "degraded_reason", "") or "")
                if reason and lifecycle_name not in domains:
                    domains.append(lifecycle_name)

        snapshot_path = str(self.config.runtime.sidecar_snapshot_path)
        config_hash = hashlib.sha256(
            f"{snapshot_path}|{max_age_ms}|{self.config.runtime.mode}".encode()
        ).hexdigest()[:12]
        return {
            "freshness": freshness,
            "venues": degraded_venues,
            "degraded_venues": degraded_venues,
            "degraded_domains": degraded_domains,
            "stale_degraded_domains": domains,
            "top_degraded_symbols": top_degraded_symbols,
            "snapshot_publish_age_ms": max(snapshot_publish_age_ms, 0),
            "market_observed_age_ms": max(market_observed_age_ms, 0),
            "per_venue_quote_count": dict(sorted(per_venue_quote_count.items())),
            "per_venue_candidate_count": dict(sorted(per_venue_candidate_count.items())),
            "source_mode": str(getattr(snapshot, "source_mode", "") or ""),
            "acquisition_mode": str(getattr(snapshot, "acquisition_mode", "") or ""),
            "snapshot_path": snapshot_path,
            "config_hash": config_hash,
            "ts_ms": now_ms,
        }

    def _refresh_entry_l2_session_readiness(self, now_ms: int) -> None:
        """Sync entry-local-L2 session legs from local-L2 book readiness."""
        if not self.config.strategy.local_l2_enabled:
            return
        from lightfee.engine.entry_local_l2 import apply_book_readiness_to_leg

        stale_after_ms = self._entry_local_l2_stale_after_ms()
        for pair_id, session in list(self.entry_l2_sessions.sessions.items()):
            for leg in session.legs.values():
                book = self.local_l2_runtime.get_book(leg.venue, leg.symbol)
                diag = dict(
                    apply_book_readiness_to_leg(
                        leg, book, now_ms=now_ms, stale_after_ms=stale_after_ms,
                    )
                )
                diag["pair_id"] = pair_id
                diag["leg_state"] = leg.state.value if hasattr(leg.state, "value") else str(leg.state)
                self._entry_l2_last_leg_diagnostics[(pair_id, leg.venue)] = diag
            session.refresh_state(now_ms, stale_after_ms=stale_after_ms)

        self._maybe_emit_entry_l2_readiness_diagnostics(now_ms)

    def _entry_l2_readiness_diagnostics_payload(self) -> dict:
        primary_pair_ids = sorted(self._tracked_primary_pair_ids)
        if primary_pair_ids:
            pair_ids = primary_pair_ids
        else:
            pair_ids = sorted(self.entry_l2_sessions.sessions.keys())

        not_ready: list[dict] = []
        reason_totals: Counter[str] = Counter()
        for pair_id in pair_ids:
            session = self.entry_l2_sessions.sessions.get(pair_id)
            if session is None:
                continue
            for venue in sorted(session.legs.keys()):
                leg = session.legs[venue]
                diag = self._entry_l2_last_leg_diagnostics.get((pair_id, venue))
                if diag is None:
                    diag = {
                        "pair_id": pair_id,
                        "venue": leg.venue,
                        "symbol": leg.symbol,
                        "ready": False,
                        "reason": (
                            leg.fault.value if getattr(leg, "fault", None) is not None
                            else (
                                leg.arming_reason.value
                                if getattr(leg, "arming_reason", None) is not None
                                else "not_ready"
                            )
                        ),
                        "detail": getattr(leg, "fault_detail", "") or "",
                        "book_status": "unknown",
                        "age_ms": None,
                        "observed_at_ms": getattr(leg, "last_seen_at_ms", 0),
                        "sequence": 0,
                        "leg_state": leg.state.value if hasattr(leg.state, "value") else str(leg.state),
                    }
                if diag.get("ready") is True:
                    continue
                reason = str(diag.get("reason", "not_ready"))
                reason_totals[reason] += 1
                if len(not_ready) < 24:
                    not_ready.append({
                        "pair_id": pair_id,
                        "venue": str(diag.get("venue", leg.venue)),
                        "symbol": str(diag.get("symbol", leg.symbol)),
                        "reason": reason,
                        "detail": str(diag.get("detail", "")),
                        "book_status": str(diag.get("book_status", "unknown")),
                        "age_ms": diag.get("age_ms"),
                        "observed_at_ms": int(diag.get("observed_at_ms", 0) or 0),
                        "sequence": int(diag.get("sequence", 0) or 0),
                        "leg_state": str(diag.get("leg_state", "")),
                    })

        reason_counts = Counter(sample["reason"] for sample in not_ready)
        return {
            "primary_pair_ids": primary_pair_ids,
            "not_ready": not_ready,
            "reason_counts": dict(sorted(reason_counts.items())),
            "reason_totals": dict(sorted(reason_totals.items())),
        }

    @staticmethod
    def _payload_fingerprint(payload: dict) -> str:
        import json

        return json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))

    def _maybe_emit_entry_l2_readiness_diagnostics(self, now_ms: int) -> None:
        if getattr(self.journal, "_file", None) is None:
            return
        diag = self._entry_l2_readiness_diagnostics_payload()
        if not diag["not_ready"]:
            return
        payload = {
            "primary_pair_ids": diag["primary_pair_ids"],
            "not_ready": diag["not_ready"],
            "reason_totals": diag["reason_totals"],
            "ts_ms": now_ms,
        }
        fingerprint = self._payload_fingerprint({
            "primary_pair_ids": payload["primary_pair_ids"],
            "not_ready": [
                {
                    "pair_id": s["pair_id"],
                    "venue": s["venue"],
                    "reason": s["reason"],
                    "detail": s["detail"],
                    "book_status": s["book_status"],
                }
                for s in payload["not_ready"]
            ],
        })
        if (
            fingerprint == self._last_entry_l2_readiness_diag_fingerprint
            and now_ms - self._last_entry_l2_readiness_diag_ts_ms < 60_000
        ):
            return
        self._last_entry_l2_readiness_diag_fingerprint = fingerprint
        self._last_entry_l2_readiness_diag_ts_ms = now_ms
        self.journal.append("runtime.entry_local_l2_readiness_diagnostics", payload)

    @staticmethod
    def _v1_tradeable_no_entry_reason(
        selection_blocker_counts: Counter,
        admission_blocker_counts: Counter | None = None,
    ) -> str | None:
        blocker_counts: Counter[str] = Counter()
        for key, value in selection_blocker_counts.items():
            count = int(value)
            if count > 0:
                blocker_counts[str(key)] += count
        if admission_blocker_counts is not None:
            for key, value in admission_blocker_counts.items():
                count = int(value)
                if count > 0:
                    blocker_counts[str(key)] += count

        blockers = {key for key, count in blocker_counts.items() if count > 0}
        if not blockers:
            return None
        if blockers == {"entry_waiting_for_finalization_window_too_early"}:
            return "tradeable_candidates_waiting_for_entry_finalization_window_too_early"
        if blockers == {"entry_finalization_window_expired"}:
            return "tradeable_candidates_expired_after_entry_finalization_window"
        if blockers <= {
            "entry_waiting_for_finalization_window_too_early",
            "entry_finalization_window_expired",
        }:
            return "tradeable_candidates_outside_entry_finalization_window"
        if blockers == {"entry_local_l2_waiting_for_prewarm_window"}:
            return "tradeable_candidates_waiting_for_entry_local_l2_prewarm_window"
        if blockers == {"entry_local_l2_waiting_for_dual_ready"}:
            return "tradeable_candidates_waiting_for_entry_local_l2_dual_ready"
        return "tradeable_candidates_blocked_by_entry_local_l2_readiness"

    def _emit_scan_no_entry_diagnostics(
        self,
        *,
        reason: str,
        snapshot,
        tradeable: list,
        selected_candidate_count: int,
        dispatched_candidate_count: int,
        remaining_slots: int,
        tradeable_selection_blocker_counts: Counter,
        candidate_blockers: dict[str, str],
        now_ms: int,
        admission_blocker_counts: Counter | None = None,
    ) -> None:
        if getattr(self.journal, "_file", None) is None:
            return
        from lightfee.engine.entry_local_l2 import make_candidate_pair_id

        blocked_reason_counts: Counter[str] = Counter()
        for candidate in getattr(snapshot, "candidates", []) or []:
            for blocked_reason in getattr(candidate, "blocked_reasons", []) or []:
                blocked_reason_counts[str(blocked_reason)] += 1

        readiness = self._entry_l2_readiness_diagnostics_payload()
        candidate_samples = []
        for rank, candidate in enumerate(list(tradeable)[:24], start=1):
            pair_id = getattr(candidate, "pair_id", "")
            if not pair_id:
                pair_id = make_candidate_pair_id(
                    str(getattr(candidate, "symbol", "")),
                    str(getattr(candidate, "long_venue", "")),
                    str(getattr(candidate, "short_venue", "")),
                )
            first_funding_ms = int(getattr(candidate, "first_funding_timestamp_ms", 0) or 0)
            candidate_samples.append({
                "rank": rank,
                "pair_id": pair_id,
                "symbol": str(getattr(candidate, "symbol", "")),
                "long_venue": str(getattr(candidate, "long_venue", "")),
                "short_venue": str(getattr(candidate, "short_venue", "")),
                "remaining_ms": first_funding_ms - now_ms if first_funding_ms > 0 else 0,
                "primary_tracked": pair_id in self._tracked_primary_pair_ids,
                "ranking_edge_bps": float(getattr(candidate, "ranking_edge_bps", 0.0) or 0.0),
                "blocked_reasons": list(getattr(candidate, "blocked_reasons", []) or [])[:8],
                "selection_blocker": candidate_blockers.get(pair_id, ""),
            })

        execution_liquidity_blocked_counts: Counter[str] = Counter()
        for reason_key, count in blocked_reason_counts.items():
            if "liquidity" in reason_key or reason_key.startswith("execution_"):
                execution_liquidity_blocked_counts[str(reason_key)] += int(count)

        admission_counts = admission_blocker_counts if admission_blocker_counts is not None else {}
        not_primary_tracked = int(
            admission_counts.get("entry_local_l2_waiting_for_primary_tracking", 0)
        )
        primary_tracked_not_ready = sum(
            int(v) for k, v in tradeable_selection_blocker_counts.items()
            if k not in {"entry_local_l2_waiting_for_primary_tracking"}
        )
        selection_bucket_counts = {
            "not_primary_tracked": not_primary_tracked,
            "primary_tracked_not_ready": primary_tracked_not_ready,
        }

        payload = {
            "reason": reason,
            "candidate_count": len(getattr(snapshot, "candidates", []) or []),
            "tradeable_count": len(tradeable),
            "selected_candidate_count": selected_candidate_count,
            "dispatched_candidate_count": dispatched_candidate_count,
            "remaining_slots": max(int(remaining_slots), 0),
            "blocked_reason_counts": dict(sorted(blocked_reason_counts.items())),
            "entry_candidate_blocked_counts": dict(sorted(blocked_reason_counts.items())),
            "execution_liquidity_blocked_counts": dict(
                sorted(execution_liquidity_blocked_counts.items())
            ),
            "entry_final_gate_blocked_counts": dict(
                sorted((str(k), int(v)) for k, v in tradeable_selection_blocker_counts.items())
            ),
            "tradeable_selection_blocker_counts": dict(
                sorted((str(k), int(v)) for k, v in tradeable_selection_blocker_counts.items())
            ),
            "selection_bucket_counts": selection_bucket_counts,
            "entry_local_l2_primary_ready_filter_active": bool(
                self.config.strategy.local_l2_enabled and self._tracked_primary_pair_ids
            ),
            "entry_local_l2_primary_not_ready_reason_counts": readiness["reason_counts"],
            "entry_local_l2_primary_not_ready_reason_totals": readiness["reason_totals"],
            "entry_local_l2_primary_not_ready_detail_samples": readiness["not_ready"][:24],
            "candidates": candidate_samples,
            "ts_ms": now_ms,
        }
        fingerprint = self._payload_fingerprint({
            "reason": payload["reason"],
            "candidate_count": payload["candidate_count"],
            "tradeable_count": payload["tradeable_count"],
            "selected_candidate_count": payload["selected_candidate_count"],
            "dispatched_candidate_count": payload["dispatched_candidate_count"],
            "tradeable_selection_blocker_counts": payload["tradeable_selection_blocker_counts"],
            "entry_local_l2_primary_not_ready_reason_totals": payload[
                "entry_local_l2_primary_not_ready_reason_totals"
            ],
            "candidates": [
                {
                    "pair_id": c["pair_id"],
                    "selection_blocker": c["selection_blocker"],
                }
                for c in payload["candidates"]
            ],
        })
        if (
            fingerprint == self._last_no_entry_diag_fingerprint
            and now_ms - self._last_no_entry_diag_ts_ms < 60_000
        ):
            return
        self._last_no_entry_diag_fingerprint = fingerprint
        self._last_no_entry_diag_ts_ms = now_ms
        self._last_no_entry_diagnostics = payload
        self.journal.append("scan.no_entry_diagnostics", payload)

    # ------------------------------------------------------------------
    # Entry dispatch
    # ------------------------------------------------------------------

    def _entry_selection_target(self, remaining_slots: int) -> int:
        """V1 selection buffer: remaining slots, expanded up to eight candidates."""
        if remaining_slots <= 0:
            return 0
        return min(max(remaining_slots, remaining_slots * 4), 8)

    def _candidate_pair_id(self, candidate) -> str:
        from lightfee.engine.entry_local_l2 import make_candidate_pair_id

        pair_id = getattr(candidate, "pair_id", "")
        if pair_id:
            return str(pair_id)
        return make_candidate_pair_id(
            str(getattr(candidate, "symbol", "")),
            str(getattr(candidate, "long_venue", "")),
            str(getattr(candidate, "short_venue", "")),
        )

    def _candidate_is_tradeable_for_selection(self, candidate) -> bool:
        if bool(getattr(candidate, "blocked", False)):
            return False
        if list(getattr(candidate, "blocked_reasons", []) or []):
            return False
        if float(getattr(candidate, "entry_notional_quote", 0.0) or 0.0) <= 0:
            return False
        return True

    def _market_quote_lookup(self, market_quotes) -> dict[tuple[str, str], object]:
        if not market_quotes:
            return {}
        items = market_quotes.items() if hasattr(market_quotes, "items") else enumerate(market_quotes)
        lookup: dict[tuple[str, str], object] = {}
        for key, quote in items:
            if isinstance(key, tuple) and len(key) == 2:
                venue = str(key[0])
                symbol = str(key[1])
            else:
                venue = str(getattr(quote, "venue", "") or "")
                symbol = str(getattr(quote, "symbol", "") or "")
                if (not venue or not symbol) and isinstance(key, str) and ":" in key:
                    venue, symbol = key.split(":", 1)
            if venue and symbol:
                lookup[(venue.lower(), symbol.upper())] = quote
        return lookup

    def _candidate_quote(
        self,
        quote_lookup: dict[tuple[str, str], object],
        venue: str,
        symbol: str,
    ):
        return quote_lookup.get((str(venue).lower(), str(symbol).upper()))

    def _entry_leg_depth_score(
        self,
        candidate,
        quote_lookup: dict[tuple[str, str], object],
        *,
        venue: str,
        side: str,
    ) -> float:
        quote = self._candidate_quote(quote_lookup, venue, str(getattr(candidate, "symbol", "")))
        if quote is None:
            return 10.0
        if side == "buy":
            price = float(getattr(quote, "ask", 0.0) or 0.0)
            top_size = float(getattr(quote, "ask_size", 0.0) or 0.0)
        else:
            price = float(getattr(quote, "bid", 0.0) or 0.0)
            top_size = float(getattr(quote, "bid_size", 0.0) or 0.0)
        if price <= 0.0 or top_size <= 0.0:
            return 10.0
        quantity = float(getattr(candidate, "entry_notional_quote", 0.0) or 0.0) / price
        if quantity <= 0.0:
            return 10.0
        return quantity / top_size

    def _runtime_candidate_risk_score(
        self,
        candidate,
        quote_lookup: dict[tuple[str, str], object],
    ) -> float:
        explicit_risk = getattr(candidate, "runtime_risk_score", None)
        if explicit_risk is not None:
            return max(float(explicit_risk or 0.0), 0.0)

        long_depth = self._entry_leg_depth_score(
            candidate,
            quote_lookup,
            venue=str(getattr(candidate, "long_venue", "")),
            side="buy",
        )
        short_depth = self._entry_leg_depth_score(
            candidate,
            quote_lookup,
            venue=str(getattr(candidate, "short_venue", "")),
            side="sell",
        )
        depth_risk = max(long_depth, short_depth, 0.0)
        selection_risk = float(getattr(candidate, "selection_risk_score", 0.0) or 0.0)
        return max(depth_risk, selection_risk, 0.0)

    def _runtime_candidate_selection_score(
        self,
        candidate,
        quote_lookup: dict[tuple[str, str], object] | None = None,
    ) -> float:
        ranking_edge = float(getattr(candidate, "ranking_edge_bps", 0.0) or 0.0)
        risk_score = self._runtime_candidate_risk_score(candidate, quote_lookup or {})
        return ranking_edge / (1.0 + max(risk_score, 0.0))

    def _candidate_final_selection_sort_key(
        self,
        candidate,
        quote_lookup: dict[tuple[str, str], object] | None = None,
    ) -> tuple[float, float, float, str]:
        return (
            -self._runtime_candidate_selection_score(candidate, quote_lookup),
            -float(getattr(candidate, "ranking_edge_bps", 0.0) or 0.0),
            -float(getattr(candidate, "worst_case_edge_bps", 0.0) or 0.0),
            self._candidate_pair_id(candidate),
        )

    def _has_pending_residual_pair(self, pair_id: str) -> bool:
        for task in self.state.pending_residual_repairs:
            if isinstance(task, dict):
                task_pair_id = task.get("pair_id", "")
            else:
                task_pair_id = getattr(task, "pair_id", "")
            if str(task_pair_id) == pair_id:
                return True
        return False

    def _select_entry_candidates(
        self,
        tradeable: list,
        *,
        now_ms: int,
        remaining_slots: int,
        selection_blocker_counts: Counter,
        candidate_blockers: dict[str, str],
        market_quotes=None,
        admission_blocker_counts: Counter | None = None,
    ) -> list:
        """V1 select_entry_candidates_from_refs parity for the final entry list."""
        target = self._entry_selection_target(remaining_slots)
        if target <= 0:
            return []

        admission_reasons = {"entry_local_l2_waiting_for_primary_tracking"}

        active_symbols = {
            str(getattr(position, "symbol", ""))
            for position in self.state.open_positions.values()
        }
        active_symbols.update(
            str(getattr(pending, "symbol", ""))
            for pending in self.state.pending_entries.values()
        )
        selected_symbols: set[str] = set()
        ranked: list = []
        selected: list = []

        for candidate in tradeable:
            if not self._candidate_is_tradeable_for_selection(candidate):
                continue
            symbol = str(getattr(candidate, "symbol", ""))
            pair_id = self._candidate_pair_id(candidate)
            blocker = self._entry_local_l2_selection_blocker(candidate, now_ms)
            if blocker:
                blocker_str = str(blocker)
                # Admission buckets (not primary tracked) vs readiness failures
                if blocker_str in admission_reasons:
                    if admission_blocker_counts is not None:
                        admission_blocker_counts[blocker_str] += 1
                else:
                    selection_blocker_counts[blocker_str] += 1
                candidate_blockers[pair_id] = blocker_str
                if blocker_str not in {
                    "entry_waiting_for_finalization_window_too_early",
                    "entry_finalization_window_expired",
                }:
                    self.journal.append(
                        "runtime.entry_blocked_local_l2_selection",
                        {
                            "symbol": symbol,
                            "pair_id": pair_id,
                            "reason": blocker_str,
                            "ts_ms": now_ms,
                        },
                    )
                continue
            ranked.append(candidate)

        quote_lookup = self._market_quote_lookup(market_quotes)
        ranked.sort(
            key=lambda candidate: self._candidate_final_selection_sort_key(
                candidate,
                quote_lookup,
            )
        )

        for candidate in ranked:
            symbol = str(getattr(candidate, "symbol", ""))
            pair_id = self._candidate_pair_id(candidate)
            if symbol in active_symbols or symbol in selected_symbols:
                continue
            if self._has_pending_residual_pair(pair_id):
                continue
            selected.append(candidate)
            selected_symbols.add(symbol)
            if len(selected) >= target:
                break
        return selected

    def _entry_finalization_window_blocker(
        self,
        first_funding_timestamp_ms: int,
        now_ms: int,
    ) -> str | None:
        """V1 final entry window: entries are allowed in [min_before, entry_window]."""
        remaining_ms = first_funding_timestamp_ms - max(now_ms, 0)
        min_before_ms = self.config.strategy.min_scan_minutes_before_funding * 60_000
        entry_window_ms = self.config.strategy.entry_window_secs * 1000

        if remaining_ms <= 0 or (min_before_ms > 0 and remaining_ms < min_before_ms):
            return "entry_finalization_window_expired"
        if entry_window_ms > 0 and remaining_ms > entry_window_ms:
            return "entry_waiting_for_finalization_window_too_early"
        return None

    def _entry_local_l2_selection_blocker(self, candidate, now_ms: int) -> str | None:
        """V1 entry local L2 selection gate: check prewarm, primary tracking, dual-ready.

        Returns a reason string if blocked, or None if ready to proceed.

        V1 (Rust: market_data.rs:1518-1526, final_gate.rs entry_final_gate_result_from_candidate_local_l2):
        - Live + local_l2_enabled → gate applies
        - Candidate must be in primary tracked set
        - Session must exist for pair_id
        - Both legs must be ready (dual-ready)
        - V1 prewarm: remaining_ms = first_funding_timestamp_ms - now_ms;
          remaining_ms > 0 && remaining_ms <= prewarm_window_secs * 1000

        Blocker reasons (V1 stable labels):
        - entry_waiting_for_finalization_window_too_early
        - entry_finalization_window_expired
        - entry_local_l2_waiting_for_prewarm_window
        - entry_local_l2_waiting_for_primary_tracking
        - entry_local_l2_waiting_for_dual_ready
        """
        if self.config.runtime.mode != "live":
            return None

        from lightfee.engine.entry_local_l2 import make_candidate_pair_id

        symbol = getattr(candidate, "symbol", "")
        long_ven = str(getattr(candidate, "long_venue", ""))
        short_ven = str(getattr(candidate, "short_venue", ""))
        pair_id = getattr(candidate, "pair_id", None)
        if not pair_id:
            pair_id = make_candidate_pair_id(symbol, long_ven, short_ven)

        # V1 prewarm: remaining_ms = first_funding_timestamp_ms - now_ms
        first_funding_ts = getattr(candidate, "first_funding_timestamp_ms", 0)
        if first_funding_ts <= 0:
            if not self.config.strategy.local_l2_enabled:
                return None
            return "entry_local_l2_waiting_for_prewarm_window"
        remaining_ms = first_funding_ts - max(now_ms, 0)
        finalization_blocker = self._entry_finalization_window_blocker(
            first_funding_ts,
            now_ms,
        )
        if finalization_blocker:
            return finalization_blocker
        if not self.config.strategy.local_l2_enabled:
            return None
        prewarm_window_ms = self.config.strategy.entry_local_l2_prewarm_window_secs * 1000
        if remaining_ms <= 0 or remaining_ms > prewarm_window_ms:
            return "entry_local_l2_waiting_for_prewarm_window"

        # Primary tracking: candidate must be in primary tracked set
        if pair_id not in self._tracked_primary_pair_ids:
            return "entry_local_l2_waiting_for_primary_tracking"

        # Session dual-ready check
        session = self.entry_l2_sessions.sessions.get(pair_id)
        if session is None:
            return "entry_local_l2_waiting_for_dual_ready"

        if not session.both_legs_ready(now_ms, stale_after_ms=self._entry_local_l2_stale_after_ms()):
            return "entry_local_l2_waiting_for_dual_ready"

        return None

    async def _dispatch_entry(self, candidate, now_ms: int, price_hint: float = 0.0) -> bool:
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
                return False

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
            return False

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
                return False

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
                return False

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
            return False

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
        # Must use the same CID generation as build_entry_orders so the
        # dedup index keys match the actual on-wire clientOrderId.
        from lightfee.venues.cid import generate_exchange_cid
        maker_venue = long_venue if maker_leg == Side.BUY else short_venue
        hedge_venue = short_venue if maker_leg == Side.BUY else long_venue
        maker_cid = generate_exchange_cid(entry_id, "m", maker_venue)
        hedge_cid = generate_exchange_cid(entry_id, "h", hedge_venue)

        if is_client_order_id_duplicate(maker_cid, self._recovery_dedup_index):
            self.journal.append(
                "runtime.entry_skipped_duplicate_client_order_id",
                {
                    "entry_id": entry_id,
                    "client_order_id": maker_cid,
                    "reason": "duplicate maker clientOrderId in recovery dedup index",
                },
            )
            return False

        if is_client_order_id_duplicate(hedge_cid, self._recovery_dedup_index):
            self.journal.append(
                "runtime.entry_skipped_duplicate_client_order_id",
                {
                    "entry_id": entry_id,
                    "client_order_id": hedge_cid,
                    "reason": "duplicate hedge clientOrderId in recovery dedup index",
                },
            )
            return False

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
            return False

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
                if getattr(result.pending_entry, "outcome", "") == "rejected":
                    self.journal.append(
                        "runtime.rejected_pending_suppressed",
                        {
                            "pending_id": result.pending_entry.pending_id,
                            "symbol": result.pending_entry.symbol,
                            "route": result.route.value,
                            "state": result.state.value,
                            "reason": "maker rejected is terminal in V1",
                        },
                    )
                    return True
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
            return False

        return True

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
