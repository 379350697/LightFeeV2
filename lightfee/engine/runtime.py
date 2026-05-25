"""Live runtime: multi-lane tick loop, snapshot consumption, supervision, export."""

from __future__ import annotations

import asyncio
import math
from collections import Counter
from typing import Optional

from lightfee.config.schema import AppConfig
from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import OrderFill, PositionSnapshot, Side, TimeInForce, Venue
from lightfee.core.errors import OrderSubmitError
from lightfee.engine.close_executor import _is_bybit_duplicate_order_link_id
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
    clear_stale_recovery_block_if_recovery_clean,
    build_persistent_state_view,
)
from lightfee.engine.state import EngineState, HedgeInflight, OpenPosition
from lightfee.engine.supervisor import Supervisor
from lightfee.persistence.journal import Journal
from lightfee.persistence.snapshot_store import SnapshotStore
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from lightfee.marketdata.l2 import L2BookStatus, L2PoolAssignment, LocalL2BookKey
from lightfee.sidecar.snapshot import evaluate_snapshot_freshness, SnapshotFreshness
from lightfee.sidecar.publisher import load_snapshot
from lightfee.strategy.discovery import discover_tradeable_candidates
from lightfee.venues.transport import is_hyperliquid_non_retryable_auth_signing_error


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

        # V1 private WS tracking: each venue gets workers started once.
        # Tracked per venue to handle reconfiguration gracefully.
        self._private_ws_started: set[Venue] = set()
        self._private_ws_symbols: dict[Venue, set[str]] = {}

        # V1 per-venue risk snapshot runtime cache
        #   key: venue → {fetched_at_ms, result: OK(Optional[ARS]) | Err(str)}
        self._risk_snapshot_cache: dict[Venue, dict] = {}

        # V1 maker-event lane state
        #   Tracks pending passive maker entries with last known price for repricing.
        #   Values are either dicts (sidecar path) or (PassiveOrderManager, float) tuples
        #   (local-L2 parity path).
        self._maker_event_state: dict[str, object] = {}  # entry_id -> dict | (manager, price)
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
        self._post_only_reject_cooldown_until_ms: dict[tuple[str, str], int] = {}

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
        passive_metadata = getattr(adapter, "passive_metadata", None)
        if callable(passive_metadata):
            try:
                metadata = passive_metadata(symbol) or {}
                min_notional = float(
                    metadata.get("min_notional", metadata.get("min_notional_quote", 0.0))
                    or 0.0
                )
                if min_notional > 0:
                    return min_notional
            except Exception:
                pass
        adapter_min = float(
            getattr(adapter, "min_notional_quote", getattr(adapter, "_min_notional_quote", 0.0))
            or 0.0
        )
        if adapter_min > 0:
            return adapter_min
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

    async def _verify_live_trading_preflights(self) -> None:
        """Run read-only venue admission checks before selector can trade."""
        blocked = {
            "api_key",
            "api_secret",
            "secret",
            "signature",
            "private_key",
            "headers",
            "auth",
        }
        for venue, adapter in sorted(
            self._venue_adapters.items(),
            key=lambda item: item[0].value if hasattr(item[0], "value") else str(item[0]),
        ):
            transport = getattr(adapter, "_transport", adapter)
            preflight_fn = getattr(transport, "verify_live_trading_preflight", None)
            if not callable(preflight_fn):
                continue
            try:
                raw_payload = await preflight_fn()
            except Exception as exc:
                raw_payload = {
                    "venue": venue.value if hasattr(venue, "value") else str(venue),
                    "status": "failed",
                    "trading_capability_trusted": False,
                    "reason": str(exc),
                }
            payload: dict[str, object] = {}
            for key, value in dict(raw_payload or {}).items():
                key_s = str(key)
                if any(token in key_s.lower() for token in blocked):
                    continue
                payload[key_s] = value
            payload.setdefault("venue", venue.value if hasattr(venue, "value") else str(venue))
            payload.setdefault("status", "ok")
            self.journal.append("startup.trading_preflight", payload)

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
        await self._verify_live_trading_preflights()

        # Phase 2 – Resolve runtime symbols (daily-universe integration point)
        symbol_info = await prepare_runtime_symbols(self.config)

        # Phase 3 – Recover or start fresh
        self.state = recover_from_snapshot(self.snapshot_store, self.journal)
        self._restore_passive_order_manager_states()
        self.state.run_id = self.journal.run_id
        if self.state.started_at_ms == 0:
            self.state.started_at_ms = wall_clock_now_ms()

        # Build recovery dedup index from recovered pending state
        self._recovery_dedup_index = build_recovery_dedup_index(self.state)
        startup_live_probe_ms = wall_clock_now_ms()
        await self._recover_startup_live_positions(
            self._startup_position_probe_symbols(symbol_info),
            startup_live_probe_ms,
        )
        current_startup_recovery_block = (
            bool(self.state.recovery_blocked_reason)
            and self.state.recovery_blocked_at_ms >= startup_live_probe_ms
        )

        # Phase 4 – Recovery-aware startup (Rust V1: finalize_startup_position_recovery)
        from lightfee.engine.recovery import needs_reconciliation, classify_startup_recovery_state

        classified_recovery_state = classify_startup_recovery_state(self.state)
        if (
            classified_recovery_state == "clean"
            and not current_startup_recovery_block
            and clear_stale_recovery_block_if_recovery_clean(
                self.state,
                self.journal,
            )
        ):
            classified_recovery_state = "clean"

        recovery_class = (
            "blocked"
            if self.state.recovery_blocked_reason
            else classified_recovery_state
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
            self.passive_close_executor.set_l2_quote_resolver(self._resolve_local_l2_quote)
            # Inject close executor for DUAL_TAKER fallback
            if self.close_executor is not None:
                self.passive_close_executor.set_close_executor(self.close_executor)

        # Phase 8 – Recover pending passive closes
        await self._recover_passive_closes()
        if not current_startup_recovery_block:
            clear_stale_recovery_block_if_recovery_clean(self.state, self.journal)

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

    async def _position_probe_symbols_for_venue(
        self, venue: Venue, adapter: VenueAdapter, symbols: list[str],
    ) -> list[str]:
        """Filter fallback single-position probes through a venue symbol catalog."""
        return await self._filter_symbols_supported_by_venue(
            venue,
            adapter,
            symbols,
            skip_event_kind="recovery.live_position_probe_symbol_skipped",
        )

    async def _filter_symbols_supported_by_venue(
        self,
        venue: Venue,
        adapter: VenueAdapter,
        symbols: list[str],
        *,
        skip_event_kind: str,
    ) -> list[str]:
        """Filter symbols through a venue-provided trading catalog when present."""
        ensure_loaded = getattr(adapter, "ensure_supported_symbols_loaded", None)
        if callable(ensure_loaded):
            try:
                maybe_coro = ensure_loaded()
                if asyncio.iscoroutine(maybe_coro):
                    await maybe_coro
            except Exception:
                pass

        try:
            supported_raw = adapter.supported_symbols()
        except Exception:
            supported_raw = []
        supported = {str(symbol) for symbol in supported_raw if str(symbol)}
        if not supported:
            return symbols

        transport = getattr(adapter, "_transport", None)
        to_venue_symbol = getattr(transport, "_venue_symbol", None)

        filtered: list[str] = []
        for symbol in symbols:
            venue_symbol = str(symbol)
            if callable(to_venue_symbol):
                try:
                    venue_symbol = str(to_venue_symbol(symbol))
                except Exception:
                    venue_symbol = str(symbol)
            if str(symbol) in supported or venue_symbol in supported:
                filtered.append(symbol)
            else:
                if skip_event_kind and getattr(self.journal, "_file", None) is not None:
                    self.journal.append(
                        skip_event_kind,
                        {
                            "venue": venue.value,
                            "symbol": symbol,
                            "venue_symbol": venue_symbol,
                            "reason": "unsupported_symbol",
                        },
                    )
        return filtered

    async def _filter_candidates_supported_by_venue_catalog(
        self,
        candidates: list,
        *,
        skip_event_kind: str = "runtime.candidate_symbol_skipped",
    ) -> list:
        """Filter live candidates through both venues' trading catalogs.

        V1 build_scan_symbol_cache only admits symbols supported by both venues
        in a directed pair. V2 sidecar snapshots can still contain public quote
        rows for symbols that are not orderable on one venue, so runtime applies
        the same catalog gate before shortlist/tracking/entry selection.
        """
        if self.config.runtime.mode == "paper":
            return list(candidates)

        venue_symbols: dict[Venue, set[str]] = {}
        candidate_venues: list[tuple[object, Venue | None, Venue | None]] = []
        for candidate in candidates:
            try:
                long_venue = Venue.from_str(str(getattr(candidate, "long_venue", "")))
            except ValueError:
                long_venue = None
            try:
                short_venue = Venue.from_str(str(getattr(candidate, "short_venue", "")))
            except ValueError:
                short_venue = None
            candidate_venues.append((candidate, long_venue, short_venue))
            symbol = str(getattr(candidate, "symbol", "") or "")
            if not symbol:
                continue
            for venue in (long_venue, short_venue):
                if venue is not None:
                    venue_symbols.setdefault(venue, set()).add(symbol)

        supported_by_venue: dict[Venue, set[str] | None] = {}
        for venue, symbols in venue_symbols.items():
            adapter = self.get_venue_adapter(venue)
            if adapter is None:
                supported_by_venue[venue] = None
                continue
            filtered = await self._filter_symbols_supported_by_venue(
                venue,
                adapter,
                sorted(symbols),
                skip_event_kind="",
            )
            supported_by_venue[venue] = set(filtered)

        filtered_candidates: list = []
        skipped = 0
        for candidate, long_venue, short_venue in candidate_venues:
            symbol = str(getattr(candidate, "symbol", "") or "")

            def venue_supports(venue: Venue | None) -> bool:
                if venue is None:
                    return True
                supported = supported_by_venue.get(venue)
                return supported is None or symbol in supported

            long_supported = venue_supports(long_venue)
            short_supported = venue_supports(short_venue)
            if long_supported and short_supported:
                filtered_candidates.append(candidate)
                continue

            skipped += 1
            if getattr(self.journal, "_file", None) is not None:
                self.journal.append(
                    skip_event_kind,
                    {
                        "symbol": symbol,
                        "pair_id": self._candidate_pair_id(candidate),
                        "long_venue": (
                            long_venue.value
                            if long_venue
                            else str(getattr(candidate, "long_venue", ""))
                        ),
                        "short_venue": (
                            short_venue.value
                            if short_venue
                            else str(getattr(candidate, "short_venue", ""))
                        ),
                        "long_supported": long_supported,
                        "short_supported": short_supported,
                        "reason": "unsupported_symbol",
                    },
                )

        if skipped > 0 and getattr(self.journal, "_file", None) is not None:
            self.journal.append(
                "runtime.tradeable_candidates_catalog_filtered",
                {
                    "input_count": len(candidates),
                    "output_count": len(filtered_candidates),
                    "skipped_count": skipped,
                },
            )
        return filtered_candidates

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

        fallback_probe_symbols: dict[Venue, list[str]] = {}
        for venue, adapter in self._venue_adapters.items():
            if venue not in fallback_venues:
                continue
            fallback_probe_symbols[venue] = await self._position_probe_symbols_for_venue(
                venue, adapter, symbols,
            )

        tasks = [
            fetch_one(venue, adapter, symbol)
            for venue, adapter in self._venue_adapters.items()
            if venue in fallback_probe_symbols
            for symbol in fallback_probe_symbols[venue]
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
            self._sync_passive_order_manager_states()
            self.snapshot_store.write(build_persistent_state_view(self.state))

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

        if self.config.runtime.mode != "paper":
            from lightfee.core.domain import Venue as VenueEnum

            filtered_pairs: set[tuple[str, str]] = set()
            venue_symbols_for_filter: dict[str, list[str]] = {}
            for venue_str, symbol in target_pairs:
                venue_symbols_for_filter.setdefault(venue_str, []).append(symbol)

            for venue_str, symbols in venue_symbols_for_filter.items():
                try:
                    ven = VenueEnum.from_str(venue_str)
                    adapter = self.get_venue_adapter(ven) if ven in self._venue_adapters else None
                except (ValueError, KeyError):
                    adapter = None
                    ven = None
                if adapter is None or ven is None:
                    filtered_pairs.update((venue_str, sym) for sym in symbols)
                    continue
                filtered_symbols = await self._filter_symbols_supported_by_venue(
                    ven,
                    adapter,
                    sorted(symbols),
                    skip_event_kind="runtime.local_l2_symbol_skipped",
                )
                filtered_pairs.update((venue_str, sym) for sym in filtered_symbols)

            target_pairs = filtered_pairs

        if not target_pairs:
            self.journal.append(
                "runtime.local_l2_phase_complete",
                {
                    "books_bootstrapped": 0,
                    "reason": "no target pairs after venue symbol catalog filtering",
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

        venue_symbols: dict[str, list[str]] = {}
        for venue_str, symbol in target_pairs:
            venue_symbols.setdefault(venue_str, []).append(symbol)

        # Step 2: Start WS streams FIRST for all venues (V1: start_local_l2_ws)
        # This ensures delta updates are captured (buffered) during bootstrap gap
        if (
            self.config.strategy.local_l2_enabled
            and getattr(self.config.strategy, 'local_l2_ws_enabled', False)
            and self.config.runtime.mode != "paper"
        ):
            ws_started = 0
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
                if (venue, sym) not in target_pairs:
                    continue
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

    async def _ensure_l2_active_for_candidates(
        self,
        candidates,
        now_ms: int,
        *,
        tracked_opportunities=None,
    ) -> None:
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

        candidates = list(candidates or [])
        tracked_opportunities = list(tracked_opportunities or [])
        tracked_keys: set[LocalL2BookKey] = set()
        pool_by_key: dict[LocalL2BookKey, L2PoolAssignment] = {}
        pool_rank = {
            L2PoolAssignment.HOT_EXEC: 0,
            L2PoolAssignment.WARM: 1,
            L2PoolAssignment.RETAINED: 2,
        }

        def venue_name(venue) -> str:
            return venue.value if hasattr(venue, "value") else str(venue or "")

        def remember_key(venue, symbol, pool: L2PoolAssignment) -> LocalL2BookKey | None:
            ven_str = venue_name(venue)
            sym = str(symbol or "")
            if not ven_str or not sym:
                return None
            key = LocalL2BookKey(venue=ven_str, symbol=sym)
            tracked_keys.add(key)
            existing = pool_by_key.get(key)
            if existing is None or pool_rank[pool] < pool_rank[existing]:
                pool_by_key[key] = pool
            return key

        for opportunity in tracked_opportunities:
            pool = (
                L2PoolAssignment.HOT_EXEC
                if getattr(getattr(opportunity, "class_", None), "value", "") == "primary_tracked"
                else L2PoolAssignment.WARM
            )
            sym = getattr(opportunity, "symbol", "")
            for venue in (
                getattr(opportunity, "long_venue", ""),
                getattr(opportunity, "short_venue", ""),
            ):
                remember_key(venue, sym, pool)

        # Collect (venue, symbol) pairs from candidates that need L2
        # CandidateInput has long_venue/short_venue as str fields (not leg objects)
        needed: dict[str, set[str]] = {}  # venue -> {symbols}
        stale_after_ms = self._entry_local_l2_stale_after_ms()
        for c in candidates:
            sym = getattr(c, 'symbol', '')
            for ven_str in (getattr(c, 'long_venue', ''), getattr(c, 'short_venue', '')):
                if not ven_str or not sym:
                    continue
                key = LocalL2BookKey(venue=ven_str, symbol=str(sym))
                tracked_keys.add(key)
                pool_by_key.setdefault(key, L2PoolAssignment.HOT_EXEC)
                desired_pool = pool_by_key.get(key, L2PoolAssignment.HOT_EXEC)
                # Skip if already active
                book = self.local_l2_runtime.get_book(ven_str, sym)
                if book is not None:
                    self.local_l2_runtime.assign(
                        ven_str, sym, desired_pool, now_ms=now_ms,
                    )
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

        for position in getattr(self.state, "open_positions", {}).values():
            sym = getattr(position, "symbol", "")
            remember_key(getattr(position, "long_venue", ""), sym, L2PoolAssignment.RETAINED)
            remember_key(getattr(position, "short_venue", ""), sym, L2PoolAssignment.RETAINED)

        for pending in getattr(self.state, "pending_entries", {}).values():
            sym = getattr(pending, "symbol", "")
            remember_key(getattr(pending, "long_venue", ""), sym, L2PoolAssignment.HOT_EXEC)
            remember_key(getattr(pending, "short_venue", ""), sym, L2PoolAssignment.HOT_EXEC)

        for pending_close in getattr(self.state, "pending_passive_closes", {}).values():
            position = getattr(pending_close, "position_snapshot", None)
            if position is None:
                continue
            sym = getattr(position, "symbol", "")
            remember_key(getattr(position, "long_venue", ""), sym, L2PoolAssignment.HOT_EXEC)
            remember_key(getattr(position, "short_venue", ""), sym, L2PoolAssignment.HOT_EXEC)

        if not needed:
            self.l2_data_plane.prune_untracked_books(
                tracked_keys,
                now_ms,
                retained_max_age_ms=max(stale_after_ms, 300_000),
            )
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
            filtered_symbols = await self._filter_symbols_supported_by_venue(
                ven,
                adapter,
                symbols_list,
                skip_event_kind="runtime.local_l2_symbol_skipped",
            )
            symbols_list = filtered_symbols[:per_venue_budget]
            if not symbols_list:
                continue

            for sym in symbols_list:
                rules = get_venue_rules(ven_str)
                key = LocalL2BookKey(venue=ven_str, symbol=sym)
                desired_pool = pool_by_key.get(key, L2PoolAssignment.HOT_EXEC)
                book = self.local_l2_runtime.ensure_book(ven_str, sym)
                self.local_l2_runtime.assign(
                    ven_str, sym, desired_pool, now_ms=now_ms,
                )
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

        self.l2_data_plane.prune_untracked_books(
            tracked_keys,
            now_ms,
            retained_max_age_ms=max(stale_after_ms, 300_000),
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
        allowed_pairs: set[tuple[str, str]] = set()
        if self.config.runtime.mode != "paper":
            from lightfee.core.domain import Venue as VenueEnum

            venue_symbols: dict[str, list[str]] = {}
            for entry in snap:
                venue = entry.get("venue", "")
                symbol = entry.get("symbol", "")
                if venue and symbol:
                    venue_symbols.setdefault(venue, []).append(symbol)

            for venue_str, symbols in venue_symbols.items():
                try:
                    venue_enum = VenueEnum.from_str(venue_str)
                    adapter = (
                        self.get_venue_adapter(venue_enum)
                        if venue_enum in self._venue_adapters
                        else None
                    )
                except (ValueError, KeyError):
                    adapter = None
                    venue_enum = None
                if adapter is None or venue_enum is None:
                    allowed_pairs.update((venue_str, symbol) for symbol in symbols)
                    continue
                filtered_symbols = await self._filter_symbols_supported_by_venue(
                    venue_enum,
                    adapter,
                    sorted(set(symbols)),
                    skip_event_kind="runtime.local_l2_symbol_skipped",
                )
                allowed_pairs.update((venue_str, symbol) for symbol in filtered_symbols)
        else:
            allowed_pairs = {
                (entry.get("venue", ""), entry.get("symbol", ""))
                for entry in snap
                if entry.get("venue", "") and entry.get("symbol", "")
            }

        for entry in snap:
            venue = entry.get("venue", "")
            symbol = entry.get("symbol", "")
            if not venue or not symbol:
                continue
            if (venue, symbol) not in allowed_pairs:
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

    def _sync_passive_order_manager_states(self) -> None:
        """Write _maker_event_state manager runtime dicts to EngineState for snapshot."""
        from lightfee.engine.passive_order_manager import PassiveOrderManager
        states: dict[str, dict] = {}
        for entry_id, stored in self._maker_event_state.items():
            if isinstance(stored, tuple) and len(stored) == 2:
                manager, price = stored
                if isinstance(manager, PassiveOrderManager):
                    d = manager.runtime_dict()
                    d["maker_price"] = price
                    states[entry_id] = d
                else:
                    states[entry_id] = {"maker_price": price}
            elif isinstance(stored, dict):
                states[entry_id] = dict(stored)
        self.state.passive_order_manager_states = states

    def _restore_passive_order_manager_states(self) -> None:
        """Restore PassiveOrderManager states from EngineState after snapshot recovery."""
        from lightfee.engine.passive_order_manager import (
            PassiveOrderManager,
            PassiveOrderManagerProfile,
        )
        profile = PassiveOrderManagerProfile(
            max_consecutive_failures=self.config.strategy.passive_max_consecutive_failures,
            failure_cooldown_ms=self.config.strategy.passive_failure_cooldown_ms,
            reprice_threshold_bps=self.config.strategy.passive_reprice_threshold_bps,
            cancel_replace_threshold_bps=self.config.strategy.passive_cancel_replace_threshold_bps,
        )
        restored: dict[str, object] = {}
        for entry_id, d in self.state.passive_order_manager_states.items():
            if not isinstance(d, dict):
                continue
            manager = PassiveOrderManager(profile)
            # Restore runtime state fields
            if d.get("consecutive_failures", 0) > 0:
                last_action = d.get("last_action_at_ms")
                if last_action is not None:
                    for _ in range(min(d.get("consecutive_failures", 0), profile.max_consecutive_failures)):
                        manager.note_failure(last_action)
            if d.get("ops_bucket_tokens") is not None:
                manager._ops_bucket_tokens = float(d["ops_bucket_tokens"])
            if d.get("cooldown_until_ms") is not None:
                manager._cooldown_until_ms = d["cooldown_until_ms"]
            # V1: restore refill anchor so next _refill_ops_bucket() does not
            # reset tokens to capacity (passive_order_manager.rs:341)
            if d.get("ops_bucket_last_refill_at_ms") is not None:
                manager._ops_bucket_last_refill_at_ms = d["ops_bucket_last_refill_at_ms"]
            if d.get("last_action_at_ms") is not None:
                manager._last_action_at_ms = d["last_action_at_ms"]
            price = float(d.get("maker_price", 0.0))
            restored[entry_id] = (manager, price)
        if restored:
            self._maker_event_state.update(restored)

    async def stop(self) -> None:
        """Graceful shutdown: stop loop, WS clients, adapter shutdown, export final state, flush journal."""
        self._running = False

        # Stop WebSocket L2 streams (V1: abort workers before adapter shutdown)
        await self.l2_data_plane.stop_ws_streams()

        # V1: stop private WS workers before adapter shutdown
        for venue, adapter in list(self._venue_adapters.items()):
            if getattr(adapter, 'supports_private_health', False):
                transport = getattr(adapter, '_transport', None)
                if transport is not None:
                    transport.stop_private_ws()
                    self.journal.append(
                        "runtime.private_ws_stopped",
                        {"venue": venue.value},
                    )

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
            self._sync_passive_order_manager_states()
            self.snapshot_store.write(build_persistent_state_view(self.state))

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
        snapshot_freshness_metrics, snapshot_freshness_ages = (
            self._snapshot_freshness_observability(
                snapshot=snapshot,
                candidates=list(getattr(snapshot, "candidates", []) or []),
                now_ms=now_ms,
            )
        )
        self.state.last_scan["snapshot_freshness_metrics"] = snapshot_freshness_metrics
        self.state.last_scan["snapshot_freshness_observed_age_ms"] = snapshot_freshness_ages
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
            tradeable = await self._filter_candidates_supported_by_venue_catalog(
                tradeable,
            )
            tradeable = self._filter_candidates_by_snapshot_freshness(
                tradeable,
                snapshot=snapshot,
                now_ms=now_ms,
                metrics=snapshot_freshness_metrics,
                ages=snapshot_freshness_ages,
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
                # V1: scan.shortlist_ready — basic shortlist generated, before post-shortlist processing
                self.journal.append(
                    "scan.shortlist_ready",
                    {
                        "candidate_count": len(tradeable),
                        "tradeable_count": len(tradeable),
                        "shortlist_candidate_count": len(tradeable),
                        "shortlist_tradeable_count": len(tradeable),
                        "snapshot_freshness": freshness.value if hasattr(freshness, "value") else str(freshness),
                        "best_pair_id": tradeable[0].pair_id if tradeable else None,
                        "ts_ms": now_ms,
                    },
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
                        tracked_opportunities=tracked,
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
                    # V1: shadow promotion — best shadow replaces worst primary
                    # when score delta, hold window, execution guard, and readiness
                    # all pass (execution_core/engine.rs:2643-2719)
                    self._apply_shadow_promotion_if_eligible(
                        tracked, now_ms,
                    )
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
        # V1: event-driven session refresh — L2 events may have changed book readiness
        # (entry_local_l2_sessions.rs:275-297 → BookUpdated → mark_leg_ready etc.)
        if events:
            self._refresh_entry_l2_session_readiness(now_ms)

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
        maker_leg = Side.BUY if strategy.maker_leg_default == "buy" else Side.SELL
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
            # V1: use the maker venue's mid price, not a single-leg fallback
            # post_only_entry_reprice_price_hint takes from working_market (entry_sync.rs:1475-1481)
            maker_venue = pending.long_venue if maker_leg == Side.BUY else pending.short_venue
            maker_mid = long_mid if maker_venue == pending.long_venue else short_mid
            mid = maker_mid
            if mid <= 0:
                continue

            # Cooldown and ops budget check via V1 PassiveOrderManager
            from lightfee.engine.passive_order_manager import (
                PassiveOrderManager,
                PassiveOrderManagerProfile,
                PassiveOrderDecisionInput,
                PassiveOrderManagerDecisionType,
                PassiveSkipReason,
            )
            maker_venue = pending.long_venue if maker_leg == Side.BUY else pending.short_venue
            stored = self._maker_event_state.get(entry_id)
            if isinstance(stored, tuple) and len(stored) == 2:
                manager, stored_price = stored
            else:
                # Fresh state or legacy dict — create new manager
                profile = PassiveOrderManagerProfile(
                    max_consecutive_failures=strategy.passive_max_consecutive_failures,
                    failure_cooldown_ms=strategy.passive_failure_cooldown_ms,
                    reprice_threshold_bps=reprice_threshold_bps,
                    cancel_replace_threshold_bps=cancel_replace_threshold_bps,
                )
                manager = PassiveOrderManager(profile)
                stored_price = stored.get("maker_price", 0.0) if isinstance(stored, dict) else 0.0
                if isinstance(stored, dict) and stored.get("consecutive_failures", 0) > 0:
                    for _ in range(stored.get("consecutive_failures", 0)):
                        manager.note_failure(stored.get("last_reprice_ms", now_ms))

            # Check if venue supports amend (V1: passive_order_supports_amend)
            # Must check __dict__ for override, not hasattr which returns True
            # for the base class NotImplementedError stub.
            from lightfee.engine.entry_sync import _adapter_supports_amend
            adapter = self._venue_adapters.get(maker_venue)
            supports_amend = _adapter_supports_amend(adapter)

            decision_input = PassiveOrderDecisionInput(
                tick_size=0.1,  # V1: venue-specific tick size
                target_price=mid,
                current_price=stored_price if stored_price > 0 else None,
                target_quantity=getattr(pending, 'long_quantity', 0) or 0,
                supports_amend=supports_amend,
            )
            decision = manager.decide(decision_input, now_ms)

            # First-seen: store initial price without reprice action
            if decision.kind == PassiveOrderManagerDecisionType.PLACE:
                self._maker_event_state[entry_id] = (manager, mid)
                continue

            if decision.kind == PassiveOrderManagerDecisionType.COOLDOWN:
                continue
            if decision.kind == PassiveOrderManagerDecisionType.HOLD:
                if decision.skip_reason == PassiveSkipReason.OPS_BUDGET_EXCEEDED:
                    self.journal.append(
                        "execution.passive_ops_rate_limited",
                        {"entry_id": entry_id, "reason": "ops_budget_exceeded",
                         "ts_ms": now_ms},
                    )
                continue

            # Determine action from decision
            if decision.kind == PassiveOrderManagerDecisionType.AMEND:
                action = "reprice"
            elif decision.kind == PassiveOrderManagerDecisionType.CANCEL_REPLACE:
                action = "cancel_replace"
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
                # V1: consume ops token BEFORE submitting (token bucket rate limiting).
                # AMEND = 1 token. CANCEL_REPLACE = 2 tokens (cancel + submit).
                manager.note_operation(now_ms)
                if action == "cancel_replace":
                    manager.note_operation(now_ms)
                result = await self._reprice_passive_maker_l2(
                    pending, mid, stored_price, action, now_ms, entry_id,
                )
                # Update PassiveOrderManager runtime tracker
                manager.note_success(now_ms)
                self._maker_event_state[entry_id] = (manager, mid)
                # Write back to authoritative PendingEntry state
                pe = self.state.pending_entries.get(entry_id)
                if pe is not None:
                    pe.maker_price = mid
                    if result.order_id:
                        pe.maker_order_id = result.order_id
                woke_positions += 1
            except Exception as e:
                manager.note_failure(now_ms)
                self._maker_event_state[entry_id] = (manager, stored_price)
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

            # --- Passive maker maintenance (V1: maintain_pending_entry_passive_order) ---
            # Active tick-level lifecycle for resting maker orders:
            # progress query → try_window check → rest_timeout → cancel → abort/finalize
            await self._maintain_pending_entry_passive_orders(now_ms)

            # --- Post-tick housekeeping ---
            await self._post_tick_housekeeping(now_ms)

            # --- Snapshot local-L2 state for persistence ---
            self._snapshot_local_l2_state()

            # --- Persist state snapshot ---
            self._sync_passive_order_manager_states()
            self.snapshot_store.write(build_persistent_state_view(self.state))

            # --- Sleep until next poll ---
            active_poll_ms = active_position_poll_interval_ms(
                self.state.lifecycle, poll_ms, active_count
            )
            await asyncio.sleep(min(poll_ms, active_poll_ms) / 1000.0)

    # ------------------------------------------------------------------
    # Passive entry maintenance (V1 maintain_pending_entry_passive_order — Fix 1)
    # ------------------------------------------------------------------

    async def _maintain_pending_entry_passive_orders(self, now_ms: int) -> None:
        """V1: maintain_pending_entry_passive_order() at tick level.

        Active maintenance for each pending entry with a resting passive maker
        order.  Replicates the V1 passive maker lifecycle:

        1. Query passive order progress from the venue adapter
        2. Apply progress (update fill quantities, progress state)
        3. maker_try_window_fill_shortfall — cancel if elapsed > 1500ms with
           fill ratio below 25% (zero-fill protection)
        4. maker_entry_rest_timeout — cancel if elapsed > 6000ms
        5. Post-cancel: zero-fill → abort, partial-fill → hedge → finalize,
           uncertain → retain for reconciliation

        V1 ref: entry_sync.rs:1554 maintain_pending_entry_passive_order()
        """
        if not self._venue_adapters:
            return

        strategy = self.config.strategy
        try_window_ms = getattr(strategy, "maker_try_window_ms", 0) or 0
        min_fill_ratio = getattr(strategy, "maker_min_fill_ratio", 0.25) or 0.25
        rest_timeout_ms = getattr(strategy, "maker_entry_rest_timeout_ms", 6000) or 6000
        poll_ms = getattr(strategy, "maker_entry_progress_poll_ms", 500) or 500

        resolved: list[str] = []

        for entry_id, pending in list(self.state.pending_entries.items()):
            po = pending.passive_order
            if po is None:
                continue
            maker_venue = pending.maker_venue()

            # Guard: must have a valid order ID to query/cancel
            if not po.order_id:
                continue

            # Respect poll interval — V1 next_progress_poll_ms gate
            if pending.next_progress_poll_ms > 0 and now_ms < pending.next_progress_poll_ms:
                continue

            # Already in reconciliation flow
            if po.cancel_requested() and po.maker_completed():
                continue

            adapter = self._venue_adapters.get(maker_venue)
            if adapter is None:
                continue

            # --- Step 1: Query passive order progress ---
            progress = None
            try:
                progress = await adapter.query_passive_order_progress(
                    symbol=pending.symbol,
                    order_id=po.order_id,
                    client_order_id=po.client_order_id or None,
                    side=pending.maker_side(),
                )
            except Exception as exc:
                self.journal.append(
                    "passive_maintenance.progress_query_error",
                    {"entry_id": entry_id, "symbol": pending.symbol,
                     "venue": str(maker_venue), "error": str(exc)},
                )
                pending.next_progress_poll_ms = now_ms + poll_ms
                continue

            # --- Step 2: Apply progress to pending entry ---
            if progress is not None:
                po.last_progress_state = progress.state
                if progress.cumulative_quantity > pending.maker_leg_filled:
                    prev_filled = pending.maker_leg_filled
                    pending.maker_leg_filled = progress.cumulative_quantity
                    if progress.average_price > 0:
                        pending.maker_fill_price = progress.average_price
                    self.journal.append(
                        "passive_maintenance.maker_progress",
                        {
                            "entry_id": entry_id, "symbol": pending.symbol,
                            "prev_filled": prev_filled,
                            "new_filled": progress.cumulative_quantity,
                            "state": progress.state.value,
                            "venue": str(maker_venue),
                        },
                    )

            # --- Step 3: maker_try_window_fill_shortfall ---
            if (
                po.cancel_requested_at_ms <= 0
                and not po.maker_completed()
                and try_window_ms > 0
            ):
                shortfall = self._maker_try_window_fill_shortfall(
                    pending, po, now_ms, try_window_ms, min_fill_ratio
                )
                if shortfall is not None:
                    elapsed_ms, fill_ratio = shortfall
                    cancel_issued = await self._cancel_pending_passive_order(
                        pending, entry_id, po, adapter, now_ms,
                        "maker_try_window_fill_ratio_below_threshold",
                    )
                    if cancel_issued:
                        self.journal.append(
                            "passive_maintenance.cancel_try_window",
                            {
                                "entry_id": entry_id, "symbol": pending.symbol,
                                "elapsed_ms": elapsed_ms,
                                "fill_ratio": round(fill_ratio, 4),
                                "try_window_ms": try_window_ms,
                                "min_fill_ratio": min_fill_ratio,
                            },
                        )
                        continue

            # --- Step 4: maker_entry_rest_timeout ---
            if (
                po.cancel_requested_at_ms <= 0
                and not po.maker_completed()
                and po.timed_out(now_ms)
            ):
                cancel_issued = await self._cancel_pending_passive_order(
                    pending, entry_id, po, adapter, now_ms,
                    "maker_entry_rest_timeout_exceeded",
                )
                if cancel_issued:
                    self.journal.append(
                        "passive_maintenance.cancel_rest_timeout",
                        {
                            "entry_id": entry_id, "symbol": pending.symbol,
                            "timeout_at_ms": po.timeout_at_ms,
                            "now_ms": now_ms,
                            "rest_timeout_ms": rest_timeout_ms,
                        },
                    )
                    continue

            # --- Step 5: Post-cancel terminal handling ---
            if po.cancel_requested() and po.maker_completed():
                cancel_elapsed = now_ms - po.cancel_requested_at_ms
                if not pending.has_any_fill():
                    removed = await self._abort_pending_entry_fail_closed(
                        pending, entry_id,
                        f"passive_maker_{po.last_progress_state.value}_zero_fill",
                    )
                    if removed:
                        resolved.append(entry_id)
                elif pending.missing_hedge_quantity() <= 1e-9:
                    await self._finalize_pending_entry(pending, entry_id, now_ms)
                    resolved.append(entry_id)
                elif cancel_elapsed > 30_000:
                    # Stale cancel with partial fill — force finalize what we have
                    await self._finalize_pending_entry(pending, entry_id, now_ms)
                    resolved.append(entry_id)
                else:
                    # Drive hedge for partial fill
                    hedge_driven = await self._drive_missing_hedge_live(
                        pending, entry_id, now_ms
                    )
                    if hedge_driven and pending.missing_hedge_quantity() <= 1e-9 and pending.maker_completed():
                        await self._finalize_pending_entry(pending, entry_id, now_ms)
                        resolved.append(entry_id)
                    else:
                        pending.next_progress_poll_ms = now_ms + poll_ms
            else:
                # Still resting — schedule next poll
                pending.next_progress_poll_ms = now_ms + poll_ms

        for eid in resolved:
            self.state.pending_entries.pop(eid, None)

    def _maker_try_window_fill_shortfall(
        self,
        pending,
        po,
        now_ms: int,
        try_window_ms: int,
        min_fill_ratio: float,
    ) -> Optional[tuple]:
        """V1: maker_try_window_fill_shortfall (entry_sync.rs:577-601).

        Only triggers for zero-fill orders.  Returns (elapsed_ms, fill_ratio)
        when the maker order has been resting beyond try_window_ms and the
        fill ratio is below min_fill_ratio.
        """
        if try_window_ms == 0:
            return None
        if pending.has_any_fill():
            return None
        if po.cancel_requested():
            return None
        if po.maker_completed():
            return None
        if po.accepted_at_ms <= 0:
            return None
        elapsed_ms = max(0, now_ms - po.accepted_at_ms)
        if elapsed_ms < try_window_ms:
            return None
        target = po.target_quantity
        if target <= 1e-9:
            return None
        fill_ratio = pending.maker_leg_filled / target
        if fill_ratio + 1e-9 >= min_fill_ratio:
            return None
        return (elapsed_ms, fill_ratio)

    async def _cancel_pending_passive_order(
        self,
        pending,
        entry_id: str,
        po,
        adapter,
        now_ms: int,
        reason: str,
    ) -> bool:
        """V1: cancel_pending_entry_passive_order (entry_sync.rs:2401-2445).

        1. Returns false if already canceled or maker completed
        2. Checks maker venue request budget
        3. Issues cancel_passive_order on the venue adapter
        4. Sets cancel_requested_at_ms and updates next_progress_poll_ms
        5. Returns true if cancel was successfully issued
        """
        if po.cancel_requested() or po.maker_completed():
            return False

        # Rate-limit gate
        maker_venue = pending.maker_venue()
        if not self._try_consume_maker_venue_budget(maker_venue, now_ms):
            pending.next_progress_poll_ms = (
                now_ms + self.config.strategy.maker_venue_budget_window_ms
            )
            self.journal.append(
                "passive_maintenance.cancel_budget_delayed",
                {"entry_id": entry_id, "venue": str(maker_venue),
                 "reason": reason},
            )
            return False

        try:
            await adapter.cancel_passive_order(
                symbol=pending.symbol,
                order_id=po.order_id,
                client_order_id=po.client_order_id or None,
            )
        except Exception as exc:
            self.journal.append(
                "passive_maintenance.cancel_error",
                {"entry_id": entry_id, "symbol": pending.symbol,
                 "venue": str(maker_venue), "error": str(exc)},
            )
            # V1: on cancel error, query progress to see if order is already
            # done — then apply terminal state if confirmed
            try:
                progress = await adapter.query_passive_order_progress(
                    symbol=pending.symbol,
                    order_id=po.order_id,
                    client_order_id=po.client_order_id or None,
                    side=pending.maker_side(),
                )
                if progress is not None and progress.state.is_terminal():
                    po.last_progress_state = progress.state
                    self.journal.append(
                        "passive_maintenance.cancel_error_resolved_via_progress",
                        {"entry_id": entry_id,
                         "resolved_state": progress.state.value},
                    )
                    # Continue to post-cancel handling in next cycle
                    pending.next_progress_poll_ms = now_ms + (
                        self.config.strategy.maker_entry_rest_timeout_ms or 6000
                    ) // 2
                    return False
            except Exception:
                pass
            pending.next_progress_poll_ms = now_ms + self._RECONCILE_RETRY_BASE_MS
            return False

        po.cancel_requested_at_ms = now_ms
        pending.next_progress_poll_ms = now_ms + self.config.strategy.maker_venue_budget_window_ms
        self.journal.append(
            "passive_maintenance.cancel_issued",
            {"entry_id": entry_id, "symbol": pending.symbol,
             "venue": str(maker_venue), "reason": reason,
             "cancel_requested_at_ms": now_ms},
        )
        return True

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

            # V1: force_terminalize_pending_entry_if_budget_exhausted()
            # runs before flat-position retention. Otherwise a zero-fill
            # maker_resting entry with both venues flat but missing maker
            # terminal evidence can be retained forever.
            if await self._force_terminalize_pending_entry_if_budget_exhausted(
                pending, entry_id, now_ms
            ):
                continue

            if result.is_flat:
                if not self._pending_entry_flat_clear_has_terminal_maker_evidence(
                    pending, result
                ):
                    self.journal.append(
                        "reconciliation.entry_flat_unresolved_maker_retained",
                        {
                            "entry_id": entry_id,
                            "symbol": pending.symbol,
                            "maker_status": self._pending_entry_reconcile_maker_status(
                                pending, result
                            ),
                            "reason": "flat_position_without_terminal_maker_order_evidence",
                        },
                    )
                    self._apply_reconcile_backoff(pending, now_ms)
                    continue
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
            if await self._force_terminalize_pending_entry_if_budget_exhausted(
                pending, entry_id, now_ms
            ):
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
            if await self._pending_entry_has_unresolved_maker_order(pending, entry_id):
                self.journal.append(
                    "reconciliation.entry_abandon_retained_unresolved_maker",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "reason": "both_venues_zero_but_maker_order_not_terminal",
                    },
                )
                return False
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

    @staticmethod
    def _pending_entry_reconcile_maker_status(pending, result) -> str:
        if getattr(pending, "maker_leg", "long") == "long":
            return str(getattr(result, "long_status", "") or "").lower()
        return str(getattr(result, "short_status", "") or "").lower()

    @staticmethod
    def _order_status_is_terminal_no_fill(status: str) -> bool:
        normalized = str(status or "").lower()
        return normalized in {
            "canceled",
            "cancelled",
            "expired",
            "rejected",
            "not_found",
            "missing",
            "notfound",
            "not_found_or_closed",
        }

    @staticmethod
    def _pending_entry_has_maker_order_reference(pending) -> bool:
        return bool(
            getattr(pending, "maker_order_id", "")
            or getattr(pending, "maker_client_order_id", "")
        )

    def _pending_entry_flat_clear_has_terminal_maker_evidence(self, pending, result) -> bool:
        if not self._pending_entry_has_maker_order_reference(pending):
            return True
        maker_status = self._pending_entry_reconcile_maker_status(pending, result)
        return self._order_status_is_terminal_no_fill(maker_status)

    async def _pending_entry_has_unresolved_maker_order(
        self, pending, entry_id: str
    ) -> bool:
        if not self._pending_entry_has_maker_order_reference(pending):
            return False
        if pending.maker_completed():
            return False

        adapter = self.get_venue_adapter(pending.maker_venue())
        if adapter is None:
            return True

        try:
            maker_side = getattr(pending, 'maker_side', None)
            if callable(maker_side):
                maker_side = maker_side()
            progress = await adapter.query_passive_order_progress(
                symbol=pending.symbol,
                order_id=getattr(pending, "maker_order_id", "") or "",
                client_order_id=getattr(pending, "maker_client_order_id", "") or None,
                side=maker_side if isinstance(maker_side, Side) else None,
            )
        except Exception as e:
            self.journal.append(
                "pending_entry.maker_terminal_evidence_unavailable",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "maker_venue": pending.maker_venue().value,
                    "error": str(e),
                },
            )
            return True

        if progress is None:
            self.journal.append(
                "pending_entry.maker_terminal_evidence_unavailable",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "maker_venue": pending.maker_venue().value,
                    "reason": "passive_order_progress_none",
                },
            )
            return True

        if getattr(progress, "cumulative_quantity", 0.0) > 1e-9:
            return True
        state = getattr(progress, "state", None)
        if state is not None and hasattr(state, "is_terminal"):
            if getattr(state, "value", "") == "filled":
                return True
            return not state.is_terminal()
        return True

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

    async def _force_terminalize_pending_entry_if_budget_exhausted(
        self, pending, entry_id: str, now_ms: int
    ) -> bool:
        """V1 force_terminalize_pending_entry_if_budget_exhausted.

        Runs before flat-position retention, matching V1's pending entry
        driver. A stale zero-fill maker order must first go through maker
        cancel and abort/cleanup once hard ceiling is reached; lack of maker
        terminal evidence must not retain it forever.

        Returns True when this pending entry was handled for this tick, even if
        cleanup failed and the entry was deliberately retained fail-closed.
        """
        budget = self._pending_entry_terminalization_budget(pending, now_ms)
        if budget is None:
            return False

        hard_ceiling_reached = bool(budget.get("hard_ceiling_reached"))
        force_terminal_reached = bool(budget.get("force_terminal_reached"))
        final_reason = str(budget["final_reason"])

        if hard_ceiling_reached and pending.repair_state:
            self.journal.append(
                "pending_entry.min_notional_hard_ceiling_cleanup",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "repair_state": pending.repair_state,
                    "final_reason": final_reason,
                    "lifetime_ms": budget["lifetime_ms"],
                },
            )

        if not pending.maker_completed():
            cancel_issued = False
            if getattr(pending, "maker_order_id", ""):
                cancel_issued = await self._recover_cancel_maker_order(
                    pending, entry_id, final_reason
                )

            if hard_ceiling_reached:
                if pending.has_any_fill() and pending.missing_hedge_quantity() <= 1e-9:
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
                    return True

                await self._abort_pending_entry(pending, entry_id, final_reason)
                return True

            if cancel_issued:
                pending.reconcile_attempt += 1
                self._apply_reconcile_backoff(pending, now_ms)
                return True

            return False

        if hard_ceiling_reached:
            if not pending.has_any_fill():
                if getattr(
                    self.config.strategy,
                    "pending_entry_force_fallback_when_tradeable",
                    False,
                ):
                    fallback_ok = await self._recover_try_taker_fallback(
                        pending, entry_id, final_reason
                    )
                    if fallback_ok:
                        return True
                abandoned = await self._try_abandon_stale_entry(pending, entry_id)
                if abandoned:
                    self.state.pending_entries.pop(entry_id, None)
                    return True

            await self._abort_pending_entry(pending, entry_id, final_reason)
            return True

        if force_terminal_reached:
            if await self._pending_entry_has_unresolved_maker_order(
                pending, entry_id
            ):
                self.journal.append(
                    "pending_entry.force_terminal_retained_unresolved_maker",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "reason": final_reason,
                        "lifetime_ms": budget["lifetime_ms"],
                    },
                )
                self._apply_reconcile_backoff(pending, now_ms)
                return True
            self.journal.append(
                "pending_entry.force_terminalized",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "reason": final_reason,
                    "lifetime_ms": budget["lifetime_ms"],
                },
            )
            self.state.pending_entries.pop(entry_id, None)
            return True

        return False

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
        """V1: flatten residual startup/recovery exposure on one venue.

        entry.rs:2711-2801, recovery.rs:1750-1870

        Returns:
          True: position was flattened (or was already zero)
          False: cleanup failed (position remains or can't verify)
          None: no adapter available (caller treats as uncertain — not success)
        """
        adapter = self.get_venue_adapter(venue)
        if adapter is None:
            return None

        from lightfee.venues.cid import generate_exchange_cid

        def cleanup_client_order_id_for_attempt(attempt: int) -> str:
            seed = f"{entry_id}:{stage}:{symbol}"
            if attempt > 1:
                seed = f"{seed}:attempt:{attempt}"
            return generate_exchange_cid(seed, "c", venue)

        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            cleanup_client_order_id = cleanup_client_order_id_for_attempt(attempt)
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

            event_kind = (
                "entry.cleanup_leg_exposure"
                if attempt == 1
                else "entry.cleanup_leg_exposure_retry"
            )
            self.journal.append(
                event_kind,
                {
                    "entry_id": entry_id,
                    "stage": stage,
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                    "venue": venue.value,
                    "symbol": symbol,
                    "size": pos.quantity,
                    "side": pos.side.value,
                    "cleanup_side": cleanup_side.value,
                    "cleanup_client_order_id": cleanup_client_order_id,
                    "client_order_id": cleanup_client_order_id,
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
                    time_in_force=TimeInForce.IOC,
                    client_order_id=cleanup_client_order_id,
                )
                fill = await adapter.place_order(req)
                self._flush_adapter_order_diagnostics(adapter)

                # V1: cleanup success needs EITHER fill covering target qty
                # OR verified-flat position after partial/ambiguous fill.
                target_qty = abs(pos.quantity)
                if fill.quantity >= target_qty - 1e-9:
                    return True

                try:
                    verify_pos = await adapter.fetch_position(symbol)
                    if verify_pos is None or abs(verify_pos.quantity) <= 1e-9:
                        return True
                except Exception:
                    pass
            except Exception as e:
                self._flush_adapter_order_diagnostics(adapter)
                is_bybit_duplicate = (
                    venue == Venue.BYBIT
                    and _is_bybit_duplicate_order_link_id(str(e))
                )
                if is_bybit_duplicate:
                    next_client_order_id = (
                        cleanup_client_order_id_for_attempt(attempt + 1)
                        if attempt < max_attempts
                        else ""
                    )
                    reconciled = await self._reconcile_bybit_duplicate_cleanup_order(
                        adapter=adapter,
                        symbol=symbol,
                        entry_id=entry_id,
                        stage=stage,
                        attempt=attempt,
                        max_attempts=max_attempts,
                        client_order_id=cleanup_client_order_id,
                        next_client_order_id=next_client_order_id,
                        target_qty=abs(pos.quantity),
                        live_pos_before=pos,
                        original_error=str(e),
                    )
                    if reconciled is True:
                        return True
                    if attempt >= max_attempts:
                        return False
                    self.journal.append(
                        "entry.cleanup_leg_exposure_retry_scheduled",
                        {
                            "entry_id": entry_id,
                            "stage": stage,
                            "next_attempt": attempt + 1,
                            "max_attempts": max_attempts,
                            "venue": venue.value,
                            "symbol": symbol,
                            "client_order_id": cleanup_client_order_id,
                            "next_client_order_id": next_client_order_id,
                            "reason": "duplicate_client_order_id_unresolved",
                        },
                    )
                    continue
                try:
                    verify_pos = await adapter.fetch_position(symbol)
                    if verify_pos is None or abs(verify_pos.quantity) <= 1e-9:
                        return True
                except Exception:
                    pass

            if attempt >= max_attempts:
                return False

            self.journal.append(
                "entry.cleanup_leg_exposure_retry_scheduled",
                {
                    "entry_id": entry_id,
                    "stage": stage,
                    "next_attempt": attempt + 1,
                    "max_attempts": max_attempts,
                    "venue": venue.value,
                    "symbol": symbol,
                },
            )

        return False

    async def _reconcile_bybit_duplicate_cleanup_order(
        self,
        *,
        adapter,
        symbol: str,
        entry_id: str,
        stage: str,
        attempt: int,
        max_attempts: int,
        client_order_id: str,
        next_client_order_id: str,
        target_qty: float,
        live_pos_before: PositionSnapshot,
        original_error: str,
    ) -> bool:
        """Reconcile Bybit duplicate cleanup order ids before retrying.

        This intentionally uses the same adapter.fetch_order_fill_reconciliation
        contract as passive close/close execution so Bybit endpoint semantics
        stay centralized in the venue adapter.
        """
        endpoints = [
            "bybit_order_realtime",
            "bybit_order_history",
            "bybit_execution_list",
        ]
        reconciliation = None
        reconcile_error = ""
        try:
            reconciliation = await adapter.fetch_order_fill_reconciliation(
                symbol, "", client_order_id,
            )
        except Exception as exc:
            reconcile_error = str(exc)

        recon_qty_raw = (
            getattr(reconciliation, "quantity", 0.0)
            if reconciliation is not None
            else 0.0
        )
        recon_qty = (
            float(recon_qty_raw)
            if isinstance(recon_qty_raw, (int, float))
            else 0.0
        )

        live_pos_after = None
        live_fetch_error = ""
        live_fetch_attempted = False
        if recon_qty < max(target_qty - 1e-9, 0.0):
            try:
                live_fetch_attempted = True
                live_pos_after = await adapter.fetch_position(symbol)
            except Exception as exc:
                live_fetch_error = str(exc)

        live_pos = (
            live_pos_after
            if live_fetch_attempted and not live_fetch_error
            else live_pos_before
        )
        live_qty = (
            abs(getattr(live_pos, "quantity", 0.0) or 0.0)
            if live_pos is not None
            else 0.0
        )
        live_side = getattr(getattr(live_pos, "side", None), "value", None)

        if recon_qty >= target_qty - 1e-9 and recon_qty > 0.0:
            decision = "filled"
            success = True
        elif live_fetch_attempted and not live_fetch_error and live_qty <= 1e-9:
            decision = "live_flat"
            success = True
        elif attempt >= max_attempts:
            decision = "failed_live_exposure_remaining"
            success = False
        else:
            decision = "retry_new_client_order_id"
            success = False

        payload = {
            "entry_id": entry_id,
            "stage": stage,
            "attempt": attempt,
            "max_attempts": max_attempts,
            "venue": Venue.BYBIT.value,
            "symbol": symbol,
            "client_order_id": client_order_id,
            "next_client_order_id": next_client_order_id,
            "reconcile_endpoints": endpoints,
            "reconciled_quantity": recon_qty,
            "target_quantity": target_qty,
            "order_id": (
                getattr(reconciliation, "order_id", "")
                if reconciliation is not None
                else ""
            ),
            "live_exposure": {
                "quantity": live_qty,
                "side": live_side,
            },
            "decision": decision,
            "original_error": original_error,
        }
        if reconcile_error:
            payload["reconcile_error"] = reconcile_error
        if live_fetch_error:
            payload["live_fetch_error"] = live_fetch_error

        self.journal.append(
            "entry.cleanup_duplicate_client_order_reconcile_result",
            payload,
        )
        return success

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
                if not self._pending_entry_flat_clear_has_terminal_maker_evidence(
                    pending, result
                ):
                    self.journal.append(
                        "recovery.force_reconcile_flat_unresolved_maker_retained",
                        {
                            "entry_id": entry_id,
                            "symbol": pending.symbol,
                            "maker_status": self._pending_entry_reconcile_maker_status(
                                pending, result
                            ),
                            "reason": "flat_position_without_terminal_maker_order_evidence",
                        },
                    )
                    continue
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
            await adapter.cancel_passive_order(
                symbol=pending.symbol,
                order_id=pending.maker_order_id,
                client_order_id=pending.maker_client_order_id or None,
            )
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
                time_in_force=TimeInForce.IOC,
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
            attempt = int(getattr(pending, "hedge_attempt_count", 0) or 0) + 1
            pending.hedge_attempt_count = attempt
            hedge_cloid = generate_exchange_cid(entry_id, f"h{attempt}", hedge_venue)
            pending.hedge_client_order_id = hedge_cloid
            pending.hedge_inflight = HedgeInflight(
                client_order_id=hedge_cloid,
                venue=hedge_venue,
                side=pending.hedge_side(),
                quantity=normalized,
                attempt=attempt,
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
                    "hedge_attempt": attempt,
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
                time_in_force=TimeInForce.IOC,
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
                        "hedge_attempt": attempt,
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
                    "hedge_attempt": attempt,
                    "order_id": getattr(fill, "order_id", ""),
                },
            )
            return False

        except OrderSubmitError as e:
            # V1: retain inflight on UNCERTAIN so reconciliation can query it;
            # only clear on REJECTED where we know the order never reached the exchange.
            submitted_inflight = pending.hedge_inflight
            try:
                reconciliation = await adapter.fetch_order_fill_reconciliation(
                    pending.symbol,
                    "",
                    hedge_cloid,
                )
            except Exception as reconcile_error:
                reconciliation = None
                self.journal.append(
                    "pending_entry.hedge_submit_reconcile_error",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "hedge_client_order_id": hedge_cloid,
                        "error": str(reconcile_error),
                    },
                )
            if reconciliation is not None and getattr(reconciliation, "quantity", 0.0) > 0:
                fill_qty = float(getattr(reconciliation, "quantity", 0.0) or 0.0)
                pending.hedge_leg_filled += fill_qty
                pending.hedge_order_id = getattr(reconciliation, "order_id", "") or ""
                pending.hedge_fill_price = float(
                    getattr(reconciliation, "average_price", 0.0)
                    or getattr(reconciliation, "price", 0.0)
                    or pending.hedge_fill_price
                    or 0.0
                )
                pending.hedge_inflight = None
                self._flush_adapter_order_diagnostics(adapter)
                self.journal.append(
                    "pending_entry.hedge_submit_result",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "outcome": "filled",
                        "reconciled": True,
                        "hedge_fill_quantity": fill_qty,
                        "hedge_fill_price": pending.hedge_fill_price,
                        "hedge_order_id": pending.hedge_order_id,
                        "hedge_client_order_id": hedge_cloid,
                        "hedge_attempt": attempt,
                        "hedge_leg_filled": pending.hedge_leg_filled,
                        "missing_hedge_remaining": pending.missing_hedge_quantity(),
                    },
                )
                if pending.missing_hedge_quantity() <= 1e-9:
                    pending.uncertain_outcome = False
                    pending.outcome = "filled"
                return True
            if (
                hedge_venue == Venue.HYPERLIQUID
                and is_hyperliquid_non_retryable_auth_signing_error(e)
            ):
                pending.hedge_inflight = None
                pending.repair_state = "non_retryable_auth_signing_failure"
                pending.uncertain_outcome = True
                enter_fail_closed(self.state)
                self.state.last_error = (
                    f"non_retryable_hyperliquid_auth_signing_failure:{entry_id}"
                )
                self._flush_adapter_order_diagnostics(adapter)
                self.journal.append(
                    "pending_entry.hedge_non_retryable_auth_signing_failure",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "hedge_venue": hedge_venue.value,
                        "hedge_client_order_id": (
                            submitted_inflight.client_order_id
                            if submitted_inflight
                            else hedge_cloid
                        ),
                        "hedge_attempt": attempt,
                        "error": str(e),
                        "reason": "non_retryable_auth_signing_failure",
                    },
                )
                return False
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
                    "hedge_attempt": attempt,
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

    async def _ensure_pending_entry_open_fill_details(
        self,
        pending,
        entry_id: str,
        now_ms: int,
    ) -> bool:
        """Gate entry.opened on confirmed price and order id for both legs."""

        async def _reconcile_leg(label: str, venue: Venue) -> None:
            adapter = self.get_venue_adapter(venue)
            if adapter is None:
                return
            order_id = getattr(pending, f"{label}_order_id", "") or ""
            client_order_id = getattr(pending, f"{label}_client_order_id", "") or ""
            if not order_id and not client_order_id:
                return
            try:
                reconciliation = await adapter.fetch_order_fill_reconciliation(
                    pending.symbol,
                    order_id,
                    client_order_id,
                )
            except Exception as exc:
                self.journal.append(
                    "pending_entry.finalize_fill_reconciliation_error",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "leg": label,
                        "venue": venue.value,
                        "order_id": order_id,
                        "client_order_id": client_order_id,
                        "error": str(exc),
                    },
                )
                return
            if reconciliation is None:
                return
            qty = float(getattr(reconciliation, "quantity", 0.0) or 0.0)
            avg_price = float(
                getattr(reconciliation, "average_price", 0.0)
                or getattr(reconciliation, "price", 0.0)
                or 0.0
            )
            reconciled_order_id = getattr(reconciliation, "order_id", "") or order_id
            before_qty = float(getattr(pending, f"{label}_leg_filled", 0.0) or 0.0)
            before_price = float(getattr(pending, f"{label}_fill_price", 0.0) or 0.0)
            before_order_id = getattr(pending, f"{label}_order_id", "") or ""
            if math.isfinite(qty) and qty >= 0:
                setattr(pending, f"{label}_leg_filled", qty)
            if avg_price > 0:
                setattr(pending, f"{label}_fill_price", avg_price)
            if reconciled_order_id:
                setattr(pending, f"{label}_order_id", reconciled_order_id)
            after_qty = float(getattr(pending, f"{label}_leg_filled", 0.0) or 0.0)
            after_price = float(getattr(pending, f"{label}_fill_price", 0.0) or 0.0)
            after_order_id = getattr(pending, f"{label}_order_id", "") or ""
            if (
                abs(after_qty - before_qty) > 1e-12
                or abs(after_price - before_price) > 1e-12
                or after_order_id != before_order_id
            ):
                self.journal.append(
                    "pending_entry.finalize_fill_reconciled",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "leg": label,
                        "venue": venue.value,
                        "before_quantity": before_qty,
                        "after_quantity": after_qty,
                        "before_price": before_price,
                        "after_price": after_price,
                        "before_order_id": before_order_id,
                        "after_order_id": after_order_id,
                    },
                )

        if (
            float(getattr(pending, "maker_leg_filled", 0.0) or 0.0) > 0.0
            or getattr(pending, "maker_order_id", "")
            or getattr(pending, "maker_client_order_id", "")
        ):
            await _reconcile_leg("maker", pending.maker_venue())
        if (
            float(getattr(pending, "hedge_leg_filled", 0.0) or 0.0) > 0.0
            or getattr(pending, "hedge_order_id", "")
            or getattr(pending, "hedge_client_order_id", "")
        ):
            await _reconcile_leg("hedge", pending.hedge_venue())

        balanced_quantity = min(
            float(getattr(pending, "maker_leg_filled", 0.0) or 0.0),
            float(getattr(pending, "hedge_leg_filled", 0.0) or 0.0),
        )
        if balanced_quantity <= 0.0:
            return True

        missing: list[str] = []
        if float(getattr(pending, "maker_fill_price", 0.0) or 0.0) <= 0.0:
            missing.append("maker_fill_price")
        if not getattr(pending, "maker_order_id", ""):
            missing.append("maker_order_id")
        if float(getattr(pending, "hedge_fill_price", 0.0) or 0.0) <= 0.0:
            missing.append("hedge_fill_price")
        if not getattr(pending, "hedge_order_id", ""):
            missing.append("hedge_order_id")

        if not missing:
            return True

        pending.uncertain_outcome = True
        pending.reconcile_next_attempt_ms = max(
            int(getattr(pending, "reconcile_next_attempt_ms", 0) or 0),
            now_ms + 1_000,
        )
        self.journal.append(
            "pending_entry.finalize_deferred_incomplete_fill",
            {
                "entry_id": entry_id,
                "symbol": pending.symbol,
                "missing_fields": missing,
                "maker_leg_filled": pending.maker_leg_filled,
                "hedge_leg_filled": pending.hedge_leg_filled,
                "maker_fill_price": pending.maker_fill_price,
                "hedge_fill_price": pending.hedge_fill_price,
                "maker_order_id": pending.maker_order_id,
                "hedge_order_id": pending.hedge_order_id,
            },
        )
        return False

    async def _finalize_pending_entry(self, pending, entry_id: str, now_ms: int) -> None:
        """Finalize a completed pending entry: build OpenPosition, write entry.opened.

        V1 parity gate (entry_sync.rs:5338-5454):
        1. Compute residual_task BEFORE the balanced_quantity branch (line 5338).
        2. balanced_quantity > 0: create OpenPosition, emit entry.opened; if residual
           exists → persist as "incremental_entry_open_partially_matched".
        3. balanced_quantity == 0 with residual (has_any_fill): persist as
           "incremental_entry_open_unmatched_residual", no open position.
        4. balanced_quantity == 0 with no fill (zero-fill): emit
           entry.passive_unfilled, remove pending.

        Zero-fill (maker=0, hedge=0) entries are safely removed as passive_unfilled.
        One-sided fill (maker>0, hedge=0) creates an unmatched residual task for
        cleanup but does NOT create an open position or emit entry.opened.
        """
        from lightfee.engine.entry import build_open_position, EntryContext, EntryType
        from lightfee.engine.residual import (
            split_entry_fill_residual,
            residual_pair_id,
        )

        maker_is_long = pending.maker_leg == "long"
        maker_side = Side.BUY if maker_is_long else Side.SELL

        if not await self._ensure_pending_entry_open_fill_details(
            pending,
            entry_id,
            now_ms,
        ):
            return

        # V1: build_residual_task is computed before branching, but only after
        # order/fill reconciliation has made pending quantities authoritative.
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

        pair_id = getattr(pending, "pair_id", "") or residual_pair_id(
            pending.symbol, pending.long_venue, pending.short_venue
        )
        residual_task = split_entry_fill_residual(
            position_id=entry_id,
            pair_id=pair_id,
            symbol=pending.symbol,
            long_venue=pending.long_venue,
            short_venue=pending.short_venue,
            long_fill=OrderFill(
                venue=pending.long_venue,
                symbol=pending.symbol,
                side=Side.BUY,
                quantity=pending.maker_leg_filled if maker_is_long else pending.hedge_leg_filled,
                price=pending.maker_fill_price if maker_is_long else pending.hedge_fill_price,
            ),
            short_fill=OrderFill(
                venue=pending.short_venue,
                symbol=pending.symbol,
                side=Side.SELL,
                quantity=pending.hedge_leg_filled if maker_is_long else pending.maker_leg_filled,
                price=pending.hedge_fill_price if maker_is_long else pending.maker_fill_price,
            ),
            created_cycle=getattr(self.state, "cycle", 0),
            now_ms=now_ms,
        )

        balanced_quantity = min(pending.maker_leg_filled, pending.hedge_leg_filled)
        balanced_quantity = max(balanced_quantity, 0.0)

        if balanced_quantity <= 0.0:
            if not pending.has_any_fill():
                # V1: !has_any_fill → zero-fill, safe to remove as passive_unfilled
                self.journal.append(
                    "entry.passive_unfilled",
                    {
                        "entry_id": entry_id,
                        "symbol": pending.symbol,
                        "pair_id": pair_id,
                        "maker_leg_filled": pending.maker_leg_filled,
                        "hedge_leg_filled": pending.hedge_leg_filled,
                        "balanced_quantity": balanced_quantity,
                        "reason": "zero_fill_unfilled_removal",
                    },
                )
                self.journal.append(
                    "pending_entry.pending_entry_finalized",
                    {
                        "entry_id": entry_id,
                        "position_id": None,
                        "maker_leg_filled": pending.maker_leg_filled,
                        "hedge_leg_filled": pending.hedge_leg_filled,
                        "maker_fill_price": pending.maker_fill_price,
                        "hedge_fill_price": pending.hedge_fill_price,
                        "finalized_as": "unfilled_zero_balanced",
                    },
                )
                self.state.pending_entries.pop(entry_id, None)
                return

            # V1: balanced_quantity == 0 but has_any_fill → one-sided exposure.
            # No open position, no entry.opened. Persist residual task if asymmetric.
            # entry_sync.rs:5436-5443: if let Some(task) = residual_task {
            #   persist_pending_residual_repair(task, "incremental_entry_open_unmatched_residual")
            # }
            if residual_task is not None:
                self._queue_pending_residual_repair(
                    residual_task,
                    "incremental_entry_open_unmatched_residual",
                )

            self.journal.append(
                "pending_entry.zero_balanced_with_fill_retained",
                {
                    "entry_id": entry_id,
                    "symbol": pending.symbol,
                    "pair_id": pair_id,
                    "maker_leg_filled": pending.maker_leg_filled,
                    "hedge_leg_filled": pending.hedge_leg_filled,
                    "balanced_quantity": balanced_quantity,
                    "reason": "one_sided_fill_retained_for_cleanup",
                },
            )
            self.journal.append(
                "pending_entry.pending_entry_finalized",
                {
                    "entry_id": entry_id,
                    "position_id": None,
                    "maker_leg_filled": pending.maker_leg_filled,
                    "hedge_leg_filled": pending.hedge_leg_filled,
                    "maker_fill_price": pending.maker_fill_price,
                    "hedge_fill_price": pending.hedge_fill_price,
                    "balanced_quantity": balanced_quantity,
                    "finalized_as": "unmatched_residual",
                },
            )
            self.state.pending_entries.pop(entry_id, None)
            return

        # --- balanced_quantity > 0: create OpenPosition and entry.opened ---
        maker_fill = OrderFill(
            venue=pending.maker_venue(),
            symbol=pending.symbol,
            side=maker_side,
            quantity=pending.maker_leg_filled,
            price=pending.maker_fill_price,
            order_id=pending.maker_order_id,
            filled_at_ms=now_ms,
        )
        hedge_fill = OrderFill(
            venue=pending.hedge_venue(),
            symbol=pending.symbol,
            side=pending.hedge_side(),
            quantity=pending.hedge_leg_filled,
            price=pending.hedge_fill_price,
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
                "balanced_quantity": balanced_quantity,
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
                "balanced_quantity": balanced_quantity,
            },
        )

        self.journal.append(
            "runtime.position_opened",
            {
                "position_id": position.position_id,
                "symbol": position.symbol,
            },
        )

        # V1: entry_sync.rs:5423-5430 — if residual exists for partially matched
        # fill (e.g. maker=10, hedge=8 → 8 balanced + 2 residual), persist it.
        if residual_task is not None:
            self._queue_pending_residual_repair(
                residual_task,
                "incremental_entry_open_partially_matched",
            )

    def _queue_pending_residual_repair(self, residual_task, reason: str) -> None:
        """Persist a residual repair task using the V1 runtime field contract."""
        from lightfee.engine.close_executor import _residual_task_to_dict

        task_dict = _residual_task_to_dict(residual_task)
        self.state.pending_residual_repairs = [
            task for task in self.state.pending_residual_repairs
            if not (
                isinstance(task, dict)
                and task.get("position_id") == task_dict["position_id"]
                and task.get("pair_id") == task_dict["pair_id"]
                and task.get("origin") == task_dict["origin"]
                and (task.get("repair_venue") or task.get("exposure_venue")) == task_dict["repair_venue"]
                and (task.get("repair_side") or task.get("exposure_side")) == task_dict["repair_side"]
            )
        ]
        self.state.pending_residual_repairs.append(task_dict)
        payload = dict(task_dict)
        payload["reason"] = reason
        self.journal.append("execution.residual_repair_queued", payload)

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
        """Process ready pending residual repair tasks during normal runtime."""
        if not self.state.pending_residual_repairs:
            return

        from lightfee.core.domain import OrderRequest
        from lightfee.venues.common import venue_reduce_only_close_exempts_min_notional
        from lightfee.venues.cid import compact_client_order_id

        repaired = 0
        for task in list(self.state.pending_residual_repairs):
            if not isinstance(task, dict):
                continue

            fields = self._pending_residual_repair_fields(task)
            if fields is None:
                self.journal.append(
                    "recovery.residual_repair_invalid_removed",
                    {"position_id": task.get("position_id", ""), "symbol": task.get("symbol", "")},
                )
                self.state.pending_residual_repairs.remove(task)
                continue

            repair_venue, repair_side, task_repair_quantity = fields
            position_id = task.get("position_id", "")
            pair_id = task.get("pair_id", "")
            symbol = task.get("symbol", "")

            if bool(task.get("local_entry_paused", False)):
                continue
            next_attempt_ms = int(task.get("next_attempt_ms", 0) or 0)
            if next_attempt_ms > 0 and now_ms < next_attempt_ms:
                continue

            adapter = self.get_venue_adapter(repair_venue)
            if adapter is None:
                if self._residual_repair_deadline_or_attempts_exhausted(task, now_ms):
                    self._pause_pending_residual_repair(task, now_ms)
                    continue
                self._reschedule_pending_residual_repair_task(task, now_ms, "adapter_missing")
                self.journal.append(
                    "recovery.residual_repair_failed",
                    {
                        "position_id": position_id,
                        "pair_id": pair_id,
                        "symbol": symbol,
                        "repair_venue": repair_venue.value,
                        "repair_side": repair_side.value,
                        "repair_quantity": task_repair_quantity,
                        "error": "adapter_missing",
                    },
                )
                continue

            try:
                live_position = await adapter.fetch_position(symbol)
            except Exception as e:
                if self._residual_repair_deadline_or_attempts_exhausted(task, now_ms):
                    self._pause_pending_residual_repair(task, now_ms)
                    continue
                self._reschedule_pending_residual_repair_task(task, now_ms, str(e))
                self.journal.append(
                    "recovery.residual_repair_failed",
                    {
                        "position_id": position_id,
                        "pair_id": pair_id,
                        "symbol": symbol,
                        "repair_venue": repair_venue.value,
                        "repair_side": repair_side.value,
                        "repair_quantity": task_repair_quantity,
                        "error": str(e),
                    },
                )
                continue

            baseline = self._residual_repair_baseline_size(task, repair_venue)
            live_size = self._signed_position_size(live_position)
            if repair_side == Side.SELL:
                live_excess_quantity = max(live_size - baseline, 0.0)
            else:
                live_excess_quantity = max(baseline - live_size, 0.0)

            if live_excess_quantity <= 1e-9:
                self.state.pending_residual_repairs.remove(task)
                self._release_residual_repair_pair_gate(pair_id, symbol)
                repaired += 1
                self.journal.append(
                    "execution.residual_repair_completed",
                    {
                        "position_id": position_id,
                        "pair_id": pair_id,
                        "symbol": symbol,
                        "origin": task.get("origin", ""),
                        "repair_venue": repair_venue.value,
                        "repair_side": repair_side.value,
                        "result": "already_flat",
                    },
                )
                continue

            if self._residual_repair_deadline_or_attempts_exhausted(task, now_ms):
                self._pause_pending_residual_repair(task, now_ms)
                continue

            repair_quantity = live_excess_quantity
            if hasattr(adapter, "normalize_quantity"):
                try:
                    repair_quantity = await adapter.normalize_quantity(symbol, repair_quantity)
                except Exception as e:
                    self._reschedule_pending_residual_repair_task(task, now_ms, str(e))
                    self.journal.append(
                        "recovery.residual_repair_failed",
                        {
                            "position_id": position_id,
                            "pair_id": pair_id,
                            "symbol": symbol,
                            "repair_venue": repair_venue.value,
                            "repair_side": repair_side.value,
                            "repair_quantity": live_excess_quantity,
                            "error": str(e),
                        },
                    )
                    continue
            if repair_quantity <= 1e-9:
                if repair_venue == Venue.OKX:
                    live_price = abs(float(getattr(live_position, "entry_price", 0.0) or 0.0))
                    self._terminalize_residual_repair_task(
                        task,
                        now_ms,
                        terminal_reason="exchange_min_quantity_dust",
                        repair_venue=repair_venue,
                        repair_side=repair_side,
                        repair_quantity=live_excess_quantity,
                        live_price=live_price,
                        min_notional=0.0,
                    )
                    continue
                self._reschedule_pending_residual_repair_task(
                    task, now_ms, "normalized_repair_quantity_zero"
                )
                self.journal.append(
                    "recovery.residual_repair_failed",
                    {
                        "position_id": position_id,
                        "pair_id": pair_id,
                        "symbol": symbol,
                        "repair_venue": repair_venue.value,
                        "repair_side": repair_side.value,
                        "repair_quantity": live_excess_quantity,
                        "error": "normalized_repair_quantity_zero",
                    },
                )
                continue

            min_notional = self._venue_min_notional(repair_venue, symbol)
            live_price = abs(float(getattr(live_position, "entry_price", 0.0) or 0.0))
            if (
                min_notional > 0
                and live_price > 0
                and repair_quantity * live_price + 1e-12 < min_notional
                and not venue_reduce_only_close_exempts_min_notional(repair_venue)
            ):
                self._terminalize_residual_repair_task(
                    task,
                    now_ms,
                    terminal_reason="exchange_min_notional_dust",
                    repair_venue=repair_venue,
                    repair_side=repair_side,
                    repair_quantity=repair_quantity,
                    live_price=live_price,
                    min_notional=min_notional,
                )
                continue

            req = OrderRequest(
                venue=repair_venue,
                symbol=symbol,
                side=repair_side,
                quantity=repair_quantity,
                price=None,
                post_only=False,
                reduce_only=True,
                time_in_force=TimeInForce.IOC,
                client_order_id=compact_client_order_id(position_id, "residual_repair"),
            )
            try:
                fill = await adapter.place_order(req)
                self._flush_adapter_order_diagnostics(adapter)
            except Exception as e:
                self._flush_adapter_order_diagnostics(adapter)
                self._reschedule_pending_residual_repair_task(task, now_ms, str(e))
                self.journal.append(
                    "recovery.residual_repair_failed",
                    {
                        "position_id": position_id,
                        "pair_id": pair_id,
                        "symbol": symbol,
                        "repair_venue": repair_venue.value,
                        "repair_side": repair_side.value,
                        "repair_quantity": repair_quantity,
                        "error": str(e),
                    },
                )
                continue

            remaining_quantity = max(live_excess_quantity - float(fill.quantity or 0.0), 0.0)
            self.state.pending_residual_repairs.remove(task)
            if remaining_quantity > 1e-9:
                updated = dict(task)
                updated["repair_venue"] = repair_venue.value
                updated["repair_side"] = repair_side.value
                updated["repair_quantity"] = remaining_quantity
                updated.pop("exposure_venue", None)
                updated.pop("exposure_side", None)
                updated.pop("exposure_quantity", None)
                updated["retry_count"] = 0
                updated["last_attempt_at_ms"] = now_ms
                updated["next_attempt_ms"] = now_ms
                self.state.pending_residual_repairs.append(updated)
            else:
                self._release_residual_repair_pair_gate(pair_id, symbol)
                repaired += 1
            self.journal.append(
                "execution.residual_repair_completed",
                {
                    "position_id": position_id,
                    "pair_id": pair_id,
                    "symbol": symbol,
                    "origin": task.get("origin", ""),
                    "repair_venue": repair_venue.value,
                    "repair_side": repair_side.value,
                    "requested_quantity": repair_quantity,
                    "filled_quantity": float(fill.quantity or 0.0),
                    "remaining_quantity": remaining_quantity,
                },
            )

        if repaired > 0:
            self.journal.append(
                "recovery.residual_repairs_complete",
                {"repaired": repaired, "ts_ms": now_ms},
            )

    def _pending_residual_repair_fields(self, task: dict) -> tuple[Venue, Side, float] | None:
        venue_raw = task.get("repair_venue") or task.get("exposure_venue")
        side_raw = task.get("repair_side") or task.get("exposure_side")
        quantity_raw = task.get("repair_quantity", task.get("exposure_quantity", 0.0))
        if venue_raw is None or side_raw is None:
            return None
        try:
            repair_venue = Venue.from_str(str(venue_raw))
            repair_side = Side(str(side_raw).strip().lower())
            repair_quantity = float(quantity_raw or 0.0)
        except Exception:
            return None
        if repair_quantity <= 1e-9:
            return None
        return repair_venue, repair_side, repair_quantity

    def _signed_position_size(self, position: PositionSnapshot | None) -> float:
        if position is None:
            return 0.0
        quantity = abs(float(position.quantity or 0.0))
        return quantity if position.side == Side.BUY else -quantity

    def _residual_repair_baseline_size(self, task: dict, repair_venue: Venue) -> float:
        position_id = task.get("position_id", "")
        position = self.state.open_positions.get(position_id)
        if position is None:
            return 0.0
        matched_quantity = float(
            position.matched_quantity
            or min(position.long_quantity, position.short_quantity)
            or 0.0
        )
        if repair_venue == position.long_venue:
            return matched_quantity
        if repair_venue == position.short_venue:
            return -matched_quantity
        return 0.0

    @staticmethod
    def _residual_repair_retry_delay_ms(attempt_count: int) -> int:
        attempt = max(int(attempt_count or 0), 1)
        return min(1_000 * (2 ** (attempt - 1)), 30_000)

    @staticmethod
    def _residual_repair_attempt_count(task: dict) -> int:
        return int(task.get("retry_count", task.get("attempt_count", 0)) or 0)

    def _residual_repair_deadline_or_attempts_exhausted(
        self, task: dict, now_ms: int,
    ) -> bool:
        deadline_ms = int(task.get("deadline_ms", 0) or 0)
        attempts = self._residual_repair_attempt_count(task)
        return (deadline_ms > 0 and now_ms >= deadline_ms) or attempts >= 3

    def _pause_pending_residual_repair(self, task: dict, now_ms: int) -> None:
        task["local_entry_paused"] = True
        task["last_attempt_at_ms"] = now_ms
        task["next_attempt_ms"] = 0
        task["last_error"] = "residual_repair_deadline_or_attempts_exhausted"
        self.journal.append(
            "execution.residual_repair_paused",
            {
                "position_id": task.get("position_id", ""),
                "pair_id": task.get("pair_id", ""),
                "symbol": task.get("symbol", ""),
                "repair_venue": task.get("repair_venue", task.get("exposure_venue", "")),
                "repair_side": task.get("repair_side", task.get("exposure_side", "")),
                "retry_count": self._residual_repair_attempt_count(task),
                "deadline_ms": int(task.get("deadline_ms", 0) or 0),
                "ts_ms": now_ms,
                "last_error": task["last_error"],
            },
        )

    def _release_residual_repair_pair_gate(self, pair_id: str, symbol: str) -> None:
        if not getattr(self.state, "live_recovery_reduce_only_pairs", None):
            return
        kept = []
        for item in self.state.live_recovery_reduce_only_pairs:
            item_pair_id = ""
            item_symbol = ""
            if isinstance(item, dict):
                item_pair_id = str(item.get("pair_id", ""))
                item_symbol = str(item.get("symbol", ""))
            else:
                item_pair_id = str(getattr(item, "pair_id", ""))
                item_symbol = str(getattr(item, "symbol", ""))
            if pair_id and item_pair_id == pair_id:
                continue
            if not pair_id and symbol and item_symbol == symbol:
                continue
            kept.append(item)
        self.state.live_recovery_reduce_only_pairs = kept

    def _terminalize_residual_repair_task(
        self,
        task: dict,
        now_ms: int,
        *,
        terminal_reason: str,
        repair_venue: Venue,
        repair_side: Side,
        repair_quantity: float,
        live_price: float,
        min_notional: float,
    ) -> None:
        try:
            self.state.pending_residual_repairs.remove(task)
        except ValueError:
            pass
        pair_id = str(task.get("pair_id", ""))
        symbol = str(task.get("symbol", ""))
        self._release_residual_repair_pair_gate(pair_id, symbol)
        self.journal.append(
            "execution.residual_repair_terminal",
            {
                "position_id": task.get("position_id", ""),
                "pair_id": pair_id,
                "symbol": symbol,
                "origin": task.get("origin", ""),
                "repair_venue": repair_venue.value,
                "repair_side": repair_side.value,
                "repair_quantity": repair_quantity,
                "live_price": live_price,
                "notional": repair_quantity * live_price,
                "min_notional": min_notional,
                "terminal_reason": terminal_reason,
                "ts_ms": now_ms,
            },
        )

    def _reschedule_pending_residual_repair_task(
        self, task: dict, now_ms: int, error: str
    ) -> None:
        retry_count = self._residual_repair_attempt_count(task) + 1
        task["retry_count"] = retry_count
        task["attempt_count"] = retry_count
        task["last_attempt_at_ms"] = now_ms
        task["last_error"] = error
        if self._residual_repair_deadline_or_attempts_exhausted(task, now_ms):
            self._pause_pending_residual_repair(task, now_ms)
            return
        task["next_attempt_ms"] = now_ms + self._residual_repair_retry_delay_ms(retry_count)

    # ------------------------------------------------------------------
    # Housekeeping
    # ------------------------------------------------------------------

    def _ensure_private_ws_started(self, now_ms: int) -> None:
        """V1: start private WS workers for live adapters when credentials/symbols ready.

        Called each tick until all live adapters with private health support have
        workers running. Tracked symbol changes trigger worker replacement.
        Idempotent: skips venues that already have workers for the same symbol set.
        """
        if self.config.runtime.mode == "paper":
            return

        tracked_symbols = self._current_tracked_private_symbols()
        for venue, adapter in self._venue_adapters.items():
            if not getattr(adapter, 'supports_private_health', False):
                continue

            transport = getattr(adapter, '_transport', None)
            if transport is None:
                continue

            symbols = tracked_symbols.get(venue, set())
            prev_symbols = self._private_ws_symbols.get(venue, set())

            # V1: empty symbols → stop any existing workers, clear tracking
            if not symbols:
                if prev_symbols:
                    transport.stop_private_ws()
                    self._private_ws_started.discard(venue)
                    self._private_ws_symbols.pop(venue, None)
                    self.journal.append(
                        "runtime.private_ws_stopped",
                        {
                            "venue": venue.value,
                            "reason": "no tracked symbols",
                        },
                    )
                continue

            # Start if never started or symbols changed
            if venue not in self._private_ws_started or symbols != prev_symbols:
                if symbols != prev_symbols and venue in self._private_ws_started:
                    # V1: worker replacement on symbol change
                    transport.stop_private_ws()

                transport.start_private_ws(list(symbols))
                self._private_ws_started.add(venue)
                self._private_ws_symbols[venue] = set(symbols)
                self.journal.append(
                    "runtime.private_ws_started",
                    {
                        "venue": venue.value,
                        "symbol_count": len(symbols),
                    },
                )

    def _current_tracked_private_symbols(self) -> dict[Venue, set[str]]:
        """Collect symbols that need private WS tracking from current state.

        V1: symbols from primary tracked entry pairs, open positions, and
        pending passive closes.
        """
        result: dict[Venue, set[str]] = {}

        # from open positions — use long/short venue + symbol if present
        for pos in self.state.open_positions.values():
            sym = getattr(pos, 'symbol', '')
            long_v = getattr(pos, 'long_venue', None)
            short_v = getattr(pos, 'short_venue', None)
            if sym:
                if long_v is not None and isinstance(long_v, Venue):
                    result.setdefault(long_v, set()).add(sym)
                if short_v is not None and isinstance(short_v, Venue):
                    result.setdefault(short_v, set()).add(sym)

        # from tracked entry pairs (V1: symbols tracked for entry)
        # pair_id format: "{symbol.lower()}:{long_venue}->{short_venue}"
        # (see entry_local_l2.py:make_candidate_pair_id)
        # IMPORTANT: make_candidate_pair_id() lowercases the symbol for stable
        # identity, so we must canonicalize it back to V2 internal uppercase
        # (e.g. "ethusdt" → "ETHUSDT") before passing to venue private WS.
        for pair_id in getattr(self, '_tracked_primary_pair_ids', set()):
            if not pair_id:
                continue
            # Try canonical format first: "sym:long->short"
            sym = ""
            long_v = None
            short_v = None
            if "->" in pair_id:
                try:
                    before_arrow, short_str = pair_id.rsplit("->", 1)
                    sym, long_str = before_arrow.split(":", 1)
                    sym = sym.upper()  # canonical V2 symbol (was lowercased by make_candidate_pair_id)
                    long_v = Venue(long_str)
                    short_v = Venue(short_str)
                except (ValueError, KeyError):
                    pass
            # Fallback: pipe-delimited format (backward compat / tests)
            if long_v is None:
                parts = pair_id.split("|")
                if len(parts) >= 3:
                    sym = parts[0].upper()  # canonical V2 symbol
                    try:
                        long_v = Venue(parts[1])
                        short_v = Venue(parts[2])
                    except ValueError:
                        continue
            if long_v is not None and short_v is not None and sym:
                result.setdefault(long_v, set()).add(sym)
                result.setdefault(short_v, set()).add(sym)

        # from pending entries (entries being executed that haven't opened yet)
        for entry in getattr(self.state, 'pending_entries', {}).values():
            sym = getattr(entry, 'symbol', '')
            long_v = getattr(entry, 'long_venue', None)
            short_v = getattr(entry, 'short_venue', None)
            if sym:
                if long_v is not None and isinstance(long_v, Venue):
                    result.setdefault(long_v, set()).add(sym)
                if short_v is not None and isinstance(short_v, Venue):
                    result.setdefault(short_v, set()).add(sym)

        # from pending passive closes (maker legs need private WS for progress)
        for pclose in getattr(self.state, 'pending_passive_closes', {}).values():
            pos = getattr(pclose, 'position_snapshot', None)
            # V1: when position_snapshot is not set, try to resolve from open_positions
            if pos is None:
                pid = getattr(pclose, 'position_id', '')
                if pid:
                    pos = self.state.open_positions.get(pid)
            if pos is not None:
                sym = getattr(pos, 'symbol', '')
                long_v = getattr(pos, 'long_venue', None)
                short_v = getattr(pos, 'short_venue', None)
                if sym:
                    if long_v is not None and isinstance(long_v, Venue):
                        result.setdefault(long_v, set()).add(sym)
                    if short_v is not None and isinstance(short_v, Venue):
                        result.setdefault(short_v, set()).add(sym)

        # from pending residual repairs — repair venue must be privately tracked
        # while the task is pending so live excess can converge without restart.
        for task in getattr(self.state, "pending_residual_repairs", []):
            if not isinstance(task, dict):
                continue
            sym = str(task.get("symbol", "") or "")
            venue_raw = task.get("repair_venue") or task.get("exposure_venue")
            if not sym or venue_raw is None:
                continue
            try:
                venue = Venue.from_str(str(venue_raw))
            except Exception:
                continue
            result.setdefault(venue, set()).add(sym)

        return result

    async def _post_tick_housekeeping(self, now_ms: int) -> None:
        """Run after every tick cycle: supervisor, reconciliation, periodic exports."""
        # V1 latch parity: a fail-closed state with no operator override,
        # no recovery block, and no recovery work is stale even after live
        # entry/recovery cleanup, not only during startup snapshot recovery.
        clear_stale_recovery_block_if_recovery_clean(self.state, self.journal)
        clear_stale_fail_closed_if_recovery_clean(self.state, self.journal)

        # V1: ensure private WS workers are running for live adapters
        self._ensure_private_ws_started(now_ms)

        # Risk-line supervision — V1: refresh_venue_health_supervisor + recompute_global_risk_mode
        # CRITICAL: risk_snapshot_cache must be injected BEFORE supervise() so
        # _collect_venue_health_views() sees current-tick AccountRiskSnapshot data.
        # If the cache is stale/empty, supervisor misdiagnoses risk_snapshot_unavailable
        # and enters fail-closed despite healthy venues.
        self.supervisor.supervise(
            now_ms,
            self.state.venue_health,
            adapters=self._venue_adapters,
            risk_snapshot_cache=self._risk_snapshot_cache,
        )

        # Reconciliation of pending/uncertain outcomes
        await self._reconcile_pending_state(now_ms)

        # V1: residual repairs are normal runtime work, not startup-only work.
        await self._recover_residual_repairs(now_ms)

        # Detect false-clean state where exchanges hold positions but V2 missed them.
        await self._maybe_recover_clean_live_positions(now_ms)
        clear_stale_recovery_block_if_recovery_clean(self.state, self.journal)

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
        if self.state.lifecycle == EngineLifecycle.RISK_ONLY:
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
        symbol = getattr(candidate, "symbol", "")
        for venue in (getattr(candidate, "long_venue", ""), getattr(candidate, "short_venue", "")):
            if not venue:
                continue
            until = self._post_only_reject_cooldown_until_ms.get((symbol, venue), 0)
            if until > 0 and now_ms < until:
                return False, f"post_only_reject_cooldown_{venue}"
        return True, ""

    @staticmethod
    def _entry_reject_is_post_only_would_take(reason: str) -> bool:
        text = str(reason or "").lower()
        return (
            "-5022" in text
            or "could not be executed as maker" in text
            or "post only order will be rejected" in text
            or "gtx_order_reject" in text
            or "post_only_would_take" in text
        )

    def _record_post_only_reject_cooldown(
        self,
        candidate,
        now_ms: int,
        reason: str,
        *,
        venue: str = "",
        side: str = "",
        price: float = 0.0,
        bbo: dict | None = None,
    ) -> None:
        cooldown_ms = int(
            getattr(
                self.config.strategy,
                "pending_entry_zero_fill_terminal_cooldown_ms",
                30_000,
            )
            or 30_000
        )
        pair_key = (
            getattr(candidate, "symbol", ""),
            getattr(candidate, "long_venue", ""),
            getattr(candidate, "short_venue", ""),
        )
        until_ms = now_ms + cooldown_ms
        self._zero_fill_cooldown_until_ms[pair_key] = until_ms
        venue = venue or pair_key[1]
        if venue:
            self._post_only_reject_cooldown_until_ms[(pair_key[0], venue)] = until_ms
        bbo_payload = dict(bbo or {})
        self.journal.append(
            "runtime.entry_post_only_reject_cooldown",
            {
                "symbol": pair_key[0],
                "venue": venue,
                "long_venue": pair_key[1],
                "short_venue": pair_key[2],
                "side": side or bbo_payload.get("side", ""),
                "price": price or bbo_payload.get("price", 0.0),
                "best_bid": bbo_payload.get("best_bid"),
                "best_ask": bbo_payload.get("best_ask"),
                "book_age_ms": bbo_payload.get("book_age_ms"),
                "stale_after_ms": bbo_payload.get("stale_after_ms"),
                "freshness": bbo_payload.get("freshness", "unknown"),
                "would_cross": bbo_payload.get("would_cross", False),
                "cooldown_until_ms": until_ms,
                "cooldown_until": until_ms,
                "cooldown_ms": cooldown_ms,
                "reason": reason[:300],
            },
        )

    def _post_only_maker_bbo_guard(
        self,
        *,
        venue: Venue,
        symbol: str,
        side: Side,
        price: float,
        now_ms: int,
    ) -> tuple[bool, str, dict]:
        venue_str = venue.value if hasattr(venue, "value") else str(venue)
        side_str = side.value if hasattr(side, "value") else str(side)
        stale_after_ms = self._entry_local_l2_stale_after_ms()
        payload = {
            "venue": venue_str,
            "symbol": symbol,
            "side": side_str,
            "price": price,
            "best_bid": None,
            "best_ask": None,
            "book_age_ms": None,
            "stale_after_ms": stale_after_ms,
            "freshness": "not_checked_local_l2_disabled",
            "would_cross": False,
        }
        if not self.config.strategy.local_l2_enabled:
            return True, "", payload

        book = self.local_l2_runtime.get_book(venue_str, symbol)
        if book is None:
            payload["freshness"] = "missing"
            return False, "missing_bbo", payload

        try:
            best_bid = float(book.best_bid())
            best_ask = float(book.best_ask())
        except Exception:
            best_bid = 0.0
            best_ask = 0.0
        try:
            age_ms = int(book.age_ms(now_ms))
        except Exception:
            observed = int(getattr(book, "observed_at_ms", 0) or 0)
            age_ms = now_ms - observed if observed > 0 else 0

        status = getattr(getattr(book, "status", None), "value", str(getattr(book, "status", "")))
        try:
            stale = bool(book.is_stale(stale_after_ms, now_ms))
        except Exception:
            stale = age_ms > stale_after_ms
        fresh = status == "hot" and not stale
        valid_bbo = best_bid > 0.0 and best_ask > best_bid
        would_cross = (
            valid_bbo
            and price > 0.0
            and (
                (side == Side.BUY and price >= best_ask)
                or (side == Side.SELL and price <= best_bid)
            )
        )

        payload.update(
            {
                "best_bid": best_bid,
                "best_ask": best_ask,
                "book_age_ms": age_ms,
                "freshness": "fresh" if fresh else "stale",
                "would_cross": would_cross,
            }
        )
        if not valid_bbo:
            payload["freshness"] = "invalid_bbo"
            return False, "invalid_bbo", payload
        if not fresh:
            return False, "stale_bbo", payload
        if would_cross:
            return False, "would_cross_bbo", payload
        return True, "", payload

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

    def _snapshot_domain_budget_ms(self, domain: str) -> int:
        domain_s = str(domain or "").lower()
        if domain_s == "liquidity":
            return int(
                getattr(
                    self.config.runtime,
                    "sidecar_perp_liquidity_budget_ms",
                    self.config.strategy.max_liquidity_snapshot_age_ms,
                )
                or self.config.strategy.max_liquidity_snapshot_age_ms
            )
        if domain_s == "quote":
            return int(
                getattr(self.config.runtime, "max_order_quote_age_ms", 0)
                or self.config.runtime.max_market_age_ms
                or self.config.runtime.sidecar_snapshot_max_age_ms
            )
        if domain_s == "market":
            return int(
                getattr(self.config.runtime, "max_market_age_ms", 0)
                or self.config.runtime.sidecar_snapshot_max_age_ms
            )
        if domain_s == "funding":
            return int(self.config.runtime.sidecar_snapshot_max_age_ms)
        return int(self.config.runtime.sidecar_snapshot_max_age_ms)

    @staticmethod
    def _snapshot_metric_key(venue: str, symbol: str, domain: str) -> str:
        return f"{str(venue).lower()}|{str(symbol).upper()}|{str(domain).lower()}"

    @staticmethod
    def _record_snapshot_metric(metrics: dict, key: str, fresh: bool) -> None:
        row = metrics.setdefault(key, {"fresh": 0, "stale": 0})
        row["fresh" if fresh else "stale"] = int(row.get("fresh" if fresh else "stale", 0)) + 1

    def _snapshot_fallback_source(self, snapshot) -> str:
        source = str(getattr(snapshot, "acquisition_mode", "") or "")
        return source or "fresh_sidecar"

    def _snapshot_quote_observed_at_ms(self, snapshot, quote) -> int:
        return (
            int(getattr(quote, "observed_at_ms", 0) or 0)
            or int(getattr(snapshot, "market_observed_at_ms", 0) or 0)
            or int(getattr(snapshot, "published_at_ms", 0) or 0)
        )

    def _snapshot_lifecycle_rows_by_venue(self, snapshot, domain: str) -> dict[str, object]:
        attr = {
            "funding": "funding_lifecycle",
            "market": "market_lifecycle",
            "liquidity": "liquidity_lifecycle",
        }.get(domain)
        if not attr:
            return {}
        rows = getattr(snapshot, attr, []) or []
        result: dict[str, object] = {}
        for row in rows:
            venue = str(getattr(row, "venue", "") or "").lower()
            if venue:
                result[venue] = row
        return result

    def _snapshot_freshness_observability(
        self,
        *,
        snapshot,
        candidates: list,
        now_ms: int,
    ) -> tuple[dict[str, dict[str, int]], dict[str, int]]:
        metrics: dict[str, dict[str, int]] = {}
        ages: dict[str, int] = {}
        if snapshot is None:
            return metrics, ages

        for quote in getattr(snapshot, "quotes", {}).values():
            venue = str(getattr(quote, "venue", "") or "").lower()
            symbol = str(getattr(quote, "symbol", "") or "").upper()
            if not venue or not symbol:
                continue
            observed_at_ms = self._snapshot_quote_observed_at_ms(snapshot, quote)
            age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
            budget_ms = self._snapshot_domain_budget_ms("quote")
            key = self._snapshot_metric_key(venue, symbol, "quote")
            self._record_snapshot_metric(metrics, key, observed_at_ms > 0 and age_ms <= budget_ms)
            ages[key] = age_ms

        lifecycle_by_domain = {
            "market": self._snapshot_lifecycle_rows_by_venue(snapshot, "market"),
            "funding": self._snapshot_lifecycle_rows_by_venue(snapshot, "funding"),
            "liquidity": self._snapshot_lifecycle_rows_by_venue(snapshot, "liquidity"),
        }
        seen: set[tuple[str, str, str]] = set()
        for candidate in candidates:
            symbol = str(getattr(candidate, "symbol", "") or "").upper()
            for venue_attr in ("long_venue", "short_venue"):
                venue = str(getattr(candidate, venue_attr, "") or "").lower()
                if not venue or not symbol:
                    continue
                for domain, rows in lifecycle_by_domain.items():
                    row = rows.get(venue)
                    if row is None:
                        continue
                    marker = (venue, symbol, domain)
                    if marker in seen:
                        continue
                    seen.add(marker)
                    observed_at_ms = int(getattr(row, "observed_at_ms", 0) or 0)
                    age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
                    budget_ms = self._snapshot_domain_budget_ms(domain)
                    key = self._snapshot_metric_key(venue, symbol, domain)
                    self._record_snapshot_metric(
                        metrics,
                        key,
                        observed_at_ms > 0 and age_ms <= budget_ms,
                    )
                    ages[key] = age_ms

        return metrics, ages

    def _candidate_snapshot_freshness_failures(
        self,
        candidate,
        *,
        snapshot,
        now_ms: int,
    ) -> list[dict]:
        if snapshot is None:
            return []
        quote_lookup = self._market_quote_lookup(getattr(snapshot, "quotes", {}) or {})
        liquidity_rows = self._snapshot_lifecycle_rows_by_venue(snapshot, "liquidity")
        fallback_source = self._snapshot_fallback_source(snapshot)
        failures: list[dict] = []
        symbol = str(getattr(candidate, "symbol", "") or "").upper()

        for venue_attr in ("long_venue", "short_venue"):
            venue = str(getattr(candidate, venue_attr, "") or "").lower()
            if not venue or not symbol:
                continue

            quote = quote_lookup.get((venue, symbol))
            quote_budget_ms = self._snapshot_domain_budget_ms("quote")
            if quote is None:
                failures.append({
                    "venue": venue,
                    "symbol": symbol,
                    "domain": "quote",
                    "age_ms": 0,
                    "budget_ms": quote_budget_ms,
                    "decision": "skip_entry",
                    "fallback_source": fallback_source,
                    "reason": "missing_quote",
                })
            else:
                observed_at_ms = self._snapshot_quote_observed_at_ms(snapshot, quote)
                age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
                bid = float(getattr(quote, "bid", 0.0) or 0.0)
                ask = float(getattr(quote, "ask", 0.0) or 0.0)
                if observed_at_ms <= 0 or age_ms > quote_budget_ms or bid <= 0.0 or ask <= 0.0:
                    failures.append({
                        "venue": venue,
                        "symbol": symbol,
                        "domain": "quote",
                        "age_ms": age_ms,
                        "budget_ms": quote_budget_ms,
                        "decision": "skip_entry",
                        "fallback_source": fallback_source,
                        "reason": "stale_quote" if age_ms > quote_budget_ms else "invalid_quote",
                    })

            liquidity = liquidity_rows.get(venue)
            if liquidity is not None:
                liq_budget_ms = self._snapshot_domain_budget_ms("liquidity")
                observed_at_ms = int(getattr(liquidity, "observed_at_ms", 0) or 0)
                age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
                if observed_at_ms <= 0 or age_ms > liq_budget_ms:
                    failures.append({
                        "venue": venue,
                        "symbol": symbol,
                        "domain": "liquidity",
                        "age_ms": age_ms,
                        "budget_ms": liq_budget_ms,
                        "decision": "skip_entry",
                        "fallback_source": fallback_source,
                        "reason": "stale_liquidity",
                    })

        return failures

    def _filter_candidates_by_snapshot_freshness(
        self,
        candidates: list,
        *,
        snapshot,
        now_ms: int,
        metrics: dict,
        ages: dict,
    ) -> list:
        filtered = []
        for candidate in candidates:
            failures = self._candidate_snapshot_freshness_failures(
                candidate,
                snapshot=snapshot,
                now_ms=now_ms,
            )
            if not failures:
                filtered.append(candidate)
                continue
            for failure in failures:
                key = self._snapshot_metric_key(
                    failure["venue"],
                    failure["symbol"],
                    failure["domain"],
                )
                if key not in metrics:
                    self._record_snapshot_metric(metrics, key, False)
                ages[key] = int(failure.get("age_ms", 0) or 0)
                payload = dict(failure)
                payload["ts_ms"] = now_ms
                payload["pair_id"] = self._candidate_pair_id(candidate)
                self.journal.append("runtime.snapshot_freshness_decision", payload)
        return filtered

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
        for venue_raw in (
            getattr(candidate, "long_venue", ""),
            getattr(candidate, "short_venue", ""),
        ):
            try:
                venue = Venue.from_str(str(venue_raw)) if venue_raw else None
            except Exception:
                venue = None
            if venue is None:
                continue
            adapter = self.get_venue_adapter(venue)
            transport = getattr(adapter, "_transport", adapter)
            trusted = getattr(transport, "trading_capability_trusted", True)
            if trusted is False:
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

    def _apply_shadow_promotion_if_eligible(
        self, tracked: list, now_ms: int,
    ) -> None:
        """V1: shadow_promotion swap — best shadow replaces worst primary.

        Rejects promotion when primary is executing, shadow not ready,
        score delta insufficient, or hold window not elapsed.
        Logs primary_hold_blocked when score qualifies but hold blocks.
        (execution_core/engine.rs:2643-2719)
        """
        if not tracked:
            return

        from lightfee.engine.entry_local_l2 import (
            TrackedOpportunityClass,
            primary_hold_window_allows_replacement,
            shadow_promotion_is_eligible,
        )

        tracked_lookup = {t.pair_id: t for t in tracked}
        primaries = [t for t in tracked if t.class_ == TrackedOpportunityClass.PRIMARY]
        shadows = [t for t in tracked if t.class_ == TrackedOpportunityClass.SHADOW]

        if not primaries or not shadows:
            return

        score_delta_bps = getattr(
            self.config.strategy,
            "shadow_promotion_score_delta_bps",
            5.0,
        )
        primary_min_hold_ms = getattr(
            self.config.strategy, "primary_min_hold_ms", 30_000,
        )

        best_shadow = max(shadows, key=lambda t: t.ranking_edge_bps)
        worst_primary = min(primaries, key=lambda t: t.ranking_edge_bps)

        primary_session = self.entry_l2_sessions.sessions.get(worst_primary.pair_id)
        primary_assigned_at = (
            primary_session.primary_assigned_at_ms if primary_session else 0
        )

        shadow_session = self.entry_l2_sessions.sessions.get(best_shadow.pair_id)
        shadow_ready = (
            shadow_session.state.value == "ready" if shadow_session else False
        )
        primary_executing = self._tracked_pair_is_executing(worst_primary.pair_id)

        hold_allows = primary_hold_window_allows_replacement(
            primary_assigned_at, now_ms, primary_min_hold_ms)

        eligible = shadow_promotion_is_eligible(
            primary=worst_primary,
            shadow=best_shadow,
            primary_assigned_at_ms=primary_assigned_at,
            now_ms=now_ms,
            primary_min_hold_ms=primary_min_hold_ms,
            shadow_promotion_score_delta_bps=score_delta_bps,
            primary_executing=primary_executing,
            shadow_ready=shadow_ready,
        )

        if eligible:
            if worst_primary.pair_id in self._tracked_primary_pair_ids:
                self._tracked_primary_pair_ids.discard(worst_primary.pair_id)
            self._tracked_primary_pair_ids.add(best_shadow.pair_id)
            best_shadow.class_ = TrackedOpportunityClass.PRIMARY
            worst_primary.class_ = TrackedOpportunityClass.SHADOW
            if shadow_session:
                shadow_session.shadow_promoted_at_ms = now_ms
            self.journal.append(
                "runtime.entry_local_l2_primary_changed",
                {
                    "promoted_pair_id": best_shadow.pair_id,
                    "demoted_pair_id": worst_primary.pair_id,
                    "reason": "shadow_promotion",
                    "ts_ms": now_ms,
                },
            )
        else:
            score_delta = best_shadow.ranking_edge_bps - worst_primary.ranking_edge_bps
            if score_delta >= score_delta_bps and not hold_allows:
                self.journal.append(
                    "runtime.entry_local_l2_shadow_blocked",
                    {
                        "shadow_pair_id": best_shadow.pair_id,
                        "primary_pair_id": worst_primary.pair_id,
                        "reason": "primary_hold_window",
                        "ts_ms": now_ms,
                    },
                )

    def _tracked_pair_is_executing(self, pair_id: str) -> bool:
        """Check if a tracked pair has a pending entry currently executing.

        V1: tracked_entry_local_l2_is_executing (engine.rs).
        """
        parts = pair_id.split(":", 2)
        if len(parts) < 3:
            return False
        long_v, short_v, symbol = parts[0], parts[1], parts[2]
        for pending in self.state.pending_entries.values():
            if (
                pending.symbol == symbol
                and pending.long_venue.value == long_v
                and pending.short_venue.value == short_v
            ):
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

    @staticmethod
    def _safe_positive_float(value) -> float:
        try:
            result = float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0
        return result if math.isfinite(result) and result > 0 else 0.0

    async def _okx_entry_base_quantity_step(
        self, venue: Venue, symbol: str,
    ) -> float | None:
        if venue != Venue.OKX:
            return 0.0
        adapter = self.get_venue_adapter(venue)
        if adapter is None:
            return None

        explicit_step = self._safe_positive_float(
            getattr(adapter, "okx_base_quantity_step", 0.0)
        )
        if explicit_step > 0:
            return explicit_step

        transport = getattr(adapter, "_transport", None)
        if transport is None:
            return 0.0

        transport_step = self._safe_positive_float(
            getattr(transport, "okx_base_quantity_step", 0.0)
        )
        if transport_step > 0:
            return transport_step

        venue_symbol = symbol
        venue_symbol_fn = getattr(transport, "_venue_symbol", None)
        if callable(venue_symbol_fn):
            try:
                venue_symbol = venue_symbol_fn(symbol)
            except Exception:
                venue_symbol = symbol

        metadata = getattr(transport, "_symbol_metadata", {}) or {}
        for key in (symbol, venue_symbol):
            meta = metadata.get(key) or {}
            if not isinstance(meta, dict):
                continue
            ct_val = self._safe_positive_float(
                meta.get("ct_val") or meta.get("ctVal") or meta.get("contract_size")
            )
            lot_sz = self._safe_positive_float(
                meta.get("lot_sz") or meta.get("lotSz") or meta.get("qty_step")
            )
            if ct_val > 0 and lot_sz > 0:
                return ct_val * lot_sz

        try:
            from lightfee.venues.symbol_rules import get_symbol_rules_cache

            rule = await get_symbol_rules_cache().get(transport, Venue.OKX, venue_symbol)
            ct_val = self._safe_positive_float(getattr(rule, "ct_val", 0.0))
            lot_sz = self._safe_positive_float(getattr(rule, "qty_step", 0.0))
            if ct_val > 0 and lot_sz > 0:
                return ct_val * lot_sz
        except Exception:
            pass

        mode = str(getattr(transport, "mode", "") or "").lower()
        if mode == "live":
            return None
        return 0.0

    async def _okx_aligned_entry_quantity(
        self,
        *,
        long_venue: Venue,
        short_venue: Venue,
        symbol: str,
        quantity: float,
        now_ms: int,
    ) -> tuple[float, float | None]:
        okx_steps: list[float] = []
        missing = False
        for venue in (long_venue, short_venue):
            step = await self._okx_entry_base_quantity_step(venue, symbol)
            if step is None:
                missing = True
            elif step > 0:
                okx_steps.append(step)
        if missing:
            return 0.0, None
        if not okx_steps:
            return quantity, 0.0
        step = max(okx_steps)
        aligned = math.floor((quantity / step) + 1e-12) * step
        if aligned <= 0:
            return 0.0, step
        return aligned, step

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

        if not self._candidate_is_tradeable_for_selection(candidate):
            self.journal.append(
                "runtime.entry_blocked_trading_capability",
                {
                    "symbol": getattr(candidate, "symbol", ""),
                    "long_venue": getattr(candidate, "long_venue", ""),
                    "short_venue": getattr(candidate, "short_venue", ""),
                    "reason": "candidate_not_tradeable_for_selection",
                    "ts_ms": now_ms,
                },
            )
            return False

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
        quantity, okx_base_step = await self._okx_aligned_entry_quantity(
            long_venue=long_venue,
            short_venue=short_venue,
            symbol=candidate.symbol,
            quantity=quantity,
            now_ms=now_ms,
        )
        if okx_base_step is None:
            self.journal.append(
                "runtime.entry_skipped_okx_contract_metadata_missing",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "raw_quantity": candidate.entry_notional_quote / price_hint,
                    "reason": "okx_ct_val_lot_sz_unconfirmed",
                    "ts_ms": now_ms,
                },
            )
            return False
        if quantity <= 0:
            self.journal.append(
                "runtime.entry_skipped_okx_contract_step",
                {
                    "symbol": candidate.symbol,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "okx_base_quantity_step": okx_base_step,
                    "raw_quantity": candidate.entry_notional_quote / price_hint,
                    "reason": "quantity_below_okx_contract_step",
                    "ts_ms": now_ms,
                },
            )
            return False

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
        if okx_base_step and okx_base_step > 0:
            min_hedgeable_chunk = max(min_hedgeable_chunk, okx_base_step)

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
            effective_quantity = plan.full_target_quantity
        else:
            entry_type = EntryType.STANDARD_DUAL_TAKER
            effective_quantity = plan.full_target_quantity

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

        maker_bbo_evidence: dict = {}
        if entry_type in (EntryType.PASSIVE_INCREMENTAL, EntryType.PASSIVE_FALLBACK):
            bbo_ok, bbo_reason, maker_bbo_evidence = self._post_only_maker_bbo_guard(
                venue=maker_venue,
                symbol=candidate.symbol,
                side=maker_leg,
                price=price_hint,
                now_ms=now_ms,
            )
            if not bbo_ok:
                payload = {
                    **maker_bbo_evidence,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "reason": bbo_reason,
                    "ts_ms": now_ms,
                }
                self.journal.append("runtime.entry_blocked_post_only_bbo", payload)
                self.journal.append(
                    "review.candidate_rejected",
                    {
                        "symbol": candidate.symbol,
                        "long_venue": long_venue.value,
                        "short_venue": short_venue.value,
                        "rejected_stage": "post_only_bbo_gate",
                        "rejected_reason": bbo_reason,
                        "ranking_edge_bps": candidate.ranking_edge_bps,
                        "expected_edge_bps": candidate.expected_edge_bps,
                        "funding_edge_bps": candidate.funding_edge_bps,
                        "ts_ms": now_ms,
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
            if (
                result.route == ExecutionRoute.REJECTED
                and self._entry_reject_is_post_only_would_take(
                    getattr(result, "reject_reason", "")
                )
            ):
                self._record_post_only_reject_cooldown(
                    candidate,
                    now_ms,
                    getattr(result, "reject_reason", ""),
                    venue=maker_venue.value,
                    side=maker_leg.value,
                    price=price_hint,
                    bbo=maker_bbo_evidence,
                )
                return True
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
                        long_price_hint=self._resolve_local_l2_mid(position.long_venue, position.symbol, now_ms=now_ms),
                        short_price_hint=self._resolve_local_l2_mid(position.short_venue, position.symbol, now_ms=now_ms),
                        short_stage="exit_short",
                        long_stage="exit_long",
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
                        long_price_hint=self._resolve_local_l2_mid(position.long_venue, position.symbol, now_ms=now_ms),
                        short_price_hint=self._resolve_local_l2_mid(position.short_venue, position.symbol, now_ms=now_ms),
                        state=self.state,
                    )

    def _resolve_local_l2_mid(self, venue, symbol: str, now_ms: int | None = None) -> float:
        """Get mid price from local L2 book or sidecar for the given venue+symbol."""
        if now_ms is None:
            now_ms = wall_clock_now_ms()
        venue_value = venue.value if hasattr(venue, 'value') else str(venue)
        budget_ms = int(self.config.strategy.max_liquidity_snapshot_age_ms or 0)
        try:
            book = self.local_l2_runtime.get_book(venue_value, symbol)
            if book is not None and book.status.value == "hot":
                age_ms = book.age_ms(now_ms)
                if budget_ms > 0 and book.is_stale(budget_ms, now_ms):
                    self.journal.append(
                        "runtime.close_price_evidence_stale",
                        {
                            "venue": venue_value,
                            "symbol": symbol,
                            "domain": "local_l2_book",
                            "age_ms": age_ms,
                            "budget_ms": budget_ms,
                            "decision": "reject_price_hint",
                            "fallback_source": "none",
                            "ts_ms": now_ms,
                        },
                    )
                    return 0.0
                mid = book.mid_price()
                if mid and mid > 0:
                    return mid
        except Exception:
            pass
        return 0.0

    def _resolve_local_l2_quote(self, venue, symbol: str) -> tuple[float, float] | None:
        """Get best bid/ask from the local L2 book for passive tick inference."""
        try:
            book = self.local_l2_runtime.get_book(
                venue.value if hasattr(venue, "value") else str(venue),
                symbol,
            )
            if book is not None and book.status.value == "hot":
                best_bid = book.best_bid()
                best_ask = book.best_ask()
                if best_bid > 0 and best_ask > best_bid:
                    return best_bid, best_ask
        except Exception:
            pass
        return None

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
