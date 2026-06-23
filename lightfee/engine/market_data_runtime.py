"""Market data runtime delegate.

This module owns behavior mechanically moved from LiveRuntime.
Do not change market-data business conditions while extracting it.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from typing import Any

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import Venue
from lightfee.engine.business_contract import quote_rewarm_handoff_contract
from lightfee.engine.bootstrap import wall_clock_now_ms
from lightfee.engine.lifecycle_sla import phase_budgets_from_strategy
from lightfee.engine.runtime_context import MarketDataRuntimeContext
from lightfee.marketdata.l2 import L2BookStatus, L2PoolAssignment, LocalL2BookKey


class EntryOpenInterestRefresher:
    """Candidate-scoped public OI refresher for entry liquidity evidence."""

    SUPPORTED_VENUES = {
        "aster",
        "binance",
        "bitget",
        "bybit",
        "gate",
        "hyperliquid",
        "okx",
    }

    def __init__(self, *, targeted_budget_s: float | None = None) -> None:
        self._clients: dict[str, Any] = {}
        if targeted_budget_s is None:
            from lightfee.venues.market_data import (
                BINANCE_STYLE_ENTRY_OPEN_INTEREST_BUDGET_S,
            )

            targeted_budget_s = BINANCE_STYLE_ENTRY_OPEN_INTEREST_BUDGET_S
        self._targeted_budget_s = max(float(targeted_budget_s or 0.0), 0.0)

    async def close(self) -> None:
        for client in list(self._clients.values()):
            close = getattr(client, "close", None)
            if callable(close):
                await close()
        self._clients.clear()

    def _client_for_venue(self, venue: str):
        venue_key = str(venue or "").strip().lower()
        client = self._clients.get(venue_key)
        if client is not None:
            return client
        from lightfee.venues.market_data import MarketDataClient
        from lightfee.venues.specs import get_spec

        venue_enum = Venue.from_str(venue_key)
        client = MarketDataClient(get_spec(venue_enum))
        client.binance_style_open_interest_enrichment_budget_s = self._targeted_budget_s
        self._clients[venue_key] = client
        return client

    async def refresh_open_interest(
        self,
        venue: str,
        symbol: str,
        *,
        now_ms: int,
    ) -> dict[str, Any] | None:
        symbol_key = str(symbol or "").strip().upper()
        if not symbol_key:
            return {
                "open_interest_quote": 0.0,
                "open_interest_evidence_status": "unsupported",
                "open_interest_evidence_reason": "unsupported_targeted_refresh",
            }
        batch = await self.refresh_open_interest_batch(
            venue,
            [symbol_key],
            now_ms=now_ms,
        )
        return batch.get(symbol_key)

    async def refresh_open_interest_batch(
        self,
        venue: str,
        symbols: list[str],
        *,
        now_ms: int,
    ) -> dict[str, dict[str, Any]]:
        venue_key = str(venue or "").strip().lower()
        symbol_keys = [
            str(symbol or "").strip().upper()
            for symbol in symbols
            if str(symbol or "").strip()
        ]
        symbol_keys = list(dict.fromkeys(symbol_keys))
        if venue_key not in self.SUPPORTED_VENUES:
            return {
                symbol: {
                    "open_interest_quote": 0.0,
                    "open_interest_evidence_status": "unsupported",
                    "open_interest_evidence_reason": "unsupported_targeted_refresh",
                }
                for symbol in symbol_keys
            }
        if not symbol_keys:
            return {}
        try:
            result = await self._client_for_venue(
                venue_key
            ).fetch_entry_open_interest_evidence(symbol_keys)
        except Exception as exc:
            return {
                symbol: {
                    "open_interest_quote": 0.0,
                    "open_interest_evidence_status": "timeout",
                    "open_interest_evidence_reason": f"{type(exc).__name__}: {exc}"[:200],
                }
                for symbol in symbol_keys
            }
        payloads: dict[str, dict[str, Any]] = {}
        for symbol_key in symbol_keys:
            ticker = result.get(f"{venue_key}:{symbol_key}")
            if ticker is None:
                payloads[symbol_key] = {
                    "open_interest_quote": 0.0,
                    "open_interest_evidence_status": "parse_error",
                    "open_interest_evidence_reason": "missing_targeted_ticker",
                }
                continue
            payloads[symbol_key] = {
                "open_interest_quote": float(
                    getattr(ticker, "open_interest_quote", 0.0) or 0.0
                ),
                "open_interest_evidence_status": str(
                    getattr(ticker, "open_interest_evidence_status", "") or "unavailable"
                ),
                "open_interest_evidence_reason": str(
                    getattr(ticker, "open_interest_evidence_reason", "")
                    or "targeted_refresh"
                ),
                "oi_candidate_count": int(getattr(ticker, "oi_candidate_count", 0) or 0),
                "oi_cache_hit_count": int(getattr(ticker, "oi_cache_hit_count", 0) or 0),
                "oi_cache_miss_count": int(getattr(ticker, "oi_cache_miss_count", 0) or 0),
                "oi_refresh_attempt_count": int(
                    getattr(ticker, "oi_refresh_attempt_count", 0) or 0
                ),
                "oi_refresh_cap": int(getattr(ticker, "oi_refresh_cap", 0) or 0),
                "oi_deferred_count": int(getattr(ticker, "oi_deferred_count", 0) or 0),
                "oi_timeout_count": int(getattr(ticker, "oi_timeout_count", 0) or 0),
                "oi_refresh_elapsed_ms": int(
                    getattr(ticker, "oi_refresh_elapsed_ms", 0) or 0
                ),
            }
        return payloads


class MarketDataRuntime:
    def __init__(self, ctx: MarketDataRuntimeContext) -> None:
        self.ctx = ctx

    @property
    def ws_bbo_rest_refresher(self):
        return getattr(self.ctx, "ws_bbo_rest_refresher", None)

    @ws_bbo_rest_refresher.setter
    def ws_bbo_rest_refresher(self, value) -> None:
        setattr(self.ctx, "ws_bbo_rest_refresher", value)

    @property
    def entry_open_interest_refresher(self):
        return getattr(self.ctx, "entry_open_interest_refresher", None)

    @entry_open_interest_refresher.setter
    def entry_open_interest_refresher(self, value) -> None:
        setattr(self.ctx, "entry_open_interest_refresher", value)

    @property
    def _entry_bbo_subscription_budgeted_keys(self) -> set[tuple[str, str]]:
        return self.ctx._entry_bbo_subscription_budgeted_keys

    @_entry_bbo_subscription_budgeted_keys.setter
    def _entry_bbo_subscription_budgeted_keys(self, value: set[tuple[str, str]]) -> None:
        self.ctx._entry_bbo_subscription_budgeted_keys = value

    @property
    def _entry_bbo_subscription_budget_excluded_keys(self) -> set[tuple[str, str]]:
        return self.ctx._entry_bbo_subscription_budget_excluded_keys

    @_entry_bbo_subscription_budget_excluded_keys.setter
    def _entry_bbo_subscription_budget_excluded_keys(self, value: set[tuple[str, str]]) -> None:
        self.ctx._entry_bbo_subscription_budget_excluded_keys = value

    @property
    def _entry_bbo_subscription_per_venue_budget(self) -> int:
        return self.ctx._entry_bbo_subscription_per_venue_budget

    @_entry_bbo_subscription_per_venue_budget.setter
    def _entry_bbo_subscription_per_venue_budget(self, value: int) -> None:
        self.ctx._entry_bbo_subscription_per_venue_budget = value

    @property
    def _last_snapshot_freshness_filter_blockers(self):
        return self.ctx._last_snapshot_freshness_filter_blockers

    @_last_snapshot_freshness_filter_blockers.setter
    def _last_snapshot_freshness_filter_blockers(self, value) -> None:
        self.ctx._last_snapshot_freshness_filter_blockers = value

    @property
    def _last_snapshot_freshness_filter_samples(self):
        return self.ctx._last_snapshot_freshness_filter_samples

    @_last_snapshot_freshness_filter_samples.setter
    def _last_snapshot_freshness_filter_samples(self, value) -> None:
        self.ctx._last_snapshot_freshness_filter_samples = value

    @property
    def _snapshot_freshness_decision_last_emit_ms(self):
        return self.ctx._snapshot_freshness_decision_last_emit_ms

    @property
    def _snapshot_freshness_decision_suppressed(self):
        return self.ctx._snapshot_freshness_decision_suppressed

    @property
    def _SNAPSHOT_FRESHNESS_DECISION_LOG_INTERVAL_MS(self) -> int:
        return self.ctx._SNAPSHOT_FRESHNESS_DECISION_LOG_INTERVAL_MS

    def get_venue_adapter(self, venue: Venue) -> VenueAdapter | None:
        return self.ctx.get_venue_adapter(venue)

    def _entry_readiness_provider_name(self) -> str:
        return self.ctx._entry_readiness_provider_name()

    def _entry_readiness_provider_uses_local_l2(self) -> bool:
        return self.ctx._entry_readiness_provider_uses_local_l2()

    def _entry_readiness_provider_uses_ws_bbo(self) -> bool:
        return self.ctx._entry_readiness_provider_uses_ws_bbo()

    def _local_l2_effective_enabled(self) -> bool:
        return self.ctx._local_l2_effective_enabled()

    def _entry_local_l2_stale_after_ms(self) -> int:
        return self.ctx._entry_local_l2_stale_after_ms()

    async def _filter_symbols_supported_by_venue(
        self,
        venue: Venue,
        adapter: VenueAdapter,
        symbols: list[str],
        *,
        skip_event_kind: str,
    ) -> list[str]:
        return await self.ctx._filter_symbols_supported_by_venue(
            venue,
            adapter,
            symbols,
            skip_event_kind=skip_event_kind,
        )

    def _append_runtime_diagnostic_event(self, *args, **kwargs) -> None:
        return self.ctx._append_runtime_diagnostic_event(*args, **kwargs)

    def _candidate_pair_id(self, candidate) -> str:
        return self.ctx._candidate_pair_id(candidate)

    def _clear_local_l2_runtime_state(self) -> None:
        return self.ctx._clear_local_l2_runtime_state()

    def _record_snapshot_scoped_status(self, *args, **kwargs) -> None:
        return self.ctx._record_snapshot_scoped_status(*args, **kwargs)

    def _candidate_requires_sidecar_perp_liquidity(self, candidate) -> bool:
        return self.ctx._candidate_requires_sidecar_perp_liquidity(candidate)

    def _entry_liquidity_qualification_decisions(self, *args, **kwargs):
        return self.ctx._entry_liquidity_qualification_decisions(*args, **kwargs)

    def _liquidity_degraded_reason_blocks_symbol(self, reason: str, symbol: str) -> bool:
        return self.ctx._liquidity_degraded_reason_blocks_symbol(reason, symbol)

    def _liquidity_lifecycle_payload(self, *args, **kwargs) -> dict:
        return self.ctx._liquidity_lifecycle_payload(*args, **kwargs)

    def _select_v1_entry_tracked_scope(self, candidates) -> tuple[list, list]:
        return self.ctx._select_v1_entry_tracked_scope(candidates)

    def _runtime_method_override(self, method_name: str):
        method = getattr(self.ctx, method_name, None)
        class_method = getattr(type(self.ctx), method_name, None)
        if getattr(method, "__func__", None) is class_method:
            return None
        return method if callable(method) else None

    async def _call_ensure_entry_bbo_active_for_candidates(
        self, candidates, now_ms: int
    ) -> None:
        override = self._runtime_method_override("_ensure_entry_bbo_active_for_candidates")
        if override is not None:
            return await override(candidates, now_ms)
        return await self._ensure_entry_bbo_active_for_candidates(candidates, now_ms)

    def _call_candidate_snapshot_freshness_decisions(self, candidate, **kwargs):
        override = self._runtime_method_override("_candidate_snapshot_freshness_decisions")
        if override is not None:
            return override(candidate, **kwargs)
        return self._candidate_snapshot_freshness_decisions(candidate, **kwargs)

    def _runtime_market_data_config_summary(self) -> dict[str, Any]:
        provider = self._entry_readiness_provider_name()
        return {
            "entry_readiness_provider_effective": provider,
            "local_l2_configured_enabled": bool(
                getattr(self.ctx.config.strategy, "local_l2_enabled", False)
            ),
            "local_l2_ws_configured_enabled": bool(
                getattr(self.ctx.config.strategy, "local_l2_ws_enabled", False)
            ),
            "local_l2_effective_enabled": self._local_l2_effective_enabled(),
            "local_l2_effective_disabled_reason": (
                "ws_bbo_quote_lease_overrides_legacy_local_l2_flag"
                if (
                    provider == "ws_bbo_quote_lease"
                    and bool(getattr(self.ctx.config.strategy, "local_l2_enabled", False))
                )
                else ""
            ),
        }

    def _refresh_runtime_market_data_config_state(self) -> None:
        self.ctx.state.runtime_market_data_config = (
            self._runtime_market_data_config_summary()
        )

    def _entry_quote_lease_max_age_ms(self) -> int:
        budgets = []
        for value in (
            getattr(self.ctx.config.runtime, "max_market_age_ms", 0),
            getattr(self.ctx.config.strategy, "entry_quote_lease_ttl_ms", 0),
        ):
            try:
                parsed = int(value or 0)
            except (TypeError, ValueError):
                parsed = 0
            if parsed > 0:
                budgets.append(parsed)
        return min(budgets) if budgets else 0

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
        self._refresh_runtime_market_data_config_state()
        if not self._local_l2_effective_enabled():
            return

        self.ctx.journal.append(
            "runtime.local_l2_phase_start",
            {"ts_ms": now_ms},
        )

        # V1: startup_local_l2_symbols() → retained + hot symbols only
        # NOT all config.symbols — L2 is only bootstrapped for symbols with activity
        target_pairs: set[tuple[str, str]] = set()
        if self._local_l2_effective_enabled():
            active_venues = list(self.ctx.venue_adapters.keys())
            venue_set = {
                v.value if hasattr(v, 'value') else str(v)
                for v in active_venues
            }

            # 1. Retained books from previous run (V1: retained_local_l2_books)
            for book in (self.ctx.state.retained_local_l2_books or []):
                ven = book.get("venue", "")
                sym = book.get("symbol", "")
                if ven in venue_set and sym:
                    target_pairs.add((ven, sym))

            # 2. Hot symbols from active positions (V1: hot_local_l2_symbols)
            hot_budget = max(
                getattr(self.ctx.config.strategy, 'local_l2_hot_exec_per_venue_budget', 20), 1,
            )
            hot_global_budget = max(
                getattr(self.ctx.config.strategy, 'local_l2_hot_exec_global_budget', 0), 0,
            )
            hot_count = 0
            hot_global_count = 0
            for pos in getattr(self.ctx.state, 'open_positions', []) or []:
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
            self.ctx.journal.append(
                "runtime.local_l2_phase_complete",
                {
                    "books_bootstrapped": 0,
                    "reason": "no target pairs — local_l2 disabled or no venues/symbols",
                    "phase_ms": wall_clock_now_ms() - now_ms,
                },
            )
            return

        if self.ctx.config.runtime.mode != "paper":
            from lightfee.core.domain import Venue as VenueEnum

            filtered_pairs: set[tuple[str, str]] = set()
            venue_symbols_for_filter: dict[str, list[str]] = {}
            for venue_str, symbol in target_pairs:
                venue_symbols_for_filter.setdefault(venue_str, []).append(symbol)

            for venue_str, symbols in venue_symbols_for_filter.items():
                try:
                    ven = VenueEnum.from_str(venue_str)
                    adapter = self.get_venue_adapter(ven) if ven in self.ctx.venue_adapters else None
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
            self.ctx.journal.append(
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
            book = self.ctx.local_l2_runtime.ensure_book(venue_str, symbol)
            book.max_depth = rules.default_depth
            book.max_sequence_gap = rules.max_sequence_gap
            if book.status == L2BookStatus.COLD:
                if self.ctx.config.runtime.mode == "paper":
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
            self._local_l2_effective_enabled()
            and getattr(self.ctx.config.strategy, 'local_l2_ws_enabled', False)
            and self.ctx.config.runtime.mode != "paper"
        ):
            ws_started = 0
            for venue_str, symbols in venue_symbols.items():
                try:
                    from lightfee.core.domain import Venue as VenueEnum
                    ven = VenueEnum.from_str(venue_str)
                    adapter = self.get_venue_adapter(ven) if ven in self.ctx.venue_adapters else None
                except (ValueError, KeyError):
                    adapter = None

                registered = self.ctx.l2_data_plane.start_ws_streams(
                    venue_str, symbols, adapter=adapter,
                )
                if registered > 0:
                    ws_started += registered

            if ws_started > 0:
                connected = await self.ctx.l2_data_plane.connect_ws_streams()
                ws_started = connected
                self.ctx.journal.append(
                    "runtime.local_l2_ws_started",
                    {
                        "stream_count": ws_started,
                        "venues": sorted(venue_symbols.keys()),
                        "ts_ms": wall_clock_now_ms(),
                    },
                )

        # Step 3: Start per-venue background bootstrap workers (V1: start_local_l2_bootstrap)
        # Each worker fetches REST snapshots with concurrency control and retry
        if self.ctx.config.runtime.mode != "paper":
            bs_total = 0
            bs_batch = getattr(self.ctx.config.strategy, 'local_l2_bootstrap_batch_size', 4)
            bs_jitter = getattr(self.ctx.config.strategy, 'local_l2_bootstrap_jitter_ms', 250)
            bs_retry = getattr(self.ctx.config.strategy, 'local_l2_bootstrap_retry_backoff_ms', 5000)

            for venue_str, symbols in venue_symbols.items():
                try:
                    from lightfee.core.domain import Venue as VenueEnum
                    ven = VenueEnum.from_str(venue_str)
                    adapter = self.get_venue_adapter(ven) if ven in self.ctx.venue_adapters else None
                except (ValueError, KeyError):
                    adapter = None

                if adapter is None or not hasattr(adapter, 'fetch_l2_snapshot'):
                    continue

                self.ctx.l2_data_plane.start_background_bootstrap(
                    venue=venue_str,
                    symbols=symbols,
                    adapter=adapter,
                    batch_size=bs_batch,
                    jitter_ms=bs_jitter,
                    retry_backoff_ms=bs_retry,
                )
                bs_total += len(symbols)

            self.ctx.journal.append(
                "runtime.local_l2_bootstrap_started",
                {
                    "venues": sorted(venue_symbols.keys()),
                    "total_symbols": bs_total,
                    "ts_ms": wall_clock_now_ms(),
                },
            )

        # Restore retained books from previous state
        books_retained = 0
        if hasattr(self.ctx.state, "retained_local_l2_books"):
            for entry in getattr(self.ctx.state, "retained_local_l2_books", []):
                venue = entry.get("venue", "")
                sym = entry.get("symbol", "")
                if (venue, sym) not in target_pairs:
                    continue
                if venue and sym:
                    book = self.ctx.local_l2_runtime.ensure_book(venue, sym)
                    if book.status == L2BookStatus.COLD:
                        book.pool = L2PoolAssignment.RETAINED
                        book.transition_to_bootstrapping(now_ms)
                        books_retained += 1

        self.ctx.journal.append(
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
        self._refresh_runtime_market_data_config_state()
        if not self._local_l2_effective_enabled():
            return
        if self.ctx.config.runtime.mode == "paper":
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
        registered_total = 0
        registered_venues: set[str] = set()
        connect_ws_streams_needed = False
        stale_after_ms = self._entry_local_l2_stale_after_ms()
        from lightfee.marketdata.local_l2_policy import BridgeMode, policy_for_venue

        def hot_book_needs_ws_lifecycle_attention(venue: str, symbol: str) -> bool:
            if not getattr(self.ctx.config.strategy, 'local_l2_ws_enabled', False):
                return False
            policy = policy_for_venue(venue)
            if policy.bridge_mode not in (
                BridgeMode.WS_SNAPSHOT_AUTHORITATIVE,
                BridgeMode.STREAM_ONLY,
            ):
                return False
            stream_state_fn = getattr(self.ctx.l2_data_plane, "ws_stream_state", None)
            if not callable(stream_state_fn):
                return False
            stream_state = stream_state_fn(venue, symbol)
            return (
                not bool(stream_state.get("registered"))
                or not bool(stream_state.get("connected"))
            )

        def venue_adapter_for_local_l2(venue: str):
            try:
                ven = Venue.from_str(venue)
                return self.get_venue_adapter(ven) if ven in self.ctx.venue_adapters else None
            except (ValueError, KeyError):
                return None

        def ensure_hot_ws_lifecycle(venue: str, symbol: str) -> None:
            nonlocal registered_total, connect_ws_streams_needed
            adapter = venue_adapter_for_local_l2(venue)
            if adapter is None or not hasattr(adapter, 'fetch_l2_snapshot'):
                return
            before_state = self.ctx.l2_data_plane.ws_stream_state(venue, symbol)
            registered = self.ctx.l2_data_plane.start_ws_streams(
                venue, [symbol], adapter=adapter,
            )
            after_state = self.ctx.l2_data_plane.ws_stream_state(venue, symbol)
            if registered > 0:
                registered_total += registered
            if (
                registered > 0
                or (
                    bool(before_state.get("registered"))
                    and not bool(before_state.get("connected"))
                )
                or (
                    bool(after_state.get("registered"))
                    and not bool(after_state.get("connected"))
                )
            ):
                connect_ws_streams_needed = True
                registered_venues.add(venue)

        async def connect_registered_ws_streams() -> None:
            nonlocal connect_ws_streams_needed
            if not connect_ws_streams_needed:
                return
            connected = await self.ctx.l2_data_plane.connect_ws_streams()
            self.ctx.journal.append(
                "runtime.local_l2_dynamic_ws_started",
                {
                    "registered_stream_count": registered_total,
                    "connected_stream_count": connected,
                    "venues": sorted(registered_venues),
                    "ts_ms": wall_clock_now_ms(),
                },
            )
            connect_ws_streams_needed = False

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
                book = self.ctx.local_l2_runtime.get_book(ven_str, sym)
                if book is not None:
                    self.ctx.local_l2_runtime.assign(
                        ven_str, sym, desired_pool, now_ms=now_ms,
                    )
                    if book.status == L2BookStatus.HOT:
                        stale = book.is_stale(stale_after_ms, now_ms)
                        crossed = book.has_crossed_book()
                        if not stale and not crossed:
                            if hot_book_needs_ws_lifecycle_attention(ven_str, str(sym)):
                                ensure_hot_ws_lifecycle(ven_str, str(sym))
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

        for position in getattr(self.ctx.state, "open_positions", {}).values():
            sym = getattr(position, "symbol", "")
            remember_key(getattr(position, "long_venue", ""), sym, L2PoolAssignment.RETAINED)
            remember_key(getattr(position, "short_venue", ""), sym, L2PoolAssignment.RETAINED)

        for pending in getattr(self.ctx.state, "pending_entries", {}).values():
            sym = getattr(pending, "symbol", "")
            remember_key(getattr(pending, "long_venue", ""), sym, L2PoolAssignment.HOT_EXEC)
            remember_key(getattr(pending, "short_venue", ""), sym, L2PoolAssignment.HOT_EXEC)

        for pending_close in getattr(self.ctx.state, "pending_passive_closes", {}).values():
            position = getattr(pending_close, "position_snapshot", None)
            if position is None:
                continue
            sym = getattr(position, "symbol", "")
            remember_key(getattr(position, "long_venue", ""), sym, L2PoolAssignment.HOT_EXEC)
            remember_key(getattr(position, "short_venue", ""), sym, L2PoolAssignment.HOT_EXEC)

        if not needed:
            await connect_registered_ws_streams()
            self.ctx.l2_data_plane.prune_untracked_books(
                tracked_keys,
                now_ms,
                retained_max_age_ms=max(stale_after_ms, 300_000),
            )
            return

        per_venue_budget = max(
            getattr(self.ctx.config.strategy, 'local_l2_hot_exec_per_venue_budget', 20), 1,
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
                adapter = self.get_venue_adapter(ven) if ven in self.ctx.venue_adapters else None
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
                book = self.ctx.local_l2_runtime.ensure_book(ven_str, sym)
                self.ctx.local_l2_runtime.assign(
                    ven_str, sym, desired_pool, now_ms=now_ms,
                )
                book.max_depth = rules.default_depth
                book.max_sequence_gap = rules.max_sequence_gap
                if book.status == L2BookStatus.COLD:
                    book.transition_to_bootstrapping(now_ms)

            if getattr(self.ctx.config.strategy, 'local_l2_ws_enabled', False):
                stream_state_fn = getattr(self.ctx.l2_data_plane, "ws_stream_state", None)
                before_states = (
                    {
                        sym: stream_state_fn(ven_str, sym)
                        for sym in symbols_list
                    }
                    if callable(stream_state_fn)
                    else {}
                )
                registered = self.ctx.l2_data_plane.start_ws_streams(
                    ven_str, symbols_list, adapter=adapter,
                )
                after_states = (
                    {
                        sym: stream_state_fn(ven_str, sym)
                        for sym in symbols_list
                    }
                    if callable(stream_state_fn)
                    else {}
                )
                if registered > 0:
                    registered_total += registered
                disconnected_registered = any(
                    bool(state.get("registered")) and not bool(state.get("connected"))
                    for state in [*before_states.values(), *after_states.values()]
                )
                if registered > 0 or disconnected_registered:
                    registered_venues.add(ven_str)
                    connect_ws_streams_needed = True

            # Start background bootstrap worker
            bs_batch = getattr(self.ctx.config.strategy, 'local_l2_bootstrap_batch_size', 4)
            bs_jitter = getattr(self.ctx.config.strategy, 'local_l2_bootstrap_jitter_ms', 250)
            bs_retry = getattr(self.ctx.config.strategy, 'local_l2_bootstrap_retry_backoff_ms', 5000)
            self.ctx.l2_data_plane.start_background_bootstrap(
                venue=ven_str,
                symbols=symbols_list,
                adapter=adapter,
                batch_size=bs_batch,
                jitter_ms=bs_jitter,
                retry_backoff_ms=bs_retry,
            )

        await connect_registered_ws_streams()

        self.ctx.l2_data_plane.prune_untracked_books(
            tracked_keys,
            now_ms,
            retained_max_age_ms=max(stale_after_ms, 300_000),
        )

    async def _ensure_entry_bbo_active_for_candidates(
        self,
        candidates,
        now_ms: int,
    ) -> None:
        """Start independent per-venue BBO streams for entry candidates.

        This is separate from LocalL2Runtime: it does not create books, bootstrap
        snapshots, replay deltas, or update entry L2 sessions.
        """
        if not self._entry_readiness_provider_uses_ws_bbo():
            self._entry_bbo_subscription_budgeted_keys = set()
            self._entry_bbo_subscription_budget_excluded_keys = set()
            self._entry_bbo_subscription_per_venue_budget = 0
            return
        if self.ctx.config.runtime.mode == "paper":
            self._entry_bbo_subscription_budgeted_keys = set()
            self._entry_bbo_subscription_budget_excluded_keys = set()
            self._entry_bbo_subscription_per_venue_budget = 0
            return

        needed: dict[str, list[str]] = {}
        seen_by_venue: dict[str, set[str]] = {}
        tracked_keys: set[tuple[str, str]] = set()
        sticky_warm_until_ms: dict[tuple[str, str], int] = dict(
            getattr(self.ctx, "_entry_bbo_sticky_warm_until_ms", {}) or {}
        )
        sticky_ttl_ms = max(
            int(
                getattr(
                    self.ctx.config.strategy,
                    "entry_ws_bbo_sticky_warm_ms",
                    120_000,
                )
                or 120_000
            ),
            self._entry_quote_lease_max_age_ms(),
        )
        retained_sticky_keys = {
            key: expires_at
            for key, expires_at in sticky_warm_until_ms.items()
            if int(expires_at or 0) > now_ms
        }
        for candidate in list(candidates or []):
            symbol = str(getattr(candidate, "symbol", "") or "").strip().upper()
            if not symbol:
                continue
            for raw_venue in (
                getattr(candidate, "long_venue", ""),
                getattr(candidate, "short_venue", ""),
            ):
                venue = str(raw_venue or "").strip().lower()
                if not venue:
                    continue
                seen = seen_by_venue.setdefault(venue, set())
                if symbol not in seen:
                    needed.setdefault(venue, []).append(symbol)
                    seen.add(symbol)
                tracked_keys.add((venue, symbol))
        current_tracked_keys = set(tracked_keys)
        for venue, symbol in sorted(retained_sticky_keys):
            seen = seen_by_venue.setdefault(venue, set())
            if symbol not in seen:
                needed.setdefault(venue, []).append(symbol)
                seen.add(symbol)
            tracked_keys.add((venue, symbol))
        sticky_warm_until_ms = {
            **retained_sticky_keys,
            **{
                key: now_ms + sticky_ttl_ms
                for key in current_tracked_keys
            },
        }
        setattr(self.ctx, "_entry_bbo_sticky_warm_until_ms", sticky_warm_until_ms)

        if not needed:
            self._entry_bbo_subscription_budgeted_keys = set()
            self._entry_bbo_subscription_budget_excluded_keys = set()
            self._entry_bbo_subscription_per_venue_budget = 0
            self.ctx.ws_bbo_data_plane.prune_untracked_quotes(
                tracked_keys,
                now_ms,
                retained_max_age_ms=300_000,
            )
            return

        per_venue_budget = max(
            getattr(
                self.ctx.config.strategy,
                "entry_ws_bbo_per_venue_budget",
                getattr(self.ctx.config.strategy, "local_l2_hot_exec_per_venue_budget", 20),
            ),
            1,
        )
        budgeted_keys: set[tuple[str, str]] = set()
        budget_excluded_keys: set[tuple[str, str]] = set()
        for venue_str, symbols in needed.items():
            venue_symbols = list(symbols)
            for symbol in venue_symbols[:per_venue_budget]:
                budgeted_keys.add((venue_str, symbol))
            for symbol in venue_symbols[per_venue_budget:]:
                budget_excluded_keys.add((venue_str, symbol))
        self._entry_bbo_subscription_budgeted_keys = budgeted_keys
        self._entry_bbo_subscription_budget_excluded_keys = budget_excluded_keys
        self._entry_bbo_subscription_per_venue_budget = per_venue_budget

        registered_total = 0
        registered_venues: set[str] = set()
        for venue_str, symbols in needed.items():
            symbols_list = list(symbols)[:per_venue_budget]
            if not symbols_list:
                continue
            adapter = None
            venue_enum = None
            try:
                venue_enum = Venue.from_str(venue_str)
                adapter = (
                    self.get_venue_adapter(venue_enum)
                    if venue_enum in self.ctx.venue_adapters
                    else None
                )
            except (ValueError, KeyError):
                adapter = None

            if adapter is not None and venue_enum is not None:
                symbols_list = await self._filter_symbols_supported_by_venue(
                    venue_enum,
                    adapter,
                    symbols_list,
                    skip_event_kind="runtime.ws_bbo_symbol_skipped",
                )
            if not symbols_list:
                continue

            registered = self.ctx.ws_bbo_data_plane.start_ws_streams(
                venue_str,
                symbols_list,
                adapter=adapter,
            )
            if registered > 0:
                registered_total += registered
                registered_venues.add(venue_str)

        if registered_total > 0:
            connected = await self.ctx.ws_bbo_data_plane.connect_ws_streams()
            self.ctx.journal.append(
                "runtime.ws_bbo_dynamic_ws_started",
                {
                    "registered_stream_count": registered_total,
                    "connected_stream_count": connected,
                    "venues": sorted(registered_venues),
                    "ts_ms": wall_clock_now_ms(),
                },
            )

        self.ctx.ws_bbo_data_plane.prune_untracked_quotes(
            tracked_keys,
            now_ms,
            retained_max_age_ms=300_000,
        )

    @staticmethod
    def _entry_quote_truth_empty_stats() -> dict[str, Any]:
        return {
            "candidate_scope": "",
            "candidate_count": 0,
            "target_count": 0,
            "all_target_count": 0,
            "must_resolve_count": 0,
            "budgeted_target_count": 0,
            "budget_exhausted_count": 0,
            "budget_excluded_without_rest_count": 0,
            "skipped_unbudgeted_count": 0,
            "skipped_untracked_count": 0,
            "cache_initial_hit_count": 0,
            "cache_wait_hit_count": 0,
            "ws_resolved_count": 0,
            "rest_attempt_count": 0,
            "rest_resolved_count": 0,
            "rest_failed_count": 0,
            "rest_throttled_count": 0,
            "wait_budget_ms": 0,
            "wait_elapsed_ms": 0,
            "resolved_count": 0,
            "failed_count": 0,
            "sources": Counter(),
            "top_quote_blocker_buckets": Counter(),
            "quote_lease_failure_counts": Counter(),
        }

    def _entry_quote_truth_record_last_scan(self, stats: dict[str, Any]) -> None:
        self.ctx.state.last_scan["quote_revalidate_candidate_scope"] = str(
            stats.get("candidate_scope", "") or ""
        )
        self.ctx.state.last_scan["quote_revalidate_candidate_count"] = int(
            stats.get("candidate_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_revalidate_all_target_count"] = int(
            stats.get("all_target_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_revalidate_target_count"] = int(
            stats.get("target_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_revalidate_skipped_untracked_count"] = int(
            stats.get("skipped_untracked_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_revalidate_resolved_count"] = int(
            stats.get("resolved_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_revalidate_failed_count"] = int(
            stats.get("failed_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_truth_must_resolve_count"] = int(
            stats.get("must_resolve_count", stats.get("target_count", 0)) or 0
        )
        self.ctx.state.last_scan["quote_truth_resolved_count"] = int(
            stats.get("resolved_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_truth_failed_count"] = int(
            stats.get("failed_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_truth_ws_resolved_count"] = int(
            stats.get("ws_resolved_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_truth_rest_resolved_count"] = int(
            stats.get("rest_resolved_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_truth_rest_throttled_count"] = int(
            stats.get("rest_throttled_count", 0) or 0
        )
        self.ctx.state.last_scan["budget_excluded_without_rest_count"] = int(
            stats.get("budget_excluded_without_rest_count", 0) or 0
        )
        sources = stats.get("sources", Counter())
        self.ctx.state.last_scan["quote_revalidate_sources"] = dict(
            sorted((str(k), int(v)) for k, v in sources.items())
        )
        buckets = stats.get("top_quote_blocker_buckets", Counter())
        self.ctx.state.last_scan["top_quote_blocker_buckets"] = dict(
            sorted((str(k), int(v)) for k, v in buckets.items())
        )
        lease_buckets = stats.get("quote_lease_failure_counts", Counter())
        self.ctx.state.last_scan["quote_lease_failure_counts"] = dict(
            sorted((str(k), int(v)) for k, v in lease_buckets.items())
        )

    def _entry_quote_probe_diagnostics_enabled(self) -> bool:
        return bool(
            getattr(
                getattr(self.ctx.config, "runtime", None),
                "debug_journal_diagnostics_enabled",
                False,
            )
        )

    def _emit_entry_quote_revalidate_probe(
        self,
        *,
        stats: dict[str, Any],
        candidate_count: int,
        now_ms: int,
    ) -> None:
        if not self._entry_quote_probe_diagnostics_enabled():
            return
        payload = {
            "enabled": True,
            "candidate_scope": str(stats.get("candidate_scope", "") or ""),
            "candidate_count": int(candidate_count or 0),
            "all_target_count": int(stats.get("all_target_count", 0) or 0),
            "target_count": int(stats.get("target_count", 0) or 0),
            "must_resolve_count": int(stats.get("must_resolve_count", 0) or 0),
            "budgeted_target_count": int(stats.get("budgeted_target_count", 0) or 0),
            "budget_exhausted_count": int(stats.get("budget_exhausted_count", 0) or 0),
            "budget_excluded_without_rest_count": int(
                stats.get("budget_excluded_without_rest_count", 0) or 0
            ),
            "skipped_unbudgeted_count": int(stats.get("skipped_unbudgeted_count", 0) or 0),
            "skipped_untracked_count": int(stats.get("skipped_untracked_count", 0) or 0),
            "cache_initial_hit_count": int(stats.get("cache_initial_hit_count", 0) or 0),
            "cache_wait_hit_count": int(stats.get("cache_wait_hit_count", 0) or 0),
            "ws_resolved_count": int(stats.get("ws_resolved_count", 0) or 0),
            "rest_attempt_count": int(stats.get("rest_attempt_count", 0) or 0),
            "rest_resolved_count": int(stats.get("rest_resolved_count", 0) or 0),
            "rest_failed_count": int(stats.get("rest_failed_count", 0) or 0),
            "resolved_count": int(stats.get("resolved_count", 0) or 0),
            "failed_count": int(stats.get("failed_count", 0) or 0),
            "wait_budget_ms": int(stats.get("wait_budget_ms", 0) or 0),
            "wait_elapsed_ms": int(stats.get("wait_elapsed_ms", 0) or 0),
            "resolved_sources": dict(
                sorted((str(k), int(v)) for k, v in stats.get("sources", Counter()).items())
            ),
            "top_quote_blocker_buckets": dict(
                sorted(
                    (str(k), int(v))
                    for k, v in stats.get("top_quote_blocker_buckets", Counter()).items()
                )
            ),
            "ts_ms": now_ms,
        }
        self._append_runtime_diagnostic_event(
            "runtime.entry_quote_revalidate_probe",
            payload,
            now_ms=now_ms,
            key_parts=("entry_quote_revalidate",),
            interval_ms=1000,
        )

    def _entry_quote_truth_overlay_quote(
        self,
        overlay: dict[tuple[str, str], Any] | None,
        venue: str,
        symbol: str,
    ) -> Any | None:
        if not overlay:
            return None
        return overlay.get((str(venue or "").lower(), str(symbol or "").upper()))

    def _entry_quote_truth_market_quotes(
        self,
        market_quotes: Any,
        overlay: dict[tuple[str, str], Any] | None,
    ) -> dict:
        merged = dict(market_quotes or {})
        for (venue, symbol), quote in (overlay or {}).items():
            merged[f"{venue}:{symbol}"] = quote
        return merged

    def _entry_quote_truth_price_hint(
        self,
        candidate: Any,
        *,
        price_hints: dict[str, float],
        overlay: dict[tuple[str, str], Any] | None,
    ) -> float:
        symbol = str(getattr(candidate, "symbol", "") or "").upper()
        mids: list[float] = []
        for venue_attr in ("long_venue", "short_venue"):
            venue = str(getattr(candidate, venue_attr, "") or "").lower()
            quote = self._entry_quote_truth_overlay_quote(overlay, venue, symbol)
            if quote is None:
                continue
            bid = float(getattr(quote, "bid", 0.0) or 0.0)
            ask = float(getattr(quote, "ask", 0.0) or 0.0)
            if bid > 0.0 and ask > bid:
                mids.append((bid + ask) / 2.0)
        if mids:
            return sum(mids) / len(mids)
        return float(price_hints.get(symbol, 0.0) or 0.0)

    def _entry_quote_revalidate_need(
        self,
        *,
        snapshot,
        quote: Any,
        now_ms: int,
        fallback_source: str,
    ) -> tuple[bool, str, dict[str, Any]]:
        if quote is None:
            return False, "", {}
        observed_at_ms = self._snapshot_quote_observed_at_ms(snapshot, quote)
        direct_observed_at_ms = self._snapshot_quote_direct_observed_at_ms(quote)
        age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
        budget_ms = self._snapshot_domain_budget_ms("quote")
        bid = float(getattr(quote, "bid", 0.0) or 0.0)
        ask = float(getattr(quote, "ask", 0.0) or 0.0)
        if direct_observed_at_ms <= 0 or bid <= 0.0 or ask <= 0.0 or ask <= bid:
            return False, "", {}
        evidence = {
            "sidecar_source": self._snapshot_quote_source(quote),
            "sidecar_observed_at_ms": observed_at_ms,
            "sidecar_age_ms": age_ms,
            "sidecar_budget_ms": budget_ms,
            "fallback_source": fallback_source,
        }
        if age_ms > budget_ms:
            return True, "quote_stale", evidence
        if fallback_source == "last_good_sidecar":
            return True, "last_good_sidecar", evidence
        return False, "", {}

    def _entry_quote_revalidate_targets(
        self,
        candidates: list,
        *,
        snapshot,
        now_ms: int,
    ) -> list[dict[str, Any]]:
        quote_lookup = self._market_quote_lookup(getattr(snapshot, "quotes", {}) or {})
        fallback_source = self._snapshot_fallback_source(snapshot)
        targets: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for rank, candidate in enumerate(list(candidates or []), start=1):
            symbol = str(getattr(candidate, "symbol", "") or "").upper()
            if not symbol:
                continue
            for venue_attr in ("long_venue", "short_venue"):
                venue = str(getattr(candidate, venue_attr, "") or "").lower()
                if not venue:
                    continue
                key = (venue, symbol)
                if key in seen:
                    continue
                quote = quote_lookup.get(key)
                needs, reason, evidence = self._entry_quote_revalidate_need(
                    snapshot=snapshot,
                    quote=quote,
                    now_ms=now_ms,
                    fallback_source=fallback_source,
                )
                if not needs:
                    continue
                seen.add(key)
                targets.append({
                    "venue": venue,
                    "symbol": symbol,
                    "candidate_rank": rank,
                    "pair_id": self._candidate_pair_id(candidate),
                    "reason": reason,
                    **evidence,
                })
        return targets

    def _entry_quote_truth_fresh_quote(
        self,
        venue: str,
        symbol: str,
        *,
        now_ms: int,
    ) -> Any | None:
        cache = self.ctx.ws_bbo_cache
        if cache is None:
            return None
        budget_ms = self._entry_quote_lease_max_age_ms()
        if budget_ms <= 0:
            return None
        return cache.fresh_quote(venue, symbol, now_ms=now_ms, max_age_ms=budget_ms)

    def _entry_quote_truth_refresher(self) -> Any:
        refresher = getattr(self, "ws_bbo_rest_refresher", None)
        if refresher is not None:
            return refresher
        from lightfee.marketdata.ws_bbo import RestTopBookQuoteRefresher

        refresher = RestTopBookQuoteRefresher(timeout_ms=750)
        setattr(self, "ws_bbo_rest_refresher", refresher)
        return refresher

    def _entry_quote_truth_accept_quote(
        self,
        quote: Any,
        *,
        now_ms: int,
    ) -> bool:
        return self._entry_quote_truth_reject_reason(quote, now_ms=now_ms) == ""

    def _schedule_entry_quote_rewarm_after_rest_stale(
        self,
        target: dict[str, Any],
        *,
        now_ms: int,
    ) -> dict[str, Any] | None:
        venue = str(target.get("venue") or "").strip().lower()
        symbol = str(target.get("symbol") or "").strip().upper()
        if not venue or not symbol:
            return None
        key = (venue, symbol)
        budgets = phase_budgets_from_strategy(self.ctx.config.strategy)
        hard_ms = int(budgets["quote_rewarm"].hard_ms or 0)
        cooldown_until_ms: dict[tuple[str, str], int] = dict(
            getattr(self.ctx, "_entry_quote_rewarm_cooldown_until_ms", {}) or {}
        )
        if int(cooldown_until_ms.get(key, 0) or 0) > now_ms:
            return None
        scheduled_at_ms: dict[tuple[str, str], int] = dict(
            getattr(self.ctx, "_entry_quote_rewarm_scheduled_at_ms", {}) or {}
        )
        first_scheduled_at_ms = int(scheduled_at_ms.get(key, 0) or 0)
        if first_scheduled_at_ms > 0 and hard_ms > 0:
            age_ms = max(now_ms - first_scheduled_at_ms, 0)
            if age_ms >= hard_ms:
                cooldown_ttl_ms = max(
                    int(
                        getattr(
                            self.ctx.config.strategy,
                            "entry_ws_bbo_sticky_warm_ms",
                            120_000,
                        )
                        or 120_000
                    ),
                    self._entry_quote_lease_max_age_ms(),
                    hard_ms,
                )
                blocked_until_ms = now_ms + cooldown_ttl_ms
                cooldown_until_ms[key] = blocked_until_ms
                scheduled_at_ms.pop(key, None)
                setattr(
                    self.ctx,
                    "_entry_quote_rewarm_cooldown_until_ms",
                    cooldown_until_ms,
                )
                setattr(
                    self.ctx,
                    "_entry_quote_rewarm_scheduled_at_ms",
                    scheduled_at_ms,
                )
                handoff = quote_rewarm_handoff_contract(
                    phase="quote_rewarm",
                    status="hard_over_budget",
                    configured_action=budgets["quote_rewarm"].action,
                    terminal_kind="runtime.entry_quote_rewarm_terminal_stale",
                )
                payload = {
                    "venue": venue,
                    "symbol": symbol,
                    "pair_id": str(target.get("pair_id") or ""),
                    "candidate_rank": int(target.get("candidate_rank") or 0),
                    "reason_bucket": "rest_resolved_but_stale",
                    "reason_family": "rest_invalid_quote",
                    "scheduled_at_ms": first_scheduled_at_ms,
                    "age_ms": age_ms,
                    "hard_ms": hard_ms,
                    "blocked_until_ms": blocked_until_ms,
                    "cooldown_ttl_ms": cooldown_ttl_ms,
                    "action_taken": handoff.get(
                        "action_taken",
                        budgets["quote_rewarm"].action,
                    ),
                    "action_evidence_kind": handoff.get(
                        "action_evidence_kind",
                        "runtime.entry_quote_rewarm_terminal_stale",
                    ),
                    "source": "entry_quote_truth",
                    "ts_ms": now_ms,
                }
                self.ctx.journal.append(
                    "runtime.entry_quote_rewarm_terminal_stale",
                    payload,
                )
                return payload
        sticky_ttl_ms = max(
            int(
                getattr(
                    self.ctx.config.strategy,
                    "entry_ws_bbo_sticky_warm_ms",
                    120_000,
                )
                or 120_000
            ),
            self._entry_quote_lease_max_age_ms(),
        )
        sticky_warm_until_ms: dict[tuple[str, str], int] = dict(
            getattr(self.ctx, "_entry_bbo_sticky_warm_until_ms", {}) or {}
        )
        expires_at_ms = now_ms + sticky_ttl_ms
        sticky_warm_until_ms[(venue, symbol)] = expires_at_ms
        setattr(self.ctx, "_entry_bbo_sticky_warm_until_ms", sticky_warm_until_ms)
        scheduled_at_ms.setdefault(key, now_ms)
        setattr(self.ctx, "_entry_quote_rewarm_scheduled_at_ms", scheduled_at_ms)
        payload = {
            "venue": venue,
            "symbol": symbol,
            "pair_id": str(target.get("pair_id") or ""),
            "candidate_rank": int(target.get("candidate_rank") or 0),
            "reason_bucket": "rest_resolved_but_stale",
            "reason_family": "rest_invalid_quote",
            "sticky_warm_until_ms": expires_at_ms,
            "sticky_ttl_ms": sticky_ttl_ms,
            "rest_quote_observed_at_ms": target.get("rest_quote_observed_at_ms"),
            "rest_quote_received_at_ms": target.get("rest_quote_received_at_ms"),
            "rest_quote_exchange_event_at_ms": target.get(
                "rest_quote_exchange_event_at_ms"
            ),
            "rest_quote_age_ms": target.get("rest_quote_age_ms"),
            "quote_validation_reject_reason": str(
                target.get("quote_validation_reject_reason") or ""
            ),
            "source": "entry_quote_truth",
            "ts_ms": now_ms,
        }
        self.ctx.journal.append(
            "runtime.entry_quote_rewarm_scheduled_after_rest_stale",
            payload,
        )
        return payload

    def _entry_quote_truth_reject_reason(
        self,
        quote: Any,
        *,
        now_ms: int,
    ) -> str:
        if quote is None:
            return "missing_quote"
        observed_at_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
        age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
        bid = float(getattr(quote, "bid", 0.0) or 0.0)
        ask = float(getattr(quote, "ask", 0.0) or 0.0)
        if observed_at_ms <= 0:
            return "missing_observed_at"
        if age_ms > self._entry_quote_lease_max_age_ms():
            return "stale"
        if bid <= 0.0 or ask <= bid:
            return "invalid_bid_ask"
        return ""

    async def _entry_quote_revalidate_for_candidates(
        self,
        candidates: list,
        *,
        snapshot,
        now_ms: int,
        candidate_scope: str = "",
        skipped_untracked_count: int = 0,
    ) -> tuple[dict[tuple[str, str], Any], dict[str, Any]]:
        overlay: dict[tuple[str, str], Any] = {}
        stats = self._entry_quote_truth_empty_stats()
        stats["candidate_scope"] = candidate_scope
        stats["candidate_count"] = len(candidates or [])
        stats["skipped_untracked_count"] = max(int(skipped_untracked_count or 0), 0)
        if (
            not candidates
            or not self._entry_readiness_provider_uses_ws_bbo()
            or self.ctx.config.runtime.mode == "paper"
        ):
            self._entry_quote_truth_record_last_scan(stats)
            self._emit_entry_quote_revalidate_probe(
                stats=stats,
                candidate_count=len(candidates or []),
                now_ms=now_ms,
            )
            return overlay, stats

        await self._call_ensure_entry_bbo_active_for_candidates(candidates, now_ms)
        all_targets = self._entry_quote_revalidate_targets(
            candidates,
            snapshot=snapshot,
            now_ms=now_ms,
        )
        stats["all_target_count"] = len(all_targets)
        if not all_targets:
            self._entry_quote_truth_record_last_scan(stats)
            self._emit_entry_quote_revalidate_probe(
                stats=stats,
                candidate_count=len(candidates or []),
                now_ms=now_ms,
            )
            return overlay, stats

        budgeted_keys = set(getattr(self, "_entry_bbo_subscription_budgeted_keys", set()) or set())
        budget_excluded_keys = set(
            getattr(self, "_entry_bbo_subscription_budget_excluded_keys", set()) or set()
        )
        targets: list[dict[str, Any]] = []
        for target in all_targets:
            key = (target["venue"], target["symbol"])
            if key in budget_excluded_keys:
                stats["budget_exhausted_count"] += 1
                target["ws_budget_excluded"] = True
                target["rest_fallback_planned"] = True
                self.ctx.journal.append(
                    "runtime.entry_ws_bbo_top_candidate_rewarm_budget_exhausted",
                    {
                        **target,
                        "outcome": "rest_fallback_planned",
                        "ts_ms": now_ms,
                    },
                )
            if (
                budgeted_keys
                and key not in budgeted_keys
                and not bool(target.get("ws_budget_excluded"))
            ):
                stats["skipped_unbudgeted_count"] += 1
                continue
            targets.append(target)

        stats["target_count"] = len(targets)
        stats["must_resolve_count"] = len(targets)
        stats["budgeted_target_count"] = sum(
            1 for target in targets if not bool(target.get("ws_budget_excluded"))
        )
        if targets:
            self.ctx.journal.append(
                "runtime.entry_quote_revalidate_targeted",
                {
                    "target_count": len(targets),
                    "targets": targets[:24],
                    "wait_budget_ms": min(self._entry_quote_lease_max_age_ms(), 750),
                    "ts_ms": now_ms,
                },
            )
            self.ctx.journal.append(
                "runtime.entry_ws_bbo_top_candidate_rewarm_started",
                {
                    "target_count": len(targets),
                    "targets": targets[:24],
                    "ts_ms": now_ms,
                },
            )

        unresolved: dict[tuple[str, str], dict[str, Any]] = {
            (target["venue"], target["symbol"]): target
            for target in targets
        }

        def collect_fresh_from_cache(stage: str) -> None:
            for key, target in list(unresolved.items()):
                quote = self._entry_quote_truth_fresh_quote(
                    target["venue"],
                    target["symbol"],
                    now_ms=now_ms,
                )
                if quote is None:
                    continue
                overlay[key] = quote
                unresolved.pop(key, None)
                if stage == "initial":
                    stats["cache_initial_hit_count"] += 1
                else:
                    stats["cache_wait_hit_count"] += 1
                stats["ws_resolved_count"] += 1

        collect_fresh_from_cache("initial")
        wait_budget_ms = min(self._entry_quote_lease_max_age_ms(), 750)
        stats["wait_budget_ms"] = wait_budget_ms
        elapsed_ms = 0
        while unresolved and elapsed_ms < wait_budget_ms:
            await asyncio.sleep(0.05)
            elapsed_ms += 50
            collect_fresh_from_cache("wait")
        stats["wait_elapsed_ms"] = elapsed_ms

        refresher = self._entry_quote_truth_refresher()
        refresh_quote_result = getattr(refresher, "refresh_quote_result", None)
        refresh_quote = getattr(refresher, "refresh_quote", None)
        if callable(refresh_quote_result) or callable(refresh_quote):
            for key, target in list(unresolved.items()):
                stats["rest_attempt_count"] += 1
                refreshed = None
                try:
                    if callable(refresh_quote_result):
                        result = refresh_quote_result(
                            target["venue"],
                            target["symbol"],
                            now_ms=now_ms,
                        )
                        refreshed = getattr(result, "quote", None)
                        rest_outcome = str(getattr(result, "outcome", "") or "")
                        target["rest_outcome"] = rest_outcome
                        target["venue_symbol"] = str(
                            getattr(result, "venue_symbol", "") or ""
                        )
                        target["url"] = str(getattr(result, "url", "") or "")
                        target["endpoint"] = str(
                            getattr(result, "endpoint", "") or "rest_topbook"
                        )
                        target["http_status"] = int(
                            getattr(result, "http_status", 0) or 0
                        )
                        target["body_excerpt"] = str(
                            getattr(result, "body_excerpt", "") or ""
                        )
                        target["attempt_interval_outcome"] = str(
                            getattr(result, "attempt_interval_outcome", "") or ""
                        )
                        target["rest_error"] = str(getattr(result, "error", "") or "")
                        if rest_outcome == "throttled":
                            stats["rest_throttled_count"] += 1
                    else:
                        refreshed = refresh_quote(
                            target["venue"],
                            target["symbol"],
                            now_ms=now_ms,
                        )
                except Exception as exc:  # pragma: no cover - defensive telemetry
                    target["rest_error"] = f"{type(exc).__name__}: {exc}"[:240]
                    target["rest_outcome"] = "http_error"
                    refreshed = None
                reject_reason = self._entry_quote_truth_reject_reason(
                    refreshed,
                    now_ms=now_ms,
                )
                if refreshed is not None:
                    observed_at_ms = int(getattr(refreshed, "observed_at_ms", 0) or 0)
                    received_at_ms = int(getattr(refreshed, "received_at_ms", 0) or 0)
                    exchange_event_at_ms = int(
                        getattr(refreshed, "exchange_event_at_ms", 0) or 0
                    )
                    target["rest_quote_observed_at_ms"] = observed_at_ms
                    target["rest_quote_received_at_ms"] = received_at_ms
                    target["rest_quote_exchange_event_at_ms"] = exchange_event_at_ms
                    target["rest_quote_age_ms"] = (
                        max(now_ms - observed_at_ms, 0)
                        if observed_at_ms > 0
                        else None
                    )
                    target["rest_quote_bid"] = float(
                        getattr(refreshed, "bid", 0.0) or 0.0
                    )
                    target["rest_quote_ask"] = float(
                        getattr(refreshed, "ask", 0.0) or 0.0
                    )
                target["quote_validation_reject_reason"] = reject_reason
                if reject_reason:
                    stats["rest_failed_count"] += 1
                    continue
                cache = self.ctx.ws_bbo_cache
                if cache is not None and hasattr(cache, "update_quote"):
                    cache.update_quote(refreshed)
                overlay[key] = refreshed
                unresolved.pop(key, None)
                stats["rest_resolved_count"] += 1
        else:
            for target in unresolved.values():
                if bool(target.get("ws_budget_excluded")):
                    stats["budget_excluded_without_rest_count"] += 1

        for key, quote in overlay.items():
            source = str(getattr(quote, "source", "") or "entry_quote_truth")
            stats["resolved_count"] += 1
            stats["sources"][source] += 1
            target = next(
                (item for item in targets if (item["venue"], item["symbol"]) == key),
                {"venue": key[0], "symbol": key[1]},
            )
            payload = {
                **target,
                "source": source,
                "reason_bucket": "rest_resolved"
                if str(target.get("rest_outcome") or "")
                else "fresh_ws_quote",
                "observed_at_ms": int(getattr(quote, "observed_at_ms", 0) or 0),
                "received_at_ms": int(getattr(quote, "received_at_ms", 0) or 0),
                "exchange_event_at_ms": int(
                    getattr(quote, "exchange_event_at_ms", 0) or 0
                ),
                "age_ms": max(
                    now_ms - int(getattr(quote, "observed_at_ms", 0) or 0),
                    0,
                ),
                "quote_bid": float(getattr(quote, "bid", 0.0) or 0.0),
                "quote_ask": float(getattr(quote, "ask", 0.0) or 0.0),
                "outcome": "resolved",
                "ts_ms": now_ms,
            }
            self.ctx.journal.append("runtime.entry_quote_revalidate_resolved", payload)
            self.ctx.journal.append(
                "runtime.entry_ws_bbo_top_candidate_rewarm_succeeded",
                payload,
            )

        for target in unresolved.values():
            stats["failed_count"] += 1
            rest_outcome = str(target.get("rest_outcome") or "")
            stream_state: dict[str, Any] = {}
            data_plane = getattr(self.ctx, "ws_bbo_data_plane", None)
            if data_plane is not None and hasattr(data_plane, "stream_state"):
                try:
                    stream_state = data_plane.stream_state(
                        target["venue"],
                        target["symbol"],
                        now_ms=now_ms,
                        max_age_ms=self._entry_quote_lease_max_age_ms(),
                    )
                except TypeError:
                    stream_state = data_plane.stream_state(
                        target["venue"],
                        target["symbol"],
                    )
                except Exception:
                    stream_state = {}
            if bool(target.get("ws_budget_excluded")) and not (
                callable(refresh_quote_result) or callable(refresh_quote)
            ):
                outcome = "budget_excluded_rest_unavailable"
                bucket = "budget_excluded_without_rest"
                reason_bucket = "budget_excluded_without_rest"
                reason_family = reason_bucket
            elif rest_outcome == "throttled":
                outcome = "rest_attempt_throttled"
                bucket = "rest_topbook_attempt_throttled"
                reason_bucket = "rest_throttled"
                reason_family = reason_bucket
            elif rest_outcome == "unsupported_symbol":
                outcome = "rest_unsupported_symbol"
                bucket = "rest_topbook_unsupported_symbol"
                reason_bucket = "rest_unsupported_symbol"
                reason_family = "rest_invalid_quote"
            elif rest_outcome in {"http_error", "parse_error"}:
                outcome = f"rest_{rest_outcome}"
                bucket = "rest_topbook_revalidate_failed"
                reason_bucket = f"rest_{rest_outcome}"
                reason_family = "rest_invalid_quote"
            elif rest_outcome == "invalid_quote":
                outcome = "rest_invalid_quote"
                bucket = "rest_topbook_revalidate_failed"
                reason_bucket = "rest_resolved_invalid_bid_ask"
                reason_family = "rest_invalid_quote"
            elif target.get("rest_error"):
                outcome = "rest_timeout"
                bucket = "rest_topbook_revalidate_failed"
                reason_bucket = "rest_timeout_or_exception"
                reason_family = "rest_invalid_quote"
            elif rest_outcome == "resolved":
                outcome = "rest_invalid_quote"
                bucket = "rest_topbook_revalidate_failed"
                reject_reason = str(
                    target.get("quote_validation_reject_reason") or ""
                )
                if reject_reason == "stale":
                    reason_bucket = "rest_resolved_but_stale"
                elif reject_reason == "missing_observed_at":
                    reason_bucket = "rest_resolved_missing_observed_at"
                else:
                    reason_bucket = "rest_resolved_invalid_bid_ask"
                reason_family = "rest_invalid_quote"
            elif stats.get("rest_attempt_count", 0):
                outcome = "rest_invalid_quote"
                bucket = "rest_topbook_revalidate_failed"
                reason_bucket = "rest_timeout_or_exception"
                reason_family = "rest_invalid_quote"
            else:
                outcome = "ws_timeout"
                bucket = "quote_revalidate_unavailable"
                reason_bucket = str(
                    stream_state.get("reason_bucket")
                    or stream_state.get("lease_state")
                    or "not_tracked"
                )
                reason_family = reason_bucket
            stats["top_quote_blocker_buckets"][bucket] += 1
            stats["quote_lease_failure_counts"][reason_bucket] += 1
            age_ms = target.get("age_ms")
            if age_ms is None:
                age_ms = target.get("sidecar_age_ms")
            if age_ms is None:
                observed_at_ms = int(target.get("observed_at_ms") or 0)
                age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else None
            payload = {
                **target,
                "outcome": outcome,
                "reason_bucket": reason_bucket,
                "reason_family": reason_family,
                "quote_validation_reject_reason": str(
                    target.get("quote_validation_reject_reason") or ""
                ),
                "rest_quote_observed_at_ms": target.get("rest_quote_observed_at_ms"),
                "rest_quote_received_at_ms": target.get("rest_quote_received_at_ms"),
                "rest_quote_exchange_event_at_ms": target.get(
                    "rest_quote_exchange_event_at_ms"
                ),
                "rest_quote_age_ms": target.get("rest_quote_age_ms"),
                "rest_quote_bid": float(target.get("rest_quote_bid") or 0.0),
                "rest_quote_ask": float(target.get("rest_quote_ask") or 0.0),
                "source": "entry_quote_truth",
                "age_ms": age_ms,
                "budget_ms": self._entry_quote_lease_max_age_ms(),
                "lease_state": str(stream_state.get("lease_state") or ""),
                "stream_tracked": bool(stream_state.get("tracked")),
                "stream_subscribed": bool(stream_state.get("subscribed")),
                "stream_connected": bool(stream_state.get("connected")),
                "stream_message_count": int(stream_state.get("message_count") or 0),
                "last_quote_age_ms": stream_state.get("last_quote_age_ms"),
                "last_error": str(stream_state.get("last_error") or ""),
                "endpoint": str(target.get("endpoint") or "rest_topbook"),
                "venue_symbol": str(target.get("venue_symbol") or ""),
                "url": str(target.get("url") or ""),
                "http_status": int(target.get("http_status") or 0),
                "body_excerpt": str(target.get("body_excerpt") or ""),
                "attempt_interval_outcome": str(
                    target.get("attempt_interval_outcome") or ""
                ),
                "rest_error": str(target.get("rest_error") or ""),
                "ws_budget_excluded": bool(target.get("ws_budget_excluded")),
                "ts_ms": now_ms,
            }
            self.ctx.journal.append("runtime.entry_quote_revalidate_failed", payload)
            self.ctx.journal.append(
                "runtime.entry_ws_bbo_top_candidate_rewarm_failed",
                payload,
            )
            if reason_bucket == "rest_resolved_but_stale":
                self._schedule_entry_quote_rewarm_after_rest_stale(
                    payload,
                    now_ms=now_ms,
                )

        self._entry_quote_truth_record_last_scan(stats)
        self._emit_entry_quote_revalidate_probe(
            stats=stats,
            candidate_count=len(candidates or []),
            now_ms=now_ms,
        )
        return overlay, stats

    def _snapshot_local_l2_state(self) -> None:
        """Snapshot local-L2 runtime state into EngineState for persistence/recovery.

        V1: PersistedRetainedLocalL2Book with bids/asks + generation tracking.
        """
        if not self._local_l2_effective_enabled():
            self._clear_local_l2_runtime_state()
            return
        diag = self.ctx.local_l2_runtime.diagnostics_snapshot()
        # Retained books metadata (V1: persisted with full book data)
        self.ctx.state.retained_local_l2_books = [
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
            for b in self.ctx.local_l2_runtime.books.values()
            if b.pool == L2PoolAssignment.RETAINED
        ]
        # Full books snapshot for recovery
        self.ctx.state.local_l2_books_snapshot = [
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
            for b in self.ctx.local_l2_runtime.books.values()
        ]
        # Session snapshot
        self.ctx.state.local_l2_session_snapshot = [
            s.diagnostics_snapshot(now_ms=wall_clock_now_ms(), stale_after_ms=5000)
            for s in self.ctx.entry_l2_sessions.sessions.values()
        ]

    def _snapshot_domain_budget_ms(self, domain: str, row=None) -> int:
        domain_s = str(domain or "").lower()
        if domain_s == "liquidity":
            configured_ms = int(
                getattr(
                    self.ctx.config.runtime,
                    "sidecar_perp_liquidity_budget_ms",
                    self.ctx.config.strategy.max_liquidity_snapshot_age_ms,
                )
                or 0
            )
            refresh_ms = int(
                getattr(self.ctx.config.runtime, "sidecar_refresh_ms", 0) or 0
            )
            timeout_ms = int(
                float(
                    getattr(
                        self.ctx.config.runtime,
                        "sidecar_liquidity_timeout_s",
                        10.0,
                    )
                    or 0.0
                )
                * 1000.0
            )
            publish_interval_ms = (
                int(getattr(row, "publish_interval_ms", 0) or 0)
                if row is not None else 0
            )
            return int(
                max(
                    configured_ms,
                    int(self.ctx.config.strategy.max_liquidity_snapshot_age_ms or 0),
                    refresh_ms * 3 if refresh_ms > 0 else 0,
                    refresh_ms + timeout_ms * 2 if timeout_ms > 0 else 0,
                    publish_interval_ms * 2 if publish_interval_ms > 0 else 0,
                    30_000,
                )
            )
        if domain_s == "quote":
            return int(
                getattr(self.ctx.config.runtime, "max_order_quote_age_ms", 0)
                or self.ctx.config.runtime.max_market_age_ms
                or self.ctx.config.runtime.sidecar_snapshot_max_age_ms
            )
        if domain_s == "market":
            return int(
                getattr(self.ctx.config.runtime, "max_market_age_ms", 0)
                or self.ctx.config.runtime.sidecar_snapshot_max_age_ms
            )
        if domain_s == "funding":
            return int(self.ctx.config.runtime.sidecar_snapshot_max_age_ms)
        return int(self.ctx.config.runtime.sidecar_snapshot_max_age_ms)

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

    @staticmethod
    def _snapshot_quote_direct_observed_at_ms(quote) -> int:
        return int(getattr(quote, "observed_at_ms", 0) or 0)

    @staticmethod
    def _snapshot_quote_source(quote) -> str:
        return str(getattr(quote, "source", "") or "sidecar_quote")

    def _snapshot_quote_observed_at_ms(self, snapshot, quote) -> int:
        return (
            self._snapshot_quote_direct_observed_at_ms(quote)
            or int(getattr(snapshot, "market_observed_at_ms", 0) or 0)
            or int(getattr(snapshot, "published_at_ms", 0) or 0)
        )

    @staticmethod
    def _snapshot_scoped_status_key(
        domain: str,
        venue: str,
        symbol: str,
        source: str,
    ) -> str:
        return (
            f"{str(domain).lower()}|{str(venue).lower()}|"
            f"{str(symbol).upper()}|{str(source).lower()}"
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
    ) -> tuple[
        dict[str, dict[str, int]],
        dict[str, int],
        dict[str, int],
        dict[str, int],
        dict[str, dict],
    ]:
        metrics: dict[str, dict[str, int]] = {}
        ages: dict[str, int] = {}
        budgets: dict[str, int] = {}
        publish_intervals: dict[str, int] = {}
        statuses: dict[str, dict] = {}
        if snapshot is None:
            return metrics, ages, budgets, publish_intervals, statuses

        market_observed_at_ms = int(getattr(snapshot, "market_observed_at_ms", 0) or 0)
        market_age_ms = (
            max(now_ms - market_observed_at_ms, 0)
            if market_observed_at_ms > 0 else 0
        )
        market_budget_ms = int(
            getattr(self.ctx.config.runtime, "max_market_age_ms", 0)
            or self._snapshot_domain_budget_ms("market")
        )
        self._record_snapshot_scoped_status(
            statuses,
            domain="market",
            venue="global",
            symbol="*",
            source="snapshot.market_observed_at_ms",
            observed_at_ms=market_observed_at_ms,
            age_ms=market_age_ms,
            budget_ms=market_budget_ms,
            fresh=market_observed_at_ms > 0 and market_age_ms <= market_budget_ms,
        )

        for quote in getattr(snapshot, "quotes", {}).values():
            venue = str(getattr(quote, "venue", "") or "").lower()
            symbol = str(getattr(quote, "symbol", "") or "").upper()
            if not venue or not symbol:
                continue
            observed_at_ms = self._snapshot_quote_observed_at_ms(snapshot, quote)
            age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
            budget_ms = self._snapshot_domain_budget_ms("quote")
            key = self._snapshot_metric_key(venue, symbol, "quote")
            fresh = observed_at_ms > 0 and age_ms <= budget_ms
            self._record_snapshot_metric(metrics, key, fresh)
            ages[key] = age_ms
            budgets[key] = budget_ms
            source = self._snapshot_quote_source(quote)
            if self._snapshot_quote_direct_observed_at_ms(quote) <= 0:
                source = "snapshot.market_observed_at_ms"
            self._record_snapshot_scoped_status(
                statuses,
                domain="quote",
                venue=venue,
                symbol=symbol,
                source=source,
                observed_at_ms=observed_at_ms,
                age_ms=age_ms,
                budget_ms=budget_ms,
                fresh=fresh,
            )

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
                    budget_ms = self._snapshot_domain_budget_ms(domain, row)
                    key = self._snapshot_metric_key(venue, symbol, domain)
                    fresh = observed_at_ms > 0 and age_ms <= budget_ms
                    self._record_snapshot_metric(metrics, key, fresh)
                    ages[key] = age_ms
                    budgets[key] = budget_ms
                    self._record_snapshot_scoped_status(
                        statuses,
                        domain=domain,
                        venue=venue,
                        symbol=symbol,
                        source=str(
                            getattr(row, "source", f"sidecar_{domain}") or f"sidecar_{domain}"
                        ),
                        observed_at_ms=observed_at_ms,
                        age_ms=age_ms,
                        budget_ms=budget_ms,
                        fresh=fresh,
                    )
                    if domain == "liquidity":
                        publish_intervals[key] = int(
                            getattr(row, "publish_interval_ms", 0) or 0
                        )

        transfer_rows = getattr(snapshot, "transfer_lifecycle", []) or []
        candidate_symbols = {
            str(getattr(candidate, "symbol", "") or "").upper()
            for candidate in candidates
            if str(getattr(candidate, "symbol", "") or "")
        } or {"*"}
        for row in transfer_rows:
            from_venue = str(getattr(row, "from_venue", "") or "").lower()
            to_venue = str(getattr(row, "to_venue", "") or "").lower()
            if not from_venue or not to_venue:
                continue
            observed_at_ms = int(getattr(row, "observed_at_ms", 0) or 0)
            age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
            budget_ms = self._snapshot_domain_budget_ms("transfer", row)
            venue = f"{from_venue}->{to_venue}"
            for symbol in sorted(candidate_symbols):
                self._record_snapshot_scoped_status(
                    statuses,
                    domain="transfer",
                    venue=venue,
                    symbol=symbol,
                    source=str(getattr(row, "source", "") or "sidecar_transfer"),
                    observed_at_ms=observed_at_ms,
                    age_ms=age_ms,
                    budget_ms=budget_ms,
                    fresh=observed_at_ms > 0 and age_ms <= budget_ms,
                )

        return metrics, ages, budgets, publish_intervals, statuses

    def _candidate_snapshot_freshness_decisions(
        self,
        candidate,
        *,
        snapshot,
        now_ms: int,
        record_liquidity_qualification: bool = False,
        entry_quote_truth_overlay: dict[tuple[str, str], Any] | None = None,
    ) -> list[dict]:
        if snapshot is None:
            return []
        quote_lookup = self._market_quote_lookup(getattr(snapshot, "quotes", {}) or {})
        liquidity_rows = self._snapshot_lifecycle_rows_by_venue(snapshot, "liquidity")
        fallback_source = self._snapshot_fallback_source(snapshot)
        decisions: list[dict] = []
        symbol = str(getattr(candidate, "symbol", "") or "").upper()
        requires_sidecar_liquidity = (
            self._candidate_requires_sidecar_perp_liquidity(candidate)
        )

        for venue_attr in ("long_venue", "short_venue"):
            venue = str(getattr(candidate, venue_attr, "") or "").lower()
            if not venue or not symbol:
                continue

            quote = quote_lookup.get((venue, symbol))
            quote_budget_ms = self._snapshot_domain_budget_ms("quote")
            if quote is None:
                decisions.append({
                    "venue": venue,
                    "symbol": symbol,
                    "domain": "quote",
                    "source": "sidecar_quote",
                    "observed_at_ms": 0,
                    "age_ms": 0,
                    "budget_ms": quote_budget_ms,
                    "decision": "skip_entry",
                    "fallback_source": fallback_source,
                    "reason": "missing_quote",
                    "blocking": True,
                })
            else:
                observed_at_ms = self._snapshot_quote_observed_at_ms(snapshot, quote)
                age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
                source = self._snapshot_quote_source(quote)
                if self._snapshot_quote_direct_observed_at_ms(quote) <= 0:
                    source = "snapshot.market_observed_at_ms"
                bid = float(getattr(quote, "bid", 0.0) or 0.0)
                ask = float(getattr(quote, "ask", 0.0) or 0.0)
                overlay_quote = self._entry_quote_truth_overlay_quote(
                    entry_quote_truth_overlay,
                    venue,
                    symbol,
                )
                overlay_resolved = self._entry_quote_truth_decision(
                    venue=venue,
                    symbol=symbol,
                    quote=overlay_quote,
                    now_ms=now_ms,
                    fallback_source=fallback_source,
                    sidecar_source=source,
                    sidecar_observed_at_ms=observed_at_ms,
                    sidecar_age_ms=age_ms,
                    sidecar_budget_ms=quote_budget_ms,
                    sidecar_reason=(
                        "last_good_sidecar"
                        if fallback_source == "last_good_sidecar"
                        else "quote_stale"
                        if age_ms > quote_budget_ms
                        else "fresh_sidecar"
                    ),
                )
                if (
                    observed_at_ms <= 0
                    or age_ms > quote_budget_ms
                    or bid <= 0.0
                    or ask <= 0.0
                    or ask <= bid
                ):
                    reason = "quote_stale" if age_ms > quote_budget_ms else "invalid_quote"
                    if reason == "quote_stale" and overlay_resolved is not None:
                        decisions.append(overlay_resolved)
                        continue
                    if reason == "quote_stale":
                        resolved = self._ws_bbo_entry_quote_resolution(
                            venue=venue,
                            symbol=symbol,
                            now_ms=now_ms,
                            sidecar_reason=reason,
                            sidecar_source=source,
                            sidecar_observed_at_ms=observed_at_ms,
                            sidecar_age_ms=age_ms,
                            sidecar_budget_ms=quote_budget_ms,
                            fallback_source=fallback_source,
                        )
                        if resolved is not None:
                            decisions.append(resolved)
                            continue
                    payload = {
                        "venue": venue,
                        "symbol": symbol,
                        "domain": "quote",
                        "source": source,
                        "observed_at_ms": observed_at_ms,
                        "age_ms": age_ms,
                        "budget_ms": quote_budget_ms,
                        "decision": "skip_entry",
                        "fallback_source": fallback_source,
                        "reason": reason,
                        "blocking": True,
                    }
                    payload.update(
                        self._snapshot_quote_evidence(
                            quote=quote,
                            observed_at_ms=observed_at_ms,
                            age_ms=age_ms,
                            budget_ms=quote_budget_ms,
                        )
                    )
                    if reason == "quote_stale":
                        payload["event_kind"] = "runtime.quote_stale"
                    decisions.append(payload)
                elif (
                    fallback_source == "last_good_sidecar"
                    and self._entry_readiness_provider_uses_ws_bbo()
                ):
                    if overlay_resolved is not None:
                        decisions.append(overlay_resolved)
                        continue
                    payload = {
                        "venue": venue,
                        "symbol": symbol,
                        "domain": "quote",
                        "source": source,
                        "observed_at_ms": observed_at_ms,
                        "age_ms": age_ms,
                        "budget_ms": quote_budget_ms,
                        "decision": "skip_entry",
                        "fallback_source": fallback_source,
                        "reason": "last_good_sidecar_revalidate_required",
                        "blocking": True,
                        "event_kind": "runtime.entry_quote_revalidate_failed",
                    }
                    payload.update(
                        self._snapshot_quote_evidence(
                            quote=quote,
                            observed_at_ms=observed_at_ms,
                            age_ms=age_ms,
                            budget_ms=quote_budget_ms,
                        )
                    )
                    decisions.append(payload)

            liquidity = liquidity_rows.get(venue)
            liq_budget_ms = self._snapshot_domain_budget_ms("liquidity", liquidity)
            liq_observed_at_ms = (
                int(getattr(liquidity, "observed_at_ms", 0) or 0)
                if liquidity is not None else 0
            )
            liq_coverage_usable = (
                int(getattr(liquidity, "coverage_usable", 0) or 0)
                if liquidity is not None else 0
            )
            liq_degraded_reason = (
                str(getattr(liquidity, "degraded_reason", "") or "")
                if liquidity is not None else ""
            )
            liq_degraded_blocks_symbol = (
                self._liquidity_degraded_reason_blocks_symbol(
                    liq_degraded_reason, symbol
                )
            )
            liq_age_ms = (
                max(now_ms - liq_observed_at_ms, 0)
                if liq_observed_at_ms > 0 else 0
            )
            liq_stale_or_missing = (
                liquidity is None
                or liq_observed_at_ms <= 0
                or liq_age_ms > liq_budget_ms
                or liq_coverage_usable <= 0
                or liq_degraded_blocks_symbol
            )
            if liq_stale_or_missing:
                reason = (
                    "perp_liquidity_stale_blocking"
                    if requires_sidecar_liquidity
                    else "perp_liquidity_stale_advisory"
                )
                decisions.append(
                    self._liquidity_lifecycle_payload(
                        row=liquidity,
                        venue=venue,
                        symbol=symbol,
                        now_ms=now_ms,
                        decision="skip_entry" if requires_sidecar_liquidity else "continue",
                        reason=reason,
                        fallback_source=fallback_source,
                    )
                )

        decisions.extend(
            self._entry_liquidity_qualification_decisions(
                candidate,
                snapshot=snapshot,
                quote_lookup=quote_lookup,
                now_ms=now_ms,
                fallback_source=fallback_source,
                record_result=record_liquidity_qualification,
            )
        )

        return decisions

    def _entry_open_interest_refresher(self) -> Any:
        refresher = getattr(self, "entry_open_interest_refresher", None)
        if refresher is not None:
            return refresher
        refresher = EntryOpenInterestRefresher()
        setattr(self, "entry_open_interest_refresher", refresher)
        return refresher

    async def _refresh_entry_candidate_open_interest_evidence(
        self,
        candidates: list,
        *,
        snapshot,
        now_ms: int,
    ) -> dict[str, Any]:
        stats = {
            "candidate_count": len(candidates or []),
            "target_count": 0,
            "attempt_count": 0,
            "resolved_count": 0,
            "failed_count": 0,
            "unsupported_count": 0,
            "timeout_count": 0,
            "blocked_after_targeted_refresh_count": 0,
            "targets": [],
        }
        if (
            not candidates
            or snapshot is None
            or str(getattr(self.ctx.config.runtime, "mode", "") or "").lower() != "live"
            or not bool(getattr(self.ctx.config.strategy, "execution_liquidity_enabled", True))
        ):
            return stats
        if getattr(self.ctx.state, "last_scan", None) is None:
            self.ctx.state.last_scan = {}

        quote_lookup = self._market_quote_lookup(getattr(snapshot, "quotes", {}) or {})
        targets: list[tuple[str, str, Any, str]] = []
        seen: set[tuple[str, str]] = set()
        for candidate in list(candidates or []):
            symbol = str(getattr(candidate, "symbol", "") or "").strip().upper()
            if not symbol:
                continue
            for venue_attr in ("long_venue", "short_venue"):
                venue = str(getattr(candidate, venue_attr, "") or "").strip().lower()
                if venue not in EntryOpenInterestRefresher.SUPPORTED_VENUES:
                    continue
                floor_getter = getattr(
                    self.ctx,
                    "_entry_liquidity_open_interest_floor_quote",
                    None,
                )
                if callable(floor_getter) and floor_getter(venue) <= 0.0:
                    continue
                key = (venue, symbol)
                if key in seen:
                    continue
                quote = quote_lookup.get(key)
                if quote is None:
                    continue
                evidence_status = str(
                    getattr(quote, "open_interest_evidence_status", "available")
                    or "available"
                ).lower()
                if evidence_status == "available":
                    continue
                seen.add(key)
                targets.append((venue, symbol, quote, evidence_status))

        stats["target_count"] = len(targets)
        stats["targets"] = [
            {
                "venue": venue,
                "symbol": symbol,
                "open_interest_evidence_status": status,
            }
            for venue, symbol, _quote, status in targets[:24]
        ]
        if not targets:
            self.ctx.state.last_scan["oi_targeted_refresh_attempt_count"] = 0
            self.ctx.state.last_scan["oi_targeted_refresh_resolved_count"] = 0
            self.ctx.state.last_scan["oi_targeted_refresh_failed_count"] = 0
            return stats

        refresher = self._entry_open_interest_refresher()
        refresh = getattr(refresher, "refresh_open_interest", None)
        batch_refresh = getattr(refresher, "refresh_open_interest_batch", None)
        if not callable(refresh) and not callable(batch_refresh):
            return stats

        self.ctx.journal.append(
            "runtime.entry_oi_targeted_refresh_started",
            {
                "target_count": len(targets),
                "targets": stats["targets"],
                "ts_ms": now_ms,
            },
        )
        def _apply_refresh_result(
            *,
            venue: str,
            symbol: str,
            quote,
            previous_status: str,
            result: dict[str, Any] | None,
            default_elapsed_ms: int,
        ) -> None:
            stats["attempt_count"] += 1
            elapsed_ms = int(
                (result or {}).get(
                    "oi_targeted_refresh_elapsed_ms",
                    (result or {}).get("oi_refresh_elapsed_ms", default_elapsed_ms),
                )
                or 0
            )
            status = str(
                (result or {}).get("open_interest_evidence_status")
                or "unavailable"
            ).lower()
            reason = str(
                (result or {}).get("open_interest_evidence_reason")
                or status
            )
            open_interest_quote = float(
                (result or {}).get("open_interest_quote", 0.0) or 0.0
            )
            payload = {
                "venue": venue,
                "symbol": symbol,
                "previous_open_interest_evidence_status": previous_status,
                "open_interest_evidence_status": status,
                "open_interest_evidence_reason": reason,
                "open_interest_quote": open_interest_quote,
                "elapsed_ms": elapsed_ms,
                "ts_ms": now_ms,
            }
            if status == "available":
                quote.open_interest = open_interest_quote
                quote.open_interest_evidence_status = "available"
                quote.open_interest_evidence_reason = reason or "targeted_refresh"
                for field in (
                    "oi_candidate_count",
                    "oi_cache_hit_count",
                    "oi_cache_miss_count",
                    "oi_refresh_attempt_count",
                    "oi_refresh_cap",
                    "oi_deferred_count",
                    "oi_timeout_count",
                    "oi_refresh_elapsed_ms",
                ):
                    if field in (result or {}):
                        setattr(quote, field, int((result or {}).get(field) or 0))
                stats["resolved_count"] += 1
                self.ctx.journal.append(
                    "runtime.entry_oi_targeted_refresh_resolved",
                    payload,
                )
            else:
                quote.open_interest_evidence_status = status
                quote.open_interest_evidence_reason = reason
                stats["failed_count"] += 1
                stats["blocked_after_targeted_refresh_count"] += 1
                if status == "timeout":
                    stats["timeout_count"] += 1
                if status == "unsupported":
                    stats["unsupported_count"] += 1
                self.ctx.journal.append(
                    "runtime.entry_oi_targeted_refresh_failed",
                    payload,
                )

        if callable(batch_refresh):
            grouped: dict[str, list[tuple[str, Any, str]]] = {}
            for venue, symbol, quote, previous_status in targets:
                grouped.setdefault(venue, []).append((symbol, quote, previous_status))
            for venue, entries in grouped.items():
                symbols = [symbol for symbol, _quote, _status in entries]
                started_ms = wall_clock_now_ms()
                try:
                    batch_results = await batch_refresh(venue, symbols, now_ms=now_ms)
                except Exception as exc:  # pragma: no cover - defensive telemetry
                    batch_results = {
                        symbol: {
                            "open_interest_quote": 0.0,
                            "open_interest_evidence_status": "timeout",
                            "open_interest_evidence_reason": f"{type(exc).__name__}: {exc}"[:200],
                        }
                        for symbol in symbols
                    }
                elapsed_ms = max(wall_clock_now_ms() - started_ms, 0)
                for symbol, quote, previous_status in entries:
                    result = (batch_results or {}).get(symbol)
                    if result is None:
                        result = {
                            "open_interest_quote": 0.0,
                            "open_interest_evidence_status": "parse_error",
                            "open_interest_evidence_reason": "missing_targeted_ticker",
                        }
                    _apply_refresh_result(
                        venue=venue,
                        symbol=symbol,
                        quote=quote,
                        previous_status=previous_status,
                        result=result,
                        default_elapsed_ms=elapsed_ms,
                    )
        else:
            for venue, symbol, quote, previous_status in targets:
                started_ms = wall_clock_now_ms()
                try:
                    result = await refresh(venue, symbol, now_ms=now_ms)
                except Exception as exc:  # pragma: no cover - defensive telemetry
                    result = {
                        "open_interest_quote": 0.0,
                        "open_interest_evidence_status": "timeout",
                        "open_interest_evidence_reason": f"{type(exc).__name__}: {exc}"[:200],
                    }
                _apply_refresh_result(
                    venue=venue,
                    symbol=symbol,
                    quote=quote,
                    previous_status=previous_status,
                    result=result,
                    default_elapsed_ms=max(wall_clock_now_ms() - started_ms, 0),
                )

        self.ctx.state.last_scan["oi_targeted_refresh_attempt_count"] = stats[
            "attempt_count"
        ]
        self.ctx.state.last_scan["oi_targeted_refresh_resolved_count"] = stats[
            "resolved_count"
        ]
        self.ctx.state.last_scan["oi_targeted_refresh_failed_count"] = stats[
            "failed_count"
        ]
        self.ctx.state.last_scan["oi_targeted_refresh_timeout_count"] = stats[
            "timeout_count"
        ]
        self.ctx.state.last_scan["oi_targeted_refresh_unsupported_count"] = stats[
            "unsupported_count"
        ]
        self.ctx.state.last_scan[
            "entry_blocked_after_targeted_refresh_count"
        ] = stats["blocked_after_targeted_refresh_count"]
        return stats

    @staticmethod
    def _snapshot_quote_evidence(
        *,
        quote,
        observed_at_ms: int,
        age_ms: int,
        budget_ms: int,
    ) -> dict:
        bid = float(getattr(quote, "bid", 0.0) or 0.0)
        ask = float(getattr(quote, "ask", 0.0) or 0.0)
        bid_size = float(getattr(quote, "bid_size", 0.0) or 0.0)
        ask_size = float(getattr(quote, "ask_size", 0.0) or 0.0)
        invalid_fields: list[str] = []
        if observed_at_ms <= 0:
            invalid_fields.append("observed_at_ms")
        if age_ms > budget_ms:
            invalid_fields.append("age_ms")
        if bid <= 0.0:
            invalid_fields.append("bid")
        if ask <= 0.0:
            invalid_fields.append("ask")
        if bid_size <= 0.0:
            invalid_fields.append("bid_size")
        if ask_size <= 0.0:
            invalid_fields.append("ask_size")
        return {
            "quote_bid": bid,
            "quote_ask": ask,
            "quote_bid_size": bid_size,
            "quote_ask_size": ask_size,
            "quote_mark_price": float(getattr(quote, "mark_price", 0.0) or 0.0),
            "quote_index_price": float(getattr(quote, "index_price", 0.0) or 0.0),
            "quote_funding_timestamp_ms": int(
                getattr(quote, "funding_timestamp_ms", 0) or 0
            ),
            "invalid_quote_fields": invalid_fields,
        }

    def _ws_bbo_entry_quote_resolution(
        self,
        *,
        venue: str,
        symbol: str,
        now_ms: int,
        sidecar_reason: str,
        sidecar_source: str,
        sidecar_observed_at_ms: int,
        sidecar_age_ms: int,
        sidecar_budget_ms: int,
        fallback_source: str,
    ) -> dict | None:
        if sidecar_reason != "quote_stale":
            return None
        if not self._entry_readiness_provider_uses_ws_bbo():
            return None
        cache = self.ctx.ws_bbo_cache
        if cache is None:
            return None
        budget_ms = self._entry_quote_lease_max_age_ms()
        if budget_ms <= 0:
            return None
        quote = cache.fresh_quote(
            venue,
            symbol,
            now_ms=now_ms,
            max_age_ms=budget_ms,
        )
        if quote is None:
            return None
        observed_at_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
        age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
        bid = float(getattr(quote, "bid", 0.0) or 0.0)
        ask = float(getattr(quote, "ask", 0.0) or 0.0)
        if observed_at_ms <= 0 or age_ms > budget_ms or bid <= 0.0 or ask <= bid:
            return None
        return {
            "venue": str(venue or "").lower(),
            "symbol": str(symbol or "").upper(),
            "domain": "quote",
            "source": "ws_bbo_quote_lease",
            "provider": "ws_bbo_quote_lease",
            "observed_at_ms": observed_at_ms,
            "age_ms": age_ms,
            "budget_ms": budget_ms,
            "decision": "continue",
            "fallback_source": fallback_source,
            "reason": f"{sidecar_reason}_resolved_by_ws_bbo",
            "blocking": False,
            "event_kind": "runtime.entry_quote_evidence_resolved_by_ws_bbo",
            "sidecar_reason": str(sidecar_reason or ""),
            "sidecar_source": str(sidecar_source or ""),
            "sidecar_observed_at_ms": int(sidecar_observed_at_ms or 0),
            "sidecar_age_ms": int(sidecar_age_ms or 0),
            "sidecar_budget_ms": int(sidecar_budget_ms or 0),
            "ws_bbo_source": str(getattr(quote, "source", "") or ""),
            "quote_bid": bid,
            "quote_ask": ask,
            "quote_bid_size": float(getattr(quote, "bid_size", 0.0) or 0.0),
            "quote_ask_size": float(getattr(quote, "ask_size", 0.0) or 0.0),
            "invalid_quote_fields": [],
            "blocker_family": "quote_evidence_resolved",
            "metric_fresh": True,
        }

    def _entry_quote_truth_decision(
        self,
        *,
        venue: str,
        symbol: str,
        quote: Any | None,
        now_ms: int,
        fallback_source: str,
        sidecar_source: str,
        sidecar_observed_at_ms: int,
        sidecar_age_ms: int,
        sidecar_budget_ms: int,
        sidecar_reason: str,
    ) -> dict | None:
        if not self._entry_quote_truth_accept_quote(quote, now_ms=now_ms):
            return None
        observed_at_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
        age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
        bid = float(getattr(quote, "bid", 0.0) or 0.0)
        ask = float(getattr(quote, "ask", 0.0) or 0.0)
        reason = (
            "last_good_revalidated_by_entry_quote_truth"
            if sidecar_reason == "last_good_sidecar"
            else f"{sidecar_reason}_resolved_by_entry_quote_truth"
        )
        event_kind = (
            "runtime.last_good_revalidated_by_entry_quote_truth"
            if sidecar_reason == "last_good_sidecar"
            else "runtime.entry_quote_evidence_resolved_by_ws_bbo"
        )
        return {
            "venue": str(venue or "").lower(),
            "symbol": str(symbol or "").upper(),
            "domain": "quote",
            "source": "entry_quote_truth",
            "provider": "entry_quote_revalidator",
            "observed_at_ms": observed_at_ms,
            "age_ms": age_ms,
            "budget_ms": self._entry_quote_lease_max_age_ms(),
            "decision": "continue",
            "fallback_source": fallback_source,
            "reason": reason,
            "blocking": False,
            "event_kind": event_kind,
            "sidecar_reason": str(sidecar_reason or ""),
            "sidecar_source": str(sidecar_source or ""),
            "sidecar_observed_at_ms": int(sidecar_observed_at_ms or 0),
            "sidecar_age_ms": int(sidecar_age_ms or 0),
            "sidecar_budget_ms": int(sidecar_budget_ms or 0),
            "entry_quote_truth_source": str(getattr(quote, "source", "") or ""),
            "quote_bid": bid,
            "quote_ask": ask,
            "quote_bid_size": float(getattr(quote, "bid_size", 0.0) or 0.0),
            "quote_ask_size": float(getattr(quote, "ask_size", 0.0) or 0.0),
            "invalid_quote_fields": [],
            "blocker_family": "quote_revalidate_resolved",
            "metric_fresh": True,
        }

    @staticmethod
    def _snapshot_freshness_evidence_fields(decision: dict) -> dict:
        keys = (
            "quote_bid",
            "quote_ask",
            "quote_bid_size",
            "quote_ask_size",
            "quote_mark_price",
            "quote_index_price",
            "quote_funding_timestamp_ms",
            "invalid_quote_fields",
            "observed_volume_24h_quote",
            "min_volume_24h_quote",
            "observed_open_interest_quote",
            "min_open_interest_quote",
            "eligibility_class",
            "consecutive_failures",
            "suppress_until_ms",
            "last_failure_at_ms",
            "last_structural_probe_at_ms",
        )
        return {key: decision[key] for key in keys if key in decision}

    def _candidate_snapshot_freshness_failures(
        self,
        candidate,
        *,
        snapshot,
        now_ms: int,
        entry_quote_truth_overlay: dict[tuple[str, str], Any] | None = None,
    ) -> list[dict]:
        return [
            decision
            for decision in self._call_candidate_snapshot_freshness_decisions(
                candidate,
                snapshot=snapshot,
                now_ms=now_ms,
                entry_quote_truth_overlay=entry_quote_truth_overlay,
            )
            if decision.get("decision") == "skip_entry"
        ]

    def _snapshot_fallback_duration_ms(
        self,
        *,
        snapshot,
        now_ms: int,
        max_age_ms: int | None = None,
    ) -> int:
        if snapshot is None:
            return 0
        snapshot_max_age_ms = int(
            max_age_ms
            if max_age_ms is not None
            else self.ctx.config.runtime.sidecar_snapshot_max_age_ms
        )
        market_max_age_ms = int(
            getattr(self.ctx.config.runtime, "max_market_age_ms", snapshot_max_age_ms)
            or snapshot_max_age_ms
        )
        stale_overages: list[int] = []
        published_at_ms = int(getattr(snapshot, "published_at_ms", 0) or 0)
        market_observed_at_ms = int(getattr(snapshot, "market_observed_at_ms", 0) or 0)
        if published_at_ms > 0:
            snapshot_publish_age_ms = max(now_ms - published_at_ms, 0)
            if snapshot_publish_age_ms > snapshot_max_age_ms:
                stale_overages.append(snapshot_publish_age_ms - snapshot_max_age_ms)
        if market_observed_at_ms > 0:
            market_observed_age_ms = max(now_ms - market_observed_at_ms, 0)
            if market_observed_age_ms > market_max_age_ms:
                stale_overages.append(market_observed_age_ms - market_max_age_ms)
        return max(stale_overages) if stale_overages else 0

    def _snapshot_candidate_scope_sample(
        self,
        *,
        candidate,
        domain: str,
        venue: str,
        source: str,
        source_age_ms: int,
        fallback_duration_ms: int,
        blocked: bool,
        block_reason: str = "",
    ) -> dict:
        symbol = str(getattr(candidate, "symbol", "") or "").upper()
        return {
            "candidate_symbol": symbol,
            "candidate_pair_id": self._candidate_pair_id(candidate),
            "domain": str(domain or "").lower(),
            "venue": str(venue or "").lower(),
            "source": str(source or ""),
            "source_age_ms": int(source_age_ms or 0),
            "fallback_duration_ms": int(fallback_duration_ms or 0),
            "blocked": bool(blocked),
            "block_reason": str(block_reason or "") if blocked else "",
        }

    @staticmethod
    def _canonical_degraded_domain(domain: str) -> str:
        domain_s = str(domain or "").lower()
        if domain_s == "market_observed_stale":
            return "market_observed"
        if domain_s == "snapshot_publish_stale":
            return "snapshot_publish"
        if domain_s.endswith("_stale"):
            return domain_s[:-6]
        return domain_s

    def _snapshot_health_candidate_freshness_scope(
        self,
        *,
        snapshot,
        now_ms: int,
        degraded_domains: list[str],
        stale_degraded_domains: list[str],
        fallback_duration_ms: int,
        candidates: list | None = None,
    ) -> list[dict]:
        scope: list[dict] = []
        if snapshot is None:
            return scope

        candidates = (
            list(candidates)
            if candidates is not None
            else list(getattr(snapshot, "candidates", []) or [])
        )
        if not candidates:
            return scope

        seen: set[tuple[str, str, str, str, str]] = set()

        def add_sample(sample: dict) -> None:
            marker = (
                str(sample.get("candidate_pair_id", "")),
                str(sample.get("candidate_symbol", "")),
                str(sample.get("domain", "")),
                str(sample.get("venue", "")),
                str(sample.get("source", "")),
            )
            if marker in seen or len(scope) >= 48:
                return
            seen.add(marker)
            scope.append(sample)

        all_domains = [
            self._canonical_degraded_domain(domain)
            for domain in list(degraded_domains) + list(stale_degraded_domains)
        ]
        market_observed_age_ms = max(
            now_ms - int(getattr(snapshot, "market_observed_at_ms", 0) or 0),
            0,
        )
        snapshot_publish_age_ms = max(
            now_ms - int(getattr(snapshot, "published_at_ms", 0) or 0),
            0,
        )
        for candidate in candidates:
            if "market_observed" in all_domains:
                add_sample(
                    self._snapshot_candidate_scope_sample(
                        candidate=candidate,
                        domain="market_observed",
                        venue="global",
                        source="snapshot.market_observed_at_ms",
                        source_age_ms=market_observed_age_ms,
                        fallback_duration_ms=fallback_duration_ms,
                        blocked=False,
                    )
                )
            if "snapshot_publish" in all_domains:
                add_sample(
                    self._snapshot_candidate_scope_sample(
                        candidate=candidate,
                        domain="snapshot_publish",
                        venue="global",
                        source="snapshot.published_at_ms",
                        source_age_ms=snapshot_publish_age_ms,
                        fallback_duration_ms=fallback_duration_ms,
                        blocked=False,
                    )
                )

            for decision in self._call_candidate_snapshot_freshness_decisions(
                candidate,
                snapshot=snapshot,
                now_ms=now_ms,
            ):
                blocked = bool(
                    decision.get("blocking", False)
                    or decision.get("decision") == "skip_entry"
                )
                sample = self._snapshot_candidate_scope_sample(
                    candidate=candidate,
                    domain=str(decision.get("domain", "")),
                    venue=str(decision.get("venue", "")),
                    source=str(decision.get("source", "")),
                    source_age_ms=int(decision.get("age_ms", 0) or 0),
                    fallback_duration_ms=fallback_duration_ms,
                    blocked=blocked,
                    block_reason=str(decision.get("reason", "")),
                )
                sample.update(self._snapshot_freshness_evidence_fields(decision))
                add_sample(sample)

        if "liquidity" in all_domains:
            liquidity_rows = self._snapshot_lifecycle_rows_by_venue(snapshot, "liquidity")
            degraded_symbols = getattr(snapshot, "degraded_symbols", {}) or {}
            degraded_venues = {
                str(venue).lower()
                for venue in list(getattr(snapshot, "degraded_venues", []) or [])
            }
            if isinstance(degraded_symbols, dict):
                degraded_venues.update(
                    str(venue).lower()
                    for venue, symbols in degraded_symbols.items()
                    if symbols
                )
            for candidate in candidates:
                symbol = str(getattr(candidate, "symbol", "") or "").upper()
                for venue_attr in ("long_venue", "short_venue"):
                    venue = str(getattr(candidate, venue_attr, "") or "").lower()
                    row = liquidity_rows.get(venue)
                    degraded_reason = (
                        str(getattr(row, "degraded_reason", "") or "")
                        if row is not None else ""
                    )
                    if venue not in degraded_venues and not degraded_reason:
                        continue
                    observed_at_ms = (
                        int(getattr(row, "observed_at_ms", 0) or 0)
                        if row is not None else 0
                    )
                    source_age_ms = (
                        max(now_ms - observed_at_ms, 0)
                        if observed_at_ms > 0 else 0
                    )
                    source = (
                        str(getattr(row, "source", "") or "sidecar_perp_liquidity")
                        if row is not None else "sidecar_perp_liquidity"
                    )
                    degraded_symbols_for_venue = []
                    if isinstance(degraded_symbols, dict):
                        degraded_symbols_for_venue = [
                            str(v).upper()
                            for v in degraded_symbols.get(venue, []) or []
                        ]
                    candidate_hit = (
                        symbol in degraded_symbols_for_venue
                        or self._liquidity_degraded_reason_blocks_symbol(
                            degraded_reason, symbol
                        )
                    )
                    add_sample(
                        self._snapshot_candidate_scope_sample(
                            candidate=candidate,
                            domain="liquidity",
                            venue=venue,
                            source=source,
                            source_age_ms=source_age_ms,
                            fallback_duration_ms=fallback_duration_ms,
                            blocked=False,
                            block_reason=(
                                "candidate_symbol_degraded"
                                if candidate_hit else ""
                            ),
                        )
                    )

        return scope

    def _snapshot_health_candidate_scope_candidates(self, snapshot) -> tuple[list, str, int, int]:
        all_candidates = list(getattr(snapshot, "candidates", []) or []) if snapshot is not None else []
        if not all_candidates:
            return [], "empty", 0, 0
        if self._entry_readiness_provider_uses_ws_bbo() or self._local_l2_effective_enabled():
            _, tracked_candidates = self._select_v1_entry_tracked_scope(all_candidates)
            return (
                tracked_candidates,
                "v1_primary_shadow",
                len(all_candidates),
                max(len(all_candidates) - len(tracked_candidates), 0),
            )
        return all_candidates, "all_snapshot_candidates", len(all_candidates), 0

    def _snapshot_freshness_decision_log_key(
        self,
        payload: dict,
    ) -> tuple[str, str, str, str]:
        return (
            str(payload.get("venue", "") or "").lower(),
            str(payload.get("symbol", "") or "").upper(),
            str(payload.get("domain", "") or ""),
            str(payload.get("reason", "") or payload.get("decision", "") or ""),
        )

    def _append_snapshot_freshness_decision_event(
        self,
        *,
        payload: dict,
        event_kind: str,
        now_ms: int,
    ) -> None:
        key = self._snapshot_freshness_decision_log_key(payload)
        last_emit_ms = self._snapshot_freshness_decision_last_emit_ms.get(key)
        suppressed = int(self._snapshot_freshness_decision_suppressed.get(key, 0))
        due = (
            last_emit_ms is None
            or now_ms - last_emit_ms >= self._SNAPSHOT_FRESHNESS_DECISION_LOG_INTERVAL_MS
        )
        if not due:
            self._snapshot_freshness_decision_suppressed[key] += 1
            return

        event_payload = dict(payload)
        if suppressed > 0:
            event_payload["compact"] = True
            event_payload["suppressed_count"] = suppressed
        else:
            event_payload["suppressed_count"] = 0
        self._snapshot_freshness_decision_last_emit_ms[key] = now_ms
        self._snapshot_freshness_decision_suppressed.pop(key, None)
        self.ctx.journal.append("runtime.snapshot_freshness_decision", event_payload)
        if event_kind:
            self.ctx.journal.append(event_kind, event_payload)

    def _filter_candidates_by_snapshot_freshness(
        self,
        candidates: list,
        *,
        snapshot,
        now_ms: int,
        metrics: dict,
        ages: dict,
        budgets: dict | None = None,
        publish_intervals: dict | None = None,
        entry_quote_truth_overlay: dict[tuple[str, str], Any] | None = None,
    ) -> list:
        filtered = []
        self._last_snapshot_freshness_filter_blockers = Counter()
        self._last_snapshot_freshness_filter_samples = []
        fallback_duration_ms = self._snapshot_fallback_duration_ms(
            snapshot=snapshot,
            now_ms=now_ms,
        )
        for candidate in candidates:
            decisions = self._call_candidate_snapshot_freshness_decisions(
                candidate,
                snapshot=snapshot,
                now_ms=now_ms,
                record_liquidity_qualification=True,
                entry_quote_truth_overlay=entry_quote_truth_overlay,
            )
            if not decisions:
                filtered.append(candidate)
                continue
            blocking = False
            for failure in decisions:
                key = self._snapshot_metric_key(
                    failure["venue"],
                    failure["symbol"],
                    failure["domain"],
                )
                if key not in metrics:
                    self._record_snapshot_metric(
                        metrics,
                        key,
                        bool(failure.get("metric_fresh", False)),
                    )
                ages[key] = int(failure.get("age_ms", 0) or 0)
                if budgets is not None:
                    budgets[key] = int(failure.get("budget_ms", 0) or 0)
                if publish_intervals is not None and "publish_interval_ms" in failure:
                    publish_intervals[key] = int(failure.get("publish_interval_ms", 0) or 0)
                payload = dict(failure)
                event_kind = str(payload.pop("event_kind", "") or "")
                payload["ts_ms"] = now_ms
                pair_id = self._candidate_pair_id(candidate)
                symbol = str(getattr(candidate, "symbol", "") or "").upper()
                blocked = bool(
                    failure.get("blocking", False)
                    or failure.get("decision") == "skip_entry"
                )
                reason = str(failure.get("reason", "snapshot_domain_stale"))
                payload["pair_id"] = pair_id
                payload["candidate_pair_id"] = pair_id
                payload["candidate_symbol"] = symbol
                payload["source_age_ms"] = int(failure.get("age_ms", 0) or 0)
                payload["fallback_duration_ms"] = fallback_duration_ms
                payload["blocked"] = blocked
                payload["block_reason"] = reason if blocked else ""
                self._append_snapshot_freshness_decision_event(
                    payload=payload,
                    event_kind=event_kind,
                    now_ms=now_ms,
                )
                if failure.get("decision") == "skip_entry":
                    blocking = True
                    self._last_snapshot_freshness_filter_blockers[reason] += 1
                    if len(self._last_snapshot_freshness_filter_samples) < 24:
                        sample = {
                            "pair_id": pair_id,
                            "candidate_pair_id": pair_id,
                            "candidate_symbol": symbol,
                            "venue": str(failure.get("venue", "")),
                            "symbol": str(failure.get("symbol", "")),
                            "domain": str(failure.get("domain", "")),
                            "source": str(failure.get("source", "")),
                            "reason": reason,
                            "source_age_ms": int(failure.get("age_ms", 0) or 0),
                            "fallback_duration_ms": fallback_duration_ms,
                            "blocked": True,
                            "block_reason": reason,
                            "age_ms": int(failure.get("age_ms", 0) or 0),
                            "budget_ms": int(failure.get("budget_ms", 0) or 0),
                        }
                        sample.update(self._snapshot_freshness_evidence_fields(failure))
                        self._last_snapshot_freshness_filter_samples.append(sample)
            if not blocking:
                filtered.append(candidate)
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
        market_max_age_ms = int(
            getattr(self.ctx.config.runtime, "max_market_age_ms", max_age_ms) or max_age_ms
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
        if market_observed_age_ms > market_max_age_ms:
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

        snapshot_path = str(self.ctx.config.runtime.sidecar_snapshot_path)
        config_hash = hashlib.sha256(
            f"{snapshot_path}|{max_age_ms}|{self.ctx.config.runtime.mode}".encode()
        ).hexdigest()[:12]
        fallback_duration_ms = self._snapshot_fallback_duration_ms(
            snapshot=snapshot,
            now_ms=now_ms,
            max_age_ms=max_age_ms,
        )
        fresh_source_ages = []
        for quote in getattr(snapshot, "quotes", {}).values():
            observed_at_ms = self._snapshot_quote_direct_observed_at_ms(quote)
            if observed_at_ms > 0:
                age_ms = max(now_ms - observed_at_ms, 0)
                if age_ms <= self._snapshot_domain_budget_ms("quote"):
                    fresh_source_ages.append(age_ms)
        fresh_source_age_ms = min(fresh_source_ages) if fresh_source_ages else 0
        (
            candidate_scope_candidates,
            candidate_scope_mode,
            candidate_scope_all_count,
            candidate_scope_skipped_count,
        ) = self._snapshot_health_candidate_scope_candidates(snapshot)

        return {
            "freshness": freshness,
            "venues": degraded_venues,
            "degraded_venues": degraded_venues,
            "degraded_domains": degraded_domains,
            "stale_degraded_domains": domains,
            "top_degraded_symbols": top_degraded_symbols,
            "snapshot_publish_age_ms": max(snapshot_publish_age_ms, 0),
            "market_observed_age_ms": max(market_observed_age_ms, 0),
            "fallback_duration_ms": fallback_duration_ms,
            "last_good_age_ms": max(snapshot_publish_age_ms, 0),
            "fresh_source_age_ms": fresh_source_age_ms,
            "candidate_freshness_candidate_scope": candidate_scope_mode,
            "candidate_freshness_candidate_count": len(candidate_scope_candidates),
            "candidate_freshness_all_candidate_count": candidate_scope_all_count,
            "candidate_freshness_skipped_untracked_count": candidate_scope_skipped_count,
            "candidate_freshness_scope": self._snapshot_health_candidate_freshness_scope(
                snapshot=snapshot,
                now_ms=now_ms,
                degraded_domains=degraded_domains,
                stale_degraded_domains=domains,
                fallback_duration_ms=fallback_duration_ms,
                candidates=candidate_scope_candidates,
            ),
            "per_venue_quote_count": dict(sorted(per_venue_quote_count.items())),
            "per_venue_candidate_count": dict(sorted(per_venue_candidate_count.items())),
            "source_mode": str(getattr(snapshot, "source_mode", "") or ""),
            "acquisition_mode": str(getattr(snapshot, "acquisition_mode", "") or ""),
            "snapshot_path": snapshot_path,
            "config_hash": config_hash,
            "ts_ms": now_ms,
        }
