"""Passive maker runtime delegate.

This module owns behavior mechanically moved from LiveRuntime.
Do not change passive maker business conditions while extracting it.
"""

from __future__ import annotations

from lightfee.core.domain import Side
from lightfee.engine.entry_sync import HedgeDriveResult
from lightfee.engine.runtime_context import PassiveMakerRuntimeContext


class PassiveMakerRuntime:
    def __init__(self, ctx: PassiveMakerRuntimeContext) -> None:
        self.ctx = ctx

    @property
    def _maker_event_state(self) -> dict[str, object]:
        return self.ctx._maker_event_state

    @property
    def _last_maker_event_ms(self) -> int:
        return self.ctx._last_maker_event_ms

    @_last_maker_event_ms.setter
    def _last_maker_event_ms(self, value: int) -> None:
        self.ctx._last_maker_event_ms = value

    def _refresh_runtime_market_data_config_state(self) -> None:
        self.ctx._refresh_runtime_market_data_config_state()

    def _local_l2_effective_enabled(self) -> bool:
        return self.ctx._local_l2_effective_enabled()

    def _refresh_entry_l2_session_readiness(self, now_ms: int) -> None:
        self.ctx._refresh_entry_l2_session_readiness(now_ms)

    def _pending_entry_l2_ready_books(self, pending, now_ms: int):
        return self.ctx._pending_entry_l2_ready_books(pending, now_ms)

    def _runtime_method_override(self, method_name: str):
        method = getattr(self.ctx, method_name, None)
        class_method = getattr(type(self.ctx), method_name, None)
        if getattr(method, "__func__", None) is class_method:
            return None
        return method if callable(method) else None

    def _pending_active_maker_side(self, pending) -> Side:
        """Use the persisted active route; config is legacy fallback only."""
        maker_leg = str(getattr(pending, "maker_leg", "") or "").lower()
        if maker_leg not in {"long", "short"}:
            maker_leg = str(getattr(pending, "entry_maker_leg", "") or "").lower()
        if maker_leg == "short":
            return Side.SELL
        if maker_leg == "long":
            return Side.BUY
        return (
            Side.BUY
            if str(self.ctx.config.strategy.maker_leg_default).lower() == "buy"
            else Side.SELL
        )

    async def _call_reprice_passive_maker(
        self,
        pending,
        new_price: float,
        old_price: float,
        action: str,
        now_ms: int,
        entry_id: str,
    ) -> None:
        override = self._runtime_method_override("_reprice_passive_maker")
        if override is not None:
            return await override(
                pending, new_price, old_price, action, now_ms, entry_id
            )
        return await self._reprice_passive_maker(
            pending, new_price, old_price, action, now_ms, entry_id
        )

    async def _call_reprice_passive_maker_l2(
        self,
        pending,
        new_price: float,
        old_price: float,
        action: str,
        now_ms: int,
        entry_id: str,
    ):
        override = self._runtime_method_override("_reprice_passive_maker_l2")
        if override is not None:
            return await override(
                pending, new_price, old_price, action, now_ms, entry_id
            )
        return await self._reprice_passive_maker_l2(
            pending, new_price, old_price, action, now_ms, entry_id
        )

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
        if not self.ctx.config.runtime.maker_event_lane_enabled:
            self._maker_event_state.clear()
            return

        # Min wake interval gating
        min_interval = self.ctx.config.runtime.maker_event_lane_min_wake_interval_ms
        if self._last_maker_event_ms > 0 and (now_ms - self._last_maker_event_ms) < min_interval:
            return

        # Only process when there are pending entries with passive maker legs
        pending_passive = [
            (eid, pe) for eid, pe in self.ctx.state.pending_entries.items()
            if pe.entry_type and "passive" in str(pe.entry_type).lower()
        ]
        if not pending_passive:
            return

        self._refresh_runtime_market_data_config_state()
        local_l2_enabled = self._local_l2_effective_enabled()
        non_parity_mode = self.ctx.config.runtime.opportunity_input_mode == "non_parity"

        if local_l2_enabled:
            # --- Parity mode: local-L2 event-driven ---
            await self._maybe_tick_maker_event_local_l2(now_ms, pending_passive)
        elif non_parity_mode:
            # --- Explicit non-parity fallback: sidecar mid-price ---
            await self._maybe_tick_maker_event_sidecar(now_ms, pending_passive)
        else:
            # Neither parity nor non-parity — sidecar fallback must be explicit opt-in.
            # local_l2_enabled=False alone does NOT activate the sidecar path.
            self.ctx.journal.append(
                "runtime.maker_event_no_eligible_mode",
                {
                    "ts_ms": now_ms,
                    "local_l2_enabled": local_l2_enabled,
                    "local_l2_configured_enabled": bool(
                        getattr(self.ctx.config.strategy, "local_l2_enabled", False)
                    ),
                    "opportunity_input_mode": self.ctx.config.runtime.opportunity_input_mode,
                    "reason": "non-parity fallback requires explicit opportunity_input_mode='non_parity'",
                },
            )

    async def _maybe_tick_maker_event_local_l2(
        self, now_ms: int, pending_passive: list,
    ) -> None:
        """Local-L2 parity maker-event lane: sync runtime, drain events, drive hedges."""
        # Sync local-L2 runtime
        events = self.ctx.local_l2_runtime.sync(now_ms)
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
            self.ctx.journal.append(
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

        strategy = self.ctx.config.strategy
        reprice_threshold_bps = strategy.passive_reprice_threshold_bps
        cancel_replace_threshold_bps = strategy.passive_cancel_replace_threshold_bps

        woke_positions = 0
        event_kinds: set[str] = set()
        wake_reasons: set[str] = set()
        min_event_age_ms = 1_000_000_000
        max_event_age_ms = 0
        venues: set[str] = set()

        for entry_id, pending in pending_passive:
            maker_leg = self._pending_active_maker_side(pending)
            # Check if any matching event involves this entry's venues
            entry_venues = {(pending.long_venue.value, pending.symbol),
                          (pending.short_venue.value, pending.symbol)}
            relevant = [e for e in matching_events if (e.venue, e.symbol) in entry_venues]
            if not relevant:
                continue

            # This execution-owned pending entry must use its exact two-leg
            # session, never a coincidentally HOT global Local-L2 book.
            session_books = self._pending_entry_l2_ready_books(pending, now_ms)
            if session_books is None:
                continue
            long_book, short_book = session_books
            long_mid = long_book.mid_price()
            short_mid = short_book.mid_price()
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
            adapter = self.ctx.venue_adapters.get(maker_venue)
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
                    self.ctx.journal.append(
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

            if self.ctx.entry_executor is None:
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
                result = await self._call_reprice_passive_maker_l2(
                    pending, mid, stored_price, action, now_ms, entry_id,
                )
                # Update PassiveOrderManager runtime tracker
                manager.note_success(now_ms)
                self._maker_event_state[entry_id] = (manager, mid)
                # Write back to authoritative PendingEntry state
                pe = self.ctx.state.pending_entries.get(entry_id)
                if pe is not None:
                    pe.maker_price = mid
                    if result.order_id:
                        pe.maker_order_id = result.order_id
                woke_positions += 1
            except Exception as e:
                manager.note_failure(now_ms)
                self._maker_event_state[entry_id] = (manager, stored_price)
                self.ctx.journal.append(
                    "runtime.maker_event_reprice_error",
                    {"entry_id": entry_id, "action": action, "error": str(e)},
                )

        self._last_maker_event_ms = now_ms
        self.ctx.local_l2_runtime.metrics.maker_event_lane_wake_total += 1
        self.ctx.journal.append(
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

        snapshot = _load_snap(self.ctx.config.runtime.sidecar_snapshot_path)
        if snapshot is None:
            return

        price_hints: dict[str, float] = {}
        for quote in snapshot.quotes.values():
            if quote.bid > 0 and quote.ask > 0:
                price_hints[quote.symbol] = (quote.bid + quote.ask) / 2.0

        strategy = self.ctx.config.strategy
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

            if self.ctx.entry_executor is None:
                continue

            try:
                await self._call_reprice_passive_maker(
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
                self.ctx.journal.append(
                    "runtime.maker_event_reprice_error",
                    {"entry_id": entry_id, "action": action, "error": str(e)},
                )

        self._last_maker_event_ms = now_ms
        if woke_positions > 0:
            self.ctx.journal.append(
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
        from lightfee.engine.entry import EntryContext, EntryType

        maker_leg = self._pending_active_maker_side(pending)

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
            opportunity_type=pending.opportunity_type,
            funding_timestamp_ms=pending.funding_timestamp_ms,
            first_funding_timestamp_ms=pending.first_funding_timestamp_ms,
            long_funding_timestamp_ms=pending.long_funding_timestamp_ms,
            short_funding_timestamp_ms=pending.short_funding_timestamp_ms,
            second_funding_timestamp_ms=pending.second_funding_timestamp_ms,
            first_funding_leg=pending.first_funding_leg,
            funding_edge_bps_entry=pending.funding_edge_bps_entry,
            total_funding_edge_bps_entry=pending.total_funding_edge_bps_entry,
            expected_edge_bps_entry=pending.expected_edge_bps_entry,
            worst_case_edge_bps_entry=pending.worst_case_edge_bps_entry,
            entry_maker_leg=pending.entry_maker_leg,
            exit_maker_leg=pending.exit_maker_leg,
            entry_cross_bps_entry=pending.entry_cross_bps_entry,
            fee_bps_entry=pending.fee_bps_entry,
            entry_slippage_bps_entry=pending.entry_slippage_bps_entry,
            transfer_bias_bps_entry=pending.transfer_bias_bps_entry,
            transfer_state_at_entry=pending.transfer_state_at_entry,
            entry_liquidity_source_at_entry=pending.entry_liquidity_source_at_entry,
            long_volume_24h_quote_at_entry=pending.long_volume_24h_quote_at_entry,
            short_volume_24h_quote_at_entry=pending.short_volume_24h_quote_at_entry,
            long_open_interest_quote_at_entry=pending.long_open_interest_quote_at_entry,
            short_open_interest_quote_at_entry=pending.short_open_interest_quote_at_entry,
            long_entry_vwap=pending.long_entry_vwap,
            short_entry_vwap=pending.short_entry_vwap,
            entry_capacity_constrained=pending.entry_capacity_constrained,
            entry_target_quantity=pending.entry_target_quantity,
            long_max_executable_quantity=pending.long_max_executable_quantity,
            short_max_executable_quantity=pending.short_max_executable_quantity,
            entry_max_executable_quantity=pending.entry_max_executable_quantity,
            entry_depth_shortfall_quantity=pending.entry_depth_shortfall_quantity,
            entry_max_executable_notional_quote=pending.entry_max_executable_notional_quote,
            entry_depth_capped_at_entry=pending.entry_depth_capped_at_entry,
            advisories=list(pending.advisories),
            blocked_reasons=list(pending.blocked_reasons),
            exit_after_first_stage=pending.exit_after_first_stage,
        )
        await self.ctx.entry_executor.execute(ctx)
        self.ctx.journal.append(
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
        from lightfee.engine.entry_sync import drive_pending_entry_hedge

        maker_leg = self._pending_active_maker_side(pending)

        result = await drive_pending_entry_hedge(
            entry_id=entry_id,
            pending=pending,
            new_price=new_price,
            old_price=old_price,
            action=action,
            now_ms=now_ms,
            adapters=self.ctx.venue_adapters,
            journal=self.ctx.journal,
            maker_leg=maker_leg,
            symbol=pending.symbol,
            long_venue=pending.long_venue,
            short_venue=pending.short_venue,
        )

        if result.outcome in ("applied", "uncertain"):
            self.ctx.journal.append(
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
