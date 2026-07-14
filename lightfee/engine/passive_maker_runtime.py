"""Passive maker runtime delegate.

This module owns behavior mechanically moved from LiveRuntime.
Do not change passive maker business conditions while extracting it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from lightfee.core.domain import Side
from lightfee.engine.business_contract import (
    classify_maker_event_reprice_reject_reason,
    maker_event_reprice_block_is_terminal,
    pending_entry_has_unhedged_maker_exposure,
)
from lightfee.engine.runtime_context import PassiveMakerRuntimeContext
from lightfee.marketdata.liquidity import execution_liquidity_from_local_l2

if TYPE_CHECKING:
    from lightfee.engine.entry_sync import HedgeDriveResult


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

    def _entry_readiness_provider_uses_ws_bbo(self) -> bool:
        return self.ctx._entry_readiness_provider_uses_ws_bbo()

    def _entry_quote_lease_max_age_ms(self) -> int:
        return self.ctx._entry_quote_lease_max_age_ms()

    def _pending_maker_side(self, pending) -> Side:
        """Resolve the durable actual maker leg without reinterpreting legacy state.

        New entries persist ``entry_maker_leg`` from the candidate/execution
        decision.  Older deserialised ``PendingEntry`` objects, however,
        acquire the dataclass default ``maker_leg='long'`` even when no maker
        was ever selected.  Treating that synthetic default as evidence
        silently changes a configured sell-maker flow into a long maker.  A
        legacy ``maker_leg`` remains authoritative once a real working-order,
        phase, or fill proves it; otherwise preserve V1's config fallback.
        """

        def side_for(label: object) -> Side | None:
            value = str(label or "").strip().lower()
            if value == "short":
                return Side.SELL
            if value == "long":
                return Side.BUY
            return None

        selected = side_for(getattr(pending, "entry_maker_leg", ""))
        if selected is not None:
            return selected
        phase_state = getattr(pending, "phase_state", None)
        active_phase_leg = (
            phase_state.get("active_maker_leg", "")
            if isinstance(phase_state, dict)
            else getattr(phase_state, "active_maker_leg", "")
        )
        selected = side_for(active_phase_leg)
        if selected is not None:
            return selected
        has_working_maker_evidence = bool(
            getattr(pending, "maker_order_id", "")
            or getattr(pending, "maker_client_order_id", "")
            or getattr(pending, "passive_order", None) is not None
            or float(getattr(pending, "maker_leg_filled", 0.0) or 0.0) > 0.0
            or float(getattr(pending, "hedge_leg_filled", 0.0) or 0.0) > 0.0
        )
        if has_working_maker_evidence:
            selected = side_for(getattr(pending, "maker_leg", ""))
            if selected is not None:
                return selected
        return (
            Side.BUY
            if self.ctx.config.strategy.maker_leg_default == "buy"
            else Side.SELL
        )

    @staticmethod
    def _maker_event_reprice_reject_reason(error: Exception) -> str:
        return classify_maker_event_reprice_reject_reason(error)

    @staticmethod
    def _maker_event_reprice_block_reason(pending) -> str:
        if pending_entry_has_unhedged_maker_exposure(pending):
            return "unhedged_single_leg_risk"
        if str(getattr(pending, "repair_state", "") or "").strip():
            return "pending_entry_repair_state"
        return ""

    def _block_maker_event_reprice(
        self,
        entry_id: str,
        pending,
        *,
        now_ms: int,
        reason: str,
        fallback_price: float = 0.0,
    ) -> None:
        stored = self._maker_event_state.get(entry_id)
        if isinstance(stored, dict) and stored.get("terminal_reject_reason"):
            if stored.get("terminal_reject_reason") == reason:
                return
            return
        stored_price = 0.0
        stored_last_reprice_ms = 0
        consecutive_failures = 0
        if isinstance(stored, dict):
            stored_price = float(stored.get("maker_price", 0.0) or 0.0)
            stored_last_reprice_ms = int(stored.get("last_reprice_ms", 0) or 0)
            consecutive_failures = int(stored.get("consecutive_failures", 0) or 0)
        elif isinstance(stored, tuple) and len(stored) == 2:
            stored_price = float(stored[1] or 0.0)
        if stored_price <= 0:
            stored_price = float(
                fallback_price
                or getattr(pending, "maker_price", 0.0)
                or getattr(pending, "maker_fill_price", 0.0)
                or 0.0
            )
        if maker_event_reprice_block_is_terminal(reason):
            self._terminally_block_maker_event_reprice(
                entry_id,
                pending,
                stored_price=stored_price,
                now_ms=now_ms,
                reason=reason,
            )
        else:
            if (
                isinstance(stored, dict)
                and stored.get("transient_block_reason") == reason
            ):
                return
            maker_leg = self._pending_maker_side(pending)
            maker_venue = (
                pending.long_venue if maker_leg == Side.BUY else pending.short_venue
            )
            self._maker_event_state[entry_id] = {
                "maker_price": stored_price,
                "last_reprice_ms": stored_last_reprice_ms,
                "consecutive_failures": consecutive_failures,
                "transient_block_reason": reason,
                "venue": (
                    maker_venue.value
                    if hasattr(maker_venue, "value")
                    else str(maker_venue)
                ),
                "symbol": pending.symbol,
            }
        self.ctx.journal.append(
            "runtime.maker_event_reprice_blocked",
            {
                "entry_id": entry_id,
                "symbol": getattr(pending, "symbol", ""),
                "reason": reason,
                "maker_leg_filled": float(
                    getattr(pending, "maker_leg_filled", 0.0) or 0.0
                ),
                "hedge_leg_filled": float(
                    getattr(pending, "hedge_leg_filled", 0.0) or 0.0
                ),
                "repair_state": str(getattr(pending, "repair_state", "") or ""),
                "ts_ms": now_ms,
            },
        )

    def _terminally_block_maker_event_reprice(
        self,
        entry_id: str,
        pending,
        *,
        stored_price: float,
        now_ms: int,
        reason: str,
    ) -> None:
        max_failures = self.ctx.config.strategy.passive_max_consecutive_failures
        maker_leg = self._pending_maker_side(pending)
        maker_venue = pending.long_venue if maker_leg == Side.BUY else pending.short_venue
        self._maker_event_state[entry_id] = {
            "maker_price": stored_price,
            "last_reprice_ms": now_ms,
            "consecutive_failures": max_failures,
            "terminal_reject_reason": reason,
            "venue": (
                maker_venue.value
                if hasattr(maker_venue, "value")
                else str(maker_venue)
            ),
            "symbol": pending.symbol,
        }

    def _refresh_entry_l2_session_readiness(self, now_ms: int) -> None:
        self.ctx._refresh_entry_l2_session_readiness(now_ms)

    def _runtime_method_override(self, method_name: str):
        method = getattr(self.ctx, method_name, None)
        class_method = getattr(type(self.ctx), method_name, None)
        if getattr(method, "__func__", None) is class_method:
            return None
        return method if callable(method) else None

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
        elif self._entry_readiness_provider_uses_ws_bbo():
            await self._maybe_tick_maker_event_ws_bbo(now_ms, pending_passive)
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
                    "local_l2_configured_enabled": self.ctx.config.strategy.local_l2_enabled,
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
            maker_leg = self._pending_maker_side(pending)
            # Check if any matching event involves this entry's venues
            entry_venues = {(pending.long_venue.value, pending.symbol),
                          (pending.short_venue.value, pending.symbol)}
            relevant = [e for e in matching_events if (e.venue, e.symbol) in entry_venues]
            if not relevant:
                continue

            # Repricing a working maker order is an execution action.  Do not
            # infer a mid from a merely HOT book: the whole book must remain
            # fresh and structurally valid at the exact reprice decision.
            long_book = self.ctx.local_l2_runtime.get_book(pending.long_venue.value, pending.symbol)
            short_book = self.ctx.local_l2_runtime.get_book(pending.short_venue.value, pending.symbol)
            # V1: use the maker venue's mid price, not a single-leg fallback
            # post_only_entry_reprice_price_hint takes from working_market (entry_sync.rs:1475-1481)
            maker_venue = pending.long_venue if maker_leg == Side.BUY else pending.short_venue
            maker_book = long_book if maker_venue == pending.long_venue else short_book
            max_age_ms = max(self.ctx.config.strategy.max_liquidity_snapshot_age_ms, 0)
            if maker_book is None or max_age_ms <= 0:
                continue
            snapshot = execution_liquidity_from_local_l2(
                maker_book,
                max_depth=1,
                max_age_ms=max_age_ms,
                now_ms=now_ms,
                require_ready=True,
            )
            if not snapshot.book_ready:
                continue
            mid = (snapshot.bids[0].price + snapshot.asks[0].price) / 2.0
            if mid <= 0:
                continue
            block_reason = self._maker_event_reprice_block_reason(pending)
            if block_reason:
                self._block_maker_event_reprice(
                    entry_id,
                    pending,
                    now_ms=now_ms,
                    reason=block_reason,
                    fallback_price=mid,
                )
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
            if isinstance(stored, dict) and stored.get("terminal_reject_reason"):
                continue
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
                reject_reason = self._maker_event_reprice_reject_reason(e)
                if reject_reason:
                    self._terminally_block_maker_event_reprice(
                        entry_id,
                        pending,
                        stored_price=stored_price,
                        now_ms=now_ms,
                        reason=reject_reason,
                    )
                else:
                    self._maker_event_state[entry_id] = (manager, stored_price)
                self.ctx.journal.append(
                    "runtime.maker_event_reprice_error",
                    {
                        "entry_id": entry_id,
                        "action": action,
                        "error": str(e),
                        "response_classification": reject_reason,
                    },
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

    async def _maybe_tick_maker_event_ws_bbo(
        self, now_ms: int, pending_passive: list,
    ) -> None:
        """WS BBO maker-event lane using the in-situ pending hedge driver."""
        strategy = self.ctx.config.strategy
        reprice_threshold_bps = strategy.passive_reprice_threshold_bps
        cancel_replace_threshold_bps = strategy.passive_cancel_replace_threshold_bps

        from lightfee.engine.entry_sync import _adapter_supports_amend
        from lightfee.engine.passive_order_manager import (
            PassiveOrderManager,
            PassiveOrderManagerDecisionType,
            PassiveOrderManagerProfile,
            PassiveOrderDecisionInput,
            PassiveSkipReason,
        )

        woke_positions = 0
        missing_quotes: list[dict[str, Any]] = []
        venues: set[str] = set()
        max_quote_age_ms = 0
        min_quote_age_ms = 1_000_000_000

        for entry_id, pending in pending_passive:
            maker_leg = self._pending_maker_side(pending)
            maker_venue = pending.long_venue if maker_leg == Side.BUY else pending.short_venue
            venue_str = maker_venue.value if hasattr(maker_venue, "value") else str(maker_venue)
            quote = None
            try:
                quote = self.ctx.ws_bbo_cache.get_quote(venue_str, pending.symbol)
            except Exception:
                quote = None
            budget_ms = self._entry_quote_lease_max_age_ms()
            bid = ask = 0.0
            observed_at_ms = 0
            if quote is not None:
                try:
                    bid = float(getattr(quote, "bid", 0.0) or 0.0)
                    ask = float(getattr(quote, "ask", 0.0) or 0.0)
                    observed_at_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
                except Exception:
                    bid = ask = 0.0
                    observed_at_ms = 0
            age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else None
            valid = bid > 0.0 and ask > bid
            fresh = (
                valid
                and observed_at_ms > 0
                and budget_ms > 0
                and age_ms is not None
                and age_ms <= budget_ms
            )
            if not fresh:
                missing_quotes.append(
                    {
                        "entry_id": entry_id,
                        "venue": venue_str,
                        "symbol": pending.symbol,
                        "reason": (
                            "stale_quote"
                            if valid and age_ms is not None and budget_ms > 0 and age_ms > budget_ms
                            else "missing_or_invalid_quote"
                        ),
                        "age_ms": age_ms,
                        "budget_ms": budget_ms,
                    }
                )
                continue

            mid = (bid + ask) / 2.0
            block_reason = self._maker_event_reprice_block_reason(pending)
            if block_reason:
                self._block_maker_event_reprice(
                    entry_id,
                    pending,
                    now_ms=now_ms,
                    reason=block_reason,
                    fallback_price=mid,
                )
                continue
            stored = self._maker_event_state.get(entry_id)
            if isinstance(stored, dict) and stored.get("terminal_reject_reason"):
                continue
            if isinstance(stored, tuple) and len(stored) == 2:
                manager, stored_price = stored
            else:
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

            adapter = self.ctx.venue_adapters.get(maker_venue)
            supports_amend = _adapter_supports_amend(adapter)
            decision_input = PassiveOrderDecisionInput(
                tick_size=0.1,
                reference_mid_price=mid,
                target_price=mid,
                current_price=stored_price if stored_price > 0 else None,
                target_quantity=getattr(pending, "long_quantity", 0) or 0,
                supports_amend=supports_amend,
            )
            decision = manager.decide(decision_input, now_ms)
            if decision.kind == PassiveOrderManagerDecisionType.PLACE:
                self._maker_event_state[entry_id] = (manager, mid)
                continue
            if decision.kind == PassiveOrderManagerDecisionType.COOLDOWN:
                continue
            if decision.kind == PassiveOrderManagerDecisionType.HOLD:
                if decision.skip_reason == PassiveSkipReason.OPS_BUDGET_EXCEEDED:
                    self.ctx.journal.append(
                        "execution.passive_ops_rate_limited",
                        {
                            "entry_id": entry_id,
                            "reason": "ops_budget_exceeded",
                            "source": "ws_bbo_quote_lease",
                            "ts_ms": now_ms,
                        },
                    )
                continue
            if decision.kind == PassiveOrderManagerDecisionType.AMEND:
                action = "reprice"
            elif decision.kind == PassiveOrderManagerDecisionType.CANCEL_REPLACE:
                action = "cancel_replace"
            else:
                continue
            if self.ctx.entry_executor is None:
                continue

            try:
                manager.note_operation(now_ms)
                if action == "cancel_replace":
                    manager.note_operation(now_ms)
                result = await self._call_reprice_passive_maker_l2(
                    pending, mid, stored_price, action, now_ms, entry_id,
                )
                manager.note_success(now_ms)
                self._maker_event_state[entry_id] = (manager, mid)
                pe = self.ctx.state.pending_entries.get(entry_id)
                if pe is not None:
                    pe.maker_price = mid
                    if result.order_id:
                        pe.maker_order_id = result.order_id
                woke_positions += 1
                venues.add(venue_str)
                if age_ms is not None:
                    min_quote_age_ms = min(min_quote_age_ms, age_ms)
                    max_quote_age_ms = max(max_quote_age_ms, age_ms)
            except Exception as e:
                manager.note_failure(now_ms)
                reject_reason = self._maker_event_reprice_reject_reason(e)
                if reject_reason:
                    self._terminally_block_maker_event_reprice(
                        entry_id,
                        pending,
                        stored_price=stored_price,
                        now_ms=now_ms,
                        reason=reject_reason,
                    )
                else:
                    self._maker_event_state[entry_id] = (manager, stored_price)
                self.ctx.journal.append(
                    "runtime.maker_event_reprice_error",
                    {
                        "entry_id": entry_id,
                        "action": action,
                        "error": str(e),
                        "source": "ws_bbo_quote_lease",
                        "response_classification": reject_reason,
                    },
                )

        if missing_quotes:
            self.ctx.journal.append(
                "runtime.maker_event_no_ws_bbo_quote",
                {
                    "ts_ms": now_ms,
                    "pending_passive_total": len(pending_passive),
                    "missing_quote_count": len(missing_quotes),
                    "samples": missing_quotes[:8],
                    "source": "ws_bbo_quote_lease",
                    "provider": "ws_bbo_quote_lease",
                    "reason": "missing_stale_or_invalid_ws_bbo_quote",
                },
            )

        self._last_maker_event_ms = now_ms
        if woke_positions > 0:
            self.ctx.journal.append(
                "runtime.maker_event_lane_wake",
                {
                    "position_count": woke_positions,
                    "pending_passive_total": len(pending_passive),
                    "source": "ws_bbo_quote_lease",
                    "venues": sorted(venues),
                    "min_quote_age_ms": (
                        min_quote_age_ms if min_quote_age_ms < 1_000_000_000 else 0
                    ),
                    "max_quote_age_ms": max_quote_age_ms,
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
            block_reason = self._maker_event_reprice_block_reason(pending)
            if block_reason:
                self._block_maker_event_reprice(
                    entry_id,
                    pending,
                    now_ms=now_ms,
                    reason=block_reason,
                    fallback_price=mid,
                )
                continue

            est = self._maker_event_state.get(entry_id, {})
            last_reprice_ms = est.get("last_reprice_ms", 0)
            if last_reprice_ms > 0 and (now_ms - last_reprice_ms) < cooldown_ms:
                continue

            failures = est.get("consecutive_failures", 0)
            if est.get("terminal_reject_reason"):
                continue
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
                reject_reason = self._maker_event_reprice_reject_reason(e)
                if reject_reason:
                    self._terminally_block_maker_event_reprice(
                        entry_id,
                        pending,
                        stored_price=stored_price,
                        now_ms=now_ms,
                        reason=reject_reason,
                    )
                else:
                    self._maker_event_state[entry_id] = {
                        "maker_price": stored_price,
                        "last_reprice_ms": now_ms,
                        "consecutive_failures": failures + 1,
                    }
                self.ctx.journal.append(
                    "runtime.maker_event_reprice_error",
                    {
                        "entry_id": entry_id,
                        "action": action,
                        "error": str(e),
                        "response_classification": reject_reason,
                    },
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
        from lightfee.core.domain import Side
        from lightfee.engine.entry import EntryContext, EntryType

        maker_leg = self._pending_maker_side(pending)

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
            expected_shortfall_bps_entry=pending.expected_shortfall_bps_entry,
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
        from lightfee.core.domain import Side
        from lightfee.engine.entry_sync import drive_pending_entry_hedge

        maker_leg = self._pending_maker_side(pending)

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
