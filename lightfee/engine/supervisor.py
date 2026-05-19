"""Supervisor: monitors open positions, triggers exits, manages risk.

V1 Rust references:
- src/engine/risk.rs: manage_open_positions (line 529), manage_open_position (line 1255)
- src/risk.rs: evaluate_position_risk (line 114)
- src/health.rs: evaluate_venue_health (line 60)
"""

from __future__ import annotations

from typing import Optional

from lightfee.config.schema import AppConfig
from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import Venue
from lightfee.engine.close_executor import CloseExecutor
from lightfee.engine.risk_actions import (
    AccountRiskSnapshot,
    PositionRiskView,
    RiskExecutionPlan,
    RiskExecutionPlanKind,
    VenueHealthAction,
    VenueHealthView,
    build_risk_execution_plan,
    evaluate_position_risk,
    evaluate_venue_health,
)
from lightfee.engine.state import EngineState, OpenPosition
from lightfee.persistence.journal import Journal
from lightfee.risk.health import evaluate_risk_health
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode, derive_engine_mode


class Supervisor:
    """Monitors open positions and applies risk rules with real actions.

    V1 parity: evaluates per-position risk, builds RiskExecutionPlans,
    executes delever closes, single-side protection, and fail-closed transitions.
    """

    def __init__(
        self,
        config: AppConfig,
        state: EngineState,
        journal: Journal,
        close_executor: Optional[CloseExecutor] = None,
    ) -> None:
        self.config = config
        self.state = state
        self.journal = journal
        self.close_executor = close_executor
        # V1: current_venue_health_views (risk.rs)
        self._venue_health_views: dict[Venue, VenueHealthView] = {}
        # V1: per-venue risk snapshot cache (set by runtime before each tick)
        self._risk_snapshot_cache: dict[Venue, dict] = {}

    # ------------------------------------------------------------------
    # Per-venue health → global risk mode
    # ------------------------------------------------------------------

    def _supervisor_action_mode(self, action: VenueHealthAction) -> GlobalRiskMode:
        """Map VenueHealthAction to GlobalRiskMode with per-line config gating.

        V1: supervisor_action_mode (state.rs:443-473).
        """
        strategy = self.config.strategy
        if action == VenueHealthAction.NORMAL:
            return GlobalRiskMode.RUNNING
        elif action == VenueHealthAction.PAUSE_ENTRY:
            if strategy.warning_line_enabled:
                return GlobalRiskMode.ENTRY_PAUSED
            return GlobalRiskMode.RUNNING
        elif action == VenueHealthAction.REDUCE_ONLY:
            if strategy.delever_line_enabled:
                return GlobalRiskMode.REDUCE_ONLY
            elif strategy.warning_line_enabled:
                return GlobalRiskMode.ENTRY_PAUSED
            return GlobalRiskMode.RUNNING
        elif action == VenueHealthAction.FAIL_CLOSED:
            if strategy.death_line_enabled:
                return GlobalRiskMode.FAIL_CLOSED
            elif strategy.delever_line_enabled:
                return GlobalRiskMode.REDUCE_ONLY
            elif strategy.warning_line_enabled:
                return GlobalRiskMode.ENTRY_PAUSED
            return GlobalRiskMode.RUNNING
        return GlobalRiskMode.RUNNING

    def _collect_venue_health_views(
        self,
        now_ms: int,
        adapters: dict[Venue, VenueAdapter],
    ) -> dict[Venue, VenueHealthView]:
        """Evaluate per-venue health including private-stream health.

        V1: refresh_venue_health_supervisor (risk.rs:151-255).
        Iterates supervised venues, fetches risk snapshots & private health,
        calls evaluate_venue_health(), detects transitions.
        """
        views: dict[Venue, VenueHealthView] = {}
        strategy = self.config.strategy

        if not strategy.risk_monitor_enabled:
            return views

        supervised = self._supervised_venues()
        for venue in supervised:
            adapter = adapters.get(venue)
            if adapter is None:
                continue

            # Risk snapshot
            risk_snapshot = self._fetch_risk_snapshot_for_venue(venue, adapter, now_ms)

            # Private health
            supports_private = adapter.supports_private_health
            private_conn_healthy: Optional[bool] = None
            private_pos_confirmed: bool = True

            if supports_private:
                # V1: adapter.cached_private_connection_health()
                conn_health = adapter.cached_private_connection_health()
                if conn_health is not None:
                    private_conn_healthy = not conn_health.is_unhealthy()
                # Position confirmation: for each active position on this venue,
                # check if private stream has cached position data
                active_on_venue = [
                    pos for pos in self.state.open_positions.values()
                    if pos.long_venue == venue or pos.short_venue == venue
                ]
                if active_on_venue:
                    private_pos_confirmed = all(
                        self._venue_private_position_confirmed(venue, pos.symbol, adapter)
                        for pos in active_on_venue
                    )

            view = evaluate_venue_health(
                strategy=strategy,
                venue=venue,
                now_ms=now_ms,
                supports_risk_health=adapter.supports_risk_health,
                risk_snapshot=risk_snapshot,
                supports_private_health=supports_private,
                private_connection_healthy=private_conn_healthy,
                private_position_confirmed=private_pos_confirmed,
            )

            # V1: detect transition to private_stream_unhealthy
            previous = self._venue_health_views.get(venue)
            private_became_unhealthy = (
                (previous is None or "private_stream_unhealthy" not in previous.reasons)
                and "private_stream_unhealthy" in view.reasons
            )
            if private_became_unhealthy:
                self.journal.append(
                    "risk.ws_disconnect",
                    {"venue": venue.value, "reasons": view.reasons, "ts_ms": now_ms},
                )

            if previous is None or previous.action != view.action or set(previous.reasons) != set(view.reasons):
                self.journal.append(
                    "venue.health_changed",
                    {
                        "venue": venue.value,
                        "action": view.action.value,
                        "health_ratio": view.health_ratio,
                        "degraded": view.degraded,
                        "reasons": view.reasons,
                        "order_health_risk_score": view.order_health_risk_score,
                    },
                )

            views[venue] = view

        self._venue_health_views = views
        return views

    def _supervised_venues(self) -> set[Venue]:
        """Collect unique venues from open positions and pending reconciliations.

        V1: supervised_venues (risk.rs:257-268).
        """
        venues: set[Venue] = set()
        for pos in self.state.open_positions.values():
            venues.add(pos.long_venue)
            venues.add(pos.short_venue)
        for rec in self.state.pending_close_reconciliations:
            venues.add(rec.get("position_snapshot", {}).get("long_venue", Venue.BINANCE))
            venues.add(rec.get("position_snapshot", {}).get("short_venue", Venue.BINANCE))
        return venues

    def _fetch_risk_snapshot_for_venue(
        self, venue: Venue, adapter: VenueAdapter, now_ms: int
    ) -> Optional[AccountRiskSnapshot]:
        """Fetch account risk snapshot from the runtime cache.

        V1: fetch_account_risk_with_runtime_cache (risk.rs:169-188).
        The runtime populates self._risk_snapshot_cache before each supervision tick.
        """
        if not adapter.supports_risk_health:
            return None
        entry = self._risk_snapshot_cache.get(venue)
        if entry is None:
            return None
        result = entry.get("result")
        if result is None:
            return None
        if isinstance(result, AccountRiskSnapshot):
            return result
        # result may be an error string (V1 Err variant) — treat as None
        return None

    def _venue_private_position_confirmed(
        self, venue: Venue, symbol: str, adapter: VenueAdapter
    ) -> bool:
        """Check if private stream has confirmed a position for this venue+symbol.

        V1: venue_private_position_confirmed (execution_core/engine.rs:4829-4843).
        - No private health support → skip check (True)
        - No cached connection health → not yet connected, skip check (True)
        - Connection unhealthy → not confirmed (False)
        - Connection healthy + cached position → confirmed (True)
        - Connection healthy + no cached position → not confirmed (False)
        """
        if not adapter.supports_private_health:
            return True
        conn_health = adapter.cached_private_connection_health()
        if conn_health is None:
            # V1: no cached health → private stream never connected / not yet available
            return True
        if conn_health.is_unhealthy():
            return False
        cached_pos = adapter.cached_position(symbol)
        return cached_pos is not None

    def update_global_risk_mode(
        self,
        venue_health_ratios: dict[str, float],
        venue_health_views: Optional[dict[Venue, VenueHealthView]] = None,
    ) -> GlobalRiskMode:
        """Recompute global risk mode from venue health views and risk ratios.

        V1: derive_global_risk_mode (state.rs:398-441) + recompute_global_risk_mode.
        Aggregates per-venue VenueHealthView actions via supervisor_action_mode(),
        taking the max across all venues.
        """
        strategy = self.config.strategy

        # V1: Start from health-ratio-based evaluation as baseline
        health = evaluate_risk_health(venue_health_ratios, strategy)
        new_mode = GlobalRiskMode.RUNNING

        if health.death_condition and strategy.death_line_enabled:
            new_mode = GlobalRiskMode.FAIL_CLOSED
        elif health.delever_condition and strategy.delever_line_enabled:
            new_mode = GlobalRiskMode.REDUCE_ONLY
        elif health.warning_condition and strategy.warning_line_enabled:
            if strategy.warning_pause_new_entries_enabled:
                new_mode = GlobalRiskMode.ENTRY_PAUSED

        # V1: Aggregate per-venue health views — each venue's health action
        # maps to a candidate GlobalRiskMode, take the max
        if venue_health_views and strategy.risk_monitor_enabled:
            for view in venue_health_views.values():
                candidate = self._supervisor_action_mode(view.action)
                if candidate.at_least(new_mode) and candidate != new_mode:
                    new_mode = candidate
                    reason = view.reasons[0] if view.reasons else f"venue_health:{view.venue.value}"
                    self.state.global_risk_reason = reason

        old_mode = self.state.risk_mode

        # V1: fail_closed_latch_can_clear — auto-recover from FAIL_CLOSED when
        # health has recovered and no blocking conditions remain (state.rs:476-487)
        if old_mode == GlobalRiskMode.FAIL_CLOSED and new_mode != GlobalRiskMode.FAIL_CLOSED:
            if self._fail_closed_can_clear():
                self.state.lifecycle = EngineLifecycle.RUNNING
                self.state.last_error = None
                self.journal.append(
                    "risk.fail_closed_auto_resumed",
                    {
                        "from_mode": old_mode.value,
                        "to_mode": new_mode.value,
                        "min_health_ratio": health.min_health_ratio,
                    },
                )

        if new_mode != old_mode:
            self.state.risk_mode = new_mode
            self.journal.append(
                "risk.global_mode_changed",
                {
                    "from": old_mode.value,
                    "to": new_mode.value,
                    "min_health_ratio": health.min_health_ratio,
                },
            )
            if new_mode == GlobalRiskMode.FAIL_CLOSED:
                self.state.lifecycle = EngineLifecycle.RISK_ONLY  # V1: FailClosed = RISK_ONLY + FAIL_CLOSED risk
                self.journal.append(
                    "risk.fail_closed_entered",
                    {"min_health_ratio": health.min_health_ratio},
                )

            # Clear entry pause journal on recovery
            if old_mode == GlobalRiskMode.ENTRY_PAUSED and new_mode == GlobalRiskMode.RUNNING:
                self.journal.append("risk.entry_pause_cleared", {})

        return new_mode

    def _fail_closed_can_clear(self) -> bool:
        """V1 fail_closed_latch_can_clear (state.rs:476-483).

        Returns True when FAIL_CLOSED can be safely auto-recovered:
        - No recovery block reason
        - No open positions that need risk protection
        """
        if self.state.recovery_blocked_reason:
            return False
        # V1: positions that triggered fail_closed must be resolved
        for pos in self.state.open_positions.values():
            if pos.last_risk_reason and "fail_closed" in pos.last_risk_reason:
                return False
            if pos.single_side_protection_triggered:
                return False
        return True

    # ------------------------------------------------------------------
    # Per-position risk supervision
    # ------------------------------------------------------------------

    def supervise_position(
        self,
        position: OpenPosition,
        now_ms: int,
        long_snapshot=None,
        short_snapshot=None,
        long_supports_risk_health: bool = True,
        short_supports_risk_health: bool = True,
    ) -> Optional[RiskExecutionPlan]:
        """Evaluate and plan risk actions for a single position.

        Returns a RiskExecutionPlan if action is needed, or None.
        Also handles delever recovery (clears step count when health recovers).
        """
        strategy = self.config.strategy

        if not strategy.risk_monitor_enabled:
            return None

        risk_view = evaluate_position_risk(
            strategy=strategy,
            now_ms=now_ms,
            long_venue=position.long_venue,
            short_venue=position.short_venue,
            long_supports_risk_health=long_supports_risk_health,
            short_supports_risk_health=short_supports_risk_health,
            long_snapshot=long_snapshot,
            short_snapshot=short_snapshot,
        )

        # Recovery: health restored → reset delever tracking
        if (
            position.risk_delever_step_count > 0
            and risk_view.min_health_ratio is not None
            and risk_view.min_health_ratio >= strategy.health_recovery_ratio
        ):
            position.risk_delever_step_count = 0
            position.last_risk_action_at_ms = 0
            self.journal.append(
                "risk.delever_recovered",
                {
                    "position_id": position.position_id,
                    "symbol": position.symbol,
                    "min_health_ratio": risk_view.min_health_ratio,
                    "health_recovery_ratio": strategy.health_recovery_ratio,
                },
            )

        # Update warning state
        self._update_warning_state(position, risk_view)

        # Build execution plan
        plan = build_risk_execution_plan(position, risk_view, strategy, now_ms)
        return plan

    def _update_warning_state(
        self, position: OpenPosition, risk_view: PositionRiskView
    ) -> None:
        """Track positions currently under warning (V1 update_warning_state_for_position)."""
        strategy = self.config.strategy
        if not strategy.risk_monitor_enabled:
            return

        if risk_view.warning_condition:
            self.journal.append(
                "risk.warning_triggered",
                {
                    "position_id": position.position_id,
                    "symbol": position.symbol,
                    "long_health_ratio": risk_view.long_health_ratio,
                    "short_health_ratio": risk_view.short_health_ratio,
                    "min_health_ratio": risk_view.min_health_ratio,
                    "degraded_reason": risk_view.degraded_reason,
                },
            )
            if not strategy.warning_line_enabled:
                self.journal.append(
                    "risk.line_disabled",
                    {
                        "position_id": position.position_id,
                        "line": "warning",
                        "symbol": position.symbol,
                    },
                )
            if not strategy.warning_pause_new_entries_enabled:
                self.journal.append(
                    "risk.line_disabled",
                    {
                        "position_id": position.position_id,
                        "line": "warning_pause_new_entries",
                        "symbol": position.symbol,
                    },
                )
        elif position.single_side_protection_triggered:
            # Warning was previously active but now cleared
            pass  # risk.warning_cleared emitted by risk mode transition handler

    # ------------------------------------------------------------------
    # Risk plan execution
    # ------------------------------------------------------------------

    async def execute_risk_plan(
        self,
        position: OpenPosition,
        plan: RiskExecutionPlan,
        now_ms: int,
        long_price_hint: float = 0.0,
        short_price_hint: float = 0.0,
    ) -> None:
        """Execute a RiskExecutionPlan: log, close, or transition lifecycle.

        V1: engine/risk.rs lines 1794-1905.
        """
        if plan.kind == RiskExecutionPlanKind.DELEVER:
            await self._execute_delever(position, plan, now_ms, long_price_hint, short_price_hint)

        elif plan.kind == RiskExecutionPlanKind.SINGLE_SIDE_PROTECTION:
            await self._execute_single_side_protection(
                position, plan, now_ms,
                long_price_hint=long_price_hint, short_price_hint=short_price_hint,
            )

        elif plan.kind == RiskExecutionPlanKind.FAIL_CLOSED:
            self._execute_fail_closed(position, plan, now_ms)

    async def _execute_delever(
        self,
        position: OpenPosition,
        plan: RiskExecutionPlan,
        now_ms: int,
        long_price_hint: float,
        short_price_hint: float,
    ) -> None:
        """Execute a synchronized delever (partial close).

        V1: engine/risk.rs lines 1806-1881.
        """
        # Log pre-action
        self.journal.append(
            "risk.delever_triggered",
            {
                "position_id": position.position_id,
                "symbol": position.symbol,
                "long_venue": position.long_venue.value,
                "short_venue": position.short_venue.value,
                "reason": plan.reason,
                "requested_quantity": plan.requested_quantity,
                "adjusted_quantity": plan.adjusted_quantity,
                "remaining_quantity": position.matched_quantity,
                "risk_delever_step_count": position.risk_delever_step_count,
            },
        )

        # If we have a close executor, use it to execute the delever close
        if self.close_executor and plan.adjusted_quantity > 0:
            self.journal.append(
                "risk.delever_close_initiated",
                {
                    "position_id": position.position_id,
                    "quantity": plan.adjusted_quantity,
                },
            )
            await self.close_executor.execute_close(
                position, plan.reason, now_ms,
                long_price_hint=long_price_hint,
                short_price_hint=short_price_hint,
                total_quantity=plan.adjusted_quantity,
                state=self.state,
            )

        # Update position tracking
        position.last_risk_action_at_ms = now_ms
        position.risk_delever_step_count += 1
        position.last_risk_reason = plan.reason

        if position.risk_delever_step_count >= self.config.strategy.max_partial_delever_steps:
            self.journal.append(
                "risk.delever_limit_reached",
                {
                    "position_id": position.position_id,
                    "symbol": position.symbol,
                    "risk_delever_step_count": position.risk_delever_step_count,
                    "max_partial_delever_steps": self.config.strategy.max_partial_delever_steps,
                },
            )

    async def _execute_single_side_protection(
        self,
        position: OpenPosition,
        plan: RiskExecutionPlan,
        now_ms: int,
        long_price_hint: float = 0.0,
        short_price_hint: float = 0.0,
    ) -> None:
        """Execute single-side protection then enter fail-closed.

        V1: engine/risk.rs lines 1883-1891 (try_single_side_protection).
        Submits a protective reduce-only close when a CloseExecutor is available
        before entering fail-closed lifecycle.
        """
        self.journal.append(
            "risk.death_triggered",
            {
                "position_id": position.position_id,
                "symbol": position.symbol,
                "long_venue": position.long_venue.value,
                "short_venue": position.short_venue.value,
                "reason": plan.reason,
                "action": "single_side_protection",
                "requested_quantity": plan.requested_quantity,
                "adjusted_quantity": plan.adjusted_quantity,
                "remaining_quantity": position.matched_quantity,
            },
        )
        self.journal.append(
            "risk.single_side_protection_triggered",
            {
                "position_id": position.position_id,
                "symbol": position.symbol,
                "reason": plan.reason,
                "protection_venue": plan.long_liquidity_source or "",
                "protection_side": "buy" if plan.short_liquidity_source else "sell",
                "requested_quantity": plan.requested_quantity,
                "adjusted_quantity": plan.adjusted_quantity,
                "remaining_quantity": position.matched_quantity,
            },
        )

        # V1: Submit protective close order on the healthier leg
        if self.close_executor and position.matched_quantity > 0:
            total_quantity = min(position.matched_quantity, plan.adjusted_quantity if plan.adjusted_quantity > 0 else position.matched_quantity)
            self.journal.append(
                "risk.death_protection_close_initiated",
                {
                    "position_id": position.position_id,
                    "quantity": total_quantity,
                },
            )
            await self.close_executor.execute_close(
                position, f"death_protection:{plan.reason}", now_ms,
                long_price_hint=long_price_hint,
                short_price_hint=short_price_hint,
                total_quantity=total_quantity,
                state=self.state,
            )

        position.last_risk_action_at_ms = now_ms
        position.last_risk_reason = plan.reason
        position.single_side_protection_triggered = True

        # Enter fail-closed after protection
        self.state.lifecycle = EngineLifecycle.RISK_ONLY  # V1: FailClosed = RISK_ONLY + FAIL_CLOSED risk
        self.state.risk_mode = GlobalRiskMode.FAIL_CLOSED
        self.journal.append(
            "risk.fail_closed_entered",
            {
                "reason": f"single_side_protection:{position.position_id}",
            },
        )

    def _execute_fail_closed(
        self,
        position: OpenPosition,
        plan: RiskExecutionPlan,
        now_ms: int,
    ) -> None:
        """Enter fail-closed immediately (no protection close).

        V1: engine/risk.rs lines 1893-1905.
        """
        self.journal.append(
            "risk.death_triggered",
            {
                "position_id": position.position_id,
                "symbol": position.symbol,
                "reason": plan.reason,
                "action": "fail_closed",
            },
        )

        position.last_risk_action_at_ms = now_ms
        position.last_risk_reason = plan.reason

        self.state.lifecycle = EngineLifecycle.RISK_ONLY  # V1: FailClosed = RISK_ONLY + FAIL_CLOSED risk
        self.state.risk_mode = GlobalRiskMode.FAIL_CLOSED
        self.journal.append(
            "risk.fail_closed_entered",
            {
                "reason": f"death_line:{plan.reason}",
                "position_id": position.position_id,
            },
        )

    # ------------------------------------------------------------------
    # Full supervision tick
    # ------------------------------------------------------------------

    def supervise(
        self,
        now_ms: int,
        venue_health_ratios: dict[str, float],
        adapters: Optional[dict[Venue, VenueAdapter]] = None,
    ) -> None:
        """Run supervision tick: evaluate global risk, update mode, log conditions.

        V1: refresh_venue_health_supervisor + recompute_global_risk_mode.
        Collects per-venue health views (including private-stream health),
        aggregates them into global risk mode via V1's supervisor_action_mode.
        """
        strategy = self.config.strategy

        if not strategy.risk_monitor_enabled:
            return

        # V1: Collect per-venue health views including private-stream health
        venue_health_views: Optional[dict[Venue, VenueHealthView]] = None
        if adapters:
            venue_health_views = self._collect_venue_health_views(now_ms, adapters)

        # Update global risk mode from aggregate health + venue health views
        self.update_global_risk_mode(venue_health_ratios, venue_health_views)

        # Log risk line triggers (informational — real actions happen per-position)
        health = evaluate_risk_health(venue_health_ratios, strategy)

        if health.death_condition:
            self.journal.append(
                "risk.death_line_triggered",
                {"min_health_ratio": health.min_health_ratio, "ts_ms": now_ms},
            )
        elif health.delever_condition:
            self.journal.append(
                "risk.delever_line_triggered",
                {"min_health_ratio": health.min_health_ratio, "ts_ms": now_ms},
            )
        elif health.warning_condition and strategy.warning_pause_new_entries_enabled:
            self.journal.append(
                "risk.warning_line_triggered",
                {"min_health_ratio": health.min_health_ratio, "ts_ms": now_ms},
            )


