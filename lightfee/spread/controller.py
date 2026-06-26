"""Spread-reversion trading controller.

The controller returns intents only. Live execution should route those intents
through the shared order/risk adapters; it does not submit orders itself.
"""

from __future__ import annotations

from lightfee.config.schema import StrategyConfig
from lightfee.spread.models import (
    SpreadDecision,
    SpreadExitIntent,
    SpreadOrderIntent,
    SpreadPosition,
    SpreadReversionCandidate,
    SpreadTradingState,
)
from lightfee.spread.modules import DegradationState, ExitRiskClassifier


class SpreadTradingController:
    def __init__(self, strategy: StrategyConfig) -> None:
        self.strategy = strategy

    def evaluate_entry(
        self,
        candidate: SpreadReversionCandidate,
        *,
        state: SpreadTradingState,
        now_ms: int,
    ) -> SpreadDecision:
        if not bool(getattr(self.strategy, "spread_reversion_enabled", False)):
            return SpreadDecision(False, "spread_reversion_disabled")
        if candidate.strategy_bucket != "spread_reversion":
            return SpreadDecision(False, "spread_candidate_wrong_bucket")
        if candidate.signal_status != "entry_ready":
            return SpreadDecision(False, "spread_candidate_not_entry_ready")
        max_age = int(getattr(self.strategy, "spread_signal_ttl_ms", 1000) or 0)
        if max_age > 0 and now_ms - int(candidate.signal_ts_ms or 0) > max_age:
            return SpreadDecision(False, "spread_signal_stale")
        if float(candidate.z_score) < float(getattr(self.strategy, "spread_entry_z", 2.0) or 0.0):
            return SpreadDecision(False, "spread_entry_z_below_threshold")
        if float(candidate.net_edge_bps) < float(
            getattr(self.strategy, "spread_min_net_edge_bps", 5.0) or 0.0
        ):
            return SpreadDecision(False, "spread_net_edge_below_threshold")
        max_positions = int(
            getattr(self.strategy, "spread_max_concurrent_positions", 1) or 0
        )
        if max_positions >= 0 and len(state.open_positions) >= max_positions:
            return SpreadDecision(False, "spread_max_concurrent_positions_reached")
        if state.pending_entry_count > 0:
            return SpreadDecision(False, "spread_pending_entry_exists")
        if state.pending_close_count > 0:
            return SpreadDecision(False, "spread_pending_close_exists")
        if self._has_symbol_conflict(candidate, state):
            return SpreadDecision(False, "spread_symbol_already_open")

        notional = self._entry_notional(candidate, state)
        if notional <= 0.0:
            return SpreadDecision(False, "spread_entry_notional_not_positive")
        if candidate.capacity_quote > 0.0 and candidate.capacity_quote < notional:
            return SpreadDecision(False, "spread_capacity_below_notional")
        return SpreadDecision(
            True,
            "spread_entry_allowed",
            intent=SpreadOrderIntent(
                candidate_id=candidate.candidate_id,
                symbol=candidate.symbol,
                long_venue=candidate.long_venue,
                short_venue=candidate.short_venue,
                entry_notional_quote=notional,
                reason="spread_entry_allowed",
            ),
            evidence={
                "z_score": candidate.z_score,
                "net_edge_bps": candidate.net_edge_bps,
                "entry_notional_quote": notional,
            },
        )

    def evaluate_exit(
        self,
        position: SpreadPosition,
        candidate: SpreadReversionCandidate | None,
        *,
        now_ms: int,
    ) -> SpreadDecision:
        if position.strategy_bucket != "spread_reversion":
            return SpreadDecision(False, "spread_position_wrong_bucket")
        max_hold = int(getattr(self.strategy, "spread_max_hold_ms", 0) or 0)
        if max_hold > 0 and now_ms - int(position.opened_at_ms or 0) > max_hold:
            return self._exit(position, "spread_max_hold_elapsed")
        if candidate is None:
            return self._degradation_decision(
                position,
                ExitRiskClassifier().classify(signal_missing=True),
            )
        degradation = self._degradation_state(candidate)
        if degradation is not DegradationState.HEALTHY:
            return self._degradation_decision(position, degradation)
        stop_z = float(getattr(self.strategy, "spread_stop_z", 3.5) or 0.0)
        if stop_z > 0.0 and float(candidate.z_score) >= stop_z:
            return self._exit(position, "spread_stop_z_reached")
        exit_z = float(getattr(self.strategy, "spread_exit_z", 0.5) or 0.0)
        if abs(float(candidate.z_score)) <= exit_z:
            return self._exit(position, "spread_converged")
        return SpreadDecision(False, "spread_exit_not_due")

    def _entry_notional(
        self,
        candidate: SpreadReversionCandidate,
        state: SpreadTradingState,
    ) -> float:
        live_notional = float(
            getattr(self.strategy, "spread_live_notional_quote", 20.0) or 0.0
        )
        candidate_notional = float(candidate.entry_notional_quote or 0.0)
        if candidate_notional > 0.0:
            live_notional = min(live_notional, candidate_notional)
        gross_cap = float(getattr(self.strategy, "spread_max_gross_quote", 50.0) or 0.0)
        remaining = gross_cap - float(state.global_gross_quote or 0.0)
        if gross_cap > 0.0:
            live_notional = min(live_notional, remaining)
        return max(live_notional, 0.0)

    @staticmethod
    def _has_symbol_conflict(
        candidate: SpreadReversionCandidate,
        state: SpreadTradingState,
    ) -> bool:
        symbol = candidate.symbol.upper()
        return any(pos.symbol.upper() == symbol for pos in state.open_positions)

    @staticmethod
    def _degradation_state(candidate: SpreadReversionCandidate) -> DegradationState:
        try:
            return DegradationState(str(candidate.degradation_state or "healthy"))
        except ValueError:
            return DegradationState.OBSERVE_DEGRADED

    def _degradation_decision(
        self,
        position: SpreadPosition,
        state: DegradationState,
    ) -> SpreadDecision:
        if state is DegradationState.RECOVERY_REQUIRED:
            return SpreadDecision(False, "spread_recovery_required")
        if state is DegradationState.FORCED_EXIT:
            return self._exit(position, "spread_forced_exit")
        if state is DegradationState.PROTECTIVE_EXIT_READY:
            return self._exit(position, "spread_protective_exit_ready")
        if state is DegradationState.OBSERVE_DEGRADED:
            return SpreadDecision(False, "spread_exit_observe_degraded")
        return SpreadDecision(False, "spread_exit_not_due")

    @staticmethod
    def _exit(position: SpreadPosition, reason: str) -> SpreadDecision:
        return SpreadDecision(
            True,
            reason,
            intent=SpreadExitIntent(
                position_id=position.position_id,
                symbol=position.symbol,
                long_venue=position.long_venue,
                short_venue=position.short_venue,
                reason=reason,
            ),
        )
