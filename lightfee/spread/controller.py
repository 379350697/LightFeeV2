"""Spread-reversion trading controller.

The controller returns intents only. Live execution should route those intents
through the shared order/risk adapters; it does not submit orders itself.
"""

from __future__ import annotations

import math

from lightfee.config.schema import StrategyConfig
from lightfee.spread.models import (
    SpreadDecision,
    SpreadExitIntent,
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
        if self.strategy.spread_reversion_enabled is not True:
            return SpreadDecision(False, "spread_reversion_disabled")
        # The controller is intentionally live-ready but has no execution
        # authority until the separate release gate is explicitly opened.
        if self.strategy.spread_live_enabled is not True:
            return SpreadDecision(False, "spread_live_disabled")
        if candidate.strategy_bucket != "spread_reversion":
            return SpreadDecision(False, "spread_candidate_wrong_bucket")
        if candidate.signal_status != "entry_ready":
            return SpreadDecision(False, "spread_candidate_not_entry_ready")
        if candidate.economics_complete is not True:
            return SpreadDecision(False, "spread_candidate_economics_incomplete")
        if candidate.fee_evidence_complete is not True:
            return SpreadDecision(False, "spread_candidate_fee_evidence_incomplete")
        # ``economics_complete`` alone is not an admission credential.  A
        # hand-built candidate or an older snapshot must not skip the contract
        # compatibility proof produced by the signed-basis builder.
        if str(candidate.contract_normalization_status or "").lower() != "complete":
            return SpreadDecision(False, "spread_contract_normalization_incomplete")
        expected_calculation_version = (
            "spread_v3_cost_normalized_reversion"
            if str(self.strategy.spread_model_epoch or "").startswith("v3_")
            else "spread_v2_signed_reversion"
        )
        if candidate.calculation_version != expected_calculation_version:
            return SpreadDecision(False, "spread_candidate_calculation_version_mismatch")
        if candidate.model_epoch != self.strategy.spread_model_epoch:
            return SpreadDecision(False, "spread_candidate_model_epoch_mismatch")
        if candidate.opportunity_label == "single_venue_dislocation":
            return SpreadDecision(False, "spread_single_venue_dislocation_paper_only")
        max_age = int(self.strategy.spread_signal_ttl_ms)
        signal_ts = int(candidate.signal_ts_ms or 0)
        if signal_ts <= 0 or signal_ts > now_ms or (
            max_age > 0 and now_ms - signal_ts > max_age
        ):
            return SpreadDecision(False, "spread_signal_stale")
        if abs(float(candidate.z_score)) < abs(float(self.strategy.spread_entry_z)):
            return SpreadDecision(False, "spread_entry_z_below_threshold")
        minimum_edge = float(self.strategy.spread_min_net_edge_bps)
        expected_edge = float(candidate.expected_net_edge_bps)
        worst_edge = float(candidate.worst_case_edge_bps)
        if not math.isfinite(expected_edge) or not math.isfinite(worst_edge):
            return SpreadDecision(False, "spread_candidate_economics_nonfinite")
        if expected_edge < minimum_edge:
            return SpreadDecision(False, "spread_expected_net_edge_below_threshold")
        if worst_edge < minimum_edge:
            return SpreadDecision(False, "spread_worst_case_edge_below_threshold")
        max_positions = int(self.strategy.spread_max_concurrent_positions)
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
        # This controller deliberately stops short of producing a live entry
        # intent in any current model epoch.  It still runs the full
        # admission calculation above so paper/research can see the exact
        # hypothetical notional and block reasons, but spread can only become
        # live after a separate executor/recovery/truth design is approved.
        return SpreadDecision(
            False,
            "spread_live_not_supported",
            intent=None,
            evidence={
                "z_score": candidate.z_score,
                "net_edge_bps": candidate.net_edge_bps,
                "hypothetical_entry_notional_quote": notional,
                "entry_intent_suppressed": True,
                "model_epoch": candidate.model_epoch,
                "calculation_version": candidate.calculation_version,
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
        max_hold = int(self.strategy.spread_max_hold_ms)
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
        stop_z = float(self.strategy.spread_stop_z)
        if stop_z > 0.0 and abs(float(candidate.z_score)) >= stop_z:
            return self._exit(position, "spread_stop_z_reached")
        exit_z = float(self.strategy.spread_exit_z)
        if abs(float(candidate.z_score)) <= exit_z:
            return self._exit(position, "spread_converged")
        return SpreadDecision(False, "spread_exit_not_due")

    def _entry_notional(
        self,
        candidate: SpreadReversionCandidate,
        state: SpreadTradingState,
    ) -> float:
        live_notional = float(self.strategy.spread_live_notional_quote)
        candidate_notional = float(candidate.entry_notional_quote or 0.0)
        if candidate_notional > 0.0:
            live_notional = min(live_notional, candidate_notional)
        gross_cap = float(self.strategy.spread_max_gross_quote)
        if gross_cap <= 0.0:
            return 0.0
        # State tracks total absolute exposure across both legs.  A new
        # delta-neutral pair consumes twice its common per-leg notional, so
        # only half of remaining gross capacity is admissible for either leg.
        remaining_gross = max(gross_cap - float(state.global_gross_quote or 0.0), 0.0)
        live_notional = min(live_notional, remaining_gross / 2.0)
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
