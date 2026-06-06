"""Engine lifecycle transitions matching Rust engine state machine."""

from __future__ import annotations

from enum import Enum

from lightfee.engine.state import EngineState
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


class LiveStartupPhase(Enum):
    """Ordered adapter startup phases (Rust: LiveStartupPhase)."""

    PRIVATE_STREAMS = "private_streams"
    MARKET_STREAMS = "market_streams"
    LOCAL_L2 = "local_l2"


def set_lifecycle(state: EngineState, lifecycle: EngineLifecycle) -> None:
    state.lifecycle = lifecycle


def set_global_risk_mode(state: EngineState, risk_mode: GlobalRiskMode) -> None:
    """V1: EngineState.set_global_risk_mode (state.rs:336-360) — direct assignment.

    V1 directly sets self.state.global_risk_mode = risk_mode without a max()
    gate. This allows recovery to transition FailClosed → Running. Callers
    are responsible for only escalating risk when appropriate.
    """
    state.risk_mode = risk_mode


def enter_fail_closed(state: EngineState) -> None:
    state.lifecycle = EngineLifecycle.RISK_ONLY
    state.risk_mode = GlobalRiskMode.FAIL_CLOSED


def is_running(state: EngineState) -> bool:
    return state.lifecycle == EngineLifecycle.RUNNING and state.risk_mode == GlobalRiskMode.RUNNING


def can_enter_new_positions(state: EngineState) -> bool:
    return is_running(state)


def transition_to_reconciling(state: EngineState) -> None:
    """Transition to RECONCILING from BOOTING."""
    set_lifecycle(state, EngineLifecycle.RECONCILING)


def transition_to_running(state: EngineState) -> None:
    """Transition to RUNNING after successful reconciliation."""
    set_lifecycle(state, EngineLifecycle.RUNNING)
    set_global_risk_mode(state, GlobalRiskMode.RUNNING)


def clear_risk_mode_for_recovery(state: EngineState, core_decision: object | None = None) -> bool:
    """Reset risk mode only after the recovery decision core permits clear.

    V1 clears recovery state at recovery completion. In V2 the decision that
    recovery is complete must come from V1RecoveryDecisionCore, so this helper
    is a state-application adapter rather than an independent stale cleaner.
    """
    from lightfee.engine.recovery_decision_core import RecoveryDecisionKind

    if core_decision is None:
        return False
    if getattr(core_decision, "block_reason", None):
        return False
    if not bool(getattr(core_decision, "entry_allowed", False)):
        return False
    if getattr(core_decision, "kind", None) not in {
        RecoveryDecisionKind.RUNNING_CLEAN,
        RecoveryDecisionKind.RUNNING_WITH_EVIDENCE_GAP,
    }:
        return False
    if state.recovery_blocked_reason and not bool(
        getattr(core_decision, "clear_previous_block", False)
    ):
        return False

    state.risk_mode = GlobalRiskMode.RUNNING
    state.lifecycle = EngineLifecycle.RUNNING
    state.recovery_blocked_reason = None
    state.recovery_blocked_at_ms = 0
    return True
