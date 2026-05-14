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


def clear_risk_mode_for_recovery(state: EngineState) -> None:
    """Explicitly reset risk_mode to RUNNING after successful startup recovery.

    V1: EngineState.clear_recovery_blocked_state() in recovery.rs —
    when recovery completes without blocking work, the fail_closed / reduced
    risk mode must be cleared. set_global_risk_mode uses .max() which keeps
    FAIL_CLOSED(3) > RUNNING(0) sticky — this bypass exists specifically
    for the recovery completion path.
    """
    state.risk_mode = GlobalRiskMode.RUNNING
    state.lifecycle = EngineLifecycle.RUNNING
