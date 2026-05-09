"""Engine lifecycle transitions matching Rust engine state machine."""

from __future__ import annotations

from lightfee.engine.state import EngineState
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


def set_lifecycle(state: EngineState, lifecycle: EngineLifecycle) -> None:
    state.lifecycle = lifecycle


def set_global_risk_mode(state: EngineState, risk_mode: GlobalRiskMode) -> None:
    state.risk_mode = state.risk_mode.max(risk_mode)


def enter_fail_closed(state: EngineState) -> None:
    state.lifecycle = EngineLifecycle.RISK_ONLY
    state.risk_mode = GlobalRiskMode.FAIL_CLOSED


def is_running(state: EngineState) -> bool:
    return state.lifecycle == EngineLifecycle.RUNNING and state.risk_mode == GlobalRiskMode.RUNNING


def can_enter_new_positions(state: EngineState) -> bool:
    return is_running(state)
