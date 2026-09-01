"""Operator control commands: pause, reduce-only, fail-closed, reconcile, resume."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


class OperatorCommand(Enum):
    PAUSE_ENTRY = "pause_entry"
    REDUCE_ONLY = "reduce_only"
    FAIL_CLOSED = "fail_closed"
    RECONCILE_NOW = "reconcile_now"
    RESUME_IF_SAFE = "resume_if_safe"


@dataclass
class OperatorControlState:
    requested_mode: Optional[GlobalRiskMode] = None
    pending_reconcile: bool = False


def apply_operator_command(
    command: OperatorCommand,
    current_risk: GlobalRiskMode,
    current_lifecycle: EngineLifecycle,
    has_blocking_recovery: bool = False,
) -> tuple[GlobalRiskMode, EngineLifecycle]:
    """Apply an operator command, returning (new_risk_mode, new_lifecycle)."""
    if command == OperatorCommand.PAUSE_ENTRY:
        return (current_risk.max(GlobalRiskMode.ENTRY_PAUSED), current_lifecycle)
    elif command == OperatorCommand.REDUCE_ONLY:
        return (current_risk.max(GlobalRiskMode.REDUCE_ONLY), current_lifecycle)
    elif command == OperatorCommand.FAIL_CLOSED:
        return (GlobalRiskMode.FAIL_CLOSED, EngineLifecycle.RISK_ONLY)
    elif command == OperatorCommand.RESUME_IF_SAFE:
        if has_blocking_recovery:
            return (current_risk, current_lifecycle)
        return (GlobalRiskMode.RUNNING, EngineLifecycle.RUNNING)
    return (current_risk, current_lifecycle)
