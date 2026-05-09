"""Operator control command implementations."""

from __future__ import annotations

from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from lightfee.risk.operator import OperatorCommand, apply_operator_command


def execute_operator_command(
    command: OperatorCommand,
    current_risk: GlobalRiskMode,
    current_lifecycle: EngineLifecycle,
    has_blocking_recovery: bool = False,
) -> tuple[GlobalRiskMode, EngineLifecycle, str]:
    """Execute an operator command and return updated state + message."""
    new_risk, new_lifecycle = apply_operator_command(
        command, current_risk, current_lifecycle, has_blocking_recovery
    )

    messages = {
        OperatorCommand.PAUSE_ENTRY: "Entries paused",
        OperatorCommand.REDUCE_ONLY: "Entered reduce-only mode",
        OperatorCommand.FAIL_CLOSED: "Entered fail-closed mode",
        OperatorCommand.RECONCILE_NOW: "Reconciliation triggered",
        OperatorCommand.RESUME_IF_SAFE: (
            "Resumed" if new_risk == GlobalRiskMode.RUNNING else "Cannot resume: unsafe or blocking recovery"
        ),
    }

    return new_risk, new_lifecycle, messages.get(command, "Command executed")
