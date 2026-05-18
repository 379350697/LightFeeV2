"""Operator control command implementations.

Rust references:
- src/engine/state.rs:769-868 (apply_operator_command with atomic persistence)
"""

from __future__ import annotations

from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode
from lightfee.risk.operator import OperatorCommand, apply_operator_command


def execute_operator_command(
    command: OperatorCommand,
    current_risk: GlobalRiskMode,
    current_lifecycle: EngineLifecycle,
    has_blocking_recovery: bool = False,
    journal: object = None,
    state: object = None,
) -> tuple[GlobalRiskMode, EngineLifecycle, str]:
    """Execute an operator command and return updated state + message.

    V1 parity: when journal and state are provided, the risk/lifecycle
    transition is made durable via append_critical before returning.
    This prevents command loss on crash (V1 state.rs:769-868).
    """
    new_risk, new_lifecycle = apply_operator_command(
        command, current_risk, current_lifecycle, has_blocking_recovery
    )

    # V1: atomic persistence — journal critical event + persist state
    if journal is not None and state is not None:
        from lightfee.engine.bootstrap import wall_clock_now_ms
        journal.append_critical(
            wall_clock_now_ms(),
            "ops.command_applied",
            {
                "command": command.value if hasattr(command, 'value') else str(command),
                "previous_risk": current_risk.value,
                "new_risk": new_risk.value,
                "previous_lifecycle": current_lifecycle.value,
                "new_lifecycle": new_lifecycle.value,
            },
        )
        state.risk_mode = new_risk
        state.lifecycle = new_lifecycle

        # V1: operator-requested mode latch prevents auto-clear on clean restart.
        # clear_stale_fail_closed_if_recovery_clean() checks this to preserve
        # operator-intended FAIL_CLOSED across restarts (state.rs:476-487).
        if command == OperatorCommand.FAIL_CLOSED:
            state.operator.requested_mode = GlobalRiskMode.FAIL_CLOSED
        elif command == OperatorCommand.RESUME_IF_SAFE and new_risk == GlobalRiskMode.RUNNING:
            state.operator.requested_mode = None

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
