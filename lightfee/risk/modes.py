"""Global risk modes and lifecycle state matching Rust engine contract."""

from __future__ import annotations

from enum import Enum


class EngineLifecycle(Enum):
    """Engine lifecycle state (Rust: EngineLifecycle)."""

    BOOTING = "booting"
    RECONCILING = "reconciling"
    RISK_ONLY = "risk_only"
    RUNNING = "running"
    FAIL_CLOSED = "fail_closed"


class GlobalRiskMode(Enum):
    """Global risk mode ordered by severity (Rust: GlobalRiskMode)."""

    RUNNING = "running"
    ENTRY_PAUSED = "entry_paused"
    REDUCE_ONLY = "reduce_only"
    FAIL_CLOSED = "fail_closed"

    def at_least(self, other: GlobalRiskMode) -> bool:
        order = {GlobalRiskMode.RUNNING: 0, GlobalRiskMode.ENTRY_PAUSED: 1, GlobalRiskMode.REDUCE_ONLY: 2, GlobalRiskMode.FAIL_CLOSED: 3}
        return order[self] >= order[other]

    def max(self, other: GlobalRiskMode) -> GlobalRiskMode:
        return self if self.at_least(other) else other


class EngineMode(Enum):
    """Synchronized engine mode (Rust: EngineMode)."""

    RUNNING = "running"
    RECOVERING = "recovering"
    FAIL_CLOSED = "fail_closed"


def derive_engine_mode(lifecycle: EngineLifecycle, risk: GlobalRiskMode) -> EngineMode:
    """Derive synchronized engine mode from lifecycle + risk mode.

    V1: FailClosed = RISK_ONLY lifecycle + FAIL_CLOSED risk mode.
    The FAIL_CLOSED lifecycle check is retained for backward compat
    with pre-C-R1 persisted states (normalize_engine_state migrates them).
    """
    if risk == GlobalRiskMode.FAIL_CLOSED or lifecycle.value == "fail_closed":
        return EngineMode.FAIL_CLOSED
    if lifecycle in (EngineLifecycle.BOOTING, EngineLifecycle.RECONCILING, EngineLifecycle.RISK_ONLY):
        return EngineMode.RECOVERING
    if lifecycle == EngineLifecycle.RUNNING and risk == GlobalRiskMode.RUNNING:
        return EngineMode.RUNNING
    return EngineMode.RECOVERING
