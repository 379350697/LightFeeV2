"""Restart recovery: snapshot load + journal replay to rebuild engine state."""

from __future__ import annotations

from lightfee.engine.state import EngineState, RecoveryWorkSnapshot
from lightfee.persistence.journal import Journal
from lightfee.persistence.snapshot_store import SnapshotStore
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


def build_recovery_snapshot(state: EngineState) -> RecoveryWorkSnapshot:
    """Assess current state for recoverability."""
    has_opens = len(state.open_positions) > 0
    has_pending = len(state.pending_entries) > 0
    has_closes = len(state.pending_closes) > 0

    # Ambiguous: we have open positions but don't know if they're valid
    ambiguous = has_opens and state.lifecycle == EngineLifecycle.BOOTING

    return RecoveryWorkSnapshot(
        has_open_positions=has_opens,
        has_pending_entries=has_pending,
        has_pending_closes=has_closes,
        ambiguous_state=ambiguous,
        lifecycle=state.lifecycle,
    )


def recover_from_snapshot(
    snapshot_store: SnapshotStore,
    journal: Journal,
) -> EngineState:
    """Load persisted state and replay journal to recover engine state."""
    state = EngineState()

    # Load snapshot
    snap = snapshot_store.read()
    if snap:
        lifecycle_str = snap.get("lifecycle", "booting")
        try:
            state.lifecycle = EngineLifecycle(lifecycle_str)
        except ValueError:
            state.lifecycle = EngineLifecycle.BOOTING

        risk_str = snap.get("risk_mode", "running")
        try:
            state.risk_mode = GlobalRiskMode(risk_str)
        except ValueError:
            state.risk_mode = GlobalRiskMode.RUNNING

        state.run_id = snap.get("run_id", "")
        state.started_at_ms = snap.get("started_at_ms", 0)
        state.last_tick_ms = snap.get("last_tick_ms", 0)
        state.tick_count = snap.get("tick_count", 0)
        state.venue_health = snap.get("venue_health", {})

    # Check recovery snapshot
    recovery = build_recovery_snapshot(state)

    if recovery.has_open_positions:
        if recovery.ambiguous_state:
            state.lifecycle = EngineLifecycle.RECONCILING
            state.risk_mode = GlobalRiskMode.REDUCE_ONLY
        else:
            state.lifecycle = EngineLifecycle.RECONCILING

    if state.lifecycle == EngineLifecycle.BOOTING:
        state.lifecycle = EngineLifecycle.RECONCILING

    return state


def is_ambiguous_live_truth(state: EngineState) -> bool:
    """Check if the live position truth is ambiguous (no private confirmation)."""
    return state.lifecycle == EngineLifecycle.RECONCILING and len(state.open_positions) > 0
