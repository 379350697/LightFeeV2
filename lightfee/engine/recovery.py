"""Restart recovery: snapshot load + journal replay to rebuild engine state.

Rust references:
- src/engine/recovery.rs (finalize_startup_position_recovery, reconcile_dust_residuals,
  process_pending_close_reconciliations, close reconciliation lifecycle)
- src/engine/state.rs (EngineState recovery/pending fields)
- src/runtime_state/persisted_engine.rs (normalize_engine_state_positions,
  persistent_state_view)
- src/observability_ops/replay_bridge.rs (journal replay state reconstruction)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from lightfee.engine.state import (
    EngineState,
    OpenPosition,
    OperatorControlState,
    PendingClose,
    PendingEntry,
    RecoveryWorkSnapshot,
)
from lightfee.persistence.journal import Journal
from lightfee.persistence.snapshot_store import SnapshotStore
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


# ---------------------------------------------------------------------------
# Recovery data structures (Rust V1: EngineState sub-structures)
# ---------------------------------------------------------------------------

@dataclass
class DustResidual:
    """A position too small to close (below venue min-notional).

    Rust V1: DustResidual in engine/state.rs — tracked separately from open
    positions so the engine doesn't try to close them. Periodically checked
    via reconcile_dust_residuals().
    """

    position_id: str
    symbol: str
    long_venue: str
    short_venue: str
    long_size: float = 0.0
    short_size: float = 0.0
    leg_notional_quote: float = 0.0
    venue_min_notional_quote: float = 0.0
    terminal_reason: str = "exchange_min_notional_dust"
    recorded_at_ms: int = 0
    last_checked_at_ms: int = 0

    @property
    def is_dust(self) -> bool:
        return self.leg_notional_quote < self.venue_min_notional_quote


@dataclass
class PendingCloseReconciliation:
    """Track a close whose fill needs live-venue confirmation.

    Rust V1: PendingCloseReconciliation in engine/state.rs — enqueued after
    a close order is submitted but outcome is uncertain or partial.
    """

    position_id: str
    symbol: str = ""
    kind: str = "final_close"
    reason: str = ""
    closed_at_ms: int = 0
    attempt_count: int = 0
    next_attempt_ms: int = 0
    created_cycle: int = 0


@dataclass
class ResidualExposureTask:
    """One-sided residual that needs repair (hedge-reject aftermath).

    Rust V1: ResidualExposureTask in engine/residual.rs — created when
    one leg fills but the other rejects, leaving lopsided exposure.
    """

    position_id: str
    pair_id: str
    symbol: str
    long_venue: str
    short_venue: str
    origin: str  # "hedge_reject", "final_close", "live_recovery"
    repair_venue: str
    repair_side: str
    repair_quantity: float
    created_cycle: int = 0
    created_at_ms: int = 0
    deadline_ms: int = 0
    attempt_count: int = 0
    next_attempt_ms: int = 0


@dataclass
class RecoveryBlockedState:
    """Reason and timestamp for recovery being blocked.

    Rust V1: recovery_blocked_reason + recovery_blocked_at_ms in EngineState.
    """

    reason: str
    blocked_at_ms: int = 0


@dataclass
class RecoveredState:
    """Fully reconstructed state after snapshot load + journal replay."""

    state: EngineState
    recovery_work: RecoveryWorkSnapshot


# ---------------------------------------------------------------------------
# Recovery snapshot assessment
# ---------------------------------------------------------------------------

def build_recovery_snapshot(state: EngineState) -> RecoveryWorkSnapshot:
    """Assess current state for recoverability.

    Rust V1: EngineState.recovery_work_snapshot() — counts open positions,
    pending entries, pending closes, residual repairs, and entry hedges.
    """
    has_opens = len(state.open_positions) > 0
    has_pending = len(state.pending_entries) > 0
    has_closes = len(state.pending_closes) > 0

    ambiguous = has_opens and state.lifecycle == EngineLifecycle.BOOTING

    return RecoveryWorkSnapshot(
        has_open_positions=has_opens,
        has_pending_entries=has_pending,
        has_pending_closes=has_closes,
        ambiguous_state=ambiguous,
        lifecycle=state.lifecycle,
    )


# ---------------------------------------------------------------------------
# State reconstruction from snapshot + journal
# ---------------------------------------------------------------------------

def _deserialize_open_position(data: dict[str, Any]) -> OpenPosition:
    """Deserialize an OpenPosition from snapshot dict."""
    from lightfee.core.domain import Venue as DomainVenue

    long_venue_str = data.get("long_venue", "binance")
    short_venue_str = data.get("short_venue", "okx")
    try:
        long_venue = DomainVenue.from_str(long_venue_str)
    except (ValueError, AttributeError):
        long_venue = DomainVenue.BINANCE
    try:
        short_venue = DomainVenue.from_str(short_venue_str)
    except (ValueError, AttributeError):
        short_venue = DomainVenue.OKX

    return OpenPosition(
        position_id=data.get("position_id", ""),
        symbol=data.get("symbol", ""),
        long_venue=long_venue,
        short_venue=short_venue,
        long_quantity=float(data.get("long_quantity", 0)),
        short_quantity=float(data.get("short_quantity", 0)),
        long_entry_price=float(data.get("long_entry_price", 0)),
        short_entry_price=float(data.get("short_entry_price", 0)),
        opened_at_ms=int(data.get("opened_at_ms", 0)),
        matched_quantity=float(data.get("matched_quantity", 0)),
        captured_funding_quote=float(data.get("captured_funding_quote", 0)),
        funding_captured=bool(data.get("funding_captured", False)),
        peak_net_quote=float(data.get("peak_net_quote", 0)),
        current_net_quote=float(data.get("current_net_quote", 0)),
        realized_price_pnl_quote=float(data.get("realized_price_pnl_quote", 0)),
        realized_exit_fee_quote=float(data.get("realized_exit_fee_quote", 0)),
        long_entry_fee_quote=float(data.get("long_entry_fee_quote", 0)),
        short_entry_fee_quote=float(data.get("short_entry_fee_quote", 0)),
    )


def _serialize_open_position(pos: OpenPosition) -> dict[str, Any]:
    """Serialize an OpenPosition for snapshot storage."""
    return {
        "position_id": pos.position_id,
        "symbol": pos.symbol,
        "long_venue": pos.long_venue.value,
        "short_venue": pos.short_venue.value,
        "long_quantity": pos.long_quantity,
        "short_quantity": pos.short_quantity,
        "long_entry_price": pos.long_entry_price,
        "short_entry_price": pos.short_entry_price,
        "opened_at_ms": pos.opened_at_ms,
        "matched_quantity": pos.matched_quantity,
        "captured_funding_quote": pos.captured_funding_quote,
        "funding_captured": pos.funding_captured,
        "peak_net_quote": pos.peak_net_quote,
        "current_net_quote": pos.current_net_quote,
        "realized_price_pnl_quote": pos.realized_price_pnl_quote,
        "realized_exit_fee_quote": pos.realized_exit_fee_quote,
        "long_entry_fee_quote": pos.long_entry_fee_quote,
        "short_entry_fee_quote": pos.short_entry_fee_quote,
    }


def _restore_state_from_snapshot_dict(snap: dict[str, Any]) -> EngineState:
    """Restore EngineState fields from a snapshot dict (without journal replay)."""
    state = EngineState()

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

    # Restore operator control state
    op = snap.get("operator", {})
    if isinstance(op, dict):
        if op.get("requested_mode"):
            try:
                state.operator.requested_mode = GlobalRiskMode(str(op["requested_mode"]))
            except ValueError:
                pass
        state.operator.pending_reconcile = bool(op.get("pending_reconcile", False))

    # Restore open positions
    pos_dict = snap.get("open_positions", {})
    if isinstance(pos_dict, dict):
        for pid, pdata in pos_dict.items():
            if isinstance(pdata, dict):
                state.open_positions[pid] = _deserialize_open_position(pdata)

    # Restore pending entries
    pend_dict = snap.get("pending_entries", {})
    if isinstance(pend_dict, dict):
        for pend_id, pdata in pend_dict.items():
            if isinstance(pdata, dict):
                from lightfee.core.domain import Side as DomainSide, Venue

                long_venue_str = str(pdata.get("long_venue", "binance"))
                short_venue_str = str(pdata.get("short_venue", "okx"))
                try:
                    long_venue = Venue.from_str(long_venue_str)
                except (ValueError, AttributeError):
                    long_venue = Venue.BINANCE
                try:
                    short_venue = Venue.from_str(short_venue_str)
                except (ValueError, AttributeError):
                    short_venue = Venue.OKX

                long_side_str = pdata.get("long_side", "buy")
                short_side_str = pdata.get("short_side", "sell")
                try:
                    long_side = DomainSide(long_side_str)
                except ValueError:
                    long_side = DomainSide.BUY
                try:
                    short_side = DomainSide(short_side_str)
                except ValueError:
                    short_side = DomainSide.SELL

                state.pending_entries[pend_id] = PendingEntry(
                    pending_id=pdata.get("pending_id", pend_id),
                    symbol=pdata.get("symbol", ""),
                    long_venue=long_venue,
                    short_venue=short_venue,
                    target_quantity=float(pdata.get("target_quantity", 0)),
                    long_side=long_side,
                    short_side=short_side,
                    created_at_ms=int(pdata.get("created_at_ms", 0)),
                    maker_order_id=str(pdata.get("maker_order_id", "")),
                    hedge_order_id=str(pdata.get("hedge_order_id", "")),
                    maker_leg_filled=float(pdata.get("maker_leg_filled", 0)),
                    hedge_leg_filled=float(pdata.get("hedge_leg_filled", 0)),
                    uncertain_outcome=bool(pdata.get("uncertain_outcome", False)),
                    entry_type=str(pdata.get("entry_type", "")),
                    maker_price=float(pdata.get("maker_price", 0)),
                    long_quantity=float(pdata.get("long_quantity", 0)),
                    short_quantity=float(pdata.get("short_quantity", 0)),
                )

    # Restore pending closes
    close_dict = snap.get("pending_closes", {})
    if isinstance(close_dict, dict):
        for cid, cdata in close_dict.items():
            if isinstance(cdata, dict):
                state.pending_closes[cid] = PendingClose(
                    close_id=cdata.get("close_id", cid),
                    position_id=cdata.get("position_id", ""),
                    reason=cdata.get("reason", ""),
                    created_at_ms=int(cdata.get("created_at_ms", 0)),
                    long_order_id=str(cdata.get("long_order_id", "")),
                    short_order_id=str(cdata.get("short_order_id", "")),
                    long_closed=float(cdata.get("long_closed", 0)),
                    short_closed=float(cdata.get("short_closed", 0)),
                    long_uncertain=bool(cdata.get("long_uncertain", False)),
                    short_uncertain=bool(cdata.get("short_uncertain", False)),
                )

    # Restore local-L2 state (V1 parity)
    retained = snap.get("retained_local_l2_books", [])
    if isinstance(retained, list):
        state.retained_local_l2_books = retained

    books_snap = snap.get("local_l2_books_snapshot", [])
    if isinstance(books_snap, list):
        state.local_l2_books_snapshot = books_snap

    session_snap = snap.get("local_l2_session_snapshot", [])
    if isinstance(session_snap, list):
        state.local_l2_session_snapshot = session_snap

    return state


def _apply_journal_replay_to_state(
    state: EngineState,
    records: list[dict[str, Any]],
) -> None:
    """Replay journal records against an already-restored EngineState.

    Processes events that happened AFTER the snapshot was written:
    - entry.opened / recovery.live_detected → add position
    - exit.closed / recovery.flat → remove position
    - exit.partial_closed → reduce matched_quantity
    - runtime.lifecycle_changed / risk_mode_changed → update modes
    """
    for record in records:
        kind = record.get("kind", "")
        payload = record.get("payload", {})

        if kind in ("entry.opened", "recovery.live_detected"):
            pid = payload.get("position_id", "")
            if pid and pid not in state.open_positions:
                state.open_positions[pid] = _deserialize_open_position(payload)

        elif kind in ("exit.closed", "exit.reconciled", "recovery.flat"):
            pid = payload.get("position_id", "")
            if pid:
                state.open_positions.pop(pid, None)
                state.pending_closes.pop(pid, None)

        elif kind == "exit.partial_closed":
            pid = payload.get("position_id", "")
            if pid and pid in state.open_positions:
                pos = state.open_positions[pid]
                matched_closed = float(payload.get("matched_closed", 0))
                if matched_closed > 0:
                    remaining = max(pos.matched_quantity - matched_closed, 0)
                    pos.long_quantity = max(pos.long_quantity - matched_closed, 0)
                    pos.short_quantity = max(pos.short_quantity - matched_closed, 0)
                    pos.matched_quantity = remaining

        elif kind == "runtime.lifecycle_changed":
            to_val = payload.get("to")
            if to_val:
                try:
                    state.lifecycle = EngineLifecycle(str(to_val))
                except ValueError:
                    pass

        elif kind == "runtime.risk_mode_changed":
            to_val = payload.get("to")
            if to_val:
                try:
                    state.risk_mode = GlobalRiskMode(str(to_val))
                except ValueError:
                    pass


def recover_from_snapshot(
    snapshot_store: SnapshotStore,
    journal: Journal,
) -> EngineState:
    """Load persisted state, replay journal, and reconstruct engine state.

    Rust V1: Engine startup loads FileStateStore snapshot, replays journal,
    and enters RECONCILING if any recovery work exists.

    Flow:
    1. Load snapshot dict → restore base EngineState
    2. Read journal records → replay events that happened after snapshot
    3. Assess recovery needs → set lifecycle/risk_mode
    """
    snap = snapshot_store.read()
    state = _restore_state_from_snapshot_dict(snap) if snap else EngineState()

    # Replay journal records to catch events after last snapshot
    journal_records = journal.read_all()
    if journal_records:
        _apply_journal_replay_to_state(state, journal_records)

    # Assess recovery needs
    recovery = build_recovery_snapshot(state)

    if recovery.has_open_positions:
        if recovery.ambiguous_state:
            state.lifecycle = EngineLifecycle.RECONCILING
            state.risk_mode = state.risk_mode.max(GlobalRiskMode.REDUCE_ONLY)
        else:
            state.lifecycle = EngineLifecycle.RECONCILING

    if state.lifecycle == EngineLifecycle.BOOTING:
        state.lifecycle = EngineLifecycle.RECONCILING

    # Clean state with no recovery work → can run
    if not recovery.has_open_positions and not recovery.has_pending_entries and not recovery.has_pending_closes:
        if state.lifecycle == EngineLifecycle.RECONCILING:
            state.lifecycle = EngineLifecycle.RUNNING

    return state


def is_ambiguous_live_truth(state: EngineState) -> bool:
    """Check if the live position truth is ambiguous (no private confirmation)."""
    return state.lifecycle == EngineLifecycle.RECONCILING and len(state.open_positions) > 0


# ---------------------------------------------------------------------------
# State snapshot serialization (Rust V1: persistent_state_view)
# ---------------------------------------------------------------------------

def build_persistent_state_view(state: EngineState) -> dict[str, Any]:
    """Build a snapshot-suitable dict of engine state.

    Rust V1: persistent_state_view() strips volatile scan fields and
    normalizes positions before serialization.
    """
    view = state.to_dict()

    # Add open position details
    view["open_positions"] = {
        pid: _serialize_open_position(pos)
        for pid, pos in state.open_positions.items()
    }

    # Add pending entry details
    view["pending_entries"] = {
        pid: {
            "pending_id": p.pending_id,
            "symbol": p.symbol,
            "long_venue": p.long_venue.value if hasattr(p.long_venue, "value") else str(p.long_venue),
            "short_venue": p.short_venue.value if hasattr(p.short_venue, "value") else str(p.short_venue),
            "target_quantity": p.target_quantity,
            "long_side": p.long_side.value if hasattr(p.long_side, "value") else str(p.long_side),
            "short_side": p.short_side.value if hasattr(p.short_side, "value") else str(p.short_side),
            "created_at_ms": p.created_at_ms,
            "maker_order_id": p.maker_order_id,
            "hedge_order_id": p.hedge_order_id,
            "maker_leg_filled": p.maker_leg_filled,
            "hedge_leg_filled": p.hedge_leg_filled,
            "uncertain_outcome": p.uncertain_outcome,
            "entry_type": p.entry_type,
            "maker_price": p.maker_price,
            "long_quantity": p.long_quantity,
            "short_quantity": p.short_quantity,
        }
        for pid, p in state.pending_entries.items()
    }

    # Add pending close details
    view["pending_closes"] = {
        cid: {
            "close_id": c.close_id,
            "position_id": c.position_id,
            "reason": c.reason,
            "created_at_ms": c.created_at_ms,
            "long_order_id": c.long_order_id,
            "short_order_id": c.short_order_id,
            "long_uncertain": c.long_uncertain,
            "short_uncertain": c.short_uncertain,
        }
        for cid, c in state.pending_closes.items()
    }

    return view


# ---------------------------------------------------------------------------
# Startup position recovery (Rust V1: finalize_startup_position_recovery)
# ---------------------------------------------------------------------------

def classify_startup_recovery_state(state: EngineState) -> str:
    """Classify the startup recovery state.

    Rust V1: finalize_startup_position_recovery() returns one of:
    - "clean" — no recovery work, safe to run
    - "recovery_needed" — has open positions or pending work
    - "fail_closed" — recovery failed or max positions exceeded

    Returns:
        One of "clean", "recovery_needed", "fail_closed"
    """
    snap = build_recovery_snapshot(state)

    if not snap.has_open_positions and not snap.has_pending_entries and not snap.has_pending_closes:
        return "clean"

    if snap.ambiguous_state:
        return "recovery_needed"

    if snap.has_open_positions:
        return "recovery_needed"

    if snap.has_pending_entries or snap.has_pending_closes:
        return "recovery_needed"

    return "clean"


def needs_reconciliation(state: EngineState) -> bool:
    """True if the engine state requires live-venue reconciliation before running.

    Rust V1: state_has_recovery_work() — checks all pending/recovery vectors.
    """
    snap = build_recovery_snapshot(state)
    return snap.has_open_positions or snap.has_pending_entries or snap.has_pending_closes


def is_safe_to_resume(state: EngineState) -> bool:
    """Check if engine state is safe to resume normal operation.

    Rust V1: state_is_safe_to_resume() — no lifecycle-blocking recovery work.
    """
    if state.lifecycle == EngineLifecycle.FAIL_CLOSED:
        return False
    if state.risk_mode == GlobalRiskMode.FAIL_CLOSED:
        # Operator override can keep fail_closed
        if state.operator.requested_mode == GlobalRiskMode.FAIL_CLOSED:
            return False
    return not needs_reconciliation(state)


def has_lifecycle_blocking_work(state: EngineState) -> bool:
    """True if there is work that blocks transition to RUNNING.

    Rust V1: EngineRecoveryWorkSnapshot.has_lifecycle_blocking_work()
    """
    snap = build_recovery_snapshot(state)
    return snap.has_open_positions or snap.has_pending_entries


def normalize_engine_state(state: EngineState) -> None:
    """Normalize engine state after recovery load.

    Rust V1: normalize_engine_state_positions() — sorts and deduplicates
    positions, migrates dust residuals, fixes timestamp defaults.
    """
    # Move dust positions out of open_positions
    dust_ids = set()
    for pid, pos in list(state.open_positions.items()):
        if pos.matched_quantity < 1e-12:
            dust_ids.add(pid)
    for pid in dust_ids:
        del state.open_positions[pid]

    # Ensure matched_quantity is set
    for pos in state.open_positions.values():
        if pos.matched_quantity == 0.0:
            pos.matched_quantity = min(pos.long_quantity, pos.short_quantity)


# ---------------------------------------------------------------------------
# Recovery dedup: prevent duplicate orders after restart (V1 clientOrderId)
# ---------------------------------------------------------------------------


def build_recovery_dedup_index(state: EngineState) -> dict[str, str]:
    """Build a dedup index from recovered pending entries and closes.

    Returns dict mapping client_order_id → pending_id or close_id.
    Used to prevent re-submitting orders that were already sent before restart.

    V1: before dispatching any entry or close, check if the same
    clientOrderId already exists in recovered pending state.
    """
    index: dict[str, str] = {}

    for pend_id, pe in state.pending_entries.items():
        if pe.maker_client_order_id:
            index[pe.maker_client_order_id] = pend_id
        if pe.hedge_client_order_id:
            index[pe.hedge_client_order_id] = pend_id

    for close_id, pc in state.pending_closes.items():
        if pc.long_client_order_id:
            index[pc.long_client_order_id] = close_id
        if pc.short_client_order_id:
            index[pc.short_client_order_id] = close_id

    return index


def is_client_order_id_duplicate(
    client_order_id: str,
    dedup_index: dict[str, str],
) -> bool:
    """Check if a clientOrderId already exists in recovered pending state.

    V1: prevents duplicate order submission after restart by checking
    the dedup index built from pending entries and closes.

    Returns True if the clientOrderId would create a duplicate.
    """
    return bool(client_order_id and client_order_id in dedup_index)


def has_pending_entry_for_symbol(
    state: EngineState,
    symbol: str,
    long_venue: str,
    short_venue: str,
) -> bool:
    """Check if there's already a pending entry for the same symbol and venues.

    V1: prevents opening duplicate positions on the same pair while
    a pending entry already exists.
    """
    for pe in state.pending_entries.values():
        if pe.symbol != symbol:
            continue
        pe_long = pe.long_venue.value if hasattr(pe.long_venue, 'value') else str(pe.long_venue)
        pe_short = pe.short_venue.value if hasattr(pe.short_venue, 'value') else str(pe.short_venue)
        if pe_long == long_venue and pe_short == short_venue:
            return True
    return False
