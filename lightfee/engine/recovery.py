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
    ActiveMakerLeg,
    CloseLegRecord,
    EngineState,
    HedgeInflight,
    OpenPosition,
    OperatorControlState,
    PassiveExecutionPhase,
    PassivePhaseState,
    PendingClose,
    PendingEntry,
    PendingPassiveClose,
    PendingPassiveLegFill,
    RecoveryWorkSnapshot,
)
from lightfee.core.domain import Side, Venue
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
    has_passive_closes = len(state.pending_passive_closes) > 0

    ambiguous = has_opens and state.lifecycle == EngineLifecycle.BOOTING

    return RecoveryWorkSnapshot(
        has_open_positions=has_opens,
        has_pending_entries=has_pending,
        has_pending_closes=has_closes,
        has_pending_passive_closes=has_passive_closes,
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
        review_id=data.get("review_id"),
        opportunity_origin_tags=list(data.get("opportunity_origin_tags", [])),
        opportunity_hint_source=data.get("opportunity_hint_source"),
        matched_quantity=float(data.get("matched_quantity", 0)),
        captured_funding_quote=float(data.get("captured_funding_quote", 0)),
        funding_captured=bool(data.get("funding_captured", False)),
        peak_net_quote=float(data.get("peak_net_quote", 0)),
        current_net_quote=float(data.get("current_net_quote", 0)),
        realized_price_pnl_quote=float(data.get("realized_price_pnl_quote", 0)),
        realized_exit_fee_quote=float(data.get("realized_exit_fee_quote", 0)),
        risk_delever_realized_price_pnl_quote=float(data.get("risk_delever_realized_price_pnl_quote", 0)),
        risk_delever_realized_exit_fee_quote=float(data.get("risk_delever_realized_exit_fee_quote", 0)),
        protection_realized_price_pnl_quote=float(data.get("protection_realized_price_pnl_quote", 0)),
        protection_realized_exit_fee_quote=float(data.get("protection_realized_exit_fee_quote", 0)),
        long_entry_fee_quote=float(data.get("long_entry_fee_quote", 0)),
        short_entry_fee_quote=float(data.get("short_entry_fee_quote", 0)),
        funding_edge_bps_entry=float(data.get("funding_edge_bps_entry", 0)),
        total_funding_edge_bps_entry=float(data.get("total_funding_edge_bps_entry", 0)),
        expected_edge_bps_entry=float(data.get("expected_edge_bps_entry", 0)),
        transfer_state_at_entry=data.get("transfer_state_at_entry"),
        entry_liquidity_source_at_entry=data.get("entry_liquidity_source_at_entry"),
        long_entry_vwap=data.get("long_entry_vwap"),
        short_entry_vwap=data.get("short_entry_vwap"),
        entry_capacity_constrained=bool(data.get("entry_capacity_constrained", False)),
        advisories=list(data.get("advisories", [])),
        blocked_reasons=list(data.get("blocked_reasons", [])),
        entry_quality_markout_5s_emitted=bool(data.get("entry_quality_markout_5s_emitted", False)),
        entry_quality_markout_30s_emitted=bool(data.get("entry_quality_markout_30s_emitted", False)),
        settlement_half_closed_quantity=float(data.get("settlement_half_closed_quantity", 0)),
        settlement_half_closed_at_ms=int(data.get("settlement_half_closed_at_ms", 0)),
        exit_reason=data.get("exit_reason"),
        # H-R5: 13 previously-missing fields
        last_risk_action_at_ms=int(data.get("last_risk_action_at_ms", 0)),
        risk_delever_step_count=int(data.get("risk_delever_step_count", 0)),
        last_risk_reason=data.get("last_risk_reason"),
        single_side_protection_triggered=bool(data.get("single_side_protection_triggered", False)),
        funding_timestamp_ms=int(data.get("funding_timestamp_ms", 0)),
        exit_after_first_stage=bool(data.get("exit_after_first_stage", False)),
        opportunity_type=data.get("opportunity_type", "aligned"),
        second_stage_enabled_at_entry=bool(data.get("second_stage_enabled_at_entry", False)),
        second_funding_timestamp_ms=int(data.get("second_funding_timestamp_ms", 0)),
        second_stage_funding_captured=bool(data.get("second_stage_funding_captured", False)),
        second_stage_funding_quote=float(data.get("second_stage_funding_quote", 0)),
        long_fill=_deserialize_order_fill(data.get("long_fill")),
        short_fill=_deserialize_order_fill(data.get("short_fill")),
    )


def _deserialize_order_fill(data: dict[str, Any] | None) -> "OrderFill | None":
    """Deserialize an OrderFill from dict."""
    if data is None or not isinstance(data, dict):
        return None
    from lightfee.core.domain import OrderFill as OF, Side as OFSide, Venue as OFVenue
    venue_str = data.get("venue", "binance")
    side_str = data.get("side", "buy")
    try:
        venue = OFVenue.from_str(venue_str) if hasattr(OFVenue, 'from_str') else OFVenue.BINANCE
    except (ValueError, AttributeError):
        venue = OFVenue.BINANCE
    try:
        side = OFSide.from_str(side_str) if hasattr(OFSide, 'from_str') else OFSide.BUY
    except (ValueError, AttributeError):
        side = OFSide.BUY
    return OF(
        venue=venue,
        symbol=str(data.get("symbol", "")),
        side=side,
        quantity=float(data.get("quantity", 0)),
        price=float(data.get("price", 0)),
        order_id=str(data.get("order_id", "")),
        client_order_id=data.get("client_order_id"),
        fee_quote=float(data.get("fee_quote", 0)) if data.get("fee_quote") is not None else None,
        filled_at_ms=int(data.get("filled_at_ms", 0)),
    )


def _restore_close_leg_records(data: list[dict[str, Any]]) -> list[CloseLegRecord]:
    """Restore CloseLegRecord list from serialized snapshot data."""
    records: list[CloseLegRecord] = []
    for item in (data or []):
        if isinstance(item, dict):
            records.append(CloseLegRecord(
                venue=str(item.get("venue", "")),
                order_id=str(item.get("order_id", "")),
                client_order_id=str(item.get("client_order_id", "")),
                quantity=float(item.get("quantity", 0)),
                average_price=float(item.get("average_price", 0)),
                fee_quote=float(item.get("fee_quote", 0)),
            ))
    return records


def _serialize_open_position(pos: OpenPosition) -> dict[str, Any]:
    """Serialize an OpenPosition for snapshot storage (all 53 fields)."""
    return {
        "position_id": pos.position_id,
        "symbol": pos.symbol,
        "review_id": pos.review_id,
        "opportunity_origin_tags": pos.opportunity_origin_tags,
        "opportunity_hint_source": pos.opportunity_hint_source,
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
        "risk_delever_realized_price_pnl_quote": pos.risk_delever_realized_price_pnl_quote,
        "risk_delever_realized_exit_fee_quote": pos.risk_delever_realized_exit_fee_quote,
        "protection_realized_price_pnl_quote": pos.protection_realized_price_pnl_quote,
        "protection_realized_exit_fee_quote": pos.protection_realized_exit_fee_quote,
        "long_entry_fee_quote": pos.long_entry_fee_quote,
        "short_entry_fee_quote": pos.short_entry_fee_quote,
        "funding_edge_bps_entry": pos.funding_edge_bps_entry,
        "total_funding_edge_bps_entry": pos.total_funding_edge_bps_entry,
        "expected_edge_bps_entry": pos.expected_edge_bps_entry,
        "transfer_state_at_entry": pos.transfer_state_at_entry,
        "entry_liquidity_source_at_entry": pos.entry_liquidity_source_at_entry,
        "long_entry_vwap": pos.long_entry_vwap,
        "short_entry_vwap": pos.short_entry_vwap,
        "entry_capacity_constrained": pos.entry_capacity_constrained,
        "advisories": pos.advisories,
        "blocked_reasons": pos.blocked_reasons,
        "entry_quality_markout_5s_emitted": pos.entry_quality_markout_5s_emitted,
        "entry_quality_markout_30s_emitted": pos.entry_quality_markout_30s_emitted,
        "settlement_half_closed_quantity": pos.settlement_half_closed_quantity,
        "settlement_half_closed_at_ms": pos.settlement_half_closed_at_ms,
        "exit_reason": pos.exit_reason,
        # H-R5: 13 previously-missing fields
        "last_risk_action_at_ms": pos.last_risk_action_at_ms,
        "risk_delever_step_count": pos.risk_delever_step_count,
        "last_risk_reason": pos.last_risk_reason,
        "single_side_protection_triggered": pos.single_side_protection_triggered,
        "funding_timestamp_ms": pos.funding_timestamp_ms,
        "exit_after_first_stage": pos.exit_after_first_stage,
        "opportunity_type": pos.opportunity_type,
        "second_stage_enabled_at_entry": pos.second_stage_enabled_at_entry,
        "second_funding_timestamp_ms": pos.second_funding_timestamp_ms,
        "second_stage_funding_captured": pos.second_stage_funding_captured,
        "second_stage_funding_quote": pos.second_stage_funding_quote,
        "long_fill": _serialize_order_fill(pos.long_fill) if pos.long_fill else None,
        "short_fill": _serialize_order_fill(pos.short_fill) if pos.short_fill else None,
    }


def _serialize_order_fill(fill) -> dict[str, Any] | None:
    """Serialize an OrderFill to a dict."""
    if fill is None:
        return None
    return {
        "venue": fill.venue.value if hasattr(fill.venue, 'value') else str(fill.venue),
        "symbol": fill.symbol,
        "side": fill.side.value if hasattr(fill.side, 'value') else str(fill.side),
        "quantity": fill.quantity,
        "price": fill.price,
        "order_id": fill.order_id,
        "client_order_id": fill.client_order_id,
        "fee_quote": fill.fee_quote,
        "filled_at_ms": fill.filled_at_ms,
    }


def _restore_hedge_inflight(raw) -> HedgeInflight | None:
    """Restore HedgeInflight from old string or new dict format.

    Backward compat: old states stored hedge_inflight as a plain string (CID).
    New format stores a dict with V1 PendingInflightHedge fields.
    Empty string or None → None.
    Non-empty string (legacy) → HedgeInflight with submitted_at_ms=0 (skip deadline).
    """
    if raw is None:
        return None
    if isinstance(raw, dict):
        return HedgeInflight.from_dict(raw)
    if isinstance(raw, str) and raw:
        return HedgeInflight(
            client_order_id=raw,
            venue=Venue.BYBIT,  # unknown for legacy, derived at call site
            side=Side.BUY,
            quantity=0.0,
            attempt=0,
            submitted_at_ms=0,  # legacy: no timestamp, skip deadline
        )
    return None


def _restore_state_from_snapshot_dict(snap: dict[str, Any]) -> EngineState:
    """Restore EngineState fields from a snapshot dict (without journal replay)."""
    state = EngineState()

    lifecycle_str = snap.get("lifecycle", "booting")
    # V1: migrate stale FAIL_CLOSED lifecycle → RISK_ONLY
    # EngineLifecycle has no FAIL_CLOSED variant; FailClosed =
    # RISK_ONLY + FAIL_CLOSED GlobalRiskMode
    if lifecycle_str == "fail_closed":
        state.lifecycle = EngineLifecycle.RISK_ONLY
    else:
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
    state.global_risk_reason = snap.get("global_risk_reason")
    state.recovery_blocked_reason = snap.get("recovery_blocked_reason")
    state.recovery_blocked_at_ms = int(snap.get("recovery_blocked_at_ms", 0))
    state.pending_residual_repairs = snap.get("pending_residual_repairs", [])
    state.live_recovery_reduce_only_pairs = snap.get("live_recovery_reduce_only_pairs", [])
    state.venue_entry_cooldowns = snap.get("venue_entry_cooldowns", {})
    state.venue_market_data_degradations = snap.get("venue_market_data_degradations", {})
    state.transfer_truth = snap.get("transfer_truth", {})
    state.entry_liquidity_qualification_records = snap.get("entry_liquidity_qualification_records", [])
    state.pending_close_reconciliations = snap.get("pending_close_reconciliations", [])
    state.passive_order_manager_states = snap.get("passive_order_manager_states", {})

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
                    metadata=pdata.get("metadata", {}),
                    maker_order_id=str(pdata.get("maker_order_id", "")),
                    hedge_order_id=str(pdata.get("hedge_order_id", "")),
                    maker_client_order_id=str(pdata.get("maker_client_order_id", "")),
                    hedge_client_order_id=str(pdata.get("hedge_client_order_id", "")),
                    maker_leg_filled=float(pdata.get("maker_leg_filled", 0)),
                    hedge_leg_filled=float(pdata.get("hedge_leg_filled", 0)),
                    deadline_ms=int(pdata.get("deadline_ms", 0)),
                    fallback_route=str(pdata.get("fallback_route", "")),
                    uncertain_outcome=bool(pdata.get("uncertain_outcome", False)),
                    reconcile_attempt=int(pdata.get("reconcile_attempt", 0)),
                    reconcile_next_attempt_ms=int(pdata.get("reconcile_next_attempt_ms", 0)),
                    entry_type=str(pdata.get("entry_type", "")),
                    maker_price=float(pdata.get("maker_price", 0)),
                    maker_fill_price=float(pdata.get("maker_fill_price", 0)),
                    hedge_fill_price=float(pdata.get("hedge_fill_price", 0)),
                    hedge_inflight=_restore_hedge_inflight(pdata.get("hedge_inflight", "")),
                    repair_state=str(pdata.get("repair_state", "")),
                    long_quantity=float(pdata.get("long_quantity", 0)),
                    short_quantity=float(pdata.get("short_quantity", 0)),
                    run_id=str(pdata.get("run_id", "")),
                    entry_route=str(pdata.get("entry_route", "")),
                    outcome=str(pdata.get("outcome", "")),
                    repost_count=int(pdata.get("repost_count", 0)),
                    zero_fill_since_ms=int(pdata.get("zero_fill_since_ms", 0)),
                    maker_leg=str(pdata.get("maker_leg", "long")),
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
                    long_client_order_id=str(cdata.get("long_client_order_id", "")),
                    short_client_order_id=str(cdata.get("short_client_order_id", "")),
                    long_target_close_qty=float(cdata.get("long_target_close_qty", 0)),
                    short_target_close_qty=float(cdata.get("short_target_close_qty", 0)),
                    long_closed=float(cdata.get("long_closed", 0)),
                    short_closed=float(cdata.get("short_closed", 0)),
                    long_uncertain=bool(cdata.get("long_uncertain", False)),
                    short_uncertain=bool(cdata.get("short_uncertain", False)),
                    deadline_ms=int(cdata.get("deadline_ms", 0)),
                    reconcile_attempt=int(cdata.get("reconcile_attempt", 0)),
                    reconcile_next_attempt_ms=int(cdata.get("reconcile_next_attempt_ms", 0)),
                    run_id=str(cdata.get("run_id", "")),
                    chunk_index=int(cdata.get("chunk_index", 0)),
                    total_chunks=int(cdata.get("total_chunks", 1)),
                    long_legs=_restore_close_leg_records(cdata.get("long_legs", [])),
                    short_legs=_restore_close_leg_records(cdata.get("short_legs", [])),
                )

    # Restore pending passive closes
    ppc_dict = snap.get("pending_passive_closes", {})
    if isinstance(ppc_dict, dict):
        for pid, pdata in ppc_dict.items():
            if isinstance(pdata, dict):
                ps_data = pdata.get("phase_state", {})
                phase_state = PassivePhaseState(
                    phase=PassiveExecutionPhase(ps_data.get("phase", "high_slippage_maker")),
                    preferred_maker_leg=ActiveMakerLeg(ps_data.get("preferred_maker_leg", "long")),
                    active_maker_leg=ActiveMakerLeg(ps_data.get("active_maker_leg", "long")),
                    phase_started_at_ms=int(ps_data.get("phase_started_at_ms", 0)),
                    cycle_attempt=int(ps_data.get("cycle_attempt", 1)),
                    cycle_started_at_ms=int(ps_data.get("cycle_started_at_ms", 0)),
                    zero_fill_cycles_in_phase=int(ps_data.get("zero_fill_cycles_in_phase", 0)),
                    maker_order_id=str(ps_data.get("maker_order_id", "")),
                    maker_client_order_id=str(ps_data.get("maker_client_order_id", "")),
                    maker_resting_limit_price=ps_data.get("maker_resting_limit_price"),
                    maker_resting_since_ms=int(ps_data.get("maker_resting_since_ms", 0)),
                )
                mf = pdata.get("maker_fill", {})
                maker_fill = PendingPassiveLegFill(
                    quantity=float(mf.get("quantity", 0)),
                    average_price=float(mf.get("average_price", 0)),
                    fee_quote=float(mf.get("fee_quote", 0)),
                    last_fill_time_ms=int(mf.get("last_fill_time_ms", 0)),
                    order_id=str(mf.get("order_id", "")),
                    client_order_id=str(mf.get("client_order_id", "")),
                )
                hf = pdata.get("hedge_fill", {})
                hedge_fill = PendingPassiveLegFill(
                    quantity=float(hf.get("quantity", 0)),
                    average_price=float(hf.get("average_price", 0)),
                    fee_quote=float(hf.get("fee_quote", 0)),
                    last_fill_time_ms=int(hf.get("last_fill_time_ms", 0)),
                    order_id=str(hf.get("order_id", "")),
                    client_order_id=str(hf.get("client_order_id", "")),
                )
                state.pending_passive_closes[pid] = PendingPassiveClose(
                    position_id=pdata.get("position_id", pid),
                    reason=pdata.get("reason", ""),
                    short_stage=pdata.get("short_stage", ""),
                    long_stage=pdata.get("long_stage", ""),
                    target_quantity=float(pdata.get("target_quantity", 0)),
                    max_slippage_bps=pdata.get("max_slippage_bps"),
                    chunk_quantities=[float(x) for x in pdata.get("chunk_quantities", [])],
                    active_chunk_index=int(pdata.get("active_chunk_index", 0)),
                    phase_state=phase_state,
                    maker_fill=maker_fill,
                    hedge_fill=hedge_fill,
                    next_retry_at_ms=int(pdata.get("next_retry_at_ms", 0)),
                    multi_phase_started_at_ms=int(pdata.get("multi_phase_started_at_ms", 0)),
                    created_cycle=int(pdata.get("created_cycle", 0)),
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

    V1: replay_journal_records (journal.py:289-397).
    Processes events that happened AFTER the snapshot was written:
    - entry.opened / recovery.live_detected → add position
    - entry.pending_registered / entry.hedge_submitted → recreate pending entry
    - exit.closed / recovery.flat → remove position
    - exit.partial_closed → reduce matched_quantity
    - exit.pending_close_registered → recreate pending close
    - runtime.lifecycle_changed / risk_mode_changed → update modes
    """
    for record in records:
        kind = record.get("kind", "")
        payload = record.get("payload", {})

        if kind in ("entry.opened", "recovery.live_detected"):
            pid = payload.get("position_id", "")
            if pid and pid not in state.open_positions:
                state.open_positions[pid] = _deserialize_open_position(payload)

        # V1: entry.pending_registered — recreate pending entry from journal
        elif kind == "entry.pending_registered":
            pid = payload.get("position_id", "")
            if pid and pid not in state.pending_entries:
                pe = _restore_pending_entry_from_journal(payload)
                if pe is not None:
                    state.pending_entries[pid] = pe

        elif kind in ("exit.closed", "exit.reconciled", "recovery.flat"):
            pid = payload.get("position_id", "")
            if pid:
                state.open_positions.pop(pid, None)
                state.pending_closes.pop(pid, None)

        # V1: exit.pending_close_registered — recreate pending close from journal
        elif kind == "exit.pending_close_registered":
            close_id = payload.get("close_id", "")
            pid = payload.get("position_id", "")
            if close_id and close_id not in state.pending_closes:
                from lightfee.engine.state import PendingClose
                state.pending_closes[close_id] = PendingClose(
                    close_id=close_id,
                    position_id=pid,
                    reason=payload.get("reason", "recovery_replay"),
                    created_at_ms=int(payload.get("created_at_ms", 0)),
                    long_order_id=payload.get("long_order_id", ""),
                    short_order_id=payload.get("short_order_id", ""),
                    long_client_order_id=payload.get("long_client_order_id", ""),
                    short_client_order_id=payload.get("short_client_order_id", ""),
                    long_closed=float(payload.get("long_closed", 0)),
                    short_closed=float(payload.get("short_closed", 0)),
                    long_uncertain=bool(payload.get("long_uncertain", False)),
                    short_uncertain=bool(payload.get("short_uncertain", True)),
                    chunk_index=int(payload.get("chunk_index", 0)),
                    total_chunks=int(payload.get("chunk_count", payload.get("total_chunks", 1))),
                )

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

        elif kind == "ops.command_applied":
            new_risk = payload.get("new_risk")
            new_lifecycle = payload.get("new_lifecycle")
            if new_risk:
                try:
                    state.risk_mode = GlobalRiskMode(str(new_risk))
                except ValueError:
                    pass
            if new_lifecycle:
                try:
                    state.lifecycle = EngineLifecycle(str(new_lifecycle))
                except ValueError:
                    pass


def _restore_pending_entry_from_journal(payload: dict[str, Any]) -> Any | None:
    """V1: restore PendingEntry from journal replay payload."""
    try:
        from lightfee.engine.state import PendingEntry, HedgeInflight
        maker_venue_str = payload.get("maker_venue", payload.get("long_venue", "binance"))
        hedge_venue_str = payload.get("hedge_venue", payload.get("short_venue", "okx"))
        try:
            maker_venue = Venue.from_str(maker_venue_str) if hasattr(Venue, 'from_str') else Venue.BINANCE
        except (ValueError, AttributeError):
            maker_venue = Venue.BINANCE
        try:
            hedge_venue = Venue.from_str(hedge_venue_str) if hasattr(Venue, 'from_str') else Venue.OKX
        except (ValueError, AttributeError):
            hedge_venue = Venue.OKX

        hedge_inflight = None
        hi_data = payload.get("hedge_inflight")
        if isinstance(hi_data, dict):
            hedge_inflight = HedgeInflight(
                client_order_id=hi_data.get("client_order_id", ""),
                venue=hedge_venue,
                side=Side.BUY if hi_data.get("side", "buy") == "buy" else Side.SELL,
                quantity=float(hi_data.get("quantity", 0)),
                attempt=int(hi_data.get("attempt", 0)),
                submitted_at_ms=int(hi_data.get("submitted_at_ms", 0)),
            )

        return PendingEntry(
            entry_id=payload.get("position_id", payload.get("entry_id", "")),
            symbol=payload.get("symbol", ""),
            maker_venue=maker_venue,
            hedge_venue=hedge_venue,
            long_venue=maker_venue if maker_venue_str != hedge_venue_str else Venue.BINANCE,
            short_venue=hedge_venue,
            long_quantity=float(payload.get("target_quantity", payload.get("long_quantity", 0))),
            short_quantity=float(payload.get("target_quantity", payload.get("short_quantity", 0))),
            maker_price=float(payload.get("maker_price", 0)),
            maker_order_id=payload.get("maker_order_id", ""),
            maker_client_order_id=payload.get("maker_client_order_id", ""),
            hedge_order_id=payload.get("hedge_order_id", ""),
            hedge_client_order_id=payload.get("hedge_client_order_id", ""),
            hedge_inflight=hedge_inflight,
            maker_leg_filled=float(payload.get("maker_leg_filled", 0)),
            hedge_leg_filled=float(payload.get("hedge_leg_filled", 0)),
            uncertain_outcome=bool(payload.get("uncertain_outcome", False)),
        )
    except Exception:
        return None


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
    has_snapshot = snap is not None
    state = _restore_state_from_snapshot_dict(snap) if has_snapshot else EngineState()

    # Emit recovery.live_detected for each position restored from snapshot
    # (V1: recovery.live_detected is recorded when live positions are detected at startup)
    snapshot_position_ids = set(state.open_positions.keys())

    # Replay journal records to catch events after last snapshot
    journal_records = journal.read_all()
    if journal_records:
        _apply_journal_replay_to_state(state, journal_records)

    for pid in snapshot_position_ids:
        if pid in state.open_positions:
            pos = state.open_positions[pid]
            _try_emit_recovery(journal, "recovery.live_detected", {
                "position_id": pid,
                "symbol": pos.symbol,
                "long_venue": pos.long_venue.value if hasattr(pos.long_venue, 'value') else str(pos.long_venue),
                "short_venue": pos.short_venue.value if hasattr(pos.short_venue, 'value') else str(pos.short_venue),
                "quantity": pos.matched_quantity,
                "long_quantity": pos.long_quantity,
                "short_quantity": pos.short_quantity,
            })

    # Remove positions that became flat after journal replay (were in snapshot but closed in journal)
    for pid in snapshot_position_ids:
        if pid not in state.open_positions:
            _try_emit_recovery(journal, "recovery.flat", {
                "position_id": pid,
                "reason": "closed_in_journal_since_snapshot",
            })

    # Assess recovery needs
    recovery = build_recovery_snapshot(state)

    if recovery.has_open_positions:
        if recovery.ambiguous_state:
            state.lifecycle = EngineLifecycle.RECONCILING
            state.risk_mode = state.risk_mode.max(GlobalRiskMode.REDUCE_ONLY)
            _try_emit_recovery(journal, "recovery.blocked", {
                "reason": "ambiguous_live_truth",
                "open_position_count": len(state.open_positions),
            })
        else:
            state.lifecycle = EngineLifecycle.RECONCILING

    if state.lifecycle == EngineLifecycle.BOOTING:
        state.lifecycle = EngineLifecycle.RECONCILING

    # Clean state with no recovery work → can run
    if not recovery.has_open_positions and not recovery.has_pending_entries and not recovery.has_pending_closes:
        if state.lifecycle == EngineLifecycle.RECONCILING:
            state.lifecycle = EngineLifecycle.RUNNING
            _try_emit_recovery(journal, "runtime.running", {
                "reason": "startup_no_recovery_work",
            })

    # V1: normalize_engine_state_positions — applied after every recovery load
    normalize_engine_state(state)

    return state


def is_ambiguous_live_truth(state: EngineState) -> bool:
    """Check if the live position truth is ambiguous (no private confirmation).

    V1: BOOTING with open positions is ambiguous because we haven't confirmed
    positions against venue truth yet. RECONCILING with open positions is also
    ambiguous until reconciliation completes.
    """
    if len(state.open_positions) == 0:
        return False
    return state.lifecycle in (EngineLifecycle.BOOTING, EngineLifecycle.RECONCILING)


# ---------------------------------------------------------------------------
# State snapshot serialization (Rust V1: persistent_state_view)
# ---------------------------------------------------------------------------

def build_persistent_state_view(state: EngineState) -> dict[str, Any]:
    """Build a snapshot-suitable dict of engine state.

    Rust V1: persistent_state_view() strips volatile scan fields and
    normalizes positions before serialization.
    """
    view = state.to_dict()

    # Add new EngineState V1 fields to persistent view
    view["global_risk_reason"] = state.global_risk_reason
    view["recovery_blocked_reason"] = state.recovery_blocked_reason
    view["recovery_blocked_at_ms"] = state.recovery_blocked_at_ms
    view["pending_residual_repairs"] = state.pending_residual_repairs
    view["live_recovery_reduce_only_pairs"] = state.live_recovery_reduce_only_pairs
    view["venue_entry_cooldowns"] = state.venue_entry_cooldowns
    view["venue_market_data_degradations"] = state.venue_market_data_degradations
    view["transfer_truth"] = state.transfer_truth
    view["entry_liquidity_qualification_records"] = state.entry_liquidity_qualification_records
    view["pending_close_reconciliations"] = state.pending_close_reconciliations

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
            "metadata": p.metadata,
            "maker_order_id": p.maker_order_id,
            "hedge_order_id": p.hedge_order_id,
            "maker_client_order_id": p.maker_client_order_id,
            "hedge_client_order_id": p.hedge_client_order_id,
            "maker_leg_filled": p.maker_leg_filled,
            "hedge_leg_filled": p.hedge_leg_filled,
            "deadline_ms": p.deadline_ms,
            "fallback_route": p.fallback_route,
            "uncertain_outcome": p.uncertain_outcome,
            "reconcile_attempt": p.reconcile_attempt,
            "reconcile_next_attempt_ms": p.reconcile_next_attempt_ms,
            "entry_type": p.entry_type,
            "maker_price": p.maker_price,
            "maker_fill_price": p.maker_fill_price,
            "hedge_fill_price": p.hedge_fill_price,
            "hedge_inflight": p.hedge_inflight.to_dict() if p.hedge_inflight else "",
            "repair_state": p.repair_state,
            "maker_leg": p.maker_leg,
            "long_quantity": p.long_quantity,
            "short_quantity": p.short_quantity,
            "run_id": p.run_id,
            "entry_route": p.entry_route,
            "outcome": p.outcome,
            "repost_count": p.repost_count,
            "zero_fill_since_ms": p.zero_fill_since_ms,
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
            "long_client_order_id": c.long_client_order_id,
            "short_client_order_id": c.short_client_order_id,
            "long_target_close_qty": c.long_target_close_qty,
            "short_target_close_qty": c.short_target_close_qty,
            "long_closed": c.long_closed,
            "short_closed": c.short_closed,
            "long_uncertain": c.long_uncertain,
            "short_uncertain": c.short_uncertain,
            "deadline_ms": c.deadline_ms,
            "reconcile_attempt": c.reconcile_attempt,
            "reconcile_next_attempt_ms": c.reconcile_next_attempt_ms,
            "run_id": c.run_id,
            "chunk_index": c.chunk_index,
            "total_chunks": c.total_chunks,
            "long_legs": [
                {
                    "venue": lr.venue,
                    "order_id": lr.order_id,
                    "client_order_id": lr.client_order_id,
                    "quantity": lr.quantity,
                    "average_price": lr.average_price,
                    "fee_quote": lr.fee_quote,
                }
                for lr in c.long_legs
            ],
            "short_legs": [
                {
                    "venue": lr.venue,
                    "order_id": lr.order_id,
                    "client_order_id": lr.client_order_id,
                    "quantity": lr.quantity,
                    "average_price": lr.average_price,
                    "fee_quote": lr.fee_quote,
                }
                for lr in c.short_legs
            ],
        }
        for cid, c in state.pending_closes.items()
    }

    # V1 parity: include pending passive closes
    view["pending_passive_closes"] = {
        pid: {
            "position_id": ppc.position_id,
            "reason": ppc.reason,
            "short_stage": ppc.short_stage,
            "long_stage": ppc.long_stage,
            "target_quantity": ppc.target_quantity,
            "max_slippage_bps": ppc.max_slippage_bps,
            "chunk_quantities": ppc.chunk_quantities,
            "active_chunk_index": ppc.active_chunk_index,
            "phase_state": {
                "phase": ppc.phase_state.phase.value,
                "preferred_maker_leg": ppc.phase_state.preferred_maker_leg.value,
                "active_maker_leg": ppc.phase_state.active_maker_leg.value,
                "phase_started_at_ms": ppc.phase_state.phase_started_at_ms,
                "cycle_attempt": ppc.phase_state.cycle_attempt,
                "cycle_started_at_ms": ppc.phase_state.cycle_started_at_ms,
                "zero_fill_cycles_in_phase": ppc.phase_state.zero_fill_cycles_in_phase,
                "maker_order_id": ppc.phase_state.maker_order_id,
                "maker_client_order_id": ppc.phase_state.maker_client_order_id,
                "maker_resting_limit_price": ppc.phase_state.maker_resting_limit_price,
                "maker_resting_since_ms": ppc.phase_state.maker_resting_since_ms,
            },
            "maker_fill": {
                "quantity": ppc.maker_fill.quantity,
                "average_price": ppc.maker_fill.average_price,
                "fee_quote": ppc.maker_fill.fee_quote,
                "last_fill_time_ms": ppc.maker_fill.last_fill_time_ms,
                "order_id": ppc.maker_fill.order_id,
                "client_order_id": ppc.maker_fill.client_order_id,
            },
            "hedge_fill": {
                "quantity": ppc.hedge_fill.quantity,
                "average_price": ppc.hedge_fill.average_price,
                "fee_quote": ppc.hedge_fill.fee_quote,
                "last_fill_time_ms": ppc.hedge_fill.last_fill_time_ms,
                "order_id": ppc.hedge_fill.order_id,
                "client_order_id": ppc.hedge_fill.client_order_id,
            },
            "next_retry_at_ms": ppc.next_retry_at_ms,
            "multi_phase_started_at_ms": ppc.multi_phase_started_at_ms,
            "created_cycle": ppc.created_cycle,
        }
        for pid, ppc in state.pending_passive_closes.items()
    }

    # V1 parity: include local-L2 state fields in snapshot
    view["retained_local_l2_books"] = [
        dict(b) if hasattr(b, '__iter__') and not isinstance(b, dict) else b
        for b in getattr(state, 'retained_local_l2_books', [])
    ]
    view["local_l2_books_snapshot"] = [
        dict(b) if hasattr(b, '__iter__') and not isinstance(b, dict) else b
        for b in getattr(state, 'local_l2_books_snapshot', [])
    ]
    view["local_l2_session_snapshot"] = [
        dict(s) if hasattr(s, '__iter__') and not isinstance(s, dict) else s
        for s in getattr(state, 'local_l2_session_snapshot', [])
    ]

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

    if not snap.has_open_positions and not snap.has_pending_entries and not snap.has_pending_closes and not snap.has_pending_passive_closes:
        return "clean"

    if snap.ambiguous_state:
        return "recovery_needed"

    if snap.has_open_positions:
        return "recovery_needed"

    if snap.has_pending_entries or snap.has_pending_closes or snap.has_pending_passive_closes:
        return "recovery_needed"

    return "clean"


def needs_reconciliation(state: EngineState) -> bool:
    """True if the engine state requires live-venue reconciliation before running.

    Rust V1: state_has_recovery_work() — checks all pending/recovery vectors.
    """
    snap = build_recovery_snapshot(state)
    return snap.has_open_positions or snap.has_pending_entries or snap.has_pending_closes or snap.has_pending_passive_closes


def is_safe_to_resume(state: EngineState) -> bool:
    """Check if engine state is safe to resume normal operation.

    Rust V1: state_is_safe_to_resume() — no lifecycle-blocking recovery work.
    """
    if state.lifecycle == EngineLifecycle.RISK_ONLY and state.risk_mode == GlobalRiskMode.FAIL_CLOSED:
        return False
    if state.risk_mode == GlobalRiskMode.FAIL_CLOSED:
        # Operator override can keep fail_closed
        if state.operator.requested_mode == GlobalRiskMode.FAIL_CLOSED:
            return False
    return not needs_reconciliation(state)


def clear_stale_fail_closed_if_recovery_clean(state: EngineState, journal: Journal | None = None) -> bool:
    """Clear persisted fail_closed only when there is no recovery or operator block.

    This is deliberately narrower than RESUME_IF_SAFE. It handles stale persisted
    state from prior incidents after open/pending work is already gone.
    """
    if state.risk_mode != GlobalRiskMode.FAIL_CLOSED:
        return False
    if state.operator.requested_mode == GlobalRiskMode.FAIL_CLOSED:
        return False
    if needs_reconciliation(state):
        return False
    if state.recovery_blocked_reason:
        return False

    previous = state.risk_mode.value
    state.risk_mode = GlobalRiskMode.RUNNING
    state.lifecycle = EngineLifecycle.RUNNING
    state.global_risk_reason = None
    state.recovery_blocked_at_ms = 0
    if journal is not None:
        _try_emit_recovery(journal, "runtime.risk_mode_changed", {
            "from": previous,
            "to": state.risk_mode.value,
            "reason": "startup_clean_stale_fail_closed_cleared",
        })
        _try_emit_recovery(journal, "runtime.stale_fail_closed_cleared", {
            "reason": "startup_clean_no_recovery_work",
        })
    return True


def _try_emit_recovery(journal: Journal, kind: str, payload: dict[str, Any]) -> None:
    """Emit a recovery diagnostic event through the journal.

    Rust V1: recovery events (recovery.blocked, recovery.flat,
    recovery.live_detected, runtime.running) are written to the journal
    during startup recovery. If the journal is not open, it is temporarily
    opened for append so that recovery diagnostics are never silently lost.
    """
    import time as _time
    ts_ms = int(_time.time() * 1000)
    if journal._file is not None:
        journal.append(kind, payload, ts_ms=ts_ms)
        return
    try:
        journal.open()
        journal.append(kind, payload, ts_ms=ts_ms)
    except Exception:
        pass
    finally:
        try:
            journal.close()
        except Exception:
            pass


def has_lifecycle_blocking_work(state: EngineState) -> bool:
    """True if there is work that blocks transition to RUNNING.

    Rust V1: EngineRecoveryWorkSnapshot.has_lifecycle_blocking_work()
    """
    snap = build_recovery_snapshot(state)
    return snap.has_open_positions or snap.has_pending_entries or snap.has_pending_passive_closes


def normalize_engine_state(state: EngineState) -> None:
    """Normalize engine state after recovery load.

    Rust V1: normalize_engine_state_positions() — sorts and deduplicates
    positions, migrates dust residuals, fixes timestamp defaults.
    Performs 12+ repair operations matching V1 semantics.
    """
    # 0. V1 parity: migrate stale FAIL_CLOSED lifecycle → RISK_ONLY
    #    V1 has no FAIL_CLOSED lifecycle variant; FailClosed = RISK_ONLY + FAIL_CLOSED risk
    if state.lifecycle.value == "fail_closed":  # type: ignore[attr-defined]
        from lightfee.risk.modes import EngineLifecycle
        state.lifecycle = EngineLifecycle.RISK_ONLY
        state.risk_mode = GlobalRiskMode.FAIL_CLOSED

    # 1. Timestamp repair: backfill zero funding_timestamp_ms
    for pos in state.open_positions.values():
        if pos.funding_timestamp_ms == 0:
            pos.funding_timestamp_ms = pos.opened_at_ms

    # 2. Edge repair: backfill zero total_funding_edge_bps_entry
    for pos in state.open_positions.values():
        if pos.total_funding_edge_bps_entry == 0.0:
            pos.total_funding_edge_bps_entry = pos.funding_edge_bps_entry

    # 3. Ensure matched_quantity is set
    for pos in state.open_positions.values():
        if pos.matched_quantity == 0.0:
            pos.matched_quantity = min(pos.long_quantity, pos.short_quantity)

    # 4. Dedup open_positions by position_id
    seen_ids: set[str] = set()
    deduped: dict[str, OpenPosition] = {}
    for pid, pos in state.open_positions.items():
        if pid not in seen_ids:
            seen_ids.add(pid)
            deduped[pid] = pos
    state.open_positions = deduped

    # 5. Dust migration: move positions with exchange_min_notional_dust exit_reason
    dust_ids: list[str] = []
    for pid, pos in list(state.open_positions.items()):
        if pos.exit_reason and "exchange_min_notional_dust" in pos.exit_reason:
            dust_ids.append(pid)
    for pid in dust_ids:
        pos = state.open_positions.pop(pid, None)
        if pos is not None:
            state.pending_residual_repairs.append({
                "position_id": pid,
                "symbol": pos.symbol,
                "long_venue": pos.long_venue.value if hasattr(pos.long_venue, 'value') else str(pos.long_venue),
                "short_venue": pos.short_venue.value if hasattr(pos.short_venue, 'value') else str(pos.short_venue),
                "reason": "exchange_min_notional_dust",
                "migrated_at_ms": pos.opened_at_ms,
            })

    # 6. Dedup live_recovery_reduce_only_pairs
    if state.live_recovery_reduce_only_pairs:
        seen_pairs: set[str] = set()
        deduped_pairs: list = []
        for pair in state.live_recovery_reduce_only_pairs:
            if isinstance(pair, dict):
                key = f"{pair.get('long_venue','')}:{pair.get('short_venue','')}:{pair.get('symbol','')}"
            else:
                key = str(pair)
            if key not in seen_pairs:
                seen_pairs.add(key)
                deduped_pairs.append(pair)
        state.live_recovery_reduce_only_pairs = deduped_pairs

    # 7. PassivePhaseState fill: ensure empty phase states have defaults
    from lightfee.engine.state import PassivePhaseState
    for ppc in state.pending_passive_closes.values():
        ps = ppc.phase_state
        if ps.phase_started_at_ms == 0:
            ps.phase_started_at_ms = ppc.created_cycle or 1
        if ps.cycle_started_at_ms == 0:
            ps.cycle_started_at_ms = ps.phase_started_at_ms
        if ps.cycle_attempt == 0:
            ps.cycle_attempt = 1

    # 8. Remove zero-quantity positions
    zero_ids = [pid for pid, pos in state.open_positions.items() if pos.matched_quantity < 1e-12]
    for pid in zero_ids:
        del state.open_positions[pid]

    # 9. Validate and clean bad pending entries (V1: type safety + normalize)
    # Drop entries with missing/broken data that would cause reconciliation failures.
    import logging
    _logger = logging.getLogger("lightfee.engine.recovery")
    bad_entry_ids: list[str] = []
    for entry_id, pe in list(state.pending_entries.items()):
        # Must have a valid symbol
        if not pe.symbol or not isinstance(pe.symbol, str) or not pe.symbol.strip():
            bad_entry_ids.append(entry_id)
            _logger.warning("recovery: dropping pending entry %s — empty symbol", entry_id)
            continue
        # Must have valid venues
        if not pe.long_venue or not pe.short_venue:
            bad_entry_ids.append(entry_id)
            _logger.warning("recovery: dropping pending entry %s — missing venue", entry_id)
            continue
        # Must have positive target quantity
        try:
            qty = float(pe.target_quantity)
            if qty <= 0:
                bad_entry_ids.append(entry_id)
                _logger.warning("recovery: dropping pending entry %s — zero quantity", entry_id)
                continue
        except (ValueError, TypeError):
            bad_entry_ids.append(entry_id)
            _logger.warning("recovery: dropping pending entry %s — unparseable quantity", entry_id)
            continue
        # Backfill missing timestamps
        if pe.created_at_ms <= 0:
            pe.created_at_ms = pe.reconcile_next_attempt_ms if pe.reconcile_next_attempt_ms > 0 else int(time.time() * 1000)
        # Ensure reconcile backoff defaults
        if pe.reconcile_next_attempt_ms <= 0:
            pe.reconcile_next_attempt_ms = pe.created_at_ms
    for eid in bad_entry_ids:
        state.pending_entries.pop(eid, None)
    if bad_entry_ids:
        _logger.warning("recovery: dropped %d bad pending entries: %s", len(bad_entry_ids), bad_entry_ids)

    # 10. Validate pending closes similarly
    bad_close_ids: list[str] = []
    for close_id, pc in list(state.pending_closes.items()):
        if not pc.position_id or not pc.symbol:
            bad_close_ids.append(close_id)
            continue
        if pc.created_at_ms <= 0:
            pc.created_at_ms = int(time.time() * 1000)
        if pc.reconcile_next_attempt_ms <= 0:
            pc.reconcile_next_attempt_ms = pc.created_at_ms
    for cid in bad_close_ids:
        state.pending_closes.pop(cid, None)

    # 11. Sort open positions by position_id (V1: deterministic ordering)
    state.open_positions = dict(sorted(state.open_positions.items()))

    # 12. Peak net quote repair: ensure peak >= current
    for pos in state.open_positions.values():
        if pos.peak_net_quote < pos.current_net_quote:
            pos.peak_net_quote = pos.current_net_quote

    # 13. Opportunity type default: ensure valid value
    for pos in state.open_positions.values():
        if not pos.opportunity_type:
            pos.opportunity_type = "aligned"


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

    for pid, ppc in state.pending_passive_closes.items():
        if ppc.phase_state.maker_client_order_id:
            index[ppc.phase_state.maker_client_order_id] = pid
        if ppc.maker_fill.client_order_id:
            index[ppc.maker_fill.client_order_id] = pid
        if ppc.hedge_fill.client_order_id:
            index[ppc.hedge_fill.client_order_id] = pid

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
