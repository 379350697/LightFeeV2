"""Engine state models and open position tracking matching Rust EngineState."""

from __future__ import annotations

import math

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from lightfee.core.domain import OrderFill, PassiveOrderState, Side, Venue
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


@dataclass
class OpenPosition:
    position_id: str
    symbol: str
    long_venue: Venue
    short_venue: Venue
    long_quantity: float
    short_quantity: float
    long_entry_price: float
    short_entry_price: float
    opened_at_ms: int
    # --- V1 entry_notional_quote: paired entry notional for funding capture ---
    entry_notional_quote: float = 0.0
    # --- Review & origin (V1 review_id, opportunity_origin_tags, opportunity_hint_source) ---
    review_id: str | None = None
    opportunity_origin_tags: list[str] = field(default_factory=list)
    opportunity_hint_source: str | None = None
    # --- Entry fees (matched Rust V1 total_entry_fee_quote per leg) ---
    long_entry_fee_quote: float = 0.0
    short_entry_fee_quote: float = 0.0
    total_entry_fee_quote: float = 0.0
    # True only when these fee values came from actual entry fill evidence.
    # Numeric zero alone is not evidence: legacy snapshots defaulted missing
    # fees to zero and must remain provisional during close reconciliation.
    entry_fee_evidence_complete: bool = False
    # --- PnL attribution (matches Rust V1 realized_* fields) ---
    realized_price_pnl_quote: float = 0.0
    realized_exit_fee_quote: float = 0.0
    # --- Risk/Protection PnL (V1 risk_delever_*, protection_*) ---
    risk_delever_realized_price_pnl_quote: float = 0.0
    risk_delever_realized_exit_fee_quote: float = 0.0
    protection_realized_price_pnl_quote: float = 0.0
    protection_realized_exit_fee_quote: float = 0.0
    # --- Funding accrual (Rust V1 captured_funding_quote, funding_captured) ---
    captured_funding_quote: float = 0.0
    funding_captured: bool = False
    # --- Edge breakdowns (V1 funding_edge_bps_entry, total_funding_edge_bps_entry, expected_edge_bps_entry) ---
    funding_edge_bps_entry: float = 0.0
    total_funding_edge_bps_entry: float = 0.0
    expected_edge_bps_entry: float = 0.0
    worst_case_edge_bps_entry: float = 0.0
    entry_cross_bps_entry: float = 0.0
    fee_bps_entry: float = 0.0
    entry_slippage_bps_entry: float = 0.0
    # --- Edge & net tracking (Rust V1 peak_net_quote, current_net_quote) ---
    peak_net_quote: float = 0.0
    current_net_quote: float = 0.0
    # --- Close deadlines (Rust V1 settlement_half_closed_*, last_risk_action_at_ms) ---
    settlement_half_closed_quantity: float = 0.0
    settlement_half_closed_at_ms: int = 0
    last_risk_action_at_ms: int = 0
    # --- Risk action tracking (Rust V1 risk_delever_step_count, last_risk_reason) ---
    risk_delever_step_count: int = 0
    last_risk_reason: str | None = None
    single_side_protection_triggered: bool = False
    # --- Matched quantity = min(long_qty, short_qty) (Rust V1 matched_quantity) ---
    matched_quantity: float = 0.0
    initial_quantity: float = 0.0
    # --- Funding timing for exit capture stages ---
    funding_timestamp_ms: int = 0
    long_funding_timestamp_ms: int = 0
    short_funding_timestamp_ms: int = 0
    exit_after_first_stage: bool = False
    # --- Funding stage tracking (Rust V1 second_stage_*, opportunity_type) ---
    opportunity_type: str = "aligned"
    first_funding_leg: str = ""
    second_stage_enabled_at_entry: bool = False
    second_funding_timestamp_ms: int = 0
    second_stage_funding_captured: bool = False
    second_stage_funding_quote: float = 0.0
    # --- Entry/exit maker leg selection (V1 entry_maker_leg, exit_maker_leg) ---
    entry_maker_leg: str = ""
    exit_maker_leg: str = ""
    # --- Transfer & liquidity (V1 transfer_state_at_entry, entry_liquidity_source_at_entry) ---
    transfer_bias_bps_entry: float = 0.0
    transfer_state_at_entry: str | None = None
    entry_liquidity_source_at_entry: str | None = None
    long_volume_24h_quote_at_entry: float = 0.0
    short_volume_24h_quote_at_entry: float = 0.0
    long_open_interest_quote_at_entry: float = 0.0
    short_open_interest_quote_at_entry: float = 0.0
    # --- VWAP (V1 long_entry_vwap, short_entry_vwap) ---
    long_entry_vwap: float | None = None
    short_entry_vwap: float | None = None
    # --- Capacity constraints (V1 entry_capacity_constrained and depth caps) ---
    entry_capacity_constrained: bool = False
    entry_target_quantity: float = 0.0
    long_max_executable_quantity: float = 0.0
    short_max_executable_quantity: float = 0.0
    entry_max_executable_quantity: float = 0.0
    entry_depth_shortfall_quantity: float = 0.0
    entry_max_executable_notional_quote: float = 0.0
    entry_depth_capped_at_entry: bool = False
    # --- Advisories & blocked reasons (V1 advisories, blocked_reasons) ---
    advisories: list[str] = field(default_factory=list)
    blocked_reasons: list[str] = field(default_factory=list)
    # --- Quality markouts (V1 entry_quality_markout_5s/30s_emitted) ---
    entry_quality_completed_at_ms: int = 0
    entry_quality_markout_5s_emitted: bool = False
    entry_quality_markout_30s_emitted: bool = False
    # --- Exit reason (V1 exit_reason) ---
    exit_reason: str | None = None
    entered_at_ms: int = 0
    # --- Fills (orders that created this position, for reconciliation) ---
    long_fill: OrderFill | None = None
    short_fill: OrderFill | None = None

    def __post_init__(self) -> None:
        if self.matched_quantity == 0.0:
            self.matched_quantity = min(self.long_quantity, self.short_quantity)
        if self.initial_quantity == 0.0:
            self.initial_quantity = self.matched_quantity
        if self.entered_at_ms == 0:
            self.entered_at_ms = self.opened_at_ms
        if self.total_entry_fee_quote == 0.0:
            self.total_entry_fee_quote = self.long_entry_fee_quote + self.short_entry_fee_quote


@dataclass
class HedgeInflight:
    """V1 PendingInflightHedge — metadata for an in-flight hedge order.

    V1: crates/lightfee-engine/src/lib.rs:569-578
    Fields: client_order_id, venue, side, quantity, attempt, submitted_at_ms,
    soft_deadline_logged.
    """

    client_order_id: str
    venue: "Venue"
    side: "Side"
    quantity: float
    attempt: int = 0
    submitted_at_ms: int = 0
    soft_deadline_logged: bool = False

    def elapsed_ms(self, now_ms: int) -> int:
        """Wall-clock ms since the hedge was submitted."""
        if self.submitted_at_ms <= 0:
            return 0
        return max(0, now_ms - self.submitted_at_ms)

    def to_dict(self) -> dict:
        return {
            "client_order_id": self.client_order_id,
            "venue": self.venue.value,
            "side": self.side.value,
            "quantity": self.quantity,
            "attempt": self.attempt,
            "submitted_at_ms": self.submitted_at_ms,
            "soft_deadline_logged": self.soft_deadline_logged,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "HedgeInflight":
        return cls(
            client_order_id=str(d.get("client_order_id", "")),
            venue=Venue(str(d.get("venue", ""))),
            side=Side(str(d.get("side", ""))),
            quantity=float(d.get("quantity", 0)),
            attempt=int(d.get("attempt", 0)),
            submitted_at_ms=int(d.get("submitted_at_ms", 0)),
            soft_deadline_logged=bool(d.get("soft_deadline_logged", False)),
        )


@dataclass
class PendingPassiveOrder:
    """V1 PendingPassiveOrder: tracks the resting maker order lifecycle.

    Mirrors V1 entry_sync.rs PendingPassiveOrder fields used by
    maintain_pending_entry_passive_order() for active tick-level maintenance:
    - maker_try_window_fill_shortfall (1500ms fill ratio check)
    - maker_entry_rest_timeout (6000ms rest timeout)
    - cancel_requested_at_ms → cancel → abort/finalize lifecycle
    """
    order_id: str = ""
    client_order_id: str = ""
    limit_price: Optional[float] = None
    target_quantity: float = 0.0
    accepted_at_ms: int = 0
    timeout_at_ms: int = 0
    cancel_requested_at_ms: int = 0  # 0 means no cancel requested
    last_progress_state: PassiveOrderState = PassiveOrderState.UNKNOWN
    fill_checkpoint_quantity: float = 0.0
    fill_checkpoint_notional_quote: float = 0.0
    fill_checkpoint_fee_quote: float = 0.0
    fill_checkpoint_last_fill_at_ms: int | None = None

    def to_dict(self) -> dict[str, Any]:
        state = self.last_progress_state
        state_value = (
            state.value if isinstance(state, PassiveOrderState) else str(state or "")
        )
        return {
            "order_id": self.order_id,
            "client_order_id": self.client_order_id,
            "limit_price": self.limit_price,
            "target_quantity": self.target_quantity,
            "accepted_at_ms": self.accepted_at_ms,
            "timeout_at_ms": self.timeout_at_ms,
            "cancel_requested_at_ms": self.cancel_requested_at_ms,
            "last_progress_state": state_value or PassiveOrderState.UNKNOWN.value,
            "fill_checkpoint_quantity": self.fill_checkpoint_quantity,
            "fill_checkpoint_notional_quote": self.fill_checkpoint_notional_quote,
            "fill_checkpoint_fee_quote": self.fill_checkpoint_fee_quote,
            "fill_checkpoint_last_fill_at_ms": self.fill_checkpoint_last_fill_at_ms,
        }

    @classmethod
    def from_dict(cls, data: Any) -> "PendingPassiveOrder | None":
        if not isinstance(data, dict):
            return None
        state_raw = data.get("last_progress_state", PassiveOrderState.UNKNOWN.value)
        if isinstance(state_raw, PassiveOrderState):
            state = state_raw
        else:
            try:
                state = PassiveOrderState(str(state_raw or ""))
            except ValueError:
                state = PassiveOrderState.UNKNOWN
        limit_price_raw = data.get("limit_price")
        limit_price = (
            None
            if limit_price_raw is None
            else float(limit_price_raw)
        )
        return cls(
            order_id=str(data.get("order_id", "") or ""),
            client_order_id=str(data.get("client_order_id", "") or ""),
            limit_price=limit_price,
            target_quantity=float(data.get("target_quantity", 0.0) or 0.0),
            accepted_at_ms=int(data.get("accepted_at_ms", 0) or 0),
            timeout_at_ms=int(data.get("timeout_at_ms", 0) or 0),
            cancel_requested_at_ms=int(data.get("cancel_requested_at_ms", 0) or 0),
            last_progress_state=state,
            fill_checkpoint_quantity=float(data.get("fill_checkpoint_quantity", 0.0) or 0.0),
            fill_checkpoint_notional_quote=float(
                data.get("fill_checkpoint_notional_quote", 0.0) or 0.0
            ),
            fill_checkpoint_fee_quote=float(data.get("fill_checkpoint_fee_quote", 0.0) or 0.0),
            fill_checkpoint_last_fill_at_ms=_optional_int(
                data.get("fill_checkpoint_last_fill_at_ms")
            ),
        )

    def maker_completed(self) -> bool:
        """V1: PendingEntryHedge.maker_completed() — terminal progress state."""
        return self.last_progress_state.is_terminal()

    def cancel_requested(self) -> bool:
        """Whether a cancel has been requested for this passive order."""
        return self.cancel_requested_at_ms > 0

    def timed_out(self, now_ms: int) -> bool:
        """V1: PendingEntryHedge.timed_out() — rest timeout exceeded."""
        return self.timeout_at_ms > 0 and now_ms >= self.timeout_at_ms


@dataclass
class PendingEntryPassivePhaseState:
    """V1 pending-entry PassivePhaseState.

    This is deliberately separate from passive-close PassivePhaseState: V1
    pending entry tracks execution_kind, hedge deadlines, and cycle delay fields
    that passive close does not own.
    """

    execution_kind: str = "entry"
    preferred_maker_leg: str = "long"
    active_maker_leg: str = "long"
    phase: str = "high_slippage_maker"
    zero_fill_cycles_in_phase: int = 0
    cycle_attempt: int = 0
    next_cycle_delay_ms: int | None = None
    small_fill_min_notional_attempts: int = 0
    hedge_deadline_at_ms: int | None = None
    hedge_timeout_grace_deadline_at_ms: int | None = None
    phase_started_at_ms: int = 0
    cycle_started_at_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "execution_kind": self.execution_kind,
            "preferred_maker_leg": self.preferred_maker_leg,
            "active_maker_leg": self.active_maker_leg,
            "phase": self.phase,
            "zero_fill_cycles_in_phase": self.zero_fill_cycles_in_phase,
            "cycle_attempt": self.cycle_attempt,
            "next_cycle_delay_ms": self.next_cycle_delay_ms,
            "small_fill_min_notional_attempts": self.small_fill_min_notional_attempts,
            "hedge_deadline_at_ms": self.hedge_deadline_at_ms,
            "hedge_timeout_grace_deadline_at_ms": self.hedge_timeout_grace_deadline_at_ms,
            "phase_started_at_ms": self.phase_started_at_ms,
            "cycle_started_at_ms": self.cycle_started_at_ms,
        }

    @classmethod
    def from_dict(cls, data: Any) -> "PendingEntryPassivePhaseState | None":
        if not isinstance(data, dict):
            return None
        return cls(
            execution_kind=str(data.get("execution_kind", "entry") or "entry"),
            preferred_maker_leg=str(data.get("preferred_maker_leg", "long") or "long"),
            active_maker_leg=str(data.get("active_maker_leg", "long") or "long"),
            phase=str(data.get("phase", "high_slippage_maker") or "high_slippage_maker"),
            zero_fill_cycles_in_phase=int(data.get("zero_fill_cycles_in_phase", 0) or 0),
            cycle_attempt=int(data.get("cycle_attempt", 0) or 0),
            next_cycle_delay_ms=_optional_int(data.get("next_cycle_delay_ms")),
            small_fill_min_notional_attempts=int(
                data.get("small_fill_min_notional_attempts", 0) or 0
            ),
            hedge_deadline_at_ms=_optional_int(data.get("hedge_deadline_at_ms")),
            hedge_timeout_grace_deadline_at_ms=_optional_int(
                data.get("hedge_timeout_grace_deadline_at_ms")
            ),
            phase_started_at_ms=int(data.get("phase_started_at_ms", 0) or 0),
            cycle_started_at_ms=int(data.get("cycle_started_at_ms", 0) or 0),
        )


@dataclass
class PendingEntryRemainderSlice:
    """V1 PendingEntryRemainderSlice: maker fill remainder metadata."""

    quantity: float = 0.0
    notional_quote: float = 0.0
    fill_at_ms: int | None = None

    def average_price(self) -> float:
        return self.notional_quote / self.quantity if self.quantity > 0.0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "quantity": self.quantity,
            "notional_quote": self.notional_quote,
            "fill_at_ms": self.fill_at_ms,
        }

    @classmethod
    def from_dict(cls, data: Any) -> "PendingEntryRemainderSlice":
        if not isinstance(data, dict):
            return cls()
        return cls(
            quantity=float(data.get("quantity", 0.0) or 0.0),
            notional_quote=float(data.get("notional_quote", 0.0) or 0.0),
            fill_at_ms=_optional_int(data.get("fill_at_ms")),
        )


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@dataclass
class PendingEntry:
    pending_id: str
    symbol: str
    long_venue: Venue
    short_venue: Venue
    target_quantity: float
    long_side: Side
    short_side: Side
    created_at_ms: int
    # --- Metadata (V1: arbitrary metadata dict for entry context) ---
    metadata: dict = field(default_factory=dict)
    # --- Order IDs for reconciliation (Rust V1 maker/hedge order tracking) ---
    maker_order_id: str = ""
    hedge_order_id: str = ""
    # --- Client order IDs for idempotency (Rust V1 clientOrderId dedup) ---
    maker_client_order_id: str = ""
    hedge_client_order_id: str = ""
    # --- Fill quantities per leg ---
    maker_leg_filled: float = 0.0
    hedge_leg_filled: float = 0.0
    # Exact cumulative fees from order/fill reconciliation.  None means the
    # fee is still unknown; 0.0 is an explicitly confirmed zero fee.
    maker_fee_quote: float | None = None
    hedge_fee_quote: float | None = None
    # --- Deadline for timeout-based fallback (Rust V1 deadline/timeout) ---
    deadline_ms: int = 0
    # --- Fallback route (Rust V1 passive_fallback / standard_taker) ---
    fallback_route: str = ""
    # --- Uncertainty flag for reconciliation (Rust V1 uncertain entry outcomes) ---
    uncertain_outcome: bool = False
    # --- Reconciliation retry tracking (Rust V1 exponential backoff) ---
    reconcile_attempt: int = 0
    reconcile_next_attempt_ms: int = 0
    # --- V1 maker-event lane repricing ---
    entry_type: str = ""
    maker_price: float = 0.0
    long_quantity: float = 0.0
    short_quantity: float = 0.0
    # --- V1 recovery dedup: run_id that created this entry ---
    run_id: str = ""
    # --- V1 entry route and outcome tracking ---
    entry_route: str = ""
    outcome: str = ""  # "filled", "rejected", "uncertain", "partial"
    # --- V1 funding lifecycle semantics retained until pending finalization ---
    opportunity_type: str = "aligned"
    funding_timestamp_ms: int = 0
    first_funding_timestamp_ms: int = 0
    long_funding_timestamp_ms: int = 0
    short_funding_timestamp_ms: int = 0
    second_funding_timestamp_ms: int = 0
    first_funding_leg: str = ""
    funding_edge_bps_entry: float = 0.0
    total_funding_edge_bps_entry: float = 0.0
    expected_edge_bps_entry: float = 0.0
    worst_case_edge_bps_entry: float = 0.0
    entry_maker_leg: str = ""
    exit_maker_leg: str = ""
    entry_cross_bps_entry: float = 0.0
    fee_bps_entry: float = 0.0
    entry_slippage_bps_entry: float = 0.0
    transfer_bias_bps_entry: float = 0.0
    transfer_state_at_entry: str | None = None
    entry_liquidity_source_at_entry: str | None = None
    long_volume_24h_quote_at_entry: float = 0.0
    short_volume_24h_quote_at_entry: float = 0.0
    long_open_interest_quote_at_entry: float = 0.0
    short_open_interest_quote_at_entry: float = 0.0
    long_entry_vwap: float | None = None
    short_entry_vwap: float | None = None
    entry_capacity_constrained: bool = False
    entry_target_quantity: float = 0.0
    long_max_executable_quantity: float = 0.0
    short_max_executable_quantity: float = 0.0
    entry_max_executable_quantity: float = 0.0
    entry_depth_shortfall_quantity: float = 0.0
    entry_max_executable_notional_quote: float = 0.0
    entry_depth_capped_at_entry: bool = False
    advisories: list[str] = field(default_factory=list)
    blocked_reasons: list[str] = field(default_factory=list)
    exit_after_first_stage: bool = False
    # --- V1 pending passive-entry lifecycle state ---
    phase_state: PendingEntryPassivePhaseState | None = None
    passive_manager_runtime: Any = field(default_factory=lambda: PassiveOrderManagerRuntime())
    created_cycle: int = 0
    repost_attempt_count: int = 0
    passive_attempt_count: int = 0
    passive_ops_total: int = 0
    maker_remainder_slices: list[PendingEntryRemainderSlice] = field(default_factory=list)
    lifetime_exhausted_logged_final_reason: str | None = None
    frozen_candidate: dict | None = None
    # --- V1 maker entry repost tracking ---
    repost_count: int = 0
    # --- V1 zero-fill terminal cooldown ---
    zero_fill_since_ms: int = 0
    # --- V1 maker-leg routing (CONTRACT RECOVERY-006) ---
    # V1: PendingEntryHedge.maker_leg: HedgeLeg — determines which venue
    # is the maker (passive) and which is the hedge (aggressive).
    # "long" = maker on long_venue (default), "short" = maker on short_venue.
    maker_leg: str = "long"
    # --- V1 hedge inflight tracking for idempotency (CONTRACT HEDGE-INFLIGHT-001) ---
    # V1: PendingInflightHedge — struct with client_order_id, venue, side,
    # quantity, attempt, submitted_at_ms, soft_deadline_logged.
    # Migrated from plain str; None means no inflight hedge.
    hedge_inflight: HedgeInflight | None = None
    # --- V1 hedge attempt counter ---
    # V1 hedge_pending_entry_delta increments this for every submitted hedge
    # attempt and includes it in the client order id seed.
    hedge_attempt_count: int = 0
    # --- V1 maker fill price for hedge price hint ---
    maker_fill_price: float = 0.0
    # --- V1 hedge fill price for entry position recording ---
    hedge_fill_price: float = 0.0
    # --- Terminal repair state for unresolvable residuals ---
    # Values: "" (active), "hedge_residual_below_min_notional" (terminal)
    repair_state: str = ""
    # --- V1 passive order lifecycle (CONTRACT PASSIVE-LIFECYCLE-001) ---
    # V1: PendingEntryHedge.passive_order: PendingPassiveOrder — tracks
    # accepted_at_ms, timeout_at_ms, cancel_requested_at_ms, last_progress_state,
    # limit_price, and target_quantity for active tick-level maker maintenance.
    passive_order: Optional[PendingPassiveOrder] = None
    # --- V1 next progress poll timestamp ---
    # V1: PendingEntryHedge.next_progress_poll_ms — when to next query
    # passive order progress and run maintain_pending_entry_passive_order.
    next_progress_poll_ms: int = 0

    def __post_init__(self) -> None:
        """Migrate legacy string hedge_inflight to HedgeInflight | None."""
        if self.repost_count == 0 and self.repost_attempt_count:
            self.repost_count = self.repost_attempt_count
        if isinstance(self.hedge_inflight, str):
            if self.hedge_inflight:
                self.hedge_inflight = HedgeInflight(
                    client_order_id=self.hedge_inflight,
                    venue=self.hedge_venue(),
                    side=self.hedge_side(),
                    quantity=0.0,
                    attempt=0,
                    submitted_at_ms=0,  # legacy: no timestamp
                )
            else:
                self.hedge_inflight = None
        if isinstance(self.phase_state, dict):
            self.phase_state = PendingEntryPassivePhaseState.from_dict(self.phase_state)
        if isinstance(self.passive_manager_runtime, dict):
            self.passive_manager_runtime = PassiveOrderManagerRuntime.from_dict(
                self.passive_manager_runtime
            )
        self.maker_remainder_slices = [
            item if isinstance(item, PendingEntryRemainderSlice)
            else PendingEntryRemainderSlice.from_dict(item)
            for item in (self.maker_remainder_slices or [])
        ]

    # --- V1 recovery helpers (CONTRACT RECOVERY-002/003) ---

    def missing_hedge_quantity(self) -> float:
        """Quantity still needed on the hedge leg.

        V1: PendingEntryHedge.missing_hedge_quantity() — the gap between
        unmatched maker fill slices and hedged quantity.
        """
        return self.unmatched_maker_quantity()

    def legacy_missing_hedge_quantity(self) -> float:
        """Pre-remainder fallback for restored entries without V1 slices."""
        balanced = min(self.maker_leg_filled, self.target_quantity)
        return max(0.0, balanced - self.hedge_leg_filled)

    def unmatched_maker_quantity(self) -> float:
        """V1: PendingEntryHedge::unmatched_maker_quantity."""
        remainder_quantity = sum(
            max(0.0, item.quantity)
            for item in self.maker_remainder_slices
        )
        if remainder_quantity > 0.0:
            return remainder_quantity
        return self.legacy_missing_hedge_quantity()

    def unmatched_maker_weighted_average_price(self) -> float | None:
        """V1: PendingEntryHedge::unmatched_maker_weighted_average_price."""
        total_quantity = 0.0
        total_notional_quote = 0.0
        for item in self.maker_remainder_slices:
            quantity = max(0.0, item.quantity)
            if quantity <= 0.0:
                continue
            total_quantity += quantity
            total_notional_quote += max(0.0, item.average_price()) * quantity
        if total_quantity > 0.0:
            return total_notional_quote / total_quantity
        if self.legacy_missing_hedge_quantity() > 0.0:
            price = self.maker_fill_price if self.maker_fill_price > 0.0 else self.maker_price
            return max(0.0, price)
        return None

    def maker_completed(self) -> bool:
        """Whether the maker leg is fully filled or terminal.

        V1: PendingEntryHedge.maker_completed() — maker leg fill >= target
        OR passive_order.last_progress_state is a terminal state
        (Filled/Canceled/Rejected).
        """
        if self.maker_leg_filled >= self.target_quantity - 1e-9:
            return True
        if self.passive_order is not None:
            return self.passive_order.maker_completed()
        return False

    def maker_passive_terminal(self) -> bool:
        """True when the passive maker order has reached a terminal progress state
        (canceled/rejected/filled), matching V1 PassiveOrderState terminal check."""
        if self.passive_order is not None:
            return self.passive_order.last_progress_state.is_terminal()
        return False

    def has_any_fill(self) -> bool:
        """Whether any leg has any fill quantity."""
        return self.maker_leg_filled > 1e-9 or self.hedge_leg_filled > 1e-9

    def push_maker_remainder_slice(
        self,
        quantity: float,
        average_price: float | None = None,
        fill_at_ms: int | None = None,
    ) -> None:
        """V1: PendingEntryHedge::push_maker_remainder_slice."""
        quantity = max(0.0, float(quantity or 0.0))
        if quantity <= 1e-9:
            return
        price = average_price
        if price is None or not math.isfinite(float(price)) or float(price) < 0.0:
            price = self.maker_fill_price if self.maker_fill_price > 0.0 else self.maker_price
        price = max(0.0, float(price or 0.0))
        self.maker_remainder_slices.append(
            PendingEntryRemainderSlice(
                quantity=quantity,
                notional_quote=price * quantity,
                fill_at_ms=fill_at_ms if fill_at_ms and fill_at_ms > 0 else None,
            )
        )

    def consume_hedge_quantity_fifo(self, hedge_quantity: float) -> float:
        """V1: PendingEntryHedge::consume_hedge_quantity_fifo."""
        remaining_quantity = max(0.0, float(hedge_quantity or 0.0))
        if remaining_quantity <= 1e-9:
            return 0.0
        if not self.maker_remainder_slices:
            legacy_quantity = self.legacy_missing_hedge_quantity()
            if legacy_quantity > 1e-9:
                self.push_maker_remainder_slice(
                    legacy_quantity,
                    self.maker_fill_price if self.maker_fill_price > 0.0 else self.maker_price,
                    None,
                )
        consumed_quantity = 0.0
        index = 0
        while remaining_quantity > 1e-9 and index < len(self.maker_remainder_slices):
            available_quantity = max(0.0, self.maker_remainder_slices[index].quantity)
            if available_quantity <= 1e-9:
                self.maker_remainder_slices.pop(index)
                continue
            take_quantity = min(remaining_quantity, available_quantity)
            ratio = take_quantity / available_quantity if available_quantity > 0.0 else 0.0
            slice_notional_quote = self.maker_remainder_slices[index].notional_quote
            self.maker_remainder_slices[index].quantity = max(
                0.0,
                available_quantity - take_quantity,
            )
            self.maker_remainder_slices[index].notional_quote = max(
                0.0,
                slice_notional_quote - (slice_notional_quote * ratio),
            )
            if self.maker_remainder_slices[index].quantity <= 1e-9:
                self.maker_remainder_slices.pop(index)
            else:
                index += 1
            remaining_quantity -= take_quantity
            consumed_quantity += take_quantity
        return consumed_quantity

    def startup_recovery_ready(self) -> bool:
        """Whether this pending entry is ready for startup recovery.

        V1: PendingEntryHedge.startup_recovery_ready() —
        true when inflight_hedge exists, cancel is requested, maker is completed,
        or hedge quantity is missing > 1e-9.

        In V2, inflight_hedge maps to uncertain_outcome (an uncertain submit
        implies an order may still be in-flight). Maker completion and missing
        hedge are computed from local fill quantities.
        """
        cancel_requested = (
            self.passive_order is not None
            and self.passive_order.cancel_requested()
        )
        return (
            self.uncertain_outcome
            or self.maker_completed()
            or self.missing_hedge_quantity() > 1e-9
            or self.hedge_inflight is not None
            or cancel_requested
        )

    def compute_lifetime_ms(self, now_ms: int) -> int:
        """Compute the lifetime of this pending entry in milliseconds."""
        if self.created_at_ms <= 0:
            return 0
        return max(0, now_ms - self.created_at_ms)

    # --- V1 venue/side routing (exact replica of PendingEntryHedge methods) ---

    def maker_venue(self):
        """V1: PendingEntryHedge.maker_venue() — venue where maker order was placed.

        entry_sync.rs:336-341 — match maker_leg { Long→long_venue, Short→short_venue }
        """
        if self.maker_leg == "short":
            return self.short_venue
        return self.long_venue

    def hedge_venue(self):
        """V1: PendingEntryHedge.hedge_venue() — venue where hedge order was placed.

        entry_sync.rs:354-359 — match maker_leg { Long→short_venue, Short→long_venue }
        """
        if self.maker_leg == "short":
            return self.long_venue
        return self.short_venue

    def maker_side(self):
        """V1: PendingEntryHedge.maker_side() — Side of the maker leg.

        entry_sync.rs:368-373 — match maker_leg { Long→Buy, Short→Sell }
        """
        if self.maker_leg == "short":
            return self.short_side
        return self.long_side

    def hedge_side(self):
        """V1: PendingEntryHedge.hedge_side() — Side of the hedge leg.

        entry_sync.rs:375-377 — maker_side().opposite()
        """
        if self.maker_leg == "short":
            return self.long_side
        return self.short_side


# ---------------------------------------------------------------------------
# Passive close state (V1 PendingPassiveClose parity)
# ---------------------------------------------------------------------------


class PassiveExecutionPhase(Enum):
    """V1 PassiveExecutionPhase: maker slippage phase for passive close."""
    HIGH_SLIPPAGE_MAKER = "high_slippage_maker"
    LOW_SLIPPAGE_MAKER = "low_slippage_maker"
    DUAL_TAKER = "dual_taker"  # fallback to aggressive


class ActiveMakerLeg(Enum):
    """Which leg is the passive maker."""
    LONG = "long"
    SHORT = "short"

    def label(self) -> str:
        return self.value


@dataclass
class PassivePhaseState:
    """V1 PassivePhaseState: tracks the current passive execution phase and cycles."""
    phase: PassiveExecutionPhase = PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER
    preferred_maker_leg: ActiveMakerLeg = ActiveMakerLeg.LONG
    active_maker_leg: ActiveMakerLeg = ActiveMakerLeg.LONG
    phase_started_at_ms: int = 0
    cycle_attempt: int = 1
    cycle_started_at_ms: int = 0
    zero_fill_cycles_in_phase: int = 0
    maker_submit_attempt: int = 0
    maker_submit_consecutive_failures: int = 0
    missing_l2_tick_consecutive_count: int = 0
    maker_order_id: str = ""
    maker_client_order_id: str = ""
    maker_resting_limit_price: Optional[float] = None
    maker_resting_since_ms: int = 0


@dataclass
class PendingPassiveLegFill:
    """V1 PendingEntryLegFill: cumulative fill tracking for a passive close leg."""
    quantity: float = 0.0
    average_price: float = 0.0
    fee_quote: float = 0.0
    # Keep absence of exchange fee evidence distinct from a confirmed zero.
    # Old snapshots omit this key and therefore recover fail-closed.
    fee_evidence_complete: bool = False
    last_fill_time_ms: int = 0
    order_id: str = ""
    client_order_id: str = ""


@dataclass
class PersistedCloseExecutionLeg:
    """V1 PersistedCloseExecutionLeg: serializable close leg for passive close."""
    fill: Optional[OrderFill] = None
    # ``False`` is an explicit evidence gap. ``None`` preserves the behavior
    # of in-memory test fixtures that predate this field; recovered snapshots
    # always materialize a boolean and therefore fail closed when absent.
    fee_evidence_complete: bool | None = None
    client_order_id: str = ""
    submit_started_at_ms: int = 0
    latency_ms: int = 0


@dataclass
class PassiveOrderManagerRuntime:
    """V1 PassiveOrderManagerRuntime: per-venue passive order management state."""
    cooldown_until_ms: Optional[int] = None
    consecutive_failures: int = 0
    last_success_ms: int = 0
    last_attempt_ms: int = 0
    ops_budget_remaining: int = 0
    ops_budget_reset_ms: int = 0
    last_operation_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "cooldown_until_ms": self.cooldown_until_ms,
            "consecutive_failures": self.consecutive_failures,
            "last_success_ms": self.last_success_ms,
            "last_attempt_ms": self.last_attempt_ms,
            "ops_budget_remaining": self.ops_budget_remaining,
            "ops_budget_reset_ms": self.ops_budget_reset_ms,
            "last_operation_ms": self.last_operation_ms,
        }

    @classmethod
    def from_dict(cls, data: Any) -> "PassiveOrderManagerRuntime":
        if not isinstance(data, dict):
            return cls()
        return cls(
            cooldown_until_ms=_optional_int(data.get("cooldown_until_ms")),
            consecutive_failures=int(data.get("consecutive_failures", 0) or 0),
            last_success_ms=int(data.get("last_success_ms", 0) or 0),
            last_attempt_ms=int(data.get("last_attempt_ms", 0) or 0),
            ops_budget_remaining=int(data.get("ops_budget_remaining", 0) or 0),
            ops_budget_reset_ms=int(data.get("ops_budget_reset_ms", 0) or 0),
            last_operation_ms=int(data.get("last_operation_ms", 0) or 0),
        )


@dataclass
class PendingPassiveClose:
    """V1 PendingPassiveClose: passive close pending state for recovery and maintenance.

    Tracks a per-position passive close lifecycle: chunked maker+taker close
    where the maker leg is GTC post-only and the hedge leg is IOC reduce-only.
    """
    position_id: str
    reason: str
    position_snapshot: Optional[OpenPosition] = None
    short_stage: str = ""
    long_stage: str = ""
    target_quantity: float = 0.0
    max_slippage_bps: Optional[float] = None
    chunk_quantities: list[float] = field(default_factory=list)
    active_chunk_index: int = 0
    phase_state: PassivePhaseState = field(default_factory=PassivePhaseState)
    maker_fill: PendingPassiveLegFill = field(default_factory=PendingPassiveLegFill)
    hedge_fill: PendingPassiveLegFill = field(default_factory=PendingPassiveLegFill)
    long_legs: list[PersistedCloseExecutionLeg] = field(default_factory=list)
    short_legs: list[PersistedCloseExecutionLeg] = field(default_factory=list)
    passive_manager_runtimes: dict[str, PassiveOrderManagerRuntime] = field(default_factory=dict)
    small_fill_min_notional_attempts: int = 0
    last_small_fill_missing_quantity: float = 0.0
    small_fill_buffer_started_at_ms: Optional[int] = None
    next_retry_at_ms: int = 0
    multi_phase_started_at_ms: int = 0
    created_cycle: int = 0
    # V1: ops token bucket rate limiting for passive close maintenance
    ops_count_this_window: int = 0
    ops_window_started_at_ms: int = 0

    def current_chunk_quantity(self) -> float:
        if self.active_chunk_index < len(self.chunk_quantities):
            return self.chunk_quantities[self.active_chunk_index]
        return 0.0

    def remaining_chunk_quantity(self) -> float:
        return max(self.current_chunk_quantity() - self.maker_fill.quantity, 0.0)

    def current_chunk_suffix(self) -> str:
        if len(self.chunk_quantities) > 1:
            return f"_chunk_{self.active_chunk_index + 1}"
        return ""

    def chunk_count(self) -> int:
        return len(self.chunk_quantities)

    def completed(self) -> bool:
        return self.active_chunk_index >= len(self.chunk_quantities)


@dataclass
class CloseLegRecord:
    """V1 CloseLegRecord: per-leg fill data for close reconciliation.

    Tracks individual fill details for each venue leg of a close execution,
    used for post-close reconciliation to verify order outcomes.
    """
    venue: str
    order_id: str = ""
    client_order_id: str = ""
    quantity: float = 0.0
    average_price: float = 0.0
    fee_quote: float = 0.0


@dataclass
class PendingClose:
    close_id: str
    position_id: str
    reason: str
    created_at_ms: int
    # --- Order IDs per leg (Rust V1 close order tracking) ---
    long_order_id: str = ""
    short_order_id: str = ""
    # --- Client order IDs for idempotency (Rust V1 clientOrderId dedup) ---
    long_client_order_id: str = ""
    short_client_order_id: str = ""
    # --- Target close quantities per leg ---
    long_target_close_qty: float = 0.0
    short_target_close_qty: float = 0.0
    # --- Fill tracking ---
    long_closed: float = 0.0
    short_closed: float = 0.0
    # --- Deadline (Rust V1 close deadline/timeout) ---
    deadline_ms: int = 0
    # --- Uncertainty flags per leg (Rust V1 uncertain close outcomes) ---
    long_uncertain: bool = False
    short_uncertain: bool = False
    # --- Reconciliation retry tracking (Rust V1 exponential backoff) ---
    reconcile_attempt: int = 0
    reconcile_next_attempt_ms: int = 0
    # --- V1 recovery dedup: run_id that created this close ---
    run_id: str = ""
    # --- V1 chunk tracking for large closes ---
    chunk_index: int = 0
    total_chunks: int = 1
    # --- V1 per-leg fill records (CloseLegRecord vectors) ---
    long_legs: list[CloseLegRecord] = field(default_factory=list)
    short_legs: list[CloseLegRecord] = field(default_factory=list)


@dataclass
class OperatorControlState:
    requested_mode: GlobalRiskMode | None = None
    pending_reconcile: bool = False


@dataclass
class RecoveryWorkSnapshot:
    has_open_positions: bool = False
    has_pending_entries: bool = False
    has_pending_closes: bool = False
    has_pending_passive_closes: bool = False
    has_pending_residual_repairs: bool = False
    ambiguous_state: bool = False
    lifecycle: EngineLifecycle = EngineLifecycle.BOOTING


MAX_PENDING_CLOSE_RECONCILIATIONS = 256


class BillingEvidenceImportError(ValueError):
    """Raised when operator billing evidence cannot safely replace a debt."""


def _reconciliation_identity_keys(item: Any) -> set[tuple[str, str]]:
    """Return the durable order identities carried by a reconciliation item."""
    if not isinstance(item, dict):
        return set()
    keys: set[tuple[str, str]] = set()
    for leg_group in (item.get("long_legs"), item.get("short_legs")):
        if not isinstance(leg_group, list):
            continue
        for leg in leg_group:
            if not isinstance(leg, dict):
                continue
            order_id = str(leg.get("order_id") or "")
            client_order_id = str(leg.get("client_order_id") or "")
            if order_id or client_order_id:
                keys.add((order_id, "" if order_id else client_order_id))
    return keys


def _reconciliation_identity_coverage(item: Any) -> tuple[int, int]:
    """Count durable identities independently for the long and short legs."""
    if not isinstance(item, dict):
        return (0, 0)
    coverage: list[int] = []
    for leg_group in (item.get("long_legs"), item.get("short_legs")):
        if not isinstance(leg_group, list):
            coverage.append(0)
            continue
        coverage.append(
            sum(
                isinstance(leg, dict)
                and bool(leg.get("order_id") or leg.get("client_order_id"))
                for leg in leg_group
            )
        )
    return (coverage[0], coverage[1])


def _reconciliation_snapshot_evidence(item: Any) -> int:
    """Count typed routing facts carried by a reconciliation snapshot."""
    if not isinstance(item, dict):
        return 0
    snapshot = item.get("position_snapshot")
    if not isinstance(snapshot, dict):
        return 0
    return sum(
        bool(snapshot.get(field))
        for field in ("position_id", "symbol", "long_venue", "short_venue")
    )


def is_unattributed_recovered_live_flat_reconciliation(item: Any) -> bool:
    """Whether a recovered-flat record has no durable V2 execution owner.

    Startup recovery represents an exchange-observed pair in local state to
    protect it from a new entry.  If that temporary pair later proves flat but
    carries neither a persisted V2 close leg nor an original V2 order identity,
    V2 cannot truthfully own its PnL.  Keep the predicate deliberately strict:
    malformed or partially identified records remain fail-closed accounting
    work.
    """
    if not isinstance(item, dict):
        return False
    position_id = str(item.get("position_id") or "")
    if not position_id.startswith("live-recovered:"):
        return False
    if str(item.get("kind") or "final") not in {"final", "partial"}:
        return False
    snapshot = item.get("position_snapshot")
    if not isinstance(snapshot, dict) or str(snapshot.get("position_id") or "") != position_id:
        return False
    if snapshot.get("entry_fee_evidence_complete") is True:
        return False
    for leg_group in (item.get("long_legs"), item.get("short_legs")):
        if not isinstance(leg_group, list):
            return False
        for leg in leg_group:
            if not isinstance(leg, dict):
                return False
            if leg.get("order_id") or leg.get("client_order_id"):
                return False
    original_payload = item.get("original_payload")
    if not isinstance(original_payload, dict):
        return False
    for identity_field in ("order_ids", "client_order_ids"):
        values = original_payload.get(identity_field)
        if not isinstance(values, list):
            return False
        if any(str(value or "") for value in values):
            return False
    return True


def pending_close_reconciliation_missing_legs(
    reconciliation: dict[str, Any],
) -> tuple[str, ...]:
    """Identify final/partial close legs that lack durable lookup identity.

    An empty leg group is only complete when its persisted expected quantity is
    explicitly zero.  Missing or malformed quantities remain unknown and must
    fail closed.  ``accepted_order_truth_gap`` callers intentionally do not use
    this helper because that task kind is allowed to contain one leg only.
    """
    snapshot = reconciliation.get("position_snapshot") or {}
    if not isinstance(snapshot, dict):
        snapshot = {}

    missing: list[str] = []
    for leg_label in ("long", "short"):
        legs = reconciliation.get(f"{leg_label}_legs")
        complete = isinstance(legs, list)
        if complete:
            complete = all(
                isinstance(leg, dict)
                and bool(leg.get("order_id") or leg.get("client_order_id"))
                for leg in legs
            )
        if complete and legs:
            continue

        expected: float | None = None
        for key in (f"{leg_label}_quantity", "matched_quantity"):
            if key not in snapshot:
                continue
            try:
                value = float(snapshot.get(key))
            except (TypeError, ValueError):
                expected = None
                break
            if math.isfinite(value) and value >= 0.0:
                expected = value
                break
            expected = None
            break
        if complete and not legs and expected is not None and expected <= 1e-12:
            continue
        missing.append(leg_label)
    return tuple(missing)


def pending_close_reconciliation_identity_evidence(
    reconciliation: dict[str, Any],
) -> dict[str, Any]:
    """Summarize durable close lookup identity without changing its meaning.

    This diagnostic is intentionally derived from the existing reconciliation
    payload and ``pending_close_reconciliation_missing_legs`` contract.  It is
    used to distinguish a real exchange order id from a CID-only lookup, a
    local recovery placeholder, and genuinely absent evidence; no execution
    or billing decision consumes the summary.
    """
    def summarize(leg_label: str) -> dict[str, int]:
        legs = reconciliation.get(f"{leg_label}_legs")
        if not isinstance(legs, list):
            legs = []

        summary = {
            "leg_count": len(legs),
            "exchange_order_id_count": 0,
            "client_order_id_only_count": 0,
            "recovery_placeholder_count": 0,
            "missing_identity_count": 0,
        }
        for leg in legs:
            if not isinstance(leg, dict):
                summary["missing_identity_count"] += 1
                continue
            order_id = str(leg.get("order_id") or "")
            client_order_id = str(leg.get("client_order_id") or "")
            if "-recovery-" in order_id.lower():
                summary["recovery_placeholder_count"] += 1
            elif order_id:
                summary["exchange_order_id_count"] += 1
            elif client_order_id:
                summary["client_order_id_only_count"] += 1
            else:
                summary["missing_identity_count"] += 1
        return summary

    return {
        "missing_identity_legs": list(
            pending_close_reconciliation_missing_legs(reconciliation)
        ),
        "long": summarize("long"),
        "short": summarize("short"),
    }


def pending_close_reconciliation_evidence_debt_reason(
    reconciliation: Any,
) -> str | None:
    """Return the non-retryable evidence gap for a billing-close task.

    V1 constructs reconciliation work from typed position snapshots and durable
    close-leg identities.  A legacy V2 record without those routing facts can
    never be repaired by another execution-history request.  It remains a
    fail-closed accounting owner, but must be classified once as an evidence
    debt instead of being retried forever.
    """
    if not isinstance(reconciliation, dict):
        return "invalid_reconciliation_item"
    if reconciliation.get("invalid_pending_close_reconciliation") is True:
        return str(reconciliation.get("reason") or "invalid_reconciliation_item")

    kind = str(reconciliation.get("kind") or "final")
    if kind == "accepted_order_truth_gap":
        # Order-truth tasks have their own one-leg contract and do not enter
        # the billing evidence route.
        return None
    if kind not in {"final", "partial"}:
        return "unsupported_reconciliation_kind"

    snapshot = reconciliation.get("position_snapshot")
    if not isinstance(snapshot, dict) or not snapshot:
        return "missing_position_snapshot"
    if not str(reconciliation.get("position_id") or snapshot.get("position_id") or ""):
        return "missing_position_id"
    if not str(reconciliation.get("symbol") or snapshot.get("symbol") or ""):
        return "missing_symbol"
    if not str(reconciliation.get("long_venue") or snapshot.get("long_venue") or "") or not str(
        reconciliation.get("short_venue") or snapshot.get("short_venue") or ""
    ):
        return "missing_position_snapshot_venues"
    if pending_close_reconciliation_missing_legs(reconciliation):
        return "missing_close_order_identity"
    return None


def pending_close_reconciliation_import_reason(
    reconciliation: Any,
) -> str | None:
    """Return the evidence gap that forbids an operator debt replacement.

    The normal reconciliation validator deliberately accepts a minimal typed
    task because live execution history remains the source of close-fill truth.
    An operator import has a stricter boundary: it must also carry the entry
    accounting facts needed to compute a non-provisional terminal result.  This
    prevents an import from merely changing an evidence-debt label while still
    allowing the runtime to eventually abandon it as accounting-unavailable.
    """
    reason = pending_close_reconciliation_evidence_debt_reason(reconciliation)
    if reason is not None:
        return reason
    if not isinstance(reconciliation, dict):
        return "invalid_reconciliation_item"
    if _reconciliation_int(reconciliation.get("closed_at_ms")) <= 0:
        return "missing_closed_at_ms"

    snapshot = reconciliation.get("position_snapshot")
    if not isinstance(snapshot, dict):
        return "missing_position_snapshot"
    position_id = str(reconciliation.get("position_id") or "")
    symbol = str(reconciliation.get("symbol") or "")
    if str(snapshot.get("position_id") or "") != position_id:
        return "position_snapshot_position_id_mismatch"
    if str(snapshot.get("symbol") or "") != symbol:
        return "position_snapshot_symbol_mismatch"

    for snapshot_field in (
        "long_quantity",
        "short_quantity",
        "long_entry_price",
        "short_entry_price",
        "total_entry_fee_quote",
        "captured_funding_quote",
    ):
        try:
            value = float(snapshot[snapshot_field])
        except (KeyError, TypeError, ValueError):
            return f"missing_or_invalid_{snapshot_field}"
        if not math.isfinite(value):
            return f"missing_or_invalid_{snapshot_field}"
        if snapshot_field in {
            "long_quantity",
            "short_quantity",
            "long_entry_price",
            "short_entry_price",
        } and value < 0.0:
            return f"missing_or_invalid_{snapshot_field}"
    if snapshot.get("entry_fee_evidence_complete") is not True:
        return "entry_fee_evidence_incomplete"

    for leg_label, venue_field in (
        ("long", "long_venue"),
        ("short", "short_venue"),
    ):
        venue = str(snapshot.get(venue_field) or "")
        legs = reconciliation.get(f"{leg_label}_legs")
        if not isinstance(legs, list):
            return f"missing_{leg_label}_legs"
        for leg in legs:
            if not isinstance(leg, dict):
                return f"invalid_{leg_label}_leg"
            if str(leg.get("venue") or "") != venue:
                return f"{leg_label}_leg_venue_mismatch"
    return None


def normalize_pending_close_reconciliations(raw: Any) -> list[dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        if _looks_like_pending_close_reconciliation(raw):
            items = [raw]
        else:
            items = list(raw.values())
    else:
        return [_invalid_pending_close_reconciliation(raw, "invalid_container")]

    normalized: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, dict):
            normalized.append(dict(item))
        else:
            normalized.append(_invalid_pending_close_reconciliation(item, "invalid_item"))
    return normalized


def _looks_like_pending_close_reconciliation(raw: dict[Any, Any]) -> bool:
    return any(
        key in raw
        for key in (
            "position_id",
            "symbol",
            "kind",
            "closed_at_ms",
            "position_snapshot",
            "long_venue",
            "short_venue",
            "long_legs",
            "short_legs",
        )
    )


def _invalid_pending_close_reconciliation(raw: Any, reason: str) -> dict[str, Any]:
    return {
        "invalid_pending_close_reconciliation": True,
        "reason": reason,
        "raw_type": type(raw).__name__,
        "raw_repr": repr(raw)[:240],
    }


def _reconciliation_int(value: Any, default: int = 0) -> int:
    """Read a persisted reconciliation integer without crashing diagnostics."""
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _build_reconciliation_summary(queue: list[dict[str, Any]]) -> dict[str, Any]:
    """Export a safe summary of the reconciliation queue for diagnostics.

    The summary exposes total count and per-kind breakdown so diagnose /
    production-health can authoritatively decide whether the queue is truly
    empty, without scanning the full task list.
    """
    total = len(queue)
    by_kind: dict[str, int] = {}
    backed_off = 0
    unknown_status = 0
    evidence_debt_count = 0
    evidence_debt_by_reason: dict[str, int] = {}
    for item in queue:
        if not isinstance(item, dict):
            unknown_status += 1
            continue
        kind = str(item.get("kind") or "unknown")
        by_kind[kind] = by_kind.get(kind, 0) + 1
        if _reconciliation_int(item.get("next_attempt_ms")) > 0:
            backed_off += 1
        if item.get("reconciliation_status") == "evidence_debt":
            evidence_debt_count += 1
            reason = str(item.get("evidence_debt_reason") or "unknown")
            evidence_debt_by_reason[reason] = (
                evidence_debt_by_reason.get(reason, 0) + 1
            )
    return {
        "total_count": total,
        "by_kind": by_kind,
        "backed_off_count": backed_off,
        "unknown_status_count": unknown_status,
        "evidence_debt_count": evidence_debt_count,
        "evidence_debt_by_reason": evidence_debt_by_reason,
    }


def pending_close_reconciliation_summary(raw: Any) -> dict[str, Any]:
    """Build the public reconciliation summary from any persisted queue form."""
    return _build_reconciliation_summary(normalize_pending_close_reconciliations(raw))


@dataclass
class EngineState:
    lifecycle: EngineLifecycle = EngineLifecycle.BOOTING
    risk_mode: GlobalRiskMode = GlobalRiskMode.RUNNING
    operator: OperatorControlState = field(default_factory=OperatorControlState)
    open_positions: dict[str, OpenPosition] = field(default_factory=dict)
    pending_entries: dict[str, PendingEntry] = field(default_factory=dict)
    pending_closes: dict[str, PendingClose] = field(default_factory=dict)
    pending_passive_closes: dict[str, PendingPassiveClose] = field(default_factory=dict)
    run_id: str = ""
    started_at_ms: int = 0
    last_tick_ms: int = 0
    tick_count: int = 0
    venue_health: dict[str, str] = field(default_factory=dict)
    # --- Recovery blocked state (V1 recovery_blocked_reason, recovery_blocked_at_ms) ---
    recovery_blocked_reason: str | None = None
    recovery_blocked_at_ms: int = 0
    # --- Global risk reason (V1 global_risk_reason) ---
    global_risk_reason: str | None = None
    # --- Hyperliquid trading admission (disabled until signer/account auth is proven) ---
    hyperliquid_trading_disabled_reason: str | None = None
    # --- Pending residual repairs (V1 pending_residual_repairs) ---
    pending_residual_repairs: list = field(default_factory=list)
    # --- Live recovery reduce-only pairs (V1 live_recovery_reduce_only_pairs) ---
    live_recovery_reduce_only_pairs: list = field(default_factory=list)
    # --- Venue entry cooldowns (V1 venue_entry_cooldowns) ---
    venue_entry_cooldowns: dict = field(default_factory=dict)
    # --- Venue market data degradations (V1 venue_market_data_degradations) ---
    venue_market_data_degradations: dict = field(default_factory=dict)
    # --- Transfer truth outage state (V1 transfer_truth) ---
    transfer_truth: dict = field(default_factory=dict)
    # --- Entry liquidity qualification records (V1 entry_liquidity_qualification_records) ---
    entry_liquidity_qualification_records: list = field(default_factory=list)
    # --- Pending close reconciliations (V1 pending_close_reconciliations) ---
    pending_close_reconciliations: list[dict[str, Any]] = field(default_factory=list)
    # --- Local-L2 state for persistence/recovery (V1 parity) ---
    retained_local_l2_books: list[dict] = field(default_factory=list)
    local_l2_books_snapshot: list[dict] = field(default_factory=list)
    local_l2_session_snapshot: list[dict] = field(default_factory=list)
    last_scan: dict | None = None
    runtime_progress: dict[str, Any] = field(default_factory=dict)
    runtime_market_data_config: dict[str, Any] = field(default_factory=dict)
    v1_lifecycle_closure: dict[str, Any] = field(default_factory=dict)
    # Slow account data; retained across restarts for private-endpoint outages.
    account_fee_snapshots: dict[str, dict[str, Any]] = field(default_factory=dict)
    # --- V1 PassiveOrderManager runtime state persistence ---
    # Maps entry_id -> PassiveOrderManager.runtime_dict()
    passive_order_manager_states: dict[str, dict] = field(default_factory=dict)

    def set_pending_close_reconciliations(self, raw: Any) -> None:
        self.pending_close_reconciliations = normalize_pending_close_reconciliations(raw)[
            -MAX_PENDING_CLOSE_RECONCILIATIONS:
        ]

    def enqueue_pending_close_reconciliation(self, item: dict[str, Any]) -> None:
        self.set_pending_close_reconciliations(self.pending_close_reconciliations)
        position_id = str(item.get("position_id") or "")
        kind = str(item.get("kind") or "final")
        candidate_keys = _reconciliation_identity_keys(item)
        candidate_coverage = _reconciliation_identity_coverage(item)
        for existing in self.pending_close_reconciliations:
            if (
                str(existing.get("position_id") or "") == position_id
                and str(existing.get("kind") or "final") == kind
            ):
                existing_keys = _reconciliation_identity_keys(existing)
                existing_coverage = _reconciliation_identity_coverage(existing)
                identity_evidence_not_weaker = all(
                    candidate >= previous
                    for candidate, previous in zip(
                        candidate_coverage, existing_coverage
                    )
                )
                # A later close-truth observation may enrich an earlier task
                # that was persisted without an exchange identity (or with
                # only one leg's identity).  Keep the same position/kind
                # owner, but replace it when the new record carries at least
                # as much identity evidence or a stronger reconciliation mode.
                stronger_mode = (
                    item.get("reconciliation_mode")
                    == "venue_execution_history_required"
                    and existing.get("reconciliation_mode")
                    != "venue_execution_history_required"
                    and identity_evidence_not_weaker
                )
                stronger_snapshot_evidence = (
                    _reconciliation_snapshot_evidence(item)
                    > _reconciliation_snapshot_evidence(existing)
                )
                replaces_evidence_debt = (
                    existing.get("reconciliation_status") == "evidence_debt"
                    and item.get("reconciliation_status") != "evidence_debt"
                    and identity_evidence_not_weaker
                    and stronger_snapshot_evidence
                )
                records_evidence_debt = (
                    item.get("reconciliation_status") == "evidence_debt"
                    and existing.get("reconciliation_status") != "evidence_debt"
                )
                if (
                    candidate_keys != existing_keys
                    and identity_evidence_not_weaker
                ) or stronger_mode or replaces_evidence_debt or records_evidence_debt:
                    index = self.pending_close_reconciliations.index(existing)
                    self.pending_close_reconciliations[index] = dict(item)
                return
        self.pending_close_reconciliations.append(dict(item))
        if len(self.pending_close_reconciliations) > MAX_PENDING_CLOSE_RECONCILIATIONS:
            self.pending_close_reconciliations = self.pending_close_reconciliations[
                -MAX_PENDING_CLOSE_RECONCILIATIONS:
            ]

    def import_pending_close_reconciliation_evidence(
        self,
        item: Any,
        *,
        evidence_reference: str,
        evidence_sha256: str,
        imported_at_ms: int,
        allow_idempotent: bool = False,
    ) -> dict[str, Any]:
        """Replace exactly one historical billing debt with stronger evidence.

        This is the sole mutable entry point for operator-supplied close
        accounting evidence.  It never creates a new owner, never changes the
        physical-close identity, and does not mark a debt reconciled.  The live
        reconciliation lane still queries exchange execution history by the
        imported durable order identities before it can emit ``exit.reconciled``.
        """
        if not isinstance(item, dict):
            raise BillingEvidenceImportError(
                "evidence reconciliation must be an object"
            )
        reference = str(evidence_reference or "").strip()
        digest = str(evidence_sha256 or "").strip().lower()
        if not reference:
            raise BillingEvidenceImportError("evidence_reference is required")
        if len(digest) != 64 or any(
            char not in "0123456789abcdef" for char in digest
        ):
            raise BillingEvidenceImportError(
                "evidence_sha256 must be a SHA-256 hex digest"
            )
        if _reconciliation_int(imported_at_ms) <= 0:
            raise BillingEvidenceImportError("imported_at_ms must be positive")

        allowed_fields = {
            "position_id",
            "kind",
            "closed_at_ms",
            "position_snapshot",
            "long_legs",
            "short_legs",
        }
        unknown_fields = sorted(set(item) - allowed_fields)
        if unknown_fields:
            raise BillingEvidenceImportError(
                "unsupported evidence fields: " + ", ".join(unknown_fields)
            )
        position_id = str(item.get("position_id") or "")
        kind = str(item.get("kind") or "final")
        closed_at_ms = _reconciliation_int(item.get("closed_at_ms"))
        if not position_id:
            raise BillingEvidenceImportError("position_id is required")
        if kind not in {"final", "partial"}:
            raise BillingEvidenceImportError("kind must be final or partial")
        if closed_at_ms <= 0:
            raise BillingEvidenceImportError("closed_at_ms must be positive")
        if "position_snapshot" in item and not isinstance(
            item["position_snapshot"], dict
        ):
            raise BillingEvidenceImportError(
                "position_snapshot must be an object"
            )
        for leg_field in ("long_legs", "short_legs"):
            if leg_field in item and not isinstance(item[leg_field], list):
                raise BillingEvidenceImportError(f"{leg_field} must be a list")

        self.set_pending_close_reconciliations(self.pending_close_reconciliations)
        owner_matches = [
            (index, existing)
            for index, existing in enumerate(self.pending_close_reconciliations)
            if isinstance(existing, dict)
            and str(existing.get("position_id") or "") == position_id
            and str(existing.get("kind") or "final") == kind
            and _reconciliation_int(existing.get("closed_at_ms")) == closed_at_ms
        ]
        if len(owner_matches) != 1:
            raise BillingEvidenceImportError(
                "evidence must match exactly one pending reconciliation owner"
            )
        index, existing = owner_matches[0]

        existing_import = existing.get("operator_evidence")
        if (
            allow_idempotent
            and existing.get("reconciliation_status") == "operator_evidence_imported"
            and isinstance(existing_import, dict)
            and str(existing_import.get("sha256") or "").lower() == digest
            and str(existing_import.get("reference") or "") == reference
        ):
            return dict(existing)
        if existing.get("reconciliation_status") != "evidence_debt":
            raise BillingEvidenceImportError("target owner is not an evidence debt")

        candidate = dict(existing)
        for evidence_field in ("position_snapshot", "long_legs", "short_legs"):
            if evidence_field in item:
                candidate[evidence_field] = item[evidence_field]

        snapshot = candidate.get("position_snapshot")
        if isinstance(snapshot, dict):
            candidate["symbol"] = str(
                candidate.get("symbol") or snapshot.get("symbol") or ""
            )
            candidate["long_venue"] = str(
                candidate.get("long_venue") or snapshot.get("long_venue") or ""
            )
            candidate["short_venue"] = str(
                candidate.get("short_venue") or snapshot.get("short_venue") or ""
            )

        import_reason = pending_close_reconciliation_import_reason(candidate)
        if import_reason is not None:
            raise BillingEvidenceImportError(
                f"imported evidence is incomplete: {import_reason}"
            )

        candidate["reconciliation_status"] = "operator_evidence_imported"
        candidate.pop("evidence_debt_reason", None)
        candidate.pop("evidence_debt_at_ms", None)
        candidate.pop("missing_close_order_identity", None)
        candidate["billing_reconciliation_required"] = True
        candidate["reconciliation_mode"] = "venue_execution_history_required"
        candidate["next_attempt_ms"] = _reconciliation_int(imported_at_ms)
        candidate["attempt_count"] = 0
        candidate["operator_evidence"] = {
            "reference": reference,
            "sha256": digest,
            "imported_at_ms": _reconciliation_int(imported_at_ms),
        }
        self.pending_close_reconciliations[index] = candidate
        return dict(candidate)

    def remove_pending_close_reconciliation(self, task: dict[str, Any]) -> bool:
        self.set_pending_close_reconciliations(self.pending_close_reconciliations)
        before = len(self.pending_close_reconciliations)
        target = (
            str(task.get("position_id") or ""),
            str(task.get("kind") or "final"),
            _reconciliation_int(task.get("closed_at_ms")),
        )
        self.pending_close_reconciliations = [
            item
            for item in self.pending_close_reconciliations
            if (
                str(item.get("position_id") or ""),
                str(item.get("kind") or "final"),
                _reconciliation_int(item.get("closed_at_ms")),
            )
            != target
        ]
        return len(self.pending_close_reconciliations) != before

    def to_dict(self) -> dict:
        pending_close_reconciliations = normalize_pending_close_reconciliations(
            self.pending_close_reconciliations
        )
        return {
            "lifecycle": self.lifecycle.value,
            "risk_mode": self.risk_mode.value,
            "run_id": self.run_id,
            "started_at_ms": self.started_at_ms,
            "last_tick_ms": self.last_tick_ms,
            "tick_count": self.tick_count,
            "open_position_count": len(self.open_positions),
            "pending_entry_count": len(self.pending_entries),
            "pending_close_count": len(self.pending_closes),
            "pending_passive_close_count": len(self.pending_passive_closes),
            "pending_close_reconciliation_count": len(pending_close_reconciliations),
            "global_risk_reason": self.global_risk_reason,
            "hyperliquid_trading_disabled_reason": self.hyperliquid_trading_disabled_reason,
            "recovery_blocked_reason": self.recovery_blocked_reason,
            "recovery_blocked_at_ms": self.recovery_blocked_at_ms,
            "pending_residual_repairs": self.pending_residual_repairs,
            "live_recovery_reduce_only_pairs": self.live_recovery_reduce_only_pairs,
            "venue_entry_cooldowns": self.venue_entry_cooldowns,
            "venue_market_data_degradations": self.venue_market_data_degradations,
            "transfer_truth": self.transfer_truth,
            "entry_liquidity_qualification_records": self.entry_liquidity_qualification_records,
            "pending_close_reconciliations": pending_close_reconciliations,
            "pending_close_reconciliation_summary": (
                pending_close_reconciliation_summary(pending_close_reconciliations)
            ),
            "last_scan": self.last_scan,
            "runtime_progress": dict(self.runtime_progress or {}),
            "runtime_market_data_config": dict(self.runtime_market_data_config or {}),
            "v1_lifecycle_closure": dict(self.v1_lifecycle_closure or {}),
            "account_fee_snapshots": dict(self.account_fee_snapshots or {}),
            "retained_local_l2_books": self.retained_local_l2_books,
            "local_l2_books_snapshot": self.local_l2_books_snapshot,
            "local_l2_session_snapshot": self.local_l2_session_snapshot,
            "passive_order_manager_states": self.passive_order_manager_states,
            "operator": {
                "requested_mode": self.operator.requested_mode.value if self.operator.requested_mode else None,
                "pending_reconcile": self.operator.pending_reconcile,
            },
            "open_positions": {
                pid: {
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
                    "entry_notional_quote": pos.entry_notional_quote,
                    "opened_at_ms": pos.opened_at_ms,
                    "matched_quantity": pos.matched_quantity,
                    "initial_quantity": pos.initial_quantity,
                    "entered_at_ms": pos.entered_at_ms,
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
                    "total_entry_fee_quote": pos.total_entry_fee_quote,
                    "entry_fee_evidence_complete": pos.entry_fee_evidence_complete,
                    "funding_edge_bps_entry": pos.funding_edge_bps_entry,
                    "total_funding_edge_bps_entry": pos.total_funding_edge_bps_entry,
                    "expected_edge_bps_entry": pos.expected_edge_bps_entry,
                    "worst_case_edge_bps_entry": pos.worst_case_edge_bps_entry,
                    "entry_cross_bps_entry": pos.entry_cross_bps_entry,
                    "fee_bps_entry": pos.fee_bps_entry,
                    "entry_slippage_bps_entry": pos.entry_slippage_bps_entry,
                    "transfer_bias_bps_entry": pos.transfer_bias_bps_entry,
                    "transfer_state_at_entry": pos.transfer_state_at_entry,
                    "entry_liquidity_source_at_entry": pos.entry_liquidity_source_at_entry,
                    "long_volume_24h_quote_at_entry": pos.long_volume_24h_quote_at_entry,
                    "short_volume_24h_quote_at_entry": pos.short_volume_24h_quote_at_entry,
                    "long_open_interest_quote_at_entry": pos.long_open_interest_quote_at_entry,
                    "short_open_interest_quote_at_entry": pos.short_open_interest_quote_at_entry,
                    "long_entry_vwap": pos.long_entry_vwap,
                    "short_entry_vwap": pos.short_entry_vwap,
                    "entry_capacity_constrained": pos.entry_capacity_constrained,
                    "entry_target_quantity": pos.entry_target_quantity,
                    "long_max_executable_quantity": pos.long_max_executable_quantity,
                    "short_max_executable_quantity": pos.short_max_executable_quantity,
                    "entry_max_executable_quantity": pos.entry_max_executable_quantity,
                    "entry_depth_shortfall_quantity": pos.entry_depth_shortfall_quantity,
                    "entry_max_executable_notional_quote": pos.entry_max_executable_notional_quote,
                    "entry_depth_capped_at_entry": pos.entry_depth_capped_at_entry,
                    "advisories": pos.advisories,
                    "blocked_reasons": pos.blocked_reasons,
                    "entry_quality_completed_at_ms": pos.entry_quality_completed_at_ms,
                    "entry_quality_markout_5s_emitted": pos.entry_quality_markout_5s_emitted,
                    "entry_quality_markout_30s_emitted": pos.entry_quality_markout_30s_emitted,
                    "settlement_half_closed_quantity": pos.settlement_half_closed_quantity,
                    "settlement_half_closed_at_ms": pos.settlement_half_closed_at_ms,
                    "exit_reason": pos.exit_reason,
                    "risk_delever_step_count": pos.risk_delever_step_count,
                    "last_risk_reason": pos.last_risk_reason,
                    "single_side_protection_triggered": pos.single_side_protection_triggered,
                    "funding_timestamp_ms": pos.funding_timestamp_ms,
                    "long_funding_timestamp_ms": pos.long_funding_timestamp_ms,
                    "short_funding_timestamp_ms": pos.short_funding_timestamp_ms,
                    "exit_after_first_stage": pos.exit_after_first_stage,
                    "opportunity_type": pos.opportunity_type,
                    "first_funding_leg": pos.first_funding_leg,
                    "second_stage_enabled_at_entry": pos.second_stage_enabled_at_entry,
                    "second_funding_timestamp_ms": pos.second_funding_timestamp_ms,
                    "second_stage_funding_captured": pos.second_stage_funding_captured,
                    "second_stage_funding_quote": pos.second_stage_funding_quote,
                    "entry_maker_leg": pos.entry_maker_leg,
                    "exit_maker_leg": pos.exit_maker_leg,
                }
                for pid, pos in self.open_positions.items()
            },
            "pending_entries": {
                pid: {
                    "pending_id": p.pending_id,
                    "symbol": p.symbol,
                    "long_venue": p.long_venue.value,
                    "short_venue": p.short_venue.value,
                    "target_quantity": p.target_quantity,
                    "long_side": p.long_side.value,
                    "short_side": p.short_side.value,
                    "created_at_ms": p.created_at_ms,
                    "metadata": p.metadata,
                    "maker_order_id": p.maker_order_id,
                    "hedge_order_id": p.hedge_order_id,
                    "maker_client_order_id": p.maker_client_order_id,
                    "hedge_client_order_id": p.hedge_client_order_id,
                    "maker_leg_filled": p.maker_leg_filled,
                    "hedge_leg_filled": p.hedge_leg_filled,
                    "maker_fee_quote": p.maker_fee_quote,
                    "hedge_fee_quote": p.hedge_fee_quote,
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
                    "hedge_attempt_count": p.hedge_attempt_count,
                    "repair_state": p.repair_state,
                    "long_quantity": p.long_quantity,
                    "short_quantity": p.short_quantity,
                    "run_id": p.run_id,
                    "entry_route": p.entry_route,
                    "maker_leg": p.maker_leg,
                    "outcome": p.outcome,
                    "opportunity_type": p.opportunity_type,
                    "funding_timestamp_ms": p.funding_timestamp_ms,
                    "first_funding_timestamp_ms": p.first_funding_timestamp_ms,
                    "long_funding_timestamp_ms": p.long_funding_timestamp_ms,
                    "short_funding_timestamp_ms": p.short_funding_timestamp_ms,
                    "second_funding_timestamp_ms": p.second_funding_timestamp_ms,
                    "first_funding_leg": p.first_funding_leg,
                    "funding_edge_bps_entry": p.funding_edge_bps_entry,
                    "total_funding_edge_bps_entry": p.total_funding_edge_bps_entry,
                    "expected_edge_bps_entry": p.expected_edge_bps_entry,
                    "worst_case_edge_bps_entry": p.worst_case_edge_bps_entry,
                    "entry_maker_leg": p.entry_maker_leg,
                    "exit_maker_leg": p.exit_maker_leg,
                    "entry_cross_bps_entry": p.entry_cross_bps_entry,
                    "fee_bps_entry": p.fee_bps_entry,
                    "entry_slippage_bps_entry": p.entry_slippage_bps_entry,
                    "transfer_bias_bps_entry": p.transfer_bias_bps_entry,
                    "transfer_state_at_entry": p.transfer_state_at_entry,
                    "entry_liquidity_source_at_entry": p.entry_liquidity_source_at_entry,
                    "long_volume_24h_quote_at_entry": p.long_volume_24h_quote_at_entry,
                    "short_volume_24h_quote_at_entry": p.short_volume_24h_quote_at_entry,
                    "long_open_interest_quote_at_entry": p.long_open_interest_quote_at_entry,
                    "short_open_interest_quote_at_entry": p.short_open_interest_quote_at_entry,
                    "long_entry_vwap": p.long_entry_vwap,
                    "short_entry_vwap": p.short_entry_vwap,
                    "entry_capacity_constrained": p.entry_capacity_constrained,
                    "entry_target_quantity": p.entry_target_quantity,
                    "long_max_executable_quantity": p.long_max_executable_quantity,
                    "short_max_executable_quantity": p.short_max_executable_quantity,
                    "entry_max_executable_quantity": p.entry_max_executable_quantity,
                    "entry_depth_shortfall_quantity": p.entry_depth_shortfall_quantity,
                    "entry_max_executable_notional_quote": p.entry_max_executable_notional_quote,
                    "entry_depth_capped_at_entry": p.entry_depth_capped_at_entry,
                    "advisories": p.advisories,
                    "blocked_reasons": p.blocked_reasons,
                    "exit_after_first_stage": p.exit_after_first_stage,
                    "phase_state": p.phase_state.to_dict() if p.phase_state else None,
                    "passive_manager_runtime": (
                        p.passive_manager_runtime.to_dict()
                        if hasattr(p.passive_manager_runtime, "to_dict")
                        else {}
                    ),
                    "created_cycle": p.created_cycle,
                    "repost_attempt_count": p.repost_attempt_count,
                    "repost_count": p.repost_count,
                    "passive_attempt_count": p.passive_attempt_count,
                    "passive_ops_total": p.passive_ops_total,
                    "maker_remainder_slices": [
                        item.to_dict() if hasattr(item, "to_dict") else dict(item)
                        for item in p.maker_remainder_slices
                    ],
                    "lifetime_exhausted_logged_final_reason": (
                        p.lifetime_exhausted_logged_final_reason
                    ),
                    "frozen_candidate": p.frozen_candidate,
                    "passive_order": p.passive_order.to_dict() if p.passive_order else None,
                    "next_progress_poll_ms": p.next_progress_poll_ms,
                    "zero_fill_since_ms": p.zero_fill_since_ms,
                }
                for pid, p in self.pending_entries.items()
            },
            "pending_closes": {
                cid: {
                    "close_id": c.close_id,
                    "position_id": c.position_id,
                    "reason": c.reason,
                    "created_at_ms": c.created_at_ms,
                    "long_order_id": c.long_order_id,
                    "short_order_id": c.short_order_id,
                    "long_closed": c.long_closed,
                    "short_closed": c.short_closed,
                    "long_uncertain": c.long_uncertain,
                    "short_uncertain": c.short_uncertain,
                }
                for cid, c in self.pending_closes.items()
            },
            "pending_passive_closes": {
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
                        "maker_submit_attempt": ppc.phase_state.maker_submit_attempt,
                        "maker_order_id": ppc.phase_state.maker_order_id,
                        "maker_client_order_id": ppc.phase_state.maker_client_order_id,
                        "maker_resting_limit_price": ppc.phase_state.maker_resting_limit_price,
                        "maker_resting_since_ms": ppc.phase_state.maker_resting_since_ms,
                    },
                    "maker_fill": {
                        "quantity": ppc.maker_fill.quantity,
                        "average_price": ppc.maker_fill.average_price,
                        "fee_quote": ppc.maker_fill.fee_quote,
                        "fee_evidence_complete": ppc.maker_fill.fee_evidence_complete,
                        "last_fill_time_ms": ppc.maker_fill.last_fill_time_ms,
                        "order_id": ppc.maker_fill.order_id,
                        "client_order_id": ppc.maker_fill.client_order_id,
                    },
                    "hedge_fill": {
                        "quantity": ppc.hedge_fill.quantity,
                        "average_price": ppc.hedge_fill.average_price,
                        "fee_quote": ppc.hedge_fill.fee_quote,
                        "fee_evidence_complete": ppc.hedge_fill.fee_evidence_complete,
                        "last_fill_time_ms": ppc.hedge_fill.last_fill_time_ms,
                        "order_id": ppc.hedge_fill.order_id,
                        "client_order_id": ppc.hedge_fill.client_order_id,
                    },
                    "next_retry_at_ms": ppc.next_retry_at_ms,
                    "multi_phase_started_at_ms": ppc.multi_phase_started_at_ms,
                    "created_cycle": ppc.created_cycle,
                }
                for pid, ppc in self.pending_passive_closes.items()
            },
        }
