"""Engine state models and open position tracking matching Rust EngineState."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from lightfee.core.domain import OrderFill, Side, Venue
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
    # --- Entry fees (matched Rust V1 total_entry_fee_quote per leg) ---
    long_entry_fee_quote: float = 0.0
    short_entry_fee_quote: float = 0.0
    # --- PnL attribution (matches Rust V1 realized_* fields) ---
    realized_price_pnl_quote: float = 0.0
    realized_exit_fee_quote: float = 0.0
    # --- Funding accrual (Rust V1 captured_funding_quote, funding_captured) ---
    captured_funding_quote: float = 0.0
    funding_captured: bool = False
    # --- Edge & net tracking (Rust V1 peak_net_quote, current_net_quote) ---
    peak_net_quote: float = 0.0
    current_net_quote: float = 0.0
    # --- Close deadlines (Rust V1 settlement_half_closed_*, last_risk_action_at_ms) ---
    settlement_half_closed_at_ms: int = 0
    last_risk_action_at_ms: int = 0
    # --- Risk action tracking (Rust V1 risk_delever_step_count, last_risk_reason) ---
    risk_delever_step_count: int = 0
    last_risk_reason: str | None = None
    single_side_protection_triggered: bool = False
    # --- Matched quantity = min(long_qty, short_qty) (Rust V1 matched_quantity) ---
    matched_quantity: float = 0.0
    # --- Funding timing for exit capture stages ---
    funding_timestamp_ms: int = 0
    exit_after_first_stage: bool = False
    # --- Funding stage tracking (Rust V1 second_stage_*, opportunity_type) ---
    opportunity_type: str = "aligned"
    second_stage_enabled_at_entry: bool = False
    second_funding_timestamp_ms: int = 0
    second_stage_funding_captured: bool = False
    second_stage_funding_quote: float = 0.0
    # --- Fills (orders that created this position, for reconciliation) ---
    long_fill: OrderFill | None = None
    short_fill: OrderFill | None = None

    def __post_init__(self) -> None:
        if self.matched_quantity == 0.0:
            self.matched_quantity = min(self.long_quantity, self.short_quantity)


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
    # --- Order IDs for reconciliation (Rust V1 maker/hedge order tracking) ---
    maker_order_id: str = ""
    hedge_order_id: str = ""
    # --- Client order IDs for idempotency (Rust V1 clientOrderId dedup) ---
    maker_client_order_id: str = ""
    hedge_client_order_id: str = ""
    # --- Fill quantities per leg ---
    maker_leg_filled: float = 0.0
    hedge_leg_filled: float = 0.0
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
    last_fill_time_ms: int = 0
    order_id: str = ""
    client_order_id: str = ""


@dataclass
class PersistedCloseExecutionLeg:
    """V1 PersistedCloseExecutionLeg: serializable close leg for passive close."""
    fill: Optional[OrderFill] = None
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
    ambiguous_state: bool = False
    lifecycle: EngineLifecycle = EngineLifecycle.BOOTING


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
    # --- Local-L2 state for persistence/recovery (V1 parity) ---
    retained_local_l2_books: list[dict] = field(default_factory=list)
    local_l2_books_snapshot: list[dict] = field(default_factory=list)
    local_l2_session_snapshot: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
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
            "retained_local_l2_books": self.retained_local_l2_books,
            "local_l2_books_snapshot": self.local_l2_books_snapshot,
            "local_l2_session_snapshot": self.local_l2_session_snapshot,
            "open_positions": {
                pid: {
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
                    "risk_delever_step_count": pos.risk_delever_step_count,
                    "last_risk_reason": pos.last_risk_reason,
                    "single_side_protection_triggered": pos.single_side_protection_triggered,
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
                for pid, ppc in self.pending_passive_closes.items()
            },
        }
