"""Engine state models and open position tracking matching Rust EngineState."""

from __future__ import annotations

from dataclasses import dataclass, field

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
    # --- Fill quantities per leg ---
    maker_leg_filled: float = 0.0
    hedge_leg_filled: float = 0.0
    # --- Deadline for timeout-based fallback (Rust V1 deadline/timeout) ---
    deadline_ms: int = 0
    # --- Fallback route (Rust V1 passive_fallback / standard_taker) ---
    fallback_route: str = ""
    # --- Uncertainty flag for reconciliation (Rust V1 uncertain entry outcomes) ---
    uncertain_outcome: bool = False


@dataclass
class PendingClose:
    close_id: str
    position_id: str
    reason: str
    created_at_ms: int
    # --- Order IDs per leg (Rust V1 close order tracking) ---
    long_order_id: str = ""
    short_order_id: str = ""
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


@dataclass
class OperatorControlState:
    requested_mode: GlobalRiskMode | None = None
    pending_reconcile: bool = False


@dataclass
class RecoveryWorkSnapshot:
    has_open_positions: bool = False
    has_pending_entries: bool = False
    has_pending_closes: bool = False
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
    run_id: str = ""
    started_at_ms: int = 0
    last_tick_ms: int = 0
    tick_count: int = 0
    venue_health: dict[str, str] = field(default_factory=dict)

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
        }
