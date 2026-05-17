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
    # --- Review & origin (V1 review_id, opportunity_origin_tags, opportunity_hint_source) ---
    review_id: str | None = None
    opportunity_origin_tags: list[str] = field(default_factory=list)
    opportunity_hint_source: str | None = None
    # --- Entry fees (matched Rust V1 total_entry_fee_quote per leg) ---
    long_entry_fee_quote: float = 0.0
    short_entry_fee_quote: float = 0.0
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
    # --- Funding timing for exit capture stages ---
    funding_timestamp_ms: int = 0
    exit_after_first_stage: bool = False
    # --- Funding stage tracking (Rust V1 second_stage_*, opportunity_type) ---
    opportunity_type: str = "aligned"
    second_stage_enabled_at_entry: bool = False
    second_funding_timestamp_ms: int = 0
    second_stage_funding_captured: bool = False
    second_stage_funding_quote: float = 0.0
    # --- Transfer & liquidity (V1 transfer_state_at_entry, entry_liquidity_source_at_entry) ---
    transfer_state_at_entry: str | None = None
    entry_liquidity_source_at_entry: str | None = None
    # --- VWAP (V1 long_entry_vwap, short_entry_vwap) ---
    long_entry_vwap: float | None = None
    short_entry_vwap: float | None = None
    # --- Capacity constraints (V1 entry_capacity_constrained) ---
    entry_capacity_constrained: bool = False
    # --- Advisories & blocked reasons (V1 advisories, blocked_reasons) ---
    advisories: list[str] = field(default_factory=list)
    blocked_reasons: list[str] = field(default_factory=list)
    # --- Quality markouts (V1 entry_quality_markout_5s/30s_emitted) ---
    entry_quality_markout_5s_emitted: bool = False
    entry_quality_markout_30s_emitted: bool = False
    # --- Exit reason (V1 exit_reason) ---
    exit_reason: str | None = None
    # --- Fills (orders that created this position, for reconciliation) ---
    long_fill: OrderFill | None = None
    short_fill: OrderFill | None = None

    def __post_init__(self) -> None:
        if self.matched_quantity == 0.0:
            self.matched_quantity = min(self.long_quantity, self.short_quantity)


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
    # --- V1 maker fill price for hedge price hint ---
    maker_fill_price: float = 0.0
    # --- V1 hedge fill price for entry position recording ---
    hedge_fill_price: float = 0.0
    # --- Terminal repair state for unresolvable residuals ---
    # Values: "" (active), "hedge_residual_below_min_notional" (terminal)
    repair_state: str = ""

    def __post_init__(self) -> None:
        """Migrate legacy string hedge_inflight to HedgeInflight | None."""
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

    # --- V1 recovery helpers (CONTRACT RECOVERY-002/003) ---

    def missing_hedge_quantity(self) -> float:
        """Quantity still needed on the hedge leg.

        V1: PendingEntryHedge.missing_hedge_quantity() — the gap between
        what the maker leg has filled and what the hedge leg has filled,
        capped by the balanced (matched) quantity.
        """
        balanced = min(self.maker_leg_filled, self.target_quantity)
        return max(0.0, balanced - self.hedge_leg_filled)

    def maker_completed(self) -> bool:
        """Whether the maker leg is fully filled.

        V1: PendingEntryHedge.maker_completed() — maker leg fill >= target.
        """
        return self.maker_leg_filled >= self.target_quantity - 1e-9

    def has_any_fill(self) -> bool:
        """Whether any leg has any fill quantity."""
        return self.maker_leg_filled > 1e-9 or self.hedge_leg_filled > 1e-9

    def startup_recovery_ready(self) -> bool:
        """Whether this pending entry is ready for startup recovery.

        V1: PendingEntryHedge.startup_recovery_ready() —
        true when inflight_hedge exists, cancel is requested, maker is completed,
        or hedge quantity is missing > 1e-9.

        In V2, inflight_hedge maps to uncertain_outcome (an uncertain submit
        implies an order may still be in-flight). Maker completion and missing
        hedge are computed from local fill quantities.
        """
        return (
            self.uncertain_outcome
            or self.maker_completed()
            or self.missing_hedge_quantity() > 1e-9
            or self.hedge_inflight is not None
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
    # --- Recovery blocked state (V1 recovery_blocked_reason, recovery_blocked_at_ms) ---
    recovery_blocked_reason: str | None = None
    recovery_blocked_at_ms: int = 0
    # --- Global risk reason (V1 global_risk_reason) ---
    global_risk_reason: str | None = None
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
    pending_close_reconciliations: list = field(default_factory=list)
    # --- Local-L2 state for persistence/recovery (V1 parity) ---
    retained_local_l2_books: list[dict] = field(default_factory=list)
    local_l2_books_snapshot: list[dict] = field(default_factory=list)
    local_l2_session_snapshot: list[dict] = field(default_factory=list)
    last_scan: dict | None = None

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
            "global_risk_reason": self.global_risk_reason,
            "recovery_blocked_reason": self.recovery_blocked_reason,
            "recovery_blocked_at_ms": self.recovery_blocked_at_ms,
            "pending_residual_repairs": self.pending_residual_repairs,
            "live_recovery_reduce_only_pairs": self.live_recovery_reduce_only_pairs,
            "venue_entry_cooldowns": self.venue_entry_cooldowns,
            "venue_market_data_degradations": self.venue_market_data_degradations,
            "transfer_truth": self.transfer_truth,
            "entry_liquidity_qualification_records": self.entry_liquidity_qualification_records,
            "pending_close_reconciliations": self.pending_close_reconciliations,
            "last_scan": self.last_scan,
            "retained_local_l2_books": self.retained_local_l2_books,
            "local_l2_books_snapshot": self.local_l2_books_snapshot,
            "local_l2_session_snapshot": self.local_l2_session_snapshot,
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
                    "maker_client_order_id": p.maker_client_order_id,
                    "hedge_client_order_id": p.hedge_client_order_id,
                    "maker_leg_filled": p.maker_leg_filled,
                    "hedge_leg_filled": p.hedge_leg_filled,
                    "deadline_ms": p.deadline_ms,
                    "uncertain_outcome": p.uncertain_outcome,
                    "reconcile_attempt": p.reconcile_attempt,
                    "reconcile_next_attempt_ms": p.reconcile_next_attempt_ms,
                    "entry_type": p.entry_type,
                    "maker_price": p.maker_price,
                    "maker_fill_price": p.maker_fill_price,
                    "hedge_fill_price": p.hedge_fill_price,
                    "hedge_inflight": p.hedge_inflight.to_dict() if p.hedge_inflight else "",
                    "repair_state": p.repair_state,
                    "long_quantity": p.long_quantity,
                    "short_quantity": p.short_quantity,
                    "maker_leg": p.maker_leg,
                    "outcome": p.outcome,
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
