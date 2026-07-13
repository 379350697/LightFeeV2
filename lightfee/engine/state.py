"""Engine state models and open position tracking matching Rust EngineState."""

from __future__ import annotations

import math

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from lightfee.core.domain import OrderFill, PassiveOrderState, Side, Venue
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


@dataclass(frozen=True)
class FundingSettlementRecord:
    """An allocated exchange-statement funding settlement.

    ``captured_funding_quote`` remains the V1 lifecycle estimate used to decide
    when a position may close.  This record is deliberately separate: only an
    exchange statement that is allocated to this internal position can make
    realised funding official.
    """

    leg: str
    venue: str
    settlement_timestamp_ms: int
    amount_quote: float
    observed_at_ms: int
    source: str
    statement_reference: str = ""

    def __post_init__(self) -> None:
        if self.leg not in {"long", "short"}:
            raise ValueError("funding settlement leg must be long or short")
        if not self.venue:
            raise ValueError("funding settlement venue is required")
        if self.settlement_timestamp_ms <= 0 or self.observed_at_ms <= 0:
            raise ValueError("funding settlement timestamps must be positive")
        if not math.isfinite(self.amount_quote):
            raise ValueError("funding settlement amount must be finite")
        if not self.source:
            raise ValueError("funding settlement source is required")

    def to_dict(self) -> dict[str, object]:
        return {
            "leg": self.leg,
            "venue": self.venue,
            "settlement_timestamp_ms": self.settlement_timestamp_ms,
            "amount_quote": self.amount_quote,
            "observed_at_ms": self.observed_at_ms,
            "source": self.source,
            "statement_reference": self.statement_reference,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> "FundingSettlementRecord":
        return cls(
            leg=str(data.get("leg", "")),
            venue=str(data.get("venue", "")),
            settlement_timestamp_ms=int(data.get("settlement_timestamp_ms", 0) or 0),
            amount_quote=float(data.get("amount_quote", 0.0) or 0.0),
            observed_at_ms=int(data.get("observed_at_ms", 0) or 0),
            source=str(data.get("source", "")),
            statement_reference=str(data.get("statement_reference", "") or ""),
        )


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
    # --- Actual funding attribution (separate from V1 lifecycle estimate) ---
    calculation_version: str = "v1_exact"
    model_epoch: str = "v1_exact"
    # The source-market timestamp for the entry economics must survive until
    # terminal attribution; it is not interchangeable with execution time.
    economics_observed_at_ms: int = 0
    funding_settlement_records: list[FundingSettlementRecord] = field(default_factory=list)
    settled_funding_quote: float = 0.0
    funding_settlement_evidence_status: str = "missing"
    funding_forecast_error_quote: float | None = None
    # --- Edge breakdowns (V1 funding_edge_bps_entry, total_funding_edge_bps_entry, expected_edge_bps_entry) ---
    funding_edge_bps_entry: float = 0.0
    total_funding_edge_bps_entry: float = 0.0
    expected_edge_bps_entry: float = 0.0
    worst_case_edge_bps_entry: float = 0.0
    expected_shortfall_bps_entry: float = 0.0
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
    expected_shortfall_bps_entry: float = 0.0
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
    # --- V1 entry timestamp contract ---
    # V1 OpenPosition.entered_at_ms = max(maker_fill.filled_at_ms, hedge_fill.filled_at_ms).
    # These are leg fill completion observations; opened_at_ms remains local finalization time.
    maker_leg_filled_at_ms: int = 0
    hedge_leg_filled_at_ms: int = 0
    maker_fill_timestamp_quality: str = ""
    hedge_fill_timestamp_quality: str = ""

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

    @staticmethod
    def _fill_timestamp_quality_rank(quality: str) -> int:
        quality = str(quality or "")
        if quality == "exchange_fill_exact":
            return 3
        if quality == "live_truth_observed":
            return 2
        if quality == "observed":
            return 2
        if quality:
            return 1
        return 0

    def _note_leg_fill_timestamp(
        self,
        *,
        leg: str,
        filled_at_ms: int,
        quality: str,
    ) -> None:
        ts_ms = int(filled_at_ms or 0)
        if ts_ms <= 0:
            return
        quality = str(quality or "observed")
        if leg == "maker":
            current_ts = int(self.maker_leg_filled_at_ms or 0)
            current_quality = str(self.maker_fill_timestamp_quality or "")
            current_rank = self._fill_timestamp_quality_rank(current_quality)
            new_rank = self._fill_timestamp_quality_rank(quality)
            if current_ts <= 0 or new_rank > current_rank or (
                new_rank == current_rank and ts_ms > current_ts
            ):
                self.maker_leg_filled_at_ms = ts_ms
                self.maker_fill_timestamp_quality = quality
            return
        current_ts = int(self.hedge_leg_filled_at_ms or 0)
        current_quality = str(self.hedge_fill_timestamp_quality or "")
        current_rank = self._fill_timestamp_quality_rank(current_quality)
        new_rank = self._fill_timestamp_quality_rank(quality)
        if current_ts <= 0 or new_rank > current_rank or (
            new_rank == current_rank and ts_ms > current_ts
        ):
            self.hedge_leg_filled_at_ms = ts_ms
            self.hedge_fill_timestamp_quality = quality

    def note_maker_fill_observed(
        self,
        filled_at_ms: int,
        *,
        quality: str = "observed",
    ) -> None:
        self._note_leg_fill_timestamp(
            leg="maker",
            filled_at_ms=filled_at_ms,
            quality=quality,
        )

    def note_hedge_fill_observed(
        self,
        filled_at_ms: int,
        *,
        quality: str = "observed",
    ) -> None:
        self._note_leg_fill_timestamp(
            leg="hedge",
            filled_at_ms=filled_at_ms,
            quality=quality,
        )

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
    maker_viability_rejected_this_cycle: bool = False
    maker_viability_rejection_reason: str = ""
    maker_viability_rejection_decision: str = ""


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
    close_order_identity_history: list[dict[str, Any]] = field(default_factory=list)
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
    has_unpaired_live_position_recoveries: bool = False
    ambiguous_state: bool = False
    lifecycle: EngineLifecycle = EngineLifecycle.BOOTING


MAX_PENDING_CLOSE_RECONCILIATIONS = 256
MAX_PENDING_FUNDING_SETTLEMENT_RECONCILIATIONS = 256
MAX_FUNDING_SETTLEMENT_STATEMENT_CLAIMS = 8_192


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


def normalize_pending_funding_settlement_reconciliations(raw: Any) -> list[dict[str, Any]]:
    """Normalize accounting-only post-close funding evidence tasks.

    These tasks never represent exchange exposure and therefore must remain
    separate from V1 pending-close truth reconciliation.  A malformed task is
    retained visibly for diagnosis rather than being coerced into a funding
    zero or a trading gate.
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        items = [raw] if any(
            key in raw for key in ("position_id", "required_settlements", "kind")
        ) else list(raw.values())
    else:
        return [{
            "invalid_pending_funding_settlement_reconciliation": True,
            "reason": "invalid_container",
            "raw_type": type(raw).__name__,
            "raw_repr": repr(raw)[:240],
        }]
    normalized: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, dict):
            normalized.append(dict(item))
        else:
            normalized.append({
                "invalid_pending_funding_settlement_reconciliation": True,
                "reason": "invalid_item",
                "raw_type": type(item).__name__,
                "raw_repr": repr(item)[:240],
            })
    return normalized


def normalize_funding_settlement_statement_claim_ledger(raw: Any) -> list[dict[str, Any]]:
    """Keep only visible mapping rows for consumed private statements.

    This ledger is deliberately separate from the pending accounting queue:
    completed tasks leave that queue, but their account-level statement claim
    must remain durable so a later duplicate task cannot recognize the same
    cash flow as a second official PnL result.
    """
    if not isinstance(raw, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        owner_id = str(item.get("owner_id") or "")
        venue = str(item.get("venue") or "").lower()
        symbol = str(item.get("symbol") or "").upper()
        try:
            timestamp = int(item.get("settlement_timestamp_ms") or 0)
        except (TypeError, ValueError):
            timestamp = 0
        quote_currency = str(item.get("quote_currency") or "").upper()
        if not owner_id or not venue or not symbol or timestamp <= 0 or not quote_currency:
            continue
        normalized.append(
            {
                "owner_id": owner_id,
                "position_id": str(item.get("position_id") or owner_id),
                "leg": str(item.get("leg") or ""),
                "venue": venue,
                "symbol": symbol,
                "settlement_timestamp_ms": timestamp,
                "quote_currency": quote_currency,
                "statement_reference": str(item.get("statement_reference") or ""),
                "recorded_at_ms": int(item.get("recorded_at_ms") or 0),
            }
        )
    return normalized


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
    # --- Independent no-owner live-position recovery work ---
    unpaired_live_position_recoveries: list = field(default_factory=list)
    # --- Venue entry cooldowns (V1 venue_entry_cooldowns) ---
    venue_entry_cooldowns: dict = field(default_factory=dict)
    # --- Route abnormal terminal incidents for rolling admission cooldowns ---
    route_abnormal_terminal_incidents: dict = field(default_factory=dict)
    # --- Venue market data degradations (V1 venue_market_data_degradations) ---
    venue_market_data_degradations: dict = field(default_factory=dict)
    # --- Transfer truth outage state (V1 transfer_truth) ---
    transfer_truth: dict = field(default_factory=dict)
    # --- Entry liquidity qualification records (V1 entry_liquidity_qualification_records) ---
    entry_liquidity_qualification_records: list = field(default_factory=list)
    # --- Pending close reconciliations (V1 pending_close_reconciliations) ---
    pending_close_reconciliations: list[dict[str, Any]] = field(default_factory=list)
    # --- Accounting-only private funding statement evidence after a close ---
    pending_funding_settlement_reconciliations: list[dict[str, Any]] = field(
        default_factory=list
    )
    # Durable ownership receipts for official private funding statements.
    funding_settlement_statement_claim_ledger: list[dict[str, Any]] = field(
        default_factory=list
    )
    # --- Local-L2 state for persistence/recovery (V1 parity) ---
    retained_local_l2_books: list[dict] = field(default_factory=list)
    local_l2_books_snapshot: list[dict] = field(default_factory=list)
    local_l2_session_snapshot: list[dict] = field(default_factory=list)
    last_scan: dict | None = None
    runtime_progress: dict[str, Any] = field(default_factory=dict)
    runtime_market_data_config: dict[str, Any] = field(default_factory=dict)
    v1_lifecycle_closure: dict[str, Any] = field(default_factory=dict)
    # --- V1 PassiveOrderManager runtime state persistence ---
    # Maps entry_id -> PassiveOrderManager.runtime_dict()
    passive_order_manager_states: dict[str, dict] = field(default_factory=dict)

    def set_pending_close_reconciliations(self, raw: Any) -> None:
        self.pending_close_reconciliations = normalize_pending_close_reconciliations(raw)[
            -MAX_PENDING_CLOSE_RECONCILIATIONS:
        ]

    def set_pending_funding_settlement_reconciliations(self, raw: Any) -> None:
        self.pending_funding_settlement_reconciliations = (
            normalize_pending_funding_settlement_reconciliations(raw)[
                -MAX_PENDING_FUNDING_SETTLEMENT_RECONCILIATIONS:
            ]
        )

    def set_funding_settlement_statement_claim_ledger(self, raw: Any) -> None:
        # Never evict a consumed statement claim: doing so would eventually
        # make an old exchange statement eligible for a second official close.
        # ``record_funding_settlement_statement_claims`` enforces the capacity
        # as a fail-closed operational boundary instead of silently weakening
        # duplicate protection.
        self.funding_settlement_statement_claim_ledger = (
            normalize_funding_settlement_statement_claim_ledger(raw)
        )

    def record_funding_settlement_statement_claims(
        self,
        task: dict[str, Any],
        *,
        recorded_at_ms: int,
    ) -> bool:
        """Atomically reserve every statement target in an official task.

        Returning false preserves fail-closed accounting: callers must leave
        the task non-official rather than remove it without a durable claim.
        """
        self.set_funding_settlement_statement_claim_ledger(
            self.funding_settlement_statement_claim_ledger
        )
        owner_id = str(task.get("position_id") or "")
        symbol = str(task.get("symbol") or "").upper()
        if not owner_id or not symbol:
            return False
        records_by_target = {
            (
                str(record.get("leg") or ""),
                str(record.get("venue") or "").lower(),
                int(record.get("settlement_timestamp_ms") or 0),
            ): record
            for record in task.get("funding_settlement_records", []) or []
            if isinstance(record, dict)
        }
        pending_rows: list[dict[str, Any]] = []
        existing_by_key = {
            (
                str(row.get("venue") or "").lower(),
                str(row.get("symbol") or "").upper(),
                int(row.get("settlement_timestamp_ms") or 0),
            ): row
            for row in self.funding_settlement_statement_claim_ledger
        }
        for required in task.get("required_settlements", []) or []:
            if not isinstance(required, dict):
                return False
            leg = str(required.get("leg") or "")
            venue = str(required.get("venue") or "").lower()
            try:
                timestamp = int(required.get("settlement_timestamp_ms") or 0)
            except (TypeError, ValueError):
                return False
            quote_currency = str(required.get("quote_currency") or "").upper()
            if leg not in {"long", "short"} or not venue or timestamp <= 0 or not quote_currency:
                return False
            key = (venue, symbol, timestamp)
            existing = existing_by_key.get(key)
            if existing is not None:
                # Even the same position may not mint a second accounting
                # receipt for a consumed account-level statement.
                return False
            record = records_by_target.get((leg, venue, timestamp), {})
            pending_rows.append(
                {
                    "owner_id": owner_id,
                    "position_id": owner_id,
                    "leg": leg,
                    "venue": venue,
                    "symbol": symbol,
                    "settlement_timestamp_ms": timestamp,
                    "quote_currency": quote_currency,
                    "statement_reference": str(record.get("statement_reference") or ""),
                    "recorded_at_ms": int(recorded_at_ms),
                }
            )
        if not pending_rows or (
            len(self.funding_settlement_statement_claim_ledger) + len(pending_rows)
            > MAX_FUNDING_SETTLEMENT_STATEMENT_CLAIMS
        ):
            return False
        self.funding_settlement_statement_claim_ledger.extend(pending_rows)
        return True

    def funding_settlement_statement_claims_for_task(
        self,
        task: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Return the exact durable claims reserved by one official task.

        The critical settlement receipt carries these rows so replay can
        reconstruct duplicate protection after a crash before the next state
        snapshot.  Returning no partial result keeps receipt creation
        fail-closed if in-memory reservation and ledger state diverge.
        """
        owner_id = str(task.get("position_id") or "")
        symbol = str(task.get("symbol") or "").upper()
        if not owner_id or not symbol:
            return []
        ledger_by_key = {
            (
                str(row.get("venue") or "").lower(),
                str(row.get("symbol") or "").upper(),
                int(row.get("settlement_timestamp_ms") or 0),
            ): row
            for row in self.funding_settlement_statement_claim_ledger
            if isinstance(row, dict)
        }
        claims: list[dict[str, Any]] = []
        for required in task.get("required_settlements", []) or []:
            if not isinstance(required, dict):
                return []
            venue = str(required.get("venue") or "").lower()
            try:
                timestamp = int(required.get("settlement_timestamp_ms") or 0)
            except (TypeError, ValueError):
                return []
            row = ledger_by_key.get((venue, symbol, timestamp))
            if (
                row is None
                or str(row.get("owner_id") or "") != owner_id
                or not venue
                or timestamp <= 0
            ):
                return []
            claims.append(dict(row))
        return claims

    def replay_funding_settlement_reconciled_receipt(
        self,
        payload: dict[str, Any],
    ) -> None:
        """Apply a durable settlement receipt without reissuing accounting.

        A journal receipt is the crash-safe commit point for post-close funding
        attribution.  Replaying its claim rows restores statement ownership
        before removing the matching pending task, making repeated replays
        idempotent and preventing a stale snapshot from minting a second PnL
        receipt.  Pre-v3 receipts have no claim rows; their task is still
        removed by durable identity for backward-compatible no-reissue
        behavior, while future receipts always include the exact claims.
        """
        owner_id = str(payload.get("position_id") or "")
        try:
            closed_at_ms = int(payload.get("closed_at_ms") or 0)
        except (TypeError, ValueError):
            return
        if not owner_id or closed_at_ms <= 0:
            return

        raw_claims = payload.get("statement_claims")
        claims = normalize_funding_settlement_statement_claim_ledger(raw_claims)
        if isinstance(raw_claims, list) and claims:
            existing_by_key = {
                (
                    str(row.get("venue") or "").lower(),
                    str(row.get("symbol") or "").upper(),
                    int(row.get("settlement_timestamp_ms") or 0),
                ): row
                for row in self.funding_settlement_statement_claim_ledger
                if isinstance(row, dict)
            }
            replayable_claims = [
                claim
                for claim in claims
                if (
                    (existing := existing_by_key.get(
                        (
                            claim["venue"],
                            claim["symbol"],
                            claim["settlement_timestamp_ms"],
                        )
                    )) is None
                    or existing == claim
                )
            ]
            self.set_funding_settlement_statement_claim_ledger(
                self.funding_settlement_statement_claim_ledger + replayable_claims
            )

        self.set_pending_funding_settlement_reconciliations(
            [
                task
                for task in self.pending_funding_settlement_reconciliations
                if not (
                    str(task.get("position_id") or "") == owner_id
                    and int(task.get("closed_at_ms") or 0) == closed_at_ms
                )
            ]
        )

    def release_funding_settlement_statement_claims(
        self,
        task: dict[str, Any],
    ) -> None:
        """Undo a just-reserved statement claim when its receipt was not durable.

        A claim only becomes consumed accounting truth together with the
        corresponding critical journal receipt.  ``record_*`` refuses any
        existing target, so a successful call owns every matching row and a
        failed receipt can safely remove precisely those rows for retry.
        """
        owner_id = str(task.get("position_id") or "")
        symbol = str(task.get("symbol") or "").upper()
        if not owner_id or not symbol:
            return
        targets: set[tuple[str, int]] = set()
        for required in task.get("required_settlements", []) or []:
            if not isinstance(required, dict):
                continue
            venue = str(required.get("venue") or "").lower()
            try:
                timestamp = int(required.get("settlement_timestamp_ms") or 0)
            except (TypeError, ValueError):
                continue
            if venue and timestamp > 0:
                targets.add((venue, timestamp))
        if not targets:
            return
        self.funding_settlement_statement_claim_ledger = [
            row
            for row in self.funding_settlement_statement_claim_ledger
            if not (
                str(row.get("owner_id") or "") == owner_id
                and str(row.get("symbol") or "").upper() == symbol
                and (
                    str(row.get("venue") or "").lower(),
                    int(row.get("settlement_timestamp_ms") or 0),
                )
                in targets
            )
        ]

    def enqueue_pending_funding_settlement_reconciliation(self, item: dict[str, Any]) -> None:
        """Upsert an accounting task without merging it into exposure truth."""
        self.set_pending_funding_settlement_reconciliations(
            self.pending_funding_settlement_reconciliations
        )
        key = (
            str(item.get("position_id") or ""),
            int(item.get("closed_at_ms") or 0),
        )
        for existing in self.pending_funding_settlement_reconciliations:
            if (
                str(existing.get("position_id") or ""),
                int(existing.get("closed_at_ms") or 0),
            ) != key:
                continue
            merged = dict(existing)
            for field_name, value in item.items():
                if value not in (None, "", [], {}):
                    merged[field_name] = value
            existing.clear()
            existing.update(merged)
            return
        self.pending_funding_settlement_reconciliations.append(dict(item))
        if len(self.pending_funding_settlement_reconciliations) > (
            MAX_PENDING_FUNDING_SETTLEMENT_RECONCILIATIONS
        ):
            self.pending_funding_settlement_reconciliations = (
                self.pending_funding_settlement_reconciliations[
                    -MAX_PENDING_FUNDING_SETTLEMENT_RECONCILIATIONS:
                ]
            )

    def enqueue_pending_close_reconciliation(self, item: dict[str, Any]) -> None:
        self.set_pending_close_reconciliations(self.pending_close_reconciliations)
        position_id = str(item.get("position_id") or "")
        kind = str(item.get("kind") or "final")

        def _row_key(row: Any, *, default_leg: str = "") -> tuple[str, str, str, str]:
            if not isinstance(row, dict):
                return "", "", "", ""
            leg = str(row.get("leg") or default_leg or "")
            venue = str(row.get("venue") or "").lower()
            order_id = str(row.get("order_id") or "")
            client_order_id = str(row.get("client_order_id") or "")
            return leg, venue, order_id, client_order_id

        def _merge_dict_rows(base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
            merged = dict(base)
            for key, value in incoming.items():
                if value in (None, "", [], {}):
                    continue
                if merged.get(key) in (None, "", [], {}):
                    merged[key] = value
                elif key in {
                    "statement_probe_candidate",
                    "truth_gap_candidate",
                    "accepted_order_truth_gap",
                    "accounting_only_backfill",
                } and value is True:
                    merged[key] = True
            return merged

        def _merge_row_list(existing: dict[str, Any], key: str, *, default_leg: str = "") -> None:
            current = [
                dict(row)
                for row in existing.get(key, []) or []
                if isinstance(row, dict)
            ]
            by_key = {
                _row_key(row, default_leg=default_leg): idx
                for idx, row in enumerate(current)
            }
            for row in item.get(key, []) or []:
                if not isinstance(row, dict):
                    continue
                row_key = _row_key(row, default_leg=default_leg)
                if row_key == ("", "", "", ""):
                    continue
                if row_key in by_key:
                    idx = by_key[row_key]
                    current[idx] = _merge_dict_rows(current[idx], row)
                    continue
                by_key[row_key] = len(current)
                current.append(dict(row))
            if current:
                existing[key] = current

        def _int_or_zero(value: Any) -> int:
            try:
                return int(value or 0)
            except (TypeError, ValueError):
                return 0

        def _merge_existing(existing: dict[str, Any]) -> None:
            _merge_row_list(existing, "long_legs", default_leg="long")
            _merge_row_list(existing, "short_legs", default_leg="short")
            _merge_row_list(existing, "statement_probe_candidates")
            _merge_row_list(existing, "unresolved_statement_probe_candidates")

            existing_count = _int_or_zero(existing.get("truth_gap_candidate_count"))
            incoming_count = _int_or_zero(item.get("truth_gap_candidate_count"))
            statement_count = len(existing.get("statement_probe_candidates", []) or [])
            truth_gap_candidate_count = max(
                existing_count,
                incoming_count,
                statement_count,
            )
            if truth_gap_candidate_count:
                existing["truth_gap_candidate_count"] = truth_gap_candidate_count

            for field_name in ("symbol", "candidate_owner_id", "missing_leg"):
                if not existing.get(field_name) and item.get(field_name):
                    existing[field_name] = item[field_name]
            for field_name in ("long_venue", "short_venue"):
                if not existing.get(field_name) and item.get(field_name):
                    existing[field_name] = item[field_name]

            incoming_snapshot = item.get("position_snapshot")
            if isinstance(incoming_snapshot, dict):
                snapshot = existing.get("position_snapshot")
                if not isinstance(snapshot, dict):
                    snapshot = {}
                snapshot = dict(snapshot)
                for key, value in incoming_snapshot.items():
                    if not snapshot.get(key) and value:
                        snapshot[key] = value
                existing["position_snapshot"] = snapshot

            for flag in ("pending_backfill",):
                if item.get(flag) is True:
                    existing[flag] = True
            if (
                item.get("accounting_only_backfill") is True
                and existing.get("blocking_trading") is not True
            ):
                existing["accounting_only_backfill"] = True
                existing["blocking_trading"] = False
            elif item.get("blocking_trading") is False and "blocking_trading" not in existing:
                existing["blocking_trading"] = False

            for field_name in (
                "component_evidence_status",
                "last_evidence_gap_reason",
            ):
                if item.get(field_name):
                    existing[field_name] = item[field_name]
            incoming_state = str(item.get("close_reconciliation_state") or "")
            if incoming_state and existing.get("blocking_trading") is not True:
                existing["close_reconciliation_state"] = incoming_state

            current_next = _int_or_zero(existing.get("next_attempt_ms"))
            incoming_next = _int_or_zero(item.get("next_attempt_ms"))
            if incoming_next and (not current_next or incoming_next < current_next):
                existing["next_attempt_ms"] = incoming_next
            if not existing.get("closed_at_ms") and item.get("closed_at_ms"):
                existing["closed_at_ms"] = item["closed_at_ms"]

        for existing in self.pending_close_reconciliations:
            if (
                str(existing.get("position_id") or "") == position_id
                and str(existing.get("kind") or "final") == kind
            ):
                _merge_existing(existing)
                return
        self.pending_close_reconciliations.append(dict(item))
        if len(self.pending_close_reconciliations) > MAX_PENDING_CLOSE_RECONCILIATIONS:
            self.pending_close_reconciliations = self.pending_close_reconciliations[
                -MAX_PENDING_CLOSE_RECONCILIATIONS:
            ]

    def remove_pending_close_reconciliation(self, task: dict[str, Any]) -> bool:
        self.set_pending_close_reconciliations(self.pending_close_reconciliations)
        before = len(self.pending_close_reconciliations)
        target = (
            str(task.get("position_id") or ""),
            str(task.get("kind") or "final"),
            int(task.get("closed_at_ms") or 0),
        )
        self.pending_close_reconciliations = [
            item
            for item in self.pending_close_reconciliations
            if (
                str(item.get("position_id") or ""),
                str(item.get("kind") or "final"),
                int(item.get("closed_at_ms") or 0),
            )
            != target
        ]
        return len(self.pending_close_reconciliations) != before

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
            "hyperliquid_trading_disabled_reason": self.hyperliquid_trading_disabled_reason,
            "recovery_blocked_reason": self.recovery_blocked_reason,
            "recovery_blocked_at_ms": self.recovery_blocked_at_ms,
            "pending_residual_repairs": self.pending_residual_repairs,
            "live_recovery_reduce_only_pairs": self.live_recovery_reduce_only_pairs,
            "unpaired_live_position_recoveries": self.unpaired_live_position_recoveries,
            "venue_entry_cooldowns": self.venue_entry_cooldowns,
            "route_abnormal_terminal_incidents": self.route_abnormal_terminal_incidents,
            "venue_market_data_degradations": self.venue_market_data_degradations,
            "transfer_truth": self.transfer_truth,
            "entry_liquidity_qualification_records": self.entry_liquidity_qualification_records,
            "pending_close_reconciliations": normalize_pending_close_reconciliations(
                self.pending_close_reconciliations
            ),
            "pending_funding_settlement_reconciliations": (
                normalize_pending_funding_settlement_reconciliations(
                    self.pending_funding_settlement_reconciliations
                )
            ),
            "funding_settlement_statement_claim_ledger": (
                normalize_funding_settlement_statement_claim_ledger(
                    self.funding_settlement_statement_claim_ledger
                )
            ),
            "last_scan": self.last_scan,
            "runtime_progress": dict(self.runtime_progress or {}),
            "runtime_market_data_config": dict(self.runtime_market_data_config or {}),
            "v1_lifecycle_closure": dict(self.v1_lifecycle_closure or {}),
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
                    "calculation_version": pos.calculation_version,
                    "model_epoch": pos.model_epoch,
                    "economics_observed_at_ms": pos.economics_observed_at_ms,
                    "funding_settlement_records": [
                        record.to_dict() for record in pos.funding_settlement_records
                    ],
                    "settled_funding_quote": pos.settled_funding_quote,
                    "funding_settlement_evidence_status": (
                        pos.funding_settlement_evidence_status
                    ),
                    "funding_forecast_error_quote": pos.funding_forecast_error_quote,
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
                    "funding_edge_bps_entry": pos.funding_edge_bps_entry,
                    "total_funding_edge_bps_entry": pos.total_funding_edge_bps_entry,
                    "expected_edge_bps_entry": pos.expected_edge_bps_entry,
                    "worst_case_edge_bps_entry": pos.worst_case_edge_bps_entry,
                    "expected_shortfall_bps_entry": pos.expected_shortfall_bps_entry,
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
                    "maker_leg_filled_at_ms": p.maker_leg_filled_at_ms,
                    "hedge_leg_filled_at_ms": p.hedge_leg_filled_at_ms,
                    "maker_fill_timestamp_quality": p.maker_fill_timestamp_quality,
                    "hedge_fill_timestamp_quality": p.hedge_fill_timestamp_quality,
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
                    "expected_shortfall_bps_entry": p.expected_shortfall_bps_entry,
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
                        "maker_viability_rejected_this_cycle": (
                            ppc.phase_state.maker_viability_rejected_this_cycle
                        ),
                        "maker_viability_rejection_reason": (
                            ppc.phase_state.maker_viability_rejection_reason
                        ),
                        "maker_viability_rejection_decision": (
                            ppc.phase_state.maker_viability_rejection_decision
                        ),
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
                    "close_order_identity_history": [
                        dict(item)
                        for item in ppc.close_order_identity_history
                        if isinstance(item, dict)
                    ],
                    "next_retry_at_ms": ppc.next_retry_at_ms,
                    "multi_phase_started_at_ms": ppc.multi_phase_started_at_ms,
                    "created_cycle": ppc.created_cycle,
                }
                for pid, ppc in self.pending_passive_closes.items()
            },
        }
