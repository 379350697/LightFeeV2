"""V1 passive close executor: maker+taker close with chunking, repricing, and recovery.

Rust references:
- src/engine/exit.rs: PendingPassiveClose (line 59)
- src/engine/exit.rs: start_pending_passive_close (line 1603)
- src/engine/exit.rs: drive_pending_passive_close (line 1710)
- src/engine/exit.rs: maintain_passive_close_order (line 860)
- src/engine/exit.rs: process_pending_passive_closes (line 2987)
- src/engine/entry.rs: align_passive_price_to_tick (line 4646)
"""

from __future__ import annotations

import asyncio
import math
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.errors import OrderSubmitError
from lightfee.core.exchange_errors import (
    ExchangeErrorEvidence,
    RequestContext,
    build_evidence_from_order_submit_error,
    build_fallback_evidence,
)
from lightfee.core.domain import (
    OrderFill,
    OrderRequest,
    PassiveOrderAck,
    PassiveOrderAmendRequest,
    PassiveOrderProgress,
    PassiveOrderState,
    Side,
    TimeInForce,
    Venue,
)
from lightfee.engine.close_executor import (
    CloseExecutionLeg,
    _is_bybit_duplicate_order_link_id,
    build_close_execution_from_legs,
    close_leg_exchange_min_notional_violation,
    compute_close_chunks,
)
from lightfee.venues.cid import compact_client_order_id
from lightfee.engine.exit import CloseExecution
from lightfee.engine.state import (
    ActiveMakerLeg,
    EngineState,
    OpenPosition,
    PassiveExecutionPhase,
    PassivePhaseState,
    PendingPassiveClose,
    PendingPassiveLegFill,
    PersistedCloseExecutionLeg,
)
from lightfee.persistence.journal import Journal
from lightfee.venues.common import align_passive_price_to_tick, resolve_price_tick
from lightfee.venues.capabilities import get_capability_flags
from lightfee.venues.specs import get_spec
from lightfee.venues.symbol_rules import get_symbol_rules_cache

# ---------------------------------------------------------------------------
# V1 constants
# ---------------------------------------------------------------------------

PASSIVE_CLOSE_PROGRESS_POLL_INTERVAL_MS = 10
PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS = 3_000
PASSIVE_CLOSE_SMALL_FILL_BUFFER_MS = 2_000
PASSIVE_CLOSE_SMALL_FILL_BUFFER_NOTIONAL_QUOTE = 10.0
PASSIVE_CLOSE_SMALL_FILL_BUFFER_MAX_WAIT_MS = 5_000
PASSIVE_CLOSE_MAX_ZERO_FILL_CYCLES = 3
PASSIVE_CLOSE_MAX_MANAGER_FAILURES = 3
PASSIVE_CLOSE_MAX_MAKER_SUBMIT_FAILURES = 3
PASSIVE_CLOSE_MAX_MISSING_L2_TICK_FAILURES = 3
PASSIVE_CLOSE_MANAGER_COOLDOWN_MS = 30_000
PASSIVE_CLOSE_DEFAULT_AMEND_THRESHOLD_BPS = 5.0
PASSIVE_CLOSE_DEFAULT_CANCEL_REPLACE_THRESHOLD_BPS = 20.0


# ---------------------------------------------------------------------------
# Passive close manager profile (V1: passive_close_manager_profile)
# ---------------------------------------------------------------------------


@dataclass
class PassiveCloseManagerProfile:
    working_timeout_ms: int = 60_000
    amend_threshold_bps: float = PASSIVE_CLOSE_DEFAULT_AMEND_THRESHOLD_BPS
    cancel_replace_threshold_bps: float = PASSIVE_CLOSE_DEFAULT_CANCEL_REPLACE_THRESHOLD_BPS
    max_consecutive_failures: int = PASSIVE_CLOSE_MAX_MANAGER_FAILURES
    cooldown_ms: int = PASSIVE_CLOSE_MANAGER_COOLDOWN_MS
    ops_budget_per_window: int = 10
    ops_budget_window_ms: int = 60_000


# ---------------------------------------------------------------------------
# Maintenance decision enums
# ---------------------------------------------------------------------------


class PassiveCloseMaintenanceOutcome(Enum):
    CONTINUE_POLLING = "continue_polling"
    RESTART_CYCLE = "restart_cycle"


class PassiveManagerDecisionKind(Enum):
    HOLD = "hold"
    AMEND = "amend"
    CANCEL_REPLACE = "cancel_replace"
    COOLDOWN = "cooldown"


@dataclass
class PassiveManagerDecision:
    kind: PassiveManagerDecisionKind
    new_price: Optional[float] = None
    new_quantity: Optional[float] = None
    until_ms: int = 0
    reason: str = ""


# ---------------------------------------------------------------------------
# Passive close config
# ---------------------------------------------------------------------------


@dataclass
class PassiveCloseConfig:
    max_zero_fill_cycles: int = PASSIVE_CLOSE_MAX_ZERO_FILL_CYCLES
    progress_poll_interval_ms: int = PASSIVE_CLOSE_PROGRESS_POLL_INTERVAL_MS
    progress_retry_window_ms: int = PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS
    small_fill_buffer_ms: int = PASSIVE_CLOSE_SMALL_FILL_BUFFER_MS
    small_fill_buffer_notional_quote: float = PASSIVE_CLOSE_SMALL_FILL_BUFFER_NOTIONAL_QUOTE
    small_fill_buffer_max_wait_ms: int = PASSIVE_CLOSE_SMALL_FILL_BUFFER_MAX_WAIT_MS
    maker_min_notional_accumulation_attempts: int = 5
    maker_cycle_retry_delays_ms: list[int] = field(default_factory=lambda: [500, 2_000, 5_000, 15_000])
    max_slippage_bps: Optional[float] = None
    default_tick_size: float = 0.01
    close_chunk_max_notional_quote: float = 0.0


# ---------------------------------------------------------------------------
# Hedge delta result (V1 structured hedge outcome)
# ---------------------------------------------------------------------------


@dataclass
class HedgeDeltaResult:
    """Structured result from a delta hedge submission.

    V1: hedge_result in drive_pending_passive_close (exit.rs line 2471-2591).
    """
    requested: float
    filled: float
    residual: float
    success: bool
    error: Optional[str] = None
    order_id: str = ""


# ---------------------------------------------------------------------------
# Passive close executor
# ---------------------------------------------------------------------------


def ops_token_available(
    pending: PendingPassiveClose, profile: PassiveCloseManagerProfile, now_ms: int
) -> bool:
    """V1: ops token bucket check for passive close maintenance.

    Simple fixed-window counter: resets only when the full window expires.
    Cooldown_ms determines the retry delay after budget exhaustion, NOT
    a sub-window counter reset.

    Extracted as a module-level function for testability; the class method
    _ops_token_available delegates to this.
    """
    window_ms = profile.ops_budget_window_ms
    if pending.ops_window_started_at_ms <= 0:
        return True
    elapsed = now_ms - pending.ops_window_started_at_ms
    if elapsed >= window_ms:
        # Full window expired — reset counter
        pending.ops_count_this_window = 0
        pending.ops_window_started_at_ms = now_ms
        return True
    return pending.ops_count_this_window < profile.ops_budget_per_window


class PassiveCloseExecutor:
    """V1 passive close executor: maker+taker close with GTC post-only maker leg.

    Owns the full lifecycle:
    - start: create PendingPassiveClose, plan chunks, submit first maker
    - drive: loop polling maker progress, hedging deltas, maintaining maker
    - maintain: hold/amend/cancel-replace decisions with budget gating
    - delta_hedge: submit IOC taker for newly filled maker quantity
    - chunk_advance: complete current chunk, move to next or finalize
    - finalize: build CloseExecution from accumulated legs
    - fallback: transition remaining quantity to aggressive close
    """

    def __init__(
        self,
        adapters: dict[Venue, VenueAdapter],
        journal: Journal,
        config_overrides: dict[str, Any] | None = None,
    ):
        self._adapters = adapters
        self._journal = journal
        overrides = config_overrides or {}
        self._config = PassiveCloseConfig(
            max_zero_fill_cycles=overrides.get("max_zero_fill_cycles", PASSIVE_CLOSE_MAX_ZERO_FILL_CYCLES),
            progress_poll_interval_ms=overrides.get("progress_poll_interval_ms", PASSIVE_CLOSE_PROGRESS_POLL_INTERVAL_MS),
            progress_retry_window_ms=overrides.get("progress_retry_window_ms", PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS),
            small_fill_buffer_ms=overrides.get("small_fill_buffer_ms", PASSIVE_CLOSE_SMALL_FILL_BUFFER_MS),
            small_fill_buffer_notional_quote=overrides.get("small_fill_buffer_notional_quote", PASSIVE_CLOSE_SMALL_FILL_BUFFER_NOTIONAL_QUOTE),
            small_fill_buffer_max_wait_ms=overrides.get("small_fill_buffer_max_wait_ms", PASSIVE_CLOSE_SMALL_FILL_BUFFER_MAX_WAIT_MS),
            maker_min_notional_accumulation_attempts=overrides.get("maker_min_notional_accumulation_attempts", 5),
            maker_cycle_retry_delays_ms=overrides.get("maker_cycle_retry_delays_ms", [500, 2_000, 5_000, 15_000]),
            max_slippage_bps=overrides.get("max_slippage_bps"),
            default_tick_size=overrides.get("default_tick_size", 0.01),
            close_chunk_max_notional_quote=overrides.get("close_chunk_max_notional_quote", 0.0),
        )
        # Inject L2 mid resolver for live repricing (set by runtime after construction)
        self._l2_mid_resolver: Optional[callable] = None
        # Inject L2 top-of-book resolver for V1 passive tick inference.
        self._l2_quote_resolver: Optional[callable] = None
        # Inject aggressive close executor for fallback (set by runtime after construction)
        self._close_executor: Optional[object] = None

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _adapter(self, venue: Venue) -> Optional[VenueAdapter]:
        return self._adapters.get(venue)

    def set_l2_mid_resolver(self, resolver: callable) -> None:
        self._l2_mid_resolver = resolver

    def set_l2_quote_resolver(self, resolver: callable) -> None:
        self._l2_quote_resolver = resolver

    def set_close_executor(self, executor: object) -> None:
        self._close_executor = executor

    def _profile(self, venue: Venue) -> PassiveCloseManagerProfile:
        return PassiveCloseManagerProfile()

    def _ops_token_available(
        self, pending: PendingPassiveClose, profile: PassiveCloseManagerProfile, now_ms: int
    ) -> bool:
        """V1: ops token bucket check for passive close maintenance."""
        return ops_token_available(pending, profile, now_ms)

    # ------------------------------------------------------------------
    # Start
    # ------------------------------------------------------------------

    async def start_pending_passive_close(
        self,
        state: EngineState,
        position: OpenPosition,
        reason: str,
        long_price_hint: float = 0.0,
        short_price_hint: float = 0.0,
        quantity: Optional[float] = None,
        short_stage: str = "exit_short",
        long_stage: str = "exit_long",
    ) -> Optional[PendingPassiveClose]:
        """V1 start_pending_passive_close (exit.rs line 1603).

        Creates a PendingPassiveClose record if one doesn't already exist
        for this position. Plans chunks, sets initial phase state, and
        persists the pending record.
        """
        pid = position.position_id
        if pid in state.pending_passive_closes:
            return None

        target = quantity or position.matched_quantity
        if target <= 0.0:
            return None

        chunk_quantities = compute_close_chunks(
            total_quantity=target,
            long_price_hint=long_price_hint,
            short_price_hint=short_price_hint,
            max_notional_quote=self._config.close_chunk_max_notional_quote,
            venue_long=position.long_venue,
            venue_short=position.short_venue,
        )
        if not chunk_quantities:
            return None

        first_chunk = chunk_quantities[0]
        # Choose preferred maker leg based on venue liquidity (V1: select_exit_maker_leg)
        preferred_leg = self._select_preferred_maker_leg(position)
        phase_state = PassivePhaseState(
            phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
            preferred_maker_leg=preferred_leg,
            active_maker_leg=preferred_leg,
            phase_started_at_ms=self._now_ms(),
            cycle_attempt=1,
            cycle_started_at_ms=self._now_ms(),
        )

        pending = PendingPassiveClose(
            position_id=pid,
            reason=reason,
            position_snapshot=position,
            short_stage=short_stage or "exit_short",
            long_stage=long_stage or "exit_long",
            target_quantity=target,
            max_slippage_bps=self._config.max_slippage_bps,
            chunk_quantities=chunk_quantities,
            active_chunk_index=0,
            phase_state=phase_state,
            multi_phase_started_at_ms=self._now_ms(),
            created_cycle=state.tick_count,
        )

        state.pending_passive_closes[pid] = pending
        self._journal.append(
            "exit.passive_close_created",
            {
                "position_id": pid,
                "reason": reason,
                "target_quantity": target,
                "chunk_count": len(chunk_quantities),
                "first_chunk_quantity": first_chunk,
                "phase": phase_state.phase.value,
                "active_maker_leg": phase_state.active_maker_leg.value,
            },
        )
        return pending

    # ------------------------------------------------------------------
    # Drive
    # ------------------------------------------------------------------

    async def drive_pending_passive_close(
        self,
        state: EngineState,
        position_id: str,
        wait_until_terminal: bool = False,
    ) -> bool:
        """V1 drive_pending_passive_close (exit.rs line 1710).

        Main loop for one passive close. Returns True if the close
        is complete and the pending record has been removed.
        """
        while True:
            pending = state.pending_passive_closes.get(position_id)
            if pending is None:
                return True  # already resolved

            now_ms = self._now_ms()
            if not wait_until_terminal and now_ms < pending.next_retry_at_ms:
                return False  # not ready yet

            position = pending.position_snapshot
            if position is None:
                # Restore from open_positions if possible
                position = state.open_positions.get(position_id)
                if position is None:
                    state.pending_passive_closes.pop(position_id, None)
                    self._journal.append(
                        "exit.passive_close_orphaned",
                        {"position_id": position_id},
                    )
                    return True

            chunk_quantity = pending.current_chunk_quantity()
            if chunk_quantity <= 0.0:
                return await self._finalize_passive_close(state, pending)

            if pending.phase_state.phase == PassiveExecutionPhase.DUAL_TAKER:
                self._journal.append(
                    "exit.passive_close_dual_taker_drive",
                    {"position_id": position_id},
                )
                return await self._fallback_to_aggressive_close(state, pending, position)

            # Determine maker leg metadata
            maker_leg = pending.phase_state.active_maker_leg
            if maker_leg == ActiveMakerLeg.LONG:
                maker_venue = position.long_venue
                maker_side = Side.SELL  # closing long = sell
                maker_leg_label = "long"
                maker_price_hint = self._resolve_local_l2_mid(position.long_venue, position.symbol)
            else:
                maker_venue = position.short_venue
                maker_side = Side.BUY  # closing short = buy
                maker_leg_label = "short"
                maker_price_hint = self._resolve_local_l2_mid(position.short_venue, position.symbol)

            adapter = self._adapter(maker_venue)
            if adapter is None:
                pending.next_retry_at_ms = now_ms + 5_000
                self._journal.append(
                    "exit.passive_close_no_adapter",
                    {"position_id": position_id, "maker_venue": maker_venue.value},
                )
                return False

            cycle_fill_before = pending.maker_fill.quantity

            # --- Poll maker progress ---
            maker_order_id = pending.phase_state.maker_order_id
            maker_client_id = pending.phase_state.maker_client_order_id

            if not maker_order_id:
                # Submit initial maker order
                success = await self._submit_maker_order(state, pending, position,
                                                          maker_venue, maker_side,
                                                          maker_leg_label, maker_price_hint,
                                                          chunk_quantity)
                if not success:
                    return False
                pending = state.pending_passive_closes.get(position_id)
                if pending is None:
                    return True
                maker_order_id = pending.phase_state.maker_order_id
                maker_client_id = pending.phase_state.maker_client_order_id

            # Poll progress
            progress = await self._poll_maker_progress(
                adapter, position.symbol, maker_order_id, maker_client_id,
                side=maker_side,
            )

            # Apply progress to pending state
            if progress is not None:
                pending = state.pending_passive_closes.get(position_id)
                if pending is None:
                    return True
                self._apply_maker_progress(pending, progress, now_ms)

                # Terminal passive states → handle
                if progress.state in (PassiveOrderState.FILLED, PassiveOrderState.CANCELED,
                                       PassiveOrderState.EXPIRED, PassiveOrderState.REJECTED):
                    if progress.state == PassiveOrderState.FILLED:
                        # Maker fully filled — hedge all outstanding quantity
                        # V1: hedges outstanding_hedge_quantity = maker_fill - hedged_close_quantity,
                        # not just the cycle delta. On retry, if maker hasn't changed but hedge
                        # is still behind, we submit a hedge for the remaining gap.
                        maker_fill_delta = pending.maker_fill.quantity - cycle_fill_before
                        unhedged_gap = pending.maker_fill.quantity - pending.hedge_fill.quantity
                        if unhedged_gap > 1e-9:
                            result = await self._submit_hedge_for_delta(state, pending, position, unhedged_gap)
                        elif maker_fill_delta > 1e-9:
                            result = await self._submit_hedge_for_delta(state, pending, position, maker_fill_delta)
                        else:
                            result = HedgeDeltaResult(requested=0.0, filled=0.0, residual=0.0, success=True)
                        # Re-read pending after hedge
                        pending = state.pending_passive_closes.get(position_id)
                        if pending is None:
                            return True
                        # Non-retryable hedge error → escalate to aggressive close
                        if not result.success and self._is_non_retryable_hedge_error(result.error or ""):
                            self._journal.append(
                                "exit.passive_close_hedge_non_retryable_escalated",
                                {
                                    "position_id": position_id,
                                    "hedge_error": result.error,
                                    "unhedged_gap": pending.maker_fill.quantity - pending.hedge_fill.quantity,
                                    "reason": "escalating to aggressive close after non-retryable hedge error",
                                },
                            )
                            pending.phase_state.phase = PassiveExecutionPhase.DUAL_TAKER
                            return False
                        # Chunk advance invariant: hedge must have caught up to maker fill
                        if pending.hedge_fill.quantity + 1e-9 >= pending.maker_fill.quantity:
                            if await self._advance_chunk(state, pending):
                                continue
                            # advance blocked — will retry
                            return False
                        # Hedge not caught up — record unhedged residual, retry
                        unhedged = pending.maker_fill.quantity - pending.hedge_fill.quantity
                        self._journal.append(
                            "exit.passive_close_unhedged_residual",
                            {
                                "position_id": position_id,
                                "maker_quantity": pending.maker_fill.quantity,
                                "hedge_quantity": pending.hedge_fill.quantity,
                                "unhedged_residual": unhedged,
                                "chunk_index": pending.active_chunk_index,
                            },
                        )
                        pending.next_retry_at_ms = now_ms + PASSIVE_CLOSE_PROGRESS_POLL_INTERVAL_MS
                        return False
                    else:
                        # Terminal non-filled maker orders must not be polled forever.
                        # Clear the stale order and fall through to the normal zero-fill
                        # phase budget so recovery can switch phase or arm DUAL_TAKER.
                        self._journal.append(
                            "exit.passive_close_maker_terminal_no_fill",
                            {
                                "position_id": position_id,
                                "maker_order_id": progress.order_id or maker_order_id,
                                "maker_client_order_id": progress.client_order_id or maker_client_id,
                                "state": progress.state.value,
                                "cumulative_quantity": progress.cumulative_quantity,
                                "phase": pending.phase_state.phase.value,
                                "zero_fill_cycles": pending.phase_state.zero_fill_cycles_in_phase,
                            },
                        )
                        pending.phase_state.maker_order_id = ""
                        pending.phase_state.maker_client_order_id = ""
                        pending.phase_state.maker_resting_limit_price = None
                        pending.phase_state.maker_resting_since_ms = 0

            # --- Delta hedge: hedge outstanding gap between maker and hedge ---
            # V1: hedges unhedged_gap, not just maker_fill_delta, so that
            # partial hedge fills from prior cycles are retried even when
            # maker_fill_delta == 0 this cycle.
            pending = state.pending_passive_closes.get(position_id)
            if pending is None:
                return True

            unhedged_gap = pending.maker_fill.quantity - pending.hedge_fill.quantity
            if unhedged_gap > 1e-9:
                # --- V1 small-fill buffer: avoid submitting hedge below min-notional ---
                # Compute hedge price hint for notional check
                if maker_leg == ActiveMakerLeg.LONG:
                    hedge_venue_for_notional = position.short_venue
                else:
                    hedge_venue_for_notional = position.long_venue
                hedge_price_hint = self._resolve_local_l2_mid(hedge_venue_for_notional, position.symbol)
                if hedge_price_hint <= 0.0:
                    hedge_price_hint = pending.maker_fill.average_price
                buffered_notional = unhedged_gap * max(hedge_price_hint, 0.0)

                # Check if maker can still accumulate (not in terminal state)
                maker_terminal = progress is not None and progress.state in (
                    PassiveOrderState.FILLED,
                    PassiveOrderState.CANCELED,
                    PassiveOrderState.REJECTED,
                    PassiveOrderState.EXPIRED,
                ) if progress is not None else False
                can_accumulate = not maker_terminal

                # Resolve buffer start timestamp
                buffer_start = pending.small_fill_buffer_started_at_ms
                if buffer_start is None:
                    if pending.maker_fill.last_fill_time_ms > 0:
                        buffer_start = pending.maker_fill.last_fill_time_ms

                decision = self._small_fill_buffer_decision(
                    buffered_notional_quote=buffered_notional,
                    buffer_notional_quote=self._config.small_fill_buffer_notional_quote,
                    buffer_wait_ms=self._config.small_fill_buffer_max_wait_ms,
                    buffer_started_at_ms=buffer_start,
                    now_ms=now_ms,
                    can_accumulate_small_fill=can_accumulate,
                )

                if decision["should_buffer"]:
                    pending.small_fill_buffer_started_at_ms = buffer_start or now_ms
                    retry_wait_ms = min(
                        PASSIVE_CLOSE_PROGRESS_POLL_INTERVAL_MS,
                        decision["remaining_wait_ms"],
                    )
                    pending.next_retry_at_ms = now_ms + max(retry_wait_ms, 1)
                    self._journal.append(
                        "exit.passive_close_small_fill_buffering",
                        {
                            "position_id": position_id,
                            "hedge_venue": hedge_venue_for_notional.value,
                            "missing_hedge_quantity": unhedged_gap,
                            "buffered_notional_quote": buffered_notional,
                            "buffered_elapsed_ms": decision["buffered_elapsed_ms"],
                            "buffer_wait_ms": self._config.small_fill_buffer_max_wait_ms,
                            "buffer_threshold_quote": self._config.small_fill_buffer_notional_quote,
                            "retry_wait_ms": retry_wait_ms,
                        },
                    )
                    if wait_until_terminal:
                        await asyncio.sleep(retry_wait_ms / 1000.0)
                        continue
                    return False

                if decision["wait_expired"]:
                    self._journal.append(
                        "exit.passive_close_small_fill_buffer_expired",
                        {
                            "position_id": position_id,
                            "hedge_venue": hedge_venue_for_notional.value,
                            "missing_hedge_quantity": unhedged_gap,
                            "buffered_notional_quote": buffered_notional,
                            "buffered_elapsed_ms": decision["buffered_elapsed_ms"],
                            "buffer_wait_ms": self._config.small_fill_buffer_max_wait_ms,
                            "buffer_threshold_quote": self._config.small_fill_buffer_notional_quote,
                        },
                    )
                else:
                    pending.small_fill_buffer_started_at_ms = None

                # --- V1: pre-submit min-notional check ---
                # V1 exit.rs:2297-2348 — normalize quantity, check min_notional_violation
                # BEFORE submitting the hedge. If below min_notional and maker can still
                # accumulate, track attempt without submitting.
                min_notional_violation = self._check_hedge_min_notional(
                    hedge_venue_for_notional, position.symbol,
                    Side.BUY if maker_leg == ActiveMakerLeg.LONG else Side.SELL,
                    unhedged_gap, hedge_price_hint,
                )
                if min_notional_violation is not None and can_accumulate:
                    # Hedge is below min notional and maker may still accumulate →
                    # do NOT submit a failing hedge. Track accumulation instead.
                    if unhedged_gap > pending.last_small_fill_missing_quantity + 1e-9:
                        pending.small_fill_min_notional_attempts += 1
                        pending.last_small_fill_missing_quantity = unhedged_gap
                    self._journal.append(
                        "exit.passive_close_min_notional_accumulating",
                        {
                            "position_id": position_id,
                            "attempt": pending.small_fill_min_notional_attempts,
                            "max_attempts": self._config.maker_min_notional_accumulation_attempts,
                            "missing_hedge_quantity": unhedged_gap,
                            "leg_notional_quote": min_notional_violation["leg_notional"],
                            "venue_min_notional_quote": min_notional_violation["min_notional"],
                            "maker_quantity": pending.maker_fill.quantity,
                            "hedge_quantity": pending.hedge_fill.quantity,
                        },
                    )
                    if pending.small_fill_min_notional_attempts >= self._config.maker_min_notional_accumulation_attempts:
                        self._journal.append(
                            "exit.passive_close_min_notional_abort",
                            {
                                "position_id": position_id,
                                "attempt": pending.small_fill_min_notional_attempts,
                                "missing_hedge_quantity": unhedged_gap,
                                "reason": "accumulation attempts exhausted",
                            },
                        )
                        pending.phase_state.phase = PassiveExecutionPhase.DUAL_TAKER
                        pending.next_retry_at_ms = 0
                        return False
                    pending.next_retry_at_ms = now_ms + PASSIVE_CLOSE_PROGRESS_POLL_INTERVAL_MS
                    return False

                # Submit hedge
                result = await self._submit_hedge_for_delta(state, pending, position, unhedged_gap)
                if not result.success:
                    pending = state.pending_passive_closes.get(position_id)
                    if pending is None:
                        return True

                    # Non-retryable hedge error → escalate regardless of maker state
                    if self._is_non_retryable_hedge_error(result.error or ""):
                        self._journal.append(
                            "exit.passive_close_hedge_non_retryable_escalated",
                            {
                                "position_id": position_id,
                                "hedge_error": result.error,
                                "unhedged_gap": unhedged_gap,
                                "reason": "escalating to aggressive close after non-retryable hedge error",
                            },
                        )
                        pending.phase_state.phase = PassiveExecutionPhase.DUAL_TAKER
                        return False

                    # V1: post-submit min-notional accumulation
                    # If maker is now terminal (not accumulating), escalate.
                    # If the hedge was above threshold and still got zero fill, it's
                    # a liquidity issue — log and retry, don't escalate.
                    if maker_terminal:
                        self._journal.append(
                            "exit.passive_close_min_notional_abort",
                            {
                                "position_id": position_id,
                                "missing_hedge_quantity": unhedged_gap,
                                "reason": "maker_terminal_after_hedge_fail",
                                "hedge_error": result.error,
                            },
                        )
                        pending.phase_state.phase = PassiveExecutionPhase.DUAL_TAKER
                        pending.next_retry_at_ms = 0
                        return False

                    is_zero_fill = result.error and "zero_fill" in str(result.error)
                    is_below_min_notional = (
                        buffered_notional > 0.0
                        and buffered_notional + 1e-9 < self._config.small_fill_buffer_notional_quote
                    )
                    if is_zero_fill and is_below_min_notional:
                        if unhedged_gap > pending.last_small_fill_missing_quantity + 1e-9:
                            pending.small_fill_min_notional_attempts += 1
                            pending.last_small_fill_missing_quantity = unhedged_gap
                        self._journal.append(
                            "exit.passive_close_min_notional_accumulating",
                            {
                                "position_id": position_id,
                                "attempt": pending.small_fill_min_notional_attempts,
                                "max_attempts": self._config.maker_min_notional_accumulation_attempts,
                                "missing_hedge_quantity": unhedged_gap,
                                "hedge_notional_quote": buffered_notional,
                                "min_notional_threshold": self._config.small_fill_buffer_notional_quote,
                                "maker_quantity": pending.maker_fill.quantity,
                                "hedge_quantity": pending.hedge_fill.quantity,
                            },
                        )
                        if pending.small_fill_min_notional_attempts >= self._config.maker_min_notional_accumulation_attempts:
                            # V1: abort maker and escalate to compensate
                            self._journal.append(
                                "exit.passive_close_min_notional_abort",
                                {
                                    "position_id": position_id,
                                    "attempt": pending.small_fill_min_notional_attempts,
                                    "missing_hedge_quantity": unhedged_gap,
                                    "reason": "accumulation attempts exhausted",
                                },
                            )
                            pending.phase_state.phase = PassiveExecutionPhase.DUAL_TAKER
                            pending.next_retry_at_ms = 0
                            return False
                        if maker_terminal:
                            # Maker is done, no more fill expected — escalate
                            self._journal.append(
                                "exit.passive_close_min_notional_abort",
                                {
                                    "position_id": position_id,
                                    "attempt": pending.small_fill_min_notional_attempts,
                                    "missing_hedge_quantity": unhedged_gap,
                                    "reason": "maker_terminal",
                                },
                            )
                            pending.phase_state.phase = PassiveExecutionPhase.DUAL_TAKER
                            pending.next_retry_at_ms = 0
                            return False
                        pending.next_retry_at_ms = now_ms + PASSIVE_CLOSE_PROGRESS_POLL_INTERVAL_MS
                        return False

                    self._journal.append(
                        "exit.passive_close_hedge_incomplete",
                        {
                            "position_id": position_id,
                            "requested": result.requested,
                            "filled": result.filled,
                            "residual": result.residual,
                            "maker_quantity": pending.maker_fill.quantity,
                            "hedge_quantity": pending.hedge_fill.quantity,
                            "chunk_index": pending.active_chunk_index,
                        },
                    )
                    pending.next_retry_at_ms = now_ms + PASSIVE_CLOSE_PROGRESS_POLL_INTERVAL_MS
                    return False

            # --- Check chunk complete (must satisfy double cumulative invariant) ---
            pending = state.pending_passive_closes.get(position_id)
            if pending is None:
                return True

            maker_full = pending.maker_fill.quantity + 1e-9 >= chunk_quantity
            hedge_caught_up = pending.hedge_fill.quantity + 1e-9 >= pending.maker_fill.quantity
            if maker_full and hedge_caught_up:
                self._journal.append(
                    "exit.passive_close_chunk_filled",
                    {
                        "position_id": position_id,
                        "chunk_index": pending.active_chunk_index,
                        "maker_quantity": pending.maker_fill.quantity,
                        "hedge_quantity": pending.hedge_fill.quantity,
                    },
                )
                if await self._advance_chunk(state, pending):
                    continue
                # advance blocked — retry next tick
                return False

            # --- Maintain maker order ---
            pending = state.pending_passive_closes.get(position_id)
            if pending is None:
                return True

            if pending.maker_fill.quantity > cycle_fill_before + 1e-9:
                # Partial fill — advance cycle, persist
                pending.phase_state.cycle_attempt += 1
                pending.phase_state.cycle_started_at_ms = now_ms
                pending.next_retry_at_ms = 0
                self._journal.append(
                    "exit.passive_close_partial_cycle_complete",
                    {
                        "position_id": position_id,
                        "cycle_attempt": pending.phase_state.cycle_attempt,
                        "maker_cumulative": pending.maker_fill.quantity,
                        "hedge_cumulative": pending.hedge_fill.quantity,
                    },
                )
                continue

            # --- Zero-fill cycle ---
            pending = state.pending_passive_closes.get(position_id)
            if pending is None:
                return True

            await self._maintain_maker_order(state, pending, position,
                                              maker_venue, maker_side, maker_leg_label,
                                              maker_price_hint, chunk_quantity)

            # Handle maintenance outcome
            pending = state.pending_passive_closes.get(position_id)
            if pending is None:
                return True

            # Still zero fill — apply retry delay
            if pending.maker_fill.quantity <= cycle_fill_before + 1e-9:
                pending.phase_state.zero_fill_cycles_in_phase += 1
                retry_delay = self._maker_cycle_retry_delay(
                    pending.phase_state.zero_fill_cycles_in_phase,
                )

                self._journal.append(
                    "execution.passive_cycle_zero_fill",
                    {
                        "position_id": position_id,
                        "phase": pending.phase_state.phase.value,
                        "zero_fill_cycles": pending.phase_state.zero_fill_cycles_in_phase,
                        "maker_leg": maker_leg_label,
                        "maker_venue": maker_venue.value,
                        "retry_delay_ms": retry_delay,
                    },
                )

                max_zero = self._config.max_zero_fill_cycles
                if pending.phase_state.zero_fill_cycles_in_phase < max_zero:
                    pending.phase_state.cycle_attempt = pending.phase_state.zero_fill_cycles_in_phase + 1
                    pending.phase_state.cycle_started_at_ms = now_ms
                    pending.next_retry_at_ms = now_ms + retry_delay
                    if wait_until_terminal and retry_delay > 0:
                        await asyncio.sleep(retry_delay / 1000.0)
                        continue
                    if not wait_until_terminal:
                        return False
                    continue

                # Phase exhaustion
                if pending.phase_state.phase == PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER:
                    # Switch to low-slippage phase, flip maker leg
                    pending.phase_state.phase = PassiveExecutionPhase.LOW_SLIPPAGE_MAKER
                    pending.phase_state.active_maker_leg = (
                        ActiveMakerLeg.SHORT if pending.phase_state.preferred_maker_leg == ActiveMakerLeg.LONG
                        else ActiveMakerLeg.LONG
                    )
                    pending.phase_state.zero_fill_cycles_in_phase = 0
                    pending.phase_state.cycle_attempt = 1
                    pending.phase_state.phase_started_at_ms = now_ms
                    pending.phase_state.cycle_started_at_ms = now_ms
                    pending.next_retry_at_ms = 0
                    pending.phase_state.maker_order_id = ""
                    pending.phase_state.maker_client_order_id = ""
                    pending.phase_state.maker_resting_limit_price = None
                    pending.maker_fill = PendingPassiveLegFill()
                    pending.hedge_fill = PendingPassiveLegFill()
                    self._journal.append(
                        "execution.passive_phase_switched",
                        {
                            "position_id": position_id,
                            "from_phase": "high_slippage_maker",
                            "to_phase": "low_slippage_maker",
                            "maker_leg": pending.phase_state.active_maker_leg.value,
                        },
                    )
                    continue

                # Both phases exhausted → fall back to dual taker (aggressive)
                pending.phase_state.phase = PassiveExecutionPhase.DUAL_TAKER
                self._journal.append(
                    "execution.dual_taker_armed",
                    {
                        "position_id": position_id,
                        "reason": "passive_phases_exhausted",
                        "remaining_quantity": pending.remaining_chunk_quantity(),
                    },
                )
                return await self._fallback_to_aggressive_close(state, pending, position)

            # Inter-cycle delay for polling
            if wait_until_terminal:
                await asyncio.sleep(self._config.progress_poll_interval_ms / 1000.0)

    # ------------------------------------------------------------------
    # Submit maker order
    # ------------------------------------------------------------------

    async def _submit_maker_order(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
        maker_venue: Venue,
        maker_side: Side,
        maker_leg_label: str,
        price_hint: float,
        chunk_quantity: float,
    ) -> bool:
        """Submit the initial GTC post-only reduce-only maker order.

        Fails closed when L2 mid or tick size is unavailable — a GTC post-only
        maker order must have a valid limit price (V1: price_hint required).
        """
        tick_size = await self._get_passive_tick_size(
            maker_venue,
            position.symbol,
            target_price=price_hint,
            side=maker_side,
        )
        if tick_size <= 0.0 or price_hint <= 0.0:
            phase = pending.phase_state
            phase.missing_l2_tick_consecutive_count += 1
            fail_count = phase.missing_l2_tick_consecutive_count

            self._journal.append(
                "exit.passive_close_missing_l2_or_tick",
                {
                    "position_id": position.position_id,
                    "maker_venue": maker_venue.value,
                    "maker_leg": maker_leg_label,
                    "price_hint": price_hint,
                    "tick_size": tick_size,
                    "consecutive_failures": fail_count,
                    "reason": "cannot submit post-only maker without valid L2 mid and tick size",
                },
            )

            if fail_count >= PASSIVE_CLOSE_MAX_MISSING_L2_TICK_FAILURES:
                self._journal.append(
                    "exit.passive_close_missing_l2_tick_escalated",
                    {
                        "position_id": position.position_id,
                        "consecutive_failures": fail_count,
                        "reason": "escalating to aggressive close after max missing-L2 failures",
                    },
                )
                pending.phase_state.phase = PassiveExecutionPhase.DUAL_TAKER
                return False

            # Exponential backoff: 3s, 6s, 12s, ... capped at 60s
            backoff_ms = min(
                PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS * (2 ** (fail_count - 1)),
                60_000,
            )
            pending.next_retry_at_ms = self._now_ms() + backoff_ms
            return False

        # Reset missing-L2 counter when L2 and tick are available
        pending.phase_state.missing_l2_tick_consecutive_count = 0

        aligned_price = align_passive_price_to_tick(price_hint, tick_size, maker_side)
        if aligned_price <= 0.0:
            self._journal.append(
                "exit.passive_close_invalid_aligned_price",
                {
                    "position_id": position.position_id,
                    "maker_venue": maker_venue.value,
                    "maker_leg": maker_leg_label,
                    "price_hint": price_hint,
                    "tick_size": tick_size,
                    "aligned_price": aligned_price,
                    "reason": "aligned price <= 0 after tick alignment",
                },
            )
            pending.next_retry_at_ms = self._now_ms() + PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS
            return False

        adapter = self._adapter(maker_venue)
        if adapter is None:
            return False

        # V1 stage resolution: maker_stage from pending's short/long_stage
        maker_stage = pending.long_stage if maker_leg_label == "long" else pending.short_stage
        maker_stage = maker_stage or ("exit_long" if maker_leg_label == "long" else "exit_short")
        attempt = pending.phase_state.maker_submit_attempt
        stage = (
            f"{maker_stage}_maker{pending.current_chunk_suffix()}"
            f"_phase_{pending.phase_state.phase.value}"
            f"_cycle_{pending.phase_state.cycle_attempt}"
            f"_attempt_{attempt}"
        )
        maker_cid = compact_client_order_id(position.position_id, stage)
        pending.phase_state.maker_submit_attempt = attempt + 1

        request = OrderRequest(
            venue=maker_venue,
            symbol=position.symbol,
            side=maker_side,
            quantity=chunk_quantity,
            price=aligned_price,
            reduce_only=True,
            post_only=True,
            time_in_force=TimeInForce.GTC,
            client_order_id=maker_cid,
        )

        try:
            ack = await adapter.submit_passive_order(request)
        except NotImplementedError:
            self._journal.append(
                "exit.passive_close_not_supported",
                {
                    "position_id": position.position_id,
                    "venue": maker_venue.value,
                    "reason": "submit_passive_order not implemented",
                },
            )
            # Fallback: mark for dual taker
            pending.phase_state.phase = PassiveExecutionPhase.DUAL_TAKER
            return False
        except OrderSubmitError as e:
            req_ctx = RequestContext.from_order_request(request)
            evidence = build_evidence_from_order_submit_error(
                e,
                venue=maker_venue.value,
                operation="submit_passive_order",
                endpoint="",
                request_context=req_ctx,
            )
            if e.is_rejected:
                self._journal.append(
                    "exit.passive_close_maker_submit_error",
                    {
                        "position_id": position.position_id,
                        "venue": maker_venue.value,
                        "error": str(e),
                        "exchange_error": evidence.to_dict(),
                        "request_context": req_ctx.to_dict(),
                        "evidence_completeness": evidence.evidence_completeness,
                    },
                )
                pending.phase_state.phase = PassiveExecutionPhase.DUAL_TAKER
                return False

            # Non-rejected error (e.g. uncertain): count failures, backoff, escalate if max reached
            phase = pending.phase_state
            phase.maker_submit_consecutive_failures += 1
            fail_count = phase.maker_submit_consecutive_failures

            self._journal.append(
                "exit.passive_close_maker_submit_error",
                {
                    "position_id": position.position_id,
                    "venue": maker_venue.value,
                    "error": str(e),
                    "exchange_error": evidence.to_dict(),
                    "request_context": req_ctx.to_dict(),
                    "evidence_completeness": evidence.evidence_completeness,
                    "consecutive_failures": fail_count,
                },
            )

            if fail_count >= PASSIVE_CLOSE_MAX_MAKER_SUBMIT_FAILURES:
                self._journal.append(
                    "exit.passive_close_maker_submit_max_failures_escalated",
                    {
                        "position_id": position.position_id,
                        "consecutive_failures": fail_count,
                        "reason": "escalating to aggressive close after max maker submit failures",
                    },
                )
                pending.phase_state.phase = PassiveExecutionPhase.DUAL_TAKER
                return False

            # Exponential backoff: 2s, 4s, 8s, ... capped at 60s
            backoff_ms = min(2_000 * (2 ** (fail_count - 1)), 60_000)
            pending.next_retry_at_ms = self._now_ms() + backoff_ms
            return False
        except Exception as e:
            req_ctx = RequestContext.from_order_request(request)
            evidence = build_fallback_evidence(
                e,
                venue=maker_venue.value,
                operation="submit_passive_order",
                request_context=req_ctx,
            )
            # Generic exceptions also count toward failure limit
            phase = pending.phase_state
            phase.maker_submit_consecutive_failures += 1
            fail_count = phase.maker_submit_consecutive_failures

            self._journal.append(
                "exit.passive_close_maker_submit_error",
                {
                    "position_id": position.position_id,
                    "venue": maker_venue.value,
                    "error": str(e),
                    "exchange_error": evidence.to_dict(),
                    "request_context": req_ctx.to_dict(),
                    "evidence_completeness": evidence.evidence_completeness,
                    "consecutive_failures": fail_count,
                },
            )

            if fail_count >= PASSIVE_CLOSE_MAX_MAKER_SUBMIT_FAILURES:
                self._journal.append(
                    "exit.passive_close_maker_submit_max_failures_escalated",
                    {
                        "position_id": position.position_id,
                        "consecutive_failures": fail_count,
                        "reason": "escalating to aggressive close after max maker submit failures",
                    },
                )
                pending.phase_state.phase = PassiveExecutionPhase.DUAL_TAKER
                return False

            backoff_ms = min(2_000 * (2 ** (fail_count - 1)), 60_000)
            pending.next_retry_at_ms = self._now_ms() + backoff_ms
            return False

        # Success: reset failure counters
        pending.phase_state.maker_submit_consecutive_failures = 0

        pending.phase_state.maker_order_id = ack.order_id
        pending.phase_state.maker_client_order_id = ack.client_order_id
        pending.phase_state.maker_resting_limit_price = aligned_price
        pending.phase_state.maker_resting_since_ms = ack.accepted_at_ms

        self._journal.append(
            "exit.passive_close_maker_submitted",
            {
                "position_id": position.position_id,
                "maker_venue": maker_venue.value,
                "maker_leg": maker_leg_label,
                "order_id": ack.order_id,
                "client_order_id": ack.client_order_id,
                "price": pending.phase_state.maker_resting_limit_price,
                "quantity": chunk_quantity,
                "chunk_index": pending.active_chunk_index,
                "phase": pending.phase_state.phase.value,
            },
        )
        return True

    # ------------------------------------------------------------------
    # Poll maker progress
    # ------------------------------------------------------------------

    async def _poll_maker_progress(
        self,
        adapter: VenueAdapter,
        symbol: str,
        order_id: str,
        client_order_id: str,
        side: Side = Side.BUY,
    ) -> Optional[PassiveOrderProgress]:
        """Query cumulative progress for a resting passive order."""
        try:
            return await adapter.query_passive_order_progress(
                symbol=symbol,
                order_id=order_id,
                client_order_id=client_order_id,
                side=side,
            )
        except NotImplementedError:
            # Fallback: adapter doesn't support passive progress query
            return None
        except Exception:
            return None

    def _apply_maker_progress(
        self,
        pending: PendingPassiveClose,
        progress: PassiveOrderProgress,
        now_ms: int,
    ) -> None:
        """Update pending state from maker progress poll result."""
        if progress.cumulative_quantity > pending.maker_fill.quantity + 1e-9:
            delta_qty = progress.cumulative_quantity - pending.maker_fill.quantity
            # Weighted average price update
            prev_total = pending.maker_fill.quantity * pending.maker_fill.average_price
            new_total = delta_qty * progress.average_price
            new_qty = pending.maker_fill.quantity + delta_qty
            pending.maker_fill.average_price = (prev_total + new_total) / new_qty if new_qty > 0 else 0.0
            pending.maker_fill.quantity = progress.cumulative_quantity
            pending.maker_fill.fee_quote += progress.fee_quote
            pending.maker_fill.last_fill_time_ms = progress.last_fill_time_ms
            pending.maker_fill.order_id = progress.order_id

            # Persist maker delta fill as a close execution leg
            maker_leg = pending.phase_state.active_maker_leg
            maker_fill = OrderFill(
                venue=progress.venue,
                symbol=progress.symbol,
                side=progress.side,
                quantity=delta_qty,
                price=progress.average_price,
                order_id=progress.order_id,
                client_order_id=progress.client_order_id,
                fee_quote=progress.fee_quote,
                filled_at_ms=progress.last_fill_time_ms or now_ms,
            )
            leg = PersistedCloseExecutionLeg(
                fill=maker_fill,
                client_order_id=progress.client_order_id,
                submit_started_at_ms=now_ms,
            )
            if maker_leg == ActiveMakerLeg.LONG:
                pending.long_legs.append(leg)
            else:
                pending.short_legs.append(leg)

            self._journal.append(
                "exit.passive_close_maker_progress",
                {
                    "position_id": pending.position_id,
                    "cumulative_quantity": progress.cumulative_quantity,
                    "average_price": progress.average_price,
                    "delta_quantity": delta_qty,
                    "state": progress.state.value,
                    "maker_leg": maker_leg.value,
                },
            )

    # ------------------------------------------------------------------
    # Delta hedge
    # ------------------------------------------------------------------

    async def _submit_hedge_for_delta(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
        delta: float,
    ) -> HedgeDeltaResult:
        """Submit IOC reduce-only taker hedge for maker fill delta.

        V1: hedges only the newly filled quantity, not the entire chunk.
        Returns a HedgeDeltaResult so callers can decide retry/fallback.
        """
        if delta <= 1e-12:
            return HedgeDeltaResult(requested=delta, filled=0.0, residual=0.0, success=True)

        maker_leg = pending.phase_state.active_maker_leg
        if maker_leg == ActiveMakerLeg.LONG:
            hedge_venue = position.short_venue
            hedge_side = Side.BUY  # closing short
            hedge_leg_label = "short"
            price_hint = self._resolve_local_l2_mid(position.short_venue, position.symbol)
        else:
            hedge_venue = position.long_venue
            hedge_side = Side.SELL  # closing long
            hedge_leg_label = "long"
            price_hint = self._resolve_local_l2_mid(position.long_venue, position.symbol)

        hedge_price = price_hint if price_hint > 0 else None
        adapter = self._adapter(hedge_venue)
        if adapter is None:
            return HedgeDeltaResult(
                requested=delta, filled=0.0, residual=delta, success=False,
                error=f"no adapter for {hedge_venue.value}",
            )

        # V1 stage resolution: hedge_stage is opposite of maker_stage
        hedge_stage = (
            pending.short_stage if hedge_leg_label == "short"
            else pending.long_stage
        )
        hedge_stage = hedge_stage or ("exit_short" if hedge_leg_label == "short" else "exit_long")
        stage = f"{hedge_stage}_hedge{pending.current_chunk_suffix()}"
        hedge_cid = compact_client_order_id(position.position_id, stage)

        request = OrderRequest(
            venue=hedge_venue,
            symbol=position.symbol,
            side=hedge_side,
            quantity=delta,
            price=hedge_price,
            reduce_only=True,
            time_in_force=TimeInForce.IOC,
            client_order_id=hedge_cid,
        )

        def record_hedge_fill(fill: OrderFill) -> None:
            previous_qty = pending.hedge_fill.quantity
            new_qty = previous_qty + fill.quantity
            pending.hedge_fill.quantity = new_qty
            prev_total = previous_qty * pending.hedge_fill.average_price
            pending.hedge_fill.average_price = (
                (prev_total + fill.quantity * fill.price) / new_qty
                if new_qty > 0 else fill.price
            )
            pending.hedge_fill.fee_quote += fill.fee_quote or 0.0
            pending.hedge_fill.last_fill_time_ms = fill.filled_at_ms
            pending.hedge_fill.order_id = fill.order_id
            pending.hedge_fill.client_order_id = hedge_cid

            leg = PersistedCloseExecutionLeg(
                fill=fill,
                client_order_id=hedge_cid,
                submit_started_at_ms=self._now_ms(),
            )
            if hedge_leg_label == "long":
                pending.long_legs.append(leg)
            else:
                pending.short_legs.append(leg)

            self._journal.append(
                "exit.passive_close_hedge_filled",
                {
                    "position_id": position.position_id,
                    "hedge_venue": hedge_venue.value,
                    "hedge_leg": hedge_leg_label,
                    "quantity": fill.quantity,
                    "price": fill.price,
                    "cumulative_hedge": pending.hedge_fill.quantity,
                    "cumulative_maker": pending.maker_fill.quantity,
                    "chunk_index": pending.active_chunk_index,
                },
            )

        try:
            fill = await adapter.place_order(request)
        except Exception as e:
            req_ctx = RequestContext.from_order_request(request)
            evidence = (
                build_evidence_from_order_submit_error(e, venue=hedge_venue.value, operation="place_order", endpoint="", request_context=req_ctx)
                if isinstance(e, OrderSubmitError)
                else build_fallback_evidence(e, venue=hedge_venue.value, operation="place_order", request_context=req_ctx)
            )
            is_bybit_duplicate = (
                hedge_venue == Venue.BYBIT
                and _is_bybit_duplicate_order_link_id(str(e))
            )
            should_reconcile = isinstance(e, OrderSubmitError) or is_bybit_duplicate
            if should_reconcile:
                reconciliation = None
                try:
                    reconciliation = await adapter.fetch_order_fill_reconciliation(
                        position.symbol, "", hedge_cid,
                    )
                except Exception as reconcile_error:
                    self._journal.append(
                        "exit.passive_close_hedge_reconcile_failed",
                        {
                            "position_id": position.position_id,
                            "hedge_venue": hedge_venue.value,
                            "hedge_leg": hedge_leg_label,
                            "client_order_id": hedge_cid,
                            "error": str(reconcile_error),
                        },
                    )

                recon_qty_raw = getattr(reconciliation, "quantity", 0.0) if reconciliation is not None else 0.0
                recon_qty = float(recon_qty_raw) if isinstance(recon_qty_raw, (int, float)) else 0.0
                if recon_qty > 1e-12:
                    recon_price_raw = getattr(reconciliation, "average_price", hedge_price or 0.0)
                    recon_price = float(recon_price_raw) if isinstance(recon_price_raw, (int, float)) else (hedge_price or 0.0)
                    fill = OrderFill(
                        venue=hedge_venue,
                        symbol=position.symbol,
                        side=getattr(reconciliation, "side", hedge_side) or hedge_side,
                        quantity=recon_qty,
                        price=recon_price,
                        order_id=getattr(reconciliation, "order_id", "") or "",
                        client_order_id=getattr(reconciliation, "client_order_id", None) or hedge_cid,
                        fee_quote=getattr(reconciliation, "fee_quote", None),
                        filled_at_ms=getattr(reconciliation, "filled_at_ms", 0) or self._now_ms(),
                    )
                    record_hedge_fill(fill)
                    residual = max(delta - fill.quantity, 0.0)
                    success = residual < 1e-12
                    event_kind = (
                        "exit.passive_close_hedge_duplicate_client_order_reconciled"
                        if is_bybit_duplicate
                        else "exit.passive_close_hedge_reconciled_after_error"
                    )
                    self._journal.append(
                        event_kind,
                        {
                            "position_id": position.position_id,
                            "hedge_venue": hedge_venue.value,
                            "hedge_leg": hedge_leg_label,
                            "client_order_id": hedge_cid,
                            "order_id": fill.order_id,
                            "requested": delta,
                            "filled": fill.quantity,
                            "residual": residual,
                            "original_error": str(e),
                        },
                    )
                    return HedgeDeltaResult(
                        requested=delta,
                        filled=fill.quantity,
                        residual=residual,
                        success=success,
                        error=None if success else "partial_fill",
                        order_id=fill.order_id,
                    )

                if is_bybit_duplicate:
                    self._journal.append(
                        "exit.passive_close_hedge_duplicate_client_order_pending_reconcile",
                        {
                            "position_id": position.position_id,
                            "hedge_venue": hedge_venue.value,
                            "hedge_leg": hedge_leg_label,
                            "client_order_id": hedge_cid,
                            "error": str(e),
                        },
                    )

            self._journal.append(
                "exit.passive_close_hedge_error",
                {
                    "position_id": position.position_id,
                    "hedge_venue": hedge_venue.value,
                    "hedge_leg": hedge_leg_label,
                    "delta": delta,
                    "error": str(e),
                    "exchange_error": evidence.to_dict(),
                    "request_context": req_ctx.to_dict(),
                    "evidence_completeness": evidence.evidence_completeness,
                },
            )
            return HedgeDeltaResult(
                requested=delta, filled=0.0, residual=delta, success=False,
                error=str(e),
            )

        filled_qty = fill.quantity if fill.quantity > 0 else 0.0
        residual = max(delta - filled_qty, 0.0)
        success = residual < 1e-12

        if fill.quantity > 0:
            record_hedge_fill(fill)

        if not success and filled_qty > 0:
            self._journal.append(
                "exit.passive_close_hedge_partial",
                {
                    "position_id": position.position_id,
                    "hedge_venue": hedge_venue.value,
                    "hedge_leg": hedge_leg_label,
                    "requested": delta,
                    "filled": filled_qty,
                    "residual": residual,
                    "chunk_index": pending.active_chunk_index,
                },
            )

        return HedgeDeltaResult(
            requested=delta, filled=filled_qty, residual=residual,
            success=success, error=None if success else "partial_fill" if filled_qty > 0 else "zero_fill",
        )

    # ------------------------------------------------------------------
    # Maintain maker order (hold / amend / cancel-replace)
    # ------------------------------------------------------------------

    async def _maintain_maker_order(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
        maker_venue: Venue,
        maker_side: Side,
        maker_leg_label: str,
        price_hint: float,
        chunk_quantity: float,
    ) -> None:
        """V1 maintain_passive_close_order (exit.rs line 860).

        Decides to hold, amend, or cancel-replace the resting maker order
        based on local L2 mid price movement and tick size.
        """
        remaining = pending.remaining_chunk_quantity()
        if remaining <= 1e-9:
            return

        now_ms = self._now_ms()
        pid = position.position_id

        tick_size = await self._get_passive_tick_size(
            maker_venue,
            position.symbol,
            target_price=price_hint,
            side=maker_side,
        )
        if tick_size <= 0.0:
            self._journal.append(
                "exit.passive_close_maintain_no_tick_size",
                {
                    "position_id": pid,
                    "venue": maker_venue.value,
                    "maker_leg": maker_leg_label,
                    "reason": "cannot reprice — no tick size available",
                },
            )
            pending.next_retry_at_ms = now_ms + PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS
            return

        if price_hint <= 0.0:
            self._journal.append(
                "exit.passive_close_maintain_no_price_hint",
                {
                    "position_id": pid,
                    "venue": maker_venue.value,
                    "maker_leg": maker_leg_label,
                    "reason": "cannot reprice — L2 mid unavailable",
                },
            )
            pending.next_retry_at_ms = now_ms + PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS
            return

        reference_mid = self._resolve_local_l2_mid(maker_venue, position.symbol)
        current_price = pending.phase_state.maker_resting_limit_price
        target_price = align_passive_price_to_tick(price_hint, tick_size, maker_side) if price_hint > 0 else None

        if current_price is None:
            self._journal.append(
                "exit.passive_close_maintain_no_resting_price",
                {
                    "position_id": pid,
                    "venue": maker_venue.value,
                    "maker_leg": maker_leg_label,
                    "reason": "no resting limit price — maker may not be submitted yet",
                },
            )
            pending.next_retry_at_ms = now_ms + PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS
            return

        if target_price is None:
            self._journal.append(
                "exit.passive_close_maintain_no_target_price",
                {
                    "position_id": pid,
                    "venue": maker_venue.value,
                    "maker_leg": maker_leg_label,
                    "price_hint": price_hint,
                    "tick_size": tick_size,
                    "reason": "aligned target price is None",
                },
            )
            pending.next_retry_at_ms = now_ms + PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS
            return

        # Close enough — hold
        price_distance_bps = abs(target_price - current_price) / current_price * 10_000 if current_price > 0 else 0
        profile = self._profile(maker_venue)

        if price_distance_bps < 1e-9:
            return  # hold: already at target

        if price_distance_bps < profile.amend_threshold_bps:
            return  # hold: within amend threshold

        # V1: ops token bucket rate limiting before any amend or cancel-replace
        if not self._ops_token_available(pending, profile, now_ms):
            self._journal.append(
                "exit.passive_close_maintain_rate_limited",
                {
                    "position_id": pid,
                    "venue": maker_venue.value,
                    "maker_leg": maker_leg_label,
                    "ops_count": pending.ops_count_this_window,
                    "ops_budget": profile.ops_budget_per_window,
                    "reason": "ops_token_exhausted",
                },
            )
            pending.next_retry_at_ms = max(pending.next_retry_at_ms, now_ms + profile.cooldown_ms)
            return

        # Consume ops token BEFORE the operation — every attempt counts
        # against the budget, whether it succeeds or fails.
        pending.ops_count_this_window += 1
        if pending.ops_window_started_at_ms <= 0:
            pending.ops_window_started_at_ms = now_ms

        # Decide amend vs cancel-replace
        if (
            self._passive_amend_supported(maker_venue)
            and price_distance_bps < profile.cancel_replace_threshold_bps
        ):
            await self._amend_maker_order(state, pending, position, maker_venue,
                                           maker_side, maker_leg_label,
                                           target_price, remaining, tick_size, reference_mid)
        else:
            if not self._passive_amend_supported(maker_venue):
                self._journal.append(
                    "exit.passive_close_amend_unsupported_cancel_replace",
                    {
                        "position_id": pid,
                        "venue": maker_venue.value,
                        "maker_leg": maker_leg_label,
                        "price_distance_bps": price_distance_bps,
                        "reason": "venue_capability_passive_amend_supported_false",
                    },
                )
            await self._cancel_replace_maker_order(state, pending, position, maker_venue,
                                                    maker_side, maker_leg_label,
                                                    target_price, remaining, tick_size, reference_mid)

    def _passive_amend_supported(self, venue: Venue) -> bool:
        try:
            return bool(get_capability_flags(venue).passive_amend_supported)
        except Exception:
            return False

    async def _amend_maker_order(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
        maker_venue: Venue,
        maker_side: Side,
        maker_leg_label: str,
        target_price: float,
        remaining_quantity: float,
        tick_size: float,
        reference_mid: Optional[float],
    ) -> None:
        """Amend resting maker order price/quantity."""
        adapter = self._adapter(maker_venue)
        if adapter is None:
            return

        amend_req = PassiveOrderAmendRequest(
            symbol=position.symbol,
            side=maker_side,
            order_id=pending.phase_state.maker_order_id,
            client_order_id=pending.phase_state.maker_client_order_id,
            new_price_hint=target_price,
            new_quantity=remaining_quantity,
        )

        now_ms = self._now_ms()
        self._journal.append(
            "exit.passive_close_amend_requested",
            {
                "position_id": position.position_id,
                "maker_venue": maker_venue.value,
                "maker_leg": maker_leg_label,
                "order_id": pending.phase_state.maker_order_id,
                "previous_price": pending.phase_state.maker_resting_limit_price,
                "new_price": target_price,
                "tick_size": tick_size,
                "reference_mid": reference_mid,
            },
        )

        try:
            ack = await adapter.amend_passive_order(amend_req)
            pending.phase_state.maker_order_id = ack.order_id
            pending.phase_state.maker_client_order_id = ack.client_order_id
            pending.phase_state.maker_resting_limit_price = target_price
            pending.phase_state.maker_resting_since_ms = ack.accepted_at_ms

            self._journal.append(
                "exit.passive_close_amend_succeeded",
                {
                    "position_id": position.position_id,
                    "order_id": ack.order_id,
                    "price": target_price,
                },
            )
        except NotImplementedError:
            # Amend not supported → cancel-replace
            await self._cancel_replace_maker_order(
                state, pending, position, maker_venue, maker_side,
                maker_leg_label, target_price, remaining_quantity, tick_size, reference_mid,
            )
        except Exception as e:
            # Transport/auth/rate-limit failure — journal, set retry, let drive loop escalate
            self._journal.append(
                "exit.passive_close_amend_failed",
                {
                    "position_id": position.position_id,
                    "error": str(e),
                    "reason": "non-unsupported failure — will retry or escalate via zero_fill budget",
                },
            )
            pending.next_retry_at_ms = self._now_ms() + PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS

    async def _cancel_replace_maker_order(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
        maker_venue: Venue,
        maker_side: Side,
        maker_leg_label: str,
        target_price: float,
        remaining_quantity: float,
        tick_size: float,
        reference_mid: Optional[float],
    ) -> None:
        """Cancel and replace resting maker order with new price."""
        adapter = self._adapter(maker_venue)
        if adapter is None:
            return

        old_order_id = pending.phase_state.maker_order_id
        old_client_id = pending.phase_state.maker_client_order_id

        now_ms = self._now_ms()
        self._journal.append(
            "exit.passive_close_cancel_replace_requested",
            {
                "position_id": position.position_id,
                "maker_venue": maker_venue.value,
                "maker_leg": maker_leg_label,
                "old_order_id": old_order_id,
                "previous_price": pending.phase_state.maker_resting_limit_price,
                "new_price": target_price,
            },
        )

        # Cancel old order
        cancel_ok = False
        try:
            await adapter.cancel_passive_order(
                symbol=position.symbol,
                order_id=old_order_id,
                client_order_id=old_client_id,
            )
            cancel_ok = True
        except NotImplementedError:
            self._journal.append(
                "exit.passive_close_cancel_not_supported",
                {"position_id": position.position_id, "order_id": old_order_id},
            )
        except Exception as e:
            self._journal.append(
                "exit.passive_close_cancel_error",
                {"position_id": position.position_id, "error": str(e)},
            )

        if not cancel_ok:
            # Cancel failed — query old order status to avoid double-order risk.
            # Only submit the new order if old order is confirmed dead.
            old_dead = await self._probe_order_dead(
                adapter, position.symbol, old_order_id, old_client_id,
                side=maker_side,
            )
            if not old_dead:
                self._journal.append(
                    "exit.passive_close_cancel_replace_blocked_double_order_risk",
                    {
                        "position_id": position.position_id,
                        "old_order_id": old_order_id,
                        "reason": "cancel failed and old order may still be alive — refusing to submit new",
                    },
                )
                pending.next_retry_at_ms = now_ms + PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS
                return

        # Submit new maker order
        submitted = await self._submit_maker_order(
            state, pending, position, maker_venue, maker_side,
            maker_leg_label, target_price, remaining_quantity,
        )

        if not submitted:
            self._journal.append(
                "exit.passive_close_cancel_replace_submit_failed",
                {
                    "position_id": position.position_id,
                    "old_order_id": old_order_id,
                    "reason": "replacement maker submit failed",
                },
            )
            if pending.phase_state.phase != PassiveExecutionPhase.DUAL_TAKER:
                pending.next_retry_at_ms = now_ms + PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS
            return

        self._journal.append(
            "exit.passive_close_cancel_replace_completed",
            {
                "position_id": position.position_id,
                "old_order_id": old_order_id,
                "new_order_id": pending.phase_state.maker_order_id,
                "price": target_price,
            },
        )

    # ------------------------------------------------------------------
    # Chunk advance
    # ------------------------------------------------------------------

    async def _advance_chunk(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
    ) -> bool:
        """V1 advance_pending_passive_close_chunk (exit.rs line 1648).

        Move to the next chunk or finalize the close. Returns True if the
        chunk was advanced (or finalized), False if advance was blocked.

        ROOT INVARIANT (non-negotiable):
          hedge_fill.quantity + eps >= maker_fill.quantity
          AND
          maker_fill.quantity + eps >= chunk_quantity

        If either fails, this method REFUSES to advance: no chunk_index bump,
        no fill reset, no finalize. The caller must retry or escalate.
        """
        chunk_quantity = pending.current_chunk_quantity()
        eps = 1e-9

        maker_ok = pending.maker_fill.quantity + eps >= chunk_quantity
        hedge_ok = pending.hedge_fill.quantity + eps >= pending.maker_fill.quantity

        if not maker_ok:
            self._journal.append(
                "exit.passive_close_advance_blocked_maker_under_chunk",
                {
                    "position_id": pending.position_id,
                    "maker_quantity": pending.maker_fill.quantity,
                    "chunk_quantity": chunk_quantity,
                    "deficit": chunk_quantity - pending.maker_fill.quantity,
                    "chunk_index": pending.active_chunk_index,
                },
            )
            pending.next_retry_at_ms = self._now_ms() + PASSIVE_CLOSE_PROGRESS_POLL_INTERVAL_MS
            return False

        if not hedge_ok:
            unhedged = pending.maker_fill.quantity - pending.hedge_fill.quantity
            self._journal.append(
                "exit.passive_close_advance_blocked_unhedged",
                {
                    "position_id": pending.position_id,
                    "maker_quantity": pending.maker_fill.quantity,
                    "hedge_quantity": pending.hedge_fill.quantity,
                    "unhedged_residual": unhedged,
                    "chunk_index": pending.active_chunk_index,
                },
            )
            pending.next_retry_at_ms = self._now_ms() + PASSIVE_CLOSE_PROGRESS_POLL_INTERVAL_MS
            return False

        pending.active_chunk_index += 1

        if pending.completed():
            await self._finalize_passive_close(state, pending)
            return True

        # Reset for next chunk
        position = pending.position_snapshot
        if position is None:
            return True

        pending.phase_state = PassivePhaseState(
            phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
            preferred_maker_leg=pending.phase_state.preferred_maker_leg,
            active_maker_leg=pending.phase_state.preferred_maker_leg,
            phase_started_at_ms=self._now_ms(),
            cycle_attempt=1,
            cycle_started_at_ms=self._now_ms(),
        )
        pending.maker_fill = PendingPassiveLegFill()
        pending.hedge_fill = PendingPassiveLegFill()
        pending.small_fill_min_notional_attempts = 0
        pending.last_small_fill_missing_quantity = 0.0
        pending.small_fill_buffer_started_at_ms = None
        pending.next_retry_at_ms = 0
        pending.multi_phase_started_at_ms = self._now_ms()

        self._journal.append(
            "exit.passive_close_advanced_chunk",
            {
                "position_id": pending.position_id,
                "new_chunk_index": pending.active_chunk_index,
                "chunk_quantity": chunk_quantity,
                "total_chunks": pending.chunk_count(),
            },
        )
        return True

    # ------------------------------------------------------------------
    # Finalize
    # ------------------------------------------------------------------

    async def _finalize_passive_close(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
    ) -> bool:
        """Build CloseExecution from accumulated legs and clean up pending state."""
        position = pending.position_snapshot
        if position is None:
            state.pending_passive_closes.pop(pending.position_id, None)
            return True

        short_legs = []
        for leg in pending.short_legs:
            if leg.fill is not None:
                short_legs.append(CloseExecutionLeg(
                    fill=leg.fill,
                    client_order_id=leg.client_order_id,
                    submit_started_at_ms=leg.submit_started_at_ms,
                    latency_ms=leg.latency_ms,
                ))

        long_legs = []
        for leg in pending.long_legs:
            if leg.fill is not None:
                long_legs.append(CloseExecutionLeg(
                    fill=leg.fill,
                    client_order_id=leg.client_order_id,
                    submit_started_at_ms=leg.submit_started_at_ms,
                    latency_ms=leg.latency_ms,
                ))

        def append_synthesized_leg(
            target: list[CloseExecutionLeg],
            *,
            pending_fill: PendingPassiveLegFill,
            existing_qty: float,
            venue: Venue,
            side: Side,
            leg_label: str,
            source: str,
        ) -> None:
            gap = max(pending_fill.quantity - existing_qty, 0.0)
            if gap <= 1e-9:
                return
            fee_quote = 0.0
            if pending_fill.quantity > 1e-12:
                fee_quote = pending_fill.fee_quote * (gap / pending_fill.quantity)
            fill = OrderFill(
                venue=venue,
                symbol=position.symbol,
                side=side,
                quantity=gap,
                price=pending_fill.average_price,
                order_id=pending_fill.order_id,
                client_order_id=pending_fill.client_order_id or None,
                fee_quote=fee_quote,
                filled_at_ms=pending_fill.last_fill_time_ms,
            )
            target.append(CloseExecutionLeg(
                fill=fill,
                client_order_id=pending_fill.client_order_id,
                submit_started_at_ms=pending_fill.last_fill_time_ms,
                latency_ms=0,
            ))
            self._journal.append(
                "exit.passive_close_synthesized_missing_leg",
                {
                    "position_id": pending.position_id,
                    "leg": leg_label,
                    "source": source,
                    "venue": venue.value,
                    "quantity": gap,
                    "price": pending_fill.average_price,
                    "order_id": pending_fill.order_id,
                    "client_order_id": pending_fill.client_order_id,
                },
            )

        existing_long_qty = sum(leg.fill.quantity for leg in long_legs if leg.fill)
        existing_short_qty = sum(leg.fill.quantity for leg in short_legs if leg.fill)
        maker_leg = pending.phase_state.active_maker_leg
        if maker_leg == ActiveMakerLeg.LONG:
            append_synthesized_leg(
                long_legs,
                pending_fill=pending.maker_fill,
                existing_qty=existing_long_qty,
                venue=position.long_venue,
                side=Side.SELL,
                leg_label="long",
                source="maker_fill",
            )
            append_synthesized_leg(
                short_legs,
                pending_fill=pending.hedge_fill,
                existing_qty=existing_short_qty,
                venue=position.short_venue,
                side=Side.BUY,
                leg_label="short",
                source="hedge_fill",
            )
        else:
            append_synthesized_leg(
                short_legs,
                pending_fill=pending.maker_fill,
                existing_qty=existing_short_qty,
                venue=position.short_venue,
                side=Side.BUY,
                leg_label="short",
                source="maker_fill",
            )
            append_synthesized_leg(
                long_legs,
                pending_fill=pending.hedge_fill,
                existing_qty=existing_long_qty,
                venue=position.long_venue,
                side=Side.SELL,
                leg_label="long",
                source="hedge_fill",
            )

        close = build_close_execution_from_legs(
            position, pending.chunk_count(), short_legs, long_legs,
        )
        close.reason = pending.reason

        # Apply close to position state
        long_closed = sum(leg.fill.quantity for leg in long_legs if leg.fill)
        short_closed = sum(leg.fill.quantity for leg in short_legs if leg.fill)
        matched_closed = min(long_closed, short_closed)

        position.matched_quantity = max(position.matched_quantity - matched_closed, 0.0)
        position.long_quantity = max(position.long_quantity - long_closed, 0.0)
        position.short_quantity = max(position.short_quantity - short_closed, 0.0)
        position.realized_price_pnl_quote += close.realized_price_pnl_quote
        position.realized_exit_fee_quote += close.long_fee_quote + close.short_fee_quote
        position.current_net_quote += close.net_quote

        # V1: detect residual from asymmetric fills (exit.rs:4903-4917)
        from lightfee.engine.close_executor import split_close_fill_residual
        now_ms = int(time.time() * 1000)
        residual = split_close_fill_residual(
            position, long_closed, short_closed, now_ms, now_ms + 30000,
        )
        if residual:
            from lightfee.engine.close_executor import _residual_task_to_dict
            state.pending_residual_repairs.append(_residual_task_to_dict(residual))
            self._journal.append(
                "exit.passive_close_residual_detected",
                {
                    "position_id": pending.position_id,
                    "exposure_quantity": residual.exposure_quantity,
                    "exposure_venue": residual.exposure_venue.value,
                },
            )

        # If fully closed, remove from open positions
        if position.matched_quantity < 1e-12:
            state.open_positions.pop(pending.position_id, None)

        # Clean up pending passive close
        state.pending_passive_closes.pop(pending.position_id, None)

        self._journal.append(
            "exit.passive_close_resolved",
            {
                "position_id": pending.position_id,
                "reason": pending.reason,
                "long_closed_qty": long_closed,
                "short_closed_qty": short_closed,
                "price_pnl": close.realized_price_pnl_quote,
                "net_quote": close.net_quote,
                "chunk_count": pending.chunk_count(),
                "total_legs": len(long_legs) + len(short_legs),
            },
        )
        return True

    # ------------------------------------------------------------------
    # Process all pending
    # ------------------------------------------------------------------

    async def process_pending_passive_closes(
        self,
        state: EngineState,
        now_ms: int,
    ) -> set[str]:
        """V1 process_pending_passive_closes (exit.rs line 2987).

        Process all ready pending passive closes. Returns the set of
        position_ids that still have pending passive closes after processing.
        """
        if not state.pending_passive_closes:
            return set()

        pending_ids = [
            pid for pid, ppc in state.pending_passive_closes.items()
            if ppc.next_retry_at_ms <= now_ms
        ]
        for pid in pending_ids:
            await self.drive_pending_passive_close(state, pid, wait_until_terminal=False)

        return set(state.pending_passive_closes.keys())

    # ------------------------------------------------------------------
    # Fallback to aggressive
    # ------------------------------------------------------------------

    def needs_aggressive_fallback(self, pending: PendingPassiveClose) -> bool:
        """True if this passive close should fall back to aggressive (dual taker)."""
        return pending.phase_state.phase == PassiveExecutionPhase.DUAL_TAKER

    async def _clear_if_live_flat(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
        *,
        source: str,
        extra: dict[str, Any] | None = None,
    ) -> bool:
        if not await self._probe_live_flatness(
            pending,
            self._adapters,
            position_snapshot=position,
        ):
            return False

        payload = {
            "position_id": pending.position_id,
            "symbol": position.symbol,
            "source": source,
        }
        if extra:
            payload.update(extra)
        self._journal.append("exit.passive_close_fallback_terminal_flat", payload)

        state.pending_passive_closes.pop(pending.position_id, None)
        state.open_positions.pop(pending.position_id, None)
        self._journal.append(
            "recovery.flat",
            {
                "position_id": pending.position_id,
                "symbol": position.symbol,
                "source": source,
            },
        )
        self._journal.append(
            "runtime.position_drift_corrected",
            {
                "position_id": pending.position_id,
                "symbol": position.symbol,
                "old_quantity": position.matched_quantity,
                "new_quantity": 0.0,
                "source": source,
            },
        )
        return True

    async def _fallback_to_aggressive_close(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
    ) -> bool:
        """Hand the remaining quantity to the aggressive CloseExecutor.

        V1 fallback semantics (dual taker):
        1. First, catch up unhedged residual (maker_fill - hedge_fill)
           by submitting a hedge for the single-sided deficit.
        2. Only then, close the paired residual (chunk_quantity - maker_fill)
           via aggressive close with total_quantity=paired_residual.
        3. Never flatten the entire position — only the current chunk residual.
        """
        maker_qty = pending.maker_fill.quantity
        hedge_qty = pending.hedge_fill.quantity
        unhedged_residual = max(maker_qty - hedge_qty, 0.0)
        paired_residual = max(pending.current_chunk_quantity() - maker_qty, 0.0)

        total_remaining = unhedged_residual + paired_residual
        if total_remaining <= 1e-9:
            return True

        if await self._clear_if_live_flat(
            state,
            pending,
            position,
            source="pending_passive_close_flat_probe",
        ):
            return True

        if self._close_executor is None:
            self._journal.append(
                "exit.passive_close_fallback_unavailable",
                {
                    "position_id": pending.position_id,
                    "reason": "no close_executor injected",
                    "unhedged_residual": unhedged_residual,
                    "paired_residual": paired_residual,
                },
            )
            pending.next_retry_at_ms = self._now_ms() + 10_000
            return False

        self._journal.append(
            "exit.passive_close_fallback_aggressive",
            {
                "position_id": pending.position_id,
                "maker_quantity": maker_qty,
                "hedge_quantity": hedge_qty,
                "unhedged_residual": unhedged_residual,
                "paired_residual": paired_residual,
                "chunk_index": pending.active_chunk_index,
                "reason": pending.reason,
            },
        )

        from lightfee.engine.close_executor import CloseExecutor

        if not isinstance(self._close_executor, CloseExecutor):
            pending.next_retry_at_ms = self._now_ms() + 5_000
            return False

        # Step 1: Catch up unhedged residual (single-sided hedge for maker-fill deficit)
        if unhedged_residual > 1e-9:
            result = await self._submit_hedge_for_delta(
                state, pending, position, unhedged_residual,
            )
            pending = state.pending_passive_closes.get(pending.position_id)
            if pending is None:
                return True
            if not result.success:
                self._journal.append(
                    "exit.passive_close_fallback_unhedged_failed",
                    {
                        "position_id": pending.position_id,
                        "unhedged_residual": unhedged_residual,
                        "hedge_result_error": result.error,
                    },
                )
                if self._is_non_retryable_hedge_error(result.error or ""):
                    if await self._clear_if_live_flat(
                        state,
                        pending,
                        position,
                        source="pending_passive_close_terminal_hedge_probe",
                        extra={
                            "unhedged_residual": unhedged_residual,
                            "hedge_result_error": result.error,
                        },
                    ):
                        return True
                pending.next_retry_at_ms = self._now_ms() + 5_000
                return False
            # Recompute paired residual after hedge catch-up
            paired_residual = max(pending.current_chunk_quantity() - pending.maker_fill.quantity, 0.0)

        # Step 2: Close paired residual via aggressive close
        if paired_residual > 1e-9:
            close_result = await self._close_executor.execute_close(
                position=position,
                reason=pending.reason,
                now_ms=self._now_ms(),
                long_price_hint=self._resolve_local_l2_mid(position.long_venue, position.symbol),
                short_price_hint=self._resolve_local_l2_mid(position.short_venue, position.symbol),
                total_quantity=paired_residual,
                state=state,
                short_stage=pending.short_stage or "exit_short",
                long_stage=pending.long_stage or "exit_long",
            )
            # Check if aggressive close actually executed
            if close_result is None:
                self._journal.append(
                    "exit.passive_close_fallback_aggressive_null_result",
                    {"position_id": pending.position_id},
                )
                if await self._clear_if_live_flat(
                    state,
                    pending,
                    position,
                    source="pending_passive_close_null_result_flat_probe",
                    extra={"paired_residual": paired_residual},
                ):
                    return True
                pending.next_retry_at_ms = self._now_ms() + 5_000
                return False

            long_closed = close_result.long_close_qty if hasattr(close_result, 'long_close_qty') else 0.0
            short_closed = close_result.short_close_qty if hasattr(close_result, 'short_close_qty') else 0.0

            if long_closed < 1e-12 and short_closed < 1e-12:
                # Zero fill — check if a PendingClose was registered for tracking
                has_pending_close = any(
                    pc.position_id == pending.position_id
                    for pc in state.pending_closes.values()
                )
                if not has_pending_close:
                    self._journal.append(
                        "exit.passive_close_fallback_zero_fill_no_pending",
                        {
                            "position_id": pending.position_id,
                            "paired_residual": paired_residual,
                            "reason": "aggressive close returned zero fill with no pending close registered",
                        },
                    )
                    if await self._clear_if_live_flat(
                        state,
                        pending,
                        position,
                        source="pending_passive_close_zero_fill_flat_probe",
                        extra={"paired_residual": paired_residual},
                    ):
                        return True
                    pending.next_retry_at_ms = self._now_ms() + 5_000
                    return False

        # After fallback close, clean up passive pending
        state.pending_passive_closes.pop(pending.position_id, None)
        self._journal.append(
            "exit.passive_close_fallback_complete",
            {"position_id": pending.position_id},
        )
        return True

    # ------------------------------------------------------------------
    # Recovery support
    # ------------------------------------------------------------------

    async def recover_passive_close(
        self,
        state: EngineState,
        position_id: str,
        adapters: dict[Venue, VenueAdapter],
    ) -> str:
        """Probe live flatness and either resume or clear passive close.

        Returns: "resumed", "cleared_flat", "ambiguous"
        """
        pending = state.pending_passive_closes.get(position_id)
        if pending is None:
            return "cleared_flat"

        position = state.open_positions.get(position_id)
        if position is None:
            # Check if position exists live on venues
            snapshot = pending.position_snapshot
            flat = await self._probe_live_flatness(pending, adapters)
            if flat:
                state.pending_passive_closes.pop(position_id, None)
                self._journal.append(
                    "exit.passive_close_recovery_cleared_flat",
                    {"position_id": position_id, "reason": "live_position_flat"},
                )
                return "cleared_flat"
            # Not flat but no open position in state → ambiguous
            return "ambiguous"

        # Still open — resume or mark as ambiguous
        if position is not None and position.matched_quantity > 0:
            pending.next_retry_at_ms = 0  # allow immediate retry
            self._journal.append(
                "exit.passive_close_recovery_resumed",
                {"position_id": position_id, "reason": pending.reason},
            )
            return "resumed"

        return "ambiguous"

    async def _probe_order_dead(
        self,
        adapter: VenueAdapter,
        symbol: str,
        order_id: str,
        client_order_id: str,
        side: Side = Side.BUY,
    ) -> bool:
        """Check whether a passive order is confirmed dead (canceled/filled/expired/rejected).

        Returns True if the order is dead or cannot be queried (conservative: a
        failed query should not permanently block progress). Returns False if the
        order is still OPEN or PARTIALLY_FILLED.
        """
        try:
            progress = await adapter.query_passive_order_progress(
                symbol=symbol,
                order_id=order_id,
                client_order_id=client_order_id,
                side=side,
            )
        except Exception:
            # Cannot query — fail-closed: treat as alive to avoid double-order risk
            return False

        if progress is None:
            return True  # no progress = order not found = dead

        dead_states = {
            PassiveOrderState.FILLED,
            PassiveOrderState.CANCELED,
            PassiveOrderState.EXPIRED,
            PassiveOrderState.REJECTED,
        }
        return progress.state in dead_states

    async def _probe_live_flatness(
        self,
        pending: PendingPassiveClose,
        adapters: dict[Venue, VenueAdapter],
        position_snapshot: OpenPosition | None = None,
    ) -> bool:
        """Check if position is flat on all relevant venues.

        Queries the long and short venue adapters for live positions.
        If both venues report zero quantity for the position symbol,
        the position is considered flat. If either venue is unreachable
        or returns ambiguous data, conservatively reports not-flat.
        """
        snapshot = position_snapshot or pending.position_snapshot
        if snapshot is None:
            return False

        symbol = snapshot.symbol
        long_venue = snapshot.long_venue
        short_venue = snapshot.short_venue

        long_flat = await self._probe_venue_flatness(long_venue, symbol, adapters)
        short_flat = await self._probe_venue_flatness(short_venue, symbol, adapters)

        if long_flat and short_flat:
            self._journal.append(
                "exit.passive_close_recovery_probe_flat",
                {
                    "position_id": pending.position_id,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "symbol": symbol,
                },
            )
            return True

        return False

    async def _probe_venue_flatness(
        self,
        venue: Venue,
        symbol: str,
        adapters: dict[Venue, VenueAdapter],
    ) -> bool:
        """Check if a single venue reports zero position for symbol."""
        adapter = adapters.get(venue)
        if adapter is None:
            return False

        try:
            pos = await adapter.fetch_position(symbol)
            qty = getattr(pos, "quantity", None)
            if (
                isinstance(qty, (int, float))
                and math.isfinite(float(qty))
                and abs(float(qty)) < 1e-9
            ):
                return True
        except Exception:
            pass

        try:
            all_positions = await adapter.fetch_all_positions()
            if isinstance(all_positions, (list, tuple)):
                for pos in all_positions:
                    qty = getattr(pos, "quantity", None)
                    if (
                        getattr(pos, "symbol", None) == symbol
                        and isinstance(qty, (int, float))
                        and math.isfinite(float(qty))
                        and abs(float(qty)) > 1e-9
                    ):
                        return False
                return True
        except Exception:
            pass

        return False

    # ------------------------------------------------------------------
    # Price and tick helpers
    # ------------------------------------------------------------------

    def _get_tick_size(self, venue: Venue, symbol: str) -> float:
        """Resolve price tick size for passive repricing."""
        adapter = self._adapter(venue)
        spec = None
        try:
            spec = get_spec(venue)
        except Exception:
            pass
        return resolve_price_tick(venue_spec=spec, adapter=adapter, symbol=symbol)

    async def _get_passive_tick_size(
        self,
        venue: Venue,
        symbol: str,
        *,
        target_price: float | None = None,
        side: Side | None = None,
    ) -> float:
        """Resolve passive order tick size using V1 metadata and quote precedence."""
        tick_size = await self._get_symbol_rule_tick_size(venue, symbol)
        if tick_size > 0.0:
            return tick_size

        tick_size = self._infer_passive_tick_from_l2_quote(
            venue,
            symbol,
            target_price=target_price,
            side=side,
        )
        if tick_size > 0.0:
            return tick_size

        return 0.0

    def _infer_passive_tick_from_l2_quote(
        self,
        venue: Venue,
        symbol: str,
        *,
        target_price: float | None = None,
        side: Side | None = None,
    ) -> float:
        quote = self._resolve_local_l2_quote(venue, symbol)
        if quote is None:
            return 0.0
        best_bid, best_ask = quote
        if not (
            math.isfinite(best_bid)
            and math.isfinite(best_ask)
            and best_bid > 0.0
            and best_ask > best_bid
        ):
            return 0.0

        tick_size = self._infer_price_tick_size([best_bid, best_ask])
        if tick_size > 0.0:
            return tick_size

        if target_price is None or side is None or not math.isfinite(target_price) or target_price <= 0.0:
            return 0.0
        spread = abs(best_ask - best_bid)
        price_distance = (
            abs(target_price - best_bid)
            if side == Side.BUY
            else abs(best_ask - target_price)
        )
        tick_size = max(price_distance, spread, sys.float_info.epsilon)
        return tick_size if math.isfinite(tick_size) and tick_size > 0.0 else 0.0

    @staticmethod
    def _infer_price_tick_size(values: list[float]) -> float:
        tick_size = 0.0
        for value in values:
            if not (math.isfinite(value) and value > 0.0):
                continue
            text = str(value)
            if "e" in text.lower():
                text = format(value, ".15f").rstrip("0").rstrip(".")
            if "." not in text:
                continue
            fractional = text.split(".", 1)[1].rstrip("0")
            if not fractional:
                continue
            inferred = 10.0 ** (-len(fractional))
            tick_size = inferred if tick_size <= 0.0 else min(tick_size, inferred)
        return tick_size

    async def _get_symbol_rule_tick_size(self, venue: Venue, symbol: str) -> float:
        adapter = self._adapter(venue)
        transport = getattr(adapter, "_transport", None) if adapter is not None else None
        if transport is None:
            return 0.0

        venue_symbol = symbol
        venue_symbol_fn = getattr(transport, "_venue_symbol", None)
        if callable(venue_symbol_fn):
            try:
                venue_symbol = venue_symbol_fn(symbol)
            except Exception:
                venue_symbol = symbol

        try:
            symbol_rule = await get_symbol_rules_cache().get(transport, venue, venue_symbol)
            if getattr(symbol_rule, "rule_source", "") == "spec_fallback":
                return 0.0
            tick_size = float(getattr(symbol_rule, "tick_size", 0.0) or 0.0)
        except Exception:
            return 0.0
        if math.isfinite(tick_size) and tick_size > 0.0:
            return tick_size
        return 0.0

    def _resolve_local_l2_mid(self, venue: Venue, symbol: str) -> float:
        """Resolve mid price from injected L2 resolver (runtime's local-L2 book).

        No adapter fallback — every adapter's fetch_market_snapshot is async,
        and calling it synchronously creates unawaited coroutine warnings.
        The runtime injects a resolver backed by the local L2 book; when that
        resolver returns 0.0 the caller must treat the price hint as unavailable.
        """
        if self._l2_mid_resolver is not None:
            try:
                mid = self._l2_mid_resolver(venue, symbol)
                if mid and mid > 0:
                    return mid
            except Exception:
                pass
        return 0.0

    def _resolve_local_l2_quote(self, venue: Venue, symbol: str) -> tuple[float, float] | None:
        """Resolve best bid/ask from injected local-L2 resolver."""
        if self._l2_quote_resolver is None:
            return None
        try:
            quote = self._l2_quote_resolver(venue, symbol)
        except Exception:
            return None
        if quote is None:
            return None
        try:
            best_bid, best_ask = quote
            best_bid = float(best_bid)
            best_ask = float(best_ask)
        except Exception:
            return None
        if (
            math.isfinite(best_bid)
            and math.isfinite(best_ask)
            and best_bid > 0.0
            and best_ask > best_bid
        ):
            return best_bid, best_ask
        return None

    def _maker_cycle_retry_delay(self, zero_fill_cycles: int) -> int:
        """V1 maker_cycle_retry_delay_ms: exponential backoff for zero-fill cycles."""
        delays = self._config.maker_cycle_retry_delays_ms
        idx = min(zero_fill_cycles, len(delays) - 1)
        return delays[idx] if idx >= 0 else delays[-1]

    @staticmethod
    def _is_non_retryable_hedge_error(error_str: str) -> bool:
        """Return True if the hedge error is non-retryable (reduce-only rejected etc)."""
        if not error_str:
            return False
        return (
            "-2022" in error_str
            or "ReduceOnly" in error_str
            or "reduce_only" in error_str.lower()
        )

    def _check_hedge_min_notional(
        self,
        hedge_venue: Venue,
        symbol: str,
        side: Side,
        quantity: float,
        price_hint: float,
    ) -> Optional[dict[str, Any]]:
        """V1: check if hedge quantity is below venue min notional.

        Returns None if the hedge passes min notional check.
        Returns dict with violation details if below min notional.
        """
        if quantity <= 1e-12:
            return {"venue": hedge_venue, "leg_notional": 0.0, "min_notional": 0.0}
        # Use venue min notional from spec if available, else buffer threshold
        from lightfee.venues.specs import get_spec
        min_notional = self._config.small_fill_buffer_notional_quote
        try:
            spec = get_spec(hedge_venue)
            if hasattr(spec, 'min_notional') and spec.min_notional:
                min_notional = max(min_notional, float(spec.min_notional))
        except Exception:
            pass
        violation = close_leg_exchange_min_notional_violation(
            hedge_venue, symbol, side, quantity,
            reduce_only=True, price_hint=price_hint,
            min_notional_quote=min_notional,
        )
        if violation is not None:
            venue, leg_notional, min_notional_val = violation
            return {
                "venue": venue,
                "leg_notional": leg_notional,
                "min_notional": min_notional_val,
                "quantity": quantity,
                "price_hint": price_hint,
            }
        return None

    # ------------------------------------------------------------------
    # Small-fill buffer (V1 passive_close_small_fill_buffer_decision)
    # ------------------------------------------------------------------

    @staticmethod
    def _small_fill_buffer_decision(
        buffered_notional_quote: float,
        buffer_notional_quote: float,
        buffer_wait_ms: int,
        buffer_started_at_ms: Optional[int],
        now_ms: int,
        can_accumulate_small_fill: bool,
    ) -> dict[str, Any]:
        """V1 passive_close_small_fill_buffer_decision (exit.rs:6212).

        Returns dict with keys:
        - should_buffer: True if the hedge should wait for more maker fill
        - wait_expired: True if the buffer window has elapsed
        - buffered_elapsed_ms: time since buffer started
        - remaining_wait_ms: remaining buffer time
        """
        can_buffer = (
            buffer_notional_quote > 0.0
            and buffered_notional_quote > 0.0
            and buffered_notional_quote + 1e-9 < buffer_notional_quote
            and can_accumulate_small_fill
        )
        if not can_buffer:
            return {
                "should_buffer": False,
                "wait_expired": False,
                "buffered_elapsed_ms": 0,
                "remaining_wait_ms": 0,
            }
        oldest_fill_at_ms = max(buffer_started_at_ms or now_ms, 0)
        buffered_elapsed_ms = max(now_ms - oldest_fill_at_ms, 0)
        effective_wait_ms = max(buffer_wait_ms, 1)
        wait_expired = buffered_elapsed_ms >= effective_wait_ms
        return {
            "should_buffer": not wait_expired,
            "wait_expired": wait_expired,
            "buffered_elapsed_ms": buffered_elapsed_ms,
            "remaining_wait_ms": max(effective_wait_ms - buffered_elapsed_ms, 1),
        }

    def _select_preferred_maker_leg(self, position: OpenPosition) -> ActiveMakerLeg:
        """V1 select_exit_maker_leg (discovery.rs line 65).

        Selects which leg to use as the passive (post-only) maker based on
        estimated slippage/bps on each venue. The leg with higher estimated
        taker cost should be the maker leg to save on fees/slippage.

        Uses runtime-injected local L2 resolver for mid + quote resolver for
        spread. Falls back to venue taker fee comparison if L2 is unavailable.
        If L2 data is missing for either venue, journals the gap and uses a
        deterministic fallback chain. Tie-break defaults to LONG (V1 behavior).
        """
        symbol = position.symbol
        pid = position.position_id

        # Resolve local L2 mid for both venues via injected resolver
        long_mid = self._resolve_local_l2_mid(position.long_venue, symbol)
        short_mid = self._resolve_local_l2_mid(position.short_venue, symbol)

        # Compute L2-based cost estimates
        long_cost_bps = self._estimate_venue_taker_cost_bps(
            position.long_venue, symbol, l2_mid=long_mid,
        )
        short_cost_bps = self._estimate_venue_taker_cost_bps(
            position.short_venue, symbol, l2_mid=short_mid,
        )

        # Track data quality for journal
        long_has_l2 = long_mid > 0.0
        short_has_l2 = short_mid > 0.0

        if not long_has_l2 or not short_has_l2:
            self._journal.append(
                "exit.passive_close_maker_leg_l2_missing",
                {
                    "position_id": pid,
                    "long_venue": position.long_venue.value,
                    "short_venue": position.short_venue.value,
                    "long_mid_available": long_has_l2,
                    "short_mid_available": short_has_l2,
                    "long_cost_bps": long_cost_bps,
                    "short_cost_bps": short_cost_bps,
                },
            )

        if long_cost_bps > short_cost_bps + 1e-9:
            self._journal.append(
                "exit.passive_close_maker_leg_selected",
                {
                    "position_id": pid,
                    "selected_leg": "long",
                    "long_taker_cost_bps": long_cost_bps,
                    "short_taker_cost_bps": short_cost_bps,
                    "long_l2_available": long_has_l2,
                    "short_l2_available": short_has_l2,
                    "reason": "long_taker_cost_higher",
                },
            )
            return ActiveMakerLeg.LONG
        elif short_cost_bps > long_cost_bps + 1e-9:
            self._journal.append(
                "exit.passive_close_maker_leg_selected",
                {
                    "position_id": pid,
                    "selected_leg": "short",
                    "long_taker_cost_bps": long_cost_bps,
                    "short_taker_cost_bps": short_cost_bps,
                    "long_l2_available": long_has_l2,
                    "short_l2_available": short_has_l2,
                    "reason": "short_taker_cost_higher",
                },
            )
            return ActiveMakerLeg.SHORT
        else:
            self._journal.append(
                "exit.passive_close_maker_leg_selected",
                {
                    "position_id": pid,
                    "selected_leg": "long",
                    "long_taker_cost_bps": long_cost_bps,
                    "short_taker_cost_bps": short_cost_bps,
                    "long_l2_available": long_has_l2,
                    "short_l2_available": short_has_l2,
                    "reason": "tie_or_equal_cost_default_long",
                },
            )
            return ActiveMakerLeg.LONG

    def _estimate_venue_taker_cost_bps(
        self, venue: Venue, symbol: str, l2_mid: float = 0.0,
    ) -> float:
        """Estimate the effective taker cost in bps for a venue.

        Uses injected L2 quote resolver for spread + venue taker fee.
        Fallback: taker fee only if L2 spread unavailable.
        """
        cost_bps = 0.0

        # Spread-based slippage from injected L2 quote resolver
        quote = self._resolve_local_l2_quote(venue, symbol)
        if quote is not None and l2_mid > 0.0:
            best_bid, best_ask = quote
            if best_bid > 0 and best_ask > best_bid:
                spread_bps = (best_ask - best_bid) / l2_mid * 10_000
                cost_bps += spread_bps

        # Add venue taker fee from spec
        try:
            spec = get_spec(venue)
            taker_fee = getattr(spec, 'taker_fee_bps', 0.0) or 0.0
            cost_bps += taker_fee
        except Exception:
            pass

        return max(cost_bps, 0.0)
