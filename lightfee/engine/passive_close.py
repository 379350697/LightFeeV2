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
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from lightfee.core.contracts import VenueAdapter
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
    build_close_execution_from_legs,
    compute_close_chunks,
)
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
from lightfee.venues.specs import get_spec

# ---------------------------------------------------------------------------
# V1 constants
# ---------------------------------------------------------------------------

PASSIVE_CLOSE_PROGRESS_POLL_INTERVAL_MS = 10
PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS = 3_000
PASSIVE_CLOSE_SMALL_FILL_BUFFER_MS = 2_000
PASSIVE_CLOSE_MAX_ZERO_FILL_CYCLES = 3
PASSIVE_CLOSE_MAX_MANAGER_FAILURES = 3
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
    maker_cycle_retry_delays_ms: list[int] = field(default_factory=lambda: [500, 2_000, 5_000, 15_000])
    max_slippage_bps: Optional[float] = None
    default_tick_size: float = 0.01
    close_chunk_max_notional_quote: float = 0.0


# ---------------------------------------------------------------------------
# Passive close executor
# ---------------------------------------------------------------------------


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
            maker_cycle_retry_delays_ms=overrides.get("maker_cycle_retry_delays_ms", [500, 2_000, 5_000, 15_000]),
            max_slippage_bps=overrides.get("max_slippage_bps"),
            default_tick_size=overrides.get("default_tick_size", 0.01),
            close_chunk_max_notional_quote=overrides.get("close_chunk_max_notional_quote", 0.0),
        )
        # Inject L2 mid resolver for live repricing (set by runtime after construction)
        self._l2_mid_resolver: Optional[callable] = None
        # Inject aggressive close executor for fallback (set by runtime after construction)
        self._close_executor: Optional[object] = None

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _adapter(self, venue: Venue) -> Optional[VenueAdapter]:
        return self._adapters.get(venue)

    def set_l2_mid_resolver(self, resolver: callable) -> None:
        self._l2_mid_resolver = resolver

    def set_close_executor(self, executor: object) -> None:
        self._close_executor = executor

    def _profile(self, venue: Venue) -> PassiveCloseManagerProfile:
        return PassiveCloseManagerProfile()

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
        short_stage: str = "",
        long_stage: str = "",
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
        # Choose preferred maker leg based on venue liquidity
        preferred_leg = ActiveMakerLeg.LONG
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
            short_stage=short_stage,
            long_stage=long_stage,
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
                        # Maker fully filled — hedge delta before advancing chunk
                        maker_fill_delta = pending.maker_fill.quantity - cycle_fill_before
                        if maker_fill_delta > 1e-9:
                            await self._submit_hedge_for_delta(state, pending, position, maker_fill_delta)
                        # Re-read pending after hedge
                        pending = state.pending_passive_closes.get(position_id)
                        if pending is None:
                            return True
                        await self._advance_chunk(state, pending)
                        continue
                    else:
                        # Maker order died → restart maintenance cycle
                        continue

            # --- Delta hedge: hedge newly filled maker quantity ---
            pending = state.pending_passive_closes.get(position_id)
            if pending is None:
                return True

            maker_fill_delta = pending.maker_fill.quantity - cycle_fill_before
            if maker_fill_delta > 1e-9:
                await self._submit_hedge_for_delta(state, pending, position, maker_fill_delta)

            # --- Check chunk complete ---
            pending = state.pending_passive_closes.get(position_id)
            if pending is None:
                return True

            if pending.maker_fill.quantity + 1e-9 >= chunk_quantity:
                self._journal.append(
                    "exit.passive_close_chunk_filled",
                    {
                        "position_id": position_id,
                        "chunk_index": pending.active_chunk_index,
                        "maker_quantity": pending.maker_fill.quantity,
                        "hedge_quantity": pending.hedge_fill.quantity,
                    },
                )
                await self._advance_chunk(state, pending)
                continue

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
        """Submit the initial GTC post-only reduce-only maker order."""
        tick_size = self._get_tick_size(maker_venue, position.symbol)
        aligned_price = align_passive_price_to_tick(price_hint, tick_size, maker_side) if tick_size > 0 else price_hint

        adapter = self._adapter(maker_venue)
        if adapter is None:
            return False

        close_id = f"pclose-{position.position_id}-{self._now_ms()}"
        maker_cid = f"{close_id}-maker{pending.current_chunk_suffix()}"

        request = OrderRequest(
            venue=maker_venue,
            symbol=position.symbol,
            side=maker_side,
            quantity=chunk_quantity,
            price=aligned_price if aligned_price > 0 else None,
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
        except Exception as e:
            self._journal.append(
                "exit.passive_close_maker_submit_error",
                {
                    "position_id": position.position_id,
                    "venue": maker_venue.value,
                    "error": str(e),
                },
            )
            pending.next_retry_at_ms = self._now_ms() + 2_000
            return False

        pending.phase_state.maker_order_id = ack.order_id
        pending.phase_state.maker_client_order_id = ack.client_order_id
        pending.phase_state.maker_resting_limit_price = aligned_price if aligned_price > 0 else price_hint
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
    ) -> Optional[PassiveOrderProgress]:
        """Query cumulative progress for a resting passive order."""
        try:
            return await adapter.query_passive_order_progress(
                symbol=symbol,
                order_id=order_id,
                client_order_id=client_order_id,
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
    ) -> None:
        """Submit IOC reduce-only taker hedge for maker fill delta.

        V1: hedges only the newly filled quantity, not the entire chunk.
        """
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
            return

        close_id = f"pclose-{position.position_id}-{self._now_ms()}"
        hedge_cid = f"{close_id}-hedge{pending.current_chunk_suffix()}"

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

        try:
            fill = await adapter.place_order(request)
        except Exception as e:
            self._journal.append(
                "exit.passive_close_hedge_error",
                {
                    "position_id": position.position_id,
                    "hedge_venue": hedge_venue.value,
                    "hedge_leg": hedge_leg_label,
                    "delta": delta,
                    "error": str(e),
                },
            )
            return

        if fill.quantity > 0:
            pending.hedge_fill.quantity += fill.quantity
            prev_total = (pending.hedge_fill.quantity - fill.quantity) * pending.hedge_fill.average_price
            pending.hedge_fill.average_price = (
                (prev_total + fill.quantity * fill.price) / pending.hedge_fill.quantity
                if pending.hedge_fill.quantity > 0 else fill.price
            )
            pending.hedge_fill.fee_quote += fill.fee_quote or 0.0
            pending.hedge_fill.last_fill_time_ms = fill.filled_at_ms
            pending.hedge_fill.order_id = fill.order_id
            pending.hedge_fill.client_order_id = hedge_cid

            # Persist hedge leg
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

        tick_size = self._get_tick_size(maker_venue, position.symbol)
        if tick_size <= 0.0:
            # Cannot reprice without tick size
            self._journal.append(
                "exit.passive_close_no_tick_size",
                {
                    "position_id": position.position_id,
                    "venue": maker_venue.value,
                    "reason": "cannot reprice — no tick size available",
                },
            )
            return

        reference_mid = self._resolve_local_l2_mid(maker_venue, position.symbol)
        current_price = pending.phase_state.maker_resting_limit_price
        target_price = align_passive_price_to_tick(price_hint, tick_size, maker_side) if price_hint > 0 else None

        if current_price is None or target_price is None:
            return

        # Close enough — hold
        price_distance_bps = abs(target_price - current_price) / current_price * 10_000 if current_price > 0 else 0
        profile = self._profile(maker_venue)

        if price_distance_bps < 1e-9:
            return  # hold: already at target

        if price_distance_bps < profile.amend_threshold_bps:
            return  # hold: within amend threshold

        # Decide amend vs cancel-replace
        if price_distance_bps < profile.cancel_replace_threshold_bps:
            await self._amend_maker_order(state, pending, position, maker_venue,
                                           maker_side, maker_leg_label,
                                           target_price, remaining, tick_size, reference_mid)
        else:
            await self._cancel_replace_maker_order(state, pending, position, maker_venue,
                                                    maker_side, maker_leg_label,
                                                    target_price, remaining, tick_size, reference_mid)

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
            self._journal.append(
                "exit.passive_close_amend_failed",
                {
                    "position_id": position.position_id,
                    "error": str(e),
                },
            )

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
        try:
            await adapter.cancel_passive_order(
                symbol=position.symbol,
                order_id=old_order_id,
                client_order_id=old_client_id,
            )
        except (NotImplementedError, Exception) as e:
            self._journal.append(
                "exit.passive_close_cancel_error",
                {"position_id": position.position_id, "error": str(e)},
            )
            # Continue with new order even if cancel fails

        # Submit new maker order
        await self._submit_maker_order(
            state, pending, position, maker_venue, maker_side,
            maker_leg_label, target_price, remaining_quantity,
        )

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
    ) -> None:
        """V1 advance_pending_passive_close_chunk (exit.rs line 1648).

        Move to the next chunk or finalize the close.
        """
        pending.active_chunk_index += 1

        if pending.completed():
            await self._finalize_passive_close(state, pending)
            return

        # Reset for next chunk
        chunk_quantity = pending.current_chunk_quantity()
        position = pending.position_snapshot
        if position is None:
            return

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

    async def _fallback_to_aggressive_close(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
    ) -> bool:
        """Hand the remaining quantity to the aggressive CloseExecutor."""
        remaining = pending.remaining_chunk_quantity()
        if remaining <= 1e-9:
            return True

        if self._close_executor is None:
            self._journal.append(
                "exit.passive_close_fallback_unavailable",
                {
                    "position_id": pending.position_id,
                    "reason": "no close_executor injected",
                    "remaining_quantity": remaining,
                },
            )
            pending.next_retry_at_ms = self._now_ms() + 10_000
            return False

        self._journal.append(
            "exit.passive_close_fallback_aggressive",
            {
                "position_id": pending.position_id,
                "remaining_quantity": remaining,
                "reason": pending.reason,
            },
        )

        from lightfee.engine.close_executor import CloseExecutor

        if not isinstance(self._close_executor, CloseExecutor):
            pending.next_retry_at_ms = self._now_ms() + 5_000
            return False

        success = await self._close_executor.execute_close(
            position=position,
            reason=pending.reason,
            now_ms=self._now_ms(),
            long_price_hint=self._resolve_local_l2_mid(position.long_venue, position.symbol),
            short_price_hint=self._resolve_local_l2_mid(position.short_venue, position.symbol),
            state=state,
        )
        if not success:
            pending.next_retry_at_ms = self._now_ms() + 5_000
            return False

        # After aggressive close, clean up passive pending
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

    async def _probe_live_flatness(
        self,
        pending: PendingPassiveClose,
        adapters: dict[Venue, VenueAdapter],
    ) -> bool:
        """Check if position is flat on all relevant venues.

        Queries the long and short venue adapters for live positions.
        If both venues report zero quantity for the position symbol,
        the position is considered flat. If either venue is unreachable
        or returns ambiguous data, conservatively reports not-flat.
        """
        snapshot = pending.position_snapshot
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
            if pos is not None and pos.quantity < 1e-9:
                return True
        except Exception:
            pass

        try:
            all_positions = await adapter.fetch_all_positions()
            if all_positions is not None:
                for pos in all_positions:
                    if pos.symbol == symbol and pos.quantity > 1e-9:
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

    def _resolve_local_l2_mid(self, venue: Venue, symbol: str) -> float:
        """Resolve mid price from injected L2 resolver (runtime's local-L2 book).

        Falls back to adapter snapshot if resolver not injected, then to 0.0.
        """
        # Primary: injected resolver from runtime.local_l2_runtime
        if self._l2_mid_resolver is not None:
            try:
                mid = self._l2_mid_resolver(venue, symbol)
                if mid and mid > 0:
                    return mid
            except Exception:
                pass

        # Fallback: adapter snapshot (synchronous, may return 0 for async adapters)
        adapter = self._adapter(venue)
        if adapter is None:
            return 0.0

        try:
            snapshot = adapter.fetch_market_snapshot([symbol])
            import asyncio
            if asyncio.iscoroutine(snapshot):
                return 0.0
            if snapshot is not None:
                for quote in getattr(snapshot, 'quotes', []):
                    if getattr(quote, 'symbol', '') == symbol:
                        bid = getattr(quote, 'bid', 0)
                        ask = getattr(quote, 'ask', 0)
                        if bid > 0 and ask > 0:
                            return (bid + ask) / 2.0
        except Exception:
            pass

        return 0.0

    def _maker_cycle_retry_delay(self, zero_fill_cycles: int) -> int:
        """V1 maker_cycle_retry_delay_ms: exponential backoff for zero-fill cycles."""
        delays = self._config.maker_cycle_retry_delays_ms
        idx = min(zero_fill_cycles, len(delays) - 1)
        return delays[idx] if idx >= 0 else delays[-1]
