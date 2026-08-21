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
from dataclasses import dataclass, field, replace
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
    OrderFillReconciliation,
    OrderRequest,
    PassiveOrderAck,
    PassiveOrderAmendRequest,
    PassiveOrderProgress,
    PassiveOrderState,
    Side,
    TimeInForce,
    Venue,
)
from lightfee.engine.bybit_duplicate_reconcile import (
    build_order_reconcile_result_payload,
    reconcile_bybit_duplicate_client_order,
)
from lightfee.engine.close_executor import (
    CloseExecutionLeg,
    _is_bybit_duplicate_order_link_id,
    build_close_execution_from_legs,
    close_accounting_evidence_gaps,
    close_leg_exchange_min_notional_violation,
    compute_close_chunks,
    register_close_accounting_reconciliation,
)
from lightfee.engine.lifecycle import enter_fail_closed
from lightfee.engine.order_submit_uncertainty import (
    build_order_submit_uncertainty_payload,
    is_order_truth_gap,
)
from lightfee.engine.order_truth_ledger import ORDER_TRUTH_LEDGER, OrderTruthFillStatus
from lightfee.venues.cid import compact_client_order_id, generate_exchange_cid
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
    is_unattributed_recovered_live_flat_reconciliation,
    pending_close_reconciliation_identity_evidence,
    pending_close_reconciliation_missing_legs,
)
from lightfee.persistence.journal import Journal
from lightfee.venues.common import (
    align_passive_price_to_tick,
    resolve_price_tick,
    venue_reduce_only_close_exempts_min_notional,
)
from lightfee.venues.capabilities import get_capability_flags
from lightfee.venues.specs import get_spec
from lightfee.venues.symbol_rules import get_symbol_rules_cache
from lightfee.engine.recovery import clear_legacy_recovery_block_via_core
from lightfee.engine.recovery_decision_core import (
    RecoveryEvidenceSnapshot,
    V1RecoveryDecisionCore,
)
from lightfee.engine.recovery_ledger import RecoveryLedger
from lightfee.engine.v1_lifecycle_closure import (
    build_v1_lifecycle_closure_table,
    closure_event_fields,
)
from lightfee.risk.modes import GlobalRiskMode

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
PASSIVE_CLOSE_POST_ONLY_LADDER_FRACTIONS = (0.0, 0.5, 0.75, 1.0)
PASSIVE_CLOSE_POST_ONLY_ATTEMPT_LIMIT = 7
PASSIVE_CLOSE_POST_ONLY_RETRY_BACKOFF_MS = (500, 1_000, 2_000, 4_000, 6_000, 8_000, 10_000)


def _passive_close_post_only_attempt_limit() -> int:
    """V1's seven-attempt post-only retry cap."""
    return PASSIVE_CLOSE_POST_ONLY_ATTEMPT_LIMIT


def _is_initial_passive_requote_error(venue: Venue, error: OrderSubmitError) -> bool:
    """V1 post-only rejects are retryable only for the venue-specific signatures."""
    message = str(error).lower()
    if "local_post_only_crosses_market" in message:
        return True
    if venue == Venue.BINANCE:
        return (
            ("-5022" in message and ("post only" in message or "post_only" in message))
            or "gtx_order_reject" in message
            or (
                "status=429" in message
                and (
                    "too many requests" in message
                    or "rate limited" in message
                    or "retry_after" in message
                )
            )
        )
    if venue == Venue.GATE:
        return "order_poc_immediate" in message
    if venue == Venue.OKX:
        return (
            "maker order timed out waiting for passive fill" in message
            and ("post only" in message or "post_only" in message)
        )
    return False


def _passive_close_post_only_retry_wait_ms(error: OrderSubmitError, attempt: int) -> int:
    """V1 retry schedule, honoring a transport-provided retry-after when present."""
    fallback_ms = PASSIVE_CLOSE_POST_ONLY_RETRY_BACKOFF_MS[
        min(attempt, len(PASSIVE_CLOSE_POST_ONLY_RETRY_BACKOFF_MS) - 1)
    ]
    marker = "retry_after_ms="
    message = str(error).lower()
    start = message.find(marker)
    if start < 0:
        return fallback_ms
    digits = []
    for char in message[start + len(marker):]:
        if not char.isdigit():
            break
        digits.append(char)
    try:
        retry_after_ms = int("".join(digits))
    except ValueError:
        return fallback_ms
    return max(fallback_ms, retry_after_ms) if retry_after_ms > 0 else fallback_ms


def _is_okx_amend_invalid_request_type_error(error: Exception) -> bool:
    text = str(error).lower()
    endpoint = str(getattr(error, "endpoint", "") or "").lower()
    exchange_code = str(getattr(error, "exchange_code", "") or "")
    exchange_msg = str(getattr(error, "exchange_msg", "") or "").lower()
    try:
        http_status = int(getattr(error, "http_status", 0) or 0)
    except (TypeError, ValueError):
        http_status = 0
    is_amend_endpoint = "amend" in endpoint or "amend-order" in text
    has_invalid_request_code = exchange_code == "50115" or "50115" in text
    has_invalid_request_msg = (
        "invalid request type" in text or "invalid request type" in exchange_msg
    )
    return (
        is_amend_endpoint
        and has_invalid_request_code
        and (has_invalid_request_msg or http_status == 405)
    )


def _is_bybit_terminal_zero_qty_reduce_only_error(
    error: OrderSubmitError,
    *,
    venue: Venue,
    evidence: ExchangeErrorEvidence,
    request_context: RequestContext,
) -> bool:
    """Bybit reduce-only close reject when live reducible quantity is already zero."""
    if venue != Venue.BYBIT:
        return False
    if request_context.reduce_only is not True:
        return False
    code = str(evidence.exchange_code or "")
    msg = str(evidence.exchange_msg or "").lower()
    text = str(error).lower()
    return (
        code == "110017"
        and (
            "orderqty will be truncated to zero" in msg
            or "orderqty will be truncated to zero" in text
        )
    )


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


class PassiveCloseLiveTruthResolution(Enum):
    CONTINUE_MAKER = "continue_maker"
    CLEARED = "cleared"
    STOP_RETRY = "stop_retry"


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
    maker_hedge_deadline_ms: int = 800


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
    truth_gap: bool = False
    accepted_order_id: str = ""
    accepted_client_order_id: str = ""
    hedge_submit_started_at_ms: int = 0
    hedge_submit_completed_at_ms: int = 0
    hedge_submit_quantity: float = 0.0
    hedge_submit_reconciled: bool = False


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
        self._runtime_mode = str(overrides.get("runtime_mode", "live") or "live").lower()
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
            maker_hedge_deadline_ms=overrides.get("maker_hedge_deadline_ms", 800),
        )
        # Inject L2 mid resolver for live repricing (set by runtime after construction)
        self._l2_mid_resolver: Optional[callable] = None
        # Inject L2 top-of-book resolver for V1 passive tick inference.
        self._l2_quote_resolver: Optional[callable] = None
        # Inject aggressive close executor for fallback (set by runtime after construction)
        self._close_executor: Optional[object] = None
        self._last_maker_progress_error: dict[str, Any] | None = None
        self._terminal_zero_fill_diagnostic_signatures: dict[
            tuple[str, str, str], tuple[str, str, str]
        ] = {}

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _v1_lifecycle_passive_close_event_fields(
        self,
        state: EngineState,
        position_id: str,
        now_ms: int,
        *,
        exchange_truth: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        previous = dict(getattr(state, "v1_lifecycle_closure", {}) or {})
        closure = build_v1_lifecycle_closure_table(
            local_state=state,
            exchange_truth=exchange_truth,
            generated_at_ms=now_ms,
            previous_table=previous,
        ).to_dict()
        state.v1_lifecycle_closure = closure
        return closure_event_fields(
            closure,
            phase="PASSIVE_CLOSE",
            owner_id=str(position_id or ""),
        )

    def _adapter(self, venue: Venue) -> Optional[VenueAdapter]:
        return self._adapters.get(venue)

    @staticmethod
    def _terminal_zero_fill_status_is_authoritative(
        progress: PassiveOrderProgress,
    ) -> bool:
        """Return whether a terminal status itself proves zero execution.

        ``REJECTED`` and ``EXPIRED`` are immutable no-fill outcomes.  A
        ``CANCELED`` order may still have executions that need a separate
        execution-history query, while ``FILLED`` with zero executions is an
        explicit contradiction and remains fail-closed.
        """
        return progress.state in (
            PassiveOrderState.REJECTED,
            PassiveOrderState.EXPIRED,
        )

    def _append_terminal_zero_fill_truth_unavailable(
        self,
        *,
        pending: PendingPassiveClose,
        position: OpenPosition,
        maker_venue: Venue,
        progress: PassiveOrderProgress,
        maker_order_id: str,
        maker_client_id: str,
        terminal_truth_error: str,
        terminal_truth_error_type: str,
        next_retry_at_ms: int,
    ) -> None:
        """Journal one distinct terminal-zero execution-truth gap per state."""
        order_id = progress.order_id or maker_order_id
        client_order_id = progress.client_order_id or maker_client_id
        result = "error" if terminal_truth_error_type else "unavailable"
        signature = (
            progress.state.value,
            result,
            terminal_truth_error_type,
        )
        key = (pending.position_id, maker_venue.value, order_id or client_order_id)
        previous = self._terminal_zero_fill_diagnostic_signatures.get(key)
        if previous == signature:
            return
        if len(self._terminal_zero_fill_diagnostic_signatures) >= 256:
            self._terminal_zero_fill_diagnostic_signatures.clear()
        self._terminal_zero_fill_diagnostic_signatures[key] = signature

        payload = {
            "position_id": pending.position_id,
            "symbol": position.symbol,
            "maker_venue": maker_venue.value,
            "maker_order_id": order_id,
            "maker_client_order_id": client_order_id,
            "state": progress.state.value,
            "decision": "retain_pending",
            "reason": (
                "execution_truth_query_error"
                if terminal_truth_error_type
                else "no_execution_truth"
            ),
            "execution_truth_query": "fetch_order_fill_reconciliation",
            "execution_truth_result": result,
            "execution_truth_error_type": terminal_truth_error_type,
            "diagnostic_emission": "initial" if previous is None else "state_changed",
            "zero_fill_cycles_in_phase": pending.phase_state.zero_fill_cycles_in_phase,
            "next_retry_at_ms": next_retry_at_ms,
        }
        if terminal_truth_error:
            payload["error"] = terminal_truth_error
        self._journal.append("exit.passive_close_terminal_zero_fill_truth_unavailable", payload)

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

    def _exit_deadline_extension_ms(
        self,
        *,
        base_hard_deadline_ms: int,
        notional_quote: float,
        quote_fresh: bool,
        has_execution_progress: bool,
        reconciled: bool,
    ) -> int:
        """V1 adaptive_hedge_deadline_extension_ms for passive exit hedges."""
        if not quote_fresh:
            return 0
        if reconciled and not has_execution_progress:
            return 0

        notional = max(float(notional_quote or 0.0), 0.0)
        if notional <= 50.0:
            base_extension_ms = 800
        elif notional <= 150.0:
            base_extension_ms = 400
        elif notional <= 300.0:
            base_extension_ms = 200
        else:
            base_extension_ms = 0

        if base_extension_ms <= 0:
            return 0

        progress_bonus_ms = 250 if has_execution_progress else 0
        reconciled_bonus_ms = 100 if reconciled and has_execution_progress else 0
        exit_cap_ms = max(int(base_hard_deadline_ms or 0) // 5, 0)
        return min(base_extension_ms + progress_bonus_ms + reconciled_bonus_ms, exit_cap_ms)

    @staticmethod
    def _deadline_clock_domains_match(started_at_ms: int, now_ms: int) -> bool:
        """Avoid mixing test-local millisecond clocks with wall-clock epoch ms."""
        epoch_floor_ms = 1_000_000_000_000
        return not started_at_ms < epoch_floor_ms <= now_ms

    def _passive_close_hedge_deadline_decision(
        self,
        pending: PendingPassiveClose,
        position: OpenPosition,
        now_ms: int,
        *,
        hedge_submit_started_at_ms: int = 0,
        hedge_submit_quantity: float = 0.0,
        reconciled: bool = False,
    ) -> dict[str, Any]:
        """V1 deadline measured from one actual hedge submit attempt."""
        unhedged_gap = max(pending.maker_fill.quantity - pending.hedge_fill.quantity, 0.0)
        attempted_quantity = max(float(hedge_submit_quantity or 0.0), 0.0)
        if attempted_quantity <= 1e-9:
            return {"hard_breached": False, "unhedged_gap": 0.0}

        started_at_ms = int(hedge_submit_started_at_ms or 0)
        if started_at_ms <= 0 or now_ms <= 0:
            return {"hard_breached": False, "unhedged_gap": unhedged_gap}
        if not self._deadline_clock_domains_match(started_at_ms, now_ms):
            return {"hard_breached": False, "unhedged_gap": unhedged_gap}

        if pending.phase_state.active_maker_leg == ActiveMakerLeg.LONG:
            hedge_venue = position.short_venue
            hedge_side = Side.BUY
            hedge_leg = "short"
        else:
            hedge_venue = position.long_venue
            hedge_side = Side.SELL
            hedge_leg = "long"

        price_hint = self._resolve_local_l2_mid(hedge_venue, position.symbol)
        if price_hint <= 0.0:
            price_hint = pending.maker_fill.average_price
        base_hard_ms = max(int(self._config.maker_hedge_deadline_ms or 0), 1)
        extension_ms = self._exit_deadline_extension_ms(
            base_hard_deadline_ms=base_hard_ms,
            notional_quote=attempted_quantity * max(price_hint, 0.0),
            quote_fresh=price_hint > 0.0,
            has_execution_progress=pending.hedge_fill.quantity > 1e-9,
            reconciled=reconciled,
        )
        hard_deadline_ms = max(base_hard_ms + extension_ms, 1)
        elapsed_ms = max(now_ms - started_at_ms, 0)
        return {
            "hard_breached": elapsed_ms > hard_deadline_ms,
            "hard_deadline_ms": hard_deadline_ms,
            "soft_deadline_ms": min(base_hard_ms // 2 + extension_ms // 2, hard_deadline_ms),
            "hedge_elapsed_ms": elapsed_ms,
            "hedge_venue": hedge_venue,
            "hedge_side": hedge_side,
            "hedge_leg": hedge_leg,
            "unhedged_gap": unhedged_gap,
            "hedge_submit_quantity": attempted_quantity,
            "price_hint": price_hint,
            "reconciled": reconciled,
        }

    async def _enforce_passive_close_hedge_submit_deadline(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
        result: HedgeDeltaResult,
        *,
        source: str,
    ) -> bool:
        """Apply the V1 deadline only after a concrete hedge submit returns."""
        if (
            result.hedge_submit_started_at_ms <= 0
            or result.hedge_submit_completed_at_ms <= 0
        ):
            return False
        deadline = self._passive_close_hedge_deadline_decision(
            pending,
            position,
            result.hedge_submit_completed_at_ms,
            hedge_submit_started_at_ms=result.hedge_submit_started_at_ms,
            hedge_submit_quantity=result.hedge_submit_quantity,
            reconciled=result.hedge_submit_reconciled,
        )
        if not deadline.get("hard_breached"):
            return False
        await self._enter_passive_close_hedge_fail_closed(
            state,
            pending,
            position,
            deadline,
            source=source,
        )
        return True

    def _passive_close_fallback_deadline_decision(
        self,
        pending: PendingPassiveClose,
        position: OpenPosition,
        now_ms: int,
    ) -> dict[str, Any]:
        """Deadline for DUAL_TAKER fallback execution itself."""
        started_at_ms = (
            pending.phase_state.phase_started_at_ms
            or pending.multi_phase_started_at_ms
            or pending.phase_state.cycle_started_at_ms
        )
        if started_at_ms <= 0 or now_ms <= 0:
            return {"hard_breached": False}
        if not self._deadline_clock_domains_match(started_at_ms, now_ms):
            return {"hard_breached": False}

        remaining_quantity = max(
            pending.current_chunk_quantity() - min(
                pending.maker_fill.quantity,
                pending.hedge_fill.quantity,
            ),
            0.0,
        )
        price_hint = max(
            self._resolve_local_l2_mid(position.long_venue, position.symbol),
            self._resolve_local_l2_mid(position.short_venue, position.symbol),
            0.0,
        )
        base_hard_ms = max(int(self._config.maker_hedge_deadline_ms or 0), 1)
        extension_ms = self._exit_deadline_extension_ms(
            base_hard_deadline_ms=base_hard_ms,
            notional_quote=remaining_quantity * price_hint,
            quote_fresh=price_hint > 0.0,
            has_execution_progress=False,
            reconciled=False,
        )
        hard_deadline_ms = max(base_hard_ms + extension_ms, 1)
        elapsed_ms = max(now_ms - started_at_ms, 0)
        return {
            "hard_breached": elapsed_ms > hard_deadline_ms,
            "hard_deadline_ms": hard_deadline_ms,
            "soft_deadline_ms": min(base_hard_ms // 2 + extension_ms // 2, hard_deadline_ms),
            "elapsed_ms": elapsed_ms,
            "remaining_quantity": remaining_quantity,
            "price_hint": price_hint,
        }

    async def _enter_passive_close_hedge_fail_closed(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
        decision: dict[str, Any],
        *,
        source: str,
    ) -> bool:
        enter_fail_closed(state)
        state.last_error = f"passive close hedge deadline breached for {pending.position_id}"
        pending.next_retry_at_ms = 0
        payload = {
            "position_id": pending.position_id,
            "symbol": position.symbol,
            "execution_kind": "exit",
            "hedge_venue": decision.get("hedge_venue").value
            if isinstance(decision.get("hedge_venue"), Venue) else str(decision.get("hedge_venue", "")),
            "hedge_side": decision.get("hedge_side").value
            if isinstance(decision.get("hedge_side"), Side) else str(decision.get("hedge_side", "")),
            "hedge_elapsed_ms": decision.get("hedge_elapsed_ms", 0),
            "deadline_ms": decision.get("hard_deadline_ms", 0),
            "soft_deadline_ms": decision.get("soft_deadline_ms", 0),
            "has_execution_progress": pending.hedge_fill.quantity > 1e-9,
            "hedge_notional_quote": decision.get("hedge_submit_quantity", 0.0)
            * max(decision.get("price_hint", 0.0), 0.0),
            "reconciled": bool(decision.get("reconciled", False)),
            "maker_order_id": str(
                getattr(pending.phase_state, "maker_order_id", "")
                or getattr(pending.maker_fill, "order_id", "")
                or ""
            ),
            "maker_client_order_id": str(
                getattr(pending.phase_state, "maker_client_order_id", "")
                or getattr(pending.maker_fill, "client_order_id", "")
                or ""
            ),
            "hedge_order_id": str(getattr(pending.hedge_fill, "order_id", "") or ""),
            "hedge_client_order_id": str(
                getattr(pending.hedge_fill, "client_order_id", "") or ""
            ),
            "source": source,
        }
        self._journal.append("execution.hedge_deadline_breached", payload)
        self._journal.append(
            "exit.passive_close_hedge_deadline_fail_closed",
            {
                **payload,
                "maker_quantity": pending.maker_fill.quantity,
                "hedge_quantity": pending.hedge_fill.quantity,
                "unhedged_gap": decision.get("unhedged_gap", 0.0),
            },
        )

        compensate = getattr(self._close_executor, "compensate_failed_full_close", None)
        if callable(compensate):
            short_legs, long_legs = self._pending_runtime_close_legs(pending)
            try:
                await compensate(
                    position=position,
                    close_reason="passive_close_hedge_deadline_breached",
                    failed_stage=(
                        pending.short_stage or "exit_short"
                        if decision.get("hedge_leg") == "short"
                        else pending.long_stage or "exit_long"
                    ),
                    failed_venue=decision.get("hedge_venue"),
                    error=RuntimeError("passive close hedge deadline breached"),
                    short_legs=short_legs,
                    long_legs=long_legs,
                    state=state,
                )
            except Exception as exc:
                self._journal.append(
                    "exit.passive_close_hedge_deadline_compensation_failed",
                    {
                        "position_id": pending.position_id,
                        "symbol": position.symbol,
                        "error": str(exc),
                        "source": source,
                    },
                )
                return False
            if await self._clear_if_live_flat(
                state,
                pending,
                position,
                source="passive_close_hedge_deadline_compensated_flat",
                extra={"source": source},
            ):
                return True
            return False

        self._journal.append(
            "exit.passive_close_hedge_deadline_compensation_unavailable",
            {
                "position_id": pending.position_id,
                "symbol": position.symbol,
                "source": source,
            },
        )
        return False

    def _hedge_truth_gap_extra(
        self,
        pending: PendingPassiveClose,
        position: OpenPosition,
        result: HedgeDeltaResult,
    ) -> dict[str, Any]:
        if pending.phase_state.active_maker_leg == ActiveMakerLeg.LONG:
            hedge_venue = position.short_venue
            hedge_leg = "short"
        else:
            hedge_venue = position.long_venue
            hedge_leg = "long"

        extra: dict[str, Any] = {
            "accepted_order_truth_gap": True,
            "hedge_venue": hedge_venue.value,
            "hedge_leg": hedge_leg,
            "flattened_venue": hedge_venue.value,
            "flattened_quantity": result.requested,
        }
        if result.accepted_order_id:
            extra["accepted_order_id"] = result.accepted_order_id
            extra["accepted_order_ids"] = [result.accepted_order_id]
        if result.accepted_client_order_id:
            extra["accepted_client_order_id"] = result.accepted_client_order_id
            extra["accepted_client_order_ids"] = [result.accepted_client_order_id]
        return extra

    def _active_hedge_truth_gap_reconciliation(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        *,
        hedge_venue: Venue,
        hedge_leg: str,
    ) -> dict[str, Any] | None:
        for reconciliation in getattr(state, "pending_close_reconciliations", []):
            if not isinstance(reconciliation, dict):
                continue
            if str(reconciliation.get("kind") or "") != "accepted_order_truth_gap":
                continue
            if str(reconciliation.get("position_id") or "") != pending.position_id:
                continue
            venue_match = str(reconciliation.get("venue") or "") == hedge_venue.value
            leg_match = str(reconciliation.get("leg") or "") == hedge_leg
            if venue_match and leg_match:
                return reconciliation

            leg_records = (
                reconciliation.get("long_legs", [])
                if hedge_leg == "long"
                else reconciliation.get("short_legs", [])
            )
            if any(
                isinstance(record, dict)
                and str(record.get("venue") or "") == hedge_venue.value
                for record in leg_records
            ):
                return reconciliation
        return None

    @staticmethod
    def _accepted_order_truth_gap_identity(
        reconciliation: dict[str, Any],
        *,
        hedge_venue: Venue,
        hedge_leg: str,
    ) -> tuple[str, str]:
        payload = reconciliation.get("original_payload")
        if not isinstance(payload, dict):
            payload = {}
        order_id = str(payload.get("accepted_order_id") or "")
        client_order_id = str(payload.get("accepted_client_order_id") or "")
        leg_records = (
            reconciliation.get("long_legs", [])
            if hedge_leg == "long"
            else reconciliation.get("short_legs", [])
        )
        for record in leg_records:
            if not isinstance(record, dict):
                continue
            if str(record.get("venue") or "") != hedge_venue.value:
                continue
            order_id = order_id or str(record.get("order_id") or "")
            client_order_id = client_order_id or str(record.get("client_order_id") or "")
        return order_id, client_order_id

    async def _handle_hedge_truth_gap_result(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
        result: HedgeDeltaResult,
        *,
        source: str,
    ) -> bool:
        extra = self._hedge_truth_gap_extra(pending, position, result)
        if await self._clear_if_live_flat(
            state,
            pending,
            position,
            source=f"{source}_live_flat",
            extra=extra,
        ):
            return True

        pending.next_retry_at_ms = self._now_ms() + 1_000
        self._journal.append(
            "exit.passive_close_hedge_ack_live_truth_pending",
            {
                "position_id": pending.position_id,
                "symbol": position.symbol,
                "requested": result.requested,
                "residual": result.residual,
                "accepted_order_id": result.accepted_order_id,
                "accepted_client_order_id": result.accepted_client_order_id,
                "decision": "retain_pending",
                "next_action": "retry_order_position_open_order_reconciliation",
                "next_retry_ms": 1_000,
                "source": source,
            },
        )
        return False

    def _enter_passive_close_execution_fail_closed(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
        decision: dict[str, Any],
        *,
        source: str,
        error: str,
    ) -> None:
        enter_fail_closed(state)
        state.last_error = f"passive close fallback deadline breached for {pending.position_id}: {error}"
        pending.next_retry_at_ms = 0
        self._journal.append(
            "execution.close_deadline_breached",
            {
                "position_id": pending.position_id,
                "symbol": position.symbol,
                "execution_kind": "exit",
                "elapsed_ms": decision.get("elapsed_ms", 0),
                "deadline_ms": decision.get("hard_deadline_ms", 0),
                "soft_deadline_ms": decision.get("soft_deadline_ms", 0),
                "remaining_quantity": decision.get("remaining_quantity", 0.0),
                "source": source,
                "error": error,
            },
        )

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

        if self._is_recovered_position(position):
            probe_pending = PendingPassiveClose(
                position_id=pid,
                reason=reason,
                position_snapshot=position,
                short_stage=short_stage or "exit_short",
                long_stage=long_stage or "exit_long",
                target_quantity=target,
                chunk_quantities=[target],
            )
            if await self._clear_if_live_flat(
                state,
                probe_pending,
                position,
                source="recovered_passive_close_start_flat_probe",
            ):
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
        # The pending close must be recoverable before any later drive cycle
        # can send an exchange request.  A periodic snapshot is not a durable
        # boundary for this owner: a process can stop after a venue accepts an
        # order but before the next snapshot is written.
        self._journal.append_critical(
            self._now_ms(),
            "exit.passive_close_registered",
            {
                "position_id": pid,
                "reason": reason,
                "pending_passive_close": self._pending_passive_close_recovery_payload(
                    pending,
                    position,
                ),
            },
        )
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

            if not maker_order_id and not maker_client_id:
                live_truth_resolution = await self._resolve_flat_maker_leg_from_live_truth(
                    state,
                    pending,
                    position,
                    maker_leg_label=maker_leg_label,
                )
                if live_truth_resolution == PassiveCloseLiveTruthResolution.CLEARED:
                    return True
                if live_truth_resolution == PassiveCloseLiveTruthResolution.STOP_RETRY:
                    return False

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
            if progress is None and self._last_maker_progress_error:
                error_payload = dict(self._last_maker_progress_error)
                self._journal.append(
                    "exit.passive_close_order_truth_unavailable",
                    {
                        "position_id": position_id,
                        "symbol": position.symbol,
                        "maker_venue": maker_venue.value,
                        "maker_leg": maker_leg_label,
                        "phase": pending.phase_state.phase.value,
                        "zero_fill_cycles": pending.phase_state.zero_fill_cycles_in_phase,
                        "source": "poll_maker_progress",
                        "decision": "retain_pending",
                        "next_action": "retry_progress_poll",
                        **error_payload,
                    },
                )
                pending.next_retry_at_ms = now_ms + PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS
                return False

            if progress is None and maker_client_id and not maker_order_id:
                # A pre-submit CID survived restart but the venue has not
                # supplied order truth yet.  Do not submit a second maker
                # order: this CID may name an accepted order whose ACK was
                # lost with the previous process.
                self._journal.append(
                    "exit.passive_close_order_truth_unavailable",
                    {
                        "position_id": position_id,
                        "symbol": position.symbol,
                        "maker_venue": maker_venue.value,
                        "maker_leg": maker_leg_label,
                        "maker_order_id": "",
                        "maker_client_order_id": maker_client_id,
                        "phase": pending.phase_state.phase.value,
                        "source": "pre_submit_close_order_intent",
                        "decision": "retain_pending_without_resubmit",
                        "next_action": "retry_progress_poll_by_client_order_id",
                    },
                )
                pending.next_retry_at_ms = (
                    now_ms + PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS
                )
                return False

            # Apply progress to pending state
            if progress is not None:
                pending = state.pending_passive_closes.get(position_id)
                if pending is None:
                    return True

                # A zero-fill terminal maker response is not enough evidence
                # to discard its identity.  Confirm executions once before a
                # phase fallback; this protects against delayed Bybit fills.
                if (
                    progress.state in (
                        PassiveOrderState.FILLED,
                        PassiveOrderState.CANCELED,
                        PassiveOrderState.REJECTED,
                        PassiveOrderState.EXPIRED,
                    )
                    and progress.cumulative_quantity <= 1e-9
                    and pending.maker_fill.quantity <= 1e-9
                ):
                    terminal_state = progress.state.value
                    terminal_truth = None
                    terminal_truth_error = ""
                    terminal_truth_error_type = ""
                    try:
                        terminal_truth = await adapter.fetch_order_fill_reconciliation(
                            position.symbol,
                            progress.order_id or maker_order_id,
                            progress.client_order_id or maker_client_id,
                        )
                    except Exception as exc:
                        terminal_truth_error = str(exc)
                        terminal_truth_error_type = type(exc).__name__
                    if terminal_truth is None:
                        if not self._terminal_zero_fill_status_is_authoritative(progress):
                            next_retry_at_ms = (
                                now_ms + PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS
                            )
                            self._append_terminal_zero_fill_truth_unavailable(
                                pending=pending,
                                position=position,
                                maker_venue=maker_venue,
                                progress=progress,
                                maker_order_id=maker_order_id,
                                maker_client_id=maker_client_id,
                                terminal_truth_error=terminal_truth_error,
                                terminal_truth_error_type=terminal_truth_error_type,
                                next_retry_at_ms=next_retry_at_ms,
                            )
                            pending.next_retry_at_ms = next_retry_at_ms
                            return False
                        authoritative_payload = {
                            "position_id": position_id,
                            "symbol": position.symbol,
                            "maker_venue": maker_venue.value,
                            "maker_order_id": progress.order_id or maker_order_id,
                            "maker_client_order_id": progress.client_order_id or maker_client_id,
                            "state": progress.state.value,
                            "source": "terminal_order_status",
                            "decision": "advance_terminal_no_fill",
                        }
                        if terminal_truth_error:
                            authoritative_payload["execution_truth_error"] = terminal_truth_error
                        self._journal.append(
                            "exit.passive_close_terminal_zero_fill_status_authoritative",
                            authoritative_payload,
                        )
                        terminal_truth = OrderFillReconciliation(
                            venue=maker_venue,
                            symbol=position.symbol,
                            side=maker_side,
                            quantity=0.0,
                            average_price=0.0,
                            order_id=progress.order_id or maker_order_id,
                            client_order_id=progress.client_order_id or maker_client_id,
                            metadata={"source": "terminal_order_status"},
                        )
                    if not isinstance(terminal_truth, OrderFillReconciliation):
                        # Adapter mocks and malformed implementations must not
                        # be interpreted as a positive fill merely because a
                        # dynamic object exposes a truthy ``quantity`` field.
                        # Only the typed execution-truth contract can change a
                        # terminal maker order into a filled close leg.
                        self._journal.append(
                            "exit.passive_close_terminal_zero_fill_truth_unavailable",
                            {
                                "position_id": position_id,
                                "symbol": position.symbol,
                                "maker_venue": maker_venue.value,
                                "maker_order_id": progress.order_id or maker_order_id,
                                "maker_client_order_id": progress.client_order_id or maker_client_id,
                                "state": progress.state.value,
                                "reason": "invalid_reconciliation_type",
                                "truth_type": type(terminal_truth).__name__,
                                "decision": "retain_pending",
                            },
                        )
                        pending.next_retry_at_ms = now_ms + PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS
                        return False
                    terminal_quantity = max(
                        float(getattr(terminal_truth, "quantity", 0.0) or 0.0),
                        0.0,
                    )
                    if (
                        terminal_quantity <= 1e-9
                        and progress.state == PassiveOrderState.FILLED
                    ):
                        # A terminal order status that says FILLED while its
                        # execution query says zero is internally inconsistent.
                        # Do not turn that contradiction into a new close leg.
                        self._journal.append(
                            "exit.passive_close_terminal_zero_fill_truth_inconsistent",
                            {
                                "position_id": position_id,
                                "symbol": position.symbol,
                                "maker_venue": maker_venue.value,
                                "maker_order_id": progress.order_id or maker_order_id,
                                "maker_client_order_id": progress.client_order_id or maker_client_id,
                                "state": progress.state.value,
                                "decision": "retain_pending",
                            },
                        )
                        pending.next_retry_at_ms = now_ms + PASSIVE_CLOSE_PROGRESS_RETRY_WINDOW_MS
                        return False
                    if terminal_quantity > 1e-9:
                        progress = PassiveOrderProgress(
                            venue=maker_venue,
                            symbol=position.symbol,
                            side=maker_side,
                            order_id=(
                                str(getattr(terminal_truth, "order_id", "") or "")
                                or progress.order_id
                                or maker_order_id
                            ),
                            client_order_id=(
                                str(getattr(terminal_truth, "client_order_id", "") or "")
                                or progress.client_order_id
                                or maker_client_id
                            ),
                            cumulative_quantity=terminal_quantity,
                            average_price=float(
                                getattr(terminal_truth, "average_price", 0.0) or 0.0
                            ),
                            fee_quote=(
                                float(getattr(terminal_truth, "fee_quote"))
                                if self._fee_evidence_complete(
                                    getattr(terminal_truth, "fee_quote", None)
                                )
                                else None
                            ),
                            last_fill_time_ms=int(
                                getattr(terminal_truth, "filled_at_ms", 0) or 0
                            ),
                            state=PassiveOrderState.FILLED,
                            observed_at_ms=now_ms,
                        )
                        self._journal.append(
                            "exit.passive_close_terminal_zero_fill_reconciled_fill",
                            {
                                "position_id": position_id,
                                "symbol": position.symbol,
                                "maker_venue": maker_venue.value,
                                "maker_leg": maker_leg_label,
                                "terminal_state": terminal_state,
                                "maker_order_id": progress.order_id,
                                "maker_client_order_id": progress.client_order_id,
                                "reconciled_quantity": terminal_quantity,
                                "truth_order_id": str(
                                    getattr(terminal_truth, "order_id", "") or ""
                                ),
                                "truth_client_order_id": str(
                                    getattr(
                                        terminal_truth, "client_order_id", ""
                                    )
                                    or ""
                                ),
                                "truth_filled_at_ms": int(
                                    getattr(terminal_truth, "filled_at_ms", 0) or 0
                                ),
                                "decision": "treat_terminal_maker_as_filled",
                            },
                        )
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
                            result = await self._submit_hedge_for_delta(
                                state, pending, position, unhedged_gap,
                                maker_terminal=True,
                            )
                        elif maker_fill_delta > 1e-9:
                            result = await self._submit_hedge_for_delta(
                                state, pending, position, maker_fill_delta,
                                maker_terminal=True,
                            )
                        else:
                            result = HedgeDeltaResult(requested=0.0, filled=0.0, residual=0.0, success=True)
                        # Re-read pending after hedge
                        pending = state.pending_passive_closes.get(position_id)
                        if pending is None:
                            return True
                        if await self._enforce_passive_close_hedge_submit_deadline(
                            state,
                            pending,
                            position,
                            result,
                            source="terminal_maker_filled_hedge_submit",
                        ):
                            return False
                        if result.truth_gap:
                            return await self._handle_hedge_truth_gap_result(
                                state,
                                pending,
                                position,
                                result,
                                source="terminal_maker_filled_hedge_ack",
                            )
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
                            chunk_quantity = pending.current_chunk_quantity()
                            if pending.maker_fill.quantity + 1e-9 >= chunk_quantity:
                                # V1 exit.rs:1826 — maker met chunk → normal advance
                                if await self._advance_chunk(state, pending):
                                    continue
                                # advance blocked (hedge check) — will retry
                                return False
                            # V1: maker_fill < chunk but maker terminal + hedge matched.
                            # V1 continues cycling → phase exhaustion → DUAL_TAKER.
                            # V2: try live flat first, then escalate to DUAL_TAKER.
                            self._journal.append(
                                "exit.passive_close_maker_filled_under_chunk",
                                {
                                    "position_id": position_id,
                                    "symbol": position.symbol,
                                    "long_venue": position.long_venue.value,
                                    "short_venue": position.short_venue.value,
                                    "phase": pending.phase_state.phase.value,
                                    "chunk_quantity": chunk_quantity,
                                    "maker_fill": pending.maker_fill.quantity,
                                    "hedge_fill": pending.hedge_fill.quantity,
                                    "chunk_index": pending.active_chunk_index,
                                    "decision": "try_live_flat_then_dual_taker",
                                    "reason": "maker terminal FILLED but under-filled chunk",
                                    "source": "passive_close_filled_handler",
                                },
                            )
                            if await self._clear_if_live_flat(
                                state, pending, position,
                                source="passive_close_maker_filled_under_chunk_live_flat",
                            ):
                                return True  # cleared via live flat truth
                            # Escalate to DUAL_TAKER (V1: phase exhaustion → DUAL_TAKER)
                            pending.phase_state.phase = PassiveExecutionPhase.DUAL_TAKER
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
                hedge_side_for_notional = (
                    Side.BUY if maker_leg == ActiveMakerLeg.LONG else Side.SELL
                )
                hedge_price_hint, hedge_price_source = await self._resolve_hedge_reference_price(
                    hedge_venue_for_notional,
                    position.symbol,
                    hedge_side_for_notional,
                    hedge_price_hint,
                )
                hedge_min_notional, hedge_min_notional_source = await self._resolve_hedge_min_notional_quote(
                    hedge_venue_for_notional,
                    position.symbol,
                )
                min_notional_violation = self._check_hedge_min_notional(
                    hedge_venue_for_notional, position.symbol,
                    hedge_side_for_notional,
                    unhedged_gap, hedge_price_hint,
                    price_source=hedge_price_source,
                    min_notional_quote=hedge_min_notional,
                    min_notional_source=hedge_min_notional_source,
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
                result = await self._submit_hedge_for_delta(
                    state, pending, position, unhedged_gap,
                    maker_terminal=maker_terminal,
                )
                pending = state.pending_passive_closes.get(position_id)
                if pending is None:
                    return True
                if await self._enforce_passive_close_hedge_submit_deadline(
                    state,
                    pending,
                    position,
                    result,
                    source="drive_unhedged_gap_submit",
                ):
                    return False
                if not result.success:
                    if result.truth_gap:
                        return await self._handle_hedge_truth_gap_result(
                            state,
                            pending,
                            position,
                            result,
                            source="passive_close_delta_hedge_ack",
                        )

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
        *,
        post_only_requote_attempt: int = 0,
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
        self._claim_close_order_intent(
            pending,
            position,
            request,
            leg_label=maker_leg_label,
            operation="submit_passive_order",
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
            if _is_initial_passive_requote_error(maker_venue, e):
                next_requote_attempt = post_only_requote_attempt + 1
                wait_ms = _passive_close_post_only_retry_wait_ms(
                    e,
                    post_only_requote_attempt,
                )
                self._journal.append(
                    "execution.passive_close_requote_retry",
                    {
                        "position_id": position.position_id,
                        "symbol": position.symbol,
                        "maker_venue": maker_venue.value,
                        "maker_leg": maker_leg_label,
                        "phase": pending.phase_state.phase.value,
                        "cycle_attempt": pending.phase_state.cycle_attempt,
                        "attempt": next_requote_attempt,
                        "wait_ms": wait_ms,
                        "error": str(e),
                        "exchange_error": evidence.to_dict(),
                        "remaining_quantity": chunk_quantity,
                        "price_hint": aligned_price,
                    },
                )
                await asyncio.sleep(wait_ms / 1000.0)
                if next_requote_attempt < _passive_close_post_only_attempt_limit():
                    requote_price_hint = self._post_only_requote_price_hint(
                        maker_venue,
                        position.symbol,
                        maker_side,
                        next_requote_attempt,
                        fallback_price=price_hint,
                    )
                    return await self._submit_maker_order(
                        state,
                        pending,
                        position,
                        maker_venue,
                        maker_side,
                        maker_leg_label,
                        requote_price_hint,
                        chunk_quantity,
                        post_only_requote_attempt=next_requote_attempt,
                    )

                self._journal.append(
                    "exit.passive_close_maker_requote_exhausted",
                    {
                        "position_id": position.position_id,
                        "symbol": position.symbol,
                        "maker_venue": maker_venue.value,
                        "maker_leg": maker_leg_label,
                        "attempts": _passive_close_post_only_attempt_limit(),
                        "error": str(e),
                        "exchange_error": evidence.to_dict(),
                        "decision": "dual_taker",
                    },
                )
                pending.phase_state.phase = PassiveExecutionPhase.DUAL_TAKER
                return False

            if e.is_rejected:
                if _is_bybit_terminal_zero_qty_reduce_only_error(
                    e,
                    venue=maker_venue,
                    evidence=evidence,
                    request_context=req_ctx,
                ):
                    self._journal.append(
                        "exit.passive_close_terminal_zero_qty_reduce_only_evidence",
                        {
                            "position_id": position.position_id,
                            "symbol": position.symbol,
                            "venue": maker_venue.value,
                            "maker_leg": maker_leg_label,
                            "error": str(e),
                            "exchange_error": evidence.to_dict(),
                            "request_context": req_ctx.to_dict(),
                            "evidence_completeness": evidence.evidence_completeness,
                            "decision": "probe_live_truth",
                            "next_action": "v1_live_truth_closure",
                        },
                    )
                    pending.phase_state.phase = PassiveExecutionPhase.DUAL_TAKER
                    live_truth_resolution = await self._resolve_flat_maker_leg_from_live_truth(
                        state,
                        pending,
                        position,
                        maker_leg_label=maker_leg_label,
                    )
                    if live_truth_resolution == PassiveCloseLiveTruthResolution.CLEARED:
                        return True
                    if live_truth_resolution == PassiveCloseLiveTruthResolution.STOP_RETRY:
                        return False
                    pending.next_retry_at_ms = self._now_ms() + 5_000
                    self._journal.append(
                        "exit.passive_close_maker_submit_error",
                        {
                            "position_id": position.position_id,
                            "venue": maker_venue.value,
                            "error": str(e),
                            "exchange_error": evidence.to_dict(),
                            "request_context": req_ctx.to_dict(),
                            "evidence_completeness": evidence.evidence_completeness,
                            "terminal_zero_qty_reduce_only": True,
                            "decision": "retain_pending",
                            "reason": "terminal_zero_qty_live_truth_not_flat",
                        },
                    )
                    return False
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
        pending.phase_state.maker_client_order_id = ack.client_order_id or maker_cid
        pending.phase_state.maker_resting_limit_price = aligned_price
        pending.phase_state.maker_resting_since_ms = ack.accepted_at_ms

        self._journal.append(
            "exit.passive_close_maker_submitted",
            {
                "position_id": position.position_id,
                "maker_venue": maker_venue.value,
                "maker_leg": maker_leg_label,
                "order_id": ack.order_id,
                "client_order_id": pending.phase_state.maker_client_order_id,
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
        self._last_maker_progress_error = None
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
        except Exception as error:
            self._last_maker_progress_error = {
                "symbol": symbol,
                "order_id": order_id,
                "client_order_id": client_order_id,
                "side": side.value,
                "error": str(error),
                "error_type": type(error).__name__,
            }
            return None

    def _claim_close_order_intent(
        self,
        pending: PendingPassiveClose,
        position: OpenPosition,
        request: OrderRequest,
        *,
        leg_label: str,
        operation: str,
    ) -> int:
        """Persist a close-order lookup key before its first network await.

        A client order ID is the recovery handle for the ambiguous interval
        where an exchange may have accepted the close but this process has not
        received an acknowledgement yet.  Store it in both durable journal
        evidence and the pending-close snapshot before submitting so a restart
        can query the venue instead of inferring accounting from flat position
        truth alone.
        """
        client_order_id = str(request.client_order_id or "")
        if not client_order_id:
            raise ValueError("close order intent requires client_order_id")
        if leg_label not in {"long", "short"}:
            raise ValueError(f"unknown close leg: {leg_label}")

        legs = pending.long_legs if leg_label == "long" else pending.short_legs
        submit_started_at_ms = self._now_ms()
        if not any(
            leg.client_order_id == client_order_id
            for leg in legs
        ):
            legs.append(
                PersistedCloseExecutionLeg(
                    fill=None,
                    client_order_id=client_order_id,
                    submit_started_at_ms=submit_started_at_ms,
                )
            )

        # The driver uses this field as its active maker lookup handle.  It
        # must be present before submit, not only after the ACK, so restart
        # polls this CID instead of creating a second maker order.
        if operation == "submit_passive_order":
            pending.phase_state.maker_client_order_id = client_order_id

        self._journal.append_critical(
            submit_started_at_ms,
            "exit.close_order_intent_claimed",
            {
                "position_id": position.position_id,
                "symbol": position.symbol,
                "venue": request.venue.value,
                "leg": leg_label,
                "operation": operation,
                "client_order_id": client_order_id,
                "side": request.side.value,
                "quantity": request.quantity,
                "reduce_only": request.reduce_only,
                "order_truth_required": True,
                "submit_started_at_ms": submit_started_at_ms,
                "pending_passive_close": self._pending_passive_close_recovery_payload(
                    pending,
                    position,
                ),
            },
        )
        return submit_started_at_ms

    @staticmethod
    def _serialize_close_order_fill(fill: OrderFill | None) -> dict[str, Any] | None:
        if fill is None:
            return None
        return {
            "venue": fill.venue.value,
            "symbol": fill.symbol,
            "side": fill.side.value,
            "quantity": fill.quantity,
            "price": fill.price,
            "order_id": fill.order_id,
            "client_order_id": fill.client_order_id,
            "fee_quote": fill.fee_quote,
            "filled_at_ms": fill.filled_at_ms,
        }

    @staticmethod
    def _fee_evidence_complete(fee_quote: float | None) -> bool:
        try:
            return math.isfinite(float(fee_quote)) and float(fee_quote) >= 0.0
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _persisted_leg_fill(leg: PersistedCloseExecutionLeg) -> OrderFill | None:
        """Expose no numeric fee when this persisted leg lacks evidence."""
        if leg.fill is None:
            return None
        if leg.fee_evidence_complete is False:
            return replace(leg.fill, fee_quote=None)
        return leg.fill

    def _pending_passive_close_recovery_payload(
        self,
        pending: PendingPassiveClose,
        position: OpenPosition,
    ) -> dict[str, Any]:
        """Return the journal-replay owner state for an in-flight close."""
        phase = pending.phase_state

        def serialize_leg(leg: PersistedCloseExecutionLeg) -> dict[str, Any]:
            fee_evidence_complete = leg.fee_evidence_complete
            if fee_evidence_complete is None:
                fee_evidence_complete = self._fee_evidence_complete(
                    leg.fill.fee_quote if leg.fill is not None else None
                )
            return {
                "fill": self._serialize_close_order_fill(leg.fill),
                "fee_evidence_complete": fee_evidence_complete,
                "client_order_id": leg.client_order_id,
                "submit_started_at_ms": leg.submit_started_at_ms,
                "latency_ms": leg.latency_ms,
            }

        def serialize_fill(fill: PendingPassiveLegFill) -> dict[str, Any]:
            return {
                "quantity": fill.quantity,
                "average_price": fill.average_price,
                "fee_quote": fill.fee_quote,
                "fee_evidence_complete": fill.fee_evidence_complete,
                "last_fill_time_ms": fill.last_fill_time_ms,
                "order_id": fill.order_id,
                "client_order_id": fill.client_order_id,
            }

        return {
            "position_id": pending.position_id,
            "reason": pending.reason,
            "position_snapshot": self._position_snapshot_for_close_reconciliation(
                pending.position_snapshot or position
            ),
            "short_stage": pending.short_stage,
            "long_stage": pending.long_stage,
            "target_quantity": pending.target_quantity,
            "max_slippage_bps": pending.max_slippage_bps,
            "chunk_quantities": list(pending.chunk_quantities),
            "active_chunk_index": pending.active_chunk_index,
            "phase_state": {
                "phase": phase.phase.value,
                "preferred_maker_leg": phase.preferred_maker_leg.value,
                "active_maker_leg": phase.active_maker_leg.value,
                "phase_started_at_ms": phase.phase_started_at_ms,
                "cycle_attempt": phase.cycle_attempt,
                "cycle_started_at_ms": phase.cycle_started_at_ms,
                "zero_fill_cycles_in_phase": phase.zero_fill_cycles_in_phase,
                "maker_submit_attempt": phase.maker_submit_attempt,
                "maker_submit_consecutive_failures": (
                    phase.maker_submit_consecutive_failures
                ),
                "missing_l2_tick_consecutive_count": (
                    phase.missing_l2_tick_consecutive_count
                ),
                "maker_order_id": phase.maker_order_id,
                "maker_client_order_id": phase.maker_client_order_id,
                "maker_resting_limit_price": phase.maker_resting_limit_price,
                "maker_resting_since_ms": phase.maker_resting_since_ms,
            },
            "maker_fill": serialize_fill(pending.maker_fill),
            "hedge_fill": serialize_fill(pending.hedge_fill),
            "long_legs": [serialize_leg(leg) for leg in pending.long_legs],
            "short_legs": [serialize_leg(leg) for leg in pending.short_legs],
            "passive_manager_runtimes": {
                str(venue): runtime.to_dict()
                for venue, runtime in pending.passive_manager_runtimes.items()
            },
            "small_fill_min_notional_attempts": (
                pending.small_fill_min_notional_attempts
            ),
            "last_small_fill_missing_quantity": (
                pending.last_small_fill_missing_quantity
            ),
            "small_fill_buffer_started_at_ms": pending.small_fill_buffer_started_at_ms,
            "next_retry_at_ms": pending.next_retry_at_ms,
            "multi_phase_started_at_ms": pending.multi_phase_started_at_ms,
            "created_cycle": pending.created_cycle,
            "ops_count_this_window": pending.ops_count_this_window,
            "ops_window_started_at_ms": pending.ops_window_started_at_ms,
        }

    @staticmethod
    def _record_close_execution_leg(
        pending: PendingPassiveClose,
        *,
        leg_label: str,
        fill: OrderFill,
        client_order_id: str,
        submitted_at_ms: int,
    ) -> None:
        """Attach execution truth to its pre-submit intent without duplicates."""
        legs = pending.long_legs if leg_label == "long" else pending.short_legs
        for leg in legs:
            if leg.client_order_id == client_order_id and leg.fill is None:
                leg.fill = fill
                leg.fee_evidence_complete = PassiveCloseExecutor._fee_evidence_complete(
                    fill.fee_quote
                )
                if not leg.submit_started_at_ms:
                    leg.submit_started_at_ms = submitted_at_ms
                return
        for leg in legs:
            existing = leg.fill
            if existing is None:
                continue
            existing_order_id = str(existing.order_id or "")
            fill_order_id = str(fill.order_id or "")
            same_identity = (
                existing.venue == fill.venue
                and (
                    (bool(fill_order_id) and existing_order_id == fill_order_id)
                    or (
                        not fill_order_id
                        and bool(client_order_id)
                        and leg.client_order_id == client_order_id
                    )
                )
            )
            if not (
                same_identity
                and abs(existing.quantity - fill.quantity) <= 1e-12
                and abs(existing.price - fill.price) <= 1e-12
                and int(existing.filled_at_ms or 0) == int(fill.filled_at_ms or 0)
            ):
                continue
            if existing.fee_quote == fill.fee_quote:
                return
            # An exact later observation can supply fee evidence that the
            # earlier progress update lacked; update rather than duplicate.
            leg.fill = fill
            leg.fee_evidence_complete = PassiveCloseExecutor._fee_evidence_complete(
                fill.fee_quote
            )
            return
        legs.append(
            PersistedCloseExecutionLeg(
                fill=fill,
                fee_evidence_complete=PassiveCloseExecutor._fee_evidence_complete(
                    fill.fee_quote
                ),
                client_order_id=client_order_id,
                submit_started_at_ms=submitted_at_ms,
            )
        )

    def _apply_maker_progress(
        self,
        pending: PendingPassiveClose,
        progress: PassiveOrderProgress,
        now_ms: int,
    ) -> None:
        """Update pending state from maker progress poll result."""
        if progress.cumulative_quantity > pending.maker_fill.quantity + 1e-9:
            prior_quantity = pending.maker_fill.quantity
            prior_notional = prior_quantity * pending.maker_fill.average_price
            prior_fee = pending.maker_fill.fee_quote
            prior_fee_evidence_complete = pending.maker_fill.fee_evidence_complete
            delta_qty = progress.cumulative_quantity - prior_quantity
            progress_client_order_id = (
                progress.client_order_id
                or pending.phase_state.maker_client_order_id
            )
            # V1's progress average_price and fee_quote are cumulative order
            # values.  Store the latest cumulative observation and derive the
            # execution-leg delta from the previous cumulative total; treating
            # either field as incremental double-counts partial fills.
            cumulative_price = (
                float(progress.average_price)
                if isinstance(progress.average_price, (int, float))
                and math.isfinite(float(progress.average_price))
                and float(progress.average_price) > 0.0
                else pending.maker_fill.average_price
            )
            cumulative_notional = progress.cumulative_quantity * cumulative_price
            delta_notional = cumulative_notional - prior_notional
            delta_price = (
                delta_notional / delta_qty
                if delta_notional > 0.0 and delta_qty > 1e-12
                else cumulative_price
            )
            pending.maker_fill.average_price = cumulative_price
            pending.maker_fill.quantity = progress.cumulative_quantity
            try:
                progress_fee = float(progress.fee_quote)
            except (TypeError, ValueError):
                progress_fee = None
            if (
                progress_fee is None
                or not math.isfinite(progress_fee)
                or progress_fee < 0.0
            ):
                pending.maker_fill.fee_evidence_complete = False
                delta_fee = None
            else:
                pending.maker_fill.fee_quote = progress_fee
                pending.maker_fill.fee_evidence_complete = True
                if prior_quantity <= 1e-12:
                    delta_fee = progress_fee
                elif prior_fee_evidence_complete and progress_fee + 1e-12 >= prior_fee:
                    delta_fee = max(progress_fee - prior_fee, 0.0)
                else:
                    # The latest cumulative fee is authoritative for this
                    # order, but cannot truthfully split a previously unknown
                    # amount across persisted delta legs.  Final accounting
                    # therefore stays on the existing reconciliation path.
                    delta_fee = None
            pending.maker_fill.last_fill_time_ms = progress.last_fill_time_ms
            pending.maker_fill.order_id = progress.order_id
            pending.maker_fill.client_order_id = progress_client_order_id

            # Persist maker delta fill as a close execution leg
            maker_leg = pending.phase_state.active_maker_leg
            maker_fill = OrderFill(
                venue=progress.venue,
                symbol=progress.symbol,
                side=progress.side,
                quantity=delta_qty,
                price=delta_price,
                order_id=progress.order_id,
                client_order_id=progress_client_order_id,
                fee_quote=delta_fee,
                filled_at_ms=progress.last_fill_time_ms or now_ms,
            )
            self._record_close_execution_leg(
                pending,
                leg_label=("long" if maker_leg == ActiveMakerLeg.LONG else "short"),
                fill=maker_fill,
                client_order_id=progress_client_order_id,
                submitted_at_ms=now_ms,
            )

            self._journal.append(
                "exit.passive_close_maker_progress",
                {
                    "position_id": pending.position_id,
                    "cumulative_quantity": progress.cumulative_quantity,
                    "average_price": progress.average_price,
                    "delta_quantity": delta_qty,
                    "fee_evidence_complete": pending.maker_fill.fee_evidence_complete,
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
        *,
        maker_terminal: bool = False,
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

        price_hint, price_source = await self._resolve_hedge_reference_price(
            hedge_venue, position.symbol, hedge_side, price_hint,
        )
        hedge_price = price_hint if price_hint > 0 else None
        adapter = self._adapter(hedge_venue)
        if adapter is None:
            return HedgeDeltaResult(
                requested=delta, filled=0.0, residual=delta, success=False,
                error=f"no adapter for {hedge_venue.value}",
            )

        try:
            normalized_delta = float(await adapter.normalize_quantity(position.symbol, delta))
        except Exception as e:
            self._journal.append(
                "exit.passive_close_hedge_normalize_failed",
                {
                    "position_id": position.position_id,
                    "hedge_venue": hedge_venue.value,
                    "hedge_leg": hedge_leg_label,
                    "requested": delta,
                    "error": str(e),
                },
            )
            return HedgeDeltaResult(
                requested=delta, filled=0.0, residual=delta, success=False,
                error=f"normalize_quantity_failed: {e}",
            )

        min_notional_quote, min_notional_source = await self._resolve_hedge_min_notional_quote(
            hedge_venue,
            position.symbol,
        )
        min_notional_violation = self._check_hedge_min_notional(
            hedge_venue, position.symbol, hedge_side,
            normalized_delta, price_hint,
            price_source=price_source,
            min_notional_quote=min_notional_quote,
            min_notional_source=min_notional_source,
        )
        dust_reason = ""
        if normalized_delta <= 1e-12:
            dust_reason = "normalized_quantity_zero"
        elif min_notional_violation is not None:
            dust_reason = str(min_notional_violation.get("reason") or "min_notional_rejected")

        if dust_reason:
            rule_evidence = await self._hedge_rule_diagnostic_payload(
                hedge_venue,
                position.symbol,
                min_notional_source=min_notional_source,
            )
            payload = {
                "position_id": position.position_id,
                "symbol": position.symbol,
                "hedge_venue": hedge_venue.value,
                "hedge_leg": hedge_leg_label,
                "requested": delta,
                "normalized_quantity": normalized_delta,
                "price_hint": price_hint,
                "price_source": price_source,
                "reason": dust_reason,
                "maker_terminal": maker_terminal,
                **rule_evidence,
            }
            if min_notional_violation is not None:
                payload.update({
                    "leg_notional_quote": min_notional_violation["leg_notional"],
                    "venue_min_notional_quote": min_notional_violation["min_notional"],
                })
            self._journal.append("exit.passive_close_hedge_dust_aborted", payload)

            if await self._clear_if_live_flat(
                state,
                pending,
                position,
                source="passive_close_hedge_dust_flat_probe",
                extra={
                    "hedge_venue": hedge_venue.value,
                    "requested": delta,
                    "normalized_quantity": normalized_delta,
                    "reason": dust_reason,
                },
            ):
                return HedgeDeltaResult(
                    requested=delta, filled=0.0, residual=0.0, success=True,
                    error=None,
                )

            if maker_terminal:
                leg_notional = 0.0
                min_notional = 0.0
                if min_notional_violation is not None:
                    leg_notional = min_notional_violation["leg_notional"]
                    min_notional = min_notional_violation["min_notional"]
                compensated = await self._abort_and_compensate_min_notional(
                    state,
                    pending,
                    position,
                    hedge_venue=hedge_venue,
                    hedge_leg=hedge_leg_label,
                    missing_quantity=delta,
                    normalized_quantity=normalized_delta,
                    leg_notional_quote=leg_notional,
                    venue_min_notional_quote=min_notional,
                    min_notional_source=min_notional_source,
                    failed_stage=(
                        pending.short_stage or "exit_short"
                        if hedge_leg_label == "short"
                        else pending.long_stage or "exit_long"
                    ),
                    source="terminal_maker_hedge_min_notional",
                )
                if compensated:
                    return HedgeDeltaResult(
                        requested=delta,
                        filled=0.0,
                        residual=0.0,
                        success=True,
                        error=None,
                    )
                pending = state.pending_passive_closes.get(position.position_id)
                if pending is not None:
                    pending.phase_state.phase = PassiveExecutionPhase.DUAL_TAKER
                    pending.next_retry_at_ms = 0
            return HedgeDeltaResult(
                requested=delta, filled=0.0, residual=delta, success=False,
                error=dust_reason,
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
            quantity=normalized_delta,
            price=hedge_price,
            reduce_only=True,
            time_in_force=TimeInForce.IOC,
            client_order_id=hedge_cid,
        )

        hedge_submit_started_at_ms = 0
        hedge_submit_completed_at_ms = 0
        hedge_submit_quantity = normalized_delta

        def with_hedge_submit_timing(
            result: HedgeDeltaResult,
            *,
            reconciled: bool = False,
        ) -> HedgeDeltaResult:
            nonlocal hedge_submit_completed_at_ms
            if hedge_submit_started_at_ms > 0:
                # A submit attempt is not terminal until any required order
                # reconciliation has returned.  V1 applies its deadline at
                # this same boundary.
                hedge_submit_completed_at_ms = self._now_ms()
            return replace(
                result,
                hedge_submit_started_at_ms=hedge_submit_started_at_ms,
                hedge_submit_completed_at_ms=hedge_submit_completed_at_ms,
                hedge_submit_quantity=hedge_submit_quantity,
                hedge_submit_reconciled=reconciled,
            )

        def record_hedge_fill(fill: OrderFill) -> None:
            nonlocal hedge_submit_completed_at_ms
            if hedge_submit_started_at_ms > 0:
                hedge_submit_completed_at_ms = self._now_ms()
            fill_client_order_id = fill.client_order_id or hedge_cid
            previous_qty = pending.hedge_fill.quantity
            new_qty = previous_qty + fill.quantity
            pending.hedge_fill.quantity = new_qty
            prev_total = previous_qty * pending.hedge_fill.average_price
            pending.hedge_fill.average_price = (
                (prev_total + fill.quantity * fill.price) / new_qty
                if new_qty > 0 else fill.price
            )
            try:
                fill_fee = float(fill.fee_quote)
            except (TypeError, ValueError):
                fill_fee = None
            if not self._fee_evidence_complete(fill_fee):
                pending.hedge_fill.fee_evidence_complete = False
            else:
                pending.hedge_fill.fee_quote += fill_fee
                if previous_qty <= 1e-12:
                    pending.hedge_fill.fee_evidence_complete = True
            pending.hedge_fill.last_fill_time_ms = fill.filled_at_ms
            pending.hedge_fill.order_id = fill.order_id
            pending.hedge_fill.client_order_id = fill_client_order_id

            self._record_close_execution_leg(
                pending,
                leg_label=hedge_leg_label,
                fill=fill,
                client_order_id=fill_client_order_id,
                submitted_at_ms=hedge_submit_started_at_ms or self._now_ms(),
            )

            maker_fill_at_ms = int(pending.maker_fill.last_fill_time_ms or 0)
            submit_started_at_ms = int(hedge_submit_started_at_ms or 0)
            submit_completed_at_ms = int(hedge_submit_completed_at_ms or 0)
            maker_to_submit_ms = 0
            if (
                maker_fill_at_ms > 0
                and submit_started_at_ms > 0
                and self._deadline_clock_domains_match(maker_fill_at_ms, submit_started_at_ms)
            ):
                maker_to_submit_ms = max(submit_started_at_ms - maker_fill_at_ms, 0)
            submit_elapsed_ms = 0
            if (
                submit_started_at_ms > 0
                and submit_completed_at_ms > 0
                and self._deadline_clock_domains_match(
                    submit_started_at_ms,
                    submit_completed_at_ms,
                )
            ):
                submit_elapsed_ms = max(
                    submit_completed_at_ms - submit_started_at_ms,
                    0,
                )

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
                    "maker_fill_at_ms": maker_fill_at_ms,
                    "hedge_submit_started_at_ms": submit_started_at_ms,
                    "hedge_submit_completed_at_ms": submit_completed_at_ms,
                    "maker_fill_to_hedge_submit_ms": maker_to_submit_ms,
                    "hedge_submit_elapsed_ms": submit_elapsed_ms,
                },
            )

        active_truth_gap = self._active_hedge_truth_gap_reconciliation(
            state,
            pending,
            hedge_venue=hedge_venue,
            hedge_leg=hedge_leg_label,
        )
        if active_truth_gap is not None:
            accepted_order_id, accepted_client_order_id = self._accepted_order_truth_gap_identity(
                active_truth_gap,
                hedge_venue=hedge_venue,
                hedge_leg=hedge_leg_label,
            )
            accepted_client_order_id = accepted_client_order_id or hedge_cid
            fill_reconciliation_attempted = False
            fill_reconciliation_result = "not_available"
            fill_reconciliation_error = ""
            reconciliation = None
            fetch_reconciliation = getattr(adapter, "fetch_order_fill_reconciliation", None)
            if callable(fetch_reconciliation) and (accepted_order_id or accepted_client_order_id):
                fill_reconciliation_attempted = True
                try:
                    reconciliation = await fetch_reconciliation(
                        position.symbol,
                        accepted_order_id,
                        accepted_client_order_id or None,
                    )
                except Exception as reconcile_error:
                    fill_reconciliation_result = "error"
                    fill_reconciliation_error = str(reconcile_error)

            truth_decision = ORDER_TRUTH_LEDGER.resolve_order_success(
                venue=hedge_venue,
                symbol=position.symbol,
                order_id=accepted_order_id,
                client_order_id=accepted_client_order_id,
                target_qty=normalized_delta,
                reconciliation=reconciliation,
                metadata=(
                    getattr(reconciliation, "metadata", None)
                    if reconciliation is not None
                    else None
                ),
            )
            if truth_decision.fill_status == OrderTruthFillStatus.CONFIRMED_FILL:
                fill_reconciliation_result = "filled"
                recon_qty = truth_decision.reconciled_qty
                recon_price_raw = getattr(
                    reconciliation, "average_price", hedge_price or 0.0,
                )
                recon_price = (
                    float(recon_price_raw)
                    if isinstance(recon_price_raw, (int, float))
                    else (hedge_price or 0.0)
                )
                fill = OrderFill(
                    venue=hedge_venue,
                    symbol=position.symbol,
                    side=getattr(reconciliation, "side", hedge_side) or hedge_side,
                    quantity=recon_qty,
                    price=recon_price,
                    order_id=(
                        str(getattr(reconciliation, "order_id", "") or "")
                        or accepted_order_id
                    ),
                    client_order_id=(
                        str(getattr(reconciliation, "client_order_id", "") or "")
                        or accepted_client_order_id
                        or hedge_cid
                    ),
                    fee_quote=getattr(reconciliation, "fee_quote", None),
                    filled_at_ms=getattr(reconciliation, "filled_at_ms", 0)
                    or self._now_ms(),
                )
                record_hedge_fill(fill)
                state.remove_pending_close_reconciliation(active_truth_gap)
                residual = max(delta - fill.quantity, 0.0)
                success = residual < 1e-12
                self._journal.append(
                    "exit.passive_close_hedge_ack_reconciled",
                    {
                        "position_id": position.position_id,
                        "hedge_venue": hedge_venue.value,
                        "hedge_leg": hedge_leg_label,
                        "accepted_order_id": accepted_order_id,
                        "accepted_client_order_id": accepted_client_order_id,
                        "requested": delta,
                        "filled": fill.quantity,
                        "residual": residual,
                        "order_truth_fill_status": truth_decision.fill_status.value,
                        "order_truth_evidence_status": (
                            truth_decision.evidence_status.value
                        ),
                        "order_truth_decision": truth_decision.decision,
                        "order_truth_missing_evidence": list(
                            truth_decision.missing_evidence
                        ),
                        "terminal_without_truth": (
                            truth_decision.terminal_without_truth
                        ),
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

            if fill_reconciliation_attempted and fill_reconciliation_result == "not_available":
                fill_reconciliation_result = (
                    "truth_gap"
                    if reconciliation is not None
                    and truth_decision.fill_status == OrderTruthFillStatus.TRUTH_GAP
                    else "missing_or_zero_fill"
                )
            self._journal.append(
                "exit.passive_close_hedge_ack_reconcile_in_progress",
                {
                    "position_id": position.position_id,
                    "hedge_venue": hedge_venue.value,
                    "hedge_leg": hedge_leg_label,
                    "accepted_order_id": accepted_order_id,
                    "accepted_client_order_id": accepted_client_order_id,
                    "requested": delta,
                    "fill_reconciliation_attempted": fill_reconciliation_attempted,
                    "fill_reconciliation_result": fill_reconciliation_result,
                    "fill_reconciliation_error": fill_reconciliation_error,
                    "order_truth_fill_status": truth_decision.fill_status.value,
                    "order_truth_evidence_status": (
                        truth_decision.evidence_status.value
                    ),
                    "order_truth_decision": truth_decision.decision,
                    "order_truth_missing_evidence": list(
                        truth_decision.missing_evidence
                    ),
                    "terminal_without_truth": (
                        truth_decision.terminal_without_truth
                    ),
                    "decision": "retain_pending_without_resubmit",
                    "next_action": "retry_order_position_open_order_reconciliation",
                },
            )
            return HedgeDeltaResult(
                requested=delta,
                filled=0.0,
                residual=delta,
                success=False,
                error="accepted_order_truth_gap_pending",
                truth_gap=True,
                accepted_order_id=accepted_order_id,
                accepted_client_order_id=accepted_client_order_id,
            )

        hedge_submit_started_at_ms = self._claim_close_order_intent(
            pending,
            position,
            request,
            leg_label=hedge_leg_label,
            operation="place_order",
        )
        try:
            fill = await adapter.place_order(request)
        except Exception as e:
            req_ctx = RequestContext.from_order_request(request)
            uncertainty_payload: dict[str, Any] = {}
            if isinstance(e, OrderSubmitError):
                uncertainty_payload = build_order_submit_uncertainty_payload(
                    e,
                    venue=hedge_venue,
                    operation="place_order",
                    request=request,
                    default_client_order_id=hedge_cid,
                )
                evidence = ExchangeErrorEvidence.from_dict(
                    uncertainty_payload.get("exchange_error", {})
                )
            else:
                evidence = build_fallback_evidence(
                    e,
                    venue=hedge_venue.value,
                    operation="place_order",
                    request_context=req_ctx,
                )
            is_bybit_duplicate = (
                hedge_venue == Venue.BYBIT
                and _is_bybit_duplicate_order_link_id(str(e))
            )
            if is_bybit_duplicate:
                duplicate_reconcile = await reconcile_bybit_duplicate_client_order(
                    adapter=adapter,
                    symbol=position.symbol,
                    client_order_id=hedge_cid,
                    target_qty=normalized_delta,
                )
                self._journal.append(
                    "order.reconcile_result",
                    build_order_reconcile_result_payload(
                        result=duplicate_reconcile,
                        symbol=position.symbol,
                        client_order_id=hedge_cid,
                        reason="duplicate_client_id",
                    ),
                )

                recon_fill_qty = duplicate_reconcile.reconciled_qty
                if recon_fill_qty > 1e-12:
                    recon_price = duplicate_reconcile.average_price or hedge_price or 0.0
                    fill = OrderFill(
                        venue=hedge_venue,
                        symbol=position.symbol,
                        side=hedge_side,
                        quantity=recon_fill_qty,
                        price=recon_price,
                        order_id=duplicate_reconcile.order_id,
                        client_order_id=duplicate_reconcile.client_order_id or hedge_cid,
                        filled_at_ms=self._now_ms(),
                    )
                    record_hedge_fill(fill)

                self._journal.append(
                    "exit.passive_close_hedge_duplicate_client_order_reconcile_result",
                    {
                        "position_id": position.position_id,
                        "hedge_venue": hedge_venue.value,
                        "hedge_leg": hedge_leg_label,
                        "client_order_id": hedge_cid,
                        "classification": duplicate_reconcile.classification,
                        "decision": duplicate_reconcile.decision,
                        "target_qty": duplicate_reconcile.target_qty,
                        "reconciled_qty": duplicate_reconcile.reconciled_qty,
                        "live_qty": duplicate_reconcile.live_qty,
                        "remaining_qty": duplicate_reconcile.remaining_qty,
                        "retry_qty": duplicate_reconcile.retry_qty,
                        "order_id": duplicate_reconcile.order_id,
                        "original_error": str(e),
                    },
                )

                if duplicate_reconcile.clear_state:
                    residual = 0.0 if duplicate_reconcile.decision == "clear_live_flat" else max(delta - recon_fill_qty, 0.0)
                    self._journal.append(
                        "exit.passive_close_hedge_duplicate_client_order_reconciled",
                        {
                            "position_id": position.position_id,
                            "hedge_venue": hedge_venue.value,
                            "hedge_leg": hedge_leg_label,
                            "client_order_id": hedge_cid,
                            "order_id": duplicate_reconcile.order_id,
                            "requested": delta,
                            "filled": recon_fill_qty,
                            "residual": residual,
                            "original_error": str(e),
                            "classification": duplicate_reconcile.classification,
                        },
                    )
                    return with_hedge_submit_timing(HedgeDeltaResult(
                        requested=delta,
                        filled=recon_fill_qty,
                        residual=residual,
                        success=residual < 1e-12,
                        error=None if residual < 1e-12 else "partial_fill",
                        order_id=duplicate_reconcile.order_id,
                    ), reconciled=True)

                if duplicate_reconcile.should_retry_with_new_client_id:
                    retry_quantity = duplicate_reconcile.remaining_qty
                    if duplicate_reconcile.live_qty > 1e-9:
                        retry_quantity = min(retry_quantity, duplicate_reconcile.live_qty)
                    retry_cid = generate_exchange_cid(
                        f"{position.position_id}:{stage}:{self._now_ms()}",
                        "dup",
                        hedge_venue,
                    )
                    retry_request = OrderRequest(
                        venue=hedge_venue,
                        symbol=position.symbol,
                        side=hedge_side,
                        quantity=retry_quantity,
                        price=hedge_price,
                        reduce_only=True,
                        time_in_force=TimeInForce.IOC,
                        client_order_id=retry_cid,
                    )
                    hedge_submit_started_at_ms = self._claim_close_order_intent(
                        pending,
                        position,
                        retry_request,
                        leg_label=hedge_leg_label,
                        operation="place_order_duplicate_retry",
                    )
                    hedge_submit_quantity = retry_quantity
                    try:
                        retry_fill = await adapter.place_order(retry_request)
                    except Exception as retry_error:
                        residual = max(delta - recon_fill_qty, 0.0)
                        self._journal.append(
                            "exit.passive_close_hedge_duplicate_client_order_retry_failed",
                            {
                                "position_id": position.position_id,
                                "hedge_venue": hedge_venue.value,
                                "hedge_leg": hedge_leg_label,
                                "client_order_id": hedge_cid,
                                "next_client_order_id": retry_cid,
                                "requested": delta,
                                "retry_quantity": retry_quantity,
                                "filled": recon_fill_qty,
                                "residual": residual,
                                "error": str(retry_error),
                            },
                        )
                        return with_hedge_submit_timing(HedgeDeltaResult(
                            requested=delta,
                            filled=recon_fill_qty,
                            residual=residual,
                            success=False,
                            error="duplicate_client_order_id_retry_failed",
                            order_id=duplicate_reconcile.order_id,
                        ), reconciled=True)
                    if retry_fill.quantity > 0:
                        retry_fill = replace(
                            retry_fill,
                            client_order_id=retry_fill.client_order_id or retry_cid,
                        )
                        record_hedge_fill(retry_fill)
                    total_filled = recon_fill_qty + max(float(retry_fill.quantity or 0.0), 0.0)
                    residual = max(delta - total_filled, 0.0)
                    self._journal.append(
                        "exit.passive_close_hedge_duplicate_client_order_retry",
                        {
                            "position_id": position.position_id,
                            "hedge_venue": hedge_venue.value,
                            "hedge_leg": hedge_leg_label,
                            "client_order_id": hedge_cid,
                            "next_client_order_id": retry_cid,
                            "requested": delta,
                            "retry_quantity": retry_quantity,
                            "filled": total_filled,
                            "residual": residual,
                        },
                    )
                    return with_hedge_submit_timing(HedgeDeltaResult(
                        requested=delta,
                        filled=total_filled,
                        residual=residual,
                        success=residual < 1e-12,
                        error=None if residual < 1e-12 else "partial_fill",
                        order_id=retry_fill.order_id or duplicate_reconcile.order_id,
                    ), reconciled=True)

                self._journal.append(
                    "exit.passive_close_hedge_duplicate_client_order_pending_reconcile",
                    {
                        "position_id": position.position_id,
                        "hedge_venue": hedge_venue.value,
                        "hedge_leg": hedge_leg_label,
                        "client_order_id": hedge_cid,
                        "classification": duplicate_reconcile.classification,
                        "decision": duplicate_reconcile.decision,
                        "error": str(e),
                    },
                )
                return with_hedge_submit_timing(HedgeDeltaResult(
                    requested=delta,
                    filled=recon_fill_qty,
                    residual=max(delta - recon_fill_qty, 0.0),
                    success=False,
                    error="duplicate_client_order_id_backoff",
                    order_id=duplicate_reconcile.order_id,
                ), reconciled=True)

            should_reconcile = isinstance(e, OrderSubmitError) or is_bybit_duplicate
            fill_reconciliation_attempted = False
            fill_reconciliation_result = ""
            if should_reconcile:
                reconciliation = None
                fill_reconciliation_attempted = True
                accepted_order_id = ""
                accepted_client_order_id = hedge_cid
                accepted_ack_confirmation = False
                if isinstance(e, OrderSubmitError):
                    accepted_order_id = str(
                        getattr(e, "accepted_order_id", "")
                        or uncertainty_payload.get("accepted_order_id")
                        or ""
                    )
                    accepted_client_order_id = str(
                        getattr(e, "accepted_client_order_id", "")
                        or uncertainty_payload.get("accepted_client_order_id")
                        or hedge_cid
                    )
                    accepted_ack_confirmation = (
                        bool(getattr(e, "order_ack_only", False))
                        or bool(accepted_order_id)
                        or bool(uncertainty_payload.get("accepted_order_truth_gap"))
                    )
                try:
                    reconciliation = await adapter.fetch_order_fill_reconciliation(
                        position.symbol, accepted_order_id, accepted_client_order_id,
                    )
                except Exception as reconcile_error:
                    fill_reconciliation_result = "error"
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
                truth_decision = ORDER_TRUTH_LEDGER.resolve_order_success(
                    venue=hedge_venue,
                    symbol=position.symbol,
                    order_id=accepted_order_id,
                    client_order_id=accepted_client_order_id,
                    target_qty=normalized_delta,
                    reconciliation=reconciliation,
                    metadata=(
                        getattr(reconciliation, "metadata", None)
                        if reconciliation is not None
                        else None
                    ),
                )
                recon_qty = truth_decision.reconciled_qty
                if truth_decision.fill_status == OrderTruthFillStatus.CONFIRMED_FILL:
                    fill_reconciliation_result = "filled"
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
                        else (
                            "exit.passive_close_hedge_confirmed_after_ack"
                            if accepted_ack_confirmation
                            else "exit.passive_close_hedge_reconciled_after_error"
                        )
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
                            "classification": (
                                "duplicate_client_order_reconciled"
                                if is_bybit_duplicate
                                else (
                                    "accepted_ack_confirmed"
                                    if accepted_ack_confirmation
                                    else "uncertain_submit_reconciled"
                                )
                            ),
                            "severity": "info",
                            "order_submit_uncertain": isinstance(e, OrderSubmitError),
                            "decision": (
                                "duplicate_client_order_reconciled_by_client_id"
                                if is_bybit_duplicate
                                else "accepted_order_reconciled_by_client_id"
                            ),
                            "order_truth_fill_status": truth_decision.fill_status.value,
                            "order_truth_evidence_status": (
                                truth_decision.evidence_status.value
                            ),
                            "order_truth_decision": truth_decision.decision,
                            "order_truth_missing_evidence": list(
                                truth_decision.missing_evidence
                            ),
                            "terminal_without_truth": (
                                truth_decision.terminal_without_truth
                            ),
                            "original_error": str(e),
                        },
                    )
                    return with_hedge_submit_timing(HedgeDeltaResult(
                        requested=delta,
                        filled=fill.quantity,
                        residual=residual,
                        success=success,
                        error=None if success else "partial_fill",
                        order_id=fill.order_id,
                    ), reconciled=True)

                if is_bybit_duplicate:
                    self._journal.append(
                        "exit.passive_close_hedge_duplicate_client_order_pending_reconcile",
                        {
                            "position_id": position.position_id,
                            "hedge_venue": hedge_venue.value,
                            "hedge_leg": hedge_leg_label,
                            "client_order_id": hedge_cid,
                            "error": str(e),
                            "order_truth_fill_status": truth_decision.fill_status.value,
                            "order_truth_evidence_status": (
                                truth_decision.evidence_status.value
                            ),
                            "order_truth_decision": truth_decision.decision,
                            "order_truth_missing_evidence": list(
                                truth_decision.missing_evidence
                            ),
                        },
                    )
                elif not fill_reconciliation_result:
                    fill_reconciliation_result = "missing_or_zero_fill"

            hedge_error_payload = {
                    "position_id": position.position_id,
                    "hedge_venue": hedge_venue.value,
                    "hedge_leg": hedge_leg_label,
                    "delta": delta,
                    "error": str(e),
                    "exchange_error": evidence.to_dict(),
                    "request_context": req_ctx.to_dict(),
                    "evidence_completeness": evidence.evidence_completeness,
            }
            hedge_error_payload.update(uncertainty_payload)
            if is_order_truth_gap(e):
                accepted_order_id = str(
                    hedge_error_payload.get("accepted_order_id")
                    or getattr(e, "accepted_order_id", "")
                    or ""
                )
                accepted_client_order_id = str(
                    hedge_error_payload.get("accepted_client_order_id")
                    or getattr(e, "accepted_client_order_id", "")
                    or hedge_cid
                    or ""
                )
                hedge_error_payload["accepted_order_id"] = accepted_order_id
                hedge_error_payload["accepted_client_order_id"] = accepted_client_order_id
                hedge_error_payload["fill_reconciliation_attempted"] = (
                    fill_reconciliation_attempted
                )
                hedge_error_payload["fill_reconciliation_result"] = (
                    fill_reconciliation_result or "not_attempted"
                )
                hedge_error_payload["fill_reconciliation_client_order_id"] = hedge_cid
                hedge_error_payload["order_truth_fill_status"] = (
                    truth_decision.fill_status.value
                    if fill_reconciliation_attempted
                    else "not_attempted"
                )
                hedge_error_payload["order_truth_evidence_status"] = (
                    truth_decision.evidence_status.value
                    if fill_reconciliation_attempted
                    else "unavailable"
                )
                hedge_error_payload["order_truth_decision"] = (
                    truth_decision.decision
                    if fill_reconciliation_attempted
                    else "retain_backoff"
                )
                hedge_error_payload["order_truth_missing_evidence"] = (
                    list(truth_decision.missing_evidence)
                    if fill_reconciliation_attempted
                    else []
                )
                self._register_accepted_order_truth_gap(
                    state,
                    pending,
                    position,
                    venue=hedge_venue,
                    leg_label=hedge_leg_label,
                    operation="place_order",
                    source="passive_close_hedge_order_truth_gap",
                    payload=hedge_error_payload,
                    request=request,
                    quantity=normalized_delta,
                )
                self._journal.append(
                    "exit.passive_close_hedge_ack_pending_reconcile",
                    hedge_error_payload,
                )
                return with_hedge_submit_timing(HedgeDeltaResult(
                    requested=delta,
                    filled=0.0,
                    residual=delta,
                    success=False,
                    error=str(e),
                    truth_gap=True,
                    accepted_order_id=accepted_order_id,
                    accepted_client_order_id=accepted_client_order_id,
                ), reconciled=True)
            self._journal.append(
                "exit.passive_close_hedge_error",
                hedge_error_payload,
            )
            return with_hedge_submit_timing(HedgeDeltaResult(
                requested=delta, filled=0.0, residual=delta, success=False,
                error=str(e),
            ))

        filled_qty = fill.quantity if fill.quantity > 0 else 0.0
        ack_order_id = str(getattr(fill, "order_id", "") or "")
        ack_client_order_id = str(getattr(fill, "client_order_id", "") or hedge_cid)
        if (
            filled_qty <= 1e-12
            and hedge_venue == Venue.BYBIT
            and (ack_order_id or ack_client_order_id)
            and callable(getattr(adapter, "fetch_order_fill_reconciliation", None))
        ):
            reconciliation = None
            reconciliation_error = ""
            try:
                reconciliation = await adapter.fetch_order_fill_reconciliation(
                    position.symbol,
                    ack_order_id,
                    ack_client_order_id,
                )
            except Exception as exc:
                reconciliation_error = str(exc)

            recon_qty_raw = (
                getattr(reconciliation, "quantity", 0.0)
                if reconciliation is not None
                else 0.0
            )
            recon_qty = (
                float(recon_qty_raw)
                if isinstance(recon_qty_raw, (int, float))
                else 0.0
            )
            truth_decision = ORDER_TRUTH_LEDGER.resolve_order_success(
                venue=hedge_venue,
                symbol=position.symbol,
                order_id=ack_order_id,
                client_order_id=ack_client_order_id,
                target_qty=normalized_delta,
                reconciliation=reconciliation,
                metadata=(
                    getattr(reconciliation, "metadata", None)
                    if reconciliation is not None
                    else None
                ),
            )
            recon_qty = truth_decision.reconciled_qty
            if truth_decision.fill_status == OrderTruthFillStatus.CONFIRMED_FILL:
                recon_price_raw = getattr(
                    reconciliation, "average_price", hedge_price or 0.0,
                )
                recon_price = (
                    float(recon_price_raw)
                    if isinstance(recon_price_raw, (int, float))
                    else (hedge_price or 0.0)
                )
                fill = OrderFill(
                    venue=hedge_venue,
                    symbol=position.symbol,
                    side=getattr(reconciliation, "side", hedge_side) or hedge_side,
                    quantity=recon_qty,
                    price=recon_price,
                    order_id=(
                        str(getattr(reconciliation, "order_id", "") or "")
                        or ack_order_id
                    ),
                    client_order_id=(
                        str(getattr(reconciliation, "client_order_id", "") or "")
                        or ack_client_order_id
                    ),
                    fee_quote=getattr(reconciliation, "fee_quote", None),
                    filled_at_ms=getattr(reconciliation, "filled_at_ms", 0)
                    or self._now_ms(),
                )
                record_hedge_fill(fill)
                residual = max(delta - fill.quantity, 0.0)
                success = residual < 1e-12
                self._journal.append(
                    "exit.passive_close_hedge_confirmed_after_ack",
                    {
                        "position_id": position.position_id,
                        "hedge_venue": hedge_venue.value,
                        "hedge_leg": hedge_leg_label,
                        "client_order_id": ack_client_order_id,
                        "order_id": fill.order_id,
                        "requested": delta,
                        "filled": fill.quantity,
                        "residual": residual,
                        "classification": "accepted_ack_confirmed",
                        "severity": "info",
                        "order_submit_uncertain": False,
                        "decision": "accepted_order_reconciled_by_client_id",
                        "order_truth_fill_status": truth_decision.fill_status.value,
                        "order_truth_evidence_status": (
                            truth_decision.evidence_status.value
                        ),
                        "order_truth_decision": truth_decision.decision,
                        "order_truth_missing_evidence": list(
                            truth_decision.missing_evidence
                        ),
                        "terminal_without_truth": truth_decision.terminal_without_truth,
                    },
                )
                return with_hedge_submit_timing(HedgeDeltaResult(
                    requested=delta,
                    filled=fill.quantity,
                    residual=residual,
                    success=success,
                    error=None if success else "partial_fill",
                    order_id=fill.order_id,
                ), reconciled=True)
            self._journal.append(
                "exit.passive_close_hedge_ack_unconfirmed",
                {
                    "position_id": position.position_id,
                    "hedge_venue": hedge_venue.value,
                    "hedge_leg": hedge_leg_label,
                    "client_order_id": ack_client_order_id,
                    "order_id": ack_order_id,
                    "requested": delta,
                    "fill_reconciliation_result": (
                        "error" if reconciliation_error else "missing_or_zero_fill"
                    ),
                    "fill_reconciliation_error": reconciliation_error,
                    "order_truth_fill_status": truth_decision.fill_status.value,
                    "order_truth_evidence_status": (
                        truth_decision.evidence_status.value
                    ),
                    "order_truth_decision": truth_decision.decision,
                    "order_truth_missing_evidence": list(
                        truth_decision.missing_evidence
                    ),
                    "decision": "retain_pending_without_resubmit",
                },
            )
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

        return with_hedge_submit_timing(HedgeDeltaResult(
            requested=delta, filled=filled_qty, residual=residual,
            success=success, error=None if success else "partial_fill" if filled_qty > 0 else "zero_fill",
        ))

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
            if maker_venue == Venue.OKX and _is_okx_amend_invalid_request_type_error(e):
                self._journal.append(
                    "exit.passive_close_amend_unsupported_cancel_replace",
                    {
                        "position_id": position.position_id,
                        "venue": maker_venue.value,
                        "maker_leg": maker_leg_label,
                        "order_id": pending.phase_state.maker_order_id,
                        "client_order_id": pending.phase_state.maker_client_order_id,
                        "reason": "okx_amend_invalid_request_type",
                        "exchange_code": str(getattr(e, "exchange_code", "") or "50115"),
                        "exchange_msg": str(
                            getattr(e, "exchange_msg", "") or "Invalid request type"
                        ),
                        "endpoint": str(
                            getattr(e, "endpoint", "")
                            or "POST /api/v5/trade/amend-order"
                        ),
                        "official_doc_url": "https://www.okx.com/docs-v5/en/#order-book-trading-trade-amend-order",
                    },
                )
                await self._cancel_replace_maker_order(
                    state,
                    pending,
                    position,
                    maker_venue,
                    maker_side,
                    maker_leg_label,
                    target_price,
                    remaining_quantity,
                    tick_size,
                    reference_mid,
                )
                return
            if maker_venue == Venue.OKX:
                progress = await self._poll_maker_progress(
                    adapter,
                    position.symbol,
                    pending.phase_state.maker_order_id,
                    pending.phase_state.maker_client_order_id,
                    side=maker_side,
                )
                if progress is not None:
                    pending.phase_state.maker_order_id = (
                        progress.order_id or pending.phase_state.maker_order_id
                    )
                    pending.phase_state.maker_client_order_id = (
                        progress.client_order_id
                        or pending.phase_state.maker_client_order_id
                    )
                    if progress.cumulative_quantity > pending.maker_fill.quantity + 1e-9:
                        self._apply_maker_progress(pending, progress, now_ms)
                    if progress.state in (
                        PassiveOrderState.OPEN,
                        PassiveOrderState.PARTIALLY_FILLED,
                    ):
                        self._journal.append(
                            "exit.passive_close_amend_order_truth_retained",
                            {
                                "position_id": position.position_id,
                                "venue": maker_venue.value,
                                "maker_leg": maker_leg_label,
                                "order_id": pending.phase_state.maker_order_id,
                                "client_order_id": pending.phase_state.maker_client_order_id,
                                "state": progress.state.value,
                                "cumulative_quantity": progress.cumulative_quantity,
                                "average_price": progress.average_price,
                                "reason": "okx_amend_failed_original_order_still_live",
                                "official_doc_rule": "okx_amend_cxlOnFail_default_false",
                            },
                        )
                        pending.next_retry_at_ms = (
                            self._now_ms() + PASSIVE_CLOSE_PROGRESS_POLL_INTERVAL_MS
                        )
                        return
                    self._journal.append(
                        "exit.passive_close_amend_order_truth_terminal",
                        {
                            "position_id": position.position_id,
                            "venue": maker_venue.value,
                            "maker_leg": maker_leg_label,
                            "order_id": progress.order_id,
                            "client_order_id": progress.client_order_id,
                            "state": progress.state.value,
                            "cumulative_quantity": progress.cumulative_quantity,
                            "reason": "okx_amend_failed_original_order_terminal",
                        },
                    )
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

        if old_order_id or old_client_id:
            # Exchange cancel ACKs are asynchronous.  Whether cancel failed or
            # was accepted, only replace after old order is confirmed dead.
            old_dead = await self._probe_order_dead(
                adapter, position.symbol, old_order_id, old_client_id,
                side=maker_side,
            )
            if not old_dead:
                reason = (
                    "cancel_ack_without_terminal_order_truth"
                    if cancel_ok
                    else "cancel_failed_old_order_may_still_be_alive"
                )
                self._journal.append(
                    "exit.passive_close_cancel_replace_blocked_double_order_risk",
                    {
                        "position_id": position.position_id,
                        "symbol": position.symbol,
                        "maker_venue": maker_venue.value,
                        "maker_leg": maker_leg_label,
                        "old_order_id": old_order_id,
                        "old_client_order_id": old_client_id,
                        "cancel_ack_received": cancel_ok,
                        "reason": reason,
                        "decision": "retain_old_order_identity",
                        "next_action": "retry_cancel_replace_after_order_truth",
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
            fill = self._persisted_leg_fill(leg)
            if fill is not None:
                short_legs.append(CloseExecutionLeg(
                    fill=fill,
                    client_order_id=leg.client_order_id,
                    submit_started_at_ms=leg.submit_started_at_ms,
                    latency_ms=leg.latency_ms,
                ))

        long_legs = []
        for leg in pending.long_legs:
            fill = self._persisted_leg_fill(leg)
            if fill is not None:
                long_legs.append(CloseExecutionLeg(
                    fill=fill,
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
            fee_quote = None
            if pending_fill.fee_evidence_complete and pending_fill.quantity > 1e-12:
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
        accounting_evidence_gaps = close_accounting_evidence_gaps(
            position,
            long_legs,
            short_legs,
        )
        accounting_evidence_complete = not accounting_evidence_gaps
        if not accounting_evidence_complete:
            register_close_accounting_reconciliation(
                state,
                self._journal,
                position,
                long_legs=long_legs,
                short_legs=short_legs,
                now_ms=self._now_ms(),
                reason=pending.reason,
                source="passive_close_execution",
                evidence_gaps=accounting_evidence_gaps,
            )

        # Apply close to position state
        long_closed = sum(leg.fill.quantity for leg in long_legs if leg.fill)
        short_closed = sum(leg.fill.quantity for leg in short_legs if leg.fill)
        matched_closed = min(long_closed, short_closed)

        position.matched_quantity = max(position.matched_quantity - matched_closed, 0.0)
        position.long_quantity = max(position.long_quantity - long_closed, 0.0)
        position.short_quantity = max(position.short_quantity - short_closed, 0.0)
        if accounting_evidence_complete:
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

        # A passive close has the same durable accounting obligation as a
        # standard close.  `exit.passive_close_resolved` below is an
        # operational lifecycle event; it is intentionally not a ledger-close
        # event.  Emit the normal terminal event only when both legs are known
        # flat and no residual task was created.
        fully_closed = (
            residual is None
            and position.matched_quantity < 1e-12
            and position.long_quantity < 1e-12
            and position.short_quantity < 1e-12
        )

        # If fully closed, remove from open positions
        if position.matched_quantity < 1e-12:
            state.open_positions.pop(pending.position_id, None)

        closure_fields = self._v1_lifecycle_passive_close_event_fields(
            state,
            pending.position_id,
            now_ms,
        )

        # Clean up pending passive close
        state.pending_passive_closes.pop(pending.position_id, None)

        resolved_payload = {
            "position_id": pending.position_id,
            "reason": pending.reason,
            "long_closed_qty": long_closed,
            "short_closed_qty": short_closed,
            "chunk_count": pending.chunk_count(),
            "total_legs": len(long_legs) + len(short_legs),
            "terminal_accounting_status": (
                "final"
                if accounting_evidence_complete
                else "pending_close_accounting_reconciliation"
            ),
            "accounting_evidence_gaps": list(accounting_evidence_gaps),
            **closure_fields,
        }
        if accounting_evidence_complete:
            resolved_payload.update(
                {
                    "price_pnl": close.realized_price_pnl_quote,
                    "net_quote": close.net_quote,
                }
            )
        self._journal.append("exit.passive_close_resolved", resolved_payload)
        if fully_closed:
            def leg_record(leg: CloseExecutionLeg) -> dict[str, Any]:
                fill = leg.fill
                return {
                    "venue": fill.venue.value,
                    "order_id": fill.order_id,
                    "client_order_id": fill.client_order_id or leg.client_order_id,
                    "quantity": fill.quantity,
                    "average_price": fill.price,
                    "fee_quote": fill.fee_quote,
                    "filled_at_ms": fill.filled_at_ms,
                }

            funding_quote = (
                position.captured_funding_quote + position.second_stage_funding_quote
            )
            entry_fee_quote = position.total_entry_fee_quote
            exit_fee_quote = position.realized_exit_fee_quote
            price_pnl_quote = position.realized_price_pnl_quote
            entry_fee_evidence_complete = (
                position.entry_fee_evidence_complete and accounting_evidence_complete
            )
            terminal_payload = {
                "position_id": position.position_id,
                "symbol": position.symbol,
                "long_venue": position.long_venue.value,
                "short_venue": position.short_venue.value,
                "reason": pending.reason,
                "closed_at_ms": now_ms,
                "close_path": "passive_close_fills",
                "long_closed_qty": long_closed,
                "short_closed_qty": short_closed,
                "exit_quantity": matched_closed,
                "long_legs": [leg_record(leg) for leg in long_legs if leg.fill],
                "short_legs": [leg_record(leg) for leg in short_legs if leg.fill],
                "price_pnl": price_pnl_quote,
                "realized_price_pnl_quote": price_pnl_quote,
                "funding_pnl_quote": funding_quote,
                "entry_fee_quote": entry_fee_quote,
                "total_entry_fee_quote": entry_fee_quote,
                "entry_fee_evidence_complete": entry_fee_evidence_complete,
                "exit_fee_quote": exit_fee_quote,
                "total_exit_fee_quote": exit_fee_quote,
                "net_quote": (
                    price_pnl_quote + funding_quote - entry_fee_quote - exit_fee_quote
                ),
                "net_quote_status": (
                    "final" if entry_fee_evidence_complete else "provisional"
                ),
                "terminal_accounting_status": (
                    "final"
                    if entry_fee_evidence_complete
                    else "provisional_entry_fee_evidence_unavailable"
                ),
                "chunk_count": pending.chunk_count(),
            }
            if entry_fee_evidence_complete:
                self._journal.append_critical(now_ms, "exit.closed", terminal_payload)
            elif accounting_evidence_complete:
                self._journal.append_critical(
                    now_ms,
                    "exit.billing_evidence_unavailable",
                    {
                        **terminal_payload,
                        "terminal_reason": (
                            "entry_fee_evidence_unavailable_after_confirmed_passive_close"
                        ),
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
            pending = state.pending_passive_closes.get(pid)
            position = state.open_positions.get(pid) or (
                pending.position_snapshot if pending is not None else None
            )
            if pending is not None and position is not None:
                if await self._clear_if_live_flat(
                    state,
                    pending,
                    position,
                    source="pending_passive_close_flat_probe",
                ):
                    continue
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
        adapters: dict[Venue, VenueAdapter] | None = None,
    ) -> bool:
        if not await self._probe_live_flatness(
            pending,
            adapters or self._adapters,
            position_snapshot=position,
        ):
            return False

        # Second confirmation: use _fetch_live_position_snapshot which
        # returns (None, error) on failure, NOT 0.0.  This prevents
        # false-flat when adapter is missing or API throws.
        long_snap, long_err = await self._fetch_live_position_snapshot(
            position.long_venue, position.symbol
        )
        short_snap, short_err = await self._fetch_live_position_snapshot(
            position.short_venue, position.symbol
        )
        if long_err or short_err:
            self._journal.append(
                "exit.passive_close_clear_flat_untrusted",
                {
                    "position_id": pending.position_id,
                    "symbol": position.symbol,
                    "long_venue": position.long_venue.value,
                    "short_venue": position.short_venue.value,
                    "long_error": long_err,
                    "short_error": short_err,
                    "live_truth_trusted": False,
                    "decision": "retain_pending",
                    "source": source,
                },
            )
            return False

        actual_long_size = abs(float(getattr(long_snap, "quantity", 0.0)))
        actual_short_size = abs(float(getattr(short_snap, "quantity", 0.0)))
        long_open_orders_flat, long_open_orders_evidence = await self._probe_venue_open_orders_flat(
            position.long_venue,
            position.symbol,
            adapters or self._adapters,
        )
        short_open_orders_flat, short_open_orders_evidence = await self._probe_venue_open_orders_flat(
            position.short_venue,
            position.symbol,
            adapters or self._adapters,
        )
        if long_open_orders_flat is not True or short_open_orders_flat is not True:
            self._journal.append(
                "exit.passive_close_clear_flat_untrusted",
                {
                    "position_id": pending.position_id,
                    "symbol": position.symbol,
                    "long_venue": position.long_venue.value,
                    "short_venue": position.short_venue.value,
                    "long_open_orders": long_open_orders_evidence,
                    "short_open_orders": short_open_orders_evidence,
                    "live_truth_trusted": False,
                    "decision": "retain_pending",
                    "source": source,
                },
            )
            return False

        self._clear_live_flat_state(
            state,
            pending,
            position,
            source=source,
            actual_long_size=actual_long_size,
            actual_short_size=actual_short_size,
            extra=extra,
            exchange_truth=self._live_flat_exchange_truth(
                position,
                long_snap=long_snap,
                short_snap=short_snap,
                long_open_orders_evidence=long_open_orders_evidence,
                short_open_orders_evidence=short_open_orders_evidence,
            ),
        )
        return True

    @staticmethod
    def _position_snapshot_for_close_reconciliation(position: OpenPosition) -> dict[str, Any]:
        return {
            "position_id": position.position_id,
            "symbol": position.symbol,
            "long_venue": position.long_venue.value,
            "short_venue": position.short_venue.value,
            "long_quantity": position.long_quantity,
            "short_quantity": position.short_quantity,
            "matched_quantity": position.matched_quantity,
            "long_entry_price": position.long_entry_price,
            "short_entry_price": position.short_entry_price,
            "long_entry_fee_quote": position.long_entry_fee_quote,
            "short_entry_fee_quote": position.short_entry_fee_quote,
            "total_entry_fee_quote": position.total_entry_fee_quote,
            "entry_fee_evidence_complete": position.entry_fee_evidence_complete,
            "captured_funding_quote": position.captured_funding_quote,
            "second_stage_funding_quote": position.second_stage_funding_quote,
            "opened_at_ms": position.opened_at_ms,
        }

    @staticmethod
    def _position_truth_record(snapshot: Any, venue: Venue, symbol: str) -> dict[str, Any]:
        quantity = getattr(snapshot, "quantity", 0.0)
        try:
            quantity_value = float(quantity or 0.0)
        except (TypeError, ValueError):
            quantity_value = 0.0
        side = getattr(snapshot, "side", None)
        return {
            "venue": venue.value,
            "symbol": symbol,
            "side": side.value if isinstance(side, Side) else str(side or ""),
            "quantity": abs(quantity_value),
            "entry_price": float(getattr(snapshot, "entry_price", 0.0) or 0.0),
            "observed_at_ms": int(getattr(snapshot, "observed_at_ms", 0) or 0),
        }

    def _live_flat_exchange_truth(
        self,
        position: OpenPosition,
        *,
        long_snap: Any,
        short_snap: Any,
        long_open_orders_evidence: str | None,
        short_open_orders_evidence: str | None,
    ) -> dict[str, Any]:
        return {
            "truth_available": True,
            "positions": [
                self._position_truth_record(long_snap, position.long_venue, position.symbol),
                self._position_truth_record(short_snap, position.short_venue, position.symbol),
            ],
            "open_orders": [],
            "open_order_truth": [
                {
                    "venue": position.long_venue.value,
                    "symbol": position.symbol,
                    "open_orders_empty": True,
                    "evidence": long_open_orders_evidence,
                },
                {
                    "venue": position.short_venue.value,
                    "symbol": position.symbol,
                    "open_orders_empty": True,
                    "evidence": short_open_orders_evidence,
                },
            ],
            "source": "passive_close_live_flat_truth",
        }

    @staticmethod
    def _close_reconciliation_record(
        *,
        venue: Venue,
        order_id: str = "",
        client_order_id: str = "",
        quantity: float = 0.0,
        average_price: float = 0.0,
        fee_quote: float | None = None,
    ) -> dict[str, Any]:
        return {
            "venue": venue.value,
            "order_id": str(order_id or ""),
            "client_order_id": str(client_order_id or ""),
            "quantity": float(quantity or 0.0),
            "average_price": float(average_price or 0.0),
            "fee_quote": (
                float(fee_quote)
                if PassiveCloseExecutor._fee_evidence_complete(fee_quote)
                else None
            ),
        }

    def _pending_close_reconciliation_records(
        self,
        pending: PendingPassiveClose,
        position: OpenPosition,
        *,
        extra: dict[str, Any] | None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        long_records: list[dict[str, Any]] = []
        short_records: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()

        def add_record(target: list[dict[str, Any]], record: dict[str, Any]) -> None:
            venue = str(record.get("venue") or "")
            order_id = str(record.get("order_id") or "")
            client_order_id = str(record.get("client_order_id") or "")
            key = (
                venue,
                order_id,
                "" if order_id else client_order_id,
            )
            has_identity = bool(key[1] or key[2])
            has_fill = float(record.get("quantity") or 0.0) > 1e-12
            if not has_identity and not has_fill:
                return
            if key in seen:
                return
            seen.add(key)
            target.append(record)

        def add_fill_state(
            target: list[dict[str, Any]],
            fill_state: PendingPassiveLegFill,
            venue: Venue,
        ) -> None:
            add_record(
                target,
                self._close_reconciliation_record(
                    venue=venue,
                    order_id=fill_state.order_id,
                    client_order_id=fill_state.client_order_id,
                    quantity=fill_state.quantity,
                    average_price=fill_state.average_price,
                    fee_quote=(
                        fill_state.fee_quote
                        if fill_state.fee_evidence_complete
                        else None
                    ),
                ),
            )

        for leg in pending.long_legs:
            fill = self._persisted_leg_fill(leg)
            if fill is None:
                continue
            add_record(
                long_records,
                self._close_reconciliation_record(
                    venue=fill.venue,
                    order_id=fill.order_id,
                    client_order_id=leg.client_order_id or fill.client_order_id or "",
                    quantity=fill.quantity,
                    average_price=fill.price,
                    fee_quote=fill.fee_quote,
                ),
            )
        for leg in pending.short_legs:
            fill = self._persisted_leg_fill(leg)
            if fill is None:
                continue
            add_record(
                short_records,
                self._close_reconciliation_record(
                    venue=fill.venue,
                    order_id=fill.order_id,
                    client_order_id=leg.client_order_id or fill.client_order_id or "",
                    quantity=fill.quantity,
                    average_price=fill.price,
                    fee_quote=fill.fee_quote,
                ),
            )

        if pending.phase_state.active_maker_leg == ActiveMakerLeg.LONG:
            add_fill_state(long_records, pending.maker_fill, position.long_venue)
            add_fill_state(short_records, pending.hedge_fill, position.short_venue)
        else:
            add_fill_state(short_records, pending.maker_fill, position.short_venue)
            add_fill_state(long_records, pending.hedge_fill, position.long_venue)

        def add_intent_records(
            target: list[dict[str, Any]],
            legs: list[PersistedCloseExecutionLeg],
            venue: Venue,
        ) -> None:
            # Every unresolved submit intent is an exchange lookup target.
            # A prior confirmed fill cannot prove that a later intent was
            # rejected; dropping it would lose the exact ACK-loss case this
            # recovery path exists to reconcile.
            for leg in legs:
                if leg.fill is None and leg.client_order_id:
                    add_record(
                        target,
                        self._close_reconciliation_record(
                            venue=venue,
                            client_order_id=leg.client_order_id,
                        ),
                    )

        add_intent_records(long_records, pending.long_legs, position.long_venue)
        add_intent_records(short_records, pending.short_legs, position.short_venue)

        if extra:
            flattened_venue = str(extra.get("flattened_venue") or "")
            flattened_quantity = float(extra.get("flattened_quantity") or 0.0)
            try:
                venue = Venue.from_str(flattened_venue)
            except ValueError:
                venue = None
            if venue is not None:
                target = (
                    long_records
                    if venue == position.long_venue
                    else short_records
                    if venue == position.short_venue
                    else None
                )
                if target is not None:
                    client_ids = [
                        str(v)
                        for v in [
                            *extra.get("force_close_client_order_ids", []),
                            *extra.get("accepted_client_order_ids", []),
                        ]
                        if v
                    ]
                    order_ids = [
                        str(v)
                        for v in [
                            *extra.get("force_close_order_ids", []),
                            *extra.get("accepted_order_ids", []),
                        ]
                        if v
                    ]
                    count = max(len(client_ids), len(order_ids))
                    for idx in range(count):
                        add_record(
                            target,
                            self._close_reconciliation_record(
                                venue=venue,
                                order_id=order_ids[idx] if idx < len(order_ids) else "",
                                client_order_id=client_ids[idx] if idx < len(client_ids) else "",
                                quantity=flattened_quantity if idx == 0 else 0.0,
                            ),
                        )

        return long_records, short_records

    def _register_close_reconciliation_after_live_flat(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
        *,
        source: str,
        payload: dict[str, Any],
        extra: dict[str, Any] | None,
    ) -> bool:
        if self._runtime_mode != "live":
            return False

        long_legs, short_legs = self._pending_close_reconciliation_records(
            pending,
            position,
            extra=extra,
        )
        closed_at_ms = self._now_ms()
        position_snapshot = self._position_snapshot_for_close_reconciliation(position)
        reconciliation = {
            "position_id": pending.position_id,
            "symbol": position.symbol,
            "kind": "final",
            "reason": pending.reason,
            "source": source,
            "closed_at_ms": closed_at_ms,
            "created_cycle": int(getattr(state, "tick_count", 0) or 0),
            "position_snapshot": position_snapshot,
            "original_payload": dict(payload),
            "long_legs": long_legs,
            "short_legs": short_legs,
            "attempt_count": 0,
            "next_attempt_ms": closed_at_ms,
        }
        missing_identity_legs = pending_close_reconciliation_missing_legs(
            reconciliation
        )

        absorbed_partial_reconciliations: list[dict[str, int | str]] = []
        absorbed_partial_tasks: list[dict[str, Any]] = []
        if missing_identity_legs:
            # A final live-flat observation may finish the exact close attempt
            # represented by one still-unsettled partial task.  Its durable
            # identities must become part of the terminal owner, not a copied
            # lookup key in a second billing task.  A partial with any missing
            # evidence remains its own fail-closed owner.
            def durable_records(
                records: Any,
                *,
                venue: Venue,
            ) -> list[dict[str, Any]]:
                retained: list[dict[str, Any]] = []
                seen: set[tuple[str, str]] = set()
                for record in records if isinstance(records, list) else []:
                    if not isinstance(record, dict):
                        continue
                    if str(record.get("venue") or "") != venue.value:
                        continue
                    order_id = str(record.get("order_id") or "")
                    client_order_id = str(record.get("client_order_id") or "")
                    if not order_id and not client_order_id:
                        continue
                    if "-recovery-" in order_id.lower():
                        continue
                    identity = (order_id, "" if order_id else client_order_id)
                    if identity in seen:
                        continue
                    seen.add(identity)
                    retained.append(dict(record))
                return retained

            def merge_records(
                current: list[dict[str, Any]],
                prior: list[dict[str, Any]],
            ) -> list[dict[str, Any]]:
                merged = [dict(record) for record in current]
                seen = {
                    (
                        str(record.get("order_id") or ""),
                        "" if record.get("order_id") else str(
                            record.get("client_order_id") or ""
                        ),
                    )
                    for record in merged
                    if record.get("order_id") or record.get("client_order_id")
                }
                for record in prior:
                    identity = (
                        str(record.get("order_id") or ""),
                        "" if record.get("order_id") else str(
                            record.get("client_order_id") or ""
                        ),
                    )
                    if identity in seen:
                        continue
                    seen.add(identity)
                    merged.append(dict(record))
                return merged

            for existing in state.pending_close_reconciliations:
                if not isinstance(existing, dict):
                    continue
                if (
                    str(existing.get("position_id") or "") != pending.position_id
                    or str(existing.get("kind") or "final") != "partial"
                    or existing.get("reconciliation_status") == "evidence_debt"
                    or pending_close_reconciliation_missing_legs(existing)
                ):
                    continue
                snapshot = existing.get("position_snapshot")
                if not isinstance(snapshot, dict) or (
                    str(existing.get("symbol") or snapshot.get("symbol") or "")
                    != position.symbol
                    or str(snapshot.get("long_venue") or "")
                    != position.long_venue.value
                    or str(snapshot.get("short_venue") or "")
                    != position.short_venue.value
                ):
                    continue
                try:
                    existing_closed_at_ms = int(existing.get("closed_at_ms") or 0)
                except (TypeError, ValueError, OverflowError):
                    continue
                if existing_closed_at_ms > closed_at_ms:
                    continue

                prior_records = {
                    "long": durable_records(
                        existing.get("long_legs"), venue=position.long_venue
                    ),
                    "short": durable_records(
                        existing.get("short_legs"), venue=position.short_venue
                    ),
                }
                # A blank final leg can only be repaired by a complete prior
                # owner carrying that exact leg.  Do not cover a submitted
                # identity-less final record or combine unrelated tasks.
                if any(
                    reconciliation.get(f"{leg_label}_legs")
                    or not prior_records[leg_label]
                    for leg_label in missing_identity_legs
                ):
                    continue

                reconciliation["long_legs"] = merge_records(
                    reconciliation["long_legs"], prior_records["long"]
                )
                reconciliation["short_legs"] = merge_records(
                    reconciliation["short_legs"], prior_records["short"]
                )
                absorbed_partial_tasks.append(existing)
                absorbed_partial_reconciliations.append(
                    {
                        "kind": "partial",
                        "closed_at_ms": existing_closed_at_ms,
                    }
                )
                break

            if absorbed_partial_tasks:
                reconciliation["absorbed_partial_reconciliations"] = (
                    absorbed_partial_reconciliations
                )
            long_legs = reconciliation["long_legs"]
            short_legs = reconciliation["short_legs"]
            missing_identity_legs = pending_close_reconciliation_missing_legs(
                reconciliation
            )
        order_key = tuple(
            sorted(
                str(record.get("order_id") or record.get("client_order_id") or "")
                for record in [*long_legs, *short_legs]
                if record.get("order_id") or record.get("client_order_id")
            )
        )
        if not order_key and not missing_identity_legs:
            # Even a zero-quantity legacy snapshot must retain an explicit
            # execution-history task when no lookup identity exists.  The
            # absence of an identity, rather than the local quantity alone,
            # determines whether venue order truth is queryable.
            missing_identity_legs = ("long", "short")
        identity_evidence = pending_close_reconciliation_identity_evidence(
            reconciliation
        )
        identity_evidence["missing_identity_legs"] = list(missing_identity_legs)
        reconciliation["identity_evidence"] = identity_evidence
        # Trusted exchange-flat truth can prove physical terminality, but not
        # ownership of a leg for which this V2 close has no durable lookup
        # identity.  Persist that distinction at the sole live-flat handoff;
        # CloseRuntime must retain it for an exact operator evidence import
        # rather than infer an order from timing, symbol, or quantity.
        unattributed_exchange_close_legs = list(missing_identity_legs)
        if unattributed_exchange_close_legs:
            reconciliation["unattributed_exchange_close_legs"] = (
                unattributed_exchange_close_legs
            )
        # A local fill quantity without either order identity cannot be queried
        # through the order-status adapters.  It is still durable accounting
        # work: dropping it would turn a proved-flat position into an
        # untracked PnL gap.  Keep the same reconciliation queue and mark the
        # task explicitly so CloseRuntime can retain it fail-closed and emit a
        # diagnostic until venue execution-history evidence is supplied.
        missing_close_order_identity = bool(missing_identity_legs)
        reconciliation.update(
            {
                "reconciliation_mode": (
                    "venue_execution_history_required"
                    if missing_close_order_identity
                    else "order_identity"
                ),
                "missing_close_order_identity": missing_close_order_identity,
                "billing_reconciliation_required": True,
            }
        )
        for existing in state.pending_close_reconciliations:
            if not isinstance(existing, dict):
                continue
            existing_key = tuple(
                sorted(
                    str(record.get("order_id") or record.get("client_order_id") or "")
                    for record in [
                        *existing.get("long_legs", []),
                        *existing.get("short_legs", []),
                    ]
                    if isinstance(record, dict)
                    and (record.get("order_id") or record.get("client_order_id"))
                )
            )
            if (
                existing.get("position_id") == pending.position_id
                and existing_key == order_key
                and not absorbed_partial_tasks
                and not (
                    missing_close_order_identity
                    and existing.get("reconciliation_mode")
                    != "venue_execution_history_required"
                )
            ):
                return True
        # Persist the complete reconciliation record before mutating the
        # in-memory queue.  If the process exits between these operations,
        # recovery can replay the critical event and rebuild the queue instead
        # of losing the billing obligation.
        registration_payload = {
            "position_id": pending.position_id,
            "symbol": position.symbol,
            "source": source,
            "long_leg_count": len(long_legs),
            "short_leg_count": len(short_legs),
            "order_ids": order_key,
            "reconciliation_mode": reconciliation["reconciliation_mode"],
            "missing_close_order_identity": missing_close_order_identity,
            "billing_reconciliation_required": True,
            "missing_identity_legs": list(missing_identity_legs),
            "identity_evidence": identity_evidence,
            "live_flat_terminal": True,
            # The complete record is the journal recovery boundary.  The
            # scalar fields above remain convenient for diagnostics and
            # backward-compatible event consumers.
            "reconciliation": dict(reconciliation),
        }
        if unattributed_exchange_close_legs:
            registration_payload["unattributed_exchange_close_legs"] = (
                unattributed_exchange_close_legs
            )
        if absorbed_partial_reconciliations:
            registration_payload["absorbed_partial_reconciliations"] = (
                absorbed_partial_reconciliations
            )
        self._journal.append_critical(
            closed_at_ms,
            "exit.pending_close_reconciliation_registered",
            registration_payload,
        )
        state.enqueue_pending_close_reconciliation(reconciliation)
        for partial in absorbed_partial_tasks:
            state.remove_pending_close_reconciliation(partial)
        return True

    def _register_accepted_order_truth_gap(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
        *,
        venue: Venue,
        leg_label: str,
        operation: str,
        source: str,
        payload: dict[str, Any],
        request: OrderRequest | None = None,
        quantity: float = 0.0,
    ) -> None:
        if self._runtime_mode != "live":
            return
        accepted_order_id = str(payload.get("accepted_order_id") or "")
        accepted_client_order_id = str(
            payload.get("accepted_client_order_id")
            or getattr(request, "client_order_id", "")
            or ""
        )
        if not accepted_order_id and not accepted_client_order_id:
            return

        record = self._close_reconciliation_record(
            venue=venue,
            order_id=accepted_order_id,
            client_order_id=accepted_client_order_id,
            quantity=0.0,
        )
        long_legs: list[dict[str, Any]] = []
        short_legs: list[dict[str, Any]] = []
        if venue == position.long_venue or leg_label == "long":
            long_legs.append(record)
        elif venue == position.short_venue or leg_label == "short":
            short_legs.append(record)
        else:
            return

        now_ms = self._now_ms()
        ledger_decision = ORDER_TRUTH_LEDGER.truth_gap_status_decision("truth_gap")
        order_truth_state = str(
            payload.get("order_truth_state") or ledger_decision.state
        )
        next_action = str(
            payload.get("next_action")
            or "reconcile_accepted_order_or_probe_live_position"
        )
        reconciliation = {
            "position_id": pending.position_id,
            "symbol": position.symbol,
            "kind": "accepted_order_truth_gap",
            "reason": pending.reason,
            "source": source,
            "operation": operation,
            "venue": venue.value,
            "leg": leg_label,
            "closed_at_ms": now_ms,
            "created_cycle": int(getattr(state, "tick_count", 0) or 0),
            "position_snapshot": self._position_snapshot_for_close_reconciliation(position),
            "original_payload": dict(payload),
            "long_legs": long_legs,
            "short_legs": short_legs,
            "attempt_count": 0,
            "next_attempt_ms": now_ms,
            "accepted_order_truth_gap": True,
            "order_truth_state": order_truth_state,
            "truth_required_by": "accepted_order_truth_gap",
            "terminal_without_truth": False,
            "requested_quantity": float(quantity or 0.0),
            "next_action": next_action,
            "probe_paths": dict(payload.get("order_truth_probe_paths") or {}),
        }
        state.enqueue_pending_close_reconciliation(reconciliation)
        self._journal.append(
            "exit.accepted_order_truth_gap_registered",
            {
                "position_id": pending.position_id,
                "symbol": position.symbol,
                "venue": venue.value,
                "leg": leg_label,
                "operation": operation,
                "source": source,
                "order_id": accepted_order_id,
                "client_order_id": accepted_client_order_id,
                "order_truth_state": order_truth_state,
                "truth_required_by": "accepted_order_truth_gap",
                "next_action": reconciliation["next_action"],
                "probe_paths": reconciliation["probe_paths"],
            },
        )

    def _emit_passive_close_terminal_resolution(
        self,
        pending: PendingPassiveClose,
        position: OpenPosition,
        *,
        source: str,
        actual_long_size: float,
        actual_short_size: float,
        extra: dict[str, Any] | None,
        exchange_truth: dict[str, Any] | None,
    ) -> None:
        """Project live-flat passive cleanup as a V1 close terminal event."""
        short_legs, long_legs = self._pending_runtime_close_legs(pending)
        long_closed = sum(leg.fill.quantity for leg in long_legs if leg.fill)
        short_closed = sum(leg.fill.quantity for leg in short_legs if leg.fill)
        if actual_long_size <= 1e-9:
            long_closed = max(
                long_closed,
                float(position.long_quantity or position.matched_quantity or 0.0),
            )
        if actual_short_size <= 1e-9:
            short_closed = max(
                short_closed,
                float(position.short_quantity or position.matched_quantity or 0.0),
            )

        now_ms = self._now_ms()
        progress_times: list[int] = []
        for fill_state in (
            getattr(pending, "maker_fill", None),
            getattr(pending, "hedge_fill", None),
        ):
            ts = int(getattr(fill_state, "last_fill_time_ms", 0) or 0)
            if ts > 0:
                progress_times.append(ts)
        for leg in [
            *getattr(pending, "short_legs", []),
            *getattr(pending, "long_legs", []),
        ]:
            fill = getattr(leg, "fill", None)
            ts = int(getattr(fill, "filled_at_ms", 0) or 0)
            if ts > 0:
                progress_times.append(ts)
        first_progress_ms = min(progress_times) if progress_times else 0
        inferred_fast_flatten = (
            "one_sided" in source
            and first_progress_ms > 0
            and now_ms - first_progress_ms <= 2_000
        )

        extra_payload = dict(extra or {})
        closure_fields = dict(extra_payload.get("closure_fields") or {})
        if not closure_fields:
            closure_fields = {
                "closure_phase": "PASSIVE_CLOSE",
                "closure_row_key": "",
                "closure_decision_id": "",
            }
        truth_payload = exchange_truth or {
            "truth_available": True,
            "positions_flat": actual_long_size <= 1e-9 and actual_short_size <= 1e-9,
            "open_orders_flat": None,
            "source": source,
        }
        self._journal.append(
            "exit.passive_close_resolved",
            {
                "position_id": pending.position_id,
                "symbol": position.symbol,
                "reason": pending.reason,
                "resolution_source": source,
                "long_closed_qty": long_closed,
                "short_closed_qty": short_closed,
                "price_pnl": 0.0,
                "net_quote": 0.0,
                "chunk_count": pending.chunk_count(),
                "total_legs": len(long_legs) + len(short_legs),
                "terminal_close_execution": True,
                "live_flat_terminal": True,
                "problem": bool(extra_payload.get("problem", False)),
                "problem_reason": str(
                    extra_payload.get("problem_reason")
                    or extra_payload.get("force_close_reason")
                    or ""
                ),
                "single_leg_fast_flatten": bool(
                    extra_payload.get("single_leg_fast_flatten", False)
                    or inferred_fast_flatten
                ),
                "single_leg_fast_flatten_threshold_ms": 2_000,
                "first_progress_ms": first_progress_ms,
                "resolved_at_ms": now_ms,
                "exchange_truth": truth_payload,
                **closure_fields,
            },
        )

    def _emit_live_flat_billing_evidence_unavailable(
        self,
        pending: PendingPassiveClose,
        position: OpenPosition,
        *,
        source: str,
        actual_long_size: float,
        actual_short_size: float,
        exchange_truth: dict[str, Any] | None,
        closure_fields: dict[str, Any],
    ) -> None:
        """Durably terminalize a proved-flat close without reconcilable fills.

        Exchange position and open-order truth is sufficient to close the
        position lifecycle, but it does not establish execution price or fees.
        The event deliberately omits ``net_quote`` so the projection closes the
        position without manufacturing a PnL fact.
        """
        long_legs, short_legs = self._pending_close_reconciliation_records(
            pending,
            position,
            extra=None,
        )
        def recovery_targets(records: list[dict[str, Any]]) -> list[dict[str, str]]:
            return [
                {
                    "venue": str(record.get("venue") or ""),
                    "order_id": str(record.get("order_id") or ""),
                    "client_order_id": str(record.get("client_order_id") or ""),
                }
                for record in records
                if record.get("order_id") or record.get("client_order_id")
            ]

        long_targets = recovery_targets(long_legs)
        short_targets = recovery_targets(short_legs)
        now_ms = self._now_ms()
        self._journal.append_critical(
            now_ms,
            "exit.billing_evidence_unavailable",
            {
                "position_id": position.position_id,
                "symbol": position.symbol,
                "long_venue": position.long_venue.value,
                "short_venue": position.short_venue.value,
                "reason": pending.reason,
                "resolution_source": source,
                # Preserve the existing terminal reason/path for downstream
                # consumers; the explicit completeness fields below carry the
                # more precise distinction for new diagnostics.
                "terminal_reason": "terminal_live_flat_without_close_order_identity",
                "close_path": "passive_close_live_flat_no_reconciliation_identity",
                "closed_at_ms": now_ms,
                "exit_quantity": position.matched_quantity,
                "expected_long_quantity": position.long_quantity,
                "expected_short_quantity": position.short_quantity,
                "actual_long_size": actual_long_size,
                "actual_short_size": actual_short_size,
                "known_long_close_quantity": sum(
                    float(record.get("quantity") or 0.0) for record in long_legs
                ),
                "known_short_close_quantity": sum(
                    float(record.get("quantity") or 0.0) for record in short_legs
                ),
                "close_quantity_evidence_complete": False,
                "close_order_identity_available": bool(long_targets or short_targets),
                "close_order_identity_complete": bool(long_targets and short_targets),
                "billing_reconciliation_required": True,
                "billing_reconciliation_targets": {
                    "long": long_targets,
                    "short": short_targets,
                },
                "entry_fee_evidence_complete": bool(
                    position.entry_fee_evidence_complete
                ),
                "terminal_accounting_status": (
                    "provisional_close_execution_evidence_unavailable"
                ),
                "net_quote_status": "provisional",
                "venue_statement_reconciled": False,
                "exchange_truth": exchange_truth or {
                    "truth_available": True,
                    "positions_flat": (
                        actual_long_size <= 1e-9 and actual_short_size <= 1e-9
                    ),
                    "source": source,
                },
                **closure_fields,
            },
        )

    def _clear_live_flat_state(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
        *,
        source: str,
        actual_long_size: float,
        actual_short_size: float,
        extra: dict[str, Any] | None = None,
        exchange_truth: dict[str, Any] | None = None,
    ) -> None:
        """Clear local passive/open state after live exchange truth is flat."""

        def add_id(values: list[str], value: Any) -> None:
            text = str(value or "")
            if text and text not in values:
                values.append(text)

        client_order_ids: list[str] = []
        order_ids: list[str] = []
        phase_state = getattr(pending, "phase_state", None)
        add_id(client_order_ids, getattr(phase_state, "maker_client_order_id", ""))
        add_id(order_ids, getattr(phase_state, "maker_order_id", ""))
        for fill_state in (
            getattr(pending, "maker_fill", None),
            getattr(pending, "hedge_fill", None),
        ):
            add_id(client_order_ids, getattr(fill_state, "client_order_id", ""))
            add_id(order_ids, getattr(fill_state, "order_id", ""))
        for leg in [
            *getattr(pending, "short_legs", []),
            *getattr(pending, "long_legs", []),
        ]:
            add_id(client_order_ids, getattr(leg, "client_order_id", ""))
            fill = getattr(leg, "fill", None)
            add_id(client_order_ids, getattr(fill, "client_order_id", ""))
            add_id(order_ids, getattr(fill, "order_id", ""))
        if extra:
            for value in extra.get("force_close_client_order_ids", []):
                add_id(client_order_ids, value)
            for value in extra.get("force_close_order_ids", []):
                add_id(order_ids, value)

        payload = {
            "position_id": pending.position_id,
            "symbol": position.symbol,
            "long_venue": position.long_venue.value,
            "short_venue": position.short_venue.value,
            "expected_size": position.matched_quantity,
            "old_quantity": position.matched_quantity,
            "actual_long_size": actual_long_size,
            "actual_short_size": actual_short_size,
            "new_quantity": 0.0,
            "source": source,
            "client_order_ids": client_order_ids,
            "order_ids": order_ids,
        }
        closure_fields = self._v1_lifecycle_passive_close_event_fields(
            state,
            pending.position_id,
            self._now_ms(),
            exchange_truth=exchange_truth,
        )
        if extra:
            payload.update(extra)
        payload.update(closure_fields)
        reconciliation_long_legs, reconciliation_short_legs = (
            self._pending_close_reconciliation_records(
                pending,
                position,
                extra=extra,
            )
        )
        external_recovery_observation = (
            is_unattributed_recovered_live_flat_reconciliation(
                {
                    "position_id": pending.position_id,
                    "kind": "final",
                    "position_snapshot": self._position_snapshot_for_close_reconciliation(
                        position
                    ),
                    "original_payload": dict(payload),
                    "long_legs": reconciliation_long_legs,
                    "short_legs": reconciliation_short_legs,
                }
            )
        )
        external_recovery_payload: dict[str, Any] | None = None
        if external_recovery_observation:
            external_recovery_payload = {
                "position_id": pending.position_id,
                "symbol": position.symbol,
                "source": source,
                "closed_at_ms": self._now_ms(),
                "accounting_owner": "external_unattributed",
                "local_order_identity_present": False,
                "exchange_truth": exchange_truth or {
                    "truth_available": True,
                    "positions_flat": (
                        actual_long_size <= 1e-9 and actual_short_size <= 1e-9
                    ),
                    "source": source,
                },
            }
        missing = object()
        original_pending = state.pending_passive_closes.get(pending.position_id, missing)
        original_open = state.open_positions.get(pending.position_id, missing)
        original_last_error = getattr(state, "last_error", None)
        original_lifecycle = state.lifecycle
        original_risk_mode = state.risk_mode
        original_recovery_blocked_reason = state.recovery_blocked_reason
        original_recovery_blocked_at_ms = state.recovery_blocked_at_ms
        original_global_risk_reason = state.global_risk_reason
        state.set_pending_close_reconciliations(
            getattr(state, "pending_close_reconciliations", [])
        )
        original_reconciliations = [
            dict(item) for item in state.pending_close_reconciliations
        ]
        failure_reason = "pending_close_reconciliation_registration_failed"
        try:
            reconciliation_registered = False
            if not external_recovery_observation:
                reconciliation_registered = (
                    self._register_close_reconciliation_after_live_flat(
                        state,
                        pending,
                        position,
                        source=source,
                        payload=payload,
                        extra=extra,
                    )
                )
            failure_reason = "managed_state_clear_failed"
            state.pending_passive_closes.pop(pending.position_id, None)
            state.open_positions.pop(pending.position_id, None)
            last_error = getattr(state, "last_error", None)
            if isinstance(last_error, str) and self._last_error_matches_live_flat_cleanup(
                last_error,
                position_id=pending.position_id,
                symbol=position.symbol,
            ):
                state.last_error = None
            failure_reason = "recovery_core_clear_failed"
            core_decision = V1RecoveryDecisionCore().decide(
                RecoveryEvidenceSnapshot(
                    local_open_positions=tuple(state.open_positions.values()),
                    pending_entries=tuple(state.pending_entries.values()),
                    residual_repairs=tuple(
                        getattr(state, "pending_residual_repairs", ()) or ()
                    ),
                    passive_closes=tuple(state.pending_passive_closes.values()),
                    exchange_truth=exchange_truth or {
                        "truth_available": False,
                        "missing_evidence": ["live_flat_exchange_truth"],
                    },
                    prior_recovery_block_reason=state.recovery_blocked_reason,
                    operator_fail_closed=(
                        getattr(state.operator, "requested_mode", None)
                        == GlobalRiskMode.FAIL_CLOSED
                    ),
                    recovery_work_items=tuple(
                        RecoveryLedger.from_local_and_exchange_truth(
                            local=state,
                            exchange_truth=exchange_truth,
                        ).work_items
                    ),
                )
            )
            clear_legacy_recovery_block_via_core(
                state,
                core_decision,
                journal=self._journal,
            )
            if not external_recovery_observation:
                failure_reason = "terminal_close_resolution_failed"
                terminal_extra = dict(extra or {})
                terminal_extra["closure_fields"] = closure_fields
                self._emit_passive_close_terminal_resolution(
                    pending,
                    position,
                    source=source,
                    actual_long_size=actual_long_size,
                    actual_short_size=actual_short_size,
                    extra=terminal_extra,
                    exchange_truth=exchange_truth,
                )
                if not reconciliation_registered:
                    failure_reason = "billing_evidence_terminalization_failed"
                    self._emit_live_flat_billing_evidence_unavailable(
                        pending,
                        position,
                        source=source,
                        actual_long_size=actual_long_size,
                        actual_short_size=actual_short_size,
                        exchange_truth=exchange_truth,
                        closure_fields=closure_fields,
                    )
        except Exception as error:
            state.pending_close_reconciliations = original_reconciliations
            if original_pending is missing:
                state.pending_passive_closes.pop(pending.position_id, None)
            else:
                state.pending_passive_closes[pending.position_id] = original_pending
            if original_open is missing:
                state.open_positions.pop(pending.position_id, None)
            else:
                state.open_positions[pending.position_id] = original_open
            state.last_error = original_last_error
            state.lifecycle = original_lifecycle
            state.risk_mode = original_risk_mode
            state.recovery_blocked_reason = original_recovery_blocked_reason
            state.recovery_blocked_at_ms = original_recovery_blocked_at_ms
            state.global_risk_reason = original_global_risk_reason
            self._journal.append(
                "exit.passive_close_live_flat_cleanup_failed",
                {
                    "position_id": pending.position_id,
                    "symbol": position.symbol,
                    "source": source,
                    "reason": failure_reason,
                    "error": str(error),
                },
            )
            return

        if external_recovery_payload is not None:
            self._journal.append_critical(
                int(external_recovery_payload["closed_at_ms"]),
                "recovery.external_pair_flat_observed",
                external_recovery_payload,
            )
        self._journal.append("runtime.position_drift_detected", payload)
        self._journal.append("exit.passive_close_fallback_terminal_flat", payload)
        self._journal.append(
            "runtime.position_lifecycle_terminal",
            {
                **payload,
                "terminal_state": "flat",
                "terminal_reason": source,
                "problem": bool(payload.get("problem", False)),
                "problem_reason": str(
                    payload.get("problem_reason")
                    or payload.get("force_close_reason")
                    or ""
                ),
            },
        )
        self._journal.append(
            "recovery.flat",
            {
                "position_id": pending.position_id,
                "symbol": position.symbol,
                "source": source,
                "billing_reconciliation_pending": bool(reconciliation_registered),
                **closure_fields,
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

    @staticmethod
    def _is_recovered_position(position: OpenPosition) -> bool:
        return str(position.position_id).startswith("live-recovered:")

    async def _fetch_live_position_size(self, venue: Venue, symbol: str) -> float:
        adapter = self._adapter(venue)
        if adapter is None:
            return 0.0
        try:
            pos = await adapter.fetch_position(symbol)
            qty = getattr(pos, "quantity", None)
            if isinstance(qty, (int, float)) and math.isfinite(float(qty)):
                return float(qty)
        except Exception:
            pass
        return 0.0

    @staticmethod
    def _last_error_matches_live_flat_cleanup(
        last_error: str,
        *,
        position_id: str,
        symbol: str,
    ) -> bool:
        if position_id and position_id in last_error:
            return True
        return (
            symbol in last_error
            and "reduceonly" in last_error.replace(" ", "").lower()
        )

    async def _fetch_live_position_snapshot(
        self,
        venue: Venue,
        symbol: str,
    ) -> tuple[Any | None, str | None]:
        adapter = self._adapter(venue)
        if adapter is None:
            return None, "adapter_missing"
        try:
            pos = await adapter.fetch_position(symbol)
        except Exception as exc:
            return None, str(exc)
        qty = getattr(pos, "quantity", None)
        side = getattr(pos, "side", None)
        if not isinstance(qty, (int, float)) or not math.isfinite(float(qty)):
            return None, f"invalid_quantity:{qty!r}"
        if not isinstance(side, Side):
            return None, f"invalid_side:{side!r}"
        return pos, None

    @staticmethod
    def _live_position_quantity(snapshot: Any | None) -> float:
        qty = getattr(snapshot, "quantity", None)
        if isinstance(qty, (int, float)) and math.isfinite(float(qty)):
            return abs(float(qty))
        return 0.0

    async def _resolve_flat_maker_leg_from_live_truth(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
        *,
        maker_leg_label: str,
    ) -> PassiveCloseLiveTruthResolution:
        """Avoid submitting a reduce-only maker order to a leg live truth says is flat."""
        live_long, live_long_error = await self._fetch_live_position_snapshot(
            position.long_venue,
            position.symbol,
        )
        live_short, live_short_error = await self._fetch_live_position_snapshot(
            position.short_venue,
            position.symbol,
        )
        if live_long_error or live_short_error:
            self._journal.append(
                "exit.passive_close_maker_leg_live_precheck_untrusted",
                {
                    "position_id": pending.position_id,
                    "symbol": position.symbol,
                    "long_venue": position.long_venue.value,
                    "short_venue": position.short_venue.value,
                    "maker_leg": maker_leg_label,
                    "long_error": live_long_error,
                    "short_error": live_short_error,
                    "decision": "continue_pending_passive_close",
                },
            )
            return PassiveCloseLiveTruthResolution.CONTINUE_MAKER

        live_long_qty = self._live_position_quantity(live_long)
        live_short_qty = self._live_position_quantity(live_short)
        maker_qty = live_long_qty if maker_leg_label == "long" else live_short_qty
        other_qty = live_short_qty if maker_leg_label == "long" else live_long_qty
        if maker_qty > 1e-9:
            return PassiveCloseLiveTruthResolution.CONTINUE_MAKER

        other_venue = position.short_venue if maker_leg_label == "long" else position.long_venue
        other_snapshot = live_short if maker_leg_label == "long" else live_long
        other_leg_label = "short" if maker_leg_label == "long" else "long"
        decision = "clear_live_flat" if other_qty <= 1e-9 else "flatten_other_live_leg"
        self._journal.append(
            "exit.passive_close_maker_leg_live_flat_precheck",
            {
                "position_id": pending.position_id,
                "symbol": position.symbol,
                "long_venue": position.long_venue.value,
                "short_venue": position.short_venue.value,
                "maker_leg": maker_leg_label,
                "live_long_size": live_long_qty,
                "live_short_size": live_short_qty,
                "decision": decision,
            },
        )

        if other_qty <= 1e-9:
            if await self._clear_if_live_flat(
                state,
                pending,
                position,
                source="passive_close_maker_leg_live_flat_precheck",
                extra={"maker_leg": maker_leg_label},
            ):
                return PassiveCloseLiveTruthResolution.CLEARED
            pending.next_retry_at_ms = self._now_ms() + 5_000
            return PassiveCloseLiveTruthResolution.STOP_RETRY

        if await self._flatten_live_one_sided_position(
            state,
            pending,
            position,
            venue=other_venue,
            live_snapshot=other_snapshot,
            leg_label=other_leg_label,
        ):
            return PassiveCloseLiveTruthResolution.CLEARED
        pending.next_retry_at_ms = max(pending.next_retry_at_ms, self._now_ms() + 5_000)
        return PassiveCloseLiveTruthResolution.STOP_RETRY

    def _pending_runtime_close_legs(
        self,
        pending: PendingPassiveClose,
    ) -> tuple[list[CloseExecutionLeg], list[CloseExecutionLeg]]:
        short_legs: list[CloseExecutionLeg] = []
        for leg in pending.short_legs:
            fill = self._persisted_leg_fill(leg)
            if fill is not None:
                short_legs.append(CloseExecutionLeg(
                    fill=fill,
                    client_order_id=leg.client_order_id,
                    submit_started_at_ms=leg.submit_started_at_ms,
                    latency_ms=leg.latency_ms,
                ))

        long_legs: list[CloseExecutionLeg] = []
        for leg in pending.long_legs:
            fill = self._persisted_leg_fill(leg)
            if fill is not None:
                long_legs.append(CloseExecutionLeg(
                    fill=fill,
                    client_order_id=leg.client_order_id,
                    submit_started_at_ms=leg.submit_started_at_ms,
                    latency_ms=leg.latency_ms,
                ))
        return short_legs, long_legs

    async def _abort_and_compensate_min_notional(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
        *,
        hedge_venue: Venue,
        hedge_leg: str,
        missing_quantity: float,
        normalized_quantity: float,
        leg_notional_quote: float,
        venue_min_notional_quote: float,
        min_notional_source: str,
        failed_stage: str,
        source: str,
    ) -> bool:
        """V1 terminal small-fill path: abort accumulation and flatten live residuals."""
        if missing_quantity > pending.last_small_fill_missing_quantity + 1e-9:
            pending.small_fill_min_notional_attempts += 1
            pending.last_small_fill_missing_quantity = missing_quantity

        accumulating_payload = {
            "position_id": position.position_id,
            "symbol": position.symbol,
            "execution_kind": "exit",
            "hedge_venue": hedge_venue.value,
            "hedge_leg": hedge_leg,
            "attempt": pending.small_fill_min_notional_attempts,
            "max_attempts": self._config.maker_min_notional_accumulation_attempts,
            "missing_hedge_quantity": missing_quantity,
            "normalized_quantity": normalized_quantity,
            "leg_notional_quote": leg_notional_quote,
            "venue_min_notional_quote": venue_min_notional_quote,
            "min_notional_source": min_notional_source,
            "source": source,
        }
        self._journal.append("execution.min_notional_accumulating", accumulating_payload)

        abort_payload = {
            "position_id": position.position_id,
            "symbol": position.symbol,
            "execution_kind": "exit",
            "hedge_venue": hedge_venue.value,
            "attempt": pending.small_fill_min_notional_attempts,
            "missing_hedge_quantity": missing_quantity,
            "normalized_quantity": normalized_quantity,
            "leg_notional_quote": leg_notional_quote,
            "venue_min_notional_quote": venue_min_notional_quote,
            "min_notional_source": min_notional_source,
            "source": source,
        }
        self._journal.append("execution.min_notional_abort_and_flatten", abort_payload)

        from lightfee.engine.close_executor import CloseExecutor

        if not isinstance(self._close_executor, CloseExecutor):
            self._journal.append(
                "exit.passive_close_min_notional_compensation_unavailable",
                {
                    "position_id": position.position_id,
                    "hedge_venue": hedge_venue.value,
                    "missing_hedge_quantity": missing_quantity,
                    "reason": "no close_executor injected",
                },
            )
            return False

        short_legs, long_legs = self._pending_runtime_close_legs(pending)
        await self._close_executor.compensate_failed_full_close(
            position=position,
            close_reason="passive_close_hedge_below_min_notional",
            failed_stage=failed_stage,
            failed_venue=hedge_venue,
            error=RuntimeError("passive close hedge leg below minimum notional"),
            short_legs=short_legs,
            long_legs=long_legs,
            state=state,
        )

        if await self._clear_if_live_flat(
            state,
            pending,
            position,
            source="passive_close_min_notional_compensated_flat",
            extra={
                "hedge_venue": hedge_venue.value,
                "hedge_leg": hedge_leg,
                "missing_hedge_quantity": missing_quantity,
            },
        ):
            return True

        pending.next_retry_at_ms = 0
        return False

    async def _flatten_live_one_sided_position(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
        *,
        venue: Venue,
        live_snapshot: Any,
        leg_label: str,
    ) -> bool:
        """Close actual live one-sided exposure before using stale local deltas."""
        live_qty = self._live_position_quantity(live_snapshot)
        if live_qty <= 1e-9:
            return True

        live_side = getattr(live_snapshot, "side", None)
        close_side = live_side.opposite() if isinstance(live_side, Side) else (
            Side.SELL if leg_label == "long" else Side.BUY
        )
        adapter = self._adapter(venue)
        if adapter is None:
            return False

        open_orders_flat, open_orders_evidence = await self._probe_venue_open_orders_flat(
            venue,
            position.symbol,
            self._adapters,
        )
        if open_orders_flat is not True:
            self._journal.append(
                "exit.passive_close_live_one_sided_truth_gap",
                {
                    "position_id": position.position_id,
                    "symbol": position.symbol,
                    "venue": venue.value,
                    "leg": leg_label,
                    "live_quantity": live_qty,
                    "live_side": live_side.value if isinstance(live_side, Side) else str(live_side),
                    "open_orders_flat": open_orders_flat,
                    "open_orders_evidence": open_orders_evidence,
                    "decision": "retain_pending",
                    "reason": "one_sided_flatten_requires_open_order_flat_proof",
                },
            )
            pending.next_retry_at_ms = self._now_ms() + 5_000
            return False

        try:
            normalized_qty = float(await adapter.normalize_quantity(position.symbol, live_qty))
        except Exception as exc:
            self._journal.append(
                "exit.passive_close_live_one_sided_normalize_failed",
                {
                    "position_id": position.position_id,
                    "venue": venue.value,
                    "requested": live_qty,
                    "error": str(exc),
                },
            )
            return False

        price_hint = self._resolve_local_l2_mid(venue, position.symbol)
        price_hint, price_source = await self._resolve_hedge_reference_price(
            venue, position.symbol, close_side, price_hint,
        )
        if price_hint <= 0.0:
            live_entry_price = self._positive_float(
                getattr(live_snapshot, "entry_price", 0.0)
            )
            if live_entry_price > 0.0:
                price_hint = live_entry_price
                price_source = "live_position_entry_price"
        min_notional_quote, min_notional_source = await self._resolve_hedge_min_notional_quote(
            venue,
            position.symbol,
        )
        min_notional_violation = self._check_hedge_min_notional(
            venue,
            position.symbol,
            close_side,
            normalized_qty,
            price_hint,
            price_source=price_source,
            min_notional_quote=min_notional_quote,
            min_notional_source=min_notional_source,
        )
        if normalized_qty <= 1e-12 or min_notional_violation is not None:
            rule_evidence = await self._hedge_rule_diagnostic_payload(
                venue,
                position.symbol,
                min_notional_source=min_notional_source,
            )
            leg_notional = 0.0
            min_notional = 0.0
            if min_notional_violation is not None:
                leg_notional = min_notional_violation["leg_notional"]
                min_notional = min_notional_violation["min_notional"]
            reason = (
                "normalized_quantity_zero"
                if normalized_qty <= 1e-12
                else str(min_notional_violation.get("reason") or "min_notional_rejected")
            )
            self._journal.append(
                "exit.passive_close_hedge_dust_aborted",
                {
                    "position_id": position.position_id,
                    "symbol": position.symbol,
                    "hedge_venue": venue.value,
                    "hedge_leg": leg_label,
                    "requested": live_qty,
                    "normalized_quantity": normalized_qty,
                    "price_hint": price_hint,
                    "price_source": price_source,
                    "reason": reason,
                    "maker_terminal": True,
                    "leg_notional_quote": leg_notional,
                    "venue_min_notional_quote": min_notional,
                    **rule_evidence,
                },
            )
            compensation_source = (
                "fallback_live_one_sided_price_unavailable"
                if reason == "price_unavailable_for_min_notional"
                else "fallback_live_one_sided_min_notional"
            )
            return await self._abort_and_compensate_min_notional(
                state,
                pending,
                position,
                hedge_venue=venue,
                hedge_leg=leg_label,
                missing_quantity=live_qty,
                normalized_quantity=normalized_qty,
                leg_notional_quote=leg_notional,
                venue_min_notional_quote=min_notional,
                min_notional_source=min_notional_source,
                failed_stage=(
                    (pending.short_stage or "exit_short")
                    if leg_label == "short"
                    else (pending.long_stage or "exit_long")
                ),
                source=compensation_source,
            )

        stage = "exit_live_one_sided_short" if leg_label == "short" else "exit_live_one_sided_long"
        client_order_id = compact_client_order_id(position.position_id, stage)
        request = OrderRequest(
            venue=venue,
            symbol=position.symbol,
            side=close_side,
            quantity=normalized_qty,
            price=price_hint if price_hint > 0 else None,
            reduce_only=True,
            time_in_force=TimeInForce.IOC,
            client_order_id=client_order_id,
        )
        self._claim_close_order_intent(
            pending,
            position,
            request,
            leg_label=leg_label,
            operation="place_order_live_one_sided",
        )
        try:
            fill = await adapter.place_order(request)
        except Exception as exc:
            uncertainty_payload: dict[str, Any] = {}
            if isinstance(exc, OrderSubmitError):
                uncertainty_payload = build_order_submit_uncertainty_payload(
                    exc,
                    venue=venue,
                    operation="place_order",
                    request=request,
                    default_client_order_id=client_order_id,
                )
            error_payload = {
                "position_id": position.position_id,
                "venue": venue.value,
                "leg": leg_label,
                "live_quantity": live_qty,
                "normalized_quantity": normalized_qty,
                "side": close_side.value,
                "error": str(exc),
            }
            error_payload.update(uncertainty_payload)
            self._journal.append(
                "exit.passive_close_live_one_sided_error",
                error_payload,
            )
            if is_order_truth_gap(exc):
                self._register_accepted_order_truth_gap(
                    state,
                    pending,
                    position,
                    venue=venue,
                    leg_label=leg_label,
                    operation="place_order",
                    source="passive_close_live_one_sided_order_truth_gap",
                    payload=error_payload,
                    request=request,
                    quantity=normalized_qty,
                )
                pending.next_retry_at_ms = self._now_ms() + 5_000
                return False
            if self._close_executor is not None:
                from lightfee.core.errors import SubmitFailureClass

                short_legs, long_legs = self._pending_runtime_close_legs(pending)
                force_payload = {
                    "position_id": position.position_id,
                    "symbol": position.symbol,
                    "venue": venue.value,
                    "leg": leg_label,
                    "live_quantity": live_qty,
                    "normalized_quantity": normalized_qty,
                    "side": close_side.value,
                    "failed_stage": stage,
                    "failed_error": str(exc),
                    "problem": True,
                    "reason": "normal_one_sided_flatten_failed_force_close",
                }
                try:
                    await self._close_executor.compensate_failed_full_close(
                        position,
                        pending.reason,
                        stage,
                        venue,
                        OrderSubmitError(
                            SubmitFailureClass.UNCERTAIN,
                            str(exc) or "one-sided flatten failed",
                        ),
                        short_legs,
                        long_legs,
                        state,
                    )
                except Exception as force_exc:
                    self._journal.append(
                        "exit.passive_close_live_one_sided_force_close_problem",
                        {
                            **force_payload,
                            "result": "failed",
                            "force_close_error": str(force_exc),
                        },
                    )
                else:
                    force_close_client_order_ids: list[str] = []
                    force_close_order_ids: list[str] = []
                    for leg in [*short_legs, *long_legs]:
                        if leg.client_order_id:
                            force_close_client_order_ids.append(leg.client_order_id)
                        fill = getattr(leg, "fill", None)
                        order_id = getattr(fill, "order_id", "")
                        if order_id:
                            force_close_order_ids.append(str(order_id))
                    self._journal.append(
                        "exit.passive_close_live_one_sided_force_close_problem",
                        {
                            **force_payload,
                            "result": "submitted",
                            "force_close_client_order_ids": force_close_client_order_ids,
                            "force_close_order_ids": force_close_order_ids,
                        },
                    )
                    if await self._clear_if_live_flat(
                        state,
                        pending,
                        position,
                        source="passive_close_live_one_sided_force_close_problem",
                        extra={
                            "flattened_venue": venue.value,
                            "flattened_quantity": normalized_qty,
                            "problem": True,
                            "force_close_reason": force_payload["reason"],
                            "force_close_client_order_ids": force_close_client_order_ids,
                            "force_close_order_ids": force_close_order_ids,
                        },
                    ):
                        return True
            if await self._clear_if_live_flat(
                state,
                pending,
                position,
                source="passive_close_live_one_sided_error_flat_probe",
                extra={
                    "flattened_venue": venue.value,
                    "flattened_quantity": normalized_qty,
                    "flatten_error": str(exc),
                },
            ):
                return True
            pending.next_retry_at_ms = self._now_ms() + 5_000
            return False
        self._journal.append(
            "exit.passive_close_live_one_sided_flatten",
            {
                "position_id": position.position_id,
                "venue": venue.value,
                "leg": leg_label,
                "live_quantity": live_qty,
                "normalized_quantity": normalized_qty,
                "filled_quantity": getattr(fill, "quantity", 0.0),
                "side": close_side.value,
                "client_order_id": client_order_id,
            },
        )

        self._record_close_execution_leg(
            pending,
            leg_label=leg_label,
            fill=fill,
            client_order_id=client_order_id,
            submitted_at_ms=self._now_ms(),
        )

        if await self._clear_if_live_flat(
            state,
            pending,
            position,
            source="passive_close_live_one_sided_flattened",
            extra={
                "flattened_venue": venue.value,
                "flattened_quantity": normalized_qty,
            },
        ):
            return True

        pending.next_retry_at_ms = self._now_ms() + 5_000
        return False

    async def _handle_live_balanced_close_target(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
        *,
        live_quantity: float,
        source: str,
    ) -> bool:
        """Close the matched quantity from live truth instead of stale local fills."""
        from lightfee.engine.close_executor import CloseExecutor

        if live_quantity <= 1e-9:
            return await self._clear_if_live_flat(
                state,
                pending,
                position,
                source=f"{source}_flat_probe",
            )

        if not isinstance(self._close_executor, CloseExecutor):
            pending.next_retry_at_ms = self._now_ms() + 5_000
            return False

        self._journal.append(
            "exit.passive_close_live_matched_close",
            {
                "position_id": pending.position_id,
                "symbol": position.symbol,
                "live_matched_quantity": live_quantity,
                "source": source,
            },
        )
        close_result = await self._close_executor.execute_close(
            position=position,
            reason=pending.reason,
            now_ms=self._now_ms(),
            long_price_hint=self._resolve_local_l2_mid(position.long_venue, position.symbol),
            short_price_hint=self._resolve_local_l2_mid(position.short_venue, position.symbol),
            total_quantity=live_quantity,
            state=state,
            short_stage=pending.short_stage or "exit_short",
            long_stage=pending.long_stage or "exit_long",
        )
        if close_result is None:
            pending.next_retry_at_ms = self._now_ms() + 5_000
            return False
        if await self._clear_if_live_flat(
            state,
            pending,
            position,
            source=f"{source}_matched_close_flat_probe",
            extra={"live_matched_quantity": live_quantity},
        ):
            return True
        pending.next_retry_at_ms = self._now_ms() + 5_000
        return False

    async def _handle_live_imbalanced_positions(
        self,
        state: EngineState,
        pending: PendingPassiveClose,
        position: OpenPosition,
        *,
        live_long: Any,
        live_short: Any,
        live_long_qty: float,
        live_short_qty: float,
    ) -> bool:
        """Rebuild fallback target from nonzero but imbalanced live positions."""
        live_matched_qty = min(live_long_qty, live_short_qty)
        excess_qty = abs(live_long_qty - live_short_qty)
        if excess_qty <= 1e-9:
            return await self._handle_live_balanced_close_target(
                state,
                pending,
                position,
                live_quantity=live_matched_qty,
                source="fallback_live_balanced",
            )

        if live_long_qty > live_short_qty:
            excess_venue = position.long_venue
            excess_snapshot = live_long
            excess_leg = "long"
            default_close_side = Side.SELL
            failed_stage = pending.long_stage or "exit_long"
        else:
            excess_venue = position.short_venue
            excess_snapshot = live_short
            excess_leg = "short"
            default_close_side = Side.BUY
            failed_stage = pending.short_stage or "exit_short"

        live_side = getattr(excess_snapshot, "side", None)
        close_side = live_side.opposite() if isinstance(live_side, Side) else default_close_side
        adapter = self._adapter(excess_venue)
        if adapter is None:
            pending.next_retry_at_ms = self._now_ms() + 5_000
            return False

        try:
            normalized_excess = float(await adapter.normalize_quantity(position.symbol, excess_qty))
        except Exception as exc:
            self._journal.append(
                "exit.passive_close_live_imbalanced_normalize_failed",
                {
                    "position_id": pending.position_id,
                    "symbol": position.symbol,
                    "excess_venue": excess_venue.value,
                    "excess_quantity": excess_qty,
                    "error": str(exc),
                },
            )
            pending.next_retry_at_ms = self._now_ms() + 5_000
            return False

        price_hint = self._resolve_local_l2_mid(excess_venue, position.symbol)
        price_hint, price_source = await self._resolve_hedge_reference_price(
            excess_venue, position.symbol, close_side, price_hint,
        )
        min_notional_quote, min_notional_source = await self._resolve_hedge_min_notional_quote(
            excess_venue,
            position.symbol,
        )
        min_notional_violation = self._check_hedge_min_notional(
            excess_venue,
            position.symbol,
            close_side,
            normalized_excess,
            price_hint,
            price_source=price_source,
            min_notional_quote=min_notional_quote,
            min_notional_source=min_notional_source,
        )
        self._journal.append(
            "exit.passive_close_live_imbalanced",
            {
                "position_id": pending.position_id,
                "symbol": position.symbol,
                "live_long_quantity": live_long_qty,
                "live_short_quantity": live_short_qty,
                "live_matched_quantity": live_matched_qty,
                "excess_venue": excess_venue.value,
                "excess_leg": excess_leg,
                "excess_quantity": excess_qty,
                "normalized_excess_quantity": normalized_excess,
                "excess_notional_quote": normalized_excess * max(price_hint, 0.0),
                "price_source": price_source,
                "min_notional_rejected": min_notional_violation is not None,
            },
        )

        if normalized_excess <= 1e-12 or min_notional_violation is not None:
            rule_evidence = await self._hedge_rule_diagnostic_payload(
                excess_venue,
                position.symbol,
                min_notional_source=min_notional_source,
            )
            leg_notional = 0.0
            min_notional = 0.0
            if min_notional_violation is not None:
                leg_notional = min_notional_violation["leg_notional"]
                min_notional = min_notional_violation["min_notional"]
            reason = (
                "normalized_quantity_zero"
                if normalized_excess <= 1e-12
                else str(min_notional_violation.get("reason") or "min_notional_rejected")
            )
            self._journal.append(
                "exit.passive_close_hedge_dust_aborted",
                {
                    "position_id": position.position_id,
                    "symbol": position.symbol,
                    "hedge_venue": excess_venue.value,
                    "hedge_leg": excess_leg,
                    "requested": excess_qty,
                    "normalized_quantity": normalized_excess,
                    "price_hint": price_hint,
                    "price_source": price_source,
                    "reason": reason,
                    "maker_terminal": True,
                    "leg_notional_quote": leg_notional,
                    "venue_min_notional_quote": min_notional,
                    **rule_evidence,
                },
            )
            compensation_source = (
                "fallback_live_imbalanced_price_unavailable"
                if reason == "price_unavailable_for_min_notional"
                else "fallback_live_imbalanced_min_notional"
            )
            return await self._abort_and_compensate_min_notional(
                state,
                pending,
                position,
                hedge_venue=excess_venue,
                hedge_leg=excess_leg,
                missing_quantity=excess_qty,
                normalized_quantity=normalized_excess,
                leg_notional_quote=leg_notional,
                venue_min_notional_quote=min_notional,
                min_notional_source=min_notional_source,
                failed_stage=failed_stage,
                source=compensation_source,
            )

        client_order_id = compact_client_order_id(
            position.position_id,
            f"exit_live_excess_{excess_leg}",
        )
        request = OrderRequest(
            venue=excess_venue,
            symbol=position.symbol,
            side=close_side,
            quantity=normalized_excess,
            price=price_hint if price_hint > 0 else None,
            reduce_only=True,
            time_in_force=TimeInForce.IOC,
            client_order_id=client_order_id,
        )
        self._claim_close_order_intent(
            pending,
            position,
            request,
            leg_label=excess_leg,
            operation="place_order_live_imbalanced_excess",
        )
        try:
            fill = await adapter.place_order(request)
        except Exception as exc:
            self._journal.append(
                "exit.passive_close_live_imbalanced_excess_error",
                {
                    "position_id": pending.position_id,
                    "symbol": position.symbol,
                    "excess_venue": excess_venue.value,
                    "excess_quantity": normalized_excess,
                    "error": str(exc),
                },
            )
            if await self._clear_if_live_flat(
                state,
                pending,
                position,
                source="fallback_live_imbalanced_excess_error_flat_probe",
                extra={
                    "excess_venue": excess_venue.value,
                    "excess_quantity": normalized_excess,
                    "flatten_error": str(exc),
                },
            ):
                return True
            pending.next_retry_at_ms = self._now_ms() + 5_000
            return False

        self._record_close_execution_leg(
            pending,
            leg_label=excess_leg,
            fill=fill,
            client_order_id=client_order_id,
            submitted_at_ms=self._now_ms(),
        )
        self._journal.append(
            "exit.passive_close_live_imbalanced_excess_flattened",
            {
                "position_id": pending.position_id,
                "symbol": position.symbol,
                "excess_venue": excess_venue.value,
                "excess_leg": excess_leg,
                "excess_quantity": normalized_excess,
                "filled_quantity": getattr(fill, "quantity", 0.0),
                "live_matched_quantity": live_matched_qty,
            },
        )

        return await self._handle_live_balanced_close_target(
            state,
            pending,
            position,
            live_quantity=live_matched_qty,
            source="fallback_live_imbalanced",
        )

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

        live_long, live_long_error = await self._fetch_live_position_snapshot(
            position.long_venue, position.symbol
        )
        live_short, live_short_error = await self._fetch_live_position_snapshot(
            position.short_venue, position.symbol
        )
        live_long_qty = self._live_position_quantity(live_long)
        live_short_qty = self._live_position_quantity(live_short)
        if live_long_error is None and live_short_error is None:
            if live_long_qty <= 1e-9 and live_short_qty <= 1e-9:
                long_open_orders_flat, long_open_orders_evidence = await self._probe_venue_open_orders_flat(
                    position.long_venue,
                    position.symbol,
                    self._adapters,
                )
                short_open_orders_flat, short_open_orders_evidence = await self._probe_venue_open_orders_flat(
                    position.short_venue,
                    position.symbol,
                    self._adapters,
                )
                if long_open_orders_flat is not True or short_open_orders_flat is not True:
                    self._journal.append(
                        "exit.passive_close_clear_flat_untrusted",
                        {
                            "position_id": pending.position_id,
                            "symbol": position.symbol,
                            "long_venue": position.long_venue.value,
                            "short_venue": position.short_venue.value,
                            "long_open_orders": long_open_orders_evidence,
                            "short_open_orders": short_open_orders_evidence,
                            "live_truth_trusted": False,
                            "decision": "retain_pending",
                            "source": "pending_passive_close_flat_probe",
                        },
                    )
                    return False
                self._clear_live_flat_state(
                    state,
                    pending,
                    position,
                    source="pending_passive_close_flat_probe",
                    actual_long_size=live_long_qty,
                    actual_short_size=live_short_qty,
                    exchange_truth=self._live_flat_exchange_truth(
                        position,
                        long_snap=live_long,
                        short_snap=live_short,
                        long_open_orders_evidence=long_open_orders_evidence,
                        short_open_orders_evidence=short_open_orders_evidence,
                    ),
                )
                return True

            live_one_sided = (live_long_qty > 1e-9) != (live_short_qty > 1e-9)
            if live_one_sided:
                if live_long_qty > 1e-9:
                    return await self._flatten_live_one_sided_position(
                        state,
                        pending,
                        position,
                        venue=position.long_venue,
                        live_snapshot=live_long,
                        leg_label="long",
                    )
                return await self._flatten_live_one_sided_position(
                    state,
                    pending,
                    position,
                    venue=position.short_venue,
                    live_snapshot=live_short,
                    leg_label="short",
                )
            if live_long_qty > 1e-9 and live_short_qty > 1e-9:
                return await self._handle_live_imbalanced_positions(
                    state,
                    pending,
                    position,
                    live_long=live_long,
                    live_short=live_short,
                    live_long_qty=live_long_qty,
                    live_short_qty=live_short_qty,
                )
            elif await self._clear_if_live_flat(
                state,
                pending,
                position,
                source="pending_passive_close_flat_probe",
            ):
                return True

        if self._close_executor is None:
            deadline = self._passive_close_fallback_deadline_decision(
                pending, position, self._now_ms(),
            )
            if deadline.get("hard_breached"):
                self._enter_passive_close_execution_fail_closed(
                    state,
                    pending,
                    position,
                    deadline,
                    source="fallback_no_close_executor",
                    error="no close_executor injected",
                )
                return False
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
            deadline = self._passive_close_fallback_deadline_decision(
                pending, position, self._now_ms(),
            )
            if deadline.get("hard_breached"):
                self._enter_passive_close_execution_fail_closed(
                    state,
                    pending,
                    position,
                    deadline,
                    source="fallback_invalid_close_executor",
                    error="close_executor is not CloseExecutor",
                )
                return False
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
            if await self._enforce_passive_close_hedge_submit_deadline(
                state,
                pending,
                position,
                result,
                source="fallback_unhedged_hedge_submit",
            ):
                return False
            if not result.success:
                if result.truth_gap:
                    return await self._handle_hedge_truth_gap_result(
                        state,
                        pending,
                        position,
                        result,
                        source="fallback_unhedged_hedge_ack",
                    )
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
            if close_result is None:
                deadline = self._passive_close_fallback_deadline_decision(
                    pending, position, self._now_ms(),
                )
                if deadline.get("hard_breached"):
                    self._enter_passive_close_execution_fail_closed(
                        state,
                        pending,
                        position,
                        deadline,
                        source="fallback_aggressive_null_result",
                        error="close_executor returned None",
                    )
                    return False
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

            long_closed = close_result.long_close_qty if hasattr(close_result, "long_close_qty") else 0.0
            short_closed = close_result.short_close_qty if hasattr(close_result, "short_close_qty") else 0.0

            if long_closed < 1e-12 and short_closed < 1e-12:
                has_pending_close = any(
                    pc.position_id == pending.position_id
                    for pc in state.pending_closes.values()
                )
                if not has_pending_close:
                    deadline = self._passive_close_fallback_deadline_decision(
                        pending, position, self._now_ms(),
                    )
                    if deadline.get("hard_breached"):
                        self._enter_passive_close_execution_fail_closed(
                            state,
                            pending,
                            position,
                            deadline,
                            source="fallback_zero_fill_no_pending",
                            error="aggressive close returned zero fill with no pending close",
                        )
                        return False
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

        if await self._clear_if_live_flat(
            state,
            pending,
            position,
            source="passive_close_recovery_flat_probe",
            adapters=adapters,
        ):
            return "cleared_flat"

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

        long_probe = await self._probe_venue_flatness_evidence(
            long_venue, symbol, adapters
        )
        short_probe = await self._probe_venue_flatness_evidence(
            short_venue, symbol, adapters
        )
        long_flat = bool(long_probe.get("flat"))
        short_flat = bool(short_probe.get("flat"))

        if long_flat and short_flat:
            # Position truth is flat. Verify open-order truth to prevent
            # false-green when exchange has resting orders not yet filled.
            long_oo_flat, long_oo_evidence = await self._probe_venue_open_orders_flat(
                long_venue, symbol, adapters
            )
            short_oo_flat, short_oo_evidence = await self._probe_venue_open_orders_flat(
                short_venue, symbol, adapters
            )
            if not long_oo_flat or not short_oo_flat:
                self._journal.append(
                    "exit.passive_close_recovery_probe_diagnostic",
                    {
                        "position_id": pending.position_id,
                        "symbol": symbol,
                        "long_venue": long_venue.value,
                        "short_venue": short_venue.value,
                        "local_quantity": pending.target_quantity,
                        "live_long_size": long_probe.get("quantity"),
                        "live_short_size": short_probe.get("quantity"),
                        "live_long_open_orders": long_oo_evidence,
                        "live_short_open_orders": short_oo_evidence,
                        "open_order_truth_trusted": long_oo_flat is not None and short_oo_flat is not None,
                        "decision": "position_flat_but_open_orders_untrusted",
                        "next_action": "retry_live_flat_probe",
                        "source": "pending_passive_close_live_flat_probe",
                    },
                )
                return False
            self._journal.append(
                "exit.passive_close_recovery_probe_flat",
                {
                    "position_id": pending.position_id,
                    "long_venue": long_venue.value,
                    "short_venue": short_venue.value,
                    "symbol": symbol,
                    "open_order_truth_trusted": True,
                },
            )
            return True

        decision = (
            "probe_incomplete"
            if long_probe.get("error") or short_probe.get("error")
            else "not_flat"
        )
        self._journal.append(
            "exit.passive_close_recovery_probe_diagnostic",
            {
                "position_id": pending.position_id,
                "symbol": symbol,
                "long_venue": long_venue.value,
                "short_venue": short_venue.value,
                "local_quantity": pending.target_quantity,
                "matched_quantity": getattr(snapshot, "matched_quantity", 0.0),
                "maker_fill": pending.maker_fill.quantity,
                "hedge_fill": pending.hedge_fill.quantity,
                "pending_phase": pending.phase_state.phase.value,
                "live_long_size": long_probe.get("quantity"),
                "live_short_size": short_probe.get("quantity"),
                "live_long_open_orders": None,
                "live_short_open_orders": None,
                "client_order_ids": [
                    cid for cid in (
                        pending.phase_state.maker_client_order_id,
                        pending.hedge_fill.client_order_id,
                    ) if cid
                ],
                "live_long_error": long_probe.get("error"),
                "live_short_error": short_probe.get("error"),
                "source": "pending_passive_close_live_flat_probe",
                "decision": decision,
                "next_action": (
                    "retry_live_flat_probe"
                    if decision == "probe_incomplete"
                    else "continue_pending_passive_close"
                ),
            },
        )
        return False

    async def _probe_venue_flatness(
        self,
        venue: Venue,
        symbol: str,
        adapters: dict[Venue, VenueAdapter],
    ) -> bool:
        """Check if a single venue reports zero position for symbol."""
        probe = await self._probe_venue_flatness_evidence(venue, symbol, adapters)
        return bool(probe.get("flat"))

    async def _probe_venue_flatness_evidence(
        self,
        venue: Venue,
        symbol: str,
        adapters: dict[Venue, VenueAdapter],
    ) -> dict[str, Any]:
        """Check one venue and retain enough evidence to explain the decision."""
        adapter = adapters.get(venue)
        if adapter is None:
            return {"flat": False, "quantity": None, "error": "adapter_missing"}

        try:
            pos = await adapter.fetch_position(symbol)
            qty = getattr(pos, "quantity", None)
            if (
                isinstance(qty, (int, float))
                and math.isfinite(float(qty))
            ):
                live_qty = abs(float(qty))
                return {
                    "flat": live_qty < 1e-9,
                    "quantity": live_qty,
                    "error": None,
                }
            return {
                "flat": False,
                "quantity": None,
                "error": f"invalid_quantity:{qty!r}",
            }
        except Exception as exc:
            direct_error = str(exc)
        else:
            direct_error = ""

        try:
            all_positions = await adapter.fetch_all_positions()
            if isinstance(all_positions, (list, tuple)):
                saw_symbol = False
                for pos in all_positions:
                    qty = getattr(pos, "quantity", None)
                    if (
                        getattr(pos, "symbol", None) == symbol
                        and isinstance(qty, (int, float))
                        and math.isfinite(float(qty))
                    ):
                        saw_symbol = True
                        live_qty = abs(float(qty))
                        if live_qty > 1e-9:
                            return {
                                "flat": False,
                                "quantity": live_qty,
                                "error": None,
                            }
                return {"flat": True, "quantity": 0.0, "error": None}
        except Exception as exc:
            fallback_error = str(exc)
            return {
                "flat": False,
                "quantity": None,
                "error": direct_error or fallback_error,
            }

        return {
            "flat": False,
            "quantity": None,
            "error": direct_error or "position_fetch_unavailable",
        }

    async def _probe_venue_open_orders_flat(
        self,
        venue: Venue,
        symbol: str,
        adapters: dict[Venue, VenueAdapter],
    ) -> tuple[bool | None, str | None]:
        """Check if a venue has no open orders for a symbol.

        Returns:
            (True, None) — no open orders (trusted flat)
            (False, evidence) — has open orders (trusted non-flat)
            (None, error) — query failed (untrusted)
        """
        adapter = adapters.get(venue)
        if adapter is None:
            return None, "adapter_missing"

        from lightfee.engine.exchange_truth import probe_venue_open_orders_flat

        return await probe_venue_open_orders_flat(adapter, venue, symbol)

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
        mid, _source = self._resolve_local_l2_mid_with_source(venue, symbol)
        return mid

    def _resolve_local_l2_mid_with_source(
        self,
        venue: Venue,
        symbol: str,
    ) -> tuple[float, str]:
        source = "local_l2"
        if self._l2_mid_resolver is not None:
            try:
                raw = self._l2_mid_resolver(venue, symbol)
                if isinstance(raw, (tuple, list)) and len(raw) >= 2:
                    mid = raw[0]
                    source = str(raw[1] or source)
                else:
                    mid = raw
                if mid and mid > 0:
                    return float(mid), source
            except Exception:
                pass
        return 0.0, source

    def _resolve_local_l2_quote(self, venue: Venue, symbol: str) -> tuple[float, float] | None:
        """Resolve best bid/ask from injected local-L2 resolver."""
        quote, _source = self._resolve_local_l2_quote_with_source(venue, symbol)
        return quote

    def _post_only_requote_price_hint(
        self,
        venue: Venue,
        symbol: str,
        side: Side,
        attempt: int,
        *,
        fallback_price: float,
    ) -> float:
        """Reprice from current Local-L2 within V1's bounded retry contract."""
        quote = self._resolve_local_l2_quote(venue, symbol)
        if quote is None:
            return fallback_price
        best_bid, best_ask = quote
        closest = (
            max(best_bid, math.nextafter(best_ask, -math.inf))
            if side == Side.BUY
            else min(best_ask, math.nextafter(best_bid, math.inf))
        )
        fraction = PASSIVE_CLOSE_POST_ONLY_LADDER_FRACTIONS[
            min(attempt, len(PASSIVE_CLOSE_POST_ONLY_LADDER_FRACTIONS) - 1)
        ]
        price = (
            best_bid + (closest - best_bid) * fraction
            if side == Side.BUY
            else best_ask - (best_ask - closest) * fraction
        )
        return price if math.isfinite(price) and price > 0.0 else fallback_price

    def _resolve_local_l2_quote_with_source(
        self,
        venue: Venue,
        symbol: str,
    ) -> tuple[tuple[float, float] | None, str]:
        if self._l2_quote_resolver is None:
            return None, "local_l2"
        try:
            quote = self._l2_quote_resolver(venue, symbol)
        except Exception:
            return None, "local_l2"
        if quote is None:
            return None, "local_l2"
        try:
            source = "local_l2"
            if isinstance(quote, (tuple, list)) and len(quote) >= 3:
                best_bid, best_ask, raw_source = quote[0], quote[1], quote[2]
                source = str(raw_source or source)
            else:
                best_bid, best_ask = quote
            best_bid = float(best_bid)
            best_ask = float(best_ask)
        except Exception:
            return None, "local_l2"
        if (
            math.isfinite(best_bid)
            and math.isfinite(best_ask)
            and best_bid > 0.0
            and best_ask > best_bid
        ):
            return (best_bid, best_ask), source
        return None, "local_l2"

    async def _resolve_hedge_reference_price(
        self,
        venue: Venue,
        symbol: str,
        side: Side,
        price_hint: float,
    ) -> tuple[float, str]:
        try:
            candidate = float(price_hint)
        except (TypeError, ValueError):
            candidate = 0.0
        if math.isfinite(candidate) and candidate > 0.0:
            return candidate, "price_hint"

        quote, quote_source = self._resolve_local_l2_quote_with_source(venue, symbol)
        if quote is not None:
            price = quote[1] if side == Side.BUY else quote[0]
            if math.isfinite(price) and price > 0.0:
                source = (
                    f"{quote_source}_best_ask"
                    if side == Side.BUY
                    else f"{quote_source}_best_bid"
                )
                return price, source

        local_mid, mid_source = self._resolve_local_l2_mid_with_source(venue, symbol)
        if math.isfinite(local_mid) and local_mid > 0.0:
            return local_mid, f"{mid_source}_mid"

        adapter = self._adapter(venue)
        if adapter is None:
            return 0.0, "price_unavailable_for_min_notional"

        try:
            snapshot = await adapter.fetch_market_snapshot([symbol])
        except Exception:
            return 0.0, "price_unavailable_for_min_notional"

        price, source = self._reference_price_from_market_snapshot(snapshot, symbol, side)
        if math.isfinite(price) and price > 0.0:
            return price, source
        return 0.0, "price_unavailable_for_min_notional"

    @staticmethod
    def _reference_price_from_market_snapshot(
        snapshot: Any,
        symbol: str,
        side: Side,
    ) -> tuple[float, str]:
        quotes = getattr(snapshot, "quotes", None)
        if not quotes:
            return 0.0, "price_unavailable_for_min_notional"

        target_keys = PassiveCloseExecutor._canonical_symbol_keys(symbol)
        selected = None
        for quote in quotes:
            quote_symbol = getattr(quote, "symbol", "")
            if PassiveCloseExecutor._canonical_symbol_keys(quote_symbol) & target_keys:
                selected = quote
                break
        if selected is None and len(quotes) == 1:
            selected = quotes[0]
        if selected is None:
            return 0.0, "price_unavailable_for_min_notional"

        bid = PassiveCloseExecutor._positive_float(getattr(selected, "bid", 0.0))
        ask = PassiveCloseExecutor._positive_float(getattr(selected, "ask", 0.0))
        if side == Side.BUY and ask > 0.0:
            return ask, "market_snapshot_best_ask"
        if side == Side.SELL and bid > 0.0:
            return bid, "market_snapshot_best_bid"
        if bid > 0.0 and ask > 0.0:
            return (bid + ask) / 2.0, "market_snapshot_mid"

        mark = PassiveCloseExecutor._positive_float(getattr(selected, "mark_price", 0.0))
        if mark > 0.0:
            return mark, "market_snapshot_mark"
        index = PassiveCloseExecutor._positive_float(getattr(selected, "index_price", 0.0))
        if index > 0.0:
            return index, "market_snapshot_index"
        return 0.0, "price_unavailable_for_min_notional"

    @staticmethod
    def _positive_float(value: Any) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return 0.0
        return parsed if math.isfinite(parsed) and parsed > 0.0 else 0.0

    @staticmethod
    def _canonical_symbol_key(symbol: Any) -> str:
        return "".join(ch for ch in str(symbol).upper() if ch.isalnum())

    @staticmethod
    def _canonical_symbol_keys(symbol: Any) -> set[str]:
        key = PassiveCloseExecutor._canonical_symbol_key(symbol)
        keys = {key} if key else set()
        for suffix in ("SWAP", "PERP"):
            if key.endswith(suffix):
                keys.add(key[:-len(suffix)])
        return {item for item in keys if item}

    async def _resolve_hedge_min_notional_quote(
        self,
        venue: Venue,
        symbol: str,
    ) -> tuple[float, str]:
        spec_min_notional = 0.0
        try:
            spec = get_spec(venue)
            spec_min_notional = float(getattr(spec, "min_notional", 0.0) or 0.0)
        except Exception:
            spec_min_notional = 0.0

        adapter = self._adapter(venue)
        transport = getattr(adapter, "_transport", None) if adapter is not None else None
        if transport is not None and venue in (Venue.BYBIT, Venue.OKX):
            venue_symbol = symbol
            venue_symbol_fn = getattr(transport, "_venue_symbol", None)
            if callable(venue_symbol_fn):
                try:
                    venue_symbol = venue_symbol_fn(symbol)
                except Exception:
                    venue_symbol = symbol
            try:
                symbol_rule = await get_symbol_rules_cache().get(
                    transport, venue, venue_symbol,
                )
                rule_source = str(getattr(symbol_rule, "rule_source", "") or "")
                rule_min_notional = float(
                    getattr(symbol_rule, "min_notional", 0.0) or 0.0
                )
                if (
                    rule_source
                    and rule_source != "spec_fallback"
                    and (rule_min_notional > 0.0 or venue == Venue.OKX)
                ):
                    return rule_min_notional, rule_source
            except Exception:
                pass

        if venue in (Venue.BYBIT, Venue.OKX):
            return spec_min_notional, "venue_spec"

        buffer_min_notional = float(self._config.small_fill_buffer_notional_quote or 0.0)
        return max(spec_min_notional, buffer_min_notional), "venue_spec_or_buffer"

    async def _hedge_rule_diagnostic_payload(
        self,
        venue: Venue,
        symbol: str,
        *,
        min_notional_source: str,
    ) -> dict[str, Any]:
        adapter = self._adapter(venue)
        transport = getattr(adapter, "_transport", None) if adapter is not None else None
        venue_symbol = symbol
        venue_symbol_fn = getattr(transport, "_venue_symbol", None)
        if callable(venue_symbol_fn):
            try:
                venue_symbol = str(venue_symbol_fn(symbol))
            except Exception:
                venue_symbol = symbol

        payload: dict[str, Any] = {
            "venue_symbol": venue_symbol,
            "min_notional_source": min_notional_source,
        }
        if transport is None:
            return payload

        # Do not fetch here; dust diagnostics must not add public metadata I/O
        # to the close execution path.
        cache = get_symbol_rules_cache()
        rule = getattr(cache, "_rules", {}).get((venue, venue_symbol))
        if rule is None:
            return payload

        rule_source = str(getattr(rule, "rule_source", "") or "")
        if rule_source:
            payload["rule_source"] = rule_source
        payload["rule_qty_step"] = float(getattr(rule, "qty_step", 0.0) or 0.0)
        payload["rule_min_quantity"] = float(getattr(rule, "min_qty", 0.0) or 0.0)
        payload["rule_min_notional_quote"] = float(
            getattr(rule, "min_notional", 0.0) or 0.0
        )
        payload["rule_ct_val"] = float(getattr(rule, "ct_val", 0.0) or 0.0)
        payload["rule_max_market_qty"] = float(
            getattr(rule, "max_market_qty", 0.0) or 0.0
        )
        return payload

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
        *,
        price_source: str = "price_hint",
        min_notional_quote: float | None = None,
        min_notional_source: str = "venue_spec",
    ) -> Optional[dict[str, Any]]:
        """V1: check if hedge quantity is below venue min notional.

        Returns None if the hedge passes min notional check.
        Returns dict with violation details if below min notional.
        """
        if quantity <= 1e-12:
            return {
                "venue": hedge_venue,
                "leg_notional": 0.0,
                "min_notional": 0.0,
                "reason": "normalized_quantity_zero",
                "price_source": price_source,
            }
        if min_notional_quote is None:
            min_notional = float(self._config.small_fill_buffer_notional_quote or 0.0)
            min_notional_source = "passive_close_small_fill_buffer"
        else:
            min_notional = float(min_notional_quote or 0.0)
        if venue_reduce_only_close_exempts_min_notional(hedge_venue):
            return None
        if not (math.isfinite(price_hint) and price_hint > 0.0):
            return None
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
                "reason": "min_notional_rejected",
                "price_source": price_source,
                "min_notional_source": min_notional_source,
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

        Uses runtime-injected quote resolver for mid + spread. Falls back to
        venue taker fee comparison if quote evidence is unavailable.
        If quote evidence is missing for either venue, journals the gap and uses a
        deterministic fallback chain. Tie-break defaults to LONG (V1 behavior).
        """
        symbol = position.symbol
        pid = position.position_id

        long_mid, long_price_source = self._resolve_local_l2_mid_with_source(
            position.long_venue, symbol,
        )
        short_mid, short_price_source = self._resolve_local_l2_mid_with_source(
            position.short_venue, symbol,
        )

        long_cost_bps = self._estimate_venue_taker_cost_bps(
            position.long_venue, symbol, l2_mid=long_mid,
        )
        short_cost_bps = self._estimate_venue_taker_cost_bps(
            position.short_venue, symbol, l2_mid=short_mid,
        )

        long_has_price_evidence = long_mid > 0.0
        short_has_price_evidence = short_mid > 0.0
        uses_local_l2_sources = (
            long_price_source == "local_l2"
            and short_price_source == "local_l2"
        )

        if not long_has_price_evidence or not short_has_price_evidence:
            self._journal.append(
                (
                    "exit.passive_close_maker_leg_l2_missing"
                    if uses_local_l2_sources
                    else "exit.passive_close_maker_leg_quote_evidence_missing"
                ),
                {
                    "position_id": pid,
                    "long_venue": position.long_venue.value,
                    "short_venue": position.short_venue.value,
                    "long_mid_available": long_has_price_evidence,
                    "short_mid_available": short_has_price_evidence,
                    "long_price_evidence_available": long_has_price_evidence,
                    "short_price_evidence_available": short_has_price_evidence,
                    "long_price_source": long_price_source,
                    "short_price_source": short_price_source,
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
                    "long_l2_available": long_has_price_evidence,
                    "short_l2_available": short_has_price_evidence,
                    "long_price_evidence_available": long_has_price_evidence,
                    "short_price_evidence_available": short_has_price_evidence,
                    "long_price_source": long_price_source,
                    "short_price_source": short_price_source,
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
                    "long_l2_available": long_has_price_evidence,
                    "short_l2_available": short_has_price_evidence,
                    "long_price_evidence_available": long_has_price_evidence,
                    "short_price_evidence_available": short_has_price_evidence,
                    "long_price_source": long_price_source,
                    "short_price_source": short_price_source,
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
                    "long_l2_available": long_has_price_evidence,
                    "short_l2_available": short_has_price_evidence,
                    "long_price_evidence_available": long_has_price_evidence,
                    "short_price_evidence_available": short_has_price_evidence,
                    "long_price_source": long_price_source,
                    "short_price_source": short_price_source,
                    "reason": "tie_or_equal_cost_default_long",
                },
            )
            return ActiveMakerLeg.LONG

    def _estimate_venue_taker_cost_bps(
        self, venue: Venue, symbol: str, l2_mid: float = 0.0,
    ) -> float:
        """Estimate the effective taker cost in bps for a venue.

        Uses injected quote resolver for spread + venue taker fee.
        Fallback: taker fee only if spread evidence is unavailable.
        """
        cost_bps = 0.0

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
