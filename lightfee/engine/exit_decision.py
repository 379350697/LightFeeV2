"""V1 exit decision engine: pure helpers matching Rust exit.rs standard_close_reason.

Rust references:
- src/engine/exit.rs: standard_close_reason (line 5197)
- src/engine/exit.rs: stale_market_safe_close_reason (line 5290)
- src/engine/exit.rs: position_delta_monitor (line 5245)
- src/engine/exit.rs: remaining_close_delay_active (line 5330)
- src/engine/exit.rs: aligned_settlement_delay_elapsed (line 5348)
- src/engine/risk.rs: update_position_funding_capture_state (line 2070)
"""

from __future__ import annotations

from typing import Optional

from lightfee.config.schema import StrategyConfig
from lightfee.engine.exit import ExitReason
from lightfee.engine.state import OpenPosition

# Rust V1 const (line 38): POSITION_DELTA_WARNING_STOP_LOSS_FRACTION
_POSITION_DELTA_WARNING_FRACTION: float = 0.5


# ---------------------------------------------------------------------------
# Core close reason decision
# ---------------------------------------------------------------------------


def standard_close_reason(
    position: OpenPosition,
    config: StrategyConfig,
    now_ms: int,
) -> Optional[ExitReason]:
    """V1 standard_close_reason (line 5197): decide exit reason from position state.

    Priority (first match wins):
    1. hard_stop — net loss threshold breached
    2. return None if remaining_close_delay_active
    3. funding_capture — aligned settlement delay elapsed
    4. trailing_exit — peak reached profit_take AND drawn down
    5. first_stage_capture — Staggered, first funding captured
    6. second_stage_capture — Staggered, second funding captured
    7. funding_capture — funding captured, net >= 0 and not waiting for second stage
    8. None — hold position
    """
    delay_ms = config.settlement_remainder_close_delay_secs * 1000
    net_stop = config.net_stop_loss_quote
    profit_take = config.profit_take_quote
    trailing_drawdown = config.trailing_drawdown_quote

    # 1. Hard stop
    if position.current_net_quote <= -net_stop:
        return ExitReason.NET_STOP_LOSS

    # 2. Delay window active → no close yet
    if remaining_close_delay_active(position, now_ms, delay_ms):
        return None

    # 3. Aligned settlement delay elapsed → funding capture
    if aligned_settlement_delay_elapsed(position, now_ms, delay_ms):
        return ExitReason.FUNDING_CAPTURE

    # 4. Trailing exit: peak >= profit_take AND drawdown from peak >= trailing
    if (
        position.peak_net_quote >= profit_take
        and position.peak_net_quote - position.current_net_quote >= trailing_drawdown
    ):
        return ExitReason.TRAILING_EXIT

    # 5. First-stage capture (Staggered)
    if (
        position.opportunity_type == "staggered"
        and position.funding_captured
        and position.exit_after_first_stage
    ):
        return ExitReason.FIRST_STAGE_CAPTURE

    # 6. Second-stage capture (Staggered)
    if (
        position.opportunity_type == "staggered"
        and position.second_stage_enabled_at_entry
        and position.second_stage_funding_captured
    ):
        return ExitReason.SECOND_STAGE_CAPTURE

    # 7. Funding capture (general, net-positive)
    if position.funding_captured and position.current_net_quote >= 0.0:
        # Don't fire if Staggered with second stage enabled but not yet captured
        if not (
            position.opportunity_type == "staggered"
            and position.second_stage_enabled_at_entry
            and not position.second_stage_funding_captured
        ):
            return ExitReason.FUNDING_CAPTURE

    # 8. No close
    return None


# ---------------------------------------------------------------------------
# Safe close when market data is stale
# ---------------------------------------------------------------------------


def stale_market_safe_close_reason(
    position: OpenPosition,
    config: StrategyConfig,
    now_ms: int,
) -> Optional[ExitReason]:
    """V1 stale_market_safe_close_reason (line 5290): safe close w/o price signals.

    Same as standard_close_reason but WITHOUT hard_stop and trailing_exit.
    Only fires for funding-based reasons that don't need market prices.
    """
    delay_ms = config.settlement_remainder_close_delay_secs * 1000

    if remaining_close_delay_active(position, now_ms, delay_ms):
        return None

    if aligned_settlement_delay_elapsed(position, now_ms, delay_ms):
        return ExitReason.FUNDING_CAPTURE

    if (
        position.opportunity_type == "staggered"
        and position.funding_captured
        and position.exit_after_first_stage
    ):
        return ExitReason.FIRST_STAGE_CAPTURE

    if (
        position.opportunity_type == "staggered"
        and position.second_stage_enabled_at_entry
        and position.second_stage_funding_captured
    ):
        return ExitReason.SECOND_STAGE_CAPTURE

    if position.funding_captured and position.current_net_quote >= 0.0:
        if not (
            position.opportunity_type == "staggered"
            and position.second_stage_enabled_at_entry
            and not position.second_stage_funding_captured
        ):
            return ExitReason.FUNDING_CAPTURE

    return None


# ---------------------------------------------------------------------------
# Mark price divergence monitor
# ---------------------------------------------------------------------------


def position_delta_monitor(
    position: OpenPosition,
    long_mark_price: float,
    short_mark_price: float,
    config: StrategyConfig,
) -> Optional[dict]:
    """V1 position_delta_monitor (line 5245): check venue mark price divergence.

    Computes abs(short_mark - long_mark) * matched_quantity. Warnings fire at
    50% of hard_stop threshold (Rust const POSITION_DELTA_WARNING_STOP_LOSS_FRACTION).
    """
    hard_stop_q = config.mark_price_delta_hard_stop_quote
    delta_quote = abs(short_mark_price - long_mark_price) * position.matched_quantity

    hard_stop = delta_quote >= hard_stop_q
    warning = delta_quote >= hard_stop_q * _POSITION_DELTA_WARNING_FRACTION

    if not warning and not hard_stop:
        return None

    return {
        "mark_price_delta_quote": delta_quote,
        "mark_price_delta_bps": 0.0,  # computed with proper ref price in full impl
        "warning_stop_loss_fraction": _POSITION_DELTA_WARNING_FRACTION,
        "warning_triggered": warning,
        "hard_stop_triggered": hard_stop,
    }


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------


def remaining_close_delay_active(
    position: OpenPosition,
    now_ms: int,
    delay_ms: int,
) -> bool:
    """V1 remaining_close_delay_active (line 5330): delay after funding still active?

    True for:
    - Aligned positions with remaining qty within delay window
    - Post-partial-close within delay window
    """
    # Aligned position: delay from funding_timestamp_ms
    if position.opportunity_type == "aligned" and position.matched_quantity > 0:
        if now_ms < position.funding_timestamp_ms + delay_ms:
            return True

    # Post-settlement-half-close delay
    if position.settlement_half_closed_at_ms > 0:
        if now_ms < position.settlement_half_closed_at_ms + delay_ms:
            return True

    return False


def aligned_settlement_delay_elapsed(
    position: OpenPosition,
    now_ms: int,
    delay_ms: int,
) -> bool:
    """V1 aligned_settlement_delay_elapsed (line 5348): settlement delay past funding?

    True when: Aligned + has quantity + evaluation_now >= funding_time + delay.
    """
    if position.opportunity_type != "aligned":
        return False
    if position.matched_quantity <= 0:
        return False
    return now_ms >= position.funding_timestamp_ms + delay_ms


def force_close_due(
    position: OpenPosition,
    config: StrategyConfig,
    now_ms: int,
) -> bool:
    """Check if Aligned position is past force-close deadline.

    Rust: settlement_force_close_delay_secs past funding → force close required.
    """
    if position.opportunity_type != "aligned":
        return False
    if position.matched_quantity <= 0:
        return False
    force_ms = config.settlement_force_close_delay_secs * 1000
    return now_ms >= position.funding_timestamp_ms + force_ms


# ---------------------------------------------------------------------------
# Funding capture state update
# ---------------------------------------------------------------------------


def update_position_funding_capture_state(
    position: OpenPosition,
    now_ms: int,
    post_funding_hold_ms: int,
) -> None:
    """V1 update_position_funding_capture_state (risk.rs line 2070): mark funding stages captured.

    Updates funding_captured, captured_funding_quote, second_stage_funding_captured,
    second_stage_funding_quote, and peak_net_quote on capture events.
    """
    # Stage 1: primary funding
    hold_deadline = position.funding_timestamp_ms + post_funding_hold_ms
    if not position.funding_captured and now_ms >= hold_deadline:
        position.funding_captured = True
        # captured_funding_quote is set by entry; keep existing if already computed
        if position.captured_funding_quote > 0:
            position.peak_net_quote = max(position.peak_net_quote, position.current_net_quote)

    # Stage 2: second funding leg (Staggered only)
    if (
        position.funding_captured
        and position.second_stage_enabled_at_entry
        and not position.second_stage_funding_captured
        and position.second_funding_timestamp_ms > position.funding_timestamp_ms
    ):
        second_hold = position.second_funding_timestamp_ms + post_funding_hold_ms
        if now_ms >= second_hold:
            position.second_stage_funding_captured = True
            position.peak_net_quote = max(position.peak_net_quote, position.current_net_quote)


# ---------------------------------------------------------------------------
# Reason classification helpers
# ---------------------------------------------------------------------------


def is_delever_close_reason(reason: str) -> bool:
    """V1 is_delever_close_reason (line 5771): reason starts with 'risk_delever'."""
    return reason.startswith("risk_delever")


def is_protection_close_reason(reason: str) -> bool:
    """V1 is_protection_close_reason (line 5775): single-side or risk protection."""
    return (
        "single_side_protection" in reason
        or reason.startswith("risk_protection")
        or reason.startswith("protection_")
    )


def normal_close_reason_uses_passive_maker_taker(reason: Optional[str]) -> bool:
    """V1 normal_close_reason_uses_passive_maker_taker (line 359): passive close path?"""
    if reason is None:
        return False
    return reason in (
        "trailing_exit",
        "first_stage_capture",
        "second_stage_capture",
        "funding_capture",
        "settlement_half_close",
        "settlement_force_close",
    )
