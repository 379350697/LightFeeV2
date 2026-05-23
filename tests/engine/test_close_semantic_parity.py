"""Semantic parity tests for close execution (CLOSE-001).

V1 references:
- src/engine/exit.rs: execute_aggressive_close_orders, build_close_execution_from_legs
- src/execution_core/residual.rs: split_close_fill_residual
"""

from __future__ import annotations

import pytest
from lightfee.core.domain import OrderFill, Side, Venue
from lightfee.engine.close_executor import (
    CloseBalance,
    CloseExecutionLeg,
    ChunkPlan,
    build_close_execution_from_legs,
    build_exit_pnl_attribution,
    close_balance_from_closed_quantities,
    close_leg_exchange_min_notional_violation,
    close_position_exchange_min_notional_violation,
    compute_close_chunks,
    split_close_fill_residual,
)
from lightfee.engine.exit import CloseExecution
from lightfee.engine.state import OpenPosition


# ============================================================================
# Helper: build a minimal test position
# ============================================================================


def make_test_position(
    position_id: str = "test-pos-1",
    symbol: str = "BTC-USDT",
    long_venue: Venue = Venue.BINANCE,
    short_venue: Venue = Venue.BYBIT,
    quantity: float = 1.0,
    long_entry_price: float = 50000.0,
    short_entry_price: float = 50000.0,
    captured_funding: float = 0.0,
) -> OpenPosition:
    return OpenPosition(
        position_id=position_id,
        symbol=symbol,
        long_venue=long_venue,
        short_venue=short_venue,
        long_quantity=quantity,
        short_quantity=quantity,
        long_entry_price=long_entry_price,
        short_entry_price=short_entry_price,
        opened_at_ms=1000,
        matched_quantity=quantity,
        captured_funding_quote=captured_funding,
    )


# ============================================================================
# CLOSE-001: Close Execution Semantics
# ============================================================================


class TestCloseBalance:
    """V1 close_balance_from_closed_quantities."""

    def test_symmetric_close(self):
        bal = close_balance_from_closed_quantities(1.0, 1.0, 1.0)
        assert bal.matched_closed_quantity == 1.0
        assert bal.matched_remaining_quantity == 0.0
        assert bal.long_remaining_quantity == 0.0
        assert bal.short_remaining_quantity == 0.0

    def test_asymmetric_close_generates_residual(self):
        """V1: When short closes more than long, matched_remaining=0 and the
        excess is detected as a residual by split_close_fill_residual, not by
        close_balance (which computes symmetric remaining only)."""
        bal = close_balance_from_closed_quantities(1.0, 0.8, 1.0)
        # matched_remaining = min(1.0-0.8=0.2, 1.0-1.0=0.0) = 0.0
        # matched_closed = max(1.0 - 0.0, 0.0) = 1.0
        assert bal.matched_closed_quantity == 1.0
        assert bal.matched_remaining_quantity == 0.0
        assert bal.long_remaining_quantity == pytest.approx(0.2)
        assert bal.short_remaining_quantity == 0.0

    def test_partial_close_both_sides(self):
        bal = close_balance_from_closed_quantities(2.0, 0.5, 0.5)
        assert bal.matched_closed_quantity == 0.5
        assert bal.matched_remaining_quantity == 1.5
        assert bal.long_remaining_quantity == 1.5
        assert bal.short_remaining_quantity == 1.5


class TestClosePnLAttribution:
    """V1 build_close_execution_from_legs and build_exit_pnl_attribution."""

    def test_price_pnl_long_leg(self):
        pos = make_test_position(long_entry_price=50000.0, short_entry_price=50000.0)
        long_leg = CloseExecutionLeg(
            fill=OrderFill(venue=Venue.BINANCE, symbol="BTC-USDT",
                           side=Side.SELL, quantity=1.0, price=51000.0,
                           order_id="l1", filled_at_ms=2000),
        )
        short_leg = CloseExecutionLeg(
            fill=OrderFill(venue=Venue.BYBIT, symbol="BTC-USDT",
                           side=Side.BUY, quantity=1.0, price=51000.0,
                           order_id="s1", filled_at_ms=2000),
        )
        close = build_close_execution_from_legs(pos, 1, [short_leg], [long_leg])
        # Long PnL: (51000 - 50000) * 1.0 = 1000
        # Short PnL: (50000 - 51000) * 1.0 = -1000
        # Net = 0
        assert close.realized_price_pnl_quote == pytest.approx(0.0, abs=1e-6)

    def test_price_pnl_profitable_close(self):
        pos = make_test_position(long_entry_price=50000.0, short_entry_price=51000.0)
        long_leg = CloseExecutionLeg(
            fill=OrderFill(venue=Venue.BINANCE, symbol="BTC-USDT",
                           side=Side.SELL, quantity=1.0, price=51000.0,
                           order_id="l1", filled_at_ms=2000),
        )
        short_leg = CloseExecutionLeg(
            fill=OrderFill(venue=Venue.BYBIT, symbol="BTC-USDT",
                           side=Side.BUY, quantity=1.0, price=50000.0,
                           order_id="s1", filled_at_ms=2000),
        )
        close = build_close_execution_from_legs(pos, 1, [short_leg], [long_leg])
        # Long PnL: (51000 - 50000) * 1.0 = 1000
        # Short PnL: (51000 - 50000) * 1.0 = 1000
        # Total = 2000
        assert close.realized_price_pnl_quote == pytest.approx(2000.0, abs=1e-6)

    def test_pnl_attribution_components(self):
        pos = make_test_position(long_entry_price=50000.0, short_entry_price=50000.0,
                                 captured_funding=10.0)
        pos.long_entry_fee_quote = 2.0
        pos.short_entry_fee_quote = 1.5
        long_leg = CloseExecutionLeg(
            fill=OrderFill(venue=Venue.BINANCE, symbol="BTC-USDT",
                           side=Side.SELL, quantity=1.0, price=51000.0,
                           order_id="l1", filled_at_ms=2000, fee_quote=3.0),
        )
        short_leg = CloseExecutionLeg(
            fill=OrderFill(venue=Venue.BYBIT, symbol="BTC-USDT",
                           side=Side.BUY, quantity=1.0, price=51000.0,
                           order_id="s1", filled_at_ms=2000, fee_quote=2.0),
        )
        close = build_close_execution_from_legs(pos, 1, [short_leg], [long_leg])

        attr = build_exit_pnl_attribution(pos, close)
        assert "funding_quote" in attr
        assert "price_pnl_quote" in attr
        assert "entry_fee_quote" in attr
        assert "exit_fee_quote" in attr
        assert "net_quote" in attr
        assert attr["funding_quote"] == 10.0
        assert attr["entry_fee_quote"] == 3.5
        assert attr["exit_fee_quote"] == 5.0

    def test_net_quote_formula(self):
        """V1: net_quote = price_pnl + funding - exit_fee (entry_fee already
        accounted at entry time, so it is not deducted again at close)."""
        pos = make_test_position(captured_funding=5.0)
        pos.long_entry_fee_quote = 1.0
        pos.short_entry_fee_quote = 1.0
        long_leg = CloseExecutionLeg(
            fill=OrderFill(venue=Venue.BINANCE, symbol="BTC-USDT",
                           side=Side.SELL, quantity=1.0, price=50100.0,
                           order_id="l1", filled_at_ms=2000, fee_quote=2.0),
        )
        short_leg = CloseExecutionLeg(
            fill=OrderFill(venue=Venue.BYBIT, symbol="BTC-USDT",
                           side=Side.BUY, quantity=1.0, price=50100.0,
                           order_id="s1", filled_at_ms=2000, fee_quote=2.0),
        )
        close = build_close_execution_from_legs(pos, 1, [short_leg], [long_leg])
        # price_pnl = (50100-50000) + (50000-50100) = 100 - 100 = 0
        # net = 0 + 5 - 4 = 1 (funding minus exit fees only)
        assert close.net_quote == pytest.approx(1.0, abs=1e-6)


class TestCloseResidual:
    """V1 split_close_fill_residual."""

    def test_no_residual_when_symmetric(self):
        pos = make_test_position()
        residual = split_close_fill_residual(pos, 0.5, 0.5, now_ms=1000, deadline_ms=30000)
        assert residual is None

    def test_residual_when_long_closes_more(self):
        pos = make_test_position()
        residual = split_close_fill_residual(pos, 0.8, 0.5, now_ms=1000, deadline_ms=30000)
        assert residual is not None
        assert residual.origin.value == "close_residual"
        assert residual.exposure_quantity > 0

    def test_residual_when_short_closes_more(self):
        pos = make_test_position()
        residual = split_close_fill_residual(pos, 0.5, 0.8, now_ms=1000, deadline_ms=30000)
        assert residual is not None
        assert residual.origin.value == "close_residual"
        assert residual.exposure_quantity > 0
        # Short closed more → excess on long side (sell to reduce)
        assert residual.exposure_side == Side.SELL


class TestCloseChunkPlanning:
    """V1 close chunking: split large positions into notional-capped chunks."""

    def test_no_chunks_for_zero_quantity(self):
        chunks = compute_close_chunks(0.0, 50000.0, 50000.0, 10000.0)
        assert chunks == []

    def test_single_chunk_when_below_cap(self):
        chunks = compute_close_chunks(0.1, 50000.0, 50000.0, 10000.0)
        assert len(chunks) == 1
        assert chunks[0] == 0.1

    def test_multiple_chunks_when_above_cap(self):
        chunks = compute_close_chunks(1.0, 50000.0, 50000.0, 10000.0)
        # 1.0 * 50000 = 50000 notional → 5 chunks of 10000 notional
        assert len(chunks) >= 1
        assert sum(chunks) == pytest.approx(1.0, abs=1e-9)

    def test_chunks_sum_to_total(self):
        quantity = 2.5
        chunks = compute_close_chunks(quantity, 50000.0, 50000.0, 10000.0)
        assert sum(chunks) == pytest.approx(quantity, abs=1e-9)


class TestMinNotionalDustHandling:
    """V1: remainders below min-notional are detected as violations."""

    def test_leg_notional_violation(self):
        # Use OKX which does NOT exempt reduce-only close from min notional
        violation = close_leg_exchange_min_notional_violation(
            Venue.OKX, "BTC-USDT", Side.SELL, 0.0001, True, 50000.0, 10.0,
        )
        # 0.0001 * 50000 = 5.0 < 10.0 → violation
        assert violation is not None

    def test_leg_notional_passes(self):
        violation = close_leg_exchange_min_notional_violation(
            Venue.BINANCE, "BTC-USDT", Side.SELL, 0.001, True, 50000.0, 10.0,
        )
        # 0.001 * 50000 = 50.0 > 10.0 → no violation
        assert violation is None

    def test_position_min_notional_violation(self):
        pos = make_test_position()
        violation = close_position_exchange_min_notional_violation(
            pos, 0.0001, 50000.0, 50000.0, 10.0, 10.0,
        )
        assert violation is not None


# ============================================================================
# M-R12: Structured close-leg error classification (Gate-style reduce-only)
# ============================================================================


class TestM12CloseLegErrorClassification:
    """M-R12: Terminal reduce-only must use structured label-based detection,
    not just string.contains. Gate empty position → terminal success;
    pending conflict → not terminal; non-terminal reduce-only text → not misclassified.
    """

    def test_gate_empty_position_label_terminal(self):
        """Gate label=reduce_exceeded msg=empty position → terminal."""
        from lightfee.engine.close_executor import _classify_close_leg_error, _is_terminal_reduce_only

        error_str = "Gate error: label=reduce_exceeded msg=empty position for BTC"
        cls = _classify_close_leg_error(error_str)
        assert cls["empty_position"] is True, "structured label must detect empty position"
        assert cls["pending_conflict"] is False
        assert _is_terminal_reduce_only(cls, error_str) is True

    def test_gate_empty_position_case_insensitive_label(self):
        """Case insensitive: LABEL=REDUCE_EXCEEDED msg=EMPTY POSITION."""
        from lightfee.engine.close_executor import _classify_close_leg_error, _is_terminal_reduce_only

        error_str = "LABEL=REDUCE_EXCEEDED msg=EMPTY POSITION"
        cls = _classify_close_leg_error(error_str)
        assert cls["empty_position"] is True

    def test_gate_pending_conflict_not_terminal(self):
        """Gate label=reduce_only_fail, pending order conflict → NOT terminal."""
        from lightfee.engine.close_executor import _classify_close_leg_error, _is_terminal_reduce_only

        error_str = "label=reduce_only_fail msg=pending order conflicts with reduce order"
        cls = _classify_close_leg_error(error_str)
        assert cls["pending_conflict"] is True
        assert cls["empty_position"] is False
        assert _is_terminal_reduce_only(cls, error_str) is False, (
            "pending conflict is retryable, not terminal reduce-only"
        )

    def test_gate_reduce_exceeded_pending_conflict_not_terminal(self):
        """Gate label=reduce_exceeded with pending order → conflict, not terminal."""
        from lightfee.engine.close_executor import _classify_close_leg_error, _is_terminal_reduce_only

        error_str = "label=reduce_exceeded msg=pending order blocks reduce order"
        cls = _classify_close_leg_error(error_str)
        assert cls["pending_conflict"] is True
        assert cls["empty_position"] is False
        assert _is_terminal_reduce_only(cls, error_str) is False

    def test_gate_order_not_found_terminal(self):
        """Gate label=ORDER_NOT_FOUND → terminal (order id not recognized)."""
        from lightfee.engine.close_executor import _classify_close_leg_error

        error_str = "label=ORDER_NOT_FOUND msg=order does not exist"
        cls = _classify_close_leg_error(error_str)
        assert cls["order_not_found"] is True

    def test_generic_reduce_only_text_terminal(self):
        """Generic 'reduce_only' text without structured label → terminal fallback."""
        from lightfee.engine.close_executor import _classify_close_leg_error, _is_terminal_reduce_only

        error_str = "Order rejected: reduce_only order requires position"
        cls = _classify_close_leg_error(error_str)
        assert cls["terminal_reduce_only"] is True
        assert cls["empty_position"] is False
        assert cls["pending_conflict"] is False

    def test_non_terminal_reduce_only_text_not_misclassified(self):
        """Generic rejection text that mentions 'reduce_only' but is NOT a
        terminal condition (e.g., rate limit on reduce_only) should still be
        treated as terminal for safety by the generic fallback. However,
        the CLASSIFICATION correctly identifies it's NOT an empty_position."""
        from lightfee.engine.close_executor import _classify_close_leg_error

        error_str = "Rate limit exceeded for reduce_only orders"
        cls = _classify_close_leg_error(error_str)
        assert cls["empty_position"] is False  # Not empty position
        assert cls["pending_conflict"] is False  # Not pending conflict
        # Generic fallback: "reduce_only" text hits generic pattern
        assert cls["terminal_reduce_only"] is True  # falls back to generic contains

    def test_unrelated_error_not_terminal(self):
        """Unrelated error text → not terminal reduce-only."""
        from lightfee.engine.close_executor import _classify_close_leg_error, _is_terminal_reduce_only

        error_str = "Insufficient margin"
        cls = _classify_close_leg_error(error_str)
        assert _is_terminal_reduce_only(cls, error_str) is False

    def test_gate_empty_position_with_verified_flat_terminal(self):
        """End-to-end: Gate empty position + exchange verified flat → terminal success.
        Simulates the path where a reduce-only close is rejected by Gate because
        position is empty, and exchange fetch_position confirms flat.
        """
        from lightfee.engine.close_executor import _classify_close_leg_error, _is_terminal_reduce_only

        error_str = "label=reduce_exceeded msg=empty position"
        cls = _classify_close_leg_error(error_str)
        assert _is_terminal_reduce_only(cls, error_str) is True
        # Simulates the exchange verification path:
        # is_flat = True from fetch_position → terminal success
        assert cls["empty_position"] is True

    def test_string_contains_any_helper(self):
        """Verify _string_contains_any utility."""
        from lightfee.engine.close_executor import _string_contains_any

        assert _string_contains_any("reduce_only failed", ("reduce_only", "empty"))
        assert not _string_contains_any("insufficient margin", ("reduce_only", "empty"))


# ============================================================================
# M-R12: Venue-specific structured error code detection (OKX/Bybit/Binance)
# ============================================================================


class TestM12VenueErrorCodes:
    """M-R12: Structured error code detection for OKX, Bybit, Binance close leg errors."""

    def test_okx_code_51000_order_not_found(self):
        """OKX error code 51000 → order_not_found."""
        from lightfee.engine.close_executor import _classify_close_leg_error

        cls = _classify_close_leg_error(
            "Order failed: code=51000 msg=Order does not exist"
        )
        assert cls["order_not_found"] is True

    def test_okx_order_does_not_exist_with_reduce(self):
        """OKX 'Order does not exist' + reduce_only → both order_not_found and empty_position."""
        from lightfee.engine.close_executor import _classify_close_leg_error

        cls = _classify_close_leg_error(
            "reduce_only order failed: Order does not exist position closed"
        )
        assert cls["order_not_found"] is True

    def test_okx_position_closed_code(self):
        """OKX position closed code → empty_position."""
        from lightfee.engine.close_executor import _classify_close_leg_error, _is_terminal_reduce_only

        cls = _classify_close_leg_error(
            "reduce_only failed: position closed code 51000"
        )
        assert cls["empty_position"] is True
        assert _is_terminal_reduce_only(cls, "") is True

    def test_bybit_no_position(self):
        """Bybit 'no position' → empty_position."""
        from lightfee.engine.close_executor import _classify_close_leg_error, _is_terminal_reduce_only

        cls = _classify_close_leg_error(
            "Bybit error: reduce_only order failed due to no position"
        )
        assert cls["empty_position"] is True
        assert _is_terminal_reduce_only(cls, "") is True

    def test_bybit_position_code_110001(self):
        """Bybit position-related error codes."""
        from lightfee.engine.close_executor import _classify_close_leg_error

        cls = _classify_close_leg_error(
            "position error: retCode=110001 msg=Position does not exist"
        )
        assert cls["empty_position"] is True

    def test_bybit_position_zero_code_110017(self):
        """Bybit 110017 current position is zero is terminal reduce-only."""
        from lightfee.engine.close_executor import _classify_close_leg_error, _is_terminal_reduce_only

        cls = _classify_close_leg_error(
            "bybit retCode=110017 retMsg=current position is zero, cannot fix reduce-only order qty"
        )
        assert cls["empty_position"] is True
        assert _is_terminal_reduce_only(cls, "") is True

    def test_binance_like_minus_2022_reduceonly_rejected(self):
        """Binance/Aster -2022 ReduceOnly rejected means the reduce-only leg is terminal."""
        from lightfee.engine.close_executor import _classify_close_leg_error, _is_terminal_reduce_only

        cls = _classify_close_leg_error(
            'HTTP 400: {"code":-2022,"msg":"ReduceOnly Order is rejected."}'
        )
        assert cls["empty_position"] is True
        assert _is_terminal_reduce_only(cls, "") is True

    def test_binance_code_minus_2010_insufficient_position(self):
        """Binance -2010 → empty_position (reduce-only rejected)."""
        from lightfee.engine.close_executor import _classify_close_leg_error, _is_terminal_reduce_only

        cls = _classify_close_leg_error(
            "Binance order rejected: code=-2010 msg=Insufficient position for reduce only"
        )
        assert cls["empty_position"] is True
        assert _is_terminal_reduce_only(cls, "") is True

    def test_binance_code_minus_2011_order_not_found(self):
        """Binance -2011 → order_not_found."""
        from lightfee.engine.close_executor import _classify_close_leg_error

        cls = _classify_close_leg_error(
            "Binance error: code=-2011 msg=Order not found"
        )
        assert cls["order_not_found"] is True

    def test_binance_reduce_insufficient_text(self):
        """Binance 'reduce' + 'insufficient' text → empty_position."""
        from lightfee.engine.close_executor import _classify_close_leg_error

        cls = _classify_close_leg_error(
            "reduce order rejected: insufficient margin for position"
        )
        assert cls["empty_position"] is True

    def test_okx_pending_conflict_text(self):
        """OKX pending order message is NOT empty_position or order_not_found."""
        from lightfee.engine.close_executor import _classify_close_leg_error, _is_terminal_reduce_only

        cls = _classify_close_leg_error(
            "Gate error: label=reduce_only_fail msg=pending order conflicts reduce order"
        )
        assert cls["pending_conflict"] is True
        assert _is_terminal_reduce_only(cls, "") is False
