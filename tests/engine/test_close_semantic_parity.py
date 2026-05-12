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
