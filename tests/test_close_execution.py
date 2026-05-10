"""Task 5: Reduce-only close executor contract tests.

Rust references:
- src/engine/exit.rs: execute_aggressive_close_orders (line 3335)
- src/engine/exit.rs: close_leg_exchange_min_notional_violation (line 3035)
- src/engine/exit.rs: close_position_exchange_min_notional_violation (line 3067)
- src/engine/exit.rs: build_close_execution_from_legs (line 1155)
- src/execution_core/helpers.rs: close_balance_from_closed_quantities (line 181)
- src/execution_core/residual.rs: split_close_fill_residual (line 75)
- src/market_gateway/ports.rs: venue_reduce_only_close_exempts_min_notional (line 1068)
"""

from __future__ import annotations

import pytest

from lightfee.core.domain import OrderFill, Side, Venue
from lightfee.engine.close_executor import (
    CloseBalance,
    CloseExecutionLeg,
    close_balance_from_closed_quantities,
    close_leg_exchange_min_notional_violation,
    close_position_exchange_min_notional_violation,
    build_close_execution_from_legs,
    split_close_fill_residual,
)
from lightfee.engine.exit import CloseExecution
from lightfee.engine.residual import ResidualOrigin
from lightfee.engine.state import OpenPosition


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_position(**overrides) -> OpenPosition:
    defaults = dict(
        position_id="p001",
        symbol="BTCUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.OKX,
        long_quantity=0.01,
        short_quantity=0.01,
        long_entry_price=50000.0,
        short_entry_price=50000.0,
        opened_at_ms=1000000,
        matched_quantity=0.01,
    )
    defaults.update(overrides)
    return OpenPosition(**defaults)


def _fake_fill(
    venue, symbol, side, quantity, price=50000.0,
    order_id="f001", fee_quote=2.5,
):
    return OrderFill(
        venue=venue, symbol=symbol, side=side,
        quantity=quantity, price=price,
        order_id=order_id, fee_quote=fee_quote,
        filled_at_ms=1000,
    )


# ---------------------------------------------------------------------------
# CloseBalance
# ---------------------------------------------------------------------------


class TestCloseBalance:
    def test_full_close_matched(self):
        """Both legs fully closed → matched_closed = qty, matched_remaining = 0."""
        bal = close_balance_from_closed_quantities(0.01, 0.01, 0.01)
        assert bal.matched_closed_quantity == 0.01
        assert bal.matched_remaining_quantity == 0.0
        assert bal.long_remaining_quantity == 0.0
        assert bal.short_remaining_quantity == 0.0

    def test_partial_close_symmetric(self):
        """Both legs closed halfway → matched remaining = 0.005."""
        bal = close_balance_from_closed_quantities(0.01, 0.005, 0.005)
        assert bal.matched_closed_quantity == 0.005
        assert bal.matched_remaining_quantity == 0.005

    def test_partial_close_asymmetric_long_less(self):
        """Long closed less than short → matched remaining follows long."""
        bal = close_balance_from_closed_quantities(0.01, 0.003, 0.008)
        assert bal.long_remaining_quantity == 0.007
        assert bal.short_remaining_quantity == 0.002
        # matched_remaining = min(0.007, 0.002) = 0.002
        assert bal.matched_remaining_quantity == 0.002
        assert bal.matched_closed_quantity == pytest.approx(0.008)

    def test_partial_close_asymmetric_short_less(self):
        """Short closed less than long → matched remaining follows short."""
        bal = close_balance_from_closed_quantities(0.01, 0.008, 0.003)
        assert bal.matched_remaining_quantity == 0.002
        assert bal.matched_closed_quantity == pytest.approx(0.008)

    def test_zero_close(self):
        """Nothing closed → matched_remaining = full qty."""
        bal = close_balance_from_closed_quantities(0.01, 0.0, 0.0)
        assert bal.matched_closed_quantity == 0.0
        assert bal.matched_remaining_quantity == 0.01


# ---------------------------------------------------------------------------
# Min notional violations
# ---------------------------------------------------------------------------


class TestCloseMinNotionalViolation:
    def test_no_violation_when_notional_above_min(self):
        result = close_leg_exchange_min_notional_violation(
            Venue.OKX, "BTCUSDT", Side.BUY, 0.01, reduce_only=True,
            price_hint=50000.0, min_notional_quote=10.0,
        )
        assert result is None

    def test_violation_when_notional_below_min(self):
        result = close_leg_exchange_min_notional_violation(
            Venue.OKX, "BTCUSDT", Side.BUY, 0.0001, reduce_only=True,
            price_hint=50000.0, min_notional_quote=10.0,
        )
        assert result is not None
        venue, leg_notional, min_n = result
        assert venue == Venue.OKX
        assert leg_notional < min_n

    def test_zero_quantity_no_violation(self):
        """V1: quantity <= 0 returns None."""
        result = close_leg_exchange_min_notional_violation(
            Venue.OKX, "BTCUSDT", Side.BUY, 0.0, reduce_only=True,
            price_hint=50000.0, min_notional_quote=10.0,
        )
        assert result is None

    def test_binance_reduce_only_exempt(self):
        """V1: Binance and Aster are exempt from reduce-only min notional."""
        result = close_leg_exchange_min_notional_violation(
            Venue.BINANCE, "BTCUSDT", Side.BUY, 0.0001, reduce_only=True,
            price_hint=50000.0, min_notional_quote=10.0,
        )
        assert result is None

    def test_aster_reduce_only_exempt(self):
        result = close_leg_exchange_min_notional_violation(
            Venue.ASTER, "BTCUSDT", Side.BUY, 0.0001, reduce_only=True,
            price_hint=50000.0, min_notional_quote=10.0,
        )
        assert result is None

    def test_bybit_not_exempt(self):
        """V1: Bybit is NOT exempt from reduce-only min notional."""
        result = close_leg_exchange_min_notional_violation(
            Venue.BYBIT, "BTCUSDT", Side.BUY, 0.0001, reduce_only=True,
            price_hint=50000.0, min_notional_quote=10.0,
        )
        assert result is not None

    def test_non_reduce_only_not_exempt(self):
        """Only reduce_only orders get exemption."""
        result = close_leg_exchange_min_notional_violation(
            Venue.BINANCE, "BTCUSDT", Side.BUY, 0.0001, reduce_only=False,
            price_hint=50000.0, min_notional_quote=10.0,
        )
        assert result is not None  # non-reduce-only, so Binance exemption doesn't apply


class TestClosePositionMinNotionalViolation:
    def test_both_legs_pass(self):
        pos = _make_position()
        result = close_position_exchange_min_notional_violation(
            pos, 0.01, 50000.0, 50000.0, 10.0, 10.0,
        )
        assert result is None

    def test_short_leg_violation(self):
        pos = _make_position(short_venue=Venue.BYBIT)
        result = close_position_exchange_min_notional_violation(
            pos, 0.0001, 50000.0, 50000.0, 10.0, 10.0,
        )
        assert result is not None
        assert result[0] == Venue.BYBIT  # short venue checked first

    def test_long_leg_violation(self):
        pos = _make_position(long_venue=Venue.BYBIT, short_venue=Venue.BINANCE)
        # Short passes (Binance exempt), long fails (Bybit not exempt)
        result = close_position_exchange_min_notional_violation(
            pos, 0.0001, 50000.0, 50000.0, 10.0, 10.0,
        )
        assert result is not None
        assert result[0] == Venue.BYBIT  # long venue


# ---------------------------------------------------------------------------
# build_close_execution_from_legs
# ---------------------------------------------------------------------------


class TestBuildCloseExecutionFromLegs:
    def test_single_chunk_both_filled(self):
        pos = _make_position(long_entry_price=50000.0, short_entry_price=50000.0)
        short_leg = CloseExecutionLeg(fill=_fake_fill(
            Venue.OKX, "BTCUSDT", Side.BUY, 0.01, 49900.0, "s001", fee_quote=2.5,
        ))
        long_leg = CloseExecutionLeg(fill=_fake_fill(
            Venue.BINANCE, "BTCUSDT", Side.SELL, 0.01, 50100.0, "l001", fee_quote=2.5,
        ))
        close = build_close_execution_from_legs(pos, 1, [short_leg], [long_leg])

        # Price PnL:
        #   long: (50100 - 50000) * 0.01 = 1.0
        #   short: (50000 - 49900) * 0.01 = 1.0
        #   total: 2.0
        assert close.realized_price_pnl_quote == pytest.approx(2.0)
        assert close.long_close_qty == 0.01
        assert close.short_close_qty == 0.01
        assert close.long_fee_quote == 2.5
        assert close.short_fee_quote == 2.5
        # net = 2.0 + funding(0) - fees(5.0) = -3.0
        assert close.net_quote == pytest.approx(-3.0)

    def test_partial_fill(self):
        """Only partial quantities on both legs."""
        pos = _make_position(long_entry_price=50000.0, short_entry_price=50000.0,
                             captured_funding_quote=10.0, funding_captured=True)
        short_leg = CloseExecutionLeg(fill=_fake_fill(
            Venue.OKX, "BTCUSDT", Side.BUY, 0.005, 49900.0, "s001",
        ))
        long_leg = CloseExecutionLeg(fill=_fake_fill(
            Venue.BINANCE, "BTCUSDT", Side.SELL, 0.005, 50100.0, "l001",
        ))
        close = build_close_execution_from_legs(pos, 1, [short_leg], [long_leg])

        # PnL per leg = 0.5 each = 1.0 total
        assert close.realized_price_pnl_quote == pytest.approx(1.0)
        # funding captured = 10.0
        assert close.funding_pnl_quote == 10.0
        # net = 1.0 + 10.0 - fees(5.0) = 6.0
        assert close.net_quote == pytest.approx(6.0)

    def test_loss_position(self):
        """Price moved against position."""
        pos = _make_position(long_entry_price=50000.0, short_entry_price=50000.0)
        short_leg = CloseExecutionLeg(fill=_fake_fill(
            Venue.OKX, "BTCUSDT", Side.BUY, 0.01, 50100.0, "s001",
        ))
        long_leg = CloseExecutionLeg(fill=_fake_fill(
            Venue.BINANCE, "BTCUSDT", Side.SELL, 0.01, 49900.0, "l001",
        ))
        close = build_close_execution_from_legs(pos, 1, [short_leg], [long_leg])

        # long: (49900 - 50000) * 0.01 = -1.0
        # short: (50000 - 50100) * 0.01 = -1.0
        assert close.realized_price_pnl_quote == pytest.approx(-2.0)


# ---------------------------------------------------------------------------
# split_close_fill_residual
# ---------------------------------------------------------------------------


class TestSplitCloseFillResidual:
    def test_symmetric_close_no_residual(self):
        pos = _make_position()
        residual = split_close_fill_residual(pos, 0.01, 0.01, 1000, 31000)
        assert residual is None

    def test_asymmetric_long_more_remaining(self):
        """Long closed less → residual on long side (SELL to close remaining)."""
        pos = _make_position()
        residual = split_close_fill_residual(pos, 0.003, 0.01, 1000, 31000)
        assert residual is not None
        assert residual.exposure_venue == pos.long_venue
        assert residual.exposure_side == Side.SELL
        assert residual.exposure_quantity == pytest.approx(0.007)
        assert residual.origin == ResidualOrigin.CLOSE_RESIDUAL

    def test_asymmetric_short_more_remaining(self):
        """Short closed less → residual on short side (BUY to close remaining)."""
        pos = _make_position()
        residual = split_close_fill_residual(pos, 0.01, 0.003, 1000, 31000)
        assert residual is not None
        assert residual.exposure_venue == pos.short_venue
        assert residual.exposure_side == Side.BUY
        assert residual.exposure_quantity == pytest.approx(0.007)

    def test_partial_symmetric_no_residual(self):
        """Both legs partially closed by same amount → no residual."""
        pos = _make_position()
        residual = split_close_fill_residual(pos, 0.005, 0.005, 1000, 31000)
        assert residual is None
