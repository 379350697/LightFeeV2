"""Task 5: PnL attribution contract tests.

Rust references:
- src/engine/exit.rs: build_exit_pnl_attribution (line 5960)
- src/engine/exit.rs: build_close_execution_from_legs (line 1155)
- src/engine/exit.rs: finalize_close_position_execution (line 4896)
"""

from __future__ import annotations

import pytest

from lightfee.core.domain import Side, Venue
from lightfee.engine.close_executor import (
    build_exit_pnl_attribution,
)
from lightfee.engine.exit import CloseExecution, compute_close_pnl
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
        long_entry_fee_quote=2.5,
        short_entry_fee_quote=2.5,
        captured_funding_quote=10.0,
        funding_captured=True,
        second_stage_funding_quote=3.0,
        second_stage_funding_captured=True,
    )
    defaults.update(overrides)
    return OpenPosition(**defaults)


# ---------------------------------------------------------------------------
# PnL attribution
# ---------------------------------------------------------------------------


class TestPnLAttribution:
    def test_price_pnl_long_profit(self):
        """Long exit > entry → positive price PnL on long leg."""
        pos = _make_position(
            long_entry_price=50000.0, short_entry_price=50000.0,
            captured_funding_quote=0.0, second_stage_funding_quote=0.0,
        )
        close = CloseExecution(
            position_id="p001", reason="funding_capture",
            long_close_price=50100.0, short_close_price=50000.0,
            long_close_qty=0.01, short_close_qty=0.01,
            long_fee_quote=2.5, short_fee_quote=2.5,
            realized_price_pnl_quote=1.0,
            net_quote=1.0 - 5.0,  # PnL - fees
        )
        attr = build_exit_pnl_attribution(pos, close)
        assert attr["price_pnl_quote"] == 1.0
        assert attr["funding_quote"] == 0.0
        assert attr["entry_fee_quote"] == 5.0
        assert attr["exit_fee_quote"] == 5.0
        assert attr["net_quote"] == 1.0 - 10.0  # price - all fees

    def test_funding_pnl_included(self):
        """Funding + second stage funding included in attribution."""
        pos = _make_position(
            captured_funding_quote=10.0, second_stage_funding_quote=3.0,
        )
        close = CloseExecution(
            position_id="p001", reason="funding_capture",
            long_close_price=50000.0, short_close_price=50000.0,
            long_close_qty=0.01, short_close_qty=0.01,
            long_fee_quote=2.5, short_fee_quote=2.5,
            realized_price_pnl_quote=0.0,
            net_quote=13.0 - 5.0 - 5.0,  # funding - entry fees - exit fees
        )
        attr = build_exit_pnl_attribution(pos, close)
        assert attr["funding_quote"] == 13.0  # 10 + 3
        assert attr["price_pnl_quote"] == 0.0
        assert attr["net_quote"] == 13.0 - 10.0  # funding - all fees

    def test_entry_fees_from_position(self):
        """Entry fees come from position fields."""
        pos = _make_position(
            long_entry_fee_quote=3.0, short_entry_fee_quote=2.0,
            captured_funding_quote=0.0, second_stage_funding_quote=0.0,
        )
        close = CloseExecution(
            position_id="p001", reason="funding_capture",
            long_close_price=50100.0, short_close_price=50000.0,
            long_close_qty=0.01, short_close_qty=0.01,
            long_fee_quote=1.0, short_fee_quote=1.0,
            realized_price_pnl_quote=1.0,
            net_quote=0.0,
        )
        attr = build_exit_pnl_attribution(pos, close)
        assert attr["entry_fee_quote"] == 5.0  # 3 + 2
        assert attr["exit_fee_quote"] == 2.0  # 1 + 1
        assert attr["net_quote"] == 1.0 + 0.0 - 5.0 - 2.0

    def test_loss_position_negative_pnl(self):
        """Price loss captured correctly."""
        pos = _make_position(
            captured_funding_quote=10.0, second_stage_funding_quote=0.0,
        )
        close = CloseExecution(
            position_id="p001", reason="hard_stop",
            long_close_price=49900.0, short_close_price=50100.0,
            long_close_qty=0.01, short_close_qty=0.01,
            long_fee_quote=2.5, short_fee_quote=2.5,
            realized_price_pnl_quote=-2.0,
            net_quote=-2.0 + 10.0 - 5.0 - 5.0,  # price + funding - all fees
        )
        attr = build_exit_pnl_attribution(pos, close)
        assert attr["price_pnl_quote"] == -2.0
        assert attr["funding_quote"] == 10.0
        assert attr["net_quote"] == -2.0


# ---------------------------------------------------------------------------
# compute_close_pnl (existing function)
# ---------------------------------------------------------------------------


class TestComputeClosePnl:
    def test_matched_quantity_is_min(self):
        """V1: matched close quantity = min(long_qty, short_qty)."""
        from lightfee.core.domain import OrderFill

        pos = _make_position(
            long_entry_price=50000.0, short_entry_price=50000.0,
            captured_funding_quote=0.0, second_stage_funding_quote=0.0,
        )
        long_fill = OrderFill(
            venue=Venue.BINANCE, symbol="BTCUSDT", side=Side.SELL,
            quantity=0.01, price=50100.0, order_id="l001",
        )
        short_fill = OrderFill(
            venue=Venue.OKX, symbol="BTCUSDT", side=Side.BUY,
            quantity=0.008, price=49900.0, order_id="s001",
        )
        result = compute_close_pnl(pos, long_fill, short_fill)

        # matched_qty = min(0.01, 0.008) = 0.008
        # long PnL: (50100 - 50000) * 0.008 = 0.8
        # short PnL: (50000 - 49900) * 0.008 = 0.8
        # total: 1.6
        assert result.realized_price_pnl_quote == pytest.approx(1.6)
        assert result.long_close_qty == 0.01
        assert result.short_close_qty == 0.008

    def test_profit_scenario(self):
        from lightfee.core.domain import OrderFill

        pos = _make_position(
            long_entry_price=50000.0, short_entry_price=50000.0,
            captured_funding_quote=0.0, second_stage_funding_quote=0.0,
        )
        long_fill = OrderFill(
            venue=Venue.BINANCE, symbol="BTCUSDT", side=Side.SELL,
            quantity=0.01, price=50100.0, order_id="l001", fee_quote=2.5,
        )
        short_fill = OrderFill(
            venue=Venue.OKX, symbol="BTCUSDT", side=Side.BUY,
            quantity=0.01, price=49900.0, order_id="s001", fee_quote=2.5,
        )
        result = compute_close_pnl(pos, long_fill, short_fill)

        # matched_qty = 0.01
        # long: (50100 - 50000) * 0.01 = 1.0
        # short: (50000 - 49900) * 0.01 = 1.0
        # total: 2.0
        assert result.realized_price_pnl_quote == pytest.approx(2.0)
        assert result.net_quote == pytest.approx(2.0 - 2.5 - 2.5)

    def test_loss_scenario(self):
        from lightfee.core.domain import OrderFill

        pos = _make_position(
            long_entry_price=50000.0, short_entry_price=50000.0,
            captured_funding_quote=0.0, second_stage_funding_quote=0.0,
        )
        long_fill = OrderFill(
            venue=Venue.BINANCE, symbol="BTCUSDT", side=Side.SELL,
            quantity=0.01, price=49800.0, order_id="l001",
        )
        short_fill = OrderFill(
            venue=Venue.OKX, symbol="BTCUSDT", side=Side.BUY,
            quantity=0.01, price=50200.0, order_id="s001",
        )
        result = compute_close_pnl(pos, long_fill, short_fill)

        # long: (49800 - 50000) * 0.01 = -2.0
        # short: (50000 - 50200) * 0.01 = -2.0
        # total: -4.0
        assert result.realized_price_pnl_quote == pytest.approx(-4.0)
