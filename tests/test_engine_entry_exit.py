"""Tests for entry and exit state machines."""

import pytest

from lightfee.core.domain import OrderFill, Side, Venue
from lightfee.engine.entry import (
    EntryContext,
    EntryState,
    EntryType,
    build_entry_orders,
    build_open_position,
)
from lightfee.engine.exit import (
    CloseExecution,
    CloseState,
    ExitReason,
    build_reduce_only_close_orders,
    compute_close_pnl,
)
from lightfee.engine.state import OpenPosition


class TestEntry:
    def test_build_entry_orders_long_maker(self):
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.1,
            short_quantity=0.1,
            long_price_hint=50000,
            short_price_hint=50100,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        maker, hedge = build_entry_orders(ctx)
        assert maker.venue == Venue.BINANCE  # long is maker
        assert maker.side == Side.BUY
        assert maker.post_only
        assert hedge.venue == Venue.OKX
        assert hedge.side == Side.SELL

    def test_build_entry_orders_short_maker(self):
        ctx = EntryContext(
            entry_id="e2",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.1,
            short_quantity=0.1,
            long_price_hint=50000,
            short_price_hint=50100,
            maker_leg=Side.SELL,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        maker, hedge = build_entry_orders(ctx)
        assert maker.venue == Venue.OKX  # short is maker
        assert maker.side == Side.SELL
        assert hedge.venue == Venue.BINANCE

    def test_build_open_position(self):
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.1,
            short_quantity=0.1,
            long_price_hint=50000,
            short_price_hint=50100,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        maker_fill = OrderFill(
            venue=Venue.BINANCE, symbol="BTCUSDT", side=Side.BUY,
            quantity=0.1, price=50000, order_id="oid1",
        )
        hedge_fill = OrderFill(
            venue=Venue.OKX, symbol="BTCUSDT", side=Side.SELL,
            quantity=0.1, price=50100, order_id="oid2",
        )
        pos = build_open_position(ctx, maker_fill, hedge_fill, 1000)
        assert pos.position_id == "e1"
        assert pos.long_entry_price == 50000  # maker was long
        assert pos.short_entry_price == 50100  # hedge was short


class TestExit:
    def test_build_reduce_only_close_orders(self):
        pos = OpenPosition(
            position_id="p1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.1,
            short_quantity=0.1,
            long_entry_price=50000,
            short_entry_price=50100,
            opened_at_ms=1000,
        )
        long_close, short_close = build_reduce_only_close_orders(pos, ExitReason.PROFIT_TAKE)
        assert long_close.reduce_only
        assert long_close.side == Side.SELL  # close long = sell
        assert short_close.reduce_only
        assert short_close.side == Side.BUY  # close short = buy

    def test_compute_close_pnl(self):
        pos = OpenPosition(
            position_id="p1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.1,
            short_quantity=0.1,
            long_entry_price=50000,
            short_entry_price=50100,
            opened_at_ms=1000,
        )
        long_fill = OrderFill(Venue.BINANCE, "BTCUSDT", Side.SELL, 0.1, 50500, fee_quote=2.5)
        short_fill = OrderFill(Venue.OKX, "BTCUSDT", Side.BUY, 0.1, 50000, fee_quote=2.5)
        close = compute_close_pnl(pos, long_fill, short_fill)
        # realized = (50500-50000)*0.1 + (50100-50000)*0.1 = 50 + 10 = 60
        assert abs(close.realized_price_pnl_quote - 60.0) < 0.01
        assert close.net_quote == 60.0 - 5.0  # 60 - 2.5 - 2.5
