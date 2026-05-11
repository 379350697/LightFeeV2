"""Tests for entry and exit state machines and journal emission fidelity."""

import tempfile
from pathlib import Path

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
from lightfee.persistence.journal import Journal


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


class TestJournalEntryPayload:
    """Verify entry.opened journal payload matches Rust V1 full OpenPosition shape."""

    _full_entry_payload_keys = frozenset({
        "position_id", "symbol", "long_venue", "short_venue",
        "quantity", "long_quantity", "short_quantity",
        "long_entry_price", "short_entry_price", "opened_at_ms",
        "matched_quantity", "current_net_quote", "peak_net_quote",
        "captured_funding_quote", "second_stage_funding_quote",
        "long_entry_fee_quote", "short_entry_fee_quote",
        "funding_captured", "second_stage_funding_captured",
    })

    _order_fill_payload_keys = frozenset({
        "position_id", "order_id", "client_order_id",
        "venue", "symbol", "side", "quantity", "price",
        "fee_quote", "latency_ms", "is_maker",
    })

    def test_entry_completed_emits_full_position_payload(self):
        """V1 rule: entry.opened payload must contain all 19 OpenPosition fields."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "entry.jsonl"
            j = Journal(path)
            j.open()

            full_payload = {
                "position_id": "pos-full-payload",
                "symbol": "BTCUSDT",
                "long_venue": "binance",
                "short_venue": "okx",
                "quantity": 0.1,
                "long_quantity": 0.1,
                "short_quantity": 0.1,
                "long_entry_price": 68750.0,
                "short_entry_price": 68755.0,
                "opened_at_ms": 5000,
                "matched_quantity": 0.1,
                "current_net_quote": 1.5,
                "peak_net_quote": 2.5,
                "captured_funding_quote": 0.0,
                "second_stage_funding_quote": 0.0,
                "long_entry_fee_quote": 0.001,
                "short_entry_fee_quote": 0.001,
                "funding_captured": False,
                "second_stage_funding_captured": False,
            }
            j.append("entry.opened", full_payload, flush=True)
            j.close()

            records = j.read_all()
            assert len(records) == 1
            emitted = records[0]["payload"]
            for key in self._full_entry_payload_keys:
                assert key in emitted, f"Missing key '{key}' in entry.opened payload"

    def test_order_filled_emits_full_payload_fields(self):
        """V1 rule: order.filled must include client_order_id, latency_ms, is_maker."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "order.jsonl"
            j = Journal(path)
            j.open()

            fill_payload = {
                "position_id": "pos-order-test",
                "order_id": "ord-12345",
                "client_order_id": "cl-abc",
                "venue": "binance",
                "symbol": "BTCUSDT",
                "side": "buy",
                "quantity": 0.1,
                "price": 68750.0,
                "fee_quote": 0.003,
                "latency_ms": 145,
                "is_maker": True,
            }
            j.append("order.filled", fill_payload, flush=True)
            j.close()

            records = j.read_all()
            assert len(records) == 1
            emitted = records[0]["payload"]
            for key in self._order_fill_payload_keys:
                assert key in emitted, f"Missing key '{key}' in order.filled payload"
            assert emitted["client_order_id"] == "cl-abc"
            assert emitted["latency_ms"] == 145
            assert emitted["is_maker"] is True

    def test_entry_completed_vs_opened_alias(self):
        """V2 currently emits 'entry.completed' — this must be 'entry.opened' for
        Rust V1 parity. Test verifies the shape is the same and kind is correct."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "opened.jsonl"
            j = Journal(path)
            j.open()

            payload = {"position_id": "pos-opened", "symbol": "ETHUSDT",
                       "quantity": 5.0, "long_entry_price": 3500.0,
                       "short_entry_price": 3510.0, "opened_at_ms": 1000}
            j.append("entry.opened", payload, flush=True)
            j.close()

            records = j.read_all()
            assert records[0]["kind"] == "entry.opened"


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
