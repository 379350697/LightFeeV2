"""Task 2: Entry state machine contract tests matching Rust V1 entry_sync.rs transitions.

Rust references:
- src/execution_core/entry_sync.rs: PendingEntryHedge, entry state transitions
- src/engine/entry.rs: EntryAttemptOutcome, EntryLegPlan
"""

from __future__ import annotations

import pytest

from lightfee.core.domain import OrderFill, OrderRequest, Side, Venue
from lightfee.engine.entry import (
    EntryContext,
    EntryState,
    EntryType,
    advance_entry_state,
    build_entry_orders,
    build_open_position,
)
from lightfee.engine.execution_planner import ExecutionRoute
from lightfee.engine.state import OpenPosition, PendingEntry
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


# ---------------------------------------------------------------------------
# Entry state transitions
# ---------------------------------------------------------------------------


class TestEntryStateTransitions:
    def test_idle_to_submitting_maker(self):
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        assert ctx.state == EntryState.IDLE
        new_ctx = advance_entry_state(ctx, EntryState.SUBMITTING_MAKER)
        assert new_ctx.state == EntryState.SUBMITTING_MAKER

    def test_submitting_maker_to_maker_resting(self):
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
            state=EntryState.SUBMITTING_MAKER,
        )
        new_ctx = advance_entry_state(ctx, EntryState.MAKER_RESTING)
        assert new_ctx.state == EntryState.MAKER_RESTING

    def test_maker_resting_to_submitting_hedge(self):
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
            state=EntryState.MAKER_RESTING,
            maker_fill=OrderFill(
                venue=Venue.BINANCE, symbol="BTCUSDT",
                side=Side.BUY, quantity=0.01, price=50000.0,
                order_id="maker1",
            ),
        )
        new_ctx = advance_entry_state(ctx, EntryState.SUBMITTING_HEDGE)
        assert new_ctx.state == EntryState.SUBMITTING_HEDGE

    def test_submitting_hedge_to_hedge_pending(self):
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
            state=EntryState.SUBMITTING_HEDGE,
        )
        new_ctx = advance_entry_state(ctx, EntryState.HEDGE_PENDING)
        assert new_ctx.state == EntryState.HEDGE_PENDING

    def test_hedge_pending_to_completed(self):
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
            state=EntryState.HEDGE_PENDING,
            maker_fill=OrderFill(
                venue=Venue.BINANCE, symbol="BTCUSDT",
                side=Side.BUY, quantity=0.01, price=50000.0,
                order_id="maker1",
            ),
            hedge_fill=OrderFill(
                venue=Venue.OKX, symbol="BTCUSDT",
                side=Side.SELL, quantity=0.01, price=50000.0,
                order_id="hedge1",
            ),
        )
        new_ctx = advance_entry_state(ctx, EntryState.COMPLETED)
        assert new_ctx.state == EntryState.COMPLETED

    def test_idle_to_failed(self):
        """IDLE -> SUBMITTING_MAKER -> FAILED path"""
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
            state=EntryState.SUBMITTING_MAKER,
        )
        new_ctx = advance_entry_state(ctx, EntryState.FAILED)
        assert new_ctx.state == EntryState.FAILED

    def test_maker_resting_to_passive_fallback(self):
        """MAKER_RESTING -> PASSIVE_FALLBACK transition"""
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
            state=EntryState.MAKER_RESTING,
        )
        new_ctx = advance_entry_state(ctx, EntryState.PASSIVE_FALLBACK)
        assert new_ctx.state == EntryState.PASSIVE_FALLBACK

    def test_submitting_hedge_to_failed_with_residual(self):
        """SUBMITTING_HEDGE -> FAILED_WITH_RESIDUAL transition"""
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
            state=EntryState.SUBMITTING_HEDGE,
            maker_fill=OrderFill(
                venue=Venue.BINANCE, symbol="BTCUSDT",
                side=Side.BUY, quantity=0.01, price=50000.0,
                order_id="maker1",
            ),
        )
        new_ctx = advance_entry_state(ctx, EntryState.FAILED_WITH_RESIDUAL)
        assert new_ctx.state == EntryState.FAILED_WITH_RESIDUAL

    def test_completed_is_terminal(self):
        """COMPLETED cannot transition further."""
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
            state=EntryState.COMPLETED,
        )
        with pytest.raises(ValueError, match="terminal"):
            advance_entry_state(ctx, EntryState.IDLE)

    def test_failed_is_terminal(self):
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
            state=EntryState.FAILED,
        )
        with pytest.raises(ValueError, match="terminal"):
            advance_entry_state(ctx, EntryState.IDLE)


# ---------------------------------------------------------------------------
# build_entry_orders — order construction from EntryContext
# ---------------------------------------------------------------------------


class TestBuildEntryOrders:
    def test_maker_buy_hedge_sell(self):
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=49900.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        maker, hedge = build_entry_orders(ctx)
        assert maker.venue == Venue.BINANCE
        assert maker.side == Side.BUY
        assert maker.price == 50000.0
        assert maker.post_only is True
        assert hedge.venue == Venue.OKX
        assert hedge.side == Side.SELL
        assert hedge.price == 49900.0
        assert hedge.post_only is False

    def test_maker_sell_hedge_buy(self):
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50100.0,
            maker_leg=Side.SELL,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        maker, hedge = build_entry_orders(ctx)
        assert maker.venue == Venue.OKX
        assert maker.side == Side.SELL
        assert maker.post_only is True
        assert hedge.venue == Venue.BINANCE
        assert hedge.side == Side.BUY
        assert hedge.post_only is False

    def test_reduce_only_not_set_on_entry(self):
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        maker, hedge = build_entry_orders(ctx)
        assert maker.reduce_only is False
        assert hedge.reduce_only is False


# ---------------------------------------------------------------------------
# build_open_position — OpenPosition from completed fills
# ---------------------------------------------------------------------------


class TestBuildOpenPosition:
    def test_builds_matched_position(self):
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        maker_fill = OrderFill(
            venue=Venue.BINANCE, symbol="BTCUSDT",
            side=Side.BUY, quantity=0.01, price=50000.0,
            order_id="m1", fee_quote=2.5,
        )
        hedge_fill = OrderFill(
            venue=Venue.OKX, symbol="BTCUSDT",
            side=Side.SELL, quantity=0.01, price=49990.0,
            order_id="h1", fee_quote=2.5,
        )
        pos = build_open_position(ctx, maker_fill, hedge_fill, 1700000000000)
        assert pos.position_id == "e1"
        assert pos.symbol == "BTCUSDT"
        assert pos.long_venue == Venue.BINANCE
        assert pos.short_venue == Venue.OKX
        assert pos.long_quantity == 0.01
        assert pos.short_quantity == 0.01
        assert pos.matched_quantity == 0.01
        assert pos.long_entry_price == 50000.0
        assert pos.short_entry_price == 49990.0
        assert pos.opened_at_ms == 1700000000000

    def test_partial_fill_uses_min_quantity(self):
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.02,
            short_quantity=0.02,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        maker_fill = OrderFill(
            venue=Venue.BINANCE, symbol="BTCUSDT",
            side=Side.BUY, quantity=0.02, price=50000.0,
        )
        hedge_fill = OrderFill(
            venue=Venue.OKX, symbol="BTCUSDT",
            side=Side.SELL, quantity=0.01, price=49900.0,
        )
        pos = build_open_position(ctx, maker_fill, hedge_fill, 0)
        assert pos.matched_quantity == 0.01

    def test_fills_stored_correctly_for_maker_sell(self):
        ctx = EntryContext(
            entry_id="e2",
            symbol="ETHUSDT",
            long_venue=Venue.BYBIT,
            short_venue=Venue.BINANCE,
            long_quantity=0.1,
            short_quantity=0.1,
            long_price_hint=3000.0,
            short_price_hint=3000.0,
            maker_leg=Side.SELL,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        maker_fill = OrderFill(
            venue=Venue.BINANCE, symbol="ETHUSDT",
            side=Side.SELL, quantity=0.1, price=3000.0,
        )
        hedge_fill = OrderFill(
            venue=Venue.BYBIT, symbol="ETHUSDT",
            side=Side.BUY, quantity=0.1, price=3010.0,
        )
        pos = build_open_position(ctx, maker_fill, hedge_fill, 0)
        # maker = SELL = short side, hedge = BUY = long side
        assert pos.short_venue == Venue.BINANCE
        assert pos.long_venue == Venue.BYBIT
        assert pos.short_entry_price == 3000.0
        assert pos.long_entry_price == 3010.0


# ---------------------------------------------------------------------------
# EntryContext with PendingEntry correlation
# ---------------------------------------------------------------------------


class TestEntryContextToPendingEntry:
    def test_entry_context_creates_pending_entry_correlation(self):
        now_ms = 1700000000000
        ctx = EntryContext(
            entry_id="e1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
            created_at_ms=now_ms,
        )
        pe = PendingEntry(
            pending_id=ctx.entry_id,
            symbol=ctx.symbol,
            long_venue=ctx.long_venue,
            short_venue=ctx.short_venue,
            target_quantity=ctx.long_quantity,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=now_ms,
            deadline_ms=now_ms + 30_000,
        )
        assert pe.pending_id == "e1"
        assert pe.symbol == "BTCUSDT"
        assert pe.long_venue == Venue.BINANCE
        assert pe.short_venue == Venue.OKX
        assert pe.deadline_ms == 1700000030000

    def test_pending_entry_fallback_route_defaults(self):
        pe = PendingEntry(
            pending_id="pe1",
            symbol="ETHUSDT",
            long_venue=Venue.BYBIT,
            short_venue=Venue.GATE,
            target_quantity=0.1,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=1000,
        )
        assert pe.fallback_route == ""
        assert pe.uncertain_outcome is False
        assert pe.maker_order_id == ""
        assert pe.hedge_order_id == ""


# ---------------------------------------------------------------------------
# Entry state lifecycle integration
# ---------------------------------------------------------------------------


class TestEntryLifecycle:
    def test_full_happy_path_state_sequence(self):
        """Full IDLE -> COMPLETED transitions."""
        ctx = EntryContext(
            entry_id="full1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
        )

        states = [
            EntryState.SUBMITTING_MAKER,
            EntryState.MAKER_RESTING,
            EntryState.SUBMITTING_HEDGE,
            EntryState.HEDGE_PENDING,
            EntryState.COMPLETED,
        ]
        for expected_next in states:
            ctx = advance_entry_state(ctx, expected_next)
            assert ctx.state == expected_next

    def test_rejected_planner_route_goes_to_failed(self):
        """When planner returns REJECTED, entry must go to FAILED."""
        ctx = EntryContext(
            entry_id="rej1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
            planned_route=ExecutionRoute.REJECTED,
        )
        ctx = advance_entry_state(ctx, EntryState.FAILED)
        assert ctx.state == EntryState.FAILED

    def test_fallback_route_sets_passive_fallback_type(self):
        """When planner returns FALLBACK_TO_STANDARD, entry type becomes PASSIVE_FALLBACK."""
        ctx = EntryContext(
            entry_id="fb1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_FALLBACK,
            planned_route=ExecutionRoute.FALLBACK_TO_STANDARD,
        )
        assert ctx.entry_type == EntryType.PASSIVE_FALLBACK

    def test_standard_dual_taker_entry_type(self):
        ctx = EntryContext(
            entry_id="sdt1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.STANDARD_DUAL_TAKER,
        )
        assert ctx.entry_type == EntryType.STANDARD_DUAL_TAKER
