"""Task 1: Domain, state, precision, and error contract tests.

Lock Rust-equivalent behavior for OpenPosition, PendingEntry, PendingClose,
OrderSubmitError, and quantity normalization.
"""

from __future__ import annotations

import math

import pytest

from lightfee.core.domain import (
    OrderFill,
    OrderRequest,
    Side,
    Venue,
)
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.core.money import floor_to_step, normalize_order_quantity
from lightfee.engine.state import (
    EngineState,
    OpenPosition,
    PendingClose,
    PendingEntry,
)
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode


# ---------------------------------------------------------------------------
# OpenPosition — production state shape
# ---------------------------------------------------------------------------


class TestOpenPositionProductionShape:
    """OpenPosition must carry enough state for live PnL, funding accrual,
    peak drawdown, risk action, and close-deadline decisions."""

    def test_open_position_has_fee_fields(self):
        pos = OpenPosition(
            position_id="p1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=0.01, short_quantity=0.01,
            long_entry_price=50000.0, short_entry_price=50000.0,
            opened_at_ms=1000,
            long_entry_fee_quote=2.5,
            short_entry_fee_quote=2.5,
        )
        assert pos.long_entry_fee_quote == 2.5
        assert pos.short_entry_fee_quote == 2.5

    def test_open_position_has_pnl_fields(self):
        pos = OpenPosition(
            position_id="p1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=0.01, short_quantity=0.01,
            long_entry_price=50000.0, short_entry_price=50000.0,
            opened_at_ms=1000,
            realized_price_pnl_quote=10.0,
            realized_exit_fee_quote=2.5,
        )
        assert pos.realized_price_pnl_quote == 10.0
        assert pos.realized_exit_fee_quote == 2.5

    def test_open_position_has_funding_accrual(self):
        pos = OpenPosition(
            position_id="p1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=0.01, short_quantity=0.01,
            long_entry_price=50000.0, short_entry_price=50000.0,
            opened_at_ms=1000,
            captured_funding_quote=5.0,
            funding_captured=True,
        )
        assert pos.captured_funding_quote == 5.0
        assert pos.funding_captured is True

    def test_open_position_has_edge_and_net_tracking(self):
        """peak_net_quote and current_net_quote needed for trailing drawdown."""
        pos = OpenPosition(
            position_id="p1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=0.01, short_quantity=0.01,
            long_entry_price=50000.0, short_entry_price=50000.0,
            opened_at_ms=1000,
            peak_net_quote=15.0,
            current_net_quote=-5.0,
        )
        assert pos.peak_net_quote == 15.0
        assert pos.current_net_quote == -5.0

    def test_open_position_has_close_deadlines(self):
        """Settlement force close and risk deadlines need timestamps."""
        pos = OpenPosition(
            position_id="p1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=0.01, short_quantity=0.01,
            long_entry_price=50000.0, short_entry_price=50000.0,
            opened_at_ms=1000,
            settlement_half_closed_at_ms=0,
            last_risk_action_at_ms=0,
        )
        assert pos.settlement_half_closed_at_ms == 0
        assert pos.last_risk_action_at_ms == 0

    def test_open_position_has_funding_timing(self):
        """Funding timestamp tracking needed for exit capture stages."""
        pos = OpenPosition(
            position_id="p1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=0.01, short_quantity=0.01,
            long_entry_price=50000.0, short_entry_price=50000.0,
            opened_at_ms=1000,
            funding_timestamp_ms=1700000000000,
            exit_after_first_stage=True,
        )
        assert pos.funding_timestamp_ms == 1700000000000
        assert pos.exit_after_first_stage is True

    def test_open_position_has_matched_quantity(self):
        """Matched quantity = min(long_qty, short_qty), stored independently."""
        pos = OpenPosition(
            position_id="p1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=0.012, short_quantity=0.010,
            long_entry_price=50000.0, short_entry_price=50000.0,
            opened_at_ms=1000,
            matched_quantity=0.010,
        )
        assert pos.matched_quantity == 0.010

    def test_open_position_defaults_are_sensible(self):
        """All new fields must have safe defaults matching V1 zero-init."""
        pos = OpenPosition(
            position_id="p1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=0.01, short_quantity=0.01,
            long_entry_price=50000.0, short_entry_price=50000.0,
            opened_at_ms=1000,
        )
        # Fee fields default to 0
        assert pos.long_entry_fee_quote == 0.0
        assert pos.short_entry_fee_quote == 0.0
        # PnL fields default to 0
        assert pos.realized_price_pnl_quote == 0.0
        assert pos.realized_exit_fee_quote == 0.0
        # Funding defaults
        assert pos.captured_funding_quote == 0.0
        assert pos.funding_captured is False
        # Edge/Net defaults
        assert pos.peak_net_quote == 0.0
        assert pos.current_net_quote == 0.0
        # Deadlines default to 0
        assert pos.settlement_half_closed_at_ms == 0
        assert pos.last_risk_action_at_ms == 0
        # Matched quantity defaults to long_quantity
        assert pos.matched_quantity == 0.01
        # Funding timing
        assert pos.funding_timestamp_ms == 0
        assert pos.exit_after_first_stage is False


# ---------------------------------------------------------------------------
# PendingEntry — production state shape
# ---------------------------------------------------------------------------


class TestPendingEntryProductionShape:
    """PendingEntry must carry maker/hedge order IDs, fill quantities,
    deadline, and uncertain outcome flags for recovery."""

    def test_pending_entry_has_order_ids(self):
        pe = PendingEntry(
            pending_id="pe1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            target_quantity=0.01,
            long_side=Side.BUY, short_side=Side.SELL,
            created_at_ms=1000,
            maker_order_id="maker123",
            hedge_order_id="hedge456",
        )
        assert pe.maker_order_id == "maker123"
        assert pe.hedge_order_id == "hedge456"

    def test_pending_entry_has_deadline(self):
        pe = PendingEntry(
            pending_id="pe1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            target_quantity=0.01,
            long_side=Side.BUY, short_side=Side.SELL,
            created_at_ms=1000,
            deadline_ms=1000 + 30_000,
        )
        assert pe.deadline_ms == 31000

    def test_pending_entry_has_fallback_state(self):
        pe = PendingEntry(
            pending_id="pe1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            target_quantity=0.01,
            long_side=Side.BUY, short_side=Side.SELL,
            created_at_ms=1000,
            fallback_route="standard_taker",
        )
        assert pe.fallback_route == "standard_taker"

    def test_pending_entry_has_uncertain_flag(self):
        pe = PendingEntry(
            pending_id="pe1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            target_quantity=0.01,
            long_side=Side.BUY, short_side=Side.SELL,
            created_at_ms=1000,
            uncertain_outcome=True,
        )
        assert pe.uncertain_outcome is True

    def test_pending_entry_has_maker_and_hedge_fill_quantities(self):
        pe = PendingEntry(
            pending_id="pe1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            target_quantity=0.01,
            long_side=Side.BUY, short_side=Side.SELL,
            created_at_ms=1000,
            maker_leg_filled=0.005,
            hedge_leg_filled=0.01,
        )
        assert pe.maker_leg_filled == 0.005
        assert pe.hedge_leg_filled == 0.01

    def test_pending_entry_defaults(self):
        pe = PendingEntry(
            pending_id="pe1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            target_quantity=0.01,
            long_side=Side.BUY, short_side=Side.SELL,
            created_at_ms=1000,
        )
        assert pe.maker_order_id == ""
        assert pe.hedge_order_id == ""
        assert pe.deadline_ms == 0
        assert pe.fallback_route == ""
        assert pe.uncertain_outcome is False


# ---------------------------------------------------------------------------
# PendingClose — production state shape
# ---------------------------------------------------------------------------


class TestPendingCloseProductionShape:
    """PendingClose must carry long/short order IDs, close quantities,
    reason, deadline, and uncertain outcome flags for recovery."""

    def test_pending_close_has_order_ids(self):
        pc = PendingClose(
            close_id="pc1", position_id="p1",
            reason="funding_capture",
            created_at_ms=1000,
            long_order_id="lo123",
            short_order_id="so456",
        )
        assert pc.long_order_id == "lo123"
        assert pc.short_order_id == "so456"

    def test_pending_close_has_close_quantities(self):
        pc = PendingClose(
            close_id="pc1", position_id="p1",
            reason="funding_capture",
            created_at_ms=1000,
            long_target_close_qty=0.005,
            short_target_close_qty=0.005,
        )
        assert pc.long_target_close_qty == 0.005
        assert pc.short_target_close_qty == 0.005

    def test_pending_close_has_deadline(self):
        pc = PendingClose(
            close_id="pc1", position_id="p1",
            reason="funding_capture",
            created_at_ms=1000,
            deadline_ms=1000 + 60_000,
        )
        assert pc.deadline_ms == 61000

    def test_pending_close_has_uncertain_flag(self):
        pc = PendingClose(
            close_id="pc1", position_id="p1",
            reason="risk_death",
            created_at_ms=1000,
            long_uncertain=True,
            short_uncertain=True,
        )
        assert pc.long_uncertain is True
        assert pc.short_uncertain is True

    def test_pending_close_defaults(self):
        pc = PendingClose(
            close_id="pc1", position_id="p1",
            reason="profit_take",
            created_at_ms=1000,
        )
        assert pc.long_order_id == ""
        assert pc.short_order_id == ""
        assert pc.long_target_close_qty == 0.0
        assert pc.short_target_close_qty == 0.0
        assert pc.deadline_ms == 0
        assert pc.long_uncertain is False
        assert pc.short_uncertain is False


# ---------------------------------------------------------------------------
# OrderSubmitError — REJECTED vs UNCERTAIN
# ---------------------------------------------------------------------------


class TestOrderSubmitError:
    def test_rejected_is_distinct_from_uncertain(self):
        rejected = OrderSubmitError(SubmitFailureClass.REJECTED, "bad")
        uncertain = OrderSubmitError(SubmitFailureClass.UNCERTAIN, "timeout")
        assert rejected.is_rejected
        assert not rejected.is_uncertain
        assert uncertain.is_uncertain
        assert not uncertain.is_rejected

    def test_rejected_class_enum_values(self):
        assert SubmitFailureClass.REJECTED.value == "rejected"
        assert SubmitFailureClass.UNCERTAIN.value == "uncertain"


# ---------------------------------------------------------------------------
# Quantity normalization — floor behavior
# ---------------------------------------------------------------------------


class TestQuantityNormalization:
    def test_floor_floors_not_rounds(self):
        assert normalize_order_quantity(0.007, 0.001) == 0.007
        assert normalize_order_quantity(0.0079, 0.001) == 0.007
        assert normalize_order_quantity(0.0011, 0.001) == 0.001

    def test_floor_to_step_alias(self):
        assert floor_to_step(1.7, 1.0) == 1.0
        assert floor_to_step(2.0, 0.5) == 2.0

    def test_normalize_zero_or_negative_returns_zero(self):
        assert normalize_order_quantity(0.0, 0.01) == 0.0
        assert normalize_order_quantity(-5.0, 1.0) == 0.0

    def test_normalize_non_finite_returns_zero(self):
        assert normalize_order_quantity(float("inf"), 1.0) == 0.0
        assert normalize_order_quantity(float("nan"), 1.0) == 0.0

    def test_normalize_below_step_returns_zero(self):
        assert normalize_order_quantity(0.0005, 0.001) == 0.0


# ---------------------------------------------------------------------------
# EngineState — serialization for recovery
# ---------------------------------------------------------------------------


class TestEngineStateSerialization:
    def test_serialization_includes_lifecycle_risk_mode(self):
        state = EngineState(
            lifecycle=EngineLifecycle.RUNNING,
            risk_mode=GlobalRiskMode.ENTRY_PAUSED,
            run_id="test-run-1",
        )
        d = state.to_dict()
        assert d["lifecycle"] == "running"
        assert d["risk_mode"] == "entry_paused"
        assert d["run_id"] == "test-run-1"

    def test_serialization_includes_position_counts(self):
        state = EngineState(run_id="r1")
        state.open_positions["p1"] = OpenPosition(
            position_id="p1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=0.01, short_quantity=0.01,
            long_entry_price=50000.0, short_entry_price=50000.0,
            opened_at_ms=1000,
        )
        state.pending_entries["pe1"] = PendingEntry(
            pending_id="pe1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            target_quantity=0.01, long_side=Side.BUY,
            short_side=Side.SELL, created_at_ms=1000,
        )
        state.pending_closes["pc1"] = PendingClose(
            close_id="pc1", position_id="p1",
            reason="funding_capture", created_at_ms=1000,
        )
        d = state.to_dict()
        assert d["open_position_count"] == 1
        assert d["pending_entry_count"] == 1
        assert d["pending_close_count"] == 1

    def test_serialization_includes_tick_stats(self):
        state = EngineState(
            run_id="r1",
            last_tick_ms=1700000000000,
            tick_count=42,
            started_at_ms=1700000000000,
        )
        d = state.to_dict()
        assert d["last_tick_ms"] == 1700000000000
        assert d["tick_count"] == 42
