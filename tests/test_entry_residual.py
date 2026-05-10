"""Task 3: Residual protection contract tests.

Rust references:
- src/execution_core/residual.rs: split_entry_fill_residual (line 25)
- src/execution_core/residual.rs: split_close_fill_residual (line 75)
- src/execution_core/entry_sync.rs: build_residual_task (line 749)
"""

from __future__ import annotations

import pytest

from lightfee.core.domain import OrderFill, Side, Venue
from lightfee.engine.entry import EntryContext, EntryType
from lightfee.engine.residual import (
    ResidualExposureTask,
    ResidualOrigin,
    split_entry_fill_residual,
)
from lightfee.engine.state import OpenPosition


# ---------------------------------------------------------------------------
# split_entry_fill_residual
# ---------------------------------------------------------------------------


class TestSplitEntryFillResidual:
    def test_matched_fills_return_none(self):
        long_fill = OrderFill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.01, 50000.0)
        short_fill = OrderFill(Venue.OKX, "BTCUSDT", Side.SELL, 0.01, 50000.0)
        result = split_entry_fill_residual(
            position_id="p1",
            pair_id="btcusdt:binance->okx",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_fill=long_fill,
            short_fill=short_fill,
            created_cycle=1,
            now_ms=1000,
            deadline_ms=60000,
        )
        assert result is None

    def test_long_excess_creates_sell_residual(self):
        long_fill = OrderFill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.02, 50000.0)
        short_fill = OrderFill(Venue.OKX, "BTCUSDT", Side.SELL, 0.01, 50000.0)
        result = split_entry_fill_residual(
            position_id="p1",
            pair_id="btcusdt:binance->okx",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_fill=long_fill,
            short_fill=short_fill,
            created_cycle=1,
            now_ms=1000,
            deadline_ms=60000,
        )
        assert result is not None
        assert result.exposure_venue == Venue.BINANCE
        assert result.exposure_side == Side.SELL  # sell to reduce long
        assert result.exposure_quantity == pytest.approx(0.01)
        assert result.origin == ResidualOrigin.ENTRY_OPEN

    def test_short_excess_creates_buy_residual(self):
        long_fill = OrderFill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.01, 50000.0)
        short_fill = OrderFill(Venue.OKX, "BTCUSDT", Side.SELL, 0.03, 50000.0)
        result = split_entry_fill_residual(
            position_id="p2",
            pair_id="btcusdt:binance->okx",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_fill=long_fill,
            short_fill=short_fill,
            created_cycle=1,
            now_ms=2000,
            deadline_ms=60000,
        )
        assert result is not None
        assert result.exposure_venue == Venue.OKX
        assert result.exposure_side == Side.BUY  # buy to reduce short
        assert result.exposure_quantity == pytest.approx(0.02)

    def test_zero_quantity_fill_handled(self):
        long_fill = OrderFill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.0, 50000.0)
        short_fill = OrderFill(Venue.OKX, "BTCUSDT", Side.SELL, 0.01, 50000.0)
        result = split_entry_fill_residual(
            position_id="p3",
            pair_id="btcusdt:binance->okx",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_fill=long_fill,
            short_fill=short_fill,
            created_cycle=1,
            now_ms=1000,
            deadline_ms=60000,
        )
        assert result is not None
        assert result.exposure_venue == Venue.OKX
        assert result.exposure_quantity == pytest.approx(0.01)


# ---------------------------------------------------------------------------
# ResidualExposureTask contract
# ---------------------------------------------------------------------------


class TestResidualExposureTaskContract:
    def test_task_holds_all_required_fields(self):
        task = ResidualExposureTask(
            position_id="p1",
            pair_id="btcusdt:binance->okx",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            origin=ResidualOrigin.ENTRY_OPEN,
            exposure_venue=Venue.BINANCE,
            exposure_side=Side.SELL,
            exposure_quantity=0.005,
            created_cycle=1,
            created_at_ms=1000,
            deadline_ms=31000,
        )
        assert task.exposure_quantity == 0.005
        assert task.origin == ResidualOrigin.ENTRY_OPEN
        assert task.deadline_ms == 31000
        assert task.retry_count == 0

    def test_deadline_default(self):
        task = ResidualExposureTask(
            position_id="p1",
            pair_id="btcusdt:binance->okx",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            origin=ResidualOrigin.ENTRY_OPEN,
            exposure_venue=Venue.BINANCE,
            exposure_side=Side.SELL,
            exposure_quantity=0.005,
        )
        assert task.deadline_ms > 0  # default computed from now + 30s

    def test_retry_increment(self):
        task = ResidualExposureTask(
            position_id="p1",
            pair_id="btcusdt:binance->okx",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            origin=ResidualOrigin.ENTRY_OPEN,
            exposure_venue=Venue.BINANCE,
            exposure_side=Side.SELL,
            exposure_quantity=0.005,
        )
        assert task.retry_count == 0
        task.increment_retry()
        assert task.retry_count == 1

    def test_max_retries_exceeded(self):
        task = ResidualExposureTask(
            position_id="p1",
            pair_id="btcusdt:binance->okx",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            origin=ResidualOrigin.ENTRY_OPEN,
            exposure_venue=Venue.BINANCE,
            exposure_side=Side.SELL,
            exposure_quantity=0.005,
        )
        assert not task.is_exhausted()
        for _ in range(3):
            task.increment_retry()
        assert task.is_exhausted()


# ---------------------------------------------------------------------------
# ResidualOrigin enum values
# ---------------------------------------------------------------------------


class TestResidualOrigin:
    def test_origin_values(self):
        assert ResidualOrigin.ENTRY_OPEN.value == "entry_open"
        assert ResidualOrigin.CLOSE_RESIDUAL.value == "close_residual"
