"""Task 7: Local L2 state machine tests — cold→bootstrapping→hot→degraded→suspended.

Rust references:
- src/execution_core/market_data.rs: L2 book state management
"""

from __future__ import annotations

import pytest

from lightfee.marketdata.l2 import (
    ExecutionLiquiditySource,
    L2BookStatus,
    L2PoolAssignment,
    LocalL2Book,
    PriceLevel,
    promote_warm_to_hot,
)


# ---------------------------------------------------------------------------
# L2BookStatus state machine
# ---------------------------------------------------------------------------


class TestL2StateMachine:
    def test_cold_to_bootstrapping(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        assert book.status == L2BookStatus.COLD
        book.transition_to_bootstrapping(now_ms=5000)
        assert book.status == L2BookStatus.BOOTSTRAPPING
        assert book.bootstrap_started_ms == 5000

    def test_bootstrapping_to_hot(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.BOOTSTRAPPING)
        book.transition_to_hot()
        assert book.status == L2BookStatus.HOT
        assert book.degrade_count == 0

    def test_hot_to_degraded(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.HOT)
        book.transition_to_degraded(error="stream disconnected")
        assert book.status == L2BookStatus.DEGRADED
        assert book.degrade_count == 1
        assert book.last_error == "stream disconnected"

    def test_degraded_to_rebuilding(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.DEGRADED)
        book.transition_to_rebuilding()
        assert book.status == L2BookStatus.REBUILDING

    def test_rebuilding_to_hot(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.REBUILDING)
        book.transition_to_hot()
        assert book.status == L2BookStatus.HOT
        assert book.degrade_count == 0  # reset

    def test_repeated_degradation_to_suspended(self):
        """V1: max_consecutive_degradations=3 → 3rd degrade goes to SUSPENDED."""
        book = LocalL2Book(
            venue="binance", symbol="BTCUSDT",
            status=L2BookStatus.HOT, max_consecutive_degradations=3,
        )
        book.transition_to_degraded(error="fail 1")
        assert book.status == L2BookStatus.DEGRADED
        assert book.degrade_count == 1

        book.transition_to_rebuilding()
        book.transition_to_hot()
        assert book.status == L2BookStatus.HOT

        book.transition_to_degraded(error="fail 2")
        assert book.status == L2BookStatus.DEGRADED
        assert book.degrade_count == 2

        book.transition_to_rebuilding()
        book.transition_to_hot()

        book.transition_to_degraded(error="fail 3")
        assert book.status == L2BookStatus.SUSPENDED  # auto-suspend on 3rd degrade
        assert book.degrade_count == 3

    def test_cold_to_hot_directly_not_allowed(self):
        """Cold cannot go to hot directly — needs bootstrapping."""
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.transition_to_hot()
        assert book.status == L2BookStatus.COLD  # unchanged

    def test_hot_to_rebuilding_not_allowed(self):
        """Hot cannot go directly to rebuilding — must degrade first."""
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.HOT)
        book.transition_to_rebuilding()
        assert book.status == L2BookStatus.HOT  # unchanged

    def test_explicit_suspend(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.HOT)
        book.transition_to_suspended()
        assert book.status == L2BookStatus.SUSPENDED


# ---------------------------------------------------------------------------
# is_healthy / is_stale
# ---------------------------------------------------------------------------


class TestBookHealth:
    def test_hot_is_healthy(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.HOT)
        assert book.is_healthy()

    def test_bootstrapping_is_healthy(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.BOOTSTRAPPING)
        assert book.is_healthy()

    def test_degraded_is_not_healthy(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.DEGRADED)
        assert not book.is_healthy()

    def test_suspended_is_not_healthy(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.SUSPENDED)
        assert not book.is_healthy()

    def test_cold_is_not_healthy(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        assert not book.is_healthy()

    def test_not_stale_when_fresh(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", observed_at_ms=10000)
        assert not book.is_stale(3000, 11000)  # age=1000ms < 3000

    def test_stale_when_exceeds_max_age(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", observed_at_ms=10000)
        assert book.is_stale(3000, 14000)  # age=4000ms > 3000

    def test_stall_detection(self):
        book = LocalL2Book(
            venue="binance", symbol="BTCUSDT",
            observed_at_ms=10000, stall_timeout_ms=60_000,
        )
        assert not book.check_stall(50000)  # 40s old, within 60s
        assert book.check_stall(80000)  # 70s old, exceeds 60s

    def test_stall_zero_observed_not_stalled(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", observed_at_ms=0)
        assert not book.check_stall(100000)


# ---------------------------------------------------------------------------
# Pool assignment
# ---------------------------------------------------------------------------


class TestPoolAssignment:
    def test_promote_warm_to_hot(self):
        books = {
            "btc_binance": LocalL2Book(
                venue="binance", symbol="BTCUSDT",
                status=L2BookStatus.HOT, pool=L2PoolAssignment.WARM,
            ),
            "eth_binance": LocalL2Book(
                venue="binance", symbol="ETHUSDT",
                status=L2BookStatus.HOT, pool=L2PoolAssignment.WARM,
            ),
        }
        promoted = promote_warm_to_hot(books, max_hot=3)
        assert promoted == 2
        for b in books.values():
            assert b.pool == L2PoolAssignment.HOT_EXEC

    def test_promote_respects_max_hot(self):
        books = {
            "btc_binance": LocalL2Book(
                venue="binance", symbol="BTCUSDT",
                status=L2BookStatus.HOT, pool=L2PoolAssignment.HOT_EXEC,
            ),
            "eth_binance": LocalL2Book(
                venue="binance", symbol="ETHUSDT",
                status=L2BookStatus.HOT, pool=L2PoolAssignment.WARM,
            ),
            "sol_binance": LocalL2Book(
                venue="binance", symbol="SOLUSDT",
                status=L2BookStatus.HOT, pool=L2PoolAssignment.WARM,
            ),
        }
        promoted = promote_warm_to_hot(books, max_hot=2)  # already 1 hot → room for 1
        assert promoted == 1

    def test_degraded_not_promoted(self):
        books = {
            "btc_binance": LocalL2Book(
                venue="binance", symbol="BTCUSDT",
                status=L2BookStatus.DEGRADED, pool=L2PoolAssignment.WARM,
            ),
        }
        promoted = promote_warm_to_hot(books, max_hot=3)
        assert promoted == 0


# ---------------------------------------------------------------------------
# ExecutionLiquiditySource
# ---------------------------------------------------------------------------


class TestExecutionLiquiditySource:
    def test_values(self):
        assert ExecutionLiquiditySource.TRUE_L2.value == "true_l2"
        assert ExecutionLiquiditySource.TOP_BOOK.value == "top_book"
        assert ExecutionLiquiditySource.CACHED.value == "cached"
        assert ExecutionLiquiditySource.NONE.value == "none"


# ---------------------------------------------------------------------------
# PriceLevel
# ---------------------------------------------------------------------------


class TestPriceLevel:
    def test_construction(self):
        level = PriceLevel(price=50000.0, quantity=0.5)
        assert level.price == 50000.0
        assert level.quantity == 0.5
