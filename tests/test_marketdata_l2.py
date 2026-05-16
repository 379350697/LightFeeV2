"""Local L2 book core tests — Rust V1 parity.

Covers:
  - State machine transitions (cold→bootstrapping→hot→degraded→suspended, resume_waiting)
  - Snapshot, delta, zero-size delete
  - Sequence gap detection, checksum verification
  - Age, staleness, readiness queries
  - New dataclasses: LocalL2BookKey, LocalL2Update, LocalL2Event, LocalL2UpdateResult
"""

from __future__ import annotations

import pytest

from lightfee.marketdata.l2 import (
    ExecutionLiquiditySource,
    L2BookStatus,
    L2PoolAssignment,
    LocalL2Book,
    LocalL2BookKey,
    LocalL2Event,
    LocalL2EventKind,
    LocalL2Update,
    LocalL2UpdateKind,
    LocalL2UpdateResult,
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

    def test_hot_to_rebuilding_allowed(self):
        """V1 parity: HOT can transition to REBUILDING on sequence gap / checksum mismatch."""
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.HOT)
        book.transition_to_rebuilding()
        assert book.status == L2BookStatus.REBUILDING

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


# ---------------------------------------------------------------------------
# LocalL2BookKey
# ---------------------------------------------------------------------------


class TestLocalL2BookKey:
    def test_construction(self):
        key = LocalL2BookKey(venue="binance", symbol="BTCUSDT")
        assert key.venue == "binance"
        assert key.symbol == "BTCUSDT"

    def test_equality(self):
        a = LocalL2BookKey(venue="binance", symbol="BTCUSDT")
        b = LocalL2BookKey(venue="binance", symbol="BTCUSDT")
        c = LocalL2BookKey(venue="bybit", symbol="BTCUSDT")
        assert a == b
        assert a != c

    def test_hashable(self):
        d: dict[LocalL2BookKey, int] = {}
        d[LocalL2BookKey(venue="binance", symbol="BTCUSDT")] = 1
        assert d[LocalL2BookKey(venue="binance", symbol="BTCUSDT")] == 1

    def test_str(self):
        key = LocalL2BookKey(venue="binance", symbol="BTCUSDT")
        assert "binance" in str(key)
        assert "BTCUSDT" in str(key)


# ---------------------------------------------------------------------------
# Snapshot application
# ---------------------------------------------------------------------------


class TestSnapshotApplication:
    def test_snapshot_creates_sorted_book(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        bids = [
            PriceLevel(price=49900, quantity=1.0),
            PriceLevel(price=50000, quantity=2.0),
            PriceLevel(price=49800, quantity=0.5),
        ]
        asks = [
            PriceLevel(price=50200, quantity=1.5),
            PriceLevel(price=50100, quantity=1.0),
            PriceLevel(price=50300, quantity=0.5),
        ]
        result = book.apply_snapshot(bids, asks, sequence=100, now_ms=10000)
        assert result.applied
        # Bids sorted descending
        assert book.bids[0].price == 50000
        assert book.bids[1].price == 49900
        assert book.bids[2].price == 49800
        # Asks sorted ascending
        assert book.asks[0].price == 50100
        assert book.asks[1].price == 50200
        assert book.asks[2].price == 50300
        assert book.sequence == 100
        assert book.last_snapshot_ms == 10000
        assert book.observed_at_ms == 10000

    def test_snapshot_emits_best_bid_ask_events(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        result = book.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.0)],
            sequence=1,
            now_ms=10000,
        )
        kinds = {e.event_kind for e in result.events}
        assert LocalL2EventKind.BEST_BID_UPDATED in kinds
        assert LocalL2EventKind.BEST_ASK_UPDATED in kinds

    def test_snapshot_trims_to_max_depth(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", max_depth=2)
        bids = [PriceLevel(price=50000 - i * 100, quantity=1.0) for i in range(5)]
        asks = [PriceLevel(price=50100 + i * 100, quantity=1.0) for i in range(5)]
        book.apply_snapshot(bids, asks, sequence=1, now_ms=10000)
        assert len(book.bids) == 2
        assert len(book.asks) == 2

    def test_snapshot_empty_lists(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        result = book.apply_snapshot([], [], now_ms=10000)
        assert result.applied
        assert len(book.bids) == 0
        assert len(book.asks) == 0


# ---------------------------------------------------------------------------
# Delta application
# ---------------------------------------------------------------------------


class TestDeltaApplication:
    def test_delta_updates_price_level(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=2.0), PriceLevel(price=49900, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.5)],
            sequence=100,
            now_ms=10000,
        )
        # Delta: update quantity at 50000
        result = book.apply_delta(
            [PriceLevel(price=50000, quantity=3.0)],
            [],
            sequence=101,
            previous_sequence=100,
            now_ms=11000,
        )
        assert result.applied
        assert book.bids[0].quantity == 3.0
        assert book.sequence == 101
        assert book.last_delta_ms == 11000

    def test_delta_deletes_zero_quantity(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=2.0), PriceLevel(price=49900, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.5)],
            sequence=100,
            now_ms=10000,
        )
        # Delta: delete 50000 level
        result = book.apply_delta(
            [PriceLevel(price=50000, quantity=0.0)],
            [],
            sequence=101,
            previous_sequence=100,
            now_ms=11000,
        )
        assert result.applied
        prices = [lvl.price for lvl in book.bids]
        assert 50000 not in prices
        assert 49900 in prices

    def test_delta_inserts_new_level(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=2.0)],
            [PriceLevel(price=50100, quantity=1.5)],
            sequence=100,
            now_ms=10000,
        )
        # Delta: insert new bid level inside the spread
        result = book.apply_delta(
            [PriceLevel(price=50050, quantity=1.0)],
            [],
            sequence=101,
            previous_sequence=100,
            now_ms=11000,
        )
        assert result.applied
        assert book.bids[0].price == 50050  # new highest bid

    def test_delta_emits_mid_price_event(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.0)],
            sequence=100,
            now_ms=10000,
        )
        result = book.apply_delta(
            [PriceLevel(price=50050, quantity=1.0)],
            [],
            sequence=101,
            now_ms=11000,
        )
        kinds = {e.event_kind for e in result.events}
        assert LocalL2EventKind.MID_PRICE_CHANGED in kinds

    def test_delta_respects_max_depth(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", max_depth=3)
        book.apply_snapshot(
            [PriceLevel(price=50000 - i * 100, quantity=1.0) for i in range(3)],
            [PriceLevel(price=50100 + i * 100, quantity=1.0) for i in range(3)],
            sequence=100,
            now_ms=10000,
        )
        # Insert a new best bid — should push out the lowest
        book.apply_delta(
            [PriceLevel(price=50050, quantity=1.0)],
            [],
            sequence=101,
            now_ms=11000,
        )
        assert len(book.bids) == 3
        assert book.bids[0].price == 50050


# ---------------------------------------------------------------------------
# Sequence gap detection
# ---------------------------------------------------------------------------


class TestSequenceGap:
    def test_small_gap_accepted_with_event(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", max_sequence_gap=10)
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.0)],
            sequence=100,
            now_ms=10000,
        )
        # Gap of 5 (prev_seq=105, current=100) — within limit
        result = book.apply_delta(
            [], [],
            sequence=105,
            previous_sequence=104,
            now_ms=11000,
        )
        assert result.applied
        assert any(e.event_kind == LocalL2EventKind.SEQUENCE_GAP for e in result.events)

    def test_large_gap_returns_rebuild_required(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", max_sequence_gap=5)
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.0)],
            sequence=100,
            now_ms=10000,
        )
        # Gap of 20 (prev_seq=120, book=100)
        result = book.apply_delta(
            [], [],
            sequence=120,
            previous_sequence=119,
            now_ms=11000,
        )
        assert not result.applied
        assert result.rebuild_required
        assert "sequence_gap" in result.fault_reason

    def test_gap_strict_when_max_zero(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", max_sequence_gap=0)
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.0)],
            sequence=100,
            now_ms=10000,
        )
        # max_sequence_gap=0 means strict continuity.
        result = book.apply_delta(
            [], [],
            sequence=1100,
            previous_sequence=1099,
            now_ms=11000,
        )
        assert not result.applied
        assert result.rebuild_required
        assert "sequence_gap" in result.fault_reason


# ---------------------------------------------------------------------------
# Checksum verification
# ---------------------------------------------------------------------------


class TestChecksumVerification:
    def test_checksum_mismatch_returns_event(self):
        book = LocalL2Book(venue="okx", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.0)],
            sequence=100,
            checksum=12345,
            now_ms=10000,
        )
        result = book.verify_checksum(expected=99999, now_ms=11000)
        assert any(e.event_kind == LocalL2EventKind.CHECKSUM_MISMATCH for e in result.events)
        assert "checksum_mismatch" in result.fault_reason

    def test_checksum_match_no_event(self):
        book = LocalL2Book(venue="okx", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.0)],
            sequence=100,
            checksum=12345,
            now_ms=10000,
        )
        actual = book.compute_checksum()
        result = book.verify_checksum(expected=actual, now_ms=11000)
        assert len(result.events) == 0

    def test_checksum_zero_skipped(self):
        book = LocalL2Book(venue="okx", symbol="BTCUSDT")
        result = book.verify_checksum(expected=0, now_ms=11000)
        assert result.applied
        assert len(result.events) == 0

    def test_checksum_is_deterministic_for_same_book_state(self):
        """V1 parity: same book state must produce identical CRC32 checksum."""
        bids = [PriceLevel(price=50000, quantity=1.5), PriceLevel(price=49900, quantity=2.0)]
        asks = [PriceLevel(price=50100, quantity=1.0), PriceLevel(price=50200, quantity=0.5)]

        book_a = LocalL2Book(venue="okx", symbol="BTCUSDT")
        book_a.apply_snapshot(bids, asks, sequence=100, now_ms=10000)
        csum_a = book_a.compute_checksum()

        book_b = LocalL2Book(venue="okx", symbol="BTCUSDT")
        book_b.apply_snapshot(bids, asks, sequence=100, now_ms=10000)
        csum_b = book_b.compute_checksum()

        assert csum_a != 0
        assert csum_a == csum_b, (
            f"CRC32 checksum must be deterministic: {csum_a} != {csum_b}"
        )

    def test_checksum_changes_when_top_of_book_changes(self):
        """V1 parity: different book state must produce different checksum."""
        book_a = LocalL2Book(venue="okx", symbol="BTCUSDT")
        book_a.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.0)],
            sequence=100, now_ms=10000,
        )
        csum_a = book_a.compute_checksum()

        book_b = LocalL2Book(venue="okx", symbol="BTCUSDT")
        book_b.apply_snapshot(
            [PriceLevel(price=50001, quantity=1.0)],  # different best bid
            [PriceLevel(price=50100, quantity=1.0)],
            sequence=100, now_ms=10000,
        )
        csum_b = book_b.compute_checksum()

        assert csum_a != csum_b, "Different top-of-book must produce different CRC32 checksum"


# ---------------------------------------------------------------------------
# Queries: mid, spread, depth, vwap, crossed
# ---------------------------------------------------------------------------


class TestBookQueries:
    def test_mid_price(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.0)],
            sequence=1,
            now_ms=10000,
        )
        assert book.mid_price() == 50050.0

    def test_mid_price_zero_when_no_bid(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot([], [PriceLevel(price=50100, quantity=1.0)])
        assert book.mid_price() == 0.0

    def test_mid_price_zero_when_no_ask(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot([PriceLevel(price=50000, quantity=1.0)], [])
        assert book.mid_price() == 0.0

    def test_spread_bps(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.0)],
            sequence=1,
            now_ms=10000,
        )
        expected = (50100 - 50000) / 50000 * 10000
        assert book.spread_bps() == pytest.approx(expected)

    def test_crossed_book_detection(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.bids = [PriceLevel(price=50100, quantity=1.0)]
        book.asks = [PriceLevel(price=50000, quantity=1.0)]
        assert book.has_crossed_book()

    def test_crossed_book_false_when_normal(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.0)],
        )
        assert not book.has_crossed_book()

    def test_vwap_buy_estimates_correctly(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0)],
            [PriceLevel(price=50100, quantity=0.5), PriceLevel(price=50200, quantity=1.0)],
        )
        filled, avg = book.vwap_buy(30000)  # target 30000 quote
        # 50100*0.5=25050, need 4950 more from 50200*1=50200 → take 4950/50200=0.0986...
        assert filled > 0
        assert avg > 50100

    def test_vwap_sell_estimates_correctly(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(price=50100, quantity=1.0), PriceLevel(price=50000, quantity=0.5)],
            [PriceLevel(price=50200, quantity=1.0)],
        )
        filled, avg = book.vwap_sell(55000)
        assert filled > 0
        assert avg < 50100

    def test_depth_bid(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(price=50000 - i * 100, quantity=1.0) for i in range(5)],
            [],
        )
        assert len(book.depth_bid(3)) == 3

    def test_depth_ask(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot(
            [],
            [PriceLevel(price=50100 + i * 100, quantity=1.0) for i in range(5)],
        )
        assert len(book.depth_ask(3)) == 3

    def test_cumulative_bid_quantity(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=2.0), PriceLevel(price=49900, quantity=3.0)],
            [],
        )
        total = book.cumulative_bid_quantity(from_price=49950)
        assert total == 2.0  # only 50000 >= 49950

    def test_cumulative_ask_quantity(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot(
            [],
            [PriceLevel(price=50100, quantity=2.0), PriceLevel(price=50200, quantity=3.0)],
        )
        total = book.cumulative_ask_quantity(to_price=50150)
        assert total == 2.0  # only 50100 <= 50150

    def test_quantity_at_price(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=2.0)],
            [PriceLevel(price=50100, quantity=1.5)],
        )
        assert book.quantity_at_price("buy", 50000) == 2.0
        assert book.quantity_at_price("sell", 50100) == 1.5
        assert book.quantity_at_price("buy", 99999) == 0.0


# ---------------------------------------------------------------------------
# is_ready and resume_waiting
# ---------------------------------------------------------------------------


class TestBookReadiness:
    def test_hot_and_fresh_is_ready(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.HOT, observed_at_ms=10000)
        assert book.is_ready(max_age_ms=5000, now_ms=12000)

    def test_stale_is_not_ready(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.HOT, observed_at_ms=10000)
        assert not book.is_ready(max_age_ms=5000, now_ms=16000)

    def test_not_hot_is_not_ready(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.BOOTSTRAPPING, observed_at_ms=10000)
        assert not book.is_ready(max_age_ms=5000, now_ms=12000)

    def test_resume_waiting_remaining(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", resume_waiting_until_ms=20000)
        assert book.resume_waiting_remaining_ms(now_ms=15000) == 5000
        assert book.resume_waiting_remaining_ms(now_ms=25000) == 0

    def test_resume_waiting_remaining_zero_when_not_set(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        assert book.resume_waiting_remaining_ms(now_ms=15000) == 0

    def test_age_ms(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", observed_at_ms=10000)
        assert book.age_ms(now_ms=13000) == 3000
        assert book.age_ms(now_ms=10000) == 0

    def test_transition_to_resume_waiting(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.HOT)
        book.transition_to_resume_waiting(until_ms=20000)
        assert book.status == L2BookStatus.RESUME_WAITING
        assert book.resume_waiting_until_ms == 20000

    def test_resume_waiting_to_hot(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.RESUME_WAITING)
        book.transition_to_hot()
        assert book.status == L2BookStatus.HOT

    def test_resume_waiting_to_bootstrapping(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.RESUME_WAITING)
        book.transition_to_bootstrapping(now_ms=10000)
        assert book.status == L2BookStatus.BOOTSTRAPPING


# ---------------------------------------------------------------------------
# clear_book
# ---------------------------------------------------------------------------


class TestClearBook:
    def test_clear_book_empties_levels(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.0)],
        )
        events = book.clear_book(now_ms=10000)
        assert len(book.bids) == 0
        assert len(book.asks) == 0
        assert any(e.event_kind == LocalL2EventKind.BOOK_CLEARED for e in events)


# ---------------------------------------------------------------------------
# key property
# ---------------------------------------------------------------------------


class TestBookKeyProperty:
    def test_key_property(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        key = book.key
        assert key.venue == "binance"
        assert key.symbol == "BTCUSDT"

    def test_same_book_same_key(self):
        a = LocalL2Book(venue="binance", symbol="BTCUSDT").key
        b = LocalL2Book(venue="binance", symbol="BTCUSDT").key
        assert a == b


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


class TestLocalL2Update:
    def test_defaults(self):
        u = LocalL2Update(venue="binance", symbol="BTCUSDT")
        assert u.update_kind == LocalL2UpdateKind.DELTA
        assert u.sequence == 0

    def test_full_construction(self):
        u = LocalL2Update(
            venue="binance", symbol="BTCUSDT",
            bids=[PriceLevel(price=50000, quantity=1.0)],
            asks=[PriceLevel(price=50100, quantity=1.0)],
            sequence=100, previous_sequence=99,
            checksum=12345, event_time_ms=10000, received_at_ms=10005,
            update_kind=LocalL2UpdateKind.SNAPSHOT,
        )
        assert u.update_kind == LocalL2UpdateKind.SNAPSHOT
        assert u.sequence == 100
        assert u.previous_sequence == 99


class TestLocalL2Event:
    def test_defaults(self):
        e = LocalL2Event(venue="binance", symbol="BTCUSDT", event_kind=LocalL2EventKind.STALE)
        assert e.venue == "binance"
        assert e.bid == 0.0

    def test_with_prices(self):
        e = LocalL2Event(
            venue="binance", symbol="BTCUSDT",
            event_kind=LocalL2EventKind.BEST_BID_UPDATED,
            bid=50000, ask=50100, mid_price=50050,
            sequence=100, observed_at_ms=10000,
        )
        assert e.bid == 50000
        assert e.ask == 50100
        assert e.mid_price == 50050


class TestLocalL2UpdateResult:
    def test_defaults(self):
        r = LocalL2UpdateResult()
        assert not r.applied
        assert not r.rebuild_required
        assert r.fault_reason == ""
        assert r.events == []

    def test_success_with_events(self):
        r = LocalL2UpdateResult(
            applied=True,
            events=[LocalL2Event(venue="v", symbol="s", event_kind=LocalL2EventKind.STALE)],
        )
        assert r.applied
        assert len(r.events) == 1

    def test_rebuild_required(self):
        r = LocalL2UpdateResult(rebuild_required=True, fault_reason="sequence_gap_20")
        assert r.rebuild_required
        assert "sequence_gap" in r.fault_reason


# ---------------------------------------------------------------------------
# ExecutionLiquiditySnapshot from LocalL2Book
# ---------------------------------------------------------------------------


class TestExecutionLiquidityFromBook:
    def test_converts_ready_book_to_true_l2(self):
        from lightfee.marketdata.liquidity import execution_liquidity_from_local_l2
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.HOT, observed_at_ms=10000)
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0), PriceLevel(price=49900, quantity=2.0)],
            [PriceLevel(price=50100, quantity=1.5), PriceLevel(price=50200, quantity=0.5)],
            now_ms=10000,
        )
        snap = execution_liquidity_from_local_l2(book, max_age_ms=5000, now_ms=12000, require_ready=True)
        assert snap.source == "true_l2"
        assert snap.book_ready
        assert len(snap.bids) == 2
        assert len(snap.asks) == 2
        assert snap.bids[0].price == 50000

    def test_not_ready_book_returns_none_source(self):
        from lightfee.marketdata.liquidity import execution_liquidity_from_local_l2
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.COLD, observed_at_ms=10000)
        snap = execution_liquidity_from_local_l2(book, max_age_ms=5000, now_ms=12000, require_ready=True)
        assert snap.source == "none"
        assert not snap.book_ready
        assert "book_not_ready" in snap.fallback_reason

    def test_stale_book_returns_none_source(self):
        from lightfee.marketdata.liquidity import execution_liquidity_from_local_l2
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.HOT, observed_at_ms=10000)
        snap = execution_liquidity_from_local_l2(book, max_age_ms=5000, now_ms=16000, require_ready=True)
        assert snap.source == "none"
        assert not snap.book_ready

    def test_no_require_ready_allows_stale(self):
        from lightfee.marketdata.liquidity import execution_liquidity_from_local_l2
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.HOT, observed_at_ms=10000)
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.0)],
        )
        snap = execution_liquidity_from_local_l2(book, max_age_ms=5000, now_ms=16000, require_ready=False)
        assert snap.source == "true_l2"

    def test_buy_uses_asks_sell_uses_bids(self):
        from lightfee.marketdata.liquidity import execution_liquidity_from_local_l2
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.HOT, observed_at_ms=10000)
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=2.0), PriceLevel(price=49900, quantity=3.0)],
            [PriceLevel(price=50100, quantity=1.0), PriceLevel(price=50200, quantity=0.5)],
            now_ms=10000,
        )
        snap = execution_liquidity_from_local_l2(book, max_age_ms=5000, now_ms=12000)
        # buy VWAP should walk asks (ascending)
        filled, avg = snap.estimate_vwap_buy(target_quote=60000)
        assert filled > 0
        assert avg >= 50100
        # sell VWAP should walk bids (descending)
        filled, avg = snap.estimate_vwap_sell(target_quote=60000)
        assert filled > 0
        assert avg <= 50000

    def test_fallback_snapshot(self):
        from lightfee.marketdata.liquidity import execution_liquidity_fallback
        from lightfee.marketdata.l2 import ExecutionLiquiditySource
        snap = execution_liquidity_fallback(
            symbol="BTCUSDT", venue="binance",
            reason="local_l2_disabled", source=ExecutionLiquiditySource.TOP_BOOK,
        )
        assert snap.source == "top_book"
        assert snap.fallback_reason == "local_l2_disabled"
        assert not snap.book_ready

    def test_respects_max_depth(self):
        from lightfee.marketdata.liquidity import execution_liquidity_from_local_l2
        book = LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.HOT, observed_at_ms=10000)
        book.apply_snapshot(
            [PriceLevel(price=50000 - i * 100, quantity=1.0) for i in range(10)],
            [PriceLevel(price=50100 + i * 100, quantity=1.0) for i in range(10)],
            now_ms=10000,
        )
        snap = execution_liquidity_from_local_l2(book, max_depth=3, max_age_ms=5000, now_ms=12000)
        assert len(snap.bids) == 3
        assert len(snap.asks) == 3
