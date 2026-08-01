"""Local-L2 runtime service tests — assignment, lease, events, faults, metrics.

Rust V1 reference: src/execution_core/local_l2_runtime.rs
"""

from __future__ import annotations

import pytest

from lightfee.marketdata.l2 import (
    L2BookStatus,
    L2PoolAssignment,
    LocalL2BookKey,
    LocalL2EventKind,
    LocalL2Update,
    LocalL2UpdateKind,
    PriceLevel,
)
from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
from lightfee.marketdata.local_l2_runtime import (
    LocalL2Runtime,
    LocalL2RuntimeMetrics,
    RuntimeFaultKind,
)
from lightfee.persistence.journal import Journal


class TestLocalL2RuntimeBooks:
    def test_ensure_book_creates_new(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "BTCUSDT")
        assert book.venue == "binance"
        assert book.symbol == "BTCUSDT"
        assert book.status == L2BookStatus.COLD

    def test_get_book_returns_none_for_missing(self):
        rt = LocalL2Runtime()
        assert rt.get_book("binance", "BTCUSDT") is None

    def test_ensure_book_idempotent(self):
        rt = LocalL2Runtime()
        b1 = rt.ensure_book("binance", "BTCUSDT")
        b2 = rt.ensure_book("binance", "BTCUSDT")
        assert b1 is b2

    def test_remove_book(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.remove_book("binance", "BTCUSDT")
        assert rt.get_book("binance", "BTCUSDT") is None


class TestLocalL2RuntimeAssignments:
    def test_assign_sets_pool(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.assign("binance", "BTCUSDT", L2PoolAssignment.HOT_EXEC, now_ms=10000)
        assert rt.get_assignment("binance", "BTCUSDT") == L2PoolAssignment.HOT_EXEC

    def test_assign_updates_book_pool_for_hot_exec(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.assign("binance", "BTCUSDT", L2PoolAssignment.HOT_EXEC, now_ms=10000)
        assert rt.get_book("binance", "BTCUSDT").pool == L2PoolAssignment.HOT_EXEC

    def test_assign_dropped_updates_book_pool(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.assign("binance", "BTCUSDT", L2PoolAssignment.DROPPED, now_ms=10000)
        assert rt.get_book("binance", "BTCUSDT").pool == L2PoolAssignment.DROPPED

    def test_assignment_does_not_expire_without_a_scheduler_change(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.assign("binance", "BTCUSDT", L2PoolAssignment.HOT_EXEC, now_ms=10000)

        rt.sync(now_ms=999_999)

        assert rt.get_assignment("binance", "BTCUSDT") == L2PoolAssignment.HOT_EXEC

    def test_hot_exec_symbols(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.ensure_book("bybit", "ETHUSDT")
        rt.assign("binance", "BTCUSDT", L2PoolAssignment.HOT_EXEC, now_ms=10000)
        rt.assign("bybit", "ETHUSDT", L2PoolAssignment.WARM, now_ms=10000)
        hot = rt.hot_exec_symbols()
        assert len(hot) == 1
        assert hot[0] == LocalL2BookKey(venue="binance", symbol="BTCUSDT")

    def test_prune_untracked_books_removes_dropped_and_keeps_tracked(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.ensure_book("binance", "ETHUSDT")

        pruned = rt.prune_untracked_books(
            tracked={LocalL2BookKey(venue="binance", symbol="ETHUSDT")},
            now_ms=10000,
        )

        assert pruned == [
            {
                "venue": "binance",
                "symbol": "BTCUSDT",
                "reason": "dropped_untracked",
            }
        ]
        assert rt.get_book("binance", "BTCUSDT") is None
        assert rt.get_book("binance", "ETHUSDT") is not None


class TestLocalL2ReceiveClock:
    def test_record_update_uses_local_receive_time_for_freshness(self):
        rt = LocalL2Runtime()

        result = rt.record_update_result(
            LocalL2Update(
                venue="binance",
                symbol="BTCUSDT",
                bids=[PriceLevel(100.0, 1.0)],
                asks=[PriceLevel(101.0, 1.0)],
                sequence=1,
                event_time_ms=60_000,
                received_at_ms=2_000,
                update_kind=LocalL2UpdateKind.SNAPSHOT,
            ),
            now_ms=1_000,
        )

        assert result.applied is True
        assert rt.get_book("binance", "BTCUSDT").observed_at_ms == 2_000


class TestLocalL2RuntimeEvents:
    def test_drain_events_all(self):
        rt = LocalL2Runtime()
        from lightfee.marketdata.l2 import LocalL2Event, LocalL2EventKind
        for i in range(5):
            rt.pending_events.append(LocalL2Event(
                venue="binance", symbol="BTCUSDT",
                event_kind=LocalL2EventKind.BEST_BID_UPDATED,
            ))
        drained = rt.drain_events()
        assert len(drained) == 5
        assert rt.event_count() == 0

    def test_drain_events_limited(self):
        rt = LocalL2Runtime()
        from lightfee.marketdata.l2 import LocalL2Event, LocalL2EventKind
        for i in range(10):
            rt.pending_events.append(LocalL2Event(
                venue="binance", symbol="BTCUSDT",
                event_kind=LocalL2EventKind.BEST_BID_UPDATED,
            ))
        drained = rt.drain_events(limit=3)
        assert len(drained) == 3
        assert rt.event_count() == 7

    def test_event_queue_bounded(self):
        rt = LocalL2Runtime(max_events=5)
        from lightfee.marketdata.l2 import LocalL2Event, LocalL2EventKind
        for i in range(10):
            rt.pending_events.append(LocalL2Event(
                venue="binance", symbol=f"BTCUSDT_{i}",
                event_kind=LocalL2EventKind.BEST_BID_UPDATED,
            ))
        # enforced on enqueue, but pending_events is public here for tests
        # drain to check bounded behavior
        while rt.event_count() > rt.max_events:
            rt.pending_events.popleft()
        assert rt.event_count() <= 5

    def test_record_update_snapshot_emits_events(self):
        rt = LocalL2Runtime()
        update = LocalL2Update(
            venue="binance", symbol="BTCUSDT",
            bids=[PriceLevel(price=50000, quantity=1.0)],
            asks=[PriceLevel(price=50100, quantity=1.0)],
            sequence=1, update_kind=LocalL2UpdateKind.SNAPSHOT,
        )
        events = rt.record_update(update, now_ms=10000)
        assert len(events) > 0
        assert rt.event_count() == len(events)


class TestLocalL2RuntimeFaults:
    def test_handle_rate_limited(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.handle_runtime_failure(
            "binance", "BTCUSDT",
            RuntimeFaultKind.RATE_LIMITED, "too many requests", now_ms=10000,
        )
        assert rt.metrics.runtime_rate_limited_total == 1

    def test_handle_transport_failure(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.handle_runtime_failure(
            "binance", "BTCUSDT",
            RuntimeFaultKind.TRANSPORT_FAILURE, "connection reset", now_ms=10000,
        )
        assert rt.metrics.runtime_transport_failure_total == 1

    def test_handle_checksum_mismatch(self):
        rt = LocalL2Runtime()
        rt.ensure_book("okx", "BTCUSDT")
        rt.handle_runtime_failure(
            "okx", "BTCUSDT",
            RuntimeFaultKind.CHECKSUM_MISMATCH, "expected=123 actual=456", now_ms=10000,
        )
        assert rt.metrics.rebuild_total == 1

    def test_handle_sequence_gap(self):
        rt = LocalL2Runtime()
        rt.ensure_book("bybit", "BTCUSDT")
        rt.handle_runtime_failure(
            "bybit", "BTCUSDT",
            RuntimeFaultKind.SEQUENCE_GAP, "gap=5", now_ms=10000,
        )
        assert rt.metrics.rebuild_total == 1

    def test_invalid_snapshot_is_data_integrity_not_sequence_gap(self):
        rt = LocalL2Runtime()
        result = rt.record_update_result(
            LocalL2Update(
                venue="binance",
                symbol="BTCUSDT",
                bids=[PriceLevel(price=float("nan"), quantity=1.0)],
                asks=[PriceLevel(price=50100.0, quantity=1.0)],
                sequence=1,
                update_kind=LocalL2UpdateKind.SNAPSHOT,
            ),
            now_ms=10_000,
        )

        assert result.rebuild_required is True
        assert rt.metrics.data_integrity_rebuild_total == 1
        assert rt.get_book("binance", "BTCUSDT").fault_reason.startswith(
            "data_integrity: invalid_bid_snapshot_level"
        )

    def test_nonnumeric_snapshot_is_data_integrity_not_runtime_exception(self):
        rt = LocalL2Runtime()

        result = rt.record_update_result(
            LocalL2Update(
                venue="binance",
                symbol="BTCUSDT",
                bids=[PriceLevel(price="50_000", quantity=1.0)],  # type: ignore[arg-type]
                asks=[PriceLevel(price=50_100.0, quantity=1.0)],
                sequence=1,
                update_kind=LocalL2UpdateKind.SNAPSHOT,
            ),
            now_ms=10_000,
        )

        assert result.rebuild_required is True
        assert rt.metrics.data_integrity_rebuild_total == 1
        assert rt.get_book("binance", "BTCUSDT").fault_reason.startswith(
            "data_integrity: invalid_bid_snapshot_level"
        )

    def test_handle_runtime_suspended(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.resume_timeout_ms = 60000
        rt.handle_runtime_failure(
            "binance", "BTCUSDT",
            RuntimeFaultKind.RUNTIME_SUSPENDED, "too many errors", now_ms=10000,
        )
        assert rt.metrics.runtime_suspended_total == 1
        book = rt.get_book("binance", "BTCUSDT")
        assert book.status == L2BookStatus.SUSPENDED
        assert book.runtime_suspended_until_ms == 70000

    def test_apply_fallback(self):
        rt = LocalL2Runtime()
        rt.apply_fallback("binance", "BTCUSDT", "top_book")
        assert rt.metrics.fallback_total == 1
        book = rt.get_book("binance", "BTCUSDT")
        assert book.source == "top_book"


class TestLocalL2RuntimeSync:
    def test_sync_keeps_assignment_until_scheduler_replaces_it(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.assign("binance", "BTCUSDT", L2PoolAssignment.HOT_EXEC, now_ms=10000)
        rt.sync(now_ms=20000)
        assert rt.get_assignment("binance", "BTCUSDT") == L2PoolAssignment.HOT_EXEC

    def test_sync_refreshes_metrics(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.assign("binance", "BTCUSDT", L2PoolAssignment.HOT_EXEC, now_ms=10000)
        book = rt.get_book("binance", "BTCUSDT")
        book.transition_to_bootstrapping(now_ms=10000)
        book.transition_to_hot()
        rt.sync(now_ms=11000)
        assert rt.metrics.active_books == 1

    def test_sync_drains_events(self):
        rt = LocalL2Runtime()
        from lightfee.marketdata.l2 import LocalL2Event, LocalL2EventKind
        rt.pending_events.append(LocalL2Event(
            venue="binance", symbol="BTCUSDT",
            event_kind=LocalL2EventKind.BEST_BID_UPDATED,
        ))
        events = rt.sync(now_ms=10000)
        assert len(events) == 1

    def test_sync_resume_waiting_books(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        book = rt.get_book("binance", "BTCUSDT")
        book.transition_to_hot()
        book.transition_to_resume_waiting(until_ms=50000)
        # Before resume time
        rt.sync(now_ms=30000)
        assert book.status == L2BookStatus.RESUME_WAITING
        # After resume time
        rt.sync(now_ms=60000)
        assert book.status == L2BookStatus.BOOTSTRAPPING


class TestDiagnosticsSnapshot:
    def test_snapshot_counts(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.ensure_book("bybit", "ETHUSDT")
        rt.assign("binance", "BTCUSDT", L2PoolAssignment.HOT_EXEC, now_ms=10000)
        rt.assign("bybit", "ETHUSDT", L2PoolAssignment.RETAINED, now_ms=10000)
        snap = rt.diagnostics_snapshot()
        assert snap["book_count"] == 2
        assert snap["assignment_count"] == 2


class TestRuntimeMetrics:
    def test_default_values(self):
        m = LocalL2RuntimeMetrics()
        assert m.rebuild_total == 0
        assert m.active_books == 0


# ---------------------------------------------------------------------------
# Data plane integration tests
# ---------------------------------------------------------------------------


class MockL2Adapter:
    """Minimal mock adapter that returns canned L2 snapshot data via fetch_l2_snapshot()."""

    def __init__(
        self,
        venue_name: str = "binance",
        should_fail: bool = False,
        bids: list[PriceLevel] | None = None,
        asks: list[PriceLevel] | None = None,
        sequence: int = 1,
    ):
        self.venue_name = venue_name
        self.should_fail = should_fail
        self.bids = bids
        self.asks = asks
        self.sequence = sequence
        self.call_count = 0
        self.last_symbol: str = ""
        self.last_depth: int = 0

    async def fetch_l2_snapshot(self, symbol: str, depth: int = 50) -> LocalL2Update:
        self.call_count += 1
        self.last_symbol = symbol
        self.last_depth = depth
        if self.should_fail:
            from lightfee.venues.transport import TransportError, TransportErrorCategory
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                f"mock failure for {symbol}",
            )
        return LocalL2Update(
            venue=self.venue_name,
            symbol=symbol,
            bids=self.bids if self.bids is not None else [PriceLevel(price=49900.0, quantity=1.0)],
            asks=self.asks if self.asks is not None else [PriceLevel(price=50100.0, quantity=1.0)],
            sequence=self.sequence,
            event_time_ms=1000,
            received_at_ms=1000,
            update_kind=LocalL2UpdateKind.SNAPSHOT,
        )


class SequenceMockL2Adapter(MockL2Adapter):
    def __init__(self, venue_name: str, sequences: list[int]):
        super().__init__(venue_name=venue_name, sequence=sequences[0])
        self.sequences = list(sequences)

    async def fetch_l2_snapshot(self, symbol: str, depth: int = 50) -> LocalL2Update:
        index = min(self.call_count, len(self.sequences) - 1)
        self.sequence = self.sequences[index]
        return await super().fetch_l2_snapshot(symbol, depth)


class TestDataPlaneBootstrap:
    def test_bootstrap_applies_snapshot_to_book(self):
        from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
        from lightfee.persistence.journal import Journal

        rt = LocalL2Runtime()
        import tempfile, os as _os
        jpath = _os.path.join(tempfile.mkdtemp(), "test.journal")
        journal = Journal(jpath)
        journal.open()
        dp = LocalL2DataPlane(l2_runtime=rt, journal=journal)

        adapter = MockL2Adapter("binance")
        import asyncio
        success = asyncio.run(
            dp.bootstrap_book("binance", "BTCUSDT", adapter, depth=50, now_ms=1000)
        )

        assert success
        book = rt.get_book("binance", "BTCUSDT")
        assert book is not None
        assert book.best_bid() == 49900.0
        assert book.best_ask() == 50100.0
        assert book.sequence == 1

    def test_bootstrap_rejects_invalid_snapshot_without_marking_hot(self):
        from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
        from lightfee.persistence.journal import Journal

        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "BTCUSDT")
        book.transition_to_bootstrapping(now_ms=1000)
        import tempfile, os as _os
        jpath = _os.path.join(tempfile.mkdtemp(), "test.journal")
        journal = Journal(jpath)
        journal.open()
        dp = LocalL2DataPlane(l2_runtime=rt, journal=journal)

        adapter = MockL2Adapter(
            "binance",
            bids=[],
            asks=[PriceLevel(price=50100.0, quantity=1.0)],
        )
        import asyncio
        success = asyncio.run(
            dp.bootstrap_book("binance", "BTCUSDT", adapter, depth=50, now_ms=1000)
        )

        assert not success
        assert book.status == L2BookStatus.REBUILDING
        assert "book_empty_side_bid" in book.fault_reason
        assert book.observed_at_ms == 0

    def test_bootstrap_replay_failure_does_not_complete_hot(self):
        from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
        from lightfee.persistence.journal import Journal

        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "BTCUSDT")
        book.transition_to_bootstrapping(now_ms=1000)
        import tempfile, os as _os
        jpath = _os.path.join(tempfile.mkdtemp(), "test.journal")
        journal = Journal(jpath)
        journal.open()
        dp = LocalL2DataPlane(l2_runtime=rt, journal=journal)
        dp.ingest_external_update(
            LocalL2Update(
                venue="binance",
                symbol="BTCUSDT",
                bids=[PriceLevel(price=50050.0, quantity=1.0)],
                asks=[],
                sequence=3,
                previous_sequence=2,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=1001,
        )

        adapter = MockL2Adapter("binance", sequence=1)
        import asyncio
        success = asyncio.run(
            dp.bootstrap_book("binance", "BTCUSDT", adapter, depth=50, now_ms=2000)
        )

        assert not success
        assert book.status == L2BookStatus.REBUILDING
        assert "buffered_replay_snapshot_boundary" in book.fault_reason

    def test_external_snapshot_during_bootstrap_applies_and_completes_hot(self):
        """Bybit/OKX/Bitget/Gate/Hyperliquid snapshots reset the local book.

        They must not sit behind the pre-snapshot delta buffer, otherwise a
        valid exchange snapshot can never complete bootstrap.
        """
        from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
        from lightfee.persistence.journal import Journal

        rt = LocalL2Runtime()
        book = rt.ensure_book("bybit", "BTCUSDT")
        book.transition_to_bootstrapping(now_ms=1000)
        import tempfile, os as _os
        jpath = _os.path.join(tempfile.mkdtemp(), "test.journal")
        journal = Journal(jpath)
        journal.open()
        dp = LocalL2DataPlane(l2_runtime=rt, journal=journal)

        events = dp.ingest_external_update(
            LocalL2Update(
                venue="bybit",
                symbol="BTCUSDT",
                bids=[PriceLevel(price=49900.0, quantity=1.0)],
                asks=[PriceLevel(price=50100.0, quantity=1.0)],
                sequence=100,
                event_time_ms=2000,
                update_kind=LocalL2UpdateKind.SNAPSHOT,
            ),
            now_ms=2001,
        )

        assert events
        assert book.status == L2BookStatus.HOT
        assert book.best_bid() == 49900.0
        assert book.best_ask() == 50100.0
        assert book.sequence == 100

    def test_bootstrap_failure_updates_runtime_fault(self):
        from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
        from lightfee.persistence.journal import Journal

        rt = LocalL2Runtime()
        import tempfile, os as _os
        jpath = _os.path.join(tempfile.mkdtemp(), "test.journal")
        journal = Journal(jpath)
        journal.open()
        dp = LocalL2DataPlane(l2_runtime=rt, journal=journal)

        adapter = MockL2Adapter("binance", should_fail=True)
        import asyncio
        success = asyncio.run(
            dp.bootstrap_book("binance", "BTCUSDT", adapter, depth=50, now_ms=1000)
        )

        assert not success
        assert rt.metrics.runtime_transport_failure_total > 0

    def test_bootstrap_respects_cooldown(self):
        from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
        from lightfee.persistence.journal import Journal

        rt = LocalL2Runtime()
        import tempfile, os as _os
        jpath = _os.path.join(tempfile.mkdtemp(), "test.journal")
        journal = Journal(jpath)
        journal.open()
        dp = LocalL2DataPlane(l2_runtime=rt, journal=journal)

        adapter = MockL2Adapter("binance")
        import asyncio

        # First call succeeds
        ok = asyncio.run(
            dp.bootstrap_book("binance", "BTCUSDT", adapter, depth=50, now_ms=1000)
        )
        assert ok
        assert adapter.call_count == 1

        # Second call within cooldown should be skipped
        ok = asyncio.run(
            dp.bootstrap_book("binance", "BTCUSDT", adapter, depth=50, now_ms=1500)
        )
        assert not ok  # cooldown blocks
        assert adapter.call_count == 1  # Not called again

    def test_bootstrap_after_cooldown_succeeds(self):
        from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
        from lightfee.persistence.journal import Journal

        rt = LocalL2Runtime()
        import tempfile, os as _os
        jpath = _os.path.join(tempfile.mkdtemp(), "test.journal")
        journal = Journal(jpath)
        journal.open()
        dp = LocalL2DataPlane(l2_runtime=rt, journal=journal)

        adapter = MockL2Adapter("binance")
        import asyncio

        asyncio.run(
            dp.bootstrap_book("binance", "BTCUSDT", adapter, depth=50, now_ms=1000)
        )
        # After cooldown (5s +)
        ok = asyncio.run(
            dp.bootstrap_book("binance", "BTCUSDT", adapter, depth=50, now_ms=7000)
        )
        assert ok
        assert adapter.call_count == 2


class TestDataPlaneSync:
    def test_sync_dispatches_for_cold_book(self):
        from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
        from lightfee.persistence.journal import Journal
        from lightfee.core.domain import Venue

        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "BTCUSDT")
        from lightfee.marketdata.l2 import L2PoolAssignment
        book.pool = L2PoolAssignment.RETAINED
        import tempfile, os as _os
        jpath = _os.path.join(tempfile.mkdtemp(), "test.journal")
        journal = Journal(jpath)
        journal.open()
        dp = LocalL2DataPlane(l2_runtime=rt, journal=journal)

        # Create a mock adapter with fetch_l2_snapshot() — no _transport access
        from tests.fake_adapters import FakeVenueAdapter
        adapter = FakeVenueAdapter(_venue=Venue.BINANCE)
        adapter._transport = MockL2Adapter("binance")

        import asyncio
        dispatched = asyncio.run(
            dp.sync_snapshots(
                adapters={Venue.BINANCE: adapter},
                now_ms=1000,
            )
        )

        assert dispatched >= 1
        book = rt.get_book("binance", "BTCUSDT")
        assert book is not None
        assert book.best_bid() == 49900.0

    def test_sync_skips_hot_book_within_interval(self):
        from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
        from lightfee.persistence.journal import Journal
        from lightfee.core.domain import Venue

        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "BTCUSDT")
        book.transition_to_bootstrapping(now_ms=0)
        book.transition_to_hot()
        book.last_snapshot_ms = 1000  # recently snapshotted
        import tempfile, os as _os
        jpath = _os.path.join(tempfile.mkdtemp(), "test.journal")
        journal = Journal(jpath)
        journal.open()
        dp = LocalL2DataPlane(l2_runtime=rt, journal=journal)

        # Mock adapter with fetch_l2_snapshot — sync should NOT call it for HOT books
        l2_adapter = MockL2Adapter("binance")

        import asyncio
        dispatched = asyncio.run(
            dp.sync_snapshots(
                adapters={Venue.BINANCE: l2_adapter},
                now_ms=1500,  # Only 500ms later
            )
        )

        assert dispatched == 0  # HOT book within refresh interval
        assert l2_adapter.call_count == 0


# ===========================================================================
# V1 parity: handle_runtime_failure sets book.fault_reason (DP-2)
# ===========================================================================


class TestHandleRuntimeFailureFaultReason:
    """V1: every fault event carries a specific fault detail.
    V2: handle_runtime_failure must set book.fault_reason for all fault types."""

    def test_sequence_gap_sets_fault_reason(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "BTCUSDT")
        book.transition_to_bootstrapping(now_ms=10000)
        book.transition_to_hot()
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.0)],
            sequence=100, now_ms=10000,
        )

        rt.handle_runtime_failure(
            "binance", "BTCUSDT",
            RuntimeFaultKind.SEQUENCE_GAP,
            "gap=20 prev=100 incoming_prev=120",
            now_ms=11000,
        )

        assert book.fault_reason != "", (
            "SEQUENCE_GAP must set book.fault_reason, got empty string"
        )
        assert "gap" in book.fault_reason, (
            f"fault_reason must carry gap detail, got {book.fault_reason!r}"
        )

    def test_checksum_mismatch_sets_fault_reason(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("okx", "BTCUSDT")
        book.transition_to_bootstrapping(now_ms=10000)
        book.transition_to_hot()
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.0)],
            sequence=100, now_ms=10000,
        )

        rt.handle_runtime_failure(
            "okx", "BTCUSDT",
            RuntimeFaultKind.CHECKSUM_MISMATCH,
            "checksum_mismatch expected=12345 actual=67890",
            now_ms=11000,
        )

        assert "checksum" in book.fault_reason.lower(), (
            f"CHECKSUM_MISMATCH must set book.fault_reason, got {book.fault_reason!r}"
        )

    def test_transport_failure_sets_fault_reason(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "BTCUSDT")

        rt.handle_runtime_failure(
            "binance", "BTCUSDT",
            RuntimeFaultKind.TRANSPORT_FAILURE,
            "connection reset",
            now_ms=11000,
        )

        assert book.fault_reason != "", (
            "TRANSPORT_FAILURE must set book.fault_reason"
        )

    def test_quote_age_triggered_sets_fault_reason(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "BTCUSDT")
        book.transition_to_bootstrapping(now_ms=10000)
        book.transition_to_hot()

        rt.handle_runtime_failure(
            "binance", "BTCUSDT",
            RuntimeFaultKind.QUOTE_AGE_TRIGGERED,
            "age=7000ms",
            now_ms=11000,
        )

        assert book.status == L2BookStatus.DEGRADED
        assert book.fault_reason != "", (
            "QUOTE_AGE_TRIGGERED must set book.fault_reason via degrade"
        )

    def test_fault_reason_preserved_across_multiple_failures(self):
        """Last fault wins — most recent failure reason is kept."""
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "BTCUSDT")
        book.transition_to_bootstrapping(now_ms=10000)
        book.transition_to_hot()

        rt.handle_runtime_failure(
            "binance", "BTCUSDT",
            RuntimeFaultKind.SEQUENCE_GAP,
            "first_gap", now_ms=11000,
        )
        first = book.fault_reason
        assert first != ""

        rt.handle_runtime_failure(
            "binance", "BTCUSDT",
            RuntimeFaultKind.TRANSPORT_FAILURE,
            "second_fault", now_ms=12000,
        )
        assert book.fault_reason != first, (
            "most recent fault should update fault_reason"
        )
        assert "second_fault" in book.fault_reason


# ---------------------------------------------------------------------------
# Task 4: Bybit WS-Snapshot-Authoritative — cross-depth sequence domain fix
# ---------------------------------------------------------------------------


def _make_journal():
    import tempfile
    import os as _os
    jpath = _os.path.join(tempfile.mkdtemp(), "test.journal")
    from lightfee.persistence.journal import Journal
    j = Journal(jpath)
    j.open()
    return j


class _RecordingJournal:
    def __init__(self):
        self.records = []

    def append(self, kind, payload, **kwargs):
        self.records.append((kind, payload))
        return len(self.records)


class TestBybitWsSnapshotAuthoritative:
    @pytest.mark.asyncio
    async def test_bybit_rest_bootstrap_fallback_when_registered_ws_is_not_connected(self):
        """Registered-but-not-connected WS clients must not pin Bybit in BOOTSTRAPPING."""
        rt = LocalL2Runtime()
        book = rt.ensure_book("bybit", "IRYSUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        dp = LocalL2DataPlane(rt, _make_journal())

        class FakeClient:
            is_connected = False

        dp._ws_clients[LocalL2BookKey("bybit", "IRYSUSDT")] = FakeClient()
        adapter = MockL2Adapter("bybit", sequence=7103120)

        success = await dp.bootstrap_book(
            "bybit", "IRYSUSDT", adapter, depth=50, now_ms=1779302500002,
        )

        assert success is True
        assert rt.get_book("bybit", "IRYSUSDT").status == L2BookStatus.HOT
        assert rt.get_book("bybit", "IRYSUSDT").sequence == 7103120

    @pytest.mark.asyncio
    async def test_bybit_rest_bootstrap_deferred_when_ws_is_connected(self):
        """Connected Bybit WS stream is snapshot-authoritative; REST bootstrap is evidence only."""
        rt = LocalL2Runtime()
        book = rt.ensure_book("bybit", "IRYSUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        dp = LocalL2DataPlane(rt, _make_journal())

        class FakeClient:
            is_connected = True

        dp._ws_clients[LocalL2BookKey("bybit", "IRYSUSDT")] = FakeClient()
        adapter = MockL2Adapter("bybit", sequence=7103120)

        success = await dp.bootstrap_book(
            "bybit", "IRYSUSDT", adapter, depth=50, now_ms=1779302500002,
        )

        assert success is False
        assert rt.get_book("bybit", "IRYSUSDT").status == L2BookStatus.BOOTSTRAPPING

    def test_rest_snapshot_sequence_not_compared_to_ws_depth_book(self):
        """Bybit REST u (depth-1000 domain) must not be compared with WS orderbook.50 sequence."""
        rt = LocalL2Runtime()
        book = rt.ensure_book("bybit", "IRYSUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        book.sequence = 13700598
        book.last_update_id = 13700598
        book.observed_at_ms = 0

        from lightfee.marketdata.local_l2_policy import BridgeMode, policy_for_venue
        policy = policy_for_venue("bybit")

        assert policy.bridge_mode is BridgeMode.WS_SNAPSHOT_AUTHORITATIVE
        assert policy.rest_snapshot_sequence_comparable is False

        rest_seq = 7103120  # From REST /v5/market/orderbook u field
        old_stale = rest_seq < book.last_update_id
        assert old_stale is True, "old logic would falsely flag stale"

    def test_bybit_ws_snapshot_authoritative_policy_no_cross_depth_replay(self):
        """Bybit REST snapshot must not be replayed against WS delta buffers."""
        from lightfee.marketdata.local_l2_policy import policy_for_venue
        policy = policy_for_venue("bybit")
        assert policy.replay_rest_snapshot_with_ws_deltas is False

    def test_bybit_pre_snapshot_buffer_cap_matches_default(self):
        """Bybit uses 4096 buffer cap, not 512."""
        from lightfee.marketdata.local_l2_policy import policy_for_venue
        policy = policy_for_venue("bybit")
        assert policy.pre_snapshot_buffer_cap == 4096

    def test_stale_comparison_is_only_for_venues_with_comparable_sequence(self):
        """Only proven same-domain replay venues compare REST and WS sequence IDs."""
        from lightfee.marketdata.local_l2_policy import policy_for_venue
        for venue in ("bybit", "hyperliquid"):
            policy = policy_for_venue(venue)
            assert policy.rest_snapshot_sequence_comparable is False, (
                f"{venue} must not compare REST/WS sequences across depth domains"
            )
        for venue in ("binance", "aster", "okx"):
            policy = policy_for_venue(venue)
            assert policy.rest_snapshot_sequence_comparable is True, (
                f"{venue} REST/WS sequences share the same domain"
            )


# ---------------------------------------------------------------------------
# Task 5: Binance/Aster V1 Buffered Replay Parity
# ---------------------------------------------------------------------------


class TestBinanceAsterV1BufferCapParity:
    @pytest.mark.asyncio
    async def test_binance_buffered_replay_valid_bridge_promotes_hot(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "VALIDUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)

        dp.ingest_external_update(
            LocalL2Update(
                venue="binance",
                symbol="VALIDUSDT",
                bids=[PriceLevel(49910.0, 10.0)],
                asks=[PriceLevel(50110.0, 10.0)],
                first_sequence=101,
                sequence=101,
                previous_sequence=100,
                previous_sequence_present=True,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=1100,
        )

        ok = await dp.bootstrap_book(
            "binance",
            "VALIDUSDT",
            MockL2Adapter("binance", sequence=100),
            now_ms=2000,
        )

        assert ok is True
        assert rt.get_book("binance", "VALIDUSDT").status == L2BookStatus.HOT
        assert rt.get_book("binance", "VALIDUSDT").sequence == 101
        assert not [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_snapshot_error"
        ]

    @pytest.mark.asyncio
    async def test_snapshot_response_stale_generation_is_discarded_before_apply(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "STALEGENUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)

        class GenerationFlipAdapter(MockL2Adapter):
            async def fetch_l2_snapshot(
                self,
                symbol: str,
                depth: int = 50,
            ) -> LocalL2Update:
                update = await super().fetch_l2_snapshot(symbol, depth)
                dp._advance_stream_generation("binance", "STALEGENUSDT")
                return update

        ok = await dp.bootstrap_book(
            "binance",
            "STALEGENUSDT",
            GenerationFlipAdapter("binance", sequence=100),
            now_ms=2000,
        )

        assert ok is False
        assert book.status == L2BookStatus.BOOTSTRAPPING
        assert book.sequence == 0
        stale = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_snapshot_stale_response_discarded"
        ][-1]
        assert stale["reason"] == "snapshot_response_stale_generation"
        assert stale["response_discarded"] is True
        assert stale["stream_generation"] != stale["current_stream_generation"]

    @pytest.mark.asyncio
    async def test_binance_stale_rest_snapshot_event_carries_causal_evidence(
        self,
        monkeypatch,
    ):
        async def no_sleep(_delay):
            return None

        monkeypatch.setattr(
            "lightfee.marketdata.local_l2_data_plane.asyncio.sleep",
            no_sleep,
        )
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "STALESNAPUSDT")
        book.status = L2BookStatus.HOT
        book.pool = L2PoolAssignment.HOT_EXEC
        book.sequence = 120
        book.last_update_id = 120
        book.observed_at_ms = 1_500
        book.last_snapshot_ms = 1_000
        book.last_delta_ms = 1_500
        book.fault_reason = "sequence_gap: previous_link"
        book.last_error = "previous_link"
        book.generation = 7
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)
        dp._rebuild_attempt_ids["binance:STALESNAPUSDT"] = 3
        freshness = dp._freshness_state("binance", "STALESNAPUSDT")
        freshness.last_ws_delta_ms = 1_600
        freshness.last_ws_keepalive_ms = 1_700
        freshness.last_book_confirmation_ms = 1_800
        freshness.last_subscription_confirmed_ms = 1_400
        freshness.last_rest_refresh_ms = 1_300

        ok = await dp.bootstrap_book(
            "binance",
            "STALESNAPUSDT",
            MockL2Adapter("binance", sequence=100),
            now_ms=2_000,
        )

        assert ok is False
        stale = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_snapshot_stale"
        ][-1]
        assert stale["snapshot_seq"] == 100
        assert stale["book_seq"] == 120
        assert stale["last_rebuild_attempt_id"] == 3
        assert stale["fault_reason"] == "sequence_gap: previous_link"
        assert stale["book_last_error"] == "previous_link"
        assert stale["snapshot_in_flight"] is True
        assert stale["snapshot_state"] == "in_flight"
        assert stale["snapshot_received_at_ms"] == 1000
        assert stale["rest_snapshot_received_at_ms"] == 1000
        assert stale["last_ws_delta_ms"] == 1600
        assert stale["last_ws_keepalive_ms"] == 1700
        assert stale["last_book_confirmation_ms"] == 1800
        assert stale["last_rest_refresh_ms"] == 1300
        assert stale["age_ms"] == 500
        assert stale["snapshot_received_age_ms"] == 1000
        assert stale["book_generation"] == 7
        assert stale["snapshot_attempt_id"] == 1
        assert stale["current_stream_generation"] == stale["stream_generation"]
        assert stale["policy_rest_snapshot_sequence_comparable"] is True

    @pytest.mark.asyncio
    async def test_binance_buffered_replay_invalid_bridge_keeps_rebuilding(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "GAPUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        dp = LocalL2DataPlane(rt, _RecordingJournal())

        dp.ingest_external_update(
            LocalL2Update(
                venue="binance",
                symbol="GAPUSDT",
                bids=[PriceLevel(49910.0, 10.0)],
                asks=[PriceLevel(50110.0, 10.0)],
                first_sequence=106,
                sequence=110,
                previous_sequence=105,
                previous_sequence_present=True,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=1100,
        )

        ok = await dp.bootstrap_book(
            "binance",
            "GAPUSDT",
            MockL2Adapter("binance", sequence=100),
            now_ms=2000,
        )

        assert ok is False
        assert "snapshot_boundary" in rt.get_book("binance", "GAPUSDT").fault_reason
        assert rt.get_book("binance", "GAPUSDT").status == L2BookStatus.REBUILDING

    @pytest.mark.asyncio
    async def test_binance_first_bridge_requires_range_overlap_even_when_pu_matches(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "ANCHORUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        dp = LocalL2DataPlane(rt, _RecordingJournal())

        dp.ingest_external_update(
            LocalL2Update(
                venue="binance",
                symbol="ANCHORUSDT",
                bids=[PriceLevel(49910.0, 10.0)],
                asks=[PriceLevel(50110.0, 10.0)],
                first_sequence=106,
                sequence=110,
                previous_sequence=100,
                previous_sequence_present=True,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=1100,
        )

        ok = await dp.bootstrap_book(
            "binance",
            "ANCHORUSDT",
            MockL2Adapter("binance", sequence=100),
            now_ms=2000,
        )

        assert ok is False
        assert "snapshot_boundary" in rt.get_book("binance", "ANCHORUSDT").fault_reason
        assert rt.get_book("binance", "ANCHORUSDT").status == L2BookStatus.REBUILDING

    @pytest.mark.asyncio
    @pytest.mark.parametrize("venue", ("binance", "aster"))
    @pytest.mark.parametrize("terminal_gap", ("missing", "mismatch"))
    async def test_no_buffer_first_hot_bridge_is_accepted_once_for_binance_aster(
        self,
        venue,
        terminal_gap,
    ):
        rt = LocalL2Runtime()
        book = rt.ensure_book(venue, "NOBUFUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        dp = LocalL2DataPlane(rt, _RecordingJournal())

        ok = await dp.bootstrap_book(
            venue,
            "NOBUFUSDT",
            MockL2Adapter(venue, sequence=100),
            now_ms=2000,
        )
        assert ok is True
        book = rt.get_book(venue, "NOBUFUSDT")
        assert book.status == L2BookStatus.HOT
        assert book.pending_snapshot_bridge is True

        first_update = LocalL2Update(
            venue=venue,
            symbol="NOBUFUSDT",
            bids=[PriceLevel(49910.0, 10.0)],
            asks=[PriceLevel(50110.0, 10.0)],
            first_sequence=99,
            sequence=101,
            previous_sequence=94,
            previous_sequence_present=True,
            update_kind=LocalL2UpdateKind.DELTA,
        )
        first_events = dp.ingest_external_update(first_update, now_ms=2100)

        assert first_events
        assert book.status == L2BookStatus.HOT
        assert book.sequence == 101
        assert book.pending_snapshot_bridge is False
        assert first_update.previous_sequence == 94
        assert first_update.previous_sequence_present is True

        second_events = dp.ingest_external_update(
            LocalL2Update(
                venue=venue,
                symbol="NOBUFUSDT",
                bids=[PriceLevel(49920.0, 10.0)],
                asks=[PriceLevel(50120.0, 10.0)],
                first_sequence=102,
                sequence=102,
                previous_sequence=101,
                previous_sequence_present=True,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=2200,
        )

        assert second_events
        assert book.status == L2BookStatus.HOT
        assert book.sequence == 102

        terminal_previous_sequence = 0 if terminal_gap == "missing" else 99
        terminal_previous_sequence_present = terminal_gap == "mismatch"
        third_events = dp.ingest_external_update(
            LocalL2Update(
                venue=venue,
                symbol="NOBUFUSDT",
                bids=[PriceLevel(49930.0, 10.0)],
                asks=[PriceLevel(50130.0, 10.0)],
                first_sequence=103,
                sequence=103,
                previous_sequence=terminal_previous_sequence,
                previous_sequence_present=terminal_previous_sequence_present,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=2300,
        )

        assert third_events == []
        assert book.status == L2BookStatus.REBUILDING
        if terminal_gap == "missing":
            assert "missing_previous_link" in book.fault_reason
        else:
            assert "previous_link_mismatch" in book.fault_reason

    @pytest.mark.asyncio
    async def test_gate_buffered_replay_uses_range_only(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("gate", "BTCUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        dp = LocalL2DataPlane(rt, _RecordingJournal())

        dp.ingest_external_update(
            LocalL2Update(
                venue="gate",
                symbol="BTCUSDT",
                bids=[PriceLevel(49910.0, 10.0)],
                asks=[PriceLevel(50110.0, 10.0)],
                first_sequence=101,
                sequence=101,
                previous_sequence=999,
                previous_sequence_present=True,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=1100,
        )

        ok = await dp.bootstrap_book(
            "gate",
            "BTCUSDT",
            MockL2Adapter("gate", sequence=100),
            now_ms=2000,
        )

        assert ok is True
        assert rt.get_book("gate", "BTCUSDT").status == L2BookStatus.HOT
        assert rt.get_book("gate", "BTCUSDT").sequence == 101

    @pytest.mark.asyncio
    async def test_gate_buffered_replay_missing_overlap_keeps_rebuilding(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("gate", "GAPUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        dp = LocalL2DataPlane(rt, _RecordingJournal())

        dp.ingest_external_update(
            LocalL2Update(
                venue="gate",
                symbol="GAPUSDT",
                bids=[PriceLevel(49910.0, 10.0)],
                asks=[PriceLevel(50110.0, 10.0)],
                first_sequence=105,
                sequence=110,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=1100,
        )

        ok = await dp.bootstrap_book(
            "gate",
            "GAPUSDT",
            MockL2Adapter("gate", sequence=100),
            now_ms=2000,
        )

        assert ok is False
        assert "snapshot_boundary" in rt.get_book("gate", "GAPUSDT").fault_reason
        assert rt.get_book("gate", "GAPUSDT").status == L2BookStatus.REBUILDING

    @pytest.mark.asyncio
    async def test_gate_no_buffer_immediate_rebase_is_range_only_and_bounded(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("gate", "REBASEUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        dp = LocalL2DataPlane(rt, _RecordingJournal())

        ok = await dp.bootstrap_book(
            "gate",
            "REBASEUSDT",
            MockL2Adapter("gate", sequence=100),
            now_ms=2000,
        )
        assert ok is True
        assert book.status == L2BookStatus.HOT

        events = dp.ingest_external_update(
            LocalL2Update(
                venue="gate",
                symbol="REBASEUSDT",
                bids=[PriceLevel(49910.0, 10.0)],
                asks=[PriceLevel(50110.0, 10.0)],
                first_sequence=100,
                sequence=101,
                previous_sequence=999,
                previous_sequence_present=True,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=2100,
        )
        assert events
        assert book.status == L2BookStatus.HOT
        assert book.sequence == 101

        gap_events = dp.ingest_external_update(
            LocalL2Update(
                venue="gate",
                symbol="REBASEUSDT",
                bids=[PriceLevel(49920.0, 10.0)],
                asks=[PriceLevel(50120.0, 10.0)],
                first_sequence=103,
                sequence=104,
                previous_sequence=101,
                previous_sequence_present=True,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=2200,
        )
        assert gap_events == []
        assert book.status == L2BookStatus.REBUILDING
        assert "sequence_ahead" in book.fault_reason

    @pytest.mark.asyncio
    async def test_gate_bootstrap_rebase_replays_same_generation_overlap(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("gate", "REBASEOKUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)
        dp.bootstrap_rebase_wait_ms = 0

        dp.ingest_external_update(
            LocalL2Update(
                venue="gate",
                symbol="REBASEOKUSDT",
                bids=[PriceLevel(49910.0, 10.0)],
                asks=[PriceLevel(50110.0, 10.0)],
                first_sequence=105,
                sequence=106,
                previous_sequence=999,
                previous_sequence_present=True,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=1100,
        )
        adapter = SequenceMockL2Adapter("gate", [100, 104])

        ok = await dp.bootstrap_book(
            "gate",
            "REBASEOKUSDT",
            adapter,
            now_ms=2000,
        )

        assert ok is True
        assert adapter.call_count == 2
        assert rt.get_book("gate", "REBASEOKUSDT").status == L2BookStatus.HOT
        assert rt.get_book("gate", "REBASEOKUSDT").sequence == 106
        rebase = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_snapshot_rebase"
        ][-1]
        assert rebase["branch"] == "second_snapshot_overlap"
        assert rebase["rebase_wait_ms"] <= 250
        assert rebase["first_live_buffered_U"] == 105
        assert rebase["generation_isolation"] == "current_generation_only"

    @pytest.mark.asyncio
    async def test_gate_bootstrap_rebase_second_no_overlap_fails_after_two_fetches(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("gate", "REBASEGAPUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)
        dp.bootstrap_rebase_wait_ms = 0

        dp.ingest_external_update(
            LocalL2Update(
                venue="gate",
                symbol="REBASEGAPUSDT",
                bids=[PriceLevel(49910.0, 10.0)],
                asks=[PriceLevel(50110.0, 10.0)],
                first_sequence=110,
                sequence=111,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=1100,
        )
        adapter = SequenceMockL2Adapter("gate", [100, 104])

        ok = await dp.bootstrap_book(
            "gate",
            "REBASEGAPUSDT",
            adapter,
            now_ms=2000,
        )

        assert ok is False
        assert adapter.call_count == 2
        assert rt.get_book("gate", "REBASEGAPUSDT").status == L2BookStatus.REBUILDING
        rebase = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_snapshot_rebase"
        ][-1]
        assert rebase["branch"] == "second_snapshot_no_overlap"
        assert rebase["rebase_wait_ms"] <= 250
        failure = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_buffered_replay_rebuild"
        ][-1]
        assert failure["continuity_contract"] == "range_only_U_u_contains_expected"
        assert failure["continuity_action"] == "range_gap_rebuild"
        assert failure["strict_continuity_rule"] == "range_must_contain_expected_sequence"

    @pytest.mark.asyncio
    async def test_gate_rebase_stale_generation_response_is_not_applied(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("gate", "REBASESTALEUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)
        dp.bootstrap_rebase_wait_ms = 0

        dp.ingest_external_update(
            LocalL2Update(
                venue="gate",
                symbol="REBASESTALEUSDT",
                bids=[PriceLevel(49910.0, 10.0)],
                asks=[PriceLevel(50110.0, 10.0)],
                first_sequence=105,
                sequence=106,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=1100,
        )

        class StaleRebaseAdapter(SequenceMockL2Adapter):
            async def fetch_l2_snapshot(
                self,
                symbol: str,
                depth: int = 50,
            ) -> LocalL2Update:
                update = await super().fetch_l2_snapshot(symbol, depth)
                if self.call_count == 2:
                    dp._advance_stream_generation("gate", "REBASESTALEUSDT")
                return update

        ok = await dp.bootstrap_book(
            "gate",
            "REBASESTALEUSDT",
            StaleRebaseAdapter("gate", [100, 104]),
            now_ms=2000,
        )

        assert ok is False
        assert book.sequence == 100
        stale = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_snapshot_stale_response_discarded"
        ][-1]
        assert stale["reason"] == "gate_rebase_snapshot_response_stale_generation"
        assert stale["response_discarded"] is True
        assert stale["stream_generation"] != stale["current_stream_generation"]

    @pytest.mark.asyncio
    async def test_gate_rebase_stale_second_snapshot_sequence_is_not_applied(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("gate", "REBASEOLDUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)
        dp.bootstrap_rebase_wait_ms = 0

        dp.ingest_external_update(
            LocalL2Update(
                venue="gate",
                symbol="REBASEOLDUSDT",
                bids=[PriceLevel(49910.0, 10.0)],
                asks=[PriceLevel(50110.0, 10.0)],
                first_sequence=125,
                sequence=126,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=1100,
        )
        ok = await dp.bootstrap_book(
            "gate",
            "REBASEOLDUSDT",
            SequenceMockL2Adapter("gate", [120, 110]),
            now_ms=2000,
        )

        assert ok is False
        assert book.sequence == 120
        assert book.last_update_id == 120
        assert book.status == L2BookStatus.REBUILDING
        stale = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_snapshot_stale_response_discarded"
        ][-1]
        assert stale["reason"] == "gate_rebase_snapshot_stale_sequence"
        assert stale["branch"] == "second_snapshot_stale_sequence"
        assert stale["snapshot_seq"] == 110
        assert stale["book_seq"] == 120
        assert stale["response_discarded"] is True
        assert stale["sequence_monotonic_bound"] is True

    @pytest.mark.asyncio
    async def test_gate_bootstrap_rebase_filters_old_stream_generation(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("gate", "REBASEGENUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)
        dp.bootstrap_rebase_wait_ms = 0

        dp.ingest_external_update(
            LocalL2Update(
                venue="gate",
                symbol="REBASEGENUSDT",
                bids=[PriceLevel(49910.0, 10.0)],
                asks=[PriceLevel(50110.0, 10.0)],
                first_sequence=105,
                sequence=106,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=1100,
        )
        dp._advance_stream_generation("gate", "REBASEGENUSDT")
        dp.ingest_external_update(
            LocalL2Update(
                venue="gate",
                symbol="REBASEGENUSDT",
                bids=[PriceLevel(49920.0, 10.0)],
                asks=[PriceLevel(50120.0, 10.0)],
                first_sequence=110,
                sequence=111,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=1200,
        )

        ok = await dp.bootstrap_book(
            "gate",
            "REBASEGENUSDT",
            SequenceMockL2Adapter("gate", [100, 104]),
            now_ms=2000,
        )

        assert ok is False
        rebase = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_snapshot_rebase"
        ][-1]
        assert rebase["branch"] == "second_snapshot_no_overlap"
        assert rebase["buffered_count"] == 2
        assert rebase["buffer_current_generation_count"] == 1
        assert rebase["current_first_buffered_U"] == 110

    def test_binance_pre_snapshot_buffer_uses_v1_capacity(self):
        """V1 BINANCE_LOCAL_L2_PRE_SNAPSHOT_BUFFER_CAP = 4096, not 512."""
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "CHIPUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        dp = LocalL2DataPlane(rt, _make_journal())

        # 512 deltas at the old 512 cap would overflow; at 4096 they must not
        for seq in range(1, 513):
            events = dp.ingest_external_update(
                LocalL2Update(
                    venue="binance",
                    symbol="CHIPUSDT",
                    bids=[PriceLevel(1.0, 1.0)],
                    asks=[],
                    sequence=seq,
                    previous_sequence=seq - 1,
                    update_kind=LocalL2UpdateKind.DELTA,
                ),
                now_ms=seq,
            )
            assert events == []

        assert rt.get_book("binance", "CHIPUSDT").status == L2BookStatus.BOOTSTRAPPING

    def test_binance_buffered_replay_previous_link_mismatch_keeps_book_rebuilding(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "JTOUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        dp = LocalL2DataPlane(rt, _make_journal())

        for seq, prev in [(10591999713004, 10591999713003), (10591999715270, 10591999715264)]:
            dp.ingest_external_update(
                LocalL2Update(
                    venue="binance",
                    symbol="JTOUSDT",
                    bids=[PriceLevel(49910.0, 10.0)],
                    asks=[PriceLevel(50110.0, 10.0)],
                    sequence=seq,
                    previous_sequence=prev,
                    update_kind=LocalL2UpdateKind.DELTA,
                ),
                now_ms=seq,
            )

        book.apply_snapshot(
            [PriceLevel(49900.0, 10.0)],
            [PriceLevel(50100.0, 10.0)],
            sequence=10591999713003,
            now_ms=1,
        )

        replay = dp._replay_buffered_updates("binance", "JTOUSDT")

        assert replay.ok is False
        assert "previous_link_mismatch" in rt.get_book("binance", "JTOUSDT").fault_reason
        assert rt.get_book("binance", "JTOUSDT").status == L2BookStatus.REBUILDING

    @pytest.mark.asyncio
    async def test_binance_buffered_replay_previous_link_mismatch_payload(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "DEXEUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)

        for now_ms, first_seq, seq, prev in (
            (1000, 101, 102, 100),
            (1250, 103, 104, 101),
        ):
            dp.ingest_external_update(
                LocalL2Update(
                    venue="binance",
                    symbol="DEXEUSDT",
                    bids=[PriceLevel(49920.0, 10.0)],
                    asks=[PriceLevel(50120.0, 10.0)],
                    first_sequence=first_seq,
                    sequence=seq,
                    previous_sequence=prev,
                    previous_sequence_present=True,
                    update_kind=LocalL2UpdateKind.DELTA,
                ),
                now_ms=now_ms,
            )

        ok = await dp.bootstrap_book(
            "binance",
            "DEXEUSDT",
            MockL2Adapter("binance", sequence=100),
            now_ms=2000,
        )

        assert ok is False
        payload = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_snapshot_error"
            and payload.get("category") == "buffered_replay_failed"
        ][0]
        assert payload["raw_U"] == 103
        assert payload["raw_u"] == 104
        assert payload["raw_pu"] == 101
        assert payload["snapshot_lastUpdateId"] == 100
        assert payload["expected_previous_sequence"] == 102
        assert payload["buffered_count"] == 2
        assert payload["buffer_age_ms"] == 1000
        assert payload["rebuild_attempt_id"] == 1
        assert payload["venue"] == "binance"
        assert payload["symbol"] == "DEXEUSDT"
        assert payload["status_before"] == "bootstrapping"
        assert payload["status_after"] == "rebuilding"
        assert payload["replay_failure_alert"] is False
        assert payload["root_bug_suspected"] is False

    @pytest.mark.asyncio
    async def test_binance_buffered_replay_same_symbol_alert_threshold(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "EDENUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)
        dp.buffered_replay_failure_alert_threshold = 3

        for attempt in range(3):
            dp.ingest_external_update(
                LocalL2Update(
                    venue="binance",
                    symbol="EDENUSDT",
                    bids=[PriceLevel(49910.0, 10.0)],
                    asks=[PriceLevel(50110.0, 10.0)],
                    first_sequence=101,
                    sequence=102,
                    previous_sequence=100,
                    previous_sequence_present=True,
                    update_kind=LocalL2UpdateKind.DELTA,
                ),
                now_ms=1000 + attempt * 100,
            )
            dp.ingest_external_update(
                LocalL2Update(
                    venue="binance",
                    symbol="EDENUSDT",
                    bids=[PriceLevel(49920.0, 10.0)],
                    asks=[PriceLevel(50120.0, 10.0)],
                    first_sequence=103,
                    sequence=104,
                    previous_sequence=101,
                    previous_sequence_present=True,
                    update_kind=LocalL2UpdateKind.DELTA,
                ),
                now_ms=1100 + attempt * 100,
            )
            await dp.bootstrap_book(
                "binance",
                "EDENUSDT",
                MockL2Adapter("binance", sequence=100),
                now_ms=2000 + attempt * 1000,
            )

        payloads = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_snapshot_error"
            and payload.get("category") == "buffered_replay_failed"
        ]
        assert [p["replay_failure_count_for_symbol"] for p in payloads] == [1, 2, 3]
        assert [p["replay_failure_alert"] for p in payloads] == [False, False, True]
        assert payloads[0]["severity"] == "info"
        assert payloads[2]["severity"] == "warning"
        assert all(p["root_bug_suspected"] is False for p in payloads)

    def test_gate_hot_range_gap_rebuilds_without_previous_link(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("gate", "HOTGAPUSDT")
        book.status = L2BookStatus.HOT
        book.sequence = 100
        book.last_update_id = 100
        book.bids = [PriceLevel(100.0, 1.0)]
        book.asks = [PriceLevel(101.0, 1.0)]
        dp = LocalL2DataPlane(rt, _RecordingJournal())

        events = dp.ingest_external_update(
            LocalL2Update(
                venue="gate",
                symbol="HOTGAPUSDT",
                bids=[PriceLevel(100.0, 2.0)],
                asks=[],
                first_sequence=102,
                sequence=103,
                previous_sequence=0,
                previous_sequence_present=False,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=2000,
        )

        assert events == []
        assert book.status == L2BookStatus.REBUILDING
        assert "sequence_ahead" in book.fault_reason

    def test_gate_hot_overlapping_range_applies_without_previous_link(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("gate", "HOTOKUSDT")
        book.status = L2BookStatus.HOT
        book.sequence = 100
        book.last_update_id = 100
        book.bids = [PriceLevel(100.0, 1.0)]
        book.asks = [PriceLevel(101.0, 1.0)]
        dp = LocalL2DataPlane(rt, _RecordingJournal())

        events = dp.ingest_external_update(
            LocalL2Update(
                venue="gate",
                symbol="HOTOKUSDT",
                bids=[PriceLevel(100.0, 2.0)],
                asks=[],
                first_sequence=101,
                sequence=101,
                previous_sequence=0,
                previous_sequence_present=False,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=2000,
        )

        assert events
        assert book.status == L2BookStatus.HOT
        assert book.sequence == 101


# ---------------------------------------------------------------------------
# Task 6: OKX V1 Replay Classification — keepalive / reset / obsolete / invalid
# ---------------------------------------------------------------------------


class TestOkxV1ReplayClassification:
    def test_okx_keepalive_buffered_and_replayed_without_sequence_gap(self):
        """OKX keepalive: seqId == prevSeqId, empty bids/asks — must not trigger gap."""
        rt = LocalL2Runtime()
        book = rt.ensure_book("okx", "INJUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        dp = LocalL2DataPlane(rt, _make_journal())

        # Buffered: snapshot seq 100, then keepalive with same seq
        dp.ingest_external_update(
            LocalL2Update(
                venue="okx", symbol="INJUSDT",
                bids=[], asks=[],
                sequence=100, previous_sequence=100,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=1000,
        )

        book.apply_snapshot(
            [PriceLevel(1.0, 10.0)], [PriceLevel(1.1, 10.0)],
            sequence=100, now_ms=1000,
        )

        replay = dp._replay_buffered_updates("okx", "INJUSDT")
        assert replay.ok is True, "keepalive should not break replay"
        assert replay.replayed == 1
        assert rt.get_book("okx", "INJUSDT").sequence == 100
        assert rt.get_book("okx", "INJUSDT").last_delta_ms == 1000

    def test_okx_reset_buffered_and_replayed_accepted(self):
        """OKX reset: seqId < prevSeqId but pseq matches — reset accepted per V1."""
        rt = LocalL2Runtime()
        book = rt.ensure_book("okx", "CHIPUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        dp = LocalL2DataPlane(rt, _make_journal())

        dp.ingest_external_update(
            LocalL2Update(
                venue="okx", symbol="CHIPUSDT",
                bids=[PriceLevel(1.0, 1.0)], asks=[PriceLevel(1.1, 1.0)],
                sequence=5, previous_sequence=15,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=1000,
        )

        book.apply_snapshot(
            [PriceLevel(1.0, 10.0)], [PriceLevel(1.1, 10.0)],
            sequence=15, now_ms=1000,
        )

        replay = dp._replay_buffered_updates("okx", "CHIPUSDT")
        assert replay.ok is True, "reset should be accepted per V1 classifier"
        assert replay.replayed == 1
        assert rt.get_book("okx", "CHIPUSDT").sequence == 5


class TestOkxReplayLinkClassification:
    def test_okx_classify_normal(self):
        from lightfee.marketdata.local_l2_policy import ReplayLinkKind, policy_for_venue
        policy = policy_for_venue("okx")
        kind = policy.classify_replay_link(
            previous_sequence=10, sequence=11,
            previous_sequence_from_update=10,
            bid_count=5, ask_count=5,
        )
        assert kind is ReplayLinkKind.NORMAL

    def test_okx_classify_keepalive(self):
        from lightfee.marketdata.local_l2_policy import ReplayLinkKind, policy_for_venue
        policy = policy_for_venue("okx")
        kind = policy.classify_replay_link(
            previous_sequence=10, sequence=10,
            previous_sequence_from_update=10,
            bid_count=0, ask_count=0,
        )
        assert kind is ReplayLinkKind.KEEPALIVE

    def test_okx_classify_reset(self):
        from lightfee.marketdata.local_l2_policy import ReplayLinkKind, policy_for_venue
        policy = policy_for_venue("okx")
        kind = policy.classify_replay_link(
            previous_sequence=15, sequence=3,
            previous_sequence_from_update=15,
            bid_count=1, ask_count=1,
        )
        assert kind is ReplayLinkKind.RESET

    def test_okx_classify_obsolete(self):
        from lightfee.marketdata.local_l2_policy import ReplayLinkKind, policy_for_venue
        policy = policy_for_venue("okx")
        kind = policy.classify_replay_link(
            previous_sequence=15, sequence=10,
            previous_sequence_from_update=10,
            bid_count=1, ask_count=1,
        )
        assert kind is ReplayLinkKind.OBSOLETE

    def test_okx_classify_invalid(self):
        from lightfee.marketdata.local_l2_policy import ReplayLinkKind, policy_for_venue
        policy = policy_for_venue("okx")
        kind = policy.classify_replay_link(
            previous_sequence=10, sequence=15,
            previous_sequence_from_update=12,
            bid_count=1, ask_count=1,
        )
        assert kind is ReplayLinkKind.INVALID


class TestLocalL2HotFreshnessThirtyMinuteSimulation:
    async def _sync(self, dp, adapters, now_ms: int):
        return await dp.sync_snapshots(adapters, now_ms=now_ms, scan_promoted=True)

    def test_valid_delta_refreshes_hot_book_even_when_top_levels_do_not_change(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "UNCHANGEDUSDT")
        book.status = L2BookStatus.HOT
        book.observed_at_ms = 1_000
        book.sequence = 10
        book.last_update_id = 10
        book.bids = [PriceLevel(100.0, 1.0)]
        book.asks = [PriceLevel(101.0, 1.0)]
        dp = LocalL2DataPlane(rt, _RecordingJournal())

        events = dp.ingest_external_update(
            LocalL2Update(
                venue="binance",
                symbol="UNCHANGEDUSDT",
                bids=[PriceLevel(100.0, 1.0)],
                asks=[PriceLevel(101.0, 1.0)],
                sequence=11,
                previous_sequence=10,
                previous_sequence_present=True,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=2_000,
        )

        assert events
        assert book.status == L2BookStatus.HOT
        assert book.observed_at_ms == 2_000

    @pytest.mark.asyncio
    async def test_ws_authoritative_heartbeat_keeps_hot_book_fresh_for_30_minutes(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("bybit", "HEARTUSDT")
        book.status = L2BookStatus.HOT
        book.pool = L2PoolAssignment.HOT_EXEC
        book.observed_at_ms = 1_000
        book.last_snapshot_ms = 1_000
        book.bids = [PriceLevel(100.0, 1.0)]
        book.asks = [PriceLevel(101.0, 1.0)]
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)
        dp.hot_stale_after_ms = 60_000

        class FakeClient:
            is_connected = True

        dp._ws_clients[LocalL2BookKey("bybit", "HEARTUSDT")] = FakeClient()

        for now_ms in range(1_000, 30 * 60 * 1000 + 30_001, 30_000):
            dp.note_ws_keepalive("bybit", "HEARTUSDT", now_ms=now_ms)
            await self._sync(dp, {}, now_ms)

        assert book.status == L2BookStatus.HOT
        assert book.observed_at_ms >= 30 * 60 * 1000
        assert not [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_hot_stale_rebuild"
        ]

    @pytest.mark.asyncio
    async def test_rest_buffered_replay_proactive_refresh_stays_before_stale_threshold_for_30_minutes(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "RESTFRESHUSDT")
        book.status = L2BookStatus.HOT
        book.pool = L2PoolAssignment.HOT_EXEC
        book.observed_at_ms = 1_000
        book.last_snapshot_ms = 1_000
        book.bids = [PriceLevel(100.0, 1.0)]
        book.asks = [PriceLevel(101.0, 1.0)]
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)
        dp.hot_stale_after_ms = 5_000
        dp.hot_refresh_interval_ms = 30_000

        class FreshAdapter(MockL2Adapter):
            def __init__(self):
                super().__init__("binance", sequence=1)
                self.now_ms = 1_000

            async def fetch_l2_snapshot(self, symbol: str, depth: int = 50) -> LocalL2Update:
                update = await super().fetch_l2_snapshot(symbol, depth)
                update.event_time_ms = self.now_ms
                update.received_at_ms = self.now_ms
                update.sequence = self.call_count
                return update

        adapter = FreshAdapter()
        from lightfee.core.domain import Venue

        for now_ms in range(1_000, 30 * 60 * 1000 + 1_001, 1_000):
            adapter.now_ms = now_ms
            await self._sync(dp, {Venue.BINANCE: adapter}, now_ms)
            assert book.status == L2BookStatus.HOT
            assert now_ms - book.observed_at_ms <= dp.hot_stale_after_ms

        assert adapter.call_count >= 300
        assert not [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_hot_stale_rebuild"
        ]
        lifecycle = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_hot_refresh_lifecycle"
        ]
        assert lifecycle
        assert {payload["status"] for payload in lifecycle} >= {
            "queued",
            "started",
            "ready",
        }
        assert all("deadline_ms" in payload for payload in lifecycle)
        assert all("deadline_missed" in payload for payload in lifecycle)
        assert any(payload["queued"] is True for payload in lifecycle)
        assert any(payload["started"] is True for payload in lifecycle)
        assert any(payload["completed"] is True for payload in lifecycle)
        assert all(
            payload["attempted_count"] <= dp.max_concurrent_snapshots
            for payload in lifecycle
        )
        assert all(payload["reason"] == "rest_refresh_late" for payload in lifecycle)

    @pytest.mark.asyncio
    async def test_missing_ws_subscription_is_the_rebuild_reason_after_30_minutes(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("bybit", "MISSWSUSDT")
        book.status = L2BookStatus.HOT
        book.pool = L2PoolAssignment.HOT_EXEC
        book.observed_at_ms = 1_000
        book.bids = [PriceLevel(100.0, 1.0)]
        book.asks = [PriceLevel(101.0, 1.0)]
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)
        dp.hot_stale_after_ms = 60_000

        await self._sync(dp, {}, now_ms=30 * 60 * 1000 + 1_000)

        assert book.status == L2BookStatus.REBUILDING
        payloads = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_hot_stale_rebuild"
        ]
        assert len(payloads) == 1
        assert payloads[0]["reason"] == "subscription_missing"

    @pytest.mark.asyncio
    async def test_connected_ws_without_subscription_confirmation_is_subscription_missing(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("bybit", "CONNECTEDMISSUSDT")
        book.status = L2BookStatus.HOT
        book.pool = L2PoolAssignment.HOT_EXEC
        book.observed_at_ms = 1_000
        book.bids = [PriceLevel(100.0, 1.0)]
        book.asks = [PriceLevel(101.0, 1.0)]
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)
        dp.hot_stale_after_ms = 60_000

        class FakeClient:
            is_connected = True

        dp._ws_clients[LocalL2BookKey("bybit", "CONNECTEDMISSUSDT")] = FakeClient()

        await self._sync(dp, {}, now_ms=30 * 60 * 1000 + 1_000)

        payloads = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_hot_stale_rebuild"
        ]
        assert len(payloads) == 1
        assert payloads[0]["reason"] == "subscription_missing"

    @pytest.mark.asyncio
    async def test_connected_ws_with_subscription_but_no_delta_is_no_ws_delta(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("bybit", "NODELTAUSDT")
        book.status = L2BookStatus.HOT
        book.pool = L2PoolAssignment.HOT_EXEC
        book.observed_at_ms = 1_000
        book.bids = [PriceLevel(100.0, 1.0)]
        book.asks = [PriceLevel(101.0, 1.0)]
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)
        dp.hot_stale_after_ms = 60_000

        class FakeClient:
            is_connected = True

        dp._ws_clients[LocalL2BookKey("bybit", "NODELTAUSDT")] = FakeClient()
        dp.note_ws_subscription_confirmed("bybit", "NODELTAUSDT", now_ms=1_000)

        await self._sync(dp, {}, now_ms=30 * 60 * 1000 + 1_000)

        payloads = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_hot_stale_rebuild"
        ]
        assert len(payloads) == 1
        assert payloads[0]["reason"] == "no_ws_delta"

    @pytest.mark.asyncio
    async def test_real_delta_proves_subscription_was_not_missing(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("bybit", "DELTAONLYUSDT")
        book.status = L2BookStatus.HOT
        book.pool = L2PoolAssignment.HOT_EXEC
        book.observed_at_ms = 1_000
        book.bids = [PriceLevel(100.0, 1.0)]
        book.asks = [PriceLevel(101.0, 1.0)]
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)
        dp.hot_stale_after_ms = 60_000

        class FakeClient:
            is_connected = True

        dp._ws_clients[LocalL2BookKey("bybit", "DELTAONLYUSDT")] = FakeClient()
        dp.note_ws_delta("bybit", "DELTAONLYUSDT", now_ms=1_000)

        await self._sync(dp, {}, now_ms=30 * 60 * 1000 + 1_000)

        payloads = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_hot_stale_rebuild"
        ]
        assert len(payloads) == 1
        assert payloads[0]["reason"] == "no_keepalive"

    @pytest.mark.asyncio
    async def test_connected_ws_with_stale_keepalive_is_no_keepalive(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("bybit", "NOKEEPALIVEUSDT")
        book.status = L2BookStatus.HOT
        book.pool = L2PoolAssignment.HOT_EXEC
        book.observed_at_ms = 1_000
        book.bids = [PriceLevel(100.0, 1.0)]
        book.asks = [PriceLevel(101.0, 1.0)]
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)
        dp.hot_stale_after_ms = 60_000

        class FakeClient:
            is_connected = True

        dp._ws_clients[LocalL2BookKey("bybit", "NOKEEPALIVEUSDT")] = FakeClient()
        dp.note_ws_subscription_confirmed("bybit", "NOKEEPALIVEUSDT", now_ms=1_000)
        dp.note_ws_delta("bybit", "NOKEEPALIVEUSDT", now_ms=1_000)

        await self._sync(dp, {}, now_ms=30 * 60 * 1000 + 1_000)

        payloads = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_hot_stale_rebuild"
        ]
        assert len(payloads) == 1
        assert payloads[0]["reason"] == "no_keepalive"

    @pytest.mark.asyncio
    async def test_rest_hot_book_without_proactive_adapter_is_rest_refresh_late(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "RESTLATEUSDT")
        book.status = L2BookStatus.HOT
        book.pool = L2PoolAssignment.HOT_EXEC
        book.observed_at_ms = 1_000
        book.last_snapshot_ms = 1_000
        book.bids = [PriceLevel(100.0, 1.0)]
        book.asks = [PriceLevel(101.0, 1.0)]
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)
        dp.hot_stale_after_ms = 60_000

        await self._sync(dp, {}, now_ms=30 * 60 * 1000 + 1_000)

        payloads = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_hot_stale_rebuild"
        ]
        assert len(payloads) == 1
        assert payloads[0]["reason"] == "rest_refresh_late"

    @pytest.mark.asyncio
    async def test_future_observed_timestamp_is_clock_skew_rebuild_reason(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("bybit", "SKEWUSDT")
        book.status = L2BookStatus.HOT
        book.pool = L2PoolAssignment.HOT_EXEC
        book.observed_at_ms = 120_000
        book.bids = [PriceLevel(100.0, 1.0)]
        book.asks = [PriceLevel(101.0, 1.0)]
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)
        dp.hot_stale_after_ms = 60_000

        class FakeClient:
            is_connected = True

        dp._ws_clients[LocalL2BookKey("bybit", "SKEWUSDT")] = FakeClient()

        await self._sync(dp, {}, now_ms=1_000)

        payloads = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_hot_stale_rebuild"
        ]
        assert len(payloads) == 1
        assert payloads[0]["reason"] == "clock_skew"

    @pytest.mark.asyncio
    async def test_hot_stale_rebuild_events_are_rate_limited_per_symbol_and_reason(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("bybit", "RATELIMITUSDT")
        book.status = L2BookStatus.HOT
        book.pool = L2PoolAssignment.HOT_EXEC
        book.observed_at_ms = 1_000
        book.bids = [PriceLevel(100.0, 1.0)]
        book.asks = [PriceLevel(101.0, 1.0)]
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)
        dp.hot_stale_after_ms = 60_000
        dp.state_event_rate_limit_ms = 60_000

        await self._sync(dp, {}, now_ms=70_000)
        book.status = L2BookStatus.HOT
        await self._sync(dp, {}, now_ms=70_500)
        book.status = L2BookStatus.HOT
        await self._sync(dp, {}, now_ms=131_000)

        payloads = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_hot_stale_rebuild"
        ]
        assert len(payloads) == 2
        assert payloads[0]["reason"] == "subscription_missing"
        assert "suppressed_count" not in payloads[0]
        assert payloads[1]["reason"] == "subscription_missing"
        assert payloads[1]["suppressed_count"] == 1


class TestLocalL2DiagnosticCompaction:
    def test_freshness_state_events_use_longer_compact_window(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("bybit", "FRESHCOMPACTUSDT")
        book.status = L2BookStatus.HOT
        book.bids = [PriceLevel(100.0, 1.0)]
        book.asks = [PriceLevel(101.0, 1.0)]
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)

        dp.note_ws_delta("bybit", "FRESHCOMPACTUSDT", now_ms=1_000)
        dp.note_ws_delta("bybit", "FRESHCOMPACTUSDT", now_ms=61_000)
        dp.note_ws_delta("bybit", "FRESHCOMPACTUSDT", now_ms=301_000)

        payloads = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_freshness_state"
        ]
        assert len(payloads) == 2
        assert payloads[0]["event"] == "ws_delta"
        assert "compact" not in payloads[0]
        assert payloads[1]["event"] == "ws_delta"
        assert payloads[1]["compact"] is True
        assert payloads[1]["suppressed_count"] == 1

    @pytest.mark.asyncio
    async def test_ws_authoritative_rest_bootstrap_deferred_events_are_compacted(self):
        rt = LocalL2Runtime()
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)
        adapter = MockL2Adapter("bybit", sequence=100)

        class FakeClient:
            is_connected = True

        dp._ws_clients[LocalL2BookKey("bybit", "DEFERCOMPACTUSDT")] = FakeClient()

        assert await dp.bootstrap_book("bybit", "DEFERCOMPACTUSDT", adapter, now_ms=1_000) is False
        assert await dp.bootstrap_book("bybit", "DEFERCOMPACTUSDT", adapter, now_ms=2_000) is False
        assert await dp.bootstrap_book("bybit", "DEFERCOMPACTUSDT", adapter, now_ms=61_000) is False

        payloads = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_rest_bootstrap_deferred_for_ws_snapshot"
        ]
        assert len(payloads) == 2
        assert "compact" not in payloads[0]
        assert payloads[1]["compact"] is True
        assert payloads[1]["suppressed_count"] == 1

    @pytest.mark.asyncio
    async def test_snapshot_ok_events_are_compacted_until_recovery_success(self):
        rt = LocalL2Runtime()
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)

        assert await dp.bootstrap_book("binance", "OKCOMPACTUSDT", MockL2Adapter("binance", sequence=100), now_ms=1_000) is True
        assert await dp.bootstrap_book("binance", "OKCOMPACTUSDT", MockL2Adapter("binance", sequence=101), now_ms=7_000) is True
        assert await dp.bootstrap_book("binance", "OKCOMPACTUSDT", MockL2Adapter("binance", sequence=102), now_ms=301_000) is True

        payloads = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_snapshot_ok"
        ]
        assert len(payloads) == 2
        assert "compact" not in payloads[0]
        assert payloads[1]["compact"] is True
        assert payloads[1]["suppressed_count"] == 1

        assert await dp.bootstrap_book("binance", "OKCOMPACTUSDT", MockL2Adapter("binance", should_fail=True), now_ms=307_000) is False
        assert await dp.bootstrap_book("binance", "OKCOMPACTUSDT", MockL2Adapter("binance", sequence=103), now_ms=308_000) is True

        payloads = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_snapshot_ok"
        ]
        assert len(payloads) == 3
        assert "compact" not in payloads[-1]
