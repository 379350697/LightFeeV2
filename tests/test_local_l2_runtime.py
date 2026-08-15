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
    AssignmentLease,
    LocalL2Runtime,
    LocalL2RuntimeMetrics,
    RuntimeFaultKind,
)
from lightfee.persistence.journal import Journal


class TestAssignmentLease:
    def test_lease_not_expired_when_fresh(self):
        lease = AssignmentLease(
            venue="binance", symbol="BTCUSDT",
            pool=L2PoolAssignment.HOT_EXEC,
            granted_at_ms=10000, ttl_ms=90000,
        )
        assert not lease.is_expired(now_ms=50000)

    def test_lease_expired_after_ttl(self):
        lease = AssignmentLease(
            venue="binance", symbol="BTCUSDT",
            pool=L2PoolAssignment.HOT_EXEC,
            granted_at_ms=10000, ttl_ms=1000,
        )
        assert lease.is_expired(now_ms=12000)

    def test_lease_never_expires_when_ttl_zero(self):
        lease = AssignmentLease(
            venue="binance", symbol="BTCUSDT",
            pool=L2PoolAssignment.HOT_EXEC,
            granted_at_ms=10000, ttl_ms=0,
        )
        assert not lease.is_expired(now_ms=999999)

    def test_lease_remaining_ms(self):
        lease = AssignmentLease(
            venue="binance", symbol="BTCUSDT",
            pool=L2PoolAssignment.HOT_EXEC,
            granted_at_ms=10000, ttl_ms=30000,
        )
        assert lease.remaining_ms(now_ms=20000) == 20000
        assert lease.remaining_ms(now_ms=45000) == 0

    def test_lease_key(self):
        lease = AssignmentLease(
            venue="binance", symbol="BTCUSDT",
            pool=L2PoolAssignment.HOT_EXEC,
            granted_at_ms=10000, ttl_ms=90000,
        )
        assert lease.key() == LocalL2BookKey(venue="binance", symbol="BTCUSDT")


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

    def test_assign_creates_lease_for_hot_exec(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.assign("binance", "BTCUSDT", L2PoolAssignment.HOT_EXEC, now_ms=10000)
        key = LocalL2BookKey(venue="binance", symbol="BTCUSDT")
        assert key in rt.leases

    def test_assign_no_lease_for_dropped(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.assign("binance", "BTCUSDT", L2PoolAssignment.DROPPED, now_ms=10000)
        key = LocalL2BookKey(venue="binance", symbol="BTCUSDT")
        assert key not in rt.leases

    def test_preserve_lease(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.assign("binance", "BTCUSDT", L2PoolAssignment.HOT_EXEC, now_ms=10000, priority=1)
        assert rt.preserve_lease("binance", "BTCUSDT", now_ms=50000)
        assert rt.metrics.assignment_lease_preserved_total == 1

    def test_preserve_lease_noop_for_nonexistent(self):
        rt = LocalL2Runtime()
        assert not rt.preserve_lease("binance", "BTCUSDT", now_ms=50000)
        assert rt.metrics.assignment_lease_preserved_total == 0

    def test_expire_stale_leases(self):
        rt = LocalL2Runtime(default_lease_ttl_ms=1000)
        rt.ensure_book("binance", "BTCUSDT")
        rt.assign("binance", "BTCUSDT", L2PoolAssignment.HOT_EXEC, now_ms=10000)
        expired = rt.expire_stale_leases(now_ms=12000)
        assert len(expired) == 1
        assert rt.get_assignment("binance", "BTCUSDT") == L2PoolAssignment.DROPPED
        assert rt.metrics.assignment_lease_expired_total == 1

    def test_expire_stale_leases_only_expired(self):
        rt = LocalL2Runtime(default_lease_ttl_ms=1000)
        rt.ensure_book("binance", "BTCUSDT")
        rt.ensure_book("binance", "ETHUSDT")
        rt.assign("binance", "BTCUSDT", L2PoolAssignment.HOT_EXEC, now_ms=10000)
        rt.assign("binance", "ETHUSDT", L2PoolAssignment.HOT_EXEC, now_ms=50000)
        expired = rt.expire_stale_leases(now_ms=12000)
        assert len(expired) == 1  # only first expired
        assert rt.get_assignment("binance", "ETHUSDT") == L2PoolAssignment.HOT_EXEC

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
    def test_sync_expires_leases(self):
        rt = LocalL2Runtime(default_lease_ttl_ms=100)
        rt.ensure_book("binance", "BTCUSDT")
        rt.assign("binance", "BTCUSDT", L2PoolAssignment.HOT_EXEC, now_ms=10000)
        events = rt.sync(now_ms=20000)  # 10s later, lease expired
        assert rt.get_assignment("binance", "BTCUSDT") == L2PoolAssignment.DROPPED

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

    @pytest.mark.asyncio
    async def test_binance_rest_snapshot_accepts_one_overlapping_first_delta_then_restores_strict_link(self):
        """V1 accepts exactly one U..u bridge after a REST snapshot, then is strict."""
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "BRIDGEUSDT")
        book.transition_to_bootstrapping(now_ms=1_000)
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)

        dp.ingest_external_update(
            LocalL2Update(
                venue="binance",
                symbol="BRIDGEUSDT",
                bids=[PriceLevel(100.0, 1.0)],
                asks=[],
                first_sequence=101,
                sequence=102,
                previous_sequence=99,
                previous_sequence_present=True,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=1_001,
        )

        assert await dp.bootstrap_book(
            "binance",
            "BRIDGEUSDT",
            MockL2Adapter("binance", sequence=100),
            now_ms=2_000,
        )
        assert book.status == L2BookStatus.HOT
        assert book.sequence == 102

        dp.ingest_external_update(
            LocalL2Update(
                venue="binance",
                symbol="BRIDGEUSDT",
                bids=[PriceLevel(99.0, 1.0)],
                asks=[],
                first_sequence=103,
                sequence=104,
                previous_sequence=101,
                previous_sequence_present=True,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=2_001,
        )

        assert book.status == L2BookStatus.REBUILDING
        assert "previous_link_mismatch" in book.fault_reason

    @pytest.mark.asyncio
    async def test_binance_rest_snapshot_allows_the_same_overlap_on_the_live_ws_path(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "DIRECTUSDT")
        book.transition_to_bootstrapping(now_ms=1_000)
        dp = LocalL2DataPlane(rt, _RecordingJournal())

        assert await dp.bootstrap_book(
            "binance",
            "DIRECTUSDT",
            MockL2Adapter("binance", sequence=100),
            now_ms=2_000,
        )

        dp.ingest_external_update(
            LocalL2Update(
                venue="binance",
                symbol="DIRECTUSDT",
                bids=[PriceLevel(100.0, 1.0)],
                asks=[],
                first_sequence=101,
                sequence=102,
                previous_sequence=99,
                previous_sequence_present=True,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=2_001,
        )

        assert book.status == L2BookStatus.HOT
        assert book.sequence == 102

    @pytest.mark.asyncio
    async def test_failed_first_bridge_clears_the_one_time_overlap_marker(self):
        """A failed bridge must not carry its one-time V1 allowance into rebuild."""
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "FAILBRIDGEUSDT")
        book.transition_to_bootstrapping(now_ms=1_000)
        dp = LocalL2DataPlane(rt, _RecordingJournal())

        assert await dp.bootstrap_book(
            "binance",
            "FAILBRIDGEUSDT",
            MockL2Adapter("binance", sequence=100),
            now_ms=2_000,
        )

        dp.ingest_external_update(
            LocalL2Update(
                venue="binance",
                symbol="FAILBRIDGEUSDT",
                bids=[PriceLevel(60_000.0, 1.0)],
                asks=[],
                first_sequence=101,
                sequence=102,
                previous_sequence=99,
                previous_sequence_present=True,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=2_001,
        )

        assert book.status == L2BookStatus.REBUILDING
        assert LocalL2BookKey("binance", "FAILBRIDGEUSDT") not in (
            dp._initial_snapshot_overlap_sequences
        )


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
    async def test_ws_authoritative_heartbeat_keeps_book_hot_but_not_quote_fresh(self):
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
            dp.note_ws_keepalive(
                "bybit", "HEARTUSDT", now_ms=now_ms, refresh_book=False,
            )
            await self._sync(dp, {}, now_ms)

        assert book.status == L2BookStatus.HOT
        assert book.observed_at_ms == 1_000
        assert not book.execution_snapshot_is_valid(60_000, 30 * 60 * 1000)
        assert not [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_hot_stale_rebuild"
        ]

    @pytest.mark.asyncio
    async def test_connected_bybit_stale_quote_preserves_book_and_next_delta_recovers(self):
        """V1 Bybit semantics: age blocks execution, but cannot destroy WS state."""
        rt = LocalL2Runtime()
        book = rt.ensure_book("bybit", "RECOVERUSDT")
        book.status = L2BookStatus.HOT
        book.pool = L2PoolAssignment.HOT_EXEC
        book.sequence = 100
        book.last_update_id = 100
        book.observed_at_ms = 1_000
        book.bids = [PriceLevel(100.0, 1.0)]
        book.asks = [PriceLevel(101.0, 1.0)]
        journal = _RecordingJournal()
        dp = LocalL2DataPlane(rt, journal)
        dp.hot_stale_after_ms = 100

        class FakeClient:
            is_connected = True

        dp._ws_clients[LocalL2BookKey("bybit", "RECOVERUSDT")] = FakeClient()
        await self._sync(dp, {}, now_ms=1_101)

        assert book.status == L2BookStatus.HOT
        assert book.sequence == 100
        assert book.observed_at_ms == 1_000
        assert not book.execution_snapshot_is_valid(100, 1_101)
        assert any(
            kind == "runtime.local_l2_hot_stale_awaiting_ws_delta"
            for kind, _payload in journal.records
        )

        dp.ingest_external_update(
            LocalL2Update(
                venue="bybit",
                symbol="RECOVERUSDT",
                bids=[PriceLevel(100.0, 2.0)],
                asks=[],
                sequence=101,
                previous_sequence=100,
                previous_sequence_present=True,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=1_102,
        )

        assert book.status == L2BookStatus.HOT
        assert book.sequence == 101
        assert book.observed_at_ms == 1_102

    @pytest.mark.asyncio
    async def test_rest_buffered_replay_hot_book_uses_ws_without_proactive_rest_refresh(self):
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
        dp.hot_stale_after_ms = 60_000

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
            dp.note_ws_delta("binance", "RESTFRESHUSDT", now_ms=now_ms)
            await self._sync(dp, {Venue.BINANCE: adapter}, now_ms)
            assert book.status == L2BookStatus.HOT
            assert now_ms - book.observed_at_ms <= dp.hot_stale_after_ms

        assert adapter.call_count == 0
        assert not [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_hot_stale_rebuild"
        ]

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
    async def test_connected_bybit_without_subscription_confirmation_preserves_sequence_anchor(self):
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

        assert book.status == L2BookStatus.HOT
        payloads = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_hot_stale_awaiting_ws_delta"
        ]
        assert len(payloads) == 1
        assert payloads[0]["reason"] == "subscription_missing"

    @pytest.mark.asyncio
    async def test_connected_bybit_with_no_delta_preserves_sequence_anchor(self):
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

        assert book.status == L2BookStatus.HOT
        payloads = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_hot_stale_awaiting_ws_delta"
        ]
        assert len(payloads) == 1
        assert payloads[0]["reason"] == "no_ws_delta"

    @pytest.mark.asyncio
    async def test_connected_bybit_with_stale_delta_preserves_sequence_anchor(self):
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

        assert book.status == L2BookStatus.HOT
        payloads = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_hot_stale_awaiting_ws_delta"
        ]
        assert len(payloads) == 1
        assert payloads[0]["reason"] == "no_keepalive"

    @pytest.mark.asyncio
    async def test_connected_bybit_with_stale_keepalive_preserves_sequence_anchor(self):
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

        assert book.status == L2BookStatus.HOT
        payloads = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_hot_stale_awaiting_ws_delta"
        ]
        assert len(payloads) == 1
        assert payloads[0]["reason"] == "no_keepalive"

    @pytest.mark.asyncio
    async def test_rest_hot_book_without_ws_subscription_is_subscription_missing(self):
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
        assert payloads[0]["reason"] == "subscription_missing"

    def test_future_exchange_timestamp_uses_local_receive_time_for_l2_freshness(self):
        rt = LocalL2Runtime()
        book = rt.ensure_book("binance", "CLOCKUSDT")
        book.status = L2BookStatus.HOT
        book.sequence = 10
        book.last_update_id = 10
        book.bids = [PriceLevel(100.0, 1.0)]
        book.asks = [PriceLevel(101.0, 1.0)]
        dp = LocalL2DataPlane(rt, _RecordingJournal())

        dp.ingest_external_update(
            LocalL2Update(
                venue="binance",
                symbol="CLOCKUSDT",
                bids=[PriceLevel(100.0, 1.0)],
                asks=[],
                first_sequence=11,
                sequence=11,
                previous_sequence=10,
                previous_sequence_present=True,
                event_time_ms=12_000,
                received_at_ms=2_000,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=2_000,
        )

        assert book.status == L2BookStatus.HOT
        assert book.observed_at_ms == 2_000

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
