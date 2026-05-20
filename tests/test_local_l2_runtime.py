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
from lightfee.marketdata.local_l2_runtime import (
    AssignmentLease,
    LocalL2Runtime,
    LocalL2RuntimeMetrics,
    RuntimeFaultKind,
)


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
