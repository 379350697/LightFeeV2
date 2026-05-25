"""V1 semantic parity: Local L2 runtime targets and diagnostics tests.

Validates:
- Bootstrap phase (initial snapshot load)
- Sequence gap detection and handling
- Checksum failure handling
- Stale book detection
- Retained snapshot semantics
- Resume metadata and timeout
- Runtime targets (active books, budget)
- Market snapshot diagnostics (missing, stale, partial, degraded)
"""

from __future__ import annotations

import time
import pytest

from lightfee.marketdata.l2 import (
    L2BookStatus,
    L2PoolAssignment,
    LocalL2Book,
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
from lightfee.marketdata.local_l2_data_plane import (
    LocalL2DataPlane,
    _BookSnapshotState,
    SNAPSHOT_INTERVAL_BOOTSTRAPPING_MS,
    SNAPSHOT_INTERVAL_COLD_MS,
    SNAPSHOT_INTERVAL_HOT_MS,
    SNAPSHOT_INTERVAL_REBUILDING_MS,
)


# ---------------------------------------------------------------------------
# LocalL2Book bootstrap and status transitions
# ---------------------------------------------------------------------------


class TestLocalL2Bootstrap:
    """Local L2 book must support bootstrap (initial snapshot load) semantics."""

    def test_book_starts_cold(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        assert book.status == L2BookStatus.COLD
        assert book.pool == L2PoolAssignment.DROPPED
        assert book.observed_at_ms == 0

    def test_bootstrap_transitions_to_bootstrapping(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        now_ms = int(time.time() * 1000)
        book.transition_to_bootstrapping(now_ms)
        assert book.status == L2BookStatus.BOOTSTRAPPING
        assert book.bootstrap_started_ms == now_ms

    def test_apply_snapshot_transitions_book(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.transition_to_bootstrapping(1000)
        book.transition_to_hot()

        now_ms = 2000
        bids = [PriceLevel(price=50000.0, quantity=1.0)]
        asks = [PriceLevel(price=50001.0, quantity=1.0)]
        result = book.apply_snapshot(bids, asks, sequence=1, now_ms=now_ms)

        assert result.applied is True
        assert book.status == L2BookStatus.HOT
        assert book.sequence == 1
        assert book.best_bid() == 50000.0
        assert book.best_ask() == 50001.0
        assert book.last_snapshot_ms == now_ms

    def test_bootstrap_from_cold_to_hot(self):
        """Full flow: COLD → BOOTSTRAPPING → snapshot → HOT."""
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        now_ms = int(time.time() * 1000)

        book.transition_to_bootstrapping(now_ms)
        assert book.status == L2BookStatus.BOOTSTRAPPING

        book.apply_snapshot(
            [PriceLevel(50000.0, 1.0)],
            [PriceLevel(50001.0, 1.0)],
            sequence=1, now_ms=now_ms,
        )
        book.transition_to_hot()
        assert book.status == L2BookStatus.HOT

    def test_retained_snapshot_book(self):
        """A book with RETAINED pool preserves snapshot for later recovery."""
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        now_ms = int(time.time() * 1000)

        book.pool = L2PoolAssignment.RETAINED
        book.apply_snapshot(
            [PriceLevel(50000.0, 1.0)],
            [PriceLevel(50001.0, 1.0)],
            sequence=100, now_ms=now_ms,
        )
        book.transition_to_hot()

        # Book should still have snapshot data even though RETAINED
        assert book.best_bid() == 50000.0
        assert book.best_ask() == 50001.0
        assert book.sequence == 100


# ---------------------------------------------------------------------------
# Sequence gap detection
# ---------------------------------------------------------------------------


class TestSequenceGapDetection:
    """Local L2 must detect sequence gaps and trigger rebuild."""

    def test_sequence_gap_detected(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.max_sequence_gap = 5
        book.sequence = 100

        # Delta with previous_sequence=120 — gap of 20 > max_gap 5
        bids = [PriceLevel(50000.0, 1.0)]
        asks = [PriceLevel(50001.0, 1.0)]
        result = book.apply_delta(
            bids, asks,
            sequence=121, previous_sequence=120,
            now_ms=2000,
        )

        assert result.applied is False
        assert result.rebuild_required is True
        assert result.events[0].event_kind == LocalL2EventKind.SEQUENCE_GAP

    def test_small_gap_within_tolerance(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.max_sequence_gap = 5
        book.sequence = 100

        # Gap of 3 — within tolerance
        result = book.apply_delta(
            [PriceLevel(50000.0, 1.0)],
            [PriceLevel(50001.0, 1.0)],
            sequence=104, previous_sequence=103,
            now_ms=2000,
        )

        assert result.applied is True
        assert result.rebuild_required is False

    def test_zero_sequence_gap_tolerance_requires_continuity(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.max_sequence_gap = 0
        book.sequence = 100

        result = book.apply_delta(
            [PriceLevel(50000.0, 1.0)],
            [PriceLevel(50001.0, 1.0)],
            sequence=111,
            previous_sequence=110,
            now_ms=2000,
        )

        assert result.applied is False
        assert result.rebuild_required is True
        assert "sequence_gap" in result.fault_reason

    def test_stale_delta_is_ignored_without_mutating_book(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.max_sequence_gap = 0
        book.apply_snapshot(
            [PriceLevel(50000.0, 1.0)],
            [PriceLevel(50100.0, 1.0)],
            sequence=100,
            now_ms=10000,
        )

        result = book.apply_delta(
            [PriceLevel(50050.0, 1.0)],
            [],
            sequence=99,
            previous_sequence=98,
            now_ms=11000,
        )

        assert result.applied is False
        assert result.rebuild_required is False
        assert result.fault_reason == "stale_update prev=100 incoming_prev=98"
        assert book.sequence == 100
        assert book.best_bid() == 50000.0

    def test_no_gap_when_zero_sequence(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.sequence = 0  # Fresh book, no sequence tracking

        result = book.apply_delta(
            [PriceLevel(50000.0, 1.0)],
            [PriceLevel(50001.0, 1.0)],
            sequence=1, previous_sequence=0,
            now_ms=2000,
        )

        assert result.applied is True

    def test_crossed_snapshot_is_rejected_without_polluting_existing_book(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(100.0, 1.0)],
            [PriceLevel(101.0, 1.0)],
            sequence=7,
            now_ms=1000,
        )

        result = book.apply_snapshot(
            [PriceLevel(102.0, 1.0)],
            [PriceLevel(101.0, 1.0)],
            sequence=8,
            now_ms=2000,
        )

        assert result.applied is False
        assert result.rebuild_required is True
        assert "crossed_or_locked_book" in result.fault_reason
        assert book.sequence == 7
        assert book.best_bid() == 100.0
        assert book.best_ask() == 101.0

    def test_crossed_delta_is_rejected_atomically_and_preserves_sequence(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(100.0, 1.0)],
            [PriceLevel(101.0, 1.0)],
            sequence=7,
            now_ms=1000,
        )

        result = book.apply_delta(
            [PriceLevel(102.0, 1.0)],
            [],
            sequence=8,
            previous_sequence=7,
            now_ms=2000,
        )

        assert result.applied is False
        assert result.rebuild_required is True
        assert "crossed_or_locked_book" in result.fault_reason
        assert book.sequence == 7
        assert book.observed_at_ms == 1000
        assert book.best_bid() == 100.0
        assert book.best_ask() == 101.0

    def test_valid_atomic_batch_that_temporarily_touches_top_does_not_rebuild(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(100.0, 1.0)],
            [PriceLevel(101.0, 1.0)],
            sequence=7,
            now_ms=1000,
        )

        result = book.apply_delta(
            [PriceLevel(101.0, 1.0)],
            [PriceLevel(101.0, 0.0), PriceLevel(102.0, 1.0)],
            sequence=8,
            previous_sequence=7,
            now_ms=2000,
        )

        assert result.applied is True
        assert result.rebuild_required is False
        assert book.sequence == 8
        assert book.best_bid() == 101.0
        assert book.best_ask() == 102.0


# ---------------------------------------------------------------------------
# Checksum verification
# ---------------------------------------------------------------------------


class TestChecksumVerification:
    """Local L2 must support checksum verification with failure handling."""

    def test_checksum_mismatch_detected(self):
        book = LocalL2Book(venue="okx", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(50000.0, 1.0)],
            [PriceLevel(50001.0, 1.0)],
            sequence=1, now_ms=1000,
        )
        actual_checksum = book.compute_checksum()
        assert actual_checksum > 0

        # Verify with wrong checksum
        result = book.verify_checksum(expected=99999, now_ms=2000)
        assert len(result.events) > 0
        assert result.events[0].event_kind == LocalL2EventKind.CHECKSUM_MISMATCH
        assert result.fault_reason != ""

    def test_checksum_verification_passes(self):
        book = LocalL2Book(venue="okx", symbol="BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(50000.0, 1.0)],
            [PriceLevel(50001.0, 1.0)],
            sequence=1, now_ms=1000,
        )
        actual = book.compute_checksum()
        result = book.verify_checksum(expected=actual, now_ms=2000)
        assert len(result.events) == 0  # No events on success

    def test_checksum_skipped_when_zeros(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        result = book.verify_checksum(expected=0, now_ms=2000)
        assert len(result.events) == 0

    def test_okx_checksum_uses_first_25_levels_and_signed_int32(self):
        book = LocalL2Book(venue="okx", symbol="BTC-USDT-SWAP")
        book.apply_snapshot(
            [PriceLevel(3366.1, 7), PriceLevel(3366, 6)],
            [PriceLevel(3366.8, 9), PriceLevel(3368, 8)],
            sequence=1,
            now_ms=1000,
        )

        assert book.compute_checksum() == -1881014294


# ---------------------------------------------------------------------------
# Stale book detection
# ---------------------------------------------------------------------------


class TestStaleBookDetection:
    """Local L2 must detect stale books based on age threshold."""

    def test_book_is_stale_when_exceeds_max_age(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.observed_at_ms = 1000
        now_ms = 7000  # 6 seconds later

        assert book.is_stale(max_age_ms=5000, now_ms=now_ms) is True

    def test_book_is_not_stale_within_max_age(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.observed_at_ms = 1000
        now_ms = 3000  # 2 seconds later

        assert book.is_stale(max_age_ms=5000, now_ms=now_ms) is False

    def test_book_without_observation_is_stale(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.observed_at_ms = 0
        now_ms = 1000

        assert book.is_stale(max_age_ms=5000, now_ms=now_ms) is True

    def test_stall_detection(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.stall_timeout_ms = 60000
        book.observed_at_ms = 1000
        now_ms = 62000  # Stall timeout exceeded

        assert book.check_stall(now_ms) is True


# ---------------------------------------------------------------------------
# Resume metadata and timeout
# ---------------------------------------------------------------------------


class TestResumeMetadata:
    """Local L2 must support resume waiting with timeout."""

    def test_resume_waiting_transition(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        now_ms = 10000
        resume_until = now_ms + 30000

        book.transition_to_resume_waiting(resume_until)
        assert book.status == L2BookStatus.RESUME_WAITING
        assert book.resume_waiting_until_ms == resume_until

    def test_resume_waiting_remaining(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.resume_waiting_until_ms = 30000
        now_ms = 15000

        remaining = book.resume_waiting_remaining_ms(now_ms)
        assert remaining == 15000

    def test_resume_waiting_expired(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.resume_waiting_until_ms = 30000
        now_ms = 35000

        remaining = book.resume_waiting_remaining_ms(now_ms)
        assert remaining == 0


# ---------------------------------------------------------------------------
# Runtime targets and budget
# ---------------------------------------------------------------------------


class TestRuntimeTargetsAndBudget:
    """Local L2 runtime must manage active books and budget constraints."""

    def test_hot_exec_symbols_tracked(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.assign("binance", "BTCUSDT", L2PoolAssignment.HOT_EXEC, now_ms=1000)

        hot = rt.hot_exec_symbols()
        assert len(hot) == 1
        assert hot[0] == LocalL2BookKey(venue="binance", symbol="BTCUSDT")

    def test_max_hot_exec_respected_in_promotion(self):
        from lightfee.marketdata.l2 import promote_warm_to_hot

        books = {
            "btc": LocalL2Book(venue="binance", symbol="BTCUSDT", status=L2BookStatus.HOT, pool=L2PoolAssignment.WARM),
            "eth": LocalL2Book(venue="binance", symbol="ETHUSDT", status=L2BookStatus.HOT, pool=L2PoolAssignment.WARM),
            "sol": LocalL2Book(venue="binance", symbol="SOLUSDT", status=L2BookStatus.HOT, pool=L2PoolAssignment.WARM),
        }

        promoted = promote_warm_to_hot(books, max_hot=2)
        assert promoted == 2  # Only 2 promoted, max_hot=2

    def test_assignment_lease_preserve(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.assign("binance", "BTCUSDT", L2PoolAssignment.HOT_EXEC, now_ms=1000)

        assert rt.preserve_lease("binance", "BTCUSDT", now_ms=5000) is True
        assert rt.metrics.assignment_lease_preserved_total == 1

    def test_assignment_lease_expiry(self):
        rt = LocalL2Runtime()
        rt.default_lease_ttl_ms = 1000
        rt.ensure_book("binance", "BTCUSDT")
        rt.assign("binance", "BTCUSDT", L2PoolAssignment.HOT_EXEC, now_ms=1000)

        # Advance past TTL
        expired = rt.expire_stale_leases(now_ms=3000)
        assert len(expired) == 1
        assert rt.get_assignment("binance", "BTCUSDT") == L2PoolAssignment.DROPPED
        assert rt.metrics.assignment_lease_expired_total == 1

    def test_metrics_refresh_counts_books_by_status(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.ensure_book("okx", "ETHUSDT")

        # Set one to bootstrapping, one to hot
        key1 = LocalL2BookKey(venue="binance", symbol="BTCUSDT")
        key2 = LocalL2BookKey(venue="okx", symbol="ETHUSDT")
        rt.books[key1].status = L2BookStatus.BOOTSTRAPPING
        rt.books[key2].status = L2BookStatus.HOT
        rt.books[key2].pool = L2PoolAssignment.HOT_EXEC
        rt.assignments[key2] = L2PoolAssignment.HOT_EXEC

        rt.sync(now_ms=1000)
        assert rt.metrics.bootstrapping_books == 1
        assert rt.metrics.active_books == 1


# ---------------------------------------------------------------------------
# Market snapshot diagnostics
# ---------------------------------------------------------------------------


class TestMarketSnapshotDiagnostics:
    """Market snapshot must preserve missing, stale, partial, degraded semantics."""

    def test_snapshot_diagnostics(self):
        """Snapshot diagnostics exports relevant counters."""
        dp = LocalL2DataPlane.__new__(LocalL2DataPlane)
        # Can't fully init without journal, but we can test the interval mapping
        assert LocalL2DataPlane._snapshot_interval_for_status(L2BookStatus.COLD) == SNAPSHOT_INTERVAL_COLD_MS
        assert LocalL2DataPlane._snapshot_interval_for_status(L2BookStatus.BOOTSTRAPPING) == SNAPSHOT_INTERVAL_BOOTSTRAPPING_MS
        assert LocalL2DataPlane._snapshot_interval_for_status(L2BookStatus.REBUILDING) == SNAPSHOT_INTERVAL_REBUILDING_MS
        assert LocalL2DataPlane._snapshot_interval_for_status(L2BookStatus.HOT) == SNAPSHOT_INTERVAL_HOT_MS

    def test_book_snapshot_state_tracks_failures(self):
        ss = _BookSnapshotState(venue="binance", symbol="BTCUSDT")
        assert ss.consecutive_failures == 0
        assert ss.max_consecutive_failures == 5
        assert ss.last_snapshot_ms == 0
        assert ss.snapshot_in_flight is False

    @pytest.mark.asyncio
    async def test_sync_snapshots_rebuilds_stale_hot_book(self):
        from lightfee.core.domain import Venue

        class Adapter:
            async def fetch_l2_snapshot(self, symbol: str, depth: int = 50) -> LocalL2Update:
                return LocalL2Update(
                    venue="binance",
                    symbol=symbol,
                    bids=[PriceLevel(110.0, 1.0)],
                    asks=[PriceLevel(111.0, 1.0)],
                    sequence=8,
                    event_time_ms=8000,
                    update_kind=LocalL2UpdateKind.SNAPSHOT,
                )

        rt = LocalL2Runtime()
        journal = type("Journal", (), {"append": lambda self, kind, payload: None})()
        dp = LocalL2DataPlane(l2_runtime=rt, journal=journal)
        dp.hot_stale_after_ms = 5000

        book = rt.ensure_book("binance", "BTCUSDT")
        book.pool = L2PoolAssignment.HOT_EXEC
        book.apply_snapshot(
            [PriceLevel(100.0, 1.0)],
            [PriceLevel(101.0, 1.0)],
            sequence=7,
            now_ms=1000,
        )
        book.transition_to_bootstrapping(1000)
        book.transition_to_hot()

        dispatched = await dp.sync_snapshots(
            {Venue.BINANCE: Adapter()},
            now_ms=8000,
            scan_promoted=True,
        )

        assert dispatched == 1
        assert book.status == L2BookStatus.HOT
        assert book.sequence == 8
        assert book.best_bid() == 110.0

    @pytest.mark.asyncio
    async def test_sync_snapshots_skips_dropped_stale_hot_book(self):
        from lightfee.core.domain import Venue

        class Adapter:
            call_count = 0

            async def fetch_l2_snapshot(self, symbol: str, depth: int = 50) -> LocalL2Update:
                self.call_count += 1
                raise AssertionError("dropped books must be pruned, not rebuilt")

        class Journal:
            def __init__(self):
                self.records = []

            def append(self, kind, payload):
                self.records.append((kind, payload))

        rt = LocalL2Runtime()
        journal = Journal()
        dp = LocalL2DataPlane(l2_runtime=rt, journal=journal)
        dp.hot_stale_after_ms = 5000

        book = rt.ensure_book("binance", "BTCUSDT")
        book.apply_snapshot(
            [PriceLevel(100.0, 1.0)],
            [PriceLevel(101.0, 1.0)],
            sequence=7,
            now_ms=1000,
        )
        book.transition_to_bootstrapping(1000)
        book.transition_to_hot()
        assert book.pool == L2PoolAssignment.DROPPED

        adapter = Adapter()
        dispatched = await dp.sync_snapshots(
            {Venue.BINANCE: adapter},
            now_ms=8000,
            scan_promoted=True,
        )

        assert dispatched == 0
        assert adapter.call_count == 0
        assert book.status == L2BookStatus.HOT
        assert not [
            record for record in journal.records
            if record[0] == "runtime.local_l2_hot_stale_rebuild"
        ]

    def test_sequence_gap_rebuild_evidence_uses_pre_transition_status(self):
        class Journal:
            def __init__(self):
                self.records = []

            def append(self, kind, payload):
                self.records.append((kind, payload))

        rt = LocalL2Runtime()
        journal = Journal()
        dp = LocalL2DataPlane(l2_runtime=rt, journal=journal)
        book = rt.ensure_book("binance", "BTCUSDT")
        book.pool = L2PoolAssignment.HOT_EXEC
        book.apply_snapshot(
            [PriceLevel(100.0, 1.0)],
            [PriceLevel(101.0, 1.0)],
            sequence=100,
            now_ms=1000,
        )
        book.transition_to_bootstrapping(1000)
        book.transition_to_hot()

        dp.ingest_external_update(
            LocalL2Update(
                venue="binance",
                symbol="BTCUSDT",
                bids=[PriceLevel(99.0, 1.0)],
                asks=[],
                first_sequence=106,
                sequence=110,
                previous_sequence=105,
                previous_sequence_present=True,
                event_time_ms=2000,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=2000,
        )

        payload = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_sequence_gap_rebuild"
        ][0]
        assert payload["status_before"] == "hot"
        assert payload["expected_sequence"] == 101
        assert payload["incoming_first_sequence"] == 106
        assert payload["policy_buffer_cap"] == 4096

    def test_hot_binance_pu_mismatch_rebuilds_even_when_range_overlaps(self):
        class Journal:
            def __init__(self):
                self.records = []

            def append(self, kind, payload):
                self.records.append((kind, payload))

        rt = LocalL2Runtime()
        journal = Journal()
        dp = LocalL2DataPlane(l2_runtime=rt, journal=journal)
        book = rt.ensure_book("binance", "BTCUSDT")
        book.pool = L2PoolAssignment.HOT_EXEC
        book.apply_snapshot(
            [PriceLevel(100.0, 1.0)],
            [PriceLevel(101.0, 1.0)],
            sequence=100,
            now_ms=1000,
        )
        book.transition_to_bootstrapping(1000)
        book.transition_to_hot()

        dp.ingest_external_update(
            LocalL2Update(
                venue="binance",
                symbol="BTCUSDT",
                bids=[PriceLevel(99.0, 1.0)],
                asks=[],
                first_sequence=101,
                sequence=102,
                previous_sequence=99,
                previous_sequence_present=True,
                event_time_ms=2000,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=2000,
        )

        assert book.status == L2BookStatus.REBUILDING
        assert "previous_link_mismatch" in book.fault_reason
        payload = [
            payload for kind, payload in journal.records
            if kind == "runtime.local_l2_sequence_gap_rebuild"
        ][0]
        assert payload["raw_U"] == 101
        assert payload["raw_u"] == 102
        assert payload["raw_pu"] == 99
        assert payload["expected_previous_sequence"] == 100
        assert payload["status_before"] == "hot"
        assert payload["status_after"] == "rebuilding"

    def test_degraded_transition_preserves_error(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.transition_to_degraded("connection timeout")
        assert book.status == L2BookStatus.DEGRADED
        assert book.last_error == "connection timeout"
        assert book.fault_reason == "connection timeout"
        assert book.degrade_count == 1

    def test_multiple_degradations_lead_to_suspended(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.max_consecutive_degradations = 2
        book.transition_to_degraded("error 1")
        assert book.status == L2BookStatus.DEGRADED
        book.transition_to_degraded("error 2")
        assert book.status == L2BookStatus.SUSPENDED
        assert book.degrade_count == 2

    def test_rebuilding_transition(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.transition_to_degraded("error")
        book.transition_to_rebuilding()
        assert book.status == L2BookStatus.REBUILDING

    def test_suspended_transition(self):
        book = LocalL2Book(venue="binance", symbol="BTCUSDT")
        book.transition_to_suspended("budget")
        assert book.status == L2BookStatus.SUSPENDED
        assert book.fault_reason == "budget"


# ---------------------------------------------------------------------------
# Runtime fault handling
# ---------------------------------------------------------------------------


class TestRuntimeFaultHandling:
    """Runtime faults must be classified and tracked correctly."""

    def test_runtime_records_fault_metrics(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")

        rt.handle_runtime_failure(
            "binance", "BTCUSDT",
            RuntimeFaultKind.CHECKSUM_MISMATCH, "bad checksum", now_ms=1000,
        )
        assert rt.metrics.rebuild_total == 1

        rt.handle_runtime_failure(
            "binance", "BTCUSDT",
            RuntimeFaultKind.RATE_LIMITED, "rate limit hit", now_ms=1000,
        )
        assert rt.metrics.runtime_rate_limited_total == 1

        rt.handle_runtime_failure(
            "binance", "BTCUSDT",
            RuntimeFaultKind.TRANSPORT_FAILURE, "timeout", now_ms=1000,
        )
        assert rt.metrics.runtime_transport_failure_total == 1

    def test_runtime_suspends_book_on_budget(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.handle_runtime_failure(
            "binance", "BTCUSDT",
            RuntimeFaultKind.BUDGET_SUSPENDED, "exceeded max hot", now_ms=1000,
        )
        assert rt.metrics.budget_suspended_total == 1
        book = rt.get_book("binance", "BTCUSDT")
        assert book.status == L2BookStatus.SUSPENDED

    def test_diagnostics_snapshot(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        diag = rt.diagnostics_snapshot()
        assert diag["book_count"] == 1
        assert "active_books" in diag
        assert "bootstrapping_books" in diag
        assert "rebuild_total" in diag


# ---------------------------------------------------------------------------
# Passive order semantics (V1 compatibility)
# ---------------------------------------------------------------------------


class TestPassiveOrderSemantics:
    """Passive order types must be semantically compatible with V1."""

    def test_unknown_state_exists(self):
        from lightfee.core.domain import PassiveOrderState
        assert hasattr(PassiveOrderState, 'UNKNOWN')
        state = PassiveOrderState.UNKNOWN
        assert state.is_active() is True
        assert state.is_terminal() is False

    def test_terminal_states(self):
        from lightfee.core.domain import PassiveOrderState
        assert PassiveOrderState.FILLED.is_terminal() is True
        assert PassiveOrderState.CANCELED.is_terminal() is True
        assert PassiveOrderState.REJECTED.is_terminal() is True
        assert PassiveOrderState.EXPIRED.is_terminal() is True

    def test_active_states(self):
        from lightfee.core.domain import PassiveOrderState
        assert PassiveOrderState.OPEN.is_active() is True
        assert PassiveOrderState.PARTIALLY_FILLED.is_active() is True

    def test_client_order_id_optional_in_ack(self):
        from lightfee.core.domain import PassiveOrderAck, Venue, Side
        ack = PassiveOrderAck(
            venue=Venue.BINANCE,
            symbol="BTCUSDT",
            side=Side.BUY,
            order_id="order123",
            # client_order_id not provided
        )
        assert ack.client_order_id == ""
        assert ack.state == PassiveOrderAck.__dataclass_fields__["state"].default  # UNKNOWN

    def test_client_order_id_optional_in_progress(self):
        from lightfee.core.domain import PassiveOrderProgress, Venue, Side, PassiveOrderState
        progress = PassiveOrderProgress(
            venue=Venue.BINANCE,
            symbol="BTCUSDT",
            side=Side.BUY,
            order_id="order123",
            # client_order_id not provided
        )
        assert progress.client_order_id == ""

    def test_resting_quantity_in_ack(self):
        from lightfee.core.domain import PassiveOrderAck, Venue, Side
        ack = PassiveOrderAck(
            venue=Venue.BINANCE,
            symbol="BTCUSDT",
            side=Side.BUY,
            order_id="order123",
            quantity=1.5,
        )
        assert ack.resting_quantity == 1.5
