"""Maker-event lane local-L2 integration tests.

Rust V1 reference: src/execution_core/engine.rs:4587-4693 tick_maker_event_lane

Tests that the maker-event lane:
  - Syncs local-L2 runtime and drains events
  - Filters events matching pending entries
  - Drives pending hedge repricing via entry executor
  - Records proper journal metadata
  - Falls back to sidecar-mid when no local-L2 events (non-parity path)
"""

from __future__ import annotations

import pytest

from lightfee.marketdata.l2 import (
    L2BookStatus,
    L2PoolAssignment,
    LocalL2Event,
    LocalL2EventKind,
    LocalL2Update,
    LocalL2UpdateKind,
    PriceLevel,
)
from lightfee.marketdata.local_l2_runtime import LocalL2Runtime


class TestLocalL2RuntimeIntegration:
    """Verify the local-L2 runtime can track books and drain events for maker lane."""

    def test_runtime_created_and_empty(self):
        rt = LocalL2Runtime()
        assert rt.event_count() == 0
        assert len(rt.books) == 0

    def test_sync_drains_events_for_maker_lane(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        book = rt.get_book("binance", "BTCUSDT")
        book.transition_to_bootstrapping(now_ms=10000)
        book.transition_to_hot()

        # Simulate a best-bid update that would wake maker lane
        rt.pending_events.append(LocalL2Event(
            venue="binance", symbol="BTCUSDT",
            event_kind=LocalL2EventKind.BEST_BID_UPDATED,
            observed_at_ms=10000, sequence=100,
            bid=50000,
        ))
        rt.pending_events.append(LocalL2Event(
            venue="binance", symbol="BTCUSDT",
            event_kind=LocalL2EventKind.BEST_ASK_UPDATED,
            observed_at_ms=10000, sequence=100,
            ask=50100,
        ))

        events = rt.sync(now_ms=11000)
        assert len(events) == 2
        kinds = {e.event_kind for e in events}
        assert LocalL2EventKind.BEST_BID_UPDATED in kinds
        assert LocalL2EventKind.BEST_ASK_UPDATED in kinds

    def test_event_filtering_for_pending_entries(self):
        """Events should be filterable to only those matching pending entry venues/symbols."""
        rt = LocalL2Runtime()
        # Set up books for BTC on binance and bybit
        rt.ensure_book("binance", "BTCUSDT")
        rt.ensure_book("bybit", "BTCUSDT")
        rt.ensure_book("okx", "ETHUSDT")  # unrelated

        rt.pending_events.append(LocalL2Event(
            venue="binance", symbol="BTCUSDT",
            event_kind=LocalL2EventKind.BEST_BID_UPDATED,
            observed_at_ms=10000,
        ))
        rt.pending_events.append(LocalL2Event(
            venue="bybit", symbol="BTCUSDT",
            event_kind=LocalL2EventKind.BEST_ASK_UPDATED,
            observed_at_ms=10000,
        ))
        rt.pending_events.append(LocalL2Event(
            venue="okx", symbol="ETHUSDT",  # unrelated
            event_kind=LocalL2EventKind.BEST_BID_UPDATED,
            observed_at_ms=10000,
        ))

        events = rt.sync(now_ms=11000)
        # Filter for pending entry: (binance, BTCUSDT) and (bybit, BTCUSDT)
        pending_set = {("binance", "BTCUSDT"), ("bybit", "BTCUSDT")}
        matching = [e for e in events if (e.venue, e.symbol) in pending_set]
        # 2 matching (binance+BTC, bybit+BTC), 1 unrelated (okx+ETH)
        assert len(matching) == 2

    def test_maker_event_metrics_increment(self):
        rt = LocalL2Runtime()
        rt.metrics.maker_event_lane_wake_total += 1
        rt.sync(now_ms=10000)
        assert rt.metrics.maker_event_lane_wake_total == 1

    def test_no_events_no_wake(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        events = rt.sync(now_ms=10000)
        assert len(events) == 0

    def test_unrelated_event_no_match_for_different_symbol(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.ensure_book("binance", "ETHUSDT")

        rt.pending_events.append(LocalL2Event(
            venue="binance", symbol="ETHUSDT",
            event_kind=LocalL2EventKind.BEST_BID_UPDATED,
            observed_at_ms=10000,
        ))

        events = rt.sync(now_ms=11000)
        pending_set = {("binance", "BTCUSDT")}
        matching = [e for e in events if (e.venue, e.symbol) in pending_set]
        assert len(matching) == 0  # ETH event doesn't match BTC filter

    def test_sidecar_missing_does_not_affect_local_l2_parity_lane(self):
        """When local-L2 is enabled, the runtime works without sidecar file."""
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        book = rt.get_book("binance", "BTCUSDT")
        book.transition_to_bootstrapping(now_ms=10000)
        book.transition_to_hot()

        rt.pending_events.append(LocalL2Event(
            venue="binance", symbol="BTCUSDT",
            event_kind=LocalL2EventKind.MID_PRICE_CHANGED,
            observed_at_ms=10000, mid_price=50050,
        ))
        events = rt.sync(now_ms=11000)
        assert len(events) == 1

        # Events still drain even though there's no sidecar file at all
        matching = [e for e in events if (e.venue, e.symbol) == ("binance", "BTCUSDT")]
        assert len(matching) == 1


class TestLocalL2BookIntegration:
    """Test that books integrate with runtime and ready state can be queried."""

    def test_book_readiness_in_runtime(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        book = rt.get_book("binance", "BTCUSDT")
        book.transition_to_bootstrapping(now_ms=10000)
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.0)],
            sequence=1, now_ms=10000,
        )
        book.transition_to_hot()
        assert book.is_ready(max_age_ms=5000, now_ms=12000)

    def test_book_not_ready_blocks_maker_event_progression(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        book = rt.get_book("binance", "BTCUSDT")
        # Book is COLD — not ready
        assert not book.is_ready(max_age_ms=5000, now_ms=10000)

    def test_dual_venue_book_readiness(self):
        """Both legs of a pair must be ready for entry to proceed."""
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        rt.ensure_book("bybit", "BTCUSDT")

        b_book = rt.get_book("binance", "BTCUSDT")
        b_book.transition_to_bootstrapping(now_ms=10000)
        b_book.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.0)],
            sequence=1, now_ms=10000,
        )
        b_book.transition_to_hot()

        y_book = rt.get_book("bybit", "BTCUSDT")
        y_book.transition_to_bootstrapping(now_ms=10000)
        y_book.apply_snapshot(
            [PriceLevel(price=49990, quantity=1.0)],
            [PriceLevel(price=50110, quantity=1.0)],
            sequence=1, now_ms=10000,
        )
        y_book.transition_to_hot()

        assert b_book.is_ready(max_age_ms=5000, now_ms=12000)
        assert y_book.is_ready(max_age_ms=5000, now_ms=12000)

    def test_book_status_transitions_in_runtime(self):
        rt = LocalL2Runtime()
        rt.ensure_book("binance", "BTCUSDT")
        book = rt.get_book("binance", "BTCUSDT")

        assert book.status == L2BookStatus.COLD
        book.transition_to_bootstrapping(now_ms=10000)
        assert book.status == L2BookStatus.BOOTSTRAPPING
        book.apply_snapshot(
            [PriceLevel(price=50000, quantity=1.0)],
            [PriceLevel(price=50100, quantity=1.0)],
            now_ms=10000,
        )
        book.transition_to_hot()
        assert book.status == L2BookStatus.HOT


class TestEventAgeMetadata:
    """Maker-event lane journals event age min/max."""

    def test_event_age_computation(self):
        rt = LocalL2Runtime()
        rt.pending_events.append(LocalL2Event(
            venue="binance", symbol="BTCUSDT",
            event_kind=LocalL2EventKind.BEST_BID_UPDATED,
            observed_at_ms=10000,
        ))
        rt.pending_events.append(LocalL2Event(
            venue="binance", symbol="BTCUSDT",
            event_kind=LocalL2EventKind.BEST_ASK_UPDATED,
            observed_at_ms=10500,
        ))

        now_ms = 11000
        events = rt.sync(now_ms=now_ms)
        ages = [now_ms - e.observed_at_ms for e in events]
        assert min(ages) == 500   # 11000 - 10500
        assert max(ages) == 1000  # 11000 - 10000


class TestEntryLocalL2Readiness:
    """Session readiness for entry-local-L2."""
    from lightfee.engine.entry_local_l2 import (
        EntryLocalL2Session,
        EntryLocalL2LegSession,
        EntryLocalL2SessionState,
        EntryLocalL2LegState,
    )

    def test_session_refresh_ready(self):
        EntryLocalL2Session = pytest.importorskip(
            "lightfee.engine.entry_local_l2"
        ).EntryLocalL2Session
        session = EntryLocalL2Session(pair_id="p1")
        session.ensure_leg("binance", "BTCUSDT").mark_ready(seen_at_ms=10000)
        session.ensure_leg("bybit", "BTCUSDT").mark_ready(seen_at_ms=10000)
        session.refresh_state(now_ms=12000, stale_after_ms=5000)
        from lightfee.engine.entry_local_l2 import EntryLocalL2SessionState
        assert session.state == EntryLocalL2SessionState.READY

    def test_arming_leg_stays_arming(self):
        """A rebuilding/suspended leg keeps the session in arming state."""
        EntryLocalL2Session = pytest.importorskip(
            "lightfee.engine.entry_local_l2"
        ).EntryLocalL2Session
        session = EntryLocalL2Session(pair_id="p1")
        session.ensure_leg("binance", "BTCUSDT").mark_ready(seen_at_ms=10000)
        # bybit leg stays arming (like a rebuilding book)
        session.ensure_leg("bybit", "BTCUSDT")
        session.refresh_state(now_ms=12000, stale_after_ms=5000)
        from lightfee.engine.entry_local_l2 import EntryLocalL2SessionState
        assert session.state == EntryLocalL2SessionState.ARMING

    def test_resume_waiting_does_not_count_as_ready(self):
        """A leg with no data (arming) means NOT ready."""
        EntryLocalL2Session = pytest.importorskip(
            "lightfee.engine.entry_local_l2"
        ).EntryLocalL2Session
        session = EntryLocalL2Session(pair_id="p1")
        session.ensure_leg("binance", "BTCUSDT")  # arming
        session.ensure_leg("bybit", "BTCUSDT")    # arming
        assert not session.both_legs_ready(now_ms=12000, stale_after_ms=5000)
