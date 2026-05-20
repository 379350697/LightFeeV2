"""V1 PrivateWsState unit tests — matching src/live/private_ws.rs test suite."""

from __future__ import annotations

import asyncio
import time

import pytest

from lightfee.core.domain import (
    OrderFill,
    OrderFillReconciliation,
    PassiveOrderState,
    Side,
    Venue,
)
from lightfee.marketdata.private_ws import (
    CumulativeOrderProgress,
    PrivateOrderUpdate,
    PrivateWsState,
    enrich_fill_from_private,
    lookup_or_wait_private_order,
    lookup_or_wait_private_order_progress,
    lookup_or_wait_private_order_progress_after,
    merge_passive_progress_sources,
    resolve_cumulative_order_progress,
)


def _now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# PrivateOrderUpdate → CumulativeOrderProgress
# ---------------------------------------------------------------------------


class TestCumulativeOrderProgress:
    def test_from_private_basic(self):
        update = PrivateOrderUpdate(
            symbol="ETHUSDT",
            order_id="order-1",
            client_order_id="entry-1",
            filled_quantity=0.01,
            average_price=2140.0,
            fee_quote=0.001,
            updated_at_ms=20,
        )
        progress = CumulativeOrderProgress.from_private(update)
        assert progress.order_id == "order-1"
        assert progress.client_order_id == "entry-1"
        assert progress.cumulative_quantity == 0.01
        assert progress.average_price == 2140.0
        assert progress.fee_quote == 0.001
        assert progress.last_fill_at_ms == 20

    def test_from_private_zero_fill_skips_fields(self):
        update = PrivateOrderUpdate(
            symbol="ETHUSDT",
            order_id="order-1",
            filled_quantity=0.0,
            average_price=2140.0,
            fee_quote=0.001,
            updated_at_ms=20,
        )
        progress = CumulativeOrderProgress.from_private(update)
        assert progress.cumulative_quantity == 0.0
        assert progress.average_price is None
        assert progress.fee_quote is None
        assert progress.last_fill_at_ms is None

    def test_from_private_carries_state(self):
        update = PrivateOrderUpdate(
            symbol="ETHUSDT",
            order_id="order-1",
            filled_quantity=0.0,
            state=PassiveOrderState.CANCELED,
            updated_at_ms=20,
        )
        progress = CumulativeOrderProgress.from_private(update)
        assert progress.state == PassiveOrderState.CANCELED


# ---------------------------------------------------------------------------
# PrivateWsState — order cache
# ---------------------------------------------------------------------------


class TestPrivateWsStateOrderCache:
    @pytest.mark.asyncio
    async def test_keeps_newest_order_update(self):
        state = PrivateWsState(max_order_entries=4)
        await state.record_order(PrivateOrderUpdate(
            symbol="ETHUSDT", order_id="old-order",
            client_order_id="entry-1", filled_quantity=0.01,
            average_price=2140.0, fee_quote=0.001, updated_at_ms=10,
        ))
        await state.record_order(PrivateOrderUpdate(
            symbol="ETHUSDT", order_id="new-order",
            client_order_id="entry-1", filled_quantity=0.011,
            average_price=2141.5, fee_quote=0.002, updated_at_ms=20,
        ))
        update = state.order_by_client_id("entry-1")
        assert update is not None
        assert update.order_id == "new-order"
        assert update.filled_quantity == 0.011

    @pytest.mark.asyncio
    async def test_rejects_stale_updates(self):
        state = PrivateWsState()
        await state.record_order(PrivateOrderUpdate(
            symbol="ETHUSDT", order_id="order-1",
            client_order_id="entry-1", filled_quantity=0.02,
            updated_at_ms=20,
        ))
        await state.record_order(PrivateOrderUpdate(
            symbol="ETHUSDT", order_id="order-1",
            client_order_id="entry-1", filled_quantity=0.01,
            updated_at_ms=10,
        ))
        update = state.order_by_client_id("entry-1")
        assert update.filled_quantity == 0.02

    @pytest.mark.asyncio
    async def test_dual_index_coheres(self):
        state = PrivateWsState()
        await state.record_order(PrivateOrderUpdate(
            symbol="ETHUSDT", order_id="order-1",
            client_order_id="entry-1", filled_quantity=0.01,
            updated_at_ms=10,
        ))
        by_client = state.order_by_client_id("entry-1")
        by_order = state.order_by_order_id("order-1")
        assert by_client is not None
        assert by_order is not None
        assert by_client.order_id == by_order.order_id

    @pytest.mark.asyncio
    async def test_mismatched_ids_return_no_progress(self):
        state = PrivateWsState()
        await state.record_order(PrivateOrderUpdate(
            symbol="ETHUSDT", order_id="order-1",
            client_order_id="entry-1", updated_at_ms=10,
        ))
        await state.record_order(PrivateOrderUpdate(
            symbol="ETHUSDT", order_id="order-2",
            client_order_id="entry-2", updated_at_ms=20,
        ))
        progress = state.order_progress_if_fresh(
            client_order_id="entry-1", order_id="order-2",
        )
        assert progress is None

    @pytest.mark.asyncio
    async def test_caps_order_cache_evicts_oldest(self):
        state = PrivateWsState(max_order_entries=2)
        await state.record_order(PrivateOrderUpdate(
            symbol="ETHUSDT", order_id="order-1",
            client_order_id="entry-1", filled_quantity=0.01,
            updated_at_ms=10,
        ))
        await state.record_order(PrivateOrderUpdate(
            symbol="ETHUSDT", order_id="order-2",
            client_order_id="entry-2", filled_quantity=0.01,
            updated_at_ms=20,
        ))
        await state.record_order(PrivateOrderUpdate(
            symbol="ETHUSDT", order_id="order-3",
            client_order_id="entry-3", filled_quantity=0.01,
            updated_at_ms=30,
        ))
        assert state.order_by_client_id("entry-1") is None
        assert state.order_by_order_id("order-1") is None
        assert state.order_by_client_id("entry-2") is not None
        assert state.order_by_client_id("entry-3") is not None


# ---------------------------------------------------------------------------
# PrivateWsState — position cache
# ---------------------------------------------------------------------------


class TestPrivateWsStatePositionCache:
    @pytest.mark.asyncio
    async def test_tracks_latest_position_only(self):
        state = PrivateWsState()
        await state.update_position("ETHUSDT", 0.02, 20)
        await state.update_position("ETHUSDT", 0.01, 10)
        pos = state.position("ETHUSDT")
        assert pos is not None
        assert pos.size == 0.02
        assert pos.updated_at_ms == 20

    @pytest.mark.asyncio
    async def test_rejects_stale_positions_when_freshness_ttl_expires(self):
        state = PrivateWsState()
        await state.update_position("ETHUSDT", 0.75, 1000)

        fresh = state.position_if_fresh("ETHUSDT", 5000, 5999)
        stale = state.position_if_fresh("ETHUSDT", 5000, 6001)

        assert fresh is not None
        assert fresh.size == 0.75
        assert stale is None

    @pytest.mark.asyncio
    async def test_positions_if_fresh_sorted(self):
        state = PrivateWsState()
        await state.update_position("BTCUSDT", 1.0, 100)
        await state.update_position("ETHUSDT", 0.5, 100)

        positions = state.positions_if_fresh(0, 0)
        symbols = [p.symbol for p in positions]
        assert symbols == sorted(symbols)


# ---------------------------------------------------------------------------
# PrivateWsState — connection health
# ---------------------------------------------------------------------------


class TestPrivateWsStateHealth:
    def test_health_starts_healthy(self):
        state = PrivateWsState()
        health = state.connection_health()
        assert not health.is_unhealthy()

    def test_repeated_failures_make_unhealthy(self):
        state = PrivateWsState()
        for i in range(5):
            state.record_connection_failure(i * 1000, 5, f"error {i}")
        assert state.connection_health().is_unhealthy()

    def test_success_clears_unhealthy(self):
        state = PrivateWsState()
        for i in range(5):
            state.record_connection_failure(i * 1000, 5, f"error {i}")
        state.record_connection_success(6000)
        assert not state.connection_health().is_unhealthy()


# ---------------------------------------------------------------------------
# PrivateWsState — worker lifecycle
# ---------------------------------------------------------------------------


class TestPrivateWsStateWorkers:
    @pytest.mark.asyncio
    async def test_worker_count_prunes_finished(self):
        state = PrivateWsState()
        async def _done():
            pass
        task = asyncio.create_task(_done())
        state.push_worker(task)
        await asyncio.sleep(0.05)
        assert state.worker_count() == 0

    @pytest.mark.asyncio
    async def test_push_worker_replaces_existing(self):
        state = PrivateWsState()
        async def _forever():
            await asyncio.sleep(3600)
        t1 = asyncio.create_task(_forever())
        state.push_worker(t1)
        assert state.worker_count() == 1
        t2 = asyncio.create_task(_forever())
        state.push_worker(t2)
        await asyncio.sleep(0.01)
        assert t1.cancelled()
        assert state.worker_count() == 1
        state.abort_workers()
        assert state.worker_count() == 0

    @pytest.mark.asyncio
    async def test_abort_workers_cancels_all(self):
        state = PrivateWsState()
        async def _forever():
            await asyncio.sleep(3600)
        tasks = [asyncio.create_task(_forever()) for _ in range(3)]
        for t in tasks:
            state.push_worker(t)
        state.abort_workers()
        await asyncio.sleep(0.01)
        assert state.worker_count() == 0
        assert all(t.cancelled() for t in tasks)


# ---------------------------------------------------------------------------
# enrich_fill_from_private
# ---------------------------------------------------------------------------


class TestEnrichFill:
    def test_enriches_from_private_update(self):
        fill = OrderFill(
            venue=Venue.BYBIT, symbol="ETHUSDT", side=Side.BUY,
            quantity=0.01, price=2140.0, fee_quote=0.0,
            order_id="rest-order", filled_at_ms=5,
        )
        update = PrivateOrderUpdate(
            symbol="ETHUSDT", order_id="new-order",
            filled_quantity=0.011, average_price=2141.5,
            fee_quote=0.002, updated_at_ms=20,
        )
        enriched = enrich_fill_from_private(fill, update)
        assert enriched.order_id == "new-order"
        assert enriched.quantity == 0.011
        assert enriched.price == 2141.5
        assert enriched.fee_quote == 0.002
        assert enriched.filled_at_ms == 20

    def test_skips_enrich_with_zero_fill(self):
        fill = OrderFill(
            venue=Venue.BYBIT, symbol="ETHUSDT", side=Side.BUY,
            quantity=0.01, price=2140.0, order_id="rest-order",
        )
        update = PrivateOrderUpdate(
            symbol="ETHUSDT", order_id="private-order",
            filled_quantity=0.0, updated_at_ms=0,
        )
        enriched = enrich_fill_from_private(fill, update)
        assert enriched.order_id == "rest-order"
        assert enriched.quantity == 0.01


# ---------------------------------------------------------------------------
# Async wait primitives
# ---------------------------------------------------------------------------


class TestLookupOrWait:
    @pytest.mark.asyncio
    async def test_returns_none_immediately_when_wait_disabled(self):
        state = PrivateWsState()

        async def _delayed_update():
            await asyncio.sleep(0.05)
            await state.record_order(PrivateOrderUpdate(
                symbol="ETHUSDT", order_id="order-1",
                client_order_id="entry-1", filled_quantity=0.01,
                updated_at_ms=15,
            ))

        task = asyncio.create_task(_delayed_update())
        started = time.monotonic()
        update = await lookup_or_wait_private_order(state, "entry-1", None, 0)
        elapsed = time.monotonic() - started
        assert update is None
        assert elapsed < 0.1
        task.cancel()

    @pytest.mark.asyncio
    async def test_captures_late_update_with_short_wait(self):
        state = PrivateWsState()

        async def _delayed_update():
            await asyncio.sleep(0.05)
            await state.record_order(PrivateOrderUpdate(
                symbol="ETHUSDT", order_id="order-2",
                client_order_id="entry-2", filled_quantity=0.011,
                average_price=2141.0, fee_quote=0.002,
                updated_at_ms=15,
            ))

        asyncio.create_task(_delayed_update())
        update = await lookup_or_wait_private_order(state, "entry-2", None, 500)
        assert update is not None
        assert update.order_id == "order-2"

    @pytest.mark.asyncio
    async def test_progress_after_waits_for_newer_update(self):
        state = PrivateWsState()
        await state.record_order(PrivateOrderUpdate(
            symbol="ETHUSDT", order_id="order-4",
            client_order_id="entry-4", filled_quantity=0.01,
            average_price=2140.0, fee_quote=0.001,
            updated_at_ms=10,
        ))

        async def _delayed_update():
            await asyncio.sleep(0.05)
            await state.record_order(PrivateOrderUpdate(
                symbol="ETHUSDT", order_id="order-4",
                client_order_id="entry-4", filled_quantity=0.012,
                average_price=2141.0, fee_quote=0.002,
                updated_at_ms=20,
            ))

        asyncio.create_task(_delayed_update())
        progress = await lookup_or_wait_private_order_progress_after(
            state, "entry-4", "order-4", after_updated_at_ms=10, wait_ms=500,
        )
        assert progress is not None
        assert progress.cumulative_quantity == 0.012
        assert progress.updated_at_ms == 20


# ---------------------------------------------------------------------------
# resolve_cumulative_order_progress
# ---------------------------------------------------------------------------


class TestResolveCumulativeOrderProgress:
    def test_prefers_reconciliation_over_private(self):
        private = CumulativeOrderProgress.from_private(PrivateOrderUpdate(
            symbol="ETHUSDT", order_id="private-order",
            client_order_id="entry-1", filled_quantity=0.011,
            average_price=2140.5, fee_quote=0.001, updated_at_ms=20,
        ))
        reconciliation = CumulativeOrderProgress.from_reconciliation(
            OrderFillReconciliation(
                venue=Venue.BYBIT, symbol="ETHUSDT", side=Side.BUY,
                order_id="reconciled-order", client_order_id="entry-1",
                quantity=0.013, average_price=2142.0, fee_quote=0.002,
                filled_at_ms=30,
            )
        )
        snapshot = CumulativeOrderProgress.from_position_snapshot(
            "order-rest", "entry-1", 0.012, 2141.0, None, 25,
        )
        resolved = resolve_cumulative_order_progress([reconciliation, snapshot, private])
        assert resolved is not None
        assert resolved.cumulative_quantity == 0.013
        assert resolved.average_price == 2142.0

    def test_merge_prefers_private_first(self):
        from lightfee.core.domain import PassiveOrderProgress
        private = CumulativeOrderProgress.from_private(PrivateOrderUpdate(
            symbol="ETHUSDT", order_id="pvt", client_order_id="e1",
            filled_quantity=0.01, average_price=2140.0, updated_at_ms=100,
        ))
        rest = CumulativeOrderProgress(
            order_id="rest", cumulative_quantity=0.0, updated_at_ms=90,
        )
        # V1 call signature: (detail_progress, reconciliation, private_progress)
        result = merge_passive_progress_sources(rest, None, private)
        assert result is not None
        assert result.cumulative_quantity == 0.01
