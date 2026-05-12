"""Semantic parity tests for review observability chain.

V1 references:
- src/engine/entry.rs
- src/runtime_state/config.rs (review_observability_enabled)
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from lightfee.core.domain import OrderFill, OrderRequest, Side, Venue
from lightfee.engine.entry import (
    EntryContext,
    EntryState,
    EntryType,
    build_open_position,
    generate_review_id,
)
from lightfee.engine.entry_sync import EntrySyncExecutor
from lightfee.engine.execution_planner import ExecutionRoute
from lightfee.core.contracts import VenueAdapter
from lightfee.persistence.journal import Journal


# ---------------------------------------------------------------------------
# Review ID generation
# ---------------------------------------------------------------------------


class TestReviewIdGeneration:
    def test_generate_review_id_has_correct_format(self):
        rid = generate_review_id()
        assert rid.startswith("rev-")
        assert len(rid) == 16  # "rev-" + 12 hex chars

    def test_generate_review_id_is_unique(self):
        ids = {generate_review_id() for _ in range(100)}
        assert len(ids) == 100


# ---------------------------------------------------------------------------
# Review ID in OpenPosition
# ---------------------------------------------------------------------------


class TestReviewIdInOpenPosition:
    def test_build_open_position_stores_review_id_when_provided(self):
        ctx = EntryContext(
            entry_id="test-001",
            symbol="BTCUSDT",
            long_venue=Venue.BYBIT,
            short_venue=Venue.BYBIT,
            long_quantity=1.0,
            short_quantity=1.0,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        maker_fill = OrderFill(
            venue=Venue.BYBIT,
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=1.0,
            price=50000.0,
            order_id="maker-1",
        )
        hedge_fill = OrderFill(
            venue=Venue.BYBIT,
            symbol="BTCUSDT",
            side=Side.SELL,
            quantity=1.0,
            price=50000.0,
            order_id="hedge-1",
        )
        position = build_open_position(ctx, maker_fill, hedge_fill, 1000, review_id="rev-abc123")
        assert position.review_id == "rev-abc123"

    def test_build_open_position_review_id_is_none_when_not_provided(self):
        ctx = EntryContext(
            entry_id="test-002",
            symbol="BTCUSDT",
            long_venue=Venue.BYBIT,
            short_venue=Venue.BYBIT,
            long_quantity=1.0,
            short_quantity=1.0,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
        )
        maker_fill = OrderFill(
            venue=Venue.BYBIT,
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=1.0,
            price=50000.0,
            order_id="maker-1",
        )
        hedge_fill = OrderFill(
            venue=Venue.BYBIT,
            symbol="BTCUSDT",
            side=Side.SELL,
            quantity=1.0,
            price=50000.0,
            order_id="hedge-1",
        )
        position = build_open_position(ctx, maker_fill, hedge_fill, 1000)
        assert position.review_id is None


# ---------------------------------------------------------------------------
# Review observability in EntrySyncExecutor
# ---------------------------------------------------------------------------
# We use a simple fake adapter that reports order success.
# The purpose is to verify that review_id is generated and propagated when
# review_observability_enabled is True.


class _FakeFillAdapter(VenueAdapter):
    """Adapter that always reports a successful fill."""

    def __init__(self, v: Venue):
        self._venue = v
        self._order_counter = 0

    @property
    def venue(self) -> Venue:
        return self._venue

    async def place_order(self, request: OrderRequest) -> OrderFill:
        self._order_counter += 1
        return OrderFill(
            venue=self._venue,
            symbol=request.symbol,
            side=request.side,
            quantity=request.quantity,
            price=request.price or 50000.0,
            order_id=f"order-{self._order_counter}",
            filled_at_ms=2000,
            fee_quote=0.5,
        )

    async def fetch_position(self, symbol: str) -> "PositionSnapshot":
        from lightfee.core.domain import PositionSnapshot
        return PositionSnapshot(venue=self._venue, symbol=symbol, quantity=0.0)


class TestEntrySyncReviewObservability:
    """V1: review_observability_enabled gates review-id generation and propagation."""

    @pytest.mark.asyncio
    async def test_review_observability_enabled_generates_review_id(self):
        journal_path = Path(tempfile.mkdtemp()) / "test.jsonl"
        journal = Journal(journal_path)
        journal.open()
        try:
            executor = EntrySyncExecutor(
                adapters={Venue.BYBIT: _FakeFillAdapter(Venue.BYBIT)},
                journal=journal,
                config_overrides={"review_observability_enabled": True},
            )
            ctx = EntryContext(
                entry_id="rev-test-001",
                symbol="BTCUSDT",
                long_venue=Venue.BYBIT,
                short_venue=Venue.BYBIT,
                long_quantity=1.0,
                short_quantity=1.0,
                long_price_hint=50000.0,
                short_price_hint=50000.0,
                maker_leg=Side.BUY,
                entry_type=EntryType.STANDARD_DUAL_TAKER,
                planned_route=ExecutionRoute.PASSIVE_INCREMENTAL,
            )
            result = await executor.execute(ctx)
            assert result.state == EntryState.COMPLETED
            assert result.open_position is not None
            assert result.open_position.review_id is not None
            assert result.open_position.review_id.startswith("rev-")
            # Check journal events
            records = journal.read_all()
            kinds = {r["kind"] for r in records}
            assert "review.assigned" in kinds
            # entry.opened should contain review_id
            opened = [r for r in records if r["kind"] == "entry.opened"]
            assert len(opened) == 1
            assert opened[0]["payload"].get("review_id", "") != ""
        finally:
            journal.close()

    @pytest.mark.asyncio
    async def test_review_observability_disabled_no_review_id(self):
        journal_path = Path(tempfile.mkdtemp()) / "test.jsonl"
        journal = Journal(journal_path)
        journal.open()
        try:
            executor = EntrySyncExecutor(
                adapters={Venue.BYBIT: _FakeFillAdapter(Venue.BYBIT)},
                journal=journal,
                config_overrides={"review_observability_enabled": False},
            )
            ctx = EntryContext(
                entry_id="rev-test-002",
                symbol="BTCUSDT",
                long_venue=Venue.BYBIT,
                short_venue=Venue.BYBIT,
                long_quantity=1.0,
                short_quantity=1.0,
                long_price_hint=50000.0,
                short_price_hint=50000.0,
                maker_leg=Side.BUY,
                entry_type=EntryType.STANDARD_DUAL_TAKER,
                planned_route=ExecutionRoute.PASSIVE_INCREMENTAL,
            )
            result = await executor.execute(ctx)
            assert result.state == EntryState.COMPLETED
            assert result.open_position is not None
            assert result.open_position.review_id is None
            records = journal.read_all()
            kinds = {r["kind"] for r in records}
            assert "review.assigned" not in kinds
        finally:
            journal.close()

    @pytest.mark.asyncio
    async def test_default_disables_review_observability(self):
        journal_path = Path(tempfile.mkdtemp()) / "test.jsonl"
        journal = Journal(journal_path)
        journal.open()
        try:
            executor = EntrySyncExecutor(
                adapters={Venue.BYBIT: _FakeFillAdapter(Venue.BYBIT)},
                journal=journal,
            )
            ctx = EntryContext(
                entry_id="rev-test-003",
                symbol="BTCUSDT",
                long_venue=Venue.BYBIT,
                short_venue=Venue.BYBIT,
                long_quantity=1.0,
                short_quantity=1.0,
                long_price_hint=50000.0,
                short_price_hint=50000.0,
                maker_leg=Side.BUY,
                entry_type=EntryType.STANDARD_DUAL_TAKER,
                planned_route=ExecutionRoute.PASSIVE_INCREMENTAL,
            )
            result = await executor.execute(ctx)
            assert result.state == EntryState.COMPLETED
            assert result.open_position is not None
            assert result.open_position.review_id is None
        finally:
            journal.close()
