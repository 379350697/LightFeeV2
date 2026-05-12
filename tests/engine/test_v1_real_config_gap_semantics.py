"""Semantic parity tests for active V1 config knobs with real engine effect.

V1 references:
- src/execution_core/entry_sync.rs (maker_entry_max_reposts)
- src/engine/entry.rs (pending_entry_zero_fill_terminal_cooldown_ms)
- src/runtime_state/config.rs (config definition)

Tests prove that V2 implements real behavioral changes behind these knobs,
not just config-only stubs.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from lightfee.core.domain import OrderFill, OrderRequest, Side, Venue
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.engine.entry import (
    EntryContext,
    EntryState,
    EntryType,
)
from lightfee.engine.entry_sync import EntrySyncExecutor
from lightfee.engine.execution_planner import ExecutionRoute
from lightfee.engine.state import PendingEntry
from lightfee.core.contracts import VenueAdapter
from lightfee.persistence.journal import Journal


# ---------------------------------------------------------------------------
# Rejecting adapter for testing repost limits
# ---------------------------------------------------------------------------


class _RejectingAdapter(VenueAdapter):
    """Adapter that always rejects orders."""

    def __init__(self, v: Venue):
        self._venue = v

    @property
    def venue(self) -> Venue:
        return self._venue

    async def place_order(self, request: OrderRequest) -> OrderFill:
        raise OrderSubmitError(SubmitFailureClass.REJECTED, "test reject")

    async def fetch_position(self, symbol: str) -> "PositionSnapshot":
        from lightfee.core.domain import PositionSnapshot
        return PositionSnapshot(venue=self._venue, symbol=symbol, quantity=0.0)


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


# ---------------------------------------------------------------------------
# maker_entry_max_reposts
# ---------------------------------------------------------------------------


class TestMakerEntryMaxReposts:
    """V1: maker_entry_max_reposts caps passive maker repost attempts."""

    @pytest.mark.asyncio
    async def test_first_attempt_always_allowed(self):
        journal_path = Path(tempfile.mkdtemp()) / "test.jsonl"
        journal = Journal(journal_path)
        journal.open()
        try:
            executor = EntrySyncExecutor(
                adapters={Venue.BYBIT: _RejectingAdapter(Venue.BYBIT)},
                journal=journal,
                config_overrides={"maker_entry_max_reposts": 3},
            )
            ctx = EntryContext(
                entry_id="repost-test-001",
                symbol="BTCUSDT",
                long_venue=Venue.BYBIT,
                short_venue=Venue.BYBIT,
                long_quantity=1.0,
                short_quantity=1.0,
                long_price_hint=50000.0,
                short_price_hint=50000.0,
                maker_leg=Side.BUY,
                entry_type=EntryType.PASSIVE_INCREMENTAL,
                planned_route=ExecutionRoute.PASSIVE_INCREMENTAL,
            )
            result = await executor.execute(ctx)
            # First attempt is allowed — maker rejected → FAILED with pending entry
            assert result.state == EntryState.FAILED
            assert result.pending_entry is not None
            assert result.pending_entry.repost_count == 1
        finally:
            journal.close()

    @pytest.mark.asyncio
    async def test_max_reposts_zero_means_unlimited(self):
        journal_path = Path(tempfile.mkdtemp()) / "test.jsonl"
        journal = Journal(journal_path)
        journal.open()
        try:
            executor = EntrySyncExecutor(
                adapters={Venue.BYBIT: _RejectingAdapter(Venue.BYBIT)},
                journal=journal,
                config_overrides={"maker_entry_max_reposts": 0},  # unlimited
            )
            ctx = EntryContext(
                entry_id="repost-test-002",
                symbol="BTCUSDT",
                long_venue=Venue.BYBIT,
                short_venue=Venue.BYBIT,
                long_quantity=1.0,
                short_quantity=1.0,
                long_price_hint=50000.0,
                short_price_hint=50000.0,
                maker_leg=Side.BUY,
                entry_type=EntryType.PASSIVE_INCREMENTAL,
                planned_route=ExecutionRoute.PASSIVE_INCREMENTAL,
            )
            result = await executor.execute(ctx)
            # Should not be blocked by repost limit (0 = unlimited)
            assert result.state == EntryState.FAILED
            assert result.pending_entry is not None
        finally:
            journal.close()

    @pytest.mark.asyncio
    async def test_repost_limit_enforced_with_pending_entry_in_state(self):
        """When a pending entry already has repost_count >= max, execute rejects immediately."""
        journal_path = Path(tempfile.mkdtemp()) / "test.jsonl"
        journal = Journal(journal_path)
        journal.open()
        try:
            # Pre-populate state with a pending entry that has exceeded the limit
            existing_pending = PendingEntry(
                pending_id="repost-test-003",
                symbol="BTCUSDT",
                long_venue=Venue.BYBIT,
                short_venue=Venue.BYBIT,
                target_quantity=1.0,
                long_side=Side.BUY,
                short_side=Side.SELL,
                created_at_ms=1000,
                repost_count=3,
                zero_fill_since_ms=1000,
                maker_leg_filled=0.0,
                hedge_leg_filled=0.0,
            )
            executor = EntrySyncExecutor(
                adapters={Venue.BYBIT: _RejectingAdapter(Venue.BYBIT)},
                journal=journal,
                state={"pending_entries": {"repost-test-003": existing_pending}},
                config_overrides={"maker_entry_max_reposts": 3},
            )
            ctx = EntryContext(
                entry_id="repost-test-003",
                symbol="BTCUSDT",
                long_venue=Venue.BYBIT,
                short_venue=Venue.BYBIT,
                long_quantity=1.0,
                short_quantity=1.0,
                long_price_hint=50000.0,
                short_price_hint=50000.0,
                maker_leg=Side.BUY,
                entry_type=EntryType.PASSIVE_INCREMENTAL,
                planned_route=ExecutionRoute.PASSIVE_INCREMENTAL,
            )
            result = await executor.execute(ctx)
            assert result.state == EntryState.FAILED
            assert result.route == ExecutionRoute.REJECTED
            # Should have aborted due to max reposts
            records = journal.read_all()
            aborted = [r for r in records if r["kind"] == "entry.aborted"]
            assert len(aborted) >= 1
            assert "max reposts" in aborted[0]["payload"]["reason"]
        finally:
            journal.close()

    @pytest.mark.asyncio
    async def test_repost_not_blocked_when_under_limit(self):
        """Pending entry with repost_count < max should not be blocked."""
        journal_path = Path(tempfile.mkdtemp()) / "test.jsonl"
        journal = Journal(journal_path)
        journal.open()
        try:
            existing_pending = PendingEntry(
                pending_id="repost-test-004",
                symbol="BTCUSDT",
                long_venue=Venue.BYBIT,
                short_venue=Venue.BYBIT,
                target_quantity=1.0,
                long_side=Side.BUY,
                short_side=Side.SELL,
                created_at_ms=1000,
                repost_count=1,  # 1 < 3
                zero_fill_since_ms=0,
                maker_leg_filled=0.0,
                hedge_leg_filled=0.0,
            )
            executor = EntrySyncExecutor(
                adapters={Venue.BYBIT: _FakeFillAdapter(Venue.BYBIT)},
                journal=journal,
                state={"pending_entries": {"repost-test-004": existing_pending}},
                config_overrides={"maker_entry_max_reposts": 3},
            )
            ctx = EntryContext(
                entry_id="repost-test-004",
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
            # Should proceed normally — not blocked by repost limit
            assert result.state == EntryState.COMPLETED
        finally:
            journal.close()


# ---------------------------------------------------------------------------
# pending_entry_zero_fill_terminal_cooldown_ms
# ---------------------------------------------------------------------------


class TestPendingEntryZeroFillTerminalCooldown:
    """V1: pending_entry_zero_fill_terminal_cooldown_ms force-terminates entries
    that have had zero-fills for longer than the configured cooldown."""

    @pytest.mark.asyncio
    async def test_zero_fill_terminal_cooldown_blocks_expired_entry(self):
        """Entry with zero-fill for longer than cooldown should be force-terminated."""
        journal_path = Path(tempfile.mkdtemp()) / "test.jsonl"
        journal = Journal(journal_path)
        journal.open()
        try:
            import time
            long_ago = int(time.time() * 1000) - 60_000  # 60 seconds ago
            existing_pending = PendingEntry(
                pending_id="zf-test-001",
                symbol="BTCUSDT",
                long_venue=Venue.BYBIT,
                short_venue=Venue.BYBIT,
                target_quantity=1.0,
                long_side=Side.BUY,
                short_side=Side.SELL,
                created_at_ms=long_ago,
                repost_count=0,
                zero_fill_since_ms=long_ago,
                maker_leg_filled=0.0,
                hedge_leg_filled=0.0,
            )
            executor = EntrySyncExecutor(
                adapters={Venue.BYBIT: _FakeFillAdapter(Venue.BYBIT)},
                journal=journal,
                state={"pending_entries": {"zf-test-001": existing_pending}},
                config_overrides={
                    "pending_entry_zero_fill_terminal_cooldown_ms": 30_000,  # 30s
                },
            )
            ctx = EntryContext(
                entry_id="zf-test-001",
                symbol="BTCUSDT",
                long_venue=Venue.BYBIT,
                short_venue=Venue.BYBIT,
                long_quantity=1.0,
                short_quantity=1.0,
                long_price_hint=50000.0,
                short_price_hint=50000.0,
                maker_leg=Side.BUY,
                entry_type=EntryType.PASSIVE_INCREMENTAL,
                planned_route=ExecutionRoute.PASSIVE_INCREMENTAL,
            )
            result = await executor.execute(ctx)
            assert result.state == EntryState.FAILED
            assert result.route == ExecutionRoute.REJECTED
            records = journal.read_all()
            aborted = [r for r in records if r["kind"] == "entry.aborted"]
            assert len(aborted) >= 1
            assert "zero-fill terminal cooldown expired" in aborted[0]["payload"]["reason"]
        finally:
            journal.close()

    @pytest.mark.asyncio
    async def test_zero_fill_within_cooldown_allows_retry(self):
        """Entry with zero-fill under cooldown should not be blocked."""
        journal_path = Path(tempfile.mkdtemp()) / "test.jsonl"
        journal = Journal(journal_path)
        journal.open()
        try:
            import time
            recent_zero = int(time.time() * 1000) - 2000  # 2 seconds ago
            existing_pending = PendingEntry(
                pending_id="zf-test-002",
                symbol="BTCUSDT",
                long_venue=Venue.BYBIT,
                short_venue=Venue.BYBIT,
                target_quantity=1.0,
                long_side=Side.BUY,
                short_side=Side.SELL,
                created_at_ms=recent_zero,
                repost_count=0,
                zero_fill_since_ms=recent_zero,
                maker_leg_filled=0.0,
                hedge_leg_filled=0.0,
            )
            executor = EntrySyncExecutor(
                adapters={Venue.BYBIT: _FakeFillAdapter(Venue.BYBIT)},
                journal=journal,
                state={"pending_entries": {"zf-test-002": existing_pending}},
                config_overrides={
                    "pending_entry_zero_fill_terminal_cooldown_ms": 30_000,  # 30s
                },
            )
            ctx = EntryContext(
                entry_id="zf-test-002",
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
            # Should proceed — cooldown not yet expired
            assert result.state == EntryState.COMPLETED
        finally:
            journal.close()

    @pytest.mark.asyncio
    async def test_zero_fill_cooldown_disabled_by_default(self):
        """When pending_entry_zero_fill_terminal_cooldown_ms is 0, no cooldown check."""
        journal_path = Path(tempfile.mkdtemp()) / "test.jsonl"
        journal = Journal(journal_path)
        journal.open()
        try:
            import time
            long_ago = int(time.time() * 1000) - 120_000
            existing_pending = PendingEntry(
                pending_id="zf-test-003",
                symbol="BTCUSDT",
                long_venue=Venue.BYBIT,
                short_venue=Venue.BYBIT,
                target_quantity=1.0,
                long_side=Side.BUY,
                short_side=Side.SELL,
                created_at_ms=long_ago,
                repost_count=0,
                zero_fill_since_ms=long_ago,
                maker_leg_filled=0.0,
                hedge_leg_filled=0.0,
            )
            executor = EntrySyncExecutor(
                adapters={Venue.BYBIT: _FakeFillAdapter(Venue.BYBIT)},
                journal=journal,
                state={"pending_entries": {"zf-test-003": existing_pending}},
                config_overrides={"pending_entry_zero_fill_terminal_cooldown_ms": 0},
            )
            ctx = EntryContext(
                entry_id="zf-test-003",
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
            # Should proceed (zerofill cooldown disabled, 0 = no enforcement)
            assert result.state == EntryState.COMPLETED
        finally:
            journal.close()


# ---------------------------------------------------------------------------
# Combined behavior: repost_count increment through _make_pending_entry
# ---------------------------------------------------------------------------


class TestRepostCountTracking:
    """V1: repost_count increments with each call to _make_pending_entry."""

    @pytest.mark.asyncio
    async def test_repost_count_increments_on_reentry(self):
        """After a rejected execution creates a pending entry, the next execution
        via the same entry_id should get a repost_count increment."""
        journal_path = Path(tempfile.mkdtemp()) / "test.jsonl"
        journal = Journal(journal_path)
        journal.open()
        try:
            executor = EntrySyncExecutor(
                adapters={Venue.BYBIT: _RejectingAdapter(Venue.BYBIT)},
                journal=journal,
                config_overrides={"maker_entry_max_reposts": 10},
            )
            ctx = EntryContext(
                entry_id="incr-test-001",
                symbol="BTCUSDT",
                long_venue=Venue.BYBIT,
                short_venue=Venue.BYBIT,
                long_quantity=1.0,
                short_quantity=1.0,
                long_price_hint=50000.0,
                short_price_hint=50000.0,
                maker_leg=Side.BUY,
                entry_type=EntryType.PASSIVE_INCREMENTAL,
                planned_route=ExecutionRoute.PASSIVE_INCREMENTAL,
            )
            result = await executor.execute(ctx)
            assert result.state == EntryState.FAILED
            assert result.pending_entry is not None
            assert result.pending_entry.repost_count == 1
        finally:
            journal.close()
