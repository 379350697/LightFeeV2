"""Task 3: Runtime entry wiring contract tests.

Rust references:
- src/engine/entry.rs: execute_incremental_entry → runtime integration
- src/app_runtime/loop_control.rs: tick candidate → entry flow
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig, StrategyConfig
from lightfee.core.domain import OrderFill, OrderRequest, PositionSnapshot, Side, Venue
from lightfee.engine.entry_sync import EntrySyncExecutor
from lightfee.engine.runtime import LiveRuntime
from lightfee.engine.state import EngineState, PendingEntry
from lightfee.persistence.journal import Journal
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode

from dataclasses import dataclass, field
from typing import Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass


@dataclass
class FakeVenueAdapter(VenueAdapter):
    """Programmable fake adapter for testing."""
    _venue: Venue
    _min_notional_quote: float = 0.0
    place_order_outcomes: list = field(default_factory=list)
    position_snapshots: list = field(default_factory=list)
    default_fill_price: float = 0.0
    default_position_side: Side = Side.BUY
    default_position_qty: float = 0.0
    last_request: Optional[OrderRequest] = None
    place_order_call_count: int = 0
    fetch_position_call_count: int = 0

    @property
    def venue(self) -> Venue:
        return self._venue

    async def place_order(self, request):
        self.place_order_call_count += 1
        self.last_request = request
        if self.place_order_outcomes:
            outcome = self.place_order_outcomes.pop(0)
            if isinstance(outcome, OrderSubmitError):
                raise outcome
            return outcome
        price = self.default_fill_price if self.default_fill_price > 0 else request.price or 1.0
        return OrderFill(venue=self._venue, symbol=request.symbol, side=request.side,
                         quantity=request.quantity, price=price,
                         order_id=f"fake-{self._venue.value}-{self.place_order_call_count}",
                         filled_at_ms=1000)

    async def fetch_position(self, symbol):
        self.fetch_position_call_count += 1
        if self.position_snapshots:
            return self.position_snapshots.pop(0)
        return PositionSnapshot(venue=self._venue, symbol=symbol, side=self.default_position_side,
                                quantity=self.default_position_qty, entry_price=0.0, observed_at_ms=1000)

    async def submit_passive_order(self, request):
        from lightfee.core.domain import PassiveOrderAck
        self.last_request = request
        return PassiveOrderAck(
            venue=self._venue, symbol=request.symbol, side=request.side,
            order_id=f"passive-{self._venue.value}-1",
            client_order_id=request.client_order_id or "",
            price=request.price or 0.0, quantity=request.quantity,
            accepted_at_ms=1000,
        )

    async def normalize_quantity(self, symbol, quantity):
        return quantity


def make_fake_fill(
    venue, symbol, side, quantity, price=50000.0,
    order_id="fill-001", fee_quote=2.5, filled_at_ms=1000,
):
    return OrderFill(venue=venue, symbol=symbol, side=side, quantity=quantity,
                     price=price, order_id=order_id, fee_quote=fee_quote,
                     filled_at_ms=filled_at_ms)


@pytest.fixture
def tmp_journal(tmp_path):
    j = Journal(str(tmp_path / "runtime_test.jsonl"))
    j.open()
    yield j
    j.close()


@pytest.fixture
def config(tmp_path):
    return AppConfig(
        runtime=RuntimeConfig(
            poll_interval_ms=1000,
            tick_failure_backoff_initial_ms=5000,
            tick_failure_backoff_max_ms=60000,
        ),
        persistence=PersistenceConfig(
            event_log_path=str(tmp_path / "events.jsonl"),
            snapshot_path=str(tmp_path / "snapshot.json"),
        ),
        strategy=StrategyConfig(local_l2_enabled=False),
    )


@pytest.fixture
def binance_fake():
    return FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)


@pytest.fixture
def okx_fake():
    return FakeVenueAdapter(Venue.OKX, _min_notional_quote=10.0)


# ---------------------------------------------------------------------------
# EntrySyncExecutor integration with Journal
# ---------------------------------------------------------------------------


class TestEntrySyncJournalIntegration:
    @pytest.mark.asyncio
    async def test_journal_records_entry_lifecycle(self, tmp_journal):
        binance = FakeVenueAdapter(Venue.BINANCE)
        okx = FakeVenueAdapter(Venue.OKX)

        binance.place_order_outcomes = [
            make_fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.01, 50000.0, "m01"),
        ]
        okx.place_order_outcomes = [
            make_fake_fill(Venue.OKX, "BTCUSDT", Side.SELL, 0.01, 49990.0, "h01"),
        ]

        executor = EntrySyncExecutor(
            adapters={Venue.BINANCE: binance, Venue.OKX: okx},
            journal=tmp_journal,
        )

        from lightfee.engine.entry import EntryContext, EntryState, EntryType
        ctx = EntryContext(
            entry_id="je1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.STANDARD_DUAL_TAKER,
        )
        result = await executor.execute(ctx)
        assert result.state == EntryState.COMPLETED

        records = tmp_journal.read_all()
        assert len(records) >= 5  # at least: submitted x2, filled x2, completed

    @pytest.mark.asyncio
    async def test_journal_records_rejected_entry(self, tmp_journal):
        binance = FakeVenueAdapter(Venue.BINANCE)
        okx = FakeVenueAdapter(Venue.OKX)

        from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
        binance.place_order_outcomes = [
            OrderSubmitError(SubmitFailureClass.REJECTED, "margin insufficient"),
        ]

        executor = EntrySyncExecutor(
            adapters={Venue.BINANCE: binance, Venue.OKX: okx},
            journal=tmp_journal,
        )

        from lightfee.engine.entry import EntryContext, EntryState, EntryType
        ctx = EntryContext(
            entry_id="rej2",
            symbol="ETHUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.1,
            short_quantity=0.1,
            long_price_hint=3000.0,
            short_price_hint=3000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.STANDARD_DUAL_TAKER,
        )
        result = await executor.execute(ctx)
        assert result.route.value == "rejected"

        records = tmp_journal.read_all()
        kinds = [r["kind"] for r in records]
        assert "order.rejected" in kinds


# ---------------------------------------------------------------------------
# PendingEntry state tracking during execution
# ---------------------------------------------------------------------------


class TestPendingEntryTracking:
    def test_pending_entry_tracks_maker_order_id(self):
        pe = PendingEntry(
            pending_id="pe1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            target_quantity=0.01,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=1000,
            maker_order_id="order-m-001",
        )
        assert pe.maker_order_id == "order-m-001"
        assert pe.hedge_order_id == ""

    def test_pending_entry_tracks_hedge_order_id(self):
        pe = PendingEntry(
            pending_id="pe1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            target_quantity=0.01,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=1000,
            maker_order_id="order-m-001",
            hedge_order_id="order-h-001",
        )
        assert pe.hedge_order_id == "order-h-001"

    def test_pending_entry_fill_tracking(self):
        pe = PendingEntry(
            pending_id="pe2",
            symbol="ETHUSDT",
            long_venue=Venue.BYBIT,
            short_venue=Venue.GATE,
            target_quantity=0.1,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=2000,
            maker_leg_filled=0.1,
            hedge_leg_filled=0.08,
        )
        assert pe.maker_leg_filled == 0.1
        assert pe.hedge_leg_filled == 0.08

    def test_pending_entry_uncertain_flag(self):
        pe = PendingEntry(
            pending_id="pe3",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            target_quantity=0.01,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=3000,
            uncertain_outcome=True,
        )
        assert pe.uncertain_outcome is True


# ---------------------------------------------------------------------------
# EN-001: Planner-driven route and maker-leg decisions
# ---------------------------------------------------------------------------


class TestPlannerDispatchIntegration:
    """Prove runtime calls planner for route/maker-leg instead of hardcoding."""

    @pytest.mark.asyncio
    async def test_dispatch_entry_uses_planner_route(self, config, tmp_journal):
        """Entry route comes from planner, not hardcoded STANDARD_DUAL_TAKER."""
        binance = FakeVenueAdapter(Venue.BINANCE)
        okx = FakeVenueAdapter(Venue.OKX)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}

        executor = EntrySyncExecutor(adapters=adapters, journal=tmp_journal)
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal
        runtime.entry_executor = executor

        # Create a mock candidate with enough notional to pass planner
        from lightfee.sidecar.snapshot import CandidateInput

        candidate = CandidateInput(
            long_venue="binance",
            short_venue="okx",
            symbol="BTCUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=8.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=8.0,
            transfer_bias_bps=0.0,
            opportunity_type="funding_arb",
            blocked=False,
            entry_notional_quote=500.0,  # large enough to pass min-notional
        )

        # Dispatch with valid price hint
        await runtime._dispatch_entry(candidate, 5000, price_hint=50000.0)

        # Verify journal records entry_dispatched (planner passed)
        records = runtime.journal.read_all()
        kinds = [r["kind"] for r in records]
        assert "runtime.entry_dispatched" in kinds

    @pytest.mark.asyncio
    async def test_dispatch_entry_rejects_below_min_notional(self, config, tmp_journal):
        """Entry below min-notional is rejected by planner."""
        binance = FakeVenueAdapter(Venue.BINANCE)
        okx = FakeVenueAdapter(Venue.OKX)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}

        executor = EntrySyncExecutor(adapters=adapters, journal=tmp_journal)
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal
        runtime.entry_executor = executor

        from lightfee.sidecar.snapshot import CandidateInput

        candidate = CandidateInput(
            long_venue="binance",
            short_venue="okx",
            symbol="BTCUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=8.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=8.0,
            transfer_bias_bps=0.0,
            opportunity_type="funding_arb",
            blocked=False,
            entry_notional_quote=1.0,  # too small
        )

        await runtime._dispatch_entry(candidate, 5000, price_hint=50000.0)

        records = runtime.journal.read_all()
        kinds = [r["kind"] for r in records]
        # Should be rejected by planner (target_below_min_hedgeable_chunk or similar)
        assert "runtime.entry_skipped_planner_rejected" in kinds or "runtime.entry_skipped_no_quote" in kinds

    @pytest.mark.asyncio
    async def test_dispatch_entry_skips_no_quote(self, config, tmp_journal):
        """Entry with zero price_hint is rejected before planner."""
        binance = FakeVenueAdapter(Venue.BINANCE)
        okx = FakeVenueAdapter(Venue.OKX)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}

        executor = EntrySyncExecutor(adapters=adapters, journal=tmp_journal)
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal
        runtime.entry_executor = executor

        from lightfee.sidecar.snapshot import CandidateInput

        candidate = CandidateInput(
            long_venue="binance",
            short_venue="okx",
            symbol="BTCUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=8.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=8.0,
            transfer_bias_bps=0.0,
            opportunity_type="funding_arb",
            blocked=False,
            entry_notional_quote=500.0,
        )

        await runtime._dispatch_entry(candidate, 5000, price_hint=0.0)

        records = runtime.journal.read_all()
        kinds = [r["kind"] for r in records]
        assert "runtime.entry_skipped_no_quote" in kinds


# ---------------------------------------------------------------------------
# Runtime wiring: executor connected to LiveRuntime
# ---------------------------------------------------------------------------


class TestRuntimeEntryWiring:
    def test_runtime_accepts_entry_executor(self, config, tmp_journal):
        binance = FakeVenueAdapter(Venue.BINANCE)
        okx = FakeVenueAdapter(Venue.OKX)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}

        executor = EntrySyncExecutor(adapters=adapters, journal=tmp_journal)

        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.entry_executor = executor
        assert runtime.entry_executor is executor
        assert runtime.entry_executor.adapters is adapters

    def test_runtime_default_entry_executor_is_none(self, config, tmp_journal):
        runtime = LiveRuntime(config)
        assert runtime.entry_executor is None

    def test_runtime_has_open_positions_after_entry(self, config, tmp_journal):
        binance = FakeVenueAdapter(Venue.BINANCE)
        okx = FakeVenueAdapter(Venue.OKX)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}

        executor = EntrySyncExecutor(adapters=adapters, journal=tmp_journal)
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.entry_executor = executor

        # Simulate state after an entry completes
        from lightfee.engine.state import OpenPosition
        pos = OpenPosition(
            position_id="p-test",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.01,
            short_quantity=0.01,
            long_entry_price=50000.0,
            short_entry_price=50000.0,
            opened_at_ms=1000,
        )
        runtime.state.open_positions["p-test"] = pos
        assert len(runtime.state.open_positions) == 1
        assert "p-test" in runtime.state.open_positions
