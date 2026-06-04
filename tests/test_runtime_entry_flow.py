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
from lightfee.engine.entry import EntryState
from lightfee.engine.entry_sync import EntryExecutionResult, EntrySyncExecutor
from lightfee.engine.execution_planner import ExecutionRoute
from lightfee.engine.reconciliation import OrderReconciler
from lightfee.engine.runtime import LiveRuntime
from lightfee.engine.state import EngineState, OpenPosition, PendingEntry
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
    okx_base_quantity_step: float = 0.0
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

    @staticmethod
    def _install_hot_book(runtime, venue: str, symbol: str, *, bid: float, ask: float, observed_at_ms: int):
        from lightfee.marketdata.l2 import L2BookStatus, PriceLevel

        book = runtime.local_l2_runtime.ensure_book(venue, symbol)
        book.status = L2BookStatus.HOT
        book.bids = [PriceLevel(price=bid, quantity=10.0)]
        book.asks = [PriceLevel(price=ask, quantity=10.0)]
        book.observed_at_ms = observed_at_ms
        return book

    @staticmethod
    def _candidate(symbol: str = "BTCUSDT"):
        from lightfee.sidecar.snapshot import CandidateInput

        return CandidateInput(
            long_venue="binance",
            short_venue="okx",
            symbol=symbol,
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

    def test_untrusted_hyperliquid_transport_is_not_tradeable_for_selection(
        self, config, tmp_journal,
    ):
        from lightfee.sidecar.snapshot import CandidateInput

        hyperliquid = FakeVenueAdapter(Venue.HYPERLIQUID)
        hyperliquid.trading_capability_trusted = False
        binance = FakeVenueAdapter(Venue.BINANCE)
        runtime = LiveRuntime(
            config,
            venue_adapters={Venue.HYPERLIQUID: hyperliquid, Venue.BINANCE: binance},
        )
        runtime.journal = tmp_journal

        candidate = CandidateInput(
            long_venue="hyperliquid",
            short_venue="binance",
            symbol="SUPERUSDT",
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

        assert runtime._candidate_is_tradeable_for_selection(candidate) is False

    def test_binance_5022_gtx_reject_classified_as_post_only_would_take(self):
        assert LiveRuntime._entry_reject_is_post_only_would_take(
            "binance error code=-5022 GTX_ORDER_REJECT: Due to the order could not be executed as maker"
        ) is True

    @pytest.mark.asyncio
    async def test_post_only_gtx_reject_sets_pair_cooldown_without_pending(
        self, config, tmp_journal,
    ):
        config.strategy.local_l2_enabled = True
        config.strategy.entry_local_l2_book_stale_after_ms = 1000
        binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
        okx = FakeVenueAdapter(Venue.OKX, _min_notional_quote=10.0)
        binance.submit_passive_order = AsyncMock(
            side_effect=OrderSubmitError(
                SubmitFailureClass.REJECTED,
                "binance error code=-5022 GTX_ORDER_REJECT: "
                "Due to the order could not be executed as maker",
            )
        )
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal
        runtime.entry_executor = EntrySyncExecutor(adapters=adapters, journal=tmp_journal)
        self._install_hot_book(runtime, "binance", "BTCUSDT", bid=50000.0, ask=50010.0, observed_at_ms=5000)
        self._install_hot_book(runtime, "okx", "BTCUSDT", bid=49990.0, ask=50000.0, observed_at_ms=5000)

        candidate = self._candidate()

        assert await runtime._dispatch_entry(candidate, 5000, price_hint=50000.0) is True
        assert runtime.state.pending_entries == {}
        pair_key = ("BTCUSDT", "binance", "okx")
        assert runtime._zero_fill_cooldown_until_ms[pair_key] > 5000
        assert runtime._gate_zero_fill_cooldown(candidate, 5001)[0] is False
        records = tmp_journal.read_all()
        kinds = [record["kind"] for record in records]
        assert "runtime.entry_post_only_reject_cooldown" in kinds
        payload = [r["payload"] for r in records if r["kind"] == "runtime.entry_post_only_reject_cooldown"][-1]
        assert payload["venue"] == "binance"
        assert payload["price"] == 50000.0
        assert payload["best_bid"] == 50000.0
        assert payload["best_ask"] == 50010.0
        assert payload["freshness"] == "fresh"
        assert payload["cooldown_until_ms"] == payload["cooldown_until"]

    @pytest.mark.asyncio
    async def test_fresh_bbo_allows_post_only_maker_submit(self, config, tmp_journal):
        config.strategy.local_l2_enabled = True
        config.strategy.entry_local_l2_book_stale_after_ms = 1000
        binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
        okx = FakeVenueAdapter(Venue.OKX, _min_notional_quote=10.0)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal
        runtime.entry_executor = EntrySyncExecutor(adapters=adapters, journal=tmp_journal)
        self._install_hot_book(runtime, "binance", "BTCUSDT", bid=50000.0, ask=50010.0, observed_at_ms=5000)
        self._install_hot_book(runtime, "okx", "BTCUSDT", bid=49990.0, ask=50000.0, observed_at_ms=5000)

        assert await runtime._dispatch_entry(self._candidate(), 5000, price_hint=50000.0) is True

        assert binance.last_request is not None
        assert binance.last_request.post_only is True
        assert binance.last_request.price == 50000.0
        assert len(runtime.state.pending_entries) == 1

    @pytest.mark.asyncio
    async def test_final_gate_blocks_fresh_bbo_with_excessive_leg_skew(
        self, config, tmp_journal,
    ):
        config.runtime.mode = "live"
        config.strategy.local_l2_enabled = True
        config.strategy.entry_local_l2_book_stale_after_ms = 1000
        config.strategy.entry_final_gate_max_skew_ms = 100
        binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
        okx = FakeVenueAdapter(Venue.OKX, _min_notional_quote=10.0)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal
        runtime.entry_executor = EntrySyncExecutor(adapters=adapters, journal=tmp_journal)
        self._install_hot_book(
            runtime, "binance", "BTCUSDT",
            bid=50000.0, ask=50010.0, observed_at_ms=5000,
        )
        self._install_hot_book(
            runtime, "okx", "BTCUSDT",
            bid=49990.0, ask=50000.0, observed_at_ms=4800,
        )

        dispatched = await runtime._dispatch_entry(
            self._candidate(),
            5000,
            price_hint=50000.0,
        )

        assert dispatched is False
        assert binance.last_request is None
        payload = [
            record["payload"]
            for record in tmp_journal.read_all()
            if record["kind"] == "runtime.entry_blocked_final_gate"
        ][-1]
        assert payload["reason"] == "execution_skew"
        assert payload["skew_ms"] == 200
        assert payload["max_skew_ms"] == 100
        assert payload["left_venue"] == "binance"
        assert payload["right_venue"] == "okx"

    @pytest.mark.asyncio
    async def test_ws_bbo_provider_dispatch_does_not_require_local_l2_books(
        self,
        config,
        tmp_journal,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.strategy.local_l2_enabled = True
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1500
        config.strategy.entry_local_l2_book_stale_after_ms = 1000
        binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
        okx = FakeVenueAdapter(Venue.OKX, _min_notional_quote=10.0)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal
        runtime.entry_executor = EntrySyncExecutor(adapters=adapters, journal=tmp_journal)
        candidate = self._candidate()
        for venue, bid, ask in (
            ("binance", 50000.0, 50010.0),
            ("okx", 49990.0, 50000.0),
        ):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="BTCUSDT",
                    bid=bid,
                    ask=ask,
                    observed_at_ms=5000,
                    received_at_ms=5000,
                    source=f"{venue}_bbo_ws",
                )
            )
        readiness = runtime.entry_readiness_provider.decide(candidate, 5000)
        assert readiness.allowed

        dispatched = await runtime._dispatch_entry(
            candidate,
            5000,
            price_hint=50000.0,
        )

        assert dispatched is True
        assert runtime.local_l2_runtime.get_book("binance", "BTCUSDT") is None
        assert binance.last_request is not None
        assert binance.last_request.post_only is True
        assert len(runtime.state.pending_entries) == 1

    @pytest.mark.asyncio
    async def test_ws_bbo_provider_dispatch_requires_selected_quote_lease(
        self,
        config,
        tmp_journal,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.runtime.mode = "live"
        config.strategy.local_l2_enabled = True
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1500
        binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
        okx = FakeVenueAdapter(Venue.OKX, _min_notional_quote=10.0)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal
        runtime.entry_executor = EntrySyncExecutor(adapters=adapters, journal=tmp_journal)
        for venue, bid, ask in (
            ("binance", 50000.0, 50010.0),
            ("okx", 49990.0, 50000.0),
        ):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="BTCUSDT",
                    bid=bid,
                    ask=ask,
                    observed_at_ms=5000,
                    received_at_ms=5000,
                    source=f"{venue}_bbo_ws",
                )
            )

        dispatched = await runtime._dispatch_entry(
            self._candidate(),
            5000,
            price_hint=50000.0,
        )

        assert dispatched is False
        assert binance.last_request is None
        payload = [
            record["payload"]
            for record in tmp_journal.read_all()
            if record["kind"] == "runtime.entry_blocked_quote_lease"
        ][-1]
        assert payload["reason"] == "missing_quote_lease"
        assert payload["provider"] == "ws_bbo_quote_lease"

    @pytest.mark.asyncio
    async def test_ws_bbo_provider_dispatch_uses_selected_quote_lease_prices(
        self,
        config,
        tmp_journal,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.runtime.mode = "live"
        config.strategy.local_l2_enabled = True
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1500
        binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
        okx = FakeVenueAdapter(Venue.OKX, _min_notional_quote=10.0)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal
        runtime.entry_executor = EntrySyncExecutor(adapters=adapters, journal=tmp_journal)
        candidate = self._candidate()
        for venue, bid, ask in (
            ("binance", 50000.0, 50010.0),
            ("okx", 49990.0, 50000.0),
        ):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="BTCUSDT",
                    bid=bid,
                    ask=ask,
                    observed_at_ms=5000,
                    received_at_ms=5000,
                    source=f"{venue}_bbo_ws",
                )
            )
        readiness = runtime.entry_readiness_provider.decide(candidate, 5000)
        assert readiness.allowed

        dispatched = await runtime._dispatch_entry(
            candidate,
            5000,
            price_hint=12345.0,
        )

        assert dispatched is True
        assert binance.last_request is not None
        assert binance.last_request.post_only is True
        assert binance.last_request.price == 50000.0

    @pytest.mark.asyncio
    async def test_ws_bbo_provider_dispatch_refreshes_expired_quote_lease(
        self,
        config,
        tmp_journal,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.runtime.mode = "live"
        config.runtime.max_market_age_ms = 30_000
        config.strategy.local_l2_enabled = True
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1500
        binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
        okx = FakeVenueAdapter(Venue.OKX, _min_notional_quote=10.0)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal
        runtime.entry_executor = EntrySyncExecutor(adapters=adapters, journal=tmp_journal)
        candidate = self._candidate()
        for venue, bid, ask in (
            ("binance", 50000.0, 50010.0),
            ("okx", 49990.0, 50000.0),
        ):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="BTCUSDT",
                    bid=bid,
                    ask=ask,
                    observed_at_ms=5000,
                    received_at_ms=5000,
                    source=f"{venue}_bbo_ws",
                )
            )
        readiness = runtime.entry_readiness_provider.decide(candidate, 5000)
        assert readiness.allowed

        for venue, bid, ask in (
            ("binance", 50020.0, 50030.0),
            ("okx", 50005.0, 50015.0),
        ):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="BTCUSDT",
                    bid=bid,
                    ask=ask,
                    observed_at_ms=7001,
                    received_at_ms=7001,
                    source=f"{venue}_bbo_ws",
                )
            )

        dispatched = await runtime._dispatch_entry(
            candidate,
            7001,
            price_hint=12345.0,
        )

        assert dispatched is True
        assert binance.last_request is not None
        assert binance.last_request.post_only is True
        assert binance.last_request.price == 50020.0
        blocked = [
            record for record in tmp_journal.read_all()
            if record["kind"] == "runtime.entry_blocked_quote_lease"
        ]
        assert blocked == []

    @pytest.mark.asyncio
    async def test_ws_bbo_post_only_guard_uses_quote_lease_age_budget(
        self,
        config,
        tmp_journal,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.runtime.mode = "live"
        config.runtime.max_market_age_ms = 3000
        config.strategy.local_l2_enabled = True
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1500
        runtime = LiveRuntime(config)
        runtime.journal = tmp_journal
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=50000.0,
                ask=50010.0,
                observed_at_ms=5000,
                received_at_ms=5000,
                source="binance_bbo_ws",
            )
        )

        ok, reason, payload = runtime._post_only_maker_bbo_guard(
            venue=Venue.BINANCE,
            symbol="BTCUSDT",
            side=Side.BUY,
            price=50000.0,
            now_ms=7001,
        )

        assert ok is False
        assert reason == "stale_bbo"
        assert payload["stale_after_ms"] == 1500

    @pytest.mark.asyncio
    async def test_ws_bbo_provider_dispatch_blocks_stale_post_only_quote(
        self,
        config,
        tmp_journal,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.strategy.local_l2_enabled = True
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1500
        config.strategy.entry_local_l2_book_stale_after_ms = 1000
        binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
        okx = FakeVenueAdapter(Venue.OKX, _min_notional_quote=10.0)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal
        runtime.entry_executor = EntrySyncExecutor(adapters=adapters, journal=tmp_journal)
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=50000.0,
                ask=50010.0,
                observed_at_ms=3000,
                received_at_ms=3000,
                source="binance_bbo_ws",
            )
        )

        dispatched = await runtime._dispatch_entry(
            self._candidate(),
            5000,
            price_hint=50000.0,
        )

        assert dispatched is False
        assert binance.last_request is None
        payload = [
            record["payload"]
            for record in tmp_journal.read_all()
            if record["kind"] == "runtime.entry_blocked_post_only_bbo"
        ][-1]
        assert payload["reason"] == "stale_bbo"
        assert payload["source"] == "ws_bbo_quote_lease"

    @pytest.mark.asyncio
    async def test_stale_bbo_blocks_post_only_maker_submit(self, config, tmp_journal):
        config.strategy.local_l2_enabled = True
        config.strategy.max_liquidity_snapshot_age_ms = 5000
        config.strategy.entry_local_l2_book_stale_after_ms = 1000
        binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
        okx = FakeVenueAdapter(Venue.OKX, _min_notional_quote=10.0)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal
        runtime.entry_executor = EntrySyncExecutor(adapters=adapters, journal=tmp_journal)
        self._install_hot_book(runtime, "binance", "BTCUSDT", bid=50000.0, ask=50010.0, observed_at_ms=3000)
        self._install_hot_book(runtime, "okx", "BTCUSDT", bid=49990.0, ask=50000.0, observed_at_ms=5000)

        assert await runtime._dispatch_entry(self._candidate(), 5000, price_hint=50000.0) is False

        assert binance.last_request is None
        assert runtime.state.pending_entries == {}
        kinds = [record["kind"] for record in tmp_journal.read_all()]
        assert "runtime.entry_blocked_post_only_bbo" in kinds

    @pytest.mark.asyncio
    async def test_crossing_bbo_blocks_post_only_maker_submit(self, config, tmp_journal):
        config.strategy.local_l2_enabled = True
        config.strategy.entry_local_l2_book_stale_after_ms = 1000
        binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
        okx = FakeVenueAdapter(Venue.OKX, _min_notional_quote=10.0)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal
        runtime.entry_executor = EntrySyncExecutor(adapters=adapters, journal=tmp_journal)
        self._install_hot_book(runtime, "binance", "BTCUSDT", bid=50000.0, ask=50010.0, observed_at_ms=5000)
        self._install_hot_book(runtime, "okx", "BTCUSDT", bid=49990.0, ask=50000.0, observed_at_ms=5000)

        assert await runtime._dispatch_entry(self._candidate(), 5000, price_hint=50010.0) is False

        assert binance.last_request is None
        payload = [
            record["payload"]
            for record in tmp_journal.read_all()
            if record["kind"] == "runtime.entry_blocked_post_only_bbo"
        ][-1]
        assert payload["reason"] == "would_cross_bbo"
        assert payload["would_cross"] is True

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
    async def test_dispatch_entry_aligns_okx_swap_quantity_to_contract_base_step(
        self, config, tmp_journal,
    ):
        binance = FakeVenueAdapter(Venue.BINANCE)
        okx = FakeVenueAdapter(Venue.OKX, okx_base_quantity_step=100.0)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal

        class CapturingExecutor:
            ctx = None

            async def execute(self, ctx):
                self.ctx = ctx
                return EntryExecutionResult(
                    route=ExecutionRoute.PASSIVE_INCREMENTAL,
                    state=EntryState.COMPLETED,
                )

        executor = CapturingExecutor()
        runtime.entry_executor = executor

        from lightfee.sidecar.snapshot import CandidateInput

        candidate = CandidateInput(
            long_venue="binance",
            short_venue="okx",
            symbol="UBUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=8.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=8.0,
            transfer_bias_bps=0.0,
            opportunity_type="funding_arb",
            blocked=False,
            entry_notional_quote=176.0,
        )

        dispatched = await runtime._dispatch_entry(candidate, 5000, price_hint=1.0)

        assert dispatched is True
        assert executor.ctx is not None
        assert executor.ctx.long_quantity == pytest.approx(100.0)
        assert executor.ctx.short_quantity == pytest.approx(100.0)
        selected = [
            r for r in runtime.journal.read_all()
            if r["kind"] == "execution.entry_selected"
        ][-1]
        assert selected["payload"]["quantity"] == pytest.approx(100.0)

    @pytest.mark.asyncio
    async def test_dispatch_entry_preserves_candidate_funding_semantics(self, config, tmp_journal):
        binance = FakeVenueAdapter(Venue.ASTER)
        bybit = FakeVenueAdapter(Venue.BYBIT)
        adapters = {Venue.ASTER: binance, Venue.BYBIT: bybit}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal

        class CapturingExecutor:
            ctx = None

            async def execute(self, ctx):
                self.ctx = ctx
                return EntryExecutionResult(
                    route=ExecutionRoute.PASSIVE_INCREMENTAL,
                    state=EntryState.COMPLETED,
                )

        executor = CapturingExecutor()
        runtime.entry_executor = executor

        from lightfee.sidecar.snapshot import CandidateInput

        first_funding_ms = 1780167600000
        second_funding_ms = 1780171200000
        candidate = CandidateInput(
            long_venue="aster",
            short_venue="bybit",
            symbol="MAGMAUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=7.45,
            expected_edge_bps=6.9,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=7.45,
            transfer_bias_bps=0.0,
            opportunity_type="staggered",
            blocked=False,
            entry_notional_quote=500.0,
            funding_timestamp_ms=first_funding_ms,
            first_funding_timestamp_ms=first_funding_ms,
            long_funding_timestamp_ms=first_funding_ms,
            short_funding_timestamp_ms=second_funding_ms,
            second_funding_timestamp_ms=second_funding_ms,
            first_funding_leg="long",
        )

        dispatched = await runtime._dispatch_entry(candidate, 1780163908797, price_hint=0.275)

        assert dispatched is True
        assert executor.ctx is not None
        assert executor.ctx.opportunity_type == "staggered"
        assert executor.ctx.funding_timestamp_ms == first_funding_ms
        assert executor.ctx.first_funding_timestamp_ms == first_funding_ms
        assert executor.ctx.long_funding_timestamp_ms == first_funding_ms
        assert executor.ctx.short_funding_timestamp_ms == second_funding_ms
        assert executor.ctx.second_funding_timestamp_ms == second_funding_ms
        assert executor.ctx.first_funding_leg == "long"
        assert executor.ctx.funding_edge_bps_entry == pytest.approx(7.45)
        assert executor.ctx.total_funding_edge_bps_entry == pytest.approx(7.45)
        assert executor.ctx.expected_edge_bps_entry == pytest.approx(6.9)

        selected = [
            r for r in runtime.journal.read_all()
            if r["kind"] == "execution.entry_selected"
        ][-1]
        assert selected["payload"]["opportunity_type"] == "staggered"
        assert selected["payload"]["funding_timestamp_ms"] == first_funding_ms
        assert selected["payload"]["second_funding_timestamp_ms"] == second_funding_ms

    @pytest.mark.asyncio
    async def test_dispatch_entry_sets_first_stage_exit_from_v1_config(self, config, tmp_journal):
        config.strategy.staggered_exit_mode = "after_first_stage"
        binance = FakeVenueAdapter(Venue.BINANCE)
        aster = FakeVenueAdapter(Venue.ASTER)
        adapters = {Venue.BINANCE: binance, Venue.ASTER: aster}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal

        class CapturingExecutor:
            ctx = None

            async def execute(self, ctx):
                self.ctx = ctx
                return EntryExecutionResult(
                    route=ExecutionRoute.PASSIVE_INCREMENTAL,
                    state=EntryState.COMPLETED,
                )

        executor = CapturingExecutor()
        runtime.entry_executor = executor

        from lightfee.sidecar.snapshot import CandidateInput

        first_funding_ms = 1780167600000
        second_funding_ms = 1780171200000
        candidate = CandidateInput(
            long_venue="binance",
            short_venue="aster",
            symbol="PRLUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=12.9,
            expected_edge_bps=12.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=12.9,
            transfer_bias_bps=0.0,
            opportunity_type="staggered",
            blocked=False,
            entry_notional_quote=30.0,
            funding_timestamp_ms=first_funding_ms,
            first_funding_timestamp_ms=first_funding_ms,
            long_funding_timestamp_ms=first_funding_ms,
            short_funding_timestamp_ms=second_funding_ms,
            second_funding_timestamp_ms=second_funding_ms,
            first_funding_leg="long",
        )

        dispatched = await runtime._dispatch_entry(candidate, 1780167385971, price_hint=0.2068)

        assert dispatched is True
        assert executor.ctx is not None
        assert executor.ctx.exit_after_first_stage is True
        selected = [
            r for r in runtime.journal.read_all()
            if r["kind"] == "execution.entry_selected"
        ][-1]
        assert selected["payload"]["exit_after_first_stage"] is True

    @pytest.mark.asyncio
    async def test_normal_exit_updates_funding_state_and_routes_first_stage_capture(
        self, config, tmp_journal,
    ):
        config.strategy.post_funding_hold_secs = 0
        config.strategy.staggered_exit_mode = "after_first_stage"
        config.strategy.profit_take_quote = 100.0
        config.strategy.net_stop_loss_quote = 20.0
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal

        class CapturingPassiveClose:
            def __init__(self):
                self.start_calls = []
                self.drive_calls = []

            async def start_pending_passive_close(self, state, position, reason, **kwargs):
                self.start_calls.append((position.position_id, reason, kwargs))
                return object()

            async def drive_pending_passive_close(
                self, state, position_id, wait_until_terminal=False,
            ):
                self.drive_calls.append((position_id, wait_until_terminal))

        passive = CapturingPassiveClose()
        runtime.passive_close_executor = passive
        first_funding_ms = 1780167600000
        second_funding_ms = 1780171200000
        position = OpenPosition(
            position_id="entry-1780167287526-PRLUSDT",
            symbol="PRLUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.ASTER,
            long_quantity=116.0,
            short_quantity=116.0,
            long_entry_price=0.2068,
            short_entry_price=0.2063,
            opened_at_ms=1780167385971,
            matched_quantity=116.0,
            funding_timestamp_ms=first_funding_ms,
            second_funding_timestamp_ms=second_funding_ms,
            opportunity_type="staggered",
            second_stage_enabled_at_entry=True,
            exit_after_first_stage=True,
            funding_captured=False,
            second_stage_funding_captured=False,
            current_net_quote=0.0,
            peak_net_quote=0.0,
        )
        runtime.state.open_positions[position.position_id] = position

        await runtime._maybe_process_normal_exits(first_funding_ms)

        assert position.funding_captured is True
        assert position.second_stage_funding_captured is False
        assert passive.start_calls == [
            (
                position.position_id,
                "first_stage_capture",
                {
                    "long_price_hint": 0.0,
                    "short_price_hint": 0.0,
                    "short_stage": "exit_short",
                    "long_stage": "exit_long",
                },
            )
        ]
        assert passive.drive_calls == [(position.position_id, False)]
        kinds = [r["kind"] for r in runtime.journal.read_all()]
        assert "runtime.funding_capture_state_updated" in kinds
        assert "runtime.normal_close_routing_passive" in kinds

    @pytest.mark.asyncio
    async def test_normal_exit_routes_force_close_due_as_settlement_force_close(
        self, config, tmp_journal,
    ):
        config.strategy.post_funding_hold_secs = 0
        config.strategy.settlement_remainder_close_delay_secs = 60
        config.strategy.settlement_force_close_delay_secs = 120
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal

        class CapturingPassiveClose:
            def __init__(self):
                self.reasons = []

            async def start_pending_passive_close(self, state, position, reason, **kwargs):
                self.reasons.append(reason)
                return object()

            async def drive_pending_passive_close(
                self, state, position_id, wait_until_terminal=False,
            ):
                return None

        passive = CapturingPassiveClose()
        runtime.passive_close_executor = passive
        funding_ms = 1780167600000
        position = OpenPosition(
            position_id="entry-force-close",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.ASTER,
            long_quantity=0.01,
            short_quantity=0.01,
            long_entry_price=50000.0,
            short_entry_price=50000.0,
            opened_at_ms=funding_ms - 30_000,
            matched_quantity=0.01,
            funding_timestamp_ms=funding_ms,
            opportunity_type="aligned",
            funding_captured=True,
            current_net_quote=0.0,
        )
        runtime.state.open_positions[position.position_id] = position

        await runtime._maybe_process_normal_exits(funding_ms + 120_000)

        assert passive.reasons == ["settlement_force_close"]

    @pytest.mark.asyncio
    async def test_normal_exit_backfills_recovered_first_stage_exit_semantics(
        self, config, tmp_journal,
    ):
        config.strategy.post_funding_hold_secs = 0
        config.strategy.staggered_exit_mode = "after_first_stage"
        config.strategy.profit_take_quote = 100.0
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal

        class CapturingPassiveClose:
            def __init__(self):
                self.reasons = []

            async def start_pending_passive_close(self, state, position, reason, **kwargs):
                self.reasons.append(reason)
                return object()

            async def drive_pending_passive_close(
                self, state, position_id, wait_until_terminal=False,
            ):
                return None

        passive = CapturingPassiveClose()
        runtime.passive_close_executor = passive
        first_funding_ms = 1780167600000
        second_funding_ms = 1780171200000
        position = OpenPosition(
            position_id="entry-1780167287526-PRLUSDT",
            symbol="PRLUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.ASTER,
            long_quantity=116.0,
            short_quantity=116.0,
            long_entry_price=0.2068,
            short_entry_price=0.2063,
            opened_at_ms=1780167385971,
            matched_quantity=116.0,
            funding_timestamp_ms=first_funding_ms,
            second_funding_timestamp_ms=second_funding_ms,
            opportunity_type="staggered",
            second_stage_enabled_at_entry=True,
            exit_after_first_stage=False,
            funding_captured=False,
            second_stage_funding_captured=False,
            current_net_quote=0.0,
        )
        runtime.state.open_positions[position.position_id] = position

        await runtime._maybe_process_normal_exits(first_funding_ms)

        assert position.exit_after_first_stage is True
        assert position.funding_captured is True
        assert passive.reasons == ["first_stage_capture"]
        kinds = [r["kind"] for r in runtime.journal.read_all()]
        assert "runtime.staggered_exit_mode_backfilled" in kinds

    @pytest.mark.asyncio
    async def test_dispatch_entry_does_not_register_rejected_pending(self, config, tmp_journal):
        """V1: deterministic maker rejection is terminal, not pending exposure."""
        binance = FakeVenueAdapter(Venue.BINANCE)
        okx = FakeVenueAdapter(Venue.OKX)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal

        class RejectedPendingExecutor:
            async def execute(self, ctx):
                return EntryExecutionResult(
                    route=ExecutionRoute.REJECTED,
                    state=EntryState.FAILED,
                    pending_entry=PendingEntry(
                        pending_id=ctx.entry_id,
                        symbol=ctx.symbol,
                        long_venue=ctx.long_venue,
                        short_venue=ctx.short_venue,
                        target_quantity=ctx.long_quantity,
                        long_side=Side.BUY,
                        short_side=Side.SELL,
                        created_at_ms=ctx.created_at_ms,
                        maker_client_order_id="maker-rejected-cid",
                        hedge_client_order_id="hedge-unused-cid",
                        outcome="rejected",
                        uncertain_outcome=True,
                    ),
                )

        runtime.entry_executor = RejectedPendingExecutor()

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

        await runtime._dispatch_entry(candidate, 5000, price_hint=50000.0)

        assert runtime.state.pending_entries == {}
        records = runtime.journal.read_all()
        kinds = [r["kind"] for r in records]
        assert "runtime.entry_dispatched" in kinds
        assert "runtime.pending_entry_registered" not in kinds

    @pytest.mark.asyncio
    async def test_reconcile_clears_zero_fill_rejected_pending_without_position_progress(
        self, config, tmp_journal
    ):
        """V1: rejected submit errors are terminal and cannot hydrate exposure."""
        binance = FakeVenueAdapter(Venue.BINANCE, default_position_qty=371.0)
        okx = FakeVenueAdapter(Venue.OKX)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal
        runtime.reconciler = OrderReconciler(adapters)
        runtime.state.pending_entries["entry-rejected"] = PendingEntry(
            pending_id="entry-rejected",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            target_quantity=1.0,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=1000,
            maker_client_order_id="maker-rejected-cid",
            hedge_client_order_id="hedge-unused-cid",
            outcome="rejected",
            uncertain_outcome=True,
            maker_leg="long",
        )

        await runtime._reconcile_pending_state(5000)

        assert "entry-rejected" not in runtime.state.pending_entries
        records = runtime.journal.read_all()
        kinds = [r["kind"] for r in records]
        assert "pending_entry.maker_progress_applied" not in kinds
        assert "pending_entry.missing_hedge_detected" not in kinds

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
