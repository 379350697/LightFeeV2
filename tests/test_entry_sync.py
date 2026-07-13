"""Task 3: Synchronized entry executor contract tests.

Rust references:
- src/execution_core/entry_sync.rs: execute_incremental_entry (line 3173)
- src/execution_core/entry_sync.rs: submit_pending_entry_passive_cycle (line 2486)
- src/execution_core/entry_sync.rs: reconcile_inflight_entry_hedge (line 4568)
- src/engine/entry.rs: execute_order_leg (line 3854)
"""

from __future__ import annotations

import pytest

from lightfee.core.domain import OrderFill, OrderRequest, Side, Venue
from lightfee.persistence.journal import Journal
from tests.fake_adapters import FakeVenueAdapter
from lightfee.engine.entry import (
    EntryContext,
    EntryState,
    EntryType,
    advance_entry_state,
    build_entry_orders,
    build_open_position,
)
from lightfee.engine.entry_sync import (
    EntryExecutionResult,
    EntrySyncExecutor,
    drive_pending_entry_hedge,
    execute_entry,
)
from lightfee.engine.execution_planner import ExecutionRoute
from lightfee.engine.residual import (
    ResidualExposureTask,
    ResidualOrigin,
    detect_residual,
)
from lightfee.engine.state import OpenPosition, PendingEntry
from lightfee.persistence.journal import Journal

from lightfee.core.contracts import VenueAdapter
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.core.domain import PositionSnapshot
from lightfee.venues.transport import TransportError, TransportErrorCategory


# Inline test helpers (avoid cross-file import issues during pytest collection)
from dataclasses import dataclass, field
from typing import Optional as _Optional


@dataclass
class _FakeAdapter(VenueAdapter):
    _venue: Venue
    _min_notional_quote: float = 0.0
    place_order_outcomes: list = field(default_factory=list)
    position_snapshots: list = field(default_factory=list)
    default_fill_price: float = 0.0
    last_request: _Optional[OrderRequest] = None
    place_order_call_count: int = 0
    submit_passive_order_call_count: int = 0
    submit_passive_order_outcomes: list = field(default_factory=list)

    @property
    def venue(self) -> Venue:
        return self._venue

    async def place_order(self, request):
        self.place_order_call_count += 1
        self.last_request = request
        if self.place_order_outcomes:
            outcome = self.place_order_outcomes.pop(0)
            if isinstance(outcome, (OrderSubmitError,)):
                raise outcome
            return outcome
        price = self.default_fill_price if self.default_fill_price > 0 else request.price or 1.0
        return OrderFill(venue=self._venue, symbol=request.symbol, side=request.side,
                         quantity=request.quantity, price=price,
                         order_id=f"fake-{self._venue.value}-{self.place_order_call_count}",
                         filled_at_ms=1000)

    async def submit_passive_order(self, request):
        from lightfee.core.domain import PassiveOrderAck
        self.submit_passive_order_call_count += 1
        self.last_request = request
        if self.submit_passive_order_outcomes:
            outcome = self.submit_passive_order_outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        return PassiveOrderAck(
            venue=self._venue, symbol=request.symbol, side=request.side,
            order_id=f"passive-{self._venue.value}-{self.submit_passive_order_call_count}",
            client_order_id=request.client_order_id or "",
            price=request.price or 0.0, quantity=request.quantity,
            accepted_at_ms=1000,
        )

    async def fetch_position(self, symbol):
        return PositionSnapshot(venue=self._venue, symbol=symbol, side=Side.BUY,
                                quantity=0.0, entry_price=0.0, observed_at_ms=1000)

    async def normalize_quantity(self, symbol, quantity):
        return quantity
def _make_rejected(reason: str = "order rejected") -> OrderSubmitError:
    return OrderSubmitError(SubmitFailureClass.REJECTED, reason)


def _make_uncertain(reason: str = "order timeout") -> OrderSubmitError:
    return OrderSubmitError(SubmitFailureClass.UNCERTAIN, reason)


def _fake_fill(
    venue, symbol, side, quantity, price=50000.0,
    order_id="fill-001", fee_quote=2.5, filled_at_ms=1000,
):
    from lightfee.core.domain import OrderFill as OF
    return OF(venue=venue, symbol=symbol, side=side, quantity=quantity,
              price=price, order_id=order_id, fee_quote=fee_quote,
              filled_at_ms=filled_at_ms)


@pytest.fixture
def journal(tmp_path):
    j = Journal(str(tmp_path / "test.jsonl"))
    j.open()
    yield j
    j.close()


@pytest.fixture
def binance():
    return _FakeAdapter(Venue.BINANCE, _min_notional_quote=10.0)


@pytest.fixture
def okx():
    return _FakeAdapter(Venue.OKX, _min_notional_quote=10.0)


@pytest.fixture
def adapters(binance, okx):
    return {Venue.BINANCE: binance, Venue.OKX: okx}


@pytest.fixture
def btc_context():
    return EntryContext(
        entry_id="e001",
        symbol="BTCUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.OKX,
        long_quantity=0.01,
        short_quantity=0.01,
        long_price_hint=50000.0,
        short_price_hint=50000.0,
        maker_leg=Side.BUY,
        entry_type=EntryType.STANDARD_DUAL_TAKER,
        created_at_ms=1000,
    )


# ---------------------------------------------------------------------------
# EntrySyncExecutor construction
# ---------------------------------------------------------------------------


class TestEntrySyncExecutorConstruction:
    def test_creates_executor_with_adapters_and_journal(self, adapters, journal):
        executor = EntrySyncExecutor(
            adapters=adapters,
            journal=journal,
            state={},
            config_overrides={"deadline_ms": 30_000},
        )
        assert executor.adapters is adapters
        assert executor.journal is journal

    def test_default_config_overrides(self, adapters, journal):
        executor = EntrySyncExecutor(adapters=adapters, journal=journal)
        assert executor.deadline_ms == 30_000
        assert executor.min_matched_ratio == 0.95


# ---------------------------------------------------------------------------
# Standard dual-taker: maker reject fails entry
# ---------------------------------------------------------------------------


class TestEntrySyncMakerReject:
    @pytest.mark.asyncio
    async def test_missing_adapter_rejects_without_secondary_error(self, journal):
        executor = EntrySyncExecutor(adapters={}, journal=journal)
        request = OrderRequest(
            venue=Venue.BINANCE,
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=0.01,
            price=50000.0,
            client_order_id="missing-adapter-cid",
        )

        result = await executor._submit_order(
            request,
            "entry-missing-adapter",
            "maker",
            1_700_000_000_000,
        )

        assert result == {
            "outcome": "rejected",
            "fill": None,
            "order_id": "",
            "reason": "no adapter for binance",
        }
        records = journal.read_all()
        assert records[-1]["kind"] == "order.rejected"
        assert records[-1]["payload"]["reason"] == "no adapter for binance"

    @pytest.mark.asyncio
    async def test_maker_reject_fails_entry_without_hedge_submit(self, adapters, journal, btc_context):
        binance_ada = adapters[Venue.BINANCE]
        okx_ada = adapters[Venue.OKX]
        binance_ada.place_order_outcomes = [_make_rejected("insufficient margin")]

        executor = EntrySyncExecutor(adapters=adapters, journal=journal)
        result = await executor.execute(btc_context)

        assert result.route == ExecutionRoute.REJECTED
        assert result.state == EntryState.FAILED
        assert result.open_position is None
        assert result.residual_task is None
        assert result.pending_entry is None
        # Hedge venue must NOT have been called
        assert okx_ada.place_order_call_count == 0
        kinds = [record["kind"] for record in journal.read_all()]
        assert "entry.aborted_failed_pending_retained" not in kinds

    @pytest.mark.asyncio
    async def test_maker_uncertain_marks_pending(self, adapters, journal, btc_context):
        binance_ada = adapters[Venue.BINANCE]
        binance_ada.place_order_outcomes = [_make_uncertain("timeout")]

        executor = EntrySyncExecutor(adapters=adapters, journal=journal)
        result = await executor.execute(btc_context)

        assert result.state in (EntryState.FAILED, EntryState.FAILED_WITH_RESIDUAL)
        assert result.has_uncertainty is True


class TestPendingEntryHedgeDrive:
    @pytest.mark.asyncio
    async def test_cancel_replace_does_not_submit_new_maker_when_cancel_fails(self, journal):
        """V1 parity: cancel-replace must not double-post when cancel is unconfirmed.

        Rust V1 `cancel_pending_entry_passive_order()` propagates cancel failure
        and keeps the pending entry in reconciliation; it does not submit a
        replacement maker order while the old maker may still be live.
        """

        class CancelFailingAdapter(_FakeAdapter):
            async def cancel_order(self, request):
                self.last_request = request
                raise RuntimeError("cancel timeout")

        maker = CancelFailingAdapter(Venue.BINANCE)
        pending = PendingEntry(
            pending_id="pe-cancel-fail",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            target_quantity=0.01,
            long_side=Side.BUY,
            short_side=Side.SELL,
            long_quantity=0.01,
            short_quantity=0.01,
            maker_order_id="old-maker-1",
            maker_client_order_id="old-maker-cid",
            maker_price=50000.0,
            created_at_ms=1000,
        )

        result = await drive_pending_entry_hedge(
            entry_id=pending.pending_id,
            pending=pending,
            new_price=50050.0,
            old_price=50000.0,
            action="cancel_replace",
            now_ms=2000,
            adapters={Venue.BINANCE: maker},
            journal=journal,
            maker_leg=Side.BUY,
            symbol=pending.symbol,
            long_venue=pending.long_venue,
            short_venue=pending.short_venue,
        )

        assert result.outcome == "uncertain"
        assert "cancel failed" in result.detail
        assert maker.place_order_call_count == 0
        assert pending.maker_order_id == "old-maker-1"
        kinds = [record["kind"] for record in journal.read_all()]
        assert "entry.hedge_drive_cancel_replace_cancel_failed" in kinds


# ---------------------------------------------------------------------------
# Hedged rejection after maker fill → residual
# ---------------------------------------------------------------------------


class TestEntrySyncHedgeRejectAfterMakerFill:
    @pytest.mark.asyncio
    async def test_hedge_reject_after_maker_fill_creates_residual(self, adapters, journal, btc_context):
        binance_ada = adapters[Venue.BINANCE]
        okx_ada = adapters[Venue.OKX]

        maker_fill = _fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.01, 50000.0, "m001")
        binance_ada.place_order_outcomes = [maker_fill]
        okx_ada.place_order_outcomes = [_make_rejected("hedge rejected")]

        executor = EntrySyncExecutor(adapters=adapters, journal=journal)
        result = await executor.execute(btc_context)

        assert result.state == EntryState.FAILED_WITH_RESIDUAL
        assert result.residual_task is not None
        assert result.residual_task.exposure_quantity == pytest.approx(0.01)
        assert result.residual_task.exposure_venue == Venue.BINANCE


# ---------------------------------------------------------------------------
# First fill repricing: never over-hedge or abandon naked delta
# ---------------------------------------------------------------------------


class TestEntrySyncPostFirstFillDecision:
    @pytest.mark.asyncio
    async def test_reprices_hedge_and_uses_actual_first_fill_quantity(
        self, adapters, journal, btc_context
    ):
        binance_ada = adapters[Venue.BINANCE]
        okx_ada = adapters[Venue.OKX]
        binance_ada.place_order_outcomes = [
            _fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.01, 50000.0, "m001"),
        ]
        okx_ada.place_order_outcomes = [
            _fake_fill(Venue.OKX, "BTCUSDT", Side.SELL, 0.01, 49940.0, "h001"),
        ]
        executor = EntrySyncExecutor(
            adapters=adapters,
            journal=journal,
            config_overrides={
                "post_first_fill_decider": lambda **_: {
                    "action": "complete_hedge",
                    "reason": "fresh_l2_complete_hedge",
                    "hedge_price": 49940.0,
                    "complete_hedge_loss_quote": 0.6,
                    "unwind_first_leg_loss_quote": 1.2,
                },
            },
        )

        result = await executor.execute(btc_context)

        assert result.state == EntryState.COMPLETED
        assert okx_ada.last_request.quantity == pytest.approx(0.01)
        assert okx_ada.last_request.price == pytest.approx(49940.0)
        decision = [
            record for record in journal.read_all()
            if record["kind"] == "entry.post_first_fill_decision"
        ]
        assert decision[-1]["payload"]["action"] == "complete_hedge"

    @pytest.mark.asyncio
    async def test_full_unwind_is_terminal_and_never_submits_hedge(
        self, adapters, journal, btc_context
    ):
        binance_ada = adapters[Venue.BINANCE]
        okx_ada = adapters[Venue.OKX]
        binance_ada.place_order_outcomes = [
            _fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.01, 50000.0, "m001"),
            _fake_fill(Venue.BINANCE, "BTCUSDT", Side.SELL, 0.01, 49950.0, "u001"),
        ]
        executor = EntrySyncExecutor(
            adapters=adapters,
            journal=journal,
            config_overrides={
                "post_first_fill_decider": lambda **_: {
                    "action": "unwind_first_leg",
                    "reason": "fresh_l2_lower_unwind_loss",
                    "unwind_price": 49950.0,
                    "complete_hedge_loss_quote": 2.0,
                    "unwind_first_leg_loss_quote": 0.5,
                },
            },
        )

        result = await executor.execute(btc_context)

        assert result.state == EntryState.FAILED
        assert result.pending_entry is None
        assert result.residual_task is None
        assert binance_ada.place_order_call_count == 2
        assert binance_ada.last_request.reduce_only is True
        assert binance_ada.last_request.side is Side.SELL
        assert okx_ada.place_order_call_count == 0
        assert any(
            record["kind"] == "entry.unwound_after_first_fill"
            for record in journal.read_all()
        )

    @pytest.mark.asyncio
    async def test_partial_unwind_becomes_residual_not_pending_entry(
        self, adapters, journal, btc_context
    ):
        binance_ada = adapters[Venue.BINANCE]
        binance_ada.place_order_outcomes = [
            _fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.01, 50000.0, "m001"),
            _fake_fill(Venue.BINANCE, "BTCUSDT", Side.SELL, 0.004, 49950.0, "u001"),
        ]
        executor = EntrySyncExecutor(
            adapters=adapters,
            journal=journal,
            config_overrides={
                "post_first_fill_decider": lambda **_: {
                    "action": "unwind_first_leg",
                    "reason": "fresh_l2_lower_unwind_loss",
                    "unwind_price": 49950.0,
                },
            },
        )

        result = await executor.execute(btc_context)

        assert result.state == EntryState.FAILED_WITH_RESIDUAL
        assert result.pending_entry is None
        assert result.residual_task is not None
        assert result.residual_task.exposure_venue is Venue.BINANCE
        assert result.residual_task.exposure_side is Side.SELL
        assert result.residual_task.exposure_quantity == pytest.approx(0.006)


# ---------------------------------------------------------------------------
# Full dual-taker: both legs fill → OpenPosition
# ---------------------------------------------------------------------------


class TestEntrySyncDualTakerSuccess:
    @pytest.mark.asyncio
    async def test_both_legs_fill_opens_matched_position(self, adapters, journal, btc_context):
        binance_ada = adapters[Venue.BINANCE]
        okx_ada = adapters[Venue.OKX]

        maker_fill = _fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.01, 50000.0, "m001", fee_quote=2.5)
        hedge_fill = _fake_fill(Venue.OKX, "BTCUSDT", Side.SELL, 0.01, 49990.0, "h001", fee_quote=2.5)
        binance_ada.place_order_outcomes = [maker_fill]
        okx_ada.place_order_outcomes = [hedge_fill]

        executor = EntrySyncExecutor(adapters=adapters, journal=journal)
        result = await executor.execute(btc_context)

        assert result.state == EntryState.COMPLETED
        assert result.open_position is not None
        pos = result.open_position
        assert pos.position_id == "e001"
        assert pos.long_quantity == 0.01
        assert pos.short_quantity == 0.01
        assert pos.matched_quantity == 0.01
        assert pos.long_entry_price == 50000.0
        assert pos.short_entry_price == 49990.0
        assert result.residual_task is None

    @pytest.mark.asyncio
    async def test_journal_entries_on_completion(self, adapters, journal, btc_context):
        binance_ada = adapters[Venue.BINANCE]
        okx_ada = adapters[Venue.OKX]
        binance_ada.place_order_outcomes = [
            _fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.01, 50000.0, "m001"),
        ]
        okx_ada.place_order_outcomes = [
            _fake_fill(Venue.OKX, "BTCUSDT", Side.SELL, 0.01, 50000.0, "h001"),
        ]

        executor = EntrySyncExecutor(adapters=adapters, journal=journal)
        await executor.execute(btc_context)

        records = journal.read_all()
        kinds = [r["kind"] for r in records]
        assert "order.submitted" in kinds
        assert "order.filled" in kinds
        assert "entry.opened" in kinds


# ---------------------------------------------------------------------------
# Partial fill behavior
# ---------------------------------------------------------------------------


class TestEntrySyncPartialFills:
    @pytest.mark.asyncio
    async def test_partial_maker_below_threshold_no_position(self, adapters, journal, btc_context):
        """Maker fill below min threshold: entry fails, no position opened."""
        binance_ada = adapters[Venue.BINANCE]
        # Fill only 10% of requested
        binance_ada.place_order_outcomes = [
            _fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.001, 50000.0, "m001"),
        ]

        executor = EntrySyncExecutor(adapters=adapters, journal=journal, config_overrides={
            "min_matched_ratio": 0.5,
        })
        result = await executor.execute(btc_context)

        assert result.open_position is None
        assert result.state == EntryState.FAILED_WITH_RESIDUAL

    @pytest.mark.asyncio
    async def test_partial_hedge_creates_residual(self, adapters, journal, btc_context):
        binance_ada = adapters[Venue.BINANCE]
        okx_ada = adapters[Venue.OKX]

        binance_ada.place_order_outcomes = [
            _fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.01, 50000.0, "m001"),
        ]
        # Hedge only partially filled (0.005 vs 0.01)
        okx_ada.place_order_outcomes = [
            _fake_fill(Venue.OKX, "BTCUSDT", Side.SELL, 0.005, 50000.0, "h001"),
        ]

        executor = EntrySyncExecutor(adapters=adapters, journal=journal)
        result = await executor.execute(btc_context)

        assert result.residual_task is not None
        assert result.residual_task.exposure_quantity == pytest.approx(0.005)


# ---------------------------------------------------------------------------
# execute_entry convenience wrapper
# ---------------------------------------------------------------------------


class TestExecuteEntryConvenience:
    @pytest.mark.asyncio
    async def test_standard_dual_taker_both_fill(self, adapters, journal):
        binance_ada = adapters[Venue.BINANCE]
        okx_ada = adapters[Venue.OKX]

        binance_ada.place_order_outcomes = [
            _fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.01, 50000.0, "m-001"),
        ]
        okx_ada.place_order_outcomes = [
            _fake_fill(Venue.OKX, "BTCUSDT", Side.SELL, 0.01, 49990.0, "h-001"),
        ]

        result = await execute_entry(
            entry_id="ee1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=49990.0,
            maker_leg=Side.BUY,
            adapters=adapters,
            journal=journal,
        )

        assert result.open_position is not None
        assert result.open_position.long_venue == Venue.BINANCE
        assert result.open_position.short_venue == Venue.OKX

    @pytest.mark.asyncio
    async def test_standard_dual_taker_maker_rejected(self, adapters, journal):
        binance_ada = adapters[Venue.BINANCE]
        binance_ada.place_order_outcomes = [_make_rejected("rejected")]

        result = await execute_entry(
            entry_id="ee2",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            quantity=0.01,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            adapters=adapters,
            journal=journal,
        )

        assert result.open_position is None
        assert result.route == ExecutionRoute.REJECTED


# ---------------------------------------------------------------------------
# Task 8: Passive maker lifecycle tests
# ---------------------------------------------------------------------------


class TestPassiveMakerLifecycle:
    """Task 8: Maker post_only must use submit_passive_order not place_order."""

    @pytest.mark.asyncio
    async def test_entry_maker_uses_submit_passive_order_not_place_order(self):
        from lightfee.engine.entry import EntryContext, EntryType
        from lightfee.core.domain import PassiveOrderAck

        maker = FakeVenueAdapter(Venue.BINANCE)
        hedge = FakeVenueAdapter(Venue.OKX)
        maker.submit_passive_order_outcomes = [
            PassiveOrderAck(
                venue=Venue.BINANCE,
                symbol="BTCUSDT",
                side=Side.BUY,
                order_id="maker-order-1",
                client_order_id="entry-1-maker",
                price=50000.0,
                quantity=0.001,
                accepted_at_ms=1000,
            )
        ]

        journal = Journal("/tmp/test_entry_passive.jsonl")
        journal.open()
        executor = EntrySyncExecutor(
            adapters={Venue.BINANCE: maker, Venue.OKX: hedge},
            journal=journal,
        )

        ctx = EntryContext(
            entry_id="entry-1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.001,
            short_quantity=0.001,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
            created_at_ms=1000,
            worst_case_edge_bps_entry=4.0,
            entry_maker_leg="long",
            exit_maker_leg="short",
            entry_cross_bps_entry=1.25,
            fee_bps_entry=2.1,
            entry_slippage_bps_entry=0.75,
            transfer_bias_bps_entry=-0.5,
            transfer_state_at_entry="ok",
            entry_liquidity_source_at_entry="local_l2",
            long_volume_24h_quote_at_entry=12_000_000.0,
            short_volume_24h_quote_at_entry=15_000_000.0,
            long_open_interest_quote_at_entry=8_000_000.0,
            short_open_interest_quote_at_entry=9_000_000.0,
            long_entry_vwap=50000.5,
            short_entry_vwap=50010.5,
            entry_capacity_constrained=True,
            entry_target_quantity=0.002,
            long_max_executable_quantity=0.0018,
            short_max_executable_quantity=0.0016,
            entry_max_executable_quantity=0.0016,
            entry_depth_shortfall_quantity=0.0004,
            entry_max_executable_notional_quote=80.0,
            entry_depth_capped_at_entry=True,
            advisories=["thin_book"],
            blocked_reasons=["capacity_cap"],
        )
        # Force post_only on the maker leg
        result = await executor.execute(ctx)

        assert maker.submit_passive_order_call_count == 1
        assert maker.place_order_call_count == 0
        assert hedge.place_order_call_count == 0
        assert result.pending_entry is not None
        assert result.pending_entry.maker_order_id == "maker-order-1"
        assert result.pending_entry.phase_state is not None
        assert result.pending_entry.phase_state.execution_kind == "entry"
        assert result.pending_entry.phase_state.preferred_maker_leg == "long"
        assert result.pending_entry.phase_state.active_maker_leg == "long"
        assert result.pending_entry.phase_state.phase == "high_slippage_maker"
        assert result.pending_entry.phase_state.phase_started_at_ms == 1000
        assert result.pending_entry.phase_state.cycle_started_at_ms == 1000
        assert result.pending_entry.phase_state.cycle_attempt == 1
        assert result.pending_entry.passive_attempt_count == 1
        assert result.pending_entry.repost_attempt_count == 0
        assert result.pending_entry.worst_case_edge_bps_entry == pytest.approx(4.0)
        assert result.pending_entry.entry_maker_leg == "long"
        assert result.pending_entry.exit_maker_leg == "short"
        assert result.pending_entry.entry_cross_bps_entry == pytest.approx(1.25)
        assert result.pending_entry.fee_bps_entry == pytest.approx(2.1)
        assert result.pending_entry.entry_slippage_bps_entry == pytest.approx(0.75)
        assert result.pending_entry.transfer_bias_bps_entry == pytest.approx(-0.5)
        assert result.pending_entry.transfer_state_at_entry == "ok"
        assert result.pending_entry.entry_liquidity_source_at_entry == "local_l2"
        assert result.pending_entry.long_volume_24h_quote_at_entry == pytest.approx(12_000_000.0)
        assert result.pending_entry.short_volume_24h_quote_at_entry == pytest.approx(15_000_000.0)
        assert result.pending_entry.long_open_interest_quote_at_entry == pytest.approx(8_000_000.0)
        assert result.pending_entry.short_open_interest_quote_at_entry == pytest.approx(9_000_000.0)
        assert result.pending_entry.long_entry_vwap == pytest.approx(50000.5)
        assert result.pending_entry.short_entry_vwap == pytest.approx(50010.5)
        assert result.pending_entry.entry_capacity_constrained is True
        assert result.pending_entry.entry_target_quantity == pytest.approx(0.002)
        assert result.pending_entry.long_max_executable_quantity == pytest.approx(0.0018)
        assert result.pending_entry.short_max_executable_quantity == pytest.approx(0.0016)
        assert result.pending_entry.entry_max_executable_quantity == pytest.approx(0.0016)
        assert result.pending_entry.entry_depth_shortfall_quantity == pytest.approx(0.0004)
        assert result.pending_entry.entry_max_executable_notional_quote == pytest.approx(80.0)
        assert result.pending_entry.entry_depth_capped_at_entry is True
        assert result.pending_entry.advisories == ["thin_book"]
        assert result.pending_entry.blocked_reasons == ["capacity_cap"]
        assert result.state == EntryState.MAKER_RESTING
        assert result.route == ExecutionRoute.PASSIVE_INCREMENTAL
        journal.close()

    @pytest.mark.asyncio
    async def test_passive_ack_only_does_not_emit_filled_or_skip_pending_reconcile(self, tmp_path):
        from lightfee.engine.entry import EntryContext, EntryType
        from lightfee.core.domain import PassiveOrderAck

        maker = FakeVenueAdapter(Venue.BINANCE)
        hedge = FakeVenueAdapter(Venue.OKX)
        maker.submit_passive_order_outcomes = [
            PassiveOrderAck(
                venue=Venue.BINANCE,
                symbol="BTCUSDT",
                side=Side.BUY,
                order_id="maker-ack-only",
                client_order_id="entry-ack-maker",
                price=50000.0,
                quantity=0.001,
                accepted_at_ms=1000,
            )
        ]

        journal = Journal(tmp_path / "entry_ack_only.jsonl")
        journal.open()
        executor = EntrySyncExecutor(
            adapters={Venue.BINANCE: maker, Venue.OKX: hedge},
            journal=journal,
        )

        ctx = EntryContext(
            entry_id="entry-ack",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.001,
            short_quantity=0.001,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
            created_at_ms=1000,
        )
        result = await executor.execute(ctx)
        journal.close()

        kinds = [r["kind"] for r in journal.read_all()]
        assert "order.passive_submitted" in kinds
        assert "order.filled" not in kinds
        assert result.open_position is None
        assert result.pending_entry is not None
        assert result.pending_entry.outcome == "maker_resting"
        assert result.pending_entry.maker_order_id == "maker-ack-only"
        assert result.state == EntryState.MAKER_RESTING
        assert result.route == ExecutionRoute.PASSIVE_INCREMENTAL
        assert result.pending_entry.hedge_order_id == ""

    @pytest.mark.asyncio
    async def test_passive_maker_reject_result_carries_structured_exchange_evidence(self, tmp_path):
        from lightfee.engine.entry import EntryContext, EntryType

        maker = FakeVenueAdapter(Venue.ASTER)
        hedge = FakeVenueAdapter(Venue.BINANCE)
        transport_error = TransportError(
            TransportErrorCategory.REQUEST_REJECTED,
            "aster_v3 POST /fapi/v3/order rejected status=400",
            status_code=400,
            body=(
                '{"code":-5018,"msg":"Youve reached the maximum notional value '
                'limit for this symbol. You can still reduce or close your '
                'position to manage your risk."}'
            ),
        )
        maker.submit_passive_order_outcomes = [
            OrderSubmitError(
                SubmitFailureClass.REJECTED,
                "aster_v3 passive order rejected: aster_v3 POST /fapi/v3/order rejected status=400",
                transport_error=transport_error,
            )
        ]

        journal = Journal(tmp_path / "entry_passive_reject_evidence.jsonl")
        journal.open()
        executor = EntrySyncExecutor(
            adapters={Venue.ASTER: maker, Venue.BINANCE: hedge},
            journal=journal,
        )

        ctx = EntryContext(
            entry_id="entry-aster-reject",
            symbol="ESPORTSUSDT",
            long_venue=Venue.ASTER,
            short_venue=Venue.BINANCE,
            long_quantity=12.0,
            short_quantity=12.0,
            long_price_hint=0.08,
            short_price_hint=0.08,
            maker_leg=Side.BUY,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
            created_at_ms=1000,
        )
        result = await executor.execute(ctx)
        journal.close()

        assert result.route == ExecutionRoute.REJECTED
        assert result.reject_evidence["venue"] == "aster"
        assert result.reject_evidence["operation"] == "submit_passive_order"
        assert result.reject_evidence["http_status"] == 400
        assert result.reject_evidence["exchange_code"] == "-5018"
        assert "maximum notional value limit" in result.reject_evidence["exchange_msg"]
        assert result.reject_evidence["request_context"]["symbol"] == "ESPORTSUSDT"
        assert result.reject_evidence["request_context"]["post_only"] is True

    @pytest.mark.asyncio
    async def test_taker_order_still_uses_place_order(self):
        maker = FakeVenueAdapter(Venue.BINANCE)
        hedge = FakeVenueAdapter(Venue.OKX)

        journal = Journal("/tmp/test_entry_taker.jsonl")
        journal.open()
        executor = EntrySyncExecutor(
            adapters={Venue.BINANCE: maker, Venue.OKX: hedge},
            journal=journal,
        )

        ctx = EntryContext(
            entry_id="entry-2",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.001,
            short_quantity=0.001,
            long_price_hint=50000.0,
            short_price_hint=50000.0,
            maker_leg=Side.BUY,
            entry_type=EntryType.STANDARD_DUAL_TAKER,
            created_at_ms=1000,
        )
        result = await executor.execute(ctx)

        assert maker.place_order_call_count == 1
        assert maker.submit_passive_order_call_count == 0
        journal.close()
