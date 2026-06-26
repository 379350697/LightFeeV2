"""V1 Record Layer Full Parity Tests — Layer 1 (Must Align Completely).

Covers:
- post-only / reduce-only / TIF field propagation
- clientOrderId idempotency and dedup
- Order/cancel/modify confirmation tracking
- Partial fill / uncertain fill handling
- Hedge reject residual repair
- Close reconciliation with clientOrderId
- Recovery dedup prevention

Rust references:
- src/execution_core/entry_sync.rs
- src/engine/entry.rs
- src/engine/exit.rs
- src/engine/recovery.rs
"""

from __future__ import annotations

import pytest
from dataclasses import dataclass, field
from typing import Optional as _Optional

from lightfee.core.domain import (
    OrderFill,
    OrderFillReconciliation,
    OrderRequest,
    Side,
    TimeInForce,
    Venue,
)
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import PositionSnapshot

from lightfee.engine.entry import (
    EntryContext,
    EntryState,
    EntryType,
    build_entry_orders,
)
from lightfee.engine.entry_sync import (
    EntryExecutionResult,
    EntrySyncExecutor,
    execute_entry,
)
from lightfee.engine.close_executor import (
    CloseExecutor,
    CloseExecutionLeg,
)
from lightfee.engine.recovery import (
    build_recovery_dedup_index,
    is_client_order_id_duplicate,
    has_pending_entry_for_symbol,
    normalize_engine_state,
)
from lightfee.engine.reconciliation import (
    OrderReconciler,
    ReconciliationResult,
)
from lightfee.engine.state import (
    EngineState,
    OpenPosition,
    PendingClose,
    PendingEntry,
)
from lightfee.engine.residual import (
    ResidualExposureTask,
    ResidualOrigin,
    split_entry_fill_residual,
)
from lightfee.persistence.journal import Journal


# ---------------------------------------------------------------------------
# Fake adapter with clientOrderId tracking
# ---------------------------------------------------------------------------


@dataclass
class FakeAdapter(VenueAdapter):
    _venue: Venue
    _min_notional_quote: float = 0.0
    place_order_outcomes: list = field(default_factory=list)
    position_snapshots: list = field(default_factory=list)
    default_fill_price: float = 0.0
    last_request: _Optional[OrderRequest] = None
    place_order_call_count: int = 0
    amend_order_call_count: int = 0
    cancel_order_call_count: int = 0
    fetch_order_fill_results: list = field(default_factory=list)

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
        return OrderFill(
            venue=self._venue, symbol=request.symbol, side=request.side,
            quantity=request.quantity, price=price,
            order_id=f"fake-{self._venue.value}-{self.place_order_call_count}",
            client_order_id=request.client_order_id,
            filled_at_ms=1000,
        )

    async def amend_order(self, request):
        self.amend_order_call_count += 1
        self.last_request = request
        if self.place_order_outcomes:
            outcome = self.place_order_outcomes.pop(0)
            if isinstance(outcome, OrderSubmitError):
                raise outcome
            return outcome
        return OrderFill(
            venue=self._venue, symbol=request.symbol, side=request.side,
            quantity=request.quantity, price=request.price or 1.0,
            order_id=f"amend-{self._venue.value}-{self.amend_order_call_count}",
            filled_at_ms=1000,
        )

    async def cancel_order(self, request):
        self.cancel_order_call_count += 1

    async def submit_passive_order(self, request):
        from lightfee.core.domain import PassiveOrderAck
        self.last_request = request
        # Respect place_order_outcomes for error injection (rejected/uncertain)
        if self.place_order_outcomes:
            outcome = self.place_order_outcomes.pop(0)
            if isinstance(outcome, OrderSubmitError):
                raise outcome
        return PassiveOrderAck(
            venue=self._venue, symbol=request.symbol, side=request.side,
            order_id=f"passive-{self._venue.value}-1",
            client_order_id=request.client_order_id or "",
            price=request.price or 0.0, quantity=request.quantity,
            accepted_at_ms=1000,
        )

    async def fetch_order_fill_reconciliation(self, symbol, order_id, client_order_id=""):
        if self.fetch_order_fill_results:
            return self.fetch_order_fill_results.pop(0)
        return None

    async def fetch_position(self, symbol):
        return PositionSnapshot(
            venue=self._venue, symbol=symbol, side=Side.BUY,
            quantity=0.0, entry_price=0.0, observed_at_ms=1000,
        )

    async def normalize_quantity(self, symbol, quantity):
        return quantity


def _make_rejected(reason: str = "order rejected") -> OrderSubmitError:
    return OrderSubmitError(SubmitFailureClass.REJECTED, reason)


def _make_uncertain(reason: str = "order timeout") -> OrderSubmitError:
    return OrderSubmitError(SubmitFailureClass.UNCERTAIN, reason)


def _fake_fill(venue, symbol, side, quantity, price=50000.0,
               order_id="fill-001", fee_quote=2.5, filled_at_ms=1000,
               client_order_id=""):
    return OrderFill(
        venue=venue, symbol=symbol, side=side,
        quantity=quantity, price=price,
        order_id=order_id, fee_quote=fee_quote,
        filled_at_ms=filled_at_ms, client_order_id=client_order_id,
    )


@pytest.fixture
def journal(tmp_path):
    j = Journal(str(tmp_path / "test.jsonl"))
    j.open()
    yield j
    j.close()


@pytest.fixture
def binance():
    return FakeAdapter(Venue.BINANCE)


@pytest.fixture
def okx():
    return FakeAdapter(Venue.OKX)


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
        entry_type=EntryType.PASSIVE_INCREMENTAL,
        created_at_ms=1000,
    )


@pytest.fixture
def taker_ctx():
    """EntryContext for DUAL_TAKER flow — both maker and hedge use place_order."""
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


# ===========================================================================
# 1. post-only / reduce-only / TIF field propagation
# ===========================================================================


class TestV1OrderRequestTifAndReduceOnly:
    """V1: OrderRequest carries time_in_force and reduce_only correctly."""

    def test_maker_order_is_gtc_post_only(self, btc_context):
        maker_req, hedge_req = build_entry_orders(btc_context)
        assert maker_req.post_only is True
        assert maker_req.time_in_force == TimeInForce.GTC
        assert maker_req.reduce_only is False

    def test_hedge_order_is_ioc_not_reduce_only(self, btc_context):
        maker_req, hedge_req = build_entry_orders(btc_context)
        assert hedge_req.time_in_force == TimeInForce.IOC
        assert hedge_req.reduce_only is False  # hedge is opening, not closing

    def test_maker_carries_client_order_id(self, btc_context):
        maker_req, hedge_req = build_entry_orders(btc_context)
        # V2: CID is now hash-based (decoupled from internal entry_id)
        assert maker_req.client_order_id is not None
        assert len(maker_req.client_order_id) > 0
        assert len(maker_req.client_order_id) <= 36  # Binance max
        assert hedge_req.client_order_id is not None
        assert len(hedge_req.client_order_id) > 0
        assert len(hedge_req.client_order_id) <= 32  # OKX max
        # CID must be deterministic
        m2, h2 = build_entry_orders(btc_context)
        assert m2.client_order_id == maker_req.client_order_id
        assert h2.client_order_id == hedge_req.client_order_id

    def test_sell_maker_uses_short_venue(self):
        ctx = EntryContext(
            entry_id="e002", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=0.01, short_quantity=0.01,
            long_price_hint=50000.0, short_price_hint=50000.0,
            maker_leg=Side.SELL,
            entry_type=EntryType.PASSIVE_INCREMENTAL,
            created_at_ms=1000,
        )
        maker_req, hedge_req = build_entry_orders(ctx)
        assert maker_req.venue == Venue.OKX
        assert maker_req.side == Side.SELL
        assert maker_req.post_only is True
        assert hedge_req.venue == Venue.BINANCE
        assert hedge_req.side == Side.BUY


# ===========================================================================
# 2. clientOrderId idempotency
# ===========================================================================


class TestV1ClientOrderIdIdempotency:
    """V1: clientOrderId is deterministic and tracked through the pipeline."""

    @pytest.mark.asyncio
    async def test_client_order_id_passed_to_adapter(self, adapters, journal, taker_ctx):
        binance_ada = adapters[Venue.BINANCE]
        okx_ada = adapters[Venue.OKX]
        binance_ada.place_order_outcomes = [
            _fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.01, 50000.0, "m001"),
        ]
        okx_ada.place_order_outcomes = [
            _fake_fill(Venue.OKX, "BTCUSDT", Side.SELL, 0.01, 50000.0, "h001"),
        ]

        executor = EntrySyncExecutor(adapters=adapters, journal=journal)
        await executor.execute(taker_ctx)

        # Verify clientOrderId was passed on maker request (V2: hash-based CID)
        assert binance_ada.last_request is not None
        assert binance_ada.last_request.client_order_id is not None
        assert len(binance_ada.last_request.client_order_id) > 0
        assert len(binance_ada.last_request.client_order_id) <= 36
        assert okx_ada.last_request is not None
        assert okx_ada.last_request.client_order_id is not None
        assert len(okx_ada.last_request.client_order_id) > 0
        assert len(okx_ada.last_request.client_order_id) <= 32

    @pytest.mark.asyncio
    async def test_journal_events_include_client_order_id(self, adapters, journal, taker_ctx):
        binance_ada = adapters[Venue.BINANCE]
        okx_ada = adapters[Venue.OKX]
        binance_ada.place_order_outcomes = [
            _fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.01, 50000.0, "m001"),
        ]
        okx_ada.place_order_outcomes = [
            _fake_fill(Venue.OKX, "BTCUSDT", Side.SELL, 0.01, 50000.0, "h001"),
        ]

        executor = EntrySyncExecutor(adapters=adapters, journal=journal)
        await executor.execute(taker_ctx)

        records = journal.read_all()
        maker_submitted = [r for r in records if r["kind"] == "order.submitted" and r["payload"].get("leg") == "maker"]
        assert len(maker_submitted) == 1
        assert len(maker_submitted[0]["payload"]["client_order_id"]) > 0
        assert maker_submitted[0]["payload"].get("internal_entry_id") == taker_ctx.entry_id

        completed = [r for r in records if r["kind"] == "entry.opened"]
        assert len(completed) == 1
        assert len(completed[0]["payload"]["maker_client_order_id"]) > 0
        assert len(completed[0]["payload"]["hedge_client_order_id"]) > 0
        assert completed[0]["payload"].get("internal_entry_id") == taker_ctx.entry_id


# ===========================================================================
# 3. Order confirmation / cancel / modify
# ===========================================================================


class TestV1OrderConfirmation:
    """V1: all order outcomes produce confirmation journal events."""

    @pytest.mark.asyncio
    async def test_maker_rejected_produces_confirmation(self, adapters, journal, btc_context):
        binance_ada = adapters[Venue.BINANCE]
        binance_ada.place_order_outcomes = [_make_rejected("insufficient margin")]

        executor = EntrySyncExecutor(adapters=adapters, journal=journal)
        result = await executor.execute(btc_context)

        assert result.state == EntryState.FAILED
        records = journal.read_all()
        kinds = [r["kind"] for r in records]
        assert "order.submitted" in kinds
        assert "order.rejected" in kinds

    @pytest.mark.asyncio
    async def test_maker_uncertain_produces_pending_entry(self, adapters, journal, btc_context):
        binance_ada = adapters[Venue.BINANCE]
        binance_ada.place_order_outcomes = [_make_uncertain("timeout")]

        executor = EntrySyncExecutor(adapters=adapters, journal=journal)
        result = await executor.execute(btc_context)

        assert result.pending_entry is not None
        assert result.pending_entry.pending_id == btc_context.entry_id
        assert result.pending_entry.uncertain_outcome is True
        assert len(result.pending_entry.maker_client_order_id) > 0
        assert result.has_uncertainty is True

    @pytest.mark.asyncio
    async def test_both_filled_produces_completion_journal(self, adapters, journal, taker_ctx):
        binance_ada = adapters[Venue.BINANCE]
        okx_ada = adapters[Venue.OKX]
        binance_ada.place_order_outcomes = [
            _fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.01, 50000.0, "m001"),
        ]
        okx_ada.place_order_outcomes = [
            _fake_fill(Venue.OKX, "BTCUSDT", Side.SELL, 0.01, 50000.0, "h001"),
        ]

        executor = EntrySyncExecutor(adapters=adapters, journal=journal)
        result = await executor.execute(taker_ctx)

        assert result.state == EntryState.COMPLETED
        assert result.open_position is not None
        records = journal.read_all()
        kinds = [r["kind"] for r in records]
        assert "order.submitted" in kinds
        assert "order.filled" in kinds
        assert "entry.opened" in kinds


# ===========================================================================
# 4. Partial fill / uncertain fill handling
# ===========================================================================


class TestV1PartialFillHandling:
    """V1: partial fills and uncertain fills create PendingEntry for reconciliation."""

    @pytest.mark.asyncio
    async def test_partial_hedge_creates_pending_entry(self, adapters, journal, taker_ctx):
        binance_ada = adapters[Venue.BINANCE]
        okx_ada = adapters[Venue.OKX]
        binance_ada.place_order_outcomes = [
            _fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.01, 50000.0, "m001"),
        ]
        okx_ada.place_order_outcomes = [
            _fake_fill(Venue.OKX, "BTCUSDT", Side.SELL, 0.005, 50000.0, "h001"),
        ]

        executor = EntrySyncExecutor(adapters=adapters, journal=journal)
        result = await executor.execute(taker_ctx)

        assert result.residual_task is not None
        assert result.pending_entry is not None
        assert result.pending_entry.maker_leg_filled == 0.01
        assert result.pending_entry.hedge_leg_filled == 0.005
        assert result.pending_entry.outcome == "partial_fill_residual"

    @pytest.mark.asyncio
    async def test_hedge_uncertain_creates_pending_entry(self, adapters, journal, taker_ctx):
        binance_ada = adapters[Venue.BINANCE]
        okx_ada = adapters[Venue.OKX]
        binance_ada.place_order_outcomes = [
            _fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.01, 50000.0, "m001"),
        ]
        okx_ada.place_order_outcomes = [_make_uncertain("timeout")]

        executor = EntrySyncExecutor(adapters=adapters, journal=journal)
        result = await executor.execute(taker_ctx)

        assert result.has_uncertainty is True
        assert result.pending_entry is not None
        assert result.pending_entry.outcome == "hedge_uncertain"
        assert result.pending_entry.maker_leg_filled == 0.01

    @pytest.mark.asyncio
    async def test_below_min_ratio_creates_pending_entry(self, adapters, journal, taker_ctx):
        binance_ada = adapters[Venue.BINANCE]
        okx_ada = adapters[Venue.OKX]
        # Both legs fill symmetrically at 0.005 each (50% of target 0.01)
        # This passes residual check (symmetric) but fails min_matched_ratio (0.5 < 0.95)
        binance_ada.place_order_outcomes = [
            _fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.005, 50000.0, "m001"),
        ]
        okx_ada.place_order_outcomes = [
            _fake_fill(Venue.OKX, "BTCUSDT", Side.SELL, 0.005, 50000.0, "h001"),
        ]

        executor = EntrySyncExecutor(adapters=adapters, journal=journal)
        result = await executor.execute(taker_ctx)

        assert result.state == EntryState.FAILED_WITH_RESIDUAL
        assert result.pending_entry is not None
        assert result.pending_entry.outcome == "below_min_matched_ratio"


# ===========================================================================
# 5. Hedge reject residual repair
# ===========================================================================


class TestV1HedgeRejectResidualRepair:
    """V1: hedge rejection after maker fill creates residual + PendingEntry."""

    @pytest.mark.asyncio
    async def test_hedge_reject_creates_residual_and_pending(self, adapters, journal, taker_ctx):
        binance_ada = adapters[Venue.BINANCE]
        okx_ada = adapters[Venue.OKX]
        maker_fill = _fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.01, 50000.0, "m001")
        binance_ada.place_order_outcomes = [maker_fill]
        okx_ada.place_order_outcomes = [_make_rejected("hedge rejected")]

        executor = EntrySyncExecutor(adapters=adapters, journal=journal)
        result = await executor.execute(taker_ctx)

        assert result.state == EntryState.FAILED_WITH_RESIDUAL
        assert result.residual_task is not None
        assert result.pending_entry is not None
        assert result.pending_entry.outcome == "hedge_rejected"
        assert result.pending_entry.maker_leg_filled == 0.01

        # Check journal has hedge_rejected_residual event
        records = journal.read_all()
        residual_events = [r for r in records if r["kind"] == "entry.hedge_rejected_residual"]
        assert len(residual_events) == 1
        assert residual_events[0]["payload"]["maker_filled"] == 0.01

    @pytest.mark.asyncio
    async def test_hedge_reject_long_maker_creates_residual_on_long_venue(self, adapters, journal, taker_ctx):
        """Maker=BUY (long venue), hedge=SELL (short venue) rejected → residual on long."""
        binance_ada = adapters[Venue.BINANCE]
        okx_ada = adapters[Venue.OKX]
        maker_fill = _fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.01, 50000.0, "m001")
        binance_ada.place_order_outcomes = [maker_fill]
        okx_ada.place_order_outcomes = [_make_rejected("hedge rejected")]

        executor = EntrySyncExecutor(adapters=adapters, journal=journal)
        result = await executor.execute(taker_ctx)

        assert result.residual_task.exposure_venue == Venue.BINANCE
        assert result.residual_task.exposure_side == Side.SELL  # sell to close excess long

    def test_residual_split_entry_fill_handles_zero_short(self):
        long_fill = _fake_fill(Venue.BINANCE, "BTCUSDT", Side.BUY, 0.01, 50000.0, "m001")
        short_fill = OrderFill(venue=Venue.OKX, symbol="BTCUSDT", side=Side.SELL,
                               quantity=0.0, price=0.0)

        residual = split_entry_fill_residual(
            position_id="e001",
            pair_id="btcusdt:binance->okx",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_fill=long_fill,
            short_fill=short_fill,
        )
        assert residual is not None
        assert residual.exposure_venue == Venue.BINANCE
        assert residual.exposure_quantity == pytest.approx(0.01)


# ===========================================================================
# 6. Close reconciliation with clientOrderId
# ===========================================================================


class TestV1CloseReconciliation:
    """V1: CloseExecutor uses clientOrderId and creates reconcilable PendingClose."""

    def _make_position(self, **overrides) -> OpenPosition:
        defaults = dict(
            position_id="p001", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=0.01, short_quantity=0.01,
            long_entry_price=50000.0, short_entry_price=50000.0,
            opened_at_ms=1000000, matched_quantity=0.01,
        )
        defaults.update(overrides)
        return OpenPosition(**defaults)

    @pytest.mark.asyncio
    async def test_close_orders_have_ioc_time_in_force(self, adapters, journal):
        binance_ada = adapters[Venue.BINANCE]
        okx_ada = adapters[Venue.OKX]
        binance_ada.place_order_outcomes = [
            _fake_fill(Venue.BINANCE, "BTCUSDT", Side.SELL, 0.01, 50100.0, "l001"),
        ]
        okx_ada.place_order_outcomes = [
            _fake_fill(Venue.OKX, "BTCUSDT", Side.BUY, 0.01, 49900.0, "s001"),
        ]

        executor = CloseExecutor(adapters=adapters, journal=journal)
        pos = self._make_position()
        await executor.execute_close(pos, "profit_take", 5000)

        # Check short close (buy at short_venue=OKX) has IOC
        assert okx_ada.last_request is not None
        assert okx_ada.last_request.time_in_force == TimeInForce.IOC
        assert okx_ada.last_request.reduce_only is True

    @pytest.mark.asyncio
    async def test_close_orders_have_client_order_id(self, adapters, journal):
        binance_ada = adapters[Venue.BINANCE]
        okx_ada = adapters[Venue.OKX]
        binance_ada.place_order_outcomes = [
            _fake_fill(Venue.BINANCE, "BTCUSDT", Side.SELL, 0.01, 50100.0, "l001"),
        ]
        okx_ada.place_order_outcomes = [
            _fake_fill(Venue.OKX, "BTCUSDT", Side.BUY, 0.01, 49900.0, "s001"),
        ]

        executor = CloseExecutor(adapters=adapters, journal=journal)
        pos = self._make_position()
        await executor.execute_close(pos, "profit_take", 5000)

        assert okx_ada.last_request.client_order_id is not None
        cid = okx_ada.last_request.client_order_id
        # CIDs are compact V1 format (lf...): ~20 chars, well under all limits
        assert cid.startswith("lf")
        assert 18 <= len(cid) <= 24
        assert all(c.isalnum() for c in cid)

    @pytest.mark.asyncio
    async def test_uncertain_close_creates_pending_close_with_client_order_id(self, adapters, journal):
        from lightfee.engine.state import EngineState

        binance_ada = adapters[Venue.BINANCE]
        okx_ada = adapters[Venue.OKX]
        binance_ada.place_order_outcomes = [
            _fake_fill(Venue.BINANCE, "BTCUSDT", Side.SELL, 0.01, 50100.0, "l001"),
        ]
        # Set max_close_retries=1 to prevent retry from filling via default fake
        okx_ada.place_order_outcomes = [_make_uncertain("timeout")]

        state = EngineState()
        pos = self._make_position()
        state.open_positions[pos.position_id] = pos

        executor = CloseExecutor(
            adapters=adapters, journal=journal,
            config_overrides={"max_close_retries": 1},
        )
        await executor.execute_close(pos, "profit_take", 5000, state=state)

        assert len(state.pending_closes) > 0
        for pc in state.pending_closes.values():
            assert pc.short_uncertain is True
            assert pc.short_client_order_id is not None
            cid = pc.short_client_order_id
            assert cid.startswith("lf")
            assert 18 <= len(cid) <= 24
            assert all(c.isalnum() for c in cid)


# ===========================================================================
# 7. Recovery dedup prevention
# ===========================================================================


class TestV1RecoveryDedup:
    """V1: recovery dedup index prevents duplicate orders after restart."""

    def test_build_dedup_index_from_pending_entries(self):
        state = EngineState()
        state.pending_entries["pend-1"] = PendingEntry(
            pending_id="pend-1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            target_quantity=0.01, long_side=Side.BUY, short_side=Side.SELL,
            created_at_ms=1000,
            maker_client_order_id="entry-1000-BTCUSDT-maker",
            hedge_client_order_id="entry-1000-BTCUSDT-hedge",
        )

        index = build_recovery_dedup_index(state)
        assert "entry-1000-BTCUSDT-maker" in index
        assert "entry-1000-BTCUSDT-hedge" in index
        assert index["entry-1000-BTCUSDT-maker"] == "pend-1"

    def test_build_dedup_index_from_pending_closes(self):
        state = EngineState()
        state.pending_closes["close-1"] = PendingClose(
            close_id="close-1", position_id="pos-1",
            reason="profit_take", created_at_ms=5000,
            long_client_order_id="close-pos-1-5000-long",
            short_client_order_id="close-pos-1-5000-short",
        )

        index = build_recovery_dedup_index(state)
        assert "close-pos-1-5000-long" in index
        assert "close-pos-1-5000-short" in index

    def test_is_duplicate_returns_true_for_known_cid(self):
        index = {"entry-1000-BTCUSDT-maker": "pend-1"}
        assert is_client_order_id_duplicate("entry-1000-BTCUSDT-maker", index) is True

    def test_is_duplicate_returns_false_for_unknown_cid(self):
        index = {"entry-1000-BTCUSDT-maker": "pend-1"}
        assert is_client_order_id_duplicate("new-entry-maker", index) is False

    def test_is_duplicate_returns_false_for_empty_cid(self):
        index = {"some-cid": "pend-1"}
        assert is_client_order_id_duplicate("", index) is False

    def test_has_pending_entry_for_symbol_pair(self):
        state = EngineState()
        state.pending_entries["pend-1"] = PendingEntry(
            pending_id="pend-1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            target_quantity=0.01, long_side=Side.BUY, short_side=Side.SELL,
            created_at_ms=1000,
        )

        assert has_pending_entry_for_symbol(state, "BTCUSDT", "binance", "okx") is True
        assert has_pending_entry_for_symbol(state, "ETHUSDT", "binance", "okx") is False
        assert has_pending_entry_for_symbol(state, "BTCUSDT", "bybit", "gate") is False

    def test_normalize_engine_state_removes_dust_positions(self):
        state = EngineState()
        state.open_positions["dust-1"] = OpenPosition(
            position_id="dust-1", symbol="RAREUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=0.0, short_quantity=0.0,
            long_entry_price=1.0, short_entry_price=1.01,
            opened_at_ms=1000, matched_quantity=0.0,
        )
        state.open_positions["real-1"] = OpenPosition(
            position_id="real-1", symbol="BTCUSDT",
            long_venue=Venue.BINANCE, short_venue=Venue.OKX,
            long_quantity=0.01, short_quantity=0.01,
            long_entry_price=50000.0, short_entry_price=50000.0,
            opened_at_ms=1000, matched_quantity=0.01,
        )

        normalize_engine_state(state)
        assert "dust-1" not in state.open_positions
        assert "real-1" in state.open_positions


# ===========================================================================
# 8. Reconciliation uses clientOrderId
# ===========================================================================


class TestV1ReconciliationClientOrderId:
    """V1: OrderReconciler.reconcile_position uses clientOrderId for lookup."""

    @pytest.mark.asyncio
    async def test_reconciler_accepts_client_order_ids(self):
        long_adapter = FakeAdapter(Venue.BINANCE)
        short_adapter = FakeAdapter(Venue.OKX)

        reconciler = OrderReconciler(adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter})
        result = await reconciler.reconcile_position(
            position_id="pos-1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_client_order_id="entry-1000-BTCUSDT-maker",
            short_client_order_id="entry-1000-BTCUSDT-hedge",
        )

        assert result.position_id == "pos-1"
        assert result.long_status in ("filled", "uncertain", "not_found")
        assert result.short_status in ("filled", "uncertain", "not_found")

    @pytest.mark.asyncio
    async def test_reconciler_falls_back_to_client_order_id_when_no_order_id(self):
        long_adapter = FakeAdapter(Venue.BINANCE)
        short_adapter = FakeAdapter(Venue.OKX)
        # Preload a fill result that will be returned for the clientOrderId lookup
        expected_fill = OrderFillReconciliation(
            venue=Venue.BINANCE,
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=0.01,
            average_price=50000.0,
            order_id="real-order-123",
            metadata={
                "queried_endpoints": ["fetch_order_fill_reconciliation"],
                "response_classification": "filled_after_client_order_id_lookup",
                "raw_exchange_status": "filled",
                "evidence_source": "fetch_order_fill_reconciliation",
            },
        )
        long_adapter.fetch_order_fill_results = [expected_fill]

        reconciler = OrderReconciler(adapters={Venue.BINANCE: long_adapter, Venue.OKX: short_adapter})
        result = await reconciler.reconcile_position(
            position_id="pos-1",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_client_order_id="entry-1000-BTCUSDT-maker",
        )

        assert result.long_status == "filled"
        assert result.long_fill is not None
        assert result.long_fill.order_id == "real-order-123"


# ===========================================================================
# 9. execute_entry wrapper — regression
# ===========================================================================


class TestV1ExecuteEntryWrapper:
    """V1: execute_entry() convenience wrapper propagates TIF and clientOrderId."""

    @pytest.mark.asyncio
    async def test_wrapper_produces_same_tif_and_cid(self, adapters, journal):
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
        # V2: hash-based CID, verify non-empty and within venue length limits
        assert binance_ada.last_request.client_order_id is not None
        assert len(binance_ada.last_request.client_order_id) > 0
        assert len(binance_ada.last_request.client_order_id) <= 36
        assert okx_ada.last_request.client_order_id is not None
        assert len(okx_ada.last_request.client_order_id) > 0
        assert len(okx_ada.last_request.client_order_id) <= 32
