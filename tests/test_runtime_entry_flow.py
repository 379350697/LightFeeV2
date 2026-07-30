"""Task 3: Runtime entry wiring contract tests.

Rust references:
- src/engine/entry.rs: execute_incremental_entry → runtime integration
- src/app_runtime/loop_control.rs: tick candidate → entry flow
"""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from lightfee.config.schema import (
    AppConfig,
    PersistenceConfig,
    RuntimeConfig,
    StrategyConfig,
    VenueConfig,
)
from lightfee.core.domain import (
    EntryLeverageEvidence,
    OrderFill,
    OrderRequest,
    PassiveOrderState,
    PositionSnapshot,
    Side,
    Venue,
)
from lightfee.engine.entry import EntryState
from lightfee.engine.entry_sync import EntryExecutionResult, EntrySyncExecutor
from lightfee.engine.execution_planner import ExecutionRoute
from lightfee.engine.reconciliation import OrderReconciler, PositionReconciliationResult
from lightfee.engine.recovery_ledger import RecoveryLedger
from lightfee.engine.runtime import LiveRuntime
from lightfee.engine.entry_dispatch_runtime import EntryDispatchRuntime
from lightfee.engine.state import (
    ActiveMakerLeg,
    OpenPosition,
    PendingPassiveOrder,
    PassiveExecutionPhase,
    PassivePhaseState,
    PendingEntry,
    PendingPassiveClose,
)
from lightfee.engine.exit_shadow import (
    ExitShadowConfig,
    ExitShadowSnapshot,
    evaluate_exit_shadow_strategies,
)
from lightfee.marketdata.l2 import L2BookStatus, PriceLevel
from lightfee.marketdata.open_interest import open_interest_sample_id
from lightfee.marketdata.ws_bbo import TopBookQuote, VenueBboCache
from lightfee.persistence.journal import Journal
from lightfee.risk.modes import EngineLifecycle, GlobalRiskMode

from dataclasses import dataclass, field
from typing import Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass


def _attach_live_oi_evidence(candidate, *, now_ms: int, value_quote: float = 2_000_000.0):
    """Attach a complete revision-bound two-leg OI receipt to live fixtures."""
    revision_id = str(
        getattr(candidate, "candidate_revision_id", "")
        or (
            f"test-revision:{candidate.symbol}:{candidate.long_venue}:"
            f"{candidate.short_venue}:{now_ms}"
        )
    )
    candidate.candidate_revision_id = revision_id

    def leg(venue: str) -> dict:
        source = "test_fixture"
        return {
            "venue": venue,
            "canonical_symbol": candidate.symbol,
            "venue_symbol": candidate.symbol,
            "status": "observed",
            "observed_at_ms": now_ms,
            "event_at_ms": 0,
            "received_at_ms": now_ms,
            "sample_id": open_interest_sample_id(
                venue=venue,
                canonical_symbol=candidate.symbol,
                venue_symbol=candidate.symbol,
                observed_at_ms=now_ms,
                source=source,
                raw_value=value_quote,
                value_quote=value_quote,
            ),
            "value_quote": value_quote,
            "raw_value": value_quote,
            "raw_unit": "quote",
            "source": source,
            "contract_multiplier": 1.0,
            "conversion_mark_price": None,
        }

    candidate.entry_open_interest_evidence = {
        "candidate_revision_id": revision_id,
        "long": leg(str(candidate.long_venue)),
        "short": leg(str(candidate.short_venue)),
    }
    return candidate


def _install_single_snapshot_fixture(monkeypatch, snapshot) -> None:
    monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: snapshot)


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
    passive_metadata_payload: Optional[dict] = None
    available_margin_quote: Optional[float] = None
    entry_account_leverage: int = 4
    entry_effective_leverage: int | None = None
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

    async def fetch_account_risk_snapshot(self):
        if self.available_margin_quote is None:
            return None
        return SimpleNamespace(
            available_balance_quote=self.available_margin_quote,
            observed_at_ms=1_000,
        )

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

    async def inspect_entry_leverage(
        self,
        symbol: str,
        leverage: int,
        *,
        notional_quote: float | None = None,
    ) -> EntryLeverageEvidence:
        """Model the verified GET required by the live entry gate."""
        return EntryLeverageEvidence(
            venue=self._venue,
            symbol=symbol,
            requested_leverage=leverage,
            effective_leverage=self._effective_entry_leverage(),
            notional_quote=float(notional_quote or 0.0),
            bracket_verified=True,
            account_verified=True,
            source="fake_inspect",
            observed_at_ms=1_000,
            account_leverage=self.entry_account_leverage,
        )

    async def ensure_entry_leverage(
        self,
        symbol: str,
        leverage: int,
        *,
        notional_quote: float | None = None,
    ) -> EntryLeverageEvidence:
        """Model a successful exchange mutation with a verifiable receipt."""
        self.entry_account_leverage = leverage
        return EntryLeverageEvidence(
            venue=self._venue,
            symbol=symbol,
            requested_leverage=leverage,
            effective_leverage=self._effective_entry_leverage(),
            notional_quote=float(notional_quote or 0.0),
            bracket_verified=True,
            account_verified=True,
            source="fake_ensure",
            observed_at_ms=1_000,
            account_leverage=self.entry_account_leverage,
        )

    def _effective_entry_leverage(self) -> int:
        if self.entry_effective_leverage is not None:
            return self.entry_effective_leverage
        return self.entry_account_leverage

    def passive_metadata(self, symbol: str) -> dict:
        if self.passive_metadata_payload is not None:
            return dict(self.passive_metadata_payload)
        return {
            "min_notional": self._min_notional_quote,
            "min_quantity": 0.001,
            "quantity_step": 0.001,
        }


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
        strategy=StrategyConfig(
            local_l2_enabled=False,
            pending_entry_pre_submit_hedgeable_fill_guard_enabled=False,
            funding_new_entries_enabled=True,
        ),
    )


@pytest.fixture
def binance_fake():
    return FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)


@pytest.fixture
def okx_fake():
    return FakeVenueAdapter(Venue.OKX, _min_notional_quote=10.0)


def test_select_entry_candidates_blocks_first_funding_too_close(config, tmp_journal):
    from lightfee.engine.entry_readiness import EntryReadinessDecision
    from lightfee.engine.runtime import LiveRuntime
    from lightfee.sidecar.snapshot import CandidateInput

    class ReadinessProvider:
        def __init__(self):
            self.calls = []

        def decide(self, candidate, now_ms, *, market_quotes=None):
            self.calls.append((candidate, now_ms))
            return EntryReadinessDecision.allow()

    config.strategy.min_scan_minutes_before_funding = 1
    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal
    readiness_provider = ReadinessProvider()
    runtime.entry_readiness_provider = readiness_provider
    now_ms = 1_000_000
    candidate = CandidateInput(
        long_venue="binance",
        short_venue="bybit",
        symbol="BTCUSDT",
        funding_diff_bps=10.0,
        funding_edge_bps=8.0,
        expected_edge_bps=5.0,
        worst_case_edge_bps=2.0,
        ranking_edge_bps=8.0,
        entry_notional_quote=30.0,
        first_funding_timestamp_ms=now_ms + 59_000,
        funding_timestamp_ms=now_ms + 59_000,
    )
    blockers = {}
    counts = Counter()

    selected = runtime._select_entry_candidates(
        [candidate],
        now_ms=now_ms,
        remaining_slots=1,
        selection_blocker_counts=counts,
        candidate_blockers=blockers,
    )

    assert selected == []
    assert counts["entry_blocked_first_funding_too_close"] == 1
    pair_id = "btcusdt:binance->bybit"
    assert blockers[pair_id] == "entry_blocked_first_funding_too_close"
    assert readiness_provider.calls == []

    records = [
        json.loads(line)
        for line in tmp_journal.path.read_text().splitlines()
        if line.strip()
    ]
    kind_counts = Counter(record["kind"] for record in records)
    assert kind_counts["runtime.entry_blocked_lifecycle_selection"] == 1
    assert kind_counts["runtime.entry_blocked_local_l2_selection"] == 0
    lifecycle_records = [
        record
        for record in records
        if record["kind"] == "runtime.entry_blocked_lifecycle_selection"
    ]
    payload = lifecycle_records[0]["payload"]
    assert payload["reason"] == "entry_blocked_first_funding_too_close"
    assert payload["pair_id"] == pair_id
    assert payload["lifecycle_evidence"] == {
        "first_funding_timestamp_ms": now_ms + 59_000,
        "remaining_to_first_funding_ms": 59_000,
        "effective_min_before_ms": 60_000,
        "source": "selection",
    }
    assert "readiness_evidence" not in payload


def test_pending_first_fill_chooses_lower_loss_unwind_from_fresh_l2(
    config, tmp_journal
):
    """A recovered maker fill must not blindly use its historical price hint."""
    config.venues = [
        VenueConfig(venue="binance", taker_fee_bps=0.5),
        VenueConfig(venue="okx", taker_fee_bps=0.5),
    ]
    config.strategy.local_l2_enabled = True
    config.strategy.entry_readiness_provider = "local_l2"
    config.strategy.max_liquidity_snapshot_age_ms = 1_000
    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal
    now_ms = 10_000
    for venue, bid, ask in (
        ("binance", 99.0, 101.0),
        ("okx", 95.0, 96.0),
    ):
        book = runtime.local_l2_runtime.ensure_book(venue, "BTCUSDT")
        book.status = L2BookStatus.HOT
        book.bids = [PriceLevel(bid, 10.0)]
        book.asks = [PriceLevel(ask, 10.0)]
        book.observed_at_ms = now_ms
    pending = PendingEntry(
        pending_id="pending-first-fill",
        symbol="BTCUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.OKX,
        target_quantity=0.1,
        long_side=Side.BUY,
        short_side=Side.SELL,
        created_at_ms=now_ms,
        maker_leg="long",
        maker_leg_filled=0.1,
        maker_fill_price=100.0,
    )

    decision = runtime._pending_entry_post_first_fill_decision(
        pending,
        entry_id=pending.pending_id,
        now_ms=now_ms,
    )

    assert decision["action"] == "unwind_first_leg"
    assert decision["hedge_price"] == pytest.approx(95.0)
    assert decision["unwind_price"] == pytest.approx(99.0)
    assert decision["unwind_first_leg_loss_quote"] < decision["complete_hedge_loss_quote"]


def test_pending_first_fill_corrupt_l2_uses_conservative_hedge_fallback(
    config, tmp_journal
):
    """A HOT lifecycle state cannot override whole-book integrity for hedging."""
    config.strategy.local_l2_enabled = True
    config.strategy.entry_readiness_provider = "local_l2"
    config.strategy.max_liquidity_snapshot_age_ms = 1_000
    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal
    now_ms = 10_000
    for venue, bid, asks in (
        ("binance", 99.0, [101.0, 101.0]),
        ("okx", 95.0, [96.0]),
    ):
        book = runtime.local_l2_runtime.ensure_book(venue, "BTCUSDT")
        book.status = L2BookStatus.HOT
        book.bids = [PriceLevel(bid, 10.0)]
        book.asks = [PriceLevel(price, 10.0) for price in asks]
        book.observed_at_ms = now_ms
    pending = PendingEntry(
        pending_id="pending-corrupt-l2",
        symbol="BTCUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.OKX,
        target_quantity=0.1,
        long_side=Side.BUY,
        short_side=Side.SELL,
        created_at_ms=now_ms,
        maker_leg="long",
        maker_leg_filled=0.1,
        maker_fill_price=100.0,
    )

    assert runtime._pending_entry_post_first_fill_decision(
        pending,
        entry_id=pending.pending_id,
        now_ms=now_ms,
    ) == {
        "action": "complete_hedge",
        "reason": "post_first_fill_market_data_unavailable_complete_hedge",
        "hedge_price": 100.0,
        "market_evidence": {},
    }


def test_pending_first_fill_compares_fee_inclusive_hedge_and_unwind_costs(
    config, tmp_journal
):
    """Recovered residual handling uses the same all-in decision contract."""
    config.strategy.local_l2_enabled = True
    config.strategy.entry_readiness_provider = "local_l2"
    config.strategy.max_liquidity_snapshot_age_ms = 1_000
    # Unwinding has the smaller raw price loss, but a much higher taker fee.
    config.venues = [
        VenueConfig(venue="binance", taker_fee_bps=100.0),
        VenueConfig(venue="okx", taker_fee_bps=0.0),
    ]
    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal
    now_ms = 10_000
    for venue, bid, ask in (
        ("binance", 99.99, 101.0),
        ("okx", 99.5, 100.0),
    ):
        book = runtime.local_l2_runtime.ensure_book(venue, "BTCUSDT")
        book.status = L2BookStatus.HOT
        book.bids = [PriceLevel(bid, 10.0)]
        book.asks = [PriceLevel(ask, 10.0)]
        book.observed_at_ms = now_ms
    pending = PendingEntry(
        pending_id="pending-fee-inclusive",
        symbol="BTCUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.OKX,
        target_quantity=0.1,
        long_side=Side.BUY,
        short_side=Side.SELL,
        created_at_ms=now_ms,
        maker_leg="long",
        maker_leg_filled=0.1,
        maker_fill_price=100.0,
    )

    decision = runtime._pending_entry_post_first_fill_decision(
        pending,
        entry_id=pending.pending_id,
        now_ms=now_ms,
    )

    assert decision["action"] == "complete_hedge"
    assert decision["unwind_first_leg_price_loss_quote"] < decision[
        "complete_hedge_price_loss_quote"
    ]
    assert decision["unwind_first_leg_fee_quote"] > decision[
        "complete_hedge_fee_quote"
    ]
    assert decision["unwind_first_leg_loss_quote"] > decision[
        "complete_hedge_loss_quote"
    ]


def test_pending_first_fill_uses_multilevel_remaining_depth(config, tmp_journal):
    """Recovered pending entries compare the remaining base quantity, not BBO."""
    config.venues = [
        VenueConfig(venue="binance", taker_fee_bps=0.0),
        VenueConfig(venue="okx", taker_fee_bps=0.0),
    ]
    config.strategy.local_l2_enabled = True
    config.strategy.entry_readiness_provider = "local_l2"
    config.strategy.max_liquidity_snapshot_age_ms = 1_000
    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal
    now_ms = 10_000
    books = {
        "binance": {
            "bids": [(99.9, 0.05), (98.0, 0.15)],
            "asks": [(101.0, 10.0)],
        },
        "okx": {
            "bids": [(99.7, 0.05), (99.4, 0.15)],
            "asks": [(100.5, 10.0)],
        },
    }
    for venue, levels in books.items():
        book = runtime.local_l2_runtime.ensure_book(venue, "BTCUSDT")
        book.status = L2BookStatus.HOT
        book.bids = [PriceLevel(price, size) for price, size in levels["bids"]]
        book.asks = [PriceLevel(price, size) for price, size in levels["asks"]]
        book.observed_at_ms = now_ms
    pending = PendingEntry(
        pending_id="pending-multilevel-depth",
        symbol="BTCUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.OKX,
        target_quantity=0.2,
        long_side=Side.BUY,
        short_side=Side.SELL,
        created_at_ms=now_ms,
        maker_leg="long",
        maker_leg_filled=0.2,
        maker_fill_price=100.0,
    )

    decision = runtime._pending_entry_post_first_fill_decision(
        pending,
        entry_id=pending.pending_id,
        now_ms=now_ms,
    )

    assert decision["action"] == "complete_hedge"
    assert decision["hedge_price"] == pytest.approx(99.4)
    assert decision["complete_hedge_loss_quote"] < decision[
        "unwind_first_leg_loss_quote"
    ]
    assert decision["market_evidence"]["l2_depth"] == {
        "source": "local_l2_multilevel_vwap",
        "remaining_base_quantity": pytest.approx(0.2),
        "hedge_vwap": pytest.approx(99.475),
        "hedge_filled_base_quantity": pytest.approx(0.2),
        "hedge_sweep_limit": pytest.approx(99.4),
        "hedge_l2_complete": True,
        "unwind_vwap": pytest.approx(98.475),
        "unwind_filled_base_quantity": pytest.approx(0.2),
        "unwind_sweep_limit": pytest.approx(98.0),
        "unwind_l2_complete": True,
    }


def test_pending_first_fill_incomplete_depth_selects_residual_repair(
    config, tmp_journal
):
    config.strategy.local_l2_enabled = True
    config.strategy.entry_readiness_provider = "local_l2"
    config.strategy.max_liquidity_snapshot_age_ms = 1_000
    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal
    now_ms = 10_000
    for venue in ("binance", "okx"):
        book = runtime.local_l2_runtime.ensure_book(venue, "BTCUSDT")
        book.status = L2BookStatus.HOT
        book.bids = [PriceLevel(99.0, 0.05)]
        book.asks = [PriceLevel(101.0, 0.05)]
        book.observed_at_ms = now_ms
    pending = PendingEntry(
        pending_id="pending-incomplete-depth",
        symbol="BTCUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.OKX,
        target_quantity=0.2,
        long_side=Side.BUY,
        short_side=Side.SELL,
        created_at_ms=now_ms,
        maker_leg="long",
        maker_leg_filled=0.2,
        maker_fill_price=100.0,
    )

    decision = runtime._pending_entry_post_first_fill_decision(
        pending,
        entry_id=pending.pending_id,
        now_ms=now_ms,
    )

    assert decision["action"] == "unwind_first_leg"
    assert (
        decision["reason"]
        == "pending_post_first_fill_incomplete_l2_depth_residual_repair"
    )
    assert decision["market_evidence"]["l2_depth"]["hedge_l2_complete"] is False
    assert decision["market_evidence"]["l2_depth"]["unwind_l2_complete"] is False


@pytest.mark.asyncio
async def test_pending_first_fill_incomplete_depth_routes_through_finalization(
    config, tmp_journal, monkeypatch
):
    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal
    pending = PendingEntry(
        pending_id="pending-residual-route",
        symbol="BTCUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.OKX,
        target_quantity=0.2,
        long_side=Side.BUY,
        short_side=Side.SELL,
        created_at_ms=1_000,
        maker_leg="long",
        maker_leg_filled=0.2,
        maker_fill_price=100.0,
    )

    monkeypatch.setattr(runtime, "get_venue_adapter", lambda _venue: object())
    monkeypatch.setattr(
        runtime,
        "_pending_entry_post_first_fill_decision",
        lambda *_args, **_kwargs: {
            "action": "unwind_first_leg",
            "reason": "pending_post_first_fill_incomplete_l2_depth_residual_repair",
            "hedge_price": 99.0,
            "market_evidence": {},
        },
    )
    finalize = AsyncMock(return_value=True)
    abort = AsyncMock(return_value=True)
    monkeypatch.setattr(runtime, "_finalize_pending_entry", finalize)
    monkeypatch.setattr(runtime, "_abort_pending_entry", abort)

    assert (
        await runtime._drive_missing_hedge_live(
            pending,
            pending.pending_id,
            2_000,
        )
        is False
    )
    finalize.assert_awaited_once_with(pending, pending.pending_id, 2_000)
    abort.assert_not_awaited()


def test_pending_first_fill_rejects_future_ws_bbo_timestamp(config, tmp_journal):
    """A future-dated top book cannot steer a post-fill hedge/unwind choice."""
    config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
    config.strategy.max_liquidity_snapshot_age_ms = 1_000
    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal
    now_ms = 10_000
    for venue, bid, ask in (("binance", 99.0, 101.0), ("okx", 95.0, 96.0)):
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue=venue,
                symbol="BTCUSDT",
                bid=bid,
                ask=ask,
                observed_at_ms=now_ms + 1,
            )
        )
    pending = PendingEntry(
        pending_id="pending-future-ws-bbo",
        symbol="BTCUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.OKX,
        target_quantity=0.1,
        long_side=Side.BUY,
        short_side=Side.SELL,
        created_at_ms=now_ms,
        maker_leg="long",
        maker_leg_filled=0.1,
        maker_fill_price=100.0,
    )

    decision = runtime._pending_entry_post_first_fill_decision(
        pending,
        entry_id=pending.pending_id,
        now_ms=now_ms,
    )

    assert decision == {
        "action": "complete_hedge",
        "reason": "post_first_fill_market_data_unavailable_complete_hedge",
        "hedge_price": 100.0,
        "market_evidence": {},
    }


def test_select_entry_candidates_does_not_own_recovery_ledger_semantics(
    config, tmp_journal
):
    from lightfee.engine.entry_readiness import EntryReadinessDecision
    from lightfee.engine.recovery_ledger import RecoveryLedger
    from lightfee.engine.runtime import LiveRuntime
    from lightfee.sidecar.snapshot import CandidateInput

    class ReadinessProvider:
        def __init__(self):
            self.calls = []

        def decide(self, candidate, now_ms, *, market_quotes=None):
            self.calls.append((candidate, now_ms))
            return EntryReadinessDecision.allow()

    config.strategy.min_scan_minutes_before_funding = 0
    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal
    readiness_provider = ReadinessProvider()
    runtime.entry_readiness_provider = readiness_provider
    runtime.recovery_ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={"open_positions": [], "pending_entries": []},
        exchange_truth={
            "truth_available": True,
            "positions": [],
            "open_orders": [
                {
                    "venue": "bybit",
                    "symbol": "TRXUSDT",
                    "side": "buy",
                    "quantity": 72.0,
                    "reduce_only": False,
                }
            ],
        },
    )
    now_ms = 1_000_000
    candidate = CandidateInput(
        long_venue="binance",
        short_venue="bybit",
        symbol="BTCUSDT",
        funding_diff_bps=10.0,
        funding_edge_bps=8.0,
        expected_edge_bps=5.0,
        worst_case_edge_bps=2.0,
        ranking_edge_bps=8.0,
        entry_notional_quote=30.0,
        first_funding_timestamp_ms=now_ms + 300_000,
        funding_timestamp_ms=now_ms + 300_000,
    )
    blockers = {}
    counts = Counter()

    selected = runtime._select_entry_candidates(
        [candidate],
        now_ms=now_ms,
        remaining_slots=1,
        selection_blocker_counts=counts,
        candidate_blockers=blockers,
    )

    assert selected == [candidate]
    assert counts["entry_blocked_recovery_ledger"] == 0
    assert blockers == {}
    assert readiness_provider.calls == [(candidate, now_ms)]

    records = [
        json.loads(line)
        for line in tmp_journal.path.read_text().splitlines()
        if line.strip()
    ]
    kind_counts = Counter(record["kind"] for record in records)
    assert kind_counts["runtime.entry_blocked_lifecycle_selection"] == 0
    assert kind_counts["runtime.entry_blocked_local_l2_selection"] == 0


def test_select_entry_candidates_falls_back_to_next_candidate_when_top_is_blocked(
    config, tmp_journal
):
    from lightfee.engine.entry_readiness import EntryReadinessDecision
    from lightfee.engine.runtime import LiveRuntime
    from lightfee.sidecar.snapshot import CandidateInput

    class ReadinessProvider:
        def decide(self, candidate, now_ms, *, market_quotes=None):
            if candidate.symbol == "TOPUSDT":
                return EntryReadinessDecision.block(
                    "entry_quote_stale",
                    evidence={"blocker_family": "stale_quote", "quote_age_ms": 9000},
                )
            return EntryReadinessDecision.allow()

    config.strategy.min_scan_minutes_before_funding = 0
    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal
    runtime.entry_readiness_provider = ReadinessProvider()
    now_ms = 1_000_000
    top_candidate = CandidateInput(
        long_venue="binance",
        short_venue="bybit",
        symbol="TOPUSDT",
        funding_diff_bps=20.0,
        funding_edge_bps=20.0,
        expected_edge_bps=15.0,
        worst_case_edge_bps=10.0,
        ranking_edge_bps=20.0,
        entry_notional_quote=30.0,
        first_funding_timestamp_ms=now_ms + 300_000,
        funding_timestamp_ms=now_ms + 300_000,
    )
    fallback_candidate = CandidateInput(
        long_venue="binance",
        short_venue="bybit",
        symbol="NEXTUSDT",
        funding_diff_bps=10.0,
        funding_edge_bps=10.0,
        expected_edge_bps=8.0,
        worst_case_edge_bps=6.0,
        ranking_edge_bps=10.0,
        entry_notional_quote=30.0,
        first_funding_timestamp_ms=now_ms + 300_000,
        funding_timestamp_ms=now_ms + 300_000,
    )
    blockers = {}
    selection_counts = Counter()

    selected = runtime._select_entry_candidates(
        [top_candidate, fallback_candidate],
        now_ms=now_ms,
        remaining_slots=1,
        selection_blocker_counts=selection_counts,
        candidate_blockers=blockers,
    )

    assert selected == [fallback_candidate]
    assert blockers["topusdt:binance->bybit"] == "entry_quote_stale"
    assert "nextusdt:binance->bybit" not in blockers
    assert selection_counts["entry_quote_stale"] == 1


def test_select_entry_candidates_does_not_attach_lifecycle_evidence_to_readiness_block(
    config, tmp_journal
):
    from lightfee.engine.entry_readiness import EntryReadinessDecision
    from lightfee.engine.runtime import LiveRuntime
    from lightfee.sidecar.snapshot import CandidateInput

    class DenyProvider:
        def decide(self, candidate, now_ms, *, market_quotes=None):
            return EntryReadinessDecision.block(
                "entry_readiness_provider_denied",
                evidence={"provider": "unit"},
            )

    config.strategy.min_scan_minutes_before_funding = 0
    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal
    runtime.entry_readiness_provider = DenyProvider()
    now_ms = 1_000_000
    candidate = CandidateInput(
        long_venue="binance",
        short_venue="bybit",
        symbol="BTCUSDT",
        funding_diff_bps=10.0,
        funding_edge_bps=8.0,
        expected_edge_bps=5.0,
        worst_case_edge_bps=2.0,
        ranking_edge_bps=8.0,
        entry_notional_quote=30.0,
        first_funding_timestamp_ms=now_ms + 300_000,
        funding_timestamp_ms=now_ms + 300_000,
    )

    selected = runtime._select_entry_candidates(
        [candidate],
        now_ms=now_ms,
        remaining_slots=1,
        selection_blocker_counts=Counter(),
        candidate_blockers={},
    )

    assert selected == []
    records = [
        json.loads(line)
        for line in tmp_journal.path.read_text().splitlines()
        if line.strip()
    ]
    assert not [
        record
        for record in records
        if record["kind"] == "runtime.entry_blocked_lifecycle_selection"
    ]
    payload = next(
        record["payload"]
        for record in records
        if record["kind"] == "runtime.entry_blocked_local_l2_selection"
    )
    assert payload["reason"] == "entry_readiness_provider_denied"
    assert "lifecycle_evidence" not in payload
    assert payload["readiness_evidence"] == {"provider": "unit"}


def test_v1_tradeable_no_entry_reason_classifies_lifecycle_blockers():
    from lightfee.engine.runtime import LiveRuntime

    assert (
        LiveRuntime._v1_tradeable_no_entry_reason(
            Counter({"entry_blocked_first_funding_too_close": 1})
        )
        == "tradeable_candidates_blocked_by_lifecycle"
    )
    assert (
        LiveRuntime._v1_tradeable_no_entry_reason(
            Counter({"entry_blocked_recovery_ledger": 1})
        )
        == "tradeable_candidates_blocked_by_recovery_ledger"
    )


def test_scan_no_entry_diagnostics_buckets_lifecycle_outside_readiness(
    config, tmp_journal
):
    from lightfee.engine.runtime import LiveRuntime

    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal

    runtime._emit_scan_no_entry_diagnostics(
        reason="tradeable_candidates_blocked_by_lifecycle",
        snapshot=SimpleNamespace(candidates=[]),
        tradeable=[],
        selected_candidate_count=0,
        dispatched_candidate_count=0,
        remaining_slots=1,
        tradeable_selection_blocker_counts=Counter({
            "entry_blocked_first_funding_too_close": 1,
            "entry_blocked_recovery_ledger": 1,
            "entry_local_l2_waiting_for_dual_ready": 2,
        }),
        candidate_blockers={},
        now_ms=1_000_000,
        admission_blocker_counts=Counter({
            "entry_local_l2_waiting_for_primary_tracking": 3,
        }),
    )

    records = [
        json.loads(line)
        for line in tmp_journal.path.read_text().splitlines()
        if line.strip()
    ]
    payload = next(
        record["payload"]
        for record in records
        if record["kind"] == "scan.no_entry_diagnostics"
    )
    assert payload["selection_bucket_counts"] == {
        "not_primary_tracked": 3,
        "primary_tracked_not_ready": 2,
        "lifecycle_selection_blocked": 2,
    }


def test_scan_no_entry_diagnostics_exposes_capacity_context(config, tmp_journal):
    from lightfee.engine.runtime import LiveRuntime

    config.strategy.max_concurrent_positions = 8
    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal
    runtime.state.open_positions["pos_open"] = OpenPosition(
        position_id="pos_open",
        symbol="KATUSDT",
        long_venue=Venue.OKX,
        short_venue=Venue.BYBIT,
        long_quantity=7600.0,
        short_quantity=7600.0,
        long_entry_price=0.006251,
        short_entry_price=0.006252,
        opened_at_ms=999_000,
        matched_quantity=7600.0,
    )

    runtime._emit_scan_no_entry_diagnostics(
        reason="tradeable_candidates_blocked_by_entry_local_l2_readiness",
        snapshot=SimpleNamespace(candidates=[]),
        tradeable=[],
        selected_candidate_count=0,
        dispatched_candidate_count=0,
        remaining_slots=7,
        tradeable_selection_blocker_counts=Counter({
            "entry_local_l2_waiting_for_dual_ready": 2,
        }),
        candidate_blockers={},
        now_ms=1_000_000,
        admission_blocker_counts=Counter(),
    )

    records = [
        json.loads(line)
        for line in tmp_journal.path.read_text().splitlines()
        if line.strip()
    ]
    payload = next(
        record["payload"]
        for record in records
        if record["kind"] == "scan.no_entry_diagnostics"
    )
    assert payload["max_concurrent_positions"] == 8
    assert payload["open_position_count"] == 1
    assert payload["remaining_slots"] == 7
    assert payload["capacity_blocked"] is False


def test_entry_opportunity_funnel_emitted_for_positive_dispatch_path(
    config,
    tmp_journal,
):
    from lightfee.engine.runtime import LiveRuntime
    from lightfee.sidecar.snapshot import CandidateInput

    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal
    runtime.state.last_scan = {
        "raw_candidate_count": 4659,
        "strategy_tradeable_count": 72,
        "catalog_admission_balance_passed_count": 56,
        "snapshot_freshness_candidate_count": 10,
        "quote_revalidate_target_count": 18,
        "quote_revalidate_resolved_count": 17,
        "quote_revalidate_failed_count": 1,
        "quote_revalidate_skipped_untracked_count": 54,
        "top_quote_blocker_buckets": {"quote_stale": 54},
    }
    candidate = CandidateInput(
        long_venue="okx",
        short_venue="bybit",
        symbol="HOMEUSDT",
        funding_diff_bps=10.0,
        funding_edge_bps=8.0,
        expected_edge_bps=5.0,
        worst_case_edge_bps=2.0,
        ranking_edge_bps=8.0,
        entry_notional_quote=30.0,
        first_funding_timestamp_ms=1_300_000,
        funding_timestamp_ms=1_300_000,
    )

    runtime._emit_entry_opportunity_funnel(
        reason="entries_dispatched",
        snapshot=SimpleNamespace(candidates=[candidate]),
        tradeable=[candidate],
        selected=[candidate],
        dispatched_candidate_count=1,
        remaining_slots=8,
        tradeable_selection_blocker_counts=Counter({
            "entry_waiting_for_finalization_window_too_early": 3,
        }),
        candidate_blockers={"hemiusdt:binance->hyperliquid": "quote_stale"},
        now_ms=1_000_000,
        admission_blocker_counts=Counter(),
    )

    records = [
        json.loads(line)
        for line in tmp_journal.path.read_text().splitlines()
        if line.strip()
    ]
    payload = next(
        record["payload"]
        for record in records
        if record["kind"] == "entry.opportunity_funnel"
    )

    assert payload["reason"] == "entries_dispatched"
    assert payload["pipeline_counts"]["raw_candidates"] == 4659
    assert payload["pipeline_counts"]["strategy_tradeable"] == 72
    assert payload["pipeline_counts"]["catalog_admission_balance_passed"] == 56
    assert payload["pipeline_counts"]["v1_primary_shadow_tracked"] == 10
    assert payload["pipeline_counts"]["quote_revalidate_target"] == 18
    assert payload["pipeline_counts"]["quote_revalidate_resolved"] == 17
    assert payload["pipeline_counts"]["quote_revalidate_failed"] == 1
    assert payload["pipeline_counts"]["quote_revalidate_skipped_untracked"] == 54
    assert payload["pipeline_counts"]["selected"] == 1
    assert payload["pipeline_counts"]["dispatched"] == 1
    assert payload["candidate_stage_blocked_counts"]["entry_selection"] == 3
    assert payload["top_quote_blocker_buckets"] == {"quote_stale": 54}
    assert payload["blocked_candidate_samples"] == [
        {
            "pair_id": "hemiusdt:binance->hyperliquid",
            "selection_blocker": "quote_stale",
        }
    ]
    assert payload["selection_blocked_candidate_samples"] == [
        {
            "pair_id": "hemiusdt:binance->hyperliquid",
            "stage": "entry_selection",
            "reason_family": "quote",
            "selection_blocker": "quote_stale",
        }
    ]
    assert payload["selected_candidates"] == [
        {
            "rank": 1,
            "pair_id": "homeusdt:okx->bybit",
            "symbol": "HOMEUSDT",
            "long_venue": "okx",
            "short_venue": "bybit",
            "ranking_edge_bps": 8.0,
            "entry_notional_quote": 30.0,
            "remaining_ms": 300000,
        }
    ]
    assert runtime.state.last_scan["opportunity_funnel"]["dispatched_candidate_count"] == 1


def test_select_entry_candidates_records_final_selection_skip_reasons(
    config,
    tmp_journal,
):
    from lightfee.engine.entry_readiness import EntryReadinessDecision
    from lightfee.engine.runtime import LiveRuntime
    from lightfee.sidecar.snapshot import CandidateInput

    class ReadinessProvider:
        def decide(self, candidate, now_ms, *, market_quotes=None):
            return EntryReadinessDecision.allow()

    def candidate(symbol: str, ranking: float = 8.0) -> CandidateInput:
        return CandidateInput(
            long_venue="okx",
            short_venue="bybit",
            symbol=symbol,
            funding_diff_bps=10.0,
            funding_edge_bps=8.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=ranking,
            entry_notional_quote=30.0,
            first_funding_timestamp_ms=1_300_000,
            funding_timestamp_ms=1_300_000,
        )

    config.strategy.entry_window_secs = 600
    config.strategy.min_scan_minutes_before_funding = 0
    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal
    runtime.entry_readiness_provider = ReadinessProvider()
    runtime.state.open_positions["btc-open"] = OpenPosition(
        position_id="btc-open",
        symbol="BTCUSDT",
        long_venue=Venue.OKX,
        short_venue=Venue.BYBIT,
        long_quantity=1.0,
        short_quantity=1.0,
        long_entry_price=1.0,
        short_entry_price=1.0,
        opened_at_ms=999_000,
        matched_quantity=1.0,
    )
    runtime.state.pending_entries["eth-pending"] = PendingEntry(
        pending_id="eth-pending",
        symbol="ETHUSDT",
        long_venue=Venue.OKX,
        short_venue=Venue.BYBIT,
        target_quantity=1.0,
        long_side=Side.BUY,
        short_side=Side.SELL,
        created_at_ms=999_000,
    )
    runtime.state.pending_residual_repairs.append({
        "pair_id": "xrpusdt:okx->bybit",
        "symbol": "XRPUSDT",
    })
    counts = Counter()
    blockers = {}

    selected = runtime._select_entry_candidates(
        [
            candidate("BTCUSDT", 12.0),
            candidate("ETHUSDT", 11.0),
            candidate("XRPUSDT", 10.0),
            candidate("HOMEUSDT", 9.0),
            candidate("HOMEUSDT", 8.0),
        ],
        now_ms=1_000_000,
        remaining_slots=8,
        selection_blocker_counts=counts,
        candidate_blockers=blockers,
    )

    assert [item.symbol for item in selected] == ["HOMEUSDT"]
    assert counts["entry_selection_symbol_has_open_position"] == 1
    assert counts["entry_selection_symbol_has_pending_entry"] == 1
    assert counts["entry_selection_pair_has_pending_residual_repair"] == 1
    assert counts["entry_selection_duplicate_symbol"] == 1
    assert blockers["btcusdt:okx->bybit"] == "entry_selection_symbol_has_open_position"
    assert blockers["ethusdt:okx->bybit"] == "entry_selection_symbol_has_pending_entry"
    assert (
        blockers["xrpusdt:okx->bybit"]
        == "entry_selection_pair_has_pending_residual_repair"
    )
    assert "homeusdt:okx->bybit" not in blockers


@pytest.mark.asyncio
async def test_select_and_dispatch_allows_other_symbol_when_active_position_has_capacity(
    config, tmp_journal
):
    from lightfee.engine.entry_readiness import EntryReadinessDecision
    from lightfee.marketdata.ws_bbo import TopBookQuote
    from lightfee.sidecar.snapshot import CandidateInput

    class ReadinessProvider:
        def decide(self, candidate, now_ms, *, market_quotes=None):
            return EntryReadinessDecision.allow()

    class CapturingExecutor:
        ctx = None

        async def execute(self, ctx):
            self.ctx = ctx
            return EntryExecutionResult(
                route=ExecutionRoute.PASSIVE_INCREMENTAL,
                state=EntryState.COMPLETED,
            )

    config.strategy.max_concurrent_positions = 8
    config.strategy.min_scan_minutes_before_funding = 0
    binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
    bybit = FakeVenueAdapter(Venue.BYBIT, _min_notional_quote=10.0)
    adapters = {Venue.BINANCE: binance, Venue.BYBIT: bybit}
    runtime = LiveRuntime(config, venue_adapters=adapters)
    runtime.journal = tmp_journal
    runtime.entry_readiness_provider = ReadinessProvider()
    executor = CapturingExecutor()
    runtime.entry_executor = executor
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING
    runtime.state.open_positions["pos_active"] = OpenPosition(
        position_id="pos_active",
        symbol="KATUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.BYBIT,
        long_quantity=7600.0,
        short_quantity=7600.0,
        long_entry_price=0.006251,
        short_entry_price=0.006252,
        opened_at_ms=999_000,
        matched_quantity=7600.0,
    )

    now_ms = 1_000_000
    for venue, bid, ask in (
        ("binance", 50000.0, 50010.0),
        ("bybit", 49990.0, 50000.0),
    ):
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue=venue,
                symbol="BTCUSDT",
                bid=bid,
                ask=ask,
                observed_at_ms=now_ms,
                received_at_ms=now_ms,
                source=f"{venue}_bbo_ws",
            )
        )
    candidate = CandidateInput(
        long_venue="binance",
        short_venue="bybit",
        symbol="BTCUSDT",
        funding_diff_bps=10.0,
        funding_edge_bps=8.0,
        expected_edge_bps=5.0,
        worst_case_edge_bps=2.0,
        ranking_edge_bps=8.0,
        entry_notional_quote=500.0,
        first_funding_timestamp_ms=now_ms + 300_000,
        funding_timestamp_ms=now_ms + 300_000,
    )

    selected = runtime._select_entry_candidates(
        [candidate],
        now_ms=now_ms,
        remaining_slots=7,
        selection_blocker_counts=Counter(),
        candidate_blockers={},
    )

    assert selected == [candidate]
    dispatched = await runtime._dispatch_entry(candidate, now_ms, price_hint=50000.0)

    assert dispatched is True
    assert executor.ctx is not None
    assert executor.ctx.symbol == "BTCUSDT"
    blocked_gates = [
        record
        for record in runtime.journal.read_all()
        if record["kind"] == "runtime.entry_blocked_gate"
    ]
    assert not blocked_gates


@pytest.mark.asyncio
async def test_dispatch_entry_cancels_selected_context_when_no_submit_evidence(
    config, tmp_journal
):
    from lightfee.marketdata.ws_bbo import TopBookQuote
    from lightfee.sidecar.snapshot import CandidateInput

    class SlowNoSubmitExecutor:
        ctx = None
        cancelled = False

        async def execute(self, ctx):
            self.ctx = ctx
            try:
                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                self.cancelled = True
                raise
            return EntryExecutionResult(
                route=ExecutionRoute.PASSIVE_INCREMENTAL,
                state=EntryState.COMPLETED,
            )

    config.strategy.max_concurrent_positions = 8
    config.strategy.min_scan_minutes_before_funding = 0
    config.strategy.selected_submit_deadline_ms = 1
    binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
    bybit = FakeVenueAdapter(Venue.BYBIT, _min_notional_quote=10.0)
    adapters = {Venue.BINANCE: binance, Venue.BYBIT: bybit}
    runtime = LiveRuntime(config, venue_adapters=adapters)
    runtime.journal = tmp_journal
    executor = SlowNoSubmitExecutor()
    runtime.entry_executor = executor
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING

    now_ms = 1_000_000
    for venue, bid, ask in (
        ("binance", 1.0, 1.01),
        ("bybit", 0.99, 1.0),
    ):
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue=venue,
                symbol="NOSUBUSDT",
                bid=bid,
                ask=ask,
                observed_at_ms=now_ms,
                received_at_ms=now_ms,
                source=f"{venue}_bbo_ws",
            )
        )
    candidate = CandidateInput(
        long_venue="binance",
        short_venue="bybit",
        symbol="NOSUBUSDT",
        funding_diff_bps=10.0,
        funding_edge_bps=8.0,
        expected_edge_bps=5.0,
        worst_case_edge_bps=2.0,
        ranking_edge_bps=8.0,
        entry_notional_quote=500.0,
        first_funding_timestamp_ms=now_ms + 300_000,
        funding_timestamp_ms=now_ms + 300_000,
    )

    dispatched = await runtime._dispatch_entry(
        candidate, now_ms, price_hint=1.0
    )

    records = runtime.journal.read_all()
    kinds = [record["kind"] for record in records]
    assert dispatched is False
    assert executor.cancelled is True
    assert "runtime.entry_selected_submit_deadline_exceeded" in kinds
    assert "runtime.entry_dispatched" not in kinds
    rejected = [
        record["payload"]
        for record in records
        if record["kind"] == "review.candidate_rejected"
    ]
    assert rejected[-1]["rejected_stage"] == "selected_pre_submit_deadline"


@pytest.mark.asyncio
async def test_selected_submit_deadline_does_not_wait_for_slow_cancel_cleanup(
    config, tmp_journal
):
    from lightfee.marketdata.ws_bbo import TopBookQuote
    from lightfee.sidecar.snapshot import CandidateInput

    class SlowCancelCleanupExecutor:
        def __init__(self) -> None:
            self.cancelled = False
            self.cleanup_finished = asyncio.Event()

        async def execute(self, _ctx):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled = True
                await asyncio.sleep(0.15)
                self.cleanup_finished.set()
                raise

    config.strategy.max_concurrent_positions = 8
    config.strategy.min_scan_minutes_before_funding = 0
    config.strategy.selected_submit_deadline_ms = 20
    binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
    bybit = FakeVenueAdapter(Venue.BYBIT, _min_notional_quote=10.0)
    runtime = LiveRuntime(
        config,
        venue_adapters={Venue.BINANCE: binance, Venue.BYBIT: bybit},
    )
    runtime.journal = tmp_journal
    executor = SlowCancelCleanupExecutor()
    runtime.entry_executor = executor
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING

    now_ms = 1_000_000
    for venue, bid, ask in (
        ("binance", 1.0, 1.01),
        ("bybit", 0.99, 1.0),
    ):
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue=venue,
                symbol="SLOWCANCELUSDT",
                bid=bid,
                ask=ask,
                observed_at_ms=now_ms,
                received_at_ms=now_ms,
                source=f"{venue}_bbo_ws",
            )
        )
    candidate = CandidateInput(
        long_venue="binance",
        short_venue="bybit",
        symbol="SLOWCANCELUSDT",
        funding_diff_bps=10.0,
        funding_edge_bps=8.0,
        expected_edge_bps=5.0,
        worst_case_edge_bps=2.0,
        ranking_edge_bps=8.0,
        entry_notional_quote=500.0,
        first_funding_timestamp_ms=now_ms + 300_000,
        funding_timestamp_ms=now_ms + 300_000,
    )

    loop = asyncio.get_running_loop()
    started = loop.time()
    dispatched = await runtime._dispatch_entry(candidate, now_ms, price_hint=1.0)
    elapsed_s = loop.time() - started

    cleanup_tasks = (
        runtime.entry_dispatch_runtime._selected_submit_cancel_cleanup_tasks
    )
    assert dispatched is False
    assert executor.cancelled is True
    assert elapsed_s < 0.08
    assert cleanup_tasks
    records_at_return = runtime.journal.read_all()
    assert any(
        record["kind"] == "runtime.entry_selected_submit_deadline_exceeded"
        for record in records_at_return
    )

    await asyncio.wait_for(executor.cleanup_finished.wait(), timeout=0.3)
    await asyncio.sleep(0)

    assert not cleanup_tasks
    assert runtime.journal.read_all() == records_at_return


@pytest.mark.asyncio
async def test_dispatch_entry_keeps_order_truth_path_when_submit_evidence_exists(
    config, tmp_journal
):
    from lightfee.marketdata.ws_bbo import TopBookQuote
    from lightfee.sidecar.snapshot import CandidateInput

    class SlowSubmittedExecutor:
        async def execute(self, ctx):
            tmp_journal.append(
                "order.submitted",
                {
                    "entry_id": ctx.entry_id,
                    "symbol": ctx.symbol,
                    "client_order_id": f"{ctx.entry_id}-m",
                },
            )
            await asyncio.sleep(0.02)
            return EntryExecutionResult(
                route=ExecutionRoute.PASSIVE_INCREMENTAL,
                state=EntryState.COMPLETED,
            )

    config.strategy.max_concurrent_positions = 8
    config.strategy.min_scan_minutes_before_funding = 0
    config.strategy.selected_submit_deadline_ms = 1
    binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
    bybit = FakeVenueAdapter(Venue.BYBIT, _min_notional_quote=10.0)
    adapters = {Venue.BINANCE: binance, Venue.BYBIT: bybit}
    runtime = LiveRuntime(config, venue_adapters=adapters)
    runtime.journal = tmp_journal
    runtime.entry_executor = SlowSubmittedExecutor()
    runtime.state.lifecycle = EngineLifecycle.RUNNING
    runtime.state.risk_mode = GlobalRiskMode.RUNNING

    now_ms = 1_000_000
    for venue, bid, ask in (
        ("binance", 1.0, 1.01),
        ("bybit", 0.99, 1.0),
    ):
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue=venue,
                symbol="SUBUSDT",
                bid=bid,
                ask=ask,
                observed_at_ms=now_ms,
                received_at_ms=now_ms,
                source=f"{venue}_bbo_ws",
            )
        )
    candidate = CandidateInput(
        long_venue="binance",
        short_venue="bybit",
        symbol="SUBUSDT",
        funding_diff_bps=10.0,
        funding_edge_bps=8.0,
        expected_edge_bps=5.0,
        worst_case_edge_bps=2.0,
        ranking_edge_bps=8.0,
        entry_notional_quote=500.0,
        first_funding_timestamp_ms=now_ms + 300_000,
        funding_timestamp_ms=now_ms + 300_000,
    )

    dispatched = await runtime._dispatch_entry(
        candidate, now_ms, price_hint=1.0
    )

    records = runtime.journal.read_all()
    kinds = [record["kind"] for record in records]
    assert dispatched is True
    assert "order.submitted" in kinds
    assert "runtime.entry_dispatched" in kinds
    assert "runtime.entry_selected_submit_deadline_exceeded" not in kinds


def test_scan_no_entry_diagnostics_includes_entry_admission_prefilter_stage(
    config, tmp_journal
):
    from lightfee.engine.runtime import LiveRuntime

    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal
    runtime._last_entry_admission_filter_blockers = Counter({
        "insufficient_margin_admission_blocked": 2,
    })
    runtime._last_entry_admission_filter_samples = [{
        "candidate_pair_id": "wldusdt:bybit->hyperliquid",
        "symbol": "WLDUSDT",
        "venue": "hyperliquid",
        "reason": "insufficient_margin_admission_blocked",
        "block_scope": "venue",
    }]

    runtime._emit_scan_no_entry_diagnostics(
        reason="no_tradeable_candidates",
        snapshot=SimpleNamespace(candidates=[]),
        tradeable=[],
        selected_candidate_count=0,
        dispatched_candidate_count=0,
        remaining_slots=1,
        tradeable_selection_blocker_counts=Counter(),
        candidate_blockers={},
        now_ms=1_000_000,
        admission_blocker_counts=Counter(),
    )

    records = [
        json.loads(line)
        for line in tmp_journal.path.read_text().splitlines()
        if line.strip()
    ]
    payload = next(
        record["payload"]
        for record in records
        if record["kind"] == "scan.no_entry_diagnostics"
    )
    assert payload["reason"] == "tradeable_candidates_blocked_by_entry_admission"
    assert payload["entry_admission_venue_degraded_counts"] == {
        "insufficient_margin_admission_blocked": 2
    }
    assert payload["candidate_stage_blocked_counts"]["entry_admission_venue_degraded"] == 2
    assert payload["entry_admission_venue_degraded_samples"][0]["venue"] == "hyperliquid"


def test_scan_no_entry_diagnostics_includes_unsupported_symbol_stage(
    config, tmp_journal
):
    from lightfee.engine.runtime import LiveRuntime

    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal
    runtime._last_candidate_catalog_filter_blockers = Counter({
        "unsupported_symbol": 2,
    })
    runtime._last_candidate_catalog_filter_samples = [{
        "candidate_pair_id": "delistedusdt:bitget->bybit",
        "pair_id": "delistedusdt:bitget->bybit",
        "symbol": "DELISTEDUSDT",
        "long_venue": "bitget",
        "short_venue": "bybit",
        "reason": "unsupported_symbol",
    }]

    runtime._emit_scan_no_entry_diagnostics(
        reason="no_tradeable_candidates",
        snapshot=SimpleNamespace(candidates=[]),
        tradeable=[],
        selected_candidate_count=0,
        dispatched_candidate_count=0,
        remaining_slots=1,
        tradeable_selection_blocker_counts=Counter(),
        candidate_blockers={},
        now_ms=1_000_000,
        admission_blocker_counts=Counter(),
    )

    records = [
        json.loads(line)
        for line in tmp_journal.path.read_text().splitlines()
        if line.strip()
    ]
    payload = next(
        record["payload"]
        for record in records
        if record["kind"] == "scan.no_entry_diagnostics"
    )
    assert payload["reason"] == "tradeable_candidates_blocked_by_unsupported_symbol"
    assert payload["unsupported_symbol_blocked_counts"] == {"unsupported_symbol": 2}
    assert payload["candidate_stage_blocked_counts"]["unsupported_symbol"] == 2
    assert payload["unsupported_symbol_blocked_samples"][0]["symbol"] == "DELISTEDUSDT"


def test_v1_tradeable_no_entry_reason_classifies_admission_blocks():
    from lightfee.engine.runtime import LiveRuntime

    reason = LiveRuntime._v1_tradeable_no_entry_reason(
        Counter(),
        admission_blocker_counts=Counter({
            "insufficient_margin_admission_blocked": 2,
        }),
    )

    assert reason == "tradeable_candidates_blocked_by_entry_admission"


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

        from lightfee.engine.entry import EntryContext, EntryType
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

        from lightfee.engine.entry import EntryContext, EntryType
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

    def test_pending_entry_dedup_blocks_same_symbol_venue_overlap_like_v1(
        self, config, tmp_journal
    ):
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        pending = PendingEntry(
            pending_id="entry-xlm-live-deferred",
            symbol="XLMUSDT",
            long_venue=Venue.BYBIT,
            short_venue=Venue.HYPERLIQUID,
            target_quantity=116.0,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=1780591991005,
            maker_client_order_id="maker-xlm-live-deferred",
            hedge_client_order_id="hedge-xlm-live-deferred",
            uncertain_outcome=True,
        )
        runtime.state.pending_entries[pending.pending_id] = pending

        candidate = SimpleNamespace(
            symbol="XLMUSDT",
            long_venue="bybit",
            short_venue="okx",
        )

        allowed, reason = runtime._gate_pending_entry_dedup(candidate)

        assert allowed is False
        assert reason == "pending_entry_protection"

    def test_recovery_ledger_orphan_maker_order_blocks_every_new_entry(
        self, config, tmp_journal
    ):
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        runtime.recovery_ledger = RecoveryLedger.from_local_and_exchange_truth(
            local={"open_positions": [], "pending_entries": []},
            exchange_truth={
                "truth_available": True,
                "positions": [],
                "open_orders": [
                    {
                        "venue": "bybit",
                        "symbol": "TRXUSDT",
                        "side": "buy",
                        "quantity": 72.0,
                        "reduce_only": False,
                    }
                ],
            },
        )

        allowed, reason = runtime._gate_recovery_ledger(
            SimpleNamespace(symbol="BTCUSDT", long_venue="binance", short_venue="okx")
        )

        assert allowed is False
        assert reason == "recovery_ledger_blocked"

    def test_recovery_ledger_blocks_same_symbol_or_venue_overlap(
        self, config, tmp_journal
    ):
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        runtime.recovery_ledger = RecoveryLedger.from_local_and_exchange_truth(
            local={
                "pending_entries": [
                    {
                        "pending_id": "entry-sei",
                        "symbol": "SEIUSDT",
                        "long_venue": "bybit",
                        "short_venue": "hyperliquid",
                    }
                ]
            },
            exchange_truth={"truth_available": True, "positions": [], "open_orders": []},
        )

        assert runtime._gate_recovery_ledger(
            SimpleNamespace(symbol="SEIUSDT", long_venue="bybit", short_venue="okx")
        ) == (False, "recovery_ledger_blocked")
        assert runtime._gate_recovery_ledger(
            SimpleNamespace(symbol="BTCUSDT", long_venue="bybit", short_venue="okx")
        ) == (True, "")
        assert runtime._gate_recovery_ledger(
            SimpleNamespace(symbol="SEIUSDT", long_venue="binance", short_venue="okx")
        ) == (True, "")
        assert runtime._gate_recovery_ledger(
            SimpleNamespace(symbol="BTCUSDT", long_venue="binance", short_venue="okx")
        ) == (True, "")

    def test_clean_recovery_ledger_allows_candidate_path(self, config, tmp_journal):
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        runtime.recovery_ledger = RecoveryLedger.from_local_and_exchange_truth(
            local={"open_positions": [], "pending_entries": []},
            exchange_truth={"truth_available": True, "positions": [], "open_orders": []},
        )

        allowed, reason = runtime._gate_recovery_ledger(
            SimpleNamespace(symbol="BTCUSDT", long_venue="binance", short_venue="okx")
        )

        assert allowed is True
        assert reason == ""

    def test_recovery_core_blocks_unrelated_entry_when_truth_required_for_work(
        self, config, tmp_journal
    ):
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        runtime.state.pending_entries["entry-sei"] = SimpleNamespace(
            pending_id="entry-sei",
            symbol="SEIUSDT",
        )
        runtime._refresh_recovery_ledger_from_exchange_truth(
            {"truth_available": False, "positions": [], "open_orders": []},
            now_ms=1778787000000,
        )

        allowed, reason = runtime._gate_recovery_ledger(
            SimpleNamespace(symbol="BTCUSDT", long_venue="binance", short_venue="okx")
        )

        assert allowed is False
        assert reason == "recovery_ledger_blocked"


@pytest.mark.asyncio
async def test_dispatch_entry_rechecks_first_funding_horizon_after_selection_delay(
    config, tmp_journal
):
    from lightfee.engine.runtime import LiveRuntime
    from lightfee.sidecar.snapshot import CandidateInput

    class FailingEntryExecutor:
        def __init__(self):
            self.called = False

        async def execute(self, _ctx):
            self.called = True
            raise AssertionError("dispatch lifecycle gate must block before execute")

    config.strategy.min_scan_minutes_before_funding = 1
    runtime = LiveRuntime(config, venue_adapters={})
    runtime.journal = tmp_journal
    executor = FailingEntryExecutor()
    runtime.entry_executor = executor
    now_ms = 1_000_000
    candidate = CandidateInput(
        long_venue="binance",
        short_venue="bybit",
        symbol="BTCUSDT",
        funding_diff_bps=10.0,
        funding_edge_bps=8.0,
        expected_edge_bps=5.0,
        worst_case_edge_bps=2.0,
        ranking_edge_bps=8.0,
        entry_notional_quote=30.0,
        first_funding_timestamp_ms=now_ms + 59_000,
        funding_timestamp_ms=now_ms + 59_000,
    )

    dispatched = await runtime._dispatch_entry(candidate, now_ms, price_hint=1.0)

    assert dispatched is False
    assert executor.called is False
    assert any(
        event["kind"] == "runtime.entry_blocked_lifecycle"
        and event["payload"]["reason"] == "entry_blocked_first_funding_too_close"
        for event in tmp_journal.read_all()
    )


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

        return _attach_live_oi_evidence(CandidateInput(
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
            first_funding_timestamp_ms=605_000,
            funding_timestamp_ms=605_000,
            long_funding_timestamp_ms=605_000,
            short_funding_timestamp_ms=605_000,
            economics_complete=True,
            economics_observed_at_ms=1_000,
            ), now_ms=5_000)

    @pytest.mark.asyncio
    async def test_ranked_flow_only_prewarms_l2_before_finalization(
        self,
        config,
        tmp_journal,
        monkeypatch,
    ):
        """V1 prewarm may start bounded L2, never quote/OI revalidation early."""
        config.runtime.mode = "live"
        config.strategy.local_l2_enabled = True
        config.strategy.entry_window_secs = 300
        config.strategy.entry_local_l2_prewarm_window_secs = 900
        config.strategy.min_scan_minutes_before_funding = 3
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        runtime.state.lifecycle = EngineLifecycle.RUNNING
        runtime.state.risk_mode = GlobalRiskMode.RUNNING
        runtime.state.last_scan = {}
        candidate = self._candidate()

        l2_activation_calls: list[tuple[list[str], int, int]] = []
        l2_sync_calls: list[int] = []
        bbo_activation_calls: list[tuple[list[str], set[str], bool]] = []
        quote_calls: list[str] = []
        oi_calls: list[str] = []

        async def ensure_l2(candidates, now_ms, *, tracked_opportunities=None):
            l2_activation_calls.append(
                (
                    [row.symbol for row in candidates],
                    now_ms,
                    len(list(tracked_opportunities or [])),
                )
            )

        async def sync_l2(now_ms, *, scan_promoted=False):
            assert scan_promoted is True
            l2_sync_calls.append(now_ms)

        async def ensure_bbo(candidates, now_ms):
            bbo_activation_calls.append(
                (
                    [row.symbol for row in candidates],
                    set(runtime._tracked_primary_pair_ids),
                    runtime.entry_l2_sessions.sessions.get(
                        runtime._candidate_pair_id(candidate)
                    )
                    is not None,
                )
            )

        async def quote_truth(candidates, **_kwargs):
            quote_calls.extend(row.symbol for row in candidates)
            return {}, {"resolved_count": len(candidates)}

        async def refresh_oi(candidates, **_kwargs):
            oi_calls.extend(row.symbol for row in candidates)
            return {"resolved_count": len(candidates)}

        async def passthrough(candidates, **_kwargs):
            return list(candidates)

        clock_calls: list[int] = []

        def advancing_clock() -> int:
            # L2 activation may await.  Final readiness must use a fresh wall
            # clock rather than the 10s timestamp captured before activation.
            value = 10_000 if not clock_calls else 17_000
            clock_calls.append(value)
            return value

        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms",
            advancing_clock,
        )
        monkeypatch.setattr(runtime, "_ensure_l2_active_for_candidates", ensure_l2)
        monkeypatch.setattr(runtime, "_sync_local_l2_data", sync_l2)
        monkeypatch.setattr(
            runtime,
            "_ensure_entry_bbo_active_for_candidates",
            ensure_bbo,
        )
        monkeypatch.setattr(
            runtime,
            "_refresh_entry_l2_session_readiness",
            lambda _now_ms: None,
        )
        monkeypatch.setattr(
            runtime,
            "_filter_candidates_supported_by_venue_catalog",
            passthrough,
        )
        monkeypatch.setattr(
            runtime,
            "_filter_candidates_by_entry_admission",
            lambda candidates, **_kwargs: list(candidates),
        )
        monkeypatch.setattr(
            runtime,
            "_filter_candidates_by_entry_balance_admission",
            passthrough,
        )
        monkeypatch.setattr(
            runtime,
            "_entry_quote_revalidate_for_candidates",
            quote_truth,
        )
        monkeypatch.setattr(
            runtime,
            "_refresh_entry_candidate_open_interest_evidence",
            refresh_oi,
        )

        await runtime._run_ranked_candidate_entry_flow(
            [candidate],
            snapshot=SimpleNamespace(quotes={}),
            price_hints={},
        )

        assert l2_activation_calls == [(["BTCUSDT"], 10_000, 1)]
        assert l2_sync_calls == [17_000]
        assert bbo_activation_calls == [
            (["BTCUSDT"], {runtime._candidate_pair_id(candidate)}, True)
        ]
        assert clock_calls[:3] == [10_000, 17_000, 17_000]
        assert quote_calls == []
        assert oi_calls == []
        assert runtime.state.last_scan["no_entry_reason"] == (
            "entry_waiting_for_finalization_window_too_early"
        )
        no_entry = [
            row["payload"]
            for row in runtime.journal.read_all()
            if row["kind"] == "scan.no_entry_ranked_candidates"
        ]
        assert no_entry[-1]["selection_blockers"] == {
            "entry_waiting_for_finalization_window_too_early": 1
        }

    @pytest.mark.asyncio
    async def test_ranked_final_flow_keeps_prewarmed_bbo_scope_and_exact_blocker(
        self,
        config,
        tmp_journal,
        monkeypatch,
    ):
        """A final candidate must not shrink the V1 warm scope or erase cause."""
        config.runtime.mode = "live"
        config.strategy.local_l2_enabled = False
        config.strategy.entry_window_secs = 300
        config.strategy.min_scan_minutes_before_funding = 3
        config.strategy.entry_local_l2_prewarm_window_secs = 900
        config.strategy.entry_local_l2_primary_count = 2
        config.strategy.shadow_entry_opportunity_count = 0
        config.strategy.max_concurrent_positions = 2
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        runtime.state.lifecycle = EngineLifecycle.RUNNING
        runtime.state.risk_mode = GlobalRiskMode.RUNNING
        runtime.state.last_scan = {}
        first = self._candidate("BTCUSDT")
        second = self._candidate("ETHUSDT")
        first.first_funding_timestamp_ms = 305_000
        second.first_funding_timestamp_ms = 305_000

        bbo_scopes: list[list[str]] = []
        final_activation_scopes: list[list[str] | None] = []

        async def ensure_bbo(candidates, _now_ms):
            bbo_scopes.append([candidate.symbol for candidate in candidates])

        async def quote_truth(candidates, **kwargs):
            activation = kwargs.get("activation_candidates")
            final_activation_scopes.append(
                None if activation is None else [candidate.symbol for candidate in activation]
            )
            return {}, {"resolved_count": len(candidates)}

        async def refresh_oi(candidates, **_kwargs):
            return {"resolved_count": len(candidates)}

        async def passthrough(candidates, **_kwargs):
            return list(candidates)

        def blocked_by_final_liquidity(candidates, **_kwargs):
            runtime._last_snapshot_freshness_filter_blockers = Counter(
                {"perp_liquidity_stale_blocking": len(candidates)}
            )
            return []

        monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 10_000)
        monkeypatch.setattr(runtime, "_ensure_entry_bbo_active_for_candidates", ensure_bbo)
        monkeypatch.setattr(
            runtime,
            "_filter_candidates_supported_by_venue_catalog",
            passthrough,
        )
        monkeypatch.setattr(
            runtime,
            "_filter_candidates_by_entry_admission",
            lambda candidates, **_kwargs: list(candidates),
        )
        monkeypatch.setattr(
            runtime,
            "_filter_candidates_by_entry_balance_admission",
            passthrough,
        )
        monkeypatch.setattr(runtime, "_entry_quote_revalidate_for_candidates", quote_truth)
        monkeypatch.setattr(
            runtime,
            "_refresh_entry_candidate_open_interest_evidence",
            refresh_oi,
        )
        monkeypatch.setattr(
            runtime,
            "_filter_candidates_by_snapshot_freshness",
            blocked_by_final_liquidity,
        )

        await runtime._run_ranked_candidate_entry_flow(
            [first, second],
            snapshot=SimpleNamespace(quotes={}),
            price_hints={},
        )

        assert bbo_scopes == [["BTCUSDT", "ETHUSDT"]]
        assert runtime.entry_l2_sessions.sessions == {}
        # Final revalidation consumes the already-warmed V1 frontier rather
        # than re-reconciling the data plane down to one candidate per loop.
        assert final_activation_scopes == [[], []]
        assert runtime.state.last_scan["no_entry_reason"] == (
            "perp_liquidity_stale_blocking"
        )
        assert runtime.state.last_scan["ranked_candidate_blockers"] == {
            runtime._candidate_pair_id(first): "perp_liquidity_stale_blocking",
            runtime._candidate_pair_id(second): "perp_liquidity_stale_blocking",
        }
        no_entry = [
            row["payload"]
            for row in runtime.journal.read_all()
            if row["kind"] == "scan.no_entry_ranked_candidates"
        ][-1]
        assert no_entry["reason"] == "perp_liquidity_stale_blocking"
        assert no_entry["selection_blockers"] == {
            "perp_liquidity_stale_blocking": 2
        }

    @pytest.mark.asyncio
    async def test_ranked_bbo_prewarm_timeout_falls_back_to_bounded_final_activation(
        self,
        config,
        tmp_journal,
        monkeypatch,
    ):
        """Slow symbol-support activation cannot consume the ranked loop."""
        config.runtime.mode = "live"
        config.strategy.local_l2_enabled = False
        config.strategy.entry_window_secs = 300
        config.strategy.min_scan_minutes_before_funding = 0
        config.strategy.entry_local_l2_prewarm_window_secs = 900
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        runtime.state.lifecycle = EngineLifecycle.RUNNING
        runtime.state.risk_mode = GlobalRiskMode.RUNNING
        runtime.state.last_scan = {}
        candidate = self._candidate("BTCUSDT")
        candidate.first_funding_timestamp_ms = 305_000

        activation_calls: list[list[str]] = []
        final_activation_scopes: list[list[str] | None] = []

        async def slow_activation(candidates, _now_ms):
            activation_calls.append([row.symbol for row in candidates])
            await asyncio.Event().wait()

        async def passthrough(candidates, **_kwargs):
            return list(candidates)

        async def quote_truth(candidates, **kwargs):
            activation = kwargs.get("activation_candidates")
            final_activation_scopes.append(
                None if activation is None else [row.symbol for row in activation]
            )
            return {}, {"resolved_count": len(candidates)}

        async def refresh_oi(candidates, **_kwargs):
            return {"resolved_count": len(candidates)}

        def block_final_evidence(_candidates, **_kwargs):
            runtime._last_snapshot_freshness_filter_blockers = Counter(
                {"perp_liquidity_stale_blocking": 1}
            )
            return []

        monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 10_000)
        monkeypatch.setattr(runtime, "_ensure_entry_bbo_active_for_candidates", slow_activation)
        monkeypatch.setattr(runtime, "_filter_candidates_supported_by_venue_catalog", passthrough)
        monkeypatch.setattr(
            runtime,
            "_filter_candidates_by_entry_admission",
            lambda candidates, **_kwargs: list(candidates),
        )
        monkeypatch.setattr(runtime, "_filter_candidates_by_entry_balance_admission", passthrough)
        monkeypatch.setattr(runtime, "_entry_quote_revalidate_for_candidates", quote_truth)
        monkeypatch.setattr(runtime, "_refresh_entry_candidate_open_interest_evidence", refresh_oi)
        monkeypatch.setattr(runtime, "_filter_candidates_by_snapshot_freshness", block_final_evidence)

        await runtime._run_ranked_candidate_entry_flow(
            [candidate],
            snapshot=SimpleNamespace(quotes={}),
            price_hints={},
        )

        assert activation_calls == [["BTCUSDT"]]
        assert final_activation_scopes == [["BTCUSDT"]]
        assert runtime.state.last_scan["entry_bbo_prewarm_active"] is False
        failure = [
            row["payload"]
            for row in runtime.journal.read_all()
            if row["kind"] == "runtime.entry_bbo_prewarm_failed"
        ][-1]
        assert failure["reason"] == "entry_bbo_prewarm_activation_timeout"
        assert failure["activation_budget_ms"] == 100

    @pytest.mark.asyncio
    async def test_ranked_catalog_rejection_preserves_pair_specific_reason(
        self,
        config,
        tmp_journal,
        monkeypatch,
    ):
        """A catalog failure must not be flattened to generic admission."""
        config.runtime.mode = "live"
        config.strategy.local_l2_enabled = False
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        runtime.state.lifecycle = EngineLifecycle.RUNNING
        runtime.state.risk_mode = GlobalRiskMode.RUNNING
        runtime.state.last_scan = {}
        candidate = self._candidate("DELISTEDUSDT")

        async def catalog_reject(rows):
            assert rows == [candidate]
            runtime._last_candidate_catalog_filter_blockers = Counter(
                {"unsupported_symbol": 1}
            )
            runtime._last_candidate_catalog_filter_samples = [
                {
                    "candidate_pair_id": runtime._candidate_pair_id(candidate),
                    "reason": "unsupported_symbol",
                }
            ]
            return []

        monkeypatch.setattr(
            runtime,
            "_filter_candidates_supported_by_venue_catalog",
            catalog_reject,
        )

        await runtime._run_ranked_candidate_entry_flow(
            [candidate],
            snapshot=SimpleNamespace(quotes={}),
            price_hints={},
        )

        pair_id = runtime._candidate_pair_id(candidate)
        assert runtime.state.last_scan["no_entry_reason"] == "unsupported_symbol"
        assert runtime.state.last_scan["ranked_candidate_blockers"] == {
            pair_id: "unsupported_symbol"
        }
        no_entry = [
            row["payload"]
            for row in runtime.journal.read_all()
            if row["kind"] == "scan.no_entry_ranked_candidates"
        ][-1]
        assert no_entry["admission_blockers"] == {"unsupported_symbol": 1}
        assert no_entry["candidate_blockers"] == [
            {"pair_id": pair_id, "reason": "unsupported_symbol"}
        ]

    def test_local_l2_missing_book_preserves_final_rejection_reason(
        self,
        config,
        tmp_journal,
        monkeypatch,
    ):
        config.runtime.mode = "live"
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        dispatch = runtime.entry_dispatch_runtime
        candidate = self._candidate()
        monkeypatch.setattr(
            dispatch,
            "_local_l2_effective_enabled",
            lambda: True,
        )

        blocked = dispatch._entry_local_l2_gate_blocked(
            candidate=candidate,
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            now_ms=17_000,
        )

        assert blocked is True
        assert runtime._last_entry_dispatch_block_reason == "missing_book"
        decisions = [
            row["payload"]
            for row in tmp_journal.read_all()
            if row["kind"] == "runtime.execution_l2_stale"
        ]
        assert {payload["l2_reason"] for payload in decisions} == {"missing_book"}
        final_block = next(
            row["payload"]
            for row in tmp_journal.read_all()
            if row["kind"] == "runtime.entry_blocked_local_l2_not_ready"
        )
        assert final_block["reason"] == "missing_book"

    @pytest.mark.asyncio
    async def test_live_dispatch_rejects_final_cross_venue_price_normalization_mismatch(
        self, config, tmp_journal, monkeypatch
    ):
        from lightfee.engine.entry_readiness import QuoteLease
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.runtime.mode = "live"
        config.strategy.funding_new_entries_enabled = True
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        candidate = self._candidate()
        lease = QuoteLease(
            pair_id="BTCUSDT:binance:okx",
            symbol="BTCUSDT",
            long_venue="binance",
            short_venue="okx",
            long_bid=99.0,
            long_ask=100.0,
            short_bid=99_999.0,
            short_ask=100_000.0,
            long_observed_at_ms=5_000,
            short_observed_at_ms=5_000,
            created_at_ms=5_000,
            expires_at_ms=6_000,
        )
        dispatch = runtime.entry_dispatch_runtime
        monkeypatch.setattr(dispatch, "_entry_initial_gate_blocked", lambda *_: False)
        monkeypatch.setattr(dispatch, "_entry_local_l2_gate_blocked", lambda **_: False)
        monkeypatch.setattr(
            dispatch,
            "_entry_price_resolution",
            lambda *_: (50_000.0, 100.0, 100_000.0, lease),
        )

        dispatched = await runtime._dispatch_entry(candidate, 5_000, price_hint=50_000.0)

        assert dispatched is False
        blockers = [
            row["payload"]
            for row in tmp_journal.read_all()
            if row["kind"] == "entry.dispatch_viability_blocked"
        ]
        assert blockers[-1]["reason"] == "cross_venue_price_normalization_mismatch"
        assert blockers[-1]["source"] == "final_entry_price_normalization"

    @pytest.mark.asyncio
    async def test_live_dispatch_rejects_candidate_without_economics_timestamp(
        self,
        config,
        tmp_journal,
    ):
        config.runtime.mode = "live"
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        candidate = self._candidate()
        candidate.economics_observed_at_ms = 0

        dispatched = await runtime._dispatch_entry(candidate, 5_000, price_hint=50_000.0)

        assert dispatched is False
        policy_events = [
            record["payload"]
            for record in tmp_journal.read_all()
            if record["kind"] == "runtime.entry_blocked_entry_policy"
        ]
        assert policy_events[-1]["reason"] == "incomplete_economics"

    @pytest.mark.asyncio
    async def test_live_dispatch_rejects_candidate_from_prior_economics_epoch(
        self,
        config,
        tmp_journal,
    ):
        """A shadow snapshot cannot retain permission after enhanced-live rollout."""
        config.runtime.mode = "live"
        config.strategy.funding_new_entries_enabled = True
        config.strategy.funding_economics_mode = "enhanced_live"
        config.strategy.funding_forecast_mode = "live"
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        candidate = self._candidate()
        candidate.calculation_version = "enhanced_shadow"
        candidate.model_epoch = "enhanced_shadow"

        dispatched = await runtime._dispatch_entry(candidate, 5_000, price_hint=50_000.0)

        assert dispatched is False
        policy_events = [
            record["payload"]
            for record in tmp_journal.read_all()
            if record["kind"] == "runtime.entry_blocked_entry_policy"
        ]
        assert policy_events[-1]["reason"] == "funding_calculation_version_mismatch"

    @pytest.mark.asyncio
    async def test_live_dispatch_does_not_require_legacy_taker_fee_evidence(
        self,
        config,
        tmp_journal,
    ):
        config.runtime.mode = "live"
        config.strategy.funding_new_entries_enabled = True
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        candidate = self._candidate()

        dispatched = await runtime._dispatch_entry(candidate, 5_000, price_hint=50_000.0)

        assert dispatched is False
        policy_events = [
            record["payload"]
            for record in tmp_journal.read_all()
            if record["kind"] == "runtime.entry_blocked_entry_policy"
        ]
        assert all(
            event.get("reason") != "missing_taker_fee_evidence"
            for event in policy_events
        )

    @pytest.mark.asyncio
    async def test_live_dispatch_rebuilds_enhanced_forecast_readiness_from_evidence(
        self,
        config,
        tmp_journal,
    ):
        """A hand-built v3 candidate cannot claim enhanced-live readiness."""
        config.runtime.mode = "live"
        config.strategy.funding_new_entries_enabled = True
        config.strategy.funding_economics_mode = "enhanced_live"
        config.strategy.funding_forecast_mode = "live"
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        candidate = self._candidate()
        candidate.calculation_version = "enhanced_live"
        candidate.model_epoch = "enhanced_live"
        candidate.forecast_ready = True
        candidate.forecast_confidence = 1.0
        candidate.forecast_sample_count = 0
        candidate.forecast_shadow_age_ms = 0

        dispatched = await runtime._dispatch_entry(candidate, 5_000, price_hint=50_000.0)

        assert dispatched is False
        policy_events = [
            record["payload"]
            for record in tmp_journal.read_all()
            if record["kind"] == "runtime.entry_blocked_entry_policy"
        ]
        assert policy_events[-1]["reason"] == "funding_forecast_not_ready"

    @pytest.mark.asyncio
    async def test_live_dispatch_rejects_unstable_enhanced_forecast_distribution(
        self,
        config,
        tmp_journal,
    ):
        config.runtime.mode = "live"
        config.strategy.funding_new_entries_enabled = True
        config.strategy.funding_economics_mode = "enhanced_live"
        config.strategy.funding_forecast_mode = "live"
        config.strategy.funding_forecast_min_samples = 1
        config.strategy.funding_forecast_shadow_min_days = 0
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        candidate = self._candidate()
        candidate.calculation_version = "enhanced_live"
        candidate.model_epoch = "enhanced_live"
        candidate.forecast_ready = True
        candidate.forecast_confidence = 1.0
        candidate.forecast_sample_count = 1
        candidate.forecast_shadow_age_ms = 0
        candidate.forecast_distribution_stable = False
        candidate.forecast_stability_reason = "p90_error_distribution_drift"

        dispatched = await runtime._dispatch_entry(candidate, 5_000, price_hint=50_000.0)

        assert dispatched is False
        policy_events = [
            record["payload"]
            for record in tmp_journal.read_all()
            if record["kind"] == "runtime.entry_blocked_entry_policy"
        ]
        assert policy_events[-1]["reason"] == "funding_forecast_distribution_unstable"

    @staticmethod
    def _binance_bybit_candidate(symbol: str = "BTCUSDT"):
        from lightfee.sidecar.snapshot import CandidateInput

        return _attach_live_oi_evidence(CandidateInput(
            long_venue="binance",
            short_venue="bybit",
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
            first_funding_timestamp_ms=605_000,
            funding_timestamp_ms=605_000,
            economics_complete=True,
        ), now_ms=5_000)

    @staticmethod
    def _capturing_executor():
        class CapturingExecutor:
            ctx = None

            async def execute(self, ctx):
                self.ctx = ctx
                return EntryExecutionResult(
                    route=ExecutionRoute.PASSIVE_INCREMENTAL,
                    state=EntryState.COMPLETED,
                )

        return CapturingExecutor()

    @pytest.mark.asyncio
    async def test_dispatch_entry_pending_close_reconciliation_blocks_only_matching_pair(
        self, config, tmp_journal,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
        bybit = FakeVenueAdapter(Venue.BYBIT, _min_notional_quote=10.0)
        runtime = LiveRuntime(
            config,
            venue_adapters={Venue.BINANCE: binance, Venue.BYBIT: bybit},
        )
        runtime.journal = tmp_journal
        runtime.state.lifecycle = EngineLifecycle.RUNNING
        runtime.state.risk_mode = GlobalRiskMode.RUNNING
        executor = self._capturing_executor()
        runtime.entry_executor = executor
        runtime.state.set_pending_close_reconciliations([
            {
                "position_id": "pos-closing",
                "kind": "final",
                "symbol": "BTCUSDT",
                "long_venue": "binance",
                "short_venue": "bybit",
            }
        ])

        blocked_candidate = self._binance_bybit_candidate("BTCUSDT")
        allowed_candidate = self._binance_bybit_candidate("ETHUSDT")
        for venue, bid, ask in (
            ("binance", 50000.0, 50010.0),
            ("bybit", 49990.0, 50000.0),
        ):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="ETHUSDT",
                    bid=bid,
                    ask=ask,
                    observed_at_ms=5001,
                    received_at_ms=5001,
                    source=f"{venue}_bbo_ws",
                )
            )

        blocked = await runtime._dispatch_entry(
            blocked_candidate,
            5000,
            price_hint=50000.0,
        )
        dispatched = await runtime._dispatch_entry(
            allowed_candidate,
            5001,
            price_hint=50000.0,
        )

        assert blocked is False
        assert dispatched is True
        assert executor.ctx is not None
        assert executor.ctx.symbol == "ETHUSDT"
        blocked_events = [
            record
            for record in runtime.journal.read_all()
            if record["kind"] == "runtime.entry_blocked_gate"
        ]
        assert len(blocked_events) == 1
        assert blocked_events[0]["payload"] == {
            "gate": "pending_close_reconciliation",
            "reason": "pending_close_reconciliation_conflict",
            "symbol": "BTCUSDT",
            "ts_ms": 5000,
        }

    @pytest.mark.asyncio
    async def test_dispatch_entry_pending_passive_close_blocks_only_matching_pair(
        self, config, tmp_journal,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
        bybit = FakeVenueAdapter(Venue.BYBIT, _min_notional_quote=10.0)
        runtime = LiveRuntime(
            config,
            venue_adapters={Venue.BINANCE: binance, Venue.BYBIT: bybit},
        )
        runtime.journal = tmp_journal
        runtime.state.lifecycle = EngineLifecycle.RUNNING
        runtime.state.risk_mode = GlobalRiskMode.RUNNING
        executor = self._capturing_executor()
        runtime.entry_executor = executor
        position = OpenPosition(
            position_id="pos-closing",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=0.01,
            short_quantity=0.01,
            long_entry_price=50000.0,
            short_entry_price=50000.0,
            opened_at_ms=1000,
            matched_quantity=0.01,
        )
        runtime.state.open_positions[position.position_id] = position
        runtime.state.pending_passive_closes[position.position_id] = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER
            ),
            next_retry_at_ms=5000,
        )

        blocked_candidate = self._binance_bybit_candidate("BTCUSDT")
        allowed_candidate = self._binance_bybit_candidate("ETHUSDT")
        for venue, bid, ask in (
            ("binance", 50000.0, 50010.0),
            ("bybit", 49990.0, 50000.0),
        ):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="ETHUSDT",
                    bid=bid,
                    ask=ask,
                    observed_at_ms=5001,
                    received_at_ms=5001,
                    source=f"{venue}_bbo_ws",
                )
            )

        blocked = await runtime._dispatch_entry(
            blocked_candidate,
            5000,
            price_hint=50000.0,
        )
        dispatched = await runtime._dispatch_entry(
            allowed_candidate,
            5001,
            price_hint=50000.0,
        )

        assert blocked is False
        assert dispatched is True
        assert executor.ctx is not None
        assert executor.ctx.symbol == "ETHUSDT"
        blocked_events = [
            record
            for record in runtime.journal.read_all()
            if record["kind"] == "runtime.entry_blocked_gate"
        ]
        assert len(blocked_events) == 1
        assert blocked_events[0]["payload"] == {
            "gate": "passive_close_in_flight",
            "reason": "passive_close_in_flight",
            "symbol": "BTCUSDT",
            "ts_ms": 5000,
        }

    @pytest.mark.asyncio
    async def test_entry_account_truth_probe_error_never_marks_generation_ready(
        self, config, tmp_journal, monkeypatch,
    ):
        class BrokenAccountTruthAdapter(FakeVenueAdapter):
            async def fetch_all_positions(self):
                return []

            async def fetch_open_orders(self, symbol: str | None = None):
                raise RuntimeError("private order probe failed")

        config.runtime.mode = "live"
        runtime = LiveRuntime(
            config,
            venue_adapters={
                Venue.ASTER: BrokenAccountTruthAdapter(Venue.ASTER),
            },
        )
        runtime.journal = tmp_journal
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms",
            lambda: 7_001,
        )

        assert await runtime._ensure_entry_account_truth_for_preparation() is False
        assert runtime._entry_account_truth_ready_at_ms == 0
        assert runtime._entry_account_truth_generation_is_ready(7_001) is False

    @pytest.mark.asyncio
    async def test_entry_account_truth_generation_uses_private_position_max_age(
        self, config, tmp_journal, monkeypatch,
    ):
        class CompleteAccountTruthAdapter(FakeVenueAdapter):
            async def fetch_all_positions(self):
                return []

            async def fetch_open_orders(self, symbol: str | None = None):
                return []

        config.runtime.mode = "live"
        config.runtime.private_position_max_age_ms = 15_000
        runtime = LiveRuntime(
            config,
            venue_adapters={
                Venue.ASTER: CompleteAccountTruthAdapter(Venue.ASTER),
            },
        )
        runtime.journal = tmp_journal
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms",
            lambda: 7_001,
        )

        assert await runtime._ensure_entry_account_truth_for_preparation() is True
        assert runtime._entry_account_truth_generation_is_ready(22_001) is True
        assert runtime._entry_account_truth_generation_is_ready(22_002) is False

    @pytest.mark.asyncio
    async def test_entry_account_truth_global_refresh_is_not_cut_short_by_aggregate_budget(
        self, config, tmp_journal,
    ):
        class SlowAccountTruthAdapter(FakeVenueAdapter):
            async def fetch_all_positions(self):
                await asyncio.sleep(0.05)
                return []

            async def fetch_open_orders(self, symbol: str | None = None):
                return []

        config.runtime.mode = "live"
        # Recovery retains its own budget for recovery paths, but ordinary
        # background all-venue truth must complete rather than being silently
        # truncated by that aggregate number.
        config.runtime.live_recovery_rest_probe_timeout_ms = 1
        runtime = LiveRuntime(
            config,
            venue_adapters={
                Venue.ASTER: SlowAccountTruthAdapter(Venue.ASTER),
            },
        )
        runtime.journal = tmp_journal

        assert await runtime._ensure_entry_account_truth_for_preparation() is True
        assert runtime._entry_account_truth_ready_at_ms > 0
        assert runtime._entry_account_truth_generation_is_ready() is True
        assert runtime._entry_account_truth_generation["errors"] == []

    @pytest.mark.asyncio
    async def test_entry_account_truth_refresh_is_singleflight(
        self, config, tmp_journal,
    ):
        release = asyncio.Event()

        class BlockingAccountTruthAdapter(FakeVenueAdapter):
            position_calls = 0
            open_order_calls = 0

            async def fetch_all_positions(self):
                self.position_calls += 1
                await release.wait()
                return []

            async def fetch_open_orders(self, symbol: str | None = None):
                self.open_order_calls += 1
                return []

        config.runtime.mode = "live"
        adapter = BlockingAccountTruthAdapter(Venue.ASTER)
        runtime = LiveRuntime(
            config,
            venue_adapters={Venue.ASTER: adapter},
        )
        runtime.journal = tmp_journal

        first = asyncio.create_task(
            runtime._ensure_entry_account_truth_for_preparation()
        )
        second = asyncio.create_task(
            runtime._ensure_entry_account_truth_for_preparation()
        )
        await asyncio.sleep(0)
        release.set()

        assert await asyncio.gather(first, second) == [True, True]
        assert adapter.position_calls == 1
        assert adapter.open_order_calls == 1

    @pytest.mark.asyncio
    async def test_entry_account_truth_submit_recheck_rejects_missing_target_receipts(
        self, config, tmp_journal, monkeypatch,
    ):
        config.runtime.mode = "live"
        config.runtime.private_position_max_age_ms = 15_000
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms",
            lambda: 7_001,
        )

        assert (
            await runtime._entry_account_truth_ready_before_dispatch(
                self._candidate("BTCUSDT")
            )
            is False
        )
        events = runtime.journal.read_all()
        assert any(event["kind"] == "runtime.entry_account_truth_incomplete" for event in events)
        assert runtime.state.last_scan["blocking_reason"] == (
            "entry_account_truth_incomplete_before_dispatch"
        )

    @pytest.mark.asyncio
    async def test_entry_account_truth_before_dispatch_uses_target_venues_only(
        self, config, tmp_journal, monkeypatch,
    ):
        class CompleteAccountTruthAdapter(FakeVenueAdapter):
            open_order_calls = 0

            async def fetch_open_orders(self, symbol: str | None = None):
                self.open_order_calls += 1
                return []

        class BrokenNonTargetAdapter(FakeVenueAdapter):
            position_calls = 0

            async def fetch_position(self, symbol: str):
                self.position_calls += 1
                raise RuntimeError("non-target venue must not be probed")

        config.runtime.mode = "live"
        adapters = {
            Venue.BINANCE: CompleteAccountTruthAdapter(Venue.BINANCE),
            Venue.OKX: CompleteAccountTruthAdapter(Venue.OKX),
            Venue.ASTER: BrokenNonTargetAdapter(Venue.ASTER),
        }
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms",
            lambda: 7_001,
        )

        assert (
            await runtime._entry_account_truth_ready_before_dispatch(
                self._candidate("BTCUSDT")
            )
            is True
        )
        assert adapters[Venue.BINANCE].fetch_position_call_count == 1
        assert adapters[Venue.OKX].open_order_calls == 1
        assert adapters[Venue.ASTER].position_calls == 0
        assert runtime.journal.read_all() == []

    @pytest.mark.asyncio
    async def test_entry_account_truth_pending_global_sweep_does_not_hold_target_pair(
        self, config, tmp_journal, monkeypatch,
    ):
        class CompleteAccountTruthAdapter(FakeVenueAdapter):
            async def fetch_all_positions(self):
                return []

            async def fetch_open_orders(self, symbol: str | None = None):
                return []

        global_sweep_started = asyncio.Event()
        release_global_sweep = asyncio.Event()

        async def slow_global_sweep(*_args, **_kwargs):
            global_sweep_started.set()
            await release_global_sweep.wait()
            return {}, {
                "truth_supported": True,
                "truth_available": True,
                "venue_count": 3,
                "complete_venue_count": 3,
                "positions": [],
                "open_orders": [],
                "probe_evidence": [],
                "errors": [],
            }

        config.runtime.mode = "live"
        runtime = LiveRuntime(
            config,
            venue_adapters={
                Venue.BINANCE: CompleteAccountTruthAdapter(Venue.BINANCE),
                Venue.OKX: CompleteAccountTruthAdapter(Venue.OKX),
                Venue.ASTER: CompleteAccountTruthAdapter(Venue.ASTER),
            },
        )
        runtime.journal = tmp_journal
        monkeypatch.setattr(
            runtime.recovery_startup_runtime,
            "_refresh_recovery_ledger_from_account_truth_with_evidence",
            slow_global_sweep,
        )

        assert await runtime._entry_account_truth_ready_for_tick() is False
        await global_sweep_started.wait()
        assert (
            await runtime._entry_account_truth_ready_before_dispatch(
                self._candidate("BTCUSDT")
            )
            is True
        )

        release_global_sweep.set()
        assert runtime._entry_account_truth_gate_task is not None
        assert await runtime._entry_account_truth_gate_task is True

    @pytest.mark.asyncio
    async def test_entry_account_truth_target_venue_probes_are_singleflight(
        self, config, tmp_journal,
    ):
        release = asyncio.Event()

        class BlockingAccountTruthAdapter(FakeVenueAdapter):
            position_calls = 0
            open_order_calls = 0

            async def fetch_position(self, symbol: str):
                self.position_calls += 1
                await release.wait()
                return PositionSnapshot(
                    venue=self._venue,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=0.0,
                    entry_price=0.0,
                    observed_at_ms=1_000,
                )

            async def fetch_open_orders(self, symbol: str | None = None):
                self.open_order_calls += 1
                return []

        config.runtime.mode = "live"
        adapters = {
            Venue.BINANCE: BlockingAccountTruthAdapter(Venue.BINANCE),
            Venue.OKX: BlockingAccountTruthAdapter(Venue.OKX),
        }
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal

        first = asyncio.create_task(
            runtime._entry_account_truth_ready_before_dispatch(
                self._candidate("BTCUSDT")
            )
        )
        second = asyncio.create_task(
            runtime._entry_account_truth_ready_before_dispatch(
                self._candidate("BTCUSDT")
            )
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert adapters[Venue.BINANCE].position_calls == 1
        assert adapters[Venue.OKX].position_calls == 1

        release.set()
        assert await asyncio.gather(first, second) == [True, True]
        assert adapters[Venue.BINANCE].open_order_calls == 1
        assert adapters[Venue.OKX].open_order_calls == 1

    @pytest.mark.asyncio
    async def test_entry_account_truth_before_dispatch_times_out_target_venue(
        self, config, tmp_journal,
    ):
        class SlowAccountTruthAdapter(FakeVenueAdapter):
            async def fetch_position(self, symbol: str):
                await asyncio.sleep(0.05)
                return await super().fetch_position(symbol)

            async def fetch_open_orders(self, symbol: str | None = None):
                return []

        class CompleteAccountTruthAdapter(FakeVenueAdapter):
            async def fetch_all_positions(self):
                return []

            async def fetch_open_orders(self, symbol: str | None = None):
                return []

        config.runtime.mode = "live"
        config.runtime.live_recovery_rest_probe_timeout_ms = 60_000
        config.runtime.entry_account_truth_per_venue_timeout_ms = 1
        runtime = LiveRuntime(
            config,
            venue_adapters={
                Venue.BINANCE: SlowAccountTruthAdapter(Venue.BINANCE),
                Venue.OKX: CompleteAccountTruthAdapter(Venue.OKX),
            },
        )
        runtime.journal = tmp_journal

        assert (
            await runtime._entry_account_truth_ready_before_dispatch(
                self._candidate("BTCUSDT")
            )
            is False
        )
        timeout_events = [
            event
            for event in runtime.journal.read_all()
            if event["kind"] == "runtime.entry_account_truth_timeout"
        ]
        assert len(timeout_events) == 1
        assert timeout_events[0]["payload"]["reason"] == (
            "entry_account_truth_timeout_before_dispatch"
        )
        assert "account_truth_timeout:1ms" in timeout_events[0]["payload"]["errors"][0]
        summaries = {
            row["venue"]: row
            for row in timeout_events[0]["payload"]["receipt_summaries"]
        }
        assert summaries[Venue.BINANCE.value]["complete"] is False
        assert summaries[Venue.BINANCE.value]["duration_ms"] >= 0
        assert summaries[Venue.BINANCE.value]["position_count"] == 0
        assert summaries[Venue.BINANCE.value]["open_order_count"] == 0
        assert summaries[Venue.OKX.value]["complete"] is True
        assert "positions" not in summaries[Venue.OKX.value]
        assert "open_orders" not in summaries[Venue.OKX.value]

    @pytest.mark.asyncio
    async def test_tick_continues_to_clean_rank_two_after_rank_one_truth_timeout_and_incomplete_broad_frontier(
        self, config, tmp_journal, monkeypatch,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote
        from lightfee.sidecar.snapshot import QuoteSnapshot, SidecarSnapshot

        class SlowAccountTruthAdapter(FakeVenueAdapter):
            async def fetch_position(self, symbol: str):
                await asyncio.sleep(0.05)
                return await super().fetch_position(symbol)

            async def fetch_open_orders(self, symbol: str | None = None):
                return []

        class CompleteAccountTruthAdapter(FakeVenueAdapter):
            async def fetch_all_positions(self):
                return []

            async def fetch_open_orders(self, symbol: str | None = None):
                return []

        def quote(venue: str, symbol: str) -> QuoteSnapshot:
            return QuoteSnapshot(
                venue=venue,
                symbol=symbol,
                bid=50_000.0,
                ask=50_010.0,
                observed_at_ms=7_000,
                funding_rate_observed_at_ms=7_000,
                funding_rate_received_at_ms=7_000,
                funding_rate_source="test_fixture",
                funding_rate_sample_id=f"funding:{venue}:{symbol}:7000:0:0",
                open_interest=5_000_000.0,
                open_interest_evidence_status="observed",
                open_interest_evidence_reason="test_fixture",
                open_interest_observed_at_ms=7_000,
                open_interest_received_at_ms=7_000,
                open_interest_source="test_fixture",
                open_interest_sample_id=f"{venue}:{symbol}:7000:test_fixture",
                open_interest_venue_symbol=symbol,
            )

        config.runtime.live_scan_recovery_success_count = 1
        config.runtime.mode = "live"
        config.runtime.sidecar_snapshot_max_age_ms = 10_000
        config.runtime.max_market_age_ms = 10_000
        config.runtime.entry_account_truth_per_venue_timeout_ms = 1
        config.strategy.entry_window_secs = 600
        config.strategy.min_scan_minutes_before_funding = 0
        rank_one = self._candidate("BTCUSDT")
        rank_two = self._candidate("ETHUSDT")
        rank_two.long_venue = "aster"
        rank_two.short_venue = "bybit"
        _attach_live_oi_evidence(rank_two, now_ms=5_000)
        for candidate in (rank_one, rank_two):
            candidate.first_funding_timestamp_ms = 7_001 + 10 * 60_000
        adapters = {
            Venue.BINANCE: SlowAccountTruthAdapter(Venue.BINANCE),
            Venue.OKX: CompleteAccountTruthAdapter(Venue.OKX),
            Venue.ASTER: CompleteAccountTruthAdapter(Venue.ASTER),
            Venue.BYBIT: CompleteAccountTruthAdapter(Venue.BYBIT),
        }
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal
        runtime.entry_executor = object()
        runtime.state.lifecycle = EngineLifecycle.RUNNING
        runtime.state.risk_mode = GlobalRiskMode.RUNNING
        snapshot = SidecarSnapshot(
            published_at_ms=7_000,
            market_observed_at_ms=7_000,
            candidate_build_diagnostics={
                "source_data_ready": True,
            },
            candidates=[rank_one, rank_two],
            quotes={
                f"{venue}:{symbol}": quote(venue, symbol)
                for venue, symbol in (
                    ("binance", "BTCUSDT"),
                    ("okx", "BTCUSDT"),
                    ("aster", "ETHUSDT"),
                    ("bybit", "ETHUSDT"),
                )
            },
        )
        for venue, symbol in (
            ("binance", "BTCUSDT"),
            ("okx", "BTCUSDT"),
            ("aster", "ETHUSDT"),
            ("bybit", "ETHUSDT"),
        ):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol=symbol,
                    bid=50_000.0,
                    ask=50_010.0,
                    observed_at_ms=7_000,
                    received_at_ms=7_000,
                    source="test_ws_bbo",
                ),
                now_ms=7_001,
            )

        async def refresh():
            return False

        dispatched_symbols: list[str] = []

        async def dispatch(candidate, now_ms, price_hint=0.0, **_kwargs):
            dispatched_symbols.append(candidate.symbol)
            return True

        monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 7_001)
        _install_single_snapshot_fixture(monkeypatch, snapshot)
        monkeypatch.setattr(
            "lightfee.engine.runtime.discover_tradeable_candidates",
            lambda candidates, _strategy, _now_ms, **_kwargs: list(candidates),
        )
        monkeypatch.setattr(
            runtime,
            "_ensure_entry_account_truth_for_preparation",
            refresh,
        )

        async def preserve_catalog_scope(candidates, **_kwargs):
            return list(candidates)

        monkeypatch.setattr(
            runtime,
            "_filter_candidates_supported_by_venue_catalog",
            preserve_catalog_scope,
        )
        monkeypatch.setattr(runtime, "_dispatch_entry", dispatch)
        monkeypatch.setattr(
            runtime,
            "_select_entry_candidates",
            lambda candidates, **_kwargs: list(candidates),
        )

        await runtime.tick()

        assert dispatched_symbols == ["ETHUSDT"]
        assert runtime.recovery_ledger is None
        timeout_events = [
            event
            for event in runtime.journal.read_all()
            if event["kind"] == "runtime.entry_account_truth_timeout"
        ]
        assert len(timeout_events) == 1
        assert timeout_events[0]["payload"]["reason"] == (
            "entry_account_truth_timeout_before_dispatch"
        )
        assert runtime.state.last_scan["dispatched_candidate_count"] == 1

    @pytest.mark.asyncio
    async def test_restart_first_oi_failure_skips_only_rank_one_candidate(
        self, config, tmp_journal, monkeypatch,
    ):
        from lightfee.engine.market_data_runtime import EntryOpenInterestRefresher

        config.runtime.mode = "live"
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        runtime.state.lifecycle = EngineLifecycle.RUNNING
        runtime.state.risk_mode = GlobalRiskMode.RUNNING
        runtime.state.last_scan = {}
        config.strategy.entry_window_secs = 600
        config.strategy.min_scan_minutes_before_funding = 0
        rank_one = self._candidate("BTCUSDT")
        rank_two = self._candidate("ETHUSDT")

        monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 7_001)

        restarted = EntryOpenInterestRefresher(targeted_budget_s=0.01)
        runtime.entry_open_interest_refresher = restarted
        assert restarted.cached_open_interest(
            "binance", "BTCUSDT", now_ms=7_001
        ) is None

        async def passthrough(candidates, **_kwargs):
            return list(candidates)

        async def quote_truth(candidates, **_kwargs):
            return {}, {"resolved_count": len(candidates)}

        oi_symbols: list[str] = []

        async def refresh_oi(candidates, **_kwargs):
            symbol = candidates[0].symbol
            oi_symbols.append(symbol)
            if symbol == "BTCUSDT":
                raise asyncio.TimeoutError("restart on-demand OI unavailable")
            return {"resolved_count": 1, "timeout_count": 0}

        async def account_ready(_candidate):
            return True, False, ""

        dispatched_symbols: list[str] = []

        async def dispatch(candidate, *_args, **_kwargs):
            dispatched_symbols.append(candidate.symbol)
            return True

        monkeypatch.setattr(
            runtime,
            "_filter_candidates_supported_by_venue_catalog",
            passthrough,
        )
        monkeypatch.setattr(
            runtime,
            "_filter_candidates_by_entry_admission",
            lambda candidates, **_kwargs: list(candidates),
        )
        monkeypatch.setattr(
            runtime,
            "_filter_candidates_by_entry_balance_admission",
            passthrough,
        )
        monkeypatch.setattr(
            runtime,
            "_entry_effective_readiness_provider_uses_ws_bbo",
            lambda: False,
        )
        monkeypatch.setattr(
            runtime,
            "_entry_local_l2_effective_enabled",
            lambda: False,
        )
        monkeypatch.setattr(
            runtime,
            "_entry_quote_revalidate_for_candidates",
            quote_truth,
        )
        monkeypatch.setattr(
            runtime,
            "_refresh_entry_candidate_open_interest_evidence",
            refresh_oi,
        )
        monkeypatch.setattr(
            runtime,
            "_filter_candidates_by_snapshot_freshness",
            lambda candidates, **_kwargs: list(candidates),
        )
        monkeypatch.setattr(
            runtime,
            "_entry_quote_truth_market_quotes",
            lambda *_args, **_kwargs: {},
        )
        monkeypatch.setattr(
            runtime,
            "_reprice_entry_candidates_for_selection",
            lambda candidates, **_kwargs: list(candidates),
        )
        monkeypatch.setattr(
            runtime,
            "_select_entry_candidates",
            lambda candidates, **_kwargs: list(candidates[:1]),
        )
        monkeypatch.setattr(
            runtime,
            "_entry_account_truth_dispatch_readiness",
            account_ready,
        )
        monkeypatch.setattr(runtime, "_dispatch_entry", dispatch)

        try:
            await runtime._run_ranked_candidate_entry_flow(
                [rank_one, rank_two],
                snapshot=SimpleNamespace(quotes={}),
                price_hints={},
            )
        finally:
            await restarted.close()

        assert oi_symbols == ["BTCUSDT", "ETHUSDT"]
        assert dispatched_symbols == ["ETHUSDT"]
        assert runtime.state.last_scan["ranked_candidate_checked_count"] == 2
        failures = [
            record["payload"]
            for record in runtime.journal.read_all()
            if record["kind"] == "runtime.entry_candidate_revalidation_failed"
        ]
        assert failures == [
            {
                "pair_id": runtime._candidate_pair_id(rank_one),
                "domain": "open_interest",
                "error": "TimeoutError: restart on-demand OI unavailable",
                "ts_ms": failures[0]["ts_ms"],
            }
        ]

    @pytest.mark.asyncio
    async def test_entry_account_truth_live_artifact_refreshes_global_recovery(
        self, config, tmp_journal, monkeypatch,
    ):
        class LivePositionAccountTruthAdapter(FakeVenueAdapter):
            async def fetch_position(self, symbol: str):
                return PositionSnapshot(
                    venue=Venue.BINANCE,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=0.5,
                    entry_price=50_000.0,
                    observed_at_ms=7_001,
                )

            async def fetch_open_orders(self, symbol: str | None = None):
                return []

        class CompleteAccountTruthAdapter(FakeVenueAdapter):
            async def fetch_all_positions(self):
                return []

            async def fetch_open_orders(self, symbol: str | None = None):
                return []

        config.runtime.mode = "live"
        runtime = LiveRuntime(
            config,
            venue_adapters={
                Venue.BINANCE: LivePositionAccountTruthAdapter(Venue.BINANCE),
                Venue.OKX: CompleteAccountTruthAdapter(Venue.OKX),
            },
        )
        runtime.journal = tmp_journal
        monkeypatch.setattr(
            "lightfee.engine.runtime.wall_clock_now_ms",
            lambda: 7_001,
        )

        assert (
            await runtime._entry_account_truth_ready_before_dispatch(
                self._candidate("BTCUSDT")
            )
            is False
        )
        assert runtime.recovery_ledger is not None
        assert runtime.recovery_decision is not None
        assert runtime.recovery_decision.entry_allowed is False
        assert runtime.state.recovery_blocked_reason == "unpaired_live_position"
        assert any(
            item.kind == "unpaired_live_position"
            for item in runtime.recovery_ledger.work_items
        )

    @pytest.mark.asyncio
    async def test_tick_uses_target_truth_without_waiting_for_global_refresh(
        self, config, tmp_journal, monkeypatch,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote
        from lightfee.sidecar.snapshot import QuoteSnapshot, SidecarSnapshot

        class CompleteAccountTruthAdapter(FakeVenueAdapter):
            async def fetch_all_positions(self):
                return []

            async def fetch_open_orders(self, symbol: str | None = None):
                return []

        config.runtime.live_scan_recovery_success_count = 1
        config.runtime.mode = "live"
        config.runtime.sidecar_snapshot_max_age_ms = 10_000
        config.runtime.max_market_age_ms = 10_000
        config.strategy.entry_window_secs = 600
        config.strategy.min_scan_minutes_before_funding = 0
        runtime = LiveRuntime(
            config,
            venue_adapters={
                Venue.BINANCE: CompleteAccountTruthAdapter(Venue.BINANCE),
                Venue.OKX: CompleteAccountTruthAdapter(Venue.OKX),
            },
        )
        runtime.journal = tmp_journal
        runtime.entry_executor = object()
        runtime.state.lifecycle = EngineLifecycle.RUNNING
        runtime.state.risk_mode = GlobalRiskMode.RUNNING
        candidate = self._candidate("BTCUSDT")
        candidate.first_funding_timestamp_ms = 7_001 + 10 * 60_000
        snapshot = SidecarSnapshot(
            published_at_ms=7_000,
            market_observed_at_ms=7_000,
            candidates=[candidate],
            quotes={
                "binance:BTCUSDT": QuoteSnapshot(
                    venue="binance",
                    symbol="BTCUSDT",
                    bid=50_000.0,
                    ask=50_010.0,
                    observed_at_ms=7_000,
                    funding_rate_observed_at_ms=7_000,
                    funding_rate_received_at_ms=7_000,
                    funding_rate_source="test_fixture",
                    funding_rate_sample_id="funding:binance:BTCUSDT:7000:0:0",
                    open_interest=5_000_000.0,
                    open_interest_evidence_status="observed",
                    open_interest_evidence_reason="test_fixture",
                    open_interest_observed_at_ms=7_000,
                    open_interest_received_at_ms=7_000,
                    open_interest_source="test_fixture",
                    open_interest_sample_id="binance:BTCUSDT:7000:test_fixture",
                    open_interest_venue_symbol="BTCUSDT",
                ),
                "okx:BTCUSDT": QuoteSnapshot(
                    venue="okx",
                    symbol="BTCUSDT",
                    bid=50_000.0,
                    ask=50_010.0,
                    observed_at_ms=7_000,
                    funding_rate_observed_at_ms=7_000,
                    funding_rate_received_at_ms=7_000,
                    funding_rate_source="test_fixture",
                    funding_rate_sample_id="funding:okx:BTCUSDT:7000:0:0",
                    open_interest=5_000_000.0,
                    open_interest_evidence_status="observed",
                    open_interest_evidence_reason="test_fixture",
                    open_interest_observed_at_ms=7_000,
                    open_interest_received_at_ms=7_000,
                    open_interest_source="test_fixture",
                    open_interest_sample_id="okx:BTCUSDT:7000:test_fixture",
                    open_interest_venue_symbol="BTC-USDT-SWAP",
                )
            },
        )
        for venue in ("binance", "okx"):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="BTCUSDT",
                    bid=50_000.0,
                    ask=50_010.0,
                    observed_at_ms=7_000,
                    received_at_ms=7_000,
                    source="test_ws_bbo",
                ),
                now_ms=7_001,
            )
        order: list[str] = []
        async def refresh():
            order.append("refresh")
            return False

        async def dispatch(candidate, now_ms, price_hint=0.0, **_kwargs):
            order.append("dispatch")
            return True

        def select_candidates(candidates, **_kwargs):
            return list(candidates)

        monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 7_001)
        _install_single_snapshot_fixture(monkeypatch, snapshot)
        monkeypatch.setattr(
            "lightfee.engine.runtime.discover_tradeable_candidates",
            lambda candidates, _strategy, _now_ms, **_kwargs: list(candidates),
        )
        monkeypatch.setattr(
            runtime,
            "_ensure_entry_account_truth_for_preparation",
            refresh,
        )
        async def preserve_catalog_scope(candidates, **_kwargs):
            return list(candidates)

        monkeypatch.setattr(
            runtime,
            "_filter_candidates_supported_by_venue_catalog",
            preserve_catalog_scope,
        )
        monkeypatch.setattr(runtime, "_dispatch_entry", dispatch)
        monkeypatch.setattr(runtime, "_select_entry_candidates", select_candidates)

        await runtime.tick()

        assert order == ["dispatch"]
        assert runtime._entry_account_truth_generation is None
        assert runtime._entry_account_truth_ready_at_ms == 0
        assert runtime._entry_account_truth_venue_receipts == {}

    @pytest.mark.asyncio
    async def test_recovery_ledger_refresh_skips_metadata_only_adapter(
        self, config, tmp_journal,
    ):
        class MetadataOnlyAdapter:
            trading_capability_trusted = True

        runtime = LiveRuntime(
            config,
            venue_adapters={Venue.OKX: MetadataOnlyAdapter()},
        )
        runtime.journal = tmp_journal

        ledger = await runtime._refresh_recovery_ledger_for_symbols(
            ["BTCUSDT"],
            7_001,
        )

        assert ledger is None
        assert runtime.recovery_ledger is None
        assert runtime.state.recovery_blocked_reason is None

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
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.strategy.local_l2_enabled = False
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
                    bid_size=1.0,
                    ask_size=1.0,
                    observed_at_ms=5000,
                    received_at_ms=5000,
                    source=f"{venue}_bbo_ws",
                )
            )

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
        assert payload["operation"] == "entry_post_only_submit"
        assert payload["post_only"] is True
        assert payload["reduce_only"] is False

    @pytest.mark.asyncio
    async def test_fresh_bbo_allows_post_only_maker_submit(self, config, tmp_journal):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.strategy.local_l2_enabled = False
        config.strategy.entry_local_l2_book_stale_after_ms = 1000
        binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
        okx = FakeVenueAdapter(Venue.OKX, _min_notional_quote=10.0)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal
        runtime.state.tick_count = 77
        runtime.entry_executor = EntrySyncExecutor(adapters=adapters, journal=tmp_journal)
        self._install_hot_book(runtime, "binance", "BTCUSDT", bid=50000.0, ask=50010.0, observed_at_ms=5000)
        self._install_hot_book(runtime, "okx", "BTCUSDT", bid=49990.0, ask=50000.0, observed_at_ms=5000)
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
                    bid_size=1.0,
                    ask_size=1.0,
                    observed_at_ms=5000,
                    received_at_ms=5000,
                    source=f"{venue}_bbo_ws",
                )
            )

        assert await runtime._dispatch_entry(self._candidate(), 5000, price_hint=50000.0) is True

        assert binance.last_request is not None
        assert binance.last_request.post_only is True
        assert binance.last_request.price == 50000.0
        assert len(runtime.state.pending_entries) == 1
        pending = next(iter(runtime.state.pending_entries.values()))
        assert pending.created_cycle == 77
        assert pending.passive_manager_runtime.consecutive_failures == 0

    @pytest.mark.asyncio
    async def test_final_gate_blocks_fresh_bbo_with_excessive_leg_skew(
        self, config, tmp_journal,
    ):
        config.runtime.mode = "live"
        config.strategy.funding_new_entries_enabled = True
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
    async def test_enhanced_live_dispatch_blocks_projected_portfolio_symbol_limit(
        self, config, tmp_journal,
    ):
        """The final L2-sized order must be admitted against live positions."""
        config.runtime.mode = "live"
        config.strategy.funding_new_entries_enabled = True
        config.strategy.local_l2_enabled = False
        config.strategy.entry_local_l2_book_stale_after_ms = 1_000
        binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
        okx = FakeVenueAdapter(Venue.OKX, _min_notional_quote=10.0)
        binance.available_margin_quote = 1_000.0
        okx.available_margin_quote = 1_000.0
        inspected = EntryLeverageEvidence(
            venue=Venue.BINANCE,
            symbol="BTCUSDT",
            requested_leverage=4,
            effective_leverage=4,
            notional_quote=50.0,
            bracket_verified=True,
            account_verified=True,
            source="test_get_only",
            observed_at_ms=5_000,
            account_leverage=4,
        )
        binance.inspect_entry_leverage = AsyncMock(return_value=inspected)
        binance.ensure_entry_leverage = AsyncMock(return_value=inspected)
        runtime = LiveRuntime(
            config,
            venue_adapters={Venue.BINANCE: binance, Venue.OKX: okx},
        )
        config.strategy.max_concurrent_positions_per_symbol = 0
        runtime.journal = tmp_journal
        runtime.entry_executor = self._capturing_executor()
        self._install_hot_book(
            runtime, "binance", "BTCUSDT",
            bid=50_000.0, ask=50_010.0, observed_at_ms=5_000,
        )
        self._install_hot_book(
            runtime, "okx", "BTCUSDT",
            bid=49_990.0, ask=50_000.0, observed_at_ms=5_000,
        )
        for venue, bid, ask in (
            ("binance", 50_000.0, 50_010.0),
            ("okx", 49_990.0, 50_000.0),
        ):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="BTCUSDT",
                    bid=bid,
                    ask=ask,
                    bid_size=1.0,
                    ask_size=1.0,
                    observed_at_ms=5_000,
                    received_at_ms=5_000,
                    source="test_ws_bbo",
                )
            )
        runtime.state.open_positions["open-btc"] = OpenPosition(
            position_id="open-btc",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=0.0016,
            short_quantity=0.0016,
            long_entry_price=50_000.0,
            short_entry_price=50_000.0,
            opened_at_ms=1_000,
            funding_timestamp_ms=605_000,
            expected_shortfall_bps_entry=10.0,
        )
        candidate = self._candidate()
        candidate.entry_notional_quote = 50.0
        candidate.entry_target_quantity = 0.001
        candidate.entry_max_executable_quantity = 0.001
        assert runtime.entry_readiness_provider.decide(candidate, 5_000).allowed

        dispatched = await runtime._dispatch_entry(candidate, 5_000, price_hint=50_000.0)

        assert dispatched is False
        assert runtime.entry_executor.ctx is None
        # Portfolio admission happens after GET-only sizing but before the
        # first mutating leverage operation.
        assert binance.inspect_entry_leverage.await_count == 1
        assert binance.ensure_entry_leverage.await_count == 0
        payload = [
            record["payload"]
            for record in tmp_journal.read_all()
            if record["kind"] == "entry.dispatch_viability_blocked"
        ][-1]
        assert payload["reason"] == "max_symbol_exposure"
        assert payload["source"] == "strategy_risk_allocator"
        assert payload["projected_symbol_exposure_quote"] > 100.0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "final_kind",
        ["missing", "incomplete", "lower"],
    )
    async def test_enhanced_live_dispatch_rejects_weakened_final_leverage_evidence(
        self,
        config,
        tmp_journal,
        monkeypatch,
        final_kind: str,
    ) -> None:
        """Sizing evidence may never survive a weaker post-set confirmation."""
        config.runtime.mode = "live"
        config.strategy.funding_new_entries_enabled = True
        # This fixture isolates post-sizing leverage confirmation.  Local-L2
        # readiness is exercised by the V1 lifecycle and composed-provider
        # tests; it is not the subject of this final-dispatch test.
        config.strategy.local_l2_enabled = False
        config.strategy.entry_local_l2_book_stale_after_ms = 1_000
        quantity_metadata = {
            "min_notional": 10.0,
            "min_quantity": 0.0001,
            "quantity_step": 0.0001,
        }
        binance = FakeVenueAdapter(
            Venue.BINANCE,
            _min_notional_quote=10.0,
            passive_metadata_payload=quantity_metadata,
        )
        okx = FakeVenueAdapter(
            Venue.OKX,
            _min_notional_quote=10.0,
            passive_metadata_payload=quantity_metadata,
        )
        binance.available_margin_quote = 1_000.0
        okx.available_margin_quote = 1_000.0
        inspected = EntryLeverageEvidence(
            venue=Venue.BINANCE,
            symbol="BTCUSDT",
            requested_leverage=4,
            effective_leverage=4,
            notional_quote=50.0,
            bracket_verified=True,
            account_verified=True,
            source="test_get_only",
            observed_at_ms=5_000,
            account_leverage=4,
        )
        if final_kind == "missing":
            final_evidence = None
        elif final_kind == "incomplete":
            final_evidence = EntryLeverageEvidence(
                venue=Venue.BINANCE,
                symbol="BTCUSDT",
                requested_leverage=4,
                effective_leverage=4,
                notional_quote=50.0,
                bracket_verified=False,
                account_verified=True,
                source="test_post_set_incomplete",
                observed_at_ms=5_000,
                account_leverage=4,
            )
        else:
            final_evidence = EntryLeverageEvidence(
                venue=Venue.BINANCE,
                symbol="BTCUSDT",
                requested_leverage=4,
                effective_leverage=2,
                notional_quote=50.0,
                bracket_verified=True,
                account_verified=True,
                source="test_post_set_lower",
                observed_at_ms=5_000,
                account_leverage=2,
            )
        binance.inspect_entry_leverage = AsyncMock(return_value=inspected)
        # A missing confirmation or lower effective leverage is treated as a
        # potentially applied mutation and therefore receives a verified
        # best-effort restore to the just-inspected account setting.
        if final_kind in {"missing", "lower"}:
            binance.ensure_entry_leverage = AsyncMock(
                side_effect=[final_evidence, inspected]
            )
        else:
            binance.ensure_entry_leverage = AsyncMock(return_value=final_evidence)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        monkeypatch.setattr(runtime, "_entry_wall_clock_now_ms", lambda: 5_000)
        runtime.journal = tmp_journal
        runtime.entry_executor = self._capturing_executor()
        self._install_hot_book(
            runtime, "binance", "BTCUSDT",
            bid=50_000.0, ask=50_010.0, observed_at_ms=5_000,
        )
        self._install_hot_book(
            runtime, "okx", "BTCUSDT",
            bid=49_990.0, ask=50_000.0, observed_at_ms=5_000,
        )
        for venue, bid, ask in (
            ("binance", 50_000.0, 50_010.0),
            ("okx", 49_990.0, 50_000.0),
        ):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="BTCUSDT",
                    bid=bid,
                    ask=ask,
                    bid_size=1.0,
                    ask_size=1.0,
                    observed_at_ms=5_000,
                    received_at_ms=5_000,
                    source="test_ws_bbo",
                )
            )
        candidate = self._candidate()
        candidate.entry_notional_quote = 50.0
        candidate.entry_target_quantity = 0.001
        candidate.entry_max_executable_quantity = 0.001
        assert runtime.entry_readiness_provider.decide(candidate, 5_000).allowed

        dispatched = await runtime._dispatch_entry(candidate, 5_000, price_hint=50_000.0)

        assert dispatched is False
        assert runtime.entry_executor.ctx is None
        assert binance.inspect_entry_leverage.await_count == 2
        assert binance.ensure_entry_leverage.await_count == (
            2 if final_kind in {"missing", "lower"} else 1
        )
        blocked = [
            record["payload"]
            for record in tmp_journal.read_all()
            if record["kind"] == "entry.dispatch_viability_blocked"
        ]
        assert blocked[-1]["reason"] == "entry_leverage_weakened_after_sizing"
        assert blocked[-1]["final_evidence_complete"] is (final_kind == "lower")
        if final_kind in {"missing", "lower"}:
            assert any(
                record["kind"] == "execution.entry_leverage_compensated"
                for record in tmp_journal.read_all()
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "prepare_error",
        [OSError("injected aster prepare failure"), asyncio.CancelledError()],
    )
    async def test_final_leverage_prepare_restores_first_venue_after_second_fails(
        self,
        config,
        tmp_journal,
        prepare_error: BaseException,
    ) -> None:
        """A rejected pair must not retain a unilateral leverage mutation."""
        config.runtime.mode = "live"
        config.strategy.live_target_leverage = 4
        binance = FakeVenueAdapter(Venue.BINANCE)
        aster = FakeVenueAdapter(Venue.ASTER)

        def evidence(venue: Venue, effective: int, account: int) -> EntryLeverageEvidence:
            return EntryLeverageEvidence(
                venue=venue,
                symbol="BTCUSDT",
                requested_leverage=4,
                effective_leverage=effective,
                notional_quote=50.0,
                bracket_verified=True,
                account_verified=True,
                source="test",
                observed_at_ms=5_000,
                account_leverage=account,
            )

        binance.inspect_entry_leverage = AsyncMock(
            return_value=evidence(Venue.BINANCE, 2, 2)
        )
        binance.ensure_entry_leverage = AsyncMock(
            side_effect=[
                # The venue accepts account leverage 4, but its active
                # notional bracket still caps executable leverage at 2.
                # Compensation must restore the account setting, not infer
                # that no mutation occurred from the capped effective value.
                evidence(Venue.BINANCE, 2, 4),
                evidence(Venue.BINANCE, 2, 2),
            ]
        )
        aster.inspect_entry_leverage = AsyncMock(
            return_value=evidence(Venue.ASTER, 2, 2)
        )
        aster.ensure_entry_leverage = AsyncMock(
            side_effect=prepare_error
        )
        runtime = LiveRuntime(
            config,
            venue_adapters={Venue.BINANCE: binance, Venue.ASTER: aster},
        )
        runtime.journal = tmp_journal

        ready, evidence_by_venue = (
            await runtime.entry_dispatch_runtime._prepare_live_entry_leverage_for_candidate(
                candidate=self._candidate(),
                now_ms=5_000,
                long_venue=Venue.BINANCE,
                short_venue=Venue.ASTER,
                notional_quote=50.0,
            )
        )

        assert ready is False
        assert evidence_by_venue == {}
        assert binance.ensure_entry_leverage.await_count == 2
        first_call, restore_call = binance.ensure_entry_leverage.await_args_list
        assert first_call.args[1] == 4
        assert restore_call.args[1] == 2
        assert restore_call.kwargs["notional_quote"] == 0.0
        # An uncertain transport failure can still mean the exchange accepted
        # the first mutation; restore is attempted on that venue as well.
        assert aster.ensure_entry_leverage.await_count == 2
        assert any(
            record["kind"] == "execution.entry_leverage_compensated"
            for record in tmp_journal.read_all()
        )

    @pytest.mark.asyncio
    async def test_final_leverage_prepare_timeout_restores_every_attempted_venue(
        self,
        config,
        tmp_journal,
    ) -> None:
        """Caller cancellation waits for the compensating transaction."""
        config.runtime.mode = "live"
        config.strategy.live_target_leverage = 4
        binance = FakeVenueAdapter(Venue.BINANCE)
        aster = FakeVenueAdapter(Venue.ASTER)

        def evidence(venue: Venue, leverage: int) -> EntryLeverageEvidence:
            return EntryLeverageEvidence(
                venue=venue,
                symbol="BTCUSDT",
                requested_leverage=4,
                effective_leverage=leverage,
                notional_quote=50.0,
                bracket_verified=True,
                account_verified=True,
                source="test",
                observed_at_ms=5_000,
                account_leverage=leverage,
            )

        binance_calls: list[int] = []
        aster_calls: list[int] = []

        async def binance_ensure(symbol: str, leverage: int, **_kwargs) -> EntryLeverageEvidence:
            binance_calls.append(leverage)
            return evidence(Venue.BINANCE, leverage)

        async def slow_aster_ensure(symbol: str, leverage: int, **_kwargs) -> EntryLeverageEvidence:
            aster_calls.append(leverage)
            if leverage == 4:
                await asyncio.sleep(0.05)
            return evidence(Venue.ASTER, leverage)

        binance.inspect_entry_leverage = AsyncMock(
            return_value=evidence(Venue.BINANCE, 2)
        )
        binance.ensure_entry_leverage = binance_ensure
        aster.inspect_entry_leverage = AsyncMock(
            return_value=evidence(Venue.ASTER, 2)
        )
        aster.ensure_entry_leverage = slow_aster_ensure
        runtime = LiveRuntime(
            config,
            venue_adapters={Venue.BINANCE: binance, Venue.ASTER: aster},
        )
        runtime.journal = tmp_journal

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                runtime.entry_dispatch_runtime._prepare_live_entry_leverage_for_candidate(
                    candidate=self._candidate(),
                    now_ms=5_000,
                    long_venue=Venue.BINANCE,
                    short_venue=Venue.ASTER,
                    notional_quote=50.0,
                ),
                timeout=0.01,
            )

        assert binance_calls == [4, 2]
        assert aster_calls == [4, 2]
        compensated = [
            record
            for record in tmp_journal.read_all()
            if record["kind"] == "execution.entry_leverage_compensated"
        ]
        assert {record["payload"]["venue"] for record in compensated} == {
            "binance",
            "aster",
        }

    @pytest.mark.asyncio
    async def test_leverage_prepare_non_live_path_completes_without_handoff_hang(
        self,
        config,
        tmp_journal,
    ) -> None:
        """A no-mutation path must still complete the outer hand-off protocol."""
        config.runtime.mode = "paper"
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal

        ready, evidence_by_venue = await asyncio.wait_for(
            runtime.entry_dispatch_runtime._prepare_live_entry_leverage_for_candidate(
                candidate=self._candidate(),
                now_ms=5_000,
                long_venue=Venue.BINANCE,
                short_venue=Venue.ASTER,
                notional_quote=50.0,
            ),
            timeout=0.05,
        )

        assert ready is True
        assert evidence_by_venue == {}

    @pytest.mark.asyncio
    async def test_leverage_ready_receipt_exists_before_prepare_returns(
        self,
        config,
        tmp_journal,
    ) -> None:
        """A successful prepare may not let dispatch submit before its receipt."""
        config.runtime.mode = "live"
        config.strategy.live_target_leverage = 4
        binance = FakeVenueAdapter(Venue.BINANCE)
        original = EntryLeverageEvidence(
            venue=Venue.BINANCE,
            symbol="BTCUSDT",
            requested_leverage=4,
            effective_leverage=2,
            notional_quote=50.0,
            bracket_verified=True,
            account_verified=True,
            source="test",
            observed_at_ms=5_000,
            account_leverage=2,
        )
        prepared = EntryLeverageEvidence(
            venue=Venue.BINANCE,
            symbol="BTCUSDT",
            requested_leverage=4,
            effective_leverage=4,
            notional_quote=50.0,
            bracket_verified=True,
            account_verified=True,
            source="test",
            observed_at_ms=5_000,
            account_leverage=4,
        )
        binance.inspect_entry_leverage = AsyncMock(
            side_effect=[original, prepared]
        )
        binance.ensure_entry_leverage = AsyncMock(return_value=prepared)
        runtime = LiveRuntime(
            config,
            venue_adapters={Venue.BINANCE: binance},
        )
        runtime.journal = tmp_journal

        ready, evidence_by_venue = (
            await runtime.entry_dispatch_runtime._prepare_live_entry_leverage_for_candidate(
                candidate=self._candidate(),
                now_ms=5_000,
                long_venue=Venue.BINANCE,
                short_venue=Venue.OKX,
                notional_quote=50.0,
            )
        )

        assert ready is True
        assert evidence_by_venue == {Venue.BINANCE: prepared}
        assert any(
            record["kind"] == "execution.entry_leverage_ready"
            for record in tmp_journal.read_all()
        )

    @pytest.mark.asyncio
    async def test_leverage_prepare_repeated_cancellation_waits_for_all_restores(
        self,
        config,
        tmp_journal,
    ) -> None:
        """A second cancellation cannot detach the compensating child task."""
        config.runtime.mode = "live"
        config.strategy.live_target_leverage = 4
        binance = FakeVenueAdapter(Venue.BINANCE)
        aster = FakeVenueAdapter(Venue.ASTER)
        aster_started = asyncio.Event()
        binance_calls: list[int] = []
        aster_calls: list[int] = []

        def evidence(venue: Venue, leverage: int) -> EntryLeverageEvidence:
            return EntryLeverageEvidence(
                venue=venue,
                symbol="BTCUSDT",
                requested_leverage=4,
                effective_leverage=leverage,
                notional_quote=50.0,
                bracket_verified=True,
                account_verified=True,
                source="test",
                observed_at_ms=5_000,
                account_leverage=leverage,
            )

        async def binance_ensure(_symbol: str, leverage: int, **_kwargs):
            binance_calls.append(leverage)
            return evidence(Venue.BINANCE, leverage)

        async def aster_ensure(_symbol: str, leverage: int, **_kwargs):
            aster_calls.append(leverage)
            if leverage == 4:
                aster_started.set()
                await asyncio.sleep(0.02)
            return evidence(Venue.ASTER, leverage)

        binance.inspect_entry_leverage = AsyncMock(return_value=evidence(Venue.BINANCE, 2))
        binance.ensure_entry_leverage = binance_ensure
        aster.inspect_entry_leverage = AsyncMock(return_value=evidence(Venue.ASTER, 2))
        aster.ensure_entry_leverage = aster_ensure
        runtime = LiveRuntime(
            config,
            venue_adapters={Venue.BINANCE: binance, Venue.ASTER: aster},
        )
        runtime.journal = tmp_journal
        task = asyncio.create_task(
            runtime.entry_dispatch_runtime._prepare_live_entry_leverage_for_candidate(
                candidate=self._candidate(),
                now_ms=5_000,
                long_venue=Venue.BINANCE,
                short_venue=Venue.ASTER,
                notional_quote=50.0,
            )
        )

        await aster_started.wait()
        task.cancel()
        await asyncio.sleep(0.001)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert binance_calls == [4, 2]
        assert aster_calls == [4, 2]

    @pytest.mark.asyncio
    async def test_leverage_compensation_continues_when_audit_append_fails(
        self,
        config,
        tmp_journal,
        monkeypatch,
    ) -> None:
        """Audit-sink failure must fail closed without skipping a venue restore."""
        config.runtime.mode = "live"
        config.strategy.live_target_leverage = 4
        binance = FakeVenueAdapter(Venue.BINANCE)
        aster = FakeVenueAdapter(Venue.ASTER)
        binance_calls: list[int] = []
        aster_calls: list[int] = []

        def evidence(venue: Venue, leverage: int) -> EntryLeverageEvidence:
            return EntryLeverageEvidence(
                venue=venue,
                symbol="BTCUSDT",
                requested_leverage=4,
                effective_leverage=leverage,
                notional_quote=50.0,
                bracket_verified=True,
                account_verified=True,
                source="test",
                observed_at_ms=5_000,
                account_leverage=leverage,
            )

        async def binance_ensure(_symbol: str, leverage: int, **_kwargs):
            binance_calls.append(leverage)
            return evidence(Venue.BINANCE, leverage)

        async def failing_aster_ensure(_symbol: str, leverage: int, **_kwargs):
            aster_calls.append(leverage)
            if leverage == 4:
                raise OSError("injected target leverage transport failure")
            return evidence(Venue.ASTER, leverage)

        binance.inspect_entry_leverage = AsyncMock(return_value=evidence(Venue.BINANCE, 2))
        binance.ensure_entry_leverage = binance_ensure
        aster.inspect_entry_leverage = AsyncMock(return_value=evidence(Venue.ASTER, 2))
        aster.ensure_entry_leverage = failing_aster_ensure
        original_append = tmp_journal.append
        compensation_append_failed = False

        def append_with_one_compensation_failure(kind, payload):
            nonlocal compensation_append_failed
            if kind == "execution.entry_leverage_compensated" and not compensation_append_failed:
                compensation_append_failed = True
                raise OSError("injected compensation receipt write failure")
            return original_append(kind, payload)

        monkeypatch.setattr(tmp_journal, "append", append_with_one_compensation_failure)
        runtime = LiveRuntime(
            config,
            venue_adapters={Venue.BINANCE: binance, Venue.ASTER: aster},
        )
        runtime.journal = tmp_journal

        ready, evidence_by_venue = (
            await runtime.entry_dispatch_runtime._prepare_live_entry_leverage_for_candidate(
                candidate=self._candidate(),
                now_ms=5_000,
                long_venue=Venue.BINANCE,
                short_venue=Venue.ASTER,
                notional_quote=50.0,
            )
        )

        assert ready is False
        assert evidence_by_venue == {}
        assert binance_calls == [4, 2]
        assert aster_calls == [4, 2]
        assert runtime.state.venue_entry_cooldowns["binance:BTCUSDT"]["reason"] == (
            "entry_leverage_compensation_failed"
        )

    @pytest.mark.asyncio
    async def test_leverage_weakened_receipt_uses_the_failing_venue_in_audit(
        self,
        config,
        tmp_journal,
    ) -> None:
        """A semantic failure's durable event must not reuse another venue's data."""
        config.runtime.mode = "live"
        config.strategy.live_target_leverage = 4
        binance = FakeVenueAdapter(Venue.BINANCE)
        aster = FakeVenueAdapter(Venue.ASTER)

        def evidence(venue: Venue, effective: int, account: int) -> EntryLeverageEvidence:
            return EntryLeverageEvidence(
                venue=venue,
                symbol="BTCUSDT",
                requested_leverage=4,
                effective_leverage=effective,
                notional_quote=50.0,
                bracket_verified=True,
                account_verified=True,
                source="test",
                observed_at_ms=5_000,
                account_leverage=account,
            )

        binance.inspect_entry_leverage = AsyncMock(return_value=evidence(Venue.BINANCE, 2, 2))
        binance.ensure_entry_leverage = AsyncMock(
            side_effect=[evidence(Venue.BINANCE, 1, 1), evidence(Venue.BINANCE, 2, 2)]
        )
        aster.inspect_entry_leverage = AsyncMock(return_value=evidence(Venue.ASTER, 2, 2))
        aster.ensure_entry_leverage = AsyncMock(
            side_effect=[evidence(Venue.ASTER, 4, 4), evidence(Venue.ASTER, 2, 2)]
        )
        runtime = LiveRuntime(
            config,
            venue_adapters={Venue.BINANCE: binance, Venue.ASTER: aster},
        )
        runtime.journal = tmp_journal

        ready, _evidence_by_venue = (
            await runtime.entry_dispatch_runtime._prepare_live_entry_leverage_for_candidate(
                candidate=self._candidate(),
                now_ms=5_000,
                long_venue=Venue.BINANCE,
                short_venue=Venue.ASTER,
                notional_quote=50.0,
            )
        )

        assert ready is False
        unavailable = [
            record["payload"]
            for record in tmp_journal.read_all()
            if record["kind"] == "execution.entry_leverage_unavailable"
        ]
        assert unavailable[-1]["venue"] == "binance"
        assert "weaker than pre-set evidence" in unavailable[-1]["raw_error"]

    @pytest.mark.asyncio
    async def test_ws_bbo_provider_dispatch_does_not_require_local_l2_books(
        self,
        config,
        tmp_journal,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.strategy.local_l2_enabled = False
        config.strategy.entry_readiness_provider = "ws_bbo_l2_on_demand"
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
                    bid_size=10.0,
                    ask_size=10.0,
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
    async def test_ws_bbo_provider_maker_event_uses_in_situ_reprice_not_new_entry(
        self,
        config,
        tmp_journal,
        monkeypatch,
    ):
        from lightfee.engine.passive_order_manager import (
            PassiveOrderManager,
            PassiveOrderManagerProfile,
        )
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.runtime.mode = "live"
        config.runtime.maker_event_lane_enabled = True
        config.runtime.maker_event_lane_min_wake_interval_ms = 0
        config.strategy.entry_readiness_provider = "ws_bbo_l2_on_demand"
        config.strategy.local_l2_enabled = True
        config.strategy.passive_reprice_threshold_bps = 1.0
        config.strategy.passive_cancel_replace_threshold_bps = 100.0
        runtime = LiveRuntime(
            config,
            venue_adapters={
                Venue.BINANCE: FakeVenueAdapter(Venue.BINANCE),
                Venue.OKX: FakeVenueAdapter(Venue.OKX),
            },
        )
        runtime.journal = tmp_journal
        pending = PendingEntry(
            pending_id="pe-ws-bbo",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            target_quantity=0.01,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=1000,
            entry_type="passive_incremental",
            maker_price=50000.0,
            long_quantity=0.01,
            short_quantity=0.01,
            maker_leg="long",
            entry_maker_leg="long",
        )
        runtime.state.pending_entries[pending.pending_id] = pending
        profile = PassiveOrderManagerProfile(
            max_consecutive_failures=3,
            failure_cooldown_ms=0,
            reprice_threshold_bps=1.0,
            cancel_replace_threshold_bps=100.0,
        )
        runtime._maker_event_state[pending.pending_id] = (
            PassiveOrderManager(profile),
            50000.0,
        )
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=50020.0,
                ask=50030.0,
                observed_at_ms=5000,
                received_at_ms=5000,
                source="binance_bbo_ws",
            )
        )
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="okx",
                symbol="BTCUSDT",
                bid=49990.0,
                ask=50000.0,
                observed_at_ms=5000,
                received_at_ms=5000,
                source="okx_bbo_ws",
            )
        )

        class RejectNewEntryExecutor:
            async def execute(self, ctx):
                raise AssertionError("WS BBO maker-event must not start a new entry flow")

        calls: list[tuple[float, float, str, str]] = []

        async def fake_reprice(pending_arg, new_price, old_price, action, now_ms, entry_id):
            calls.append((new_price, old_price, action, entry_id))
            return SimpleNamespace(order_id="amended-maker-1")

        runtime.entry_executor = RejectNewEntryExecutor()
        monkeypatch.setattr(runtime, "_reprice_passive_maker_l2", fake_reprice)

        await runtime._maybe_tick_maker_event(5000)

        assert calls == [(50020.0, 50000.0, "cancel_replace", pending.pending_id)]
        assert runtime.local_l2_runtime.get_book("binance", "BTCUSDT") is None
        assert runtime.state.pending_entries[pending.pending_id].maker_order_id == "amended-maker-1"
        records = tmp_journal.read_all()
        assert any(
            record["kind"] == "runtime.maker_event_lane_wake"
            and record["payload"]["source"] == "ws_bbo_quote_lease"
            for record in records
        )
        assert all(
            not str(record["kind"]).startswith("runtime.local_l2_")
            for record in records
        )

    @pytest.mark.asyncio
    async def test_ws_bbo_provider_maker_event_uses_same_side_ask_for_sell_maker(
        self,
        config,
        tmp_journal,
        monkeypatch,
    ):
        from lightfee.engine.passive_order_manager import (
            PassiveOrderManager,
            PassiveOrderManagerProfile,
        )

        config.runtime.mode = "live"
        config.runtime.maker_event_lane_enabled = True
        config.runtime.maker_event_lane_min_wake_interval_ms = 0
        config.strategy.entry_readiness_provider = "ws_bbo_l2_on_demand"
        config.strategy.local_l2_enabled = True
        config.strategy.passive_reprice_threshold_bps = 1.0
        config.strategy.passive_cancel_replace_threshold_bps = 100.0
        runtime = LiveRuntime(
            config,
            venue_adapters={
                Venue.BINANCE: FakeVenueAdapter(Venue.BINANCE),
                Venue.OKX: FakeVenueAdapter(Venue.OKX),
            },
        )
        runtime.journal = tmp_journal
        pending = PendingEntry(
            pending_id="pe-ws-bbo-short-maker",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            target_quantity=0.01,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=1000,
            entry_type="passive_incremental",
            maker_price=50000.0,
            long_quantity=0.01,
            short_quantity=0.01,
            maker_leg="short",
            entry_maker_leg="short",
        )
        runtime.state.pending_entries[pending.pending_id] = pending
        profile = PassiveOrderManagerProfile(
            max_consecutive_failures=3,
            failure_cooldown_ms=0,
            reprice_threshold_bps=1.0,
            cancel_replace_threshold_bps=100.0,
        )
        runtime._maker_event_state[pending.pending_id] = (
            PassiveOrderManager(profile),
            50000.0,
        )
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=49990.0,
                ask=50000.0,
                observed_at_ms=5000,
                received_at_ms=5000,
                source="binance_bbo_ws",
            )
        )
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="okx",
                symbol="BTCUSDT",
                bid=50020.0,
                ask=50040.0,
                observed_at_ms=5000,
                received_at_ms=5000,
                source="okx_bbo_ws",
            )
        )

        class RejectNewEntryExecutor:
            async def execute(self, ctx):
                raise AssertionError("WS BBO maker-event must not start a new entry flow")

        calls: list[tuple[float, float, str, str]] = []

        async def fake_reprice(pending_arg, new_price, old_price, action, now_ms, entry_id):
            calls.append((new_price, old_price, action, entry_id))
            return SimpleNamespace(order_id="amended-maker-short")

        runtime.entry_executor = RejectNewEntryExecutor()
        monkeypatch.setattr(runtime, "_reprice_passive_maker_l2", fake_reprice)

        await runtime._maybe_tick_maker_event(5000)

        assert calls == [(50040.0, 50000.0, "cancel_replace", pending.pending_id)]
        assert runtime.state.pending_entries[pending.pending_id].maker_order_id == (
            "amended-maker-short"
        )

    @pytest.mark.parametrize(
        (
            "hedge_observed_at_ms",
            "ttl_ms",
            "max_skew_ms",
            "expected_role",
            "expected_reason",
        ),
        [
            (4800, 100, 1000, "hedge", "stale_bbo"),
            (4800, 1000, 100, "maker_hedge_pair", "quote_skew_exceeded"),
        ],
    )
    @pytest.mark.asyncio
    async def test_ws_bbo_provider_maker_event_blocks_stale_or_skewed_hedge_bbo(
        self,
        config,
        tmp_journal,
        monkeypatch,
        hedge_observed_at_ms,
        ttl_ms,
        max_skew_ms,
        expected_role,
        expected_reason,
    ):
        from lightfee.engine.passive_order_manager import (
            PassiveOrderManager,
            PassiveOrderManagerProfile,
        )

        config.runtime.mode = "live"
        config.runtime.maker_event_lane_enabled = True
        config.runtime.maker_event_lane_min_wake_interval_ms = 0
        config.strategy.entry_readiness_provider = "ws_bbo_l2_on_demand"
        config.strategy.local_l2_enabled = True
        config.strategy.entry_quote_lease_ttl_ms = ttl_ms
        config.strategy.entry_final_gate_max_skew_ms = max_skew_ms
        config.strategy.passive_reprice_threshold_bps = 1.0
        config.strategy.passive_cancel_replace_threshold_bps = 100.0
        runtime = LiveRuntime(
            config,
            venue_adapters={
                Venue.BINANCE: FakeVenueAdapter(Venue.BINANCE),
                Venue.OKX: FakeVenueAdapter(Venue.OKX),
            },
        )
        runtime.journal = tmp_journal
        pending = PendingEntry(
            pending_id=f"pe-ws-bbo-{expected_reason}",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            target_quantity=0.01,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=1000,
            entry_type="passive_incremental",
            maker_price=50000.0,
            long_quantity=0.01,
            short_quantity=0.01,
            maker_leg="long",
            entry_maker_leg="long",
        )
        runtime.state.pending_entries[pending.pending_id] = pending
        profile = PassiveOrderManagerProfile(
            max_consecutive_failures=3,
            failure_cooldown_ms=0,
            reprice_threshold_bps=1.0,
            cancel_replace_threshold_bps=100.0,
        )
        runtime._maker_event_state[pending.pending_id] = (
            PassiveOrderManager(profile),
            50000.0,
        )
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=50020.0,
                ask=50030.0,
                observed_at_ms=5000,
                received_at_ms=5000,
                source="binance_bbo_ws",
            )
        )
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="okx",
                symbol="BTCUSDT",
                bid=49990.0,
                ask=50000.0,
                observed_at_ms=hedge_observed_at_ms,
                received_at_ms=hedge_observed_at_ms,
                source="okx_bbo_ws",
            )
        )

        class RejectNewEntryExecutor:
            async def execute(self, ctx):
                raise AssertionError("WS BBO maker-event must not start a new entry flow")

        calls: list[tuple[float, float, str, str]] = []

        async def fake_reprice(pending_arg, new_price, old_price, action, now_ms, entry_id):
            calls.append((new_price, old_price, action, entry_id))
            return SimpleNamespace(order_id="must-not-submit")

        runtime.entry_executor = RejectNewEntryExecutor()
        monkeypatch.setattr(runtime, "_reprice_passive_maker_l2", fake_reprice)

        await runtime._maybe_tick_maker_event(5000)

        assert calls == []
        records = tmp_journal.read_all()
        payload = [
            record["payload"]
            for record in records
            if record["kind"] == "runtime.maker_event_no_ws_bbo_quote"
        ][-1]
        sample = payload["samples"][0]
        assert sample["role"] == expected_role
        assert sample["reason"] == expected_reason
        assert "runtime.maker_event_lane_wake" not in [
            record["kind"] for record in records
        ]

    @pytest.mark.asyncio
    async def test_ws_bbo_provider_dispatch_requires_selected_quote_lease(
        self,
        config,
        tmp_journal,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.runtime.mode = "live"
        config.strategy.funding_new_entries_enabled = True
        config.strategy.local_l2_enabled = False
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
                    bid_size=10.0,
                    ask_size=10.0,
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
        assert payload["provider"] == "ws_bbo_l2_on_demand"

    def test_ws_bbo_provider_stale_execution_lease_records_both_leg_ages(
        self,
        config,
        tmp_journal,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.runtime.mode = "live"
        config.strategy.funding_new_entries_enabled = True
        config.strategy.local_l2_enabled = False
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1500
        runtime = LiveRuntime(config)
        runtime.journal = tmp_journal
        candidate = self._candidate()
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=50000.0,
                ask=50010.0,
                observed_at_ms=1000,
                received_at_ms=1000,
                source="binance_bbo_ws",
            )
        )
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="okx",
                symbol="BTCUSDT",
                bid=49990.0,
                ask=50000.0,
                observed_at_ms=2500,
                received_at_ms=2500,
                source="okx_bbo_ws",
            )
        )
        readiness = runtime.entry_readiness_provider.decide(candidate, 2500)
        assert readiness.allowed

        reason, _lease, evidence = runtime._entry_quote_lease_execution_check(
            candidate,
            3001,
        )

        assert reason == "stale_quote_lease"
        assert evidence["blocker_family"] == "stale_quote"
        assert evidence["quote_age_ms"] == {"long": 2001, "short": 501}
        assert evidence["long_age_ms"] == 2001
        assert evidence["short_age_ms"] == 501

    def test_ws_bbo_execution_lease_carries_sizes_and_blocks_skew(
        self,
        config,
        tmp_journal,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.runtime.mode = "live"
        config.strategy.funding_new_entries_enabled = True
        config.strategy.local_l2_enabled = False
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1500
        config.strategy.entry_final_gate_max_skew_ms = 100
        runtime = LiveRuntime(config)
        runtime.journal = tmp_journal
        candidate = self._candidate()
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=50000.0,
                ask=50010.0,
                bid_size=1.25,
                ask_size=2.5,
                observed_at_ms=2500,
                received_at_ms=2500,
                source="binance_bbo_ws",
            )
        )
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="okx",
                symbol="BTCUSDT",
                bid=49990.0,
                ask=50000.0,
                bid_size=3.5,
                ask_size=4.75,
                observed_at_ms=2650,
                received_at_ms=2650,
                source="okx_bbo_ws",
            )
        )
        readiness = runtime.entry_readiness_provider.decide(candidate, 2650)
        assert readiness.allowed
        lease = runtime.entry_readiness_provider.get_lease(
            runtime._candidate_pair_id(candidate)
        )
        assert lease.long_bid_size == pytest.approx(1.25)
        assert lease.long_ask_size == pytest.approx(2.5)
        assert lease.short_bid_size == pytest.approx(3.5)
        assert lease.short_ask_size == pytest.approx(4.75)

        reason, _lease, evidence = runtime._entry_quote_lease_execution_check(
            candidate,
            2700,
        )

        assert reason == "quote_lease_skew_exceeded"
        assert evidence["quote_observation_skew_ms"] == 150
        assert evidence["quote_observation_max_skew_ms"] == 100
        assert evidence["long_bid_size"] == pytest.approx(1.25)
        assert evidence["long_ask_size"] == pytest.approx(2.5)
        assert evidence["short_bid_size"] == pytest.approx(3.5)
        assert evidence["short_ask_size"] == pytest.approx(4.75)

    def test_ws_bbo_execution_and_final_lease_block_candidate_revision_mismatch(
        self,
        config,
        tmp_journal,
    ):
        from dataclasses import replace
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.runtime.mode = "live"
        config.strategy.funding_new_entries_enabled = True
        config.strategy.local_l2_enabled = False
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1500
        config.strategy.entry_final_gate_max_skew_ms = 250
        runtime = LiveRuntime(config)
        runtime.journal = tmp_journal
        candidate = self._candidate()
        oi_revision_id = candidate.entry_open_interest_evidence["candidate_revision_id"]
        candidate.candidate_revision_id = ""
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=50000.0,
                ask=50010.0,
                bid_size=10.0,
                ask_size=10.0,
                observed_at_ms=2500,
                received_at_ms=2500,
                source="binance_bbo_ws",
            )
        )
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="okx",
                symbol="BTCUSDT",
                bid=49990.0,
                ask=50000.0,
                bid_size=10.0,
                ask_size=10.0,
                observed_at_ms=2500,
                received_at_ms=2500,
                source="okx_bbo_ws",
            )
        )
        readiness = runtime.entry_readiness_provider.decide(candidate, 2500)
        assert readiness.allowed
        pair_id = runtime._candidate_pair_id(candidate)
        lease = runtime.entry_readiness_provider.get_lease(pair_id)
        assert lease.candidate_revision_id == ""
        stale_lease = replace(
            lease,
            candidate_revision_id="stale-quote-lease-revision",
        )
        runtime.entry_readiness_provider._leases[pair_id] = stale_lease

        reason, checked_lease, evidence = runtime._entry_quote_lease_execution_check(
            candidate,
            2500,
        )

        assert reason == "quote_lease_candidate_revision_mismatch"
        assert checked_lease is stale_lease
        assert evidence["candidate_revision_id"]
        assert evidence["candidate_revision_id"] != oi_revision_id
        assert evidence["lease_candidate_revision_id"] == "stale-quote-lease-revision"
        assert (
            runtime._final_quote_lease_reason(candidate, stale_lease, 2500)
            == "final_quote_lease_candidate_revision_mismatch"
        )

    def test_ws_bbo_execution_and_final_lease_allow_legacy_empty_revision(
        self,
        config,
        tmp_journal,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.runtime.mode = "live"
        config.strategy.funding_new_entries_enabled = True
        config.strategy.local_l2_enabled = False
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1500
        config.strategy.entry_final_gate_max_skew_ms = 250
        runtime = LiveRuntime(config)
        runtime.journal = tmp_journal
        candidate = self._candidate()
        candidate.candidate_revision_id = ""
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=50000.0,
                ask=50010.0,
                bid_size=10.0,
                ask_size=10.0,
                observed_at_ms=2500,
                received_at_ms=2500,
                source="binance_bbo_ws",
            )
        )
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="okx",
                symbol="BTCUSDT",
                bid=49990.0,
                ask=50000.0,
                bid_size=10.0,
                ask_size=10.0,
                observed_at_ms=2500,
                received_at_ms=2500,
                source="okx_bbo_ws",
            )
        )
        readiness = runtime.entry_readiness_provider.decide(candidate, 2500)
        assert readiness.allowed
        lease = runtime.entry_readiness_provider.get_lease(
            runtime._candidate_pair_id(candidate)
        )
        assert lease.candidate_revision_id == ""

        reason, checked_lease, evidence = runtime._entry_quote_lease_execution_check(
            candidate,
            2500,
        )

        assert reason == ""
        assert checked_lease is lease
        assert evidence["candidate_revision_id"]
        assert evidence["lease_candidate_revision_id"] == ""
        assert runtime._final_quote_lease_reason(candidate, lease, 2500) == ""

    def test_ws_bbo_execution_lease_blocks_insufficient_side_capacity(
        self,
        config,
        tmp_journal,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.runtime.mode = "live"
        config.strategy.funding_new_entries_enabled = True
        config.strategy.local_l2_enabled = False
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1500
        config.strategy.entry_final_gate_max_skew_ms = 250
        runtime = LiveRuntime(config)
        runtime.journal = tmp_journal
        candidate = self._candidate()
        candidate.entry_maker_leg = "long"
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=50000.0,
                ask=50010.0,
                bid_size=0.001,
                ask_size=2.5,
                observed_at_ms=2500,
                received_at_ms=2500,
                source="binance_bbo_ws",
            )
        )
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="okx",
                symbol="BTCUSDT",
                bid=49990.0,
                ask=50000.0,
                bid_size=3.5,
                ask_size=4.75,
                observed_at_ms=2500,
                received_at_ms=2500,
                source="okx_bbo_ws",
            )
        )
        readiness = runtime.entry_readiness_provider.decide(candidate, 2500)
        assert readiness.allowed

        reason, _lease, evidence = runtime._entry_quote_lease_execution_check(
            candidate,
            2500,
        )

        assert reason == "quote_lease_insufficient_bbo_capacity"
        assert evidence["blocker_family"] == "insufficient_capacity"
        assert evidence["quote_lease_required_base_quantity"] > 0.0
        assert evidence["quote_lease_capacity_failed_legs"] == [
            {
                "leg": "long",
                "role": "maker",
                "side": "bid",
                "size_field": "long_bid_size",
                "available_base_quantity": 0.001,
                "required_base_quantity": evidence[
                    "quote_lease_required_base_quantity"
                ],
            }
        ]

    @pytest.mark.asyncio
    async def test_ws_bbo_dispatch_defers_capacity_to_final_l2_orientation(
        self,
        config,
        tmp_journal,
        monkeypatch,
    ):
        config.runtime.mode = "live"
        config.strategy.funding_new_entries_enabled = True
        config.strategy.local_l2_enabled = True
        config.strategy.entry_readiness_provider = "ws_bbo_l2_on_demand"
        config.strategy.entry_quote_lease_ttl_ms = 1500
        config.strategy.max_liquidity_snapshot_age_ms = 1500
        config.strategy.entry_final_gate_max_skew_ms = 250
        config.strategy.min_expected_edge_bps = 0.0
        config.strategy.min_worst_case_edge_bps = 0.0
        config.strategy.max_single_venue_exposure_quote = 10_000.0
        config.strategy.max_symbol_exposure_quote = 10_000.0
        config.strategy.funding_max_venue_pair_exposure_quote = 10_000.0
        config.strategy.funding_max_global_gross_exposure_quote = 20_000.0
        config.strategy.funding_max_settlement_bucket_exposure_quote = 20_000.0
        config.strategy.funding_max_correlation_group_exposure_quote = 20_000.0
        binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
        okx = FakeVenueAdapter(Venue.OKX, _min_notional_quote=10.0)
        binance.available_margin_quote = 1_000.0
        okx.available_margin_quote = 1_000.0
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime._entry_wall_clock_now_ms = lambda: 5000
        runtime.journal = tmp_journal
        runtime.entry_executor = EntrySyncExecutor(adapters=adapters, journal=tmp_journal)
        candidate = self._candidate()
        candidate.first_funding_timestamp_ms = 305_000
        candidate.entry_maker_leg = "long"
        candidate.entry_target_quantity = 1.0
        candidate.entry_max_executable_quantity = 1.0
        candidate.entry_notional_quote = 101.5
        candidate.economics_observed_at_ms = 5000

        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=101.0,
                bid_size=0.001,
                ask_size=1.0,
                observed_at_ms=5000,
                received_at_ms=5000,
                source="binance_bbo_ws",
            )
        )
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="okx",
                symbol="BTCUSDT",
                bid=102.0,
                ask=103.0,
                bid_size=0.001,
                ask_size=1.0,
                observed_at_ms=5000,
                received_at_ms=5000,
                source="okx_bbo_ws",
            )
        )
        long_book = runtime.local_l2_runtime.ensure_book("binance", "BTCUSDT")
        long_book.status = L2BookStatus.HOT
        long_book.bids = [PriceLevel(price=100.0, quantity=0.001)]
        long_book.asks = [PriceLevel(price=101.0, quantity=1.0)]
        long_book.observed_at_ms = 5000
        short_book = runtime.local_l2_runtime.ensure_book("okx", "BTCUSDT")
        short_book.status = L2BookStatus.HOT
        short_book.bids = [PriceLevel(price=102.0, quantity=0.001)]
        short_book.asks = [PriceLevel(price=103.0, quantity=1.0)]
        short_book.observed_at_ms = 5000
        from lightfee.engine.entry_local_l2 import (
            TrackedOpportunity,
            TrackedOpportunityClass,
        )

        tracked = TrackedOpportunity(
            pair_id=runtime._candidate_pair_id(candidate),
            symbol=candidate.symbol,
            long_venue=candidate.long_venue,
            short_venue=candidate.short_venue,
            ranking_edge_bps=candidate.ranking_edge_bps,
            class_=TrackedOpportunityClass.PRIMARY,
        )
        runtime._tracked_primary_pair_ids = {tracked.pair_id}
        runtime.entry_l2_sessions.track_opportunity(tracked, 5000)
        runtime._refresh_entry_l2_session_readiness(5000)
        readiness = runtime.entry_readiness_provider.decide(candidate, 5000)
        assert readiness.allowed

        dispatched = await runtime._dispatch_entry(
            candidate,
            5000,
            price_hint=101.5,
        )

        records = tmp_journal.read_all()
        assert dispatched is True, [
            (
                record["kind"],
                record["payload"].get("reason"),
                record["payload"].get("source"),
            )
            for record in records
        ]
        assert candidate.entry_maker_leg == "short"
        assert okx.last_request is not None
        assert okx.last_request.side == Side.SELL
        assert okx.last_request.price == pytest.approx(103.0)
        assert any(
            record["kind"] == "runtime.entry_passive_maker_orientation_selected"
            and record["payload"]["previous_entry_maker_leg"] == "long"
            and record["payload"]["selected_entry_maker_leg"] == "short"
            for record in records
        )
        assert not any(
            record["kind"] == "runtime.entry_blocked_quote_lease"
            and record["payload"].get("reason")
            == "quote_lease_insufficient_bbo_capacity"
            for record in records
        )

    @pytest.mark.asyncio
    async def test_ws_bbo_provider_dispatch_uses_selected_quote_lease_prices(
        self,
        config,
        tmp_journal,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.runtime.mode = "live"
        config.strategy.funding_new_entries_enabled = True
        config.strategy.local_l2_enabled = False
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1500
        binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
        okx = FakeVenueAdapter(Venue.OKX, _min_notional_quote=10.0)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime._entry_wall_clock_now_ms = lambda: 5000
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
                    bid_size=1.0,
                    ask_size=1.0,
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

    def test_pending_passive_repost_gate_blocks_stale_hedge_ws_bbo(
        self,
        config,
        tmp_journal,
    ):
        config.runtime.mode = "live"
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 100
        runtime = LiveRuntime(config)
        runtime.journal = tmp_journal
        pending = PendingEntry(
            pending_id="pending-bbo-stale",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            target_quantity=0.01,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=5_000,
            maker_leg="long",
        )
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=50000.0,
                ask=50010.0,
                bid_size=1.0,
                ask_size=1.0,
                observed_at_ms=5_000,
                received_at_ms=5_000,
                source="binance_bbo_ws",
            )
        )
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="okx",
                symbol="BTCUSDT",
                bid=49990.0,
                ask=50000.0,
                bid_size=1.0,
                ask_size=1.0,
                observed_at_ms=4_800,
                received_at_ms=4_800,
                source="okx_bbo_ws",
            )
        )

        reason, evidence = runtime._pending_entry_passive_repost_quote_gate(
            pending,
            now_ms=5_000,
        )

        assert reason == "passive_repost_hedge_stale_bbo"
        assert evidence["hedge_age_ms"] == 200
        assert evidence["hedge_stale_after_ms"] == 100

    def test_pending_passive_repost_gate_blocks_skewed_ws_bbo(
        self,
        config,
        tmp_journal,
    ):
        config.runtime.mode = "live"
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1_000
        config.strategy.entry_final_gate_max_skew_ms = 100
        runtime = LiveRuntime(config)
        runtime.journal = tmp_journal
        pending = PendingEntry(
            pending_id="pending-bbo-skew",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            target_quantity=0.01,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=5_000,
            maker_leg="long",
        )
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=50000.0,
                ask=50010.0,
                bid_size=1.0,
                ask_size=1.0,
                observed_at_ms=5_000,
                received_at_ms=5_000,
                source="binance_bbo_ws",
            )
        )
        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="okx",
                symbol="BTCUSDT",
                bid=49990.0,
                ask=50000.0,
                bid_size=1.0,
                ask_size=1.0,
                observed_at_ms=4_850,
                received_at_ms=4_850,
                source="okx_bbo_ws",
            )
        )

        reason, evidence = runtime._pending_entry_passive_repost_quote_gate(
            pending,
            now_ms=5_000,
        )

        assert reason == "passive_repost_quote_skew_exceeded"
        assert evidence["quote_observation_skew_ms"] == 150
        assert evidence["quote_observation_max_skew_ms"] == 100

    @pytest.mark.asyncio
    async def test_ws_bbo_post_only_guard_reprices_crossing_maker_quote_once(
        self,
        config,
        tmp_journal,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.runtime.mode = "live"
        config.strategy.local_l2_enabled = False
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1500
        binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
        okx = FakeVenueAdapter(Venue.OKX, _min_notional_quote=10.0)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime._entry_wall_clock_now_ms = lambda: 5100
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
                    bid_size=10.0,
                    ask_size=10.0,
                    observed_at_ms=5000,
                    received_at_ms=5000,
                    source=f"{venue}_bbo_ws",
                )
            )
        readiness = runtime.entry_readiness_provider.decide(candidate, 5000)
        assert readiness.allowed

        runtime.ws_bbo_cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=49980.0,
                ask=49990.0,
                bid_size=10.0,
                ask_size=10.0,
                observed_at_ms=5100,
                received_at_ms=5100,
                source="binance_bbo_ws",
            )
        )

        dispatched = await runtime._dispatch_entry(
            candidate,
            5100,
            price_hint=12345.0,
        )

        assert dispatched is True
        assert binance.last_request is not None
        assert binance.last_request.post_only is True
        assert binance.last_request.price == 49980.0
        assert not [
            record for record in tmp_journal.read_all()
            if record["kind"] == "runtime.entry_blocked_post_only_bbo"
        ]

    @pytest.mark.asyncio
    async def test_ws_bbo_provider_dispatch_refreshes_expired_quote_lease(
        self,
        config,
        tmp_journal,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.runtime.mode = "live"
        config.strategy.funding_new_entries_enabled = True
        config.runtime.max_market_age_ms = 30_000
        config.strategy.local_l2_enabled = False
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1500
        binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
        okx = FakeVenueAdapter(Venue.OKX, _min_notional_quote=10.0)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime._entry_wall_clock_now_ms = lambda: 7001
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
                    bid_size=10.0,
                    ask_size=10.0,
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
                    bid_size=10.0,
                    ask_size=10.0,
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
    async def test_ws_bbo_provider_rechecks_lease_immediately_before_executor_submit(
        self,
        config,
        tmp_journal,
        monkeypatch,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.runtime.mode = "live"
        config.strategy.funding_new_entries_enabled = True
        config.strategy.local_l2_enabled = False
        config.strategy.entry_readiness_provider = "ws_bbo_quote_lease"
        config.strategy.entry_quote_lease_ttl_ms = 1500
        binance = FakeVenueAdapter(Venue.BINANCE, _min_notional_quote=10.0)
        okx = FakeVenueAdapter(Venue.OKX, _min_notional_quote=10.0)
        adapters = {Venue.BINANCE: binance, Venue.OKX: okx}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        clock = {"now_ms": 5_000}
        runtime._entry_wall_clock_now_ms = lambda: clock["now_ms"]
        runtime.journal = tmp_journal
        runtime.entry_executor = EntrySyncExecutor(
            adapters=adapters,
            journal=tmp_journal,
        )
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
                    bid_size=10.0,
                    ask_size=10.0,
                    observed_at_ms=5_000,
                    received_at_ms=5_000,
                    source=f"{venue}_bbo_ws",
                )
            )
        readiness = runtime.entry_readiness_provider.decide(candidate, 5_000)
        assert readiness.allowed

        def expire_during_pre_submit_window(_ctx):
            clock["now_ms"] = 6_500
            return None

        monkeypatch.setattr(
            runtime.entry_dispatch_runtime,
            "_capture_entry_execution_benchmark_receipt",
            expire_during_pre_submit_window,
        )

        dispatched = await runtime._dispatch_entry(
            candidate,
            5_000,
            price_hint=12345.0,
        )

        assert dispatched is False
        assert binance.last_request is None
        blocked = [
            record["payload"]
            for record in tmp_journal.read_all()
            if record["kind"] == "entry.dispatch_viability_blocked"
            and record["payload"].get("source")
            == "executor_submit_quote_lease"
        ]
        assert blocked[-1]["reason"] in {
            "expired_quote_lease",
            "expired_final_quote_lease",
        }
        assert blocked[-1]["source"] == "executor_submit_quote_lease"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("legacy_provider", (None, "local_l2"))
    async def test_post_only_guard_uses_effective_composed_bbo_for_defaulted_and_legacy_provider(
        self,
        config,
        tmp_journal,
        legacy_provider,
    ):
        from lightfee.marketdata.ws_bbo import TopBookQuote

        config.runtime.mode = "live"
        config.strategy.local_l2_enabled = True
        config.strategy.entry_quote_lease_ttl_ms = 1500
        if legacy_provider is not None:
            config.strategy.entry_readiness_provider = legacy_provider
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
            now_ms=5000,
        )

        assert ok is True
        assert reason == ""
        assert payload["source"] == "ws_bbo_quote_lease"
        assert payload["provider"] == "ws_bbo_quote_lease"
        assert payload["domain"] == "ws_bbo_cache"

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

        config.strategy.local_l2_enabled = False
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
    async def test_crossing_bbo_reprices_post_only_maker_submit(self, config, tmp_journal):
        from lightfee.marketdata.ws_bbo import TopBookQuote

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

        assert await runtime._dispatch_entry(self._candidate(), 5000, price_hint=50010.0) is True

        assert binance.last_request is not None
        assert binance.last_request.price == 50000.0
        assert not [
            record for record in tmp_journal.read_all()
            if record["kind"] == "runtime.entry_blocked_post_only_bbo"
        ]

    @pytest.mark.asyncio
    async def test_dispatch_entry_uses_planner_route(self, config, tmp_journal):
        """Entry route comes from planner, not hardcoded STANDARD_DUAL_TAKER."""
        from lightfee.marketdata.ws_bbo import TopBookQuote

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
            first_funding_timestamp_ms=605_000,
            funding_timestamp_ms=605_000,
        )
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
            first_funding_timestamp_ms=605_000,
            funding_timestamp_ms=605_000,
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
    async def test_dispatch_entry_uses_single_common_quantity_plan_for_home_okx_bybit(
        self, config, tmp_journal,
    ):
        config.strategy.maker_initial_slice_ratio = 1.0
        okx = FakeVenueAdapter(Venue.OKX, okx_base_quantity_step=100.0)
        bybit = FakeVenueAdapter(Venue.BYBIT)
        adapters = {Venue.OKX: okx, Venue.BYBIT: bybit}
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
            long_venue="okx",
            short_venue="bybit",
            symbol="HOMEUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=8.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=8.0,
            transfer_bias_bps=0.0,
            opportunity_type="funding_arb",
            blocked=False,
            entry_notional_quote=720.5692497072687,
            first_funding_timestamp_ms=605_000,
            funding_timestamp_ms=605_000,
        )

        for venue in ("okx", "bybit"):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="HOMEUSDT",
                    bid=0.99,
                    ask=1.01,
                    observed_at_ms=5000,
                    received_at_ms=5000,
                    source=f"{venue}_bbo_ws",
                )
            )

        dispatched = await runtime._dispatch_entry(candidate, 5000, price_hint=1.0)

        assert dispatched is True
        assert executor.ctx is not None
        assert executor.ctx.long_quantity == pytest.approx(700.0)
        assert executor.ctx.short_quantity == pytest.approx(700.0)
        records = runtime.journal.read_all()
        selected = [
            r for r in records
            if r["kind"] == "execution.entry_selected"
        ][-1]
        assert selected["payload"]["quantity"] == pytest.approx(700.0)
        quantity_plan = [
            r for r in records
            if r["kind"] == "execution.entry_quantity_plan"
        ][-1]
        payload = quantity_plan["payload"]
        assert payload["symbol"] == "HOMEUSDT"
        assert payload["raw_quantity"] == pytest.approx(720.5692497072687)
        assert payload["common_quantity"] == pytest.approx(700.0)
        assert payload["full_target_quantity"] == pytest.approx(700.0)
        assert payload["initial_maker_target_quantity"] == pytest.approx(700.0)
        assert payload["effective_quantity"] == pytest.approx(700.0)
        assert payload["quantity_plan_reason"] == "exchange_step_rounding"
        assert payload["quantity_contract_status"] == "hedgeable_adjusted"
        assert payload["unhedgeable_residual_quantity"] == pytest.approx(
            20.5692497072687
        )
        assert payload["venue_quantity_steps"]["okx"] == pytest.approx(100.0)
        assert payload["venue_quantity_steps"]["bybit"] == pytest.approx(0.001)
        assert payload["venue_quantity_metadata"]["okx"]["quantity_step"] == pytest.approx(100.0)

    @pytest.mark.asyncio
    async def test_entry_symbol_rule_converts_gate_contract_grid_to_base_units(
        self,
        config,
        monkeypatch,
    ):
        from lightfee.venues.symbol_rules import SymbolRule

        gate = FakeVenueAdapter(Venue.GATE)
        gate._transport = SimpleNamespace(
            mode="live",
            _venue_symbol=lambda symbol: symbol.replace("USDT", "_USDT"),
        )
        runtime = LiveRuntime(config, venue_adapters={Venue.GATE: gate})

        class RulesCache:
            async def get(self, transport, venue, venue_symbol):
                assert transport is gate._transport
                assert venue == Venue.GATE
                assert venue_symbol == "TINY_USDT"
                return SymbolRule(
                    tick_size=0.0001,
                    qty_step=1.0,
                    min_qty=2.0,
                    min_notional=7.0,
                    contract_multiplier=0.01,
                    rule_source="gate_contracts",
                )

        monkeypatch.setattr(
            "lightfee.venues.symbol_rules.get_symbol_rules_cache",
            lambda: RulesCache(),
        )

        evidence = await runtime.entry_dispatch_runtime._entry_symbol_order_metadata(
            Venue.GATE,
            "TINYUSDT",
        )

        assert evidence["missing_fields"] == []
        assert evidence["source"] == "symbol_rule:gate_contracts"
        assert evidence["quantity_step"] == pytest.approx(0.01)
        assert evidence["min_quantity"] == pytest.approx(0.02)
        assert evidence["min_notional"] == pytest.approx(7.0)
        assert evidence["contract_step"] == pytest.approx(1.0)
        assert evidence["contract_multiplier"] == pytest.approx(0.01)

    @pytest.mark.asyncio
    async def test_symbol_rules_cache_uses_bitget_contract_metadata(self):
        from lightfee.venues.specs import bitget_spec
        from lightfee.venues.symbol_rules import SymbolRulesCache

        async def public_get(path, *, params=None):
            assert path == "/api/v2/mix/market/contracts"
            assert params == {"productType": "USDT-FUTURES"}
            return {
                "data": [
                    {
                        "symbol": "TINYUSDT",
                        "sizeMultiplier": "0.01",
                        "minTradeNum": "0.02",
                        "minTradeUSDT": "7",
                        "pricePlace": "4",
                        "priceEndStep": "1",
                    }
                ]
            }

        transport = SimpleNamespace(
            _spec=bitget_spec(),
            # Startup's compatibility catalog used to retain only these two
            # fields. Entry rules must refresh the full row, not mistake this
            # partial cache for complete min-notional/tick evidence.
            _symbol_metadata={
                "TINYUSDT": {
                    "sizeMultiplier": "0.001",
                    "minTradeNum": "0.001",
                }
            },
            _public_get=public_get,
        )

        rule = await SymbolRulesCache().get(
            transport,
            Venue.BITGET,
            "TINYUSDT",
        )

        assert rule.rule_source == "bitget_contracts"
        assert rule.qty_step == pytest.approx(0.01)
        assert rule.min_qty == pytest.approx(0.02)
        assert rule.min_notional == pytest.approx(7.0)
        assert rule.tick_size == pytest.approx(0.0001)

    @pytest.mark.asyncio
    async def test_symbol_rules_cache_uses_hyperliquid_size_decimals(self):
        from lightfee.venues.specs import hyperliquid_spec
        from lightfee.venues.symbol_rules import SymbolRulesCache

        async def resolve_asset_meta(asset_name):
            assert asset_name == "BTC"
            return {
                "asset_index": 0,
                "sz_decimals": 5,
                "price_decimals": 1,
            }

        transport = SimpleNamespace(
            _spec=hyperliquid_spec(),
            _hl_resolve_asset_meta=resolve_asset_meta,
        )

        rule = await SymbolRulesCache().get(
            transport,
            Venue.HYPERLIQUID,
            "BTC",
        )

        assert rule.rule_source == "hyperliquid_meta"
        assert rule.qty_step == pytest.approx(0.00001)
        assert rule.min_qty == pytest.approx(0.00001)
        assert rule.min_notional == pytest.approx(10.0)

    @pytest.mark.asyncio
    async def test_live_entry_invalidates_spec_fallback_and_blocks_first_leg(
        self,
        config,
        monkeypatch,
    ):
        from lightfee.venues.symbol_rules import SymbolRule

        bybit = FakeVenueAdapter(Venue.BYBIT)
        bybit._transport = SimpleNamespace(
            mode="live",
            _venue_symbol=lambda symbol: symbol,
        )
        runtime = LiveRuntime(config, venue_adapters={Venue.BYBIT: bybit})

        class RulesCache:
            invalidated = []

            async def get(self, _transport, _venue, _venue_symbol):
                return SymbolRule(
                    tick_size=0.01,
                    qty_step=0.001,
                    min_qty=0.001,
                    min_notional=5.0,
                    rule_source="spec_fallback",
                )

            def invalidate(self, venue, venue_symbol):
                self.invalidated.append((venue, venue_symbol))

        rules_cache = RulesCache()
        monkeypatch.setattr(
            "lightfee.venues.symbol_rules.get_symbol_rules_cache",
            lambda: rules_cache,
        )

        evidence = await runtime.entry_dispatch_runtime._entry_symbol_order_metadata(
            Venue.BYBIT,
            "NEWUSDT",
        )

        assert evidence["source"] == "dynamic_symbol_rule_unavailable"
        assert evidence["missing_fields"] == ["dynamic_symbol_rule"]
        assert rules_cache.invalidated == [(Venue.BYBIT, "NEWUSDT")]

    @pytest.mark.asyncio
    async def test_dispatch_entry_dynamic_hedge_minimum_blocks_before_maker_fill(
        self,
        config,
        tmp_journal,
        monkeypatch,
    ):
        from lightfee.sidecar.snapshot import CandidateInput
        from lightfee.venues.symbol_rules import SymbolRule

        config.strategy.min_entry_leg_notional_quote = 1.0
        gate = FakeVenueAdapter(Venue.GATE)
        bybit = FakeVenueAdapter(Venue.BYBIT)
        gate._transport = SimpleNamespace(
            mode="live",
            _venue_symbol=lambda symbol: symbol.replace("USDT", "_USDT"),
        )
        bybit._transport = SimpleNamespace(
            mode="live",
            _venue_symbol=lambda symbol: symbol,
        )
        runtime = LiveRuntime(
            config,
            venue_adapters={Venue.GATE: gate, Venue.BYBIT: bybit},
        )
        runtime.journal = tmp_journal

        class RulesCache:
            async def get(self, _transport, venue, _venue_symbol):
                if venue == Venue.GATE:
                    return SymbolRule(
                        tick_size=0.0001,
                        qty_step=1.0,
                        min_qty=1.0,
                        min_notional=1.0,
                        contract_multiplier=0.01,
                        rule_source="gate_contracts",
                    )
                return SymbolRule(
                    tick_size=0.01,
                    qty_step=0.001,
                    min_qty=0.001,
                    min_notional=100.0,
                    rule_source="instruments-info",
                )

        monkeypatch.setattr(
            "lightfee.venues.symbol_rules.get_symbol_rules_cache",
            lambda: RulesCache(),
        )

        class CapturingExecutor:
            called = False

            async def execute(self, _ctx):
                self.called = True
                raise AssertionError("dynamic hedge minimum must block first leg")

        executor = CapturingExecutor()
        runtime.entry_executor = executor
        candidate = CandidateInput(
            long_venue="gate",
            short_venue="bybit",
            symbol="TINYUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=8.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=8.0,
            opportunity_type="funding_arb",
            blocked=False,
            entry_notional_quote=50.0,
            first_funding_timestamp_ms=605_000,
            funding_timestamp_ms=605_000,
        )

        assert await runtime._dispatch_entry(candidate, 5_000, price_hint=1.0) is False
        assert executor.called is False
        blocked = [
            record["payload"]
            for record in tmp_journal.read_all()
            if record["kind"] == "entry.dispatch_viability_blocked"
            and record["payload"].get("source") == "entry_pair_minimum"
        ]
        assert blocked[-1]["reason"] == "entry_pair_minimum_not_met"
        short_failure = next(
            failure
            for failure in blocked[-1]["pair_minimum_failures"]
            if failure["leg"] == "short"
        )
        assert short_failure["min_notional_quote"] == pytest.approx(100.0)

    @pytest.mark.asyncio
    async def test_passive_entry_keeps_planned_quantity_without_canary_resize(
        self,
        config,
        tmp_journal,
        monkeypatch,
    ):
        from lightfee.engine.entry_readiness import QuoteLease

        config.runtime.mode = "live"
        config.strategy.funding_new_entries_enabled = True
        config.strategy.local_l2_enabled = True
        config.strategy.entry_local_l2_book_stale_after_ms = 1_000
        config.strategy.max_liquidity_snapshot_age_ms = 1_000
        config.strategy.min_entry_leg_notional_quote = 1.0
        config.strategy.maker_initial_slice_ratio = 0.2
        config.strategy.max_single_venue_exposure_quote = 10_000.0
        config.strategy.max_symbol_exposure_quote = 10_000.0
        config.strategy.funding_max_venue_pair_exposure_quote = 10_000.0
        config.strategy.funding_max_global_gross_exposure_quote = 20_000.0
        config.strategy.funding_max_settlement_bucket_exposure_quote = 20_000.0
        config.strategy.funding_max_correlation_group_exposure_quote = 20_000.0
        quantity_metadata = {
            "quantity_step": 0.1,
            "min_quantity": 0.1,
            "min_notional": 1.0,
        }
        binance = FakeVenueAdapter(
            Venue.BINANCE,
            passive_metadata_payload=quantity_metadata,
        )
        bybit = FakeVenueAdapter(
            Venue.BYBIT,
            passive_metadata_payload=quantity_metadata,
        )
        runtime = LiveRuntime(
            config,
            venue_adapters={Venue.BINANCE: binance, Venue.BYBIT: bybit},
        )
        runtime.journal = tmp_journal
        self._install_hot_book(
            runtime,
            "binance",
            "BTCUSDT",
            bid=99.0,
            ask=100.0,
            observed_at_ms=5_000,
        )
        self._install_hot_book(
            runtime,
            "bybit",
            "BTCUSDT",
            bid=101.0,
            ask=102.0,
            observed_at_ms=5_000,
        )
        for venue, bid, ask in (
            ("binance", 99.0, 100.0),
            ("bybit", 101.0, 102.0),
        ):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="BTCUSDT",
                    bid=bid,
                    ask=ask,
                    bid_size=10.0,
                    ask_size=10.0,
                    observed_at_ms=5_000,
                    received_at_ms=5_000,
                    source="test_ws_bbo",
                )
            )
        candidate = self._binance_bybit_candidate()
        candidate.entry_target_quantity = 1.0
        candidate.entry_max_executable_quantity = 1.0
        candidate.entry_notional_quote = 100.5
        candidate.economics_observed_at_ms = 5_000
        initial_lease = QuoteLease(
            pair_id="BTCUSDT:binance:bybit",
            symbol="BTCUSDT",
            long_venue="binance",
            short_venue="bybit",
            long_bid=99.0,
            long_ask=100.0,
            short_bid=101.0,
            short_ask=102.0,
            long_observed_at_ms=5_000,
            short_observed_at_ms=5_000,
            created_at_ms=5_000,
            expires_at_ms=6_000,
            long_buy_vwap=100.0,
            short_sell_vwap=101.0,
            long_l2_capacity_quantity=10.0,
            short_l2_capacity_quantity=10.0,
            l2_vwap_quantity=1.0,
            l2_vwap_complete=True,
        )
        dispatch = runtime.entry_dispatch_runtime
        monkeypatch.setattr(dispatch, "_entry_initial_gate_blocked", lambda *_: False)
        monkeypatch.setattr(
            dispatch,
            "_entry_price_resolution",
            lambda *_: (100.5, 99.0, 101.0, initial_lease),
        )

        async def fixed_quantity_resolution(**_kwargs):
            return (
                1.0,
                1.0,
                0.0,
                0.1,
                0.1,
                dict(quantity_metadata),
                dict(quantity_metadata),
            )

        monkeypatch.setattr(
            dispatch,
            "_resolve_entry_quantity_steps",
            fixed_quantity_resolution,
        )

        async def leverage_inspection(**_kwargs):
            return True, {}

        async def margin_resolution(**kwargs):
            return kwargs["current_quantity"], False

        async def leverage_prepare(**_kwargs):
            return True, {}

        async def precheck(**_kwargs):
            return True

        monkeypatch.setattr(
            dispatch,
            "_inspect_live_entry_leverage_for_candidate",
            leverage_inspection,
        )
        monkeypatch.setattr(
            dispatch,
            "_resolve_live_margin_quantity",
            margin_resolution,
        )
        monkeypatch.setattr(
            dispatch,
            "_prepare_live_entry_leverage_for_candidate",
            leverage_prepare,
        )
        monkeypatch.setattr(dispatch, "_precheck_entry_admission", precheck)
        monkeypatch.setattr(
            dispatch,
            "_entry_quote_lease_execution_check",
            lambda *_, **__: ("", initial_lease, {}),
        )
        monkeypatch.setattr(dispatch, "_final_quote_lease_reason", lambda *_: "")

        revalidations: list[tuple[str, float, float]] = []

        def revalidate(*, quote_lease, required_base_quantity, source, **_kwargs):
            revalidations.append(
                (
                    source,
                    float(required_base_quantity),
                    float(getattr(quote_lease, "l2_vwap_quantity", 0.0) or 0.0),
                )
            )
            return True

        monkeypatch.setattr(dispatch, "_revalidate_final_entry_economics", revalidate)

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
        runtime._entry_wall_clock_now_ms = lambda: 5_000

        dispatched = await runtime._dispatch_entry(candidate, 5_000, price_hint=100.5)
        assert dispatched is True, [
            (
                record["kind"],
                record["payload"].get("reason"),
                record["payload"].get("source"),
            )
            for record in tmp_journal.read_all()
        ]
        assert all("canary" not in source for source, _, _ in revalidations)
        assert executor.ctx is not None
        assert candidate.entry_target_quantity == pytest.approx(1.0)
        assert candidate.entry_notional_quote == pytest.approx(100.0)
        assert candidate.entry_max_leg_notional_quote == pytest.approx(101.0)
        assert candidate.expected_profit_quote == pytest.approx(
            candidate.entry_notional_quote * candidate.expected_edge_bps / 10_000.0
        )
        assert candidate.worst_case_profit_quote == pytest.approx(
            candidate.entry_notional_quote
            * candidate.worst_case_edge_bps
            / 10_000.0
        )

    @pytest.mark.asyncio
    async def test_dispatch_entry_allows_hedgeable_plan_when_fill_increment_uses_small_fill_buffer(
        self, config, tmp_journal,
    ):
        config.strategy.maker_initial_slice_ratio = 1.0
        config.strategy.min_entry_leg_notional_quote = 1.0
        config.strategy.pending_entry_pre_submit_hedgeable_fill_guard_enabled = True
        gate = FakeVenueAdapter(
            Venue.GATE,
            passive_metadata_payload={
                "min_notional": 1.0,
                "min_quantity": 1.0,
                "quantity_step": 0.1,
                "contract_step": 1.0,
                "contract_multiplier": 0.01,
                "quantity_units": "gate_contracts_to_base",
            },
        )
        bybit = FakeVenueAdapter(
            Venue.BYBIT,
            passive_metadata_payload={
                "min_notional": 1.0,
                "min_quantity": 0.001,
                "quantity_step": 0.001,
            },
        )
        adapters = {Venue.GATE: gate, Venue.BYBIT: bybit}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal

        class CapturingExecutor:
            called = False

            async def execute(self, ctx):
                self.called = True
                return EntryExecutionResult(
                    route=ExecutionRoute.PASSIVE_INCREMENTAL,
                    state=EntryState.COMPLETED,
                )

        executor = CapturingExecutor()
        runtime.entry_executor = executor

        from lightfee.sidecar.snapshot import CandidateInput

        candidate = CandidateInput(
            long_venue="gate",
            short_venue="bybit",
            symbol="TINYUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=8.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=8.0,
            transfer_bias_bps=0.0,
            opportunity_type="funding_arb",
            blocked=False,
            entry_notional_quote=50.0,
            first_funding_timestamp_ms=605_000,
            funding_timestamp_ms=605_000,
        )

        for venue in ("gate", "bybit"):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="TINYUSDT",
                    bid=0.99,
                    ask=1.01,
                    observed_at_ms=5000,
                    received_at_ms=5000,
                    source=f"{venue}_bbo_ws",
                )
            )

        dispatched = await runtime._dispatch_entry(candidate, 5000, price_hint=1.0)

        assert dispatched is True
        assert executor.called is True
        records = runtime.journal.read_all()
        blockers = [
            r["payload"]
            for r in records
            if r["kind"] == "runtime.entry_blocked_pre_submit_hedgeability"
        ]
        assert blockers == []
        advisory = [
            r["payload"]
            for r in records
            if r["kind"] == "runtime.entry_pre_submit_hedgeability_advisory"
        ][-1]
        assert advisory["reason"] == "maker_fill_increment_below_hedge_min_chunk"
        assert advisory["maker_fill_increment_base"] == pytest.approx(0.01)
        assert advisory["min_hedgeable_chunk"] == pytest.approx(1.0)
        assert advisory["small_fill_buffer_required"] is True
        assert advisory["planned_clip_hedgeable"] is True
        assert advisory["cooldown_scope"] == "symbol"

    @pytest.mark.asyncio
    async def test_dispatch_entry_fail_closes_gate_when_quantity_metadata_is_missing(
        self, config, tmp_journal,
    ):
        config.strategy.maker_initial_slice_ratio = 1.0
        config.strategy.min_entry_leg_notional_quote = 1.0
        config.strategy.pending_entry_pre_submit_hedgeable_fill_guard_enabled = True
        gate = FakeVenueAdapter(
            Venue.GATE,
            passive_metadata_payload={},
        )
        bybit = FakeVenueAdapter(
            Venue.BYBIT,
            passive_metadata_payload={
                "min_notional": 1.0,
                "min_quantity": 0.001,
                "quantity_step": 0.001,
            },
        )
        adapters = {Venue.GATE: gate, Venue.BYBIT: bybit}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal

        class CapturingExecutor:
            called = False

            async def execute(self, ctx):
                self.called = True
                return EntryExecutionResult(
                    route=ExecutionRoute.PASSIVE_INCREMENTAL,
                    state=EntryState.COMPLETED,
                )

        executor = CapturingExecutor()
        runtime.entry_executor = executor

        from lightfee.sidecar.snapshot import CandidateInput

        candidate = CandidateInput(
            long_venue="gate",
            short_venue="bybit",
            symbol="TINYUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=8.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=8.0,
            transfer_bias_bps=0.0,
            opportunity_type="funding_arb",
            blocked=False,
            entry_notional_quote=50.0,
            first_funding_timestamp_ms=605_000,
            funding_timestamp_ms=605_000,
        )

        dispatched = await runtime._dispatch_entry(candidate, 5000, price_hint=1.0)

        assert dispatched is False
        assert executor.called is False
        records = runtime.journal.read_all()
        blockers = [
            r["payload"]
            for r in records
            if r["kind"] == "runtime.entry_blocked_pre_submit_hedgeability"
        ]
        assert blockers == []
        skipped = [
            r["payload"]
            for r in records
            if r["kind"] == "runtime.entry_skipped_quantity_metadata_missing"
        ][-1]
        assert skipped["reason"] == "quantity_metadata_missing"
        assert skipped["missing_venues"] == ["gate"]
        assert skipped["missing_fields"]["gate"] == ["metadata"]

    @pytest.mark.asyncio
    async def test_dispatch_entry_guard_disabled_allows_passive_submit_but_logs_unit_evidence(
        self, config, tmp_journal,
    ):
        config.strategy.maker_initial_slice_ratio = 1.0
        config.strategy.min_entry_leg_notional_quote = 1.0
        config.strategy.pending_entry_pre_submit_hedgeable_fill_guard_enabled = False
        gate = FakeVenueAdapter(
            Venue.GATE,
            passive_metadata_payload={
                "min_notional": 1.0,
                "min_quantity": 1.0,
                "quantity_step": 0.1,
                "contract_step": 1.0,
                "contract_multiplier": 0.01,
                "quantity_units": "gate_contracts_to_base",
            },
        )
        bybit = FakeVenueAdapter(
            Venue.BYBIT,
            passive_metadata_payload={
                "min_notional": 1.0,
                "min_quantity": 0.001,
                "quantity_step": 0.001,
            },
        )
        adapters = {Venue.GATE: gate, Venue.BYBIT: bybit}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal

        class CapturingExecutor:
            called = False

            async def execute(self, ctx):
                self.called = True
                return EntryExecutionResult(
                    route=ExecutionRoute.PASSIVE_INCREMENTAL,
                    state=EntryState.COMPLETED,
                )

        executor = CapturingExecutor()
        runtime.entry_executor = executor

        from lightfee.sidecar.snapshot import CandidateInput

        candidate = CandidateInput(
            long_venue="gate",
            short_venue="bybit",
            symbol="TINYUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=8.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=8.0,
            transfer_bias_bps=0.0,
            opportunity_type="funding_arb",
            blocked=False,
            entry_notional_quote=50.0,
            first_funding_timestamp_ms=605_000,
            funding_timestamp_ms=605_000,
        )

        for venue in ("gate", "bybit"):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="TINYUSDT",
                    bid=0.99,
                    ask=1.01,
                    observed_at_ms=5000,
                    received_at_ms=5000,
                    source=f"{venue}_bbo_ws",
                )
            )

        dispatched = await runtime._dispatch_entry(candidate, 5000, price_hint=1.0)

        assert dispatched is True
        assert executor.called is True
        records = runtime.journal.read_all()
        advisory = [
            r["payload"]
            for r in records
            if r["kind"] == "runtime.entry_pre_submit_hedgeability_advisory"
        ][-1]
        assert advisory["reason"] == "maker_fill_increment_below_hedge_min_chunk"
        assert advisory["guard_enabled"] is False
        assert advisory["maker_fill_increment_base"] == pytest.approx(0.01)

    @pytest.mark.asyncio
    async def test_dispatch_entry_contract_adjusts_home_1856_to_hedgeable_1800(
        self, config, tmp_journal,
    ):
        config.strategy.maker_initial_slice_ratio = 1.0
        okx = FakeVenueAdapter(Venue.OKX, okx_base_quantity_step=100.0)
        bybit = FakeVenueAdapter(Venue.BYBIT)
        runtime = LiveRuntime(
            config,
            venue_adapters={Venue.OKX: okx, Venue.BYBIT: bybit},
        )
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
            long_venue="okx",
            short_venue="bybit",
            symbol="HOMEUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=8.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=8.0,
            transfer_bias_bps=0.0,
            opportunity_type="funding_arb",
            blocked=False,
            entry_notional_quote=1856.0,
            first_funding_timestamp_ms=605_000,
            funding_timestamp_ms=605_000,
        )

        for venue in ("okx", "bybit"):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="HOMEUSDT",
                    bid=0.99,
                    ask=1.01,
                    observed_at_ms=5000,
                    received_at_ms=5000,
                    source=f"{venue}_bbo_ws",
                )
            )

        dispatched = await runtime._dispatch_entry(candidate, 5000, price_hint=1.0)

        assert dispatched is True
        assert executor.ctx is not None
        assert executor.ctx.long_quantity == pytest.approx(1800.0)
        assert executor.ctx.short_quantity == pytest.approx(1800.0)
        records = runtime.journal.read_all()
        assert "order.passive_submitted" not in [r["kind"] for r in records]
        payload = [
            r["payload"]
            for r in records
            if r["kind"] == "execution.entry_quantity_plan"
        ][-1]
        assert payload["raw_quantity"] == pytest.approx(1856.0)
        assert payload["common_quantity"] == pytest.approx(1800.0)
        assert payload["full_target_quantity"] == pytest.approx(1800.0)
        assert payload["effective_quantity"] == pytest.approx(1800.0)
        assert payload["quantity_contract_status"] == "hedgeable_adjusted"
        assert payload["unhedgeable_residual_quantity"] == pytest.approx(56.0)

    @pytest.mark.asyncio
    async def test_dispatch_entry_skips_when_non_okx_quantity_metadata_missing(
        self, config, tmp_journal,
    ):
        okx = FakeVenueAdapter(Venue.OKX, okx_base_quantity_step=100.0)
        bybit = FakeVenueAdapter(Venue.BYBIT, passive_metadata_payload={})
        adapters = {Venue.OKX: okx, Venue.BYBIT: bybit}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal

        class CapturingExecutor:
            called = False

            async def execute(self, ctx):
                self.called = True
                return EntryExecutionResult(
                    route=ExecutionRoute.PASSIVE_INCREMENTAL,
                    state=EntryState.COMPLETED,
                )

        executor = CapturingExecutor()
        runtime.entry_executor = executor

        from lightfee.sidecar.snapshot import CandidateInput

        candidate = CandidateInput(
            long_venue="okx",
            short_venue="bybit",
            symbol="HOMEUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=8.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=8.0,
            transfer_bias_bps=0.0,
            opportunity_type="funding_arb",
            blocked=False,
            entry_notional_quote=720.5692497072687,
            first_funding_timestamp_ms=605_000,
            funding_timestamp_ms=605_000,
        )

        dispatched = await runtime._dispatch_entry(candidate, 5000, price_hint=1.0)

        assert dispatched is False
        assert executor.called is False
        skipped = [
            r for r in runtime.journal.read_all()
            if r["kind"] == "runtime.entry_skipped_quantity_metadata_missing"
        ][-1]
        assert skipped["payload"]["symbol"] == "HOMEUSDT"
        assert skipped["payload"]["missing_venues"] == ["bybit"]

    @pytest.mark.asyncio
    async def test_dispatch_entry_skips_when_non_okx_min_notional_metadata_missing(
        self, config, tmp_journal,
    ):
        okx = FakeVenueAdapter(Venue.OKX, okx_base_quantity_step=100.0)
        bybit = FakeVenueAdapter(
            Venue.BYBIT,
            passive_metadata_payload={"quantity_step": 0.001, "min_quantity": 0.001},
        )
        adapters = {Venue.OKX: okx, Venue.BYBIT: bybit}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal

        class CapturingExecutor:
            called = False

            async def execute(self, ctx):
                self.called = True
                return EntryExecutionResult(
                    route=ExecutionRoute.PASSIVE_INCREMENTAL,
                    state=EntryState.COMPLETED,
                )

        executor = CapturingExecutor()
        runtime.entry_executor = executor

        from lightfee.sidecar.snapshot import CandidateInput

        candidate = CandidateInput(
            long_venue="okx",
            short_venue="bybit",
            symbol="HOMEUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=8.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=8.0,
            transfer_bias_bps=0.0,
            opportunity_type="funding_arb",
            blocked=False,
            entry_notional_quote=720.5692497072687,
            first_funding_timestamp_ms=605_000,
            funding_timestamp_ms=605_000,
        )

        dispatched = await runtime._dispatch_entry(candidate, 5000, price_hint=1.0)

        assert dispatched is False
        assert executor.called is False
        skipped = [
            r for r in runtime.journal.read_all()
            if r["kind"] == "runtime.entry_skipped_quantity_metadata_missing"
        ][-1]
        assert skipped["payload"]["symbol"] == "HOMEUSDT"
        assert skipped["payload"]["missing_venues"] == ["bybit"]
        assert skipped["payload"]["missing_fields"]["bybit"] == ["min_notional"]

    @pytest.mark.asyncio
    async def test_dispatch_entry_skips_when_non_okx_min_quantity_metadata_zero(
        self, config, tmp_journal,
    ):
        okx = FakeVenueAdapter(Venue.OKX, okx_base_quantity_step=100.0)
        bybit = FakeVenueAdapter(
            Venue.BYBIT,
            passive_metadata_payload={
                "quantity_step": 0.001,
                "min_quantity": 0.0,
                "min_notional": 5.0,
            },
        )
        adapters = {Venue.OKX: okx, Venue.BYBIT: bybit}
        runtime = LiveRuntime(config, venue_adapters=adapters)
        runtime.journal = tmp_journal

        class CapturingExecutor:
            called = False

            async def execute(self, ctx):
                self.called = True
                return EntryExecutionResult(
                    route=ExecutionRoute.PASSIVE_INCREMENTAL,
                    state=EntryState.COMPLETED,
                )

        executor = CapturingExecutor()
        runtime.entry_executor = executor

        from lightfee.sidecar.snapshot import CandidateInput

        candidate = CandidateInput(
            long_venue="okx",
            short_venue="bybit",
            symbol="HOMEUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=8.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=8.0,
            transfer_bias_bps=0.0,
            opportunity_type="funding_arb",
            blocked=False,
            entry_notional_quote=720.5692497072687,
            first_funding_timestamp_ms=605_000,
            funding_timestamp_ms=605_000,
        )

        dispatched = await runtime._dispatch_entry(candidate, 5000, price_hint=1.0)

        assert dispatched is False
        assert executor.called is False
        skipped = [
            r for r in runtime.journal.read_all()
            if r["kind"] == "runtime.entry_skipped_quantity_metadata_missing"
        ][-1]
        assert skipped["payload"]["symbol"] == "HOMEUSDT"
        assert skipped["payload"]["missing_venues"] == ["bybit"]
        assert skipped["payload"]["missing_fields"]["bybit"] == ["min_quantity"]

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
                    pending_entry=PendingEntry(
                        pending_id=ctx.entry_id,
                        symbol=ctx.symbol,
                        long_venue=ctx.long_venue,
                        short_venue=ctx.short_venue,
                        target_quantity=ctx.long_quantity,
                        long_side=Side.BUY,
                        short_side=Side.SELL,
                        created_at_ms=ctx.created_at_ms,
                    ),
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
            blocked_reasons=[],
            entry_notional_quote=500.0,
            funding_timestamp_ms=first_funding_ms,
            first_funding_timestamp_ms=first_funding_ms,
            long_funding_timestamp_ms=first_funding_ms,
            short_funding_timestamp_ms=second_funding_ms,
            second_funding_timestamp_ms=second_funding_ms,
            first_funding_leg="long",
            entry_maker_leg="long",
            exit_maker_leg="short",
            entry_cross_bps=1.25,
            fee_bps=2.1,
            entry_slippage_bps=0.75,
            transfer_state_at_entry="ok",
            entry_liquidity_source_at_entry="local_l2",
            long_volume_24h_quote=12_000_000.0,
            short_volume_24h_quote=15_000_000.0,
            long_open_interest_quote_at_entry=8_000_000.0,
            short_open_interest_quote_at_entry=9_000_000.0,
            long_entry_vwap=50000.5,
            short_entry_vwap=50010.5,
            entry_capacity_constrained=True,
            entry_target_quantity=0.16,
            long_max_executable_quantity=0.18,
            short_max_executable_quantity=0.16,
            entry_max_executable_quantity=0.16,
            entry_depth_shortfall_quantity=0.04,
            entry_max_executable_notional_quote=8000.0,
            entry_depth_capped_at_entry=True,
            advisories=["thin_book"],
        )

        now_ms = 1780163908797
        for venue in ("aster", "bybit"):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="MAGMAUSDT",
                    bid=49990.0,
                    ask=50010.0,
                    observed_at_ms=now_ms,
                    received_at_ms=now_ms,
                    source=f"{venue}_bbo_ws",
                )
            )

        dispatched = await runtime._dispatch_entry(candidate, now_ms, price_hint=50000.0)

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
        assert executor.ctx.worst_case_edge_bps_entry == pytest.approx(2.0)
        assert executor.ctx.entry_maker_leg == "long"
        assert executor.ctx.exit_maker_leg == "short"
        assert executor.ctx.entry_cross_bps_entry == pytest.approx(1.25)
        assert executor.ctx.fee_bps_entry == pytest.approx(2.1)
        assert executor.ctx.entry_slippage_bps_entry == pytest.approx(0.75)
        assert executor.ctx.transfer_bias_bps_entry == pytest.approx(0.0)
        assert executor.ctx.transfer_state_at_entry == "ok"
        assert executor.ctx.entry_liquidity_source_at_entry == "local_l2"
        assert executor.ctx.long_volume_24h_quote_at_entry == pytest.approx(12_000_000.0)
        assert executor.ctx.short_volume_24h_quote_at_entry == pytest.approx(15_000_000.0)
        assert executor.ctx.long_open_interest_quote_at_entry == pytest.approx(8_000_000.0)
        assert executor.ctx.short_open_interest_quote_at_entry == pytest.approx(9_000_000.0)
        assert executor.ctx.long_entry_vwap == pytest.approx(50000.5)
        assert executor.ctx.short_entry_vwap == pytest.approx(50010.5)
        assert executor.ctx.entry_capacity_constrained is True
        assert executor.ctx.entry_target_quantity == pytest.approx(0.16)
        assert executor.ctx.long_max_executable_quantity == pytest.approx(0.18)
        assert executor.ctx.short_max_executable_quantity == pytest.approx(0.16)
        assert executor.ctx.entry_max_executable_quantity == pytest.approx(0.16)
        assert executor.ctx.entry_depth_shortfall_quantity == pytest.approx(0.04)
        assert executor.ctx.entry_max_executable_notional_quote == pytest.approx(8000.0)
        assert executor.ctx.entry_depth_capped_at_entry is True
        assert executor.ctx.advisories == ["thin_book"]
        assert executor.ctx.blocked_reasons == []
        pending = next(iter(runtime.state.pending_entries.values()))
        assert pending.frozen_candidate is not None
        assert pending.frozen_candidate["symbol"] == "MAGMAUSDT"
        assert pending.frozen_candidate["pair_id"] == ""
        assert pending.frozen_candidate["ranking_edge_bps"] == pytest.approx(7.45)
        assert pending.frozen_candidate["entry_notional_quote"] == pytest.approx(500.0)
        assert pending.frozen_candidate["first_funding_leg"] == "long"
        assert pending.frozen_candidate["entry_maker_leg"] == "long"
        assert pending.frozen_candidate["exit_maker_leg"] == "short"

        selected = [
            r for r in runtime.journal.read_all()
            if r["kind"] == "execution.entry_selected"
        ][-1]
        assert selected["payload"]["opportunity_type"] == "staggered"
        assert selected["payload"]["funding_timestamp_ms"] == first_funding_ms
        assert selected["payload"]["second_funding_timestamp_ms"] == second_funding_ms

    def test_entry_context_keeps_retired_es_field_neutral_for_new_entries(self, config, tmp_journal):
        """New entries must not carry a retired ES admission value into recovery state."""
        from lightfee.engine.entry import EntryType
        from lightfee.sidecar.snapshot import CandidateInput

        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        candidate = CandidateInput(
            long_venue="binance",
            short_venue="bybit",
            symbol="BTCUSDT",
            funding_diff_bps=8.0,
            funding_edge_bps=8.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=5.0,
            entry_target_quantity=0.01,
        )

        def build_context():
            return runtime.entry_dispatch_runtime._build_entry_context(
                candidate=candidate,
                entry_id="es-entry",
                long_venue=Venue.BINANCE,
                short_venue=Venue.BYBIT,
                effective_quantity=0.01,
                long_order_price_hint=50_000.0,
                short_order_price_hint=50_000.0,
                maker_leg=Side.BUY,
                entry_type=EntryType.STANDARD_DUAL_TAKER,
                route=ExecutionRoute.FALLBACK_TO_STANDARD,
                now_ms=1_000,
            )

        config.runtime.mode = "live"
        assert build_context().expected_shortfall_bps_entry == 0.0
        assert (
            runtime.entry_dispatch_runtime._build_entry_context(
                candidate=candidate,
                entry_id="es-entry",
                long_venue=Venue.BINANCE,
                short_venue=Venue.BYBIT,
                effective_quantity=0.01,
                long_order_price_hint=50_000.0,
                short_order_price_hint=50_000.0,
                maker_leg=Side.BUY,
                entry_type=EntryType.STANDARD_DUAL_TAKER,
                route=ExecutionRoute.FALLBACK_TO_STANDARD,
                now_ms=1_000,
                expected_shortfall_bps_entry=12.5,
            ).expected_shortfall_bps_entry
            == 0.0
        )

    @pytest.mark.asyncio
    async def test_dispatch_entry_sets_first_stage_exit_from_v1_config(self, config, tmp_journal):
        config.strategy.staggered_exit_mode = "after_first_stage"
        config.strategy.min_scan_minutes_before_funding = 0
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

        now_ms = 1780167385971
        for venue in ("binance", "aster"):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="PRLUSDT",
                    bid=0.2067,
                    ask=0.2069,
                    observed_at_ms=now_ms,
                    received_at_ms=now_ms,
                    source=f"{venue}_bbo_ws",
                )
            )

        dispatched = await runtime._dispatch_entry(candidate, now_ms, price_hint=0.2068)

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
                    "exit_shadow_id": "",
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
    async def test_normal_exit_shadow_records_strategy_decisions_without_extra_close_calls(
        self, config, tmp_journal,
    ):
        config.strategy.post_funding_hold_secs = 0
        config.strategy.settlement_remainder_close_delay_secs = 0
        config.strategy.exit_shadow_enabled = True
        config.strategy.exit_shadow_markout_horizons_ms = [1000]
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal

        cache = VenueBboCache()
        cache.update_quote(
            TopBookQuote(
                venue="binance",
                symbol="BTCUSDT",
                bid=100.0,
                ask=100.1,
                bid_size=12.0,
                ask_size=3.0,
                observed_at_ms=1780167600000,
            )
        )
        cache.update_quote(
            TopBookQuote(
                venue="aster",
                symbol="BTCUSDT",
                bid=100.2,
                ask=100.3,
                bid_size=10.0,
                ask_size=4.0,
                observed_at_ms=1780167600000,
            )
        )
        runtime.ws_bbo_cache = cache

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
        funding_ms = 1780167600000
        position = OpenPosition(
            position_id="entry-shadow-close",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.ASTER,
            long_quantity=0.01,
            short_quantity=0.01,
            long_entry_price=100.0,
            short_entry_price=101.0,
            opened_at_ms=funding_ms - 30_000,
            matched_quantity=0.01,
            funding_timestamp_ms=funding_ms,
            opportunity_type="aligned",
            funding_captured=False,
            current_net_quote=0.0,
        )
        runtime.state.open_positions[position.position_id] = position

        await runtime._maybe_process_normal_exits(funding_ms)

        records = runtime.journal.read_all()
        kinds = [record["kind"] for record in records]
        assert kinds.count("exit_shadow.strategy_decision") == 5
        assert kinds.count("exit_shadow.path_markout") == 3
        shadow_id = next(
            record["payload"]["shadow_id"]
            for record in records
            if record["kind"] == "exit_shadow.strategy_decision"
        )
        routing_payload = next(
            record["payload"]
            for record in records
            if record["kind"] == "runtime.normal_close_routing_passive"
        )
        assert routing_payload["exit_shadow_id"] == shadow_id
        assert passive.start_calls == [
            (
                position.position_id,
                "funding_capture",
                {
                    "long_price_hint": 0.0,
                    "short_price_hint": 0.0,
                    "short_stage": "exit_short",
                    "long_stage": "exit_long",
                    "exit_shadow_id": shadow_id,
                },
            )
        ]
        assert passive.drive_calls == [(position.position_id, False)]

        runtime.state.pending_passive_closes[position.position_id] = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
                active_maker_leg=ActiveMakerLeg.LONG,
            ),
        )

        await runtime._maybe_process_normal_exits(funding_ms + 1)

        retry_records = runtime.journal.read_all()
        retry_kinds = [record["kind"] for record in retry_records]
        assert retry_kinds.count("exit_shadow.strategy_decision") == 5
        assert len(passive.start_calls) == 1
        assert passive.drive_calls == [(position.position_id, False)]

    def test_exit_shadow_local_l2_quote_preserves_stale_observed_at(self, config):
        config.strategy.entry_readiness_provider = "local_l2"
        runtime = LiveRuntime(config, venue_adapters={})
        now_ms = 20_000
        stale_observed_at_ms = 10_000
        for venue in ("binance", "aster"):
            book = runtime.local_l2_runtime.ensure_book(venue, "BTCUSDT")
            book.bids = [PriceLevel(100.0, 12.0)]
            book.asks = [PriceLevel(100.1, 3.0)]
            book.status = L2BookStatus.HOT
            book.observed_at_ms = stale_observed_at_ms

        position = OpenPosition(
            position_id="entry-shadow-stale-l2",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.ASTER,
            long_quantity=0.01,
            short_quantity=0.01,
            long_entry_price=100.0,
            short_entry_price=101.0,
            opened_at_ms=1_000,
            matched_quantity=0.01,
        )

        market = runtime.close_runtime._exit_shadow_market(position, now_ms)
        snapshot = ExitShadowSnapshot(
            position=position,
            reason="funding_capture",
            market=market,
        )

        decisions = evaluate_exit_shadow_strategies(
            snapshot,
            ExitShadowConfig(enabled=True, max_quote_age_ms=500, max_l2_age_ms=500),
        )

        assert market.long_quote is not None
        assert market.long_quote.observed_at_ms == stale_observed_at_ms
        assert {decision.direction for decision in decisions} == {"neutral"}
        assert all("stale" in decision.reason for decision in decisions)

    @pytest.mark.asyncio
    async def test_pending_passive_close_overdue_arms_dual_taker_despite_future_retry(
        self, config, tmp_journal,
    ):
        config.strategy.settlement_force_close_delay_secs = 120
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal

        class CapturingPassiveClose:
            def __init__(self):
                self.seen_phase = None
                self.seen_retry = None

            async def process_pending_passive_closes(self, state, now_ms):
                pending = state.pending_passive_closes["entry-overdue-passive"]
                self.seen_phase = pending.phase_state.phase
                self.seen_retry = pending.next_retry_at_ms
                return set(state.pending_passive_closes.keys())

        passive = CapturingPassiveClose()
        runtime.passive_close_executor = passive
        funding_ms = 1780167600000
        position = OpenPosition(
            position_id="entry-overdue-passive",
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
        runtime.state.pending_passive_closes[position.position_id] = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.HIGH_SLIPPAGE_MAKER,
            ),
            next_retry_at_ms=funding_ms + 8 * 60 * 60 * 1000,
        )

        await runtime._maybe_tick_passive_close(funding_ms + 120_001)

        assert passive.seen_phase == PassiveExecutionPhase.DUAL_TAKER
        assert passive.seen_retry == 0
        kinds = [r["kind"] for r in runtime.journal.read_all()]
        assert "runtime.passive_close_deadline_fallback_armed" in kinds

    @pytest.mark.asyncio
    async def test_overdue_passive_close_fallback_already_dual_taker_is_idempotent(
        self, config, tmp_journal,
    ):
        config.strategy.post_funding_hold_secs = 0
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        funding_ms = 1780167600000
        position = OpenPosition(
            position_id="entry-overdue-passive-already-armed",
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
        runtime.state.pending_passive_closes[position.position_id] = PendingPassiveClose(
            position_id=position.position_id,
            reason="funding_capture",
            position_snapshot=position,
            target_quantity=0.01,
            chunk_quantities=[0.01],
            phase_state=PassivePhaseState(
                phase=PassiveExecutionPhase.DUAL_TAKER,
            ),
            next_retry_at_ms=funding_ms + 8 * 60 * 60 * 1000,
        )

        now_ms = funding_ms + config.strategy.settlement_force_close_delay_secs * 1000 + 1
        runtime._arm_overdue_passive_close_fallbacks(now_ms)

        assert runtime.state.pending_passive_closes[position.position_id].next_retry_at_ms == 0
        kinds = [r["kind"] for r in runtime.journal.read_all()]
        assert "runtime.passive_close_deadline_fallback_armed" not in kinds

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
            first_funding_timestamp_ms=605_000,
            funding_timestamp_ms=605_000,
        )

        for venue in ("binance", "okx"):
            runtime.ws_bbo_cache.update_quote(
                TopBookQuote(
                    venue=venue,
                    symbol="BTCUSDT",
                    bid=49990.0,
                    ask=50010.0,
                    observed_at_ms=5000,
                    received_at_ms=5000,
                    source=f"{venue}_bbo_ws",
                )
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
    async def test_reconcile_retains_pending_when_finalize_defers_missing_fill_details(
        self, config, tmp_journal
    ):
        """V1: deferred finalization is still pending recovery work, not resolved."""

        class PricelessFilledReconciler:
            async def reconcile_position(self, **kwargs):
                return PositionReconciliationResult(
                    position_id=kwargs["position_id"],
                    symbol=kwargs["symbol"],
                    long_status="filled",
                    short_status="filled",
                    long_fill=OrderFill(
                        venue=Venue.BYBIT,
                        symbol=kwargs["symbol"],
                        side=Side.BUY,
                        quantity=116.0,
                        price=0.0,
                        order_id="maker-xlm-filled",
                        filled_at_ms=1780591992000,
                    ),
                    short_fill=OrderFill(
                        venue=Venue.HYPERLIQUID,
                        symbol=kwargs["symbol"],
                        side=Side.SELL,
                        quantity=116.0,
                        price=0.0,
                        order_id="hedge-xlm-filled",
                        filled_at_ms=1780591992000,
                    ),
                )

            def drain_order_diagnostics(self):
                return []

        bybit = FakeVenueAdapter(Venue.BYBIT)
        hyperliquid = FakeVenueAdapter(Venue.HYPERLIQUID)
        runtime = LiveRuntime(
            config,
            venue_adapters={Venue.BYBIT: bybit, Venue.HYPERLIQUID: hyperliquid},
        )
        runtime.journal = tmp_journal
        runtime.reconciler = PricelessFilledReconciler()
        pending = PendingEntry(
            pending_id="entry-xlm-priceless",
            symbol="XLMUSDT",
            long_venue=Venue.BYBIT,
            short_venue=Venue.HYPERLIQUID,
            target_quantity=116.0,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=1780591991000,
            maker_order_id="maker-xlm-filled",
            hedge_order_id="hedge-xlm-filled",
            maker_client_order_id="maker-xlm-cid",
            hedge_client_order_id="hedge-xlm-cid",
            uncertain_outcome=True,
        )
        runtime.state.pending_entries[pending.pending_id] = pending

        await runtime._reconcile_pending_state(1780591993000)

        assert pending.pending_id in runtime.state.pending_entries
        assert runtime.state.open_positions == {}
        records = tmp_journal.read_all()
        kinds = [record["kind"] for record in records]
        assert "pending_entry.finalize_deferred_incomplete_fill" in kinds
        assert "pending_entry.pending_entry_finalized" not in kinds

    @pytest.mark.asyncio
    async def test_force_terminal_zero_fill_uses_finalizer_not_blind_pop(
        self, config, tmp_journal
    ):
        """V1: force-terminal zero fill still emits terminal no-fill evidence."""
        config.strategy.pending_entry_force_terminal_after_ms = 1_000
        config.strategy.pending_entry_hard_ceiling_ms = 120_000
        runtime = LiveRuntime(config, venue_adapters={})
        runtime.journal = tmp_journal
        pending = PendingEntry(
            pending_id="entry-zero-force-terminal",
            symbol="WLDUSDT",
            long_venue=Venue.BYBIT,
            short_venue=Venue.HYPERLIQUID,
            target_quantity=38.7,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=1_000,
            maker_client_order_id="",
            hedge_client_order_id="",
            uncertain_outcome=True,
            passive_order=PendingPassiveOrder(
                last_progress_state=PassiveOrderState.CANCELED,
                target_quantity=38.7,
            ),
        )
        runtime.state.pending_entries[pending.pending_id] = pending

        handled = await runtime._force_terminalize_pending_entry_if_budget_exhausted(
            pending,
            pending.pending_id,
            3_000,
        )

        assert handled is True
        assert pending.pending_id not in runtime.state.pending_entries
        records = tmp_journal.read_all()
        kinds = [record["kind"] for record in records]
        assert "entry.passive_unfilled" in kinds
        assert "pending_entry.pending_entry_finalized" in kinds
        assert "pending_entry.force_terminalized" not in kinds

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
            first_funding_timestamp_ms=605_000,
            funding_timestamp_ms=605_000,
        )

        await runtime._dispatch_entry(candidate, 5000, price_hint=50000.0)

        records = runtime.journal.read_all()
        # Venue minimums are now checked before route construction so the
        # failure keeps its exact economic meaning instead of being collapsed
        # into a generic planner rejection.
        blocked = [
            record["payload"]
            for record in records
            if record["kind"] == "entry.dispatch_viability_blocked"
        ]
        assert blocked[-1]["reason"] == "entry_pair_minimum_not_met"

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
            first_funding_timestamp_ms=605_000,
            funding_timestamp_ms=605_000,
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
