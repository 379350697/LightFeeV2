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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig, StrategyConfig
from lightfee.core.domain import (
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
from lightfee.engine.state import (
    EngineState,
    OpenPosition,
    PendingPassiveOrder,
    PassiveExecutionPhase,
    PassivePhaseState,
    PendingEntry,
    PendingPassiveClose,
)
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


def test_select_entry_candidates_blocks_recovery_ledger_before_readiness(
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

    pair_id = "btcusdt:binance->bybit"
    assert selected == []
    assert counts["entry_blocked_recovery_ledger"] == 1
    assert blockers[pair_id] == "entry_blocked_recovery_ledger"
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
    assert payload["reason"] == "entry_blocked_recovery_ledger"
    evidence = payload["lifecycle_evidence"]
    assert evidence["source"] == "selection"
    assert evidence["truth_available"] is True
    assert evidence["blocking_work"][0]["kind"] == "orphan_maker_order"


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
            first_funding_timestamp_ms=605_000,
            funding_timestamp_ms=605_000,
        )

    @pytest.mark.asyncio
    async def test_tick_refreshes_recovery_ledger_before_dispatch(
        self, config, tmp_journal, monkeypatch,
    ):
        from lightfee.sidecar.snapshot import QuoteSnapshot, SidecarSnapshot

        config.runtime.live_scan_recovery_success_count = 1
        config.runtime.sidecar_snapshot_max_age_ms = 10_000
        config.runtime.max_market_age_ms = 10_000
        runtime = LiveRuntime(
            config,
            venue_adapters={
                Venue.BINANCE: FakeVenueAdapter(Venue.BINANCE),
                Venue.OKX: FakeVenueAdapter(Venue.OKX),
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
                ),
                "okx:BTCUSDT": QuoteSnapshot(
                    venue="okx",
                    symbol="BTCUSDT",
                    bid=50_000.0,
                    ask=50_010.0,
                    observed_at_ms=7_000,
                )
            },
        )
        order: list[str] = []
        refreshed_symbols: list[str] = []

        async def refresh(symbols, now_ms):
            order.append("refresh")
            refreshed_symbols.extend(symbols)
            return None

        async def dispatch(candidate, now_ms, price_hint=0.0):
            order.append("dispatch")
            return False

        def select_candidates(candidates, **_kwargs):
            return list(candidates)

        monkeypatch.setattr("lightfee.engine.runtime.wall_clock_now_ms", lambda: 7_001)
        monkeypatch.setattr("lightfee.engine.runtime.load_snapshot", lambda _path: snapshot)
        monkeypatch.setattr(
            "lightfee.engine.runtime.discover_tradeable_candidates",
            lambda candidates, _strategy, _now_ms: list(candidates),
        )
        monkeypatch.setattr(runtime, "_refresh_recovery_ledger_for_symbols", refresh)
        monkeypatch.setattr(runtime, "_dispatch_entry", dispatch)
        monkeypatch.setattr(runtime, "_select_entry_candidates", select_candidates)

        await runtime.tick()

        assert order[:2] == ["refresh", "dispatch"]
        assert refreshed_symbols == ["BTCUSDT"]

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
        runtime.state.tick_count = 77
        runtime.entry_executor = EntrySyncExecutor(adapters=adapters, journal=tmp_journal)
        self._install_hot_book(runtime, "binance", "BTCUSDT", bid=50000.0, ask=50010.0, observed_at_ms=5000)
        self._install_hot_book(runtime, "okx", "BTCUSDT", bid=49990.0, ask=50000.0, observed_at_ms=5000)

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
            first_funding_timestamp_ms=605_000,
            funding_timestamp_ms=605_000,
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
            entry_target_quantity=0.2,
            long_max_executable_quantity=0.18,
            short_max_executable_quantity=0.16,
            entry_max_executable_quantity=0.16,
            entry_depth_shortfall_quantity=0.04,
            entry_max_executable_notional_quote=8000.0,
            entry_depth_capped_at_entry=True,
            advisories=["thin_book"],
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
        assert executor.ctx.entry_target_quantity == pytest.approx(0.2)
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
