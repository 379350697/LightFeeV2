from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from lightfee.core.domain import FundingSettlement, Venue
from lightfee.engine.close_executor import CloseExecutor
from lightfee.engine.close_runtime import CloseRuntime
from lightfee.engine.exit import CloseExecution
from lightfee.engine.recovery import (
    _restore_state_from_snapshot_dict,
    build_persistent_state_view,
    recover_from_snapshot,
)
from lightfee.engine.state import EngineState, OpenPosition
from lightfee.persistence.journal import Journal
from lightfee.persistence.snapshot_store import SnapshotStore
from lightfee.strategy.funding_settlement_reconciler import FundingSettlementReconciler


def _position(
    *,
    position_id: str = "position-1",
    long_venue: Venue = Venue.BINANCE,
    short_venue: Venue = Venue.OKX,
    timestamp: int = 1_000,
    **overrides: object,
) -> OpenPosition:
    fields: dict[str, object] = {
        "position_id": position_id,
        "symbol": "BTCUSDT",
        "long_venue": long_venue,
        "short_venue": short_venue,
        "long_quantity": 1.0,
        "short_quantity": 1.0,
        "long_entry_price": 100.0,
        "short_entry_price": 100.0,
        "opened_at_ms": 1,
        "funding_timestamp_ms": timestamp,
        "long_funding_timestamp_ms": timestamp,
        "short_funding_timestamp_ms": timestamp,
        "funding_captured": True,
        "captured_funding_quote": 0.4,
    }
    fields.update(overrides)
    return OpenPosition(**fields)


class _StatementsAdapter:
    def __init__(self, rows: list[FundingSettlement]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, int, int]] = []

    async def fetch_funding_settlements(
        self, symbol: str, *, start_time_ms: int, end_time_ms: int,
    ) -> list[FundingSettlement]:
        self.calls.append((symbol, start_time_ms, end_time_ms))
        return list(self.rows)


class _BlockingStatementsAdapter(_StatementsAdapter):
    def __init__(self, rows: list[FundingSettlement]) -> None:
        super().__init__(rows)
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def fetch_funding_settlements(
        self, symbol: str, *, start_time_ms: int, end_time_ms: int,
    ) -> list[FundingSettlement]:
        self.calls.append((symbol, start_time_ms, end_time_ms))
        self.started.set()
        await self.release.wait()
        return list(self.rows)


def _statement(
    venue: Venue,
    amount: float,
    *,
    timestamp: int = 1_000,
    reference: str = "statement-1",
    quote_currency: str = "USDT",
) -> FundingSettlement:
    return FundingSettlement(
        venue=venue,
        symbol="BTCUSDT",
        settlement_timestamp_ms=timestamp,
        amount_quote=amount,
        quote_currency=quote_currency,
        observed_at_ms=1_200,
        source="private_statement",
        statement_reference=reference,
    )


@pytest.mark.asyncio
async def test_reconciler_allocates_only_exact_single_owner_statement_targets() -> None:
    position = _position()
    binance = _StatementsAdapter([_statement(Venue.BINANCE, -0.1, reference="binance-1")])
    okx = _StatementsAdapter([_statement(Venue.OKX, 0.5, reference="okx-1")])

    results = await FundingSettlementReconciler().reconcile_open_positions(
        [position], {Venue.BINANCE: binance, Venue.OKX: okx},
    )

    assert len(results) == 1
    assert results[0].official is True
    assert results[0].settled_funding_quote == pytest.approx(0.4)
    assert position.funding_settlement_evidence_status == "complete"
    assert len(position.funding_settlement_records) == 2
    # The query is bounded and grouped once per venue/symbol, not per leg.
    assert binance.calls == [("BTCUSDT", 1, 301_000)]
    assert okx.calls == [("BTCUSDT", 1, 301_000)]


@pytest.mark.asyncio
async def test_reconciler_does_not_allocate_a_statement_shared_by_two_positions() -> None:
    first = _position(position_id="first", short_venue=Venue.BINANCE)
    second = _position(position_id="second", short_venue=Venue.BINANCE)
    adapter = _StatementsAdapter([_statement(Venue.BINANCE, 0.4)])

    results = await FundingSettlementReconciler().reconcile_open_positions(
        [first, second], {Venue.BINANCE: adapter},
    )

    assert {result.reason for result in results} == {"ambiguous_position_ownership"}
    assert not first.funding_settlement_records
    assert not second.funding_settlement_records
    assert all(result.official is False for result in results)


@pytest.mark.asyncio
async def test_reconciler_rejects_timestamp_and_currency_near_matches() -> None:
    position = _position()
    adapter = _StatementsAdapter([
        _statement(Venue.BINANCE, 0.4, timestamp=1_001, reference="near-time"),
        _statement(Venue.BINANCE, 0.4, reference="wrong-currency", quote_currency="USDC"),
    ])

    results = await FundingSettlementReconciler().reconcile_open_positions(
        [position], {Venue.BINANCE: adapter},
    )

    assert results[0].official is False
    assert results[0].reason == "statement_quote_currency_mismatch"
    assert not position.funding_settlement_records


@pytest.mark.asyncio
async def test_post_close_task_remains_non_official_until_all_private_statements_arrive() -> None:
    position = _position()
    task = FundingSettlementReconciler.new_pending_task(
        position,
        closed_at_ms=2_000,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    assert task is not None

    reconciler = FundingSettlementReconciler()
    incomplete, missing = await reconciler.reconcile_pending_task(
        task,
        {Venue.BINANCE: _StatementsAdapter([_statement(Venue.BINANCE, -0.1)])},
        now_ms=3_000,
    )
    assert missing.official is False
    assert incomplete["status"] == "unresolved_statement_evidence"
    assert "official_net_quote" not in incomplete

    complete, result = await reconciler.reconcile_pending_task(
        incomplete,
        {
            Venue.BINANCE: _StatementsAdapter([_statement(Venue.BINANCE, -0.1, reference="statement-1")]),
            Venue.OKX: _StatementsAdapter([_statement(Venue.OKX, 0.5, reference="okx-1")]),
        },
        now_ms=4_000,
    )
    assert result.official is True
    assert complete["official_funding_quote"] == pytest.approx(0.4)
    assert complete["official_net_quote"] == pytest.approx(0.9)
    assert complete["funding_forecast_error_quote"] == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_pending_batch_rejects_a_statement_claimed_by_two_closed_positions() -> None:
    first = FundingSettlementReconciler.new_pending_task(
        _position(position_id="closed-first"),
        closed_at_ms=2_000,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    second = FundingSettlementReconciler.new_pending_task(
        _position(position_id="closed-second", short_venue=Venue.ASTER),
        closed_at_ms=2_000,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    assert first is not None and second is not None

    reconciled = await FundingSettlementReconciler().reconcile_pending_tasks(
        [first, second],
        {Venue.BINANCE: _StatementsAdapter([_statement(Venue.BINANCE, 0.4)])},
        now_ms=3_000,
    )

    assert {result.reason for _, result in reconciled} == {
        "ambiguous_position_ownership"
    }
    assert all(result.official is False for _, result in reconciled)
    assert all(updated.get("official_pnl") is not True for updated, _ in reconciled)


@pytest.mark.asyncio
async def test_pending_batch_reserves_matching_open_position_statement_claim() -> None:
    task = FundingSettlementReconciler.new_pending_task(
        _position(position_id="closed-position"),
        closed_at_ms=2_000,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    assert task is not None
    open_position = _position(position_id="still-open", short_venue=Venue.ASTER)

    [(updated, result)] = await FundingSettlementReconciler().reconcile_pending_tasks(
        [task],
        {Venue.BINANCE: _StatementsAdapter([_statement(Venue.BINANCE, 0.4)])},
        now_ms=3_000,
        ownership_positions=[open_position],
    )

    assert result.official is False
    assert result.reason == "ambiguous_position_ownership"
    assert updated.get("official_pnl") is not True


def test_close_registration_survives_snapshot_before_position_is_removed() -> None:
    state = EngineState()
    position = _position(
        long_entry_fee_quote=0.1,
        short_entry_fee_quote=0.1,
        realized_price_pnl_quote=1.0,
        realized_exit_fee_quote=0.3,
    )
    task = FundingSettlementReconciler.register_closed_position_task(
        state,
        position,
        closed_at_ms=2_000,
        price_pnl_quote=position.realized_price_pnl_quote,
        exit_fee_quote=position.realized_exit_fee_quote,
    )

    assert task is not None
    snapshot = build_persistent_state_view(state)
    assert snapshot["pending_funding_settlement_reconciliations"] == [task]
    restored = _restore_state_from_snapshot_dict(snapshot)
    assert restored.pending_funding_settlement_reconciliations == [task]


def test_aggressive_close_registers_funding_statement_task_before_removing_position() -> None:
    state = EngineState()
    position = _position(
        long_entry_fee_quote=0.1,
        short_entry_fee_quote=0.1,
        realized_price_pnl_quote=0.7,
        realized_exit_fee_quote=0.3,
    )
    state.open_positions[position.position_id] = position
    journal = _RuntimeJournal()
    executor = CloseExecutor(adapters={}, journal=journal)
    close = CloseExecution(
        position_id=position.position_id,
        reason="funding_capture",
        long_close_price=100.0,
        short_close_price=100.0,
        long_close_qty=1.0,
        short_close_qty=1.0,
    )

    executor._writeback_to_state(
        state,
        position,
        close,
        long_closed=1.0,
        short_closed=1.0,
        long_uncertain=False,
        short_uncertain=False,
        now_ms=2_000,
        reason="funding_capture",
    )

    assert position.position_id not in state.open_positions
    assert len(state.pending_funding_settlement_reconciliations) == 1
    task = state.pending_funding_settlement_reconciliations[0]
    assert task["price_pnl_quote"] == pytest.approx(0.7)
    assert task["entry_fee_quote"] == pytest.approx(0.2)
    assert task["exit_fee_quote"] == pytest.approx(0.3)
    assert [kind for kind, _ in journal.records] == [
        "funding.settlement_reconciliation_registered",
    ]


class _RuntimeJournal:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, object]]] = []

    def append(self, kind: str, payload: dict[str, object]) -> None:
        self.records.append((kind, payload))

    def append_critical(
        self,
        _now_ms: int,
        kind: str,
        payload: dict[str, object],
    ) -> None:
        self.records.append((kind, payload))


class _FailOnceCriticalJournal(_RuntimeJournal):
    """Inject a receipt durability failure after statement claims are reserved."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_next_critical = True

    def append_critical(
        self,
        now_ms: int,
        kind: str,
        payload: dict[str, object],
    ) -> None:
        if self.fail_next_critical:
            self.fail_next_critical = False
            raise OSError("injected critical journal failure")
        super().append_critical(now_ms, kind, payload)


class _FailNthCriticalJournal(_RuntimeJournal):
    """Fail one receipt after earlier receipts in the same batch are durable."""

    def __init__(self, fail_on_call: int, *, fail_deferred_diagnostic: bool = False) -> None:
        super().__init__()
        self.fail_on_call = fail_on_call
        self.fail_deferred_diagnostic = fail_deferred_diagnostic
        self.critical_calls = 0

    def append(self, kind: str, payload: dict[str, object]) -> None:
        if (
            self.fail_deferred_diagnostic
            and kind == "funding.settlement_reconciliation_deferred"
        ):
            raise OSError("injected deferred diagnostic journal failure")
        super().append(kind, payload)

    def append_critical(
        self,
        now_ms: int,
        kind: str,
        payload: dict[str, object],
    ) -> None:
        self.critical_calls += 1
        if self.critical_calls == self.fail_on_call:
            raise OSError("injected later critical journal failure")
        super().append_critical(now_ms, kind, payload)


@pytest.mark.asyncio
async def test_runtime_reconciles_complete_task_without_reopening_exposure_lifecycle() -> None:
    state = EngineState()
    task = FundingSettlementReconciler.new_pending_task(
        _position(),
        closed_at_ms=2_000,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    assert task is not None
    state.enqueue_pending_funding_settlement_reconciliation(task)
    journal = _RuntimeJournal()
    runtime = CloseRuntime(
        SimpleNamespace(
            state=state,
            config=SimpleNamespace(runtime=SimpleNamespace(mode="live")),
            venue_adapters={
                Venue.BINANCE: _StatementsAdapter([
                    _statement(Venue.BINANCE, -0.1, reference="binance-1")
                ]),
                Venue.OKX: _StatementsAdapter([
                    _statement(Venue.OKX, 0.5, reference="okx-1")
                ]),
            },
            journal=journal,
        )
    )

    await runtime._process_pending_funding_settlement_reconciliations(now_ms=3_000)

    assert state.open_positions == {}
    assert state.pending_funding_settlement_reconciliations == []
    record = next(payload for kind, payload in journal.records if kind == "funding.settlement_reconciled")
    assert record["official_funding_quote"] == pytest.approx(0.4)
    assert record["official_net_quote"] == pytest.approx(0.9)
    assert record["price_pnl_quote"] == pytest.approx(1.0)
    assert record["entry_fee_quote"] == pytest.approx(0.2)
    assert record["exit_fee_quote"] == pytest.approx(0.3)
    assert record["lifecycle_forecast_funding_quote"] == pytest.approx(0.4)
    assert len(record["statement_claims"]) == 2


@pytest.mark.asyncio
async def test_recovery_replays_durable_receipt_after_pre_snapshot_crash(tmp_path) -> None:
    """A durable receipt must beat an older snapshot with the same task queued."""
    task = FundingSettlementReconciler.new_pending_task(
        _position(),
        closed_at_ms=2_000,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    assert task is not None

    stale_state = EngineState()
    stale_state.enqueue_pending_funding_settlement_reconciliation(task)
    snapshot = SnapshotStore(tmp_path / "state.json")
    snapshot.write(build_persistent_state_view(stale_state))

    receipt_state = EngineState()
    receipt_state.enqueue_pending_funding_settlement_reconciliation(dict(task))
    receipt_journal = _RuntimeJournal()
    runtime = CloseRuntime(
        SimpleNamespace(
            state=receipt_state,
            config=SimpleNamespace(runtime=SimpleNamespace(mode="live")),
            venue_adapters={
                Venue.BINANCE: _StatementsAdapter([
                    _statement(Venue.BINANCE, -0.1, reference="binance-1")
                ]),
                Venue.OKX: _StatementsAdapter([
                    _statement(Venue.OKX, 0.5, reference="okx-1")
                ]),
            },
            journal=receipt_journal,
        )
    )
    await runtime._process_pending_funding_settlement_reconciliations(now_ms=3_000)
    receipt = next(
        payload
        for kind, payload in receipt_journal.records
        if kind == "funding.settlement_reconciled"
    )

    journal = Journal(tmp_path / "journal.jsonl")
    journal.open()
    try:
        journal.append_critical(3_000, "funding.settlement_reconciled", receipt)
    finally:
        journal.close()

    restored = recover_from_snapshot(snapshot, Journal(tmp_path / "journal.jsonl"))
    assert restored.pending_funding_settlement_reconciliations == []
    assert restored.funding_settlement_statement_claim_ledger == receipt["statement_claims"]

    # Journal replay is run on every restart; it must remain idempotent.
    restored_again = recover_from_snapshot(snapshot, Journal(tmp_path / "journal.jsonl"))
    assert restored_again.pending_funding_settlement_reconciliations == []
    assert restored_again.funding_settlement_statement_claim_ledger == receipt["statement_claims"]


@pytest.mark.asyncio
async def test_nonblocking_critical_receipt_failure_releases_claims_for_retry() -> None:
    state = EngineState()
    task = FundingSettlementReconciler.new_pending_task(
        _position(),
        closed_at_ms=2_000,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    assert task is not None
    state.enqueue_pending_funding_settlement_reconciliation(task)
    journal = _FailOnceCriticalJournal()
    runtime = CloseRuntime(
        SimpleNamespace(
            state=state,
            config=SimpleNamespace(runtime=SimpleNamespace(mode="live")),
            venue_adapters={
                Venue.BINANCE: _StatementsAdapter([_statement(Venue.BINANCE, -0.1)]),
                Venue.OKX: _StatementsAdapter([_statement(Venue.OKX, 0.5)]),
            },
            journal=journal,
        )
    )

    await runtime.drive_pending_funding_settlement_reconciliations_nonblocking(3_000)
    for _ in range(3):
        await asyncio.sleep(0)
    await runtime.drive_pending_funding_settlement_reconciliations_nonblocking(3_000)

    assert state.funding_settlement_statement_claim_ledger == []
    assert len(state.pending_funding_settlement_reconciliations) == 1
    retained = state.pending_funding_settlement_reconciliations[0]
    assert retained["last_reason"] == "reconciliation_error:OSError"
    assert not any(
        kind == "funding.settlement_reconciled" for kind, _ in journal.records
    )

    retained["next_attempt_ms"] = 3_100
    await runtime.drive_pending_funding_settlement_reconciliations_nonblocking(3_100)
    for _ in range(3):
        await asyncio.sleep(0)
    await runtime.drive_pending_funding_settlement_reconciliations_nonblocking(3_100)

    assert state.pending_funding_settlement_reconciliations == []
    assert len(state.funding_settlement_statement_claim_ledger) == 2
    assert [kind for kind, _ in journal.records].count(
        "funding.settlement_reconciled"
    ) == 1


@pytest.mark.asyncio
async def test_synchronous_critical_receipt_failure_releases_claims_and_defers() -> None:
    state = EngineState()
    task = FundingSettlementReconciler.new_pending_task(
        _position(),
        closed_at_ms=2_000,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    assert task is not None
    state.enqueue_pending_funding_settlement_reconciliation(task)
    journal = _FailOnceCriticalJournal()
    runtime = CloseRuntime(
        SimpleNamespace(
            state=state,
            config=SimpleNamespace(runtime=SimpleNamespace(mode="live")),
            venue_adapters={
                Venue.BINANCE: _StatementsAdapter([_statement(Venue.BINANCE, -0.1)]),
                Venue.OKX: _StatementsAdapter([_statement(Venue.OKX, 0.5)]),
            },
            journal=journal,
        )
    )

    await runtime._process_pending_funding_settlement_reconciliations(now_ms=3_000)

    assert state.funding_settlement_statement_claim_ledger == []
    assert len(state.pending_funding_settlement_reconciliations) == 1
    retained = state.pending_funding_settlement_reconciliations[0]
    assert retained["last_reason"] == "reconciliation_error:OSError"
    assert retained["official_pnl"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_deferred_diagnostic", [False, True])
async def test_later_critical_receipt_failure_keeps_earlier_batch_receipt_committed(
    fail_deferred_diagnostic: bool,
) -> None:
    state = EngineState()
    first = FundingSettlementReconciler.new_pending_task(
        _position(position_id="first", timestamp=1_000),
        closed_at_ms=2_000,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    second = FundingSettlementReconciler.new_pending_task(
        _position(position_id="second", short_venue=Venue.ASTER, timestamp=2_000),
        closed_at_ms=2_001,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    assert first is not None and second is not None
    state.enqueue_pending_funding_settlement_reconciliation(first)
    state.enqueue_pending_funding_settlement_reconciliation(second)
    journal = _FailNthCriticalJournal(
        fail_on_call=2,
        fail_deferred_diagnostic=fail_deferred_diagnostic,
    )
    runtime = CloseRuntime(
        SimpleNamespace(
            state=state,
            config=SimpleNamespace(runtime=SimpleNamespace(mode="live")),
            venue_adapters={
                Venue.BINANCE: _StatementsAdapter([
                    _statement(
                        Venue.BINANCE,
                        -0.1,
                        timestamp=1_000,
                        reference="binance-1000",
                    ),
                    _statement(
                        Venue.BINANCE,
                        -0.2,
                        timestamp=2_000,
                        reference="binance-2000",
                    ),
                ]),
                Venue.OKX: _StatementsAdapter([
                    _statement(
                        Venue.OKX,
                        0.5,
                        timestamp=1_000,
                        reference="okx-1000",
                    )
                ]),
                Venue.ASTER: _StatementsAdapter([
                    _statement(
                        Venue.ASTER,
                        0.4,
                        timestamp=2_000,
                        reference="aster-2000",
                    )
                ]),
            },
            journal=journal,
        )
    )

    await runtime.drive_pending_funding_settlement_reconciliations_nonblocking(3_000)
    for _ in range(3):
        await asyncio.sleep(0)
    await runtime.drive_pending_funding_settlement_reconciliations_nonblocking(3_000)

    assert [task["position_id"] for task in state.pending_funding_settlement_reconciliations] == [
        "second"
    ]
    assert state.pending_funding_settlement_reconciliations[0]["last_reason"] == (
        "reconciliation_error:OSError"
    )
    assert {
        row["owner_id"] for row in state.funding_settlement_statement_claim_ledger
    } == {"first"}
    assert [kind for kind, _ in journal.records].count(
        "funding.settlement_reconciled"
    ) == 1


@pytest.mark.asyncio
async def test_runtime_batch_never_marks_shared_closed_claims_official() -> None:
    state = EngineState()
    first = FundingSettlementReconciler.new_pending_task(
        _position(position_id="closed-first"),
        closed_at_ms=2_000,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    second = FundingSettlementReconciler.new_pending_task(
        _position(position_id="closed-second", short_venue=Venue.ASTER),
        closed_at_ms=2_000,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    assert first is not None and second is not None
    state.enqueue_pending_funding_settlement_reconciliation(first)
    state.enqueue_pending_funding_settlement_reconciliation(second)
    journal = _RuntimeJournal()
    runtime = CloseRuntime(
        SimpleNamespace(
            state=state,
            config=SimpleNamespace(runtime=SimpleNamespace(mode="live")),
            venue_adapters={Venue.BINANCE: _StatementsAdapter([_statement(Venue.BINANCE, 0.4)])},
            journal=journal,
        )
    )

    await runtime._process_pending_funding_settlement_reconciliations(now_ms=3_000)

    assert len(state.pending_funding_settlement_reconciliations) == 2
    assert {
        task["last_reason"]
        for task in state.pending_funding_settlement_reconciliations
    } == {"ambiguous_position_ownership"}
    assert not any(kind == "funding.settlement_reconciled" for kind, _ in journal.records)


@pytest.mark.asyncio
async def test_runtime_backoff_task_reserves_statement_ownership_for_due_task() -> None:
    state = EngineState()
    backoff = FundingSettlementReconciler.new_pending_task(
        _position(position_id="backoff"),
        closed_at_ms=2_000,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    due = FundingSettlementReconciler.new_pending_task(
        _position(position_id="due", short_venue=Venue.ASTER),
        closed_at_ms=2_000,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    assert backoff is not None and due is not None
    backoff["next_attempt_ms"] = 4_000
    state.enqueue_pending_funding_settlement_reconciliation(backoff)
    state.enqueue_pending_funding_settlement_reconciliation(due)
    journal = _RuntimeJournal()
    runtime = CloseRuntime(
        SimpleNamespace(
            state=state,
            config=SimpleNamespace(runtime=SimpleNamespace(mode="live")),
            venue_adapters={Venue.BINANCE: _StatementsAdapter([_statement(Venue.BINANCE, 0.4)])},
            journal=journal,
        )
    )

    await runtime._process_pending_funding_settlement_reconciliations(now_ms=3_000)

    assert len(state.pending_funding_settlement_reconciliations) == 2
    due_task = next(
        task
        for task in state.pending_funding_settlement_reconciliations
        if task.get("position_id") == "due"
    )
    assert due_task["last_reason"] == "ambiguous_position_ownership"
    assert not any(kind == "funding.settlement_reconciled" for kind, _ in journal.records)


@pytest.mark.asyncio
async def test_runtime_consumed_statement_ledger_blocks_later_duplicate_task() -> None:
    state = EngineState()
    first = FundingSettlementReconciler.new_pending_task(
        _position(position_id="first"),
        closed_at_ms=2_000,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    assert first is not None
    state.enqueue_pending_funding_settlement_reconciliation(first)
    journal = _RuntimeJournal()
    runtime = CloseRuntime(
        SimpleNamespace(
            state=state,
            config=SimpleNamespace(runtime=SimpleNamespace(mode="live")),
            venue_adapters={
                Venue.BINANCE: _StatementsAdapter([_statement(Venue.BINANCE, -0.1)]),
                Venue.OKX: _StatementsAdapter([_statement(Venue.OKX, 0.5)]),
                Venue.ASTER: _StatementsAdapter([_statement(Venue.ASTER, 0.5)]),
            },
            journal=journal,
        )
    )

    await runtime._process_pending_funding_settlement_reconciliations(now_ms=3_000)

    assert state.pending_funding_settlement_reconciliations == []
    assert state.funding_settlement_statement_claim_ledger
    later = FundingSettlementReconciler.new_pending_task(
        _position(position_id="later", short_venue=Venue.ASTER),
        closed_at_ms=4_000,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    assert later is not None
    state.enqueue_pending_funding_settlement_reconciliation(later)

    await runtime._process_pending_funding_settlement_reconciliations(now_ms=5_000)

    assert len(state.pending_funding_settlement_reconciliations) == 1
    assert state.pending_funding_settlement_reconciliations[0]["last_reason"] == (
        "ambiguous_position_ownership"
    )
    assert [kind for kind, _ in journal.records].count("funding.settlement_reconciled") == 1


@pytest.mark.asyncio
async def test_nonblocking_runtime_reconciliation_preserves_task_enqueued_while_io_waits() -> None:
    state = EngineState()
    first = FundingSettlementReconciler.new_pending_task(
        _position(position_id="first"),
        closed_at_ms=2_000,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    assert first is not None
    state.enqueue_pending_funding_settlement_reconciliation(first)
    binance = _BlockingStatementsAdapter([_statement(Venue.BINANCE, -0.1)])
    runtime = CloseRuntime(
        SimpleNamespace(
            state=state,
            config=SimpleNamespace(runtime=SimpleNamespace(mode="live")),
            venue_adapters={
                Venue.BINANCE: binance,
                Venue.OKX: _StatementsAdapter([_statement(Venue.OKX, 0.5)]),
            },
            journal=_RuntimeJournal(),
        )
    )

    await asyncio.wait_for(
        runtime.drive_pending_funding_settlement_reconciliations_nonblocking(3_000),
        timeout=0.1,
    )
    await asyncio.wait_for(binance.started.wait(), timeout=0.1)
    assert len(state.pending_funding_settlement_reconciliations) == 1

    second = FundingSettlementReconciler.new_pending_task(
        _position(
            position_id="queued-during-io",
            long_venue=Venue.ASTER,
            short_venue=Venue.BYBIT,
        ),
        closed_at_ms=3_001,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    assert second is not None
    state.enqueue_pending_funding_settlement_reconciliation(second)
    binance.release.set()
    for _ in range(3):
        await asyncio.sleep(0)
    await runtime.drive_pending_funding_settlement_reconciliations_nonblocking(3_100)

    assert [
        task["position_id"] for task in state.pending_funding_settlement_reconciliations
    ] == ["queued-during-io"]


@pytest.mark.asyncio
async def test_nonblocking_runtime_marks_enqueue_during_io_with_same_claim_ambiguous() -> None:
    state = EngineState()
    first = FundingSettlementReconciler.new_pending_task(
        _position(position_id="first"),
        closed_at_ms=2_000,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    assert first is not None
    state.enqueue_pending_funding_settlement_reconciliation(first)
    binance = _BlockingStatementsAdapter([_statement(Venue.BINANCE, -0.1)])
    journal = _RuntimeJournal()
    runtime = CloseRuntime(
        SimpleNamespace(
            state=state,
            config=SimpleNamespace(runtime=SimpleNamespace(mode="live")),
            venue_adapters={
                Venue.BINANCE: binance,
                Venue.OKX: _StatementsAdapter([_statement(Venue.OKX, 0.5)]),
            },
            journal=journal,
        )
    )

    await runtime.drive_pending_funding_settlement_reconciliations_nonblocking(3_000)
    await asyncio.wait_for(binance.started.wait(), timeout=0.1)
    duplicate = FundingSettlementReconciler.new_pending_task(
        _position(position_id="duplicate", short_venue=Venue.ASTER),
        closed_at_ms=3_001,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    assert duplicate is not None
    state.enqueue_pending_funding_settlement_reconciliation(duplicate)
    binance.release.set()
    for _ in range(3):
        await asyncio.sleep(0)
    await runtime.drive_pending_funding_settlement_reconciliations_nonblocking(3_100)

    assert len(state.pending_funding_settlement_reconciliations) == 2
    assert {
        task["last_reason"] for task in state.pending_funding_settlement_reconciliations
    } == {"ambiguous_position_ownership"}
    assert not any(kind == "funding.settlement_reconciled" for kind, _ in journal.records)


@pytest.mark.asyncio
async def test_nonblocking_runtime_timeout_releases_singleflight_for_retry() -> None:
    state = EngineState()
    task = FundingSettlementReconciler.new_pending_task(
        _position(),
        closed_at_ms=2_000,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    assert task is not None
    state.enqueue_pending_funding_settlement_reconciliation(task)
    blocking = _BlockingStatementsAdapter([])
    runtime = CloseRuntime(
        SimpleNamespace(
            state=state,
            config=SimpleNamespace(runtime=SimpleNamespace(mode="live")),
            venue_adapters={Venue.BINANCE: blocking},
            journal=_RuntimeJournal(),
        )
    )
    runtime._FUNDING_SETTLEMENT_RECONCILIATION_TIMEOUT_S = 0.01

    await runtime.drive_pending_funding_settlement_reconciliations_nonblocking(3_000)
    await asyncio.wait_for(blocking.started.wait(), timeout=0.1)
    await asyncio.sleep(0.02)
    await runtime.drive_pending_funding_settlement_reconciliations_nonblocking(3_100)

    retained = state.pending_funding_settlement_reconciliations[0]
    assert retained["last_reason"] == "reconciliation_error:TimeoutError"
    assert runtime._funding_settlement_reconciliation_worker is None

    retained["next_attempt_ms"] = 3_100
    await runtime.drive_pending_funding_settlement_reconciliations_nonblocking(3_100)
    assert runtime._funding_settlement_reconciliation_worker is not None
    await runtime.shutdown_pending_funding_settlement_reconciliation_worker()


@pytest.mark.asyncio
async def test_runtime_expiry_keeps_unofficial_accounting_evidence_visible() -> None:
    state = EngineState()
    task = FundingSettlementReconciler.new_pending_task(
        _position(),
        closed_at_ms=2_000,
        price_pnl_quote=1.0,
        entry_fee_quote=0.2,
        exit_fee_quote=0.3,
    )
    assert task is not None
    task["deadline_ms"] = 2_500
    state.enqueue_pending_funding_settlement_reconciliation(task)
    journal = _RuntimeJournal()
    runtime = CloseRuntime(
        SimpleNamespace(
            state=state,
            config=SimpleNamespace(runtime=SimpleNamespace(mode="live")),
            venue_adapters={},
            journal=journal,
        )
    )

    await runtime._process_pending_funding_settlement_reconciliations(now_ms=3_000)

    assert len(state.pending_funding_settlement_reconciliations) == 1
    retained = state.pending_funding_settlement_reconciliations[0]
    assert retained["status"] == "expired_statement_evidence"
    assert retained["official_pnl"] is False
    assert "official_net_quote" not in retained
    assert [kind for kind, _ in journal.records] == [
        "funding.settlement_reconciliation_expired"
    ]
