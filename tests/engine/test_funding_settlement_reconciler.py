from __future__ import annotations

from types import SimpleNamespace

import pytest

from lightfee.core.domain import FundingSettlement, Venue
from lightfee.engine.close_executor import CloseExecutor
from lightfee.engine.close_runtime import CloseRuntime
from lightfee.engine.exit import CloseExecution
from lightfee.engine.recovery import (
    _restore_state_from_snapshot_dict,
    build_persistent_state_view,
)
from lightfee.engine.state import EngineState, OpenPosition
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
