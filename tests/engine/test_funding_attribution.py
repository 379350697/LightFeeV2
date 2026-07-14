from __future__ import annotations

from dataclasses import replace

import pytest

from lightfee.core.domain import Venue
from lightfee.engine.close_executor import build_exit_pnl_attribution
from lightfee.engine.exit import CloseExecution
from lightfee.engine.recovery import _deserialize_open_position, _serialize_open_position
from lightfee.engine.state import FundingSettlementRecord, OpenPosition
from lightfee.strategy.attribution import StrategyAttributionService


def _position(**overrides: object) -> OpenPosition:
    values: dict[str, object] = {
        "position_id": "funding-pos-1",
        "symbol": "BTCUSDT",
        "long_venue": Venue.BINANCE,
        "short_venue": Venue.OKX,
        "long_quantity": 1.0,
        "short_quantity": 1.0,
        "long_entry_price": 100.0,
        "short_entry_price": 100.0,
        "opened_at_ms": 1,
        "funding_timestamp_ms": 1_000,
        "long_funding_timestamp_ms": 1_000,
        "short_funding_timestamp_ms": 1_000,
        "funding_captured": True,
        "captured_funding_quote": 2.0,
    }
    values.update(overrides)
    return OpenPosition(**values)


def _record(leg: str, venue: str, amount_quote: float, *, timestamp: int = 1_000) -> FundingSettlementRecord:
    return FundingSettlementRecord(
        leg=leg,
        venue=venue,
        settlement_timestamp_ms=timestamp,
        amount_quote=amount_quote,
        observed_at_ms=1_050,
        source="exchange_statement",
        statement_reference=f"{venue}-{leg}-{timestamp}",
    )


def test_lifecycle_estimate_is_never_reported_as_official_without_statement_facts() -> None:
    position = _position()

    attribution = StrategyAttributionService.refresh(position)

    assert attribution.evidence_status == "missing"
    assert attribution.official is False
    assert position.settled_funding_quote == 0.0

    close = CloseExecution(
        position_id=position.position_id,
        reason="funding_capture",
        long_close_price=100.0,
        short_close_price=100.0,
        long_close_qty=1.0,
        short_close_qty=1.0,
        long_fee_quote=0.0,
        short_fee_quote=0.0,
        realized_price_pnl_quote=0.0,
        funding_pnl_quote=2.0,
        net_quote=2.0,
    )
    pnl = build_exit_pnl_attribution(position, close)
    assert pnl["funding_quote"] == 2.0  # exact V1 lifecycle compatibility
    assert pnl["official_pnl"] is False
    assert pnl["official_net_quote"] is None


def test_execution_shortfall_is_an_audit_cost_not_a_v1_net_pnl_rewrite() -> None:
    position = _position(
        execution_benchmark_complete=True,
        entry_implementation_shortfall_quote=1.25,
    )
    close = CloseExecution(
        position_id=position.position_id,
        reason="funding_capture",
        long_close_price=100.0,
        short_close_price=100.0,
        long_close_qty=1.0,
        short_close_qty=1.0,
        long_fee_quote=0.0,
        short_fee_quote=0.0,
        realized_price_pnl_quote=0.0,
        funding_pnl_quote=2.0,
        net_quote=2.0,
        implementation_shortfall_quote=0.75,
    )

    attribution = build_exit_pnl_attribution(position, close)

    assert attribution["implementation_shortfall_quote"] == pytest.approx(2.0)
    assert attribution["execution_benchmark_complete"] is True
    # V1 realised/funding PnL remains the unchanged accounting series.
    assert attribution["net_quote"] == pytest.approx(2.0)


def test_recovery_does_not_coerce_execution_benchmark_evidence() -> None:
    position = _position(execution_benchmark_complete=True)
    persisted = _serialize_open_position(position)
    persisted["execution_benchmark_complete"] = "false"

    restored = _deserialize_open_position(persisted)

    assert restored.execution_benchmark_complete is False


def test_complete_two_leg_statement_settlement_makes_official_pnl_and_roundtrips() -> None:
    position = _position()
    attribution = StrategyAttributionService.record_settlements(
        position,
        [
            _record("long", "binance", -1.0),
            _record("short", "okx", 3.5),
            # A duplicate record must not double count the exchange fact.
            _record("short", "okx", 3.5),
        ],
    )

    assert attribution.evidence_status == "complete"
    assert attribution.official is True
    assert attribution.settled_funding_quote == pytest.approx(2.5)
    assert attribution.forecast_error_quote == pytest.approx(0.5)
    assert len(position.funding_settlement_records) == 2

    restored = _deserialize_open_position(_serialize_open_position(position))
    assert restored.funding_settlement_evidence_status == "complete"
    assert restored.settled_funding_quote == pytest.approx(2.5)
    assert len(restored.funding_settlement_records) == 2


def test_staggered_second_stage_requires_only_settled_legs_not_a_future_leg() -> None:
    position = _position(
        opportunity_type="staggered",
        first_funding_leg="long",
        short_funding_timestamp_ms=2_000,
        second_funding_timestamp_ms=2_000,
    )
    first = StrategyAttributionService.record_settlements(
        position, [_record("long", "binance", 1.2)]
    )

    assert first.official is True
    assert first.required_settlements == (("long", "binance", 1_000),)

    position.second_stage_funding_captured = True
    position.second_stage_funding_quote = 0.8
    second = StrategyAttributionService.refresh(position)
    assert second.evidence_status == "partial"
    assert second.official is False
    second = StrategyAttributionService.record_settlements(
        position, [_record("short", "okx", -0.4, timestamp=2_000)]
    )
    assert second.official is True
    assert second.settled_funding_quote == pytest.approx(0.8)


def test_conflicting_recovered_statement_records_cannot_be_official() -> None:
    position = _position(
        funding_settlement_records=[
            _record("long", "binance", -1.0),
            replace(_record("long", "binance", -1.0), amount_quote=-2.0),
            _record("short", "okx", 3.5),
        ]
    )

    attribution = StrategyAttributionService.refresh(position)

    assert attribution.evidence_status == "conflict"
    assert attribution.official is False


def test_conflicting_statement_arrival_cannot_silently_keep_first_record() -> None:
    position = _position()
    first = StrategyAttributionService.record_settlements(
        position,
        [
            _record("long", "binance", -1.0),
            _record("short", "okx", 3.5),
        ],
    )
    assert first.official is True

    conflicting = StrategyAttributionService.record_settlements(
        position,
        [replace(_record("long", "binance", -1.0), amount_quote=-2.0)],
    )

    assert conflicting.evidence_status == "conflict"
    assert conflicting.official is False
