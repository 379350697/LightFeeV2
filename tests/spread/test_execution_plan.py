from __future__ import annotations

import pytest

from lightfee.core.domain import Side, Venue
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.execution_plan import (
    SpreadExecutionPlanner,
    SpreadExecutionPlanError,
)
from lightfee.spread.models import SpreadOrderIntent, SpreadPosition


def _quote(
    venue: str,
    *,
    bid: float,
    ask: float,
    observed_at_ms: int,
    bid_size: float = 1.0,
    ask_size: float = 1.0,
) -> QuoteSnapshot:
    return QuoteSnapshot(
        venue=venue,
        symbol="BTCUSDT",
        bid=bid,
        ask=ask,
        bid_size=bid_size,
        ask_size=ask_size,
        observed_at_ms=observed_at_ms,
        funding_timestamp_ms=0,
    )


def test_entry_plan_reuses_order_request_contract_without_submitting() -> None:
    planner = SpreadExecutionPlanner(signal_ttl_ms=1_000)
    intent = SpreadOrderIntent(
        candidate_id="spread:BTCUSDT:cheap->rich",
        symbol="BTCUSDT",
        long_venue="binance",
        short_venue="okx",
        entry_notional_quote=20.0,
        reason="spread_entry_allowed",
    )

    plan = planner.build_entry_plan(
        intent,
        quotes={
            "binance:BTCUSDT": _quote("binance", bid=99.9, ask=100.0, observed_at_ms=10_000),
            "okx:BTCUSDT": _quote("okx", bid=101.0, ask=101.1, observed_at_ms=10_000),
        },
        now_ms=10_100,
    )

    assert plan.strategy_bucket == "spread_reversion"
    assert plan.long_request.venue == Venue.BINANCE
    assert plan.long_request.side == Side.BUY
    assert plan.long_request.quantity == pytest.approx(0.2)
    assert plan.long_request.price_hint == pytest.approx(100.0)
    assert plan.long_request.reduce_only is False
    assert plan.short_request.venue == Venue.OKX
    assert plan.short_request.side == Side.SELL
    assert plan.short_request.quantity == pytest.approx(20.0 / 101.0)
    assert plan.short_request.price_hint == pytest.approx(101.0)
    assert plan.short_request.reduce_only is False
    assert plan.long_request.client_order_id.startswith("lf-spread-entry-long-")
    assert plan.short_request.client_order_id.startswith("lf-spread-entry-short-")


def test_exit_plan_uses_reduce_only_reverse_legs() -> None:
    planner = SpreadExecutionPlanner(signal_ttl_ms=1_000)
    position = SpreadPosition(
        position_id="pos-1",
        symbol="BTCUSDT",
        long_venue="binance",
        short_venue="okx",
        entry_spread_bps=40.0,
        entry_z_score=2.2,
        entry_notional_quote=20.0,
        opened_at_ms=1_000,
    )

    plan = planner.build_exit_plan(
        position,
        quotes={
            "binance:BTCUSDT": _quote("binance", bid=100.5, ask=100.6, observed_at_ms=20_000),
            "okx:BTCUSDT": _quote("okx", bid=100.7, ask=100.8, observed_at_ms=20_000),
        },
        now_ms=20_100,
        reason="spread_converged",
    )

    assert plan.long_request.side == Side.SELL
    assert plan.long_request.reduce_only is True
    assert plan.long_request.price_hint == pytest.approx(100.5)
    assert plan.short_request.side == Side.BUY
    assert plan.short_request.reduce_only is True
    assert plan.short_request.price_hint == pytest.approx(100.8)
    assert plan.reason == "spread_converged"


def test_execution_plan_blocks_stale_or_insufficient_quote_capacity() -> None:
    planner = SpreadExecutionPlanner(signal_ttl_ms=50)
    intent = SpreadOrderIntent(
        candidate_id="spread:BTCUSDT:cheap->rich",
        symbol="BTCUSDT",
        long_venue="binance",
        short_venue="okx",
        entry_notional_quote=20.0,
        reason="spread_entry_allowed",
    )

    with pytest.raises(SpreadExecutionPlanError, match="spread_quote_stale"):
        planner.build_entry_plan(
            intent,
            quotes={
                "binance:BTCUSDT": _quote("binance", bid=99.9, ask=100.0, observed_at_ms=10_000),
                "okx:BTCUSDT": _quote("okx", bid=101.0, ask=101.1, observed_at_ms=10_000),
            },
            now_ms=10_100,
        )

    with pytest.raises(SpreadExecutionPlanError, match="spread_quote_capacity_below_notional"):
        planner.build_entry_plan(
            intent,
            quotes={
                "binance:BTCUSDT": _quote(
                    "binance",
                    bid=99.9,
                    ask=100.0,
                    ask_size=0.01,
                    observed_at_ms=11_000,
                ),
                "okx:BTCUSDT": _quote("okx", bid=101.0, ask=101.1, observed_at_ms=11_000),
            },
            now_ms=11_000,
        )
