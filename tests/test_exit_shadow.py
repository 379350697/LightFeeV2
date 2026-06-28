from __future__ import annotations

import pytest

from lightfee.core.domain import Venue
from lightfee.engine.exit_shadow import (
    ExitShadowConfig,
    ExitShadowMarket,
    ExitShadowQuote,
    ExitShadowSnapshot,
    ExitShadowTracker,
    evaluate_exit_shadow_strategies,
)
from lightfee.engine.state import OpenPosition
from lightfee.marketdata.l2 import L2BookStatus, L2PoolAssignment, LocalL2Book, PriceLevel


def _position() -> OpenPosition:
    return OpenPosition(
        position_id="pos-shadow",
        symbol="BTCUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.ASTER,
        long_quantity=0.01,
        short_quantity=0.01,
        long_entry_price=100.0,
        short_entry_price=101.0,
        matched_quantity=0.01,
        opened_at_ms=1_000,
    )


def _book(venue: str, *, bid_size: float, ask_size: float, observed_at_ms: int = 10_000):
    return LocalL2Book(
        venue=venue,
        symbol="BTCUSDT",
        bids=[
            PriceLevel(100.0, bid_size),
            PriceLevel(99.9, bid_size / 2),
            PriceLevel(99.8, bid_size / 4),
        ],
        asks=[
            PriceLevel(100.1, ask_size),
            PriceLevel(100.2, ask_size / 2),
            PriceLevel(100.3, ask_size / 4),
        ],
        status=L2BookStatus.HOT,
        pool=L2PoolAssignment.HOT_EXEC,
        observed_at_ms=observed_at_ms,
        max_depth=3,
    )


def _market(now_ms: int = 10_100) -> ExitShadowMarket:
    return ExitShadowMarket(
        long_quote=ExitShadowQuote(
            venue="binance",
            symbol="BTCUSDT",
            bid=100.0,
            ask=100.1,
            bid_size=12.0,
            ask_size=3.0,
            observed_at_ms=10_000,
        ),
        short_quote=ExitShadowQuote(
            venue="aster",
            symbol="BTCUSDT",
            bid=100.2,
            ask=100.3,
            bid_size=10.0,
            ask_size=4.0,
            observed_at_ms=10_000,
        ),
        long_book=_book("binance", bid_size=12.0, ask_size=3.0),
        short_book=_book("aster", bid_size=10.0, ask_size=4.0),
        now_ms=now_ms,
    )


def test_exit_shadow_bots_emit_direction_and_recommended_path():
    snapshot = ExitShadowSnapshot(position=_position(), reason="funding_capture", market=_market())

    decisions = evaluate_exit_shadow_strategies(snapshot, ExitShadowConfig(enabled=True))

    by_bot = {decision.bot_id: decision for decision in decisions}
    assert by_bot["top_book_imbalance"].direction == "bullish"
    assert by_bot["top_book_imbalance"].recommended_path == "short_first_then_long"
    assert by_bot["multi_level_l2_imbalance"].direction == "bullish"
    assert by_bot["micro_mid_momentum"].direction == "neutral"
    assert by_bot["micro_mid_momentum"].reason == "missing_mid_momentum_history"
    assert by_bot["cross_venue_pressure"].direction == "bullish"
    assert by_bot["cost_aware_vote"].direction == "bullish"
    assert all(decision.confidence > 0.0 for decision in decisions)


def test_exit_shadow_stale_market_data_forces_neutral_decisions():
    stale = _market(now_ms=20_000)
    snapshot = ExitShadowSnapshot(position=_position(), reason="funding_capture", market=stale)

    decisions = evaluate_exit_shadow_strategies(
        snapshot,
        ExitShadowConfig(enabled=True, max_quote_age_ms=500, max_l2_age_ms=500),
    )

    assert {decision.direction for decision in decisions} == {"neutral"}
    assert {decision.recommended_path for decision in decisions} == {"simultaneous_close"}
    assert all("stale" in decision.reason for decision in decisions)


def test_exit_shadow_tracker_emits_three_path_markouts_and_summary():
    tracker = ExitShadowTracker(ExitShadowConfig(enabled=True, markout_horizons_ms=(1000,)))
    snapshot = ExitShadowSnapshot(position=_position(), reason="funding_capture", market=_market())
    start_events = tracker.on_close_trigger(snapshot)

    assert [event["kind"] for event in start_events].count("exit_shadow.strategy_decision") == 5
    path_events = [e for e in start_events if e["kind"] == "exit_shadow.path_markout"]
    assert {e["payload"]["path"] for e in path_events} == {
        "simultaneous_close",
        "short_first_then_long",
        "long_first_then_short",
    }
    assert {e["payload"]["horizon_ms"] for e in path_events} == {0}

    future = ExitShadowMarket(
        long_quote=ExitShadowQuote("binance", "BTCUSDT", 101.0, 101.1, 8.0, 5.0, 11_100),
        short_quote=ExitShadowQuote("aster", "BTCUSDT", 101.2, 101.3, 7.0, 5.0, 11_100),
        now_ms=11_100,
    )
    due_events = tracker.evaluate_markouts(future)

    due_path_events = [e for e in due_events if e["kind"] == "exit_shadow.path_markout"]
    assert {e["payload"]["horizon_ms"] for e in due_path_events} == {1000}
    assert {e["payload"]["path"] for e in due_path_events} == {
        "simultaneous_close",
        "short_first_then_long",
        "long_first_then_short",
    }
    assert [e["kind"] for e in due_events].count("exit_shadow.strategy_summary") == 5
