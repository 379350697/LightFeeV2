from __future__ import annotations

import pytest

from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.reversion import (
    SpreadReversionConfig,
    SpreadStatsTracker,
    build_spread_reversion_candidates,
)


def _quote(
    venue: str,
    *,
    bid: float,
    ask: float,
    observed_at_ms: int,
    bid_size: float = 1.0,
    ask_size: float = 1.0,
    funding_rate_bps: float = 0.0,
) -> QuoteSnapshot:
    return QuoteSnapshot(
        venue=venue,
        symbol="BTCUSDT",
        bid=bid,
        ask=ask,
        observed_at_ms=observed_at_ms,
        bid_size=bid_size,
        ask_size=ask_size,
        funding_rate_bps=funding_rate_bps,
        funding_timestamp_ms=0,
    )


def test_spread_reversion_candidates_do_not_require_funding_timestamps() -> None:
    tracker = SpreadStatsTracker()
    cfg = SpreadReversionConfig(
        min_samples=3,
        min_history_ms=0,
        min_fair_price_confidence=0.0,
        min_liquidity_capacity_ratio=1.0,
        entry_z=1.0,
        min_net_edge_bps=1.0,
        signal_ttl_ms=1_000,
        quote_skew_ms=250,
        live_notional_quote=20.0,
    )

    now_ms = 10_000
    for i, spread in enumerate([8.0, 9.0, 10.0, 11.0], start=1):
        cheap_mid = 100.0
        rich_mid = cheap_mid * (1.0 + spread / 10_000.0)
        quotes = {
            "cheap:BTCUSDT": _quote("cheap", bid=99.99, ask=100.01, observed_at_ms=now_ms - 10),
            "rich:BTCUSDT": _quote(
                "rich",
                bid=rich_mid - 0.01,
                ask=rich_mid + 0.01,
                observed_at_ms=now_ms - 10 + i,
            ),
        }
        candidates = build_spread_reversion_candidates(
            quotes,
            ["BTCUSDT"],
            tracker=tracker,
            config=cfg,
            now_ms=now_ms,
        )

    assert candidates
    candidate = candidates[0]
    assert candidate.strategy_bucket == "spread_reversion"
    assert candidate.long_venue == "cheap"
    assert candidate.short_venue == "rich"
    assert candidate.funding_timestamp_ms == 0
    assert candidate.first_funding_timestamp_ms == 0
    assert candidate.signal_status == "entry_ready"


def test_spread_reversion_blocks_cold_start_without_calling_funding_window() -> None:
    tracker = SpreadStatsTracker()
    cfg = SpreadReversionConfig(
        min_samples=5,
        min_history_ms=0,
        min_fair_price_confidence=0.0,
        min_liquidity_capacity_ratio=1.0,
        entry_z=1.0,
        min_net_edge_bps=0.0,
    )
    now_ms = 20_000
    quotes = {
        "cheap:BTCUSDT": _quote("cheap", bid=99.9, ask=100.0, observed_at_ms=now_ms),
        "rich:BTCUSDT": _quote("rich", bid=100.5, ask=100.6, observed_at_ms=now_ms),
    }

    candidates = build_spread_reversion_candidates(
        quotes,
        ["BTCUSDT"],
        tracker=tracker,
        config=cfg,
        now_ms=now_ms,
    )

    assert candidates == []
    state = tracker.snapshot("BTCUSDT", "cheap", "rich")
    assert state is not None
    assert state.sample_count == 1


def test_spread_reversion_respects_quote_ttl_and_skew() -> None:
    tracker = SpreadStatsTracker()
    cfg = SpreadReversionConfig(
        min_samples=1,
        min_history_ms=0,
        min_fair_price_confidence=0.0,
        min_liquidity_capacity_ratio=1.0,
        entry_z=0.0,
        min_net_edge_bps=0.0,
        signal_ttl_ms=500,
        quote_skew_ms=50,
    )

    stale = build_spread_reversion_candidates(
        {
            "cheap:BTCUSDT": _quote("cheap", bid=99.9, ask=100.0, observed_at_ms=1_000),
            "rich:BTCUSDT": _quote("rich", bid=100.5, ask=100.6, observed_at_ms=1_000),
        },
        ["BTCUSDT"],
        tracker=tracker,
        config=cfg,
        now_ms=2_000,
    )
    assert stale == []

    skewed = build_spread_reversion_candidates(
        {
            "cheap:BTCUSDT": _quote("cheap", bid=99.9, ask=100.0, observed_at_ms=3_000),
            "rich:BTCUSDT": _quote("rich", bid=100.5, ask=100.6, observed_at_ms=3_200),
        },
        ["BTCUSDT"],
        tracker=tracker,
        config=cfg,
        now_ms=3_200,
    )
    assert skewed == []


def test_net_edge_deducts_fees_slippage_and_funding_carry() -> None:
    tracker = SpreadStatsTracker()
    cfg = SpreadReversionConfig(
        min_samples=2,
        min_history_ms=0,
        min_fair_price_confidence=0.0,
        min_liquidity_capacity_ratio=1.0,
        entry_z=0.0,
        min_net_edge_bps=0.0,
        taker_fee_bps_by_venue={"cheap": 1.0, "rich": 1.5},
        slippage_reserve_bps=2.0,
        adverse_selection_buffer_bps=1.0,
        expected_hold_ms=3_600_000,
    )
    now_ms = 50_000
    quotes = {
        "cheap:BTCUSDT": _quote(
            "cheap",
            bid=99.9,
            ask=100.0,
            observed_at_ms=now_ms,
            funding_rate_bps=4.0,
        ),
        "rich:BTCUSDT": _quote(
            "rich",
            bid=101.0,
            ask=101.1,
            observed_at_ms=now_ms,
            funding_rate_bps=12.0,
        ),
    }

    build_spread_reversion_candidates(
        quotes,
        ["BTCUSDT"],
        tracker=tracker,
        config=cfg,
        now_ms=now_ms,
    )
    candidates = build_spread_reversion_candidates(
        quotes,
        ["BTCUSDT"],
        tracker=tracker,
        config=cfg,
        now_ms=now_ms + 1,
    )

    assert candidates
    candidate = candidates[0]
    assert candidate.executable_spread_bps > candidate.net_edge_bps
    assert candidate.fee_bps == pytest.approx(2.5)
    assert candidate.slippage_reserve_bps == pytest.approx(2.0)


def test_spread_reversion_blocks_low_fair_price_confidence() -> None:
    tracker = SpreadStatsTracker()
    cfg = SpreadReversionConfig(
        min_samples=1,
        min_history_ms=0,
        min_fair_price_confidence=1.0,
        min_liquidity_capacity_ratio=1.0,
        entry_z=0.0,
        min_net_edge_bps=0.0,
        slippage_reserve_bps=0.0,
        adverse_selection_buffer_bps=0.0,
    )
    now_ms = 60_000

    candidates = build_spread_reversion_candidates(
        {
            "cheap:BTCUSDT": _quote(
                "cheap",
                bid=99.9,
                ask=100.0,
                observed_at_ms=now_ms,
                bid_size=10.0,
                ask_size=10.0,
            ),
            "rich:BTCUSDT": _quote(
                "rich",
                bid=101.0,
                ask=101.1,
                observed_at_ms=now_ms,
                bid_size=10.0,
                ask_size=10.0,
            ),
        },
        ["BTCUSDT"],
        tracker=tracker,
        config=cfg,
        now_ms=now_ms,
    )

    assert candidates == []


def test_spread_reversion_blocks_missing_top_book_size() -> None:
    tracker = SpreadStatsTracker()
    cfg = SpreadReversionConfig(
        min_samples=1,
        min_history_ms=0,
        min_fair_price_confidence=0.0,
        min_liquidity_capacity_ratio=1.0,
        entry_z=0.0,
        min_net_edge_bps=0.0,
        slippage_reserve_bps=0.0,
        adverse_selection_buffer_bps=0.0,
    )
    now_ms = 70_000

    candidates = build_spread_reversion_candidates(
        {
            "cheap:BTCUSDT": _quote(
                "cheap",
                bid=99.9,
                ask=100.0,
                observed_at_ms=now_ms,
                bid_size=10.0,
                ask_size=0.0,
            ),
            "rich:BTCUSDT": _quote(
                "rich",
                bid=101.0,
                ask=101.1,
                observed_at_ms=now_ms,
                bid_size=10.0,
                ask_size=10.0,
            ),
        },
        ["BTCUSDT"],
        tracker=tracker,
        config=cfg,
        now_ms=now_ms,
    )

    assert candidates == []


def test_spread_reversion_blocks_capacity_below_required_ratio() -> None:
    tracker = SpreadStatsTracker()
    cfg = SpreadReversionConfig(
        min_samples=1,
        min_history_ms=0,
        min_fair_price_confidence=0.0,
        min_liquidity_capacity_ratio=1.25,
        entry_z=0.0,
        min_net_edge_bps=0.0,
        live_notional_quote=20.0,
        max_gross_quote=50.0,
        slippage_reserve_bps=0.0,
        adverse_selection_buffer_bps=0.0,
    )
    now_ms = 80_000

    candidates = build_spread_reversion_candidates(
        {
            "cheap:BTCUSDT": _quote(
                "cheap",
                bid=99.9,
                ask=100.0,
                observed_at_ms=now_ms,
                bid_size=10.0,
                ask_size=0.20,
            ),
            "rich:BTCUSDT": _quote(
                "rich",
                bid=101.0,
                ask=101.1,
                observed_at_ms=now_ms,
                bid_size=0.20,
                ask_size=10.0,
            ),
        },
        ["BTCUSDT"],
        tracker=tracker,
        config=cfg,
        now_ms=now_ms,
    )

    assert candidates == []


def test_spread_reversion_blocks_short_history_even_with_enough_samples() -> None:
    tracker = SpreadStatsTracker()
    cfg = SpreadReversionConfig(
        min_samples=3,
        min_history_ms=300_000,
        min_fair_price_confidence=0.0,
        min_liquidity_capacity_ratio=1.0,
        entry_z=0.0,
        min_net_edge_bps=0.0,
        slippage_reserve_bps=0.0,
        adverse_selection_buffer_bps=0.0,
    )
    base_ms = 90_000
    candidates = []
    for i, spread in enumerate([10.0, 12.0, 14.0], start=1):
        rich_mid = 100.0 * (1.0 + spread / 10_000.0)
        candidates = build_spread_reversion_candidates(
            {
                "cheap:BTCUSDT": _quote(
                    "cheap",
                    bid=99.9,
                    ask=100.0,
                    observed_at_ms=base_ms + i * 1_000,
                    bid_size=10.0,
                    ask_size=10.0,
                ),
                "rich:BTCUSDT": _quote(
                    "rich",
                    bid=rich_mid - 0.01,
                    ask=rich_mid + 0.01,
                    observed_at_ms=base_ms + i * 1_000,
                    bid_size=10.0,
                    ask_size=10.0,
                ),
            },
            ["BTCUSDT"],
            tracker=tracker,
            config=cfg,
            now_ms=base_ms + i * 1_000,
        )

    assert candidates == []
