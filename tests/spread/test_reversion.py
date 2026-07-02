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
    symbol: str = "BTCUSDT",
    bid: float,
    ask: float,
    observed_at_ms: int,
    bid_size: float = 1.0,
    ask_size: float = 1.0,
    funding_rate_bps: float = 0.0,
) -> QuoteSnapshot:
    return QuoteSnapshot(
        venue=venue,
        symbol=symbol,
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
        slippage_reserve_bps=0.0,
        adverse_selection_buffer_bps=0.0,
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
        min_executable_spread_bps=0.0,
        max_executable_spread_bps=0.0,
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
    assert candidate.fee_bps == pytest.approx(5.0)
    assert candidate.slippage_reserve_bps == pytest.approx(8.0)
    assert candidate.net_edge_bps == pytest.approx(
        candidate.executable_spread_bps
        - candidate.fee_bps
        - candidate.slippage_reserve_bps
        - candidate.adverse_selection_buffer_bps
        - candidate.funding_carry_bps
    )


def test_spread_reversion_respects_executable_spread_window() -> None:
    tracker = SpreadStatsTracker()
    cfg = SpreadReversionConfig(
        min_samples=1,
        min_history_ms=0,
        min_fair_price_confidence=0.0,
        min_liquidity_capacity_ratio=1.0,
        entry_z=0.0,
        min_net_edge_bps=-1_000.0,
        min_executable_spread_bps=50.0,
        max_executable_spread_bps=300.0,
        slippage_reserve_bps=0.0,
        adverse_selection_buffer_bps=0.0,
    )
    now_ms = 55_000

    low = build_spread_reversion_candidates(
        {
            "cheap:BTCUSDT": _quote("cheap", bid=99.9, ask=100.0, observed_at_ms=now_ms),
            "rich:BTCUSDT": _quote("rich", bid=100.3, ask=100.4, observed_at_ms=now_ms),
        },
        ["BTCUSDT"],
        tracker=tracker,
        config=cfg,
        now_ms=now_ms,
    )
    middle = build_spread_reversion_candidates(
        {
            "cheap:ETHUSDT": _quote(
                "cheap",
                symbol="ETHUSDT",
                bid=99.9,
                ask=100.0,
                observed_at_ms=now_ms,
            ),
            "rich:ETHUSDT": _quote(
                "rich",
                symbol="ETHUSDT",
                bid=100.8,
                ask=100.9,
                observed_at_ms=now_ms,
            ),
        },
        ["ETHUSDT"],
        tracker=tracker,
        config=cfg,
        now_ms=now_ms,
    )
    high = build_spread_reversion_candidates(
        {
            "cheap:SOLUSDT": _quote(
                "cheap",
                symbol="SOLUSDT",
                bid=99.9,
                ask=100.0,
                observed_at_ms=now_ms,
            ),
            "rich:SOLUSDT": _quote(
                "rich",
                symbol="SOLUSDT",
                bid=104.0,
                ask=104.1,
                observed_at_ms=now_ms,
            ),
        },
        ["SOLUSDT"],
        tracker=tracker,
        config=cfg,
        now_ms=now_ms,
    )

    assert low == []
    assert len(middle) == 1
    assert middle[0].executable_spread_bps >= 50.0
    assert high == []


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


def test_spread_reversion_allows_enabled_single_venue_dislocation_for_paper() -> None:
    tracker = SpreadStatsTracker()
    cfg = SpreadReversionConfig(
        min_samples=1,
        min_history_ms=0,
        min_fair_price_confidence=1.0,
        min_liquidity_capacity_ratio=1.0,
        entry_z=0.0,
        min_net_edge_bps=5.0,
        signal_ttl_ms=20_000,
        quote_skew_ms=2_000,
        live_notional_quote=20.0,
        max_gross_quote=50.0,
        slippage_reserve_bps=0.0,
        adverse_selection_buffer_bps=0.0,
        fair_price_max_venue_premium_bps=150.0,
        fair_price_min_venues=3,
        single_venue_dislocation_enabled=True,
        single_venue_dislocation_min_anchor_venues=3,
    )
    now_ms = 65_000
    quotes = {
        "gate:LABUSDT": _quote(
            "gate",
            symbol="LABUSDT",
            bid=11.98,
            ask=12.00,
            observed_at_ms=now_ms - 100,
            bid_size=20.0,
            ask_size=20.0,
        ),
        "okx:LABUSDT": _quote(
            "okx",
            symbol="LABUSDT",
            bid=11.99,
            ask=12.01,
            observed_at_ms=now_ms - 150,
            bid_size=20.0,
            ask_size=20.0,
        ),
        "binance:LABUSDT": _quote(
            "binance",
            symbol="LABUSDT",
            bid=12.02,
            ask=12.04,
            observed_at_ms=now_ms - 120,
            bid_size=20.0,
            ask_size=20.0,
        ),
        "bybit:LABUSDT": _quote(
            "bybit",
            symbol="LABUSDT",
            bid=12.38,
            ask=12.39,
            observed_at_ms=now_ms - 1_000,
            bid_size=20.0,
            ask_size=20.0,
        ),
    }

    candidates = build_spread_reversion_candidates(
        quotes,
        ["LABUSDT"],
        tracker=tracker,
        config=cfg,
        now_ms=now_ms,
    )

    candidate = next(c for c in candidates if c.short_venue == "bybit")
    assert candidate.long_venue == "gate"
    assert candidate.opportunity_label == "single_venue_dislocation"
    assert "fair_outlier_override" in candidate.screening_reasons
    assert candidate.fair_price_confidence == pytest.approx(1.0)
    assert candidate.net_edge_bps > 5.0


def test_single_venue_dislocation_does_not_wait_for_rolling_zscore() -> None:
    tracker = SpreadStatsTracker()
    cfg = SpreadReversionConfig(
        min_samples=120,
        min_history_ms=300_000,
        min_fair_price_confidence=1.0,
        min_liquidity_capacity_ratio=1.0,
        entry_z=2.0,
        min_net_edge_bps=5.0,
        signal_ttl_ms=20_000,
        quote_skew_ms=2_000,
        live_notional_quote=20.0,
        max_gross_quote=50.0,
        slippage_reserve_bps=0.0,
        adverse_selection_buffer_bps=0.0,
        fair_price_max_venue_premium_bps=150.0,
        fair_price_min_venues=3,
        single_venue_dislocation_enabled=True,
        single_venue_dislocation_min_anchor_venues=3,
    )
    now_ms = 65_000
    quotes = {
        "gate:LABUSDT": _quote(
            "gate",
            symbol="LABUSDT",
            bid=11.98,
            ask=12.00,
            observed_at_ms=now_ms - 100,
            bid_size=20.0,
            ask_size=20.0,
        ),
        "okx:LABUSDT": _quote(
            "okx",
            symbol="LABUSDT",
            bid=11.99,
            ask=12.01,
            observed_at_ms=now_ms - 150,
            bid_size=20.0,
            ask_size=20.0,
        ),
        "binance:LABUSDT": _quote(
            "binance",
            symbol="LABUSDT",
            bid=12.02,
            ask=12.04,
            observed_at_ms=now_ms - 120,
            bid_size=20.0,
            ask_size=20.0,
        ),
        "bybit:LABUSDT": _quote(
            "bybit",
            symbol="LABUSDT",
            bid=12.38,
            ask=12.39,
            observed_at_ms=now_ms - 1_000,
            bid_size=20.0,
            ask_size=20.0,
        ),
    }

    candidates = build_spread_reversion_candidates(
        quotes,
        ["LABUSDT"],
        tracker=tracker,
        config=cfg,
        now_ms=now_ms,
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.opportunity_label == "single_venue_dislocation"
    assert candidate.signal_status == "entry_ready"
    assert candidate.sample_count == 1
    assert candidate.z_score == 0.0


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


def test_spread_scoring_prioritizes_large_capacity_over_extreme_thin_zscore() -> None:
    tracker = SpreadStatsTracker()
    cfg = SpreadReversionConfig(
        min_samples=120,
        min_history_ms=0,
        min_fair_price_confidence=0.0,
        min_liquidity_capacity_ratio=1.25,
        entry_z=0.0,
        min_net_edge_bps=0.0,
        slippage_reserve_bps=0.0,
        adverse_selection_buffer_bps=0.0,
        live_notional_quote=20.0,
        max_gross_quote=20.0,
        signal_ttl_ms=1_000,
        quote_skew_ms=250,
    )

    def pair(
        symbol: str,
        spread_bps: float,
        capacity_quote: float,
        now_ms: int,
    ) -> dict[str, QuoteSnapshot]:
        cheap_ask = 100.0
        rich_bid = cheap_ask * (1.0 + spread_bps / 10_000.0)
        top_size = capacity_quote / cheap_ask
        return {
            f"cheap:{symbol}": _quote(
                "cheap",
                symbol=symbol,
                bid=cheap_ask - 0.01,
                ask=cheap_ask,
                observed_at_ms=now_ms,
                ask_size=top_size,
            ),
            f"rich:{symbol}": _quote(
                "rich",
                symbol=symbol,
                bid=rich_bid,
                ask=rich_bid + 0.01,
                observed_at_ms=now_ms,
                bid_size=top_size,
            ),
        }

    candidates = []
    for i in range(120):
        now_ms = 500_000 + i
        thin_spread = 8.0 if i < 119 else 70.0
        deep_spread = 8.0 if i < 119 else 30.0
        candidates = build_spread_reversion_candidates(
            {
                **pair("THINUSDT", thin_spread, 45.0, now_ms),
                **pair("DEEPUSDT", deep_spread, 600.0, now_ms),
            },
            ["THINUSDT", "DEEPUSDT"],
            tracker=tracker,
            config=cfg,
            now_ms=now_ms,
        )

    assert [candidate.symbol for candidate in candidates[:2]] == ["DEEPUSDT", "THINUSDT"]
    thin = next(candidate for candidate in candidates if candidate.symbol == "THINUSDT")
    deep = next(candidate for candidate in candidates if candidate.symbol == "DEEPUSDT")
    assert thin.capacity_quote < 50.0
    assert deep.capacity_quote >= 500.0
    assert thin.z_score > cfg.score_z_cap
