"""Tests for strategy discovery, scoring, and market view."""

from lightfee.config.schema import StrategyConfig
from lightfee.sidecar.snapshot import CandidateInput, QuoteSnapshot
from lightfee.strategy.discovery import discover_tradeable_candidates
from lightfee.strategy.market_view import (
    compute_raw_cross_bps,
    compute_reference_mid,
    select_maker_leg,
)


FUNDING_TS_MS = 600_000
NOW_IN_SCAN_WINDOW_MS = 0


class TestMarketView:
    def test_reference_mid(self):
        long_q = QuoteSnapshot(venue="a", symbol="BTCUSDT", bid=49900, ask=50000)
        short_q = QuoteSnapshot(venue="b", symbol="BTCUSDT", bid=50100, ask=50200)
        mid = compute_reference_mid(long_q, short_q)
        assert mid == (50000 + 50100) / 2.0

    def test_raw_cross_bps_positive(self):
        long_q = QuoteSnapshot(venue="a", symbol="BTCUSDT", bid=49900, ask=50000)
        short_q = QuoteSnapshot(venue="b", symbol="BTCUSDT", bid=50100, ask=50200)
        cross = compute_raw_cross_bps(long_q, short_q)
        # mid = (50000 + 50100) / 2 = 50050; cross = 100 / 50050 * 10000
        expected = (100.0 / 50050.0) * 10000.0
        assert abs(cross - expected) < 0.01

    def test_select_maker_leg(self):
        long_q = QuoteSnapshot(venue="a", symbol="BTCUSDT", bid=49900, ask=50000)  # spread=100
        short_q = QuoteSnapshot(venue="b", symbol="BTCUSDT", bid=50100, ask=50150)  # spread=50
        assert select_maker_leg(long_q, short_q) == "long"


class TestDiscovery:
    def test_transient_rejection_does_not_mutate_cached_candidate(self):
        candidate = CandidateInput(
            long_venue="binance",
            short_venue="okx",
            symbol="BTCUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=10.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=2.5,
            entry_notional_quote=30.0,
            first_funding_timestamp_ms=FUNDING_TS_MS,
        )
        rejecting = StrategyConfig(
            funding_new_entries_enabled=True,
            min_expected_edge_bps=6.0,
        )
        accepting = StrategyConfig(
            funding_new_entries_enabled=True,
            min_expected_edge_bps=1.0,
        )

        for _ in range(100):
            assert discover_tradeable_candidates([candidate], rejecting, 0) == []
        assert candidate.blocked_reasons == []
        assert discover_tradeable_candidates([candidate], accepting, 0) == [candidate]

    def test_entry_freeze_blocks_only_new_funding_candidates(self):
        config = StrategyConfig(funding_new_entries_enabled=False)
        candidate = CandidateInput(
            long_venue="binance", short_venue="okx", symbol="BTCUSDT",
            funding_diff_bps=10.0, funding_edge_bps=10.0,
            expected_edge_bps=5.0, worst_case_edge_bps=2.0,
            ranking_edge_bps=2.5, entry_notional_quote=30.0,
            first_funding_timestamp_ms=FUNDING_TS_MS,
        )

        assert discover_tradeable_candidates([candidate], config, 0) == []
        assert candidate.blocked_reasons == []

    def test_entry_freeze_treats_non_boolean_config_as_disabled(self):
        config = StrategyConfig(funding_new_entries_enabled="false")  # type: ignore[arg-type]
        candidate = CandidateInput(
            long_venue="binance", short_venue="okx", symbol="BTCUSDT",
            funding_diff_bps=10.0, funding_edge_bps=10.0,
            expected_edge_bps=5.0, worst_case_edge_bps=2.0,
            ranking_edge_bps=2.5, entry_notional_quote=30.0,
            first_funding_timestamp_ms=FUNDING_TS_MS,
        )

        assert discover_tradeable_candidates([candidate], config, 0) == []
        assert candidate.blocked_reasons == []

    def test_live_discovery_requires_complete_economics(self):
        config = StrategyConfig()
        candidate = CandidateInput(
            long_venue="binance", short_venue="okx", symbol="BTCUSDT",
            funding_diff_bps=10.0, funding_edge_bps=10.0,
            expected_edge_bps=5.0, worst_case_edge_bps=2.0,
            ranking_edge_bps=2.5, entry_notional_quote=30.0,
            first_funding_timestamp_ms=FUNDING_TS_MS, economics_complete=False,
        )

        assert discover_tradeable_candidates(
            [candidate], config, 0, require_complete_economics=True
        ) == []
        assert candidate.blocked_reasons == []

    def test_live_discovery_rejects_truthy_non_boolean_economics_flag(self):
        config = StrategyConfig(funding_new_entries_enabled=True)
        candidate = CandidateInput(
            long_venue="binance", short_venue="okx", symbol="BTCUSDT",
            funding_diff_bps=10.0, funding_edge_bps=10.0,
            expected_edge_bps=5.0, worst_case_edge_bps=2.0,
            ranking_edge_bps=2.5, entry_notional_quote=30.0,
            first_funding_timestamp_ms=FUNDING_TS_MS,
            economics_complete="true",  # type: ignore[arg-type]
            economics_observed_at_ms=1,
        )

        assert discover_tradeable_candidates(
            [candidate], config, 0, require_complete_economics=True
        ) == []
        assert candidate.blocked_reasons == []

    def test_filters_below_funding_edge_floor(self):
        config = StrategyConfig(min_funding_edge_bps=6.0, max_concurrent_positions=8)
        candidates = [
            CandidateInput(
                long_venue="binance", short_venue="okx", symbol="BTCUSDT",
                funding_diff_bps=3.0, funding_edge_bps=3.0,
                expected_edge_bps=5.0, worst_case_edge_bps=2.0,
                ranking_edge_bps=2.5,
            )
        ]
        result = discover_tradeable_candidates(candidates, config, 0)
        assert len(result) == 0

    def test_passes_above_all_floors(self):
        config = StrategyConfig(
            min_funding_edge_bps=6.0, min_expected_edge_bps=1.0, min_worst_case_edge_bps=0.0,
            max_concurrent_positions=8, funding_new_entries_enabled=True,
        )
        candidates = [
            CandidateInput(
                long_venue="binance", short_venue="okx", symbol="BTCUSDT",
                funding_diff_bps=10.0, funding_edge_bps=10.0,
                expected_edge_bps=5.0, worst_case_edge_bps=2.0,
                ranking_edge_bps=2.5, entry_notional_quote=100.0,
                first_funding_timestamp_ms=FUNDING_TS_MS,  # V1 parity: required for tradeable
            )
        ]
        result = discover_tradeable_candidates(candidates, config, NOW_IN_SCAN_WINDOW_MS)
        assert len(result) == 1

    def test_static_economics_filter_but_notional_is_a_sizing_constraint(self):
        config = StrategyConfig(
            funding_new_entries_enabled=True,
            min_funding_edge_bps=0.0,
            min_expected_edge_bps=8.0,
            min_worst_case_edge_bps=3.0,
        )

        def candidate(symbol: str, *, expected: float, worst: float, notional: float):
            return CandidateInput(
                long_venue="bybit",
                short_venue="okx",
                symbol=symbol,
                funding_diff_bps=20.0,
                funding_edge_bps=20.0,
                expected_edge_bps=expected,
                worst_case_edge_bps=worst,
                ranking_edge_bps=20.0,
                entry_notional_quote=notional,
                first_funding_timestamp_ms=FUNDING_TS_MS,
            )

        low_expected = candidate("LOW_EXPECTED", expected=7.99, worst=4.0, notional=30.0)
        low_worst = candidate("LOW_WORST", expected=9.0, worst=2.99, notional=30.0)
        oversized = candidate("OVERSIZED", expected=9.0, worst=4.0, notional=30.01)
        eligible = candidate("ELIGIBLE", expected=9.0, worst=4.0, notional=30.0)

        result = discover_tradeable_candidates(
            [low_expected, low_worst, oversized, eligible],
            config,
            NOW_IN_SCAN_WINDOW_MS,
        )

        assert result == [oversized, eligible]
        assert low_expected.blocked_reasons == []
        assert low_worst.blocked_reasons == []
        assert oversized.blocked_reasons == []

    def test_ranks_by_ranking_edge_desc(self):
        config = StrategyConfig(max_concurrent_positions=8, funding_new_entries_enabled=True)
        candidates = [
            CandidateInput(
                long_venue="a", short_venue="b", symbol="X",
                funding_diff_bps=10, funding_edge_bps=10,
                expected_edge_bps=5, worst_case_edge_bps=2, ranking_edge_bps=2.0,
                entry_notional_quote=100.0,
                first_funding_timestamp_ms=FUNDING_TS_MS,  # V1 parity: required for tradeable
            ),
            CandidateInput(
                long_venue="a", short_venue="c", symbol="X",
                funding_diff_bps=10, funding_edge_bps=10,
                expected_edge_bps=5, worst_case_edge_bps=2, ranking_edge_bps=5.0,
                entry_notional_quote=100.0,
                first_funding_timestamp_ms=FUNDING_TS_MS,  # V1 parity: required for tradeable
            ),
        ]
        result = discover_tradeable_candidates(candidates, config, NOW_IN_SCAN_WINDOW_MS)
        assert result[0].ranking_edge_bps == 5.0

    def test_preserves_v1_shortlist_pool_beyond_position_capacity(self):
        config = StrategyConfig(max_concurrent_positions=2, funding_new_entries_enabled=True)
        candidates = [
            CandidateInput(
                long_venue="a", short_venue=f"b{i}", symbol=f"S{i}",
                funding_diff_bps=10, funding_edge_bps=10,
                expected_edge_bps=5, worst_case_edge_bps=2,
                ranking_edge_bps=float(10 - i),
                entry_notional_quote=100.0,
                first_funding_timestamp_ms=FUNDING_TS_MS,
            )
            for i in range(5)
        ]

        result = discover_tradeable_candidates(candidates, config, NOW_IN_SCAN_WINDOW_MS)

        assert [c.symbol for c in result] == ["S0", "S1", "S2", "S3", "S4"]

    def test_skips_blocked_candidates(self):
        config = StrategyConfig(max_concurrent_positions=8)
        candidates = [
            CandidateInput(
                long_venue="a", short_venue="b", symbol="X",
                funding_diff_bps=10, funding_edge_bps=10,
                expected_edge_bps=5, worst_case_edge_bps=2, ranking_edge_bps=5.0,
                blocked=True, blocked_reasons=["test"],
            )
        ]
        result = discover_tradeable_candidates(candidates, config, 0)
        assert len(result) == 0

    def test_max_scan_zero_falls_back_to_entry_window(self):
        config = StrategyConfig(
            max_scan_minutes_before_funding=0,
            min_scan_minutes_before_funding=3,
            entry_window_secs=480,
            max_concurrent_positions=8,
            min_funding_edge_bps=0,
        )
        candidate = CandidateInput(
            long_venue="a", short_venue="b", symbol="X",
            funding_diff_bps=10, funding_edge_bps=10,
            expected_edge_bps=5, worst_case_edge_bps=2, ranking_edge_bps=5.0,
            entry_notional_quote=100.0,
            first_funding_timestamp_ms=600_000,
        )

        result = discover_tradeable_candidates([candidate], config, 0)

        assert result == []
        assert candidate.blocked_reasons == []


class TestZeroSizeGate:
    """V2 fix: entry_notional_quote must be non-zero for tradeable candidates."""

    def test_zero_entry_notional_blocked(self):
        config = StrategyConfig(
            max_concurrent_positions=8,
            min_funding_edge_bps=0,
            funding_new_entries_enabled=True,
        )
        candidates = [
            CandidateInput(
                long_venue="a", short_venue="b", symbol="X",
                funding_diff_bps=10, funding_edge_bps=10,
                expected_edge_bps=5, worst_case_edge_bps=2, ranking_edge_bps=5.0,
                entry_notional_quote=0.0,  # V2 fix: zero should be blocked
                first_funding_timestamp_ms=FUNDING_TS_MS,
            )
        ]
        result = discover_tradeable_candidates(candidates, config, 0)
        assert len(result) == 0

    def test_nonzero_entry_notional_passes(self):
        config = StrategyConfig(
            max_concurrent_positions=8,
            min_funding_edge_bps=0,
            funding_new_entries_enabled=True,
        )
        candidates = [
            CandidateInput(
                long_venue="a", short_venue="b", symbol="X",
                funding_diff_bps=10, funding_edge_bps=10,
                expected_edge_bps=5, worst_case_edge_bps=2, ranking_edge_bps=5.0,
                entry_notional_quote=100.0,
                first_funding_timestamp_ms=FUNDING_TS_MS,
            )
        ]
        result = discover_tradeable_candidates(candidates, config, NOW_IN_SCAN_WINDOW_MS)
        assert len(result) == 1
