"""Tests for strategy discovery, scoring, and market view."""

import pytest

from lightfee.config.schema import StrategyConfig
from lightfee.sidecar.snapshot import CandidateInput, QuoteSnapshot
from lightfee.strategy.discovery import BlockReason, discover_tradeable_candidates
from lightfee.strategy.market_view import (
    compute_raw_cross_bps,
    compute_reference_mid,
    select_maker_leg,
)
from lightfee.strategy.scoring import (
    compute_expected_edge_bps,
    compute_ranking_edge_bps,
    compute_worst_case_edge_bps,
)
from lightfee.strategy.transfer_bias import TransferState, evaluate_transfer_bias


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


class TestScoring:
    def test_expected_edge_bps(self):
        config = StrategyConfig()
        edge = compute_expected_edge_bps(
            funding_edge_bps=10.0,
            cross_bps=2.0,
            long_fee_bps=0.5,
            short_fee_bps=0.5,
            long_slippage_bps=1.0,
            short_slippage_bps=1.0,
            config=config,
        )
        # 10 + 2 - (0.5+0.5)*2 - (1+1)*2 - 3 - 1 = 12 - 2 - 4 - 4 = 2
        assert abs(edge - 2.0) < 0.01

    def test_worst_case_edge(self):
        config = StrategyConfig(execution_buffer_bps=2.0)
        worst = compute_worst_case_edge_bps(5.0, config)
        assert worst == 3.0

    def test_ranking_edge(self):
        rank = compute_ranking_edge_bps(3.0, 0.5)
        assert rank == 3.5


class TestDiscovery:
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
            max_concurrent_positions=8,
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

    def test_ranks_by_ranking_edge_desc(self):
        config = StrategyConfig(max_concurrent_positions=8)
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


class TestZeroSizeGate:
    """V2 fix: entry_notional_quote must be non-zero for tradeable candidates."""

    def test_zero_entry_notional_blocked(self):
        config = StrategyConfig(max_concurrent_positions=8, min_funding_edge_bps=0)
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
        config = StrategyConfig(max_concurrent_positions=8, min_funding_edge_bps=0)
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


class TestTransferBias:
    def test_clear_transfer_positive_bias(self):
        config = StrategyConfig(transfer_healthy_bias_bps=0.25)
        assert evaluate_transfer_bias(TransferState.CLEAR, config) == 0.25

    def test_degraded_transfer_negative_bias(self):
        config = StrategyConfig(transfer_degraded_bias_bps=-0.5)
        assert evaluate_transfer_bias(TransferState.DEGRADED, config) == -0.5
