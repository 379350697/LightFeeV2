"""Task 7: Market data freshness tests.

Rust references:
- src/execution_core/market_data.rs: MarketFreshness, stale/degraded detection
"""

from __future__ import annotations

import pytest

from lightfee.marketdata.freshness import (
    MarketFreshness,
    allows_candidate,
    is_market_data_fresh,
)


class TestIsMarketDataFresh:
    def test_fresh_within_limit(self):
        assert is_market_data_fresh(10000, 3000, 11000)

    def test_stale_exceeds_limit(self):
        assert not is_market_data_fresh(10000, 3000, 14000)

    def test_fresh_at_boundary(self):
        assert is_market_data_fresh(10000, 3000, 13000)

    def test_stale_one_past_boundary(self):
        assert not is_market_data_fresh(10000, 3000, 13001)


class TestAllowsCandidate:
    def test_allows_candidate_when_fresh(self):
        assert allows_candidate(10000, 3000, 11000)

    def test_rejects_candidate_when_stale(self):
        assert not allows_candidate(10000, 3000, 14000)


class TestMarketFreshness:
    def test_classify_fresh_and_stale(self):
        f = MarketFreshness(max_age_ms=3000, now_ms=13000)
        f.evaluate({"binance": 11000, "okx": 8000, "bybit": 12000})
        assert "binance" in f.fresh_venues
        assert "bybit" in f.fresh_venues
        assert "okx" in f.stale_venues  # 8000 → age=5000ms > 3000

    def test_any_stale(self):
        f = MarketFreshness(max_age_ms=3000, now_ms=13000)
        f.evaluate({"binance": 8000, "okx": 12000})
        assert f.any_stale()

    def test_none_stale(self):
        f = MarketFreshness(max_age_ms=3000, now_ms=13000)
        f.evaluate({"binance": 11000, "okx": 12000})
        assert not f.any_stale()

    def test_transfer_stale_to_degraded(self):
        f = MarketFreshness(max_age_ms=3000, now_ms=13000)
        f.evaluate({"binance": 8000, "okx": 12000})
        degraded = f.transfer_stale_venues()
        assert "binance" in degraded
        assert len(f.stale_venues) == 0

    def test_is_venue_degraded(self):
        f = MarketFreshness(degraded_venues=["binance"])
        assert f.is_venue_degraded("binance")
        assert not f.is_venue_degraded("okx")

    def test_duplicate_transfer_no_double_add(self):
        f = MarketFreshness(max_age_ms=3000, now_ms=13000)
        f.evaluate({"binance": 8000})
        f.transfer_stale_venues()  # binance → degraded
        assert len(f.degraded_venues) == 1
        # simulate stale again, re-transfer
        f.evaluate({"binance": 8000})
        f.transfer_stale_venues()
        assert len(f.degraded_venues) == 1  # no duplicate

    def test_degraded_symbols_storage(self):
        f = MarketFreshness(
            degraded_symbols=[("binance", "BTCUSDT"), ("okx", "ETHUSDT")],
        )
        assert ("binance", "BTCUSDT") in f.degraded_symbols
