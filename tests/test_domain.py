"""Tests for core domain contracts matching Rust reference behavior."""

import pytest

from lightfee.core.domain import (
    FundingOpportunityType,
    FundingSnapshot,
    MarketSnapshot,
    PositionSnapshot,
    Side,
    Symbol,
    Venue,
    VenueMarketQuote,
    VenueMarketSnapshot,
)
from lightfee.core.money import compute_notional_drift_pct, floor_to_step, normalize_order_quantity
from lightfee.core.time import age_ms, is_stale, now_ms


class TestVenue:
    def test_all_seven_venues_exist(self):
        for name in ("binance", "okx", "bybit", "bitget", "gate", "aster", "hyperliquid"):
            assert Venue.from_str(name) is not None

    def test_from_str_normalizes_case_and_aliases(self):
        assert Venue.from_str("BINANCE") == Venue.BINANCE
        assert Venue.from_str("gate_io") == Venue.GATE
        assert Venue.from_str("gateio") == Venue.GATE

    def test_from_str_rejects_unknown(self):
        with pytest.raises(ValueError, match="unsupported venue"):
            Venue.from_str("unknown_exchange")

    def test_str_roundtrip(self):
        for venue in Venue:
            assert str(venue) == venue.value


class TestSymbol:
    def test_normalizes_to_uppercase(self):
        s = Symbol(" ethusdt ")
        assert str(s) == "ETHUSDT"

    def test_rejects_blank(self):
        with pytest.raises(ValueError, match="symbol must not be blank"):
            Symbol("   ")

    def test_equality_and_hash(self):
        a = Symbol("BTCUSDT")
        b = Symbol("btcusdt")
        assert a == b
        assert hash(a) == hash(b)
        assert {a, b} == {a}


class TestSide:
    def test_opposite(self):
        assert Side.BUY.opposite() == Side.SELL
        assert Side.SELL.opposite() == Side.BUY

    def test_signed_qty(self):
        assert Side.BUY.signed_qty(2.5) == 2.5
        assert Side.SELL.signed_qty(2.5) == -2.5


class TestFundingOpportunityType:
    def test_supports_aligned_and_staggered(self):
        assert FundingOpportunityType.ALIGNED.value == "aligned"
        assert FundingOpportunityType.STAGGERED.value == "staggered"


class TestDomainDataclasses:
    def test_funding_snapshot_is_frozen(self):
        fs = FundingSnapshot(
            venue=Venue.BINANCE,
            symbol="BTCUSDT",
            funding_rate_bps=10.0,
            funding_timestamp_ms=1000,
            observed_at_ms=2000,
        )
        assert fs.funding_rate_bps == 10.0
        with pytest.raises(Exception):
            fs.funding_rate_bps = 20.0

    def test_market_snapshot(self):
        ms = MarketSnapshot(
            venue=Venue.OKX,
            symbol="ETHUSDT",
            bid=3000.0,
            ask=3001.0,
            observed_at_ms=1000,
        )
        assert ms.bid == 3000.0

    def test_position_snapshot(self):
        ps = PositionSnapshot(
            venue=Venue.BINANCE,
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=0.1,
            entry_price=50000.0,
            observed_at_ms=1000,
        )
        assert ps.side == Side.BUY

    def test_venue_market_snapshot(self):
        quotes = (
            VenueMarketQuote(symbol="BTCUSDT", bid=50000.0, ask=50001.0, bid_size=1.0, ask_size=1.0),
        )
        vms = VenueMarketSnapshot(venue=Venue.BINANCE, observed_at_ms=1000, quotes=quotes)
        assert len(vms.quotes) == 1


class TestMoneyNormalization:
    def test_normalize_order_quantity_floor_to_step(self):
        assert normalize_order_quantity(1.0, 0.001) == 1.0
        result = normalize_order_quantity(0.30000000000000004, 0.1)
        assert abs(result - 0.3) <= 1e-9, f"got {result!r}"

    def test_normalize_rejects_non_finite(self):
        assert normalize_order_quantity(float("inf"), 0.001) == 0.0
        assert normalize_order_quantity(1.0, 0.0) == 0.0
        assert normalize_order_quantity(-1.0, 0.001) == 0.0

    def test_floor_to_step_matches_normalize(self):
        for qty, step in ((1.0, 0.001), (0.30000000000000004, 0.1), (123.456789, 0.01)):
            assert floor_to_step(qty, step) == normalize_order_quantity(qty, step)

    def test_normalize_keeps_exact_decimal_boundaries(self):
        for quantity, step, expected in [(0.3, 0.1, 0.3), (0.29, 0.01, 0.29), (0.21, 0.07, 0.21)]:
            result = normalize_order_quantity(quantity, step)
            assert abs(result - expected) <= 1e-9, (
                f"qty={quantity} step={step} result={result!r} expected={expected}"
            )

    def test_compute_notional_drift_pct(self):
        assert compute_notional_drift_pct(1000.0, 1050.0) == 5.0
        assert compute_notional_drift_pct(1000.0, 1000.0) == 0.0
        assert compute_notional_drift_pct(0.0, 100.0) == 0.0


class TestTime:
    def test_now_ms_is_recent(self):
        import time
        t = now_ms()
        assert abs(t - int(time.time() * 1000)) < 1000

    def test_age_ms(self):
        assert age_ms(1000, 3000) == 2000
        assert age_ms(3000, 1000) == 0

    def test_is_stale(self):
        assert is_stale(1000, 500, wall_clock_now_ms=2000)
        assert not is_stale(1000, 2000, wall_clock_now_ms=2000)
