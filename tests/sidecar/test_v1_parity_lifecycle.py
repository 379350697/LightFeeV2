"""Tests for V1 parity: per-domain lifecycle, last-good fallback, per-symbol degradation."""

from __future__ import annotations

import json

import pytest

from lightfee.sidecar.publisher import load_snapshot, publish_snapshot
from lightfee.sidecar.snapshot import (
    CandidateInput,
    FundingLifecycle,
    LiquidityLifecycle,
    MarketLifecycle,
    QuoteSnapshot,
    SidecarSnapshot,
    SnapshotFreshness,
    TransferLifecycle,
    evaluate_snapshot_freshness,
)


class TestLifecycleCoverageDegradation:
    """Lifecycle dataclasses must carry coverage_usable and degraded_reason."""

    def test_funding_lifecycle_has_coverage_and_reason(self):
        fl = FundingLifecycle(venue="binance", observed_at_ms=1000, symbol_count=5,
                              coverage_usable=5, degraded_reason="")
        assert fl.coverage_usable == 5
        assert fl.degraded_reason == ""

    def test_funding_lifecycle_degraded(self):
        fl = FundingLifecycle(venue="okx", observed_at_ms=1000, symbol_count=3,
                              coverage_usable=0, degraded_reason="timeout")
        assert fl.coverage_usable == 0
        assert fl.degraded_reason == "timeout"

    def test_market_lifecycle_has_coverage_and_reason(self):
        ml = MarketLifecycle(venue="bybit", observed_at_ms=1000, symbol_count=3,
                             coverage_usable=2, degraded_reason="BTCUSDT: fetch failed")
        assert ml.coverage_usable == 2

    def test_liquidity_lifecycle_has_coverage_and_reason(self):
        ll = LiquidityLifecycle(venue="gate", observed_at_ms=1000, symbol_count=0,
                                coverage_usable=0, degraded_reason="venue degraded")
        assert ll.coverage_usable == 0

    def test_transfer_lifecycle_has_coverage_and_reason(self):
        tl = TransferLifecycle(from_venue="binance", to_venue="okx", observed_at_ms=1000,
                               coverage_usable=0, degraded_reason="")
        assert tl.coverage_usable == 0


class TestDegradedSymbols:
    """SidecarSnapshot must track per-symbol degradation."""

    def test_degraded_symbols_default_empty(self):
        s = SidecarSnapshot()
        assert s.degraded_symbols == {}

    def test_degraded_symbols_persist(self):
        s = SidecarSnapshot(degraded_symbols={"binance": ["BTCUSDT", "ETHUSDT"]})
        assert s.degraded_symbols["binance"] == ["BTCUSDT", "ETHUSDT"]

    def test_degraded_symbols_roundtrip(self):
        import tempfile
        from pathlib import Path
        s = SidecarSnapshot(
            degraded_symbols={"okx": ["BTCUSDT"]},
            degraded_venues=["gate"],
        )
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "snap.json"
            publish_snapshot(s, path)
            loaded = load_snapshot(path)
            assert loaded is not None
            assert loaded.degraded_symbols == {"okx": ["BTCUSDT"]}
            assert loaded.degraded_venues == ["gate"]

    def test_freshness_degrades_with_symbol_failures(self):
        s = SidecarSnapshot(
            published_at_ms=1000,
            degraded_symbols={"binance": ["BTCUSDT"]},
        )
        result = evaluate_snapshot_freshness(s, max_age_ms=5000, now_ms=2000)
        assert result == SnapshotFreshness.DEGRADED


class TestLastGoodFallback:
    """Last-good quotes injection must preserve candidates across transient failures."""

    def test_inject_last_good_returns_previous_quotes(self):
        from lightfee.sidecar.service import SidecarService
        quotes = {
            "binance:BTCUSDT": QuoteSnapshot(venue="binance", symbol="BTCUSDT",
                                              bid=50000, ask=50001, funding_rate_bps=10),
        }
        svc = object.__new__(SidecarService)
        svc._last_good_quotes = quotes
        svc._last_good_at_ms = 1000

        result = svc._inject_last_good("binance", ["BTCUSDT"])
        assert len(result) == 1
        assert result["binance:BTCUSDT"].bid == 50000

    def test_inject_last_good_empty_when_no_cache(self):
        from lightfee.sidecar.service import SidecarService
        svc = object.__new__(SidecarService)
        svc._last_good_quotes = {}
        svc._last_good_at_ms = 0

        result = svc._inject_last_good("binance", ["BTCUSDT"])
        assert len(result) == 0

    def test_inject_last_good_only_matching_venue(self):
        from lightfee.sidecar.service import SidecarService
        quotes = {
            "binance:BTCUSDT": QuoteSnapshot(venue="binance", symbol="BTCUSDT", bid=1, ask=2),
            "okx:BTCUSDT": QuoteSnapshot(venue="okx", symbol="BTCUSDT", bid=3, ask=4),
        }
        svc = object.__new__(SidecarService)
        svc._last_good_quotes = quotes
        svc._last_good_at_ms = 1000

        result = svc._inject_last_good("binance", ["BTCUSDT"])
        assert len(result) == 1
        assert "binance:BTCUSDT" in result
        assert "okx:BTCUSDT" not in result


class TestSnapshotRoundTripWithAllV1Fields:
    """Full snapshot round-trip preserves all V1 parity lifecycle fields."""

    def test_round_trip_lifecycle_coverage(self):
        import tempfile
        from pathlib import Path
        s = SidecarSnapshot(
            published_at_ms=1000,
            funding_lifecycle=[
                FundingLifecycle(venue="a", observed_at_ms=1000, symbol_count=10,
                                 coverage_usable=10, degraded_reason=""),
                FundingLifecycle(venue="b", observed_at_ms=1000, symbol_count=0,
                                 coverage_usable=0, degraded_reason="timeout"),
            ],
            market_lifecycle=[
                MarketLifecycle(venue="a", observed_at_ms=1000, symbol_count=10,
                                coverage_usable=8, degraded_reason="BTCUSDT: fetch failed; ETHUSDT: fetch failed"),
            ],
            liquidity_lifecycle=[
                LiquidityLifecycle(venue="a", observed_at_ms=1000, symbol_count=8,
                                   coverage_usable=8, degraded_reason=""),
            ],
            degraded_symbols={"a": ["BTCUSDT", "ETHUSDT"]},
        )
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "snap.json"
            publish_snapshot(s, path)
            loaded = load_snapshot(path)
            assert loaded is not None
            assert len(loaded.funding_lifecycle) == 2
            fl_a = loaded.funding_lifecycle[0]
            assert fl_a.coverage_usable == 10
            assert fl_a.degraded_reason == ""
            fl_b = loaded.funding_lifecycle[1]
            assert fl_b.coverage_usable == 0
            assert fl_b.degraded_reason == "timeout"
            ml = loaded.market_lifecycle[0]
            assert ml.coverage_usable == 8
            assert "BTCUSDT" in ml.degraded_reason
            ll = loaded.liquidity_lifecycle[0]
            assert ll.coverage_usable == 8
            assert loaded.degraded_symbols == {"a": ["BTCUSDT", "ETHUSDT"]}


class TestAcquisitionModeReflectsDegradation:
    """When degraded_venues present, acquisition_mode should indicate fallback."""

    def test_acquisition_mode_fresh_when_clean(self):
        s = SidecarSnapshot(acquisition_mode="fresh_sidecar", degraded_venues=[])
        assert s.acquisition_mode == "fresh_sidecar"

    def test_acquisition_mode_can_indicate_last_good(self):
        s = SidecarSnapshot(acquisition_mode="last_good_sidecar", degraded_venues=["x"])
        assert s.acquisition_mode == "last_good_sidecar"
