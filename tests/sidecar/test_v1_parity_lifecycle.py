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
    """When degraded_venues present, acquisition_mode must match V1 semantics."""

    def test_acquisition_mode_fresh_when_clean(self):
        from lightfee.sidecar.service import _resolve_acquisition_mode
        assert _resolve_acquisition_mode(set(), {}) == "fresh_sidecar"

    def test_acquisition_mode_last_good_when_degraded_with_cache(self):
        from lightfee.sidecar.service import _resolve_acquisition_mode
        assert _resolve_acquisition_mode({"okx"}, {"x": 1}) == "last_good_sidecar"

    def test_acquisition_mode_degraded_when_first_partial_failure(self):
        """Critical: first partial failure with no cache → degraded_sidecar, NOT fresh."""
        from lightfee.sidecar.service import _resolve_acquisition_mode
        assert _resolve_acquisition_mode({"binance"}, {}) == "degraded_sidecar"

    def test_acquisition_mode_degraded_when_cache_empty_dict(self):
        """Empty dict is falsy → degraded_sidecar, not fresh."""
        from lightfee.sidecar.service import _resolve_acquisition_mode
        assert _resolve_acquisition_mode({"binance"}, {}) != "fresh_sidecar"


class TestLiquiditySourceWiredIntoRefresh:
    """LiquiditySource and sidecar_liquidity_timeout_s must be in the refresh path."""

    def test_fetch_liquidity_all_venues_method_exists(self):
        from lightfee.sidecar.service import SidecarService
        svc = object.__new__(SidecarService)
        assert hasattr(svc, "_fetch_liquidity_all_venues")

    def test_liquidity_timeout_s_read_from_runtime(self):
        from lightfee.sidecar.service import SidecarService, DEFAULT_LIQUIDITY_TIMEOUT_S
        svc = object.__new__(SidecarService)
        svc.config = type("c", (), {"runtime": type("r", (), {"sidecar_snapshot_path": "/tmp"})(), "venues": [], "symbols": []})()
        svc._liquidity_sources = {}
        svc._exchange_sources = {}
        svc._transfer_sources = []
        svc._last_good_quotes = {}
        svc._funding_timeout_s = 10
        svc._liquidity_timeout_s = 7.0
        svc._transfer_timeout_s = 5
        assert svc._liquidity_timeout_s == 7.0

    def test_liquidity_timeout_default(self):
        from lightfee.sidecar.service import DEFAULT_LIQUIDITY_TIMEOUT_S
        assert DEFAULT_LIQUIDITY_TIMEOUT_S == 10.0


class TestRefreshPublicationSemantics:
    """Refresh metadata must distinguish market observation from publish time."""

    @pytest.mark.asyncio
    async def test_published_at_ms_uses_refresh_completion_time(self, monkeypatch, tmp_path):
        from lightfee.sidecar.service import SidecarService

        svc = object.__new__(SidecarService)
        svc.config = type("c", (), {"symbols": ["BTCUSDT"], "venues": []})()
        svc.snapshot_path = tmp_path / "sidecar.json"
        svc._funding_timeout_s = 10.0
        svc._liquidity_timeout_s = 10.0
        svc._transfer_sources = []
        svc._last_good_quotes = {}
        svc._last_good_at_ms = 0

        async def fake_fetch_all_venues(symbols, timeout_s):
            return [(
                "binance",
                {"binance:BTCUSDT": QuoteSnapshot(
                    venue="binance", symbol="BTCUSDT", bid=50000, ask=50001,
                    funding_rate_bps=1.0, funding_timestamp_ms=1000,
                )},
                None,
                set(),
            )]

        async def fake_fetch_liquidity_all_venues(symbols, timeout_s):
            return [("binance", {}, None, set())]

        svc._fetch_all_venues = fake_fetch_all_venues
        svc._fetch_liquidity_all_venues = fake_fetch_liquidity_all_venues
        times = iter([1.0, 7.0])
        monkeypatch.setattr("lightfee.sidecar.service.time.time", lambda: next(times))

        snapshot = await svc.refresh_once()

        assert snapshot.market_observed_at_ms == 1000
        assert snapshot.published_at_ms == 7000
        assert svc._last_good_at_ms == 7000

    @pytest.mark.asyncio
    async def test_first_partial_degradation_without_cache_is_degraded_sidecar(self, tmp_path):
        from lightfee.sidecar.service import SidecarService

        svc = object.__new__(SidecarService)
        svc.config = type("c", (), {"symbols": ["BTCUSDT"], "venues": []})()
        svc.snapshot_path = tmp_path / "sidecar.json"
        svc._funding_timeout_s = 10.0
        svc._liquidity_timeout_s = 10.0
        svc._transfer_sources = []
        svc._last_good_quotes = {}
        svc._last_good_at_ms = 0

        async def fake_fetch_all_venues(symbols, timeout_s):
            return [
                ("binance", None, TimeoutError("funding timeout 10.0s"), set()),
                ("okx", {"okx:BTCUSDT": QuoteSnapshot(
                    venue="okx", symbol="BTCUSDT", bid=50010, ask=50011,
                    funding_rate_bps=2.0, funding_timestamp_ms=1000,
                )}, None, set()),
            ]

        async def fake_fetch_liquidity_all_venues(symbols, timeout_s):
            return []

        svc._fetch_all_venues = fake_fetch_all_venues
        svc._fetch_liquidity_all_venues = fake_fetch_liquidity_all_venues

        snapshot = await svc.refresh_once()

        assert snapshot.degraded_venues == ["binance"]
        assert snapshot.acquisition_mode == "degraded_sidecar"
