"""Tests for V1 parity: per-domain lifecycle, last-good fallback, per-symbol degradation."""

from __future__ import annotations

import asyncio
import sys
import time

import pytest

from lightfee.config.schema import RuntimeConfig, StrategyConfig, VenueConfig
from lightfee.marketdata.open_interest import open_interest_sample_id
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
    funding_rate_sample_id,
)


def _strict_liquidity_quote(**overrides) -> QuoteSnapshot:
    """Return a quote carrying an explicit, typed volume/OI proof."""
    values = {
        "venue": "binance",
        "symbol": "BTCUSDT",
        "bid": 100.0,
        "ask": 101.0,
        "observed_at_ms": 1_000,
        "funding_rate_bps": 1.0,
        "funding_rate_observed_at_ms": 1_000,
        "funding_rate_received_at_ms": 1_000,
        "funding_rate_source": "test_fixture",
        "funding_rate_sample_id": "funding:binance:BTCUSDT:1000:0:0",
        "funding_timestamp_ms": 2_000,
        "funding_interval_ms": 28_800_000,
        "volume_24h_quote": 10_000_000.0,
        "open_interest": 2_000_000.0,
        "open_interest_evidence_status": "observed",
        "open_interest_evidence_reason": "test_fixture",
        "open_interest_observed_at_ms": 1_000,
        "open_interest_event_at_ms": 0,
        "open_interest_received_at_ms": 1_000,
        "open_interest_source": "test_fixture",
        "open_interest_sample_id": "",
        "open_interest_venue_symbol": "BTCUSDT",
        "raw_open_interest": 2_000_000.0,
        "raw_open_interest_unit": "quote",
        "open_interest_contract_multiplier": 1.0,
        "underlying": "BTC",
        "quote_currency": "USDT",
        "contract_type": "linear",
        "contract_multiplier": 1.0,
        "mark_index_source": "test_fixture",
        "price_precision": 2,
        "quantity_precision": 3,
        "price_tick": 0.01,
        "quantity_step_base": 0.001,
        "min_quantity_base": 0.001,
        "min_notional_quote": 1.0,
        "min_notional_evidence_complete": True,
        "venue_status": "active",
        "contract_normalization_complete": True,
    }
    values.update(overrides)
    if "funding_rate_observed_at_ms" not in overrides:
        values["funding_rate_observed_at_ms"] = int(values["observed_at_ms"])
    if "funding_rate_received_at_ms" not in overrides:
        values["funding_rate_received_at_ms"] = int(
            values["funding_rate_observed_at_ms"]
        )
    try:
        values["funding_rate_sample_id"] = funding_rate_sample_id(
            venue=str(values["venue"]),
            symbol=str(values["symbol"]),
            observed_at_ms=int(values["funding_rate_observed_at_ms"]),
            rate_bps=float(values["funding_rate_bps"]),
            funding_timestamp_ms=int(values["funding_timestamp_ms"]),
        )
    except (TypeError, ValueError, OverflowError):
        values["funding_rate_sample_id"] = ""
    if "underlying" not in overrides:
        values["underlying"] = str(values["symbol"]).removesuffix("USDT")
    values["open_interest_sample_id"] = open_interest_sample_id(
        venue=str(values["venue"]),
        canonical_symbol=str(values["symbol"]),
        venue_symbol=str(values["open_interest_venue_symbol"]),
        observed_at_ms=int(
            values.get("open_interest_event_at_ms")
            or values["open_interest_observed_at_ms"]
        ),
        source=str(values["open_interest_source"]),
        raw_value=float(values["raw_open_interest"]),
        value_quote=float(values["open_interest"]),
    )
    return QuoteSnapshot(**values)


async def _wait_for_full_audit_publish(service) -> None:
    """Wait only in tests that assert the asynchronous audit artifact."""
    task = service._audit_publish_task
    if task is not None:
        await task


def _stub_auxiliary_market_sources(service) -> None:
    """Keep lifecycle-unit tests on their explicit quote fixtures."""

    async def empty_bbo(_symbols):
        return {}

    for source in service._funding_entry_bbo_sources.values():
        source.fetch_spread_bbo = empty_bbo


class TestLifecycleCoverageDegradation:
    """Lifecycle dataclasses must carry coverage_usable and degraded_reason."""

    def test_funding_lifecycle_has_coverage_and_reason(self):
        fl = FundingLifecycle(
            venue="binance",
            observed_at_ms=1000,
            symbol_count=5,
            coverage_usable=5,
            degraded_reason="",
        )
        assert fl.coverage_usable == 5
        assert fl.degraded_reason == ""

    def test_funding_lifecycle_degraded(self):
        fl = FundingLifecycle(
            venue="okx",
            observed_at_ms=1000,
            symbol_count=3,
            coverage_usable=0,
            degraded_reason="timeout",
        )
        assert fl.coverage_usable == 0
        assert fl.degraded_reason == "timeout"

    def test_market_lifecycle_has_coverage_and_reason(self):
        ml = MarketLifecycle(
            venue="bybit",
            observed_at_ms=1000,
            symbol_count=3,
            coverage_usable=2,
            degraded_reason="BTCUSDT: fetch failed",
        )
        assert ml.coverage_usable == 2

    def test_liquidity_lifecycle_has_coverage_and_reason(self):
        ll = LiquidityLifecycle(
            venue="gate",
            observed_at_ms=1000,
            symbol_count=0,
            coverage_usable=0,
            degraded_reason="venue degraded",
        )
        assert ll.coverage_usable == 0

    def test_transfer_lifecycle_has_coverage_and_reason(self):
        tl = TransferLifecycle(
            from_venue="binance",
            to_venue="okx",
            observed_at_ms=1000,
            coverage_usable=0,
            degraded_reason="",
        )
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
            published_at_ms=1_000,
            market_observed_at_ms=1_000,
            candidate_build_observed_at_ms=1_000,
            candidate_build_diagnostics={
                "input_quote_count": 0,
                "requested_symbol_count": 1,
                "requested_symbols": ["BTCUSDT"],
                "requested_venues": ["gate", "okx"],
                "directional_pair_count": 0,
                "output_candidate_count": 0,
                "future_input_quote_count": 0,
                "rejection_counts": {},
            },
            degraded_symbols={"okx": ["BTCUSDT"]},
            degraded_venues=["gate"],
            source_mode="direct_market",
            acquisition_mode="degraded_sidecar",
            funding_lifecycle=[
                FundingLifecycle(
                    venue="gate",
                    observed_at_ms=1_000,
                    symbol_count=1,
                    coverage_usable=0,
                    degraded_reason="timeout",
                ),
                FundingLifecycle(
                    venue="okx",
                    observed_at_ms=1_000,
                    symbol_count=1,
                    coverage_usable=0,
                    degraded_reason="BTCUSDT: fetch failed",
                ),
            ],
            market_lifecycle=[
                MarketLifecycle(
                    venue=venue,
                    observed_at_ms=1_000,
                    symbol_count=1,
                    coverage_usable=0,
                    degraded_reason=(
                        "BTCUSDT: market unavailable"
                        if venue == "okx"
                        else "market unavailable"
                    ),
                )
                for venue in ("gate", "okx")
            ],
            liquidity_lifecycle=[
                LiquidityLifecycle(
                    venue=venue,
                    observed_at_ms=1_000,
                    symbol_count=1,
                    coverage_usable=0,
                    degraded_reason=(
                        "BTCUSDT: liquidity unavailable"
                        if venue == "okx"
                        else "liquidity unavailable"
                    ),
                )
                for venue in ("gate", "okx")
            ],
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
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance", symbol="BTCUSDT", bid=50000, ask=50001, funding_rate_bps=10
            ),
        }
        svc = object.__new__(SidecarService)
        svc._last_good_quotes = quotes
        svc._last_good_at_ms = 1000

        result = svc._inject_last_good("binance", ["BTCUSDT"], now_ms=1500)
        assert len(result) == 1
        assert result["binance:BTCUSDT"].bid == 50000
        assert result["binance:BTCUSDT"] is not quotes["binance:BTCUSDT"]

    def test_inject_last_good_empty_when_no_cache(self):
        from lightfee.sidecar.service import SidecarService

        svc = object.__new__(SidecarService)
        svc._last_good_quotes = {}
        svc._last_good_at_ms = 0

        result = svc._inject_last_good("binance", ["BTCUSDT"], now_ms=1500)
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

        result = svc._inject_last_good("binance", ["BTCUSDT"], now_ms=1500)
        assert len(result) == 1
        assert "binance:BTCUSDT" in result
        assert "okx:BTCUSDT" not in result

    def test_inject_last_good_enforces_per_key_ttl_and_future_guard(self):
        from lightfee.sidecar.service import SidecarService

        svc = object.__new__(SidecarService)
        svc.config = type(
            "Config",
            (),
            {"runtime": RuntimeConfig(live_scan_last_good_max_age_ms=500)},
        )()
        svc._last_good_quotes = {
            "binance:BTCUSDT": QuoteSnapshot(
                venue="binance", symbol="BTCUSDT", bid=1, ask=2,
                observed_at_ms=1_000,
            ),
            "binance:ETHUSDT": QuoteSnapshot(
                venue="binance", symbol="ETHUSDT", bid=3, ask=4,
                observed_at_ms=2_000,
            ),
        }
        svc._last_good_at_ms = 0
        svc._last_good_at_ms_by_key = {
            "binance:BTCUSDT": 1_000,
            "binance:ETHUSDT": 2_000,
        }

        exact = svc._inject_last_good(
            "binance", ["BTCUSDT", "ETHUSDT"], now_ms=1_500
        )
        expired = svc._inject_last_good(
            "binance", ["BTCUSDT", "ETHUSDT"], now_ms=1_501
        )

        assert list(exact) == ["binance:BTCUSDT"]
        assert expired == {}


class TestSidecarResourceLifecycle:
    """Sidecar runtime must release public HTTP clients on every exit path."""

    def test_app_once_closes_service(self, monkeypatch):
        from lightfee.apps import sidecar as sidecar_app

        calls: list[str] = []
        config = type(
            "Config",
            (),
            {
                "runtime": type("Runtime", (), {"sidecar_refresh_ms": 1})(),
            },
        )()

        class FakeService:
            def __init__(self, config):
                calls.append("init")

            async def refresh_once(self):
                calls.append("refresh")

            async def close(self):
                calls.append("close")

        monkeypatch.setattr(sidecar_app, "load_config", lambda path: config)
        monkeypatch.setattr(sidecar_app, "SidecarService", FakeService)
        monkeypatch.setattr(sys, "argv", ["lightfee-sidecar", "--once"])

        sidecar_app.main()

        assert calls == ["init", "refresh", "close"]

    @pytest.mark.asyncio
    async def test_service_close_continues_after_source_close_error(self):
        from lightfee.sidecar.service import SidecarService

        closed: list[str] = []

        class Source:
            def __init__(self, name: str, *, fail: bool = False) -> None:
                self.name = name
                self.fail = fail

            async def close(self) -> None:
                closed.append(self.name)
                if self.fail:
                    raise RuntimeError(f"{self.name} close failed")

        svc = object.__new__(SidecarService)
        svc._exchange_sources = {
            "bad": Source("exchange_bad", fail=True),
            "good": Source("exchange_good"),
        }
        await svc.close()

        assert closed == ["exchange_bad", "exchange_good"]

    @pytest.mark.asyncio
    async def test_service_close_uses_source_snapshot_when_close_mutates_registry(self):
        from lightfee.sidecar.service import SidecarService

        closed: list[str] = []
        svc = object.__new__(SidecarService)

        class MutatingSource:
            async def close(self) -> None:
                closed.append("mutating")
                svc._exchange_sources.clear()

        class Source:
            async def close(self) -> None:
                closed.append("good")

        svc._exchange_sources = {
            "mutating": MutatingSource(),
            "good": Source(),
        }

        await svc.close()

        assert closed == ["mutating", "good"]


class TestSnapshotRoundTripWithAllV1Fields:
    """Full snapshot round-trip preserves all V1 parity lifecycle fields."""

    def test_round_trip_lifecycle_coverage(self):
        import tempfile
        from pathlib import Path

        requested_symbols = [
            "BTCUSDT",
            "ETHUSDT",
            *(f"SYM{index}USDT" for index in range(2, 10)),
        ]
        s = SidecarSnapshot(
            published_at_ms=1000,
            market_observed_at_ms=1000,
            candidate_build_observed_at_ms=1000,
            candidate_build_diagnostics={
                "input_quote_count": 10,
                "requested_symbol_count": 10,
                "requested_symbols": requested_symbols,
                "requested_venues": ["a", "b"],
                "directional_pair_count": 0,
                "output_candidate_count": 0,
                "future_input_quote_count": 0,
                "rejection_counts": {},
            },
            funding_lifecycle=[
                FundingLifecycle(
                    venue="a",
                    observed_at_ms=1000,
                    symbol_count=10,
                    coverage_usable=10,
                    degraded_reason="",
                ),
                FundingLifecycle(
                    venue="b",
                    observed_at_ms=1000,
                    symbol_count=10,
                    coverage_usable=0,
                    degraded_reason="timeout",
                ),
            ],
            market_lifecycle=[
                MarketLifecycle(
                    venue="a",
                    observed_at_ms=1000,
                    symbol_count=10,
                    coverage_usable=8,
                    degraded_reason="BTCUSDT: fetch failed; ETHUSDT: fetch failed",
                ),
                MarketLifecycle(
                    venue="b",
                    observed_at_ms=1000,
                    symbol_count=10,
                    coverage_usable=0,
                    degraded_reason="market unavailable",
                ),
            ],
            liquidity_lifecycle=[
                LiquidityLifecycle(
                    venue="a",
                    observed_at_ms=1000,
                    symbol_count=10,
                    coverage_usable=8,
                    degraded_reason="BTCUSDT: unavailable; ETHUSDT: unavailable",
                ),
                LiquidityLifecycle(
                    venue="b",
                    observed_at_ms=1000,
                    symbol_count=10,
                    coverage_usable=0,
                    degraded_reason="liquidity unavailable",
                ),
            ],
            degraded_symbols={"a": ["BTCUSDT", "ETHUSDT"]},
            degraded_venues=["b"],
            source_mode="direct_market",
            acquisition_mode="degraded_sidecar",
            quotes={
                f"a:{symbol}": QuoteSnapshot(
                    venue="a",
                    symbol=symbol,
                    bid=100.0,
                    ask=101.0,
                    observed_at_ms=1_000,
                    funding_rate_bps=1.0,
                    funding_timestamp_ms=2_000,
                    funding_interval_ms=28_800_000,
                )
                for symbol in requested_symbols
            },
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

        assert _resolve_acquisition_mode(set(), {}, set()) == "fresh_sidecar"

    def test_acquisition_mode_last_good_when_degraded_with_injected_fallback(self):
        from lightfee.sidecar.service import _resolve_acquisition_mode

        assert (
            _resolve_acquisition_mode(
                {"okx"}, {}, {"okx:BTCUSDT"}
            )
            == "last_good_sidecar"
        )

    def test_acquisition_mode_degraded_when_first_partial_failure(self):
        """Critical: first partial failure with no cache → degraded_sidecar, NOT fresh."""
        from lightfee.sidecar.service import _resolve_acquisition_mode

        assert _resolve_acquisition_mode({"binance"}, {}, set()) == "degraded_sidecar"

    def test_acquisition_mode_degraded_when_cache_empty_dict(self):
        """Empty dict is falsy → degraded_sidecar, not fresh."""
        from lightfee.sidecar.service import _resolve_acquisition_mode

        assert _resolve_acquisition_mode({"binance"}, {}, set()) != "fresh_sidecar"

    def test_acquisition_mode_unavailable_when_no_quote_payload(self):
        from lightfee.sidecar.service import _resolve_acquisition_mode

        assert (
            _resolve_acquisition_mode(
                {"binance"},
                {},
                set(),
                has_usable_payload=False,
            )
            == "unavailable"
        )

    def test_partial_symbol_failure_is_not_labeled_fresh(self):
        from lightfee.sidecar.service import _resolve_acquisition_mode

        assert (
            _resolve_acquisition_mode(set(), {"okx": ["BTCUSDT"]}, set())
            == "degraded_sidecar"
        )


class TestLiquidityEvidenceWiredIntoRefresh:
    """The refresh owns broad liquidity evidence; audit never adds a source lane."""

    def test_no_background_liquidity_fetch_method_exists(self):
        from lightfee.sidecar.service import SidecarService

        svc = object.__new__(SidecarService)
        assert not hasattr(svc, "_fetch_liquidity_all_venues")

    @pytest.mark.asyncio
    async def test_funding_fetch_treats_bulk_endpoint_omission_as_unlisted(self):
        from lightfee.sidecar.service import SidecarService

        class SilentPartialSource:
            async def fetch_all(self, symbols):
                return {
                    "binance:BTCUSDT": QuoteSnapshot(
                        venue="binance",
                        symbol="BTCUSDT",
                        bid=100.0,
                        ask=101.0,
                        observed_at_ms=123,
                    )
                }

        service = object.__new__(SidecarService)
        service.config = type(
            "Config",
            (),
            {"venues": [type("Venue", (), {"venue": "binance"})()]},
        )()
        service._exchange_sources = {"binance": SilentPartialSource()}

        results = await service._fetch_all_venues(
            ["BTCUSDT", "ETHUSDT"], timeout_s=1.0
        )

        venue, quotes, error, failed_symbols = results[0]
        assert venue == "binance"
        assert error is None
        assert set(quotes or {}) == {"binance:BTCUSDT"}
        # The service sends the configured union to every venue.  A bulk
        # endpoint omitting ETH means that venue does not list it; it is not a
        # failed fetch for a symbol the venue promised to return.
        assert failed_symbols == set()

    @pytest.mark.asyncio
    async def test_refresh_binds_missing_and_crossed_symbols_to_lifecycle_proof(
        self, tmp_path
    ):
        from lightfee.config.schema import AppConfig, VenueConfig
        from lightfee.sidecar.service import SidecarService

        class MissingAndCrossedSource:
            async def fetch_all(self, symbols):
                return {
                    "binance:BTCUSDT": _strict_liquidity_quote(
                        bid=101.0,
                        ask=100.0,
                        observed_at_ms=123,
                        open_interest_observed_at_ms=123,
                        open_interest_received_at_ms=123,
                    )
                }

            async def close(self):
                return None

        snapshot_path = tmp_path / "sidecar.json"
        config = AppConfig(
            symbols=["BTCUSDT", "ETHUSDT"],
            runtime=RuntimeConfig(sidecar_snapshot_path=str(snapshot_path)),
            venues=[VenueConfig(venue="binance")],
        )
        service = SidecarService(config)
        _stub_auxiliary_market_sources(service)
        service._exchange_sources["binance"] = MissingAndCrossedSource()

        try:
            snapshot = await service.refresh_once()
            await _wait_for_full_audit_publish(service)
        finally:
            await service.close()

        assert snapshot.degraded_venues == ["binance"]
        assert snapshot.degraded_symbols == {"binance": ["BTCUSDT"]}
        assert snapshot.acquisition_mode == "degraded_sidecar"
        assert snapshot.funding_lifecycle[0].symbol_count == 1
        assert snapshot.funding_lifecycle[0].coverage_usable == 1
        assert snapshot.funding_lifecycle[0].degraded_reason == ""
        assert snapshot.market_lifecycle[0].symbol_count == 1
        assert snapshot.market_lifecycle[0].coverage_usable == 0
        assert "BTCUSDT: crossed BBO" in snapshot.market_lifecycle[0].degraded_reason
        assert snapshot.liquidity_lifecycle[0].symbol_count == 1
        assert snapshot.liquidity_lifecycle[0].coverage_usable == 1
        assert load_snapshot(snapshot_path) is not None

    @pytest.mark.parametrize(
        "invalid_bid",
        [0.0, -1.0, float("nan"), float("inf"), "bad"],
    )
    @pytest.mark.asyncio
    async def test_refresh_filters_invalid_bbo_without_losing_valid_symbol(
        self, tmp_path, invalid_bid
    ):
        from lightfee.config.schema import AppConfig, VenueConfig
        from lightfee.sidecar.service import SidecarService

        class InvalidBboSource:
            async def fetch_all(self, symbols):
                return {
                    "binance:BTCUSDT": _strict_liquidity_quote(
                        bid=invalid_bid,
                        ask=100.0,
                        observed_at_ms=123,
                        open_interest_observed_at_ms=123,
                        open_interest_received_at_ms=123,
                    ),
                    "binance:ETHUSDT": _strict_liquidity_quote(
                        symbol="ETHUSDT",
                        bid=50.0,
                        ask=51.0,
                        observed_at_ms=123,
                        funding_rate_bps=2.0,
                        open_interest_observed_at_ms=123,
                        open_interest_received_at_ms=123,
                        open_interest_venue_symbol="ETHUSDT",
                    ),
                }

            async def close(self):
                return None

        snapshot_path = tmp_path / "sidecar.json"
        config = AppConfig(
            symbols=["BTCUSDT", "ETHUSDT"],
            runtime=RuntimeConfig(sidecar_snapshot_path=str(snapshot_path)),
            venues=[VenueConfig(venue="binance")],
        )
        service = SidecarService(config)
        _stub_auxiliary_market_sources(service)
        service._exchange_sources["binance"] = InvalidBboSource()

        try:
            snapshot = await service.refresh_once()
            await _wait_for_full_audit_publish(service)
        finally:
            await service.close()

        assert set(snapshot.quotes) == {"binance:ETHUSDT"}
        assert snapshot.degraded_symbols == {"binance": ["BTCUSDT"]}
        assert snapshot.funding_lifecycle[0].coverage_usable == 1
        assert snapshot.market_lifecycle[0].coverage_usable == 1
        assert "BTCUSDT:" in snapshot.market_lifecycle[0].degraded_reason
        assert snapshot.liquidity_lifecycle[0].coverage_usable == 1
        assert load_snapshot(snapshot_path) is not None

    @pytest.mark.asyncio
    async def test_refresh_bad_funding_scalar_is_locally_degraded_not_aborted(
        self, tmp_path
    ):
        from lightfee.config.schema import AppConfig, VenueConfig
        from lightfee.sidecar.service import SidecarService

        class QuoteSource:
            def __init__(self, venue, funding_rate):
                self.venue = venue
                self.funding_rate = funding_rate

            async def fetch_all(self, symbols):
                return {
                    f"{self.venue}:BTCUSDT": _strict_liquidity_quote(
                        venue=self.venue,
                        symbol="BTCUSDT",
                        bid=100.0,
                        ask=101.0,
                        observed_at_ms=123,
                        funding_rate_bps=self.funding_rate,
                        open_interest_observed_at_ms=123,
                        open_interest_received_at_ms=123,
                        open_interest_venue_symbol="BTCUSDT",
                    )
                }

            async def close(self):
                return None

        snapshot_path = tmp_path / "sidecar.json"
        config = AppConfig(
            symbols=["BTCUSDT"],
            runtime=RuntimeConfig(sidecar_snapshot_path=str(snapshot_path)),
            venues=[
                VenueConfig(venue="binance"),
                VenueConfig(venue="okx"),
            ],
        )
        service = SidecarService(config)
        _stub_auxiliary_market_sources(service)
        service._exchange_sources["binance"] = QuoteSource("binance", "bad")
        service._exchange_sources["okx"] = QuoteSource("okx", 2.0)

        try:
            snapshot = await service.refresh_once()
            await _wait_for_full_audit_publish(service)
        finally:
            await service.close()

        funding = {row.venue: row for row in snapshot.funding_lifecycle}
        assert funding["binance"].coverage_usable == 0
        assert "BTCUSDT: funding_rate_invalid" in funding["binance"].degraded_reason
        assert snapshot.degraded_symbols == {"binance": ["BTCUSDT"]}
        assert snapshot.candidates == []
        assert snapshot.candidate_build_diagnostics["rejection_counts"] == {}
        assert load_snapshot(snapshot_path) is not None

    @pytest.mark.asyncio
    async def test_refresh_reuses_quote_snapshots_for_liquidity_without_second_public_fetch(
        self, tmp_path
    ):
        from lightfee.config.schema import AppConfig, RuntimeConfig, VenueConfig
        from lightfee.sidecar.service import SidecarService

        class FakeExchangeSource:
            calls = 0

            async def fetch_all(self, symbols):
                type(self).calls += 1
                return {
                    "binance:BTCUSDT": _strict_liquidity_quote(
                        observed_at_ms=123,
                        open_interest_observed_at_ms=123,
                        open_interest_received_at_ms=123,
                    )
                }

            async def close(self):
                return None

        config = AppConfig(
            symbols=["BTCUSDT"],
            runtime=RuntimeConfig(sidecar_snapshot_path=str(tmp_path / "sidecar.json")),
            venues=[VenueConfig(venue="binance")],
        )
        service = SidecarService(config)
        _stub_auxiliary_market_sources(service)
        service._exchange_sources["binance"] = FakeExchangeSource()

        try:
            snapshot = await service.refresh_once()
            await _wait_for_full_audit_publish(service)
        finally:
            await service.close()

        assert snapshot.degraded_venues == []
        assert len(snapshot.liquidity_lifecycle) == 1
        assert snapshot.liquidity_lifecycle[0].coverage_usable == 1
        assert snapshot.liquidity_lifecycle[0].symbol_count == 1
        assert FakeExchangeSource.calls == 1


class TestRefreshPublicationSemantics:
    """Refresh metadata must distinguish market observation from publish time."""

    @pytest.mark.asyncio
    async def test_published_at_ms_uses_refresh_completion_time(self, monkeypatch, tmp_path):
        from lightfee.sidecar.service import SidecarService

        svc = object.__new__(SidecarService)
        svc.config = type(
            "c",
            (),
            {
                "runtime": RuntimeConfig(),
                "symbols": ["BTCUSDT"],
                "strategy": StrategyConfig(),
                "venues": [VenueConfig(venue="binance")],
            },
        )()
        svc.snapshot_path = tmp_path / "sidecar.json"
        svc._funding_timeout_s = 10.0
        svc._liquidity_timeout_s = 10.0
        svc._last_good_quotes = {}
        svc._last_good_at_ms = 0
        svc._last_liquidity_publish_at_ms = 1000

        async def fake_fetch_all_venues(symbols, timeout_s):
            return [
                (
                    "binance",
                    {
                        "binance:BTCUSDT": _strict_liquidity_quote(
                            venue="binance",
                            symbol="BTCUSDT",
                            bid=50000,
                            ask=50001,
                            observed_at_ms=1000,
                            funding_rate_bps=1.0,
                            funding_timestamp_ms=1000,
                            funding_interval_ms=28_800_000,
                            underlying="BTC",
                            quote_currency="USDT",
                            contract_type="linear",
                            contract_multiplier=1.0,
                            mark_index_source="venue_index",
                            price_precision=2,
                            quantity_precision=3,
                            price_tick=0.01,
                            quantity_step_base=0.001,
                            min_quantity_base=0.001,
                            min_notional_quote=5.0,
                            min_notional_evidence_complete=True,
                            venue_status="active",
                            contract_normalization_complete=True,
                        )
                    },
                    None,
                    set(),
                )
            ]

        svc._fetch_all_venues = fake_fetch_all_venues
        times = iter([1.0, 5.0, 6.0, 7.0])
        monkeypatch.setattr("lightfee.sidecar.service.time.time", lambda: next(times))

        snapshot = await svc.refresh_once()

        assert snapshot.market_observed_at_ms == 1000
        assert snapshot.published_at_ms == 7000
        assert svc._last_good_at_ms == 7000
        assert snapshot.quotes["binance:BTCUSDT"].observed_at_ms == 1000
        assert snapshot.quotes["binance:BTCUSDT"].source == "sidecar_quote"
        assert snapshot.liquidity_lifecycle[0].observed_at_ms == 1000
        assert snapshot.liquidity_lifecycle[0].published_at_ms == 7000
        assert snapshot.liquidity_lifecycle[0].publish_interval_ms == 6000
        assert snapshot.liquidity_lifecycle[0].domain == "perp_liquidity"
        assert snapshot.liquidity_lifecycle[0].source == "sidecar_perp_liquidity"

    @pytest.mark.asyncio
    async def test_slow_venue_does_not_block_live_entry_generation(self, tmp_path):
        from lightfee.config.schema import AppConfig
        from lightfee.sidecar.publisher import funding_entry_snapshot_path
        from lightfee.sidecar.service import SidecarService

        release_slow = asyncio.Event()
        fetch_counts: dict[str, int] = {}

        class Source:
            def __init__(self, venue: str, *, wait: bool = False) -> None:
                self.venue = venue
                self.wait = wait

            async def fetch_all(self, symbols):
                fetch_counts[self.venue] = fetch_counts.get(self.venue, 0) + 1
                if self.wait:
                    await release_slow.wait()
                now_ms = int(time.time() * 1_000)
                return {
                    f"{self.venue}:BTCUSDT": _strict_liquidity_quote(
                        venue=self.venue,
                        observed_at_ms=now_ms,
                        open_interest_observed_at_ms=now_ms,
                        open_interest_received_at_ms=now_ms,
                    )
                }

            async def close(self):
                return None

        snapshot_path = tmp_path / "sidecar.json"
        service = SidecarService(
            AppConfig(
                symbols=["BTCUSDT"],
                runtime=RuntimeConfig(
                    sidecar_snapshot_path=str(snapshot_path),
                    sidecar_funding_timeout_s=5.0,
                ),
                venues=[
                    VenueConfig(venue="binance"),
                    VenueConfig(venue="okx"),
                    VenueConfig(venue="bybit"),
                ],
            )
        )
        _stub_auxiliary_market_sources(service)
        service._exchange_sources = {
            "binance": Source("binance"),
            "okx": Source("okx"),
            "bybit": Source("bybit", wait=True),
        }

        started = time.monotonic()
        first = await service.refresh_once()
        elapsed = time.monotonic() - started

        assert elapsed < 0.55
        assert {"binance:BTCUSDT", "okx:BTCUSDT"} <= set(first.quotes)
        assert "bybit" in first.degraded_venues
        assert funding_entry_snapshot_path(snapshot_path).exists()

        release_slow.set()
        await asyncio.wait_for(
            service.entry_venue_republish_event.wait(),
            timeout=0.5,
        )
        republish_started = time.monotonic()
        second = await service.refresh_entry_from_latest_cache()
        republish_elapsed = time.monotonic() - republish_started
        assert republish_elapsed < 0.5
        assert "bybit:BTCUSDT" in second.quotes
        assert "bybit" not in second.degraded_venues
        assert fetch_counts == {"binance": 1, "okx": 1, "bybit": 1}
        await service.close()

    @pytest.mark.asyncio
    async def test_liquidity_success_publish_interval_ignores_failed_refresh(
        self, monkeypatch, tmp_path
    ):
        from lightfee.sidecar.service import SidecarService

        svc = object.__new__(SidecarService)
        svc.config = type(
            "c",
            (),
            {
                "runtime": RuntimeConfig(),
                "symbols": ["BTCUSDT"],
                "strategy": StrategyConfig(),
                "venues": [VenueConfig(venue="binance")],
            },
        )()
        svc.snapshot_path = tmp_path / "sidecar.json"
        svc._funding_timeout_s = 10.0
        svc._liquidity_timeout_s = 10.0
        svc._last_good_quotes = {}
        svc._last_good_at_ms = 0
        svc._last_liquidity_publish_at_ms = 1000

        quote_has_liquidity_proof = False

        async def fake_fetch_all_venues(symbols, timeout_s):
            quote_factory = (
                _strict_liquidity_quote
                if quote_has_liquidity_proof
                else QuoteSnapshot
            )
            return [
                (
                    "binance",
                    {
                        "binance:BTCUSDT": quote_factory(
                            venue="binance",
                            symbol="BTCUSDT",
                            bid=50000,
                            ask=50001,
                            observed_at_ms=1000,
                            funding_rate_bps=1.0,
                            funding_timestamp_ms=1000,
                            funding_interval_ms=28_800_000,
                        )
                    },
                    None,
                    set(),
                )
            ]

        svc._fetch_all_venues = fake_fetch_all_venues
        times = iter([2.0, 5.0, 6.0, 7.0])
        monkeypatch.setattr("lightfee.sidecar.service.time.time", lambda: next(times))

        failed = await svc.refresh_once()

        assert failed.liquidity_lifecycle[0].coverage_usable == 0
        assert failed.liquidity_lifecycle[0].publish_interval_ms == 0
        assert svc._last_liquidity_publish_at_ms == 1000

        quote_has_liquidity_proof = True
        times = iter([8.0, 9.0, 10.0, 11.0])
        monkeypatch.setattr("lightfee.sidecar.service.time.time", lambda: next(times))

        successful = await svc.refresh_once()

        assert successful.liquidity_lifecycle[0].coverage_usable == 1
        assert successful.liquidity_lifecycle[0].publish_interval_ms == 10000
        assert svc._last_liquidity_publish_at_ms == 11000

    @pytest.mark.asyncio
    async def test_liquidity_publish_interval_is_tracked_per_venue(self, monkeypatch, tmp_path):
        from lightfee.sidecar.service import SidecarService

        svc = object.__new__(SidecarService)
        svc.config = type(
            "c",
            (),
            {
                "runtime": RuntimeConfig(),
                "symbols": ["BTCUSDT"],
                "strategy": StrategyConfig(),
                "venues": [
                    VenueConfig(venue="okx"),
                    VenueConfig(venue="bybit"),
                ],
            },
        )()
        svc.snapshot_path = tmp_path / "sidecar.json"
        svc._funding_timeout_s = 10.0
        svc._liquidity_timeout_s = 10.0
        svc._last_good_quotes = {}
        svc._last_good_at_ms = 0
        svc._last_liquidity_publish_at_ms = 1000
        svc._last_liquidity_publish_at_ms_by_key = {
            ("perp_liquidity", "sidecar_perp_liquidity", "okx"): 1000,
            ("perp_liquidity", "sidecar_perp_liquidity", "bybit"): 4000,
        }

        async def fake_fetch_all_venues(symbols, timeout_s):
            return [
                (
                    "okx",
                    {
                        "okx:BTCUSDT": _strict_liquidity_quote(
                            venue="okx",
                            symbol="BTCUSDT",
                            bid=50000,
                            ask=50001,
                            observed_at_ms=1000,
                            funding_rate_bps=1.0,
                            funding_timestamp_ms=1000,
                            funding_interval_ms=28_800_000,
                        )
                    },
                    None,
                    set(),
                ),
                (
                    "bybit",
                    {
                        "bybit:BTCUSDT": QuoteSnapshot(
                            venue="bybit",
                            symbol="BTCUSDT",
                            bid=50002,
                            ask=50003,
                            observed_at_ms=1000,
                            funding_rate_bps=2.0,
                            funding_timestamp_ms=1000,
                            funding_interval_ms=28_800_000,
                        )
                    },
                    None,
                    set(),
                ),
            ]

        svc._fetch_all_venues = fake_fetch_all_venues
        times = iter([8.0, 9.0, 10.0, 11.0])
        monkeypatch.setattr("lightfee.sidecar.service.time.time", lambda: next(times))

        snapshot = await svc.refresh_once()
        rows = {row.venue: row for row in snapshot.liquidity_lifecycle}

        assert rows["okx"].coverage_usable == 1
        assert rows["okx"].publish_interval_ms == 10000
        assert rows["okx"].published_at_ms == 11000
        assert rows["bybit"].coverage_usable == 0
        assert rows["bybit"].publish_interval_ms == 0
        assert rows["bybit"].published_at_ms == 4000
        assert (
            svc._last_liquidity_publish_at_ms_by_key[
                ("perp_liquidity", "sidecar_perp_liquidity", "okx")
            ]
            == 11000
        )
        assert (
            svc._last_liquidity_publish_at_ms_by_key[
                ("perp_liquidity", "sidecar_perp_liquidity", "bybit")
            ]
            == 4000
        )

    def test_candidate_sizing_liquidity_source_roundtrip(self, tmp_path):
        candidate = CandidateInput(
            long_venue="okx",
            short_venue="bybit",
            symbol="BTCUSDT",
            funding_diff_bps=10.0,
            funding_edge_bps=10.0,
            expected_edge_bps=5.0,
            worst_case_edge_bps=2.0,
            ranking_edge_bps=10.0,
            entry_notional_quote=50.0,
            funding_timestamp_ms=2_000,
            first_funding_timestamp_ms=2_000,
            long_funding_timestamp_ms=2_000,
            short_funding_timestamp_ms=2_000,
            sizing_liquidity_source="sidecar_perp_liquidity",
            forecast_shadow_age_ms=7 * 24 * 60 * 60 * 1000,
            forecast_sample_count=42,
            forecast_ready=True,
            blocked=True,
            blocked_reasons=["incomplete_v3_economics"],
            economics_incomplete_reason="incomplete_v3_economics",
        )
        snapshot = SidecarSnapshot(
            published_at_ms=1_000,
            market_observed_at_ms=1_000,
            candidate_build_observed_at_ms=1_000,
            candidate_build_diagnostics={
                "input_quote_count": 2,
                "requested_symbol_count": 1,
                "requested_symbols": ["BTCUSDT"],
                "requested_venues": ["bybit", "okx"],
                "directional_pair_count": 1,
                "output_candidate_count": 1,
                "future_input_quote_count": 0,
                "rejection_counts": {},
            },
            source_mode="direct_market",
            acquisition_mode="fresh_sidecar",
            funding_lifecycle=[
                FundingLifecycle(
                    venue=venue,
                    observed_at_ms=1_000,
                    symbol_count=1,
                    coverage_usable=1,
                )
                for venue in ("bybit", "okx")
            ],
            market_lifecycle=[
                MarketLifecycle(
                    venue=venue,
                    observed_at_ms=1_000,
                    symbol_count=1,
                    coverage_usable=1,
                )
                for venue in ("bybit", "okx")
            ],
            liquidity_lifecycle=[
                LiquidityLifecycle(
                    venue=venue,
                    observed_at_ms=1_000,
                    symbol_count=1,
                    coverage_usable=1,
                )
                for venue in ("bybit", "okx")
            ],
            quotes={
                f"{venue}:BTCUSDT": QuoteSnapshot(
                    venue=venue,
                    symbol="BTCUSDT",
                    bid=50_000,
                    ask=50_001,
                    observed_at_ms=1_000,
                    funding_rate_bps=1.0,
                    funding_timestamp_ms=2_000,
                    funding_interval_ms=28_800_000,
                )
                for venue in ("bybit", "okx")
            },
            candidates=[candidate],
        )

        path = tmp_path / "snapshot.json"
        publish_snapshot(snapshot, path)
        loaded = load_snapshot(path)

        assert loaded is not None
        assert loaded.candidates[0].sizing_liquidity_source == "sidecar_perp_liquidity"
        assert loaded.candidates[0].forecast_shadow_age_ms == 7 * 24 * 60 * 60 * 1000
        assert loaded.candidates[0].forecast_sample_count == 42
        assert loaded.candidates[0].forecast_ready is True

    @pytest.mark.asyncio
    async def test_first_partial_degradation_without_cache_is_degraded_sidecar(self, tmp_path):
        from lightfee.sidecar.service import SidecarService

        svc = object.__new__(SidecarService)
        svc.config = type(
            "c",
            (),
            {
                "runtime": RuntimeConfig(),
                "symbols": ["BTCUSDT"],
                "strategy": StrategyConfig(),
                "venues": [
                    VenueConfig(venue="binance"),
                    VenueConfig(venue="okx"),
                ],
            },
        )()
        svc.snapshot_path = tmp_path / "sidecar.json"
        svc._funding_timeout_s = 10.0
        svc._liquidity_timeout_s = 10.0
        svc._last_good_quotes = {}
        svc._last_good_at_ms = 0

        async def fake_fetch_all_venues(symbols, timeout_s):
            return [
                ("binance", None, TimeoutError("funding timeout 10.0s"), set()),
                (
                    "okx",
                    {
                        "okx:BTCUSDT": _strict_liquidity_quote(
                            venue="okx",
                            symbol="BTCUSDT",
                            bid=50010,
                            ask=50011,
                            observed_at_ms=1000,
                            funding_rate_bps=2.0,
                            funding_timestamp_ms=1000,
                            open_interest_observed_at_ms=1000,
                            open_interest_received_at_ms=1000,
                        )
                    },
                    None,
                    set(),
                ),
            ]

        svc._fetch_all_venues = fake_fetch_all_venues

        snapshot = await svc.refresh_once()

        assert snapshot.degraded_venues == ["binance"]
        assert snapshot.acquisition_mode == "degraded_sidecar"

    @pytest.mark.asyncio
    async def test_all_venue_failure_without_cache_is_unavailable(self, tmp_path):
        from lightfee.sidecar.service import SidecarService

        svc = object.__new__(SidecarService)
        svc.config = type(
            "c",
            (),
            {
                "runtime": RuntimeConfig(),
                "symbols": ["BTCUSDT"],
                "strategy": StrategyConfig(),
                "venues": [
                    VenueConfig(venue="binance"),
                    VenueConfig(venue="okx"),
                ],
            },
        )()
        svc.snapshot_path = tmp_path / "sidecar.json"
        svc._funding_timeout_s = 10.0
        svc._liquidity_timeout_s = 10.0
        svc._last_good_quotes = {}
        svc._last_good_at_ms = 0

        async def failed_market(symbols, timeout_s):
            return [
                (venue, None, TimeoutError("funding unavailable"), set())
                for venue in ("binance", "okx")
            ]

        svc._fetch_all_venues = failed_market

        snapshot = await svc.refresh_once()

        assert snapshot.quotes == {}
        assert snapshot.candidates == []
        assert snapshot.acquisition_mode == "unavailable"
        assert load_snapshot(svc.snapshot_path) is not None


def test_liquidity_lifecycle_keeps_deferred_oi_separate_from_proof_failure():
    """The audit must not call a deliberate OI handoff a missing proof."""
    from lightfee.sidecar.service import _liquidity_lifecycle_from_quotes

    deferred = QuoteSnapshot(
        venue="binance",
        symbol="BTCUSDT",
        bid=100.0,
        ask=101.0,
        volume_24h_quote=10_000_000.0,
        open_interest_evidence_status="unavailable",
        open_interest_evidence_reason="entry_targeted_revalidation_required",
    )
    actual_gap = QuoteSnapshot(
        venue="binance",
        symbol="ETHUSDT",
        bid=50.0,
        ask=51.0,
        volume_24h_quote=10_000_000.0,
        open_interest_evidence_status="timeout",
        open_interest_evidence_reason="timeout_waiting_for_oi",
    )

    deferred_only = _liquidity_lifecycle_from_quotes(
        configured_venues=["binance"],
        quotes={"binance:BTCUSDT": deferred},
        listed_symbols_by_venue={"binance": {"BTCUSDT"}},
        market_quality_failed_symbols={},
        observed_at_ms=1_000,
    )
    lifecycle = _liquidity_lifecycle_from_quotes(
        configured_venues=["binance"],
        quotes={
            "binance:BTCUSDT": deferred,
            "binance:ETHUSDT": actual_gap,
        },
        listed_symbols_by_venue={"binance": {"BTCUSDT", "ETHUSDT"}},
        market_quality_failed_symbols={},
        observed_at_ms=1_000,
    )

    assert deferred_only[0].coverage_usable == 0
    assert deferred_only[0].degraded_reason == ""
    assert lifecycle[0].coverage_usable == 0
    assert lifecycle[0].degraded_reason == "strict_liquidity_proof_missing:1"
