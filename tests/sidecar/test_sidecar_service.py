"""Tests for SidecarService: concurrent fetch, degradation, no Chillybot."""

from __future__ import annotations

import asyncio
import json

import pytest

from lightfee.config.schema import AppConfig, StrategyConfig
from lightfee.sidecar.pairing import FundingCandidateService, build_same_symbol_pairs
from lightfee.sidecar.service import SidecarService
from lightfee.sidecar.snapshot import CandidateInput, QuoteSnapshot, SidecarSnapshot


def test_recovery_constructed_calibrator_keeps_strategy_stability_contract(tmp_path):
    service = object.__new__(SidecarService)
    config = AppConfig()
    config.runtime.sidecar_snapshot_path = str(tmp_path / "snapshot.json")
    config.strategy = StrategyConfig(
        funding_forecast_min_samples=18,
        funding_forecast_stability_max_quantile_drift_bps=1.25,
    )
    service.config = config
    service.snapshot_path = config.runtime.sidecar_snapshot_path

    calibrator = service._ensure_forecast_calibrator()

    assert calibrator._min_samples == 18
    assert calibrator._max_quantile_drift_bps == 1.25


def test_funding_candidate_service_reuses_prepared_context() -> None:
    quotes = {
        "cheap:BTCUSDT": QuoteSnapshot(
            venue="cheap", symbol="BTCUSDT", bid=99.9, ask=100.0,
            funding_rate_bps=2.0, funding_timestamp_ms=1_000_000,
        ),
        "rich:BTCUSDT": QuoteSnapshot(
            venue="rich", symbol="BTCUSDT", bid=100.3, ask=100.4,
            funding_rate_bps=8.0, funding_timestamp_ms=1_000_000,
        ),
    }
    service = FundingCandidateService(
        strategy=StrategyConfig(
            entry_notional_cap_quote=30.0,
            live_entry_notional_cap_quote=30.0,
            funding_missing_margin_fallback_notional_quote=15.0,
        ),
        venue_fee_bps={"cheap": 1.0, "rich": 1.0},
        venue_maker_fee_bps={"cheap": 0.5, "rich": 0.5},
        venue_notional_caps={"cheap": 25.0, "rich": 25.0},
        passive_execution_enabled=False,
    )

    first = service.build(quotes, ["BTCUSDT"], observed_at_ms=1)
    allocator_id = id(service._allocator)
    second = service.build(quotes, ["BTCUSDT"], observed_at_ms=1)

    assert first == second
    assert id(service._allocator) == allocator_id


class TestSidecarPairingV2:
    """Pairing must produce V2-native candidate identity fields."""

    def test_pair_id_in_candidate(self):
        q = {
            "binance:btcusdt": QuoteSnapshot(
                venue="binance", symbol="BTCUSDT",
                bid=50000, ask=50001, funding_rate_bps=5.0,
                funding_timestamp_ms=1700000001000,
            ),
            "okx:btcusdt": QuoteSnapshot(
                venue="okx", symbol="BTCUSDT",
                bid=50100, ask=50200, funding_rate_bps=15.0,
                funding_timestamp_ms=1700000002000,
            ),
        }
        strategy = StrategyConfig(
            entry_notional_cap_quote=40.0,
            live_entry_notional_cap_quote=30.0,
            funding_missing_margin_fallback_notional_quote=15.0,
        )
        candidates = build_same_symbol_pairs(q, ["BTCUSDT"], strategy=strategy)
        assert len(candidates) == 1
        c = candidates[0]
        assert c.pair_id == "btcusdt:binance->okx"
        assert c.long_venue == "binance"
        assert c.short_venue == "okx"

    def test_direction_consistent_using_mid(self):
        """V2 fix: direction_consistent uses mid prices, not ask."""
        q = {
            "binance:btcusdt": QuoteSnapshot(
                venue="binance", symbol="BTCUSDT",
                bid=50000, ask=50001, funding_rate_bps=5.0,
                funding_timestamp_ms=1700000001000,
            ),
            "okx:btcusdt": QuoteSnapshot(
                venue="okx", symbol="BTCUSDT",
                bid=50100, ask=50200, funding_rate_bps=15.0,
                funding_timestamp_ms=1700000002000,
            ),
        }
        strategy = StrategyConfig(
            entry_notional_cap_quote=40.0,
            live_entry_notional_cap_quote=30.0,
            funding_missing_margin_fallback_notional_quote=15.0,
        )
        candidates = build_same_symbol_pairs(q, ["BTCUSDT"], strategy=strategy)
        assert len(candidates) >= 1
        c = candidates[0]
        # funding_diff > 0 and short_mid >= long_mid
        assert c.direction_consistent is True

    def test_interval_aligned(self):
        q = {
            "binance:btcusdt": QuoteSnapshot(
                venue="binance", symbol="BTCUSDT",
                bid=50000, ask=50001, funding_rate_bps=5.0,
                funding_timestamp_ms=1700000000000,
            ),
            "okx:btcusdt": QuoteSnapshot(
                venue="okx", symbol="BTCUSDT",
                bid=50100, ask=50200, funding_rate_bps=15.0,
                funding_timestamp_ms=1700000001000,  # 1 second apart
            ),
        }
        candidates = build_same_symbol_pairs(q, ["BTCUSDT"])
        assert len(candidates) >= 1
        c = candidates[0]
        diff = abs(1700000000000 - 1700000001000)
        assert diff <= 60000
        assert c.interval_aligned is True
        assert c.opportunity_type == "aligned"

    def test_interval_staggered(self):
        q = {
            "binance:btcusdt": QuoteSnapshot(
                venue="binance", symbol="BTCUSDT",
                bid=50000, ask=50001, funding_rate_bps=5.0,
                funding_timestamp_ms=1700000000000,
            ),
            "okx:btcusdt": QuoteSnapshot(
                venue="okx", symbol="BTCUSDT",
                bid=50100, ask=50200, funding_rate_bps=15.0,
                funding_timestamp_ms=1700000100000,  # 100 seconds apart
            ),
        }
        candidates = build_same_symbol_pairs(q, ["BTCUSDT"])
        assert len(candidates) >= 1
        c = candidates[0]
        assert c.interval_aligned is False
        assert c.opportunity_type == "staggered"

    def test_first_funding_leg_and_timestamps(self):
        q = {
            "binance:btcusdt": QuoteSnapshot(
                venue="binance", symbol="BTCUSDT",
                bid=50000, ask=50001, funding_rate_bps=5.0,
                funding_timestamp_ms=1700000001000,
            ),
            "okx:btcusdt": QuoteSnapshot(
                venue="okx", symbol="BTCUSDT",
                bid=50100, ask=50200, funding_rate_bps=15.0,
                funding_timestamp_ms=1700000002000,
            ),
        }
        candidates = build_same_symbol_pairs(q, ["BTCUSDT"])
        assert len(candidates) >= 1
        c = candidates[0]
        # binance has earlier timestamp → first_funding_leg = "long"
        assert c.first_funding_leg == "long"
        assert c.first_funding_timestamp_ms == 1700000001000
        assert c.second_funding_timestamp_ms == 1700000002000
        assert c.long_funding_timestamp_ms == 1700000001000
        assert c.short_funding_timestamp_ms == 1700000002000

    def test_entry_notional_quote_nonzero(self):
        q = {
            "binance:btcusdt": QuoteSnapshot(
                venue="binance", symbol="BTCUSDT",
                bid=50000, ask=50001, funding_rate_bps=5.0,
                funding_timestamp_ms=1700000001000,
            ),
            "okx:btcusdt": QuoteSnapshot(
                venue="okx", symbol="BTCUSDT",
                bid=50100, ask=50200, funding_rate_bps=15.0,
                funding_timestamp_ms=1700000002000,
            ),
        }
        strategy = StrategyConfig(
            entry_notional_cap_quote=40.0,
            live_entry_notional_cap_quote=30.0,
            funding_missing_margin_fallback_notional_quote=15.0,
        )
        candidates = build_same_symbol_pairs(q, ["BTCUSDT"], strategy=strategy)
        assert len(candidates) >= 1
        c = candidates[0]
        assert c.entry_notional_quote > 0.0
        assert c.entry_notional_quote == pytest.approx(15.0)
        assert c.entry_target_quantity * ((50001 + 50100) / 2.0) == pytest.approx(
            c.entry_notional_quote
        )

    def test_direction_inconsistent_when_short_mid_below_long_mid(self):
        """When short mid is below long mid, direction_consistent should be False."""
        q = {
            "binance:btcusdt": QuoteSnapshot(
                venue="binance", symbol="BTCUSDT",
                bid=60000, ask=60100, funding_rate_bps=5.0,
                funding_timestamp_ms=1700000001000,
            ),
            "okx:btcusdt": QuoteSnapshot(
                venue="okx", symbol="BTCUSDT",
                bid=50000, ask=50100, funding_rate_bps=15.0,
                funding_timestamp_ms=1700000002000,
            ),
        }
        candidates = build_same_symbol_pairs(q, ["BTCUSDT"])
        if candidates:
            c = candidates[0]
            assert c.direction_consistent is False


class TestSidecarSnapshotV2:
    """Snapshot must include all V2 candidate identity fields."""

    def test_schema_is_v3(self):
        s = SidecarSnapshot()
        assert s.schema_version == 3

    def test_candidate_has_v2_fields(self):
        c = CandidateInput(
            long_venue="a", short_venue="b", symbol="X",
            funding_diff_bps=10, funding_edge_bps=10,
            expected_edge_bps=5, worst_case_edge_bps=2, ranking_edge_bps=5,
            direction_consistent=True, interval_aligned=True,
        )
        assert c.direction_consistent is True
        assert c.interval_aligned is True

    def test_no_chillybot_strings_in_default_snapshot(self):
        s = SidecarSnapshot()
        # JSON round-trip should contain no "chillybot" string
        raw = json.dumps({
            "degraded_venues": s.degraded_venues,
            "source_mode": s.source_mode,
            "quotes": {},
            "candidates": [],
        })
        assert "chillybot" not in raw.lower()

    def test_source_mode_is_direct_market(self):
        """Default source_mode should be direct_market or empty (not Chillybot)."""
        s = SidecarSnapshot()
        assert "chillybot" not in s.source_mode.lower()
        assert "chillybot" not in s.acquisition_mode.lower()


class TestDegradation:
    """Partial venue failure must degrade, not clear all candidates."""

    def test_empty_degraded_venues_by_default(self):
        s = SidecarSnapshot()
        assert s.degraded_venues == []

    def test_degraded_venues_persist(self):
        s = SidecarSnapshot(degraded_venues=["gate", "bitget"])
        assert "gate" in s.degraded_venues
        assert "bitget" in s.degraded_venues
        assert s.candidates == []

    def test_quotes_kept_even_with_degradation(self):
        s = SidecarSnapshot(
            degraded_venues=["bad_venue"],
            quotes={"good:btcusdt": QuoteSnapshot(
                venue="good", symbol="BTCUSDT", bid=1, ask=2,
            )},
        )
        assert len(s.quotes) == 1
