import json
import os
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

from lightfee.ops.production_health import (
    analyze_current_state,
    analyze_resolver_config,
    analyze_sidecar_snapshot,
    analyze_spread_snapshot,
    analyze_strategy_entry_policy,
    analyze_systemd_unit,
    summarize_reports,
)
from scripts.diagnose_live import _build_state_consistency
from scripts import verify_production_services as vps


def _complete_quote_lifecycles(venues: list[str], observed_at_ms: int) -> dict:
    def rows() -> list[dict]:
        return [
            {
                "venue": venue,
                "observed_at_ms": observed_at_ms,
                "funding_rate_bps": 1.0,
                "funding_timestamp_ms": observed_at_ms + 28_800_000,
                "funding_interval_ms": 28_800_000,
                "symbol_count": 1,
                "coverage_usable": 1,
                "degraded_reason": "",
            }
            for venue in venues
        ]

    return {
        "funding_lifecycle": rows(),
        "market_lifecycle": rows(),
        "liquidity_lifecycle": rows(),
    }


def _fresh_seven_venue_snapshot() -> dict:
    venues = ["aster", "binance", "bitget", "bybit", "gate", "hyperliquid", "okx"]
    observed_at_ms = 1_778_786_998_000
    return {
        "schema_version": 4,
        "published_at_ms": 1_778_786_999_000,
        "market_observed_at_ms": observed_at_ms,
        "candidate_build_observed_at_ms": 1_778_786_998_500,
        "candidate_build_diagnostics": {
            "input_quote_count": 7,
            "requested_symbol_count": 1,
            "requested_symbols": ["BTCUSDT"],
            "requested_venues": venues,
            "directional_pair_count": 0,
            "output_candidate_count": 0,
            "future_input_quote_count": 0,
            "rejection_counts": {},
        },
        "quotes": {
            f"{venue}:BTCUSDT": {
                "venue": venue,
                "symbol": "BTCUSDT",
                "bid": 65_000.0,
                "ask": 65_001.0,
                "observed_at_ms": observed_at_ms,
                "funding_rate_bps": 1.0,
                "funding_timestamp_ms": observed_at_ms + 28_800_000,
                "funding_interval_ms": 28_800_000,
            }
            for venue in venues
        },
        "degraded_venues": [],
        "degraded_domains": [],
        "degraded_symbols": {},
        **_complete_quote_lifecycles(venues, observed_at_ms),
        "transfer_lifecycle": [],
        "source_mode": "direct_market",
        "acquisition_mode": "fresh_sidecar",
        "candidates": [],
    }


def _complete_unblocked_candidate() -> dict:
    return {
        "long_venue": "binance",
        "short_venue": "okx",
        "symbol": "BTCUSDT",
        "funding_diff_bps": 10.0,
        "funding_edge_bps": 10.0,
        "expected_edge_bps": 10.0,
        "worst_case_edge_bps": 8.0,
        "ranking_edge_bps": 8.0,
        "first_stage_funding_edge_bps": 10.0,
        "first_stage_expected_edge_bps": 10.0,
        "first_stage_worst_case_edge_bps": 8.0,
        "second_stage_incremental_funding_edge_bps": 0.0,
        "second_stage_worst_case_funding_edge_bps": 0.0,
        "stagger_gap_ms": 0,
        "entry_notional_quote": 100.0,
        "entry_target_quantity": 1.0,
        "entry_max_executable_quantity": 1.0,
        "gross_signal_edge_bps": 0.0,
        "entry_cross_bps": 0.0,
        "expected_exit_cross_bps": 0.0,
        "entry_fee_bps": 0.0,
        "exit_fee_bps": 0.0,
        "fee_bps": 0.0,
        "entry_slippage_bps": 0.0,
        "exit_slippage_bps": 0.0,
        "adverse_selection_bps": 0.0,
        "capital_buffer_bps": 0.0,
        "execution_buffer_bps": 0.0,
        "venue_risk_haircut_bps": 0.0,
        "transfer_or_inventory_bias_bps": 0.0,
        "expected_net_edge_bps": 10.0,
        "long_taker_fee_bps": 0.0,
        "short_taker_fee_bps": 0.0,
        "taker_fee_evidence_complete": True,
        "forecast_distribution_stable": False,
        "forecast_stability_reason": "not_calibrated",
        "forecast_worst_funding_edge_bps": 8.0,
        "economics_complete": True,
        "economics_observed_at_ms": 1_778_786_998_500,
        "calculation_version": "v1_exact",
        "model_epoch": "v1_exact",
        "funding_timestamp_ms": 1_778_815_798_000,
        "first_funding_timestamp_ms": 1_778_815_798_000,
        "long_funding_timestamp_ms": 1_778_815_798_000,
        "short_funding_timestamp_ms": 1_778_815_798_000,
    }


def _add_complete_contract_proof(quote: dict) -> None:
    quote.update(
        {
            "underlying": "BTC",
            "quote_currency": "USDT",
            "contract_type": "linear",
            "contract_multiplier": 1.0,
            "mark_index_source": "venue_index",
            "price_precision": 2,
            "quantity_precision": 3,
            "price_tick": 0.01,
            "quantity_step_base": 0.001,
            "min_quantity_base": 0.001,
            "min_notional_quote": 5.0,
            "min_notional_evidence_complete": True,
            "venue_status": "active",
            "contract_normalization_complete": True,
        }
    )


def test_sidecar_unit_rejects_missing_config():
    text = """
[Service]
WorkingDirectory=/root/projects/LightFee
ExecStart=/root/projects/LightFee/target/release/opportunity_input_sidecar
"""
    report = analyze_systemd_unit("lightfee-sidecar.service", text)
    assert not report.ok
    assert "missing_explicit_config" in report.fingerprints


def test_sidecar_unit_rejects_example_config():
    text = """
[Service]
EnvironmentFile=/etc/lightfee/lightfee.env
ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.sidecar --config config/example.toml
"""
    report = analyze_systemd_unit("lightfee-sidecar.service", text)
    assert not report.ok
    assert "example_config_in_production" in report.fingerprints


def test_sidecar_unit_accepts_current_rust_v1_live_contract():
    text = """
[Service]
WorkingDirectory=/root/projects/LightFee
EnvironmentFile=/etc/lightfee/lightfee.env
ExecStart=/root/projects/LightFee/target/release/opportunity_input_sidecar --config /root/projects/LightFee/config/live.auto.toml
LimitNOFILE=65536
Restart=always
"""
    report = analyze_systemd_unit("lightfee-sidecar.service", text)
    assert report.ok


def test_snapshot_rejects_fixture_four_venue_shape():
    snapshot = {
        "market_observed_at_ms": 1710000075000,
        "quotes": {
            "binance:BTCUSDT": {
                "venue": "binance",
                "symbol": "BTCUSDT",
                "bid": 100.0,
                "ask": 100.0,
            },
            "okx:BTCUSDT": {"venue": "okx", "symbol": "BTCUSDT", "bid": 100.0, "ask": 100.0},
            "bybit:BTCUSDT": {"venue": "bybit", "symbol": "BTCUSDT", "bid": 100.0, "ask": 100.0},
            "hyperliquid:BTCUSDT": {
                "venue": "hyperliquid",
                "symbol": "BTCUSDT",
                "bid": 100.0,
                "ask": 100.0,
            },
        },
        "degraded_venues": [],
    }
    report = analyze_sidecar_snapshot(snapshot, now_ms=1778787000000, max_age_ms=10_000)
    assert not report.ok
    assert "fixture_timestamp" in report.fingerprints
    assert "quote_venue_count_lt_7" in report.fingerprints


def test_snapshot_accepts_fresh_seven_venue_shape():
    snapshot = _fresh_seven_venue_snapshot()
    report = analyze_sidecar_snapshot(snapshot, now_ms=1778787000000, max_age_ms=10_000)
    assert report.ok


def test_snapshot_transport_green_is_not_funding_ready_when_all_candidates_blocked():
    snapshot = _fresh_seven_venue_snapshot()
    funding_timestamp_ms = 1_778_815_798_000
    snapshot["candidate_build_diagnostics"]["directional_pair_count"] = 1
    snapshot["candidate_build_diagnostics"]["output_candidate_count"] = 1
    snapshot["candidates"] = [
        {
            "long_venue": "binance",
            "short_venue": "okx",
            "symbol": "BTCUSDT",
            "funding_diff_bps": 1.0,
            "funding_edge_bps": 1.0,
            "expected_edge_bps": 1.0,
            "worst_case_edge_bps": 1.0,
            "ranking_edge_bps": 1.0,
            "funding_timestamp_ms": funding_timestamp_ms,
            "first_funding_timestamp_ms": funding_timestamp_ms,
            "long_funding_timestamp_ms": funding_timestamp_ms,
            "short_funding_timestamp_ms": funding_timestamp_ms,
            "blocked": True,
            "blocked_reasons": ["long_contract_normalization_incomplete"],
            "economics_complete": False,
            "economics_incomplete_reason": "long_contract_normalization_incomplete",
        }
    ]

    report = analyze_sidecar_snapshot(
        snapshot,
        now_ms=1_778_787_000_000,
        max_age_ms=10_000,
    )

    assert report.details["contract_errors"] == []
    assert not report.ok
    assert "funding_entry_readiness_no_unblocked_candidates" in report.fingerprints
    assert report.details["candidate_count"] == 1
    assert report.details["blocked_candidate_count"] == 1
    assert report.details["unblocked_candidate_count"] == 0
    assert report.details["funding_entry_ready"] is False
    assert report.details["blocked_reason_counts"] == {
        "economics_observation_missing": 1,
        "long_contract_normalization_incomplete": 1,
    }


def test_snapshot_cannot_be_green_when_funding_coverage_is_unproved():
    snapshot = _fresh_seven_venue_snapshot()
    snapshot["quotes"]["binance:BTCUSDT"].pop("funding_interval_ms")

    report = analyze_sidecar_snapshot(
        snapshot,
        now_ms=1_778_787_000_000,
        max_age_ms=10_000,
    )

    assert not report.ok
    assert "sidecar_diagnostics_contract_invalid" in report.fingerprints
    assert "funding_interval_evidence_incomplete" in report.fingerprints
    assert report.details["funding_interval_known_counts_by_venue"]["binance"] == 0
    assert report.details["funding_interval_quote_counts_by_venue"]["binance"] == 1
    assert report.details["funding_interval_missing_quote_keys"] == ["binance:BTCUSDT"]
    assert (
        "lifecycle_coverage_exceeds_quote_symbols:funding:binance"
        in report.details["contract_errors"]
    )


def test_snapshot_rejects_lifecycle_total_above_requested_universe():
    snapshot = _fresh_seven_venue_snapshot()
    snapshot["funding_lifecycle"][0]["symbol_count"] = 2

    report = analyze_sidecar_snapshot(
        snapshot,
        now_ms=1_778_787_000_000,
        max_age_ms=10_000,
    )

    assert not report.ok
    assert "lifecycle_requested_count_exceeded:funding:aster" in report.details["contract_errors"]


def test_snapshot_rejects_lifecycle_total_below_returned_quote_universe():
    snapshot = _fresh_seven_venue_snapshot()
    snapshot["degraded_symbols"] = {"aster": ["BTCUSDT"]}
    snapshot["acquisition_mode"] = "degraded_sidecar"
    snapshot["funding_lifecycle"][0].update(
        {
            "symbol_count": 0,
            "coverage_usable": 0,
            "degraded_reason": "BTCUSDT: funding unavailable",
        }
    )

    report = analyze_sidecar_snapshot(
        snapshot,
        now_ms=1_778_787_000_000,
        max_age_ms=10_000,
    )

    assert not report.ok
    assert "lifecycle_total_below_quote_symbols:funding:aster" in report.details["contract_errors"]


def test_snapshot_rejects_arbitrary_seven_venues_replacing_expected_venue():
    snapshot = _fresh_seven_venue_snapshot()
    snapshot["quotes"]["mexc:BTCUSDT"] = snapshot["quotes"].pop("aster:BTCUSDT")
    snapshot["quotes"]["mexc:BTCUSDT"]["venue"] = "mexc"
    for lifecycle_field in (
        "funding_lifecycle",
        "market_lifecycle",
        "liquidity_lifecycle",
    ):
        for record in snapshot[lifecycle_field]:
            if record["venue"] == "aster":
                record["venue"] = "mexc"
    snapshot["candidate_build_diagnostics"]["requested_venues"] = sorted(
        {"mexc"} | (set(snapshot["candidate_build_diagnostics"]["requested_venues"]) - {"aster"})
    )

    report = analyze_sidecar_snapshot(
        snapshot,
        now_ms=1_778_787_000_000,
        max_age_ms=10_000,
    )

    assert report.details["contract_errors"] == []
    assert not report.ok
    assert report.details["missing_venues"] == ["aster"]
    assert "quote_venue_count_lt_7" in report.fingerprints
    assert "quote_venue_set_unexpected" in report.fingerprints
    assert "requested_venue_set_mismatch" in report.fingerprints


def test_snapshot_rejects_expected_venues_plus_unconfigured_extra_venue():
    snapshot = _fresh_seven_venue_snapshot()
    extra = dict(snapshot["quotes"]["binance:BTCUSDT"])
    extra["venue"] = "mexc"
    snapshot["quotes"]["mexc:BTCUSDT"] = extra
    snapshot["candidate_build_diagnostics"]["input_quote_count"] = 8
    snapshot["candidate_build_diagnostics"]["requested_venues"] = sorted(
        [*snapshot["candidate_build_diagnostics"]["requested_venues"], "mexc"]
    )
    for lifecycle_field in (
        "funding_lifecycle",
        "market_lifecycle",
        "liquidity_lifecycle",
    ):
        row = dict(snapshot[lifecycle_field][0])
        row["venue"] = "mexc"
        snapshot[lifecycle_field].append(row)

    report = analyze_sidecar_snapshot(
        snapshot,
        now_ms=1_778_787_000_000,
        max_age_ms=10_000,
    )

    assert report.details["contract_errors"] == []
    assert not report.ok
    assert report.details["missing_venues"] == []
    assert report.details["unexpected_venues"] == ["mexc"]
    assert "quote_venue_set_unexpected" in report.fingerprints
    assert "requested_venue_set_mismatch" in report.fingerprints


def test_snapshot_unavailable_mode_is_never_production_green():
    snapshot = _fresh_seven_venue_snapshot()
    venues = snapshot["candidate_build_diagnostics"]["requested_venues"]
    snapshot["quotes"] = {}
    snapshot["candidate_build_diagnostics"]["input_quote_count"] = 0
    snapshot["degraded_venues"] = list(venues)
    snapshot["acquisition_mode"] = "unavailable"
    for lifecycle_field in (
        "funding_lifecycle",
        "market_lifecycle",
        "liquidity_lifecycle",
    ):
        for record in snapshot[lifecycle_field]:
            record["coverage_usable"] = 0
            record["degraded_reason"] = "venue unavailable"

    report = analyze_sidecar_snapshot(
        snapshot,
        now_ms=1_778_787_000_000,
        max_age_ms=10_000,
    )

    assert report.details["contract_errors"] == []
    assert not report.ok
    assert "sidecar_snapshot_unavailable" in report.fingerprints


def test_snapshot_declared_degradation_cannot_be_production_green():
    snapshot = _fresh_seven_venue_snapshot()
    snapshot["quotes"]["aster:BTCUSDT"]["bid"] = 65_002.0
    snapshot["degraded_venues"] = ["aster"]
    snapshot["degraded_symbols"] = {"aster": ["BTCUSDT"]}
    snapshot["acquisition_mode"] = "degraded_sidecar"
    aster_market = next(row for row in snapshot["market_lifecycle"] if row["venue"] == "aster")
    aster_market["coverage_usable"] = 0
    aster_market["degraded_reason"] = "BTCUSDT: crossed BBO"
    aster_liquidity = next(
        row for row in snapshot["liquidity_lifecycle"] if row["venue"] == "aster"
    )
    aster_liquidity["coverage_usable"] = 0
    aster_liquidity["degraded_reason"] = "BTCUSDT: crossed BBO"

    report = analyze_sidecar_snapshot(
        snapshot,
        now_ms=1_778_787_000_000,
        max_age_ms=10_000,
    )

    assert report.details["contract_errors"] == []
    assert not report.ok
    assert "sidecar_snapshot_degraded" in report.fingerprints


def test_snapshot_scoped_symbol_degradation_keeps_healthy_candidates_green():
    snapshot = _fresh_seven_venue_snapshot()
    snapshot["quotes"]["aster:BTCUSDT"]["bid"] = 65_002.0
    snapshot["degraded_symbols"] = {"aster": ["BTCUSDT"]}
    snapshot["acquisition_mode"] = "degraded_sidecar"
    for lifecycle_name in ("market_lifecycle", "liquidity_lifecycle"):
        lifecycle = next(row for row in snapshot[lifecycle_name] if row["venue"] == "aster")
        lifecycle["coverage_usable"] = 0
        lifecycle["degraded_reason"] = "BTCUSDT: crossed BBO"
    snapshot["candidate_build_diagnostics"].update(
        {
            "directional_pair_count": 2,
            "output_candidate_count": 1,
            "rejection_counts": {"invalid_trade_quote": 1},
        }
    )
    _add_complete_contract_proof(snapshot["quotes"]["binance:BTCUSDT"])
    _add_complete_contract_proof(snapshot["quotes"]["okx:BTCUSDT"])
    snapshot["candidates"] = [_complete_unblocked_candidate()]

    report = analyze_sidecar_snapshot(
        snapshot,
        now_ms=1_778_787_000_000,
        max_age_ms=10_000,
    )

    assert report.ok
    assert report.details["unblocked_candidate_count"] == 1
    assert report.details["degraded_symbols"] == {"aster": ["BTCUSDT"]}


def test_snapshot_unscoped_interval_gap_remains_critical_with_healthy_candidates():
    snapshot = _fresh_seven_venue_snapshot()
    snapshot["quotes"]["okx:BTCUSDT"]["funding_interval_ms"] = 0
    snapshot["funding_lifecycle"][-1]["coverage_usable"] = 0
    snapshot["funding_lifecycle"][-1]["degraded_reason"] = "BTCUSDT: funding_interval_ms_invalid"
    snapshot["candidate_build_diagnostics"].update(
        {"directional_pair_count": 1, "output_candidate_count": 1}
    )
    funding_timestamp_ms = 1_778_815_798_000
    snapshot["candidates"] = [
        {
            "long_venue": "binance",
            "short_venue": "aster",
            "symbol": "BTCUSDT",
            "funding_diff_bps": 1.0,
            "funding_edge_bps": 1.0,
            "expected_edge_bps": 1.0,
            "worst_case_edge_bps": 1.0,
            "ranking_edge_bps": 1.0,
            "funding_timestamp_ms": funding_timestamp_ms,
            "first_funding_timestamp_ms": funding_timestamp_ms,
            "long_funding_timestamp_ms": funding_timestamp_ms,
            "short_funding_timestamp_ms": funding_timestamp_ms,
            "blocked": False,
            "blocked_reasons": [],
            "economics_complete": True,
            "economics_incomplete_reason": "",
            "economics_observed_at_ms": 1_778_786_998_500,
        }
    ]

    report = analyze_sidecar_snapshot(
        snapshot,
        now_ms=1_778_787_000_000,
        max_age_ms=10_000,
    )

    assert not report.ok
    assert "funding_interval_evidence_incomplete" in report.fingerprints


def test_sidecar_snapshot_rejects_unknown_schema_and_missing_proof() -> None:
    report = analyze_sidecar_snapshot(
        {
            "schema_version": 999,
            "published_at_ms": 1778786999000,
            "market_observed_at_ms": 1778786998000,
            "quotes": {},
        },
        now_ms=1778787000000,
        max_age_ms=10_000,
    )

    assert not report.ok
    assert "sidecar_diagnostics_contract_missing" in report.fingerprints


def test_sidecar_snapshot_malformed_input_fails_closed_without_exception() -> None:
    root_report = analyze_sidecar_snapshot(
        [],
        now_ms=1778787000000,
        max_age_ms=10_000,
    )
    malformed_report = analyze_sidecar_snapshot(
        {
            "schema_version": 4,
            "published_at_ms": "bad",
            "market_observed_at_ms": {},
            "candidate_build_observed_at_ms": [],
            "candidate_build_diagnostics": [],
            "degraded_venues": 7,
            "degraded_domains": None,
            "degraded_symbols": "bad",
            "quotes": [],
            "candidates": {},
        },
        now_ms=1778787000000,
        max_age_ms=10_000,
    )

    assert root_report.fingerprints == ["sidecar_snapshot_root_invalid"]
    assert not malformed_report.ok
    assert "sidecar_diagnostics_contract_invalid" in malformed_report.fingerprints


def test_sidecar_health_rejects_quote_after_claimed_candidate_watermark() -> None:
    snapshot = {
        "schema_version": 4,
        "published_at_ms": 1_000,
        "market_observed_at_ms": 1_000,
        "candidate_build_observed_at_ms": 1_000,
        "candidate_build_diagnostics": {
            "input_quote_count": 1,
            "requested_symbol_count": 1,
            "requested_symbols": ["BTCUSDT"],
            "requested_venues": ["binance"],
            "directional_pair_count": 0,
            "output_candidate_count": 0,
            "future_input_quote_count": 0,
            "rejection_counts": {},
        },
        **_complete_quote_lifecycles(["binance"], 1_000),
        "transfer_lifecycle": [],
        "degraded_venues": [],
        "degraded_domains": [],
        "degraded_symbols": {},
        "source_mode": "direct_market",
        "acquisition_mode": "fresh_sidecar",
        "quotes": {
            "binance:BTCUSDT": {
                "venue": "binance",
                "symbol": "BTCUSDT",
                "bid": 50_000,
                "ask": 50_001,
                "observed_at_ms": 1_001,
            }
        },
        "candidates": [],
    }

    report = analyze_sidecar_snapshot(snapshot, now_ms=1_000, max_age_ms=10_000)

    assert not report.ok
    assert "sidecar_diagnostics_contract_invalid" in report.fingerprints
    assert any(
        error.startswith("quote_after_candidate_watermark:")
        for error in report.details["contract_errors"]
    )


def test_snapshot_rejects_candidate_builder_watermark_mismatch():
    venues = ["aster", "binance", "bitget", "bybit", "gate", "hyperliquid", "okx"]
    snapshot = {
        "market_observed_at_ms": 1778786998000,
        "quotes": {
            f"{venue}:BTCUSDT": {
                "venue": venue,
                "symbol": "BTCUSDT",
                "bid": 65000.0,
                "ask": 65001.0,
            }
            for venue in venues
        },
        "candidate_build_diagnostics": {
            "directional_pair_count": 42,
            "output_candidate_count": 0,
            "rejection_counts": {"quote_after_candidate_watermark": 42},
        },
    }

    report = analyze_sidecar_snapshot(
        snapshot,
        now_ms=1778787000000,
        max_age_ms=10_000,
    )

    assert not report.ok
    assert "candidate_build_watermark_rejected_quotes" in report.fingerprints


def test_spread_snapshot_rejects_stalled_input_pipeline():
    report = analyze_spread_snapshot(
        {
            "published_at_ms": 1778786900000,
            "market_observed_at_ms": 1778786900000,
            "source_mode": "sidecar_snapshot_stale",
            "input_quote_count": 100,
            "valid_quote_count": 0,
            "rejection_counts": {},
            "candidates": [],
        },
        now_ms=1778787000000,
        max_age_ms=10_000,
    )

    assert not report.ok
    assert "spread_snapshot_stale" in report.fingerprints
    assert "spread_input_pipeline_stalled" in report.fingerprints


def test_spread_snapshot_accepts_fresh_explained_zero_candidates():
    report = analyze_spread_snapshot(
        {
            "schema_version": 4,
            "decision_at_ms": 1778786998500,
            "published_at_ms": 1778786999000,
            "market_observed_at_ms": 1778786998000,
            "source_mode": "sidecar_snapshot",
            "degraded_venues": [],
            "degraded_symbols": {},
            "input_quote_count": 100,
            "valid_quote_count": 100,
            "evaluated_pair_count": 10,
            "accepted_pair_count": 0,
            "paper_configured_enabled": False,
            "paper_admission_enabled": False,
            "paper_tracked_count": 0,
            "paper_refresh_status": "disabled",
            "paper_event_count": 0,
            "paper_last_success_at_ms": 0,
            "rejection_counts": {"insufficient_history": 10},
            "paper_admission_rejection_counts": {},
            "candidates": [],
        },
        now_ms=1778787000000,
        max_age_ms=10_000,
    )

    assert report.ok


def test_spread_snapshot_rejects_unexplained_zero_paper_admission() -> None:
    report = analyze_spread_snapshot(
        {
            "schema_version": 4,
            "decision_at_ms": 1778786998500,
            "published_at_ms": 1778786999000,
            "market_observed_at_ms": 1778786998000,
            "source_mode": "sidecar_snapshot",
            "degraded_venues": [],
            "degraded_symbols": {},
            "input_quote_count": 2,
            "valid_quote_count": 2,
            "evaluated_pair_count": 1,
            "accepted_pair_count": 1,
            "paper_configured_enabled": True,
            "paper_admission_enabled": True,
            "paper_tracked_count": 0,
            "paper_refresh_status": "success",
            "paper_event_count": 0,
            "paper_last_success_at_ms": 1778786998500,
            "rejection_counts": {},
            "paper_admission_rejection_counts": {},
            "candidates": [{}],
        },
        now_ms=1778787000000,
        max_age_ms=10_000,
    )

    assert not report.ok
    assert "spread_paper_admission_unexplained_zero" in report.fingerprints


def test_spread_snapshot_rejects_publication_before_decision():
    report = analyze_spread_snapshot(
        {
            "decision_at_ms": 1778786999500,
            "published_at_ms": 1778786999000,
            "market_observed_at_ms": 1778786998000,
            "source_mode": "sidecar_snapshot",
            "input_quote_count": 2,
            "valid_quote_count": 2,
            "evaluated_pair_count": 1,
            "accepted_pair_count": 0,
            "rejection_counts": {"insufficient_history_samples": 1},
            "paper_admission_rejection_counts": {},
            "candidates": [],
        },
        now_ms=1778787000000,
        max_age_ms=10_000,
    )

    assert not report.ok
    assert "spread_publication_watermark_invalid" in report.fingerprints


def test_spread_snapshot_rejects_legacy_schema_without_proof_contract():
    report = analyze_spread_snapshot(
        {
            "schema_version": 3,
            "published_at_ms": 1778786999000,
            "market_observed_at_ms": 1778786998000,
            "source_mode": "sidecar_snapshot",
            "input_quote_count": 2,
            "valid_quote_count": 2,
            "rejection_counts": {"insufficient_history_samples": 1},
            "candidates": [],
        },
        now_ms=1778787000000,
        max_age_ms=10_000,
    )

    assert not report.ok
    assert report.severity == "critical"
    assert "spread_diagnostics_contract_missing" in report.fingerprints


def test_spread_snapshot_rejects_unknown_future_schema() -> None:
    report = analyze_spread_snapshot(
        {
            "schema_version": 999,
            "decision_at_ms": 1778786998500,
            "published_at_ms": 1778786999000,
            "market_observed_at_ms": 1778786998000,
            "source_mode": "sidecar_snapshot",
            "degraded_venues": [],
            "degraded_symbols": {},
            "input_quote_count": 0,
            "valid_quote_count": 0,
            "evaluated_pair_count": 0,
            "accepted_pair_count": 0,
            "paper_configured_enabled": False,
            "paper_admission_enabled": False,
            "paper_tracked_count": 0,
            "paper_refresh_status": "disabled",
            "paper_event_count": 0,
            "paper_last_success_at_ms": 0,
            "rejection_counts": {},
            "candidates": [],
        },
        now_ms=1778787000000,
        max_age_ms=10_000,
    )

    assert not report.ok
    assert report.severity == "critical"
    assert "spread_diagnostics_contract_missing" in report.fingerprints


def test_spread_snapshot_rejects_non_object_root_without_exception() -> None:
    report = analyze_spread_snapshot(
        [],
        now_ms=1778787000000,
        max_age_ms=10_000,
    )

    assert not report.ok
    assert report.severity == "critical"
    assert report.fingerprints == ["spread_snapshot_root_invalid"]


def test_spread_snapshot_partial_input_is_warning_not_green():
    report = analyze_spread_snapshot(
        {
            "schema_version": 4,
            "decision_at_ms": 1778786998500,
            "published_at_ms": 1778786999000,
            "market_observed_at_ms": 1778786998000,
            "source_mode": "sidecar_snapshot_partial",
            "degraded_venues": [],
            "degraded_symbols": {"rich": ["ETHUSDT"]},
            "input_quote_count": 3,
            "valid_quote_count": 2,
            "evaluated_pair_count": 1,
            "accepted_pair_count": 0,
            "paper_configured_enabled": False,
            "paper_admission_enabled": False,
            "paper_tracked_count": 0,
            "paper_refresh_status": "disabled",
            "paper_event_count": 0,
            "paper_last_success_at_ms": 0,
            "rejection_counts": {"insufficient_history_samples": 1},
            "paper_admission_rejection_counts": {},
            "candidates": [],
        },
        now_ms=1778787000000,
        max_age_ms=10_000,
    )

    assert not report.ok
    assert report.severity == "warning"
    assert "spread_source_sidecar_snapshot_partial" in report.fingerprints
    assert "spread_degraded_inputs" in report.fingerprints


def test_spread_snapshot_scoped_partial_input_with_live_pipeline_is_green():
    report = analyze_spread_snapshot(
        {
            "schema_version": 4,
            "decision_at_ms": 1778786998500,
            "published_at_ms": 1778786999000,
            "market_observed_at_ms": 1778786998000,
            "source_mode": "sidecar_snapshot_partial",
            "degraded_venues": [],
            "degraded_symbols": {"okx": ["ETHUSDT"]},
            "input_quote_count": 3,
            "valid_quote_count": 2,
            "evaluated_pair_count": 1,
            "accepted_pair_count": 0,
            "paper_configured_enabled": False,
            "paper_admission_enabled": False,
            "paper_tracked_count": 0,
            "paper_refresh_status": "disabled",
            "paper_event_count": 0,
            "paper_last_success_at_ms": 0,
            "rejection_counts": {"insufficient_history_samples": 1},
            "paper_admission_rejection_counts": {},
            "candidates": [],
        },
        now_ms=1778787000000,
        max_age_ms=10_000,
    )

    assert report.ok


def test_spread_snapshot_rejects_invalid_count_invariants():
    report = analyze_spread_snapshot(
        {
            "schema_version": 4,
            "decision_at_ms": 1778786998500,
            "published_at_ms": 1778786999000,
            "market_observed_at_ms": 1778786998000,
            "source_mode": "sidecar_snapshot",
            "degraded_venues": [],
            "degraded_symbols": {},
            "input_quote_count": 2,
            "valid_quote_count": 3,
            "evaluated_pair_count": 1,
            "accepted_pair_count": 2,
            "paper_configured_enabled": False,
            "paper_admission_enabled": False,
            "paper_tracked_count": 0,
            "paper_refresh_status": "disabled",
            "paper_event_count": -1,
            "paper_last_success_at_ms": 0,
            "rejection_counts": {"bad": -1},
            "candidates": [{}, {}, {}],
        },
        now_ms=1778787000000,
        max_age_ms=10_000,
    )

    assert not report.ok
    assert "spread_diagnostics_contract_invalid" in report.fingerprints
    assert "spread_diagnostics_count_invariant_invalid" in report.fingerprints


def test_spread_snapshot_malformed_degradation_returns_critical_not_exception():
    report = analyze_spread_snapshot(
        {
            "schema_version": 4,
            "decision_at_ms": 1778786998500,
            "published_at_ms": 1778786999000,
            "market_observed_at_ms": 1778786998000,
            "source_mode": "sidecar_snapshot",
            "degraded_venues": 1,
            "degraded_symbols": {},
            "input_quote_count": 0,
            "valid_quote_count": 0,
            "evaluated_pair_count": 0,
            "accepted_pair_count": 0,
            "paper_configured_enabled": False,
            "paper_admission_enabled": False,
            "paper_tracked_count": 0,
            "paper_refresh_status": "disabled",
            "paper_event_count": 0,
            "paper_last_success_at_ms": 0,
            "rejection_counts": {},
            "candidates": [],
        },
        now_ms=1778787000000,
        max_age_ms=10_000,
    )

    assert not report.ok
    assert report.severity == "critical"
    assert "spread_diagnostics_contract_invalid" in report.fingerprints
    assert report.details["degraded_venues"] == []


def test_spread_snapshot_rejects_causal_and_state_misgreen_paths():
    base = {
        "schema_version": 4,
        "decision_at_ms": 1778786998500,
        "published_at_ms": 1778786999000,
        "market_observed_at_ms": 1778786998000,
        "source_mode": "sidecar_snapshot",
        "degraded_venues": [],
        "degraded_symbols": {},
        "input_quote_count": 0,
        "valid_quote_count": 0,
        "evaluated_pair_count": 0,
        "accepted_pair_count": 0,
        "paper_configured_enabled": False,
        "paper_admission_enabled": False,
        "paper_tracked_count": 0,
        "paper_refresh_status": "disabled",
        "paper_event_count": 0,
        "paper_last_success_at_ms": 0,
        "rejection_counts": {},
        "candidates": [],
    }
    cases = [
        (
            {"market_observed_at_ms": 1778786998600},
            "spread_publication_watermark_invalid",
        ),
        (
            {
                "paper_configured_enabled": True,
                "paper_admission_enabled": True,
                "paper_refresh_status": "success",
                "paper_last_success_at_ms": 1778786998400,
            },
            "spread_paper_refresh_not_proven",
        ),
        ({"source_mode": "mystery"}, "spread_source_mode_unknown"),
        (
            {"rejection_counts": {"impossible": 1}},
            "spread_pair_attribution_incomplete",
        ),
        (
            {
                "paper_tracked_count": 1,
                "paper_event_count": 1,
                "paper_last_success_at_ms": 1778786998500,
            },
            "spread_paper_disabled_state_invalid",
        ),
    ]

    for mutation, expected_fingerprint in cases:
        report = analyze_spread_snapshot(
            {**base, **mutation},
            now_ms=1778787000000,
            max_age_ms=10_000,
        )
        assert not report.ok, mutation
        assert expected_fingerprint in report.fingerprints, mutation


def test_strategy_entry_policy_reports_disabled_live_entries_without_hiding_state():
    class Strategy:
        funding_new_entries_enabled = False
        funding_canary_enabled = False
        spread_reversion_enabled = True
        spread_paper_enabled = True
        spread_live_enabled = False

    report = analyze_strategy_entry_policy(Strategy(), runtime_mode="live")

    assert report.ok
    assert report.details["funding_live_entry_ready"] is False
    assert report.details["spread_paper_ready"] is True
    assert report.details["spread_execution_contract"] == "paper_only"

    required = analyze_strategy_entry_policy(
        Strategy(),
        runtime_mode="live",
        require_entry_enabled=True,
    )
    assert not required.ok
    assert "funding_live_entry_disabled" in required.fingerprints


def test_verifier_spread_ttl_uses_strictest_runtime_and_signal_policy():
    class Runtime:
        sidecar_snapshot_max_age_ms = 10_000

    class Config:
        runtime = Runtime()
        strategy = SimpleNamespace(spread_signal_ttl_ms=1_000)

    assert vps._resolve_spread_snapshot_max_age_ms(None, Config()) == 1_000
    assert vps._resolve_spread_snapshot_max_age_ms(3_000, Config()) == 3_000
    assert vps._resolve_spread_snapshot_max_age_ms(None, None) == 60_000


def test_systemd_active_report_fails_closed_on_inactive(monkeypatch):
    monkeypatch.setattr(
        vps.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=3,
            stdout="inactive\n",
            stderr="",
        ),
    )

    report = vps._systemd_active_report("lightfee-spread-bbo.service")

    assert not report.ok
    assert "systemd_service_not_active" in report.fingerprints


def test_spread_bbo_runtime_rejects_snapshot_from_previous_process(monkeypatch):
    now_ms = 1_778_787_000_000
    snapshot = SimpleNamespace(
        published_at_ms=now_ms - 100,
        batch_started_at_ms=now_ms - 10_000,
        producer_generation_id="boot:41",
        configured_venues=["okx"],
        quotes={
            "okx:BTCUSDT": SimpleNamespace(
                venue="okx",
                observed_at_ms=now_ms - 100,
            )
        },
    )
    config = SimpleNamespace(
        venues=[SimpleNamespace(venue="okx")],
        strategy=SimpleNamespace(spread_signal_ttl_ms=1_000),
    )
    monkeypatch.setattr(vps, "load_spread_quote_snapshot", lambda path: snapshot)
    monkeypatch.setattr(vps, "_systemd_main_pid", lambda name: 42)
    monkeypatch.setattr(vps, "_process_started_at_ms", lambda pid: now_ms - 1_000)
    monkeypatch.setattr(vps, "producer_generation_id", lambda pid: f"boot:{pid}")

    report = vps._spread_bbo_runtime_report(
        "/tmp/spread-bbo.json",
        app_config=config,
        now_ms=now_ms,
    )

    assert not report.ok
    assert "spread_bbo_snapshot_generation_mismatch" in report.fingerprints


def test_spread_bbo_runtime_samples_clock_after_snapshot_load(monkeypatch):
    published_at_ms = 2_000
    snapshot = SimpleNamespace(
        published_at_ms=published_at_ms,
        batch_started_at_ms=1_900,
        producer_generation_id="boot:42",
        configured_venues=["okx"],
        quotes={
            "okx:BTCUSDT": SimpleNamespace(
                venue="okx",
                observed_at_ms=published_at_ms,
            )
        },
    )
    config = SimpleNamespace(
        venues=[SimpleNamespace(venue="okx")],
        strategy=SimpleNamespace(spread_signal_ttl_ms=1_000),
    )
    monkeypatch.setattr(vps, "load_spread_quote_snapshot", lambda path: snapshot)
    monkeypatch.setattr(vps.time, "time", lambda: 2.1)
    monkeypatch.setattr(vps, "_systemd_main_pid", lambda name: 42)
    monkeypatch.setattr(vps, "_process_started_at_ms", lambda pid: 1_800)
    monkeypatch.setattr(vps, "producer_generation_id", lambda pid: f"boot:{pid}")

    report = vps._spread_bbo_runtime_report(
        "/tmp/spread-bbo.json",
        app_config=config,
        now_ms=None,
    )

    assert report.ok
    assert report.details["checked_at_ms"] == 2_100


def test_spread_runtime_samples_clock_after_snapshot_load(monkeypatch):
    snapshot = {
        "schema_version": 4,
        "decision_at_ms": 2_000,
        "published_at_ms": 2_050,
        "market_observed_at_ms": 2_000,
        "source_mode": "sidecar_snapshot",
        "degraded_venues": [],
        "degraded_symbols": {},
        "input_quote_count": 2,
        "valid_quote_count": 2,
        "evaluated_pair_count": 1,
        "accepted_pair_count": 0,
        "paper_configured_enabled": False,
        "paper_admission_enabled": False,
        "paper_tracked_count": 0,
        "paper_refresh_status": "disabled",
        "paper_event_count": 0,
        "paper_last_success_at_ms": 0,
        "rejection_counts": {"insufficient_history": 1},
        "paper_admission_rejection_counts": {},
        "candidates": [],
    }
    read_completed = {"value": False}

    def delayed_read(_path):
        read_completed["value"] = True
        return snapshot

    monkeypatch.setattr(vps, "_read_json", delayed_read)
    monkeypatch.setattr(
        vps.time,
        "time",
        lambda: 2.1 if read_completed["value"] else 1.9,
    )

    report = vps._spread_snapshot_runtime_report(
        "/tmp/spread.json",
        max_age_ms=1_000,
        now_ms=None,
    )

    assert report.ok
    assert report.details["publish_age_ms"] == 50


def test_spread_bbo_runtime_rejects_producer_declared_degradation(monkeypatch):
    now_ms = 2_000
    snapshot = SimpleNamespace(
        published_at_ms=now_ms - 100,
        batch_started_at_ms=now_ms - 200,
        producer_generation_id="boot:42",
        configured_venues=["binance", "okx"],
        degraded_venues=["okx"],
        degraded_symbols={"binance": ["ETHUSDT"]},
        quotes={
            "binance:BTCUSDT": SimpleNamespace(
                venue="binance",
                observed_at_ms=now_ms - 100,
            ),
            "okx:BTCUSDT": SimpleNamespace(
                venue="okx",
                observed_at_ms=now_ms - 100,
            ),
        },
    )
    config = SimpleNamespace(
        venues=[SimpleNamespace(venue="binance"), SimpleNamespace(venue="okx")],
        strategy=SimpleNamespace(spread_signal_ttl_ms=1_000),
    )
    monkeypatch.setattr(vps, "load_spread_quote_snapshot", lambda path: snapshot)
    monkeypatch.setattr(vps, "_systemd_main_pid", lambda name: 42)
    monkeypatch.setattr(vps, "_process_started_at_ms", lambda pid: 1_000)
    monkeypatch.setattr(vps, "producer_generation_id", lambda pid: f"boot:{pid}")

    report = vps._spread_bbo_runtime_report(
        "/tmp/spread-bbo.json",
        app_config=config,
        now_ms=now_ms,
    )

    assert not report.ok
    assert "spread_bbo_venue_degraded" in report.fingerprints
    assert "spread_bbo_symbol_degraded" in report.fingerprints
    assert report.details["degraded_venues"] == ["okx"]
    assert report.details["degraded_symbols"] == {"binance": ["ETHUSDT"]}


def test_current_state_flags_stale_fail_closed_clean_state():
    state = {
        "lifecycle": "running",
        "risk_mode": "fail_closed",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "recovery_blocked_reason": None,
    }
    report = analyze_current_state(state, now_ms=1778787000000, max_tick_age_ms=10_000)
    assert not report.ok
    assert "stale_fail_closed_clean_state" in report.fingerprints


def test_current_state_blocks_only_active_pending_close_reconciliations():
    base_state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "generated_at_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "last_scan": {"ts_ms": 1778786998000},
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
        },
    }

    blocking = analyze_current_state(
        {
            **base_state,
            "pending_close_reconciliation_count": 1,
            "pending_close_reconciliation_blocking_count": 1,
            "pending_close_reconciliation_terminal_flat_count": 0,
            "pending_close_reconciliation_symbols": ["HUSDT"],
        },
        now_ms=1778787000000,
        max_tick_age_ms=10_000,
    )
    assert not blocking.ok
    assert "pending_close_reconciliations_active" in blocking.fingerprints
    assert blocking.details["pending_close_reconciliation_blocking_count"] == 1

    terminal_flat_audit = analyze_current_state(
        {
            **base_state,
            "pending_close_reconciliation_count": 1,
            "pending_close_reconciliation_blocking_count": 0,
            "pending_close_reconciliation_terminal_flat_count": 1,
            "pending_close_reconciliation_symbols": ["HUSDT"],
        },
        now_ms=1778787000000,
        max_tick_age_ms=10_000,
    )
    assert terminal_flat_audit.ok
    assert terminal_flat_audit.details["pending_close_reconciliation_terminal_flat_count"] == 1


def test_current_state_preserves_recent_auto_fail_closed_recovery_as_detail_only():
    state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "last_scan": {"candidate_count": 10, "tradeable_count": 2},
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
        },
        "auto_fail_closed_summary": {
            "recent_incident": True,
            "recovered_count": 1,
            "cleanup_failed_count": 0,
            "latest_event": {
                "kind": "runtime.auto_fail_closed_recovered",
                "final_status": "recovered",
                "symbols": ["LINKUSDT"],
                "venues": ["bybit"],
            },
        },
    }

    report = analyze_current_state(state, now_ms=1778787000000, max_tick_age_ms=10_000)

    assert report.ok
    assert report.fingerprints == []
    assert report.details["auto_fail_closed_summary"]["recent_incident"] is True
    assert report.details["auto_fail_closed_summary"]["latest_event"]["final_status"] == "recovered"


def test_current_state_preserves_recent_stale_risk_alignment_as_detail_only():
    state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "last_scan": {"candidate_count": 10, "tradeable_count": 2},
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
        },
        "stale_risk_state_alignment_summary": {
            "recent_incident": True,
            "aligned_count": 1,
            "blocked_count": 0,
            "latest_event": {
                "kind": "runtime.stale_risk_state_aligned",
                "symbols": ["HOMEUSDT"],
                "venues": ["aster"],
                "new_lifecycle": "running",
                "new_risk_mode": "running",
            },
        },
    }

    report = analyze_current_state(state, now_ms=1778787000000, max_tick_age_ms=10_000)

    assert report.ok
    assert report.fingerprints == []
    summary = report.details["stale_risk_state_alignment_summary"]
    assert summary["recent_incident"] is True
    assert summary["latest_event"]["new_lifecycle"] == "running"


def test_current_state_clean_local_exchange_nonzero_is_critical():
    state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "last_scan": {"candidate_count": 10, "tradeable_count": 2},
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": True,
            "positions": {
                "bybit": {
                    "BIOUSDT": {
                        "venue": "bybit",
                        "symbol": "BIOUSDT",
                        "side": "buy",
                        "quantity": 1444.0,
                        "entry_price": 0.03321,
                    }
                }
            },
        },
    }

    report = analyze_current_state(state, now_ms=1778787000000, max_tick_age_ms=10_000)

    assert not report.ok
    assert report.severity == "critical"
    assert "exchange_truth_mismatch" in report.fingerprints
    assert "nonzero_live_position" in report.fingerprints
    assert report.details["exchange_truth_mismatches"][0]["symbol"] == "BIOUSDT"


def test_current_state_pending_entry_live_conflict_lists_conflict_reasons():
    state = {
        "lifecycle": "risk_only",
        "risk_mode": "fail_closed",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 1,
        "pending_close_count": 0,
        "pending_entries": [
            {
                "pending_id": "entry-home",
                "symbol": "HOMEUSDT",
                "long_venue": "okx",
                "short_venue": "bybit",
                "maker_leg": "long",
                "maker_leg_filled": 1600.0,
                "hedge_leg_filled": 1600.0,
            }
        ],
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": True,
            "positions": {
                "okx": {"HOMEUSDT": {"venue": "okx", "symbol": "HOMEUSDT", "quantity": 0.0}},
                "bybit": {
                    "HOMEUSDT": {
                        "venue": "bybit",
                        "symbol": "HOMEUSDT",
                        "side": "sell",
                        "quantity": 1600.0,
                    }
                },
            },
            "open_orders": {"okx": {"HOMEUSDT": []}, "bybit": {"HOMEUSDT": []}},
        },
    }

    report = analyze_current_state(state, now_ms=1778787000000, max_tick_age_ms=10_000)

    detail = report.details["pending_entry_live_conflicts"]["details"][0]
    assert "okx fill evidence conflicts with okx live flat" in detail["conflict_reasons"]
    assert "live position owned by pending conflict" in detail["conflict_reasons"]


def test_current_state_clean_local_exchange_open_order_is_critical():
    state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "last_scan": {"candidate_count": 10, "tradeable_count": 2},
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": True,
            "positions": {"bybit": {}},
            "open_orders": {
                "bybit": {
                    "*": [
                        {
                            "venue": "bybit",
                            "symbol": "TRXUSDT",
                            "side": "buy",
                            "quantity": 72.0,
                            "reduce_only": False,
                            "order_id": "live-maker",
                        }
                    ]
                }
            },
        },
    }

    report = analyze_current_state(state, now_ms=1778787000000, max_tick_age_ms=10_000)

    assert not report.ok
    assert report.severity == "critical"
    assert "exchange_truth_mismatch" in report.fingerprints
    assert "live_open_order" in report.fingerprints
    mismatch = report.details["exchange_truth_mismatches"][0]
    assert mismatch["check"] == "unexpected_live_open_order"
    assert mismatch["symbol"] == "TRXUSDT"


def test_current_state_local_open_exchange_leg_quantity_mismatch_is_critical():
    state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 1,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "open_positions": [
            {
                "position_id": "pos-beat",
                "symbol": "BEATUSDT",
                "long_venue": "aster",
                "short_venue": "bybit",
                "quantity": 24.0,
            }
        ],
        "last_scan": {"candidate_count": 10, "tradeable_count": 2},
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": True,
            "positions": {
                "aster": {
                    "BEATUSDT": {
                        "venue": "aster",
                        "symbol": "BEATUSDT",
                        "side": "buy",
                        "quantity": 0.0,
                    }
                },
                "bybit": {
                    "BEATUSDT": {
                        "venue": "bybit",
                        "symbol": "BEATUSDT",
                        "side": "sell",
                        "quantity": 9.0,
                    },
                    "BIOUSDT": {
                        "venue": "bybit",
                        "symbol": "BIOUSDT",
                        "side": "buy",
                        "quantity": 1444.0,
                    },
                },
            },
        },
    }

    report = analyze_current_state(state, now_ms=1778787000000, max_tick_age_ms=10_000)

    assert not report.ok
    assert report.severity == "critical"
    assert "exchange_truth_mismatch" in report.fingerprints
    assert "local_exchange_position_mismatch" in report.fingerprints
    checks = {m["check"] for m in report.details["exchange_truth_mismatches"]}
    assert "local_live_leg_missing_or_quantity_mismatch" in checks
    assert "unexpected_live_position" in checks


def test_diagnose_state_consistency_names_exchange_nonzero_local_flat():
    local_state = {
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "positions": [],
    }
    exchange_truth = {
        "available": True,
        "confidence": "high",
        "has_nonzero_position": True,
        "positions": {
            "bybit": {
                "BIOUSDT": {
                    "venue": "bybit",
                    "symbol": "BIOUSDT",
                    "side": "buy",
                    "quantity": 1444.0,
                    "entry_price": 0.03321,
                }
            }
        },
        "fetch_status": {"bybit": {"status": "ok", "positions_failed": []}},
    }

    consistency = _build_state_consistency(local_state, exchange_truth)

    assert consistency["state_mismatch"] is True
    assert consistency["state_verdict"] == "exchange_truth_mismatch"
    assert "exchange_truth_mismatch" in consistency["fingerprints"]
    assert "nonzero_live_position" in consistency["fingerprints"]
    detail = consistency["details"][0]
    assert detail["check"] == "nonzero_live_position"
    assert detail["live_positions"][0]["symbol"] == "BIOUSDT"


def test_diagnose_state_consistency_flags_local_open_live_leg_quantity_mismatch():
    local_state = {
        "open_position_count": 1,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "positions": [
            {
                "position_id": "pos-beat",
                "symbol": "BEATUSDT",
                "long_venue": "aster",
                "short_venue": "bybit",
                "quantity": 24.0,
            }
        ],
    }
    exchange_truth = {
        "available": True,
        "confidence": "high",
        "has_nonzero_position": True,
        "positions": {
            "aster": {
                "BEATUSDT": {
                    "venue": "aster",
                    "symbol": "BEATUSDT",
                    "side": "buy",
                    "quantity": 0.0,
                }
            },
            "bybit": {
                "BEATUSDT": {
                    "venue": "bybit",
                    "symbol": "BEATUSDT",
                    "side": "sell",
                    "quantity": 9.0,
                },
                "BIOUSDT": {
                    "venue": "bybit",
                    "symbol": "BIOUSDT",
                    "side": "buy",
                    "quantity": 1444.0,
                },
            },
        },
        "fetch_status": {
            "aster": {"status": "ok", "positions_failed": []},
            "bybit": {"status": "ok", "positions_failed": []},
        },
    }

    consistency = _build_state_consistency(local_state, exchange_truth)

    assert consistency["state_mismatch"] is True
    assert consistency["state_verdict"] == "exchange_truth_mismatch"
    assert "exchange_truth_mismatch" in consistency["fingerprints"]
    assert "local_exchange_position_mismatch" in consistency["fingerprints"]
    checks = {d["check"] for d in consistency["details"]}
    assert "local_live_leg_missing_or_quantity_mismatch" in checks
    assert "unexpected_live_position" in checks


def test_state_consistency_accepts_exchange_side_enum_labels_for_local_open_legs():
    local_state = {
        "open_position_count": 1,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "positions": [
            {
                "position_id": "pos-home",
                "symbol": "HOMEUSDT",
                "long_venue": "binance",
                "short_venue": "bybit",
                "quantity": 12.0,
            }
        ],
    }
    exchange_truth = {
        "available": True,
        "confidence": "high",
        "has_nonzero_position": True,
        "positions": {
            "binance": {
                "HOMEUSDT": {
                    "venue": "binance",
                    "symbol": "HOMEUSDT",
                    "side": "Side.BUY",
                    "quantity": 12.0,
                },
            },
            "bybit": {
                "HOMEUSDT": {
                    "venue": "bybit",
                    "symbol": "HOMEUSDT",
                    "side": "Side.SELL",
                    "quantity": 12.0,
                },
            },
        },
        "fetch_status": {
            "binance": {"status": "ok", "positions_failed": []},
            "bybit": {"status": "ok", "positions_failed": []},
        },
    }

    consistency = _build_state_consistency(local_state, exchange_truth)
    report = analyze_current_state(
        {
            **local_state,
            "exchange_truth": exchange_truth,
            "last_tick_ms": 1781531700000,
        },
        now_ms=1781531700100,
        max_tick_age_ms=1_000,
    )

    assert "local_exchange_position_mismatch" not in consistency["fingerprints"]
    assert "local_exchange_position_mismatch" not in report.fingerprints


def test_resolver_requires_okx_capable_priority():
    text = "nameserver 42.116.255.180\nnameserver 8.8.8.8\nnameserver 1.1.1.1\n"
    report = analyze_resolver_config(text)
    assert not report.ok
    assert "unverified_resolver_first" in report.fingerprints


def test_summary_is_failed_when_any_report_critical():
    bad = analyze_systemd_unit(
        "lightfee-sidecar.service", "[Service]\nExecStart=lightfee-sidecar\n"
    )
    summary = summarize_reports([bad])
    assert not summary.ok
    assert summary.critical_count == 1


def test_systemd_unit_requires_explicit_nofile_limit_for_live_and_sidecar():
    text = (
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/lightfee-sidecar --config /opt/lightfee-v2/config/live.toml\n"
    )

    report = analyze_systemd_unit("lightfee-sidecar.service", text)

    assert not report.ok
    assert "missing_limit_nofile" in report.fingerprints


def test_systemd_unit_rejects_invalid_nofile_limit():
    text = (
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/lightfee-sidecar --config /opt/lightfee-v2/config/live.toml\n"
        "LimitNOFILE=not-a-number\n"
    )

    report = analyze_systemd_unit("lightfee-sidecar.service", text)

    assert not report.ok
    assert "missing_limit_nofile" in report.fingerprints


def test_live_unit_must_not_require_sidecar_service():
    text = (
        "[Unit]\n"
        "Requires=lightfee-sidecar.service\n"
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "LimitNOFILE=65536\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.live --config /opt/lightfee-v2/config/live.toml\n"
    )

    report = analyze_systemd_unit("lightfee-live.service", text)

    assert not report.ok
    assert "live_requires_sidecar_service" in report.fingerprints


def test_verify_production_services_cli_json_success(tmp_path):
    unit_dir = tmp_path / "systemd"
    unit_dir.mkdir()
    (unit_dir / "lightfee-sidecar.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "Environment=LIGHTFEE_EXTERNAL_SPREAD_BBO=1\n"
        "ExecStart=/root/projects/LightFee/target/release/opportunity_input_sidecar --config /root/projects/LightFee/config/live.auto.toml\n"
        "LimitNOFILE=65536\n"
    )
    (unit_dir / "lightfee-spread-bbo.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.spread_bbo --config /opt/lightfee-v2/config/live.toml\n"
        "LimitNOFILE=65536\n"
    )
    (unit_dir / "lightfee-spread-sidecar.service").write_text(
        "[Unit]\nWants=lightfee-spread-bbo.service\n"
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.spread_sidecar --config /opt/lightfee-v2/config/live.toml\n"
        "LimitNOFILE=65536\n"
    )
    (unit_dir / "lightfee-live.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.live --config /opt/lightfee-v2/config/live.toml\n"
        "LimitNOFILE=65536\n"
    )
    snapshot = tmp_path / "snapshot.json"
    venues = ["aster", "binance", "bitget", "bybit", "gate", "hyperliquid", "okx"]
    snapshot.write_text(
        json.dumps(
            {
                "schema_version": 4,
                "published_at_ms": 1778786999000,
                "market_observed_at_ms": 1778786998000,
                "candidate_build_observed_at_ms": 1778786998500,
                "candidate_build_diagnostics": {
                    "input_quote_count": 7,
                    "requested_symbol_count": 1,
                    "requested_symbols": ["BTCUSDT"],
                    "requested_venues": venues,
                    "directional_pair_count": 0,
                    "output_candidate_count": 0,
                    "future_input_quote_count": 0,
                    "rejection_counts": {},
                },
                "quotes": {
                    f"{v}:BTCUSDT": {
                        "venue": v,
                        "symbol": "BTCUSDT",
                        "bid": 65000,
                        "ask": 65001,
                        "observed_at_ms": 1778786998000,
                        "funding_rate_bps": 1.0,
                        "funding_timestamp_ms": 1778815798000,
                        "funding_interval_ms": 28_800_000,
                    }
                    for v in venues
                },
                "degraded_venues": [],
                "degraded_domains": [],
                "degraded_symbols": {},
                **_complete_quote_lifecycles(venues, 1778786998000),
                "transfer_lifecycle": [],
                "source_mode": "direct_market",
                "acquisition_mode": "fresh_sidecar",
                "candidates": [],
            }
        )
    )
    spread_snapshot = tmp_path / "spread-snapshot.json"
    spread_snapshot.write_text(
        json.dumps(
            {
                "schema_version": 4,
                "decision_at_ms": 1778786998500,
                "published_at_ms": 1778786999000,
                "market_observed_at_ms": 1778786998000,
                "source_mode": "sidecar_snapshot",
                "degraded_venues": [],
                "degraded_symbols": {},
                "input_quote_count": 7,
                "valid_quote_count": 7,
                "evaluated_pair_count": 3,
                "accepted_pair_count": 0,
                "paper_configured_enabled": False,
                "paper_admission_enabled": False,
                "paper_tracked_count": 0,
                "paper_refresh_status": "disabled",
                "paper_event_count": 0,
                "paper_last_success_at_ms": 0,
                "rejection_counts": {"insufficient_history": 3},
                "paper_admission_rejection_counts": {},
                "candidates": [],
            }
        )
    )
    current = tmp_path / "current.json"
    current.write_text(
        json.dumps(
            {
                "lifecycle": "running",
                "risk_mode": "running",
                "last_tick_ms": 1778786999000,
                "open_position_count": 0,
                "pending_entry_count": 0,
                "pending_close_count": 0,
                "last_scan": {"candidate_count": 10, "tradeable_count": 2},
                "exchange_truth": {
                    "available": True,
                    "confidence": "high",
                    "has_nonzero_position": False,
                    "has_open_order": False,
                    "positions": {},
                    "open_orders": {},
                },
            }
        )
    )
    resolv = tmp_path / "resolv.conf"
    resolv.write_text("nameserver 1.1.1.1\nnameserver 8.8.8.8\n")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/verify_production_services.py",
            "--unit-dir",
            str(unit_dir),
            "--snapshot",
            str(snapshot),
            "--spread-snapshot",
            str(spread_snapshot),
            "--current-state",
            str(current),
            "--resolv-conf",
            str(resolv),
            "--now-ms",
            "1778787000000",
            "--json",
        ],
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr + result.stdout
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    reports = {report["name"]: report for report in payload["reports"]}
    assert reports["spread_snapshot"]["ok"] is True


def test_verify_production_services_cli_default_allows_production_scan_gap(tmp_path):
    unit_dir = tmp_path / "systemd"
    unit_dir.mkdir()
    (unit_dir / "lightfee-sidecar.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "Environment=LIGHTFEE_EXTERNAL_SPREAD_BBO=1\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/lightfee-sidecar --config /opt/lightfee-v2/config/live.toml\n"
        "LimitNOFILE=65536\n"
    )
    (unit_dir / "lightfee-spread-bbo.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.spread_bbo --config /opt/lightfee-v2/config/live.toml\n"
        "LimitNOFILE=65536\n"
    )
    (unit_dir / "lightfee-spread-sidecar.service").write_text(
        "[Unit]\nWants=lightfee-spread-bbo.service\n"
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.spread_sidecar --config /opt/lightfee-v2/config/live.toml\n"
        "LimitNOFILE=65536\n"
    )
    (unit_dir / "lightfee-live.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.live --config /opt/lightfee-v2/config/live.toml\n"
        "LimitNOFILE=65536\n"
    )
    snapshot = tmp_path / "snapshot.json"
    venues = ["aster", "binance", "bitget", "bybit", "gate", "hyperliquid", "okx"]
    snapshot.write_text(
        json.dumps(
            {
                "schema_version": 4,
                "published_at_ms": 1778786956000,
                "market_observed_at_ms": 1778786955000,
                "candidate_build_observed_at_ms": 1778786955500,
                "candidate_build_diagnostics": {
                    "input_quote_count": 7,
                    "requested_symbol_count": 1,
                    "requested_symbols": ["BTCUSDT"],
                    "requested_venues": venues,
                    "directional_pair_count": 0,
                    "output_candidate_count": 0,
                    "future_input_quote_count": 0,
                    "rejection_counts": {},
                },
                "quotes": {
                    f"{v}:BTCUSDT": {
                        "venue": v,
                        "symbol": "BTCUSDT",
                        "bid": 65000,
                        "ask": 65001,
                        "observed_at_ms": 1778786955000,
                        "funding_rate_bps": 1.0,
                        "funding_timestamp_ms": 1778815755000,
                        "funding_interval_ms": 28_800_000,
                    }
                    for v in venues
                },
                "degraded_venues": [],
                "degraded_domains": [],
                "degraded_symbols": {},
                **_complete_quote_lifecycles(venues, 1778786955000),
                "transfer_lifecycle": [],
                "source_mode": "direct_market",
                "acquisition_mode": "fresh_sidecar",
                "candidates": [],
            }
        )
    )
    current = tmp_path / "current.json"
    current.write_text(
        json.dumps(
            {
                "lifecycle": "running",
                "risk_mode": "running",
                "last_tick_ms": 1778786999000,
                "open_position_count": 0,
                "pending_entry_count": 0,
                "pending_close_count": 0,
                "last_scan": {"candidate_count": 10, "tradeable_count": 2},
                "exchange_truth": {
                    "available": True,
                    "confidence": "high",
                    "has_nonzero_position": False,
                    "has_open_order": False,
                    "positions": {},
                    "open_orders": {},
                },
            }
        )
    )
    resolv = tmp_path / "resolv.conf"
    resolv.write_text("nameserver 1.1.1.1\nnameserver 8.8.8.8\n")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/verify_production_services.py",
            "--unit-dir",
            str(unit_dir),
            "--snapshot",
            str(snapshot),
            "--current-state",
            str(current),
            "--resolv-conf",
            str(resolv),
            "--now-ms",
            "1778787000000",
            "--json",
        ],
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr + result.stdout
    payload = json.loads(result.stdout)
    assert payload["ok"] is True


def test_verify_production_services_cli_checks_spread_sidecar_unit(tmp_path):
    unit_dir = tmp_path / "systemd"
    unit_dir.mkdir()
    (unit_dir / "lightfee-sidecar.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/lightfee-sidecar --config /opt/lightfee-v2/config/live.toml\n"
        "LimitNOFILE=65536\n"
    )
    (unit_dir / "lightfee-spread-sidecar.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/lightfee-spread-sidecar --config /opt/lightfee-v2/config/live.toml\n"
    )
    (unit_dir / "lightfee-live.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.live --config /opt/lightfee-v2/config/live.toml\n"
        "LimitNOFILE=65536\n"
    )
    snapshot = tmp_path / "snapshot.json"
    venues = ["aster", "binance", "bitget", "bybit", "gate", "hyperliquid", "okx"]
    snapshot.write_text(
        json.dumps(
            {
                "market_observed_at_ms": 1778786998000,
                "quotes": {
                    f"{v}:BTCUSDT": {"venue": v, "symbol": "BTCUSDT", "bid": 65000, "ask": 65001}
                    for v in venues
                },
                "degraded_venues": [],
            }
        )
    )
    current = tmp_path / "current.json"
    current.write_text(
        json.dumps(
            {
                "lifecycle": "running",
                "risk_mode": "running",
                "last_tick_ms": 1778786999000,
                "open_position_count": 0,
                "pending_entry_count": 0,
                "pending_close_count": 0,
                "last_scan": {"candidate_count": 10, "tradeable_count": 2},
                "exchange_truth": {
                    "available": True,
                    "confidence": "high",
                    "has_nonzero_position": False,
                    "has_open_order": False,
                    "positions": {},
                    "open_orders": {},
                },
            }
        )
    )
    resolv = tmp_path / "resolv.conf"
    resolv.write_text("nameserver 1.1.1.1\nnameserver 8.8.8.8\n")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/verify_production_services.py",
            "--unit-dir",
            str(unit_dir),
            "--snapshot",
            str(snapshot),
            "--current-state",
            str(current),
            "--resolv-conf",
            str(resolv),
            "--now-ms",
            "1778787000000",
            "--json",
        ],
        text=True,
        capture_output=True,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    reports = {report["name"]: report for report in payload["reports"]}
    report = reports["systemd:lightfee-spread-sidecar.service"]
    assert "missing_limit_nofile" in report["fingerprints"]


def test_verify_production_services_cli_requires_exchange_truth_evidence(tmp_path):
    unit_dir = tmp_path / "systemd"
    unit_dir.mkdir()
    (unit_dir / "lightfee-sidecar.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/lightfee-sidecar --config /opt/lightfee-v2/config/live.toml\n"
        "LimitNOFILE=65536\n"
    )
    (unit_dir / "lightfee-spread-sidecar.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.spread_sidecar --config /opt/lightfee-v2/config/live.toml\n"
        "LimitNOFILE=65536\n"
    )
    (unit_dir / "lightfee-live.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.live --config /opt/lightfee-v2/config/live.toml\n"
        "LimitNOFILE=65536\n"
    )
    snapshot = tmp_path / "snapshot.json"
    venues = ["aster", "binance", "bitget", "bybit", "gate", "hyperliquid", "okx"]
    snapshot.write_text(
        json.dumps(
            {
                "market_observed_at_ms": 1778786998000,
                "quotes": {
                    f"{v}:BTCUSDT": {"venue": v, "symbol": "BTCUSDT", "bid": 65000, "ask": 65001}
                    for v in venues
                },
                "degraded_venues": [],
            }
        )
    )
    current = tmp_path / "current.json"
    current.write_text(
        json.dumps(
            {
                "schema": "lightfee.current_state.v1",
                "mode": "live",
                "lifecycle": "running",
                "risk_mode": "running",
                "last_tick_ms": 1778786999000,
                "open_position_count": 0,
                "pending_entry_count": 0,
                "pending_close_count": 0,
                "last_scan": {"candidate_count": 10, "tradeable_count": 2},
            }
        )
    )
    resolv = tmp_path / "resolv.conf"
    resolv.write_text("nameserver 1.1.1.1\nnameserver 8.8.8.8\n")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/verify_production_services.py",
            "--unit-dir",
            str(unit_dir),
            "--snapshot",
            str(snapshot),
            "--current-state",
            str(current),
            "--resolv-conf",
            str(resolv),
            "--now-ms",
            "1778787000000",
            "--json",
        ],
        text=True,
        capture_output=True,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    current_report = [report for report in payload["reports"] if report["name"] == "current_state"][
        0
    ]
    assert "exchange_truth_missing" in current_report["fingerprints"]
    assert current_report["details"]["exchange_truth_required"] is True
    assert current_report["details"]["recovery_decision"]["kind"] == "RUNNING_WITH_EVIDENCE_GAP"


def test_verify_production_services_attaches_exchange_truth_from_systemd_env_file(
    tmp_path,
    monkeypatch,
):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    current_state = runtime_dir / "live-state-current.json"
    env_file = tmp_path / "lightfee.env"
    env_file.write_text(
        "LIGHTFEE_BYBIT_API_KEY=key-from-file\nLIGHTFEE_BYBIT_API_SECRET=secret-from-file\n"
    )
    state = {
        "schema": "lightfee.current_state.v1",
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
    }
    seen: dict[str, object] = {}

    def fake_exchange_truth(runtime_dir_arg, symbols, venues=None):
        seen["runtime_dir"] = runtime_dir_arg
        seen["symbols"] = list(symbols)
        seen["venues"] = venues
        seen["api_key"] = os.environ.get("LIGHTFEE_BYBIT_API_KEY")
        return {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        }

    monkeypatch.delenv("LIGHTFEE_BYBIT_API_KEY", raising=False)
    enriched = vps._attach_exchange_truth_if_missing(
        state,
        current_state_path=current_state,
        unit_texts={
            "lightfee-live.service": (f"[Service]\nEnvironmentFile={env_file}\n"),
        },
        exchange_truth_builder=fake_exchange_truth,
    )

    assert enriched["exchange_truth"]["available"] is True
    assert enriched["exchange_truth_source"] == "verify_production_services_probe"
    assert seen["runtime_dir"] == str(runtime_dir)
    assert seen["symbols"] == []
    assert seen["venues"] is None
    assert seen["api_key"] == "key-from-file"


def test_verify_production_services_attaches_auto_fail_closed_summary(tmp_path):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    current_state = runtime_dir / "live-state-current.json"
    state = {"generated_at_ms": 1778787000000}
    current_state.write_text(json.dumps(state))
    (runtime_dir / "events.jsonl").write_text(
        json.dumps(
            {
                "ts_ms": 1778786998000,
                "kind": "runtime.auto_fail_closed_recovered",
                "payload": {
                    "source": "auto_pending_entry_abort",
                    "reason": "deadline breach",
                    "symbols": ["LINKUSDT"],
                    "venues": ["bybit"],
                    "new_risk_mode": "running",
                    "residual_blockers": [],
                },
            }
        )
        + "\n"
    )

    enriched = vps._attach_auto_fail_closed_summary_if_missing(
        state,
        current_state_path=current_state,
    )

    summary = enriched["auto_fail_closed_summary"]
    assert summary["recent_incident"] is True
    assert summary["recovered_count"] == 1
    assert summary["latest_event"]["symbols"] == ["LINKUSDT"]


def test_verify_production_services_attaches_stale_risk_state_alignment_summary(tmp_path):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    current_state = runtime_dir / "live-state-current.json"
    state = {"generated_at_ms": 1778787000000}
    current_state.write_text(json.dumps(state))
    (runtime_dir / "events.jsonl").write_text(
        json.dumps(
            {
                "ts_ms": 1778786998000,
                "kind": "runtime.stale_risk_state_aligned",
                "payload": {
                    "source": "repair_stale_risk_state",
                    "symbols": ["HOMEUSDT"],
                    "venues": ["aster"],
                    "previous_lifecycle": "risk_only",
                    "previous_risk_mode": "running",
                    "new_lifecycle": "running",
                    "new_risk_mode": "running",
                    "terminalized_records": ["rec-1"],
                },
            }
        )
        + "\n"
    )

    enriched = vps._attach_stale_risk_state_alignment_summary_if_missing(
        state,
        current_state_path=current_state,
    )

    summary = enriched["stale_risk_state_alignment_summary"]
    assert summary["recent_incident"] is True
    assert summary["aligned_count"] == 1
    assert summary["latest_event"]["symbols"] == ["HOMEUSDT"]


def test_verify_production_services_ignores_old_or_unrelated_jsonl_events(tmp_path):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    current_state = runtime_dir / "live-state-current.json"
    state = {"generated_at_ms": 1778787000000}
    current_state.write_text(json.dumps(state))
    old_ts = 1778787000000 - (25 * 3600 * 1000)
    (runtime_dir / "events.jsonl").write_text(
        json.dumps(
            {
                "ts_ms": old_ts,
                "kind": "runtime.auto_fail_closed_recovered",
                "payload": {
                    "source": "auto_pending_entry_abort",
                    "symbols": ["OLDUSDT"],
                    "new_risk_mode": "running",
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (runtime_dir / "unrelated.jsonl").write_text(
        json.dumps(
            {
                "ts_ms": 1778786999000,
                "kind": "runtime.auto_fail_closed_recovered",
                "payload": {
                    "source": "auto_pending_entry_abort",
                    "symbols": ["NOISEUSDT"],
                    "new_risk_mode": "running",
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    enriched = vps._attach_auto_fail_closed_summary_if_missing(
        state,
        current_state_path=current_state,
    )

    assert "auto_fail_closed_summary" not in enriched


def test_verify_production_services_exchange_truth_probe_times_out(
    tmp_path,
    monkeypatch,
):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    current_state = runtime_dir / "live-state-current.json"
    state = {
        "schema": "lightfee.current_state.v1",
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
    }

    def stuck_exchange_truth(_runtime_dir_arg, _symbols, _venues=None):
        time.sleep(0.2)
        return {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        }

    monkeypatch.setattr(vps, "EXCHANGE_TRUTH_PROBE_TIMEOUT_S", 0.01, raising=False)
    started_at = time.monotonic()

    enriched = vps._attach_exchange_truth_if_missing(
        state,
        current_state_path=current_state,
        unit_texts={},
        exchange_truth_builder=stuck_exchange_truth,
    )

    elapsed_s = time.monotonic() - started_at
    assert elapsed_s < 0.15
    assert enriched["exchange_truth"]["available"] is False
    assert "exchange_truth_fetch_failed" in enriched["exchange_truth"]["missing_evidence"]
    assert "timed out" in enriched["exchange_truth"]["errors"][0].lower()


def test_verify_production_services_preserves_exchange_truth_probe_evidence(tmp_path):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    current_state = runtime_dir / "live-state-current.json"
    state = {
        "schema": "lightfee.current_state.v1",
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
    }

    def fake_exchange_truth(_runtime_dir_arg, _symbols, _venues=None):
        return {
            "available": True,
            "truth_available": True,
            "confidence": "partial",
            "positions": {},
            "open_orders": {},
            "fetch_status": {
                "bybit": {"status": "ok"},
                "okx": {
                    "status": "retryable_error",
                    "error": "HTTP 429 rate limit; retry after 1s",
                },
            },
            "open_order_probe_evidence": {
                "okx": {
                    "TRXUSDT": {
                        "classification": "open_order_probe_retryable_error",
                        "endpoint": "/api/v5/trade/orders-pending",
                        "method": "GET",
                        "error": "HTTP 429 rate limit; retry after 1s",
                    }
                }
            },
        }

    enriched = vps._attach_exchange_truth_if_missing(
        state,
        current_state_path=current_state,
        unit_texts={},
        exchange_truth_builder=fake_exchange_truth,
    )

    exchange_truth = enriched["exchange_truth"]
    assert exchange_truth["fetch_status"]["okx"]["status"] == "retryable_error"
    assert exchange_truth["errors"] == ["okx: HTTP 429 rate limit; retry after 1s"]
    assert exchange_truth["probe_evidence"][0]["classification"] == (
        "open_order_probe_retryable_error"
    )
    assert exchange_truth["probe_evidence"][0]["error"] == ("HTTP 429 rate limit; retry after 1s")


def test_production_gate_does_not_report_clean_when_open_orders_present():
    state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "last_scan": {"candidate_count": 1, "tradeable_count": 1},
        "exchange_truth": {
            "available": True,
            "truth_available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": True,
            "positions": {"bybit": {}},
            "open_orders": {
                "bybit": {
                    "TRXUSDT": [
                        {
                            "venue": "bybit",
                            "symbol": "TRXUSDT",
                            "side": "buy",
                            "quantity": 72.0,
                            "reduce_only": False,
                            "order_id": "live-maker",
                        }
                    ]
                }
            },
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=10_000,
        require_exchange_truth=True,
    )

    assert report.ok is False
    assert report.severity == "critical"
    assert "exchange_truth_mismatch" in report.fingerprints
    assert "live_open_order" in report.fingerprints
    assert report.details["recovery_decision"]["kind"] == ("BLOCK_OR_FLATTEN_LIVE_ARTIFACT")
    assert report.details["exchange_truth_mismatches"][0]["check"] == ("unexpected_live_open_order")


def test_current_state_weak_order_truth_gap_is_not_green_even_when_flat():
    state = {
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786999000,
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "last_scan": {"candidate_count": 1, "tradeable_count": 1},
        "recent_events": [
            {
                "kind": "exit.passive_close_hedge_confirmed_after_ack",
                "payload": {
                    "position_id": "pos-weak-truth",
                    "symbol": "BTCUSDT",
                    "order_truth_fill_status": "truth_gap",
                    "order_truth_evidence_status": "unavailable",
                    "order_truth_decision": "retain_backoff",
                    "order_truth_missing_evidence": ["fill_confirmation"],
                    "terminal_without_truth": False,
                },
            }
        ],
        "exchange_truth": {
            "available": True,
            "truth_available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {"bybit": {}},
            "open_orders": {"bybit": {}},
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=10_000,
        require_exchange_truth=True,
    )

    assert report.ok is False
    assert report.severity == "critical"
    assert "order_truth_gap_unresolved" in report.fingerprints
    assert report.details["weak_order_truth_events"][0]["symbol"] == "BTCUSDT"


def test_verify_production_services_cli_json_failure(tmp_path):
    unit_dir = tmp_path / "systemd"
    unit_dir.mkdir()
    (unit_dir / "lightfee-sidecar.service").write_text("[Service]\nExecStart=lightfee-sidecar\n")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/verify_production_services.py",
            "--unit-dir",
            str(unit_dir),
            "--now-ms",
            "1778787000000",
            "--json",
        ],
        text=True,
        capture_output=True,
    )
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["critical_count"] >= 1


def test_deploy_systemd_templates_pass_contract():
    sidecar = Path("deploy/systemd/lightfee-sidecar.service").read_text()
    spread_bbo = Path("deploy/systemd/lightfee-spread-bbo.service").read_text()
    spread_sidecar = Path("deploy/systemd/lightfee-spread-sidecar.service").read_text()
    live = Path("deploy/systemd/lightfee-live.service").read_text()
    assert analyze_systemd_unit("lightfee-sidecar.service", sidecar).ok
    assert analyze_systemd_unit("lightfee-spread-bbo.service", spread_bbo).ok
    assert analyze_systemd_unit("lightfee-spread-sidecar.service", spread_sidecar).ok
    assert analyze_systemd_unit("lightfee-live.service", live).ok


def test_trade_optimization_report_timer_is_six_hour_readonly_job():
    service = Path("deploy/systemd/lightfee-trade-optimization-report.service").read_text()
    timer = Path("deploy/systemd/lightfee-trade-optimization-report.timer").read_text()

    assert "Type=oneshot" in service
    assert "ExecStart=/opt/lightfee-v2/scripts/run_trade_optimization_report.sh" in service
    assert "lightfee.apps.live" not in service
    assert "OnUnitActiveSec=6h" in timer
    assert "Persistent=true" in timer
    assert "Unit=lightfee-trade-optimization-report.service" in timer


def test_spread_sidecar_systemd_template_uses_module_entrypoint():
    text = Path("deploy/systemd/lightfee-spread-sidecar.service").read_text()
    assert ".venv/bin/python3 -m lightfee.apps.spread_sidecar" in text
    assert ".venv/bin/lightfee-spread-sidecar" not in text


def test_deploy_dns_template_prefers_verified_resolver():
    text = Path("deploy/network/NetworkManager-lightfee-dns.conf").read_text()
    assert analyze_resolver_config(text).ok


def test_current_state_tick_stale_is_not_critical_when_flat_with_recent_scan_progress():
    state = {
        "schema": "lightfee.current_state.v1",
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786960000,
        "last_scan": {
            "ts_ms": 1778786995000,
            "candidate_count": 12,
            "selected_candidate_count": 0,
            "dispatched_candidate_count": 0,
        },
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=15_000,
        require_exchange_truth=True,
    )

    assert report.ok is True
    assert "live_tick_stale" not in report.fingerprints
    assert report.details["tick_stale_suppressed_by_runtime_progress"] is True


def test_current_state_tick_stale_remains_critical_with_exporter_only_heartbeat():
    state = {
        "schema": "lightfee.current_state.v1",
        "generated_at_ms": 1778786995000,
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786960000,
        "last_scan": {
            "ts_ms": 1778786800000,
            "candidate_count": 12,
            "selected_candidate_count": 0,
            "dispatched_candidate_count": 0,
        },
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=15_000,
        require_exchange_truth=True,
    )

    assert report.ok is False
    assert report.severity == "critical"
    assert "live_tick_stale" in report.fingerprints
    assert "exporter_only_progress" in report.fingerprints
    assert report.details["tick_stale_suppressed_by_runtime_progress"] is False
    assert report.details["current_state_age_ms"] == 5000
    assert report.details["progress_source"] == "exporter_only"
    assert report.details["exporter_only_progress"] is True


def test_current_state_tick_stale_is_not_critical_with_recent_runtime_lane_progress():
    state = {
        "schema": "lightfee.current_state.v1",
        "generated_at_ms": 1778786995000,
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786960000,
        "last_scan": {
            "ts_ms": 1778786800000,
            "candidate_count": 12,
        },
        "runtime_progress": {
            "loop_iteration_started_ms": 1778786993000,
            "loop_iteration_completed_ms": 1778786990000,
            "last_lane_progress_ms": 1778786994000,
            "active_lane": "",
            "active_lane_started_ms": 0,
            "active_lane_budget_ms": 0,
            "active_lane_overdue": False,
        },
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=15_000,
        require_exchange_truth=True,
    )

    assert report.ok is True
    assert "live_tick_stale" not in report.fingerprints
    assert report.details["tick_stale_suppressed_by_runtime_progress"] is True
    assert report.details["progress_source"] == "runtime_lane"
    assert report.details["exporter_only_progress"] is False


def test_current_state_tick_stale_is_not_critical_with_bounded_active_lane():
    state = {
        "schema": "lightfee.current_state.v1",
        "generated_at_ms": 1778786995000,
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786960000,
        "last_scan": {
            "ts_ms": 1778786800000,
            "candidate_count": 12,
        },
        "runtime_progress": {
            "loop_iteration_started_ms": 1778786993000,
            "loop_iteration_completed_ms": 1778786990000,
            "last_lane_progress_ms": 1778786900000,
            "active_lane": "full_tick",
            "active_lane_started_ms": 1778786992000,
            "active_lane_budget_ms": 15_000,
            "active_lane_overdue": False,
        },
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=15_000,
        require_exchange_truth=True,
    )

    assert report.ok is True
    assert "live_tick_stale" not in report.fingerprints
    assert report.details["tick_stale_suppressed_by_runtime_progress"] is True
    assert report.details["progress_source"] == "active_bounded_lane"


def test_current_state_journal_positive_fill_conflict_owns_historical_live_single_leg():
    state = {
        "schema": "lightfee.current_state.v1",
        "generated_at_ms": 1778786995000,
        "mode": "live",
        "lifecycle": "risk_only",
        "risk_mode": "running",
        "last_tick_ms": 1778786990000,
        "last_scan": {"ts_ms": 1778786990000},
        "open_position_count": 0,
        "open_positions": [],
        "pending_entry_count": 0,
        "pending_entries": [],
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "journal_events": [
            {
                "kind": "pending_entry.positive_fill_live_truth_conflict",
                "payload": {
                    "entry_id": "entry-home",
                    "symbol": "HOMEUSDT",
                    "maker_leg_filled": 1600.0,
                    "hedge_leg_filled": 1600.0,
                    "matched_quantity": 1600.0,
                    "live_long_quantity": 0.0,
                    "live_short_quantity": 1600.0,
                    "live_balanced_quantity": 0.0,
                },
            }
        ],
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": True,
            "has_open_order": False,
            "positions": {
                "bybit": {
                    "HOMEUSDT": {
                        "venue": "bybit",
                        "symbol": "HOMEUSDT",
                        "side": "Side.SELL",
                        "quantity": 1600.0,
                    }
                },
                "okx": {},
            },
            "open_orders": {
                "bybit": {"HOMEUSDT": []},
                "okx": {"HOMEUSDT": []},
            },
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=15_000,
        require_exchange_truth=True,
    )

    rows = report.details["v1_lifecycle_closure"]["rows"]
    assert "nonzero_live_position" in report.fingerprints
    assert any(
        row["owner_id"] == "entry-home"
        and row["terminality"] == "owned_pending_entry_live_conflict"
        and row["details"].get("kind") == "owned_pending_entry_live_conflict"
        for row in rows
    )
    assert not any("unpaired_live_position" in row["row_key"] for row in rows)


def test_current_state_tick_stale_is_critical_when_active_lane_overdue():
    state = {
        "schema": "lightfee.current_state.v1",
        "generated_at_ms": 1778786995000,
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786960000,
        "last_scan": {
            "ts_ms": 1778786800000,
            "candidate_count": 12,
        },
        "runtime_progress": {
            "loop_iteration_started_ms": 1778786965000,
            "loop_iteration_completed_ms": 1778786900000,
            "last_lane_progress_ms": 1778786900000,
            "active_lane": "full_tick",
            "active_lane_started_ms": 1778786965000,
            "active_lane_budget_ms": 15_000,
            "active_lane_overdue": True,
        },
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=15_000,
        require_exchange_truth=True,
    )

    assert report.ok is False
    assert report.severity == "critical"
    assert "live_tick_stale" in report.fingerprints
    assert report.details["tick_stale_suppressed_by_runtime_progress"] is False
    assert report.details["progress_source"] == "exporter_only"


def test_current_state_tick_stale_is_not_critical_when_flat_with_recent_progress_and_medium_truth():
    state = {
        "schema": "lightfee.current_state.v1",
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786960000,
        "last_scan": {
            "ts_ms": 1778786995000,
            "candidate_count": 12,
            "selected_candidate_count": 0,
            "dispatched_candidate_count": 0,
        },
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "exchange_truth": {
            "available": True,
            "confidence": "medium",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=15_000,
        require_exchange_truth=True,
    )

    assert "live_tick_stale" not in report.fingerprints
    assert "exchange_truth_confidence_not_high" in report.fingerprints
    assert report.severity == "critical"
    assert report.details["tick_stale_suppressed_by_runtime_progress"] is True


def test_current_state_tick_stale_remains_critical_without_runtime_progress():
    state = {
        "schema": "lightfee.current_state.v1",
        "mode": "live",
        "lifecycle": "running",
        "risk_mode": "running",
        "last_tick_ms": 1778786960000,
        "last_scan": {
            "ts_ms": 1778786900000,
            "candidate_count": 12,
        },
        "open_position_count": 0,
        "pending_entry_count": 0,
        "pending_close_count": 0,
        "pending_residual_repair_count": 0,
        "exchange_truth": {
            "available": True,
            "confidence": "high",
            "has_nonzero_position": False,
            "has_open_order": False,
            "positions": {},
            "open_orders": {},
        },
    }

    report = analyze_current_state(
        state,
        now_ms=1778787000000,
        max_tick_age_ms=15_000,
        require_exchange_truth=True,
    )

    assert report.ok is False
    assert report.severity == "critical"
    assert "live_tick_stale" in report.fingerprints
    assert report.details["tick_stale_suppressed_by_runtime_progress"] is False


def test_cli_reports_missing_snapshot_and_current_state(tmp_path):
    unit_dir = tmp_path / "systemd"
    unit_dir.mkdir()
    (unit_dir / "lightfee-sidecar.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/root/projects/LightFee/target/release/opportunity_input_sidecar --config /root/projects/LightFee/config/live.auto.toml\n"
        "LimitNOFILE=65536\n"
    )
    (unit_dir / "lightfee-spread-sidecar.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.spread_sidecar --config /opt/lightfee-v2/config/live.toml\n"
        "LimitNOFILE=65536\n"
    )
    (unit_dir / "lightfee-live.service").write_text(
        "[Service]\n"
        "EnvironmentFile=/etc/lightfee/lightfee.env\n"
        "ExecStart=/opt/lightfee-v2/.venv/bin/python3 -m lightfee.apps.live --config /opt/lightfee-v2/config/live.toml\n"
        "LimitNOFILE=65536\n"
    )
    resolv = tmp_path / "resolv.conf"
    resolv.write_text("nameserver 1.1.1.1\nnameserver 8.8.8.8\n")

    # snapshot and current-state paths point to non-existent files
    snapshot = tmp_path / "no-such-snapshot.json"
    current = tmp_path / "no-such-current.json"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/verify_production_services.py",
            "--unit-dir",
            str(unit_dir),
            "--snapshot",
            str(snapshot),
            "--current-state",
            str(current),
            "--resolv-conf",
            str(resolv),
            "--now-ms",
            "1778787000000",
            "--json",
        ],
        text=True,
        capture_output=True,
    )
    assert result.returncode == 1, result.stderr + result.stdout
    payload = json.loads(result.stdout)
    assert payload["critical_count"] >= 2  # snapshot + current-state both critical
    fingerprints = [fp for r in payload["reports"] for fp in r.get("fingerprints", [])]
    assert "snapshot_file_missing" in fingerprints
    assert "current_state_file_missing" in fingerprints
