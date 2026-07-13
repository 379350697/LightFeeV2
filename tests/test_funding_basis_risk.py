"""Adversarial tests for dynamic paired-basis Expected Shortfall admission."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from lightfee.config.schema import AppConfig
from lightfee.config.validation import validate_config
from lightfee.engine.funding_risk_runtime import FundingRiskRuntime
from lightfee.strategy.funding_basis_risk import (
    FundingBasisExpectedShortfallModel,
    restore_funding_basis_risk_checkpoint,
)
from lightfee.strategy.risk_allocator import StrategyRiskAllocator


def _model() -> FundingBasisExpectedShortfallModel:
    return FundingBasisExpectedShortfallModel(
        window_ms=20_000,
        max_samples=128,
        max_pairs=8,
        horizon_ms=100,
        min_samples=6,
        min_history_ms=500,
        confidence=0.8,
        quote_skew_ms=10,
    )


def _observe(
    model: FundingBasisExpectedShortfallModel,
    *,
    at_ms: int,
    basis_bps: float,
) -> None:
    # This symmetric construction has an exact signed basis of `basis_bps`
    # versus the fixed 100 quote reference mid.
    mid_a = 100.0 * (1.0 + basis_bps / 20_000.0)
    mid_b = 100.0 * (1.0 - basis_bps / 20_000.0)
    batch_id = model.begin_observation_batch()
    assert model.observe_pair(
        symbol="BTCUSDT",
        venue_a="binance",
        venue_b="bybit",
        bid_a=mid_a - 0.01,
        ask_a=mid_a + 0.01,
        observed_a_ms=at_ms,
        bid_b=mid_b - 0.01,
        ask_b=mid_b + 0.01,
        observed_b_ms=at_ms,
        now_ms=at_ms,
        batch_id=batch_id,
    )


def test_basis_expected_shortfall_is_directional_and_excludes_current_snapshot() -> None:
    model = _model()
    # The current batch drops basis by 10 bps.  It must not affect an entry
    # decided by that same public snapshot.
    for index in range(7):
        _observe(model, at_ms=1_000 + index * 100, basis_bps=0.0)
    _observe(model, at_ms=1_700, basis_bps=-10.0)

    same_snapshot = model.estimate(
        symbol="BTCUSDT",
        long_venue="binance",
        short_venue="bybit",
        now_ms=1_701,
    )
    assert same_snapshot.evidence_complete is False
    assert same_snapshot.reason == "nonpositive_basis_expected_shortfall"

    # A later batch permits the previous drop to become one historical return.
    _observe(model, at_ms=1_800, basis_bps=-10.0)
    long_a = model.estimate(
        symbol="BTCUSDT",
        long_venue="binance",
        short_venue="bybit",
        now_ms=1_801,
    )
    assert long_a.evidence_complete is True
    # At 80% confidence with seven historical returns the upper tail contains
    # two observations: one 10 bps adverse move and one zero, so ES is 5 bps.
    assert long_a.expected_shortfall_bps == pytest.approx(5.0)

    # Reversing the paired legs turns the same downward move into a gain, not
    # a loss. A zero one-sided tail remains fail-closed.
    long_b = model.estimate(
        symbol="BTCUSDT",
        long_venue="bybit",
        short_venue="binance",
        now_ms=1_801,
    )
    assert long_b.evidence_complete is False
    assert long_b.reason == "nonpositive_basis_expected_shortfall"


def test_basis_expected_shortfall_refuses_missing_or_skewed_quote_evidence() -> None:
    model = _model()
    batch_id = model.begin_observation_batch()
    assert (
        model.observe_pair(
            symbol="BTCUSDT",
            venue_a="binance",
            venue_b="bybit",
            bid_a=100.0,
            ask_a=101.0,
            observed_a_ms=1_000,
            bid_b=100.0,
            ask_b=101.0,
            observed_b_ms=0,
            now_ms=1_000,
            batch_id=batch_id,
        )
        is False
    )
    assert (
        model.observe_pair(
            symbol="BTCUSDT",
            venue_a="binance",
            venue_b="bybit",
            bid_a=100.0,
            ask_a=101.0,
            observed_a_ms=1_000,
            bid_b=100.0,
            ask_b=101.0,
            observed_b_ms=1_100,
            now_ms=1_100,
            batch_id=batch_id,
        )
        is False
    )
    assert (
        model.observe_pair(
            symbol="BTCUSDT",
            venue_a="binance",
            venue_b="bybit",
            bid_a=100.0,
            ask_a=101.0,
            observed_a_ms=1_000.5,
            bid_b=100.0,
            ask_b=101.0,
            observed_b_ms=1_000,
            now_ms=1_000,
            batch_id=batch_id,
        )
        is False
    )


def test_basis_expected_shortfall_checkpoint_is_bounded_and_stale_restore_cold_starts(tmp_path) -> None:
    model = _model()
    for index in range(8):
        _observe(model, at_ms=1_000 + index * 100, basis_bps=-float(index))
    path = tmp_path / "risk.json"
    path.write_text(json.dumps(model.checkpoint(now_ms=1_800)), encoding="utf-8")

    restored = _model()
    assert restore_funding_basis_risk_checkpoint(
        restored, path, now_ms=1_900, max_age_ms=2_000
    )
    assert restored.state_count == 1

    assert not restore_funding_basis_risk_checkpoint(
        _model(), path, now_ms=4_001, max_age_ms=2_000
    )
    path.write_text("{not-json", encoding="utf-8")
    assert not restore_funding_basis_risk_checkpoint(
        _model(), path, now_ms=1_900, max_age_ms=2_000
    )


def test_basis_expected_shortfall_checkpoint_rejects_fractional_numeric_state(tmp_path) -> None:
    path = tmp_path / "risk.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "saved_at_ms": 1_800,
                "next_batch_id": 2,
                "states": {
                    "BTCUSDT|binance|bybit": [
                        {
                            "observed_at_ms": 1_000.5,
                            "signed_basis_bps": 1.0,
                            "batch_id": 1,
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    assert not restore_funding_basis_risk_checkpoint(
        _model(), path, now_ms=1_900, max_age_ms=2_000
    )


def test_expected_shortfall_budget_caps_common_base_quantity_without_relaxation() -> None:
    limit = StrategyRiskAllocator().limit_base_quantity_by_expected_shortfall(
        long_entry_price=100.0,
        short_entry_price=110.0,
        current_base_quantity=5.0,
        expected_shortfall_bps=100.0,
        expected_shortfall_budget_quote=1.1,
    )

    # ES loss is 1% of reference notional: 1.1 quote permits 110 quote, or
    # exactly one common base quantity at the more expensive executable leg.
    assert limit.evidence_complete is True
    assert limit.base_quantity == pytest.approx(1.0)
    assert limit.maximum_reference_notional_quote == pytest.approx(110.0)
    assert limit.constrained is True


def test_funding_risk_runtime_refuses_individually_stale_snapshot_quotes(tmp_path) -> None:
    config = AppConfig()
    config.runtime.funding_basis_risk_checkpoint_path = str(tmp_path / "risk.json")
    config.runtime.max_market_age_ms = 1_000
    runtime = FundingRiskRuntime(SimpleNamespace(config=config))

    status = runtime.observe_fresh_snapshot(
        SimpleNamespace(
            quotes={
                "binance:BTCUSDT": SimpleNamespace(
                    venue="binance",
                    symbol="BTCUSDT",
                    bid=100.0,
                    ask=101.0,
                    observed_at_ms=900,
                ),
                "bybit:BTCUSDT": SimpleNamespace(
                    venue="bybit",
                    symbol="BTCUSDT",
                    bid=100.0,
                    ask=101.0,
                    observed_at_ms=2_000,
                ),
            }
        ),
        now_ms=2_000,
    )

    assert status["observed_pair_count"] == 0
    assert status["rejected_pair_count"] == 0
    assert runtime.model.state_count == 0


def test_funding_risk_runtime_mark_unhealthy_fails_closed_candidate_estimates(tmp_path) -> None:
    config = AppConfig()
    config.runtime.funding_basis_risk_checkpoint_path = str(tmp_path / "risk.json")
    runtime = FundingRiskRuntime(SimpleNamespace(config=config))
    runtime._checkpoint_healthy = True

    runtime.mark_unhealthy("RuntimeError")
    estimate = runtime.estimate_candidate(
        SimpleNamespace(symbol="BTCUSDT"),
        long_venue=SimpleNamespace(value="binance"),
        short_venue=SimpleNamespace(value="bybit"),
        now_ms=1_000,
    )

    assert estimate.evidence_complete is False
    assert estimate.reason == "basis_risk_checkpoint_not_durable"
    assert runtime._last_checkpoint_error == "RuntimeError"


def test_live_funding_entry_requires_dynamic_es_and_positive_budget() -> None:
    config = AppConfig()
    config.runtime.mode = "live"
    config.strategy.risk_monitor_enabled = True
    config.strategy.funding_new_entries_enabled = True
    config.strategy.funding_dynamic_expected_shortfall_enabled = False
    config.strategy.funding_expected_shortfall_budget_quote = 0.0

    issues = validate_config(config)

    assert any("funding_dynamic_expected_shortfall_enabled" in issue for issue in issues)
    assert any("funding_expected_shortfall_budget_quote" in issue for issue in issues)
