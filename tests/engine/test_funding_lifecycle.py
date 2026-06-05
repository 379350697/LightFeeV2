from types import SimpleNamespace

import pytest

from lightfee.config.schema import StrategyConfig
from lightfee.engine.funding_lifecycle import FundingLifecycle


def _candidate(first_ms: int):
    return SimpleNamespace(
        symbol="BTCUSDT",
        first_funding_timestamp_ms=first_ms,
        funding_timestamp_ms=first_ms,
        long_funding_timestamp_ms=first_ms,
        short_funding_timestamp_ms=first_ms,
        second_funding_timestamp_ms=0,
        opportunity_type="aligned",
    )


def test_entry_horizon_blocks_under_60_seconds_by_default():
    cfg = StrategyConfig()
    now_ms = 1_000_000
    candidate = _candidate(now_ms + 59_000)

    decision = FundingLifecycle.entry_horizon(candidate, now_ms, cfg)

    assert decision.allowed is False
    assert decision.reason == "entry_blocked_first_funding_too_close"
    assert decision.remaining_to_first_funding_ms == 59_000
    assert decision.effective_min_before_ms == 300_000


def test_entry_horizon_allows_when_remaining_meets_existing_min_scan():
    cfg = StrategyConfig()
    cfg.min_scan_minutes_before_funding = 3
    cfg.entry_min_first_funding_remaining_secs = 60
    now_ms = 1_000_000
    candidate = _candidate(now_ms + 180_000)

    decision = FundingLifecycle.entry_horizon(candidate, now_ms, cfg)

    assert decision.allowed is True
    assert decision.reason == ""
    assert decision.effective_min_before_ms == 180_000


def test_entry_horizon_uses_v1_min_scan_only_when_min_scan_is_zero():
    cfg = StrategyConfig()
    cfg.min_scan_minutes_before_funding = 0
    cfg.entry_min_first_funding_remaining_secs = 60
    now_ms = 1_000_000
    candidate = _candidate(now_ms + 59_999)

    decision = FundingLifecycle.entry_horizon(candidate, now_ms, cfg)

    assert decision.allowed is True
    assert decision.reason == ""
    assert decision.effective_min_before_ms == 0


def test_entry_horizon_blocks_missing_first_funding():
    cfg = StrategyConfig()

    decision = FundingLifecycle.entry_horizon(_candidate(0), 1_000_000, cfg)

    assert decision.allowed is False
    assert decision.reason == "entry_blocked_first_funding_missing"


def test_position_positive_ms_preserves_open_position_compare_semantics():
    assert FundingLifecycle.position_positive_ms(1_000_000) == 1_000_000
    assert FundingLifecycle.position_positive_ms(0) == 0
    assert FundingLifecycle.position_positive_ms(-1) == 0

    with pytest.raises(TypeError):
        FundingLifecycle.position_positive_ms("1000000")
