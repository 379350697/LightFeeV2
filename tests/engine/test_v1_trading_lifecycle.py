from types import SimpleNamespace

from lightfee.config.schema import StrategyConfig
from lightfee.engine.v1_lifecycle import V1TradingLifecycle


def _candidate(symbol="BTCUSDT", first_ms=1_300_000):
    return SimpleNamespace(
        symbol=symbol,
        long_venue="binance",
        short_venue="bybit",
        first_funding_timestamp_ms=first_ms,
        funding_timestamp_ms=first_ms,
        long_funding_timestamp_ms=first_ms,
        short_funding_timestamp_ms=first_ms,
        blocked=False,
        blocked_reasons=[],
        entry_notional_quote=30.0,
    )


def _pending_entry(first_ms=1_300_000, maker_filled=0.0, hedge_filled=0.0):
    return SimpleNamespace(
        symbol="BTCUSDT",
        first_funding_timestamp_ms=first_ms,
        funding_timestamp_ms=first_ms,
        long_funding_timestamp_ms=first_ms,
        short_funding_timestamp_ms=first_ms,
        maker_leg_filled=maker_filled,
        hedge_leg_filled=hedge_filled,
    )


def test_pending_without_positive_exposure_becomes_nonviable_when_first_funding_too_close():
    cfg = StrategyConfig()
    cfg.min_scan_minutes_before_funding = 1

    decision = V1TradingLifecycle.pending_entry_viability(
        _pending_entry(first_ms=1_059_000),
        now_ms=1_000_000,
        strategy=cfg,
    )

    assert decision.allowed is False
    assert decision.reason == "pending_entry_viability_first_funding_too_close"
    assert decision.evidence == {
        "first_funding_timestamp_ms": 1_059_000,
        "remaining_to_first_funding_ms": 59_000,
        "effective_min_before_ms": 60_000,
        "source": "pending_entry",
    }


def test_pending_positive_exposure_is_not_discarded_when_first_funding_too_close():
    cfg = StrategyConfig()
    cfg.min_scan_minutes_before_funding = 1

    decision = V1TradingLifecycle.pending_entry_viability(
        _pending_entry(first_ms=1_059_000, maker_filled=10.0),
        now_ms=1_000_000,
        strategy=cfg,
    )

    assert decision.allowed is True
    assert decision.reason == "pending_entry_terminality_positive_fill_recovery"
    assert decision.evidence == {
        "first_funding_timestamp_ms": 1_059_000,
        "remaining_to_first_funding_ms": 59_000,
        "effective_min_before_ms": 60_000,
        "source": "pending_entry",
    }


def test_pending_positive_exposure_outside_horizon_allows_without_recovery_reason():
    cfg = StrategyConfig()
    cfg.min_scan_minutes_before_funding = 1

    decision = V1TradingLifecycle.pending_entry_viability(
        _pending_entry(first_ms=1_300_000, hedge_filled=10.0),
        now_ms=1_000_000,
        strategy=cfg,
    )

    assert decision.allowed is True
    assert decision.reason == ""
    assert decision.evidence == {
        "first_funding_timestamp_ms": 1_300_000,
        "remaining_to_first_funding_ms": 300_000,
        "effective_min_before_ms": 60_000,
        "source": "pending_entry",
    }


def test_entry_admissibility_does_not_own_recovery_ledger_semantics():
    cfg = StrategyConfig()
    cfg.min_scan_minutes_before_funding = 0

    class Ledger:
        truth_available = False

        def allows_new_entry(self, candidate):
            raise AssertionError("recovery gate is owned by runtime/core")

    decision = V1TradingLifecycle.entry_admissibility(
        _candidate("BTCUSDT", first_ms=1_300_000),
        now_ms=1_000_000,
        strategy=cfg,
        recovery_ledger=Ledger(),
        source="unit",
    )

    assert decision.allowed is True


def test_entry_admissibility_blocks_first_funding_too_close():
    cfg = StrategyConfig()
    cfg.min_scan_minutes_before_funding = 1
    candidate = _candidate(first_ms=1_059_000)

    decision = V1TradingLifecycle.entry_admissibility(
        candidate,
        now_ms=1_000_000,
        strategy=cfg,
        recovery_ledger=None,
    )

    assert decision.allowed is False
    assert decision.reason == "entry_blocked_first_funding_too_close"
    assert set(decision.evidence) == {
        "first_funding_timestamp_ms",
        "remaining_to_first_funding_ms",
        "effective_min_before_ms",
        "source",
    }
    assert decision.evidence["first_funding_timestamp_ms"] == 1_059_000
    assert decision.evidence["remaining_to_first_funding_ms"] == 59_000
    assert decision.evidence["effective_min_before_ms"] == 60_000
    assert decision.evidence["source"] == "candidate"


def test_entry_admissibility_allows_clean_candidate():
    cfg = StrategyConfig()
    candidate = _candidate(first_ms=1_300_000)

    decision = V1TradingLifecycle.entry_admissibility(
        candidate,
        now_ms=1_000_000,
        strategy=cfg,
        recovery_ledger=None,
    )

    assert decision.allowed is True
    assert decision.reason == ""
    assert set(decision.evidence) == {
        "first_funding_timestamp_ms",
        "remaining_to_first_funding_ms",
        "effective_min_before_ms",
        "source",
    }
    assert decision.evidence["first_funding_timestamp_ms"] == 1_300_000
    assert decision.evidence["remaining_to_first_funding_ms"] == 300_000
    assert decision.evidence["effective_min_before_ms"] == 300_000
    assert decision.evidence["source"] == "candidate"
