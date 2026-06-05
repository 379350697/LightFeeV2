from types import SimpleNamespace

import pytest

from lightfee.config.schema import StrategyConfig
from lightfee.engine.recovery_ledger import RecoveryLedger
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


def test_entry_admissibility_blocks_recovery_ledger_before_funding_horizon():
    cfg = StrategyConfig()
    cfg.min_scan_minutes_before_funding = 0
    ledger = RecoveryLedger.from_local_and_exchange_truth(
        local={"open_positions": [], "pending_entries": []},
        exchange_truth={
            "truth_available": True,
            "positions": [],
            "open_orders": [
                {
                    "venue": "bybit",
                    "symbol": "TRXUSDT",
                    "side": "buy",
                    "quantity": 72.0,
                    "reduce_only": False,
                }
            ],
        },
    )

    decision = V1TradingLifecycle.entry_admissibility(
        _candidate("BTCUSDT", first_ms=1_059_000),
        now_ms=1_000_000,
        strategy=cfg,
        recovery_ledger=ledger,
        source="unit",
    )

    assert decision.allowed is False
    assert decision.reason == "entry_blocked_recovery_ledger"
    assert decision.evidence["source"] == "unit"
    assert decision.evidence["truth_available"] is True
    assert decision.evidence["blocking_work"] == [
        {
            "kind": "orphan_maker_order",
            "symbol": "TRXUSDT",
            "decision": {
                "outcome": "fail_closed_operator_block",
                "reason": "live_order_without_runtime_owner",
            },
        }
    ]


def test_entry_admissibility_raises_unexpected_ledger_type_error():
    class ExplodingLedger:
        allows_new_entries = True

        def allows_new_entry(self, candidate):
            raise TypeError("boom")

    with pytest.raises(TypeError, match="boom"):
        V1TradingLifecycle.entry_admissibility(
            _candidate("BTCUSDT"),
            now_ms=1_000_000,
            strategy=StrategyConfig(),
            recovery_ledger=ExplodingLedger(),
        )


@pytest.mark.parametrize(
    ("allows_new_entry", "expected_blocks"),
    [(True, False), (False, True)],
)
def test_ledger_blocks_uses_non_callable_allows_new_entry(
    allows_new_entry, expected_blocks
):
    ledger = SimpleNamespace(allows_new_entry=allows_new_entry)

    assert (
        V1TradingLifecycle._ledger_blocks(_candidate(), ledger) is expected_blocks
    )


@pytest.mark.parametrize(
    ("allows", "expected_blocks"),
    [(True, False), (False, True)],
)
def test_ledger_blocks_uses_callable_allows_new_entries(allows, expected_blocks):
    class Ledger:
        def allows_new_entries(self):
            return allows

    assert (
        V1TradingLifecycle._ledger_blocks(_candidate(), Ledger())
        is expected_blocks
    )


def test_ledger_blocks_propagates_callable_allows_new_entries_type_error():
    class Ledger:
        def allows_new_entries(self):
            raise TypeError("bad allows")

    with pytest.raises(TypeError, match="bad allows"):
        V1TradingLifecycle._ledger_blocks(_candidate(), Ledger())


@pytest.mark.parametrize(
    ("blocking_work", "expected_blocks"),
    [(True, True), (False, False)],
)
def test_ledger_blocks_falls_back_to_callable_has_blocking_work(
    blocking_work, expected_blocks
):
    class Ledger:
        def has_blocking_work(self):
            return blocking_work

    assert (
        V1TradingLifecycle._ledger_blocks(_candidate(), Ledger())
        is expected_blocks
    )


def test_ledger_blocks_propagates_callable_has_blocking_work_type_error():
    class Ledger:
        def has_blocking_work(self):
            raise TypeError("bad blocking work")

    with pytest.raises(TypeError, match="bad blocking work"):
        V1TradingLifecycle._ledger_blocks(_candidate(), Ledger())


def test_ledger_evidence_omits_malformed_work_items_but_keeps_source_and_truth():
    ledger = SimpleNamespace(
        truth_available=True,
        work_items=object(),
    )

    assert V1TradingLifecycle._ledger_evidence(ledger, "unit") == {
        "source": "unit",
        "truth_available": True,
    }


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
