from __future__ import annotations

import pytest

from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.models import SpreadReversionCandidate
from lightfee.spread.paper import SpreadPaperConfig, SpreadPaperTracker


def _candidate(
    *,
    signal_ts_ms: int = 1_000,
    entry_notional_quote: float = 20.0,
) -> SpreadReversionCandidate:
    return SpreadReversionCandidate(
        candidate_id="btc:cheap:rich",
        symbol="BTCUSDT",
        long_venue="cheap",
        short_venue="rich",
        spread_mid_bps=50.0,
        executable_spread_bps=40.0,
        rolling_mean_bps=10.0,
        rolling_std_bps=5.0,
        z_score=3.0,
        net_edge_bps=20.0,
        sample_count=120,
        signal_ts_ms=signal_ts_ms,
        long_quote_ts_ms=signal_ts_ms,
        short_quote_ts_ms=signal_ts_ms,
        entry_notional_quote=entry_notional_quote,
        capacity_quote=100.0,
        signal_status="entry_ready",
    )


def _quote(
    venue: str,
    *,
    bid: float,
    ask: float,
    observed_at_ms: int,
    funding_rate_bps: float = 0.0,
    funding_timestamp_ms: int = 0,
) -> QuoteSnapshot:
    return QuoteSnapshot(
        venue=venue,
        symbol="BTCUSDT",
        bid=bid,
        ask=ask,
        observed_at_ms=observed_at_ms,
        bid_size=10.0,
        ask_size=10.0,
        funding_rate_bps=funding_rate_bps,
        funding_timestamp_ms=funding_timestamp_ms,
    )


def _quotes(
    *,
    now_ms: int,
    long_bid: float = 100.0,
    long_ask: float = 100.1,
    short_bid: float = 101.0,
    short_ask: float = 101.1,
    long_funding_bps: float = 8.0,
    short_funding_bps: float = 4.0,
    long_funding_ts_ms: int = 3_000,
    short_funding_ts_ms: int = 4_000,
) -> dict[str, QuoteSnapshot]:
    return {
        "cheap:BTCUSDT": _quote(
            "cheap",
            bid=long_bid,
            ask=long_ask,
            observed_at_ms=now_ms,
            funding_rate_bps=long_funding_bps,
            funding_timestamp_ms=long_funding_ts_ms,
        ),
        "rich:BTCUSDT": _quote(
            "rich",
            bid=short_bid,
            ask=short_ask,
            observed_at_ms=now_ms,
            funding_rate_bps=short_funding_bps,
            funding_timestamp_ms=short_funding_ts_ms,
        ),
    }


def test_spread_paper_defaults_are_disabled() -> None:
    cfg = SpreadPaperConfig()
    tracker = SpreadPaperTracker(cfg)

    assert tracker.enabled is False
    assert tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0) is None
    assert tracker.evaluate_due(2_000, _quotes(now_ms=2_000)) == []


def test_register_deduplicates_and_respects_finalist_limit() -> None:
    tracker = SpreadPaperTracker(
        SpreadPaperConfig(enabled=True, finalist_limit=1, markout_secs=[1])
    )
    candidate = _candidate()
    quotes = _quotes(now_ms=1_000)

    registered = tracker.register(candidate, quotes, finalist_rank=0)

    assert registered is not None
    assert registered["kind"] == "opportunity.paper_registered"
    assert registered["payload"]["paper_id"] == "spread:btc:cheap:rich:1000"
    assert registered["payload"]["paper_order_status"] == "open"
    assert registered["payload"]["long_leg"]["entry_price"] > 0.0
    assert registered["payload"]["short_leg"]["entry_price"] > 0.0
    assert tracker.register(candidate, quotes, finalist_rank=0) is None
    assert tracker.register(_candidate(signal_ts_ms=1_001), quotes, finalist_rank=1) is None
    assert tracker.tracked_count == 1


def test_markout_uses_taker_fees_slippage_and_funding_breakdown() -> None:
    tracker = SpreadPaperTracker(
        SpreadPaperConfig(
            enabled=True,
            finalist_limit=10,
            markout_secs=[1],
            terminal_secs=1,
            taker_fee_bps_by_venue={"cheap": 1.0, "rich": 2.0},
            slippage_buffer_bps=5.0,
            default_funding_interval_ms=8_000,
        )
    )
    candidate = _candidate(entry_notional_quote=20.0)
    entry_quotes = _quotes(
        now_ms=1_000,
        long_bid=99.9,
        long_ask=100.0,
        short_bid=101.0,
        short_ask=101.1,
        long_funding_bps=8.0,
        short_funding_bps=4.0,
        long_funding_ts_ms=1_500,
        short_funding_ts_ms=5_000,
    )
    assert tracker.register(candidate, entry_quotes, finalist_rank=0) is not None

    exit_quotes = _quotes(
        now_ms=2_000,
        long_bid=100.4,
        long_ask=100.5,
        short_bid=100.6,
        short_ask=100.7,
        long_funding_bps=8.0,
        short_funding_bps=4.0,
        long_funding_ts_ms=1_500,
        short_funding_ts_ms=5_000,
    )
    events = tracker.evaluate_due(2_000, exit_quotes)

    assert [event["kind"] for event in events] == [
        "opportunity.paper_markout",
        "opportunity.paper_closed",
    ]
    payload = events[0]["payload"]
    assert payload["paper_id"] == "spread:btc:cheap:rich:1000"
    assert payload["candidate_id"] == candidate.candidate_id
    assert payload["paper_entry_notional_quote"] == pytest.approx(20.0)
    assert payload["paper_entry_fee_quote"] == pytest.approx(20.0 * (1.0 + 2.0) / 10_000.0)
    assert payload["paper_exit_fee_quote"] > 0.0
    assert payload["paper_fee_quote"] == pytest.approx(
        payload["paper_entry_fee_quote"] + payload["paper_exit_fee_quote"]
    )
    assert payload["paper_entry_slippage_quote"] > 0.0
    assert payload["paper_exit_slippage_quote"] > 0.0
    assert payload["paper_slippage_quote"] == pytest.approx(
        payload["paper_entry_slippage_quote"] + payload["paper_exit_slippage_quote"]
    )
    assert payload["accrued_funding_estimate_quote"] == pytest.approx(-0.001)
    assert payload["settlement_realized_funding_quote"] == pytest.approx(-0.016)
    assert payload["paper_funding_quote"] == pytest.approx(
        payload["accrued_funding_estimate_quote"]
    )
    assert payload["paper_net_quote"] == pytest.approx(
        payload["paper_gross_quote"]
        + payload["paper_funding_quote"]
        - payload["paper_fee_quote"]
        - payload["paper_slippage_quote"]
    )
    assert payload["long_leg"]["entry_price"] > 100.0
    assert payload["long_leg"]["exit_price"] < 100.4
    assert payload["short_leg"]["entry_price"] < 101.0
    assert payload["short_leg"]["exit_price"] > 100.7
    assert payload["opportunity_label"] in {
        "good_trade_missed",
        "bad_trade_correctly_rejected",
    }
    assert tracker.tracked_count == 0


def test_missing_exit_quote_emits_unknown_payload() -> None:
    tracker = SpreadPaperTracker(
        SpreadPaperConfig(enabled=True, finalist_limit=10, markout_secs=[1])
    )
    assert tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0) is not None

    events = tracker.evaluate_due(2_000, {})

    payload = events[0]["payload"]
    assert payload["paper_net_quote"] is None
    assert payload["opportunity_label"] == "unknown_due_to_missing_snapshot"
    assert payload["market_snapshot"]["snapshot_available"] is False
    assert payload["paper_fee_quote"] == pytest.approx(payload["paper_entry_fee_quote"])


def test_tracker_restores_open_orders_from_local_journal_records() -> None:
    cfg = SpreadPaperConfig(enabled=True, finalist_limit=10, markout_secs=[1], terminal_secs=2)
    tracker = SpreadPaperTracker(cfg)
    registered = tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None

    restored = SpreadPaperTracker(cfg)
    restored.restore_from_records([registered])

    assert restored.tracked_count == 1
    markouts = restored.evaluate_due(2_000, _quotes(now_ms=2_000))
    assert [event["kind"] for event in markouts] == ["opportunity.paper_markout"]
    assert restored.tracked_count == 1

    restarted = SpreadPaperTracker(cfg)
    restarted.restore_from_records([registered, *markouts])
    closed = restarted.evaluate_due(3_000, _quotes(now_ms=3_000))

    assert [event["kind"] for event in closed] == ["opportunity.paper_closed"]
    assert restarted.tracked_count == 0


def test_tracker_does_not_restore_closed_orders() -> None:
    cfg = SpreadPaperConfig(enabled=True, finalist_limit=10, markout_secs=[1], terminal_secs=1)
    tracker = SpreadPaperTracker(cfg)
    registered = tracker.register(_candidate(), _quotes(now_ms=1_000), finalist_rank=0)
    assert registered is not None
    closed_events = tracker.evaluate_due(2_000, _quotes(now_ms=2_000))

    restored = SpreadPaperTracker(cfg)
    restored.restore_from_records([registered, *closed_events])

    assert restored.tracked_count == 0
    assert restored.evaluate_due(3_000, _quotes(now_ms=3_000)) == []
    assert restored.register(_candidate(), _quotes(now_ms=3_000), finalist_rank=0) is None


def test_tracker_does_not_reregister_same_signal_after_terminal_close() -> None:
    cfg = SpreadPaperConfig(enabled=True, finalist_limit=10, markout_secs=[1], terminal_secs=1)
    tracker = SpreadPaperTracker(cfg)
    candidate = _candidate()
    assert tracker.register(candidate, _quotes(now_ms=1_000), finalist_rank=0) is not None
    closed_events = tracker.evaluate_due(2_000, _quotes(now_ms=2_000))

    assert [event["kind"] for event in closed_events] == [
        "opportunity.paper_markout",
        "opportunity.paper_closed",
    ]
    assert tracker.tracked_count == 0
    assert tracker.register(candidate, _quotes(now_ms=3_000), finalist_rank=0) is None
    assert (
        tracker.register(_candidate(signal_ts_ms=1_001), _quotes(now_ms=3_000), finalist_rank=0)
        is not None
    )
