from __future__ import annotations

import pytest

from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.models import SpreadReversionCandidate
from lightfee.spread.paper import SpreadPaperConfig, SpreadPaperTracker


def _candidate(
    *,
    symbol: str = "BTCUSDT",
    signal_ts_ms: int = 1_000,
    entry_notional_quote: float = 20.0,
    opportunity_label: str = "spread_reversion",
    long_venue: str = "cheap",
    short_venue: str = "rich",
    executable_spread_bps: float = 40.0,
    net_edge_bps: float = 20.0,
    z_score: float = 3.0,
    capacity_quote: float = 100.0,
) -> SpreadReversionCandidate:
    return SpreadReversionCandidate(
        candidate_id=f"spread:{symbol}:{long_venue}->{short_venue}",
        symbol=symbol,
        long_venue=long_venue,
        short_venue=short_venue,
        spread_mid_bps=50.0,
        executable_spread_bps=executable_spread_bps,
        rolling_mean_bps=10.0,
        rolling_std_bps=5.0,
        z_score=z_score,
        net_edge_bps=net_edge_bps,
        sample_count=120,
        signal_ts_ms=signal_ts_ms,
        long_quote_ts_ms=signal_ts_ms,
        short_quote_ts_ms=signal_ts_ms,
        entry_notional_quote=entry_notional_quote,
        capacity_quote=capacity_quote,
        signal_status="entry_ready",
        fair_price=100.5,
        venue_premium_bps=25.0,
        liquidity_evidence_status="top_book_size_available",
        opportunity_label=opportunity_label,
    )


def _quote(
    venue: str,
    *,
    bid: float,
    ask: float,
    observed_at_ms: int,
    funding_rate_bps: float = 0.0,
    funding_timestamp_ms: int = 0,
    bid_size: float = 10.0,
    ask_size: float = 10.0,
    mark_price: float = 0.0,
    index_price: float = 0.0,
    volume_24h_quote: float = 0.0,
    open_interest: float = 0.0,
) -> QuoteSnapshot:
    return QuoteSnapshot(
        venue=venue,
        symbol="BTCUSDT",
        bid=bid,
        ask=ask,
        observed_at_ms=observed_at_ms,
        bid_size=bid_size,
        ask_size=ask_size,
        funding_rate_bps=funding_rate_bps,
        funding_timestamp_ms=funding_timestamp_ms,
        mark_price=mark_price,
        index_price=index_price,
        volume_24h_quote=volume_24h_quote,
        open_interest=open_interest,
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
            mark_price=100.2,
            index_price=100.1,
            volume_24h_quote=1_000_000.0,
            open_interest=50_000.0,
        ),
        "rich:BTCUSDT": _quote(
            "rich",
            bid=short_bid,
            ask=short_ask,
            observed_at_ms=now_ms,
            funding_rate_bps=short_funding_bps,
            funding_timestamp_ms=short_funding_ts_ms,
            mark_price=101.2,
            index_price=101.1,
            volume_24h_quote=2_000_000.0,
            open_interest=60_000.0,
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
    assert registered["payload"]["paper_id"] == "spread:spread:BTCUSDT:cheap->rich:1000"
    assert registered["payload"]["paper_order_status"] == "open"
    assert registered["payload"]["long_leg"]["entry_price"] > 0.0
    assert registered["payload"]["short_leg"]["entry_price"] > 0.0
    assert tracker.register(candidate, quotes, finalist_rank=0) is None
    assert tracker.register(_candidate(signal_ts_ms=1_001), quotes, finalist_rank=1) is None
    assert tracker.tracked_count == 1


def test_register_many_creates_independent_bot_positions() -> None:
    tracker = SpreadPaperTracker(
        SpreadPaperConfig(
            enabled=True,
            finalist_limit=10,
            markout_secs=[1],
            paper_bot_ids=[
                "tt_conservative",
                "mt_long_maker",
                "mt_short_maker",
                "mt_selected_maker",
                "core_v1_bot",
                "core_v1_z10_bot",
            ],
            taker_fee_bps_by_venue={"binance": 1.0, "gate": 2.0},
            maker_fee_bps_by_venue={"binance": 0.1, "gate": 0.2},
        )
    )
    candidate = _candidate(
        long_venue="binance",
        short_venue="gate",
        executable_spread_bps=120.0,
        net_edge_bps=90.0,
        z_score=12.0,
    )
    quotes = {
        "binance:BTCUSDT": _quote(
            "binance",
            bid=99.8,
            ask=100.0,
            observed_at_ms=1_000,
            volume_24h_quote=2_000_000.0,
        ),
        "gate:BTCUSDT": _quote(
            "gate",
            bid=101.0,
            ask=101.2,
            observed_at_ms=1_000,
            volume_24h_quote=2_000_000.0,
        ),
    }

    events = tracker.register_many(candidate, quotes, finalist_rank=0)

    bot_ids = [event["payload"]["paper_bot_id"] for event in events]
    assert bot_ids == [
        "tt_conservative",
        "mt_long_maker",
        "mt_short_maker",
        "mt_selected_maker",
        "core_v1_bot",
        "core_v1_z10_bot",
    ]
    assert len({event["payload"]["paper_id"] for event in events}) == len(events)
    by_bot = {event["payload"]["paper_bot_id"]: event["payload"] for event in events}
    assert by_bot["tt_conservative"]["paper_cohort"] == "baseline_current"
    assert by_bot["core_v1_bot"]["paper_cohort"] == "core_v1"
    assert by_bot["core_v1_z10_bot"]["paper_cohort"] == "core_v1_z10"
    assert by_bot["mt_long_maker"]["long_leg"]["entry_liquidity_role"] == "maker"
    assert by_bot["mt_long_maker"]["long_leg"]["entry_raw_price"] == pytest.approx(99.8)
    assert by_bot["mt_long_maker"]["long_leg"]["entry_fee_bps"] == pytest.approx(0.1)
    assert by_bot["mt_short_maker"]["short_leg"]["entry_liquidity_role"] == "maker"
    assert by_bot["mt_short_maker"]["short_leg"]["entry_raw_price"] == pytest.approx(101.2)
    assert by_bot["mt_selected_maker"]["paper_maker_leg"] == "long"
    assert tracker.tracked_count == 6


def test_core_and_control_bots_filter_candidates_by_factor() -> None:
    tracker = SpreadPaperTracker(
        SpreadPaperConfig(
            enabled=True,
            finalist_limit=10,
            markout_secs=[1],
            paper_bot_ids=[
                "core_v1_bot",
                "core_v1_exec100_bot",
                "core_v1_z10_bot",
                "bad_pair_control_bot",
                "low_liquidity_control_bot",
                "low_edge_control_bot",
            ],
        )
    )
    quotes = {
        "binance:BTCUSDT": _quote(
            "binance",
            bid=99.8,
            ask=100.0,
            observed_at_ms=1_000,
            volume_24h_quote=20_000.0,
        ),
        "gate:BTCUSDT": _quote(
            "gate",
            bid=101.0,
            ask=101.2,
            observed_at_ms=1_000,
            volume_24h_quote=20_000.0,
        ),
    }
    low_edge = _candidate(
        long_venue="binance",
        short_venue="gate",
        executable_spread_bps=55.0,
        net_edge_bps=30.0,
        z_score=4.0,
        capacity_quote=30.0,
    )

    low_edge_events = tracker.register_many(low_edge, quotes, finalist_rank=0)

    assert [event["payload"]["paper_bot_id"] for event in low_edge_events] == [
        "low_liquidity_control_bot",
        "low_edge_control_bot",
    ]

    bad_pair = _candidate(
        signal_ts_ms=2_000,
        long_venue="binance",
        short_venue="bybit",
        executable_spread_bps=120.0,
        net_edge_bps=90.0,
        z_score=12.0,
    )
    bad_quotes = {
        "binance:BTCUSDT": _quote(
            "binance",
            bid=99.8,
            ask=100.0,
            observed_at_ms=2_000,
            volume_24h_quote=2_000_000.0,
        ),
        "bybit:BTCUSDT": _quote(
            "bybit",
            bid=101.0,
            ask=101.2,
            observed_at_ms=2_000,
            volume_24h_quote=2_000_000.0,
        ),
    }

    bad_pair_events = tracker.register_many(bad_pair, bad_quotes, finalist_rank=0)

    assert [event["payload"]["paper_bot_id"] for event in bad_pair_events] == [
        "bad_pair_control_bot"
    ]


def test_delayed_maker_hedge_bot_fills_hedge_from_later_quote() -> None:
    tracker = SpreadPaperTracker(
        SpreadPaperConfig(
            enabled=True,
            finalist_limit=10,
            markout_secs=[2],
            terminal_secs=2,
            paper_bot_ids=["mt_selected_maker_delay_1000ms"],
        )
    )
    candidate = _candidate(
        long_venue="binance",
        short_venue="gate",
        executable_spread_bps=120.0,
        net_edge_bps=90.0,
        z_score=12.0,
    )
    entry_quotes = {
        "binance:BTCUSDT": _quote("binance", bid=99.8, ask=100.0, observed_at_ms=1_000),
        "gate:BTCUSDT": _quote("gate", bid=101.0, ask=101.2, observed_at_ms=1_000),
    }

    registered = tracker.register_many(candidate, entry_quotes, finalist_rank=0)

    assert len(registered) == 1
    payload = registered[0]["payload"]
    assert payload["paper_bot_id"] == "mt_selected_maker_delay_1000ms"
    assert payload["paper_hedge_delay_ms"] == 1000
    assert payload["paper_maker_leg"] == "long"
    assert payload["short_leg"]["entry_pending"] is True
    assert payload["short_leg"]["entry_price"] is None

    later_quotes = {
        "binance:BTCUSDT": _quote("binance", bid=100.2, ask=100.4, observed_at_ms=2_000),
        "gate:BTCUSDT": _quote("gate", bid=100.5, ask=100.7, observed_at_ms=2_000),
    }
    hedge_events = tracker.evaluate_due(2_000, later_quotes)

    assert [event["kind"] for event in hedge_events] == ["opportunity.paper_hedge_filled"]
    hedge_payload = hedge_events[0]["payload"]
    assert hedge_payload["short_leg"]["entry_pending"] is False
    assert hedge_payload["short_leg"]["entry_raw_price"] == pytest.approx(100.5)
    assert hedge_payload["short_leg"]["entry_observed_at_ms"] == 2_000

    closed_events = tracker.evaluate_due(3_000, later_quotes)

    assert [event["kind"] for event in closed_events] == [
        "opportunity.paper_markout",
        "opportunity.paper_closed",
    ]
    assert closed_events[-1]["payload"]["paper_net_quote"] is not None


def test_register_excludes_default_symbols_and_non_allowed_labels() -> None:
    tracker = SpreadPaperTracker(
        SpreadPaperConfig(enabled=True, finalist_limit=10, markout_secs=[1])
    )

    assert (
        tracker.register(_candidate(symbol="BBUSDT"), _quotes(now_ms=1_000), finalist_rank=0)
        is None
    )
    assert (
        tracker.register(_candidate(symbol="QNTUSDT"), _quotes(now_ms=1_000), finalist_rank=0)
        is None
    )
    assert (
        tracker.register(
            _candidate(opportunity_label="single_venue_dislocation"),
            _quotes(now_ms=1_000),
            finalist_rank=0,
        )
        is None
    )
    assert tracker.tracked_count == 0


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
    assert payload["paper_id"] == "spread:spread:BTCUSDT:cheap->rich:1000"
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
    assert payload["entry_market_snapshot"]["long_quote"]["ask_size"] == pytest.approx(10.0)
    assert payload["entry_market_snapshot"]["short_quote"]["bid_size"] == pytest.approx(10.0)
    assert payload["exit_market_snapshot"]["long_quote"]["bid"] == pytest.approx(100.4)
    assert payload["exit_market_snapshot"]["short_quote"]["ask"] == pytest.approx(100.7)
    assert payload["candidate_snapshot"]["executable_spread_bps"] == pytest.approx(40.0)
    assert payload["candidate_snapshot"]["fair_price"] == pytest.approx(100.5)
    assert payload["funding_advantage_bps"] == pytest.approx(-4.0)
    assert payload["long_leg"]["mark_price"] == pytest.approx(100.2)
    assert payload["short_leg"]["open_interest"] == pytest.approx(60_000.0)
    assert payload["opportunity_label"] in {
        "good_trade_missed",
        "bad_trade_correctly_rejected",
    }
    assert tracker.tracked_count == 0


def test_episode_cooldown_blocks_repeated_pair_until_window_expires() -> None:
    cfg = SpreadPaperConfig(
        enabled=True,
        finalist_limit=10,
        markout_secs=[],
        terminal_secs=1,
        episode_cooldown_ms=1_800_000,
    )
    tracker = SpreadPaperTracker(cfg)
    assert tracker.register(_candidate(signal_ts_ms=1_000), _quotes(now_ms=1_000), finalist_rank=0)
    assert [event["kind"] for event in tracker.evaluate_due(2_000, _quotes(now_ms=2_000))] == [
        "opportunity.paper_closed"
    ]

    assert (
        tracker.register(_candidate(signal_ts_ms=60_000), _quotes(now_ms=60_000), finalist_rank=0)
        is None
    )
    assert (
        tracker.register(
            _candidate(signal_ts_ms=1_801_001),
            _quotes(now_ms=1_801_001),
            finalist_rank=0,
        )
        is not None
    )


def test_episode_cooldown_restores_from_journal_records() -> None:
    cfg = SpreadPaperConfig(
        enabled=True,
        finalist_limit=10,
        markout_secs=[],
        terminal_secs=1,
        episode_cooldown_ms=1_800_000,
    )
    tracker = SpreadPaperTracker(cfg)
    registered = tracker.register(
        _candidate(signal_ts_ms=1_000),
        _quotes(now_ms=1_000),
        finalist_rank=0,
    )
    assert registered is not None
    closed = tracker.evaluate_due(2_000, _quotes(now_ms=2_000))

    restored = SpreadPaperTracker(cfg)
    restored.restore_from_records([registered, *closed])

    assert (
        restored.register(_candidate(signal_ts_ms=60_000), _quotes(now_ms=60_000), finalist_rank=0)
        is None
    )


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
    cfg = SpreadPaperConfig(
        enabled=True,
        finalist_limit=10,
        markout_secs=[1],
        terminal_secs=1,
        episode_cooldown_ms=0,
    )
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
    cfg = SpreadPaperConfig(
        enabled=True,
        finalist_limit=10,
        markout_secs=[1],
        terminal_secs=1,
        episode_cooldown_ms=0,
    )
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
