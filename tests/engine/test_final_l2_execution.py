"""Regression tests for the final L2-to-standard-IOC execution contract."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from lightfee.config.schema import VenueConfig
from lightfee.core.domain import Side, Venue
from lightfee.engine.entry_dispatch_runtime import (
    EntryDispatchRuntime,
    _align_base_quantity_down,
    _common_base_quantity_step,
    _l2_vwap_and_sweep_limit_for_base_quantity,
    _standard_ioc_price_hints,
)
from lightfee.engine.entry import EntryContext, EntryType
from lightfee.engine.entry_readiness import QuoteLease
from lightfee.marketdata.l2 import PriceLevel


def _lease(**overrides: object) -> QuoteLease:
    values: dict[str, object] = {
        "pair_id": "BTCUSDT:binance:bybit",
        "symbol": "BTCUSDT",
        "long_venue": "binance",
        "short_venue": "bybit",
        "long_bid": 99.0,
        "long_ask": 100.0,
        "short_bid": 102.0,
        "short_ask": 103.0,
        "long_observed_at_ms": 1,
        "short_observed_at_ms": 1,
        "created_at_ms": 1,
        "expires_at_ms": 2,
    }
    values.update(overrides)
    return QuoteLease(**values)  # type: ignore[arg-type]


def test_l2_vwap_returns_the_last_consumed_level_as_ioc_bound() -> None:
    asks = [
        PriceLevel(price=100.0, quantity=0.4),
        PriceLevel(price=101.0, quantity=0.8),
        PriceLevel(price=102.0, quantity=5.0),
    ]
    vwap, filled, sweep_limit = _l2_vwap_and_sweep_limit_for_base_quantity(
        asks,
        1.0,
    )

    assert filled == 1.0
    assert vwap == 100.6
    assert sweep_limit == 101.0


def test_complete_l2_lease_uses_sweep_limits_not_bbo_for_standard_ioc() -> None:
    lease = _lease(
        long_buy_vwap=100.6,
        short_sell_vwap=101.4,
        long_buy_sweep_limit=101.0,
        short_sell_sweep_limit=101.0,
        l2_vwap_complete=True,
    )

    assert _standard_ioc_price_hints(lease) == (101.0, 101.0)


def test_incomplete_l2_lease_cannot_invent_sweep_limits() -> None:
    lease = _lease(
        long_buy_sweep_limit=101.0,
        short_sell_sweep_limit=101.0,
        l2_vwap_complete=False,
    )

    assert _standard_ioc_price_hints(lease) == (100.0, 102.0)


def test_truthy_l2_complete_flag_cannot_enable_sweep_limits() -> None:
    lease = _lease(
        long_buy_sweep_limit=101.0,
        short_sell_sweep_limit=101.0,
        l2_vwap_complete="true",
    )

    assert _standard_ioc_price_hints(lease) == (100.0, 102.0)


def test_common_base_quantity_grid_uses_decimal_lcm_not_largest_step() -> None:
    step = _common_base_quantity_step(0.003, 0.002)

    assert step == 0.006
    assert _align_base_quantity_down(0.017, step) == 0.012


def test_final_quote_lease_rejects_a_future_market_timestamp() -> None:
    now_ms = 1_000
    lease = _lease(
        long_observed_at_ms=now_ms + 1,
        short_observed_at_ms=now_ms,
        expires_at_ms=now_ms + 500,
    )
    runtime = object.__new__(EntryDispatchRuntime)
    runtime.ctx = SimpleNamespace(
        config=SimpleNamespace(runtime=SimpleNamespace(mode="live")),
        entry_readiness_provider=SimpleNamespace(get_lease=lambda _pair_id: lease),
        _entry_readiness_provider_name=lambda: "quote_lease",
        _entry_readiness_provider_uses_quote_lease=lambda: True,
        _candidate_pair_id=lambda _candidate: lease.pair_id,
        _entry_quote_lease_max_age_ms=lambda: 100,
        _quote_lease_blocker_family=lambda reason: (
            "stale_quote" if reason == "stale_quote_lease" else "unknown"
        ),
    )
    candidate = SimpleNamespace(
        symbol="BTCUSDT", long_venue="binance", short_venue="bybit"
    )

    reason, returned_lease, evidence = runtime._entry_quote_lease_execution_check(
        candidate, now_ms
    )

    assert reason == "stale_quote_lease"
    assert returned_lease is lease
    assert evidence["long_timestamp_after_now"] is True


def test_truthy_quote_refresh_decision_cannot_bypass_the_stale_lease_gate() -> None:
    runtime = object.__new__(EntryDispatchRuntime)
    runtime.ctx = SimpleNamespace(
        entry_readiness_provider=SimpleNamespace(
            decide=lambda _candidate, _now_ms: SimpleNamespace(
                allowed="true", reason="", evidence={}
            )
        )
    )
    runtime._entry_readiness_provider_name = lambda: "ws_bbo_quote_lease"
    lease = _lease()

    reason, returned_lease, evidence = runtime._refresh_entry_quote_lease_for_execution(
        SimpleNamespace(),
        1_000,
        "stale_quote_lease",
        lease,
        {},
    )

    assert reason == "stale_quote_lease"
    assert returned_lease is lease
    assert evidence["execution_refresh_block_reason"] == ""


def test_immediate_post_first_fill_decision_includes_taker_fees() -> None:
    """The live first-fill path cannot choose an unwind on raw price alone."""

    class Book:
        observed_at_ms = 1_000

        def __init__(self, bid: float, ask: float) -> None:
            self._bid = bid
            self._ask = ask

        def best_bid(self) -> float:
            return self._bid

        def best_ask(self) -> float:
            return self._ask

    books = {
        "binance": Book(99.99, 101.0),
        "bybit": Book(99.5, 100.0),
    }
    runtime = object.__new__(EntryDispatchRuntime)
    runtime.ctx = SimpleNamespace(
        config=SimpleNamespace(
            strategy=SimpleNamespace(max_liquidity_snapshot_age_ms=1_000),
            venues=[
                VenueConfig(venue="binance", taker_fee_bps=100.0),
                VenueConfig(venue="bybit", taker_fee_bps=0.0),
            ],
        ),
        local_l2_runtime=SimpleNamespace(get_book=lambda venue, _symbol: books[venue]),
        _local_l2_effective_enabled=lambda: True,
    )
    entry = EntryContext(
        entry_id="entry-fee-inclusive",
        symbol="BTCUSDT",
        long_venue=Venue.BINANCE,
        short_venue=Venue.BYBIT,
        long_quantity=0.1,
        short_quantity=0.1,
        long_price_hint=100.0,
        short_price_hint=99.5,
        maker_leg=Side.BUY,
        entry_type=EntryType.PASSIVE_INCREMENTAL,
    )

    decision = asyncio.run(
        runtime.decide_after_first_fill(
            ctx=entry,
            maker_fill=SimpleNamespace(quantity=0.1, price=100.0),
            hedge_request=SimpleNamespace(price=99.5),
            now_ms=1_000,
        )
    )

    assert decision["action"] == "complete_hedge"
    assert decision["unwind_first_leg_price_loss_quote"] < decision[
        "complete_hedge_price_loss_quote"
    ]
    assert decision["unwind_first_leg_loss_quote"] > decision[
        "complete_hedge_loss_quote"
    ]
