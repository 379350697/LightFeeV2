from __future__ import annotations

from types import SimpleNamespace

from lightfee.marketdata.l2 import L2BookStatus, LocalL2Book, PriceLevel
from lightfee.marketdata.l2_depth_bridge import (
    attach_local_l2_depth,
    load_local_l2_depth_bridge,
    publish_local_l2_depth_bridge,
)
from lightfee.sidecar.snapshot import QuoteSnapshot


def _hot_book() -> LocalL2Book:
    book = LocalL2Book(venue="binance", symbol="BTCUSDT")
    book.status = L2BookStatus.HOT
    book.apply_snapshot(
        [PriceLevel(100.0, 1.5), PriceLevel(99.5, 2.0), PriceLevel(99.0, 3.0)],
        [PriceLevel(101.0, 1.0), PriceLevel(101.5, 2.0), PriceLevel(102.0, 3.0)],
        sequence=42,
        now_ms=1_000,
    )
    return book


def test_bridge_exports_fresh_hot_book_and_attaches_matching_depth(tmp_path) -> None:
    path = tmp_path / "local-l2-depth.json"
    assert publish_local_l2_depth_bridge(
        path,
        [_hot_book()],
        now_ms=1_000,
        max_age_ms=100,
        max_levels=2,
    ) == 1

    bridge = load_local_l2_depth_bridge(path, now_ms=1_050, max_age_ms=100)
    quote = QuoteSnapshot(
        venue="binance",
        symbol="BTCUSDT",
        bid=100.0,
        ask=101.0,
        observed_at_ms=1_050,
    )
    attached, rejected = attach_local_l2_depth(
        {"binance:BTCUSDT": quote}, bridge, max_quote_skew_ms=50
    )

    assert (attached, rejected) == (1, 0)
    assert quote.bid_depth == ((100.0, 1.5), (99.5, 2.0))
    assert quote.ask_depth == ((101.0, 1.0), (101.5, 2.0))


def test_bridge_never_overwrites_bbo_and_stale_or_non_hot_books_disappear(tmp_path) -> None:
    path = tmp_path / "local-l2-depth.json"
    book = _hot_book()
    publish_local_l2_depth_bridge(
        path,
        [book],
        now_ms=1_000,
        max_age_ms=100,
        max_levels=3,
    )
    bridge = load_local_l2_depth_bridge(path, now_ms=1_050, max_age_ms=100)
    mismatched = QuoteSnapshot(
        venue="binance",
        symbol="BTCUSDT",
        bid=99.0,
        ask=101.0,
        observed_at_ms=1_050,
    )
    attached, rejected = attach_local_l2_depth(
        {"binance:BTCUSDT": mismatched}, bridge, max_quote_skew_ms=50
    )

    assert (attached, rejected) == (0, 1)
    assert mismatched.bid_depth == ()
    assert mismatched.bid == 99.0
    assert load_local_l2_depth_bridge(path, now_ms=1_101, max_age_ms=100) == {}

    book.status = L2BookStatus.COLD
    assert publish_local_l2_depth_bridge(
        path,
        [book],
        now_ms=1_100,
        max_age_ms=100,
        max_levels=3,
    ) == 0
    assert load_local_l2_depth_bridge(path, now_ms=1_100, max_age_ms=100) == {}


def test_main_sidecar_merges_bridge_without_direct_market_fetch(tmp_path) -> None:
    from lightfee.sidecar.service import SidecarService

    path = tmp_path / "local-l2-depth.json"
    publish_local_l2_depth_bridge(
        path,
        [_hot_book()],
        now_ms=1_000,
        max_age_ms=100,
        max_levels=2,
    )
    service = object.__new__(SidecarService)
    service.config = SimpleNamespace(
        runtime=SimpleNamespace(
            local_l2_depth_bridge_enabled=True,
            local_l2_depth_bridge_path=str(path),
            max_market_age_ms=100,
        ),
        strategy=SimpleNamespace(spread_quote_skew_ms=50),
    )
    quote = QuoteSnapshot(
        venue="binance",
        symbol="BTCUSDT",
        bid=100.0,
        ask=101.0,
        observed_at_ms=1_050,
    )

    service._attach_local_l2_depth_bridge({"binance:BTCUSDT": quote}, 1_050)

    assert quote.bid_depth[0] == (100.0, 1.5)
    assert quote.ask_depth[0] == (101.0, 1.0)


def test_bridge_rejects_price_matched_depth_from_a_different_market_instant(tmp_path) -> None:
    path = tmp_path / "local-l2-depth.json"
    publish_local_l2_depth_bridge(
        path,
        [_hot_book()],
        now_ms=1_000,
        max_age_ms=100,
        max_levels=2,
    )
    bridge = load_local_l2_depth_bridge(path, now_ms=1_050, max_age_ms=100)
    quote = QuoteSnapshot(
        venue="binance",
        symbol="BTCUSDT",
        bid=100.0,
        ask=101.0,
        # The price is unchanged, but this BBO was collected too far after
        # the local ladder to make its lower levels executable evidence.
        observed_at_ms=1_050,
    )

    attached, rejected = attach_local_l2_depth(
        {"binance:BTCUSDT": quote}, bridge, max_quote_skew_ms=49
    )

    assert (attached, rejected) == (0, 1)
    assert quote.bid_depth == ()


def test_live_runtime_bridge_writer_exports_only_existing_local_books(tmp_path) -> None:
    from lightfee.engine.market_data_runtime import MarketDataRuntime

    path = tmp_path / "local-l2-depth.json"
    runtime = MarketDataRuntime(
        SimpleNamespace(
            config=SimpleNamespace(
                runtime=SimpleNamespace(
                    local_l2_depth_bridge_enabled=True,
                    local_l2_depth_bridge_path=str(path),
                    local_l2_depth_bridge_publish_interval_ms=1,
                    local_l2_depth_bridge_max_levels=2,
                )
            ),
            local_l2_runtime=SimpleNamespace(books={"book": _hot_book()}),
            _entry_local_l2_stale_after_ms=lambda: 100,
            journal=SimpleNamespace(append=lambda *_args, **_kwargs: None),
        )
    )

    runtime._publish_local_l2_depth_bridge(1_000)
    bridge = load_local_l2_depth_bridge(path, now_ms=1_050, max_age_ms=100)

    assert bridge[("binance", "BTCUSDT")].bid_depth[0] == (100.0, 1.5)
