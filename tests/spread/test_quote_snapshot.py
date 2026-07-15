from __future__ import annotations

import json

from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.quote_snapshot import (
    SpreadQuoteSnapshot,
    load_spread_quote_snapshot,
    publish_spread_quote_snapshot,
    spread_quote_snapshot_path,
)


def _quote(*, observed_at_ms: int = 1_100) -> QuoteSnapshot:
    return QuoteSnapshot(
        venue="binance",
        symbol="BTCUSDT",
        bid=100.0,
        ask=100.1,
        bid_size=5.0,
        ask_size=4.0,
        observed_at_ms=observed_at_ms,
        underlying="BTC",
        quote_currency="USDT",
        contract_multiplier=1.0,
        contract_normalization_complete=False,
    )


def _snapshot(*, observed_at_ms: int = 1_100) -> SpreadQuoteSnapshot:
    return SpreadQuoteSnapshot(
        published_at_ms=1_200,
        market_observed_at_ms=observed_at_ms,
        batch_started_at_ms=1_000,
        configured_venues=["binance"],
        degraded_venues=[],
        degraded_symbols={},
        quotes={"binance:BTCUSDT": _quote(observed_at_ms=observed_at_ms)},
    )


def test_spread_quote_snapshot_round_trip_is_compact_and_complete(tmp_path) -> None:
    path = tmp_path / "quotes.json"

    publish_spread_quote_snapshot(_snapshot(), path)
    loaded = load_spread_quote_snapshot(path)

    assert loaded is not None
    assert loaded.market_observed_at_ms == 1_100
    assert loaded.quotes["binance:BTCUSDT"].bid == 100.0
    payload = json.loads(path.read_text())
    assert "candidates" not in payload
    assert payload["schema_version"] == 2
    assert isinstance(payload["quotes"]["binance:BTCUSDT"], list)
    assert loaded.quotes["binance:BTCUSDT"] == _quote()


def test_spread_quote_snapshot_rejects_future_quote_without_replacing_last_good(
    tmp_path,
) -> None:
    path = tmp_path / "quotes.json"
    publish_spread_quote_snapshot(_snapshot(), path)
    before = path.read_bytes()

    future = _snapshot(observed_at_ms=1_300)
    try:
        publish_spread_quote_snapshot(future, path)
    except ValueError as exc:
        assert "watermark_order_invalid" in str(exc) or "quote_from_future" in str(exc)
    else:
        raise AssertionError("future quote snapshot must fail closed")

    assert path.read_bytes() == before


def test_prevalidated_hot_path_still_rejects_future_quote(tmp_path) -> None:
    path = tmp_path / "quotes.json"
    publish_spread_quote_snapshot(_snapshot(), path)
    before = path.read_bytes()

    try:
        publish_spread_quote_snapshot(
            _snapshot(observed_at_ms=1_300),
            path,
            validate_contract=False,
        )
    except ValueError as exc:
        assert "quote_from_future" in str(exc) or "watermark_order_invalid" in str(exc)
    else:
        raise AssertionError("prevalidated hot path must fail closed")

    assert path.read_bytes() == before


def test_spread_quote_snapshot_rejects_unknown_or_mismatched_identity(tmp_path) -> None:
    path = tmp_path / "quotes.json"
    publish_spread_quote_snapshot(_snapshot(), path)
    payload = json.loads(path.read_text())
    venue_index = payload["quote_fields"].index("venue")
    payload["quotes"]["binance:BTCUSDT"][venue_index] = "bybit"
    path.write_text(json.dumps(payload))

    assert load_spread_quote_snapshot(path) is None


def test_spread_quote_snapshot_accepts_last_good_quote_predating_batch(tmp_path) -> None:
    path = tmp_path / "quotes.json"
    snapshot = SpreadQuoteSnapshot(
        published_at_ms=2_000,
        market_observed_at_ms=1_100,
        batch_started_at_ms=1_500,
        configured_venues=["binance"],
        degraded_venues=["binance"],
        degraded_symbols={},
        quotes={"binance:BTCUSDT": _quote(observed_at_ms=1_100)},
    )

    publish_spread_quote_snapshot(snapshot, path)

    loaded = load_spread_quote_snapshot(path)
    assert loaded is not None
    assert loaded.market_observed_at_ms < loaded.batch_started_at_ms


def test_spread_quote_snapshot_path_is_a_sibling_contract() -> None:
    assert str(spread_quote_snapshot_path("runtime/opportunity-input-snapshot.json")) == (
        "runtime/opportunity-input-snapshot.spread-quotes.v2.json"
    )


def test_spread_quote_snapshot_v1_remains_readable(tmp_path) -> None:
    path = tmp_path / "legacy-quotes.json"
    quote = _quote()
    payload = {
        "schema_version": 1,
        "published_at_ms": 1_200,
        "market_observed_at_ms": 1_100,
        "batch_started_at_ms": 1_000,
        "source_mode": "sidecar_market_fast_path",
        "configured_venues": ["binance"],
        "degraded_venues": [],
        "degraded_symbols": {},
        "quotes": {"binance:BTCUSDT": quote.__dict__},
    }
    path.write_text(json.dumps(payload))

    loaded = load_spread_quote_snapshot(path)

    assert loaded is not None
    assert loaded.schema_version == 1
    assert loaded.quotes["binance:BTCUSDT"] == quote
