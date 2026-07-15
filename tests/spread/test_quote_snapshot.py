from __future__ import annotations

import json

import lightfee.spread.quote_snapshot as quote_snapshot_module
import pytest
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.quote_snapshot import (
    FULL_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
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


def _snapshot(
    *,
    observed_at_ms: int = 1_100,
    schema_version: int | None = None,
) -> SpreadQuoteSnapshot:
    kwargs = {} if schema_version is None else {"schema_version": schema_version}
    return SpreadQuoteSnapshot(
        **kwargs,
        published_at_ms=1_200,
        market_observed_at_ms=observed_at_ms,
        batch_started_at_ms=1_000,
        configured_venues=["binance"],
        degraded_venues=[],
        degraded_symbols={},
        quotes={"binance:BTCUSDT": _quote(observed_at_ms=observed_at_ms)},
    )


def test_spread_quote_snapshot_v4_round_trip_contains_only_hot_bbo_fields(tmp_path) -> None:
    path = tmp_path / "quotes.json"

    publish_spread_quote_snapshot(_snapshot(), path)
    loaded = load_spread_quote_snapshot(path)

    assert loaded is not None
    assert loaded.market_observed_at_ms == 1_100
    assert loaded.quotes["binance:BTCUSDT"].bid == 100.0
    payload = json.loads(path.read_text())
    assert "candidates" not in payload
    assert payload["schema_version"] == 4
    assert payload["producer_generation_id"]
    assert isinstance(payload["quotes"]["binance:BTCUSDT"], list)
    assert payload["quote_fields"] == [
        "venue",
        "symbol",
        "bid",
        "ask",
        "observed_at_ms",
        "source",
        "bid_size",
        "ask_size",
    ]
    hot = loaded.quotes["binance:BTCUSDT"]
    assert hot.bid_size == 5.0
    assert hot.underlying == ""


def test_spread_quote_snapshot_v3_full_contract_remains_readable_and_is_larger(
    tmp_path,
) -> None:
    hot_path = tmp_path / "hot.json"
    full_path = tmp_path / "full.json"
    publish_spread_quote_snapshot(_snapshot(), hot_path)
    publish_spread_quote_snapshot(
        _snapshot(schema_version=FULL_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION),
        full_path,
    )

    loaded = load_spread_quote_snapshot(full_path)

    assert loaded is not None
    assert loaded.schema_version == 3
    assert loaded.quotes["binance:BTCUSDT"] == _quote()
    assert hot_path.stat().st_size < full_path.stat().st_size * 0.6


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


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source", ""),
        ("bid_size", -1.0),
        ("ask_size", float("inf")),
    ],
)
def test_spread_quote_snapshot_v4_rejects_malformed_hot_evidence(
    tmp_path,
    field,
    value,
) -> None:
    path = tmp_path / "quotes.json"
    publish_spread_quote_snapshot(_snapshot(), path)
    payload = json.loads(path.read_text())
    field_index = payload["quote_fields"].index(field)
    payload["quotes"]["binance:BTCUSDT"][field_index] = value
    path.write_text(json.dumps(payload))

    assert load_spread_quote_snapshot(path) is None


def test_spread_quote_snapshot_v4_rejects_unknown_row_field(tmp_path) -> None:
    path = tmp_path / "quotes.json"
    publish_spread_quote_snapshot(_snapshot(), path)
    payload = json.loads(path.read_text())
    payload["quote_fields"].append("untrusted")
    payload["quotes"]["binance:BTCUSDT"].append(1)
    path.write_text(json.dumps(payload))

    assert load_spread_quote_snapshot(path) is None


@pytest.mark.parametrize(
    "schema_version",
    [4, FULL_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION],
)
def test_spread_quote_snapshot_rejects_non_scalar_identity_without_raising(
    tmp_path,
    schema_version,
) -> None:
    path = tmp_path / "quotes.json"
    publish_spread_quote_snapshot(
        _snapshot(schema_version=schema_version),
        path,
    )
    payload = json.loads(path.read_text())
    venue_index = payload["quote_fields"].index("venue")
    payload["quotes"]["binance:BTCUSDT"][venue_index] = []
    path.write_text(json.dumps(payload))

    assert load_spread_quote_snapshot(path) is None


def test_spread_quote_snapshot_rejects_non_scalar_degraded_venue_without_raising(
    tmp_path,
) -> None:
    path = tmp_path / "quotes.json"
    publish_spread_quote_snapshot(_snapshot(), path)
    payload = json.loads(path.read_text())
    payload["degraded_venues"] = [[]]
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


def test_spread_quote_snapshot_v2_remains_readable(tmp_path) -> None:
    path = tmp_path / "legacy-v2-quotes.json"
    publish_spread_quote_snapshot(
        _snapshot(schema_version=FULL_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION),
        path,
    )
    payload = json.loads(path.read_text())
    payload["schema_version"] = 2
    payload.pop("producer_generation_id")
    path.write_text(json.dumps(payload))

    loaded = load_spread_quote_snapshot(path)

    assert loaded is not None
    assert loaded.schema_version == 2
    assert loaded.producer_generation_id == ""


def test_producer_generation_distinguishes_reused_pid(monkeypatch) -> None:
    monkeypatch.setattr(
        quote_snapshot_module,
        "process_start_ticks",
        lambda pid: 100,
    )
    first = quote_snapshot_module.producer_generation_id(42)
    monkeypatch.setattr(
        quote_snapshot_module,
        "process_start_ticks",
        lambda pid: 200,
    )
    second = quote_snapshot_module.producer_generation_id(42)

    assert first != second
    assert first.endswith(":42:100")
    assert second.endswith(":42:200")
