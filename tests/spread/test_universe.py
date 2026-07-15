from __future__ import annotations

import json

from lightfee.config.schema import AppConfig, VenueConfig
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.universe import (
    SPREAD_SAMPLING_MAX_PAIR_COUNT,
    resolve_spread_sampling_symbols,
)


def _quote(
    symbol: str,
    venue: str,
    volume: float,
    *,
    multiplier: float = 1.0,
) -> QuoteSnapshot:
    return QuoteSnapshot(
        venue=venue,
        symbol=symbol,
        bid=100.0,
        ask=101.0,
        volume_24h_quote=volume,
        underlying=symbol.removesuffix("USDT"),
        quote_currency="USDT",
        contract_type="linear",
        contract_multiplier=multiplier,
    )


def _enabled_config(symbols: list[str], path: str, *, max_symbols: int = 96) -> AppConfig:
    config = AppConfig(symbols=symbols)
    config.venues = [VenueConfig(venue="a"), VenueConfig(venue="b")]
    config.runtime.daily_universe.enabled = True
    config.runtime.daily_universe.path = path
    config.runtime.daily_universe.max_symbols = max_symbols
    return config


def test_disabled_sampling_universe_preserves_configured_semantics(tmp_path) -> None:
    config = AppConfig(symbols=["ethusdt", "BTCUSDT", "ETHUSDT"])

    selected = resolve_spread_sampling_symbols(
        config,
        {},
        quote_eligible=lambda _quote: False,
    )

    assert selected == ("ETHUSDT", "BTCUSDT")


def test_disabled_daily_universe_still_bounds_oversized_spread_sampling(tmp_path) -> None:
    symbols = [f"S{index}USDT" for index in range(30)]
    venues = [f"v{index}" for index in range(7)]
    config = AppConfig(
        symbols=symbols,
        venues=[VenueConfig(venue=venue) for venue in venues],
    )
    metadata = {
        f"{venue}:{symbol}": _quote(symbol, venue, 1_000_000.0 - symbol_index)
        for symbol_index, symbol in enumerate(symbols)
        for venue in venues[:2]
    }

    selected = resolve_spread_sampling_symbols(
        config,
        metadata,
        quote_eligible=lambda _quote: True,
    )

    assert config.runtime.daily_universe.enabled is False
    assert len(selected) == SPREAD_SAMPLING_MAX_PAIR_COUNT // 21


def test_explicit_empty_daily_universe_fails_closed(tmp_path) -> None:
    path = tmp_path / "daily.json"
    path.write_text(json.dumps({"symbols": []}), encoding="utf-8")
    config = _enabled_config(["BTCUSDT", "ETHUSDT"], str(path))
    metadata = {
        "binance:BTCUSDT": _quote("BTCUSDT", "binance", 10_000.0),
        "okx:BTCUSDT": _quote("BTCUSDT", "okx", 10_000.0),
    }

    selected = resolve_spread_sampling_symbols(
        config,
        metadata,
        quote_eligible=lambda _quote: True,
    )

    assert selected == ()


def test_sampling_ranks_the_weak_compatible_hedge_leg(tmp_path) -> None:
    config = _enabled_config(
        ["ONEUSDT", "BALUSDT", "BADUSDT"],
        str(tmp_path / "missing.json"),
    )
    metadata = {
        "a:ONEUSDT": _quote("ONEUSDT", "a", 1_000_000_000.0),
        "b:ONEUSDT": _quote("ONEUSDT", "b", 10.0),
        "a:BALUSDT": _quote("BALUSDT", "a", 2_000.0),
        "b:BALUSDT": _quote("BALUSDT", "b", 1_500.0),
        "a:BADUSDT": _quote("BADUSDT", "a", 5_000.0, multiplier=1.0),
        "b:BADUSDT": _quote("BADUSDT", "b", 5_000.0, multiplier=2.0),
    }

    selected = resolve_spread_sampling_symbols(
        config,
        metadata,
        quote_eligible=lambda _quote: True,
    )

    assert selected == ("BALUSDT", "ONEUSDT")


def test_daily_file_is_an_allow_list_before_liquidity_ranking(tmp_path) -> None:
    path = tmp_path / "daily.json"
    path.write_text(json.dumps({"symbols": ["LOWUSDT"]}), encoding="utf-8")
    config = _enabled_config(["HIGHUSDT", "LOWUSDT"], str(path))
    metadata = {
        "a:HIGHUSDT": _quote("HIGHUSDT", "a", 9_000.0),
        "b:HIGHUSDT": _quote("HIGHUSDT", "b", 8_000.0),
        "a:LOWUSDT": _quote("LOWUSDT", "a", 900.0),
        "b:LOWUSDT": _quote("LOWUSDT", "b", 800.0),
    }

    selected = resolve_spread_sampling_symbols(
        config,
        metadata,
        quote_eligible=lambda _quote: True,
    )

    assert selected == ("LOWUSDT",)


def test_sampling_pair_budget_bounds_the_expensive_signal_path(tmp_path) -> None:
    symbols = [f"S{index}USDT" for index in range(30)]
    venues = [f"v{index}" for index in range(7)]
    config = _enabled_config(symbols, str(tmp_path / "missing.json"), max_symbols=96)
    config.venues = [VenueConfig(venue=venue) for venue in venues]
    metadata = {
        f"{venue}:{symbol}": _quote(symbol, venue, 1_000_000.0 - symbol_index)
        for symbol_index, symbol in enumerate(symbols)
        for venue in venues
    }

    selected = resolve_spread_sampling_symbols(
        config,
        metadata,
        quote_eligible=lambda _quote: True,
    )

    pair_count = len(selected) * (len(venues) * (len(venues) - 1) // 2)
    assert pair_count <= SPREAD_SAMPLING_MAX_PAIR_COUNT
    assert len(selected) == SPREAD_SAMPLING_MAX_PAIR_COUNT // 21


def test_sampling_budget_charges_temporarily_missing_configured_venues(tmp_path) -> None:
    symbols = [f"S{index}USDT" for index in range(30)]
    venues = [f"v{index}" for index in range(7)]
    config = _enabled_config(symbols, str(tmp_path / "missing.json"), max_symbols=96)
    config.venues = [VenueConfig(venue=venue) for venue in venues]
    # Only two venues are visible during startup.  The other five may recover
    # later, so selecting 384 two-leg symbols would not be a lifecycle bound.
    metadata = {
        f"{venue}:{symbol}": _quote(symbol, venue, 1_000_000.0 - symbol_index)
        for symbol_index, symbol in enumerate(symbols)
        for venue in venues[:2]
    }

    selected = resolve_spread_sampling_symbols(
        config,
        metadata,
        quote_eligible=lambda _quote: True,
    )

    worst_case_pair_count = len(selected) * 21
    assert worst_case_pair_count <= SPREAD_SAMPLING_MAX_PAIR_COUNT
    assert len(selected) == SPREAD_SAMPLING_MAX_PAIR_COUNT // 21
