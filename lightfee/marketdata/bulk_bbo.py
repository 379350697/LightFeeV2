"""Lightweight venue-wide BBO acquisition for spread sampling.

This module is intentionally separate from funding, OI, mark/index and
instrument discovery.  It reuses only cached contract metadata needed to
normalize contract-lot sizes into canonical base quantity.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from lightfee.core.domain import Venue
from lightfee.marketdata.ws_bbo import TopBookQuote
from lightfee.venues.market_data import (
    _GATE_CONTRACT_METADATA_MAX_AGE_MS,
    _OKX_CONTRACT_METADATA_MAX_AGE_MS,
    _funding_timestamp_ms_or_seconds,
    _now_ms,
    _positive_exchange_number,
    _safe_float,
    PublicTransportError,
)

if TYPE_CHECKING:
    from lightfee.venues.market_data import MarketDataClient


def _event_timestamp_ms(*values: Any) -> int:
    for value in values:
        timestamp_ms = _funding_timestamp_ms_or_seconds(value)
        if timestamp_ms > 0:
            return timestamp_ms
    return 0


def _symbol_map(
    client: "MarketDataClient",
    requested: set[str],
) -> dict[str, str]:
    venue_symbols: dict[str, str] = {}
    for canonical in requested:
        venue_symbol = client._to_venue_symbol(canonical)
        venue_symbols[venue_symbol] = canonical
    return venue_symbols


async def _hydrate_contract_size_metadata(
    client: "MarketDataClient",
    requested: set[str],
) -> None:
    """Fill this worker's static lot-size cache before parsing BBO depth.

    The funding sidecar and dedicated BBO sidecar are separate processes, so
    their in-memory contract metadata is not shared. OKX and Gate publish BBO
    sizes in contract lots; a cold BBO process must first obtain the static
    venue-wide contract document before it can claim executable base depth.
    The client retains that document for one hour with its real receipt time.
    """

    venue = client.venue
    if venue not in (Venue.OKX, Venue.GATE):
        return

    if venue == Venue.OKX:
        cache = client._okx_contract_metadata_by_key
        cache_prefix = "okx"
        field = "ctVal"
        max_age_ms = _OKX_CONTRACT_METADATA_MAX_AGE_MS
        path = "/api/v5/public/instruments"
        params: dict[str, str] | None = {"instType": "SWAP"}
    else:
        cache = client._gate_contract_metadata_by_key
        cache_prefix = "gate"
        field = "quanto_multiplier"
        max_age_ms = _GATE_CONTRACT_METADATA_MAX_AGE_MS
        path = client.spec.funding_contracts_path
        params = None

    now_ms = _now_ms()
    missing_metadata = any(
        _cached_multiplier(
            cache,
            f"{cache_prefix}:{canonical}",
            field=field,
            received_at_ms=now_ms,
            max_age_ms=max_age_ms,
        )
        <= 0.0
        for canonical in requested
    )
    if not missing_metadata or not path:
        return

    try:
        raw, metadata_received_at_ms = await client._cached_public_get_with_received_at(
            path,
            params=params,
            max_age_ms=max_age_ms,
        )
    except PublicTransportError:
        # Unknown lot conversion is not an implicit 1x conversion. The data
        # plane will reject the zero-size quote while other venues continue.
        return
    if metadata_received_at_ms <= 0:
        return

    venue_symbols = _symbol_map(client, requested)
    if venue == Venue.OKX:
        items = raw.get("data", []) if isinstance(raw, dict) else []
        for item in items if isinstance(items, list) else []:
            if not isinstance(item, dict):
                continue
            canonical = venue_symbols.get(str(item.get("instId", "") or ""))
            if canonical is not None:
                cache[f"{cache_prefix}:{canonical}"] = (
                    dict(item),
                    metadata_received_at_ms,
                )
        return

    items = raw if isinstance(raw, list) else [raw]
    for item in items:
        if not isinstance(item, dict):
            continue
        venue_symbol = str(item.get("name", item.get("contract", "")) or "")
        canonical = venue_symbols.get(venue_symbol)
        if canonical is not None:
            cache[f"{cache_prefix}:{canonical}"] = (
                dict(item),
                metadata_received_at_ms,
            )


async def _fetch_payload(
    client: "MarketDataClient",
) -> tuple[Any, int]:
    spec = client.spec
    venue = client.venue
    if venue in (Venue.BINANCE, Venue.ASTER, Venue.GATE):
        return await client._public_get_with_received_at(spec.market_snapshot_path)
    if venue == Venue.OKX:
        return await client._public_get_with_received_at(
            spec.market_snapshot_path,
            params={"instType": "SWAP"},
        )
    if venue == Venue.BYBIT:
        return await client._public_get_with_received_at(
            spec.market_snapshot_path,
            params={"category": "linear"},
        )
    if venue == Venue.BITGET:
        return await client._public_get_with_received_at(
            spec.market_snapshot_path,
            params={"productType": "USDT-FUTURES"},
        )
    if venue == Venue.HYPERLIQUID:
        return await client._public_post_with_received_at(
            spec.market_snapshot_path,
            body={"type": "metaAndAssetCtxs"},
        )
    return {}, 0


def _rows_and_root_time(
    venue: Venue,
    raw: Any,
) -> tuple[list[dict[str, Any]], Any]:
    root_time: Any = 0
    if venue == Venue.BYBIT:
        result = raw.get("result", raw) if isinstance(raw, dict) else raw
        rows = result.get("list", []) if isinstance(result, dict) else result
        root_time = raw.get("time", 0) if isinstance(raw, dict) else 0
    elif venue in (Venue.OKX, Venue.BITGET):
        rows = raw.get("data", []) if isinstance(raw, dict) else raw
        if isinstance(raw, dict):
            root_time = raw.get("ts", raw.get("requestTime", 0))
    else:
        rows = raw
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list):
        return [], root_time
    return [row for row in rows if isinstance(row, dict)], root_time


def _cached_multiplier(
    cache: dict[str, tuple[dict[str, Any], int]],
    key: str,
    *,
    field: str,
    received_at_ms: int,
    max_age_ms: int,
) -> float:
    cached = cache.get(key)
    if cached is None:
        return 0.0
    metadata, observed_at_ms = cached
    age_ms = received_at_ms - int(observed_at_ms)
    if 0 <= age_ms <= max_age_ms:
        return _positive_exchange_number(metadata.get(field))
    if age_ms > max_age_ms:
        cache.pop(key, None)
    return 0.0


def _bbo_values(
    client: "MarketDataClient",
    row: dict[str, Any],
    *,
    canonical: str,
    received_at_ms: int,
) -> tuple[float, float, float, float]:
    venue = client.venue
    if venue in (Venue.BINANCE, Venue.ASTER):
        return (
            _safe_float(row.get("bidPrice", row.get("b", 0))),
            _safe_float(row.get("askPrice", row.get("a", 0))),
            _safe_float(row.get("bidQty", row.get("B", 0))),
            _safe_float(row.get("askQty", row.get("A", 0))),
        )
    if venue == Venue.OKX:
        multiplier = _cached_multiplier(
            client._okx_contract_metadata_by_key,
            f"okx:{canonical}",
            field="ctVal",
            received_at_ms=received_at_ms,
            max_age_ms=_OKX_CONTRACT_METADATA_MAX_AGE_MS,
        )
        return (
            _safe_float(row.get("bidPx", 0)),
            _safe_float(row.get("askPx", 0)),
            _safe_float(row.get("bidSz", 0)) * multiplier,
            _safe_float(row.get("askSz", 0)) * multiplier,
        )
    if venue == Venue.BYBIT:
        return (
            _safe_float(row.get("bid1Price", row.get("bidPrice", 0))),
            _safe_float(row.get("ask1Price", row.get("askPrice", 0))),
            _safe_float(row.get("bid1Size", row.get("bidSize", 0))),
            _safe_float(row.get("ask1Size", row.get("askSize", 0))),
        )
    if venue == Venue.BITGET:
        return (
            _safe_float(row.get("bidPr", row.get("bestBid", 0))),
            _safe_float(row.get("askPr", row.get("bestAsk", 0))),
            _safe_float(row.get("bidSz", row.get("bid1Size", 0))),
            _safe_float(row.get("askSz", row.get("ask1Size", 0))),
        )
    if venue == Venue.GATE:
        multiplier = _cached_multiplier(
            client._gate_contract_metadata_by_key,
            f"gate:{canonical}",
            field="quanto_multiplier",
            received_at_ms=received_at_ms,
            max_age_ms=_GATE_CONTRACT_METADATA_MAX_AGE_MS,
        )
        return (
            _safe_float(row.get("highest_bid", row.get("bid", 0))),
            _safe_float(row.get("lowest_ask", row.get("ask", 0))),
            _safe_float(row.get("highest_size", row.get("bid_size", 0)))
            * multiplier,
            _safe_float(row.get("lowest_size", row.get("ask_size", 0)))
            * multiplier,
        )
    return 0.0, 0.0, 0.0, 0.0


def _hyperliquid_quotes(
    raw: Any,
    *,
    requested: set[str],
    received_at_ms: int,
) -> dict[str, TopBookQuote]:
    if not isinstance(raw, list) or len(raw) < 2:
        return {}
    universe_root = raw[0]
    universe = (
        universe_root.get("universe", [])
        if isinstance(universe_root, dict)
        else universe_root
    )
    contexts = raw[1]
    if not isinstance(universe, list) or not isinstance(contexts, list):
        return {}
    quotes: dict[str, TopBookQuote] = {}
    for index, item in enumerate(universe):
        if not isinstance(item, dict) or index >= len(contexts):
            continue
        context = contexts[index]
        if not isinstance(context, dict):
            continue
        name = str(item.get("name", "") or "")
        canonical = f"{name}USDT".upper()
        if canonical not in requested:
            continue
        impact_prices = context.get("impactPxs")
        prices = (
            sorted(_safe_float(value) for value in impact_prices[:2])
            if isinstance(impact_prices, list) and len(impact_prices) >= 2
            else []
        )
        mid = _safe_float(context.get("midPx", context.get("markPx", 0)))
        bid = prices[0] if prices and prices[0] > 0.0 else mid
        ask = prices[1] if len(prices) > 1 and prices[1] > 0.0 else mid
        if not (bid > 0.0 and ask > 0.0 and bid <= ask):
            continue
        impact_notional = 20_000.0 if name in {"BTC", "ETH"} else 6_000.0
        quotes[f"hyperliquid:{canonical}"] = TopBookQuote(
            venue="hyperliquid",
            symbol=canonical,
            bid=bid,
            ask=ask,
            bid_size=impact_notional / bid,
            ask_size=impact_notional / ask,
            observed_at_ms=received_at_ms,
            received_at_ms=received_at_ms,
            exchange_event_at_ms=_event_timestamp_ms(context.get("time")),
            source="sidecar_bulk_impact_quote_rest",
        )
    return quotes


async def fetch_top_book_quotes(
    client: "MarketDataClient",
    symbols: list[str],
) -> dict[str, TopBookQuote]:
    """Return receipt-time BBO/impact quotes from one bulk public request."""
    requested = {
        str(symbol).strip().upper()
        for symbol in symbols
        if str(symbol).strip()
    }
    if not requested:
        return {}
    await _hydrate_contract_size_metadata(client, requested)
    raw, received_at_ms = await _fetch_payload(client)
    if received_at_ms <= 0:
        return {}
    if client.venue == Venue.HYPERLIQUID:
        return _hyperliquid_quotes(
            raw,
            requested=requested,
            received_at_ms=received_at_ms,
        )

    venue_symbols = _symbol_map(client, requested)
    rows, root_time = _rows_and_root_time(client.venue, raw)
    quotes: dict[str, TopBookQuote] = {}
    for row in rows:
        venue_symbol = str(
            row.get("symbol", row.get("instId", row.get("contract", "")))
            or ""
        )
        canonical = venue_symbols.get(venue_symbol)
        if canonical is None:
            continue
        bid, ask, bid_size, ask_size = _bbo_values(
            client,
            row,
            canonical=canonical,
            received_at_ms=received_at_ms,
        )
        if not (bid > 0.0 and ask > 0.0 and bid <= ask):
            continue
        venue_name = client.venue.value
        key = f"{venue_name}:{canonical}"
        quotes[key] = TopBookQuote(
            venue=venue_name,
            symbol=canonical,
            bid=bid,
            ask=ask,
            bid_size=bid_size,
            ask_size=ask_size,
            observed_at_ms=received_at_ms,
            received_at_ms=received_at_ms,
            exchange_event_at_ms=_event_timestamp_ms(
                row.get("ts"),
                row.get("time_ms"),
                row.get("time"),
                row.get("E"),
                row.get("T"),
                root_time,
            ),
            source="sidecar_bulk_bbo_rest",
        )
    return quotes
