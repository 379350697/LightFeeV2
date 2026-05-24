"""Dynamic per-symbol trading rules cache.

Fetches precision/quantity/notional filters from exchange public endpoints
and caches them, so passive (and taker) orders always use current symbol rules
instead of relying solely on static VenueSpec defaults.

Venue endpoints:
- Binance/Aster: GET /fapi/v1/exchangeInfo → PRICE_FILTER, LOT_SIZE, MIN_NOTIONAL
- Bybit: GET /v5/market/instruments-info?category=linear&symbol=X
- OKX: GET /api/v5/public/instruments?instType=SWAP&instId=X
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from lightfee.core.domain import Venue


@dataclass(frozen=True, slots=True)
class SymbolRule:
    tick_size: float
    qty_step: float
    min_qty: float
    min_notional: float
    ct_val: float = 0.0  # OKX contract value (ctVal) for base→contract sizing
    max_market_qty: float = 0.0  # OKX maxMktSz, contract units for derivatives
    rule_source: str = ""  # "exchangeInfo", "instruments-info", "instrument", "spec_fallback"


class SymbolRulesCache:
    """Thread-unsafe in-memory cache for per-symbol exchange trading rules.

    Usage:
        cache = SymbolRulesCache()
        rule = await cache.get(transport, venue, symbol)
    """

    def __init__(self) -> None:
        self._rules: dict[tuple[Venue, str], SymbolRule] = {}

    async def get(
        self,
        transport: Any,
        venue: Venue,
        venue_symbol: str,
    ) -> SymbolRule:
        key = (venue, venue_symbol)
        if key in self._rules:
            return self._rules[key]

        rule = await self._fetch(transport, venue, venue_symbol)
        self._rules[key] = rule
        return rule

    async def _fetch(
        self,
        transport: Any,
        venue: Venue,
        venue_symbol: str,
    ) -> SymbolRule:
        if venue in (Venue.BINANCE, Venue.ASTER):
            return await self._fetch_binance_aster(transport, venue, venue_symbol)
        elif venue == Venue.BYBIT:
            return await self._fetch_bybit(transport, venue_symbol)
        elif venue == Venue.OKX:
            return await self._fetch_okx(transport, venue_symbol)
        else:
            return self._spec_fallback(transport, venue_symbol)

    async def _fetch_binance_aster(
        self, transport: Any, venue: Venue, venue_symbol: str,
    ) -> SymbolRule:
        try:
            raw = await transport._public_get(
                "/fapi/v1/exchangeInfo", params={"symbol": venue_symbol}
            )
        except Exception:
            return self._spec_fallback(transport, venue_symbol)

        symbols = raw.get("symbols", [])
        for s in symbols:
            if s.get("symbol", "").upper() == venue_symbol.upper():
                return self._parse_binance_filters(s, venue_symbol, transport)
        return self._spec_fallback(transport, venue_symbol)

    def _parse_binance_filters(
        self, s: dict[str, Any], venue_symbol: str, transport: Any,
    ) -> SymbolRule:
        tick_size = 0.0
        qty_step = 0.0
        min_qty = 0.0
        min_notional = 0.0

        for f in s.get("filters", []):
            ft = f.get("filterType", "")
            if ft == "PRICE_FILTER":
                tick_size = float(f.get("tickSize", 0))
            elif ft == "LOT_SIZE":
                qty_step = float(f.get("stepSize", 0))
                min_qty = float(f.get("minQty", 0))
            elif ft == "MIN_NOTIONAL":
                min_notional = float(f.get("notional", 0))

        # Fill gaps with spec defaults
        spec = transport._spec
        if tick_size <= 0:
            tick_size = float(spec.price_tick or 0.0)
        if qty_step <= 0:
            qty_step = float(spec.quantity_step or 0.0)
        if min_qty <= 0:
            min_qty = float(spec.min_quantity or 0.0)
        if min_notional <= 0:
            min_notional = float(spec.min_notional or 0.0)

        return SymbolRule(
            tick_size=tick_size,
            qty_step=qty_step,
            min_qty=min_qty,
            min_notional=min_notional,
            rule_source="exchangeInfo",
        )

    async def _fetch_bybit(self, transport: Any, venue_symbol: str) -> SymbolRule:
        try:
            raw = await transport._public_get(
                "/v5/market/instruments-info",
                params={"category": "linear", "symbol": venue_symbol},
            )
        except Exception:
            return self._spec_fallback(transport, venue_symbol)

        result = raw.get("result", {})
        items = result.get("list", []) if isinstance(result, dict) else []
        if not items:
            return self._spec_fallback(transport, venue_symbol)

        item = items[0]
        price_filter = item.get("priceFilter", {})
        lot_filter = item.get("lotSizeFilter", {})

        tick_size = float(price_filter.get("tickSize", 0) or 0)
        qty_step = float(lot_filter.get("qtyStep", 0) or 0)
        min_qty = float(lot_filter.get("minOrderQty", 0) or 0)
        min_notional = float(lot_filter.get("minNotionalValue", 0) or 0)

        spec = transport._spec
        if tick_size <= 0:
            tick_size = float(spec.price_tick or 0.0)
        if qty_step <= 0:
            qty_step = float(spec.quantity_step or 0.0)
        if min_qty <= 0:
            min_qty = float(spec.min_quantity or 0.0)
        if min_notional <= 0:
            min_notional = float(spec.min_notional or 0.0)

        return SymbolRule(
            tick_size=tick_size,
            qty_step=qty_step,
            min_qty=min_qty,
            min_notional=min_notional,
            rule_source="instruments-info",
        )

    async def _fetch_okx(self, transport: Any, venue_symbol: str) -> SymbolRule:
        try:
            raw = await transport._public_get(
                "/api/v5/public/instruments",
                params={"instType": "SWAP", "instId": venue_symbol},
            )
        except Exception:
            return self._spec_fallback(transport, venue_symbol)

        data = raw.get("data", [])
        if not data:
            return self._spec_fallback(transport, venue_symbol)

        item = data[0]
        tick_size = float(item.get("tickSz", 0) or 0)
        lot_sz = float(item.get("lotSz", 0) or 0)
        min_sz = float(item.get("minSz", 0) or 0)
        ct_val = float(item.get("ctVal", 0) or 0)
        max_mkt_sz = float(item.get("maxMktSz", 0) or 0)

        spec = transport._spec
        if tick_size <= 0:
            tick_size = float(spec.price_tick or 0.0)
        if lot_sz <= 0:
            lot_sz = float(spec.quantity_step or 0.0)
        if min_sz <= 0:
            min_sz = float(spec.min_quantity or 0.0)
        min_notional = float(spec.min_notional or 0.0)

        return SymbolRule(
            tick_size=tick_size,
            qty_step=lot_sz,
            min_qty=min_sz,
            min_notional=min_notional,
            ct_val=ct_val,
            max_market_qty=max_mkt_sz,
            rule_source="instrument",
        )

    def _spec_fallback(self, transport: Any, venue_symbol: str) -> SymbolRule:
        spec = transport._spec
        return SymbolRule(
            tick_size=float(spec.price_tick or 0.0),
            qty_step=float(spec.quantity_step or 0.0),
            min_qty=float(spec.min_quantity or 0.0),
            min_notional=float(spec.min_notional or 0.0),
            rule_source="spec_fallback",
        )

    def invalidate(self, venue: Venue, symbol: str) -> None:
        self._rules.pop((venue, symbol), None)

    def clear(self) -> None:
        self._rules.clear()


# Module-level singleton for use across transports
_global_rules_cache: Optional[SymbolRulesCache] = None


def get_symbol_rules_cache() -> SymbolRulesCache:
    global _global_rules_cache
    if _global_rules_cache is None:
        _global_rules_cache = SymbolRulesCache()
    return _global_rules_cache
