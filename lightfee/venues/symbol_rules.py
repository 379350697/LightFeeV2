"""Dynamic per-symbol trading rules cache.

Fetches precision/quantity/notional filters from exchange public endpoints
and caches them, so passive (and taker) orders always use current symbol rules
instead of relying solely on static VenueSpec defaults.

Venue endpoints:
- Binance/Aster: GET /fapi/v1/exchangeInfo → PRICE_FILTER, LOT_SIZE, MIN_NOTIONAL
- Bybit: GET /v5/market/instruments-info?category=linear&symbol=X
- OKX: GET /api/v5/public/instruments?instType=SWAP&instId=X
- Bitget: GET /api/v2/mix/market/contracts?productType=USDT-FUTURES&symbol=X
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import math
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
    rule_source: str = ""  # "exchangeInfo", "instruments-info", "instrument", "contracts", "spec_fallback"


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
        # A spec fallback is an admission-time failure signal for live order
        # preparation, not a durable exchange rule.  Keep only authoritative
        # venue metadata so a transient public-endpoint failure can recover on
        # the next attempt instead of pinning the process to stale defaults.
        if rule.rule_source != "spec_fallback":
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
        elif venue == Venue.BITGET:
            return await self._fetch_bitget(transport, venue_symbol)
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

        try:
            for f in s.get("filters", []):
                ft = f.get("filterType", "")
                if ft == "PRICE_FILTER":
                    tick_size = float(f.get("tickSize", 0))
                elif ft == "LOT_SIZE":
                    qty_step = float(f.get("stepSize", 0))
                    min_qty = float(f.get("minQty", 0))
                elif ft == "MIN_NOTIONAL":
                    min_notional = float(f.get("notional", 0))
        except (TypeError, ValueError):
            return self._spec_fallback(transport, venue_symbol)

        # A partial exchangeInfo response cannot be labelled as an
        # exchange-backed rule and supplemented with static VenueSpec values.
        # Live preparation sees this fallback and rejects the order without
        # sending a precision guess to the private endpoint.
        required_values = (tick_size, qty_step, min_qty, min_notional)
        if not all(math.isfinite(value) and value > 0.0 for value in required_values):
            return self._spec_fallback(transport, venue_symbol)

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

    async def _fetch_bitget(self, transport: Any, venue_symbol: str) -> SymbolRule:
        """Fetch the Bitget Mix contract precision contract.

        Bitget publishes the price step as ``priceEndStep`` at the decimal
        precision ``pricePlace`` and the quantity multiple as
        ``sizeMultiplier``.  These values are the wire contract; static
        VenueSpec defaults must never be mixed into a live rule.
        """
        try:
            raw = await transport._public_get(
                "/api/v2/mix/market/contracts",
                params={"productType": "USDT-FUTURES", "symbol": venue_symbol},
            )
        except Exception:
            return self._spec_fallback(transport, venue_symbol)

        data = raw.get("data", []) if isinstance(raw, dict) else []
        if not isinstance(data, list):
            return self._spec_fallback(transport, venue_symbol)
        item = next(
            (
                row for row in data
                if isinstance(row, dict)
                and str(row.get("symbol", "")).upper() == venue_symbol.upper()
            ),
            None,
        )
        if item is None:
            return self._spec_fallback(transport, venue_symbol)

        def decimal_field(name: str, *, allow_zero: bool = False) -> Decimal | None:
            value = item.get(name)
            if value in (None, ""):
                return None
            try:
                parsed = Decimal(str(value))
            except (InvalidOperation, TypeError, ValueError):
                return None
            if not parsed.is_finite() or (parsed < 0 if allow_zero else parsed <= 0):
                return None
            return parsed

        price_end_step = decimal_field("priceEndStep")
        price_place = decimal_field("pricePlace", allow_zero=True)
        size_multiplier = decimal_field("sizeMultiplier")
        volume_place = decimal_field("volumePlace", allow_zero=True)
        min_trade_num = decimal_field("minTradeNum")
        min_trade_usdt = decimal_field("minTradeUSDT")

        # ``pricePlace``/``volumePlace`` are decimal counts, not arbitrary
        # real-valued quantities.  Reject malformed catalog rows rather than
        # manufacturing a precision rule from partial metadata.
        if price_end_step is None or price_place is None:
            return self._spec_fallback(transport, venue_symbol)
        if price_place != price_place.to_integral_value():
            return self._spec_fallback(transport, venue_symbol)
        price_tick = price_end_step * (Decimal(10) ** -int(price_place))

        qty_step = size_multiplier
        if qty_step is None and volume_place is not None:
            if volume_place != volume_place.to_integral_value():
                return self._spec_fallback(transport, venue_symbol)
            qty_step = Decimal(10) ** -int(volume_place)
        if qty_step is None or min_trade_num is None:
            return self._spec_fallback(transport, venue_symbol)

        values = (price_tick, qty_step, min_trade_num)
        if not all(value.is_finite() and value > 0 for value in values):
            return self._spec_fallback(transport, venue_symbol)

        return SymbolRule(
            tick_size=float(price_tick),
            qty_step=float(qty_step),
            min_qty=float(min_trade_num),
            min_notional=float(min_trade_usdt or Decimal("0")),
            rule_source="contracts",
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
