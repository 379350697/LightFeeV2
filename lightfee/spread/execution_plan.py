"""Spread order-request planning.

This layer reuses the shared `OrderRequest` contract while staying short of
submitting orders. A live executor can consume these plans after wiring order
truth, recovery, and persistence around the spread strategy bucket.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from lightfee.core.domain import OrderRequest, Side, Venue
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.models import SpreadOrderIntent, SpreadPosition


class SpreadExecutionPlanError(ValueError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class SpreadExecutionPlan:
    strategy_bucket: str
    action: str
    reason: str
    long_request: OrderRequest
    short_request: OrderRequest


class SpreadExecutionPlanner:
    def __init__(self, *, signal_ttl_ms: int = 1000) -> None:
        self.signal_ttl_ms = max(int(signal_ttl_ms or 0), 0)

    def build_entry_plan(
        self,
        intent: SpreadOrderIntent,
        *,
        quotes: dict[str, QuoteSnapshot],
        now_ms: int,
    ) -> SpreadExecutionPlan:
        long_quote = self._quote_for(quotes, intent.long_venue, intent.symbol)
        short_quote = self._quote_for(quotes, intent.short_venue, intent.symbol)
        self._require_fresh(long_quote, now_ms)
        self._require_fresh(short_quote, now_ms)
        notional = float(intent.entry_notional_quote or 0.0)
        if notional <= 0.0:
            raise SpreadExecutionPlanError("spread_notional_not_positive")
        long_price = float(long_quote.ask or 0.0)
        short_price = float(short_quote.bid or 0.0)
        if long_price <= 0.0 or short_price <= 0.0:
            raise SpreadExecutionPlanError("spread_quote_price_invalid")
        self._require_capacity(long_quote, Side.BUY, notional)
        self._require_capacity(short_quote, Side.SELL, notional)
        return SpreadExecutionPlan(
            strategy_bucket=intent.strategy_bucket,
            action="entry",
            reason=intent.reason,
            long_request=OrderRequest(
                venue=Venue.from_str(intent.long_venue),
                symbol=intent.symbol,
                side=Side.BUY,
                quantity=notional / long_price,
                price_hint=long_price,
                observed_at_ms=int(long_quote.observed_at_ms or 0),
                reduce_only=False,
                client_order_id=_client_order_id("entry-long", intent.candidate_id),
            ),
            short_request=OrderRequest(
                venue=Venue.from_str(intent.short_venue),
                symbol=intent.symbol,
                side=Side.SELL,
                quantity=notional / short_price,
                price_hint=short_price,
                observed_at_ms=int(short_quote.observed_at_ms or 0),
                reduce_only=False,
                client_order_id=_client_order_id("entry-short", intent.candidate_id),
            ),
        )

    def build_exit_plan(
        self,
        position: SpreadPosition,
        *,
        quotes: dict[str, QuoteSnapshot],
        now_ms: int,
        reason: str,
    ) -> SpreadExecutionPlan:
        long_quote = self._quote_for(quotes, position.long_venue, position.symbol)
        short_quote = self._quote_for(quotes, position.short_venue, position.symbol)
        self._require_fresh(long_quote, now_ms)
        self._require_fresh(short_quote, now_ms)
        notional = float(position.entry_notional_quote or 0.0)
        if notional <= 0.0:
            raise SpreadExecutionPlanError("spread_notional_not_positive")
        long_price = float(long_quote.bid or 0.0)
        short_price = float(short_quote.ask or 0.0)
        if long_price <= 0.0 or short_price <= 0.0:
            raise SpreadExecutionPlanError("spread_quote_price_invalid")
        return SpreadExecutionPlan(
            strategy_bucket=position.strategy_bucket,
            action="exit",
            reason=reason,
            long_request=OrderRequest(
                venue=Venue.from_str(position.long_venue),
                symbol=position.symbol,
                side=Side.SELL,
                quantity=notional / long_price,
                price_hint=long_price,
                observed_at_ms=int(long_quote.observed_at_ms or 0),
                reduce_only=True,
                client_order_id=_client_order_id("exit-long", position.position_id),
            ),
            short_request=OrderRequest(
                venue=Venue.from_str(position.short_venue),
                symbol=position.symbol,
                side=Side.BUY,
                quantity=notional / short_price,
                price_hint=short_price,
                observed_at_ms=int(short_quote.observed_at_ms or 0),
                reduce_only=True,
                client_order_id=_client_order_id("exit-short", position.position_id),
            ),
        )

    @staticmethod
    def _quote_for(
        quotes: dict[str, QuoteSnapshot],
        venue: str,
        symbol: str,
    ) -> QuoteSnapshot:
        wanted_venue = str(venue or "").lower()
        wanted_symbol = str(symbol or "").upper()
        for quote in quotes.values():
            if (
                str(getattr(quote, "venue", "") or "").lower() == wanted_venue
                and str(getattr(quote, "symbol", "") or "").upper() == wanted_symbol
            ):
                return quote
        raise SpreadExecutionPlanError("spread_quote_missing")

    def _require_fresh(self, quote: QuoteSnapshot, now_ms: int) -> None:
        observed = int(getattr(quote, "observed_at_ms", 0) or 0)
        if observed <= 0:
            raise SpreadExecutionPlanError("spread_quote_stale")
        if self.signal_ttl_ms and now_ms - observed > self.signal_ttl_ms:
            raise SpreadExecutionPlanError("spread_quote_stale")

    @staticmethod
    def _require_capacity(
        quote: QuoteSnapshot,
        side: Side,
        notional: float,
    ) -> None:
        if side == Side.BUY:
            capacity = float(getattr(quote, "ask_size", 0.0) or 0.0) * float(quote.ask or 0.0)
        else:
            capacity = float(getattr(quote, "bid_size", 0.0) or 0.0) * float(quote.bid or 0.0)
        if capacity > 0.0 and capacity < notional:
            raise SpreadExecutionPlanError("spread_quote_capacity_below_notional")


def _client_order_id(prefix: str, source: str) -> str:
    digest = hashlib.sha1(str(source or "").encode("utf-8")).hexdigest()[:12]
    return f"lf-spread-{prefix}-{digest}"
