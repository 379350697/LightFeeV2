"""Independent WebSocket best-bid/ask feed for entry quote readiness.

This module intentionally does not write to LocalL2Runtime.  It consumes
venue-native top-of-book ticker channels and stores only BBO quotes.
"""

from __future__ import annotations

import asyncio
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from typing import Any, Optional

import httpx
import websockets


@dataclass(frozen=True)
class TopBookQuote:
    venue: str
    symbol: str
    bid: float
    ask: float
    bid_size: float = 0.0
    ask_size: float = 0.0
    observed_at_ms: int = 0
    received_at_ms: int = 0
    exchange_event_at_ms: int = 0
    source: str = ""


@dataclass(frozen=True)
class RestTopBookQuoteResult:
    venue: str
    symbol: str
    outcome: str
    quote: TopBookQuote | None = None
    venue_symbol: str = ""
    endpoint: str = ""
    url: str = ""
    http_status: int = 0
    body_excerpt: str = ""
    bid: float = 0.0
    ask: float = 0.0
    observed_at_ms: int = 0
    received_at_ms: int = 0
    exchange_event_at_ms: int = 0
    attempt_interval_outcome: str = ""
    error: str = ""


class VenueBboCache:
    """Small normalized cache keyed by venue+canonical symbol."""

    def __init__(self) -> None:
        self._quotes: dict[tuple[str, str], TopBookQuote] = {}

    @staticmethod
    def _key(venue: str, symbol: str) -> tuple[str, str]:
        return str(venue or "").strip().lower(), str(symbol or "").strip().upper()

    def update_quote(
        self,
        quote: TopBookQuote,
        *,
        now_ms: int = 0,
        current_max_age_ms: int = 0,
    ) -> bool:
        if quote.bid <= 0.0 or quote.ask <= 0.0 or quote.bid >= quote.ask:
            return False
        venue, symbol = self._key(quote.venue, quote.symbol)
        if not venue or not symbol:
            return False
        normalized = replace(quote, venue=venue, symbol=symbol)
        current = self._quotes.get((venue, symbol))
        if current is not None:
            # Exchange event time and local receipt time are different clocks.
            # Compare exchange sequence time only when both observations expose
            # it; otherwise fall back to the comparable local receipt clock.
            current_event_ms = int(current.exchange_event_at_ms or 0)
            incoming_event_ms = int(normalized.exchange_event_at_ms or 0)
            current_received_ms = int(
                current.received_at_ms or current.observed_at_ms or 0
            )
            incoming_received_ms = int(
                normalized.received_at_ms or normalized.observed_at_ms or 0
            )
            incoming_is_rest = "rest" in str(normalized.source or "").lower()
            if (
                current_event_ms > 0
                and incoming_event_ms <= 0
                and incoming_is_rest
            ):
                # A REST receipt timestamp cannot prove that its market event
                # is newer than a WS event timestamp.  Keep the comparable WS
                # observation while its local lease is still valid.  Callers
                # that know the lease TTL may explicitly allow REST to replace
                # an expired observation; callers without that context remain
                # conservative.
                current_is_fresh = True
                if now_ms > 0 and current_max_age_ms > 0:
                    current_observed_ms = int(
                        current.observed_at_ms or current.received_at_ms or 0
                    )
                    current_is_fresh = bool(
                        current_observed_ms > 0
                        and current_observed_ms <= now_ms
                        and now_ms - current_observed_ms <= current_max_age_ms
                    )
                if current_is_fresh:
                    return False
            if current_event_ms > 0 and incoming_event_ms > 0:
                if incoming_event_ms < current_event_ms:
                    return False
                if (
                    incoming_event_ms == current_event_ms
                    and current_received_ms > incoming_received_ms
                ):
                    return False
            elif current_received_ms > incoming_received_ms > 0:
                return False
        self._quotes[(venue, symbol)] = normalized
        return True

    def get_quote(self, venue: str, symbol: str) -> TopBookQuote | None:
        return self._quotes.get(self._key(venue, symbol))

    def fresh_quote(
        self,
        venue: str,
        symbol: str,
        *,
        now_ms: int,
        max_age_ms: int,
    ) -> TopBookQuote | None:
        quote = self.get_quote(venue, symbol)
        if quote is None:
            return None
        if (
            quote.observed_at_ms <= 0
            or quote.observed_at_ms > now_ms
            or max_age_ms <= 0
        ):
            return None
        if now_ms - quote.observed_at_ms > max_age_ms:
            return None
        return quote

    def prune(
        self,
        tracked: set[tuple[str, str]],
        *,
        now_ms: int,
        retained_max_age_ms: int,
    ) -> None:
        tracked_keys = {self._key(venue, symbol) for venue, symbol in tracked}
        for key, quote in list(self._quotes.items()):
            if key in tracked_keys:
                continue
            observed = int(quote.observed_at_ms or 0)
            if observed <= 0 or now_ms - observed > retained_max_age_ms:
                self._quotes.pop(key, None)


def binance_bbo_stream_url(symbol: str) -> str:
    return f"wss://fstream.binance.com/public/ws/{symbol.lower()}@bookTicker"


def aster_bbo_stream_url(symbol: str) -> str:
    return f"wss://fstream.asterdex.com/ws/{symbol.lower()}@bookTicker"


def okx_bbo_stream_url() -> str:
    return "wss://ws.okx.com:8443/ws/v5/public"


def bybit_bbo_stream_url() -> str:
    return "wss://stream.bybit.com/v5/public/linear"


def bitget_bbo_stream_url() -> str:
    return "wss://ws.bitget.com/v2/ws/public"


def gate_bbo_stream_url() -> str:
    return "wss://fx-ws.gateio.ws/v4/ws/usdt"


def hyperliquid_bbo_stream_url() -> str:
    return "wss://api.hyperliquid.xyz/ws"


class RestTopBookQuoteRefresher:
    """Short-timeout public REST top-book refresh for tracked WS BBO gaps."""

    SUPPORTED_VENUES = {
        "binance",
        "aster",
        "bybit",
        "okx",
        "bitget",
        "gate",
        "hyperliquid",
    }
    MIN_ATTEMPT_INTERVAL_MS = 1_000
    GLOBAL_ASYNC_CONCURRENCY = 12
    VENUE_ASYNC_CONCURRENCY = 2

    def __init__(
        self,
        *,
        client: httpx.Client | None = None,
        async_client: httpx.AsyncClient | None = None,
        timeout_ms: int = 750,
        venue_async_concurrency: int | None = None,
        min_attempt_interval_ms: int | None = None,
    ) -> None:
        self._client = client
        self._owns_client = client is None
        self._async_client = async_client
        self._owns_async_client = async_client is None
        self._timeout_s = max(min(int(timeout_ms or 750), 1_000), 100) / 1000.0
        self._min_attempt_interval_ms = (
            self.MIN_ATTEMPT_INTERVAL_MS
            if min_attempt_interval_ms is None
            else max(int(min_attempt_interval_ms), 1)
        )
        self._last_attempt_ms: dict[tuple[str, str], int] = {}
        self._global_async_semaphore = asyncio.Semaphore(
            self.GLOBAL_ASYNC_CONCURRENCY
        )
        per_venue_concurrency = (
            self.VENUE_ASYNC_CONCURRENCY
            if venue_async_concurrency is None
            else max(
                min(int(venue_async_concurrency), self.GLOBAL_ASYNC_CONCURRENCY),
                1,
            )
        )
        self._venue_async_semaphores = {
            venue: asyncio.Semaphore(per_venue_concurrency)
            for venue in self.SUPPORTED_VENUES
        }
        self._async_inflight: dict[
            tuple[str, str], asyncio.Task[RestTopBookQuoteResult]
        ] = {}
        self._async_inflight_waiters: dict[tuple[str, str], int] = {}

    def close(self) -> None:
        if self._owns_client and self._client is not None:
            self._client.close()
            self._client = None

    async def aclose(self) -> None:
        inflight = list(self._async_inflight.values())
        self._async_inflight.clear()
        self._async_inflight_waiters.clear()
        for task in inflight:
            if not task.done():
                task.cancel()
        if inflight:
            await asyncio.gather(*inflight, return_exceptions=True)
        if self._owns_async_client and self._async_client is not None:
            await self._async_client.aclose()
            self._async_client = None

    async def arefresh_quote_result(
        self,
        venue: str,
        symbol: str,
        *,
        now_ms: int,
    ) -> RestTopBookQuoteResult:
        """Refresh one quote without blocking the event loop."""

        venue_key = str(venue or "").strip().lower()
        symbol_key = str(symbol or "").strip().upper()
        key = (venue_key, symbol_key)
        task = self._async_inflight.get(key)
        if task is None:
            task = asyncio.create_task(
                self._arefresh_quote_result_once(
                    venue_key,
                    symbol_key,
                    now_ms=now_ms,
                )
            )
            self._async_inflight[key] = task

            def _clear_inflight(
                completed: asyncio.Task[RestTopBookQuoteResult],
            ) -> None:
                if self._async_inflight.get(key) is completed:
                    self._async_inflight.pop(key, None)

            task.add_done_callback(_clear_inflight)

        self._async_inflight_waiters[key] = (
            self._async_inflight_waiters.get(key, 0) + 1
        )
        try:
            # One cancelled waiter must not cancel work still shared by another
            # waiter.  The final waiter, however, owns cancellation below.
            return await asyncio.shield(task)
        finally:
            remaining_waiters = self._async_inflight_waiters.get(key, 1) - 1
            if remaining_waiters > 0:
                self._async_inflight_waiters[key] = remaining_waiters
            else:
                self._async_inflight_waiters.pop(key, None)
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                if self._async_inflight.get(key) is task:
                    self._async_inflight.pop(key, None)

    async def _arefresh_quote_result_once(
        self,
        venue_key: str,
        symbol_key: str,
        *,
        now_ms: int,
    ) -> RestTopBookQuoteResult:
        metadata = self._quote_request_metadata(venue_key, symbol_key)
        rejected = self._begin_attempt(
            venue_key,
            symbol_key,
            now_ms=now_ms,
            metadata=metadata,
        )
        if rejected is not None:
            return rejected
        try:
            method, url, request_kwargs = self._quote_request(
                venue_key,
                symbol_key,
            )
            client = self._async_client
            if client is None:
                client = httpx.AsyncClient(timeout=httpx.Timeout(self._timeout_s))
                self._async_client = client
            venue_semaphore = self._venue_async_semaphores[venue_key]
            async with self._global_async_semaphore, venue_semaphore:
                response = await client.request(
                    method,
                    url,
                    timeout=self._timeout_s,
                    **request_kwargs,
                )
            response.raise_for_status()
            raw = response.json()
            received_at_ms = int(time.time() * 1000)
            quote = self._quote_from_raw(
                venue_key,
                symbol_key,
                raw,
                received_at_ms=received_at_ms,
            )
        except Exception as exc:
            return self._error_result(
                venue_key,
                symbol_key,
                metadata=metadata,
                exc=exc,
            )
        return self._resolved_result(
            venue_key,
            symbol_key,
            metadata=metadata,
            quote=quote,
        )

    def refresh_quote(
        self,
        venue: str,
        symbol: str,
        *,
        now_ms: int,
    ) -> TopBookQuote | None:
        return self.refresh_quote_result(venue, symbol, now_ms=now_ms).quote

    def refresh_quote_result(
        self,
        venue: str,
        symbol: str,
        *,
        now_ms: int,
    ) -> RestTopBookQuoteResult:
        venue_key = str(venue or "").strip().lower()
        symbol_key = str(symbol or "").strip().upper()
        metadata = self._quote_request_metadata(venue_key, symbol_key)
        rejected = self._begin_attempt(
            venue_key,
            symbol_key,
            now_ms=now_ms,
            metadata=metadata,
        )
        if rejected is not None:
            return rejected

        try:
            quote = self._refresh_quote_uncached(venue_key, symbol_key, now_ms)
        except Exception as exc:
            return self._error_result(
                venue_key,
                symbol_key,
                metadata=metadata,
                exc=exc,
            )
        return self._resolved_result(
            venue_key,
            symbol_key,
            metadata=metadata,
            quote=quote,
        )

    def _begin_attempt(
        self,
        venue: str,
        symbol: str,
        *,
        now_ms: int,
        metadata: dict[str, str],
    ) -> RestTopBookQuoteResult | None:
        if venue not in self.SUPPORTED_VENUES or not symbol:
            return RestTopBookQuoteResult(
                venue=venue,
                symbol=symbol,
                outcome="unsupported_symbol",
            )
        key = (venue, symbol)
        last_attempt_ms = int(self._last_attempt_ms.get(key, 0) or 0)
        if (
            last_attempt_ms > 0
            and now_ms - last_attempt_ms < self._min_attempt_interval_ms
        ):
            return RestTopBookQuoteResult(
                venue=venue,
                symbol=symbol,
                venue_symbol=metadata.get("venue_symbol", ""),
                endpoint=metadata.get("endpoint", ""),
                url=metadata.get("url", ""),
                outcome="throttled",
                attempt_interval_outcome="min_interval_not_elapsed",
            )
        self._last_attempt_ms[key] = now_ms
        return None

    @staticmethod
    def _error_result(
        venue: str,
        symbol: str,
        *,
        metadata: dict[str, str],
        exc: Exception,
    ) -> RestTopBookQuoteResult:
        response = exc.response if isinstance(exc, httpx.HTTPStatusError) else None
        status = int(response.status_code if response is not None else 0)
        body = _response_body_excerpt(response)
        parse_error = isinstance(exc, (ValueError, TypeError, json.JSONDecodeError))
        outcome = "parse_error" if parse_error else "http_error"
        if isinstance(exc, httpx.HTTPStatusError) and _looks_like_unsupported_symbol(status, body):
            outcome = "unsupported_symbol"
        return RestTopBookQuoteResult(
            venue=venue,
            symbol=symbol,
            venue_symbol=metadata.get("venue_symbol", ""),
            endpoint=metadata.get("endpoint", ""),
            url=metadata.get("url", ""),
            http_status=status,
            body_excerpt=body,
            outcome=outcome,
            error=f"{type(exc).__name__}: {exc}"[:240],
        )

    @staticmethod
    def _resolved_result(
        venue: str,
        symbol: str,
        *,
        metadata: dict[str, str],
        quote: TopBookQuote | None,
    ) -> RestTopBookQuoteResult:
        if quote is None:
            return RestTopBookQuoteResult(
                venue=venue,
                symbol=symbol,
                venue_symbol=metadata.get("venue_symbol", ""),
                endpoint=metadata.get("endpoint", ""),
                url=metadata.get("url", ""),
                outcome="invalid_quote",
            )
        return RestTopBookQuoteResult(
            venue=venue,
            symbol=symbol,
            quote=quote,
            venue_symbol=metadata.get("venue_symbol", ""),
            endpoint=metadata.get("endpoint", ""),
            url=metadata.get("url", ""),
            outcome="resolved",
            bid=float(quote.bid),
            ask=float(quote.ask),
            observed_at_ms=int(quote.observed_at_ms or 0),
            received_at_ms=int(quote.received_at_ms or 0),
            exchange_event_at_ms=int(quote.exchange_event_at_ms or 0),
        )

    def _quote_request_metadata(self, venue: str, symbol: str) -> dict[str, str]:
        from lightfee.core.domain import Venue
        from lightfee.venues.specs import get_spec

        try:
            venue_enum = Venue.from_str(venue)
            spec = get_spec(venue_enum)
            venue_symbol = spec.symbol_to_venue(symbol) if spec.symbol_to_venue else symbol
        except Exception:
            return {"venue_symbol": symbol, "endpoint": "", "url": ""}
        if venue == "okx":
            endpoint = "/api/v5/market/ticker"
        else:
            endpoint = spec.market_snapshot_path
        return {
            "venue_symbol": venue_symbol,
            "endpoint": endpoint,
            "url": spec.public_base_url + endpoint,
        }

    def _refresh_quote_uncached(
        self,
        venue: str,
        symbol: str,
        now_ms: int,
    ) -> TopBookQuote | None:
        method, url, request_kwargs = self._quote_request(venue, symbol)
        if method == "POST":
            raw = self._post_json(url, json=request_kwargs["json"])
        else:
            raw = self._get_json(url, params=request_kwargs["params"])
        # The synchronous compatibility API treats its injected clock as the
        # receipt clock.  The live async path records the actual arrival time.
        received_at_ms = int(now_ms)
        return self._quote_from_raw(
            venue,
            symbol,
            raw,
            received_at_ms=received_at_ms,
        )

    def _quote_request(
        self,
        venue: str,
        symbol: str,
    ) -> tuple[str, str, dict[str, Any]]:
        from lightfee.core.domain import Venue
        from lightfee.venues.specs import get_spec

        venue_enum = Venue.from_str(venue)
        spec = get_spec(venue_enum)
        venue_symbol = spec.symbol_to_venue(symbol) if spec.symbol_to_venue else symbol
        url = spec.public_base_url + spec.market_snapshot_path
        if venue in {"binance", "aster"}:
            return "GET", url, {"params": {"symbol": venue_symbol}}
        if venue == "bybit":
            return "GET", url, {"params": {"category": "linear", "symbol": venue_symbol}}
        if venue == "okx":
            return "GET", spec.public_base_url + "/api/v5/market/ticker", {"params": {"instId": venue_symbol}}
        if venue == "bitget":
            return "GET", url, {"params": {"productType": "USDT-FUTURES", "symbol": venue_symbol}}
        if venue == "gate":
            return "GET", url, {"params": {"contract": venue_symbol}}
        if venue == "hyperliquid":
            return "POST", url, {"json": {"type": "l2Book", "coin": venue_symbol}}
        raise ValueError(f"unsupported venue: {venue}")

    def _quote_from_raw(
        self,
        venue: str,
        symbol: str,
        raw: Any,
        *,
        received_at_ms: int,
    ) -> TopBookQuote | None:
        from lightfee.core.domain import Venue
        from lightfee.venues.specs import get_spec

        venue_enum = Venue.from_str(venue)
        spec = get_spec(venue_enum)
        venue_symbol = spec.symbol_to_venue(symbol) if spec.symbol_to_venue else symbol

        if venue in {"binance", "aster"}:
            row = self._select_row(raw, venue_symbol)
            exchange_event_at_ms = _int_ms(
                row.get("time"),
                row.get("E"),
                row.get("T"),
            )
            return self._make_rest_quote(
                venue=venue,
                symbol=symbol,
                bid=_float_value(row.get("bidPrice"), row.get("b")),
                ask=_float_value(row.get("askPrice"), row.get("a")),
                bid_size=_float_value(row.get("bidQty"), row.get("B")),
                ask_size=_float_value(row.get("askQty"), row.get("A")),
                observed_at_ms=received_at_ms,
                received_at_ms=received_at_ms,
                exchange_event_at_ms=exchange_event_at_ms,
            )

        if venue == "bybit":
            result = raw.get("result") if isinstance(raw, dict) else {}
            rows = result.get("list") if isinstance(result, dict) else []
            row = self._select_row(rows, venue_symbol)
            return self._make_rest_quote(
                venue=venue,
                symbol=symbol,
                bid=_float_value(row.get("bid1Price"), row.get("bidPrice")),
                ask=_float_value(row.get("ask1Price"), row.get("askPrice")),
                bid_size=_float_value(row.get("bid1Size"), row.get("bidSize")),
                ask_size=_float_value(row.get("ask1Size"), row.get("askSize")),
                observed_at_ms=_int_ms(row.get("ts"), raw.get("time"), received_at_ms),
                received_at_ms=received_at_ms,
                exchange_event_at_ms=_int_ms(row.get("ts"), raw.get("time")),
            )

        if venue == "okx":
            row = self._select_row(
                raw.get("data") if isinstance(raw, dict) else [],
                venue_symbol,
            )
            return self._make_rest_quote(
                venue=venue,
                symbol=symbol,
                bid=_float_value(row.get("bidPx")),
                ask=_float_value(row.get("askPx")),
                bid_size=_float_value(row.get("bidSz")),
                ask_size=_float_value(row.get("askSz")),
                observed_at_ms=_int_ms(row.get("ts"), raw.get("ts"), received_at_ms),
                received_at_ms=received_at_ms,
                exchange_event_at_ms=_int_ms(row.get("ts"), raw.get("ts")),
            )

        if venue == "bitget":
            rows = raw.get("data") if isinstance(raw, dict) else raw
            row = self._select_row(rows, venue_symbol)
            return self._make_rest_quote(
                venue=venue,
                symbol=symbol,
                bid=_float_value(row.get("bidPr"), row.get("bid1Price")),
                ask=_float_value(row.get("askPr"), row.get("ask1Price")),
                bid_size=_float_value(row.get("bidSz"), row.get("bid1Size")),
                ask_size=_float_value(row.get("askSz"), row.get("ask1Size")),
                observed_at_ms=_int_ms(row.get("ts"), raw.get("requestTime"), received_at_ms),
                received_at_ms=received_at_ms,
                exchange_event_at_ms=_int_ms(row.get("ts"), raw.get("requestTime")),
            )

        if venue == "gate":
            row = self._select_row(raw, venue_symbol)
            return self._make_rest_quote(
                venue=venue,
                symbol=symbol,
                bid=_float_value(row.get("highest_bid"), row.get("bid")),
                ask=_float_value(row.get("lowest_ask"), row.get("ask")),
                bid_size=_float_value(row.get("highest_size"), row.get("bid_size")),
                ask_size=_float_value(row.get("lowest_size"), row.get("ask_size")),
                observed_at_ms=_int_ms(row.get("time_ms"), row.get("time"), received_at_ms),
                received_at_ms=received_at_ms,
                exchange_event_at_ms=_int_ms(row.get("time_ms"), row.get("time")),
            )

        if venue == "hyperliquid":
            levels = raw.get("levels") if isinstance(raw, dict) else []
            bids = levels[0] if isinstance(levels, list) and len(levels) > 0 else []
            asks = levels[1] if isinstance(levels, list) and len(levels) > 1 else []
            bid_row = _first_row(bids)
            ask_row = _first_row(asks)
            return self._make_rest_quote(
                venue=venue,
                symbol=symbol,
                bid=_float_value(bid_row.get("px"), bid_row.get("price")),
                ask=_float_value(ask_row.get("px"), ask_row.get("price")),
                bid_size=_float_value(bid_row.get("sz"), bid_row.get("size")),
                ask_size=_float_value(ask_row.get("sz"), ask_row.get("size")),
                observed_at_ms=_int_ms(raw.get("time"), received_at_ms),
                received_at_ms=received_at_ms,
                exchange_event_at_ms=_int_ms(raw.get("time")),
            )

        return None

    def _get_json(self, url: str, *, params: dict[str, Any]) -> Any:
        if self._client is None:
            self._client = httpx.Client(timeout=httpx.Timeout(self._timeout_s))
        response = self._client.get(url, params=params, timeout=self._timeout_s)
        response.raise_for_status()
        return response.json()

    def _post_json(self, url: str, *, json: dict[str, Any]) -> Any:
        if self._client is None:
            self._client = httpx.Client(timeout=httpx.Timeout(self._timeout_s))
        response = self._client.post(url, json=json, timeout=self._timeout_s)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _select_row(raw: Any, venue_symbol: str) -> dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        if not isinstance(raw, list):
            return {}
        if len(raw) == 1 and isinstance(raw[0], dict):
            return raw[0]
        for row in raw:
            if not isinstance(row, dict):
                continue
            symbol = str(
                row.get("symbol")
                or row.get("instId")
                or row.get("contract")
                or ""
            )
            if symbol == venue_symbol:
                return row
        return {}

    @staticmethod
    def _make_rest_quote(
        *,
        venue: str,
        symbol: str,
        bid: float,
        ask: float,
        bid_size: float,
        ask_size: float,
        observed_at_ms: int,
        received_at_ms: int,
        exchange_event_at_ms: int = 0,
    ) -> TopBookQuote | None:
        quote = TopBookQuote(
            venue=venue,
            symbol=symbol,
            bid=bid,
            ask=ask,
            bid_size=bid_size,
            ask_size=ask_size,
            # Freshness is measured on the local receipt clock.  Exchange
            # event time remains available separately for ordering/auditing.
            observed_at_ms=received_at_ms or observed_at_ms,
            received_at_ms=received_at_ms,
            exchange_event_at_ms=exchange_event_at_ms,
            source=f"{venue}_rest_top_book",
        )
        if quote.bid <= 0.0 or quote.ask <= 0.0 or quote.bid >= quote.ask:
            return None
        return quote


def _float_value(*values: Any) -> float:
    for value in values:
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def _int_ms(*values: Any) -> int:
    for value in values:
        if value in (None, ""):
            continue
        try:
            parsed = int(float(value))
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            return parsed
    return 0


def _first_row(value: Any) -> dict[str, Any]:
    if isinstance(value, list) and value and isinstance(value[0], dict):
        return value[0]
    if isinstance(value, dict):
        return value
    return {}


def _response_body_excerpt(response: httpx.Response | None) -> str:
    if response is None:
        return ""
    try:
        text = response.text
    except Exception:
        return ""
    return text[:240]


def _looks_like_unsupported_symbol(status: int, body: str) -> bool:
    body_l = str(body or "").lower()
    return status in {400, 404} and (
        "invalid symbol" in body_l
        or "unknown symbol" in body_l
        or "symbol not found" in body_l
        or "-1121" in body_l
    )


@dataclass
class BboWsClient(ABC):
    venue: str
    symbol: str
    cache: VenueBboCache
    venue_symbol: str = ""

    OPEN_TIMEOUT_SECONDS: float = field(default=10.0, init=False)
    reconnect_delay_initial_s: float = 1.0
    reconnect_delay_max_s: float = 30.0
    ping_interval_s: float = 20.0

    _task: Optional[asyncio.Task] = field(default=None, init=False)
    _ws: Any = field(default=None, init=False)
    _closed: bool = field(default=False, init=False)
    _connected: bool = field(default=False, init=False)
    _subscribed: bool = field(default=False, init=False)
    _message_count: int = field(default=0, init=False)
    _last_quote: TopBookQuote | None = field(default=None, init=False)
    _last_error: str = field(default="", init=False)
    _last_control_message: str = field(default="", init=False)

    @property
    def wire_symbol(self) -> str:
        return self.venue_symbol or self.symbol

    @property
    def is_connected(self) -> bool:
        return self._connected and self._ws is not None

    @property
    def message_count(self) -> int:
        return self._message_count

    def state_snapshot(
        self,
        *,
        quote: TopBookQuote | None = None,
        now_ms: int = 0,
        max_age_ms: int = 0,
    ) -> dict[str, Any]:
        last_quote_age_ms: int | None = None
        if quote is not None and now_ms > 0 and int(quote.observed_at_ms or 0) > 0:
            last_quote_age_ms = max(now_ms - int(quote.observed_at_ms or 0), 0)
        subscribed = bool(self._subscribed or self.build_subscribe_message() is None)
        if quote is not None and max_age_ms > 0 and last_quote_age_ms is not None:
            if last_quote_age_ms <= max_age_ms:
                lease_state = "fresh"
            else:
                lease_state = "stale_ws_quote"
        elif not self.is_connected:
            lease_state = "not_connected"
        elif subscribed:
            lease_state = "subscribed_no_message"
        else:
            lease_state = "not_subscribed"
        return {
            "venue": str(self.venue),
            "symbol": str(self.symbol).upper(),
            "wire_symbol": str(self.wire_symbol),
            "tracked": True,
            "connected": bool(self.is_connected),
            "subscribed": subscribed,
            "message_count": int(self._message_count),
            "last_quote_age_ms": last_quote_age_ms,
            "last_quote_source": str(getattr(quote, "source", "") or ""),
            "lease_state": lease_state,
            "reason_bucket": lease_state,
            "last_error": str(self._last_error or ""),
            "last_control_message": str(self._last_control_message or ""),
        }

    async def start(self) -> None:
        if self._task is not None:
            return
        self._closed = False
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self._close_ws()

    @abstractmethod
    def websocket_url(self) -> str:
        ...

    @abstractmethod
    def build_subscribe_message(self) -> Optional[dict]:
        ...

    @abstractmethod
    def parse_bbo_message(
        self,
        raw: dict[str, Any],
        *,
        received_at_ms: int,
    ) -> TopBookQuote | None:
        ...

    async def _run_loop(self) -> None:
        delay = self.reconnect_delay_initial_s
        while not self._closed:
            try:
                await self._connect_and_read()
                delay = self.reconnect_delay_initial_s
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._last_error = f"{type(exc).__name__}: {exc}"
            if self._closed:
                break
            await asyncio.sleep(delay)
            delay = min(delay * 2, self.reconnect_delay_max_s)

    async def _connect_and_read(self) -> None:
        async with websockets.connect(
            self.websocket_url(),
            ping_interval=self.ping_interval_s,
            open_timeout=self.OPEN_TIMEOUT_SECONDS,
            close_timeout=5,
            max_size=2**20,
        ) as ws:
            self._ws = ws
            self._connected = True
            sub_msg = self.build_subscribe_message()
            if sub_msg is not None:
                await ws.send(json.dumps(sub_msg))
            async for raw_msg in ws:
                await self._handle_message(raw_msg)
        self._connected = False
        self._ws = None

    async def _handle_message(self, raw_msg: str | bytes) -> None:
        try:
            if isinstance(raw_msg, bytes):
                raw_msg = raw_msg.decode("utf-8")
            payload = json.loads(raw_msg)
        except json.JSONDecodeError:
            return
        if not isinstance(payload, dict):
            return
        if isinstance(payload.get("data"), dict) and "stream" in payload:
            payload = payload["data"]
        received_at_ms = int(time.time() * 1000)
        if self._handle_control_message(payload):
            return
        quote = self.parse_bbo_message(payload, received_at_ms=received_at_ms)
        if quote is None:
            return
        if self.cache.update_quote(quote):
            self._message_count += 1

    def _handle_control_message(self, payload: dict[str, Any]) -> bool:
        event = str(payload.get("event", "")).lower()
        op = str(payload.get("op", "")).lower()
        channel = str(payload.get("channel", "")).lower()

        code = str(payload.get("code") or payload.get("retCode") or "")
        msg = str(
            payload.get("msg")
            or payload.get("ret_msg")
            or payload.get("retMsg")
            or ""
        )
        success = payload.get("success")
        if event == "error" or success is False or (code and code not in {"0", "200"}):
            self._last_error = (
                f"ws_control_error code={code or 'unknown'} msg={msg or 'unknown'}"
            )
            self._last_control_message = self._last_error
            return True

        if event in {"subscribe", "unsubscribe"} or op in {"subscribe", "unsubscribe"}:
            if event == "subscribe" or op == "subscribe":
                self._subscribed = True
            elif event == "unsubscribe" or op == "unsubscribe":
                self._subscribed = False
            self._last_control_message = event or op
            return True
        if op in {"subscribe", "unsubscribe", "ping", "pong"}:
            return True
        if channel in {"subscriptionresponse", "subscription_response", "pong", "heartbeat"}:
            if channel in {"subscriptionresponse", "subscription_response"}:
                self._subscribed = True
            self._last_control_message = channel
            return True
        if event in {"ping", "pong"}:
            return True
        return False

    async def _close_ws(self) -> None:
        self._connected = False
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    def _make_quote(
        self,
        *,
        bid: float,
        ask: float,
        bid_size: float,
        ask_size: float,
        observed_at_ms: int,
        received_at_ms: int,
        source: str,
        symbol: str = "",
    ) -> TopBookQuote | None:
        quote = TopBookQuote(
            venue=self.venue,
            symbol=symbol or self.symbol,
            bid=bid,
            ask=ask,
            bid_size=bid_size,
            ask_size=ask_size,
            # Never mix venue event clocks with local freshness clocks.
            observed_at_ms=received_at_ms or observed_at_ms,
            received_at_ms=received_at_ms,
            exchange_event_at_ms=observed_at_ms,
            source=source,
        )
        if quote.bid <= 0.0 or quote.ask <= 0.0 or quote.bid >= quote.ask:
            return None
        self._last_quote = quote
        return quote

    def _merge_with_last(
        self,
        *,
        bid: float,
        ask: float,
        bid_size: float,
        ask_size: float,
    ) -> tuple[float, float, float, float]:
        last = self._last_quote
        if last is None:
            return bid, ask, bid_size, ask_size
        return (
            bid if bid > 0.0 else last.bid,
            ask if ask > 0.0 else last.ask,
            bid_size if bid_size > 0.0 else last.bid_size,
            ask_size if ask_size > 0.0 else last.ask_size,
        )


@dataclass
class BinanceBboWsClient(BboWsClient):
    def websocket_url(self) -> str:
        return binance_bbo_stream_url(self.symbol)

    def build_subscribe_message(self) -> Optional[dict]:
        return None

    def parse_bbo_message(
        self,
        raw: dict[str, Any],
        *,
        received_at_ms: int,
    ) -> TopBookQuote | None:
        if raw.get("e") not in ("bookTicker", None) and "b" not in raw:
            return None
        return self._make_quote(
            bid=_float_value(raw.get("b")),
            ask=_float_value(raw.get("a")),
            bid_size=_float_value(raw.get("B")),
            ask_size=_float_value(raw.get("A")),
            observed_at_ms=_int_ms(raw.get("E"), raw.get("T")),
            received_at_ms=received_at_ms,
            source=f"{self.venue}_book_ticker",
            symbol=str(raw.get("s") or self.symbol).upper(),
        )


@dataclass
class AsterBboWsClient(BinanceBboWsClient):
    def websocket_url(self) -> str:
        return aster_bbo_stream_url(self.symbol)


@dataclass
class OkxBboWsClient(BboWsClient):
    def websocket_url(self) -> str:
        return okx_bbo_stream_url()

    def build_subscribe_message(self) -> Optional[dict]:
        return {
            "op": "subscribe",
            "args": [{"channel": "tickers", "instId": self.wire_symbol}],
        }

    def parse_bbo_message(
        self,
        raw: dict[str, Any],
        *,
        received_at_ms: int,
    ) -> TopBookQuote | None:
        arg = raw.get("arg") or {}
        if arg.get("channel") != "tickers":
            return None
        row = _first_row(raw.get("data"))
        if not row:
            return None
        return self._make_quote(
            bid=_float_value(row.get("bidPx")),
            ask=_float_value(row.get("askPx")),
            bid_size=_float_value(row.get("bidSz")),
            ask_size=_float_value(row.get("askSz")),
            observed_at_ms=_int_ms(row.get("ts"), raw.get("ts")),
            received_at_ms=received_at_ms,
            source="okx_tickers",
        )


@dataclass
class BybitBboWsClient(BboWsClient):
    def websocket_url(self) -> str:
        return bybit_bbo_stream_url()

    def build_subscribe_message(self) -> Optional[dict]:
        return {"op": "subscribe", "args": [f"tickers.{self.wire_symbol}"]}

    def parse_bbo_message(
        self,
        raw: dict[str, Any],
        *,
        received_at_ms: int,
    ) -> TopBookQuote | None:
        topic = str(raw.get("topic", ""))
        if not topic.startswith("tickers."):
            return None
        row = _first_row(raw.get("data"))
        if not row:
            return None
        bid = _float_value(row.get("bid1Price"), row.get("bidPrice"), row.get("bidPx"))
        ask = _float_value(row.get("ask1Price"), row.get("askPrice"), row.get("askPx"))
        bid_size = _float_value(row.get("bid1Size"), row.get("bidSize"), row.get("bidSz"))
        ask_size = _float_value(row.get("ask1Size"), row.get("askSize"), row.get("askSz"))
        bid, ask, bid_size, ask_size = self._merge_with_last(
            bid=bid,
            ask=ask,
            bid_size=bid_size,
            ask_size=ask_size,
        )
        return self._make_quote(
            bid=bid,
            ask=ask,
            bid_size=bid_size,
            ask_size=ask_size,
            observed_at_ms=_int_ms(raw.get("ts"), row.get("ts")),
            received_at_ms=received_at_ms,
            source="bybit_tickers",
        )


@dataclass
class BitgetBboWsClient(BboWsClient):
    def websocket_url(self) -> str:
        return bitget_bbo_stream_url()

    def build_subscribe_message(self) -> Optional[dict]:
        return {
            "op": "subscribe",
            "args": [{
                "instType": "USDT-FUTURES",
                "channel": "ticker",
                "instId": self.wire_symbol,
            }],
        }

    def parse_bbo_message(
        self,
        raw: dict[str, Any],
        *,
        received_at_ms: int,
    ) -> TopBookQuote | None:
        arg = raw.get("arg") or {}
        channel = str(arg.get("channel", arg.get("topic", "")))
        if channel != "ticker":
            return None
        row = _first_row(raw.get("data"))
        if not row:
            return None
        return self._make_quote(
            bid=_float_value(row.get("bidPr"), row.get("bid1Price")),
            ask=_float_value(row.get("askPr"), row.get("ask1Price")),
            bid_size=_float_value(row.get("bidSz"), row.get("bid1Size")),
            ask_size=_float_value(row.get("askSz"), row.get("ask1Size")),
            observed_at_ms=_int_ms(row.get("ts"), raw.get("ts")),
            received_at_ms=received_at_ms,
            source="bitget_ticker",
        )


@dataclass
class GateBboWsClient(BboWsClient):
    def websocket_url(self) -> str:
        return gate_bbo_stream_url()

    def build_subscribe_message(self) -> Optional[dict]:
        return {
            "time": int(time.time()),
            "channel": "futures.book_ticker",
            "event": "subscribe",
            "payload": [self.wire_symbol],
        }

    def parse_bbo_message(
        self,
        raw: dict[str, Any],
        *,
        received_at_ms: int,
    ) -> TopBookQuote | None:
        if raw.get("channel") != "futures.book_ticker":
            return None
        result = raw.get("result") or {}
        if not isinstance(result, dict):
            return None
        return self._make_quote(
            bid=_float_value(result.get("b")),
            ask=_float_value(result.get("a")),
            bid_size=_float_value(result.get("B")),
            ask_size=_float_value(result.get("A")),
            observed_at_ms=_int_ms(result.get("t"), raw.get("time_ms")),
            received_at_ms=received_at_ms,
            source="gate_futures_book_ticker",
        )


@dataclass
class HyperliquidBboWsClient(BboWsClient):
    def websocket_url(self) -> str:
        return hyperliquid_bbo_stream_url()

    def build_subscribe_message(self) -> Optional[dict]:
        return {
            "method": "subscribe",
            "subscription": {"type": "bbo", "coin": self.wire_symbol},
        }

    def parse_bbo_message(
        self,
        raw: dict[str, Any],
        *,
        received_at_ms: int,
    ) -> TopBookQuote | None:
        channel = raw.get("channel")
        data = raw.get("data") or {}
        if channel == "bbo":
            bbo = data.get("bbo") or []
            bid_row = bbo[0] if len(bbo) > 0 and isinstance(bbo[0], dict) else {}
            ask_row = bbo[1] if len(bbo) > 1 and isinstance(bbo[1], dict) else {}
            return self._make_quote(
                bid=_float_value(bid_row.get("px")),
                ask=_float_value(ask_row.get("px")),
                bid_size=_float_value(bid_row.get("sz")),
                ask_size=_float_value(ask_row.get("sz")),
                observed_at_ms=_int_ms(data.get("time")),
                received_at_ms=received_at_ms,
                source="hyperliquid_bbo",
            )
        if channel == "l2Book":
            levels = data.get("levels") or []
            bids = levels[0] if len(levels) > 0 else []
            asks = levels[1] if len(levels) > 1 else []
            bid_row = bids[0] if bids and isinstance(bids[0], dict) else {}
            ask_row = asks[0] if asks and isinstance(asks[0], dict) else {}
            return self._make_quote(
                bid=_float_value(bid_row.get("px")),
                ask=_float_value(ask_row.get("px")),
                bid_size=_float_value(bid_row.get("sz")),
                ask_size=_float_value(ask_row.get("sz")),
                observed_at_ms=_int_ms(data.get("time")),
                received_at_ms=received_at_ms,
                source="hyperliquid_l2_book_top",
            )
        return None


BBO_CLIENT_REGISTRY: dict[str, type[BboWsClient]] = {
    "binance": BinanceBboWsClient,
    "okx": OkxBboWsClient,
    "bybit": BybitBboWsClient,
    "bitget": BitgetBboWsClient,
    "gate": GateBboWsClient,
    "aster": AsterBboWsClient,
    "hyperliquid": HyperliquidBboWsClient,
}


def create_bbo_ws_client(
    *,
    venue: str,
    symbol: str,
    cache: VenueBboCache,
) -> BboWsClient | None:
    venue_key = str(venue or "").strip().lower()
    cls = BBO_CLIENT_REGISTRY.get(venue_key)
    if cls is None:
        return None

    venue_symbol = symbol
    try:
        from lightfee.core.domain import Venue
        from lightfee.venues.specs import get_spec

        venue_enum = Venue.from_str(venue_key)
        spec = get_spec(venue_enum)
        if spec.symbol_to_venue is not None:
            venue_symbol = spec.symbol_to_venue(symbol)
    except (ValueError, KeyError):
        pass

    return cls(
        venue=venue_key,
        symbol=str(symbol or "").strip().upper(),
        venue_symbol=str(venue_symbol or symbol),
        cache=cache,
    )


class VenueBboDataPlane:
    """Owns BBO WS clients without touching LocalL2Runtime."""

    def __init__(self, cache: VenueBboCache, journal: Any = None) -> None:
        self._cache = cache
        self._journal = journal
        self._clients: dict[tuple[str, str], BboWsClient] = {}

    @staticmethod
    def _key(venue: str, symbol: str) -> tuple[str, str]:
        return str(venue or "").strip().lower(), str(symbol or "").strip().upper()

    def start_ws_streams(self, venue: str, symbols: list[str], adapter: Any = None) -> int:
        started = 0
        for symbol in symbols:
            key = self._key(venue, symbol)
            if not key[0] or not key[1] or key in self._clients:
                continue
            client = create_bbo_ws_client(venue=key[0], symbol=key[1], cache=self._cache)
            if client is None:
                continue
            self._clients[key] = client
            started += 1
        return started

    async def connect_ws_streams(self) -> int:
        connected = 0
        for client in list(self._clients.values()):
            if not client.is_connected:
                await client.start()
                connected += 1
        return connected

    async def stop_ws_streams(self, *, per_client_timeout_s: float = 5.0) -> None:
        for client in list(self._clients.values()):
            try:
                await asyncio.wait_for(client.stop(), timeout=per_client_timeout_s)
            except asyncio.TimeoutError:
                task = getattr(client, "_task", None)
                if task is not None and not task.done():
                    task.cancel()
                client._closed = True
                client._connected = False
                client._ws = None
        self._clients.clear()

    async def reconcile_ws_streams(
        self,
        tracked: set[tuple[str, str]],
        *,
        per_client_timeout_s: float = 1.0,
    ) -> int:
        """Stop and forget clients outside the current generation budget."""

        tracked_keys = {self._key(venue, symbol) for venue, symbol in tracked}
        stale_clients = [
            (key, client)
            for key, client in list(self._clients.items())
            if key not in tracked_keys
        ]

        async def _stop_one(
            key: tuple[str, str],
            client: BboWsClient,
        ) -> None:
            try:
                await asyncio.wait_for(
                    client.stop(),
                    timeout=per_client_timeout_s,
                )
            except asyncio.TimeoutError:
                task = getattr(client, "_task", None)
                if task is not None and not task.done():
                    task.cancel()
                client._closed = True
                client._connected = False
                client._ws = None
            finally:
                if self._clients.get(key) is client:
                    self._clients.pop(key, None)

        if stale_clients:
            await asyncio.gather(
                *(
                    _stop_one(key, client)
                    for key, client in stale_clients
                )
            )
        return len(stale_clients)

    @property
    def active_ws_stream_count(self) -> int:
        return sum(1 for client in self._clients.values() if client.is_connected)

    def get_quote(self, venue: str, symbol: str) -> TopBookQuote | None:
        return self._cache.get_quote(venue, symbol)

    def stream_state(
        self,
        venue: str,
        symbol: str,
        *,
        now_ms: int = 0,
        max_age_ms: int = 0,
    ) -> dict[str, Any]:
        key = self._key(venue, symbol)
        client = self._clients.get(key)
        if client is None:
            return {
                "venue": key[0],
                "symbol": key[1],
                "tracked": False,
                "connected": False,
                "subscribed": False,
                "message_count": 0,
                "last_quote_age_ms": None,
                "last_quote_source": "",
                "lease_state": "not_tracked",
                "reason_bucket": "not_tracked",
                "last_error": "",
            }
        return client.state_snapshot(
            quote=self._cache.get_quote(key[0], key[1]),
            now_ms=now_ms,
            max_age_ms=max_age_ms,
        )

    def stream_states(self) -> list[dict[str, Any]]:
        return [
            client.state_snapshot()
            for _, client in sorted(self._clients.items())
        ]

    def fresh_quote(
        self,
        venue: str,
        symbol: str,
        *,
        now_ms: int,
        max_age_ms: int,
    ) -> TopBookQuote | None:
        return self._cache.fresh_quote(
            venue,
            symbol,
            now_ms=now_ms,
            max_age_ms=max_age_ms,
        )

    def prune_untracked_quotes(
        self,
        tracked: set[tuple[str, str]],
        now_ms: int,
        *,
        retained_max_age_ms: int,
    ) -> None:
        self._cache.prune(
            tracked,
            now_ms=now_ms,
            retained_max_age_ms=retained_max_age_ms,
        )
