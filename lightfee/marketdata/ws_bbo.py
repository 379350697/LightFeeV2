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
    source: str = ""


class VenueBboCache:
    """Small normalized cache keyed by venue+canonical symbol."""

    def __init__(self) -> None:
        self._quotes: dict[tuple[str, str], TopBookQuote] = {}

    @staticmethod
    def _key(venue: str, symbol: str) -> tuple[str, str]:
        return str(venue or "").strip().lower(), str(symbol or "").strip().upper()

    def update_quote(self, quote: TopBookQuote) -> bool:
        if quote.bid <= 0.0 or quote.ask <= 0.0 or quote.bid >= quote.ask:
            return False
        venue, symbol = self._key(quote.venue, quote.symbol)
        if not venue or not symbol:
            return False
        normalized = replace(quote, venue=venue, symbol=symbol)
        current = self._quotes.get((venue, symbol))
        if current is not None and quote.observed_at_ms > 0:
            if current.observed_at_ms > quote.observed_at_ms:
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
        if quote.observed_at_ms <= 0 or max_age_ms <= 0:
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
    return f"wss://fstream.binance.com/ws/{symbol.lower()}@bookTicker"


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
    _message_count: int = field(default=0, init=False)
    _last_quote: TopBookQuote | None = field(default=None, init=False)
    _last_error: str = field(default="", init=False)

    @property
    def wire_symbol(self) -> str:
        return self.venue_symbol or self.symbol

    @property
    def is_connected(self) -> bool:
        return self._connected and self._ws is not None

    @property
    def message_count(self) -> int:
        return self._message_count

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
        if self._is_control_message(payload):
            return
        if isinstance(payload.get("data"), dict) and "stream" in payload:
            payload = payload["data"]
        received_at_ms = int(time.time() * 1000)
        quote = self.parse_bbo_message(payload, received_at_ms=received_at_ms)
        if quote is None:
            return
        if self.cache.update_quote(quote):
            self._message_count += 1

    @staticmethod
    def _is_control_message(payload: dict[str, Any]) -> bool:
        event = str(payload.get("event", "")).lower()
        op = str(payload.get("op", "")).lower()
        channel = str(payload.get("channel", "")).lower()
        if event in {"subscribe", "unsubscribe", "ping", "pong"}:
            return True
        if op in {"subscribe", "unsubscribe", "ping", "pong"}:
            return True
        if channel in {"subscriptionresponse", "subscription_response", "pong", "heartbeat"}:
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
            observed_at_ms=observed_at_ms or received_at_ms,
            received_at_ms=received_at_ms,
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

    @property
    def active_ws_stream_count(self) -> int:
        return sum(1 for client in self._clients.values() if client.is_connected)

    def get_quote(self, venue: str, symbol: str) -> TopBookQuote | None:
        return self._cache.get_quote(venue, symbol)

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
