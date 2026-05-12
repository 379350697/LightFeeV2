"""Local-L2 WebSocket streaming client — per-venue real-time L2 delta ingestion.

Rust V1 references:
  - src/live/aster.rs:236-268 (begin_aster_local_l2_ws_session)
  - src/market_gateway/ports.rs (WsWorkerCategory, ws_worker_categories)

Each venue's WebSocket depth stream is connected in a background task.
Parsed deltas are fed into LocalL2DataPlane.ingest_external_update() which
routes them to LocalL2Runtime.record_update().

Lifecycle:
  1. REST snapshot bootstrap (via data plane) initialises the book
  2. WS stream connects and subscribes to per-symbol depth deltas
  3. Each delta is parsed into LocalL2Update and ingested
  4. On disconnect, the book falls back to REST periodic refresh
"""

from __future__ import annotations

import asyncio
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional, TYPE_CHECKING

import websockets
import websockets.exceptions as ws_exc

from lightfee.marketdata.l2 import (
    LocalL2Update,
    LocalL2UpdateKind,
    PriceLevel,
)
from lightfee.venues.transport import TransportError, TransportErrorCategory

if TYPE_CHECKING:
    from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane


# ---------------------------------------------------------------------------
# Venue WebSocket URL builders
# ---------------------------------------------------------------------------


def binance_depth_stream_url(symbol: str) -> str:
    """Binance USDM futures per-symbol depth delta stream (100ms)."""
    sym = symbol.lower()
    return f"wss://fstream.binance.com/ws/{sym}@depth@100ms"


def okx_depth_stream_url() -> str:
    """OKX V5 public depth channel (all symbols, books channel)."""
    return "wss://ws.okx.com:8443/ws/v5/public"


def bybit_depth_stream_url() -> str:
    """Bybit V5 public orderbook depth stream."""
    return "wss://stream.bybit.com/v5/public/linear"


def bitget_depth_stream_url() -> str:
    """Bitget V2 public depth WebSocket."""
    return "wss://ws.bitget.com/v2/ws/public"


def gate_depth_stream_url() -> str:
    """Gate.io futures USDT WebSocket."""
    return "wss://fx-ws.gateio.ws/v4/ws/usdt"


def aster_depth_stream_url(symbol: str) -> str:
    """Aster (Binance-compatible) per-symbol depth delta stream."""
    return f"wss://fstream.aster.exchange/ws/{symbol.lower()}@depth@100ms"


# ---------------------------------------------------------------------------
# WS client state
# ---------------------------------------------------------------------------


class WsClientState:
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    CLOSED = "closed"


# ---------------------------------------------------------------------------
# Abstract base WS client
# ---------------------------------------------------------------------------


@dataclass
class LocalL2WsClient(ABC):
    """Abstract WebSocket L2 depth client for a single venue/symbol pair.

    Subclasses implement _build_subscribe_message() and _parse_depth_message()
    for venue-specific wire formats.
    """

    venue: str
    symbol: str  # canonical internal symbol (e.g. BTCUSDT)
    data_plane: "LocalL2DataPlane"
    venue_symbol: str = ""  # venue wire symbol (e.g. BTC-USDT-SWAP for OKX); defaults to symbol

    # V1: bounded connection setup (OS TCP timeout can be 60+ s)
    OPEN_TIMEOUT_SECONDS: float = field(default=10.0, init=False)

    # Config
    reconnect_delay_initial_s: float = 1.0
    reconnect_delay_max_s: float = 30.0
    ping_interval_s: float = 20.0

    # Runtime state
    _state: str = field(default=WsClientState.DISCONNECTED, init=False)
    _ws: Any = field(default=None, init=False)
    _reconnect_delay_s: float = field(default=0.0, init=False)
    _task: Optional[asyncio.Task] = field(default=None, init=False)
    _message_count: int = field(default=0, init=False)
    _error_count: int = field(default=0, init=False)
    _last_message_ms: int = field(default=0, init=False)
    _last_error: str = field(default="", init=False)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the WS client as a background task."""
        if self._task is not None:
            return
        self._state = WsClientState.CONNECTING
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """Stop the WS client and clean up."""
        self._state = WsClientState.CLOSED
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self._close_ws()

    @property
    def state(self) -> str:
        return self._state

    @property
    def wire_symbol(self) -> str:
        """Venue wire symbol (e.g. BTC-USDT-SWAP), falling back to canonical symbol."""
        return self.venue_symbol or self.symbol

    @property
    def is_connected(self) -> bool:
        return self._state == WsClientState.CONNECTED and self._ws is not None

    @property
    def message_count(self) -> int:
        return self._message_count

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------

    @abstractmethod
    def websocket_url(self) -> str:
        """Return the WebSocket endpoint URL for this venue."""
        ...

    @abstractmethod
    def build_subscribe_message(self) -> Optional[dict]:
        """Build the subscribe request payload for the depth channel.

        Return None if this venue's stream URL auto-subscribes (no subscribe
        frame needed) — e.g. Binance/Aster per‑symbol streams.
        """
        ...

    @abstractmethod
    def parse_depth_message(self, raw: dict) -> Optional[LocalL2Update]:
        """Parse a raw WS message into a LocalL2Update, or None if not a depth update."""
        ...

    # ------------------------------------------------------------------
    # Connection loop
    # ------------------------------------------------------------------

    async def _run_loop(self) -> None:
        """Main connection loop with reconnect backoff."""
        while self._state not in (WsClientState.CLOSED,):
            try:
                await self._connect_and_read()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._error_count += 1
                self._last_error = f"{type(e).__name__}: {e}"

            if self._state == WsClientState.CLOSED:
                break

            # Reconnect backoff
            self._state = WsClientState.RECONNECTING
            delay = self._reconnect_delay_s or self.reconnect_delay_initial_s
            await asyncio.sleep(delay)
            self._reconnect_delay_s = min(
                delay * 2, self.reconnect_delay_max_s
            )

    async def _connect_and_read(self) -> None:
        """Establish WS connection, subscribe, and read messages.

        On each (re)connect, notifies the data plane to advance the stream
        generation and reset sequence state for any bootstrapping book.
        V1: reset_binance_local_l2_bootstrap_stream_state_for_instance in on_connect closure.
        """
        url = self.websocket_url()
        self._state = WsClientState.CONNECTING

        async with websockets.connect(
            url,
            ping_interval=self.ping_interval_s,
            open_timeout=self.OPEN_TIMEOUT_SECONDS,
            close_timeout=5,
            max_size=2**20,  # 1MB
        ) as ws:
            self._ws = ws
            self._state = WsClientState.CONNECTED
            self._reconnect_delay_s = 0  # reset on successful connect

            # Subscribe to depth channel (skip if venue auto-subscribes)
            sub_msg = self.build_subscribe_message()
            if sub_msg is not None:
                await ws.send(json.dumps(sub_msg))

            # Notify data plane: new WS stream → advance generation, clear old buffers
            # V1: on_connect closure calling reset_binance_local_l2_bootstrap_stream_state_for_instance
            self.data_plane.reset_stream_state(self.venue, [self.symbol])

            # Read loop
            async for raw_msg in ws:
                await self._handle_message(raw_msg)

    async def _handle_message(self, raw_msg: str | bytes) -> None:
        """Parse a single WS message and ingest into the data plane."""
        try:
            if isinstance(raw_msg, bytes):
                raw_msg = raw_msg.decode("utf-8")
            payload = json.loads(raw_msg)
        except json.JSONDecodeError:
            return

        # Venue-specific parsing
        update = self.parse_depth_message(payload)
        if update is None:
            return

        now_ms = int(time.time() * 1000)
        self._message_count += 1
        self._last_message_ms = now_ms

        try:
            self.data_plane.ingest_external_update(update, now_ms)
        except Exception:
            self._error_count += 1

    async def _close_ws(self) -> None:
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None


# ---------------------------------------------------------------------------
# Binance USDM Futures WS client
# ---------------------------------------------------------------------------

# Binance delta stream: {"e":"depthUpdate","E":...,"s":"BTCUSDT","U":...,"u":...,"b":[[p,q],...],"a":[[p,q],...]}

@dataclass
class BinanceL2WsClient(LocalL2WsClient):
    """Binance USDM futures depth delta WebSocket client."""

    def websocket_url(self) -> str:
        return binance_depth_stream_url(self.symbol)

    def build_subscribe_message(self) -> Optional[dict]:
        # Binance per-symbol streams auto-subscribe on connect — no subscribe message needed
        return None

    def parse_depth_message(self, raw: dict) -> Optional[LocalL2Update]:
        if raw.get("e") != "depthUpdate":
            return None

        return LocalL2Update(
            venue=self.venue,
            symbol=self.symbol,
            bids=[PriceLevel(price=float(p), quantity=float(q)) for p, q in raw.get("b", [])],
            asks=[PriceLevel(price=float(p), quantity=float(q)) for p, q in raw.get("a", [])],
            sequence=int(raw.get("u", 0)),
            previous_sequence=int(raw.get("U", 0)) - 1 if raw.get("U") else 0,
            event_time_ms=int(raw.get("E", 0)),
            received_at_ms=int(time.time() * 1000),
            update_kind=LocalL2UpdateKind.DELTA,
        )


# ---------------------------------------------------------------------------
# OKX V5 WS client
# ---------------------------------------------------------------------------

# OKX depth channel (books):
# Subscribe: {"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT-SWAP"}]}
# Response: {"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"snapshot","data":[{"asks":[[p,q,...]],"bids":[[p,q,...]],"ts":...,"checksum":-1}]}
# Delta:     {"arg":{...},"action":"update","data":[{"asks":[[p,q,...]],"bids":[[p,q,...]],"ts":...,"checksum":-1}]}

@dataclass
class OkxL2WsClient(LocalL2WsClient):
    """OKX V5 public depth WebSocket client."""

    def websocket_url(self) -> str:
        return okx_depth_stream_url()

    def build_subscribe_message(self) -> Optional[dict]:
        # Subscribe with venue wire symbol (e.g. BTC-USDT-SWAP) to match OKX API
        return {
            "op": "subscribe",
            "args": [{"channel": "books", "instId": self.wire_symbol}],
        }

    def parse_depth_message(self, raw: dict) -> Optional[LocalL2Update]:
        arg = raw.get("arg", {})
        if arg.get("channel") != "books":
            return None

        action = raw.get("action", "")
        data_list = raw.get("data", [])
        if not data_list:
            return None

        row = data_list[0]
        now_ms = int(time.time() * 1000)

        kind = LocalL2UpdateKind.SNAPSHOT if action == "snapshot" else LocalL2UpdateKind.DELTA

        return LocalL2Update(
            venue=self.venue,
            symbol=self.symbol,
            bids=[PriceLevel(price=float(p), quantity=float(q)) for p, q, *_ in row.get("bids", [])],
            asks=[PriceLevel(price=float(p), quantity=float(q)) for p, q, *_ in row.get("asks", [])],
            sequence=int(row.get("seqId", 0)),
            checksum=int(row.get("checksum", -1)),
            event_time_ms=int(row.get("ts", 0)),
            received_at_ms=now_ms,
            update_kind=kind,
        )


# ---------------------------------------------------------------------------
# Bybit V5 WS client
# ---------------------------------------------------------------------------

# Bybit orderbook depth:
# Subscribe: {"op":"subscribe","args":["orderbook.1.BTCUSDT"]}
# Response: {"topic":"orderbook.1.BTCUSDT","type":"snapshot","data":{"s":"BTCUSDT","b":[[p,q],...],"a":[[p,q],...],"u":...,"seq":...}}
# Delta: same but type="delta"

@dataclass
class BybitL2WsClient(LocalL2WsClient):
    """Bybit V5 linear orderbook depth WebSocket client."""

    def websocket_url(self) -> str:
        return bybit_depth_stream_url()

    def build_subscribe_message(self) -> Optional[dict]:
        return {
            "op": "subscribe",
            "args": [f"orderbook.1.{self.symbol}"],
        }

    def parse_depth_message(self, raw: dict) -> Optional[LocalL2Update]:
        topic = raw.get("topic", "")
        if not topic.startswith("orderbook"):
            return None

        msg_type = raw.get("type", "")
        data = raw.get("data", {})
        now_ms = int(time.time() * 1000)

        kind = LocalL2UpdateKind.SNAPSHOT if msg_type == "snapshot" else LocalL2UpdateKind.DELTA

        return LocalL2Update(
            venue=self.venue,
            symbol=self.symbol,
            bids=[PriceLevel(price=float(p), quantity=float(q)) for p, q in data.get("b", [])],
            asks=[PriceLevel(price=float(p), quantity=float(q)) for p, q in data.get("a", [])],
            sequence=int(data.get("u", data.get("seq", 0))),
            event_time_ms=int(raw.get("ts", 0)),
            received_at_ms=now_ms,
            update_kind=kind,
        )


# ---------------------------------------------------------------------------
# Bitget V2 WS client
# ---------------------------------------------------------------------------

# Bitget depth channel:
# Subscribe: {"op":"subscribe","args":[{"instType":"USDT-FUTURES","channel":"books","instId":"BTCUSDT"}]}
# Snapshot:  {"action":"snapshot","arg":{...},"data":[{"asks":[[p,q]],"bids":[[p,q]],"ts":"...","checksum":...}]}
# Delta:     {"action":"update","arg":{...},"data":[{"asks":[[p,q]],"bids":[[p,q]],"ts":"...","checksum":...}]}

@dataclass
class BitgetL2WsClient(LocalL2WsClient):
    """Bitget V2 USDT-FUTURES depth WebSocket client."""

    def websocket_url(self) -> str:
        return bitget_depth_stream_url()

    def build_subscribe_message(self) -> Optional[dict]:
        return {
            "op": "subscribe",
            "args": [{
                "instType": "USDT-FUTURES",
                "channel": "books",
                "instId": self.symbol,
            }],
        }

    def parse_depth_message(self, raw: dict) -> Optional[LocalL2Update]:
        action = raw.get("action", "")
        if action not in ("snapshot", "update"):
            return None
        arg = raw.get("arg", {})
        if arg.get("channel") != "books":
            return None

        data_list = raw.get("data", [])
        if not data_list:
            return None

        row = data_list[0]
        now_ms = int(time.time() * 1000)

        kind = LocalL2UpdateKind.SNAPSHOT if action == "snapshot" else LocalL2UpdateKind.DELTA

        return LocalL2Update(
            venue=self.venue,
            symbol=self.symbol,
            bids=[PriceLevel(price=float(p), quantity=float(q)) for p, q in row.get("bids", [])],
            asks=[PriceLevel(price=float(p), quantity=float(q)) for p, q in row.get("asks", [])],
            sequence=int(row.get("checksum", 0)),
            event_time_ms=int(row.get("ts", 0)),
            received_at_ms=now_ms,
            update_kind=kind,
        )


# ---------------------------------------------------------------------------
# Gate.io V4 WS client
# ---------------------------------------------------------------------------

# Gate order_book channel:
# Subscribe: {"time":1715000000,"channel":"futures.order_book","event":"subscribe","payload":["BTC_USDT","20","100ms"]}
# Snapshot:  {"time":...,"channel":"futures.order_book","event":"all","result":{"t":...,"id":...,"contract":"BTC_USDT","asks":[[p,q]],"bids":[[p,q]]}}
# Delta:     {"time":...,"channel":"futures.order_book","event":"update","result":{...}}

@dataclass
class GateL2WsClient(LocalL2WsClient):
    """Gate.io futures USDT order book WebSocket client."""

    def gate_symbol(self) -> str:
        """Convert internal symbol to Gate WS format: BTCUSDT → BTC_USDT.

        Deprecated: prefer self.wire_symbol which uses the venue spec's symbol_to_venue.
        """
        return self.wire_symbol

    def websocket_url(self) -> str:
        return gate_depth_stream_url()

    def build_subscribe_message(self) -> Optional[dict]:
        # Subscribe with venue wire symbol (e.g. BTC_USDT) to match Gate API
        return {
            "time": int(time.time()),
            "channel": "futures.order_book",
            "event": "subscribe",
            "payload": [self.wire_symbol, "20", "100ms"],
        }

    def parse_depth_message(self, raw: dict) -> Optional[LocalL2Update]:
        if raw.get("channel") != "futures.order_book":
            return None

        event = raw.get("event", "")
        if event not in ("all", "update"):
            return None

        result = raw.get("result", {})
        if not result:
            return None

        now_ms = int(time.time() * 1000)
        kind = LocalL2UpdateKind.SNAPSHOT if event == "all" else LocalL2UpdateKind.DELTA

        return LocalL2Update(
            venue=self.venue,
            symbol=self.symbol,
            bids=[PriceLevel(price=float(p), quantity=float(q)) for p, q in result.get("bids", [])],
            asks=[PriceLevel(price=float(p), quantity=float(q)) for p, q in result.get("asks", [])],
            sequence=int(result.get("id", result.get("t", 0))),
            event_time_ms=int(raw.get("time", 0)) * 1000,  # Gate uses seconds
            received_at_ms=now_ms,
            update_kind=kind,
        )


# ---------------------------------------------------------------------------
# Aster (Binance-compatible) WS client
# ---------------------------------------------------------------------------

# Aster uses Binance-compatible depthUpdate messages:
# {"e":"depthUpdate","E":...,"s":"BTCUSDT","U":...,"u":...,"b":[[p,q],...],"a":[[p,q],...]}

@dataclass
class AsterL2WsClient(LocalL2WsClient):
    """Aster perpetuals depth delta WebSocket client (Binance-compatible)."""

    def websocket_url(self) -> str:
        return aster_depth_stream_url(self.symbol)

    def build_subscribe_message(self) -> Optional[dict]:
        return None

    def parse_depth_message(self, raw: dict) -> Optional[LocalL2Update]:
        if raw.get("e") != "depthUpdate":
            return None

        return LocalL2Update(
            venue=self.venue,
            symbol=self.symbol,
            bids=[PriceLevel(price=float(p), quantity=float(q)) for p, q in raw.get("b", [])],
            asks=[PriceLevel(price=float(p), quantity=float(q)) for p, q in raw.get("a", [])],
            sequence=int(raw.get("u", 0)),
            previous_sequence=int(raw.get("U", 0)) - 1 if raw.get("U") else 0,
            event_time_ms=int(raw.get("E", 0)),
            received_at_ms=int(time.time() * 1000),
            update_kind=LocalL2UpdateKind.DELTA,
        )


# ---------------------------------------------------------------------------
# Hyperliquid L2 poller (REST-based — no public WS depth stream)
# ---------------------------------------------------------------------------

# Hyperliquid has no public WebSocket depth endpoint. L2 data comes from
# POST /info {"type": "l2Book"} → REST periodic polling is the canonical path.
# This poller wraps REST into the WS client interface so all 7 venues
# have a uniform ingest path through LocalL2DataPlane.ingest_external_update().

_HYPERLIQUID_L2_POLL_INTERVAL_S = 1.0  # 1s poll interval (max HL rate limit: ~10/s)


@dataclass
class HyperliquidL2Poller(LocalL2WsClient):
    """Hyperliquid L2 REST poller — wraps periodic /info polling in WS client interface.

    Hyperliquid has no WebSocket depth stream. REST POST /info {"type":"l2Book"}
    is the only L2 data source. This poller uses the data plane's adapter reference
    to call fetch_l2_snapshot() periodically, feeding results into ingest_external_update().
    """

    poll_interval_s: float = _HYPERLIQUID_L2_POLL_INTERVAL_S
    _adapter: Any = field(default=None, init=False)

    def set_adapter(self, adapter) -> None:
        self._adapter = adapter

    def websocket_url(self) -> str:
        return ""  # No WS — REST only

    def build_subscribe_message(self) -> Optional[dict]:
        return None

    def parse_depth_message(self, raw: dict) -> Optional[LocalL2Update]:
        return None  # Not used — _run_loop is overridden

    async def _run_loop(self) -> None:
        """REST polling loop instead of WS read loop.

        Polls adapter.fetch_l2_snapshot() periodically and feeds results
        into the data plane.  If no adapter is set, logs an error and
        increments the error counter — the poller must be injected with
        a real adapter to ingest data.
        """
        adapter_none_logged = False
        while self._state not in (WsClientState.CLOSED,):
            self._state = WsClientState.CONNECTED
            try:
                if self._adapter is not None:
                    update = await self._adapter.fetch_l2_snapshot(
                        symbol=self.symbol, depth=50,
                    )
                    self._message_count += 1
                    self._last_message_ms = int(time.time() * 1000)
                    self.data_plane.ingest_external_update(
                        update, self._last_message_ms,
                    )
                else:
                    if not adapter_none_logged:
                        self._error_count += 1
                        adapter_none_logged = True
            except asyncio.CancelledError:
                break
            except Exception:
                self._error_count += 1

            await asyncio.sleep(self.poll_interval_s)


# ---------------------------------------------------------------------------
# WS client factory
# ---------------------------------------------------------------------------


WS_CLIENT_REGISTRY: dict[str, type[LocalL2WsClient]] = {
    "binance": BinanceL2WsClient,
    "okx": OkxL2WsClient,
    "bybit": BybitL2WsClient,
    "bitget": BitgetL2WsClient,
    "gate": GateL2WsClient,
    "aster": AsterL2WsClient,
    "hyperliquid": HyperliquidL2Poller,
}


def create_ws_client(
    venue: str,
    symbol: str,
    data_plane: "LocalL2DataPlane",
) -> Optional[LocalL2WsClient]:
    """Create a WebSocket L2 client for the given venue.

    symbol is the canonical internal symbol (e.g. BTCUSDT).
    The venue wire symbol is resolved from the venue spec (e.g. BTC-USDT-SWAP for OKX).

    Returns None if the venue has no WS client implementation yet.
    """
    cls = WS_CLIENT_REGISTRY.get(venue)
    if cls is None:
        return None

    # Resolve venue wire symbol from the venue spec
    venue_sym = symbol
    try:
        from lightfee.venues.specs import get_spec
        from lightfee.core.domain import Venue
        ven = Venue.from_str(venue)
        spec = get_spec(ven)
        if spec.symbol_to_venue is not None:
            venue_sym = spec.symbol_to_venue(symbol)
    except (ValueError, KeyError):
        pass

    return cls(venue=venue, symbol=symbol, venue_symbol=venue_sym, data_plane=data_plane)
