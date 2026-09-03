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


def hyperliquid_depth_stream_url() -> str:
    """Hyperliquid public WebSocket."""
    return "wss://api.hyperliquid.xyz/ws"


def aster_depth_stream_url(symbol: str) -> str:
    """Aster (Binance-compatible) per-symbol depth delta stream."""
    return f"wss://fstream.asterdex.com/ws/{symbol.lower()}@depth@100ms"


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
    _connect_attempt_count: int = field(default=0, init=False)
    _last_connected_ms: int = field(default=0, init=False)
    _last_disconnected_ms: int = field(default=0, init=False)
    _last_disconnect_reason: str = field(default="", init=False)
    _subscription_mode: str = field(default="unknown", init=False)
    _last_update_kind: str = field(default="", init=False)
    _last_raw_U: int = field(default=0, init=False)
    _last_raw_u: int = field(default=0, init=False)
    _last_raw_pu: int = field(default=0, init=False)
    _last_update_event_time_ms: int = field(default=0, init=False)

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
        self._record_transport_event("stopped", int(time.time() * 1000))

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

    def diagnostics_snapshot(self) -> dict[str, object]:
        """Return the latest receipt and transport evidence for one WS stream."""
        return {
            "client_state": self._state,
            "message_count": self._message_count,
            "error_count": self._error_count,
            "connect_attempt_count": self._connect_attempt_count,
            "last_message_ms": self._last_message_ms,
            "last_connected_ms": self._last_connected_ms,
            "last_disconnected_ms": self._last_disconnected_ms,
            "last_disconnect_reason": self._last_disconnect_reason,
            "last_error": self._last_error,
            "reconnect_delay_ms": int(self._reconnect_delay_s * 1000),
            "subscription_mode": self._subscription_mode,
            "last_update_kind": self._last_update_kind,
            "last_raw_U": self._last_raw_U,
            "last_raw_u": self._last_raw_u,
            "last_raw_pu": self._last_raw_pu,
            "last_update_event_time_ms": self._last_update_event_time_ms,
        }

    @staticmethod
    def _error_text(error: Exception) -> str:
        return f"{type(error).__name__}: {error}"[:500]

    def _record_transport_event(
        self,
        event: str,
        now_ms: int,
        **extra: object,
    ) -> None:
        try:
            self.data_plane.record_ws_transport_event(
                self.venue,
                self.symbol,
                event,
                now_ms=now_ms,
                **extra,
            )
        except Exception:
            # Diagnostics must not break the market-data stream they observe.
            pass

    def _record_error(self, error: Exception) -> str:
        self._error_count += 1
        self._last_error = self._error_text(error)
        return self._last_error

    def _remember_update(self, update: LocalL2Update) -> None:
        self._last_update_kind = update.update_kind.value
        self._last_raw_U = int(update.first_sequence or 0)
        self._last_raw_u = int(update.sequence or 0)
        self._last_raw_pu = int(update.previous_sequence or 0)
        self._last_update_event_time_ms = int(update.event_time_ms or 0)

    def request_reconnect(self, reason: str) -> bool:
        """Close this symbol's live stream so the existing loop reconnects it."""
        if not self.is_connected:
            return False
        now_ms = int(time.time() * 1000)
        self._last_disconnect_reason = str(reason or "rebuild_required")
        self._record_transport_event(
            "reconnect_requested",
            now_ms,
            reason=self._last_disconnect_reason,
        )
        asyncio.create_task(self._close_ws())
        return True

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

    def build_application_heartbeat(self) -> Optional[dict | str]:
        """Return an exchange-native heartbeat frame, if RFC control pings are insufficient."""
        return None

    def control_message_confirms_book(self) -> bool:
        """Whether subscription/pong control frames may refresh quote age."""
        return True

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
                if self._state != WsClientState.CLOSED:
                    now_ms = int(time.time() * 1000)
                    self._last_disconnected_ms = now_ms
                    if not self._last_disconnect_reason:
                        self._last_disconnect_reason = "read_loop_ended"
                    self._record_transport_event(
                        "read_loop_ended",
                        now_ms,
                        close_code=getattr(self._ws, "close_code", None),
                        close_reason=str(getattr(self._ws, "close_reason", "") or "")[:500],
                    )
            except asyncio.CancelledError:
                break
            except Exception as e:
                now_ms = int(time.time() * 1000)
                error = self._record_error(e)
                self._last_disconnected_ms = now_ms
                self._last_disconnect_reason = error
                self._record_transport_event(
                    "connect_error" if self._state == WsClientState.CONNECTING else "read_error",
                    now_ms,
                    error=error,
                )
            finally:
                self.data_plane.note_ws_stream_unready(self.venue, self.symbol)

            if self._state == WsClientState.CLOSED:
                break

            # Reconnect backoff
            self._state = WsClientState.RECONNECTING
            delay = self._reconnect_delay_s or self.reconnect_delay_initial_s
            self._record_transport_event(
                "reconnect_scheduled",
                int(time.time() * 1000),
                reconnect_delay_ms=int(delay * 1000),
            )
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
        application_heartbeat = self.build_application_heartbeat()
        self._state = WsClientState.CONNECTING
        self._connect_attempt_count += 1
        self._record_transport_event(
            "connect_attempt",
            int(time.time() * 1000),
            connect_attempt=self._connect_attempt_count,
        )

        async with websockets.connect(
            url,
            ping_interval=None if application_heartbeat is not None else self.ping_interval_s,
            open_timeout=self.OPEN_TIMEOUT_SECONDS,
            close_timeout=5,
            max_size=2**20,  # 1MB
        ) as ws:
            self._ws = ws
            self._state = WsClientState.CONNECTED
            self._reconnect_delay_s = 0  # reset on successful connect
            self._last_disconnect_reason = ""
            self._last_connected_ms = int(time.time() * 1000)

            # Subscribe to depth channel (skip if venue auto-subscribes)
            sub_msg = self.build_subscribe_message()
            if sub_msg is not None:
                await ws.send(json.dumps(sub_msg))
                self._subscription_mode = "explicit"
            else:
                self._subscription_mode = "url_auto"

            # Notify data plane: new WS stream → advance generation, clear old buffers
            # V1: on_connect closure calling reset_binance_local_l2_bootstrap_stream_state_for_instance
            self.data_plane.reset_stream_state(self.venue, [self.symbol])
            self._record_transport_event("connected", self._last_connected_ms)
            self._record_transport_event(
                "subscription_sent" if sub_msg is not None else "url_auto_subscribed",
                int(time.time() * 1000),
            )
            if sub_msg is None:
                self.data_plane.note_ws_stream_ready(self.venue, self.symbol)

            heartbeat_task: asyncio.Task | None = None
            if application_heartbeat is not None:
                heartbeat_task = asyncio.create_task(
                    self._send_application_heartbeats(ws, application_heartbeat),
                )
            try:
                async for raw_msg in ws:
                    await self._handle_message(raw_msg)
            finally:
                if heartbeat_task is not None:
                    heartbeat_task.cancel()
                    try:
                        await heartbeat_task
                    except asyncio.CancelledError:
                        pass

    async def _handle_message(self, raw_msg: str | bytes) -> None:
        """Parse a single WS message and ingest into the data plane."""
        try:
            if isinstance(raw_msg, bytes):
                raw_msg = raw_msg.decode("utf-8")
            payload = json.loads(raw_msg)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            error_text = self._record_error(error)
            self._record_transport_event(
                "decode_error",
                int(time.time() * 1000),
                error=error_text,
                raw_size=len(raw_msg),
            )
            return

        now_ms = int(time.time() * 1000)
        if self._is_subscription_failure(payload):
            self._record_transport_event(
                "subscription_rejected",
                now_ms,
                error=str(payload.get("error", "") or payload.get("ret_msg", ""))[:500],
                code=next(
                    (
                        payload.get(field)
                        for field in ("code", "retCode", "sCode")
                        if payload.get(field) not in (None, "", 0, "0")
                    ),
                    None,
                ),
            )
            return
        if self._is_subscription_confirmation(payload):
            self.data_plane.note_ws_subscription_confirmed(
                self.venue,
                self.symbol,
                now_ms=now_ms,
                refresh_book=self.control_message_confirms_book(),
            )
            self._record_transport_event("subscription_confirmed", now_ms)
            return
        if self._is_keepalive_message(payload):
            self.data_plane.note_ws_keepalive(
                self.venue,
                self.symbol,
                now_ms=now_ms,
                refresh_book=self.control_message_confirms_book(),
            )
            return

        # Venue-specific parsing
        try:
            update = self.parse_depth_message(payload)
        except Exception as error:
            error_text = self._record_error(error)
            self._record_transport_event("parse_error", now_ms, error=error_text)
            raise
        if update is None:
            return

        self._message_count += 1
        self._last_message_ms = now_ms
        self._remember_update(update)
        self.data_plane.note_ws_stream_ready(self.venue, self.symbol)

        try:
            self.data_plane.ingest_external_update(update, now_ms)
        except Exception as error:
            error_text = self._record_error(error)
            self._record_transport_event("ingest_error", now_ms, error=error_text)

    def _is_subscription_confirmation(self, payload: dict) -> bool:
        event = str(payload.get("event", "")).lower()
        op = str(payload.get("op", "")).lower()
        ret_msg = str(payload.get("ret_msg", "")).lower()
        channel = str(payload.get("channel", "")).lower()

        if self._is_subscription_failure(payload):
            return False
        if event == "subscribe":
            return True
        if op == "subscribe" and payload.get("success") is True:
            return True
        if "subscribe" in ret_msg and payload.get("success") is True:
            return True
        if channel in {"subscriptionresponse", "subscription_response"}:
            return True
        return False

    async def _send_application_heartbeats(self, ws: Any, message: dict | str) -> None:
        try:
            while True:
                await asyncio.sleep(self.ping_interval_s)
                await ws.send(message if isinstance(message, str) else json.dumps(message))
                self._record_transport_event(
                    "application_ping_sent",
                    int(time.time() * 1000),
                )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            error_text = self._record_error(error)
            self._record_transport_event(
                "application_ping_error",
                int(time.time() * 1000),
                error=error_text,
            )

    @staticmethod
    def _is_subscription_failure(payload: dict) -> bool:
        if payload.get("success") is False:
            return True
        if payload.get("error") is not None:
            return True

        for field in ("code", "retCode", "sCode"):
            value = payload.get(field)
            if value in (None, "", 0, "0"):
                continue
            return True

        data = payload.get("data")
        if isinstance(data, list):
            for row in data:
                if not isinstance(row, dict):
                    continue
                value = row.get("sCode")
                if value not in (None, "", 0, "0"):
                    return True
        return False

    def _is_keepalive_message(self, payload: dict) -> bool:
        event = str(payload.get("event", "")).lower()
        op = str(payload.get("op", "")).lower()
        channel = str(payload.get("channel", "")).lower()
        ret_msg = str(payload.get("ret_msg", "")).lower()

        if event in {"ping", "pong"} or op in {"ping", "pong"}:
            return True
        if channel in {"pong", "heartbeat"}:
            return True
        if "pong" in ret_msg or "heartbeat" in ret_msg:
            return True
        if "ping" in payload or "pong" in payload:
            return True
        return False

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

        first_sequence = int(raw.get("U", 0) or 0)
        previous_sequence_present = raw.get("pu") is not None
        return LocalL2Update(
            venue=self.venue,
            symbol=self.symbol,
            bids=[PriceLevel(price=float(p), quantity=float(q)) for p, q in raw.get("b", [])],
            asks=[PriceLevel(price=float(p), quantity=float(q)) for p, q in raw.get("a", [])],
            first_sequence=first_sequence,
            sequence=int(raw.get("u", 0)),
            previous_sequence=(
                int(raw.get("pu", 0))
                if previous_sequence_present
                else 0
            ),
            previous_sequence_present=previous_sequence_present,
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

        bids_raw = row.get("bids", [])
        asks_raw = row.get("asks", [])
        return LocalL2Update(
            venue=self.venue,
            symbol=self.symbol,
            bids=[PriceLevel(price=float(p), quantity=float(q)) for p, q, *_ in bids_raw],
            asks=[PriceLevel(price=float(p), quantity=float(q)) for p, q, *_ in asks_raw],
            sequence=int(row.get("seqId", 0)),
            previous_sequence=int(row.get("prevSeqId", raw.get("prevSeqId", 0))),
            previous_sequence_present=("prevSeqId" in row or "prevSeqId" in raw),
            checksum=int(row.get("checksum", -1)),
            raw_bids=[(str(p), str(q)) for p, q, *_ in bids_raw],
            raw_asks=[(str(p), str(q)) for p, q, *_ in asks_raw],
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
            "args": [f"orderbook.50.{self.symbol}"],
        }

    def build_application_heartbeat(self) -> Optional[dict]:
        # Bybit V5 public WS specifies an application JSON ping every 20 seconds.
        return {"op": "ping"}

    def control_message_confirms_book(self) -> bool:
        # Pong/subscribe success proves transport liveness, not an executable book.
        return False

    def parse_depth_message(self, raw: dict) -> Optional[LocalL2Update]:
        topic = raw.get("topic", "")
        if not topic.startswith("orderbook"):
            return None

        msg_type = raw.get("type", "")
        data = raw.get("data", {})
        now_ms = int(time.time() * 1000)

        kind = LocalL2UpdateKind.SNAPSHOT if msg_type == "snapshot" else LocalL2UpdateKind.DELTA

        previous_sequence_present = data.get("pu") is not None
        return LocalL2Update(
            venue=self.venue,
            symbol=self.symbol,
            bids=[PriceLevel(price=float(p), quantity=float(q)) for p, q in data.get("b", [])],
            asks=[PriceLevel(price=float(p), quantity=float(q)) for p, q in data.get("a", [])],
            sequence=int(data.get("u", data.get("seq", 0))),
            previous_sequence=int(data.get("pu", 0)) if previous_sequence_present else 0,
            previous_sequence_present=previous_sequence_present,
            event_time_ms=int(raw.get("ts", 0)),
            received_at_ms=now_ms,
            update_kind=kind,
        )


# ---------------------------------------------------------------------------
# Bitget V2 WS client
# ---------------------------------------------------------------------------

# Bitget depth channel:
# Subscribe: {"op":"subscribe","args":[{"instType":"USDT-FUTURES","channel":"books","instId":"BTCUSDT"}]}
# Snapshot:  {"action":"snapshot","arg":{...},"data":[{"asks":[[p,q]],"bids":[[p,q]],"seq":123,"pseq":0,"checksum":...}]}
# Delta:     {"action":"update","arg":{...},"data":[{"asks":[[p,q]],"bids":[[p,q]],"seq":124,"pseq":123,"checksum":...}]}

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

    def build_application_heartbeat(self) -> str:
        # Bitget V2 requires the literal text frame "ping", not an RFC ping
        # and not a JSON payload, or it closes an idle connection after 2 min.
        return "ping"

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

        bids_raw = row.get("bids", row.get("b", []))
        asks_raw = row.get("asks", row.get("a", []))
        sequence = int(row.get("seq", row.get("seqId", 0)) or 0)
        previous_sequence = int(row.get("pseq", row.get("prevSeqId", 0)) or 0)
        previous_sequence_present = "pseq" in row or "prevSeqId" in row

        return LocalL2Update(
            venue=self.venue,
            symbol=self.symbol,
            bids=[PriceLevel(price=float(p), quantity=float(q)) for p, q in bids_raw],
            asks=[PriceLevel(price=float(p), quantity=float(q)) for p, q in asks_raw],
            sequence=sequence,
            previous_sequence=previous_sequence,
            previous_sequence_present=previous_sequence_present,
            checksum=int(row.get("checksum", 0) or 0),
            raw_bids=[(str(p), str(q)) for p, q in bids_raw],
            raw_asks=[(str(p), str(q)) for p, q in asks_raw],
            event_time_ms=int(row.get("ts", 0)),
            received_at_ms=now_ms,
            update_kind=kind,
        )


# ---------------------------------------------------------------------------
# Gate.io V4 WS client
# ---------------------------------------------------------------------------

# Gate V1 local-L2 channel:
# Subscribe: {"time":1715000000,"channel":"futures.obu","event":"subscribe","payload":["ob.BTC_USDT.400"]}
# Snapshot/update payloads may arrive on futures.obu or futures.order_book_update
# with U/u sequence ranges.

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
        return {
            "time": int(time.time()),
            "channel": "futures.obu",
            "event": "subscribe",
            "payload": [f"ob.{self.wire_symbol}.400"],
        }

    def parse_depth_message(self, raw: dict) -> Optional[LocalL2Update]:
        channel = raw.get("channel")
        if channel not in ("futures.order_book", "futures.order_book_update", "futures.obu"):
            return None

        event = raw.get("event", "")
        if channel == "futures.order_book" and event not in ("all", "update"):
            return None

        result = raw.get("result") or raw.get("data") or {}
        if not result:
            return None

        now_ms = int(time.time() * 1000)
        is_full = bool(result.get("full")) or event == "all"
        kind = LocalL2UpdateKind.SNAPSHOT if is_full else LocalL2UpdateKind.DELTA
        bids_raw = result.get("bids", result.get("b", []))
        asks_raw = result.get("asks", result.get("a", []))
        sequence = int(result.get("u", result.get("id", result.get("t", 0))) or 0)
        first_sequence = int(result.get("U", result.get("prev_t", 0)) or 0)

        def _price_size(row):
            if isinstance(row, dict):
                return str(row.get("p", 0)), str(row.get("s", 0))
            return str(row[0]), str(row[1])

        return LocalL2Update(
            venue=self.venue,
            symbol=self.symbol,
            bids=[PriceLevel(price=float(p), quantity=float(q)) for p, q in map(_price_size, bids_raw)],
            asks=[PriceLevel(price=float(p), quantity=float(q)) for p, q in map(_price_size, asks_raw)],
            first_sequence=first_sequence,
            sequence=sequence,
            # Gate's U..u identifies the inclusive update range.  It is not
            # a previous-link (pu), so feeding U into the generic pu check
            # makes every valid overlapping batch look discontinuous.
            previous_sequence=0,
            previous_sequence_present=False,
            event_time_ms=int(result.get("t", int(raw.get("time", 0)) * 1000) or 0),
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

        first_sequence = int(raw.get("U", 0) or 0)
        previous_sequence_present = raw.get("pu") is not None
        return LocalL2Update(
            venue=self.venue,
            symbol=self.symbol,
            bids=[PriceLevel(price=float(p), quantity=float(q)) for p, q in raw.get("b", [])],
            asks=[PriceLevel(price=float(p), quantity=float(q)) for p, q in raw.get("a", [])],
            first_sequence=first_sequence,
            sequence=int(raw.get("u", 0)),
            previous_sequence=(
                int(raw.get("pu", 0))
                if previous_sequence_present
                else 0
            ),
            previous_sequence_present=previous_sequence_present,
            event_time_ms=int(raw.get("E", 0)),
            received_at_ms=int(time.time() * 1000),
            update_kind=LocalL2UpdateKind.DELTA,
        )


# ---------------------------------------------------------------------------
# Hyperliquid public L2Book WS client
# ---------------------------------------------------------------------------

@dataclass
class HyperliquidL2WsClient(LocalL2WsClient):
    """Hyperliquid l2Book WebSocket client (V1 StreamOnly / HeartbeatAge)."""

    def set_adapter(self, adapter) -> None:
        # Backward-compatible no-op: V1 local-L2 is stream-only.
        return None

    def websocket_url(self) -> str:
        return hyperliquid_depth_stream_url()

    def build_subscribe_message(self) -> Optional[dict]:
        return {
            "method": "subscribe",
            "subscription": {"type": "l2Book", "coin": self.wire_symbol},
        }

    def parse_depth_message(self, raw: dict) -> Optional[LocalL2Update]:
        if raw.get("channel") != "l2Book":
            return None
        data = raw.get("data") or {}
        levels = data.get("levels") or []
        bids_raw = levels[0] if len(levels) > 0 else []
        asks_raw = levels[1] if len(levels) > 1 else []

        def _levels(rows):
            result = []
            for row in rows:
                price = float(row.get("px", 0))
                quantity = float(row.get("sz", 0))
                if price > 0 and quantity > 0:
                    result.append(PriceLevel(price=price, quantity=quantity))
            return result

        return LocalL2Update(
            venue=self.venue,
            symbol=self.symbol,
            bids=_levels(bids_raw),
            asks=_levels(asks_raw),
            sequence=0,
            event_time_ms=int(data.get("time", 0) or 0),
            received_at_ms=int(time.time() * 1000),
            update_kind=LocalL2UpdateKind.SNAPSHOT,
        )


HyperliquidL2Poller = HyperliquidL2WsClient


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
    "hyperliquid": HyperliquidL2WsClient,
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
