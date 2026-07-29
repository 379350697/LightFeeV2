"""V1 private WS full parity tests: real event parser fixtures + worker lifecycle.

Covers: Binance TRADE_LITE/ORDER_TRADE_UPDATE/ACCOUNT_UPDATE, Aster equivalents,
OKX ctVal conversion, Bybit positionIdx/execution, Bitget orders/positions,
Gate futures.orders/futures.positions, Hyperliquid hydrate/userEvents/orderUpdates/NoData.

Worker lifecycle tests exercise the production worker methods with fake websocket
transports, verifying record_private_ws_success/failure on real paths.
"""

from __future__ import annotations

import asyncio
import json
import time
from unittest.mock import AsyncMock, Mock, patch
from typing import Optional

import pytest

from lightfee.core.domain import PassiveOrderState, Side, Venue
from lightfee.marketdata.private_ws import (
    PrivateOrderUpdate,
    PrivateWsState,
    _now_ms,
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


async def _sleep_short():
    await asyncio.sleep(0.02)


async def _wait_until(predicate, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    assert predicate()


def _okx_subscribe_request_ids(sent: list[str], inst_id: str) -> dict[str, str]:
    request_ids: dict[str, str] = {}
    for raw in sent:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if payload.get("op") != "subscribe":
            continue
        request_id = str(payload.get("id") or "")
        for arg in payload.get("args") or []:
            if not isinstance(arg, dict):
                continue
            if arg.get("instId") == inst_id and request_id:
                request_ids[str(arg.get("channel") or "")] = request_id
    return request_ids


# ============================================================================
# V1 real-event parser fixtures
# ============================================================================


class TestBinanceV1FuturesEvents:
    """Binance V1 futures user-data stream events (not spot-style)."""

    @pytest.mark.asyncio
    async def test_trade_lite_partial_fill(self):
        """V1 TRADE_LITE: per-trade fill notification (futures)."""
        from lightfee.venues.binance_private_ws import handle_binance_private_message

        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({
            "e": "TRADE_LITE",
            "s": "ETHUSDT",
            "i": 123456789,
            "c": "binance-client-1",
            "l": "0.01",   # last filled qty
            "L": "2140.00",  # last filled price
            "T": 1700000000000,
            "E": 1700000000000,
        })
        handle_binance_private_message(state, symbol_map, raw)
        await _sleep_short()
        update = state.order_by_client_id("binance-client-1")
        assert update is not None
        assert update.filled_quantity == 0.01
        assert update.average_price == 2140.0
        assert update.updated_at_ms == 1700000000000

    @pytest.mark.asyncio
    async def test_order_trade_update_full_fill(self):
        """V1 ORDER_TRADE_UPDATE: order lifecycle with cumulative fill."""
        from lightfee.venues.binance_private_ws import handle_binance_private_message

        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({
            "e": "ORDER_TRADE_UPDATE",
            "E": 1700000001000,
            "o": {
                "s": "ETHUSDT",
                "i": 987654321,
                "c": "binance-client-2",
                "z": "0.05",    # cumulative filled qty
                "ap": "2141.00",  # average price
                "n": "0.001",     # commission amount
                "N": "USDT",      # commission asset
                "T": 1700000001000,
            },
        })
        handle_binance_private_message(state, symbol_map, raw)
        await _sleep_short()
        update = state.order_by_order_id("987654321")
        assert update is not None
        assert update.filled_quantity == 0.05
        assert update.average_price == 2141.0
        assert update.fee_quote == 0.001
        assert update.updated_at_ms == 1700000001000

    @pytest.mark.asyncio
    async def test_account_update_position(self):
        """V1 ACCOUNT_UPDATE: position push from futures account data."""
        from lightfee.venues.binance_private_ws import handle_binance_private_message

        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({
            "e": "ACCOUNT_UPDATE",
            "E": 1700000002000,
            "a": {
                "P": [
                    {"s": "ETHUSDT", "pa": "0.5", "ps": "LONG"},
                ],
            },
        })
        handle_binance_private_message(state, symbol_map, raw)
        await _sleep_short()
        pos = state.position("ETHUSDT")
        assert pos is not None
        assert pos.size == 0.5
        assert pos.updated_at_ms == 1700000002000

    @pytest.mark.asyncio
    async def test_trade_lite_missing_symbol_ignored(self):
        """TRADE_LITE with unknown symbol is silently ignored."""
        from lightfee.venues.binance_private_ws import handle_binance_private_message

        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}  # only ETHUSDT
        raw = json.dumps({
            "e": "TRADE_LITE",
            "s": "BTCUSDT",  # not in symbol_map
            "i": 111,
            "l": "0.1",
            "L": "60000",
            "T": 1700000000000,
        })
        handle_binance_private_message(state, symbol_map, raw)
        await _sleep_short()
        assert state.order_by_order_id("111") is None


class TestAsterV1FuturesEvents:
    """Aster V1 futures user-data stream events."""

    @pytest.mark.asyncio
    async def test_trade_lite(self):
        from lightfee.venues.aster_private_ws import handle_aster_private_message

        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({
            "e": "TRADE_LITE",
            "s": "ETHUSDT",
            "i": 111222,
            "c": "aster-client-1",
            "l": "0.02",
            "L": "2142.00",
            "T": 1700000000000,
        })
        handle_aster_private_message(state, symbol_map, raw)
        await _sleep_short()
        update = state.order_by_client_id("aster-client-1")
        assert update is not None
        assert update.filled_quantity == 0.02

    @pytest.mark.asyncio
    async def test_order_trade_update(self):
        from lightfee.venues.aster_private_ws import handle_aster_private_message

        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({
            "e": "ORDER_TRADE_UPDATE",
            "E": 1700000001000,
            "o": {
                "s": "ETHUSDT",
                "i": 333444,
                "c": "aster-client-2",
                "z": "0.03",
                "ap": "2143.00",
                "n": "0.002",
                "N": "USDT",
                "T": 1700000001000,
            },
        })
        handle_aster_private_message(state, symbol_map, raw)
        await _sleep_short()
        update = state.order_by_order_id("333444")
        assert update is not None
        assert update.filled_quantity == 0.03
        assert update.average_price == 2143.0
        assert update.fee_quote == 0.002

    @pytest.mark.asyncio
    async def test_account_update(self):
        from lightfee.venues.aster_private_ws import handle_aster_private_message

        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({
            "e": "ACCOUNT_UPDATE",
            "E": 1700000002000,
            "a": {
                "P": [
                    {"s": "ETHUSDT", "pa": "1.0", "ps": "LONG"},
                    {"s": "ETHUSDT", "pa": "0.5", "ps": "SHORT"},
                ],
            },
        })
        handle_aster_private_message(state, symbol_map, raw)
        await _sleep_short()
        pos = state.position("ETHUSDT")
        assert pos is not None
        # V1: net position aggregation (1.0 LONG + (-0.5) SHORT = 0.5)
        assert pos.size == 0.5


class TestOkxCtValConversion:
    """OKX ctVal contracts→base quantity conversion in private WS parser."""

    @pytest.mark.asyncio
    async def test_order_with_ct_val(self):
        """accFillSz in contracts × ctVal → base quantity."""
        from lightfee.venues.okx_private_ws import handle_okx_private_message

        state = PrivateWsState()
        symbol_map = {"ETH-USDT-SWAP": "ETHUSDT"}
        # ETH ctVal = 0.1 (1 contract = 0.1 ETH)
        ct_val_map = {"ETH-USDT-SWAP": 0.1}
        raw = json.dumps({
            "arg": {"channel": "orders", "instType": "SWAP", "instId": "ETH-USDT-SWAP"},
            "data": [{
                "instId": "ETH-USDT-SWAP",
                "ordId": "okx-ct-1",
                "clOrdId": "okx-ct-client-1",
                "accFillSz": "5",      # 5 contracts
                "avgPx": "2140.00",
                "state": "PARTIALLY_FILLED",
                "fillFeeCcy": "USDT",
                "fillFee": "0.005",
                "fillTime": "1700000000000",
            }],
        })
        handle_okx_private_message(state, symbol_map, [], raw, subscribed=True, ct_val_map=ct_val_map)
        await _sleep_short()
        update = state.order_by_order_id("okx-ct-1")
        assert update is not None
        # 5 contracts × 0.1 ctVal = 0.5 base qty
        assert update.filled_quantity == 0.5

    @pytest.mark.asyncio
    async def test_position_with_ct_val(self):
        """pos in contracts × ctVal → base quantity."""
        from lightfee.venues.okx_private_ws import handle_okx_private_message

        state = PrivateWsState()
        symbol_map = {"ETH-USDT-SWAP": "ETHUSDT"}
        ct_val_map = {"ETH-USDT-SWAP": 0.1}
        raw = json.dumps({
            "arg": {"channel": "positions", "instType": "SWAP", "instId": "ETH-USDT-SWAP"},
            "data": [{
                "instId": "ETH-USDT-SWAP",
                "pos": "10",          # 10 contracts
                "posSide": "long",
                "uTime": "1700000000000",
            }],
        })
        handle_okx_private_message(state, symbol_map, [], raw, subscribed=True, ct_val_map=ct_val_map)
        await _sleep_short()
        pos = state.position("ETHUSDT")
        assert pos is not None
        # 10 contracts × 0.1 ctVal = 1.0 base qty
        assert pos.size == 1.0

    @pytest.mark.asyncio
    async def test_no_ct_val_defaults_to_one(self):
        """When ct_val_map is empty, OKX private order fill is skipped fail-closed."""
        from lightfee.venues.okx_private_ws import handle_okx_private_message

        state = PrivateWsState()
        symbol_map = {"ETH-USDT-SWAP": "ETHUSDT"}
        raw = json.dumps({
            "arg": {"channel": "orders", "instType": "SWAP", "instId": "ETH-USDT-SWAP"},
            "data": [{
                "instId": "ETH-USDT-SWAP",
                "ordId": "okx-no-ct",
                "accFillSz": "3",
                "avgPx": "2140.00",
                "state": "FILLED",
                "uTime": "1700000000000",
            }],
        })
        handle_okx_private_message(state, symbol_map, [], raw, subscribed=True)
        await _sleep_short()
        update = state.order_by_order_id("okx-no-ct")
        assert update is None


class TestBybitPrivateParserExtended:
    """Bybit: execution, position, and positionIdx events."""

    @pytest.mark.asyncio
    async def test_execution_event(self):
        from lightfee.venues.bybit_private_ws import handle_bybit_private_message

        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({
            "topic": "execution",
            "data": [{
                "symbol": "ETHUSDT",
                "orderId": "bybit-exec-1",
                "orderLinkId": "bybit-exec-client-1",
                "execQty": "0.02",
                "execPrice": "2140.00",
                "execFee": "0.001",
                "execTime": "1700000000000",
            }],
        })
        handle_bybit_private_message(state, symbol_map, raw, subscribed=True)
        await _sleep_short()
        update = state.order_by_order_id("bybit-exec-1")
        assert update is not None
        assert update.filled_quantity == 0.02

    @pytest.mark.asyncio
    async def test_position_event_hedge_mode(self):
        """V1 Bybit position update with positionIdx (hedge mode)."""
        from lightfee.venues.bybit_private_ws import handle_bybit_private_message

        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({
            "topic": "position",
            "data": [{
                "symbol": "ETHUSDT",
                "size": "0.5",
                "side": "Buy",  # long
                "positionIdx": "1",  # linear one-way: 0=both, 1=Buy(long), 2=Sell(short)
                "updatedTime": "1700000000000",
            }],
        })
        handle_bybit_private_message(state, symbol_map, raw, subscribed=True)
        await _sleep_short()
        pos = state.position("ETHUSDT")
        assert pos is not None
        assert pos.size > 0  # long

    @pytest.mark.asyncio
    async def test_position_event_hedge_mode_nets_same_symbol_rows(self):
        from lightfee.venues.bybit_private_ws import handle_bybit_private_message

        state = PrivateWsState()
        raw = json.dumps({
            "topic": "position",
            "data": [
                {
                    "symbol": "ETHUSDT",
                    "size": "1.0",
                    "side": "Buy",
                    "positionIdx": "1",
                    "updatedTime": "1700000000000",
                },
                {
                    "symbol": "ETHUSDT",
                    "size": "0.25",
                    "side": "Sell",
                    "positionIdx": "2",
                    "updatedTime": "1700000000001",
                },
            ],
        })

        handle_bybit_private_message(
            state,
            {"ETHUSDT": "ETHUSDT"},
            raw,
            subscribed=True,
        )
        await _sleep_short()

        pos = state.position("ETHUSDT")
        assert pos is not None
        assert pos.size == 0.75
        assert pos.updated_at_ms == 1700000000001


class TestBitgetPrivateParserExtended:
    """Bitget: orders and positions channel events."""

    @pytest.mark.asyncio
    async def test_positions_channel(self):
        from lightfee.venues.bitget_private_ws import handle_bitget_private_message

        state = PrivateWsState()
        symbol_map = {"ETHUSDT_UMCBL": "ETHUSDT"}
        raw = json.dumps({
            "arg": {"channel": "positions", "instId": "ETHUSDT_UMCBL"},
            "data": [{
                "instId": "ETHUSDT_UMCBL",
                "total": "0.3",
                "posSide": "long",
                "uTime": "1700000000000",
            }],
        })
        handle_bitget_private_message(state, symbol_map, raw, subscribed=True)
        await _sleep_short()
        pos = state.position("ETHUSDT")
        assert pos is not None
        assert pos.size == 0.3


class TestGatePrivateParserExtended:
    """Gate: futures.orders and futures.positions channel events."""

    @pytest.mark.asyncio
    async def test_futures_positions_event(self):
        from lightfee.venues.gate_private_ws import handle_gate_private_message

        state = PrivateWsState()
        symbol_map = {"ETH_USDT": "ETHUSDT"}
        raw = json.dumps({
            "channel": "futures.positions",
            "event": "update",
            "result": [{
                "contract": "ETH_USDT",
                "size": "5",
                "update_time_ms": 1700000000000,
            }],
        })
        handle_gate_private_message(state, symbol_map, raw)
        await _sleep_short()
        pos = state.position("ETHUSDT")
        assert pos is not None
        assert pos.size == 5.0

    @pytest.mark.asyncio
    async def test_futures_orders_canceled(self):
        from lightfee.venues.gate_private_ws import handle_gate_private_message

        state = PrivateWsState()
        symbol_map = {"ETH_USDT": "ETHUSDT"}
        raw = json.dumps({
            "channel": "futures.orders",
            "event": "update",
            "result": [{
                "contract": "ETH_USDT",
                "id": "gate-canceled-1",
                "text": "gate-client-canceled",
                "fill_total": "0",
                "finish_as": "CANCELLED",
                "finish_time_ms": 1700000000000,
            }],
        })
        handle_gate_private_message(state, symbol_map, raw)
        await _sleep_short()
        update = state.order_by_order_id("gate-canceled-1")
        assert update is not None
        assert update.state == PassiveOrderState.CANCELED
        assert update.filled_quantity == 0.0


class TestHyperliquidPrivateParserExtended:
    """Hyperliquid: hydrate/userEvents/orderUpdates/NoData."""

    @pytest.mark.asyncio
    async def test_order_update_open(self):
        from lightfee.venues.hyperliquid_private_ws import _apply_hyperliquid_private_message

        state = PrivateWsState()
        symbol_map = {"ETH": "ETHUSDT"}
        raw = json.dumps({
            "channel": "orderUpdates",
            "data": [{
                "order": {
                    "coin": "ETH",
                    "oid": 999001,
                    "cloid": "hl-open-1",
                    "filledSz": "0.0",
                    "limitPx": "2150.00",
                    "timestamp": 1700000000000,
                },
                "status": "open",
            }],
        })
        _apply_hyperliquid_private_message(state, symbol_map, raw)
        await _sleep_short()
        update = state.order_by_order_id("999001")
        assert update is not None
        assert update.state == PassiveOrderState.OPEN

    @pytest.mark.asyncio
    async def test_user_fill_event(self):
        from lightfee.venues.hyperliquid_private_ws import _apply_hyperliquid_private_message

        state = PrivateWsState()
        symbol_map = {"ETH": "ETHUSDT"}
        raw = json.dumps({
            "channel": "user",
            "data": {
                "fills": [{
                    "coin": "ETH",
                    "oid": 999002,
                    "cloid": "hl-fill-1",
                    "filledSz": "0.1",
                    "px": "2140.00",
                    "fee": "0.001",
                    "time": 1700000001000,
                }],
            },
        })
        _apply_hyperliquid_private_message(state, symbol_map, raw)
        await _sleep_short()
        update = state.order_by_order_id("999002")
        assert update is not None
        assert update.filled_quantity == 0.1
        assert update.average_price == 2140.0

    @pytest.mark.asyncio
    async def test_nodata_does_not_crash(self):
        """V1: Hyperliquid NoData or empty payload is silently ignored."""
        from lightfee.venues.hyperliquid_private_ws import _apply_hyperliquid_private_message

        state = PrivateWsState()
        symbol_map = {"ETH": "ETHUSDT"}
        # NoData-like payload
        raw = json.dumps({"channel": "pong"})
        _apply_hyperliquid_private_message(state, symbol_map, raw)
        await _sleep_short()
        # Should not have created any orders
        assert state.order_by_order_id("anything") is None


# ============================================================================
# Worker lifecycle tests with fake websocket transports
# ============================================================================


class _FakeWebSocket:
    """Fake websocket that feeds canned messages and simulates errors.

    Use _fake_connect() to create a callable suitable for patching
    websockets.connect.
    """

    def __init__(self, messages: list[str], close_after: int = 999,
                 connect_fail: bool = False, recv_fail: bool = False):
        self.messages = messages
        self.close_after = close_after
        self.connect_fail = connect_fail
        self.recv_fail = recv_fail
        self._count = 0
        self.sent: list[str] = []
        self.closed = False
        self.pings: int = 0

    async def send(self, msg: str) -> None:
        self.sent.append(msg)

    async def recv(self) -> str:
        if self.recv_fail:
            raise RuntimeError("simulated recv error")
        if self._count >= len(self.messages) or self._count >= self.close_after:
            from websockets.exceptions import ConnectionClosed
            raise ConnectionClosed(1000, "normal")
        msg = self.messages[self._count]
        self._count += 1
        return msg

    async def ping(self) -> None:
        self.pings += 1

    async def close(self) -> None:
        self.closed = True

    async def __aenter__(self):
        if self.connect_fail:
            raise RuntimeError("simulated connect error")
        return self

    async def __aexit__(self, *args):
        self.closed = True


class _BlockingFakeWebSocket(_FakeWebSocket):
    """Fake websocket that stays open until the worker explicitly closes it."""

    def __init__(self):
        super().__init__(messages=[])
        self._closed_event = asyncio.Event()

    async def recv(self) -> str:
        await self._closed_event.wait()
        from websockets.exceptions import ConnectionClosed
        raise ConnectionClosed(1000, "closed by test")

    async def close(self) -> None:
        self.closed = True
        self._closed_event.set()

    async def __aexit__(self, *args):
        await self.close()


class _MessageThenBlockingFakeWebSocket(_BlockingFakeWebSocket):
    """Fake websocket that emits canned messages, then stays open."""

    def __init__(self, messages: list[str]):
        super().__init__()
        self.messages = messages

    async def recv(self) -> str:
        if self._count < len(self.messages):
            msg = self.messages[self._count]
            self._count += 1
            return msg
        return await super().recv()


class _QueuedFakeWebSocket(_FakeWebSocket):
    """Fake websocket that lets tests feed messages after the worker starts."""

    def __init__(self):
        super().__init__(messages=[])
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._closed_event = asyncio.Event()

    def feed(self, message: str) -> None:
        self._queue.put_nowait(message)

    async def recv(self) -> str:
        if self.closed:
            from websockets.exceptions import ConnectionClosed
            raise ConnectionClosed(1000, "closed by test")
        get_task = asyncio.create_task(self._queue.get())
        close_task = asyncio.create_task(self._closed_event.wait())
        try:
            done, _ = await asyncio.wait(
                {get_task, close_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if close_task in done:
                from websockets.exceptions import ConnectionClosed
                raise ConnectionClosed(1000, "closed by test")
            return get_task.result()
        finally:
            for task in (get_task, close_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(get_task, close_task, return_exceptions=True)

    async def close(self) -> None:
        self.closed = True
        self._closed_event.set()

    async def __aexit__(self, *args):
        await self.close()


def _fake_connect(fake_ws: _FakeWebSocket):
    """Return a callable that returns the async context manager.

    For `async with websockets.connect(url) as ws:` (Binance pattern).
    websockets.connect(url) is a regular function that returns an async context manager.
    """
    def _connect(url, **kwargs):
        return fake_ws
    return _connect


def _fake_connect_awaitable(fake_ws: _FakeWebSocket):
    """Return an async callable for `await websockets.connect(url)` (OKX pattern).

    OKX uses `ws = await websockets.connect(ws_url)`.
    """
    async def _connect(url, **kwargs):
        return fake_ws
    return _connect


class _FakeTransport:
    """Minimal transport that records success/failure calls for testing."""

    def __init__(self, venue: Venue = Venue.BINANCE):
        self._success_count = 0
        self._failure_count = 0
        self._last_error = ""
        self._private_ws_state = PrivateWsState()
        self._spec = Mock()
        self._spec.venue_id = venue
        self._spec.rest_url = "https://api.binance.com"
        self._spec.private_base_url = "https://api.binance.com"
        self._credential = Mock()
        self._credential.api_key = "test-api-key"
        self._credential.api_secret = "test-api-secret"
        self._credential.api_passphrase = ""
        self._credential.wallet_private_key = ""
        self._credential.account_address = ""
        self._symbol_metadata: dict = {}
        self._diagnostics: list[dict] = []
        self._private_ws_symbol_map: dict[str, str] = {}
        self._private_ws_pending_symbol_updates: set[str] = set()

    def record_private_ws_success(self, now_ms: int) -> None:
        self._success_count += 1

    def record_private_ws_failure(self, now_ms: int, error: str,
                                   unhealthy_after: int = 5) -> None:
        self._failure_count += 1
        self._last_error = error

    def _venue_symbol(self, sym: str) -> str:
        return sym

    def _record_order_diagnostic(self, kind: str, payload: dict) -> None:
        self._diagnostics.append({"kind": kind, "payload": payload})

    def cached_private_connection_health(self):
        from lightfee.marketdata.resilience import ConnectionHealth
        return ConnectionHealth()

    @property
    def private_ws_state(self):
        return self._private_ws_state

    def start_private_ws(self, symbols):
        self._private_ws_symbol_map = {
            self._venue_symbol(symbol): symbol
            for symbol in symbols
            if symbol
        }
        self._private_ws_pending_symbol_updates.clear()
        venue = self._spec.venue_id
        if venue == Venue.BINANCE:
            from lightfee.venues.binance_private_ws import start_binance_private_ws
            start_binance_private_ws(self, symbols)
        elif venue == Venue.ASTER:
            from lightfee.venues.aster_private_ws import start_aster_private_ws
            start_aster_private_ws(self, symbols)
        elif venue == Venue.OKX:
            from lightfee.venues.okx_private_ws import start_okx_private_ws
            start_okx_private_ws(self, symbols)
        elif venue == Venue.BYBIT:
            from lightfee.venues.bybit_private_ws import start_bybit_private_ws
            start_bybit_private_ws(self, symbols)
        elif venue == Venue.BITGET:
            from lightfee.venues.bitget_private_ws import start_bitget_private_ws
            start_bitget_private_ws(self, symbols)
        elif venue == Venue.GATE:
            from lightfee.venues.gate_private_ws import start_gate_private_ws
            start_gate_private_ws(self, symbols)
        elif venue == Venue.HYPERLIQUID:
            from lightfee.venues.hyperliquid_private_ws import start_hyperliquid_private_ws
            start_hyperliquid_private_ws(self, symbols)

    def stop_private_ws(self):
        self._private_ws_state.abort_workers()

    async def _request(self, method: str, path: str, **kwargs):
        if "listenKey" in path:
            if "DELETE" in method or "PUT" in method:
                return {}
            return {"listenKey": "test-listen-key"}
        if "time" in path:
            return {"data": [{"ts": "1700000000000"}]}
        return {}


class _ListenKeyCaptureTransport:
    def __init__(self):
        self.calls = []
        self._failure_count = 0
        self._last_error = ""

    async def _request_listen_key(self, method, path, *, api_key, params=None):
        self.calls.append((method, path, api_key, params))
        if method == "POST":
            return {"listenKey": "lk-start"}
        return {}

    def record_private_ws_failure(self, now_ms: int, error: str,
                                   unhealthy_after: int = 5) -> None:
        self._failure_count += 1
        self._last_error = error


class TestListenKeyRequestShape:
    """V1 listenKey REST calls are API-key user-stream calls, not trading orders."""

    @pytest.mark.asyncio
    async def test_binance_listen_key_helpers_use_v1_request_shape(self):
        from lightfee.venues.binance_private_ws import (
            _close_binance_listen_key,
            _keepalive_binance_listen_key,
            _start_binance_listen_key,
        )

        transport = _ListenKeyCaptureTransport()

        listen_key = await _start_binance_listen_key(transport, "api-key")
        await _keepalive_binance_listen_key(transport, "api-key", listen_key)
        await _close_binance_listen_key(transport, "api-key", listen_key)

        assert listen_key == "lk-start"
        assert transport.calls == [
            ("POST", "/fapi/v1/listenKey", "api-key", None),
            ("PUT", "/fapi/v1/listenKey", "api-key", {"listenKey": "lk-start"}),
            ("DELETE", "/fapi/v1/listenKey", "api-key", {"listenKey": "lk-start"}),
        ]

    @pytest.mark.asyncio
    async def test_aster_listen_key_helpers_use_v1_request_shape(self):
        from lightfee.venues.aster_private_ws import (
            _close_aster_listen_key,
            _keepalive_aster_listen_key,
            _start_aster_listen_key,
        )

        transport = _ListenKeyCaptureTransport()

        listen_key = await _start_aster_listen_key(transport, "api-key")
        await _keepalive_aster_listen_key(transport, "api-key", listen_key)
        await _close_aster_listen_key(transport, "api-key", listen_key)

        assert listen_key == "lk-start"
        assert transport.calls == [
            ("POST", "/fapi/v1/listenKey", "api-key", None),
            ("PUT", "/fapi/v1/listenKey", "api-key", {"listenKey": "lk-start"}),
            ("DELETE", "/fapi/v1/listenKey", "api-key", {"listenKey": "lk-start"}),
        ]


class TestBinanceWorkerLifecycle:
    """Binance private WS worker lifecycle with fake websocket."""

    @pytest.mark.asyncio
    async def test_success_on_connect_and_message(self):
        """connect + message → record_private_ws_success is called."""
        transport = _FakeTransport()

        with patch(
            "lightfee.venues.binance_private_ws.websockets.connect",
            side_effect=_fake_connect(_FakeWebSocket(
                messages=[json.dumps({
                    "e": "executionReport",
                    "s": "ETHUSDT",
                    "i": "1",
                    "c": "test-client",
                    "X": "FILLED",
                    "z": "0.01",
                    "ap": "2140.00",
                    "T": 1700000000000,
                })],
                close_after=1,
            )),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.1)
            transport.stop_private_ws()
            await _sleep_short()

        # connect success + message success (at least 2)
        assert transport._success_count >= 2

    @pytest.mark.asyncio
    async def test_failure_on_connect_error(self):
        """connect failure → record_private_ws_failure is called."""
        transport = _FakeTransport()

        def _connect_fail(url, **kwargs):
            raise RuntimeError("simulated connect error")

        with patch(
            "lightfee.venues.binance_private_ws.websockets.connect",
            side_effect=_connect_fail,
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.15)
            transport.stop_private_ws()
            await _sleep_short()

        assert transport._failure_count >= 1
        assert "connect" in transport._last_error.lower()

    @pytest.mark.asyncio
    async def test_idempotent_start(self):
        """Repeated start with same symbols does not create extra workers."""
        transport = _FakeTransport()

        with patch(
            "lightfee.venues.binance_private_ws.websockets.connect",
            side_effect=_fake_connect(_FakeWebSocket(messages=[])),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            # push_worker replaces existing workers; count should stay 1
            assert transport.private_ws_state.worker_count() == 1

    @pytest.mark.asyncio
    async def test_symbol_update_reaches_running_binance_worker(self):
        """Binance listenKey streams are account-scoped; dynamic symbols update the live parser map."""
        from lightfee.venues.transport import VenueTransport

        transport = _FakeTransport(venue=Venue.BINANCE)
        ws = _QueuedFakeWebSocket()

        with patch(
            "lightfee.venues.binance_private_ws.websockets.connect",
            side_effect=_fake_connect(ws),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _wait_until(lambda: transport._success_count >= 1)

            VenueTransport.update_private_ws_symbols(transport, ["BTCUSDT"])
            ws.feed(json.dumps({
                "e": "executionReport",
                "s": "BTCUSDT",
                "i": "binance-dynamic-1",
                "c": "binance-dynamic-client-1",
                "X": "FILLED",
                "z": "0.02",
                "ap": "65000.00",
                "T": 1700000000100,
            }))
            await _wait_until(
                lambda: transport.private_ws_state.order_by_order_id("binance-dynamic-1")
                is not None
            )
            transport.stop_private_ws()
            await _sleep_short()

        update = transport.private_ws_state.order_by_order_id("binance-dynamic-1")
        assert update is not None
        assert update.symbol == "BTCUSDT"
        assert update.filled_quantity == pytest.approx(0.02)


class TestOkxWorkerLifecycle:
    """OKX private WS worker lifecycle with fake websocket."""

    @pytest.mark.asyncio
    async def test_login_then_subscribe(self):
        """OKX: connect → login → subscribe flow."""
        transport = _FakeTransport()
        transport._spec.venue_id = Venue.OKX
        transport._spec.rest_url = "https://www.okx.com"
        transport._symbol_metadata = {"ETHUSDT": {"ct_val": 1.0}}

        # Override _request to return proper server time for OKX signing
        async def _fake_okx_request(method, path, **kwargs):
            if "time" in path:
                return {"data": [{"ts": "1700000000000"}]}
            return {}
        transport._request = _fake_okx_request

        with patch(
            "lightfee.venues.okx_private_ws.websockets.connect",
            side_effect=_fake_connect_awaitable(_FakeWebSocket(
                messages=[
                    json.dumps({"event": "login", "code": "0"}),
                    json.dumps({
                        "arg": {"channel": "orders", "instType": "SWAP", "instId": "ETH-USDT-SWAP"},
                        "data": [{
                            "instId": "ETH-USDT-SWAP",
                            "ordId": "okx-lifecycle-1",
                            "clOrdId": "okx-lifecycle-client-1",
                            "accFillSz": "1",
                            "avgPx": "2140.00",
                            "state": "FILLED",
                            "fillTime": "1700000000100",
                        }],
                    }),
                ],
                close_after=2,
            )),
        ):
            from lightfee.venues.okx_private_ws import start_okx_private_ws
            start_okx_private_ws(transport, ["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.15)
            transport.stop_private_ws()
            await _sleep_short()

        # connect success + message success at minimum
        assert transport._success_count >= 2

    @pytest.mark.asyncio
    async def test_failure_on_login_reject(self):
        """recv failure → record_private_ws_failure."""
        transport = _FakeTransport()
        transport._spec.venue_id = Venue.OKX
        transport._spec.rest_url = "https://www.okx.com"

        async def _fake_okx_request(method, path, **kwargs):
            if "time" in path:
                return {"data": [{"ts": "1700000000000"}]}
            return {}
        transport._request = _fake_okx_request

        with patch(
            "lightfee.venues.okx_private_ws.websockets.connect",
            side_effect=_fake_connect_awaitable(_FakeWebSocket(messages=[], recv_fail=True)),
        ):
            from lightfee.venues.okx_private_ws import start_okx_private_ws
            start_okx_private_ws(transport, ["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.15)
            transport.stop_private_ws()
            await _sleep_short()

        assert transport._failure_count >= 1

    @pytest.mark.asyncio
    async def test_initial_okx_subscription_requires_both_channel_acks_before_parsing(self):
        """Initial OKX subscription uses the same both-channel ACK gate."""
        from lightfee.venues.okx_private_ws import start_okx_private_ws

        transport = _FakeTransport()
        transport._spec.venue_id = Venue.OKX
        transport._spec.rest_url = "https://www.okx.com"
        transport._spec.private_base_url = "https://www.okx.com"
        transport._venue_symbol = lambda symbol: symbol.replace("USDT", "-USDT-SWAP")
        transport._symbol_metadata = {"ETHUSDT": {"ct_val": 0.1}}

        async def _fake_okx_request(method, path, **kwargs):
            if "time" in path:
                return {"data": [{"ts": "1700000000000"}]}
            return {}
        transport._request = _fake_okx_request

        ws = _QueuedFakeWebSocket()
        with patch(
            "lightfee.venues.okx_private_ws.websockets.connect",
            side_effect=_fake_connect_awaitable(ws),
        ):
            start_okx_private_ws(transport, ["ETHUSDT"])
            await _wait_until(lambda: any('"op": "login"' in msg for msg in ws.sent))
            ws.feed(json.dumps({"event": "login", "code": "0"}))
            await _wait_until(
                lambda: set(_okx_subscribe_request_ids(ws.sent, "ETH-USDT-SWAP"))
                == {"orders", "positions"}
            )
            request_ids = _okx_subscribe_request_ids(ws.sent, "ETH-USDT-SWAP")
            eth_order_message = json.dumps({
                "arg": {
                    "channel": "orders",
                    "instType": "SWAP",
                    "instId": "ETH-USDT-SWAP",
                },
                "data": [{
                    "instId": "ETH-USDT-SWAP",
                    "ordId": "okx-initial-ack-1",
                    "clOrdId": "okx-initial-ack-client-1",
                    "accFillSz": "3",
                    "avgPx": "3000.00",
                    "state": "FILLED",
                    "uTime": "1700000000300",
                }],
            })

            ws.feed(json.dumps({
                "event": "subscribe",
                "code": "0",
                "id": request_ids["orders"],
                "arg": {
                    "channel": "orders",
                    "instType": "SWAP",
                    "instId": "ETH-USDT-SWAP",
                },
            }))
            ws.feed(eth_order_message)
            await asyncio.sleep(0.05)
            assert (
                transport.private_ws_state.order_by_order_id("okx-initial-ack-1")
                is None
            )

            ws.feed(json.dumps({
                "event": "subscribe",
                "code": "0",
                "id": request_ids["positions"],
                "arg": {
                    "channel": "positions",
                    "instType": "SWAP",
                    "instId": "ETH-USDT-SWAP",
                },
            }))
            ws.feed(eth_order_message)
            await _wait_until(
                lambda: transport.private_ws_state.order_by_order_id(
                    "okx-initial-ack-1"
                )
                is not None
            )
            transport.stop_private_ws()
            await _sleep_short()

        update = transport.private_ws_state.order_by_order_id("okx-initial-ack-1")
        assert update is not None
        assert update.filled_quantity == pytest.approx(0.3)

    @pytest.mark.asyncio
    async def test_symbol_update_subscribes_existing_okx_worker_and_updates_ct_val(self):
        """OKX runtime additions must subscribe on the live socket and parse with ctVal."""
        from lightfee.venues.okx_private_ws import start_okx_private_ws
        from lightfee.venues.transport import VenueTransport

        transport = _FakeTransport()
        transport._spec.venue_id = Venue.OKX
        transport._spec.rest_url = "https://www.okx.com"
        transport._spec.private_base_url = "https://www.okx.com"
        transport._venue_symbol = lambda symbol: symbol.replace("USDT", "-USDT-SWAP")
        transport._symbol_metadata = {
            "ETHUSDT": {"ct_val": 0.1},
            "BTCUSDT": {"ct_val": 0.01},
        }

        async def _fake_okx_request(method, path, **kwargs):
            if "time" in path:
                return {"data": [{"ts": "1700000000000"}]}
            return {}
        transport._request = _fake_okx_request

        ws = _QueuedFakeWebSocket()
        with patch(
            "lightfee.venues.okx_private_ws.websockets.connect",
            side_effect=_fake_connect_awaitable(ws),
        ):
            start_okx_private_ws(transport, ["ETHUSDT"])
            await _wait_until(lambda: any('"op": "login"' in msg for msg in ws.sent))
            ws.feed(json.dumps({"event": "login", "code": "0"}))
            await _wait_until(
                lambda: sum('"op": "subscribe"' in msg for msg in ws.sent) >= 2
            )

            VenueTransport.update_private_ws_symbols(transport, ["BTCUSDT"])
            ws.feed(json.dumps({"event": "subscribe", "code": "0"}))
            await _wait_until(
                lambda: any("BTC-USDT-SWAP" in msg for msg in ws.sent)
            )
            request_ids = _okx_subscribe_request_ids(ws.sent, "BTC-USDT-SWAP")
            assert set(request_ids) == {"orders", "positions"}

            btc_order_message = json.dumps({
                "arg": {
                    "channel": "orders",
                    "instType": "SWAP",
                    "instId": "BTC-USDT-SWAP",
                },
                "data": [{
                    "instId": "BTC-USDT-SWAP",
                    "ordId": "okx-dynamic-1",
                    "clOrdId": "okx-dynamic-client-1",
                    "accFillSz": "2",
                    "avgPx": "65000.00",
                    "state": "FILLED",
                    "uTime": "1700000000200",
                }],
            })
            ws.feed(json.dumps({
                "event": "subscribe",
                "code": "0",
                "id": request_ids["orders"],
                "arg": {
                    "channel": "orders",
                    "instType": "SWAP",
                    "instId": "BTC-USDT-SWAP",
                },
            }))
            ws.feed(btc_order_message)
            await asyncio.sleep(0.05)
            assert (
                transport.private_ws_state.order_by_order_id("okx-dynamic-1")
                is None
            )
            assert transport._private_ws_pending_symbol_updates == {
                "BTC-USDT-SWAP"
            }

            ws.feed(json.dumps({
                "event": "subscribe",
                "code": "0",
                "id": request_ids["positions"],
                "arg": {
                    "channel": "positions",
                    "instType": "SWAP",
                    "instId": "BTC-USDT-SWAP",
                },
            }))
            ws.feed(btc_order_message)
            await _wait_until(
                lambda: transport.private_ws_state.order_by_order_id("okx-dynamic-1")
                is not None
            )
            transport.stop_private_ws()
            await _sleep_short()

        update = transport.private_ws_state.order_by_order_id("okx-dynamic-1")
        assert update is not None
        assert update.filled_quantity == pytest.approx(0.02)
        assert "BTC-USDT-SWAP" not in transport._private_ws_pending_symbol_updates

    @pytest.mark.asyncio
    async def test_symbol_update_missing_okx_ct_val_is_retained_for_retry(self):
        """OKX additions without trusted ctVal must fail closed and remain pending."""
        from lightfee.venues.okx_private_ws import start_okx_private_ws
        from lightfee.venues.transport import VenueTransport

        transport = _FakeTransport()
        transport._spec.venue_id = Venue.OKX
        transport._spec.rest_url = "https://www.okx.com"
        transport._spec.private_base_url = "https://www.okx.com"
        transport._venue_symbol = lambda symbol: symbol.replace("USDT", "-USDT-SWAP")
        transport._symbol_metadata = {"ETHUSDT": {"ct_val": 0.1}}

        async def _fake_okx_request(method, path, **kwargs):
            if "time" in path:
                return {"data": [{"ts": "1700000000000"}]}
            return {}
        transport._request = _fake_okx_request

        ws = _QueuedFakeWebSocket()
        with patch(
            "lightfee.venues.okx_private_ws.websockets.connect",
            side_effect=_fake_connect_awaitable(ws),
        ):
            start_okx_private_ws(transport, ["ETHUSDT"])
            await _wait_until(lambda: any('"op": "login"' in msg for msg in ws.sent))
            ws.feed(json.dumps({"event": "login", "code": "0"}))
            await _wait_until(
                lambda: sum('"op": "subscribe"' in msg for msg in ws.sent) >= 2
            )

            VenueTransport.update_private_ws_symbols(transport, ["BTCUSDT"])
            sent_before_update = len(ws.sent)
            ws.feed(json.dumps({"event": "subscribe", "code": "0"}))
            await _wait_until(lambda: transport._failure_count >= 1)
            transport.stop_private_ws()
            await _sleep_short()

        assert transport._last_error.startswith(
            "okx private ws ctVal metadata missing for symbol update"
        )
        assert transport._private_ws_pending_symbol_updates == {"BTC-USDT-SWAP"}
        assert not any("BTC-USDT-SWAP" in msg for msg in ws.sent[sent_before_update:])

    @pytest.mark.asyncio
    async def test_symbol_update_okx_subscribe_error_retains_pending_and_retries(self):
        """OKX subscribe rejection must not activate or clear the dynamic addition."""
        from lightfee.venues.okx_private_ws import start_okx_private_ws
        from lightfee.venues.transport import VenueTransport

        transport = _FakeTransport()
        transport._spec.venue_id = Venue.OKX
        transport._spec.rest_url = "https://www.okx.com"
        transport._spec.private_base_url = "https://www.okx.com"
        transport._venue_symbol = lambda symbol: symbol.replace("USDT", "-USDT-SWAP")
        transport._symbol_metadata = {
            "ETHUSDT": {"ct_val": 0.1},
            "BTCUSDT": {"ct_val": 0.01},
        }

        async def _fake_okx_request(method, path, **kwargs):
            if "time" in path:
                return {"data": [{"ts": "1700000000000"}]}
            return {}
        transport._request = _fake_okx_request

        ws = _QueuedFakeWebSocket()
        with (
            patch(
                "lightfee.venues.okx_private_ws.websockets.connect",
                side_effect=_fake_connect_awaitable(ws),
            ),
            patch(
                "lightfee.venues.okx_private_ws.OKX_PRIVATE_WS_SUBSCRIBE_RETRY_INITIAL_MS",
                1,
            ),
        ):
            start_okx_private_ws(transport, ["ETHUSDT"])
            await _wait_until(lambda: any('"op": "login"' in msg for msg in ws.sent))
            ws.feed(json.dumps({"event": "login", "code": "0"}))
            await _wait_until(
                lambda: sum('"op": "subscribe"' in msg for msg in ws.sent) >= 2
            )

            VenueTransport.update_private_ws_symbols(transport, ["BTCUSDT"])
            ws.feed(json.dumps({"msg": "pong"}))
            await _wait_until(
                lambda: set(_okx_subscribe_request_ids(ws.sent, "BTC-USDT-SWAP"))
                == {"orders", "positions"}
            )
            request_ids = _okx_subscribe_request_ids(ws.sent, "BTC-USDT-SWAP")
            order_request_id = request_ids["orders"]
            order_send_count = lambda: sum(
                order_request_id in msg and '"op": "subscribe"' in msg
                for msg in ws.sent
            )
            assert order_send_count() == 1

            ws.feed(json.dumps({
                "event": "error",
                "id": order_request_id,
                "code": "60012",
                "msg": "subscribe rejected",
                "arg": {
                    "channel": "orders",
                    "instType": "SWAP",
                    "instId": "BTC-USDT-SWAP",
                },
            }))
            await _wait_until(lambda: transport._failure_count >= 1)
            await asyncio.sleep(0.02)
            ws.feed(json.dumps({"msg": "pong"}))
            await _wait_until(lambda: order_send_count() >= 2)
            transport.stop_private_ws()
            await _sleep_short()

        assert transport._last_error.startswith("okx private ws subscribe rejected")
        assert transport._private_ws_pending_symbol_updates == {"BTC-USDT-SWAP"}
        assert "BTC-USDT-SWAP" not in getattr(
            transport,
            "_okx_private_ws_active_symbol_map",
            {},
        )


class TestPrivateWsResolver:
    """Private WS + REST fallback resolution in query_passive_order_progress."""

    @pytest.mark.asyncio
    async def test_returns_terminal_state_with_zero_fill(self):
        """V1: CANCELED/REJECTED/EXPIRED with 0 fill are authoritative evidence."""
        from lightfee.marketdata.private_ws import CumulativeOrderProgress
        from lightfee.core.domain import PassiveOrderState

        state = PrivateWsState()
        await state.record_order(PrivateOrderUpdate(
            symbol="ETHUSDT",
            order_id="canceled-order",
            client_order_id="canceled-client",
            filled_quantity=0.0,
            state=PassiveOrderState.CANCELED,
            updated_at_ms=int(time.time() * 1000),
        ))

        progress = state.order_progress_if_fresh(
            client_order_id="canceled-client",
            max_age_ms=60_000,
            wall_clock_now_ms=int(time.time() * 1000),
        )
        assert progress is not None
        assert progress.state == PassiveOrderState.CANCELED
        assert progress.cumulative_quantity == 0.0

    @pytest.mark.asyncio
    async def test_returns_open_state_with_zero_fill(self):
        """V1: OPEN with 0 fill confirms the private WS sees the order."""
        state = PrivateWsState()
        await state.record_order(PrivateOrderUpdate(
            symbol="ETHUSDT",
            order_id="open-order",
            client_order_id="open-client",
            filled_quantity=0.0,
            state=PassiveOrderState.OPEN,
            updated_at_ms=int(time.time() * 1000),
        ))

        progress = state.order_progress_if_fresh(
            client_order_id="open-client",
            max_age_ms=60_000,
            wall_clock_now_ms=int(time.time() * 1000),
        )
        assert progress is not None
        assert progress.state == PassiveOrderState.OPEN

    @pytest.mark.asyncio
    async def test_stale_update_falls_through(self):
        """Stale update beyond max_age_ms is not returned."""
        state = PrivateWsState()
        now = int(time.time() * 1000)
        await state.record_order(PrivateOrderUpdate(
            symbol="ETHUSDT",
            order_id="stale-order",
            filled_quantity=0.01,
            updated_at_ms=now - 60_000,
        ))

        progress = state.order_progress_if_fresh(
            order_id="stale-order",
            max_age_ms=5_000,
            wall_clock_now_ms=now,
        )
        assert progress is None

    @pytest.mark.asyncio
    async def test_after_updated_at_ms_captures_new_update(self):
        """lookup_or_wait_private_order_progress_after waits for newer update."""
        from lightfee.marketdata.private_ws import lookup_or_wait_private_order_progress_after

        state = PrivateWsState()
        await state.record_order(PrivateOrderUpdate(
            symbol="ETHUSDT",
            order_id="after-order",
            client_order_id="after-client",
            filled_quantity=0.01,
            updated_at_ms=10,
        ))

        # Record update after 30ms
        async def _delayed():
            await asyncio.sleep(0.03)
            await state.record_order(PrivateOrderUpdate(
                symbol="ETHUSDT",
                order_id="after-order",
                client_order_id="after-client",
                filled_quantity=0.012,
                updated_at_ms=20,
            ))

        asyncio.create_task(_delayed())
        progress = await lookup_or_wait_private_order_progress_after(
            state, "after-client", "after-order",
            after_updated_at_ms=10, wait_ms=500,
        )
        assert progress is not None
        assert progress.cumulative_quantity == 0.012


# ============================================================================
# Aster worker lifecycle tests
# ============================================================================


class TestAsterWorkerLifecycle:
    """Aster private WS worker lifecycle: listenKey + connect + message + close."""

    @pytest.mark.asyncio
    async def test_success_on_connect_and_message(self):
        """success on listenKey start + connect + message."""
        transport = _FakeTransport(venue=Venue.ASTER)
        transport._spec.rest_url = "https://fapi.aster.com"

        with patch(
            "lightfee.venues.aster_private_ws.websockets.connect",
            side_effect=_fake_connect(_FakeWebSocket(
                messages=[json.dumps({
                    "e": "TRADE_LITE",
                    "s": "ETHUSDT",
                    "i": 111,
                    "c": "aster-client-1",
                    "l": "0.01",
                    "L": "2140.00",
                    "T": 1700000000000,
                })],
                close_after=1,
            )),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.15)
            transport.stop_private_ws()
            await _sleep_short()

        # listenKey start is internal — success on connect + message
        assert transport._success_count >= 2

    @pytest.mark.asyncio
    async def test_incremental_symbol_update_keeps_parser_map_live_for_truth_events(self):
        """Aster account stream decodes symbols added after the worker starts."""
        from lightfee.venues.specs import get_spec
        from lightfee.venues.transport import LiveCredential, VenueTransport

        transport = VenueTransport(
            get_spec(Venue.ASTER),
            mode="paper",
            credential=LiveCredential(api_key="test-api-key", api_secret="test-secret"),
        )
        ws = _QueuedFakeWebSocket()
        connect_urls: list[str] = []

        async def _request_listen_key(method, path, *, api_key, params=None):
            if method == "POST":
                return {"listenKey": "lk-incremental"}
            return {}

        def _connect(url, **kwargs):
            connect_urls.append(url)
            return ws

        transport._request_listen_key = _request_listen_key
        with patch(
            "lightfee.venues.aster_private_ws.websockets.connect",
            side_effect=_connect,
        ):
            transport.start_private_ws(["ETHUSDT"])
            try:
                await _wait_until(lambda: bool(connect_urls))
                added = transport.update_private_ws_symbols(["BTCUSDT"])
                assert added == {"BTCUSDT"}
                assert transport.consume_private_ws_symbol_updates() == {}

                ws.feed(json.dumps({
                    "e": "TRADE_LITE",
                    "s": "BTCUSDT",
                    "i": 333,
                    "c": "aster-incremental-btc",
                    "l": "0.25",
                    "L": "65000.00",
                    "T": 1700000004000,
                }))
                ws.feed(json.dumps({
                    "e": "ACCOUNT_UPDATE",
                    "E": 1700000005000,
                    "a": {"P": [{
                        "s": "BTCUSDT",
                        "pa": "0.40",
                        "ps": "SHORT",
                    }]},
                }))

                await _wait_until(
                    lambda: (
                        transport.private_ws_state.order_by_client_id(
                            "aster-incremental-btc"
                        )
                        is not None
                    )
                )
                await _wait_until(
                    lambda: transport.private_ws_state.position("BTCUSDT") is not None
                )
            finally:
                transport.stop_private_ws()
                await _sleep_short()

        update = transport.private_ws_state.order_by_client_id("aster-incremental-btc")
        assert update is not None
        assert update.symbol == "BTCUSDT"
        assert update.filled_quantity == 0.25
        position = transport.private_ws_state.position("BTCUSDT")
        assert position is not None
        assert position.size == -0.40
        assert position.updated_at_ms == 1700000005000

    @pytest.mark.asyncio
    async def test_failure_on_listen_key_start(self):
        """listenKey start failure → record_private_ws_failure."""
        transport = _FakeTransport(venue=Venue.ASTER)
        transport._spec.rest_url = "https://fapi.aster.com"

        async def _fail_listen_key(method, path, **kwargs):
            if "listenKey" in path and "POST" in method:
                raise RuntimeError("simulated listenKey failure")
            return {}
        transport._request = _fail_listen_key

        transport.start_private_ws(["ETHUSDT"])
        await _sleep_short()
        await asyncio.sleep(0.15)
        transport.stop_private_ws()
        await _sleep_short()

        assert transport._failure_count >= 1
        assert "listenkey" in transport._last_error.lower()

    @pytest.mark.asyncio
    async def test_failure_on_connect_error(self):
        """connect failure after listenKey → record_private_ws_failure."""
        transport = _FakeTransport(venue=Venue.ASTER)
        transport._spec.rest_url = "https://fapi.aster.com"

        with patch(
            "lightfee.venues.aster_private_ws.websockets.connect",
            side_effect=RuntimeError("simulated connect error"),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.15)
            transport.stop_private_ws()
            await _sleep_short()

        assert transport._failure_count >= 1
        assert "connect" in transport._last_error.lower()

    @pytest.mark.asyncio
    async def test_failure_on_close(self):
        """ConnectionClosed → record_private_ws_failure."""
        transport = _FakeTransport(venue=Venue.ASTER)
        transport._spec.rest_url = "https://fapi.aster.com"

        with patch(
            "lightfee.venues.aster_private_ws.websockets.connect",
            side_effect=_fake_connect(_FakeWebSocket(messages=[], recv_fail=True)),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.15)
            transport.stop_private_ws()
            await _sleep_short()

        assert transport._failure_count >= 1

    @pytest.mark.asyncio
    async def test_invalid_listen_key_keepalive_closes_ws_and_rotates(self):
        """Aster -1125 keepalive is terminal: close old WS and reconnect with a new listenKey."""
        from lightfee.venues.transport import TransportError, TransportErrorCategory

        transport = _FakeTransport(venue=Venue.ASTER)
        transport._spec.rest_url = "https://fapi.aster.com"
        transport._spec.private_base_url = "https://fapi.aster.com"
        listen_keys = ["lk-old", "lk-new"]
        requests: list[tuple[str, Optional[str]]] = []
        connect_urls: list[str] = []

        async def _request(method: str, path: str, **kwargs):
            params = kwargs.get("params") or {}
            requests.append((method, params.get("listenKey")))
            if method == "POST":
                return {"listenKey": listen_keys.pop(0)}
            if method == "PUT" and params.get("listenKey") == "lk-old":
                raise TransportError(
                    TransportErrorCategory.REQUEST_REJECTED,
                    'HTTP 400: {"code":-1125,"msg":"This listenKey does not exist."}',
                    status_code=400,
                    body='{"code":-1125,"msg":"This listenKey does not exist."}',
                )
            return {}

        old_ws = _BlockingFakeWebSocket()
        new_ws = _MessageThenBlockingFakeWebSocket(
            messages=[json.dumps({
                "e": "TRADE_LITE",
                "s": "ETHUSDT",
                "i": 222,
                "c": "rotated-client",
                "l": "0.03",
                "L": "2145.00",
                "T": 1700000003000,
            })],
        )
        sockets = [old_ws, new_ws]

        def _connect(url, **kwargs):
            connect_urls.append(url)
            return sockets[len(connect_urls) - 1]

        transport._request = _request
        with (
            patch("lightfee.venues.aster_private_ws.ASTER_LISTEN_KEY_KEEPALIVE_SECS", 0.01),
            patch("lightfee.marketdata.resilience.compute_backoff_ms", return_value=1),
            patch("lightfee.venues.aster_private_ws.websockets.connect", side_effect=_connect),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _wait_until(lambda: old_ws.closed and len(connect_urls) >= 2)
            await _wait_until(
                lambda: transport.private_ws_state.order_by_client_id("rotated-client") is not None
            )
            transport.stop_private_ws()
            await _sleep_short()

        assert old_ws.closed is True
        assert connect_urls == [
            "wss://fapi.aster.com/ws/lk-old",
            "wss://fapi.aster.com/ws/lk-new",
        ]
        assert requests.count(("POST", None)) == 2
        assert ("PUT", "lk-old") in requests
        assert transport.private_ws_state.order_by_client_id("rotated-client") is not None
        rotation_events = [
            event["payload"]
            for event in transport._diagnostics
            if event["kind"] == "aster.listen_key_rotation"
        ]
        assert rotation_events
        rotation = rotation_events[-1]
        assert rotation["invalid_reason"] == "invalid_listen_key_-1125"
        assert rotation["rotation_count"] == 1
        assert rotation["reconnect_result"] == "success"
        assert rotation["listenKey_created_at"] is not None
        assert "last_keepalive_ok_at" in rotation
        assert rotation["private_ws_silent_age"] is not None
        assert rotation["new_listenKey_created_at"] is not None

    @pytest.mark.asyncio
    async def test_transient_keepalive_failure_does_not_create_new_listen_keys(self):
        """Generic keepalive failures are not terminal invalid-listen-key rotations."""
        transport = _FakeTransport(venue=Venue.ASTER)
        transport._spec.rest_url = "https://fapi.aster.com"
        transport._spec.private_base_url = "https://fapi.aster.com"
        requests: list[tuple[str, Optional[str]]] = []
        connect_urls: list[str] = []
        put_attempts = 0

        async def _request(method: str, path: str, **kwargs):
            nonlocal put_attempts
            params = kwargs.get("params") or {}
            requests.append((method, params.get("listenKey")))
            if method == "POST":
                return {"listenKey": "lk-stable"}
            if method == "PUT":
                put_attempts += 1
                if put_attempts == 1:
                    raise RuntimeError("temporary keepalive timeout")
                return {}
            return {}

        ws = _BlockingFakeWebSocket()

        def _connect(url, **kwargs):
            connect_urls.append(url)
            return ws

        transport._request = _request
        with (
            patch("lightfee.venues.aster_private_ws.ASTER_LISTEN_KEY_KEEPALIVE_SECS", 0.01),
            patch("lightfee.venues.aster_private_ws.websockets.connect", side_effect=_connect),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _wait_until(lambda: put_attempts >= 2)
            await asyncio.sleep(0.04)
            transport.stop_private_ws()
            await _sleep_short()

        assert ws.closed is True
        assert connect_urls == ["wss://fapi.aster.com/ws/lk-stable"]
        assert requests.count(("POST", None)) == 1
        assert not any(event["kind"] == "aster.listen_key_rotation" for event in transport._diagnostics)


# ============================================================================
# Bybit worker lifecycle tests
# ============================================================================


class TestBybitWorkerLifecycle:
    """Bybit private WS worker lifecycle: auth + subscribe."""

    @pytest.mark.asyncio
    async def test_auth_and_subscribe_success(self):
        """auth → subscribe → message → success_count increases."""
        transport = _FakeTransport(venue=Venue.BYBIT)
        transport._spec.rest_url = "https://api.bybit.com"

        with patch(
            "lightfee.venues.bybit_private_ws.websockets.connect",
            side_effect=_fake_connect_awaitable(_FakeWebSocket(
                messages=[
                    json.dumps({"op": "auth", "success": True}),
                    json.dumps({"op": "subscribe", "success": True}),
                    json.dumps({
                        "topic": "execution",
                        "data": [{
                            "symbol": "ETHUSDT",
                            "orderId": "bybit-lifecycle-1",
                            "orderLinkId": "bybit-lifecycle-client-1",
                            "execQty": "0.02",
                            "execPrice": "2140.00",
                            "execTime": "1700000000000",
                        }],
                    }),
                ],
                close_after=3,
            )),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.15)
            transport.stop_private_ws()
            await _sleep_short()

        # connect success + auth/message success
        assert transport._success_count >= 2

    @pytest.mark.asyncio
    async def test_symbol_update_reaches_running_bybit_worker(self):
        """Bybit account-level private topics must parse newly tracked symbols live."""
        from lightfee.venues.transport import VenueTransport

        transport = _FakeTransport(venue=Venue.BYBIT)
        transport._spec.rest_url = "https://api.bybit.com"
        ws = _QueuedFakeWebSocket()

        with patch(
            "lightfee.venues.bybit_private_ws.websockets.connect",
            side_effect=_fake_connect_awaitable(ws),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _wait_until(lambda: any('"op": "auth"' in msg for msg in ws.sent))
            ws.feed(json.dumps({"op": "auth", "success": True}))
            await _wait_until(
                lambda: any('"op": "subscribe"' in msg for msg in ws.sent)
            )

            VenueTransport.update_private_ws_symbols(transport, ["BTCUSDT"])
            ws.feed(json.dumps({
                "topic": "execution",
                "data": [{
                    "symbol": "BTCUSDT",
                    "orderId": "bybit-dynamic-1",
                    "orderLinkId": "bybit-dynamic-client-1",
                    "execQty": "0.02",
                    "execPrice": "65000.00",
                    "execTime": "1700000000100",
                }],
            }))
            await _wait_until(
                lambda: transport.private_ws_state.order_by_order_id("bybit-dynamic-1")
                is not None
            )
            transport.stop_private_ws()
            await _sleep_short()

        update = transport.private_ws_state.order_by_order_id("bybit-dynamic-1")
        assert update is not None
        assert update.filled_quantity == pytest.approx(0.02)
        assert transport._private_ws_pending_symbol_updates == set()

    @pytest.mark.asyncio
    async def test_failure_on_connect_error(self):
        """connect failure → record_private_ws_failure."""
        transport = _FakeTransport(venue=Venue.BYBIT)
        transport._spec.rest_url = "https://api.bybit.com"

        with patch(
            "lightfee.venues.bybit_private_ws.websockets.connect",
            side_effect=RuntimeError("simulated connect error"),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.15)
            transport.stop_private_ws()
            await _sleep_short()

        assert transport._failure_count >= 1
        assert "connect" in transport._last_error.lower()

    @pytest.mark.asyncio
    async def test_failure_on_auth_send(self):
        """auth send failure → record_private_ws_failure."""
        transport = _FakeTransport(venue=Venue.BYBIT)
        transport._spec.rest_url = "https://api.bybit.com"

        # Simulate recv error before auth message goes through
        with patch(
            "lightfee.venues.bybit_private_ws.websockets.connect",
            side_effect=_fake_connect_awaitable(_FakeWebSocket(messages=[], recv_fail=True)),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.15)
            transport.stop_private_ws()
            await _sleep_short()

        assert transport._failure_count >= 1


# ============================================================================
# Bitget worker lifecycle tests
# ============================================================================


class TestBitgetWorkerLifecycle:
    """Bitget private WS worker lifecycle: login + subscribe."""

    @pytest.mark.asyncio
    async def test_login_and_subscribe_success(self):
        """login → subscribe → message → success_count increases."""
        transport = _FakeTransport(venue=Venue.BITGET)
        transport._spec.rest_url = "https://api.bitget.com"

        with patch(
            "lightfee.venues.bitget_private_ws.websockets.connect",
            side_effect=_fake_connect_awaitable(_FakeWebSocket(
                messages=[
                    json.dumps({"event": "login", "code": "0"}),
                    json.dumps({"event": "subscribe"}),
                    json.dumps({
                        "arg": {"channel": "orders", "instId": "ETHUSDT_UMCBL"},
                        "data": [{
                            "instId": "ETHUSDT_UMCBL",
                            "orderId": "bg-lifecycle-1",
                            "clientOid": "bg-lifecycle-client-1",
                            "accBaseVolume": "0.03",
                            "avgPrice": "2140.0",
                            "status": "FILLED",
                            "uTime": "1700000000000",
                        }],
                    }),
                ],
                close_after=3,
            )),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.15)
            transport.stop_private_ws()
            await _sleep_short()

        assert transport._success_count >= 2

    @pytest.mark.asyncio
    async def test_symbol_update_reaches_running_bitget_worker_with_default_scope(self):
        """Bitget private orders/positions use instId=default and dynamic symbols update the live parser map."""
        from lightfee.venues.transport import VenueTransport

        transport = _FakeTransport(venue=Venue.BITGET)
        transport._spec.rest_url = "https://api.bitget.com"
        ws = _QueuedFakeWebSocket()

        with patch(
            "lightfee.venues.bitget_private_ws.websockets.connect",
            side_effect=_fake_connect_awaitable(ws),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _wait_until(lambda: any('"op": "login"' in msg for msg in ws.sent))
            ws.feed(json.dumps({"event": "login", "code": "0"}))
            await _wait_until(
                lambda: any('"op": "subscribe"' in msg for msg in ws.sent)
            )
            subscribe_msg = next(
                json.loads(msg)
                for msg in ws.sent
                if isinstance(msg, str) and '"op": "subscribe"' in msg
            )
            assert subscribe_msg["args"] == [
                {"instType": "USDT-FUTURES", "channel": "positions", "instId": "default"},
                {"instType": "USDT-FUTURES", "channel": "orders", "instId": "default"},
            ]

            VenueTransport.update_private_ws_symbols(transport, ["BTCUSDT"])
            ws.feed(json.dumps({
                "arg": {
                    "channel": "orders",
                    "instType": "USDT-FUTURES",
                    "instId": "default",
                },
                "data": [{
                    "instId": "BTCUSDT",
                    "orderId": "bitget-dynamic-1",
                    "clientOid": "bitget-dynamic-client-1",
                    "accBaseVolume": "0.05",
                    "priceAvg": "65000.00",
                    "uTime": "1700000000500",
                }],
            }))
            await _wait_until(
                lambda: transport.private_ws_state.order_by_order_id("bitget-dynamic-1")
                is not None
            )
            transport.stop_private_ws()
            await _sleep_short()

        update = transport.private_ws_state.order_by_order_id("bitget-dynamic-1")
        assert update is not None
        assert update.symbol == "BTCUSDT"
        assert update.filled_quantity == pytest.approx(0.05)

    @pytest.mark.asyncio
    async def test_failure_on_connect_error(self):
        """connect failure → record_private_ws_failure."""
        transport = _FakeTransport(venue=Venue.BITGET)
        transport._spec.rest_url = "https://api.bitget.com"

        with patch(
            "lightfee.venues.bitget_private_ws.websockets.connect",
            side_effect=RuntimeError("simulated connect error"),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.15)
            transport.stop_private_ws()
            await _sleep_short()

        assert transport._failure_count >= 1
        assert "connect" in transport._last_error.lower()

    @pytest.mark.asyncio
    async def test_failure_on_login_send(self):
        """login send failure → record_private_ws_failure."""
        transport = _FakeTransport(venue=Venue.BITGET)
        transport._spec.rest_url = "https://api.bitget.com"

        with patch(
            "lightfee.venues.bitget_private_ws.websockets.connect",
            side_effect=_fake_connect_awaitable(_FakeWebSocket(messages=[], recv_fail=True)),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.15)
            transport.stop_private_ws()
            await _sleep_short()

        assert transport._failure_count >= 1


# ============================================================================
# Gate worker lifecycle tests
# ============================================================================


class TestGateWorkerLifecycle:
    """Gate private WS worker lifecycle: futures.orders + futures.positions signed subscribe."""

    @pytest.mark.asyncio
    async def test_signed_subscribe_and_message_success(self):
        """V1 contract-list subscribe requires both Gate channel ACKs before data."""
        transport = _FakeTransport(venue=Venue.GATE)
        transport._spec.rest_url = "https://api.gateio.ws/api/v4"
        ws = _FakeWebSocket(
            messages=[
                json.dumps({
                    "channel": "futures.orders",
                    "event": "subscribe",
                    "error": None,
                    "result": {"status": "success"},
                }),
                json.dumps({
                    "channel": "futures.positions",
                    "event": "subscribe",
                    "error": None,
                    "result": {"status": "success"},
                }),
                json.dumps({
                    "channel": "futures.orders",
                    "event": "update",
                    "result": [{
                        "contract": "ETH_USDT",
                        "id": "gate-lifecycle-1",
                        "text": "gate-lifecycle-client-1",
                        "fill_total": "0.02",
                        "fill_price": "2140.0",
                        "finish_as": "PARTIAL",
                        "finish_time_ms": 1700000000000,
                    }],
                }),
            ],
            close_after=3,
        )

        with (
            patch(
                "lightfee.venues.gate_private_ws.websockets.connect",
                side_effect=_fake_connect_awaitable(ws),
            ),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.15)
            transport.stop_private_ws()
            await _sleep_short()

        subscribe_payloads = [
            json.loads(message)["payload"]
            for message in ws.sent
            if isinstance(message, str) and "futures." in message
        ]
        assert subscribe_payloads == [["ETHUSDT"], ["ETHUSDT"]]
        # Initial dual ACK + message success.
        assert transport._success_count >= 2

    @pytest.mark.asyncio
    async def test_failure_on_connect_error(self):
        """connect failure → record_private_ws_failure."""
        transport = _FakeTransport(venue=Venue.GATE)
        transport._spec.rest_url = "https://api.gateio.ws/api/v4"

        with patch(
            "lightfee.venues.gate_private_ws.websockets.connect",
            side_effect=RuntimeError("simulated connect error"),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.15)
            transport.stop_private_ws()
            await _sleep_short()

        assert transport._failure_count >= 1
        assert "connect" in transport._last_error.lower()

    @pytest.mark.asyncio
    async def test_failure_on_subscribe_send(self):
        """subscribe send failure → record_private_ws_failure."""
        transport = _FakeTransport(venue=Venue.GATE)
        transport._spec.rest_url = "https://api.gateio.ws/api/v4"

        with patch(
            "lightfee.venues.gate_private_ws.websockets.connect",
            side_effect=_fake_connect_awaitable(_FakeWebSocket(messages=[], recv_fail=True)),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.15)
            transport.stop_private_ws()
            await _sleep_short()

        assert transport._failure_count >= 1

    @pytest.mark.asyncio
    async def test_futures_positions_event(self):
        """futures.positions update → position cache populated."""
        transport = _FakeTransport(venue=Venue.GATE)
        transport._spec.rest_url = "https://api.gateio.ws/api/v4"
        transport._symbol_metadata["ETHUSDT"] = {"quanto_multiplier": "1"}

        # _venue_symbol is identity → symbol_map maps ETHUSDT→ETHUSDT
        # So Gate contract in message must match the canonical symbol
        with patch(
            "lightfee.venues.gate_private_ws.websockets.connect",
            side_effect=_fake_connect_awaitable(_FakeWebSocket(
                messages=[
                    json.dumps({
                        "channel": "futures.orders",
                        "event": "subscribe",
                        "error": None,
                        "result": {"status": "success"},
                    }),
                    json.dumps({
                        "channel": "futures.positions",
                        "event": "subscribe",
                        "error": None,
                        "result": {"status": "success"},
                    }),
                    json.dumps({
                        "channel": "futures.positions",
                        "event": "update",
                        "result": [{
                            "contract": "ETHUSDT",
                            "size": "3",
                            "update_time_ms": 1700000000000,
                        }],
                    }),
                ],
                close_after=3,
            )),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.3)
            transport.stop_private_ws()
            await _sleep_short()

        pos = transport.private_ws_state.position("ETHUSDT")
        assert pos is not None, f"position cache should have ETHUSDT after futures.positions push"
        assert pos.size == 3.0

    @pytest.mark.asyncio
    async def test_symbol_update_subscribes_one_contract_and_waits_for_dual_ack(self):
        """Dynamic Gate contracts remain inactive until orders + positions ACK."""
        from lightfee.venues.transport import VenueTransport

        transport = _FakeTransport(venue=Venue.GATE)
        transport._spec.rest_url = "https://api.gateio.ws/api/v4"
        transport._symbol_metadata = {
            "ETHUSDT": {"quanto_multiplier": "1"},
            "BTCUSDT": {"quanto_multiplier": "0.01"},
        }
        ws = _QueuedFakeWebSocket()

        with patch(
            "lightfee.venues.gate_private_ws.websockets.connect",
            side_effect=_fake_connect_awaitable(ws),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _wait_until(lambda: len(ws.sent) >= 2)
            initial_subscriptions = [
                json.loads(message)
                for message in ws.sent
                if isinstance(message, str) and "futures." in message
            ]
            assert [message["payload"] for message in initial_subscriptions] == [
                ["ETHUSDT"],
                ["ETHUSDT"],
            ]
            for message in initial_subscriptions:
                ws.feed(json.dumps({
                    "id": message["id"],
                    "channel": message["channel"],
                    "event": "subscribe",
                    "error": None,
                    "result": {"status": "success"},
                }))
            await _wait_until(lambda: transport._success_count >= 1)

            VenueTransport.update_private_ws_symbols(transport, ["BTCUSDT"])
            await _wait_until(lambda: len(ws.sent) >= 4)
            dynamic_subscriptions = [
                json.loads(message)
                for message in ws.sent[2:]
                if isinstance(message, str) and "futures." in message
            ]
            assert [message["payload"] for message in dynamic_subscriptions] == [
                ["BTCUSDT"],
                ["BTCUSDT"],
            ]

            # A pre-ACK push must be ignored, even though the shared parser map
            # has already learned BTCUSDT from update_private_ws_symbols().
            ws.feed(json.dumps({
                "channel": "futures.orders",
                "event": "update",
                "result": [{
                    "contract": "BTCUSDT",
                    "id": "gate-dynamic-1",
                    "text": "gate-dynamic-client-1",
                    "fill_total": "3",
                    "fill_price": "65000.0",
                    "finish_as": "FILLED",
                    "finish_time_ms": 1700000000600,
                }],
            }))
            await _sleep_short()
            assert transport.private_ws_state.order_by_order_id("gate-dynamic-1") is None

            for message in dynamic_subscriptions:
                ws.feed(json.dumps({
                    "id": message["id"],
                    "channel": message["channel"],
                    "event": "subscribe",
                    "error": None,
                    "result": {"status": "success"},
                }))
            await _wait_until(
                lambda: "BTCUSDT" in getattr(
                    transport,
                    "_gate_private_ws_active_symbol_map",
                    {},
                )
            )
            ws.feed(json.dumps({
                "channel": "futures.orders",
                "event": "update",
                "result": [{
                    "contract": "BTCUSDT",
                    "id": "gate-dynamic-2",
                    "text": "gate-dynamic-client-2",
                    "fill_total": "3",
                    "fill_price": "65000.0",
                    "finish_as": "FILLED",
                    "finish_time_ms": 1700000000700,
                }],
            }))
            await _wait_until(
                lambda: transport.private_ws_state.order_by_order_id("gate-dynamic-2")
                is not None
            )
            transport.stop_private_ws()
            await _sleep_short()

        update = transport.private_ws_state.order_by_order_id("gate-dynamic-2")
        assert update is not None
        assert update.symbol == "BTCUSDT"
        assert update.filled_quantity == pytest.approx(0.03)
        assert not getattr(transport, "_private_ws_pending_symbol_updates", {})

    @pytest.mark.asyncio
    async def test_subscribe_error_never_activates_initial_contract(self):
        """A Gate subscribe error is a health failure, not a successful connection."""
        transport = _FakeTransport(venue=Venue.GATE)
        transport._spec.rest_url = "https://api.gateio.ws/api/v4"
        ws = _FakeWebSocket(
            messages=[
                json.dumps({
                    "channel": "futures.orders",
                    "event": "subscribe",
                    "error": None,
                    "result": {"status": "success"},
                }),
                json.dumps({
                    "channel": "futures.positions",
                    "event": "subscribe",
                    "error": {"label": "INVALID_ARGUMENT", "message": "bad contract"},
                    "result": None,
                }),
            ],
            close_after=2,
        )

        with patch(
            "lightfee.venues.gate_private_ws.websockets.connect",
            side_effect=_fake_connect_awaitable(ws),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.15)
            transport.stop_private_ws()
            await _sleep_short()

        assert transport._failure_count >= 1
        assert "subscribe rejected" in transport._last_error
        assert not getattr(transport, "_gate_private_ws_active_symbol_map", {})

    def test_dynamic_subscribe_error_keeps_contract_pending_for_reconnect(self):
        """A dynamic ACK rejection preserves the contract for reconnect retry."""
        from lightfee.venues.gate_private_ws import _apply_gate_private_subscription_event

        transport = _FakeTransport(venue=Venue.GATE)
        pending_subscriptions = {
            "3": {
                "channel": "futures.orders",
                "contracts": ("BTCUSDT",),
                "sent_at_ms": _now_ms(),
            },
        }
        active_symbol_map: dict[str, str] = {}

        rejection = _apply_gate_private_subscription_event(
            transport,
            {
                "id": 3,
                "channel": "futures.orders",
                "event": "subscribe",
                "error": {"label": "INVALID_ARGUMENT"},
                "result": None,
            },
            pending_subscriptions,
            {"BTCUSDT": "BTCUSDT"},
            active_symbol_map,
            {},
        )

        assert rejection is not None
        assert "subscribe rejected" in rejection
        assert pending_subscriptions["3"]["contracts"] == ("BTCUSDT",)
        assert "BTCUSDT" not in active_symbol_map

    def test_subscribe_ack_requires_explicit_success_status(self):
        """A subscribe-shaped payload without Gate's success result is not an ACK."""
        from lightfee.venues.gate_private_ws import _apply_gate_private_subscription_event

        transport = _FakeTransport(venue=Venue.GATE)
        pending_subscriptions = {
            "1": {
                "channel": "futures.orders",
                "contracts": ("ETHUSDT",),
                "sent_at_ms": _now_ms(),
            },
        }
        active_symbol_map: dict[str, str] = {}

        rejection = _apply_gate_private_subscription_event(
            transport,
            {
                "id": 1,
                "channel": "futures.orders",
                "event": "subscribe",
                "error": None,
                "result": {},
            },
            pending_subscriptions,
            {"ETHUSDT": "ETHUSDT"},
            active_symbol_map,
            {},
        )

        assert rejection is not None
        assert "subscribe rejected" in rejection
        assert "1" in pending_subscriptions
        assert not active_symbol_map

    @pytest.mark.asyncio
    async def test_missing_subscribe_ack_is_a_connection_failure(self):
        """An initial Gate subscription cannot become healthy by merely opening the socket."""
        transport = _FakeTransport(venue=Venue.GATE)
        transport._spec.rest_url = "https://api.gateio.ws/api/v4"
        ws = _QueuedFakeWebSocket()

        with (
            patch(
                "lightfee.venues.gate_private_ws.websockets.connect",
                side_effect=_fake_connect_awaitable(ws),
            ),
            patch(
                "lightfee.venues.gate_private_ws.GATE_PRIVATE_SUBSCRIBE_ACK_TIMEOUT_SECS",
                0,
            ),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _wait_until(lambda: transport._failure_count >= 1)
            transport.stop_private_ws()
            await _sleep_short()

        assert "subscribe ACK timeout" in transport._last_error
        assert transport._success_count == 0


# ============================================================================
# Hyperliquid worker lifecycle tests
# ============================================================================


class TestHyperliquidWorkerLifecycle:
    """Hyperliquid private WS worker lifecycle: hydrate + subscribe + userEvents/orderUpdates/NoData."""

    @pytest.mark.asyncio
    async def test_hydrate_and_subscribe_success(self):
        """hydrate → subscribe userEvents + orderUpdates → message → success_count."""
        transport = _FakeTransport(venue=Venue.HYPERLIQUID)
        transport._spec.rest_url = "https://api.hyperliquid.xyz"
        transport._credential.account_address = "0x1234"

        # Override _request to return hydrate data
        async def _fake_hl_request(method, path, **kwargs):
            if path == "/info":
                return {
                    "assetPositions": [{
                        "position": {"coin": "ETH", "szi": "1.5"},
                    }],
                }
            return {}
        transport._request = _fake_hl_request

        with patch(
            "lightfee.venues.hyperliquid_private_ws.websockets.connect",
            side_effect=_fake_connect_awaitable(_FakeWebSocket(
                messages=[
                    json.dumps({
                        "channel": "orderUpdates",
                        "data": [{
                            "order": {
                                "coin": "ETH",
                                "oid": 999101,
                                "cloid": "hl-lifecycle-1",
                                "filledSz": "0.0",
                                "limitPx": "2150.0",
                                "timestamp": 1700000000000,
                            },
                            "status": "open",
                        }],
                    }),
                ],
                close_after=1,
            )),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.2)
            transport.stop_private_ws()
            await _sleep_short()

        # hydrate + connect + subscribe + message successes
        assert transport._success_count >= 2

    @pytest.mark.asyncio
    async def test_failure_on_connect_error(self):
        """connect failure → record_private_ws_failure."""
        transport = _FakeTransport(venue=Venue.HYPERLIQUID)
        transport._spec.rest_url = "https://api.hyperliquid.xyz"
        transport._credential.account_address = "0x1234"

        async def _fake_hl_request(method, path, **kwargs):
            return {}
        transport._request = _fake_hl_request

        with patch(
            "lightfee.venues.hyperliquid_private_ws.websockets.connect",
            side_effect=RuntimeError("simulated connect error"),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.2)
            transport.stop_private_ws()
            await _sleep_short()

        assert transport._failure_count >= 1
        assert "connect" in transport._last_error.lower()

    @pytest.mark.asyncio
    async def test_nodata_error_triggers_failure(self):
        """NoData error → record_private_ws_failure → worker rebuilds."""
        transport = _FakeTransport(venue=Venue.HYPERLIQUID)
        transport._spec.rest_url = "https://api.hyperliquid.xyz"
        transport._credential.account_address = "0x1234"

        async def _fake_hl_request(method, path, **kwargs):
            return {}
        transport._request = _fake_hl_request

        with patch(
            "lightfee.venues.hyperliquid_private_ws.websockets.connect",
            side_effect=_fake_connect_awaitable(_FakeWebSocket(
                messages=[
                    json.dumps({"error": "No data"}),
                ],
                close_after=1,
            )),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.2)
            transport.stop_private_ws()
            await _sleep_short()

        # NoData error → failure recorded + worker exits loop → reconnect → connect failure or another failure
        assert transport._failure_count >= 1
        assert "No data" in transport._last_error

    @pytest.mark.asyncio
    async def test_user_fill_event(self):
        """user fill event → private state updated via worker loop."""
        transport = _FakeTransport(venue=Venue.HYPERLIQUID)
        transport._spec.rest_url = "https://api.hyperliquid.xyz"
        transport._credential.account_address = "0x1234"

        async def _fake_hl_request(method, path, **kwargs):
            return {}
        transport._request = _fake_hl_request

        # _venue_symbol is identity → symbol_map maps ETHUSDT→ETHUSDT
        # Must use ETHUSDT as coin in test message
        with patch(
            "lightfee.venues.hyperliquid_private_ws.websockets.connect",
            side_effect=_fake_connect_awaitable(_FakeWebSocket(
                messages=[
                    json.dumps({
                        "channel": "user",
                        "data": {
                            "fills": [{
                                "coin": "ETHUSDT",
                                "oid": 999102,
                                "cloid": "hl-fill-lifecycle",
                                "filledSz": "0.2",
                                "px": "2150.00",
                                "fee": "0.002",
                                "time": 1700000001000,
                            }],
                        },
                    }),
                ],
                close_after=1,
            )),
        ):
            transport.start_private_ws(["ETHUSDT"])
            await _sleep_short()
            await asyncio.sleep(0.3)
            transport.stop_private_ws()
            await _sleep_short()

        update = transport.private_ws_state.order_by_order_id("999102")
        assert update is not None, f"order 999102 should be in private state after user fill event"
        assert update.filled_quantity == 0.2
        assert update.average_price == 2150.0


# ============================================================================
# OKX ct_val_map unit tests
# ============================================================================


class TestOkxCtValMap:
    """OKX _build_okx_ct_val_map: canonical symbol lookup + default 1.0."""

    def test_canonical_symbol_lookup(self):
        """metadata keyed by canonical symbol → finds ct_val via value in symbol_map."""
        from lightfee.venues.okx_private_ws import _build_okx_ct_val_map

        transport = _FakeTransport(venue=Venue.OKX)
        transport._symbol_metadata = {"ETHUSDT": {"ct_val": 0.1}}
        symbol_map = {"ETH-USDT-SWAP": "ETHUSDT"}

        result = _build_okx_ct_val_map(transport, symbol_map)
        assert result == {"ETH-USDT-SWAP": 0.1}

    def test_vendor_key_fallback(self):
        """metadata keyed by vendor symbol → finds ct_val via key in symbol_map (fallback)."""
        from lightfee.venues.okx_private_ws import _build_okx_ct_val_map

        transport = _FakeTransport(venue=Venue.OKX)
        transport._symbol_metadata = {"ETH-USDT-SWAP": {"ct_val": 0.01}}
        symbol_map = {"ETH-USDT-SWAP": "ETHUSDT"}

        result = _build_okx_ct_val_map(transport, symbol_map)
        assert result == {"ETH-USDT-SWAP": 0.01}

    def test_missing_metadata_defaults_to_one(self):
        """metadata missing → no trusted ct_val entry is produced."""
        from lightfee.venues.okx_private_ws import _build_okx_ct_val_map

        transport = _FakeTransport(venue=Venue.OKX)
        transport._symbol_metadata = {}
        symbol_map = {"ETH-USDT-SWAP": "ETHUSDT", "BTC-USDT-SWAP": "BTCUSDT"}

        result = _build_okx_ct_val_map(transport, symbol_map)
        assert result == {}

    def test_mixed_metadata(self):
        """some metadata present, some missing → only trusted ct_val entries are kept."""
        from lightfee.venues.okx_private_ws import _build_okx_ct_val_map

        transport = _FakeTransport(venue=Venue.OKX)
        transport._symbol_metadata = {"ETHUSDT": {"ct_val": 0.1}}
        symbol_map = {
            "ETH-USDT-SWAP": "ETHUSDT",
            "BTC-USDT-SWAP": "BTCUSDT",
        }

        result = _build_okx_ct_val_map(transport, symbol_map)
        assert result == {"ETH-USDT-SWAP": 0.1}

    def test_zero_ct_val_ignored_defaults_to_one(self):
        """ct_val=0 in metadata is ignored and does not fallback to 1.0."""
        from lightfee.venues.okx_private_ws import _build_okx_ct_val_map

        transport = _FakeTransport(venue=Venue.OKX)
        transport._symbol_metadata = {"ETHUSDT": {"ct_val": 0}}
        symbol_map = {"ETH-USDT-SWAP": "ETHUSDT"}

        result = _build_okx_ct_val_map(transport, symbol_map)
        assert result == {}


# ============================================================================
# Runtime private symbols collection tests
# ============================================================================


class TestRuntimePrivateSymbols:
    """_current_tracked_private_symbols: pending_passive_closes, tracked pairs, etc."""

    def _make_runtime(self):
        from lightfee.engine.state import EngineState
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig
        import tempfile
        import os

        tmpdir = tempfile.mkdtemp()
        config = AppConfig(
            runtime=RuntimeConfig(mode="live"),
            persistence=PersistenceConfig(
                event_log_path=os.path.join(tmpdir, "test_private_symbols.jsonl"),
                snapshot_path=os.path.join(tmpdir, "test_private_symbols_snap.jsonl"),
            ),
        )
        rt = LiveRuntime(config)
        return rt

    def test_pending_passive_closes_produces_symbols(self):
        """Only pending_passive_closes with position_snapshot → returns long/short venue symbols."""
        from lightfee.engine.state import OpenPosition, PendingPassiveClose

        rt = self._make_runtime()
        pos = OpenPosition(
            position_id="pos-1",
            symbol="ETHUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            long_quantity=1.0,
            short_quantity=1.0,
            long_entry_price=2000.0,
            short_entry_price=2000.0,
            opened_at_ms=1000,
        )
        rt.state.pending_passive_closes["ppc-1"] = PendingPassiveClose(
            position_id="pos-1",
            reason="test",
            position_snapshot=pos,
        )

        result = rt._current_tracked_private_symbols()
        assert Venue.BINANCE in result
        assert "ETHUSDT" in result[Venue.BINANCE]
        assert Venue.BYBIT in result
        assert "ETHUSDT" in result[Venue.BYBIT]

    def test_pending_passive_closes_without_snapshot_falls_back_to_open_positions(self):
        """When position_snapshot is None, resolve via open_positions by position_id."""
        from lightfee.engine.state import OpenPosition, PendingPassiveClose

        rt = self._make_runtime()
        pos = OpenPosition(
            position_id="pos-2",
            symbol="BTCUSDT",
            long_venue=Venue.OKX,
            short_venue=Venue.BYBIT,
            long_quantity=0.1,
            short_quantity=0.1,
            long_entry_price=60000.0,
            short_entry_price=60000.0,
            opened_at_ms=1000,
        )
        rt.state.open_positions["pos-2"] = pos
        rt.state.pending_passive_closes["ppc-2"] = PendingPassiveClose(
            position_id="pos-2",
            reason="test",
            position_snapshot=None,
        )

        result = rt._current_tracked_private_symbols()
        assert Venue.OKX in result
        assert "BTCUSDT" in result[Venue.OKX]
        assert Venue.BYBIT in result
        assert "BTCUSDT" in result[Venue.BYBIT]

    def test_tracked_pair_ids_parsed_correctly(self):
        """Canonical pair_id format 'sym:long->short' → correct venue symbols.

        Uppercase pair_id (backward compat / direct construction) still works.
        """
        rt = self._make_runtime()
        rt._tracked_primary_pair_ids = {"ETHUSDT:binance->bybit"}

        result = rt._current_tracked_private_symbols()
        assert Venue.BINANCE in result, f"got venues: {list(result.keys())}"
        assert "ETHUSDT" in result[Venue.BINANCE], f"got symbols: {result[Venue.BINANCE]}"
        assert Venue.BYBIT in result
        assert "ETHUSDT" in result[Venue.BYBIT]

    def test_tracked_pair_ids_lowercase_from_make_candidate_pair_id(self):
        """make_candidate_pair_id() produces lowercase pair_id → runtime canonicalizes to uppercase.

        This is the ROOT FIX for C-R2 private WS symbol mismatch:
        make_candidate_pair_id("ETHUSDT","binance","bybit") → "ethusdt:binance->bybit"
        _current_tracked_private_symbols() must output "ETHUSDT" (uppercase), not "ethusdt".
        """
        from lightfee.engine.entry_local_l2 import make_candidate_pair_id

        # Prove make_candidate_pair_id produces lowercase symbol
        pair_id = make_candidate_pair_id("ETHUSDT", "binance", "bybit")
        assert pair_id == "ethusdt:binance->bybit", (
            f"make_candidate_pair_id must lowercase the symbol: got {pair_id}"
        )

        rt = self._make_runtime()
        rt._tracked_primary_pair_ids = {pair_id}

        result = rt._current_tracked_private_symbols()
        assert Venue.BINANCE in result, f"got venues: {list(result.keys())}"
        assert "ETHUSDT" in result[Venue.BINANCE], (
            f"BUG: lowercase pair_id produced lowercase symbol in private WS: "
            f"{result.get(Venue.BINANCE, set())}"
        )
        assert "ethusdt" not in result.get(Venue.BINANCE, set()), (
            "lowercase symbol must NOT leak into private WS symbols"
        )
        assert Venue.BYBIT in result
        assert "ETHUSDT" in result[Venue.BYBIT]
        assert "ethusdt" not in result.get(Venue.BYBIT, set())

    def test_pipe_delimited_pair_id_also_canonicalizes(self):
        """Pipe-delimited fallback also canonicalizes lowercase to uppercase."""
        rt = self._make_runtime()
        rt._tracked_primary_pair_ids = {"ethusdt|binance|bybit"}

        result = rt._current_tracked_private_symbols()
        assert Venue.BINANCE in result
        assert "ETHUSDT" in result[Venue.BINANCE]
        assert "ethusdt" not in result.get(Venue.BINANCE, set())
        assert Venue.BYBIT in result
        assert "ETHUSDT" in result[Venue.BYBIT]

    def test_okx_pair_id_produces_canonical_symbol_for_venue_conversion(self):
        """OKX pair_id lowercase → canonical uppercase → _venue_symbol can convert.

        make_candidate_pair_id("ETHUSDT","okx","binance") → "ethusdt:okx->binance"
        Runtime must output "ETHUSDT" so that transport._venue_symbol("ETHUSDT")
        can map to "ETH-USDT-SWAP" for the private WS subscribe message.
        """
        from lightfee.engine.entry_local_l2 import make_candidate_pair_id

        pair_id = make_candidate_pair_id("ETHUSDT", "okx", "binance")
        assert pair_id == "ethusdt:okx->binance"

        rt = self._make_runtime()
        rt._tracked_primary_pair_ids = {pair_id}

        result = rt._current_tracked_private_symbols()
        assert Venue.OKX in result
        assert "ETHUSDT" in result[Venue.OKX]
        assert "ethusdt" not in result.get(Venue.OKX, set())
        assert Venue.BINANCE in result
        assert "ETHUSDT" in result[Venue.BINANCE]

    def test_gate_and_hyperliquid_pair_ids_also_canonicalize(self):
        """Gate/Hyperliquid pair_ids also canonicalize lowercase → uppercase."""
        from lightfee.engine.entry_local_l2 import make_candidate_pair_id

        # Gate
        pair_id_gate = make_candidate_pair_id("BTCUSDT", "gate", "bybit")
        assert pair_id_gate == "btcusdt:gate->bybit"
        rt = self._make_runtime()
        rt._tracked_primary_pair_ids = {pair_id_gate}
        result = rt._current_tracked_private_symbols()
        assert Venue.GATE in result
        assert "BTCUSDT" in result[Venue.GATE]
        assert "btcusdt" not in result.get(Venue.GATE, set())

        # Hyperliquid
        rt2 = self._make_runtime()
        pair_id_hl = make_candidate_pair_id("SOLUSDT", "hyperliquid", "binance")
        assert pair_id_hl == "solusdt:hyperliquid->binance"
        rt2._tracked_primary_pair_ids = {pair_id_hl}
        result2 = rt2._current_tracked_private_symbols()
        assert Venue.HYPERLIQUID in result2
        assert "SOLUSDT" in result2[Venue.HYPERLIQUID]
        assert "solusdt" not in result2.get(Venue.HYPERLIQUID, set())

    def test_multiple_pair_ids_mixed_case_produce_unique_canonical(self):
        """Multiple pair_ids with different cases merge to single canonical symbol set."""
        from lightfee.engine.entry_local_l2 import make_candidate_pair_id

        pair_id_lower = make_candidate_pair_id("ETHUSDT", "binance", "bybit")
        rt = self._make_runtime()
        rt._tracked_primary_pair_ids = {
            pair_id_lower,  # "ethusdt:binance->bybit"
            "ETHUSDT:binance->bybit",  # uppercase (backward compat)
        }

        result = rt._current_tracked_private_symbols()
        assert Venue.BINANCE in result
        # Both should canonicalize to the same set entry
        assert result[Venue.BINANCE] == {"ETHUSDT", }

    def test_open_positions_produces_symbols(self):
        """open_positions with long/short venue → correct venue symbols."""
        from lightfee.engine.state import OpenPosition

        rt = self._make_runtime()
        rt.state.open_positions["pos-3"] = OpenPosition(
            position_id="pos-3",
            symbol="SOLUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            long_quantity=10.0,
            short_quantity=10.0,
            long_entry_price=100.0,
            short_entry_price=100.0,
            opened_at_ms=1000,
        )

        result = rt._current_tracked_private_symbols()
        assert Venue.BINANCE in result
        assert "SOLUSDT" in result[Venue.BINANCE]
        assert Venue.OKX in result
        assert "SOLUSDT" in result[Venue.OKX]

    def test_empty_state_returns_empty_dict(self):
        """No positions/entries/pairs → empty result."""
        rt = self._make_runtime()
        result = rt._current_tracked_private_symbols()
        assert result == {}

    def test_pending_entries_produces_symbols(self):
        """pending_entries with symbol and venues → correct venue symbols."""
        from lightfee.engine.state import PendingEntry

        rt = self._make_runtime()
        rt.state.pending_entries["entry-1"] = PendingEntry(
            pending_id="entry-1",
            symbol="AVAXUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.BYBIT,
            target_quantity=5.0,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=1000,
        )

        result = rt._current_tracked_private_symbols()
        assert Venue.BINANCE in result, f"got venues: {list(result.keys())}"
        assert "AVAXUSDT" in result[Venue.BINANCE]
        assert Venue.BYBIT in result
        assert "AVAXUSDT" in result[Venue.BYBIT]


# ============================================================================
# Parser regression: lowercase pair_id → runtime → symbol_map → parser match
# ============================================================================


class TestLowercasePairIdSymbolMapRegression:
    """Prove that lowercase pair_id from make_candidate_pair_id() does NOT cause
    parser symbol_map misses after the runtime canonicalizes symbols.

    The bug: make_candidate_pair_id("ETHUSDT","binance","bybit") → "ethusdt:...".
    Without canonicalization, _current_tracked_private_symbols() outputs "ethusdt",
    start_binance_private_ws builds symbol_map = {"ethusdt": "ethusdt"}, and
    Binance TRADE_LITE "s":"ETHUSDT" → symbol_map.get("ETHUSDT") → None → DROPPED.
    """

    @pytest.mark.asyncio
    async def test_binance_trade_lite_canonical_symbol_map_match(self):
        """Binance TRADE_LITE s='ETHUSDT' hits symbol_map built from runtime canonical output."""
        from lightfee.venues.binance_private_ws import handle_binance_private_message

        # Simulate what runtime DOES: canonical uppercase symbols
        canonical_symbols = ["ETHUSDT"]
        symbol_map = {s: s for s in canonical_symbols}  # Binance identity
        state = PrivateWsState()

        raw = json.dumps({
            "e": "TRADE_LITE",
            "s": "ETHUSDT",
            "i": 999001,
            "c": "regression-client-1",
            "l": "0.05",
            "L": "2150.00",
            "T": 1700000000000,
        })
        handle_binance_private_message(state, symbol_map, raw)
        await _sleep_short()
        update = state.order_by_order_id("999001")
        assert update is not None, (
            "BUG: Binance TRADE_LITE ETHUSDT should match symbol_map "
            "when runtime outputs canonical uppercase symbols"
        )
        assert update.filled_quantity == 0.05

    @pytest.mark.asyncio
    async def test_lowercase_symbol_map_causes_binance_parser_miss(self):
        """Demonstrate the bug: lowercase symbol_map → Binance TRADE_LITE 'ETHUSDT' misses."""
        from lightfee.venues.binance_private_ws import handle_binance_private_message

        # Old bug: runtime passes lowercase symbols directly
        lowercase_symbols = ["ethusdt"]
        buggy_symbol_map = {s: s for s in lowercase_symbols}
        state = PrivateWsState()

        raw = json.dumps({
            "e": "TRADE_LITE",
            "s": "ETHUSDT",
            "i": 999002,
            "c": "regression-client-2",
            "l": "0.05",
            "L": "2150.00",
            "T": 1700000000000,
        })
        handle_binance_private_message(state, buggy_symbol_map, raw)
        await _sleep_short()
        update = state.order_by_order_id("999002")
        assert update is None, (
            "This is the OLD BUG: lowercase symbol_map misses uppercase exchange push. "
            "If this assertion fails (update is not None), the parser now handles "
            "case-insensitive matching — remove this test and update the audit."
        )

    @pytest.mark.asyncio
    async def test_aster_trade_lite_canonical_symbol_map_match(self):
        """Aster TRADE_LITE s='ETHUSDT' hits symbol_map built from runtime canonical output."""
        from lightfee.venues.aster_private_ws import handle_aster_private_message

        canonical_symbols = ["ETHUSDT"]
        symbol_map = {s: s for s in canonical_symbols}
        state = PrivateWsState()

        raw = json.dumps({
            "e": "TRADE_LITE",
            "s": "ETHUSDT",
            "i": 999003,
            "c": "aster-regression-1",
            "l": "0.03",
            "L": "2151.00",
            "T": 1700000000000,
        })
        handle_aster_private_message(state, symbol_map, raw)
        await _sleep_short()
        update = state.order_by_client_id("aster-regression-1")
        assert update is not None, (
            "BUG: Aster TRADE_LITE ETHUSDT should match canonical symbol_map"
        )
        assert update.filled_quantity == 0.03

    @pytest.mark.asyncio
    async def test_bybit_execution_canonical_symbol_map_match(self):
        """Bybit execution topic s='ETHUSDT' hits symbol_map built from canonical output."""
        from lightfee.venues.bybit_private_ws import handle_bybit_private_message

        canonical_symbols = ["ETHUSDT"]
        symbol_map = {s: s for s in canonical_symbols}
        state = PrivateWsState()

        raw = json.dumps({
            "topic": "execution",
            "data": [{
                "symbol": "ETHUSDT",
                "orderId": "bybit-regr-1",
                "orderLinkId": "bybit-regr-client-1",
                "execQty": "0.02",
                "execPrice": "2150.00",
                "execTime": "1700000000000",
            }],
        })
        handle_bybit_private_message(state, symbol_map, raw, subscribed=True)
        await _sleep_short()
        update = state.order_by_order_id("bybit-regr-1")
        assert update is not None, (
            "BUG: Bybit execution ETHUSDT should match canonical symbol_map"
        )
        assert update.filled_quantity == 0.02

    @pytest.mark.asyncio
    async def test_lowercase_symbol_map_causes_bybit_parser_miss(self):
        """Demonstrate the bug: lowercase symbol_map → Bybit execution 'ETHUSDT' misses."""
        from lightfee.venues.bybit_private_ws import handle_bybit_private_message

        lowercase_symbols = ["ethusdt"]
        buggy_symbol_map = {s: s for s in lowercase_symbols}
        state = PrivateWsState()

        raw = json.dumps({
            "topic": "execution",
            "data": [{
                "symbol": "ETHUSDT",
                "orderId": "bybit-regr-miss-1",
                "orderLinkId": "bybit-regr-miss-client-1",
                "execQty": "0.02",
                "execPrice": "2150.00",
                "execTime": "1700000000000",
            }],
        })
        handle_bybit_private_message(state, buggy_symbol_map, raw, subscribed=True)
        await _sleep_short()
        update = state.order_by_order_id("bybit-regr-miss-1")
        assert update is None, (
            "OLD BUG continued: lowercase symbol_map misses Bybit execution match"
        )

    @pytest.mark.asyncio
    async def test_end_to_end_make_candidate_pair_id_to_parser_resolution(self):
        """Full path: make_candidate_pair_id → runtime canonicalize → parser resolves.

        This is the definitive regression test proving C-R2 lowercase symbol bug is fixed.
        """
        from lightfee.engine.entry_local_l2 import make_candidate_pair_id
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.engine.state import EngineState
        from lightfee.config.schema import AppConfig, PersistenceConfig, RuntimeConfig
        import tempfile, os

        # 1. make_candidate_pair_id produces lowercase pair_id
        pair_id = make_candidate_pair_id("ETHUSDT", "binance", "bybit")
        assert "ethusdt" in pair_id, f"pair_id must contain lowercase symbol: {pair_id}"

        # 2. Build runtime with this real tracked pair_id
        tmpdir = tempfile.mkdtemp()
        config = AppConfig(
            runtime=RuntimeConfig(mode="live"),
            persistence=PersistenceConfig(
                event_log_path=os.path.join(tmpdir, "test_e2e.jsonl"),
                snapshot_path=os.path.join(tmpdir, "test_e2e_snap.jsonl"),
            ),
        )
        rt = LiveRuntime(config)
        rt._tracked_primary_pair_ids = {pair_id}

        # 3. Runtime canonicalizes → outputs uppercase
        symbols = rt._current_tracked_private_symbols()
        assert Venue.BINANCE in symbols
        assert "ETHUSDT" in symbols[Venue.BINANCE], (
            f"C-R2 ROOT CAUSE: runtime failed to canonicalize {pair_id!r} "
            f"→ got {symbols.get(Venue.BINANCE, set())!r}"
        )
        assert "ethusdt" not in symbols.get(Venue.BINANCE, set()), (
            f"C-R2 BUG: lowercase 'ethusdt' leaked into private WS symbols from {pair_id!r}"
        )
        assert Venue.BYBIT in symbols
        assert "ETHUSDT" in symbols[Venue.BYBIT]

        # 4. Build symbol_map as start_binance_private_ws would (identity venue)
        binance_symbols = sorted(symbols[Venue.BINANCE])
        symbol_map = {s: s for s in binance_symbols}
        assert symbol_map == {"ETHUSDT": "ETHUSDT"}, (
            f"symbol_map must use canonical uppercase: got {symbol_map}"
        )

        # 5. Binance parser resolves exchange push correctly
        from lightfee.venues.binance_private_ws import handle_binance_private_message

        state = PrivateWsState()
        raw = json.dumps({
            "e": "TRADE_LITE",
            "s": "ETHUSDT",
            "i": 888888,
            "c": "e2e-client-1",
            "l": "0.10",
            "L": "2160.00",
            "T": 1700000000000,
        })
        handle_binance_private_message(state, symbol_map, raw)
        await _sleep_short()
        update = state.order_by_order_id("888888")
        assert update is not None, (
            "C-R2 END-TO-END: Binance TRADE_LITE ETHUSDT resolved correctly "
            "after make_candidate_pair_id → runtime canonicalize → symbol_map"
        )
        assert update.filled_quantity == 0.10
        assert update.average_price == 2160.0


# ============================================================================
# M-R8: Bitget V1 private WS parser field compatibility + fallback
# ============================================================================


class TestBitgetV1PrivateOrderParser:
    """M-R8: Bitget private WS must parse V1 fixture fields (ordId, clientOid,
    baseVolume, priceAvg, fee, uTime/cTime/updateTime) + normalize_contract_symbol
    fallback when symbol_map misses.
    """

    @pytest.mark.asyncio
    async def test_v1_fixture_instId_btcusdt_ordId_baseVolume_priceAvg_fee(self):
        """V1 fixture: instId=BTCUSDT, ordId=123, baseVolume=0.5, priceAvg=65000, fee=-1.2."""
        from lightfee.venues.bitget_private_ws import handle_bitget_private_message

        state = PrivateWsState()
        symbol_map = {"BTCUSDT": "BTCUSDT"}
        raw = json.dumps({
            "arg": {"channel": "orders", "instType": "USDT-FUTURES"},
            "data": [{
                "instId": "BTCUSDT",
                "ordId": "123",
                "baseVolume": "0.5",
                "priceAvg": "65000",
                "fee": "-1.2",
                "uTime": "1700000001000",
            }],
        })
        handle_bitget_private_message(state, symbol_map, raw, subscribed=True)
        await _sleep_short()
        update = state.order_by_order_id("123")
        assert update is not None, "V1 fixture must record private order update"
        assert update.filled_quantity == 0.5
        assert update.average_price == 65000
        assert update.fee_quote == 1.2  # V1: abs(fee)
        assert update.updated_at_ms == 1700000001000

    @pytest.mark.asyncio
    async def test_v1_fixture_clientOid_and_orderId_variants(self):
        """V1 field: clientOid=client-abc, orderId=456, fillQty=0.3, fillPriceAvg=64000."""
        from lightfee.venues.bitget_private_ws import handle_bitget_private_message

        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({
                        "arg": {"channel": "orders", "instType": "USDT-FUTURES"},
            "data": [{
                "instId": "ETHUSDT",
                "orderId": "456",
                "clientOid": "client-abc",
                "fillQty": "0.3",
                "fillPriceAvg": "64000",
                "filledFee": "0.96",
                "cTime": "1700000002000",
            }],
        })
        handle_bitget_private_message(state, symbol_map, raw, subscribed=True)
        await _sleep_short()
        update = state.order_by_client_id("client-abc")
        assert update is not None
        assert update.order_id == "456"
        assert update.filled_quantity == 0.3
        assert update.average_price == 64000
        assert update.fee_quote == 0.96

    @pytest.mark.asyncio
    async def test_v1_fixture_clOrdId_and_size_avgPrice(self):
        """V1 field: clOrdId=cid-789, size=0.15, avgPrice=43000, totalFee=0.5."""
        from lightfee.venues.bitget_private_ws import handle_bitget_private_message

        state = PrivateWsState()
        symbol_map = {"BTCUSDT": "BTCUSDT"}
        raw = json.dumps({
                        "arg": {"channel": "orders", "instType": "USDT-FUTURES"},
            "data": [{
                "instId": "BTCUSDT",
                "ordId": "789",
                "clOrdId": "cid-789",
                "size": "0.15",
                "avgPrice": "43000",
                "totalFee": "0.5",
                "updateTime": "1700000003000",
            }],
        })
        handle_bitget_private_message(state, symbol_map, raw, subscribed=True)
        await _sleep_short()
        update = state.order_by_client_id("cid-789")
        assert update is not None
        assert update.filled_quantity == 0.15
        assert update.average_price == 43000
        assert update.fee_quote == 0.5
        assert update.updated_at_ms == 1700000003000

    @pytest.mark.asyncio
    async def test_symbol_map_miss_fallback_normalize_contract_symbol(self):
        """When symbol_map has no 'BTCUSDT', V1 normalize_contract_symbol fallback
        maps it to canonical 'BTCUSDT' instead of dropping the message."""
        from lightfee.venues.bitget_private_ws import handle_bitget_private_message

        state = PrivateWsState()
        symbol_map = {}  # No BTCUSDT mapping → fallback
        raw = json.dumps({
                        "arg": {"channel": "orders", "instType": "USDT-FUTURES"},
            "data": [{
                "instId": "BTCUSDT",
                "ordId": "ncs-001",
                "baseVolume": "0.8",
                "priceAvg": "66000",
                "fee": "-1.5",
                "uTime": "1700000004000",
            }],
        })
        handle_bitget_private_message(state, symbol_map, raw, subscribed=True)
        await _sleep_short()
        update = state.order_by_order_id("ncs-001")
        assert update is not None, (
            "symbol_map miss must NOT drop message; "
            "normalize_contract_symbol should map 'BTCUSDT' → 'BTCUSDT'"
        )
        assert update.filled_quantity == 0.8

    @pytest.mark.asyncio
    async def test_symbol_map_miss_with_dash_underscore_normalized(self):
        """V1 normalize_contract_symbol: strip, uppercase, remove _ and -."""
        from lightfee.venues.bitget_private_ws import handle_bitget_private_message

        state = PrivateWsState()
        symbol_map = {}  # Empty → fallback for all
        raw = json.dumps({
                        "arg": {"channel": "orders", "instType": "USDT-FUTURES"},
            "data": [{
                "instId": "btc-usdt_swap",
                "ordId": "ncs-002",
                "baseVolume": "0.2",
                "priceAvg": "67000",
                "uTime": "1700000005000",
            }],
        })
        handle_bitget_private_message(state, symbol_map, raw, subscribed=True)
        await _sleep_short()
        update = state.order_by_order_id("ncs-002")
        assert update is not None, (
            "symbol with dash/underscore must normalize: "
            "'btc-usdt_swap' → 'BTCUSDTSWAP'"
        )

    @pytest.mark.asyncio
    async def test_position_symbol_map_miss_fallback(self):
        """Position updates also use normalize_contract_symbol fallback."""
        from lightfee.venues.bitget_private_ws import handle_bitget_private_message

        state = PrivateWsState()
        symbol_map = {}  # Empty → fallback
        raw = json.dumps({
                        "arg": {"channel": "positions", "instType": "USDT-FUTURES"},
            "data": [{
                "instId": "BTCUSDT",
                "total": "0.05",
                "holdSide": "long",
                "uTime": str(_now_ms()),
            }],
        })
        handle_bitget_private_message(state, symbol_map, raw, subscribed=True)
        await asyncio.sleep(0.1)  # Allow create_task to process
        pos = state.position_if_fresh("BTCUSDT", 30_000, _now_ms())
        assert pos is not None
        assert pos.size > 0  # long → positive signed size

    @pytest.mark.asyncio
    async def test_private_cache_persists_order_for_passive_progress(self):
        """After record_order, private state returns order progress for the symbol."""
        from lightfee.venues.bitget_private_ws import handle_bitget_private_message

        state = PrivateWsState()
        symbol_map = {"BTCUSDT": "BTCUSDT"}
        raw = json.dumps({
                        "arg": {"channel": "orders", "instType": "USDT-FUTURES"},
            "data": [{
                "instId": "BTCUSDT",
                "ordId": "pp-001",
                "clientOid": "cid-pp-001",
                "baseVolume": "0.6",
                "priceAvg": "65000",
                "fee": "-1.0",
                "uTime": str(_now_ms()),
            }],
        })
        handle_bitget_private_message(state, symbol_map, raw, subscribed=True)
        await _sleep_short()

        # Query via client order ID
        progress = state.order_progress_if_fresh(
            client_order_id="cid-pp-001", max_age_ms=30_000,
        )
        assert progress is not None, "Private progress must be queryable from cache"
        assert progress.cumulative_quantity == 0.6
        assert progress.average_price == 65000

    @pytest.mark.asyncio
    async def test_zero_fill_terminal_order_still_recorded(self):
        """V1: 0-fill CANCELED orders are recorded — terminal state with 0 fill
        is authoritative evidence the order is done."""
        from lightfee.venues.bitget_private_ws import handle_bitget_private_message

        state = PrivateWsState()
        symbol_map = {"BTCUSDT": "BTCUSDT"}
        raw = json.dumps({
                        "arg": {"channel": "orders", "instType": "USDT-FUTURES"},
            "data": [{
                "instId": "BTCUSDT",
                "ordId": "zero-cxl-001",
                "baseVolume": "0",
                "avgPrice": "0",
                "status": "CANCELED",
                "uTime": str(_now_ms()),
            }],
        })
        handle_bitget_private_message(state, symbol_map, raw, subscribed=True)
        await _sleep_short()
        update = state.order_by_order_id("zero-cxl-001")
        assert update is not None, "0-fill terminal order must still be recorded"
        # V1: handle_bitget_private_message for Bitget explicitly sets state=None
        # (bitget.rs:4915). State is resolved later via REST detail during merge.
        assert update.filled_quantity == 0.0


# ============================================================================
# Runtime private worker ownership regression (CL-161)
# ============================================================================


class _LifecycleTransport:
    def __init__(self) -> None:
        self.starts: list[list[str]] = []
        self.stops = 0
        self.updates: list[list[str]] = []

    def start_private_ws(self, symbols: list[str]) -> None:
        self.starts.append(list(symbols))

    def stop_private_ws(self) -> None:
        self.stops += 1

    def update_private_ws_symbols(self, symbols: list[str]) -> set[str]:
        self.updates.append(list(symbols))
        return set(symbols)


class _LifecycleAdapter:
    supports_private_health = True

    def __init__(self, transport: _LifecycleTransport) -> None:
        self._transport = transport


class TestRuntimePrivateWsLifecycle:
    """Candidate changes must not own the private account stream lifecycle."""

    @staticmethod
    def _runtime_with_transport(transport: _LifecycleTransport):
        from lightfee.config.schema import AppConfig, RuntimeConfig
        from lightfee.engine.runtime import LiveRuntime

        runtime = LiveRuntime(AppConfig(runtime=RuntimeConfig(mode="live")))
        runtime._venue_adapters = {
            Venue.BINANCE: _LifecycleAdapter(transport),
        }
        journal_events: list[tuple[str, dict]] = []
        runtime.journal.append = lambda kind, payload, **_kwargs: journal_events.append(
            (kind, payload)
        )
        return runtime, journal_events

    def test_empty_candidate_interval_never_stops_existing_worker(self):
        transport = _LifecycleTransport()
        runtime, journal_events = self._runtime_with_transport(transport)
        tracked = {Venue.BINANCE: {"ETHUSDT"}}
        runtime._current_tracked_private_symbols = lambda: tracked

        runtime._ensure_private_ws_started(1)
        tracked = {}
        runtime._ensure_private_ws_started(2)
        tracked = {Venue.BINANCE: {"ETHUSDT"}}
        runtime._ensure_private_ws_started(3)

        assert transport.starts == [["ETHUSDT"]]
        assert transport.stops == 0
        assert transport.updates == []
        assert Venue.BINANCE in runtime._private_ws_started
        assert all(kind != "runtime.private_ws_stopped" for kind, _ in journal_events)

    def test_new_symbol_updates_running_worker_in_place(self):
        transport = _LifecycleTransport()
        runtime, _ = self._runtime_with_transport(transport)
        tracked = {Venue.BINANCE: {"ETHUSDT"}}
        runtime._current_tracked_private_symbols = lambda: tracked

        runtime._ensure_private_ws_started(1)
        tracked = {Venue.BINANCE: {"BTCUSDT"}}
        runtime._ensure_private_ws_started(2)

        assert transport.starts == [["ETHUSDT"]]
        assert transport.stops == 0
        assert transport.updates == [["BTCUSDT"]]
        assert runtime._private_ws_symbols[Venue.BINANCE] == {"ETHUSDT", "BTCUSDT"}


class TestPrivateWsSymbolUpdateQueue:
    def test_okx_additions_are_queued_for_in_band_subscription(self):
        from lightfee.venues.specs import get_spec
        from lightfee.venues.transport import VenueTransport

        transport = VenueTransport(get_spec(Venue.OKX), mode="paper")
        transport.start_private_ws(["ETHUSDT"])
        added = transport.update_private_ws_symbols(["BTCUSDT"])

        assert added == {"BTC-USDT-SWAP"}
        assert transport.consume_private_ws_symbol_updates() == {
            "BTC-USDT-SWAP": "BTCUSDT"
        }
        assert transport.consume_private_ws_symbol_updates() == {}

    def test_binance_additions_do_not_queue_a_reconnect_or_subscription(self):
        from lightfee.venues.specs import get_spec
        from lightfee.venues.transport import VenueTransport

        transport = VenueTransport(get_spec(Venue.BINANCE), mode="paper")
        transport.start_private_ws(["ETHUSDT"])
        added = transport.update_private_ws_symbols(["BTCUSDT"])

        assert added == {"BTCUSDT"}
        assert transport._private_ws_symbol_map == {
            "ETHUSDT": "ETHUSDT",
            "BTCUSDT": "BTCUSDT",
        }
        assert transport.consume_private_ws_symbol_updates() == {}
