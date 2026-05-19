"""V1 venue private WS parser fixture tests — one per venue, matching V1 semantics."""

from __future__ import annotations

import json

import pytest

from lightfee.core.domain import PassiveOrderState
from lightfee.marketdata.private_ws import PrivateWsState
from lightfee.venues.binance_private_ws import handle_binance_private_message
from lightfee.venues.okx_private_ws import handle_okx_private_message
from lightfee.venues.bybit_private_ws import handle_bybit_private_message
from lightfee.venues.bitget_private_ws import handle_bitget_private_message
from lightfee.venues.gate_private_ws import handle_gate_private_message
from lightfee.venues.aster_private_ws import handle_aster_private_message
from lightfee.venues.hyperliquid_private_ws import _apply_hyperliquid_private_message


# ---------------------------------------------------------------------------
# Binance parser fixtures
# ---------------------------------------------------------------------------


class TestBinancePrivateParser:
    @pytest.mark.asyncio
    async def test_execution_report_new_order(self):
        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({
            "e": "executionReport",
            "s": "ETHUSDT",
            "i": "order-123",
            "c": "client-1",
            "X": "NEW",
            "z": "0", "ap": "0", "n": "0",
            "T": 1700000000000,
        })
        handle_binance_private_message(state, symbol_map, raw)
        await asyncio_sleep_short()
        update = state.order_by_order_id("order-123")
        assert update is not None
        assert update.state == PassiveOrderState.OPEN

    @pytest.mark.asyncio
    async def test_execution_report_full_fill(self):
        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({
            "e": "executionReport",
            "s": "ETHUSDT",
            "i": "order-456",
            "c": "client-2",
            "X": "FILLED",
            "z": "0.05", "ap": "2140.50", "n": "0.001", "N": "USDT",
            "T": 1700000001000,
        })
        handle_binance_private_message(state, symbol_map, raw)
        await asyncio_sleep_short()
        update = state.order_by_order_id("order-456")
        assert update is not None
        assert update.state == PassiveOrderState.FILLED
        assert update.filled_quantity == 0.05
        assert update.average_price == 2140.50
        assert update.fee_quote == 0.001

    @pytest.mark.asyncio
    async def test_execution_report_canceled(self):
        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({
            "e": "executionReport", "s": "ETHUSDT",
            "i": "order-789", "c": "client-3",
            "X": "CANCELED", "z": "0", "ap": "0",
            "T": 1700000002000,
        })
        handle_binance_private_message(state, symbol_map, raw)
        await asyncio_sleep_short()
        update = state.order_by_order_id("order-789")
        assert update is not None
        assert update.state == PassiveOrderState.CANCELED

    @pytest.mark.asyncio
    async def test_unknown_event_ignored(self):
        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({"e": "unknown", "data": {}})
        handle_binance_private_message(state, symbol_map, raw)
        await asyncio_sleep_short()
        assert state.worker_count() == 0  # no crash


# ---------------------------------------------------------------------------
# OKX parser fixtures
# ---------------------------------------------------------------------------


class TestOkxPrivateParser:
    @pytest.mark.asyncio
    async def test_login_ack_triggers_subscribe(self):
        state = PrivateWsState()
        symbol_map = {"ETH-USDT-SWAP": "ETHUSDT"}
        sub_msgs = ['{"op":"subscribe","args":[{"channel":"orders","instType":"SWAP","instId":"ETH-USDT-SWAP"}]}']
        raw = json.dumps({"event": "login", "code": "0"})
        result, subscribed = handle_okx_private_message(state, symbol_map, sub_msgs, raw, False)
        assert result == sub_msgs
        assert subscribed is True

    @pytest.mark.asyncio
    async def test_order_update_partial_fill(self):
        state = PrivateWsState()
        symbol_map = {"ETH-USDT-SWAP": "ETHUSDT"}
        sub_msgs = []
        raw = json.dumps({
            "arg": {"channel": "orders", "instType": "SWAP", "instId": "ETH-USDT-SWAP"},
            "data": [{
                "instId": "ETH-USDT-SWAP",
                "ordId": "okx-order-1",
                "clOrdId": "okx-client-1",
                "accFillSz": "0.5",
                "avgPx": "2140.00",
                "state": "PARTIALLY_FILLED",
                "fillFeeCcy": "USDT",
                "fillFee": "0.002",
                "uTime": "1700000000000",
            }],
        })
        handle_okx_private_message(state, symbol_map, sub_msgs, raw, subscribed=True)
        await asyncio_sleep_short()
        update = state.order_by_order_id("okx-order-1")
        assert update is not None
        assert update.filled_quantity == 0.5
        assert update.average_price == 2140.0
        assert update.fee_quote == 0.002
        assert update.state == PassiveOrderState.PARTIALLY_FILLED

    @pytest.mark.asyncio
    async def test_position_update(self):
        state = PrivateWsState()
        symbol_map = {"ETH-USDT-SWAP": "ETHUSDT"}
        sub_msgs = []
        raw = json.dumps({
            "arg": {"channel": "positions", "instType": "SWAP", "instId": "ETH-USDT-SWAP"},
            "data": [{
                "instId": "ETH-USDT-SWAP",
                "pos": "2.5", "posSide": "long",
                "uTime": "1700000000000",
            }],
        })
        handle_okx_private_message(state, symbol_map, sub_msgs, raw, subscribed=True)
        await asyncio_sleep_short()
        pos = state.position("ETHUSDT")
        assert pos is not None
        assert pos.size == 2.5


# ---------------------------------------------------------------------------
# Bybit parser fixtures
# ---------------------------------------------------------------------------


class TestBybitPrivateParser:
    @pytest.mark.asyncio
    async def test_auth_ack_triggers_subscribe(self):
        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({"op": "auth", "success": True})
        to_send, subscribed = handle_bybit_private_message(state, symbol_map, raw, False)
        assert to_send is not None
        assert subscribed is True

    @pytest.mark.asyncio
    async def test_order_update(self):
        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({
            "topic": "order",
            "data": [{
                "symbol": "ETHUSDT", "orderId": "bybit-1",
                "orderLinkId": "bybit-client-1",
                "cumExecQty": "0.03", "avgPrice": "2140.00",
                "cumExecFee": "0.001", "orderStatus": "PARTIALLYFILLED",
                "updatedTime": "1700000000000",
            }],
        })
        handle_bybit_private_message(state, symbol_map, raw, subscribed=True)
        await asyncio_sleep_short()
        update = state.order_by_order_id("bybit-1")
        assert update is not None
        assert update.filled_quantity == 0.03


# ---------------------------------------------------------------------------
# Bitget parser fixtures
# ---------------------------------------------------------------------------


class TestBitgetPrivateParser:
    @pytest.mark.asyncio
    async def test_login_ack_subscribe(self):
        state = PrivateWsState()
        symbol_map = {"ETHUSDT_UMCBL": "ETHUSDT"}
        raw = json.dumps({"event": "login", "code": "0"})
        to_send, subscribed = handle_bitget_private_message(state, symbol_map, raw, False)
        assert to_send is not None
        assert subscribed is True

    @pytest.mark.asyncio
    async def test_order_update(self):
        state = PrivateWsState()
        symbol_map = {"ETHUSDT_UMCBL": "ETHUSDT"}
        raw = json.dumps({
            "arg": {"channel": "orders", "instId": "ETHUSDT_UMCBL"},
            "data": [{
                "instId": "ETHUSDT_UMCBL", "orderId": "bg-1",
                "clientOid": "bg-client-1",
                "accBaseVolume": "0.02", "avgPrice": "2140.00",
                "status": "PARTIALLY_FILLED",
                "uTime": "1700000000000",
            }],
        })
        handle_bitget_private_message(state, symbol_map, raw, subscribed=True)
        await asyncio_sleep_short()
        update = state.order_by_order_id("bg-1")
        assert update is not None
        assert update.filled_quantity == 0.02


# ---------------------------------------------------------------------------
# Gate parser fixtures
# ---------------------------------------------------------------------------


class TestGatePrivateParser:
    @pytest.mark.asyncio
    async def test_order_update(self):
        state = PrivateWsState()
        symbol_map = {"ETH_USDT": "ETHUSDT"}
        raw = json.dumps({
            "channel": "futures.orders",
            "event": "update",
            "result": [{
                "contract": "ETH_USDT", "id": "gate-1",
                "text": "gate-client-1",
                "fill_total": "0.01", "fill_price": "2140.00",
                "fee": "0.001", "finish_as": "PARTIAL",
                "finish_time_ms": 1700000000000,
            }],
        })
        handle_gate_private_message(state, symbol_map, raw)
        await asyncio_sleep_short()
        update = state.order_by_order_id("gate-1")
        assert update is not None
        assert update.filled_quantity == 0.01


# ---------------------------------------------------------------------------
# Aster parser fixtures
# ---------------------------------------------------------------------------


class TestAsterPrivateParser:
    @pytest.mark.asyncio
    async def test_execution_report(self):
        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({
            "e": "executionReport", "s": "ETHUSDT",
            "i": "aster-1", "c": "aster-client-1",
            "X": "FILLED", "z": "0.05", "ap": "2140.50",
            "n": "0.001", "T": 1700000000000,
        })
        handle_aster_private_message(state, symbol_map, raw)
        await asyncio_sleep_short()
        update = state.order_by_order_id("aster-1")
        assert update is not None
        assert update.state == PassiveOrderState.FILLED


# ---------------------------------------------------------------------------
# Hyperliquid parser fixtures
# ---------------------------------------------------------------------------


class TestHyperliquidPrivateParser:
    @pytest.mark.asyncio
    async def test_user_fill_event(self):
        state = PrivateWsState()
        symbol_map = {"ETH": "ETHUSDT"}
        raw = json.dumps({
            "channel": "user",
            "data": {
                "fills": [{
                    "coin": "ETH", "oid": 12345, "cloid": "hl-client-1",
                    "filledSz": "0.05", "px": "2140.00",
                    "fee": "0.001", "time": 1700000000000,
                }],
            },
        })
        _apply_hyperliquid_private_message(state, symbol_map, raw)
        await asyncio_sleep_short()
        update = state.order_by_order_id("12345")
        assert update is not None
        assert update.filled_quantity == 0.05

    @pytest.mark.asyncio
    async def test_order_update_event(self):
        state = PrivateWsState()
        symbol_map = {"ETH": "ETHUSDT"}
        raw = json.dumps({
            "channel": "orderUpdates",
            "data": [{
                "order": {
                    "coin": "ETH", "oid": 67890, "cloid": "hl-client-2",
                    "filledSz": "0.0", "limitPx": "2150.00",
                    "timestamp": 1700000000000,
                },
                "status": "open",
            }],
        })
        _apply_hyperliquid_private_message(state, symbol_map, raw)
        await asyncio_sleep_short()
        update = state.order_by_order_id("67890")
        assert update is not None
        assert update.state == PassiveOrderState.OPEN


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


async def asyncio_sleep_short():
    import asyncio
    await asyncio.sleep(0.01)
