"""V1 Bitget private WebSocket worker + parser (login + subscribe-based).

Exact semantic port of src/live/bitget.rs private WS paths.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
from typing import Any, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from lightfee.core.domain import PassiveOrderState
from lightfee.marketdata.private_ws import (
    PrivateOrderUpdate,
    _now_ms,
)

logger = logging.getLogger(__name__)

BITGET_PRIVATE_PING_INTERVAL_SECS = 20


def _bitget_private_ws_url(base_url: str) -> str:
    normalized = base_url.rstrip("/")
    if "bitget.com" in normalized:
        return "wss://ws.bitget.com/v2/ws/private"
    if normalized.startswith("https://"):
        return normalized.replace("https://", "wss://") + "/v2/ws/private"
    return normalized


def _bitget_hmac_sha256_base64(secret: str, message: str) -> str:
    mac = hmac.new(
        secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    )
    return base64.b64encode(mac.digest()).decode("utf-8")


def _bitget_passive_order_state(status: str) -> Optional[PassiveOrderState]:
    s = status.upper()
    if s in ("NEW", "INIT", "LIVE"):
        return PassiveOrderState.OPEN
    if s == "PARTIALLY_FILLED":
        return PassiveOrderState.PARTIALLY_FILLED
    if s == "FILLED":
        return PassiveOrderState.FILLED
    if s in ("CANCELED", "CANCELLED"):
        return PassiveOrderState.CANCELED
    return None


def _build_bitget_subscribe(inst_type: str) -> str:
    """V1 build_bitget_subscribe: subscribe to positions + orders channels.

    The current private-channel schema uses ``instId=default`` for the
    account-wide product subscription; it continues to cover symbols added to
    the transport-owned parser map without replacing the socket.
    """
    return json.dumps({
        "op": "subscribe",
        "args": [
            {"instType": inst_type, "channel": "positions", "instId": "default"},
            {"instType": inst_type, "channel": "orders", "instId": "default"},
        ],
    })


def _normalize_contract_symbol(raw: str) -> str:
    """V1 normalize_contract_symbol: strip, uppercase, remove _ and -."""
    return raw.strip().upper().replace("_", "").replace("-", "")


def _json_string(row: dict[str, Any], keys: list[str]) -> str:
    for k in keys:
        v = row.get(k)
        if v is not None:
            return str(v)
    return ""


def _json_f64(row: dict[str, Any], keys: list[str]) -> Optional[float]:
    for k in keys:
        v = row.get(k)
        if v is not None:
            try:
                return float(v)
            except (ValueError, TypeError):
                continue
    return None


def _json_i64(row: dict[str, Any], keys: list[str]) -> Optional[int]:
    for k in keys:
        v = row.get(k)
        if v is not None:
            try:
                val = int(v)
                # V1: auto-convert second-precision timestamps to ms
                if val < 10_000_000_000:
                    return val * 1000
                return val
            except (ValueError, TypeError):
                continue
    return None


def _handle_bitget_order_data(
    data: list[dict[str, Any]],
    symbol_map: dict[str, str],
    private_state,
) -> None:
    """V1 handle_bitget_private_message orders path.

    V1 field compatibility:
    - orderId: ordId/orderId
    - clientOid: clientOid/clOrdId
    - filledQty: baseVolume/filledQty/fillQty/size
    - avgPrice: priceAvg/fillPriceAvg/averagePrice/avgPrice
    - fee: fee/totalFee/filledFee (abs)
    - timestamp: uTime/cTime/updateTime (auto ms conversion)
    """
    loop = asyncio.get_running_loop()
    for row in data:
        venue_symbol = (
            row.get("instId") or row.get("symbol") or ""
        )
        if isinstance(venue_symbol, str):
            venue_symbol = venue_symbol.strip()
        symbol = symbol_map.get(venue_symbol)
        if symbol is None:
            # V1 fallback: normalize_contract_symbol when symbol_map miss
            symbol = _normalize_contract_symbol(venue_symbol)
            if not symbol:
                continue
        order_id = _json_string(row, ["ordId", "orderId"])
        client_id = _json_string(row, ["clientOid", "clOrdId"])
        filled_qty = _json_f64(row, ["accBaseVolume", "baseVolume", "filledQty", "fillQty", "fillSz", "size"]) or 0.0
        avg_price = _json_f64(row, ["priceAvg", "fillPriceAvg", "averagePrice", "avgPrice"])
        fee_quote = _json_f64(row, ["fee", "totalFee", "filledFee"])
        if fee_quote is not None:
            fee_quote = abs(fee_quote)
        else:
            # V1: also try feeDetail.totalFee
            fee_detail = row.get("feeDetail")
            if isinstance(fee_detail, dict):
                fee_quote = _json_f64(fee_detail, ["totalFee"])
                if fee_quote is not None:
                    fee_quote = abs(fee_quote)
        # V1: handle_bitget_private_message explicitly sets state=None for
        # Bitget orders (bitget.rs:4915). State is resolved later via REST
        # detail during merge, not from WS push which may carry stale status.
        ts = _json_i64(row, ["uTime", "cTime", "updateTime"]) or _now_ms()
        update = PrivateOrderUpdate(
            symbol=symbol,
            order_id=order_id,
            client_order_id=client_id if client_id else None,
            filled_quantity=filled_qty,
            average_price=avg_price if avg_price is not None and avg_price > 0 else None,
            fee_quote=fee_quote,
            state=None,
            updated_at_ms=ts,
        )
        loop.create_task(private_state.record_order(update))


def _handle_bitget_position_data(
    data: list[dict[str, Any]],
    symbol_map: dict[str, str],
    private_state,
) -> None:
    """V1 handle_bitget_private_message positions path.

    V1 field compatibility:
    - size: total/available/holdVolume/size (abs)
    - holdSide: holdSide/posSide/hold_mode → signed
    - timestamp: uTime/cTime/updateTime
    """
    loop = asyncio.get_running_loop()
    for row in data:
        venue_symbol = (
            row.get("instId") or row.get("symbol") or ""
        )
        if isinstance(venue_symbol, str):
            venue_symbol = venue_symbol.strip()
        symbol = symbol_map.get(venue_symbol)
        if symbol is None:
            symbol = _normalize_contract_symbol(venue_symbol)
            if not symbol:
                continue
        raw_size = abs(_json_f64(row, ["total", "available", "holdVolume", "size"]) or 0.0)
        hold_side = _json_string(row, ["holdSide", "posSide", "hold_mode"]).lower()
        if hold_side in ("long", "buy"):
            signed = raw_size
        elif hold_side in ("short", "sell"):
            signed = -raw_size
        else:
            signed = raw_size
        ts = _json_i64(row, ["uTime", "cTime", "updateTime"]) or _now_ms()
        loop.create_task(private_state.update_position(symbol, signed, ts))


def handle_bitget_private_message(
    private_state,
    symbol_map: dict[str, str],
    raw: str,
    subscribed: bool = False,
) -> tuple[Optional[str], bool]:
    """V1 handle_bitget_private_message()."""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None, subscribed

    event = payload.get("event", "")
    code = str(payload.get("code", "0"))

    # Login ack → subscribe (V1: positions + orders, no per-symbol instId)
    if event == "login" and code == "0":
        return _build_bitget_subscribe("USDT-FUTURES"), True

    if event == "subscribe":
        return None, subscribed

    if not subscribed:
        return None, subscribed

    arg = payload.get("arg", {})
    channel = arg.get("channel", "")
    data = payload.get("data")
    if not isinstance(data, list):
        return None, subscribed

    if channel == "orders":
        _handle_bitget_order_data(data, symbol_map, private_state)
    elif channel == "positions":
        _handle_bitget_position_data(data, symbol_map, private_state)

    return None, subscribed


async def _bitget_private_ws_loop(
    transport,
    api_key: str,
    api_secret: str,
    api_passphrase: str,
    ws_url: str,
    symbol_map: dict[str, str],
    private_state,
    unhealthy_after_failures: int,
    reconnect_initial_ms: int,
    reconnect_max_ms: int,
) -> None:
    from lightfee.marketdata.resilience import compute_backoff_ms

    failures = 0
    while True:
        try:
            ws = await websockets.connect(ws_url)
        except Exception as e:
            transport.record_private_ws_failure(
                _now_ms(), f"bitget private ws connect failed: {e}", unhealthy_after_failures
            )
            failures += 1
            delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
            await asyncio.sleep(delay / 1000.0)
            continue

        transport.record_private_ws_success(_now_ms())

        # Login
        timestamp = str(int(_now_ms() / 1000))
        sign = _bitget_hmac_sha256_base64(api_secret, timestamp)
        login_msg = json.dumps({
            "op": "login",
            "args": [{"apiKey": api_key, "passphrase": api_passphrase, "timestamp": timestamp, "sign": sign}],
        })

        try:
            await ws.send(login_msg)
        except Exception as e:
            transport.record_private_ws_failure(
                _now_ms(), f"bitget login send failed: {e}", unhealthy_after_failures
            )
            failures += 1
            delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
            await ws.close()
            await asyncio.sleep(delay / 1000.0)
            continue

        subscribed = False

        async def _ping_loop():
            while True:
                await asyncio.sleep(BITGET_PRIVATE_PING_INTERVAL_SECS)
                try:
                    await ws.send("ping")
                except Exception:
                    break

        ping_task = asyncio.create_task(_ping_loop())

        try:
            while True:
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                except ConnectionClosed as e:
                    transport.record_private_ws_failure(
                        _now_ms(), f"bitget private ws closed: {e}", unhealthy_after_failures
                    )
                    break

                if isinstance(message, bytes):
                    continue

                to_send, subscribed = handle_bitget_private_message(
                    private_state, symbol_map, message, subscribed
                )
                transport.record_private_ws_success(_now_ms())
                if to_send:
                    await ws.send(to_send)

        except Exception as e:
            transport.record_private_ws_failure(
                _now_ms(), f"bitget private ws receive failed: {e}", unhealthy_after_failures
            )
        finally:
            ping_task.cancel()
            try:
                await ping_task
            except asyncio.CancelledError:
                pass
            await ws.close()

        failures += 1
        delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
        await asyncio.sleep(delay / 1000.0)


def start_bitget_private_ws(transport, symbols: list[str]) -> None:
    credential = transport._credential
    if credential is None or not credential.api_key:
        return
    if not symbols:
        return

    base_url = transport._spec.private_base_url.rstrip("/")
    ws_url = _bitget_private_ws_url(base_url)
    private_state = transport._private_ws_state
    # Bitget subscribes to account-wide USDT-FUTURES order/position topics;
    # retain the transport-owned mapping across candidate changes.
    symbol_map = getattr(transport, "_private_ws_symbol_map", None)
    if symbol_map is None:
        symbol_map = {transport._venue_symbol(s): s for s in symbols}
    reconnect_initial_ms, reconnect_max_ms, unhealthy_after_failures = (
        transport.private_ws_reconnect_policy()
    )

    task = asyncio.create_task(
        _bitget_private_ws_loop(
            transport=transport,
            api_key=credential.api_key,
            api_secret=credential.api_secret,
            api_passphrase=credential.api_passphrase or "",
            ws_url=ws_url,
            symbol_map=symbol_map,
            private_state=private_state,
            unhealthy_after_failures=unhealthy_after_failures,
            reconnect_initial_ms=reconnect_initial_ms,
            reconnect_max_ms=reconnect_max_ms,
        )
    )
    private_state.push_worker(task)
    logger.info("bitget private WS worker started for %d symbols", len(symbols))
