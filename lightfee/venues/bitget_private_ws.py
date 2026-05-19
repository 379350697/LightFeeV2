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


def _build_bitget_subscribe(inst_type: str, symbols: list[str]) -> str:
    return json.dumps({
        "op": "subscribe",
        "args": [{"instType": inst_type, "channel": "orders", "instId": s} for s in symbols]
        + [{"instType": inst_type, "channel": "positions", "instId": s} for s in symbols],
    })


def _handle_bitget_order_data(
    data: list[dict[str, Any]],
    symbol_map: dict[str, str],
    private_state,
) -> None:
    loop = asyncio.get_running_loop()
    for row in data:
        venue_symbol = row.get("instId", "")
        symbol = symbol_map.get(venue_symbol)
        if symbol is None:
            continue
        order_id = str(row.get("orderId", ""))
        client_id = row.get("clientOid", "")
        filled_qty = float(row.get("accBaseVolume", row.get("fillSz", 0)) or 0)
        avg_price = float(row.get("avgPrice", row.get("fillPx", 0)) or 0)
        fee_quote = None
        fee_val = row.get("fee", "")
        fee_ccy = row.get("feeCcy", "")
        if fee_val and fee_ccy in ("USDT", "USDC"):
            fee_quote = abs(float(fee_val))
        status = row.get("status", "")
        state = _bitget_passive_order_state(status)
        ts = int(row.get("uTime", row.get("cTime", _now_ms())))
        update = PrivateOrderUpdate(
            symbol=symbol,
            order_id=order_id,
            client_order_id=client_id if client_id else None,
            filled_quantity=filled_qty,
            average_price=avg_price if avg_price > 0 else None,
            fee_quote=fee_quote,
            state=state,
            updated_at_ms=ts,
        )
        loop.create_task(private_state.record_order(update))


def _handle_bitget_position_data(
    data: list[dict[str, Any]],
    symbol_map: dict[str, str],
    private_state,
) -> None:
    loop = asyncio.get_running_loop()
    for row in data:
        venue_symbol = row.get("instId", "")
        symbol = symbol_map.get(venue_symbol)
        if symbol is None:
            continue
        size = float(row.get("available", row.get("total", 0)) or 0)
        pos_side = row.get("posSide", "")
        signed = size if pos_side == "long" else -size
        ts = int(row.get("uTime", _now_ms()))
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

    # Login ack → subscribe
    if event == "login" and code == "0":
        inst_ids = list(symbol_map.keys())
        return _build_bitget_subscribe("USDT-FUTURES", inst_ids), True

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

    base_url = transport._spec.rest_url.rstrip("/")
    ws_url = _bitget_private_ws_url(base_url)
    private_state = transport._private_ws_state
    symbol_map = {transport._venue_symbol(s): s for s in symbols}

    task = asyncio.create_task(
        _bitget_private_ws_loop(
            transport=transport,
            api_key=credential.api_key,
            api_secret=credential.api_secret,
            api_passphrase=credential.api_passphrase or "",
            ws_url=ws_url,
            symbol_map=symbol_map,
            private_state=private_state,
            unhealthy_after_failures=5,
            reconnect_initial_ms=1_000,
            reconnect_max_ms=60_000,
        )
    )
    private_state.push_worker(task)
    logger.info("bitget private WS worker started for %d symbols", len(symbols))
