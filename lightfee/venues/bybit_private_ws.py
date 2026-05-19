"""V1 Bybit private WebSocket worker + parser (auth + subscribe-based).

Exact semantic port of src/live/bybit.rs private WS paths.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
from typing import Any, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from lightfee.core.domain import PassiveOrderState, Side, Venue
from lightfee.marketdata.private_ws import (
    PrivateOrderUpdate,
    _now_ms,
)

logger = logging.getLogger(__name__)

BYBIT_PRIVATE_PING_INTERVAL_SECS = 20


def _bybit_private_ws_url(base_url: str) -> str:
    normalized = base_url.rstrip("/")
    if "bybit.com" in normalized:
        return "wss://stream.bybit.com/v5/private"
    if normalized.startswith("https://"):
        return normalized.replace("https://", "wss://") + "/v5/private"
    return normalized


def _bybit_hmac_sha256_hex(secret: str, message: str) -> str:
    mac = hmac.new(
        secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    )
    return mac.hexdigest()


def _bybit_passive_order_state(status: str, filled: float) -> Optional[PassiveOrderState]:
    s = status.upper()
    if s in ("NEW", "UNTRIGGERED", "ACTIVE"):
        return PassiveOrderState.PARTIALLY_FILLED if filled > 0 else PassiveOrderState.OPEN
    if s == "PARTIALLYFILLED":
        return PassiveOrderState.PARTIALLY_FILLED
    if s == "FILLED":
        return PassiveOrderState.FILLED
    if s in ("CANCELLED", "CANCELED"):
        return PassiveOrderState.CANCELED
    if s == "REJECTED":
        return PassiveOrderState.REJECTED
    return None


def _build_bybit_subscribe(topics: list[str]) -> str:
    return json.dumps({"op": "subscribe", "args": topics})


def _handle_bybit_order_message(
    data: list[dict[str, Any]],
    symbol_map: dict[str, str],
    private_state,
) -> None:
    loop = asyncio.get_running_loop()
    for row in data:
        venue_symbol = row.get("symbol", "")
        symbol = symbol_map.get(venue_symbol)
        if symbol is None:
            continue
        order_id = str(row.get("orderId", ""))
        client_id = row.get("orderLinkId", "")
        filled_qty = float(row.get("cumExecQty", 0) or 0)
        avg_price = float(row.get("avgPrice", 0) or 0)
        fee_quote = None
        cum_fee = row.get("cumExecFee", "")
        if cum_fee:
            fee_quote = abs(float(cum_fee))
        status = row.get("orderStatus", "")
        state = _bybit_passive_order_state(status, filled_qty)
        ts = int(row.get("updatedTime", _now_ms()))
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


def _handle_bybit_execution_message(
    data: list[dict[str, Any]],
    symbol_map: dict[str, str],
    private_state,
) -> None:
    loop = asyncio.get_running_loop()
    for row in data:
        venue_symbol = row.get("symbol", "")
        symbol = symbol_map.get(venue_symbol)
        if symbol is None:
            continue
        order_id = str(row.get("orderId", ""))
        client_id = row.get("orderLinkId", "")
        exec_qty = float(row.get("execQty", 0) or 0)
        exec_price = float(row.get("execPrice", 0) or 0)
        fee_quote = None
        fee_val = row.get("execFee", "")
        if fee_val:
            fee_quote = abs(float(fee_val))
        ts = int(row.get("execTime", _now_ms()))
        update = PrivateOrderUpdate(
            symbol=symbol,
            order_id=order_id,
            client_order_id=client_id if client_id else None,
            filled_quantity=exec_qty,
            average_price=exec_price if exec_price > 0 else None,
            fee_quote=fee_quote,
            state=PassiveOrderState.PARTIALLY_FILLED,
            updated_at_ms=ts,
        )
        loop.create_task(private_state.record_order(update))


def _handle_bybit_position_message(
    data: list[dict[str, Any]],
    symbol_map: dict[str, str],
    private_state,
) -> None:
    loop = asyncio.get_running_loop()
    for row in data:
        venue_symbol = row.get("symbol", "")
        symbol = symbol_map.get(venue_symbol)
        if symbol is None:
            continue
        size_str = row.get("size", "0")
        side = row.get("side", "")
        contracts = float(size_str or 0)
        signed = contracts if side == "Buy" else -contracts
        ts = int(row.get("updatedTime", _now_ms()))
        loop.create_task(private_state.update_position(symbol, signed, ts))


def handle_bybit_private_message(
    private_state,
    symbol_map: dict[str, str],
    raw: str,
    subscribed: bool = False,
) -> tuple[Optional[str], bool]:
    """V1 handle_bybit_private_message()."""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None, subscribed

    op = payload.get("op", "")
    topic = payload.get("topic", "")

    # Auth ack → trigger subscribe
    if op == "auth" and payload.get("success"):
        topics = [
            "order",
            "execution",
            "position",
        ]
        return _build_bybit_subscribe(topics), True

    # Subscribe ack
    if op == "subscribe" and payload.get("success"):
        return None, subscribed

    if not subscribed:
        return None, subscribed

    data = payload.get("data")
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        return None, subscribed

    if topic.startswith("order"):
        _handle_bybit_order_message(data, symbol_map, private_state)
    elif topic.startswith("execution"):
        _handle_bybit_execution_message(data, symbol_map, private_state)
    elif topic.startswith("position"):
        _handle_bybit_position_message(data, symbol_map, private_state)

    return None, subscribed


async def _bybit_private_ws_loop(
    transport,
    api_key: str,
    api_secret: str,
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
                _now_ms(), f"bybit private ws connect failed: {e}", unhealthy_after_failures
            )
            failures += 1
            delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
            await asyncio.sleep(delay / 1000.0)
            continue

        transport.record_private_ws_success(_now_ms())

        # Auth
        expires = _now_ms() + 10_000
        signature = _bybit_hmac_sha256_hex(api_secret, f"GET/realtime{expires}")
        auth_msg = json.dumps({"op": "auth", "args": [api_key, expires, signature]})

        try:
            await ws.send(auth_msg)
        except Exception as e:
            transport.record_private_ws_failure(
                _now_ms(), f"bybit auth send failed: {e}", unhealthy_after_failures
            )
            failures += 1
            delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
            await ws.close()
            await asyncio.sleep(delay / 1000.0)
            continue

        subscribed = False

        async def _ping_loop():
            while True:
                await asyncio.sleep(BYBIT_PRIVATE_PING_INTERVAL_SECS)
                try:
                    await ws.send(json.dumps({"op": "ping"}))
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
                        _now_ms(), f"bybit private ws closed: {e}", unhealthy_after_failures
                    )
                    break

                if isinstance(message, bytes):
                    continue

                to_send, subscribed = handle_bybit_private_message(
                    private_state, symbol_map, message, subscribed
                )
                transport.record_private_ws_success(_now_ms())
                if to_send:
                    await ws.send(to_send)

        except Exception as e:
            transport.record_private_ws_failure(
                _now_ms(), f"bybit private ws receive failed: {e}", unhealthy_after_failures
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


def start_bybit_private_ws(transport, symbols: list[str]) -> None:
    credential = transport._credential
    if credential is None or not credential.api_key:
        return
    if not symbols:
        return

    base_url = transport._spec.rest_url.rstrip("/")
    ws_url = _bybit_private_ws_url(base_url)
    private_state = transport._private_ws_state
    symbol_map = {transport._venue_symbol(s): s for s in symbols}

    task = asyncio.create_task(
        _bybit_private_ws_loop(
            transport=transport,
            api_key=credential.api_key,
            api_secret=credential.api_secret,
            ws_url=ws_url,
            symbol_map=symbol_map,
            private_state=private_state,
            unhealthy_after_failures=5,
            reconnect_initial_ms=1_000,
            reconnect_max_ms=60_000,
        )
    )
    private_state.push_worker(task)
    logger.info("bybit private WS worker started for %d symbols", len(symbols))
