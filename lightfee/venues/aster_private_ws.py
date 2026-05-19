"""V1 Aster private WebSocket worker + parser (listenKey-based, similar to Binance).

Exact semantic port of src/live/aster.rs private WS paths.
Aster uses a listenKey mechanism similar to Binance but with Aster-specific endpoints.
"""

from __future__ import annotations

import asyncio
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

ASTER_LISTEN_KEY_KEEPALIVE_SECS = 30 * 60
ASTER_PRIVATE_PING_INTERVAL_SECS = 20


async def _start_aster_listen_key(transport, api_key: str) -> str:
    try:
        raw = await transport._request(
            "POST",
            "/fapi/v1/listenKey",
            private=True,
        )
        listen_key = raw.get("listenKey", "")
        if not listen_key:
            raise ValueError("aster listenKey response missing listenKey")
        return listen_key
    except Exception as e:
        transport.record_private_ws_failure(
            _now_ms(), "aster listenKey start failed: " + str(e)
        )
        raise


async def _keepalive_aster_listen_key(transport, api_key: str, listen_key: str) -> None:
    try:
        await transport._request("PUT", "/fapi/v1/listenKey", private=True)
        logger.debug("aster listenKey keepalive success")
    except Exception as e:
        logger.warning("aster listenKey keepalive failed: %s", e)
        raise


async def _close_aster_listen_key(transport, api_key: str, listen_key: str) -> None:
    try:
        await transport._request("DELETE", "/fapi/v1/listenKey", private=True)
        logger.debug("aster listenKey closed")
    except Exception as e:
        logger.debug("aster listenKey close ignored: %s", e)


def _aster_ws_base_url(base_url: str) -> str:
    normalized = base_url.rstrip("/")
    if "aster" in normalized.lower():
        return normalized.replace("https://", "wss://").replace("http://", "ws://") + "/ws"
    return normalized


def handle_aster_private_message(
    private_state,
    symbol_map: dict[str, str],
    raw: str,
) -> None:
    """V1 handle_aster_private_message() — user data stream events."""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return

    event_type = payload.get("e", "")

    if event_type == "executionReport":
        _parse_aster_execution_report(payload, private_state, symbol_map)
    elif event_type == "outboundAccountPosition":
        _parse_aster_account_position(payload, private_state, symbol_map)


def _parse_aster_execution_report(
    event: dict[str, Any],
    private_state,
    symbol_map: dict[str, str],
) -> None:
    venue_symbol = event.get("s", "")
    symbol = symbol_map.get(venue_symbol)
    if symbol is None:
        return

    order_id = str(event.get("i", ""))
    client_order_id = event.get("c", "")
    if client_order_id:
        client_order_id = str(client_order_id)

    filled_qty = float(event.get("z", 0) or 0)
    avg_price = float(event.get("ap", 0) or 0)
    fee_quote = None
    commission = event.get("n", "")
    if commission:
        fee = float(commission)
        if fee > 0:
            fee_quote = fee

    status = event.get("X", "").upper()
    state_map = {
        "NEW": PassiveOrderState.OPEN,
        "PARTIALLY_FILLED": PassiveOrderState.PARTIALLY_FILLED,
        "FILLED": PassiveOrderState.FILLED,
        "CANCELED": PassiveOrderState.CANCELED,
        "REJECTED": PassiveOrderState.REJECTED,
        "EXPIRED": PassiveOrderState.EXPIRED,
    }
    state = state_map.get(status)

    updated_at_ms = int(event.get("T", event.get("E", 0)) or 0)
    if updated_at_ms <= 0:
        updated_at_ms = _now_ms()

    update = PrivateOrderUpdate(
        symbol=symbol,
        order_id=order_id,
        client_order_id=client_order_id if client_order_id else None,
        filled_quantity=filled_qty,
        average_price=avg_price if avg_price > 0 else None,
        fee_quote=fee_quote,
        state=state,
        updated_at_ms=updated_at_ms,
    )
    loop = asyncio.get_running_loop()
    loop.create_task(private_state.record_order(update))


def _parse_aster_account_position(
    event: dict[str, Any],
    private_state,
    symbol_map: dict[str, str],
) -> None:
    balances = event.get("B", [])
    loop = asyncio.get_running_loop()
    for balance in balances:
        asset = balance.get("a", "")
        symbol = symbol_map.get(asset)
        if symbol is None:
            continue
        wallet_balance = float(balance.get("wb", 0) or 0)
        updated_at_ms = int(event.get("E", _now_ms()))
        loop.create_task(
            private_state.update_position(symbol, wallet_balance, updated_at_ms)
        )


async def _aster_private_ws_loop(
    transport,
    api_key: str,
    ws_base_url: str,
    symbol_map: dict[str, str],
    private_state,
    unhealthy_after_failures: int,
    reconnect_initial_ms: int,
    reconnect_max_ms: int,
) -> None:
    from lightfee.marketdata.resilience import compute_backoff_ms

    failures = 0
    while True:
        listen_key: Optional[str] = None
        try:
            listen_key = await _start_aster_listen_key(transport, api_key)
        except Exception:
            failures += 1
            delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
            await asyncio.sleep(delay / 1000.0)
            continue

        keepalive_done = asyncio.Event()

        async def _keepalive_loop():
            try:
                while not keepalive_done.is_set():
                    try:
                        await asyncio.wait_for(
                            keepalive_done.wait(), timeout=ASTER_LISTEN_KEY_KEEPALIVE_SECS
                        )
                        break
                    except asyncio.TimeoutError:
                        pass
                    try:
                        await _keepalive_aster_listen_key(transport, api_key, listen_key)
                    except Exception:
                        break
            except Exception:
                pass

        keepalive_task = asyncio.create_task(_keepalive_loop())

        url = f"{ws_base_url}/{listen_key}"
        try:
            async with websockets.connect(url) as ws:
                transport.record_private_ws_success(_now_ms())
                failures = 0
                logger.debug("aster private websocket connected")

                while True:
                    try:
                        message = await asyncio.wait_for(
                            ws.recv(), timeout=ASTER_PRIVATE_PING_INTERVAL_SECS
                        )
                    except asyncio.TimeoutError:
                        try:
                            await ws.ping()
                            transport.record_private_ws_success(_now_ms())
                        except Exception as e:
                            transport.record_private_ws_failure(
                                _now_ms(),
                                f"aster private ws ping failed: {e}",
                                unhealthy_after_failures,
                            )
                            break
                        continue

                    if isinstance(message, bytes):
                        continue

                    try:
                        handle_aster_private_message(private_state, symbol_map, message)
                        transport.record_private_ws_success(_now_ms())
                    except Exception as e:
                        logger.debug("aster private ws message ignored: %s", e)

        except ConnectionClosed as e:
            transport.record_private_ws_failure(
                _now_ms(), f"aster private ws closed: {e}", unhealthy_after_failures
            )
        except Exception as e:
            transport.record_private_ws_failure(
                _now_ms(), f"aster private ws connect/recv failed: {e}", unhealthy_after_failures
            )

        keepalive_done.set()
        keepalive_task.cancel()
        try:
            await keepalive_task
        except asyncio.CancelledError:
            pass

        if listen_key:
            try:
                await _close_aster_listen_key(transport, api_key, listen_key)
            except Exception:
                pass

        failures += 1
        delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
        await asyncio.sleep(delay / 1000.0)


def start_aster_private_ws(transport, symbols: list[str]) -> None:
    credential = transport._credential
    if credential is None or not credential.api_key:
        return
    if not symbols:
        return

    base_url = transport._spec.rest_url.rstrip("/")
    ws_base_url = _aster_ws_base_url(base_url)
    private_state = transport._private_ws_state
    symbol_map = {transport._venue_symbol(s): s for s in symbols}

    task = asyncio.create_task(
        _aster_private_ws_loop(
            transport=transport,
            api_key=credential.api_key,
            ws_base_url=ws_base_url,
            symbol_map=symbol_map,
            private_state=private_state,
            unhealthy_after_failures=5,
            reconnect_initial_ms=1_000,
            reconnect_max_ms=60_000,
        )
    )
    private_state.push_worker(task)
    logger.info("aster private WS worker started for %d symbols", len(symbols))
