"""V1 Hyperliquid private WebSocket worker + parser (hydrate + subscribe-based).

Exact semantic port of src/live/hyperliquid.rs private WS paths.
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

HYPERLIQUID_PRIVATE_PING_INTERVAL_SECS = 50
HYPERLIQUID_WS_PING_INTERVAL_SECS = 50


def _hyperliquid_ws_url_from_base_url(base_url: str) -> str:
    normalized = base_url.rstrip("/")
    if "hyperliquid.xyz" in normalized or "hyperliquid.com" in normalized:
        return "wss://api.hyperliquid.xyz/ws"
    if normalized.startswith("https://"):
        return normalized.replace("https://", "wss://") + "/ws"
    if normalized.startswith("http://"):
        return normalized.replace("http://", "ws://") + "/ws"
    return normalized


async def _hyperliquid_hydrate(transport, private_state, symbol_map: dict[str, str], account_address: str) -> None:
    """V1: hydrate private position state from info API before subscribing."""
    try:
        raw = await transport._request(
            "POST",
            "/info",
            private=False,
            body={
                "type": "clearinghouseState",
                "user": account_address,
            },
        )
        positions = raw.get("assetPositions", [])
        loop = asyncio.get_running_loop()
        for pos in positions:
            coin = pos.get("position", {}).get("coin", "")
            symbol = symbol_map.get(coin)
            if symbol is None:
                continue
            szi = pos.get("position", {}).get("szi", "0")
            try:
                size = float(szi)
            except (ValueError, TypeError):
                size = 0.0
            loop.create_task(private_state.update_position(symbol, size, _now_ms()))
    except Exception as e:
        logger.warning("hyperliquid hydrate state failed: %s", e)


def _apply_hyperliquid_private_message(
    private_state,
    symbol_map: dict[str, str],
    raw: str,
) -> None:
    """V1 apply_hyperliquid_private_message() — parse user events and order updates."""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return

    channel = payload.get("channel", "")

    if channel == "user":
        data = payload.get("data", {})
        fills = data.get("fills", [])
        loop = asyncio.get_running_loop()
        for fill in fills:
            coin = fill.get("coin", "")
            symbol = symbol_map.get(coin)
            if symbol is None:
                continue
            order_id = str(fill.get("oid", ""))
            cloid = fill.get("cloid", "")
            filled_sz = float(fill.get("filledSz", fill.get("sz", 0)) or 0)
            px = float(fill.get("px", 0) or 0)
            fee = float(fill.get("fee", 0) or 0)
            ts = int(fill.get("time", fill.get("tid", _now_ms())) or 0)
            if ts <= 0:
                ts = _now_ms()
            update = PrivateOrderUpdate(
                symbol=symbol,
                order_id=order_id,
                client_order_id=cloid if cloid else None,
                filled_quantity=abs(filled_sz),
                average_price=px if px > 0 else None,
                fee_quote=abs(fee) if fee != 0 else None,
                state=PassiveOrderState.PARTIALLY_FILLED if abs(filled_sz) > 0 else None,
                updated_at_ms=ts,
            )
            loop.create_task(private_state.record_order(update))

    elif channel == "orderUpdates":
        data = payload.get("data", [])
        if isinstance(data, dict):
            data = [data]
        loop = asyncio.get_running_loop()
        for order in data:
            if not isinstance(order, dict):
                continue
            order_data = order.get("order", order)
            coin = order_data.get("coin", "")
            symbol = symbol_map.get(coin)
            if symbol is None:
                continue
            order_id = str(order_data.get("oid", ""))
            cloid = order_data.get("cloid", "")
            filled_sz = float(order_data.get("filledSz", order_data.get("sz", 0)) or 0)
            limit_px = float(order_data.get("limitPx", 0) or 0)
            status = order.get("status", "")
            ts = order_data.get("timestamp", _now_ms())
            if isinstance(ts, (int, float)):
                ts = int(ts)
            else:
                ts = _now_ms()

            state = None
            if status == "open":
                state = PassiveOrderState.OPEN
            elif status == "filled":
                state = PassiveOrderState.FILLED
            elif status == "canceled":
                state = PassiveOrderState.CANCELED
            elif status == "rejected":
                state = PassiveOrderState.REJECTED

            update = PrivateOrderUpdate(
                symbol=symbol,
                order_id=order_id,
                client_order_id=cloid if cloid else None,
                filled_quantity=abs(filled_sz),
                average_price=limit_px if limit_px > 0 else None,
                state=state,
                updated_at_ms=ts,
            )
            loop.create_task(private_state.record_order(update))


async def _hyperliquid_private_ws_loop(
    transport,
    ws_url: str,
    account_address: str,
    symbol_map: dict[str, str],
    private_state,
    unhealthy_after_failures: int,
    reconnect_initial_ms: int,
    reconnect_max_ms: int,
) -> None:
    from lightfee.marketdata.resilience import compute_backoff_ms

    failures = 0
    while True:
        # 1) Hydrate positions
        await _hyperliquid_hydrate(transport, private_state, symbol_map, account_address)

        # 2) Connect
        try:
            ws = await websockets.connect(ws_url)
        except Exception as e:
            transport.record_private_ws_failure(
                _now_ms(),
                f"hyperliquid private ws connect failed: {e}",
                unhealthy_after_failures,
            )
            failures += 1
            delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
            await asyncio.sleep(delay / 1000.0)
            continue

        # 3) Subscribe to user events
        try:
            await ws.send(json.dumps({
                "method": "subscribe",
                "subscription": {"type": "userEvents", "user": account_address},
            }))
        except Exception as e:
            transport.record_private_ws_failure(
                _now_ms(),
                f"hyperliquid user events subscribe failed: {e}",
                unhealthy_after_failures,
            )
            failures += 1
            delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
            await ws.close()
            await asyncio.sleep(delay / 1000.0)
            continue

        # 4) Subscribe to order updates
        try:
            await ws.send(json.dumps({
                "method": "subscribe",
                "subscription": {"type": "orderUpdates", "user": account_address},
            }))
        except Exception as e:
            transport.record_private_ws_failure(
                _now_ms(),
                f"hyperliquid order updates subscribe failed: {e}",
                unhealthy_after_failures,
            )
            failures += 1
            delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
            await ws.close()
            await asyncio.sleep(delay / 1000.0)
            continue

        transport.record_private_ws_success(_now_ms())
        failures = 0
        logger.debug("hyperliquid private websocket connected and subscribed")

        async def _ping_loop():
            while True:
                await asyncio.sleep(HYPERLIQUID_PRIVATE_PING_INTERVAL_SECS)
                try:
                    await ws.send(json.dumps({"method": "ping"}))
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
                        _now_ms(),
                        f"hyperliquid private ws closed: {e}",
                        unhealthy_after_failures,
                    )
                    break

                if isinstance(message, bytes):
                    continue

                if not message.startswith("{"):
                    continue

                try:
                    payload = json.loads(message)
                except json.JSONDecodeError:
                    continue

                # Handle pong / subscription response
                channel = payload.get("channel", "")
                if channel == "pong":
                    continue

                # Error handling
                if "error" in payload:
                    error_msg = str(payload.get("error", ""))
                    transport.record_private_ws_failure(
                        _now_ms(),
                        f"hyperliquid ws error: {error_msg}",
                        unhealthy_after_failures,
                    )
                    if error_msg == "No data":
                        break
                    continue

                # Process data
                if channel in ("user", "orderUpdates"):
                    transport.record_private_ws_success(_now_ms())
                    try:
                        _apply_hyperliquid_private_message(
                            private_state, symbol_map, message
                        )
                    except Exception as e:
                        logger.debug("hyperliquid private ws message ignored: %s", e)

        except Exception as e:
            transport.record_private_ws_failure(
                _now_ms(),
                f"hyperliquid private ws receive failed: {e}",
                unhealthy_after_failures,
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


def start_hyperliquid_private_ws(transport, symbols: list[str]) -> None:
    credential = transport._credential
    if credential is None:
        return
    account_address = credential.account_address
    if not account_address:
        return
    if not symbols:
        return

    base_url = transport._spec.private_base_url.rstrip("/")
    ws_url = _hyperliquid_ws_url_from_base_url(base_url)
    private_state = transport._private_ws_state
    symbol_map = {transport._venue_symbol(s): s for s in symbols}

    task = asyncio.create_task(
        _hyperliquid_private_ws_loop(
            transport=transport,
            ws_url=ws_url,
            account_address=account_address,
            symbol_map=symbol_map,
            private_state=private_state,
            unhealthy_after_failures=5,
            reconnect_initial_ms=1_000,
            reconnect_max_ms=60_000,
        )
    )
    private_state.push_worker(task)
    logger.info("hyperliquid private WS worker started for %d symbols", len(symbols))
