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


async def _request_aster_listen_key(
    transport, method: str, api_key: str, listen_key: str | None = None,
) -> dict[str, Any]:
    params = {"listenKey": listen_key} if listen_key else None
    request_listen_key = getattr(transport, "_request_listen_key", None)
    if request_listen_key is not None:
        return await request_listen_key(
            method,
            "/fapi/v1/listenKey",
            api_key=api_key,
            params=params,
        )
    return await transport._request(
        method,
        "/fapi/v1/listenKey",
        params=params,
        private=True,
    )


async def _start_aster_listen_key(transport, api_key: str) -> str:
    try:
        raw = await _request_aster_listen_key(transport, "POST", api_key)
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
        await _request_aster_listen_key(
            transport, "PUT", api_key, listen_key
        )
        logger.debug("aster listenKey keepalive success")
    except Exception as e:
        logger.warning("aster listenKey keepalive failed: %s", e)
        raise


async def _close_aster_listen_key(transport, api_key: str, listen_key: str) -> None:
    try:
        await _request_aster_listen_key(
            transport, "DELETE", api_key, listen_key
        )
        logger.debug("aster listenKey closed")
    except Exception as e:
        logger.debug("aster listenKey close ignored: %s", e)


def _aster_ws_base_url(base_url: str) -> str:
    normalized = base_url.rstrip("/")
    if "aster" in normalized.lower():
        return normalized.replace("https://", "wss://").replace("http://", "ws://") + "/ws"
    return normalized


# ---------------------------------------------------------------------------
# Aster private message parser — V1 futures events (primary path)
# ---------------------------------------------------------------------------


def _parse_aster_trade_lite(
    event: dict[str, Any],
    private_state,
    symbol_map: dict[str, str],
) -> None:
    """V1 TRADE_LITE handler for Aster — per-trade fill notifications."""
    venue_symbol = event.get("s", "")
    symbol = symbol_map.get(venue_symbol)
    if symbol is None:
        return

    raw_order_id = event.get("i")
    order_id = str(raw_order_id).strip('"') if raw_order_id is not None else ""

    raw_c = event.get("c")
    client_order_id: Optional[str] = None
    if isinstance(raw_c, str) and raw_c:
        client_order_id = raw_c

    raw_l = event.get("l")
    filled_qty = float(raw_l or 0) if raw_l is not None else 0.0

    raw_L = event.get("L")
    avg_price: Optional[float] = None
    if raw_L is not None and raw_L != "0":
        try:
            avg_price = float(raw_L)
        except (ValueError, TypeError):
            pass

    updated_at_ms = int(event.get("T") or event.get("E") or _now_ms())

    update = PrivateOrderUpdate(
        symbol=symbol,
        order_id=order_id,
        client_order_id=client_order_id,
        filled_quantity=filled_qty,
        average_price=avg_price,
        fee_quote=None,
        state=None,
        updated_at_ms=updated_at_ms,
    )
    loop = asyncio.get_running_loop()
    loop.create_task(private_state.record_order(update))


def _parse_aster_order_trade_update(
    event: dict[str, Any],
    private_state,
    symbol_map: dict[str, str],
) -> None:
    """V1 ORDER_TRADE_UPDATE handler for Aster — order life cycle updates."""
    order = event.get("o")
    if not isinstance(order, dict):
        return

    venue_symbol = order.get("s", "")
    symbol = symbol_map.get(venue_symbol)
    if symbol is None:
        return

    raw_order_id = order.get("i")
    order_id = str(raw_order_id).strip('"') if raw_order_id is not None else ""

    raw_c = order.get("c")
    client_order_id: Optional[str] = None
    if isinstance(raw_c, str) and raw_c:
        client_order_id = raw_c

    raw_z = order.get("z")
    filled_qty = float(raw_z or 0) if raw_z is not None else 0.0

    raw_ap = order.get("ap")
    avg_price: Optional[float] = None
    if raw_ap is not None and raw_ap != "0":
        try:
            avg_price = float(raw_ap)
        except (ValueError, TypeError):
            pass

    fee_quote: Optional[float] = None
    commission_asset = order.get("N")
    commission = order.get("n")
    if (commission_asset in ("USDT", "USDC")) and commission:
        try:
            f = float(commission)
            if f > 0:
                fee_quote = f
        except (ValueError, TypeError):
            pass

    ts_val = order.get("T") or event.get("E")
    updated_at_ms = _now_ms()
    if ts_val is not None:
        try:
            updated_at_ms = int(ts_val)
        except (ValueError, TypeError):
            pass

    update = PrivateOrderUpdate(
        symbol=symbol,
        order_id=order_id,
        client_order_id=client_order_id,
        filled_quantity=filled_qty,
        average_price=avg_price,
        fee_quote=fee_quote,
        state=None,
        updated_at_ms=updated_at_ms,
    )
    loop = asyncio.get_running_loop()
    loop.create_task(private_state.record_order(update))


def _parse_aster_account_update(
    event: dict[str, Any],
    private_state,
    symbol_map: dict[str, str],
) -> None:
    """V1 ACCOUNT_UPDATE handler for Aster — net position aggregation."""
    event_time = int(event.get("E", _now_ms()))
    net_positions: dict[str, float] = {}

    positions = (
        event.get("a", {}).get("P", [])
        if isinstance(event.get("a"), dict)
        else []
    )
    if not isinstance(positions, list):
        positions = []

    for pos in positions:
        if not isinstance(pos, dict):
            continue
        venue_symbol = pos.get("s", "")
        symbol = symbol_map.get(venue_symbol)
        if symbol is None:
            continue

        raw_pa = pos.get("pa")
        qty = float(raw_pa or 0) if raw_pa is not None else 0.0
        pos_side = pos.get("ps", "")
        if pos_side == "LONG":
            signed = abs(qty)
        elif pos_side == "SHORT":
            signed = -abs(qty)
        else:
            signed = qty
        net_positions[symbol] = net_positions.get(symbol, 0.0) + signed

    loop = asyncio.get_running_loop()
    for sym, size in net_positions.items():
        loop.create_task(
            private_state.update_position(sym, size, event_time)
        )


# Backward-compat spot-style parsers
# ---------------------------------------------------------------------------


def handle_aster_private_message(
    private_state,
    symbol_map: dict[str, str],
    raw: str,
) -> None:
    """V1 handle_aster_private_message() — dispatch user data stream events.

    V1 futures semantics: TRADE_LITE, ORDER_TRADE_UPDATE, ACCOUNT_UPDATE.
    Also handles spot-style executionReport/outboundAccountPosition for backward
    compatibility with cross-venue tests.
    """
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return

    event_type = payload.get("e", "")

    # V1 futures events (primary path for Aster USDⓈ-M futures)
    if event_type == "TRADE_LITE":
        _parse_aster_trade_lite(payload, private_state, symbol_map)
    elif event_type == "ORDER_TRADE_UPDATE":
        _parse_aster_order_trade_update(payload, private_state, symbol_map)
    elif event_type == "ACCOUNT_UPDATE":
        _parse_aster_account_update(payload, private_state, symbol_map)
    # Backward-compat spot-style events
    elif event_type == "executionReport":
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

    base_url = transport._spec.private_base_url.rstrip("/")
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
