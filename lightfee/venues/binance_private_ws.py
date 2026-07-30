"""V1 Binance private WebSocket worker + parser (listenKey-based).

Exact semantic port of src/live/binance.rs private WS paths.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from lightfee.core.domain import PassiveOrderState, PositionSnapshot, Side, Venue
from lightfee.marketdata.private_ws import (
    PrivateOrderUpdate,
    PrivatePositionUpdate,
    _now_ms,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants (V1 exact)
# ---------------------------------------------------------------------------

BINANCE_LISTEN_KEY_KEEPALIVE_SECS = 30 * 60  # 30 minutes
BINANCE_PRIVATE_PING_INTERVAL_SECS = 20


# ---------------------------------------------------------------------------
# ListenKey REST helpers
# ---------------------------------------------------------------------------


async def _request_binance_listen_key(
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


async def _start_binance_listen_key(
    transport, api_key: str
) -> str:
    """V1 start_binance_listen_key() — POST /fapi/v1/listenKey."""
    try:
        raw = await _request_binance_listen_key(transport, "POST", api_key)
        listen_key = raw.get("listenKey", "")
        if not listen_key:
            raise ValueError("binance listenKey response missing listenKey")
        return listen_key
    except Exception as e:
        transport.record_private_ws_failure(
            _now_ms(), "binance listenKey start failed: " + str(e)
        )
        raise


async def _keepalive_binance_listen_key(
    transport, api_key: str, listen_key: str
) -> None:
    """V1 keepalive_binance_listen_key() — PUT /fapi/v1/listenKey."""
    try:
        await _request_binance_listen_key(
            transport, "PUT", api_key, listen_key
        )
        logger.debug("binance listenKey keepalive success")
    except Exception as e:
        logger.warning("binance listenKey keepalive failed: %s", e)
        raise


async def _close_binance_listen_key(
    transport, api_key: str, listen_key: str
) -> None:
    """V1 close_binance_listen_key() — DELETE /fapi/v1/listenKey."""
    try:
        await _request_binance_listen_key(
            transport, "DELETE", api_key, listen_key
        )
        logger.debug("binance listenKey closed")
    except Exception as e:
        logger.debug("binance listenKey close ignored: %s", e)


# ---------------------------------------------------------------------------
# Binance private message parser — V1 futures events (primary path)
# ---------------------------------------------------------------------------


def _parse_binance_trade_lite(
    event: dict[str, Any],
    private_state,
    symbol_map: dict[str, str],
) -> None:
    """V1 TRADE_LITE handler — per-trade fill notifications (futures)."""
    venue_symbol = event.get("s", "")
    symbol = symbol_map.get(venue_symbol)
    if symbol is None:
        return

    raw_order_id = event.get("i")
    if raw_order_id is not None:
        order_id = str(raw_order_id).strip('"')
    else:
        order_id = ""

    client_order_id = event.get("c")
    if isinstance(client_order_id, str) and client_order_id:
        pass
    else:
        client_order_id = None

    # V1: filled quantity from "l" (last filled qty)
    raw_l = event.get("l")
    filled_qty = float(raw_l or 0) if raw_l is not None else 0.0

    # V1: average price from "L" (last filled price)
    raw_L = event.get("L")
    avg_price: Optional[float] = None
    if raw_L is not None and raw_L != "0":
        try:
            avg_price = float(raw_L)
        except (ValueError, TypeError):
            pass

    # V1: timestamp: T (trade time) first, fallback to E (event time)
    updated_at_ms = int(
        event.get("T") or event.get("E") or _now_ms())

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
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(private_state.record_order(update))
    except RuntimeError:
        pass


def _parse_binance_order_trade_update(
    event: dict[str, Any],
    private_state,
    symbol_map: dict[str, str],
) -> None:
    """V1 ORDER_TRADE_UPDATE handler — order life cycle updates (futures)."""
    order = event.get("o")
    if not isinstance(order, dict):
        return

    venue_symbol = order.get("s", "")
    symbol = symbol_map.get(venue_symbol)
    if symbol is None:
        return

    raw_order_id = order.get("i")
    if raw_order_id is not None:
        order_id = str(raw_order_id).strip('"')
    else:
        order_id = ""

    client_order_id = order.get("c")
    if isinstance(client_order_id, str) and client_order_id:
        pass
    else:
        client_order_id = None

    # V1: cumulative filled quantity "z"
    raw_z = order.get("z")
    filled_qty = float(raw_z or 0) if raw_z is not None else 0.0

    # V1: average price from "ap"
    raw_ap = order.get("ap")
    avg_price: Optional[float] = None
    if raw_ap is not None and raw_ap != "0":
        try:
            avg_price = float(raw_ap)
        except (ValueError, TypeError):
            pass

    # V1: fee quote from commission asset + amount
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

    # V1: timestamp: T (transaction time), fallback to E (event time)
    ts_val = order.get("T") or event.get("E")
    updated_at_ms: int = _now_ms()
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
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(private_state.record_order(update))
    except RuntimeError:
        pass


# Backward-compat spot-style parsers (also used by some endpoints)
# ---------------------------------------------------------------------------


def _parse_binance_execution_report(
    event: dict[str, Any],
    private_state,
    symbol_map: dict[str, str],
) -> None:
    """V1 handle_binance_private_message — executionReport handler."""
    venue_symbol = event.get("s", "")
    symbol = symbol_map.get(venue_symbol)
    if symbol is None:
        return

    order_id = str(event.get("i", ""))
    client_order_id = event.get("c", "")
    if client_order_id:
        client_order_id = str(client_order_id)

    # V1: cumulative filled quantity from "z"
    filled_qty = float(event.get("z", 0) or 0)
    avg_price = float(event.get("ap", 0) or 0)
    fee_quote = None
    commission = event.get("n", "")
    commission_asset = event.get("N", "")
    if commission and commission_asset:
        fee = float(commission)
        if fee > 0:
            fee_quote = fee

    # V1: determine passive order state from "X" (order status)
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

    # V1: use transaction time "T" if available, else event time "E"
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

    # Schedule the update on the event loop
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(private_state.record_order(update))
    except RuntimeError:
        pass


def _parse_binance_account_position(
    event: dict[str, Any],
    private_state,
    symbol_map: dict[str, str],
) -> None:
    """V1 handle_binance_private_message — outboundAccountPosition handler."""
    balances = event.get("B", [])
    for balance in balances:
        asset = balance.get("a", "")
        symbol = symbol_map.get(asset)
        if symbol is None:
            continue
        wallet_balance = float(balance.get("wb", 0) or 0)
        updated_at_ms = int(event.get("E", _now_ms()))
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(
                private_state.update_position(symbol, wallet_balance, updated_at_ms)
            )
        except RuntimeError:
            pass


def handle_binance_private_message(
    private_state,
    symbol_map: dict[str, str],
    raw: str,
) -> None:
    """V1 handle_binance_private_message() — dispatch user data stream events.

    V1 futures semantics: TRADE_LITE, ORDER_TRADE_UPDATE, ACCOUNT_UPDATE.
    Also handles spot-style executionReport/outboundAccountPosition for backward
    compatibility with cross-venue tests.
    """
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return

    event_type = payload.get("e", "")

    # V1 futures events (primary path for Binance USDⓈ-M futures)
    if event_type == "TRADE_LITE":
        _parse_binance_trade_lite(payload, private_state, symbol_map)
    elif event_type == "ORDER_TRADE_UPDATE":
        _parse_binance_order_trade_update(payload, private_state, symbol_map)
    elif event_type == "ACCOUNT_UPDATE":
        _parse_binance_account_update(payload, private_state, symbol_map)
    # Backward-compat spot-style events (also valid on some endpoints)
    elif event_type == "executionReport":
        _parse_binance_execution_report(payload, private_state, symbol_map)
    elif event_type == "outboundAccountPosition":
        _parse_binance_account_position(payload, private_state, symbol_map)
    # listenKeyExpired — handle as informational
    elif event_type == "listenKeyExpired":
        logger.warning("binance listenKey expired")


def _parse_binance_account_update(
    event: dict[str, Any],
    private_state,
    symbol_map: dict[str, str],
) -> None:
    """V1: handle ACCOUNT_UPDATE (futures account data) — position updates."""
    data = event.get("a", {})
    positions = data.get("P", [])
    updated_at_ms = int(event.get("E", _now_ms()))
    for pos in positions:
        venue_symbol = pos.get("s", "")
        symbol = symbol_map.get(venue_symbol)
        if symbol is None:
            continue
        position_amount = float(pos.get("pa", 0) or 0)
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(
                private_state.update_position(symbol, position_amount, updated_at_ms)
            )
        except RuntimeError:
            pass


# ---------------------------------------------------------------------------
# Binance private WS worker
# ---------------------------------------------------------------------------


def _binance_ws_base_url(base_url: str) -> str:
    """V1 binance_ws_base_url()."""
    normalized = base_url.rstrip("/")
    if "testnet" in normalized:
        return "wss://stream.binancefuture.com"
    if "binance.com" in normalized:
        return "wss://fstream.binance.com"
    if normalized.startswith("https://"):
        return normalized.replace("https://", "wss://")
    if normalized.startswith("http://"):
        return normalized.replace("http://", "ws://")
    return normalized


async def _binance_private_ws_loop(
    transport,
    api_key: str,
    ws_base_url: str,
    symbol_map: dict[str, str],
    private_state,
    unhealthy_after_failures: int,
    reconnect_initial_ms: int,
    reconnect_max_ms: int,
) -> None:
    """V1 binance private WS loop — listenKey lifecycle, connect, message loop.

    This is the inner loop spawned as an asyncio Task. It handles:
    - listenKey start/keepalive/close
    - websocket connect + message dispatch
    - health recording on all paths
    - reconnect with backoff
    """
    from lightfee.marketdata.resilience import compute_backoff_ms

    failures = 0
    while True:
        # 1) Start listenKey
        listen_key: Optional[str] = None
        try:
            listen_key = await _start_binance_listen_key(transport, api_key)
        except Exception:
            failures += 1
            delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
            await asyncio.sleep(delay / 1000.0)
            continue

        # 2) Spawn keepalive task
        keepalive_done = asyncio.Event()

        async def _keepalive_loop():
            try:
                while not keepalive_done.is_set():
                    try:
                        await asyncio.wait_for(
                            keepalive_done.wait(),
                            timeout=BINANCE_LISTEN_KEY_KEEPALIVE_SECS,
                        )
                        break
                    except asyncio.TimeoutError:
                        pass
                    try:
                        await _keepalive_binance_listen_key(
                            transport, api_key, listen_key
                        )
                    except Exception:
                        break
            except Exception:
                pass

        keepalive_task = asyncio.create_task(_keepalive_loop())

        # 3) Connect
        url = f"{ws_base_url}/ws/{listen_key}"
        try:
            async with websockets.connect(url) as ws:
                transport.record_private_ws_success(_now_ms())
                failures = 0
                logger.debug("binance private websocket connected")

                # 4) Message loop
                while True:
                    try:
                        message = await asyncio.wait_for(
                            ws.recv(), timeout=BINANCE_PRIVATE_PING_INTERVAL_SECS
                        )
                    except asyncio.TimeoutError:
                        # Send ping
                        try:
                            await ws.ping()
                            transport.record_private_ws_success(_now_ms())
                        except Exception as e:
                            transport.record_private_ws_failure(
                                _now_ms(),
                                f"binance private ws ping failed: {e}",
                                unhealthy_after_failures,
                            )
                            break
                        continue

                    if isinstance(message, bytes):
                        continue

                    # Dispatch message
                    try:
                        handle_binance_private_message(
                            private_state, symbol_map, message
                        )
                        transport.record_private_ws_success(_now_ms())
                    except Exception as e:
                        logger.debug(
                            "binance private websocket message ignored: %s", e
                        )

        except ConnectionClosed as e:
            transport.record_private_ws_failure(
                _now_ms(),
                f"binance private ws closed: {e}",
                unhealthy_after_failures,
            )
        except Exception as e:
            transport.record_private_ws_failure(
                _now_ms(),
                f"binance private ws connect/recv failed: {e}",
                unhealthy_after_failures,
            )

        # 5) Cleanup
        keepalive_done.set()
        keepalive_task.cancel()
        try:
            await keepalive_task
        except asyncio.CancelledError:
            pass

        if listen_key:
            try:
                await _close_binance_listen_key(transport, api_key, listen_key)
            except Exception:
                pass

        # 6) Backoff & reconnect
        failures += 1
        delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
        await asyncio.sleep(delay / 1000.0)


def start_binance_private_ws(transport, symbols: list[str]) -> None:
    """V1 start_private_ws() for Binance — spawn the private WS worker task.

    Called from VenueTransport._start_binance_private_ws().
    """
    credential = transport._credential
    if credential is None or not credential.api_key:
        return
    if not symbols:
        return

    api_key = credential.api_key
    base_url = transport._spec.private_base_url.rstrip("/")
    ws_base_url = _binance_ws_base_url(base_url)
    private_state = transport._private_ws_state

    from lightfee.venues.transport import LiveCredential

    # The transport owns a mutable map for the worker lifetime.  Binance user
    # data is account-wide, so candidate changes update parsing state without
    # recreating the listenKey/WebSocket session.
    symbol_map = getattr(transport, "_private_ws_symbol_map", None)
    if symbol_map is None:
        symbol_map = {transport._venue_symbol(s): s for s in symbols}

    reconnect_initial_ms, reconnect_max_ms, unhealthy_after_failures = (
        transport.private_ws_reconnect_policy()
    )

    task = asyncio.create_task(
        _binance_private_ws_loop(
            transport=transport,
            api_key=api_key,
            ws_base_url=ws_base_url,
            symbol_map=symbol_map,
            private_state=private_state,
            unhealthy_after_failures=unhealthy_after_failures,
            reconnect_initial_ms=reconnect_initial_ms,
            reconnect_max_ms=reconnect_max_ms,
        )
    )
    private_state.push_worker(task)
    logger.info("binance private WS worker started for %d symbols", len(symbols))
