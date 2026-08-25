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
BINANCE_LISTEN_KEY_KEEPALIVE_MAX_FAILURES = 3
BINANCE_LISTEN_KEY_VALIDITY_SECS = 60 * 60


def _record_binance_private_ws_event(
    transport,
    kind: str,
    payload: dict[str, Any],
) -> None:
    record = getattr(transport, "_record_order_diagnostic", None)
    if record is not None:
        record(kind, payload)
    logger.info("%s %s", kind, payload)


async def _cancel_task(task: asyncio.Task) -> None:
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception:
        pass


def _is_binance_listen_key_expired_event(raw: str) -> bool:
    try:
        payload = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return False
    return isinstance(payload, dict) and payload.get("e") == "listenKeyExpired"


def _binance_listen_key_expires_at(
    listen_key_created_at: int | None,
    last_keepalive_ok_at: int | None,
) -> int | None:
    last_success_at = last_keepalive_ok_at or listen_key_created_at
    if last_success_at is None:
        return None
    return last_success_at + (BINANCE_LISTEN_KEY_VALIDITY_SECS * 1000)


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
    rotation_count = 0
    pending_rotation: Optional[dict[str, Any]] = None
    while True:
        # 1) Start listenKey
        listen_key: Optional[str] = None
        listen_key_created_at: Optional[int] = None
        try:
            listen_key = await _start_binance_listen_key(transport, api_key)
            listen_key_created_at = _now_ms()
            _record_binance_private_ws_event(
                transport,
                "binance.listen_key_created",
                {
                    "listen_key_created_at": listen_key_created_at,
                    "last_listen_key_success_at": listen_key_created_at,
                    "last_keepalive_ok_at": None,
                    "listen_key_expires_at": _binance_listen_key_expires_at(
                        listen_key_created_at,
                        None,
                    ),
                    "rotation_count": rotation_count,
                    "reconnect_result": (
                        "pending" if pending_rotation is not None else "initial"
                    ),
                },
            )
        except Exception:
            failures += 1
            if pending_rotation is not None:
                pending_rotation["reconnect_result"] = "failed_listen_key_create"
                _record_binance_private_ws_event(
                    transport,
                    "binance.listen_key_rotation",
                    pending_rotation,
                )
            delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
            await asyncio.sleep(delay / 1000.0)
            continue

        # 2) Keep the key alive. A single REST failure is retried on the same
        # key; only a bounded retry-budget breach rotates the private stream.
        keepalive_done = asyncio.Event()
        rotate_listen_key = asyncio.Event()
        rotation_reason: Optional[str] = None
        keepalive_attempt_count = 0
        last_keepalive_ok_at: Optional[int] = None
        private_ws_last_ok_at: Optional[int] = None

        async def _keepalive_loop() -> None:
            nonlocal keepalive_attempt_count, last_keepalive_ok_at, rotation_reason
            delay_secs = BINANCE_LISTEN_KEY_KEEPALIVE_SECS
            try:
                while not keepalive_done.is_set():
                    try:
                        await asyncio.wait_for(
                            keepalive_done.wait(), timeout=delay_secs
                        )
                        return
                    except asyncio.TimeoutError:
                        pass
                    try:
                        await _keepalive_binance_listen_key(
                            transport, api_key, listen_key
                        )
                    except Exception as exc:
                        keepalive_attempt_count += 1
                        if (
                            keepalive_attempt_count
                            >= BINANCE_LISTEN_KEY_KEEPALIVE_MAX_FAILURES
                        ):
                            rotation_reason = "keepalive_retry_budget_exhausted"
                            transport.record_private_ws_failure(
                                _now_ms(),
                                "binance listenKey keepalive retry budget exhausted "
                                f"after {keepalive_attempt_count} attempts: {exc}",
                                1,
                            )
                            rotate_listen_key.set()
                            return
                        retry_ms = compute_backoff_ms(
                            reconnect_initial_ms,
                            reconnect_max_ms,
                            keepalive_attempt_count,
                        )
                        _record_binance_private_ws_event(
                            transport,
                            "binance.listen_key_keepalive_retry",
                            {
                                "listen_key_created_at": listen_key_created_at,
                                "last_listen_key_success_at": (
                                    last_keepalive_ok_at or listen_key_created_at
                                ),
                                "last_keepalive_ok_at": last_keepalive_ok_at,
                                "listen_key_expires_at": _binance_listen_key_expires_at(
                                    listen_key_created_at,
                                    last_keepalive_ok_at,
                                ),
                                "keepalive_attempt_count": keepalive_attempt_count,
                                "retry_after_ms": retry_ms,
                            },
                        )
                        delay_secs = retry_ms / 1000.0
                        continue
                    last_keepalive_ok_at = _now_ms()
                    keepalive_attempt_count = 0
                    _record_binance_private_ws_event(
                        transport,
                        "binance.listen_key_keepalive_ok",
                        {
                            "listen_key_created_at": listen_key_created_at,
                            "last_listen_key_success_at": last_keepalive_ok_at,
                            "last_keepalive_ok_at": last_keepalive_ok_at,
                            "listen_key_expires_at": _binance_listen_key_expires_at(
                                listen_key_created_at,
                                last_keepalive_ok_at,
                            ),
                            "keepalive_attempt_count": keepalive_attempt_count,
                        },
                    )
                    delay_secs = BINANCE_LISTEN_KEY_KEEPALIVE_SECS
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                rotation_reason = "keepalive_worker_error"
                transport.record_private_ws_failure(
                    _now_ms(),
                    f"binance listenKey keepalive worker failed: {exc}",
                    1,
                )
                rotate_listen_key.set()

        keepalive_task = asyncio.create_task(_keepalive_loop())

        # 3) Connect
        url = f"{ws_base_url}/ws/{listen_key}"
        try:
            async with websockets.connect(
                url,
                ping_interval=BINANCE_PRIVATE_PING_INTERVAL_SECS,
                ping_timeout=BINANCE_PRIVATE_PING_INTERVAL_SECS,
            ) as ws:
                now_ms = _now_ms()
                private_ws_last_ok_at = now_ms
                transport.record_private_ws_success(now_ms)
                failures = 0
                logger.debug("binance private websocket connected")
                if pending_rotation is not None:
                    pending_rotation.update(
                        {
                            "new_listen_key_created_at": listen_key_created_at,
                            "reconnect_result": "success",
                        }
                    )
                    _record_binance_private_ws_event(
                        transport,
                        "binance.listen_key_rotation",
                        pending_rotation,
                    )
                    pending_rotation = None

                async def _rotate_current_stream(reason: str) -> None:
                    nonlocal pending_rotation, rotation_count
                    now = _now_ms()
                    rotation_count += 1
                    pending_rotation = {
                        "reason": reason,
                        "rotation_count": rotation_count,
                        "listen_key_created_at": listen_key_created_at,
                        "last_listen_key_success_at": (
                            last_keepalive_ok_at or listen_key_created_at
                        ),
                        "last_keepalive_ok_at": last_keepalive_ok_at,
                        "listen_key_expires_at": _binance_listen_key_expires_at(
                            listen_key_created_at,
                            last_keepalive_ok_at,
                        ),
                        "keepalive_attempt_count": keepalive_attempt_count,
                        "private_ws_silent_age": (
                            now - private_ws_last_ok_at
                            if private_ws_last_ok_at is not None
                            else None
                        ),
                        "reconnect_result": "pending",
                    }
                    health_error = (
                        "binance listenKey expired; rotating"
                        if reason == "listen_key_expired_event"
                        else f"binance listenKey {reason.replace('_', ' ')}; rotating"
                    )
                    if reason not in {
                        "keepalive_retry_budget_exhausted",
                        "keepalive_worker_error",
                    }:
                        transport.record_private_ws_failure(
                            now,
                            health_error,
                            1,
                        )
                    await ws.close()

                # 4) Message loop. ``websockets`` owns Ping/Pong and closes a
                # dead connection on its configured pong timeout. Health is
                # therefore updated only by connect or an actual user event.
                while True:
                    recv_task = asyncio.create_task(ws.recv())
                    rotate_task = asyncio.create_task(rotate_listen_key.wait())
                    try:
                        done, _ = await asyncio.wait(
                            {recv_task, rotate_task},
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    except asyncio.CancelledError:
                        await _cancel_task(recv_task)
                        await _cancel_task(rotate_task)
                        raise
                    if rotate_task in done and rotate_listen_key.is_set():
                        await _cancel_task(recv_task)
                        await _cancel_task(rotate_task)
                        await _rotate_current_stream(
                            rotation_reason or "keepalive_rotation_requested"
                        )
                        break
                    await _cancel_task(rotate_task)

                    message = recv_task.result()
                    if isinstance(message, bytes):
                        continue
                    if _is_binance_listen_key_expired_event(message):
                        await _rotate_current_stream("listen_key_expired_event")
                        break

                    # Dispatch message
                    try:
                        handle_binance_private_message(
                            private_state, symbol_map, message
                        )
                        now_ms = _now_ms()
                        private_ws_last_ok_at = now_ms
                        transport.record_private_ws_success(now_ms)
                    except Exception as exc:
                        logger.debug(
                            "binance private websocket message ignored: %s", exc
                        )

        except ConnectionClosed as exc:
            transport.record_private_ws_failure(
                _now_ms(),
                f"binance private ws closed: {exc}",
                unhealthy_after_failures,
            )
        except Exception as exc:
            transport.record_private_ws_failure(
                _now_ms(),
                f"binance private ws connect/recv failed: {exc}",
                unhealthy_after_failures,
            )
        finally:
            keepalive_done.set()
            await _cancel_task(keepalive_task)
            if listen_key:
                await _close_binance_listen_key(transport, api_key, listen_key)

        # 5) Backoff & reconnect
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

    # V1: venue_symbol mapping
    symbol_map = {
        transport._venue_symbol(s): s for s in symbols
    }

    # V1: reconnect parameters from runtime config
    reconnect_initial_ms = 1_000
    reconnect_max_ms = 60_000
    unhealthy_after_failures = 5

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
