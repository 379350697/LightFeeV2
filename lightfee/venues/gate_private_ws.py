"""V1 Gate private WebSocket worker + parser (signed channel subscribe).

Exact semantic port of src/live/gate.rs private WS paths.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import math
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed

from lightfee.marketdata.private_ws import (
    PrivateOrderUpdate,
    _now_ms,
)

from lightfee.venues.transport import _gate_order_execution

logger = logging.getLogger(__name__)

GATE_PRIVATE_PING_INTERVAL_SECS = 20
GATE_PRIVATE_SUBSCRIBE_TIMEOUT_SECS = 10


def _gate_ws_url(base_url: str) -> str:
    normalized = base_url.rstrip("/")
    if "gate.io" in normalized:
        return "wss://fx-ws.gateio.ws/v4/ws/usdt"
    if normalized.startswith("https://"):
        return normalized.replace("https://", "wss://") + "/v4/ws/usdt"
    return normalized


def _gate_ws_auth(api_key: str, api_secret: str, channel: str, event: str, now_s: int) -> dict:
    message = f"channel={channel}&event={event}&time={now_s}"
    signature = hmac.new(
        api_secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha512,
    ).hexdigest()
    return {"method": "api_key", "KEY": api_key, "SIGN": signature}


def _handle_gate_order_data(
    data: list[dict[str, Any]],
    symbol_map: dict[str, str],
    private_state,
    contract_multipliers: dict[str, float],
) -> bool:
    loop = asyncio.get_running_loop()
    validated = bool(data)
    for row in data:
        contract = row.get("contract", "")
        symbol = symbol_map.get(contract)
        if symbol is None:
            validated = False
            continue
        raw_id = row.get("id")
        if raw_id is not None and (isinstance(raw_id, bool) or not isinstance(raw_id, (str, int))
                                   or (isinstance(raw_id, int) and raw_id <= 0)):
            raise ValueError("gate_invalid_order_id")
        order_id = str(raw_id).strip() if raw_id is not None else ""
        text = row.get("text")
        if text is not None and not isinstance(text, str):
            raise ValueError("gate_invalid_client_order_id")
        # Source labels such as web/api/app must not merge unrelated orders.
        client_id = text[2:] if text and text.startswith("t-") and text[2:].strip() else None
        if not order_id and not client_id:
            raise ValueError("gate_order_identity_missing")
        _, filled_qty, state = _gate_order_execution(row, contract_multipliers[contract])
        avg_price = float(row.get("fill_price", 0) or 0)
        fee_quote = float(row.get("fee", 0) or 0)
        ts = int(row.get("finish_time_ms", row.get("update_time_ms", _now_ms())))
        update = PrivateOrderUpdate(
            symbol=symbol,
            order_id=order_id,
            client_order_id=client_id if client_id else None,
            filled_quantity=filled_qty,
            average_price=avg_price if avg_price > 0 else None,
            fee_quote=fee_quote if fee_quote > 0 else None,
            state=state,
            updated_at_ms=ts,
        )
        loop.create_task(private_state.record_order(update))
    return validated


def _handle_gate_position_data(
    data: list[dict[str, Any]],
    symbol_map: dict[str, str],
    private_state,
    contract_multipliers: dict[str, float],
) -> bool:
    loop = asyncio.get_running_loop()
    # A single signed cache slot cannot represent independent hedge legs.
    # REST remains the authoritative position source for Gate.
    dual = any(str(row.get("mode", "")).startswith("dual") for row in data)
    validated = bool(data)
    updates = {}
    for row in data:
        contract = row.get("contract", "")
        symbol = symbol_map.get(contract)
        if symbol is None:
            validated = False
            continue
        size = float(row["size"]) * contract_multipliers[contract]
        if not math.isfinite(size) or (not dual and symbol in updates):
            raise ValueError("gate_ambiguous_position_update")
        ts = int(row.get("update_time_ms", _now_ms()))
        updates[symbol] = (size, ts)
    if dual:
        logger.warning("gate dual position update requires REST truth")
        return validated
    for symbol, (size, ts) in updates.items():
        loop.create_task(private_state.update_position(symbol, size, ts))
    return validated


def handle_gate_private_message(
    private_state,
    symbol_map: dict[str, str],
    raw: str,
    contract_multipliers: dict[str, float],
) -> bool:
    """V1 handle_gate_private_message()."""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return False

    channel = payload.get("channel", "")
    event = payload.get("event", "")
    result = payload.get("result")

    # Subscription ack
    if event == "subscribe" and result is not None:
        return False

    # Data messages
    if isinstance(result, list):
        if channel == "futures.orders":
            return _handle_gate_order_data(result, symbol_map, private_state, contract_multipliers)
        elif channel == "futures.positions":
            return _handle_gate_position_data(result, symbol_map, private_state, contract_multipliers)
    elif isinstance(result, dict):
        if channel == "futures.orders":
            return _handle_gate_order_data([result], symbol_map, private_state, contract_multipliers)
        elif channel == "futures.positions":
            return _handle_gate_position_data([result], symbol_map, private_state, contract_multipliers)
    return False


async def _gate_private_ws_loop(
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
    # Subscription ACKs cannot erase a data-normalization failure on reconnect.
    awaiting_valid_updates: set[str | None] = set()
    while True:
        try:
            ws = await websockets.connect(ws_url, additional_headers={"X-Gate-Size-Decimal": "1"})
        except Exception as e:
            transport.record_private_ws_failure(
                _now_ms(), f"gate private ws connect failed: {e}", unhealthy_after_failures
            )
            failures += 1
            delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
            await asyncio.sleep(delay / 1000.0)
            continue

        # Build signed subscriptions
        now_s = int(_now_ms() / 1000)
        orders_auth = _gate_ws_auth(api_key, api_secret, "futures.orders", "subscribe", now_s)
        positions_auth = _gate_ws_auth(api_key, api_secret, "futures.positions", "subscribe", now_s)
        contract_list = list(symbol_map.keys())

        orders_sub = json.dumps({
            "time": now_s,
            "channel": "futures.orders",
            "event": "subscribe",
            "payload": contract_list,
            "auth": orders_auth,
        })
        positions_sub = json.dumps({
            "time": now_s,
            "channel": "futures.positions",
            "event": "subscribe",
            "payload": contract_list,
            "auth": positions_auth,
        })

        # Send subscriptions
        send_ok = False
        try:
            await ws.send(orders_sub)
            await ws.send(positions_sub)
            send_ok = True
        except Exception as e:
            transport.record_private_ws_failure(
                _now_ms(), f"gate subscribe send failed: {e}", unhealthy_after_failures
            )

        if not send_ok:
            failures += 1
            delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
            await ws.close()
            await asyncio.sleep(delay / 1000.0)
            continue

        pending_channels = {"futures.orders", "futures.positions"}
        subscribe_deadline = asyncio.get_running_loop().time() + GATE_PRIVATE_SUBSCRIBE_TIMEOUT_SECS

        async def _ping_loop():
            while True:
                await asyncio.sleep(GATE_PRIVATE_PING_INTERVAL_SECS)
                try:
                    await ws.send(json.dumps({"time": int(_now_ms() / 1000), "channel": "futures.ping"}))
                except Exception:
                    break

        ping_task = asyncio.create_task(_ping_loop())

        try:
            while True:
                if pending_channels and asyncio.get_running_loop().time() >= subscribe_deadline:
                    raise TimeoutError(f"gate subscribe timeout: missing={sorted(pending_channels)}")
                try:
                    timeout = (min(1.0, max(subscribe_deadline - asyncio.get_running_loop().time(), 0.001))
                               if pending_channels else 1.0)
                    message = await asyncio.wait_for(ws.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    continue
                except ConnectionClosed as e:
                    transport.record_private_ws_failure(
                        _now_ms(), f"gate private ws closed: {e}", unhealthy_after_failures
                    )
                    break

                if isinstance(message, bytes):
                    continue

                channel = event = None
                try:
                    payload = json.loads(message)
                    if not isinstance(payload, dict):
                        raise ValueError("gate private ws response must be an object")
                    channel, event = payload.get("channel"), payload.get("event")
                    if payload.get("error") is not None:
                        raise ValueError(f"channel={channel} event={event} error={payload['error']}")
                    if event == "subscribe":
                        if channel not in {"futures.orders", "futures.positions"}:
                            continue
                        result = payload.get("result")
                        if not isinstance(result, dict) or result.get("status") != "success":
                            raise ValueError(f"channel={channel} event={event} result={result}")
                        pending_channels.discard(channel)
                        if not pending_channels and not awaiting_valid_updates:
                            failures = 0
                            transport.record_private_ws_success(_now_ms())
                        continue
                    if channel not in {"futures.orders", "futures.positions"} or event != "update":
                        continue
                    rows = payload.get("result")
                    rows = [rows] if isinstance(rows, dict) else rows
                    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                        raise ValueError(f"channel={channel} event={event} invalid update result={rows!r}")
                    multipliers = {}
                    if isinstance(rows, list):
                        for row in rows:
                            contract = row.get("contract") if isinstance(row, dict) else None
                            if contract in symbol_map and contract not in multipliers:
                                rule = await transport._gate_symbol_rule(contract)
                                multipliers[contract] = rule.ct_val
                    validated = handle_gate_private_message(private_state, symbol_map, message, multipliers)
                    if validated:
                        awaiting_valid_updates.discard(channel)
                        awaiting_valid_updates.discard(None)
                    if validated and not pending_channels and not awaiting_valid_updates:
                        failures = 0
                        transport.record_private_ws_success(_now_ms())
                except Exception as e:
                    if event != "subscribe":
                        awaiting_valid_updates.add(channel if channel in {"futures.orders", "futures.positions"} else None)
                    transport.record_private_ws_failure(
                        _now_ms(), f"gate private ws normalization failed: {e}", unhealthy_after_failures
                    )
                    logger.warning("gate private ws normalization failed: %s", e)
                    break

        except Exception as e:
            transport.record_private_ws_failure(
                _now_ms(), f"gate private ws receive failed: {e}", unhealthy_after_failures
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


def start_gate_private_ws(transport, symbols: list[str]) -> None:
    credential = transport._credential
    if credential is None or not credential.api_key:
        return
    if not symbols:
        return

    base_url = transport._spec.private_base_url.rstrip("/")
    ws_url = _gate_ws_url(base_url)
    private_state = transport._private_ws_state
    symbol_map = {transport._venue_symbol(s): s for s in symbols}

    task = asyncio.create_task(
        _gate_private_ws_loop(
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
    logger.info("gate private WS worker started for %d symbols", len(symbols))
