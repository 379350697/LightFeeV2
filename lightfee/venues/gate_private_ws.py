"""V1 Gate private WebSocket worker + parser (signed channel subscribe).

Exact semantic port of src/live/gate.rs private WS paths.
"""

from __future__ import annotations

import asyncio
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

GATE_PRIVATE_PING_INTERVAL_SECS = 20


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


def _gate_passive_order_state(status: str) -> Optional[PassiveOrderState]:
    s = status.upper()
    if s in ("OPEN", "UNTRI"):
        return PassiveOrderState.OPEN
    if s == "PARTIAL":
        return PassiveOrderState.PARTIALLY_FILLED
    if s in ("FINISHED", "CLOSED", "FILLED"):
        return PassiveOrderState.FILLED
    if s in ("CANCELLED", "CANCELED"):
        return PassiveOrderState.CANCELED
    if s == "EXPIRED":
        return PassiveOrderState.EXPIRED
    return None


def _handle_gate_order_data(
    data: list[dict[str, Any]],
    symbol_map: dict[str, str],
    private_state,
    contract_multiplier_map: dict[str, float] | None = None,
) -> None:
    loop = asyncio.get_running_loop()
    for row in data:
        contract = row.get("contract", "")
        symbol = symbol_map.get(contract)
        if symbol is None:
            continue
        order_id = str(row.get("id", ""))
        client_id = row.get("text", "")
        filled_contracts = float(row.get("fill_total", 0) or 0)
        contract_multiplier = (
            float(contract_multiplier_map.get(contract, 0.0) or 0.0)
            if contract_multiplier_map is not None
            else 1.0
        )
        if contract_multiplier_map is not None and contract_multiplier <= 0.0:
            continue
        filled_qty = abs(filled_contracts) * contract_multiplier
        avg_price = float(row.get("fill_price", 0) or 0)
        fee_quote = float(row.get("fee", 0) or 0)
        status = row.get("finish_as", row.get("status", ""))
        state = _gate_passive_order_state(status)
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


def _handle_gate_position_data(
    data: list[dict[str, Any]],
    symbol_map: dict[str, str],
    private_state,
    contract_multiplier_map: dict[str, float] | None = None,
) -> None:
    loop = asyncio.get_running_loop()
    for row in data:
        contract = row.get("contract", "")
        symbol = symbol_map.get(contract)
        if symbol is None:
            continue
        size_contracts = float(row.get("size", 0) or 0)
        contract_multiplier = (
            float(contract_multiplier_map.get(contract, 0.0) or 0.0)
            if contract_multiplier_map is not None
            else 1.0
        )
        if contract_multiplier_map is not None and contract_multiplier <= 0.0:
            continue
        size = size_contracts * contract_multiplier
        ts = int(row.get("update_time_ms", _now_ms()))
        loop.create_task(private_state.update_position(symbol, size, ts))


def handle_gate_private_message(
    private_state,
    symbol_map: dict[str, str],
    raw: str,
    contract_multiplier_map: dict[str, float] | None = None,
) -> None:
    """V1 handle_gate_private_message()."""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return

    channel = payload.get("channel", "")
    event = payload.get("event", "")
    result = payload.get("result")

    # Subscription ack
    if event == "subscribe" and result is not None:
        return

    # Data messages
    if isinstance(result, list):
        if channel == "futures.orders":
            _handle_gate_order_data(
                result,
                symbol_map,
                private_state,
                contract_multiplier_map=contract_multiplier_map,
            )
        elif channel == "futures.positions":
            _handle_gate_position_data(
                result,
                symbol_map,
                private_state,
                contract_multiplier_map=contract_multiplier_map,
            )
    elif isinstance(result, dict):
        if channel == "futures.orders":
            _handle_gate_order_data(
                [result],
                symbol_map,
                private_state,
                contract_multiplier_map=contract_multiplier_map,
            )
        elif channel == "futures.positions":
            _handle_gate_position_data(
                [result],
                symbol_map,
                private_state,
                contract_multiplier_map=contract_multiplier_map,
            )


def _gate_contract_multiplier_from_metadata(metadata: Any) -> float:
    if not isinstance(metadata, dict):
        return 0.0
    for field in (
        "quanto_multiplier",
        "quantoMultiplier",
        "contract_multiplier",
        "contractMultiplier",
        "contract_size",
        "contractSize",
        "ct_val",
        "ctVal",
    ):
        try:
            value = float(metadata.get(field) or 0.0)
        except (TypeError, ValueError):
            value = 0.0
        if value > 0.0:
            return value
    return 0.0


def _build_gate_contract_multiplier_map(transport, symbol_map: dict[str, str]) -> dict[str, float]:
    metadata_map = getattr(transport, "_symbol_metadata", {}) or {}
    result: dict[str, float] = {}
    for venue_symbol, symbol in symbol_map.items():
        multiplier = _gate_contract_multiplier_from_metadata(
            metadata_map.get(venue_symbol)
        )
        if multiplier <= 0.0:
            multiplier = _gate_contract_multiplier_from_metadata(metadata_map.get(symbol))
        if multiplier > 0.0:
            result[venue_symbol] = multiplier
    return result


def _build_gate_private_subscribe_messages(
    api_key: str,
    api_secret: str,
    contract_list: list[str],
) -> list[str]:
    """Build the signed order and position subscriptions for given contracts."""
    now_s = int(_now_ms() / 1000)
    messages: list[str] = []
    for channel in ("futures.orders", "futures.positions"):
        auth = _gate_ws_auth(api_key, api_secret, channel, "subscribe", now_s)
        messages.append(
            json.dumps(
                {
                    "time": now_s,
                    "channel": channel,
                    "event": "subscribe",
                    "payload": contract_list,
                    "auth": auth,
                }
            )
        )
    return messages


async def _gate_private_ws_loop(
    transport,
    api_key: str,
    api_secret: str,
    ws_url: str,
    symbol_map: dict[str, str],
    private_state,
    contract_multiplier_map: dict[str, float],
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
                _now_ms(), f"gate private ws connect failed: {e}", unhealthy_after_failures
            )
            failures += 1
            delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
            await asyncio.sleep(delay / 1000.0)
            continue

        transport.record_private_ws_success(_now_ms())

        # Send subscriptions
        send_ok = False
        try:
            for message in _build_gate_private_subscribe_messages(
                api_key,
                api_secret,
                sorted(symbol_map),
            ):
                await ws.send(message)
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

        failures = 0

        async def _ping_loop():
            while True:
                await asyncio.sleep(GATE_PRIVATE_PING_INTERVAL_SECS)
                try:
                    await ws.send("ping")
                except Exception:
                    break

        ping_task = asyncio.create_task(_ping_loop())

        try:
            while True:
                consume_updates = getattr(
                    transport,
                    "consume_private_ws_symbol_updates",
                    None,
                )
                updates = consume_updates() if callable(consume_updates) else {}
                if updates:
                    contract_multiplier_map.update(
                        _build_gate_contract_multiplier_map(transport, updates)
                    )
                    for update_message in _build_gate_private_subscribe_messages(
                        api_key,
                        api_secret,
                        sorted(updates),
                    ):
                        await ws.send(update_message)
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                except ConnectionClosed as e:
                    transport.record_private_ws_failure(
                        _now_ms(), f"gate private ws closed: {e}", unhealthy_after_failures
                    )
                    break

                if isinstance(message, bytes):
                    continue

                try:
                    handle_gate_private_message(
                        private_state,
                        symbol_map,
                        message,
                        contract_multiplier_map=contract_multiplier_map,
                    )
                    transport.record_private_ws_success(_now_ms())
                except Exception as e:
                    logger.debug("gate private ws message ignored: %s", e)

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
    # Gate subscriptions are instrument-scoped.  The worker retains this map
    # and receives additions through in-band signed subscribe messages.
    symbol_map = getattr(transport, "_private_ws_symbol_map", None)
    if symbol_map is None:
        symbol_map = {transport._venue_symbol(s): s for s in symbols}
    contract_multiplier_map = _build_gate_contract_multiplier_map(transport, symbol_map)

    task = asyncio.create_task(
        _gate_private_ws_loop(
            transport=transport,
            api_key=credential.api_key,
            api_secret=credential.api_secret,
            ws_url=ws_url,
            symbol_map=symbol_map,
            private_state=private_state,
            contract_multiplier_map=contract_multiplier_map,
            unhealthy_after_failures=5,
            reconnect_initial_ms=1_000,
            reconnect_max_ms=60_000,
        )
    )
    private_state.push_worker(task)
    logger.info("gate private WS worker started for %d symbols", len(symbols))
