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
GATE_PRIVATE_SUBSCRIBE_ACK_TIMEOUT_SECS = 5
GATE_PRIVATE_CHANNELS = ("futures.orders", "futures.positions")


def _shared_gate_private_ws_symbol_map(
    transport,
    symbols: list[str],
) -> dict[str, str]:
    symbol_map = getattr(transport, "_private_ws_symbol_map", None)
    if not isinstance(symbol_map, dict):
        symbol_map = {}
        transport._private_ws_symbol_map = symbol_map
    for symbol in symbols:
        if symbol:
            symbol_map.setdefault(transport._venue_symbol(symbol), symbol)
    return symbol_map


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


def _next_gate_private_subscription_id(transport) -> int:
    """Return a transport-scoped request id for correlating Gate subscribe ACKs."""
    sequence = int(getattr(transport, "_gate_private_ws_subscribe_sequence", 0) or 0) + 1
    transport._gate_private_ws_subscribe_sequence = sequence
    return sequence


def _register_gate_private_subscription_requests(
    transport,
    api_key: str,
    api_secret: str,
    contracts: list[str],
    pending_subscriptions: dict[str, dict[str, Any]],
) -> list[str]:
    """Build V1-format contract subscriptions with explicit ACK correlation.

    V1 subscribed with a contract-list payload (rather than Gate's newer
    all-contract shortcut).  Keeping that payload is intentional compatibility;
    the additive request id lets this worker prove both private channels accepted
    every initial or dynamically added contract before parsing its events.
    """
    normalized_contracts = sorted({str(contract) for contract in contracts if contract})
    if not normalized_contracts:
        return []

    now_s = int(_now_ms() / 1000)
    messages: list[str] = []
    for channel in GATE_PRIVATE_CHANNELS:
        request_id = _next_gate_private_subscription_id(transport)
        message = json.dumps({
            "id": request_id,
            "time": now_s,
            "channel": channel,
            "event": "subscribe",
            "payload": normalized_contracts,
            "auth": _gate_ws_auth(api_key, api_secret, channel, "subscribe", now_s),
        })
        pending_subscriptions[str(request_id)] = {
            "channel": channel,
            "contracts": tuple(normalized_contracts),
            "sent_at_ms": 0,
        }
        messages.append(message)
    return messages


async def _send_gate_private_subscription_message(
    transport,
    ws,
    pending_subscriptions: dict[str, dict[str, Any]],
    message: str,
    unhealthy_after_failures: int,
) -> bool:
    """Send a Gate subscription and retain it as pending until its ACK arrives."""
    try:
        payload = json.loads(message)
    except json.JSONDecodeError:
        payload = {}
    request_id = str(payload.get("id") or "")
    try:
        await ws.send(message)
    except Exception as exc:
        transport.record_private_ws_failure(
            _now_ms(),
            f"gate private ws subscribe send failed: {exc}",
            unhealthy_after_failures,
        )
        return False
    state = pending_subscriptions.get(request_id)
    if state is not None:
        state["sent_at_ms"] = _now_ms()
    return True


def _gate_subscription_timed_out(
    pending_subscriptions: dict[str, dict[str, Any]],
    now_ms: int,
) -> str | None:
    """Return a precise timeout reason for an outstanding Gate subscription."""
    timeout_ms = GATE_PRIVATE_SUBSCRIBE_ACK_TIMEOUT_SECS * 1000
    for request_id, state in pending_subscriptions.items():
        sent_at_ms = int(state.get("sent_at_ms") or 0)
        if sent_at_ms <= 0 or now_ms - sent_at_ms < timeout_ms:
            continue
        return (
            "gate private ws subscribe ACK timeout"
            f": id={request_id} channel={state.get('channel') or '-'}"
            f" contracts={','.join(state.get('contracts') or ())}"
        )
    return None


def _activate_gate_acknowledged_contracts(
    transport,
    contracts: tuple[str, ...],
    channel: str,
    symbol_map: dict[str, str],
    active_symbol_map: dict[str, str],
    acked_channels_by_contract: dict[str, set[str]],
) -> None:
    """Activate a contract only after both Gate private channels ACK it."""
    pending_updates = getattr(transport, "_private_ws_pending_symbol_updates", None)
    for contract in contracts:
        acked_channels_by_contract.setdefault(contract, set()).add(channel)
        if acked_channels_by_contract[contract] < set(GATE_PRIVATE_CHANNELS):
            continue
        symbol = symbol_map.get(contract)
        if not symbol:
            continue
        active_symbol_map[contract] = symbol
        if isinstance(pending_updates, set):
            pending_updates.discard(contract)


def _apply_gate_private_subscription_event(
    transport,
    payload: dict[str, Any],
    pending_subscriptions: dict[str, dict[str, Any]],
    symbol_map: dict[str, str],
    active_symbol_map: dict[str, str],
    acked_channels_by_contract: dict[str, set[str]],
) -> str | None:
    """Apply one Gate subscribe ACK/error, returning a reconnect reason on error."""
    if str(payload.get("event") or "") != "subscribe":
        return None

    channel = str(payload.get("channel") or "")
    request_id = str(payload.get("id") or "")
    state = pending_subscriptions.get(request_id)
    if state is None and not request_id:
        # Gate's generic response schema documents id echoing, but its older
        # examples omit it. There is at most one outstanding request per
        # channel, so this fallback remains unambiguous.
        matching = [
            (pending_id, candidate)
            for pending_id, candidate in pending_subscriptions.items()
            if candidate.get("channel") == channel
        ]
        if len(matching) == 1:
            request_id, state = matching[0]
    if state is None:
        return None

    expected_channel = str(state.get("channel") or "")
    if channel != expected_channel:
        return (
            "gate private ws subscribe ACK channel mismatch"
            f": id={request_id or '-'} expected={expected_channel or '-'}"
            f" actual={channel or '-'}"
        )

    error = payload.get("error")
    result = payload.get("result")
    status = ""
    if isinstance(result, dict):
        status = str(result.get("status") or "")
    # A subscribe event is an ACK only when Gate gives its explicit success
    # result.  An event with no error but no success status is not enough to
    # prove that the private channel is delivering for this contract.
    if error is not None or not isinstance(result, dict) or status.lower() != "success":
        return (
            "gate private ws subscribe rejected"
            f": id={request_id or '-'} channel={channel or '-'}"
            f" error={error!r} status={status or '-'}"
        )

    contracts = tuple(str(contract) for contract in state.get("contracts") or ())
    _activate_gate_acknowledged_contracts(
        transport,
        contracts,
        channel,
        symbol_map,
        active_symbol_map,
        acked_channels_by_contract,
    )
    pending_subscriptions.pop(request_id, None)
    return None


async def _apply_gate_private_ws_symbol_updates(
    transport,
    ws,
    api_key: str,
    api_secret: str,
    symbol_map: dict[str, str],
    active_symbol_map: dict[str, str],
    pending_subscriptions: dict[str, dict[str, Any]],
    unhealthy_after_failures: int,
) -> bool:
    """Subscribe one newly tracked Gate contract after the prior ACK completes."""
    if pending_subscriptions:
        return True
    pending_updates = getattr(transport, "_private_ws_pending_symbol_updates", None)
    if not isinstance(pending_updates, set) or not pending_updates:
        return True

    additions = [
        contract
        for contract in sorted(pending_updates)
        if contract in symbol_map and contract not in active_symbol_map
    ]
    if not additions:
        return True

    # Preserve V1's wire format and make each dynamic addition independently
    # observable: send one contract to orders and positions, then await both ACKs.
    messages = _register_gate_private_subscription_requests(
        transport,
        api_key,
        api_secret,
        [additions[0]],
        pending_subscriptions,
    )
    for message in messages:
        if not await _send_gate_private_subscription_message(
            transport,
            ws,
            pending_subscriptions,
            message,
            unhealthy_after_failures,
        ):
            return False
    return True


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


def _sync_gate_contract_multiplier_map(
    transport,
    symbol_map: dict[str, str],
    contract_multiplier_map: dict[str, float],
) -> dict[str, float]:
    metadata_map = getattr(transport, "_symbol_metadata", {}) or {}
    for venue_symbol, symbol in symbol_map.items():
        if contract_multiplier_map.get(venue_symbol, 0.0) > 0.0:
            continue
        multiplier = _gate_contract_multiplier_from_metadata(
            metadata_map.get(venue_symbol)
        )
        if multiplier <= 0.0:
            multiplier = _gate_contract_multiplier_from_metadata(metadata_map.get(symbol))
        if multiplier > 0.0:
            contract_multiplier_map[venue_symbol] = multiplier
    return contract_multiplier_map


def _build_gate_contract_multiplier_map(transport, symbol_map: dict[str, str]) -> dict[str, float]:
    return _sync_gate_contract_multiplier_map(transport, symbol_map, {})


def _shared_gate_contract_multiplier_map(
    transport,
    symbol_map: dict[str, str],
) -> dict[str, float]:
    contract_multiplier_map = getattr(
        transport,
        "_private_ws_contract_multiplier_map",
        None,
    )
    if not isinstance(contract_multiplier_map, dict):
        contract_multiplier_map = {}
        transport._private_ws_contract_multiplier_map = contract_multiplier_map
    return _sync_gate_contract_multiplier_map(
        transport,
        symbol_map,
        contract_multiplier_map,
    )


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

        # V1 subscribes to the initial tracked-contract list. The V2 worker
        # keeps that payload but does not accept it as live until both channels
        # ACK; later additions use the same one-contract payload and ACK rule.
        initial_contracts = sorted(symbol_map)
        pending_subscriptions: dict[str, dict[str, Any]] = {}
        active_symbol_map: dict[str, str] = {}
        acked_channels_by_contract: dict[str, set[str]] = {}
        transport._gate_private_ws_active_symbol_map = active_symbol_map
        transport._gate_private_ws_pending_subscriptions = pending_subscriptions
        initial_messages = _register_gate_private_subscription_requests(
            transport,
            api_key,
            api_secret,
            initial_contracts,
            pending_subscriptions,
        )
        send_ok = True
        for message in initial_messages:
            if not await _send_gate_private_subscription_message(
                transport,
                ws,
                pending_subscriptions,
                message,
                unhealthy_after_failures,
            ):
                send_ok = False
                break

        if not send_ok or not initial_messages:
            failures += 1
            delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
            await ws.close()
            await asyncio.sleep(delay / 1000.0)
            continue

        private_ready = False

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
                now_ms = _now_ms()
                timeout_reason = _gate_subscription_timed_out(
                    pending_subscriptions,
                    now_ms,
                )
                if timeout_reason:
                    transport.record_private_ws_failure(
                        now_ms,
                        timeout_reason,
                        unhealthy_after_failures,
                    )
                    break
                if private_ready and not await _apply_gate_private_ws_symbol_updates(
                    transport,
                    ws,
                    api_key,
                    api_secret,
                    symbol_map,
                    active_symbol_map,
                    pending_subscriptions,
                    unhealthy_after_failures,
                ):
                    break
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
                    _sync_gate_contract_multiplier_map(
                        transport,
                        symbol_map,
                        contract_multiplier_map,
                    )
                    payload = json.loads(message)
                    if not isinstance(payload, dict):
                        continue
                    subscription_error = _apply_gate_private_subscription_event(
                        transport,
                        payload,
                        pending_subscriptions,
                        symbol_map,
                        active_symbol_map,
                        acked_channels_by_contract,
                    )
                    if subscription_error:
                        transport.record_private_ws_failure(
                            _now_ms(),
                            subscription_error,
                            unhealthy_after_failures,
                        )
                        break

                    if not private_ready and all(
                        contract in active_symbol_map
                        for contract in initial_contracts
                    ):
                        private_ready = True
                        failures = 0
                        transport.record_private_ws_success(_now_ms())

                    # Do not accept an update for a newly discovered contract
                    # before both channel subscriptions ACK that contract.
                    if str(payload.get("event") or "") != "subscribe" and private_ready:
                        handle_gate_private_message(
                            private_state,
                            active_symbol_map,
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
    # V1 uses an explicit tracked-contract list. Keep the parser/multiplier maps
    # shared, but activate each contract only after orders + positions ACK.
    symbol_map = _shared_gate_private_ws_symbol_map(transport, symbols)
    contract_multiplier_map = _shared_gate_contract_multiplier_map(transport, symbol_map)
    reconnect_initial_ms, reconnect_max_ms, unhealthy_after_failures = (
        transport.private_ws_reconnect_policy()
    )

    task = asyncio.create_task(
        _gate_private_ws_loop(
            transport=transport,
            api_key=credential.api_key,
            api_secret=credential.api_secret,
            ws_url=ws_url,
            symbol_map=symbol_map,
            private_state=private_state,
            contract_multiplier_map=contract_multiplier_map,
            unhealthy_after_failures=unhealthy_after_failures,
            reconnect_initial_ms=reconnect_initial_ms,
            reconnect_max_ms=reconnect_max_ms,
        )
    )
    private_state.push_worker(task)
    logger.info("gate private WS worker started for %d symbols", len(symbols))
