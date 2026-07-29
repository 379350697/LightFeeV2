"""V1 OKX private WebSocket worker + parser (login + subscribe-based).

Exact semantic port of src/live/okx.rs private WS paths.
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

# ---------------------------------------------------------------------------
# Constants (V1 exact)
# ---------------------------------------------------------------------------

OKX_PRIVATE_WS_IDLE_TIMEOUT_SECS = 90
OKX_PRIVATE_WS_WATCHDOG_TICK_SECS = 5
OKX_PRIVATE_WS_PING_INTERVAL_SECS = 20
OKX_PRIVATE_WS_SUBSCRIBE_RETRY_INITIAL_MS = 1_000
OKX_PRIVATE_WS_SUBSCRIBE_RETRY_MAX_MS = 30_000


# ---------------------------------------------------------------------------
# OKX helpers
# ---------------------------------------------------------------------------


def _okx_hmac_sha256_base64(secret: str, message: str) -> str:
    """V1 hmac_sha256_base64()."""
    mac = hmac.new(
        secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    )
    return base64.b64encode(mac.digest()).decode("utf-8")


def _okx_private_ws_url(base_url: str) -> str:
    """V1 okx_private_ws_url() — resolve from REST base."""
    normalized = base_url.rstrip("/")
    if "okx.com" in normalized:
        return "wss://ws.okx.com:8443/ws/v5/private"
    if normalized.startswith("https://"):
        return normalized.replace("https://", "wss://") + "/ws/v5/private"
    if normalized.startswith("http://"):
        return normalized.replace("http://", "ws://") + "/ws/v5/private"
    return normalized


def _build_okx_private_subscribe_messages(
    symbol_map: dict[str, str]
) -> list[str]:
    """V1 build_okx_private_subscribe_messages() — order + position channels."""
    inst_ids = list(symbol_map.keys())
    messages = []
    # Order channel
    if inst_ids:
        # Split into chunks of 100
        for i in range(0, len(inst_ids), 100):
            chunk = inst_ids[i : i + 100]
            messages.append(
                json.dumps(
                    {
                        "op": "subscribe",
                        "args": [
                            {"channel": "orders", "instType": "SWAP", "instId": inst_id}
                            for inst_id in chunk
                        ],
                    }
                )
            )
    # Position channel
    if inst_ids:
        # Split into chunks of 100
        for i in range(0, len(inst_ids), 100):
            chunk = inst_ids[i : i + 100]
            messages.append(
                json.dumps(
                    {
                        "op": "subscribe",
                        "args": [
                            {
                                "channel": "positions",
                                "instType": "SWAP",
                                "instId": inst_id,
                            }
                            for inst_id in chunk
                        ],
                    }
                )
            )
    return messages


def _shared_okx_private_ws_symbol_map(
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


def _okx_symbol_map_with_ct_val(
    symbol_map: dict[str, str],
    ct_val_map: dict[str, float],
) -> dict[str, str]:
    return {
        venue_symbol: symbol
        for venue_symbol, symbol in symbol_map.items()
        if venue_symbol in ct_val_map
    }


def _next_okx_private_subscription_id(transport, channel: str) -> str:
    sequence = int(getattr(transport, "_okx_private_ws_subscribe_sequence", 0) or 0) + 1
    transport._okx_private_ws_subscribe_sequence = sequence
    return f"lfp{sequence:x}{channel[:1]}"


def _register_okx_private_subscription_requests(
    transport,
    symbol_map: dict[str, str],
    pending_subscriptions: dict[str, dict[str, Any]],
) -> list[str]:
    inst_ids = list(symbol_map.keys())
    messages: list[str] = []
    for channel in ("orders", "positions"):
        for i in range(0, len(inst_ids), 100):
            chunk = inst_ids[i : i + 100]
            if not chunk:
                continue
            request_id = _next_okx_private_subscription_id(transport, channel)
            args = [
                {"channel": channel, "instType": "SWAP", "instId": inst_id}
                for inst_id in chunk
            ]
            message = json.dumps(
                {
                    "id": request_id,
                    "op": "subscribe",
                    "args": args,
                }
            )
            pending_subscriptions[request_id] = {
                "message": message,
                "symbols": {
                    inst_id: symbol_map[inst_id]
                    for inst_id in chunk
                    if inst_id in symbol_map
                },
                "expected": {
                    (channel, inst_id)
                    for inst_id in chunk
                },
                "acked": set(),
                "attempts": 0,
                "last_sent_ms": 0,
                "next_retry_ms": 0,
            }
            messages.append(message)
    transport._okx_private_ws_pending_subscriptions = pending_subscriptions
    return messages


def _okx_subscription_retry_delay_ms(attempts: int) -> int:
    exponent = max(min(int(attempts or 1) - 1, 5), 0)
    delay = OKX_PRIVATE_WS_SUBSCRIBE_RETRY_INITIAL_MS * (2 ** exponent)
    return min(delay, OKX_PRIVATE_WS_SUBSCRIBE_RETRY_MAX_MS)


def _okx_subscription_pending_for_inst(
    pending_subscriptions: dict[str, dict[str, Any]],
    inst_id: str,
) -> bool:
    return any(
        inst_id in (state.get("symbols") or {})
        for state in pending_subscriptions.values()
    )


async def _send_okx_private_subscription_message(
    transport,
    ws,
    pending_subscriptions: dict[str, dict[str, Any]],
    message: str,
    unhealthy_after_failures: int,
) -> bool:
    request_id = ""
    try:
        payload = json.loads(message)
        request_id = str(payload.get("id") or "")
    except json.JSONDecodeError:
        request_id = ""
    try:
        await ws.send(message)
    except Exception as exc:
        if request_id and request_id in pending_subscriptions:
            state = pending_subscriptions[request_id]
            attempts = int(state.get("attempts") or 0) + 1
            state["attempts"] = attempts
            state["next_retry_ms"] = (
                _now_ms() + _okx_subscription_retry_delay_ms(attempts)
            )
        transport.record_private_ws_failure(
            _now_ms(),
            f"okx private ws subscribe send failed: {exc}",
            unhealthy_after_failures,
        )
        return False
    if request_id and request_id in pending_subscriptions:
        state = pending_subscriptions[request_id]
        state["attempts"] = int(state.get("attempts") or 0) + 1
        state["last_sent_ms"] = _now_ms()
        state["next_retry_ms"] = 0
    return True


def _activate_okx_acknowledged_subscriptions(
    transport,
    inst_id: str,
    symbol_map: dict[str, str],
    active_symbol_map: dict[str, str],
    acked_channels_by_inst: dict[str, set[str]],
) -> None:
    if acked_channels_by_inst.get(inst_id, set()) < {"orders", "positions"}:
        return
    symbol = symbol_map.get(inst_id)
    if symbol is None:
        return
    active_symbol_map[inst_id] = symbol
    pending_updates = getattr(transport, "_private_ws_pending_symbol_updates", None)
    if isinstance(pending_updates, set):
        pending_updates.discard(inst_id)
    transport._okx_private_ws_active_symbol_map = active_symbol_map


def _apply_okx_private_subscription_event(
    transport,
    payload: dict[str, Any],
    pending_subscriptions: dict[str, dict[str, Any]],
    active_symbol_map: dict[str, str],
    symbol_map: dict[str, str],
    acked_channels_by_inst: dict[str, set[str]],
    unhealthy_after_failures: int,
) -> None:
    event = str(payload.get("event") or "")
    if event not in {"subscribe", "error"}:
        return
    request_id = str(payload.get("id") or "")
    state = pending_subscriptions.get(request_id)
    arg = payload.get("arg")
    if not isinstance(arg, dict):
        arg = {}
    channel = str(arg.get("channel") or "")
    inst_id = str(arg.get("instId") or "")
    code = str(payload.get("code") or "")

    if event == "error" or (event == "subscribe" and code not in {"", "0"}):
        if state is not None:
            attempts = int(state.get("attempts") or 1)
            state["next_retry_ms"] = (
                _now_ms() + _okx_subscription_retry_delay_ms(attempts)
            )
        transport.record_private_ws_failure(
            _now_ms(),
            "okx private ws subscribe rejected"
            f": id={request_id or '-'} channel={channel or '-'}"
            f" instId={inst_id or '-'} code={code or '-'}"
            f" msg={payload.get('msg') or ''}",
            unhealthy_after_failures,
        )
        return

    if state is None or not channel or not inst_id:
        return
    ack_key = (channel, inst_id)
    expected = state.get("expected")
    if not isinstance(expected, set) or ack_key not in expected:
        return
    acked = state.get("acked")
    if not isinstance(acked, set):
        acked = set()
        state["acked"] = acked
    acked.add(ack_key)
    acked_channels_by_inst.setdefault(inst_id, set()).add(channel)
    _activate_okx_acknowledged_subscriptions(
        transport,
        inst_id,
        symbol_map,
        active_symbol_map,
        acked_channels_by_inst,
    )
    if expected.issubset(acked):
        pending_subscriptions.pop(request_id, None)
    transport._okx_private_ws_pending_subscriptions = pending_subscriptions


async def _retry_okx_private_subscriptions(
    transport,
    ws,
    pending_subscriptions: dict[str, dict[str, Any]],
    unhealthy_after_failures: int,
) -> None:
    now_ms = _now_ms()
    for state in list(pending_subscriptions.values()):
        next_retry_ms = int(state.get("next_retry_ms") or 0)
        if next_retry_ms <= 0 or next_retry_ms > now_ms:
            continue
        message = str(state.get("message") or "")
        if not message:
            continue
        await _send_okx_private_subscription_message(
            transport,
            ws,
            pending_subscriptions,
            message,
            unhealthy_after_failures,
        )


# ---------------------------------------------------------------------------
# OKX private message parser
# ---------------------------------------------------------------------------


def _passive_order_state_from_okx(
    state_str: Optional[str], filled_qty: float
) -> Optional[PassiveOrderState]:
    """V1 okx_passive_order_state()."""
    if state_str is None:
        return None
    s = state_str.upper()
    if s in ("CANCELED", "CANCELLED"):
        return PassiveOrderState.CANCELED
    if s == "FILLED":
        return PassiveOrderState.FILLED
    if s == "PARTIALLY_FILLED":
        return PassiveOrderState.PARTIALLY_FILLED
    if s == "REJECTED":
        return PassiveOrderState.REJECTED
    if s in ("LIVE", "OPEN"):
        if filled_qty > 0:
            return PassiveOrderState.PARTIALLY_FILLED
        return PassiveOrderState.OPEN
    return None


def _parse_okx_order_data(
    row: dict[str, Any],
    symbol_map: dict[str, str],
    private_state,
    ct_val_map: dict[str, float],
) -> None:
    """V1 OKX orders channel handler — ctVal conversion contracts→base."""
    venue_symbol = row.get("instId", "")
    symbol = symbol_map.get(venue_symbol)
    if symbol is None:
        return
    ct_val = float(ct_val_map.get(venue_symbol, 0.0) or 0.0)
    if ct_val <= 0:
        logger.warning("okx private order skipped without trusted ctVal: %s", venue_symbol)
        return

    order_id = str(row.get("ordId", ""))
    client_order_id = row.get("clOrdId", "")
    if isinstance(client_order_id, str) and client_order_id:
        pass
    else:
        client_order_id = None

    # V1: accFillSz / fillSz for cumulative filled (in contracts)
    acc_fill = row.get("accFillSz")
    if acc_fill is None or acc_fill == "":
        acc_fill = row.get("fillSz", "0")
    filled_contracts = float(acc_fill or 0)
    filled_qty = abs(filled_contracts) * ct_val

    # V1: avgPx > fillPx
    avg_px_str = (
        row.get("avgPx") or row.get("fillPx")
    )
    avg_price = None
    if avg_px_str and avg_px_str != "" and avg_px_str != "0":
        try:
            avg_price = float(avg_px_str)
        except (ValueError, TypeError):
            pass

    # V1: fillFeeCcy + fillFee
    fee_quote = None
    fee_ccy = row.get("fillFeeCcy", "")
    fee_val = row.get("fillFee", "")
    if fee_ccy in ("USDT", "USDC") and fee_val:
        try:
            f = float(fee_val)
            fee_quote = abs(f)
        except (ValueError, TypeError):
            pass

    state = _passive_order_state_from_okx(
        row.get("state"), filled_qty
    )

    # V1: fillTime > uTime for timestamp
    ts_str = row.get("fillTime") or row.get("uTime") or row.get("cTime")
    updated_at_ms = _now_ms()
    if ts_str and ts_str != "":
        try:
            updated_at_ms = int(ts_str)
        except (ValueError, TypeError):
            pass

    update = PrivateOrderUpdate(
        symbol=symbol,
        order_id=order_id,
        client_order_id=client_order_id,
        filled_quantity=filled_qty,
        average_price=avg_price,
        fee_quote=fee_quote,
        state=state,
        updated_at_ms=updated_at_ms,
    )
    asyncio.get_running_loop().create_task(private_state.record_order(update))


def _parse_okx_position_data(
    rows: list[dict[str, Any]],
    symbol_map: dict[str, str],
    private_state,
    ct_val_map: dict[str, float],
) -> None:
    """V1 OKX positions channel handler — net position aggregation.

    Contracts→base conversion via ctVal (V1: positions_by_symbol * ct_val).
    """
    net_positions: dict[str, float] = {}
    updated_at_ms = _now_ms()
    for row in rows:
        venue_symbol = row.get("instId", "")
        symbol = symbol_map.get(venue_symbol)
        if symbol is None:
            continue
        ct_val = float(ct_val_map.get(venue_symbol, 0.0) or 0.0)
        if ct_val <= 0:
            logger.warning(
                "okx private position skipped without trusted ctVal: %s",
                venue_symbol,
            )
            continue

        # V1: use uTime from first row
        ts_str = row.get("uTime") or row.get("cTime")
        if ts_str:
            try:
                updated_at_ms = max(updated_at_ms, int(ts_str))
            except (ValueError, TypeError):
                pass

        pos_str = row.get("pos", "0")
        contracts = float(pos_str or 0)
        pos_side = row.get("posSide", "")
        if pos_side == "long":
            signed = abs(contracts)
        elif pos_side == "short":
            signed = -abs(contracts)
        else:
            signed = contracts

        # V1: convert contracts to base quantity
        net_positions[symbol] = net_positions.get(symbol, 0.0) + signed * ct_val

    loop = asyncio.get_running_loop()
    for symbol, size in net_positions.items():
        loop.create_task(private_state.update_position(symbol, size, updated_at_ms))


def handle_okx_private_message(
    private_state,
    symbol_map: dict[str, str],
    subscribe_messages: list[str],
    raw: str,
    subscribed: bool = False,
    ct_val_map: Optional[dict[str, float]] = None,
) -> tuple[Optional[list[str]], bool]:
    """V1 handle_okx_private_message() — returns subscribe payloads if needed.

    Returns (subscribe_messages_to_send, new_subscribed_state).
    """
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None, subscribed

    # Login ack handling
    event = payload.get("event", "")
    code = str(payload.get("code", ""))

    if event == "login" and code == "0":
        return subscribe_messages, True  # subscribed

    if event == "subscribe":
        return None, subscribed

    # Pong response — just acknowledge
    if "data" not in payload:
        msg = payload.get("msg", "")
        if isinstance(msg, str) and msg.lower() == "pong":
            return None, subscribed

    if not subscribed:
        return None, subscribed

    arg = payload.get("arg")
    if arg is None:
        return None, subscribed

    channel = arg.get("channel", "")
    data = payload.get("data")
    if not isinstance(data, list):
        return None, subscribed

    if channel == "orders":
        for row in data:
            if isinstance(row, dict):
                _parse_okx_order_data(row, symbol_map, private_state, ct_val_map or {})
    elif channel == "positions":
        _parse_okx_position_data(data, symbol_map, private_state, ct_val_map or {})

    return None, subscribed


# ---------------------------------------------------------------------------
# OKX private WS worker
# ---------------------------------------------------------------------------


async def _fetch_okx_server_timestamp(transport) -> str:
    """V1 fetch_okx_server_timestamp_ms() — returns timestamp string for signing."""
    raw = await transport._request(
        "GET",
        "/api/v5/public/time",
        private=False,
    )
    data = raw.get("data", [])
    if isinstance(data, list) and data:
        ts = data[0].get("ts", "")
        if ts:
            return f"{float(ts) / 1000.0:.3f}"
    raise ValueError("failed to fetch okx server time")


async def _apply_okx_private_ws_symbol_updates(
    transport,
    ws,
    symbol_map: dict[str, str],
    active_symbol_map: dict[str, str],
    pending_subscriptions: dict[str, dict[str, Any]],
    ct_val_map: dict[str, float],
    unhealthy_after_failures: int,
) -> None:
    pending_updates = getattr(transport, "_private_ws_pending_symbol_updates", None)
    if not isinstance(pending_updates, set) or not pending_updates:
        return

    additions = {
        str(venue_symbol): str(symbol_map[venue_symbol])
        for venue_symbol in pending_updates
        if str(venue_symbol) and venue_symbol in symbol_map
    }
    if not additions:
        return

    update_ct_val_map = _build_okx_ct_val_map(transport, additions)
    missing_ct_val = sorted(set(additions) - set(update_ct_val_map))
    if missing_ct_val:
        reported_missing = getattr(
            transport,
            "_private_ws_pending_symbol_update_missing_ctval",
            set(),
        )
        if not isinstance(reported_missing, set):
            reported_missing = set()
        newly_missing = [
            venue_symbol
            for venue_symbol in missing_ct_val
            if venue_symbol not in reported_missing
        ]
        if newly_missing:
            transport.record_private_ws_failure(
                _now_ms(),
                "okx private ws ctVal metadata missing for symbol update: "
                + ",".join(newly_missing[:10]),
                unhealthy_after_failures,
            )
        reported_missing.update(missing_ct_val)
        transport._private_ws_pending_symbol_update_missing_ctval = reported_missing

    ready_additions = {
        venue_symbol: symbol
        for venue_symbol, symbol in additions.items()
        if venue_symbol in update_ct_val_map
    }
    if not ready_additions:
        return

    symbol_map.update(ready_additions)
    ct_val_map.update(
        {
            venue_symbol: update_ct_val_map[venue_symbol]
            for venue_symbol in ready_additions
        }
    )
    unsent_additions = {
        venue_symbol: symbol
        for venue_symbol, symbol in ready_additions.items()
        if venue_symbol not in active_symbol_map
        and not _okx_subscription_pending_for_inst(
            pending_subscriptions,
            venue_symbol,
        )
    }
    for subscribe_message in _register_okx_private_subscription_requests(
        transport,
        unsent_additions,
        pending_subscriptions,
    ):
        await _send_okx_private_subscription_message(
            transport,
            ws,
            pending_subscriptions,
            subscribe_message,
            unhealthy_after_failures,
        )

    reported_missing = getattr(
        transport,
        "_private_ws_pending_symbol_update_missing_ctval",
        set(),
    )
    if isinstance(reported_missing, set):
        reported_missing.difference_update(ready_additions)


async def _okx_private_ws_loop(
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
    ct_val_map: Optional[dict[str, float]] = None,
) -> None:
    """V1 OKX private WS loop — connect, login, subscribe, message + watchdog loop."""
    from lightfee.marketdata.resilience import compute_backoff_ms

    if ct_val_map is None:
        ct_val_map = {}
    failures = 0

    while True:
        # Rebuild from the current shared map before each real transport
        # reconnect.  The same map also receives in-band additions below.
        subscribe_messages = _build_okx_private_subscribe_messages(symbol_map)
        # 1) Connect
        try:
            ws = await websockets.connect(ws_url)
        except Exception as e:
            transport.record_private_ws_failure(
                _now_ms(),
                f"okx private ws connect failed: {e}",
                unhealthy_after_failures,
            )
            failures += 1
            delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
            await asyncio.sleep(delay / 1000.0)
            continue

        transport.record_private_ws_success(_now_ms())

        # 2) Fetch server timestamp
        try:
            timestamp = await _fetch_okx_server_timestamp(transport)
        except Exception as e:
            transport.record_private_ws_failure(
                _now_ms(),
                f"okx server time fetch failed: {e}",
                unhealthy_after_failures,
            )
            failures += 1
            delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
            await ws.close()
            await asyncio.sleep(delay / 1000.0)
            continue

        pending_subscriptions: dict[str, dict[str, Any]] = {}
        acked_channels_by_inst: dict[str, set[str]] = {}
        active_symbol_map: dict[str, str] = {}
        transport._okx_private_ws_active_symbol_map = active_symbol_map
        subscribe_messages = _register_okx_private_subscription_requests(
            transport,
            _okx_symbol_map_with_ct_val(symbol_map, ct_val_map),
            pending_subscriptions,
        )

        # 3) Sign login
        sign_message = f"{timestamp}GET/users/self/verify"
        signature = _okx_hmac_sha256_base64(api_secret, sign_message)
        login = json.dumps(
            {
                "op": "login",
                "args": [
                    {
                        "apiKey": api_key,
                        "passphrase": api_passphrase,
                        "timestamp": timestamp,
                        "sign": signature,
                    }
                ],
            }
        )

        try:
            await ws.send(login)
        except Exception as e:
            transport.record_private_ws_failure(
                _now_ms(),
                f"okx login send failed: {e}",
                unhealthy_after_failures,
            )
            failures += 1
            delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
            await ws.close()
            await asyncio.sleep(delay / 1000.0)
            continue

        # 4) Message loop with ping + watchdog
        subscribed = False
        last_message_ms = _now_ms()

        async def _ping_loop():
            while True:
                await asyncio.sleep(OKX_PRIVATE_WS_PING_INTERVAL_SECS)
                try:
                    await ws.send("ping")
                except Exception:
                    break

        async def _watchdog_loop():
            while True:
                await asyncio.sleep(OKX_PRIVATE_WS_WATCHDOG_TICK_SECS)
                now = _now_ms()
                if (
                    now - last_message_ms
                    > OKX_PRIVATE_WS_IDLE_TIMEOUT_SECS * 1000
                ):
                    transport.record_private_ws_failure(
                        now,
                        "okx private ws idle timeout",
                        unhealthy_after_failures,
                    )
                    break

        ping_task = asyncio.create_task(_ping_loop())
        watchdog_task = asyncio.create_task(_watchdog_loop())

        try:
            while True:
                if subscribed:
                    await _retry_okx_private_subscriptions(
                        transport,
                        ws,
                        pending_subscriptions,
                        unhealthy_after_failures,
                    )
                    await _apply_okx_private_ws_symbol_updates(
                        transport,
                        ws,
                        symbol_map,
                        active_symbol_map,
                        pending_subscriptions,
                        ct_val_map,
                        unhealthy_after_failures,
                    )
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    if subscribed:
                        await _retry_okx_private_subscriptions(
                            transport,
                            ws,
                            pending_subscriptions,
                            unhealthy_after_failures,
                        )
                        await _apply_okx_private_ws_symbol_updates(
                            transport,
                            ws,
                            symbol_map,
                            active_symbol_map,
                            pending_subscriptions,
                            ct_val_map,
                            unhealthy_after_failures,
                        )
                    continue
                except ConnectionClosed as e:
                    transport.record_private_ws_failure(
                        _now_ms(),
                        f"okx private ws closed: {e}",
                        unhealthy_after_failures,
                    )
                    break

                if isinstance(message, bytes):
                    continue

                last_message_ms = _now_ms()
                transport.record_private_ws_success(last_message_ms)

                payload = None
                try:
                    parsed = json.loads(message)
                    if isinstance(parsed, dict):
                        payload = parsed
                except json.JSONDecodeError:
                    payload = None
                if isinstance(payload, dict):
                    _apply_okx_private_subscription_event(
                        transport,
                        payload,
                        pending_subscriptions,
                        active_symbol_map,
                        symbol_map,
                        acked_channels_by_inst,
                        unhealthy_after_failures,
                    )

                to_send, subscribed = handle_okx_private_message(
                    private_state,
                    active_symbol_map,
                    subscribe_messages,
                    message,
                    subscribed,
                    ct_val_map,
                )

                if to_send:
                    for sub_msg in to_send:
                        await _send_okx_private_subscription_message(
                            transport,
                            ws,
                            pending_subscriptions,
                            sub_msg,
                            unhealthy_after_failures,
                        )
                if subscribed:
                    await _retry_okx_private_subscriptions(
                        transport,
                        ws,
                        pending_subscriptions,
                        unhealthy_after_failures,
                    )
                    await _apply_okx_private_ws_symbol_updates(
                        transport,
                        ws,
                        symbol_map,
                        active_symbol_map,
                        pending_subscriptions,
                        ct_val_map,
                        unhealthy_after_failures,
                    )

        except Exception as e:
            transport.record_private_ws_failure(
                _now_ms(),
                f"okx private ws receive failed: {e}",
                unhealthy_after_failures,
            )
        finally:
            ping_task.cancel()
            watchdog_task.cancel()
            try:
                await ping_task
            except asyncio.CancelledError:
                pass
            try:
                await watchdog_task
            except asyncio.CancelledError:
                pass
            await ws.close()

        failures += 1
        delay = compute_backoff_ms(reconnect_initial_ms, reconnect_max_ms, failures)
        await asyncio.sleep(delay / 1000.0)


def _build_okx_ct_val_map(transport, symbol_map: dict[str, str]) -> dict[str, float]:
    """Build ct_val map from trusted contract metadata — vendor_sym → ct_val.

    metadata is keyed by canonical symbol, but may also be keyed by vendor sym.
    Do not default SWAP instruments to 1.0: OKX private position/order sizes
    are contracts, and a silent fallback pollutes base-unit state.
    """
    ct_val_map: dict[str, float] = {}
    metadata = getattr(transport, '_symbol_metadata', {}) or {}
    for vendor_sym, canonical_sym in symbol_map.items():
        meta = metadata.get(canonical_sym) or metadata.get(vendor_sym) or {}
        ct_val = 0.0
        for key in ("ct_val", "ctVal", "contract_size", "contractSize"):
            try:
                ct_val = float(meta.get(key, 0) or 0)
            except (TypeError, ValueError):
                ct_val = 0.0
            if ct_val > 0:
                break
        if ct_val > 0:
            ct_val_map[vendor_sym] = ct_val
    return ct_val_map


def start_okx_private_ws(transport, symbols: list[str]) -> None:
    """V1 start_private_ws() for OKX."""
    credential = transport._credential
    if credential is None or not credential.api_key:
        return
    if not symbols:
        return

    api_key = credential.api_key
    api_secret = credential.api_secret
    api_passphrase = credential.api_passphrase or ""
    base_url = transport._spec.private_base_url.rstrip("/")
    ws_url = _okx_private_ws_url(base_url)
    private_state = transport._private_ws_state

    symbol_map = _shared_okx_private_ws_symbol_map(transport, symbols)
    ct_val_map = _build_okx_ct_val_map(transport, symbol_map)
    missing_ct_val = sorted(set(symbol_map) - set(ct_val_map))
    if missing_ct_val:
        transport.record_private_ws_failure(
            _now_ms(),
            "okx private ws ctVal metadata missing: "
            + ",".join(missing_ct_val[:10]),
            unhealthy_after=1,
        )
        logger.warning(
            "okx private WS not started: missing ctVal for %d symbols",
            len(missing_ct_val),
        )
        return

    task = asyncio.create_task(
        _okx_private_ws_loop(
            transport=transport,
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
            ws_url=ws_url,
            symbol_map=symbol_map,
            private_state=private_state,
            unhealthy_after_failures=5,
            reconnect_initial_ms=1_000,
            reconnect_max_ms=60_000,
            ct_val_map=ct_val_map,
        )
    )
    private_state.push_worker(task)
    logger.info("okx private WS worker started for %d symbols", len(symbols))
