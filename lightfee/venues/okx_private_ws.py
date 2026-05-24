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

# ---------------------------------------------------------------------------
# Constants (V1 exact)
# ---------------------------------------------------------------------------

OKX_PRIVATE_WS_IDLE_TIMEOUT_SECS = 90
OKX_PRIVATE_WS_WATCHDOG_TICK_SECS = 5
OKX_PRIVATE_WS_PING_INTERVAL_SECS = 20


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

    subscribe_messages = _build_okx_private_subscribe_messages(symbol_map)
    failures = 0

    while True:
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
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
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

                to_send, subscribed = handle_okx_private_message(
                    private_state,
                    symbol_map,
                    subscribe_messages,
                    message,
                    subscribed,
                    ct_val_map,
                )

                if to_send:
                    for sub_msg in to_send:
                        await ws.send(sub_msg)

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

    symbol_map = {transport._venue_symbol(s): s for s in symbols}
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
