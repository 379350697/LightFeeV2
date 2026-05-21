#!/usr/bin/env python3
"""Live public-network probe for Local-L2 REST/WS bridge validation.

Captures real exchange payloads and computes old-vs-new bridge decisions
using the venue policy module.  For each venue:
  - Binance: WS diff-depth + REST /fapi/v1/depth bridge validation
  - Bybit: WS orderbook.50 snapshot/delta + REST /v5/market/orderbook
    sequence-domain comparison
  - OKX: WS books snapshot/update + keepalive/reset/checksum capture
  - Bitget: UTA depth subscribe request/response + seq/pseq capture
  - Gate: legacy futures.order_book and futures.order_book_update schema capture
  - Hyperliquid: /info l2Book poller freshness

No API secrets are required — this uses public market data endpoints only.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from typing import Optional

import websockets

from lightfee.core.domain import Venue
from lightfee.marketdata.local_l2_policy import policy_for_venue
from lightfee.venues.specs import get_spec

JsonObject = dict
ProbeHandler = Callable[[argparse.Namespace], Awaitable[JsonObject]]
HANDLERS: dict[str, ProbeHandler] = {}


def _wire_symbol(venue: str, symbol: str) -> str:
    spec = get_spec(Venue.from_str(venue))
    canonical = symbol.upper()
    if spec.symbol_to_venue is None:
        return canonical
    return spec.symbol_to_venue(canonical)


def _hyperliquid_level_counts(data: dict) -> tuple[int, int]:
    levels = data.get("levels", []) if isinstance(data, dict) else []
    bids = levels[0] if isinstance(levels, list) and len(levels) > 0 else []
    asks = levels[1] if isinstance(levels, list) and len(levels) > 1 else []

    def _nonzero_count(side: object) -> int:
        if not isinstance(side, list):
            return 0
        count = 0
        for level in side:
            if not isinstance(level, dict):
                continue
            try:
                if float(level.get("sz", 0) or 0) > 0:
                    count += 1
            except (TypeError, ValueError):
                continue
        return count

    return _nonzero_count(bids), _nonzero_count(asks)


def _gate_subscribe_message(pair: str, now_s: Optional[int] = None) -> dict:
    # Gate's legacy futures.order_book channel expects interval "0".
    # The "100ms" interval is for futures.order_book_update, not this channel.
    return {
        "time": int(time.time()) if now_s is None else now_s,
        "channel": "futures.order_book",
        "event": "subscribe",
        "payload": [pair, "20", "0"],
    }

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


async def probe(args: argparse.Namespace) -> JsonObject:
    venue = args.venue.lower()
    handler = HANDLERS.get(venue)
    if handler is None:
        return {
            "ok": False,
            "venue": venue,
            "symbol": args.symbol.upper(),
            "duration_s": args.duration_s,
            "error": f"unsupported venue: {venue}",
            "supported_venues": sorted(HANDLERS),
        }
    return await handler(args)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Local-L2 live public probe — REST/WS bridge validation"
    )
    parser.add_argument("--venue", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--duration-s", type=float, default=20.0)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    result = asyncio.run(probe(args))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result.get("ok") else 1


# ---------------------------------------------------------------------------
# Bybit
# ---------------------------------------------------------------------------


async def _probe_bybit(args: argparse.Namespace) -> JsonObject:
    symbol = args.symbol.upper()
    wire_symbol = _wire_symbol("bybit", symbol)
    ws_events: list[dict] = []
    started_at = time.time()

    async def _collect_ws():
        url = "wss://stream.bybit.com/v5/public/linear"
        async with websockets.connect(url, open_timeout=10.0) as ws:
            sub = {"op": "subscribe", "args": [f"orderbook.50.{wire_symbol}"]}
            await ws.send(json.dumps(sub))
            while time.time() - started_at < args.duration_s:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    ws_events.append(json.loads(raw))
                except asyncio.TimeoutError:
                    ws_events.append({"probe_note": "ws_timeout"})
                    break

    try:
        await asyncio.wait_for(_collect_ws(), timeout=args.duration_s + 5.0)
    except asyncio.TimeoutError:
        ws_events.append({"probe_note": "probe_timeout"})
    except Exception as exc:
        return {
            "ok": False, "venue": "bybit", "symbol": symbol,
            "error": f"ws_error: {exc}",
        }

    # Classify captured events
    snapshots = [e for e in ws_events if e.get("type") == "snapshot"]
    deltas = [e for e in ws_events if e.get("type") == "delta"]
    first_snapshot = snapshots[0] if snapshots else None
    ws_u = first_snapshot.get("data", {}).get("u") if first_snapshot else None
    ws_seq = first_snapshot.get("data", {}).get("seq") if first_snapshot else None

    # Compute old stale decision for comparison
    policy = policy_for_venue("bybit")
    sequence_comparable = policy.rest_snapshot_sequence_comparable
    rest_u = None
    rest_seq = None
    old_stale = None

    # The REST u field at 1000-level depth is NOT comparable to WS orderbook.50 u
    try:
        import urllib.request
        req = urllib.request.Request(
            f"https://api.bybit.com/v5/market/orderbook?category=linear&symbol={wire_symbol}&limit=50",
            headers={"Accept": "application/json"},
        )
        resp = urllib.request.urlopen(req, timeout=10.0)
        rest_data = json.loads(resp.read())
        if rest_data.get("retCode") == 0:
            result = rest_data.get("result", {})
            rest_u = result.get("u")
            rest_seq = result.get("seq")
            old_stale = (rest_u is not None and ws_u is not None and rest_u < ws_u)
    except Exception as exc:
        rest_u = f"fetch_error: {exc}"

    return {
        "ok": True,
        "venue": "bybit",
        "symbol": symbol,
        "wire_symbol": wire_symbol,
        "depth": 50,
        "bridge_mode": policy.bridge_mode.value,
        "ws_u": ws_u,
        "ws_seq": ws_seq,
        "rest_u": rest_u,
        "rest_seq": rest_seq,
        "sequence_comparable": sequence_comparable,
        "replay_rest_snapshot_with_ws_deltas": policy.replay_rest_snapshot_with_ws_deltas,
        "old_stale_decision": old_stale,
        "snapshot_count": len(snapshots),
        "delta_count": len(deltas),
        "events": ws_events[:20],
    }


HANDLERS["bybit"] = _probe_bybit


# ---------------------------------------------------------------------------
# Binance
# ---------------------------------------------------------------------------


async def _probe_binance(args: argparse.Namespace) -> JsonObject:
    symbol = args.symbol.upper()
    wire_symbol = _wire_symbol("binance", symbol)
    lower = wire_symbol.lower()
    ws_events: list[dict] = []
    started_at = time.time()

    async def _collect_ws():
        url = f"wss://fstream.binance.com/ws/{lower}@depth"
        async with websockets.connect(url, open_timeout=10.0) as ws:
            while time.time() - started_at < args.duration_s:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    ws_events.append(json.loads(raw))
                except asyncio.TimeoutError:
                    ws_events.append({"probe_note": "ws_timeout"})
                    break

    try:
        await asyncio.wait_for(_collect_ws(), timeout=args.duration_s + 5.0)
    except asyncio.TimeoutError:
        ws_events.append({"probe_note": "probe_timeout"})
    except Exception as exc:
        return {"ok": False, "venue": "binance", "symbol": symbol, "error": f"ws_error: {exc}"}

    policy = policy_for_venue("binance")
    cap_512_overflow = len(ws_events) >= 512
    cap_4096_bridge_preserved = len(ws_events) < 4096

    # Fetch REST snapshot and compute official bridge start
    rest_u = None
    rest_U = None
    bridge_ok = None
    try:
        import urllib.request
        req_full = urllib.request.Request(
            f"https://fapi.binance.com/fapi/v1/depth?symbol={wire_symbol}&limit=50",
            headers={"Accept": "application/json"},
        )
        resp = urllib.request.urlopen(req_full, timeout=10.0)
        rest_data = json.loads(resp.read())
        rest_u = rest_data.get("lastUpdateId")
        rest_U = rest_data.get("lastUpdateId")  # same field
        if rest_u and ws_events and not isinstance(ws_events[0].get("probe_note"), str):
            first_delta = ws_events[0]
            U = first_delta.get("U", 0)
            u = first_delta.get("u", 0)
            bridge_ok = U <= rest_u <= u
    except Exception as exc:
        rest_u = f"fetch_error: {exc}"

    return {
        "ok": True,
        "venue": "binance",
        "symbol": symbol,
        "wire_symbol": wire_symbol,
        "depth": 50,
        "bridge_mode": policy.bridge_mode.value,
        "pre_snapshot_buffer_cap": policy.pre_snapshot_buffer_cap,
        "rest_lastUpdateId": rest_u,
        "rest_U": rest_U,
        "bridge_ok": bridge_ok,
        "cap_512_would_overflow": cap_512_overflow,
        "cap_4096_would_preserve_bridge": cap_4096_bridge_preserved,
        "ws_event_count": len(ws_events),
        "events": ws_events[:20],
    }


HANDLERS["binance"] = _probe_binance


# ---------------------------------------------------------------------------
# OKX
# ---------------------------------------------------------------------------


async def _probe_okx(args: argparse.Namespace) -> JsonObject:
    symbol = args.symbol.upper()
    inst_id = _wire_symbol("okx", symbol)
    ws_events: list[dict] = []
    started_at = time.time()

    async def _collect_ws():
        url = "wss://ws.okx.com:8443/ws/v5/public"
        async with websockets.connect(url, open_timeout=10.0) as ws:
            sub = {"op": "subscribe", "args": [{"channel": "books", "instId": inst_id}]}
            await ws.send(json.dumps(sub))
            while time.time() - started_at < args.duration_s:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    ws_events.append(json.loads(raw))
                except asyncio.TimeoutError:
                    ws_events.append({"probe_note": "ws_timeout"})
                    break

    try:
        await asyncio.wait_for(_collect_ws(), timeout=args.duration_s + 5.0)
    except asyncio.TimeoutError:
        ws_events.append({"probe_note": "probe_timeout"})
    except Exception as exc:
        return {"ok": False, "venue": "okx", "symbol": symbol, "error": f"ws_error: {exc}"}

    policy = policy_for_venue("okx")
    classifications: list[dict] = []
    prev_seq: Optional[int] = None

    for e in ws_events:
        data = e.get("data", [])
        if isinstance(data, list) and len(data) > 0:
            item = data[0]
            seq_id = int(item.get("seqId", 0))
            prev_seq_id = int(item.get("prevSeqId", 0))
            bids = item.get("bids", [])
            asks = item.get("asks", [])
            # Check for checksum field (OKX v5)
            checksum = item.get("checksum")
            kind = policy.classify_replay_link(
                previous_sequence=prev_seq or seq_id,
                sequence=seq_id,
                previous_sequence_from_update=prev_seq_id,
                bid_count=len(bids),
                ask_count=len(asks),
            )
            classifications.append({
                "seq_id": seq_id,
                "prev_seq_id": prev_seq_id,
                "bid_count": len(bids),
                "ask_count": len(asks),
                "action": e.get("action", ""),
                "checksum": checksum,
                "link_kind": kind.value,
            })
            prev_seq = seq_id

    return {
        "ok": True,
        "venue": "okx",
        "symbol": symbol,
        "inst_id": inst_id,
        "bridge_mode": policy.bridge_mode.value,
        "ws_event_count": len(ws_events),
        "classifications": classifications,
    }


HANDLERS["okx"] = _probe_okx


# ---------------------------------------------------------------------------
# Bitget UTA
# ---------------------------------------------------------------------------


async def _probe_bitget(args: argparse.Namespace) -> JsonObject:
    symbol = args.symbol.upper()
    wire_symbol = _wire_symbol("bitget", symbol)
    ws_events: list[dict] = []
    started_at = time.time()
    subscribe_sent: dict = {}
    subscribe_resp: Optional[dict] = None

    async def _collect_ws():
        nonlocal subscribe_resp
        url = "wss://ws.bitget.com/v2/ws/public"
        async with websockets.connect(url, open_timeout=10.0) as ws:
            sub = {
                "op": "subscribe",
                "args": [{"instType": "USDT-FUTURES", "channel": "books", "instId": wire_symbol}],
            }
            subscribe_sent.update(sub)
            await ws.send(json.dumps(sub))
            while time.time() - started_at < args.duration_s:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    evt = json.loads(raw)
                    ws_events.append(evt)
                    if subscribe_resp is None and evt.get("event") == "subscribe":
                        subscribe_resp = evt
                except asyncio.TimeoutError:
                    ws_events.append({"probe_note": "ws_timeout"})
                    break

    try:
        await asyncio.wait_for(_collect_ws(), timeout=args.duration_s + 5.0)
    except asyncio.TimeoutError:
        ws_events.append({"probe_note": "probe_timeout"})
    except Exception as exc:
        return {"ok": False, "venue": "bitget", "symbol": symbol, "error": f"ws_error: {exc}"}

    snapshots = [e for e in ws_events if e.get("action") == "snapshot"]
    updates = [e for e in ws_events if e.get("action") == "update"]
    first_data = None
    seq = pseq = None
    arg_channel = arg_topic = None
    for e in ws_events:
        if e.get("data") and isinstance(e["data"], list) and len(e["data"]) > 0:
            first_data = e["data"][0]
            seq = first_data.get("seq")
            pseq = first_data.get("pseq")
            arg = e.get("arg", {})
            arg_channel = arg.get("channel")
            arg_topic = arg.get("topic")
            break

    return {
        "ok": True,
        "venue": "bitget",
        "symbol": symbol,
        "wire_symbol": wire_symbol,
        "subscribe_request": subscribe_sent,
        "subscribe_response": subscribe_resp,
        "arg_channel": arg_channel,
        "arg_topic": arg_topic,
        "first_seq": seq,
        "first_pseq": pseq,
        "snapshot_count": len(snapshots),
        "update_count": len(updates),
        "events": ws_events[:10],
    }


HANDLERS["bitget"] = _probe_bitget


# ---------------------------------------------------------------------------
# Gate
# ---------------------------------------------------------------------------


async def _probe_gate(args: argparse.Namespace) -> JsonObject:
    symbol = args.symbol.upper()
    pair = _wire_symbol("gate", symbol)
    ws_events: list[dict] = []
    started_at = time.time()
    subscribe_request: dict = {}

    async def _collect_ws():
        url = "wss://fx-ws.gateio.ws/v4/ws/usdt"
        async with websockets.connect(url, open_timeout=10.0) as ws:
            sub = _gate_subscribe_message(pair)
            subscribe_request.update(sub)
            await ws.send(json.dumps(sub))
            while time.time() - started_at < args.duration_s:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    ws_events.append(json.loads(raw))
                except asyncio.TimeoutError:
                    ws_events.append({"probe_note": "ws_timeout"})
                    break

    try:
        await asyncio.wait_for(_collect_ws(), timeout=args.duration_s + 5.0)
    except asyncio.TimeoutError:
        ws_events.append({"probe_note": "probe_timeout"})
    except Exception as exc:
        return {"ok": False, "venue": "gate", "symbol": symbol, "error": f"ws_error: {exc}"}

    # Extract key schema fields
    channel_field = None
    full_field = None
    update_id = None
    first_depth_id = None
    subscribe_response = None
    subscribe_failed = False
    subscribe_error = None
    for e in ws_events:
        if subscribe_response is None and e.get("event") == "subscribe":
            subscribe_response = e
            result = e.get("result")
            subscribe_error = e.get("error")
            subscribe_failed = bool(subscribe_error)
            if isinstance(result, dict) and result.get("status") == "fail":
                subscribe_failed = True
        if subscribe_failed:
            continue
        result = e.get("result", {}) if "result" in e else e
        channel_field = e.get("channel", result.get("channel"))
        full_field = result.get("full")
        if result.get("id") is not None:
            update_id = result.get("id")
        if result.get("u") is not None:
            first_depth_id = result.get("u")
        break

    policy = policy_for_venue("gate")

    return {
        "ok": not subscribe_failed,
        "venue": "gate",
        "symbol": symbol,
        "pair": pair,
        "subscribe_request": subscribe_request,
        "subscribe_response": subscribe_response,
        "subscribe_failed": subscribe_failed,
        "subscribe_error": subscribe_error,
        "bridge_mode": policy.bridge_mode.value,
        "channel": channel_field,
        "full": full_field,
        "update_id": update_id,
        "first_depth_id": first_depth_id,
        "ws_event_count": len(ws_events),
        "events": ws_events[:10],
        "note": "Gate probe captures legacy futures.order_book schema; switch to order_book_update only if this channel proves unable to maintain readiness",
    }


HANDLERS["gate"] = _probe_gate


# ---------------------------------------------------------------------------
# Hyperliquid
# ---------------------------------------------------------------------------


async def _probe_hyperliquid(args: argparse.Namespace) -> JsonObject:
    symbol = args.symbol.upper()
    coin = _wire_symbol("hyperliquid", symbol)
    try:
        import urllib.request
        body = json.dumps({"type": "l2Book", "coin": coin}).encode()
        req = urllib.request.Request(
            "https://api.hyperliquid.xyz/info",
            data=body,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        resp = urllib.request.urlopen(req, timeout=10.0)
        data = json.loads(resp.read())
        bid_count, ask_count = _hyperliquid_level_counts(data)
        empty_side = bid_count == 0 or ask_count == 0

        return {
            "ok": True,
            "venue": "hyperliquid",
            "symbol": symbol,
            "wire_symbol": coin,
            "bridge_mode": policy_for_venue("hyperliquid").bridge_mode.value,
            "bid_levels": bid_count,
            "ask_levels": ask_count,
            "total_levels": bid_count + ask_count,
            "empty_side": empty_side,
            "response_keys": list(data.keys()) if isinstance(data, dict) else "non_dict",
        }
    except Exception as exc:
        return {"ok": False, "venue": "hyperliquid", "symbol": symbol, "error": f"fetch_error: {exc}"}


HANDLERS["hyperliquid"] = _probe_hyperliquid


if __name__ == "__main__":
    raise SystemExit(main())
