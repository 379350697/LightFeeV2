#!/usr/bin/env python3
"""Probe the production Local-L2 WebSocket client with public market data.

This is a short, read-only diagnostic: it starts the same LocalL2WsClient used
by the live data plane, waits for parsed updates, then prints the persisted
transport evidence.  It never uses exchange credentials or submits orders.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
import time
from pathlib import Path
from typing import Any

from lightfee.marketdata.l2 import LocalL2BookKey
from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
from lightfee.marketdata.local_l2_runtime import LocalL2Runtime
from lightfee.marketdata.local_l2_ws import create_ws_client
from lightfee.persistence.journal import Journal


def _transport_events(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        record["payload"]
        for record in records
        if record.get("kind") == "runtime.local_l2_ws_transport"
    ]


def _rebuild_events(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "kind": record["kind"],
            "payload": record["payload"],
        }
        for record in records
        if record.get("kind") in {
            "runtime.local_l2_sequence_gap_rebuild",
            "runtime.local_l2_buffered_replay_rebuild",
            "runtime.local_l2_hot_stale_rebuild",
            "runtime.local_l2_hot_stale_awaiting_ws_delta",
        }
    ]


async def probe(venue: str, symbol: str, duration_s: float, min_messages: int) -> dict[str, Any]:
    """Run one production client through its real public WS connection path."""
    with tempfile.TemporaryDirectory(prefix="lightfee-local-l2-probe-") as tmpdir:
        journal = Journal(Path(tmpdir) / "probe.journal")
        journal.open()
        runtime = LocalL2Runtime()
        runtime.ensure_book(venue, symbol)
        data_plane = LocalL2DataPlane(runtime, journal)
        client = create_ws_client(venue, symbol, data_plane)
        if client is None:
            journal.close()
            return {
                "ok": False,
                "venue": venue,
                "symbol": symbol,
                "error": "no Local-L2 WebSocket client registered for venue",
            }

        data_plane.start_worker(LocalL2BookKey(venue, symbol), client)
        started = time.monotonic()
        state_before_stop: dict[str, Any] = {}
        try:
            await client.start()
            while time.monotonic() - started < duration_s:
                state_before_stop = data_plane.ws_stream_state(venue, symbol)
                if int(state_before_stop["message_count"]) >= min_messages:
                    break
                await asyncio.sleep(0.1)
        finally:
            state_before_stop = data_plane.ws_stream_state(venue, symbol)
            await data_plane.stop_ws_streams()
            records = journal.read_all()
            journal.close()

    transport = _transport_events(records)
    rebuilds = _rebuild_events(records)
    explicit_subscription = state_before_stop["subscription_mode"] == "explicit"
    subscription_ok = (
        not explicit_subscription
        or int(state_before_stop["last_subscription_confirmed_ms"]) > 0
    )
    received_enough = int(state_before_stop["message_count"]) >= min_messages
    connected = int(state_before_stop["last_connected_ms"]) > 0
    return {
        "ok": bool(connected and subscription_ok and received_enough),
        "venue": venue,
        "symbol": symbol,
        "duration_s": round(time.monotonic() - started, 3),
        "minimum_messages": min_messages,
        # The production client also leaves proxy at websockets.connect's
        # default (True), so this probe follows the same Clash/environment route.
        "websockets_proxy_mode": "library_default_true",
        "stream_state_before_stop": state_before_stop,
        "transport_events": transport,
        "rebuild_events": rebuilds,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--venue", required=True, choices=("aster", "binance", "bybit"))
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--duration-s", type=float, default=12.0)
    parser.add_argument("--min-messages", type=int, default=2)
    args = parser.parse_args()
    if args.duration_s <= 0 or args.min_messages <= 0:
        parser.error("--duration-s and --min-messages must be positive")

    result = asyncio.run(
        probe(args.venue, args.symbol.upper(), args.duration_s, args.min_messages)
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
