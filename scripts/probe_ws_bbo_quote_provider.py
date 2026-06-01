#!/usr/bin/env python3
"""Read-only public WS BBO probe for the entry quote provider."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from typing import Any

from lightfee.marketdata.ws_bbo import VenueBboCache, VenueBboDataPlane


async def probe(args: argparse.Namespace) -> dict[str, Any]:
    venue = str(args.venue or "").strip().lower()
    symbol = str(args.symbol or "").strip().upper()
    duration_s = max(float(getattr(args, "duration_s", 3.0) or 3.0), 0.5)
    cache = VenueBboCache()
    data_plane = VenueBboDataPlane(cache=cache)
    started = data_plane.start_ws_streams(venue, [symbol])
    if started <= 0:
        return {
            "ok": False,
            "venue": venue,
            "symbol": symbol,
            "source": "ws_bbo_quote_provider",
            "classification": "unsupported_venue_or_symbol",
            "stream_state": data_plane.stream_state(venue, symbol),
        }

    deadline = time.monotonic() + duration_s
    try:
        await data_plane.connect_ws_streams()
        quote = None
        while time.monotonic() < deadline:
            quote = cache.get_quote(venue, symbol)
            if quote is not None:
                break
            await asyncio.sleep(0.05)
        state = data_plane.stream_state(venue, symbol)
        if quote is None:
            return {
                "ok": False,
                "venue": venue,
                "symbol": symbol,
                "source": "ws_bbo_quote_provider",
                "classification": "missing_quote",
                "stream_state": state,
            }
        return {
            "ok": True,
            "venue": quote.venue,
            "symbol": quote.symbol,
            "source": "ws_bbo_quote_provider",
            "classification": "quote_received",
            "bid": quote.bid,
            "ask": quote.ask,
            "bid_size": quote.bid_size,
            "ask_size": quote.ask_size,
            "observed_at_ms": quote.observed_at_ms,
            "received_at_ms": quote.received_at_ms,
            "quote_source": quote.source,
            "stream_state": state,
            "readiness_effect": "evidence_only_no_runtime_state",
        }
    finally:
        await data_plane.stop_ws_streams(per_client_timeout_s=1.0)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only public WS BBO probe for entry quote readiness"
    )
    parser.add_argument("--venue", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--duration-s", type=float, default=3.0)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(probe(args)), sort_keys=True))


if __name__ == "__main__":
    main()
