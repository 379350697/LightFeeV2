#!/usr/bin/env python3
"""Probe real exchange payloads for local-L2 regression examples.

This is intentionally a live public-API probe, not a unit test.  It fetches the
same classes of payloads that previously failed in production and verifies that
the current code parses or filters them with the expected exchange semantics.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from lightfee.marketdata.l2 import LocalL2UpdateKind
from lightfee.marketdata.local_l2_venues import parse_l2_update
from lightfee.venues.binance import BinanceAdapter
from lightfee.venues.bitget import BitgetAdapter


@dataclass
class ProbeResult:
    name: str
    ok: bool
    detail: dict[str, Any] = field(default_factory=dict)
    error: str = ""


def _now_ms() -> int:
    return int(time.time() * 1000)


def _update_detail(update) -> dict[str, Any]:
    return {
        "venue": update.venue,
        "symbol": update.symbol,
        "kind": update.update_kind.value,
        "sequence": update.sequence,
        "previous_sequence": update.previous_sequence,
        "bid_count": len(update.bids),
        "ask_count": len(update.asks),
        "event_time_ms": update.event_time_ms,
    }


def _record_sync(name: str, fn) -> ProbeResult:
    try:
        return ProbeResult(name=name, ok=True, detail=fn())
    except Exception as exc:
        return ProbeResult(name=name, ok=False, error=f"{type(exc).__name__}: {exc}")


def _record_async(name: str, fn, timeout_s: float) -> ProbeResult:
    async def runner() -> dict[str, Any]:
        return await asyncio.wait_for(fn(), timeout=timeout_s)

    try:
        return ProbeResult(name=name, ok=True, detail=asyncio.run(runner()))
    except Exception as exc:
        return ProbeResult(name=name, ok=False, error=f"{type(exc).__name__}: {exc}")


def _get_json(url: str, params: dict[str, Any], timeout_s: float) -> dict[str, Any]:
    query = urlencode(params)
    request_url = f"{url}?{query}" if query else url
    request = Request(request_url, headers={"User-Agent": "LightFeeV2-regression-probe/1.0"})
    with urlopen(request, timeout=timeout_s) as response:
        body = response.read().decode("utf-8")
    raw = json.loads(body)
    if not isinstance(raw, dict):
        raise AssertionError(f"expected JSON object from {url}, got {type(raw).__name__}")
    return raw


def _binance_rest_depth_snapshot(timeout_s: float) -> dict[str, Any]:
    raw = _get_json(
        "https://fapi.binance.com/fapi/v1/depth",
        {"symbol": "BTCUSDT", "limit": "5"},
        timeout_s,
    )
    update = parse_l2_update("binance", raw, symbol="BTCUSDT", now_ms=_now_ms())
    if update.update_kind != LocalL2UpdateKind.SNAPSHOT:
        raise AssertionError(f"expected snapshot, got {update.update_kind.value}")
    if not update.bids or not update.asks:
        raise AssertionError("expected non-empty Binance BTCUSDT bid/ask sides")
    return _update_detail(update)


def _okx_rest_books_snapshot(timeout_s: float) -> dict[str, Any]:
    raw = _get_json(
        "https://www.okx.com/api/v5/market/books",
        {"instId": "BTC-USDT-SWAP", "sz": "5"},
        timeout_s,
    )
    update = parse_l2_update("okx", raw, symbol="BTC-USDT-SWAP", now_ms=_now_ms())
    if update.update_kind != LocalL2UpdateKind.SNAPSHOT:
        raise AssertionError(f"expected snapshot, got {update.update_kind.value}")
    if not update.bids or not update.asks:
        raise AssertionError("expected non-empty OKX BTC-USDT-SWAP bid/ask sides")
    return _update_detail(update)


def _bybit_rest_orderbook_snapshot(timeout_s: float) -> dict[str, Any]:
    raw = _get_json(
        "https://api.bybit.com/v5/market/orderbook",
        {"category": "linear", "symbol": "BTCUSDT", "limit": "50"},
        timeout_s,
    )
    update = parse_l2_update("bybit", raw, symbol="BTCUSDT", now_ms=_now_ms())
    if update.update_kind != LocalL2UpdateKind.SNAPSHOT:
        raise AssertionError(f"expected snapshot, got {update.update_kind.value}")
    if not update.bids or not update.asks:
        raise AssertionError("expected non-empty Bybit BTCUSDT bid/ask sides")
    return _update_detail(update)


def _bitget_rest_orderbook_snapshot(timeout_s: float) -> dict[str, Any]:
    raw = _get_json(
        "https://api.bitget.com/api/v3/market/orderbook",
        {"category": "USDT-FUTURES", "symbol": "BTCUSDT", "limit": "5"},
        timeout_s,
    )
    update = parse_l2_update("bitget", raw, symbol="BTCUSDT", now_ms=_now_ms())
    if update.update_kind != LocalL2UpdateKind.SNAPSHOT:
        raise AssertionError(f"expected snapshot, got {update.update_kind.value}")
    if not update.bids or not update.asks:
        raise AssertionError("expected non-empty Bitget BTCUSDT bid/ask sides")
    return _update_detail(update)


async def _binance_catalog_filters_settling_symbol(timeout_s: float) -> dict[str, Any]:
    raw = _get_json(
        "https://fapi.binance.com/fapi/v1/exchangeInfo",
        {},
        timeout_s,
    )
    adapter = BinanceAdapter(mode="paper")
    try:
        rows = raw.get("symbols", []) if isinstance(raw, dict) else []
        sys_row = next(
            (row for row in rows if isinstance(row, dict) and row.get("symbol") == "SYSUSDT"),
            None,
        )

        async def mock_request(method: str, path: str, **kwargs):
            if method != "GET" or path != "/fapi/v1/exchangeInfo":
                raise AssertionError(f"unexpected Binance catalog request: {method} {path}")
            return raw

        adapter._transport._request = mock_request
        await adapter.ensure_supported_symbols_loaded()
        supported = set(adapter.supported_symbols())
        if "BTCUSDT" not in supported:
            raise AssertionError("BTCUSDT missing from Binance supported symbol catalog")
        if sys_row is not None and sys_row.get("status") != "TRADING" and "SYSUSDT" in supported:
            raise AssertionError(f"SYSUSDT status={sys_row.get('status')} leaked into catalog")
        return {
            "supported_count": len(supported),
            "btc_supported": "BTCUSDT" in supported,
            "sys_status": sys_row.get("status") if sys_row else "missing",
            "sys_contract_type": sys_row.get("contractType") if sys_row else "missing",
            "sys_supported": "SYSUSDT" in supported,
        }
    finally:
        try:
            await asyncio.wait_for(adapter.shutdown(), timeout=1.0)
        except Exception:
            pass


async def _bitget_catalog_loads_public_contracts(timeout_s: float) -> dict[str, Any]:
    raw = _get_json(
        "https://api.bitget.com/api/v2/mix/market/contracts",
        {"productType": "USDT-FUTURES"},
        timeout_s,
    )
    adapter = BitgetAdapter(mode="paper")
    try:
        async def mock_request(method: str, path: str, **kwargs):
            if method != "GET" or path != "/api/v2/mix/market/contracts":
                raise AssertionError(f"unexpected Bitget catalog request: {method} {path}")
            return raw

        adapter._transport._request = mock_request
        await adapter.ensure_supported_symbols_loaded()
        supported = set(adapter.supported_symbols())
        if "BTCUSDT" not in supported:
            raise AssertionError("BTCUSDT missing from Bitget supported symbol catalog")
        return {
            "supported_count": len(supported),
            "btc_supported": "BTCUSDT" in supported,
        }
    finally:
        try:
            await asyncio.wait_for(adapter.shutdown(), timeout=1.0)
        except Exception:
            pass


def run(timeout_s: float) -> list[ProbeResult]:
    return [
        _record_sync("binance_rest_depth_snapshot", lambda: _binance_rest_depth_snapshot(timeout_s)),
        _record_sync("okx_rest_books_snapshot", lambda: _okx_rest_books_snapshot(timeout_s)),
        _record_sync("bybit_rest_orderbook_snapshot", lambda: _bybit_rest_orderbook_snapshot(timeout_s)),
        _record_sync("bitget_rest_orderbook_snapshot", lambda: _bitget_rest_orderbook_snapshot(timeout_s)),
        _record_async(
            "binance_catalog_filters_settling_symbol",
            lambda: _binance_catalog_filters_settling_symbol(timeout_s),
            timeout_s,
        ),
        _record_async(
            "bitget_catalog_loads_public_contracts",
            lambda: _bitget_catalog_loads_public_contracts(timeout_s),
            timeout_s,
        ),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout-s", type=float, default=12.0)
    parser.add_argument("--json", action="store_true", help="emit JSON only")
    args = parser.parse_args()

    results = run(args.timeout_s)
    payload = {
        "ok": all(result.ok for result in results),
        "results": [result.__dict__ for result in results],
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    else:
        for result in results:
            status = "ok" if result.ok else "fail"
            print(f"{status} {result.name} {json.dumps(result.detail if result.ok else {'error': result.error}, ensure_ascii=False, sort_keys=True)}")
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
