#!/usr/bin/env python3
"""Active Bybit lifecycle probe for small-size ACK-only/passive-close testing.

This probe is intentionally isolated from the production runtime.  It uses
existing venue adapter contracts, writes JSON to stdout, and refuses to submit
orders unless --confirm-live-orders is passed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from dataclasses import asdict, is_dataclass
from typing import Any

from lightfee.config.loader import load_config
from lightfee.core.domain import (
    OrderRequest,
    PassiveOrderState,
    PositionSnapshot,
    Side,
    TimeInForce,
    Venue,
)
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.venues.bybit import BybitAdapter
from lightfee.venues.registry import build_adapter, build_adapter_map
from lightfee.venues.transport import LiveCredential

MAX_ACTIVE_PROBE_NOTIONAL_QUOTE = 10.0
PROBE_CLIENT_PREFIX = "lfprobe"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Small active Bybit ACK-only/passive-close lifecycle probe"
    )
    parser.add_argument("--config", default="config/live.toml")
    parser.add_argument("--venue", default="bybit")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--max-notional-quote", type=float, default=10.0)
    parser.add_argument("--confirm-live-orders", action="store_true")
    parser.add_argument(
        "--dry-run-precheck-only",
        action="store_true",
        help="Run admission precheck and exchange truth only; never submit orders.",
    )
    parser.add_argument("--settle-timeout-s", type=float, default=12.0)
    parser.add_argument("--poll-interval-s", type=float, default=0.5)
    return parser.parse_args(argv)


def _live_orders_allowed(args: argparse.Namespace) -> bool:
    return bool(args.confirm_live_orders and not args.dry_run_precheck_only)


def _validate_live_order_args(args: argparse.Namespace) -> None:
    if str(args.venue).lower() != Venue.BYBIT.value:
        raise ValueError("active lifecycle probe only supports bybit")
    if float(args.max_notional_quote) <= 0:
        raise ValueError("max-notional-quote must be positive")
    if float(args.max_notional_quote) > MAX_ACTIVE_PROBE_NOTIONAL_QUOTE:
        raise ValueError(
            f"max-notional-quote must be <= {MAX_ACTIVE_PROBE_NOTIONAL_QUOTE:g}"
        )
    if not args.confirm_live_orders and not args.dry_run_precheck_only:
        raise ValueError(
            "--confirm-live-orders is required for active order submission; "
            "use --dry-run-precheck-only for non-mutating validation"
        )


def _now_ms() -> int:
    return int(time.time() * 1000)


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if hasattr(value, "value"):
        return value.value
    return value


def _client_id(kind: str) -> str:
    # Bybit orderLinkId limit is 36 chars; keep it compact and unique.
    return f"{PROBE_CLIENT_PREFIX}{kind}{int(time.time() * 1000) % 10_000_000}"


def _position_is_flat(position: PositionSnapshot | None) -> bool:
    return position is None or abs(float(getattr(position, "quantity", 0.0) or 0.0)) < 1e-12


def _side_to_close(position: PositionSnapshot) -> Side:
    return Side.SELL if position.side == Side.BUY else Side.BUY


def _submit_failure_class(exc: OrderSubmitError) -> SubmitFailureClass | None:
    return getattr(exc, "class_", getattr(exc, "failure_class", None))


def _reconciliation_payload(result: dict[str, Any]) -> dict[str, Any]:
    payload = dict(result)
    classification = payload.pop("classification", None)
    if classification is not None:
        payload["reconciliation_classification"] = classification
    return payload


async def _build_bybit_adapter(args: argparse.Namespace):
    try:
        config = load_config(args.config)
        for venue_config in getattr(config, "venues", ()) or ():
            try:
                venue = Venue.from_str(getattr(venue_config, "venue", ""))
            except ValueError:
                continue
            if venue != Venue.BYBIT:
                continue
            return build_adapter(
                Venue.BYBIT,
                venue_config,
                config.runtime.mode,
                exchange_http_timeout_ms=config.runtime.exchange_http_timeout_ms,
            )
    except Exception:
        pass

    api_key = os.environ.get("LIGHTFEE_BYBIT_API_KEY", "").strip("\r")
    api_secret = os.environ.get("LIGHTFEE_BYBIT_API_SECRET", "").strip("\r")
    if not (api_key and api_secret):
        raise RuntimeError(
            "Bybit credentials not found in config adapter map or "
            "LIGHTFEE_BYBIT_API_KEY/LIGHTFEE_BYBIT_API_SECRET"
        )
    return BybitAdapter(
        mode="live",
        credential=LiveCredential(api_key=api_key, api_secret=api_secret),
    )


async def _shutdown_adapter(adapter: Any) -> None:
    shutdown = getattr(adapter, "shutdown", None)
    if callable(shutdown):
        await shutdown()
        return
    transport = getattr(adapter, "_transport", None)
    close = getattr(transport, "close", None)
    if callable(close):
        await close()


async def _precheck_order_admission(adapter: Any, request: OrderRequest) -> dict[str, Any]:
    precheck = getattr(adapter, "precheck_order_admission", None)
    if callable(precheck):
        return _jsonable(await precheck(request))
    transport = getattr(adapter, "_transport", None)
    precheck = getattr(transport, "precheck_order_admission", None)
    if callable(precheck):
        return _jsonable(await precheck(request))
    return {"status": "skipped", "reason": "precheck_not_available"}


async def _market_quote(adapter: Any, symbol: str) -> dict[str, float]:
    snapshot = await adapter.fetch_market_snapshot([symbol])
    for quote in getattr(snapshot, "quotes", ()) or ():
        if str(getattr(quote, "symbol", "")).upper() == symbol.upper():
            bid = float(getattr(quote, "bid", 0.0) or 0.0)
            ask = float(getattr(quote, "ask", 0.0) or 0.0)
            if bid > 0 and ask > 0:
                return {
                    "bid": bid,
                    "ask": ask,
                    "mid": (bid + ask) / 2.0,
                    "observed_at_ms": float(getattr(snapshot, "observed_at_ms", 0) or 0),
                    "source": "adapter_market_snapshot",
                }
    return await _ws_bbo_quote(Venue.BYBIT.value, symbol, duration_s=3.0)


async def _ws_bbo_quote(venue: str, symbol: str, duration_s: float) -> dict[str, float]:
    from lightfee.marketdata.ws_bbo import VenueBboCache, VenueBboDataPlane

    cache = VenueBboCache()
    data_plane = VenueBboDataPlane(cache=cache)
    started = data_plane.start_ws_streams(venue, [symbol])
    if started <= 0:
        raise RuntimeError(f"WS BBO stream unsupported for {venue}:{symbol}")
    deadline = time.monotonic() + max(duration_s, 0.5)
    try:
        await data_plane.connect_ws_streams()
        quote = None
        while time.monotonic() < deadline:
            quote = cache.get_quote(venue, symbol)
            if quote is not None:
                break
            await asyncio.sleep(0.05)
        if quote is None:
            raise RuntimeError(f"no usable WS BBO quote for {venue}:{symbol}")
        bid = float(quote.bid or 0.0)
        ask = float(quote.ask or 0.0)
        if bid <= 0 or ask <= 0:
            raise RuntimeError(f"invalid WS BBO quote for {venue}:{symbol}")
        return {
            "bid": bid,
            "ask": ask,
            "mid": (bid + ask) / 2.0,
            "observed_at_ms": float(quote.observed_at_ms or 0),
            "received_at_ms": float(quote.received_at_ms or 0),
            "source": "ws_bbo_quote_lease",
        }
    finally:
        await data_plane.stop_ws_streams(per_client_timeout_s=1.0)


async def _normalize_probe_quantity(adapter: Any, symbol: str, notional: float, price: float) -> float:
    raw_qty = float(notional) / float(price)
    normalize = getattr(adapter, "normalize_quantity", None)
    if callable(normalize):
        qty = float(await normalize(symbol, raw_qty))
    else:
        qty = raw_qty
    if qty <= 0:
        raise RuntimeError(f"normalized quantity is zero for {symbol}")
    return qty


async def _fetch_current_truth(adapter: Any, symbol: str) -> dict[str, Any]:
    position = await adapter.fetch_position(symbol)
    all_positions = None
    fetch_all = getattr(adapter, "fetch_all_positions", None)
    if callable(fetch_all):
        try:
            all_positions = await fetch_all()
        except Exception as exc:
            all_positions = [{"classification": "fetch_all_positions_failed", "error": str(exc)[:300]}]
    return {
        "position": _jsonable(position),
        "flat": _position_is_flat(position),
        "all_positions": _jsonable(all_positions),
        "observed_at_ms": _now_ms(),
    }


async def _reconcile_order(
    adapter: Any,
    *,
    symbol: str,
    order_id: str,
    client_order_id: str,
) -> dict[str, Any]:
    reconcile = getattr(adapter, "fetch_order_fill_reconciliation", None)
    if not callable(reconcile):
        return {"classification": "reconciliation_unavailable"}
    result = await reconcile(symbol, order_id, client_order_id)
    return {
        "classification": "filled" if result and float(result.quantity or 0.0) > 0 else "no_fill_truth",
        "reconciliation": _jsonable(result),
    }


async def _poll_passive_close(
    adapter: Any,
    *,
    symbol: str,
    side: Side,
    order_id: str,
    client_order_id: str,
    timeout_s: float,
    poll_interval_s: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + max(timeout_s, 0.0)
    progress_payload: dict[str, Any] = {"classification": "not_polled"}
    query = getattr(adapter, "query_passive_order_progress", None)
    while callable(query) and time.monotonic() <= deadline:
        progress = await query(
            symbol=symbol,
            order_id=order_id,
            client_order_id=client_order_id,
            side=side,
        )
        if progress is not None:
            progress_payload = {
                "classification": "progress_seen",
                "progress": _jsonable(progress),
            }
            state = getattr(progress, "state", PassiveOrderState.UNKNOWN)
            if state == PassiveOrderState.FILLED or float(progress.cumulative_quantity or 0.0) > 0:
                return progress_payload
        await asyncio.sleep(max(poll_interval_s, 0.05))
    return progress_payload


async def _cancel_probe_order(adapter: Any, symbol: str, order_id: str, client_order_id: str) -> dict[str, Any]:
    cancel = getattr(adapter, "cancel_passive_order", None)
    if not callable(cancel):
        return {"classification": "cancel_unavailable"}
    try:
        ack = await cancel(symbol, order_id, client_order_id)
        return {"classification": "cancel_ack", "ack": _jsonable(ack)}
    except Exception as exc:
        return {"classification": "cancel_failed", "error": str(exc)[:500]}


async def _submit_reduce_only_ioc(
    adapter: Any,
    *,
    symbol: str,
    side: Side,
    quantity: float,
    price_hint: float,
) -> dict[str, Any]:
    cid = _client_id("ro")
    request = OrderRequest(
        venue=Venue.BYBIT,
        symbol=symbol,
        side=side,
        quantity=quantity,
        reduce_only=True,
        client_order_id=cid,
        time_in_force=TimeInForce.IOC,
        price_hint=price_hint,
        observed_at_ms=_now_ms(),
    )
    try:
        fill = await adapter.place_order(request)
        return {"classification": "filled", "fill": _jsonable(fill)}
    except OrderSubmitError as exc:
        if _submit_failure_class(exc) != SubmitFailureClass.UNCERTAIN:
            raise
        reconciled = await _reconcile_order(
            adapter,
            symbol=symbol,
            order_id=getattr(exc, "order_id", "") or "",
            client_order_id=cid,
        )
        return {
            "classification": "uncertain_reconciled",
            **_reconciliation_payload(reconciled),
        }


async def run_probe(args: argparse.Namespace) -> dict[str, Any]:
    _validate_live_order_args(args)
    symbol = str(args.symbol).upper()
    adapter = await _build_bybit_adapter(args)
    orders_submitted = 0

    try:
        initial_truth = await _fetch_current_truth(adapter, symbol)
        if not initial_truth["flat"]:
            return {
                "ok": False,
                "mode": "blocked_existing_position",
                "venue": Venue.BYBIT.value,
                "symbol": symbol,
                "reason": "probe refuses to run while target symbol is non-flat",
                "initial_truth": initial_truth,
                "orders_submitted": 0,
            }

        quote = await _market_quote(adapter, symbol)
        quantity = await _normalize_probe_quantity(
            adapter, symbol, float(args.max_notional_quote), quote["mid"]
        )
        open_cid = _client_id("op")
        open_request = OrderRequest(
            venue=Venue.BYBIT,
            symbol=symbol,
            side=Side.BUY,
            quantity=quantity,
            reduce_only=False,
            client_order_id=open_cid,
            time_in_force=TimeInForce.IOC,
            price_hint=quote["ask"] * 1.002,
            observed_at_ms=_now_ms(),
        )
        admission_precheck = await _precheck_order_admission(adapter, open_request)

        if not _live_orders_allowed(args):
            final_truth = await _fetch_current_truth(adapter, symbol)
            return {
                "ok": True,
                "mode": "dry_run_precheck_only",
                "venue": Venue.BYBIT.value,
                "symbol": symbol,
                "quote": quote,
                "quantity": quantity,
                "admission_precheck": admission_precheck,
                "initial_truth": initial_truth,
                "final_truth": final_truth,
                "orders_submitted": 0,
            }

        open_order: dict[str, Any]
        try:
            fill = await adapter.place_order(open_request)
            orders_submitted += 1
            open_order = {"classification": "filled", "fill": _jsonable(fill)}
        except OrderSubmitError as exc:
            orders_submitted += 1
            if _submit_failure_class(exc) != SubmitFailureClass.UNCERTAIN:
                raise
            open_order = {
                "classification": "ack_only_reconciled",
                "error": str(exc)[:500],
                **_reconciliation_payload(
                    await _reconcile_order(
                        adapter,
                        symbol=symbol,
                        order_id=getattr(exc, "order_id", "") or "",
                        client_order_id=open_cid,
                    )
                ),
            }

        post_open_truth = await _fetch_current_truth(adapter, symbol)
        if post_open_truth["flat"]:
            return {
                "ok": False,
                "mode": "active_live_orders",
                "venue": Venue.BYBIT.value,
                "symbol": symbol,
                "reason": "open order did not produce live position truth",
                "admission_precheck": admission_precheck,
                "open_order": open_order,
                "post_open_truth": post_open_truth,
                "orders_submitted": orders_submitted,
            }

        position = await adapter.fetch_position(symbol)
        close_side = _side_to_close(position)
        close_cid = _client_id("pc")
        close_price = quote["ask"] if close_side == Side.SELL else quote["bid"]
        close_request = OrderRequest(
            venue=Venue.BYBIT,
            symbol=symbol,
            side=close_side,
            quantity=abs(float(position.quantity)),
            price=close_price,
            reduce_only=True,
            client_order_id=close_cid,
            post_only=True,
            time_in_force=TimeInForce.POST_ONLY,
            price_hint=close_price,
            observed_at_ms=_now_ms(),
        )
        passive_ack = await adapter.submit_passive_order(close_request)
        orders_submitted += 1
        progress = await _poll_passive_close(
            adapter,
            symbol=symbol,
            side=close_side,
            order_id=passive_ack.order_id,
            client_order_id=passive_ack.client_order_id or close_cid,
            timeout_s=float(args.settle_timeout_s),
            poll_interval_s=float(args.poll_interval_s),
        )
        reconciliation = await _reconcile_order(
            adapter,
            symbol=symbol,
            order_id=passive_ack.order_id,
            client_order_id=passive_ack.client_order_id or close_cid,
        )

        close_truth = await _fetch_current_truth(adapter, symbol)
        fallback: dict[str, Any] | None = None
        cancel_result: dict[str, Any] | None = None
        if not close_truth["flat"]:
            cancel_result = await _cancel_probe_order(
                adapter, symbol, passive_ack.order_id, passive_ack.client_order_id or close_cid
            )
            current = await adapter.fetch_position(symbol)
            if not _position_is_flat(current):
                fallback = await _submit_reduce_only_ioc(
                    adapter,
                    symbol=symbol,
                    side=_side_to_close(current),
                    quantity=abs(float(current.quantity)),
                    price_hint=quote["bid"] if _side_to_close(current) == Side.SELL else quote["ask"],
                )
                orders_submitted += 1
            close_truth = await _fetch_current_truth(adapter, symbol)

        passive_classification = (
            "ack_only_resolved"
            if close_truth["flat"]
            else "ack_only_unresolved"
        )
        return {
            "ok": bool(close_truth["flat"]),
            "mode": "active_live_orders",
            "venue": Venue.BYBIT.value,
            "symbol": symbol,
            "quote": quote,
            "quantity": quantity,
            "admission_precheck": admission_precheck,
            "initial_truth": initial_truth,
            "open_order": open_order,
            "post_open_truth": post_open_truth,
            "passive_close": {
                "classification": passive_classification,
                "ack": _jsonable(passive_ack),
                "progress": progress,
                "reconciliation": reconciliation,
                "cancel": cancel_result,
                "fallback_reduce_only_ioc": fallback,
            },
            "final_truth": close_truth,
            "orders_submitted": orders_submitted,
        }
    finally:
        await _shutdown_adapter(adapter)


def main() -> int:
    args = parse_args()
    try:
        result = asyncio.run(run_probe(args))
    except Exception as exc:
        result = {
            "ok": False,
            "classification": "probe_exception",
            "error": str(exc)[:1000],
            "orders_submitted": 0,
        }
    print(json.dumps(_jsonable(result), ensure_ascii=False, sort_keys=True))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
