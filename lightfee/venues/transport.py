"""Shared venue transport: HTTP client, auth signing, error classification.

VenueTransport inherits public-data methods from MarketDataClient and adds
private trading methods (order, position, account risk, signed requests).
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import math
import datetime
import random
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, replace
from decimal import Decimal
from enum import Enum
from typing import Any, Optional

import httpx

from lightfee.core.domain import (
    OrderFill,
    OrderFillReconciliation,
    OrderRequest,
    PositionSnapshot,
    Side,
    Venue,
    VenueMarketQuote,
    VenueMarketSnapshot,
)
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.venues.common import normalize_venue_quantity
from lightfee.venues.market_data import MarketDataClient
from lightfee.venues.specs import AuthScheme, VenueSpec
from lightfee.venues.symbol_rules import get_symbol_rules_cache


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LiveCredential:
    api_key: str = ""
    api_secret: str = ""
    api_passphrase: str = ""
    wallet_private_key: str = ""
    account_address: str = ""


def _missing_hyperliquid_signing_dependencies() -> list[str]:
    import importlib.util

    required = [
        ("Crypto.Hash", "pycryptodome"),
        ("eth_account", "eth-account"),
        ("msgpack", "msgpack"),
    ]
    missing: list[str] = []
    for module_name, package_name in required:
        if importlib.util.find_spec(module_name) is None:
            missing.append(package_name)
    return missing


def _floor_to_step(value: float, step: float) -> float:
    if step <= 0 or not math.isfinite(value):
        return value
    return math.floor((value / step) + 1e-12) * step


def _ceil_to_step(value: float, step: float) -> float:
    if step <= 0 or not math.isfinite(value):
        return value
    return math.ceil((value / step) - 1e-12) * step


def _format_decimal(value: float) -> str:
    return format(Decimal(str(round(float(value), 12))).normalize(), "f")


def _step_decimals(step: float) -> int:
    if step <= 0 or not math.isfinite(step):
        return 12
    text = f"{step:.12f}".rstrip("0").rstrip(".")
    return len(text.split(".", 1)[1]) if "." in text else 0


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------


class _BybitOrderNotFound(Exception):
    """Internal sentinel: Bybit retCode=110001, order does not exist."""


class TransportErrorCategory(Enum):
    TRANSPORT_FAILURE = "transport_failure"
    AUTH_FAILURE = "auth_failure"
    AUTHORIZATION_FAILURE = "authorization_failure"
    UNSUPPORTED_CAPABILITY = "unsupported_capability"
    REQUEST_REJECTED = "request_rejected"
    ORDER_STATE_UNCERTAIN = "order_state_uncertain"
    NORMALIZATION_FAILURE = "normalization_failure"


class TransportError(Exception):
    def __init__(self, category: TransportErrorCategory, message: str,
                 status_code: int = 0, body: str = "",
                 headers: dict[str, str] | None = None) -> None:
        super().__init__(message)
        self.category = category
        self.status_code = status_code
        self.body = body
        self.headers: dict[str, str] = headers or {}


def classify_transport_error(
    status_code: int, body: str
) -> Optional[TransportErrorCategory]:
    if 200 <= status_code < 300:
        return None
    if status_code == 401:
        return TransportErrorCategory.AUTH_FAILURE
    if status_code == 403:
        return TransportErrorCategory.AUTHORIZATION_FAILURE
    if status_code == 400:
        return TransportErrorCategory.REQUEST_REJECTED
    if status_code in (429, 502, 503, 504):
        return TransportErrorCategory.TRANSPORT_FAILURE
    if status_code >= 500:
        return TransportErrorCategory.TRANSPORT_FAILURE
    return TransportErrorCategory.TRANSPORT_FAILURE


def _safe_float(value: Any, *, default: float = 0.0) -> float:
    """Safe numeric conversion: never raise on empty strings, None, or bad input.

    V2 improvement: replaces direct float(exchange_value) calls that crash on
    empty strings or list-shaped data.
    """
    if value is None:
        return default
    if isinstance(value, str) and value.strip() == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _require_bybit_success(raw: dict[str, Any], context: str) -> None:
    """Raise REJECTED if Bybit retCode is non-zero."""
    if int(raw.get("retCode", 0) or 0) != 0:
        raise OrderSubmitError(
            SubmitFailureClass.REJECTED,
            f"{context}: bybit retCode={raw.get('retCode')} retMsg={raw.get('retMsg', '')}",
        )


def _require_bitget_success(raw: dict[str, Any], context: str) -> None:
    """Raise REJECTED if Bitget code is not success."""
    code = str(raw.get("code", "00000"))
    if code not in ("00000", "0"):
        raise OrderSubmitError(
            SubmitFailureClass.REJECTED,
            f"{context}: bitget code={code} msg={raw.get('msg', '')}",
        )


# ---------------------------------------------------------------------------
# Bybit V5 body builders (Task 3)
# ---------------------------------------------------------------------------


def _format_quantity(qty: float) -> str:
    """Format a quantity for exchange order bodies (no scientific notation)."""
    if qty == 0.0:
        return "0"
    return f"{qty:f}".rstrip("0").rstrip(".")


def _format_price(price: float) -> str:
    """Format a price for exchange order bodies."""
    if price == 0.0:
        return "0"
    return f"{price:.2f}"


def _bybit_side(side: Side) -> str:
    return "Buy" if side == Side.BUY else "Sell"


def _bybit_position_idx(request: "OrderRequest", *, hedge_mode: bool) -> int:
    if not hedge_mode:
        return 0
    return 1 if request.side == Side.BUY else 2


def _build_bybit_order_body(
    request: "OrderRequest",
    venue_sym: str,
    *,
    passive: bool,
    hedge_mode: bool,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "category": "linear",
        "symbol": venue_sym,
        "side": _bybit_side(request.side),
        "orderType": "Limit" if (passive or request.price is not None) else "Market",
        "qty": _format_quantity(request.quantity),
        "reduceOnly": bool(request.reduce_only),
        "positionIdx": _bybit_position_idx(request, hedge_mode=hedge_mode),
    }
    if request.client_order_id:
        body["orderLinkId"] = request.client_order_id
    if request.price is not None and request.price > 0:
        body["price"] = _format_price(request.price)
    if passive:
        body["timeInForce"] = "PostOnly"
    return body


# ---------------------------------------------------------------------------
# Bitget profile-aware order builder (Task 4)
# ---------------------------------------------------------------------------


def _build_bitget_order_request(
    request: "OrderRequest",
    venue_sym: str,
    *,
    passive: bool,
    profile: str,
    hedge_mode: bool,
) -> tuple[str, dict[str, Any]]:
    """Build Bitget order path and body based on account profile (Classic vs UTA).

    Classic: /api/v2/mix/order/place-order with productType/marginMode/marginCoin
    UTA:     /api/v3/trade/place-order with category/qty/side/timeInForce/clientOid

    Returns (request_path, body_dict).
    """
    side = "buy" if request.side == Side.BUY else "sell"
    if profile == "uta":
        body: dict[str, Any] = {
            "category": "USDT-FUTURES",
            "symbol": venue_sym,
            "qty": _format_quantity(request.quantity),
            "side": side,
            "orderType": "limit" if (passive or request.price is not None) else "market",
            "clientOid": request.client_order_id or "",
        }
        if passive or request.price is not None:
            body["timeInForce"] = "post_only" if passive else "ioc"
            body["price"] = _format_price(request.price or 0.0)
        if hedge_mode:
            body["posSide"] = "long" if request.side == Side.BUY else "short"
        else:
            body["reduceOnly"] = "yes" if request.reduce_only else "no"
        return "/api/v3/trade/place-order", body

    # Classic
    body = {
        "symbol": venue_sym,
        "productType": "USDT-FUTURES",
        "marginMode": "crossed",
        "marginCoin": "USDT",
        "size": _format_quantity(request.quantity),
        "side": side,
        "orderType": "limit" if (passive or request.price is not None) else "market",
        "force": "post_only" if passive else "ioc",
        "clientOid": request.client_order_id or "",
    }
    if passive or request.price is not None:
        body["price"] = _format_price(request.price or 0.0)
    if hedge_mode:
        body["tradeSide"] = "open" if not request.reduce_only else "close"
    else:
        body["reduceOnly"] = "YES" if request.reduce_only else "NO"
    return "/api/v2/mix/order/place-order", body


def _parse_retry_after_ms(headers: dict[str, str]) -> Optional[int]:
    """Extract Retry-After value from response headers, returned as milliseconds.

    V1: parse_retry_after_ms() — supports both delta-seconds and HTTP-date.
    """
    retry_after = headers.get("Retry-After", headers.get("retry-after", ""))
    if not retry_after:
        return None
    try:
        # delta-seconds (e.g. "120")
        return int(retry_after) * 1000
    except ValueError:
        pass
    try:
        # HTTP-date (e.g. "Wed, 21 Oct 2015 07:28:00 GMT")
        from email.utils import parsedate_to_datetime
        retry_dt = parsedate_to_datetime(retry_after)
        now_dt = datetime.datetime.now(datetime.timezone.utc)
        delta = (retry_dt - now_dt).total_seconds()
        return max(0, int(delta * 1000))
    except Exception:
        return None


def _parse_reset_header_ms(headers: dict[str, str], header_name: str, now_ms: int) -> int | None:
    """Parse a reset-timestamp header (e.g. X-Bapi-Limit-Reset-Timestamp) into ms from now."""
    value = headers.get(header_name, "")
    if not value:
        return None
    try:
        reset_ts = int(value)
        delay = reset_ts - now_ms
        return max(0, delay)
    except (ValueError, TypeError):
        return None


def _parse_venue_retry_after_ms(
    venue: Venue, headers: dict[str, str], now_ms: int,
) -> int | None:
    """Parse venue-specific retry delay from response headers.

    V1: checks Retry-After first, then venue-specific reset headers.
    """
    # Common: Retry-After
    retry = _parse_retry_after_ms(headers)
    if retry is not None:
        return retry

    # Bybit: X-Bapi-Limit-Reset-Timestamp
    if venue == Venue.BYBIT:
        reset = _parse_reset_header_ms(headers, "X-Bapi-Limit-Reset-Timestamp", now_ms)
        if reset is not None:
            return reset

    # Gate: X-RateLimit-Reset or X-Gate-RateLimit-Reset
    if venue == Venue.GATE:
        for hdr in ("X-RateLimit-Reset", "X-Gate-RateLimit-Reset"):
            reset = _parse_reset_header_ms(headers, hdr, now_ms)
            if reset is not None:
                return reset

    return None


def _normalize_rest_endpoint_key(method: str, path: str) -> str:
    """Normalize a REST endpoint to V1 key format: 'METHOD /path'."""
    clean_path = path.split("?", 1)[0]
    return f"{method.upper()} {clean_path}".strip()


def _normalize_host_scope(base_url: str) -> str:
    """Extract hostname from base URL for 'host:<hostname>' scope."""
    from urllib.parse import urlparse
    parsed = urlparse(base_url.rstrip("/"))
    host = parsed.netloc or parsed.path.split("/")[0]
    return f"host:{host}"


def _map_to_submit_error(
    category: TransportErrorCategory, message: str
) -> OrderSubmitError:
    if category in (
        TransportErrorCategory.AUTH_FAILURE,
        TransportErrorCategory.AUTHORIZATION_FAILURE,
        TransportErrorCategory.REQUEST_REJECTED,
        TransportErrorCategory.UNSUPPORTED_CAPABILITY,
        TransportErrorCategory.NORMALIZATION_FAILURE,
    ):
        return OrderSubmitError(SubmitFailureClass.REJECTED, message)
    return OrderSubmitError(SubmitFailureClass.UNCERTAIN, message)


# ---------------------------------------------------------------------------
# Signing helpers
# ---------------------------------------------------------------------------


def build_hmac_sha256_hex(secret: str, payload: str) -> str:
    mac = hmac.new(secret.encode(), payload.encode(), hashlib.sha256)
    return mac.hexdigest()


def build_hmac_sha256_base64(secret: str, payload: str) -> str:
    mac = hmac.new(secret.encode(), payload.encode(), hashlib.sha256)
    return base64.b64encode(mac.digest()).decode()


def _iso8601_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + (
        str(datetime.datetime.now(datetime.timezone.utc).microsecond // 1000).zfill(3)
    ) + "Z"


def _iso8601_from_ms(timestamp_ms: int) -> str:
    """Convert epoch milliseconds to ISO-8601 string (OKX format)."""
    dt = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0, tz=datetime.timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def build_hmac_sha512_hex(secret: str, payload: str) -> str:
    mac = hmac.new(secret.encode(), payload.encode(), hashlib.sha512)
    return mac.hexdigest()


def _sign_payload(scheme: AuthScheme, secret: str, payload: str) -> str:
    if scheme == AuthScheme.HMAC_SHA256_HEX:
        return build_hmac_sha256_hex(secret, payload)
    if scheme == AuthScheme.HMAC_SHA256_BASE64:
        return build_hmac_sha256_base64(secret, payload)
    if scheme == AuthScheme.HMAC_SHA512_HEX:
        return build_hmac_sha512_hex(secret, payload)
    raise ValueError(f"unsupported auth scheme: {scheme}")


def _build_query_v1(params: list[tuple[str, str]]) -> str:
    """Build URL-encoded query string, preserving caller order (V1: form_urlencoded::Serializer)."""
    from urllib.parse import urlencode
    return urlencode(params)


# ---------------------------------------------------------------------------
# Rate limiter — V1-aligned: scoped cooldowns + host pacing
# ---------------------------------------------------------------------------


class EndpointRateLimiter:
    """V1-aligned rate limiter with per-scope cooldown + pacing.

    V1 reference: resilience.rs EndpointRateLimiter
    - ``initial_ms`` / ``max_ms``: exponential backoff for rate-limit cooldowns
    - ``pacing_interval_ms``: minimum interval between requests (0 = no pacing)
    - ``wait_until_ready_for_scopes``: block until all scopes are out of cooldown
    - ``pace_for_scopes``: enforce minimum pacing interval
    - ``record_rate_limit_for_scopes``: record a rate-limit event (429/418)
    - ``record_success_for_scopes``: record success (no-op, matching V1)
    """

    def __init__(self, initial_ms: int, max_ms: int, pacing_interval_ms: int = 0) -> None:
        self._initial_ms = max(initial_ms, 1)
        self._max_ms = max(max_ms, self._initial_ms)
        self._pacing_interval_ms = pacing_interval_ms
        # Per-scope cooldown state
        self._cooldowns: dict[str, tuple[int, int]] = {}  # scope -> (next_allowed_at_ms, failures)
        # Per-scope pacing state
        self._last_request_ms: dict[str, int] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Cooldown helpers
    # ------------------------------------------------------------------

    def _failure_backoff_ms(self, failures: int) -> int:
        """Exponential backoff: initial * 2^failures, capped at max_ms.

        V1: failure_backoff_delay_ms — left-shift by min(failures, 20), clamp to max.
        """
        shift = min(failures, 20)
        return min(self._initial_ms << shift, self._max_ms)

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def wait_until_ready_for_scopes(self, scopes: list[str]) -> None:
        """Block until all scopes are out of cooldown."""
        while True:
            delay_ms = self._cooldown_remaining_ms_for_scopes(scopes)
            if delay_ms is None:
                return
            await asyncio.sleep(delay_ms / 1000.0)

    async def pace_for_scopes(self, scopes: list[str]) -> None:
        """Enforce minimum pacing interval for each scope."""
        if self._pacing_interval_ms <= 0:
            return
        now_ms = self._now_ms()
        delay_ms = 0
        async with self._lock:
            for scope in scopes:
                last = self._last_request_ms.get(scope, 0)
                remaining = last + self._pacing_interval_ms - now_ms
                if remaining > delay_ms:
                    delay_ms = remaining
            if delay_ms > 0:
                # Mark all scopes as being used at now_ms + delay_ms
                next_at = now_ms + delay_ms
                for scope in scopes:
                    self._last_request_ms[scope] = next_at
        if delay_ms > 0:
            await asyncio.sleep(delay_ms / 1000.0)

    def record_rate_limit_for_scopes(self, scopes: list[str], retry_after_ms: Optional[int] = None) -> int:
        """Record a rate-limit event. Returns the cooldown delay in ms."""
        now_ms = self._now_ms()
        retry_after = retry_after_ms or 0
        max_delay = retry_after
        for scope in scopes:
            if not scope:
                continue
            cooldown = self._cooldowns.get(scope)
            if cooldown is None:
                failures = 0
                next_at = 0
            else:
                next_at, failures = cooldown
            backoff = self._failure_backoff_ms(failures)
            delay_ms = max(backoff, retry_after)
            new_next_at = max(next_at, now_ms + delay_ms)
            self._cooldowns[scope] = (new_next_at, failures + 1)
            if delay_ms > max_delay:
                max_delay = delay_ms
        return max_delay

    def record_success_for_scopes(self, scopes: list[str]) -> None:
        """Record a successful request. No-op in V1, matching here."""
        pass

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _cooldown_remaining_ms_for_scopes(self, scopes: list[str]) -> Optional[int]:
        """Return the max remaining cooldown across all scopes, or None if all clear."""
        now_ms = self._now_ms()
        max_remaining = 0
        for scope in scopes:
            cooldown = self._cooldowns.get(scope)
            if cooldown is not None:
                remaining = cooldown[0] - now_ms
                if remaining > max_remaining:
                    max_remaining = remaining
        return max_remaining if max_remaining > 0 else None


# Global rate limiter instance, created once and shared across transports.
# V1 uses Arc<EndpointRateLimiter> with pacing (1000ms initial, 8000ms max, 25ms interval).
# For V2 we create it lazily — the first caller initialises it.
_global_rate_limiter: Optional[EndpointRateLimiter] = None
_rate_limiter_lock = asyncio.Lock()


async def _get_global_rate_limiter(pacing_ms: int = 25) -> EndpointRateLimiter:
    """Return the global EndpointRateLimiter, creating it on first call."""
    global _global_rate_limiter
    if _global_rate_limiter is not None:
        return _global_rate_limiter
    async with _rate_limiter_lock:
        if _global_rate_limiter is not None:
            return _global_rate_limiter
        # V1 defaults: 1000ms initial, 8000ms max, pacing_interval=25ms
        _global_rate_limiter = EndpointRateLimiter(1000, 8000, pacing_ms)
        return _global_rate_limiter


# ---------------------------------------------------------------------------
# Venue-specific position parsers (Task 7)
# ---------------------------------------------------------------------------


def _parse_bybit_position(raw: dict[str, Any], symbol: str, now_ms: int) -> "PositionSnapshot":
    """Parse Bybit V5 position list response.

    Envelope: {"retCode":0, "result":{"list":[{...}]}}
    Uses safe_float for all exchange-returned fields.
    """
    _require_bybit_success(raw, "bybit position failed")
    rows = ((raw.get("result") or {}).get("list") or [])
    net = 0.0
    entry_price = 0.0
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("symbol") and row.get("symbol") != symbol:
            continue
        qty = abs(_safe_float(row.get("size"), default=0.0))
        side = str(row.get("side", ""))
        net += qty if side == "Buy" else -qty if side == "Sell" else 0.0
        entry_price = _safe_float(row.get("avgPrice") or row.get("entryPrice"), default=entry_price)
    return PositionSnapshot(
        venue=Venue.BYBIT,
        symbol=symbol,
        side=Side.BUY if net >= 0 else Side.SELL,
        quantity=abs(net),
        entry_price=entry_price,
        observed_at_ms=now_ms,
    )


def _parse_bitget_position(raw: dict[str, Any], symbol: str, now_ms: int) -> "PositionSnapshot":
    """Parse Bitget position response for both Classic and UTA formats.

    Classic: {"code":"00000","data":[{"symbol":"BTCUSDT","total":"0.01","holdSide":"long",...}]}
    UTA:     {"code":"00000","data":[{"symbol":"BTCUSDT","total":"0.01","holdSide":"long",...}]}
    Both use data array; key fields: total/available, holdSide/posSide, openPriceAvg/avgPrice.
    """
    _require_bitget_success(raw, "bitget position failed")
    data = raw.get("data", [])
    rows = data if isinstance(data, list) else data.get("list", []) if isinstance(data, dict) else []
    net = 0.0
    entry_price = 0.0
    for row in rows:
        if not isinstance(row, dict):
            continue
        if _normalize_symbol(row.get("symbol", "")) != _normalize_symbol(symbol):
            continue
        qty = abs(_safe_float(row.get("total") or row.get("available") or row.get("holdVolume") or row.get("size")))
        hold_side = str(row.get("holdSide") or row.get("posSide") or "").lower()
        net += qty if hold_side in ("long", "buy") else -qty if hold_side in ("short", "sell") else qty
        entry_price = _safe_float(row.get("openPriceAvg") or row.get("avgPrice"), default=entry_price)
    return PositionSnapshot(
        venue=Venue.BITGET,
        symbol=symbol,
        side=Side.BUY if net >= 0 else Side.SELL,
        quantity=abs(net),
        entry_price=entry_price,
        observed_at_ms=now_ms,
    )


def _parse_okx_position(raw: dict[str, Any], symbol: str, now_ms: int, *, contract_size: float = 1.0) -> "PositionSnapshot":
    """Parse OKX position response with contract size scaling.

    OKX format: {"code":"0","data":[{"instId":"BTC-USDT-SWAP","pos":"1","posSide":"long","avgPx":"50000",...}]}
    """
    data = raw.get("data", [])
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = [data]
    else:
        rows = []
    net = 0.0
    entry_price = 0.0
    for row in rows:
        if not isinstance(row, dict):
            continue
        inst = row.get("instId", "")
        if inst and inst != symbol:
            continue
        qty = abs(_safe_float(row.get("pos"), default=0.0)) * contract_size
        pos_side = str(row.get("posSide", "")).lower()
        net += qty if pos_side == "long" else -qty if pos_side == "short" else qty
        entry_price = _safe_float(row.get("avgPx"), default=entry_price)
    return PositionSnapshot(
        venue=Venue.OKX,
        symbol=symbol,
        side=Side.BUY if net >= 0 else Side.SELL,
        quantity=abs(net),
        entry_price=entry_price,
        observed_at_ms=now_ms,
    )


def _parse_binance_like_position(raw: dict[str, Any], symbol: str, now_ms: int,
                                  *, venue: "Venue" = Venue.BINANCE) -> "PositionSnapshot":
    """Parse Binance/Aster position (flat response or list).

    Binance: [{"symbol":"BTCUSDT","positionSide":"LONG","positionAmt":"0.01","entryPrice":"50000"}]
    Aster:   [{"symbol":"BTCUSDT","positionSide":"LONG","positionAmt":"0.01","entryPrice":"50000"}]
    """
    rows = raw if isinstance(raw, list) else [raw]
    net = 0.0
    entry_price = 0.0
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("symbol") and row.get("symbol") != symbol:
            continue
        qty = abs(_safe_float(row.get("positionAmt"), default=0.0))
        pos_side = str(row.get("positionSide", "")).upper()
        if "SHORT" in pos_side:
            net -= qty
        elif "LONG" in pos_side:
            net += qty
        else:
            amt = _safe_float(row.get("positionAmt"), default=0.0)
            net += amt
        entry_price = _safe_float(row.get("entryPrice"), default=entry_price)
    return PositionSnapshot(
        venue=venue,
        symbol=symbol,
        side=Side.BUY if net >= 0 else Side.SELL,
        quantity=abs(net),
        entry_price=entry_price,
        observed_at_ms=now_ms,
    )


def _parse_gate_position(raw: dict[str, Any], symbol: str, now_ms: int) -> "PositionSnapshot":
    """Parse Gate.io position (flat dict or list).

    Gate: {"contract":"BTCUSDT","size":"-1","entry_price":"50000"}
    """
    rows = raw if isinstance(raw, list) else [raw]
    net = 0.0
    entry_price = 0.0
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("contract") and row.get("contract") != symbol:
            continue
        size = _safe_float(row.get("size"), default=0.0)
        net += size
        entry_price = _safe_float(row.get("entry_price"), default=entry_price)
    return PositionSnapshot(
        venue=Venue.GATE,
        symbol=symbol,
        side=Side.BUY if net >= 0 else Side.SELL,
        quantity=abs(net),
        entry_price=entry_price,
        observed_at_ms=now_ms,
    )


def _parse_hyperliquid_position(raw: dict[str, Any], symbol: str, now_ms: int) -> "PositionSnapshot":
    """Parse Hyperliquid clearinghouse state for position."""
    positions = raw.get("assetPositions", [])
    for p in positions:
        pos_data = p.get("position", {}) if isinstance(p, dict) else {}
        pos_sym = pos_data.get("coin", "")
        if pos_sym == symbol or not symbol:
            szi = _safe_float(pos_data.get("szi", 0))
            return PositionSnapshot(
                venue=Venue.HYPERLIQUID,
                symbol=pos_sym or symbol,
                side=Side.BUY if szi > 0 else Side.SELL,
                quantity=abs(szi),
                entry_price=_safe_float(pos_data.get("entryPx", 0)),
                observed_at_ms=now_ms,
            )
    return PositionSnapshot(
        venue=Venue.HYPERLIQUID,
        symbol=symbol,
        side=Side.BUY,
        quantity=0.0,
        entry_price=0.0,
        observed_at_ms=now_ms,
    )


def _parse_generic_position(
    raw: dict[str, Any], spec: "VenueSpec", symbol: str, now_ms: int
) -> "PositionSnapshot":
    """Generic position parser fallback with safe numeric handling.

    Used when the venue-specific parser returns zero (flat data / test fixtures).
    """
    data = raw
    if isinstance(raw, dict):
        if "data" in raw and isinstance(raw["data"], dict):
            data = raw["data"]
        elif "data" in raw and isinstance(raw["data"], list) and raw["data"]:
            data = raw["data"][0]
        elif "result" in raw and isinstance(raw["result"], dict) and raw["result"].get("list"):
            data = raw["result"]["list"][0]
        elif "result" in raw and isinstance(raw["result"], list) and raw["result"]:
            data = raw["result"][0]

    pos_side_str = str(data.get(
        "positionSide",
        data.get("posSide",
        data.get("holdSide",
        data.get("side", "")))
    )).upper()

    qty_raw = str(data.get(
        "positionAmt",
        data.get("pos",
        data.get("size",
        data.get("total",
        data.get("volume", "0"))))
    ))

    pos_side_upper = pos_side_str.upper()
    short_indicators = ("SHORT", "SELL", "SHORT_SIDE")
    long_indicators = ("LONG", "BUY")
    if any(ind in pos_side_upper for ind in short_indicators):
        side = Side.SELL
    elif any(ind in pos_side_upper for ind in long_indicators):
        side = Side.BUY
    elif qty_raw:
        qty_val = _safe_float(qty_raw, default=0.0)
        side = Side.SELL if qty_val < 0 else Side.BUY
    else:
        side = Side.BUY

    qty = abs(_safe_float(qty_raw)) if qty_raw else 0.0
    entry_price = _safe_float(data.get(
        "entryPrice",
        data.get("avgPrice",
        data.get("avgPx",
        data.get("openPriceAvg",
        data.get("entry_price", 0))))
    ))

    return PositionSnapshot(
        venue=spec.venue_id,
        symbol=symbol,
        side=side,
        quantity=qty,
        entry_price=entry_price,
        observed_at_ms=now_ms,
    )


def _venue_position_rows(raw: Any, venue: Venue) -> list[dict[str, Any]]:
    if venue in (Venue.BINANCE, Venue.ASTER, Venue.GATE):
        rows = raw if isinstance(raw, list) else raw.get("data", []) if isinstance(raw, dict) else []
    elif venue == Venue.BYBIT:
        rows = ((raw.get("result") or {}).get("list") or []) if isinstance(raw, dict) else []
    elif venue == Venue.BITGET:
        data = raw.get("data", []) if isinstance(raw, dict) else []
        rows = data if isinstance(data, list) else data.get("list", []) if isinstance(data, dict) else []
    elif venue == Venue.OKX:
        data = raw.get("data", []) if isinstance(raw, dict) else []
        rows = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
    elif venue == Venue.HYPERLIQUID:
        rows = raw.get("assetPositions", []) if isinstance(raw, dict) else []
    else:
        rows = []
    return [row for row in rows if isinstance(row, dict)]


def _position_row_symbol(row: dict[str, Any], venue: Venue) -> str:
    if venue == Venue.OKX:
        return str(row.get("instId", ""))
    if venue == Venue.GATE:
        return str(row.get("contract", ""))
    if venue == Venue.HYPERLIQUID:
        pos_data = row.get("position", {}) if isinstance(row, dict) else {}
        return str(pos_data.get("coin", ""))
    return str(row.get("symbol", ""))


def _canonical_position_symbol(spec: VenueSpec, venue_symbol: str) -> str:
    if spec.symbol_from_venue is not None:
        return spec.symbol_from_venue(venue_symbol)
    return venue_symbol


def _position_row_parse_payload(row: dict[str, Any], venue: Venue) -> Any:
    if venue == Venue.BYBIT:
        return {"retCode": 0, "result": {"list": [row]}}
    if venue == Venue.BITGET:
        return {"code": "00000", "data": [row]}
    if venue == Venue.OKX:
        return {"data": [row]}
    if venue == Venue.HYPERLIQUID:
        return {"assetPositions": [row]}
    return [row]


def _normalize_symbol(sym: str) -> str:
    """Normalize a symbol string for comparison."""
    return str(sym or "").strip().upper()


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------


class VenueTransport(MarketDataClient):
    """Shared async transport: inherits public data from MarketDataClient, adds private trading."""

    def __init__(
        self,
        spec: VenueSpec,
        mode: str = "paper",
        credential: Optional[LiveCredential] = None,
        exchange_http_timeout_ms: int = 10000,
        rate_limiter: Optional[EndpointRateLimiter] = None,
    ) -> None:
        super().__init__(spec, exchange_http_timeout_ms=exchange_http_timeout_ms, rate_limiter=rate_limiter)
        self.mode = mode
        self._credential = credential
        self._hl_meta_cache: dict[str, int] = {}
        self._symbol_metadata: dict[str, dict[str, Any]] = {}  # sym → vendor contract info
        self._time_offset_ms: int | None = None  # V1: cached server-time offset
        self._order_diagnostics: list[dict[str, Any]] = []

        if mode == "live":
            self._validate_live_credentials(credential)

    # ------------------------------------------------------------------
    # Credential validation
    # ------------------------------------------------------------------

    def _validate_live_credentials(self, credential: Optional[LiveCredential]) -> None:
        if credential is None:
            raise ValueError(
                f"live mode requires credentials for {self._spec.venue_id.value}"
            )
        # Wallet-key venues (Hyperliquid) use private key + account address,
        # not api_key + api_secret.
        if self._spec.requires_wallet_key:
            if not credential.wallet_private_key:
                raise ValueError(
                    f"live mode requires wallet_private_key for {self._spec.venue_id.value}"
                )
            missing = _missing_hyperliquid_signing_dependencies()
            if missing:
                raise ValueError(
                    "live mode missing signing dependencies for "
                    f"{self._spec.venue_id.value}: {', '.join(missing)}"
                )
            return
        if not credential.api_key:
            raise ValueError(
                f"live mode requires api_key for {self._spec.venue_id.value}"
            )
        if not credential.api_secret:
            raise ValueError(
                f"live mode requires api_secret for {self._spec.venue_id.value}"
            )
        if self._spec.requires_passphrase and not credential.api_passphrase:
            raise ValueError(
                f"live mode requires passphrase for {self._spec.venue_id.value}"
            )

    @property
    def order_diagnostics(self) -> list[dict[str, Any]]:
        return list(self._order_diagnostics)

    def drain_order_diagnostics(self) -> list[dict[str, Any]]:
        events = list(self._order_diagnostics)
        self._order_diagnostics.clear()
        return events

    def _record_order_diagnostic(self, kind: str, payload: dict[str, Any]) -> None:
        blocked = ("secret", "signature", "api_key", "private_key", "header", "auth")
        clean = {
            str(k): v
            for k, v in payload.items()
            if not any(token in str(k).lower() for token in blocked)
        }
        self._order_diagnostics.append({"kind": kind, "payload": clean})

    def startup_preflight(self) -> dict[str, Any]:
        missing: list[str] = []
        if self._spec.requires_wallet_key:
            missing = _missing_hyperliquid_signing_dependencies()
        return {
            "venue": self._spec.venue_id.value,
            "status": "failed" if missing else "ok",
            "missing_dependencies": missing,
            "endpoint": self._spec.order_path,
            "product_type": self._product_type(),
        }

    def _product_type(self) -> str:
        if self._spec.venue_id in (Venue.BINANCE, Venue.ASTER):
            return "usdm_futures"
        if self._spec.venue_id == Venue.BYBIT:
            return "linear"
        if self._spec.venue_id == Venue.OKX:
            return "swap"
        if self._spec.venue_id == Venue.BITGET:
            return "mix"
        if self._spec.venue_id == Venue.GATE:
            return "futures"
        if self._spec.venue_id == Venue.HYPERLIQUID:
            return "perp"
        return ""

    def preflight_order_request(
        self, request: OrderRequest, symbol_rule: Any = None,
    ) -> dict[str, Any]:
        """Normalize qty/price and validate min notional for an order request.

        When symbol_rule (SymbolRule) is provided, uses those dynamic rules
        instead of static VenueSpec values.  This allows passive orders to
        pass through the same normalization path as taker orders.
        """
        spec = self._spec
        tick_size = (
            float(symbol_rule.tick_size)
            if symbol_rule is not None and symbol_rule.tick_size > 0
            else float(spec.price_tick or 0.0)
        )
        quantity_step = (
            float(symbol_rule.qty_step)
            if symbol_rule is not None and symbol_rule.qty_step > 0
            else float(spec.quantity_step or 0.0)
        )
        min_qty = (
            float(symbol_rule.min_qty)
            if symbol_rule is not None and symbol_rule.min_qty > 0
            else float(spec.min_quantity or 0.0)
        )
        min_notional = (
            float(symbol_rule.min_notional)
            if symbol_rule is not None and symbol_rule.min_notional > 0
            else float(spec.min_notional or 0.0)
        )
        rule_source = (
            symbol_rule.rule_source
            if symbol_rule is not None and symbol_rule.rule_source
            else "spec"
        )

        raw_qty = float(request.quantity)
        raw_price = float(request.price) if request.price is not None else None
        quantized_qty = _floor_to_step(raw_qty, quantity_step) if quantity_step > 0 else raw_qty
        quantized_qty = round(quantized_qty, _step_decimals(quantity_step))
        if spec.venue_id == Venue.GATE:
            quantized_qty = float(int(quantized_qty))
        quantized_price = raw_price
        if raw_price is not None and tick_size > 0:
            quantized_price = (
                _floor_to_step(raw_price, tick_size)
                if request.side == Side.BUY
                else _ceil_to_step(raw_price, tick_size)
            )
            quantized_price = round(float(quantized_price), _step_decimals(tick_size))
        payload = {
            "venue": spec.venue_id.value,
            "symbol": request.symbol,
            "endpoint": spec.order_path,
            "product_type": self._product_type(),
            "category": self._product_type(),
            "client_order_id": request.client_order_id or "",
            "order_id": request.order_id or "",
            "raw_price": raw_price,
            "raw_qty": raw_qty,
            "quantized_price": quantized_price,
            "quantized_qty": quantized_qty,
            "tick_size": tick_size,
            "quantity_step": quantity_step,
            "min_qty": min_qty,
            "min_notional": min_notional,
            "rule_source": rule_source,
        }
        if quantized_qty <= 0 or not math.isfinite(quantized_qty):
            payload["response_classification"] = "precision_rejected"
            payload["reason"] = "quantity_step_rejected"
            self._record_order_diagnostic("order.submit_result", payload)
            raise OrderSubmitError(SubmitFailureClass.REJECTED, "quantity_step_rejected")
        if quantized_qty < min_qty:
            payload["response_classification"] = "precision_rejected"
            payload["reason"] = "min_qty_rejected"
            self._record_order_diagnostic("order.submit_result", payload)
            raise OrderSubmitError(
                SubmitFailureClass.REJECTED,
                f"min_qty_rejected qty={quantized_qty} min_qty={min_qty}",
            )
        notional_price = quantized_price if quantized_price is not None else raw_price
        if notional_price is not None and not request.reduce_only:
            notional = abs(quantized_qty * float(notional_price))
            if notional < min_notional:
                payload["response_classification"] = "precision_rejected"
                payload["reason"] = "min_notional_rejected"
                self._record_order_diagnostic("order.submit_result", payload)
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    f"min_notional_rejected notional={notional} min_notional={min_notional}",
                )
        payload["response_classification"] = "attempt"
        return payload

    # ------------------------------------------------------------------
    # Server-time offset (V1 parity)
    # ------------------------------------------------------------------

    async def _server_timestamp_ms(self) -> int:
        """V1: fetch server time, cache offset. Raises on failure — NO fallback to local time.

        Binance/Aster: offset = server_time - now_ms - safety_margin
        OKX: offset = server_time - now_ms
        Bybit: offset = server_time - now_ms (auth timestamp applies separate backoff)
        """
        now_ms = int(time.time() * 1000)
        spec = self._spec

        if self._time_offset_ms is not None:
            return now_ms + self._time_offset_ms

        if not spec.server_time_path:
            return now_ms

        # V1: server-time fetch must go through rate limiter path (send_public_request)
        raw = await self._fetch_server_time_via_limiter("GET", spec.server_time_path)
        server_time = self._parse_server_time(raw)
        if server_time <= 0:
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                f"{spec.venue_id.value}: failed to decode server time from response",
            )
        offset = server_time - now_ms - spec.server_time_safety_margin_ms
        self._time_offset_ms = offset
        return now_ms + offset

    async def _fetch_server_time_via_limiter(self, method: str, path: str) -> dict[str, Any]:
        """V1: server-time request through limiter + pacing (send_public_request path).

        On 429/418: record cooldown/backoff on all scopes exactly as _request() /
        V1 send_*_request_with_limiter wrappers do, before raising TransportError.
        """
        base_url = self._spec.public_base_url
        scopes = self._rest_rate_limit_scopes(method, path, base_url, private=False)

        if self._rate_limiter is not None:
            await self._rate_limiter.wait_until_ready_for_scopes(scopes)
            await self._rate_limiter.pace_for_scopes(scopes)

        from lightfee.rate_limit.engine import global_rate_limit_runtime as _get_global_rt
        global_rt = _get_global_rt()
        if global_rt is not None:
            await global_rt.async_wait_until_ready_for_scopes(scopes)

        client = await self._get_client()
        url = base_url + path
        resp = await client.get(url)

        if resp.status_code >= 400:
            # V1: record rate-limit for 429/418 on ALL scopes before raising
            if resp.status_code in (429, 418):
                now_ms = int(time.time() * 1000)
                retry_after_ms = _parse_venue_retry_after_ms(
                    self._spec.venue_id, dict(resp.headers), now_ms,
                )
                if self._rate_limiter is not None:
                    self._rate_limiter.record_rate_limit_for_scopes(
                        scopes, retry_after_ms=retry_after_ms,
                    )
                if global_rt is not None:
                    global_rt.record_rate_limit_for_scopes(
                        scopes, retry_after_ms=retry_after_ms or 0,
                    )
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                f"server-time {method} {path} returned {resp.status_code}",
                status_code=resp.status_code, body=resp.text,
                headers=dict(resp.headers),
            )
        # V1: record success for all scopes
        if self._rate_limiter is not None:
            self._rate_limiter.record_success_for_scopes(scopes)
        if not resp.text:
            return {}
        return resp.json()

    def _parse_server_time(self, raw: dict[str, Any]) -> int:
        """Parse server time from venue-specific response."""
        spec = self._spec
        vid = spec.venue_id

        # Binance/Aster: {"serverTime": 1234567890000}
        if vid in (Venue.BINANCE, Venue.ASTER):
            return int(raw.get("serverTime", 0))

        # OKX: {"code":"0","data":[{"ts":"1234567890000"}]}
        if vid == Venue.OKX:
            data = raw.get("data", [])
            if isinstance(data, list) and data:
                ts = data[0].get("ts", "0")
                return int(ts)
            return 0

        # Bybit: {"retCode":0,"result":{"timeSecond":"1234567890","timeNano":"..."}}
        if vid == Venue.BYBIT:
            result = raw.get("result", {})
            if isinstance(result, dict):
                time_second = result.get("timeSecond")
                if time_second:
                    return int(time_second) * 1000
                time_nano = result.get("timeNano")
                if time_nano:
                    return int(time_nano) // 1_000_000
            return 0

        return 0

    def _clear_server_time_offset(self) -> None:
        """Clear cached server-time offset (V1: on timestamp/signature error)."""
        self._time_offset_ms = None

    # ------------------------------------------------------------------
    # Auth header construction
    # ------------------------------------------------------------------

    async def _build_auth_headers_async(
        self,
        method: str,
        path: str,
        query_string: str = "",
        body: str = "",
        private: bool = False,
    ) -> dict[str, str]:
        spec = self._spec
        cred = self._credential

        # Public endpoints must not include auth headers (V1 parity)
        if not private or cred is None:
            return {}

        headers: dict[str, str] = {}

        # --- EIP712 (Hyperliquid) ---
        if spec.auth_scheme == AuthScheme.EIP712:
            headers["Content-Type"] = "application/json"
            return headers

        # --- Binance / Aster: query-string signature with server time ---
        if spec.signature_param:
            ts = str(await self._server_timestamp_ms())
            payload = query_string.lstrip("?") if query_string else ""
            if spec.timestamp_param and spec.timestamp_param not in (payload or ""):
                ts_param = f"{spec.timestamp_param}={ts}"
                payload = f"{ts_param}&{payload}" if payload else ts_param
            headers[spec.api_key_header] = cred.api_key
            sig = _sign_payload(spec.auth_scheme, cred.api_secret, payload)
            return headers

        # --- Gate: HMAC-SHA512 with body hash, seconds timestamp, newline payload ---
        if spec.auth_scheme == AuthScheme.HMAC_SHA512_HEX:
            import hashlib as _hashlib
            ts = str(int(time.time()))
            body_hash = _hashlib.sha512((body or "").encode()).hexdigest()
            sign_payload = f"{method.upper()}\n{path}\n{query_string.lstrip('?')}\n{body_hash}\n{ts}"
            sig = _sign_payload(spec.auth_scheme, cred.api_secret, sign_payload)
            headers["KEY"] = cred.api_key
            headers["Timestamp"] = ts
            headers["SIGN"] = sig
            headers["Content-Type"] = "application/json"
            headers["X-Gate-Size-Decimal"] = "1"
            return headers

        # --- Bitget: specific ACCESS-* headers + signing (not OKX) ---
        if spec.venue_id == Venue.BITGET:
            ts = str(int(time.time() * 1000))
            request_path = f"{path}?{query_string.lstrip('?')}" if query_string else path
            sign_payload = ts + method.upper() + request_path + (body or "")
            sig = _sign_payload(spec.auth_scheme, cred.api_secret, sign_payload)
            headers["ACCESS-KEY"] = cred.api_key
            headers["ACCESS-SIGN"] = sig
            headers["ACCESS-TIMESTAMP"] = ts
            headers["ACCESS-PASSPHRASE"] = cred.api_passphrase
            headers["Content-Type"] = "application/json"
            headers["locale"] = "en-US"
            return headers

        # --- Header-based signature (OKX, Bybit) ---
        if spec.signature_header:
            if spec.use_iso8601_timestamp:
                # OKX: use server-time offset for timestamp
                server_ms = await self._server_timestamp_ms()
                ts = _iso8601_from_ms(server_ms)
            else:
                # Bybit: V1 applies 1500ms safety backoff to auth timestamp
                server_ms = await self._server_timestamp_ms()
                if spec.venue_id == Venue.BYBIT:
                    server_ms = max(0, server_ms - 1500)  # V1: BYBIT_AUTH_TIMESTAMP_BACKOFF_MS
                ts = str(server_ms)

            headers[spec.api_key_header] = cred.api_key
            headers[spec.timestamp_header] = ts

            if spec.requires_passphrase and spec.passphrase_header:
                headers[spec.passphrase_header] = cred.api_passphrase

            if spec.recv_window_header:
                # Bybit: use X-BAPI-RECV-WINDOW=5000
                headers[spec.recv_window_header] = str(spec.recv_window_ms or 5000)

            # OKX style: ts + method + path (+ query_string for GET/DELETE, body for POST)
            if spec.use_iso8601_timestamp:
                if query_string and method.upper() in ("GET", "DELETE"):
                    sign_payload = ts + method.upper() + path + query_string
                else:
                    sign_payload = ts + method.upper() + path + (body or "")
            else:
                # Bybit V5 style: ts + api_key + recv_window
                recv = headers.get(spec.recv_window_header, "5000")
                if query_string and method.upper() in ("GET", "DELETE"):
                    sign_payload = ts + cred.api_key + recv + query_string.lstrip("?")
                else:
                    sign_payload = ts + cred.api_key + recv + (body or "")

            sig = _sign_payload(spec.auth_scheme, cred.api_secret, sign_payload)
            headers[spec.signature_header] = sig

        return headers

    # Sync auth headers kept for backward-compatible test calls
    def _build_auth_headers(
        self,
        method: str,
        path: str,
        query_string: str = "",
        body: str = "",
        private: bool = False,
    ) -> dict[str, str]:
        spec = self._spec
        cred = self._credential
        if not private or cred is None:
            return {}
        headers: dict[str, str] = {}
        if spec.auth_scheme == AuthScheme.EIP712:
            headers["Content-Type"] = "application/json"
            return headers
        if spec.signature_param:
            ts = str(int(time.time() * 1000))
            payload = query_string.lstrip("?") if query_string else ""
            if spec.timestamp_param and spec.timestamp_param not in (payload or ""):
                ts_param = f"{spec.timestamp_param}={ts}"
                payload = f"{ts_param}&{payload}" if payload else ts_param
            headers[spec.api_key_header] = cred.api_key
            return headers
        if spec.auth_scheme == AuthScheme.HMAC_SHA512_HEX:
            import hashlib as _hashlib
            ts = str(int(time.time()))
            body_hash = _hashlib.sha512((body or "").encode()).hexdigest()
            sign_payload = f"{method.upper()}\n{path}\n{query_string.lstrip('?')}\n{body_hash}\n{ts}"
            sig = _sign_payload(spec.auth_scheme, cred.api_secret, sign_payload)
            headers["KEY"] = cred.api_key
            headers["Timestamp"] = ts
            headers["SIGN"] = sig
            headers["Content-Type"] = "application/json"
            headers["X-Gate-Size-Decimal"] = "1"
            return headers
        if spec.venue_id == Venue.BITGET:
            ts = str(int(time.time() * 1000))
            request_path = f"{path}?{query_string.lstrip('?')}" if query_string else path
            sign_payload = ts + method.upper() + request_path + (body or "")
            sig = _sign_payload(spec.auth_scheme, cred.api_secret, sign_payload)
            headers["ACCESS-KEY"] = cred.api_key
            headers["ACCESS-SIGN"] = sig
            headers["ACCESS-TIMESTAMP"] = ts
            headers["ACCESS-PASSPHRASE"] = cred.api_passphrase
            headers["Content-Type"] = "application/json"
            headers["locale"] = "en-US"
            return headers
        if spec.signature_header:
            if spec.use_iso8601_timestamp:
                ts = _iso8601_now()
            else:
                ts = str(int(time.time() * 1000))
                # V1: Bybit auth timestamp backoff not possible in sync path (no server time),
                # but live path uses _build_auth_headers_async which applies -1500ms.
            headers[spec.api_key_header] = cred.api_key
            headers[spec.timestamp_header] = ts
            if spec.requires_passphrase and spec.passphrase_header:
                headers[spec.passphrase_header] = cred.api_passphrase
            if spec.recv_window_header:
                headers[spec.recv_window_header] = "5000"
            if spec.use_iso8601_timestamp:
                if query_string and method.upper() in ("GET", "DELETE"):
                    sign_payload = ts + method.upper() + path + query_string
                else:
                    sign_payload = ts + method.upper() + path + (body or "")
            else:
                recv = headers.get(spec.recv_window_header, "5000")
                if query_string and method.upper() in ("GET", "DELETE"):
                    sign_payload = ts + cred.api_key + recv + query_string.lstrip("?")
                else:
                    sign_payload = ts + cred.api_key + recv + (body or "")
            sig = _sign_payload(spec.auth_scheme, cred.api_secret, sign_payload)
            headers[spec.signature_header] = sig
        return headers

    async def _build_signed_request_async(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
        body: Optional[dict[str, Any]] = None,
        private: bool = False,
    ) -> tuple[str, dict[str, str], Optional[str]]:
        """Build signed request with V1 parity: server time, recvWindow, query-only for Binance/Aster."""
        spec = self._spec
        query_string = ""
        req_body: Optional[str] = None
        cred = self._credential

        # Binance/Aster private requests: query-only signing with recvWindow
        if spec.signature_param and private and cred:
            # Build ordered query params (V1: preserve caller order, no sort)
            qp_list: list[tuple[str, str]] = []
            if params:
                for k, v in params.items():
                    qp_list.append((k, str(v)))
            # V1: recvWindow BEFORE timestamp
            if spec.recv_window_ms:
                qp_list.append(("recvWindow", str(spec.recv_window_ms)))
            ts = str(await self._server_timestamp_ms())
            if spec.timestamp_param:
                qp_list.append((spec.timestamp_param, ts))
            # Build query without signature, then sign
            encoded = _build_query_v1(qp_list)
            sig = _sign_payload(spec.auth_scheme, cred.api_secret, encoded)
            qp_list.append((spec.signature_param, sig))
            query_string = "?" + _build_query_v1(qp_list)
            req_body = None  # Binance/Aster: no body for order placement

        elif spec.signature_param and not private:
            # Public request — no signature, just params
            if params:
                qp_list = [(k, str(v)) for k, v in params.items()]
                query_string = "?" + _build_query_v1(qp_list)
        elif params:
            qp_list = [(k, str(v)) for k, v in sorted(params.items())]
            query_string = "?" + _build_query_v1(qp_list) if qp_list else ""

        if body is not None:
            req_body = json.dumps(body)

        headers = await self._build_auth_headers_async(method, path, query_string, req_body or "", private=private)
        if req_body and "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"

        return query_string, headers, req_body

    # Keep sync version for backward compatibility with tests
    def _build_signed_request(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
        body: Optional[dict[str, Any]] = None,
        private: bool = False,
    ) -> tuple[str, dict[str, str], Optional[str]]:
        """Synchronous fallback: uses wall-clock time. Tests should prefer async path."""
        spec = self._spec
        query_string = ""
        req_body: Optional[str] = None
        cred = self._credential

        if spec.signature_param and private and cred:
            qp_list: list[tuple[str, str]] = []
            if params:
                for k, v in params.items():
                    qp_list.append((k, str(v)))
            # V1: recvWindow BEFORE timestamp
            if spec.recv_window_ms:
                qp_list.append(("recvWindow", str(spec.recv_window_ms)))
            ts = str(int(time.time() * 1000))
            if spec.timestamp_param:
                qp_list.append((spec.timestamp_param, ts))
            encoded = _build_query_v1(qp_list)
            sig = _sign_payload(spec.auth_scheme, cred.api_secret, encoded)
            qp_list.append((spec.signature_param, sig))
            query_string = "?" + _build_query_v1(qp_list)
            req_body = None

        elif spec.signature_param and not private:
            if params:
                qp_list = [(k, str(v)) for k, v in params.items()]
                query_string = "?" + _build_query_v1(qp_list)
        elif params:
            qp_list = [(k, str(v)) for k, v in sorted(params.items())]
            query_string = "?" + _build_query_v1(qp_list) if qp_list else ""

        # Binance/Aster: body fields go into query string, not JSON body
        if body is not None and not (spec.signature_param and private and cred):
            req_body = json.dumps(body)

        headers = self._build_auth_headers(method, path, query_string, req_body or "", private=private)
        if req_body and "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"

        return query_string, headers, req_body

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
        body: Optional[dict[str, Any]] = None,
        private: bool = False,
        _retry_ts_error: bool = True,
    ) -> dict[str, Any]:
        """Send an HTTP request with V1 parity: server time, scoped rate limiting, time error retry."""
        client = await self._get_client()
        base_url = (
            self._spec.private_base_url if private
            else self._spec.public_base_url
        )
        qs, headers, req_body = await self._build_signed_request_async(method, path, params, body, private=private)
        url = base_url + path + qs

        # V1-aligned scoped rate limiting
        scopes = self._rest_rate_limit_scopes(method, path, base_url, private=private)

        if self._rate_limiter is not None:
            await self._rate_limiter.wait_until_ready_for_scopes(scopes)
            await self._rate_limiter.pace_for_scopes(scopes)

        # Global runtime check
        from lightfee.rate_limit.engine import global_rate_limit_runtime as _get_global_rt
        global_rt = _get_global_rt()
        if global_rt is not None:
            await global_rt.async_wait_until_ready_for_scopes(scopes)

        try:
            if method.upper() == "GET":
                resp = await client.get(url, headers=headers)
            elif method.upper() == "POST":
                resp = await client.post(url, headers=headers, content=req_body)
            elif method.upper() == "PUT":
                resp = await client.put(url, headers=headers, content=req_body)
            elif method.upper() == "DELETE":
                resp = await client.delete(url, headers=headers)
            else:
                raise ValueError(f"unsupported HTTP method: {method}")

            if resp.status_code >= 400:
                # V1: record rate-limit for 429/418 to trigger cooldown on all scopes
                if resp.status_code in (429, 418):
                    now_ms = int(time.time() * 1000)
                    retry_after_ms = _parse_venue_retry_after_ms(
                        self._spec.venue_id, dict(resp.headers), now_ms,
                    )
                    if self._rate_limiter is not None:
                        self._rate_limiter.record_rate_limit_for_scopes(
                            scopes, retry_after_ms=retry_after_ms,
                        )
                    # Also record in global runtime
                    if global_rt is not None:
                        global_rt.record_rate_limit_for_scopes(
                            scopes, retry_after_ms=retry_after_ms or 0,
                        )

                # Binance/Aster time error retry (Task 10)
                if _retry_ts_error and private and self._is_time_offset_retryable(
                    resp.status_code, resp.text
                ):
                    self._clear_server_time_offset()
                    await asyncio.sleep(0.1)  # V1: short delay before retry
                    return await self._request(
                        method, path, params=params, body=body,
                        private=private, _retry_ts_error=False,
                    )

                cat = classify_transport_error(resp.status_code, resp.text)
                if cat:
                    raise TransportError(
                        cat, f"HTTP {resp.status_code}: {resp.text[:200]}",
                        status_code=resp.status_code, body=resp.text,
                        headers=dict(resp.headers),
                    )

            # V1: record success for all scopes
            if self._rate_limiter is not None:
                self._rate_limiter.record_success_for_scopes(scopes)

            if not resp.text:
                return {}
            return resp.json()
        except httpx.TimeoutException:
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                f"timeout: {method} {path}",
            )
        except httpx.NetworkError as e:
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                f"network error: {method} {path}: {e}",
            )
        except TransportError:
            raise
        except Exception as e:
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                f"unexpected error: {method} {path}: {e}",
            )

    def _is_time_offset_retryable(self, status_code: int, body: str) -> bool:
        """V1: should_retry_binance_order_error — retry once on time/signature/order-mode/5xx errors.

        V1 predicate matches (case-insensitive, on error message):
          code=-1021, recvwindow, timestamp, position-side mismatch, 500-504
        """
        spec = self._spec
        if spec.venue_id not in (Venue.BINANCE, Venue.ASTER):
            return False
        msg = f"status={status_code} {body}".lower()
        return (
            "code=-1021" in msg
            or "recvwindow" in msg
            or "timestamp" in msg
            or ("position side" in msg and "setting" in msg)
            or "positionside" in msg
            or "status=500" in msg
            or "status=502" in msg
            or "status=503" in msg
            or "status=504" in msg
        )

    def _rest_rate_limit_scopes(
        self, method: str, path: str, base_url: str, *, private: bool = False,
    ) -> list[str]:
        """Derive V1 rate-limit scopes for a REST request.

        V1 reference: augment_scopes_from_config (rate_limit/mod.rs:586-636).
        Scopes are derived from [venue.*.scopes] in rate_limits.toml / built-in config,
        NOT from VenueSpec.endpoint_scope_map (which is empty).
        """
        spec = self._spec
        endpoint = _normalize_rest_endpoint_key(method, path)
        host_scope = _normalize_host_scope(base_url)
        scopes = [endpoint, host_scope]

        # Venue scope
        venue_id = spec.venue_id.value
        venue_scope = f"venue:{venue_id}"
        scopes.append(venue_scope)

        # V1: derive group scopes from rate-limit config [venue.*.scopes]
        from lightfee.rate_limit.config import built_in_defaults
        from lightfee.rate_limit.engine import global_rate_limit_runtime as _get_global_rt

        # Try global runtime config first, fall back to built-in defaults
        global_rt = _get_global_rt()
        if global_rt is not None and global_rt.config_manager is not None:
            config = global_rt.config_manager.config
        else:
            config = built_in_defaults()

        venue_config = config.venues.get(venue_id) if config else None
        if venue_config is not None:
            # Look up endpoint -> group mapping from [venue.*.scopes]
            scope_map = getattr(venue_config, "scopes", {}) or {}
            if endpoint in scope_map:
                group_name = scope_map[endpoint]
                scopes.append(f"group:{venue_id}:{group_name}")
                scopes.append(f"group:{group_name}")
            # Also derive from default group weights if no explicit scope mapping
            group_weights = getattr(venue_config, "group_weights", {}) or {}
            for group_name in group_weights:
                if group_name not in ("ws_public", "ws_private"):
                    continue  # only websocket groups are auto-included; REST groups use scopes map

        return scopes

    # ------------------------------------------------------------------
    # Market snapshot
    # ------------------------------------------------------------------

    async def fetch_market_snapshot(self, symbols: list[str]) -> VenueMarketSnapshot:
        spec = self._spec
        now_ms = int(time.time() * 1000)

        if self.mode == "paper":
            quotes = tuple(
                VenueMarketQuote(
                    symbol=self._venue_symbol(sym),
                    bid=0.0,
                    ask=0.0,
                )
                for sym in symbols
            )
            return VenueMarketSnapshot(
                venue=spec.venue_id,
                observed_at_ms=now_ms,
                quotes=quotes,
            )

        # Live path: call the venue market endpoint
        try:
            raw = await self._request("GET", spec.market_snapshot_path)
            return self._parse_market_snapshot(raw, symbols, now_ms)
        except TransportError:
            raise
        except Exception as e:
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                f"market snapshot failed: {e}",
            )

    def _parse_market_snapshot(
        self, raw: dict[str, Any], symbols: list[str], now_ms: int
    ) -> VenueMarketSnapshot:
        spec = self._spec
        quotes: list[VenueMarketQuote] = []

        if spec.venue_id == Venue.BINANCE or spec.venue_id == Venue.ASTER:
            # Response is a list or single dict of {symbol, bidPrice, askPrice, ...}
            items = raw if isinstance(raw, list) else [raw]
            for item in items:
                sym = item.get("symbol", "")
                if sym:
                    quotes.append(
                        VenueMarketQuote(
                            symbol=sym,
                            bid=float(item.get("bidPrice", 0)),
                            ask=float(item.get("askPrice", 0)),
                            bid_size=float(item.get("bidQty", 0)),
                            ask_size=float(item.get("askQty", 0)),
                        )
                    )
        elif spec.venue_id in (Venue.OKX, Venue.BYBIT):
            # Bybit V5 wraps tickers in result.list; OKX uses data[]
            if spec.venue_id == Venue.BYBIT:
                result = raw.get("result", raw)
                if isinstance(result, dict):
                    items = result.get("list", [])
                else:
                    items = result if isinstance(result, list) else [result]
            else:
                data = raw.get("data", raw)
                items = data if isinstance(data, list) else [data]
            for item in items:
                sym = item.get("instId", item.get("symbol", ""))
                if sym:
                    quotes.append(
                        VenueMarketQuote(
                            symbol=sym,
                            bid=float(item.get("bidPx", item.get("bid1Price", 0))),
                            ask=float(item.get("askPx", item.get("ask1Price", 0))),
                            bid_size=float(item.get("bidSz", item.get("bid1Size", 0))),
                            ask_size=float(item.get("askSz", item.get("ask1Size", 0))),
                        )
                    )
        elif spec.venue_id == Venue.BITGET:
            data = raw.get("data", raw)
            items = data if isinstance(data, list) else [data]
            for item in items:
                sym = item.get("symbol", "")
                if sym:
                    quotes.append(
                        VenueMarketQuote(
                            symbol=sym,
                            bid=float(item.get("bestBid", 0)),
                            ask=float(item.get("bestAsk", 0)),
                        )
                    )
        elif spec.venue_id == Venue.GATE:
            items = raw if isinstance(raw, list) else [raw]
            for item in items:
                sym = item.get("contract", "")
                if sym:
                    quotes.append(
                        VenueMarketQuote(
                            symbol=sym,
                            bid=float(item.get("mark_price", 0)),
                            ask=float(item.get("mark_price", 0)),
                        )
                    )
        elif spec.venue_id == Venue.HYPERLIQUID:
            # Info API returns differently structured data
            if isinstance(raw, list):
                for item in raw:
                    quotes.append(
                        VenueMarketQuote(
                            symbol=item.get("name", item.get("coin", "")),
                            bid=float(item.get("markPx", 0)),
                            ask=float(item.get("markPx", 0)),
                        )
                    )

        return VenueMarketSnapshot(
            venue=spec.venue_id,
            observed_at_ms=now_ms,
            quotes=tuple(quotes),
        )

    # ------------------------------------------------------------------
    # Local-L2 snapshot — REST order book depth bootstrap
    # ------------------------------------------------------------------

    async def fetch_l2_snapshot(
        self, symbol: str, depth: int = 50,
        retry_initial_ms: int = 5000,
        retry_max_ms: int = 40000,
        max_retries: int = 8,
    ) -> "LocalL2Update":
        """Fetch full order book depth snapshot for local-L2 bootstrap.

        Returns a canonical LocalL2Update that can be fed directly into
        LocalL2Runtime.record_update().

        Uses the venue's public REST depth endpoint — no authentication needed.
        Hyperliquid uses POST to /info API with l2Book type.

        Retry: V1-aligned exponential backoff with jitter.
        V1 bootstrap_binance_local_l2_symbol loops indefinitely with
        FailureBackoff (5s initial, 40s max).  Python equivalent retries
        up to *max_retries* times.
        """
        from lightfee.marketdata.local_l2_venues import parse_l2_update

        spec = self._spec
        venue_sym = self._venue_symbol(symbol)
        now_ms = int(time.time() * 1000)

        if not spec.l2_snapshot_path:
            raise TransportError(
                TransportErrorCategory.UNSUPPORTED_CAPABILITY,
                f"L2 snapshot not supported for {spec.venue_id.value}",
            )

        # Bitget metadata guard: block unsupported symbols before HTTP
        # V1: bitget_fetch_execution_liquidity_snapshot() requires metadata before
        # calling /api/v3/market/orderbook.  When metadata is empty (not yet loaded)
        # or the symbol is absent, reject immediately — never send an orderbook HTTP
        # request for an unsupported symbol.
        if spec.venue_id == Venue.BITGET:
            if not self._symbol_metadata or venue_sym not in self._symbol_metadata:
                raise TransportError(
                    TransportErrorCategory.REQUEST_REJECTED,
                    f"bitget execution liquidity metadata missing for {venue_sym}",
                )

        if self.mode == "paper":
            from lightfee.marketdata.l2 import LocalL2Update, LocalL2UpdateKind
            return LocalL2Update(
                venue=spec.venue_id.value,
                symbol=venue_sym,
                update_kind=LocalL2UpdateKind.SNAPSHOT,
                received_at_ms=now_ms,
            )

        failures = 0
        while True:
            try:
                if spec.venue_id == Venue.HYPERLIQUID:
                    body = {"type": "l2Book", "coin": venue_sym}
                    raw = await self._request("POST", spec.l2_snapshot_path, body=body)
                else:
                    params: dict[str, Any] = {}
                    if spec.venue_id == Venue.BINANCE or spec.venue_id == Venue.ASTER:
                        params["symbol"] = venue_sym
                        params["limit"] = str(depth)
                    elif spec.venue_id == Venue.OKX:
                        params["instId"] = venue_sym
                        params["sz"] = str(depth)
                    elif spec.venue_id == Venue.BYBIT:
                        params["category"] = "linear"
                        params["symbol"] = venue_sym
                        params["limit"] = str(depth)
                    elif spec.venue_id == Venue.BITGET:
                        params["category"] = "USDT-FUTURES"  # V1: BITGET_PRODUCT_TYPE
                        params["symbol"] = venue_sym
                        params["limit"] = str(depth)
                    elif spec.venue_id == Venue.GATE:
                        params["contract"] = venue_sym
                        params["limit"] = str(depth)

                    raw = await self._request("GET", spec.l2_snapshot_path, params=params)

                result = parse_l2_update(
                    spec.venue_id.value, payload=raw,
                    symbol=venue_sym, now_ms=now_ms,
                )
                result.symbol = symbol
                return result

            except TransportError as e:
                if e.category != TransportErrorCategory.TRANSPORT_FAILURE:
                    raise
                failures += 1
                if failures > max_retries:
                    raise
                # V1 exponential backoff with jitter: delay = min(initial * 2^failures, max)
                shift = min(failures - 1, 20)
                backoff_ms = min(retry_initial_ms << shift, retry_max_ms)
                # V1: extract Retry-After from rate-limit response, take max
                retry_after_ms = 0
                if e.status_code in (429, 418):
                    retry_after_ms = _parse_retry_after_ms(e.headers) or 0
                delay_ms = max(backoff_ms, retry_after_ms)
                jitter = random.randint(0, max(delay_ms // 5, 1))
                delay_ms += jitter
                await asyncio.sleep(delay_ms / 1000.0)

            except Exception as e:
                raise TransportError(
                    TransportErrorCategory.TRANSPORT_FAILURE,
                    f"L2 snapshot failed for {spec.venue_id.value}:{symbol}: {e}",
                )

    # ------------------------------------------------------------------
    # Position fetch
    # ------------------------------------------------------------------

    async def fetch_all_positions(self) -> list[PositionSnapshot]:
        spec = self._spec
        now_ms = int(time.time() * 1000)

        if self.mode == "paper":
            return []

        try:
            if spec.venue_id == Venue.HYPERLIQUID:
                body = {
                    "type": "clearinghouseState",
                    "user": self._credential.account_address if self._credential else "",
                }
                raw = await self._request("POST", spec.position_path, body=body, private=True)
            else:
                params: dict[str, Any] = {}
                if spec.venue_id == Venue.BYBIT:
                    params["category"] = "linear"
                    params["settleCoin"] = "USDT"
                elif spec.venue_id == Venue.BITGET:
                    params["productType"] = "USDT-FUTURES"
                    params["marginCoin"] = "USDT"
                raw = await self._request("GET", spec.position_path, params=params, private=True)
            return self._parse_all_positions(raw, now_ms)
        except TransportError:
            raise
        except Exception as e:
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                f"position fetch all failed: {e}",
            )

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        spec = self._spec
        venue_sym = self._venue_symbol(symbol)
        now_ms = int(time.time() * 1000)

        if self.mode == "paper":
            return PositionSnapshot(
                venue=spec.venue_id,
                symbol=venue_sym,
                side=Side.BUY,
                quantity=0.0,
                entry_price=0.0,
                observed_at_ms=now_ms,
            )

        try:
            params: dict[str, Any] = {}
            if spec.venue_id == Venue.BITGET:
                params["symbol"] = venue_sym
                params["marginCoin"] = "USDT"
            elif spec.venue_id == Venue.BYBIT:
                params["category"] = "linear"
                params["symbol"] = venue_sym
            elif spec.venue_id == Venue.OKX:
                params["instId"] = venue_sym
            elif spec.venue_id == Venue.HYPERLIQUID:
                body = {"type": "clearinghouseState", "user": self._credential.account_address if self._credential else ""}
                raw = await self._request("POST", spec.position_path, body=body, private=True)
                return self._parse_position(raw, venue_sym, now_ms)

            raw = await self._request("GET", spec.position_path, params=params, private=True)
            return self._parse_position(raw, venue_sym, now_ms)
        except TransportError:
            raise
        except Exception as e:
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                f"position fetch failed: {e}",
            )

    def _parse_all_positions(self, raw: Any, now_ms: int) -> list[PositionSnapshot]:
        spec = self._spec
        positions: list[PositionSnapshot] = []
        for row in _venue_position_rows(raw, spec.venue_id):
            venue_symbol = _position_row_symbol(row, spec.venue_id)
            if not venue_symbol:
                continue
            payload = _position_row_parse_payload(row, spec.venue_id)
            pos = self._parse_position(payload, venue_symbol, now_ms)
            if abs(pos.quantity) <= 1e-9:
                continue
            positions.append(
                replace(pos, symbol=_canonical_position_symbol(spec, venue_symbol))
            )
        return positions

    def _parse_position(
        self, raw: dict[str, Any], symbol: str, now_ms: int
    ) -> PositionSnapshot:
        """Dispatch to venue-specific position parser.

        V2 root fix: each venue gets its own parser with safe numeric handling
        and type-aware envelope extraction. Falls back to generic parsing for
        flat data that doesn't match venue envelopes.
        """
        spec = self._spec
        venue_sym = self._venue_symbol(symbol)

        if spec.venue_id == Venue.BYBIT:
            result = _parse_bybit_position(raw, venue_sym, now_ms)
            if result.quantity > 0 or not isinstance(raw, dict):
                return result
            # Fallback: flat data may be passed by tests
            return _parse_generic_position(raw, spec, venue_sym, now_ms)
        if spec.venue_id == Venue.BITGET:
            result = _parse_bitget_position(raw, venue_sym, now_ms)
            if result.quantity > 0 or not isinstance(raw, dict):
                return result
            return _parse_generic_position(raw, spec, venue_sym, now_ms)
        if spec.venue_id == Venue.OKX:
            result = _parse_okx_position(raw, venue_sym, now_ms, contract_size=spec.contract_size)
            if result.quantity > 0 or not isinstance(raw, dict):
                return result
            return _parse_generic_position(raw, spec, venue_sym, now_ms)
        if spec.venue_id in (Venue.BINANCE, Venue.ASTER):
            result = _parse_binance_like_position(raw, venue_sym, now_ms, venue=spec.venue_id)
            if result.quantity > 0 or not isinstance(raw, dict):
                return result
            return _parse_generic_position(raw, spec, venue_sym, now_ms)
        if spec.venue_id == Venue.GATE:
            result = _parse_gate_position(raw, venue_sym, now_ms)
            if result.quantity > 0 or not isinstance(raw, dict):
                return result
            return _parse_generic_position(raw, spec, venue_sym, now_ms)
        if spec.venue_id == Venue.HYPERLIQUID:
            return _parse_hyperliquid_position(raw, venue_sym, now_ms)

        # Fallback: zero position
        return PositionSnapshot(
            venue=spec.venue_id,
            symbol=venue_sym,
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=now_ms,
        )

    # ------------------------------------------------------------------
    # Account risk snapshot (V1: per-venue account risk polling)
    # ------------------------------------------------------------------

    async def fetch_account_risk_snapshot(self) -> Optional[Any]:
        """Fetch an account risk snapshot for risk health evaluation.

        V1: Each venue adapter calls its specific private REST endpoint
        and converts the response into an AccountRiskSnapshot.
        Supported: Binance, OKX, Bybit, Aster, Bitget, Gate.
        Unsupported: Hyperliquid (no account risk endpoint).
        """
        from lightfee.engine.risk_actions import AccountRiskSnapshot as ARS

        spec = self._spec
        if self.mode != "live":
            return None
        if not spec.account_risk_path:
            return None

        now_ms = int(time.time() * 1000)

        try:
            if spec.venue_id == Venue.BINANCE or spec.venue_id == Venue.ASTER:
                raw = await self._request("GET", spec.account_risk_path, private=True)
                equity = float(raw.get("totalMarginBalance", 0))
                maint_margin_val = raw.get("totalMaintMargin")
                if maint_margin_val is None or str(maint_margin_val).strip() == "":
                    return None
                maint = float(maint_margin_val)
                if maint <= 0.0:
                    return None
                snapshot = ARS(
                    venue=spec.venue_id,
                    equity_quote=equity,
                    maintenance_margin_quote=maint,
                    health_ratio=equity / maint,
                    observed_at_ms=now_ms,
                    source="fapi_account",
                )
                snapshot.available_balance_quote = float(raw.get("availableBalance", 0))
                return snapshot

            elif spec.venue_id == Venue.OKX:
                raw = await self._request("GET", spec.account_risk_path, private=True)
                data_list = raw.get("data", [])
                if not data_list:
                    return None
                row = data_list[0] if isinstance(data_list, list) else data_list
                equity = float(row.get("totalEq", 0))
                mmr = row.get("mmr")
                if mmr is None:
                    return None
                maint = float(mmr)
                if maint <= 0.0:
                    return None
                snapshot = ARS(
                    venue=spec.venue_id,
                    equity_quote=equity,
                    maintenance_margin_quote=maint,
                    health_ratio=equity / maint,
                    observed_at_ms=now_ms,
                    source="okx_account_balance",
                )
                snapshot.available_balance_quote = float(row.get("availEq", 0)) if row.get("availEq") else None
                return snapshot

            elif spec.venue_id == Venue.BYBIT:
                raw = await self._request(
                    "GET", spec.account_risk_path,
                    params={"accountType": "UNIFIED"}, private=True,
                )
                result = raw.get("result", {})
                acct_list = result.get("list", []) if isinstance(result, dict) else []
                if not acct_list:
                    return None
                row = acct_list[0]
                equity = float(row.get("totalEquity", 0))
                maint_margin_val = row.get("totalMaintenanceMargin")
                if maint_margin_val is None:
                    return None
                maint = float(maint_margin_val)
                if maint <= 0.0:
                    return None
                snapshot = ARS(
                    venue=spec.venue_id,
                    equity_quote=equity,
                    maintenance_margin_quote=maint,
                    health_ratio=equity / maint,
                    observed_at_ms=now_ms,
                    source="bybit_wallet_balance",
                )
                snapshot.available_balance_quote = float(row.get("totalAvailableBalance", 0)) if row.get("totalAvailableBalance") else None
                return snapshot

            elif spec.venue_id == Venue.BITGET:
                raw = await self._request("GET", spec.account_risk_path, private=True)
                data = raw.get("data", raw)
                # V1: bitget_account_asset_row — find USDT margin row
                row = None
                if isinstance(data, dict):
                    row = data
                elif isinstance(data, list):
                    row = next(
                        (r for r in data if isinstance(r, dict) and
                         str(r.get("marginCoin", "")).upper() == "USDT"),
                        data[0] if data else None,
                    )
                if not row or not isinstance(row, dict):
                    return None
                maint = None
                for key in ("maintenanceMargin", "maintMargin", "maintainMargin", "maintenance_margin"):
                    if key in row:
                        val = row[key]
                        if val is not None and str(val).strip():
                            maint = float(val)
                            break
                if maint is None or maint <= 0.0:
                    return None
                equity = None
                for key in ("usdtEquity", "equity", "accountEquity"):
                    if key in row:
                        equity = float(row[key])
                        break
                if equity is None:
                    return None
                snapshot = ARS(
                    venue=spec.venue_id,
                    equity_quote=equity,
                    maintenance_margin_quote=maint,
                    health_ratio=equity / maint,
                    observed_at_ms=now_ms,
                    source="bitget_account_risk",
                )
                avail = None
                for key in ("available", "availableBalance", "crossedMaxAvailable"):
                    if key in row:
                        avail = float(row[key])
                        break
                snapshot.available_balance_quote = avail
                return snapshot

            elif spec.venue_id == Venue.GATE:
                raw = await self._request("GET", spec.account_risk_path, private=True)
                # V1: gate_account_risk_snapshot_from_wallet_row
                # Response is a single dict with account fields
                maint = None
                for key in ("maintenance_margin", "maintenanceMargin", "maint_margin", "maintMargin"):
                    if key in raw:
                        val = raw[key]
                        if val is not None and str(val).strip():
                            maint = float(val)
                            break
                if maint is None or maint <= 0.0:
                    return None
                equity = None
                for key in ("total", "equity", "total_balance"):
                    if key in raw:
                        equity = float(raw[key])
                        break
                if equity is None:
                    return None
                snapshot = ARS(
                    venue=spec.venue_id,
                    equity_quote=equity,
                    maintenance_margin_quote=maint,
                    health_ratio=equity / maint,
                    observed_at_ms=now_ms,
                    source="gate_account_risk",
                )
                avail = None
                for key in ("available", "available_balance"):
                    if key in raw:
                        avail = float(raw[key])
                        break
                snapshot.available_balance_quote = avail
                return snapshot

            else:
                return None
        except TransportError:
            raise
        except Exception as e:
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                f"account risk snapshot failed: {e}",
            )

    # ------------------------------------------------------------------
    # Order placement
    # ------------------------------------------------------------------

    async def place_order(self, request: OrderRequest) -> OrderFill:
        spec = self._spec
        venue_sym = self._venue_symbol(request.symbol)
        now_ms = int(time.time() * 1000)

        if self.mode == "paper":
            order_id = str(uuid.uuid4()).replace("-", "")[:20]
            return OrderFill(
                venue=spec.venue_id,
                symbol=venue_sym,
                side=request.side,
                quantity=request.quantity,
                price=request.price or 0.0,
                order_id=order_id,
                client_order_id=request.client_order_id,
                filled_at_ms=now_ms,
            )

        try:
            preflight = self.preflight_order_request(request)
            self._record_order_diagnostic("order.submit_attempt", preflight)
            request = replace(
                request,
                quantity=float(preflight["quantized_qty"]),
                price=(
                    None
                    if preflight["quantized_price"] is None
                    else float(preflight["quantized_price"])
                ),
            )
            body: dict[str, Any] = {
                "symbol": venue_sym,
                "side": request.side.value.upper(),
                "quantity": _format_decimal(request.quantity),
            }

            if spec.venue_id == Venue.BINANCE or spec.venue_id == Venue.ASTER:
                body["type"] = "MARKET"
                if request.reduce_only:
                    body["reduceOnly"] = "true"
                if request.price is not None:
                    body["type"] = "LIMIT"
                    body["price"] = _format_decimal(request.price)
                    body["timeInForce"] = "GTC"
            elif spec.venue_id == Venue.OKX:
                # V1: refresh posMode on first order (lazy, cached thereafter)
                if getattr(self, '_pos_mode_cache', None) is None:
                    await self._refresh_okx_pos_mode()
                pos_side = self._okx_pos_side(request.side, request.reduce_only)

                # V1: OKX contract sizing — base_qty / ctVal → contracts, floored to lotSz
                rules_cache = get_symbol_rules_cache()
                symbol_rule = await rules_cache.get(self, spec.venue_id, venue_sym)
                ct_val = float(getattr(symbol_rule, 'ct_val', 0) or 0)
                lot_sz = float(getattr(symbol_rule, 'qty_step', 0) or 0)
                base_qty = float(request.quantity)

                if ct_val > 0 and lot_sz > 0:
                    contract_qty = _floor_to_step(base_qty / ct_val, lot_sz)
                elif ct_val > 0:
                    contract_qty = base_qty / ct_val
                else:
                    contract_qty = base_qty

                body = {
                    "instId": venue_sym,
                    "tdMode": "cross",
                    "side": request.side.value.lower(),
                    "posSide": pos_side,
                    "ordType": "market",
                    "sz": _format_decimal(contract_qty),
                }
                if request.price is not None:
                    body["ordType"] = "limit"
                    body["px"] = _format_decimal(request.price)
                if request.client_order_id:
                    body["clOrdId"] = request.client_order_id
                if request.reduce_only:
                    body["reduceOnly"] = "true"
                # Enrich diagnostic with OKX-specific evidence
                preflight["pos_side"] = pos_side
                preflight["pos_mode"] = self._okx_pos_mode
                preflight["ct_val"] = ct_val
                preflight["base_qty"] = base_qty
                preflight["contract_qty"] = contract_qty
                preflight["lot_sz"] = lot_sz
                preflight["body_field_names"] = sorted(body.keys())
                preflight["body_sanitized"] = {
                    k: v for k, v in body.items()
                    if k not in ("clOrdId",)
                }
                self._record_order_diagnostic("order.submit_attempt", preflight)
            elif spec.venue_id == Venue.BYBIT:
                body = _build_bybit_order_body(
                    request, venue_sym, passive=False,
                    hedge_mode=self._hedge_mode,
                )
                if "qty" in body:
                    body["qty"] = _format_decimal(request.quantity)
                if request.price is not None:
                    body["price"] = _format_decimal(request.price)
            elif spec.venue_id == Venue.BITGET:
                body["marginCoin"] = "USDT"
                body["orderType"] = "market"
                if request.reduce_only:
                    body["reduceOnly"] = "true"
            elif spec.venue_id == Venue.GATE:
                body["contract"] = venue_sym
                body["size"] = int(request.quantity)
                if request.reduce_only:
                    body["reduce_only"] = True
                if request.price is not None:
                    body["price"] = _format_decimal(request.price)
            elif spec.venue_id == Venue.HYPERLIQUID:
                is_buy = request.side == Side.BUY
                tif = "Gtc" if request.price else "Ioc"
                limit_px = _format_decimal(request.price) if request.price else "0"

                if self.mode == "live":
                    from lightfee.venues.hyperliquid_signing import (
                        build_hyperliquid_exchange_payload,
                        build_hyperliquid_order_action,
                    )

                    asset_index = await self._hl_resolve_asset_index(venue_sym)
                    cloid = request.client_order_id or ""
                    action = build_hyperliquid_order_action(
                        symbol=venue_sym,
                        is_buy=is_buy,
                        quantity=request.quantity,
                        price=float(limit_px),
                        reduce_only=request.reduce_only,
                        tif=tif,
                        cloid=cloid if cloid else None,
                    )
                    # Override the placeholder asset index with the resolved one
                    action["orders"][0]["a"] = asset_index

                    vault_addr = None
                    if self._credential and self._credential.account_address:
                        vault_addr = self._credential.account_address

                    body = build_hyperliquid_exchange_payload(
                        action=action,
                        private_key_hex=self._credential.api_secret if self._credential else "",
                        vault_address=vault_addr,
                        is_mainnet=True,
                        cloid=cloid if cloid else None,
                    )
                else:
                    paper_order: dict[str, Any] = {
                        "a": 0,  # asset index — placeholder for paper
                        "b": is_buy,
                        "p": limit_px,
                        "s": str(request.quantity),
                        "r": request.reduce_only,
                        "t": {"limit": {"tif": tif}},
                    }
                    if request.client_order_id:
                        paper_order["c"] = request.client_order_id
                    body = {
                        "action": {
                            "type": "order",
                            "orders": [paper_order],
                            "grouping": "na",
                        },
                        "type": "order",
                    }
                    if request.client_order_id:
                        body["cloid"] = request.client_order_id
                    if self._credential and self._credential.account_address:
                        body["vaultAddress"] = self._credential.account_address

            # V1: Binance/Aster use query-only private signing (order fields in query, not body)
            if spec.venue_id in (Venue.BINANCE, Venue.ASTER):
                raw = await self._request("POST", spec.order_path, params=body, private=True)
            else:
                raw = await self._request("POST", spec.order_path, body=body, private=True)

            # V2: venue-specific success guard before parsing
            if spec.venue_id == Venue.BYBIT:
                _require_bybit_success(raw, "bybit order failed")
            elif spec.venue_id == Venue.BITGET:
                _require_bitget_success(raw, "bitget order failed")

            try:
                fill = self._parse_order_fill(raw, request, venue_sym, now_ms)
            except OrderSubmitError as exc:
                order_id, client_order_id = self._extract_order_identifiers(raw)
                result_payload = dict(preflight)
                result_payload["order_id"] = order_id
                result_payload["client_order_id"] = client_order_id or request.client_order_id or ""
                result_payload["response_classification"] = (
                    "ack_accepted" if exc.class_ == SubmitFailureClass.UNCERTAIN and order_id
                    else exc.class_.value
                )
                self._record_order_diagnostic("order.submit_result", result_payload)
                raise
            result_payload = dict(preflight)
            result_payload["order_id"] = fill.order_id
            result_payload["client_order_id"] = fill.client_order_id or request.client_order_id or ""
            result_payload["response_classification"] = "filled"
            self._record_order_diagnostic("order.submit_result", result_payload)
            return fill

        except TransportError as e:
            if e.category == TransportErrorCategory.REQUEST_REJECTED:
                raise _map_to_submit_error(e.category, str(e))
            raise _map_to_submit_error(e.category, str(e))
        except OrderSubmitError:
            raise
        except Exception as e:
            raise OrderSubmitError(SubmitFailureClass.UNCERTAIN, str(e))

    def _parse_order_fill(
        self,
        raw: dict[str, Any],
        request: OrderRequest,
        venue_sym: str,
        now_ms: int,
    ) -> OrderFill:
        spec = self._spec

        # Hyperliquid exchange response: {"status": "ok", "response": {"type": "order", "data": {"statuses": [...]}}}
        if spec.venue_id == Venue.HYPERLIQUID:
            resp_status = str(raw.get("status", "")).lower()
            if resp_status == "err":
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    str(raw.get("response", "Hyperliquid exchange error")),
                )
            response = raw.get("response", {})
            if isinstance(response, dict):
                inner_data = response.get("data", response)
                statuses = inner_data.get("statuses", []) if isinstance(inner_data, dict) else []
            else:
                statuses = []

            if not statuses:
                raise OrderSubmitError(
                    SubmitFailureClass.UNCERTAIN,
                    "Hyperliquid order response contains no statuses",
                )

            status_entry = statuses[0]
            if "filled" in status_entry:
                filled = status_entry["filled"]
                return OrderFill(
                    venue=spec.venue_id,
                    symbol=venue_sym,
                    side=request.side,
                    quantity=abs(float(filled.get("totalSz", 0))),
                    price=float(filled.get("avgPx", request.price or 0)),
                    order_id=str(filled.get("oid", "")),
                    client_order_id=request.client_order_id,
                    filled_at_ms=now_ms,
                )
            elif "resting" in status_entry:
                resting = status_entry["resting"]
                oid = str(resting.get("oid", ""))
                raise OrderSubmitError(
                    SubmitFailureClass.UNCERTAIN,
                    f"Hyperliquid order resting (oid={oid}) — fill not confirmed",
                )
            else:
                raise OrderSubmitError(
                    SubmitFailureClass.UNCERTAIN,
                    f"Hyperliquid unknown order status: {list(status_entry.keys())}",
                )

        data = raw.get("data", raw)

        # Bybit V5 nests the order under "result"
        if isinstance(data, dict) and "result" in data and isinstance(data["result"], dict):
            if data["result"].get("orderId") or data["result"].get("orderLinkId"):
                data = data["result"]
        elif isinstance(data, dict) and "result" in data and isinstance(data["result"], list) and data["result"]:
            data = data["result"][0]

        if isinstance(data, dict):
            pass
        elif isinstance(data, list) and data:
            data = data[0]

        if not isinstance(data, dict):
            raise OrderSubmitError(
                SubmitFailureClass.UNCERTAIN,
                "unexpected order response shape — cannot parse fill",
            )

        order_id = str(data.get(
            "orderId",
            data.get("ordId",
            data.get("id",
            data.get("order_id", "")))
        ))

        # Explicit fill quantity fields — do NOT fall back to request.quantity
        exec_qty_raw = data.get(
            "executedQty",
            data.get("cumExecQty",
            data.get("cumQty",
            data.get("fillSz",
            data.get("filledQty",
            data.get("filled_size", None)))))
        )

        exec_price_raw = data.get(
            "avgPrice",
            data.get("fillPx",
            data.get("fill_price", None))
        )

        status_str = str(data.get("status", "")).upper()
        has_fill_status = status_str in ("FILLED", "FINISHED", "CLOSED")

        if exec_qty_raw is not None:
            exec_qty = abs(float(exec_qty_raw))
            exec_price = float(exec_price_raw) if exec_price_raw is not None else float(data.get("price", 0))
        elif has_fill_status:
            exec_qty = abs(float(data.get("size", data.get("quantity", 0))))
            exec_price = float(data.get("price", data.get("avgPrice", 0)))
        elif order_id:
            raise OrderSubmitError(
                SubmitFailureClass.UNCERTAIN,
                f"order accepted (id={order_id}) but fill not confirmed — "
                "no executedQty/cumQty/fillSz in response",
            )
        else:
            raise OrderSubmitError(
                SubmitFailureClass.UNCERTAIN,
                "order response contains no order id and no fill data",
            )

        return OrderFill(
            venue=spec.venue_id,
            symbol=venue_sym,
            side=request.side,
            quantity=exec_qty,
            price=exec_price,
            order_id=order_id,
            client_order_id=request.client_order_id,
            filled_at_ms=now_ms,
        )

    @staticmethod
    def _extract_order_identifiers(raw: dict[str, Any]) -> tuple[str, str]:
        data: Any = raw.get("data", raw)
        if isinstance(data, dict) and isinstance(data.get("result"), dict):
            data = data["result"]
        elif isinstance(data, dict) and isinstance(data.get("result"), list) and data["result"]:
            data = data["result"][0]
        elif isinstance(data, list) and data:
            data = data[0]
        if not isinstance(data, dict):
            return "", ""
        order_id = str(data.get("orderId", data.get("ordId", data.get("id", data.get("order_id", "")))) or "")
        client_order_id = str(
            data.get(
                "orderLinkId",
                data.get("clientOrderId", data.get("clOrdId", data.get("client_order_id", ""))),
            )
            or ""
        )
        return order_id, client_order_id

    # ------------------------------------------------------------------
    # Order status reconciliation (Task 11)
    # ------------------------------------------------------------------

    async def fetch_order_status(
        self,
        symbol: str,
        *,
        order_id: str = "",
        client_order_id: str = "",
    ) -> Optional["OrderFillReconciliation"]:
        """Query order status by exchange order ID or client order ID.

        Bybit (V1: bybit.rs:2820-2894): two-step —
          1. If no order_id, resolve via /v5/order/realtime with orderLinkId
          2. Query /v5/execution/list with resolved orderId
          3. Aggregate execQty * execPrice for weighted avg, abs(execFee), max(execTime)
          4. total_quantity <= 0 → None

        Bitget (V1: bitget.rs:2912-2949): single-step —
          /api/v3/trade/order-info with orderId/clientOid,
          multi-key fallback for price/fee, quantity <= 0 → None

        Returns OrderFillReconciliation with filled quantity if found, or None.
        """
        spec = self._spec
        venue_sym = self._venue_symbol(symbol)
        now_ms = int(time.time() * 1000)

        if self.mode != "live":
            return None

        try:
            if spec.venue_id == Venue.BYBIT:
                return await self._fetch_order_status_bybit(
                    venue_sym, order_id, client_order_id, now_ms,
                )

            elif spec.venue_id == Venue.BITGET:
                params: dict[str, Any] = {}
                if order_id:
                    params["orderId"] = order_id
                if client_order_id:
                    params["clientOid"] = client_order_id
                raw = await self._request(
                    "GET", "/api/v3/trade/order-info", params=params, private=True,
                )
                return self._parse_order_status_bitget(raw, venue_sym, now_ms)

        except TransportError as e:
            if e.category == TransportErrorCategory.REQUEST_REJECTED:
                raise
            return None
        except _BybitOrderNotFound:
            return None

        return None

    async def _fetch_order_status_bybit(
        self,
        venue_sym: str,
        order_id: str,
        client_order_id: str,
        now_ms: int,
    ) -> Optional["OrderFillReconciliation"]:
        """Bybit V1 two-step reconciliation: resolve orderId → execution/list → aggregate.

        V1: bybit.rs fetch_order_fill_reconciliation (lines 2820-2894)
        """
        resolved_order_id = order_id
        resolved_client_id = ""

        # Step 1: resolve orderId from client_order_id if needed
        if not resolved_order_id:
            params: dict[str, Any] = {
                "category": "linear",
                "symbol": venue_sym,
                "openOnly": 0,
            }
            if client_order_id:
                params["orderLinkId"] = client_order_id
            else:
                return None  # need at least one identifier

            raw = await self._request(
                "GET", "/v5/order/realtime", params=params, private=True,
            )
            # V1: check retCode before parsing (Bybit error codes doc)
            self._require_bybit_reconciliation_success(raw, "bybit order realtime")

            result = raw.get("result", {})
            data = result.get("list", [None])[0] if isinstance(result, dict) and result.get("list") else result
            if not isinstance(data, dict):
                return None

            resolved_order_id = str(data.get("orderId", ""))
            resolved_client_id = str(data.get("orderLinkId", ""))
            if not resolved_order_id:
                return None

        # Step 2: query /v5/execution/list with resolved orderId
        exec_params = {
            "category": "linear",
            "symbol": venue_sym,
            "orderId": resolved_order_id,
        }
        exec_raw = await self._request(
            "GET", "/v5/execution/list", params=exec_params, private=True,
        )
        # V1: check retCode for execution list too
        self._require_bybit_reconciliation_success(exec_raw, "bybit execution reconciliation")

        # Step 3: aggregate executions
        return self._parse_bybit_execution_list(
            exec_raw, venue_sym, resolved_order_id, resolved_client_id, now_ms,
        )

    def _require_bybit_reconciliation_success(
        self, raw: dict[str, Any], context: str,
    ) -> None:
        """Check Bybit retCode for reconciliation responses.

        V1: bybit.rs checks ret_code and surfaces business errors.
        Docs: https://bybit-exchange.github.io/docs/v5/error

        - retCode=0: success → no-op
        - retCode=110001 (Order does not exist): returns None (caller handles)
        - Other non-zero: raises TransportError(REQUEST_REJECTED)
        """
        ret_code = int(raw.get("retCode", 0) or 0)
        if ret_code == 0:
            return
        if ret_code == 110001:
            # Order does not exist — caller should return None
            raise _BybitOrderNotFound()
        raise TransportError(
            TransportErrorCategory.REQUEST_REJECTED,
            f"{context}: bybit retCode={ret_code} retMsg={raw.get('retMsg', '')}",
        )

    def _parse_order_status_bybit(
        self, raw: dict[str, Any], venue_sym: str, now_ms: int,
    ) -> Optional["OrderFillReconciliation"]:
        """Parse Bybit /v5/order/realtime response into OrderFillReconciliation.

        V1: total_quantity <= 0 → None. NEW/ACTIVE with 0 fill NOT treated as filled.
        """
        result = raw.get("result", {})
        data = result.get("list", [None])[0] if isinstance(result, dict) and result.get("list") else result
        if not isinstance(data, dict):
            return None

        order_id = str(data.get("orderId", ""))
        client_id = str(data.get("orderLinkId", ""))
        cum_qty = _safe_float(data.get("cumExecQty", "0"))
        avg_price = _safe_float(data.get("avgPrice", "0"))
        side_raw = str(data.get("side", "")).strip()
        if side_raw == "Buy":
            side = Side.BUY
        elif side_raw == "Sell":
            side = Side.SELL
        elif cum_qty <= 0.0:
            return None  # zero qty: no side needed (V1: Err for missing, but qty=0 exits early)
        else:
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                f"bybit order status has invalid/missing side value {side_raw!r}",
            )
        status_str = str(data.get("orderStatus", "")).upper()
        filled_at = int(data.get("updatedTime", now_ms))

        # V1: only Filled/PartiallyFilled orders with quantity > 0
        if status_str not in ("FILLED", "PARTIALLY_FILLED"):
            return None
        if cum_qty <= 0.0:
            return None

        return OrderFillReconciliation(
            venue=Venue.BYBIT,
            symbol=venue_sym,
            side=side,
            quantity=cum_qty,
            average_price=avg_price,
            order_id=order_id,
            client_order_id=client_id,
            filled_at_ms=filled_at,
        )

    def _parse_bybit_execution_list(
        self,
        raw: dict[str, Any],
        venue_sym: str,
        order_id: str,
        client_order_id: str,
        now_ms: int,
    ) -> Optional["OrderFillReconciliation"]:
        """Parse Bybit /v5/execution/list response, aggregate executions.

        V1: bybit.rs lines 2870-2893 — sum execQty, weighted notional,
        abs(execFee), max(execTime). total_quantity <= 0 → None.

        Side is parsed from each execution entry (official field: side=Buy|Sell).
        All execution sides must be consistent — mismatch raises TransportError
        rather than silently picking one.
        """
        result = raw.get("result", {})
        executions = result.get("list", []) if isinstance(result, dict) else []
        if not executions:
            return None

        total_qty = 0.0
        weighted_notional = 0.0
        total_fee = 0.0
        latest_fill_ms = 0
        resolved_side: Optional[Side] = None

        for ex in executions:
            if not isinstance(ex, dict):
                continue
            qty = _safe_float(ex.get("execQty", "0"))
            price = _safe_float(ex.get("execPrice", "0"))
            fee = _safe_float(ex.get("execFee", "0"))
            ex_time = int(ex.get("execTime", 0))
            total_qty += qty
            weighted_notional += price * qty
            total_fee += abs(fee)
            if ex_time > latest_fill_ms:
                latest_fill_ms = ex_time

            # V1: parse side from execution (docs: side=Buy|Sell)
            side_raw = str(ex.get("side", "")).strip()
            if side_raw:
                if side_raw == "Buy":
                    ex_side = Side.BUY
                elif side_raw == "Sell":
                    ex_side = Side.SELL
                else:
                    raise TransportError(
                        TransportErrorCategory.REQUEST_REJECTED,
                        f"bybit execution has invalid side value {side_raw!r} "
                        f"(orderId={order_id})",
                    )
                if resolved_side is None:
                    resolved_side = ex_side
                elif resolved_side != ex_side:
                    raise TransportError(
                        TransportErrorCategory.REQUEST_REJECTED,
                        f"bybit execution list has inconsistent sides: "
                        f"{resolved_side.value} vs {ex_side.value} (orderId={order_id})",
                    )

        if total_qty <= 0.0:
            return None

        if resolved_side is None:
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                f"bybit execution list has no side field (orderId={order_id})",
            )

        return OrderFillReconciliation(
            venue=Venue.BYBIT,
            symbol=venue_sym,
            side=resolved_side,
            quantity=total_qty,
            average_price=weighted_notional / total_qty,
            order_id=order_id,
            client_order_id=client_order_id or None,
            fee_quote=total_fee if total_fee > 0 else None,
            filled_at_ms=latest_fill_ms if latest_fill_ms > 0 else now_ms,
        )

    def _parse_order_status_bitget(
        self, raw: dict[str, Any], venue_sym: str, now_ms: int,
    ) -> Optional["OrderFillReconciliation"]:
        """Parse Bitget /api/v3/trade/order-info response into OrderFillReconciliation.

        V1: quantity <= 0 → None. Multi-key fallback for price/fee/orderId/clientOid.
        """
        _require_bitget_success(raw, "bitget order status failed")
        data = raw.get("data", raw)
        if isinstance(data, list):
            data = data[0] if data else {}
        if not isinstance(data, dict):
            return None

        order_id = str(data.get("orderId", ""))
        client_id = str(data.get("clientOid", ""))
        # V1 multi-key fallback: cumExecQty, baseVolume, filledQty, fillQty, filled_amount, size
        cum_qty = _safe_float(
            data.get("cumExecQty", data.get("baseVolume", data.get("filledQty", data.get("fillQty", data.get("filled_amount", data.get("size", data.get("fillSz", "0")))))))
        )
        avg_price = _safe_float(
            data.get("priceAvg", data.get("avgPrice", data.get("fillPriceAvg", data.get("averagePrice", "0"))))
        )
        side_str = str(data.get("side", "buy")).lower()
        side = Side.BUY if side_str == "buy" else Side.SELL
        filled_at = int(data.get("uTime", data.get("cTime", data.get("updateTime", data.get("filledTime", now_ms)))))

        # V1: quantity <= 0 → None
        if cum_qty <= 0.0:
            return None

        # Fee extraction with multi-key fallback (V1: bitget.rs:2934-2938)
        fee_quote = None
        fee_val = data.get("fee", data.get("totalFee", data.get("filledFee")))
        if fee_val is not None:
            fee_quote = abs(_safe_float(fee_val))
        if fee_quote is None and "feeDetail" in data:
            fd = data["feeDetail"]
            if isinstance(fd, list):
                # Sum individual fee entries (official UTA shape)
                fee_sum = 0.0
                for entry in fd:
                    if isinstance(entry, dict):
                        fee_sum += abs(_safe_float(entry.get("fee", "0")))
                if fee_sum > 0:
                    fee_quote = fee_sum
            elif isinstance(fd, dict):
                tf = fd.get("totalFee")
                if tf is not None:
                    fee_quote = abs(_safe_float(tf))

        return OrderFillReconciliation(
            venue=Venue.BITGET,
            symbol=venue_sym,
            side=side,
            quantity=cum_qty,
            average_price=avg_price,
            order_id=order_id,
            client_order_id=client_id,
            fee_quote=fee_quote,
            filled_at_ms=filled_at,
        )

    # ------------------------------------------------------------------
    # Passive order contract (V1: GTC post-only maker order lifecycle)
    # ------------------------------------------------------------------

    async def submit_passive_order(self, request: OrderRequest) -> "PassiveOrderAck":
        """Submit a GTC post-only maker order. Returns ack, not fill.

        V2 root-fix changes:
        - Preflight/normalization runs before body building (was missing).
        - Body is venue-specific — no generic fields polluting all exchanges.
        - reduceOnly is NOT hardcoded; it respects request.reduce_only.
        - OKX uses sz/clOrdId (not quantity/newClientOrderId).
        - Diagnostic logging (order.submit_attempt / order.submit_result).
        """
        from lightfee.core.domain import PassiveOrderAck, PassiveOrderState
        from lightfee.venues.symbol_rules import get_symbol_rules_cache

        spec = self._spec
        venue_sym = self._venue_symbol(request.symbol)
        now_ms = int(time.time() * 1000)

        if self.mode == "paper":
            order_id = str(uuid.uuid4()).replace("-", "")[:20]
            cid = request.client_order_id or ""
            return PassiveOrderAck(
                venue=spec.venue_id,
                symbol=venue_sym,
                side=request.side,
                order_id=order_id,
                client_order_id=cid,
                price=request.price or 0.0,
                quantity=request.quantity,
                accepted_at_ms=now_ms,
                state=PassiveOrderState.OPEN,
            )

        try:
            # --- Preflight: fetch dynamic symbol rules + normalize ---
            rules_cache = get_symbol_rules_cache()
            symbol_rule = await rules_cache.get(self, spec.venue_id, venue_sym)

            try:
                preflight = self.preflight_order_request(request, symbol_rule=symbol_rule)
            except OrderSubmitError:
                raise
            except Exception:
                preflight = self.preflight_order_request(request)

            quantized_qty = float(preflight["quantized_qty"])
            quantized_price = preflight["quantized_price"]
            tick_size = float(preflight.get("tick_size", 0))
            qty_step = float(preflight.get("quantity_step", 0))
            min_qty = float(preflight.get("min_qty", 0))
            min_notional = float(preflight.get("min_notional", 0))
            rule_source = str(preflight.get("rule_source", "spec"))
            cid = request.client_order_id or ""

            # --- V1: OKX posMode refresh + contract sizing ---
            if spec.venue_id == Venue.OKX:
                if getattr(self, '_pos_mode_cache', None) is None:
                    await self._refresh_okx_pos_mode()
            ct_val_sz = float(getattr(symbol_rule, 'ct_val', 0)) if symbol_rule else 0.0
            lot_sz = float(getattr(symbol_rule, 'qty_step', 0)) if symbol_rule else (qty_step or 0.0)
            if spec.venue_id == Venue.OKX and ct_val_sz > 0:
                contract_qty = _floor_to_step(quantized_qty / ct_val_sz, lot_sz) if lot_sz > 0 else (quantized_qty / ct_val_sz)
            else:
                contract_qty = quantized_qty

            # --- Build venue-specific body ---
            body = self._build_passive_order_body(
                request, venue_sym, contract_qty, quantized_price, cid,
            )

            # --- Diagnostic: order.submit_attempt ---
            ct_val = float(getattr(symbol_rule, 'ct_val', 0)) if symbol_rule else 0.0
            attempt_payload = {
                "venue": spec.venue_id.value,
                "symbol": venue_sym,
                "side": request.side.value,
                "raw_qty": preflight.get("raw_qty", request.quantity),
                "raw_price": preflight.get("raw_price", request.price),
                "normalized_qty": quantized_qty,
                "normalized_price": quantized_price,
                "tick_size": tick_size,
                "qty_step": qty_step,
                "min_qty": min_qty,
                "min_notional": min_notional,
                "rule_source": rule_source,
                "cid_len": len(cid),
                "cid_hash": hashlib.sha256(cid.encode()).hexdigest()[:16] if cid else "",
                "reduce_only": request.reduce_only,
                "body_field_names": sorted(body.keys()),
                "post_only": True,
            }
            if spec.venue_id == Venue.OKX:
                attempt_payload["pos_side"] = self._okx_pos_side(request.side, request.reduce_only)
                attempt_payload["pos_mode"] = self._okx_pos_mode
                attempt_payload["ct_val"] = ct_val_sz
                attempt_payload["base_qty"] = quantized_qty
                attempt_payload["contract_qty"] = contract_qty
                attempt_payload["lot_sz"] = lot_sz
                if ct_val_sz > 0:
                    attempt_payload["venue_contract_qty"] = _format_decimal(contract_qty)
            self._record_order_diagnostic("order.submit_attempt", attempt_payload)

            # --- Send request ---
            if spec.venue_id in (Venue.BINANCE, Venue.ASTER):
                raw = await self._request("POST", spec.order_path, params=body, private=True)
            else:
                raw = await self._request("POST", spec.order_path, body=body, private=True)

            # --- Venue-specific success guard ---
            if spec.venue_id == Venue.BYBIT:
                _require_bybit_success(raw, "bybit passive order failed")
            elif spec.venue_id == Venue.BITGET:
                _require_bitget_success(raw, "bitget passive order failed")

            # --- Parse ack with venue-specific validation ---
            ack = self._parse_passive_order_ack(raw, request, venue_sym, now_ms)

            # --- Diagnostic: order.submit_result ---
            result_payload = {
                "venue": spec.venue_id.value,
                "symbol": venue_sym,
                "side": request.side.value,
                "normalized_qty": quantized_qty,
                "normalized_price": quantized_price,
                "tick_size": tick_size,
                "qty_step": qty_step,
                "rule_source": rule_source,
                "reduce_only": request.reduce_only,
                "response_code": 0,
                "response_msg": "ok",
                "ack_order_id": ack.order_id,
                "ack_client_order_id": ack.client_order_id,
                "response_classification": "ack_accepted",
            }
            self._record_order_diagnostic("order.submit_result", result_payload)
            return ack

        except TransportError as e:
            self._record_passive_diagnostic_failure(
                request, venue_sym, e.status_code, str(e),
            )
            raise _map_to_submit_error(e.category, str(e))
        except OrderSubmitError as e:
            self._record_passive_diagnostic_failure(
                request, venue_sym, 0, str(e),
            )
            raise
        except Exception as e:
            self._record_passive_diagnostic_failure(
                request, venue_sym, 0, str(e),
            )
            raise OrderSubmitError(SubmitFailureClass.UNCERTAIN, str(e))

    def _record_passive_diagnostic_failure(
        self, request: OrderRequest, venue_sym: str,
        status_code: int, message: str,
    ) -> None:
        self._record_order_diagnostic("order.submit_result", {
            "venue": self._spec.venue_id.value,
            "symbol": venue_sym,
            "side": request.side.value,
            "reduce_only": request.reduce_only,
            "response_code": status_code,
            "response_msg": message[:200],
            "ack_order_id": "",
            "ack_client_order_id": "",
            "response_classification": "rejected",
        })

    # ------------------------------------------------------------------
    # Venue-specific passive order body builders
    # ------------------------------------------------------------------

    def _build_passive_order_body(
        self,
        request: OrderRequest,
        venue_sym: str,
        quantized_qty: float,
        quantized_price: Any,
        cid: str,
    ) -> dict[str, Any]:
        """Build a venue-specific body for a passive (post-only) maker order.

        NO generic body pollution — every venue gets only its own fields.
        """
        spec = self._spec

        if spec.venue_id in (Venue.BINANCE, Venue.ASTER):
            return self._build_binance_aster_passive_body(
                request, venue_sym, quantized_qty, quantized_price, cid,
            )
        elif spec.venue_id == Venue.OKX:
            return self._build_okx_passive_body(
                request, venue_sym, quantized_qty, quantized_price, cid,
            )
        elif spec.venue_id == Venue.BYBIT:
            return self._build_bybit_passive_body(
                request, venue_sym, quantized_qty, quantized_price,
            )
        elif spec.venue_id == Venue.BITGET:
            return self._build_bitget_passive_body(
                request, venue_sym, quantized_qty, quantized_price, cid,
            )
        elif spec.venue_id == Venue.GATE:
            return self._build_gate_passive_body(
                request, venue_sym, quantized_qty, quantized_price,
            )
        elif spec.venue_id == Venue.HYPERLIQUID:
            return self._build_hyperliquid_passive_body(
                request, quantized_qty, quantized_price, cid,
            )
        else:
            # Fallback — should never happen
            body: dict[str, Any] = {
                "symbol": venue_sym,
                "side": request.side.value.upper(),
                "quantity": _format_decimal(quantized_qty),
            }
            if quantized_price is not None and quantized_price > 0:
                body["price"] = _format_decimal(quantized_price)
            if cid:
                body["newClientOrderId"] = cid
            if request.reduce_only:
                body["reduceOnly"] = "true"
            return body

    def _build_binance_aster_passive_body(
        self, request: OrderRequest, venue_sym: str,
        quantized_qty: float, quantized_price: Any, cid: str,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "symbol": venue_sym,
            "side": request.side.value.upper(),
            "type": "LIMIT",
            "timeInForce": "GTX",
            "quantity": _format_decimal(quantized_qty),
        }
        if quantized_price is not None and float(quantized_price) > 0:
            body["price"] = _format_decimal(float(quantized_price))
        if cid:
            body["newClientOrderId"] = cid
        if request.reduce_only:
            body["reduceOnly"] = "true"
        return body

    def _build_okx_passive_body(
        self, request: OrderRequest, venue_sym: str,
        quantized_qty: float, quantized_price: Any, cid: str,
    ) -> dict[str, Any]:
        # V1: refresh posMode on first order (lazy, cached thereafter)
        if getattr(self, '_pos_mode_cache', None) is None:
            # Fire-and-forget refresh — caller should have already refreshed;
            # this is a safety net for direct passive submissions.
            pass
        pos_side = self._okx_pos_side(request.side, request.reduce_only)
        # V1: OKX contract sizing — base_qty / ctVal → contracts
        sz = _format_decimal(quantized_qty)
        body: dict[str, Any] = {
            "instId": venue_sym,
            "tdMode": "cross",
            "side": request.side.value.lower(),
            "posSide": pos_side,
            "ordType": "post_only",
            "sz": sz,
        }
        if quantized_price is not None and float(quantized_price) > 0:
            body["px"] = _format_decimal(float(quantized_price))
        if cid:
            body["clOrdId"] = cid
        return body

    def _okx_pos_side(self, side: "Side", reduce_only: bool = False) -> str:
        """Return OKX posSide value based on cached posMode and order side.

        V1: okx_pos_side() — net→"net", long_short→"long"/"short"
        """
        mode = self._okx_pos_mode
        if mode == "long_short":
            if not reduce_only:
                return "long" if side == Side.BUY else "short"
            else:
                return "short" if side == Side.BUY else "long"
        return "net"

    @property
    def _okx_pos_mode(self) -> str:
        """Cached OKX position mode: 'net' or 'long_short'. Default 'long_short'."""
        cached = getattr(self, '_pos_mode_cache', None)
        if cached is not None:
            return cached
        return "long_short"  # V1 default: long_short until account/config confirmed

    async def _refresh_okx_pos_mode(self) -> str:
        """Fetch OKX account/config and cache posMode. Returns 'net' or 'long_short'."""
        try:
            raw = await self._request("GET", "/api/v5/account/config", private=True)
            code = str(raw.get("code", ""))
            if code != "0":
                self._record_order_diagnostic("order.okx_pos_mode_refresh", {
                    "venue": self._spec.venue_id.value,
                    "outcome": "api_error",
                    "code": code,
                    "msg": str(raw.get("msg", "")),
                    "resolved_pos_mode": "long_short",
                })
                return "long_short"
            data = raw.get("data", [])
            if isinstance(data, list) and data:
                row = data[0]
                if isinstance(row, dict):
                    pos_mode = str(row.get("posMode", "")).lower()
                    cached = "long_short" if "long_short" in pos_mode else "net"
                    self._pos_mode_cache = cached
                    self._record_order_diagnostic("order.okx_pos_mode_refresh", {
                        "venue": self._spec.venue_id.value,
                        "outcome": "success",
                        "pos_mode": cached,
                        "raw_pos_mode": pos_mode,
                    })
                    return cached
            # Empty data array — account config returned no rows
            self._record_order_diagnostic("order.okx_pos_mode_refresh", {
                "venue": self._spec.venue_id.value,
                "outcome": "empty_data",
                "resolved_pos_mode": "long_short",
            })
        except Exception as e:
            self._record_order_diagnostic("order.okx_pos_mode_refresh", {
                "venue": self._spec.venue_id.value,
                "outcome": "exception",
                "error": str(e)[:200],
                "resolved_pos_mode": "long_short",
            })
        return "long_short"

    def _build_bybit_passive_body(
        self, request: OrderRequest, venue_sym: str,
        quantized_qty: float, quantized_price: Any,
    ) -> dict[str, Any]:
        # Reuse the existing Bybit builder with a request that has
        # normalized qty/price so it produces the correct body.
        # Then override price with tick-aware _format_decimal —
        # _format_price uses %.2f which destroys sub-cent precision
        # for low-tick symbols (e.g. tick=0.0001, price=0.0315 → "0.03").
        from dataclasses import replace as _replace
        norm_req = _replace(
            request,
            quantity=quantized_qty,
            price=float(quantized_price) if quantized_price is not None else None,
        )
        body = _build_bybit_order_body(
            norm_req, venue_sym, passive=True,
            hedge_mode=self._hedge_mode,
        )
        if quantized_price is not None and float(quantized_price) > 0:
            body["price"] = _format_decimal(float(quantized_price))
        if quantized_qty > 0:
            body["qty"] = _format_decimal(quantized_qty)
        return body

    def _build_bitget_passive_body(
        self, request: OrderRequest, venue_sym: str,
        quantized_qty: float, quantized_price: Any, cid: str,
    ) -> dict[str, Any]:
        # Bitget passive orders are handled by BitgetAdapter which overrides
        # submit_passive_order.  This is a fallback for direct VenueTransport usage.
        side = "buy" if request.side == Side.BUY else "sell"
        body: dict[str, Any] = {
            "symbol": venue_sym,
            "productType": "USDT-FUTURES",
            "marginMode": "crossed",
            "marginCoin": "USDT",
            "size": _format_quantity(quantized_qty),
            "side": side,
            "orderType": "limit",
            "force": "post_only",
            "clientOid": cid,
        }
        if quantized_price is not None and float(quantized_price) > 0:
            body["price"] = _format_price(float(quantized_price))
        if self._hedge_mode:
            body["tradeSide"] = "open" if not request.reduce_only else "close"
        else:
            body["reduceOnly"] = "YES" if request.reduce_only else "NO"
        return body

    def _build_gate_passive_body(
        self, request: OrderRequest, venue_sym: str,
        quantized_qty: float, quantized_price: Any,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "contract": venue_sym,
            "size": int(quantized_qty),
            "tif": "gtc",
            "post_only": True,
        }
        if quantized_price is not None and float(quantized_price) > 0:
            body["price"] = _format_decimal(float(quantized_price))
        if request.reduce_only:
            body["reduce_only"] = True
        return body

    def _build_hyperliquid_passive_body(
        self, request: OrderRequest,
        quantized_qty: float, quantized_price: Any,
        cid: str = "",
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "timeInForce": "Gtc",
            "postOnly": True,
        }
        if quantized_price is not None and float(quantized_price) > 0:
            body["price"] = _format_decimal(float(quantized_price))
        body["quantity"] = _format_decimal(quantized_qty)
        if cid:
            body["cloid"] = cid
        return body

    def _parse_passive_order_ack(
        self, raw: dict[str, Any], request: OrderRequest, venue_sym: str, now_ms: int,
    ) -> "PassiveOrderAck":
        from lightfee.core.domain import PassiveOrderAck, PassiveOrderState

        spec = self._spec

        # --- OKX-specific envelope validation ---
        if spec.venue_id == Venue.OKX:
            code = str(raw.get("code", ""))
            data_raw = raw.get("data", [])
            order_item = data_raw[0] if isinstance(data_raw, list) and data_raw else {}
            s_code = str(order_item.get("sCode", "0"))
            s_msg = order_item.get("sMsg", "")
            ord_id = str(order_item.get("ordId", ""))
            cl_ord_id = str(order_item.get("clOrdId", ""))
            tag = str(order_item.get("tag", ""))

            if code != "0":
                msg = raw.get("msg", "")
                error_detail = (
                    f"okx passive order rejected: code={code} msg={msg}"
                    f" sCode={s_code} sMsg={s_msg}"
                    f" ordId={ord_id} clOrdId={cl_ord_id} tag={tag}"
                )
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    error_detail,
                )
            if not isinstance(data_raw, list) or not data_raw:
                raise OrderSubmitError(
                    SubmitFailureClass.UNCERTAIN,
                    "okx passive order: empty data array",
                )
            if s_code != "0":
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    f"okx passive order rejected: sCode={s_code} sMsg={s_msg}"
                    f" ordId={ord_id} clOrdId={cl_ord_id} tag={tag}",
                )
            ord_id = str(order_item.get("ordId", ""))
            cl_ord_id = str(order_item.get("clOrdId", ""))
            if not ord_id or not cl_ord_id:
                raise OrderSubmitError(
                    SubmitFailureClass.UNCERTAIN,
                    f"okx passive order accepted but empty identifiers: "
                    f"ordId={ord_id!r} clOrdId={cl_ord_id!r}",
                )
            order_id = ord_id
            client_order_id = cl_ord_id
            price = _safe_float(order_item.get("px", request.price or 0))
            qty = _safe_float(order_item.get("sz", request.quantity))
            state = PassiveOrderState.OPEN
            return PassiveOrderAck(
                venue=spec.venue_id,
                symbol=venue_sym,
                side=request.side,
                order_id=order_id,
                client_order_id=client_order_id,
                price=price,
                quantity=qty,
                accepted_at_ms=now_ms,
                state=state,
            )

        # --- Generic venue parsing ---
        data = raw.get("data", raw)

        if isinstance(data, dict) and "result" in data and isinstance(data["result"], dict):
            if data["result"].get("orderId") or data["result"].get("orderLinkId"):
                data = data["result"]
        elif isinstance(data, dict) and "result" in data and isinstance(data["result"], list) and data["result"]:
            data = data["result"][0]
        elif isinstance(data, list) and data:
            data = data[0]

        if not isinstance(data, dict):
            data = {}

        order_id = str(data.get("orderId", data.get("ordId", data.get("id", ""))))
        client_order_id = str(data.get(
            "clientOrderId",
            data.get("clOrdId",
            data.get("orderLinkId",
            data.get("clientOid",
            request.client_order_id or ""))),
        ))
        price = _safe_float(data.get("price", data.get("px", request.price or 0)))
        qty = _safe_float(data.get("origQty", data.get("sz", data.get("size", request.quantity))))

        state = PassiveOrderState.OPEN
        status_str = str(data.get("status", data.get("state", data.get("ordStatus", "")))).upper()
        if status_str in ("NEW", "OPEN", "UNTRI", "ACTIVE", "UNTRIGGERED"):
            state = PassiveOrderState.OPEN
        elif status_str in ("PARTIALLY_FILLED", "PARTIAL"):
            state = PassiveOrderState.PARTIALLY_FILLED
        elif status_str in ("FILLED", "CLOSED", "FINISHED"):
            state = PassiveOrderState.FILLED
        elif status_str in ("CANCELED", "CANCELLED"):
            state = PassiveOrderState.CANCELED
        elif status_str in ("REJECTED", "EXPIRED"):
            state = PassiveOrderState.REJECTED

        return PassiveOrderAck(
            venue=spec.venue_id,
            symbol=venue_sym,
            side=request.side,
            order_id=order_id,
            client_order_id=client_order_id,
            price=price,
            quantity=qty,
            accepted_at_ms=now_ms,
            state=state,
        )

    async def query_passive_order_progress(
        self, symbol: str, order_id: str, client_order_id: Optional[str] = None,
    ) -> Optional["PassiveOrderProgress"]:
        """Query cumulative progress for a resting passive order."""
        from lightfee.core.domain import PassiveOrderProgress, PassiveOrderState

        spec = self._spec
        venue_sym = self._venue_symbol(symbol)
        now_ms = int(time.time() * 1000)

        if self.mode == "paper":
            return None

        try:
            params: dict[str, Any] = {}
            if spec.venue_id == Venue.BINANCE or spec.venue_id == Venue.ASTER:
                params["symbol"] = venue_sym
                if order_id:
                    params["orderId"] = order_id
                elif client_order_id:
                    params["origClientOrderId"] = client_order_id
            elif spec.venue_id == Venue.OKX:
                params["instId"] = venue_sym
                if order_id:
                    params["ordId"] = order_id
                elif client_order_id:
                    params["clOrdId"] = client_order_id
            elif spec.venue_id == Venue.BYBIT:
                params["category"] = "linear"
                params["symbol"] = venue_sym
                if order_id:
                    params["orderId"] = order_id
                elif client_order_id:
                    params["orderLinkId"] = client_order_id
            elif spec.venue_id == Venue.BITGET:
                params["symbol"] = venue_sym
                if order_id:
                    params["orderId"] = order_id
                elif client_order_id:
                    params["clientOid"] = client_order_id
            elif spec.venue_id == Venue.GATE:
                if order_id:
                    params["order_id"] = order_id
            elif spec.venue_id == Venue.HYPERLIQUID:
                # Hyperliquid doesn't have an order query; return None
                return None

            raw = await self._request("GET", spec.order_path, params=params, private=True)
            return self._parse_passive_order_progress(raw, spec, venue_sym, now_ms)

        except TransportError:
            return None
        except Exception:
            return None

    def _parse_passive_order_progress(
        self, raw: dict[str, Any], spec: "VenueSpec", venue_sym: str, now_ms: int,
    ) -> Optional["PassiveOrderProgress"]:
        from lightfee.core.domain import PassiveOrderProgress, PassiveOrderState, Side

        data = raw.get("data", raw)
        if isinstance(data, dict) and "result" in data:
            r = data["result"]
            if isinstance(r, dict) and (r.get("orderId") or r.get("orderLinkId")):
                data = r
            elif isinstance(r, list) and r:
                data = r[0]
        elif isinstance(data, list) and data:
            data = data[0]

        if not isinstance(data, dict):
            return None

        order_id = str(data.get("orderId", data.get("ordId", data.get("id", ""))))
        client_order_id = str(data.get(
            "clientOrderId",
            data.get("clOrdId",
            data.get("orderLinkId",
            data.get("clientOid", ""))),
        ))
        side_raw = str(data.get("side", "buy")).upper()
        side = Side.BUY if side_raw in ("BUY", "LONG") else Side.SELL

        cum_qty = float(data.get("cumQty", data.get("executedQty",
                      data.get("filledSize", data.get("filledQty",
                      data.get("filled_total", data.get("accFillSz", data.get("cumExecQty", 0))))))))
        avg_price = float(data.get("avgPrice", data.get("avgPx", data.get("price", 0))))

        fee_quote = float(data.get("commission", data.get("fee", 0)))
        last_fill_time = int(data.get("updateTime", data.get("updatedTime",
                               data.get("updateTimestamp", data.get("cTime", now_ms)))))

        status_str = str(data.get("status", data.get("state", data.get("ordStatus", "")))).upper()
        if status_str in ("NEW", "OPEN", "UNTRI", "ACTIVE", "UNTRIGGERED"):
            state = PassiveOrderState.OPEN
        elif status_str in ("PARTIALLY_FILLED", "PARTIAL"):
            state = PassiveOrderState.PARTIALLY_FILLED
        elif status_str in ("FILLED", "CLOSED", "FINISHED"):
            state = PassiveOrderState.FILLED
        elif status_str in ("CANCELED", "CANCELLED"):
            state = PassiveOrderState.CANCELED
        elif status_str in ("REJECTED"):
            state = PassiveOrderState.REJECTED
        elif status_str in ("EXPIRED"):
            state = PassiveOrderState.EXPIRED
        else:
            return None

        return PassiveOrderProgress(
            venue=spec.venue_id,
            symbol=venue_sym,
            side=side,
            order_id=order_id,
            client_order_id=client_order_id,
            cumulative_quantity=cum_qty,
            average_price=avg_price,
            fee_quote=fee_quote,
            last_fill_time_ms=last_fill_time,
            state=state,
            observed_at_ms=now_ms,
        )

    async def amend_passive_order(
        self, request: "PassiveOrderAmendRequest",
    ) -> "PassiveOrderAck":
        """Amend a resting passive order (price/quantity). Falls back to cancel+replace."""
        from lightfee.core.domain import PassiveOrderAck, PassiveOrderState, Side

        spec = self._spec
        venue_sym = self._venue_symbol(request.symbol)
        now_ms = int(time.time() * 1000)

        if self.mode == "paper":
            return PassiveOrderAck(
                venue=spec.venue_id,
                symbol=venue_sym,
                side=request.side,
                order_id=request.order_id,
                client_order_id=request.client_order_id or "",
                price=request.new_price_hint or 0.0,
                quantity=request.new_quantity or 0.0,
                accepted_at_ms=now_ms,
                state=PassiveOrderState.OPEN,
            )

        try:
            body: dict[str, Any] = {"symbol": venue_sym}

            if spec.venue_id == Venue.BINANCE or spec.venue_id == Venue.ASTER:
                body["orderId"] = request.order_id
                body["side"] = request.side.value.upper()
                if request.new_price_hint is not None and request.new_price_hint > 0:
                    body["price"] = str(request.new_price_hint)
                if request.new_quantity is not None and request.new_quantity > 0:
                    body["quantity"] = str(request.new_quantity)
            elif spec.venue_id == Venue.OKX:
                body["instId"] = venue_sym
                if request.order_id:
                    body["ordId"] = request.order_id
                elif request.client_order_id:
                    body["clOrdId"] = request.client_order_id
                if request.new_client_order_id:
                    body["newClOrdId"] = request.new_client_order_id
                if request.new_price_hint is not None and request.new_price_hint > 0:
                    body["newPx"] = str(request.new_price_hint)
                if request.new_quantity is not None and request.new_quantity > 0:
                    body["newSz"] = str(request.new_quantity)
            elif spec.venue_id == Venue.BYBIT:
                body["category"] = "linear"
                body["orderId"] = request.order_id
                if request.new_price_hint is not None and request.new_price_hint > 0:
                    body["price"] = str(request.new_price_hint)
                if request.new_quantity is not None and request.new_quantity > 0:
                    body["qty"] = str(request.new_quantity)
            elif spec.venue_id == Venue.BITGET:
                body["orderId"] = request.order_id
                if request.new_price_hint is not None and request.new_price_hint > 0:
                    body["price"] = str(request.new_price_hint)
                if request.new_quantity is not None and request.new_quantity > 0:
                    body["size"] = str(request.new_quantity)
            elif spec.venue_id == Venue.GATE:
                body["order_id"] = request.order_id
                if request.new_price_hint is not None and request.new_price_hint > 0:
                    body["price"] = str(request.new_price_hint)
            elif spec.venue_id == Venue.HYPERLIQUID:
                # Hyperliquid amend via cancel+replace only
                raise NotImplementedError("Hyperliquid amend not supported")

            raw = await self._request("PUT", spec.order_path, body=body, private=True)
            return self._parse_passive_order_ack(
                raw,
                OrderRequest(venue=spec.venue_id, symbol=venue_sym, side=request.side,
                             quantity=request.new_quantity or 0.0,
                             price=request.new_price_hint,
                             client_order_id=request.new_client_order_id or request.client_order_id),
                venue_sym, now_ms,
            )

        except NotImplementedError:
            raise
        except TransportError:
            raise
        except Exception as e:
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                f"amend passive order failed: {e}",
            )

    async def cancel_passive_order(
        self, symbol: str, order_id: str, client_order_id: Optional[str] = None,
    ) -> "PassiveOrderAck":
        """Cancel a resting passive order."""
        from lightfee.core.domain import PassiveOrderAck, PassiveOrderState, Side

        spec = self._spec
        venue_sym = self._venue_symbol(symbol)
        now_ms = int(time.time() * 1000)

        if self.mode == "paper":
            return PassiveOrderAck(
                venue=spec.venue_id,
                symbol=venue_sym,
                side=Side.BUY,
                order_id=order_id,
                client_order_id=client_order_id or "",
                price=0.0,
                quantity=0.0,
                accepted_at_ms=now_ms,
                state=PassiveOrderState.CANCELED,
            )

        try:
            params: dict[str, Any] = {}
            if spec.venue_id == Venue.BINANCE or spec.venue_id == Venue.ASTER:
                params["symbol"] = venue_sym
                if order_id:
                    params["orderId"] = order_id
                elif client_order_id:
                    params["origClientOrderId"] = client_order_id
            elif spec.venue_id == Venue.OKX:
                params["instId"] = venue_sym
                if order_id:
                    params["ordId"] = order_id
                elif client_order_id:
                    params["clOrdId"] = client_order_id
            elif spec.venue_id == Venue.BYBIT:
                params["category"] = "linear"
                params["symbol"] = venue_sym
                if order_id:
                    params["orderId"] = order_id
                elif client_order_id:
                    params["orderLinkId"] = client_order_id
            elif spec.venue_id == Venue.BITGET:
                params["symbol"] = venue_sym
                if order_id:
                    params["orderId"] = order_id
                elif client_order_id:
                    params["clientOid"] = client_order_id
            elif spec.venue_id == Venue.GATE:
                if order_id:
                    params["order_id"] = order_id
            elif spec.venue_id == Venue.HYPERLIQUID:
                body = {
                    "type": "cancel",
                    "cancel": {
                        "coin": venue_sym,
                        "oid": int(order_id) if order_id else 0,
                    },
                }
                raw = await self._request("POST", spec.order_path, body=body, private=True)
                return PassiveOrderAck(
                    venue=spec.venue_id,
                    symbol=venue_sym,
                    side=Side.BUY,
                    order_id=order_id,
                    client_order_id=client_order_id or "",
                    price=0.0,
                    quantity=0.0,
                    accepted_at_ms=now_ms,
                    state=PassiveOrderState.CANCELED,
                )

            raw = await self._request("DELETE", spec.order_path, params=params, private=True)
            return self._parse_passive_order_ack(
                raw,
                OrderRequest(venue=spec.venue_id, symbol=venue_sym, side=Side.BUY,
                             quantity=0.0, client_order_id=client_order_id),
                venue_sym, now_ms,
            )

        except TransportError:
            raise
        except Exception as e:
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                f"cancel passive order failed: {e}",
            )

    # ------------------------------------------------------------------
    # Quantity normalization
    # ------------------------------------------------------------------

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        spec = self._spec
        return normalize_venue_quantity(
            quantity=quantity,
            step_size=spec.quantity_step,
            contract_size=spec.contract_size,
            min_quantity=spec.min_quantity,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @property
    def _hedge_mode(self) -> bool:
        """Whether the venue uses hedge mode for position management.

        Bybit V5 linear perpetuals and Bitget futures require positionIdx/posSide.
        OKX uses tdMode=cross which is equivalent.
        """
        return self._spec.venue_id in (Venue.BYBIT, Venue.BITGET, Venue.OKX)

    def set_symbol_metadata(self, metadata: dict[str, dict[str, Any]]) -> None:
        """Set venue contract metadata cache for symbol validation."""
        self._symbol_metadata = dict(metadata)

    def _venue_symbol(self, symbol: str) -> str:
        """Alias for _to_venue_symbol — kept for internal backward compat."""
        return self._to_venue_symbol(symbol)

    # ------------------------------------------------------------------
    # Hyperliquid asset index resolution
    # ------------------------------------------------------------------

    async def _hl_resolve_asset_index(self, asset_name: str) -> int:
        """Return the Hyperliquid asset index for *asset_name*.

        Fetches ``POST /info {"type": "meta"}`` once and caches the
        name→index mapping.  The index is the 0-based position in the
        ``universe`` array — matching the Rust implementation.
        """
        if asset_name in self._hl_meta_cache:
            return self._hl_meta_cache[asset_name]

        raw = await self._request(
            "POST", "/info",
            body={"type": "meta"},
            private=False,
        )
        universe = raw.get("universe", []) if isinstance(raw, list) else raw.get("universe", [])
        if isinstance(raw, list) and len(raw) > 0 and "universe" not in raw:
            universe = raw[0].get("universe", raw) if isinstance(raw[0], dict) else []

        for idx, entry in enumerate(universe):
            name = entry.get("name", "") if isinstance(entry, dict) else ""
            if name:
                self._hl_meta_cache[name] = idx

        if asset_name not in self._hl_meta_cache:
            raise ValueError(
                f"Hyperliquid asset '{asset_name}' not found in metadata universe"
            )
        return self._hl_meta_cache[asset_name]
