"""Shared venue transport: HTTP client, auth signing, error classification."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import datetime
import random
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
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
from lightfee.venues.specs import AuthScheme, VenueSpec


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


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------


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


def _normalize_symbol(sym: str) -> str:
    """Normalize a symbol string for comparison."""
    return str(sym or "").strip().upper()


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------


class VenueTransport:
    """Shared async transport that owns HTTP lifecycle, auth, and error mapping."""

    def __init__(
        self,
        spec: VenueSpec,
        mode: str = "paper",
        credential: Optional[LiveCredential] = None,
        exchange_http_timeout_ms: int = 10000,
        rate_limiter: Optional[EndpointRateLimiter] = None,
    ) -> None:
        self._spec = spec
        self.mode = mode
        self._credential = credential
        self._exchange_http_timeout_ms = exchange_http_timeout_ms
        self._rate_limiter = rate_limiter
        self._client: Optional[httpx.AsyncClient] = None
        self._hl_meta_cache: dict[str, int] = {}
        self._symbol_metadata: dict[str, dict[str, Any]] = {}  # sym → vendor contract info

        if mode == "live":
            self._validate_live_credentials(credential)

    @property
    def venue(self) -> Venue:
        return self._spec.venue_id

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            timeout_s = self._exchange_http_timeout_ms / 1000.0
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(timeout_s),
                limits=httpx.Limits(max_keepalive_connections=4),
            )
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

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

    # ------------------------------------------------------------------
    # Auth header construction
    # ------------------------------------------------------------------

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

        # Public endpoints must not include auth headers (V1 parity)
        if not private or cred is None:
            return {}

        headers: dict[str, str] = {}

        # --- EIP712 (Hyperliquid) ---
        if spec.auth_scheme == AuthScheme.EIP712:
            headers["Content-Type"] = "application/json"
            return headers

        # --- Binance / Aster: query-string signature ---
        if spec.signature_param:
            ts = str(int(time.time() * 1000))
            payload = query_string.lstrip("?") if query_string else ""
            if spec.timestamp_param and spec.timestamp_param not in (payload or ""):
                ts_param = f"{spec.timestamp_param}={ts}"
                payload = f"{ts_param}&{payload}" if payload else ts_param
            headers[spec.api_key_header] = cred.api_key
            sig = _sign_payload(spec.auth_scheme, cred.api_secret, payload)
            # signature is added to query params in _build_signed_request
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
                ts = _iso8601_now()
            else:
                ts = str(int(time.time() * 1000))

            headers[spec.api_key_header] = cred.api_key
            headers[spec.timestamp_header] = ts

            if spec.requires_passphrase and spec.passphrase_header:
                headers[spec.passphrase_header] = cred.api_passphrase

            if spec.recv_window_header:
                headers[spec.recv_window_header] = "5000"

            # OKX style: ts + method + path (+ query_string for GET/DELETE, body for POST)
            if spec.use_iso8601_timestamp:
                if query_string and method.upper() in ("GET", "DELETE"):
                    sign_payload = ts + method.upper() + path + query_string
                else:
                    sign_payload = ts + method.upper() + path + (body or "")
            else:
                # Bybit V5 style: ts + api_key + recv_window
                #   GET/DELETE: + query_string_without_question_mark
                #   POST:       + JSON body
                recv = headers.get(spec.recv_window_header, "5000")
                if query_string and method.upper() in ("GET", "DELETE"):
                    sign_payload = ts + cred.api_key + recv + query_string.lstrip("?")
                else:
                    sign_payload = ts + cred.api_key + recv + (body or "")

            sig = _sign_payload(spec.auth_scheme, cred.api_secret, sign_payload)
            headers[spec.signature_header] = sig

        return headers

    def _build_signed_request(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
        body: Optional[dict[str, Any]] = None,
        private: bool = False,
    ) -> tuple[str, dict[str, str], Optional[str]]:
        spec = self._spec
        query_string = ""
        req_body: Optional[str] = None

        # Binance / Aster: only sign private requests. V1 sends public depth
        # requests without auth headers — adding them causes unnecessary
        # server-side verification overhead.
        if spec.signature_param and private and self._credential:
            qp: dict[str, Any] = dict(params) if params else {}
            ts = str(int(time.time() * 1000))
            if spec.timestamp_param:
                qp[spec.timestamp_param] = ts
            encoded = "&".join(f"{k}={v}" for k, v in sorted(qp.items()))
            sig = _sign_payload(spec.auth_scheme, self._credential.api_secret, encoded)
            qp[spec.signature_param] = sig
            query_string = "?" + "&".join(
                f"{k}={v}" for k, v in sorted(qp.items())
            )
        elif spec.signature_param and not private:
            # Public request — no signature, just params
            if params:
                qp = dict(params)
                if spec.timestamp_param and spec.timestamp_param not in qp:
                    qp[spec.timestamp_param] = str(int(time.time() * 1000))
                query_string = "?" + "&".join(
                    f"{k}={v}" for k, v in sorted(qp.items())
                )
        elif params:
            query_parts = []
            for k, v in sorted(params.items()):
                query_parts.append(f"{k}={v}")
            if query_parts:
                query_string = "?" + "&".join(query_parts)

        if body is not None:
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
    ) -> dict[str, Any]:
        client = await self._get_client()
        base_url = (
            self._spec.private_base_url if private
            else self._spec.public_base_url
        )
        qs, headers, req_body = self._build_signed_request(method, path, params, body, private=private)
        url = base_url + path + qs

        # V1-aligned rate limiting: wait_until_ready + pace before every request
        if self._rate_limiter is not None:
            rate_limit_scope = f"host:{base_url}"
            await self._rate_limiter.wait_until_ready_for_scopes([rate_limit_scope])
            await self._rate_limiter.pace_for_scopes([rate_limit_scope])

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
                # V1: record rate-limit for 429/418 to trigger cooldown
                if resp.status_code in (429, 418) and self._rate_limiter is not None:
                    rate_limit_scope = f"host:{base_url}"
                    retry_after_ms = _parse_retry_after_ms(resp.headers)
                    self._rate_limiter.record_rate_limit_for_scopes(
                        [rate_limit_scope], retry_after_ms=retry_after_ms,
                    )
                cat = classify_transport_error(resp.status_code, resp.text)
                if cat:
                    raise TransportError(
                        cat, f"HTTP {resp.status_code}: {resp.text[:200]}",
                        status_code=resp.status_code, body=resp.text,
                        headers=dict(resp.headers),
                    )

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
            body: dict[str, Any] = {
                "symbol": venue_sym,
                "side": request.side.value.upper(),
                "quantity": str(request.quantity),
            }

            if spec.venue_id == Venue.BINANCE or spec.venue_id == Venue.ASTER:
                body["type"] = "MARKET"
                if request.reduce_only:
                    body["reduceOnly"] = "true"
                if request.price is not None:
                    body["type"] = "LIMIT"
                    body["price"] = str(request.price)
                    body["timeInForce"] = "GTC"
            elif spec.venue_id == Venue.OKX:
                body["instId"] = venue_sym
                body["tdMode"] = "cross"
                body["ordType"] = "market"
                if request.reduce_only:
                    body["reduceOnly"] = "true"
            elif spec.venue_id == Venue.BYBIT:
                body = _build_bybit_order_body(
                    request, venue_sym, passive=False,
                    hedge_mode=self._hedge_mode,
                )
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
                    body["price"] = str(request.price)
            elif spec.venue_id == Venue.HYPERLIQUID:
                is_buy = request.side == Side.BUY
                tif = "Gtc" if request.price else "Ioc"
                limit_px = str(request.price) if request.price else "0"

                if self.mode == "live":
                    from lightfee.venues.hyperliquid_signing import (
                        build_hyperliquid_exchange_payload,
                        build_hyperliquid_order_action,
                    )

                    asset_index = await self._hl_resolve_asset_index(venue_sym)
                    action = build_hyperliquid_order_action(
                        symbol=venue_sym,
                        is_buy=is_buy,
                        quantity=request.quantity,
                        price=float(limit_px),
                        reduce_only=request.reduce_only,
                        tif=tif,
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
                    )
                else:
                    body = {
                        "action": {
                            "type": "order",
                            "orders": [{
                                "a": 0,  # asset index — placeholder for paper
                                "b": is_buy,
                                "p": limit_px,
                                "s": str(request.quantity),
                                "r": request.reduce_only,
                                "t": {"limit": {"tif": tif}},
                            }],
                            "grouping": "na",
                        },
                        "type": "order",
                    }
                    if self._credential and self._credential.account_address:
                        body["vaultAddress"] = self._credential.account_address

            raw = await self._request("POST", spec.order_path, body=body, private=True)

            # V2: venue-specific success guard before parsing
            if spec.venue_id == Venue.BYBIT:
                _require_bybit_success(raw, "bybit order failed")
            elif spec.venue_id == Venue.BITGET:
                _require_bitget_success(raw, "bitget order failed")

            return self._parse_order_fill(raw, request, venue_sym, now_ms)

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

        except TransportError:
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

        # Step 3: aggregate executions
        return self._parse_bybit_execution_list(
            exec_raw, venue_sym, resolved_order_id, resolved_client_id, now_ms,
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
        side_str = str(data.get("side", "Buy"))
        side = Side.BUY if side_str == "Buy" else Side.SELL
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
        """
        result = raw.get("result", {})
        executions = result.get("list", []) if isinstance(result, dict) else []
        if not executions:
            return None

        total_qty = 0.0
        weighted_notional = 0.0
        total_fee = 0.0
        latest_fill_ms = 0
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

        if total_qty <= 0.0:
            return None

        return OrderFillReconciliation(
            venue=Venue.BYBIT,
            symbol=venue_sym,
            side=Side.BUY,
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
        # V1 multi-key fallback: baseVolume, filledQty, fillQty, size
        cum_qty = _safe_float(
            data.get("baseVolume", data.get("filledQty", data.get("fillSz", "0")))
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
            if isinstance(fd, dict):
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
        """Submit a GTC post-only reduce-only maker order. Returns ack, not fill."""
        from lightfee.core.domain import PassiveOrderAck, PassiveOrderState

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
            body: dict[str, Any] = {
                "symbol": venue_sym,
                "side": request.side.value.upper(),
                "quantity": str(request.quantity),
                "reduceOnly": "true",
            }

            if request.client_order_id:
                body["newClientOrderId"] = request.client_order_id

            if spec.venue_id == Venue.BINANCE or spec.venue_id == Venue.ASTER:
                body["type"] = "LIMIT"
                body["timeInForce"] = "GTX"  # post-only
                if request.price is not None and request.price > 0:
                    body["price"] = str(request.price)
            elif spec.venue_id == Venue.OKX:
                body["instId"] = venue_sym
                body["tdMode"] = "cross"
                body["ordType"] = "post_only"
                if request.price is not None and request.price > 0:
                    body["px"] = str(request.price)
            elif spec.venue_id == Venue.BYBIT:
                body = _build_bybit_order_body(
                    request, venue_sym, passive=True,
                    hedge_mode=self._hedge_mode,
                )
            elif spec.venue_id == Venue.BITGET:
                body["marginCoin"] = "USDT"
                body["orderType"] = "limit"
                body["timeInForceValue"] = "post_only"
                if request.price is not None and request.price > 0:
                    body["price"] = str(request.price)
            elif spec.venue_id == Venue.GATE:
                body["contract"] = venue_sym
                body["size"] = int(request.quantity)
                body["price"] = str(request.price) if request.price else "0"
                body["tif"] = "gtc"
                body["reduce_only"] = True
                body["post_only"] = True
            elif spec.venue_id == Venue.HYPERLIQUID:
                if request.price is not None and request.price > 0:
                    body["price"] = str(request.price)
                body["timeInForce"] = "Gtc"
                body["postOnly"] = True

            raw = await self._request("POST", spec.order_path, body=body, private=True)

            # V2: venue-specific success guard before parsing
            if spec.venue_id == Venue.BYBIT:
                _require_bybit_success(raw, "bybit passive order failed")
            elif spec.venue_id == Venue.BITGET:
                _require_bitget_success(raw, "bitget passive order failed")

            return self._parse_passive_order_ack(raw, request, venue_sym, now_ms)

        except TransportError as e:
            raise _map_to_submit_error(e.category, str(e))
        except OrderSubmitError:
            raise
        except Exception as e:
            raise OrderSubmitError(SubmitFailureClass.UNCERTAIN, str(e))

    def _parse_passive_order_ack(
        self, raw: dict[str, Any], request: OrderRequest, venue_sym: str, now_ms: int,
    ) -> "PassiveOrderAck":
        from lightfee.core.domain import PassiveOrderAck, PassiveOrderState

        spec = self._spec
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
        spec = self._spec
        if spec.symbol_to_venue is not None:
            return spec.symbol_to_venue(symbol)
        return symbol

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
