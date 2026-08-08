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
from decimal import Decimal, ROUND_FLOOR
from enum import Enum
from typing import Any, Optional

import httpx

from lightfee.core.domain import (
    AccountBalanceSnapshot,
    OrderFill,
    OrderFillReconciliation,
    OrderRequest,
    PositionSnapshot,
    Side,
    TimeInForce,
    Venue,
    VenueMarketQuote,
    VenueMarketSnapshot,
)
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.marketdata.private_ws import (
    CumulativeOrderProgress,
    PrivateOrderUpdate,
    PrivateWsState,
    enrich_fill_from_private,
    lookup_or_wait_private_order,
    lookup_or_wait_private_order_progress,
    merge_passive_progress_sources,
)
from lightfee.marketdata.resilience import ConnectionHealth
from lightfee.venues.common import normalize_venue_quantity
from lightfee.venues.market_data import MarketDataClient
from lightfee.venues.specs import (
    AuthScheme,
    BitgetContractFamily,
    VenueOperation,
    VenueOperationContract,
    VenueSpec,
    get_operation_contract,
    get_spec,
)
from lightfee.venues.symbol_rules import get_symbol_rules_cache

import logging


# httpx/httpcore emit full request URLs (including signed query params) at INFO.
# The root logger is configured to INFO in the app entrypoints, so without an
# explicit override every private Aster request URL — signature, signer, nonce,
# user — would be written to journald.  Force these libraries to WARNING so
# they only surface genuine failures, never signed query strings.
def configure_http_client_logging() -> None:
    for name in ("httpx", "httpcore", "httpcore.http11", "httpcore.http2"):
        logger = logging.getLogger(name)
        if logger.level == logging.NOTSET or logger.level < logging.WARNING:
            logger.setLevel(logging.WARNING)


configure_http_client_logging()


ASTER_DEFAULT_REMAINING_OPENABLE_LEVERAGE = 4
OKX_POSITION_REST_CACHE_MAX_AGE_MS = 30_000
OKX_CANCEL_ORDER_PATH = "/api/v5/trade/cancel-order"
OKX_PUBLIC_INSTRUMENTS_PATH = "/api/v5/public/instruments"
OKX_ACCOUNT_INSTRUMENTS_PATH = "/api/v5/account/instruments"


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
    wallet_mode: str = "account_wallet"


def _missing_hyperliquid_signing_dependencies() -> list[str]:
    import importlib.util

    required = [
        ("Crypto.Hash", "pycryptodome"),
        ("eth_account", "eth-account"),
        ("msgpack", "msgpack"),
    ]
    missing: list[str] = []
    for module_name, package_name in required:
        try:
            available = importlib.util.find_spec(module_name) is not None
        except ModuleNotFoundError:
            available = False
        if not available:
            missing.append(package_name)
    return missing


def _derive_hyperliquid_account_address(wallet_private_key: str) -> str:
    """V1 Hyperliquid parity: account defaults to the wallet address."""
    missing = _missing_hyperliquid_signing_dependencies()
    if missing:
        return ""
    try:
        from eth_account import Account

        return str(Account.from_key(wallet_private_key).address)
    except Exception as exc:
        raise ValueError(
            "failed to derive hyperliquid account_address from wallet_private_key"
        ) from exc


def _normalize_hyperliquid_wallet_mode(wallet_mode: str) -> str:
    mode = str(wallet_mode or "account_wallet").strip().lower().replace("-", "_")
    if mode in ("account", "account_wallet", "wallet"):
        return "account_wallet"
    if mode in ("api", "api_wallet", "agent", "agent_wallet"):
        return "api_wallet"
    return mode


def _normalize_hyperliquid_credential(credential: LiveCredential) -> LiveCredential:
    wallet_mode = _normalize_hyperliquid_wallet_mode(credential.wallet_mode)
    if wallet_mode != credential.wallet_mode:
        credential = replace(credential, wallet_mode=wallet_mode)
    if wallet_mode == "api_wallet":
        return credential
    if credential.account_address or not credential.wallet_private_key:
        return credential
    return replace(
        credential,
        account_address=_derive_hyperliquid_account_address(
            credential.wallet_private_key
        ),
    )


def _floor_to_step(value: float, step: float) -> float:
    if step <= 0 or not math.isfinite(value) or not math.isfinite(step):
        return value
    value_dec = Decimal(str(value))
    step_dec = Decimal(str(step))
    steps = (value_dec / step_dec).to_integral_value(rounding=ROUND_FLOOR)
    return float(steps * step_dec)


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


def _okx_contract_order_diagnostics(
    *,
    base_qty: float,
    ct_val: float,
    lot_sz: float,
    min_sz: float,
    max_mkt_sz: float = 0.0,
) -> dict[str, Any]:
    """Return OKX base→contract sizing evidence and local reject reason.

    The engine boundary is canonical base quantity. OKX derivative wire `sz`
    is contract count, and OKX `lotSz`/`minSz` are contract units.
    """
    payload = {
        "base_qty": float(base_qty),
        "ct_val": float(ct_val or 0.0),
        "lot_sz": float(lot_sz or 0.0),
        "min_sz": float(min_sz or 0.0),
        "max_mkt_sz": float(max_mkt_sz or 0.0),
        "quantity_units": "base_to_contracts",
    }

    if ct_val <= 0 or not math.isfinite(ct_val):
        payload["contract_qty"] = 0.0
        payload["reject_reason"] = "missing_ct_val"
        return payload

    if lot_sz > 0 and math.isfinite(lot_sz):
        contract_qty = _floor_to_step(base_qty / ct_val, lot_sz)
    else:
        contract_qty = base_qty / ct_val
    if not math.isfinite(contract_qty):
        contract_qty = 0.0

    payload["contract_qty"] = float(contract_qty)
    if contract_qty <= 0:
        payload["reject_reason"] = "contract_qty_zero"
    elif min_sz > 0 and contract_qty + 1e-12 < min_sz:
        payload["reject_reason"] = "contract_qty_below_min_sz"
    elif max_mkt_sz > 0 and contract_qty - 1e-12 > max_mkt_sz:
        payload["reject_reason"] = "contract_qty_above_max_mkt_sz"
    return payload


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


async def _resolve_bitget_contract_family_for_truth(transport: Any) -> BitgetContractFamily:
    resolver = getattr(transport, "_bitget_resolve_contract_family", None)
    if callable(resolver):
        return await resolver()
    if getattr(transport, "mode", "") != "live":
        return BitgetContractFamily.UTA_V3
    raise TransportError(
        TransportErrorCategory.REQUEST_REJECTED,
        "Bitget private truth requires an explicit account family resolver",
        status_code=400,
        body='{"code":"LFV2_BITGET_FAMILY_UNRESOLVED","msg":"missing Bitget account family resolver"}',
    )


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


def _parse_optional_float(value: Any) -> Optional[float]:
    """V1 parse_optional_f64_field parity: returns None for missing/empty values.

    V1 (entry_sync.rs): parse_optional_f64_field returns Ok(None) for
    None, empty string, "--", "null", "n/a", "nan". Only actual numeric
    strings are parsed to Some(f64). Uses eq_ignore_ascii_case so all
    case variants of "nan" match. This prevents
    `could not convert string to float: ''` errors in Bybit risk snapshots
    where exchange fields like totalMaintenanceMargin may be empty strings.
    Non-finite floats (nan, inf, -inf) also return None.
    """
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return None
        lower = stripped.lower()
        if lower in ("--", "null", "n/a"):
            return None
        if lower == "nan":
            return None
        # Check for inf variants after NaN, since "nan" != "inf"
        if lower in ("inf", "infinity", "-inf", "-infinity"):
            return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result


def _hyperliquid_spot_usdc_available(raw: Any) -> Optional[tuple[float, float]]:
    if not isinstance(raw, dict):
        return None
    for item in raw.get("balances") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("coin", "") or "").upper() != "USDC":
            continue
        total = _parse_optional_float(item.get("total"))
        hold = _parse_optional_float(item.get("hold"))
        if total is None:
            return None
        held = max(hold or 0.0, 0.0)
        available = max(total - held, 0.0)
        return available, held
    return None


def _require_bybit_success(raw: dict[str, Any], context: str) -> None:
    """Raise REJECTED if Bybit retCode is non-zero."""
    if int(raw.get("retCode", 0) or 0) != 0:
        err = OrderSubmitError(
            SubmitFailureClass.REJECTED,
            f"{context}: bybit retCode={raw.get('retCode')} retMsg={raw.get('retMsg', '')}",
        )
        err.exchange_response_body = json.dumps(raw, separators=(",", ":"))
        raise err


def _require_bitget_success(raw: dict[str, Any], context: str) -> None:
    """Raise REJECTED if Bitget code is not success."""
    code = str(raw.get("code", "00000"))
    if code not in ("00000", "0"):
        raise OrderSubmitError(
            SubmitFailureClass.REJECTED,
            f"{context}: bitget code={code} msg={raw.get('msg', '')}",
        )


def _require_aster_success(raw: dict[str, Any], context: str) -> None:
    """Raise REJECTED if Aster FAPI returns a non-success JSON code."""
    code = str(raw.get("code", "0"))
    if code not in ("0", "200"):
        raise OrderSubmitError(
            SubmitFailureClass.REJECTED,
            f"{context}: aster code={code} msg={raw.get('msg', '')}",
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
    if request.reduce_only:
        return 1 if request.side == Side.SELL else 2
    return 1 if request.side == Side.BUY else 2


def _build_bybit_order_body(
    request: "OrderRequest",
    venue_sym: str,
    *,
    passive: bool,
    hedge_mode: bool,
) -> dict[str, Any]:
    use_limit = passive or (
        request.price is not None and request.time_in_force != TimeInForce.IOC
    )
    body: dict[str, Any] = {
        "category": "linear",
        "symbol": venue_sym,
        "side": _bybit_side(request.side),
        "orderType": "Limit" if use_limit else "Market",
        "qty": _format_quantity(request.quantity),
        "reduceOnly": bool(request.reduce_only),
        "positionIdx": _bybit_position_idx(request, hedge_mode=hedge_mode),
    }
    if request.client_order_id:
        body["orderLinkId"] = request.client_order_id
    if use_limit and request.price is not None and request.price > 0:
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

    Classic uses productType/marginMode/marginCoin. UTA uses
    category/qty/side/timeInForce/clientOid. The request path is read from the
    family-specific operation contract.
    """
    side = "buy" if request.side == Side.BUY else "sell"
    classic_side = side
    if hedge_mode and request.reduce_only:
        classic_side = "sell" if request.side == Side.BUY else "buy"
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
            if request.reduce_only:
                body["posSide"] = "short" if request.side == Side.BUY else "long"
            else:
                body["posSide"] = "long" if request.side == Side.BUY else "short"
        else:
            body["reduceOnly"] = "yes" if request.reduce_only else "no"
        return get_operation_contract(
            get_spec(Venue.BITGET),
            VenueOperation.CREATE_ORDER,
            resolved_account_family=BitgetContractFamily.UTA_V3,
        ).path, body

    # Classic
    body = {
        "symbol": venue_sym,
        "productType": "USDT-FUTURES",
        "marginMode": "crossed",
        "marginCoin": "USDT",
        "size": _format_quantity(request.quantity),
        "side": classic_side,
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
    return get_operation_contract(
        get_spec(Venue.BITGET),
        VenueOperation.CREATE_ORDER,
        resolved_account_family=BitgetContractFamily.CLASSIC_MIX_V2,
    ).path, body


def _bitget_contract_params(
    contract: "VenueOperationContract",
    *,
    venue_sym: str = "",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {}
    for item in contract.required_params:
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        params[key] = value
    if venue_sym:
        params.setdefault("symbol", venue_sym)
    if extra:
        params.update(extra)
    return params


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
    category: TransportErrorCategory, message: str,
    transport_error: Optional[TransportError] = None,
) -> OrderSubmitError:
    if category in (
        TransportErrorCategory.AUTH_FAILURE,
        TransportErrorCategory.AUTHORIZATION_FAILURE,
        TransportErrorCategory.REQUEST_REJECTED,
        TransportErrorCategory.UNSUPPORTED_CAPABILITY,
        TransportErrorCategory.NORMALIZATION_FAILURE,
    ):
        return OrderSubmitError(
            SubmitFailureClass.REJECTED, message,
            transport_error=transport_error,
        )
    return OrderSubmitError(
        SubmitFailureClass.UNCERTAIN, message,
        transport_error=transport_error,
    )


def _transport_error_text(error: Exception) -> str:
    body = getattr(error, "body", "")
    return f"{error} {body}".lower()


def _is_aster_max_notional_error(error: Exception) -> bool:
    message = _transport_error_text(error)
    return "code=-5018" in message or "-5018" in message \
        or "maximum notional value limit" in message


def _is_post_only_would_take_reject(venue: "Venue", error: Exception) -> bool:
    if venue not in (Venue.BINANCE, Venue.ASTER):
        return False
    message = _transport_error_text(error)
    return (
        "-5022" in message
        or "could not be executed as maker" in message
        or "post only order will be rejected" in message
        or "post_only" in message
        or "gtx" in message
    )


def _passive_submit_reject_classification(venue: "Venue", error: Exception) -> str:
    message = _transport_error_text(error)
    if venue == Venue.BYBIT and (
        "30228" in message
        or "110023" in message
        or "110042" in message
        or "110137" in message
        or "no new positions during delisting" in message
        or ("delivery" in message and "reduce" in message)
        or "only reduce-only" in message
    ):
        return "new_position_not_allowed"
    if venue == Venue.BYBIT and (
        "110007" in message
        or "available balance is insufficient" in message
        or "insufficient available balance" in message
    ):
        return "insufficient_balance_admission_blocked"
    if venue == Venue.BYBIT and (
        "110126" in message
        or "110125" in message
        or "110123" in message
        or "must sign required agreement" in message
        or "agree to the trading terms" in message
    ):
        return "bybit_trading_terms_required"
    if venue == Venue.BINANCE and (
        "-2019" in message
        or "margin is insufficient" in message
    ):
        return "insufficient_margin_admission_blocked"
    if venue == Venue.BINANCE and _is_post_only_would_take_reject(venue, error):
        return "post_only_would_take"
    if venue in (Venue.BINANCE, Venue.ASTER) and (
        "-2027" in message
        or "max_leverage_ratio" in message
        or "maximum allowable position at current leverage" in message
    ):
        return "leverage_admission_blocked"
    if venue == Venue.ASTER and _is_aster_max_notional_error(error):
        return "max_notional_admission_blocked"
    if _is_post_only_would_take_reject(venue, error):
        return "post_only_would_take"
    return "rejected"


def _okx_symbol_rule_has_trusted_contract_source(symbol_rule: Any) -> bool:
    source = str(getattr(symbol_rule, "rule_source", "") or "").lower()
    return source == "instrument" or source.endswith("_instrument")


def is_hyperliquid_non_retryable_auth_signing_error(error: Exception) -> bool:
    """Classify Hyperliquid auth/signing failures that must not be retried."""
    message = _transport_error_text(error)
    return (
        "user or api wallet" in message
        and "does not exist" in message
    ) or (
        "recovered signer" in message
    ) or (
        "api wallet" in message
        and ("not authorized" in message or "unauthorized" in message)
    ) or (
        "invalid signature" in message
    ) or (
        "signature" in message and "wallet" in message
    )


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
            # Reserve this request's pacing slot while holding the lock. The
            # first request for a scope has delay 0, but still must publish a
            # timestamp so concurrent first-batch callers cannot all pass.
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

    Classic uses data rows with holdSide/openPriceAvg and size-like fields such
    as total, baseVolume, holdVolume, or size. UTA uses data.list rows with
    posSide/avgPrice and qty/total/available.
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
        qty = abs(_safe_float(
            row.get("total")
            or row.get("baseVolume")
            or row.get("qty")
            or row.get("available")
            or row.get("holdVolume")
            or row.get("size")
        ))
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
        contracts = _safe_float(row.get("pos"), default=0.0)
        pos_side = str(row.get("posSide", "")).lower()
        if pos_side == "long":
            signed_contracts = abs(contracts)
        elif pos_side == "short":
            signed_contracts = -abs(contracts)
        else:
            signed_contracts = contracts
        net += signed_contracts * contract_size
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


def _parse_gate_position(raw: dict[str, Any], symbol: str, now_ms: int, *, contract_size: float = 1.0) -> "PositionSnapshot":
    """Parse Gate.io position (flat dict or list).

    Gate: {"contract":"BTCUSDT","size":"-1","entry_price":"50000"}
    V1: size is in contracts; multiply by contract_size for base quantity (gate.rs:2325-2328)
    """
    rows = raw if isinstance(raw, list) else [raw]
    net = 0.0
    entry_price = 0.0
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("contract") and row.get("contract") != symbol:
            continue
        size = _safe_float(row.get("size"), default=0.0) * contract_size
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
# V1 parity: cancel absent-order detection helpers
# ---------------------------------------------------------------------------


def _cancel_response_indicates_absent_order(raw: dict[str, Any], venue_id: "Venue") -> bool:
    """Check if a successful cancel HTTP response means the order was already absent.

    V1: bitget_payload_indicates_absent_order — codes 40109/43001.
    Returns True when the exchange confirms the order doesn't exist (already filled,
    canceled, expired). In that case cancel is effectively complete.
    """
    if venue_id == Venue.BITGET:
        code = str(raw.get("code", ""))
        if code in ("40109", "43001"):
            return True
    if venue_id == Venue.OKX:
        data = raw.get("data", [])
        if isinstance(data, list) and data:
            item = data[0] if data else {}
            s_code = str(item.get("sCode", "0"))
            s_msg = str(item.get("sMsg", "")).lower()
            if s_code in ("1", "2", "51400", "51603"):
                return True
            if "order does not exist" in s_msg or "order not found" in s_msg:
                return True
    if venue_id == Venue.BYBIT:
        # V1: bybit_cancel_order_missing_is_terminal(ret_code=110001)
        # Bybit returns 200 with retCode=110001 when order doesn't exist
        ret_code = raw.get("retCode", raw.get("ret_code", 0))
        if str(ret_code) == "110001":
            return True
        result = raw.get("result", raw.get("data", {}))
        if isinstance(result, dict):
            ret_code = result.get("retCode", result.get("ret_code", 0))
            if str(ret_code) == "110001":
                return True
    if venue_id == Venue.HYPERLIQUID:
        response = raw.get("response", raw)
        data = response.get("data", {}) if isinstance(response, dict) else {}
        statuses = data.get("statuses", []) if isinstance(data, dict) else []
        if isinstance(statuses, list):
            for item in statuses:
                msg = str(item.get("error", item) if isinstance(item, dict) else item).lower()
                if "order was never placed, already canceled, or filled" in msg:
                    return True
    return False


def _cancel_error_indicates_absent_order(
    body: str, status_code: int, venue_id: "Venue",
) -> bool:
    """Check if an HTTP error from cancel means the order was already absent.

    V1: bitget_error_indicates_absent_order catches codes 40109/43001 in error chain.
    Other venues have equivalent "order not found" signatures.
    """
    msg = body.lower()
    if venue_id == Venue.BITGET:
        if 'code=40109' in msg or 'code=43001' in msg:
            return True
        if '"code":"40109"' in msg or '"code":"43001"' in msg:
            return True
    if venue_id in (Venue.BINANCE, Venue.ASTER):
        if '-2011' in body or 'unknown order' in msg:
            return True
    if venue_id == Venue.OKX:
        if 'order does not exist' in msg:
            return True
    if venue_id == Venue.BYBIT:
        # V1: bybit_cancel_order_missing_is_terminal (bybit.rs:4012-4017)
        if 'order not found' in msg or '110001' in body or '170130' in body:
            return True
    if venue_id == Venue.GATE:
        if 'order_not_found' in msg or 'ORDER_NOT_FOUND' in body:
            return True
    return False


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
        if (
            mode == "live"
            and spec.requires_wallet_key
            and spec.venue_id == Venue.HYPERLIQUID
            and credential is not None
        ):
            credential = _normalize_hyperliquid_credential(credential)
        self.mode = mode
        self._credential = credential
        self._hl_meta_cache: dict[str, int] = {}
        self._hl_asset_meta_cache: dict[str, dict[str, int]] = {}
        self._symbol_metadata: dict[str, dict[str, Any]] = {}  # sym → vendor contract info
        self._okx_swap_instruments_loaded = False
        self._time_offset_ms: int | None = None  # V1: cached server-time offset
        self._order_diagnostics: list[dict[str, Any]] = []
        self._entry_leverage_ready_cache: dict[str, int] = {}
        self._trading_capability_trusted = not (
            mode == "live" and spec.venue_id == Venue.HYPERLIQUID
        )
        self._trading_preflight_status: dict[str, Any] = {}

        # V1: private WebSocket connection health tracking.
        # The private WS state owns the canonical health object; setup below.
        self._private_ws_health = ConnectionHealth()

        # V1: position cache populated by fetch_position() / fetch_all_positions()
        # Map of symbol → (PositionSnapshot, cached_at_ms)
        self._position_cache: dict[str, tuple[PositionSnapshot, int]] = {}
        self._all_positions_cache: tuple[list[PositionSnapshot], int] | None = None

        # V1: shared private WS state — order/position caches, health, workers.
        # Each venue transport owns one PrivateWsState. Workers update this
        # directly via record_order/update_position/record_connection_*.
        self._private_ws_state = PrivateWsState()

        if mode == "live":
            self._validate_live_credentials(credential)

    # ------------------------------------------------------------------
    # V1: private WebSocket health (updated by external WS connection manager)
    # ------------------------------------------------------------------

    def cached_private_connection_health(self) -> Optional[ConnectionHealth]:
        """V1: return private WS connection health for supervisor evaluation.

        Returns the ConnectionHealth object tracking the private stream.
        The external private WS manager calls record_private_ws_success/failure
        to keep this current.
        """
        return self._private_ws_health

    def record_private_ws_success(self, now_ms: int) -> None:
        """V1: called by private WS connection on successful message/connect."""
        self._private_ws_health.record_success(now_ms)
        self._private_ws_state.record_connection_success(now_ms)

    def record_private_ws_failure(self, now_ms: int, error: str, unhealthy_after: int = 5) -> None:
        """V1: called by private WS connection on failure/disconnect."""
        self._private_ws_health.record_failure(now_ms, unhealthy_after, error)
        self._private_ws_state.record_connection_failure(now_ms, unhealthy_after, error)

    def cached_position(self, symbol: str) -> Optional[PositionSnapshot]:
        """V1: return cached position for a symbol (from fetch_position or private stream).

        Checks private WS state first (authoritative push), then REST cache as fallback.
        Returns None if no cached position or cache is stale.
        """
        # Private WS push is authoritative
        now_ms = int(time.time() * 1000)
        private_pos = self._private_ws_state.position_if_fresh(symbol, 30_000, now_ms)
        if private_pos is not None:
            return PositionSnapshot(
                venue=self._spec.venue_id,
                symbol=private_pos.symbol,
                side=Side.BUY if private_pos.size >= 0 else Side.SELL,
                quantity=abs(private_pos.size),
                entry_price=0.0,
                observed_at_ms=private_pos.updated_at_ms,
            )

        # REST fallback
        entry = self._position_cache.get(symbol)
        if entry is None:
            return None
        snapshot, cached_at_ms = entry
        if (now_ms - cached_at_ms) > 30_000:
            return None
        return snapshot

    # ------------------------------------------------------------------
    # Credential validation
    # ------------------------------------------------------------------

    def _validate_live_credentials(self, credential: Optional[LiveCredential]) -> None:
        if credential is None:
            raise ValueError(
                f"live mode requires credentials for {self._spec.venue_id.value}"
            )
        if self._spec.requires_wallet_key:
            if self._spec.venue_id == Venue.ASTER:
                from lightfee.venues.aster_v3 import (
                    credential_has_aster_v3_signer,
                )

                if not credential_has_aster_v3_signer(credential):
                    # Aster public market data is still usable without a valid
                    # Pro API V3 signer. The Aster adapter gates private calls.
                    return
                return
            # Hyperliquid uses private key + account address, not api_key + api_secret.
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

    def _hyperliquid_info_operation_request(
        self,
        operation: VenueOperation,
        *,
        account_address: str,
    ) -> tuple[str, str, dict[str, Any], bool]:
        contract = get_operation_contract(self._spec, operation)
        if (
            self._spec.venue_id != Venue.HYPERLIQUID
            or not contract.supported
            or contract.method.upper() != "POST"
            or contract.path != "/info"
            or contract.payload != "body"
        ):
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                f"hyperliquid info operation contract invalid: {operation.value}",
            )

        body: dict[str, Any] = {}
        for param in contract.required_params:
            key, sep, raw_value = param.partition("=")
            if not sep or not key:
                raise TransportError(
                    TransportErrorCategory.REQUEST_REJECTED,
                    f"hyperliquid info operation contract malformed: {operation.value}",
                )
            if raw_value == "configured_account_address":
                value = account_address
            else:
                value = raw_value
            body[key] = value

        if body.get("user") != account_address or not body.get("type"):
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                f"hyperliquid info operation contract incomplete: {operation.value}",
            )
        return contract.method.upper(), contract.path, body, contract.private

    @property
    def trading_capability_trusted(self) -> bool:
        """True only after read-only live preflight trusts order submission."""
        return bool(self._trading_capability_trusted)

    def _hyperliquid_trading_disabled_reason(self) -> str | None:
        if self.mode != "live" or self._spec.venue_id != Venue.HYPERLIQUID:
            return None
        if self.trading_capability_trusted:
            return None
        if not self._trading_preflight_status:
            return "trading_preflight_not_verified"
        return str(
            self._trading_preflight_status.get("reason")
            or "trading_preflight_failed"
        )

    def _reject_hyperliquid_trading_if_disabled(self) -> None:
        reason = self._hyperliquid_trading_disabled_reason()
        if reason is None:
            return
        raise OrderSubmitError(
            SubmitFailureClass.REJECTED,
            f"hyperliquid_trading_disabled:{reason}",
        )

    async def verify_live_trading_preflight(self) -> dict[str, Any]:
        """Run live trading preflight before allowing Hyperliquid orders.

        Account-wallet mode uses V1 direct-wallet semantics: the signing wallet
        is the account. API-wallet mode proves agent authorization with
        Hyperliquid's official no-op exchange action, which marks only the
        nonce as used.
        """
        if self.mode != "live" or self._spec.venue_id != Venue.HYPERLIQUID:
            self._trading_capability_trusted = True
            self._trading_preflight_status = {
                "venue": self._spec.venue_id.value,
                "status": "ok",
                "trading_capability_trusted": True,
            }
            return dict(self._trading_preflight_status)

        payload: dict[str, Any] = {
            "venue": self._spec.venue_id.value,
            "status": "failed",
            "trading_capability_trusted": False,
            "authorization_mode": "account_wallet",
            "wallet_matches_account": False,
            "signer_matches_account": False,
            "authorization_verified": False,
            "api_wallet_authorization_verified": False,
            "clearinghouse_state_readable": False,
        }
        cred = self._credential
        if cred is None or not cred.wallet_private_key:
            payload["reason"] = "missing_wallet_private_key"
            self._trading_capability_trusted = False
            self._trading_preflight_status = payload
            return dict(payload)

        authorization_mode = _normalize_hyperliquid_wallet_mode(cred.wallet_mode)
        payload["authorization_mode"] = (
            authorization_mode if authorization_mode in ("account_wallet", "api_wallet")
            else "invalid"
        )
        if authorization_mode not in ("account_wallet", "api_wallet"):
            payload["reason"] = "invalid_hyperliquid_wallet_mode"
            payload["configured_wallet_mode"] = cred.wallet_mode
            self._trading_capability_trusted = False
            self._trading_preflight_status = payload
            return dict(payload)

        try:
            wallet_address = _derive_hyperliquid_account_address(cred.wallet_private_key)
        except Exception as exc:
            payload["reason"] = "wallet_private_key_derivation_failed"
            payload["error"] = str(exc)[:200]
            self._trading_capability_trusted = False
            self._trading_preflight_status = payload
            return dict(payload)

        account_address = (cred.account_address or wallet_address or "").strip()
        account_matches_wallet = (
            bool(account_address)
            and wallet_address.lower() == account_address.lower()
        )
        payload["wallet_matches_account"] = account_matches_wallet
        payload["signer_matches_account"] = account_matches_wallet
        payload["account_address_present"] = bool(account_address)
        payload["configured_account_address"] = account_address
        payload["signer_address"] = wallet_address

        if not account_address:
            payload["reason"] = "missing_account_address"
            self._trading_capability_trusted = False
            self._trading_preflight_status = payload
            return dict(payload)

        if authorization_mode == "account_wallet" and not account_matches_wallet:
            payload["reason"] = "account_wallet_signer_mismatch"
            self._trading_capability_trusted = False
            self._trading_preflight_status = payload
            return dict(payload)

        try:
            raw = await self._request(
                "POST",
                "/info",
                body={"type": "clearinghouseState", "user": account_address},
                private=False,
            )
            payload["clearinghouse_state_readable"] = isinstance(raw, dict)
        except Exception as exc:
            payload["reason"] = "clearinghouse_state_unreadable"
            payload["error"] = str(exc)[:200]
            self._trading_capability_trusted = False
            self._trading_preflight_status = payload
            return dict(payload)

        if not payload["clearinghouse_state_readable"]:
            payload["reason"] = "clearinghouse_state_unreadable"
            self._trading_capability_trusted = False
            self._trading_preflight_status = payload
            return dict(payload)

        if authorization_mode == "api_wallet":
            try:
                from lightfee.venues.hyperliquid_signing import (
                    build_hyperliquid_exchange_payload,
                )

                proof_body = build_hyperliquid_exchange_payload(
                    action={"type": "noop"},
                    private_key_hex=cred.wallet_private_key,
                    vault_address=None,
                    is_mainnet=True,
                )
                proof_raw = await self._request(
                    "POST",
                    "/exchange",
                    body=proof_body,
                    private=True,
                )
            except Exception as exc:
                payload["reason"] = "api_wallet_authorization_unverified"
                payload["authorization_error"] = str(exc)[:200]
                self._trading_capability_trusted = False
                self._trading_preflight_status = payload
                return dict(payload)

            if not (
                isinstance(proof_raw, dict)
                and proof_raw.get("status") == "ok"
            ):
                payload["reason"] = "api_wallet_authorization_unverified"
                if isinstance(proof_raw, dict):
                    authorization_error = (
                        proof_raw.get("response")
                        or proof_raw.get("error")
                        or proof_raw.get("message")
                        or proof_raw
                    )
                else:
                    authorization_error = proof_raw
                payload["authorization_error"] = str(authorization_error)[:200]
                self._trading_capability_trusted = False
                self._trading_preflight_status = payload
                return dict(payload)

            payload["api_wallet_authorization_verified"] = True
        else:
            payload["api_wallet_authorization_verified"] = False

        payload["status"] = "ok"
        payload["trading_capability_trusted"] = True
        payload["authorization_verified"] = True
        self._trading_capability_trusted = True
        self._trading_preflight_status = payload
        return dict(payload)

    # ------------------------------------------------------------------
    # Private WS state access
    # ------------------------------------------------------------------

    @property
    def private_ws_state(self) -> PrivateWsState:
        """V1: the shared PrivateWsState for this venue transport."""
        return self._private_ws_state

    # ------------------------------------------------------------------
    # Private WS lifecycle (called by runtime)
    # ------------------------------------------------------------------

    def start_private_ws(self, symbols: list[str]) -> None:
        """Start private WS worker(s) for tracked symbols.

        Dispatches to the venue-specific private WS worker.
        Each venue handles auth/subscribe/heartbeat/reconnect independently.
        The worker must call record_private_ws_success/failure on real paths.
        """
        venue = self._spec.venue_id
        if venue == Venue.BINANCE:
            self._start_binance_private_ws(symbols)
        elif venue == Venue.ASTER:
            self._start_aster_private_ws(symbols)
        elif venue == Venue.OKX:
            self._start_okx_private_ws(symbols)
        elif venue == Venue.BYBIT:
            self._start_bybit_private_ws(symbols)
        elif venue == Venue.BITGET:
            self._start_bitget_private_ws(symbols)
        elif venue == Venue.GATE:
            self._start_gate_private_ws(symbols)
        elif venue == Venue.HYPERLIQUID:
            self._start_hyperliquid_private_ws(symbols)

    def stop_private_ws(self) -> None:
        """Stop all private WS workers for this venue."""
        self._private_ws_state.abort_workers()

    def private_ws_worker_count(self) -> int:
        """V1: number of active private WS workers for this venue."""
        return self._private_ws_state.worker_count()

    # ------------------------------------------------------------------
    # Private WS venue-specific starters (stubs — implemented in venue modules)
    # ------------------------------------------------------------------

    def _start_binance_private_ws(self, symbols: list[str]) -> None:
        from lightfee.venues.binance_private_ws import start_binance_private_ws
        start_binance_private_ws(self, symbols)

    def _start_aster_private_ws(self, symbols: list[str]) -> None:
        from lightfee.venues.aster_private_ws import start_aster_private_ws
        start_aster_private_ws(self, symbols)

    def _start_okx_private_ws(self, symbols: list[str]) -> None:
        from lightfee.venues.okx_private_ws import start_okx_private_ws
        start_okx_private_ws(self, symbols)

    def _start_bybit_private_ws(self, symbols: list[str]) -> None:
        from lightfee.venues.bybit_private_ws import start_bybit_private_ws
        start_bybit_private_ws(self, symbols)

    def _start_bitget_private_ws(self, symbols: list[str]) -> None:
        from lightfee.venues.bitget_private_ws import start_bitget_private_ws
        start_bitget_private_ws(self, symbols)

    def _start_gate_private_ws(self, symbols: list[str]) -> None:
        from lightfee.venues.gate_private_ws import start_gate_private_ws
        start_gate_private_ws(self, symbols)

    def _start_hyperliquid_private_ws(self, symbols: list[str]) -> None:
        from lightfee.venues.hyperliquid_private_ws import start_hyperliquid_private_ws
        start_hyperliquid_private_ws(self, symbols)

    # ------------------------------------------------------------------
    # Private fill / progress delegation (private-first, REST fallback)
    # ------------------------------------------------------------------

    def private_order_progress(
        self,
        client_order_id: Optional[str] = None,
        order_id: Optional[str] = None,
        max_age_ms: int = 0,
    ) -> Optional[CumulativeOrderProgress]:
        """V1: get order progress from private state cache."""
        now_ms = int(time.time() * 1000)
        return self._private_ws_state.order_progress_if_fresh(
            client_order_id=client_order_id,
            order_id=order_id,
            max_age_ms=max_age_ms,
            wall_clock_now_ms=now_ms,
        )

    async def lookup_or_wait_private_order(
        self,
        client_order_id: Optional[str] = None,
        order_id: Optional[str] = None,
        wait_ms: int = 0,
    ) -> Optional[PrivateOrderUpdate]:
        """V1: lookup or wait for a private order update."""
        return await lookup_or_wait_private_order(
            self._private_ws_state,
            client_order_id=client_order_id,
            order_id=order_id,
            wait_ms=wait_ms,
        )

    async def lookup_or_wait_private_order_progress(
        self,
        client_order_id: Optional[str] = None,
        order_id: Optional[str] = None,
        wait_ms: int = 0,
    ) -> Optional[CumulativeOrderProgress]:
        """V1: lookup or wait for private order progress."""
        return await lookup_or_wait_private_order_progress(
            self._private_ws_state,
            client_order_id=client_order_id,
            order_id=order_id,
            wait_ms=wait_ms,
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

    def _record_order_reconcile_query(
        self,
        *,
        symbol: str,
        order_id: str = "",
        client_order_id: str = "",
        queried_endpoints: Optional[list[str]] = None,
        response_classification: str = "",
        uncertain_subtype: str = "",
        next_action: str = "reconcile_again_after_backoff",
    ) -> None:
        endpoints = [str(e) for e in (queried_endpoints or []) if str(e)]
        if not endpoints:
            endpoints = ["fetch_order_status"]
        classification = response_classification or uncertain_subtype or "uncertain"
        identifier_kind = (
            "order_id"
            if order_id
            else "client_order_id"
            if client_order_id
            else "missing"
        )
        self._record_order_diagnostic(
            "order.reconcile_query",
            {
                "venue": self._spec.venue_id.value,
                "symbol": symbol,
                "observed_at_ms": int(time.time() * 1000),
                "client_order_id": client_order_id,
                "order_id": order_id,
                "exchange_order_id": order_id,
                "identifier_kind": identifier_kind,
                "has_order_id": bool(order_id),
                "has_client_order_id": bool(client_order_id),
                "queried_endpoints": endpoints,
                "endpoint_responses": [
                    {"endpoint": endpoint, "classification": classification}
                    for endpoint in endpoints
                ],
                "response_classification": classification,
                "uncertain_subtype": uncertain_subtype,
                "next_action": next_action,
            },
        )

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
            "trading_capability_trusted": self.trading_capability_trusted,
            "trading_preflight_status": dict(self._trading_preflight_status),
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
        if symbol_rule is not None and spec.venue_id == Venue.OKX:
            # OKX SWAP/FUTURES/OPTION SymbolRule qty_step/min_qty are lotSz/minSz
            # in contracts. The engine request quantity is base units; contract
            # validation happens later via _okx_contract_order_diagnostics.
            quantity_step = 0.0
            min_qty = 0.0
        else:
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
        if symbol_rule is not None and spec.venue_id == Venue.OKX:
            min_notional = float(symbol_rule.min_notional or 0.0)
        else:
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

    async def precheck_order_admission(self, request: OrderRequest) -> dict[str, Any]:
        """Validate a Bybit order through the official non-mutating pre-check API."""
        spec = self._spec
        venue_sym = self._venue_symbol(request.symbol)
        if spec.venue_id != Venue.BYBIT:
            return {
                "venue": spec.venue_id.value,
                "symbol": venue_sym,
                "status": "skipped",
                "reason": "precheck_not_supported_for_venue",
            }
        if self.mode == "paper":
            return {
                "venue": spec.venue_id.value,
                "symbol": venue_sym,
                "status": "skipped",
                "reason": "paper_mode",
            }
        if request.reduce_only:
            return {
                "venue": spec.venue_id.value,
                "symbol": venue_sym,
                "status": "skipped",
                "reason": "reduce_only_exempt",
            }

        symbol_rule = None
        try:
            symbol_rule = await get_symbol_rules_cache().get(
                self,
                spec.venue_id,
                venue_sym,
            )
        except Exception:
            symbol_rule = None
        preflight = self.preflight_order_request(request, symbol_rule=symbol_rule)
        request = replace(
            request,
            quantity=float(preflight["quantized_qty"]),
            price=(
                None
                if preflight["quantized_price"] is None
                else float(preflight["quantized_price"])
            ),
        )
        passive = bool(
            request.post_only or request.time_in_force == TimeInForce.POST_ONLY
        )
        body = _build_bybit_order_body(
            request,
            venue_sym,
            passive=passive,
            hedge_mode=self._hedge_mode,
        )
        if "qty" in body:
            body["qty"] = _format_decimal(request.quantity)
        if request.price is not None and (
            passive or request.time_in_force != TimeInForce.IOC
        ):
            body["price"] = _format_decimal(request.price)
        preflight["body_field_names"] = sorted(body.keys())
        preflight["body_sanitized"] = {
            k: v for k, v in body.items() if k not in ("orderLinkId",)
        }
        preflight["precheck_endpoint"] = "/v5/order/pre-check"
        self._record_order_diagnostic("order.precheck_attempt", preflight)

        try:
            raw = await self._request(
                "POST",
                "/v5/order/pre-check",
                body=body,
                private=True,
            )
            _require_bybit_success(raw, "bybit order precheck failed")
        except TransportError as e:
            result_payload = dict(preflight)
            result_payload["response_code"] = e.status_code
            result_payload["response_msg"] = str(e)[:500]
            result_payload["response_body"] = e.body[:1000]
            result_payload["response_classification"] = (
                "rejected"
                if e.category in (
                    TransportErrorCategory.AUTH_FAILURE,
                    TransportErrorCategory.AUTHORIZATION_FAILURE,
                    TransportErrorCategory.REQUEST_REJECTED,
                    TransportErrorCategory.UNSUPPORTED_CAPABILITY,
                    TransportErrorCategory.NORMALIZATION_FAILURE,
                )
                else "uncertain"
            )
            self._record_order_diagnostic("order.precheck_result", result_payload)
            raise _map_to_submit_error(e.category, str(e), transport_error=e)
        except OrderSubmitError as e:
            result_payload = dict(preflight)
            result_payload["response_classification"] = e.class_.value
            result_payload["response_msg"] = str(e)[:500]
            self._record_order_diagnostic("order.precheck_result", result_payload)
            raise
        except Exception as e:
            result_payload = dict(preflight)
            result_payload["response_classification"] = "uncertain"
            result_payload["response_msg"] = str(e)[:500]
            self._record_order_diagnostic("order.precheck_result", result_payload)
            raise OrderSubmitError(SubmitFailureClass.UNCERTAIN, str(e)) from e

        result_payload = dict(preflight)
        result_payload["response_classification"] = "accepted"
        if isinstance(raw, dict):
            result_payload["response_msg"] = str(raw.get("retMsg", ""))[:500]
        self._record_order_diagnostic("order.precheck_result", result_payload)
        return {
            "venue": spec.venue_id.value,
            "symbol": venue_sym,
            "status": "ok",
            "response_classification": "accepted",
        }

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
            top_level_time = raw.get("time")
            if top_level_time:
                return int(top_level_time)
            result = raw.get("result", {})
            if isinstance(result, dict):
                time_nano = result.get("timeNano")
                if time_nano:
                    return int(time_nano) // 1_000_000
                time_second = result.get("timeSecond")
                if time_second:
                    return int(time_second) * 1000
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

        if body is not None and not (spec.signature_param and private and cred):
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

                if private and self._is_time_offset_retryable(resp.status_code, resp.text):
                    if _retry_ts_error:
                        self._clear_server_time_offset()
                        await asyncio.sleep(0.1)  # V1: short delay before retry
                        return await self._request(
                            method, path, params=params, body=body,
                            private=private, _retry_ts_error=False,
                        )
                    raise TransportError(
                        TransportErrorCategory.TRANSPORT_FAILURE,
                        self._time_offset_retry_exhausted_message(
                            method, path, resp.status_code, resp.text, headers
                        ),
                        status_code=resp.status_code, body=resp.text,
                        headers=dict(resp.headers),
                    )

                cat = classify_transport_error(resp.status_code, resp.text)
                if cat:
                    raise TransportError(
                        cat, f"HTTP {resp.status_code}: {resp.text[:200]}",
                        status_code=resp.status_code, body=resp.text,
                        headers=dict(resp.headers),
                    )

            if not resp.text:
                # V1: record success for all scopes
                if self._rate_limiter is not None:
                    self._rate_limiter.record_success_for_scopes(scopes)
                return {}
            raw = resp.json()
            if private and self._is_time_offset_retryable(resp.status_code, resp.text):
                if _retry_ts_error:
                    self._clear_server_time_offset()
                    await asyncio.sleep(0.1)
                    return await self._request(
                        method, path, params=params, body=body,
                        private=private, _retry_ts_error=False,
                    )
                raise TransportError(
                    TransportErrorCategory.TRANSPORT_FAILURE,
                    self._time_offset_retry_exhausted_message(
                        method, path, resp.status_code, resp.text, headers
                    ),
                    status_code=resp.status_code, body=resp.text,
                    headers=dict(resp.headers),
                )

            # V1: record success for all scopes
            if self._rate_limiter is not None:
                self._rate_limiter.record_success_for_scopes(scopes)
            return raw
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

    async def _request_listen_key(
        self,
        method: str,
        path: str,
        *,
        api_key: str,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Send Binance/Aster user-stream listenKey requests.

        V1 parity: these endpoints use only the API-key header and optional
        listenKey query parameter. They are not trading signed requests.
        """
        client = await self._get_client()
        base_url = self._spec.private_base_url
        qp_list: list[tuple[str, str]] = []
        if params:
            qp_list = [(k, str(v)) for k, v in params.items() if v is not None]
        query_string = "?" + _build_query_v1(qp_list) if qp_list else ""
        url = base_url + path + query_string
        headers: dict[str, str] = {}
        if self._spec.api_key_header and api_key:
            headers[self._spec.api_key_header] = api_key

        scopes = self._rest_rate_limit_scopes(method, path, base_url, private=True)
        if self._rate_limiter is not None:
            await self._rate_limiter.wait_until_ready_for_scopes(scopes)
            await self._rate_limiter.pace_for_scopes(scopes)

        from lightfee.rate_limit.engine import global_rate_limit_runtime as _get_global_rt
        global_rt = _get_global_rt()
        if global_rt is not None:
            await global_rt.async_wait_until_ready_for_scopes(scopes)

        try:
            method_upper = method.upper()
            if method_upper == "POST":
                resp = await client.post(url, headers=headers)
            elif method_upper == "PUT":
                resp = await client.put(url, headers=headers)
            elif method_upper == "DELETE":
                resp = await client.delete(url, headers=headers)
            elif method_upper == "GET":
                resp = await client.get(url, headers=headers)
            else:
                raise ValueError(f"unsupported HTTP method: {method}")

            if resp.status_code >= 400:
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
                cat = classify_transport_error(resp.status_code, resp.text)
                raise TransportError(
                    cat or TransportErrorCategory.TRANSPORT_FAILURE,
                    f"HTTP {resp.status_code}: {resp.text[:200]}",
                    status_code=resp.status_code,
                    body=resp.text,
                    headers=dict(resp.headers),
                )

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

    def _time_offset_retry_exhausted_message(
        self,
        method: str,
        path: str,
        status_code: int,
        body: str,
        request_headers: dict[str, str],
    ) -> str:
        spec = self._spec
        fields = [
            f"{spec.venue_id.value}: timestamp/recv_window error after server-time refresh",
            f"method={method.upper()}",
            f"path={path}",
            f"status={status_code}",
            f"server_time_path={spec.server_time_path or ''}",
        ]
        if spec.timestamp_header:
            fields.append(
                f"auth_timestamp_ms={request_headers.get(spec.timestamp_header, '')}"
            )
        if spec.recv_window_header:
            fields.append(
                f"recv_window_ms={request_headers.get(spec.recv_window_header, spec.recv_window_ms or '')}"
            )
        fields.append(f"body={body[:200]}")
        return " ".join(fields)

    def _is_time_offset_retryable(self, status_code: int, body: str) -> bool:
        """V1/doc aligned retry for server timestamp and recv_window errors.

        V1 predicate matches (case-insensitive, on error message):
          code=-1021, recvwindow, timestamp, position-side mismatch, 500-504

        Bybit V1 prevents most clock drift with server time and a 1500ms auth
        backoff. Bybit official V5 code 10002 is the remaining timestamp /
        recv_window window failure, so V2 clears the cached offset and retries
        once with a fresh server-time sample.
        """
        spec = self._spec
        msg = f"status={status_code} {body}".lower()
        if spec.venue_id in (Venue.BINANCE, Venue.ASTER):
            if status_code < 400:
                return False
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
        if spec.venue_id == Venue.BYBIT:
            ret_code = ""
            try:
                raw = json.loads(body) if body else {}
            except (TypeError, ValueError, json.JSONDecodeError):
                raw = {}
            if isinstance(raw, dict):
                ret_code = str(raw.get("retCode", raw.get("ret_code", "")) or "")
            if status_code < 400:
                return ret_code == "10002"
            return (
                ret_code == "10002"
                or "retcode\":10002" in msg
                or "ret_code\":10002" in msg
                or "recv_window" in msg
                or "recvwindow" in msg
                or "timestamp" in msg
            )
        return False

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
            if spec.venue_id == Venue.OKX:
                positions = await self._parse_all_positions_okx(raw, now_ms)
            else:
                positions = self._parse_all_positions(raw, now_ms)
            # Populate position cache for supervisor private position confirmation
            for pos in positions:
                self._position_cache[pos.symbol] = (pos, now_ms)
            self._all_positions_cache = (positions, now_ms)
            return positions
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
            snapshot = PositionSnapshot(
                venue=spec.venue_id,
                symbol=venue_sym,
                side=Side.BUY,
                quantity=0.0,
                entry_price=0.0,
                observed_at_ms=now_ms,
            )
            self._position_cache[symbol] = (snapshot, now_ms)
            return snapshot

        if spec.venue_id == Venue.OKX:
            cached = self.cached_position(symbol)
            if cached is None and spec.symbol_from_venue is not None:
                cached = self.cached_position(spec.symbol_from_venue(venue_sym))
            if cached is not None:
                cached_entry = (
                    self._position_cache.get(cached.symbol)
                    or self._position_cache.get(symbol)
                )
                if cached_entry is None:
                    return cached
                cached_at = cached_entry[1]
                if now_ms - cached_at <= OKX_POSITION_REST_CACHE_MAX_AGE_MS:
                    return cached
            all_cached = self._all_positions_cache
            if (
                all_cached is not None
                and now_ms - all_cached[1] <= OKX_POSITION_REST_CACHE_MAX_AGE_MS
            ):
                for pos in all_cached[0]:
                    if pos.symbol in {symbol, venue_sym}:
                        return pos
                snapshot = PositionSnapshot(
                    venue=spec.venue_id,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=0.0,
                    entry_price=0.0,
                    observed_at_ms=now_ms,
                )
                self._position_cache[symbol] = (snapshot, now_ms)
                return snapshot

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
                snapshot = self._parse_position(raw, venue_sym, now_ms)
                self._position_cache[symbol] = (snapshot, now_ms)
                return snapshot

            raw = await self._request("GET", spec.position_path, params=params, private=True)
            if spec.venue_id == Venue.OKX:
                contract_size = await self._okx_contract_size_for_venue_symbol(venue_sym)
                snapshot = self._parse_position(
                    raw,
                    symbol,
                    now_ms,
                    contract_size_override=contract_size,
                )
            else:
                snapshot = self._parse_position(raw, venue_sym, now_ms)
            self._position_cache[symbol] = (snapshot, now_ms)
            return snapshot
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

    async def _parse_all_positions_okx(
        self,
        raw: Any,
        now_ms: int,
    ) -> list[PositionSnapshot]:
        spec = self._spec
        positions: list[PositionSnapshot] = []
        for row in _venue_position_rows(raw, Venue.OKX):
            venue_symbol = _position_row_symbol(row, Venue.OKX)
            if not venue_symbol:
                continue
            payload = _position_row_parse_payload(row, Venue.OKX)
            contract_size = await self._okx_contract_size_for_venue_symbol(venue_symbol)
            pos = self._parse_position(
                payload,
                _canonical_position_symbol(spec, venue_symbol),
                now_ms,
                contract_size_override=contract_size,
            )
            if abs(pos.quantity) <= 1e-9:
                continue
            positions.append(
                replace(pos, symbol=_canonical_position_symbol(spec, venue_symbol))
            )
        return positions

    async def _ensure_okx_swap_instrument_metadata_loaded(self) -> None:
        """Preload OKX SWAP metadata carrying ctVal/ctType from official instruments APIs."""
        if self._spec.venue_id != Venue.OKX or self._okx_swap_instruments_loaded:
            return
        loaded = False
        try:
            raw = await self._public_get(
                OKX_PUBLIC_INSTRUMENTS_PATH,
                {"instType": "SWAP"},
            )
            loaded = self._okx_cache_instrument_metadata(raw) or loaded
        except Exception:
            pass
        if not loaded and self.mode == "live":
            try:
                raw = await self._request(
                    "GET",
                    OKX_ACCOUNT_INSTRUMENTS_PATH,
                    params={"instType": "SWAP"},
                    private=True,
                )
                loaded = self._okx_cache_instrument_metadata(raw) or loaded
            except Exception:
                pass
        if loaded:
            self._okx_swap_instruments_loaded = True

    def _okx_cache_instrument_metadata(self, raw: Any) -> bool:
        rows = raw.get("data", []) if isinstance(raw, dict) else raw
        if not isinstance(rows, list):
            return False
        cached = False
        for row in rows:
            if not isinstance(row, dict):
                continue
            inst_id = str(row.get("instId", "") or "")
            if not inst_id:
                continue
            metadata = dict(row)
            self._symbol_metadata[inst_id] = metadata
            if self._spec.symbol_from_venue is not None:
                try:
                    canonical = self._spec.symbol_from_venue(inst_id)
                    if canonical:
                        self._symbol_metadata[canonical] = metadata
                except Exception:
                    pass
            cached = True
        return cached

    async def _okx_contract_size_for_venue_symbol(self, venue_symbol: str) -> float:
        """Resolve OKX ctVal used to convert position contracts into base size."""
        spec = self._spec
        if self.mode == "live":
            await self._ensure_okx_swap_instrument_metadata_loaded()

        metadata_candidates: list[str] = [venue_symbol]
        if spec.symbol_from_venue is not None:
            try:
                canonical = spec.symbol_from_venue(venue_symbol)
                metadata_candidates.append(canonical)
            except Exception:
                pass

        for key in metadata_candidates:
            metadata = self._symbol_metadata.get(key)
            if not isinstance(metadata, dict):
                continue
            ct_type = str(
                metadata.get(
                    "ctType",
                    metadata.get("ct_type", metadata.get("contractType", "")),
                )
                or ""
            )
            has_contract_type_evidence = bool(ct_type)
            for field in ("ct_val", "ctVal", "contract_size", "contractSize"):
                value = _safe_float(metadata.get(field), default=0.0)
                if value > 0 and has_contract_type_evidence:
                    return value
                if value > 0 and self.mode == "paper":
                    return value
            if not has_contract_type_evidence:
                raise self._okx_missing_ct_val_error(venue_symbol, "metadata_missing")
            raise self._okx_missing_ct_val_error(venue_symbol, "metadata_missing")

        if self._okx_swap_instruments_loaded:
            raise self._okx_missing_ct_val_error(venue_symbol, "instrument_missing")

        raise self._okx_missing_ct_val_error(venue_symbol, "metadata_missing")

    @staticmethod
    def _okx_missing_ct_val_error(
        venue_symbol: str,
        classification: str,
    ) -> TransportError:
        return TransportError(
            TransportErrorCategory.NORMALIZATION_FAILURE,
            (
                "okx_contract_metadata_missing_ct_val "
                f"classification={classification} "
                f"instId={venue_symbol}"
            ),
        )

    def _parse_position(
        self,
        raw: dict[str, Any],
        symbol: str,
        now_ms: int,
        *,
        contract_size_override: float | None = None,
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
            contract_size = (
                contract_size_override
                if contract_size_override is not None
                else spec.contract_size
            )
            result = _parse_okx_position(raw, venue_sym, now_ms, contract_size=contract_size)
            if result.quantity > 0 or not isinstance(raw, dict):
                return result
            return _parse_generic_position(raw, spec, venue_sym, now_ms)
        if spec.venue_id in (Venue.BINANCE, Venue.ASTER):
            result = _parse_binance_like_position(raw, venue_sym, now_ms, venue=spec.venue_id)
            if result.quantity > 0 or not isinstance(raw, dict):
                return result
            return _parse_generic_position(raw, spec, venue_sym, now_ms)
        if spec.venue_id == Venue.GATE:
            result = _parse_gate_position(raw, venue_sym, now_ms, contract_size=spec.contract_size)
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
    # Account balance snapshot (entry admission)
    # ------------------------------------------------------------------

    async def fetch_account_balance_snapshot(self) -> Optional[AccountBalanceSnapshot]:
        """Fetch available account balance for entry admission checks."""
        spec = self._spec
        if self.mode != "live":
            return None

        now_ms = int(time.time() * 1000)
        try:
            if spec.venue_id == Venue.HYPERLIQUID:
                account_address = self._credential.account_address if self._credential else ""
                if not account_address:
                    return None
                method, path, body, private = self._hyperliquid_info_operation_request(
                    VenueOperation.POSITION,
                    account_address=account_address,
                )
                raw = await self._request(
                    method,
                    path,
                    body=body,
                    private=private,
                )
                if not isinstance(raw, dict):
                    return None
                withdrawable = _parse_optional_float(raw.get("withdrawable"))
                if withdrawable is None:
                    return None
                cross = raw.get("crossMarginSummary")
                margin = raw.get("marginSummary")
                account_value = None
                if isinstance(cross, dict):
                    account_value = _parse_optional_float(cross.get("accountValue"))
                if account_value is None and isinstance(margin, dict):
                    account_value = _parse_optional_float(margin.get("accountValue"))
                locked = 0.0
                if account_value is not None:
                    locked = max(account_value - withdrawable, 0.0)
                balance_classification = (
                    "margin_view_available"
                    if withdrawable > 1e-9
                    else "margin_view_zero"
                )
                user_abstraction = ""
                spot_usdc_available = None
                if withdrawable <= 1e-9:
                    method, path, body, private = self._hyperliquid_info_operation_request(
                        VenueOperation.USER_ABSTRACTION,
                        account_address=account_address,
                    )
                    abstraction = await self._request(
                        method,
                        path,
                        body=body,
                        private=private,
                    )
                    user_abstraction = str(abstraction or "")
                    if abstraction == "unifiedAccount":
                        method, path, body, private = self._hyperliquid_info_operation_request(
                            VenueOperation.SPOT_CLEARINGHOUSE_STATE,
                            account_address=account_address,
                        )
                        spot_raw = await self._request(
                            method,
                            path,
                            body=body,
                            private=private,
                        )
                        spot_available = _hyperliquid_spot_usdc_available(spot_raw)
                        if spot_available is not None:
                            available, held = spot_available
                            spot_usdc_available = available
                            if available > withdrawable:
                                withdrawable = available
                                locked = held
                                balance_classification = "unified_collateral_available"
                return AccountBalanceSnapshot(
                    venue=Venue.HYPERLIQUID,
                    asset="USDC",
                    free=max(withdrawable, 0.0),
                    locked=locked,
                    observed_at_ms=now_ms,
                    balance_classification=balance_classification,
                    user_abstraction=user_abstraction,
                    spot_usdc_available=spot_usdc_available,
                )
            return None
        except TransportError:
            raise
        except Exception as e:
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                f"account balance snapshot failed: {e}",
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
                equity = _parse_optional_float(raw.get("totalMarginBalance"))
                if equity is None:
                    return None
                maint = _parse_optional_float(raw.get("totalMaintMargin"))
                if maint is None or maint <= 0.0:
                    return None
                snapshot = ARS(
                    venue=spec.venue_id,
                    equity_quote=equity,
                    maintenance_margin_quote=maint,
                    health_ratio=equity / maint,
                    observed_at_ms=now_ms,
                    source="fapi_account",
                )
                snapshot.available_balance_quote = _parse_optional_float(raw.get("availableBalance"))
                return snapshot

            elif spec.venue_id == Venue.OKX:
                raw = await self._request("GET", spec.account_risk_path, private=True)
                data_list = raw.get("data", [])
                if not data_list:
                    return None
                row = data_list[0] if isinstance(data_list, list) else data_list
                equity = _parse_optional_float(row.get("totalEq"))
                if equity is None or equity <= 0.0:
                    return None
                maint = _parse_optional_float(row.get("mmr"))
                if maint is None or maint <= 0.0:
                    return None
                snapshot = ARS(
                    venue=spec.venue_id,
                    equity_quote=equity,
                    maintenance_margin_quote=maint,
                    health_ratio=equity / maint,
                    observed_at_ms=now_ms,
                    source="okx_account_balance",
                )
                snapshot.available_balance_quote = _parse_optional_float(row.get("availEq"))
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
                equity = _parse_optional_float(row.get("totalEquity"))
                if equity is None:
                    return None
                maint = _parse_optional_float(row.get("totalMaintenanceMargin"))
                if maint is not None and maint > 0.0:
                    snapshot = ARS(
                        venue=spec.venue_id,
                        equity_quote=equity,
                        maintenance_margin_quote=maint,
                        health_ratio=equity / maint,
                        observed_at_ms=now_ms,
                        source="bybit_wallet_balance",
                    )
                    snapshot.available_balance_quote = _parse_optional_float(row.get("totalAvailableBalance"))
                    return snapshot

                # In Bybit ISOLATED_MARGIN, account-wide maintenance fields are
                # not applicable.  Only treat that as normal after private
                # position truth proves every derivative category is flat.
                account_info = await self._request(
                    "GET", "/v5/account/info", private=True,
                )
                account_result = account_info.get("result", {}) if isinstance(account_info, dict) else {}
                margin_mode = str(
                    account_result.get("marginMode", "")
                    if isinstance(account_result, dict)
                    else ""
                ).upper()
                if margin_mode != "ISOLATED_MARGIN":
                    return None

                async def bybit_position_rows(
                    category: str,
                    *,
                    settle_coin: str | None = None,
                ) -> list[dict[str, Any]] | None:
                    """Read every Bybit position page or fail closed."""
                    rows: list[dict[str, Any]] = []
                    cursor = ""
                    seen_cursors: set[str] = set()
                    for _ in range(100):
                        params: dict[str, Any] = {"category": category, "limit": 200}
                        if settle_coin:
                            params["settleCoin"] = settle_coin
                        if cursor:
                            params["cursor"] = cursor
                        positions_raw = await self._request(
                            "GET", "/v5/position/list", params=params, private=True,
                        )
                        positions_result = (
                            positions_raw.get("result", {})
                            if isinstance(positions_raw, dict)
                            else {}
                        )
                        page_rows = (
                            positions_result.get("list", [])
                            if isinstance(positions_result, dict)
                            else None
                        )
                        if not isinstance(page_rows, list) or not all(
                            isinstance(position_row, dict) for position_row in page_rows
                        ):
                            return None
                        rows.extend(page_rows)
                        next_cursor = str(
                            positions_result.get("nextPageCursor", "")
                        )
                        if not next_cursor:
                            return rows
                        if next_cursor in seen_cursors:
                            return None
                        seen_cursors.add(next_cursor)
                        cursor = next_cursor
                    return None

                # A USDT-linear-only probe cannot establish account-wide
                # normality.  Query all documented derivative categories and
                # paginate each one before accepting zero maintenance margin.
                position_rows: list[dict[str, Any]] = []
                for category, settle_coin in (
                    ("linear", "USDT"),
                    ("linear", "USDC"),
                    ("inverse", None),
                    ("option", None),
                ):
                    category_rows = await bybit_position_rows(
                        category, settle_coin=settle_coin,
                    )
                    if category_rows is None:
                        return None
                    position_rows.extend(category_rows)
                open_position_margins: list[float] = []
                for position_row in position_rows:
                    size = _parse_optional_float(position_row.get("size"))
                    if size is None:
                        return None
                    if abs(size) <= 1e-12:
                        continue
                    position_mm = _parse_optional_float(position_row.get("positionMM"))
                    if position_mm is None or position_mm <= 0.0:
                        # A live isolated position without numeric per-position
                        # MM remains V1's unsupported/degraded condition.
                        return None
                    open_position_margins.append(position_mm)

                if open_position_margins:
                    maint = sum(open_position_margins)
                    snapshot = ARS(
                        venue=spec.venue_id,
                        equity_quote=equity,
                        maintenance_margin_quote=maint,
                        health_ratio=equity / maint,
                        observed_at_ms=now_ms,
                        source="bybit_isolated_all_derivative_position_mm",
                    )
                else:
                    snapshot = ARS(
                        venue=spec.venue_id,
                        equity_quote=equity,
                        maintenance_margin_quote=0.0,
                        health_ratio=0.0,
                        observed_at_ms=now_ms,
                        source="bybit_isolated_all_derivative_position_truth",
                        zero_maintenance_is_normal=True,
                    )
                snapshot.available_balance_quote = _parse_optional_float(row.get("totalAvailableBalance"))
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
                        parsed = _parse_optional_float(row[key])
                        if parsed is not None:
                            maint = parsed
                            break
                if maint is None or maint <= 0.0:
                    return None
                equity = None
                for key in ("usdtEquity", "equity", "accountEquity"):
                    if key in row:
                        parsed = _parse_optional_float(row[key])
                        if parsed is not None:
                            equity = parsed
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
                        parsed = _parse_optional_float(row[key])
                        if parsed is not None:
                            avail = parsed
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
                        parsed = _parse_optional_float(raw[key])
                        if parsed is not None:
                            maint = parsed
                            break
                if maint is None or maint <= 0.0:
                    return None
                equity = None
                for key in ("total", "equity", "total_balance"):
                    if key in raw:
                        parsed = _parse_optional_float(raw[key])
                        if parsed is not None:
                            equity = parsed
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
                        parsed = _parse_optional_float(raw[key])
                        if parsed is not None:
                            avail = parsed
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

        self._reject_hyperliquid_trading_if_disabled()

        preflight: dict[str, Any] | None = None
        result_recorded = False
        hl_asset_meta: dict[str, int] | None = None

        try:
            if spec.venue_id == Venue.HYPERLIQUID:
                from lightfee.venues.hyperliquid_signing import (
                    hyperliquid_ioc_price_and_size,
                )

                hl_asset_meta = (
                    await self._hl_resolve_asset_meta(venue_sym)
                    if self.mode == "live"
                    else {"asset_index": 0, "sz_decimals": 0, "price_decimals": 6}
                )
                reference_price = 0.0
                reference_price_source = "none"
                for source, candidate in (
                    ("price_hint", request.price_hint),
                    ("mark_price_hint", request.mark_price_hint),
                    ("price", request.price),
                ):
                    try:
                        candidate_f = float(candidate or 0.0)
                    except (TypeError, ValueError):
                        candidate_f = 0.0
                    if math.isfinite(candidate_f) and candidate_f > 0.0:
                        reference_price = candidate_f
                        reference_price_source = source
                        break
                if reference_price <= 0.0:
                    fallback_diag: dict[str, Any] = {
                        "venue": spec.venue_id.value,
                        "symbol": venue_sym,
                        "side": request.side.value,
                        "raw_qty": request.quantity,
                        "raw_price": 0.0,
                        "reference_price_source": "l2_snapshot_unavailable",
                        "quantity_step": 10 ** (-hl_asset_meta["sz_decimals"]),
                        "tick_size": 0.0,
                        "min_qty": 0.0,
                        "min_notional": 0.0,
                        "rule_source": "hyperliquid_meta",
                        "asset_index": hl_asset_meta["asset_index"],
                        "sz_decimals": hl_asset_meta["sz_decimals"],
                        "price_decimals": hl_asset_meta["price_decimals"],
                    }
                    try:
                        snapshot = await self.fetch_l2_snapshot(
                            request.symbol,
                            depth=20,
                            retry_initial_ms=0,
                            retry_max_ms=0,
                            max_retries=0,
                        )
                    except Exception as exc:
                        preflight = fallback_diag
                        preflight["snapshot_error"] = str(exc)[:300]
                        raise OrderSubmitError(
                            SubmitFailureClass.REJECTED,
                            "Hyperliquid IOC order requires positive reference "
                            "price; l2Book fallback unavailable",
                        ) from exc

                    best_bid = (
                        float(snapshot.bids[0].price)
                        if getattr(snapshot, "bids", None)
                        else 0.0
                    )
                    best_ask = (
                        float(snapshot.asks[0].price)
                        if getattr(snapshot, "asks", None)
                        else 0.0
                    )
                    reference_price = best_ask if request.side == Side.BUY else best_bid
                    reference_price_source = (
                        "l2_snapshot_best_ask"
                        if request.side == Side.BUY
                        else "l2_snapshot_best_bid"
                    )
                    if (
                        not math.isfinite(reference_price)
                        or reference_price <= 0.0
                    ):
                        preflight = fallback_diag
                        preflight["reference_price_source"] = reference_price_source
                        preflight["best_bid"] = best_bid
                        preflight["best_ask"] = best_ask
                        raise OrderSubmitError(
                            SubmitFailureClass.REJECTED,
                            "Hyperliquid IOC order requires positive reference "
                            "price; l2Book side is empty",
                        )
                if reference_price <= 0.0:
                    raise OrderSubmitError(
                        SubmitFailureClass.REJECTED,
                        "Hyperliquid IOC order requires positive reference price",
                    )
                limit_px, wire_qty = hyperliquid_ioc_price_and_size(
                    side_is_buy=request.side == Side.BUY,
                    quantity=request.quantity,
                    reference_price=reference_price,
                    sz_decimals=hl_asset_meta["sz_decimals"],
                    price_decimals=hl_asset_meta["price_decimals"],
                )
                preflight = {
                    "venue": spec.venue_id.value,
                    "symbol": venue_sym,
                    "side": request.side.value,
                    "raw_qty": request.quantity,
                    "raw_price": reference_price,
                    "reference_price_source": reference_price_source,
                    "quantized_qty": wire_qty,
                    "quantized_price": limit_px,
                    "quantity_step": 10 ** (-hl_asset_meta["sz_decimals"]),
                    "tick_size": 0.0,
                    "min_qty": 0.0,
                    "min_notional": 0.0,
                    "rule_source": "hyperliquid_meta",
                    "asset_index": hl_asset_meta["asset_index"],
                    "sz_decimals": hl_asset_meta["sz_decimals"],
                    "price_decimals": hl_asset_meta["price_decimals"],
                }
                if wire_qty <= 0.0 or limit_px <= 0.0:
                    raise OrderSubmitError(
                        SubmitFailureClass.REJECTED,
                        "Hyperliquid normalized IOC order has zero quantity or price",
                    )
                request = replace(request, quantity=wire_qty, price=limit_px)
            else:
                symbol_rule = None
                if spec.venue_id == Venue.BYBIT:
                    try:
                        symbol_rule = await get_symbol_rules_cache().get(
                            self, spec.venue_id, venue_sym,
                        )
                    except Exception:
                        symbol_rule = None
                preflight = self.preflight_order_request(request, symbol_rule=symbol_rule)
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
                fapi_hedge_mode = await self._refresh_fapi_position_mode()
                body["type"] = "MARKET"
                if fapi_hedge_mode:
                    body["positionSide"] = self._fapi_position_side(
                        request.side, request.reduce_only
                    )
                elif request.reduce_only:
                    body["reduceOnly"] = "true"
                if request.price is not None and request.time_in_force != TimeInForce.IOC:
                    body["type"] = "LIMIT"
                    body["price"] = _format_decimal(request.price)
                    body["timeInForce"] = "GTC"
                # V1: Binance/Aster newClientOrderId max 36 chars (FAPI constraint)
                if request.client_order_id:
                    body["newClientOrderId"] = request.client_order_id[:36]
                preflight["position_mode"] = (
                    "hedge" if fapi_hedge_mode else "one_way"
                )
                if "positionSide" in body:
                    preflight["position_side"] = body["positionSide"]
            elif spec.venue_id == Venue.OKX:
                # V1: refresh posMode on first order (lazy, cached thereafter)
                if getattr(self, '_pos_mode_cache', None) is None:
                    await self._refresh_okx_pos_mode()
                pos_side = self._okx_pos_side(request.side, request.reduce_only)

                # V1: OKX order quantities are always canonical base size at the
                # engine boundary; wire sz is contracts = base_qty / ctVal.
                rules_cache = get_symbol_rules_cache()
                symbol_rule = await rules_cache.get(self, spec.venue_id, venue_sym)
                ct_val = float(getattr(symbol_rule, 'ct_val', 0) or 0)
                lot_sz = float(getattr(symbol_rule, 'qty_step', 0) or 0)
                min_sz = float(getattr(symbol_rule, 'min_qty', 0) or 0)
                max_mkt_sz = float(getattr(symbol_rule, 'max_market_qty', 0) or 0)
                base_qty = float(request.quantity)
                okx_sizing = _okx_contract_order_diagnostics(
                    base_qty=base_qty,
                    ct_val=ct_val,
                    lot_sz=lot_sz,
                    min_sz=min_sz,
                    max_mkt_sz=max_mkt_sz,
                )
                preflight.update(okx_sizing)
                contract_qty = float(okx_sizing["contract_qty"])
                reject_reason = okx_sizing.get("reject_reason")
                if reject_reason:
                    preflight["response_classification"] = "rejected"
                    self._record_order_diagnostic("order.submit_result", preflight)
                    result_recorded = True
                    raise OrderSubmitError(
                        SubmitFailureClass.REJECTED,
                        str(reject_reason),
                    )

                body = {
                    "instId": venue_sym,
                    "tdMode": "cross",
                    "side": request.side.value.lower(),
                    "posSide": pos_side,
                    "ordType": "market",
                    "sz": _format_decimal(contract_qty),
                }
                if request.price is not None and request.time_in_force != TimeInForce.IOC:
                    body["ordType"] = "limit"
                    body["px"] = _format_decimal(request.price)
                if request.client_order_id:
                    body["clOrdId"] = request.client_order_id
                if request.reduce_only:
                    body["reduceOnly"] = "true"
                # Enrich diagnostic with OKX-specific evidence
                preflight["pos_side"] = pos_side
                preflight["pos_mode"] = self._okx_pos_mode
                preflight["contract_qty"] = contract_qty
                preflight["body_field_names"] = sorted(body.keys())
                preflight["body_sanitized"] = {
                    k: v for k, v in body.items()
                    if k not in ("clOrdId",)
                }
            elif spec.venue_id == Venue.BYBIT:
                body = _build_bybit_order_body(
                    request, venue_sym, passive=False,
                    hedge_mode=self._hedge_mode,
                )
                if "qty" in body:
                    body["qty"] = _format_decimal(request.quantity)
                if (
                    request.price is not None
                    and request.time_in_force != TimeInForce.IOC
                ):
                    body["price"] = _format_decimal(request.price)
            elif spec.venue_id == Venue.BITGET:
                body["marginCoin"] = "USDT"
                body["orderType"] = "market"
                if request.reduce_only:
                    body["reduceOnly"] = "true"
            elif spec.venue_id == Venue.GATE:
                signed_size = int(request.quantity)
                if request.side == Side.SELL:
                    signed_size = -signed_size
                gate_ioc = request.time_in_force == TimeInForce.IOC
                body = {
                    "contract": venue_sym,
                    "size": signed_size,
                    "price": (
                        _format_decimal(request.price)
                        if request.price is not None and not gate_ioc
                        else "0"
                    ),
                    "tif": "gtc" if request.price is not None and not gate_ioc else "ioc",
                }
                if request.reduce_only:
                    body["reduce_only"] = True
            elif spec.venue_id == Venue.HYPERLIQUID:
                is_buy = request.side == Side.BUY
                tif = "Ioc"

                if self.mode == "live":
                    from lightfee.venues.hyperliquid_signing import (
                        build_hyperliquid_exchange_payload,
                        build_hyperliquid_order_action,
                        hyperliquid_cloid_for_client_order,
                    )

                    if hl_asset_meta is None:
                        hl_asset_meta = await self._hl_resolve_asset_meta(venue_sym)
                    wire_cloid = (
                        hyperliquid_cloid_for_client_order(request.client_order_id)
                        if request.client_order_id
                        else None
                    )
                    action = build_hyperliquid_order_action(
                        symbol=venue_sym,
                        is_buy=is_buy,
                        quantity=request.quantity,
                        price=float(request.price or 0.0),
                        reduce_only=request.reduce_only,
                        tif=tif,
                        cloid=wire_cloid,
                        asset_index=hl_asset_meta["asset_index"],
                        sz_decimals=hl_asset_meta["sz_decimals"],
                        price_decimals=hl_asset_meta["price_decimals"],
                    )

                    body = build_hyperliquid_exchange_payload(
                        action=action,
                        private_key_hex=(
                            self._credential.wallet_private_key
                            if self._credential else ""
                        ),
                        vault_address=None,
                        is_mainnet=True,
                    )
                else:
                    from lightfee.venues.hyperliquid_signing import float_to_wire_string

                    paper_order: dict[str, Any] = {
                        "a": 0,  # asset index — placeholder for paper
                        "b": is_buy,
                        "p": float_to_wire_string(float(request.price or 0.0)),
                        "s": float_to_wire_string(request.quantity),
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

            if "body_field_names" not in preflight:
                preflight["body_field_names"] = sorted(body.keys())
            if "body_sanitized" not in preflight:
                preflight["body_sanitized"] = {
                    k: v for k, v in body.items()
                    if k not in (
                        "clOrdId",
                        "orderLinkId",
                        "newClientOrderId",
                        "clientOrderId",
                        "cloid",
                    )
                }
            self._record_order_diagnostic("order.submit_attempt", preflight)

            contract = get_operation_contract(spec, VenueOperation.CREATE_ORDER)
            if not contract.supported:
                raise TransportError(
                    TransportErrorCategory.UNSUPPORTED_CAPABILITY,
                    f"{spec.venue_id.value} create order contract unsupported",
                )
            if contract.payload == "params":
                raw = await self._request(
                    contract.method,
                    contract.path,
                    params=body,
                    private=contract.private,
                )
            else:
                raw = await self._request(
                    contract.method,
                    contract.path,
                    body=body,
                    private=contract.private,
                )

            # V2: venue-specific success guard before parsing
            if spec.venue_id == Venue.BYBIT:
                _require_bybit_success(raw, "bybit order failed")
            elif spec.venue_id == Venue.BITGET:
                _require_bitget_success(raw, "bitget order failed")
            elif spec.venue_id == Venue.ASTER:
                _require_aster_success(raw, "aster order failed")

            try:
                okx_contract_size = None
                if spec.venue_id == Venue.OKX and preflight is not None:
                    candidate_ct_val = float(preflight.get("ct_val") or 0.0)
                    if candidate_ct_val > 0.0:
                        okx_contract_size = candidate_ct_val
                if spec.venue_id == Venue.OKX:
                    fill = self._parse_order_fill(
                        raw,
                        request,
                        venue_sym,
                        now_ms,
                        contract_size_override=okx_contract_size,
                    )
                else:
                    fill = self._parse_order_fill(raw, request, venue_sym, now_ms)
            except OrderSubmitError as exc:
                order_id, client_order_id = self._extract_order_identifiers(raw)
                if (
                    spec.venue_id == Venue.OKX
                    and exc.class_ == SubmitFailureClass.UNCERTAIN
                    and order_id
                ):
                    fill = await self._okx_ack_fill_from_preflight(
                        request=request,
                        venue_sym=venue_sym,
                        preflight=preflight,
                        order_id=order_id,
                        client_order_id=client_order_id or request.client_order_id or "",
                        now_ms=now_ms,
                    )
                    result_payload = dict(preflight)
                    result_payload["order_id"] = fill.order_id
                    result_payload["client_order_id"] = (
                        fill.client_order_id or request.client_order_id or ""
                    )
                    result_payload["response_classification"] = "ack_accepted"
                    self._record_order_diagnostic("order.submit_result", result_payload)
                    result_recorded = True
                    return fill
                result_payload = dict(preflight)
                result_payload["order_id"] = order_id
                result_payload["client_order_id"] = client_order_id or request.client_order_id or ""
                result_payload["response_classification"] = (
                    "ack_accepted" if exc.class_ == SubmitFailureClass.UNCERTAIN and order_id
                    else exc.class_.value
                )
                self._record_order_diagnostic("order.submit_result", result_payload)
                result_recorded = True
                raise
            result_payload = dict(preflight)
            result_payload["order_id"] = fill.order_id
            result_payload["client_order_id"] = fill.client_order_id or request.client_order_id or ""
            result_payload["response_classification"] = "filled"
            self._record_order_diagnostic("order.submit_result", result_payload)
            result_recorded = True
            return fill

        except TransportError as e:
            if (
                spec.venue_id == Venue.HYPERLIQUID
                and is_hyperliquid_non_retryable_auth_signing_error(e)
            ):
                self._trading_capability_trusted = False
            if preflight is not None and not result_recorded:
                result_payload = dict(preflight)
                result_payload["response_code"] = e.status_code
                result_payload["response_msg"] = str(e)[:500]
                result_payload["response_body"] = e.body[:1000]
                result_payload["response_classification"] = (
                    "rejected"
                    if e.category in (
                        TransportErrorCategory.AUTH_FAILURE,
                        TransportErrorCategory.AUTHORIZATION_FAILURE,
                        TransportErrorCategory.REQUEST_REJECTED,
                        TransportErrorCategory.UNSUPPORTED_CAPABILITY,
                        TransportErrorCategory.NORMALIZATION_FAILURE,
                    )
                    else "uncertain"
                )
                self._record_order_diagnostic("order.submit_result", result_payload)
            raise _map_to_submit_error(e.category, str(e), transport_error=e)
        except OrderSubmitError as e:
            if (
                spec.venue_id == Venue.HYPERLIQUID
                and is_hyperliquid_non_retryable_auth_signing_error(e)
            ):
                self._trading_capability_trusted = False
            if preflight is not None and not result_recorded:
                result_payload = dict(preflight)
                result_payload["response_classification"] = e.class_.value
                result_payload["response_msg"] = str(e)[:500]
                self._record_order_diagnostic("order.submit_result", result_payload)
            raise
        except Exception as e:
            raise OrderSubmitError(SubmitFailureClass.UNCERTAIN, str(e))

    async def _okx_ack_fill_from_preflight(
        self,
        *,
        request: OrderRequest,
        venue_sym: str,
        preflight: dict[str, Any],
        order_id: str,
        client_order_id: str,
        now_ms: int,
    ) -> OrderFill:
        """V1-compatible OKX market-order ack projection.

        OKX REST acks often contain only ordId/clOrdId. Rust V1 treats an
        accepted market order as the submitted base quantity and lets later
        position reconciliation prove any residual. Keep the stricter
        uncertain behavior for other venues.
        """
        contract_qty = float(preflight.get("contract_qty") or 0.0)
        ct_val = float(preflight.get("ct_val") or 0.0)
        quantity = (
            contract_qty * ct_val
            if contract_qty > 0 and ct_val > 0
            else float(request.quantity)
        )

        price = float(
            request.price_hint
            or request.mark_price_hint
            or request.price
            or 0.0
        )
        if price <= 0:
            try:
                snapshot = await self.fetch_market_snapshot([request.symbol])
                for quote in snapshot.quotes:
                    if quote.symbol not in (request.symbol, venue_sym):
                        continue
                    candidate = quote.ask if request.side == Side.BUY else quote.bid
                    if candidate > 0:
                        price = float(candidate)
                        break
            except Exception:
                price = 0.0

        return OrderFill(
            venue=Venue.OKX,
            symbol=venue_sym,
            side=request.side,
            quantity=quantity,
            price=price,
            order_id=order_id,
            client_order_id=client_order_id or request.client_order_id,
            filled_at_ms=now_ms,
        )

    def _parse_order_fill(
        self,
        raw: dict[str, Any],
        request: OrderRequest,
        venue_sym: str,
        now_ms: int,
        *,
        contract_size_override: float | None = None,
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
            if isinstance(status_entry, str):
                raise OrderSubmitError(SubmitFailureClass.REJECTED, status_entry)
            if not isinstance(status_entry, dict):
                raise OrderSubmitError(
                    SubmitFailureClass.UNCERTAIN,
                    f"Hyperliquid unknown order status: {status_entry!r}",
                )
            if "error" in status_entry:
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    str(status_entry.get("error", "")),
                )
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
            err = OrderSubmitError(
                SubmitFailureClass.UNCERTAIN,
                f"order accepted (id={order_id}) but fill not confirmed — "
                "no executedQty/cumQty/fillSz in response",
            )
            missing_fill_fields = [
                "executedQty",
                "cumExecQty",
                "cumQty",
                "fillSz",
                "filledQty",
                "filled_size",
            ]
            accepted_client_order_id = str(
                data.get(
                    "orderLinkId",
                    data.get(
                        "clientOrderId",
                        data.get("clOrdId", request.client_order_id),
                    ),
                )
                or request.client_order_id
                or ""
            )
            err.order_ack_only = True
            err.accepted_order_id = order_id
            err.accepted_client_order_id = accepted_client_order_id
            err.fill_confirmation_missing_fields = missing_fill_fields
            err.exchange_response_body = json.dumps(raw, separators=(",", ":"))
            raise err
        else:
            raise OrderSubmitError(
                SubmitFailureClass.UNCERTAIN,
                "order response contains no order id and no fill data",
            )
        if spec.venue_id == Venue.OKX:
            exec_qty = self._okx_contracts_to_base_quantity(
                exec_qty,
                contract_size_override,
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
    def _okx_contracts_to_base_quantity(
        contract_quantity: float,
        contract_size: float | None,
    ) -> float:
        ct_val = float(contract_size or 0.0)
        quantity = abs(float(contract_quantity or 0.0))
        return quantity * ct_val if ct_val > 0.0 else quantity

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

        Bitget uses the resolved account-family ORDER_STATUS contract with
        orderId/clientOid, then applies multi-key fallback for price/fee.

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

            elif spec.venue_id == Venue.BINANCE:
                return await self._fetch_order_status_binance(
                    venue_sym, order_id, client_order_id, now_ms,
                )

            elif spec.venue_id == Venue.OKX:
                return await self._fetch_order_status_okx(
                    venue_sym, order_id, client_order_id, now_ms,
                )

            elif spec.venue_id == Venue.BITGET:
                query_params: dict[str, Any] = {}
                if order_id:
                    query_params["orderId"] = order_id
                if client_order_id:
                    query_params["clientOid"] = client_order_id
                family = await _resolve_bitget_contract_family_for_truth(self)
                contract = get_operation_contract(
                    spec,
                    VenueOperation.ORDER_STATUS,
                    resolved_account_family=family,
                )
                params = _bitget_contract_params(
                    contract,
                    venue_sym=venue_sym,
                    extra=query_params,
                )
                raw = await self._request(
                    contract.method,
                    contract.path,
                    params=params,
                    private=contract.private,
                )
                return self._parse_order_status_bitget(
                    raw,
                    venue_sym,
                    now_ms,
                    resolved_account_family=family,
                    queried_endpoint=contract.path,
                )

        except TransportError as e:
            if e.category == TransportErrorCategory.REQUEST_REJECTED:
                raise
            self._record_order_reconcile_query(
                symbol=venue_sym,
                order_id=order_id,
                client_order_id=client_order_id,
                queried_endpoints=["fetch_order_status"],
                response_classification=f"transport_error:{e.category.value}",
                uncertain_subtype="submit_timeout",
            )
            return None
        except _BybitOrderNotFound:
            return None

        return None

    async def _fetch_order_status_binance(
        self,
        venue_sym: str,
        order_id: str,
        client_order_id: str,
        now_ms: int,
    ) -> Optional["OrderFillReconciliation"]:
        if not order_id and not client_order_id:
            self._record_order_reconcile_query(
                symbol=venue_sym,
                queried_endpoints=["/fapi/v1/order"],
                response_classification="missing_order_identifier",
                uncertain_subtype="execution_not_found",
            )
            return None

        params: dict[str, Any] = {"symbol": venue_sym}
        order_id_text = str(order_id or "").strip()
        client_order_id_text = str(client_order_id or "").strip()
        if order_id_text and order_id_text.isdigit():
            params["orderId"] = order_id_text
        elif client_order_id_text:
            params["origClientOrderId"] = client_order_id_text
        elif (
            order_id_text
            and (
                len(order_id_text) > 36
                or "-recovery-" in order_id_text.lower()
            )
        ):
            self._record_order_reconcile_query(
                symbol=venue_sym,
                order_id=order_id,
                client_order_id=client_order_id,
                queried_endpoints=["/fapi/v1/order"],
                response_classification="invalid_local_order_identifier",
                uncertain_subtype="invalid_local_order_identifier",
                next_action="check_live_position",
            )
            return None
        else:
            params["origClientOrderId"] = order_id_text
        try:
            raw = await self._request(
                "GET",
                "/fapi/v1/order",
                params=params,
                private=True,
            )
        except TransportError as error:
            # Binance returns a normal missing historical order as HTTP 400.
            # Keep the transport classification for all other rejected
            # requests, but let reconciliation treat -2013/-2011 as no fill
            # and establish live-position truth on the next step, as V1 does.
            raw_error: dict[str, Any] = {}
            try:
                parsed_error = json.loads(error.body)
                if isinstance(parsed_error, dict):
                    raw_error = parsed_error
            except (TypeError, ValueError):
                pass
            code = raw_error.get("code")
            if (
                error.category == TransportErrorCategory.REQUEST_REJECTED
                and error.status_code == 400
                and str(code) in ("-2011", "-2013")
            ):
                msg = str(raw_error.get("msg", ""))
                self._record_order_reconcile_query(
                    symbol=venue_sym,
                    order_id=order_id,
                    client_order_id=client_order_id,
                    queried_endpoints=["/fapi/v1/order"],
                    response_classification=f"binance_error_{code}:{msg}",
                    uncertain_subtype="open_order_not_found",
                    next_action="check_live_position",
                )
                return None
            raise

        code = raw.get("code") if isinstance(raw, dict) else None
        if code is not None and str(code).lstrip("-").isdigit() and int(code) < 0:
            msg = str(raw.get("msg", ""))
            subtype = "open_order_not_found" if str(code) in ("-2011", "-2013") else "execution_not_found"
            self._record_order_reconcile_query(
                symbol=venue_sym,
                order_id=order_id,
                client_order_id=client_order_id,
                queried_endpoints=["/fapi/v1/order"],
                response_classification=f"binance_error_{code}:{msg}",
                uncertain_subtype=subtype,
            )
            return None

        result = self._parse_order_status_binance(raw, venue_sym, now_ms)
        classification = "filled" if result is not None else self._classify_binance_zero_fill(raw)
        subtype = "" if result is not None else (
            "stale_accepted_order"
            if classification == "stale_accepted_order"
            else "execution_not_found"
        )
        self._record_order_reconcile_query(
            symbol=venue_sym,
            order_id=str(raw.get("orderId", order_id)) if isinstance(raw, dict) else order_id,
            client_order_id=str(raw.get("clientOrderId", client_order_id)) if isinstance(raw, dict) else client_order_id,
            queried_endpoints=["/fapi/v1/order"],
            response_classification=classification,
            uncertain_subtype=subtype,
            next_action="clear_uncertain_state" if result is not None else "check_live_position",
        )
        return result

    @staticmethod
    def _classify_binance_zero_fill(raw: dict[str, Any]) -> str:
        status = str(raw.get("status", "")).upper() if isinstance(raw, dict) else ""
        if status in ("NEW", "PENDING_NEW", "PARTIALLY_FILLED"):
            return "stale_accepted_order"
        if status in ("CANCELED", "EXPIRED", "REJECTED"):
            return "closed_order_not_found"
        return "execution_not_found"

    def _parse_order_status_binance(
        self,
        raw: dict[str, Any],
        venue_sym: str,
        now_ms: int,
    ) -> Optional["OrderFillReconciliation"]:
        if not isinstance(raw, dict):
            return None
        qty = _safe_float(raw.get("executedQty", raw.get("cumQty", "0")))
        if qty <= 0.0:
            return None
        side_raw = str(raw.get("side", "")).upper()
        if side_raw == "BUY":
            side = Side.BUY
        elif side_raw == "SELL":
            side = Side.SELL
        else:
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                f"binance order status has invalid/missing side value {side_raw!r}",
            )
        avg_price = _safe_float(raw.get("avgPrice", raw.get("price", "0")))
        if avg_price <= 0.0:
            quote_qty = _safe_float(raw.get("cumQuote", raw.get("cumQuoteQty", "0")))
            if quote_qty > 0.0:
                avg_price = quote_qty / qty
        status = str(raw.get("status", ""))
        return OrderFillReconciliation(
            venue=Venue.BINANCE,
            symbol=venue_sym,
            side=side,
            quantity=qty,
            average_price=avg_price,
            order_id=str(raw.get("orderId", "")),
            client_order_id=str(raw.get("clientOrderId", "")) or None,
            filled_at_ms=int(raw.get("updateTime", raw.get("time", now_ms)) or now_ms),
            metadata={
                "raw_exchange_status": status,
                "queried_endpoints": ["/fapi/v1/order"],
                "response_classification": "filled",
            },
        )

    async def _fetch_order_status_okx(
        self,
        venue_sym: str,
        order_id: str,
        client_order_id: str,
        now_ms: int,
    ) -> Optional["OrderFillReconciliation"]:
        if not order_id and not client_order_id:
            self._record_order_reconcile_query(
                symbol=venue_sym,
                queried_endpoints=["/api/v5/trade/order"],
                response_classification="missing_order_identifier",
                uncertain_subtype="execution_not_found",
            )
            return None

        order_id_text = str(order_id or "").strip()
        client_order_id_text = str(client_order_id or "").strip()
        params: dict[str, Any] = {"instId": venue_sym}
        if order_id_text and order_id_text.isdigit():
            params["ordId"] = order_id_text
        elif client_order_id_text:
            params["clOrdId"] = client_order_id_text
        elif order_id_text:
            params["clOrdId"] = order_id_text

        endpoints: list[str] = ["/api/v5/trade/order"]
        open_raw = await self._request(
            "GET", "/api/v5/trade/order", params=params, private=True,
        )
        open_row = self._okx_order_status_row(open_raw)
        if open_row:
            open_result = await self._fetch_okx_trade_fills_reconciliation(
                venue_sym=venue_sym,
                order_row=open_row,
                fallback_order_id=order_id_text,
                fallback_client_order_id=client_order_id_text,
                now_ms=now_ms,
                endpoints=endpoints,
            )
            if open_result is not None:
                self._record_order_reconcile_query(
                    symbol=venue_sym,
                    order_id=open_result.order_id,
                    client_order_id=open_result.client_order_id or client_order_id,
                    queried_endpoints=list(
                        open_result.metadata.get("queried_endpoints", endpoints)
                    ),
                    response_classification="filled",
                    next_action="clear_uncertain_state",
                )
            return open_result

        history_params = dict(params)
        history_params["instType"] = "SWAP"
        endpoints.append("/api/v5/trade/orders-history")
        history_raw = await self._request(
            "GET", "/api/v5/trade/orders-history", params=history_params, private=True,
        )
        history_row = self._okx_order_status_row(history_raw)
        if history_row:
            history_result = await self._fetch_okx_trade_fills_reconciliation(
                venue_sym=venue_sym,
                order_row=history_row,
                fallback_order_id=order_id_text,
                fallback_client_order_id=client_order_id_text,
                now_ms=now_ms,
                endpoints=endpoints,
            )
            if history_result is None:
                return None
            self._record_order_reconcile_query(
                symbol=venue_sym,
                order_id=history_result.order_id,
                client_order_id=history_result.client_order_id or client_order_id,
                queried_endpoints=list(
                    history_result.metadata.get("queried_endpoints", endpoints)
                ),
                response_classification="filled",
                next_action="clear_uncertain_state",
            )
            return history_result

        self._record_order_reconcile_query(
            symbol=venue_sym,
            order_id=order_id,
            client_order_id=client_order_id,
            queried_endpoints=endpoints,
            response_classification="open_order_not_found;closed_order_not_found",
            uncertain_subtype="closed_order_not_found",
            next_action="check_live_position",
        )
        return None

    @staticmethod
    def _okx_order_status_row(raw: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(raw, dict) or str(raw.get("code", "0")) != "0":
            return {}
        data = raw.get("data", [])
        if isinstance(data, list):
            row = data[0] if data else {}
        elif isinstance(data, dict):
            row = data
        else:
            row = {}
        return row if isinstance(row, dict) else {}

    async def _fetch_okx_trade_fills_reconciliation(
        self,
        *,
        venue_sym: str,
        order_row: dict[str, Any],
        fallback_order_id: str,
        fallback_client_order_id: str,
        now_ms: int,
        endpoints: list[str],
    ) -> Optional["OrderFillReconciliation"]:
        resolved_order_id = str(order_row.get("ordId") or fallback_order_id or "").strip()
        resolved_client_id = str(
            order_row.get("clOrdId") or fallback_client_order_id or ""
        ).strip()
        if not resolved_order_id:
            self._record_order_reconcile_query(
                symbol=venue_sym,
                order_id=fallback_order_id,
                client_order_id=resolved_client_id,
                queried_endpoints=list(endpoints),
                response_classification="detail_found;missing_ord_id",
                uncertain_subtype="execution_not_found",
                next_action="check_live_position",
            )
            return None

        fill_endpoints = list(endpoints) + ["/api/v5/trade/fills"]
        fills_raw = await self._request(
            "GET",
            "/api/v5/trade/fills",
            params={
                "instType": "SWAP",
                "instId": venue_sym,
                "ordId": resolved_order_id,
            },
            private=True,
        )
        if not self._okx_trade_fills_has_quantity(fills_raw):
            self._record_order_reconcile_query(
                symbol=venue_sym,
                order_id=resolved_order_id,
                client_order_id=resolved_client_id,
                queried_endpoints=fill_endpoints,
                response_classification="detail_found;fills_empty",
                uncertain_subtype="execution_not_found",
                next_action="check_live_position",
            )
            return None

        contract_size = await self._okx_contract_size_for_venue_symbol(venue_sym)
        result = self._parse_okx_trade_fills(
            fills_raw,
            venue_sym=venue_sym,
            order_id=resolved_order_id,
            client_order_id=resolved_client_id,
            now_ms=now_ms,
            contract_size=contract_size,
            raw_exchange_status=str(order_row.get("state", "")),
            queried_endpoints=fill_endpoints,
        )
        if result is None:
            self._record_order_reconcile_query(
                symbol=venue_sym,
                order_id=resolved_order_id,
                client_order_id=resolved_client_id,
                queried_endpoints=fill_endpoints,
                response_classification="detail_found;fills_unparseable",
                uncertain_subtype="execution_not_found",
                next_action="check_live_position",
            )
        return result

    @staticmethod
    def _okx_order_status_has_fill_quantity(raw: dict[str, Any]) -> bool:
        if not isinstance(raw, dict) or str(raw.get("code", "0")) != "0":
            return False
        data = raw.get("data", [])
        if isinstance(data, list):
            row = data[0] if data else {}
        elif isinstance(data, dict):
            row = data
        else:
            row = {}
        if not isinstance(row, dict) or not row:
            return False
        return _safe_float(row.get("accFillSz", row.get("fillSz", "0"))) > 0.0

    def _parse_order_status_okx(
        self,
        raw: dict[str, Any],
        venue_sym: str,
        now_ms: int,
        *,
        contract_size: float | None = None,
    ) -> Optional["OrderFillReconciliation"]:
        if not isinstance(raw, dict):
            return None
        if str(raw.get("code", "0")) != "0":
            return None
        data = raw.get("data", [])
        if isinstance(data, list):
            row = data[0] if data else {}
        elif isinstance(data, dict):
            row = data
        else:
            row = {}
        if not isinstance(row, dict) or not row:
            return None
        contract_qty = _safe_float(row.get("accFillSz", row.get("fillSz", "0")))
        if contract_qty <= 0.0:
            return None
        qty = self._okx_contracts_to_base_quantity(contract_qty, contract_size)
        side_raw = str(row.get("side", "")).lower()
        if side_raw == "buy":
            side = Side.BUY
        elif side_raw == "sell":
            side = Side.SELL
        else:
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                f"okx order status has invalid/missing side value {side_raw!r}",
            )
        avg_price = _safe_float(row.get("avgPx", row.get("fillPx", "0")))
        return OrderFillReconciliation(
            venue=Venue.OKX,
            symbol=venue_sym,
            side=side,
            quantity=qty,
            average_price=avg_price,
            order_id=str(row.get("ordId", "")),
            client_order_id=str(row.get("clOrdId", "")) or None,
            fee_quote=abs(_safe_float(row.get("fee", "0"))) or None,
            filled_at_ms=int(row.get("uTime", row.get("fillTime", now_ms)) or now_ms),
            metadata={
                "raw_exchange_status": str(row.get("state", "")),
                "queried_endpoints": ["/api/v5/trade/order"],
                "response_classification": "filled",
                "evidence_source": "okx_order_detail",
                "quantity_units": "contracts_to_base",
                "contract_qty": contract_qty,
                "ct_val": float(contract_size or 0.0),
            },
        )

    @staticmethod
    def _okx_trade_fills_has_quantity(raw: dict[str, Any]) -> bool:
        if not isinstance(raw, dict) or str(raw.get("code", "0")) != "0":
            return False
        data = raw.get("data", [])
        rows = data if isinstance(data, list) else [data]
        return any(
            isinstance(row, dict)
            and _safe_float(row.get("fillSz", row.get("sz", "0"))) > 0.0
            for row in rows
        )

    def _parse_okx_trade_fills(
        self,
        raw: dict[str, Any],
        *,
        venue_sym: str,
        order_id: str,
        client_order_id: str,
        now_ms: int,
        contract_size: float,
        raw_exchange_status: str,
        queried_endpoints: list[str],
    ) -> Optional["OrderFillReconciliation"]:
        if not isinstance(raw, dict) or str(raw.get("code", "0")) != "0":
            return None
        data = raw.get("data", [])
        rows = data if isinstance(data, list) else [data]
        total_qty = 0.0
        total_contract_qty = 0.0
        weighted_notional = 0.0
        total_fee = 0.0
        latest_fill_ms = 0
        resolved_side: Side | None = None
        resolved_client_id = client_order_id
        for row in rows:
            if not isinstance(row, dict):
                continue
            contract_qty = _safe_float(row.get("fillSz", row.get("sz", "0")))
            if contract_qty <= 0.0:
                continue
            side_raw = str(row.get("side", "")).lower()
            if side_raw == "buy":
                side = Side.BUY
            elif side_raw == "sell":
                side = Side.SELL
            else:
                raise TransportError(
                    TransportErrorCategory.REQUEST_REJECTED,
                    f"okx trade fill has invalid/missing side value {side_raw!r}",
                )
            if resolved_side is None:
                resolved_side = side
            elif resolved_side != side:
                raise TransportError(
                    TransportErrorCategory.REQUEST_REJECTED,
                    "okx trade fills mixed sides for one order",
                )
            fill_qty = self._okx_contracts_to_base_quantity(contract_qty, contract_size)
            fill_price = _safe_float(row.get("fillPx", row.get("px", "0")))
            total_contract_qty += contract_qty
            total_qty += fill_qty
            weighted_notional += fill_qty * fill_price
            total_fee += abs(_safe_float(row.get("fee", "0")))
            fill_ms = int(row.get("ts", row.get("fillTime", now_ms)) or now_ms)
            latest_fill_ms = max(latest_fill_ms, fill_ms)
            resolved_client_id = str(row.get("clOrdId") or resolved_client_id or "")

        if total_qty <= 0.0 or resolved_side is None:
            return None
        return OrderFillReconciliation(
            venue=Venue.OKX,
            symbol=venue_sym,
            side=resolved_side,
            quantity=total_qty,
            average_price=weighted_notional / total_qty,
            order_id=order_id,
            client_order_id=resolved_client_id or None,
            fee_quote=total_fee if total_fee > 0.0 else None,
            filled_at_ms=latest_fill_ms if latest_fill_ms > 0 else now_ms,
            metadata={
                "evidence_source": "okx_trade_fills",
                "queried_endpoints": list(queried_endpoints),
                "raw_exchange_status": raw_exchange_status,
                "response_classification": "filled",
                "quantity_units": "contracts_to_base",
                "contract_qty": total_contract_qty,
                "ct_val": float(contract_size or 0.0),
                "live_position_confirmed": False,
            },
        )

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
        queried_endpoints: list[str] = []

        # Step 1: resolve orderId from client_order_id if needed.
        # V1 duplicate orderLinkId reconciliation must search open/realtime
        # state first, then closed order history, before aggregating executions.
        if not resolved_order_id:
            if client_order_id:
                lookup_specs = [
                    (
                        "/v5/order/realtime",
                        {
                            "category": "linear",
                            "symbol": venue_sym,
                            "openOnly": 0,
                            "orderLinkId": client_order_id,
                        },
                        "bybit order realtime",
                    ),
                    (
                        "/v5/order/history",
                        {
                            "category": "linear",
                            "symbol": venue_sym,
                            "orderLinkId": client_order_id,
                        },
                        "bybit order history",
                    ),
                ]
            else:
                self._record_order_reconcile_query(
                    symbol=venue_sym,
                    queried_endpoints=["/v5/order/realtime"],
                    response_classification="missing_order_identifier",
                    uncertain_subtype="execution_not_found",
                )
                return None  # need at least one identifier

            for path, params, context in lookup_specs:
                queried_endpoints.append(path)
                raw = await self._request(
                    "GET", path, params=params, private=True,
                )
                try:
                    self._require_bybit_reconciliation_success(raw, context)
                except _BybitOrderNotFound:
                    continue

                result = raw.get("result", {})
                data = (
                    result.get("list", [None])[0]
                    if isinstance(result, dict) and result.get("list")
                    else result
                )
                if not isinstance(data, dict):
                    continue

                resolved_order_id = str(data.get("orderId", ""))
                resolved_client_id = str(data.get("orderLinkId", ""))
                if resolved_order_id:
                    break

            if not resolved_order_id:
                self._record_order_reconcile_query(
                    symbol=venue_sym,
                    client_order_id=client_order_id,
                    queried_endpoints=queried_endpoints,
                    response_classification="open_order_not_found;closed_order_not_found",
                    uncertain_subtype="closed_order_not_found",
                    next_action="check_live_position",
                )
                return None

        # Step 2: query /v5/execution/list with resolved orderId
        exec_params = {
            "category": "linear",
            "symbol": venue_sym,
            "orderId": resolved_order_id,
        }
        queried_endpoints.append("/v5/execution/list")
        exec_raw = await self._request(
            "GET", "/v5/execution/list", params=exec_params, private=True,
        )
        # V1: check retCode for execution list too
        self._require_bybit_reconciliation_success(exec_raw, "bybit execution reconciliation")

        # Step 3: aggregate executions
        reconciliation = self._parse_bybit_execution_list(
            exec_raw, venue_sym, resolved_order_id, resolved_client_id, now_ms,
        )
        if reconciliation is not None:
            metadata = dict(reconciliation.metadata or {})
            metadata["queried_endpoints"] = list(queried_endpoints)
            metadata["response_classification"] = "filled"
            reconciliation = replace(reconciliation, metadata=metadata)
            self._record_order_reconcile_query(
                symbol=venue_sym,
                order_id=reconciliation.order_id,
                client_order_id=reconciliation.client_order_id or client_order_id,
                queried_endpoints=queried_endpoints,
                response_classification="filled",
                next_action="clear_uncertain_state",
            )
            return reconciliation

        self._record_order_reconcile_query(
            symbol=venue_sym,
            order_id=resolved_order_id,
            client_order_id=resolved_client_id or client_order_id,
            queried_endpoints=queried_endpoints,
            response_classification="execution_not_found",
            uncertain_subtype="execution_not_found",
            next_action="check_live_position",
        )
        return None

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
        self,
        raw: dict[str, Any],
        venue_sym: str,
        now_ms: int,
        *,
        resolved_account_family: object = None,
        queried_endpoint: str = "",
    ) -> Optional["OrderFillReconciliation"]:
        """Parse Bitget order-status response into OrderFillReconciliation.

        V1: quantity <= 0 → None. Multi-key fallback for price/fee/orderId/clientOid.
        """
        _require_bitget_success(raw, "bitget order status failed")
        data = raw.get("data", raw)
        if isinstance(data, list):
            data = data[0] if data else {}
        if not isinstance(data, dict):
            return None

        # V1: json_string(&row, &["orderId", "ordId"]).unwrap_or_else(|| order_id.to_string())
        # Note: caller provides fallback order_id
        order_id = str(data.get("orderId", data.get("ordId", "")))
        client_id = str(data.get("clientOid", ""))
        # V1 multi-key fallback: cumExecQty, baseVolume, filledQty, fillQty, filled_amount, size
        cum_qty = _safe_float(
            data.get("cumExecQty", data.get("baseVolume", data.get("filledQty", data.get("fillQty", data.get("filled_amount", data.get("size", data.get("fillSz", "0")))))))
        )
        avg_price = _safe_float(
            data.get("priceAvg", data.get("avgPrice", data.get("fillPriceAvg", data.get("averagePrice", "0"))))
        )
        filled_at = int(data.get("uTime", data.get("cTime", data.get("updateTime", data.get("filledTime", now_ms)))))

        # V1: quantity <= 0 → None
        if cum_qty <= 0.0:
            return None
        side_str = str(data.get("side", "") or "").lower()
        if side_str == "buy":
            side = Side.BUY
        elif side_str == "sell":
            side = Side.SELL
        else:
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                f"bitget order status has invalid/missing side value {side_str!r}",
            )

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

        family = (
            getattr(resolved_account_family, "value", None)
            or resolved_account_family
            or ""
        )
        raw_status = str(
            data.get(
                "status",
                data.get("orderStatus", data.get("state", "")),
            )
            or ""
        ).lower()
        if raw_status in {"", "full-fill", "full_fill", "filled"}:
            raw_status = "filled"

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
            metadata={
                "resolved_account_family": str(family),
                "side": side_str,
                "raw_exchange_status": raw_status,
                "response_classification": "filled",
                "evidence_source": "bitget_order_fill_truth",
                "queried_endpoints": [queried_endpoint] if queried_endpoint else [],
            },
        )

    # ------------------------------------------------------------------
    # Passive order contract (V1: GTC post-only maker order lifecycle)
    # ------------------------------------------------------------------

    async def _fetch_aster_remaining_openable_notional(
        self, venue_sym: str,
    ) -> Optional[float]:
        raw = await self._request(
            "GET",
            "/fapi/v1/remainingOpenableNotionalValue",
            params={
                "symbol": venue_sym,
                "leverage": ASTER_DEFAULT_REMAINING_OPENABLE_LEVERAGE,
            },
            private=True,
        )
        value = raw.get("remainingOpenableNotionalValue") if isinstance(raw, dict) else None
        remaining = _safe_float(value, default=-1.0)
        if math.isfinite(remaining) and remaining >= 0.0:
            return remaining
        return None

    async def _aster_apply_remaining_openable_headroom(
        self,
        request: OrderRequest,
        venue_sym: str,
        quantized_qty: float,
        quantized_price: Any,
        qty_step: float,
        min_qty: float,
    ) -> tuple[float, dict[str, Any]]:
        payload: dict[str, Any] = {}
        if self._spec.venue_id != Venue.ASTER or request.reduce_only:
            return quantized_qty, payload
        if quantized_qty <= 0.0:
            return quantized_qty, payload
        price = _safe_float(quantized_price, default=0.0)
        if price <= 0.0:
            return quantized_qty, payload

        try:
            remaining = await self._fetch_aster_remaining_openable_notional(venue_sym)
        except Exception as exc:
            payload["aster_headroom_error"] = str(exc)[:200]
            return quantized_qty, payload

        if remaining is None:
            return quantized_qty, payload

        max_qty = max(remaining / price, 0.0)
        adjusted_qty = min(quantized_qty, max_qty)
        if qty_step > 0:
            adjusted_qty = _floor_to_step(adjusted_qty, qty_step)
        payload.update({
            "aster_headroom_source": "remaining_openable_notional",
            "aster_remaining_openable_notional": remaining,
            "aster_requested_qty": quantized_qty,
            "aster_max_qty": max_qty,
        })
        if adjusted_qty + 1e-12 < quantized_qty:
            if adjusted_qty + 1e-12 < max(min_qty, 0.0):
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    "aster entry notional headroom exhausted: "
                    f"remaining_openable_notional={remaining} price={price}",
                )
            payload["aster_headroom_clamped"] = True
            return adjusted_qty, payload
        payload["aster_headroom_clamped"] = False
        return quantized_qty, payload

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

        self._reject_hyperliquid_trading_if_disabled()

        result_recorded = False

        try:
            # --- Preflight: fetch dynamic symbol rules + normalize ---
            symbol_rule = None
            if spec.venue_id == Venue.HYPERLIQUID:
                from lightfee.venues.hyperliquid_signing import (
                    round_to_decimals,
                    round_to_significant_and_decimal,
                )

                hl_meta = (
                    await self._hl_resolve_asset_meta(venue_sym)
                    if self.mode == "live"
                    else {"asset_index": 0, "sz_decimals": 0, "price_decimals": 6}
                )
                quantized_qty_hl = round_to_decimals(
                    request.quantity,
                    hl_meta["sz_decimals"],
                )
                quantized_price_hl = round_to_significant_and_decimal(
                    float(request.price or 0.0),
                    5,
                    hl_meta["price_decimals"],
                )
                if quantized_qty_hl <= 0.0 or quantized_price_hl <= 0.0:
                    raise OrderSubmitError(
                        SubmitFailureClass.REJECTED,
                        "Hyperliquid passive order normalized to zero quantity or price",
                    )
                preflight = {
                    "venue": spec.venue_id.value,
                    "symbol": venue_sym,
                    "side": request.side.value,
                    "raw_qty": request.quantity,
                    "raw_price": request.price,
                    "quantized_qty": quantized_qty_hl,
                    "quantized_price": quantized_price_hl,
                    "tick_size": 0.0,
                    "quantity_step": 10 ** (-hl_meta["sz_decimals"]),
                    "min_qty": 0.0,
                    "min_notional": 0.0,
                    "rule_source": "hyperliquid_meta",
                    "asset_index": hl_meta["asset_index"],
                    "sz_decimals": hl_meta["sz_decimals"],
                    "price_decimals": hl_meta["price_decimals"],
                }
            else:
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
            aster_headroom_payload: dict[str, Any] = {}

            # --- V1: OKX posMode refresh + contract sizing ---
            if spec.venue_id == Venue.OKX:
                if getattr(self, '_pos_mode_cache', None) is None:
                    await self._refresh_okx_pos_mode()
            if spec.venue_id in (Venue.BINANCE, Venue.ASTER):
                await self._refresh_fapi_position_mode()
            if spec.venue_id == Venue.ASTER:
                quantized_qty, aster_headroom_payload = (
                    await self._aster_apply_remaining_openable_headroom(
                        request,
                        venue_sym,
                        quantized_qty,
                        quantized_price,
                        qty_step,
                        min_qty,
                    )
                )
            ct_val_sz = float(getattr(symbol_rule, 'ct_val', 0)) if symbol_rule else 0.0
            lot_sz = float(getattr(symbol_rule, 'qty_step', 0)) if symbol_rule else (qty_step or 0.0)
            if spec.venue_id == Venue.OKX:
                okx_sizing = _okx_contract_order_diagnostics(
                    base_qty=quantized_qty,
                    ct_val=ct_val_sz,
                    lot_sz=lot_sz,
                    min_sz=float(getattr(symbol_rule, 'min_qty', 0) if symbol_rule else min_qty),
                    max_mkt_sz=float(getattr(symbol_rule, 'max_market_qty', 0) if symbol_rule else 0.0),
                )
                preflight.update(okx_sizing)
                contract_qty = float(okx_sizing["contract_qty"])
                reject_reason = okx_sizing.get("reject_reason")
                if reject_reason:
                    result_payload = dict(preflight)
                    result_payload["response_classification"] = "rejected"
                    self._record_order_diagnostic("order.submit_result", result_payload)
                    result_recorded = True
                    raise OrderSubmitError(
                        SubmitFailureClass.REJECTED,
                        str(reject_reason),
                    )
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
            if aster_headroom_payload:
                attempt_payload.update(aster_headroom_payload)
            if spec.venue_id == Venue.OKX:
                attempt_payload["pos_side"] = self._okx_pos_side(request.side, request.reduce_only)
                attempt_payload["pos_mode"] = self._okx_pos_mode
                attempt_payload["ct_val"] = ct_val_sz
                attempt_payload["base_qty"] = quantized_qty
                attempt_payload["contract_qty"] = contract_qty
                attempt_payload["lot_sz"] = lot_sz
                if ct_val_sz > 0:
                    attempt_payload["venue_contract_qty"] = _format_decimal(contract_qty)
            if spec.venue_id in (Venue.BINANCE, Venue.ASTER):
                attempt_payload["position_mode"] = (
                    "hedge" if self._fapi_position_hedge_mode else "one_way"
                )
                if "positionSide" in body:
                    attempt_payload["position_side"] = body["positionSide"]
            self._record_order_diagnostic("order.submit_attempt", attempt_payload)

            aster_retry_after_max_notional = False
            contract = get_operation_contract(spec, VenueOperation.CREATE_ORDER)
            if not contract.supported:
                raise TransportError(
                    TransportErrorCategory.UNSUPPORTED_CAPABILITY,
                    f"{spec.venue_id.value} passive create contract unsupported",
                )

            async def _send_passive_order(current_body: dict[str, Any]):
                if contract.payload == "params":
                    return await self._request(
                        contract.method,
                        contract.path,
                        params=current_body,
                        private=contract.private,
                    )
                return await self._request(
                    contract.method,
                    contract.path,
                    body=current_body,
                    private=contract.private,
                )

            # --- Send request ---
            try:
                raw = await _send_passive_order(body)
            except TransportError as first_error:
                if spec.venue_id != Venue.ASTER or not _is_aster_max_notional_error(first_error):
                    raise
                retry_qty, retry_payload = await self._aster_apply_remaining_openable_headroom(
                    request,
                    venue_sym,
                    quantized_qty,
                    quantized_price,
                    qty_step,
                    min_qty,
                )
                if retry_qty + 1e-12 >= quantized_qty:
                    raise
                quantized_qty = retry_qty
                contract_qty = retry_qty
                body = self._build_passive_order_body(
                    request, venue_sym, contract_qty, quantized_price, cid,
                )
                if retry_payload:
                    self._record_order_diagnostic(
                        "order.submit_attempt",
                        {
                            **attempt_payload,
                            **retry_payload,
                            "normalized_qty": quantized_qty,
                            "body_field_names": sorted(body.keys()),
                            "aster_retry_after_max_notional": True,
                        },
                    )
                aster_retry_after_max_notional = True
                raw = await _send_passive_order(body)

            # --- Venue-specific success guard ---
            if spec.venue_id == Venue.BYBIT:
                _require_bybit_success(raw, "bybit passive order failed")
            elif spec.venue_id == Venue.BITGET:
                _require_bitget_success(raw, "bitget passive order failed")
            elif spec.venue_id == Venue.ASTER:
                _require_aster_success(raw, "aster passive order failed")

            # --- Parse ack with venue-specific validation ---
            ack_request = request
            if spec.venue_id == Venue.HYPERLIQUID:
                ack_request = replace(
                    request,
                    quantity=quantized_qty,
                    price=float(quantized_price or 0.0),
                )
            ack = self._parse_passive_order_ack(raw, ack_request, venue_sym, now_ms)

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
                "aster_retry_after_max_notional": aster_retry_after_max_notional,
            }
            self._record_order_diagnostic("order.submit_result", result_payload)
            return ack

        except TransportError as e:
            if (
                spec.venue_id == Venue.HYPERLIQUID
                and is_hyperliquid_non_retryable_auth_signing_error(e)
            ):
                self._trading_capability_trusted = False
            classification = _passive_submit_reject_classification(spec.venue_id, e)
            if not result_recorded:
                self._record_passive_diagnostic_failure(
                    request, venue_sym, e.status_code, str(e),
                    classification=classification,
                )
            raise _map_to_submit_error(e.category, str(e), transport_error=e)
        except OrderSubmitError as e:
            if (
                spec.venue_id == Venue.HYPERLIQUID
                and is_hyperliquid_non_retryable_auth_signing_error(e)
            ):
                self._trading_capability_trusted = False
            classification = _passive_submit_reject_classification(spec.venue_id, e)
            if not result_recorded:
                self._record_passive_diagnostic_failure(
                    request, venue_sym, 0, str(e),
                    classification=classification,
                )
            raise
        except Exception as e:
            if not result_recorded:
                self._record_passive_diagnostic_failure(
                    request, venue_sym, 0, str(e),
                )
            raise OrderSubmitError(SubmitFailureClass.UNCERTAIN, str(e))

    def _record_passive_diagnostic_failure(
        self, request: OrderRequest, venue_sym: str,
        status_code: int, message: str, *, classification: str = "rejected",
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
            "response_classification": classification,
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
                request, venue_sym, quantized_qty, quantized_price, cid,
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
            body["newClientOrderId"] = cid[:36]  # V1: Binance/Aster 36-char limit
        if self._fapi_position_hedge_mode:
            body["positionSide"] = self._fapi_position_side(
                request.side, request.reduce_only
            )
        elif request.reduce_only:
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

    def _fapi_position_side(self, side: "Side", reduce_only: bool = False) -> str:
        """Return Binance/Aster Hedge Mode positionSide for the order intent."""
        if not reduce_only:
            return "LONG" if side == Side.BUY else "SHORT"
        return "SHORT" if side == Side.BUY else "LONG"

    @property
    def _fapi_position_hedge_mode(self) -> bool:
        """Cached Binance/Aster Futures position mode. Unknown defaults to one-way."""
        return bool(getattr(self, "_fapi_position_hedge_mode_cache", False))

    async def _refresh_fapi_position_mode(self) -> bool:
        """Fetch Binance/Aster Futures position mode and cache Hedge vs One-way."""
        if self._spec.venue_id not in (Venue.BINANCE, Venue.ASTER):
            return False
        cached = getattr(self, "_fapi_position_hedge_mode_cache", None)
        if cached is not None:
            return bool(cached)
        try:
            raw = await self._request("GET", "/fapi/v1/positionSide/dual", private=True)
            value = raw.get("dualSidePosition")
            hedge_mode = value is True or str(value).lower() == "true"
            self._fapi_position_hedge_mode_cache = hedge_mode
            self._record_order_diagnostic("order.fapi_position_mode_refresh", {
                "venue": self._spec.venue_id.value,
                "outcome": "success",
                "position_mode": "hedge" if hedge_mode else "one_way",
                "raw_dual_side_position": value,
            })
            return hedge_mode
        except Exception as e:
            self._record_order_diagnostic("order.fapi_position_mode_refresh", {
                "venue": self._spec.venue_id.value,
                "outcome": "exception",
                "error": str(e)[:200],
                "resolved_position_mode": "one_way",
            })
            return False

    def _extract_fapi_leverage_bracket(
        self,
        raw: Any,
        venue_sym: str,
        notional_quote: float | None,
    ) -> dict[str, Any]:
        rows = raw if isinstance(raw, list) else [raw]
        symbol_row: dict[str, Any] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_symbol = str(row.get("symbol", "") or "")
            if not row_symbol or row_symbol == venue_sym:
                symbol_row = row
                break
        brackets = symbol_row.get("brackets", []) if isinstance(symbol_row, dict) else []
        if not isinstance(brackets, list) or not brackets:
            return {
                "initial_leverage": 0,
                "notional_floor": 0.0,
                "notional_cap": 0.0,
                "raw_symbol": str(symbol_row.get("symbol", venue_sym) if isinstance(symbol_row, dict) else venue_sym),
            }
        notional = max(float(notional_quote or 0.0), 0.0)
        chosen = brackets[-1]
        for bracket in brackets:
            if not isinstance(bracket, dict):
                continue
            floor = _safe_float(bracket.get("notionalFloor", 0), default=0.0)
            cap = _safe_float(bracket.get("notionalCap", 0), default=0.0)
            if cap <= 0 or (notional + 1e-12 >= floor and notional <= cap + 1e-12):
                chosen = bracket
                break
        if not isinstance(chosen, dict):
            chosen = {}
        return {
            "initial_leverage": int(_safe_float(chosen.get("initialLeverage", 0), default=0.0)),
            "notional_floor": _safe_float(chosen.get("notionalFloor", 0), default=0.0),
            "notional_cap": _safe_float(chosen.get("notionalCap", 0), default=0.0),
            "bracket": int(_safe_float(chosen.get("bracket", 0), default=0.0)),
            "raw_symbol": str(symbol_row.get("symbol", venue_sym) if isinstance(symbol_row, dict) else venue_sym),
        }

    def _extract_fapi_position_leverage(self, raw: Any, venue_sym: str) -> int:
        rows = raw if isinstance(raw, list) else [raw]
        leverages: set[int] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_symbol = str(row.get("symbol", venue_sym) or venue_sym)
            if row_symbol != venue_sym:
                continue
            lev = int(_safe_float(row.get("leverage", 0), default=0.0))
            if lev > 0:
                leverages.add(lev)
        if len(leverages) == 1:
            return next(iter(leverages))
        return 0

    async def ensure_entry_leverage(
        self,
        symbol: str,
        leverage: int,
        *,
        notional_quote: float | None = None,
    ) -> None:
        """V1 live-entry parity: prepare Binance-compatible symbol leverage before orders."""
        if self._spec.venue_id not in (Venue.BINANCE, Venue.ASTER):
            return
        target = int(leverage or 0)
        if target <= 0 or self.mode != "live":
            return

        venue_sym = self._venue_symbol(symbol)
        cache_key = f"{venue_sym}:{target}:{round(float(notional_quote or 0.0), 8)}"
        cached_effective = self._entry_leverage_ready_cache.get(cache_key)
        if cached_effective is not None:
            self._record_order_diagnostic(
                "order.entry_leverage_ready",
                {
                    "venue": self._spec.venue_id.value,
                    "symbol": venue_sym,
                    "requested_leverage": target,
                    "effective_leverage": cached_effective,
                    "outcome": "cached_ready",
                },
            )
            return

        payload: dict[str, Any] = {
            "venue": self._spec.venue_id.value,
            "symbol": venue_sym,
            "requested_leverage": target,
            "requested_notional_quote": float(notional_quote or 0.0),
            "position_endpoint": self._spec.position_path,
            "bracket_endpoint": "/fapi/v1/leverageBracket",
            "set_leverage_endpoint": "/fapi/v1/leverage",
        }

        try:
            position_raw = await self._request(
                "GET",
                self._spec.position_path,
                params={"symbol": venue_sym},
                private=True,
            )
            current_leverage = self._extract_fapi_position_leverage(position_raw, venue_sym)
            payload["position_risk_leverage"] = current_leverage

            bracket_raw = await self._request(
                "GET",
                "/fapi/v1/leverageBracket",
                params={"symbol": venue_sym},
                private=True,
            )
            bracket = self._extract_fapi_leverage_bracket(
                bracket_raw,
                venue_sym,
                notional_quote,
            )
            bracket_initial = int(bracket.get("initial_leverage", 0) or 0)
            effective = min(target, bracket_initial) if bracket_initial > 0 else target
            effective = max(int(effective), 1)
            payload.update(
                {
                    "effective_leverage": effective,
                    "bracket_initial_leverage": bracket_initial,
                    "bracket": bracket.get("bracket", 0),
                    "notional_floor": bracket.get("notional_floor", 0.0),
                    "notional_cap": bracket.get("notional_cap", 0.0),
                }
            )

            if current_leverage == effective:
                payload["outcome"] = "already_ready"
                self._entry_leverage_ready_cache[cache_key] = effective
                self._record_order_diagnostic("order.entry_leverage_ready", payload)
                return

            response = await self._request(
                "POST",
                "/fapi/v1/leverage",
                params={"symbol": venue_sym, "leverage": effective},
                private=True,
            )
            response_leverage = int(_safe_float(response.get("leverage", 0), default=0.0)) if isinstance(response, dict) else 0
            payload["response_leverage"] = response_leverage
            payload["response_max_notional_value"] = (
                response.get("maxNotionalValue") if isinstance(response, dict) else None
            )
            if response_leverage and response_leverage != effective:
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    "entry leverage prepare returned unexpected leverage "
                    f"symbol={venue_sym} expected={effective} actual={response_leverage}",
                )
            payload["outcome"] = "set"
            self._entry_leverage_ready_cache[cache_key] = effective
            self._record_order_diagnostic("order.entry_leverage_ready", payload)
        except OrderSubmitError:
            payload["outcome"] = "rejected"
            self._record_order_diagnostic("order.entry_leverage_unavailable", payload)
            raise
        except TransportError as exc:
            payload["outcome"] = "transport_error"
            payload["error"] = str(exc)[:300]
            payload["status_code"] = exc.status_code
            payload["response_body"] = exc.body[:500]
            self._record_order_diagnostic("order.entry_leverage_unavailable", payload)
            raise OrderSubmitError(
                SubmitFailureClass.REJECTED,
                f"entry leverage prepare failed: {exc} {exc.body[:200]}",
            ) from exc
        except Exception as exc:
            payload["outcome"] = "exception"
            payload["error"] = str(exc)[:300]
            self._record_order_diagnostic("order.entry_leverage_unavailable", payload)
            raise

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
        self, request: OrderRequest, venue_sym: str,
        quantized_qty: float, quantized_price: Any,
        cid: str = "",
    ) -> dict[str, Any]:
        from lightfee.venues.hyperliquid_signing import (
            build_hyperliquid_exchange_payload,
            build_hyperliquid_order_action,
            hyperliquid_cloid_for_client_order,
        )

        meta = self._hl_cached_asset_meta(venue_sym)
        wire_cloid = hyperliquid_cloid_for_client_order(cid) if cid else None
        action = build_hyperliquid_order_action(
            symbol=venue_sym,
            is_buy=request.side == Side.BUY,
            quantity=quantized_qty,
            price=float(quantized_price or 0.0),
            reduce_only=request.reduce_only,
            tif="Alo",
            cloid=wire_cloid,
            asset_index=meta["asset_index"],
            sz_decimals=meta["sz_decimals"],
            price_decimals=meta["price_decimals"],
        )
        if self.mode != "live":
            return {"action": action, "type": "order"}
        if self._credential is None or not self._credential.wallet_private_key:
            raise OrderSubmitError(
                SubmitFailureClass.REJECTED,
                "Hyperliquid passive order requires wallet_private_key",
            )
        return build_hyperliquid_exchange_payload(
            action=action,
            private_key_hex=self._credential.wallet_private_key,
            vault_address=None,
            is_mainnet=True,
        )

    def _parse_passive_order_ack(
        self, raw: dict[str, Any], request: OrderRequest, venue_sym: str, now_ms: int,
    ) -> "PassiveOrderAck":
        from lightfee.core.domain import PassiveOrderAck, PassiveOrderState

        spec = self._spec

        if spec.venue_id == Venue.HYPERLIQUID:
            resp_status = str(raw.get("status", "")).lower()
            if resp_status == "err":
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    str(raw.get("response", "Hyperliquid exchange error")),
                )
            response = raw.get("response", {})
            statuses: list[Any] = []
            if isinstance(response, dict):
                inner_data = response.get("data", response)
                if isinstance(inner_data, dict):
                    statuses = inner_data.get("statuses", []) or []
            if not statuses:
                raise OrderSubmitError(
                    SubmitFailureClass.UNCERTAIN,
                    "Hyperliquid passive order response contains no statuses",
                )
            status_entry = statuses[0]
            if isinstance(status_entry, str):
                raise OrderSubmitError(SubmitFailureClass.REJECTED, status_entry)
            if not isinstance(status_entry, dict):
                raise OrderSubmitError(
                    SubmitFailureClass.UNCERTAIN,
                    f"Hyperliquid passive unknown status: {status_entry!r}",
                )
            if "error" in status_entry:
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    str(status_entry.get("error", "")),
                )
            order_info = status_entry.get("resting") or status_entry.get("filled")
            if not isinstance(order_info, dict):
                raise OrderSubmitError(
                    SubmitFailureClass.UNCERTAIN,
                    f"Hyperliquid passive unknown status: {list(status_entry.keys())}",
                )
            oid = str(order_info.get("oid", ""))
            if not oid:
                raise OrderSubmitError(
                    SubmitFailureClass.UNCERTAIN,
                    "Hyperliquid passive accepted without oid",
                )
            state = (
                PassiveOrderState.FILLED
                if "filled" in status_entry
                else PassiveOrderState.OPEN
            )
            qty = _safe_float(
                order_info.get("totalSz", order_info.get("sz", request.quantity)),
                default=float(request.quantity),
            )
            price = _safe_float(
                order_info.get("avgPx", order_info.get("limitPx", request.price or 0.0)),
                default=float(request.price or 0.0),
            )
            return PassiveOrderAck(
                venue=spec.venue_id,
                symbol=venue_sym,
                side=request.side,
                order_id=oid,
                client_order_id=request.client_order_id or "",
                price=price,
                quantity=qty,
                accepted_at_ms=now_ms,
                state=state,
            )

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
        side: "Side | None" = None,
    ) -> Optional["PassiveOrderProgress"]:
        """Query cumulative progress for a resting passive order.

        V1 semantics:
        - Bitget: REST detail + private progress + reconciliation → merge
          (V1 bitget.rs:2483 fetch_passive_order_progress).
        - Other venues: private-first with REST fallback.
        """
        from lightfee.core.domain import PassiveOrderProgress, PassiveOrderState

        spec = self._spec
        venue_sym = self._venue_symbol(symbol)
        now_ms = int(time.time() * 1000)

        if self.mode == "paper":
            return None

        # Bitget: full V1 REST+private+reconciliation merge
        if spec.venue_id == Venue.BITGET:
            return await self._query_passive_order_progress_bitget(
                symbol, venue_sym, order_id, client_order_id, side, now_ms,
            )

        # Other venues: private-first, REST fallback
        private_progress = self.private_order_progress(
            client_order_id=client_order_id,
            order_id=order_id,
            max_age_ms=15_000,
        )
        if private_progress is not None:
            resolved_side = side or Side.BUY
            return PassiveOrderProgress(
                venue=spec.venue_id,
                symbol=symbol,
                side=resolved_side,
                order_id=private_progress.order_id or order_id,
                client_order_id=private_progress.client_order_id or client_order_id or "",
                cumulative_quantity=private_progress.cumulative_quantity,
                average_price=private_progress.average_price or 0.0,
                fee_quote=private_progress.fee_quote or 0.0,
                last_fill_time_ms=private_progress.last_fill_at_ms or 0,
                state=private_progress.state or PassiveOrderState.UNKNOWN,
                observed_at_ms=now_ms,
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
            elif spec.venue_id == Venue.GATE:
                if order_id:
                    params["order_id"] = order_id
            elif spec.venue_id == Venue.HYPERLIQUID:
                result = await self._query_hyperliquid_order(spec, venue_sym, order_id, client_order_id, now_ms)
                if result is not None:
                    raw, is_list = result
                    if is_list:
                        return self._parse_hl_order_list(raw, spec, venue_sym, order_id, client_order_id, now_ms)
                    return self._parse_passive_order_progress(raw, spec, venue_sym, now_ms)
                return None

            contract = get_operation_contract(spec, VenueOperation.ORDER_STATUS)
            if not contract.supported:
                return None
            query_path = contract.path
            if "{order_id}" in query_path and order_id:
                query_path = query_path.replace("{order_id}", str(order_id))
                params.pop("order_id", None)
            if contract.payload == "params":
                raw = await self._request(
                    contract.method,
                    query_path,
                    params=params,
                    private=contract.private,
                )
            else:
                raw = await self._request(
                    contract.method,
                    query_path,
                    body=params,
                    private=contract.private,
                )
            return self._parse_passive_order_progress(raw, spec, venue_sym, now_ms)

        except TransportError:
            return None
        except Exception:
            return None

    async def _query_passive_order_progress_bitget(
        self, symbol: str, venue_sym: str, order_id: str,
        client_order_id: Optional[str], side: "Side | None", now_ms: int,
    ) -> Optional["PassiveOrderProgress"]:
        """V1 Bitget fetch_passive_order_progress (bitget.rs:2483-2562).

        Full merge: REST order detail + private WS progress + reconciliation.
        Priority: reconciliation > REST detail > private WS.

        Bitget REST detail is parsed DIRECTLY from the raw response using
        V1 multi-key fallbacks (NOT through generic _parse_passive_order_progress
        which lacks Bitget-specific field names for fee/timestamp/quantity).
        """
        from lightfee.core.domain import PassiveOrderProgress, PassiveOrderState, Side as DomainSide
        from lightfee.marketdata.private_ws import (
            CumulativeOrderProgress,
            merge_passive_progress_sources,
            should_fetch_passive_reconciliation,
        )

        spec = self._spec
        detail_data: Optional[dict[str, Any]] = None
        detail_progress: Optional[CumulativeOrderProgress] = None
        # V1: original_quantity + status extracted from REST detail for state detection
        # (bitget.rs:2576-2583). State is computed AFTER merge using merged cumulative_quantity.
        _btg_original_qty: float = 0.0
        _btg_status_str: str = ""

        # 1. Fetch REST order detail (V1: fetch_bitget_order_detail → parse fields)
        try:
            if order_id or client_order_id:
                raw = await self._fetch_bitget_order_detail(
                    venue_sym, order_id, client_order_id,
                )
                if raw is not None:
                    # V1: bitget_data() — extract "data" field, verify code success
                    data = raw.get("data", raw)
                    if isinstance(data, list):
                        data = data[0] if data else {}
                    if isinstance(data, dict) and data:
                        detail_data = data
        except (TransportError, Exception):
            pass

        if detail_data is not None:
            # V1: directly extract fields from detail row with multi-key fallback
            # (bitget.rs:2496-2533, json_string / json_f64 / json_i64 per-field)
            def _bf(keys, default=""):
                """Bitget field: first non-empty string value from key list."""
                for k in keys:
                    v = detail_data.get(k)
                    if v is not None and str(v).strip():
                        return str(v)
                return default

            def _bf_f64(keys, default=0.0):
                for k in keys:
                    v = detail_data.get(k)
                    if v is not None:
                        try:
                            fv = float(v)
                            if fv > 0.0 and __import__('math').isfinite(fv):
                                return fv
                        except (TypeError, ValueError):
                            continue
                return default

            def _bf_i64(keys, default=0):
                for k in keys:
                    v = detail_data.get(k)
                    if v is not None:
                        try:
                            iv = int(v)
                            if iv > 0:
                                # V1: seconds → ms conversion (bitget.rs:2533)
                                return iv if iv >= 10_000_000_000 else iv * 1000
                        except (TypeError, ValueError):
                            continue
                return default

            # Fee: V1 json_f64(row, &["fee","totalFee","filledFee"])
            #      .or_else(|| row.get("feeDetail").and_then(|v| json_f64(v, &["totalFee"]).map(f64::abs)))
            def _bf_fee():
                for k in ("fee", "totalFee", "filledFee"):
                    v = detail_data.get(k)
                    if v is not None:
                        try:
                            fv = float(v)
                            if __import__('math').isfinite(fv):
                                return abs(fv)
                        except (TypeError, ValueError):
                            continue
                fd = detail_data.get("feeDetail")
                if isinstance(fd, dict):
                    tf = fd.get("totalFee")
                    if tf is not None:
                        try:
                            return abs(float(tf))
                        except (TypeError, ValueError):
                            pass
                elif isinstance(fd, list):
                    fee_sum = 0.0
                    for entry in fd:
                        if isinstance(entry, dict):
                            try:
                                fee_sum += abs(float(entry.get("fee", "0")))
                            except (TypeError, ValueError):
                                pass
                    if fee_sum > 0:
                        return fee_sum
                return 0.0

            # V1: save status + original_quantity for post-merge state detection
            # State is computed AFTER merge using merged.cumulative_quantity
            # (bitget.rs:2560-2590)
            _btg_original_qty = _bf_f64(["size", "qty", "baseSize", "amount"])
            _btg_status_str = _bf(["state", "status", "orderStatus", "ordStatus"]).lower()

            detail_progress = CumulativeOrderProgress.from_position_snapshot(
                order_id=_bf(["orderId", "ordId"]) or order_id,
                client_order_id=_bf(["clientOid"]) or client_order_id,
                cumulative_quantity=_bf_f64([
                    "baseVolume", "filledQty", "fillQty", "filled_amount", "size",
                ]),
                average_price=_bf_f64([
                    "priceAvg", "fillPriceAvg", "averagePrice", "avgPrice",
                ]) or None,
                fee_quote=_bf_fee() or None,
                updated_at_ms=_bf_i64([
                    "updateTime", "update_time_ms", "cTime", "uTime",
                ]) or now_ms,
            )

        if detail_progress is None:
            detail_progress = CumulativeOrderProgress()

        # 2. Look up private WS progress (V1: lookup_or_wait_private_order_progress)
        private_progress = self.private_order_progress(
            client_order_id=client_order_id,
            order_id=order_id,
            max_age_ms=15_000,
        )

        # 3. Fetch reconciliation (V1: fetch_order_fill_reconciliation →
        #    fetch_bitget_order_detail → parse as OrderFillReconciliation)
        reconciliation = None
        if should_fetch_passive_reconciliation(detail_progress, private_progress):
            try:
                recon_raw = await self._fetch_bitget_order_detail(
                    venue_sym, order_id, client_order_id,
                )
                if recon_raw is not None:
                    recon = self._parse_order_status_bitget(
                        recon_raw, venue_sym, now_ms,
                    )
                    if recon is not None and recon.quantity > 0:
                        reconciliation = recon
            except Exception:
                pass

        # 4. Merge all sources: highest qty wins; tied qty → recon > detail > private
        merged = merge_passive_progress_sources(
            detail_progress, reconciliation, private_progress,
        )

        # 5. Determine state (V1: bitget_passive_order_state, bitget.rs:2560-2590)
        # State ALWAYS from REST detail status + merged cumulative_quantity.
        # Never from merged.state (which could carry private WS or reconciliation state).
        # V1: if detail.is_none() && private_progress.is_some() && cumulative_quantity <= 0.0
        #     → Resting; else → bitget_passive_order_state(status, merged_qty, original_qty)
        if detail_data is None and private_progress is not None \
                and merged.cumulative_quantity <= 0.0:
            state = PassiveOrderState.OPEN  # V1: Resting
        else:
            merged_qty = merged.cumulative_quantity
            is_filled = (
                _btg_original_qty > 0.0
                and merged_qty + 1e-9 >= _btg_original_qty
            )
            if _btg_status_str in ("filled", "closed", "completed", "done", "success") \
                    or is_filled:
                state = PassiveOrderState.FILLED
            elif _btg_status_str in (
                "partial_filled", "partial_fill", "partially_filled",
                "partial", "partial filled",
            ) or merged_qty > 0.0:
                state = PassiveOrderState.PARTIALLY_FILLED
            elif _btg_status_str in ("canceled", "cancelled", "cancel", "expired", "terminated"):
                state = PassiveOrderState.CANCELED
            elif _btg_status_str in ("rejected", "reject", "failed", "invalid"):
                state = PassiveOrderState.REJECTED
            elif _btg_status_str in ("open", "live", "resting", "new", "pending", "triggered"):
                state = PassiveOrderState.OPEN
            else:
                # V1: bitget_passive_order_state returns Unknown for unrecognized
                # status regardless of quantity (bitget.rs:3795)
                state = PassiveOrderState.UNKNOWN

        # 6. V1: detail None + private None + 0 fill → None
        if detail_data is None and private_progress is None and merged.cumulative_quantity <= 0.0:
            return None

        # 7. Side from detail or caller fallback (V1: bitget.rs:2565-2574)
        resolved_side: "DomainSide" = side or DomainSide.BUY
        if detail_data is not None:
            side_str = str(detail_data.get("side", "")).lower()
            if side_str == "buy":
                resolved_side = DomainSide.BUY
            elif side_str == "sell":
                resolved_side = DomainSide.SELL

        return PassiveOrderProgress(
            venue=spec.venue_id,
            symbol=symbol,
            side=resolved_side,
            order_id=merged.order_id or order_id,
            client_order_id=merged.client_order_id or client_order_id or "",
            cumulative_quantity=merged.cumulative_quantity,
            average_price=merged.average_price or 0.0,
            fee_quote=merged.fee_quote or 0.0,
            last_fill_time_ms=merged.last_fill_at_ms or 0,
            state=state,
            observed_at_ms=now_ms,
        )

    async def _fetch_bitget_order_detail(
        self, venue_sym: str, order_id: str, client_order_id: Optional[str],
    ) -> Optional[dict[str, Any]]:
        """Fetch Bitget order detail through the resolved account-family contract."""
        # Build query params: orderId or clientOid
        query_params: dict[str, Any] = {}
        if order_id and order_id != "bitget-unknown":
            query_params["orderId"] = order_id
        elif client_order_id:
            query_params["clientOid"] = client_order_id
        else:
            return None

        family = await _resolve_bitget_contract_family_for_truth(self)
        contract = get_operation_contract(
            self._spec,
            VenueOperation.ORDER_STATUS,
            resolved_account_family=family,
        )
        params = _bitget_contract_params(
            contract,
            venue_sym=venue_sym,
            extra=query_params,
        )
        try:
            raw = await self._request(
                contract.method,
                contract.path,
                params=params,
                private=contract.private,
            )
            code = str(raw.get("code", ""))
            if code in ("40109", "43001"):
                return None
            if code not in ("00000", "0"):
                return None
            return raw
        except (TransportError, Exception):
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
                      data.get("baseVolume", data.get("filled_total", data.get("accFillSz",
                      data.get("fillSz", data.get("cumExecQty", data.get("size", 0)))))))))))
        if spec.venue_id == Venue.OKX and cum_qty > 0.0:
            contract_size = self._okx_cached_contract_size_for_progress(data, venue_sym)
            if contract_size <= 0.0:
                return None
            cum_qty = self._okx_contracts_to_base_quantity(cum_qty, contract_size)
        avg_price = float(data.get("avgPrice", data.get("priceAvg",
                         data.get("fillPriceAvg", data.get("averagePrice",
                         data.get("avgPx", data.get("price", 0)))))))

        fee_quote = float(data.get("commission", data.get("fee", 0)))
        last_fill_time = int(data.get("updateTime", data.get("updatedTime",
                               data.get("updateTimestamp", data.get("cTime",
                               data.get("uTime", now_ms))))))

        status_str = str(data.get("status", data.get("state", data.get("ordStatus", "")))).upper()
        if status_str in ("NEW", "OPEN", "UNTRI", "ACTIVE", "UNTRIGGERED", "NEW_ORDER", "TRIGGERED"):
            state = PassiveOrderState.OPEN
        elif status_str in ("PARTIALLY_FILLED", "PARTIAL", "PARTIALLY_CANCELED"):
            state = PassiveOrderState.PARTIALLY_FILLED
        elif status_str in ("FILLED", "CLOSED", "FINISHED", "COMPLETED", "DONE"):
            state = PassiveOrderState.FILLED
        elif status_str in ("CANCELED", "CANCELLED", "TERMINATED"):
            state = PassiveOrderState.CANCELED
        elif status_str in ("REJECTED"):
            state = PassiveOrderState.REJECTED
        elif status_str in ("EXPIRED"):
            # V1: expired orders are terminal like Canceled (bitget.rs:3780)
            state = PassiveOrderState.CANCELED
        elif status_str == "LIVE":
            # V1 OKX: "live" → cum > 0 → PartiallyFilled else Resting (okx.rs:5716-5722)
            state = PassiveOrderState.PARTIALLY_FILLED if cum_qty > 0 else PassiveOrderState.OPEN
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

    def _okx_cached_contract_size_for_progress(
        self,
        data: dict[str, Any],
        venue_sym: str,
    ) -> float:
        keys = [
            str(data.get("instId", "") or ""),
            venue_sym,
        ]
        if self._spec.symbol_from_venue is not None:
            for key in tuple(keys):
                if not key:
                    continue
                try:
                    keys.append(self._spec.symbol_from_venue(key))
                except Exception:
                    pass
        for key in keys:
            metadata = self._symbol_metadata.get(key)
            if not isinstance(metadata, dict):
                continue
            for field in ("ct_val", "ctVal", "contract_size", "contractSize"):
                value = _safe_float(metadata.get(field), default=0.0)
                if value > 0.0:
                    return value
        return 0.0

    async def amend_passive_order(
        self, request: "PassiveOrderAmendRequest",
    ) -> "PassiveOrderAck":
        """Amend a resting passive order (price/quantity). Falls back to cancel+replace."""
        from lightfee.core.domain import PassiveOrderAck, PassiveOrderState, Side

        spec = self._spec
        venue_sym = self._venue_symbol(request.symbol)
        now_ms = int(time.time() * 1000)
        contract = get_operation_contract(spec, VenueOperation.AMEND_ORDER)

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

        if not contract.supported:
            raise NotImplementedError(
                f"{spec.venue_id.value} passive amend not supported: "
                f"{contract.official_doc_url or 'cancel_replace_required'}"
            )

        try:
            body: dict[str, Any] = {"symbol": venue_sym}

            if spec.venue_id == Venue.BINANCE:
                body["orderId"] = request.order_id
                body["side"] = request.side.value.upper()
                if request.new_price_hint is not None and request.new_price_hint > 0:
                    body["price"] = _format_decimal(request.new_price_hint)
                if request.new_quantity is not None and request.new_quantity > 0:
                    body["quantity"] = _format_decimal(request.new_quantity)
            elif spec.venue_id == Venue.ASTER:
                raise NotImplementedError("Aster passive amend not supported")
            elif spec.venue_id == Venue.OKX:
                body["instId"] = venue_sym
                if request.order_id:
                    body["ordId"] = request.order_id
                elif request.client_order_id:
                    body["clOrdId"] = request.client_order_id
                if request.new_client_order_id:
                    body["newClOrdId"] = request.new_client_order_id
                if request.new_price_hint is not None and request.new_price_hint > 0:
                    body["newPx"] = _format_decimal(request.new_price_hint)
                if request.new_quantity is not None and request.new_quantity > 0:
                    if (
                        self.mode == "live"
                        and venue_sym not in self._symbol_metadata
                        and request.symbol not in self._symbol_metadata
                    ):
                        await self._ensure_okx_swap_instrument_metadata_loaded()
                    metadata = (
                        self._symbol_metadata.get(venue_sym)
                        or self._symbol_metadata.get(request.symbol)
                        or {}
                    )
                    ct_val = _safe_float(
                        metadata.get(
                            "ct_val",
                            metadata.get("ctVal", metadata.get("contract_size", 0.0)),
                        ),
                        default=0.0,
                    )
                    lot_sz = _safe_float(
                        metadata.get(
                            "lot_sz",
                            metadata.get("lotSz", metadata.get("qty_step", 0.0)),
                        ),
                        default=0.0,
                    )
                    min_sz = _safe_float(
                        metadata.get(
                            "min_sz",
                            metadata.get("minSz", metadata.get("min_qty", 0.0)),
                        ),
                        default=0.0,
                    )
                    sizing = _okx_contract_order_diagnostics(
                        base_qty=float(request.new_quantity),
                        ct_val=ct_val,
                        lot_sz=lot_sz,
                        min_sz=min_sz,
                    )
                    reject_reason = sizing.get("reject_reason")
                    if reject_reason:
                        raise TransportError(
                            TransportErrorCategory.NORMALIZATION_FAILURE,
                            "okx passive amend contract quantity invalid: "
                            f"{reject_reason}",
                        )
                    body["newSz"] = _format_decimal(float(sizing["contract_qty"]))
                body["cxlOnFail"] = False
            elif spec.venue_id == Venue.BYBIT:
                body["category"] = "linear"
                body["orderId"] = request.order_id
                if request.new_price_hint is not None and request.new_price_hint > 0:
                    body["price"] = _format_decimal(request.new_price_hint)
                if request.new_quantity is not None and request.new_quantity > 0:
                    body["qty"] = _format_decimal(request.new_quantity)
            elif spec.venue_id == Venue.BITGET:
                raise NotImplementedError("Bitget passive amend not supported")
            elif spec.venue_id == Venue.GATE:
                raise NotImplementedError("Gate passive amend not supported")
            elif spec.venue_id == Venue.HYPERLIQUID:
                # Hyperliquid amend via cancel+replace only
                raise NotImplementedError("Hyperliquid amend not supported")

            if contract.payload == "params":
                raw = await self._request(
                    contract.method, contract.path, params=body, private=contract.private
                )
            else:
                raw = await self._request(
                    contract.method, contract.path, body=body, private=contract.private
                )
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

    async def _cancel_okx_passive_order_once(
        self,
        venue_sym: str,
        order_id: str,
        client_order_id: Optional[str],
        now_ms: int,
    ) -> "PassiveOrderAck":
        from lightfee.core.domain import PassiveOrderAck, PassiveOrderState, Side

        body: dict[str, Any] = {"instId": venue_sym}
        if order_id:
            body["ordId"] = order_id
        elif client_order_id:
            body["clOrdId"] = client_order_id
        raw = await self._request(
            "POST",
            OKX_CANCEL_ORDER_PATH,
            body=body,
            private=True,
        )
        if _cancel_response_indicates_absent_order(raw, Venue.OKX):
            return PassiveOrderAck(
                venue=Venue.OKX,
                symbol=venue_sym,
                side=Side.BUY,
                order_id=order_id,
                client_order_id=client_order_id or "",
                price=0.0,
                quantity=0.0,
                accepted_at_ms=now_ms,
                state=PassiveOrderState.CANCELED,
            )

        code = str(raw.get("code", "0"))
        row = raw.get("data", [{}])
        if isinstance(row, list) and row:
            row = row[0]
        if not isinstance(row, dict):
            row = {}
        s_code = str(row.get("sCode", "0"))
        if code != "0" or s_code != "0":
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                "okx cancel passive order failed: "
                f"code={raw.get('code')} msg={raw.get('msg', '')} "
                f"sCode={row.get('sCode', '')} sMsg={row.get('sMsg', '')}",
                status_code=400,
                body=json.dumps(raw, ensure_ascii=False),
            )
        return PassiveOrderAck(
            venue=Venue.OKX,
            symbol=venue_sym,
            side=Side.BUY,
            order_id=order_id or str(row.get("ordId", "") or ""),
            client_order_id=client_order_id or str(row.get("clOrdId", "") or ""),
            price=0.0,
            quantity=0.0,
            accepted_at_ms=now_ms,
            state=PassiveOrderState.CANCELED,
        )

    async def cancel_passive_order(
        self, symbol: str, order_id: str, client_order_id: Optional[str] = None,
    ) -> "PassiveOrderAck":
        """Cancel a resting passive order.

        V1 parity: treats "order not found" as success — order already absent
        from exchange is effectively canceled. Returns CANCELED ack in that case.
        """
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
            contract = get_operation_contract(spec, VenueOperation.CANCEL_ORDER)
            if not contract.supported:
                raise TransportError(
                    TransportErrorCategory.UNSUPPORTED_CAPABILITY,
                    f"{spec.venue_id.value} passive cancel contract unsupported",
                )
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
                return await self._cancel_okx_passive_order_once(
                    venue_sym,
                    order_id,
                    client_order_id,
                    now_ms,
                )
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
                try:
                    oid = int(order_id)
                except (TypeError, ValueError):
                    raise TransportError(
                        TransportErrorCategory.ORDER_STATE_UNCERTAIN,
                        "hyperliquid cancel requires numeric exchange order id",
                    )
                if self._credential is None or not self._credential.wallet_private_key:
                    raise TransportError(
                        TransportErrorCategory.AUTH_FAILURE,
                        "hyperliquid cancel requires wallet_private_key",
                    )
                from lightfee.venues.hyperliquid_signing import (
                    build_hyperliquid_exchange_payload,
                )

                meta = await self._hl_resolve_asset_meta(venue_sym)
                action = {
                    "type": "cancel",
                    "cancels": [{"a": int(meta["asset_index"]), "o": oid}],
                }
                body = build_hyperliquid_exchange_payload(
                    action=action,
                    private_key_hex=self._credential.wallet_private_key,
                    vault_address=None,
                    is_mainnet=True,
                )
                raw = await self._request(
                    contract.method,
                    contract.path,
                    body=body,
                    private=contract.private,
                )
                cancel_already_terminal = _cancel_response_indicates_absent_order(
                    raw, spec.venue_id
                )
                # V1: check HL cancel response (hyperliquid.rs cancel path)
                # Response: {"status": "ok", "response": {"type": "cancel", "data": {"statuses": [...]}}}
                status = str(raw.get("status", "")).lower()
                response_data = raw.get("response", raw)
                if not cancel_already_terminal and isinstance(response_data, dict):
                    data = response_data.get("data", {})
                    if isinstance(data, dict):
                        statuses = data.get("statuses", [])
                        if isinstance(statuses, list):
                            for item in statuses:
                                if isinstance(item, dict) and item.get("error"):
                                    status = "error"
                                    break
                                if "error" in str(item).lower():
                                    status = "error"
                                    break
                if status != "ok":
                    raise TransportError(
                        TransportErrorCategory.REQUEST_REJECTED,
                        f"Hyperliquid cancel rejected: {raw}",
                    )
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

            cancel_path = contract.path
            if "{order_id}" in cancel_path and order_id:
                cancel_path = cancel_path.replace("{order_id}", str(order_id))
                params.pop("order_id", None)
            if contract.payload == "params":
                raw = await self._request(
                    contract.method,
                    cancel_path,
                    params=params,
                    private=contract.private,
                )
            else:
                raw = await self._request(
                    contract.method,
                    cancel_path,
                    body=params,
                    private=contract.private,
                )

            # V1: successful HTTP response may still indicate order was already absent
            if _cancel_response_indicates_absent_order(raw, spec.venue_id):
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
            if spec.venue_id == Venue.BYBIT:
                ret_code = int(raw.get("retCode", 0) or 0)
                if ret_code != 0:
                    raise TransportError(
                        TransportErrorCategory.REQUEST_REJECTED,
                        "bybit cancel passive order failed: "
                        f"retCode={raw.get('retCode')} retMsg={raw.get('retMsg', '')}",
                        status_code=400,
                        body=json.dumps(raw, ensure_ascii=False),
                    )
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

            return self._parse_passive_order_ack(
                raw,
                OrderRequest(venue=spec.venue_id, symbol=venue_sym, side=Side.BUY,
                             quantity=0.0, client_order_id=client_order_id),
                venue_sym, now_ms,
            )

        except TransportError as e:
            # V1: HTTP error from cancel may mean the order is already absent
            if _cancel_error_indicates_absent_order(
                getattr(e, 'body', '') or str(e),
                getattr(e, 'status_code', 0),
                spec.venue_id,
            ):
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
        venue_sym = self._venue_symbol(symbol)
        if spec.venue_id == Venue.BYBIT and self.mode == "live":
            try:
                symbol_rule = await get_symbol_rules_cache().get(self, Venue.BYBIT, venue_sym)
            except Exception:
                symbol_rule = None
            if symbol_rule is not None:
                return normalize_venue_quantity(
                    quantity=quantity,
                    step_size=float(getattr(symbol_rule, "qty_step", 0.0) or 0.0),
                    contract_size=spec.contract_size,
                    min_quantity=float(getattr(symbol_rule, "min_qty", 0.0) or 0.0),
                )
        if spec.venue_id == Venue.OKX:
            venue_sym = self._venue_symbol(symbol)
            if self.mode == "live":
                await self._ensure_okx_swap_instrument_metadata_loaded()
            metadata = (
                self._symbol_metadata.get(venue_sym)
                or self._symbol_metadata.get(symbol)
                or {}
            )
            ct_type = str(
                metadata.get(
                    "ctType",
                    metadata.get("ct_type", metadata.get("contractType", "")),
                )
                or ""
            )
            ct_val = _safe_float(
                metadata.get("ct_val", metadata.get("ctVal", metadata.get("contract_size", 0.0))),
                default=0.0,
            )
            lot_sz = _safe_float(
                metadata.get("lot_sz", metadata.get("lotSz", metadata.get("qty_step", 0.0))),
                default=0.0,
            )
            min_sz = _safe_float(
                metadata.get("min_sz", metadata.get("minSz", metadata.get("min_qty", 0.0))),
                default=0.0,
            )
            symbol_rule = None
            if ct_val <= 0.0 or lot_sz <= 0.0 or min_sz <= 0.0:
                try:
                    symbol_rule = await get_symbol_rules_cache().get(self, Venue.OKX, venue_sym)
                except Exception:
                    symbol_rule = None
                if symbol_rule is not None:
                    rule_lot_sz = _safe_float(
                        getattr(symbol_rule, "qty_step", 0.0),
                        default=0.0,
                    )
                    rule_min_sz = _safe_float(
                        getattr(symbol_rule, "min_qty", 0.0),
                        default=0.0,
                    )
                    if lot_sz <= 0.0 and rule_lot_sz > 0.0:
                        lot_sz = rule_lot_sz
                    if min_sz <= 0.0 and rule_min_sz > 0.0:
                        min_sz = rule_min_sz
                    if metadata and (ct_val > 0.0 or lot_sz > 0.0 or min_sz > 0.0):
                        merged = dict(metadata)
                        if ct_val > 0.0:
                            merged["ct_val"] = ct_val
                        if lot_sz > 0.0:
                            merged["lot_sz"] = lot_sz
                        if min_sz > 0.0:
                            merged["min_sz"] = min_sz
                        self._symbol_metadata[venue_sym] = merged
                        if symbol != venue_sym:
                            self._symbol_metadata[symbol] = merged
            diagnostics = _okx_contract_order_diagnostics(
                base_qty=float(quantity),
                ct_val=ct_val,
                lot_sz=lot_sz,
                min_sz=min_sz,
            )
            reject_reason = diagnostics.get("reject_reason")
            if reject_reason == "missing_ct_val":
                classification = (
                    "metadata_missing"
                    if metadata
                    else "instrument_missing"
                    if self._okx_swap_instruments_loaded
                    else "metadata_missing"
                )
                raise self._okx_missing_ct_val_error(venue_sym, classification)
            if reject_reason in ("contract_qty_zero", "contract_qty_below_min_sz"):
                return 0.0
            contract_qty = float(diagnostics.get("contract_qty", 0.0) or 0.0)
            return contract_qty * ct_val

        # Binance/Aster live mode: use dynamic exchangeInfo rules
        # to get per-symbol qty_step/min_qty instead of static VenueSpec
        # defaults, preventing -1111 LOT_SIZE errors on symbols with
        # non-standard precision (e.g. HIGHUSDT step=1).
        if spec.venue_id in (Venue.BINANCE, Venue.ASTER) and self.mode == "live":
            try:
                cache = get_symbol_rules_cache()
                rule = await cache.get(self, spec.venue_id, venue_sym)
                if rule and getattr(rule, "rule_source", "") == "exchangeInfo":
                    return normalize_venue_quantity(
                        quantity=quantity,
                        step_size=rule.qty_step,
                        contract_size=spec.contract_size,
                        min_quantity=rule.min_qty,
                    )
            except Exception:
                pass  # Fall through to static default

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

    def _hl_cache_asset_universe(self, universe: Any) -> None:
        from lightfee.venues.hyperliquid_signing import hyperliquid_price_decimals

        if not isinstance(universe, list):
            return
        for idx, entry in enumerate(universe):
            if not isinstance(entry, dict):
                continue
            name = str(entry.get("name", "") or "")
            if not name:
                continue
            sz_decimals = int(entry.get("szDecimals", 0) or 0)
            price_decimals = hyperliquid_price_decimals(idx, sz_decimals)
            self._hl_meta_cache[name] = idx
            self._hl_asset_meta_cache[name] = {
                "asset_index": idx,
                "sz_decimals": sz_decimals,
                "price_decimals": price_decimals,
            }
            self._symbol_metadata[name] = {
                **entry,
                "asset_index": idx,
                "sz_decimals": sz_decimals,
                "price_decimals": price_decimals,
            }

    def _hl_cached_asset_meta(self, asset_name: str) -> dict[str, int]:
        cached = self._hl_asset_meta_cache.get(asset_name)
        if cached is not None:
            return dict(cached)
        metadata = self._symbol_metadata.get(asset_name, {})
        if isinstance(metadata, dict) and "asset_index" in metadata:
            asset_index = int(metadata.get("asset_index", 0) or 0)
            sz_decimals = int(
                metadata.get("sz_decimals", metadata.get("szDecimals", 0)) or 0
            )
            price_decimals = int(metadata.get("price_decimals", 0) or 0)
            if price_decimals <= 0:
                from lightfee.venues.hyperliquid_signing import hyperliquid_price_decimals

                price_decimals = hyperliquid_price_decimals(asset_index, sz_decimals)
            return {
                "asset_index": asset_index,
                "sz_decimals": sz_decimals,
                "price_decimals": price_decimals,
            }
        if asset_name in self._hl_meta_cache:
            from lightfee.venues.hyperliquid_signing import hyperliquid_price_decimals

            asset_index = int(self._hl_meta_cache[asset_name])
            return {
                "asset_index": asset_index,
                "sz_decimals": 0,
                "price_decimals": hyperliquid_price_decimals(asset_index, 0),
            }
        raise ValueError(
            f"Hyperliquid asset '{asset_name}' not found in metadata universe"
        )

    async def _hl_resolve_asset_meta(self, asset_name: str) -> dict[str, int]:
        if asset_name in self._hl_asset_meta_cache:
            return dict(self._hl_asset_meta_cache[asset_name])
        if asset_name in self._hl_meta_cache:
            return self._hl_cached_asset_meta(asset_name)

        raw = await self._request(
            "POST", "/info",
            body={"type": "meta"},
            private=False,
        )
        universe = raw.get("universe", []) if isinstance(raw, dict) else []
        if isinstance(raw, list) and raw:
            universe = raw[0].get("universe", raw) if isinstance(raw[0], dict) else raw
        self._hl_cache_asset_universe(universe)
        return self._hl_cached_asset_meta(asset_name)

    async def _hl_resolve_asset_index(self, asset_name: str) -> int:
        """Return the Hyperliquid asset index for *asset_name*.

        Fetches ``POST /info {"type": "meta"}`` once and caches the
        name→index mapping.  The index is the 0-based position in the
        ``universe`` array — matching the Rust implementation.
        """
        return (await self._hl_resolve_asset_meta(asset_name))["asset_index"]

    async def _query_hyperliquid_order(
        self, spec, symbol: str, order_id: str, client_order_id: str | None, now_ms: int,
    ):
        """V1: query Hyperliquid info API for order status (hyperliquid.rs:2033-2130)."""
        if self._credential is None or not self._credential.account_address:
            return None
        user = self._credential.account_address if self._credential else ""
        if client_order_id:
            try:
                raw = await self._request(
                    "POST", "/info",
                    body={"type": "historicalOrders", "user": user},
                    private=False,
                )
                if raw is not None:
                    return (raw, True)
            except Exception:
                pass
        if order_id:
            try:
                oid = int(order_id)
                raw = await self._request(
                    "POST", "/info",
                    body={"type": "orderStatus", "user": user, "oid": oid},
                    private=False,
                )
                if raw is not None:
                    return (raw, False)
            except (ValueError, Exception):
                pass
        try:
            raw = await self._request(
                "POST", "/info",
                body={"type": "historicalOrders", "user": user},
                private=False,
            )
            if raw is not None:
                return (raw, True)
        except Exception:
            pass
        return None

    def _parse_hl_order_list(
        self, raw, spec, symbol: str, order_id: str,
        client_order_id: str | None, now_ms: int,
    ):
        """Parse Hyperliquid historicalOrders response for matching order."""
        from lightfee.core.domain import PassiveOrderProgress, PassiveOrderState, Side
        orders = raw if isinstance(raw, list) else raw.get("historicalOrders", raw.get("orders", []))
        client_order_ids: set[str] = set()
        if client_order_id:
            client_order_ids.add(client_order_id)
            from lightfee.venues.hyperliquid_signing import (
                hyperliquid_cloid_for_client_order,
            )
            client_order_ids.add(hyperliquid_cloid_for_client_order(client_order_id))
        for entry in (orders if isinstance(orders, list) else []):
            if not isinstance(entry, dict):
                continue
            entry_oid = str(entry.get("oid", ""))
            entry_cloid = str(entry.get("cloid", ""))
            if order_id and entry_oid != order_id:
                continue
            if client_order_ids and entry_cloid not in client_order_ids:
                continue
            status = str(entry.get("status", "")).lower()
            side_raw = str(entry.get("side", "")).upper()
            side = Side.BUY if side_raw == "B" else Side.SELL
            orig_sz = float(entry.get("origSz", 0))
            sz = float(entry.get("sz", orig_sz))
            total_sz = float(entry.get("totalSz", sz))
            limit_px = float(entry.get("limitPx", 0))
            avg_px = float(entry.get("avgPx", 0))
            oid = str(entry.get("oid", ""))
            if status == "filled":
                return PassiveOrderProgress(
                    venue=spec.venue_id, symbol=symbol, side=side,
                    order_id=oid, client_order_id=entry_cloid,
                    cumulative_quantity=total_sz,
                    average_price=avg_px if avg_px > 0 else limit_px,
                    fee_quote=0.0, last_fill_time_ms=now_ms,
                    state=PassiveOrderState.FILLED, observed_at_ms=now_ms,
                )
            if status in ("open", "resting", "triggered"):
                return PassiveOrderProgress(
                    venue=spec.venue_id, symbol=symbol, side=side,
                    order_id=oid, client_order_id=entry_cloid,
                    cumulative_quantity=orig_sz - sz,
                    average_price=limit_px,
                    fee_quote=0.0, last_fill_time_ms=now_ms,
                    state=PassiveOrderState.OPEN, observed_at_ms=now_ms,
                )
            if status in ("canceled", "rejected"):
                return PassiveOrderProgress(
                    venue=spec.venue_id, symbol=symbol, side=side,
                    order_id=oid, client_order_id=entry_cloid,
                    cumulative_quantity=orig_sz - sz,
                    average_price=0.0,
                    fee_quote=0.0, last_fill_time_ms=now_ms,
                    state=PassiveOrderState.CANCELED if status == "canceled" else PassiveOrderState.REJECTED,
                    observed_at_ms=now_ms,
                )
        return None
