"""Aster Pro API V3 private REST client.

Public Aster market data is still Binance-compatible FAPI. Private account,
position, and order surfaces are not: Pro API V3 uses an API-wallet signer with
an EIP-712 signature payload.
"""

from __future__ import annotations

import asyncio
import json
import math
import time
import urllib.parse
from typing import Any, Optional

import httpx

from lightfee.core.domain import (
    OrderFill,
    OrderRequest,
    PassiveOrderAck,
    PassiveOrderProgress,
    PassiveOrderState,
    PositionSnapshot,
    Side,
    TimeInForce,
    Venue,
)
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.engine.risk_actions import AccountRiskSnapshot
from lightfee.venues.transport import (
    LiveCredential,
    TransportError,
    TransportErrorCategory,
    _format_decimal,
    _normalize_host_scope,
    _normalize_rest_endpoint_key,
    _parse_binance_like_position,
    _parse_optional_float,
    _parse_venue_retry_after_ms,
    _safe_float,
)


ASTER_V3_PRIVATE_BASE_URL = "https://fapi.asterdex.com"
ASTER_V3_DOC_URL = (
    "https://github.com/asterdex/api-docs/blob/master/"
    "V3%28Recommended%29/EN/aster-finance-futures-api-v3.md"
)

ASTER_V3_ORDER_PATH = "/fapi/v3/order"
ASTER_V3_USER_TRADES_PATH = "/fapi/v3/userTrades"
ASTER_V3_OPEN_ORDERS_PATH = "/fapi/v3/openOrders"
ASTER_V3_POSITION_PATH = "/fapi/v3/positionRisk"
ASTER_V3_POSITION_MODE_PATH = "/fapi/v3/positionSide/dual"
ASTER_V3_ACCOUNT_PATH = "/fapi/v3/accountWithJoinMargin"
ASTER_V3_LEVERAGE_PATH = "/fapi/v3/leverage"
ASTER_V3_LEVERAGE_BRACKET_PATH = "/fapi/v3/leverageBracket"


def _missing_aster_v3_signing_dependencies() -> list[str]:
    import importlib.util

    return [] if importlib.util.find_spec("eth_account") is not None else ["eth-account"]


def _normalize_private_key(value: str) -> str:
    key = str(value or "").strip()
    if key and not key.startswith("0x"):
        key = "0x" + key
    return key


def _derive_signer_address(private_key: str) -> str:
    missing = _missing_aster_v3_signing_dependencies()
    if missing:
        raise ValueError(
            "live mode missing signing dependencies for aster: "
            + ", ".join(missing)
        )
    try:
        from eth_account import Account

        return str(Account.from_key(private_key).address)
    except Exception as exc:
        raise ValueError("failed to derive aster signer from private key") from exc


def _aster_v3_typed_data(message: str) -> dict[str, Any]:
    return {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "Message": [{"name": "msg", "type": "string"}],
        },
        "primaryType": "Message",
        "domain": {
            "name": "AsterSignTransaction",
            "version": "1",
            "chainId": 1666,
            "verifyingContract": "0x0000000000000000000000000000000000000000",
        },
        "message": {"msg": message},
    }


def _sign_aster_v3_message(message: str, private_key: str) -> str:
    try:
        from eth_account import Account
        from eth_account.messages import encode_typed_data

        signable = encode_typed_data(full_message=_aster_v3_typed_data(message))
        signed = Account.sign_message(signable, private_key=private_key)
        return signed.signature.hex()
    except Exception as exc:
        raise ValueError("failed to sign aster v3 request") from exc


def _microsecond_nonce() -> int:
    return int(time.time() * 1_000_000)


def _body_is_invalid_symbol(body: str) -> bool:
    """Return True if an Aster response body is a -1121 Invalid symbol."""
    text = str(body or "").lower()
    return "-1121" in text or "invalid symbol" in text


def _extract_rows(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        return [row for row in raw if isinstance(row, dict)]
    if not isinstance(raw, dict):
        return []
    data = raw.get("data", raw)
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        result = data.get("result")
        if isinstance(result, list):
            return [row for row in result if isinstance(row, dict)]
        if isinstance(result, dict):
            return [result]
        return [data]
    return []


def _require_aster_v3_success(raw: Any, context: str) -> None:
    if not isinstance(raw, dict):
        return
    code = raw.get("code")
    if code is None:
        return
    code_s = str(code)
    if code_s in ("0", "200"):
        return
    raise OrderSubmitError(
        SubmitFailureClass.REJECTED,
        f"{context}: aster_v3 code={code_s} msg={raw.get('msg', '')}",
    )


def _order_state(raw_status: Any, filled_qty: float = 0.0) -> PassiveOrderState:
    status = str(raw_status or "").upper()
    if status in ("NEW", "OPEN", "ACTIVE"):
        return PassiveOrderState.PARTIALLY_FILLED if filled_qty > 0 else PassiveOrderState.OPEN
    if status in ("PARTIALLY_FILLED", "PARTIAL"):
        return PassiveOrderState.PARTIALLY_FILLED
    if status in ("FILLED", "CLOSED", "FINISHED", "COMPLETED", "DONE"):
        return PassiveOrderState.FILLED
    if status in ("CANCELED", "CANCELLED"):
        return PassiveOrderState.CANCELED
    if status == "EXPIRED":
        return PassiveOrderState.EXPIRED
    if status == "REJECTED":
        return PassiveOrderState.REJECTED
    return PassiveOrderState.UNKNOWN


def _aster_hedge_position_side(side: Side, reduce_only: bool) -> str:
    """Map the V2 signed side into Aster's documented Hedge-mode position side."""
    if reduce_only:
        return "SHORT" if side == Side.BUY else "LONG"
    return "LONG" if side == Side.BUY else "SHORT"


class AsterV3Client:
    """Private Aster Pro API V3 client isolated from Binance HMAC transport."""

    def __init__(
        self,
        credential: LiveCredential,
        exchange_http_timeout_ms: int = 10000,
        http_client: Optional[httpx.AsyncClient] = None,
        rate_limiter: Any = None,
    ) -> None:
        private_key = credential.wallet_private_key or credential.api_secret
        private_key = _normalize_private_key(private_key)
        if not private_key:
            raise ValueError(
                "live mode requires LIGHTFEE_ASTER_WALLET_PRIVATE_KEY "
                "or LIGHTFEE_ASTER_API_SECRET containing the Aster API-wallet private key"
            )
        self._credential = credential
        self._private_key = private_key
        self._signer = _derive_signer_address(private_key)
        self._user = str(credential.account_address or "").strip()
        self._secret_source = (
            "wallet_private_key"
            if credential.wallet_private_key
            else "api_secret_legacy_private_key"
        )
        self._client = http_client or httpx.AsyncClient(
            timeout=exchange_http_timeout_ms / 1000.0
        )
        self._owns_client = http_client is None
        self._rate_limiter = rate_limiter
        # Aster documents this as account-level state.  Query it once on the
        # private order path, then retain the V1-style client-lifetime cache.
        self._position_mode_is_hedge: bool | None = None
        self._position_mode_lock = asyncio.Lock()

    @property
    def signer_address(self) -> str:
        return self._signer

    @property
    def user_address(self) -> str:
        return self._user

    @property
    def credential_source(self) -> str:
        return self._secret_source

    def build_signed_request(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
        *,
        nonce: int | None = None,
    ) -> tuple[str, dict[str, str], None]:
        signed_params: dict[str, str] = {}
        for key, value in (params or {}).items():
            if value is None:
                continue
            signed_params[str(key)] = str(value)
        signed_params["nonce"] = str(nonce if nonce is not None else _microsecond_nonce())
        signed_params["signer"] = self._signer
        if self._user:
            signed_params["user"] = self._user

        encoded = urllib.parse.urlencode(signed_params)
        signature = _sign_aster_v3_message(encoded, self._private_key)
        signed_params["signature"] = signature
        query = "?" + urllib.parse.urlencode(signed_params)
        return query, {"Content-Type": "application/x-www-form-urlencoded"}, None

    def _rest_rate_limit_scopes(self, method: str, path: str) -> list[str]:
        endpoint = _normalize_rest_endpoint_key(method, path)
        scopes = [
            endpoint,
            _normalize_host_scope(ASTER_V3_PRIVATE_BASE_URL),
            "venue:aster",
        ]

        from lightfee.rate_limit.config import built_in_defaults
        from lightfee.rate_limit.engine import global_rate_limit_runtime

        global_rt = global_rate_limit_runtime()
        if global_rt is not None and global_rt.config_manager is not None:
            config = global_rt.config_manager.config
        else:
            config = built_in_defaults()

        venue_config = config.venues.get("aster") if config else None
        if venue_config is not None:
            group_name = (getattr(venue_config, "scopes", {}) or {}).get(endpoint)
            if group_name:
                scopes.append(f"group:aster:{group_name}")
                scopes.append(f"group:{group_name}")
        return scopes

    def _request_weight_override(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]],
    ) -> float | None:
        if (
            method.upper() == "GET"
            and path == ASTER_V3_OPEN_ORDERS_PATH
            and not (params or {}).get("symbol")
        ):
            return 40.0
        return None

    async def _wait_until_rate_limit_ready(
        self,
        scopes: list[str],
        weight_override: float | None,
    ) -> None:
        if self._rate_limiter is not None:
            await self._rate_limiter.wait_until_ready_for_scopes(scopes)
            await self._rate_limiter.pace_for_scopes(scopes)

        from lightfee.rate_limit.engine import global_rate_limit_runtime

        global_rt = global_rate_limit_runtime()
        if global_rt is not None:
            await global_rt.async_wait_until_ready_for_scopes(
                scopes,
                weight_override=weight_override,
            )

    def _record_rate_limit_response(
        self,
        scopes: list[str],
        response: httpx.Response,
    ) -> None:
        retry_after_ms = _parse_venue_retry_after_ms(
            Venue.ASTER,
            dict(response.headers),
            int(time.time() * 1000),
        )
        if self._rate_limiter is not None:
            self._rate_limiter.record_rate_limit_for_scopes(
                scopes,
                retry_after_ms=retry_after_ms,
            )

        from lightfee.rate_limit.engine import global_rate_limit_runtime

        global_rt = global_rate_limit_runtime()
        if global_rt is not None:
            global_rt.record_rate_limit_for_scopes(
                scopes,
                retry_after_ms=retry_after_ms or 0,
            )

    def _record_success_response(self, scopes: list[str]) -> None:
        if self._rate_limiter is not None:
            self._rate_limiter.record_success_for_scopes(scopes)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
    ) -> Any:
        method_upper = method.upper()
        scopes = self._rest_rate_limit_scopes(method_upper, path)
        weight_override = self._request_weight_override(method_upper, path, params)
        await self._wait_until_rate_limit_ready(scopes, weight_override)

        query, headers, body = self.build_signed_request(method, path, params=params)
        url = ASTER_V3_PRIVATE_BASE_URL.rstrip("/") + path + query
        try:
            response = await self._client.request(
                method_upper,
                url,
                headers=headers,
                content=body,
            )
        except httpx.TimeoutException as exc:
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                f"aster_v3 request timeout: {method.upper()} {path}",
            ) from exc
        except httpx.HTTPError as exc:
            # A bare ConnectError has an empty str(exc); preserve the exception
            # class (and root cause class) so the diagnostic is classifiable.
            # Never include the request URL: it carries the signed query.
            cause = exc.__cause__
            detail = str(exc) or type(exc).__name__
            if cause is not None:
                detail = f"{detail} ({type(cause).__name__})" if detail else type(cause).__name__
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                f"aster_v3 request failed: {method.upper()} {path}: {detail}",
            ) from exc

        text = response.text
        if response.status_code >= 400:
            if response.status_code in (429, 418):
                self._record_rate_limit_response(scopes, response)
            category = (
                TransportErrorCategory.AUTH_FAILURE
                if response.status_code in (401, 403)
                else TransportErrorCategory.REQUEST_REJECTED
            )
            exc = TransportError(
                category,
                f"aster_v3 {method_upper} {path} rejected status={response.status_code}",
                status_code=response.status_code,
                body=text,
                headers=dict(response.headers),
            )
            if _body_is_invalid_symbol(text):
                exc.invalid_symbol = True
                exc.invalid_symbol_body = exc.body
            raise exc
        try:
            raw = response.json()
        except ValueError:
            raw = {}
        if isinstance(raw, dict) and str(raw.get("code", "0")) not in ("0", "200"):
            exc = TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                f"aster_v3 {method_upper} {path} rejected code={raw.get('code')} msg={raw.get('msg', '')}",
                status_code=response.status_code,
                body=json.dumps(raw, ensure_ascii=False),
                headers=dict(response.headers),
            )
            if str(raw.get("code", "")) == "-1121":
                exc.invalid_symbol = True
                exc.invalid_symbol_body = exc.body
            raise exc
        self._record_success_response(scopes)
        return raw

    async def fetch_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        raw = await self._request("GET", ASTER_V3_OPEN_ORDERS_PATH, params=params)
        return _extract_rows(raw)

    async def _position_mode_hedge(self) -> bool:
        cached = self._position_mode_is_hedge
        if cached is not None:
            return cached
        async with self._position_mode_lock:
            cached = self._position_mode_is_hedge
            if cached is not None:
                return cached
            raw = await self._request("GET", ASTER_V3_POSITION_MODE_PATH)
            data = raw.get("data", raw) if isinstance(raw, dict) else raw
            value = data.get("dualSidePosition") if isinstance(data, dict) else None
            if not isinstance(value, bool):
                raise TransportError(
                    TransportErrorCategory.REQUEST_REJECTED,
                    "aster_v3 position mode response missing boolean dualSidePosition",
                    body=json.dumps(raw, ensure_ascii=False),
                )
            self._position_mode_is_hedge = value
            return value

    async def fetch_all_positions(self) -> list[PositionSnapshot]:
        raw = await self._request("GET", ASTER_V3_POSITION_PATH)
        now_ms = int(time.time() * 1000)
        rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
        for row in _extract_rows(raw):
            symbol = str(row.get("symbol", "") or "")
            if symbol:
                rows_by_symbol.setdefault(symbol, []).append(row)
        positions: list[PositionSnapshot] = []
        for symbol, rows in rows_by_symbol.items():
            pos = _parse_binance_like_position(rows, symbol, now_ms, venue=Venue.ASTER)
            if abs(pos.quantity) > 1e-9:
                positions.append(pos)
        return positions

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        raw = await self._request(
            "GET", ASTER_V3_POSITION_PATH, params={"symbol": symbol}
        )
        now_ms = int(time.time() * 1000)
        rows = _extract_rows(raw)
        return _parse_binance_like_position(rows, symbol, now_ms, venue=Venue.ASTER)

    async def precheck_order_admission(self, request: OrderRequest) -> dict[str, Any]:
        """Prove an Aster V3 opening order fits current documented capacity.

        Aster V3 does not expose the legacy
        ``remainingOpenableNotionalValue`` endpoint.  Its documented V3
        ``positionRisk`` response supplies ``maxNotionalValue`` instead, so
        opening capacity is derived only from V3 position and open-order truth.
        Missing or malformed evidence is fail-closed: a paired entry must not
        make its other leg live when the Aster hedge cannot be proven admissible.
        """
        if request.reduce_only:
            return {
                "venue": Venue.ASTER.value,
                "symbol": request.symbol,
                "status": "skipped",
                "reason": "reduce_only_exempt",
            }

        price = next(
            (
                float(value)
                for value in (
                    request.price,
                    request.price_hint,
                    request.mark_price_hint,
                )
                if value is not None and math.isfinite(float(value)) and float(value) > 0.0
            ),
            0.0,
        )
        quantity = float(request.quantity)
        if not math.isfinite(quantity) or quantity <= 0.0:
            raise OrderSubmitError(
                SubmitFailureClass.REJECTED,
                "aster v3 capacity precheck rejected: order notional evidence unavailable",
            )

        position_raw = await self._request(
            "GET",
            ASTER_V3_POSITION_PATH,
            params={"symbol": request.symbol},
        )
        rows = [
            row
            for row in _extract_rows(position_raw)
            if str(row.get("symbol", request.symbol) or request.symbol) == request.symbol
        ]
        if not rows:
            raise OrderSubmitError(
                SubmitFailureClass.REJECTED,
                "aster v3 capacity precheck rejected: positionRisk evidence unavailable",
            )

        capacity_limits: list[float] = []
        observed_mark_prices: list[float] = []
        current_position_notional = 0.0
        for row in rows:
            max_notional = _parse_optional_float(row.get("maxNotionalValue"))
            if max_notional is None or not math.isfinite(max_notional) or max_notional <= 0.0:
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    "aster v3 capacity precheck rejected: maxNotionalValue unavailable",
                )
            capacity_limits.append(max_notional)

            mark_price = _parse_optional_float(row.get("markPrice"))
            if (
                mark_price is not None
                and math.isfinite(mark_price)
                and mark_price > 0.0
            ):
                observed_mark_prices.append(mark_price)

            position_notional = _parse_optional_float(row.get("notional"))
            if position_notional is None:
                position_amount = _safe_float(row.get("positionAmt"), default=0.0)
                if not math.isfinite(position_amount):
                    raise OrderSubmitError(
                        SubmitFailureClass.REJECTED,
                        "aster v3 capacity precheck rejected: position amount unavailable",
                    )
                if abs(position_amount) > 1e-9:
                    if (
                        mark_price is None
                        or not math.isfinite(mark_price)
                        or mark_price <= 0.0
                    ):
                        raise OrderSubmitError(
                            SubmitFailureClass.REJECTED,
                            "aster v3 capacity precheck rejected: position mark price unavailable",
                        )
                    position_notional = position_amount * mark_price
                else:
                    position_notional = 0.0
            if not math.isfinite(position_notional):
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    "aster v3 capacity precheck rejected: position notional unavailable",
                )
            current_position_notional += abs(position_notional)

        if price <= 0.0 and observed_mark_prices:
            price = observed_mark_prices[0]
        if price <= 0.0:
            raise OrderSubmitError(
                SubmitFailureClass.REJECTED,
                "aster v3 capacity precheck rejected: order notional evidence unavailable",
            )
        requested_notional = abs(quantity * price)

        open_orders_raw = await self._request(
            "GET",
            ASTER_V3_OPEN_ORDERS_PATH,
            params={"symbol": request.symbol},
        )
        open_order_notional = 0.0
        for order in _extract_rows(open_orders_raw):
            if str(order.get("symbol", request.symbol) or request.symbol) != request.symbol:
                continue
            if str(order.get("reduceOnly", "false")).strip().lower() in {
                "1",
                "true",
                "yes",
            }:
                continue
            open_quantity = _safe_float(
                order.get("origQty", order.get("quantity", order.get("qty"))),
                default=0.0,
            )
            if not math.isfinite(open_quantity) or open_quantity <= 1e-9:
                continue
            open_price = _parse_optional_float(order.get("price"))
            if open_price is None or not math.isfinite(open_price) or open_price <= 0.0:
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    "aster v3 capacity precheck rejected: open-order notional unavailable",
                )
            open_order_notional += abs(open_quantity * open_price)

        max_notional = min(capacity_limits)
        consumed_notional = current_position_notional + open_order_notional
        remaining_notional = max_notional - consumed_notional
        if requested_notional > remaining_notional + 1e-9:
            raise OrderSubmitError(
                SubmitFailureClass.REJECTED,
                "aster v3 capacity precheck rejected: maximum notional value limit "
                f"(requested={requested_notional:.8f}, remaining={max(remaining_notional, 0.0):.8f})",
            )
        return {
            "venue": Venue.ASTER.value,
            "symbol": request.symbol,
            "status": "ok",
            "requested_notional": requested_notional,
            "current_position_notional": current_position_notional,
            "open_order_notional": open_order_notional,
            "max_notional_value": max_notional,
            "remaining_notional": remaining_notional,
            "source": "aster_v3_position_risk_and_open_orders",
        }

    async def fetch_account_risk_snapshot(self) -> AccountRiskSnapshot | None:
        raw = await self._request("GET", ASTER_V3_ACCOUNT_PATH)
        rows = _extract_rows(raw)
        data = rows[0] if rows else raw if isinstance(raw, dict) else {}
        if not isinstance(data, dict):
            return None
        equity = _parse_optional_float(data.get("totalMarginBalance"))
        maint = _parse_optional_float(data.get("totalMaintMargin"))
        if equity is None or maint is None or maint <= 0:
            return None
        snapshot = AccountRiskSnapshot(
            venue=Venue.ASTER,
            equity_quote=equity,
            maintenance_margin_quote=maint,
            health_ratio=equity / maint,
            observed_at_ms=int(time.time() * 1000),
            source="aster_v3_account_with_join_margin",
        )
        snapshot.available_balance_quote = _parse_optional_float(
            data.get("availableBalance")
        )
        return snapshot

    async def ensure_entry_leverage(
        self,
        symbol: str,
        leverage: int,
        *,
        notional_quote: float | None = None,
    ) -> None:
        target = int(leverage or 0)
        if target <= 0:
            return

        position_raw = await self._request(
            "GET",
            ASTER_V3_POSITION_PATH,
            params={"symbol": symbol},
        )
        current_leverages: set[int] = set()
        for row in _extract_rows(position_raw):
            row_symbol = str(row.get("symbol", symbol) or symbol)
            if row_symbol != symbol:
                continue
            lev = int(_safe_float(row.get("leverage"), default=0.0))
            if lev > 0:
                current_leverages.add(lev)
        current = next(iter(current_leverages)) if len(current_leverages) == 1 else 0

        effective = target
        try:
            bracket_raw = await self._request(
                "GET",
                ASTER_V3_LEVERAGE_BRACKET_PATH,
                params={"symbol": symbol},
            )
            notional = max(float(notional_quote or 0.0), 0.0)
            matched = False
            for row in _extract_rows(bracket_raw):
                brackets = row.get("brackets") if isinstance(row, dict) else None
                if not isinstance(brackets, list):
                    continue
                for bracket in brackets:
                    if not isinstance(bracket, dict):
                        continue
                    floor = _safe_float(bracket.get("notionalFloor"), default=0.0)
                    cap = _safe_float(bracket.get("notionalCap"), default=0.0)
                    max_leverage = int(_safe_float(bracket.get("initialLeverage"), default=0.0))
                    if max_leverage <= 0:
                        continue
                    if cap <= 0 or (notional + 1e-12 >= floor and notional <= cap + 1e-12):
                        effective = min(target, max_leverage)
                        matched = True
                        break
                if matched:
                    break
        except TransportError:
            effective = target

        if current == effective:
            return
        await self._request(
            "POST",
            ASTER_V3_LEVERAGE_PATH,
            params={"symbol": symbol, "leverage": effective},
        )

    def _order_params(
        self,
        request: OrderRequest,
        *,
        passive: bool,
        hedge_mode: bool,
    ) -> dict[str, Any]:
        use_limit = passive or (
            request.price is not None and request.time_in_force != TimeInForce.IOC
        )
        params: dict[str, Any] = {
            "symbol": request.symbol,
            "side": request.side.value.upper(),
            "type": "LIMIT" if use_limit else "MARKET",
            "quantity": _format_decimal(request.quantity),
        }
        if use_limit:
            if request.price is None:
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    "aster_v3 limit order requires price",
                )
            params["price"] = _format_decimal(request.price)
            params["timeInForce"] = (
                "GTX"
                if passive or request.post_only or request.time_in_force == TimeInForce.POST_ONLY
                else "GTC"
            )
        # Aster V3 rejects timeInForce on MARKET orders. IOC remains domain
        # execution intent for the close/hedge callers, not a MARKET wire field.
        if hedge_mode:
            # Hedge Mode requires positionSide and rejects reduceOnly.
            params["positionSide"] = _aster_hedge_position_side(
                request.side, request.reduce_only
            )
        elif request.reduce_only:
            params["reduceOnly"] = "true"
        if request.client_order_id:
            params["newClientOrderId"] = request.client_order_id
        return params

    def _parse_order_fill(
        self,
        raw: Any,
        request: OrderRequest,
        now_ms: int,
    ) -> OrderFill:
        _require_aster_v3_success(raw, "aster v3 order failed")
        rows = _extract_rows(raw)
        data = rows[0] if rows else raw if isinstance(raw, dict) else {}
        if not isinstance(data, dict):
            data = {}
        order_id = str(data.get("orderId", data.get("id", "")) or "")
        executed_qty = _safe_float(data.get("executedQty"), default=0.0)
        avg_price = _safe_float(
            data.get("avgPrice", data.get("price", request.price or 0.0)),
            default=0.0,
        )
        if executed_qty <= 0 and order_id:
            err = OrderSubmitError(
                SubmitFailureClass.UNCERTAIN,
                f"aster_v3 order accepted (id={order_id}) but fill not confirmed",
            )
            err.order_ack_only = True
            err.accepted_order_id = order_id
            err.accepted_client_order_id = str(
                data.get("clientOrderId", request.client_order_id or "") or ""
            )
            err.exchange_response_body = json.dumps(raw, separators=(",", ":"))
            raise err
        if executed_qty <= 0:
            raise OrderSubmitError(
                SubmitFailureClass.UNCERTAIN,
                "aster_v3 order response contains no executedQty",
            )
        return OrderFill(
            venue=Venue.ASTER,
            symbol=request.symbol,
            side=request.side,
            quantity=executed_qty,
            price=avg_price,
            order_id=order_id,
            client_order_id=str(data.get("clientOrderId", request.client_order_id or "") or ""),
            filled_at_ms=now_ms,
        )

    async def place_order(self, request: OrderRequest) -> OrderFill:
        try:
            params = self._order_params(
                request,
                passive=False,
                hedge_mode=await self._position_mode_hedge(),
            )
            # V3 defaults to ACK, which cannot prove the fill parsed below.
            params["newOrderRespType"] = "RESULT"
            raw = await self._request("POST", ASTER_V3_ORDER_PATH, params=params)
        except TransportError as exc:
            if exc.category in (
                TransportErrorCategory.AUTH_FAILURE,
                TransportErrorCategory.AUTHORIZATION_FAILURE,
                TransportErrorCategory.REQUEST_REJECTED,
            ):
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    f"aster_v3 order rejected: {exc}",
                ) from exc
            raise
        return self._parse_order_fill(raw, request, int(time.time() * 1000))

    async def submit_passive_order(self, request: OrderRequest) -> PassiveOrderAck:
        try:
            params = self._order_params(
                request,
                passive=True,
                hedge_mode=await self._position_mode_hedge(),
            )
            raw = await self._request("POST", ASTER_V3_ORDER_PATH, params=params)
        except TransportError as exc:
            if exc.category in (
                TransportErrorCategory.AUTH_FAILURE,
                TransportErrorCategory.AUTHORIZATION_FAILURE,
                TransportErrorCategory.REQUEST_REJECTED,
            ):
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    f"aster_v3 passive order rejected: {exc}",
                ) from exc
            raise
        _require_aster_v3_success(raw, "aster v3 passive order failed")
        rows = _extract_rows(raw)
        data = rows[0] if rows else raw if isinstance(raw, dict) else {}
        if not isinstance(data, dict):
            data = {}
        now_ms = int(time.time() * 1000)
        return PassiveOrderAck(
            venue=Venue.ASTER,
            symbol=request.symbol,
            side=request.side,
            order_id=str(data.get("orderId", data.get("id", "")) or ""),
            client_order_id=str(data.get("clientOrderId", request.client_order_id or "") or ""),
            price=_safe_float(data.get("price", request.price or 0.0), default=0.0),
            quantity=_safe_float(data.get("origQty", request.quantity), default=request.quantity),
            accepted_at_ms=now_ms,
            state=_order_state(data.get("status"), _safe_float(data.get("executedQty"), default=0.0)),
        )

    async def query_passive_order_progress(
        self,
        symbol: str,
        order_id: str,
        client_order_id: Optional[str] = None,
        side: Side | None = None,
    ) -> PassiveOrderProgress | None:
        params: dict[str, Any] = {"symbol": symbol}
        if order_id:
            params["orderId"] = order_id
        elif client_order_id:
            params["origClientOrderId"] = client_order_id
        else:
            return None
        try:
            raw = await self._request("GET", ASTER_V3_ORDER_PATH, params=params)
        except TransportError as exc:
            if getattr(exc, "invalid_symbol", False):
                # A -1121 here is proof the symbol is no longer tradable; the
                # caller must mark it unsupported and fail closed rather than
                # treat the missing progress as a benign retry signal.
                raise OrderSubmitError(
                    SubmitFailureClass.REJECTED,
                    f"aster_v3 query invalid symbol: {exc}",
                ) from exc
            return None
        rows = _extract_rows(raw)
        data = rows[0] if rows else raw if isinstance(raw, dict) else {}
        if not isinstance(data, dict):
            return None
        now_ms = int(time.time() * 1000)
        cum_qty = _safe_float(data.get("executedQty"), default=0.0)
        return PassiveOrderProgress(
            venue=Venue.ASTER,
            symbol=symbol,
            side=side or (Side.BUY if str(data.get("side", "BUY")).upper() == "BUY" else Side.SELL),
            order_id=str(data.get("orderId", order_id) or ""),
            client_order_id=str(data.get("clientOrderId", client_order_id or "") or ""),
            cumulative_quantity=cum_qty,
            average_price=_safe_float(data.get("avgPrice", data.get("price", 0)), default=0.0),
            fee_quote=None,
            last_fill_time_ms=int(_safe_float(data.get("updateTime"), default=0.0)),
            state=_order_state(data.get("status"), cum_qty),
            observed_at_ms=now_ms,
        )

    async def cancel_passive_order(
        self,
        symbol: str,
        order_id: str,
        client_order_id: Optional[str] = None,
    ) -> PassiveOrderAck:
        params: dict[str, Any] = {"symbol": symbol}
        if order_id:
            params["orderId"] = order_id
        elif client_order_id:
            params["origClientOrderId"] = client_order_id
        raw = await self._request("DELETE", ASTER_V3_ORDER_PATH, params=params)
        _require_aster_v3_success(raw, "aster v3 cancel order failed")
        rows = _extract_rows(raw)
        data = rows[0] if rows else raw if isinstance(raw, dict) else {}
        if not isinstance(data, dict):
            data = {}
        return PassiveOrderAck(
            venue=Venue.ASTER,
            symbol=symbol,
            side=Side.BUY,
            order_id=str(data.get("orderId", order_id) or ""),
            client_order_id=str(data.get("clientOrderId", client_order_id or "") or ""),
            price=_safe_float(data.get("price"), default=0.0),
            quantity=_safe_float(data.get("origQty"), default=0.0),
            accepted_at_ms=int(time.time() * 1000),
            state=PassiveOrderState.CANCELED,
        )

    async def fetch_order_status(
        self,
        symbol: str,
        order_id: str = "",
        client_order_id: Optional[str] = None,
    ) -> Optional[OrderFill]:
        request = OrderRequest(
            venue=Venue.ASTER,
            symbol=symbol,
            side=Side.BUY,
            quantity=0.0,
            client_order_id=client_order_id,
        )
        progress = await self.query_passive_order_progress(
            symbol,
            order_id,
            client_order_id,
        )
        if progress is None or progress.cumulative_quantity <= 0:
            return None
        return OrderFill(
            venue=Venue.ASTER,
            symbol=symbol,
            side=progress.side,
            quantity=progress.cumulative_quantity,
            price=progress.average_price,
            order_id=progress.order_id,
            client_order_id=progress.client_order_id or request.client_order_id,
            filled_at_ms=progress.last_fill_time_ms or progress.observed_at_ms,
        )

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()


def credential_has_aster_v3_signer(credential: LiveCredential | None) -> bool:
    if credential is None or _missing_aster_v3_signing_dependencies():
        return False
    private_key = _normalize_private_key(
        credential.wallet_private_key or credential.api_secret
    )
    if not private_key:
        return False
    try:
        _derive_signer_address(private_key)
    except ValueError:
        return False
    return True
