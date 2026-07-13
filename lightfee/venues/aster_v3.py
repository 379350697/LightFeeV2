"""Aster Pro API V3 private REST client.

Public Aster market data is still Binance-compatible FAPI. Private account,
position, and order surfaces are not: Pro API V3 uses an API-wallet signer with
an EIP-712 signature payload.
"""

from __future__ import annotations

import json
import time
import urllib.parse
from typing import Any, Optional

import httpx

from lightfee.core.domain import (
    EntryLeverageEvidence,
    OrderFill,
    OrderFillReconciliation,
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
ASTER_V3_ACCOUNT_PATH = "/fapi/v3/accountWithJoinMargin"
ASTER_V3_LEVERAGE_PATH = "/fapi/v3/leverage"
ASTER_V3_LEVERAGE_BRACKET_PATH = "/fapi/v3/leverageBracket"
ASTER_REMAINING_OPENABLE_NOTIONAL_PATH = "/fapi/v1/remainingOpenableNotionalValue"


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
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                f"aster_v3 request failed: {method.upper()} {path}: {exc}",
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
            raise TransportError(
                category,
                f"aster_v3 {method_upper} {path} rejected status={response.status_code}",
                status_code=response.status_code,
                body=text,
                headers=dict(response.headers),
            )
        try:
            raw = response.json()
        except ValueError:
            raw = {}
        if isinstance(raw, dict) and str(raw.get("code", "0")) not in ("0", "200"):
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                f"aster_v3 {method_upper} {path} rejected code={raw.get('code')} msg={raw.get('msg', '')}",
                status_code=response.status_code,
                body=json.dumps(raw, ensure_ascii=False),
                headers=dict(response.headers),
            )
        self._record_success_response(scopes)
        return raw

    async def fetch_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        raw = await self._request("GET", ASTER_V3_OPEN_ORDERS_PATH, params=params)
        return _extract_rows(raw)

    async def fetch_all_positions(self) -> list[PositionSnapshot]:
        raw = await self._request("GET", ASTER_V3_POSITION_PATH)
        now_ms = int(time.time() * 1000)
        positions: list[PositionSnapshot] = []
        for row in _extract_rows(raw):
            symbol = str(row.get("symbol", "") or "")
            if not symbol:
                continue
            pos = _parse_binance_like_position(row, symbol, now_ms, venue=Venue.ASTER)
            if abs(pos.quantity) > 1e-9:
                positions.append(pos)
        return positions

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        raw = await self._request(
            "GET", ASTER_V3_POSITION_PATH, params={"symbol": symbol}
        )
        now_ms = int(time.time() * 1000)
        rows = _extract_rows(raw)
        data = rows[0] if rows else raw if isinstance(raw, dict) else {}
        if not isinstance(data, dict):
            data = {}
        return _parse_binance_like_position(data, symbol, now_ms, venue=Venue.ASTER)

    async def fetch_account_risk_snapshot(self) -> AccountRiskSnapshot | None:
        raw = await self._request("GET", ASTER_V3_ACCOUNT_PATH)
        rows = _extract_rows(raw)
        data = rows[0] if rows else raw if isinstance(raw, dict) else {}
        if not isinstance(data, dict):
            return None
        equity = _parse_optional_float(data.get("totalMarginBalance"))
        maint = _parse_optional_float(data.get("totalMaintMargin"))
        available = _parse_optional_float(data.get("availableBalance"))
        if equity is None:
            equity = available
        if equity is None:
            return None
        maint_for_snapshot = maint if maint is not None and maint > 0 else 1e-9
        snapshot = AccountRiskSnapshot(
            venue=Venue.ASTER,
            equity_quote=equity,
            maintenance_margin_quote=maint_for_snapshot,
            health_ratio=equity / maint_for_snapshot,
            observed_at_ms=int(time.time() * 1000),
            source="aster_v3_account_with_join_margin",
        )
        snapshot.available_balance_quote = available
        return snapshot

    async def fetch_remaining_openable_notional(
        self,
        symbol: str,
        leverage: int,
    ) -> float | None:
        raw = await self._request(
            "GET",
            ASTER_REMAINING_OPENABLE_NOTIONAL_PATH,
            params={"symbol": symbol, "leverage": int(leverage or 0)},
        )
        value = raw.get("remainingOpenableNotionalValue") if isinstance(raw, dict) else None
        remaining = _safe_float(value, default=-1.0)
        if remaining >= 0.0:
            return remaining
        return None

    async def ensure_entry_leverage(
        self,
        symbol: str,
        leverage: int,
        *,
        notional_quote: float | None = None,
    ) -> EntryLeverageEvidence | None:
        target = int(leverage or 0)
        if target <= 0:
            return None

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
        bracket_verified = False
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
            bracket_verified = matched
        except TransportError:
            effective = target

        if current == effective:
            return EntryLeverageEvidence(
                venue=Venue.ASTER,
                symbol=symbol,
                requested_leverage=target,
                effective_leverage=effective,
                notional_quote=max(float(notional_quote or 0.0), 0.0),
                bracket_verified=bracket_verified,
                account_verified=bracket_verified,
                source="aster_v3_position_risk",
                observed_at_ms=int(time.time() * 1000),
            )
        response = await self._request(
            "POST",
            ASTER_V3_LEVERAGE_PATH,
            params={"symbol": symbol, "leverage": effective},
        )
        response_leverage = int(
            _safe_float(
                response.get("leverage") if isinstance(response, dict) else None,
                default=0.0,
            )
        )
        if response_leverage and response_leverage != effective:
            raise OrderSubmitError(
                SubmitFailureClass.REJECTED,
                "entry leverage prepare returned unexpected leverage "
                f"symbol={symbol} expected={effective} actual={response_leverage}",
            )
        # Do not promote a set-leverage acknowledgement into sizing evidence.
        # The position endpoint is the account truth source and catches a
        # concurrent mutation or an asynchronously applied venue setting.
        post_set_raw = await self._request(
            "GET",
            ASTER_V3_POSITION_PATH,
            params={"symbol": symbol},
        )
        post_set_leverages: set[int] = set()
        for row in _extract_rows(post_set_raw):
            row_symbol = str(row.get("symbol", symbol) or symbol)
            if row_symbol != symbol:
                continue
            value = int(_safe_float(row.get("leverage"), default=0.0))
            if value > 0:
                post_set_leverages.add(value)
        post_set_leverage = (
            next(iter(post_set_leverages))
            if len(post_set_leverages) == 1
            else 0
        )
        if post_set_leverage != effective:
            raise OrderSubmitError(
                SubmitFailureClass.REJECTED,
                "entry leverage post-set position verification failed "
                f"symbol={symbol} expected={effective} actual={post_set_leverage}",
            )
        return EntryLeverageEvidence(
            venue=Venue.ASTER,
            symbol=symbol,
            requested_leverage=target,
            effective_leverage=effective,
            notional_quote=max(float(notional_quote or 0.0), 0.0),
            bracket_verified=bracket_verified,
            account_verified=True,
            source="aster_v3_post_set_position_risk",
            observed_at_ms=int(time.time() * 1000),
        )

    def _order_params(
        self,
        request: OrderRequest,
        *,
        passive: bool,
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
        if request.reduce_only:
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
        params = self._order_params(request, passive=False)
        try:
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
        params = self._order_params(request, passive=True)
        try:
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
        except TransportError:
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
            fee_quote=0.0,
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
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> Optional[OrderFillReconciliation]:
        progress = await self.query_passive_order_progress(
            symbol,
            order_id,
            client_order_id,
        )
        if progress is None or progress.cumulative_quantity <= 0:
            return None
        metadata = {
            "raw_exchange_status": progress.state.value,
            "queried_endpoints": [ASTER_V3_ORDER_PATH],
            "response_classification": "filled",
            "evidence_source": "aster_v3_order_status",
        }
        if start_time_ms is not None:
            metadata["query_start_time_ms"] = int(start_time_ms)
        if end_time_ms is not None:
            metadata["query_end_time_ms"] = int(end_time_ms)
        return OrderFillReconciliation(
            venue=Venue.ASTER,
            symbol=symbol,
            side=progress.side,
            quantity=progress.cumulative_quantity,
            average_price=progress.average_price,
            order_id=progress.order_id,
            client_order_id=progress.client_order_id or client_order_id,
            fee_quote=progress.fee_quote,
            filled_at_ms=progress.last_fill_time_ms or progress.observed_at_ms,
            metadata=metadata,
        )

    async def fetch_account_fill_reconciliations(
        self,
        symbol: str,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[OrderFillReconciliation]:
        params: dict[str, Any] = {
            "symbol": symbol,
            "limit": 1000,
        }
        if start_time_ms is not None and int(start_time_ms or 0) > 0:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None and int(end_time_ms or 0) > 0:
            params["endTime"] = int(end_time_ms)
        raw = await self._request("GET", ASTER_V3_USER_TRADES_PATH, params=params)
        _require_aster_v3_success(raw, "aster v3 user trades history failed")
        fills: list[OrderFillReconciliation] = []
        for row in _extract_rows(raw):
            qty = _safe_float(row.get("qty", row.get("quantity", "0")), default=0.0)
            price = _safe_float(row.get("price", "0"), default=0.0)
            if qty <= 0.0 or price <= 0.0:
                continue
            side_raw = str(row.get("side", "")).upper()
            if side_raw == "BUY":
                side = Side.BUY
            elif side_raw == "SELL":
                side = Side.SELL
            else:
                raise TransportError(
                    TransportErrorCategory.REQUEST_REJECTED,
                    f"aster v3 userTrades row has invalid/missing side {side_raw!r}",
                )
            commission_asset = str(row.get("commissionAsset") or "").upper()
            fee_quote = None
            if commission_asset in {"USDT", "USDC", "BUSD", ""}:
                fee_quote = abs(
                    _safe_float(row.get("commission", "0"), default=0.0)
                ) or None
            trade_id = str(row.get("id", row.get("tradeId", "")) or "")
            fills.append(
                OrderFillReconciliation(
                    venue=Venue.ASTER,
                    symbol=str(row.get("symbol") or symbol),
                    side=side,
                    quantity=qty,
                    average_price=price,
                    order_id=str(row.get("orderId", "")),
                    client_order_id=str(row.get("clientOrderId", "")) or None,
                    fee_quote=fee_quote,
                    filled_at_ms=int(row.get("time", row.get("updateTime", 0)) or 0),
                    metadata={
                        "evidence_source": "aster_v3_user_trades_history",
                        "queried_endpoints": [ASTER_V3_USER_TRADES_PATH],
                        "trade_id": trade_id,
                        "positionSide": str(row.get("positionSide", "")),
                        "buyer": row.get("buyer"),
                        "maker": row.get("maker"),
                        "realizedPnl": str(row.get("realizedPnl", "")),
                        "raw_side": side_raw,
                        "response_classification": "filled",
                    },
                )
            )
        return fills

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
