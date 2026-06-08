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
    _parse_binance_like_position,
    _parse_optional_float,
    _safe_float,
)


ASTER_V3_PRIVATE_BASE_URL = "https://fapi.asterdex.com"
ASTER_V3_DOC_URL = (
    "https://github.com/asterdex/api-docs/blob/master/"
    "V3%28Recommended%29/EN/aster-finance-futures-api-v3.md"
)

ASTER_V3_ORDER_PATH = "/fapi/v3/order"
ASTER_V3_OPEN_ORDERS_PATH = "/fapi/v3/openOrders"
ASTER_V3_POSITION_PATH = "/fapi/v3/positionRisk"
ASTER_V3_ACCOUNT_PATH = "/fapi/v3/accountWithJoinMargin"


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

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
    ) -> Any:
        query, headers, body = self.build_signed_request(method, path, params=params)
        url = ASTER_V3_PRIVATE_BASE_URL.rstrip("/") + path + query
        try:
            response = await self._client.request(
                method.upper(),
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
            category = (
                TransportErrorCategory.AUTH_FAILURE
                if response.status_code in (401, 403)
                else TransportErrorCategory.REQUEST_REJECTED
            )
            raise TransportError(
                category,
                f"aster_v3 {method.upper()} {path} rejected status={response.status_code}",
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
                f"aster_v3 {method.upper()} {path} rejected code={raw.get('code')} msg={raw.get('msg', '')}",
                status_code=response.status_code,
                body=json.dumps(raw, ensure_ascii=False),
                headers=dict(response.headers),
            )
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
        elif request.time_in_force == TimeInForce.IOC:
            params["timeInForce"] = "IOC"
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
