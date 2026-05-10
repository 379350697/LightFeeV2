"""Shared venue transport: HTTP client, auth signing, error classification."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import datetime
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

import httpx

from lightfee.core.domain import (
    OrderFill,
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
                 status_code: int = 0, body: str = "") -> None:
        super().__init__(message)
        self.category = category
        self.status_code = status_code
        self.body = body


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
# Transport
# ---------------------------------------------------------------------------


class VenueTransport:
    """Shared async transport that owns HTTP lifecycle, auth, and error mapping."""

    def __init__(
        self,
        spec: VenueSpec,
        mode: str = "paper",
        credential: Optional[LiveCredential] = None,
    ) -> None:
        self._spec = spec
        self.mode = mode
        self._credential = credential
        self._client: Optional[httpx.AsyncClient] = None
        self._hl_meta_cache: dict[str, int] = {}

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
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(10.0),
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
        if self._spec.requires_wallet_key and not credential.wallet_private_key:
            raise ValueError(
                f"live mode requires wallet_private_key for {self._spec.venue_id.value}"
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
    ) -> dict[str, str]:
        spec = self._spec
        cred = self._credential

        if cred is None:
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
    ) -> tuple[str, dict[str, str], Optional[str]]:
        spec = self._spec
        query_string = ""
        req_body: Optional[str] = None

        # Binance / Aster always require timestamp + signature in the query
        # string, even for POST requests with a JSON body.
        if spec.signature_param:
            qp: dict[str, Any] = dict(params) if params else {}
            ts = str(int(time.time() * 1000))
            if spec.timestamp_param:
                qp[spec.timestamp_param] = ts
            if self._credential:
                encoded = "&".join(f"{k}={v}" for k, v in sorted(qp.items()))
                sig = _sign_payload(spec.auth_scheme, self._credential.api_secret, encoded)
                qp[spec.signature_param] = sig
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

        headers = self._build_auth_headers(method, path, query_string, req_body or "")
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
            self._spec.private_base_url
            if private or method.upper() == "POST"
            else self._spec.public_base_url
        )
        qs, headers, req_body = self._build_signed_request(method, path, params, body)
        url = base_url + path + qs

        try:
            if method.upper() == "GET":
                resp = await client.get(url, headers=headers)
            elif method.upper() == "POST":
                resp = await client.post(url, headers=headers, content=req_body)
            elif method.upper() == "DELETE":
                resp = await client.delete(url, headers=headers)
            else:
                raise ValueError(f"unsupported HTTP method: {method}")

            if resp.status_code >= 400:
                cat = classify_transport_error(resp.status_code, resp.text)
                if cat:
                    raise TransportError(
                        cat, f"HTTP {resp.status_code}: {resp.text[:200]}",
                        status_code=resp.status_code, body=resp.text,
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
        spec = self._spec
        data = raw

        if isinstance(raw, dict):
            if "data" in raw and isinstance(raw["data"], dict):
                data = raw["data"]
            elif "data" in raw and isinstance(raw["data"], list) and raw["data"]:
                data = raw["data"][0]
            elif "result" in raw and isinstance(raw["result"], dict) and raw["result"].get("list"):
                # Bybit V5: {"result": {"list": [...]}}
                data = raw["result"]["list"][0]
            elif "result" in raw and isinstance(raw["result"], list) and raw["result"]:
                data = raw["result"][0]

        if spec.venue_id == Venue.HYPERLIQUID:
            # Parse from clearinghouse state
            positions = data.get("assetPositions", [])
            for p in positions:
                pos_sym = p.get("position", {}).get("coin", "")
                if pos_sym == symbol or not symbol:
                    return PositionSnapshot(
                        venue=spec.venue_id,
                        symbol=pos_sym or symbol,
                        side=Side.BUY if float(p.get("position", {}).get("szi", 0)) > 0 else Side.SELL,
                        quantity=abs(float(p.get("position", {}).get("szi", 0))),
                        entry_price=float(p.get("position", {}).get("entryPx", 0)),
                        observed_at_ms=now_ms,
                    )
            return PositionSnapshot(
                venue=spec.venue_id,
                symbol=symbol,
                side=Side.BUY,
                quantity=0.0,
                entry_price=0.0,
                observed_at_ms=now_ms,
            )

        # Generic position parsing — wide fallback chains for per-venue field names
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

        # Determine side: explicit indicators (case-insensitive), then sign of quantity.
        # Mirrors Rust V1 behavior where position side strings vary per venue
        # (e.g. OKX uses "short"/"long", Binance uses "SHORT"/"LONG", etc.).
        pos_side_upper = pos_side_str.upper()
        short_indicators = ("SHORT", "SELL", "SHORT_SIDE")
        long_indicators = ("LONG", "BUY")
        if any(ind in pos_side_upper for ind in short_indicators):
            side = Side.SELL
        elif any(ind in pos_side_upper for ind in long_indicators):
            side = Side.BUY
        elif qty_raw:
            try:
                if float(qty_raw) < 0:
                    side = Side.SELL
                else:
                    side = Side.BUY
            except ValueError:
                side = Side.BUY
        else:
            side = Side.BUY

        qty = abs(float(qty_raw)) if qty_raw else 0.0

        entry_price = float(data.get(
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
                body["category"] = "linear"
                body["orderType"] = "Market"
                if request.reduce_only:
                    body["reduceOnly"] = "true"
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
