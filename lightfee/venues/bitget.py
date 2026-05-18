"""Bitget Mix V2 adapter (detect classic vs UTA)."""

from __future__ import annotations

import logging
from enum import Enum
from typing import Any, Optional

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import (
    OrderFill,
    OrderRequest,
    PositionSnapshot,
    Venue,
    VenueMarketSnapshot,
)
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.venues.specs import bitget_spec
from lightfee.venues.transport import LiveCredential, TransportError, TransportErrorCategory, VenueTransport

logger = logging.getLogger("lightfee.venues.bitget")


class BitgetAccountProfile(Enum):
    CLASSIC = "classic"
    UTA = "uta"


def _is_classic_mode_error(status_code: int, body: dict) -> bool:
    """Detect Bitget error responses that indicate a classic account."""
    if not isinstance(body, dict):
        return False
    code = str(body.get("code", ""))
    msg = str(body.get("msg", "")).lower()
    # Bitget returns specific codes and messages for classic vs UTA mismatch
    if code in ("40034", "40035", "40036", "40102", "43011"):
        return True
    if "classic account" in msg or "uta" in msg or "unified account" in msg:
        return True
    if status_code == 400 and ("unsupported" in msg or "not supported" in msg):
        return True
    return False


def _payload_indicates_classic(payload: dict) -> bool:
    """Check whether a successful payload reports classic account mode."""
    if not isinstance(payload, dict):
        return False
    data = payload.get("data", {})
    if isinstance(data, dict):
        if data.get("accountType") == "classic":
            return True
        if data.get("accountMode") == "classic":
            return True
    return False


def _extract_position_hedge_mode(payload: dict) -> Optional[bool]:
    """Extract Bitget one-way vs hedge position mode when the payload exposes it."""
    data: Any = payload.get("data", payload) if isinstance(payload, dict) else {}
    row: Any = data
    if isinstance(data, list):
        row = data[0] if data else {}
    if not isinstance(row, dict):
        return None
    if "posMode" not in row and "holdMode" not in row:
        return None
    raw_mode = str(row.get("posMode", row.get("holdMode", "")))
    return "hedge" in raw_mode.strip().lower()


def _parse_position_hedge_mode(payload: dict) -> bool:
    """Parse Bitget one-way vs hedge position mode.

    V1 defaults missing/unknown mode to one-way; only explicit hedge-like
    values enable tradeSide/posSide hedge semantics.
    """
    return _extract_position_hedge_mode(payload) or False


def _parse_bitget_risk_from_rowlike(raw: dict, now_ms: int):
    """Parse an AccountRiskSnapshot from a Bitget account assets response.

    V1: bitget_account_asset_row + bitget_account_risk_snapshot_from_account_row
    Uses multi-field-name fallback chains per Rust V1 (bitget.rs:5740-5777).
    Returns None when maintenance_margin is missing or <= 0.
    """
    from lightfee.engine.risk_actions import AccountRiskSnapshot as ARS

    data = raw.get("data", raw)
    # Find USDT margin row
    row = None
    if isinstance(data, dict):
        row = data
    elif isinstance(data, list):
        row = next(
            (r for r in data if isinstance(r, dict)
             and str(r.get("marginCoin", "")).upper() == "USDT"),
            data[0] if data else None,
        )
    if not row or not isinstance(row, dict):
        return None

    # Maintenance margin: multi-key fallback
    maint = None
    for key in ("maintenanceMargin", "maintMargin", "maintainMargin", "maintenance_margin"):
        if key in row:
            val = row[key]
            if val is not None and str(val).strip():
                maint = float(val)
                break
    if maint is None or maint <= 0.0:
        return None

    # Equity: multi-key fallback
    equity = None
    for key in ("usdtEquity", "equity", "accountEquity"):
        if key in row:
            equity = float(row[key])
            break
    if equity is None:
        return None

    snapshot = ARS(
        venue=Venue.BITGET,
        equity_quote=equity,
        maintenance_margin_quote=maint,
        health_ratio=equity / maint,
        observed_at_ms=now_ms,
        source="bitget_account_risk",
    )
    # Available balance: multi-key fallback
    avail = None
    for key in ("available", "availableBalance", "crossedMaxAvailable"):
        if key in row:
            avail = float(row[key])
            break
    snapshot.available_balance_quote = avail
    return snapshot


class BitgetAdapter(VenueAdapter):
    """Bitget Mix V2 adapter with classic-vs-UTA detection.

    On first use, the adapter probes the UTA endpoints. If the exchange
    reports a classic account, it falls back to the classic (Mix V2)
    endpoints and caches the profile so subsequent requests use the
    correct path automatically.
    """

    def __init__(
        self,
        mode: str = "paper",
        credential: Optional[LiveCredential] = None,
        exchange_http_timeout_ms: int = 10000,
        rate_limiter: Any = None,
    ) -> None:
        spec = bitget_spec()
        self._transport = VenueTransport(spec=spec, mode=mode, credential=credential,
                                         exchange_http_timeout_ms=exchange_http_timeout_ms,
                                         rate_limiter=rate_limiter)
        self._mode = mode
        self._profile: Optional[BitgetAccountProfile] = None
        self._profile_locked: bool = False
        self._position_hedge_mode: Optional[bool] = None

    @property
    def venue(self) -> Venue:
        return Venue.BITGET

    @property
    def supports_risk_health(self) -> bool:
        # V1 parity: Bitget risk_health is UNSUPPORTED — the account endpoint
        # does not provide reliable margin/equity data for risk evaluation.
        return False

    # ------------------------------------------------------------------
    # Profile detection
    # ------------------------------------------------------------------

    @property
    def account_profile(self) -> Optional[BitgetAccountProfile]:
        return self._profile

    async def detect_position_hedge_mode(self, symbol: str) -> bool:
        """Detect Bitget position mode for classic futures accounts.

        V1: GET /api/v2/mix/account/account and parse posMode/holdMode.
        Classic one-way close orders use reduceOnly; hedge close orders use
        tradeSide/posSide. Sending the wrong family is rejected by Bitget.
        """
        if self._position_hedge_mode is not None:
            return self._position_hedge_mode
        if self._mode != "live":
            self._position_hedge_mode = True
            return self._position_hedge_mode

        venue_sym = self._transport._venue_symbol(symbol)
        raw = await self._transport._request(
            "GET",
            "/api/v2/mix/account/account",
            params={
                "symbol": venue_sym,
                "productType": "USDT-FUTURES",
                "marginCoin": "USDT",
            },
            private=True,
        )
        self._position_hedge_mode = _parse_position_hedge_mode(raw)
        return self._position_hedge_mode

    async def detect_profile(self) -> BitgetAccountProfile:
        """Detect account profile (UTA vs Classic) via a lightweight probe.

        Caches the result so subsequent calls are free.

        Only falls back to CLASSIC when the exchange explicitly indicates a
        classic/UTA mismatch. Auth failures (401/403), rate limits (429),
        network errors, and other transport failures propagate immediately.
        """
        if self._profile is not None:
            return self._profile

        # Paper mode defaults to UTA
        if self._mode != "live":
            self._profile = BitgetAccountProfile.UTA
            return self._profile

        # Probe UTA endpoint
        try:
            raw = await self._transport._request(
                "GET",
                "/api/v3/position/current-position",
                params={"category": "USDT-FUTURES"},
                private=True,
            )
            if _payload_indicates_classic(raw):
                self._profile = BitgetAccountProfile.CLASSIC
            else:
                self._profile = BitgetAccountProfile.UTA
        except TransportError as e:
            # Only fall back to CLASSIC on explicit classic-mode errors
            status_code = getattr(e, "status_code", 0)
            body_str = getattr(e, "body", "")
            body_dict: dict = {}
            if body_str:
                try:
                    import json as _json
                    body_dict = _json.loads(body_str)
                except Exception:
                    body_dict = {}
            if _is_classic_mode_error(status_code, body_dict):
                self._profile = BitgetAccountProfile.CLASSIC
            elif e.category in (
                TransportErrorCategory.AUTH_FAILURE,
                TransportErrorCategory.AUTHORIZATION_FAILURE,
            ):
                raise TransportError(
                    TransportErrorCategory.AUTH_FAILURE,
                    f"Bitget profile detection failed: auth error (HTTP {status_code})",
                    status_code=status_code,
                    body=body_str,
                ) from e
            elif e.category == TransportErrorCategory.TRANSPORT_FAILURE:
                raise TransportError(
                    TransportErrorCategory.TRANSPORT_FAILURE,
                    f"Bitget profile detection failed: transport error (HTTP {status_code})",
                    status_code=status_code,
                    body=body_str,
                ) from e
            else:
                raise

        return self._profile

    # ------------------------------------------------------------------
    # Adapter methods with profile-aware routing
    # ------------------------------------------------------------------

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        if self._mode != "live":
            return await self._transport.fetch_position(symbol)

        profile = await self.detect_profile()
        venue_sym = self._transport._venue_symbol(symbol)

        if profile == BitgetAccountProfile.CLASSIC:
            params = {
                "symbol": venue_sym,
                "productType": "USDT-FUTURES",
                "marginCoin": "USDT",
            }
            raw = await self._transport._request(
                "GET", "/api/v2/mix/position/single-position", params=params, private=True
            )
        else:
            params = {
                "symbol": venue_sym,
                "category": "USDT-FUTURES",
            }
            raw = await self._transport._request(
                "GET", "/api/v3/position/current-position", params=params, private=True
            )

        now_ms = int(__import__("time").time() * 1000)
        return self._transport._parse_position(raw, venue_sym, now_ms)

    async def fetch_all_positions(self) -> list[PositionSnapshot]:
        if self._mode != "live":
            return await self._transport.fetch_all_positions()

        profile = await self.detect_profile()
        if profile == BitgetAccountProfile.CLASSIC:
            raw = await self._transport._request(
                "GET",
                "/api/v2/mix/position/all-position",
                params={"productType": "USDT-FUTURES", "marginCoin": "USDT"},
                private=True,
            )
        else:
            raw = await self._transport._request(
                "GET",
                "/api/v3/position/current-position",
                params={"category": "USDT-FUTURES"},
                private=True,
            )

        now_ms = int(__import__("time").time() * 1000)
        return self._transport._parse_all_positions(raw, now_ms)

    async def place_order(self, request: OrderRequest) -> OrderFill:
        if self._mode != "live":
            return await self._transport.place_order(request)

        try:
            from lightfee.venues.transport import (
                _build_bitget_order_request,
                _require_bitget_success,
            )

            profile = await self.detect_profile()
            venue_sym = self._transport._venue_symbol(request.symbol)
            now_ms = int(__import__("time").time() * 1000)
            hedge_mode = self._transport._hedge_mode
            if profile == BitgetAccountProfile.CLASSIC:
                hedge_mode = await self.detect_position_hedge_mode(request.symbol)

            req_path, body = _build_bitget_order_request(
                request, venue_sym,
                passive=False,
                profile=profile.value,
                hedge_mode=hedge_mode,
            )
            raw = await self._transport._request("POST", req_path, body=body, private=True)

            _require_bitget_success(raw, "bitget order failed")
            return self._transport._parse_order_fill(raw, request, venue_sym, now_ms)
        except TransportError as e:
            if e.category == TransportErrorCategory.REQUEST_REJECTED:
                raise OrderSubmitError(SubmitFailureClass.REJECTED, str(e)) from e
            raise OrderSubmitError(SubmitFailureClass.UNCERTAIN, str(e)) from e
        except OrderSubmitError:
            raise
        except Exception as e:
            raise OrderSubmitError(SubmitFailureClass.UNCERTAIN, str(e)) from e

    async def submit_passive_order(self, request: OrderRequest):
        """Submit a GTC post-only maker order via profile-aware routing."""
        from lightfee.core.domain import PassiveOrderAck

        if self._mode != "live":
            return await self._transport.submit_passive_order(request)

        try:
            from lightfee.venues.transport import (
                _build_bitget_order_request,
                _require_bitget_success,
            )

            profile = await self.detect_profile()
            venue_sym = self._transport._venue_symbol(request.symbol)
            now_ms = int(__import__("time").time() * 1000)
            hedge_mode = self._transport._hedge_mode
            if profile == BitgetAccountProfile.CLASSIC:
                hedge_mode = await self.detect_position_hedge_mode(request.symbol)

            req_path, body = _build_bitget_order_request(
                request, venue_sym,
                passive=True,
                profile=profile.value,
                hedge_mode=hedge_mode,
            )
            raw = await self._transport._request("POST", req_path, body=body, private=True)

            _require_bitget_success(raw, "bitget passive order failed")
            return self._transport._parse_passive_order_ack(raw, request, venue_sym, now_ms)
        except TransportError as e:
            if e.category == TransportErrorCategory.REQUEST_REJECTED:
                raise OrderSubmitError(SubmitFailureClass.REJECTED, str(e)) from e
            raise OrderSubmitError(SubmitFailureClass.UNCERTAIN, str(e)) from e
        except OrderSubmitError:
            raise
        except Exception as e:
            raise OrderSubmitError(SubmitFailureClass.UNCERTAIN, str(e)) from e

    async def fetch_market_snapshot(self, symbols: list[str]) -> VenueMarketSnapshot:
        return await self._transport.fetch_market_snapshot(symbols)

    # ------------------------------------------------------------------
    # Risk snapshot with profile-aware classic fallback (V1: bitget.rs:836-866, 2896-2902)
    # ------------------------------------------------------------------

    _BITGET_CLASSIC_ACCOUNT_PATH = "/api/v2/mix/account/accounts"
    _BITGET_CLASSIC_PRODUCT_TYPE = "USDT-FUTURES"

    async def fetch_account_risk_snapshot(self):
        """Fetch account risk snapshot with profile-aware routing.

        Rust V1 flow:
        1. If profile is already CLASSIC → go directly to classic endpoint.
        2. Try UTA endpoint /api/v3/account/assets.
        3. If classic/UTA mismatch error → cache CLASSIC, retry classic endpoint.
        4. If payload indicates classic mode → cache CLASSIC, retry classic endpoint.
        5. Auth/rate-limit/network errors propagate — never fall back.
        """
        from lightfee.engine.risk_actions import AccountRiskSnapshot as ARS
        import time as _time

        if self._mode != "live":
            return None

        now_ms = int(_time.time() * 1000)

        # If profile is already CLASSIC, go directly to classic endpoint
        if self._profile == BitgetAccountProfile.CLASSIC:
            return await self._fetch_classic_risk_snapshot(now_ms)

        # Try UTA endpoint
        try:
            raw = await self._transport._request(
                "GET", self._transport._spec.account_risk_path, private=True
            )
            # Check if successful payload indicates classic mode
            if _payload_indicates_classic(raw):
                self._profile = BitgetAccountProfile.CLASSIC
                return await self._fetch_classic_risk_snapshot(now_ms)

            return _parse_bitget_risk_from_rowlike(raw, now_ms)

        except TransportError as e:
            status_code = getattr(e, "status_code", 0)
            body_str = getattr(e, "body", "")
            body_dict: dict = {}
            if body_str:
                try:
                    import json as _json
                    body_dict = _json.loads(body_str)
                except Exception:
                    body_dict = {}

            # Only classic/UTA mismatch triggers fallback
            if _is_classic_mode_error(status_code, body_dict):
                self._profile = BitgetAccountProfile.CLASSIC
                return await self._fetch_classic_risk_snapshot(now_ms)

            # Auth/rate-limit/network → propagate, never fall back
            if e.category in (
                TransportErrorCategory.AUTH_FAILURE,
                TransportErrorCategory.AUTHORIZATION_FAILURE,
                TransportErrorCategory.TRANSPORT_FAILURE,
            ):
                raise

            raise

    async def _fetch_classic_risk_snapshot(self, now_ms: int):
        """Fetch risk snapshot from Bitget classic account endpoint.

        V1: GET /api/v2/mix/account/accounts?productType=USDT-FUTURES
        """
        from lightfee.engine.risk_actions import AccountRiskSnapshot as ARS

        raw = await self._transport._request(
            "GET",
            self._BITGET_CLASSIC_ACCOUNT_PATH,
            params={"productType": self._BITGET_CLASSIC_PRODUCT_TYPE},
            private=True,
        )
        return _parse_bitget_risk_from_rowlike(raw, now_ms)

    # ------------------------------------------------------------------
    # Local-L2 snapshot with metadata guard (V1: bitget.rs:5464-5512)
    # ------------------------------------------------------------------

    async def fetch_l2_snapshot(
        self, symbol: str, depth: int = 50,
    ) -> "LocalL2Update":
        """Fetch Bitget order book depth with metadata guard.

        V1: bitget_fetch_execution_liquidity_snapshot() requires symbol metadata
        before making any HTTP call. If metadata is missing for the symbol,
        raises TransportError(REQUEST_REJECTED) without sending an HTTP request
        (avoids exchange 400172 errors from unsupported symbols).
        """
        from lightfee.venues.transport import TransportError, TransportErrorCategory

        venue_sym = self._transport._venue_symbol(symbol)

        # Ensure symbol catalog/metadata is loaded
        if not self._transport._symbol_metadata:
            await self._load_symbol_metadata()

        # Guard: block unsupported symbols before any HTTP call
        if venue_sym not in self._transport._symbol_metadata:
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                f"bitget execution liquidity metadata missing for {venue_sym}",
            )

        return await self._transport.fetch_l2_snapshot(symbol=symbol, depth=depth)

    async def _load_symbol_metadata(self) -> None:
        """Load Bitget contract metadata from /api/v2/mix/market/contracts.

        V1: refresh_symbol_catalog() — fetches contract catalog, filters
        tradeable contracts, and populates metadata + supported_symbols.
        """
        raw = await self._transport._request(
            "GET",
            "/api/v2/mix/market/contracts",
            params={"productType": "USDT-FUTURES"},
            private=False,
        )
        data = raw.get("data", raw)
        items = data if isinstance(data, list) else [data]
        metadata: dict[str, dict[str, Any]] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            sym = item.get("symbol", "")
            if not sym:
                continue
            metadata[sym] = {
                "sizeMultiplier": str(item.get("sizeMultiplier", "0.001")),
                "minTradeNum": str(item.get("minTradeNum", "1")),
                "pricePlace": str(item.get("pricePlace", "2")),
                "volumePlace": str(item.get("volumePlace", "0")),
                "symbolName": str(item.get("symbolName", sym)),
            }
        self._transport.set_symbol_metadata(metadata)

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        return await self._transport.normalize_quantity(symbol, quantity)

    async def fetch_order_fill_reconciliation(
        self,
        symbol: str,
        order_id: str,
        client_order_id: Optional[str] = None,
    ) -> Optional["OrderFillReconciliation"]:
        """V1: Bitget fetch_order_fill_reconciliation via /api/v3/trade/order-info.

        Uses clientOid for client_order_id lookup (V1: bitget.rs:2912-2948).
        Falls through to transport.fetch_order_status then converts to
        OrderFillReconciliation.
        """
        from lightfee.core.domain import OrderFillReconciliation

        status = await self._transport.fetch_order_status(
            symbol, order_id=order_id, client_order_id=client_order_id or "",
        )
        if status is not None:
            return status
        return None

    async def shutdown(self) -> None:
        await self._transport.close()
