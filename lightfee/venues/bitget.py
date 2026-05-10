"""Bitget Mix V2 adapter (detect classic vs UTA)."""

from __future__ import annotations

import logging
from enum import Enum
from typing import Optional

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
    ) -> None:
        spec = bitget_spec()
        self._transport = VenueTransport(spec=spec, mode=mode, credential=credential)
        self._mode = mode
        self._profile: Optional[BitgetAccountProfile] = None
        self._profile_locked: bool = False

    @property
    def venue(self) -> Venue:
        return Venue.BITGET

    # ------------------------------------------------------------------
    # Profile detection
    # ------------------------------------------------------------------

    @property
    def account_profile(self) -> Optional[BitgetAccountProfile]:
        return self._profile

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
                params={"productType": "USDT-FUTURES"},
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
                "productType": "USDT-FUTURES",
            }
            raw = await self._transport._request(
                "GET", "/api/v3/position/current-position", params=params, private=True
            )

        now_ms = int(__import__("time").time() * 1000)
        return self._transport._parse_position(raw, venue_sym, now_ms)

    async def place_order(self, request: OrderRequest) -> OrderFill:
        if self._mode != "live":
            return await self._transport.place_order(request)

        try:
            profile = await self.detect_profile()
            venue_sym = self._transport._venue_symbol(request.symbol)
            now_ms = int(__import__("time").time() * 1000)

            body: dict = {
                "symbol": venue_sym,
                "side": request.side.value.upper(),
                "quantity": str(request.quantity),
                "marginCoin": "USDT",
                "orderType": "market",
            }
            if request.reduce_only:
                body["reduceOnly"] = "true"

            if profile == BitgetAccountProfile.CLASSIC:
                raw = await self._transport._request(
                    "POST", "/api/v2/mix/order/placeOrder", body=body, private=True
                )
            else:
                raw = await self._transport._request(
                    "POST", "/api/v3/order/placeOrder", body=body, private=True
                )

            return self._transport._parse_order_fill(raw, request, venue_sym, now_ms)
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

    async def normalize_quantity(self, symbol: str, quantity: float) -> float:
        return await self._transport.normalize_quantity(symbol, quantity)

    async def shutdown(self) -> None:
        await self._transport.close()
