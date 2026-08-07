"""Bitget Mix V2 adapter (detect classic vs UTA)."""

from __future__ import annotations

import logging
import json
import time
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
from lightfee.engine.exchange_truth import request_venue_operation
from lightfee.venues.entry_tradability import (
    entry_tradability_blocked,
    entry_tradability_unavailable,
)
from lightfee.venues.specs import (
    BitgetContractFamily,
    VenueOperation,
    bitget_spec,
    get_operation_contract,
)
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


_BITGET_ORDER_DIAGNOSTIC_KEYS = (
    "category",
    "symbol",
    "productType",
    "marginMode",
    "marginCoin",
    "qty",
    "size",
    "side",
    "orderType",
    "force",
    "timeInForce",
    "price",
    "tradeSide",
    "posSide",
    "reduceOnly",
    "clientOid",
)


def _bitget_order_body_diagnostic(body: dict[str, Any]) -> dict[str, Any]:
    return {
        key: body[key]
        for key in _BITGET_ORDER_DIAGNOSTIC_KEYS
        if key in body
    }


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


def _profile_from_contract_family(family: BitgetContractFamily) -> BitgetAccountProfile:
    if family == BitgetContractFamily.CLASSIC_MIX_V2:
        return BitgetAccountProfile.CLASSIC
    return BitgetAccountProfile.UTA


def _contract_family_from_profile(profile: BitgetAccountProfile) -> BitgetContractFamily:
    if profile == BitgetAccountProfile.CLASSIC:
        return BitgetContractFamily.CLASSIC_MIX_V2
    return BitgetContractFamily.UTA_V3


def _coerce_bitget_contract_family(value: Any) -> BitgetContractFamily | None:
    if value is None:
        return None
    if isinstance(value, BitgetContractFamily):
        return value
    text = str(value).strip().lower()
    aliases = {
        "classic": BitgetContractFamily.CLASSIC_MIX_V2,
        "classic_mix": BitgetContractFamily.CLASSIC_MIX_V2,
        "classic_mix_v2": BitgetContractFamily.CLASSIC_MIX_V2,
        "mix_v2": BitgetContractFamily.CLASSIC_MIX_V2,
        "v2": BitgetContractFamily.CLASSIC_MIX_V2,
        "uta": BitgetContractFamily.UTA_V3,
        "uta_v3": BitgetContractFamily.UTA_V3,
        "v3": BitgetContractFamily.UTA_V3,
    }
    if text in aliases:
        return aliases[text]
    return BitgetContractFamily(text)


def _bitget_contract_params(contract) -> dict[str, str]:
    params: dict[str, str] = {}
    for item in contract.required_params:
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        params[key] = value
    return params


def _transport_error_body_dict(error: TransportError) -> dict[str, Any]:
    body = getattr(error, "body", "")
    if not body:
        return {}
    try:
        parsed = json.loads(body)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


class BitgetContractFamilyResolver:
    """Resolve and cache the Bitget private-truth contract family for this runtime."""

    def __init__(
        self,
        transport: VenueTransport,
        *,
        configured_family: BitgetContractFamily | str | None = None,
    ) -> None:
        self._transport = transport
        self._configured_family = _coerce_bitget_contract_family(configured_family)
        self._resolved_family: BitgetContractFamily | None = None

    @property
    def resolved_family(self) -> BitgetContractFamily | None:
        return self._resolved_family

    def lock(self, family: BitgetContractFamily | str) -> BitgetContractFamily:
        self._resolved_family = _coerce_bitget_contract_family(family)
        if self._resolved_family is None:
            raise ValueError("Bitget contract family cannot be None")
        return self._resolved_family

    async def resolve(self) -> BitgetContractFamily:
        if self._resolved_family is not None:
            return self._resolved_family

        if self._transport.mode != "live":
            return self.lock(BitgetContractFamily.UTA_V3)

        target_family = self._configured_family
        if target_family == BitgetContractFamily.CLASSIC_MIX_V2:
            self._resolved_family = await self._resolve_explicit_classic()
            return self._resolved_family
        if target_family == BitgetContractFamily.UTA_V3:
            await self._probe_uta()
            return self.lock(BitgetContractFamily.UTA_V3)

        try:
            raw = await self._probe_uta()
        except TransportError as error:
            if self._is_classic_mismatch(error):
                await self._probe_classic()
                return self.lock(BitgetContractFamily.CLASSIC_MIX_V2)
            raise
        if _payload_indicates_classic(raw):
            await self._probe_classic()
            return self.lock(BitgetContractFamily.CLASSIC_MIX_V2)
        return self.lock(BitgetContractFamily.UTA_V3)

    async def _resolve_explicit_classic(self) -> BitgetContractFamily:
        try:
            raw = await self._probe_uta()
        except TransportError as error:
            if self._is_classic_mismatch(error):
                await self._probe_classic()
                return self.lock(BitgetContractFamily.CLASSIC_MIX_V2)
            raise
        if _payload_indicates_classic(raw):
            await self._probe_classic()
            return self.lock(BitgetContractFamily.CLASSIC_MIX_V2)
        raise TransportError(
            TransportErrorCategory.REQUEST_REJECTED,
            "Bitget configured classic family validation failed: UTA probe succeeded",
            status_code=400,
            body='{"code":"LFV2_BITGET_FAMILY_MISMATCH","msg":"configured classic but account resolved UTA"}',
        )

    async def _probe_uta(self) -> dict[str, Any]:
        contract = get_operation_contract(
            self._transport._spec,
            VenueOperation.POSITION,
            resolved_account_family=BitgetContractFamily.UTA_V3,
        )
        return await self._transport._request(
            contract.method,
            contract.path,
            params=_bitget_contract_params(contract),
            private=contract.private,
        )

    async def _probe_classic(self) -> dict[str, Any]:
        contract = get_operation_contract(
            self._transport._spec,
            VenueOperation.ALL_POSITIONS,
            resolved_account_family=BitgetContractFamily.CLASSIC_MIX_V2,
        )
        return await self._transport._request(
            contract.method,
            contract.path,
            params=_bitget_contract_params(contract),
            private=contract.private,
        )

    @staticmethod
    def _is_classic_mismatch(error: TransportError) -> bool:
        status_code = getattr(error, "status_code", 0)
        return _is_classic_mode_error(status_code, _transport_error_body_dict(error))


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
        account_family: BitgetContractFamily | str | None = None,
    ) -> None:
        spec = bitget_spec()
        self._transport = VenueTransport(spec=spec, mode=mode, credential=credential,
                                         exchange_http_timeout_ms=exchange_http_timeout_ms,
                                         rate_limiter=rate_limiter)
        self._mode = mode
        self._profile: Optional[BitgetAccountProfile] = None
        self._contract_family_resolver = BitgetContractFamilyResolver(
            self._transport,
            configured_family=account_family,
        )
        self._transport._bitget_resolve_contract_family = self.resolve_contract_family
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

    @property
    def supports_private_health(self) -> bool:
        return self._transport.mode == "live"

    def supported_symbols(self) -> list[str]:
        """Return the loaded Bitget contract catalog symbols, if available."""
        metadata = getattr(self._transport, "_symbol_metadata", {}) or {}
        return sorted(str(symbol) for symbol in metadata.keys())

    async def ensure_supported_symbols_loaded(self) -> None:
        """Populate Bitget contract metadata when startup recovery needs it."""
        if not self._transport._symbol_metadata:
            await self._load_symbol_metadata()

    async def precheck_entry_tradability(self, symbol: str) -> dict[str, Any]:
        """Require Bitget to currently permit a USDT perpetual opening order."""
        venue_symbol = self._transport._venue_symbol(symbol)
        raw = await self._transport._request(
            "GET",
            "/api/v2/mix/market/contracts",
            params={"productType": "USDT-FUTURES", "symbol": venue_symbol},
            private=False,
        )
        if (
            not isinstance(raw, dict)
            or str(raw.get("code", "00000")) != "00000"
            or not isinstance(raw.get("data"), list)
        ):
            raise entry_tradability_unavailable(
                Venue.BITGET.value,
                venue_symbol,
                "contracts_response_missing_or_unsuccessful",
            )
        row = next(
            (
                item
                for item in raw["data"]
                if isinstance(item, dict)
                and str(item.get("symbol", "")).upper() == venue_symbol.upper()
            ),
            None,
        )
        if row is None:
            raise entry_tradability_blocked(
                Venue.BITGET.value,
                venue_symbol,
                symbol_status="MISSING",
                symbol_type="MISSING",
            )
        symbol_status = str(row.get("symbolStatus", "")).lower()
        symbol_type = str(row.get("symbolType", "")).lower()
        limit_open_time = str(row.get("limitOpenTime", "") or "")
        if limit_open_time not in ("", "-1"):
            try:
                limit_open_at_ms = int(limit_open_time)
            except ValueError:
                raise entry_tradability_unavailable(
                    Venue.BITGET.value,
                    venue_symbol,
                    "limitOpenTime_not_an_integer",
                )
            if limit_open_at_ms > 0 and limit_open_at_ms <= int(time.time() * 1000):
                raise entry_tradability_blocked(
                    Venue.BITGET.value,
                    venue_symbol,
                    symbol_status=symbol_status or "MISSING",
                    symbol_type=symbol_type or "MISSING",
                    limit_open_time=limit_open_time,
                )
        if symbol_status != "normal" or symbol_type != "perpetual":
            raise entry_tradability_blocked(
                Venue.BITGET.value,
                venue_symbol,
                symbol_status=symbol_status or "MISSING",
                symbol_type=symbol_type or "MISSING",
                limit_open_time=limit_open_time or "MISSING",
            )
        return {
            "venue": Venue.BITGET.value,
            "symbol": venue_symbol,
            "status": "ok",
            "symbol_status": symbol_status,
            "symbol_type": symbol_type,
            "limit_open_time": limit_open_time,
        }

    # ------------------------------------------------------------------
    # Profile detection
    # ------------------------------------------------------------------

    @property
    def account_profile(self) -> Optional[BitgetAccountProfile]:
        return self._profile

    @property
    def resolved_contract_family(self) -> BitgetContractFamily | None:
        return self._contract_family_resolver.resolved_family

    async def resolve_contract_family(self) -> BitgetContractFamily:
        if self._profile is not None and self._contract_family_resolver.resolved_family is None:
            return self._contract_family_resolver.lock(_contract_family_from_profile(self._profile))
        family = await self._contract_family_resolver.resolve()
        self._profile = _profile_from_contract_family(family)
        return family

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
        family = await self.resolve_contract_family()
        self._profile = _profile_from_contract_family(family)
        return self._profile

    # ------------------------------------------------------------------
    # Adapter methods with profile-aware routing
    # ------------------------------------------------------------------

    async def fetch_position(self, symbol: str) -> PositionSnapshot:
        if self._mode != "live":
            return await self._transport.fetch_position(symbol)

        family = await self.resolve_contract_family()
        venue_sym = self._transport._venue_symbol(symbol)
        raw, _request = await request_venue_operation(
            self._transport,
            Venue.BITGET,
            VenueOperation.POSITION,
            symbol=symbol,
            resolved_account_family=family,
        )

        now_ms = int(__import__("time").time() * 1000)
        return self._transport._parse_position(raw, venue_sym, now_ms)

    async def fetch_all_positions(self) -> list[PositionSnapshot]:
        if self._mode != "live":
            return await self._transport.fetch_all_positions()

        family = await self.resolve_contract_family()
        raw, _request = await request_venue_operation(
            self._transport,
            Venue.BITGET,
            VenueOperation.ALL_POSITIONS,
            resolved_account_family=family,
        )

        now_ms = int(__import__("time").time() * 1000)
        return self._transport._parse_all_positions(raw, now_ms)

    async def place_order(self, request: OrderRequest) -> OrderFill:
        if self._mode != "live":
            return await self._transport.place_order(request)

        attempt_payload: dict[str, Any] | None = None
        try:
            from lightfee.venues.transport import (
                _build_bitget_order_request,
                _require_bitget_success,
            )

            family = await self.resolve_contract_family()
            profile = _profile_from_contract_family(family)
            venue_sym = self._transport._venue_symbol(request.symbol)
            now_ms = int(__import__("time").time() * 1000)
            hedge_mode = self._transport._hedge_mode
            if profile == BitgetAccountProfile.CLASSIC:
                hedge_mode = await self.detect_position_hedge_mode(request.symbol)

            _builder_path, body = _build_bitget_order_request(
                request, venue_sym,
                passive=False,
                profile=profile.value,
                hedge_mode=hedge_mode,
            )
            contract = get_operation_contract(
                self._transport._spec,
                VenueOperation.CREATE_ORDER,
                resolved_account_family=family,
            )
            req_path = contract.path
            attempt_payload = {
                "venue": Venue.BITGET.value,
                "symbol": venue_sym,
                "side": request.side.value,
                "reduce_only": request.reduce_only,
                "raw_qty": request.quantity,
                "client_order_id": request.client_order_id or "",
                "endpoint": req_path,
                "account_profile": profile.value,
                "hedge_mode": hedge_mode,
                "body_sanitized": _bitget_order_body_diagnostic(body),
                "response_classification": "attempt",
            }
            self._transport._record_order_diagnostic(
                "order.submit_attempt", attempt_payload
            )
            raw = await self._transport._request("POST", req_path, body=body, private=True)

            _require_bitget_success(raw, "bitget order failed")
            fill = self._transport._parse_order_fill(raw, request, venue_sym, now_ms)
            result_payload = dict(attempt_payload)
            result_payload["response_code"] = 0
            result_payload["response_msg"] = "ok"
            result_payload["order_id"] = fill.order_id
            result_payload["ack_client_order_id"] = (
                fill.client_order_id or request.client_order_id or ""
            )
            result_payload["response_classification"] = "filled"
            self._transport._record_order_diagnostic(
                "order.submit_result", result_payload
            )
            return fill
        except TransportError as e:
            if attempt_payload is not None:
                result_payload = dict(attempt_payload)
                result_payload["response_code"] = e.status_code
                result_payload["response_msg"] = (e.body or str(e))[:500]
                result_payload["response_classification"] = (
                    "rejected"
                    if e.category == TransportErrorCategory.REQUEST_REJECTED
                    else "uncertain"
                )
                self._transport._record_order_diagnostic(
                    "order.submit_result", result_payload
                )
            if e.category == TransportErrorCategory.REQUEST_REJECTED:
                raise OrderSubmitError(SubmitFailureClass.REJECTED, str(e)) from e
            raise OrderSubmitError(SubmitFailureClass.UNCERTAIN, str(e)) from e
        except OrderSubmitError as e:
            if attempt_payload is not None:
                result_payload = dict(attempt_payload)
                result_payload["response_code"] = 0
                result_payload["response_msg"] = str(e)[:500]
                result_payload["response_classification"] = e.class_.value
                self._transport._record_order_diagnostic(
                    "order.submit_result", result_payload
                )
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

            family = await self.resolve_contract_family()
            profile = _profile_from_contract_family(family)
            venue_sym = self._transport._venue_symbol(request.symbol)
            now_ms = int(__import__("time").time() * 1000)
            hedge_mode = self._transport._hedge_mode
            if profile == BitgetAccountProfile.CLASSIC:
                hedge_mode = await self.detect_position_hedge_mode(request.symbol)

            _builder_path, body = _build_bitget_order_request(
                request, venue_sym,
                passive=True,
                profile=profile.value,
                hedge_mode=hedge_mode,
            )
            contract = get_operation_contract(
                self._transport._spec,
                VenueOperation.CREATE_ORDER,
                resolved_account_family=family,
            )
            req_path = contract.path
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

    async def fetch_account_risk_snapshot(self):
        """Fetch account risk snapshot with profile-aware routing.

        Uses the resolved Bitget contract family. Only an explicit classic/UTA
        mismatch from the UTA risk endpoint may lock the runtime to Classic and
        retry the Classic family risk contract.
        """
        import time as _time

        if self._mode != "live":
            return None

        now_ms = int(_time.time() * 1000)
        family = await self.resolve_contract_family()
        try:
            raw, _request = await request_venue_operation(
                self._transport,
                Venue.BITGET,
                VenueOperation.ACCOUNT_RISK,
                resolved_account_family=family,
            )
            if _payload_indicates_classic(raw):
                self._contract_family_resolver.lock(BitgetContractFamily.CLASSIC_MIX_V2)
                self._profile = BitgetAccountProfile.CLASSIC
                return await self._fetch_classic_risk_snapshot(now_ms)
            return _parse_bitget_risk_from_rowlike(raw, now_ms)
        except TransportError as e:
            if (
                family == BitgetContractFamily.UTA_V3
                and BitgetContractFamilyResolver._is_classic_mismatch(e)
            ):
                self._contract_family_resolver.lock(BitgetContractFamily.CLASSIC_MIX_V2)
                self._profile = BitgetAccountProfile.CLASSIC
                return await self._fetch_classic_risk_snapshot(now_ms)
            raise

    async def _fetch_classic_risk_snapshot(self, now_ms: int):
        """Fetch risk snapshot from Bitget classic account endpoint.

        V1: Classic risk uses the Mix v2 account risk family contract.
        """
        raw, _request = await request_venue_operation(
            self._transport,
            Venue.BITGET,
            VenueOperation.ACCOUNT_RISK,
            resolved_account_family=BitgetContractFamily.CLASSIC_MIX_V2,
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
        """V1: Bitget fetch_order_fill_reconciliation via order-status truth.

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
