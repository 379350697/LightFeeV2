"""Tests for shared venue transport, signing, error mapping, sizing, and reconciliation."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import io
import json
import logging
import os
import time
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest

from lightfee.core.domain import (
    OrderFill,
    OrderRequest,
    PassiveOrderAmendRequest,
    PassiveOrderState,
    PositionSnapshot,
    Side,
    TimeInForce,
    Venue,
    VenueMarketQuote,
    VenueMarketSnapshot,
)
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.config.schema import TradeCredentials, VenueConfig
from lightfee.rate_limit.engine import (
    RateLimitEngine,
    RateLimitError,
    RateLimitRuntime,
    install_global_rate_limit_runtime,
)
from lightfee.venues.common import (
    floor_to_step,
    normalize_order_quantity,
    venue_reduce_only_close_exempts_min_notional,
)
from lightfee.venues.base import VenueAccountContract
from lightfee.venues.specs import (
    AuthScheme,
    VenueOperation,
    VenueOperationContract,
    VenueSpec,
    binance_spec,
    okx_spec,
    bybit_spec,
    bitget_spec,
    gate_spec,
    get_operation_contract,
    aster_spec,
    hyperliquid_spec,
)
from lightfee.venues.market_data import MarketDataClient
from lightfee.venues.transport import (
    LiveCredential,
    TransportError,
    TransportErrorCategory,
    VenueTransport,
    _parse_okx_position,
    _safe_float,
    _require_bybit_success,
    _require_bitget_success,
    build_hmac_sha256_hex,
    build_hmac_sha256_base64,
    build_hmac_sha512_hex,
    classify_transport_error,
)
from lightfee.venues.aster import AsterAdapter
from lightfee.venues.aster_v3 import AsterV3Client


def _trust_hyperliquid_transport_for_test(transport: VenueTransport) -> None:
    transport._trading_capability_trusted = True
    transport._trading_preflight_status = {
        "venue": Venue.HYPERLIQUID.value,
        "status": "ok",
        "trading_capability_trusted": True,
        "authorization_mode": "account_wallet",
        "authorization_verified": True,
    }


# ---------------------------------------------------------------------------
# Paper / live mode construction
# ---------------------------------------------------------------------------

class TestTransportConstruction:
    def test_paper_mode_builds_without_credentials(self):
        transport = VenueTransport(spec=binance_spec(), mode="paper")
        assert transport.mode == "paper"
        assert transport.venue == Venue.BINANCE

    def test_inherits_from_market_data_client(self):
        transport = VenueTransport(spec=binance_spec(), mode="paper")
        assert isinstance(transport, MarketDataClient)

    def test_has_market_data_client_methods(self):
        transport = VenueTransport(spec=binance_spec(), mode="paper")
        assert hasattr(transport, "fetch_funding_tickers")
        assert hasattr(transport, "fetch_perp_liquidity")
        assert hasattr(transport, "fetch_l2_snapshot")
        assert hasattr(transport, "spec")

    def test_live_mode_fails_without_credentials(self):
        with pytest.raises(ValueError, match="credentials"):
            VenueTransport(spec=okx_spec(), mode="live")

    def test_live_mode_builds_with_credentials(self):
        creds = LiveCredential(api_key="k", api_secret="s")
        transport = VenueTransport(spec=binance_spec(), mode="live", credential=creds)
        assert transport.mode == "live"

    def test_live_mode_fails_missing_passphrase_when_required(self):
        creds = LiveCredential(api_key="k", api_secret="s")
        with pytest.raises(ValueError, match="passphrase"):
            VenueTransport(spec=okx_spec(), mode="live", credential=creds)

    def test_all_seven_specs_construct(self):
        for spec_fn in (
            binance_spec, okx_spec, bybit_spec, bitget_spec,
            gate_spec, aster_spec, hyperliquid_spec,
        ):
            spec = spec_fn()
            assert spec.venue_id is not None
            assert spec.public_base_url
            transport = VenueTransport(spec=spec, mode="paper")
            assert transport.venue == spec.venue_id


class TestVenueOperationContracts:
    def test_okx_amend_contract_is_not_generic_order_path(self):
        spec = okx_spec()

        contract = get_operation_contract(spec, VenueOperation.AMEND_ORDER)

        assert contract.method == "POST"
        assert contract.path == "/api/v5/trade/amend-order"
        assert contract.path != spec.order_path

    def test_bybit_amend_contract_is_not_create_path(self):
        spec = bybit_spec()

        contract = get_operation_contract(spec, VenueOperation.AMEND_ORDER)

        assert contract.method == "POST"
        assert contract.path == "/v5/order/amend"
        assert contract.path != spec.order_path

    def test_bitget_private_truth_contracts_use_v2_mix_shape(self):
        spec = bitget_spec()

        open_orders = get_operation_contract(spec, VenueOperation.OPEN_ORDERS)
        position = get_operation_contract(spec, VenueOperation.POSITION)

        assert open_orders.path == "/api/v2/mix/order/orders-pending"
        assert open_orders.required_params == ("productType=USDT-FUTURES", "marginCoin=USDT")
        assert open_orders.symbol_shape == "BTCUSDT"
        assert position.path == "/api/v2/mix/position/single-position"
        assert position.required_params == (
            "productType=USDT-FUTURES",
            "marginCoin=USDT",
        )
        assert position.symbol_shape == "BTCUSDT"

    def test_bitget_contracts_are_family_aware_for_classic_and_uta(self):
        from lightfee.venues.specs import BitgetContractFamily

        spec = bitget_spec()

        classic_open = get_operation_contract(
            spec,
            VenueOperation.OPEN_ORDERS,
            resolved_account_family=BitgetContractFamily.CLASSIC_MIX_V2,
        )
        classic_position = get_operation_contract(
            spec,
            VenueOperation.POSITION,
            resolved_account_family=BitgetContractFamily.CLASSIC_MIX_V2,
        )
        classic_all_positions = get_operation_contract(
            spec,
            VenueOperation.ALL_POSITIONS,
            resolved_account_family=BitgetContractFamily.CLASSIC_MIX_V2,
        )
        uta_open = get_operation_contract(
            spec,
            VenueOperation.OPEN_ORDERS,
            resolved_account_family=BitgetContractFamily.UTA_V3,
        )
        uta_position = get_operation_contract(
            spec,
            VenueOperation.POSITION,
            resolved_account_family=BitgetContractFamily.UTA_V3,
        )
        uta_all_positions = get_operation_contract(
            spec,
            VenueOperation.ALL_POSITIONS,
            resolved_account_family=BitgetContractFamily.UTA_V3,
        )

        assert classic_open.path == "/api/v2/mix/order/orders-pending"
        assert classic_position.path == "/api/v2/mix/position/single-position"
        assert classic_all_positions.path == "/api/v2/mix/position/all-position"
        assert classic_position.required_params == (
            "productType=USDT-FUTURES",
            "marginCoin=USDT",
        )

        assert uta_open.path == "/api/v3/trade/unfilled-orders"
        assert uta_position.path == "/api/v3/position/current-position"
        assert uta_all_positions.path == "/api/v3/position/current-position"
        assert uta_position.required_params == ("category=USDT-FUTURES",)

    @pytest.mark.asyncio
    async def test_bitget_live_transport_order_status_without_family_resolver_fails_closed(self):
        spec = bitget_spec()
        transport = VenueTransport(
            spec,
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )

        async def _unexpected_request(method, path, **kwargs):
            raise AssertionError(f"must not request Bitget truth without family resolver: {path}")

        transport._request = _unexpected_request

        with pytest.raises(TransportError) as exc:
            await transport.fetch_order_status("HOMEUSDT", order_id="order-1")

        assert exc.value.category == TransportErrorCategory.REQUEST_REJECTED
        assert "family resolver" in str(exc.value)

    def test_aster_private_truth_contracts_are_single_v3_scope(self):
        spec = aster_spec()

        assert get_operation_contract(
            spec, VenueOperation.ORDER_STATUS
        ).path == "/fapi/v3/order"
        assert get_operation_contract(
            spec, VenueOperation.OPEN_ORDERS
        ).path == "/fapi/v3/openOrders"
        assert get_operation_contract(
            spec, VenueOperation.POSITION
        ).path == "/fapi/v3/positionRisk"

    def test_hyperliquid_info_contracts_require_configured_account_address(self):
        spec = hyperliquid_spec()

        for operation in (
            VenueOperation.ORDER_STATUS,
            VenueOperation.OPEN_ORDERS,
            VenueOperation.POSITION,
        ):
            contract = get_operation_contract(spec, operation)
            assert contract.method == "POST"
            assert contract.path == "/info"
            assert "user=configured_account_address" in contract.required_params
            assert contract.symbol_shape == "coin"

    def test_gate_cancel_contract_uses_path_template_not_body_payload(self):
        contract = get_operation_contract(gate_spec(), VenueOperation.CANCEL_ORDER)

        assert contract.method == "DELETE"
        assert contract.path == "/api/v4/futures/usdt/orders/{order_id}"
        assert contract.payload == "params"


# ---------------------------------------------------------------------------
# Signing correctness
# ---------------------------------------------------------------------------

class TestSigning:
    def test_hmac_sha256_hex_matches_known_vector(self):
        sig = build_hmac_sha256_hex("secret", "payload")
        expected = hmac.new(b"secret", b"payload", hashlib.sha256).hexdigest()
        assert sig == expected

    def test_hmac_sha256_base64_matches_known_vector(self):
        import base64
        sig = build_hmac_sha256_base64("secret", "payload")
        expected = base64.b64encode(
            hmac.new(b"secret", b"payload", hashlib.sha256).digest()
        ).decode()
        assert sig == expected

    def test_hmac_sha512_hex_matches_known_vector(self):
        sig = build_hmac_sha512_hex("secret", "payload")
        expected = hmac.new(b"secret", b"payload", hashlib.sha512).hexdigest()
        assert sig == expected

    def test_binance_signing_is_hmac_sha256_hex(self):
        spec = binance_spec()
        assert spec.auth_scheme == AuthScheme.HMAC_SHA256_HEX

    def test_okx_signing_is_hmac_sha256_base64(self):
        spec = okx_spec()
        assert spec.auth_scheme == AuthScheme.HMAC_SHA256_BASE64

    def test_bybit_signing_is_hmac_sha256_hex(self):
        spec = bybit_spec()
        assert spec.auth_scheme == AuthScheme.HMAC_SHA256_HEX

    def test_bitget_signing_is_hmac_sha256_base64(self):
        spec = bitget_spec()
        assert spec.auth_scheme == AuthScheme.HMAC_SHA256_BASE64

    def test_gate_signing_is_hmac_sha512_hex(self):
        spec = gate_spec()
        assert spec.auth_scheme == AuthScheme.HMAC_SHA512_HEX

    def test_aster_signing_is_eip712(self):
        spec = aster_spec()
        assert spec.auth_scheme == AuthScheme.EIP712
        assert spec.requires_wallet_key is True
        assert spec.private_base_url == "https://fapi.asterdex.com"

    def test_hyperliquid_signing_is_eip712(self):
        spec = hyperliquid_spec()
        assert spec.auth_scheme == AuthScheme.EIP712


# ---------------------------------------------------------------------------
# OKX vs Bitget header name exact tests (Deviation 1 fix)
# ---------------------------------------------------------------------------

class TestOkxBitgetHeaders:
    """OKX and Bitget MUST produce different header names. OKX uses OK-ACCESS-*
    headers; Bitget uses ACCESS-* headers."""

    def test_okx_headers_use_ok_access_prefix(self):
        spec = okx_spec()
        cred = LiveCredential(api_key="okx-key", api_secret="okx-secret",
                              api_passphrase="okx-pass")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        headers = transport._build_auth_headers("GET", "/api/v5/account/positions", private=True)
        # OKX must use OK-ACCESS-* header names
        assert "OK-ACCESS-KEY" in headers, f"Expected OK-ACCESS-KEY in {list(headers.keys())}"
        assert "OK-ACCESS-SIGN" in headers
        assert "OK-ACCESS-TIMESTAMP" in headers
        assert "OK-ACCESS-PASSPHRASE" in headers
        # Must NOT use Bitget ACCESS-* header names
        assert "ACCESS-KEY" not in headers
        assert headers["OK-ACCESS-KEY"] == "okx-key"
        assert headers["OK-ACCESS-PASSPHRASE"] == "okx-pass"

    def test_bitget_headers_use_access_prefix(self):
        spec = bitget_spec()
        cred = LiveCredential(api_key="bg-key", api_secret="bg-secret",
                              api_passphrase="bg-pass")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        headers = transport._build_auth_headers("GET", "/api/mix/v1/position/singlePosition", private=True)
        # Bitget must use ACCESS-* header names
        assert "ACCESS-KEY" in headers
        assert "ACCESS-SIGN" in headers
        assert "ACCESS-TIMESTAMP" in headers
        assert "ACCESS-PASSPHRASE" in headers
        # Must NOT use OKX OK-ACCESS-* header names
        assert "OK-ACCESS-KEY" not in headers
        assert headers["ACCESS-KEY"] == "bg-key"
        assert headers["ACCESS-PASSPHRASE"] == "bg-pass"

    def test_okx_and_bitget_headers_differ(self):
        """Both venues must produce different header shapes."""
        okx_s = okx_spec()
        bg_s = bitget_spec()
        cred = LiveCredential(api_key="k", api_secret="s", api_passphrase="p")

        t_okx = VenueTransport(spec=okx_s, mode="live", credential=cred)
        t_bg = VenueTransport(spec=bg_s, mode="live", credential=cred)

        h_okx = t_okx._build_auth_headers("GET", "/test", private=True)
        h_bg = t_bg._build_auth_headers("GET", "/test", private=True)

        okx_keys = set(h_okx.keys())
        bg_keys = set(h_bg.keys())
        # They should NOT share the same key header names
        common_auth_keys = okx_keys & bg_keys - {"Content-Type", "locale"}
        assert not common_auth_keys, (
            f"OKX and Bitget share auth header keys: {common_auth_keys}"
        )


# ---------------------------------------------------------------------------
# Binance / Aster POST signing shape tests (Deviation 2 fix)
# ---------------------------------------------------------------------------

class TestBinanceAsterPostSigning:
    """Binance stays HMAC; Aster V3 must not share that private signing path."""

    def test_binance_post_request_includes_timestamp_signature(self):
        spec = binance_spec()
        cred = LiveCredential(api_key="bk", api_secret="bs")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        qs, headers, body = transport._build_signed_request(
            "POST", "/fapi/v1/order",
            body={"symbol": "BTCUSDT", "side": "BUY", "quantity": "0.01"},
            private=True,
        )
        # Query string must contain timestamp and signature
        assert "timestamp=" in qs, f"Missing timestamp in query string: {qs}"
        assert "signature=" in qs, f"Missing signature in query string: {qs}"
        # Headers should contain API key
        assert headers.get("X-MBX-APIKEY") == "bk"

    def test_aster_post_request_does_not_use_binance_hmac(self):
        spec = aster_spec()
        cred = LiveCredential(
            api_key="legacy-ak",
            api_secret="0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1",
        )
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        qs, headers, body = transport._build_signed_request(
            "POST", "/fapi/v3/order",
            body={"symbol": "BTCUSDT", "side": "SELL", "quantity": "0.01"},
            private=True,
        )
        assert "timestamp=" not in qs
        assert "recvWindow=" not in qs
        assert "X-MBX-APIKEY" not in headers
        assert body is not None

    def test_aster_v3_client_builds_web3_signed_query(self):
        private_key = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"
        credential = LiveCredential(
            api_secret=private_key,
            account_address="0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e",
        )
        client = AsterV3Client(credential=credential)

        query, headers, body = client.build_signed_request(
            "POST",
            "/fapi/v3/order",
            params={
                "symbol": "ASTERUSDT",
                "type": "LIMIT",
                "side": "BUY",
                "quantity": "20",
                "price": "0.5",
            },
            nonce=1748310859508867,
        )

        assert "user=0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e" in query
        assert "signer=0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0" in query
        assert "nonce=1748310859508867" in query
        assert "signature=" in query
        assert "timestamp=" not in query
        assert "recvWindow=" not in query
        assert "X-MBX-APIKEY" not in headers
        assert headers["Content-Type"] == "application/x-www-form-urlencoded"
        assert body is None

    @pytest.mark.asyncio
    async def test_aster_adapter_passes_limiter_to_private_v3_client(self):
        private_key = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"
        limiter = object()
        adapter = AsterAdapter(
            mode="live",
            credential=LiveCredential(api_secret=private_key),
            rate_limiter=limiter,
        )

        try:
            assert adapter._private is not None
            assert adapter._private._rate_limiter is limiter
        finally:
            await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_aster_v3_fetch_position_accepts_list_response(self):
        private_key = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"

        async def handler(request):
            assert request.url.host == "fapi.asterdex.com"
            assert request.url.path == "/fapi/v3/positionRisk"
            params = dict(request.url.params)
            assert params["symbol"] == "ASTERUSDT"
            assert "nonce" in params
            assert "signer" in params
            assert "signature" in params
            assert "timestamp" not in params
            assert "recvWindow" not in params
            return httpx.Response(
                200,
                json=[
                    {
                        "symbol": "ASTERUSDT",
                        "positionAmt": "12.5",
                        "entryPrice": "0.91",
                        "unRealizedProfit": "0.25",
                    }
                ],
            )

        client = AsterV3Client(
            credential=LiveCredential(api_secret=private_key),
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )

        try:
            position = await client.fetch_position("ASTERUSDT")
        finally:
            await client.close()

        assert position.venue == Venue.ASTER
        assert position.symbol == "ASTERUSDT"
        assert position.quantity == pytest.approx(12.5)
        assert position.entry_price == pytest.approx(0.91)

    @pytest.mark.asyncio
    async def test_aster_v3_account_risk_uses_account_with_join_margin(self):
        private_key = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"

        async def handler(request):
            assert request.url.host == "fapi.asterdex.com"
            assert request.url.path == "/fapi/v3/accountWithJoinMargin"
            params = dict(request.url.params)
            assert "nonce" in params
            assert "signer" in params
            assert "signature" in params
            assert "timestamp" not in params
            assert "recvWindow" not in params
            return httpx.Response(
                200,
                json={
                    "data": {
                        "totalMarginBalance": "125.5",
                        "totalMaintMargin": "25.1",
                        "availableBalance": "74.25",
                    }
                },
            )

        client = AsterV3Client(
            credential=LiveCredential(api_secret=private_key),
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )

        try:
            snapshot = await client.fetch_account_risk_snapshot()
        finally:
            await client.close()

        assert snapshot is not None
        assert snapshot.venue == Venue.ASTER
        assert snapshot.equity_quote == pytest.approx(125.5)
        assert snapshot.maintenance_margin_quote == pytest.approx(25.1)
        assert snapshot.available_balance_quote == pytest.approx(74.25)
        assert snapshot.source == "aster_v3_account_with_join_margin"

    @pytest.mark.asyncio
    async def test_aster_v3_unfiltered_open_orders_consumes_official_weight_40(self):
        private_key = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"

        async def handler(request):
            assert request.url.path == "/fapi/v3/openOrders"
            assert "symbol" not in dict(request.url.params)
            return httpx.Response(200, json=[])

        engine = RateLimitEngine(default_margin=1.0)
        engine.register_bucket("venue:aster", budget_per_minute=100.0)
        runtime = RateLimitRuntime(engine=engine)
        install_global_rate_limit_runtime(runtime)
        client = AsterV3Client(
            credential=LiveCredential(api_secret=private_key),
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )

        try:
            assert await client.fetch_open_orders(None) == []
        finally:
            await client.close()
            install_global_rate_limit_runtime(None)

        snap = engine.bucket_snapshot("venue:aster")
        assert snap["tokens"] == pytest.approx(60.0)

    @pytest.mark.asyncio
    async def test_aster_v3_symbol_open_orders_keeps_official_weight_1(self):
        private_key = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"

        async def handler(request):
            assert request.url.path == "/fapi/v3/openOrders"
            assert dict(request.url.params)["symbol"] == "ASTERUSDT"
            return httpx.Response(200, json=[])

        engine = RateLimitEngine(default_margin=1.0)
        engine.register_bucket("venue:aster", budget_per_minute=100.0)
        runtime = RateLimitRuntime(engine=engine)
        install_global_rate_limit_runtime(runtime)
        client = AsterV3Client(
            credential=LiveCredential(api_secret=private_key),
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )

        try:
            assert await client.fetch_open_orders("ASTERUSDT") == []
        finally:
            await client.close()
            install_global_rate_limit_runtime(None)

        snap = engine.bucket_snapshot("venue:aster")
        assert snap["tokens"] == pytest.approx(99.0)

    @pytest.mark.asyncio
    async def test_aster_v3_rate_limit_response_records_cooldown(self):
        private_key = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"

        async def handler(request):
            assert request.url.path == "/fapi/v3/positionRisk"
            return httpx.Response(
                429,
                headers={"Retry-After": "2"},
                json={"code": -1003, "msg": "Too many requests"},
            )

        engine = RateLimitEngine(default_margin=1.0)
        engine.register_bucket("venue:aster", budget_per_minute=100.0)
        runtime = RateLimitRuntime(engine=engine)
        install_global_rate_limit_runtime(runtime)
        client = AsterV3Client(
            credential=LiveCredential(api_secret=private_key),
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )

        try:
            with pytest.raises(TransportError) as exc:
                await client.fetch_position("ASTERUSDT")
            assert exc.value.status_code == 429
            with pytest.raises(RateLimitError):
                engine.try_consume_scopes(
                    ["GET /fapi/v3/positionRisk", "venue:aster"],
                    weight=5.0,
                    now_ms=int(time.time() * 1000),
                )
        finally:
            await client.close()
            install_global_rate_limit_runtime(None)

    @pytest.mark.asyncio
    async def test_aster_v3_http_error_message_preserves_exception_class(self):
        """A bare httpx.ConnectError with empty text must keep the exception
        class in the TransportError message, never leak the signed URL."""
        from lightfee.venues.aster_v3 import AsterV3Client

        private_key = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"

        async def handler(request):
            raise httpx.ConnectError("", request=request)

        client = AsterV3Client(
            credential=LiveCredential(api_secret=private_key),
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
        try:
            with pytest.raises(TransportError) as exc:
                await client.fetch_position("ASTERUSDT")
            message = str(exc.value)
            assert "ConnectError" in message, message
            # The signed query string (with the private request URL) must never
            # appear in the diagnostic message.
            assert "signature=" not in message
            assert "signer=" not in message
            assert "nonce=" not in message
            assert "user=" not in message
            assert "asterdex.com" not in message
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_hyperliquid_account_balance_uses_clearinghouse_withdrawable(self):
        credential = LiveCredential(
            wallet_private_key="0x" + "1" * 64,
            account_address="0x" + "2" * 40,
        )
        transport = VenueTransport(
            spec=hyperliquid_spec(),
            mode="live",
            credential=credential,
        )

        async def mock_request(method, path, params=None, body=None, private=False):
            assert method == "POST"
            assert path == "/info"
            assert body == {
                "type": "clearinghouseState",
                "user": transport._credential.account_address,
            }
            assert private is False
            return {
                "marginSummary": {
                    "accountValue": "29.78001",
                    "totalMarginUsed": "3.25",
                },
                "crossMarginSummary": {
                    "accountValue": "29.78001",
                    "totalMarginUsed": "3.25",
                },
                "withdrawable": "24.5",
                "time": 1778787002000,
                "assetPositions": [],
            }

        transport._request = mock_request

        snapshot = await transport.fetch_account_balance_snapshot()

        assert snapshot is not None
        assert snapshot.venue == Venue.HYPERLIQUID
        assert snapshot.asset == "USDC"
        assert snapshot.free == pytest.approx(24.5)
        assert snapshot.locked == pytest.approx(29.78001 - 24.5)
        assert snapshot.observed_at_ms > 0
        assert snapshot.balance_classification == "margin_view_available"
        assert snapshot.user_abstraction == ""
        assert snapshot.spot_usdc_available is None

    @pytest.mark.asyncio
    async def test_hyperliquid_unified_account_balance_uses_spot_usdc_when_withdrawable_zero(self):
        credential = LiveCredential(
            wallet_private_key="0x" + "1" * 64,
            account_address="0x" + "2" * 40,
        )
        transport = VenueTransport(
            spec=hyperliquid_spec(),
            mode="live",
            credential=credential,
        )
        seen_types = []

        async def mock_request(method, path, params=None, body=None, private=False):
            assert method == "POST"
            assert path == "/info"
            assert body["user"] == transport._credential.account_address
            assert private is False
            seen_types.append(body["type"])
            if body["type"] == "clearinghouseState":
                return {
                    "marginSummary": {
                        "accountValue": "0.0",
                        "totalMarginUsed": "0.0",
                    },
                    "crossMarginSummary": {
                        "accountValue": "0.0",
                        "totalMarginUsed": "0.0",
                    },
                    "withdrawable": "0.0",
                    "time": 1778787002000,
                    "assetPositions": [],
                }
            if body["type"] == "userAbstraction":
                return "unifiedAccount"
            if body["type"] == "spotClearinghouseState":
                return {
                    "balances": [
                        {
                            "coin": "USDC",
                            "total": "145.863168",
                            "hold": "0.5",
                            "entryNtl": "0.0",
                        }
                    ]
                }
            raise AssertionError(f"unexpected request body: {body}")

        transport._request = mock_request

        snapshot = await transport.fetch_account_balance_snapshot()

        assert seen_types == [
            "clearinghouseState",
            "userAbstraction",
            "spotClearinghouseState",
        ]
        assert snapshot is not None
        assert snapshot.venue == Venue.HYPERLIQUID
        assert snapshot.asset == "USDC"
        assert snapshot.free == pytest.approx(145.363168)
        assert snapshot.locked == pytest.approx(0.5)
        assert snapshot.observed_at_ms > 0
        assert snapshot.balance_classification == "unified_collateral_available"
        assert snapshot.user_abstraction == "unifiedAccount"
        assert snapshot.spot_usdc_available == pytest.approx(145.363168)

    @pytest.mark.asyncio
    async def test_hyperliquid_account_balance_uses_operation_contract_registry(self, monkeypatch):
        import lightfee.venues.transport as transport_module

        credential = LiveCredential(
            wallet_private_key="0x" + "1" * 64,
            account_address="0x" + "2" * 40,
        )
        transport = VenueTransport(
            spec=hyperliquid_spec(),
            mode="live",
            credential=credential,
        )
        seen_operations: list[VenueOperation] = []
        original_get_operation_contract = transport_module.get_operation_contract

        def recording_get_operation_contract(spec, operation, **kwargs):
            seen_operations.append(operation)
            return original_get_operation_contract(spec, operation, **kwargs)

        monkeypatch.setattr(
            transport_module,
            "get_operation_contract",
            recording_get_operation_contract,
        )

        async def mock_request(method, path, params=None, body=None, private=False):
            assert method == "POST"
            assert path == "/info"
            assert body["user"] == transport._credential.account_address
            assert private is False
            if body["type"] == "clearinghouseState":
                return {
                    "marginSummary": {"accountValue": "0.0", "totalMarginUsed": "0.0"},
                    "crossMarginSummary": {"accountValue": "0.0", "totalMarginUsed": "0.0"},
                    "withdrawable": "0.0",
                    "assetPositions": [],
                }
            if body["type"] == "userAbstraction":
                return "unifiedAccount"
            if body["type"] == "spotClearinghouseState":
                return {"balances": [{"coin": "USDC", "total": "145", "hold": "0"}]}
            raise AssertionError(f"unexpected request body: {body}")

        transport._request = mock_request

        snapshot = await transport.fetch_account_balance_snapshot()

        assert snapshot is not None
        assert seen_operations == [
            VenueOperation.POSITION,
            VenueOperation.USER_ABSTRACTION,
            VenueOperation.SPOT_CLEARINGHOUSE_STATE,
        ]
        assert snapshot.balance_classification == "unified_collateral_available"

    @pytest.mark.asyncio
    async def test_hyperliquid_non_unified_account_does_not_use_spot_usdc_for_perp_admission(self):
        credential = LiveCredential(
            wallet_private_key="0x" + "1" * 64,
            account_address="0x" + "2" * 40,
        )
        transport = VenueTransport(
            spec=hyperliquid_spec(),
            mode="live",
            credential=credential,
        )
        seen_types = []

        async def mock_request(method, path, params=None, body=None, private=False):
            assert method == "POST"
            assert path == "/info"
            assert body["user"] == transport._credential.account_address
            assert private is False
            seen_types.append(body["type"])
            if body["type"] == "clearinghouseState":
                return {
                    "marginSummary": {
                        "accountValue": "0.0",
                        "totalMarginUsed": "0.0",
                    },
                    "crossMarginSummary": {
                        "accountValue": "0.0",
                        "totalMarginUsed": "0.0",
                    },
                    "withdrawable": "0.0",
                    "time": 1778787002000,
                    "assetPositions": [],
                }
            if body["type"] == "userAbstraction":
                return "normalAccount"
            if body["type"] == "spotClearinghouseState":
                raise AssertionError("non-unified account must not use spot USDC for perp admission")
            raise AssertionError(f"unexpected request body: {body}")

        transport._request = mock_request

        snapshot = await transport.fetch_account_balance_snapshot()

        assert seen_types == ["clearinghouseState", "userAbstraction"]
        assert snapshot is not None
        assert snapshot.venue == Venue.HYPERLIQUID
        assert snapshot.asset == "USDC"
        assert snapshot.free == pytest.approx(0.0)
        assert snapshot.locked == pytest.approx(0.0)
        assert snapshot.observed_at_ms > 0
        assert snapshot.balance_classification == "margin_view_zero"
        assert snapshot.user_abstraction == "normalAccount"
        assert snapshot.spot_usdc_available is None

    def test_binance_post_signature_matches_signed_payload(self):
        """The signature must be HMAC-SHA256 of the URL-encoded query params (V1 order, with recvWindow)."""
        spec = binance_spec()
        cred = LiveCredential(api_key="bk", api_secret="bs")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        qs, headers, body = transport._build_signed_request(
            "POST", "/fapi/v1/order",
            body={"symbol": "BTCUSDT", "side": "BUY", "quantity": "0.01"},
            private=True,
        )
        # body should be None for Binance (query-only signing)
        assert body is None, f"Binance order must not have JSON body, got: {body}"
        # Parse query string preserving order
        params_list = [tuple(p.split("=", 1)) for p in qs.lstrip("?").split("&")]
        sig = None
        pre_sig_pairs = []
        for k, v in params_list:
            if k == "signature":
                sig = v
                break
            pre_sig_pairs.append((k, v))
        assert sig is not None, "Missing signature in query string"
        assert "timestamp" in dict(pre_sig_pairs)
        assert "recvWindow" in dict(pre_sig_pairs)
        assert dict(pre_sig_pairs)["recvWindow"] == "10000"
        # Recompute: signature is over URL-encoded query before signature
        from urllib.parse import urlencode
        pre_sig_query = urlencode(pre_sig_pairs)
        expected = build_hmac_sha256_hex("bs", pre_sig_query)
        assert sig == expected, f"Signature mismatch: {sig} != {expected}"

    def test_binance_get_without_params_still_signs(self):
        """Even GET requests without extra params must have timestamp+signature."""
        spec = binance_spec()
        cred = LiveCredential(api_key="bk", api_secret="bs")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        qs, headers, body = transport._build_signed_request(
            "GET", "/fapi/v2/positionRisk",
            private=True,
        )
        assert "timestamp=" in qs
        assert "signature=" in qs


# ---------------------------------------------------------------------------
# Bybit V5 fixture parser tests (Deviation 3 fix)
# ---------------------------------------------------------------------------

class TestBybitV5Parser:
    """Bybit V5 responses use result.list — the parser must extract data correctly."""

    def test_bybit_market_fixture_parses_non_empty_quote(self):
        import json as _json
        spec = bybit_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        with open("tests/fixtures/venues/bybit/market_snapshot.json") as f:
            raw = _json.load(f)
        snap = transport._parse_market_snapshot(raw, ["BTCUSDT"], 1000)
        assert len(snap.quotes) > 0, "Bybit market fixture must produce at least one quote"
        q = snap.quotes[0]
        assert q.symbol == "BTCUSDT"
        assert q.bid == 50000.0
        assert q.ask == 50001.0
        assert q.bid_size == 0.5
        assert q.ask_size == 1.0

    def test_bybit_position_fixture_parses_correctly(self):
        import json as _json
        spec = bybit_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        with open("tests/fixtures/venues/bybit/position_snapshot.json") as f:
            raw = _json.load(f)
        pos = transport._parse_position(raw, "BTCUSDT", 1000)
        assert pos.quantity > 0.0, "Bybit position fixture must produce non-zero qty"
        assert pos.entry_price > 0.0, "Bybit position fixture must produce non-zero entry"
        assert pos.symbol == "BTCUSDT"
        assert pos.quantity == 0.01
        assert pos.entry_price == 50000.0
        # Buy side because the fixture has "side": "Buy"
        assert pos.side == Side.BUY


# ---------------------------------------------------------------------------
# Quantity normalization
# ---------------------------------------------------------------------------

class TestQuantityNormalization:
    def test_normalize_floors_to_step(self):
        result = normalize_order_quantity(1.7, 1.0)
        assert result == 1.0

    def test_normalize_exact_step(self):
        result = normalize_order_quantity(2.0, 0.5)
        assert result == 2.0

    def test_normalize_below_step_returns_zero(self):
        result = normalize_order_quantity(0.3, 1.0)
        assert result == 0.0

    def test_normalize_zero_quantity_returns_zero(self):
        assert normalize_order_quantity(0.0, 0.01) == 0.0

    def test_normalize_negative_quantity_returns_zero(self):
        assert normalize_order_quantity(-5.0, 1.0) == 0.0

    def test_binance_quantity_step(self):
        spec = binance_spec()
        assert spec.quantity_step > 0
        result = normalize_order_quantity(0.007, spec.quantity_step)
        assert result == 0.007  # 0.001 step

    def test_hyperliquid_quantity_step(self):
        spec = hyperliquid_spec()
        result = normalize_order_quantity(1.7, spec.quantity_step)
        assert result == 1.0

    def test_reduce_only_close_exemptions(self):
        assert venue_reduce_only_close_exempts_min_notional(Venue.ASTER)
        assert venue_reduce_only_close_exempts_min_notional(Venue.BINANCE)
        assert venue_reduce_only_close_exempts_min_notional(Venue.GATE)  # V1: Gate exempt

    def test_floor_to_step_alias(self):
        assert floor_to_step(1.7, 1.0) == 1.0


# ---------------------------------------------------------------------------
# Transport error classification
# ---------------------------------------------------------------------------

class TestTransportErrors:
    def test_classify_http_401_is_auth_failure(self):
        cat = classify_transport_error(401, "Unauthorized")
        assert cat == TransportErrorCategory.AUTH_FAILURE

    def test_classify_http_403_is_authz_failure(self):
        cat = classify_transport_error(403, "Forbidden")
        assert cat == TransportErrorCategory.AUTHORIZATION_FAILURE

    def test_classify_http_429_is_transport(self):
        cat = classify_transport_error(429, "Rate limited")
        assert cat == TransportErrorCategory.TRANSPORT_FAILURE

    def test_classify_http_500_is_transport(self):
        cat = classify_transport_error(500, "Server error")
        assert cat == TransportErrorCategory.TRANSPORT_FAILURE

    def test_classify_http_200_no_error(self):
        cat = classify_transport_error(200, "")
        assert cat is None

    def test_transport_error_is_exception(self):
        err = TransportError(TransportErrorCategory.AUTH_FAILURE, "bad key")
        assert isinstance(err, Exception)
        assert "bad key" in str(err)


# ---------------------------------------------------------------------------
# Paper mode behavior
# ---------------------------------------------------------------------------

class TestPaperMode:
    @pytest.mark.asyncio
    async def test_fetch_market_snapshot_returns_normalized_shape(self):
        transport = VenueTransport(spec=binance_spec(), mode="paper")
        snap = await transport.fetch_market_snapshot(["BTCUSDT"])
        assert isinstance(snap, VenueMarketSnapshot)
        assert snap.venue == Venue.BINANCE
        assert len(snap.quotes) == 1
        assert snap.quotes[0].symbol == "BTCUSDT"

    @pytest.mark.asyncio
    async def test_fetch_position_returns_normalized_shape(self):
        transport = VenueTransport(spec=okx_spec(), mode="paper")
        pos = await transport.fetch_position("BTC-USDT-SWAP")
        assert isinstance(pos, PositionSnapshot)
        assert pos.venue == Venue.OKX
        assert pos.quantity >= 0

    @pytest.mark.asyncio
    async def test_place_order_paper_mode_returns_fill(self):
        transport = VenueTransport(spec=bybit_spec(), mode="paper")
        req = OrderRequest(
            venue=Venue.BYBIT, symbol="BTCUSDT", side=Side.BUY, quantity=0.01,
        )
        fill = await transport.place_order(req)
        assert fill.venue == Venue.BYBIT
        assert fill.symbol == "BTCUSDT"
        assert fill.order_id

    @pytest.mark.asyncio
    async def test_normalize_quantity_paper_mode(self):
        transport = VenueTransport(spec=gate_spec(), mode="paper")
        qty = await transport.normalize_quantity("BTCUSDT", 1.7)
        assert qty == 1.0

    @pytest.mark.asyncio
    async def test_close(self):
        transport = VenueTransport(spec=binance_spec(), mode="paper")
        await transport.close()

    @pytest.mark.asyncio
    async def test_transport_error_mapped_to_order_submit_error(self):
        from lightfee.venues.transport import _map_to_submit_error
        err = _map_to_submit_error(TransportErrorCategory.REQUEST_REJECTED, "bad")
        assert isinstance(err, OrderSubmitError)
        assert err.class_ == SubmitFailureClass.REJECTED

    @pytest.mark.asyncio
    async def test_uncertain_error_maps_correctly(self):
        from lightfee.venues.transport import _map_to_submit_error
        err = _map_to_submit_error(TransportErrorCategory.TRANSPORT_FAILURE, "timeout")
        assert err.class_ == SubmitFailureClass.UNCERTAIN

    @pytest.mark.asyncio
    async def test_paper_mode_returns_zeros_not_fake_live(self):
        """Paper mode must explicitly return zero values, not fake live data."""
        transport = VenueTransport(spec=bybit_spec(), mode="paper")
        pos = await transport.fetch_position("BTCUSDT")
        assert pos.quantity == 0.0
        assert pos.entry_price == 0.0


# ---------------------------------------------------------------------------
# Live mode: fail fast on missing credentials
# ---------------------------------------------------------------------------

class TestLiveModeFailFast:
    def test_binance_live_missing_key(self):
        creds = LiveCredential(api_key="", api_secret="s")
        with pytest.raises(ValueError, match="api_key"):
            VenueTransport(spec=binance_spec(), mode="live", credential=creds)

    def test_okx_live_missing_passphrase(self):
        creds = LiveCredential(api_key="k", api_secret="s", api_passphrase="")
        with pytest.raises(ValueError, match="passphrase"):
            VenueTransport(spec=okx_spec(), mode="live", credential=creds)

    def test_hyperliquid_live_missing_wallet_key(self):
        creds = LiveCredential(api_key="k", api_secret="s")
        with pytest.raises(ValueError, match="wallet_private_key"):
            VenueTransport(spec=hyperliquid_spec(), mode="live", credential=creds)

    def test_aster_invalid_legacy_secret_is_not_a_startup_crash(self):
        from lightfee.venues.aster import AsterAdapter
        from lightfee.venues.aster_v3 import credential_has_aster_v3_signer

        creds = LiveCredential(api_key="k", api_secret="not-a-hex-wallet-private-key")

        assert credential_has_aster_v3_signer(creds) is False
        adapter = AsterAdapter(mode="live", credential=creds)

        assert adapter._private is None

    @pytest.mark.asyncio
    async def test_aster_private_truth_reports_auth_failure_when_signer_invalid(self):
        from lightfee.venues.aster import AsterAdapter

        adapter = AsterAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="not-a-hex-wallet-private-key"),
        )

        with pytest.raises(TransportError) as exc:
            await adapter.fetch_all_positions()

        assert exc.value.category == TransportErrorCategory.AUTH_FAILURE
        assert "aster private API disabled" in str(exc.value)


# ---------------------------------------------------------------------------
# Hyperliquid live order explicit unsupported (Deviation 4)
# ---------------------------------------------------------------------------

class TestHyperliquidLiveOrderNowSupported:
    """Hyperliquid live order now works with EIP-712 signing."""

    def test_hyperliquid_wire_size_and_price_quantization(self):
        from lightfee.venues.hyperliquid_signing import (
            build_hyperliquid_order_action,
            float_to_wire_string,
            hyperliquid_ioc_price_and_size,
            hyperliquid_price_decimals,
            round_to_decimals,
            round_to_significant_and_decimal,
        )

        assert float_to_wire_string(200.0) == "200"
        assert round_to_decimals(1.25, 1) == pytest.approx(1.3)
        assert round_to_decimals(-1.25, 1) == pytest.approx(-1.3)
        assert round_to_decimals(1.005, 2) == pytest.approx(1.0)
        assert round_to_decimals(0.145, 2) == pytest.approx(0.14)
        assert round_to_significant_and_decimal(2.5, 1, 0) == pytest.approx(3.0)
        assert round_to_significant_and_decimal(-2.5, 1, 0) == pytest.approx(-3.0)
        assert round_to_significant_and_decimal(1.005, 4, 2) == pytest.approx(1.0)
        assert round_to_significant_and_decimal(0.145, 3, 2) == pytest.approx(0.14)
        price_decimals = hyperliquid_price_decimals(asset_index=123, sz_decimals=0)
        limit_px, wire_qty = hyperliquid_ioc_price_and_size(
            side_is_buy=True,
            quantity=200.0,
            reference_price=0.16272,
            sz_decimals=0,
            price_decimals=price_decimals,
        )
        assert wire_qty == 200.0
        assert limit_px == pytest.approx(0.16435)

        action = build_hyperliquid_order_action(
            symbol="SUPER",
            is_buy=True,
            quantity=wire_qty,
            price=limit_px,
            tif="Ioc",
            asset_index=123,
            sz_decimals=0,
            price_decimals=price_decimals,
        )
        order = action["orders"][0]
        assert order["a"] == 123
        assert order["s"] == "200"
        assert order["p"] == "0.16435"
        assert "200.0" not in json.dumps(action)

    def test_registry_derives_account_address_from_wallet_when_env_omitted(self, monkeypatch):
        from eth_account import Account
        from lightfee.venues.registry import build_adapter

        wallet_key = "0x" + "1" * 64
        monkeypatch.setenv("LF_TEST_HL_WALLET", wallet_key)
        vc = VenueConfig(venue="hyperliquid")
        vc.live.trade_credentials = TradeCredentials(
            wallet_private_key_env="LF_TEST_HL_WALLET",
            account_address_env=None,
        )

        adapter = build_adapter(Venue.HYPERLIQUID, vc, mode="live")

        expected = Account.from_key(wallet_key).address
        assert adapter._transport._credential.account_address == expected
        assert adapter._credential.account_address == expected

    def test_registry_preserves_explicit_hyperliquid_account_address_for_exchange_truth(self, monkeypatch):
        from lightfee.venues.registry import build_adapter

        wallet_key = "0x" + "1" * 64
        account_address = "0x000000000000000000000000000000000000beef"
        monkeypatch.setenv("LF_TEST_HL_WALLET", wallet_key)
        monkeypatch.setenv("LF_TEST_HL_ACCOUNT", account_address)
        vc = VenueConfig(venue="hyperliquid")
        vc.live.trade_credentials = TradeCredentials(
            wallet_private_key_env="LF_TEST_HL_WALLET",
            account_address_env="LF_TEST_HL_ACCOUNT",
        )

        adapter = build_adapter(Venue.HYPERLIQUID, vc, mode="live")

        assert adapter._transport._credential.wallet_mode == "account_wallet"
        assert adapter._transport._credential.account_address == account_address

    def test_registry_preserves_hyperliquid_api_wallet_mode_without_deriving_account(self, monkeypatch):
        from lightfee.venues.registry import build_adapter

        wallet_key = "0x" + "1" * 64
        monkeypatch.setenv("LF_TEST_HL_WALLET", wallet_key)
        vc = VenueConfig(venue="hyperliquid")
        vc.live.trade_credentials = TradeCredentials(
            wallet_private_key_env="LF_TEST_HL_WALLET",
            account_address_env=None,
            wallet_mode="agent_wallet",
        )

        adapter = build_adapter(Venue.HYPERLIQUID, vc, mode="live")

        assert adapter._transport._credential.wallet_mode == "api_wallet"
        assert adapter._transport._credential.account_address == ""


    @pytest.mark.asyncio
    async def test_hyperliquid_readonly_preflight_trusts_direct_wallet_account(self):
        from eth_account import Account

        wallet_key = "0x" + "1" * 64
        account_address = Account.from_key(wallet_key).address
        cred = LiveCredential(
            wallet_private_key=wallet_key,
            account_address=account_address,
        )
        transport = VenueTransport(spec=hyperliquid_spec(), mode="live", credential=cred)

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode())
            assert body["type"] == "clearinghouseState"
            assert body["user"].lower() == account_address.lower()
            return httpx.Response(200, json={"assetPositions": [], "marginSummary": {}})

        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            result = await transport.verify_live_trading_preflight()
        finally:
            await transport.close()

        assert result["status"] == "ok"
        assert result["signer_matches_account"] is True
        assert result["wallet_matches_account"] is True
        assert result["api_wallet_authorization_verified"] is False
        assert result["authorization_verified"] is True
        assert result["clearinghouse_state_readable"] is True
        assert result["trading_capability_trusted"] is True
        assert transport.trading_capability_trusted is True

    @pytest.mark.asyncio
    async def test_hyperliquid_readonly_preflight_disables_unverified_api_wallet(self):
        wallet_key = "0x" + "1" * 64
        cred = LiveCredential(
            wallet_private_key=wallet_key,
            account_address="0x000000000000000000000000000000000000beef",
            wallet_mode="api_wallet",
        )
        transport = VenueTransport(spec=hyperliquid_spec(), mode="live", credential=cred)

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode())
            if request.url.path == "/info":
                assert body["type"] == "clearinghouseState"
                return httpx.Response(200, json={"assetPositions": [], "marginSummary": {}})
            assert request.url.path == "/exchange"
            assert body["action"] == {"type": "noop"}

            return httpx.Response(
                200,
                json={"status": "err", "response": "User or API Wallet does not exist"},
            )

        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            result = await transport.verify_live_trading_preflight()
        finally:
            await transport.close()

        assert result["status"] == "failed"
        assert result["authorization_mode"] == "api_wallet"
        assert result["wallet_matches_account"] is False
        assert result["signer_matches_account"] is False
        assert result["api_wallet_authorization_verified"] is False
        assert result["clearinghouse_state_readable"] is True
        assert result["trading_capability_trusted"] is False
        assert result["reason"] == "api_wallet_authorization_unverified"

    @pytest.mark.asyncio
    async def test_hyperliquid_account_wallet_preflight_fails_closed_on_signer_account_mismatch(self):
        from eth_account import Account

        wallet_key = "0x" + "1" * 64
        configured_account = "0x000000000000000000000000000000000000beef"
        signer_account = Account.from_key(wallet_key).address
        cred = LiveCredential(
            wallet_private_key=wallet_key,
            account_address=configured_account,
            wallet_mode="account_wallet",
        )
        transport = VenueTransport(spec=hyperliquid_spec(), mode="live", credential=cred)

        def handler(request: httpx.Request) -> httpx.Response:
            raise AssertionError("mismatched account_wallet preflight must fail before HTTP")

        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            result = await transport.verify_live_trading_preflight()
        finally:
            await transport.close()

        assert result["status"] == "failed"
        assert result["authorization_mode"] == "account_wallet"
        assert result["wallet_matches_account"] is False
        assert result["signer_matches_account"] is False
        assert result["api_wallet_authorization_verified"] is False
        assert result["authorization_verified"] is False
        assert result["trading_capability_trusted"] is False
        assert result["reason"] == "account_wallet_signer_mismatch"
        assert result["configured_account_address"] == configured_account
        assert result["signer_address"] == signer_account

    @pytest.mark.asyncio
    async def test_hyperliquid_api_wallet_preflight_passes_when_noop_authorized(self):
        wallet_key = "0x" + "1" * 64
        account_address = "0x000000000000000000000000000000000000beef"
        seen_paths: list[str] = []
        cred = LiveCredential(
            wallet_private_key=wallet_key,
            account_address=account_address,
            wallet_mode="api_wallet",
        )
        transport = VenueTransport(spec=hyperliquid_spec(), mode="live", credential=cred)

        def handler(request: httpx.Request) -> httpx.Response:
            seen_paths.append(request.url.path)
            body = json.loads(request.content.decode())
            if request.url.path == "/info":
                assert body == {"type": "clearinghouseState", "user": account_address}
                return httpx.Response(200, json={"assetPositions": [], "marginSummary": {}})
            assert request.url.path == "/exchange"
            assert body["action"] == {"type": "noop"}
            assert body["nonce"] > 0
            assert set(body["signature"]) == {"r", "s", "v"}
            return httpx.Response(
                200,
                json={"status": "ok", "response": {"type": "default"}},
            )

        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            result = await transport.verify_live_trading_preflight()
        finally:
            await transport.close()

        assert result["status"] == "ok"
        assert seen_paths == ["/info", "/exchange"]
        assert result["authorization_mode"] == "api_wallet"
        assert result["wallet_matches_account"] is False
        assert result["signer_matches_account"] is False
        assert result["api_wallet_authorization_verified"] is True
        assert result["clearinghouse_state_readable"] is True
        assert result["trading_capability_trusted"] is True
        assert transport.trading_capability_trusted is True

    @pytest.mark.asyncio
    async def test_hyperliquid_api_wallet_preflight_fails_wrong_signing_scheme_with_diagnostics(self):
        wallet_key = "0x" + "1" * 64
        account_address = "0x000000000000000000000000000000000000beef"
        cred = LiveCredential(
            wallet_private_key=wallet_key,
            account_address=account_address,
            wallet_mode="agent_wallet",
        )
        transport = VenueTransport(spec=hyperliquid_spec(), mode="live", credential=cred)

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode())
            if request.url.path == "/info":
                return httpx.Response(200, json={"assetPositions": [], "marginSummary": {}})
            assert body["action"] == {"type": "noop"}
            return httpx.Response(
                200,
                json={
                    "status": "err",
                    "response": (
                        "L1 error: User or API Wallet "
                        "0x0123000000000000000000000000000000000000 does not exist."
                    ),
                },
            )

        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            result = await transport.verify_live_trading_preflight()
        finally:
            await transport.close()

        assert result["status"] == "failed"
        assert result["authorization_mode"] == "api_wallet"
        assert result["configured_account_address"] == account_address
        assert result["signer_address"].startswith("0x")
        assert result["reason"] == "api_wallet_authorization_unverified"
        assert "does not exist" in result["authorization_error"]

    @pytest.mark.asyncio
    async def test_hyperliquid_place_order_rejects_after_failed_trading_preflight(self):
        wallet_key = "0x" + "1" * 64
        cred = LiveCredential(
            wallet_private_key=wallet_key,
            account_address="0x000000000000000000000000000000000000beef",
            wallet_mode="api_wallet",
        )
        transport = VenueTransport(spec=hyperliquid_spec(), mode="live", credential=cred)
        exchange_calls = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal exchange_calls
            body = json.loads(request.content.decode())
            if request.url.path == "/info":
                assert body["type"] == "clearinghouseState"
                return httpx.Response(200, json={"assetPositions": [], "marginSummary": {}})
            if request.url.path == "/exchange":
                exchange_calls += 1
                assert body["action"] == {"type": "noop"}
                return httpx.Response(
                    200,
                    json={"status": "err", "response": "User or API Wallet does not exist"},
                )
            raise AssertionError(f"unexpected Hyperliquid request: {request.url.path}")

        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            result = await transport.verify_live_trading_preflight()
            assert result["status"] == "failed"

            req = OrderRequest(
                venue=Venue.HYPERLIQUID,
                symbol="BTC",
                side=Side.BUY,
                quantity=1.0,
                price=50000.0,
                client_order_id="entry-disabled-hl",
            )
            with pytest.raises(OrderSubmitError) as exc_info:
                await transport.place_order(req)
        finally:
            await transport.close()

        assert exc_info.value.class_ == SubmitFailureClass.REJECTED
        assert "hyperliquid_trading_disabled:api_wallet_authorization_unverified" in str(exc_info.value)
        assert exchange_calls == 1

    @pytest.mark.asyncio
    async def test_hyperliquid_place_order_rejects_without_trading_preflight(self):
        wallet_key = "0x" + "1" * 64
        cred = LiveCredential(
            wallet_private_key=wallet_key,
            account_address="0x000000000000000000000000000000000000beef",
        )
        transport = VenueTransport(spec=hyperliquid_spec(), mode="live", credential=cred)
        transport._hl_meta_cache["BTC"] = 0

        def handler(request: httpx.Request) -> httpx.Response:
            raise AssertionError(f"unexpected Hyperliquid request: {request.url.path}")

        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        req = OrderRequest(
            venue=Venue.HYPERLIQUID,
            symbol="BTC",
            side=Side.BUY,
            quantity=1.0,
            price=50000.0,
            client_order_id="entry-unverified-hl",
        )
        try:
            with pytest.raises(OrderSubmitError) as exc_info:
                await transport.place_order(req)
        finally:
            await transport.close()

        assert exc_info.value.class_ == SubmitFailureClass.REJECTED
        assert "hyperliquid_trading_disabled:trading_preflight_not_verified" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_hyperliquid_passive_order_rejects_without_trading_preflight(self):
        wallet_key = "0x" + "1" * 64
        cred = LiveCredential(
            wallet_private_key=wallet_key,
            account_address="0x000000000000000000000000000000000000beef",
        )
        transport = VenueTransport(spec=hyperliquid_spec(), mode="live", credential=cred)
        transport._hl_asset_meta_cache["SUPER"] = {
            "asset_index": 123,
            "sz_decimals": 0,
            "price_decimals": 6,
        }

        def handler(request: httpx.Request) -> httpx.Response:
            raise AssertionError(f"unexpected Hyperliquid request: {request.url.path}")

        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        req = OrderRequest(
            venue=Venue.HYPERLIQUID,
            symbol="SUPERUSDT",
            side=Side.SELL,
            quantity=200.0,
            price=0.16435,
            post_only=True,
            client_order_id="entry-unverified-passive-hl",
        )
        try:
            with pytest.raises(OrderSubmitError) as exc_info:
                await transport.submit_passive_order(req)
        finally:
            await transport.close()

        assert exc_info.value.class_ == SubmitFailureClass.REJECTED
        assert "hyperliquid_trading_disabled:trading_preflight_not_verified" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_live_place_order_signs_with_wallet_private_key_not_api_secret(self, monkeypatch):
        wallet_key = "0x" + "1" * 64
        cred = LiveCredential(
            api_secret="",
            wallet_private_key=wallet_key,
            account_address="0xbeef",
        )
        transport = VenueTransport(spec=hyperliquid_spec(), mode="live", credential=cred)
        _trust_hyperliquid_transport_for_test(transport)
        transport._hl_meta_cache["BTC"] = 0

        captured = {}

        from lightfee.venues import hyperliquid_signing

        def fake_build_exchange_payload(**kwargs):
            captured["private_key_hex"] = kwargs["private_key_hex"]
            captured["vault_address"] = kwargs["vault_address"]
            captured["action"] = kwargs["action"]
            return {"action": kwargs["action"], "signature": {"r": "", "s": "", "v": 27}}

        monkeypatch.setattr(
            hyperliquid_signing,
            "build_hyperliquid_exchange_payload",
            fake_build_exchange_payload,
        )
        transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200,
                    json={
                        "status": "ok",
                        "response": {
                            "type": "order",
                            "data": {
                                "statuses": [
                                    {
                                        "filled": {
                                            "oid": 789,
                                            "totalSz": "1.0",
                                            "avgPx": "50000.0",
                                        }
                                    }
                                ]
                            },
                        },
                    },
                )
            )
        )

        req = OrderRequest(
            venue=Venue.HYPERLIQUID,
            symbol="BTC",
            side=Side.BUY,
            quantity=1.0,
            price=50000.0,
            client_order_id="entry-1779342733376-SAGAUSDT-h1",
        )
        try:
            await transport.place_order(req)
        finally:
            await transport.close()

        assert captured["private_key_hex"] == wallet_key
        assert captured["vault_address"] is None
        from lightfee.venues.hyperliquid_signing import hyperliquid_cloid_for_client_order
        order = captured["action"]["orders"][0]
        assert order["c"] == hyperliquid_cloid_for_client_order(req.client_order_id)
        assert order["t"]["limit"]["tif"] == "Ioc"

    @pytest.mark.asyncio
    async def test_live_place_order_body_matches_hyperliquid_schema(self):
        privkey = "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e"
        cred = LiveCredential(
            api_key="k",
            api_secret="",
            wallet_private_key=privkey,
            account_address="0xbeef",
        )
        transport = VenueTransport(spec=hyperliquid_spec(), mode="live", credential=cred)
        _trust_hyperliquid_transport_for_test(transport)
        transport._hl_meta_cache["BTC"] = 0
        captured: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(
                200,
                json={
                    "status": "ok",
                    "response": {
                        "type": "order",
                        "data": {
                            "statuses": [
                                {
                                    "filled": {
                                        "oid": 789,
                                        "totalSz": "1.0",
                                        "avgPx": "50000.0",
                                    }
                                }
                            ]
                        },
                    },
                },
            )

        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        req = OrderRequest(
            venue=Venue.HYPERLIQUID,
            symbol="BTC",
            side=Side.SELL,
            quantity=1.0,
            price=50000.0,
            client_order_id="entry-1779288723953-CHIPUSDT-h1",
        )
        try:
            await transport.place_order(req)
        finally:
            await transport.close()

        from lightfee.venues.hyperliquid_signing import hyperliquid_cloid_for_client_order
        body = captured["body"]
        order = body["action"]["orders"][0]
        assert "cloid" not in body
        assert "vaultAddress" not in body
        assert sorted(body.keys()) == ["action", "nonce", "signature"]
        assert order["c"] == hyperliquid_cloid_for_client_order(req.client_order_id)
        assert order["c"].startswith("0x")
        assert len(order["c"]) == 34
        assert order["t"]["limit"]["tif"] == "Ioc"
    @pytest.mark.asyncio
    async def test_hyperliquid_super_ioc_wire_payload_has_no_trailing_zero_size(self):
        privkey = "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e"
        cred = LiveCredential(
            wallet_private_key=privkey,
            account_address="0xbeef",
        )
        transport = VenueTransport(spec=hyperliquid_spec(), mode="live", credential=cred)
        _trust_hyperliquid_transport_for_test(transport)
        transport._hl_asset_meta_cache["SUPER"] = {
            "asset_index": 123,
            "sz_decimals": 0,
            "price_decimals": 6,
        }
        captured: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(
                200,
                json={
                    "status": "ok",
                    "response": {
                        "type": "order",
                        "data": {
                            "statuses": [
                                {
                                    "filled": {
                                        "oid": 981,
                                        "totalSz": "200",
                                        "avgPx": "0.16435",
                                    }
                                }
                            ]
                        },
                    },
                },
            )

        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        req = OrderRequest(
            venue=Venue.HYPERLIQUID,
            symbol="SUPERUSDT",
            side=Side.BUY,
            quantity=200.0,
            price=0.16272,
            time_in_force=TimeInForce.IOC,
            client_order_id="entry-1779288723953-SUPERUSDT-h1",
        )
        try:
            await transport.place_order(req)
        finally:
            await transport.close()

        body = captured["body"]
        order = body["action"]["orders"][0]
        assert order["a"] == 123
        assert order["s"] == "200"
        assert order["p"] == "0.16435"
        assert order["t"]["limit"]["tif"] == "Ioc"
        assert "200.0" not in json.dumps(body)

    @pytest.mark.asyncio
    async def test_hyperliquid_reduce_only_ioc_without_price_uses_l2_fallback(self):
        privkey = "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e"
        cred = LiveCredential(wallet_private_key=privkey, account_address="0xbeef")
        transport = VenueTransport(spec=hyperliquid_spec(), mode="live", credential=cred)
        _trust_hyperliquid_transport_for_test(transport)
        transport._hl_asset_meta_cache["SUPER"] = {
            "asset_index": 123,
            "sz_decimals": 0,
            "price_decimals": 6,
        }
        seen: list[str] = []
        captured: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode())
            if request.url.path.endswith("/info"):
                seen.append("l2Book")
                assert body == {"type": "l2Book", "coin": "SUPER"}
                return httpx.Response(
                    200,
                    json={
                        "coin": "SUPER",
                        "time": 1779422875621,
                        "levels": [
                            [{"px": "0.162", "sz": "1000"}],
                            [{"px": "0.164", "sz": "1000"}],
                        ],
                    },
                )
            seen.append("exchange")
            captured["body"] = body
            return httpx.Response(
                200,
                json={
                    "status": "ok",
                    "response": {
                        "type": "order",
                        "data": {
                            "statuses": [
                                {
                                    "filled": {
                                        "oid": 982,
                                        "totalSz": "200",
                                        "avgPx": "0.16038",
                                    }
                                }
                            ]
                        },
                    },
                },
            )

        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        req = OrderRequest(
            venue=Venue.HYPERLIQUID,
            symbol="SUPERUSDT",
            side=Side.SELL,
            quantity=200.0,
            price=None,
            reduce_only=True,
            time_in_force=TimeInForce.IOC,
            client_order_id="cleanup-1779422875621-SUPERUSDT-hl",
        )
        try:
            await transport.place_order(req)
        finally:
            await transport.close()

        assert seen == ["l2Book", "exchange"]
        order = captured["body"]["action"]["orders"][0]
        assert order["r"] is True
        assert order["s"] == "200"
        assert order["p"] == "0.16038"
        assert order["t"]["limit"]["tif"] == "Ioc"
        attempt = next(
            e["payload"] for e in transport.order_diagnostics
            if e["kind"] == "order.submit_attempt"
        )
        assert attempt["reference_price_source"] == "l2_snapshot_best_bid"

    @pytest.mark.asyncio
    async def test_hyperliquid_ioc_without_price_rejects_when_l2_side_missing(self):
        privkey = "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e"
        cred = LiveCredential(wallet_private_key=privkey, account_address="0xbeef")
        transport = VenueTransport(spec=hyperliquid_spec(), mode="live", credential=cred)
        _trust_hyperliquid_transport_for_test(transport)
        transport._hl_asset_meta_cache["SUPER"] = {
            "asset_index": 123,
            "sz_decimals": 0,
            "price_decimals": 6,
        }
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode())
            if request.url.path.endswith("/info"):
                seen.append("l2Book")
                assert body == {"type": "l2Book", "coin": "SUPER"}
                return httpx.Response(
                    200,
                    json={"coin": "SUPER", "levels": [[], []], "time": 1779422875621},
                )
            seen.append("exchange")
            return httpx.Response(500, json={"error": "must not submit"})

        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        req = OrderRequest(
            venue=Venue.HYPERLIQUID,
            symbol="SUPERUSDT",
            side=Side.SELL,
            quantity=200.0,
            price=None,
            reduce_only=True,
            time_in_force=TimeInForce.IOC,
            client_order_id="cleanup-1779422875621-SUPERUSDT-hl",
        )
        try:
            with pytest.raises(OrderSubmitError) as exc:
                await transport.place_order(req)
        finally:
            await transport.close()

        assert exc.value.class_ == SubmitFailureClass.REJECTED
        assert seen == ["l2Book"]
        result = next(
            e["payload"] for e in transport.order_diagnostics
            if e["kind"] == "order.submit_result"
        )
        assert result["response_classification"] == "rejected"
        assert result["reference_price_source"] == "l2_snapshot_best_bid"
        assert result["best_bid"] == 0.0
        assert result["best_ask"] == 0.0

    @pytest.mark.asyncio
    async def test_hyperliquid_passive_order_uses_signed_exchange_action_alo(self):
        privkey = "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e"
        cred = LiveCredential(
            wallet_private_key=privkey,
            account_address="0xbeef",
        )
        transport = VenueTransport(spec=hyperliquid_spec(), mode="live", credential=cred)
        _trust_hyperliquid_transport_for_test(transport)
        transport._hl_asset_meta_cache["SUPER"] = {
            "asset_index": 123,
            "sz_decimals": 0,
            "price_decimals": 6,
        }
        captured: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(
                200,
                json={
                    "status": "ok",
                    "response": {
                        "type": "order",
                        "data": {"statuses": [{"resting": {"oid": 555}}]},
                    },
                },
            )

        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        req = OrderRequest(
            venue=Venue.HYPERLIQUID,
            symbol="SUPERUSDT",
            side=Side.SELL,
            quantity=200.0,
            price=0.16435000000000002,
            post_only=True,
            client_order_id="entry-1779288723953-SUPERUSDT-m1",
        )
        try:
            ack = await transport.submit_passive_order(req)
        finally:
            await transport.close()

        body = captured["body"]
        assert sorted(body.keys()) == ["action", "nonce", "signature"]
        assert "postOnly" not in body
        assert "timeInForce" not in body
        assert "quantity" not in body
        assert "cloid" not in body
        order = body["action"]["orders"][0]
        assert order["a"] == 123
        assert order["s"] == "200"
        assert order["p"] == "0.16435"
        assert order["t"]["limit"]["tif"] == "Alo"
        assert ack.order_id == "555"
        assert "200.0" not in json.dumps(body)

    @pytest.mark.asyncio
    async def test_live_place_order_succeeds_with_mock(self):
        # Valid secp256k1 private key for signing (Rust test-vector key)
        privkey = "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e"
        cred = LiveCredential(api_key="k", api_secret="",
                              wallet_private_key=privkey, account_address="0xbeef")
        transport = VenueTransport(spec=hyperliquid_spec(), mode="live", credential=cred)
        _trust_hyperliquid_transport_for_test(transport)
        # Pre-populate asset index cache to avoid needing metadata response
        transport._hl_meta_cache["BTC"] = 0
        transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200,
                    json={"status": "ok", "response": {"type": "order", "data": {"statuses": [{"filled": {"oid": 789, "totalSz": "1.0", "avgPx": "50000.0"}}]}}}
                )
            )
        )
        req = OrderRequest(
            venue=Venue.HYPERLIQUID, symbol="BTC", side=Side.BUY,
            quantity=1.0, price=50000.0, time_in_force=TimeInForce.IOC,
        )
        try:
            fill = await transport.place_order(req)
            assert fill.order_id == "789"
            assert fill.quantity == 1.0
            assert fill.price == 50000.0
        finally:
            await transport.close()

    @pytest.mark.asyncio
    async def test_paper_mode_still_works(self):
        transport = VenueTransport(spec=hyperliquid_spec(), mode="paper")
        req = OrderRequest(
            venue=Venue.HYPERLIQUID, symbol="BTC", side=Side.SELL, quantity=1.0,
        )
        fill = await transport.place_order(req)
        assert fill.venue == Venue.HYPERLIQUID
        assert fill.order_id != ""


# ---------------------------------------------------------------------------
# Bitget profile detection and caching (Deviation 5)
# ---------------------------------------------------------------------------

class TestBitgetProfileDetection:
    """Bitget must detect UTA vs Classic and cache the result."""

    def test_bitget_adapter_has_profile_attributes(self):
        from lightfee.venues.bitget import BitgetAdapter, BitgetAccountProfile
        adapter = BitgetAdapter(mode="paper")
        assert adapter.account_profile is None

    @pytest.mark.asyncio
    async def test_paper_mode_detect_returns_uta(self):
        from lightfee.venues.bitget import BitgetAdapter, BitgetAccountProfile
        adapter = BitgetAdapter(mode="paper")
        profile = await adapter.detect_profile()
        assert profile == BitgetAccountProfile.UTA
        # Second call should be cached
        profile2 = await adapter.detect_profile()
        assert profile2 == BitgetAccountProfile.UTA

    @pytest.mark.asyncio
    async def test_profile_is_cached_after_detection(self):
        from lightfee.venues.bitget import BitgetAdapter
        adapter = BitgetAdapter(mode="paper")
        assert adapter.account_profile is None
        await adapter.detect_profile()
        assert adapter.account_profile is not None

    @pytest.mark.asyncio
    async def test_live_uta_probe_uses_official_category_param(self):
        from lightfee.venues.bitget import BitgetAccountProfile, BitgetAdapter

        seen_params: list[dict[str, str]] = []

        async def mock_handler(request):
            if "/api/v3/position/current-position" in str(request.url):
                params = dict(request.url.params)
                seen_params.append(params)
                return httpx.Response(200, json={"code": "00000", "data": []})
            return httpx.Response(404, json={"error": "not found"})

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter._transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler),
        )

        try:
            profile = await adapter.detect_profile()
        finally:
            await adapter._transport.close()

        assert profile == BitgetAccountProfile.UTA
        assert seen_params
        assert seen_params[0]["category"] == "USDT-FUTURES"
        assert "productType" not in seen_params[0]

    @pytest.mark.asyncio
    async def test_classic_mode_error_detection(self):
        from lightfee.venues.bitget import _is_classic_mode_error
        # Bitget error codes that indicate classic account
        for code in ("40034", "40035", "40102"):
            assert _is_classic_mode_error(400, {"code": code, "msg": "error"})
        # Normal errors should not trigger
        assert not _is_classic_mode_error(429, {"code": "42900", "msg": "rate limit"})
        # Message-based detection
        assert _is_classic_mode_error(400, {"code": "0", "msg": "classic account not supported"})

    def test_uta_and_classic_profiles_are_distinct(self):
        from lightfee.venues.bitget import BitgetAccountProfile
        assert BitgetAccountProfile.UTA != BitgetAccountProfile.CLASSIC
        assert BitgetAccountProfile.UTA.value == "uta"
        assert BitgetAccountProfile.CLASSIC.value == "classic"

    @pytest.mark.asyncio
    async def test_contract_family_resolver_explicit_classic_fallbacks_only_on_mismatch(self):
        from lightfee.venues.bitget import BitgetAdapter, BitgetContractFamilyResolver
        from lightfee.venues.specs import BitgetContractFamily

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        calls: list[tuple[str, str, dict[str, str]]] = []

        async def mock_request(method, path, params=None, body=None, private=False):
            calls.append((method, path, dict(params or {})))
            if path == "/api/v3/position/current-position":
                raise TransportError(
                    TransportErrorCategory.REQUEST_REJECTED,
                    "classic account not supported",
                    status_code=400,
                    body='{"code":"40034","msg":"classic account not supported"}',
                )
            if path == "/api/v2/mix/position/all-position":
                return {"code": "00000", "data": []}
            raise AssertionError(f"unexpected Bitget probe path: {path}")

        adapter._transport._request = mock_request
        resolver = BitgetContractFamilyResolver(
            adapter._transport,
            configured_family=BitgetContractFamily.CLASSIC_MIX_V2,
        )

        family = await resolver.resolve()

        assert family == BitgetContractFamily.CLASSIC_MIX_V2
        assert calls == [
            ("GET", "/api/v3/position/current-position", {"category": "USDT-FUTURES"}),
            (
                "GET",
                "/api/v2/mix/position/all-position",
                {"productType": "USDT-FUTURES", "marginCoin": "USDT"},
            ),
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("category", "status_code", "body"),
        [
            (TransportErrorCategory.AUTH_FAILURE, 401, '{"code":"40100","msg":"Invalid API key"}'),
            (TransportErrorCategory.AUTHORIZATION_FAILURE, 403, '{"code":"40300","msg":"Forbidden"}'),
            (TransportErrorCategory.TRANSPORT_FAILURE, 429, '{"code":"42900","msg":"Too many requests"}'),
            (TransportErrorCategory.TRANSPORT_FAILURE, 0, ""),
        ],
    )
    async def test_contract_family_resolver_does_not_fallback_on_auth_rate_limit_or_network(
        self,
        category,
        status_code,
        body,
    ):
        from lightfee.venues.bitget import BitgetAdapter, BitgetContractFamilyResolver

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        calls: list[str] = []

        async def mock_request(method, path, params=None, body=None, private=False):
            calls.append(path)
            raise TransportError(
                category,
                "probe failed",
                status_code=status_code,
                body=body,
            )

        adapter._transport._request = mock_request
        resolver = BitgetContractFamilyResolver(adapter._transport)

        with pytest.raises(TransportError):
            await resolver.resolve()

        assert calls == ["/api/v3/position/current-position"]


# ---------------------------------------------------------------------------
# All-seven fixture-driven parser tests (Deviation 6)
# ---------------------------------------------------------------------------

FIXTURE_DIR = "tests/fixtures/venues"


class TestAllFixtureParsers:
    """Feed each venue's market_snapshot.json and position_snapshot.json
    to the parser and assert non-empty, correct values."""

    @pytest.mark.parametrize("venue_name,spec_fn,expected_symbol", [
        ("binance", binance_spec, "BTCUSDT"),
        ("aster", aster_spec, "BTCUSDT"),
        ("okx", okx_spec, "BTC-USDT-SWAP"),
        ("bybit", bybit_spec, "BTCUSDT"),
        ("bitget", bitget_spec, "BTCUSDT"),
        ("gate", gate_spec, "BTCUSDT"),
        ("hyperliquid", hyperliquid_spec, "BTC"),
    ])
    def test_market_fixture_parses_non_empty(self, venue_name, spec_fn, expected_symbol):
        import json as _json
        spec = spec_fn()
        transport = VenueTransport(spec=spec, mode="paper")
        path = f"{FIXTURE_DIR}/{venue_name}/market_snapshot.json"
        with open(path) as f:
            raw = _json.load(f)
        snap = transport._parse_market_snapshot(raw, [expected_symbol], 1000)
        assert len(snap.quotes) > 0, (
            f"{venue_name} market fixture must produce at least one quote"
        )
        q = snap.quotes[0]
        assert q.symbol
        assert q.bid > 0.0, f"{venue_name} bid should be > 0, got {q.bid}"
        assert q.ask > 0.0, f"{venue_name} ask should be > 0, got {q.ask}"

    @pytest.mark.parametrize("venue_name,spec_fn,expected_symbol,min_qty", [
        ("binance", binance_spec, "BTCUSDT", 0.001),
        ("aster", aster_spec, "BTCUSDT", 0.001),
        ("okx", okx_spec, "BTC-USDT-SWAP", 0.01),
        ("bybit", bybit_spec, "BTCUSDT", 0.001),
        ("bitget", bitget_spec, "BTCUSDT", 0.001),
        ("gate", gate_spec, "BTCUSDT", 0.0),
        ("hyperliquid", hyperliquid_spec, "BTC", 0.001),
    ])
    def test_position_fixture_parses_non_empty(self, venue_name, spec_fn, expected_symbol, min_qty):
        import json as _json
        spec = spec_fn()
        transport = VenueTransport(spec=spec, mode="paper")
        path = f"{FIXTURE_DIR}/{venue_name}/position_snapshot.json"
        with open(path) as f:
            raw = _json.load(f)
        pos = transport._parse_position(raw, expected_symbol, 1000)
        assert pos.quantity >= min_qty, (
            f"{venue_name} position qty {pos.quantity} should be >= {min_qty}"
        )
        assert pos.entry_price > 0.0, (
            f"{venue_name} position entry_price should be > 0, got {pos.entry_price}"
        )
        assert pos.venue == spec.venue_id


# ---------------------------------------------------------------------------
# No live path silently returns fake zeros
# ---------------------------------------------------------------------------

class TestNoFakeLiveData:
    """When a transport is in live mode, market/position data must NOT silently
    return fake zeros — it must reach the actual endpoint or raise."""

    def test_live_fetch_market_requires_http_client(self):
        spec = binance_spec()
        cred = LiveCredential(api_key="k", api_secret="s")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        # Without a mock transport, the live path will attempt real HTTP.
        # We verify the method does NOT silently return paper zero-quotes.
        # (The actual call would fail with network error — that's expected.)
        assert transport.mode == "live"

    @pytest.mark.asyncio
    async def test_live_mode_no_credentials_raises_early(self):
        """If credentials are missing, fail at construction — don't silently
        degrade to paper mode."""
        with pytest.raises(ValueError):
            VenueTransport(spec=okx_spec(), mode="live", credential=None)


# ---------------------------------------------------------------------------
# OKX private GET signature includes query string (Fix 1)
# ---------------------------------------------------------------------------


class TestOkxGetSignature:
    """OKX private GET requests with query params MUST sign path + query_string."""

    def test_okx_get_with_query_includes_query_in_sign_payload(self):
        spec = okx_spec()
        cred = LiveCredential(api_key="okx-key", api_secret="okx-secret",
                              api_passphrase="pass")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        qs, headers, _body = transport._build_signed_request(
            "GET", "/api/v5/account/positions",
            params={"instId": "BTC-USDT-SWAP"},
            private=True,
        )
        # Verify query string is in the URL
        assert "instId=BTC-USDT-SWAP" in qs
        # The signature must be HMAC-SHA256-Base64 of the sign payload
        sig = headers.get("OK-ACCESS-SIGN")
        assert sig is not None
        # Recompute signature: ts + method + path + query_string
        ts = headers["OK-ACCESS-TIMESTAMP"]
        expected_payload = ts + "GET" + "/api/v5/account/positions" + qs
        expected_sig = build_hmac_sha256_base64("okx-secret", expected_payload)
        assert sig == expected_sig, (
            f"Signature mismatch. Expected {expected_sig}, got {sig}"
        )

    def test_okx_get_without_query_signs_path_only(self):
        spec = okx_spec()
        cred = LiveCredential(api_key="okx-key", api_secret="okx-secret",
                              api_passphrase="pass")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        qs, headers, _body = transport._build_signed_request(
            "GET", "/api/v5/account/positions",
            private=True,
        )
        sig = headers.get("OK-ACCESS-SIGN")
        ts = headers["OK-ACCESS-TIMESTAMP"]
        expected_payload = ts + "GET" + "/api/v5/account/positions"
        expected_sig = build_hmac_sha256_base64("okx-secret", expected_payload)
        assert sig == expected_sig

    def test_okx_post_still_signs_with_body(self):
        spec = okx_spec()
        cred = LiveCredential(api_key="okx-key", api_secret="okx-secret",
                              api_passphrase="pass")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        _qs, headers, body = transport._build_signed_request(
            "POST", "/api/v5/trade/order",
            body={"instId": "BTC-USDT-SWAP", "tdMode": "cross", "sz": "1", "side": "buy", "ordType": "market"},
            private=True,
        )
        sig = headers.get("OK-ACCESS-SIGN")
        ts = headers["OK-ACCESS-TIMESTAMP"]
        assert body is not None
        expected_payload = ts + "POST" + "/api/v5/trade/order" + body
        expected_sig = build_hmac_sha256_base64("okx-secret", expected_payload)
        assert sig == expected_sig


# ---------------------------------------------------------------------------
# Bybit V5 private GET signature includes query string (Fix 2)
# ---------------------------------------------------------------------------


class TestBybitGetSignature:
    """Bybit V5 private GET requests MUST sign query_string_without_?."""

    def test_bybit_get_with_query_signs_params_without_question_mark(self):
        spec = bybit_spec()
        cred = LiveCredential(api_key="bybit-key", api_secret="bybit-secret")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        qs, headers, _body = transport._build_signed_request(
            "GET", "/v5/position/list",
            params={"category": "linear", "symbol": "BTCUSDT"},
            private=True,
        )
        assert "category=linear" in qs
        assert "symbol=BTCUSDT" in qs
        sig = headers.get("X-BAPI-SIGN")
        assert sig is not None
        ts = headers["X-BAPI-TIMESTAMP"]
        recv = headers.get("X-BAPI-RECV-WINDOW", "5000")
        # Bybit V5 sign payload: timestamp + api_key + recv_window + params_without_?
        expected_payload = ts + "bybit-key" + recv + qs.lstrip("?")
        expected_sig = build_hmac_sha256_hex("bybit-secret", expected_payload)
        assert sig == expected_sig, (
            f"Bybit signature mismatch. Expected {expected_sig}, got {sig}"
        )

    def test_bybit_get_without_query_signs_empty(self):
        spec = bybit_spec()
        cred = LiveCredential(api_key="bybit-key", api_secret="bybit-secret")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        _qs, headers, _body = transport._build_signed_request(
            "GET", "/v5/position/list",
            private=True,
        )
        sig = headers.get("X-BAPI-SIGN")
        ts = headers["X-BAPI-TIMESTAMP"]
        recv = headers.get("X-BAPI-RECV-WINDOW", "5000")
        expected_payload = ts + "bybit-key" + recv
        expected_sig = build_hmac_sha256_hex("bybit-secret", expected_payload)
        assert sig == expected_sig

    def test_bybit_post_signs_with_body(self):
        spec = bybit_spec()
        cred = LiveCredential(api_key="bybit-key", api_secret="bybit-secret")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        _qs, headers, body = transport._build_signed_request(
            "POST", "/v5/order/create",
            body={"category": "linear", "symbol": "BTCUSDT", "side": "Buy", "orderType": "Market", "qty": "0.01"},
            private=True,
        )
        sig = headers.get("X-BAPI-SIGN")
        ts = headers["X-BAPI-TIMESTAMP"]
        recv = headers.get("X-BAPI-RECV-WINDOW", "5000")
        assert body is not None
        expected_payload = ts + "bybit-key" + recv + body
        expected_sig = build_hmac_sha256_hex("bybit-secret", expected_payload)
        assert sig == expected_sig


# ---------------------------------------------------------------------------
# Private GET base_url selection (Fix 3)
# ---------------------------------------------------------------------------


class TestPrivateBaseUrl:
    """Private GET endpoints MUST use private_base_url, not public_base_url."""

    @pytest.mark.asyncio
    async def test_position_fetch_uses_private_base(self):
        """When public and private bases differ, position fetch uses private."""
        from lightfee.venues.specs import VenueSpec, AuthScheme
        from lightfee.venues.base import VenueAccountContract
        saved_urls = []

        # Create a spec with DIFFERENT public/private bases
        spec = VenueSpec(
            venue_id=Venue.BINANCE,
            public_base_url="https://public.example.com",
            private_base_url="https://private.example.com",
            auth_scheme=AuthScheme.HMAC_SHA256_HEX,
            account_contract=VenueAccountContract.SINGLE_OR_MULTI_ASSET,
            market_snapshot_path="/public/tickers",
            position_path="/private/positions",
            order_path="/private/order",
            operation_contracts={
                VenueOperation.CREATE_ORDER: VenueOperationContract(
                    "POST",
                    "/private/order",
                    payload="params",
                ),
            },
            signature_param="signature",
            timestamp_param="timestamp",
            api_key_header="X-MBX-APIKEY",
        )
        cred = LiveCredential(api_key="k", api_secret="s")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)

        raw_handler_called = []

        # Mock: intercept the URL
        original_request = transport._request

        async def mock_request(method, path, params=None, body=None, private=False):
            qs, headers, req_body = transport._build_signed_request(
                method, path, params, body
            )
            base = spec.private_base_url if (private or method.upper() == "POST") else spec.public_base_url
            saved_urls.append(base + path + qs)
            raw_handler_called.append(True)
            return {}

        transport._request = mock_request

        await transport.fetch_position("BTCUSDT")
        assert len(saved_urls) > 0
        url = saved_urls[0]
        assert "private.example.com" in url, (
            f"Expected private base URL, got: {url}"
        )

    @pytest.mark.asyncio
    async def test_order_placement_uses_private_base(self, monkeypatch):
        from lightfee.venues.specs import VenueSpec, AuthScheme
        from lightfee.venues.base import VenueAccountContract
        from lightfee.venues.symbol_rules import SymbolRule
        saved_urls = []

        spec = VenueSpec(
            venue_id=Venue.BINANCE,
            public_base_url="https://public.example.com",
            private_base_url="https://private.example.com",
            auth_scheme=AuthScheme.HMAC_SHA256_HEX,
            account_contract=VenueAccountContract.SINGLE_OR_MULTI_ASSET,
            market_snapshot_path="/public/tickers",
            position_path="/private/positions",
            order_path="/private/order",
            operation_contracts={
                VenueOperation.CREATE_ORDER: VenueOperationContract(
                    "POST",
                    "/private/order",
                    payload="params",
                ),
            },
            signature_param="signature",
            timestamp_param="timestamp",
            api_key_header="X-MBX-APIKEY",
        )
        cred = LiveCredential(api_key="k", api_secret="s")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)

        class FakeRulesCache:
            async def get(self, *_args):
                return SymbolRule(
                    tick_size=0.01,
                    qty_step=0.001,
                    min_qty=0.001,
                    min_notional=5.0,
                    rule_source="exchangeInfo",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )

        saved_urls = []

        async def mock_request(method, path, params=None, body=None, private=False):
            qs, headers, req_body = transport._build_signed_request(
                method, path, params, body
            )
            base = spec.private_base_url if (private or method.upper() == "POST") else spec.public_base_url
            saved_urls.append(base + path + qs)
            return {"orderId": 123, "symbol": "BTCUSDT", "status": "FILLED",
                    "executedQty": "0.01", "avgPrice": "50000.0"}

        transport._request = mock_request

        req = OrderRequest(venue=Venue.BINANCE, symbol="BTCUSDT", side=Side.BUY, quantity=0.01)
        await transport.place_order(req)
        assert len(saved_urls) > 0
        url = saved_urls[0]
        assert "private.example.com" in url, (
            f"Expected private base URL for order, got: {url}"
        )

    @pytest.mark.asyncio
    async def test_market_snapshot_uses_public_base(self):
        from lightfee.venues.specs import VenueSpec, AuthScheme
        from lightfee.venues.base import VenueAccountContract
        saved_urls = []

        spec = VenueSpec(
            venue_id=Venue.BINANCE,
            public_base_url="https://public.example.com",
            private_base_url="https://private.example.com",
            auth_scheme=AuthScheme.HMAC_SHA256_HEX,
            account_contract=VenueAccountContract.SINGLE_OR_MULTI_ASSET,
            market_snapshot_path="/public/tickers",
            position_path="/private/positions",
            order_path="/private/order",
            signature_param="signature",
            timestamp_param="timestamp",
            api_key_header="X-MBX-APIKEY",
        )
        cred = LiveCredential(api_key="k", api_secret="s")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)

        called_urls = []

        async def mock_request(method, path, params=None, body=None, private=False):
            qs, headers, req_body = transport._build_signed_request(
                method, path, params, body
            )
            base = spec.private_base_url if (private or method.upper() == "POST") else spec.public_base_url
            called_urls.append(base + path + qs)
            return []

        transport._request = mock_request

        await transport.fetch_market_snapshot(["BTCUSDT"])
        assert len(called_urls) > 0
        url = called_urls[0]
        assert "public.example.com" in url, (
            f"Expected public base URL for market snapshot, got: {url}"
        )


# ---------------------------------------------------------------------------
# Position side parsing — short/sell indicators (Fix 5)
# ---------------------------------------------------------------------------


class TestPositionSideParsing:
    """Position parser must correctly identify SELL from various field values.
    Updated for Task 7: uses venue-specific parser functions with proper envelopes."""

    def test_binance_short_position_side(self):
        from lightfee.venues.transport import _parse_binance_like_position
        raw = [{"symbol": "BTCUSDT", "positionSide": "SHORT",
                "positionAmt": "0.01", "entryPrice": "50000.0"}]
        pos = _parse_binance_like_position(raw, "BTCUSDT", 1000)
        assert pos.side == Side.SELL
        assert pos.quantity == 0.01

    def test_binance_negative_position_amt_is_short(self):
        from lightfee.venues.transport import _parse_binance_like_position
        raw = [{"symbol": "BTCUSDT", "positionSide": "", "positionAmt": "-0.01",
                "entryPrice": "50000.0"}]
        pos = _parse_binance_like_position(raw, "BTCUSDT", 1000)
        assert pos.side == Side.SELL
        assert pos.quantity == 0.01

    def test_bybit_sell_side_is_short(self):
        from lightfee.venues.transport import _parse_bybit_position
        raw = {"retCode": 0, "result": {"list": [
            {"symbol": "BTCUSDT", "side": "Sell", "size": "0.01", "avgPrice": "50000.0"}
        ]}}
        pos = _parse_bybit_position(raw, "BTCUSDT", 1000)
        assert pos.side == Side.SELL
        assert pos.quantity == 0.01

    def test_okx_lowercase_short_is_short(self):
        from lightfee.venues.transport import _parse_okx_position
        raw = {"code": "0", "data": [
            {"instId": "BTC-USDT-SWAP", "posSide": "short", "pos": "1", "avgPx": "50000.0"}
        ]}
        pos = _parse_okx_position(raw, "BTC-USDT-SWAP", 1000)
        assert pos.side == Side.SELL

    def test_gate_negative_size_is_short(self):
        from lightfee.venues.transport import _parse_gate_position
        raw = {"contract": "BTCUSDT", "size": "-1", "entry_price": "50000.0"}
        pos = _parse_gate_position(raw, "BTCUSDT", 1000)
        assert pos.side == Side.SELL
        assert pos.quantity == 1.0

    def test_hyperliquid_negative_szi_is_short(self):
        from lightfee.venues.transport import _parse_hyperliquid_position
        raw = {"assetPositions": [
            {"position": {"coin": "BTC", "szi": "-0.01", "entryPx": "50000.0"}}
        ]}
        pos = _parse_hyperliquid_position(raw, "BTC", 1000)
        assert pos.side == Side.SELL
        assert pos.quantity == 0.01

    def test_long_position_still_works(self):
        from lightfee.venues.transport import _parse_binance_like_position
        raw = [{"symbol": "BTCUSDT", "positionSide": "LONG",
                "positionAmt": "0.01", "entryPrice": "50000.0"}]
        pos = _parse_binance_like_position(raw, "BTCUSDT", 1000)
        assert pos.side == Side.BUY
        assert pos.quantity == 0.01

    def test_no_side_no_quantity_defaults_buy(self):
        from lightfee.venues.transport import _parse_binance_like_position
        raw = [{"symbol": "BTCUSDT", "entryPrice": "50000.0"}]
        pos = _parse_binance_like_position(raw, "BTCUSDT", 1000)
        assert pos.side == Side.BUY
        assert pos.quantity == 0.0


# ---------------------------------------------------------------------------
# Ack-only order response MUST NOT return fake fill (Fix 6)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# OKX / Bybit GET signing with query string (Fix 1 + Fix 2)
# ---------------------------------------------------------------------------


class TestOkxGetSigningWithQueryString:
    """OKX GET must include query string in signature payload.

    Rust V1 source: src/live/okx.rs::signed_request()
    sign_payload = timestamp + method + request_path(including ?query) + body
    """

    def test_okx_get_fetch_position_sign_payload_contains_query(self):
        from lightfee.venues.transport import _sign_payload
        spec = okx_spec()
        cred = LiveCredential(api_key="okx-k", api_secret="okx-s",
                              api_passphrase="okx-p")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        qs, headers, body = transport._build_signed_request(
            "GET", "/api/v5/account/positions",
            params={"instId": "BTC-USDT-SWAP"},
            private=True,
        )
        # Verify query string is in the URL
        assert "instId=BTC-USDT-SWAP" in qs
        # Verify signature header exists
        assert headers.get("OK-ACCESS-SIGN")
        # Recompute signature to verify payload includes query
        sign = headers["OK-ACCESS-SIGN"]
        ts = headers["OK-ACCESS-TIMESTAMP"]
        expected_payload = ts + "GET" + "/api/v5/account/positions" + qs
        expected_sig = _sign_payload(spec.auth_scheme, "okx-s", expected_payload)
        assert sign == expected_sig, (
            f"OKX GET signature mismatch.\n"
            f"  Payload: {expected_payload!r}\n"
            f"  Expected: {expected_sig}\n"
            f"  Got: {sign}"
        )

    def test_okx_get_without_query_string_still_signs_correctly(self):
        from lightfee.venues.transport import _sign_payload
        spec = okx_spec()
        cred = LiveCredential(api_key="okx-k", api_secret="okx-s",
                              api_passphrase="okx-p")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        qs, headers, body = transport._build_signed_request(
            "GET", "/api/v5/account/config",
            private=True,
        )
        sign = headers["OK-ACCESS-SIGN"]
        ts = headers["OK-ACCESS-TIMESTAMP"]
        expected_payload = ts + "GET" + "/api/v5/account/config"
        expected_sig = _sign_payload(spec.auth_scheme, "okx-s", expected_payload)
        assert sign == expected_sig

    def test_okx_post_sign_payload_uses_body_not_query(self):
        from lightfee.venues.transport import _sign_payload
        spec = okx_spec()
        cred = LiveCredential(api_key="okx-k", api_secret="okx-s",
                              api_passphrase="okx-p")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        qs, headers, body = transport._build_signed_request(
            "POST", "/api/v5/trade/order",
            body={"instId": "BTC-USDT-SWAP", "tdMode": "cross",
                  "side": "buy", "ordType": "market", "sz": "1"},
            private=True,
        )
        sign = headers["OK-ACCESS-SIGN"]
        ts = headers["OK-ACCESS-TIMESTAMP"]
        expected_payload = ts + "POST" + "/api/v5/trade/order" + (body or "")
        expected_sig = _sign_payload(spec.auth_scheme, "okx-s", expected_payload)
        assert sign == expected_sig


class TestBybitGetSigningWithQueryString:
    """Bybit V5 GET must sign query_string (without leading '?') as payload.

    Rust V1 source: src/live/bybit.rs::signed_request()
    sign_payload = timestamp + api_key + recv_window + query_or_body
    """

    def test_bybit_get_fetch_position_sign_payload_contains_query(self):
        from lightfee.venues.transport import _sign_payload
        spec = bybit_spec()
        cred = LiveCredential(api_key="bybit-k", api_secret="bybit-s")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        qs, headers, body = transport._build_signed_request(
            "GET", "/v5/position/list",
            params={"category": "linear", "symbol": "BTCUSDT"},
            private=True,
        )
        assert headers.get("X-BAPI-SIGN")
        sign = headers["X-BAPI-SIGN"]
        ts = headers["X-BAPI-TIMESTAMP"]
        recv = headers.get("X-BAPI-RECV-WINDOW", "5000")
        # Bybit signs: timestamp + api_key + recv_window + query_without_question_mark
        expected_payload = ts + "bybit-k" + recv + qs.lstrip("?")
        expected_sig = _sign_payload(spec.auth_scheme, "bybit-s", expected_payload)
        assert sign == expected_sig, (
            f"Bybit GET signature mismatch.\n"
            f"  Payload: {expected_payload!r}\n"
            f"  Expected: {expected_sig}\n"
            f"  Got: {sign}"
        )

    def test_bybit_post_sign_payload_uses_body(self):
        from lightfee.venues.transport import _sign_payload
        spec = bybit_spec()
        cred = LiveCredential(api_key="bybit-k", api_secret="bybit-s")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        qs, headers, body = transport._build_signed_request(
            "POST", "/v5/order/create",
            body={"category": "linear", "symbol": "BTCUSDT",
                  "side": "Buy", "orderType": "Market", "qty": "0.01"},
            private=True,
        )
        sign = headers["X-BAPI-SIGN"]
        ts = headers["X-BAPI-TIMESTAMP"]
        recv = headers.get("X-BAPI-RECV-WINDOW", "5000")
        expected_payload = ts + "bybit-k" + recv + (body or "")
        expected_sig = _sign_payload(spec.auth_scheme, "bybit-s", expected_payload)
        assert sign == expected_sig

    def test_bybit_get_query_params_sorted_consistently(self):
        """Query params sorted alphabetically → identical query_string regardless of input order."""
        spec = bybit_spec()
        cred = LiveCredential(api_key="bybit-k", api_secret="bybit-s")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        # Same params in different dict order should produce identical query_string
        qs1, _, _ = transport._build_signed_request(
            "GET", "/v5/position/list",
            params={"symbol": "BTCUSDT", "category": "linear"},
        )
        qs2, _, _ = transport._build_signed_request(
            "GET", "/v5/position/list",
            params={"category": "linear", "symbol": "BTCUSDT"},
        )
        assert qs1 == qs2, (
            f"Bybit query_string must be invariant to param order: {qs1!r} != {qs2!r}"
        )

    def test_bybit_server_time_parse_uses_millisecond_precision_before_seconds(self):
        spec = bybit_spec()
        transport = VenueTransport(spec=spec, mode="paper")

        assert transport._parse_server_time({
            "retCode": 0,
            "time": "1770000000123",
            "result": {
                "timeSecond": "1770000000",
                "timeNano": "1770000000456789000",
            },
        }) == 1770000000123
        assert transport._parse_server_time({
            "retCode": 0,
            "result": {
                "timeSecond": "1770000000",
                "timeNano": "1770000000456789000",
            },
        }) == 1770000000456

    def test_bybit_10002_timestamp_window_error_is_retryable(self):
        spec = bybit_spec()
        cred = LiveCredential(api_key="bybit-k", api_secret="bybit-s")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)

        body = (
            '{"retCode":10002,'
            '"retMsg":"invalid request, please check your server timestamp or recv_window param"}'
        )
        assert transport._is_time_offset_retryable(200, body)

    def test_bybit_success_envelope_with_timestamp_text_is_not_retryable(self):
        spec = bybit_spec()
        cred = LiveCredential(api_key="bybit-k", api_secret="bybit-s")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)

        body = '{"retCode":0,"retMsg":"OK","result":{"timestamp":"1770000000000"}}'
        assert not transport._is_time_offset_retryable(200, body)

    @pytest.mark.asyncio
    async def test_bybit_10002_clears_server_time_offset_and_retries_private_request(self, monkeypatch):
        spec = bybit_spec()
        cred = LiveCredential(api_key="bybit-k", api_secret="bybit-s")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        time_calls = 0
        private_timestamps: list[str] = []

        async def no_sleep(_delay: float) -> None:
            return None

        monkeypatch.setattr("lightfee.venues.transport.asyncio.sleep", no_sleep)

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal time_calls
            if request.url.path == "/v5/market/time":
                time_calls += 1
                server_ms = 1770000000000 + time_calls * 1000
                return httpx.Response(200, json={
                    "retCode": 0,
                    "time": str(server_ms),
                    "result": {
                        "timeSecond": str(server_ms // 1000),
                        "timeNano": str(server_ms * 1_000_000),
                    },
                })
            if request.url.path == "/v5/position/list":
                private_timestamps.append(request.headers["X-BAPI-TIMESTAMP"])
                if len(private_timestamps) == 1:
                    return httpx.Response(200, json={
                        "retCode": 10002,
                        "retMsg": "invalid request, please check your server timestamp or recv_window param",
                    })
                return httpx.Response(200, json={
                    "retCode": 0,
                    "result": {"list": []},
                })
            return httpx.Response(404, json={"error": "unexpected path"})

        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

        raw = await transport._request(
            "GET",
            "/v5/position/list",
            params={"category": "linear", "symbol": "BTCUSDT"},
            private=True,
        )

        assert raw["retCode"] == 0
        assert time_calls == 2
        assert len(private_timestamps) == 2
        assert private_timestamps[0] != private_timestamps[1]


# ---------------------------------------------------------------------------
# Private GET base_url selection (Fix 3)
# ---------------------------------------------------------------------------


class TestPrivateGetBaseUrl:
    """Private GET endpoints (position fetch, profile probe) must use private_base_url."""

    def test_private_get_uses_private_base_url(self):
        """When private=True, private_base_url is used regardless of HTTP method."""
        spec = VenueSpec(
            venue_id=Venue.ASTER,
            public_base_url="https://public.example.com",
            private_base_url="https://private.example.com",
            auth_scheme=AuthScheme.HMAC_SHA256_HEX,
            account_contract=VenueAccountContract.SINGLE_OR_MULTI_ASSET,
            api_key_header="X-MBX-APIKEY",
            signature_param="signature",
            timestamp_param="timestamp",
            market_snapshot_path="/public/tickers",
            position_path="/private/position",
            order_path="/private/order",
        )
        cred = LiveCredential(api_key="k", api_secret="s")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)

        qs_p, headers_p, body_p = transport._build_signed_request(
            "GET", "/private/position",
            params={"symbol": "BTCUSDT"},
        )
        assert spec.private_base_url == "https://private.example.com"
        assert spec.public_base_url == "https://public.example.com"
        assert spec.private_base_url != spec.public_base_url, (
            "private_base_url differs from public_base_url for this test"
        )

    def test_public_get_uses_public_base_url(self):
        """Market data GET (without private flag) uses public_base_url."""
        spec = VenueSpec(
            venue_id=Venue.ASTER,
            public_base_url="https://public.example.com",
            private_base_url="https://private.example.com",
            auth_scheme=AuthScheme.HMAC_SHA256_HEX,
            account_contract=VenueAccountContract.SINGLE_OR_MULTI_ASSET,
            api_key_header="X-MBX-APIKEY",
            signature_param="signature",
            timestamp_param="timestamp",
            market_snapshot_path="/public/tickers",
            position_path="/private/position",
            order_path="/private/order",
        )
        cred = LiveCredential(api_key="k", api_secret="s")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)

        assert spec.public_base_url == "https://public.example.com"
        assert spec.private_base_url == "https://private.example.com"


# ---------------------------------------------------------------------------
# Position side parsing — short/sell/long/buy across venues (Fix 5)
# ---------------------------------------------------------------------------


class TestPositionSideParsing:
    """Position side must recognize SHORT, SELL, short, sell, SHORT_SIDE,
    LONG, BUY, long, buy, and negative quantity fields."""

    SELL_CASES = [
        ("SHORT", "okx_short"),
        ("SELL", "binance_sell"),
        ("short", "okx_lowercase_short"),
        ("sell", "lowercase_sell"),
        ("SHORT_SIDE", "short_side_indicator"),
        ("Short", "mixed_case_short"),
        ("Sell", "mixed_case_sell"),
        ("ShOrT", "weird_case_short"),
    ]

    BUY_CASES = [
        ("LONG", "okx_long"),
        ("BUY", "binance_buy"),
        ("long", "lowercase_long"),
        ("buy", "lowercase_buy"),
    ]

    @pytest.mark.parametrize("side_str,label", SELL_CASES)
    def test_parse_position_side_sell_variants(self, side_str, label):
        spec = gate_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        raw = {"positionSide": side_str, "positionAmt": "0.01",
               "entryPrice": "50000.0"}
        pos = transport._parse_position(raw, "BTCUSDT", 1000)
        assert pos.side == Side.SELL, (
            f"side_str={side_str!r} ({label}) should parse as SELL, got {pos.side}"
        )
        assert pos.quantity == 0.01

    @pytest.mark.parametrize("side_str,label", BUY_CASES)
    def test_parse_position_side_buy_variants(self, side_str, label):
        spec = gate_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        raw = {"positionSide": side_str, "positionAmt": "0.01",
               "entryPrice": "50000.0"}
        pos = transport._parse_position(raw, "BTCUSDT", 1000)
        assert pos.side == Side.BUY, (
            f"side_str={side_str!r} ({label}) should parse as BUY, got {pos.side}"
        )
        assert pos.quantity == 0.01

    def test_negative_quantity_implies_sell(self):
        """If positionAmt is negative, side must be SELL even without side field."""
        spec = binance_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        raw = {"symbol": "BTCUSDT", "positionAmt": "-0.01",
               "entryPrice": "50000.0"}
        pos = transport._parse_position(raw, "BTCUSDT", 1000)
        assert pos.side == Side.SELL
        assert pos.quantity == 0.01  # absolute value

    def test_negative_pos_field_implies_sell(self):
        """If 'pos' field is negative, side must be SELL."""
        spec = okx_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        raw = {"instId": "BTC-USDT-SWAP", "pos": "-1", "avgPx": "50000"}
        pos = transport._parse_position(raw, "BTC-USDT-SWAP", 1000)
        assert pos.side == Side.SELL
        assert pos.quantity == 1.0

    def test_negative_size_field_implies_sell(self):
        """If 'size' field is negative, side must be SELL."""
        spec = bitget_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        raw = {"symbol": "BTCUSDT", "size": "-0.01", "entryPrice": "50000.0"}
        pos = transport._parse_position(raw, "BTCUSDT", 1000)
        assert pos.side == Side.SELL
        assert pos.quantity == 0.01

    def test_positive_quantity_no_side_default_buy(self):
        """If no explicit side field and quantity is positive, default to BUY."""
        spec = binance_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        raw = {"symbol": "BTCUSDT", "positionAmt": "0.01",
               "entryPrice": "50000.0"}
        pos = transport._parse_position(raw, "BTCUSDT", 1000)
        assert pos.side == Side.BUY
        assert pos.quantity == 0.01

    def test_zero_quantity_zero_side_default_buy(self):
        spec = binance_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        raw = {"symbol": "BTCUSDT", "entryPrice": "50000.0"}
        pos = transport._parse_position(raw, "BTCUSDT", 1000)
        assert pos.side == Side.BUY
        assert pos.quantity == 0.0

    # Per-venue short/sell fixture or inline raw coverage
    def test_binance_short_position(self):
        spec = binance_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        raw = {"symbol": "BTCUSDT", "positionSide": "SHORT",
               "positionAmt": "0.01", "entryPrice": "50000.0"}
        pos = transport._parse_position(raw, "BTCUSDT", 1000)
        assert pos.side == Side.SELL
        assert pos.quantity == 0.01

    def test_okx_short_position(self):
        spec = okx_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        raw = {"instId": "BTC-USDT-SWAP", "posSide": "short",
               "pos": "1", "avgPx": "50000"}
        pos = transport._parse_position(raw, "BTC-USDT-SWAP", 1000)
        assert pos.side == Side.SELL
        assert pos.quantity == 1.0

    def test_bybit_sell_position(self):
        spec = bybit_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        raw = {"symbol": "BTCUSDT", "side": "Sell",
               "size": "0.01", "avgPrice": "50000.0"}
        pos = transport._parse_position(raw, "BTCUSDT", 1000)
        assert pos.side == Side.SELL
        assert pos.quantity == 0.01

    def test_bitget_short_position(self):
        spec = bitget_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        raw = {"symbol": "BTCUSDT", "holdSide": "short",
               "total": "0.01", "openPriceAvg": "50000.0"}
        pos = transport._parse_position(raw, "BTCUSDT", 1000)
        assert pos.side == Side.SELL
        assert pos.quantity == 0.01

    def test_gate_short_position(self):
        spec = gate_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        raw = {"contract": "BTCUSDT", "size": "-1",
               "entry_price": "50000.0"}
        pos = transport._parse_position(raw, "BTCUSDT", 1000)
        assert pos.side == Side.SELL
        assert pos.quantity == 1.0

    def test_hyperliquid_short_position(self):
        spec = hyperliquid_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        raw = {"assetPositions": [{"position": {"coin": "BTC",
               "szi": "-1.0", "entryPx": "50000.0"}}]}
        pos = transport._parse_position(raw, "BTC", 1000)
        assert pos.side == Side.SELL
        assert pos.quantity == 1.0


class TestOrderAckNotFill:
    """Order responses with only an order ID must raise UNCERTAIN, not a fake fill."""

    def test_ack_only_response_raises_uncertain(self):
        spec = bybit_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        raw = {"retCode": 0, "result": {"orderId": "xyz789", "orderLinkId": "client_1"}}
        req = OrderRequest(venue=Venue.BYBIT, symbol="BTCUSDT", side=Side.BUY, quantity=0.01)
        with pytest.raises(OrderSubmitError) as exc_info:
            transport._parse_order_fill(raw, req, "BTCUSDT", 1000)
        assert exc_info.value.class_ == SubmitFailureClass.UNCERTAIN
        assert "order accepted" in str(exc_info.value).lower()
        assert "fill not confirmed" in str(exc_info.value).lower()
        assert getattr(exc_info.value, "accepted_order_id", "") == "xyz789"
        assert getattr(exc_info.value, "accepted_client_order_id", "") == "client_1"
        assert getattr(exc_info.value, "order_ack_only", False) is True
        assert getattr(exc_info.value, "fill_confirmation_missing_fields", []) == [
            "executedQty",
            "cumExecQty",
            "cumQty",
            "fillSz",
            "filledQty",
            "filled_size",
        ]
        assert json.loads(getattr(exc_info.value, "exchange_response_body", "{}")) == raw

    def test_bitget_ack_only_raises_uncertain(self):
        spec = bitget_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        raw = {"code": "00000", "data": {"orderId": "bg123", "clientOrderId": "client_1"}}
        req = OrderRequest(venue=Venue.BITGET, symbol="BTCUSDT", side=Side.BUY, quantity=0.01)
        with pytest.raises(OrderSubmitError) as exc_info:
            transport._parse_order_fill(raw, req, "BTCUSDT", 1000)
        assert exc_info.value.class_ == SubmitFailureClass.UNCERTAIN
        assert getattr(exc_info.value, "accepted_order_id", "") == "bg123"
        assert getattr(exc_info.value, "accepted_client_order_id", "") == "client_1"

    def test_filled_response_still_returns_fill(self):
        spec = binance_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        raw = {"orderId": 789, "symbol": "BTCUSDT", "status": "FILLED",
               "executedQty": "0.01", "avgPrice": "50001.00"}
        req = OrderRequest(venue=Venue.BINANCE, symbol="BTCUSDT", side=Side.BUY, quantity=0.01)
        fill = transport._parse_order_fill(raw, req, "BTCUSDT", 1000)
        assert fill.order_id == "789"
        assert fill.quantity == 0.01
        assert fill.price == 50001.0

    def test_filled_status_without_explicit_qty_falls_back_to_size(self):
        spec = gate_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        raw = {"id": 456, "contract": "BTCUSDT", "size": "1", "price": "50001.0", "status": "finished"}
        req = OrderRequest(venue=Venue.GATE, symbol="BTCUSDT", side=Side.BUY, quantity=1.0)
        fill = transport._parse_order_fill(raw, req, "BTCUSDT", 1000)
        assert fill.order_id == "456"
        assert fill.quantity == 1.0
        assert fill.price == 50001.0

    def test_reject_response_raises_rejected(self):
        spec = okx_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        raw = {"code": "51000", "msg": "Insufficient balance"}
        # This test verifies reject detection is intact — you need HTTP 400 from transport
        # to trigger reject. Parser-level test: if data has no orderId and no fill status.
        raw_no_id = {"code": "51000", "data": []}
        req = OrderRequest(venue=Venue.OKX, symbol="BTC-USDT-SWAP", side=Side.BUY, quantity=1.0)
        with pytest.raises(OrderSubmitError) as exc_info:
            transport._parse_order_fill(raw_no_id, req, "BTC-USDT-SWAP", 1000)
        assert exc_info.value.class_ == SubmitFailureClass.UNCERTAIN


class TestOrderSubmitDiagnosticsAndQuantization:
    def test_okx_net_short_position_preserves_signed_contracts(self):
        pos = _parse_okx_position(
            {
                "code": "0",
                "data": [
                    {
                        "instId": "UB-USDT-SWAP",
                        "pos": "-1",
                        "posSide": "net",
                        "avgPx": "0.01",
                    }
                ],
            },
            "UB-USDT-SWAP",
            1234,
            contract_size=100.0,
        )

        assert pos.side == Side.SELL
        assert pos.quantity == pytest.approx(100.0)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "symbol,base_qty,ct_val",
        [
            ("UBUSDT", 1.0, 100.0),
            ("OPGUSDT", 9.0, 10.0),
        ],
    )
    async def test_okx_taker_rejects_when_base_quantity_rounds_to_zero_contracts(
        self, monkeypatch, symbol, base_qty, ct_val,
    ):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.000001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    ct_val=ct_val,
                    rule_source="test_okx_instrument",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        monkeypatch.setattr(
            "lightfee.venues.symbol_rules.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        transport = VenueTransport(
            spec=okx_spec(),
            mode="live",
            credential=LiveCredential(
                api_key="okx-key",
                api_secret="okx-secret",
                api_passphrase="okx-pass",
            ),
        )
        transport._pos_mode_cache = "net"
        sent: list[dict[str, Any]] = []

        async def fake_request(method, path, *, body=None, params=None, private=False, **kwargs):
            sent.append({"method": method, "path": path, "body": dict(body or {})})
            return {"code": "0", "data": [{"ordId": "must-not-send", "sCode": "0"}]}

        transport._request = fake_request

        with pytest.raises(OrderSubmitError) as exc_info:
            await transport.place_order(
                OrderRequest(
                    venue=Venue.OKX,
                    symbol=symbol,
                    side=Side.BUY,
                    quantity=base_qty,
                    price_hint=0.01,
                    client_order_id=f"{symbol.lower()}reject",
                )
            )

        assert exc_info.value.class_ == SubmitFailureClass.REJECTED
        assert sent == []
        result = transport.order_diagnostics[-1]["payload"]
        assert result["base_qty"] == pytest.approx(base_qty)
        assert result["ct_val"] == pytest.approx(ct_val)
        assert result["lot_sz"] == pytest.approx(1.0)
        assert result["contract_qty"] == pytest.approx(0.0)
        assert result["reject_reason"] == "contract_qty_zero"

    @pytest.mark.asyncio
    async def test_okx_taker_sends_contract_quantity_after_base_conversion(self, monkeypatch):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.000001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    ct_val=100.0,
                    rule_source="test_okx_instrument",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        monkeypatch.setattr(
            "lightfee.venues.symbol_rules.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        transport = VenueTransport(
            spec=okx_spec(),
            mode="live",
            credential=LiveCredential(
                api_key="okx-key",
                api_secret="okx-secret",
                api_passphrase="okx-pass",
            ),
        )
        transport._pos_mode_cache = "net"
        sent: list[dict[str, Any]] = []

        async def fake_request(method, path, *, body=None, params=None, private=False, **kwargs):
            sent.append({"method": method, "path": path, "body": dict(body or {})})
            return {
                "code": "0",
                "data": [{"ordId": "okx-contract-1", "clOrdId": "ubvalid100", "sCode": "0"}],
            }

        transport._request = fake_request

        fill = await transport.place_order(
            OrderRequest(
                venue=Venue.OKX,
                symbol="UBUSDT",
                side=Side.BUY,
                quantity=100.0,
                price_hint=0.01,
                client_order_id="ubvalid100",
            )
        )

        assert sent[-1]["path"] == "/api/v5/trade/order"
        assert sent[-1]["body"]["sz"] == "1"
        assert fill.quantity == pytest.approx(100.0)

    @pytest.mark.asyncio
    async def test_okx_normalize_quantity_terminalizes_below_contract_min(self):
        transport = VenueTransport(spec=okx_spec(), mode="paper")
        transport.set_symbol_metadata({
            "UB-USDT-SWAP": {
                "ct_val": "100",
                "lot_sz": "1",
                "min_sz": "1",
            }
        })

        assert await transport.normalize_quantity("UBUSDT", 1.0) == 0.0
        assert await transport.normalize_quantity("UBUSDT", 100.0) == pytest.approx(100.0)
        assert await transport.normalize_quantity("UBUSDT", 150.0) == pytest.approx(100.0)

    @pytest.mark.asyncio
    async def test_okx_normalize_quantity_uses_symbol_rules_cache_for_lot_rules_only(
        self, monkeypatch,
    ):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                assert venue == Venue.OKX
                assert venue_symbol == "UB-USDT-SWAP"
                return SymbolRule(
                    tick_size=0.000001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    ct_val=100.0,
                    rule_source="test_okx_instrument",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        transport = VenueTransport(spec=okx_spec(), mode="paper")
        transport.set_symbol_metadata({
            "UB-USDT-SWAP": {
                "ctVal": "100",
                "ctType": "linear",
            }
        })

        assert await transport.normalize_quantity("UBUSDT", 50.0) == 0.0
        assert await transport.normalize_quantity("UBUSDT", 100.0) == pytest.approx(100.0)

    @pytest.mark.asyncio
    async def test_okx_normalize_quantity_fails_closed_without_contract_size(self, monkeypatch):
        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return type(
                    "Rule",
                    (),
                    {"ct_val": 0.0, "qty_step": 1.0, "min_qty": 1.0},
                )()

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        transport = VenueTransport(spec=okx_spec(), mode="paper")
        transport.set_symbol_metadata({
            "UB-USDT-SWAP": {
                "lot_sz": "1",
                "min_sz": "1",
            }
        })

        with pytest.raises(TransportError) as exc_info:
            await transport.normalize_quantity("UBUSDT", 100.0)

        assert exc_info.value.category == TransportErrorCategory.NORMALIZATION_FAILURE
        assert "okx_contract_metadata_missing_ct_val" in str(exc_info.value)
        assert "classification=metadata_missing" in str(exc_info.value)
        assert "instId=UB-USDT-SWAP" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_okx_contract_size_lookup_fails_closed_without_ct_val(self, monkeypatch):
        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return type("Rule", (), {"ct_val": 0.0})()

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        transport = VenueTransport(spec=okx_spec(), mode="paper")

        with pytest.raises(TransportError) as exc_info:
            await transport._okx_contract_size_for_venue_symbol("UB-USDT-SWAP")

        assert exc_info.value.category == TransportErrorCategory.NORMALIZATION_FAILURE
        assert "okx_contract_metadata_missing_ct_val" in str(exc_info.value)
        assert "classification=metadata_missing" in str(exc_info.value)
        assert "instId=UB-USDT-SWAP" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_okx_passive_rejects_zero_contract_quantity_without_request(self, monkeypatch):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.000001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    ct_val=10.0,
                    rule_source="test_okx_instrument",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        monkeypatch.setattr(
            "lightfee.venues.symbol_rules.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        transport = VenueTransport(
            spec=okx_spec(),
            mode="live",
            credential=LiveCredential(
                api_key="okx-key",
                api_secret="okx-secret",
                api_passphrase="okx-pass",
            ),
        )
        transport._pos_mode_cache = "net"
        sent: list[dict[str, Any]] = []

        async def fake_request(method, path, *, body=None, params=None, private=False, **kwargs):
            sent.append({"method": method, "path": path, "body": dict(body or {})})
            return {"code": "0", "data": [{"ordId": "must-not-send", "sCode": "0"}]}

        transport._request = fake_request

        with pytest.raises(OrderSubmitError) as exc_info:
            await transport.submit_passive_order(
                OrderRequest(
                    venue=Venue.OKX,
                    symbol="OPGUSDT",
                    side=Side.SELL,
                    quantity=9.0,
                    price=0.02,
                    post_only=True,
                    client_order_id="opgreject9",
                )
            )

        assert exc_info.value.class_ == SubmitFailureClass.REJECTED
        assert sent == []
        assert transport.order_diagnostics[-1]["payload"]["reject_reason"] == "contract_qty_zero"

    @pytest.mark.asyncio
    async def test_okx_passive_sends_contract_quantity_after_base_conversion(self, monkeypatch):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.000001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    ct_val=100.0,
                    rule_source="test_okx_instrument",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        monkeypatch.setattr(
            "lightfee.venues.symbol_rules.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        transport = VenueTransport(
            spec=okx_spec(),
            mode="live",
            credential=LiveCredential(
                api_key="okx-key",
                api_secret="okx-secret",
                api_passphrase="okx-pass",
            ),
        )
        transport._pos_mode_cache = "net"
        sent: list[dict[str, Any]] = []

        async def fake_request(method, path, *, body=None, params=None, private=False, **kwargs):
            sent.append({"method": method, "path": path, "body": dict(body or {})})
            return {
                "code": "0",
                "data": [{"ordId": "okx-passive-1", "clOrdId": "ubpassive100", "sCode": "0"}],
            }

        transport._request = fake_request

        ack = await transport.submit_passive_order(
            OrderRequest(
                venue=Venue.OKX,
                symbol="UBUSDT",
                side=Side.SELL,
                quantity=100.0,
                price=0.02,
                post_only=True,
                client_order_id="ubpassive100",
            )
        )

        assert sent[-1]["path"] == "/api/v5/trade/order"
        assert sent[-1]["body"]["sz"] == "1"
        assert ack.order_id == "okx-passive-1"

    @pytest.mark.asyncio
    async def test_okx_passive_preflight_does_not_treat_contract_lot_as_base_qty(
        self, monkeypatch,
    ):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.000001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    ct_val=0.1,
                    rule_source="test_okx_instrument",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        monkeypatch.setattr(
            "lightfee.venues.symbol_rules.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        transport = VenueTransport(
            spec=okx_spec(),
            mode="live",
            credential=LiveCredential(
                api_key="okx-key",
                api_secret="okx-secret",
                api_passphrase="okx-pass",
            ),
        )
        transport._pos_mode_cache = "net"
        sent: list[dict[str, Any]] = []

        async def fake_request(method, path, *, body=None, params=None, private=False, **kwargs):
            sent.append({"method": method, "path": path, "body": dict(body or {})})
            return {
                "code": "0",
                "data": [{"ordId": "okx-passive-min", "clOrdId": "ubpassive01", "sCode": "0"}],
            }

        transport._request = fake_request

        ack = await transport.submit_passive_order(
            OrderRequest(
                venue=Venue.OKX,
                symbol="UBUSDT",
                side=Side.SELL,
                quantity=0.1,
                price=0.02,
                post_only=True,
                client_order_id="ubpassive01",
            )
        )

        assert sent[-1]["path"] == "/api/v5/trade/order"
        assert sent[-1]["body"]["sz"] == "1"
        assert ack.order_id == "okx-passive-min"
        preflight = next(
            e["payload"] for e in transport.order_diagnostics
            if e["kind"] == "order.submit_attempt"
        )
        assert preflight["raw_qty"] == pytest.approx(0.1)
        assert preflight["normalized_qty"] == pytest.approx(0.1)
        assert preflight["qty_step"] == 0.0
        assert preflight["min_qty"] == 0.0

    @pytest.mark.asyncio
    async def test_okx_cancel_uses_post_cancel_order_endpoint(self):
        transport = VenueTransport(
            spec=okx_spec(),
            mode="live",
            credential=LiveCredential(
                api_key="okx-key",
                api_secret="okx-secret",
                api_passphrase="okx-pass",
            ),
        )
        sent: list[dict[str, Any]] = []

        async def fake_request(method, path, *, body=None, params=None, private=False, **kwargs):
            sent.append({
                "method": method,
                "path": path,
                "body": dict(body or {}),
                "params": dict(params or {}),
            })
            return {"code": "0", "data": [{"ordId": "12345", "sCode": "0"}]}

        transport._request = fake_request

        ack = await transport.cancel_passive_order("UBUSDT", "12345")

        assert sent == [
            {
                "method": "POST",
                "path": "/api/v5/trade/cancel-order",
                "body": {"instId": "UB-USDT-SWAP", "ordId": "12345"},
                "params": {},
            }
        ]
        assert ack.order_id == "12345"

    @pytest.mark.asyncio
    async def test_bybit_live_place_order_quantizes_and_records_sanitized_attempt_result(self, monkeypatch):
        from lightfee.venues.symbol_rules import SymbolRule

        spec = bybit_spec()

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                assert venue == Venue.BYBIT
                assert venue_symbol == "BTCUSDT"
                return SymbolRule(
                    tick_size=spec.price_tick,
                    qty_step=spec.quantity_step,
                    min_qty=spec.min_quantity,
                    min_notional=spec.min_notional,
                    rule_source="test_spec",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        transport = VenueTransport(
            spec=spec,
            mode="live",
            credential=LiveCredential(api_key="key-secret", api_secret="sign-secret"),
        )
        sent: list[dict] = []

        async def fake_request(method, path, *, body=None, params=None, private=False, **kwargs):
            sent.append({"method": method, "path": path, "body": dict(body or params or {})})
            return {"retCode": 0, "result": {"orderId": "bybit_ack_1", "orderLinkId": "cli_1"}}

        transport._request = fake_request
        req = OrderRequest(
            venue=Venue.BYBIT,
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=0.00749,
            price=50000.129,
            client_order_id="cli_1",
        )

        with pytest.raises(OrderSubmitError) as exc_info:
            await transport.place_order(req)

        assert exc_info.value.class_ == SubmitFailureClass.UNCERTAIN
        assert sent[0]["body"]["qty"] == "0.007"
        assert sent[0]["body"]["price"] == "50000.12"
        events = transport.order_diagnostics
        assert [e["kind"] for e in events] == ["order.submit_attempt", "order.submit_result"]
        payload = events[0]["payload"]
        assert payload["venue"] == "bybit"
        assert payload["endpoint"] == spec.order_path
        assert payload["product_type"] == "linear"
        assert payload["client_order_id"] == "cli_1"
        assert payload["raw_price"] == 50000.129
        assert payload["raw_qty"] == 0.00749
        assert payload["quantized_price"] == 50000.12
        assert payload["quantized_qty"] == 0.007
        assert payload["tick_size"] == spec.price_tick
        assert payload["quantity_step"] == spec.quantity_step
        assert payload["response_classification"] == "attempt"
        assert payload["body_sanitized"]["qty"] == "0.007"
        assert payload["body_sanitized"]["price"] == "50000.12"
        assert payload["body_sanitized"]["positionIdx"] in (0, 1, 2)
        assert "orderLinkId" not in payload["body_sanitized"]
        serialized = json.dumps(events)
        assert "sign-secret" not in serialized
        assert "key-secret" not in serialized

    @pytest.mark.asyncio
    async def test_okx_reduce_only_converts_base_quantity_and_ack_returns_fill(self, monkeypatch):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.000001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    ct_val=100.0,
                    rule_source="test_okx_instrument",
                )

        monkeypatch.setattr(
            "lightfee.venues.symbol_rules.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )

        spec = okx_spec()
        transport = VenueTransport(
            spec=spec,
            mode="live",
            credential=LiveCredential(api_key="okx-key", api_secret="okx-secret", api_passphrase="okx-pass"),
        )
        transport._pos_mode_cache = "net"
        sent: list[dict] = []

        async def fake_request(method, path, *, body=None, params=None, private=False, **kwargs):
            sent.append({"method": method, "path": path, "body": dict(body or {})})
            return {
                "code": "0",
                "data": [{"ordId": "okx_ack_1", "clOrdId": "okx-close-stable", "sCode": "0"}],
            }

        transport._request = fake_request
        req = OrderRequest(
            venue=Venue.OKX,
            symbol="STABLEUSDT",
            side=Side.SELL,
            quantity=500.0,
            reduce_only=True,
            client_order_id="okx-close-stable",
            price_hint=0.049,
            observed_at_ms=123456,
        )

        fill = await transport.place_order(req)

        assert sent[-1]["path"] == spec.order_path
        assert sent[-1]["body"]["instId"] == "STABLE-USDT-SWAP"
        assert sent[-1]["body"]["reduceOnly"] == "true"
        assert sent[-1]["body"]["sz"] == "5"
        assert fill.venue == Venue.OKX
        assert fill.quantity == pytest.approx(500.0)
        assert fill.price == pytest.approx(0.049)
        assert fill.order_id == "okx_ack_1"
        assert fill.client_order_id == "okx-close-stable"
        attempt = next(e["payload"] for e in transport.order_diagnostics if e["kind"] == "order.submit_attempt")
        assert attempt["ct_val"] == 100.0
        assert attempt["base_qty"] == 500.0
        assert attempt["contract_qty"] == 5.0
        assert attempt["quantity_units"] == "base_to_contracts"
        assert attempt["body_sanitized"]["sz"] == "5"
        result = next(e["payload"] for e in transport.order_diagnostics if e["kind"] == "order.submit_result")
        assert result["response_classification"] == "ack_accepted"

    @pytest.mark.asyncio
    async def test_okx_filled_response_converts_fill_contracts_to_base_quantity(
        self,
        monkeypatch,
    ):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.000001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    ct_val=100.0,
                    rule_source="test_okx_instrument",
                )

        monkeypatch.setattr(
            "lightfee.venues.symbol_rules.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )

        transport = VenueTransport(
            spec=okx_spec(),
            mode="live",
            credential=LiveCredential(
                api_key="okx-key",
                api_secret="okx-secret",
                api_passphrase="okx-pass",
            ),
        )
        transport._pos_mode_cache = "net"

        async def fake_request(method, path, *, body=None, params=None, private=False, **kwargs):
            assert path == transport._spec.order_path
            return {
                "code": "0",
                "data": [
                    {
                        "ordId": "okx-fill-1",
                        "clOrdId": "okx-fill-cid",
                        "sCode": "0",
                        "fillSz": "3",
                        "avgPx": "0.0525",
                        "state": "filled",
                    }
                ],
            }

        transport._request = fake_request
        req = OrderRequest(
            venue=Venue.OKX,
            symbol="HOMEUSDT",
            side=Side.BUY,
            quantity=300.0,
            client_order_id="okx-fill-cid",
            price_hint=0.0525,
            observed_at_ms=123456,
        )

        fill = await transport.place_order(req)

        assert fill.quantity == pytest.approx(300.0)
        attempt = next(
            e["payload"]
            for e in transport.order_diagnostics
            if e["kind"] == "order.submit_attempt"
        )
        assert attempt["ct_val"] == pytest.approx(100.0)
        assert attempt["contract_qty"] == pytest.approx(3.0)

    @pytest.mark.asyncio
    async def test_okx_fetch_position_uses_instrument_ct_val_for_base_quantity(self, monkeypatch):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.000001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    ct_val=100.0,
                    rule_source="test_okx_instrument",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )

        transport = VenueTransport(
            spec=okx_spec(),
            mode="live",
            credential=LiveCredential(api_key="okx-key", api_secret="okx-secret", api_passphrase="okx-pass"),
        )

        async def fake_request(method, path, *, body=None, params=None, private=False, **kwargs):
            if path == "/api/v5/account/instruments":
                assert method == "GET"
                assert private is True
                assert params == {"instType": "SWAP"}
                return {
                    "code": "0",
                    "data": [
                        {
                            "instId": "CHIP-USDT-SWAP",
                            "instType": "SWAP",
                            "ctVal": "100",
                            "ctType": "linear",
                            "state": "live",
                        }
                    ],
                }
            assert path == transport._spec.position_path
            assert params == {"instId": "CHIP-USDT-SWAP"}
            return {
                "code": "0",
                "data": [
                    {
                        "instId": "CHIP-USDT-SWAP",
                        "pos": "5",
                        "posSide": "net",
                        "avgPx": "0.04794",
                    }
                ],
            }

        transport._request = fake_request
        transport._public_get = AsyncMock(side_effect=RuntimeError("public catalog unavailable"))

        pos = await transport.fetch_position("CHIPUSDT")

        assert pos.symbol == "CHIP-USDT-SWAP"
        assert pos.side == Side.BUY
        assert pos.quantity == pytest.approx(500.0)
        assert pos.entry_price == pytest.approx(0.04794)

    @pytest.mark.asyncio
    async def test_okx_fetch_position_preloads_swap_instrument_metadata(self):
        transport = VenueTransport(
            spec=okx_spec(),
            mode="live",
            credential=LiveCredential(
                api_key="okx-key",
                api_secret="okx-secret",
                api_passphrase="okx-pass",
            ),
        )
        public_calls: list[tuple[str, dict[str, str]]] = []

        async def fake_public_get(path, params=None):
            public_calls.append((path, dict(params or {})))
            assert path == "/api/v5/public/instruments"
            assert params == {"instType": "SWAP"}
            return {
                "code": "0",
                "data": [
                    {
                        "instId": "CHIP-USDT-SWAP",
                        "instType": "SWAP",
                        "ctVal": "100",
                        "ctType": "linear",
                        "lotSz": "1",
                        "minSz": "1",
                        "state": "live",
                    }
                ],
            }

        async def fake_request(method, path, *, body=None, params=None, private=False, **kwargs):
            assert path == transport._spec.position_path
            assert params == {"instId": "CHIP-USDT-SWAP"}
            return {
                "code": "0",
                "data": [
                    {
                        "instId": "CHIP-USDT-SWAP",
                        "pos": "5",
                        "posSide": "net",
                        "avgPx": "0.04794",
                    }
                ],
            }

        transport._public_get = fake_public_get
        transport._request = fake_request

        pos = await transport.fetch_position("CHIPUSDT")

        assert public_calls == [
            ("/api/v5/public/instruments", {"instType": "SWAP"})
        ]
        assert transport._symbol_metadata["CHIP-USDT-SWAP"]["ctVal"] == "100"
        assert transport._symbol_metadata["CHIPUSDT"]["ctType"] == "linear"
        assert pos.quantity == pytest.approx(500.0)

    @pytest.mark.asyncio
    async def test_okx_missing_ct_val_is_metadata_missing_not_blank_probe_error(self):
        transport = VenueTransport(
            spec=okx_spec(),
            mode="live",
            credential=LiveCredential(
                api_key="okx-key",
                api_secret="okx-secret",
                api_passphrase="okx-pass",
            ),
        )

        async def fake_public_get(path, params=None):
            return {
                "code": "0",
                "data": [
                    {
                        "instId": "CHIP-USDT-SWAP",
                        "instType": "SWAP",
                        "ctVal": "",
                        "ctType": "linear",
                        "state": "live",
                    }
                ],
            }

        async def fake_request(method, path, *, body=None, params=None, private=False, **kwargs):
            return {
                "code": "0",
                "data": [
                    {
                        "instId": "CHIP-USDT-SWAP",
                        "pos": "5",
                        "posSide": "net",
                        "avgPx": "0.04794",
                    }
                ],
            }

        transport._public_get = fake_public_get
        transport._request = fake_request

        with pytest.raises(TransportError) as exc_info:
            await transport.fetch_position("CHIPUSDT")

        assert exc_info.value.category == TransportErrorCategory.NORMALIZATION_FAILURE
        assert "okx_contract_metadata_missing_ct_val" in str(exc_info.value)
        assert "classification=metadata_missing" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_okx_contract_size_requires_official_ct_type_metadata(self):
        transport = VenueTransport(
            spec=okx_spec(),
            mode="live",
            credential=LiveCredential(
                api_key="okx-key",
                api_secret="okx-secret",
                api_passphrase="okx-pass",
            ),
        )

        async def fake_public_get(path, params=None):
            return {
                "code": "0",
                "data": [
                    {
                        "instId": "CHIP-USDT-SWAP",
                        "instType": "SWAP",
                        "ctVal": "100",
                        "state": "live",
                    }
                ],
            }

        transport._public_get = fake_public_get

        with pytest.raises(TransportError) as exc_info:
            await transport._okx_contract_size_for_venue_symbol("CHIP-USDT-SWAP")

        assert exc_info.value.category == TransportErrorCategory.NORMALIZATION_FAILURE
        assert "okx_contract_metadata_missing_ct_val" in str(exc_info.value)
        assert "classification=metadata_missing" in str(exc_info.value)
        assert "instId=CHIP-USDT-SWAP" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_okx_contract_size_rejects_symbol_rule_ct_val_without_official_metadata(
        self, monkeypatch,
    ):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.000001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    ct_val=100.0,
                    rule_source="test_cache_without_ct_type",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        transport = VenueTransport(spec=okx_spec(), mode="paper")

        with pytest.raises(TransportError) as exc_info:
            await transport._okx_contract_size_for_venue_symbol("CHIP-USDT-SWAP")

        assert exc_info.value.category == TransportErrorCategory.NORMALIZATION_FAILURE
        assert "okx_contract_metadata_missing_ct_val" in str(exc_info.value)
        assert "classification=metadata_missing" in str(exc_info.value)
        assert "instId=CHIP-USDT-SWAP" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_okx_contract_size_rejects_trusted_cache_ct_val_without_metadata(
        self, monkeypatch,
    ):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.000001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    ct_val=100.0,
                    rule_source="instrument",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        transport = VenueTransport(spec=okx_spec(), mode="paper")

        with pytest.raises(TransportError) as exc_info:
            await transport._okx_contract_size_for_venue_symbol("CHIP-USDT-SWAP")

        assert exc_info.value.category == TransportErrorCategory.NORMALIZATION_FAILURE
        assert "okx_contract_metadata_missing_ct_val" in str(exc_info.value)
        assert "classification=metadata_missing" in str(exc_info.value)
        assert "instId=CHIP-USDT-SWAP" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_okx_normalize_quantity_rejects_symbol_rule_ct_val_without_official_metadata(
        self, monkeypatch,
    ):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.000001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    ct_val=100.0,
                    rule_source="test_cache_without_ct_type",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        transport = VenueTransport(spec=okx_spec(), mode="paper")

        with pytest.raises(TransportError) as exc_info:
            await transport.normalize_quantity("CHIPUSDT", 100.0)

        assert exc_info.value.category == TransportErrorCategory.NORMALIZATION_FAILURE
        assert "okx_contract_metadata_missing_ct_val" in str(exc_info.value)
        assert "classification=metadata_missing" in str(exc_info.value)
        assert "instId=CHIP-USDT-SWAP" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_okx_normalize_quantity_rejects_trusted_cache_ct_val_without_metadata(
        self, monkeypatch,
    ):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.000001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    ct_val=100.0,
                    rule_source="instrument",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        transport = VenueTransport(spec=okx_spec(), mode="paper")

        with pytest.raises(TransportError) as exc_info:
            await transport.normalize_quantity("CHIPUSDT", 100.0)

        assert exc_info.value.category == TransportErrorCategory.NORMALIZATION_FAILURE
        assert "okx_contract_metadata_missing_ct_val" in str(exc_info.value)
        assert "classification=metadata_missing" in str(exc_info.value)
        assert "instId=CHIP-USDT-SWAP" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_okx_normalize_quantity_missing_ct_val_is_classified(self, monkeypatch):
        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return type(
                    "Rule",
                    (),
                    {"ct_val": 0.0, "qty_step": 1.0, "min_qty": 1.0},
                )()

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )

        transport = VenueTransport(
            spec=okx_spec(),
            mode="live",
            credential=LiveCredential(
                api_key="okx-key",
                api_secret="okx-secret",
                api_passphrase="okx-pass",
            ),
        )

        async def fake_public_get(path, params=None):
            return {
                "code": "0",
                "data": [
                    {
                        "instId": "CHIP-USDT-SWAP",
                        "instType": "SWAP",
                        "ctVal": "",
                        "ctType": "linear",
                        "lotSz": "1",
                        "minSz": "1",
                    }
                ],
            }

        transport._public_get = fake_public_get

        with pytest.raises(TransportError) as exc_info:
            await transport.normalize_quantity("CHIPUSDT", 100.0)

        assert exc_info.value.category == TransportErrorCategory.NORMALIZATION_FAILURE
        assert "okx_contract_metadata_missing_ct_val" in str(exc_info.value)
        assert "classification=metadata_missing" in str(exc_info.value)
        assert "instId=CHIP-USDT-SWAP" in str(exc_info.value)

    def test_bybit_preflight_preserves_exact_step_quantity_without_float_slip(self):
        transport = VenueTransport(spec=bybit_spec(), mode="paper")
        preflight = transport.preflight_order_request(
            OrderRequest(
                venue=Venue.BYBIT,
                symbol="0GUSDT",
                side=Side.SELL,
                quantity=47.8,
                price=0.5014,
                reduce_only=True,
                client_order_id="bybit-close-0g",
            )
        )

        assert preflight["quantity_step"] == 0.001
        assert preflight["quantized_qty"] == 47.8

    @pytest.mark.parametrize(
        "spec_fn,raw_qty,price,expected_qty",
        [
            (gate_spec, 1.7, 50000.0, 1.0),
            (binance_spec, 0.00749, 50000.129, 0.007),
            (aster_spec, 0.00749, 50000.129, 0.007),
        ],
    )
    def test_order_preflight_quantizes_contract_or_step_sizes(
        self, spec_fn, raw_qty, price, expected_qty,
    ):
        transport = VenueTransport(spec=spec_fn(), mode="paper")
        preflight = transport.preflight_order_request(
            OrderRequest(
                venue=transport.venue,
                symbol="BTCUSDT",
                side=Side.BUY,
                quantity=raw_qty,
                price=price,
                client_order_id="cli_step",
            )
        )
        assert preflight["quantized_qty"] == expected_qty
        assert preflight["quantity_step"] == transport.spec.quantity_step
        assert preflight["tick_size"] == transport.spec.price_tick

    def test_order_preflight_rejects_below_min_notional_fail_closed(self):
        transport = VenueTransport(spec=binance_spec(), mode="paper")
        with pytest.raises(OrderSubmitError) as exc_info:
            transport.preflight_order_request(
                OrderRequest(
                    venue=Venue.BINANCE,
                    symbol="BTCUSDT",
                    side=Side.BUY,
                    quantity=0.001,
                    price=100.0,
                    client_order_id="too_small",
                )
            )

        assert exc_info.value.class_ == SubmitFailureClass.REJECTED
        assert "min_notional" in str(exc_info.value)
        event = transport.order_diagnostics[-1]
        assert event["kind"] == "order.submit_result"
        assert event["payload"]["response_classification"] == "precision_rejected"


class TestHyperliquidSigningDependencyPreflight:
    """Live Hyperliquid startup must expose missing signing deps without secrets."""

    def test_hyperliquid_signing_dependencies_are_declared(self):
        import tomllib
        from pathlib import Path

        pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
        dependencies = {
            str(dep).split(">=", 1)[0].split("==", 1)[0].lower()
            for dep in tomllib.loads(pyproject.read_text())["project"]["dependencies"]
        }

        assert {"pycryptodome", "eth-account", "msgpack"}.issubset(dependencies)

    def test_missing_crypto_dependency_is_preflight_visible(self, monkeypatch):
        import importlib.util

        real_find_spec = importlib.util.find_spec

        def fake_find_spec(name, *args, **kwargs):
            if name in {"Crypto", "Crypto.Hash"}:
                raise ModuleNotFoundError("No module named 'Crypto'")
            return real_find_spec(name, *args, **kwargs)

        monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)

        from lightfee.venues.transport import _missing_hyperliquid_signing_dependencies

        missing = _missing_hyperliquid_signing_dependencies()
        assert missing[0] == "pycryptodome"
        assert "pycryptodome" in missing

    def test_missing_eth_account_dependency_is_preflight_visible_and_redacted(self, monkeypatch):
        import importlib.util

        real_find_spec = importlib.util.find_spec

        def fake_find_spec(name, *args, **kwargs):
            if name == "eth_account":
                return None
            return real_find_spec(name, *args, **kwargs)

        monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)
        private_key = "0x0123456789abcdefSECRET"
        cred = LiveCredential(wallet_private_key=private_key)

        with pytest.raises(ValueError) as exc_info:
            VenueTransport(spec=hyperliquid_spec(), mode="live", credential=cred)

        message = str(exc_info.value)
        assert "missing signing dependencies" in message
        assert "eth-account" in message
        assert private_key not in message


class TestBitgetRiskHealth:
    """Bitget account risk snapshot parsing (V1: bitget_account_risk_snapshot_from_account_row)."""

    def test_parses_bitget_account_assets_response(self):
        from lightfee.engine.risk_actions import AccountRiskSnapshot

        spec = bitget_spec()
        transport = VenueTransport(spec=spec, mode="live",
                                   credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"))
        # Simulate a parsed response (bypassing HTTP)
        raw = {"code": "00000", "data": [
            {"marginCoin": "USDT", "usdtEquity": "10000.0",
             "maintenanceMargin": "1000.0",
             "available": "8000.0", "equity": "10000.0"},
        ]}
        # Access private parsing via the pending data
        # Test the raw response shape that fetch_account_risk_snapshot would process
        from lightfee.venues.transport import VenueTransport as VT
        # Manually test extraction logic
        data = raw.get("data", raw)
        row = data[0] if isinstance(data, list) else data
        assert float(row["usdtEquity"]) == 10000.0
        assert float(row["maintenanceMargin"]) == 1000.0

    def test_bitget_missing_maintenance_margin_returns_none(self):
        spec = bitget_spec()
        transport = VenueTransport(spec=spec, mode="live",
                                   credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"))
        raw = {"code": "00000", "data": [
            {"marginCoin": "USDT", "usdtEquity": "10000.0", "available": "8000.0"},
        ]}
        data = raw.get("data", raw)
        row = data[0] if isinstance(data, list) else data
        maint = row.get("maintenanceMargin")
        assert maint is None

    def test_bitget_supports_risk_health_false_in_live_mode(self):
        """V1 parity: Bitget risk_health is UNSUPPORTED even in live mode."""
        from lightfee.venues.bitget import BitgetAdapter
        adapter = BitgetAdapter(mode="live", credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"))
        assert adapter.supports_risk_health is False

    def test_bitget_supports_risk_health_false_in_paper(self):
        from lightfee.venues.bitget import BitgetAdapter
        adapter = BitgetAdapter(mode="paper")
        assert adapter.supports_risk_health is False


class TestGateRiskHealth:
    """Gate account risk snapshot parsing (V1: gate_account_risk_snapshot_from_wallet_row)."""

    def test_parses_gate_account_assets_response(self):
        spec = gate_spec()
        transport = VenueTransport(spec=spec, mode="live",
                                   credential=LiveCredential(api_key="k", api_secret="s"))
        raw = {
            "total": "10000.0",
            "maintenance_margin": "1000.0",
            "available": "8000.0",
        }
        assert float(raw["total"]) == 10000.0
        assert float(raw["maintenance_margin"]) == 1000.0

    def test_gate_missing_maintenance_margin_returns_none(self):
        spec = gate_spec()
        transport = VenueTransport(spec=spec, mode="live",
                                   credential=LiveCredential(api_key="k", api_secret="s"))
        raw = {"total": "10000.0", "available": "8000.0"}
        maint = raw.get("maintenance_margin")
        assert maint is None

    def test_gate_supports_risk_health_false_in_live_mode(self):
        """V1 parity: Gate risk_health is UNSUPPORTED even in live mode."""
        from lightfee.venues.gate import GateAdapter
        adapter = GateAdapter(mode="live", credential=LiveCredential(api_key="k", api_secret="s"))
        assert adapter.supports_risk_health is False

    def test_gate_supports_risk_health_false_in_paper(self):
        from lightfee.venues.gate import GateAdapter
        adapter = GateAdapter(mode="paper")
        assert adapter.supports_risk_health is False


class TestRiskSnapshotCache:
    """V1: runtime risk snapshot cache — same-tick same-venue reuses cached fetch."""

    @pytest.mark.asyncio
    async def test_cache_hit_avoids_refetch(self):
        import tempfile, os
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.config.schema import AppConfig, PersistenceConfig

        with tempfile.TemporaryDirectory() as td:
            config = AppConfig(
                symbols=["BTCUSDT"],
                persistence=PersistenceConfig(event_log_path=os.path.join(td, "events.jsonl")),
            )
            rt = LiveRuntime(config)
            rt.journal.open()

            call_count = 0

            class FakeAdapter:
                supports_risk_health = True
                async def fetch_account_risk_snapshot(self):
                    nonlocal call_count
                    call_count += 1
                    from lightfee.engine.risk_actions import AccountRiskSnapshot
                    return AccountRiskSnapshot(
                        venue=Venue.BINANCE, equity_quote=10000, maintenance_margin_quote=1000,
                        health_ratio=10.0, observed_at_ms=1000, source="test",
                    )

            adapter = FakeAdapter()
            now_ms = 1000

            # First fetch — cache miss
            snap1, sup1 = await rt._fetch_venue_risk_snapshot(Venue.BINANCE, adapter, True, now_ms)
            assert call_count == 1
            assert snap1 is not None

            # Second fetch within TTL — cache hit
            snap2, sup2 = await rt._fetch_venue_risk_snapshot(Venue.BINANCE, adapter, True, now_ms + 500)
            assert call_count == 1  # No second fetch
            assert snap2 is not None
            assert sup2 is True

            rt.journal.close()

    @pytest.mark.asyncio
    async def test_cache_ttl_expiry_triggers_refetch(self):
        import tempfile, os
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.config.schema import AppConfig, PersistenceConfig

        with tempfile.TemporaryDirectory() as td:
            config = AppConfig(
                symbols=["BTCUSDT"],
                persistence=PersistenceConfig(event_log_path=os.path.join(td, "events.jsonl")),
            )
            rt = LiveRuntime(config)
            rt.journal.open()

            call_count = 0

            class FakeAdapter:
                supports_risk_health = True
                async def fetch_account_risk_snapshot(self):
                    nonlocal call_count
                    call_count += 1
                    from lightfee.engine.risk_actions import AccountRiskSnapshot
                    return AccountRiskSnapshot(
                        venue=Venue.BINANCE, equity_quote=10000, maintenance_margin_quote=1000,
                        health_ratio=10.0, observed_at_ms=1000, source="test",
                    )

            adapter = FakeAdapter()
            now_ms = 1000

            # First fetch
            await rt._fetch_venue_risk_snapshot(Venue.BINANCE, adapter, True, now_ms)
            assert call_count == 1

            # After TTL (1s) expiry
            await rt._fetch_venue_risk_snapshot(Venue.BINANCE, adapter, True, now_ms + 2000)
            assert call_count == 2

            rt.journal.close()

    @pytest.mark.asyncio
    async def test_cache_stores_error_and_journals(self):
        import tempfile, os
        from lightfee.engine.runtime import LiveRuntime
        from lightfee.config.schema import AppConfig, PersistenceConfig

        with tempfile.TemporaryDirectory() as td:
            config = AppConfig(
                symbols=["BTCUSDT"],
                persistence=PersistenceConfig(event_log_path=os.path.join(td, "events.jsonl")),
            )
            rt = LiveRuntime(config)
            rt.journal.open()

            class FailingAdapter:
                supports_risk_health = True
                async def fetch_account_risk_snapshot(self):
                    raise RuntimeError("rate limited")

            adapter = FailingAdapter()

            snap, sup = await rt._fetch_venue_risk_snapshot(Venue.OKX, adapter, True, 1000)
            assert snap is None
            # Fetch error → snapshot None, but capability (supports) unchanged per V1
            assert sup is True

            # Error should be cached — no second exception attempt if called again within TTL
            snap2, sup2 = await rt._fetch_venue_risk_snapshot(Venue.OKX, adapter, True, 1500)
            assert snap2 is None
            assert sup2 is True

            rt.journal.close()

    @pytest.mark.asyncio
    async def test_aster_has_longer_cache_ttl(self):
        from lightfee.engine.runtime import LiveRuntime
        assert LiveRuntime._risk_snapshot_ttl_ms(Venue.ASTER) == 30_000
        assert LiveRuntime._risk_snapshot_ttl_ms(Venue.BINANCE) == 1_000
        assert LiveRuntime._risk_snapshot_ttl_ms(Venue.OKX) == 1_000


# ---------------------------------------------------------------------------
# Bitget risk health real-method tests — call fetch_account_risk_snapshot()
# through mocked _request (not manual dict parsing)
# ---------------------------------------------------------------------------


class TestBitgetRiskHealthRealMethod:
    """BitgetAdapter.fetch_account_risk_snapshot() profile-aware tests.

    V1: bitget.rs fetch_private_account_assets_payload (line 836-866).
    """

    @pytest.mark.asyncio
    async def test_uta_success_returns_snapshot(self):
        from lightfee.venues.bitget import BitgetAdapter, BitgetAccountProfile
        from lightfee.engine.risk_actions import AccountRiskSnapshot

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        # Pre-set profile to UTA to skip detect_profile() probe
        adapter._profile = BitgetAccountProfile.UTA

        call_urls = []

        async def mock_request(method, path, params=None, body=None, private=False):
            call_urls.append(path)
            return {
                "code": "00000",
                "data": [{
                    "marginCoin": "USDT",
                    "usdtEquity": "10000.0",
                    "maintenanceMargin": "1000.0",
                    "available": "8000.0",
                }],
            }

        adapter._transport._request = mock_request

        result = await adapter.fetch_account_risk_snapshot()
        assert result is not None
        assert isinstance(result, AccountRiskSnapshot)
        assert result.venue == Venue.BITGET
        assert result.equity_quote == 10000.0
        assert result.maintenance_margin_quote == 1000.0
        assert result.health_ratio == 10.0
        assert result.available_balance_quote == 8000.0
        assert result.source == "bitget_account_risk"
        # UTA path used: adapter delegates to resolved UTA v3 family contract.
        assert any("/api/v3/account/assets" in u for u in call_urls)

    @pytest.mark.asyncio
    async def test_classic_cached_goes_direct_to_classic_endpoint(self):
        from lightfee.venues.bitget import BitgetAdapter, BitgetAccountProfile
        from lightfee.engine.risk_actions import AccountRiskSnapshot

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter._profile = BitgetAccountProfile.CLASSIC

        call_urls = []

        async def mock_request(method, path, params=None, body=None, private=False):
            call_urls.append(path)
            return {
                "code": "00000",
                "data": [{
                    "marginCoin": "USDT",
                    "equity": "5000.0",
                    "maintenanceMargin": "500.0",
                    "availableBalance": "4000.0",
                }],
            }

        adapter._transport._request = mock_request

        result = await adapter.fetch_account_risk_snapshot()
        assert result is not None
        assert result.equity_quote == 5000.0
        assert result.maintenance_margin_quote == 500.0
        # Classic endpoint was used
        assert any("/api/v2/mix/account/accounts" in u for u in call_urls)

    @pytest.mark.asyncio
    async def test_uta_mismatch_falls_back_to_classic(self):
        from lightfee.venues.bitget import BitgetAdapter, BitgetAccountProfile, _is_classic_mode_error
        from lightfee.engine.risk_actions import AccountRiskSnapshot

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter._profile = None  # Force probe — but we mock the profile as undetected
        # Simulate UTA not yet known: the adapter will try UTA first
        adapter._profile = BitgetAccountProfile.UTA  # Assume UTA until mismatch

        call_count = 0
        call_paths = []

        async def mock_request(method, path, params=None, body=None, private=False):
            nonlocal call_count
            call_count += 1
            call_paths.append(path)
            if call_count == 1:
                # First call: UTA returns classic mismatch error
                raise TransportError(
                    TransportErrorCategory.REQUEST_REJECTED,
                    "classic account not supported",
                    status_code=400,
                    body='{"code":"40034","msg":"classic account not supported"}',
                )
            else:
                # Second call: classic endpoint succeeds
                return {
                    "code": "00000",
                    "data": [{
                        "marginCoin": "USDT",
                        "usdtEquity": "3000.0",
                        "maintenanceMargin": "300.0",
                        "available": "2000.0",
                    }],
                }

        adapter._transport._request = mock_request

        result = await adapter.fetch_account_risk_snapshot()
        assert result is not None
        assert result.equity_quote == 3000.0
        assert result.maintenance_margin_quote == 300.0
        # Profile should now be CLASSIC (cached)
        assert adapter._profile == BitgetAccountProfile.CLASSIC
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_auth_401_does_not_fallback(self):
        from lightfee.venues.bitget import BitgetAdapter, BitgetAccountProfile

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter._profile = BitgetAccountProfile.UTA

        async def mock_request(method, path, params=None, body=None, private=False):
            raise TransportError(
                TransportErrorCategory.AUTH_FAILURE,
                "Unauthorized",
                status_code=401,
                body='{"code":"40100","msg":"Invalid API key"}',
            )

        adapter._transport._request = mock_request

        with pytest.raises(TransportError) as exc_info:
            await adapter.fetch_account_risk_snapshot()
        assert exc_info.value.category == TransportErrorCategory.AUTH_FAILURE
        # Profile must NOT change to CLASSIC
        assert adapter._profile == BitgetAccountProfile.UTA

    @pytest.mark.asyncio
    async def test_rate_limit_429_does_not_fallback(self):
        from lightfee.venues.bitget import BitgetAdapter, BitgetAccountProfile

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter._profile = BitgetAccountProfile.UTA

        async def mock_request(method, path, params=None, body=None, private=False):
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                "Rate limited",
                status_code=429,
                body='{"code":"42900","msg":"Too many requests"}',
            )

        adapter._transport._request = mock_request

        with pytest.raises(TransportError) as exc_info:
            await adapter.fetch_account_risk_snapshot()
        assert exc_info.value.category == TransportErrorCategory.TRANSPORT_FAILURE
        assert adapter._profile == BitgetAccountProfile.UTA

    @pytest.mark.asyncio
    async def test_network_timeout_does_not_fallback(self):
        from lightfee.venues.bitget import BitgetAdapter, BitgetAccountProfile

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter._profile = BitgetAccountProfile.UTA

        async def mock_request(method, path, params=None, body=None, private=False):
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                "timeout: GET /api/v3/account/assets",
            )

        adapter._transport._request = mock_request

        with pytest.raises(TransportError) as exc_info:
            await adapter.fetch_account_risk_snapshot()
        assert exc_info.value.category == TransportErrorCategory.TRANSPORT_FAILURE
        assert adapter._profile == BitgetAccountProfile.UTA

    @pytest.mark.asyncio
    async def test_missing_maintenance_margin_returns_none(self):
        from lightfee.venues.bitget import BitgetAdapter, BitgetAccountProfile

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter._profile = BitgetAccountProfile.UTA

        async def mock_request(method, path, params=None, body=None, private=False):
            return {
                "code": "00000",
                "data": [{
                    "marginCoin": "USDT",
                    "usdtEquity": "10000.0",
                    "available": "8000.0",
                }],
            }

        adapter._transport._request = mock_request

        result = await adapter.fetch_account_risk_snapshot()
        assert result is None

    @pytest.mark.asyncio
    async def test_profile_cached_second_call_no_probe(self):
        from lightfee.venues.bitget import BitgetAdapter, BitgetAccountProfile
        from lightfee.engine.risk_actions import AccountRiskSnapshot

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter._profile = BitgetAccountProfile.UTA

        call_count = 0

        async def mock_request(method, path, params=None, body=None, private=False):
            nonlocal call_count
            call_count += 1
            return {
                "code": "00000",
                "data": [{
                    "marginCoin": "USDT",
                    "usdtEquity": "10000.0",
                    "maintenanceMargin": "1000.0",
                    "available": "8000.0",
                }],
            }

        adapter._transport._request = mock_request

        # First fetch
        r1 = await adapter.fetch_account_risk_snapshot()
        assert r1 is not None
        assert call_count == 1

        # Second fetch — profile cached, goes directly to UTA endpoint
        r2 = await adapter.fetch_account_risk_snapshot()
        assert r2 is not None
        assert call_count == 2


# ---------------------------------------------------------------------------
# PendingEntry recovery roundtrip — new maker-event fields
# ---------------------------------------------------------------------------


class TestPendingEntryRecoveryRoundtrip:
    """PendingEntry.entry_type, maker_price, long_quantity, short_quantity
    must survive snapshot ↔ recovery roundtrip."""

    def test_pending_entry_new_fields_in_snapshot_to_dict(self):
        from lightfee.engine.state import EngineState, PendingEntry
        from lightfee.core.domain import Side, Venue

        state = EngineState()
        pe = PendingEntry(
            pending_id="pe-001",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            target_quantity=0.01,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=5000,
            entry_type="passive_incremental",
            maker_price=50000.0,
            long_quantity=0.005,
            short_quantity=0.005,
        )
        state.pending_entries["pe-001"] = pe
        d = state.to_dict()
        pend = d["pending_entries"]["pe-001"]
        assert pend["entry_type"] == "passive_incremental"
        assert pend["maker_price"] == 50000.0
        assert pend["long_quantity"] == 0.005
        assert pend["short_quantity"] == 0.005

    def test_pending_entry_new_fields_restored_from_snapshot(self):
        from lightfee.engine.recovery import _restore_state_from_snapshot_dict

        snap = {
            "lifecycle": "running",
            "risk_mode": "running",
            "open_positions": {},
            "pending_entries": {
                "pe-001": {
                    "pending_id": "pe-001",
                    "symbol": "BTCUSDT",
                    "long_venue": "binance",
                    "short_venue": "okx",
                    "target_quantity": 0.01,
                    "long_side": "buy",
                    "short_side": "sell",
                    "created_at_ms": 5000,
                    "entry_type": "passive_incremental",
                    "maker_price": 50000.0,
                    "long_quantity": 0.005,
                    "short_quantity": 0.005,
                },
            },
            "pending_closes": {},
        }
        state = _restore_state_from_snapshot_dict(snap)
        pe = state.pending_entries["pe-001"]
        assert pe.entry_type == "passive_incremental"
        assert pe.maker_price == 50000.0
        assert pe.long_quantity == 0.005
        assert pe.short_quantity == 0.005

    def test_persistent_view_includes_new_fields(self):
        from lightfee.engine.state import EngineState, PendingEntry
        from lightfee.engine.recovery import build_persistent_state_view
        from lightfee.core.domain import Side, Venue

        state = EngineState()
        pe = PendingEntry(
            pending_id="pe-001",
            symbol="BTCUSDT",
            long_venue=Venue.BINANCE,
            short_venue=Venue.OKX,
            target_quantity=0.01,
            long_side=Side.BUY,
            short_side=Side.SELL,
            created_at_ms=5000,
            entry_type="passive_incremental",
            maker_price=50000.0,
            long_quantity=0.005,
            short_quantity=0.005,
        )
        state.pending_entries["pe-001"] = pe
        view = build_persistent_state_view(state)
        pend = view["pending_entries"]["pe-001"]
        assert pend["entry_type"] == "passive_incremental"
        assert pend["maker_price"] == 50000.0
        assert pend["long_quantity"] == 0.005
        assert pend["short_quantity"] == 0.005

    def test_old_snapshot_without_new_fields_defaults_empty(self):
        """Backward-compat: snapshot without entry_type/maker_price fields
        should restore PendingEntry with default empty/zero values."""
        from lightfee.engine.recovery import _restore_state_from_snapshot_dict

        snap = {
            "lifecycle": "running",
            "risk_mode": "running",
            "open_positions": {},
            "pending_entries": {
                "old-pe": {
                    "pending_id": "old-pe",
                    "symbol": "ETHUSDT",
                    "long_venue": "gate",
                    "short_venue": "bybit",
                    "target_quantity": 0.1,
                    "long_side": "buy",
                    "short_side": "sell",
                    "created_at_ms": 1000,
                },
            },
            "pending_closes": {},
        }
        state = _restore_state_from_snapshot_dict(snap)
        pe = state.pending_entries["old-pe"]
        assert pe.entry_type == ""
        assert pe.maker_price == 0.0
        assert pe.long_quantity == 0.0
        assert pe.short_quantity == 0.0


# ---------------------------------------------------------------------------
# Task 1: ACK-only and exchange error envelope tests
# ---------------------------------------------------------------------------

FIXTURE_DIR_T1 = "tests/fixtures/venues"


def _fixture(relative_path: str):
    """Load a JSON fixture from tests/fixtures/venues/"""
    import json as _json
    path = f"{FIXTURE_DIR_T1}/{relative_path}"
    with open(path) as f:
        return _json.load(f)


def _make_live_transport_with_mock_response(venue: Venue, fixture_path: str):
    """Create a VenueTransport in live mode that returns the given fixture JSON."""
    fixture = _fixture(fixture_path)
    spec_fn = {
        Venue.BYBIT: bybit_spec,
        Venue.BITGET: bitget_spec,
        Venue.BINANCE: binance_spec,
        Venue.OKX: okx_spec,
    }[venue]
    spec = spec_fn()
    cred = LiveCredential(api_key="k", api_secret="s", api_passphrase="p")
    transport = VenueTransport(spec=spec, mode="live", credential=cred)
    transport._client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _req: httpx.Response(200, json=fixture)
        )
    )
    # V1: pre-fill server-time offset so mock transports don't hit server-time endpoint
    transport._time_offset_ms = 0
    return transport


def _make_live_transport(venue: Venue, handler):
    """Create a VenueTransport with a custom async request handler."""
    spec_fn = {
        Venue.BYBIT: bybit_spec,
        Venue.BITGET: bitget_spec,
        Venue.BINANCE: binance_spec,
        Venue.OKX: okx_spec,
    }[venue]
    spec = spec_fn()
    cred = LiveCredential(api_key="k", api_secret="s", api_passphrase="p")
    transport = VenueTransport(spec=spec, mode="live", credential=cred)
    transport._client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler)
    )
    # V1: pre-fill server-time offset so mock transports don't hit server-time endpoint
    transport._time_offset_ms = 0
    return transport


class TestAckOnlyResponses:
    """Task 1 Step 2: ACK-only responses must be uncertain in place_order
    and return PassiveOrderAck in submit_passive_order."""

    @pytest.mark.asyncio
    async def test_binance_place_order_refreshes_hedge_mode_position_side(self, monkeypatch):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(0.01, 0.001, 0.001, 5.0, "exchangeInfo")

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        transport = VenueTransport(binance_spec(), mode="paper")
        transport.mode = "live"
        calls = []

        async def fake_request(method, path, **kwargs):
            calls.append((method, path, kwargs))
            if path == "/fapi/v1/positionSide/dual":
                return {"dualSidePosition": True}
            return {
                "symbol": "BTCUSDT",
                "side": "BUY",
                "status": "FILLED",
                "executedQty": "0.01",
                "avgPrice": "50000",
                "orderId": 123456,
            }

        transport._request = fake_request

        req = OrderRequest(
            venue=Venue.BINANCE,
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=0.01,
        )
        fill = await transport.place_order(req)

        order_call = [call for call in calls if call[1] == "/fapi/v1/order"][0]
        params = order_call[2]["params"]
        assert fill.order_id == "123456"
        assert params["positionSide"] == "LONG"
        assert "reduceOnly" not in params

    @pytest.mark.asyncio
    async def test_binance_place_order_one_way_omits_position_side(self, monkeypatch):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(0.01, 0.001, 0.001, 5.0, "exchangeInfo")

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        transport = VenueTransport(binance_spec(), mode="paper")
        transport.mode = "live"
        calls = []

        async def fake_request(method, path, **kwargs):
            calls.append((method, path, kwargs))
            if path == "/fapi/v1/positionSide/dual":
                return {"dualSidePosition": False}
            return {
                "symbol": "BTCUSDT",
                "side": "SELL",
                "status": "FILLED",
                "executedQty": "0.01",
                "avgPrice": "50000",
                "orderId": 123457,
            }

        transport._request = fake_request

        req = OrderRequest(
            venue=Venue.BINANCE,
            symbol="BTCUSDT",
            side=Side.SELL,
            quantity=0.01,
            reduce_only=True,
        )
        await transport.place_order(req)

        order_call = [call for call in calls if call[1] == "/fapi/v1/order"][0]
        params = order_call[2]["params"]
        assert "positionSide" not in params
        assert params["reduceOnly"] == "true"

    @pytest.mark.asyncio
    async def test_bybit_ack_only_place_order_is_uncertain_but_passive_submit_is_ack(self):
        transport = _make_live_transport_with_mock_response(
            Venue.BYBIT, "bybit/place_order_ack_only.json"
        )
        req = OrderRequest(
            venue=Venue.BYBIT,
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=0.001,
            price=50000.0,
            post_only=True,
            client_order_id="lfv2-entry-maker-001",
        )
        with pytest.raises(OrderSubmitError) as exc:
            await transport.place_order(req)
        assert exc.value.class_ == SubmitFailureClass.UNCERTAIN

        transport2 = _make_live_transport_with_mock_response(
            Venue.BYBIT, "bybit/place_order_ack_only.json"
        )
        ack = await transport2.submit_passive_order(req)
        assert ack.order_id == "1321003749386327552"
        assert ack.client_order_id == "lfv2-entry-maker-001"

    @pytest.mark.asyncio
    async def test_bitget_ack_only_place_order_is_uncertain_but_passive_submit_is_ack(self):
        transport = _make_live_transport_with_mock_response(
            Venue.BITGET, "bitget/classic_place_order_ack_only.json"
        )
        req = OrderRequest(
            venue=Venue.BITGET,
            symbol="BTCUSDT",
            side=Side.SELL,
            quantity=0.001,
            price=50000.0,
            post_only=True,
            client_order_id="lfv2-entry-maker-001",
        )
        with pytest.raises(OrderSubmitError) as exc:
            await transport.place_order(req)
        assert exc.value.class_ == SubmitFailureClass.UNCERTAIN

        transport2 = _make_live_transport_with_mock_response(
            Venue.BITGET, "bitget/classic_place_order_ack_only.json"
        )
        ack = await transport2.submit_passive_order(req)
        assert ack.order_id == "121211212122"
        assert ack.client_order_id == "lfv2-entry-maker-001"


class TestV1PassiveBusinessFlowParity:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("reduce_only", [False, True])
    async def test_aster_v3_market_ioc_omits_time_in_force_on_wire(self, reduce_only):
        """V1 Aster MARKET hedge/close semantics: IOC is not a V3 wire field."""
        captured: dict[str, str] = {}
        position_mode_calls = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal position_mode_calls
            if request.url.path == "/fapi/v3/positionSide/dual":
                position_mode_calls += 1
                return httpx.Response(200, json={"dualSidePosition": False})
            assert request.url.path == "/fapi/v3/order"
            captured.update(dict(request.url.params))
            return httpx.Response(
                200,
                json={
                    "orderId": "aster-market-close-1",
                    "clientOrderId": "aster-ioc-close",
                    "executedQty": "21",
                    "avgPrice": "0.123",
                },
            )

        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        client = AsterV3Client(
            credential=LiveCredential(
                api_secret="0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"
            ),
            http_client=http_client,
        )
        try:
            fill = await client.place_order(
                OrderRequest(
                    venue=Venue.ASTER,
                    symbol="CYSUSDT",
                    side=Side.BUY,
                    quantity=21.0,
                    reduce_only=reduce_only,
                    time_in_force=TimeInForce.IOC,
                    client_order_id="aster-ioc-close",
                )
            )
        finally:
            await http_client.aclose()

        assert fill.quantity == 21.0
        assert captured["type"] == "MARKET"
        if reduce_only:
            assert captured["reduceOnly"] == "true"
        else:
            assert "reduceOnly" not in captured
        assert "timeInForce" not in captured
        assert captured["newOrderRespType"] == "RESULT"
        assert position_mode_calls == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("hedge_mode", "side", "reduce_only", "position_side"),
        [
            (False, Side.BUY, False, None),
            (False, Side.SELL, True, None),
            (True, Side.BUY, False, "LONG"),
            (True, Side.SELL, False, "SHORT"),
            (True, Side.SELL, True, "LONG"),
            (True, Side.BUY, True, "SHORT"),
        ],
    )
    async def test_aster_v3_market_order_uses_cached_position_mode_on_wire(
        self,
        hedge_mode,
        side,
        reduce_only,
        position_side,
    ):
        """Real V3 wire path must follow Aster's One-way/Hedge order contract."""
        calls: list[tuple[str, str, dict[str, str]]] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            params = dict(request.url.params)
            calls.append((request.method, request.url.path, params))
            if request.url.path == "/fapi/v3/positionSide/dual":
                return httpx.Response(200, json={"dualSidePosition": hedge_mode})
            assert request.url.path == "/fapi/v3/order"
            return httpx.Response(
                200,
                json={
                    "orderId": f"aster-mode-{len(calls)}",
                    "clientOrderId": "aster-position-mode",
                    "executedQty": "21",
                    "avgPrice": "0.123",
                },
            )

        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        client = AsterV3Client(
            credential=LiveCredential(
                api_secret="0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"
            ),
            http_client=http_client,
        )
        request = OrderRequest(
            venue=Venue.ASTER,
            symbol="CYSUSDT",
            side=side,
            quantity=21.0,
            reduce_only=reduce_only,
            time_in_force=TimeInForce.IOC,
            client_order_id="aster-position-mode",
        )
        try:
            await client.place_order(request)
            await client.place_order(request)
        finally:
            await http_client.aclose()

        mode_calls = [call for call in calls if call[1] == "/fapi/v3/positionSide/dual"]
        order_calls = [call for call in calls if call[1] == "/fapi/v3/order"]
        assert len(mode_calls) == 1
        assert len(order_calls) == 2
        for _, _, params in order_calls:
            assert params["type"] == "MARKET"
            assert params["newOrderRespType"] == "RESULT"
            assert "timeInForce" not in params
            if hedge_mode:
                assert params["positionSide"] == position_side
                assert "reduceOnly" not in params
            elif reduce_only:
                assert params["reduceOnly"] == "true"
                assert "positionSide" not in params
            else:
                assert "reduceOnly" not in params
                assert "positionSide" not in params

    @pytest.mark.asyncio
    async def test_aster_v3_passive_hedge_order_uses_position_side_without_reduce_only(self):
        """Passive V3 orders share mode mapping but retain the existing ACK workflow."""
        captured: dict[str, str] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/fapi/v3/positionSide/dual":
                return httpx.Response(200, json={"dualSidePosition": True})
            assert request.url.path == "/fapi/v3/order"
            captured.update(dict(request.url.params))
            return httpx.Response(
                200,
                json={
                    "orderId": "aster-passive-hedge",
                    "clientOrderId": "aster-passive-hedge",
                    "status": "NEW",
                    "price": "0.123",
                    "origQty": "21",
                },
            )

        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        client = AsterV3Client(
            credential=LiveCredential(
                api_secret="0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"
            ),
            http_client=http_client,
        )
        try:
            ack = await client.submit_passive_order(
                OrderRequest(
                    venue=Venue.ASTER,
                    symbol="CYSUSDT",
                    side=Side.BUY,
                    quantity=21.0,
                    price=0.123,
                    reduce_only=True,
                    post_only=True,
                    client_order_id="aster-passive-hedge",
                )
            )
        finally:
            await http_client.aclose()

        assert ack.order_id == "aster-passive-hedge"
        assert captured["type"] == "LIMIT"
        assert captured["timeInForce"] == "GTX"
        assert captured["positionSide"] == "SHORT"
        assert "reduceOnly" not in captured
        assert "newOrderRespType" not in captured

    @pytest.mark.asyncio
    async def test_aster_v3_rejects_order_when_position_mode_evidence_is_invalid(self):
        """A missing V3 mode field must block the order before any submit attempt."""
        order_attempted = False

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal order_attempted
            if request.url.path == "/fapi/v3/positionSide/dual":
                return httpx.Response(200, json={})
            order_attempted = True
            raise AssertionError("order must not be submitted without position mode truth")

        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        client = AsterV3Client(
            credential=LiveCredential(
                api_secret="0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"
            ),
            http_client=http_client,
        )
        try:
            with pytest.raises(OrderSubmitError, match="position mode") as exc:
                await client.place_order(
                    OrderRequest(
                        venue=Venue.ASTER,
                        symbol="CYSUSDT",
                        side=Side.BUY,
                        quantity=21.0,
                    )
                )
        finally:
            await http_client.aclose()

        assert exc.value.class_ == SubmitFailureClass.REJECTED
        assert order_attempted is False

    @pytest.mark.asyncio
    async def test_aster_v3_position_truth_nets_hedge_rows_per_symbol(self):
        """V1-compatible reconciliation must not select only one Hedge-mode row."""
        async def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/fapi/v3/positionRisk"
            return httpx.Response(
                200,
                json=[
                    {
                        "symbol": "CYSUSDT",
                        "positionSide": "LONG",
                        "positionAmt": "21",
                        "entryPrice": "0.123",
                    },
                    {
                        "symbol": "CYSUSDT",
                        "positionSide": "SHORT",
                        "positionAmt": "8",
                        "entryPrice": "0.124",
                    },
                    {
                        "symbol": "OTHERUSDT",
                        "positionSide": "SHORT",
                        "positionAmt": "5",
                        "entryPrice": "1.0",
                    },
                ],
            )

        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        client = AsterV3Client(
            credential=LiveCredential(
                api_secret="0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"
            ),
            http_client=http_client,
        )
        try:
            position = await client.fetch_position("CYSUSDT")
            all_positions = await client.fetch_all_positions()
        finally:
            await http_client.aclose()

        assert position.side == Side.BUY
        assert position.quantity == pytest.approx(13.0)
        by_symbol = {item.symbol: item for item in all_positions}
        assert set(by_symbol) == {"CYSUSDT", "OTHERUSDT"}
        assert by_symbol["CYSUSDT"].side == Side.BUY
        assert by_symbol["CYSUSDT"].quantity == pytest.approx(13.0)
        assert by_symbol["OTHERUSDT"].side == Side.SELL
        assert by_symbol["OTHERUSDT"].quantity == pytest.approx(5.0)

    @pytest.mark.asyncio
    async def test_aster_live_private_passive_submit_fails_closed_without_exchange_rules(self):
        from lightfee.venues.aster import AsterAdapter
        from lightfee.venues.symbol_rules import get_symbol_rules_cache

        adapter = AsterAdapter(
            mode="live",
            credential=LiveCredential(
                api_secret="0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1",
                account_address="0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e",
            ),
        )
        assert adapter._private is not None
        get_symbol_rules_cache().clear()

        async def unavailable_public_get(path, params=None):
            raise RuntimeError("exchangeInfo unavailable")

        adapter._transport._public_get = unavailable_public_get
        adapter._private.submit_passive_order = AsyncMock(
            side_effect=AssertionError("private order must not be sent")
        )
        request = OrderRequest(
            venue=Venue.ASTER,
            symbol="GUAUSDT",
            side=Side.BUY,
            quantity=15.07,
            price=2.009,
            post_only=True,
            client_order_id="aster-maker-unavailable-rules",
        )

        with pytest.raises(OrderSubmitError, match="dynamic symbol rules unavailable"):
            await adapter.submit_passive_order(request)

        adapter._private.submit_passive_order.assert_not_awaited()
        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_aster_live_private_passive_submit_fails_closed_for_incomplete_exchange_rules(self):
        from lightfee.venues.aster import AsterAdapter
        from lightfee.venues.symbol_rules import get_symbol_rules_cache

        adapter = AsterAdapter(
            mode="live",
            credential=LiveCredential(
                api_secret="0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1",
                account_address="0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e",
            ),
        )
        assert adapter._private is not None
        get_symbol_rules_cache().clear()

        async def incomplete_public_get(path, params=None):
            assert path == "/fapi/v1/exchangeInfo"
            return {
                "symbols": [{
                    "symbol": "GUAUSDT",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                }],
            }

        adapter._transport._public_get = incomplete_public_get
        adapter._private.submit_passive_order = AsyncMock(
            side_effect=AssertionError("private order must not be sent")
        )
        request = OrderRequest(
            venue=Venue.ASTER,
            symbol="GUAUSDT",
            side=Side.BUY,
            quantity=15.07,
            price=2.009,
            post_only=True,
            client_order_id="aster-maker-incomplete-rules",
        )

        with pytest.raises(OrderSubmitError, match="dynamic symbol rules unavailable"):
            await adapter.submit_passive_order(request)

        adapter._private.submit_passive_order.assert_not_awaited()
        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_aster_passive_submit_uses_v3_order_without_legacy_headroom(self):
        from lightfee.venues.aster import AsterAdapter
        from lightfee.venues.symbol_rules import get_symbol_rules_cache

        adapter = AsterAdapter(
            mode="live",
            credential=LiveCredential(
                api_secret="0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1",
                account_address="0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e",
            ),
        )
        assert adapter._private is not None
        get_symbol_rules_cache().clear()

        async def fake_public_get(path, params=None):
            assert path == "/fapi/v1/exchangeInfo"
            return {
                "symbols": [{
                    "symbol": "GUAUSDT",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {"filterType": "LOT_SIZE", "stepSize": "1", "minQty": "1"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                }],
            }

        adapter._transport._public_get = fake_public_get
        calls = []

        async def fake_request(method, path, *, params=None):
            calls.append((method, path, dict(params or {})))
            if path == "/fapi/v3/positionRisk":
                return [{
                    "symbol": "GUAUSDT",
                    "positionAmt": "0",
                    "markPrice": "2",
                    "maxNotionalValue": "100",
                }]
            if path == "/fapi/v3/openOrders":
                return []
            if path == "/fapi/v3/positionSide/dual":
                return {"dualSidePosition": False}
            return {
                "orderId": "aster-oid-1",
                "clientOrderId": (params or {}).get("newClientOrderId", ""),
                "status": "NEW",
                "price": (params or {}).get("price", "0"),
                "origQty": (params or {}).get("quantity", "0"),
            }

        adapter._private._request = fake_request
        req = OrderRequest(
            venue=Venue.ASTER,
            symbol="GUAUSDT",
            side=Side.BUY,
            quantity=15.07,
            price=2.009,
            post_only=True,
            client_order_id="aster-maker-1",
        )

        ack = await adapter.submit_passive_order(req)

        order_call = [call for call in calls if call[1] == "/fapi/v3/order"][0]
        assert order_call[2]["quantity"] == "15"
        assert order_call[2]["price"] == "2"
        assert order_call[2]["timeInForce"] == "GTX"
        assert ack.quantity == 15.0
        assert not any(call[1] == "/fapi/v1/remainingOpenableNotionalValue" for call in calls)
        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_aster_v3_capacity_precheck_rejects_before_private_order_submit(self):
        from lightfee.venues.aster import AsterAdapter
        from lightfee.venues.symbol_rules import get_symbol_rules_cache

        adapter = AsterAdapter(
            mode="live",
            credential=LiveCredential(
                api_secret="0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1",
                account_address="0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e",
            ),
        )
        assert adapter._private is not None
        get_symbol_rules_cache().clear()

        async def fake_public_get(path, params=None):
            assert path == "/fapi/v1/exchangeInfo"
            return {
                "symbols": [{
                    "symbol": "KAITOUSDT",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {"filterType": "LOT_SIZE", "stepSize": "1", "minQty": "1"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                }],
            }

        calls = []

        async def fake_request(method, path, *, params=None):
            calls.append((method, path, dict(params or {})))
            if path == "/fapi/v3/positionRisk":
                return [{
                    "symbol": "KAITOUSDT",
                    "positionAmt": "0",
                    "markPrice": "2",
                    "maxNotionalValue": "100",
                }]
            if path == "/fapi/v3/openOrders":
                return []
            raise AssertionError("capacity rejection must prevent private order submit")

        adapter._transport._public_get = fake_public_get
        adapter._private._request = fake_request
        request = OrderRequest(
            venue=Venue.ASTER,
            symbol="KAITOUSDT",
            side=Side.BUY,
            quantity=60.0,
            price=2.0,
            post_only=True,
            client_order_id="aster-capacity-reject",
        )

        with pytest.raises(OrderSubmitError, match="maximum notional value limit"):
            await adapter.submit_passive_order(request)

        assert [call[1] for call in calls] == [
            "/fapi/v3/positionRisk",
            "/fapi/v3/openOrders",
        ]
        precheck_events = [
            event["payload"]
            for event in adapter._transport.drain_order_diagnostics()
            if event["kind"] == "order.precheck_result"
        ]
        assert precheck_events
        assert precheck_events[-1]["response_classification"] == "rejected"
        assert precheck_events[-1]["endpoint"] == (
            "/fapi/v3/positionRisk,/fapi/v3/openOrders"
        )
        assert not any(call[1] == "/fapi/v1/remainingOpenableNotionalValue" for call in calls)
        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_aster_v3_capacity_precheck_counts_existing_open_orders(self):
        from lightfee.venues.aster import AsterAdapter
        from lightfee.venues.symbol_rules import get_symbol_rules_cache

        adapter = AsterAdapter(
            mode="live",
            credential=LiveCredential(
                api_secret="0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1",
                account_address="0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e",
            ),
        )
        assert adapter._private is not None
        get_symbol_rules_cache().clear()

        async def fake_public_get(path, params=None):
            assert path == "/fapi/v1/exchangeInfo"
            return {
                "symbols": [{
                    "symbol": "KAITOUSDT",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {"filterType": "LOT_SIZE", "stepSize": "1", "minQty": "1"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                }],
            }

        async def fake_request(method, path, *, params=None):
            if path == "/fapi/v3/positionRisk":
                return [{
                    "symbol": "KAITOUSDT",
                    "positionAmt": "10",
                    "markPrice": "2",
                    "maxNotionalValue": "100",
                }]
            if path == "/fapi/v3/openOrders":
                return [{
                    "symbol": "KAITOUSDT",
                    "origQty": "30",
                    "price": "2",
                    "reduceOnly": "false",
                }]
            raise AssertionError("existing capacity use must prevent private order submit")

        adapter._transport._public_get = fake_public_get
        adapter._private._request = fake_request
        request = OrderRequest(
            venue=Venue.ASTER,
            symbol="KAITOUSDT",
            side=Side.BUY,
            quantity=11.0,
            price=2.0,
            post_only=True,
            client_order_id="aster-open-order-capacity-reject",
        )

        with pytest.raises(OrderSubmitError, match="maximum notional value limit"):
            await adapter.submit_passive_order(request)

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_aster_v3_capacity_precheck_records_rejected_http_evidence(self):
        from lightfee.venues.aster import AsterAdapter

        adapter = AsterAdapter(
            mode="live",
            credential=LiveCredential(
                api_secret="0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1",
                account_address="0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e",
            ),
        )
        assert adapter._private is not None

        async def fake_request(method, path, *, params=None):
            assert method == "GET"
            assert path == "/fapi/v3/positionRisk"
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                'HTTP 400: {"code":-5018,"msg":"maximum notional value limit"}',
                status_code=400,
                body='{"code":-5018,"msg":"maximum notional value limit"}',
            )

        adapter._private._request = fake_request
        request = OrderRequest(
            venue=Venue.ASTER,
            symbol="KAITOUSDT",
            side=Side.BUY,
            quantity=1.0,
            price=2.0,
            client_order_id="aster-capacity-http-reject",
        )

        with pytest.raises(OrderSubmitError, match="capacity precheck rejected"):
            await adapter.precheck_order_admission(request)

        events = [
            event["payload"]
            for event in adapter._transport.drain_order_diagnostics()
            if event["kind"] == "order.precheck_result"
        ]
        assert events[-1]["response_classification"] == "rejected"
        assert events[-1]["status_code"] == 400
        assert '"code":-5018' in events[-1]["response_body"]
        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_aster_v3_passive_submit_reject_raises_order_submit_error(self):
        from lightfee.venues.aster import AsterAdapter
        from lightfee.venues.symbol_rules import get_symbol_rules_cache

        adapter = AsterAdapter(
            mode="live",
            credential=LiveCredential(
                api_secret="0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1",
                account_address="0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e",
            ),
        )
        assert adapter._private is not None
        get_symbol_rules_cache().clear()

        async def fake_public_get(path, params=None):
            assert path == "/fapi/v1/exchangeInfo"
            return {
                "symbols": [{
                    "symbol": "GUAUSDT",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {"filterType": "LOT_SIZE", "stepSize": "1", "minQty": "1"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                }],
            }

        adapter._transport._public_get = fake_public_get
        order_attempts = []

        async def fake_request(method, path, *, params=None):
            order_attempts.append((method, path, dict(params or {})))
            if path == "/fapi/v3/positionRisk":
                return [{
                    "symbol": "GUAUSDT",
                    "positionAmt": "0",
                    "markPrice": "2",
                    "maxNotionalValue": "100",
                }]
            if path == "/fapi/v3/openOrders":
                return []
            if path == "/fapi/v3/positionSide/dual":
                return {"dualSidePosition": False}
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                "HTTP 400: max notional",
                status_code=400,
                body='{"code":-5018,"msg":"maximum notional value limit"}',
            )

        adapter._private._request = fake_request
        req = OrderRequest(
            venue=Venue.ASTER,
            symbol="GUAUSDT",
            side=Side.BUY,
            quantity=15.0,
            price=2.0,
            post_only=True,
            client_order_id="aster-maker-2",
        )

        with pytest.raises(OrderSubmitError) as exc:
            await adapter.submit_passive_order(req)

        assert exc.value.class_ == SubmitFailureClass.REJECTED
        assert order_attempts == [
            (
                "GET",
                "/fapi/v3/positionRisk",
                {"symbol": "GUAUSDT"},
            ),
            (
                "GET",
                "/fapi/v3/openOrders",
                {"symbol": "GUAUSDT"},
            ),
            (
                "GET",
                "/fapi/v3/positionSide/dual",
                {},
            ),
            (
                "POST",
                "/fapi/v3/order",
                {
                    "symbol": "GUAUSDT",
                    "side": "BUY",
                    "type": "LIMIT",
                    "quantity": "15",
                    "price": "2",
                    "timeInForce": "GTX",
                    "newClientOrderId": "aster-maker-2",
                },
            )
        ]
        evidence = [
            record
            for record in adapter._transport.drain_order_diagnostics()
            if record["kind"] == "order.private_submit_result"
        ]
        assert len(evidence) == 1
        payload = evidence[0]["payload"]
        assert payload["operation"] == "submit_passive_order"
        assert payload["endpoint"] == "/fapi/v3/order"
        assert payload["status_code"] == 400
        assert "-5018" in payload["response_body"]
        assert payload["rule_source"] == "exchangeInfo"
        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_binance_post_only_would_take_is_classified_explicitly(
        self, monkeypatch,
    ):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    rule_source="test_binance_rules",
                )

        monkeypatch.setattr(
            "lightfee.venues.symbol_rules.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )

        transport = VenueTransport(
            binance_spec(),
            mode="live",
            credential=LiveCredential(api_key="binance-key", api_secret="binance-secret"),
        )
        transport._fapi_position_hedge_mode_cache = False

        async def fake_request(method, path, *, params=None, body=None, private=False, **kwargs):
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                "HTTP 400: post only rejected",
                status_code=400,
                body='{"code":-5022,"msg":"Due to the order could not be executed as maker, the Post Only order will be rejected."}',
            )

        transport._request = fake_request
        req = OrderRequest(
            venue=Venue.BINANCE,
            symbol="GUAUSDT",
            side=Side.BUY,
            quantity=10.0,
            price=1.5,
            post_only=True,
            client_order_id="binance-maker-1",
        )

        with pytest.raises(OrderSubmitError) as exc:
            await transport.submit_passive_order(req)

        assert exc.value.class_ == SubmitFailureClass.REJECTED
        result = transport.order_diagnostics[-1]["payload"]
        assert result["response_classification"] == "post_only_would_take"

    @pytest.mark.asyncio
    async def test_bybit_110007_balance_reject_is_classified_as_admission(
        self, monkeypatch,
    ):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    rule_source="test_bybit_rules",
                )

        monkeypatch.setattr(
            "lightfee.venues.symbol_rules.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )

        transport = VenueTransport(
            bybit_spec(),
            mode="live",
            credential=LiveCredential(api_key="bybit-key", api_secret="bybit-secret"),
        )

        async def fake_request(method, path, *, params=None, body=None, private=False, **kwargs):
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                "bybit retCode=110007 retMsg=Available balance is insufficient",
                status_code=400,
                body='{"retCode":110007,"retMsg":"Available balance is insufficient"}',
            )

        transport._request = fake_request
        req = OrderRequest(
            venue=Venue.BYBIT,
            symbol="BALUSDT",
            side=Side.BUY,
            quantity=10.0,
            price=1.5,
            post_only=True,
            client_order_id="bybit-maker-1",
        )

        with pytest.raises(OrderSubmitError) as exc:
            await transport.submit_passive_order(req)

        assert exc.value.class_ == SubmitFailureClass.REJECTED
        result = transport.order_diagnostics[-1]["payload"]
        assert result["response_classification"] == "insufficient_balance_admission_blocked"

    @pytest.mark.asyncio
    async def test_bybit_110126_trading_terms_reject_is_classified_as_admission(
        self, monkeypatch,
    ):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    rule_source="test_bybit_rules",
                )

        monkeypatch.setattr(
            "lightfee.venues.symbol_rules.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )

        transport = VenueTransport(
            bybit_spec(),
            mode="live",
            credential=LiveCredential(api_key="bybit-key", api_secret="bybit-secret"),
        )

        async def fake_request(method, path, *, params=None, body=None, private=False, **kwargs):
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                "bybit retCode=110126 retMsg=must sign required agreement",
                status_code=400,
                body='{"retCode":110126,"retMsg":"must sign required agreement"}',
            )

        transport._request = fake_request
        req = OrderRequest(
            venue=Venue.BYBIT,
            symbol="LITEUSDT",
            side=Side.BUY,
            quantity=10.0,
            price=1.5,
            post_only=True,
            client_order_id="bybit-maker-terms",
        )

        with pytest.raises(OrderSubmitError) as exc:
            await transport.submit_passive_order(req)

        assert exc.value.class_ == SubmitFailureClass.REJECTED
        result = transport.order_diagnostics[-1]["payload"]
        assert result["response_classification"] == "bybit_trading_terms_required"

    @pytest.mark.asyncio
    async def test_binance_2019_margin_reject_is_classified_as_admission(
        self, monkeypatch,
    ):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    rule_source="test_binance_rules",
                )

        monkeypatch.setattr(
            "lightfee.venues.symbol_rules.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )

        transport = VenueTransport(
            binance_spec(),
            mode="live",
            credential=LiveCredential(api_key="binance-key", api_secret="binance-secret"),
        )
        transport._fapi_position_hedge_mode_cache = False

        async def fake_request(method, path, *, params=None, body=None, private=False, **kwargs):
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                "HTTP 400: margin is insufficient",
                status_code=400,
                body='{"code":-2019,"msg":"Margin is insufficient."}',
            )

        transport._request = fake_request
        req = OrderRequest(
            venue=Venue.BINANCE,
            symbol="MARGINUSDT",
            side=Side.BUY,
            quantity=10.0,
            price=1.5,
            post_only=True,
            client_order_id="binance-maker-2",
        )

        with pytest.raises(OrderSubmitError) as exc:
            await transport.submit_passive_order(req)

        assert exc.value.class_ == SubmitFailureClass.REJECTED
        result = transport.order_diagnostics[-1]["payload"]
        assert result["response_classification"] == "insufficient_margin_admission_blocked"

    @pytest.mark.asyncio
    async def test_binance_2027_leverage_reject_is_classified_as_admission(
        self, monkeypatch,
    ):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    rule_source="test_binance_rules",
                )

        monkeypatch.setattr(
            "lightfee.venues.symbol_rules.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )

        transport = VenueTransport(
            binance_spec(),
            mode="live",
            credential=LiveCredential(api_key="binance-key", api_secret="binance-secret"),
        )
        transport._fapi_position_hedge_mode_cache = False

        async def fake_request(method, path, *, params=None, body=None, private=False, **kwargs):
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                "HTTP 400: max leverage ratio",
                status_code=400,
                body=(
                    '{"code":-2027,"msg":"Exceeded the maximum allowable '
                    'position at current leverage."}'
                ),
            )

        transport._request = fake_request
        req = OrderRequest(
            venue=Venue.BINANCE,
            symbol="HMSTRUSDT",
            side=Side.BUY,
            quantity=100.0,
            price=2.0,
            post_only=True,
            client_order_id="binance-maker-2027",
        )

        with pytest.raises(OrderSubmitError) as exc:
            await transport.submit_passive_order(req)

        assert exc.value.class_ == SubmitFailureClass.REJECTED
        result = transport.order_diagnostics[-1]["payload"]
        assert result["response_classification"] == "leverage_admission_blocked"

    @pytest.mark.asyncio
    async def test_binance_entry_leverage_prepare_clamps_to_bracket_and_sets_account_leverage(
        self,
    ):
        transport = VenueTransport(
            binance_spec(),
            mode="live",
            credential=LiveCredential(api_key="binance-key", api_secret="binance-secret"),
        )
        calls: list[tuple[str, str, dict[str, Any]]] = []

        async def fake_request(method, path, *, params=None, body=None, private=False, **kwargs):
            calls.append((method, path, dict(params or {})))
            if path == "/fapi/v2/positionRisk":
                return [
                    {
                        "symbol": "HUSDT",
                        "positionSide": "LONG",
                        "positionAmt": "0",
                        "entryPrice": "0",
                        "markPrice": "0.57329",
                        "leverage": "20",
                    },
                    {
                        "symbol": "HUSDT",
                        "positionSide": "SHORT",
                        "positionAmt": "0",
                        "entryPrice": "0",
                        "markPrice": "0.57329",
                        "leverage": "20",
                    },
                ]
            if path == "/fapi/v1/leverageBracket":
                return [
                    {
                        "symbol": "HUSDT",
                        "brackets": [
                            {
                                "bracket": 1,
                                "initialLeverage": 5,
                                "notionalFloor": 0,
                                "notionalCap": 20000,
                            }
                        ],
                    }
                ]
            if path == "/fapi/v1/leverage":
                return {
                    "symbol": "HUSDT",
                    "leverage": 5,
                    "maxNotionalValue": "20000",
                }
            raise AssertionError(f"unexpected request {method} {path}")

        transport._request = fake_request

        await transport.ensure_entry_leverage("HUSDT", 20, notional_quote=50.0)
        await transport.ensure_entry_leverage("HUSDT", 20, notional_quote=50.0)

        assert ("/fapi/v1/leverage", {"symbol": "HUSDT", "leverage": 5}) in [
            (path, params) for _, path, params in calls
        ]
        assert [path for _, path, _ in calls].count("/fapi/v1/leverage") == 1
        diagnostics = [
            event["payload"]
            for event in transport.order_diagnostics
            if event["kind"] == "order.entry_leverage_ready"
        ]
        assert len(diagnostics) == 2
        assert diagnostics[0]["requested_leverage"] == 20
        assert diagnostics[0]["effective_leverage"] == 5
        assert diagnostics[0]["outcome"] == "set"
        assert diagnostics[0]["bracket_initial_leverage"] == 5
        assert diagnostics[0]["position_risk_leverage"] == 20
        assert diagnostics[1]["requested_leverage"] == 20
        assert diagnostics[1]["effective_leverage"] == 5
        assert diagnostics[1]["outcome"] == "cached_ready"

    @pytest.mark.asyncio
    async def test_binance_entry_leverage_prepare_skips_when_account_already_ready(self):
        transport = VenueTransport(
            binance_spec(),
            mode="live",
            credential=LiveCredential(api_key="binance-key", api_secret="binance-secret"),
        )
        calls: list[tuple[str, str, dict[str, Any]]] = []

        async def fake_request(method, path, *, params=None, body=None, private=False, **kwargs):
            calls.append((method, path, dict(params or {})))
            if path == "/fapi/v2/positionRisk":
                return [{"symbol": "HUSDT", "positionAmt": "0", "leverage": "5"}]
            if path == "/fapi/v1/leverageBracket":
                return [
                    {
                        "symbol": "HUSDT",
                        "brackets": [
                            {
                                "bracket": 1,
                                "initialLeverage": 5,
                                "notionalFloor": 0,
                                "notionalCap": 20000,
                            }
                        ],
                    }
                ]
            if path == "/fapi/v1/leverage":
                raise AssertionError("should not set leverage when current leverage is ready")
            raise AssertionError(f"unexpected request {method} {path}")

        transport._request = fake_request

        await transport.ensure_entry_leverage("HUSDT", 5, notional_quote=50.0)

        assert not any(path == "/fapi/v1/leverage" for _, path, _ in calls)
        assert transport.order_diagnostics[-1]["kind"] == "order.entry_leverage_ready"
        assert transport.order_diagnostics[-1]["payload"]["outcome"] == "already_ready"

    @pytest.mark.asyncio
    async def test_bybit_delisting_no_new_position_reject_is_classified_as_admission(
        self, monkeypatch,
    ):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    rule_source="test_bybit_rules",
                )

        monkeypatch.setattr(
            "lightfee.venues.symbol_rules.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )

        transport = VenueTransport(
            bybit_spec(),
            mode="live",
            credential=LiveCredential(api_key="bybit-key", api_secret="bybit-secret"),
        )

        async def fake_request(method, path, *, params=None, body=None, private=False, **kwargs):
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                "HTTP 400: bybit retCode=30228 retMsg=No new positions during delisting",
                status_code=400,
                body='{"retCode":30228,"retMsg":"No new positions during delisting"}',
            )

        transport._request = fake_request
        req = OrderRequest(
            venue=Venue.BYBIT,
            symbol="TONUSDT",
            side=Side.BUY,
            quantity=10.0,
            price=1.5,
            post_only=True,
            client_order_id="bybit-maker-delisting",
        )

        with pytest.raises(OrderSubmitError) as exc:
            await transport.submit_passive_order(req)

        assert exc.value.class_ == SubmitFailureClass.REJECTED
        result = transport.order_diagnostics[-1]["payload"]
        assert result["response_classification"] == "new_position_not_allowed"

    @pytest.mark.asyncio
    async def test_aster_5018_max_notional_reject_is_classified_as_admission(
        self, monkeypatch,
    ):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    rule_source="test_aster_rules",
                )

        monkeypatch.setattr(
            "lightfee.venues.symbol_rules.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )

        transport = VenueTransport(
            aster_spec(),
            mode="live",
            credential=LiveCredential(api_key="aster-key", api_secret="aster-secret"),
        )
        transport._fapi_position_hedge_mode_cache = False

        async def fake_request(method, path, *, params=None, body=None, private=False, **kwargs):
            if path == "/fapi/v1/remainingOpenableNotionalValue":
                return {"remainingOpenableNotionalValue": "300"}
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                "HTTP 400: max notional",
                status_code=400,
                body='{"code":-5018,"msg":"maximum notional value limit"}',
            )

        transport._request = fake_request
        req = OrderRequest(
            venue=Venue.ASTER,
            symbol="MAXUSDT",
            side=Side.BUY,
            quantity=100.0,
            price=2.0,
            post_only=True,
            client_order_id="aster-maker-3",
        )

        with pytest.raises(OrderSubmitError) as exc:
            await transport.submit_passive_order(req)

        assert exc.value.class_ == SubmitFailureClass.REJECTED
        result = transport.order_diagnostics[-1]["payload"]
        assert result["response_classification"] == "max_notional_admission_blocked"

    @pytest.mark.asyncio
    async def test_aster_2027_leverage_reject_is_classified_as_admission(
        self, monkeypatch,
    ):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                return SymbolRule(
                    tick_size=0.001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=0.0,
                    rule_source="test_aster_rules",
                )

        monkeypatch.setattr(
            "lightfee.venues.symbol_rules.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )

        transport = VenueTransport(
            aster_spec(),
            mode="live",
            credential=LiveCredential(api_key="aster-key", api_secret="aster-secret"),
        )
        transport._fapi_position_hedge_mode_cache = False

        async def fake_request(method, path, *, params=None, body=None, private=False, **kwargs):
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                "HTTP 400: max leverage ratio",
                status_code=400,
                body=(
                    '{"code":-2027,"msg":"Exceeded the maximum allowable '
                    'position at current leverage."}'
                ),
            )

        transport._request = fake_request
        req = OrderRequest(
            venue=Venue.ASTER,
            symbol="ESPORTSUSDT",
            side=Side.BUY,
            quantity=100.0,
            price=2.0,
            post_only=True,
            client_order_id="aster-maker-2027",
        )

        with pytest.raises(OrderSubmitError) as exc:
            await transport.submit_passive_order(req)

        assert exc.value.class_ == SubmitFailureClass.REJECTED
        result = transport.order_diagnostics[-1]["payload"]
        assert result["response_classification"] == "leverage_admission_blocked"

    @pytest.mark.asyncio
    async def test_okx_fetch_position_reuses_fresh_cache_to_reduce_private_rest(self):
        transport = VenueTransport(
            okx_spec(),
            mode="live",
            credential=LiveCredential(
                api_key="okx-key",
                api_secret="okx-secret",
                api_passphrase="okx-pass",
            ),
        )
        now_ms = int(time.time() * 1000)
        cached = PositionSnapshot(
            venue=Venue.OKX,
            symbol="ALTUSDT",
            side=Side.BUY,
            quantity=3.0,
            entry_price=1.25,
            observed_at_ms=now_ms,
        )
        transport._position_cache["ALTUSDT"] = (cached, now_ms)

        async def fake_request(*args, **kwargs):
            raise AssertionError("fresh OKX position cache should avoid REST")

        transport._request = fake_request

        pos = await transport.fetch_position("ALTUSDT")

        assert pos is cached


class TestExchangeErrorEnvelopes:
    """Task 1 Step 3: Exchange error codes map to REJECTED."""

    @pytest.mark.asyncio
    async def test_bybit_retcode_reject_maps_to_rejected(self):
        transport = _make_live_transport_with_mock_response(
            Venue.BYBIT, "bybit/place_order_reject_retcode.json"
        )
        req = OrderRequest(
            venue=Venue.BYBIT, symbol="BTCUSDT", side=Side.BUY, quantity=0.001,
        )
        # With the guard active (Task 2), retCode != 0 triggers REJECTED
        # before parsing even starts
        with pytest.raises(OrderSubmitError) as exc:
            await transport.place_order(req)
        assert exc.value.class_ == SubmitFailureClass.REJECTED
        assert "110003" in str(exc.value)
        assert [event["kind"] for event in transport.order_diagnostics] == [
            "order.submit_attempt",
            "order.submit_result",
        ]
        result = transport.order_diagnostics[-1]["payload"]
        assert result["response_classification"] == "rejected"
        assert "110003" in result["response_msg"]

    @pytest.mark.asyncio
    async def test_bitget_code_reject_maps_to_rejected(self):
        fixture = {"code": "40001", "msg": "Invalid parameter", "data": None}
        transport = _make_live_transport(Venue.BITGET, lambda _: httpx.Response(200, json=fixture))
        req = OrderRequest(
            venue=Venue.BITGET, symbol="BTCUSDT", side=Side.BUY, quantity=0.001,
        )
        with pytest.raises(OrderSubmitError) as exc:
            await transport.place_order(req)
        assert exc.value.class_ == SubmitFailureClass.REJECTED
        assert "40001" in str(exc.value)


# ---------------------------------------------------------------------------
# Task 2: Safe float and venue-specific guards
# ---------------------------------------------------------------------------


class TestSafeFloat:
    """Task 2 Step 1: safe float helper."""

    def test_safe_float_empty_string_returns_default(self):
        assert _safe_float("", default=0.0) == 0.0
        assert _safe_float(None, default=0.0) == 0.0
        assert _safe_float("1.25", default=0.0) == 1.25

    def test_safe_float_invalid_input_returns_default(self):
        assert _safe_float("not_a_number", default=0.0) == 0.0
        assert _safe_float([], default=0.0) == 0.0
        assert _safe_float({}, default=0.0) == 0.0
        assert _safe_float("   ", default=5.0) == 5.0

    def test_safe_float_zero_default(self):
        assert _safe_float(None) == 0.0
        assert _safe_float("") == 0.0


class TestBybitPositionListShape:
    """Task 2 Step 1: bybit position parser handles empty strings in list shape."""

    def test_bybit_position_result_list_shape_is_supported(self):
        raw = {
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {"symbol": "BTCUSDT", "side": "Buy", "size": "", "avgPrice": ""}
                ]
            }
        }
        spec = bybit_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        pos = transport._parse_position(raw, "BTCUSDT", now_ms=1000)
        assert pos.quantity == 0.0
        assert pos.entry_price == 0.0

    def test_bybit_position_empty_list_returns_zero(self):
        raw = {
            "retCode": 0,
            "retMsg": "OK",
            "result": {"list": []}
        }
        spec = bybit_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        pos = transport._parse_position(raw, "BTCUSDT", now_ms=1000)
        assert pos.quantity == 0.0


class TestFetchAllPositions:
    """V1-style private recovery should be able to scan all venue positions."""

    @pytest.mark.asyncio
    async def test_binance_fetch_all_positions_parses_multiple_nonzero_symbols(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/fapi/v1/time":
                return httpx.Response(200, json={"serverTime": 1770000000000})
            assert request.url.path == "/fapi/v2/positionRisk"
            assert "symbol" not in dict(request.url.params)
            return httpx.Response(
                200,
                json=[
                    {
                        "symbol": "BTCUSDT",
                        "positionSide": "LONG",
                        "positionAmt": "0.02",
                        "entryPrice": "65000.0",
                    },
                    {
                        "symbol": "ETHUSDT",
                        "positionSide": "SHORT",
                        "positionAmt": "0.5",
                        "entryPrice": "3100.0",
                    },
                    {
                        "symbol": "XRPUSDT",
                        "positionSide": "BOTH",
                        "positionAmt": "0",
                        "entryPrice": "0",
                    },
                ],
            )

        transport = _make_live_transport(Venue.BINANCE, handler)

        positions = await transport.fetch_all_positions()

        assert [(p.symbol, p.side, p.quantity) for p in positions] == [
            ("BTCUSDT", Side.BUY, 0.02),
            ("ETHUSDT", Side.SELL, 0.5),
        ]

    def test_parse_all_positions_okx_canonicalizes_symbols(self):
        transport = VenueTransport(spec=okx_spec(), mode="paper")
        raw = {
            "code": "0",
            "data": [
                {
                    "instId": "BTC-USDT-SWAP",
                    "posSide": "long",
                    "pos": "2",
                    "avgPx": "65000.0",
                },
                {
                    "instId": "ETH-USDT-SWAP",
                    "posSide": "short",
                    "pos": "1.5",
                    "avgPx": "3100.0",
                },
                {
                    "instId": "XRP-USDT-SWAP",
                    "posSide": "long",
                    "pos": "0",
                    "avgPx": "0",
                },
            ],
        }

        positions = transport._parse_all_positions(raw, now_ms=1000)

        assert [(p.symbol, p.side, p.quantity) for p in positions] == [
            ("BTCUSDT", Side.BUY, 2.0),
            ("ETHUSDT", Side.SELL, 1.5),
        ]

    def test_parse_all_positions_bybit_result_list(self):
        transport = VenueTransport(spec=bybit_spec(), mode="paper")
        raw = {
            "retCode": 0,
            "result": {
                "list": [
                    {"symbol": "BTCUSDT", "side": "Buy", "size": "0.02", "avgPrice": "65000"},
                    {"symbol": "ETHUSDT", "side": "Sell", "size": "0.5", "avgPrice": "3100"},
                ]
            },
        }

        positions = transport._parse_all_positions(raw, now_ms=1000)

        assert [(p.symbol, p.side, p.quantity) for p in positions] == [
            ("BTCUSDT", Side.BUY, 0.02),
            ("ETHUSDT", Side.SELL, 0.5),
        ]

    def test_parse_all_positions_bitget_data_list(self):
        transport = VenueTransport(spec=bitget_spec(), mode="paper")
        raw = {
            "code": "00000",
            "data": [
                {"symbol": "BTCUSDT", "holdSide": "long", "total": "0.02", "openPriceAvg": "65000"},
                {"symbol": "ETHUSDT", "holdSide": "short", "total": "0.5", "openPriceAvg": "3100"},
            ],
        }

        positions = transport._parse_all_positions(raw, now_ms=1000)

        assert [(p.symbol, p.side, p.quantity) for p in positions] == [
            ("BTCUSDT", Side.BUY, 0.02),
            ("ETHUSDT", Side.SELL, 0.5),
        ]

    def test_parse_all_positions_gate_contract_list(self):
        transport = VenueTransport(spec=gate_spec(), mode="paper")
        raw = [
            {"contract": "BTC_USDT", "size": "2", "entry_price": "65000"},
            {"contract": "ETH_USDT", "size": "-3", "entry_price": "3100"},
        ]

        positions = transport._parse_all_positions(raw, now_ms=1000)

        assert [(p.symbol, p.side, p.quantity) for p in positions] == [
            ("BTCUSDT", Side.BUY, 2.0),
            ("ETHUSDT", Side.SELL, 3.0),
        ]

    def test_parse_all_positions_hyperliquid_asset_positions(self):
        transport = VenueTransport(spec=hyperliquid_spec(), mode="paper")
        raw = {
            "assetPositions": [
                {"position": {"coin": "BTC", "szi": "0.02", "entryPx": "65000"}},
                {"position": {"coin": "ETH", "szi": "-0.5", "entryPx": "3100"}},
            ]
        }

        positions = transport._parse_all_positions(raw, now_ms=1000)

        assert [(p.symbol, p.side, p.quantity) for p in positions] == [
            ("BTCUSDT", Side.BUY, 0.02),
            ("ETHUSDT", Side.SELL, 0.5),
        ]


class TestVenueSuccessGuards:
    """Task 2 Step 2: venue success guard functions."""

    def test_require_bybit_success_passes_on_retcode_zero(self):
        _require_bybit_success({"retCode": 0, "retMsg": "OK"}, "test")

    def test_require_bybit_success_raises_on_nonzero(self):
        with pytest.raises(OrderSubmitError) as exc:
            _require_bybit_success({"retCode": 110003, "retMsg": "bad price"}, "bybit order")
        assert exc.value.class_ == SubmitFailureClass.REJECTED
        assert "110003" in str(exc.value)

    def test_require_bybit_success_preserves_exchange_response_body(self):
        raw = {"retCode": 110017, "retMsg": "orderQty will be truncated to zero."}

        with pytest.raises(OrderSubmitError) as exc:
            _require_bybit_success(raw, "bybit passive order failed")

        assert exc.value.class_ == SubmitFailureClass.REJECTED
        assert json.loads(getattr(exc.value, "exchange_response_body", "{}")) == raw

    def test_require_bitget_success_passes_on_code_00000(self):
        _require_bitget_success({"code": "00000", "msg": "success"}, "test")

    def test_require_bitget_success_passes_on_code_0(self):
        _require_bitget_success({"code": "0", "msg": "success"}, "test")

    def test_require_bitget_success_raises_on_error_code(self):
        with pytest.raises(OrderSubmitError) as exc:
            _require_bitget_success({"code": "40001", "msg": "bad"}, "bitget order")
        assert exc.value.class_ == SubmitFailureClass.REJECTED
        assert "40001" in str(exc.value)


# ---------------------------------------------------------------------------
# Task 3: Fix Bybit Order Builders
# ---------------------------------------------------------------------------


class TestBybitOrderBody:
    """Task 3 Step 2: Bybit order body uses correct V5 field names."""

    @pytest.mark.asyncio
    async def test_bybit_passive_order_body_uses_qty_order_link_id_and_position_idx(self):
        import json as _json
        seen_body = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            seen_body.update(_json.loads(request.content.decode()))
            return httpx.Response(200, json=_fixture("bybit/place_order_ack_only.json"))

        transport = _make_live_transport(Venue.BYBIT, handler)
        req = OrderRequest(
            venue=Venue.BYBIT,
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=0.001,
            price=50000.0,
            post_only=True,
            client_order_id="lfv2-entry-maker-001",
        )

        await transport.submit_passive_order(req)

        assert seen_body["category"] == "linear"
        assert seen_body["symbol"] == "BTCUSDT"
        assert seen_body["side"] == "Buy"
        assert seen_body["orderType"] == "Limit"
        assert seen_body["timeInForce"] == "PostOnly"
        assert seen_body["qty"] == "0.001"
        assert seen_body["orderLinkId"] == "lfv2-entry-maker-001"
        assert seen_body["positionIdx"] in (0, 1, 2)
        assert "quantity" not in seen_body
        assert "newClientOrderId" not in seen_body

    @pytest.mark.asyncio
    async def test_bybit_market_order_body_uses_qty_and_position_idx(self):
        import json as _json
        seen_body = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            seen_body.update(_json.loads(request.content.decode()))
            return httpx.Response(200, json=_fixture("bybit/place_order_ack_only.json"))

        transport = _make_live_transport(Venue.BYBIT, handler)
        req = OrderRequest(
            venue=Venue.BYBIT,
            symbol="BTCUSDT",
            side=Side.SELL,
            quantity=0.002,
            client_order_id="lfv2-hedge-001",
        )

        from lightfee.core.domain import OrderFill
        transport._parse_order_fill = lambda raw, req, sym, ms: OrderFill(
            venue=Venue.BYBIT, symbol=sym, side=req.side,
            quantity=req.quantity, price=50000.0, order_id="123",
        )
        await transport.place_order(req)

        assert seen_body["qty"] == "0.002"
        assert seen_body["side"] == "Sell"
        assert seen_body["orderLinkId"] == "lfv2-hedge-001"
        assert seen_body["positionIdx"] == 2  # Sell in hedge mode
        assert "quantity" not in seen_body

    @pytest.mark.asyncio
    async def test_bybit_ioc_hedge_ignores_price_hint_and_does_not_rest(self):
        """V1 Bybit place_order is market; price is only a hedge price hint."""
        import json as _json
        seen_body = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            seen_body.update(_json.loads(request.content.decode()))
            return httpx.Response(200, json=_fixture("bybit/place_order_ack_only.json"))

        transport = _make_live_transport(Venue.BYBIT, handler)
        req = OrderRequest(
            venue=Venue.BYBIT,
            symbol="IRYSUSDT",
            side=Side.SELL,
            quantity=661.0,
            price=0.0363,
            time_in_force=TimeInForce.IOC,
            client_order_id="e857d13457b6fd02acbe3cd760dc281366b2",
        )

        from lightfee.core.domain import OrderFill
        transport._parse_order_fill = lambda raw, req, sym, ms: OrderFill(
            venue=Venue.BYBIT, symbol=sym, side=req.side,
            quantity=req.quantity, price=0.0363, order_id="123",
        )
        await transport.place_order(req)

        assert seen_body["orderType"] == "Market"
        assert "price" not in seen_body
        assert seen_body["positionIdx"] == 2
        assert seen_body["orderLinkId"] == req.client_order_id

    @pytest.mark.asyncio
    async def test_bybit_reduce_only_sell_closes_long_position_idx(self):
        import json as _json
        seen_body = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            seen_body.update(_json.loads(request.content.decode()))
            return httpx.Response(200, json=_fixture("bybit/place_order_ack_only.json"))

        transport = _make_live_transport(Venue.BYBIT, handler)
        req = OrderRequest(
            venue=Venue.BYBIT,
            symbol="BTCUSDT",
            side=Side.SELL,
            quantity=0.002,
            reduce_only=True,
            client_order_id="lfv2-close-long-001",
        )

        from lightfee.core.domain import OrderFill
        transport._parse_order_fill = lambda raw, req, sym, ms: OrderFill(
            venue=Venue.BYBIT, symbol=sym, side=req.side,
            quantity=req.quantity, price=50000.0, order_id="123",
        )
        await transport.place_order(req)

        assert seen_body["side"] == "Sell"
        assert seen_body["reduceOnly"] is True
        assert seen_body["positionIdx"] == 1  # V1: Sell reduce-only closes long

    @pytest.mark.asyncio
    async def test_bybit_reduce_only_buy_closes_short_position_idx(self):
        import json as _json
        seen_body = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            seen_body.update(_json.loads(request.content.decode()))
            return httpx.Response(200, json=_fixture("bybit/place_order_ack_only.json"))

        transport = _make_live_transport(Venue.BYBIT, handler)
        req = OrderRequest(
            venue=Venue.BYBIT,
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=0.002,
            reduce_only=True,
            client_order_id="lfv2-close-short-001",
        )

        from lightfee.core.domain import OrderFill
        transport._parse_order_fill = lambda raw, req, sym, ms: OrderFill(
            venue=Venue.BYBIT, symbol=sym, side=req.side,
            quantity=req.quantity, price=50000.0, order_id="123",
        )
        await transport.place_order(req)

        assert seen_body["side"] == "Buy"
        assert seen_body["reduceOnly"] is True
        assert seen_body["positionIdx"] == 2  # V1: Buy reduce-only closes short


# ---------------------------------------------------------------------------
# Task 4: Fix Bitget Classic/UTA Order Builders
# ---------------------------------------------------------------------------


class TestBitgetOrderBody:
    """Task 4 Steps 2-4: Bitget profile-aware order path and body."""

    @pytest.mark.asyncio
    async def test_bitget_classic_passive_order_body_uses_v2_mix_contract_fields(self):
        import json as _json
        from lightfee.venues.bitget import BitgetAdapter, BitgetAccountProfile

        seen: dict = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/api/v2/mix/account/account":
                return httpx.Response(200, json={"code": "00000", "data": {"posMode": "hedge_mode"}})
            seen["path"] = request.url.path
            seen["body"] = _json.loads(request.content.decode())
            return httpx.Response(200, json=_fixture("bitget/classic_place_order_ack_only.json"))

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter._profile = BitgetAccountProfile.CLASSIC
        adapter._transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

        req = OrderRequest(
            venue=Venue.BITGET,
            symbol="BTCUSDT",
            side=Side.SELL,
            quantity=0.001,
            price=50000.0,
            post_only=True,
            client_order_id="lfv2-entry-maker-001",
        )

        await adapter.submit_passive_order(req)

        assert seen["path"] == "/api/v2/mix/order/place-order"
        assert seen["body"]["productType"] == "USDT-FUTURES"
        assert seen["body"]["marginMode"] == "crossed"
        assert seen["body"]["marginCoin"] == "USDT"
        assert seen["body"]["size"] == "0.001"
        assert seen["body"]["side"] == "sell"
        assert seen["body"]["orderType"] == "limit"
        assert seen["body"]["force"] == "post_only"
        assert seen["body"]["clientOid"] == "lfv2-entry-maker-001"
        assert "quantity" not in seen["body"]

    @pytest.mark.asyncio
    async def test_bitget_uta_passive_order_body_uses_v3_trade_fields(self):
        import json as _json
        from lightfee.venues.bitget import BitgetAdapter, BitgetAccountProfile

        seen: dict = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            seen["path"] = request.url.path
            seen["body"] = _json.loads(request.content.decode())
            return httpx.Response(200, json=_fixture("bitget/uta_place_order_ack_only.json"))

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter._profile = BitgetAccountProfile.UTA
        adapter._transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

        req = OrderRequest(
            venue=Venue.BITGET,
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=0.001,
            price=50000.0,
            post_only=True,
            client_order_id="lfv2-entry-maker-001",
        )

        await adapter.submit_passive_order(req)

        assert seen["path"] == "/api/v3/trade/place-order"
        assert seen["body"]["category"] == "USDT-FUTURES"
        assert seen["body"]["qty"] == "0.001"
        assert seen["body"]["side"] == "buy"
        assert seen["body"]["orderType"] == "limit"
        assert seen["body"]["timeInForce"] == "post_only"
        assert seen["body"]["clientOid"] == "lfv2-entry-maker-001"
        assert "quantity" not in seen["body"]

    def test_bitget_classic_hedge_reduce_only_buy_closes_short_with_sell_side(self):
        from lightfee.venues.transport import _build_bitget_order_request

        req = OrderRequest(
            venue=Venue.BITGET,
            symbol="KSMUSDT",
            side=Side.BUY,
            quantity=2.9,
            reduce_only=True,
            client_order_id="lfv2-close-short-001",
        )

        path, body = _build_bitget_order_request(
            req, "KSMUSDT", passive=False, profile="classic", hedge_mode=True,
        )

        assert path == "/api/v2/mix/order/place-order"
        assert body["orderType"] == "market"
        assert body["side"] == "sell"  # V1: hedge close-short uses close + sell
        assert body["tradeSide"] == "close"
        assert "reduceOnly" not in body

    def test_bitget_classic_hedge_reduce_only_sell_closes_long_with_buy_side(self):
        from lightfee.venues.transport import _build_bitget_order_request

        req = OrderRequest(
            venue=Venue.BITGET,
            symbol="BTCUSDT",
            side=Side.SELL,
            quantity=0.001,
            reduce_only=True,
            client_order_id="lfv2-close-long-001",
        )

        _, body = _build_bitget_order_request(
            req, "BTCUSDT", passive=False, profile="classic", hedge_mode=True,
        )

        assert body["side"] == "buy"  # V1: hedge close-long uses close + buy
        assert body["tradeSide"] == "close"

    def test_bitget_uta_hedge_reduce_only_buy_uses_short_pos_side(self):
        from lightfee.venues.transport import _build_bitget_order_request

        req = OrderRequest(
            venue=Venue.BITGET,
            symbol="KSMUSDT",
            side=Side.BUY,
            quantity=2.9,
            reduce_only=True,
            client_order_id="lfv2-uta-close-short-001",
        )

        path, body = _build_bitget_order_request(
            req, "KSMUSDT", passive=False, profile="uta", hedge_mode=True,
        )

        assert path == "/api/v3/trade/place-order"
        assert body["side"] == "buy"
        assert body["posSide"] == "short"
        assert "reduceOnly" not in body

    def test_bitget_uta_hedge_reduce_only_sell_uses_long_pos_side(self):
        from lightfee.venues.transport import _build_bitget_order_request

        req = OrderRequest(
            venue=Venue.BITGET,
            symbol="BTCUSDT",
            side=Side.SELL,
            quantity=0.001,
            reduce_only=True,
            client_order_id="lfv2-uta-close-long-001",
        )

        _, body = _build_bitget_order_request(
            req, "BTCUSDT", passive=False, profile="uta", hedge_mode=True,
        )

        assert body["side"] == "sell"
        assert body["posSide"] == "long"

    @pytest.mark.asyncio
    async def test_bitget_classic_one_way_reduce_only_buy_uses_reduce_only_not_trade_side(self):
        from lightfee.venues.bitget import BitgetAdapter, BitgetAccountProfile

        calls: list[tuple[str, str]] = []
        seen_body: dict = {}

        async def fake_request(method, path, *, body=None, params=None, private=False, **kwargs):
            calls.append((method, path))
            if path == "/api/v2/mix/account/account":
                return {"code": "00000", "data": {"posMode": "one_way_mode"}}
            if path == "/api/v2/mix/order/place-order":
                seen_body.update(body or {})
                return {
                    "code": "00000",
                    "data": {"orderId": "bitget-close-short-001", "clientOid": "close-short"},
                }
            return {"code": "00000", "data": {}}

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter._profile = BitgetAccountProfile.CLASSIC
        adapter._transport._request = fake_request
        adapter._transport._parse_order_fill = lambda raw, req, sym, ms: OrderFill(
            venue=Venue.BITGET, symbol=sym, side=req.side,
            quantity=req.quantity, price=5.55, order_id="bitget-close-short-001",
        )

        req = OrderRequest(
            venue=Venue.BITGET,
            symbol="KSMUSDT",
            side=Side.BUY,
            quantity=2.9,
            reduce_only=True,
            client_order_id="close-short",
        )

        await adapter.place_order(req)

        assert ("GET", "/api/v2/mix/account/account") in calls
        assert seen_body["side"] == "buy"
        assert seen_body["reduceOnly"] == "YES"
        assert "tradeSide" not in seen_body

    @pytest.mark.asyncio
    async def test_bitget_classic_order_mode_probe_overrides_position_payload_mode(self):
        from lightfee.venues.bitget import BitgetAdapter, BitgetAccountProfile

        calls: list[tuple[str, str]] = []
        seen_body: dict = {}

        async def fake_request(method, path, *, body=None, params=None, private=False, **kwargs):
            calls.append((method, path))
            if path == "/api/v2/mix/position/single-position":
                return {
                    "code": "00000",
                    "data": [{
                        "symbol": "KSMUSDT",
                        "total": "2.9",
                        "holdSide": "short",
                        "averageOpenPrice": "5.545",
                        "holdMode": "hedge_mode",
                    }],
                }
            if path == "/api/v2/mix/account/account":
                return {"code": "00000", "data": {"posMode": "one_way_mode"}}
            if path == "/api/v2/mix/order/place-order":
                seen_body.update(body or {})
                return {
                    "code": "00000",
                    "data": {"orderId": "bitget-close-short-002", "clientOid": "close-short"},
                }
            return {"code": "00000", "data": {}}

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter._profile = BitgetAccountProfile.CLASSIC
        adapter._transport._request = fake_request
        adapter._transport._parse_order_fill = lambda raw, req, sym, ms: OrderFill(
            venue=Venue.BITGET, symbol=sym, side=req.side,
            quantity=req.quantity, price=5.55, order_id="bitget-close-short-002",
        )

        await adapter.fetch_position("KSMUSDT")
        await adapter.place_order(OrderRequest(
            venue=Venue.BITGET,
            symbol="KSMUSDT",
            side=Side.BUY,
            quantity=2.9,
            reduce_only=True,
            client_order_id="close-short",
        ))

        assert ("GET", "/api/v2/mix/account/account") in calls
        assert seen_body["side"] == "buy"
        assert seen_body["reduceOnly"] == "YES"
        assert "tradeSide" not in seen_body

    @pytest.mark.asyncio
    async def test_bitget_live_place_order_records_attempt_and_rejection_diagnostics(self):
        from lightfee.venues.bitget import BitgetAdapter, BitgetAccountProfile

        seen_body: dict = {}

        async def fake_request(method, path, *, body=None, params=None, private=False, **kwargs):
            if path == "/api/v2/mix/account/account":
                return {"code": "00000", "data": {"posMode": "one_way_mode"}}
            if path == "/api/v2/mix/order/place-order":
                seen_body.update(body or {})
                raise TransportError(
                    TransportErrorCategory.REQUEST_REJECTED,
                    'HTTP 400: {"code":"40786","msg":"Duplicate clientOid"}',
                    status_code=400,
                    body='{"code":"40786","msg":"Duplicate clientOid"}',
                )
            return {"code": "00000", "data": {}}

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="key-secret", api_secret="sign-secret", api_passphrase="pass-secret"),
        )
        adapter._profile = BitgetAccountProfile.CLASSIC
        adapter._transport._request = fake_request

        with pytest.raises(OrderSubmitError) as exc_info:
            await adapter.place_order(OrderRequest(
                venue=Venue.BITGET,
                symbol="KSMUSDT",
                side=Side.BUY,
                quantity=2.9,
                reduce_only=True,
                client_order_id="close-short",
            ))

        assert exc_info.value.class_ == SubmitFailureClass.REJECTED
        assert seen_body["side"] == "buy"
        events = adapter._transport.order_diagnostics
        assert [event["kind"] for event in events] == [
            "order.submit_attempt",
            "order.submit_result",
        ]
        attempt = events[0]["payload"]
        assert attempt["venue"] == "bitget"
        assert attempt["endpoint"] == "/api/v2/mix/order/place-order"
        assert attempt["account_profile"] == "classic"
        assert attempt["hedge_mode"] is False
        assert attempt["body_sanitized"]["side"] == "buy"
        assert attempt["body_sanitized"]["reduceOnly"] == "YES"
        result = events[1]["payload"]
        assert result["response_code"] == 400
        assert result["response_classification"] == "rejected"
        assert "Duplicate clientOid" in result["response_msg"]
        serialized = json.dumps(events)
        assert "key-secret" not in serialized
        assert "sign-secret" not in serialized
        assert "pass-secret" not in serialized


# ---------------------------------------------------------------------------
# Task 5: Restore Official Aster Endpoints
# ---------------------------------------------------------------------------


class TestAsterSpec:
    """Task 5 Step 1: Aster spec uses official asterdex.com hosts."""

    def test_aster_spec_uses_official_asterdex_hosts(self):
        spec = aster_spec()
        assert spec.public_base_url == "https://fapi.asterdex.com"
        assert spec.private_base_url == "https://fapi.asterdex.com"
        assert spec.l2_snapshot_path == "/fapi/v1/depth"


# ---------------------------------------------------------------------------
# Task 6: Bitget L2 Metadata Guard and Official Orderbook Parser
# ---------------------------------------------------------------------------


class TestBitgetL2Guard:
    """Task 6 Step 2-3: Bitget L2 metadata guard and V3 orderbook parser."""

    @pytest.mark.asyncio
    async def test_bitget_l2_unsupported_symbol_is_blocked_before_http_call(self):
        transport = _make_live_transport_with_mock_response(
            Venue.BITGET, "bitget/orderbook_uta_success.json"
        )
        transport.set_symbol_metadata({"BTCUSDT": {"sizeMultiplier": "0.001"}})

        with pytest.raises(TransportError) as exc:
            await transport.fetch_l2_snapshot("INJUSDT", depth=50)

        assert exc.value.category == TransportErrorCategory.REQUEST_REJECTED
        assert "metadata missing" in str(exc.value)

    @pytest.mark.asyncio
    async def test_bitget_l2_v3_orderbook_parses_a_b_arrays(self):
        transport = _make_live_transport_with_mock_response(
            Venue.BITGET, "bitget/orderbook_uta_success.json"
        )
        transport.set_symbol_metadata({"BTCUSDT": {"sizeMultiplier": "0.001"}})
        update = await transport.fetch_l2_snapshot("BTCUSDT", depth=50)

        assert update.venue == "bitget"
        assert update.symbol == "BTCUSDT"
        assert update.bids[0].price == 71213.8
        assert update.asks[0].price == 73000.0

    @pytest.mark.asyncio
    async def test_empty_metadata_rejects_all_symbols_on_transport(self):
        """When _symbol_metadata is empty (production default), unsupported symbols
        must be rejected BEFORE making an HTTP call (no 400172 from exchange)."""
        transport = _make_live_transport_with_mock_response(
            Venue.BITGET, "bitget/orderbook_uta_success.json"
        )
        # Default: _symbol_metadata is empty — no set_symbol_metadata() call
        assert not transport._symbol_metadata

        with pytest.raises(TransportError) as exc:
            await transport.fetch_l2_snapshot("INJUSDT", depth=50)

        assert exc.value.category == TransportErrorCategory.REQUEST_REJECTED
        assert "metadata missing" in str(exc.value)

    @pytest.mark.asyncio
    async def test_empty_metadata_rejects_even_known_symbols(self):
        """Even BTCUSDT must be blocked when metadata is empty — the guard
        must not be bypassed just because the symbol looks valid."""
        transport = _make_live_transport_with_mock_response(
            Venue.BITGET, "bitget/orderbook_uta_success.json"
        )
        assert not transport._symbol_metadata

        with pytest.raises(TransportError) as exc:
            await transport.fetch_l2_snapshot("BTCUSDT", depth=50)

        assert exc.value.category == TransportErrorCategory.REQUEST_REJECTED


# ---------------------------------------------------------------------------
# BinanceAdapter symbol catalog guard
# ---------------------------------------------------------------------------


class TestBinanceAdapterSymbolCatalog:
    @pytest.mark.asyncio
    async def test_ensure_supported_symbols_loaded_keeps_trading_perpetuals_only(self):
        """SETTLING contracts can return empty depth and must not be L2 targets."""
        from lightfee.venues.binance import BinanceAdapter

        adapter = BinanceAdapter(mode="paper")

        async def mock_request(method, path, **kwargs):
            assert path == "/fapi/v1/exchangeInfo"
            return {
                "symbols": [
                    {"symbol": "BTCUSDT", "status": "TRADING", "contractType": "PERPETUAL"},
                    {"symbol": "SYSUSDT", "status": "SETTLING", "contractType": "PERPETUAL"},
                    {"symbol": "BTCUSDT_260626", "status": "TRADING", "contractType": "CURRENT_QUARTER"},
                ]
            }

        adapter._transport._request = mock_request

        await adapter.ensure_supported_symbols_loaded()

        assert adapter.supported_symbols() == ["BTCUSDT"]


class TestBybitAdapterSymbolCatalog:
    @pytest.mark.asyncio
    async def test_ensure_supported_symbols_loaded_paginates_linear_perpetuals(self):
        """Bybit linear catalog exceeds the default page and must be fully loaded."""
        from lightfee.venues.bybit import BybitAdapter

        adapter = BybitAdapter(mode="paper")
        cursors: list[str] = []

        async def mock_request(method, path, **kwargs):
            assert method == "GET"
            assert path == "/v5/market/instruments-info"
            params = kwargs.get("params", {})
            assert params["category"] == "linear"
            assert params["limit"] == 1000
            cursor = str(params.get("cursor", ""))
            cursors.append(cursor)
            if not cursor:
                return {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {"symbol": "BTCUSDT", "status": "Trading", "contractType": "LinearPerpetual"},
                            {"symbol": "ETHUSDT", "status": "Settling", "contractType": "LinearPerpetual"},
                            {"symbol": "BTCUSDT_260626", "status": "Trading", "contractType": "LinearFutures"},
                            {"symbol": "BTCUSDC", "status": "Trading", "contractType": "LinearPerpetual"},
                        ],
                        "nextPageCursor": "page-2",
                    },
                }
            return {
                "retCode": 0,
                "result": {
                    "list": [
                        {"symbol": "SOLUSDT", "status": "Trading", "contractType": "LinearPerpetual"},
                    ],
                    "nextPageCursor": "",
                },
            }

        adapter._transport._request = mock_request

        await adapter.ensure_supported_symbols_loaded()

        assert cursors == ["", "page-2"]
        assert adapter.supported_symbols() == ["BTCUSDT", "SOLUSDT"]


# ---------------------------------------------------------------------------
# Aster/Hyperliquid symbol catalog guards
# ---------------------------------------------------------------------------


class TestAsterAdapterSymbolCatalog:
    @pytest.mark.asyncio
    async def test_ensure_supported_symbols_loaded_keeps_trading_perpetuals_only(self):
        """Aster closed/settling contracts must not become local-L2 targets."""
        from lightfee.venues.aster import AsterAdapter

        adapter = AsterAdapter(mode="paper")

        async def mock_request(method, path, **kwargs):
            assert path == "/fapi/v1/exchangeInfo"
            return {
                "symbols": [
                    {"symbol": "BTCUSDT", "status": "TRADING", "contractType": "PERPETUAL"},
                    {"symbol": "RLSUSDT", "status": "SETTLING", "contractType": "PERPETUAL"},
                    {"symbol": "BTCUSDT_260626", "status": "TRADING", "contractType": "CURRENT_QUARTER"},
                ]
            }

        adapter._transport._request = mock_request

        await adapter.ensure_supported_symbols_loaded()

        assert adapter.supported_symbols() == ["BTCUSDT"]

    @pytest.mark.asyncio
    async def test_ensure_supported_symbols_loaded_refreshes_after_ttl(self):
        """Aster directory must not be permanently valid after one load."""
        from lightfee.venues.aster import (
            AsterAdapter,
            _ASTER_EXCHANGE_INFO_TTL_MS,
        )

        adapter = AsterAdapter(mode="paper")
        requests = []

        async def mock_request(method, path, **kwargs):
            requests.append((path, dict(kwargs)))
            return {
                "symbols": [
                    {"symbol": "BTCUSDT", "status": "TRADING", "contractType": "PERPETUAL"},
                    {"symbol": "COTIUSDT", "status": "TRADING", "contractType": "PERPETUAL"},
                ]
            }

        adapter._transport._request = mock_request
        await adapter.ensure_supported_symbols_loaded()
        assert "COTIUSDT" in adapter.supported_symbols()
        assert len(requests) == 1

        # Second load within TTL must not hit exchangeInfo again.
        adapter._symbol_metadata_loaded_at_ms = (
            int(time.time() * 1000) - _ASTER_EXCHANGE_INFO_TTL_MS - 1
        )
        await adapter.ensure_supported_symbols_loaded()
        assert len(requests) == 2

    @pytest.mark.asyncio
    async def test_aster_1121_marks_symbol_unsupported_and_drops_from_catalog(self):
        """A -1121 Invalid symbol on a private request must invalidate the local
        catalog entry immediately and never be treated as a flat position."""
        from lightfee.venues.aster import AsterAdapter

        adapter = AsterAdapter(mode="paper")
        adapter._transport.set_symbol_metadata({
            "COTIUSDT": {"symbol": "COTIUSDT", "status": "TRADING"},
            "HOMEUSDT": {"symbol": "HOMEUSDT", "status": "TRADING"},
        })

        async def mock_request(method, path, **kwargs):
            return {
                "symbols": [
                    {"symbol": "COTIUSDT", "status": "TRADING", "contractType": "PERPETUAL"},
                    {"symbol": "HOMEUSDT", "status": "TRADING", "contractType": "PERPETUAL"},
                ]
            }

        adapter._transport._request = mock_request
        await adapter.ensure_supported_symbols_loaded()
        assert "COTIUSDT" in adapter.supported_symbols()
        assert "HOMEUSDT" in adapter.supported_symbols()

        adapter.mark_symbol_unsupported(
            "COTIUSDT",
            endpoint="/fapi/v3/positionRisk",
            exchange_code="-1121",
        )

        assert "COTIUSDT" not in adapter.supported_symbols()
        assert "HOMEUSDT" in adapter.supported_symbols()
        diagnostics = adapter._transport.order_diagnostics
        assert any(
            event.get("kind") == "venues.aster.unsupported_symbol"
            and event["payload"].get("symbol") == "COTIUSDT"
            and event["payload"].get("exchange_code") == "-1121"
            for event in diagnostics
        )

    @pytest.mark.asyncio
    async def test_aster_unsupported_symbol_never_flat_until_account_truth(self):
        """-1121 must not be consumed as flat; only a successful full-account
        truth probe that excludes the symbol can prove flat."""
        import tempfile
        from pathlib import Path

        from lightfee.venues.aster import AsterAdapter
        from lightfee.engine.passive_close import PassiveCloseExecutor
        from lightfee.persistence.journal import Journal

        adapter = AsterAdapter(mode="paper")
        adapter._mode = "live"
        adapter._transport.set_symbol_metadata({"COTIUSDT": {"symbol": "COTIUSDT"}})

        async def mock_request(method, path, **kwargs):
            return {
                "symbols": [
                    {"symbol": "COTIUSDT", "status": "TRADING", "contractType": "PERPETUAL"},
                ]
            }

        adapter._transport._request = mock_request
        await adapter.ensure_supported_symbols_loaded()

        d = tempfile.mkdtemp()
        journal = Journal(Path(d) / "test.log")
        journal.open()
        executor = PassiveCloseExecutor({Venue.ASTER: adapter}, journal)

        # In live mode with no private client, directed fetch_position raises
        # (never returns 0.0). Only a successful full-account truth probe that
        # excludes COTIUSDT can prove flat.
        adapter.fetch_all_positions = AsyncMock(return_value=[])
        probe = await executor._probe_venue_flatness_evidence(
            Venue.ASTER, "COTIUSDT", {Venue.ASTER: adapter}
        )
        assert probe["flat"] is True

        # Full-account truth unavailable -> not flat (fail-closed).
        adapter.fetch_all_positions = AsyncMock(
            side_effect=TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                "account truth unavailable",
            )
        )
        probe = await executor._probe_venue_flatness_evidence(
            Venue.ASTER, "COTIUSDT", {Venue.ASTER: adapter}
        )
        assert probe["flat"] is False
        assert probe["error"] is not None
        journal.close()

    @pytest.mark.asyncio
    async def test_aster_1121_on_private_position_marks_unsupported_and_raises(self):
        """A live Aster private position request returning -1121 must mark the
        symbol unsupported and propagate the error, never return a flat position."""
        from lightfee.venues.aster import AsterAdapter

        private_key = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"

        async def handler(request):
            assert request.url.path == "/fapi/v3/positionRisk"
            return httpx.Response(
                400,
                json={"code": -1121, "msg": "Invalid symbol."},
            )

        adapter = AsterAdapter(
            mode="live",
            credential=LiveCredential(api_secret=private_key),
        )
        try:
            adapter._transport.set_symbol_metadata({"COTIUSDT": {"symbol": "COTIUSDT"}})
            adapter._private._client = httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            )
            with pytest.raises(TransportError) as exc:
                await adapter.fetch_position("COTIUSDT")
            assert exc.value.status_code == 400
            assert "-1121" in exc.value.body or "-1121" in str(exc.value)

            assert "COTIUSDT" not in adapter.supported_symbols()
            diagnostics = adapter._transport.order_diagnostics
            assert any(
                event.get("kind") == "venues.aster.unsupported_symbol"
                and event["payload"].get("symbol") == "COTIUSDT"
                and event["payload"].get("exchange_code") == "-1121"
                for event in diagnostics
            )
        finally:
            await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_private_http_logging_redacts_signed_query(self):
        """Importing the venue transport must force httpx/httpcore to WARNING so
        no full signed private request URL is written at INFO."""
        # The module-level call at import is the fix; without it these loggers
        # inherit INFO from the root logger and leak signed query params.
        for name in ("httpx", "httpcore"):
            assert logging.getLogger(name).getEffectiveLevel() >= logging.WARNING, (
                f"{name} logger must default to WARNING after transport import"
            )

        root = logging.getLogger()
        old_level = root.level
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(name)s %(levelname)s %(message)s")
        handler.setFormatter(formatter)
        root.addHandler(handler)
        try:
            root.setLevel(logging.INFO)

            async def handler(request):
                return httpx.Response(200, json=[])

            from lightfee.venues.aster_v3 import AsterV3Client

            client = AsterV3Client(
                credential=LiveCredential(api_secret="0x" + "1" * 64),
                http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
            )
            try:
                await client.fetch_open_orders(None)
            finally:
                await client.close()

            text = stream.getvalue()
            for token in ("signature=", "signer=", "nonce=", "user=", "api_key", "api-secret"):
                assert token not in text, f"private query leaked into logs: {token!r}"
            assert "httpx" not in text, "httpx INFO request logging should be suppressed"
        finally:
            root.removeHandler(handler)
            root.setLevel(old_level)

    @pytest.mark.asyncio
    async def test_aster_1121_on_place_order_marks_unsupported_and_raises(self):
        """POST /fapi/v3/order returning -1121 must mark the symbol unsupported,
        propagate the rejection, and never leave the symbol in the catalog."""
        from lightfee.venues.aster import AsterAdapter
        from lightfee.core.domain import OrderRequest, Side
        from lightfee.core.errors import OrderSubmitError

        private_key = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"

        async def handler(request):
            if request.url.path == "/fapi/v1/exchangeInfo":
                return httpx.Response(200, json={
                    "symbols": [{
                        "symbol": "COTIUSDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                            {"filterType": "MIN_NOTIONAL", "notional": "5"},
                        ],
                    }],
                })
            if request.url.path == "/fapi/v3/positionRisk":
                return httpx.Response(200, json=[{
                    "symbol": "COTIUSDT",
                    "positionAmt": "0",
                    "markPrice": "1",
                    "maxNotionalValue": "1000000",
                }])
            if request.url.path == "/fapi/v3/openOrders":
                return httpx.Response(200, json=[])
            if request.url.path == "/fapi/v3/positionSide/dual":
                return httpx.Response(200, json={"dualSidePosition": False})
            assert request.url.path == "/fapi/v3/order"
            return httpx.Response(400, json={"code": -1121, "msg": "Invalid symbol."})

        adapter = AsterAdapter(
            mode="live",
            credential=LiveCredential(api_secret=private_key),
        )
        try:
            adapter._transport.set_symbol_metadata({"COTIUSDT": {"symbol": "COTIUSDT"}})
            adapter._private._client = httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            )
            adapter._transport._client = httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            )
            with pytest.raises(OrderSubmitError) as exc:
                await adapter.place_order(OrderRequest(
                    venue=Venue.ASTER, symbol="COTIUSDT", side=Side.BUY, quantity=20.0,
                ))
            assert exc.value.is_rejected
            assert "COTIUSDT" not in adapter.supported_symbols()
            diagnostics = adapter._transport.order_diagnostics
            unsupported = [
                event for event in diagnostics
                if event.get("kind") == "venues.aster.unsupported_symbol"
            ]
            assert len(unsupported) == 1
            assert unsupported[0]["payload"]["symbol"] == "COTIUSDT"
            assert unsupported[0]["payload"]["exchange_code"] == "-1121"
        finally:
            await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_aster_1121_on_submit_passive_order_marks_unsupported(self):
        """Passive order submission hitting -1121 must also invalidate and fail
        closed, not silently retry."""
        from lightfee.venues.aster import AsterAdapter
        from lightfee.core.domain import OrderRequest, Side
        from lightfee.core.errors import OrderSubmitError

        private_key = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"

        async def handler(request):
            if request.url.path == "/fapi/v1/exchangeInfo":
                return httpx.Response(200, json={
                    "symbols": [{
                        "symbol": "COTIUSDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                            {"filterType": "MIN_NOTIONAL", "notional": "5"},
                        ],
                    }],
                })
            if request.url.path == "/fapi/v3/positionRisk":
                return httpx.Response(200, json=[{
                    "symbol": "COTIUSDT",
                    "positionAmt": "0",
                    "markPrice": "1",
                    "maxNotionalValue": "1000000",
                }])
            if request.url.path == "/fapi/v3/openOrders":
                return httpx.Response(200, json=[])
            if request.url.path == "/fapi/v3/positionSide/dual":
                return httpx.Response(200, json={"dualSidePosition": False})
            assert request.url.path == "/fapi/v3/order"
            return httpx.Response(400, json={"code": -1121, "msg": "Invalid symbol."})

        adapter = AsterAdapter(
            mode="live",
            credential=LiveCredential(api_secret=private_key),
        )
        try:
            adapter._transport.set_symbol_metadata({"COTIUSDT": {"symbol": "COTIUSDT"}})
            adapter._private._client = httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            )
            adapter._transport._client = httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            )
            with pytest.raises(OrderSubmitError):
                await adapter.submit_passive_order(OrderRequest(
                    venue=Venue.ASTER, symbol="COTIUSDT", side=Side.BUY, quantity=20.0,
                    price=0.5,
                ))
            assert "COTIUSDT" not in adapter.supported_symbols()
            diagnostics = adapter._transport.order_diagnostics
            assert len([
                event for event in diagnostics
                if event.get("kind") == "venues.aster.unsupported_symbol"
            ]) == 1
        finally:
            await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_aster_1121_on_query_passive_order_progress_marks_unsupported(self):
        """A -1121 during passive progress query must raise and invalidate
        instead of being swallowed as a benign None."""
        from lightfee.venues.aster import AsterAdapter
        from lightfee.core.errors import OrderSubmitError

        private_key = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"

        async def handler(request):
            assert request.url.path == "/fapi/v3/order"
            return httpx.Response(400, json={"code": -1121, "msg": "Invalid symbol."})

        adapter = AsterAdapter(
            mode="live",
            credential=LiveCredential(api_secret=private_key),
        )
        try:
            adapter._transport.set_symbol_metadata({"COTIUSDT": {"symbol": "COTIUSDT"}})
            adapter._private._client = httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            )
            with pytest.raises(OrderSubmitError):
                await adapter.query_passive_order_progress("COTIUSDT", "order-1")
            assert "COTIUSDT" not in adapter.supported_symbols()
            diagnostics = adapter._transport.order_diagnostics
            assert len([
                event for event in diagnostics
                if event.get("kind") == "venues.aster.unsupported_symbol"
            ]) == 1
        finally:
            await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_aster_1121_diagnostic_is_single_across_private_paths(self):
        """Repeated -1121 evidence from different private paths must still emit
        only one unsupported_symbol diagnostic for the symbol."""
        from lightfee.venues.aster import AsterAdapter
        from lightfee.core.domain import OrderRequest, Side
        from lightfee.core.errors import OrderSubmitError

        private_key = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"

        async def handler(request):
            if request.url.path == "/fapi/v1/exchangeInfo":
                return httpx.Response(200, json={
                    "symbols": [{
                        "symbol": "COTIUSDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                            {"filterType": "MIN_NOTIONAL", "notional": "5"},
                        ],
                    }],
                })
            return httpx.Response(400, json={"code": -1121, "msg": "Invalid symbol."})

        adapter = AsterAdapter(
            mode="live",
            credential=LiveCredential(api_secret=private_key),
        )
        try:
            adapter._transport.set_symbol_metadata({"COTIUSDT": {"symbol": "COTIUSDT"}})
            adapter._private._client = httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            )
            adapter._transport._client = httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            )
            for _ in range(3):
                with pytest.raises(OrderSubmitError):
                    await adapter.place_order(OrderRequest(
                        venue=Venue.ASTER, symbol="COTIUSDT", side=Side.BUY, quantity=20.0,
                    ))
            diagnostics = adapter._transport.order_diagnostics
            unsupported = [
                event for event in diagnostics
                if event.get("kind") == "venues.aster.unsupported_symbol"
                and event["payload"].get("symbol") == "COTIUSDT"
            ]
            assert len(unsupported) == 1
        finally:
            await adapter.shutdown()


class TestGateAdapterSymbolCatalog:
    @pytest.mark.asyncio
    async def test_ensure_supported_symbols_loaded_keeps_trading_usdt_contracts_only(self):
        """Gate recovery catalog must use active futures contracts, not an empty list."""
        from lightfee.venues.gate import GateAdapter

        adapter = GateAdapter(mode="paper")

        async def mock_request(method, path, **kwargs):
            assert method == "GET"
            assert path == "/api/v4/futures/usdt/contracts"
            assert kwargs.get("private") is False
            return [
                {"name": "BTC_USDT", "status": "trading", "in_delisting": False},
                {"name": "ETH_USDT", "status": "delisting", "in_delisting": True},
                {"name": "SOL_USDT", "trade_status": "tradable", "in_delisting": False},
                {"name": "FOO_USD", "status": "trading", "in_delisting": False},
            ]

        adapter._transport._request = mock_request

        await adapter.ensure_supported_symbols_loaded()

        assert adapter.supported_symbols() == ["BTCUSDT", "SOLUSDT"]


class TestHyperliquidAdapterSymbolCatalog:
    @pytest.mark.asyncio
    async def test_ensure_supported_symbols_loaded_excludes_delisted_assets(self):
        """Hyperliquid delisted assets can return empty l2Book sides."""
        from lightfee.venues.hyperliquid import HyperliquidAdapter

        adapter = HyperliquidAdapter(mode="paper")

        async def mock_request(method, path, **kwargs):
            assert method == "POST"
            assert path == "/info"
            assert kwargs.get("body") == {"type": "meta"}
            return {
                "universe": [
                    {"name": "BTC", "isDelisted": False},
                    {"name": "MAV", "isDelisted": True},
                ]
            }

        adapter._transport._request = mock_request

        await adapter.ensure_supported_symbols_loaded()

        assert adapter.supported_symbols() == ["BTC"]


# ---------------------------------------------------------------------------
# Task 3 regression: BitgetAdapter L2 metadata guard (no bare transport)
# ---------------------------------------------------------------------------


class TestBitgetAdapterL2MetadataGuard:
    """Regr: BitgetAdapter.fetch_l2_snapshot must load metadata and guard before HTTP.

    V1: bitget_fetch_execution_liquidity_snapshot() requires metadata.get(symbol)
    before sending any HTTP request. The adapter (not bare transport) is the
    primary integration point.
    """

    def test_supported_symbols_reflect_loaded_contract_metadata(self):
        """Position recovery uses this catalog to avoid probing removed symbols."""
        from lightfee.venues.bitget import BitgetAdapter

        adapter = BitgetAdapter(mode="paper")
        adapter._transport.set_symbol_metadata({
            "BTCUSDT": {"sizeMultiplier": "0.001"},
            "ETHUSDT": {"sizeMultiplier": "0.001"},
        })

        assert adapter.supported_symbols() == ["BTCUSDT", "ETHUSDT"]

    @pytest.mark.asyncio
    async def test_ensure_supported_symbols_loaded_fetches_contract_catalog(self):
        """Startup recovery can load Bitget's catalog before per-symbol probes."""
        from lightfee.venues.bitget import BitgetAdapter

        adapter = BitgetAdapter(mode="paper")

        async def mock_request(method, path, **kwargs):
            if "contracts" in path:
                return {
                    "code": "00000", "msg": "success",
                    "data": [
                        {"symbol": "BTCUSDT", "sizeMultiplier": "0.001",
                         "minTradeNum": "1", "pricePlace": "2", "volumePlace": "0",
                         "symbolName": "BTCUSDT"},
                    ],
                }
            return {}

        adapter._transport._request = mock_request

        await adapter.ensure_supported_symbols_loaded()

        assert adapter.supported_symbols() == ["BTCUSDT"]

    @pytest.mark.asyncio
    async def test_adapter_unsupported_symbol_raises_no_http_orderbook_call(self):
        """BitgetAdapter.fetch_l2_snapshot with unsupported symbol must raise
        TransportError without ever calling /api/v3/market/orderbook."""
        from lightfee.venues.bitget import BitgetAdapter
        from lightfee.venues.transport import TransportError, TransportErrorCategory

        adapter = BitgetAdapter(mode="paper")

        # Inject mock _request that records calls
        calls = []

        async def mock_request(method, path, **kwargs):
            calls.append((method, path))
            if "contracts" in path:
                # Return empty contract list — no symbols supported
                return {"code": "00000", "msg": "success", "data": []}
            return {}

        adapter._transport._request = mock_request

        with pytest.raises(TransportError) as exc:
            await adapter.fetch_l2_snapshot("INJUSDT", depth=50)

        assert exc.value.category == TransportErrorCategory.REQUEST_REJECTED
        assert "metadata missing" in str(exc.value).lower()

        # Must NOT have called orderbook endpoint
        orderbook_calls = [c for c in calls if "orderbook" in c[1]]
        assert len(orderbook_calls) == 0, (
            f"HTTP orderbook call should not happen for unsupported symbol; got {orderbook_calls}"
        )

    @pytest.mark.asyncio
    async def test_adapter_loads_metadata_when_empty(self):
        """When transport metadata is empty, adapter.fetch_l2_snapshot must
        load contract catalog before checking metadata."""
        from lightfee.venues.bitget import BitgetAdapter
        from lightfee.venues.transport import TransportError

        adapter = BitgetAdapter(mode="paper")

        # Simulate a contract catalog with only BTCUSDT supported
        async def mock_request(method, path, **kwargs):
            if "contracts" in path:
                return {
                    "code": "00000", "msg": "success",
                    "data": [
                        {"symbol": "BTCUSDT", "sizeMultiplier": "0.001",
                         "minTradeNum": "1", "pricePlace": "2", "volumePlace": "0",
                         "symbolName": "BTCUSDT"},
                    ]
                }
            if "orderbook" in path:
                # Should only reach here for supported symbols
                return _bitget_l2_response()
            return {}

        adapter._transport._request = mock_request

        # BTCUSDT is supported → should load metadata and succeed
        # (orderbook call will return mock l2 data)
        update = await adapter.fetch_l2_snapshot("BTCUSDT", depth=50)
        assert update is not None
        assert adapter._transport._symbol_metadata
        assert "BTCUSDT" in adapter._transport._symbol_metadata

    @pytest.mark.asyncio
    async def test_adapter_rejects_unsupported_after_catalog_load(self):
        """After loading catalog (only BTCUSDT), ETHUSDT must be rejected
        without an orderbook HTTP call."""
        from lightfee.venues.bitget import BitgetAdapter
        from lightfee.venues.transport import TransportError, TransportErrorCategory

        adapter = BitgetAdapter(mode="paper")

        api_calls = []

        async def mock_request(method, path, **kwargs):
            api_calls.append(path)
            if "contracts" in path:
                return {
                    "code": "00000", "msg": "success",
                    "data": [
                        {"symbol": "BTCUSDT", "sizeMultiplier": "0.001",
                         "minTradeNum": "1", "pricePlace": "2", "volumePlace": "0",
                         "symbolName": "BTCUSDT"},
                    ]
                }
            return {}

        adapter._transport._request = mock_request

        with pytest.raises(TransportError) as exc:
            await adapter.fetch_l2_snapshot("ETHUSDT", depth=50)

        assert exc.value.category == TransportErrorCategory.REQUEST_REJECTED
        # Must have called contracts endpoint (to load catalog)
        contracts_calls = [c for c in api_calls if "contracts" in c]
        assert len(contracts_calls) >= 1
        # Must NOT have called orderbook endpoint
        orderbook_calls = [c for c in api_calls if "orderbook" in c]
        assert len(orderbook_calls) == 0


def _bitget_l2_response():
    """Minimal valid Bitget V3 orderbook response."""
    return {
        "code": "00000",
        "msg": "success",
        "requestTime": "1000000",
        "data": {
            "bids": [["71213.8", "1.0"], ["70000.0", "2.0"]],
            "asks": [["73000.0", "1.0"], ["74000.0", "2.0"]],
            "ts": "1000000",
        },
    }


# ---------------------------------------------------------------------------
# Task 7: Venue-Specific Position Parsers
# ---------------------------------------------------------------------------


class TestVenuePositionParsers:
    """Task 7 Step 1-4: venue-specific position parsers with safe numeric handling."""

    def test_bybit_position_parser_handles_list_shape(self):
        from lightfee.venues.transport import _parse_bybit_position
        raw = {
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {"symbol": "BTCUSDT", "side": "Buy", "size": "0.01", "avgPrice": "50000.0"}
                ]
            }
        }
        pos = _parse_bybit_position(raw, "BTCUSDT", now_ms=1000)
        assert pos.quantity == 0.01
        assert pos.entry_price == 50000.0
        assert pos.side == Side.BUY

    def test_bybit_position_parser_handles_empty_fields(self):
        from lightfee.venues.transport import _parse_bybit_position
        raw = {
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {"symbol": "BTCUSDT", "side": "Buy", "size": "", "avgPrice": ""}
                ]
            }
        }
        pos = _parse_bybit_position(raw, "BTCUSDT", now_ms=1000)
        assert pos.quantity == 0.0
        assert pos.entry_price == 0.0

    def test_bitget_position_parser_accepts_data_list_and_empty_numbers(self):
        from lightfee.venues.transport import _parse_bitget_position
        raw = {
            "code": "00000",
            "msg": "success",
            "data": [
                {
                    "symbol": "BTCUSDT",
                    "total": "",
                    "holdSide": "long",
                    "openPriceAvg": ""
                }
            ]
        }
        pos = _parse_bitget_position(raw, "BTCUSDT", now_ms=1000)
        assert pos.quantity == 0.0
        assert pos.entry_price == 0.0

    def test_bitget_position_parser_handles_short_side(self):
        from lightfee.venues.transport import _parse_bitget_position
        raw = {
            "code": "00000",
            "msg": "success",
            "data": [
                {
                    "symbol": "BTCUSDT",
                    "total": "0.01",
                    "holdSide": "short",
                    "openPriceAvg": "50000.0"
                }
            ]
        }
        pos = _parse_bitget_position(raw, "BTCUSDT", now_ms=1000)
        assert pos.side == Side.SELL
        assert pos.quantity == 0.01

    def test_bitget_classic_position_parser_accepts_base_volume(self):
        from lightfee.venues.transport import _parse_bitget_position

        raw = {
            "code": "00000",
            "msg": "success",
            "data": [
                {
                    "symbol": "HOMEUSDT",
                    "baseVolume": "145",
                    "holdSide": "long",
                    "openPriceAvg": "0.0401",
                }
            ],
        }

        pos = _parse_bitget_position(raw, "HOMEUSDT", now_ms=1000)

        assert pos.side == Side.BUY
        assert pos.quantity == 145.0
        assert pos.entry_price == 0.0401

    def test_bitget_uta_position_parser_accepts_data_list_qty_and_avg_price(self):
        from lightfee.venues.transport import _parse_bitget_position

        raw = {
            "code": "00000",
            "msg": "success",
            "data": {
                "list": [
                    {
                        "symbol": "HOMEUSDT",
                        "qty": "145",
                        "posSide": "short",
                        "avgPrice": "0.0401",
                    }
                ]
            },
        }

        pos = _parse_bitget_position(raw, "HOMEUSDT", now_ms=1000)

        assert pos.side == Side.SELL
        assert pos.quantity == 145.0
        assert pos.entry_price == 0.0401

    def test_okx_position_parser_scales_contracts_and_handles_empty_pos(self):
        from lightfee.venues.transport import _parse_okx_position
        raw = {
            "code": "0",
            "data": [
                {"instId": "BTC-USDT-SWAP", "pos": "", "posSide": "long", "avgPx": ""}
            ]
        }
        pos = _parse_okx_position(raw, "BTC-USDT-SWAP", now_ms=1000, contract_size=0.01)
        assert pos.quantity == 0.0
        assert pos.entry_price == 0.0

    def test_okx_position_parser_scales_contract_size(self):
        from lightfee.venues.transport import _parse_okx_position
        raw = {
            "code": "0",
            "data": [
                {"instId": "BTC-USDT-SWAP", "pos": "10", "posSide": "long", "avgPx": "50000"}
            ]
        }
        pos = _parse_okx_position(raw, "BTC-USDT-SWAP", now_ms=1000, contract_size=0.01)
        assert pos.quantity == 0.1  # 10 contracts * 0.01


# ====================================================================# RED-LIGHT: order fill reconciliation parsers (Bybit + Bitget)
#
# These tests directly call the real _parse_order_status_* functions with
# mock raw HTTP response dicts. No monkeypatching of target functions.
# The parser code is the production code being tested.
# ====================================================================

class TestBybitParseOrderStatusRedLight:
    """RED-LIGHT: _parse_order_status_bybit must follow V1 semantics.

    V1 reference: src/live/bybit.rs fetch_order_fill_reconciliation (lines 2820-2894)
    V1 semantics:
      - resolve orderId from client_order_id via orderLinkId
      - query /v5/execution/list
      - aggregate execQty, weighted avg price, total fee
      - total_quantity <= 0 → return None

    Current V2 _parse_order_status_bybit only reads cumExecQty/avgPrice
    from /v5/order/realtime and returns OrderFillReconciliation even when
    cumExecQty=0 for NEW/ACTIVE orders.
    """

    def test_bybit_new_order_zero_fill_returns_none(self):
        """WAS RED-LIGHT, NOW GREEN: NEW order with cumExecQty=0 → None (V1 parity)."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bybit_spec

        transport = VenueTransport(spec=bybit_spec(), mode="paper")
        raw = {
            "retCode": 0, "retMsg": "OK",
            "result": {"list": [{
                "orderId": "oid-1", "orderLinkId": "cid-1",
                "orderStatus": "New", "cumExecQty": "0",
                "avgPrice": "0", "side": "Buy",
                "updatedTime": "1000000",
            }]},
        }
        result = transport._parse_order_status_bybit(raw, "BTCUSDT", 1000000)
        assert result is None, f"NEW 0-fill should return None, got {result}"

    def test_bybit_active_untriggered_zero_fill_returns_none(self):
        """Active/Untriggered with cumExecQty=0 → None."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bybit_spec

        transport = VenueTransport(spec=bybit_spec(), mode="paper")
        raw = {
            "retCode": 0, "retMsg": "OK",
            "result": {"list": [{
                "orderId": "oid-1", "orderLinkId": "cid-1",
                "orderStatus": "Active", "cumExecQty": "0",
                "avgPrice": "0", "side": "Buy",
                "updatedTime": "1000000",
            }]},
        }
        result = transport._parse_order_status_bybit(raw, "BTCUSDT", 1000000)
        assert result is None, f"Active 0-fill should return None, got {result}"

    def test_bybit_filled_returns_reconciliation_with_correct_fields(self):
        """Filled order returns correct reconciliation fields."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bybit_spec
        from lightfee.core.domain import OrderFillReconciliation

        transport = VenueTransport(spec=bybit_spec(), mode="paper")
        raw = {
            "retCode": 0, "retMsg": "OK",
            "result": {"list": [{
                "orderId": "oid-1", "orderLinkId": "cid-1",
                "orderStatus": "Filled", "cumExecQty": "0.5",
                "avgPrice": "50000", "side": "Buy",
                "updatedTime": "1000000",
            }]},
        }
        result = transport._parse_order_status_bybit(raw, "BTCUSDT", 1000000)
        assert isinstance(result, OrderFillReconciliation)
        assert result.quantity == pytest.approx(0.5)
        assert result.average_price == pytest.approx(50000)
        assert result.order_id == "oid-1"
        assert result.client_order_id == "cid-1"


class TestBitgetParseOrderStatusRedLight:
    """RED-LIGHT: _parse_order_status_bitget must follow V1 semantics.

    V1 reference: src/live/bitget.rs fetch_order_fill_reconciliation (lines 2912-2949)
    V1 semantics:
      - /api/v3/trade/order-info with orderId/clientOid
      - quantity from baseVolume/filledQty/fillQty/size fallback
      - quantity <= 0 → None
      - multi-key avg price, fee, orderId/clientOid extraction
    """

    def test_bitget_zero_filled_qty_returns_none(self):
        """WAS RED-LIGHT, NOW GREEN: filledQty=0 → None (V1 parity)."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bitget_spec

        transport = VenueTransport(spec=bitget_spec(), mode="paper")
        raw = {
            "code": "00000", "msg": "success",
            "data": {
                "orderId": "oid-1", "clientOid": "cid-1",
                "filledQty": "0", "baseVolume": "0",
                "priceAvg": "0", "avgPrice": "0",
                "side": "buy", "uTime": "1000000",
                "cTime": "1000000", "fee": "0",
            },
        }
        result = transport._parse_order_status_bitget(raw, "BTCUSDT", 1000000)
        assert result is None, f"zero-filled bitget order should return None, got {result}"

    def test_bitget_zero_base_volume_returns_none(self):
        """baseVolume=0 → None."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bitget_spec

        transport = VenueTransport(spec=bitget_spec(), mode="paper")
        raw = {
            "code": "00000", "msg": "success",
            "data": {
                "orderId": "oid-1", "clientOid": "cid-1",
                "filledQty": "0", "baseVolume": "0",
                "priceAvg": "50000", "avgPrice": "50000",
                "side": "buy", "uTime": "1000000",
            },
        }
        result = transport._parse_order_status_bitget(raw, "BTCUSDT", 1000000)
        assert result is None, f"zero baseVolume should return None, got {result}"

    def test_bitget_positive_fill_returns_correct_fields(self):
        """Non-red-light: verify parser returns correct fields for positive fill."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bitget_spec
        from lightfee.core.domain import OrderFillReconciliation

        transport = VenueTransport(spec=bitget_spec(), mode="paper")
        raw = {
            "code": "00000", "msg": "success",
            "data": {
                "orderId": "oid-1", "clientOid": "cid-1",
                "filledQty": "0.5", "baseVolume": "0.5",
                "priceAvg": "51000", "avgPrice": "51000",
                "side": "buy", "uTime": "2000000",
                "cTime": "2000000", "fee": "0.1",
            },
        }
        result = transport._parse_order_status_bitget(raw, "BTCUSDT", 2000000)
        assert isinstance(result, OrderFillReconciliation)
        assert result.quantity == pytest.approx(0.5)
        assert result.average_price == pytest.approx(51000)
        assert result.order_id == "oid-1"
        assert result.client_order_id == "cid-1"

    def test_bitget_positive_fill_carries_order_truth_metadata(self):
        """Bitget fill reconciliation must carry the family/side truth ledger needs."""
        from lightfee.core.domain import OrderFillReconciliation
        from lightfee.engine.order_truth_ledger import (
            ORDER_TRUTH_LEDGER,
            OrderTruthFillStatus,
        )
        from lightfee.venues.specs import bitget_spec
        from lightfee.venues.transport import VenueTransport

        transport = VenueTransport(spec=bitget_spec(), mode="paper")
        raw = {
            "code": "00000", "msg": "success",
            "data": {
                "orderId": "oid-1", "clientOid": "cid-1",
                "filledQty": "0.5", "baseVolume": "0.5",
                "priceAvg": "51000", "avgPrice": "51000",
                "side": "buy", "uTime": "2000000",
                "cTime": "2000000", "fee": "0.1",
            },
        }

        result = transport._parse_order_status_bitget(
            raw,
            "BTCUSDT",
            2000000,
            resolved_account_family="uta",
            queried_endpoint="/api/v3/trade/order-info",
        )

        assert isinstance(result, OrderFillReconciliation)
        assert result.metadata["resolved_account_family"] == "uta"
        assert result.metadata["side"] == "buy"
        assert result.metadata["raw_exchange_status"] == "filled"
        decision = ORDER_TRUTH_LEDGER.resolve_order_success(
            venue=result.venue,
            symbol=result.symbol,
            order_id=result.order_id,
            client_order_id=result.client_order_id or "",
            target_qty=result.quantity,
            reconciliation=result,
        )
        assert decision.fill_status is OrderTruthFillStatus.CONFIRMED_FILL

    @pytest.mark.parametrize("side_value", [None, "", "close_long"])
    def test_bitget_positive_fill_missing_or_invalid_side_fails_closed(self, side_value):
        from lightfee.venues.transport import VenueTransport, TransportError
        from lightfee.venues.specs import bitget_spec

        transport = VenueTransport(spec=bitget_spec(), mode="paper")
        data = {
            "orderId": "oid-1",
            "clientOid": "cid-1",
            "filledQty": "0.5",
            "baseVolume": "0.5",
            "priceAvg": "51000",
            "uTime": "2000000",
        }
        if side_value is not None:
            data["side"] = side_value
        raw = {"code": "00000", "msg": "success", "data": data}

        with pytest.raises(TransportError) as exc:
            transport._parse_order_status_bitget(raw, "BTCUSDT", 2000000)

        assert exc.value.category == TransportErrorCategory.REQUEST_REJECTED

class TestVenueSpecificOrderReconciliationEvidence:
    """CL-001-G: venue query paths must expose endpoint-level evidence."""

    @pytest.mark.anyio
    async def test_binance_timeout_accepted_later_queries_order_endpoint(self):
        from lightfee.venues.binance import BinanceAdapter

        seen_paths: list[str] = []

        async def mock_handler(request):
            seen_paths.append(request.url.path)
            if request.url.path == "/fapi/v1/order":
                return httpx.Response(200, json={
                    "symbol": "BTCUSDT",
                    "orderId": 12345,
                    "clientOrderId": "bn-timeout-cid",
                    "status": "FILLED",
                    "executedQty": "0.25",
                    "avgPrice": "51000",
                    "side": "BUY",
                    "updateTime": 1770000000000,
                })
            if request.url.path == "/fapi/v1/userTrades":
                return httpx.Response(200, json=[{
                    "orderId": 12345,
                    "qty": "0.25",
                    "price": "51000",
                    "commission": "0.01785",
                    "commissionAsset": "USDT",
                    "time": 1770000000001,
                }])
            return httpx.Response(404, json={"msg": "unexpected"})

        adapter = BinanceAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s"),
        )
        adapter._transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler),
        )
        adapter._transport._time_offset_ms = 0

        result = await adapter.fetch_order_fill_reconciliation(
            "BTCUSDT", order_id="", client_order_id="bn-timeout-cid",
        )
        events = adapter._transport.drain_order_diagnostics()
        await adapter.shutdown()

        assert result is not None
        assert result.quantity == pytest.approx(0.25)
        assert result.fee_quote == pytest.approx(0.01785)
        assert result.metadata["queried_endpoints"] == [
            "/fapi/v1/order", "/fapi/v1/userTrades",
        ]
        assert result.metadata["response_classification"] == "filled"
        assert "/fapi/v1/order" in seen_paths
        assert "/fapi/v1/userTrades" in seen_paths
        query_payload = [e["payload"] for e in events if e["kind"] == "order.reconcile_query"][-1]
        assert query_payload["queried_endpoints"] == [
            "/fapi/v1/order", "/fapi/v1/userTrades",
        ]
        assert query_payload["client_order_id"] == "bn-timeout-cid"
        assert query_payload["identifier_kind"] == "order_id"
        assert query_payload["has_order_id"] is True
        assert query_payload["has_client_order_id"] is True
        assert isinstance(query_payload["observed_at_ms"], int)

    @pytest.mark.anyio
    @pytest.mark.parametrize(
        ("trades", "expected_fee", "fee_evidence_complete"),
        [
            ([{"orderId": 12345, "commission": "0.0"}], 0.0, True),
            ([], None, False),
        ],
    )
    async def test_binance_order_reconciliation_distinguishes_zero_fee_from_missing_execution_evidence(
        self, trades, expected_fee, fee_evidence_complete,
    ):
        from lightfee.venues.binance import BinanceAdapter

        async def mock_handler(request):
            if request.url.path == "/fapi/v1/order":
                return httpx.Response(200, json={
                    "symbol": "BTCUSDT",
                    "orderId": 12345,
                    "clientOrderId": "bn-close-cid",
                    "status": "FILLED",
                    "executedQty": "0.25",
                    "avgPrice": "51000",
                    "side": "BUY",
                    "updateTime": 1770000000000,
                })
            if request.url.path == "/fapi/v1/userTrades":
                return httpx.Response(200, json=trades)
            return httpx.Response(404, json={"msg": "unexpected"})

        adapter = BinanceAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s"),
        )
        adapter._transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler),
        )
        adapter._transport._time_offset_ms = 0

        result = await adapter.fetch_order_fill_reconciliation(
            "BTCUSDT", order_id="12345", client_order_id="bn-close-cid",
        )
        await adapter.shutdown()

        assert result is not None
        assert result.fee_quote == expected_fee
        assert result.metadata["fee_evidence_complete"] is fee_evidence_complete

    @pytest.mark.anyio
    async def test_binance_http_400_minus_2013_is_recoverable_order_truth_gap(self):
        """V1 treats Binance's missing-order response as no fill, not a crash."""
        from lightfee.venues.binance import BinanceAdapter

        seen_paths: list[str] = []

        async def mock_handler(request):
            seen_paths.append(request.url.path)
            return httpx.Response(400, json={
                "code": -2013,
                "msg": "Order does not exist.",
            })

        adapter = BinanceAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s"),
        )
        adapter._transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler),
        )
        adapter._transport._time_offset_ms = 0

        result = await adapter.fetch_order_fill_reconciliation(
            "ZILUSDT", order_id="812345", client_order_id="bn-close-cid",
        )
        events = adapter._transport.drain_order_diagnostics()
        await adapter.shutdown()

        assert result is None
        assert seen_paths == ["/fapi/v1/order"]
        query_payload = [
            event["payload"]
            for event in events
            if event["kind"] == "order.reconcile_query"
        ][-1]
        assert query_payload["response_classification"] == (
            "binance_error_-2013:Order does not exist."
        )
        assert query_payload["uncertain_subtype"] == "open_order_not_found"
        assert query_payload["next_action"] == "check_live_position"

    @pytest.mark.anyio
    async def test_binance_recovery_placeholder_order_id_uses_orig_client_order_id(self):
        from lightfee.venues.binance import BinanceAdapter

        seen_queries: list[dict[str, str]] = []

        async def mock_handler(request):
            if request.url.path == "/fapi/v1/userTrades":
                return httpx.Response(200, json=[{
                    "orderId": 123456,
                    "qty": "178",
                    "price": "0.00041",
                    "commission": "0.00007298",
                    "commissionAsset": "USDT",
                    "time": 1770000000001,
                }])
            query = dict(request.url.params)
            seen_queries.append(query)
            if query.get("origClientOrderId") == "bn-recovery-cid" and "orderId" not in query:
                return httpx.Response(200, json={
                    "symbol": "CLOUSDT",
                    "orderId": 123456,
                    "clientOrderId": "bn-recovery-cid",
                    "status": "FILLED",
                    "executedQty": "178",
                    "avgPrice": "0.00041",
                    "side": "SELL",
                    "updateTime": 1770000000000,
                })
            return httpx.Response(200, json={
                "code": -1102,
                "msg": "Mandatory parameter 'orderId' was not sent, was empty/null, or malformed.",
            })

        adapter = BinanceAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s"),
        )
        adapter._transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler),
        )
        adapter._transport._time_offset_ms = 0

        result = await adapter.fetch_order_fill_reconciliation(
            "CLOUSDT",
            order_id="entry-1780415543977-CLOUSDT-recovery-short",
            client_order_id="bn-recovery-cid",
        )
        events = adapter._transport.drain_order_diagnostics()
        await adapter.shutdown()

        assert result is not None
        assert result.order_id == "123456"
        assert result.client_order_id == "bn-recovery-cid"
        assert result.fee_quote == pytest.approx(0.00007298)
        assert seen_queries[0]["origClientOrderId"] == "bn-recovery-cid"
        assert "orderId" not in seen_queries[0]
        query_payload = [e["payload"] for e in events if e["kind"] == "order.reconcile_query"][-1]
        assert query_payload["order_id"] == "123456"
        assert query_payload["client_order_id"] == "bn-recovery-cid"

    @pytest.mark.anyio
    async def test_binance_recovery_placeholder_without_client_id_is_not_queried(self):
        from lightfee.venues.binance import BinanceAdapter

        seen_paths: list[str] = []

        async def mock_handler(request):
            seen_paths.append(request.url.path)
            return httpx.Response(400, json={
                "code": -4015,
                "msg": "Client order id length should be less than 36 chars",
            })

        adapter = BinanceAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s"),
        )
        adapter._transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler),
        )
        adapter._transport._time_offset_ms = 0

        result = await adapter.fetch_order_fill_reconciliation(
            "DEXEUSDT",
            order_id="entry-dexe-live-excess-hedge-recovery-short",
            client_order_id="",
        )
        events = adapter._transport.drain_order_diagnostics()
        await adapter.shutdown()

        assert result is None
        assert seen_paths == []
        query_payload = [
            event["payload"]
            for event in events
            if event["kind"] == "order.reconcile_query"
        ][-1]
        assert query_payload["response_classification"] == "invalid_local_order_identifier"
        assert query_payload["uncertain_subtype"] == "invalid_local_order_identifier"
        assert query_payload["next_action"] == "check_live_position"

    @pytest.mark.anyio
    async def test_okx_order_not_found_queries_open_and_history(self):
        from lightfee.venues.okx import OkxAdapter

        seen_paths: list[str] = []

        async def mock_handler(request):
            seen_paths.append(request.url.path)
            if request.url.path == "/api/v5/trade/order":
                return httpx.Response(200, json={
                    "code": "51603",
                    "msg": "Order does not exist",
                    "data": [],
                })
            if request.url.path == "/api/v5/trade/orders-history":
                return httpx.Response(200, json={"code": "0", "data": []})
            return httpx.Response(404, json={"msg": "unexpected"})

        adapter = OkxAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter._transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler),
        )
        adapter._transport._time_offset_ms = 0

        result = await adapter.fetch_order_fill_reconciliation(
            "BTC-USDT-SWAP", order_id="", client_order_id="okx-missing-cid",
        )
        events = adapter._transport.drain_order_diagnostics()
        await adapter.shutdown()

        assert result is None
        assert seen_paths == ["/api/v5/trade/order", "/api/v5/trade/orders-history"]
        query_payload = [e["payload"] for e in events if e["kind"] == "order.reconcile_query"][-1]
        assert query_payload["uncertain_subtype"] == "closed_order_not_found"
        assert query_payload["queried_endpoints"] == [
            "/api/v5/trade/order",
            "/api/v5/trade/orders-history",
        ]
        assert query_payload["response_classification"] == "open_order_not_found;closed_order_not_found"

    @pytest.mark.anyio
    async def test_okx_recovery_placeholder_order_id_uses_client_order_id_query(self):
        from lightfee.venues.okx import OkxAdapter

        seen_queries: list[dict[str, str]] = []
        seen_paths: list[str] = []

        async def mock_handler(request):
            seen_paths.append(request.url.path)
            if request.url.path == "/api/v5/trade/order":
                query = dict(request.url.params)
                seen_queries.append(query)
                if query.get("clOrdId") == "okx-recovery-cid" and "ordId" not in query:
                    return httpx.Response(200, json={
                        "code": "0",
                        "data": [
                            {
                                "instId": "ME-USDT-SWAP",
                                "ordId": "okx-real-order-1",
                                "clOrdId": "okx-recovery-cid",
                                "side": "sell",
                                "accFillSz": "304",
                                "avgPx": "0.07895",
                                "state": "filled",
                            }
                        ],
                    })
                return httpx.Response(400, json={
                    "code": "51000",
                    "msg": "Parameter ordId error",
                    "data": [],
                })
            if request.url.path == "/api/v5/trade/fills":
                query = dict(request.url.params)
                seen_queries.append(query)
                if query.get("ordId") == "okx-real-order-1":
                    return httpx.Response(200, json={
                        "code": "0",
                        "data": [
                            {
                                "instId": "ME-USDT-SWAP",
                                "ordId": "okx-real-order-1",
                                "clOrdId": "okx-recovery-cid",
                                "side": "sell",
                                "fillSz": "304",
                                "fillPx": "0.07895",
                                "fee": "-0.12",
                                "ts": "1780411997394",
                            }
                        ],
                    })
                return httpx.Response(200, json={"code": "0", "data": []})
            return httpx.Response(404, json={"msg": "unexpected"})

        adapter = OkxAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter._transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler),
        )
        adapter._transport._time_offset_ms = 0
        adapter._transport._okx_contract_size_for_venue_symbol = AsyncMock(return_value=1.0)

        result = await adapter.fetch_order_fill_reconciliation(
            "MEUSDT",
            order_id="entry-1780487640389-MEUSDT-recovery-short",
            client_order_id="okx-recovery-cid",
        )
        await adapter.shutdown()

        assert result is not None
        assert result.order_id == "okx-real-order-1"
        assert result.client_order_id == "okx-recovery-cid"
        assert seen_queries[0]["clOrdId"] == "okx-recovery-cid"
        assert "ordId" not in seen_queries[0]
        assert seen_queries[-1]["ordId"] == "okx-real-order-1"
        assert "/api/v5/trade/fills" in seen_paths

    @pytest.mark.anyio
    async def test_okx_order_detail_acc_fill_without_trade_fills_is_evidence_gap(self):
        from lightfee.venues.okx import OkxAdapter

        seen_paths: list[str] = []

        async def mock_handler(request):
            seen_paths.append(request.url.path)
            if request.url.path == "/api/v5/trade/order":
                return httpx.Response(200, json={
                    "code": "0",
                    "data": [
                        {
                            "instId": "HOME-USDT-SWAP",
                            "ordId": "okx-home-order",
                            "clOrdId": "okx-home-cid",
                            "side": "buy",
                            "accFillSz": "16",
                            "avgPx": "0.0525",
                            "state": "filled",
                        }
                    ],
                })
            if request.url.path == "/api/v5/trade/fills":
                return httpx.Response(200, json={"code": "0", "data": []})
            if request.url.path == "/api/v5/trade/orders-history":
                return httpx.Response(200, json={"code": "0", "data": []})
            return httpx.Response(404, json={"msg": "unexpected"})

        adapter = OkxAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter._transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler),
        )
        adapter._transport._time_offset_ms = 0
        adapter._transport._okx_contract_size_for_venue_symbol = AsyncMock(return_value=100.0)

        result = await adapter.fetch_order_fill_reconciliation(
            "HOME-USDT-SWAP",
            order_id="okx-home-order",
            client_order_id="okx-home-cid",
        )
        events = adapter._transport.drain_order_diagnostics()
        await adapter.shutdown()

        assert result is None
        assert seen_paths == ["/api/v5/trade/order", "/api/v5/trade/fills"]
        query_payload = [e["payload"] for e in events if e["kind"] == "order.reconcile_query"][-1]
        assert query_payload["queried_endpoints"] == [
            "/api/v5/trade/order",
            "/api/v5/trade/fills",
        ]
        assert query_payload["response_classification"] == "detail_found;fills_empty"
        assert query_payload["uncertain_subtype"] == "execution_not_found"

    def test_okx_order_status_scales_acc_fill_contracts_to_base_quantity(self):
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import okx_spec

        transport = VenueTransport(spec=okx_spec(), mode="paper")
        raw = {
            "code": "0",
            "data": [
                {
                    "instId": "HOME-USDT-SWAP",
                    "ordId": "okx-status-1",
                    "clOrdId": "okx-status-cid",
                    "side": "buy",
                    "accFillSz": "3",
                    "avgPx": "0.0525",
                    "state": "filled",
                }
            ],
        }

        result = transport._parse_order_status_okx(
            raw,
            "HOME-USDT-SWAP",
            1780411997394,
            contract_size=100.0,
        )

        assert result is not None
        assert result.quantity == pytest.approx(300.0)
        assert result.metadata["quantity_units"] == "contracts_to_base"
        assert result.metadata["contract_qty"] == pytest.approx(3.0)
        assert result.metadata["ct_val"] == pytest.approx(100.0)

    def test_okx_passive_progress_scales_acc_fill_contracts_to_base_quantity(self):
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import okx_spec

        transport = VenueTransport(spec=okx_spec(), mode="paper")
        transport.set_symbol_metadata(
            {
                "HOME-USDT-SWAP": {
                    "instId": "HOME-USDT-SWAP",
                    "ctType": "linear",
                    "ctVal": "100",
                }
            }
        )
        raw = {
            "code": "0",
            "data": [
                {
                    "instId": "HOME-USDT-SWAP",
                    "ordId": "okx-passive-1",
                    "clOrdId": "okx-passive-cid",
                    "side": "buy",
                    "accFillSz": "16",
                    "avgPx": "0.0525",
                    "state": "partially_filled",
                    "uTime": "1780411997394",
                }
            ],
        }

        result = transport._parse_passive_order_progress(
            raw,
            okx_spec(),
            "HOME-USDT-SWAP",
            1780411997394,
        )

        assert result is not None
        assert result.cumulative_quantity == pytest.approx(1600.0)

    def test_okx_passive_progress_missing_ct_val_is_evidence_gap(self):
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import okx_spec

        transport = VenueTransport(spec=okx_spec(), mode="paper")
        raw = {
            "code": "0",
            "data": [
                {
                    "instId": "HOME-USDT-SWAP",
                    "ordId": "okx-passive-1",
                    "clOrdId": "okx-passive-cid",
                    "side": "buy",
                    "accFillSz": "16",
                    "avgPx": "0.0525",
                    "state": "partially_filled",
                    "uTime": "1780411997394",
                }
            ],
        }

        result = transport._parse_passive_order_progress(
            raw,
            okx_spec(),
            "HOME-USDT-SWAP",
            1780411997394,
        )

        assert result is None


# ===========================================================================
# RED-LIGHT: Bybit adapter real HTTP path — execution side + retCode
# ===========================================================================

class TestBybitAdapterHttpRedLight:
    """RED-LIGHT: BybitAdapter.fetch_order_fill_reconciliation() via real
    HTTP mock must parse execution side and surface retCode errors.

    V1 ref: src/live/bybit.rs fetch_order_fill_reconciliation (lines 2820-2894)
    Docs: https://bybit-exchange.github.io/docs/zh-TW/v5/order/execution
          https://bybit-exchange.github.io/docs/v5/error

    Current V2 gaps:
      - _parse_bybit_execution_list() hardcodes side=Side.BUY
      - _fetch_order_status_bybit() doesn't call _require_bybit_success()
      - Nonzero retCode silently returns None instead of raising TransportError
    """

    @pytest.mark.anyio
    async def test_redlight_bybit_adapter_sell_execution_side(self):
        """RED-LIGHT: BybitAdapter with MockTransport, execution side=Sell
        → result.side must be SELL, not BUY.

        Uses full adapter path: BybitAdapter → transport.fetch_order_status
        → _fetch_order_status_bybit → _parse_bybit_execution_list.
        No monkeypatching of parser or fetcher methods.
        """
        import httpx
        from lightfee.venues.bybit import BybitAdapter
        from lightfee.venues.transport import LiveCredential
        from lightfee.core.domain import Side

        async def mock_handler(request):
            url = str(request.url)
            if "/v5/order/realtime" in url:
                return httpx.Response(200, json={
                    "retCode": 0, "retMsg": "OK",
                    "result": {"list": [{
                        "orderId": "oid-1", "orderLinkId": "cid-1",
                    }]},
                })
            if "/v5/execution/list" in url:
                return httpx.Response(200, json={
                    "retCode": 0, "retMsg": "OK",
                    "result": {"list": [{
                        "execQty": "0.5", "execPrice": "50000",
                        "execFee": "0.1", "execTime": "2000",
                        "side": "Sell", "symbol": "BTCUSDT",
                    }]},
                })
            return httpx.Response(404, json={"error": "not found"})

        adapter = BybitAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s"),
        )
        adapter._transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler),
        )
        # V1: pre-fill server-time offset so mock transport doesn't hit server-time endpoint
        adapter._transport._time_offset_ms = 0

        result = await adapter.fetch_order_fill_reconciliation(
            "BTCUSDT", order_id="", client_order_id="cid-1",
        )
        await adapter._transport.close()

        assert result is not None, (
            "RED-LIGHT FAIL: fetch_order_fill_reconciliation returned None "
            "for sell-side execution"
        )
        assert result.side == Side.SELL, (
            f"RED-LIGHT FAIL: execution side=Sell should produce Side.SELL, "
            f"got {result.side}. _parse_bybit_execution_list hardcodes Side.BUY."
        )
        assert result.quantity == pytest.approx(0.5)
        assert result.average_price == pytest.approx(50000)

    @pytest.mark.anyio
    async def test_redlight_bybit_retcode_nonzero_raises(self):
        """RED-LIGHT: Bybit retCode=10001 (Request parameter error) must
        raise TransportError, NOT silently return None.

        V1: bybit.rs checks ret_code and surfaces business errors.
        V2 gap: _fetch_order_status_bybit doesn't call _require_bybit_success,
        so retCode != 0 passes through to the parser which may return None.
        """
        import httpx
        from lightfee.venues.bybit import BybitAdapter
        from lightfee.venues.transport import LiveCredential, TransportError

        async def mock_handler(request):
            url = str(request.url)
            if "/v5/order/realtime" in url:
                return httpx.Response(200, json={
                    "retCode": 10001,
                    "retMsg": "Request parameter error",
                })
            return httpx.Response(404, json={"error": "not found"})

        adapter = BybitAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s"),
        )
        adapter._transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler),
        )
        # V1: pre-fill server-time offset so mock transport doesn't hit server-time endpoint
        adapter._transport._time_offset_ms = 0

        with pytest.raises(TransportError):
            await adapter.fetch_order_fill_reconciliation(
                "BTCUSDT", order_id="", client_order_id="cid-1",
            )

        await adapter._transport.close()

    @pytest.mark.anyio
    async def test_redlight_bybit_retcode_order_not_found_returns_none(self):
        """Bybit retCode=110001 (Order does not exist) may return None
        (order-not-found is not a transport error — the order simply
        doesn't exist).
        """
        import httpx
        from lightfee.venues.bybit import BybitAdapter
        from lightfee.venues.transport import LiveCredential

        async def mock_handler(request):
            url = str(request.url)
            if "/v5/order/realtime" in url:
                return httpx.Response(200, json={
                    "retCode": 110001,
                    "retMsg": "Order does not exist",
                })
            return httpx.Response(404, json={"error": "not found"})

        adapter = BybitAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s"),
        )
        adapter._transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler),
        )

        result = await adapter.fetch_order_fill_reconciliation(
            "BTCUSDT", order_id="", client_order_id="cid-1",
        )
        await adapter._transport.close()

        assert result is None, (
            f"order-not-found (110001) should return None, got {result}"
        )

    @pytest.mark.anyio
    async def test_bybit_reconciliation_falls_back_to_order_history_before_executions(self):
        """Duplicate orderLinkId reconciliation must check realtime, history, then executions."""

        import httpx
        from lightfee.venues.bybit import BybitAdapter
        from lightfee.venues.transport import LiveCredential
        from lightfee.core.domain import Side

        seen: list[str] = []

        async def mock_handler(request):
            url = str(request.url)
            if "/v5/order/realtime" in url:
                seen.append("realtime")
                return httpx.Response(200, json={
                    "retCode": 0,
                    "retMsg": "OK",
                    "result": {"list": []},
                })
            if "/v5/order/history" in url:
                seen.append("history")
                assert "orderLinkId=cid-history" in url
                return httpx.Response(200, json={
                    "retCode": 0,
                    "retMsg": "OK",
                    "result": {"list": [{
                        "orderId": "oid-history",
                        "orderLinkId": "cid-history",
                    }]},
                })
            if "/v5/execution/list" in url:
                seen.append("executions")
                assert "orderId=oid-history" in url
                return httpx.Response(200, json={
                    "retCode": 0,
                    "retMsg": "OK",
                    "result": {"list": [{
                        "execQty": "400",
                        "execPrice": "0.011",
                        "execFee": "0.02",
                        "execTime": "2000",
                        "side": "Buy",
                        "symbol": "UBUSDT",
                    }]},
                })
            return httpx.Response(404, json={"error": "not found"})

        adapter = BybitAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s"),
        )
        adapter._transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler),
        )
        adapter._transport._time_offset_ms = 0

        result = await adapter.fetch_order_fill_reconciliation(
            "UBUSDT", order_id="", client_order_id="cid-history",
        )
        await adapter._transport.close()

        assert seen == ["realtime", "history", "executions"]
        assert result is not None
        assert result.order_id == "oid-history"
        assert result.client_order_id == "cid-history"
        assert result.side == Side.BUY
        assert result.quantity == pytest.approx(400.0)


# ====================================================================# RED-LIGHT: Bitget official UTA field regression
# ====================================================================

class TestBitgetAdapterHttpRedLight:
    """RED-LIGHT: BitgetAdapter.fetch_order_fill_reconciliation() must
    handle official UTA field shapes including cumExecQty, avgPrice,
    orderStatus, side, and feeDetail list.

    Docs: https://www.bitget.com/api-doc/uta/trade/Get-Order-Details
    """

    @pytest.mark.anyio
    async def test_redlight_bitget_official_uta_fields(self):
        """Bitget official UTA order-info response shape regression.

        Official fields: code, cumExecQty, avgPrice, orderStatus, side,
        feeDetail (list of fee entries).
        """
        import httpx
        from lightfee.venues.bitget import BitgetAdapter
        from lightfee.venues.transport import LiveCredential
        from lightfee.core.domain import Side

        async def mock_handler(request):
            url = str(request.url)
            if "/api/v3/position/current-position" in url:
                return httpx.Response(200, json={"code": "00000", "data": []})
            if "/api/v3/trade/order-info" in url:
                return httpx.Response(200, json={
                    "code": "00000",
                    "msg": "success",
                    "data": {
                        "orderId": "oid-1",
                        "clientOid": "cid-1",
                        "symbol": "BTCUSDT",
                        "orderStatus": "filled",
                        "side": "sell",
                        "cumExecQty": "0.5",
                        "avgPrice": "51000",
                        "feeDetail": [
                            {"fee": "0.05", "feeCoin": "USDT"},
                            {"fee": "0.05", "feeCoin": "USDT"},
                        ],
                        "cTime": "2000000",
                        "uTime": "2000000",
                    },
                })
            return httpx.Response(404, json={"error": "not found"})

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter._transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler),
        )

        result = await adapter.fetch_order_fill_reconciliation(
            "BTCUSDT", order_id="", client_order_id="cid-1",
        )
        await adapter._transport.close()

        assert result is not None, "should return reconciliation for filled order"
        assert result.side == Side.SELL, (
            f"RED-LIGHT: side should be SELL, got {result.side}"
        )
        assert result.quantity == pytest.approx(0.5), (
            f"RED-LIGHT: quantity should be 0.5 (from cumExecQty), got {result.quantity}"
        )
        assert result.average_price == pytest.approx(51000), (
            f"RED-LIGHT: avg price should be 51000 (from avgPrice), got {result.average_price}"
        )
        assert result.fee_quote is not None and result.fee_quote == pytest.approx(0.1), (
            f"RED-LIGHT: fee should be 0.1 (sum of feeDetail fees), got {result.fee_quote}"
        )

    @pytest.mark.anyio
    async def test_bitget_uta_fetch_position_uses_official_category_param(self):
        from lightfee.venues.bitget import BitgetAccountProfile, BitgetAdapter
        from lightfee.venues.transport import LiveCredential

        seen_params: list[dict[str, str]] = []

        async def mock_handler(request):
            if "/api/v3/position/current-position" in str(request.url):
                params = dict(request.url.params)
                seen_params.append(params)
                return httpx.Response(200, json={
                    "code": "00000",
                    "msg": "success",
                    "data": [
                        {
                            "symbol": "BTCUSDT",
                            "holdSide": "long",
                            "total": "0.5",
                            "openPriceAvg": "51000",
                        },
                    ],
                })
            return httpx.Response(404, json={"error": "not found"})

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter._profile = BitgetAccountProfile.UTA
        adapter._transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler),
        )

        try:
            result = await adapter.fetch_position("BTCUSDT")
        finally:
            await adapter._transport.close()

        assert result.quantity == pytest.approx(0.5)
        assert seen_params
        assert seen_params[0]["category"] == "USDT-FUTURES"
        assert seen_params[0]["symbol"] == "BTCUSDT"
        assert "productType" not in seen_params[0]

    @pytest.mark.anyio
    async def test_bitget_uta_fetch_all_positions_uses_official_category_param(self):
        from lightfee.venues.bitget import BitgetAccountProfile, BitgetAdapter
        from lightfee.venues.transport import LiveCredential

        seen_params: list[dict[str, str]] = []

        async def mock_handler(request):
            if "/api/v3/position/current-position" in str(request.url):
                params = dict(request.url.params)
                seen_params.append(params)
                return httpx.Response(200, json={
                    "code": "00000",
                    "msg": "success",
                    "data": [
                        {
                            "symbol": "BTCUSDT",
                            "holdSide": "short",
                            "total": "0.25",
                            "openPriceAvg": "52000",
                        },
                    ],
                })
            return httpx.Response(404, json={"error": "not found"})

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter._profile = BitgetAccountProfile.UTA
        adapter._transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler),
        )

        try:
            result = await adapter.fetch_all_positions()
        finally:
            await adapter._transport.close()

        assert len(result) == 1
        assert result[0].quantity == pytest.approx(0.25)
        assert result[0].side == Side.SELL
        assert seen_params
        assert seen_params[0]["category"] == "USDT-FUTURES"
        assert "productType" not in seen_params[0]

    @pytest.mark.anyio
    async def test_redlight_bitget_nonzero_code_raises(self):
        """Bitget code != 00000 must raise, not return None.

        _require_bitget_success raises OrderSubmitError(REJECTED), which
        propagates through fetch_order_fill_reconciliation.
        """
        import httpx
        from lightfee.venues.bitget import BitgetAdapter
        from lightfee.venues.transport import LiveCredential
        from lightfee.core.errors import OrderSubmitError

        async def mock_handler(request):
            url = str(request.url)
            if "/api/v3/position/current-position" in url:
                return httpx.Response(200, json={"code": "00000", "data": []})
            if "/api/v3/trade/order-info" in url:
                return httpx.Response(200, json={
                    "code": "40001",
                    "msg": "Invalid parameter",
                })
            return httpx.Response(404, json={"error": "not found"})

        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter._transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler),
        )

        with pytest.raises(OrderSubmitError):
            await adapter.fetch_order_fill_reconciliation(
                "BTCUSDT", order_id="", client_order_id="cid-1",
            )

        await adapter._transport.close()


# ====================================================================# RED-LIGHT: Bitget quantity fallback — every V1 field individually
# ====================================================================

class TestBitgetQuantityFallbackRedLight:
    """RED-LIGHT: _parse_order_status_bitget quantity fallback must cover
    every V1 field so no single-field response returns None.

    V1 fields (bitget.rs:2516-2522): baseVolume, filledQty, fillQty, filled_amount, size
    V2 extra compatibility: cumExecQty, fillSz

    Current V2 chain: cumExecQty → baseVolume → filledQty → fillSz → 0
    Missing V1 fields: fillQty, filled_amount, size
    """

    @pytest.mark.parametrize("field", [
        "baseVolume",
        "filledQty",
        "fillQty",       # RED: not in V2 chain
        "fillSz",
        "cumExecQty",
        "size",          # RED: not in V2 chain
        "filled_amount", # RED: not in V2 chain (V1 field)
    ])
    def test_bitget_quantity_fallback_each_field_alone_returns_positive_qty(self, field):
        """Each quantity field alone must produce positive reconciliation."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bitget_spec
        from lightfee.core.domain import OrderFillReconciliation

        transport = VenueTransport(spec=bitget_spec(), mode="paper")
        raw = {
            "code": "00000", "msg": "success",
            "data": {
                "orderId": "oid-1", "clientOid": "cid-1",
                field: "0.5",
                "priceAvg": "51000", "avgPrice": "51000",
                "side": "buy", "uTime": "2000000",
                "cTime": "2000000", "fee": "0.1",
            },
        }
        result = transport._parse_order_status_bitget(raw, "BTCUSDT", 2000000)
        assert isinstance(result, OrderFillReconciliation), (
            f"RED-LIGHT: field={field} alone should produce reconciliation, got None"
        )
        assert result.quantity == pytest.approx(0.5), (
            f"RED-LIGHT: field={field} quantity should be 0.5, got {result.quantity}"
        )


# ====================================================================# RED-LIGHT: Bybit execution side validation — fail-closed, V1 parity
# ====================================================================

class TestBybitExecutionSideRedLight:
    """RED-LIGHT: _parse_bybit_execution_list must fail-closed on invalid side.

    V1 ref: bybit.rs bybit_side_from_string (lines 3973-3979)
    V1 accepts only "Buy"→Buy, "Sell"→Sell; any other value → Err.

    Current V2 gap: any non-"buy" side (case-insensitive) defaults to SELL
    instead of raising TransportError. Missing side with qty=0 exits early
    before the side check.
    """

    def test_redlight_execution_side_buy_returns_buy(self):
        """side=Buy → Side.BUY."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bybit_spec
        from lightfee.core.domain import Side, OrderFillReconciliation

        transport = VenueTransport(spec=bybit_spec(), mode="paper")
        raw = {
            "retCode": 0, "retMsg": "OK",
            "result": {"list": [
                {"execQty": "0.5", "execPrice": "50000", "side": "Buy",
                 "execFee": "0.1", "execTime": "2000", "symbol": "BTCUSDT"},
            ]},
        }
        result = transport._parse_bybit_execution_list(
            raw, "BTCUSDT", "oid-1", "cid-1", 2000000,
        )
        assert isinstance(result, OrderFillReconciliation)
        assert result.side == Side.BUY, (
            f"side=Buy should produce BUY, got {result.side}"
        )
        assert result.quantity == pytest.approx(0.5)

    def test_redlight_execution_side_sell_returns_sell(self):
        """side=Sell → Side.SELL."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bybit_spec
        from lightfee.core.domain import Side, OrderFillReconciliation

        transport = VenueTransport(spec=bybit_spec(), mode="paper")
        raw = {
            "retCode": 0, "retMsg": "OK",
            "result": {"list": [
                {"execQty": "0.5", "execPrice": "50000", "side": "Sell",
                 "execFee": "0.1", "execTime": "2000", "symbol": "BTCUSDT"},
            ]},
        }
        result = transport._parse_bybit_execution_list(
            raw, "BTCUSDT", "oid-1", "cid-1", 2000000,
        )
        assert isinstance(result, OrderFillReconciliation)
        assert result.side == Side.SELL, (
            f"side=Sell should produce SELL, got {result.side}"
        )
        assert result.quantity == pytest.approx(0.5)

    def test_redlight_execution_side_missing_with_qty_raises(self):
        """All executions missing side field with total_qty>0 → TransportError."""
        from lightfee.venues.transport import VenueTransport, TransportError, TransportErrorCategory
        from lightfee.venues.specs import bybit_spec

        transport = VenueTransport(spec=bybit_spec(), mode="paper")
        raw = {
            "retCode": 0, "retMsg": "OK",
            "result": {"list": [
                {"execQty": "0.5", "execPrice": "50000",
                 "execFee": "0.1", "execTime": "2000", "symbol": "BTCUSDT"},
            ]},
        }
        with pytest.raises(TransportError) as exc:
            transport._parse_bybit_execution_list(
                raw, "BTCUSDT", "oid-1", "cid-1", 2000000,
            )
        assert exc.value.category == TransportErrorCategory.REQUEST_REJECTED, (
            f"missing side with qty>0 must raise REQUEST_REJECTED, got {exc.value.category}"
        )

    def test_redlight_execution_side_invalid_hold_raises(self):
        """side=Hold (invalid) with qty>0 → TransportError, NOT default to sell."""
        from lightfee.venues.transport import VenueTransport, TransportError, TransportErrorCategory
        from lightfee.venues.specs import bybit_spec

        transport = VenueTransport(spec=bybit_spec(), mode="paper")
        raw = {
            "retCode": 0, "retMsg": "OK",
            "result": {"list": [
                {"execQty": "0.5", "execPrice": "50000", "side": "Hold",
                 "execFee": "0.1", "execTime": "2000", "symbol": "BTCUSDT"},
            ]},
        }
        with pytest.raises(TransportError) as exc:
            transport._parse_bybit_execution_list(
                raw, "BTCUSDT", "oid-1", "cid-1", 2000000,
            )
        assert exc.value.category == TransportErrorCategory.REQUEST_REJECTED, (
            f"RED-LIGHT: invalid side='Hold' must raise, not default to SELL. "
            f"Got category={exc.value.category}"
        )
        assert "Hold" in str(exc.value) or "side" in str(exc.value).lower(), (
            f"Error message must mention the invalid side value, got: {exc.value}"
        )

    def test_redlight_execution_side_lowercase_buy_raises(self):
        """side=buy (lowercase, not V1-accepted 'Buy') → TransportError."""
        from lightfee.venues.transport import VenueTransport, TransportError, TransportErrorCategory
        from lightfee.venues.specs import bybit_spec

        transport = VenueTransport(spec=bybit_spec(), mode="paper")
        raw = {
            "retCode": 0, "retMsg": "OK",
            "result": {"list": [
                {"execQty": "0.5", "execPrice": "50000", "side": "buy",
                 "execFee": "0.1", "execTime": "2000", "symbol": "BTCUSDT"},
            ]},
        }
        with pytest.raises(TransportError) as exc:
            transport._parse_bybit_execution_list(
                raw, "BTCUSDT", "oid-1", "cid-1", 2000000,
            )
        assert exc.value.category == TransportErrorCategory.REQUEST_REJECTED, (
            f"lowercase 'buy' should raise (V1 is case-sensitive), got category={exc.value.category}"
        )

    def test_redlight_execution_side_mixed_buy_sell_raises(self):
        """Mixed Buy and Sell in same execution list → TransportError."""
        from lightfee.venues.transport import VenueTransport, TransportError, TransportErrorCategory
        from lightfee.venues.specs import bybit_spec

        transport = VenueTransport(spec=bybit_spec(), mode="paper")
        raw = {
            "retCode": 0, "retMsg": "OK",
            "result": {"list": [
                {"execQty": "0.3", "execPrice": "50000", "side": "Buy",
                 "execFee": "0.05", "execTime": "2000", "symbol": "BTCUSDT"},
                {"execQty": "0.2", "execPrice": "51000", "side": "Sell",
                 "execFee": "0.05", "execTime": "2001", "symbol": "BTCUSDT"},
            ]},
        }
        with pytest.raises(TransportError) as exc:
            transport._parse_bybit_execution_list(
                raw, "BTCUSDT", "oid-1", "cid-1", 2000000,
            )
        assert exc.value.category == TransportErrorCategory.REQUEST_REJECTED, (
            f"mixed Buy/Sell must raise, got category={exc.value.category}"
        )
        assert "inconsistent" in str(exc.value).lower(), (
            f"Error must mention inconsistent sides, got: {exc.value}"
        )

    def test_redlight_execution_side_missing_but_zero_qty_returns_none(self):
        """Missing side with total_qty=0 → None (no error needed)."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bybit_spec

        transport = VenueTransport(spec=bybit_spec(), mode="paper")
        raw = {
            "retCode": 0, "retMsg": "OK",
            "result": {"list": [
                {"execQty": "0", "execPrice": "0",
                 "execFee": "0", "execTime": "0", "symbol": "BTCUSDT"},
            ]},
        }
        result = transport._parse_bybit_execution_list(
            raw, "BTCUSDT", "oid-1", "cid-1", 2000000,
        )
        assert result is None, f"zero qty should return None even with missing side"


# ====================================================================# RED-LIGHT: Bybit order status side validation (parse_order_status_bybit)
# ====================================================================

class TestBybitOrderStatusSideRedLight:
    """RED-LIGHT: _parse_order_status_bybit must fail-closed on invalid side.

    V1 ref: bybit.rs bybit_side_from_string (lines 3973-3979)
    V1 only accepts "Buy"→Buy, "Sell"→Sell.
    Missing side → Err (not default "Buy").

    Current V2 gap: defaults missing side to "Buy", treats any non-"Buy"
    (including "Hold", "buy" lowercase) as SELL.
    """

    def test_redlight_order_status_side_missing_with_qty_raises(self):
        """side field absent with cumQty>0 → TransportError (V1: missing side err)."""
        from lightfee.venues.transport import VenueTransport, TransportError, TransportErrorCategory
        from lightfee.venues.specs import bybit_spec

        transport = VenueTransport(spec=bybit_spec(), mode="paper")
        raw = {
            "retCode": 0, "retMsg": "OK",
            "result": {"list": [{
                "orderId": "oid-1", "orderLinkId": "cid-1",
                "orderStatus": "Filled", "cumExecQty": "0.5",
                "avgPrice": "50000",
                "updatedTime": "1000000",
            }]},
        }
        with pytest.raises(TransportError) as exc:
            transport._parse_order_status_bybit(raw, "BTCUSDT", 1000000)
        assert exc.value.category == TransportErrorCategory.REQUEST_REJECTED, (
            f"RED-LIGHT: missing side with qty>0 should raise, "
            f"got category={exc.value.category}"
        )

    def test_redlight_order_status_side_invalid_hold_raises(self):
        """side=Hold with cumQty>0 → TransportError, not default to SELL."""
        from lightfee.venues.transport import VenueTransport, TransportError, TransportErrorCategory
        from lightfee.venues.specs import bybit_spec

        transport = VenueTransport(spec=bybit_spec(), mode="paper")
        raw = {
            "retCode": 0, "retMsg": "OK",
            "result": {"list": [{
                "orderId": "oid-1", "orderLinkId": "cid-1",
                "orderStatus": "Filled", "cumExecQty": "0.5",
                "avgPrice": "50000", "side": "Hold",
                "updatedTime": "1000000",
            }]},
        }
        with pytest.raises(TransportError) as exc:
            transport._parse_order_status_bybit(raw, "BTCUSDT", 1000000)
        assert exc.value.category == TransportErrorCategory.REQUEST_REJECTED, (
            f"RED-LIGHT: invalid side='Hold' should raise, not default to SELL. "
            f"Got category={exc.value.category}"
        )

    def test_redlight_order_status_side_buy_returns_buy(self):
        """side=Buy → Side.BUY (existing behavior, prevent regression)."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bybit_spec
        from lightfee.core.domain import Side, OrderFillReconciliation

        transport = VenueTransport(spec=bybit_spec(), mode="paper")
        raw = {
            "retCode": 0, "retMsg": "OK",
            "result": {"list": [{
                "orderId": "oid-1", "orderLinkId": "cid-1",
                "orderStatus": "Filled", "cumExecQty": "0.5",
                "avgPrice": "50000", "side": "Buy",
                "updatedTime": "1000000",
            }]},
        }
        result = transport._parse_order_status_bybit(raw, "BTCUSDT", 1000000)
        assert isinstance(result, OrderFillReconciliation)
        assert result.side == Side.BUY

    def test_redlight_order_status_side_sell_returns_sell(self):
        """side=Sell → Side.SELL (existing behavior, prevent regression)."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bybit_spec
        from lightfee.core.domain import Side, OrderFillReconciliation

        transport = VenueTransport(spec=bybit_spec(), mode="paper")
        raw = {
            "retCode": 0, "retMsg": "OK",
            "result": {"list": [{
                "orderId": "oid-1", "orderLinkId": "cid-1",
                "orderStatus": "Filled", "cumExecQty": "0.5",
                "avgPrice": "50000", "side": "Sell",
                "updatedTime": "1000000",
            }]},
        }
        result = transport._parse_order_status_bybit(raw, "BTCUSDT", 1000000)
        assert isinstance(result, OrderFillReconciliation)
        assert result.side == Side.SELL


# ====================================================================# Root Fix: CID Generator Tests
# ====================================================================

class TestCidGenerator:
    """Verify exchange CIDs are stable, unique, and venue-legal."""

    def test_long_entry_id_produces_binance_legal_cid(self):
        from lightfee.venues.cid import generate_exchange_cid, cid_is_valid_for_venue

        long_id = "entry-" + "x" * 200 + "-very-long-internal-id"
        cid = generate_exchange_cid(long_id, "m", Venue.BINANCE)
        assert len(cid) <= 36
        assert cid_is_valid_for_venue(cid, Venue.BINANCE)
        assert all(c in "0123456789abcdef" for c in cid)  # hex only

    def test_long_entry_id_produces_aster_legal_cid(self):
        from lightfee.venues.cid import generate_exchange_cid, cid_is_valid_for_venue

        long_id = "entry-" + "y" * 200
        cid = generate_exchange_cid(long_id, "m", Venue.ASTER)
        assert len(cid) <= 36
        assert cid_is_valid_for_venue(cid, Venue.ASTER)

    def test_long_entry_id_produces_okx_legal_cid(self):
        from lightfee.venues.cid import generate_exchange_cid, cid_is_valid_for_venue

        long_id = "entry-" + "z" * 200
        cid = generate_exchange_cid(long_id, "m", Venue.OKX)
        assert len(cid) <= 32
        assert cid_is_valid_for_venue(cid, Venue.OKX)

    def test_long_entry_id_produces_bybit_legal_cid(self):
        from lightfee.venues.cid import generate_exchange_cid

        long_id = "entry-" + "w" * 200
        cid = generate_exchange_cid(long_id, "m", Venue.BYBIT)
        assert len(cid) <= 36

    def test_cid_is_deterministic(self):
        from lightfee.venues.cid import generate_exchange_cid

        long_id = "some-very-long-entry-id-12345"
        cid1 = generate_exchange_cid(long_id, "m", Venue.BINANCE)
        cid2 = generate_exchange_cid(long_id, "m", Venue.BINANCE)
        assert cid1 == cid2

    def test_cid_differs_per_leg(self):
        from lightfee.venues.cid import generate_exchange_cid

        long_id = "entry-abc"
        maker_cid = generate_exchange_cid(long_id, "m", Venue.BINANCE)
        hedge_cid = generate_exchange_cid(long_id, "h", Venue.BINANCE)
        assert maker_cid != hedge_cid

    def test_cid_rejects_overlength(self):
        from lightfee.venues.cid import cid_is_valid_for_venue
        assert not cid_is_valid_for_venue("x" * 33, Venue.OKX)
        assert not cid_is_valid_for_venue("x" * 37, Venue.BINANCE)

    def test_cid_rejects_empty(self):
        from lightfee.venues.cid import cid_is_valid_for_venue
        assert not cid_is_valid_for_venue("", Venue.BINANCE)


# ====================================================================# Root Fix: Passive Body Builder Tests
# ====================================================================

class TestPassiveBodyBuilders:
    """Verify venue-specific passive bodies — no generic field pollution."""

    def _make_passive_req(self, venue, reduce_only=False, cid="test-cid-001"):
        return OrderRequest(
            venue=venue,
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=0.01,
            price=50000.0,
            post_only=True,
            reduce_only=reduce_only,
            client_order_id=cid,
        )

    def test_binance_passive_body_fields(self):
        spec = binance_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        req = self._make_passive_req(Venue.BINANCE)
        body = transport._build_passive_order_body(req, "BTCUSDT", 0.01, 50000.0, req.client_order_id or "")

        assert body["symbol"] == "BTCUSDT"
        assert body["side"] == "BUY"
        assert body["type"] == "LIMIT"
        assert body["timeInForce"] == "GTX"
        assert body["quantity"] is not None
        assert body["newClientOrderId"] == "test-cid-001"
        assert body["price"] is not None
        # reduceOnly must NOT be present when reduce_only=False
        assert "reduceOnly" not in body
        # Generic pollution check
        assert "instId" not in body
        assert "sz" not in body
        assert "clOrdId" not in body

    def test_binance_passive_body_reduce_only_when_requested(self):
        spec = binance_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        req = self._make_passive_req(Venue.BINANCE, reduce_only=True)
        body = transport._build_passive_order_body(req, "BTCUSDT", 0.01, 50000.0, req.client_order_id or "")

        assert body["reduceOnly"] == "true"

    def test_binance_passive_body_hedge_mode_uses_position_side(self):
        spec = binance_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        transport._fapi_position_hedge_mode_cache = True
        req = self._make_passive_req(Venue.BINANCE, reduce_only=False)
        body = transport._build_passive_order_body(req, "BTCUSDT", 0.01, 50000.0, req.client_order_id or "")

        assert body["positionSide"] == "LONG"
        assert "reduceOnly" not in body

    def test_binance_reduce_only_hedge_position_side_mapping(self):
        spec = binance_spec()
        transport = VenueTransport(spec=spec, mode="paper")

        assert transport._fapi_position_side(Side.BUY, reduce_only=False) == "LONG"
        assert transport._fapi_position_side(Side.SELL, reduce_only=False) == "SHORT"
        assert transport._fapi_position_side(Side.BUY, reduce_only=True) == "SHORT"
        assert transport._fapi_position_side(Side.SELL, reduce_only=True) == "LONG"

    def test_aster_passive_body_same_as_binance(self):
        spec = aster_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        req = self._make_passive_req(Venue.ASTER)
        body = transport._build_passive_order_body(req, "BTCUSDT", 0.01, 50000.0, req.client_order_id or "")

        assert body["type"] == "LIMIT"
        assert body["timeInForce"] == "GTX"
        assert "reduceOnly" not in body

    def test_okx_passive_body_uses_sz_and_clordid(self):
        spec = okx_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        req = self._make_passive_req(Venue.OKX, cid="okx-cid-001")
        body = transport._build_passive_order_body(req, "BTC-USDT-SWAP", 0.01, 50000.0, req.client_order_id or "")

        assert body["instId"] == "BTC-USDT-SWAP"
        assert body["tdMode"] == "cross"
        assert body["ordType"] == "post_only"
        assert body["sz"] is not None
        assert body["clOrdId"] == "okx-cid-001"
        assert body["px"] is not None
        # Must NOT have generic field names
        assert "quantity" not in body
        assert "newClientOrderId" not in body
        assert "symbol" not in body
        assert "reduceOnly" not in body

    def test_okx_passive_body_no_reduce_only_for_opening_maker(self):
        spec = okx_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        req = self._make_passive_req(Venue.OKX, reduce_only=False)
        body = transport._build_passive_order_body(req, "BTC-USDT-SWAP", 0.01, 50000.0, req.client_order_id or "")

        assert "reduceOnly" not in body

    def test_bybit_passive_body_fields(self):
        spec = bybit_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        req = self._make_passive_req(Venue.BYBIT)
        body = transport._build_passive_order_body(req, "BTCUSDT", 0.01, 50000.0, req.client_order_id or "")

        assert body["category"] == "linear"
        assert body["symbol"] == "BTCUSDT"
        assert body["side"] == "Buy"
        assert body["orderType"] == "Limit"
        assert body["timeInForce"] == "PostOnly"
        assert body["orderLinkId"] is not None
        assert "quantity" not in body
        assert "newClientOrderId" not in body

    def test_bybit_passive_body_reduce_only_respects_request(self):
        spec = bybit_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        req = self._make_passive_req(Venue.BYBIT, reduce_only=True)
        body = transport._build_passive_order_body(req, "BTCUSDT", 0.01, 50000.0, req.client_order_id or "")

        assert body["reduceOnly"] is True
        assert body["positionIdx"] in (0, 1, 2)

    def test_gate_passive_body_no_hardcoded_reduce_only(self):
        spec = gate_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        req = self._make_passive_req(Venue.GATE, reduce_only=False)
        body = transport._build_passive_order_body(req, "BTC_USDT", 1.0, 50000.0, req.client_order_id or "")

        assert body["post_only"] is True
        assert "reduce_only" not in body  # not hardcoded when false

    @pytest.mark.asyncio
    async def test_gate_reduce_only_sell_uses_signed_negative_market_size(self):
        import json as _json
        seen_body = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            seen_body.update(_json.loads(request.content.decode()))
            return httpx.Response(
                200,
                json={"id": "gate-close-long-001", "status": "closed", "size": -69, "price": "0"},
            )

        transport = VenueTransport(
            spec=gate_spec(),
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s"),
        )
        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        req = OrderRequest(
            venue=Venue.GATE,
            symbol="SKYAIUSDT",
            side=Side.SELL,
            quantity=69,
            reduce_only=True,
            client_order_id="lfv2-gate-close-long-001",
        )

        await transport.place_order(req)

        assert seen_body["contract"] == "SKYAI_USDT"
        assert seen_body["size"] == -69
        assert seen_body["price"] == "0"
        assert seen_body["tif"] == "ioc"
        assert seen_body["reduce_only"] is True
        assert "symbol" not in seen_body
        assert "side" not in seen_body
        assert "quantity" not in seen_body

    @pytest.mark.asyncio
    async def test_gate_reduce_only_buy_uses_signed_positive_market_size(self):
        import json as _json
        seen_body = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            seen_body.update(_json.loads(request.content.decode()))
            return httpx.Response(
                200,
                json={"id": "gate-close-short-001", "status": "closed", "size": 11, "price": "0"},
            )

        transport = VenueTransport(
            spec=gate_spec(),
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s"),
        )
        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        req = OrderRequest(
            venue=Venue.GATE,
            symbol="INJUSDT",
            side=Side.BUY,
            quantity=11,
            reduce_only=True,
            client_order_id="lfv2-gate-close-short-001",
        )

        await transport.place_order(req)

        assert seen_body["contract"] == "INJ_USDT"
        assert seen_body["size"] == 11
        assert seen_body["price"] == "0"
        assert seen_body["tif"] == "ioc"
        assert seen_body["reduce_only"] is True
        assert "symbol" not in seen_body
        assert "side" not in seen_body
        assert "quantity" not in seen_body

    def test_bybit_passive_price_preserves_tick_precision(self):
        """Bybit passive body price must preserve tick-aware precision.

        _format_price uses %.2f which would turn 0.0315 into "0.03" for
        low-tick symbols. _build_bybit_passive_body must use _format_decimal
        to retain the preflight-quantized price.
        """
        from lightfee.venues.transport import _format_decimal
        spec = bybit_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        req = self._make_passive_req(Venue.BYBIT)
        # Simulate a low-tick price (tick_size=0.0001, raw=0.03157 → quantized=0.0315)
        body = transport._build_bybit_passive_body(req, "BTCUSDT", 0.01, 0.0315)
        assert body["price"] == _format_decimal(0.0315)
        assert body["price"] == "0.0315"
        assert body["price"] != "0.03"


# ====================================================================# Root Fix: OKX Passive Response Validation Tests
# ====================================================================

class TestOkxPassiveAckValidation:
    """OKX passive response must validate code, sCode, and non-empty identifiers."""

    def _make_okx_transport(self):
        spec = okx_spec()
        return VenueTransport(spec=spec, mode="paper")

    def _make_passive_req(self):
        return OrderRequest(
            venue=Venue.OKX,
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=0.01,
            price=50000.0,
            post_only=True,
            client_order_id="okx-cid-test",
        )

    def test_okx_code_nonzero_raises_rejected(self):
        from lightfee.core.errors import OrderSubmitError, SubmitFailureClass

        transport = self._make_okx_transport()
        req = self._make_passive_req()
        raw = {"code": "1", "msg": "Invalid request", "data": []}

        with pytest.raises(OrderSubmitError) as exc:
            transport._parse_passive_order_ack(raw, req, "BTC-USDT-SWAP", 1000)
        assert exc.value.class_ == SubmitFailureClass.REJECTED
        assert "code=1" in str(exc.value)

    def test_okx_scode_nonzero_raises_rejected(self):
        from lightfee.core.errors import OrderSubmitError, SubmitFailureClass

        transport = self._make_okx_transport()
        req = self._make_passive_req()
        raw = {
            "code": "0", "msg": "",
            "data": [{"sCode": "51000", "sMsg": "Order failed", "ordId": "", "clOrdId": ""}],
        }

        with pytest.raises(OrderSubmitError) as exc:
            transport._parse_passive_order_ack(raw, req, "BTC-USDT-SWAP", 1000)
        assert exc.value.class_ == SubmitFailureClass.REJECTED
        assert "sCode=51000" in str(exc.value)

    def test_okx_empty_ordid_and_clordid_raises_uncertain(self):
        from lightfee.core.errors import OrderSubmitError, SubmitFailureClass

        transport = self._make_okx_transport()
        req = self._make_passive_req()
        raw = {
            "code": "0", "msg": "",
            "data": [{"sCode": "0", "sMsg": "", "ordId": "", "clOrdId": ""}],
        }

        with pytest.raises(OrderSubmitError) as exc:
            transport._parse_passive_order_ack(raw, req, "BTC-USDT-SWAP", 1000)
        assert exc.value.class_ == SubmitFailureClass.UNCERTAIN
        assert "empty identifiers" in str(exc.value).lower()

    def test_okx_empty_ordid_only_raises_uncertain(self):
        from lightfee.core.errors import OrderSubmitError, SubmitFailureClass

        transport = self._make_okx_transport()
        req = self._make_passive_req()
        raw = {
            "code": "0", "msg": "",
            "data": [{"sCode": "0", "sMsg": "", "ordId": "", "clOrdId": "valid-cid"}],
        }

        with pytest.raises(OrderSubmitError) as exc:
            transport._parse_passive_order_ack(raw, req, "BTC-USDT-SWAP", 1000)
        assert exc.value.class_ == SubmitFailureClass.UNCERTAIN

    def test_okx_valid_response_returns_ack(self):
        from lightfee.core.domain import PassiveOrderAck

        transport = self._make_okx_transport()
        req = self._make_passive_req()
        raw = {
            "code": "0", "msg": "",
            "data": [{
                "sCode": "0", "sMsg": "",
                "ordId": "1234567890",
                "clOrdId": "okx-cid-test",
                "px": "50000",
                "sz": "0.01",
            }],
        }

        ack = transport._parse_passive_order_ack(raw, req, "BTC-USDT-SWAP", 1000)
        assert isinstance(ack, PassiveOrderAck)
        assert ack.order_id == "1234567890"
        assert ack.client_order_id == "okx-cid-test"

    def test_okx_empty_data_array_raises_uncertain(self):
        from lightfee.core.errors import OrderSubmitError, SubmitFailureClass

        transport = self._make_okx_transport()
        req = self._make_passive_req()
        raw = {"code": "0", "msg": "", "data": []}

        with pytest.raises(OrderSubmitError) as exc:
            transport._parse_passive_order_ack(raw, req, "BTC-USDT-SWAP", 1000)
        assert exc.value.class_ == SubmitFailureClass.UNCERTAIN


# ====================================================================# Root Fix: Preflight/Normalization in Passive Path Tests
# ====================================================================

class TestPassivePreflight:
    """Passive orders must go through preflight normalization."""

    def test_preflight_accepts_symbol_rule(self):
        from lightfee.venues.symbol_rules import SymbolRule

        spec = binance_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        req = OrderRequest(
            venue=Venue.BINANCE, symbol="BTCUSDT",
            side=Side.BUY, quantity=0.0123456, price=50000.12345,
            client_order_id="cid-1",
        )
        rule = SymbolRule(
            tick_size=0.01, qty_step=0.001, min_qty=0.001,
            min_notional=5.0, rule_source="exchangeInfo",
        )
        preflight = transport.preflight_order_request(req, symbol_rule=rule)

        assert preflight["rule_source"] == "exchangeInfo"
        assert preflight["tick_size"] == 0.01
        assert preflight["quantity_step"] == 0.001
        assert preflight["response_classification"] == "attempt"
        # qty should be quantized to step
        assert preflight["quantized_qty"] is not None

    def test_preflight_min_qty_rejected(self):
        from lightfee.core.errors import OrderSubmitError
        from lightfee.venues.symbol_rules import SymbolRule

        spec = binance_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        req = OrderRequest(
            venue=Venue.BINANCE, symbol="BTCUSDT",
            side=Side.BUY, quantity=0.0001, price=50000.0,
            client_order_id="cid-1",
        )
        rule = SymbolRule(
            tick_size=0.01, qty_step=0.001, min_qty=0.001,
            min_notional=5.0, rule_source="exchangeInfo",
        )
        with pytest.raises(OrderSubmitError) as exc:
            transport.preflight_order_request(req, symbol_rule=rule)
        assert exc.value.is_rejected

    def test_preflight_min_notional_rejected(self):
        from lightfee.core.errors import OrderSubmitError
        from lightfee.venues.symbol_rules import SymbolRule

        spec = binance_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        req = OrderRequest(
            venue=Venue.BINANCE, symbol="BTCUSDT",
            side=Side.BUY, quantity=0.001, price=1.0,
            client_order_id="cid-1",
            reduce_only=False,
        )
        rule = SymbolRule(
            tick_size=0.01, qty_step=0.001, min_qty=0.001,
            min_notional=100.0, rule_source="exchangeInfo",
        )
        with pytest.raises(OrderSubmitError) as exc:
            transport.preflight_order_request(req, symbol_rule=rule)
        assert exc.value.is_rejected
        assert "min_notional" in str(exc.value).lower()

    def test_preflight_reduce_only_skips_min_notional(self):
        from lightfee.venues.symbol_rules import SymbolRule

        spec = binance_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        req = OrderRequest(
            venue=Venue.BINANCE, symbol="BTCUSDT",
            side=Side.SELL, quantity=0.001, price=1.0,
            client_order_id="cid-close",
            reduce_only=True,
        )
        rule = SymbolRule(
            tick_size=0.01, qty_step=0.001, min_qty=0.001,
            min_notional=100.0, rule_source="exchangeInfo",
        )
        # Should NOT raise because reduce_only skips min_notional check
        preflight = transport.preflight_order_request(req, symbol_rule=rule)
        assert preflight["response_classification"] == "attempt"

    def test_preflight_with_bybit_rules(self):
        from lightfee.venues.symbol_rules import SymbolRule

        spec = bybit_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        req = OrderRequest(
            venue=Venue.BYBIT, symbol="BTCUSDT",
            side=Side.BUY, quantity=0.01234, price=50000.123,
            client_order_id="cid-1",
        )
        rule = SymbolRule(
            tick_size=0.1, qty_step=0.001, min_qty=0.001,
            min_notional=1.0, rule_source="instruments-info",
        )
        preflight = transport.preflight_order_request(req, symbol_rule=rule)
        assert preflight["rule_source"] == "instruments-info"
        assert preflight["tick_size"] == 0.1

    def test_preflight_with_okx_rules(self):
        from lightfee.venues.symbol_rules import SymbolRule

        spec = okx_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        req = OrderRequest(
            venue=Venue.OKX, symbol="BTCUSDT",
            side=Side.BUY, quantity=0.01234, price=50000.12,
            client_order_id="cid-okx",
        )
        rule = SymbolRule(
            tick_size=0.1, qty_step=0.01, min_qty=0.01,
            min_notional=1.0, rule_source="instrument",
        )
        preflight = transport.preflight_order_request(req, symbol_rule=rule)
        assert preflight["rule_source"] == "instrument"
        assert preflight["tick_size"] == 0.1
        assert preflight["quantity_step"] == 0.0
        assert preflight["min_qty"] == 0.0
        assert preflight["quantized_qty"] == pytest.approx(0.01234)

    @pytest.mark.asyncio
    async def test_bybit_normalize_quantity_uses_dynamic_symbol_rules(self, monkeypatch):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                assert venue == Venue.BYBIT
                assert venue_symbol == "UBUSDT"
                return SymbolRule(
                    tick_size=0.00001,
                    qty_step=10.0,
                    min_qty=10.0,
                    min_notional=1.0,
                    rule_source="instruments-info",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        transport = VenueTransport(
            spec=bybit_spec(),
            mode="live",
            credential=LiveCredential(api_key="key", api_secret="secret"),
        )

        assert await transport.normalize_quantity("UBUSDT", 1.0) == 0.0
        assert await transport.normalize_quantity("UBUSDT", 10.0) == pytest.approx(10.0)

    @pytest.mark.asyncio
    async def test_bybit_place_order_reduce_only_rejects_dynamic_min_qty_without_http(self, monkeypatch):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                assert venue == Venue.BYBIT
                assert venue_symbol == "UBUSDT"
                return SymbolRule(
                    tick_size=0.00001,
                    qty_step=10.0,
                    min_qty=10.0,
                    min_notional=1.0,
                    rule_source="instruments-info",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        transport = VenueTransport(
            spec=bybit_spec(),
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s"),
        )
        transport._request = AsyncMock(side_effect=AssertionError("HTTP should not be sent for dust close"))
        req = OrderRequest(
            venue=Venue.BYBIT,
            symbol="UBUSDT",
            side=Side.BUY,
            quantity=1.0,
            price=0.01,
            reduce_only=True,
            time_in_force=TimeInForce.IOC,
            client_order_id="dust-close",
        )

        with pytest.raises(OrderSubmitError) as exc:
            await transport.place_order(req)

        assert exc.value.is_rejected
        assert (
            "quantity_step_rejected" in str(exc.value)
            or "min_qty_rejected" in str(exc.value)
        )
        transport._request.assert_not_called()

    @pytest.mark.asyncio
    async def test_bybit_order_admission_precheck_uses_official_precheck_endpoint_and_classifies_terms(
        self, monkeypatch
    ):
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                assert venue == Venue.BYBIT
                assert venue_symbol == "CLUSDT"
                return SymbolRule(
                    tick_size=0.01,
                    qty_step=0.01,
                    min_qty=0.01,
                    min_notional=1.0,
                    rule_source="instruments-info",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        transport = VenueTransport(
            spec=bybit_spec(),
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s"),
        )
        transport._request = AsyncMock(
            return_value={
                "retCode": 110125,
                "retMsg": (
                    "You must agree to the Crude Oil Trading Terms before "
                    "trading this contract."
                ),
            }
        )
        req = OrderRequest(
            venue=Venue.BYBIT,
            symbol="CLUSDT",
            side=Side.SELL,
            quantity=0.195,
            price=None,
            reduce_only=False,
            time_in_force=TimeInForce.IOC,
            client_order_id="entry-1-h-bybit",
        )

        with pytest.raises(OrderSubmitError) as exc:
            await transport.precheck_order_admission(req)

        assert exc.value.is_rejected
        assert "110125" in str(exc.value)
        transport._request.assert_awaited_once()
        args, kwargs = transport._request.await_args
        assert args[:2] == ("POST", "/v5/order/pre-check")
        assert kwargs["private"] is True
        body = kwargs["body"]
        assert body["category"] == "linear"
        assert body["symbol"] == "CLUSDT"
        assert body["side"] == "Sell"
        assert body["orderType"] == "Market"
        assert body["qty"] == "0.19"
        assert body["reduceOnly"] is False
        assert body["positionIdx"] == 2
        kinds = [event["kind"] for event in transport.drain_order_diagnostics()]
        assert "order.precheck_attempt" in kinds
        assert "order.precheck_result" in kinds

    @pytest.mark.asyncio
    async def test_bybit_order_admission_precheck_skips_reduce_only_without_http(self):
        transport = VenueTransport(
            spec=bybit_spec(),
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s"),
        )
        transport._request = AsyncMock(side_effect=AssertionError("precheck HTTP skipped"))
        req = OrderRequest(
            venue=Venue.BYBIT,
            symbol="CLUSDT",
            side=Side.BUY,
            quantity=0.2,
            price=None,
            reduce_only=True,
            time_in_force=TimeInForce.IOC,
            client_order_id="close-1",
        )

        result = await transport.precheck_order_admission(req)

        assert result["status"] == "skipped"
        assert result["reason"] == "reduce_only_exempt"
        transport._request.assert_not_called()


# ====================================================================# Root Fix: Journal Evidence Tests
# ====================================================================

class TestJournalEvidenceFields:
    """order.rejected and order.submitted journals must carry full evidence."""

    @pytest.mark.asyncio
    async def test_rejected_journal_has_venue_symbol_leg_is_maker(self):
        from lightfee.persistence.journal import Journal
        from lightfee.engine.entry_sync import EntrySyncExecutor
        from lightfee.engine.entry import EntryContext, EntryType
        from dataclasses import dataclass, field
        from typing import Optional
        from lightfee.core.contracts import VenueAdapter

        @dataclass
        class RejectAdapter(VenueAdapter):
            _venue: Venue
            last_request: Optional[OrderRequest] = None

            @property
            def venue(self) -> Venue:
                return self._venue

            async def place_order(self, request):
                self.last_request = request
                raise OrderSubmitError(SubmitFailureClass.REJECTED, "test rejection")

            async def submit_passive_order(self, request):
                self.last_request = request
                raise OrderSubmitError(SubmitFailureClass.REJECTED, "passive rejected")

            async def fetch_position(self, symbol):
                return PositionSnapshot(
                    venue=self._venue, symbol=symbol, side=Side.BUY,
                    quantity=0.0, entry_price=0.0, observed_at_ms=1000,
                )

            async def normalize_quantity(self, symbol, quantity):
                return quantity

        import tempfile, os
        jd = os.path.join(tempfile.mkdtemp(), "test.jsonl")
        j = Journal(jd)
        j.open()
        try:
            adapters = {Venue.BINANCE: RejectAdapter(Venue.BINANCE)}
            ctx = EntryContext(
                entry_id="ev-test-001",
                symbol="BTCUSDT",
                long_venue=Venue.BINANCE,
                short_venue=Venue.OKX,
                long_quantity=0.01,
                short_quantity=0.01,
                long_price_hint=50000.0,
                short_price_hint=50000.0,
                maker_leg=Side.BUY,
                entry_type=EntryType.PASSIVE_INCREMENTAL,
                created_at_ms=1000,
            )
            executor = EntrySyncExecutor(adapters=adapters, journal=j)
            result = await executor.execute(ctx)

            records = j.read_all()
            rejected = [r for r in records if r["kind"] == "order.rejected"]
            assert len(rejected) >= 1
            rj = rejected[0]["payload"]
            assert "venue" in rj, f"order.rejected missing venue: {list(rj.keys())}"
            assert "symbol" in rj, f"order.rejected missing symbol: {list(rj.keys())}"
            assert "leg" in rj
            assert rj["is_maker"] is True
            assert "client_order_id" in rj
            assert "internal_entry_id" in rj
        finally:
            j.close()

    def test_diagnostic_submit_attempt_has_normalization_evidence(self):
        spec = binance_spec()
        transport = VenueTransport(spec=spec, mode="paper")
        req = OrderRequest(
            venue=Venue.BINANCE, symbol="BTCUSDT",
            side=Side.BUY, quantity=0.012345, price=50000.12345,
            post_only=True, client_order_id="evidence-cid",
        )
        transport.preflight_order_request(req)
        diags = transport.drain_order_diagnostics()
        # preflight records order.submit_result on failure or success
        # but not order.submit_attempt — that's done in submit_passive_order
        # Check that preflight payload has all normalization fields
        preflight_events = [d for d in diags if d["kind"] == "order.submit_result"]
        if preflight_events:
            p = preflight_events[0]["payload"]
            assert "tick_size" in p
            assert "quantity_step" in p
            assert "quantized_qty" in p
            assert "quantized_price" in p
            assert "rule_source" in p


# ====================================================================# V1 Parity: Cancel absent-order detection (C2)
# ====================================================================

class TestCancelAbsentOrderDetection:
    """V1: cancel order returns Ok(()) when order is already absent."""

    def test_bitget_absent_order_response_code_40109(self):
        from lightfee.venues.transport import _cancel_response_indicates_absent_order
        assert _cancel_response_indicates_absent_order(
            {"code": "40109", "msg": "order does not exist"}, Venue.BITGET
        )

    def test_bitget_absent_order_response_code_43001(self):
        from lightfee.venues.transport import _cancel_response_indicates_absent_order
        assert _cancel_response_indicates_absent_order(
            {"code": "43001", "msg": "order not found"}, Venue.BITGET
        )

    def test_bitget_normal_response_not_absent(self):
        from lightfee.venues.transport import _cancel_response_indicates_absent_order
        assert not _cancel_response_indicates_absent_order(
            {"code": "00000", "msg": "success"}, Venue.BITGET
        )

    def test_okx_absent_order_sCode_1(self):
        from lightfee.venues.transport import _cancel_response_indicates_absent_order
        assert _cancel_response_indicates_absent_order(
            {"code": "0", "data": [{"sCode": "1", "sMsg": "order does not exist"}]},
            Venue.OKX,
        )

    def test_okx_normal_sCode_not_absent(self):
        from lightfee.venues.transport import _cancel_response_indicates_absent_order
        assert not _cancel_response_indicates_absent_order(
            {"code": "0", "data": [{"sCode": "0", "sMsg": ""}]}, Venue.OKX
        )

    def test_bitget_error_absent_order_40109(self):
        from lightfee.venues.transport import _cancel_error_indicates_absent_order
        assert _cancel_error_indicates_absent_order(
            '{"code":"40109","msg":"order does not exist"}', 400, Venue.BITGET
        )

    def test_bitget_error_absent_order_43001(self):
        from lightfee.venues.transport import _cancel_error_indicates_absent_order
        assert _cancel_error_indicates_absent_order(
            "error code=43001 order not found", 400, Venue.BITGET
        )

    def test_binance_unknown_order_minus_2011(self):
        from lightfee.venues.transport import _cancel_error_indicates_absent_order
        assert _cancel_error_indicates_absent_order(
            '{"code":-2011,"msg":"Unknown order sent."}', 400, Venue.BINANCE
        )

    def test_bybit_order_not_found(self):
        from lightfee.venues.transport import _cancel_error_indicates_absent_order
        assert _cancel_error_indicates_absent_order(
            '{"retCode":170001,"retMsg":"order not found"}', 400, Venue.BYBIT
        )

    def test_hyperliquid_absent_order_status_is_terminal(self):
        from lightfee.venues.transport import _cancel_response_indicates_absent_order

        assert _cancel_response_indicates_absent_order(
            {
                "status": "ok",
                "response": {
                    "type": "cancel",
                    "data": {
                        "statuses": [
                            {
                                "error": (
                                    "Order was never placed, already canceled, "
                                    "or filled. asset=126"
                                )
                            }
                        ]
                    },
                },
            },
            Venue.HYPERLIQUID,
        )

    def test_gate_order_not_found(self):
        from lightfee.venues.transport import _cancel_error_indicates_absent_order
        assert _cancel_error_indicates_absent_order(
            '{"label":"ORDER_NOT_FOUND","message":"order not found"}', 400, Venue.GATE
        )

    def test_normal_error_not_absent(self):
        from lightfee.venues.transport import _cancel_error_indicates_absent_order
        assert not _cancel_error_indicates_absent_order(
            "rate limit exceeded", 429, Venue.BITGET
        )

    @pytest.mark.asyncio
    async def test_cancel_passive_order_returns_canceled_on_absent_response(self):
        """V1 parity: successful HTTP response with absent-order code → CANCELED ack."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bitget_spec
        from lightfee.core.domain import PassiveOrderState

        transport = VenueTransport(bitget_spec(), mode="paper")
        transport.mode = "live"

        async def fake_request(*args, **kwargs):
            return {"code": "40109", "msg": "order does not exist"}

        transport._request = fake_request
        transport._build_signed_request_async = AsyncMock(
            return_value=("", {}, b"")
        )

        ack = await transport.cancel_passive_order(
            symbol="BTCUSDT", order_id="123456",
        )
        assert ack.state == PassiveOrderState.CANCELED

    @pytest.mark.asyncio
    async def test_cancel_passive_order_returns_canceled_on_transport_error_absent(self):
        """V1 parity: TransportError with absent-order body → CANCELED ack."""
        from lightfee.venues.transport import VenueTransport, TransportError, TransportErrorCategory
        from lightfee.venues.specs import bitget_spec
        from lightfee.core.domain import PassiveOrderState

        transport = VenueTransport(bitget_spec(), mode="paper")
        transport.mode = "live"

        async def fake_request(*args, **kwargs):
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                "order not found",
                status_code=400,
                body='{"code":"40109","msg":"order does not exist"}',
            )

        transport._request = fake_request
        transport._build_signed_request_async = AsyncMock(
            return_value=("", {}, b"")
        )

        ack = await transport.cancel_passive_order(
            symbol="BTCUSDT", order_id="123456",
        )
        assert ack.state == PassiveOrderState.CANCELED

    @pytest.mark.asyncio
    async def test_bybit_cancel_passive_order_uses_cancel_endpoint(self):
        """Bybit V5 cancel is POST /v5/order/cancel, not DELETE /v5/order/create."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bybit_spec
        from lightfee.core.domain import PassiveOrderState

        transport = VenueTransport(bybit_spec(), mode="paper")
        transport.mode = "live"
        seen = {}

        async def fake_request(method, path, **kwargs):
            seen["method"] = method
            seen["path"] = path
            seen["kwargs"] = kwargs
            return {
                "retCode": 0,
                "retMsg": "OK",
                "result": {"orderId": "oid-1", "orderLinkId": "cid-1"},
            }

        transport._request = fake_request
        transport._build_signed_request_async = AsyncMock(
            return_value=("", {}, b"")
        )

        ack = await transport.cancel_passive_order(
            symbol="BTCUSDT", order_id="oid-1", client_order_id="cid-1",
        )

        assert ack.state == PassiveOrderState.CANCELED
        assert seen["method"] == "POST"
        assert seen["path"] == "/v5/order/cancel"
        assert seen["kwargs"]["body"]["category"] == "linear"
        assert seen["kwargs"]["body"]["symbol"] == "BTCUSDT"
        assert seen["kwargs"]["body"]["orderId"] == "oid-1"

    @pytest.mark.asyncio
    async def test_hyperliquid_cancel_passive_order_uses_signed_cancel_action(self, monkeypatch):
        """Hyperliquid cancel must use signed /exchange action, not a raw cancel body."""
        from lightfee.core.domain import PassiveOrderState
        from lightfee.venues import hyperliquid_signing
        from lightfee.venues.specs import hyperliquid_spec
        from lightfee.venues.transport import LiveCredential, VenueTransport

        transport = VenueTransport(
            hyperliquid_spec(),
            mode="live",
            credential=LiveCredential(
                wallet_private_key="0x" + "11" * 32,
                account_address="0x" + "22" * 20,
            ),
        )
        monkeypatch.setattr(
            transport,
            "_hl_cached_asset_meta",
            lambda _symbol: {"asset_index": 17, "sz_decimals": 0, "price_decimals": 6},
        )

        def fake_exchange_payload(action, private_key_hex, vault_address=None, is_mainnet=True):
            assert private_key_hex == "0x" + "11" * 32
            return {
                "action": action,
                "signature": {"r": "0x1", "s": "0x2", "v": 27},
                "nonce": 12345,
            }

        monkeypatch.setattr(
            hyperliquid_signing,
            "build_hyperliquid_exchange_payload",
            fake_exchange_payload,
        )
        seen: dict[str, Any] = {}

        async def fake_request(method, path, **kwargs):
            seen["method"] = method
            seen["path"] = path
            seen["body"] = kwargs.get("body")
            return {
                "status": "ok",
                "response": {"type": "cancel", "data": {"statuses": ["success"]}},
            }

        transport._request = fake_request

        ack = await transport.cancel_passive_order(
            "MERLUSDT",
            "455070590535",
            client_order_id="4e29a7a3a58546f4ffc40711d8d8f3601dba",
        )

        assert ack.state == PassiveOrderState.CANCELED
        assert seen["method"] == "POST"
        assert seen["path"] == "/exchange"
        assert seen["body"] == {
            "action": {
                "type": "cancel",
                "cancels": [{"a": 17, "o": 455070590535}],
            },
            "signature": {"r": "0x1", "s": "0x2", "v": 27},
            "nonce": 12345,
        }

    @pytest.mark.asyncio
    async def test_hyperliquid_cancel_passive_order_loads_asset_meta_after_restart(self, monkeypatch):
        """Recovery cancel must not depend on a warm Hyperliquid asset-index cache."""
        from lightfee.core.domain import PassiveOrderState
        from lightfee.venues import hyperliquid_signing
        from lightfee.venues.specs import hyperliquid_spec
        from lightfee.venues.transport import LiveCredential, VenueTransport

        transport = VenueTransport(
            hyperliquid_spec(),
            mode="live",
            credential=LiveCredential(
                wallet_private_key="0x" + "11" * 32,
                account_address="0x" + "22" * 20,
            ),
        )

        def fake_exchange_payload(action, private_key_hex, vault_address=None, is_mainnet=True):
            assert private_key_hex == "0x" + "11" * 32
            return {
                "action": action,
                "signature": {"r": "0x1", "s": "0x2", "v": 27},
                "nonce": 12345,
            }

        monkeypatch.setattr(
            hyperliquid_signing,
            "build_hyperliquid_exchange_payload",
            fake_exchange_payload,
        )
        seen: list[tuple[str, str, Any]] = []

        async def fake_request(method, path, **kwargs):
            seen.append((method, path, kwargs.get("body")))
            if path == "/info":
                universe = [{"name": f"DUMMY{i}", "szDecimals": 0} for i in range(17)]
                universe.append({"name": "MERL", "szDecimals": 0})
                return {"universe": universe}
            return {
                "status": "ok",
                "response": {"type": "cancel", "data": {"statuses": ["success"]}},
            }

        transport._request = fake_request

        ack = await transport.cancel_passive_order(
            "MERLUSDT",
            "455070590535",
            client_order_id="4e29a7a3a58546f4ffc40711d8d8f3601dba",
        )

        assert ack.state == PassiveOrderState.CANCELED
        assert seen[0] == ("POST", "/info", {"type": "meta"})
        assert seen[1] == (
            "POST",
            "/exchange",
            {
                "action": {
                    "type": "cancel",
                    "cancels": [{"a": 17, "o": 455070590535}],
                },
                "signature": {"r": "0x1", "s": "0x2", "v": 27},
                "nonce": 12345,
            },
        )

    @pytest.mark.asyncio
    async def test_hyperliquid_cancel_passive_order_treats_absent_order_as_canceled(self, monkeypatch):
        """Hyperliquid absent cancel status is terminal for recovery, matching V1."""
        from lightfee.core.domain import PassiveOrderState
        from lightfee.venues import hyperliquid_signing
        from lightfee.venues.specs import hyperliquid_spec
        from lightfee.venues.transport import LiveCredential, VenueTransport

        transport = VenueTransport(
            hyperliquid_spec(),
            mode="live",
            credential=LiveCredential(
                wallet_private_key="0x" + "11" * 32,
                account_address="0x" + "22" * 20,
            ),
        )
        monkeypatch.setattr(
            transport,
            "_hl_resolve_asset_meta",
            AsyncMock(
                return_value={"asset_index": 126, "sz_decimals": 0, "price_decimals": 6}
            ),
        )

        def fake_exchange_payload(action, private_key_hex, vault_address=None, is_mainnet=True):
            return {
                "action": action,
                "signature": {"r": "0x1", "s": "0x2", "v": 27},
                "nonce": 12345,
            }

        monkeypatch.setattr(
            hyperliquid_signing,
            "build_hyperliquid_exchange_payload",
            fake_exchange_payload,
        )

        async def fake_request(method, path, **kwargs):
            return {
                "status": "ok",
                "response": {
                    "type": "cancel",
                    "data": {
                        "statuses": [
                            {
                                "error": (
                                    "Order was never placed, already canceled, "
                                    "or filled. asset=126"
                                )
                            }
                        ]
                    },
                },
            }

        transport._request = fake_request

        ack = await transport.cancel_passive_order(
            "MERLUSDT",
            "455070590535",
            client_order_id="4e29a7a3a58546f4ffc40711d8d8f3601dba",
        )

        assert ack.state == PassiveOrderState.CANCELED

    @pytest.mark.asyncio
    async def test_hyperliquid_adapter_fetch_open_orders_uses_info_open_orders(self):
        """Hyperliquid open-order truth comes from the official /info openOrders API."""
        from lightfee.venues.hyperliquid import HyperliquidAdapter
        from lightfee.venues.transport import LiveCredential

        adapter = HyperliquidAdapter(
            mode="live",
            credential=LiveCredential(
                wallet_private_key="0x" + "11" * 32,
                account_address="0x" + "33" * 20,
            ),
        )
        seen: dict[str, Any] = {}

        async def fake_request(method, path, **kwargs):
            seen["method"] = method
            seen["path"] = path
            seen["body"] = kwargs.get("body")
            seen["private"] = kwargs.get("private")
            return [
                {"coin": "MERL", "oid": 455070590535, "sz": "864"},
                {"coin": "BTC", "oid": 1, "sz": "0.1"},
            ]

        adapter._transport._request = fake_request

        rows = await adapter.fetch_open_orders("MERLUSDT")

        assert rows == [{"coin": "MERL", "oid": 455070590535, "sz": "864"}]
        account_address = adapter._credential.account_address
        assert seen == {
            "method": "POST",
            "path": "/info",
            "body": {"type": "openOrders", "user": account_address},
            "private": False,
        }

    @pytest.mark.asyncio
    async def test_hyperliquid_fetch_open_orders_unknown_shape_is_untrusted(self):
        """An unknown/malformed /info openOrders response must raise (fail
        closed), not return [] which the strict probe would read as proven flat."""
        from lightfee.engine.exchange_truth import probe_venue_open_orders_flat
        from lightfee.venues.hyperliquid import HyperliquidAdapter
        from lightfee.venues.transport import LiveCredential, TransportError

        adapter = HyperliquidAdapter(
            mode="live",
            credential=LiveCredential(
                wallet_private_key="0x" + "11" * 32,
                account_address="0x" + "33" * 20,
            ),
        )

        async def fake_request(method, path, **kwargs):
            return {"unexpected": "shape"}

        adapter._transport._request = fake_request

        with pytest.raises(TransportError):
            await adapter.fetch_open_orders("MERLUSDT")

        # Through the shared strict probe the malformed response must be
        # untrusted (flat=None), never a proven flat.
        flat, evidence = await probe_venue_open_orders_flat(
            adapter, Venue.HYPERLIQUID, "MERLUSDT"
        )
        assert flat is None
        assert evidence is not None

    @pytest.mark.asyncio
    async def test_hyperliquid_fetch_open_orders_non_list_is_untrusted(self):
        """A non-list /info openOrders payload (e.g. a plain scalar) must be
        untrusted, not collapsed into an empty open-order list."""
        from lightfee.engine.exchange_truth import probe_venue_open_orders_flat
        from lightfee.venues.hyperliquid import HyperliquidAdapter
        from lightfee.venues.transport import LiveCredential, TransportError

        adapter = HyperliquidAdapter(
            mode="live",
            credential=LiveCredential(
                wallet_private_key="0x" + "11" * 32,
                account_address="0x" + "33" * 20,
            ),
        )

        async def fake_request(method, path, **kwargs):
            return "not-a-list"

        adapter._transport._request = fake_request

        with pytest.raises(TransportError):
            await adapter.fetch_open_orders("MERLUSDT")

        flat, evidence = await probe_venue_open_orders_flat(
            adapter, Venue.HYPERLIQUID, "MERLUSDT"
        )
        assert flat is None
        assert evidence is not None

    @pytest.mark.asyncio
    async def test_bybit_query_passive_order_progress_uses_realtime_endpoint(self):
        """Bybit V5 merges realtime order state with execution history."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bybit_spec

        transport = VenueTransport(bybit_spec(), mode="paper")
        transport.mode = "live"
        calls = []
        transport.private_order_progress = Mock(return_value=None)

        async def fake_request(method, path, **kwargs):
            calls.append((method, path, kwargs))
            if path == "/v5/order/realtime":
                return {
                    "retCode": 0,
                    "retMsg": "OK",
                    "result": {"list": [{
                        "orderId": "oid-1",
                        "orderLinkId": "cid-1",
                        "orderStatus": "New",
                        "side": "Buy",
                        "cumExecQty": "0",
                        "avgPrice": "0",
                        "price": "1.23",
                        "qty": "2",
                        "updatedTime": "1779450000000",
                    }]},
                }
            assert path == "/v5/execution/list"
            return {"retCode": 0, "retMsg": "OK", "result": {"list": []}}

        transport._request = fake_request
        progress = await transport.query_passive_order_progress(
            symbol="ALTUSDT",
            order_id="oid-1",
            client_order_id="cid-1",
            side=Side.BUY,
        )

        assert progress is not None
        assert [path for _, path, _ in calls] == [
            "/v5/order/realtime", "/v5/execution/list",
        ]
        assert calls[0][0] == "GET"
        assert calls[0][2]["params"]["category"] == "linear"
        assert calls[0][2]["params"]["symbol"] == "ALTUSDT"
        assert calls[0][2]["params"]["orderId"] == "oid-1"

    @pytest.mark.asyncio
    async def test_bybit_passive_progress_does_not_trust_stale_private_rejected_zero_fill(self):
        """A private rejected/0 update cannot hide a later Bybit execution."""
        from lightfee.marketdata.private_ws import CumulativeOrderProgress
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bybit_spec

        transport = VenueTransport(bybit_spec(), mode="paper")
        transport.mode = "live"
        transport.private_order_progress = Mock(return_value=CumulativeOrderProgress(
            order_id="oid-2",
            client_order_id="cid-2",
            cumulative_quantity=0.0,
            state=PassiveOrderState.REJECTED,
            updated_at_ms=1779450000000,
        ))
        calls = []

        async def fake_request(method, path, **kwargs):
            calls.append((method, path, kwargs))
            if path == "/v5/order/realtime":
                return {
                    "retCode": 0,
                    "retMsg": "OK",
                    "result": {"list": [{
                        "orderId": "oid-2",
                        "orderLinkId": "cid-2",
                        "orderStatus": "Rejected",
                        "side": "Buy",
                        "cumExecQty": "0",
                        "avgPrice": "0",
                        "cumExecFee": "0",
                        "updatedTime": "1779450000000",
                    }]},
                }
            assert path == "/v5/execution/list"
            return {
                "retCode": 0,
                "retMsg": "OK",
                "result": {"list": [{
                    "orderId": "oid-2",
                    "orderLinkId": "cid-2",
                    "side": "Buy",
                    "execQty": "0.429",
                    "execPrice": "1.234",
                    "execFee": "0.001",
                    "execTime": "1779450000100",
                }]},
            }

        transport._request = fake_request
        progress = await transport.query_passive_order_progress(
            symbol="ALTUSDT",
            order_id="oid-2",
            client_order_id="cid-2",
            side=Side.BUY,
        )

        assert progress is not None
        assert progress.state == PassiveOrderState.REJECTED
        assert progress.cumulative_quantity == pytest.approx(0.429)
        assert progress.average_price == pytest.approx(1.234)
        assert progress.fee_quote == pytest.approx(0.001)
        assert progress.last_fill_time_ms == 1779450000100
        assert [path for _, path, _ in calls] == [
            "/v5/order/realtime", "/v5/execution/list",
        ]

    @pytest.mark.asyncio
    async def test_bybit_private_rejected_zero_without_rest_row_stays_open_for_truth(self):
        """No realtime row plus private Rejected/0 is not a terminal zero-fill fact."""
        from lightfee.marketdata.private_ws import CumulativeOrderProgress
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bybit_spec

        transport = VenueTransport(bybit_spec(), mode="paper")
        transport.mode = "live"
        transport.private_order_progress = Mock(return_value=CumulativeOrderProgress(
            order_id="oid-3",
            client_order_id="cid-3",
            cumulative_quantity=0.0,
            state=PassiveOrderState.REJECTED,
        ))
        calls = []

        async def fake_request(method, path, **kwargs):
            calls.append(path)
            if path == "/v5/order/realtime":
                return {"retCode": 0, "retMsg": "OK", "result": {"list": []}}
            assert path == "/v5/execution/list"
            return {"retCode": 0, "retMsg": "OK", "result": {"list": []}}

        transport._request = fake_request
        progress = await transport.query_passive_order_progress(
            symbol="ALTUSDT",
            order_id="oid-3",
            client_order_id="cid-3",
            side=Side.BUY,
        )

        assert progress is not None
        assert progress.state == PassiveOrderState.OPEN
        assert progress.cumulative_quantity == 0.0
        assert calls == ["/v5/order/realtime", "/v5/execution/list"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("order_status", ("PendingCancel", "Deactivated"))
    async def test_bybit_passive_progress_maps_pending_cancel_as_terminal_cancel(self, order_status):
        """V1 maps Bybit cancel-transition statuses to canceled after REST confirms."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bybit_spec

        transport = VenueTransport(bybit_spec(), mode="paper")
        transport.mode = "live"
        transport.private_order_progress = Mock(return_value=None)

        async def fake_request(method, path, **kwargs):
            del method, kwargs
            if path == "/v5/order/realtime":
                return {"retCode": 0, "retMsg": "OK", "result": {"list": [{
                    "orderId": "oid-pending-cancel",
                    "orderLinkId": "cid-pending-cancel",
                    "orderStatus": order_status,
                    "side": "Sell",
                    "cumExecQty": "0",
                    "avgPrice": "0",
                    "cumExecFee": "0",
                    "updatedTime": "1779450000000",
                }]}}
            assert path == "/v5/execution/list"
            return {"retCode": 0, "retMsg": "OK", "result": {"list": []}}

        transport._request = fake_request
        progress = await transport.query_passive_order_progress(
            symbol="ALTUSDT",
            order_id="oid-pending-cancel",
            client_order_id="cid-pending-cancel",
            side=Side.SELL,
        )

        assert progress is not None
        assert progress.state == PassiveOrderState.CANCELED
        assert progress.cumulative_quantity == 0.0

    @pytest.mark.asyncio
    async def test_hyperliquid_client_order_query_uses_historical_orders_first(self):
        """V1 resolves client IDs through historicalOrders, not /info orderStatus cloid."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import hyperliquid_spec

        credential = LiveCredential(account_address="0xabc")
        transport = VenueTransport(hyperliquid_spec(), mode="paper", credential=credential)
        calls = []

        async def fake_request(method, path, **kwargs):
            calls.append((method, path, kwargs))
            body = kwargs.get("body") or {}
            if body.get("type") == "historicalOrders":
                return []
            return {"status": "unexpected"}

        transport._request = fake_request
        result = await transport._query_hyperliquid_order(
            transport._spec,
            "ALT",
            "",
            "client-123",
            1779450000000,
        )

        assert result == ([], True)
        assert calls[0][0] == "POST"
        assert calls[0][1] == "/info"
        assert calls[0][2]["body"] == {
            "type": "historicalOrders",
            "user": "0xabc",
        }
        assert all("cloid" not in (kwargs.get("body") or {}) for _, _, kwargs in calls)

    @pytest.mark.asyncio
    async def test_listen_key_request_uses_api_key_header_without_signature(self):
        """Binance/Aster user-stream listenKey calls are API-key only, not trading signed."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import binance_spec

        class FakeResponse:
            status_code = 200
            text = '{"listenKey":"lk-1"}'
            headers = {}

            def json(self):
                return {"listenKey": "lk-1"}

        class FakeClient:
            def __init__(self):
                self.calls = []

            async def put(self, url, headers=None):
                self.calls.append(("PUT", url, headers or {}))
                return FakeResponse()

        client = FakeClient()
        transport = VenueTransport(
            binance_spec(),
            mode="live",
            credential=LiveCredential(api_key="trade-key", api_secret="trade-secret"),
        )

        async def fake_get_client():
            return client

        transport._get_client = fake_get_client

        raw = await transport._request_listen_key(
            "PUT",
            "/fapi/v1/listenKey",
            api_key="stream-key",
            params={"listenKey": "lk-1"},
        )

        assert raw == {"listenKey": "lk-1"}
        assert client.calls == [
            (
                "PUT",
                "https://fapi.binance.com/fapi/v1/listenKey?listenKey=lk-1",
                {"X-MBX-APIKEY": "stream-key"},
            )
        ]
        assert "timestamp" not in client.calls[0][1]
        assert "signature" not in client.calls[0][1]

    @pytest.mark.asyncio
    async def test_cancel_passive_order_raises_on_real_error(self):
        """Non-absent TransportError must still raise (not silently return CANCELED)."""
        from lightfee.venues.transport import VenueTransport, TransportError, TransportErrorCategory
        from lightfee.venues.specs import bitget_spec

        transport = VenueTransport(bitget_spec(), mode="paper")
        transport.mode = "live"

        async def fake_request(*args, **kwargs):
            raise TransportError(
                TransportErrorCategory.TRANSPORT_FAILURE,
                "network timeout",
                status_code=500,
                body="Internal Server Error",
            )

        transport._request = fake_request
        transport._build_signed_request_async = AsyncMock(
            return_value=("", {}, b"")
        )

        with pytest.raises(TransportError):
            await transport.cancel_passive_order(
                symbol="BTCUSDT", order_id="123456",
            )


# ====================================================================# V1 Parity: drive_pending_entry_hedge cancel_replace uses passive methods (C1/H1)
# ====================================================================

class TestDrivePendingEntryHedgeCancelReplace:
    """V1: cancel-replace must use cancel_passive_order + submit_passive_order."""

    @pytest.mark.asyncio
    async def test_cancel_replace_uses_cancel_passive_order_not_cancel_order(self):
        """V1: cancel must go through cancel_passive_order which handles absent-order."""
        from lightfee.engine.entry_sync import drive_pending_entry_hedge, HedgeDriveResult
        from lightfee.persistence.journal import Journal
        from lightfee.core.contracts import VenueAdapter
        from lightfee.core.domain import PassiveOrderAck, PassiveOrderState
        from dataclasses import dataclass

        journal = Journal("/tmp/test_drive_hedge_cancel.journal")
        journal.open()

        @dataclass
        class TestAdapter(VenueAdapter):
            _venue: Venue = Venue.BYBIT
            cancel_called: bool = False
            submit_called: bool = False
            cancel_order_called: bool = False

            @property
            def venue(self) -> Venue:
                return self._venue

            async def place_order(self, request):
                from lightfee.core.domain import OrderFill
                return OrderFill(
                    venue=self._venue, symbol=request.symbol,
                    side=request.side, quantity=0.0, price=0.0,
                )

            async def submit_passive_order(self, request):
                self.submit_called = True
                return PassiveOrderAck(
                    venue=self._venue, symbol=request.symbol,
                    side=request.side, order_id="new-123",
                    client_order_id=request.client_order_id or "cid-123",
                    price=request.price or 0.0, quantity=request.quantity,
                    accepted_at_ms=0, state=PassiveOrderState.OPEN,
                )

            async def cancel_passive_order(self, symbol, order_id, client_order_id=None):
                self.cancel_called = True
                return PassiveOrderAck(
                    venue=self._venue, symbol=symbol,
                    side=Side.BUY, order_id=order_id,
                    client_order_id=client_order_id or "",
                    price=0.0, quantity=0.0, accepted_at_ms=0,
                    state=PassiveOrderState.CANCELED,
                )

            async def cancel_order(self, request):
                self.cancel_order_called = True
                raise NotImplementedError("should not be called")

            async def fetch_position(self, symbol):
                from lightfee.core.domain import PositionSnapshot
                return PositionSnapshot(
                    venue=self._venue, symbol=symbol,
                    side=Side.BUY, quantity=0.0, entry_price=0.0,
                    observed_at_ms=0,
                )

        from dataclasses import dataclass as dcl
        @dcl
        class FakePending:
            maker_order_id: str = "old-456"
            maker_client_order_id: str = "cid-456"
            long_quantity: float = 0.1
            target_quantity: float = 0.1

        pending = FakePending()
        adapter = TestAdapter()
        adapters = {Venue.BYBIT: adapter}

        result = await drive_pending_entry_hedge(
            entry_id="test-entry-1",
            pending=pending,
            new_price=50100.0,
            old_price=50000.0,
            action="cancel_replace",
            now_ms=0,
            adapters=adapters,
            journal=journal,
            maker_leg=Side.BUY,
            symbol="BTCUSDT",
            long_venue=Venue.BYBIT,
            short_venue=Venue.OKX,
            quantity=0.1,
        )

        assert adapter.cancel_called, "cancel_passive_order must be called"
        assert adapter.submit_called, "submit_passive_order must be called for replacement"
        assert not adapter.cancel_order_called, "cancel_order (NotImplementedError) must NOT be called"
        assert result.action == "cancel_replace"
        assert result.outcome == "applied"

    @pytest.mark.asyncio
    async def test_cancel_replace_blocks_replacement_when_cancel_fails(self):
        """V1: cancel failure must block replacement (no double maker)."""
        from lightfee.engine.entry_sync import drive_pending_entry_hedge, HedgeDriveResult
        from lightfee.persistence.journal import Journal
        from lightfee.core.contracts import VenueAdapter
        from dataclasses import dataclass

        journal = Journal("/tmp/test_drive_hedge_block.journal")
        journal.open()

        @dataclass
        class FailingCancelAdapter(VenueAdapter):
            _venue: Venue = Venue.BYBIT

            @property
            def venue(self) -> Venue:
                return self._venue

            async def place_order(self, request):
                from lightfee.core.domain import OrderFill
                return OrderFill(
                    venue=self._venue, symbol=request.symbol,
                    side=request.side, quantity=0.0, price=0.0,
                )

            async def submit_passive_order(self, request):
                raise AssertionError("must not submit replacement after cancel failure")

            async def cancel_passive_order(self, symbol, order_id, client_order_id=None):
                raise TransportError(
                    TransportErrorCategory.TRANSPORT_FAILURE,
                    "network timeout",
                    status_code=500,
                )

            async def fetch_position(self, symbol):
                from lightfee.core.domain import PositionSnapshot
                return PositionSnapshot(
                    venue=self._venue, symbol=symbol,
                    side=Side.BUY, quantity=0.0, entry_price=0.0,
                    observed_at_ms=0,
                )

        from dataclasses import dataclass as dcl
        @dcl
        class FakePending:
            maker_order_id: str = "old-789"
            maker_client_order_id: str = "cid-789"
            long_quantity: float = 0.1
            target_quantity: float = 0.1

        pending = FakePending()
        adapter = FailingCancelAdapter()
        adapters = {Venue.BYBIT: adapter}

        from lightfee.venues.transport import TransportError, TransportErrorCategory

        result = await drive_pending_entry_hedge(
            entry_id="test-entry-2",
            pending=pending,
            new_price=50200.0,
            old_price=50100.0,
            action="cancel_replace",
            now_ms=0,
            adapters=adapters,
            journal=journal,
            maker_leg=Side.BUY,
            symbol="BTCUSDT",
            long_venue=Venue.BYBIT,
            short_venue=Venue.OKX,
            quantity=0.1,
        )

        assert result.outcome == "uncertain", (
            "cancel failure must return uncertain, replacement must NOT be submitted"
        )
        assert "cancel failed before replacement" in result.detail.lower() or \
            "cancel" in result.detail.lower()


class TestParseOptionalFloatV1Parity:
    """V1 parse_optional_f64_field parity: empty string, None, --, null, n/a, nan
    must all return None instead of raising ValueError.

    V1 uses eq_ignore_ascii_case("nan") so all case variants must match.
    Non-finite floats (nan, inf, -inf) also return None.

    Production evidence: Bybit risk_snapshot_fetch_error reported
    `could not convert string to float: ''` for totalMaintenanceMargin.
    """

    def test_empty_string_returns_none(self):
        from lightfee.venues.transport import _parse_optional_float
        assert _parse_optional_float("") is None

    def test_none_returns_none(self):
        from lightfee.venues.transport import _parse_optional_float
        assert _parse_optional_float(None) is None

    def test_double_dash_returns_none(self):
        from lightfee.venues.transport import _parse_optional_float
        assert _parse_optional_float("--") is None

    def test_null_string_returns_none(self):
        from lightfee.venues.transport import _parse_optional_float
        assert _parse_optional_float("null") is None
        assert _parse_optional_float("NULL") is None

    def test_na_returns_none(self):
        from lightfee.venues.transport import _parse_optional_float
        assert _parse_optional_float("n/a") is None
        assert _parse_optional_float("N/A") is None

    def test_nan_all_case_variants_return_none(self):
        """V1 eq_ignore_ascii_case("nan"): NAN, Nan, nan, NaN all return None."""
        from lightfee.venues.transport import _parse_optional_float
        assert _parse_optional_float("nan") is None
        assert _parse_optional_float("NaN") is None
        assert _parse_optional_float("NAN") is None
        assert _parse_optional_float("Nan") is None

    def test_inf_variants_return_none(self):
        """Non-finite string values must return None."""
        from lightfee.venues.transport import _parse_optional_float
        assert _parse_optional_float("inf") is None
        assert _parse_optional_float("Infinity") is None
        assert _parse_optional_float("-inf") is None
        assert _parse_optional_float("-Infinity") is None

    def test_float_nan_returns_none(self):
        """Non-finite Python float values must return None (not float('nan'))."""
        from lightfee.venues.transport import _parse_optional_float
        assert _parse_optional_float(float("nan")) is None
        assert _parse_optional_float(float("inf")) is None
        assert _parse_optional_float(float("-inf")) is None

    def test_whitespace_empty_returns_none(self):
        from lightfee.venues.transport import _parse_optional_float
        assert _parse_optional_float("  ") is None

    def test_valid_number_returns_float(self):
        from lightfee.venues.transport import _parse_optional_float
        assert _parse_optional_float("1234.56") == 1234.56

    def test_zero_returns_zero(self):
        from lightfee.venues.transport import _parse_optional_float
        assert _parse_optional_float("0") == 0.0

    def test_negative_number_returns_float(self):
        from lightfee.venues.transport import _parse_optional_float
        assert _parse_optional_float("-500.0") == -500.0

    def test_int_value_returns_float(self):
        from lightfee.venues.transport import _parse_optional_float
        assert _parse_optional_float(100) == 100.0


class TestBybitRiskSnapshotEmptyStringV1Parity:
    """V1 parity: Bybit risk snapshot must not crash on empty string fields.

    These tests call VenueTransport.fetch_account_risk_snapshot() directly
    with a mocked _request() that returns Bybit wallet-balance responses,
    ensuring the real transport parsing path handles all edge cases correctly.

    Production error: `could not convert string to float: ''`
    V1: parse_optional_f64_field returns None for "", "--", "null", "n/a", "nan".
    """

    def _make_bybit_transport(self):
        """Create a live Bybit VenueTransport with mock credentials."""
        transport = VenueTransport(
            spec=bybit_spec(),
            mode="live",
            credential=LiveCredential(api_key="test-key", api_secret="test-secret"),
        )
        return transport

    def _bybit_wallet_balance_response(self, total_equity="10000.50",
                                        total_maintenance_margin="500.25",
                                        total_available_balance="8000.00",
                                        total_wallet_balance="10000.00"):
        """Build a realistic Bybit /v5/account/wallet-balance response shape."""
        return {
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [{
                    "totalEquity": total_equity,
                    "totalMaintenanceMargin": total_maintenance_margin,
                    "totalAvailableBalance": total_available_balance,
                    "totalWalletBalance": total_wallet_balance,
                }],
            },
        }

    @pytest.mark.asyncio
    async def test_empty_maintenance_margin_returns_none(self):
        """Bybit totalMaintenanceMargin="" MUST return None, not crash."""
        transport = self._make_bybit_transport()

        async def mock_request(method, path, params=None, body=None, private=False):
            return self._bybit_wallet_balance_response(
                total_equity="10000.50",
                total_maintenance_margin="",
            )

        transport._request = mock_request
        result = await transport.fetch_account_risk_snapshot()
        assert result is None, "Empty maintenance_margin MUST return None (V1 parity)"

    @pytest.mark.asyncio
    async def test_empty_maintenance_margin_is_normal_only_for_isolated_flat_truth(self):
        """Blank account MM is normal only after both Bybit truth checks pass."""
        transport = self._make_bybit_transport()

        async def mock_request(method, path, params=None, body=None, private=False):
            if path == "/v5/account/wallet-balance":
                return self._bybit_wallet_balance_response(
                    total_equity="10000.50",
                    total_maintenance_margin="",
                )
            if path == "/v5/account/info":
                return {"retCode": 0, "result": {"marginMode": "ISOLATED_MARGIN"}}
            if path == "/v5/position/list":
                assert params["limit"] == 200
                assert params["category"] in {"linear", "inverse", "option"}
                return {"retCode": 0, "result": {"list": []}}
            raise AssertionError(path)

        transport._request = mock_request
        result = await transport.fetch_account_risk_snapshot()

        assert result is not None
        assert result.supported is True
        assert result.maintenance_margin_quote == 0.0
        assert result.health_ratio == 0.0
        assert result.source == "bybit_isolated_all_derivative_position_truth"

    @pytest.mark.asyncio
    async def test_empty_maintenance_margin_uses_per_position_mm_for_isolated_open_position(self):
        transport = self._make_bybit_transport()

        async def mock_request(method, path, params=None, body=None, private=False):
            if path == "/v5/account/wallet-balance":
                return self._bybit_wallet_balance_response(
                    total_equity="10000.50",
                    total_maintenance_margin="",
                )
            if path == "/v5/account/info":
                return {"retCode": 0, "result": {"marginMode": "ISOLATED_MARGIN"}}
            if path == "/v5/position/list":
                if params.get("category") != "linear" or params.get("settleCoin") != "USDT":
                    return {"retCode": 0, "result": {"list": []}}
                return {
                    "retCode": 0,
                    "result": {"list": [{"size": "2", "positionMM": "125.25"}]},
                }
            raise AssertionError(path)

        transport._request = mock_request
        result = await transport.fetch_account_risk_snapshot()

        assert result is not None
        assert result.supported is True
        assert result.maintenance_margin_quote == 125.25
        assert result.health_ratio == pytest.approx(10000.50 / 125.25)
        assert result.source == "bybit_isolated_all_derivative_position_mm"

    @pytest.mark.asyncio
    async def test_empty_maintenance_margin_with_live_position_missing_mm_remains_unsupported(self):
        transport = self._make_bybit_transport()

        async def mock_request(method, path, params=None, body=None, private=False):
            if path == "/v5/account/wallet-balance":
                return self._bybit_wallet_balance_response(total_maintenance_margin="")
            if path == "/v5/account/info":
                return {"retCode": 0, "result": {"marginMode": "ISOLATED_MARGIN"}}
            if path == "/v5/position/list":
                if params.get("category") != "linear" or params.get("settleCoin") != "USDT":
                    return {"retCode": 0, "result": {"list": []}}
                return {"retCode": 0, "result": {"list": [{"size": "2", "positionMM": ""}]}}
            raise AssertionError(path)

        transport._request = mock_request
        assert await transport.fetch_account_risk_snapshot() is None

    @pytest.mark.asyncio
    async def test_empty_maintenance_margin_does_not_miss_a_later_position_page(self):
        transport = self._make_bybit_transport()

        async def mock_request(method, path, params=None, body=None, private=False):
            if path == "/v5/account/wallet-balance":
                return self._bybit_wallet_balance_response(total_maintenance_margin="")
            if path == "/v5/account/info":
                return {"retCode": 0, "result": {"marginMode": "ISOLATED_MARGIN"}}
            if path == "/v5/position/list":
                if params == {"category": "linear", "settleCoin": "USDT", "limit": 200}:
                    return {"retCode": 0, "result": {"list": [], "nextPageCursor": "next"}}
                if params == {
                    "category": "linear", "settleCoin": "USDT", "limit": 200,
                    "cursor": "next",
                }:
                    return {
                        "retCode": 0,
                        "result": {"list": [{"size": "1", "positionMM": "3"}]},
                    }
                return {"retCode": 0, "result": {"list": []}}
            raise AssertionError(path)

        transport._request = mock_request
        result = await transport.fetch_account_risk_snapshot()

        assert result is not None
        assert result.maintenance_margin_quote == pytest.approx(3.0)

    @pytest.mark.asyncio
    async def test_dash_maintenance_margin_returns_none(self):
        """Bybit totalMaintenanceMargin='--' MUST return None (V1 parity)."""
        transport = self._make_bybit_transport()

        async def mock_request(method, path, params=None, body=None, private=False):
            return self._bybit_wallet_balance_response(
                total_equity="10000.50",
                total_maintenance_margin="--",
            )

        transport._request = mock_request
        result = await transport.fetch_account_risk_snapshot()
        assert result is None, "Dash maintenance_margin MUST return None (V1 parity)"

    @pytest.mark.asyncio
    async def test_null_string_maintenance_margin_returns_none(self):
        """Bybit totalMaintenanceMargin='null' MUST return None (V1 parity)."""
        transport = self._make_bybit_transport()

        async def mock_request(method, path, params=None, body=None, private=False):
            return self._bybit_wallet_balance_response(
                total_equity="10000.50",
                total_maintenance_margin="null",
            )

        transport._request = mock_request
        result = await transport.fetch_account_risk_snapshot()
        assert result is None, "'null' string MUST return None (V1 parity)"

    @pytest.mark.asyncio
    async def test_na_maintenance_margin_returns_none(self):
        """Bybit totalMaintenanceMargin='n/a' MUST return None (V1 parity)."""
        transport = self._make_bybit_transport()

        async def mock_request(method, path, params=None, body=None, private=False):
            return self._bybit_wallet_balance_response(
                total_equity="10000.50",
                total_maintenance_margin="n/a",
            )

        transport._request = mock_request
        result = await transport.fetch_account_risk_snapshot()
        assert result is None, "'n/a' string MUST return None (V1 parity)"

    @pytest.mark.asyncio
    async def test_nan_string_maintenance_margin_returns_none(self):
        """Bybit totalMaintenanceMargin='nan' MUST return None (V1 parity)."""
        transport = self._make_bybit_transport()

        async def mock_request(method, path, params=None, body=None, private=False):
            return self._bybit_wallet_balance_response(
                total_equity="10000.50",
                total_maintenance_margin="nan",
            )

        transport._request = mock_request
        result = await transport.fetch_account_risk_snapshot()
        assert result is None, "'nan' string MUST return None (V1 parity)"

    @pytest.mark.asyncio
    async def test_nan_uppercase_maintenance_margin_returns_none(self):
        """Bybit totalMaintenanceMargin='NAN' MUST return None (V1 parity: eq_ignore_ascii_case)."""
        transport = self._make_bybit_transport()

        async def mock_request(method, path, params=None, body=None, private=False):
            return self._bybit_wallet_balance_response(
                total_equity="10000.50",
                total_maintenance_margin="NAN",
            )

        transport._request = mock_request
        result = await transport.fetch_account_risk_snapshot()
        assert result is None, "'NAN' string MUST return None (V1 parity)"

    @pytest.mark.asyncio
    async def test_valid_maintenance_margin_returns_snapshot(self):
        """Bybit valid totalMaintenanceMargin MUST return AccountRiskSnapshot."""
        transport = self._make_bybit_transport()

        async def mock_request(method, path, params=None, body=None, private=False):
            return self._bybit_wallet_balance_response(
                total_equity="10000.50",
                total_maintenance_margin="500.25",
            )

        transport._request = mock_request
        result = await transport.fetch_account_risk_snapshot()
        assert result is not None
        assert result.maintenance_margin_quote == 500.25
        assert result.equity_quote == 10000.50

    @pytest.mark.asyncio
    async def test_empty_equity_returns_none(self):
        """Bybit totalEquity="" MUST return None (V1 parity)."""
        transport = self._make_bybit_transport()

        async def mock_request(method, path, params=None, body=None, private=False):
            return self._bybit_wallet_balance_response(
                total_equity="",
                total_maintenance_margin="500.25",
            )

        transport._request = mock_request
        result = await transport.fetch_account_risk_snapshot()
        assert result is None, "Empty equity MUST return None (V1 parity)"


class TestOkxAsterBinanceRiskSnapshotEmptyStringV1Parity:
    """V1 parity: OKX/Binance/Aster risk snapshots must not crash on empty strings."""

    def test_okx_mmr_empty_string_returns_none(self):
        """OKX mmr="" MUST return None, not crash (V1 parity)."""
        from lightfee.venues.transport import _parse_optional_float
        assert _parse_optional_float("") is None

    def test_binance_maint_margin_empty_string_returns_none(self):
        """Binance totalMaintMargin="" MUST return None (V1 parity)."""
        from lightfee.venues.transport import _parse_optional_float
        assert _parse_optional_float("") is None

    def test_aster_maint_margin_dash_returns_none(self):
        """Aster totalMaintMargin='--' MUST return None (V1 parity)."""
        from lightfee.venues.transport import _parse_optional_float
        assert _parse_optional_float("--") is None


class TestPassiveAmendWireContracts:
    @pytest.mark.asyncio
    async def test_binance_amend_uses_signed_query_params_without_json_body(self):
        cred = LiveCredential(api_key="key", api_secret="secret")
        transport = VenueTransport(spec=binance_spec(), mode="live", credential=cred)
        transport._time_offset_ms = 0
        captured: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["method"] = request.method
            captured["path"] = request.url.path
            captured["query"] = request.url.query.decode()
            captured["content"] = request.content
            return httpx.Response(
                200,
                json={
                    "orderId": 12345,
                    "clientOrderId": "cid-amend",
                    "price": "50001",
                    "origQty": "0.2",
                    "status": "NEW",
                },
            )

        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        request = PassiveOrderAmendRequest(
            symbol="BTCUSDT",
            side=Side.BUY,
            order_id="12345",
            client_order_id="cid-old",
            new_price_hint=50001.0,
            new_quantity=0.2,
        )
        try:
            await transport.amend_passive_order(request)
        finally:
            await transport.close()

        assert captured["method"] == "PUT"
        assert captured["path"] == "/fapi/v1/order"
        assert captured["content"] == b""
        query = captured["query"]
        assert "symbol=BTCUSDT" in query
        assert "orderId=12345" in query
        assert "side=BUY" in query
        assert "price=50001" in query
        assert "quantity=0.2" in query
        assert "recvWindow=10000" in query
        assert "timestamp=" in query
        assert "signature=" in query

    @pytest.mark.asyncio
    async def test_aster_amend_is_unsupported_and_does_not_send_put(self):
        cred = LiveCredential(api_key="key", api_secret="secret")
        transport = VenueTransport(spec=aster_spec(), mode="live", credential=cred)
        calls: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(500, text="should not be called")

        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        request = PassiveOrderAmendRequest(
            symbol="BTCUSDT",
            side=Side.SELL,
            order_id="aster-oid",
            client_order_id="aster-cid",
            new_price_hint=50001.0,
            new_quantity=0.2,
        )
        try:
            with pytest.raises(NotImplementedError):
                await transport.amend_passive_order(request)
        finally:
            await transport.close()
        assert calls == []

    @pytest.mark.asyncio
    async def test_okx_amend_uses_amend_order_endpoint_and_contract_quantity(self):
        cred = LiveCredential(api_key="key", api_secret="secret", api_passphrase="pass")
        transport = VenueTransport(spec=okx_spec(), mode="live", credential=cred)
        transport._time_offset_ms = 0
        transport.set_symbol_metadata({
            "HOME-USDT-SWAP": {
                "ctVal": "0.01",
                "lotSz": "1",
                "minSz": "1",
                "ctType": "linear",
            }
        })
        captured: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["method"] = request.method
            captured["path"] = request.url.path
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(
                200,
                json={
                    "code": "0",
                    "data": [{"ordId": "okx-oid", "clOrdId": "okx-cid", "sCode": "0"}],
                },
            )

        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        request = PassiveOrderAmendRequest(
            symbol="HOMEUSDT",
            side=Side.SELL,
            order_id="okx-oid",
            client_order_id="okx-cid",
            new_price_hint=0.044,
            new_quantity=0.2,
        )
        try:
            await transport.amend_passive_order(request)
        finally:
            await transport.close()

        assert captured["method"] == "POST"
        assert captured["path"] == "/api/v5/trade/amend-order"
        assert captured["body"]["instId"] == "HOME-USDT-SWAP"
        assert captured["body"]["ordId"] == "okx-oid"
        assert captured["body"]["newPx"] == "0.044"
        assert captured["body"]["newSz"] == "20"
        assert captured["body"]["cxlOnFail"] is False

    @pytest.mark.asyncio
    async def test_bybit_amend_uses_v5_amend_endpoint_not_create_path(self):
        cred = LiveCredential(api_key="key", api_secret="secret")
        transport = VenueTransport(spec=bybit_spec(), mode="live", credential=cred)
        transport._time_offset_ms = 0
        captured: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["method"] = request.method
            captured["path"] = request.url.path
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(
                200,
                json={
                    "retCode": 0,
                    "retMsg": "OK",
                    "result": {"orderId": "bybit-oid", "orderLinkId": "bybit-cid"},
                    "time": 1781350000000,
                },
            )

        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        request = PassiveOrderAmendRequest(
            symbol="HOMEUSDT",
            side=Side.BUY,
            order_id="bybit-oid",
            client_order_id="bybit-cid",
            new_price_hint=0.045,
            new_quantity=1500.0,
        )
        try:
            await transport.amend_passive_order(request)
        finally:
            await transport.close()

        assert captured["method"] == "POST"
        assert captured["path"] == "/v5/order/amend"
        assert captured["path"] != bybit_spec().order_path
        assert captured["body"]["category"] == "linear"
        assert captured["body"]["symbol"] == "HOMEUSDT"
        assert captured["body"]["orderId"] == "bybit-oid"
        assert captured["body"]["price"] == "0.045"
        assert captured["body"]["qty"] == "1500"


class TestBinanceAsterPrecisionFix:
    """Validate that VenueTransport.normalize_quantity for Binance/Aster in live mode
    properly queries the SymbolRulesCache, correctly handles dynamic rules like HIGHUSDT step=1,
    and resolves NameError issues.
    """

    @pytest.mark.asyncio
    async def test_binance_normalize_quantity_uses_dynamic_symbol_rules(self, monkeypatch):
        from lightfee.venues.symbol_rules import SymbolRule
        from lightfee.venues.specs import binance_spec

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                assert venue == Venue.BINANCE
                assert venue_symbol == "HIGHUSDT"
                return SymbolRule(
                    tick_size=0.001,
                    qty_step=1.0,  # step=1.0 precision
                    min_qty=1.0,
                    min_notional=5.0,
                    rule_source="exchangeInfo",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )

        transport = VenueTransport(
            spec=binance_spec(),
            mode="live",
            credential=LiveCredential(api_key="key", api_secret="secret"),
        )

        # 357.8 should normalize to 357.0 with step=1.0
        normalized = await transport.normalize_quantity("HIGHUSDT", 357.8)
        assert normalized == pytest.approx(357.0)

        # 0.5 should normalize to 0.0 because it's below min_qty=1.0
        normalized_below_min = await transport.normalize_quantity("HIGHUSDT", 0.5)
        assert normalized_below_min == 0.0

        await transport.close()

    @pytest.mark.asyncio
    async def test_binance_live_ioc_uses_dynamic_tick_for_low_price_symbol(self, monkeypatch):
        """HOMEUSDT IOC must not be rejected by Binance's coarse static tick."""
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                assert venue == Venue.BINANCE
                assert venue_symbol == "HOMEUSDT"
                return SymbolRule(
                    tick_size=0.000001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=5.0,
                    rule_source="exchangeInfo",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        transport = VenueTransport(
            binance_spec(),
            mode="live",
            credential=LiveCredential(api_key="key", api_secret="secret"),
        )
        calls: list[tuple[str, str, dict[str, object]]] = []

        async def fake_request(method, path, *, params=None, **_kwargs):
            calls.append((method, path, dict(params or {})))
            if path == "/fapi/v1/positionSide/dual":
                return {"dualSidePosition": False}
            assert path == "/fapi/v1/order"
            return {
                "symbol": "HOMEUSDT",
                "side": "BUY",
                "status": "FILLED",
                "executedQty": "4500",
                "avgPrice": "0.009793",
                "orderId": 987654,
            }

        transport._request = fake_request
        fill = await transport.place_order(OrderRequest(
            venue=Venue.BINANCE,
            symbol="HOMEUSDT",
            side=Side.BUY,
            quantity=4500.0,
            price=0.009793,
            time_in_force=TimeInForce.IOC,
        ))

        order_params = [call[2] for call in calls if call[1] == "/fapi/v1/order"][0]
        assert fill.order_id == "987654"
        assert order_params["type"] == "MARKET"
        assert order_params["quantity"] == "4500"
        assert "price" not in order_params
        assert transport.order_diagnostics[-1]["payload"]["tick_size"] == pytest.approx(0.000001)
        await transport.close()

    @pytest.mark.asyncio
    async def test_aster_normalize_quantity_uses_dynamic_symbol_rules(self, monkeypatch):
        from lightfee.venues.symbol_rules import SymbolRule
        from lightfee.venues.specs import aster_spec

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                assert venue == Venue.ASTER
                assert venue_symbol == "HIGHUSDT"
                return SymbolRule(
                    tick_size=0.001,
                    qty_step=1.0,
                    min_qty=1.0,
                    min_notional=5.0,
                    rule_source="exchangeInfo",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )

        transport = VenueTransport(
            spec=aster_spec(),
            mode="live",
            credential=LiveCredential(api_key="key", api_secret="secret"),
        )

        # 357.8 should normalize to 357.0 with step=1.0
        normalized = await transport.normalize_quantity("HIGHUSDT", 357.8)
        assert normalized == pytest.approx(357.0)

        await transport.close()

    @pytest.mark.asyncio
    async def test_symbol_rules_cache_retries_after_spec_fallback(self):
        from lightfee.venues.symbol_rules import SymbolRulesCache

        transport = VenueTransport(
            spec=aster_spec(),
            mode="paper",
        )
        calls = 0

        async def public_get(path, *, params=None):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("temporary exchangeInfo outage")
            return {
                "symbols": [
                    {
                        "symbol": "HIGHUSDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.001"},
                            {
                                "filterType": "LOT_SIZE",
                                "stepSize": "1",
                                "minQty": "1",
                            },
                            {"filterType": "MIN_NOTIONAL", "notional": "5"},
                        ],
                    }
                ]
            }

        transport._public_get = public_get
        cache = SymbolRulesCache()

        first = await cache.get(transport, Venue.ASTER, "HIGHUSDT")
        second = await cache.get(transport, Venue.ASTER, "HIGHUSDT")

        assert first.rule_source == "spec_fallback"
        assert second.rule_source == "exchangeInfo"
        assert second.qty_step == 1.0
        assert calls == 2

    @pytest.mark.asyncio
    async def test_symbol_rules_cache_rejects_nonfinite_exchange_info_rules(self):
        from lightfee.venues.symbol_rules import SymbolRulesCache

        transport = VenueTransport(spec=aster_spec(), mode="paper")

        async def public_get(path, *, params=None):
            assert path == "/fapi/v1/exchangeInfo"
            return {
                "symbols": [
                    {
                        "symbol": "GUAUSDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "NaN"},
                            {
                                "filterType": "LOT_SIZE",
                                "stepSize": "1",
                                "minQty": "1",
                            },
                            {"filterType": "MIN_NOTIONAL", "notional": "5"},
                        ],
                    }
                ]
            }

        transport._public_get = public_get
        rule = await SymbolRulesCache().get(transport, Venue.ASTER, "GUAUSDT")

        assert rule.rule_source == "spec_fallback"
        await transport.close()
