"""Tests for shared venue transport, signing, error mapping, and sizing."""

from __future__ import annotations

import hashlib
import hmac
import os
import time
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from lightfee.core.domain import (
    OrderRequest,
    PositionSnapshot,
    Side,
    Venue,
    VenueMarketQuote,
    VenueMarketSnapshot,
)
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.venues.common import (
    floor_to_step,
    normalize_order_quantity,
    venue_reduce_only_close_exempts_min_notional,
)
from lightfee.venues.specs import (
    AuthScheme,
    VenueSpec,
    binance_spec,
    okx_spec,
    bybit_spec,
    bitget_spec,
    gate_spec,
    aster_spec,
    hyperliquid_spec,
)
from lightfee.venues.transport import (
    LiveCredential,
    TransportError,
    TransportErrorCategory,
    VenueTransport,
    build_hmac_sha256_hex,
    build_hmac_sha256_base64,
    build_hmac_sha512_hex,
    classify_transport_error,
)


# ---------------------------------------------------------------------------
# Paper / live mode construction
# ---------------------------------------------------------------------------

class TestTransportConstruction:
    def test_paper_mode_builds_without_credentials(self):
        transport = VenueTransport(spec=binance_spec(), mode="paper")
        assert transport.mode == "paper"
        assert transport.venue == Venue.BINANCE

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

    def test_aster_signing_is_hmac_sha256_hex(self):
        spec = aster_spec()
        assert spec.auth_scheme == AuthScheme.HMAC_SHA256_HEX

    def test_hyperliquid_signing_is_eip712(self):
        spec = hyperliquid_spec()
        assert spec.auth_scheme == AuthScheme.EIP712


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
        assert not venue_reduce_only_close_exempts_min_notional(Venue.GATE)

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
        """When a transport error occurs, it maps to OrderSubmitError."""
        from lightfee.venues.transport import _map_to_submit_error
        err = _map_to_submit_error(TransportErrorCategory.REQUEST_REJECTED, "bad")
        assert isinstance(err, OrderSubmitError)
        assert err.class_ == SubmitFailureClass.REJECTED

    @pytest.mark.asyncio
    async def test_uncertain_error_maps_correctly(self):
        from lightfee.venues.transport import _map_to_submit_error
        err = _map_to_submit_error(TransportErrorCategory.TRANSPORT_FAILURE, "timeout")
        assert err.class_ == SubmitFailureClass.UNCERTAIN


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
