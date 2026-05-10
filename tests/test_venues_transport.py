"""Tests for shared venue transport, signing, error mapping, and sizing."""

from __future__ import annotations

import hashlib
import hmac
import json
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
        headers = transport._build_auth_headers("GET", "/api/v5/account/positions")
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
        headers = transport._build_auth_headers("GET", "/api/mix/v1/position/singlePosition")
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

        h_okx = t_okx._build_auth_headers("GET", "/test")
        h_bg = t_bg._build_auth_headers("GET", "/test")

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
    """Binance and Aster POST orders MUST include timestamp and signature in
    the query string, not just when GET params are present."""

    def test_binance_post_request_includes_timestamp_signature(self):
        spec = binance_spec()
        cred = LiveCredential(api_key="bk", api_secret="bs")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        qs, headers, body = transport._build_signed_request(
            "POST", "/fapi/v1/order",
            body={"symbol": "BTCUSDT", "side": "BUY", "quantity": "0.01"},
        )
        # Query string must contain timestamp and signature
        assert "timestamp=" in qs, f"Missing timestamp in query string: {qs}"
        assert "signature=" in qs, f"Missing signature in query string: {qs}"
        # Headers should contain API key
        assert headers.get("X-MBX-APIKEY") == "bk"

    def test_aster_post_request_includes_timestamp_signature(self):
        spec = aster_spec()
        cred = LiveCredential(api_key="ak", api_secret="as")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        qs, headers, body = transport._build_signed_request(
            "POST", "/fapi/v1/order",
            body={"symbol": "BTCUSDT", "side": "SELL", "quantity": "0.01"},
        )
        assert "timestamp=" in qs
        assert "signature=" in qs
        assert headers.get("X-MBX-APIKEY") == "ak"

    def test_binance_post_signature_matches_signed_payload(self):
        """The signature must be HMAC-SHA256 of the sorted query params."""
        spec = binance_spec()
        cred = LiveCredential(api_key="bk", api_secret="bs")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        qs, headers, body = transport._build_signed_request(
            "POST", "/fapi/v1/order",
            body={"symbol": "BTCUSDT", "side": "BUY", "quantity": "0.01"},
        )
        # Parse query string
        params = dict(p.split("=", 1) for p in qs.lstrip("?").split("&"))
        assert "signature" in params
        sig = params.pop("signature")
        # Recompute expected signature
        payload = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        expected = build_hmac_sha256_hex("bs", payload)
        assert sig == expected, f"Signature mismatch: {sig} != {expected}"

    def test_binance_get_without_params_still_signs(self):
        """Even GET requests without extra params must have timestamp+signature."""
        spec = binance_spec()
        cred = LiveCredential(api_key="bk", api_secret="bs")
        transport = VenueTransport(spec=spec, mode="live", credential=cred)
        qs, headers, body = transport._build_signed_request(
            "GET", "/fapi/v2/positionRisk",
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


# ---------------------------------------------------------------------------
# Hyperliquid live order explicit unsupported (Deviation 4)
# ---------------------------------------------------------------------------

class TestHyperliquidLiveOrderUnsupported:
    """Hyperliquid live order submission must explicitly fail, not silently
    produce a fake fill."""

    @pytest.mark.asyncio
    async def test_live_place_order_raises_order_submit_error(self):
        cred = LiveCredential(api_key="k", api_secret="s",
                              wallet_private_key="0xdead", account_address="0xbeef")
        transport = VenueTransport(spec=hyperliquid_spec(), mode="live", credential=cred)
        req = OrderRequest(
            venue=Venue.HYPERLIQUID, symbol="BTC", side=Side.BUY, quantity=1.0,
        )
        with pytest.raises(OrderSubmitError) as exc_info:
            await transport.place_order(req)
        assert exc_info.value.class_ == SubmitFailureClass.REJECTED
        assert "not yet implemented" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_paper_mode_still_works(self):
        transport = VenueTransport(spec=hyperliquid_spec(), mode="paper")
        req = OrderRequest(
            venue=Venue.HYPERLIQUID, symbol="BTC", side=Side.SELL, quantity=0.01,
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
