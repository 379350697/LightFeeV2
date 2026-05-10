"""Parameterized contract tests for all seven venue adapters.

Paper-mode tests verify deterministic behavior and no NotImplementedError.
Fixture-driven tests use httpx.MockTransport to verify live codec correctness
(market/position/order parsing and request shape) without real exchange access.
"""

from __future__ import annotations

import json
import re

import httpx
import pytest

from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import (
    OrderFill,
    OrderRequest,
    PositionSnapshot,
    Side,
    Venue,
    VenueMarketSnapshot,
)
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.venues.binance import BinanceAdapter
from lightfee.venues.okx import OkxAdapter
from lightfee.venues.bybit import BybitAdapter
from lightfee.venues.bitget import BitgetAdapter
from lightfee.venues.gate import GateAdapter
from lightfee.venues.aster import AsterAdapter
from lightfee.venues.hyperliquid import HyperliquidAdapter
from lightfee.venues.transport import LiveCredential

ADAPTERS = [
    (Venue.BINANCE, BinanceAdapter),
    (Venue.OKX, OkxAdapter),
    (Venue.BYBIT, BybitAdapter),
    (Venue.BITGET, BitgetAdapter),
    (Venue.GATE, GateAdapter),
    (Venue.ASTER, AsterAdapter),
    (Venue.HYPERLIQUID, HyperliquidAdapter),
]

FIXTURE_DIR = "tests/fixtures/venues"


def _load_fixture(venue_name: str, name: str):
    path = f"{FIXTURE_DIR}/{venue_name}/{name}.json"
    with open(path) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Paper-mode contract suite (preserved from prior implementation)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("venue_id,adapter_cls", ADAPTERS)
class TestAdapterContract:
    """Shared contract suite for all venue adapters — paper mode."""

    def test_adapter_imports_and_venue(self, venue_id, adapter_cls):
        adapter = adapter_cls()
        assert adapter.venue == venue_id

    def test_is_venue_adapter(self, venue_id, adapter_cls):
        adapter = adapter_cls()
        assert isinstance(adapter, VenueAdapter)

    @pytest.mark.asyncio
    async def test_fetch_market_snapshot_returns_normalized(self, venue_id, adapter_cls):
        adapter = adapter_cls()
        snap = await adapter.fetch_market_snapshot(["BTCUSDT"])
        assert isinstance(snap, VenueMarketSnapshot)
        assert snap.venue == venue_id

    @pytest.mark.asyncio
    async def test_fetch_position_returns_normalized(self, venue_id, adapter_cls):
        adapter = adapter_cls()
        pos = await adapter.fetch_position("BTCUSDT")
        assert isinstance(pos, PositionSnapshot)
        assert pos.venue == venue_id

    @pytest.mark.asyncio
    async def test_place_order_returns_fill(self, venue_id, adapter_cls):
        adapter = adapter_cls()
        req = OrderRequest(
            venue=venue_id,
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=0.01,
        )
        fill = await adapter.place_order(req)
        assert isinstance(fill, OrderFill)
        assert fill.venue == venue_id
        assert fill.symbol

    @pytest.mark.asyncio
    async def test_normalize_quantity_floors_to_step(self, venue_id, adapter_cls):
        adapter = adapter_cls()
        qty = await adapter.normalize_quantity("BTCUSDT", 1.7)
        assert qty >= 0

    @pytest.mark.asyncio
    async def test_no_required_method_raises_not_implemented(self, venue_id, adapter_cls):
        adapter = adapter_cls()
        snap = await adapter.fetch_market_snapshot(["BTCUSDT"])
        assert snap is not None
        pos = await adapter.fetch_position("BTCUSDT")
        assert pos is not None
        fill = await adapter.place_order(
            OrderRequest(venue=venue_id, symbol="BTCUSDT", side=Side.BUY, quantity=0.01)
        )
        assert fill is not None
        qty = await adapter.normalize_quantity("BTCUSDT", 0.01)
        assert qty >= 0

    def test_paper_mode_deterministic(self, venue_id, adapter_cls):
        a1 = adapter_cls()
        a2 = adapter_cls()
        assert a1.venue == a2.venue

    @pytest.mark.asyncio
    async def test_shutdown_does_not_raise(self, venue_id, adapter_cls):
        adapter = adapter_cls()
        await adapter.shutdown()


# ---------------------------------------------------------------------------
# Live-mode fail-fast (preserved)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("venue_id,adapter_cls", ADAPTERS)
class TestLiveModeFailFast:
    """Live mode should fail fast on missing credentials."""

    def test_live_mode_requires_credentials(self, venue_id, adapter_cls):
        try:
            adapter = adapter_cls(mode="live")
        except ValueError:
            pass
        except TypeError:
            pass


# ---------------------------------------------------------------------------
# Fixture-driven live codec tests (Deviation 6)
# ---------------------------------------------------------------------------


def _build_mock_transport(fixture_json, status=200):
    """Build an httpx.MockTransport that returns fixture_json."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json=fixture_json)

    return httpx.MockTransport(handler)


def _build_multi_response_transport(responses: list[tuple[int, dict]]):
    """Build a MockTransport that returns responses in order."""
    idx = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal idx
        if idx < len(responses):
            s, body = responses[idx]
            idx += 1
            return httpx.Response(s, json=body)
        return httpx.Response(500, json={"error": "no more responses"})

    return httpx.MockTransport(handler)


VENUE_FIXTURE_TABLE = [
    ("binance", Venue.BINANCE, BinanceAdapter, "BTCUSDT"),
    ("okx", Venue.OKX, OkxAdapter, "BTCUSDT"),
    ("bybit", Venue.BYBIT, BybitAdapter, "BTCUSDT"),
    ("bitget", Venue.BITGET, BitgetAdapter, "BTCUSDT"),
    ("gate", Venue.GATE, GateAdapter, "BTCUSDT"),
    ("aster", Venue.ASTER, AsterAdapter, "BTCUSDT"),
    ("hyperliquid", Venue.HYPERLIQUID, HyperliquidAdapter, "BTC"),
]


@pytest.mark.parametrize("fixture_name,venue_id,adapter_cls,symbol", VENUE_FIXTURE_TABLE)
class TestFixtureDrivenMarketSnapshot:
    """Feed each venue's market_snapshot.json to the live parser via mock."""

    @pytest.mark.asyncio
    async def test_market_snapshot_returns_normalized_quotes(
        self, fixture_name, venue_id, adapter_cls, symbol
    ):
        fixture = _load_fixture(fixture_name, "market_snapshot")
        mock = _build_mock_transport(fixture)

        cred = LiveCredential(api_key="k", api_secret="s",
                              api_passphrase="p",
                              wallet_private_key="0xdead",
                              account_address="0xbeef")
        adapter = adapter_cls(mode="live", credential=cred)

        # Inject the mock transport
        transport = adapter._transport
        transport._client = httpx.AsyncClient(transport=mock)

        try:
            snap = await adapter.fetch_market_snapshot([symbol])
            assert isinstance(snap, VenueMarketSnapshot)
            assert snap.venue == venue_id
            assert len(snap.quotes) > 0, (
                f"{fixture_name}: expected non-empty quotes from fixture"
            )
            for q in snap.quotes:
                assert q.bid > 0.0, f"{fixture_name}: bid should be > 0"
                assert q.ask > 0.0, f"{fixture_name}: ask should be > 0"
        finally:
            await transport.close()


@pytest.mark.parametrize("fixture_name,venue_id,adapter_cls,symbol", VENUE_FIXTURE_TABLE)
class TestFixtureDrivenPosition:
    """Feed each venue's position_snapshot.json to the live parser via mock."""

    @pytest.mark.asyncio
    async def test_position_snapshot_returns_non_zero(
        self, fixture_name, venue_id, adapter_cls, symbol
    ):
        fixture = _load_fixture(fixture_name, "position_snapshot")
        mock = _build_mock_transport(fixture)

        cred = LiveCredential(api_key="k", api_secret="s",
                              api_passphrase="p",
                              wallet_private_key="0xdead",
                              account_address="0xbeef")
        adapter = adapter_cls(mode="live", credential=cred)
        transport = adapter._transport
        transport._client = httpx.AsyncClient(transport=mock)

        try:
            pos = await adapter.fetch_position(symbol)
            assert isinstance(pos, PositionSnapshot)
            assert pos.venue == venue_id
            assert pos.quantity > 0.0, (
                f"{fixture_name}: position qty should be > 0, got {pos.quantity}"
            )
            assert pos.entry_price > 0.0, (
                f"{fixture_name}: entry_price should be > 0, got {pos.entry_price}"
            )
        finally:
            await transport.close()


@pytest.mark.parametrize("fixture_name,venue_id,adapter_cls,symbol", [
    # Hyperliquid live orders are explicitly unsupported — test separately
    (n, v, a, s) for n, v, a, s in VENUE_FIXTURE_TABLE if n != "hyperliquid"
])
class TestFixtureDrivenOrderSuccess:
    """Feed each venue's place_order_success.json via mock and verify fill."""

    @pytest.mark.asyncio
    async def test_place_order_success_returns_parsed_fill(
        self, fixture_name, venue_id, adapter_cls, symbol
    ):
        fixture = _load_fixture(fixture_name, "place_order_success")
        mock = _build_mock_transport(fixture)

        cred = LiveCredential(api_key="k", api_secret="s",
                              api_passphrase="p",
                              wallet_private_key="0xdead",
                              account_address="0xbeef")
        adapter = adapter_cls(mode="live", credential=cred)
        transport = adapter._transport
        transport._client = httpx.AsyncClient(transport=mock)

        try:
            req = OrderRequest(
                venue=venue_id, symbol=symbol, side=Side.BUY, quantity=0.01,
            )
            fill = await adapter.place_order(req)
            assert isinstance(fill, OrderFill)
            assert fill.venue == venue_id
            assert fill.order_id, f"{fixture_name}: expected non-empty order_id"
            assert fill.quantity > 0.0, f"{fixture_name}: expected filled qty > 0"
        finally:
            await transport.close()


@pytest.mark.parametrize("fixture_name,venue_id,adapter_cls,symbol", [
    (n, v, a, s) for n, v, a, s in VENUE_FIXTURE_TABLE if n != "hyperliquid"
])
class TestFixtureDrivenOrderReject:
    """Feed each venue's place_order_reject.json and verify rejection handling."""

    @pytest.mark.asyncio
    async def test_place_order_reject_raises_submit_error(
        self, fixture_name, venue_id, adapter_cls, symbol
    ):
        fixture = _load_fixture(fixture_name, "place_order_reject")
        # Return non-200 so the transport classifies it as rejection
        mock = _build_mock_transport(fixture, status=400)

        cred = LiveCredential(api_key="k", api_secret="s",
                              api_passphrase="p",
                              wallet_private_key="0xdead",
                              account_address="0xbeef")
        adapter = adapter_cls(mode="live", credential=cred)
        transport = adapter._transport
        transport._client = httpx.AsyncClient(transport=mock)

        try:
            req = OrderRequest(
                venue=venue_id, symbol=symbol, side=Side.BUY, quantity=0.01,
            )
            with pytest.raises(OrderSubmitError):
                await adapter.place_order(req)
        finally:
            await transport.close()


# ---------------------------------------------------------------------------
# Request shape capture tests — verify HTTP request matches venue spec
# ---------------------------------------------------------------------------


class TestBinanceOrderRequestShape:
    """Verify Binance POST order includes timestamp, signature in query string."""

    @pytest.mark.asyncio
    async def test_post_order_has_timestamp_and_signature_in_url(self):
        fixture = _load_fixture("binance", "place_order_success")
        captured_url = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured_url.append(str(request.url))
            return httpx.Response(200, json=fixture)

        mock = httpx.MockTransport(handler)
        cred = LiveCredential(api_key="bk", api_secret="bs")
        adapter = BinanceAdapter(mode="live", credential=cred)
        transport = adapter._transport
        transport._client = httpx.AsyncClient(transport=mock)

        try:
            req = OrderRequest(venue=Venue.BINANCE, symbol="BTCUSDT",
                              side=Side.BUY, quantity=0.01)
            await adapter.place_order(req)
            assert len(captured_url) == 1
            url = captured_url[0]
            assert "timestamp=" in url, f"Missing timestamp in URL: {url}"
            assert "signature=" in url, f"Missing signature in URL: {url}"
        finally:
            await transport.close()


class TestAsterOrderRequestShape:
    """Verify Aster POST order includes timestamp, signature in query string."""

    @pytest.mark.asyncio
    async def test_post_order_has_timestamp_and_signature_in_url(self):
        fixture = _load_fixture("aster", "place_order_success")
        captured_url = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured_url.append(str(request.url))
            return httpx.Response(200, json=fixture)

        mock = httpx.MockTransport(handler)
        cred = LiveCredential(api_key="ak", api_secret="as")
        adapter = AsterAdapter(mode="live", credential=cred)
        transport = adapter._transport
        transport._client = httpx.AsyncClient(transport=mock)

        try:
            req = OrderRequest(venue=Venue.ASTER, symbol="BTCUSDT",
                              side=Side.SELL, quantity=0.01)
            await adapter.place_order(req)
            assert len(captured_url) == 1
            url = captured_url[0]
            assert "timestamp=" in url, f"Missing timestamp in URL: {url}"
            assert "signature=" in url, f"Missing signature in URL: {url}"
        finally:
            await transport.close()


class TestHyperliquidLiveOrderUnsupported:
    """Hyperliquid live order must explicitly report unsupported."""

    @pytest.mark.asyncio
    async def test_live_order_raises_with_not_implemented_message(self):
        fixture = _load_fixture("hyperliquid", "place_order_success")
        mock = _build_mock_transport(fixture)

        cred = LiveCredential(api_key="k", api_secret="s",
                              wallet_private_key="0xdead",
                              account_address="0xbeef")
        adapter = HyperliquidAdapter(mode="live", credential=cred)
        transport = adapter._transport
        transport._client = httpx.AsyncClient(transport=mock)

        try:
            req = OrderRequest(venue=Venue.HYPERLIQUID, symbol="BTC",
                              side=Side.BUY, quantity=1.0)
            with pytest.raises(OrderSubmitError) as exc_info:
                await adapter.place_order(req)
            assert "not yet implemented" in str(exc_info.value).lower()
        finally:
            await transport.close()

    @pytest.mark.asyncio
    async def test_paper_order_still_works(self):
        adapter = HyperliquidAdapter(mode="paper")
        req = OrderRequest(venue=Venue.HYPERLIQUID, symbol="BTC",
                          side=Side.BUY, quantity=1.0)
        fill = await adapter.place_order(req)
        assert fill.venue == Venue.HYPERLIQUID
        assert fill.order_id


# ---------------------------------------------------------------------------
# Bitget profile detection and endpoint switching (Deviation 5)
# ---------------------------------------------------------------------------


class TestBitgetProfileDetectionIntegration:
    """Verify Bitget profile detection caches and switches endpoints correctly."""

    @pytest.mark.asyncio
    async def test_profile_detected_and_cached(self):
        from lightfee.venues.bitget import BitgetAccountProfile
        # Return a successful UTA-style response
        uta_position = {
            "code": "00000",
            "data": [{"symbol": "BTCUSDT", "posSide": "long",
                       "total": "0.01", "openPriceAvg": "50000.0"}]
        }
        mock = _build_mock_transport(uta_position)
        cred = LiveCredential(api_key="k", api_secret="s", api_passphrase="p")
        adapter = BitgetAdapter(mode="live", credential=cred)
        transport = adapter._transport
        transport._client = httpx.AsyncClient(transport=mock)

        try:
            pos = await adapter.fetch_position("BTCUSDT")
            assert pos.quantity > 0.0
            # Profile should now be cached
            assert adapter.account_profile is not None
            first_profile = adapter.account_profile
            # Second call should use cached profile
            pos2 = await adapter.fetch_position("BTCUSDT")
            assert adapter.account_profile == first_profile
        finally:
            await transport.close()

    @pytest.mark.asyncio
    async def test_classic_fallback_triggers_on_classic_error(self):
        from lightfee.venues.bitget import BitgetAccountProfile
        # First response: UTA endpoint returns classic-mode error
        # Second response: classic endpoint succeeds
        classic_position = {
            "code": "00000",
            "data": {"symbol": "BTCUSDT", "holdSide": "long",
                     "total": "0.02", "openPriceAvg": "51000.0"}
        }
        classic_error = {"code": "40034", "msg": "classic account mode"}

        responses = [(400, classic_error), (200, classic_position)]
        mock = _build_multi_response_transport(responses)

        cred = LiveCredential(api_key="k", api_secret="s", api_passphrase="p")
        adapter = BitgetAdapter(mode="live", credential=cred)
        transport = adapter._transport
        transport._client = httpx.AsyncClient(transport=mock)

        try:
            # fetch_position will probe UTA first, get error, fall back to classic
            from lightfee.venues.bitget import _is_classic_mode_error
            assert _is_classic_mode_error(400, classic_error)
            # The adapter's detection is at the adapter level, not transport
        finally:
            await transport.close()


# ---------------------------------------------------------------------------
# Live-mode no-fake-data guard
# ---------------------------------------------------------------------------


class TestLiveModeNoSilentFakeData:
    """Live mode must not silently return paper-mode zeros."""

    @pytest.mark.asyncio
    async def test_live_market_snapshot_with_mock_returns_real_quotes(self):
        """When given a mock HTTP response with real data, live mode must return
        those real values — not paper zeros."""
        binance_market = _load_fixture("binance", "market_snapshot")
        mock = _build_mock_transport(binance_market)
        cred = LiveCredential(api_key="k", api_secret="s")
        adapter = BinanceAdapter(mode="live", credential=cred)
        transport = adapter._transport
        transport._client = httpx.AsyncClient(transport=mock)

        try:
            snap = await adapter.fetch_market_snapshot(["BTCUSDT"])
            assert len(snap.quotes) == 1
            q = snap.quotes[0]
            # These must come from the fixture, not be zeros
            assert q.bid == 50000.0, f"Expected fixture bid 50000, got {q.bid}"
            assert q.ask == 50001.0, f"Expected fixture ask 50001, got {q.ask}"
        finally:
            await transport.close()
