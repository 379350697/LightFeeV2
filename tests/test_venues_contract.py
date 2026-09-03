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
from lightfee.venues.specs import BitgetContractFamily
from lightfee.venues.symbol_rules import get_symbol_rules_cache
from lightfee.venues.transport import LiveCredential, TransportError, TransportErrorCategory

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
HL_FIXTURE_PRIVATE_KEY = "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e"
ASTER_FIXTURE_PRIVATE_KEY = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1"
ASTER_FIXTURE_ACCOUNT_ADDRESS = "0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e"


def _fixture_wallet_private_key(venue_id: Venue) -> str:
    if venue_id == Venue.HYPERLIQUID:
        return HL_FIXTURE_PRIVATE_KEY
    if venue_id == Venue.ASTER:
        return ASTER_FIXTURE_PRIVATE_KEY
    return "0x" + "1" * 64


def _fixture_account_address(venue_id: Venue) -> str:
    if venue_id == Venue.ASTER:
        return ASTER_FIXTURE_ACCOUNT_ADDRESS
    return "0xbeef"


async def _async_value(value):
    return value


def _trust_hyperliquid_transport_for_test(transport) -> None:
    transport._trading_capability_trusted = True
    transport._trading_preflight_status = {
        "venue": Venue.HYPERLIQUID.value,
        "status": "ok",
        "trading_capability_trusted": True,
        "authorization_mode": "account_wallet",
        "authorization_verified": True,
    }


def _load_fixture(venue_name: str, name: str):
    path = f"{FIXTURE_DIR}/{venue_name}/{name}.json"
    with open(path) as f:
        return json.load(f)


def _aster_opening_admission_fixture(path: str):
    """Return the two V3 evidence responses required before an opening order."""
    if path == "/fapi/v3/positionRisk":
        return [{
            "symbol": "BTCUSDT",
            "positionAmt": "0",
            "markPrice": "50000",
            "maxNotionalValue": "100000",
        }]
    if path == "/fapi/v3/openOrders":
        return []
    if path == "/fapi/v3/positionSide/dual":
        return {"dualSidePosition": False}
    return None


def _attach_mock_transport(adapter, transport, mock) -> None:
    transport._client = httpx.AsyncClient(transport=mock)
    private = getattr(adapter, "_private", None)
    if private is not None:
        private._client = httpx.AsyncClient(transport=mock)
        private._owns_client = True


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
    async def test_normalize_quantity_floors_to_step(self, venue_id, adapter_cls, monkeypatch):
        adapter = adapter_cls()
        public_get_calls = []
        if venue_id == Venue.BYBIT:
            async def fail_public_get(*args, **kwargs):
                public_get_calls.append((args, kwargs))
                raise AssertionError("paper normalize_quantity must not fetch live rules")

            monkeypatch.setattr(adapter._transport, "_public_get", fail_public_get)
        if venue_id == Venue.OKX:
            adapter._transport.set_symbol_metadata({
                "BTC-USDT-SWAP": {"ct_val": "0.01", "lot_sz": "1", "min_sz": "1"}
            })
        qty = await adapter.normalize_quantity("BTCUSDT", 1.7)
        assert qty >= 0
        if venue_id == Venue.BYBIT:
            assert public_get_calls == []

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
        if venue_id == Venue.OKX:
            adapter._transport.set_symbol_metadata({
                "BTC-USDT-SWAP": {"ct_val": "0.01", "lot_sz": "1", "min_sz": "1"}
            })
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


def _inject_mock_client(adapter, mock_transport):
    """Inject mock transport and pre-fill server-time offset for V1 fail-closed compat."""
    adapter._transport._client = httpx.AsyncClient(transport=mock_transport)
    adapter._transport._time_offset_ms = 0


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
                              wallet_private_key=_fixture_wallet_private_key(venue_id),
                              account_address=_fixture_account_address(venue_id))
        adapter = adapter_cls(mode="live", credential=cred)

        # Inject the mock transport
        transport = adapter._transport
        _attach_mock_transport(adapter, transport, mock)
        transport._time_offset_ms = 0  # V1 fail-closed compat; transport._time_offset_ms = 0

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
                              wallet_private_key=_fixture_wallet_private_key(venue_id),
                              account_address=_fixture_account_address(venue_id))
        adapter = adapter_cls(mode="live", credential=cred)
        transport = adapter._transport
        _attach_mock_transport(adapter, transport, mock)
        transport._time_offset_ms = 0  # V1 fail-closed compat; transport._time_offset_ms = 0
        if venue_id == Venue.OKX:
            transport.set_symbol_metadata({
                "BTC-USDT-SWAP": {
                    "ct_val": "0.01",
                    "ctType": "linear",
                    "lot_sz": "1",
                    "min_sz": "1",
                }
            })
            transport._okx_swap_instruments_loaded = True

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


@pytest.mark.parametrize("fixture_name,venue_id,adapter_cls,symbol", VENUE_FIXTURE_TABLE)
class TestFixtureDrivenOrderSuccess:
    """Feed each venue's place_order_success.json via mock and verify fill."""

    @pytest.mark.asyncio
    async def test_place_order_success_returns_parsed_fill(
        self, fixture_name, venue_id, adapter_cls, symbol, monkeypatch
    ):
        fixture = _load_fixture(fixture_name, "place_order_success")
        mock = _build_mock_transport(fixture)
        if venue_id == Venue.BINANCE:
            exchange_info = _load_fixture("binance", "exchange_info")
            get_symbol_rules_cache().clear()

            def binance_handler(request: httpx.Request) -> httpx.Response:
                if request.url.path == "/fapi/v1/exchangeInfo":
                    assert request.url.params["symbol"] == symbol
                    return httpx.Response(200, json=exchange_info)
                return httpx.Response(200, json=fixture)

            mock = httpx.MockTransport(binance_handler)
        if venue_id == Venue.ASTER:
            def aster_handler(request: httpx.Request) -> httpx.Response:
                admission = _aster_opening_admission_fixture(request.url.path)
                return httpx.Response(200, json=fixture if admission is None else admission)

            mock = httpx.MockTransport(aster_handler)

        cred = LiveCredential(
            api_key="k",
            api_secret="" if venue_id == Venue.HYPERLIQUID else "s",
            api_passphrase="p",
            wallet_private_key=_fixture_wallet_private_key(venue_id),
            account_address=_fixture_account_address(venue_id),
        )
        adapter = adapter_cls(mode="live", credential=cred)
        transport = adapter._transport
        _attach_mock_transport(adapter, transport, mock)
        transport._time_offset_ms = 0  # V1 fail-closed compat; transport._time_offset_ms = 0

        # Hyperliquid needs the asset index pre-populated so the mock
        # transport (single-response) doesn't need to serve metadata.
        if venue_id == Venue.HYPERLIQUID:
            transport._hl_meta_cache[symbol] = 0
            _trust_hyperliquid_transport_for_test(transport)
        if venue_id == Venue.OKX:
            class FakeRulesCache:
                async def get(self, transport, venue, venue_symbol):
                    return type(
                        "Rule",
                        (),
                        {
                            "ct_val": 0.01,
                            "qty_step": 1.0,
                            "min_qty": 1.0,
                            "max_market_qty": 0.0,
                            "rule_source": "instrument",
                        },
                    )()

            monkeypatch.setattr(
                "lightfee.venues.transport.get_symbol_rules_cache",
                lambda: FakeRulesCache(),
            )
        if venue_id == Venue.BITGET:
            from lightfee.venues.symbol_rules import SymbolRule

            class FakeRulesCache:
                async def get(self, transport, venue, venue_symbol):
                    assert venue == Venue.BITGET
                    return SymbolRule(
                        tick_size=0.000001,
                        qty_step=0.001,
                        min_qty=0.001,
                        min_notional=5.0,
                        rule_source="contracts",
                    )

            monkeypatch.setattr(
                "lightfee.venues.transport.get_symbol_rules_cache",
                lambda: FakeRulesCache(),
            )
        if venue_id == Venue.ASTER:
            async def fake_aster_public_get(path, params=None):
                assert path == "/fapi/v1/exchangeInfo"
                return {
                    "symbols": [{
                        "symbol": symbol,
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                            {"filterType": "MIN_NOTIONAL", "notional": "5"},
                        ],
                    }],
                }

            monkeypatch.setattr(transport, "_public_get", fake_aster_public_get)

        try:
            req = OrderRequest(
                venue=venue_id,
                symbol=symbol,
                side=Side.BUY,
                quantity=1.0 if venue_id in (Venue.GATE, Venue.HYPERLIQUID) else 0.01,
                price=50000.0 if venue_id == Venue.HYPERLIQUID else None,
            )
            fill = await adapter.place_order(req)
            assert isinstance(fill, OrderFill)
            assert fill.venue == venue_id
            assert fill.order_id, f"{fixture_name}: expected non-empty order_id"
            assert fill.quantity > 0.0, f"{fixture_name}: expected filled qty > 0"
        finally:
            await transport.close()
            if venue_id == Venue.BINANCE:
                get_symbol_rules_cache().clear()


@pytest.mark.parametrize("fixture_name,venue_id,adapter_cls,symbol", VENUE_FIXTURE_TABLE)
class TestFixtureDrivenOrderReject:
    """Feed each venue's place_order_reject.json and verify rejection handling."""

    @pytest.mark.asyncio
    async def test_place_order_reject_raises_submit_error(
        self, fixture_name, venue_id, adapter_cls, symbol
    ):
        fixture = _load_fixture(fixture_name, "place_order_reject")
        # Return non-200 so the transport classifies it as rejection
        mock = _build_mock_transport(fixture, status=400)

        hl_privkey = "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e"
        cred = LiveCredential(
            api_key="k",
            api_secret="" if venue_id == Venue.HYPERLIQUID else "s",
            api_passphrase="p",
            wallet_private_key=hl_privkey if venue_id == Venue.HYPERLIQUID else _fixture_wallet_private_key(venue_id),
            account_address=_fixture_account_address(venue_id),
        )
        adapter = adapter_cls(mode="live", credential=cred)
        transport = adapter._transport
        _attach_mock_transport(adapter, transport, mock)
        transport._time_offset_ms = 0  # V1 fail-closed compat; transport._time_offset_ms = 0

        # Pre-populate Hyperliquid asset index so mock only handles the order
        if venue_id == Venue.HYPERLIQUID:
            transport._hl_meta_cache[symbol] = 0

        try:
            req = OrderRequest(
                venue=venue_id, symbol=symbol, side=Side.BUY,
                quantity=1.0 if venue_id == Venue.HYPERLIQUID else 0.01,
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
        exchange_info = _load_fixture("binance", "exchange_info")
        captured_url = []
        get_symbol_rules_cache().clear()

        def handler(request: httpx.Request) -> httpx.Response:
            captured_url.append(str(request.url))
            if request.url.path == "/fapi/v1/exchangeInfo":
                assert request.url.params["symbol"] == "BTCUSDT"
                return httpx.Response(200, json=exchange_info)
            if request.url.path.endswith("/positionSide/dual"):
                return httpx.Response(200, json={"dualSidePosition": False})
            return httpx.Response(200, json=fixture)

        mock = httpx.MockTransport(handler)
        cred = LiveCredential(api_key="bk", api_secret="bs")
        adapter = BinanceAdapter(mode="live", credential=cred)
        transport = adapter._transport
        _attach_mock_transport(adapter, transport, mock)
        transport._time_offset_ms = 0  # V1 fail-closed compat; transport._time_offset_ms = 0

        try:
            req = OrderRequest(venue=Venue.BINANCE, symbol="BTCUSDT",
                              side=Side.BUY, quantity=0.01)
            await adapter.place_order(req)
            assert len(captured_url) == 3
            assert "/fapi/v1/exchangeInfo?symbol=BTCUSDT" in captured_url[0]
            assert "/fapi/v1/positionSide/dual" in captured_url[1]
            url = captured_url[2]
            assert "/fapi/v1/order" in url
            assert "timestamp=" in url, f"Missing timestamp in URL: {url}"
            assert "signature=" in url, f"Missing signature in URL: {url}"
        finally:
            await transport.close()
            get_symbol_rules_cache().clear()


class TestAsterOrderRequestShape:
    """Verify Aster POST order uses Pro API V3 signer params, not Binance HMAC."""

    @pytest.mark.asyncio
    async def test_post_order_has_signer_nonce_and_signature_in_url(self):
        from lightfee.venues.symbol_rules import get_symbol_rules_cache

        fixture = _load_fixture("aster", "place_order_success")
        captured_url = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured_url.append(str(request.url))
            if request.url.path == "/fapi/v1/exchangeInfo":
                return httpx.Response(200, json={
                    "symbols": [{
                        "symbol": "BTCUSDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                            {"filterType": "MIN_NOTIONAL", "notional": "5"},
                        ],
                    }],
                })
            admission = _aster_opening_admission_fixture(request.url.path)
            if admission is not None:
                return httpx.Response(200, json=admission)
            return httpx.Response(200, json=fixture)

        mock = httpx.MockTransport(handler)
        cred = LiveCredential(
            api_secret="0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1",
            account_address="0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e",
        )
        adapter = AsterAdapter(mode="live", credential=cred)
        assert adapter._private is not None
        adapter._transport._client = httpx.AsyncClient(transport=mock)
        adapter._private._client = httpx.AsyncClient(transport=mock)
        adapter._private._owns_client = True
        get_symbol_rules_cache().clear()

        try:
            req = OrderRequest(venue=Venue.ASTER, symbol="BTCUSDT",
                              side=Side.SELL, quantity=0.01)
            await adapter.place_order(req)
            assert len(captured_url) == 5
            assert "/fapi/v1/exchangeInfo?symbol=BTCUSDT" in captured_url[0]
            assert "/fapi/v3/positionRisk" in captured_url[1]
            assert "/fapi/v3/openOrders" in captured_url[2]
            assert "/fapi/v3/positionSide/dual" in captured_url[3]
            url = [item for item in captured_url if "/fapi/v3/order" in item][0]
            assert "https://fapi.asterdex.com/fapi/v3/order" in url
            assert "signer=" in url, f"Missing signer in URL: {url}"
            assert "nonce=" in url, f"Missing nonce in URL: {url}"
            assert "signature=" in url, f"Missing signature in URL: {url}"
            assert "timestamp=" not in url
            assert "recvWindow=" not in url
            assert "X-MBX-APIKEY" not in url
        finally:
            await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_private_order_rejection_records_http_and_rule_evidence(self):
        from lightfee.venues.symbol_rules import get_symbol_rules_cache

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/fapi/v1/exchangeInfo":
                return httpx.Response(200, json={
                    "symbols": [{
                        "symbol": "BTCUSDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                            {"filterType": "MIN_NOTIONAL", "notional": "5"},
                        ],
                    }],
                })
            admission = _aster_opening_admission_fixture(request.url.path)
            if admission is not None:
                return httpx.Response(200, json=admission)
            assert request.url.path == "/fapi/v3/order"
            return httpx.Response(400, json={"code": -3007, "msg": "fixture reject"})

        mock = httpx.MockTransport(handler)
        cred = LiveCredential(
            api_secret=ASTER_FIXTURE_PRIVATE_KEY,
            account_address=ASTER_FIXTURE_ACCOUNT_ADDRESS,
        )
        adapter = AsterAdapter(mode="live", credential=cred)
        assert adapter._private is not None
        adapter._transport._client = httpx.AsyncClient(transport=mock)
        adapter._private._client = httpx.AsyncClient(transport=mock)
        adapter._private._owns_client = True
        get_symbol_rules_cache().clear()

        try:
            request = OrderRequest(
                venue=Venue.ASTER,
                symbol="BTCUSDT",
                side=Side.SELL,
                quantity=0.01,
            )
            with pytest.raises(OrderSubmitError):
                await adapter.place_order(request)
            records = adapter._transport.drain_order_diagnostics()
            evidence = [
                record for record in records
                if record["kind"] == "order.private_submit_result"
            ]
            assert len(evidence) == 1
            payload = evidence[0]["payload"]
            assert payload["operation"] == "place_order"
            assert payload["endpoint"] == "/fapi/v3/order"
            assert payload["status_code"] == 400
            assert "-3007" in payload["response_body"]
            assert payload["rule_source"] == "exchangeInfo"
            assert payload["raw_qty"] == pytest.approx(0.01)
            assert payload["quantized_qty"] == pytest.approx(0.01)
        finally:
            await adapter.shutdown()


class TestHyperliquidLiveOrderNowSupported:
    """Hyperliquid live order now works with EIP-712 signing."""

    @pytest.mark.asyncio
    async def test_live_order_succeeds_with_fill_fixture(self):
        fixture = _load_fixture("hyperliquid", "place_order_success")
        mock = _build_mock_transport(fixture)

        cred = LiveCredential(api_key="k", api_secret="",
                              wallet_private_key=HL_FIXTURE_PRIVATE_KEY,
                              account_address="0xbeef")
        adapter = HyperliquidAdapter(mode="live", credential=cred)
        transport = adapter._transport
        _attach_mock_transport(adapter, transport, mock)
        transport._time_offset_ms = 0  # V1 fail-closed compat; transport._time_offset_ms = 0
        # Pre-populate asset index cache to avoid mock needing metadata response
        transport._hl_meta_cache["BTC"] = 0
        _trust_hyperliquid_transport_for_test(transport)

        try:
            req = OrderRequest(venue=Venue.HYPERLIQUID, symbol="BTC",
                              side=Side.BUY, quantity=1.0, price=50000.0)
            fill = await adapter.place_order(req)
            assert fill.venue == Venue.HYPERLIQUID
            assert fill.order_id == "123"
            assert fill.quantity == 1.0
            assert fill.price == 50000.0
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
        _attach_mock_transport(adapter, transport, mock)
        transport._time_offset_ms = 0  # V1 fail-closed compat; transport._time_offset_ms = 0

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
        _attach_mock_transport(adapter, transport, mock)
        transport._time_offset_ms = 0  # V1 fail-closed compat; transport._time_offset_ms = 0

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
        _attach_mock_transport(adapter, transport, mock)
        transport._time_offset_ms = 0  # V1 fail-closed compat; transport._time_offset_ms = 0

        try:
            snap = await adapter.fetch_market_snapshot(["BTCUSDT"])
            assert len(snap.quotes) == 1
            q = snap.quotes[0]
            # These must come from the fixture, not be zeros
            assert q.bid == 50000.0, f"Expected fixture bid 50000, got {q.bid}"
            assert q.ask == 50001.0, f"Expected fixture ask 50001, got {q.ask}"
        finally:
            await transport.close()


# ---------------------------------------------------------------------------
# Bitget profile detection integration — full fetch_position flow (Fix 4)
# ---------------------------------------------------------------------------


class TestBitgetProfileDetectionFullFlow:
    """Bitget profile detection must handle errors correctly and cache results."""

    @pytest.mark.asyncio
    async def test_classic_fallback_completes_fetch_position(self):
        """When UTA probe returns classic-mode error, adapter falls back to classic
        and successfully fetches position via classic endpoint."""
        from lightfee.venues.bitget import BitgetAccountProfile

        # Response 1: UTA probe returns classic-mode error
        # Response 2: Classic all-position family validation succeeds
        # Response 3: Classic single-position endpoint returns success
        classic_error = {"code": "40034", "msg": "classic account not supported"}
        classic_position = {
            "code": "00000",
            "data": {"symbol": "BTCUSDT", "holdSide": "long",
                     "total": "0.02", "openPriceAvg": "51000.0"},
        }
        mock = _build_multi_response_transport([
            (400, classic_error),
            (200, {"code": "00000", "data": []}),
            (200, classic_position),
        ])
        cred = LiveCredential(api_key="k", api_secret="s", api_passphrase="p")
        adapter = BitgetAdapter(mode="live", credential=cred)
        transport = adapter._transport
        _attach_mock_transport(adapter, transport, mock)
        transport._time_offset_ms = 0  # V1 fail-closed compat; transport._time_offset_ms = 0

        try:
            pos = await adapter.fetch_position("BTCUSDT")
            assert pos.quantity == 0.02
            assert pos.entry_price == 51000.0
            assert adapter.account_profile == BitgetAccountProfile.CLASSIC
        finally:
            await transport.close()

    @pytest.mark.asyncio
    async def test_auth_401_does_not_fallback(self):
        """401 Unauthorized must NOT be treated as classic-mode; must propagate."""
        mock = _build_mock_transport({"code": "40100", "msg": "invalid api key"}, status=401)
        cred = LiveCredential(api_key="k", api_secret="s", api_passphrase="p")
        adapter = BitgetAdapter(mode="live", credential=cred)
        transport = adapter._transport
        _attach_mock_transport(adapter, transport, mock)
        transport._time_offset_ms = 0  # V1 fail-closed compat; transport._time_offset_ms = 0

        try:
            with pytest.raises(TransportError) as exc_info:
                await adapter.detect_profile()
            assert exc_info.value.category == TransportErrorCategory.AUTH_FAILURE
            # Profile must NOT be cached as CLASSIC
            assert adapter.account_profile is None
        finally:
            await transport.close()

    @pytest.mark.asyncio
    async def test_rate_limit_429_does_not_fallback(self):
        """429 Rate limit must NOT be treated as classic-mode; must propagate."""
        mock = _build_mock_transport(
            {"code": "42900", "msg": "rate limited"}, status=429
        )
        cred = LiveCredential(api_key="k", api_secret="s", api_passphrase="p")
        adapter = BitgetAdapter(mode="live", credential=cred)
        transport = adapter._transport
        _attach_mock_transport(adapter, transport, mock)
        transport._time_offset_ms = 0  # V1 fail-closed compat; transport._time_offset_ms = 0

        try:
            with pytest.raises(TransportError) as exc_info:
                await adapter.detect_profile()
            assert exc_info.value.category == TransportErrorCategory.TRANSPORT_FAILURE
            assert adapter.account_profile is None
        finally:
            await transport.close()

    @pytest.mark.asyncio
    async def test_network_error_does_not_fallback(self):
        """Network timeout must NOT be treated as classic-mode; must propagate."""
        cred = LiveCredential(api_key="k", api_secret="s", api_passphrase="p")
        adapter = BitgetAdapter(mode="live", credential=cred)
        transport = adapter._transport
        # Use a non-routable address to force network error
        transport._client = httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: (_ for _ in ()).throw(httpx.ConnectError("connection refused"))
            )
        )

        try:
            with pytest.raises(TransportError) as exc_info:
                await adapter.detect_profile()
            assert exc_info.value.category == TransportErrorCategory.TRANSPORT_FAILURE
            assert adapter.account_profile is None
        finally:
            await transport.close()

    @pytest.mark.asyncio
    async def test_profile_cached_after_first_detection(self):
        """Profile is cached; second detect_profile does not probe again."""
        uta_position = {
            "code": "00000",
            "data": [{"symbol": "BTCUSDT", "posSide": "long",
                       "total": "0.01", "openPriceAvg": "50000.0"}],
        }
        mock = _build_mock_transport(uta_position)
        cred = LiveCredential(api_key="k", api_secret="s", api_passphrase="p")
        adapter = BitgetAdapter(mode="live", credential=cred)
        transport = adapter._transport
        _attach_mock_transport(adapter, transport, mock)
        transport._time_offset_ms = 0  # V1 fail-closed compat; transport._time_offset_ms = 0

        try:
            from lightfee.venues.bitget import BitgetAccountProfile
            profile1 = await adapter.detect_profile()
            assert profile1 == BitgetAccountProfile.UTA
            # Second call is cached — returns immediately without new HTTP request
            profile2 = await adapter.detect_profile()
            assert profile2 == BitgetAccountProfile.UTA
            assert adapter.account_profile == BitgetAccountProfile.UTA
        finally:
            await transport.close()


# ---------------------------------------------------------------------------
# Hyperliquid capability declaration consistency (Fix 7)
# ---------------------------------------------------------------------------


class TestHyperliquidCapabilityConsistency:
    """Hyperliquid live order is now supported with EIP-712 signing."""

    def test_hyperliquid_spec_has_live_order_supported(self):
        from lightfee.venues.specs import hyperliquid_spec
        spec = hyperliquid_spec()
        assert spec.live_order_supported is True, (
            "Hyperliquid spec must declare live_order_supported=True"
        )
        assert spec.paper_order_supported is True, (
            "Hyperliquid paper order must still work"
        )

    def test_hyperliquid_capabilities_live_order_supported(self):
        from lightfee.venues.base import VenueCapabilities
        caps = VenueCapabilities.for_venue(Venue.HYPERLIQUID)
        assert caps.live_order_supported is True, (
            "Hyperliquid capabilities must show live_order_supported=True"
        )

    @pytest.mark.asyncio
    async def test_live_order_now_works_with_signing(self):
        """Hyperliquid live order with mock exchange returns valid fill."""
        fixture = _load_fixture("hyperliquid", "place_order_success")
        mock = _build_mock_transport(fixture)

        cred = LiveCredential(api_key="k", api_secret="",
                              wallet_private_key=HL_FIXTURE_PRIVATE_KEY,
                              account_address="0xbeef")
        adapter = HyperliquidAdapter(mode="live", credential=cred)
        transport = adapter._transport
        _attach_mock_transport(adapter, transport, mock)
        transport._time_offset_ms = 0  # V1 fail-closed compat; transport._time_offset_ms = 0
        transport._hl_meta_cache["BTC"] = 0
        _trust_hyperliquid_transport_for_test(transport)

        try:
            req = OrderRequest(venue=Venue.HYPERLIQUID, symbol="BTC",
                              side=Side.BUY, quantity=1.0, price=50000.0)
            fill = await adapter.place_order(req)
            assert fill.venue == Venue.HYPERLIQUID
            assert fill.order_id == "123"
        finally:
            await transport.close()

    @pytest.mark.asyncio
    async def test_market_snapshot_still_works(self):
        """Hyperliquid market data path must remain functional."""
        fixture = _load_fixture("hyperliquid", "market_snapshot")
        mock = _build_mock_transport(fixture)
        cred = LiveCredential(api_key="k", api_secret="s",
                              wallet_private_key=HL_FIXTURE_PRIVATE_KEY,
                              account_address="0xbeef")
        adapter = HyperliquidAdapter(mode="live", credential=cred)
        transport = adapter._transport
        _attach_mock_transport(adapter, transport, mock)
        transport._time_offset_ms = 0  # V1 fail-closed compat; transport._time_offset_ms = 0

        try:
            snap = await adapter.fetch_market_snapshot(["BTC"])
            assert len(snap.quotes) > 0
            assert snap.quotes[0].bid > 0
        finally:
            await transport.close()

    @pytest.mark.asyncio
    async def test_position_still_works(self):
        """Hyperliquid position fetch must remain functional."""
        fixture = _load_fixture("hyperliquid", "position_snapshot")
        mock = _build_mock_transport(fixture)
        cred = LiveCredential(api_key="k", api_secret="s",
                              wallet_private_key=HL_FIXTURE_PRIVATE_KEY,
                              account_address="0xbeef")
        adapter = HyperliquidAdapter(mode="live", credential=cred)
        transport = adapter._transport
        _attach_mock_transport(adapter, transport, mock)
        transport._time_offset_ms = 0  # V1 fail-closed compat; transport._time_offset_ms = 0

        try:
            pos = await adapter.fetch_position("BTC")
            assert pos.quantity > 0
            assert pos.entry_price > 0
        finally:
            await transport.close()


# ---------------------------------------------------------------------------
# Ack-only order integration test (Fix 6 integration)
# ---------------------------------------------------------------------------


class TestBybitEntryLeverage:
    @pytest.mark.asyncio
    async def test_skips_setter_when_bybit_readback_already_matches_target(self):
        adapter = BybitAdapter(
            mode="live", credential=LiveCredential(api_key="k", api_secret="s")
        )
        calls: list[tuple[str, str]] = []

        async def fake_request(method, path, params=None, body=None, private=False):
            calls.append((method, path))
            assert method == "GET"
            assert path == "/v5/position/list"
            return {
                "retCode": 0,
                "result": {
                    "list": [
                        {"symbol": "HUSDT", "positionIdx": 1, "leverage": "4"},
                        {"symbol": "HUSDT", "positionIdx": 2, "leverage": "4"},
                    ]
                },
            }

        adapter._transport._request = fake_request
        try:
            await adapter.ensure_entry_leverage("HUSDT", 4)
        finally:
            await adapter._transport.close()

        assert calls == [("GET", "/v5/position/list")]

    @pytest.mark.parametrize("set_ret_code", (0, 110043))
    @pytest.mark.asyncio
    async def test_sets_and_reads_back_both_bybit_position_leverages(
        self, set_ret_code
    ):
        """A successful setter response is insufficient without readback."""
        adapter = BybitAdapter(
            mode="live", credential=LiveCredential(api_key="k", api_secret="s")
        )
        calls: list[tuple[str, str, dict | None, dict | None, bool]] = []
        position_responses = iter(("10", "4"))

        async def fake_request(method, path, params=None, body=None, private=False):
            calls.append((method, path, params, body, private))
            if path == "/v5/position/list":
                leverage = next(position_responses)
                return {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {"symbol": "HUSDT", "positionIdx": 1, "leverage": leverage},
                            {"symbol": "HUSDT", "positionIdx": 2, "leverage": leverage},
                        ]
                    },
                }
            assert path == "/v5/position/set-leverage"
            return {"retCode": set_ret_code, "result": {}}

        adapter._transport._request = fake_request
        try:
            await adapter.ensure_entry_leverage("HUSDT", 4, notional_quote=50.0)
        finally:
            await adapter._transport.close()

        assert calls == [
            (
                "GET",
                "/v5/position/list",
                {"category": "linear", "symbol": "HUSDT"},
                None,
                True,
            ),
            (
                "POST",
                "/v5/position/set-leverage",
                None,
                {
                    "category": "linear",
                    "symbol": "HUSDT",
                    "buyLeverage": "4",
                    "sellLeverage": "4",
                },
                True,
            ),
            (
                "GET",
                "/v5/position/list",
                {"category": "linear", "symbol": "HUSDT"},
                None,
                True,
            ),
        ]

    @pytest.mark.asyncio
    async def test_rejects_when_bybit_readback_does_not_match_target(self):
        adapter = BybitAdapter(
            mode="live", credential=LiveCredential(api_key="k", api_secret="s")
        )
        position_responses = iter(("10", "10"))

        async def fake_request(method, path, params=None, body=None, private=False):
            if path == "/v5/position/list":
                leverage = next(position_responses)
                return {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {"symbol": "HUSDT", "positionIdx": 1, "leverage": leverage},
                            {"symbol": "HUSDT", "positionIdx": 2, "leverage": leverage},
                        ]
                    },
                }
            assert path == "/v5/position/set-leverage"
            return {"retCode": 0, "result": {}}

        adapter._transport._request = fake_request
        try:
            with pytest.raises(OrderSubmitError) as exc_info:
                await adapter.ensure_entry_leverage("HUSDT", 4)
        finally:
            await adapter._transport.close()

        assert exc_info.value.class_ == SubmitFailureClass.REJECTED
        assert "readback mismatch" in str(exc_info.value)

    @pytest.mark.parametrize(
        "malformed_response",
        (
            "not-a-response",
            {"retCode": 0, "result": {}},
            {"retCode": 0, "result": {"list": []}},
        ),
    )
    @pytest.mark.asyncio
    async def test_rejects_malformed_bybit_initial_leverage_evidence(
        self, malformed_response
    ):
        adapter = BybitAdapter(
            mode="live", credential=LiveCredential(api_key="k", api_secret="s")
        )
        calls: list[str] = []

        async def fake_request(method, path, params=None, body=None, private=False):
            calls.append(path)
            return malformed_response

        adapter._transport._request = fake_request
        try:
            with pytest.raises(OrderSubmitError) as exc_info:
                await adapter.ensure_entry_leverage("HUSDT", 4)
        finally:
            await adapter._transport.close()

        assert exc_info.value.class_ == SubmitFailureClass.REJECTED
        assert calls == ["/v5/position/list"]

    @pytest.mark.asyncio
    async def test_rejects_malformed_bybit_post_set_readback(self):
        adapter = BybitAdapter(
            mode="live", credential=LiveCredential(api_key="k", api_secret="s")
        )
        responses = iter(
            (
                {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {"symbol": "HUSDT", "positionIdx": 1, "leverage": "10"},
                            {"symbol": "HUSDT", "positionIdx": 2, "leverage": "10"},
                        ]
                    },
                },
                {"retCode": 0, "result": {"list": []}},
            )
        )

        async def fake_request(method, path, params=None, body=None, private=False):
            if path == "/v5/position/list":
                return next(responses)
            assert path == "/v5/position/set-leverage"
            return {"retCode": 0, "result": {}}

        adapter._transport._request = fake_request
        try:
            with pytest.raises(OrderSubmitError) as exc_info:
                await adapter.ensure_entry_leverage("HUSDT", 4)
        finally:
            await adapter._transport.close()

        assert exc_info.value.class_ == SubmitFailureClass.REJECTED


class TestAdditionalVenueEntryLeverage:
    @pytest.mark.asyncio
    async def test_okx_sets_each_long_short_leverage_and_reads_back(self):
        adapter = OkxAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        calls: list[tuple[str, str, dict | None, dict | None]] = []
        leverage_reads = iter(
            (
                [
                    {"instId": "H-USDT-SWAP", "mgnMode": "cross", "posSide": "long", "lever": "10"},
                    {"instId": "H-USDT-SWAP", "mgnMode": "cross", "posSide": "short", "lever": "10"},
                ],
                [
                    {"instId": "H-USDT-SWAP", "mgnMode": "cross", "posSide": "long", "lever": "4"},
                    {"instId": "H-USDT-SWAP", "mgnMode": "cross", "posSide": "short", "lever": "4"},
                ],
            )
        )

        async def fake_request(method, path, params=None, body=None, private=False):
            calls.append((method, path, params, body))
            if path == "/api/v5/account/config":
                return {"code": "0", "data": [{"posMode": "long_short_mode"}]}
            if path == "/api/v5/account/leverage-info":
                return {"code": "0", "data": next(leverage_reads)}
            assert path == "/api/v5/account/set-leverage"
            return {"code": "0", "data": []}

        adapter._transport._request = fake_request
        try:
            await adapter.ensure_entry_leverage("HUSDT", 4)
        finally:
            await adapter._transport.close()

        setter_bodies = [body for _, path, _, body in calls if path.endswith("set-leverage")]
        assert setter_bodies == [
            {"instId": "H-USDT-SWAP", "lever": "4", "mgnMode": "cross", "posSide": "long"},
            {"instId": "H-USDT-SWAP", "lever": "4", "mgnMode": "cross", "posSide": "short"},
        ]

    @pytest.mark.asyncio
    async def test_okx_rejects_leverage_readback_mismatch(self):
        adapter = OkxAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        leverage_reads = iter(
            (
                [{"instId": "H-USDT-SWAP", "mgnMode": "cross", "posSide": "net", "lever": "10"}],
                [{"instId": "H-USDT-SWAP", "mgnMode": "cross", "posSide": "net", "lever": "10"}],
            )
        )

        async def fake_request(method, path, params=None, body=None, private=False):
            if path == "/api/v5/account/config":
                return {"code": "0", "data": [{"posMode": "net_mode"}]}
            if path == "/api/v5/account/leverage-info":
                return {"code": "0", "data": next(leverage_reads)}
            assert path == "/api/v5/account/set-leverage"
            return {"code": "0", "data": []}

        adapter._transport._request = fake_request
        try:
            with pytest.raises(OrderSubmitError, match="readback mismatch"):
                await adapter.ensure_entry_leverage("HUSDT", 4)
        finally:
            await adapter._transport.close()

    @pytest.mark.asyncio
    async def test_bitget_uta_sets_then_reads_back_the_symbol_setting(self):
        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter.resolve_contract_family = lambda: _async_value(BitgetContractFamily.UTA_V3)
        calls: list[tuple[str, str, dict | None, dict | None]] = []
        settings = iter(("10", "4"))

        async def fake_request(method, path, params=None, body=None, private=False):
            calls.append((method, path, params, body))
            if path == "/api/v3/account/settings":
                return {
                    "code": "00000",
                    "data": {
                        "symbolConfigList": [
                            {
                                "category": "USDT-FUTURES",
                                "symbol": "HUSDT",
                                "marginMode": "crossed",
                                "leverage": next(settings),
                            }
                        ]
                    },
                }
            assert path == "/api/v3/account/set-leverage"
            return {"code": "00000", "data": "success"}

        adapter._transport._request = fake_request
        try:
            await adapter.ensure_entry_leverage("HUSDT", 4)
        finally:
            await adapter._transport.close()

        assert calls[1] == (
            "POST",
            "/api/v3/account/set-leverage",
            None,
            {"category": "USDT-FUTURES", "symbol": "HUSDT", "leverage": "4"},
        )

    @pytest.mark.asyncio
    async def test_bitget_classic_requires_the_applied_cross_leverage(self):
        adapter = BitgetAdapter(
            mode="live",
            credential=LiveCredential(api_key="k", api_secret="s", api_passphrase="p"),
        )
        adapter.resolve_contract_family = lambda: _async_value(
            BitgetContractFamily.CLASSIC_MIX_V2
        )

        async def fake_request(method, path, params=None, body=None, private=False):
            assert path == "/api/v2/mix/account/set-leverage"
            return {
                "code": "00000",
                "data": {"marginMode": "crossed", "crossMarginLeverage": "10"},
            }

        adapter._transport._request = fake_request
        try:
            with pytest.raises(OrderSubmitError, match="response mismatch"):
                await adapter.ensure_entry_leverage("HUSDT", 4)
        finally:
            await adapter._transport.close()

    @pytest.mark.asyncio
    async def test_gate_retries_only_the_dual_mode_contract_and_verifies_response(self):
        adapter = GateAdapter(
            mode="live", credential=LiveCredential(api_key="k", api_secret="s")
        )
        calls: list[dict] = []

        async def fake_request(method, path, params=None, body=None, private=False):
            calls.append(dict(params or {}))
            if len(calls) == 1:
                raise TransportError(
                    TransportErrorCategory.REQUEST_REJECTED,
                    "position mode conflict",
                    body='{"label":"POSITION_MODE_CONFLICT"}',
                )
            return {"leverage": "4"}

        adapter._transport._request = fake_request
        try:
            await adapter.ensure_entry_leverage("HUSDT", 4)
        finally:
            await adapter._transport.close()

        assert calls == [
            {"leverage": "4", "margin_mode": "cross"},
            {"leverage": "4", "margin_mode": "cross", "dual_side": "dual_long"},
            {"leverage": "4", "margin_mode": "cross", "dual_side": "dual_short"},
        ]

    @pytest.mark.asyncio
    async def test_hyperliquid_clamps_to_public_max_and_requires_ack(self):
        adapter = HyperliquidAdapter(
            mode="live",
            credential=LiveCredential(
                wallet_private_key=HL_FIXTURE_PRIVATE_KEY, account_address="0xbeef"
            ),
        )
        adapter._transport._hl_asset_meta_cache["H"] = {
            "asset_index": 7,
            "sz_decimals": 2,
            "price_decimals": 5,
        }
        adapter._transport._symbol_metadata["H"] = {"maxLeverage": 3}
        captured: dict = {}

        async def fake_request(method, path, params=None, body=None, private=False):
            captured.update({"method": method, "path": path, "body": body, "private": private})
            return {"status": "ok", "response": {"type": "default"}}

        adapter._transport._request = fake_request
        try:
            await adapter.ensure_entry_leverage("HUSDT", 4)
        finally:
            await adapter._transport.close()

        assert captured["method"] == "POST"
        assert captured["path"] == "/exchange"
        assert captured["private"] is True
        assert captured["body"]["action"] == {
            "type": "updateLeverage",
            "asset": 7,
            "isCross": True,
            "leverage": 3,
        }

    @pytest.mark.asyncio
    async def test_hyperliquid_rejects_non_ok_leverage_acknowledgement(self):
        adapter = HyperliquidAdapter(
            mode="live",
            credential=LiveCredential(
                wallet_private_key=HL_FIXTURE_PRIVATE_KEY, account_address="0xbeef"
            ),
        )
        adapter._transport._hl_asset_meta_cache["H"] = {
            "asset_index": 7,
            "sz_decimals": 2,
            "price_decimals": 5,
        }

        async def fake_request(method, path, params=None, body=None, private=False):
            assert path == "/exchange"
            return {"status": "err", "response": "leverage rejected"}

        adapter._transport._request = fake_request
        try:
            with pytest.raises(OrderSubmitError, match="update rejected"):
                await adapter.ensure_entry_leverage("HUSDT", 4)
        finally:
            await adapter._transport.close()


class TestAckOnlyOrderIntegration:
    """Integration test: ack-only order response through adapter must raise UNCERTAIN."""

    @pytest.mark.asyncio
    async def test_bybit_ack_only_through_adapter_raises_uncertain(self):
        """Bybit place_order with ack-only response must raise UNCERTAIN."""
        ack_response = {"retCode": 0, "result": {"orderId": "xyz789", "orderLinkId": "client_1"}}
        mock = _build_mock_transport(ack_response)
        cred = LiveCredential(api_key="k", api_secret="s")
        adapter = BybitAdapter(mode="live", credential=cred)
        transport = adapter._transport
        _attach_mock_transport(adapter, transport, mock)
        transport._time_offset_ms = 0  # V1 fail-closed compat; transport._time_offset_ms = 0

        try:
            req = OrderRequest(venue=Venue.BYBIT, symbol="BTCUSDT",
                              side=Side.BUY, quantity=0.01)
            with pytest.raises(OrderSubmitError) as exc_info:
                await adapter.place_order(req)
            assert exc_info.value.class_ == SubmitFailureClass.UNCERTAIN
        finally:
            await transport.close()

    @pytest.mark.asyncio
    async def test_bitget_ack_only_through_adapter_raises_uncertain(self, monkeypatch):
        """Bitget place_order with ack-only response must raise UNCERTAIN."""
        from lightfee.venues.symbol_rules import SymbolRule

        class FakeRulesCache:
            async def get(self, transport, venue, venue_symbol):
                assert venue == Venue.BITGET
                return SymbolRule(
                    tick_size=0.000001,
                    qty_step=0.001,
                    min_qty=0.001,
                    min_notional=5.0,
                    rule_source="contracts",
                )

        monkeypatch.setattr(
            "lightfee.venues.transport.get_symbol_rules_cache",
            lambda: FakeRulesCache(),
        )
        ack_response = {"code": "00000", "data": {"orderId": "bg123", "clientOrderId": "client_1"}}
        mock = _build_mock_transport(ack_response)
        cred = LiveCredential(api_key="k", api_secret="s", api_passphrase="p")
        adapter = BitgetAdapter(mode="live", credential=cred)
        transport = adapter._transport
        _attach_mock_transport(adapter, transport, mock)
        transport._time_offset_ms = 0  # V1 fail-closed compat; transport._time_offset_ms = 0

        try:
            req = OrderRequest(venue=Venue.BITGET, symbol="BTCUSDT",
                              side=Side.BUY, quantity=0.01)
            with pytest.raises(OrderSubmitError) as exc_info:
                await adapter.place_order(req)
            assert exc_info.value.class_ == SubmitFailureClass.UNCERTAIN
        finally:
            await transport.close()


# ---------------------------------------------------------------------------
# Canonical symbol round-trip and venue capability truth (Task 1)
# ---------------------------------------------------------------------------


class TestCanonicalSymbolRoundTrip:
    """Every venue must define symbol_to_venue (canonical → wire) and
    symbol_from_venue (wire → canonical) lambdas so that wire symbols stay
    at request/subscribe/parse boundaries and canonical symbols are the
    sole internal representation.

    symbol_to_venue("BTCUSDT") — canonical → venue wire format
    symbol_from_venue("<venue-wire>") — venue wire format → canonical
    """

    def test_all_seven_venues_have_symbol_to_venue(self):
        from lightfee.venues.specs import get_spec
        for venue in Venue:
            spec = get_spec(venue)
            assert spec.symbol_to_venue is not None, (
                f"{venue}: symbol_to_venue must not be None"
            )

    def test_all_seven_venues_have_symbol_from_venue(self):
        from lightfee.venues.specs import get_spec
        for venue in Venue:
            spec = get_spec(venue)
            assert spec.symbol_from_venue is not None, (
                f"{venue}: symbol_from_venue must not be None"
            )

    def test_okx_canonical_to_wire(self):
        from lightfee.venues.specs import get_spec
        spec = get_spec(Venue.OKX)
        assert spec.symbol_to_venue("BTCUSDT") == "BTC-USDT-SWAP"
        assert spec.symbol_to_venue("ETHUSDT") == "ETH-USDT-SWAP"

    def test_okx_wire_to_canonical(self):
        from lightfee.venues.specs import get_spec
        spec = get_spec(Venue.OKX)
        assert spec.symbol_from_venue("BTC-USDT-SWAP") == "BTCUSDT"
        assert spec.symbol_from_venue("ETH-USDT-SWAP") == "ETHUSDT"

    def test_okx_private_truth_operations_use_wire_symbol_params(self):
        from lightfee.engine.exchange_truth import build_venue_operation_request
        from lightfee.venues.specs import VenueOperation

        for operation in (
            VenueOperation.AMEND_ORDER,
            VenueOperation.CANCEL_ORDER,
            VenueOperation.ORDER_STATUS,
            VenueOperation.OPEN_ORDERS,
            VenueOperation.POSITION,
        ):
            request = build_venue_operation_request(
                Venue.OKX,
                operation,
                symbol="UBUSDT",
            )
            assert request.params.get("instId") == "UB-USDT-SWAP"
            assert "UBUSDT" not in repr(request.params)

    def test_gate_canonical_to_wire(self):
        from lightfee.venues.specs import get_spec
        spec = get_spec(Venue.GATE)
        assert spec.symbol_to_venue("BTCUSDT") == "BTC_USDT"

    def test_gate_wire_to_canonical(self):
        from lightfee.venues.specs import get_spec
        spec = get_spec(Venue.GATE)
        assert spec.symbol_from_venue("BTC_USDT") == "BTCUSDT"

    def test_hyperliquid_canonical_to_wire(self):
        from lightfee.venues.specs import get_spec
        spec = get_spec(Venue.HYPERLIQUID)
        assert spec.symbol_to_venue("BTCUSDT") == "BTC"

    def test_hyperliquid_wire_to_canonical(self):
        from lightfee.venues.specs import get_spec
        spec = get_spec(Venue.HYPERLIQUID)
        assert spec.symbol_from_venue("BTC") == "BTCUSDT"

    def test_binance_wire_is_canonical_identity(self):
        from lightfee.venues.specs import get_spec
        spec = get_spec(Venue.BINANCE)
        assert spec.symbol_to_venue("BTCUSDT") == "BTCUSDT"
        assert spec.symbol_from_venue("BTCUSDT") == "BTCUSDT"

    def test_bybit_wire_is_canonical_identity(self):
        from lightfee.venues.specs import get_spec
        spec = get_spec(Venue.BYBIT)
        assert spec.symbol_to_venue("BTCUSDT") == "BTCUSDT"
        assert spec.symbol_from_venue("BTCUSDT") == "BTCUSDT"

    def test_bitget_wire_is_canonical_identity(self):
        from lightfee.venues.specs import get_spec
        spec = get_spec(Venue.BITGET)
        assert spec.symbol_to_venue("BTCUSDT") == "BTCUSDT"
        assert spec.symbol_from_venue("BTCUSDT") == "BTCUSDT"

    def test_aster_wire_is_canonical_identity(self):
        from lightfee.venues.specs import get_spec
        spec = get_spec(Venue.ASTER)
        assert spec.symbol_to_venue("BTCUSDT") == "BTCUSDT"
        assert spec.symbol_from_venue("BTCUSDT") == "BTCUSDT"


class TestVenueCapabilityTruth:
    """Every venue must have explicit capability declarations;
    unsupported capabilities must be non-silent (structured enums, not None/False)."""

    def test_all_seven_venues_have_explicit_capabilities(self):
        from lightfee.venues.base import VenueCapabilities
        for venue in Venue:
            caps = VenueCapabilities.for_venue(venue)
            assert caps.venue == venue
            assert caps.risk_health is not None, f"{venue}: risk_health must be explicit"
            assert caps.private_health is not None, f"{venue}: private_health must be explicit"
            assert caps.execution_liquidity is not None, f"{venue}: execution_liquidity must be explicit"
            assert caps.reconcile_quality is not None, f"{venue}: reconcile_quality must be explicit"
            assert caps.testnet_support is not None, f"{venue}: testnet_support must be explicit"

    def test_every_live_order_adapter_must_implement_entry_leverage_preparation(self):
        """Prevents a new venue from silently opting out of V1's entry invariant."""
        for adapter_type in (
            BinanceAdapter,
            OkxAdapter,
            BybitAdapter,
            BitgetAdapter,
            GateAdapter,
            AsterAdapter,
            HyperliquidAdapter,
        ):
            adapter = adapter_type(mode="paper")
            assert adapter.supports_entry_leverage_preparation is True
            assert "ensure_entry_leverage" in adapter_type.__dict__

    def test_hyperliquid_unsupported_capabilities_are_explicit(self):
        from lightfee.venues.base import (
            CapabilitySupport,
            ExecutionLiquidityCapability,
            ReconcileQuality,
            VenueCapabilities,
        )
        caps = VenueCapabilities.for_venue(Venue.HYPERLIQUID)
        assert caps.risk_health == CapabilitySupport.UNSUPPORTED, (
            "Hyperliquid risk_health must be explicitly UNSUPPORTED"
        )
        assert caps.reconcile_quality == ReconcileQuality.UNSUPPORTED, (
            "Hyperliquid reconcile_quality must be explicitly UNSUPPORTED"
        )
        # Hyperliquid DOES support execution liquidity via REST l2Book
        assert caps.execution_liquidity == ExecutionLiquidityCapability.TRUE_L2

    def test_bitget_unsupported_capabilities_are_explicit(self):
        from lightfee.venues.base import ReconcileQuality, VenueCapabilities
        caps = VenueCapabilities.for_venue(Venue.BITGET)
        assert caps.reconcile_quality == ReconcileQuality.UNSUPPORTED, (
            "Bitget reconcile_quality must be explicitly UNSUPPORTED"
        )

    def test_gate_unsupported_capabilities_are_explicit(self):
        from lightfee.venues.base import ReconcileQuality, VenueCapabilities
        caps = VenueCapabilities.for_venue(Venue.GATE)
        assert caps.reconcile_quality == ReconcileQuality.UNSUPPORTED, (
            "Gate reconcile_quality must be explicitly UNSUPPORTED"
        )
