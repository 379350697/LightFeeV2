"""V1 venue private WS parser fixture tests — one per venue, matching V1 semantics."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import patch

import pytest

from lightfee.core.domain import PassiveOrderState
from lightfee.marketdata.private_ws import PrivateWsState
from lightfee.venues.binance_private_ws import handle_binance_private_message
from lightfee.venues.okx_private_ws import (
    _build_okx_ct_val_map,
    handle_okx_private_message,
)
from lightfee.venues.bybit_private_ws import handle_bybit_private_message
from lightfee.venues.bitget_private_ws import handle_bitget_private_message
from lightfee.venues.gate_private_ws import handle_gate_private_message
from lightfee.venues.aster_private_ws import handle_aster_private_message
from lightfee.venues.hyperliquid_private_ws import _apply_hyperliquid_private_message


async def _asyncio_sleep_short():
    await asyncio.sleep(0.02)


# ---------------------------------------------------------------------------
# Binance parser fixtures
# ---------------------------------------------------------------------------


class TestBinancePrivateParser:
    @pytest.mark.asyncio
    async def test_execution_report_new_order(self):
        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({
            "e": "executionReport",
            "s": "ETHUSDT",
            "i": "order-123",
            "c": "client-1",
            "X": "NEW",
            "z": "0", "ap": "0", "n": "0",
            "T": 1700000000000,
        })
        handle_binance_private_message(state, symbol_map, raw)
        await asyncio_sleep_short()
        update = state.order_by_order_id("order-123")
        assert update is not None
        assert update.state == PassiveOrderState.OPEN

    @pytest.mark.asyncio
    async def test_execution_report_full_fill(self):
        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({
            "e": "executionReport",
            "s": "ETHUSDT",
            "i": "order-456",
            "c": "client-2",
            "X": "FILLED",
            "z": "0.05", "ap": "2140.50", "n": "0.001", "N": "USDT",
            "T": 1700000001000,
        })
        handle_binance_private_message(state, symbol_map, raw)
        await asyncio_sleep_short()
        update = state.order_by_order_id("order-456")
        assert update is not None
        assert update.state == PassiveOrderState.FILLED
        assert update.filled_quantity == 0.05
        assert update.average_price == 2140.50
        assert update.fee_quote == 0.001

    @pytest.mark.asyncio
    async def test_execution_report_canceled(self):
        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({
            "e": "executionReport", "s": "ETHUSDT",
            "i": "order-789", "c": "client-3",
            "X": "CANCELED", "z": "0", "ap": "0",
            "T": 1700000002000,
        })
        handle_binance_private_message(state, symbol_map, raw)
        await asyncio_sleep_short()
        update = state.order_by_order_id("order-789")
        assert update is not None
        assert update.state == PassiveOrderState.CANCELED

    @pytest.mark.asyncio
    async def test_unknown_event_ignored(self):
        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({"e": "unknown", "data": {}})
        handle_binance_private_message(state, symbol_map, raw)
        await asyncio_sleep_short()
        assert state.worker_count() == 0  # no crash


# ---------------------------------------------------------------------------
# OKX parser fixtures
# ---------------------------------------------------------------------------


class TestOkxPrivateParser:
    @pytest.mark.asyncio
    async def test_login_ack_triggers_subscribe(self):
        state = PrivateWsState()
        symbol_map = {"ETH-USDT-SWAP": "ETHUSDT"}
        sub_msgs = ['{"op":"subscribe","args":[{"channel":"orders","instType":"SWAP","instId":"ETH-USDT-SWAP"}]}']
        raw = json.dumps({"event": "login", "code": "0"})
        result, subscribed = handle_okx_private_message(state, symbol_map, sub_msgs, raw, False)
        assert result == sub_msgs
        assert subscribed is True

    @pytest.mark.asyncio
    async def test_order_update_partial_fill(self):
        state = PrivateWsState()
        symbol_map = {"ETH-USDT-SWAP": "ETHUSDT"}
        sub_msgs = []
        raw = json.dumps({
            "arg": {"channel": "orders", "instType": "SWAP", "instId": "ETH-USDT-SWAP"},
            "data": [{
                "instId": "ETH-USDT-SWAP",
                "ordId": "okx-order-1",
                "clOrdId": "okx-client-1",
                "accFillSz": "0.5",
                "avgPx": "2140.00",
                "state": "PARTIALLY_FILLED",
                "fillFeeCcy": "USDT",
                "fillFee": "0.002",
                "uTime": "1700000000000",
            }],
        })
        handle_okx_private_message(
            state,
            symbol_map,
            sub_msgs,
            raw,
            subscribed=True,
            ct_val_map={"ETH-USDT-SWAP": 1.0},
        )
        await asyncio_sleep_short()
        update = state.order_by_order_id("okx-order-1")
        assert update is not None
        assert update.filled_quantity == 0.5
        assert update.average_price == 2140.0
        assert update.fee_quote == 0.002
        assert update.state == PassiveOrderState.PARTIALLY_FILLED

    @pytest.mark.asyncio
    async def test_position_update(self):
        state = PrivateWsState()
        symbol_map = {"ETH-USDT-SWAP": "ETHUSDT"}
        sub_msgs = []
        raw = json.dumps({
            "arg": {"channel": "positions", "instType": "SWAP", "instId": "ETH-USDT-SWAP"},
            "data": [{
                "instId": "ETH-USDT-SWAP",
                "pos": "2.5", "posSide": "long",
                "uTime": "1700000000000",
            }],
        })
        handle_okx_private_message(
            state,
            symbol_map,
            sub_msgs,
            raw,
            subscribed=True,
            ct_val_map={"ETH-USDT-SWAP": 1.0},
        )
        await asyncio_sleep_short()
        pos = state.position("ETHUSDT")
        assert pos is not None
        assert pos.size == 2.5

    @pytest.mark.asyncio
    async def test_position_update_net_short_uses_ct_val_contract_size(self):
        state = PrivateWsState()
        symbol_map = {"UB-USDT-SWAP": "UBUSDT"}
        raw = json.dumps({
            "arg": {"channel": "positions", "instType": "SWAP", "instId": "UB-USDT-SWAP"},
            "data": [{
                "instId": "UB-USDT-SWAP",
                "pos": "-1",
                "posSide": "net",
                "uTime": "1700000000000",
            }],
        })

        handle_okx_private_message(
            state,
            symbol_map,
            [],
            raw,
            subscribed=True,
            ct_val_map={"UB-USDT-SWAP": 100.0},
        )

        await asyncio_sleep_short()
        pos = state.position("UBUSDT")
        assert pos is not None
        assert pos.size == pytest.approx(-100.0)

    @pytest.mark.asyncio
    async def test_position_update_without_ct_val_does_not_cache_contracts_as_base(self):
        state = PrivateWsState()
        symbol_map = {"UB-USDT-SWAP": "UBUSDT"}
        raw = json.dumps({
            "arg": {"channel": "positions", "instType": "SWAP", "instId": "UB-USDT-SWAP"},
            "data": [{
                "instId": "UB-USDT-SWAP",
                "pos": "-1",
                "posSide": "net",
                "uTime": "1700000000000",
            }],
        })

        handle_okx_private_message(
            state,
            symbol_map,
            [],
            raw,
            subscribed=True,
            ct_val_map={},
        )

        await asyncio_sleep_short()
        assert state.position("UBUSDT") is None

    def test_build_ct_val_map_does_not_default_known_swap_to_one(self):
        class Transport:
            _symbol_metadata = {}

        ct_val_map = _build_okx_ct_val_map(
            Transport(),
            {"UB-USDT-SWAP": "UBUSDT"},
        )

        assert ct_val_map == {}


# ---------------------------------------------------------------------------
# Bybit parser fixtures
# ---------------------------------------------------------------------------


class TestBybitPrivateParser:
    @pytest.mark.asyncio
    async def test_auth_ack_triggers_subscribe(self):
        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({"op": "auth", "success": True})
        to_send, subscribed = handle_bybit_private_message(state, symbol_map, raw, False)
        assert to_send is not None
        assert subscribed is True

    @pytest.mark.asyncio
    async def test_order_update(self):
        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({
            "topic": "order",
            "data": [{
                "symbol": "ETHUSDT", "orderId": "bybit-1",
                "orderLinkId": "bybit-client-1",
                "cumExecQty": "0.03", "avgPrice": "2140.00",
                "cumExecFee": "0.001", "orderStatus": "PARTIALLYFILLED",
                "updatedTime": "1700000000000",
            }],
        })
        handle_bybit_private_message(state, symbol_map, raw, subscribed=True)
        await asyncio_sleep_short()
        update = state.order_by_order_id("bybit-1")
        assert update is not None
        assert update.filled_quantity == 0.03


# ---------------------------------------------------------------------------
# Bitget parser fixtures
# ---------------------------------------------------------------------------


class TestBitgetPrivateParser:
    @pytest.mark.asyncio
    async def test_login_ack_subscribe(self):
        state = PrivateWsState()
        symbol_map = {"ETHUSDT_UMCBL": "ETHUSDT"}
        raw = json.dumps({"event": "login", "code": "0"})
        to_send, subscribed = handle_bitget_private_message(state, symbol_map, raw, False)
        assert to_send is not None
        assert subscribed is True

    @pytest.mark.asyncio
    async def test_order_update(self):
        state = PrivateWsState()
        symbol_map = {"ETHUSDT_UMCBL": "ETHUSDT"}
        raw = json.dumps({
            "arg": {"channel": "orders", "instId": "ETHUSDT_UMCBL"},
            "data": [{
                "instId": "ETHUSDT_UMCBL", "orderId": "bg-1",
                "clientOid": "bg-client-1",
                "accBaseVolume": "0.02", "avgPrice": "2140.00",
                "status": "PARTIALLY_FILLED",
                "uTime": "1700000000000",
            }],
        })
        handle_bitget_private_message(state, symbol_map, raw, subscribed=True)
        await asyncio_sleep_short()
        update = state.order_by_order_id("bg-1")
        assert update is not None
        assert update.filled_quantity == 0.02


# ---------------------------------------------------------------------------
# Gate parser fixtures
# ---------------------------------------------------------------------------


class TestGatePrivateParser:
    @pytest.mark.asyncio
    async def test_order_update(self):
        state = PrivateWsState()
        symbol_map = {"ETH_USDT": "ETHUSDT"}
        raw = json.dumps({
            "channel": "futures.orders",
            "event": "update",
            "result": [{
                "contract": "ETH_USDT", "id": "gate-1",
                "text": "gate-client-1",
                "fill_total": "0.01", "fill_price": "2140.00",
                "fee": "0.001", "finish_as": "PARTIAL",
                "finish_time_ms": 1700000000000,
            }],
        })
        handle_gate_private_message(state, symbol_map, raw)
        await asyncio_sleep_short()
        update = state.order_by_order_id("gate-1")
        assert update is not None
        assert update.filled_quantity == 0.01

    @pytest.mark.asyncio
    async def test_order_update_converts_contract_fill_total_to_base_quantity(self):
        state = PrivateWsState()
        symbol_map = {"SIREN_USDT": "SIRENUSDT"}
        contract_multiplier_map = {"SIREN_USDT": 100.0}
        raw = json.dumps({
            "channel": "futures.orders",
            "event": "update",
            "result": [{
                "contract": "SIREN_USDT", "id": "gate-siren-1",
                "text": "gate-siren-client-1",
                "fill_total": "4", "fill_price": "0.03291",
                "fee": "0.001", "finish_as": "PARTIAL",
                "finish_time_ms": 1782442539834,
            }],
        })

        handle_gate_private_message(
            state,
            symbol_map,
            raw,
            contract_multiplier_map=contract_multiplier_map,
        )
        await asyncio_sleep_short()

        update = state.order_by_order_id("gate-siren-1")
        assert update is not None
        assert update.filled_quantity == pytest.approx(400.0)


# ---------------------------------------------------------------------------
# Aster parser fixtures
# ---------------------------------------------------------------------------


class TestAsterPrivateParser:
    @pytest.mark.asyncio
    async def test_execution_report(self):
        state = PrivateWsState()
        symbol_map = {"ETHUSDT": "ETHUSDT"}
        raw = json.dumps({
            "e": "executionReport", "s": "ETHUSDT",
            "i": "aster-1", "c": "aster-client-1",
            "X": "FILLED", "z": "0.05", "ap": "2140.50",
            "n": "0.001", "T": 1700000000000,
        })
        handle_aster_private_message(state, symbol_map, raw)
        await asyncio_sleep_short()
        update = state.order_by_order_id("aster-1")
        assert update is not None
        assert update.state == PassiveOrderState.FILLED


# ---------------------------------------------------------------------------
# Hyperliquid parser fixtures
# ---------------------------------------------------------------------------


class TestHyperliquidPrivateParser:
    @pytest.mark.asyncio
    async def test_user_fill_event(self):
        state = PrivateWsState()
        symbol_map = {"ETH": "ETHUSDT"}
        raw = json.dumps({
            "channel": "user",
            "data": {
                "fills": [{
                    "coin": "ETH", "oid": 12345, "cloid": "hl-client-1",
                    "filledSz": "0.05", "px": "2140.00",
                    "fee": "0.001", "time": 1700000000000,
                }],
            },
        })
        _apply_hyperliquid_private_message(state, symbol_map, raw)
        await asyncio_sleep_short()
        update = state.order_by_order_id("12345")
        assert update is not None
        assert update.filled_quantity == 0.05

    @pytest.mark.asyncio
    async def test_order_update_event(self):
        state = PrivateWsState()
        symbol_map = {"ETH": "ETHUSDT"}
        raw = json.dumps({
            "channel": "orderUpdates",
            "data": [{
                "order": {
                    "coin": "ETH", "oid": 67890, "cloid": "hl-client-2",
                    "filledSz": "0.0", "limitPx": "2150.00",
                    "timestamp": 1700000000000,
                },
                "status": "open",
            }],
        })
        _apply_hyperliquid_private_message(state, symbol_map, raw)
        await asyncio_sleep_short()
        update = state.order_by_order_id("67890")
        assert update is not None
        assert update.state == PassiveOrderState.OPEN


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


async def asyncio_sleep_short():
    import asyncio
    await asyncio.sleep(0.01)


# ============================================================================
# M-R8: Merge passive progress sources (V1 merge_passive_progress_sources)
# ============================================================================


class TestMergePassiveProgressSources:
    """V1 merge_passive_progress_sources: reconciliation > REST detail > private WS."""

    def test_merge_prefers_reconciliation_over_detail_and_private(self):
        """Reconciliation has highest priority for all fields."""
        from lightfee.marketdata.private_ws import (
            CumulativeOrderProgress,
            merge_passive_progress_sources,
        )

        detail = CumulativeOrderProgress.from_position_snapshot(
            "detail-order", "detail-client", 0.012, 2141.0, None, 25,
        )
        private = CumulativeOrderProgress.from_position_snapshot(
            "private-order", "private-client", 0.011, 2140.0, 0.001, 20,
        )

        class FakeRecon:
            order_id = "reconciled-order"
            client_order_id = "reconciled-client"
            quantity = 0.013
            average_price = 2142.0
            fee_quote = 0.002
            filled_at_ms = 30

        merged = merge_passive_progress_sources(detail, FakeRecon(), private)

        assert merged.order_id == "reconciled-order"
        assert merged.client_order_id == "reconciled-client"
        assert merged.cumulative_quantity == 0.013
        assert merged.average_price == 2142.0
        assert merged.fee_quote == 0.002
        assert merged.last_fill_at_ms == 30

    def test_merge_detail_without_recon_uses_detail(self):
        """Without reconciliation, REST detail fields win."""
        from lightfee.marketdata.private_ws import (
            CumulativeOrderProgress,
            merge_passive_progress_sources,
        )

        detail = CumulativeOrderProgress.from_position_snapshot(
            "detail-order", "detail-client", 0.012, 2141.0, None, 25,
        )
        private = CumulativeOrderProgress.from_position_snapshot(
            "private-order", "private-client", 0.005, 2130.0, 0.001, 10,
        )

        merged = merge_passive_progress_sources(detail, None, private)

        assert merged.order_id == "detail-order"
        assert merged.cumulative_quantity == 0.012
        assert merged.average_price == 2141.0

    def test_merge_private_wins_when_higher_quantity(self):
        """When private has higher fill qty, its fields take priority."""
        from lightfee.marketdata.private_ws import (
            CumulativeOrderProgress,
            merge_passive_progress_sources,
        )

        detail = CumulativeOrderProgress.from_position_snapshot(
            "detail-order", "detail-client", 0.005, 2140.0, None, 10,
        )
        private = CumulativeOrderProgress.from_position_snapshot(
            "private-order", "private-client", 0.020, 2150.0, 0.003, 30,
        )

        merged = merge_passive_progress_sources(detail, None, private)

        assert merged.cumulative_quantity == 0.020
        assert merged.average_price == 2150.0
        assert merged.order_id == "private-order"

    def test_merge_zero_fill_all_sources_returns_detail(self):
        """All sources zero fill → returns detail_progress."""
        from lightfee.marketdata.private_ws import (
            CumulativeOrderProgress,
            merge_passive_progress_sources,
        )

        detail = CumulativeOrderProgress()
        merged = merge_passive_progress_sources(detail, None, None)

        assert merged.cumulative_quantity == 0.0
        assert merged.order_id is None

    def test_should_fetch_reconciliation_with_detail_fill(self):
        """Reconciliation needed when REST detail has fill quantity."""
        from lightfee.marketdata.private_ws import (
            CumulativeOrderProgress,
            should_fetch_passive_reconciliation,
        )

        detail = CumulativeOrderProgress.from_position_snapshot(
            "oid", "cid", 0.01, None, None, None,
        )
        assert should_fetch_passive_reconciliation(detail, None) is True

    def test_should_fetch_reconciliation_with_order_identity(self):
        """Reconciliation needed when order_id or client_order_id exists."""
        from lightfee.marketdata.private_ws import (
            CumulativeOrderProgress,
            should_fetch_passive_reconciliation,
        )

        detail = CumulativeOrderProgress.from_position_snapshot(
            "oid", None, 0.0, None, None, None,
        )
        assert should_fetch_passive_reconciliation(detail, None) is True

        detail_no_id = CumulativeOrderProgress()
        assert should_fetch_passive_reconciliation(detail_no_id, None) is False

    def test_should_fetch_reconciliation_with_private_fill(self):
        """Reconciliation needed when private has fill, even if detail is empty."""
        from lightfee.marketdata.private_ws import (
            CumulativeOrderProgress,
            should_fetch_passive_reconciliation,
        )

        detail = CumulativeOrderProgress()
        private = CumulativeOrderProgress.from_position_snapshot(
            "poid", "pcid", 0.005, None, None, None,
        )
        assert should_fetch_passive_reconciliation(detail, private) is True

    def test_resolve_cumulative_order_progress_empty(self):
        """Empty sources → None."""
        from lightfee.marketdata.private_ws import resolve_cumulative_order_progress

        assert resolve_cumulative_order_progress([]) is None

    def test_resolve_cumulative_order_progress_single_source(self):
        """Single source → returns itself."""
        from lightfee.marketdata.private_ws import (
            CumulativeOrderProgress,
            resolve_cumulative_order_progress,
        )

        src = CumulativeOrderProgress.from_position_snapshot(
            "solo", "solo-cid", 0.05, 50000.0, 1.0, 100,
        )
        result = resolve_cumulative_order_progress([src])
        assert result is not None
        assert result.cumulative_quantity == 0.05
        assert result.order_id == "solo"


# ============================================================================
# M-R8: Bitget passive progress real runtime tests (NOT source inspection)
# ============================================================================


class TestBitgetPassiveProgressEndpoint:
    """M-R8: Bitget passive progress runtime verification.

    These tests use fake _request monkeypatched on real VenueTransport instances
    to prove the actual production call paths, not inspect.getsource() strings.
    """

    @pytest.mark.asyncio
    async def test_fetch_bitget_order_detail_hits_uta_not_place_order(self, monkeypatch):
        """Real runtime: _fetch_bitget_order_detail must call /api/v3/trade/order-info
        (UTA), NOT spec.order_path (/api/v2/mix/order/place-order)."""
        from lightfee.venues.transport import VenueTransport, TransportErrorCategory
        from lightfee.venues.specs import bitget_spec

        spec = bitget_spec()
        calls = []

        async def _fake_request(method, path, **kwargs):
            calls.append((method, path, kwargs.get("params")))
            if "/api/v3/trade/order-info" in path:
                return {
                    "code": "00000",
                    "data": {
                        "orderId": "o123",
                        "clientOid": "c456",
                        "baseVolume": "0.5",
                        "priceAvg": "65000.0",
                        "fee": "-1.2",
                        "uTime": "1700000001000",
                        "side": "buy",
                        "status": "PARTIALLY_FILLED",
                    },
                }
            return {"code": "00000", "data": {}}

        with patch.object(VenueTransport, '_validate_live_credentials', return_value=None):
            transport = VenueTransport(spec, mode='mock')
        monkeypatch.setattr(transport, '_request', _fake_request)
        monkeypatch.setattr(transport, 'mode', 'mock')

        result = await transport._fetch_bitget_order_detail(
            "ETHUSDT", "o123", "c456",
        )

        # Must NOT hit place-order endpoint
        place_order_paths = [
            c for c in calls if "place-order" in str(c)
        ]
        assert len(place_order_paths) == 0, (
            f"_fetch_bitget_order_detail MUST NOT use place-order endpoint, "
            f"but calls included: {place_order_paths}"
        )
        # Must hit UTA endpoint
        uta_calls = [c for c in calls if "/api/v3/trade/order-info" in c[1]]
        assert len(uta_calls) >= 1, (
            f"Must call UTA /api/v3/trade/order-info, got calls: {calls}"
        )
        # Must NOT reference spec.order_path in actual calls
        assert result is not None
        assert result["data"]["orderId"] == "o123"
        assert result["data"]["baseVolume"] == "0.5"

    @pytest.mark.asyncio
    async def test_fetch_bitget_order_detail_no_import_error(self, monkeypatch):
        """Regression: _fetch_bitget_order_detail must not raise ImportError.
        TransportErrorCategory is defined in transport.py, not core.errors.
        This test directly exercises the runtime path (no source inspection)."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bitget_spec

        spec = bitget_spec()

        async def _fake_request(method, path, **kwargs):
            return {
                "code": "00000",
                "data": {"orderId": "o123", "baseVolume": "0.1", "priceAvg": "65000"},
            }

        with patch.object(VenueTransport, '_validate_live_credentials', return_value=None):
            transport = VenueTransport(spec, mode='mock')
        monkeypatch.setattr(transport, '_request', _fake_request)
        monkeypatch.setattr(transport, 'mode', 'mock')

        result = await transport._fetch_bitget_order_detail(
            "ETHUSDT", "o123", None,
        )
        assert result is not None, (
            "_fetch_bitget_order_detail must succeed without ImportError"
        )

    @pytest.mark.asyncio
    async def test_query_passive_order_progress_bitget_uta_happy_path(self, monkeypatch):
        """query_passive_order_progress for Bitget: calls UTA, parses, merges,
        returns correct PassiveOrderProgress with cumulative_quantity/avg/fee."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bitget_spec
        from lightfee.core.domain import Side, PassiveOrderProgress

        spec = bitget_spec()
        calls = []

        async def _fake_request(method, path, **kwargs):
            calls.append((method, path, kwargs.get("params")))
            if "/api/v3/trade/order-info" in path:
                return {
                    "code": "00000",
                    "data": {
                        "orderId": "o-uta-1",
                        "clientOid": "c-uta-1",
                        "baseVolume": "0.075",
                        "priceAvg": "50000.0",
                        "fee": "1.5",
                        "uTime": "1700000002000",
                        "side": "sell",
                        "status": "PARTIALLY_FILLED",
                    },
                }
            return {"code": "00000", "data": {}}

        with patch.object(VenueTransport, '_validate_live_credentials', return_value=None):
            transport = VenueTransport(spec, mode='mock')
        monkeypatch.setattr(transport, '_request', _fake_request)
        monkeypatch.setattr(transport, 'mode', 'mock')
        monkeypatch.setattr(transport, 'private_order_progress', lambda **kw: None)

        result = await transport.query_passive_order_progress(
            "ETHUSDT", "o-uta-1", "c-uta-1", side=Side.SELL,
        )

        assert result is not None, "query_passive_order_progress must return a result"
        assert isinstance(result, PassiveOrderProgress)
        assert result.cumulative_quantity == 0.075
        assert result.average_price == 50000.0
        assert result.fee_quote == 1.5
        assert result.order_id == "o-uta-1"
        assert result.client_order_id == "c-uta-1"
        assert result.last_fill_time_ms == 1700000002000
        assert result.evidence["progress_source"] == "bitget_rest_private_reconciliation_merge"
        assert result.evidence["detail_present"] is True
        assert result.evidence["private_progress_present"] is False
        assert result.evidence["reconciliation_present"] is True
        assert result.evidence["detail_status"] == "partially_filled"
        assert result.evidence["detail_cumulative_quantity"] == pytest.approx(0.075)
        assert result.evidence["reconciliation_quantity"] == pytest.approx(0.075)
        assert result.evidence["merged_cumulative_quantity"] == pytest.approx(0.075)
        assert result.evidence["state"] == "partially_filled"
        # Must NOT use place-order
        place_order_calls = [c for c in calls if "place-order" in str(c)]
        assert len(place_order_calls) == 0

    @pytest.mark.asyncio
    async def test_classic_family_lock_uses_classic_order_detail_only(self, monkeypatch):
        """Resolved classic family uses /api/v2/mix/order/detail without UTA probing."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import BitgetContractFamily, bitget_spec
        from lightfee.core.domain import Side, PassiveOrderProgress

        spec = bitget_spec()
        calls = []
        classic_calls_count = 0

        async def _fake_request(method, path, **kwargs):
            nonlocal classic_calls_count
            calls.append((method, path, kwargs.get("params")))
            if "/api/v3/trade/order-info" in path:
                raise AssertionError(f"must not probe UTA after classic family lock: {calls}")
            if "/api/v2/mix/order/detail" in path:
                classic_calls_count += 1
                return {
                    "code": "00000",
                    "data": {
                        "orderId": "o-classic-1",
                        "clientOid": "c-classic-1",
                        "baseVolume": "0.050",
                        "priceAvg": "48000.0",
                        "fee": "0.8",
                        "uTime": "1700000003000",
                        "side": "buy",
                        "status": "FILLED",
                    },
                }
            return {"code": "00000", "data": {}}

        async def _resolve_classic_family():
            return BitgetContractFamily.CLASSIC_MIX_V2

        with patch.object(VenueTransport, '_validate_live_credentials', return_value=None):
            transport = VenueTransport(spec, mode='mock')
        monkeypatch.setattr(transport, '_request', _fake_request)
        monkeypatch.setattr(transport, 'mode', 'mock')
        monkeypatch.setattr(transport, 'private_order_progress', lambda **kw: None)
        monkeypatch.setattr(
            transport,
            '_bitget_resolve_contract_family',
            _resolve_classic_family,
            raising=False,
        )

        result = await transport.query_passive_order_progress(
            "ETHUSDT", "o-classic-1", "c-classic-1", side=Side.BUY,
        )

        assert classic_calls_count >= 1
        assert all("/api/v3/" not in path for _method, path, _params in calls)
        classic_params = [c[2] for c in calls if "/api/v2/mix/order/detail" in c[1]]
        assert any(p.get("productType") == "USDT-FUTURES" for p in classic_params if p)
        assert any(p.get("marginCoin") == "USDT" for p in classic_params if p)
        assert any(p.get("symbol") == "ETHUSDT" for p in classic_params if p)
        # Must NOT use place-order
        place_order_calls = [c for c in calls if "place-order" in str(c)]
        assert len(place_order_calls) == 0

        assert result is not None
        assert result.cumulative_quantity == 0.050
        assert result.average_price == 48000.0

    @pytest.mark.asyncio
    async def test_reconciliation_participates_in_merge_with_highest_qty(self, monkeypatch):
        """Reconciliation call count, endpoint, and merge: when detail/private/
        recon have different quantities, the highest-quantity source wins merge."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bitget_spec
        from lightfee.core.domain import Side
        from lightfee.marketdata.private_ws import (
            CumulativeOrderProgress,
            PrivateOrderUpdate,
        )

        spec = bitget_spec()
        call_count = 0

        async def _fake_request(method, path, **kwargs):
            nonlocal call_count
            call_count += 1
            if "/api/v3/trade/order-info" in path:
                return {
                    "code": "00000",
                    "data": {
                        "orderId": "o-recon-1",
                        "clientOid": "c-recon-1",
                        "baseVolume": "0.010",  # detail: small fill
                        "priceAvg": "50000.0",
                        "fee": "0.5",
                        "uTime": "1700000004000",
                        "side": "buy",
                        "status": "PARTIALLY_FILLED",
                    },
                }
            return {"code": "00000", "data": {}}

        with patch.object(VenueTransport, '_validate_live_credentials', return_value=None):
            transport = VenueTransport(spec, mode='mock')
        monkeypatch.setattr(transport, '_request', _fake_request)
        monkeypatch.setattr(transport, 'mode', 'mock')

        # Inject private WS progress with higher fill than detail (0.020 > 0.010)
        state = transport._private_ws_state
        await state.record_order(PrivateOrderUpdate(
            symbol="ETHUSDT",
            order_id="o-recon-1",
            client_order_id="c-recon-1",
            filled_quantity=0.020,
            average_price=51000.0,
            fee_quote=0.8,
            state=None,
            updated_at_ms=1700000004500,
        ))
        transport.private_order_progress = lambda **kw: (
            CumulativeOrderProgress.from_private(
                PrivateOrderUpdate(
                    symbol="ETHUSDT",
                    order_id="o-recon-1",
                    client_order_id="c-recon-1",
                    filled_quantity=0.020,
                    average_price=51000.0,
                    fee_quote=0.8,
                    updated_at_ms=1700000004500,
                )
            )
        )

        result = await transport.query_passive_order_progress(
            "ETHUSDT", "o-recon-1", "c-recon-1", side=Side.BUY,
        )

        # Both REST detail and reconciliation call _fetch_bitget_order_detail
        # (V1 matches: fetch_order_fill_reconciliation also calls fetch_bitget_order_detail)
        assert call_count >= 2, (
            f"Expected at least 2 _request calls (detail + reconciliation), got {call_count}"
        )
        # Reconciliation call: must also use UTA endpoint
        uta_calls = call_count  # all calls go to UTA in this test
        assert uta_calls >= 2

        # Private WS has 0.020 which is > detail 0.010 → private should win on quantity
        assert result is not None
        assert result.cumulative_quantity == 0.020, (
            f"Highest qty source (private=0.020) should win, got {result.cumulative_quantity}"
        )
        assert result.average_price == 51000.0
        assert result.fee_quote == 0.8
        assert result.order_id == "o-recon-1"

    @pytest.mark.asyncio
    async def test_reconciliation_wins_when_higher_qty_than_detail_and_private(self, monkeypatch):
        """When reconciliation has highest fill qty, it determines all fields
        per V1 priority: reconciliation > REST detail > private WS."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bitget_spec
        from lightfee.core.domain import Side
        from lightfee.marketdata.private_ws import (
            CumulativeOrderProgress,
            PrivateOrderUpdate,
        )

        spec = bitget_spec()
        request_num = 0

        async def _fake_request(method, path, **kwargs):
            nonlocal request_num
            request_num += 1
            if "/api/v3/trade/order-info" in path:
                if request_num == 1:
                    # REST detail: small fill
                    return {
                        "code": "00000",
                        "data": {
                            "orderId": "o-tri-1",
                            "clientOid": "c-tri-1",
                            "baseVolume": "0.005",
                            "priceAvg": "49000.0",
                            "fee": "0.2",
                            "uTime": "1700000005000",
                            "side": "sell",
                            "status": "PARTIALLY_FILLED",
                        },
                    }
                else:
                    # Reconciliation: larger fill (V1: same endpoint, second call)
                    return {
                        "code": "00000",
                        "data": {
                            "orderId": "o-tri-1",
                            "clientOid": "c-tri-1",
                            "baseVolume": "0.030",
                            "priceAvg": "52000.0",
                            "fee": "2.0",
                            "uTime": "1700000006000",
                            "side": "sell",
                            "status": "FILLED",
                        },
                    }
            return {"code": "00000", "data": {}}

        with patch.object(VenueTransport, '_validate_live_credentials', return_value=None):
            transport = VenueTransport(spec, mode='mock')
        monkeypatch.setattr(transport, '_request', _fake_request)
        monkeypatch.setattr(transport, 'mode', 'mock')

        # Private WS: medium fill (0.012)
        state = transport._private_ws_state
        await state.record_order(PrivateOrderUpdate(
            symbol="ETHUSDT",
            order_id="o-tri-1",
            client_order_id="c-tri-1",
            filled_quantity=0.012,
            average_price=50000.0,
            fee_quote=0.6,
            state=None,
            updated_at_ms=1700000005500,
        ))
        transport.private_order_progress = lambda **kw: (
            CumulativeOrderProgress.from_private(
                PrivateOrderUpdate(
                    symbol="ETHUSDT",
                    order_id="o-tri-1",
                    client_order_id="c-tri-1",
                    filled_quantity=0.012,
                    average_price=50000.0,
                    fee_quote=0.6,
                    updated_at_ms=1700000005500,
                )
            )
        )

        result = await transport.query_passive_order_progress(
            "ETHUSDT", "o-tri-1", "c-tri-1", side=Side.SELL,
        )

        # Reconciliation has 0.030 > private 0.012 > detail 0.005
        assert result is not None
        assert result.cumulative_quantity == 0.030, (
            f"Reconciliation (0.030) should win, got {result.cumulative_quantity}"
        )
        assert result.average_price == 52000.0
        assert result.fee_quote == 2.0
        assert result.last_fill_time_ms == 1700000006000
        assert request_num >= 2

    @pytest.mark.asyncio
    async def test_absent_order_code_40109_returns_none(self, monkeypatch):
        """Absent order code 40109/43001 → _fetch_bitget_order_detail returns None,
        query_passive_order_progress returns None."""
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bitget_spec
        from lightfee.core.domain import Side

        spec = bitget_spec()

        async def _fake_request(method, path, **kwargs):
            return {"code": "40109", "msg": "order does not exist", "data": None}

        with patch.object(VenueTransport, '_validate_live_credentials', return_value=None):
            transport = VenueTransport(spec, mode='mock')
        monkeypatch.setattr(transport, '_request', _fake_request)
        monkeypatch.setattr(transport, 'mode', 'mock')
        monkeypatch.setattr(transport, 'private_order_progress', lambda **kw: None)

        result = await transport.query_passive_order_progress(
            "ETHUSDT", "nonexistent", None, side=Side.BUY,
        )
        assert result is None, (
            "Absent order (code=40109) must return None"
        )

    def test_merge_has_three_sources_recon_detail_private(self):
        """merge_passive_progress_sources receives reconciliation, detail, private.
        V1 priority: highest cumulative_quantity wins; tied quantity prefers
        reconciliation > REST detail > private WS."""
        from lightfee.marketdata.private_ws import (
            CumulativeOrderProgress,
            merge_passive_progress_sources,
        )

        detail = CumulativeOrderProgress.from_position_snapshot(
            "detail-oid", "detail-cid", 0.010, 50000.0, 1.0, 100,
        )
        private = CumulativeOrderProgress.from_position_snapshot(
            "private-oid", "private-cid", 0.005, 50100.0, 0.5, 50,
        )

        class ReconFill:
            order_id = "recon-oid"
            client_order_id = "recon-cid"
            quantity = 0.015
            average_price = 50200.0
            fee_quote = 1.5
            filled_at_ms = 150

        merged = merge_passive_progress_sources(detail, ReconFill(), private)

        # Reconciliation has highest qty (0.015) → wins
        assert merged.cumulative_quantity == 0.015
        assert merged.order_id == "recon-oid"
        assert merged.average_price == 50200.0
        assert merged.fee_quote == 1.5
        assert merged.source == "reconciliation"

    def test_equal_quantity_reconciliation_wins_over_detail(self):
        """When reconciliation and detail have same qty, reconciliation wins on
        data fields (V1 priority)."""
        from lightfee.marketdata.private_ws import (
            CumulativeOrderProgress,
            merge_passive_progress_sources,
        )

        detail = CumulativeOrderProgress.from_position_snapshot(
            "d-oid", "d-cid", 0.010, 50000.0, 1.0, 100,
        )

        class ReconFill:
            order_id = "r-oid"
            client_order_id = "r-cid"
            quantity = 0.010
            average_price = 50100.0
            fee_quote = 1.2
            filled_at_ms = 120

        merged = merge_passive_progress_sources(detail, ReconFill(), None)

        # Same qty → reconciliation wins on price/fee/timestamp
        assert merged.cumulative_quantity == 0.010
        assert merged.average_price == 50100.0
        assert merged.fee_quote == 1.2
        assert merged.last_fill_at_ms == 120
        assert merged.source == "reconciliation"

    def test_identity_fallback_from_detail_when_recon_missing(self):
        """When reconciliation has fill but no order_id, fallback to detail's
        order_id/client_order_id (V1 identity fallback)."""
        from lightfee.marketdata.private_ws import (
            CumulativeOrderProgress,
            merge_passive_progress_sources,
        )

        detail = CumulativeOrderProgress.from_position_snapshot(
            "d-oid", "d-cid", 0.005, 50000.0, 1.0, 100,
        )

        class ReconFill:
            order_id = ""  # empty in reconciliation
            client_order_id = None
            quantity = 0.015
            average_price = 50200.0
            fee_quote = 1.5
            filled_at_ms = 150

        merged = merge_passive_progress_sources(detail, ReconFill(), None)

        assert merged.cumulative_quantity == 0.015
        assert merged.order_id == "d-oid", (
            "When reconciliation has no order_id, fallback to detail"
        )
        assert merged.client_order_id == "d-cid"

    # ------------------------------------------------------------------
    # Regression: V1 timestamp max() semantics (resolve_cumulative_order_progress)
    # ------------------------------------------------------------------

    def test_timestamp_last_fill_max_within_highest_sources(self):
        """V1: last_fill_at_ms uses max() across highest-quantity sources,
        NOT first-non-null. When reconciliation and detail have equal qty
        but different timestamps, max() must win.

        V1: ports.rs:252-260 — filter_map(|s| s.last_fill_at_ms).max()
        Reproduces old drift: first-in-list (recon=100) would win over
        detail=200, but V1 max() should pick 200.
        """
        from lightfee.marketdata.private_ws import (
            CumulativeOrderProgress,
            resolve_cumulative_order_progress,
        )

        # Two sources with equal cumulative_quantity, different timestamps
        detail = CumulativeOrderProgress.from_position_snapshot(
            "oid", "cid", 0.010, 50000.0, 1.0, 100,
        )
        detail.last_fill_at_ms = 200
        detail.source = "rest_snapshot"

        private = CumulativeOrderProgress.from_position_snapshot(
            "oid", "cid", 0.010, 50000.0, 1.0, 100,
        )
        private.last_fill_at_ms = 100
        private.source = "reconciliation"

        result = resolve_cumulative_order_progress([private, detail])

        # Both have qty=0.010, both are in highest tier.
        # V1 max() across highest → 200 (detail), NOT 100 (private/first).
        assert result is not None
        assert result.last_fill_at_ms == 200, (
            f"V1 max() semantics: equal qty → max timestamp 200, got {result.last_fill_at_ms}"
        )

    def test_timestamp_updated_at_max_within_highest_fallback_to_all(self):
        """V1: updated_at_ms uses max() within highest-quantity sources first,
        falls back to max() across ALL sources when highest have no timestamp.

        V1: ports.rs:242-250
        """
        from lightfee.marketdata.private_ws import (
            CumulativeOrderProgress,
            resolve_cumulative_order_progress,
        )

        # Highest-qty source has no updated_at_ms → fallback to all sources
        highest = CumulativeOrderProgress.from_position_snapshot(
            "oid", "cid", 0.020, 50000.0, None, None,
        )
        highest.updated_at_ms = None
        highest.source = "rest_snapshot"

        fallback = CumulativeOrderProgress.from_position_snapshot(
            "oid", "cid", 0.010, 50000.0, None, 50,
        )
        fallback.updated_at_ms = 300
        fallback.source = "private_ws"

        result = resolve_cumulative_order_progress([highest, fallback])

        assert result is not None
        assert result.cumulative_quantity == 0.020
        assert result.updated_at_ms == 300, (
            f"V1 fallback to all sources max(): highest has no updated_at_ms, "
            f"must use max from all sources (300), got {result.updated_at_ms}"
        )

    def test_timestamp_last_fill_fallback_to_all_sources(self):
        """V1: when highest sources have no last_fill_at_ms, max() falls back
        to all sources. (ports.rs:252-260)
        """
        from lightfee.marketdata.private_ws import (
            CumulativeOrderProgress,
            resolve_cumulative_order_progress,
        )

        highest = CumulativeOrderProgress.from_position_snapshot(
            "oid", "cid", 0.030, 50000.0, None, None,
        )
        highest.last_fill_at_ms = None
        highest.source = "reconciliation"

        fallback = CumulativeOrderProgress.from_position_snapshot(
            "oid", "cid", 0.010, 50000.0, None, 50,
        )
        fallback.last_fill_at_ms = 500
        fallback.source = "private_ws"

        result = resolve_cumulative_order_progress([highest, fallback])

        assert result is not None
        assert result.cumulative_quantity == 0.030
        assert result.last_fill_at_ms == 500, (
            f"V1 fallback: highest has no last_fill, must use max from all sources (500), "
            f"got {result.last_fill_at_ms}"
        )

    # ------------------------------------------------------------------
    # Regression: V1 Bitget state source order
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_bitget_state_comes_from_rest_detail_not_private_ws(self, monkeypatch):
        """V1: Bitget passive progress final state is determined by
        bitget_passive_order_state(status, merged_cum_qty, original_qty),
        NOT from merged.state (which could carry private WS state).

        Scenario: REST detail status=open, baseVolume=0.5, size=1.0.
        Private WS has quantity=0.5 but (artificially) state=FILLED.
        After fix: state must be PARTIALLY_FILLED (cum_qty=0.5 > 0 with
        status=open), NOT FILLED from private WS.

        V1: bitget.rs:2560-2590 — state from bitget_passive_order_state
        """
        from lightfee.venues.transport import VenueTransport
        from lightfee.venues.specs import bitget_spec
        from lightfee.core.domain import Side, PassiveOrderState
        from lightfee.marketdata.private_ws import (
            CumulativeOrderProgress,
            PrivateOrderUpdate,
        )

        spec = bitget_spec()

        async def _fake_request(method, path, **kwargs):
            return {
                "code": "00000",
                "data": {
                    "orderId": "o-state-1",
                    "clientOid": "c-state-1",
                    "baseVolume": "0.5",     # REST detail: half filled
                    "priceAvg": "50000.0",
                    "fee": "1.0",
                    "uTime": "1700000007000",
                    "side": "buy",
                    "status": "open",         # REST detail: order still open
                    "size": "1.0",            # original quantity = 1.0
                },
            }

        with patch.object(VenueTransport, '_validate_live_credentials', return_value=None):
            transport = VenueTransport(spec, mode='mock')
        monkeypatch.setattr(transport, '_request', _fake_request)
        monkeypatch.setattr(transport, 'mode', 'mock')

        # Inject private WS progress with same fill qty BUT state=FILLED
        # (simulating a bug where WS carries state that should be ignored)
        state = transport._private_ws_state
        await state.record_order(PrivateOrderUpdate(
            symbol="ETHUSDT",
            order_id="o-state-1",
            client_order_id="c-state-1",
            filled_quantity=0.5,
            average_price=50000.0,
            fee_quote=1.0,
            state=PassiveOrderState.FILLED,  # ← WS claims FILLED (wrong!)
            updated_at_ms=1700000007500,
        ))
        transport.private_order_progress = lambda **kw: (
            CumulativeOrderProgress.from_private(
                PrivateOrderUpdate(
                    symbol="ETHUSDT",
                    order_id="o-state-1",
                    client_order_id="c-state-1",
                    filled_quantity=0.5,
                    average_price=50000.0,
                    fee_quote=1.0,
                    state=PassiveOrderState.FILLED,  # ← should be ignored
                    updated_at_ms=1700000007500,
                )
            )
        )

        result = await transport.query_passive_order_progress(
            "ETHUSDT", "o-state-1", "c-state-1", side=Side.BUY,
        )

        # V1: bitget_passive_order_state("open", 0.5, Some(1.0))
        # cum_qty=0.5 > 0 → PARTIALLY_FILLED
        # NOT FILLED (status is "open", not "filled", and cum_qty < original_qty)
        assert result is not None
        assert result.state == PassiveOrderState.PARTIALLY_FILLED, (
            f"V1: state must be PARTIALLY_FILLED from REST detail status=open + "
            f"merged_qty=0.5 > 0, NOT FILLED from private WS. Got {result.state}"
        )
        assert result.cumulative_quantity == 0.5
