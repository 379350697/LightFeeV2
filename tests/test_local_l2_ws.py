"""Local-L2 WebSocket streaming client tests.

Tests WS client message parsing, data plane WS lifecycle,
and ingest_external_update() bridging from WS→runtime.
"""

from __future__ import annotations

import asyncio
import json

import pytest

import lightfee.marketdata.local_l2_ws as local_l2_ws_module
from lightfee.marketdata.l2 import (
    L2BookStatus,
    L2PoolAssignment,
    LocalL2BookKey,
    LocalL2Update,
    LocalL2UpdateKind,
    PriceLevel,
)
from lightfee.marketdata.local_l2_runtime import LocalL2Runtime
from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
from lightfee.marketdata.local_l2_ws import (
    AsterL2WsClient,
    BinanceL2WsClient,
    BitgetL2WsClient,
    BybitL2WsClient,
    GateL2WsClient,
    HyperliquidL2WsClient,
    OkxL2WsClient,
    create_ws_client,
    WS_CLIENT_REGISTRY,
    LocalL2WsClient,
)
from lightfee.persistence.journal import Journal


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_journal() -> Journal:
    import tempfile
    import os as _os
    jpath = _os.path.join(tempfile.mkdtemp(), "test.journal")
    j = Journal(jpath)
    j.open()
    return j


def _make_data_plane() -> tuple[LocalL2DataPlane, LocalL2Runtime, Journal]:
    rt = LocalL2Runtime()
    journal = _make_journal()
    dp = LocalL2DataPlane(l2_runtime=rt, journal=journal)
    return dp, rt, journal


def _close_data_plane(dp: LocalL2DataPlane, journal: Journal) -> None:
    """Best-effort cleanup for helper-created local-L2 test resources."""
    try:
        asyncio.run(dp.stop_ws_streams())
    except Exception:
        pass
    try:
        journal.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# WS client message parsing
# ---------------------------------------------------------------------------


class TestBinanceL2WsClientParsing:
    def test_parses_depth_update_delta(self):
        dp, rt, _ = _make_data_plane()
        client = BinanceL2WsClient(
            venue="binance", symbol="BTCUSDT", data_plane=dp,
        )

        raw = {
            "e": "depthUpdate",
            "E": 1715000000000,
            "s": "BTCUSDT",
            "U": 1001,
            "u": 1010,
            "b": [["50000.00", "1.5"], ["49900.00", "2.0"]],
            "a": [["50100.00", "0.5"], ["50200.00", "1.0"]],
        }

        update = client.parse_depth_message(raw)
        assert update is not None
        assert update.venue == "binance"
        assert update.symbol == "BTCUSDT"
        assert update.update_kind == LocalL2UpdateKind.DELTA
        assert update.sequence == 1010
        assert update.first_sequence == 1001
        assert update.previous_sequence == 0
        assert update.previous_sequence_present is False
        assert len(update.bids) == 2
        assert update.bids[0].price == 50000.0
        assert update.bids[0].quantity == 1.5
        assert len(update.asks) == 2
        assert update.asks[0].price == 50100.0
        assert update.asks[0].quantity == 0.5
        _close_data_plane(dp, _)

    def test_parses_pu_as_previous_sequence(self):
        dp, rt, _ = _make_data_plane()
        client = BinanceL2WsClient(
            venue="binance", symbol="BTCUSDT", data_plane=dp,
        )

        raw = {
            "e": "depthUpdate",
            "E": 1715000000000,
            "s": "BTCUSDT",
            "U": 1001,
            "u": 1010,
            "pu": 1009,
            "b": [["50000.00", "1.5"]],
            "a": [["50100.00", "0.5"]],
        }

        update = client.parse_depth_message(raw)
        assert update is not None
        assert update.previous_sequence == 1009
        _close_data_plane(dp, _)

    def test_ignores_non_depth_messages(self):
        dp, rt, _ = _make_data_plane()
        client = BinanceL2WsClient(
            venue="binance", symbol="BTCUSDT", data_plane=dp,
        )
        # Trade event, not depth
        raw = {"e": "trade", "s": "BTCUSDT", "p": "50000.00"}
        assert client.parse_depth_message(raw) is None
        _close_data_plane(dp, _)

    def test_handles_empty_books(self):
        dp, rt, _ = _make_data_plane()
        client = BinanceL2WsClient(
            venue="binance", symbol="BTCUSDT", data_plane=dp,
        )
        raw = {
            "e": "depthUpdate",
            "E": 1715000000000,
            "s": "BTCUSDT",
            "U": 1,
            "u": 1,
            "b": [],
            "a": [],
        }
        update = client.parse_depth_message(raw)
        assert update is not None
        assert len(update.bids) == 0
        assert len(update.asks) == 0
        _close_data_plane(dp, _)


class TestLocalL2WsFreshnessEvidence:
    @pytest.mark.asyncio
    async def test_bybit_subscription_confirmation_does_not_refresh_hot_book(self):
        dp, rt, journal = _make_data_plane()
        try:
            book = rt.ensure_book("bybit", "BTCUSDT")
            book.status = L2BookStatus.HOT
            book.pool = L2PoolAssignment.HOT_EXEC
            book.observed_at_ms = 1_000
            book.bids = [PriceLevel(100.0, 1.0)]
            book.asks = [PriceLevel(101.0, 1.0)]
            client = BybitL2WsClient(venue="bybit", symbol="BTCUSDT", data_plane=dp)
            dp.start_worker(LocalL2BookKey("bybit", "BTCUSDT"), client)

            await client._handle_message('{"op":"subscribe","success":true,"ret_msg":"subscribe"}')

            assert book.observed_at_ms == 1_000
            records = journal.read_all()
            confirmed = [
                record for record in records
                if record["kind"] == "runtime.local_l2_ws_transport"
                and record["payload"]["event"] == "subscription_confirmed"
            ]
            assert len(confirmed) == 1
            assert confirmed[0]["payload"]["ws_stream"]["subscription_mode"] == "unknown"
        finally:
            journal.close()

    @pytest.mark.asyncio
    async def test_failed_subscription_confirmation_does_not_refresh_hot_book(self):
        dp, rt, journal = _make_data_plane()
        try:
            book = rt.ensure_book("bybit", "BTCUSDT")
            book.status = L2BookStatus.HOT
            book.observed_at_ms = 1_000
            book.bids = [PriceLevel(100.0, 1.0)]
            book.asks = [PriceLevel(101.0, 1.0)]
            client = BybitL2WsClient(venue="bybit", symbol="BTCUSDT", data_plane=dp)
            dp.start_worker(LocalL2BookKey("bybit", "BTCUSDT"), client)

            await client._handle_message(
                '{"event":"subscribe","success":false,"error":"subscribe failed"}'
            )

            assert book.observed_at_ms == 1_000
            records = journal.read_all()
            rejection = [
                record for record in records
                if record["kind"] == "runtime.local_l2_ws_transport"
                and record["payload"]["event"] == "subscription_rejected"
            ]
            assert len(rejection) == 1
            assert rejection[0]["payload"]["ws_stream"]["client_state"] == "disconnected"
        finally:
            journal.close()

    @pytest.mark.asyncio
    async def test_bybit_keepalive_does_not_refresh_hot_book(self):
        dp, rt, journal = _make_data_plane()
        try:
            book = rt.ensure_book("bybit", "ETHUSDT")
            book.status = L2BookStatus.HOT
            book.observed_at_ms = 1_000
            book.bids = [PriceLevel(100.0, 1.0)]
            book.asks = [PriceLevel(101.0, 1.0)]
            client = BybitL2WsClient(venue="bybit", symbol="ETHUSDT", data_plane=dp)

            await client._handle_message('{"op":"pong"}')

            assert book.observed_at_ms == 1_000
        finally:
            journal.close()

    @pytest.mark.asyncio
    async def test_bybit_sequence_gap_requests_existing_client_reconnect(self):
        """A real Bybit delta continuity fault must close this symbol's WS session."""
        dp, rt, journal = _make_data_plane()

        class CloseableWs:
            def __init__(self):
                self.close_calls = 0

            async def close(self):
                self.close_calls += 1

        try:
            book = rt.ensure_book("bybit", "BTCUSDT")
            book.status = L2BookStatus.HOT
            book.sequence = 100
            book.last_update_id = 100
            book.observed_at_ms = 1_000
            book.bids = [PriceLevel(100.0, 1.0)]
            book.asks = [PriceLevel(101.0, 1.0)]
            ws = CloseableWs()
            client = BybitL2WsClient(venue="bybit", symbol="BTCUSDT", data_plane=dp)
            client._state = "connected"
            client._ws = ws
            dp.start_worker(LocalL2BookKey("bybit", "BTCUSDT"), client)

            await client._handle_message(json.dumps({
                "topic": "orderbook.50.BTCUSDT",
                "type": "delta",
                "data": {
                    "s": "BTCUSDT", "b": [], "a": [], "u": 105, "pu": 103,
                },
            }))
            await asyncio.sleep(0)

            assert book.status == L2BookStatus.REBUILDING
            assert ws.close_calls == 1
            records = journal.read_all()
            assert any(
                record["kind"] == "runtime.local_l2_ws_transport"
                and record["payload"]["event"] == "reconnect_requested"
                for record in records
            )
        finally:
            journal.close()

    @pytest.mark.asyncio
    async def test_connect_failure_records_reconnect_evidence(self, monkeypatch):
        """The real WS loop must journal failures before it backs off and retries."""
        dp, _rt, journal = _make_data_plane()
        attempted = asyncio.Event()

        class FailingConnect:
            async def __aenter__(self):
                attempted.set()
                raise ConnectionRefusedError("test connection refused")

            async def __aexit__(self, exc_type, exc, tb):
                return False

        client = BybitL2WsClient(venue="bybit", symbol="BTCUSDT", data_plane=dp)
        dp.start_worker(LocalL2BookKey("bybit", "BTCUSDT"), client)
        monkeypatch.setattr(
            local_l2_ws_module.websockets,
            "connect",
            lambda *args, **kwargs: FailingConnect(),
        )
        try:
            await client.start()
            await asyncio.wait_for(attempted.wait(), timeout=1)
            await asyncio.sleep(0)
            await client.stop()

            records = journal.read_all()
            events = [
                record["payload"] for record in records
                if record["kind"] == "runtime.local_l2_ws_transport"
            ]
            connect_error = next(item for item in events if item["event"] == "connect_error")
            retry = next(item for item in events if item["event"] == "reconnect_scheduled")
            assert connect_error["ws_stream"]["error_count"] == 1
            assert "ConnectionRefusedError" in connect_error["ws_stream"]["last_error"]
            assert retry["reconnect_delay_ms"] == 1000
        finally:
            await client.stop()
            journal.close()

    @pytest.mark.asyncio
    async def test_aster_url_subscription_records_connected_generation(self, monkeypatch):
        """Aster's URL-auto subscription has no JSON acknowledgement to preserve."""
        dp, _rt, journal = _make_data_plane()
        connected = asyncio.Event()
        hold_read_loop = asyncio.Event()

        class HoldingConnect:
            close_code = None
            close_reason = ""

            async def __aenter__(self):
                connected.set()
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            def __aiter__(self):
                return self

            async def __anext__(self):
                await hold_read_loop.wait()
                raise StopAsyncIteration

        client = AsterL2WsClient(venue="aster", symbol="BTCUSDT", data_plane=dp)
        dp.start_worker(LocalL2BookKey("aster", "BTCUSDT"), client)
        monkeypatch.setattr(
            local_l2_ws_module.websockets,
            "connect",
            lambda *args, **kwargs: HoldingConnect(),
        )
        try:
            await client.start()
            await asyncio.wait_for(connected.wait(), timeout=1)
            await asyncio.sleep(0)
            await client.stop()

            records = journal.read_all()
            events = [
                record["payload"] for record in records
                if record["kind"] == "runtime.local_l2_ws_transport"
            ]
            connected_event = next(item for item in events if item["event"] == "connected")
            auto_subscribed = next(item for item in events if item["event"] == "url_auto_subscribed")
            assert connected_event["ws_stream"]["stream_generation"] == 1
            assert auto_subscribed["ws_stream"]["subscription_mode"] == "url_auto"
        finally:
            await client.stop()
            journal.close()

    @pytest.mark.asyncio
    async def test_bybit_connection_uses_json_application_heartbeat(self, monkeypatch):
        """Bybit V5 requires its JSON ping; RFC control pings are disabled there."""
        dp, _rt, journal = _make_data_plane()
        received_ping = asyncio.Event()
        release_read_loop = asyncio.Event()
        sent_messages = []
        connect_kwargs = {}

        class HoldingConnect:
            close_code = None
            close_reason = ""

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def send(self, raw):
                message = json.loads(raw)
                sent_messages.append(message)
                if message == {"op": "ping"}:
                    received_ping.set()

            def __aiter__(self):
                return self

            async def __anext__(self):
                await release_read_loop.wait()
                raise StopAsyncIteration

        def connect(*args, **kwargs):
            connect_kwargs.update(kwargs)
            return HoldingConnect()

        client = BybitL2WsClient(venue="bybit", symbol="BTCUSDT", data_plane=dp)
        client.ping_interval_s = 0.01
        dp.start_worker(LocalL2BookKey("bybit", "BTCUSDT"), client)
        monkeypatch.setattr(local_l2_ws_module.websockets, "connect", connect)
        try:
            await client.start()
            await asyncio.wait_for(received_ping.wait(), timeout=1)

            assert connect_kwargs["ping_interval"] is None
            assert {"op": "subscribe", "args": ["orderbook.50.BTCUSDT"]} in sent_messages
            assert {"op": "ping"} in sent_messages
        finally:
            release_read_loop.set()
            await client.stop()
            journal.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("client_type", "venue"),
        [(BinanceL2WsClient, "binance"), (AsterL2WsClient, "aster")],
    )
    async def test_sequence_gap_evidence_carries_receipt_and_stream_state(self, client_type, venue):
        """A production WS message that causes a gap keeps both sides of the evidence."""
        dp, rt, journal = _make_data_plane()
        try:
            book = rt.ensure_book(venue, "BTCUSDT")
            book.status = L2BookStatus.HOT
            book.sequence = 100
            book.last_update_id = 100
            book.bids = [PriceLevel(100.0, 1.0)]
            book.asks = [PriceLevel(101.0, 1.0)]
            client = client_type(venue=venue, symbol="BTCUSDT", data_plane=dp)
            client._state = "connected"
            dp.start_worker(LocalL2BookKey(venue, "BTCUSDT"), client)

            await client._handle_message(json.dumps({
                "e": "depthUpdate", "E": 1_000, "s": "BTCUSDT",
                "U": 106, "u": 110, "pu": 105,
                "b": [["100.0", "1.0"]], "a": [["101.0", "1.0"]],
            }))

            records = journal.read_all()
            gap = next(
                record["payload"] for record in records
                if record["kind"] == "runtime.local_l2_sequence_gap_rebuild"
            )
            stream = gap["ws_stream"]
            assert stream["client_state"] == "connected"
            assert stream["message_count"] == 1
            assert stream["last_raw_U"] == 106
            assert stream["last_raw_u"] == 110
            assert stream["last_raw_pu"] == 105
            assert gap["raw_U"] == 106
            assert gap["raw_u"] == 110
            assert gap["raw_pu"] == 105
        finally:
            journal.close()

    @pytest.mark.asyncio
    async def test_hot_stale_evidence_carries_ws_transport_state(self):
        dp, rt, journal = _make_data_plane()
        try:
            book = rt.ensure_book("bybit", "BTCUSDT")
            book.status = L2BookStatus.HOT
            book.pool = L2PoolAssignment.HOT_EXEC
            book.observed_at_ms = 1_000
            book.bids = [PriceLevel(100.0, 1.0)]
            book.asks = [PriceLevel(101.0, 1.0)]
            client = BybitL2WsClient(venue="bybit", symbol="BTCUSDT", data_plane=dp)
            client._state = "connected"
            client._ws = object()
            client._message_count = 3
            client._error_count = 2
            client._last_message_ms = 1_000
            client._last_error = "ConnectionResetError: prior reset"
            dp.start_worker(LocalL2BookKey("bybit", "BTCUSDT"), client)
            dp.hot_stale_after_ms = 100

            await dp.sync_snapshots({}, now_ms=1_101)

            records = journal.read_all()
            stale = next(
                record["payload"] for record in records
                if record["kind"] == "runtime.local_l2_hot_stale_awaiting_ws_delta"
            )
            stream = stale["ws_stream"]
            assert stream["connected"] is True
            assert stream["message_count"] == 3
            assert stream["error_count"] == 2
            assert stream["last_message_ms"] == 1_000
            assert stream["last_error"] == "ConnectionResetError: prior reset"
        finally:
            journal.close()


class TestOkxL2WsClientParsing:
    def test_parses_snapshot(self):
        dp, rt, _ = _make_data_plane()
        client = OkxL2WsClient(
            venue="okx", symbol="BTC-USDT-SWAP", data_plane=dp,
        )

        raw = {
            "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
            "action": "snapshot",
            "data": [{
                "asks": [["50100.00", "1.0", "0", "1"], ["50200.00", "2.0", "0", "1"]],
                "bids": [["50000.00", "0.5", "0", "1"], ["49900.00", "1.5", "0", "1"]],
                "ts": "1715000000000",
                "checksum": -1,
            }],
        }

        update = client.parse_depth_message(raw)
        assert update is not None
        assert update.venue == "okx"
        assert update.update_kind == LocalL2UpdateKind.SNAPSHOT
        assert len(update.bids) == 2
        assert update.bids[0].price == 50000.0
        assert update.bids[0].quantity == 0.5
        _close_data_plane(dp, _)

    def test_parses_delta(self):
        dp, rt, _ = _make_data_plane()
        client = OkxL2WsClient(
            venue="okx", symbol="BTC-USDT-SWAP", data_plane=dp,
        )

        raw = {
            "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
            "action": "update",
            "data": [{
                "asks": [["50100.00", "0", "0", "1"]],
                "bids": [],
                "ts": "1715000001000",
                "checksum": 12345,
                "seqId": 99,
            }],
        }

        update = client.parse_depth_message(raw)
        assert update is not None
        assert update.update_kind == LocalL2UpdateKind.DELTA
        assert update.sequence == 99
        assert update.checksum == 12345
        _close_data_plane(dp, _)

    def test_parses_row_prev_seq_id(self):
        dp, rt, _ = _make_data_plane()
        client = OkxL2WsClient(
            venue="okx", symbol="BTC-USDT-SWAP", data_plane=dp,
        )

        raw = {
            "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
            "action": "update",
            "data": [{
                "asks": [],
                "bids": [["50000.00", "0.5", "0", "1"]],
                "ts": "1715000001000",
                "checksum": -855196043,
                "prevSeqId": 80,
                "seqId": 99,
            }],
        }

        update = client.parse_depth_message(raw)
        assert update is not None
        assert update.previous_sequence == 80
        assert update.checksum == -855196043
        _close_data_plane(dp, _)

    def test_ignores_non_books_channel(self):
        dp, rt, _ = _make_data_plane()
        client = OkxL2WsClient(
            venue="okx", symbol="BTC-USDT-SWAP", data_plane=dp,
        )
        raw = {"arg": {"channel": "tickers", "instId": "BTC-USDT-SWAP"}, "data": []}
        assert client.parse_depth_message(raw) is None
        _close_data_plane(dp, _)


class TestBybitL2WsClientParsing:
    def test_subscribes_to_level_50_book(self):
        dp, rt, _ = _make_data_plane()
        client = BybitL2WsClient(
            venue="bybit", symbol="BTCUSDT", data_plane=dp,
        )

        assert client.build_subscribe_message() == {
            "op": "subscribe",
            "args": ["orderbook.50.BTCUSDT"],
        }
        _close_data_plane(dp, _)

    def test_parses_snapshot(self):
        dp, rt, _ = _make_data_plane()
        client = BybitL2WsClient(
            venue="bybit", symbol="BTCUSDT", data_plane=dp,
        )

        raw = {
            "topic": "orderbook.1.BTCUSDT",
            "type": "snapshot",
            "ts": 1715000000000,
            "data": {
                "s": "BTCUSDT",
                "b": [["50000.00", "2.5"], ["49900.00", "1.0"]],
                "a": [["50100.00", "1.5"]],
                "u": 200,
                "seq": 199,
            },
        }

        update = client.parse_depth_message(raw)
        assert update is not None
        assert update.venue == "bybit"
        assert update.update_kind == LocalL2UpdateKind.SNAPSHOT
        assert update.sequence == 200
        assert len(update.bids) == 2
        _close_data_plane(dp, _)

    def test_parses_delta(self):
        dp, rt, _ = _make_data_plane()
        client = BybitL2WsClient(
            venue="bybit", symbol="BTCUSDT", data_plane=dp,
        )

        raw = {
            "topic": "orderbook.1.BTCUSDT",
            "type": "delta",
            "data": {
                "s": "BTCUSDT",
                "b": [],
                "a": [["50100.00", "0"]],  # price level deleted
                "u": 201,
                "seq": 200,
            },
        }

        update = client.parse_depth_message(raw)
        assert update is not None
        assert update.update_kind == LocalL2UpdateKind.DELTA
        assert len(update.asks) == 1
        assert update.asks[0].quantity == 0  # deletion
        _close_data_plane(dp, _)


class TestBitgetL2WsClientParsing:
    def test_parses_snapshot(self):
        dp, rt, _ = _make_data_plane()
        client = BitgetL2WsClient(
            venue="bitget", symbol="BTCUSDT", data_plane=dp,
        )

        raw = {
            "action": "snapshot",
            "arg": {"instType": "USDT-FUTURES", "channel": "books", "instId": "BTCUSDT"},
            "data": [{
                "asks": [["50100.00", "1.0"], ["50200.00", "0.5"]],
                "bids": [["50000.00", "2.0"], ["49900.00", "1.5"]],
                "ts": "1715000000000",
                "seq": 123,
                "pseq": 0,
                "checksum": 98765,
            }],
        }

        update = client.parse_depth_message(raw)
        assert update is not None
        assert update.venue == "bitget"
        assert update.symbol == "BTCUSDT"
        assert update.update_kind == LocalL2UpdateKind.SNAPSHOT
        assert update.sequence == 123
        assert update.previous_sequence == 0
        assert update.checksum == 98765
        assert len(update.bids) == 2
        assert update.bids[0].price == 50000.0
        assert update.bids[0].quantity == 2.0
        assert len(update.asks) == 2

    def test_parses_delta(self):
        dp, rt, _ = _make_data_plane()
        client = BitgetL2WsClient(
            venue="bitget", symbol="BTCUSDT", data_plane=dp,
        )

        raw = {
            "action": "update",
            "arg": {"instType": "USDT-FUTURES", "channel": "books", "instId": "BTCUSDT"},
            "data": [{
                "asks": [["50100.00", "0"]],
                "bids": [],
                "ts": "1715000001000",
                "seq": 124,
                "pseq": 123,
                "checksum": 98766,
            }],
        }

        update = client.parse_depth_message(raw)
        assert update is not None
        assert update.update_kind == LocalL2UpdateKind.DELTA
        assert update.sequence == 124
        assert update.previous_sequence == 123
        assert update.checksum == 98766
        assert len(update.asks) == 1
        assert update.asks[0].price == 50100.0
        assert update.asks[0].quantity == 0  # deletion

    def test_ignores_non_depth_channels(self):
        dp, rt, _ = _make_data_plane()
        client = BitgetL2WsClient(
            venue="bitget", symbol="BTCUSDT", data_plane=dp,
        )
        raw = {"action": "update", "arg": {"channel": "tickers", "instId": "BTCUSDT"}, "data": []}
        assert client.parse_depth_message(raw) is None
        _close_data_plane(dp, _)

    def test_ignores_non_action_messages(self):
        dp, rt, _ = _make_data_plane()
        client = BitgetL2WsClient(
            venue="bitget", symbol="BTCUSDT", data_plane=dp,
        )
        raw = {"event": "subscribe", "arg": {"channel": "books"}}
        assert client.parse_depth_message(raw) is None
        _close_data_plane(dp, _)


class TestGateL2WsClientParsing:
    def test_build_subscribe_message_uses_v1_obu_channel(self):
        dp, rt, _ = _make_data_plane()
        client = GateL2WsClient(
            venue="gate", symbol="BTCUSDT", venue_symbol="BTC_USDT", data_plane=dp,
        )

        message = client.build_subscribe_message()

        assert message is not None
        assert message["channel"] == "futures.obu"
        assert message["payload"] == ["ob.BTC_USDT.400"]
        _close_data_plane(dp, _)

    def test_parses_snapshot_all_event(self):
        dp, rt, _ = _make_data_plane()
        client = GateL2WsClient(
            venue="gate", symbol="BTCUSDT", data_plane=dp,
        )

        raw = {
            "time": 1715000000,
            "channel": "futures.order_book",
            "event": "all",
            "result": {
                "t": 1715000000000,
                "id": 123456,
                "contract": "BTC_USDT",
                "asks": [["50100.00", "1.5"], ["50200.00", "0.5"]],
                "bids": [["50000.00", "2.0"], ["49900.00", "1.0"]],
            },
        }

        update = client.parse_depth_message(raw)
        assert update is not None
        assert update.venue == "gate"
        assert update.symbol == "BTCUSDT"
        assert update.update_kind == LocalL2UpdateKind.SNAPSHOT
        assert update.sequence == 123456
        assert len(update.bids) == 2
        assert update.bids[0].price == 50000.0
        assert update.bids[0].quantity == 2.0
        assert len(update.asks) == 2
        _close_data_plane(dp, _)

    def test_parses_delta_update_event(self):
        dp, rt, _ = _make_data_plane()
        client = GateL2WsClient(
            venue="gate", symbol="BTCUSDT", data_plane=dp,
        )

        raw = {
            "time": 1715000001,
            "channel": "futures.order_book",
            "event": "update",
            "result": {
                "t": 1715000001000,
                "id": 123457,
                "contract": "BTC_USDT",
                "asks": [["50100.00", "0"]],
                "bids": [["50000.00", "2.5"]],
            },
        }

        update = client.parse_depth_message(raw)
        assert update is not None
        assert update.update_kind == LocalL2UpdateKind.DELTA
        assert update.sequence == 123457
        assert len(update.asks) == 1
        assert update.asks[0].quantity == 0  # deletion
        _close_data_plane(dp, _)

    def test_ignores_non_orderbook_channels(self):
        dp, rt, _ = _make_data_plane()
        client = GateL2WsClient(
            venue="gate", symbol="BTCUSDT", data_plane=dp,
        )
        raw = {"time": 1715000000, "channel": "futures.trades", "event": "update", "result": {}}
        assert client.parse_depth_message(raw) is None
        _close_data_plane(dp, _)

    def test_ignores_non_data_events(self):
        dp, rt, _ = _make_data_plane()
        client = GateL2WsClient(
            venue="gate", symbol="BTCUSDT", data_plane=dp,
        )
        raw = {"time": 1715000000, "channel": "futures.order_book", "event": "subscribe"}
        assert client.parse_depth_message(raw) is None
        _close_data_plane(dp, _)

    def test_gate_symbol_conversion(self):
        dp, rt, _ = _make_data_plane()
        # gate_symbol() now delegates to wire_symbol which uses venue_symbol
        client = GateL2WsClient(
            venue="gate", symbol="BTCUSDT", data_plane=dp,
            venue_symbol="BTC_USDT",
        )
        assert client.gate_symbol() == "BTC_USDT"
        assert client.wire_symbol == "BTC_USDT"
        _close_data_plane(dp, _)

    def test_gate_wire_symbol_defaults_to_canonical(self):
        dp, rt, _ = _make_data_plane()
        client = GateL2WsClient(
            venue="gate", symbol="BTCUSDT", data_plane=dp,
        )
        # Without venue_symbol, wire_symbol falls back to canonical symbol
        assert client.wire_symbol == "BTCUSDT"
        _close_data_plane(dp, _)


class TestAsterL2WsClientParsing:
    def test_uses_official_aster_futures_stream_host(self):
        dp, rt, _ = _make_data_plane()
        client = AsterL2WsClient(
            venue="aster", symbol="BTCUSDT", data_plane=dp,
        )

        assert client.websocket_url() == "wss://fstream.asterdex.com/ws/btcusdt@depth@100ms"
        _close_data_plane(dp, _)

    def test_parses_pu_as_previous_sequence(self):
        dp, rt, _ = _make_data_plane()
        client = AsterL2WsClient(
            venue="aster", symbol="BTCUSDT", data_plane=dp,
        )

        raw = {
            "e": "depthUpdate",
            "E": 1715000000000,
            "s": "BTCUSDT",
            "U": 1001,
            "u": 1010,
            "pu": 1009,
            "b": [["50000.00", "1.5"]],
            "a": [["50100.00", "0.5"]],
        }

        update = client.parse_depth_message(raw)
        assert update is not None
        assert update.previous_sequence == 1009
        _close_data_plane(dp, _)

    def test_parses_depth_update_delta(self):
        dp, rt, _ = _make_data_plane()
        client = AsterL2WsClient(
            venue="aster", symbol="BTCUSDT", data_plane=dp,
        )

        raw = {
            "e": "depthUpdate",
            "E": 1715000000000,
            "s": "BTCUSDT",
            "U": 501,
            "u": 510,
            "b": [["50000.00", "1.5"], ["49900.00", "2.0"]],
            "a": [["50100.00", "0.5"], ["50200.00", "1.0"]],
        }

        update = client.parse_depth_message(raw)
        assert update is not None
        assert update.venue == "aster"
        assert update.symbol == "BTCUSDT"
        assert update.update_kind == LocalL2UpdateKind.DELTA
        assert update.sequence == 510
        assert update.first_sequence == 501
        assert update.previous_sequence == 0
        assert update.previous_sequence_present is False
        assert len(update.bids) == 2
        assert update.bids[0].price == 50000.0
        assert update.bids[0].quantity == 1.5
        assert len(update.asks) == 2
        _close_data_plane(dp, _)

    def test_ignores_non_depth_messages(self):
        dp, rt, _ = _make_data_plane()
        client = AsterL2WsClient(
            venue="aster", symbol="BTCUSDT", data_plane=dp,
        )
        raw = {"e": "trade", "s": "BTCUSDT", "p": "50000.00"}
        assert client.parse_depth_message(raw) is None
        _close_data_plane(dp, _)

    def test_handles_empty_books(self):
        dp, rt, _ = _make_data_plane()
        client = AsterL2WsClient(
            venue="aster", symbol="BTCUSDT", data_plane=dp,
        )
        raw = {
            "e": "depthUpdate",
            "E": 1715000000000,
            "s": "BTCUSDT",
            "U": 1,
            "u": 1,
            "b": [],
            "a": [],
        }
        update = client.parse_depth_message(raw)
        assert update is not None
        assert len(update.bids) == 0
        assert len(update.asks) == 0
        _close_data_plane(dp, _)


class TestHyperliquidL2WsClient:
    def test_client_is_ws_client_subclass(self):
        dp, rt, _ = _make_data_plane()
        client = HyperliquidL2WsClient(
            venue="hyperliquid", symbol="BTCUSDT", data_plane=dp,
            venue_symbol="BTC",
        )
        assert isinstance(client, LocalL2WsClient)
        _close_data_plane(dp, _)

    def test_websocket_url_is_public_l2book_endpoint(self):
        dp, rt, _ = _make_data_plane()
        client = HyperliquidL2WsClient(
            venue="hyperliquid", symbol="BTCUSDT", data_plane=dp,
            venue_symbol="BTC",
        )
        assert client.websocket_url() == "wss://api.hyperliquid.xyz/ws"
        _close_data_plane(dp, _)

    def test_parse_depth_message_parses_l2book(self):
        dp, rt, _ = _make_data_plane()
        client = HyperliquidL2WsClient(
            venue="hyperliquid", symbol="BTCUSDT", data_plane=dp,
            venue_symbol="BTC",
        )
        update = client.parse_depth_message({
            "channel": "l2Book",
            "data": {
                "coin": "BTC",
                "time": 1715000000000,
                "levels": [
                    [{"px": "50000", "sz": "1"}],
                    [{"px": "50100", "sz": "1"}],
                ],
            },
        })
        assert update is not None
        assert update.symbol == "BTCUSDT"
        assert update.update_kind == LocalL2UpdateKind.SNAPSHOT
        _close_data_plane(dp, _)

    def test_set_adapter_is_stream_only_noop(self):
        dp, rt, _ = _make_data_plane()
        client = HyperliquidL2WsClient(
            venue="hyperliquid", symbol="BTCUSDT", data_plane=dp,
            venue_symbol="BTC",
        )
        adapter = object()
        client.set_adapter(adapter)
        assert not hasattr(client, "_adapter")
        _close_data_plane(dp, _)


# ---------------------------------------------------------------------------
# WS client factory
# ---------------------------------------------------------------------------


class TestWsClientFactory:
    def test_creates_client_for_registered_venue(self):
        dp, rt, _ = _make_data_plane()
        client = create_ws_client("binance", "BTCUSDT", dp)
        assert isinstance(client, BinanceL2WsClient)
        assert client.venue == "binance"
        assert client.symbol == "BTCUSDT"

    def test_creates_okx_client(self):
        dp, rt, _ = _make_data_plane()
        client = create_ws_client("okx", "BTCUSDT", dp)
        assert isinstance(client, OkxL2WsClient)
        # verify venue wire symbol was resolved
        assert client.wire_symbol == "BTC-USDT-SWAP"
        assert client.symbol == "BTCUSDT"

    def test_creates_bybit_client(self):
        dp, rt, _ = _make_data_plane()
        client = create_ws_client("bybit", "BTCUSDT", dp)
        assert isinstance(client, BybitL2WsClient)

    def test_returns_none_for_unregistered_venue(self):
        dp, rt, _ = _make_data_plane()
        client = create_ws_client("unknown_venue", "BTC", dp)
        assert client is None

    def test_registry_has_expected_venues(self):
        assert "binance" in WS_CLIENT_REGISTRY
        assert "okx" in WS_CLIENT_REGISTRY
        assert "bybit" in WS_CLIENT_REGISTRY
        assert "bitget" in WS_CLIENT_REGISTRY
        assert "gate" in WS_CLIENT_REGISTRY
        assert "aster" in WS_CLIENT_REGISTRY
        assert "hyperliquid" in WS_CLIENT_REGISTRY

    def test_creates_bitget_client(self):
        dp, rt, _ = _make_data_plane()
        client = create_ws_client("bitget", "BTCUSDT", dp)
        assert isinstance(client, BitgetL2WsClient)

    def test_creates_gate_client(self):
        dp, rt, _ = _make_data_plane()
        client = create_ws_client("gate", "BTCUSDT", dp)
        assert isinstance(client, GateL2WsClient)

    def test_creates_aster_client(self):
        dp, rt, _ = _make_data_plane()
        client = create_ws_client("aster", "BTCUSDT", dp)
        assert isinstance(client, AsterL2WsClient)

    def test_creates_hyperliquid_client(self):
        dp, rt, _ = _make_data_plane()
        client = create_ws_client("hyperliquid", "BTCUSDT", dp)
        assert isinstance(client, HyperliquidL2WsClient)
        # HL: canonical BTCUSDT → wire BTC
        assert client.wire_symbol == "BTC"
        assert client.symbol == "BTCUSDT"


# ---------------------------------------------------------------------------
# Data plane ingest_external_update
# ---------------------------------------------------------------------------


class TestDataPlaneIngestExternalUpdate:
    def test_ingest_routes_to_runtime_record_update(self):
        dp, rt, _ = _make_data_plane()

        update = LocalL2Update(
            venue="binance",
            symbol="BTCUSDT",
            bids=[PriceLevel(price=50000.0, quantity=1.0)],
            asks=[PriceLevel(price=50100.0, quantity=1.0)],
            sequence=42,
            update_kind=LocalL2UpdateKind.SNAPSHOT,
        )

        events = dp.ingest_external_update(update, now_ms=1000)
        assert len(events) > 0

        book = rt.get_book("binance", "BTCUSDT")
        assert book is not None
        assert book.sequence == 42
        assert book.best_bid() == 50000.0

    def test_ingest_multiple_updates_across_venues(self):
        dp, rt, _ = _make_data_plane()

        binance_update = LocalL2Update(
            venue="binance", symbol="BTCUSDT",
            bids=[PriceLevel(price=50000.0, quantity=1.0)],
            asks=[PriceLevel(price=50100.0, quantity=1.0)],
            sequence=1,
            update_kind=LocalL2UpdateKind.SNAPSHOT,
        )
        bybit_update = LocalL2Update(
            venue="bybit", symbol="ETHUSDT",
            bids=[PriceLevel(price=3000.0, quantity=1.0)],
            asks=[PriceLevel(price=3010.0, quantity=1.0)],
            sequence=1,
            update_kind=LocalL2UpdateKind.SNAPSHOT,
        )

        dp.ingest_external_update(binance_update, now_ms=1000)
        dp.ingest_external_update(bybit_update, now_ms=1000)

        bnb_book = rt.get_book("binance", "BTCUSDT")
        bybit_book = rt.get_book("bybit", "ETHUSDT")
        assert bnb_book is not None
        assert bybit_book is not None
        assert bnb_book.best_bid() == 50000.0
        assert bybit_book.best_bid() == 3000.0


# ---------------------------------------------------------------------------
# Data plane WS streaming lifecycle
# ---------------------------------------------------------------------------


class TestDataPlaneWsStreams:
    def test_start_ws_streams_returns_started_count(self):
        dp, rt, _ = _make_data_plane()

        started = dp.start_ws_streams("binance", ["BTCUSDT"])
        # WS clients are created but not connected (no event loop in sync test)
        assert started == 1
        assert dp.active_ws_stream_count == 0  # Not connected yet

    def test_start_ws_streams_skips_duplicates(self):
        dp, rt, _ = _make_data_plane()

        dp.start_ws_streams("binance", ["BTCUSDT"])
        started = dp.start_ws_streams("binance", ["BTCUSDT"])
        assert started == 0  # Already started

    def test_ws_stream_state_exposes_registration_connection_and_freshness(self):
        dp, rt, _ = _make_data_plane()

        assert dp.ws_stream_state("bybit", "BTCUSDT")["registered"] is False

        dp.start_ws_streams("bybit", ["BTCUSDT"])
        dp.note_ws_subscription_confirmed("bybit", "BTCUSDT", now_ms=1000)
        dp.note_ws_delta("bybit", "BTCUSDT", now_ms=1100)

        state = dp.ws_stream_state("bybit", "BTCUSDT")
        assert state["registered"] is True
        assert state["connected"] is False
        assert state["freshness_state_present"] is True
        assert state["last_subscription_confirmed_ms"] == 1000
        assert state["last_ws_delta_ms"] == 1100

    def test_start_ws_streams_ignores_unregistered_venues(self):
        dp, rt, _ = _make_data_plane()

        started = dp.start_ws_streams("unknown_venue", ["BTC"])
        assert started == 0

    def test_diagnostics_includes_ws_counts(self):
        dp, rt, _ = _make_data_plane()

        dp.start_ws_streams("binance", ["BTCUSDT", "ETHUSDT"])
        diag = dp.diagnostics_snapshot()
        assert diag["ws_stream_count"] == 2
        assert diag["ws_connected_count"] == 0  # Not connected (no event loop)


# ---------------------------------------------------------------------------
# Interface boundary: no _transport access in production paths
# ---------------------------------------------------------------------------


class TestNoTransportAccessInProductionPaths:
    """Verify that production paths never reach into adapter._transport directly."""

    def test_bootstrap_book_uses_adapter_not_transport(self):
        """bootstrap_book() accepts an adapter, not a raw transport."""
        dp, rt, _ = _make_data_plane()

        class FakeAdapter:
            async def fetch_l2_snapshot(self, symbol, depth=50):
                return LocalL2Update(
                    venue="test",
                    symbol=symbol,
                    bids=[PriceLevel(price=100, quantity=1)],
                    asks=[PriceLevel(price=101, quantity=1)],
                    sequence=1,
                    update_kind=LocalL2UpdateKind.SNAPSHOT,
                )

        adapter = FakeAdapter()
        success = asyncio.run(
            dp.bootstrap_book("test", "BTCUSDT", adapter, depth=50, now_ms=1000)
        )
        assert success
        book = rt.get_book("test", "BTCUSDT")
        assert book is not None
        assert book.best_bid() == 100

    def test_sync_snapshots_does_not_access_transport_directly(self):
        """sync_snapshots() calls adapter.fetch_l2_snapshot(), not adapter._transport."""
        dp, rt, _ = _make_data_plane()
        from lightfee.core.domain import Venue

        from lightfee.marketdata.l2 import L2PoolAssignment
        book = rt.ensure_book("binance", "BTCUSDT")
        book.pool = L2PoolAssignment.RETAINED

        class NoTransportAttr:
            """Adapter that has fetch_l2_snapshot() but NO _transport attribute."""
            async def fetch_l2_snapshot(self, symbol, depth=50):
                return LocalL2Update(
                    venue="binance",
                    symbol=symbol,
                    bids=[PriceLevel(price=50000, quantity=1)],
                    asks=[PriceLevel(price=50100, quantity=1)],
                    sequence=1,
                    update_kind=LocalL2UpdateKind.SNAPSHOT,
                )

        adapter = NoTransportAttr()
        dispatched = asyncio.run(
            dp.sync_snapshots(adapters={Venue.BINANCE: adapter}, now_ms=1000)
        )
        assert dispatched >= 1


# ---------------------------------------------------------------------------
# Worker categories (V1: ws_worker_categories)
# ---------------------------------------------------------------------------


class TestWorkerCategories:
    """V1 parity: worker category diagnostics on LocalL2DataPlane."""

    def test_empty_worker_categories_when_no_streams(self):
        dp, rt, _ = _make_data_plane()
        cats = dp.ws_worker_categories()
        assert cats == []

    def test_single_worker_category(self):
        dp, rt, _ = _make_data_plane()
        client = BinanceL2WsClient(
            venue="binance", symbol="BTCUSDT", data_plane=dp,
        )
        dp.start_worker(LocalL2BookKey(venue="binance", symbol="BTCUSDT"), client)
        cats = dp.ws_worker_categories()
        assert len(cats) == 1
        assert cats[0]["venue"] == "binance"
        assert cats[0]["category"] == "market_local_l2"
        assert cats[0]["active_count"] == 1
        assert cats[0]["expected_max"] == 1
        assert cats[0]["risk_relevant"] is True

    def test_multiple_venues_separate_categories(self):
        dp, rt, _ = _make_data_plane()
        dp.start_worker(
            LocalL2BookKey(venue="binance", symbol="BTCUSDT"),
            BinanceL2WsClient(venue="binance", symbol="BTCUSDT", data_plane=dp),
        )
        dp.start_worker(
            LocalL2BookKey(venue="binance", symbol="ETHUSDT"),
            BinanceL2WsClient(venue="binance", symbol="ETHUSDT", data_plane=dp),
        )
        dp.start_worker(
            LocalL2BookKey(venue="okx", symbol="BTCUSDT"),
            OkxL2WsClient(venue="okx", symbol="BTCUSDT", data_plane=dp, venue_symbol="BTC-USDT-SWAP"),
        )
        cats = dp.ws_worker_categories()
        venues = {c["venue"]: c["active_count"] for c in cats}
        assert venues == {"binance": 2, "okx": 1}

    def test_suspicious_worker_count_false_when_exact(self):
        dp, rt, _ = _make_data_plane()
        dp.start_worker(
            LocalL2BookKey(venue="binance", symbol="BTCUSDT"),
            BinanceL2WsClient(venue="binance", symbol="BTCUSDT", data_plane=dp),
        )
        assert not dp.suspicious_worker_count()

    def test_diagnostics_snapshot_includes_worker_categories(self):
        dp, rt, _ = _make_data_plane()
        dp.start_worker(
            LocalL2BookKey(venue="binance", symbol="BTCUSDT"),
            BinanceL2WsClient(venue="binance", symbol="BTCUSDT", data_plane=dp),
        )
        diag = dp.diagnostics_snapshot()
        assert "ws_worker_categories" in diag
        assert "suspicious_worker_count" in diag
        assert diag["ws_stream_count"] == 1


class TestWorkerLifecycle:
    """V1 parity: explicit worker lifecycle (start/stop/abort)."""

    def test_start_worker_registers_client(self):
        dp, rt, _ = _make_data_plane()
        key = LocalL2BookKey(venue="binance", symbol="BTCUSDT")
        client = BinanceL2WsClient(venue="binance", symbol="BTCUSDT", data_plane=dp)
        dp.start_worker(key, client)
        assert len(dp._ws_clients) == 1  # registered in clients dict
        assert dp._ws_clients[key] is client

    def test_start_worker_idempotent(self):
        dp, rt, _ = _make_data_plane()
        key = LocalL2BookKey(venue="binance", symbol="BTCUSDT")
        client = BinanceL2WsClient(venue="binance", symbol="BTCUSDT", data_plane=dp)
        dp.start_worker(key, client)
        dp.start_worker(key, client)
        # Still only one worker — no duplicate
        assert len(dp._ws_clients) == 1

    def test_stop_worker_unregisters(self):
        dp, rt, _ = _make_data_plane()
        key = LocalL2BookKey(venue="binance", symbol="BTCUSDT")
        client = BinanceL2WsClient(venue="binance", symbol="BTCUSDT", data_plane=dp)
        dp.start_worker(key, client)
        removed = dp.stop_worker(key)
        assert removed
        assert len(dp._ws_clients) == 0

    def test_stop_worker_missing_returns_false(self):
        dp, rt, _ = _make_data_plane()
        removed = dp.stop_worker(LocalL2BookKey(venue="binance", symbol="BTCUSDT"))
        assert not removed

    def test_abort_workers_clears_all(self):
        dp, rt, _ = _make_data_plane()
        dp.start_worker(
            LocalL2BookKey(venue="binance", symbol="BTCUSDT"),
            BinanceL2WsClient(venue="binance", symbol="BTCUSDT", data_plane=dp),
        )
        dp.start_worker(
            LocalL2BookKey(venue="okx", symbol="ETHUSDT"),
            OkxL2WsClient(venue="okx", symbol="ETHUSDT", data_plane=dp, venue_symbol="ETH-USDT-SWAP"),
        )
        assert len(dp._ws_clients) == 2
        count = dp.abort_workers()
        assert count >= 0  # abort counts tasks, which may be 0 if no start()
        assert len(dp._ws_clients) == 0  # clients dict is cleared regardless

    def test_ws_connect_has_open_timeout(self):
        """WebSocket connection setup must be bounded (V1: OS TCP timeout can be 60+ s)."""
        assert LocalL2WsClient.OPEN_TIMEOUT_SECONDS == 10.0
        # Verify subclasses inherit the constant
        assert BinanceL2WsClient.OPEN_TIMEOUT_SECONDS == 10.0
        assert OkxL2WsClient.OPEN_TIMEOUT_SECONDS == 10.0
        assert HyperliquidL2WsClient.OPEN_TIMEOUT_SECONDS == 10.0


# ---------------------------------------------------------------------------
# Task 4: Bybit WS snapshot authoritative reset test
# ---------------------------------------------------------------------------


class TestBybitWsSnapshotAuthoritativeReset:
    def test_bybit_ws_snapshot_is_authoritative_reset(self):
        dp, rt, journal = _make_data_plane()
        try:
            client = BybitL2WsClient(venue="bybit", symbol="IRYSUSDT", data_plane=dp)
            first = client.parse_depth_message({
                "topic": "orderbook.50.IRYSUSDT",
                "type": "snapshot",
                "ts": 1779302500000,
                "data": {"s": "IRYSUSDT", "b": [["0.0200", "1000"]], "a": [["0.0201", "1000"]], "u": 13700598, "seq": 7103120},
            })
            second = client.parse_depth_message({
                "topic": "orderbook.50.IRYSUSDT",
                "type": "snapshot",
                "ts": 1779302501000,
                "data": {"s": "IRYSUSDT", "b": [["0.0199", "900"]], "a": [["0.0202", "1100"]], "u": 1, "seq": 7103200},
            })

            assert first.update_kind.value == "snapshot"
            assert second.update_kind.value == "snapshot"
            assert second.sequence == 1
        finally:
            _close_data_plane(dp, journal)
