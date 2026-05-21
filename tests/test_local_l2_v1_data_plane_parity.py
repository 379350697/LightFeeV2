from __future__ import annotations

import binascii

from lightfee.marketdata.l2 import (
    L2BookStatus,
    LocalL2Update,
    LocalL2UpdateKind,
    PriceLevel,
)
from lightfee.marketdata.local_l2_data_plane import LocalL2DataPlane
from lightfee.marketdata.local_l2_runtime import LocalL2Runtime
from lightfee.marketdata.local_l2_ws import (
    BinanceL2WsClient,
    BitgetL2WsClient,
    BybitL2WsClient,
    GateL2WsClient,
    HyperliquidL2WsClient,
)
from lightfee.persistence.journal import Journal


def _make_journal(tmp_path):
    journal = Journal(str(tmp_path / "local-l2-parity.jsonl"))
    journal.open()
    return journal


def _make_data_plane(tmp_path):
    runtime = LocalL2Runtime()
    journal = _make_journal(tmp_path)
    return LocalL2DataPlane(runtime, journal), runtime, journal


def _checksum(raw: str) -> int:
    value = binascii.crc32(raw.encode()) & 0xFFFFFFFF
    if value >= 2**31:
        value -= 2**32
    return value


def test_binance_buffered_replay_uses_first_update_range(tmp_path):
    dp, runtime, journal = _make_data_plane(tmp_path)
    try:
        book = runtime.ensure_book("binance", "BTCUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING

        dp.ingest_external_update(
            LocalL2Update(
                venue="binance",
                symbol="BTCUSDT",
                bids=[PriceLevel(100.0, 2.0)],
                asks=[PriceLevel(101.0, 2.0)],
                first_sequence=101,
                sequence=105,
                previous_sequence=0,
                previous_sequence_present=False,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=1000,
        )
        book.apply_snapshot(
            [PriceLevel(99.0, 1.0)],
            [PriceLevel(102.0, 1.0)],
            sequence=100,
            now_ms=900,
        )

        replay = dp._replay_buffered_updates("binance", "BTCUSDT")

        assert replay.ok is True
        assert replay.replayed == 1
        assert runtime.get_book("binance", "BTCUSDT").sequence == 105
    finally:
        journal.close()


def test_aster_pre_snapshot_buffer_overflow_drops_oldest_without_rebuild(tmp_path):
    dp, runtime, journal = _make_data_plane(tmp_path)
    try:
        book = runtime.ensure_book("aster", "IRYSUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING

        for seq in range(1, 4098):
            dp.ingest_external_update(
                LocalL2Update(
                    venue="aster",
                    symbol="IRYSUSDT",
                    bids=[PriceLevel(0.04, 660.0)],
                    asks=[PriceLevel(0.05, 660.0)],
                    first_sequence=seq,
                    sequence=seq,
                    update_kind=LocalL2UpdateKind.DELTA,
                ),
                now_ms=seq,
            )

        key = "aster:IRYSUSDT"
        buffered = dp._pre_snapshot_buffers[key]
        assert book.status == L2BookStatus.BOOTSTRAPPING
        assert len(buffered) == 4096
        assert buffered[0].update.sequence == 2
        assert buffered[-1].update.sequence == 4097
    finally:
        journal.close()


def test_aster_buffered_replay_accepts_previous_link_anchor(tmp_path):
    dp, runtime, journal = _make_data_plane(tmp_path)
    try:
        book = runtime.ensure_book("aster", "IRYSUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING

        dp.ingest_external_update(
            LocalL2Update(
                venue="aster",
                symbol="IRYSUSDT",
                bids=[PriceLevel(0.04, 660.0)],
                asks=[PriceLevel(0.05, 660.0)],
                first_sequence=105,
                sequence=105,
                previous_sequence=100,
                previous_sequence_present=True,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=1000,
        )
        book.apply_snapshot(
            [PriceLevel(0.039, 1.0)],
            [PriceLevel(0.051, 1.0)],
            sequence=100,
            now_ms=900,
        )

        replay = dp._replay_buffered_updates("aster", "IRYSUSDT")

        assert replay.ok is True
        assert replay.replayed == 1
        assert runtime.get_book("aster", "IRYSUSDT").sequence == 105
    finally:
        journal.close()


def test_bybit_parser_and_runtime_follow_pu_only_when_present(tmp_path):
    dp, runtime, journal = _make_data_plane(tmp_path)
    try:
        client = BybitL2WsClient(venue="bybit", symbol="XMRUSDT", data_plane=dp)
        parsed = client.parse_depth_message(
            {
                "topic": "orderbook.50.XMRUSDT",
                "type": "delta",
                "ts": 1779339900000,
                "data": {
                    "s": "XMRUSDT",
                    "b": [["300.0", "1"]],
                    "a": [["301.0", "1"]],
                    "u": 105,
                    "pu": 100,
                },
            }
        )
        assert parsed is not None
        assert parsed.previous_sequence == 100
        assert parsed.previous_sequence_present is True

        book = runtime.ensure_book("bybit", "XMRUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        book.apply_snapshot(
            [PriceLevel(299.0, 1.0)],
            [PriceLevel(302.0, 1.0)],
            sequence=100,
            now_ms=1,
        )
        book.transition_to_hot()

        no_pu = LocalL2Update(
            venue="bybit",
            symbol="XMRUSDT",
            bids=[PriceLevel(300.0, 1.0)],
            asks=[],
            sequence=105,
            previous_sequence=0,
            previous_sequence_present=False,
            update_kind=LocalL2UpdateKind.DELTA,
        )
        result = runtime.record_update_result(no_pu, now_ms=2)

        assert result.applied is True
        assert result.rebuild_required is False
        assert runtime.get_book("bybit", "XMRUSDT").status == L2BookStatus.HOT
        assert runtime.get_book("bybit", "XMRUSDT").sequence == 105
    finally:
        journal.close()


def test_bitget_pseq_zero_marks_snapshot_boundary_rebuild(tmp_path):
    dp, runtime, journal = _make_data_plane(tmp_path)
    try:
        book = runtime.ensure_book("bitget", "BTCUSDT")
        book.status = L2BookStatus.BOOTSTRAPPING
        book.apply_snapshot(
            [PriceLevel(64999.0, 1.0)],
            [PriceLevel(65001.0, 1.0)],
            sequence=42,
            now_ms=1,
        )
        book.transition_to_hot()

        update = LocalL2Update(
            venue="bitget",
            symbol="BTCUSDT",
            bids=[PriceLevel(64998.0, 1.0)],
            asks=[],
            sequence=43,
            previous_sequence=0,
            previous_sequence_present=True,
            update_kind=LocalL2UpdateKind.DELTA,
        )

        result = runtime.record_update_result(update, now_ms=2)

        assert result.applied is False
        assert result.rebuild_required is True
        assert runtime.get_book("bitget", "BTCUSDT").status == L2BookStatus.REBUILDING
        assert runtime.get_book("bitget", "BTCUSDT").sequence == 0
    finally:
        journal.close()


def test_okx_checksum_uses_raw_wire_strings_not_float_normalization(tmp_path):
    dp, runtime, journal = _make_data_plane(tmp_path)
    try:
        raw = "100.0:1.0:101.0:1.0"
        update = LocalL2Update(
            venue="okx",
            symbol="BTCUSDT",
            bids=[PriceLevel(100.0, 1.0)],
            asks=[PriceLevel(101.0, 1.0)],
            raw_bids=[("100.0", "1.0")],
            raw_asks=[("101.0", "1.0")],
            sequence=10,
            checksum=_checksum(raw),
            update_kind=LocalL2UpdateKind.SNAPSHOT,
        )

        result = runtime.record_update_result(update, now_ms=1)

        assert result.applied is True
        assert result.rebuild_required is False
        assert runtime.get_book("okx", "BTCUSDT").status != L2BookStatus.REBUILDING
    finally:
        journal.close()


def test_checksum_mismatch_clears_raw_checksum_book_before_rebuild(tmp_path):
    dp, runtime, journal = _make_data_plane(tmp_path)
    try:
        book = runtime.ensure_book("okx", "BTCUSDT")
        book.transition_to_bootstrapping(now_ms=1)
        ok_checksum = _checksum("100.0:1.0:101.0:1.0")
        runtime.record_update_result(
            LocalL2Update(
                venue="okx",
                symbol="BTCUSDT",
                bids=[PriceLevel(100.0, 1.0)],
                asks=[PriceLevel(101.0, 1.0)],
                raw_bids=[("100.0", "1.0")],
                raw_asks=[("101.0", "1.0")],
                sequence=10,
                checksum=ok_checksum,
                update_kind=LocalL2UpdateKind.SNAPSHOT,
            ),
            now_ms=1,
        )
        book.transition_to_hot()

        result = runtime.record_update_result(
            LocalL2Update(
                venue="okx",
                symbol="BTCUSDT",
                bids=[PriceLevel(100.0, 2.0)],
                asks=[],
                raw_bids=[("100.0", "2.0")],
                raw_asks=[],
                sequence=11,
                previous_sequence=10,
                previous_sequence_present=True,
                checksum=12345,
                update_kind=LocalL2UpdateKind.DELTA,
            ),
            now_ms=2,
        )

        book = runtime.get_book("okx", "BTCUSDT")
        assert result.rebuild_required is True
        assert book.status == L2BookStatus.REBUILDING
        assert book.raw_checksum_bids == []
        assert book.raw_checksum_asks == []
        assert book.pending_snapshot_bridge is False
    finally:
        journal.close()


def test_gate_ws_uses_v1_obu_subscription_and_range_parser(tmp_path):
    dp, _runtime, journal = _make_data_plane(tmp_path)
    try:
        client = GateL2WsClient(
            venue="gate",
            symbol="BTCUSDT",
            venue_symbol="BTC_USDT",
            data_plane=dp,
        )

        assert client.build_subscribe_message()["channel"] == "futures.obu"
        assert client.build_subscribe_message()["payload"] == ["ob.BTC_USDT.400"]

        update = client.parse_depth_message(
            {
                "channel": "futures.obu",
                "data": {
                    "name": "BTC_USDT",
                    "t": 1710001234500,
                    "U": 100,
                    "u": 101,
                    "b": [{"p": "64998.0", "s": "1"}],
                    "a": [{"p": "65002.0", "s": "1"}],
                },
            }
        )

        assert update is not None
        assert update.update_kind == LocalL2UpdateKind.DELTA
        assert update.first_sequence == 100
        assert update.previous_sequence == 100
        assert update.previous_sequence_present is True
        assert update.sequence == 101
    finally:
        journal.close()


def test_hyperliquid_uses_l2book_websocket_not_rest_poller(tmp_path):
    dp, _runtime, journal = _make_data_plane(tmp_path)
    try:
        client = HyperliquidL2WsClient(
            venue="hyperliquid",
            symbol="PROVEUSDT",
            venue_symbol="PROVE",
            data_plane=dp,
        )

        assert client.websocket_url() == "wss://api.hyperliquid.xyz/ws"
        assert client.build_subscribe_message() == {
            "method": "subscribe",
            "subscription": {"type": "l2Book", "coin": "PROVE"},
        }

        update = client.parse_depth_message(
            {
                "channel": "l2Book",
                "data": {
                    "coin": "PROVE",
                    "time": 1779339900000,
                    "levels": [
                        [{"px": "1.00", "sz": "12"}],
                        [{"px": "1.01", "sz": "10"}],
                    ],
                },
            }
        )

        assert update is not None
        assert update.update_kind == LocalL2UpdateKind.SNAPSHOT
        assert update.symbol == "PROVEUSDT"
        assert update.bids[0].price == 1.0
        assert update.asks[0].price == 1.01
    finally:
        journal.close()
