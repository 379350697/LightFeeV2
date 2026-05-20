"""Venue L2 normalization fixtures — per-exchange sequence/checksum/depth tests.

Rust V1 reference: src/market_gateway/venue_rules.rs
"""

from __future__ import annotations

import pytest

from lightfee.marketdata.local_l2_venues import (
    BootstrapMode,
    ChecksumMode,
    LocalL2VenueRules,
    SequenceMode,
    VENUE_RULES,
    get_venue_rules,
)


class TestVenueRulesExist:
    def test_all_seven_venues_have_rules(self):
        for venue in ["binance", "aster", "okx", "bybit", "bitget", "gate", "hyperliquid"]:
            assert venue in VENUE_RULES, f"Missing rules for {venue}"

    def test_get_venue_rules_returns_default_for_unknown(self):
        rules = get_venue_rules("unknown_venue")
        assert rules.venue == "unknown_venue"
        assert rules.default_depth == 50


class TestBinanceRules:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.rules = VENUE_RULES["binance"]

    def test_incremental_sequence(self):
        assert self.rules.sequence_mode == SequenceMode.INCREMENTAL

    def test_no_checksum(self):
        assert self.rules.checksum_mode == ChecksumMode.NONE
        assert not self.rules.should_verify_checksum()

    def test_default_depth_50(self):
        assert self.rules.default_depth == 50

    def test_symbol_uppercase(self):
        assert self.rules.normalise_symbol("btcusdt") == "BTCUSDT"

    def test_rebuild_on_gap(self):
        assert self.rules.should_rebuild_on_sequence_gap(gap=100)


class TestOKXRules:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.rules = VENUE_RULES["okx"]

    def test_checksum_mode_okx_crc32(self):
        assert self.rules.checksum_mode == ChecksumMode.OKX_CRC32
        assert self.rules.should_verify_checksum()

    def test_checksum_on_each_update(self):
        assert self.rules.checksum_on_each_update

    def test_max_sequence_gap_2(self):
        assert self.rules.max_sequence_gap == 2
        # gap <=2 should NOT trigger rebuild
        assert not self.rules.should_rebuild_on_sequence_gap(gap=1)
        assert not self.rules.should_rebuild_on_sequence_gap(gap=2)
        # gap >2 SHOULD trigger rebuild
        assert self.rules.should_rebuild_on_sequence_gap(gap=3)


class TestBybitRules:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.rules = VENUE_RULES["bybit"]

    def test_incremental_sequence(self):
        assert self.rules.sequence_mode == SequenceMode.INCREMENTAL

    def test_max_sequence_gap_2(self):
        assert self.rules.max_sequence_gap == 2

    def test_no_checksum(self):
        assert self.rules.checksum_mode == ChecksumMode.NONE


class TestGateRules:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.rules = VENUE_RULES["gate"]

    def test_timestamp_sequence_mode(self):
        assert self.rules.sequence_mode == SequenceMode.TIMESTAMP

    def test_no_checksum(self):
        assert self.rules.checksum_mode == ChecksumMode.NONE


class TestHyperliquidRules:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.rules = VENUE_RULES["hyperliquid"]

    def test_no_sequence(self):
        assert self.rules.sequence_mode == SequenceMode.NONE

    def test_snapshot_required(self):
        assert self.rules.bootstrap_mode == BootstrapMode.SNAPSHOT_REQUIRED

    def test_symbol_uppercase(self):
        assert self.rules.normalise_symbol("btc-usdt") == "BTC-USDT"


class TestAsterRules:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.rules = VENUE_RULES["aster"]

    def test_shallow_depth(self):
        assert self.rules.default_depth == 20

    def test_no_sequence(self):
        assert self.rules.sequence_mode == SequenceMode.NONE


class TestBitgetRules:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.rules = VENUE_RULES["bitget"]

    def test_incremental_sequence(self):
        assert self.rules.sequence_mode == SequenceMode.INCREMENTAL

    def test_default_depth_50(self):
        assert self.rules.default_depth == 50


class TestSymbolNormalization:
    @pytest.mark.parametrize("mode,input_val,expected", [
        ("uppercase", "btcusdt", "BTCUSDT"),
        ("lowercase", "BTCUSDT", "btcusdt"),
        ("strip_slash", "BTC/USDT", "BTCUSDT"),
        ("strip_slash", "btc/usdt", "BTCUSDT"),
        ("uppercase", "AlreadyUpper", "ALREADYUPPER"),
    ])
    def test_normalization(self, mode, input_val, expected):
        rules = LocalL2VenueRules(venue="test", symbol_normalize=mode)
        assert rules.normalise_symbol(input_val) == expected


# ---------------------------------------------------------------------------
# Venue L2 payload parser tests — snapshot, delta, malformed per venue
# ---------------------------------------------------------------------------


class TestParseBinanceL2Update:
    def test_parse_snapshot(self):
        from lightfee.marketdata.local_l2_venues import parse_binance_l2_update
        payload = {
            "lastUpdateId": 1027024,
            "bids": [["50000.00", "1.500"], ["49900.00", "2.000"]],
            "asks": [["50100.00", "1.200"], ["50200.00", "3.000"]],
        }
        update = parse_binance_l2_update(payload, symbol="BTCUSDT", now_ms=9000)
        assert update.update_kind.value == "snapshot"
        assert update.venue == "binance"
        assert update.symbol == "BTCUSDT"
        assert update.sequence == 1027024
        assert len(update.bids) == 2
        assert update.bids[0].price == 50000.0
        assert update.bids[0].quantity == 1.5
        assert len(update.asks) == 2
        assert update.asks[0].price == 50100.0
        assert update.asks[0].quantity == 1.2

    def test_parse_delta(self):
        from lightfee.marketdata.local_l2_venues import parse_binance_l2_update
        payload = {
            "E": 1234567890,
            "s": "BTCUSDT",
            "u": 1027025,
            "pu": 1027024,
            "bids": [["50010.00", "0.500"]],
            "asks": [],
        }
        update = parse_binance_l2_update(payload, now_ms=9000)
        assert update.update_kind.value == "delta"
        assert update.sequence == 1027025
        assert update.previous_sequence == 1027024
        assert update.event_time_ms == 1234567890
        assert len(update.bids) == 1
        assert len(update.asks) == 0

    def test_zero_qty_filtered(self):
        from lightfee.marketdata.local_l2_venues import parse_binance_l2_update
        payload = {
            "lastUpdateId": 1,
            "bids": [["50000.00", "0.000"], ["49900.00", "1.000"]],
            "asks": [["50100.00", "0.000"]],
        }
        update = parse_binance_l2_update(payload, symbol="BTCUSDT")
        assert len(update.bids) == 1  # zero qty filtered
        assert len(update.asks) == 0


class TestParseOKXL2Update:
    def test_parse_snapshot(self):
        from lightfee.marketdata.local_l2_venues import parse_okx_l2_update
        payload = {
            "arg": {"instId": "BTC-USDT-SWAP"},
            "action": "snapshot",
            "data": [{
                "bids": [["50000.00", "1.500", "0", "1"],
                         ["49900.00", "2.000", "0", "1"]],
                "asks": [["50100.00", "1.200", "0", "1"]],
                "seqId": 12345,
                "ts": "1680000000000",
                "checksum": 123456,
            }],
        }
        update = parse_okx_l2_update(payload, now_ms=9000)
        assert update.update_kind.value == "snapshot"
        assert update.venue == "okx"
        assert update.symbol == "BTC-USDT-SWAP"
        assert update.sequence == 12345
        assert update.checksum == 123456
        assert update.event_time_ms == 1680000000000
        assert len(update.bids) == 2
        assert len(update.asks) == 1

    def test_parse_delta(self):
        from lightfee.marketdata.local_l2_venues import parse_okx_l2_update
        payload = {
            "arg": {"instId": "ETH-USDT-SWAP"},
            "action": "update",
            "data": [{
                "bids": [["2000.00", "0.000", "0", "1"]],
                "asks": [["2005.00", "3.500", "0", "1"]],
                "seqId": 12346,
                "ts": "1680000001000",
                "checksum": 0,
            }],
        }
        update = parse_okx_l2_update(payload, now_ms=9000)
        assert update.update_kind.value == "delta"
        assert update.sequence == 12346
        assert len(update.bids) == 0  # zero qty filtered

    def test_missing_data_raises(self):
        from lightfee.marketdata.local_l2_venues import parse_okx_l2_update
        import pytest
        with pytest.raises(ValueError, match="missing data array"):
            parse_okx_l2_update({"action": "snapshot", "data": []}, now_ms=9000)


class TestParseBybitL2Update:
    def test_parse_snapshot(self):
        from lightfee.marketdata.local_l2_venues import parse_bybit_l2_update
        payload = {
            "topic": "orderbook.200.BTCUSDT",
            "type": "snapshot",
            "data": {
                "s": "BTCUSDT",
                "b": [["50000.00", "1.500"], ["49900.00", "2.000"]],
                "a": [["50100.00", "1.200"]],
                "u": 555,
                "seq": 555,
                "ts": 1680000000000,
            },
        }
        update = parse_bybit_l2_update(payload, now_ms=9000)
        assert update.update_kind.value == "snapshot"
        assert update.venue == "bybit"
        assert update.symbol == "BTCUSDT"
        assert update.sequence == 555
        assert update.event_time_ms == 1680000000000
        assert len(update.bids) == 2

    def test_parse_rest_result_snapshot(self):
        from lightfee.marketdata.local_l2_venues import parse_bybit_l2_update
        payload = {
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "s": "BTCUSDT",
                "b": [["65485.47", "47.081829"]],
                "a": [["65557.7", "16.606555"]],
                "ts": 1716863719031,
                "u": 230704,
                "seq": 1432604333,
                "cts": 1716863718905,
            },
            "time": 1716863719382,
        }

        update = parse_bybit_l2_update(payload, now_ms=9000)

        assert update.update_kind.value == "snapshot"
        assert update.symbol == "BTCUSDT"
        assert update.sequence == 230704
        assert update.event_time_ms == 1716863719031
        assert update.bids[0].price == 65485.47
        assert update.asks[0].price == 65557.7

    def test_parse_delta(self):
        from lightfee.marketdata.local_l2_venues import parse_bybit_l2_update
        payload = {
            "topic": "orderbook.200.BTCUSDT",
            "type": "delta",
            "data": {
                "s": "BTCUSDT",
                "b": [["50010.00", "0.500"]],
                "a": [],
                "seq": 556,
                "ts": 1680000001000,
            },
        }
        update = parse_bybit_l2_update(payload, now_ms=9000)
        assert update.update_kind.value == "delta"
        assert update.sequence == 556

    def test_zero_qty_filtered(self):
        from lightfee.marketdata.local_l2_venues import parse_bybit_l2_update
        payload = {
            "type": "snapshot",
            "data": {
                "s": "BTCUSDT",
                "b": [["50000.00", "0.000"]],
                "a": [["50100.00", "0.000"]],
                "seq": 1,
            },
        }
        update = parse_bybit_l2_update(payload)
        assert len(update.bids) == 0
        assert len(update.asks) == 0


class TestParseBitgetL2Update:
    def test_parse_snapshot(self):
        from lightfee.marketdata.local_l2_venues import parse_bitget_l2_update
        payload = {
            "action": "snapshot",
            "arg": {"instId": "BTCUSDT"},
            "data": [{
                "bids": [["50000.00", "1.500"], ["49900.00", "2.000"]],
                "asks": [["50100.00", "1.200"]],
                "seq": 789,
                "pseq": 0,
                "checksum": 12345,
                "ts": "1680000000000",
            }],
        }
        update = parse_bitget_l2_update(payload, now_ms=9000)
        assert update.update_kind.value == "snapshot"
        assert update.venue == "bitget"
        assert update.symbol == "BTCUSDT"
        assert update.sequence == 789
        assert update.previous_sequence == 0
        assert update.checksum == 12345
        assert len(update.bids) == 2

    def test_parse_delta(self):
        from lightfee.marketdata.local_l2_venues import parse_bitget_l2_update
        payload = {
            "action": "update",
            "arg": {"instId": "ETHUSDT"},
            "data": [{
                "bids": [["2000.00", "5.000"]],
                "asks": [],
                "seq": 790,
                "pseq": 789,
                "ts": "1680000001000",
            }],
        }
        update = parse_bitget_l2_update(payload)
        assert update.update_kind.value == "delta"
        assert update.symbol == "ETHUSDT"
        assert update.sequence == 790
        assert update.previous_sequence == 789

    def test_missing_data_raises(self):
        from lightfee.marketdata.local_l2_venues import parse_bitget_l2_update
        import pytest
        with pytest.raises(ValueError, match="missing data array"):
            parse_bitget_l2_update({"action": "snapshot", "data": []})


class TestParseGateL2Update:
    def test_parse_snapshot(self):
        from lightfee.marketdata.local_l2_venues import parse_gate_l2_update
        payload = {
            "id": 123,
            "t": 1680000000000,
            "contract": "BTC_USDT",
            "bids": [{"p": "50000.00", "s": 1.5}, {"p": "49900.00", "s": 2.0}],
            "asks": [{"p": "50100.00", "s": 1.2}],
        }
        update = parse_gate_l2_update(payload, now_ms=9000)
        assert update.update_kind.value == "snapshot"
        assert update.venue == "gate"
        # Gate normalizes _ to empty and uppercases
        assert update.symbol == "BTCUSDT"
        assert update.sequence == 1680000000000
        assert len(update.bids) == 2
        assert update.bids[0].price == 50000.0

    def test_parse_delta(self):
        from lightfee.marketdata.local_l2_venues import parse_gate_l2_update
        payload = {
            "event": "update",
            "t": 1680000001000,
            "contract": "ETH_USDT",
            "bids": [{"p": "2000.00", "s": 5.0}],
            "asks": [],
        }
        update = parse_gate_l2_update(payload)
        assert update.update_kind.value == "delta"
        assert update.sequence == 1680000001000

    def test_array_level_format(self):
        from lightfee.marketdata.local_l2_venues import parse_gate_l2_update
        payload = {
            "t": 1680000000000,
            "contract": "BTC_USDT",
            "bids": [["50000.00", "1.500"]],
            "asks": [["50100.00", "1.000"]],
        }
        update = parse_gate_l2_update(payload)
        assert len(update.bids) == 1
        assert update.bids[0].price == 50000.0

    def test_zero_qty_filtered(self):
        from lightfee.marketdata.local_l2_venues import parse_gate_l2_update
        payload = {
            "t": 1680000000000,
            "contract": "BTC_USDT",
            "bids": [{"p": "50000.00", "s": 0.0}],
            "asks": [{"p": "50100.00", "s": 0.0}],
        }
        update = parse_gate_l2_update(payload)
        assert len(update.bids) == 0
        assert len(update.asks) == 0


class TestParseHyperliquidL2Update:
    def test_parse_snapshot(self):
        from lightfee.marketdata.local_l2_venues import parse_hyperliquid_l2_update
        payload = {
            "channel": "l2Book",
            "data": {
                "coin": "BTC",
                "time": 1680000000000,
                "levels": [
                    [{"px": "50000.00", "sz": "1.500"},
                     {"px": "49900.00", "sz": "2.000"}],
                    [{"px": "50100.00", "sz": "1.200"}],
                ],
            },
        }
        update = parse_hyperliquid_l2_update(payload, symbol="BTC", now_ms=9000)
        assert update.update_kind.value == "snapshot"
        assert update.venue == "hyperliquid"
        assert update.symbol == "BTC"
        assert update.sequence == 0  # Hyperliquid has no sequence
        assert len(update.bids) == 2
        assert len(update.asks) == 1
        assert update.bids[0].price == 50000.0
        assert update.event_time_ms == 1680000000000

    def test_empty_levels(self):
        from lightfee.marketdata.local_l2_venues import parse_hyperliquid_l2_update
        payload = {
            "data": {
                "coin": "BTC",
                "levels": [],
            },
        }
        update = parse_hyperliquid_l2_update(payload)
        assert len(update.bids) == 0
        assert len(update.asks) == 0

    def test_rest_api_format(self):
        from lightfee.marketdata.local_l2_venues import parse_hyperliquid_l2_update
        payload = {
            "levels": [
                [{"px": "50000.00", "sz": "1.500"}],
                [{"px": "50100.00", "sz": "1.200"}],
            ],
        }
        update = parse_hyperliquid_l2_update(payload, symbol="BTC")
        assert len(update.bids) == 1
        assert len(update.asks) == 1


class TestParseAsterL2Update:
    def test_parse_snapshot(self):
        from lightfee.marketdata.local_l2_venues import parse_aster_l2_update
        payload = {
            "lastUpdateId": 555,
            "bids": [["50000.00", "1.500"]],
            "asks": [["50100.00", "1.200"]],
        }
        update = parse_aster_l2_update(payload, symbol="BTCUSDT", now_ms=9000)
        assert update.update_kind.value == "snapshot"
        assert update.venue == "aster"
        assert update.symbol == "BTCUSDT"
        assert update.sequence == 555
        assert len(update.bids) == 1

    def test_parse_delta_when_zero_seq(self):
        from lightfee.marketdata.local_l2_venues import parse_aster_l2_update
        payload = {
            "lastUpdateId": 0,
            "bids": [["50010.00", "0.500"]],
            "asks": [],
        }
        update = parse_aster_l2_update(payload)
        assert update.update_kind.value == "delta"

    def test_zero_qty_filtered(self):
        from lightfee.marketdata.local_l2_venues import parse_aster_l2_update
        payload = {
            "lastUpdateId": 1,
            "bids": [["50000.00", "0.000"]],
            "asks": [["50100.00", "0.000"]],
        }
        update = parse_aster_l2_update(payload)
        assert len(update.bids) == 0
        assert len(update.asks) == 0


class TestParseL2UpdateDispatch:
    def test_dispatch_to_correct_parser(self):
        from lightfee.marketdata.local_l2_venues import parse_l2_update
        payload = {
            "lastUpdateId": 1,
            "bids": [["50000.00", "1.000"]],
            "asks": [["50100.00", "1.000"]],
        }
        update = parse_l2_update("binance", payload, symbol="BTCUSDT")
        assert update.venue == "binance"
        assert update.symbol == "BTCUSDT"

    def test_dispatch_okx(self):
        from lightfee.marketdata.local_l2_venues import parse_l2_update
        payload = {
            "arg": {"instId": "BTC-USDT-SWAP"},
            "action": "snapshot",
            "data": [{
                "bids": [["50000.00", "1.000", "0", "1"]],
                "asks": [["50100.00", "1.000", "0", "1"]],
                "seqId": 1, "ts": "1680000000000", "checksum": 0,
            }],
        }
        update = parse_l2_update("okx", payload)
        assert update.venue == "okx"
        assert update.symbol == "BTC-USDT-SWAP"

    def test_unknown_venue_raises(self):
        from lightfee.marketdata.local_l2_venues import parse_l2_update
        import pytest
        with pytest.raises(ValueError, match="No L2 parser for venue: unknown"):
            parse_l2_update("unknown", {})
    def test_no_rebuild_when_disabled(self):
        rules = LocalL2VenueRules(venue="test", reconnect_rebuild_on_gap=False)
        assert not rules.should_rebuild_on_sequence_gap(gap=1000)

    def test_rebuild_when_gap_exceeds_max(self):
        rules = LocalL2VenueRules(venue="test", max_sequence_gap=5, reconnect_rebuild_on_gap=True)
        assert rules.should_rebuild_on_sequence_gap(gap=6)

    def test_no_rebuild_within_gap_limit(self):
        rules = LocalL2VenueRules(venue="test", max_sequence_gap=5, reconnect_rebuild_on_gap=True)
        assert not rules.should_rebuild_on_sequence_gap(gap=3)
