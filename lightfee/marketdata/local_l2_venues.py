"""Venue L2 normalization — per-exchange sequence/checksum/depth rules.

Rust V1 reference: src/market_gateway/local_l2_state_machine.rs
                      src/market_gateway/venue_rules.rs

Each venue has a LocalL2VenueRules profile that controls:
  - default local-L2 depth
  - sequence mode (incremental, timestamp, none)
  - checksum mode (crc32, okx_crc32, none)
  - symbol normalization (venue-specific symbol format)
  - snapshot bootstrap requirement
  - reconnect/rebuild trigger policy
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class SequenceMode(Enum):
    INCREMENTAL = "incremental"
    TIMESTAMP = "timestamp"
    NONE = "none"


class ChecksumMode(Enum):
    CRC32 = "crc32"
    OKX_CRC32 = "okx_crc32"
    NONE = "none"


class BootstrapMode(Enum):
    SNAPSHOT_REQUIRED = "snapshot_required"
    DELTA_OK = "delta_ok"


@dataclass
class LocalL2VenueRules:
    venue: str
    default_depth: int = 50
    sequence_mode: SequenceMode = SequenceMode.INCREMENTAL
    checksum_mode: ChecksumMode = ChecksumMode.NONE
    bootstrap_mode: BootstrapMode = BootstrapMode.SNAPSHOT_REQUIRED
    max_sequence_gap: int = 0
    symbol_normalize: str = "uppercase"  # uppercase, lowercase, strip_slash
    reconnect_rebuild_on_gap: bool = True
    checksum_on_each_update: bool = False

    def normalise_symbol(self, raw: str) -> str:
        if self.symbol_normalize == "uppercase":
            return raw.upper()
        elif self.symbol_normalize == "lowercase":
            return raw.lower()
        elif self.symbol_normalize == "strip_slash":
            return raw.replace("/", "").upper()
        return raw

    def should_rebuild_on_sequence_gap(self, gap: int) -> bool:
        if not self.reconnect_rebuild_on_gap:
            return False
        if self.max_sequence_gap > 0 and gap <= self.max_sequence_gap:
            return False
        return True

    def should_verify_checksum(self) -> bool:
        return self.checksum_mode != ChecksumMode.NONE


# ---------------------------------------------------------------------------
# Per-venue rules — Rust V1 equivalents
# ---------------------------------------------------------------------------


def _binance_rules() -> LocalL2VenueRules:
    return LocalL2VenueRules(
        venue="binance",
        default_depth=50,
        sequence_mode=SequenceMode.INCREMENTAL,
        checksum_mode=ChecksumMode.NONE,
        max_sequence_gap=0,  # Binance sends full snapshot on reconnect
        reconnect_rebuild_on_gap=True,
    )


def _aster_rules() -> LocalL2VenueRules:
    return LocalL2VenueRules(
        venue="aster",
        default_depth=20,
        sequence_mode=SequenceMode.NONE,
        checksum_mode=ChecksumMode.NONE,
        bootstrap_mode=BootstrapMode.SNAPSHOT_REQUIRED,
        reconnect_rebuild_on_gap=True,
    )


def _okx_rules() -> LocalL2VenueRules:
    return LocalL2VenueRules(
        venue="okx",
        default_depth=50,
        sequence_mode=SequenceMode.INCREMENTAL,
        checksum_mode=ChecksumMode.OKX_CRC32,
        max_sequence_gap=2,
        reconnect_rebuild_on_gap=True,
        checksum_on_each_update=True,
    )


def _bybit_rules() -> LocalL2VenueRules:
    return LocalL2VenueRules(
        venue="bybit",
        default_depth=50,
        sequence_mode=SequenceMode.INCREMENTAL,
        checksum_mode=ChecksumMode.NONE,
        max_sequence_gap=2,
        reconnect_rebuild_on_gap=True,
    )


def _bitget_rules() -> LocalL2VenueRules:
    return LocalL2VenueRules(
        venue="bitget",
        default_depth=50,
        sequence_mode=SequenceMode.INCREMENTAL,
        checksum_mode=ChecksumMode.NONE,
        max_sequence_gap=2,
        reconnect_rebuild_on_gap=True,
    )


def _gate_rules() -> LocalL2VenueRules:
    return LocalL2VenueRules(
        venue="gate",
        default_depth=50,
        sequence_mode=SequenceMode.TIMESTAMP,
        checksum_mode=ChecksumMode.NONE,
        max_sequence_gap=0,
        reconnect_rebuild_on_gap=True,
    )


def _hyperliquid_rules() -> LocalL2VenueRules:
    return LocalL2VenueRules(
        venue="hyperliquid",
        default_depth=50,
        sequence_mode=SequenceMode.NONE,
        checksum_mode=ChecksumMode.NONE,
        bootstrap_mode=BootstrapMode.SNAPSHOT_REQUIRED,
        reconnect_rebuild_on_gap=True,
        symbol_normalize="uppercase",
    )


# Registry
VENUE_RULES: dict[str, LocalL2VenueRules] = {
    "binance": _binance_rules(),
    "aster": _aster_rules(),
    "okx": _okx_rules(),
    "bybit": _bybit_rules(),
    "bitget": _bitget_rules(),
    "gate": _gate_rules(),
    "hyperliquid": _hyperliquid_rules(),
}


def get_venue_rules(venue: str) -> LocalL2VenueRules:
    """Get L2 venue rules; returns a default if the venue is unknown."""
    return VENUE_RULES.get(venue, LocalL2VenueRules(venue=venue))


def all_venue_rules() -> dict[str, LocalL2VenueRules]:
    return dict(VENUE_RULES)


# ---------------------------------------------------------------------------
# Per-venue raw payload → LocalL2Update parsers
# ---------------------------------------------------------------------------
# Each parser handles snapshot vs delta, bids/asks normalization,
# sequence/checksum mapping, and malformed payload detection.


def parse_binance_l2_update(
    payload: dict, venue: str = "binance", symbol: str = "", now_ms: int = 0,
) -> LocalL2Update:
    """Parse Binance partial book depth stream or REST snapshot into LocalL2Update.

    Binance format (REST /depth endpoint or WS diff depth):
      {"lastUpdateId": 123, "bids": [["50000", "1.0"], ...], "asks": [["50100", "1.5"], ...]}

    Delta detection: if lastUpdateId < internal sequence, treat as stale (event emitted separately).
    """
    from lightfee.marketdata.l2 import LocalL2Update, LocalL2UpdateKind, PriceLevel

    bids_raw = payload.get("bids", [])
    asks_raw = payload.get("asks", [])
    seq = payload.get("lastUpdateId", payload.get("u", 0))

    bids = [PriceLevel(price=float(b[0]), quantity=float(b[1])) for b in bids_raw if float(b[1]) > 0]
    asks = [PriceLevel(price=float(a[0]), quantity=float(a[1])) for a in asks_raw if float(a[1]) > 0]

    kind = LocalL2UpdateKind.SNAPSHOT if "lastUpdateId" in payload and seq > 0 and not payload.get("E") \
           else LocalL2UpdateKind.DELTA

    return LocalL2Update(
        venue=venue, symbol=symbol or payload.get("s", "").upper(),
        bids=bids, asks=asks,
        sequence=seq, previous_sequence=payload.get("pu", seq - 1),
        event_time_ms=payload.get("E", now_ms), received_at_ms=now_ms,
        update_kind=kind,
    )


def parse_okx_l2_update(
    payload: dict, venue: str = "okx", symbol: str = "", now_ms: int = 0,
) -> LocalL2Update:
    """Parse OKX books / books-l2-tbt snapshot/delta into LocalL2Update.

    OKX format:
      {"arg": {"instId": "BTC-USDT-SWAP"}, "action": "snapshot"/"update",
       "data": [{"bids": [["50000", "1.0", "0", "1"]], "asks": [...],
                 "seqId": 123, "ts": "1234567890", "checksum": 0}]}

    Snapshot vs delta determined by "action" field.
    OKX checksum is verified separately via venue rules (ChecksumMode.OKX_CRC32).
    """
    from lightfee.marketdata.l2 import LocalL2Update, LocalL2UpdateKind, PriceLevel

    data = payload.get("data", [payload] if isinstance(payload, dict) else [])
    if not data:
        raise ValueError("OKX L2 update: missing data array")

    entry = data[0] if isinstance(data, list) else data
    bids_raw = entry.get("bids", [])
    asks_raw = entry.get("asks", [])

    bids = [PriceLevel(price=float(b[0]), quantity=float(b[1])) for b in bids_raw if float(b[1]) > 0]
    asks = [PriceLevel(price=float(a[0]), quantity=float(a[1])) for a in asks_raw if float(a[1]) > 0]

    seq = int(entry.get("seqId", 0))
    prev_seq = payload.get("prevSeqId", seq - 1)
    checksum = entry.get("checksum", 0)

    action = payload.get("action", "update")
    kind = LocalL2UpdateKind.SNAPSHOT if action == "snapshot" else LocalL2UpdateKind.DELTA

    inst = symbol or payload.get("arg", {}).get("instId", "")
    ts = int(entry.get("ts", now_ms))

    return LocalL2Update(
        venue=venue, symbol=inst,
        bids=bids, asks=asks,
        sequence=seq, previous_sequence=prev_seq,
        checksum=checksum,
        event_time_ms=ts, received_at_ms=now_ms,
        update_kind=kind,
    )


def parse_bybit_l2_update(
    payload: dict, venue: str = "bybit", symbol: str = "", now_ms: int = 0,
) -> LocalL2Update:
    """Parse Bybit orderbook snapshot/delta into LocalL2Update.

    Bybit format (WS orderbook.200):
      {"topic": "orderbook.200.BTCUSDT", "type": "snapshot"/"delta",
       "data": {"s": "BTCUSDT", "b": [["50000", "1.0"]], "a": [["50100", "1.5"]],
                "u": 123, "seq": 123, "ts": 1234567890}}
    """
    from lightfee.marketdata.l2 import LocalL2Update, LocalL2UpdateKind, PriceLevel

    data = payload.get("data", payload)
    bids_raw = data.get("b", data.get("bids", []))
    asks_raw = data.get("a", data.get("asks", []))

    bids = [PriceLevel(price=float(b[0]), quantity=float(b[1])) for b in bids_raw if float(b[1]) > 0]
    asks = [PriceLevel(price=float(a[0]), quantity=float(a[1])) for a in asks_raw if float(a[1]) > 0]

    seq = int(data.get("seq", data.get("u", 0)))
    kind = LocalL2UpdateKind.SNAPSHOT if payload.get("type") == "snapshot" else LocalL2UpdateKind.DELTA

    return LocalL2Update(
        venue=venue, symbol=symbol or data.get("s", ""),
        bids=bids, asks=asks,
        sequence=seq, previous_sequence=payload.get("prevSeq", seq - 1),
        event_time_ms=int(data.get("ts", now_ms)), received_at_ms=now_ms,
        update_kind=kind,
    )


def parse_bitget_l2_update(
    payload: dict, venue: str = "bitget", symbol: str = "", now_ms: int = 0,
) -> LocalL2Update:
    """Parse Bitget orderbook snapshot/delta into LocalL2Update.

    Bitget format:
      {"action": "snapshot"/"update", "arg": {"instId": "BTCUSDT"},
       "data": [{"bids": [["50000", "1.0"]], "asks": [["50100", "1.5"]],
                 "seqId": 123, "ts": "1234567890"}]}
    """
    from lightfee.marketdata.l2 import LocalL2Update, LocalL2UpdateKind, PriceLevel

    data = payload.get("data", [])
    if not data:
        raise ValueError("Bitget L2 update: missing data array")
    entry = data[0] if isinstance(data, list) else data

    bids_raw = entry.get("bids", [])
    asks_raw = entry.get("asks", [])

    bids = [PriceLevel(price=float(b[0]), quantity=float(b[1])) for b in bids_raw if float(b[1]) > 0]
    asks = [PriceLevel(price=float(a[0]), quantity=float(a[1])) for a in asks_raw if float(a[1]) > 0]

    seq = int(entry.get("seqId", 0))
    action = payload.get("action", "update")
    kind = LocalL2UpdateKind.SNAPSHOT if action == "snapshot" else LocalL2UpdateKind.DELTA

    return LocalL2Update(
        venue=venue, symbol=symbol or payload.get("arg", {}).get("instId", ""),
        bids=bids, asks=asks,
        sequence=seq, previous_sequence=payload.get("prevSeqId", seq - 1),
        event_time_ms=int(entry.get("ts", now_ms)), received_at_ms=now_ms,
        update_kind=kind,
    )


def parse_gate_l2_update(
    payload: dict, venue: str = "gate", symbol: str = "", now_ms: int = 0,
) -> LocalL2Update:
    """Parse Gate.io orderbook snapshot/delta into LocalL2Update.

    Gate format (REST /futures/usdt/order_book or WS futures.order_book_update):
      {"id": 123, "t": 1234567890, "contract": "BTC_USDT",
       "bids": [{"p": "50000", "s": 1.0}], "asks": [{"p": "50100", "s": 1.5}]}

    Gate uses timestamp-based sequence (SequenceMode.TIMESTAMP).
    """
    from lightfee.marketdata.l2 import LocalL2Update, LocalL2UpdateKind, PriceLevel

    bids_raw = payload.get("bids", [])
    asks_raw = payload.get("asks", [])

    # Gate bids/asks may be dicts {"p": price, "s": size} or arrays [price, size]
    def _parse_level(level):
        if isinstance(level, dict):
            return PriceLevel(price=float(level.get("p", 0)), quantity=float(level.get("s", 0)))
        return PriceLevel(price=float(level[0]), quantity=float(level[1]))

    bids = [_parse_level(b) for b in bids_raw]
    asks = [_parse_level(a) for a in asks_raw]
    bids = [b for b in bids if b.quantity > 0]
    asks = [a for a in asks if a.quantity > 0]

    seq = int(payload.get("t", payload.get("id", 0)))
    kind = LocalL2UpdateKind.SNAPSHOT if not payload.get("event") else LocalL2UpdateKind.DELTA

    return LocalL2Update(
        venue=venue, symbol=symbol or payload.get("contract", "").replace("_", "").upper(),
        bids=bids, asks=asks,
        sequence=seq, previous_sequence=payload.get("prev_t", 0),
        event_time_ms=int(payload.get("t", now_ms)), received_at_ms=now_ms,
        update_kind=kind,
    )


def parse_hyperliquid_l2_update(
    payload: dict, venue: str = "hyperliquid", symbol: str = "", now_ms: int = 0,
) -> LocalL2Update:
    """Parse Hyperliquid L2 book snapshot into LocalL2Update.

    Hyperliquid format (REST info API /l2Book):
      {"levels": [[{"px": "50000", "sz": "1.0"}], [{"px": "50100", "sz": "1.5"}]]}
    or WS format:
      {"channel": "l2Book", "data": {"coin": "BTC", "levels": [[...], [...]],
       "time": 1234567890}}

    Hyperliquid has no sequence numbers — uses SequenceMode.NONE.
    """
    from lightfee.marketdata.l2 import LocalL2Update, LocalL2UpdateKind, PriceLevel

    data = payload.get("data", payload)
    levels = data.get("levels", [])

    def _parse_side(raw_levels: list) -> list[PriceLevel]:
        if not raw_levels:
            return []
        result = []
        for lvl in raw_levels:
            if isinstance(lvl, dict):
                px = float(lvl.get("px", 0))
                sz = float(lvl.get("sz", 0))
            else:
                px = float(lvl.get("px", lvl.get("price", 0)))
                sz = float(lvl.get("sz", lvl.get("size", 0)))
            if sz > 0:
                result.append(PriceLevel(price=px, quantity=sz))
        return result

    bids = _parse_side(levels[0]) if len(levels) > 0 else []
    asks = _parse_side(levels[1]) if len(levels) > 1 else []

    sym = symbol or data.get("coin", "")

    return LocalL2Update(
        venue=venue, symbol=sym,
        bids=bids, asks=asks,
        sequence=0,  # Hyperliquid has no sequence
        event_time_ms=int(data.get("time", now_ms)), received_at_ms=now_ms,
        update_kind=LocalL2UpdateKind.SNAPSHOT,
    )


def parse_aster_l2_update(
    payload: dict, venue: str = "aster", symbol: str = "", now_ms: int = 0,
) -> LocalL2Update:
    """Parse Aster orderbook snapshot into LocalL2Update.

    Aster format mirrors Binance-style depth:
      {"lastUpdateId": 123, "bids": [["50000", "1.0"]], "asks": [["50100", "1.5"]]}
    """
    from lightfee.marketdata.l2 import LocalL2Update, LocalL2UpdateKind, PriceLevel

    bids_raw = payload.get("bids", [])
    asks_raw = payload.get("asks", [])

    bids = [PriceLevel(price=float(b[0]), quantity=float(b[1])) for b in bids_raw if float(b[1]) > 0]
    asks = [PriceLevel(price=float(a[0]), quantity=float(a[1])) for a in asks_raw if float(a[1]) > 0]

    seq = payload.get("lastUpdateId", 0)

    return LocalL2Update(
        venue=venue, symbol=symbol,
        bids=bids, asks=asks,
        sequence=seq, previous_sequence=seq - 1,
        event_time_ms=now_ms, received_at_ms=now_ms,
        update_kind=LocalL2UpdateKind.SNAPSHOT if seq > 0 else LocalL2UpdateKind.DELTA,
    )


# Per-venue parser dispatch table
VENUE_L2_PARSERS: dict[str, callable] = {
    "binance": parse_binance_l2_update,
    "aster": parse_aster_l2_update,
    "okx": parse_okx_l2_update,
    "bybit": parse_bybit_l2_update,
    "bitget": parse_bitget_l2_update,
    "gate": parse_gate_l2_update,
    "hyperliquid": parse_hyperliquid_l2_update,
}


def parse_l2_update(
    venue: str, payload: dict, symbol: str = "", now_ms: int = 0,
) -> LocalL2Update:
    """Parse a raw venue L2 payload into a normalized LocalL2Update.

    Selects the appropriate venue-specific parser.
    Returns LocalL2Update with venue-appropriate fields.
    """
    parser = VENUE_L2_PARSERS.get(venue)
    if parser is None:
        raise ValueError(f"No L2 parser for venue: {venue}")
    return parser(payload, venue=venue, symbol=symbol, now_ms=now_ms)
