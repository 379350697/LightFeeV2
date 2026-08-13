from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


V1_BINANCE_BUFFER_CAP = 4096
LEGACY_GENERIC_BUFFER_CAP = 512


class BridgeMode(Enum):
    REST_SNAPSHOT_BUFFERED_REPLAY = "rest_snapshot_buffered_replay"
    WS_SNAPSHOT_AUTHORITATIVE = "ws_snapshot_authoritative"
    REST_POLLING_SNAPSHOT_ONLY = "rest_polling_snapshot_only"
    STREAM_ONLY = "stream_only"


class ReplayLinkKind(Enum):
    NORMAL = "normal"
    KEEPALIVE = "keepalive"
    RESET = "reset"
    OBSOLETE = "obsolete"
    INVALID = "invalid"


def sequence_range_overlaps_expected(
    first_sequence: int, sequence: int, expected_sequence: int,
) -> bool:
    """Whether a ranged delta contains the next sequence required locally."""
    return (
        first_sequence > 0
        and sequence > 0
        and first_sequence <= expected_sequence <= sequence
    )


@dataclass(frozen=True)
class LocalL2VenuePolicy:
    venue: str
    bridge_mode: BridgeMode
    pre_snapshot_buffer_cap: int
    rest_snapshot_sequence_comparable: bool = True
    replay_rest_snapshot_with_ws_deltas: bool = True
    allows_overlapping_previous_link: bool = False

    def classify_replay_link(
        self,
        *,
        previous_sequence: int,
        sequence: int,
        previous_sequence_from_update: int,
        bid_count: int,
        ask_count: int,
    ) -> ReplayLinkKind:
        if self.venue == "okx":
            if previous_sequence_from_update == previous_sequence and sequence == previous_sequence and bid_count == 0 and ask_count == 0:
                return ReplayLinkKind.KEEPALIVE
            if previous_sequence_from_update == previous_sequence and sequence < previous_sequence:
                return ReplayLinkKind.RESET
            if previous_sequence_from_update == previous_sequence and sequence > previous_sequence:
                return ReplayLinkKind.NORMAL
            if sequence <= previous_sequence:
                return ReplayLinkKind.OBSOLETE
            return ReplayLinkKind.INVALID

        if previous_sequence_from_update > 0:
            if previous_sequence_from_update == previous_sequence:
                return ReplayLinkKind.NORMAL
            if sequence <= previous_sequence:
                return ReplayLinkKind.OBSOLETE
            return ReplayLinkKind.INVALID

        if sequence <= previous_sequence:
            return ReplayLinkKind.OBSOLETE
        return ReplayLinkKind.NORMAL


def policy_for_venue(venue: str) -> LocalL2VenuePolicy:
    normalized = str(venue).lower()
    if normalized == "binance":
        return LocalL2VenuePolicy(
            venue=normalized,
            bridge_mode=BridgeMode.REST_SNAPSHOT_BUFFERED_REPLAY,
            pre_snapshot_buffer_cap=V1_BINANCE_BUFFER_CAP,
        )
    if normalized == "aster":
        return LocalL2VenuePolicy(
            venue=normalized,
            bridge_mode=BridgeMode.REST_SNAPSHOT_BUFFERED_REPLAY,
            pre_snapshot_buffer_cap=V1_BINANCE_BUFFER_CAP,
            allows_overlapping_previous_link=True,
        )
    if normalized == "okx":
        return LocalL2VenuePolicy(
            venue=normalized,
            bridge_mode=BridgeMode.REST_SNAPSHOT_BUFFERED_REPLAY,
            pre_snapshot_buffer_cap=V1_BINANCE_BUFFER_CAP,
        )
    if normalized == "hyperliquid":
        return LocalL2VenuePolicy(
            venue=normalized,
            bridge_mode=BridgeMode.STREAM_ONLY,
            pre_snapshot_buffer_cap=0,
            rest_snapshot_sequence_comparable=False,
            replay_rest_snapshot_with_ws_deltas=False,
        )
    if normalized == "bybit":
        return LocalL2VenuePolicy(
            venue=normalized,
            bridge_mode=BridgeMode.WS_SNAPSHOT_AUTHORITATIVE,
            pre_snapshot_buffer_cap=V1_BINANCE_BUFFER_CAP,
            rest_snapshot_sequence_comparable=False,
            replay_rest_snapshot_with_ws_deltas=False,
        )
    if normalized in {"bitget", "gate"}:
        return LocalL2VenuePolicy(
            venue=normalized,
            bridge_mode=BridgeMode.REST_SNAPSHOT_BUFFERED_REPLAY,
            pre_snapshot_buffer_cap=LEGACY_GENERIC_BUFFER_CAP,
        )
    return LocalL2VenuePolicy(
        venue=normalized,
        bridge_mode=BridgeMode.REST_SNAPSHOT_BUFFERED_REPLAY,
        pre_snapshot_buffer_cap=LEGACY_GENERIC_BUFFER_CAP,
    )
