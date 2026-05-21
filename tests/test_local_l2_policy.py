from __future__ import annotations

from lightfee.marketdata.local_l2_policy import (
    BridgeMode,
    ReplayLinkKind,
    policy_for_venue,
)


def test_binance_uses_v1_rest_snapshot_buffered_replay_policy():
    policy = policy_for_venue("binance")

    assert policy.bridge_mode is BridgeMode.REST_SNAPSHOT_BUFFERED_REPLAY
    assert policy.pre_snapshot_buffer_cap == 4096
    assert policy.rest_snapshot_sequence_comparable is True


def test_aster_uses_binance_style_rest_snapshot_buffered_replay_policy():
    policy = policy_for_venue("aster")

    assert policy.bridge_mode is BridgeMode.REST_SNAPSHOT_BUFFERED_REPLAY
    assert policy.pre_snapshot_buffer_cap == 4096
    assert policy.rest_snapshot_sequence_comparable is True


def test_bybit_ws_snapshot_is_authoritative_and_rest_sequence_not_comparable():
    policy = policy_for_venue("bybit")

    assert policy.bridge_mode is BridgeMode.WS_SNAPSHOT_AUTHORITATIVE
    assert policy.rest_snapshot_sequence_comparable is False
    assert policy.replay_rest_snapshot_with_ws_deltas is False


def test_okx_replay_classifier_accepts_keepalive_and_reset():
    policy = policy_for_venue("okx")

    keepalive = policy.classify_replay_link(
        previous_sequence=15,
        sequence=15,
        previous_sequence_from_update=15,
        bid_count=0,
        ask_count=0,
    )
    reset = policy.classify_replay_link(
        previous_sequence=15,
        sequence=3,
        previous_sequence_from_update=15,
        bid_count=1,
        ask_count=1,
    )

    assert keepalive is ReplayLinkKind.KEEPALIVE
    assert reset is ReplayLinkKind.RESET


def test_okx_uses_v1_rest_snapshot_buffered_replay_policy():
    policy = policy_for_venue("okx")

    assert policy.bridge_mode is BridgeMode.REST_SNAPSHOT_BUFFERED_REPLAY
    assert policy.pre_snapshot_buffer_cap == 4096
    assert policy.rest_snapshot_sequence_comparable is True
    assert policy.replay_rest_snapshot_with_ws_deltas is True


def test_bitget_and_gate_preserve_legacy_bridge_until_probe_evidence():
    for venue in ("bitget", "gate"):
        policy = policy_for_venue(venue)

        assert policy.bridge_mode is BridgeMode.REST_SNAPSHOT_BUFFERED_REPLAY
        assert policy.pre_snapshot_buffer_cap == 512
        assert policy.rest_snapshot_sequence_comparable is True
        assert policy.replay_rest_snapshot_with_ws_deltas is True
