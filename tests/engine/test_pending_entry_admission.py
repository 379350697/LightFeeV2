from __future__ import annotations

import pytest

from lightfee.engine.pending_entry_admission import (
    PendingEntryAdmissionCore,
    PendingEntryAdmissionDecisionKind,
    PendingEntryAdmissionRequest,
)


def _request(**overrides) -> PendingEntryAdmissionRequest:
    values = {
        "symbol": "LABUSDT",
        "long_venue": "bitget",
        "short_venue": "bybit",
        "maker_venue": "bitget",
        "hedge_venue": "bybit",
        "entry_type": "passive_incremental",
        "maker_metadata": {
            "quantity_step": 0.001,
            "quantity_units": "base",
        },
        "maker_quantity_step": 0.001,
        "hedge_quantity_step": 0.001,
        "min_hedgeable_chunk": 0.3999,
        "full_target_quantity": 2.3995,
        "initial_maker_target_quantity": 1.1998,
        "guard_enabled": True,
        "small_fill_buffer_enabled": True,
        "ts_ms": 1_000,
    }
    values.update(overrides)
    return PendingEntryAdmissionRequest(**values)


def test_allows_planned_hedgeable_clip_when_maker_increment_requires_small_fill_buffer():
    decision = PendingEntryAdmissionCore.decide(_request())

    assert decision.kind is PendingEntryAdmissionDecisionKind.ALLOW_WITH_ADVISORY
    assert decision.can_submit is True
    assert decision.event_kind == "runtime.entry_pre_submit_hedgeability_advisory"
    assert decision.payload["reason"] == "maker_fill_increment_below_hedge_min_chunk"
    assert decision.payload["small_fill_buffer_required"] is True
    assert decision.payload["planned_clip_hedgeable"] is True
    assert decision.payload["maker_fill_increment_base"] == pytest.approx(0.001)
    assert decision.payload["min_hedgeable_chunk"] == pytest.approx(0.3999)


def test_blocks_gate_when_contract_unit_truth_is_missing():
    decision = PendingEntryAdmissionCore.decide(
        _request(
            long_venue="gate",
            short_venue="bybit",
            maker_venue="gate",
            hedge_venue="bybit",
            maker_metadata={},
            maker_quantity_step=0.0,
            min_hedgeable_chunk=225.25,
            full_target_quantity=450.5,
            initial_maker_target_quantity=225.25,
        )
    )

    assert decision.kind is PendingEntryAdmissionDecisionKind.BLOCK
    assert decision.can_submit is False
    assert decision.event_kind == "runtime.entry_blocked_pre_submit_hedgeability"
    assert decision.payload["reason"] == "maker_fill_unit_truth_unavailable"
    assert decision.payload["maker_venue"] == "gate"
    assert decision.payload["contract_multiplier"] == pytest.approx(0.0)
    assert decision.payload["raw_contract_step"] == pytest.approx(0.0)


def test_blocks_when_planned_maker_clip_is_not_hedgeable():
    decision = PendingEntryAdmissionCore.decide(
        _request(
            full_target_quantity=0.3999,
            initial_maker_target_quantity=0.1999,
            min_hedgeable_chunk=0.3999,
        )
    )

    assert decision.kind is PendingEntryAdmissionDecisionKind.BLOCK
    assert decision.can_submit is False
    assert decision.payload["reason"] == "planned_maker_clip_below_hedge_min_chunk"
    assert decision.payload["planned_clip_hedgeable"] is False


def test_standard_dual_taker_is_outside_pending_entry_admission():
    decision = PendingEntryAdmissionCore.decide(
        _request(entry_type="standard_dual_taker")
    )

    assert decision.kind is PendingEntryAdmissionDecisionKind.ALLOW
    assert decision.can_submit is True
    assert decision.event_kind == ""
    assert decision.payload == {}
