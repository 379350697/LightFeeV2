from __future__ import annotations

import json
from pathlib import Path

import pytest

from lightfee.core.domain import PositionSnapshot, Side, Venue
from lightfee.engine.reconciliation import PositionReconciliationResult
from lightfee.engine.runtime import LiveRuntime
from tests.test_pending_entry_v1_semantic_drift import (
    _CapturingReconciler,
    _NoFillReconciliationAdapter,
    _pending_entry,
    _runtime,
    config,
    tmp_journal,
)


pytestmark = pytest.mark.live_harness

FIXTURE = Path("tests/fixtures/live_incidents/2026-05-27/pending_entry_v1_semantic_drift.jsonl")


def _events() -> list[dict]:
    return [json.loads(line) for line in FIXTURE.read_text().splitlines() if line.strip()]


def _payloads(kind: str) -> list[dict]:
    return [event["payload"] for event in _events() if event["kind"] == kind]


def test_incident_fixture_captures_false_flat_clear_then_live_position():
    kinds = [event["kind"] for event in _events()]
    stale_query = _payloads("order.reconcile_query")[0]
    truth = _payloads("exchange.truth")[0]
    beat_error = _payloads("reconciliation.entry_reconcile_error")[0]

    assert "reconciliation.entry_cleared_flat" in kinds
    assert stale_query["response_classification"] == "stale_accepted_order"
    assert truth["has_nonzero_position"] is True
    assert truth["positions"]["binance"]["MUBARAKUSDT"]["quantity"] == 1758.0
    assert beat_error["client_order_id"] == "planned-hedge-cid-beat"
    assert "Order does not exist" in beat_error["error"]


@pytest.mark.asyncio
async def test_incident_flat_reconcile_retains_maker_until_terminal_evidence(
    config, tmp_journal,
):
    submitted = _payloads("order.passive_submitted")[0]
    selected = _payloads("entry.selected")[0]
    result_payload = _payloads("order.reconcile_result")[0]
    result = PositionReconciliationResult(
        position_id=selected["entry_id"],
        symbol=selected["symbol"],
        long_status=result_payload["long_status"],
        short_status=result_payload["short_status"],
        long_position=PositionSnapshot(
            venue=Venue.BINANCE,
            symbol=selected["symbol"],
            side=Side.BUY,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1779844549100,
        ),
        short_position=PositionSnapshot(
            venue=Venue.BYBIT,
            symbol=selected["symbol"],
            side=Side.SELL,
            quantity=0.0,
            entry_price=0.0,
            observed_at_ms=1779844549100,
        ),
        is_flat=True,
    )
    runtime = _runtime(config, tmp_journal, _CapturingReconciler(result))
    pending = _pending_entry(
        pending_id=selected["entry_id"],
        symbol=selected["symbol"],
        target_quantity=selected["target_quantity"],
        maker_order_id=submitted["order_id"],
        maker_client_order_id=submitted["client_order_id"],
        created_at_ms=submitted["accepted_at_ms"],
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._reconcile_pending_state(now_ms=1779844549300)

    kinds = [event["kind"] for event in tmp_journal.read_all()]
    assert pending.pending_id in runtime.state.pending_entries
    assert "reconciliation.entry_flat_unresolved_maker_retained" in kinds
    assert "reconciliation.entry_cleared_flat" not in kinds


@pytest.mark.asyncio
async def test_incident_live_position_hydrates_pending_entry_instead_of_live_recovery(
    config, tmp_journal,
):
    selected = [
        payload for payload in _payloads("execution.entry_selected")
        if payload["symbol"] == "PRLUSDT"
    ][0]
    submitted = [
        payload for payload in _payloads("order.passive_submitted")
        if payload["entry_id"] == selected["entry_id"]
    ][0]
    live = [
        payload for payload in _payloads("recovery.live_detected")
        if payload.get("source_entry_id") == selected["entry_id"]
    ][0]

    result = PositionReconciliationResult(
        position_id=selected["entry_id"],
        symbol=selected["symbol"],
        long_status="filled",
        short_status="uncertain",
        long_position=PositionSnapshot(
            venue=Venue(live["long_venue"]),
            symbol=selected["symbol"],
            side=Side.BUY,
            quantity=live["long_quantity"],
            entry_price=live["long_entry_price"],
            observed_at_ms=1779861315888,
        ),
        short_position=PositionSnapshot(
            venue=Venue(live["short_venue"]),
            symbol=selected["symbol"],
            side=Side.SELL,
            quantity=live["short_quantity"],
            entry_price=live["short_entry_price"],
            observed_at_ms=1779861315888,
        ),
        is_flat=False,
    )
    runtime = LiveRuntime(
        config,
        venue_adapters={
            Venue(live["long_venue"]): _NoFillReconciliationAdapter(),
            Venue(live["short_venue"]): _NoFillReconciliationAdapter(),
        },
    )
    runtime.journal = tmp_journal
    runtime.reconciler = _CapturingReconciler(result)
    pending = _pending_entry(
        pending_id=selected["entry_id"],
        symbol=selected["symbol"],
        target_quantity=selected["target_quantity"],
        long_venue=Venue(live["long_venue"]),
        short_venue=Venue(live["short_venue"]),
        maker_leg_filled=live["long_quantity"],
        hedge_leg_filled=0.0,
        maker_fill_price=live["long_entry_price"],
        maker_order_id=submitted["order_id"],
        maker_client_order_id=submitted["client_order_id"],
        hedge_order_id="",
        hedge_fill_price=0.0,
    )
    runtime.state.pending_entries[pending.pending_id] = pending

    await runtime._reconcile_pending_state(now_ms=1779861316000)

    assert pending.pending_id not in runtime.state.pending_entries
    opened = runtime.state.open_positions[pending.pending_id]
    assert opened.long_quantity == pytest.approx(live["matched_quantity"])
    assert opened.short_quantity == pytest.approx(live["matched_quantity"])
    assert opened.short_entry_price == pytest.approx(live["short_entry_price"])
    kinds = [event["kind"] for event in tmp_journal.read_all()]
    assert "entry.opened" in kinds
    assert "pending_entry.finalize_deferred_incomplete_fill" not in kinds
