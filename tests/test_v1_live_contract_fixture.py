"""One V1 fixture drives entry payload, close and recovery parity together."""

from __future__ import annotations

import json
from pathlib import Path

from lightfee.config.schema import StrategyConfig
from lightfee.core.domain import Side, Venue
from lightfee.engine.entry import EntryContext, EntryType, build_entry_orders
from lightfee.engine.exit import build_reduce_only_close_orders
from lightfee.engine.exit_decision import standard_close_reason
from lightfee.engine.exchange_truth import ExchangeTruthPosition, ExchangeTruthSnapshot
from lightfee.engine.recovery_decision_core import (
    RecoveryEvidenceSnapshot,
    V1RecoveryDecisionCore,
)
from lightfee.engine.state import OpenPosition


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "v1_live_contract_parity.json"


def _load_fixture() -> dict[str, object]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _order_payload(request) -> dict[str, object]:
    return {
        "venue": request.venue.value,
        "symbol": request.symbol,
        "side": request.side.value,
        "quantity": request.quantity,
        "price": request.price,
        "reduce_only": request.reduce_only,
        "post_only": request.post_only,
        "time_in_force": (
            request.time_in_force.value if request.time_in_force is not None else None
        ),
        "client_order_id": request.client_order_id,
    }


def test_v1_fixture_preserves_final_order_payload_close_and_recovery_contract() -> None:
    """Compare the same V1 fixture across all three compatibility boundaries.

    The fixture records the V1 behavioural contract, not Rust's process-local
    client-id hash implementation.  Its exact CIDs are the documented V2
    deterministic, exchange-legal equivalent for the same entry identity.
    """
    fixture = _load_fixture()

    entry = fixture["entry"]
    assert isinstance(entry, dict)
    entry_context = entry["context"]
    assert isinstance(entry_context, dict)
    ctx = EntryContext(
        entry_id=str(entry_context["entry_id"]),
        symbol=str(entry_context["symbol"]),
        long_venue=Venue.from_str(str(entry_context["long_venue"])),
        short_venue=Venue.from_str(str(entry_context["short_venue"])),
        long_quantity=float(entry_context["long_quantity"]),
        short_quantity=float(entry_context["short_quantity"]),
        long_price_hint=float(entry_context["long_price_hint"]),
        short_price_hint=float(entry_context["short_price_hint"]),
        maker_leg=Side(str(entry_context["maker_leg"])),
        entry_type=EntryType(str(entry_context["entry_type"])),
    )
    maker, hedge = build_entry_orders(ctx)
    expected_entry_orders = entry["final_order_payloads"]
    assert isinstance(expected_entry_orders, list)
    assert [
        {"role": "maker", **_order_payload(maker)},
        {"role": "hedge", **_order_payload(hedge)},
    ] == expected_entry_orders

    close = fixture["close"]
    assert isinstance(close, dict)
    position_values = close["position"]
    assert isinstance(position_values, dict)
    position = OpenPosition(
        position_id=str(position_values["position_id"]),
        symbol=str(position_values["symbol"]),
        long_venue=Venue.from_str(str(position_values["long_venue"])),
        short_venue=Venue.from_str(str(position_values["short_venue"])),
        long_quantity=float(position_values["long_quantity"]),
        short_quantity=float(position_values["short_quantity"]),
        long_entry_price=float(position_values["long_entry_price"]),
        short_entry_price=float(position_values["short_entry_price"]),
        opened_at_ms=int(position_values["opened_at_ms"]),
        matched_quantity=float(position_values["matched_quantity"]),
        funding_timestamp_ms=int(position_values["funding_timestamp_ms"]),
        funding_captured=bool(position_values["funding_captured"]),
        current_net_quote=float(position_values["current_net_quote"]),
    )
    config_values = close["config"]
    assert isinstance(config_values, dict)
    config = StrategyConfig(**config_values)
    reason = standard_close_reason(position, config, int(close["now_ms"]))
    assert reason is not None
    assert reason.value == close["expected_reason"]
    long_close, short_close = build_reduce_only_close_orders(position, reason)
    expected_close_orders = close["final_order_payloads"]
    assert isinstance(expected_close_orders, list)
    assert [
        {"role": "long_close", **_order_payload(long_close)},
        {"role": "short_close", **_order_payload(short_close)},
    ] == expected_close_orders

    recovery = fixture["recovery"]
    assert isinstance(recovery, dict)
    truth_values = recovery["exchange_truth"]
    assert isinstance(truth_values, dict)
    truth_positions = truth_values["positions"]
    assert isinstance(truth_positions, list)
    exchange_truth = ExchangeTruthSnapshot(
        available=bool(truth_values["available"]),
        confidence=str(truth_values["confidence"]),
        positions=tuple(
            ExchangeTruthPosition(
                venue=str(raw["venue"]),
                symbol=str(raw["symbol"]),
                side=str(raw["side"]),
                quantity=float(raw["quantity"]),
                entry_price=float(raw["entry_price"]),
            )
            for raw in truth_positions
        ),
    )
    decision = V1RecoveryDecisionCore().decide(
        RecoveryEvidenceSnapshot(exchange_truth=exchange_truth)
    )
    expected_decision = recovery["expected_decision"]
    assert isinstance(expected_decision, dict)
    assert {
        "kind": decision.kind.value,
        "entry_allowed": decision.entry_allowed,
        "block_reason": decision.block_reason,
        "management_action": decision.management_action.value,
    } == expected_decision
