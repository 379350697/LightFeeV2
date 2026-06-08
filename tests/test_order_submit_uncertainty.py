"""Tests for order-submit uncertainty evidence paths."""

from __future__ import annotations

from lightfee.core.domain import Venue
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.engine.order_submit_uncertainty import (
    build_order_submit_uncertainty_payload,
    order_truth_probe_paths,
)


def test_aster_order_truth_paths_use_pro_api_v3_not_binance_legacy_paths():
    paths = order_truth_probe_paths(Venue.ASTER)

    assert paths["rest_order_status"] == "GET /fapi/v3/order"
    assert paths["open_order_truth"] == "GET /fapi/v3/openOrders"
    assert paths["live_position_probe"] == "GET /fapi/v3/positionRisk"
    assert "private_ws_order_topic" not in paths
    assert order_truth_probe_paths(Venue.BINANCE)["rest_order_status"] == "GET /fapi/v1/order"


def test_aster_ack_only_uncertainty_payload_uses_pro_api_v3_truth_paths():
    error = OrderSubmitError(SubmitFailureClass.UNCERTAIN, "order accepted but fill not confirmed")
    error.order_ack_only = True
    error.accepted_order_id = "12345"

    payload = build_order_submit_uncertainty_payload(error, venue=Venue.ASTER)

    assert payload["order_ack_only"] is True
    assert payload["order_truth_probe_paths"]["rest_order_status"] == "GET /fapi/v3/order"
    assert payload["order_truth_probe_paths"]["open_order_truth"] == "GET /fapi/v3/openOrders"
    assert payload["order_truth_probe_paths"]["live_position_probe"] == "GET /fapi/v3/positionRisk"
    assert "private_ws_order_topic" not in payload["order_truth_probe_paths"]


def test_bybit_ack_only_truth_gap_has_history_fills_and_live_truth_paths():
    error = OrderSubmitError(
        SubmitFailureClass.UNCERTAIN,
        "order accepted but fill not confirmed",
    )
    error.order_ack_only = True
    error.accepted_order_id = "oid-1"
    error.accepted_client_order_id = "cid-1"

    payload = build_order_submit_uncertainty_payload(error, venue=Venue.BYBIT)

    assert payload["order_ack_only"] is True
    assert payload["accepted_order_id"] == "oid-1"
    assert payload["accepted_client_order_id"] == "cid-1"
    assert payload["accepted_order_truth_gap"] is True
    assert payload["truth_required_by"] == "accepted_order_truth_gap"
    assert payload["terminal_without_truth"] is False
    assert payload["order_truth_probe_paths"] == {
        "rest_order_status": "GET /v5/order/realtime",
        "rest_order_history": "GET /v5/order/history",
        "rest_execution_history": "GET /v5/execution/list",
        "open_order_truth": "GET /v5/order/realtime",
        "private_ws_order_topic": "order",
        "private_ws_execution_topic": "execution",
        "live_position_probe": "GET /v5/position/list",
    }
