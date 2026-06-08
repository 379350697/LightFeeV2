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
