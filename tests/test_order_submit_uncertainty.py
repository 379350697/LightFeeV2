"""Tests for order-submit uncertainty evidence paths."""

from __future__ import annotations

from lightfee.core.domain import Venue
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.engine.bybit_duplicate_reconcile import (
    BybitDuplicateReconcileResult,
    build_order_reconcile_result_payload,
)
from lightfee.engine.order_truth_ledger import ORDER_TRUTH_LEDGER
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


def test_bitget_truth_gap_uses_v2_mix_contract_paths_and_canonical_symbol_shape():
    paths = order_truth_probe_paths(Venue.BITGET)

    assert paths["rest_order_status"] == "GET /api/v2/mix/order/detail"
    assert paths["open_order_truth"] == "GET /api/v2/mix/order/orders-pending"
    assert paths["live_position_probe"] == "GET /api/v2/mix/position/single-position"
    assert paths["symbol_shape"] == "BTCUSDT"
    assert paths["required_params"] == "productType=USDT-FUTURES,marginCoin=USDT"


def test_bitget_truth_gap_probe_paths_are_family_aware_for_uta_v3():
    from lightfee.venues.specs import BitgetContractFamily

    paths = order_truth_probe_paths(
        Venue.BITGET,
        resolved_account_family=BitgetContractFamily.UTA_V3,
    )

    assert paths["rest_order_status"] == "GET /api/v3/trade/order-info"
    assert paths["open_order_truth"] == "GET /api/v3/trade/unfilled-orders"
    assert paths["live_position_probe"] == "GET /api/v3/position/current-position"
    assert paths["symbol_shape"] == "BTCUSDT"
    assert paths["required_params"] == "category=USDT-FUTURES"


def test_hyperliquid_truth_gap_documents_account_address_info_endpoint():
    paths = order_truth_probe_paths(Venue.HYPERLIQUID)

    assert paths["rest_order_status"] == "POST /info orderStatus"
    assert paths["open_order_truth"] == "POST /info openOrders"
    assert paths["live_position_probe"] == "POST /info clearinghouseState"
    assert paths["account_identity"] == "configured_account_address_not_agent_wallet"


def test_ack_gap_and_duplicate_reconcile_share_order_truth_ledger():
    error = OrderSubmitError(
        SubmitFailureClass.UNCERTAIN,
        "order accepted but fill not confirmed",
    )
    error.order_ack_only = True
    error.accepted_order_id = "oid-1"
    error.accepted_client_order_id = "cid-1"

    ack_payload = build_order_submit_uncertainty_payload(error, venue=Venue.BYBIT)

    duplicate_payload = build_order_reconcile_result_payload(
        result=BybitDuplicateReconcileResult(
            classification="none",
            decision="backoff_recheck",
            target_qty=1.0,
            reconciled_qty=0.0,
            live_qty=0.0,
            remaining_qty=1.0,
            retry_qty=1.0,
            client_order_id="cid-1",
        ),
        symbol="HOMEUSDT",
        client_order_id="cid-1",
        reason="duplicate_client_id",
    )

    assert ack_payload["order_truth_ledger"] == "shared_v1"
    assert duplicate_payload["order_truth_ledger"] == "shared_v1"
    assert duplicate_payload["truth_required_by"] == "duplicate_client_id"
    assert duplicate_payload["order_truth_probe_paths"] == (
        ORDER_TRUTH_LEDGER.probe_paths(Venue.BYBIT)
    )
