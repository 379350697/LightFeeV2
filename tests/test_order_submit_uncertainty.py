"""Tests for order-submit uncertainty evidence paths."""

from __future__ import annotations

import pytest

from lightfee.core.domain import Venue
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.engine.bybit_duplicate_reconcile import (
    BybitDuplicateReconcileResult,
    build_order_reconcile_result_payload,
    reconcile_bybit_duplicate_client_order,
)
from lightfee.engine.order_truth_ledger import (
    ORDER_TRUTH_LEDGER,
    OrderTruthDecision,
    OrderTruthEvidenceStatus,
    OrderTruthFillStatus,
)
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


@pytest.mark.parametrize(
    ("venue", "metadata", "expected_status", "expected_decision"),
    [
        (
            Venue.OKX,
            {
                "evidence_source": "okx_order_detail",
                "response_classification": "detail_found;fills_empty",
                "queried_endpoints": ["/api/v5/trade/order", "/api/v5/trade/fills"],
            },
            OrderTruthFillStatus.TRUTH_GAP,
            "retain_backoff",
        ),
        (
            Venue.BYBIT,
            {
                "evidence_source": "bybit_order_realtime",
                "response_classification": "accepted_ack_without_execution",
                "queried_endpoints": ["/v5/order/realtime"],
            },
            OrderTruthFillStatus.TRUTH_GAP,
            "retain_backoff",
        ),
        (
            Venue.BINANCE,
            {
                "raw_exchange_status": "NEW",
                "response_classification": "stale_accepted_order",
                "queried_endpoints": ["/fapi/v1/order"],
            },
            OrderTruthFillStatus.TRUTH_GAP,
            "retain_backoff",
        ),
        (
            Venue.BITGET,
            {
                "response_classification": "positive_quantity_missing_side",
                "queried_endpoints": ["/api/v3/trade/order-info"],
            },
            OrderTruthFillStatus.UNSUPPORTED_FAIL_CLOSED,
            "retain_fail_closed",
        ),
        (
            Venue.HYPERLIQUID,
            {
                "response_classification": "account_identity_mismatch",
                "queried_endpoints": ["POST /info orderStatus"],
            },
            OrderTruthFillStatus.UNSUPPORTED_FAIL_CLOSED,
            "retain_fail_closed",
        ),
    ],
)
def test_order_truth_resolution_keeps_weak_exchange_evidence_out_of_confirmed_fill(
    venue,
    metadata,
    expected_status,
    expected_decision,
):
    decision = ORDER_TRUTH_LEDGER.resolve_order_success(
        venue=venue,
        symbol="HOMEUSDT",
        order_id="oid-1",
        client_order_id="cid-1",
        target_qty=1600.0,
        reconciliation=None,
        metadata=metadata,
    )

    assert decision.fill_status is expected_status
    assert decision.evidence_status is OrderTruthEvidenceStatus.UNAVAILABLE
    assert decision.decision == expected_decision
    assert decision.reconciled_qty == 0.0
    assert decision.terminal_without_truth is False


def test_order_truth_resolution_rejects_positive_quantity_from_order_detail_only():
    from lightfee.core.domain import OrderFillReconciliation, Side

    reconciliation = OrderFillReconciliation(
        venue=Venue.OKX,
        symbol="HOMEUSDT",
        side=Side.BUY,
        quantity=1600.0,
        average_price=0.01,
        order_id="oid-1",
        client_order_id="cid-1",
        metadata={
            "evidence_source": "okx_order_detail",
            "response_classification": "detail_found;fills_empty",
            "queried_endpoints": ["/api/v5/trade/order", "/api/v5/trade/fills"],
        },
    )

    decision = ORDER_TRUTH_LEDGER.resolve_order_success(
        venue=Venue.OKX,
        symbol="HOMEUSDT",
        order_id="oid-1",
        client_order_id="cid-1",
        target_qty=1600.0,
        reconciliation=reconciliation,
    )

    assert decision.fill_status is OrderTruthFillStatus.TRUTH_GAP
    assert decision.evidence_status is OrderTruthEvidenceStatus.UNAVAILABLE
    assert decision.decision == "retain_backoff"
    assert decision.reconciled_qty == 0.0


def test_order_truth_resolution_rejects_positive_quantity_without_metadata():
    from lightfee.core.domain import OrderFillReconciliation, Side

    reconciliation = OrderFillReconciliation(
        venue=Venue.OKX,
        symbol="HOMEUSDT",
        side=Side.BUY,
        quantity=1600.0,
        average_price=0.01,
        order_id="oid-1",
        client_order_id="cid-1",
        metadata={},
    )

    decision = ORDER_TRUTH_LEDGER.resolve_order_success(
        venue=Venue.OKX,
        symbol="HOMEUSDT",
        order_id="oid-1",
        client_order_id="cid-1",
        target_qty=1600.0,
        reconciliation=reconciliation,
    )

    assert decision.fill_status is OrderTruthFillStatus.TRUTH_GAP
    assert decision.evidence_status is OrderTruthEvidenceStatus.UNAVAILABLE
    assert decision.decision == "retain_backoff"
    assert decision.reconciled_qty == 0.0
    assert "fill_confirmation" in decision.missing_evidence


@pytest.mark.parametrize(
    ("venue", "source", "endpoint"),
    [
        (Venue.BINANCE, "binance_order_status", "/fapi/v1/order"),
        (Venue.ASTER, "aster_order_status", "/fapi/v3/order"),
    ],
)
def test_order_truth_resolution_accepts_binance_style_filled_order_status(
    venue,
    source,
    endpoint,
):
    from lightfee.core.domain import OrderFillReconciliation, Side

    reconciliation = OrderFillReconciliation(
        venue=venue,
        symbol="HOMEUSDT",
        side=Side.BUY,
        quantity=1600.0,
        average_price=0.01,
        order_id="oid-1",
        client_order_id="cid-1",
        metadata={
            "evidence_source": source,
            "raw_exchange_status": "FILLED",
            "response_classification": "filled",
            "queried_endpoints": [endpoint],
        },
    )

    decision = ORDER_TRUTH_LEDGER.resolve_order_success(
        venue=venue,
        symbol="HOMEUSDT",
        order_id="oid-1",
        client_order_id="cid-1",
        target_qty=1600.0,
        reconciliation=reconciliation,
    )

    assert decision.fill_status is OrderTruthFillStatus.CONFIRMED_FILL
    assert decision.evidence_status is OrderTruthEvidenceStatus.AVAILABLE
    assert decision.decision == "terminal_fill"
    assert decision.reconciled_qty == pytest.approx(1600.0)


def test_order_truth_resolution_confirmed_fill_requires_positive_fill_reconciliation():
    from lightfee.core.domain import OrderFillReconciliation, Side

    reconciliation = OrderFillReconciliation(
        venue=Venue.BYBIT,
        symbol="HOMEUSDT",
        side=Side.SELL,
        quantity=1600.0,
        average_price=0.01,
        order_id="oid-1",
        client_order_id="cid-1",
        metadata={
            "evidence_source": "bybit_execution_list",
            "queried_endpoints": ["/v5/execution/list"],
        },
    )

    decision = ORDER_TRUTH_LEDGER.resolve_order_success(
        venue=Venue.BYBIT,
        symbol="HOMEUSDT",
        order_id="oid-1",
        client_order_id="cid-1",
        target_qty=1600.0,
        reconciliation=reconciliation,
    )

    assert decision.fill_status is OrderTruthFillStatus.CONFIRMED_FILL
    assert decision.evidence_status is OrderTruthEvidenceStatus.AVAILABLE
    assert decision.decision == "terminal_fill"
    assert decision.reconciled_qty == pytest.approx(1600.0)
    assert decision.average_price == pytest.approx(0.01)


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


@pytest.mark.parametrize(
    ("classification", "decision", "expected_state"),
    [
        ("full", "clear", "resolved_flat"),
        ("partial", "retry_new_client_order_id", "resolved_position"),
    ],
)
def test_duplicate_reconcile_payload_fallback_state_uses_duplicate_decision(
    classification,
    decision,
    expected_state,
):
    payload = build_order_reconcile_result_payload(
        result=BybitDuplicateReconcileResult(
            classification=classification,
            decision=decision,
            target_qty=10.0,
            reconciled_qty=4.0,
            live_qty=6.0,
            remaining_qty=6.0,
            retry_qty=6.0,
            client_order_id="cid-duplicate",
        ),
        symbol="HOMEUSDT",
        client_order_id="cid-duplicate",
        reason="duplicate_client_id",
    )

    assert payload["order_truth_state"] == expected_state


@pytest.mark.asyncio
async def test_bybit_duplicate_facade_delegates_decision_to_order_truth_ledger(monkeypatch):
    calls: list[dict[str, object]] = []

    class Adapter:
        async def fetch_order_fill_reconciliation(self, symbol, order_id, client_order_id):
            return type(
                "Fill",
                (),
                {
                    "quantity": 0.0,
                    "order_id": "",
                    "client_order_id": client_order_id,
                    "average_price": 0.0,
                },
            )()

        async def fetch_position(self, symbol):
            return type("Position", (), {"quantity": 3.0, "side": None})()

    def fake_resolve_duplicate_conflict(**kwargs):
        calls.append(kwargs)
        return OrderTruthDecision(
            state="resolved_position",
            classification="ledger_decided",
            decision="retry_new_client_order_id",
            target_qty=3.0,
            reconciled_qty=0.0,
            live_qty=3.0,
            remaining_qty=3.0,
            retry_qty=3.0,
            client_order_id="cid-ledger",
        )

    monkeypatch.setattr(
        ORDER_TRUTH_LEDGER,
        "resolve_duplicate_conflict",
        fake_resolve_duplicate_conflict,
    )

    result = await reconcile_bybit_duplicate_client_order(
        adapter=Adapter(),
        symbol="HOMEUSDT",
        client_order_id="cid-ledger",
        target_qty=3.0,
    )

    assert calls
    assert calls[0]["venue"] == Venue.BYBIT
    assert calls[0]["symbol"] == "HOMEUSDT"
    assert calls[0]["client_order_id"] == "cid-ledger"
    assert result.classification == "ledger_decided"
    assert result.order_truth_state == "resolved_position"
    assert result.should_retry_with_new_client_id is True
