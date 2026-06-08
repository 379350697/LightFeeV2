"""Shared order-submit uncertainty evidence for live state machines."""

from __future__ import annotations

from typing import Any

from lightfee.core.domain import Venue
from lightfee.core.errors import OrderSubmitError
from lightfee.core.exchange_errors import (
    RequestContext,
    build_evidence_from_order_submit_error,
)


ORDER_TRUTH_GAP_NEXT_ACTION = "reconcile_accepted_order_or_probe_live_position"
ORDER_TRUTH_GAP_MISSING_EVIDENCE = (
    "fill_confirmation",
    "order_realtime_status",
    "private_ws_execution",
    "open_order_truth",
)


def order_truth_probe_paths(venue: Venue | None) -> dict[str, str]:
    if venue == Venue.BYBIT:
        return {
            "rest_order_status": "GET /v5/order/realtime",
            "open_order_truth": "GET /v5/order/realtime",
            "private_ws_order_topic": "order",
            "private_ws_execution_topic": "execution",
            "live_position_probe": "GET /v5/position/list",
        }
    if venue == Venue.OKX:
        return {
            "rest_order_status": "GET /api/v5/trade/order",
            "open_order_truth": "GET /api/v5/trade/orders-pending",
            "private_ws_order_topic": "orders",
            "live_position_probe": "GET /api/v5/account/positions",
        }
    if venue == Venue.BINANCE:
        return {
            "rest_order_status": "GET /fapi/v1/order",
            "open_order_truth": "GET /fapi/v1/openOrders",
            "private_ws_order_topic": "ORDER_TRADE_UPDATE",
            "live_position_probe": "GET /fapi/v2/positionRisk",
        }
    if venue == Venue.ASTER:
        return {
            "rest_order_status": "GET /fapi/v3/order",
            "open_order_truth": "GET /fapi/v3/openOrders",
            "live_position_probe": "GET /fapi/v3/positionRisk",
        }
    return {
        "rest_order_status": "adapter.fetch_order_fill_reconciliation",
        "open_order_truth": "adapter.fetch_open_orders",
        "live_position_probe": "adapter.fetch_position",
    }


def is_order_truth_gap(error: Exception) -> bool:
    if bool(getattr(error, "order_ack_only", False)):
        return True
    if not isinstance(error, OrderSubmitError) or not error.is_uncertain:
        return False
    text = str(error).lower()
    return (
        "fill not confirmed" in text
        or "no order id and no fill data" in text
        or "order response contains no order id" in text
    )


def build_order_submit_uncertainty_payload(
    error: OrderSubmitError,
    *,
    venue: Venue | None = None,
    operation: str = "",
    request: Any = None,
    default_client_order_id: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    request_context = (
        RequestContext.from_order_request(request)
        if request is not None
        else RequestContext(client_order_id=default_client_order_id)
    )
    exchange_evidence = build_evidence_from_order_submit_error(
        error,
        venue=venue.value if venue is not None else "",
        operation=operation,
        endpoint=str(getattr(error, "endpoint", "") or ""),
        request_context=request_context,
    )
    payload["exchange_error"] = exchange_evidence.to_dict()
    payload["missing_evidence"] = list(exchange_evidence.missing_evidence)
    payload["evidence_completeness"] = exchange_evidence.evidence_completeness

    if not is_order_truth_gap(error):
        return payload

    if bool(getattr(error, "order_ack_only", False)):
        payload["order_ack_only"] = True
    accepted_order_id = str(getattr(error, "accepted_order_id", "") or "")
    accepted_client_order_id = str(
        getattr(error, "accepted_client_order_id", "")
        or default_client_order_id
        or ""
    )
    if accepted_order_id:
        payload["accepted_order_id"] = accepted_order_id
    if accepted_client_order_id:
        payload["accepted_client_order_id"] = accepted_client_order_id
    missing_fields = getattr(error, "fill_confirmation_missing_fields", []) or []
    if missing_fields:
        payload["fill_confirmation_missing_fields"] = [
            str(field) for field in missing_fields
        ]
    raw_body = str(getattr(error, "exchange_response_body", "") or "")
    if raw_body:
        payload["exchange_response_body"] = raw_body[:4000]

    for item in ORDER_TRUTH_GAP_MISSING_EVIDENCE:
        if item not in payload["missing_evidence"]:
            payload["missing_evidence"].append(item)
    if not accepted_order_id:
        for item in ("order_id", "accepted_order_id"):
            if item not in payload["missing_evidence"]:
                payload["missing_evidence"].append(item)
    payload["order_truth_probe_paths"] = order_truth_probe_paths(venue)
    payload["next_action"] = ORDER_TRUTH_GAP_NEXT_ACTION
    return payload
