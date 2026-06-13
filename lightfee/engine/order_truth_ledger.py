"""Shared order-truth ledger for submit uncertainty and duplicate ids."""

from __future__ import annotations

from typing import Any

from lightfee.core.domain import Venue
from lightfee.core.errors import OrderSubmitError
from lightfee.core.exchange_errors import (
    RequestContext,
    build_evidence_from_order_submit_error,
)
from lightfee.venues.specs import VenueOperation, get_operation_contract, get_spec


ORDER_TRUTH_GAP_NEXT_ACTION = "reconcile_accepted_order_or_probe_live_position"
ORDER_TRUTH_GAP_MISSING_EVIDENCE = (
    "fill_confirmation",
    "order_realtime_status",
    "private_ws_execution",
    "open_order_truth",
)

PRIVATE_WS_TRUTH_TOPICS: dict[Venue, dict[str, str]] = {
    Venue.BYBIT: {
        "private_ws_order_topic": "order",
        "private_ws_execution_topic": "execution",
    },
    Venue.OKX: {
        "private_ws_order_topic": "orders",
        "private_ws_execution_topic": "orders",
    },
    Venue.BINANCE: {
        "private_ws_order_topic": "ORDER_TRADE_UPDATE",
    },
}


class OrderTruthLedger:
    """Single semantic entry for order truth gaps and idempotency conflicts."""

    name = "shared_v1"

    def contract_probe_label(
        self,
        venue: Venue,
        operation: VenueOperation,
        *,
        resolved_account_family: object = None,
    ) -> str:
        contract = get_operation_contract(
            get_spec(venue),
            operation,
            resolved_account_family=resolved_account_family,
        )
        if not contract.supported:
            return ""
        label = f"{contract.method} {contract.path}"
        if venue == Venue.HYPERLIQUID and contract.path == "/info":
            for param in contract.required_params:
                if param.startswith("type="):
                    return f"{label} {param.removeprefix('type=')}"
        return label

    def probe_paths(
        self,
        venue: Venue | None,
        *,
        resolved_account_family: object = None,
    ) -> dict[str, str]:
        if venue is None:
            return {
                "rest_order_status": "adapter.fetch_order_fill_reconciliation",
                "open_order_truth": "adapter.fetch_open_orders",
                "live_position_probe": "adapter.fetch_position",
            }
        spec = get_spec(venue)
        paths = {
            "rest_order_status": self.contract_probe_label(
                venue,
                VenueOperation.ORDER_STATUS,
                resolved_account_family=resolved_account_family,
            ),
            "open_order_truth": self.contract_probe_label(
                venue,
                VenueOperation.OPEN_ORDERS,
                resolved_account_family=resolved_account_family,
            ),
            "live_position_probe": self.contract_probe_label(
                venue,
                VenueOperation.POSITION,
                resolved_account_family=resolved_account_family,
            ),
        }
        order_history = self.contract_probe_label(
            venue,
            VenueOperation.ORDER_HISTORY,
            resolved_account_family=resolved_account_family,
        )
        if order_history:
            paths["rest_order_history"] = order_history
        execution_history = self.contract_probe_label(
            venue,
            VenueOperation.EXECUTION_HISTORY,
            resolved_account_family=resolved_account_family,
        )
        if execution_history:
            if venue == Venue.OKX:
                paths["rest_fills_history"] = execution_history
            else:
                paths["rest_execution_history"] = execution_history
        l2_book = self.contract_probe_label(
            venue,
            VenueOperation.L2_BOOK,
            resolved_account_family=resolved_account_family,
        )
        if l2_book:
            paths["l2_book_truth"] = l2_book
        if spec.venue_id in PRIVATE_WS_TRUTH_TOPICS:
            paths.update(PRIVATE_WS_TRUTH_TOPICS[spec.venue_id])
        position_contract = get_operation_contract(
            spec,
            VenueOperation.POSITION,
            resolved_account_family=resolved_account_family,
        )
        if position_contract.symbol_shape != "canonical":
            paths["symbol_shape"] = position_contract.symbol_shape
        if position_contract.required_params:
            paths["required_params"] = ",".join(position_contract.required_params)
        if venue == Venue.HYPERLIQUID:
            paths["account_identity"] = "configured_account_address_not_agent_wallet"
        return {key: value for key, value in paths.items() if value}

    def is_order_truth_gap(self, error: Exception) -> bool:
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

    def build_submit_uncertainty_payload(
        self,
        error: OrderSubmitError,
        *,
        venue: Venue | None = None,
        operation: str = "",
        request: Any = None,
        default_client_order_id: str = "",
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"order_truth_ledger": self.name}
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

        if not self.is_order_truth_gap(error):
            return payload

        if bool(getattr(error, "order_ack_only", False)):
            payload["order_ack_only"] = True
        payload["accepted_order_truth_gap"] = True
        payload["truth_required_by"] = "accepted_order_truth_gap"
        payload["terminal_without_truth"] = False
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
        payload["order_truth_probe_paths"] = self.probe_paths(venue)
        payload["next_action"] = ORDER_TRUTH_GAP_NEXT_ACTION
        return payload

    def duplicate_reconcile_endpoints(self, venue: Venue) -> tuple[str, ...]:
        if venue == Venue.BYBIT:
            return (
                "bybit_order_realtime",
                "bybit_order_history",
                "bybit_execution_list",
            )
        return tuple(
            value
            for key, value in self.probe_paths(venue).items()
            if key
            in (
                "rest_order_status",
                "rest_order_history",
                "rest_execution_history",
                "rest_fills_history",
            )
        )

    def build_duplicate_reconcile_result_payload(
        self,
        *,
        result: Any,
        venue: Venue,
        symbol: str,
        client_order_id: str,
        reason: str,
    ) -> dict[str, Any]:
        endpoints = list(self.duplicate_reconcile_endpoints(venue))
        order_id = str(getattr(result, "order_id", "") or "")
        classification = str(getattr(result, "classification", "") or "")
        normalized_reason = self.normalize_duplicate_reason(reason)
        return {
            "venue": venue.value,
            "symbol": symbol,
            "endpoint": endpoints[0] if endpoints else "",
            "queried_endpoints": endpoints,
            "endpoint_responses": [
                {
                    "endpoint": endpoint,
                    "classification": classification,
                }
                for endpoint in endpoints
            ],
            "product_type": "reconciliation",
            "category": "reconciliation",
            "order_id": order_id,
            "exchange_order_id": order_id,
            "client_order_id": client_order_id,
            "status": classification,
            "reason": reason,
            "uncertain_subtype": normalized_reason,
            "truth_required_by": normalized_reason,
            "terminal_without_truth": False,
            "order_truth_ledger": self.name,
            "order_truth_probe_paths": self.probe_paths(venue),
            "raw_exchange_status": classification,
            "fill_qty": float(getattr(result, "reconciled_qty", 0.0) or 0.0),
            "fill_price": float(getattr(result, "average_price", 0.0) or 0.0),
            "position_qty": float(getattr(result, "live_qty", 0.0) or 0.0),
            "position_side": str(getattr(result, "live_side", "") or ""),
            "live_position_delta": {
                "quantity": float(getattr(result, "live_qty", 0.0) or 0.0),
                "signed_quantity": float(getattr(result, "live_qty", 0.0) or 0.0),
                "side": str(getattr(result, "live_side", "") or ""),
                "observed_at_ms": 0,
                "source": "fetch_position",
            },
            "next_action": str(getattr(result, "decision", "") or ""),
            "hedge_submitted": False,
            "raw_price": None,
            "raw_qty": None,
            "quantized_price": None,
            "quantized_qty": None,
            "tick_size": None,
            "quantity_step": None,
            "response_classification": classification,
            "target_qty": float(getattr(result, "target_qty", 0.0) or 0.0),
            "reconciled_qty": float(getattr(result, "reconciled_qty", 0.0) or 0.0),
            "live_qty": float(getattr(result, "live_qty", 0.0) or 0.0),
            "remaining_qty": float(getattr(result, "remaining_qty", 0.0) or 0.0),
            "retry_qty": float(getattr(result, "retry_qty", 0.0) or 0.0),
        }

    def normalize_duplicate_reason(self, reason: str) -> str:
        text = str(reason or "").lower()
        if (
            "110072" in text
            or "orderlinkedid" in text
            or "duplicate" in text
            or text == "duplicate_client_id"
        ):
            return "duplicate_client_id"
        return "duplicate_client_id"


ORDER_TRUTH_LEDGER = OrderTruthLedger()
