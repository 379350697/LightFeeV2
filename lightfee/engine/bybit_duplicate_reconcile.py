"""Bybit duplicate orderLinkId reconciliation helpers.

Bybit V5 documents retCode 110072 as "OrderLinkedID is duplicate":
https://bybit-exchange.github.io/docs/v5/error

That is an idempotency conflict, not an ordinary placement failure.  Callers
must reconcile the original client id before deciding whether to clear, retry
with a new client id, or back off.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lightfee.core.domain import PositionSnapshot, Venue


BYBIT_DUPLICATE_RECONCILE_ENDPOINTS = [
    "bybit_order_realtime",
    "bybit_order_history",
    "bybit_execution_list",
]


@dataclass(frozen=True)
class BybitDuplicateReconcileResult:
    classification: str
    decision: str
    target_qty: float
    reconciled_qty: float
    live_qty: float
    remaining_qty: float
    retry_qty: float
    live_side: str | None = None
    order_id: str = ""
    client_order_id: str = ""
    average_price: float = 0.0
    reconcile_error: str = ""
    live_fetch_error: str = ""

    @property
    def clear_state(self) -> bool:
        return self.decision in ("clear", "clear_live_flat")

    @property
    def should_retry_with_new_client_id(self) -> bool:
        return self.decision == "retry_new_client_order_id" and self.retry_qty > 1e-9


async def reconcile_bybit_duplicate_client_order(
    *,
    adapter: Any,
    symbol: str,
    client_order_id: str,
    target_qty: float,
    live_pos_before: PositionSnapshot | None = None,
) -> BybitDuplicateReconcileResult:
    """Classify a Bybit duplicate orderLinkId using order and live evidence."""

    reconciliation = None
    reconcile_error = ""
    try:
        reconciliation = await adapter.fetch_order_fill_reconciliation(
            symbol, "", client_order_id,
        )
    except Exception as exc:
        reconcile_error = str(exc)

    reconciled_qty = _positive_float(getattr(reconciliation, "quantity", 0.0))
    target_qty = max(_positive_float(target_qty), 0.0)
    remaining_qty = max(target_qty - reconciled_qty, 0.0)

    live_pos_after = None
    live_fetch_error = ""
    live_fetch_attempted = False
    try:
        live_fetch_attempted = True
        live_pos_after = await adapter.fetch_position(symbol)
    except Exception as exc:
        live_fetch_error = str(exc)

    live_pos = (
        live_pos_after
        if live_fetch_attempted and live_fetch_error == ""
        else live_pos_before
    )
    live_qty = _positive_float(getattr(live_pos, "quantity", 0.0))
    live_side = getattr(getattr(live_pos, "side", None), "value", None)
    live_flat = live_fetch_attempted and live_fetch_error == "" and live_qty <= 1e-9

    if (
        reconciled_qty >= max(target_qty - 1e-9, 0.0)
        and reconciled_qty > 0.0
        and live_fetch_error
    ):
        classification = "unknown_transient"
        decision = "backoff_recheck"
    elif (
        reconciled_qty >= max(target_qty - 1e-9, 0.0)
        and reconciled_qty > 0.0
        and live_qty > 1e-9
    ):
        classification = "stale_full_live_nonzero"
        decision = "retry_new_client_order_id"
        remaining_qty = live_qty
    elif reconciled_qty >= max(target_qty - 1e-9, 0.0) and reconciled_qty > 0.0:
        classification = "full"
        decision = "clear_live_flat"
    elif live_flat:
        classification = "none" if reconciled_qty <= 1e-9 else "partial"
        decision = "clear_live_flat"
    elif reconciled_qty > 1e-9:
        classification = "partial"
        decision = "retry_new_client_order_id"
    elif reconcile_error or live_fetch_error:
        classification = "unknown_transient"
        decision = "backoff_recheck"
    else:
        classification = "none"
        decision = "backoff_recheck"

    retry_qty = remaining_qty
    if decision == "retry_new_client_order_id" and live_qty > 1e-9:
        retry_qty = min(remaining_qty, live_qty)

    return BybitDuplicateReconcileResult(
        classification=classification,
        decision=decision,
        target_qty=target_qty,
        reconciled_qty=reconciled_qty,
        live_qty=live_qty,
        remaining_qty=remaining_qty,
        retry_qty=retry_qty,
        live_side=live_side,
        order_id=getattr(reconciliation, "order_id", "") if reconciliation is not None else "",
        client_order_id=(
            getattr(reconciliation, "client_order_id", None) or client_order_id
            if reconciliation is not None
            else client_order_id
        ),
        average_price=_positive_float(
            getattr(
                reconciliation,
                "average_price",
                getattr(reconciliation, "price", 0.0),
            )
        ),
        reconcile_error=reconcile_error,
        live_fetch_error=live_fetch_error,
    )


def build_order_reconcile_result_payload(
    *,
    result: BybitDuplicateReconcileResult,
    symbol: str,
    client_order_id: str,
    reason: str,
) -> dict[str, Any]:
    """Build the unified order.reconcile_result payload used by cleanup paths."""

    return {
        "venue": Venue.BYBIT.value,
        "symbol": symbol,
        "endpoint": BYBIT_DUPLICATE_RECONCILE_ENDPOINTS[0],
        "queried_endpoints": list(BYBIT_DUPLICATE_RECONCILE_ENDPOINTS),
        "endpoint_responses": [
            {
                "endpoint": endpoint,
                "classification": result.classification,
            }
            for endpoint in BYBIT_DUPLICATE_RECONCILE_ENDPOINTS
        ],
        "product_type": "reconciliation",
        "category": "reconciliation",
        "order_id": result.order_id,
        "exchange_order_id": result.order_id,
        "client_order_id": client_order_id,
        "status": result.classification,
        "reason": reason,
        "uncertain_subtype": "duplicate_client_id",
        "raw_exchange_status": result.classification,
        "fill_qty": result.reconciled_qty,
        "fill_price": result.average_price,
        "position_qty": result.live_qty,
        "position_side": result.live_side or "",
        "live_position_delta": {
            "quantity": result.live_qty,
            "signed_quantity": result.live_qty,
            "side": result.live_side or "",
            "observed_at_ms": 0,
            "source": "fetch_position",
        },
        "next_action": result.decision,
        "hedge_submitted": False,
        "raw_price": None,
        "raw_qty": None,
        "quantized_price": None,
        "quantized_qty": None,
        "tick_size": None,
        "quantity_step": None,
        "response_classification": result.classification,
        "target_qty": result.target_qty,
        "reconciled_qty": result.reconciled_qty,
        "live_qty": result.live_qty,
        "remaining_qty": result.remaining_qty,
        "retry_qty": result.retry_qty,
    }


def _positive_float(value: Any) -> float:
    try:
        parsed = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if parsed < 0.0:
        return abs(parsed)
    return parsed
