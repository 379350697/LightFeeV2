"""Shared order-submit uncertainty evidence for live state machines."""

from __future__ import annotations

from typing import Any

from lightfee.core.domain import Venue
from lightfee.core.errors import OrderSubmitError
from lightfee.engine.order_truth_ledger import (
    ORDER_TRUTH_GAP_MISSING_EVIDENCE,
    ORDER_TRUTH_GAP_NEXT_ACTION,
    ORDER_TRUTH_LEDGER,
    PRIVATE_WS_TRUTH_TOPICS,
)


def order_truth_probe_paths(
    venue: Venue | None,
    *,
    resolved_account_family: object = None,
) -> dict[str, str]:
    return ORDER_TRUTH_LEDGER.probe_paths(
        venue,
        resolved_account_family=resolved_account_family,
    )


def is_order_truth_gap(error: Exception) -> bool:
    return ORDER_TRUTH_LEDGER.is_order_truth_gap(error)


def build_order_submit_uncertainty_payload(
    error: OrderSubmitError,
    *,
    venue: Venue | None = None,
    operation: str = "",
    request: Any = None,
    default_client_order_id: str = "",
) -> dict[str, Any]:
    return ORDER_TRUTH_LEDGER.build_submit_uncertainty_payload(
        error,
        venue=venue,
        operation=operation,
        request=request,
        default_client_order_id=default_client_order_id,
    )
