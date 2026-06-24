"""Venue-neutral duplicate client order id reconciliation helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lightfee.core.domain import PositionSnapshot, Venue
from lightfee.engine.order_truth_ledger import ORDER_TRUTH_LEDGER, OrderTruthDecision


@dataclass(frozen=True)
class DuplicateClientReconcileResult:
    classification: str
    decision: str
    target_qty: float
    reconciled_qty: float
    live_qty: float
    remaining_qty: float
    retry_qty: float
    order_truth_state: str = ""
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


async def reconcile_duplicate_client_order(
    *,
    venue: Venue,
    adapter: Any,
    symbol: str,
    client_order_id: str,
    target_qty: float,
    live_pos_before: PositionSnapshot | None = None,
) -> DuplicateClientReconcileResult:
    reconciliation = None
    reconcile_error = ""
    try:
        reconciliation = await adapter.fetch_order_fill_reconciliation(
            symbol, "", client_order_id,
        )
    except Exception as exc:
        reconcile_error = str(exc)

    live_pos_after = None
    live_fetch_error = ""
    live_fetch_attempted = False
    try:
        live_fetch_attempted = True
        live_pos_after = await adapter.fetch_position(symbol)
    except Exception as exc:
        live_fetch_error = str(exc)

    decision = ORDER_TRUTH_LEDGER.resolve_duplicate_conflict(
        venue=venue,
        symbol=symbol,
        client_order_id=client_order_id,
        target_qty=target_qty,
        reconciliation=reconciliation,
        live_pos_before=live_pos_before,
        live_pos_after=live_pos_after,
        reconcile_error=reconcile_error,
        live_fetch_error=live_fetch_error,
        live_fetch_attempted=live_fetch_attempted,
    )
    return _to_duplicate_client_result(decision)


def build_duplicate_client_reconcile_result_payload(
    *,
    result: DuplicateClientReconcileResult,
    venue: Venue,
    symbol: str,
    client_order_id: str,
    reason: str,
) -> dict[str, Any]:
    return ORDER_TRUTH_LEDGER.build_duplicate_reconcile_result_payload(
        result=result,
        venue=venue,
        symbol=symbol,
        client_order_id=client_order_id,
        reason=reason,
    )


def _to_duplicate_client_result(result: Any) -> DuplicateClientReconcileResult:
    if isinstance(result, DuplicateClientReconcileResult):
        return result
    if not isinstance(result, OrderTruthDecision):
        return DuplicateClientReconcileResult(
            classification=str(getattr(result, "classification", "") or ""),
            decision=str(getattr(result, "decision", "") or ""),
            target_qty=float(getattr(result, "target_qty", 0.0) or 0.0),
            reconciled_qty=float(getattr(result, "reconciled_qty", 0.0) or 0.0),
            live_qty=float(getattr(result, "live_qty", 0.0) or 0.0),
            remaining_qty=float(getattr(result, "remaining_qty", 0.0) or 0.0),
            retry_qty=float(getattr(result, "retry_qty", 0.0) or 0.0),
            order_truth_state=str(
                getattr(result, "order_truth_state", "")
                or getattr(result, "state", "")
                or ""
            ),
            live_side=getattr(result, "live_side", None),
            order_id=str(getattr(result, "order_id", "") or ""),
            client_order_id=str(getattr(result, "client_order_id", "") or ""),
            average_price=float(getattr(result, "average_price", 0.0) or 0.0),
            reconcile_error=str(getattr(result, "reconcile_error", "") or ""),
            live_fetch_error=str(getattr(result, "live_fetch_error", "") or ""),
        )
    return DuplicateClientReconcileResult(
        classification=result.classification,
        decision=result.decision,
        target_qty=result.target_qty,
        reconciled_qty=result.reconciled_qty,
        live_qty=result.live_qty,
        remaining_qty=result.remaining_qty,
        retry_qty=result.retry_qty,
        order_truth_state=str(
            getattr(result, "order_truth_state", "")
            or getattr(result, "state", "")
            or ""
        ),
        live_side=result.live_side,
        order_id=result.order_id,
        client_order_id=result.client_order_id,
        average_price=result.average_price,
        reconcile_error=result.reconcile_error,
        live_fetch_error=result.live_fetch_error,
    )
