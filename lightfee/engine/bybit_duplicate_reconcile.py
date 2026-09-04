"""Shared duplicate-client-id reconciliation helpers.

Bybit V5 documents retCode 110072 as "OrderLinkedID is duplicate":
https://bybit-exchange.github.io/docs/v5/error

Bitget documents code 40786 as "Duplicate clientOid".  Both responses have
the same idempotency contract: the client id may already have reached the
exchange, so callers must reconcile that id before clearing state or retrying.

That is an idempotency conflict, not an ordinary placement failure.  Callers
must reconcile the original client id before deciding whether to clear, retry
with a new client id, or back off.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lightfee.core.domain import PositionSnapshot, Venue
from lightfee.engine.order_truth_ledger import ORDER_TRUTH_LEDGER, OrderTruthDecision


BYBIT_DUPLICATE_RECONCILE_ENDPOINTS = list(
    ORDER_TRUTH_LEDGER.duplicate_reconcile_endpoints(Venue.BYBIT)
)


@dataclass(frozen=True)
class DuplicateClientOrderReconcileResult:
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


BybitDuplicateReconcileResult = DuplicateClientOrderReconcileResult


def is_duplicate_client_order_error(venue: Venue, error: Any) -> bool:
    """Recognize a venue's duplicate client-id response.

    This is deliberately limited to documented duplicate signatures.  A
    generic ``"duplicate"`` substring would incorrectly route unrelated
    validation errors into an order reconciliation path.
    """

    parts = [str(error or "")]
    for attr in ("exchange_response_body", "raw_response", "body", "code", "msg"):
        value = getattr(error, attr, "")
        if value:
            parts.append(str(value))
    text = " ".join(parts).lower().replace("_", "")
    if venue == Venue.BYBIT:
        return "110072" in text or ("orderlinkedid" in text and "duplicate" in text)
    if venue == Venue.BITGET:
        return "40786" in text or (
            "duplicate clientoid" in text
            or ("clientoid" in text and "duplicate" in text)
        )
    return False


async def reconcile_duplicate_client_order(
    *,
    adapter: Any,
    venue: Venue,
    symbol: str,
    client_order_id: str,
    target_qty: float,
    live_pos_before: PositionSnapshot | None = None,
) -> DuplicateClientOrderReconcileResult:
    """Classify a duplicate client id using order and live evidence."""

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
    return _to_duplicate_result(decision)


async def reconcile_bybit_duplicate_client_order(
    *,
    adapter: Any,
    symbol: str,
    client_order_id: str,
    target_qty: float,
    live_pos_before: PositionSnapshot | None = None,
) -> BybitDuplicateReconcileResult:
    """Compatibility facade for existing Bybit callers."""

    return await reconcile_duplicate_client_order(
        adapter=adapter,
        venue=Venue.BYBIT,
        symbol=symbol,
        client_order_id=client_order_id,
        target_qty=target_qty,
        live_pos_before=live_pos_before,
    )


def build_order_reconcile_result_payload(
    *,
    result: BybitDuplicateReconcileResult,
    symbol: str,
    client_order_id: str,
    reason: str,
) -> dict[str, Any]:
    """Build the unified order.reconcile_result payload used by cleanup paths."""

    return ORDER_TRUTH_LEDGER.build_duplicate_reconcile_result_payload(
        result=result,
        venue=Venue.BYBIT,
        symbol=symbol,
        client_order_id=client_order_id,
        reason=reason,
    )


def build_duplicate_reconcile_result_payload(
    *,
    result: DuplicateClientOrderReconcileResult,
    venue: Venue,
    symbol: str,
    client_order_id: str,
    reason: str,
) -> dict[str, Any]:
    """Build a duplicate reconciliation payload for any supported venue."""

    return ORDER_TRUTH_LEDGER.build_duplicate_reconcile_result_payload(
        result=result,
        venue=venue,
        symbol=symbol,
        client_order_id=client_order_id,
        reason=reason,
    )


def _to_duplicate_result(result: Any) -> DuplicateClientOrderReconcileResult:
    if isinstance(result, DuplicateClientOrderReconcileResult):
        return result
    if not isinstance(result, OrderTruthDecision):
        return DuplicateClientOrderReconcileResult(
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
    return DuplicateClientOrderReconcileResult(
        classification=result.classification,
        decision=result.decision,
        target_qty=result.target_qty,
        reconciled_qty=result.reconciled_qty,
        live_qty=result.live_qty,
        remaining_qty=result.remaining_qty,
        retry_qty=result.retry_qty,
        order_truth_state=result.state,
        live_side=result.live_side,
        order_id=result.order_id,
        client_order_id=result.client_order_id,
        average_price=result.average_price,
        reconcile_error=result.reconcile_error,
        live_fetch_error=result.live_fetch_error,
    )
