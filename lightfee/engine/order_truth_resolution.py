"""Shared accepted-order truth resolution helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from lightfee.core.domain import OrderFill, PositionSnapshot, Side, Venue
from lightfee.engine.order_truth_ledger import (
    ORDER_TRUTH_LEDGER,
    OrderTruthFillStatus,
)
from lightfee.engine.reconciliation import _recon_fill_price


FetchOpenOrders = Callable[[Any, Venue, str], Awaitable[list[Any]]]
GetAdapter = Callable[[Venue], Any]


@dataclass(frozen=True)
class AcceptedOrderTruthResolution:
    status: str
    fill: OrderFill | None
    payload: dict[str, Any]


def open_order_items(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if not isinstance(raw, dict):
        return [raw]
    if raw.get("error"):
        raise RuntimeError(str(raw.get("error")))
    result = raw.get("result")
    if isinstance(result, dict) and isinstance(result.get("list"), list):
        return result["list"]
    if isinstance(raw.get("data"), list):
        return raw["data"]
    if isinstance(raw.get("list"), list):
        return raw["list"]
    return []


def position_snapshot_evidence(position: PositionSnapshot | None) -> dict[str, Any]:
    if position is None:
        return {"available": False, "quantity": 0.0}
    return {
        "available": True,
        "venue": position.venue.value,
        "symbol": position.symbol,
        "side": position.side.value,
        "quantity": float(position.quantity or 0.0),
        "entry_price": float(position.entry_price or 0.0),
        "observed_at_ms": int(position.observed_at_ms or 0),
    }


async def resolve_accepted_order_truth(
    *,
    adapter: Any,
    venue: Venue,
    side: Side,
    symbol: str,
    target_qty: float,
    accepted_order_id: str,
    accepted_client_order_id: str,
    now_ms: int,
    probe_venues: list[Venue],
    get_adapter: GetAdapter,
    fetch_open_orders: FetchOpenOrders,
    baseline_quantity: float = 0.0,
    live_excess_mode: str = "absolute",
) -> AcceptedOrderTruthResolution:
    payload: dict[str, Any] = {
        "accepted_order_id": accepted_order_id,
        "accepted_client_order_id": accepted_client_order_id,
        "accepted_order_truth_gap": True,
        "order_truth_state": "ack_only_accepted",
        "truth_required_by": "accepted_order_truth_gap",
        "terminal_without_truth": False,
        "next_action": "reconcile_accepted_order_or_probe_live_position",
        "order_truth_probe_paths": ORDER_TRUTH_LEDGER.probe_paths(venue),
    }

    fetch_reconciliation = getattr(adapter, "fetch_order_fill_reconciliation", None)
    if callable(fetch_reconciliation) and (accepted_order_id or accepted_client_order_id):
        try:
            reconciliation = await fetch_reconciliation(
                symbol,
                accepted_order_id,
                accepted_client_order_id or None,
            )
        except Exception as exc:
            payload["fill_reconciliation_result"] = "error"
            payload["fill_reconciliation_error"] = str(exc) or exc.__class__.__name__
            decision = ORDER_TRUTH_LEDGER.truth_gap_status_decision("truth_unavailable")
            payload["resolution_state"] = decision.state
            payload["ledger_decision"] = decision.decision
            return AcceptedOrderTruthResolution("truth_unavailable", None, payload)

        truth_decision = ORDER_TRUTH_LEDGER.resolve_order_success(
            venue=venue,
            symbol=symbol,
            order_id=accepted_order_id,
            client_order_id=accepted_client_order_id,
            target_qty=target_qty,
            reconciliation=reconciliation,
            metadata=(
                getattr(reconciliation, "metadata", None)
                if reconciliation is not None
                else None
            ),
        )
        payload.update(
            {
                "order_truth_fill_status": truth_decision.fill_status.value,
                "order_truth_evidence_status": truth_decision.evidence_status.value,
                "order_truth_decision": truth_decision.decision,
                "order_truth_missing_evidence": list(truth_decision.missing_evidence),
                "terminal_without_truth": truth_decision.terminal_without_truth,
            }
        )
        if truth_decision.fill_status == OrderTruthFillStatus.CONFIRMED_FILL:
            payload["fill_reconciliation_result"] = "filled"
            fill = OrderFill(
                venue=venue,
                symbol=symbol,
                side=getattr(reconciliation, "side", side) or side,
                quantity=truth_decision.reconciled_qty,
                price=_recon_fill_price(reconciliation),
                order_id=(
                    str(getattr(reconciliation, "order_id", "") or "")
                    or accepted_order_id
                ),
                client_order_id=(
                    str(getattr(reconciliation, "client_order_id", "") or "")
                    or accepted_client_order_id
                    or None
                ),
                fee_quote=getattr(reconciliation, "fee_quote", None),
                filled_at_ms=int(getattr(reconciliation, "filled_at_ms", 0) or now_ms),
            )
            decision = ORDER_TRUTH_LEDGER.truth_gap_status_decision("filled")
            payload["resolution_state"] = decision.state
            payload["ledger_decision"] = decision.decision
            return AcceptedOrderTruthResolution("filled", fill, payload)
        reconciliation_qty = (
            float(getattr(reconciliation, "quantity", 0.0) or 0.0)
            if reconciliation is not None
            else 0.0
        )
        if reconciliation_qty > 1e-12:
            payload["fill_reconciliation_result"] = "truth_gap"
            decision = ORDER_TRUTH_LEDGER.truth_gap_status_decision("truth_gap")
            payload["resolution_state"] = decision.state
            payload["ledger_decision"] = decision.decision
            return AcceptedOrderTruthResolution("truth_gap", None, payload)
        payload["fill_reconciliation_result"] = "missing_or_zero_fill"
    else:
        payload["fill_reconciliation_result"] = "not_available"

    live_positions: dict[str, dict[str, Any]] = {}
    open_order_count = 0
    open_order_counts_by_venue: dict[str, int] = {}
    live_quantity = 0.0
    signed_live_size = 0.0
    for probe_venue in probe_venues:
        probe_adapter = get_adapter(probe_venue)
        if probe_adapter is None:
            continue
        try:
            live_position = await probe_adapter.fetch_position(symbol)
            open_orders = await fetch_open_orders(probe_adapter, probe_venue, symbol)
        except Exception as exc:
            payload["live_truth_error"] = str(exc) or exc.__class__.__name__
            decision = ORDER_TRUTH_LEDGER.truth_gap_status_decision("truth_unavailable")
            payload["resolution_state"] = decision.state
            payload["ledger_decision"] = decision.decision
            return AcceptedOrderTruthResolution("truth_unavailable", None, payload)
        live_positions[probe_venue.value] = position_snapshot_evidence(live_position)
        position_quantity = abs(float(getattr(live_position, "quantity", 0.0) or 0.0))
        live_quantity = max(live_quantity, position_quantity)
        if probe_venue == venue and live_position is not None:
            signed_live_size = (
                position_quantity
                if getattr(live_position, "side", None) == Side.BUY
                else -position_quantity
            )
        venue_open_order_count = len(open_orders)
        open_order_count += venue_open_order_count
        open_order_counts_by_venue[probe_venue.value] = venue_open_order_count

    if live_excess_mode == "residual_baseline":
        if side == Side.SELL:
            live_excess_quantity = max(signed_live_size - baseline_quantity, 0.0)
        else:
            live_excess_quantity = max(baseline_quantity - signed_live_size, 0.0)
    else:
        live_excess_quantity = live_quantity

    payload.update(
        {
            "open_order_count": open_order_count,
            "open_order_counts_by_venue": open_order_counts_by_venue,
            "live_truth_venues": [venue.value for venue in probe_venues],
            "live_positions": live_positions,
            "live_quantity": live_quantity,
            "live_excess_quantity": live_excess_quantity,
            "baseline_quantity": baseline_quantity,
            "live_size": signed_live_size,
        }
    )
    if open_order_count > 0:
        decision = ORDER_TRUTH_LEDGER.truth_gap_status_decision("open_order_present")
        payload["resolution_state"] = decision.state
        payload["ledger_decision"] = decision.decision
        return AcceptedOrderTruthResolution("open_order_present", None, payload)
    if live_excess_quantity <= 1e-9:
        decision = ORDER_TRUTH_LEDGER.truth_gap_status_decision("live_flat")
        payload["resolution_state"] = decision.state
        payload["ledger_decision"] = decision.decision
        return AcceptedOrderTruthResolution("live_flat", None, payload)
    decision = ORDER_TRUTH_LEDGER.truth_gap_status_decision("truth_gap")
    payload["resolution_state"] = decision.state
    payload["ledger_decision"] = decision.decision
    return AcceptedOrderTruthResolution("truth_gap", None, payload)
