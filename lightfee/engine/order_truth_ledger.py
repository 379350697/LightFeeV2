"""Shared order-truth ledger for submit uncertainty and duplicate ids."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Mapping

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


class OrderTruthFillStatus(StrEnum):
    """Explicit exchange-truth result for order success decisions."""

    CONFIRMED_FILL = "confirmed_fill"
    CONFIRMED_NO_FILL = "confirmed_no_fill"
    TRUTH_GAP = "truth_gap"
    LIVE_POSITION_PRESENT = "live_position_present"
    LIVE_FLAT = "live_flat"
    UNSUPPORTED_FAIL_CLOSED = "unsupported_fail_closed"


class OrderTruthEvidenceStatus(StrEnum):
    """Whether the fill decision is backed by fresh exchange truth."""

    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class OrderTruthDecision:
    """Owner-neutral next action for uncertain order truth."""

    state: str
    classification: str
    decision: str
    target_qty: float = 0.0
    reconciled_qty: float = 0.0
    live_qty: float = 0.0
    remaining_qty: float = 0.0
    retry_qty: float = 0.0
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


@dataclass(frozen=True)
class OrderTruthSuccessDecision:
    """Order success decision with fill truth separated from weak order evidence."""

    fill_status: OrderTruthFillStatus
    evidence_status: OrderTruthEvidenceStatus
    decision: str
    venue: Venue | None = None
    symbol: str = ""
    target_qty: float = 0.0
    reconciled_qty: float = 0.0
    live_qty: float = 0.0
    remaining_qty: float = 0.0
    order_id: str = ""
    client_order_id: str = ""
    average_price: float = 0.0
    queried_endpoints: tuple[str, ...] = ()
    missing_evidence: tuple[str, ...] = ()
    response_classification: str = ""
    terminal_without_truth: bool = False

    @property
    def confirmed_fill(self) -> bool:
        return (
            self.fill_status == OrderTruthFillStatus.CONFIRMED_FILL
            and self.reconciled_qty > 1e-9
        )


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

    def accepted_order_state(self, error: Exception) -> str:
        if bool(getattr(error, "order_ack_only", False)):
            return "ack_only_accepted"
        if self.is_order_truth_gap(error):
            return "truth_gap"
        return "unresolved"

    def truth_gap_status_decision(self, status: str) -> OrderTruthDecision:
        status_text = str(status or "").lower()
        if status_text in {"filled", "accepted_order_reconciled"}:
            return OrderTruthDecision(
                state="resolved_flat",
                classification=status_text,
                decision="clear",
            )
        if status_text in {"live_flat", "accepted_order_live_flat"}:
            return OrderTruthDecision(
                state="resolved_flat",
                classification=status_text,
                decision="clear_live_flat",
            )
        if status_text in {"open_order_present", "position_present", "live_position_present"}:
            return OrderTruthDecision(
                state="resolved_position",
                classification=status_text,
                decision="retain",
            )
        return OrderTruthDecision(
            state="unresolved",
            classification=status_text or "truth_gap",
            decision="backoff_recheck",
        )

    def resolve_order_success(
        self,
        *,
        venue: Venue,
        symbol: str,
        order_id: str = "",
        client_order_id: str = "",
        target_qty: float = 0.0,
        reconciliation: Any = None,
        metadata: Mapping[str, Any] | None = None,
        live_position: Any = None,
        open_order_present: bool | None = None,
    ) -> OrderTruthSuccessDecision:
        """Resolve order success from fill truth, live truth, and weak evidence.

        ACKs, order-detail status, and positive detail-only counters are not fill
        proof. A confirmed fill requires a positive reconciliation quantity from
        an exchange fill/execution path or a venue adapter that has already
        normalized fill truth into OrderFillReconciliation.
        """

        reconciliation_metadata = (
            getattr(reconciliation, "metadata", None)
            if reconciliation is not None
            else None
        )
        merged_metadata: dict[str, Any] = {}
        if isinstance(reconciliation_metadata, Mapping):
            merged_metadata.update(reconciliation_metadata)
        if isinstance(metadata, Mapping):
            merged_metadata.update(metadata)

        reconciled_qty = _positive_float(getattr(reconciliation, "quantity", 0.0))
        target_qty = max(_positive_float(target_qty), 0.0)
        live_qty = _positive_float(getattr(live_position, "quantity", 0.0))
        average_price = _positive_float(
            getattr(
                reconciliation,
                "average_price",
                getattr(reconciliation, "price", 0.0),
            )
        )
        resolved_order_id = str(
            getattr(reconciliation, "order_id", "") or order_id or ""
        )
        resolved_client_order_id = str(
            getattr(reconciliation, "client_order_id", None)
            or client_order_id
            or ""
        )
        endpoints = _tuple_texts(merged_metadata.get("queried_endpoints"))
        classification = str(
            merged_metadata.get("response_classification")
            or merged_metadata.get("classification")
            or merged_metadata.get("raw_exchange_status")
            or ""
        )
        evidence_source = str(merged_metadata.get("evidence_source") or "")
        raw_status = str(merged_metadata.get("raw_exchange_status") or "")
        classification_text = " ".join(
            part.lower()
            for part in (classification, evidence_source, raw_status)
            if part
        )

        if _classification_is_fail_closed(classification_text):
            return OrderTruthSuccessDecision(
                fill_status=OrderTruthFillStatus.UNSUPPORTED_FAIL_CLOSED,
                evidence_status=OrderTruthEvidenceStatus.UNAVAILABLE,
                decision="retain_fail_closed",
                venue=venue,
                symbol=symbol,
                target_qty=target_qty,
                live_qty=live_qty,
                remaining_qty=target_qty,
                order_id=resolved_order_id,
                client_order_id=resolved_client_order_id,
                queried_endpoints=endpoints,
                missing_evidence=("supported_fill_truth",),
                response_classification=classification,
                terminal_without_truth=False,
            )

        if (
            reconciliation is not None
            and reconciled_qty > 1e-9
            and not _classification_is_weak_fill_source(classification_text)
        ):
            return OrderTruthSuccessDecision(
                fill_status=OrderTruthFillStatus.CONFIRMED_FILL,
                evidence_status=OrderTruthEvidenceStatus.AVAILABLE,
                decision="terminal_fill",
                venue=venue,
                symbol=symbol,
                target_qty=target_qty,
                reconciled_qty=reconciled_qty,
                live_qty=live_qty,
                remaining_qty=max(target_qty - reconciled_qty, 0.0),
                order_id=resolved_order_id,
                client_order_id=resolved_client_order_id,
                average_price=average_price,
                queried_endpoints=endpoints,
                response_classification=classification,
                terminal_without_truth=False,
            )

        if live_qty > 1e-9:
            return OrderTruthSuccessDecision(
                fill_status=OrderTruthFillStatus.LIVE_POSITION_PRESENT,
                evidence_status=OrderTruthEvidenceStatus.AVAILABLE,
                decision="retain_owned_cleanup",
                venue=venue,
                symbol=symbol,
                target_qty=target_qty,
                live_qty=live_qty,
                remaining_qty=target_qty,
                order_id=resolved_order_id,
                client_order_id=resolved_client_order_id,
                queried_endpoints=endpoints,
                missing_evidence=("fill_confirmation",),
                response_classification=classification,
                terminal_without_truth=False,
            )

        if "live_flat" in classification_text or (
            open_order_present is False and "no_open_order" in classification_text
        ):
            return OrderTruthSuccessDecision(
                fill_status=OrderTruthFillStatus.LIVE_FLAT,
                evidence_status=OrderTruthEvidenceStatus.AVAILABLE,
                decision="terminal_no_fill",
                venue=venue,
                symbol=symbol,
                target_qty=target_qty,
                live_qty=live_qty,
                remaining_qty=target_qty,
                order_id=resolved_order_id,
                client_order_id=resolved_client_order_id,
                queried_endpoints=endpoints,
                missing_evidence=(),
                response_classification=classification,
                terminal_without_truth=False,
            )

        missing_evidence = _missing_order_truth_evidence(
            venue=venue,
            classification_text=classification_text,
            endpoints=endpoints,
        )
        return OrderTruthSuccessDecision(
            fill_status=OrderTruthFillStatus.TRUTH_GAP,
            evidence_status=OrderTruthEvidenceStatus.UNAVAILABLE,
            decision="retain_backoff",
            venue=venue,
            symbol=symbol,
            target_qty=target_qty,
            live_qty=live_qty,
            remaining_qty=target_qty,
            order_id=resolved_order_id,
            client_order_id=resolved_client_order_id,
            queried_endpoints=endpoints,
            missing_evidence=missing_evidence,
            response_classification=classification,
            terminal_without_truth=False,
        )

    def duplicate_reconcile_state(self, *, classification: str, decision: str) -> str:
        classification_text = str(classification or "").lower()
        decision_text = str(decision or "").lower()
        if decision_text in {"clear", "clear_live_flat"}:
            return "resolved_flat"
        if decision_text in {"retry_new_client_order_id", "retain"}:
            return "resolved_position"
        if decision_text in {"backoff_recheck", "reconcile_before_terminal"}:
            return "unresolved"
        if classification_text == "full":
            return "resolved_flat"
        if classification_text in {"partial", "stale_full_live_nonzero"}:
            return "resolved_position"
        return "unresolved"

    def resolve_duplicate_conflict(
        self,
        *,
        venue: Venue,
        symbol: str,
        client_order_id: str,
        target_qty: float,
        reconciliation: Any = None,
        live_pos_before: Any = None,
        live_pos_after: Any = None,
        reconcile_error: str = "",
        live_fetch_error: str = "",
        live_fetch_attempted: bool = False,
    ) -> OrderTruthDecision:
        """Resolve duplicate client-id conflicts from order, fill, and position truth."""

        reconciled_qty = _positive_float(getattr(reconciliation, "quantity", 0.0))
        target_qty = max(_positive_float(target_qty), 0.0)
        remaining_qty = max(target_qty - reconciled_qty, 0.0)
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
            state = "unresolved"
            classification = "unknown_transient"
            decision = "backoff_recheck"
        elif (
            reconciled_qty >= max(target_qty - 1e-9, 0.0)
            and reconciled_qty > 0.0
            and live_qty > 1e-9
        ):
            state = "resolved_position"
            classification = "stale_full_live_nonzero"
            decision = "retry_new_client_order_id"
            remaining_qty = live_qty
        elif reconciled_qty >= max(target_qty - 1e-9, 0.0) and reconciled_qty > 0.0:
            state = "resolved_flat"
            classification = "full"
            decision = "clear_live_flat"
        elif live_flat:
            state = "resolved_flat"
            classification = "none" if reconciled_qty <= 1e-9 else "partial"
            decision = "clear_live_flat"
        elif reconciled_qty > 1e-9:
            state = "resolved_position"
            classification = "partial"
            decision = "retry_new_client_order_id"
        elif reconcile_error or live_fetch_error:
            state = "unresolved"
            classification = "unknown_transient"
            decision = "backoff_recheck"
        else:
            state = "unresolved"
            classification = "none"
            decision = "backoff_recheck"

        retry_qty = remaining_qty
        if decision == "retry_new_client_order_id" and live_qty > 1e-9:
            retry_qty = min(remaining_qty, live_qty)

        return OrderTruthDecision(
            state=state,
            classification=classification,
            decision=decision,
            target_qty=target_qty,
            reconciled_qty=reconciled_qty,
            live_qty=live_qty,
            remaining_qty=remaining_qty,
            retry_qty=retry_qty,
            live_side=live_side,
            order_id=(
                getattr(reconciliation, "order_id", "")
                if reconciliation is not None
                else ""
            ),
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
        payload["order_truth_state"] = self.accepted_order_state(error)
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
        payload["ledger_decision"] = "backoff_recheck"
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
        decision = str(getattr(result, "decision", "") or "")
        normalized_reason = self.normalize_duplicate_reason(reason)
        return {
            "venue": venue.value,
            "symbol": symbol,
            "order_truth_state": str(
                getattr(result, "order_truth_state", "")
                or
                getattr(result, "state", "")
                or self.duplicate_reconcile_state(
                    classification=classification,
                    decision=decision,
                )
            ),
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
            "next_action": decision,
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


def _positive_float(value: Any) -> float:
    if value is None or not isinstance(value, (int, float, str)):
        return 0.0
    try:
        parsed = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if parsed < 0.0:
        return abs(parsed)
    return parsed


def _tuple_texts(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,) if value else ()
    if isinstance(value, Mapping):
        return tuple(str(key) for key in value if str(key))
    try:
        return tuple(str(item) for item in value if str(item))
    except TypeError:
        text = str(value)
        return (text,) if text else ()


def _classification_is_fail_closed(text: str) -> bool:
    return any(
        marker in text
        for marker in (
            "positive_quantity_missing_side",
            "missing_side",
            "invalid_side",
            "family_unresolved",
            "account_identity_mismatch",
            "account_mismatch",
            "account_address_missing",
            "unsupported_fail_closed",
            "unsupported",
        )
    )


def _classification_is_weak_fill_source(text: str) -> bool:
    return any(
        marker in text
        for marker in (
            "okx_order_detail",
            "bybit_order_realtime",
            "bybit_order_history",
            "binance_order_status",
            "aster_order_status",
            "bitget_order_detail",
            "gate_order_detail",
            "detail_found;fills_empty",
            "fills_empty",
            "execution_not_found",
            "accepted_ack_without_execution",
            "stale_accepted_order",
            "new",
            "pending_new",
        )
    )


def _missing_order_truth_evidence(
    *,
    venue: Venue,
    classification_text: str,
    endpoints: tuple[str, ...],
) -> tuple[str, ...]:
    missing: list[str] = []
    endpoint_text = " ".join(endpoint.lower() for endpoint in endpoints)
    if venue == Venue.OKX and "fills" not in endpoint_text:
        missing.append("okx_fills_or_fills_history")
    elif venue == Venue.BYBIT and "execution" not in endpoint_text:
        missing.append("bybit_execution_list")
    elif venue in {Venue.BINANCE, Venue.ASTER}:
        missing.extend(("executed_qty_positive", "open_order_or_position_truth"))
    elif venue == Venue.BITGET:
        missing.append("bitget_fill_truth_with_side_and_family")
    elif venue == Venue.GATE:
        missing.append("gate_order_open_position_truth")
    elif venue == Venue.HYPERLIQUID:
        missing.append("hyperliquid_configured_account_order_truth")
    if "fills_empty" in classification_text or "execution_not_found" in classification_text:
        missing.append("positive_exchange_fill")
    if "stale_accepted_order" in classification_text or "new" in classification_text:
        missing.append("terminal_order_or_live_position_truth")
    if "accepted_ack" in classification_text or "ack_without_execution" in classification_text:
        missing.append("post_ack_execution_truth")
    if not missing:
        missing.append("fill_confirmation")
    return tuple(dict.fromkeys(missing))


ORDER_TRUTH_LEDGER = OrderTruthLedger()
