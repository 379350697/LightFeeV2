"""Shared runtime exchange-truth snapshot shapes.

This module owns the importable data shape used by runtime code, diagnostics,
and production verification. CLI scripts may render the payload, but they should
not create a separate business interpretation of exchange truth.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from lightfee.core.domain import Venue
from lightfee.venues.specs import VenueOperation, get_operation_contract, get_spec
from lightfee.venues.transport import TransportError, TransportErrorCategory


@dataclass(frozen=True)
class VenueOperationRequest:
    method: str
    path: str
    params: dict[str, Any] = field(default_factory=dict)
    body: dict[str, Any] = field(default_factory=dict)
    private: bool = True

    @property
    def label(self) -> str:
        suffix = ""
        if self.path == "/info":
            info_type = self.body.get("type")
            if info_type:
                suffix = f" {info_type}"
        return f"{self.method} {self.path}{suffix}"


def build_venue_operation_request(
    venue: Venue,
    operation: VenueOperation,
    *,
    symbol: str = "",
    account_address: str = "",
    agent_wallet_address: str = "",
    resolved_account_family: Any = None,
) -> VenueOperationRequest:
    """Build a private-truth request from the venue operation contract."""
    spec = get_spec(venue)
    contract = get_operation_contract(
        spec,
        operation,
        resolved_account_family=resolved_account_family,
    )
    if not contract.supported:
        raise NotImplementedError(f"{venue.value}:{operation.value}:unsupported")

    venue_symbol = _venue_contract_symbol(spec, symbol)
    params: dict[str, Any] = {}
    body: dict[str, Any] = {}
    fixed = body if contract.payload == "body" and contract.path == "/info" else params
    for item in contract.required_params:
        if "=" not in item:
            continue
        key, raw_value = item.split("=", 1)
        fixed[key] = _resolve_contract_param_value(
            raw_value,
            venue_symbol=venue_symbol,
            account_address=account_address,
            agent_wallet_address=agent_wallet_address,
        )

    if venue in (Venue.BINANCE, Venue.ASTER):
        if venue_symbol:
            params.setdefault("symbol", venue_symbol)
    elif venue == Venue.BYBIT:
        params.setdefault("category", "linear")
        if operation in (VenueOperation.OPEN_ORDERS, VenueOperation.ORDER_STATUS):
            params.setdefault("settleCoin", "USDT")
        if venue_symbol:
            params.setdefault("symbol", venue_symbol)
    elif venue == Venue.OKX:
        if venue_symbol:
            params.setdefault("instId", venue_symbol)
        elif operation in (VenueOperation.OPEN_ORDERS, VenueOperation.POSITION):
            params.setdefault("instType", "SWAP")
    elif venue == Venue.BITGET:
        if venue_symbol:
            params.setdefault("symbol", venue_symbol)
    elif venue == Venue.GATE:
        if operation == VenueOperation.OPEN_ORDERS:
            params.setdefault("status", "open")
        if venue_symbol:
            params.setdefault("contract", venue_symbol)

    return VenueOperationRequest(
        method=contract.method,
        path=contract.path,
        params=params,
        body=body,
        private=contract.private,
    )


async def request_venue_operation(
    transport: Any,
    venue: Venue,
    operation: VenueOperation,
    *,
    symbol: str = "",
    account_address: str = "",
    agent_wallet_address: str = "",
    resolved_account_family: Any = None,
) -> tuple[Any, VenueOperationRequest]:
    if venue == Venue.BITGET and resolved_account_family is None:
        resolver = getattr(transport, "_bitget_resolve_contract_family", None)
        if callable(resolver):
            resolved_account_family = await resolver()
        else:
            raise TransportError(
                TransportErrorCategory.REQUEST_REJECTED,
                "Bitget private truth requires an explicit account family resolver",
                status_code=400,
                body='{"code":"LFV2_BITGET_FAMILY_UNRESOLVED","msg":"missing Bitget account family resolver"}',
            )
    request = build_venue_operation_request(
        venue,
        operation,
        symbol=symbol,
        account_address=account_address,
        agent_wallet_address=agent_wallet_address,
        resolved_account_family=resolved_account_family,
    )
    kwargs: dict[str, Any] = {"private": request.private}
    if request.params:
        kwargs["params"] = request.params
    elif request.method == "GET":
        kwargs["params"] = {}
    if request.body:
        kwargs["body"] = request.body
    raw = await transport._request(request.method, request.path, **kwargs)
    return raw, request


def _venue_contract_symbol(spec: Any, symbol: str) -> str:
    if not symbol:
        return ""
    convert = getattr(spec, "symbol_to_venue", None)
    if callable(convert):
        try:
            return str(convert(symbol))
        except Exception:
            return symbol
    return symbol


def _resolve_contract_param_value(
    raw_value: str,
    *,
    venue_symbol: str,
    account_address: str,
    agent_wallet_address: str,
) -> str:
    if raw_value == "configured_account_address":
        return account_address
    if raw_value == "agent_wallet_address":
        return agent_wallet_address
    if raw_value == "coin":
        return venue_symbol
    return raw_value


@dataclass(frozen=True)
class ExchangeTruthPosition:
    venue: str
    symbol: str
    side: str
    quantity: float
    entry_price: float = 0.0
    observed_at_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "venue": self.venue.lower(),
            "symbol": self.symbol.upper(),
            "side": self.side.lower(),
            "quantity": float(self.quantity),
            "entry_price": float(self.entry_price),
            "observed_at_ms": int(self.observed_at_ms or 0),
        }


@dataclass(frozen=True)
class ExchangeTruthOpenOrder:
    venue: str
    symbol: str
    side: str
    quantity: float
    price: float = 0.0
    reduce_only: bool = False
    order_id: str = ""
    client_order_id: str = ""
    observed_at_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "venue": self.venue.lower(),
            "symbol": self.symbol.upper(),
            "side": self.side.lower(),
            "quantity": float(self.quantity),
            "price": float(self.price),
            "reduce_only": bool(self.reduce_only),
            "order_id": self.order_id,
            "client_order_id": self.client_order_id,
            "observed_at_ms": int(self.observed_at_ms or 0),
        }


@dataclass(frozen=True)
class ExchangeTruthProbeEvidence:
    venue: str
    symbol: str = ""
    endpoint: str = ""
    method: str = ""
    timeout_budget_s: float = 0.0
    started_at_ms: int = 0
    finished_at_ms: int = 0
    classification: str = ""
    error: str = ""

    @property
    def unsupported_symbol(self) -> bool:
        return self.classification.startswith("unsupported_symbol")

    @property
    def timed_out(self) -> bool:
        text = "{} {}".format(self.classification, self.error).lower()
        return "timeout" in text or "timed out" in text

    def to_dict(self) -> dict[str, Any]:
        return {
            "venue": self.venue.lower(),
            "symbol": self.symbol.upper(),
            "endpoint": self.endpoint,
            "method": self.method,
            "timeout_budget_s": float(self.timeout_budget_s or 0.0),
            "started_at_ms": int(self.started_at_ms or 0),
            "finished_at_ms": int(self.finished_at_ms or 0),
            "classification": self.classification,
            "error": self.error,
            "unsupported_symbol": self.unsupported_symbol,
            "timed_out": self.timed_out,
        }


@dataclass(frozen=True)
class ExchangeTruthSnapshot:
    available: bool
    confidence: str = "low"
    schema_version: str = ""
    snapshot_version: Any | None = None
    venues: tuple[str, ...] = ()
    positions: tuple[ExchangeTruthPosition, ...] = ()
    open_orders: tuple[ExchangeTruthOpenOrder, ...] = ()
    probe_evidence: tuple[ExchangeTruthProbeEvidence, ...] = ()
    fetch_status: Mapping[str, Any] = field(default_factory=dict)
    errors: tuple[str, ...] = ()
    missing_evidence: tuple[str, ...] = ()

    @property
    def truth_available(self) -> bool:
        return bool(self.available)

    def to_legacy_payload(self) -> dict[str, Any]:
        positions: dict[str, dict[str, Any]] = {}
        for position in self.positions:
            item = position.to_dict()
            positions.setdefault(item["venue"], {})[item["symbol"]] = item

        open_orders: dict[str, dict[str, list[dict[str, Any]]]] = {}
        for order in self.open_orders:
            item = order.to_dict()
            open_orders.setdefault(item["venue"], {}).setdefault(item["symbol"], []).append(item)

        evidence = [item.to_dict() for item in self.probe_evidence]
        payload = {
            "available": bool(self.available),
            "truth_available": self.truth_available,
            "available_venues": list(self.venues),
            "confidence": self.confidence,
            "positions": positions,
            "open_orders": open_orders,
            "probe_evidence": evidence,
            "position_probe_evidence": _nested_probe_evidence(evidence, "position"),
            "open_order_probe_evidence": _nested_probe_evidence(evidence, "open_order"),
            "has_nonzero_position": any(abs(item.quantity) > 1e-9 for item in self.positions),
            "has_open_order": any(abs(item.quantity) > 1e-9 for item in self.open_orders),
            "fetch_status": dict(self.fetch_status),
            "errors": list(self.errors),
            "missing_evidence": list(self.missing_evidence),
        }
        if self.schema_version:
            payload["schema_version"] = self.schema_version
        if self.snapshot_version is not None:
            payload["snapshot_version"] = self.snapshot_version
        return normalize_exchange_truth_payload(payload)


def normalize_exchange_truth_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return a backwards-compatible payload with the shared truth fields added."""
    normalized = dict(payload)
    normalized["available"] = bool(normalized.get("available", False))
    normalized["truth_available"] = bool(
        normalized.get("truth_available", normalized["available"])
    )
    normalized.setdefault("confidence", "low")
    normalized.setdefault("positions", {})
    normalized.setdefault("open_orders", {})
    normalized["errors"] = _merge_fetch_status_errors(
        normalized.get("errors", []),
        normalized.get("fetch_status", {}),
    )
    normalized.setdefault("missing_evidence", [])
    normalized.setdefault(
        "has_nonzero_position",
        _has_nonzero_position(normalized.get("positions", {})),
    )
    normalized.setdefault(
        "has_open_order",
        _has_open_order(normalized.get("open_orders", {})),
    )
    if "available_venues" not in normalized:
        normalized["available_venues"] = _available_venues_from_status(
            normalized.get("fetch_status", {})
        )
    if "probe_evidence" not in normalized:
        normalized["probe_evidence"] = _flatten_probe_evidence(
            normalized.get("position_probe_evidence", {}),
            normalized.get("open_order_probe_evidence", {}),
        )
    else:
        normalized["probe_evidence"] = [
            _normalize_probe_evidence(item)
            for item in normalized.get("probe_evidence", [])
            if isinstance(item, Mapping)
        ]
    return normalized


def snapshot_from_legacy_payload(payload: Mapping[str, Any]) -> ExchangeTruthSnapshot:
    normalized = normalize_exchange_truth_payload(payload)
    positions: list[ExchangeTruthPosition] = []
    for venue, venue_positions in (normalized.get("positions") or {}).items():
        if not isinstance(venue_positions, Mapping):
            continue
        for symbol, item in venue_positions.items():
            if not isinstance(item, Mapping):
                continue
            positions.append(
                ExchangeTruthPosition(
                    venue=str(item.get("venue") or venue),
                    symbol=str(item.get("symbol") or symbol),
                    side=str(item.get("side") or ""),
                    quantity=_float(item.get("quantity")),
                    entry_price=_float(item.get("entry_price")),
                    observed_at_ms=int(item.get("observed_at_ms") or 0),
                )
            )

    open_orders: list[ExchangeTruthOpenOrder] = []
    for venue, venue_orders in (normalized.get("open_orders") or {}).items():
        if not isinstance(venue_orders, Mapping):
            continue
        for symbol, rows in venue_orders.items():
            for row in rows if isinstance(rows, list) else []:
                if not isinstance(row, Mapping):
                    continue
                open_orders.append(
                    ExchangeTruthOpenOrder(
                        venue=str(row.get("venue") or venue),
                        symbol=str(row.get("symbol") or symbol),
                        side=str(row.get("side") or ""),
                        quantity=_float(row.get("quantity")),
                        price=_float(row.get("price")),
                        reduce_only=bool(row.get("reduce_only")),
                        order_id=str(row.get("order_id") or ""),
                        client_order_id=str(row.get("client_order_id") or ""),
                        observed_at_ms=int(row.get("observed_at_ms") or 0),
                    )
                )

    return ExchangeTruthSnapshot(
        available=bool(normalized.get("available")),
        confidence=str(normalized.get("confidence") or "low"),
        schema_version=str(normalized.get("schema_version") or ""),
        snapshot_version=normalized.get("snapshot_version"),
        venues=tuple(str(v) for v in normalized.get("available_venues", []) or []),
        positions=tuple(positions),
        open_orders=tuple(open_orders),
        probe_evidence=tuple(
            ExchangeTruthProbeEvidence(
                venue=str(item.get("venue") or ""),
                symbol=str(item.get("symbol") or ""),
                endpoint=str(item.get("endpoint") or ""),
                method=str(item.get("method") or ""),
                timeout_budget_s=_float(item.get("timeout_budget_s")),
                started_at_ms=int(item.get("started_at_ms") or 0),
                finished_at_ms=int(item.get("finished_at_ms") or 0),
                classification=str(item.get("classification") or ""),
                error=str(item.get("error") or ""),
            )
            for item in normalized.get("probe_evidence", [])
            if isinstance(item, Mapping)
        ),
        fetch_status=dict(normalized.get("fetch_status", {}) or {}),
        errors=tuple(str(item) for item in normalized.get("errors", []) or []),
        missing_evidence=tuple(
            str(item) for item in normalized.get("missing_evidence", []) or []
        ),
    )


def _nested_probe_evidence(
    evidence: list[dict[str, Any]], family: str
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for item in evidence:
        classification = str(item.get("classification") or "")
        if family == "position" and "position" not in classification and "flat" not in classification:
            continue
        if family == "open_order" and "open_order" not in classification:
            continue
        venue = str(item.get("venue") or "").lower()
        symbol = str(item.get("symbol") or "*").upper() or "*"
        if not venue:
            continue
        result.setdefault(venue, {})[symbol] = dict(item)
    return result


def _flatten_probe_evidence(
    position_evidence: Any,
    open_order_evidence: Any,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    items.extend(_flatten_probe_evidence_family(position_evidence, "position"))
    items.extend(_flatten_probe_evidence_family(open_order_evidence, "open_order"))
    return items


def _flatten_probe_evidence_family(value: Any, family: str) -> list[dict[str, Any]]:
    if not isinstance(value, Mapping):
        return []
    flattened: list[dict[str, Any]] = []
    for venue, by_symbol in value.items():
        if not isinstance(by_symbol, Mapping):
            continue
        for symbol, evidence in by_symbol.items():
            if not isinstance(evidence, Mapping):
                continue
            item = dict(evidence)
            item.setdefault("venue", venue)
            item.setdefault("symbol", symbol)
            item.setdefault("probe_family", family)
            flattened.append(_normalize_probe_evidence(item))
    return flattened


def _normalize_probe_evidence(item: Mapping[str, Any]) -> dict[str, Any]:
    evidence = ExchangeTruthProbeEvidence(
        venue=str(item.get("venue") or ""),
        symbol=str(item.get("symbol") or ""),
        endpoint=str(item.get("endpoint") or item.get("method_name") or ""),
        method=str(item.get("method") or ""),
        timeout_budget_s=_float(item.get("timeout_budget_s")),
        started_at_ms=int(item.get("started_at_ms") or 0),
        finished_at_ms=int(item.get("finished_at_ms") or 0),
        classification=str(item.get("classification") or ""),
        error=str(item.get("error") or ""),
    ).to_dict()
    if "probe_family" in item:
        evidence["probe_family"] = item["probe_family"]
    return evidence


def _has_nonzero_position(positions: Any) -> bool:
    if not isinstance(positions, Mapping):
        return False
    for venue_positions in positions.values():
        if not isinstance(venue_positions, Mapping):
            continue
        for item in venue_positions.values():
            if isinstance(item, Mapping) and abs(_float(item.get("quantity"))) > 1e-9:
                return True
    return False


def _has_open_order(open_orders: Any) -> bool:
    if not isinstance(open_orders, Mapping):
        return False
    for venue_orders in open_orders.values():
        if not isinstance(venue_orders, Mapping):
            continue
        for rows in venue_orders.values():
            if isinstance(rows, list) and rows:
                return True
    return False


def _available_venues_from_status(fetch_status: Any) -> list[str]:
    if not isinstance(fetch_status, Mapping):
        return []
    return [
        str(venue)
        for venue, status in fetch_status.items()
        if isinstance(status, Mapping)
        and status.get("status") in {"ok", "partial"}
    ]


def _merge_fetch_status_errors(existing: Any, fetch_status: Any) -> list[str]:
    errors = [str(item) for item in existing or []]
    seen = set(errors)
    if not isinstance(fetch_status, Mapping):
        return errors
    for venue, status in fetch_status.items():
        if not isinstance(status, Mapping):
            continue
        error = str(status.get("error") or "")
        if not error:
            continue
        item = f"{venue}: {error}"
        if item in seen:
            continue
        seen.add(item)
        errors.append(item)
    return errors


def _float(value: Any) -> float:
    try:
        result = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if result != result:
        return 0.0
    return result
