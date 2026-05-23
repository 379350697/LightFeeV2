"""Exchange error evidence schema — unified structured error capture.

Normalises exchange/API failures across venues so diagnose and Telegram
consume the same structured facts, not free-text grep.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Transport error type
# ---------------------------------------------------------------------------

class TransportErrorType:
    HTTP_STATUS = "http_status"
    TIMEOUT = "timeout"
    NETWORK_ERROR = "network_error"
    EXCHANGE_RETCODE = "exchange_retcode"
    UNKNOWN = "unknown"


# ---------------------------------------------------------------------------
# Completeness flags
# ---------------------------------------------------------------------------

class EvidenceCompleteness:
    COMPLETE = "complete"
    MISSING_EXCHANGE_BODY = "missing_exchange_body"
    MISSING_EXCHANGE_CODE_OR_MSG = "missing_exchange_code_or_msg"
    MISSING_BODY = "missing_body"
    MISSING_EXCHANGE_CODE = "missing_exchange_code"
    MISSING_REQUEST_CONTEXT = "missing_request_context"
    UNPARSED_EXCHANGE_BODY = "unparsed_exchange_body"
    PARTIAL = "partial"
    TRANSPORT_ONLY = "transport_only"


# ---------------------------------------------------------------------------
# Request context
# ---------------------------------------------------------------------------

@dataclass
class RequestContext:
    """Order/request parameters captured at submit time for diagnostics."""

    symbol: str = ""
    side: str = ""
    order_type: str = ""
    time_in_force: str = ""
    quantity: float = 0.0
    price: float = 0.0
    reduce_only: bool = False
    post_only: bool = False
    position_side: str = ""
    client_order_id: str = ""
    position_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "order_type": self.order_type,
            "time_in_force": self.time_in_force,
            "quantity": self.quantity,
            "price": self.price,
            "reduce_only": self.reduce_only,
            "post_only": self.post_only,
            "position_side": self.position_side,
            "client_order_id": self.client_order_id,
            "position_id": self.position_id,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> RequestContext:
        return cls(
            symbol=str(d.get("symbol", "")),
            side=str(d.get("side", "")),
            order_type=str(d.get("order_type", d.get("type", ""))),
            time_in_force=str(d.get("time_in_force", "")),
            quantity=float(d.get("quantity", 0)),
            price=float(d.get("price", 0)),
            reduce_only=bool(d.get("reduce_only", False)),
            post_only=bool(d.get("post_only", False)),
            position_side=str(d.get("position_side", "")),
            client_order_id=str(d.get("client_order_id", "")),
            position_id=str(d.get("position_id", "")),
        )

    @classmethod
    def from_order_request(cls, request: Any) -> RequestContext:
        """Build from an OrderRequest (lightfee.core.domain.OrderRequest)."""
        side_raw = getattr(request, "side", "")
        side_str = side_raw.value if hasattr(side_raw, "value") else str(side_raw)
        return cls(
            symbol=getattr(request, "symbol", ""),
            side=side_str,
            order_type=str(getattr(request, "order_type", "")),
            time_in_force=str(getattr(request, "time_in_force", "")),
            quantity=float(getattr(request, "quantity", 0)),
            price=float(getattr(request, "price", 0) or 0),
            reduce_only=bool(getattr(request, "reduce_only", False)),
            post_only=bool(getattr(request, "post_only", False)),
            position_side=str(getattr(request, "position_side", "")),
            client_order_id=str(getattr(request, "client_order_id", "")),
            position_id=str(getattr(request, "position_id", "")),
        )


# ---------------------------------------------------------------------------
# Exchange error evidence
# ---------------------------------------------------------------------------

@dataclass
class ExchangeErrorEvidence:
    """Unified exchange error evidence — venue-agnostic.

    Captures everything available from a failed exchange/API call so
    downstream consumers (diagnose, Telegram) work from facts, not guesses.
    """

    venue: str = ""
    operation: str = ""  # place_order, submit_passive_order, cancel_order, ...
    endpoint: str = ""
    transport_error_type: str = TransportErrorType.UNKNOWN

    # HTTP-layer
    http_status: int = 0
    raw_body: str = ""
    raw_body_truncated: bool = False

    # Exchange-layer
    exchange_code: str = ""
    exchange_msg: str = ""

    # Request context
    request_context: RequestContext = field(default_factory=RequestContext)

    # Completeness
    evidence_completeness: str = EvidenceCompleteness.PARTIAL
    missing_evidence: list[str] = field(default_factory=list)
    confidence: str = "low"  # high / medium / low

    # Arbitrary extra fields (venue-specific, e.g. retCode, sCode, label)
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "operation": self.operation,
            "endpoint": self.endpoint,
            "transport_error_type": self.transport_error_type,
            "http_status": self.http_status,
            "raw_body": self.raw_body,
            "raw_body_truncated": self.raw_body_truncated,
            "exchange_code": self.exchange_code,
            "exchange_msg": self.exchange_msg,
            "request_context": self.request_context.to_dict(),
            "evidence_completeness": self.evidence_completeness,
            "missing_evidence": list(self.missing_evidence),
            "confidence": self.confidence,
            "extra": dict(self.extra),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ExchangeErrorEvidence:
        return cls(
            venue=str(d.get("venue", "")),
            operation=str(d.get("operation", "")),
            endpoint=str(d.get("endpoint", "")),
            transport_error_type=str(
                d.get("transport_error_type", TransportErrorType.UNKNOWN)
            ),
            http_status=int(d.get("http_status", 0)),
            raw_body=str(d.get("raw_body", "")),
            raw_body_truncated=bool(d.get("raw_body_truncated", False)),
            exchange_code=str(d.get("exchange_code", "")),
            exchange_msg=str(d.get("exchange_msg", "")),
            request_context=RequestContext.from_dict(
                d.get("request_context", {})
            ),
            evidence_completeness=str(
                d.get("evidence_completeness", EvidenceCompleteness.PARTIAL)
            ),
            missing_evidence=list(d.get("missing_evidence", [])),
            confidence=str(d.get("confidence", "low")),
            extra=dict(d.get("extra", {})),
        )

    def assess_completeness(self) -> None:
        """Recompute evidence_completeness, missing_evidence, and confidence."""
        _extract_exchange_fields(self)
        missing: list[str] = []

        has_body = bool(self.raw_body)
        has_code = bool(self.exchange_code)
        has_msg = bool(self.exchange_msg)
        has_req_ctx = bool(self.request_context.symbol)
        body_parseable = self._body_is_parseable() if has_body else False

        if not has_body:
            missing.append("exchange_response_body")
        else:
            if not body_parseable:
                missing.append("exchange_response_body_unparseable")
            elif not has_code and not has_msg:
                missing.append("exchange_code_or_msg")

        if not has_code and not has_msg:
            if "exchange_code_or_msg" not in missing:
                missing.append("exchange_code_or_msg")
        if not has_code:
            missing.append("exchange_error_code")
        if not has_msg:
            missing.append("exchange_error_msg")
        if not has_req_ctx:
            missing.append("request_context")

        self.missing_evidence = missing

        transport_only_no_body = (
            self.transport_error_type in (
                TransportErrorType.TIMEOUT,
                TransportErrorType.NETWORK_ERROR,
            )
            or self.http_status in (0, 401)
        )

        if not missing:
            self.evidence_completeness = EvidenceCompleteness.COMPLETE
        elif not has_body and not has_code and transport_only_no_body:
            self.evidence_completeness = EvidenceCompleteness.TRANSPORT_ONLY
        elif not has_body:
            self.evidence_completeness = EvidenceCompleteness.MISSING_EXCHANGE_BODY
        elif has_body and not body_parseable:
            self.evidence_completeness = EvidenceCompleteness.UNPARSED_EXCHANGE_BODY
        elif not has_code and not has_msg:
            self.evidence_completeness = EvidenceCompleteness.MISSING_EXCHANGE_CODE_OR_MSG
        elif not has_code:
            self.evidence_completeness = EvidenceCompleteness.MISSING_EXCHANGE_CODE
        else:
            self.evidence_completeness = EvidenceCompleteness.PARTIAL

        if self.evidence_completeness == EvidenceCompleteness.COMPLETE:
            self.confidence = "high"
        elif self.evidence_completeness == EvidenceCompleteness.TRANSPORT_ONLY:
            self.confidence = "low"
        else:
            self.confidence = "medium"

    def _body_is_parseable(self) -> bool:
        import json as _json
        if not self.raw_body:
            return False
        try:
            parsed = _json.loads(self.raw_body)
            return isinstance(parsed, dict)
        except (_json.JSONDecodeError, ValueError):
            return False


# ---------------------------------------------------------------------------
# Factory: build evidence from exceptions / raw data
# ---------------------------------------------------------------------------


def build_evidence_from_transport_error(
    error: Any,
    venue: str = "",
    operation: str = "",
    endpoint: str = "",
    request_context: Optional[RequestContext] = None,
) -> ExchangeErrorEvidence:
    """Build evidence from a TransportError (lightfee.venues.transport.TransportError)."""
    status_code = int(getattr(error, "status_code", 0) or 0)
    body = str(getattr(error, "body", "") or "")
    category = str(getattr(error, "category", "") or "")

    transport_type = TransportErrorType.HTTP_STATUS
    if status_code == 0:
        transport_type = TransportErrorType.UNKNOWN

    evidence = ExchangeErrorEvidence(
        venue=venue,
        operation=operation,
        endpoint=endpoint,
        transport_error_type=transport_type,
        http_status=status_code,
        raw_body=body[:2000] if body else "",
        raw_body_truncated=len(body) > 2000,
        request_context=request_context or RequestContext(),
    )

    # Try to extract exchange code/msg from body
    _extract_exchange_fields(evidence)

    evidence.assess_completeness()
    return evidence


def build_evidence_from_order_submit_error(
    error: Any,
    venue: str = "",
    operation: str = "",
    endpoint: str = "",
    request_context: Optional[RequestContext] = None,
) -> ExchangeErrorEvidence:
    """Build evidence from an OrderSubmitError.

    Tries to extract any TransportError embedded in the exception chain.
    """
    import traceback

    # Walk cause chain for a TransportError
    cause = error
    seen = set()
    transport_err = None
    while cause is not None and id(cause) not in seen:
        seen.add(id(cause))
        cause_name = type(cause).__qualname__
        if cause_name == "TransportError":
            transport_err = cause
            break
        cause = getattr(cause, "__cause__", None)

    if transport_err is not None:
        return build_evidence_from_transport_error(
            transport_err,
            venue=venue,
            operation=operation,
            endpoint=endpoint,
            request_context=request_context,
        )

    # No transport error in chain — build from string
    err_str = str(error)
    evidence = ExchangeErrorEvidence(
        venue=venue,
        operation=operation,
        endpoint=endpoint,
        transport_error_type=TransportErrorType.UNKNOWN,
        raw_body="",
        request_context=request_context or RequestContext(),
    )

    # Try to parse exchange code/msg from error string
    _extract_exchange_fields_from_string(evidence, err_str)

    evidence.assess_completeness()
    return evidence


def build_fallback_evidence(
    error: Any,
    venue: str = "",
    operation: str = "",
    endpoint: str = "",
    request_context: Optional[RequestContext] = None,
) -> ExchangeErrorEvidence:
    """Build fallback evidence when the exception type is unknown.

    Always results in transport_only or partial completeness — never complete.
    """
    err_str = str(error)

    # Check for common transport error attributes
    status_code = int(getattr(error, "status_code", 0) or 0)
    body = str(getattr(error, "body", "") or "")
    response = getattr(error, "response", None)

    if response is not None and hasattr(response, "status_code"):
        status_code = status_code or int(getattr(response, "status_code", 0) or 0)
    if response is not None and hasattr(response, "text"):
        body = body or str(getattr(response, "text", "") or "")

    # Network/timeout detection
    err_lower = err_str.lower()
    if any(kw in err_lower for kw in ("timeout", "timed out", "connect timeout")):
        transport_type = TransportErrorType.TIMEOUT
    elif any(kw in err_lower for kw in (
        "connection", "network", "dns", "refused", "reset", "unreachable",
    )):
        transport_type = TransportErrorType.NETWORK_ERROR
    elif status_code > 0:
        transport_type = TransportErrorType.HTTP_STATUS
    else:
        transport_type = TransportErrorType.UNKNOWN

    evidence = ExchangeErrorEvidence(
        venue=venue,
        operation=operation,
        endpoint=endpoint,
        transport_error_type=transport_type,
        http_status=status_code,
        raw_body=body[:2000] if body else "",
        raw_body_truncated=len(body) > 2000,
        request_context=request_context or RequestContext(),
    )

    _extract_exchange_fields(evidence)
    _extract_exchange_fields_from_string(evidence, err_str)

    evidence.assess_completeness()
    return evidence


def _extract_exchange_fields(evidence: ExchangeErrorEvidence) -> None:
    """Attempt to extract exchange_code/exchange_msg from raw_body JSON.

    Supports venue-specific and generic field names across 30+ exchanges.
    """
    import json as _json

    if not evidence.raw_body:
        return

    try:
        parsed = _json.loads(evidence.raw_body)
    except (_json.JSONDecodeError, ValueError):
        return

    if not isinstance(parsed, dict):
        return

    # Priority-ordered extraction: venue-specific first, then generic

    # Bybit V5: retCode / retMsg
    if "retCode" in parsed:
        evidence.exchange_code = str(parsed.get("retCode", ""))
        evidence.exchange_msg = str(parsed.get("retMsg", ""))
        evidence.extra["retCode"] = evidence.exchange_code
        evidence.extra["retMsg"] = evidence.exchange_msg
        if evidence.transport_error_type in (TransportErrorType.HTTP_STATUS, TransportErrorType.UNKNOWN):
            evidence.transport_error_type = TransportErrorType.EXCHANGE_RETCODE
        return

    # OKX V5: code / msg (code="0" = success)
    if "code" in parsed and "sCode" not in parsed:
        code = str(parsed.get("code", ""))
        msg = str(parsed.get("msg", ""))
        if code != "0":
            evidence.exchange_code = code
            evidence.exchange_msg = msg
            evidence.extra["code"] = code
            evidence.extra["msg"] = msg
            if evidence.transport_error_type in (TransportErrorType.HTTP_STATUS, TransportErrorType.UNKNOWN):
                evidence.transport_error_type = TransportErrorType.EXCHANGE_RETCODE
            return
        # OKX success — fall through to generic extraction for other fields

    # Generic error field extraction — covers Binance, Bitget, Gate, KuCoin, etc.
    _extract_generic_exchange_fields(evidence, parsed)


def _extract_generic_exchange_fields(
    evidence: ExchangeErrorEvidence, parsed: dict[str, Any]
) -> None:
    """Generic exchange error field extraction from parsed JSON body.

    Tries these field names in priority order:
    1. error code: code, errorCode, error, sCode, errCode, ret_code
    2. error message: msg, message, errorMessage, errMsg, retMsg, ret_msg, detail
    """
    # Don't overwrite already-extracted fields
    if evidence.exchange_code:
        return

    # Priority-ordered code field names
    code_candidates = [
        ("code", "msg"),
        ("errorCode", "errorMessage"),
        ("sCode", "sMsg"),
        ("errCode", "errMsg"),
        ("ret_code", "ret_msg"),
        ("label", "message"),
        ("detail", "detail"),
    ]

    for code_key, msg_key in code_candidates:
        code_val = str(parsed.get(code_key, ""))
        if code_val and code_val not in ("0", "00000", "ok", ""):
            msg_val = str(parsed.get(msg_key, ""))
            evidence.exchange_code = code_val
            evidence.exchange_msg = msg_val
            evidence.extra[code_key] = code_val
            evidence.extra[msg_key] = msg_val
            if evidence.transport_error_type in (
                TransportErrorType.HTTP_STATUS, TransportErrorType.UNKNOWN,
            ):
                evidence.transport_error_type = TransportErrorType.EXCHANGE_RETCODE
            return

        # Also check by binance-specific code/msg at top level (backward compat)
        if code_key == "code" and code_val and code_val != "0" and code_val != "00000":
            msg_val = str(parsed.get("msg", ""))
            evidence.exchange_code = code_val
            evidence.exchange_msg = msg_val
            evidence.extra["code"] = code_val
            evidence.extra["msg"] = msg_val
            if evidence.transport_error_type in (
                TransportErrorType.HTTP_STATUS, TransportErrorType.UNKNOWN,
            ):
                evidence.transport_error_type = TransportErrorType.EXCHANGE_RETCODE
            return

    error_val = str(parsed.get("error", ""))
    message_val = str(parsed.get("message", ""))
    if error_val and message_val:
        evidence.exchange_code = error_val
        evidence.exchange_msg = message_val
        evidence.extra["error"] = error_val
        evidence.extra["message"] = message_val
        if evidence.transport_error_type in (
            TransportErrorType.HTTP_STATUS, TransportErrorType.UNKNOWN,
        ):
            evidence.transport_error_type = TransportErrorType.EXCHANGE_RETCODE
        return

    # Gate: label/message
    label = str(parsed.get("label", ""))
    message = str(parsed.get("message", ""))
    if label or message:
        evidence.exchange_code = label
        evidence.exchange_msg = message
        evidence.extra["label"] = label
        evidence.extra["message"] = message
        return

    # Hyperliquid: status/response
    status = str(parsed.get("status", ""))
    if status and status != "ok":
        evidence.exchange_code = status
        evidence.exchange_msg = str(parsed.get("response", ""))
        evidence.extra["status"] = status


def _extract_exchange_fields_from_string(
    evidence: ExchangeErrorEvidence, err_str: str
) -> None:
    """Fallback: try to extract exchange code/msg patterns from error string."""
    if evidence.exchange_code:
        return

    import re

    lower = err_str.lower()

    # Bybit retCode pattern
    m = re.search(r"retcode[=:\s]*(-?\d+)", lower)
    if m:
        evidence.exchange_code = m.group(1)
        evidence.extra.setdefault("retCode", evidence.exchange_code)

    # Generic code pattern: code=-XXXX or sCode="XXXX"
    if not evidence.exchange_code:
        m = re.search(r"""code[=:]\s*"?(-?\d+(?:\.\d+)?)""", lower)
        if m:
            evidence.exchange_code = m.group(1)

    # retMsg from Bybit
    m = re.search(r"retmsg[=:]\s*'?\"?([^'\"]+)", lower)
    if m and not evidence.exchange_msg:
        evidence.exchange_msg = m.group(1).strip()

    # Generic msg pattern
    if not evidence.exchange_msg:
        m = re.search(r"""msg[=:]\s*"?([^"]+)"?""", lower)
        if m:
            evidence.exchange_msg = m.group(1).strip()[:200]

    if evidence.exchange_code and evidence.transport_error_type in (
        TransportErrorType.HTTP_STATUS, TransportErrorType.UNKNOWN,
    ):
        evidence.transport_error_type = TransportErrorType.EXCHANGE_RETCODE
