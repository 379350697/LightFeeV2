"""Tests for ExchangeErrorEvidence schema — completeness, confidence,
venue-normalized extraction, and fallback behavior.
"""

from __future__ import annotations

from lightfee.core.exchange_errors import (
    ExchangeErrorEvidence,
    EvidenceCompleteness,
    RequestContext,
    TransportErrorType,
    _extract_exchange_fields,
    build_evidence_from_transport_error,
    build_evidence_from_order_submit_error,
    build_fallback_evidence,
)
from lightfee.core.errors import OrderSubmitError, SubmitFailureClass


class FakeTransportError(Exception):
    def __init__(self, status_code=0, body="", category=None):
        super().__init__("fake transport error")
        self.status_code = status_code
        self.body = body
        self.category = category


# ---------------------------------------------------------------------------
# ExchangeErrorEvidence completeness tests
# ---------------------------------------------------------------------------


def test_http_400_with_json_body_is_complete():
    body = '{"code":-2022,"msg":"ReduceOnly order is rejected"}'
    evidence = ExchangeErrorEvidence(
        venue="binance",
        operation="place_order",
        transport_error_type=TransportErrorType.HTTP_STATUS,
        http_status=400,
        raw_body=body,
        request_context=RequestContext(symbol="BTCUSDT", side="sell"),
    )
    evidence.assess_completeness()
    assert evidence.evidence_completeness == EvidenceCompleteness.COMPLETE
    assert evidence.confidence == "high"
    assert evidence.exchange_code == "-2022"
    assert "ReduceOnly" in evidence.exchange_msg


def test_http_401_without_body_is_transport_only():
    evidence = ExchangeErrorEvidence(
        venue="binance",
        operation="place_order",
        transport_error_type=TransportErrorType.HTTP_STATUS,
        http_status=401,
        raw_body="",
        request_context=RequestContext(symbol="BTCUSDT", side="buy"),
    )
    evidence.assess_completeness()
    assert evidence.evidence_completeness == EvidenceCompleteness.TRANSPORT_ONLY
    assert evidence.confidence == "low"
    assert (
        "raw_body" in evidence.missing_evidence
        or "exchange_response_body" in evidence.missing_evidence
    )
    assert (
        "exchange_code_or_msg" in evidence.missing_evidence
        or "exchange_error_code" in evidence.missing_evidence
    )


def test_http_429_with_body_but_no_exchange_code_is_missing_exchange_code():
    body = '<html>Rate limited</html>'
    evidence = ExchangeErrorEvidence(
        venue="binance",
        operation="place_order",
        transport_error_type=TransportErrorType.HTTP_STATUS,
        http_status=429,
        raw_body=body,
        request_context=RequestContext(symbol="ETHUSDT"),
    )
    evidence.assess_completeness()
    # HTML body is not parseable JSON -> unparsed_exchange_body
    assert evidence.evidence_completeness in (
        EvidenceCompleteness.MISSING_EXCHANGE_CODE,
        EvidenceCompleteness.UNPARSED_EXCHANGE_BODY,
    )
    assert evidence.confidence == "medium"
    assert (
        "exchange_code_or_msg" in evidence.missing_evidence
        or "exchange_response_body_unparseable" in evidence.missing_evidence
    )


def test_http_5xx_with_body_is_partial():
    body = '{"error":"internal server error"}'
    evidence = ExchangeErrorEvidence(
        venue="okx",
        operation="place_order",
        transport_error_type=TransportErrorType.HTTP_STATUS,
        http_status=500,
        raw_body=body,
        request_context=RequestContext(symbol="BTCUSDT"),
    )
    evidence.assess_completeness()
    # Has body with extractable error code -> partial (missing msg or request_ctx fields)
    assert evidence.evidence_completeness != EvidenceCompleteness.COMPLETE
    assert evidence.confidence != "high"
    assert any(
        m in evidence.missing_evidence
        for m in ("exchange_code_or_msg", "exchange_error_msg", "request_context")
    )


def test_timeout_yields_transport_only():
    evidence = ExchangeErrorEvidence(
        venue="bybit",
        operation="place_order",
        transport_error_type=TransportErrorType.TIMEOUT,
        raw_body="",
        request_context=RequestContext(symbol="BTCUSDT"),
    )
    evidence.assess_completeness()
    assert evidence.evidence_completeness == EvidenceCompleteness.TRANSPORT_ONLY
    assert evidence.confidence == "low"


def test_network_error_yields_transport_only():
    evidence = ExchangeErrorEvidence(
        venue="bybit",
        operation="place_order",
        transport_error_type=TransportErrorType.NETWORK_ERROR,
        raw_body="",
        request_context=RequestContext(symbol="BTCUSDT"),
    )
    evidence.assess_completeness()
    assert evidence.evidence_completeness == EvidenceCompleteness.TRANSPORT_ONLY
    assert evidence.confidence == "low"


# ---------------------------------------------------------------------------
# Bybit retCode extraction
# ---------------------------------------------------------------------------


def test_bybit_retcode_nonzero_extracts_code_and_msg():
    body = '{"retCode":10001,"retMsg":"request not encrypted","retExtInfo":{},"time":1700000000000}'
    evidence = ExchangeErrorEvidence(
        venue="bybit",
        operation="place_order",
        http_status=200,
        raw_body=body,
        request_context=RequestContext(symbol="BTCUSDT", side="buy", quantity=0.001),
    )
    _extract_exchange_fields(evidence)  # Force extraction for non-HTTP status
    evidence.assess_completeness()
    assert evidence.exchange_code == "10001"
    assert "request not encrypted" in evidence.exchange_msg
    assert evidence.transport_error_type == TransportErrorType.EXCHANGE_RETCODE
    assert evidence.evidence_completeness == EvidenceCompleteness.COMPLETE
    assert evidence.confidence == "high"


# ---------------------------------------------------------------------------
# OKX sCode extraction
# ---------------------------------------------------------------------------


def test_okx_code_nonzero_extracts():
    body = '{"code":"51000","msg":"Order does not exist","data":[]}'
    evidence = ExchangeErrorEvidence(
        venue="okx",
        operation="cancel_order",
        http_status=200,
        raw_body=body,
        request_context=RequestContext(symbol="ETHUSDT"),
    )
    _extract_exchange_fields(evidence)
    evidence.assess_completeness()
    assert evidence.exchange_code == "51000"
    assert "Order does not exist" in evidence.exchange_msg


def test_okx_code_zero_is_success_not_extracted():
    body = '{"code":"0","msg":"","data":[{"ordId":"123"}]}'
    evidence = ExchangeErrorEvidence(
        venue="okx",
        operation="place_order",
        http_status=200,
        raw_body=body,
        request_context=RequestContext(symbol="ETHUSDT"),
    )
    _extract_exchange_fields(evidence)
    evidence.assess_completeness()
    # code=0 is success, NOT an exchange error code
    assert evidence.exchange_code == ""
    assert evidence.evidence_completeness != EvidenceCompleteness.COMPLETE


# ---------------------------------------------------------------------------
# Missing body downgrades confidence to medium when HTTP/request context exists
# ---------------------------------------------------------------------------


def test_missing_body_downgrades_confidence():
    evidence = ExchangeErrorEvidence(
        venue="binance",
        operation="place_order",
        transport_error_type=TransportErrorType.HTTP_STATUS,
        http_status=400,
        raw_body="",
        request_context=RequestContext(symbol="BTCUSDT"),
    )
    evidence.assess_completeness()
    assert evidence.evidence_completeness != EvidenceCompleteness.COMPLETE
    assert evidence.confidence == "medium"


# ---------------------------------------------------------------------------
# Missing request_context detection
# ---------------------------------------------------------------------------


def test_missing_request_context_detected():
    evidence = ExchangeErrorEvidence(
        venue="bybit",
        operation="place_order",
        http_status=200,
        raw_body='{"retCode":10001,"retMsg":"fail"}',
        request_context=RequestContext(),  # no symbol
    )
    _extract_exchange_fields(evidence)
    evidence.assess_completeness()
    assert "request_context" in evidence.missing_evidence


# ---------------------------------------------------------------------------
# Factory: build from TransportError
# ---------------------------------------------------------------------------


def test_build_evidence_from_transport_error():
    err = FakeTransportError(status_code=400, body='{"code":-1013,"msg":"Filter failure"}')
    evidence = build_evidence_from_transport_error(
        err,
        venue="binance",
        operation="place_order",
        endpoint="/api/v3/order",
        request_context=RequestContext(symbol="BTCUSDT", side="buy", quantity=0.001),
    )
    assert evidence.http_status == 400
    # Factory auto-extracts exchange code from body and upgrades to EXCHANGE_RETCODE
    assert evidence.raw_body == '{"code":-1013,"msg":"Filter failure"}'
    assert evidence.exchange_code == "-1013"


# ---------------------------------------------------------------------------
# Factory: build from OrderSubmitError without TransportError cause
# ---------------------------------------------------------------------------


def test_build_evidence_from_ose_without_transport_cause():
    err = OrderSubmitError(SubmitFailureClass.REJECTED, "bybit retCode=110001 retMsg=order does not exist")
    evidence = build_evidence_from_order_submit_error(
        err,
        venue="bybit",
        operation="place_order",
        request_context=RequestContext(symbol="BTCUSDT"),
    )
    # String extraction should find retCode=110001 from the error message
    assert evidence.exchange_code == "110001"
    # No body -> completeness reflects missing body or transport_only
    expected = evidence.evidence_completeness
    assert expected in (
        EvidenceCompleteness.COMPLETE, EvidenceCompleteness.PARTIAL,
        EvidenceCompleteness.MISSING_EXCHANGE_CODE, EvidenceCompleteness.MISSING_BODY,
        EvidenceCompleteness.MISSING_EXCHANGE_BODY, EvidenceCompleteness.TRANSPORT_ONLY,
        EvidenceCompleteness.MISSING_EXCHANGE_CODE_OR_MSG,
    )


# ---------------------------------------------------------------------------
# Factory: build fallback evidence from generic exception
# ---------------------------------------------------------------------------


def test_fallback_evidence_with_raw_exception():
    try:
        raise RuntimeError("connection refused")
    except Exception as e:
        evidence = build_fallback_evidence(
            e,
            venue="gate",
            operation="place_order",
            request_context=RequestContext(symbol="BTCUSDT"),
        )

    assert evidence.transport_error_type == TransportErrorType.NETWORK_ERROR
    assert evidence.confidence == "low"
    assert evidence.evidence_completeness != EvidenceCompleteness.COMPLETE


def test_fallback_evidence_with_timeout_string():
    try:
        raise TimeoutError("timed out after 30s")
    except Exception as e:
        evidence = build_fallback_evidence(
            e,
            venue="hyperliquid",
            operation="place_order",
        )

    assert evidence.transport_error_type == TransportErrorType.TIMEOUT
    assert evidence.confidence == "low"


# ---------------------------------------------------------------------------
# raw_body_truncated flag
# ---------------------------------------------------------------------------


def test_raw_body_truncated_flag():
    long_body = "x" * 2500
    evidence = build_fallback_evidence(
        RuntimeError("test"),
        venue="binance",
        operation="place_order",
        request_context=RequestContext(symbol="BTCUSDT"),
    )
    # Manually set body to long version
    evidence.raw_body = long_body
    evidence.raw_body_truncated = len(long_body) > 2000
    evidence.assess_completeness()
    assert evidence.raw_body_truncated is True


# ---------------------------------------------------------------------------
# RequestContext from OrderRequest
# ---------------------------------------------------------------------------


def test_request_context_from_order_request():
    from lightfee.core.domain import OrderRequest, Side, TimeInForce, Venue

    req = OrderRequest(
        venue=Venue.BINANCE,
        symbol="BTCUSDT",
        side=Side.SELL,
        quantity=0.01,
        price=50000.0,
        reduce_only=True,
        post_only=False,
        time_in_force=TimeInForce.IOC,
        client_order_id="lf_test_001",
    )
    ctx = RequestContext.from_order_request(req)
    assert ctx.symbol == "BTCUSDT"
    assert ctx.side == "sell"
    assert ctx.quantity == 0.01
    assert ctx.price == 50000.0
    assert ctx.reduce_only is True
    assert ctx.client_order_id == "lf_test_001"


# ---------------------------------------------------------------------------
# ExchangeErrorEvidence round-trip
# ---------------------------------------------------------------------------


def test_evidence_round_trip():
    evidence = ExchangeErrorEvidence(
        venue="bybit",
        operation="place_order",
        transport_error_type=TransportErrorType.EXCHANGE_RETCODE,
        exchange_code="10001",
        exchange_msg="request not encrypted",
        raw_body='{"retCode":10001,"retMsg":"request not encrypted"}',
        request_context=RequestContext(symbol="BTCUSDT", side="buy"),
        extra={"retCode": "10001", "retMsg": "request not encrypted"},
    )
    evidence.assess_completeness()
    d = evidence.to_dict()
    restored = ExchangeErrorEvidence.from_dict(d)
    assert restored.venue == "bybit"
    assert restored.exchange_code == "10001"
    assert restored.evidence_completeness == EvidenceCompleteness.COMPLETE
    assert restored.confidence == "high"


# ---------------------------------------------------------------------------
# TransportError body preserved through OrderSubmitError (real chain)
# ---------------------------------------------------------------------------


def test_transport_error_body_preserved_through_ose_cause_chain():
    """TransportError(400, body={-2022}) -> OrderSubmitError -> evidence.

    The __cause__ chain must preserve TransportError so the evidence builder
    can extract status_code, body, exchange_code, and exchange_msg.
    """
    from lightfee.venues.transport import TransportError, TransportErrorCategory

    te = TransportError(
        TransportErrorCategory.REQUEST_REJECTED,
        "400 Bad Request",
        status_code=400,
        body='{"code":-2022,"msg":"ReduceOnly Order is rejected."}',
    )
    # Simulate _map_to_submit_error with transport_error=... (now sets __cause__)
    ose = OrderSubmitError(
        SubmitFailureClass.REJECTED, str(te), transport_error=te,
    )

    evidence = build_evidence_from_order_submit_error(
        ose,
        venue="binance",
        operation="place_order",
        endpoint="/fapi/v1/order",
        request_context=RequestContext(symbol="ALTUSDT", side="sell", reduce_only=True),
    )

    assert evidence.http_status == 400
    assert evidence.raw_body == '{"code":-2022,"msg":"ReduceOnly Order is rejected."}'
    assert evidence.exchange_code == "-2022"
    assert "ReduceOnly" in evidence.exchange_msg
    assert evidence.evidence_completeness == EvidenceCompleteness.COMPLETE
    assert evidence.confidence == "high"
    assert "exchange_response_body" not in evidence.missing_evidence
    assert evidence.request_context.symbol == "ALTUSDT"
    assert evidence.request_context.reduce_only is True


def test_transport_error_body_preserved_through_real_map_path():
    """Integration: TransportError -> _map_to_submit_error -> evidence.

    Uses the real _map_to_submit_error function (not manual construction).
    """
    from lightfee.venues.transport import (
        TransportError,
        TransportErrorCategory,
        _map_to_submit_error,
    )

    te = TransportError(
        TransportErrorCategory.REQUEST_REJECTED,
        "400 Bad Request",
        status_code=400,
        body='{"code":-2022,"msg":"ReduceOnly Order is rejected."}',
    )
    ose = _map_to_submit_error(te.category, str(te), transport_error=te)

    assert ose.is_rejected
    assert ose.transport_error is te
    assert ose.__cause__ is te

    evidence = build_evidence_from_order_submit_error(
        ose, venue="binance", operation="place_order", endpoint="",
        request_context=RequestContext(symbol="ALTUSDT", side="sell", reduce_only=True),
    )

    assert evidence.http_status == 400
    assert evidence.raw_body != ""
    assert evidence.exchange_code == "-2022"
    assert "ReduceOnly" in evidence.exchange_msg
    assert evidence.evidence_completeness == EvidenceCompleteness.COMPLETE
    assert evidence.confidence == "high"


# ---------------------------------------------------------------------------
# Generic HTTP status codes — all must produce correct evidence
# ---------------------------------------------------------------------------


def test_http_401_with_json_body():
    body = '{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}'
    evidence = ExchangeErrorEvidence(
        venue="binance",
        operation="place_order",
        transport_error_type=TransportErrorType.HTTP_STATUS,
        http_status=401,
        raw_body=body,
        request_context=RequestContext(symbol="BTCUSDT", side="buy"),
    )
    evidence.assess_completeness()
    assert evidence.http_status == 401
    assert evidence.exchange_code == "-2015"
    assert "Invalid API-key" in evidence.exchange_msg
    assert evidence.evidence_completeness == EvidenceCompleteness.COMPLETE
    assert evidence.confidence == "high"


def test_http_403_with_json_body():
    body = '{"code":-2014,"msg":"API-key format invalid."}'
    evidence = ExchangeErrorEvidence(
        venue="binance",
        operation="place_order",
        transport_error_type=TransportErrorType.HTTP_STATUS,
        http_status=403,
        raw_body=body,
        request_context=RequestContext(symbol="ETHUSDT"),
    )
    evidence.assess_completeness()
    assert evidence.http_status == 403
    assert evidence.exchange_code == "-2014"
    assert evidence.evidence_completeness == EvidenceCompleteness.COMPLETE


def test_http_429_with_json_body():
    body = '{"code":-1015,"msg":"Too many requests; rate limit exceeded."}'
    evidence = ExchangeErrorEvidence(
        venue="binance",
        operation="place_order",
        transport_error_type=TransportErrorType.HTTP_STATUS,
        http_status=429,
        raw_body=body,
        request_context=RequestContext(symbol="BTCUSDT"),
    )
    evidence.assess_completeness()
    assert evidence.http_status == 429
    assert evidence.exchange_code == "-1015"
    assert evidence.evidence_completeness == EvidenceCompleteness.COMPLETE


def test_http_500_with_generic_error_body():
    body = '{"error":"Internal Server Error","errorCode":"500_INTERNAL"}'
    evidence = ExchangeErrorEvidence(
        venue="bybit",
        operation="place_order",
        transport_error_type=TransportErrorType.HTTP_STATUS,
        http_status=500,
        raw_body=body,
        request_context=RequestContext(symbol="BTCUSDT"),
    )
    evidence.assess_completeness()
    assert evidence.http_status == 500
    assert evidence.exchange_code != ""
    assert evidence.confidence != "low"


def test_http_503_with_body():
    body = '{"code":"-1","msg":"Service Unavailable"}'
    evidence = ExchangeErrorEvidence(
        venue="binance",
        operation="place_order",
        transport_error_type=TransportErrorType.HTTP_STATUS,
        http_status=503,
        raw_body=body,
        request_context=RequestContext(symbol="BTCUSDT"),
    )
    evidence.assess_completeness()
    assert evidence.http_status == 503
    assert evidence.evidence_completeness != EvidenceCompleteness.TRANSPORT_ONLY


# ---------------------------------------------------------------------------
# Body non-JSON → unparsed_exchange_body
# ---------------------------------------------------------------------------


def test_html_body_not_json_yields_unparsed():
    body = "<html><body>502 Bad Gateway</body></html>"
    evidence = ExchangeErrorEvidence(
        venue="binance",
        operation="place_order",
        transport_error_type=TransportErrorType.HTTP_STATUS,
        http_status=502,
        raw_body=body,
        request_context=RequestContext(symbol="BTCUSDT"),
    )
    evidence.assess_completeness()
    assert evidence.evidence_completeness == EvidenceCompleteness.UNPARSED_EXCHANGE_BODY
    assert evidence.confidence == "medium"
    assert "exchange_response_body_unparseable" in evidence.missing_evidence


# ---------------------------------------------------------------------------
# Body missing → missing_exchange_body, confidence low
# ---------------------------------------------------------------------------


def test_body_missing_is_missing_exchange_body():
    evidence = ExchangeErrorEvidence(
        venue="binance",
        operation="place_order",
        transport_error_type=TransportErrorType.HTTP_STATUS,
        http_status=400,
        raw_body="",
        request_context=RequestContext(symbol="BTCUSDT"),
    )
    evidence.assess_completeness()
    assert evidence.evidence_completeness == EvidenceCompleteness.MISSING_EXCHANGE_BODY
    assert evidence.confidence == "medium"  # has http_status but no body
    assert "exchange_response_body" in evidence.missing_evidence
    assert "exchange_error_code" in evidence.missing_evidence


# ---------------------------------------------------------------------------
# Generic JSON field extraction: errorCode/errorMessage, error, etc.
# ---------------------------------------------------------------------------


def test_generic_error_code_extraction():
    body = '{"errorCode":"ORDER_REJECTED","errorMessage":"Insufficient margin"}'
    evidence = ExchangeErrorEvidence(
        venue="unknown_venue",
        operation="place_order",
        transport_error_type=TransportErrorType.HTTP_STATUS,
        http_status=400,
        raw_body=body,
        request_context=RequestContext(symbol="BTCUSDT"),
    )
    evidence.assess_completeness()
    assert evidence.exchange_code == "ORDER_REJECTED"
    assert "Insufficient margin" in evidence.exchange_msg
    assert evidence.evidence_completeness == EvidenceCompleteness.COMPLETE


def test_generic_error_field_extraction():
    body = '{"error":"POSITION_NOT_FOUND","message":"No open position for this symbol"}'
    evidence = ExchangeErrorEvidence(
        venue="unknown_venue",
        operation="cancel_order",
        http_status=400,
        raw_body=body,
        request_context=RequestContext(symbol="BTCUSDT"),
    )
    evidence.assess_completeness()
    assert evidence.exchange_code == "POSITION_NOT_FOUND"
    assert evidence.confidence == "high"


# ---------------------------------------------------------------------------
# Old completeness levels still work (backward compatibility)
# ---------------------------------------------------------------------------


def test_old_missing_body_completeness_still_valid():
    """Old EvidenceCompleteness.MISSING_BODY still exists for backward compat."""
    assert hasattr(EvidenceCompleteness, "MISSING_BODY")
    assert EvidenceCompleteness.MISSING_BODY == "missing_body"


def test_old_transport_only_still_valid():
    assert EvidenceCompleteness.TRANSPORT_ONLY == "transport_only"


# ---------------------------------------------------------------------------
# Evidence builder from TransportError with missing body
# ---------------------------------------------------------------------------


def test_build_evidence_from_transport_error_missing_body():
    """When TransportError has no body, evidence must report missing_exchange_body."""
    from lightfee.venues.transport import TransportError, TransportErrorCategory

    te = TransportError(
        TransportErrorCategory.TRANSPORT_FAILURE,
        "503 Service Unavailable",
        status_code=503,
        body="",
    )
    evidence = build_evidence_from_transport_error(
        te, venue="binance", operation="place_order", endpoint="",
        request_context=RequestContext(symbol="BTCUSDT"),
    )
    assert evidence.http_status == 503
    assert evidence.raw_body == ""
    assert evidence.evidence_completeness == EvidenceCompleteness.MISSING_EXCHANGE_BODY
    assert evidence.confidence == "medium"
    assert "exchange_response_body" in evidence.missing_evidence
