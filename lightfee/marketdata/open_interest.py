"""Typed open-interest evidence shared by sidecar and live entry gates.

Unknown or untrusted OI is represented by ``None`` plus a reasoned status.  A
numeric zero is reserved for an exchange-observed zero and must never be used
as a transport/parser fallback.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
from math import isfinite


class OpenInterestEvidenceStatus(str, Enum):
    OBSERVED = "observed"
    UNSUPPORTED = "unsupported"
    SYMBOL_NOT_LISTED = "symbol_not_listed"
    SYMBOL_MISMATCH = "symbol_mismatch"
    AMBIGUOUS_MAPPING = "ambiguous_mapping"
    DEFERRED = "deferred"
    TIMEOUT = "timeout"
    RATE_LIMITED = "rate_limited"
    HTTP_ERROR = "http_error"
    PARSE_ERROR = "parse_error"
    STALE = "stale"
    UNAVAILABLE = "unavailable"


_LEGACY_STATUS_ALIASES = {
    "available": OpenInterestEvidenceStatus.OBSERVED.value,
    "deferred_by_cap": OpenInterestEvidenceStatus.DEFERRED.value,
    "refresh_inflight": OpenInterestEvidenceStatus.DEFERRED.value,
    "symbol_not_listed_before_http": OpenInterestEvidenceStatus.SYMBOL_NOT_LISTED.value,
    "missing_mark_price": OpenInterestEvidenceStatus.PARSE_ERROR.value,
}


def normalize_open_interest_status(value: object) -> str:
    raw = str(value or "").strip().lower()
    raw = _LEGACY_STATUS_ALIASES.get(raw, raw)
    valid = {status.value for status in OpenInterestEvidenceStatus}
    return raw if raw in valid else OpenInterestEvidenceStatus.UNAVAILABLE.value


def open_interest_sample_id(
    *,
    venue: str,
    canonical_symbol: str,
    venue_symbol: str,
    observed_at_ms: int,
    source: str,
    raw_value: float | None,
    value_quote: float | None,
) -> str:
    material = "|".join(
        (
            str(venue or "").lower(),
            str(canonical_symbol or "").upper(),
            str(venue_symbol or "").upper(),
            str(max(int(observed_at_ms or 0), 0)),
            str(source or ""),
            "" if raw_value is None else format(float(raw_value), ".17g"),
            "" if value_quote is None else format(float(value_quote), ".17g"),
        )
    )
    return sha256(material.encode("utf-8")).hexdigest()[:24]


@dataclass(frozen=True)
class OpenInterestEvidence:
    canonical_symbol: str
    venue_symbol: str
    value_quote: float | None
    raw_value: float | None
    raw_unit: str
    contract_multiplier: float | None
    conversion_mark_price: float | None
    observed_at_ms: int | None
    received_at_ms: int
    source: str
    status: str
    reason: str
    sample_id: str

    @property
    def is_observed(self) -> bool:
        return (
            normalize_open_interest_status(self.status)
            == OpenInterestEvidenceStatus.OBSERVED.value
            and self.value_quote is not None
            and isfinite(float(self.value_quote))
            and float(self.value_quote) >= 0.0
            and bool(self.canonical_symbol)
            and bool(self.venue_symbol)
            and int(self.observed_at_ms or 0) > 0
            and int(self.received_at_ms or 0) >= int(self.observed_at_ms or 0)
            and bool(self.source)
            and bool(self.sample_id)
        )


def validated_open_interest_evidence(
    evidence: OpenInterestEvidence,
) -> OpenInterestEvidence:
    """Return a fail-closed evidence value with normalized status semantics."""
    status = normalize_open_interest_status(evidence.status)
    value = evidence.value_quote
    raw_value = evidence.raw_value
    invalid_value = (
        value is None
        or not isfinite(float(value))
        or float(value) < 0.0
        or (raw_value is not None and not isfinite(float(raw_value)))
    )
    identity_invalid = not evidence.canonical_symbol or not evidence.venue_symbol
    timestamp_invalid = (
        int(evidence.observed_at_ms or 0) <= 0
        or int(evidence.received_at_ms or 0) < int(evidence.observed_at_ms or 0)
    )
    if status == OpenInterestEvidenceStatus.OBSERVED.value:
        if identity_invalid:
            status = OpenInterestEvidenceStatus.SYMBOL_MISMATCH.value
        elif invalid_value:
            status = OpenInterestEvidenceStatus.PARSE_ERROR.value
        elif timestamp_invalid:
            status = OpenInterestEvidenceStatus.STALE.value
        elif not evidence.source:
            status = OpenInterestEvidenceStatus.PARSE_ERROR.value

    sample_id = evidence.sample_id
    if status == OpenInterestEvidenceStatus.OBSERVED.value and not sample_id:
        sample_id = open_interest_sample_id(
            venue="",
            canonical_symbol=evidence.canonical_symbol,
            venue_symbol=evidence.venue_symbol,
            observed_at_ms=int(evidence.observed_at_ms or 0),
            source=evidence.source,
            raw_value=raw_value,
            value_quote=value,
        )
    if status != OpenInterestEvidenceStatus.OBSERVED.value:
        value = None
        sample_id = ""

    return OpenInterestEvidence(
        canonical_symbol=str(evidence.canonical_symbol or "").upper(),
        venue_symbol=str(evidence.venue_symbol or "").upper(),
        value_quote=None if value is None else float(value),
        raw_value=None if raw_value is None else float(raw_value),
        raw_unit=str(evidence.raw_unit or ""),
        contract_multiplier=evidence.contract_multiplier,
        conversion_mark_price=evidence.conversion_mark_price,
        observed_at_ms=(
            int(evidence.observed_at_ms or 0)
            if int(evidence.observed_at_ms or 0) > 0
            else None
        ),
        received_at_ms=max(int(evidence.received_at_ms or 0), 0),
        source=str(evidence.source or ""),
        status=status,
        reason=str(evidence.reason or status),
        sample_id=sample_id,
    )
