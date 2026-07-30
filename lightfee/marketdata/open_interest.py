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
from typing import Any


OPEN_INTEREST_EVENT_FUTURE_SKEW_MS = 5_000
ENTRY_OPEN_INTEREST_CACHE_FALLBACK_MAX_AGE_MS = 10 * 60 * 1_000


def bounded_open_interest_cache_fallback_max_age_ms(value: object = None) -> int:
    """Return the configured slow-evidence age bounded by the hard 10m cap."""
    try:
        parsed = int(value) if value is not None else 0
    except (TypeError, ValueError, OverflowError):
        parsed = 0
    if parsed <= 0:
        return ENTRY_OPEN_INTEREST_CACHE_FALLBACK_MAX_AGE_MS
    return min(parsed, ENTRY_OPEN_INTEREST_CACHE_FALLBACK_MAX_AGE_MS)


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


def _open_interest_evidence_field(source: Any, name: str, default: Any = None) -> Any:
    if isinstance(source, dict):
        return source.get(name, default)
    return getattr(source, name, default)


def open_interest_uses_cache_fallback(source: Any) -> bool:
    reason = str(
        _open_interest_evidence_field(
            source,
            "open_interest_evidence_reason",
            _open_interest_evidence_field(source, "reason", ""),
        )
        or ""
    ).lower()
    return bool(
        _open_interest_evidence_field(source, "open_interest_cache_fallback", False)
    ) or "cache_fallback" in reason


def open_interest_max_age_ms_for_evidence(
    source: Any,
    *,
    default_max_age_ms: int,
) -> int:
    try:
        default_age = max(int(default_max_age_ms), 1)
    except (TypeError, ValueError, OverflowError):
        default_age = 1
    if not open_interest_uses_cache_fallback(source):
        return default_age
    # Cached fallback evidence is a bounded exception to the normal evidence
    # budget.  A producer-provided marker or a wider caller budget must not
    # extend its admissible freshness window.
    return min(
        default_age,
        bounded_open_interest_cache_fallback_max_age_ms(
            _open_interest_evidence_field(
                source,
                "open_interest_cache_fallback_max_age_ms",
                ENTRY_OPEN_INTEREST_CACHE_FALLBACK_MAX_AGE_MS,
            )
        ),
    )


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


def observed_open_interest_proof_reason(
    *,
    venue: str,
    canonical_symbol: str,
    venue_symbol: str,
    value_quote: object,
    raw_value: object,
    raw_unit: str,
    contract_multiplier: object,
    conversion_mark_price: object,
    observed_at_ms: object,
    event_at_ms: object,
    received_at_ms: object,
    source: str,
    sample_id: str,
) -> str:
    """Validate the common economic, time and identity OI proof contract."""
    numeric_inputs = (value_quote, raw_value)
    if any(isinstance(value, bool) for value in numeric_inputs):
        return "invalid_numeric_value"
    try:
        value = float(value_quote)
        raw = float(raw_value)
    except (TypeError, ValueError, OverflowError):
        return "invalid_numeric_value"
    if not isfinite(value) or value < 0.0 or not isfinite(raw) or raw < 0.0:
        return "invalid_numeric_value"

    unit = str(raw_unit or "").strip().lower()

    def positive(value: object) -> float | None:
        if isinstance(value, bool):
            return None
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return parsed if isfinite(parsed) and parsed > 0.0 else None

    mark = positive(conversion_mark_price)
    multiplier = positive(contract_multiplier)
    if unit == "quote":
        expected_quote = raw
    elif unit == "base" and mark is not None:
        expected_quote = raw * mark
    elif unit == "contracts" and mark is not None and multiplier is not None:
        expected_quote = raw * multiplier * mark
    else:
        return "invalid_unit_conversion"
    if abs(value - expected_quote) > max(1e-6, abs(expected_quote) * 1e-9):
        return "economic_identity_mismatch"

    if any(
        isinstance(value, bool)
        for value in (observed_at_ms, event_at_ms, received_at_ms)
    ):
        return "invalid_timestamp"
    try:
        observed = int(observed_at_ms or 0)
        event = int(event_at_ms or 0)
        received = int(received_at_ms or 0)
    except (TypeError, ValueError, OverflowError):
        return "invalid_timestamp"
    if (
        observed <= 0
        or received < observed
        or event < 0
        or event > received + OPEN_INTEREST_EVENT_FUTURE_SKEW_MS
    ):
        return "invalid_timestamp"
    if not str(venue_symbol or "").strip() or not str(source or "").strip():
        return "identity_unavailable"
    expected_sample_id = open_interest_sample_id(
        venue=venue,
        canonical_symbol=canonical_symbol,
        venue_symbol=venue_symbol,
        observed_at_ms=event or observed,
        source=source,
        raw_value=raw,
        value_quote=value,
    )
    if not str(sample_id or "").strip() or sample_id != expected_sample_id:
        return "sample_id_mismatch"
    return ""


@dataclass(frozen=True)
class OpenInterestEvidence:
    canonical_symbol: str
    venue_symbol: str
    value_quote: float | None
    raw_value: float | None
    raw_unit: str
    contract_multiplier: float | None
    conversion_mark_price: float | None
    # Local observation/receipt clock.  Exchange timestamps live in
    # ``event_at_ms`` because vendor clocks are not ordered against this one.
    observed_at_ms: int | None
    event_at_ms: int | None
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
            and (
                int(self.event_at_ms or 0) <= 0
                or int(self.event_at_ms or 0)
                <= int(self.received_at_ms or 0)
                + OPEN_INTEREST_EVENT_FUTURE_SKEW_MS
            )
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
        or (
            int(evidence.event_at_ms or 0) > 0
            and int(evidence.event_at_ms or 0)
            > int(evidence.received_at_ms or 0)
            + OPEN_INTEREST_EVENT_FUTURE_SKEW_MS
        )
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
        event_at_ms=(
            int(evidence.event_at_ms or 0)
            if int(evidence.event_at_ms or 0) > 0
            else None
        ),
        received_at_ms=max(int(evidence.received_at_ms or 0), 0),
        source=str(evidence.source or ""),
        status=status,
        reason=str(evidence.reason or status),
        sample_id=sample_id,
    )


def open_interest_timestamps_are_fresh(
    *,
    observed_at_ms: int,
    received_at_ms: int,
    event_at_ms: int = 0,
    now_ms: int,
    max_age_ms: int,
    future_skew_ms: int = OPEN_INTEREST_EVENT_FUTURE_SKEW_MS,
) -> bool:
    """Validate local receipt freshness and the independent exchange clock."""
    try:
        observed = int(observed_at_ms)
        received = int(received_at_ms)
        event = int(event_at_ms or 0)
        now = int(now_ms)
        max_age = max(int(max_age_ms), 1)
        skew = max(int(future_skew_ms), 0)
    except (TypeError, ValueError, OverflowError):
        return False
    if observed <= 0 or received < observed:
        return False
    if observed > now or received > now or now - observed > max_age:
        return False
    if event > 0 and (event > now + skew or now - event > max_age):
        return False
    return True
