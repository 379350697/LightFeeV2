"""Venue private capability admission semantics.

Private venue failures are not global trading stops. They isolate the affected
venue for new-risk admission while leaving reduce-only cleanup to the recovery
executor, which can then report a critical blocker if cleanup is also denied.
"""

from __future__ import annotations

from lightfee.core.domain import Venue

HEALTHY = "healthy"
AUTH_INVALID = "auth_invalid"
PERMISSION_DENIED = "permission_denied"
TRUTH_UNAVAILABLE = "truth_unavailable"
ORDER_PERMISSION_UNAVAILABLE = "order_permission_unavailable"

VENUE_AUTH_INVALID_REASON = "venue_auth_invalid"
VENUE_PERMISSION_DENIED_REASON = "venue_permission_denied"
VENUE_TRUTH_UNAVAILABLE_REASON = "venue_private_truth_unavailable"
VENUE_ORDER_PERMISSION_UNAVAILABLE_REASON = "venue_order_permission_unavailable"

PRIVATE_HEALTH_ADMISSION_REASONS = {
    VENUE_AUTH_INVALID_REASON,
    VENUE_PERMISSION_DENIED_REASON,
    VENUE_TRUTH_UNAVAILABLE_REASON,
    VENUE_ORDER_PERMISSION_UNAVAILABLE_REASON,
}

_REASON_TO_HEALTH_STATUS = {
    VENUE_AUTH_INVALID_REASON: AUTH_INVALID,
    VENUE_PERMISSION_DENIED_REASON: PERMISSION_DENIED,
    VENUE_TRUTH_UNAVAILABLE_REASON: TRUTH_UNAVAILABLE,
    VENUE_ORDER_PERMISSION_UNAVAILABLE_REASON: ORDER_PERMISSION_UNAVAILABLE,
}


def is_private_health_admission_reason(reason: str) -> bool:
    return str(reason or "") in PRIVATE_HEALTH_ADMISSION_REASONS


def private_health_status_for_admission_reason(reason: str) -> str:
    return _REASON_TO_HEALTH_STATUS.get(str(reason or ""), "")


def classify_private_health_error(
    venue: Venue,
    reason: str,
) -> tuple[str, str] | None:
    """Return ``(admission_reason, health_status)`` for private capability errors."""

    text = str(reason or "").lower()
    if not text:
        return None

    if venue == Venue.BYBIT:
        if (
            "33004" in text
            or "api key has expired" in text
            or "apikey has expired" in text
        ):
            return VENUE_AUTH_INVALID_REASON, AUTH_INVALID
        if (
            "invalid api key" in text
            or "invalid api-key" in text
            or "api key is invalid" in text
            or "apikey is invalid" in text
        ):
            return VENUE_AUTH_INVALID_REASON, AUTH_INVALID
        if (
            "permission" in text
            and ("denied" in text or "not permitted" in text or "api key" in text)
        ):
            return VENUE_PERMISSION_DENIED_REASON, PERMISSION_DENIED
        if "ip" in text and "not match" in text and "api" in text:
            return VENUE_PERMISSION_DENIED_REASON, PERMISSION_DENIED

    return None
