"""Small common contract for execution-time entry tradability checks.

The selection catalog is intentionally allowed to be cached.  This module is
only for the last, non-mutating check immediately before an opening order.
It keeps two facts distinct:

* an exchange explicitly says the instrument cannot open a new position;
* the current instrument state cannot be established.

The dispatch runtime treats the latter as fail-closed evidence uncertainty,
not as a confirmed exchange rejection.
"""

from __future__ import annotations

from typing import Any

from lightfee.core.errors import OrderSubmitError, SubmitFailureClass
from lightfee.venues.transport import TransportError, TransportErrorCategory


ENTRY_TRADABILITY_BLOCKED_MARKER = "entry-tradability-blocked"
ENTRY_TRADABILITY_UNAVAILABLE_MARKER = "entry-tradability-unavailable"


def entry_tradability_blocked(
    venue: str,
    symbol: str,
    **facts: Any,
) -> OrderSubmitError:
    """Return a confirmed, non-retryable opening-admission rejection."""
    detail = " ".join(
        f"{key}={value}" for key, value in facts.items() if value not in (None, "")
    )
    suffix = f" {detail}" if detail else ""
    return OrderSubmitError(
        SubmitFailureClass.REJECTED,
        f"{venue} {ENTRY_TRADABILITY_BLOCKED_MARKER}: symbol={symbol}{suffix}",
    )


def entry_tradability_unavailable(
    venue: str,
    symbol: str,
    reason: str,
) -> TransportError:
    """Return an evidence-gap error for malformed or unusable public data."""
    return TransportError(
        TransportErrorCategory.REQUEST_REJECTED,
        f"{venue} {ENTRY_TRADABILITY_UNAVAILABLE_MARKER}: "
        f"symbol={symbol} reason={reason}",
    )
