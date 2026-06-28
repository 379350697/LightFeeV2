"""Canonical symbol identity helpers for recovery ownership.

Recovery compares local strategy symbols with exchange-truth artifacts.  Some
venues report wire symbols (for example OKX ``ACT-USDT-SWAP``) while local
state stores canonical strategy symbols (``ACTUSDT``).  These helpers normalize
only the recovery comparison key and leave raw venue symbols intact in evidence.
"""

from __future__ import annotations

from typing import Any

from lightfee.core.domain import Venue
from lightfee.venues.specs import get_spec


def canonical_recovery_symbol(symbol: Any, venue: Any = "") -> str:
    text = _text(symbol).upper()
    if not text:
        return ""
    venue_obj = _venue_from_any(venue)
    if venue_obj is not None:
        try:
            spec = get_spec(venue_obj)
            if spec.symbol_from_venue is not None:
                converted = _text(spec.symbol_from_venue(text)).upper()
                if converted:
                    return converted
        except Exception:
            pass
    return text


def _venue_from_any(value: Any) -> Venue | None:
    if isinstance(value, Venue):
        return value
    if hasattr(value, "value"):
        value = value.value
    text = _text(value).lower()
    if not text:
        return None
    try:
        return Venue.from_str(text)
    except ValueError:
        return None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value or "").strip()
