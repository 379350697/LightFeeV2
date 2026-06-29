"""Order identity normalization shared across truth-probe paths."""

from __future__ import annotations

from typing import Any


_ABSENT_ORDER_IDENTITY_VALUES = {"", "none", "null"}


def normalize_order_identity(value: Any) -> str:
    """Return a usable order/client identity, or empty when exchange omitted it."""
    text = str(value or "").strip()
    if text.lower() in _ABSENT_ORDER_IDENTITY_VALUES:
        return ""
    return text
