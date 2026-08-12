"""Strict, shared parsing helpers for slow account fee snapshots."""

from __future__ import annotations

import math
from typing import Any


def fee_rate_bps(value: Any, field: str) -> float:
    """Convert an exchange decimal fee rate to basis points.

    Negative maker rates are valid rebates and must be preserved.
    """
    try:
        rate = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"missing or invalid {field}") from exc
    if not math.isfinite(rate):
        raise ValueError(f"non-finite {field}")
    return rate * 10_000.0


def fee_rate_from_mapping(row: dict[str, Any], field: str, *keys: str) -> float:
    """Read the first available exchange field and convert its decimal rate."""
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return fee_rate_bps(row[key], field)
    raise ValueError(f"missing {field}")


def first_mapping(value: Any, field: str) -> dict[str, Any]:
    """Return a response object or first response row, rejecting malformed data."""
    if isinstance(value, dict):
        return value
    if isinstance(value, list) and value and isinstance(value[0], dict):
        return value[0]
    raise ValueError(f"missing {field}")
