"""Shared diagnostic side-label semantics.

The trading adapters keep their own exchange-native side fields. This module is
only for diagnostics that compare business legs against exchange truth labels.
"""

from __future__ import annotations

from typing import Any


def normalize_position_side_label(value: Any) -> str:
    """Normalize common exchange/domain side labels for diagnostic comparison."""
    raw = str(value or "").strip().lower()
    if not raw:
        return "unknown"
    if "." in raw:
        raw = raw.rsplit(".", 1)[-1]
    raw = raw.replace("-", "_")
    if raw in {"buy", "sell", "long", "short"}:
        return raw
    return "unknown"


def side_matches_business_leg(actual: Any, expected_leg: Any) -> bool:
    actual_label = normalize_position_side_label(actual)
    expected = normalize_position_side_label(expected_leg)
    if expected == "long":
        return actual_label in {"buy", "long"}
    if expected == "short":
        return actual_label in {"sell", "short"}
    if expected == "buy":
        return actual_label in {"buy", "long"}
    if expected == "sell":
        return actual_label in {"sell", "short"}
    return False
