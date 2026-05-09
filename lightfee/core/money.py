"""Money and quantity normalization primitives matching Rust reference behavior."""

from __future__ import annotations

import math


def normalize_order_quantity(quantity: float, step: float) -> float:
    """Floor quantity to nearest step, matching Rust normalize_order_quantity."""
    if not math.isfinite(quantity) or not math.isfinite(step) or quantity <= 0.0 or step <= 0.0:
        return 0.0
    units = math.floor(quantity / step + 1e-9)
    if units <= 0.0:
        return 0.0
    return units * step


def floor_to_step(value: float, step: float) -> float:
    """Alias for normalize_order_quantity used in planners."""
    return normalize_order_quantity(value, step)


def compute_notional_drift_pct(requested_notional: float, executable_notional: float) -> float:
    """Compute absolute percentage drift between requested and executable notional."""
    if requested_notional <= 0.0:
        return 0.0
    return (abs(executable_notional - requested_notional) / requested_notional) * 100.0
