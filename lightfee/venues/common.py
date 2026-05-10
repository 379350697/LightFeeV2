"""Venue common utilities: quantity normalization, sizing, reduce-only exemptions."""

from __future__ import annotations

import math

from lightfee.core.domain import Venue
from lightfee.core.money import floor_to_step, normalize_order_quantity


def venue_reduce_only_close_exempts_min_notional(venue: Venue) -> bool:
    """Aster and Binance reduce-only closes are exempt from min notional checks."""
    return venue in (Venue.ASTER, Venue.BINANCE)


def floor_quantity_to_step(quantity: float, step_size: float) -> float:
    """Floor quantity to the venue step size, respecting min quantity."""
    return normalize_order_quantity(quantity, step_size)


def apply_contract_size(quantity: float, contract_size: float) -> float:
    """Convert quantity by contract size multiplier. Returns quantity * contract_size."""
    if not math.isfinite(quantity) or not math.isfinite(contract_size):
        return 0.0
    return quantity * contract_size


def check_min_notional(
    quantity: float,
    price: float,
    min_notional: float,
    venue: Venue,
    reduce_only: bool = False,
) -> bool:
    """Check whether quantity * price meets min notional, respecting exemptions."""
    if venue_reduce_only_close_exempts_min_notional(venue) and reduce_only:
        return True
    notional = quantity * price
    return notional >= min_notional


def normalize_venue_quantity(
    quantity: float,
    step_size: float,
    contract_size: float = 1.0,
    min_quantity: float = 0.0,
) -> float:
    """Full quantity normalization: floor to step, apply contract, enforce min."""
    if quantity <= 0.0 or not math.isfinite(quantity):
        return 0.0
    floored = floor_quantity_to_step(quantity, step_size)
    if floored <= 0.0:
        return 0.0
    result = apply_contract_size(floored, contract_size)
    if result < min_quantity:
        return 0.0
    return result


__all__ = [
    "floor_to_step",
    "normalize_order_quantity",
    "venue_reduce_only_close_exempts_min_notional",
    "floor_quantity_to_step",
    "apply_contract_size",
    "check_min_notional",
    "normalize_venue_quantity",
]
