"""Venue common utilities: quantity normalization, sizing, reduce-only exemptions."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Optional

from lightfee.core.domain import Venue
from lightfee.core.money import floor_to_step, normalize_order_quantity

if TYPE_CHECKING:
    from lightfee.core.domain import Side
    from lightfee.venues.specs import VenueSpec


def venue_reduce_only_close_exempts_min_notional(venue: Venue) -> bool:
    """Aster, Binance, and Gate reduce-only closes are exempt from min notional checks.

    V1: exit.rs:1948-1949 — venue_reduce_only_close_exempts_min_notional
    allows these venues to bypass min-notional on reduce-only close because
    the exchange itself rejects the order before it reaches the order book
    when the position is already flat or too small.
    """
    return venue in (Venue.ASTER, Venue.BINANCE, Venue.GATE)


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


# ---------------------------------------------------------------------------
# V1 passive price tick alignment (entry.rs:4646 align_passive_price_to_tick)
# ---------------------------------------------------------------------------


def align_passive_price_to_tick(price: float, tick_size: float, side: "Side") -> float:
    """V1 align_passive_price_to_tick (entry.rs line 4646).

    Buy:  floor(price / tick) * tick  (don't overpay)
    Sell: ceil(price / tick) * tick   (don't undersell)
    """
    from lightfee.core.domain import Side as _Side

    if not math.isfinite(price) or not math.isfinite(tick_size) or tick_size <= 0.0:
        return price
    if isinstance(side, str):
        side = _Side(side)
    if side == _Side.BUY:
        return math.floor(price / tick_size) * tick_size
    else:
        return math.ceil(price / tick_size) * tick_size


def resolve_price_tick(
    venue_spec: Optional["VenueSpec"] = None,
    adapter: Optional[object] = None,
    symbol: str = "",
) -> float:
    """Resolve canonical price tick for passive repricing.

    Precedence:
    1. Adapter's price_tick_size(symbol) if available
    2. VenueSpec.price_tick if > 0
    3. 0.0 (caller must not proceed with passive repricing)

    V1: passive_order_tick_size() in entry.rs line 2957.
    """
    if adapter is not None:
        tick = getattr(adapter, 'price_tick_size', lambda s: None)(symbol)
        if tick and tick > 0.0 and math.isfinite(tick):
            return tick
    if venue_spec is not None and venue_spec.price_tick > 0.0:
        return venue_spec.price_tick
    return 0.0


__all__ = [
    "floor_to_step",
    "normalize_order_quantity",
    "venue_reduce_only_close_exempts_min_notional",
    "floor_quantity_to_step",
    "apply_contract_size",
    "check_min_notional",
    "normalize_venue_quantity",
]
