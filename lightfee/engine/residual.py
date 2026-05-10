"""Residual one-leg exposure tracking matching Rust V1 residual.rs.

Rust references:
- src/execution_core/residual.rs: split_entry_fill_residual (line 25)
- src/execution_core/residual.rs: split_close_fill_residual (line 75)
- src/execution_core/entry_sync.rs: build_residual_task (line 749)
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from enum import Enum

from lightfee.core.domain import OrderFill, Side, Venue


class ResidualOrigin(Enum):
    ENTRY_OPEN = "entry_open"
    CLOSE_RESIDUAL = "close_residual"


_EPS = 1e-9
_RESIDUAL_REPAIR_MAX_ATTEMPTS = 3
_RESIDUAL_DEADLINE_DEFAULT_MS = 30_000


def approx_eq(a: float, b: float) -> bool:
    return abs(a - b) <= _EPS


def residual_pair_id(symbol: str, long_venue: Venue, short_venue: Venue) -> str:
    return f"{symbol.lower()}:{long_venue.value}->{short_venue.value}"


@dataclass
class ResidualExposureTask:
    position_id: str
    pair_id: str
    symbol: str
    long_venue: Venue
    short_venue: Venue
    origin: ResidualOrigin
    exposure_venue: Venue
    exposure_side: Side  # side to close the residual exposure
    exposure_quantity: float
    created_cycle: int = 0
    created_at_ms: int = 0
    deadline_ms: int = 0
    retry_count: int = 0
    last_attempt_at_ms: int = 0

    def __post_init__(self) -> None:
        if self.created_at_ms == 0:
            self.created_at_ms = int(time.time() * 1000)
        if self.deadline_ms == 0:
            self.deadline_ms = self.created_at_ms + _RESIDUAL_DEADLINE_DEFAULT_MS

    def increment_retry(self) -> None:
        self.retry_count += 1
        self.last_attempt_at_ms = int(time.time() * 1000)

    def is_exhausted(self) -> bool:
        return self.retry_count >= _RESIDUAL_REPAIR_MAX_ATTEMPTS


def split_entry_fill_residual(
    position_id: str,
    pair_id: str,
    symbol: str,
    long_venue: Venue,
    short_venue: Venue,
    long_fill: OrderFill,
    short_fill: OrderFill,
    created_cycle: int = 0,
    now_ms: int = 0,
    deadline_ms: int = 0,
) -> ResidualExposureTask | None:
    """V1 split_entry_fill_residual (line 25).

    Detects asymmetric dual-leg fills and returns a residual exposure task
    for the excess leg, or None when fills are matched.
    """
    long_qty = max(long_fill.quantity, 0.0)
    short_qty = max(short_fill.quantity, 0.0)

    if approx_eq(long_qty, short_qty):
        return None

    if now_ms == 0:
        now_ms = int(time.time() * 1000)
    if deadline_ms == 0:
        deadline_ms = now_ms + _RESIDUAL_DEADLINE_DEFAULT_MS

    if long_qty > short_qty:
        return ResidualExposureTask(
            position_id=position_id,
            pair_id=pair_id,
            symbol=symbol,
            long_venue=long_venue,
            short_venue=short_venue,
            origin=ResidualOrigin.ENTRY_OPEN,
            exposure_venue=long_venue,
            exposure_side=Side.SELL,  # sell to reduce excess long
            exposure_quantity=long_qty - short_qty,
            created_cycle=created_cycle,
            created_at_ms=now_ms,
            deadline_ms=deadline_ms,
        )
    else:
        return ResidualExposureTask(
            position_id=position_id,
            pair_id=pair_id,
            symbol=symbol,
            long_venue=long_venue,
            short_venue=short_venue,
            origin=ResidualOrigin.ENTRY_OPEN,
            exposure_venue=short_venue,
            exposure_side=Side.BUY,  # buy to reduce excess short
            exposure_quantity=short_qty - long_qty,
            created_cycle=created_cycle,
            created_at_ms=now_ms,
            deadline_ms=deadline_ms,
        )


def detect_residual(
    long_fill: OrderFill,
    short_fill: OrderFill,
) -> ResidualExposureTask | None:
    """Simplified detection: returns task if fills are asymmetric."""
    long_qty = max(long_fill.quantity, 0.0)
    short_qty = max(short_fill.quantity, 0.0)

    if approx_eq(long_qty, short_qty):
        return None

    if long_qty > short_qty:
        return ResidualExposureTask(
            position_id="",
            pair_id="",
            symbol=long_fill.symbol,
            long_venue=long_fill.venue,
            short_venue=short_fill.venue,
            origin=ResidualOrigin.ENTRY_OPEN,
            exposure_venue=long_fill.venue,
            exposure_side=Side.SELL,
            exposure_quantity=long_qty - short_qty,
        )
    else:
        return ResidualExposureTask(
            position_id="",
            pair_id="",
            symbol=short_fill.symbol,
            long_venue=long_fill.venue,
            short_venue=short_fill.venue,
            origin=ResidualOrigin.ENTRY_OPEN,
            exposure_venue=short_fill.venue,
            exposure_side=Side.BUY,
            exposure_quantity=short_qty - long_qty,
        )
