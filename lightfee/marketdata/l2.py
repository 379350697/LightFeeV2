"""Local L2 order book management."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class L2BookStatus(Enum):
    COLD = "cold"
    BOOTSTRAPPING = "bootstrapping"
    HOT = "hot"
    DEGRADED = "degraded"
    REBUILDING = "rebuilding"
    SUSPENDED = "suspended"


class L2PoolAssignment(Enum):
    HOT_EXEC = "hot_exec"
    WARM = "warm"
    RETAINED = "retained"
    DROPPED = "dropped"


@dataclass
class PriceLevel:
    price: float
    quantity: float


@dataclass
class LocalL2Book:
    venue: str
    symbol: str
    bids: list[PriceLevel] = field(default_factory=list)
    asks: list[PriceLevel] = field(default_factory=list)
    status: L2BookStatus = L2BookStatus.COLD
    pool: L2PoolAssignment = L2PoolAssignment.DROPPED
    observed_at_ms: int = 0
