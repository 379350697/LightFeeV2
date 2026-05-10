"""Local L2 order book management with state machine transitions."""

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
    bootstrap_started_ms: int = 0
    degrade_count: int = 0
    last_error: str = ""

    # -- State machine transitions ---------------------------------------

    def transition_to_bootstrapping(self, now_ms: int) -> None:
        if self.status in (L2BookStatus.COLD, L2BookStatus.REBUILDING):
            self.status = L2BookStatus.BOOTSTRAPPING
            self.bootstrap_started_ms = now_ms

    def transition_to_hot(self) -> None:
        if self.status in (L2BookStatus.BOOTSTRAPPING, L2BookStatus.REBUILDING):
            self.status = L2BookStatus.HOT
            self.degrade_count = 0

    def transition_to_degraded(self, error: str = "") -> None:
        self.status = L2BookStatus.DEGRADED
        self.degrade_count += 1
        self.last_error = error

    def transition_to_rebuilding(self) -> None:
        if self.status == L2BookStatus.DEGRADED:
            self.status = L2BookStatus.REBUILDING

    def transition_to_suspended(self) -> None:
        self.status = L2BookStatus.SUSPENDED

    def is_healthy(self) -> bool:
        return self.status in (L2BookStatus.HOT, L2BookStatus.BOOTSTRAPPING)

    def is_stale(self, max_age_ms: int, now_ms: int) -> bool:
        return (now_ms - self.observed_at_ms) > max_age_ms


def promote_warm_to_hot(
    books: dict[str, LocalL2Book],
    max_hot: int = 3,
) -> int:
    """Promote top WARM books to HOT_EXEC pool (up to max_hot)."""
    hot_count = sum(1 for b in books.values() if b.pool == L2PoolAssignment.HOT_EXEC)
    promoted = 0
    for book in books.values():
        if hot_count >= max_hot:
            break
        if book.pool == L2PoolAssignment.WARM and book.status == L2BookStatus.HOT:
            book.pool = L2PoolAssignment.HOT_EXEC
            hot_count += 1
            promoted += 1
    return promoted
