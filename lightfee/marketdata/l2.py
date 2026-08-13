"""Local L2 order book management — Rust V1 parity data structures and operations.

Covers:
  - Book keys, status, pool assignment, update/event dataclasses
  - Snapshot, delta, zero-size delete, sort, depth trim
  - Sequence gap detection, checksum verification hook
  - Age, staleness, readiness, status transitions
  - ExecutionLiquiditySource for true-L2 vs fallback tracking
"""

from __future__ import annotations

import binascii
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class L2BookStatus(Enum):
    COLD = "cold"
    BOOTSTRAPPING = "bootstrapping"
    HOT = "hot"
    DEGRADED = "degraded"
    REBUILDING = "rebuilding"
    SUSPENDED = "suspended"
    RESUME_WAITING = "resume_waiting"


class L2PoolAssignment(Enum):
    HOT_EXEC = "hot_exec"
    WARM = "warm"
    RETAINED = "retained"
    DROPPED = "dropped"


class LocalL2UpdateKind(Enum):
    SNAPSHOT = "snapshot"
    DELTA = "delta"
    ZERO_DELETE = "zero_delete"
    RESUME = "resume"
    REBUILD = "rebuild"


class LocalL2EventKind(Enum):
    BEST_BID_UPDATED = "best_bid_updated"
    BEST_ASK_UPDATED = "best_ask_updated"
    SPREAD_CHANGED = "spread_changed"
    MID_PRICE_CHANGED = "mid_price_changed"
    DEPTH_CHANGED = "depth_changed"
    SEQUENCE_GAP = "sequence_gap"
    CHECKSUM_MISMATCH = "checksum_mismatch"
    STALE = "stale"
    STATUS_TRANSITION = "status_transition"
    REBUILD_REQUIRED = "rebuild_required"
    RESUME = "resume"
    RESUMED = "resumed"
    BOOK_CLEARED = "book_cleared"


class ExecutionLiquiditySource(Enum):
    TRUE_L2 = "true_l2"
    TOP_BOOK = "top_book"
    CACHED = "cached"
    NONE = "none"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class PriceLevel:
    price: float
    quantity: float


@dataclass(frozen=True)
class LocalL2BookKey:
    venue: str
    symbol: str

    def __str__(self) -> str:
        return f"{self.venue}:{self.symbol}"


@dataclass
class LocalL2Update:
    venue: str
    symbol: str
    bids: list[PriceLevel] = field(default_factory=list)
    asks: list[PriceLevel] = field(default_factory=list)
    # V1 parity: some venues carry an update range (U..u) separately from
    # previous-link metadata (pu/pseq/prevSeqId).  Do not collapse these into
    # one field; Binance/Aster/Gate bridge snapshots by range overlap.
    first_sequence: int = 0
    sequence: int = 0
    previous_sequence: int = 0
    previous_sequence_present: bool = False
    checksum: int = 0
    raw_bids: list[tuple[str, str]] = field(default_factory=list)
    raw_asks: list[tuple[str, str]] = field(default_factory=list)
    event_time_ms: int = 0
    received_at_ms: int = 0
    update_kind: LocalL2UpdateKind = LocalL2UpdateKind.DELTA


@dataclass
class LocalL2Event:
    venue: str
    symbol: str
    event_kind: LocalL2EventKind
    wake_reason: str = ""
    observed_at_ms: int = 0
    sequence: int = 0
    detail: str = ""
    bid: float = 0.0
    ask: float = 0.0
    mid_price: float = 0.0


@dataclass
class LocalL2UpdateResult:
    applied: bool = False
    events: list[LocalL2Event] = field(default_factory=list)
    fault_reason: str = ""
    rebuild_required: bool = False


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

    # --- Rust V1 required fields ---
    last_update_id: int = 0
    sequence: int = 0
    checksum: int = 0
    raw_checksum_bids: list[tuple[float, float, str, str]] = field(default_factory=list)
    raw_checksum_asks: list[tuple[float, float, str, str]] = field(default_factory=list)
    last_snapshot_ms: int = 0
    last_delta_ms: int = 0
    resume_waiting_until_ms: int = 0
    runtime_suspended_until_ms: int = 0
    source: str = ""
    fault_reason: str = ""
    pending_snapshot_bridge: bool = False

    # --- V1 degrade/suspend thresholds ---
    max_consecutive_degradations: int = 3
    stall_timeout_ms: int = 60_000
    max_depth: int = 50
    stale_age_ms: int = 5_000

    # --- Check sequence gaps ---
    max_sequence_gap: int = 0  # 0 = strict continuity

    # ------------------------------------------------------------------
    # Pure book operations
    # ------------------------------------------------------------------

    def apply_snapshot(
        self,
        bids: list[PriceLevel],
        asks: list[PriceLevel],
        sequence: int = 0,
        checksum: int = 0,
        now_ms: int = 0,
        max_depth: int = 0,
    ) -> LocalL2UpdateResult:
        """Replace entire book with a snapshot. Returns events emitted."""
        depth = max_depth or self.max_depth
        next_bids = _sort_bids(bids)[:depth] if depth > 0 else _sort_bids(bids)
        next_asks = _sort_asks(asks)[:depth] if depth > 0 else _sort_asks(asks)
        fault = _book_structure_fault(next_bids, next_asks)
        if fault:
            return LocalL2UpdateResult(
                applied=False,
                events=[
                    _make_event(
                        self,
                        LocalL2EventKind.REBUILD_REQUIRED,
                        now_ms,
                        sequence,
                        detail=fault,
                    )
                ],
                fault_reason=fault,
                rebuild_required=True,
            )

        self.bids = next_bids
        self.asks = next_asks
        self.sequence = sequence
        if sequence > 0:
            self.last_update_id = sequence
        self.checksum = checksum
        self.last_snapshot_ms = now_ms
        self.observed_at_ms = now_ms

        events: list[LocalL2Event] = []
        if self.bids:
            events.append(_make_event(self, LocalL2EventKind.BEST_BID_UPDATED, now_ms, sequence,
                                      bid=self.bids[0].price))
        if self.asks:
            events.append(_make_event(self, LocalL2EventKind.BEST_ASK_UPDATED, now_ms, sequence,
                                      ask=self.asks[0].price))
        return LocalL2UpdateResult(applied=True, events=events)

    def apply_delta(
        self,
        bids: list[PriceLevel],
        asks: list[PriceLevel],
        sequence: int = 0,
        previous_sequence: int = 0,
        now_ms: int = 0,
        max_depth: int = 0,
    ) -> LocalL2UpdateResult:
        """Merge delta levels into current book. Zero-qty levels are deleted."""
        prev_seq = previous_sequence

        same_sequence_refresh = (
            self.sequence > 0
            and sequence == self.sequence
            and prev_seq == self.sequence
        )

        # V1 parity: when a venue omits previous-link metadata, do not synthesize
        # sequence - 1.  Bybit legitimately omits pu on some updates and V1 only
        # enforces the link when pu is present.  Stale numbered updates are still
        # ignored without mutating the book.
        if (
            self.sequence > 0
            and sequence > 0
            and sequence <= self.sequence
            and not same_sequence_refresh
            and prev_seq != self.sequence
        ):
            incoming = (
                f"incoming_prev={prev_seq}"
                if prev_seq > 0
                else f"incoming_sequence={sequence}"
            )
            return LocalL2UpdateResult(
                applied=False,
                events=[],
                fault_reason=(
                    f"stale_update prev={self.sequence} {incoming}"
                ),
                rebuild_required=False,
            )

        # Sequence gap detection. max_sequence_gap=0 means strict continuity.
        if self.sequence > 0 and prev_seq > 0 and prev_seq != self.sequence:
            gap = prev_seq - self.sequence
            if gap < 0:
                return LocalL2UpdateResult(
                    applied=False,
                    events=[],
                    fault_reason=(
                        f"stale_update prev={self.sequence} incoming_prev={prev_seq}"
                    ),
                    rebuild_required=False,
                )
            if gap > 0 and (self.max_sequence_gap <= 0 or gap > self.max_sequence_gap):
                return LocalL2UpdateResult(
                    applied=False,
                    events=[_make_event(self, LocalL2EventKind.SEQUENCE_GAP, now_ms, sequence,
                                        detail=f"gap={gap} prev={self.sequence} incoming_prev={prev_seq}")],
                    fault_reason=f"sequence_gap_{gap}",
                    rebuild_required=True,
                )
            if gap > 0:
                events = [_make_event(self, LocalL2EventKind.SEQUENCE_GAP, now_ms, sequence,
                                      detail=f"small_gap={gap}")]
            else:
                events = []
        else:
            events = []

        depth = max_depth or self.max_depth

        next_bids = _merge_levels(self.bids, bids, side="bid", max_depth=depth)
        next_asks = _merge_levels(self.asks, asks, side="ask", max_depth=depth)
        fault = _book_structure_fault(next_bids, next_asks)
        if fault:
            events.append(
                _make_event(
                    self,
                    LocalL2EventKind.REBUILD_REQUIRED,
                    now_ms,
                    sequence,
                    detail=fault,
                )
            )
            return LocalL2UpdateResult(
                applied=False,
                events=events,
                fault_reason=fault,
                rebuild_required=True,
            )

        self.bids = next_bids
        self.asks = next_asks

        self.sequence = sequence
        if sequence > 0:
            self.last_update_id = sequence
        self.last_delta_ms = now_ms
        self.observed_at_ms = now_ms

        if self.bids:
            events.append(_make_event(self, LocalL2EventKind.BEST_BID_UPDATED, now_ms, sequence,
                                      bid=self.bids[0].price))
        if self.asks:
            events.append(_make_event(self, LocalL2EventKind.BEST_ASK_UPDATED, now_ms, sequence,
                                      ask=self.asks[0].price))
        mid = self.mid_price()
        if mid > 0:
            events.append(_make_event(self, LocalL2EventKind.MID_PRICE_CHANGED, now_ms, sequence,
                                      mid_price=mid))

        return LocalL2UpdateResult(applied=True, events=events)

    def apply_raw_checksum_update(
        self,
        bids: list[tuple[str, str]],
        asks: list[tuple[str, str]],
        update_kind: LocalL2UpdateKind,
        max_depth: int = 200,
    ) -> None:
        """Maintain V1-style raw-string checksum book for OKX/Bitget.

        V1 computes CRC over the original exchange strings, not float-normalized
        values.  The execution book can use floats, but checksum verification
        must retain the wire text and process zero-size deletes.
        """
        depth = max(1, max_depth)
        if update_kind == LocalL2UpdateKind.SNAPSHOT:
            self.raw_checksum_bids = _sanitize_raw_checksum_side(bids, descending=True, max_depth=depth)
            self.raw_checksum_asks = _sanitize_raw_checksum_side(asks, descending=False, max_depth=depth)
            return

        self.raw_checksum_bids = _merge_raw_checksum_side(
            self.raw_checksum_bids,
            bids,
            descending=True,
            max_depth=depth,
        )
        self.raw_checksum_asks = _merge_raw_checksum_side(
            self.raw_checksum_asks,
            asks,
            descending=False,
            max_depth=depth,
        )

    def compute_raw_checksum(self) -> int:
        if not self.raw_checksum_bids or not self.raw_checksum_asks:
            return 0
        parts: list[str] = []
        for idx in range(25):
            if idx < len(self.raw_checksum_bids):
                _, _, price_text, quantity_text = self.raw_checksum_bids[idx]
                parts.append(f"{price_text}:{quantity_text}")
            if idx < len(self.raw_checksum_asks):
                _, _, price_text, quantity_text = self.raw_checksum_asks[idx]
                parts.append(f"{price_text}:{quantity_text}")
        raw = ":".join(parts)
        checksum = binascii.crc32(raw.encode()) & 0xFFFFFFFF
        if checksum >= 2**31:
            checksum -= 2**32
        return checksum

    def verify_raw_checksum(self, expected: int, now_ms: int) -> LocalL2UpdateResult:
        actual = self.compute_raw_checksum()
        if actual != 0 and expected != 0 and actual != expected:
            return LocalL2UpdateResult(
                applied=True,
                events=[_make_event(self, LocalL2EventKind.CHECKSUM_MISMATCH, now_ms, self.sequence,
                                    detail=f"expected={expected} actual={actual}")],
                fault_reason=f"checksum_mismatch expected={expected} actual={actual}",
            )
        return LocalL2UpdateResult(applied=True, events=[])

    def verify_checksum(self, expected: int, now_ms: int) -> LocalL2UpdateResult:
        """Optional checksum verification hook. Returns checksum_mismatch event on failure."""
        actual = self.compute_checksum()
        if actual != 0 and expected != 0 and actual != expected:
            return LocalL2UpdateResult(
                applied=True,
                events=[_make_event(self, LocalL2EventKind.CHECKSUM_MISMATCH, now_ms, self.sequence,
                                    detail=f"expected={expected} actual={actual}")],
                fault_reason=f"checksum_mismatch expected={expected} actual={actual}",
            )
        return LocalL2UpdateResult(applied=True, events=[])

    def compute_checksum(self) -> int:
        """Deterministic signed CRC32 checksum over top book prices and sizes.

        Uses the OKX order-book checksum layout: up to 25 bid/ask levels,
        alternating bid price:size and ask price:size, returned as signed int32.
        Non-OKX venues should set ChecksumMode.NONE (CRC32 is never silently applied).
        """
        if not self.bids or not self.asks:
            return 0
        parts: list[str] = []
        for idx in range(25):
            if idx < len(self.bids):
                bid = self.bids[idx]
                parts.extend([_checksum_number(bid.price), _checksum_number(bid.quantity)])
            if idx < len(self.asks):
                ask = self.asks[idx]
                parts.extend([_checksum_number(ask.price), _checksum_number(ask.quantity)])
        raw = ":".join(parts)
        checksum = binascii.crc32(raw.encode()) & 0xFFFFFFFF
        if checksum >= 2**31:
            checksum -= 2**32
        return checksum

    def clear_book(self, now_ms: int) -> list[LocalL2Event]:
        """Clear all levels, emit BOOK_CLEARED event."""
        self.bids.clear()
        self.asks.clear()
        self.observed_at_ms = now_ms
        return [_make_event(self, LocalL2EventKind.BOOK_CLEARED, now_ms, self.sequence)]

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def best_bid(self) -> float:
        return self.bids[0].price if self.bids else 0.0

    def best_ask(self) -> float:
        return self.asks[0].price if self.asks else 0.0

    def mid_price(self) -> float:
        bb = self.best_bid()
        ba = self.best_ask()
        if bb > 0 and ba > 0:
            return (bb + ba) / 2.0
        return 0.0

    def spread_bps(self) -> float:
        bb = self.best_bid()
        ba = self.best_ask()
        if bb > 0 and ba > 0:
            return (ba - bb) / bb * 10000.0
        return 0.0

    def depth_bid(self, depth: int) -> list[PriceLevel]:
        return self.bids[:depth]

    def depth_ask(self, depth: int) -> list[PriceLevel]:
        return self.asks[:depth]

    def quantity_at_price(self, side: str, price: float) -> float:
        levels = self.bids if side.lower() in ("buy", "bid") else self.asks
        for lvl in levels:
            if lvl.price == price:
                return lvl.quantity
        return 0.0

    def cumulative_bid_quantity(self, from_price: float, depth: int = 0) -> float:
        d = depth or len(self.bids)
        return sum(lvl.quantity for lvl in self.bids[:d] if lvl.price >= from_price)

    def cumulative_ask_quantity(self, to_price: float, depth: int = 0) -> float:
        d = depth or len(self.asks)
        return sum(lvl.quantity for lvl in self.asks[:d] if lvl.price <= to_price)

    def vwap_buy(self, target_quote: float) -> tuple[float, float]:
        """Estimate VWAP and filled quantity for buying target_quote notional."""
        return _walk_levels(self.asks, target_quote)

    def vwap_sell(self, target_quote: float) -> tuple[float, float]:
        """Estimate VWAP and filled quantity for selling target_quote notional."""
        return _walk_levels(self.bids, target_quote)

    def has_crossed_book(self) -> bool:
        bb = self.best_bid()
        ba = self.best_ask()
        return bb > 0 and ba > 0 and bb >= ba

    # ------------------------------------------------------------------
    # Age / readiness
    # ------------------------------------------------------------------

    def is_stale(self, max_age_ms: int, now_ms: int) -> bool:
        if self.observed_at_ms <= 0:
            return True
        return (now_ms - self.observed_at_ms) > max_age_ms

    def is_healthy(self) -> bool:
        return self.status in (L2BookStatus.HOT, L2BookStatus.BOOTSTRAPPING)

    def is_ready(self, max_age_ms: int, now_ms: int) -> bool:
        """True if book is HOT and within freshness window."""
        return self.status == L2BookStatus.HOT and not self.is_stale(max_age_ms, now_ms)

    def has_valid_execution_structure(self) -> bool:
        """Whether the retained snapshot satisfies V1 execution-book invariants."""
        def valid_side(levels: list[PriceLevel], *, descending: bool) -> bool:
            if not levels:
                return False
            previous_price: float | None = None
            for level in levels:
                try:
                    price = float(level.price)
                    quantity = float(level.quantity)
                except (AttributeError, TypeError, ValueError):
                    return False
                if (
                    not math.isfinite(price)
                    or not math.isfinite(quantity)
                    or price <= 0.0
                    or quantity <= 0.0
                ):
                    return False
                if previous_price is not None:
                    ordered = previous_price > price if descending else previous_price < price
                    if not ordered:
                        return False
                previous_price = price
            return True

        return (
            valid_side(self.bids, descending=True)
            and valid_side(self.asks, descending=False)
            and self.best_bid() < self.best_ask()
        )

    def execution_snapshot_is_valid(self, max_age_ms: int, now_ms: int) -> bool:
        """V1 export contract: a fresh, structurally valid L2 snapshot.

        Lifecycle status is owned by the entry Local-L2 session.  A DEGRADED
        session may consume its last valid snapshot until it becomes stale or
        structurally invalid.
        """
        return not self.is_stale(max_age_ms, now_ms) and self.has_valid_execution_structure()

    def age_ms(self, now_ms: int) -> int:
        if self.observed_at_ms <= 0:
            return 0
        return now_ms - self.observed_at_ms

    def check_stall(self, now_ms: int) -> bool:
        if self.observed_at_ms == 0:
            return False
        return (now_ms - self.observed_at_ms) > self.stall_timeout_ms

    def resume_waiting_remaining_ms(self, now_ms: int) -> int:
        if self.resume_waiting_until_ms <= 0:
            return 0
        return max(0, self.resume_waiting_until_ms - now_ms)

    # ------------------------------------------------------------------
    # State machine transitions
    # ------------------------------------------------------------------

    def transition_to_bootstrapping(self, now_ms: int) -> None:
        if self.status in (L2BookStatus.COLD, L2BookStatus.REBUILDING, L2BookStatus.RESUME_WAITING):
            self.status = L2BookStatus.BOOTSTRAPPING
            self.bootstrap_started_ms = now_ms
            # V1 parity: fault_reason is preserved through BOOTSTRAPPING so
            # apply_book_readiness_to_leg can derive the correct arming_reason
            # (e.g., sequence_gap → SEQUENCE_GAP, stale → STALE_BOOK_RECOVERY).
            # fault_reason is cleared only on successful transition_to_hot().

    def transition_to_hot(self) -> None:
        if self.status in (
            L2BookStatus.BOOTSTRAPPING,
            L2BookStatus.REBUILDING,
            L2BookStatus.DEGRADED,
            L2BookStatus.RESUME_WAITING,
        ):
            self.status = L2BookStatus.HOT
            self.fault_reason = ""

    def transition_to_degraded(self, error: str = "") -> None:
        self.status = L2BookStatus.DEGRADED
        self.degrade_count += 1
        self.last_error = error
        self.fault_reason = error
        if self.degrade_count >= self.max_consecutive_degradations:
            self.status = L2BookStatus.SUSPENDED

    def transition_to_rebuilding(self, now_ms: int = 0) -> None:
        if self.status in (L2BookStatus.DEGRADED, L2BookStatus.SUSPENDED,
                           L2BookStatus.HOT, L2BookStatus.BOOTSTRAPPING):
            self.status = L2BookStatus.REBUILDING

    def transition_to_suspended(self, reason: str = "") -> None:
        self.status = L2BookStatus.SUSPENDED
        if reason:
            self.fault_reason = reason

    def transition_to_resume_waiting(self, until_ms: int) -> None:
        self.status = L2BookStatus.RESUME_WAITING
        self.resume_waiting_until_ms = until_ms

    @property
    def key(self) -> LocalL2BookKey:
        return LocalL2BookKey(venue=self.venue, symbol=self.symbol)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sort_bids(levels: list[PriceLevel]) -> list[PriceLevel]:
    return sorted(levels, key=lambda x: x.price, reverse=True)


def _sort_asks(levels: list[PriceLevel]) -> list[PriceLevel]:
    return sorted(levels, key=lambda x: x.price)


def _book_structure_fault(bids: list[PriceLevel], asks: list[PriceLevel]) -> str:
    if not bids:
        return "book_empty_side_bid"
    if not asks:
        return "book_empty_side_ask"
    best_bid = bids[0].price
    best_ask = asks[0].price
    if best_bid <= 0 or best_ask <= 0:
        return f"non_positive_top best_bid={best_bid} best_ask={best_ask}"
    if best_bid >= best_ask:
        return f"crossed_or_locked_book best_bid={best_bid} best_ask={best_ask}"
    return ""


def _checksum_number(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return format(value, "f").rstrip("0").rstrip(".")


def _raw_checksum_level(level: tuple[str, str]) -> tuple[float, float, str, str] | None:
    if len(level) < 2:
        return None
    price_text = str(level[0])
    quantity_text = str(level[1])
    try:
        price = float(price_text)
        quantity = float(quantity_text)
    except (TypeError, ValueError):
        return None
    if not (price > 0):
        return None
    return (price, quantity, price_text, quantity_text)


def _sanitize_raw_checksum_side(
    levels: list[tuple[str, str]],
    *,
    descending: bool,
    max_depth: int,
) -> list[tuple[float, float, str, str]]:
    parsed = [
        item for item in (_raw_checksum_level(level) for level in levels)
        if item is not None and item[1] > 0
    ]
    parsed.sort(key=lambda item: item[0], reverse=descending)
    return parsed[:max_depth]


def _merge_raw_checksum_side(
    existing: list[tuple[float, float, str, str]],
    incoming: list[tuple[str, str]],
    *,
    descending: bool,
    max_depth: int,
) -> list[tuple[float, float, str, str]]:
    by_price: dict[float, tuple[float, float, str, str]] = {
        level[0]: level for level in existing
    }
    for raw_level in incoming:
        parsed = _raw_checksum_level(raw_level)
        if parsed is None:
            continue
        price, quantity, _, _ = parsed
        if quantity <= 0:
            by_price.pop(price, None)
        else:
            by_price[price] = parsed
    merged = list(by_price.values())
    merged.sort(key=lambda item: item[0], reverse=descending)
    return merged[:max_depth]


def _merge_levels(
    existing: list[PriceLevel],
    incoming: list[PriceLevel],
    side: str,
    max_depth: int = 0,
) -> list[PriceLevel]:
    """Merge delta levels into existing levels.

    - qty == 0 → delete level
    - price exists → update qty
    - price new → insert
    - re-sort after merge
    """
    price_map: dict[float, float] = {lvl.price: lvl.quantity for lvl in existing}
    for lvl in incoming:
        if lvl.quantity <= 0:
            price_map.pop(lvl.price, None)
        else:
            price_map[lvl.price] = lvl.quantity

    merged = [PriceLevel(price=p, quantity=q) for p, q in price_map.items()]
    if side == "bid":
        merged = _sort_bids(merged)
    else:
        merged = _sort_asks(merged)

    if max_depth > 0:
        merged = merged[:max_depth]
    return merged


def _make_event(
    book: LocalL2Book,
    kind: LocalL2EventKind,
    now_ms: int,
    sequence: int,
    bid: float = 0.0,
    ask: float = 0.0,
    mid_price: float = 0.0,
    detail: str = "",
) -> LocalL2Event:
    return LocalL2Event(
        venue=book.venue,
        symbol=book.symbol,
        event_kind=kind,
        observed_at_ms=now_ms,
        sequence=sequence,
        bid=bid,
        ask=ask,
        mid_price=mid_price,
        detail=detail,
    )


def _walk_levels(levels: list[PriceLevel], target_quote: float) -> tuple[float, float]:
    if not levels or target_quote <= 0:
        return (0.0, 0.0)
    filled_quote = 0.0
    cost_basis = 0.0
    for lvl in levels:
        level_quote = lvl.price * lvl.quantity
        if level_quote <= 0:
            continue
        take = min(level_quote, target_quote - filled_quote)
        filled_quote += take
        cost_basis += take * lvl.price
        if filled_quote >= target_quote:
            break
    if filled_quote <= 0:
        return (0.0, 0.0)
    return (filled_quote, cost_basis / filled_quote)


# ---------------------------------------------------------------------------
# Pool management
# ---------------------------------------------------------------------------

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
