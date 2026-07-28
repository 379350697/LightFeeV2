"""V1 private WebSocket state: order/position caches, connection health, workers.

Direct V1 port of src/live/private_ws.rs — WsPrivateState and all helpers.
Shared state plumbing only; venue protocol logic lives in per-venue workers.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional

from lightfee.core.domain import (
    OrderFill,
    PassiveOrderState,
    PositionSnapshot,
    Side,
    Venue,
)
from lightfee.marketdata.resilience import ConnectionHealth

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_MAX_ORDER_ENTRIES: int = 512


# ---------------------------------------------------------------------------
# Private order / position update types (V1 exact equivalents)
# ---------------------------------------------------------------------------


@dataclass
class PrivateOrderUpdate:
    """V1 PrivateOrderUpdate: snapshot of a private order from WS push."""

    symbol: str
    order_id: str
    client_order_id: Optional[str] = None
    filled_quantity: Optional[float] = None
    average_price: Optional[float] = None
    fee_quote: Optional[float] = None
    state: Optional[PassiveOrderState] = None
    updated_at_ms: int = 0


@dataclass
class PrivatePositionUpdate:
    """V1 PrivatePositionUpdate: size + timestamp from private WS push."""

    symbol: str
    size: float
    updated_at_ms: int = 0


# ---------------------------------------------------------------------------
# Cumulative order progress (V1 equivalent)
# ---------------------------------------------------------------------------


@dataclass
class CumulativeOrderProgress:
    """V1 CumulativeOrderProgress: flattened progress for reconciliation."""

    order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    cumulative_quantity: float = 0.0
    average_price: Optional[float] = None
    fee_quote: Optional[float] = None
    state: Optional[PassiveOrderState] = None
    updated_at_ms: Optional[int] = None
    last_fill_at_ms: Optional[int] = None
    # V1 tagged-enum discriminant for priority resolution:
    # "reconciliation" (highest), "rest_snapshot", "private_ws" (lowest)
    source: str = "private_ws"

    @staticmethod
    def from_private(update: PrivateOrderUpdate) -> CumulativeOrderProgress:
        """V1 CumulativeOrderProgress::from_private() — exact port."""
        cumulative_quantity = 0.0
        if (
            update.filled_quantity is not None
            and update.filled_quantity > 0.0
            and _is_finite(update.filled_quantity)
        ):
            cumulative_quantity = update.filled_quantity
        return CumulativeOrderProgress(
            order_id=update.order_id if update.order_id else None,
            client_order_id=(
                update.client_order_id
                if update.client_order_id
                else None
            ),
            cumulative_quantity=cumulative_quantity,
            average_price=(
                update.average_price
                if cumulative_quantity > 0.0
                and update.average_price is not None
                and update.average_price > 0.0
                and _is_finite(update.average_price)
                else None
            ),
            fee_quote=(
                update.fee_quote
                if cumulative_quantity > 0.0
                and update.fee_quote is not None
                and update.fee_quote >= 0.0
                and _is_finite(update.fee_quote)
                else None
            ),
            state=update.state,
            updated_at_ms=update.updated_at_ms if update.updated_at_ms > 0 else None,
            last_fill_at_ms=(
                update.updated_at_ms
                if cumulative_quantity > 0.0 and update.updated_at_ms > 0
                else None
            ),
            source="private_ws",
        )

    @staticmethod
    def from_position_snapshot(
        order_id: Optional[str],
        client_order_id: Optional[str],
        cumulative_quantity: float,
        average_price: Optional[float],
        fee_quote: Optional[float],
        updated_at_ms: Optional[int],
    ) -> CumulativeOrderProgress:
        """V1 CumulativeOrderProgress::from_snapshot() — REST fallback."""
        has_fill = cumulative_quantity > 0.0 and updated_at_ms is not None and updated_at_ms > 0
        return CumulativeOrderProgress(
            order_id=order_id,
            client_order_id=client_order_id,
            cumulative_quantity=cumulative_quantity,
            average_price=average_price,
            fee_quote=fee_quote,
            updated_at_ms=updated_at_ms,
            last_fill_at_ms=updated_at_ms if has_fill else None,
            source="rest_snapshot",
        )

    @staticmethod
    def from_reconciliation(recon) -> CumulativeOrderProgress:
        """V1 CumulativeOrderProgress::from_reconciliation()."""
        return CumulativeOrderProgress(
            order_id=recon.order_id if recon.order_id else None,
            client_order_id=recon.client_order_id,
            cumulative_quantity=recon.quantity,
            average_price=recon.average_price,
            fee_quote=recon.fee_quote,
            updated_at_ms=recon.filled_at_ms,
            last_fill_at_ms=recon.filled_at_ms if recon.filled_at_ms > 0 else None,
            source="reconciliation",
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_finite(value: float) -> bool:
    import math

    return math.isfinite(value)


def _canonical_order_key(update: PrivateOrderUpdate) -> Optional[str]:
    """V1 canonical_order_key() — use order_id or client_order_id."""
    if update.order_id:
        return f"order:{update.order_id}"
    if update.client_order_id:
        return f"client:{update.client_order_id}"
    return None


def _private_order_updates_match(
    left: PrivateOrderUpdate, right: PrivateOrderUpdate
) -> bool:
    """V1 private_order_updates_match()."""
    if left.order_id and left.order_id == right.order_id:
        return True
    if (
        left.client_order_id
        and right.client_order_id
        and left.client_order_id == right.client_order_id
    ):
        return True
    return False


def _now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# PrivateWsState (V1 WsPrivateState exact port)
# ---------------------------------------------------------------------------


class PrivateWsState:
    """V1 WsPrivateState: order/position caches, connection health, worker lifecycle.

    Thread-safe via threading.Lock (protects both sync reads and async writes).
    Async wait via asyncio.Event + version counter.
    """

    def __init__(self, max_order_entries: int = DEFAULT_MAX_ORDER_ENTRIES) -> None:
        self._max_order_entries = max(max_order_entries, 1)

        # Order cache: keyed by canonical key, with dual index
        self._orders: dict[str, PrivateOrderUpdate] = {}
        self._client_index: dict[str, str] = {}  # client_order_id → canonical_key
        self._order_index: dict[str, str] = {}  # order_id → canonical_key

        # Position cache
        self._positions: dict[str, PrivatePositionUpdate] = {}

        # Connection health
        self._health = ConnectionHealth()

        # Worker tracking: list of asyncio Tasks
        self._workers: list[asyncio.Task] = []

        # Async notification for lookup_or_wait
        self._order_update_version: int = 0
        self._order_update_event = asyncio.Event()

        # Lock for thread-safe access (threading.Lock — works in both sync + async)
        import threading
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Order cache
    # ------------------------------------------------------------------

    async def record_order(self, update: PrivateOrderUpdate) -> None:
        """V1 record_order() — newest-update-wins, bounded, dual-indexed."""
        default_key = _canonical_order_key(update)
        if default_key is None:
            return

        with self._lock:
            # Resolve canonical key via existing indexes
            cache_key = (
                self._order_index.get(update.order_id)
                or (
                    update.client_order_id
                    and self._client_index.get(update.client_order_id)
                )
                or default_key
            )

            # Reject stale updates
            existing = self._orders.get(cache_key)
            if existing is not None and existing.updated_at_ms > update.updated_at_ms:
                return

            # Store
            self._orders[cache_key] = update
            if update.client_order_id:
                self._client_index[update.client_order_id] = cache_key
            if update.order_id:
                self._order_index[update.order_id] = cache_key

            # Evict oldest if over capacity
            while len(self._orders) > self._max_order_entries:
                oldest_key = min(
                    self._orders.keys(),
                    key=lambda k: self._orders[k].updated_at_ms,
                )
                oldest = self._orders.pop(oldest_key)
                if (
                    oldest.client_order_id
                    and self._client_index.get(oldest.client_order_id) == oldest_key
                ):
                    del self._client_index[oldest.client_order_id]
                if (
                    oldest.order_id
                    and self._order_index.get(oldest.order_id) == oldest_key
                ):
                    del self._order_index[oldest.order_id]

            # Notify waiters
            self._order_update_version += 1
            self._order_update_event.set()
            self._order_update_event.clear()

    def order_by_client_id(self, client_order_id: str) -> Optional[PrivateOrderUpdate]:
        """V1 order_by_client_id()."""
        with self._lock:
            key = self._client_index.get(client_order_id)
            if key is None:
                return None
            return self._orders.get(key)

    def order_by_order_id(self, order_id: str) -> Optional[PrivateOrderUpdate]:
        """V1 order_by_order_id()."""
        with self._lock:
            key = self._order_index.get(order_id)
            if key is None:
                return None
            return self._orders.get(key)

    def active_orders_for_symbol_if_fresh(
        self,
        symbol: str,
        max_age_ms: int,
        wall_clock_now_ms: int,
    ) -> list[PrivateOrderUpdate]:
        """Return fresh concrete active-order truth for one symbol."""
        symbol_key = str(symbol or "").upper()
        with self._lock:
            return sorted(
                [
                    update
                    for update in self._orders.values()
                    if str(update.symbol or "").upper() == symbol_key
                    and update.state is not None
                    and not update.state.is_terminal()
                    and (
                        max_age_ms <= 0
                        or wall_clock_now_ms - update.updated_at_ms <= max_age_ms
                    )
                ],
                key=lambda update: (update.updated_at_ms, update.order_id),
                reverse=True,
            )

    def order_progress_if_fresh(
        self,
        client_order_id: Optional[str] = None,
        order_id: Optional[str] = None,
        max_age_ms: int = 0,
        wall_clock_now_ms: int = 0,
    ) -> Optional[CumulativeOrderProgress]:
        """V1 order_progress_if_fresh() — resolve + freshness check."""
        with self._lock:
            by_client: Optional[PrivateOrderUpdate] = None
            if client_order_id:
                key = self._client_index.get(client_order_id)
                if key is not None:
                    by_client = self._orders.get(key)
            by_order: Optional[PrivateOrderUpdate] = None
            if order_id:
                key = self._order_index.get(order_id)
                if key is not None:
                    by_order = self._orders.get(key)

            update: Optional[PrivateOrderUpdate] = None
            if by_client is not None and by_order is not None:
                if not _private_order_updates_match(by_client, by_order):
                    return None
                update = (
                    by_client
                    if by_client.updated_at_ms >= by_order.updated_at_ms
                    else by_order
                )
            elif by_client is not None:
                update = by_client
            elif by_order is not None:
                update = by_order

        if update is None:
            return None

        if (
            max_age_ms > 0
            and wall_clock_now_ms - update.updated_at_ms > max_age_ms
        ):
            return None
        return CumulativeOrderProgress.from_private(update)

    # ------------------------------------------------------------------
    # Position cache
    # ------------------------------------------------------------------

    async def update_position(
        self, symbol: str, size: float, updated_at_ms: int
    ) -> None:
        """V1 update_position() — newest-update-wins."""
        with self._lock:
            existing = self._positions.get(symbol)
            if existing is not None and existing.updated_at_ms > updated_at_ms:
                return
            self._positions[symbol] = PrivatePositionUpdate(
                symbol=symbol, size=size, updated_at_ms=updated_at_ms
            )

    def position(self, symbol: str) -> Optional[PrivatePositionUpdate]:
        """V1 position()."""
        with self._lock:
            return self._positions.get(symbol)

    def position_if_fresh(
        self, symbol: str, max_age_ms: int, wall_clock_now_ms: int
    ) -> Optional[PrivatePositionUpdate]:
        """V1 position_if_fresh()."""
        with self._lock:
            pos = self._positions.get(symbol)
            if pos is None:
                return None
        if max_age_ms <= 0:
            return pos
        age_ms = wall_clock_now_ms - pos.updated_at_ms
        if age_ms > max_age_ms:
            return None
        return pos

    def positions_if_fresh(
        self, max_age_ms: int, wall_clock_now_ms: int
    ) -> list[PrivatePositionUpdate]:
        """V1 positions_if_fresh()."""
        with self._lock:
            positions = sorted(
                [
                    p
                    for p in self._positions.values()
                    if max_age_ms <= 0
                    or wall_clock_now_ms - p.updated_at_ms <= max_age_ms
                ],
                key=lambda p: p.symbol,
            )
            return positions

    # ------------------------------------------------------------------
    # Connection health
    # ------------------------------------------------------------------

    def record_connection_success(self, now_ms: int) -> None:
        """V1 record_connection_success()."""
        self._health.record_success(now_ms)

    def record_connection_failure(
        self, now_ms: int, unhealthy_after_failures: int, error: str
    ) -> None:
        """V1 record_connection_failure()."""
        self._health.record_failure(now_ms, unhealthy_after_failures, error)

    def connection_health(self) -> ConnectionHealth:
        """V1 connection_health()."""
        return self._health

    # ------------------------------------------------------------------
    # Worker lifecycle
    # ------------------------------------------------------------------

    def push_worker(self, worker: asyncio.Task) -> None:
        """V1 push_worker() — replaces existing live workers, prunes finished."""
        self._prune_finished_workers()
        for existing in self._workers:
            existing.cancel()
        self._workers.clear()
        self._workers.append(worker)

    def abort_workers(self) -> None:
        """V1 abort_workers()."""
        self._prune_finished_workers()
        for worker in self._workers:
            worker.cancel()
        self._workers.clear()

    def worker_count(self) -> int:
        """V1 worker_count()."""
        self._prune_finished_workers()
        return len(self._workers)

    def _prune_finished_workers(self) -> None:
        self._workers = [w for w in self._workers if not w.done()]

    # ------------------------------------------------------------------
    # Async wait primitives
    # ------------------------------------------------------------------

    async def _wait_for_update(self, after_version: int, timeout_ms: int) -> bool:
        """Wait for an order update notification with timeout."""
        if self._order_update_version > after_version:
            return True
        if timeout_ms <= 0:
            return self._order_update_version > after_version
        try:
            await asyncio.wait_for(
                self._order_update_event.wait(), timeout=timeout_ms / 1000.0
            )
            return self._order_update_version > after_version
        except asyncio.TimeoutError:
            return False

    # ------------------------------------------------------------------
    # Position cache sync to transport
    # ------------------------------------------------------------------

    def populate_position_cache(
        self, transport_position_cache: dict
    ) -> None:
        """Push private position updates into the transport-level position cache.

        Each entry: symbol → (PositionSnapshot, cached_at_ms).
        Only writes fresh updates (newest-update-wins vs existing cache).
        """
        now_ms = _now_ms()
        for symbol, pos in self._positions.items():
            existing = transport_position_cache.get(symbol)
            if existing is not None:
                _, cached_at = existing
                if cached_at >= pos.updated_at_ms:
                    continue
            transport_position_cache[symbol] = (
                PositionSnapshot(
                    venue=Venue.BINANCE,  # caller overrides
                    symbol=symbol,
                    side=Side.BUY if pos.size >= 0 else Side.SELL,
                    quantity=abs(pos.size),
                    entry_price=0.0,
                    observed_at_ms=pos.updated_at_ms,
                ),
                now_ms,
            )


# ---------------------------------------------------------------------------
# Merge passive progress sources (V1 merge_passive_progress_sources)
# ---------------------------------------------------------------------------


def resolve_cumulative_order_progress(
    sources: list[CumulativeOrderProgress],
) -> Optional[CumulativeOrderProgress]:
    """V1 resolve_cumulative_order_progress (ports.rs:147).

    Priority: highest cumulative_quantity wins. Within that tier:
    reconciliation > REST detail > private WS.
    Identity fields (order_id, client_order_id) come from the
    highest-quantity sources; fall back to any source.
    """
    if not sources:
        return None

    # Find max cumulative quantity
    cumulative_qty = 0.0
    for s in sources:
        qty = s.cumulative_quantity if _is_finite(s.cumulative_quantity) else 0.0
        if qty > cumulative_qty:
            cumulative_qty = qty

    # Select sources at or near the max (within tolerance)
    if cumulative_qty > 0.0:
        tolerance = max(cumulative_qty * 1e-9, 1e-12)
        highest = [
            s for s in sources
            if abs(max(s.cumulative_quantity, 0.0) - cumulative_qty) <= tolerance
        ]
        if not highest:
            highest = sources
    else:
        highest = sources

    # Identity fields: prefer highest-quantity sources, then any source
    def _first_from(sources_list, field):
        for s in sources_list:
            val = getattr(s, field, None)
            if val is not None and val != "" and val != 0:
                return val
        return None

    order_id = _first_from(highest, "order_id") or _first_from(sources, "order_id")
    client_order_id = _first_from(highest, "client_order_id") or _first_from(sources, "client_order_id")

    # Price: from highest-quantity sources
    avg_price = None
    for s in highest:
        p = s.average_price
        if p is not None and p > 0.0 and _is_finite(p):
            avg_price = p
            break
    if avg_price is None:
        for s in sources:
            p = s.average_price
            if p is not None and p > 0.0 and _is_finite(p):
                avg_price = p
                break

    # Fee: from highest-quantity sources
    fee = None
    for s in highest:
        f = s.fee_quote
        if f is not None and f >= 0.0 and _is_finite(f):
            fee = f
            break

    # Timestamps: V1 max() within highest-quantity sources, fallback to all sources
    # (ports.rs:242-260 — filter_map + max on highest, or_else max on all)
    last_fill_at_ms = None
    for s in highest:
        t = s.last_fill_at_ms
        if t is not None and t > 0 and (last_fill_at_ms is None or t > last_fill_at_ms):
            last_fill_at_ms = t
    if last_fill_at_ms is None:
        for s in sources:
            t = s.last_fill_at_ms
            if t is not None and t > 0 and (last_fill_at_ms is None or t > last_fill_at_ms):
                last_fill_at_ms = t

    # State: from highest-quantity source that has one, fallback to all sources
    # V1: find_map on highest, or_else find_map on all sources (ports.rs:236-240)
    state = None
    for s in highest:
        if s.state is not None:
            state = s.state
            break
    if state is None:
        for s in sources:
            if s.state is not None:
                state = s.state
                break

    # Updated at: V1 max() within highest-quantity sources, fallback to all sources
    # (ports.rs:242-250)
    updated_at_ms = None
    for s in highest:
        t = s.updated_at_ms
        if t is not None and (updated_at_ms is None or t > updated_at_ms):
            updated_at_ms = t
    if updated_at_ms is None:
        for s in sources:
            t = s.updated_at_ms
            if t is not None and (updated_at_ms is None or t > updated_at_ms):
                updated_at_ms = t

    return CumulativeOrderProgress(
        order_id=order_id,
        client_order_id=client_order_id,
        cumulative_quantity=cumulative_qty,
        average_price=avg_price,
        fee_quote=fee,
        state=state,
        updated_at_ms=updated_at_ms,
        last_fill_at_ms=last_fill_at_ms,
        source="reconciliation" if any(s.source == "reconciliation" for s in highest)
        else "rest_snapshot" if any(s.source == "rest_snapshot" for s in highest)
        else "private_ws",
    )


def merge_passive_progress_sources(
    detail_progress: CumulativeOrderProgress,
    reconciliation,
    private_progress: Optional[CumulativeOrderProgress],
) -> CumulativeOrderProgress:
    """V1 merge_passive_progress_sources (passive_progress.rs:6).

    Merge priority: reconciliation > REST detail > private WS.
    """
    recon_progress = None
    if reconciliation is not None:
        recon_progress = CumulativeOrderProgress.from_reconciliation(reconciliation)
    all_sources = []
    if recon_progress is not None:
        all_sources.append(recon_progress)
    all_sources.append(detail_progress)
    if private_progress is not None:
        all_sources.append(private_progress)
    return resolve_cumulative_order_progress(all_sources) or detail_progress


def should_fetch_passive_reconciliation(
    detail_progress: CumulativeOrderProgress,
    private_progress: Optional[CumulativeOrderProgress] = None,
) -> bool:
    """V1 should_fetch_passive_reconciliation (passive_progress.rs:47).

    Fetch reconciliation when any source has fill quantity or order identity.
    """
    if detail_progress.cumulative_quantity > 0.0:
        return True
    if detail_progress.order_id is not None or detail_progress.client_order_id is not None:
        return True
    if private_progress is not None:
        if private_progress.cumulative_quantity > 0.0:
            return True
        if private_progress.order_id is not None or private_progress.client_order_id is not None:
            return True
    return False


# ---------------------------------------------------------------------------
# Fill enrichment (V1 enrich_fill_from_private)
# ---------------------------------------------------------------------------


def enrich_fill_from_private(
    fill: OrderFill, update: PrivateOrderUpdate
) -> OrderFill:
    """V1 enrich_fill_from_private() — overwrite fill fields from private data."""
    if (
        update.filled_quantity is not None
        and update.filled_quantity > 0.0
        and _is_finite(update.filled_quantity)
    ):
        fill = OrderFill(
            venue=fill.venue,
            symbol=fill.symbol,
            side=fill.side,
            quantity=update.filled_quantity,
            price=(
                update.average_price
                if update.average_price is not None
                and update.average_price > 0.0
                and _is_finite(update.average_price)
                else fill.price
            ),
            order_id=update.order_id if update.order_id else fill.order_id,
            client_order_id=fill.client_order_id,
            fee_quote=(
                update.fee_quote
                if update.fee_quote is not None
                and update.fee_quote >= 0.0
                and _is_finite(update.fee_quote)
                else fill.fee_quote
            ),
            filled_at_ms=(
                update.updated_at_ms
                if update.updated_at_ms > 0
                else fill.filled_at_ms
            ),
        )
    return fill


# ---------------------------------------------------------------------------
# Async wait primitives (V1 lookup_or_wait_*)
# ---------------------------------------------------------------------------


async def lookup_or_wait_private_order(
    state: PrivateWsState,
    client_order_id: Optional[str] = None,
    order_id: Optional[str] = None,
    wait_ms: int = 0,
) -> Optional[PrivateOrderUpdate]:
    """V1 lookup_or_wait_private_order()."""
    return await _lookup_or_wait_private_order_where(
        state, client_order_id, order_id, wait_ms, lambda _: True
    )


async def lookup_or_wait_private_order_progress(
    state: PrivateWsState,
    client_order_id: Optional[str] = None,
    order_id: Optional[str] = None,
    wait_ms: int = 0,
) -> Optional[CumulativeOrderProgress]:
    """V1 lookup_or_wait_private_order_progress()."""
    update = await lookup_or_wait_private_order(
        state, client_order_id, order_id, wait_ms
    )
    if update is None:
        return None
    return CumulativeOrderProgress.from_private(update)


async def lookup_or_wait_private_order_progress_after(
    state: PrivateWsState,
    client_order_id: Optional[str] = None,
    order_id: Optional[str] = None,
    after_updated_at_ms: int = 0,
    wait_ms: int = 0,
) -> Optional[CumulativeOrderProgress]:
    """V1 lookup_or_wait_private_order_progress_after()."""
    update = await _lookup_or_wait_private_order_where(
        state,
        client_order_id,
        order_id,
        wait_ms,
        lambda u: u.updated_at_ms > after_updated_at_ms,
    )
    if update is None:
        return None
    return CumulativeOrderProgress.from_private(update)


async def _lookup_or_wait_private_order_where(
    state: PrivateWsState,
    client_order_id: Optional[str],
    order_id: Optional[str],
    wait_ms: int,
    predicate,
) -> Optional[PrivateOrderUpdate]:
    """V1 lookup_or_wait_private_order_where() inner loop."""

    def _lookup() -> Optional[PrivateOrderUpdate]:
        update = None
        if client_order_id:
            update = state.order_by_client_id(client_order_id)
        if update is None and order_id:
            update = state.order_by_order_id(order_id)
        if update is not None and predicate(update):
            return update
        return None

    result = _lookup()
    if result is not None:
        return result
    if wait_ms <= 0:
        return None

    deadline = _now_ms() + wait_ms
    while True:
        now = _now_ms()
        if now >= deadline:
            return _lookup()
        remaining = deadline - now
        before_version = state._order_update_version
        woke = await state._wait_for_update(before_version, int(remaining))
        if not woke:
            return _lookup()
        result = _lookup()
        if result is not None:
            return result


# ---------------------------------------------------------------------------
# PrivateWsClientState / PrivateWsEvent / PrivateWsEventKind (test compatibility)
#
# These are lightweight compatibility types used by tests/test_ws_resilience.py
# to exercise private-stream health and fill buffering without a full WS worker.
# They are NOT used in production — production uses PrivateWsState + venue workers.
# ---------------------------------------------------------------------------

from enum import Enum


class PrivateWsEventKind(Enum):
    """Event kind discriminator for private WS events (test compatibility)."""
    ORDER_ACK = "order_ack"
    ORDER_FILL = "order_fill"
    ORDER_CANCEL = "order_cancel"
    ORDER_REJECT = "order_reject"
    POSITION_UPDATE = "position_update"


@dataclass
class PrivateWsEvent:
    """Single private WS event (test + reconciliation compatibility)."""
    venue: Venue = Venue.BINANCE
    kind: PrivateWsEventKind = PrivateWsEventKind.ORDER_FILL
    symbol: str = ""
    order_id: str = ""
    client_order_id: Optional[str] = None
    side: Side = Side.BUY
    quantity: float = 0.0
    price: float = 0.0
    fee_quote: float = 0.0
    observed_at_ms: int = 0

    def to_order_fill(self) -> Optional[OrderFill]:
        """Convert to OrderFill; returns None for non-fill events."""
        if self.kind != PrivateWsEventKind.ORDER_FILL:
            return None
        if self.quantity <= 0:
            return None
        return OrderFill(
            venue=self.venue,
            symbol=self.symbol,
            side=self.side,
            quantity=self.quantity,
            price=self.price,
            fee_quote=self.fee_quote,
            order_id=self.order_id,
            filled_at_ms=self.observed_at_ms,
        )


class PrivateWsClientState:
    """Per-venue private WS client state (test + reconciliation compatibility).

    Owns a WsStreamState for health tracking and a pending reconciliation
    buffer for fill events that need to be matched against open order state.
    """

    def __init__(self, venue: Venue = Venue.BINANCE) -> None:
        from lightfee.marketdata.ws import WsStreamState
        self.venue = venue
        self.stream = WsStreamState()
        self.position_confirmed: bool = False
        self.last_position_update_ms: int = 0
        self.pending_reconciliation: list[PrivateWsEvent] = []

    def on_position_confirmed(self, now_ms: int) -> None:
        self.position_confirmed = True
        self.last_position_update_ms = now_ms

    def on_fill_event(self, event: PrivateWsEvent) -> None:
        self.pending_reconciliation.append(event)

    def drain_reconciliation_events(self) -> list[PrivateWsEvent]:
        events = self.pending_reconciliation[:]
        self.pending_reconciliation.clear()
        return events

    def is_healthy(self) -> bool:
        return self.stream.is_healthy()
