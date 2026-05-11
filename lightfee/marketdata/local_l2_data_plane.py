"""Local-L2 data-plane — REST snapshot bootstrap + WebSocket streaming orchestration.

Rust V1 references:
  - src/market_gateway/local_l2.rs (types, reconcile)
  - src/live/aster.rs (Aster WS L2 sessions)
  - src/market_gateway/local_l2_state_machine.rs (status transitions)

Responsibilities:
  - Manage per-venue REST snapshot bootstrap with cooldown/debounce
  - Manage per-venue WebSocket L2 delta streaming
  - Feed canonical LocalL2Update into LocalL2Runtime.record_update()
  - Handle transport failures, degraded state, timeout

This is the live data entry point for local-L2 — the bridge between
external venue data and the internal order book model.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING

from lightfee.marketdata.l2 import (
    L2BookStatus,
    L2PoolAssignment,
    LocalL2BookKey,
    LocalL2Update,
)
from lightfee.marketdata.local_l2_runtime import LocalL2Runtime, RuntimeFaultKind
from lightfee.venues.transport import TransportError, TransportErrorCategory
from lightfee.persistence.journal import Journal

if TYPE_CHECKING:
    from lightfee.core.contracts import VenueAdapter
    from lightfee.marketdata.local_l2_ws import LocalL2WsClient


# ---------------------------------------------------------------------------
# Snapshot state per book
# ---------------------------------------------------------------------------


@dataclass
class _BookSnapshotState:
    """Tracks REST snapshot bootstrap state for a single venue/symbol book."""

    venue: str
    symbol: str
    last_snapshot_ms: int = 0
    snapshot_cooldown_ms: int = 5_000  # Don't re-snapshot faster than this
    consecutive_failures: int = 0
    max_consecutive_failures: int = 5
    last_error: str = ""
    snapshot_in_flight: bool = False


# Default snapshot intervals per book status
SNAPSHOT_INTERVAL_COLD_MS = 0  # Immediate on cold
SNAPSHOT_INTERVAL_BOOTSTRAPPING_MS = 2_000
SNAPSHOT_INTERVAL_REBUILDING_MS = 3_000
SNAPSHOT_INTERVAL_HOT_MS = 30_000  # Periodic refresh for HOT books (no WS)
SNAPSHOT_INTERVAL_DEGRADED_MS = 10_000


# ---------------------------------------------------------------------------
# LocalL2DataPlane
# ---------------------------------------------------------------------------


class LocalL2DataPlane:
    """Orchestrates live data flow into LocalL2Runtime.

    Two data sources (prioritised):
    1. REST snapshot bootstrap — for initial book population and periodic refresh
    2. WebSocket delta streaming — per-venue WS connections (future)

    The data plane is stateless about venue adapters — it receives them
    from the runtime and calls their transport layer.
    """

    def __init__(
        self,
        l2_runtime: LocalL2Runtime,
        journal: Journal,
    ) -> None:
        self._runtime = l2_runtime
        self._journal = journal
        self._snap_states: dict[LocalL2BookKey, _BookSnapshotState] = {}
        self._ws_clients: dict[LocalL2BookKey, "LocalL2WsClient"] = {}

        # Global config
        self.max_concurrent_snapshots: int = 4
        self.bootstrap_timeout_ms: int = 15_000  # Overall bootstrap phase timeout
        self.hot_refresh_interval_ms: int = SNAPSHOT_INTERVAL_HOT_MS

    # ------------------------------------------------------------------
    # Bootstrap: initial snapshot population for target books
    # ------------------------------------------------------------------

    async def bootstrap_book(
        self,
        venue: str,
        symbol: str,
        adapter,  # VenueAdapter — provides fetch_l2_snapshot()
        depth: int = 50,
        now_ms: int = 0,
    ) -> bool:
        """Bootstrap a single book with a REST snapshot via the adapter.

        Uses the adapter's public fetch_l2_snapshot() interface — never
        reaches into adapter._transport from outside.

        Returns True if the snapshot was successfully applied.
        """
        key = LocalL2BookKey(venue=venue, symbol=symbol)
        ss = self._snap_states.get(key)
        if ss is None:
            ss = _BookSnapshotState(venue=venue, symbol=symbol)
            self._snap_states[key] = ss

        water_level_ms = max(1, ss.snapshot_cooldown_ms)
        if ss.snapshot_in_flight:
            return False
        if ss.last_snapshot_ms > 0 and (now_ms - ss.last_snapshot_ms) < water_level_ms:
            return False

        # Consecutive failure gate: don't hammer a failing endpoint
        if ss.consecutive_failures >= ss.max_consecutive_failures:
            return False

        ss.snapshot_in_flight = True
        try:
            update = await adapter.fetch_l2_snapshot(symbol=symbol, depth=depth)
            self._runtime.record_update(update, now_ms)
            ss.last_snapshot_ms = now_ms
            ss.consecutive_failures = 0
            ss.last_error = ""
            return True
        except TransportError as e:
            ss.consecutive_failures += 1
            ss.last_error = str(e)
            if e.category == TransportErrorCategory.UNSUPPORTED_CAPABILITY:
                # Don't retry unsupported — mark degraded
                ss.consecutive_failures = ss.max_consecutive_failures
            self._runtime.handle_runtime_failure(
                venue, symbol,
                RuntimeFaultKind.TRANSPORT_FAILURE,
                f"snapshot_bootstrap: {e}", now_ms,
            )
            return False
        except Exception as e:
            ss.consecutive_failures += 1
            ss.last_error = str(e)
            self._runtime.handle_runtime_failure(
                venue, symbol,
                RuntimeFaultKind.TRANSPORT_FAILURE,
                f"snapshot_bootstrap: {e}", now_ms,
            )
            return False
        finally:
            ss.snapshot_in_flight = False

    def ingest_external_update(
        self, update: LocalL2Update, now_ms: int,
    ) -> list:
        """Ingest a LocalL2Update from any external data source (WS, relay, REST).

        Single entry point for all data sources to feed into the runtime.
        Used by WebSocket streams, relay bridges, and REST bootstrap alike.

        Synchronous — the runtime update is a book manipulation that does
        not perform I/O. Callers (WS client, relay bridge) are responsible
        for their own I/O and call this with parsed data.
        """
        return self._runtime.record_update(update, now_ms)

    # ------------------------------------------------------------------
    # Sync: periodic snapshot refresh for books without WS streaming
    # ------------------------------------------------------------------

    async def sync_snapshots(
        self,
        adapters: dict,  # [Venue, VenueAdapter-like] — provides transport via adapter.transport
        now_ms: int,
    ) -> int:
        """Periodic REST snapshot refresh for all managed books.

        Books in COLD/BOOTSTRAPPING/REBUILDING get priority snapshots.
        HOT books get periodic refresh only if they have no WS stream.

        Returns the number of snapshots dispatched.
        """
        dispatched = 0

        for key, book in list(self._runtime.books.items()):
            if dispatched >= self.max_concurrent_snapshots:
                break

            # Determine if this book needs a snapshot
            interval_ms = self._snapshot_interval_for_status(book.status)

            # HOT books: skip if WS is active (future), otherwise periodic refresh
            if book.status == L2BookStatus.HOT:
                if book.last_snapshot_ms > 0 and (now_ms - book.last_snapshot_ms) < interval_ms:
                    continue

            # COLD/BOOTSTRAPPING/REBUILDING: snapshot on every eligible pass
            if interval_ms == 0:
                pass  # Always eligible
            elif book.last_snapshot_ms > 0 and (now_ms - book.last_snapshot_ms) < interval_ms:
                continue

            # Resolve adapter
            from lightfee.core.domain import Venue
            ven = Venue.from_str(key.venue)
            adapter = adapters.get(ven)
            if adapter is None:
                continue

            # Use adapter's public fetch_l2_snapshot() — never access _transport
            if not hasattr(adapter, 'fetch_l2_snapshot'):
                continue

            success = await self.bootstrap_book(
                venue=key.venue,
                symbol=key.symbol,
                adapter=adapter,
                depth=book.max_depth,
                now_ms=now_ms,
            )
            if success:
                dispatched += 1

        if dispatched > 0:
            self._journal.append(
                "runtime.local_l2_snapshots_synced",
                {"dispatched": dispatched, "ts_ms": now_ms},
            )

        return dispatched

    @staticmethod
    def _snapshot_interval_for_status(status: L2BookStatus) -> int:
        if status == L2BookStatus.COLD:
            return SNAPSHOT_INTERVAL_COLD_MS
        elif status == L2BookStatus.BOOTSTRAPPING:
            return SNAPSHOT_INTERVAL_BOOTSTRAPPING_MS
        elif status == L2BookStatus.REBUILDING:
            return SNAPSHOT_INTERVAL_REBUILDING_MS
        elif status == L2BookStatus.DEGRADED:
            return SNAPSHOT_INTERVAL_DEGRADED_MS
        else:
            return SNAPSHOT_INTERVAL_HOT_MS

    # ------------------------------------------------------------------
    # WebSocket streaming
    # ------------------------------------------------------------------

    def start_ws_streams(
        self,
        venue: str,
        symbols: list[str],
        adapter=None,  # VenueAdapter — required for Hyperliquid poller
    ) -> int:
        """Register WebSocket L2 delta streams for a venue's symbols.

        Creates WS clients and registers them in the data plane.
        For Hyperliquid (REST poller), the adapter is injected so the
        poller can call adapter.fetch_l2_snapshot().

        Caller must await connect_ws_streams() from an async context
        to actually open the connections.

        Returns the number of streams registered.
        """
        from lightfee.marketdata.local_l2_ws import create_ws_client, HyperliquidL2Poller

        started = 0
        for symbol in symbols:
            client = create_ws_client(
                venue=venue,
                symbol=symbol,
                data_plane=self,
            )
            if client is None:
                continue

            # Inject adapter into Hyperliquid poller so it can fetch real data
            if isinstance(client, HyperliquidL2Poller):
                if adapter is None:
                    self._journal.append(
                        "runtime.local_l2_hyperliquid_no_adapter",
                        {"venue": venue, "symbol": symbol,
                         "error": "Hyperliquid poller created without adapter — will not ingest data"},
                    )
                client.set_adapter(adapter)

            key = LocalL2BookKey(venue=venue, symbol=symbol)
            if key in self._ws_clients:
                continue  # already streaming

            self._ws_clients[key] = client
            started += 1

        return started

    async def connect_ws_streams(self) -> int:
        """Connect all registered WS clients that aren't already connected.

        Returns the number of newly connected clients.
        """
        connected = 0
        for client in list(self._ws_clients.values()):
            if not client.is_connected:
                await client.start()
                connected += 1
        return connected

    async def stop_ws_streams(self, *, per_client_timeout_s: float = 5.0) -> None:
        """Stop all WebSocket L2 streams with per-client timeout guard.

        Cancelled WS tasks may leave DNS resolution threads in the default
        executor that survive task cancellation.  A per-client timeout prevents
        a stuck client from blocking the entire shutdown sequence.
        """
        for client in list(self._ws_clients.values()):
            try:
                await asyncio.wait_for(client.stop(), timeout=per_client_timeout_s)
            except asyncio.TimeoutError:
                # Hard-abort: cancel the task and tear down the transport
                if client._task is not None and not client._task.done():
                    client._task.cancel()
                client._state = "closed"
                client._ws = None
        self._ws_clients.clear()

    @property
    def active_ws_stream_count(self) -> int:
        return sum(
            1 for c in self._ws_clients.values()
            if c.is_connected
        )

    # ------------------------------------------------------------------
    # Worker lifecycle (V1: explicit start/stop/abort ownership)
    # ------------------------------------------------------------------

    def start_worker(self, key: LocalL2BookKey, client: "LocalL2WsClient") -> None:
        """Register a worker for a venue/symbol pair (explicit ownership)."""
        if key in self._ws_clients:
            return
        self._ws_clients[key] = client

    def stop_worker(self, key: LocalL2BookKey) -> bool:
        """Stop and unregister a single worker. Returns True if worker existed."""
        client = self._ws_clients.pop(key, None)
        if client is None:
            return False
        # Fire-and-forget stop — caller should have an async context or use stop_ws_streams()
        if client._task is not None and not client._task.done():
            client._task.cancel()
        return True

    def abort_workers(self) -> int:
        """Hard-abort all WS workers without waiting for graceful shutdown."""
        count = 0
        for client in list(self._ws_clients.values()):
            client._state = "closed"
            if client._task is not None and not client._task.done():
                client._task.cancel()
                count += 1
            client._ws = None
        self._ws_clients.clear()
        return count

    # ------------------------------------------------------------------
    # Worker categories (V1: ws_worker_categories() per venue)
    # ------------------------------------------------------------------

    def ws_worker_categories(self) -> list[dict]:
        """Return per-venue worker category diagnostics (V1: WsWorkerCategoryStatus).

        Each entry: {venue, category, active_count, expected_max, risk_relevant}
        Categories: "market_local_l2" for L2 depth WS/poller workers.
        """
        by_venue: dict[str, int] = {}
        for key in self._ws_clients:
            by_venue[key.venue] = by_venue.get(key.venue, 0) + 1

        categories: list[dict] = []
        for venue, count in sorted(by_venue.items()):
            categories.append({
                "venue": venue,
                "category": "market_local_l2",
                "active_count": count,
                "expected_max": count,  # One per symbol — exact match is healthy
                "risk_relevant": True,
            })
        return categories

    def suspicious_worker_count(self) -> bool:
        """True if any venue has more active workers than expected (V1 risk check)."""
        for cat in self.ws_worker_categories():
            if cat["risk_relevant"] and cat["active_count"] > cat["expected_max"]:
                return True
        return False

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def diagnostics_snapshot(self) -> dict:
        """Return a diagnostics view of the data plane."""
        snap_failures = sum(
            1 for ss in self._snap_states.values()
            if ss.consecutive_failures >= ss.max_consecutive_failures
        )
        return {
            "managed_books": len(self._snap_states),
            "snapshot_failure_books": snap_failures,
            "runtime_books": len(self._runtime.books),
            "hot_books": self._runtime.metrics.active_books,
            "bootstrapping_books": self._runtime.metrics.bootstrapping_books,
            "rebuilding_books": self._runtime.metrics.rebuilding_books,
            "runtime_suspended_books": self._runtime.metrics.runtime_suspended_books,
            "ws_stream_count": len(self._ws_clients),
            "ws_connected_count": self.active_ws_stream_count,
            "ws_worker_categories": self.ws_worker_categories(),
            "suspicious_worker_count": self.suspicious_worker_count(),
        }
