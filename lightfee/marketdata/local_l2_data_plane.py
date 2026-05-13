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
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING

from lightfee.marketdata.l2 import (
    L2BookStatus,
    L2PoolAssignment,
    LocalL2BookKey,
    LocalL2Update,
    LocalL2UpdateKind,
)
from lightfee.marketdata.local_l2_runtime import LocalL2Runtime, RuntimeFaultKind
from lightfee.venues.transport import TransportError, TransportErrorCategory
from lightfee.persistence.journal import Journal

if TYPE_CHECKING:
    from lightfee.core.contracts import VenueAdapter
    from lightfee.marketdata.local_l2_ws import LocalL2WsClient

# Pre-snapshot buffer capacity per symbol (V1: BINANCE_LOCAL_L2_PRE_SNAPSHOT_BUFFER_CAP)
_PRE_SNAPSHOT_BUFFER_CAP = 512


# ---------------------------------------------------------------------------
# Buffered WS update wrapper (V1: BufferedBinanceLocalL2DepthUpdate)
# ---------------------------------------------------------------------------


@dataclass
class _BufferedUpdate:
    """Wrapper tagging a WS delta with stream generation for stale-filtering.

    V1: BufferedBinanceLocalL2DepthUpdate { generation, observed_at_ms, update }
    """

    generation: int
    observed_at_ms: int
    update: LocalL2Update


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

        # Pre-snapshot buffers: keyed by "venue:symbol" → deque of _BufferedUpdate
        # V1: binance_local_l2_pre_snapshot_buffers()
        self._pre_snapshot_buffers: dict[str, deque[_BufferedUpdate]] = {}

        # Stream generations: keyed by "venue:symbol" → generation counter (starts at 1)
        # V1: binance_local_l2_stream_generations() — advanced on WS reconnect and
        # bootstrap start to invalidate buffered updates from old streams.
        self._stream_generations: dict[str, int] = {}

        # Background bootstrap worker tasks: keyed by "venue"
        self._bootstrap_tasks: dict[str, asyncio.Task] = {}

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

            # V1: binance_local_l2_snapshot_is_stale — reject snapshot if sequence <= book's
            book = self._runtime.get_book(venue, symbol)
            if book is not None and update.sequence > 0 and book.last_update_id > 0:
                if update.sequence <= book.last_update_id:
                    self._journal.append(
                        "runtime.local_l2_snapshot_stale",
                        {"venue": venue, "symbol": symbol,
                         "snapshot_seq": update.sequence, "book_seq": book.last_update_id},
                    )
                    # Small delay to avoid tight loop re-fetching the same stale snapshot
                    await asyncio.sleep(0.25)
                    return False

            self._runtime.record_update(update, now_ms)

            # Replay buffered WS updates accumulated during bootstrap gap (V1 parity)
            replayed = self._replay_buffered_updates(venue, symbol)
            if replayed > 0:
                self._journal.append(
                    "runtime.local_l2_buffered_replay",
                    {"venue": venue, "symbol": symbol, "replayed": replayed},
                )

            # Mark book HOT after snapshot + buffered replay (V1: complete_bootstrap)
            book = self._runtime.get_book(venue, symbol)
            if book is not None and book.status == L2BookStatus.BOOTSTRAPPING:
                book.transition_to_hot()

            ss.last_snapshot_ms = now_ms
            ss.consecutive_failures = 0
            ss.last_error = ""
            self._journal.append(
                "runtime.local_l2_snapshot_ok",
                {"venue": venue, "symbol": symbol},
            )
            return True
        except TransportError as e:
            ss.consecutive_failures += 1
            ss.last_error = str(e)
            if e.category == TransportErrorCategory.UNSUPPORTED_CAPABILITY:
                # Don't retry unsupported — mark degraded
                ss.consecutive_failures = ss.max_consecutive_failures
            # V1: clear pre-snapshot buffers on failure (clear_binance_local_l2_depth_updates_for_instance)
            buf_key = f"{venue}:{symbol}"
            self._pre_snapshot_buffers.pop(buf_key, None)
            self._runtime.handle_runtime_failure(
                venue, symbol,
                RuntimeFaultKind.TRANSPORT_FAILURE,
                f"snapshot_bootstrap: {e}", now_ms,
            )
            self._journal.append(
                "runtime.local_l2_snapshot_error",
                {"venue": venue, "symbol": symbol, "error": str(e), "category": str(e.category)},
            )
            return False
        except Exception as e:
            ss.consecutive_failures += 1
            ss.last_error = str(e)
            # V1: clear pre-snapshot buffers on any failure
            buf_key = f"{venue}:{symbol}"
            self._pre_snapshot_buffers.pop(buf_key, None)
            self._runtime.handle_runtime_failure(
                venue, symbol,
                RuntimeFaultKind.TRANSPORT_FAILURE,
                f"snapshot_bootstrap: {e}", now_ms,
            )
            self._journal.append(
                "runtime.local_l2_snapshot_error",
                {"venue": venue, "symbol": symbol, "error": str(e)},
            )
            return False
        finally:
            ss.snapshot_in_flight = False

    # ------------------------------------------------------------------
    # Stream generation (V1: binance_local_l2_stream_generations)
    # ------------------------------------------------------------------

    def _current_stream_generation(self, venue: str, symbol: str) -> int:
        """Return current generation for a venue/symbol stream, initialising if needed.

        V1: current_binance_local_l2_stream_generation()
        """
        key = f"{venue}:{symbol}"
        gen = self._stream_generations.get(key)
        if gen is None:
            gen = 1
            self._stream_generations[key] = gen
        return gen

    def _advance_stream_generation(self, venue: str, symbol: str) -> int:
        """Advance and return the stream generation for a venue/symbol.

        V1: advance_binance_local_l2_stream_generation()
        """
        key = f"{venue}:{symbol}"
        gen = self._stream_generations.get(key, 0) + 1
        self._stream_generations[key] = gen
        return gen

    def reset_stream_state(self, venue: str, symbols: list[str]) -> None:
        """Reset stream state for bootstrapping/rebuilding books.

        V1: reset_binance_local_l2_bootstrap_stream_state_for_instance()
        Called on WS reconnect and bootstrap start to invalidate buffered
        updates from old streams and reset sequence tracking.

        For each symbol that is BOOTSTRAPPING or REBUILDING:
          - Advance stream generation (old buffered updates filtered out on replay)
          - Clear pre-snapshot buffer
          - Reset book sequence so a fresh snapshot isn't rejected as stale
        """
        for symbol in symbols:
            key = f"{venue}:{symbol}"
            self._advance_stream_generation(venue, symbol)

            book = self._runtime.get_book(venue, symbol)
            if book is None:
                continue
            if book.status not in (L2BookStatus.BOOTSTRAPPING, L2BookStatus.REBUILDING):
                continue

            # Clear buffered updates from old stream (V1: clear_..._for_instance)
            self._pre_snapshot_buffers.pop(key, None)

            # Reset sequence so snapshot isn't treated as stale (V1: set_last_sequence(None))
            book.sequence = 0
            book.last_update_id = 0

    # ------------------------------------------------------------------
    # Ingest: WS/REST update entry point
    # ------------------------------------------------------------------

    def ingest_external_update(
        self, update: LocalL2Update, now_ms: int,
    ) -> list:
        """Ingest a LocalL2Update from any external data source (WS, relay, REST).

        V1 parity: when the target book is BOOTSTRAPPING or REBUILDING, buffer
        delta updates tagged with the current stream generation. Buffered updates
        are replayed after the REST snapshot completes (see bootstrap_book).

        Single entry point for all data sources to feed into the runtime.
        """
        key = f"{update.venue}:{update.symbol}"
        book = self._runtime.get_book(update.venue, update.symbol)

        # Buffer delta updates during bootstrap/rebuild gap
        # V1: handle_binance_local_l2_ws_message_for_instance lines 4423-4435
        if book is not None and book.status in (L2BookStatus.BOOTSTRAPPING, L2BookStatus.REBUILDING):
            buf = self._pre_snapshot_buffers.get(key)
            if buf is None:
                buf = deque()
                self._pre_snapshot_buffers[key] = buf
            if len(buf) >= _PRE_SNAPSHOT_BUFFER_CAP:
                # Overflow → rebuild required (V1: mark_status(Rebuilding), set_last_sequence(None))
                buf.clear()
                book.sequence = 0
                book.last_update_id = 0
                book.transition_to_rebuilding(now_ms)
                self._runtime.handle_runtime_failure(
                    update.venue, update.symbol,
                    RuntimeFaultKind.SEQUENCE_GAP,
                    "pre_snapshot_buffer_overflow", now_ms,
                )
                return []
            gen = self._current_stream_generation(update.venue, update.symbol)
            buf.append(_BufferedUpdate(generation=gen, observed_at_ms=now_ms, update=update))
            return []

        # If there are leftover buffered updates (edge case: book became HOT while
        # buffered updates were still pending), replay them first.
        if book is not None and self._pre_snapshot_buffers.get(key):
            self._replay_buffered_updates(update.venue, update.symbol)

        return self._runtime.record_update(update, now_ms)

    def _replay_buffered_updates(self, venue: str, symbol: str) -> int:
        """Replay buffered WS updates accumulated during bootstrap gap.

        V1: replay_binance_buffered_local_l2_depth_updates_for_instance()
        - Filters out updates from old stream generations
        - Skips updates already covered by the snapshot (sequence <= book.sequence)
        - Detects sequence gaps that require a rebuild
        - Returns number of updates replayed (0 if buffer empty or all filtered)
        """
        key = f"{venue}:{symbol}"
        buf = self._pre_snapshot_buffers.pop(key, None)
        if not buf:
            return 0

        book = self._runtime.get_book(venue, symbol)
        if book is None:
            return 0

        current_gen = self._current_stream_generation(venue, symbol)
        previous_sequence = book.sequence  # snapshot's sequence

        # Filter: keep only current-generation updates (V1: retain by generation)
        # Convert to list for indexed access
        filtered: list[_BufferedUpdate] = [
            b for b in buf if b.generation == current_gen
        ]
        if not filtered:
            return 0

        # Drop updates already covered by the snapshot (V1: final_update_id <= previous_sequence)
        while filtered and filtered[0].update.sequence <= previous_sequence:
            filtered.pop(0)
        if not filtered:
            return 0

        # Find first update that bridges snapshot to live stream
        # V1: first_update_id <= expected <= final_update_id
        expected = previous_sequence + 1
        start_index = None
        for i, bu in enumerate(filtered):
            first_id = bu.update.previous_sequence + 1 if bu.update.previous_sequence > 0 else bu.update.sequence
            if first_id <= expected <= bu.update.sequence:
                start_index = i
                break

        if start_index is None:
            # No overlap — gap between snapshot and buffered updates
            book.sequence = 0
            book.last_update_id = 0
            book.transition_to_rebuilding(int(asyncio.get_event_loop().time() * 1000))
            self._runtime.handle_runtime_failure(
                venue, symbol, RuntimeFaultKind.SEQUENCE_GAP,
                "buffered_replay_snapshot_boundary: no overlapping update", 0,
            )
            return 0

        # Replay from overlap point with continuity check
        # V1: buffered_updates.into_iter().skip(start_index).enumerate()
        replayed = 0
        for i, bu in enumerate(filtered[start_index:], start=start_index):
            if bu.update.sequence <= previous_sequence:
                continue

            if i > start_index:
                # Check continuity between consecutive buffered updates
                if bu.update.previous_sequence > 0 and bu.update.previous_sequence != previous_sequence:
                    book.sequence = 0
                    book.last_update_id = 0
                    book.transition_to_rebuilding(int(asyncio.get_event_loop().time() * 1000))
                    self._runtime.handle_runtime_failure(
                        venue, symbol, RuntimeFaultKind.SEQUENCE_GAP,
                        f"buffered_replay_previous_link_mismatch: expected {previous_sequence} got {bu.update.previous_sequence}", 0,
                    )
                    return replayed
            elif bu.update.previous_sequence > 0 and expected < bu.update.previous_sequence + 1:
                # First replay: gap between snapshot and first buffered
                book.sequence = 0
                book.last_update_id = 0
                book.transition_to_rebuilding(int(asyncio.get_event_loop().time() * 1000))
                self._runtime.handle_runtime_failure(
                    venue, symbol, RuntimeFaultKind.SEQUENCE_GAP,
                    f"buffered_replay_snapshot_boundary: expected {expected} got {bu.update.previous_sequence + 1}", 0,
                )
                return replayed

            try:
                self._runtime.record_update(bu.update, bu.observed_at_ms)
                previous_sequence = bu.update.sequence
                replayed += 1
            except Exception:
                book.sequence = 0
                book.last_update_id = 0
                book.transition_to_rebuilding(int(asyncio.get_event_loop().time() * 1000))
                self._runtime.handle_runtime_failure(
                    venue, symbol, RuntimeFaultKind.SEQUENCE_GAP,
                    f"buffered_replay_apply_failed at index {i}", 0,
                )
                return replayed

        return replayed

    # ------------------------------------------------------------------
    # Sync: periodic snapshot refresh for books without WS streaming
    # ------------------------------------------------------------------

    async def sync_snapshots(
        self,
        adapters: dict,
        now_ms: int,
        *,
        scan_promoted: bool = False,
    ) -> int:
        """Periodic REST snapshot refresh — only for books without active WS stream.

        V1: REST snapshots are ONLY for bootstrap. After HOT, WS deltas maintain
        the book.  This poller exists only as a fallback for books that lost their
        WS stream (DEGRADED/REBUILDING) or never had one.

        scan_promoted=True (post-shortlist) allows refreshing books promoted by the
        scan phase; False (pre-scan) refreshes execution-owned books only.
        """
        dispatched = 0

        for key, book in list(self._runtime.books.items()):
            if dispatched >= self.max_concurrent_snapshots:
                break

            # V1: only snapshot books that need recovery — HOT books rely on WS deltas
            if book.status == L2BookStatus.HOT:
                continue

            # V1 dual-phase gating: pre-scan only refreshes execution-owned books
            # (RETAINED or HOT_EXEC); post-shortlist allows scan-promoted books too
            if not scan_promoted and book.pool not in (L2PoolAssignment.RETAINED, L2PoolAssignment.HOT_EXEC):
                continue

            interval_ms = self._snapshot_interval_for_status(book.status)
            if interval_ms > 0 and book.last_snapshot_ms > 0:
                if (now_ms - book.last_snapshot_ms) < interval_ms:
                    continue

            from lightfee.core.domain import Venue
            ven = Venue.from_str(key.venue)
            adapter = adapters.get(ven)
            if adapter is None:
                continue
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

    def start_background_bootstrap(
        self,
        venue: str,
        symbols: list[str],
        adapter,  # VenueAdapter
        *,
        batch_size: int = 4,
        jitter_ms: int = 250,
        retry_backoff_ms: int = 5000,
    ) -> None:
        """Spawn a background bootstrap worker for a single venue.

        V1 parity: spawn_binance_local_l2_bootstrap_worker().
        The worker iterates symbols with batch concurrency, fetches REST
        snapshots, and applies them through bootstrap_book() which handles
        buffered WS replay and transition to HOT.

        Does NOT block — the worker runs as a background asyncio task.
        Call cancel_background_bootstrap(venue) to abort.
        """
        # Cancel any existing bootstrap task for this venue
        self.cancel_background_bootstrap(venue)

        # Reset stream state: advance generation, clear old buffers, reset sequences
        # V1: start_local_l2_bootstrap_at → reset_binance_local_l2_bootstrap_stream_state_for_instance
        # A rebuild bootstrap must drop any previous snapshot sequence so a fresh
        # depth snapshot with the same exchange sequence is not treated as stale.
        self.reset_stream_state(venue, symbols)

        async def _bootstrap_worker() -> None:
            import random
            sem = asyncio.Semaphore(max(1, batch_size))

            async def _bootstrap_one(index: int, symbol: str) -> None:
                # Jittered initial delay to stagger requests (V1: binance_local_l2_bootstrap_initial_delay_ms)
                delay_ms = jitter_ms * (index % batch_size) // batch_size
                if delay_ms > 0:
                    await asyncio.sleep(delay_ms / 1000.0)

                book = self._runtime.get_book(venue, symbol)
                if book is None:
                    return
                # V1: never skip based on status — every status reaches the
                # snapshot fetch. Blocking is only for the resume_without_bootstrap
                # window, which V2 handles via RESUME_WAITING status.
                # Skip only HOT (already bootstrapped) and SUSPENDED (paused).
                if book.status in (L2BookStatus.HOT, L2BookStatus.SUSPENDED):
                    return

                backoff = retry_backoff_ms
                max_backoff = retry_backoff_ms * 8
                while True:  # V1: loop {} — infinite retry until success
                    now_ms = int(asyncio.get_event_loop().time() * 1000)
                    book = self._runtime.get_book(venue, symbol)

                    # V1: resume_without_bootstrap inline polling (250ms slices)
                    if book is not None:
                        remaining = book.resume_waiting_remaining_ms(now_ms)
                        if remaining > 0:
                            await asyncio.sleep(min(remaining, 250) / 1000.0)
                            continue

                    if book is None or book.status == L2BookStatus.HOT:
                        return

                    if book.status not in (L2BookStatus.BOOTSTRAPPING, L2BookStatus.DEGRADED):
                        book.transition_to_bootstrapping(now_ms)

                    success = await self.bootstrap_book(
                        venue=venue, symbol=symbol,
                        adapter=adapter,
                        depth=book.max_depth,
                        now_ms=now_ms,
                    )
                    if success:
                        return

                    delay = min(backoff, max_backoff)
                    backoff = min(backoff * 2, max_backoff)
                    # Add jitter
                    delay = delay + random.randint(0, jitter_ms)
                    await asyncio.sleep(delay / 1000.0)

            async def _bootstrap_with_sem(index: int, symbol: str) -> None:
                async with sem:
                    await _bootstrap_one(index, symbol)

            # Create tasks for all symbols and run with concurrency control
            tasks = [
                _bootstrap_with_sem(i, sym)
                for i, sym in enumerate(symbols)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # Log any exceptions that were swallowed by return_exceptions=True
            import logging
            _logger = logging.getLogger("lightfee.marketdata")
            error_count = 0
            for r in results:
                if isinstance(r, Exception):
                    error_count += 1
                    _logger.error("bootstrap worker %s exception: %s", venue, r)
            if error_count > 0:
                _logger.warning("bootstrap worker %s: %d tasks failed with exceptions", venue, error_count)

            self._journal.append(
                "runtime.local_l2_bootstrap_worker_done",
                {"venue": venue, "symbol_count": len(symbols)},
            )

        self._bootstrap_tasks[venue] = asyncio.create_task(_bootstrap_worker())

    def cancel_background_bootstrap(self, venue: str) -> None:
        """Cancel a running background bootstrap worker for a venue, if any."""
        task = self._bootstrap_tasks.pop(venue, None)
        if task is not None and not task.done():
            task.cancel()

    def cancel_all_bootstrap_tasks(self) -> int:
        """Cancel all background bootstrap workers. Returns count cancelled."""
        count = 0
        for task in list(self._bootstrap_tasks.values()):
            if not task.done():
                task.cancel()
                count += 1
        self._bootstrap_tasks.clear()
        return count

    @property
    def bootstrap_tasks_count(self) -> int:
        return sum(1 for t in self._bootstrap_tasks.values() if not t.done())

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

        Also cancels all background bootstrap workers.
        """
        self.cancel_all_bootstrap_tasks()
        for client in list(self._ws_clients.values()):
            try:
                await asyncio.wait_for(client.stop(), timeout=per_client_timeout_s)
            except asyncio.TimeoutError:
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
        """Hard-abort all WS workers and bootstrap tasks without waiting for graceful shutdown."""
        self.cancel_all_bootstrap_tasks()
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
            "buffered_symbols": len(self._pre_snapshot_buffers),
            "buffer_total_updates": sum(len(q) for q in self._pre_snapshot_buffers.values()),
            "bootstrap_tasks_active": self.bootstrap_tasks_count,
        }
