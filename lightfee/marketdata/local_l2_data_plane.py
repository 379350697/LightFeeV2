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
import time
from collections import deque
from dataclasses import dataclass, field, replace
from typing import Callable, Optional, TYPE_CHECKING

from lightfee.marketdata.l2 import (
    L2BookStatus,
    L2PoolAssignment,
    LocalL2BookKey,
    LocalL2Update,
    LocalL2UpdateKind,
)
from lightfee.marketdata.local_l2_policy import BridgeMode, policy_for_venue
from lightfee.marketdata.local_l2_runtime import LocalL2Runtime, RuntimeFaultKind
from lightfee.venues.transport import TransportError, TransportErrorCategory
from lightfee.persistence.journal import Journal

if TYPE_CHECKING:
    from lightfee.core.contracts import VenueAdapter
    from lightfee.marketdata.local_l2_ws import LocalL2WsClient

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


@dataclass
class _BufferedReplayResult:
    replayed: int = 0
    ok: bool = True
    failure_evidence: dict = field(default_factory=dict)


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


@dataclass
class _BookFreshnessState:
    """Tracks non-book-changing evidence that a HOT book is still authoritative."""

    last_ws_delta_ms: int = 0
    last_ws_keepalive_ms: int = 0
    last_book_confirmation_ms: int = 0
    last_subscription_confirmed_ms: int = 0
    last_rest_refresh_ms: int = 0


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
        # The HOT-book lifecycle has the same per-venue freshness owner as
        # entry Local-L2 readiness. An integer remains accepted for focused
        # tests and standalone uses; the live runtime installs a resolver so
        # a venue-specific V1 grace (notably OKX) is not demoted by another
        # venue's shorter bound.
        self.hot_stale_after_ms: int | Callable[[str], int] = 300_000
        self.buffered_replay_failure_alert_threshold: int = 3
        self._buffered_replay_failure_counts: dict[str, int] = {}
        self._rebuild_attempt_ids: dict[str, int] = {}
        self._freshness_states: dict[LocalL2BookKey, _BookFreshnessState] = {}
        self._state_event_last_ms: dict[tuple[str, str, str, str], int] = {}
        self._state_event_suppressed: dict[tuple[str, str, str, str], int] = {}
        self.state_event_rate_limit_ms: int = 60_000
        self.freshness_state_event_rate_limit_ms: int = 300_000
        self.snapshot_ok_event_rate_limit_ms: int = 300_000
        self.clock_skew_tolerance_ms: int = 5_000
        self.bootstrap_rebase_wait_ms: int = 250

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

            policy = policy_for_venue(venue)
            book = self._runtime.get_book(venue, symbol)

            # V1: binance_local_l2_snapshot_is_stale — reject older snapshots. Equal
            # sequence snapshots can be a valid refresh when the book has not changed.
            #
            # Venue policy guard: only venues whose REST and WS sequences are proven
            # comparable may use stale comparison. Bybit/Hyperliquid opt out; Bitget
            # and Gate intentionally keep legacy behavior until probe evidence proves
            # a venue-specific change is required.
            if (
                book is not None
                and update.sequence > 0
                and book.last_update_id > 0
                and policy.rest_snapshot_sequence_comparable
            ):
                if update.sequence < book.last_update_id:
                    self._journal.append(
                        "runtime.local_l2_snapshot_stale",
                        {"venue": venue, "symbol": symbol,
                         "snapshot_seq": update.sequence, "book_seq": book.last_update_id,
                         "policy_bridge_mode": policy.bridge_mode.value,
                         "reason_class": "stale_snapshot"},
                    )
                    # Small delay to avoid tight loop re-fetching the same stale snapshot
                    await asyncio.sleep(0.25)
                    return False

            # For WS-snapshot-authoritative venues with an active WS client, the REST
            # snapshot is secondary evidence — it must not be applied as the primary
            # bootstrap/recovery anchor when a WS stream is providing book snapshots.
            if policy.bridge_mode is BridgeMode.WS_SNAPSHOT_AUTHORITATIVE:
                ws_key = LocalL2BookKey(venue=venue, symbol=symbol)
                ws_client = self._ws_clients.get(ws_key)
                if ws_client is not None and getattr(ws_client, "is_connected", False):
                    self._append_rate_limited_state_event(
                        "runtime.local_l2_rest_bootstrap_deferred_for_ws_snapshot",
                        {"venue": venue, "symbol": symbol,
                         "snapshot_seq": update.sequence, "book_seq": getattr(book, "last_update_id", 0) if book else 0,
                         "policy": policy.bridge_mode.value},
                        now_ms,
                        reason=policy.bridge_mode.value,
                    )
                    return False

            apply_result = self._runtime.record_update_result(update, now_ms)
            if not apply_result.applied or apply_result.rebuild_required:
                ss.consecutive_failures += 1
                ss.last_error = apply_result.fault_reason or "snapshot_apply_failed"
                self._clear_rate_limited_state_event(
                    "runtime.local_l2_snapshot_ok",
                    venue,
                    symbol,
                    reason="ok",
                )
                self._journal.append(
                    "runtime.local_l2_snapshot_error",
                    {
                        "venue": venue,
                        "symbol": symbol,
                        "error": ss.last_error,
                        "category": "snapshot_apply_failed",
                    },
                )
                return False

            # Replay buffered WS updates accumulated during bootstrap gap (V1 parity)
            # Skip replay only for policies that cannot bridge REST snapshots with
            # WS deltas. OKX stays on the V1 buffered replay classifier.
            replay = _BufferedReplayResult()
            if policy.replay_rest_snapshot_with_ws_deltas:
                gate_rebase_replay = await self._gate_immediate_rebase_replay_if_needed(
                    venue=venue,
                    symbol=symbol,
                    adapter=adapter,
                    depth=depth,
                    now_ms=now_ms,
                )
                replay = (
                    gate_rebase_replay
                    if gate_rebase_replay is not None
                    else self._replay_buffered_updates(venue, symbol, now_ms=now_ms)
                )
            if replay.replayed > 0:
                self._journal.append(
                    "runtime.local_l2_buffered_replay",
                    {"venue": venue, "symbol": symbol, "replayed": replay.replayed},
                )
            if not replay.ok:
                book = self._runtime.get_book(venue, symbol)
                ss.consecutive_failures += 1
                ss.last_error = (
                    book.fault_reason if book is not None and book.fault_reason
                    else "buffered_replay_failed"
                )
                self._clear_rate_limited_state_event(
                    "runtime.local_l2_snapshot_ok",
                    venue,
                    symbol,
                    reason="ok",
                )
                self._journal.append(
                    "runtime.local_l2_snapshot_error",
                    {
                        "venue": venue,
                        "symbol": symbol,
                        "error": ss.last_error,
                        "category": "buffered_replay_failed",
                        **replay.failure_evidence,
                    },
                )
                return False

            # Mark book HOT after snapshot + buffered replay (V1: complete_bootstrap)
            book = self._runtime.get_book(venue, symbol)
            if book is not None and book.status in (
                L2BookStatus.BOOTSTRAPPING,
                L2BookStatus.REBUILDING,
                L2BookStatus.DEGRADED,
            ):
                book.transition_to_hot()
            if book is not None and venue in {"binance", "aster"}:
                book.pending_snapshot_bridge = (
                    replay.replayed == 0
                    and book.sequence > 0
                    and book.status == L2BookStatus.HOT
                )

            ss.last_snapshot_ms = now_ms
            ss.consecutive_failures = 0
            ss.last_error = ""
            self._freshness_state(venue, symbol).last_rest_refresh_ms = now_ms
            self._buffered_replay_failure_counts.pop(f"{venue}:{symbol}", None)
            self._append_rate_limited_state_event(
                "runtime.local_l2_snapshot_ok",
                {"venue": venue, "symbol": symbol},
                now_ms,
                reason="ok",
                interval_ms=self.snapshot_ok_event_rate_limit_ms,
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
            self._clear_rate_limited_state_event(
                "runtime.local_l2_snapshot_ok",
                venue,
                symbol,
                reason="ok",
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
            self._clear_rate_limited_state_event(
                "runtime.local_l2_snapshot_ok",
                venue,
                symbol,
                reason="ok",
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

    def _next_rebuild_attempt_id(self, venue: str, symbol: str) -> int:
        key = f"{venue}:{symbol}"
        attempt = self._rebuild_attempt_ids.get(key, 0) + 1
        self._rebuild_attempt_ids[key] = attempt
        return attempt

    @staticmethod
    def _status_value(status) -> str:
        return status.value if hasattr(status, "value") else str(status)

    def _freshness_state(self, venue: str, symbol: str) -> _BookFreshnessState:
        key = LocalL2BookKey(venue=venue, symbol=symbol)
        state = self._freshness_states.get(key)
        if state is None:
            state = _BookFreshnessState()
            self._freshness_states[key] = state
        return state

    def _append_rate_limited_state_event(
        self,
        kind: str,
        payload: dict,
        now_ms: int,
        *,
        reason: str = "",
        interval_ms: int | None = None,
    ) -> bool:
        venue = str(payload.get("venue", ""))
        symbol = str(payload.get("symbol", ""))
        event_key = (kind, venue, symbol, reason)
        last_ms = self._state_event_last_ms.get(event_key, 0)
        rate_limit_ms = self.state_event_rate_limit_ms if interval_ms is None else interval_ms
        if (
            last_ms > 0
            and now_ms > 0
            and (now_ms - last_ms) < rate_limit_ms
        ):
            self._state_event_suppressed[event_key] = (
                self._state_event_suppressed.get(event_key, 0) + 1
            )
            return False

        suppressed = self._state_event_suppressed.pop(event_key, 0)
        if suppressed:
            payload = dict(payload)
            payload["compact"] = True
            payload["suppressed_count"] = suppressed
        self._state_event_last_ms[event_key] = now_ms
        self._journal.append(kind, payload)
        return True

    def _clear_rate_limited_state_event(
        self,
        kind: str,
        venue: str,
        symbol: str,
        *,
        reason: str = "",
    ) -> None:
        event_key = (kind, str(venue), str(symbol), reason)
        self._state_event_last_ms.pop(event_key, None)
        self._state_event_suppressed.pop(event_key, None)

    def _mark_book_fresh_from_evidence(
        self,
        venue: str,
        symbol: str,
        evidence_ms: int,
    ) -> None:
        if evidence_ms <= 0:
            return
        book = self._runtime.get_book(venue, symbol)
        if book is None or book.status != L2BookStatus.HOT:
            return
        if book.bids and book.asks and evidence_ms > int(getattr(book, "observed_at_ms", 0) or 0):
            book.observed_at_ms = evidence_ms

    def _record_freshness_evidence(
        self,
        venue: str,
        symbol: str,
        now_ms: int,
        field_name: str,
        event_name: str,
        observed_at_ms: int = 0,
    ) -> None:
        evidence_ms = int(observed_at_ms or now_ms or time.time() * 1000)
        state = self._freshness_state(venue, symbol)
        setattr(state, field_name, max(int(getattr(state, field_name, 0) or 0), evidence_ms))
        self._mark_book_fresh_from_evidence(venue, symbol, evidence_ms)
        self._append_rate_limited_state_event(
            "runtime.local_l2_freshness_state",
            {
                "venue": venue,
                "symbol": symbol,
                "event": event_name,
                "evidence_at_ms": evidence_ms,
                "ts_ms": now_ms,
            },
            now_ms,
            reason=event_name,
            interval_ms=self.freshness_state_event_rate_limit_ms,
        )

    def note_ws_delta(
        self,
        venue: str,
        symbol: str,
        *,
        now_ms: int,
        observed_at_ms: int = 0,
    ) -> None:
        self._record_freshness_evidence(
            venue, symbol, now_ms, "last_ws_delta_ms", "ws_delta", observed_at_ms,
        )

    def note_ws_keepalive(
        self,
        venue: str,
        symbol: str,
        *,
        now_ms: int,
        observed_at_ms: int = 0,
    ) -> None:
        self._record_freshness_evidence(
            venue, symbol, now_ms, "last_ws_keepalive_ms", "ws_keepalive", observed_at_ms,
        )

    def note_ws_book_confirmation(
        self,
        venue: str,
        symbol: str,
        *,
        now_ms: int,
        observed_at_ms: int = 0,
    ) -> None:
        self._record_freshness_evidence(
            venue, symbol, now_ms, "last_book_confirmation_ms", "book_confirmation", observed_at_ms,
        )

    def note_ws_subscription_confirmed(
        self,
        venue: str,
        symbol: str,
        *,
        now_ms: int,
    ) -> None:
        self._record_freshness_evidence(
            venue, symbol, now_ms, "last_subscription_confirmed_ms", "subscription_confirmed",
        )

    def _ws_client_connected(self, key: LocalL2BookKey) -> bool:
        client = self._ws_clients.get(key)
        return bool(client is not None and getattr(client, "is_connected", False))

    def ws_stream_state(self, venue: str, symbol: str) -> dict[str, object]:
        """Return WS lifecycle evidence for a local-L2 venue/symbol stream."""
        key = LocalL2BookKey(venue=str(venue), symbol=str(symbol))
        client = self._ws_clients.get(key)
        state = self._freshness_states.get(key)
        return {
            "venue": key.venue,
            "symbol": key.symbol,
            "registered": client is not None,
            "connected": bool(client is not None and getattr(client, "is_connected", False)),
            "freshness_state_present": state is not None,
            "last_subscription_confirmed_ms": (
                int(state.last_subscription_confirmed_ms) if state is not None else 0
            ),
            "last_ws_delta_ms": int(state.last_ws_delta_ms) if state is not None else 0,
            "last_ws_keepalive_ms": (
                int(state.last_ws_keepalive_ms) if state is not None else 0
            ),
            "last_book_confirmation_ms": (
                int(state.last_book_confirmation_ms) if state is not None else 0
            ),
            "last_rest_refresh_ms": int(state.last_rest_refresh_ms) if state is not None else 0,
        }

    def _effective_hot_freshness_ms(
        self,
        key: LocalL2BookKey,
        book,
    ) -> int:
        state = self._freshness_states.get(key)
        evidence_ms = 0
        if state is not None:
            evidence_ms = max(
                state.last_ws_delta_ms,
                state.last_ws_keepalive_ms,
                state.last_book_confirmation_ms,
                state.last_subscription_confirmed_ms,
                state.last_rest_refresh_ms,
            )
        return max(int(getattr(book, "observed_at_ms", 0) or 0), evidence_ms)

    def _hot_proactive_refresh_interval_ms(self, stale_after_ms: int) -> int:
        configured = int(getattr(self, "hot_refresh_interval_ms", 0) or 0)
        if stale_after_ms <= 0:
            return configured
        proactive = max(250, (stale_after_ms * 3) // 4)
        if configured <= 0:
            return proactive
        return min(configured, proactive)

    def _hot_stale_after_ms_for_venue(self, venue: str) -> int:
        """Resolve the configured HOT-book freshness bound for one venue."""
        configured = getattr(self, "hot_stale_after_ms", 0)
        try:
            value = (
                configured(str(venue).strip().lower())
                if callable(configured)
                else configured
            )
            return max(int(value), 0)
        except (TypeError, ValueError, OverflowError):
            return 0

    def _hot_refresh_due(self, key: LocalL2BookKey, book, now_ms: int, stale_after_ms: int) -> bool:
        interval_ms = self._hot_proactive_refresh_interval_ms(stale_after_ms)
        if interval_ms <= 0:
            return False
        ss = self._snap_states.get(key)
        state = self._freshness_states.get(key)
        last_refresh_ms = max(
            int(getattr(book, "last_snapshot_ms", 0) or 0),
            int(getattr(ss, "last_snapshot_ms", 0) or 0) if ss is not None else 0,
            int(getattr(state, "last_rest_refresh_ms", 0) or 0) if state is not None else 0,
        )
        return last_refresh_ms <= 0 or (now_ms - last_refresh_ms) >= interval_ms

    def _classify_hot_stale_reason(
        self,
        key: LocalL2BookKey,
        book,
        now_ms: int,
        stale_after_ms: int,
        policy,
    ) -> str:
        observed_at_ms = int(getattr(book, "observed_at_ms", 0) or 0)
        if observed_at_ms > now_ms + self.clock_skew_tolerance_ms:
            return "clock_skew"

        if policy.bridge_mode in (
            BridgeMode.REST_SNAPSHOT_BUFFERED_REPLAY,
            BridgeMode.REST_POLLING_SNAPSHOT_ONLY,
        ):
            if self._hot_refresh_due(key, book, now_ms, stale_after_ms):
                return "rest_refresh_late"

        if policy.bridge_mode in (BridgeMode.WS_SNAPSHOT_AUTHORITATIVE, BridgeMode.STREAM_ONLY):
            if not self._ws_client_connected(key):
                return "subscription_missing"
            state = self._freshness_states.get(key)
            if state is None:
                return "subscription_missing"
            has_ws_evidence = max(
                state.last_subscription_confirmed_ms,
                state.last_ws_delta_ms,
                state.last_ws_keepalive_ms,
                state.last_book_confirmation_ms,
            ) > 0
            if not has_ws_evidence:
                return "subscription_missing"
            if state.last_ws_delta_ms <= 0:
                return "no_ws_delta"
            keepalive_ms = max(
                state.last_ws_keepalive_ms,
                state.last_book_confirmation_ms,
                state.last_subscription_confirmed_ms,
            )
            if keepalive_ms <= 0 or (now_ms - keepalive_ms) > stale_after_ms:
                return "no_keepalive"

        return "unknown"

    @staticmethod
    def _buffer_age_ms(buf: deque[_BufferedUpdate], now_ms: int) -> int:
        if not buf:
            return 0
        first_observed = int(getattr(buf[0], "observed_at_ms", 0) or 0)
        if first_observed <= 0 or now_ms <= 0:
            return 0
        return max(0, now_ms - first_observed)

    def _generation_filtered_buffer(
        self,
        venue: str,
        symbol: str,
        generation: int,
    ) -> tuple[deque[_BufferedUpdate], list[_BufferedUpdate]]:
        buf = self._pre_snapshot_buffers.get(f"{venue}:{symbol}") or deque()
        filtered = [b for b in list(buf) if b.generation == generation]
        return buf, filtered

    @staticmethod
    def _buffer_after_snapshot(
        filtered: list[_BufferedUpdate],
        snapshot_sequence: int,
    ) -> list[_BufferedUpdate]:
        return [
            b for b in filtered
            if int(getattr(b.update, "sequence", 0) or 0) > snapshot_sequence
        ]

    def _buffer_has_snapshot_boundary_overlap(
        self,
        filtered: list[_BufferedUpdate],
        snapshot_sequence: int,
    ) -> bool:
        expected = snapshot_sequence + 1
        return any(
            self._range_contains_expected(b.update, expected)
            for b in self._buffer_after_snapshot(filtered, snapshot_sequence)
        )

    def _gate_rebase_buffer_evidence(
        self,
        *,
        venue: str,
        symbol: str,
        branch: str,
        generation: int,
        initial_snapshot_seq: int,
        rebase_snapshot_seq: int,
        buf: deque[_BufferedUpdate],
        filtered: list[_BufferedUpdate],
        now_ms: int,
        rebase_wait_ms: int,
    ) -> dict:
        current_after_rebase = self._buffer_after_snapshot(filtered, rebase_snapshot_seq)
        first_current = filtered[0].update if filtered else None
        last_current = filtered[-1].update if filtered else None
        first_live = current_after_rebase[0].update if current_after_rebase else None
        return {
            "venue": venue,
            "symbol": symbol,
            "reason": "gate_immediate_snapshot_rebase",
            "branch": branch,
            "stream_generation": generation,
            "rebase_attempt": 1,
            "snapshot_fetch_count": 2,
            "initial_snapshot_seq": initial_snapshot_seq,
            "rebase_snapshot_seq": rebase_snapshot_seq,
            "initial_expected_sequence": initial_snapshot_seq + 1,
            "rebase_expected_sequence": rebase_snapshot_seq + 1,
            "buffered_count": len(buf),
            "buffer_current_generation_count": len(filtered),
            "buffer_age_ms": self._buffer_age_ms(buf, now_ms),
            "first_buffered_sequence": int(getattr(buf[0].update, "sequence", 0) or 0) if buf else 0,
            "last_buffered_sequence": int(getattr(buf[-1].update, "sequence", 0) or 0) if buf else 0,
            "current_first_buffered_U": int(getattr(first_current, "first_sequence", 0) or 0) if first_current else 0,
            "current_first_buffered_u": int(getattr(first_current, "sequence", 0) or 0) if first_current else 0,
            "current_last_buffered_U": int(getattr(last_current, "first_sequence", 0) or 0) if last_current else 0,
            "current_last_buffered_u": int(getattr(last_current, "sequence", 0) or 0) if last_current else 0,
            "first_live_buffered_U": int(getattr(first_live, "first_sequence", 0) or 0) if first_live else 0,
            "first_live_buffered_u": int(getattr(first_live, "sequence", 0) or 0) if first_live else 0,
            "rebase_wait_ms": rebase_wait_ms,
            "generation_isolation": "current_generation_only",
        }

    async def _gate_immediate_rebase_replay_if_needed(
        self,
        *,
        venue: str,
        symbol: str,
        adapter,
        depth: int,
        now_ms: int,
    ) -> _BufferedReplayResult | None:
        if venue != "gate":
            return None
        book = self._runtime.get_book(venue, symbol)
        if book is None:
            return None
        initial_snapshot_seq = int(getattr(book, "sequence", 0) or 0)
        if initial_snapshot_seq <= 0:
            return None
        generation = self._current_stream_generation(venue, symbol)
        _buf, filtered = self._generation_filtered_buffer(venue, symbol, generation)
        current_after_initial = self._buffer_after_snapshot(filtered, initial_snapshot_seq)
        if not current_after_initial:
            return None
        if self._buffer_has_snapshot_boundary_overlap(filtered, initial_snapshot_seq):
            return None

        rebase_wait_ms = min(
            max(int(getattr(self, "bootstrap_rebase_wait_ms", 250) or 0), 0),
            250,
        )
        if rebase_wait_ms > 0:
            await asyncio.sleep(rebase_wait_ms / 1_000.0)

        rebase_update = await adapter.fetch_l2_snapshot(symbol=symbol, depth=depth)
        apply_result = self._runtime.record_update_result(rebase_update, now_ms)
        rebase_snapshot_seq = int(getattr(rebase_update, "sequence", 0) or 0)
        buf, filtered = self._generation_filtered_buffer(venue, symbol, generation)
        if not apply_result.applied or apply_result.rebuild_required:
            evidence = self._gate_rebase_buffer_evidence(
                venue=venue,
                symbol=symbol,
                branch="second_snapshot_apply_failed",
                generation=generation,
                initial_snapshot_seq=initial_snapshot_seq,
                rebase_snapshot_seq=rebase_snapshot_seq,
                buf=buf,
                filtered=filtered,
                now_ms=now_ms,
                rebase_wait_ms=rebase_wait_ms,
            )
            evidence["error"] = apply_result.fault_reason or "snapshot_apply_failed"
            self._journal.append("runtime.local_l2_snapshot_rebase", evidence)
            return _BufferedReplayResult(ok=False, failure_evidence=evidence)

        if self._buffer_after_snapshot(filtered, rebase_snapshot_seq) and not (
            self._buffer_has_snapshot_boundary_overlap(filtered, rebase_snapshot_seq)
        ):
            evidence = self._gate_rebase_buffer_evidence(
                venue=venue,
                symbol=symbol,
                branch="second_snapshot_no_overlap",
                generation=generation,
                initial_snapshot_seq=initial_snapshot_seq,
                rebase_snapshot_seq=rebase_snapshot_seq,
                buf=buf,
                filtered=filtered,
                now_ms=now_ms,
                rebase_wait_ms=rebase_wait_ms,
            )
            self._journal.append("runtime.local_l2_snapshot_rebase", evidence)
            expected = rebase_snapshot_seq + 1
            failure_update = self._buffer_after_snapshot(filtered, rebase_snapshot_seq)[0].update
            return self._mark_rebuilding_from_buffered_replay_failure(
                venue=venue,
                symbol=symbol,
                book=self._runtime.get_book(venue, symbol),
                reason=(
                    "buffered_replay_snapshot_boundary: "
                    f"expected {expected} got {self._range_first_sequence(failure_update)}"
                ),
                now_ms=now_ms,
                snapshot_last_update_id=rebase_snapshot_seq,
                expected_previous_sequence=rebase_snapshot_seq,
                buf=buf,
                filtered=filtered,
                failure_update=failure_update,
                replayed=0,
                replay_index=-1,
            )

        evidence = self._gate_rebase_buffer_evidence(
            venue=venue,
            symbol=symbol,
            branch=(
                "second_snapshot_overlap"
                if self._buffer_after_snapshot(filtered, rebase_snapshot_seq)
                else "second_snapshot_covers_buffer"
            ),
            generation=generation,
            initial_snapshot_seq=initial_snapshot_seq,
            rebase_snapshot_seq=rebase_snapshot_seq,
            buf=buf,
            filtered=filtered,
            now_ms=now_ms,
            rebase_wait_ms=rebase_wait_ms,
        )
        self._journal.append("runtime.local_l2_snapshot_rebase", evidence)
        return self._replay_buffered_updates(venue, symbol, now_ms=now_ms)

    @staticmethod
    def _range_first_sequence(update: LocalL2Update) -> int:
        first_sequence = int(getattr(update, "first_sequence", 0) or 0)
        if first_sequence > 0:
            return first_sequence
        if update.venue != "gate":
            previous_sequence = int(getattr(update, "previous_sequence", 0) or 0)
            if previous_sequence > 0:
                return previous_sequence + 1
        return int(getattr(update, "sequence", 0) or 0)

    @classmethod
    def _range_contains_expected(
        cls,
        update: LocalL2Update,
        expected_sequence: int,
    ) -> bool:
        if expected_sequence <= 0:
            return True
        first_sequence = cls._range_first_sequence(update)
        final_sequence = int(getattr(update, "sequence", 0) or 0)
        return first_sequence <= expected_sequence <= final_sequence

    def _buffered_replay_failure_evidence(
        self,
        *,
        venue: str,
        symbol: str,
        reason: str,
        book,
        status_before: str,
        status_after: str,
        now_ms: int,
        rebuild_attempt_id: int,
        snapshot_last_update_id: int,
        expected_previous_sequence: int,
        buf: deque[_BufferedUpdate],
        filtered: list[_BufferedUpdate],
        failure_update: LocalL2Update | None,
        replayed: int,
        replay_index: int,
    ) -> dict:
        key = f"{venue}:{symbol}"
        failure_count = self._buffered_replay_failure_counts.get(key, 0) + 1
        self._buffered_replay_failure_counts[key] = failure_count
        threshold = max(1, int(self.buffered_replay_failure_alert_threshold or 1))
        alert = failure_count >= threshold
        policy = policy_for_venue(venue)
        raw_u = int(getattr(failure_update, "sequence", 0) or 0) if failure_update else 0
        raw_U = int(getattr(failure_update, "first_sequence", 0) or 0) if failure_update else 0
        raw_pu = int(getattr(failure_update, "previous_sequence", 0) or 0) if failure_update else 0
        previous_present = bool(
            getattr(failure_update, "previous_sequence_present", False)
        ) if failure_update else False
        if venue == "gate":
            continuity_contract = "range_only_U_u_contains_expected"
            continuity_action = "range_gap_rebuild"
            strict_continuity_rule = "range_must_contain_expected_sequence"
            semantic_action = "range_only_rebuild"
        else:
            continuity_contract = "previous_link_pu_equals_previous_u"
            continuity_action = "strict_previous_link_rebuild"
            strict_continuity_rule = "pu_must_equal_previous_u"
            semantic_action = "strict_rebuild"
        return {
            "venue": venue,
            "symbol": symbol,
            "error": reason,
            "reason": reason,
            "rebuild_trigger": reason,
            "rebuild_attempt_id": rebuild_attempt_id,
            "snapshot_lastUpdateId": snapshot_last_update_id,
            "snapshot_last_update_id": snapshot_last_update_id,
            "expected_previous_sequence": expected_previous_sequence,
            "raw_U": raw_U,
            "raw_u": raw_u,
            "raw_pu": raw_pu,
            "incoming_first_sequence": raw_U,
            "incoming_sequence": raw_u,
            "incoming_previous_sequence": raw_pu,
            "previous_sequence_present": previous_present,
            "buffered_count": len(buf),
            "filtered_buffered_count": len(filtered),
            "buffer_age_ms": self._buffer_age_ms(buf, now_ms),
            "first_buffer_observed_at_ms": int(getattr(buf[0], "observed_at_ms", 0) or 0) if buf else 0,
            "last_buffer_observed_at_ms": int(getattr(buf[-1], "observed_at_ms", 0) or 0) if buf else 0,
            "replayed": replayed,
            "replay_index": replay_index,
            "status_before": status_before,
            "status_after": status_after,
            "status_transition": f"{status_before}->{status_after}",
            "book_seq": int(getattr(book, "sequence", 0) or 0),
            "book_last_update_id": int(getattr(book, "last_update_id", 0) or 0),
            "policy_bridge_mode": policy.bridge_mode.value,
            "policy_buffer_cap": policy.pre_snapshot_buffer_cap,
            "reason_class": "buffered_replay_failed",
            "continuity_contract": continuity_contract,
            "continuity_action": continuity_action,
            "strict_continuity_rule": strict_continuity_rule,
            "semantic_action": semantic_action,
            "root_bug_suspected": False,
            "replay_failure_count_for_symbol": failure_count,
            "replay_failure_alert_threshold": threshold,
            "replay_failure_alert": alert,
            "severity": "warning" if alert else "info",
            "evidence_level": "warning" if alert else "info",
        }

    def _mark_rebuilding_from_buffered_replay_failure(
        self,
        *,
        venue: str,
        symbol: str,
        book,
        reason: str,
        now_ms: int,
        snapshot_last_update_id: int,
        expected_previous_sequence: int,
        buf: deque[_BufferedUpdate],
        filtered: list[_BufferedUpdate],
        failure_update: LocalL2Update | None,
        replayed: int,
        replay_index: int,
    ) -> _BufferedReplayResult:
        status_before = self._status_value(book.status)
        rebuild_attempt_id = self._next_rebuild_attempt_id(venue, symbol)
        book.sequence = 0
        book.last_update_id = 0
        book.fault_reason = reason
        book.transition_to_rebuilding(now_ms)
        status_after = self._status_value(book.status)
        self._runtime.handle_runtime_failure(
            venue,
            symbol,
            RuntimeFaultKind.SEQUENCE_GAP,
            reason,
            now_ms,
        )
        evidence = self._buffered_replay_failure_evidence(
            venue=venue,
            symbol=symbol,
            reason=reason,
            book=book,
            status_before=status_before,
            status_after=status_after,
            now_ms=now_ms,
            rebuild_attempt_id=rebuild_attempt_id,
            snapshot_last_update_id=snapshot_last_update_id,
            expected_previous_sequence=expected_previous_sequence,
            buf=buf,
            filtered=filtered,
            failure_update=failure_update,
            replayed=replayed,
            replay_index=replay_index,
        )
        self._journal.append("runtime.local_l2_buffered_replay_rebuild", evidence)
        return _BufferedReplayResult(replayed=replayed, ok=False, failure_evidence=evidence)

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

        # Official venue semantics: WS/REST snapshots are authoritative book
        # resets.  Do not park them behind the pre-snapshot delta buffer; Bybit,
        # OKX, Bitget, Gate, and Hyperliquid all use snapshots to establish or
        # re-establish the local book.
        if update.update_kind == LocalL2UpdateKind.SNAPSHOT:
            self._pre_snapshot_buffers.pop(key, None)
            result = self._runtime.record_update_result(update, now_ms)
            book = self._runtime.get_book(update.venue, update.symbol)
            if result.applied and not result.rebuild_required and book is not None:
                book.transition_to_hot()
                if update.venue in {"binance", "aster"}:
                    book.pending_snapshot_bridge = update.sequence > 0
                self.note_ws_book_confirmation(
                    update.venue,
                    update.symbol,
                    now_ms=now_ms,
                )
            return result.events

        if book is not None and book.status not in (L2BookStatus.BOOTSTRAPPING, L2BookStatus.REBUILDING):
            bridge_previous_sequence = int(getattr(book, "sequence", 0) or 0)
            apply_snapshot_bridge_anchor = (
                update.venue in {"binance", "aster"}
                and bool(getattr(book, "pending_snapshot_bridge", False))
                and bridge_previous_sequence > 0
                and self._range_contains_expected(update, bridge_previous_sequence + 1)
                and (update.previous_sequence_present or update.previous_sequence > 0)
                and update.previous_sequence != bridge_previous_sequence
            )
            if self._range_update_requires_rebuild(book, update, now_ms):
                return []
            if apply_snapshot_bridge_anchor:
                update = replace(update, previous_sequence=bridge_previous_sequence)

        # Buffer delta updates during bootstrap/rebuild gap
        # V1: handle_binance_local_l2_ws_message_for_instance lines 4423-4435
        if book is not None and book.status in (L2BookStatus.BOOTSTRAPPING, L2BookStatus.REBUILDING):
            buf = self._pre_snapshot_buffers.get(key)
            if buf is None:
                buf = deque()
                self._pre_snapshot_buffers[key] = buf
            cap = policy_for_venue(update.venue).pre_snapshot_buffer_cap
            if len(buf) >= cap:
                # V1 Aster/Binance semantics: keep the newest window while a
                # REST snapshot is in flight. The replay boundary decides if a
                # rebuild is needed; overflow itself is not terminal evidence.
                buf.popleft()
            gen = self._current_stream_generation(update.venue, update.symbol)
            buf.append(_BufferedUpdate(generation=gen, observed_at_ms=now_ms, update=update))
            return []

        # If there are leftover buffered updates (edge case: book became HOT while
        # buffered updates were still pending), replay them first.
        if book is not None and self._pre_snapshot_buffers.get(key):
            replay = self._replay_buffered_updates(update.venue, update.symbol)
            if not replay.ok:
                return []

        if update.venue == "gate" and (
            update.previous_sequence != 0 or update.previous_sequence_present
        ):
            update = replace(
                update,
                previous_sequence=0,
                previous_sequence_present=False,
            )

        result = self._runtime.record_update_result(update, now_ms)
        if result.applied and not result.rebuild_required:
            self.note_ws_delta(
                update.venue,
                update.symbol,
                now_ms=now_ms,
            )
        return result.events

    def _range_update_requires_rebuild(
        self,
        book,
        update: LocalL2Update,
        now_ms: int,
    ) -> bool:
        if update.venue not in {"binance", "aster", "gate"}:
            return False
        if update.sequence <= 0 or getattr(book, "sequence", 0) <= 0:
            return False
        if update.sequence <= book.sequence:
            return True

        expected = book.sequence + 1
        first_sequence = self._range_first_sequence(update)

        if update.venue == "gate":
            if not self._range_contains_expected(update, expected):
                self._mark_rebuilding_from_stream_gap(
                    book,
                    update,
                    now_ms,
                    f"sequence_ahead: expected {expected} got {first_sequence}",
                )
                return True
            return False

        if getattr(book, "pending_snapshot_bridge", False):
            if self._range_contains_expected(update, expected):
                book.pending_snapshot_bridge = False
                return False
            book.pending_snapshot_bridge = False
            self._mark_rebuilding_from_stream_gap(
                book,
                update,
                now_ms,
                f"sequence_ahead: expected {expected} got {first_sequence}",
            )
            return True

        has_previous_link = (
            (update.previous_sequence_present or update.previous_sequence > 0)
            and update.previous_sequence > 0
        )
        if has_previous_link:
            if update.previous_sequence != book.sequence:
                self._mark_rebuilding_from_stream_gap(
                    book,
                    update,
                    now_ms,
                    f"previous_link_mismatch: expected {book.sequence} got {update.previous_sequence}",
                )
                return True
            book.pending_snapshot_bridge = False
            if not self._range_contains_expected(update, expected):
                self._mark_rebuilding_from_stream_gap(
                    book,
                    update,
                    now_ms,
                    f"sequence_ahead: expected {expected} got {first_sequence}",
                )
                return True
            return False

        self._mark_rebuilding_from_stream_gap(
            book,
            update,
            now_ms,
            f"missing_previous_link: expected {book.sequence}",
        )
        return True

    def _mark_rebuilding_from_stream_gap(
        self,
        book,
        update: LocalL2Update,
        now_ms: int,
        reason: str,
    ) -> None:
        previous_book_seq = getattr(book, "sequence", 0)
        previous_book_last_update_id = getattr(book, "last_update_id", 0)
        status_before = book.status.value if hasattr(book.status, "value") else str(book.status)
        pool_before = book.pool.value if hasattr(book.pool, "value") else str(book.pool)
        expected_sequence = previous_book_seq + 1 if previous_book_seq > 0 else 0
        incoming_first_sequence = update.first_sequence
        if incoming_first_sequence <= 0:
            if update.previous_sequence > 0:
                incoming_first_sequence = update.previous_sequence + 1
            else:
                incoming_first_sequence = update.sequence

        buf = self._pre_snapshot_buffers.get(f"{update.venue}:{update.symbol}")
        buffered_count = len(buf) if buf is not None else 0
        first_buffered_sequence = 0
        last_buffered_sequence = 0
        if buf:
            first_buffered_sequence = int(getattr(buf[0].update, "sequence", 0) or 0)
            last_buffered_sequence = int(getattr(buf[-1].update, "sequence", 0) or 0)
        policy = policy_for_venue(update.venue)

        book.sequence = 0
        book.last_update_id = 0
        book.pending_snapshot_bridge = False
        book.fault_reason = reason
        book.transition_to_rebuilding(now_ms)
        status_after = book.status.value if hasattr(book.status, "value") else str(book.status)
        rebuild_attempt_id = self._next_rebuild_attempt_id(update.venue, update.symbol)
        self._runtime.handle_runtime_failure(
            update.venue,
            update.symbol,
            RuntimeFaultKind.SEQUENCE_GAP,
            reason,
            now_ms,
        )
        self._journal.append(
            "runtime.local_l2_sequence_gap_rebuild",
            self._rebuild_evidence(
                venue=update.venue,
                symbol=update.symbol,
                rebuild_trigger=reason,
                incoming_sequence=update.sequence,
                incoming_previous_sequence=update.previous_sequence,
                incoming_first_sequence=incoming_first_sequence,
                raw_U=update.first_sequence,
                raw_u=update.sequence,
                raw_pu=update.previous_sequence,
                previous_sequence_present=update.previous_sequence_present,
                expected_sequence=expected_sequence,
                expected_previous_sequence=previous_book_seq,
                snapshot_last_update_id=previous_book_last_update_id,
                rebuild_attempt_id=rebuild_attempt_id,
                buffered_count=buffered_count,
                first_buffered_sequence=first_buffered_sequence,
                last_buffered_sequence=last_buffered_sequence,
                policy_buffer_cap=policy.pre_snapshot_buffer_cap,
                book_seq=previous_book_seq,
                reason_class="sequence_gap",
                status_before=status_before,
                status_after=status_after,
                pool_before=pool_before,
            ),
        )

    def _replay_buffered_updates(
        self, venue: str, symbol: str, now_ms: int | None = None,
    ) -> _BufferedReplayResult:
        """Replay buffered WS updates accumulated during bootstrap gap.

        V1: replay_binance_buffered_local_l2_depth_updates_for_instance()
        - Filters out updates from old stream generations
        - Skips updates already covered by the snapshot (sequence <= book.sequence)
        - Detects sequence gaps that require a rebuild
        - Returns replay count plus ok=false when replay failure must keep the
          book out of HOT
        """
        key = f"{venue}:{symbol}"
        buf = self._pre_snapshot_buffers.pop(key, None)
        if not buf:
            return _BufferedReplayResult()

        book = self._runtime.get_book(venue, symbol)
        if book is None:
            return _BufferedReplayResult()

        replay_now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        current_gen = self._current_stream_generation(venue, symbol)
        previous_sequence = book.sequence  # snapshot's sequence
        snapshot_last_update_id = previous_sequence

        # Filter: keep only current-generation updates (V1: retain by generation)
        # Convert to list for indexed access
        filtered: list[_BufferedUpdate] = [
            b for b in buf if b.generation == current_gen
        ]
        if not filtered:
            return _BufferedReplayResult()

        policy = policy_for_venue(venue)
        if policy.venue == "okx":
            replayed = 0
            for i, bu in enumerate(filtered):
                link_kind = policy.classify_replay_link(
                    previous_sequence=previous_sequence,
                    sequence=bu.update.sequence,
                    previous_sequence_from_update=bu.update.previous_sequence,
                    bid_count=len(bu.update.bids),
                    ask_count=len(bu.update.asks),
                )
                if link_kind.value == "obsolete":
                    continue
                if link_kind.value == "invalid":
                    reason = (
                        f"buffered_replay_invalid_link: expected {previous_sequence} "
                        f"got {bu.update.previous_sequence}->{bu.update.sequence}"
                    )
                    return self._mark_rebuilding_from_buffered_replay_failure(
                        venue=venue,
                        symbol=symbol,
                        book=book,
                        reason=reason,
                        now_ms=replay_now_ms,
                        snapshot_last_update_id=snapshot_last_update_id,
                        expected_previous_sequence=previous_sequence,
                        buf=buf,
                        filtered=filtered,
                        failure_update=bu.update,
                        replayed=replayed,
                        replay_index=i,
                    )
                try:
                    replay_result = self._runtime.record_update_result(
                        bu.update, bu.observed_at_ms,
                    )
                    if replay_result.rebuild_required:
                        reason = (
                            replay_result.fault_reason
                            or f"buffered_replay_apply_failed at index {i}"
                        )
                        return self._mark_rebuilding_from_buffered_replay_failure(
                            venue=venue,
                            symbol=symbol,
                            book=book,
                            reason=reason,
                            now_ms=replay_now_ms,
                            snapshot_last_update_id=snapshot_last_update_id,
                            expected_previous_sequence=previous_sequence,
                            buf=buf,
                            filtered=filtered,
                            failure_update=bu.update,
                            replayed=replayed,
                            replay_index=i,
                        )
                    if replay_result.applied:
                        previous_sequence = bu.update.sequence
                        replayed += 1
                except Exception:
                    reason = f"buffered_replay_apply_failed at index {i}"
                    return self._mark_rebuilding_from_buffered_replay_failure(
                        venue=venue,
                        symbol=symbol,
                        book=book,
                        reason=reason,
                        now_ms=replay_now_ms,
                        snapshot_last_update_id=snapshot_last_update_id,
                        expected_previous_sequence=previous_sequence,
                        buf=buf,
                        filtered=filtered,
                        failure_update=bu.update,
                        replayed=replayed,
                        replay_index=i,
                    )

            return _BufferedReplayResult(replayed=replayed)

        # Drop updates already covered by the snapshot (V1: final_update_id <= previous_sequence)
        while filtered and filtered[0].update.sequence <= previous_sequence:
            filtered.pop(0)
        if not filtered:
            return _BufferedReplayResult()

        # Find first update that bridges snapshot to live stream
        # V1: first_update_id <= expected <= final_update_id
        expected = previous_sequence + 1
        start_index = None
        for i, bu in enumerate(filtered):
            previous_link_matches_anchor = (
                policy.venue not in {"binance", "aster", "gate"}
                and (bu.update.previous_sequence_present or bu.update.previous_sequence > 0)
                and bu.update.previous_sequence == previous_sequence
            )
            if self._range_contains_expected(bu.update, expected) or previous_link_matches_anchor:
                start_index = i
                break

        if start_index is None:
            # No overlap — gap between snapshot and buffered updates
            return self._mark_rebuilding_from_buffered_replay_failure(
                venue=venue,
                symbol=symbol,
                book=book,
                reason="buffered_replay_snapshot_boundary: no overlapping update",
                now_ms=replay_now_ms,
                snapshot_last_update_id=snapshot_last_update_id,
                expected_previous_sequence=previous_sequence,
                buf=buf,
                filtered=filtered,
                failure_update=filtered[0].update if filtered else None,
                replayed=0,
                replay_index=-1,
            )

        # Replay from overlap point with continuity check
        # V1: buffered_updates.into_iter().skip(start_index).enumerate()
        replayed = 0
        for i, bu in enumerate(filtered[start_index:], start=start_index):
            if bu.update.sequence <= previous_sequence:
                continue
            is_first_replay = i == start_index
            has_previous_link = (
                (bu.update.previous_sequence_present or bu.update.previous_sequence > 0)
                and bu.update.previous_sequence > 0
            )
            expected = previous_sequence + 1
            if policy.venue in {"binance", "aster"} and not is_first_replay:
                if not has_previous_link:
                    reason = f"buffered_replay_missing_previous_link: expected {previous_sequence}"
                    return self._mark_rebuilding_from_buffered_replay_failure(
                        venue=venue,
                        symbol=symbol,
                        book=book,
                        reason=reason,
                        now_ms=replay_now_ms,
                        snapshot_last_update_id=snapshot_last_update_id,
                        expected_previous_sequence=previous_sequence,
                        buf=buf,
                        filtered=filtered,
                        failure_update=bu.update,
                        replayed=replayed,
                        replay_index=i,
                    )
                if bu.update.previous_sequence != previous_sequence:
                    reason = (
                        f"buffered_replay_previous_link_mismatch: expected {previous_sequence} "
                        f"got {bu.update.previous_sequence}"
                    )
                    return self._mark_rebuilding_from_buffered_replay_failure(
                        venue=venue,
                        symbol=symbol,
                        book=book,
                        reason=reason,
                        now_ms=replay_now_ms,
                        snapshot_last_update_id=snapshot_last_update_id,
                        expected_previous_sequence=previous_sequence,
                        buf=buf,
                        filtered=filtered,
                        failure_update=bu.update,
                        replayed=replayed,
                        replay_index=i,
                    )
            if policy.venue in {"binance", "aster", "gate"}:
                first_sequence = self._range_first_sequence(bu.update)
                if not self._range_contains_expected(bu.update, expected):
                    reason = (
                        f"buffered_replay_snapshot_boundary: expected {expected} "
                        f"got {first_sequence}"
                    )
                    return self._mark_rebuilding_from_buffered_replay_failure(
                        venue=venue,
                        symbol=symbol,
                        book=book,
                        reason=reason,
                        now_ms=replay_now_ms,
                        snapshot_last_update_id=snapshot_last_update_id,
                        expected_previous_sequence=previous_sequence,
                        buf=buf,
                        filtered=filtered,
                        failure_update=bu.update,
                        replayed=replayed,
                        replay_index=i,
                    )

            # V1/Binance semantics split the initial REST-to-WS bridge from the
            # subsequent WS chain. The first replayed event is admitted by its
            # U..u range; only later events require pu == the previous accepted u.
            if (
                has_previous_link
                and bu.update.previous_sequence != previous_sequence
                and policy.venue != "gate"
                and (
                    not is_first_replay
                    or policy.venue not in {"binance", "aster"}
                )
            ):
                reason = (
                    f"buffered_replay_previous_link_mismatch: expected {previous_sequence} "
                    f"got {bu.update.previous_sequence}"
                )
                if is_first_replay and expected < bu.update.previous_sequence + 1:
                    reason = (
                        f"buffered_replay_snapshot_boundary: expected {expected} "
                        f"got {bu.update.previous_sequence + 1}"
                    )
                return self._mark_rebuilding_from_buffered_replay_failure(
                    venue=venue,
                    symbol=symbol,
                    book=book,
                    reason=reason,
                    now_ms=replay_now_ms,
                    snapshot_last_update_id=snapshot_last_update_id,
                    expected_previous_sequence=previous_sequence,
                    buf=buf,
                    filtered=filtered,
                    failure_update=bu.update,
                    replayed=replayed,
                    replay_index=i,
                )

            replay_update = bu.update
            if (
                is_first_replay
                and policy.venue in {"binance", "aster"}
                and has_previous_link
                and bu.update.previous_sequence != previous_sequence
            ):
                # LocalL2Book applies deltas against a previous-sequence anchor.
                # For the first valid U..u bridge that anchor is the REST
                # snapshot; retain the raw pu on bu.update for diagnostics and
                # use a non-mutating replay copy for application.
                replay_update = replace(
                    bu.update,
                    previous_sequence=previous_sequence,
                )
            elif policy.venue == "gate" and (
                bu.update.previous_sequence != 0 or bu.update.previous_sequence_present
            ):
                replay_update = replace(
                    bu.update,
                    previous_sequence=0,
                    previous_sequence_present=False,
                )

            try:
                replay_result = self._runtime.record_update_result(
                    replay_update, bu.observed_at_ms,
                )
                if replay_result.rebuild_required:
                    reason = (
                        replay_result.fault_reason
                        or f"buffered_replay_apply_failed at index {i}"
                    )
                    return self._mark_rebuilding_from_buffered_replay_failure(
                        venue=venue,
                        symbol=symbol,
                        book=book,
                        reason=reason,
                        now_ms=replay_now_ms,
                        snapshot_last_update_id=snapshot_last_update_id,
                        expected_previous_sequence=previous_sequence,
                        buf=buf,
                        filtered=filtered,
                        failure_update=bu.update,
                        replayed=replayed,
                        replay_index=i,
                    )
                if replay_result.applied:
                    previous_sequence = bu.update.sequence
                    replayed += 1
            except Exception:
                reason = f"buffered_replay_apply_failed at index {i}"
                return self._mark_rebuilding_from_buffered_replay_failure(
                    venue=venue,
                    symbol=symbol,
                    book=book,
                    reason=reason,
                    now_ms=replay_now_ms,
                    snapshot_last_update_id=snapshot_last_update_id,
                    expected_previous_sequence=previous_sequence,
                    buf=buf,
                    filtered=filtered,
                    failure_update=bu.update,
                    replayed=replayed,
                    replay_index=i,
                )

        return _BufferedReplayResult(replayed=replayed)

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
        from lightfee.core.domain import Venue

        snapshot_candidates: list[tuple[int, str, str, object, int]] = []
        for key, book in list(self._runtime.books.items()):
            if book.pool == L2PoolAssignment.DROPPED:
                continue

            policy = policy_for_venue(key.venue)

            # V1: HOT books rely on WS deltas, but stale HOT books must be
            # demoted and rebuilt instead of remaining permanently not-ready.
            if book.status == L2BookStatus.HOT:
                stale_after_ms = self._hot_stale_after_ms_for_venue(key.venue)
                effective_freshness_ms = self._effective_hot_freshness_ms(key, book)
                observed_at_ms = int(getattr(book, "observed_at_ms", 0) or 0)
                clock_skew = observed_at_ms > now_ms + self.clock_skew_tolerance_ms
                effective_stale = (
                    clock_skew
                    or stale_after_ms > 0
                    and (
                        effective_freshness_ms <= 0
                        or (now_ms - effective_freshness_ms) > stale_after_ms
                    )
                )
                if stale_after_ms <= 0 or not effective_stale:
                    self._mark_book_fresh_from_evidence(
                        key.venue,
                        key.symbol,
                        effective_freshness_ms,
                    )
                    if policy.bridge_mode not in (
                        BridgeMode.REST_SNAPSHOT_BUFFERED_REPLAY,
                        BridgeMode.REST_POLLING_SNAPSHOT_ONLY,
                    ):
                        continue
                    if not self._hot_refresh_due(key, book, now_ms, stale_after_ms):
                        continue
                reason = self._classify_hot_stale_reason(
                    key, book, now_ms, stale_after_ms, policy,
                )
                if (
                    policy.bridge_mode in (
                        BridgeMode.REST_SNAPSHOT_BUFFERED_REPLAY,
                        BridgeMode.REST_POLLING_SNAPSHOT_ONLY,
                    )
                    and not effective_stale
                ):
                    pass
                else:
                    status_before = book.status.value if hasattr(book.status, "value") else str(book.status)
                    pool_before = book.pool.value if hasattr(book.pool, "value") else str(book.pool)
                    book.fault_reason = f"stale_hot_book:{reason}"
                    book.transition_to_rebuilding(now_ms)
                    status_after = book.status.value if hasattr(book.status, "value") else str(book.status)
                    self._append_rate_limited_state_event(
                        "runtime.local_l2_hot_stale_rebuild",
                        {
                            "venue": key.venue,
                            "symbol": key.symbol,
                            "book_status": status_before,
                            "status_before": status_before,
                            "status_after": status_after,
                            "pool": pool_before,
                            "pool_before": pool_before,
                            "age_ms": book.age_ms(now_ms),
                            "effective_age_ms": (
                                now_ms - effective_freshness_ms if effective_freshness_ms > 0 else 0
                            ),
                            "observed_at_ms": book.observed_at_ms,
                            "effective_freshness_ms": effective_freshness_ms,
                            "stale_after_ms": stale_after_ms,
                            "last_update_id": book.last_update_id,
                            "sequence": book.sequence,
                            "bid_count": len(book.bids) if book.bids else 0,
                            "ask_count": len(book.asks) if book.asks else 0,
                            "ts_ms": now_ms,
                            "policy_bridge_mode": policy.bridge_mode.value,
                            "reason": reason,
                            "reason_class": reason,
                        },
                        now_ms,
                        reason=reason,
                    )
                    if policy.bridge_mode is BridgeMode.STREAM_ONLY:
                        continue

            if policy.bridge_mode is BridgeMode.STREAM_ONLY:
                continue

            # V1 dual-phase gating: pre-scan only refreshes execution-owned books
            # (RETAINED or HOT_EXEC); post-shortlist allows scan-promoted books too
            if not scan_promoted and book.pool not in (L2PoolAssignment.RETAINED, L2PoolAssignment.HOT_EXEC):
                continue

            interval_ms = self._snapshot_interval_for_status(book.status)
            if book.status == L2BookStatus.HOT:
                interval_ms = self._hot_proactive_refresh_interval_ms(
                    self._hot_stale_after_ms_for_venue(key.venue)
                )
            ss = self._snap_states.get(key)
            if ss is not None and ss.snapshot_in_flight:
                continue
            state = self._freshness_states.get(key)
            last_snapshot_ms = int(getattr(book, "last_snapshot_ms", 0) or 0)
            if book.status == L2BookStatus.HOT:
                last_snapshot_ms = max(
                    last_snapshot_ms,
                    int(getattr(ss, "last_snapshot_ms", 0) or 0) if ss is not None else 0,
                    int(getattr(state, "last_rest_refresh_ms", 0) or 0) if state is not None else 0,
                )
            if interval_ms > 0 and last_snapshot_ms > 0:
                if (now_ms - last_snapshot_ms) < interval_ms:
                    continue

            ven = Venue.from_str(key.venue)
            adapter = adapters.get(ven)
            if adapter is None:
                continue
            if not hasattr(adapter, 'fetch_l2_snapshot'):
                continue

            deadline_ms = 0
            if interval_ms > 0 and last_snapshot_ms > 0:
                deadline_ms = last_snapshot_ms + interval_ms
            snapshot_candidates.append(
                (deadline_ms, key.venue, key.symbol, adapter, book.max_depth)
            )

        limit = max(0, int(self.max_concurrent_snapshots or 0))
        if limit <= 0 or not snapshot_candidates:
            return 0

        snapshot_candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        selected_candidates = snapshot_candidates[:limit]

        async def _bootstrap_candidate(candidate: tuple[int, str, str, object, int]) -> bool:
            _deadline_ms, venue, symbol, adapter, depth = candidate
            return await self.bootstrap_book(
                venue=venue,
                symbol=symbol,
                adapter=adapter,
                depth=depth,
                now_ms=now_ms,
            )

        results = await asyncio.gather(
            *(_bootstrap_candidate(candidate) for candidate in selected_candidates),
            return_exceptions=True,
        )
        dispatched = sum(1 for result in results if result is True)

        if selected_candidates:
            self._journal.append(
                "runtime.local_l2_snapshots_synced",
                {
                    "dispatched": dispatched,
                    "attempted": len(selected_candidates),
                    "ts_ms": now_ms,
                },
            )

        return dispatched

    def prune_untracked_books(
        self,
        tracked: set[LocalL2BookKey],
        now_ms: int,
        *,
        retained_max_age_ms: int = 300_000,
        retained_global_limit: int = 128,
        retained_per_venue_limit: int = 32,
    ) -> list[dict]:
        pruned = self._runtime.prune_untracked_books(
            tracked,
            now_ms,
            retained_max_age_ms=retained_max_age_ms,
            retained_global_limit=retained_global_limit,
            retained_per_venue_limit=retained_per_venue_limit,
        )
        if not pruned:
            return []

        for item in pruned:
            key = LocalL2BookKey(venue=item["venue"], symbol=item["symbol"])
            self.stop_worker(key)
            self._snap_states.pop(key, None)
            self._freshness_states.pop(key, None)
            for event_key in list(self._state_event_last_ms):
                if event_key[1] == key.venue and event_key[2] == key.symbol:
                    self._state_event_last_ms.pop(event_key, None)
            for event_key in list(self._state_event_suppressed):
                if event_key[1] == key.venue and event_key[2] == key.symbol:
                    self._state_event_suppressed.pop(event_key, None)
            self._pre_snapshot_buffers.pop(f"{key.venue}:{key.symbol}", None)
            self._stream_generations.pop(f"{key.venue}:{key.symbol}", None)

        self._journal.append(
            "runtime.local_l2_books_pruned",
            {"pruned_count": len(pruned), "items": pruned, "ts_ms": now_ms},
        )
        return pruned

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
        if policy_for_venue(venue).bridge_mode is BridgeMode.STREAM_ONLY:
            self._journal.append(
                "runtime.local_l2_stream_only_bootstrap_skipped",
                {"venue": venue, "symbol_count": len(symbols)},
            )
            return

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
                    now_ms = int(time.time() * 1000)
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
        adapter=None,  # kept for call-site compatibility
    ) -> int:
        """Register WebSocket L2 delta streams for a venue's symbols.

        Creates WS clients and registers them in the data plane.
        Hyperliquid is V1 stream-only (l2Book WS); the adapter argument is
        accepted for compatibility with runtime call sites.

        Caller must await connect_ws_streams() from an async context
        to actually open the connections.

        Returns the number of streams registered.
        """
        from lightfee.marketdata.local_l2_ws import create_ws_client

        started = 0
        for symbol in symbols:
            client = create_ws_client(
                venue=venue,
                symbol=symbol,
                data_plane=self,
            )
            if client is None:
                continue

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

    def _rebuild_evidence(
        self, *, venue: str, symbol: str,
        rebuild_trigger: str = "",
        buffered_count: int = 0,
        replayed_count: int = 0,
        first_buffered_sequence: int = 0,
        last_buffered_sequence: int = 0,
        incoming_sequence: int = 0,
        incoming_previous_sequence: int = 0,
        incoming_first_sequence: int = 0,
        expected_sequence: int = 0,
        policy_buffer_cap: int = 0,
        book_seq: int = 0,
        snapshot_seq: int = 0,
        reason_class: str = "",
        status_before: str = "",
        status_after: str = "",
        pool_before: str = "",
        **extra,
    ) -> dict:
        """Build a structured evidence payload for rebuild/transition logging.

        Includes venue policy, buffer state, and sequence-domain evidence so
        production diagnostics can distinguish real sequence gaps from
        config/domain-drift false positives.
        """
        book = self._runtime.get_book(venue, symbol)
        obs_ms = int(getattr(book, "observed_at_ms", 0) or 0)
        seq = int(getattr(book, "sequence", 0) or 0)
        bid_count = len(getattr(book, "bids", []) or [])
        ask_count = len(getattr(book, "asks", []) or [])
        top_bid = book.best_bid() if hasattr(book, "best_bid") else 0.0
        top_ask = book.best_ask() if hasattr(book, "best_ask") else 0.0
        policy = policy_for_venue(venue)
        current_status = (
            book.status.value if book is not None and hasattr(book.status, "value")
            else str(getattr(book, "status", "unknown")) if book else "missing"
        )
        current_pool = (
            book.pool.value if book is not None and hasattr(book.pool, "value")
            else str(getattr(book, "pool", "unknown")) if book else "missing"
        )
        payload = {
            "venue": venue,
            "symbol": symbol,
            "status_before": status_before or current_status,
            "status_after": status_after or current_status,
            "pool_before": pool_before or current_pool,
            "pool": current_pool,
            "observed_at_ms": obs_ms,
            "sequence": seq,
            "bid_count": bid_count,
            "ask_count": ask_count,
            "top_bid": top_bid,
            "top_ask": top_ask,
            "rebuild_trigger": rebuild_trigger,
            "buffered_count": buffered_count,
            "replayed_count": replayed_count,
            "first_buffered_sequence": first_buffered_sequence,
            "last_buffered_sequence": last_buffered_sequence,
            "incoming_sequence": incoming_sequence,
            "incoming_previous_sequence": incoming_previous_sequence,
            "incoming_first_sequence": incoming_first_sequence,
            "expected_sequence": expected_sequence,
            "policy_buffer_cap": policy_buffer_cap,
            "book_seq": book_seq,
            "snapshot_seq": snapshot_seq,
            "reason_class": reason_class,
            "policy_bridge_mode": policy.bridge_mode.value,
        }
        payload.update(extra)
        return payload

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
