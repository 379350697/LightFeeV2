"""Local-L2 runtime service — assignment, budget, events, metrics.

Rust V1 reference: src/execution_core/local_l2_runtime.rs
                      src/execution_core/local_l2_runtime_decision.rs
                      src/execution_core/local_l2_targeting.rs

Responsibilities:
  - Manage LocalL2Book collection with assignment pools
  - Scheduler-owned pool assignments
  - Pending events queue with bounded drain
  - Runtime fault handling: rate-limited, transport failure, checksum/sequence/age
  - Metrics counters matching Rust V1 naming
  - sync() refreshes assignments, drains events, updates metrics
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto

from lightfee.marketdata.l2 import (
    L2BookStatus,
    L2PoolAssignment,
    LocalL2Book,
    LocalL2BookKey,
    LocalL2Event,
    LocalL2EventKind,
    LocalL2Update,
    LocalL2UpdateKind,
    LocalL2UpdateResult,
    raw_checksum_levels_valid,
)


# ---------------------------------------------------------------------------
# Runtime fault classification
# ---------------------------------------------------------------------------


class RuntimeFaultKind(Enum):
    TRANSPORT_FAILURE = auto()
    RATE_LIMITED = auto()
    CHECKSUM_MISMATCH = auto()
    SEQUENCE_GAP = auto()
    DATA_INTEGRITY = auto()
    QUOTE_AGE_TRIGGERED = auto()
    RESUME_EXPIRED = auto()
    BUDGET_SUSPENDED = auto()
    RUNTIME_SUSPENDED = auto()


def _runtime_fault_for_rebuild_reason(reason: str) -> RuntimeFaultKind:
    """Classify invalid market-data content separately from transport gaps."""
    normalized = str(reason or "").lower()
    if "checksum" in normalized:
        return RuntimeFaultKind.CHECKSUM_MISMATCH
    if normalized.startswith(("invalid_", "nonfinite_", "book_")):
        return RuntimeFaultKind.DATA_INTEGRITY
    return RuntimeFaultKind.SEQUENCE_GAP


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


@dataclass
class LocalL2RuntimeMetrics:
    rebuild_total: int = 0
    resume_expired_total: int = 0
    fallback_total: int = 0
    budget_suspended_total: int = 0
    runtime_suspended_total: int = 0
    runtime_rate_limited_total: int = 0
    runtime_transport_failure_total: int = 0
    data_integrity_rebuild_total: int = 0
    assignment_empty_total: int = 0
    maker_event_lane_wake_total: int = 0
    active_books: int = 0
    retained_books: int = 0
    bootstrapping_books: int = 0
    rebuilding_books: int = 0
    resume_waiting_books: int = 0
    budget_suspended_books: int = 0
    runtime_suspended_books: int = 0
    hot_exec_not_ready_books: int = 0


# ---------------------------------------------------------------------------
# Local L2 Runtime
# ---------------------------------------------------------------------------


@dataclass
class LocalL2Runtime:
    books: dict[LocalL2BookKey, LocalL2Book] = field(default_factory=dict)
    assignments: dict[LocalL2BookKey, L2PoolAssignment] = field(default_factory=dict)
    pending_events: deque[LocalL2Event] = field(default_factory=deque)
    metrics: LocalL2RuntimeMetrics = field(default_factory=LocalL2RuntimeMetrics)

    # Config
    max_events: int = 512
    max_hot_exec: int = 16
    max_warm: int = 32
    resume_timeout_ms: int = 60_000

    # ------------------------------------------------------------------
    # Book management
    # ------------------------------------------------------------------

    def get_book(self, venue: str, symbol: str) -> LocalL2Book | None:
        return self.books.get(LocalL2BookKey(venue=venue, symbol=symbol))

    def ensure_book(self, venue: str, symbol: str) -> LocalL2Book:
        key = LocalL2BookKey(venue=venue, symbol=symbol)
        if key not in self.books:
            book = LocalL2Book(venue=venue, symbol=symbol)
            # Apply venue rules defaults
            from lightfee.marketdata.local_l2_venues import get_venue_rules
            rules = get_venue_rules(venue)
            book.max_depth = rules.default_depth
            book.max_sequence_gap = rules.max_sequence_gap
            self.books[key] = book
            self.assignments[key] = L2PoolAssignment.DROPPED
        return self.books[key]

    def record_update(
        self, update: LocalL2Update, now_ms: int
    ) -> list[LocalL2Event]:
        return self.record_update_result(update, now_ms).events

    def record_update_result(
        self, update: LocalL2Update, now_ms: int
    ) -> LocalL2UpdateResult:
        """Apply a raw L2 update to the matching book, applying venue rules.

        Venue-specific checksum verification and sequence gap thresholds are
        applied from the per-venue rules profile.
        """
        book = self.ensure_book(update.venue, update.symbol)
        if (
            (update.raw_bids or update.raw_asks)
            and not raw_checksum_levels_valid(
                update.raw_bids,
                update.raw_asks,
                allow_zero_quantity=update.update_kind == LocalL2UpdateKind.DELTA,
            )
        ):
            return self._mark_sequence_boundary_rebuild(
                book,
                update,
                now_ms,
                "invalid_raw_checksum_level",
                fault=RuntimeFaultKind.DATA_INTEGRITY,
            )
        preflight = self._preflight_venue_update(book, update, now_ms)
        if preflight is not None:
            for event in preflight.events:
                self._enqueue_event(event)
            return preflight

        result = self._apply_update(book, update, now_ms)
        if result.applied and not result.rebuild_required:
            self._update_raw_checksum_book(book, update)

        # Venue rules: checksum verification after apply
        from lightfee.marketdata.local_l2_venues import get_venue_rules
        rules = get_venue_rules(update.venue)
        if rules.should_verify_checksum() and update.checksum != 0:
            if update.raw_bids or update.raw_asks or book.raw_checksum_bids or book.raw_checksum_asks:
                checksum_result = book.verify_raw_checksum(update.checksum, now_ms)
            else:
                checksum_result = book.verify_checksum(update.checksum, now_ms)
            result.events.extend(checksum_result.events)
            if checksum_result.fault_reason:
                book.raw_checksum_bids.clear()
                book.raw_checksum_asks.clear()
                book.pending_snapshot_bridge = False
                checksum_result.rebuild_required = True
                result = checksum_result

        if update.venue == "bitget" and result.applied and not result.rebuild_required:
            book.pending_snapshot_bridge = (
                update.update_kind == LocalL2UpdateKind.SNAPSHOT
                and update.sequence > 0
            )

        for event in result.events:
            self._enqueue_event(event)

        # Trigger rebuild on sequence gap or checksum mismatch
        if result.rebuild_required:
            book.transition_to_rebuilding(now_ms)
            reason = result.fault_reason or "rebuild_required"
            self.handle_runtime_failure(
                update.venue, update.symbol,
                _runtime_fault_for_rebuild_reason(reason),
                reason, now_ms,
            )

        return result

    def _preflight_venue_update(
        self,
        book: LocalL2Book,
        update: LocalL2Update,
        now_ms: int,
    ) -> LocalL2UpdateResult | None:
        if update.venue != "bitget" or update.update_kind != LocalL2UpdateKind.DELTA:
            return None
        if book.sequence <= 0 or update.sequence <= 0:
            return None

        if update.previous_sequence_present and update.previous_sequence == 0:
            return self._mark_sequence_boundary_rebuild(
                book,
                update,
                now_ms,
                "bitget_pseq_zero_snapshot_boundary",
            )

        if update.sequence <= book.sequence:
            return LocalL2UpdateResult(
                applied=False,
                events=[],
                fault_reason=(
                    f"stale_update prev={book.sequence} incoming_sequence={update.sequence}"
                ),
                rebuild_required=False,
            )

        if not update.previous_sequence_present:
            if self._bitget_missing_prev_sequence_admissible(book, update):
                return None
            return self._mark_sequence_boundary_rebuild(
                book,
                update,
                now_ms,
                "bitget_missing_prev_sequence",
            )

        if update.previous_sequence != book.sequence:
            snapshot_bridge_matches = (
                book.pending_snapshot_bridge
                and update.previous_sequence <= book.sequence <= update.sequence
            )
            if snapshot_bridge_matches:
                return None
            return self._mark_sequence_boundary_rebuild(
                book,
                update,
                now_ms,
                "bitget_previous_link_mismatch",
            )

        return None

    def _bitget_missing_prev_sequence_admissible(
        self,
        book: LocalL2Book,
        update: LocalL2Update,
    ) -> bool:
        if update.checksum == 0 or update.sequence <= book.sequence:
            return False
        if not (book.pending_snapshot_bridge or book.status == L2BookStatus.HOT):
            return False
        if not (book.raw_checksum_bids or book.raw_checksum_asks):
            return False
        candidate = LocalL2Book(venue=book.venue, symbol=book.symbol)
        candidate.raw_checksum_bids = list(book.raw_checksum_bids)
        candidate.raw_checksum_asks = list(book.raw_checksum_asks)
        if not candidate.apply_raw_checksum_update(
            update.raw_bids,
            update.raw_asks,
            LocalL2UpdateKind.DELTA,
        ):
            return False
        return candidate.compute_raw_checksum() == update.checksum

    def _mark_sequence_boundary_rebuild(
        self,
        book: LocalL2Book,
        update: LocalL2Update,
        now_ms: int,
        reason: str,
        *,
        fault: RuntimeFaultKind = RuntimeFaultKind.SEQUENCE_GAP,
    ) -> LocalL2UpdateResult:
        book.bids.clear()
        book.asks.clear()
        book.sequence = 0
        book.last_update_id = 0
        book.raw_checksum_bids.clear()
        book.raw_checksum_asks.clear()
        book.pending_snapshot_bridge = False
        book.fault_reason = reason
        book.transition_to_rebuilding(now_ms)
        self.handle_runtime_failure(
            update.venue,
            update.symbol,
            fault,
            reason,
            now_ms,
        )
        return LocalL2UpdateResult(
            applied=False,
            events=[],
            fault_reason=reason,
            rebuild_required=True,
        )

    @staticmethod
    def _update_raw_checksum_book(book: LocalL2Book, update: LocalL2Update) -> bool:
        if update.raw_bids or update.raw_asks:
            return book.apply_raw_checksum_update(
                update.raw_bids,
                update.raw_asks,
                update.update_kind,
            )
        return True

    def _apply_update(
        self, book: LocalL2Book, update: LocalL2Update, now_ms: int
    ):
        """Apply and produce result with events.

        observed_at_ms uses the local receipt timestamp. Exchange event
        timestamps can move backward/forward relative to the process clock and
        must not make HOT freshness look newer than locally observed evidence.
        """
        effective_ms = update.received_at_ms if update.received_at_ms > 0 else now_ms
        if update.update_kind == LocalL2UpdateKind.SNAPSHOT:
            return book.apply_snapshot(
                update.bids, update.asks,
                sequence=update.sequence,
                checksum=update.checksum,
                now_ms=effective_ms,
            )
        else:
            return book.apply_delta(
                update.bids, update.asks,
                sequence=update.sequence,
                previous_sequence=update.previous_sequence,
                now_ms=effective_ms,
            )

    def remove_book(self, venue: str, symbol: str) -> None:
        key = LocalL2BookKey(venue=venue, symbol=symbol)
        self.books.pop(key, None)
        self.assignments.pop(key, None)

    def prune_untracked_books(
        self,
        tracked: set[LocalL2BookKey],
        now_ms: int,
        *,
        retained_max_age_ms: int = 300_000,
        retained_global_limit: int = 128,
        retained_per_venue_limit: int = 32,
    ) -> list[dict]:
        """Prune untracked DROPPED books and over-budget stale RETAINED books.

        Rust V1 parity: dropped books are not a passive cache. Once a book is
        outside the tracked execution/scan set, V1 removes it instead of letting
        stale monitoring rebuild it forever.
        """
        tracked = set(tracked or set())
        prune_reasons: dict[LocalL2BookKey, str] = {}
        retained_candidates: list[tuple[int, str, str, LocalL2BookKey]] = []

        for key, book in list(self.books.items()):
            if key in tracked:
                continue

            pool = self.assignments.get(key, getattr(book, "pool", L2PoolAssignment.DROPPED))
            if pool == L2PoolAssignment.DROPPED:
                prune_reasons[key] = "dropped_untracked"
                continue

            if pool != L2PoolAssignment.RETAINED:
                continue

            observed_at_ms = int(getattr(book, "observed_at_ms", 0) or 0)
            age_ms = now_ms - observed_at_ms if observed_at_ms > 0 else retained_max_age_ms + 1
            if age_ms > retained_max_age_ms:
                prune_reasons[key] = "retained_expired"
                continue

            retained_candidates.append((observed_at_ms, key.venue, key.symbol, key))

        retained_candidates.sort(key=lambda item: (-item[0], item[1], item[2]))
        retained_global_count = 0
        retained_venue_counts: dict[str, int] = {}
        for _observed, venue, _symbol, key in retained_candidates:
            venue_count = retained_venue_counts.get(venue, 0)
            if retained_global_count < retained_global_limit and venue_count < retained_per_venue_limit:
                retained_global_count += 1
                retained_venue_counts[venue] = venue_count + 1
                continue
            prune_reasons[key] = "retained_over_budget"

        pruned: list[dict] = []
        for key in sorted(prune_reasons, key=lambda item: (item.venue, item.symbol)):
            reason = prune_reasons[key]
            self.remove_book(key.venue, key.symbol)
            pruned.append({
                "venue": key.venue,
                "symbol": key.symbol,
                "reason": reason,
            })
        return pruned

    # ------------------------------------------------------------------
    # Assignment semantics
    # ------------------------------------------------------------------

    def assign(
        self, venue: str, symbol: str, pool: L2PoolAssignment,
        now_ms: int = 0, priority: int = 0,
    ) -> None:
        """Assign a symbol to a pool until the scheduler changes its scope.

        V1's opt-in 90-second lease belongs to unready *primary candidates*
        in the entry selector. A Local-L2 book must not independently expire,
        since that would silently override the scheduler's current assignment.
        """
        key = LocalL2BookKey(venue=venue, symbol=symbol)
        self.assignments[key] = pool
        book = self.books.get(key)
        if book is not None:
            book.pool = pool

    def get_assignment(self, venue: str, symbol: str) -> L2PoolAssignment:
        return self.assignments.get(
            LocalL2BookKey(venue=venue, symbol=symbol), L2PoolAssignment.DROPPED
        )

    def hot_exec_symbols(self) -> list[LocalL2BookKey]:
        return [
            key for key, a in self.assignments.items()
            if a == L2PoolAssignment.HOT_EXEC
        ]

    # ------------------------------------------------------------------
    # Events queue
    # ------------------------------------------------------------------

    def _enqueue_event(self, event: LocalL2Event) -> None:
        self.pending_events.append(event)
        # Bounded queue: drop oldest when overflow
        while len(self.pending_events) > self.max_events:
            self.pending_events.popleft()

    def drain_events(self, limit: int = 0) -> list[LocalL2Event]:
        """Drain pending events up to limit (0 = all)."""
        if limit <= 0:
            drained = list(self.pending_events)
            self.pending_events.clear()
            return drained
        drained = []
        for _ in range(min(limit, len(self.pending_events))):
            drained.append(self.pending_events.popleft())
        return drained

    def event_count(self) -> int:
        return len(self.pending_events)

    # ------------------------------------------------------------------
    # Runtime fault handling
    # ------------------------------------------------------------------

    def handle_runtime_failure(
        self, venue: str, symbol: str, fault: RuntimeFaultKind, detail: str, now_ms: int
    ) -> None:
        """Record a runtime fault and update book status + metrics.

        V1 parity: every fault event carries a specific fault detail that
        must be written to book.fault_reason so diagnostics and entry
        session arming can derive the correct arming_reason.
        """
        book = self.ensure_book(venue, symbol)

        if fault == RuntimeFaultKind.RATE_LIMITED:
            self.metrics.runtime_rate_limited_total += 1
            book.fault_reason = f"rate_limited: {detail}"
        elif fault == RuntimeFaultKind.TRANSPORT_FAILURE:
            self.metrics.runtime_transport_failure_total += 1
            book.fault_reason = f"transport_failure: {detail}"
        elif fault == RuntimeFaultKind.CHECKSUM_MISMATCH:
            self.metrics.rebuild_total += 1
            book.fault_reason = f"checksum_mismatch: {detail}"
        elif fault == RuntimeFaultKind.SEQUENCE_GAP:
            self.metrics.rebuild_total += 1
            book.fault_reason = f"sequence_gap: {detail}"
        elif fault == RuntimeFaultKind.DATA_INTEGRITY:
            self.metrics.rebuild_total += 1
            self.metrics.data_integrity_rebuild_total += 1
            book.fault_reason = f"data_integrity: {detail}"
        elif fault == RuntimeFaultKind.QUOTE_AGE_TRIGGERED:
            book.transition_to_degraded(f"quote_age: {detail}")
        elif fault == RuntimeFaultKind.RESUME_EXPIRED:
            self.metrics.resume_expired_total += 1
            book.fault_reason = f"resume_expired: {detail}"
        elif fault == RuntimeFaultKind.BUDGET_SUSPENDED:
            self.metrics.budget_suspended_total += 1
            book.transition_to_suspended("budget")
        elif fault == RuntimeFaultKind.RUNTIME_SUSPENDED:
            self.metrics.runtime_suspended_total += 1
            book.runtime_suspended_until_ms = now_ms + self.resume_timeout_ms
            book.transition_to_suspended("runtime")

    def apply_fallback(self, venue: str, symbol: str, reason: str) -> None:
        """Mark a book as fallen back to TOP_BOOK or CACHED source."""
        book = self.ensure_book(venue, symbol)
        book.source = reason
        self.metrics.fallback_total += 1

    # ------------------------------------------------------------------
    # Sync: refresh assignments and metrics
    # ------------------------------------------------------------------

    def sync(self, now_ms: int) -> list[LocalL2Event]:
        """Periodic sync: refresh metrics and drain relevant events.

        Returns events that may wake the maker-event lane.
        """
        # Resume waiting: check if any books can resume
        for key, book in list(self.books.items()):
            if book.status == L2BookStatus.RESUME_WAITING:
                if book.resume_waiting_remaining_ms(now_ms) == 0:
                    book.transition_to_bootstrapping(now_ms)
                    self._enqueue_event(LocalL2Event(
                        venue=book.venue, symbol=book.symbol,
                        event_kind=LocalL2EventKind.RESUMED,
                        observed_at_ms=now_ms, detail="resume_waiting_complete",
                    ))

        # Refresh metrics
        self._refresh_metrics(now_ms)

        # Drain and return maker-relevant events
        return self.drain_events(limit=64)

    def _refresh_metrics(self, now_ms: int) -> None:
        """Refresh runtime metrics counters — mirrors Rust refresh_local_l2_metrics."""
        self.metrics.active_books = 0
        self.metrics.retained_books = 0
        self.metrics.bootstrapping_books = 0
        self.metrics.rebuilding_books = 0
        self.metrics.resume_waiting_books = 0
        self.metrics.budget_suspended_books = 0
        self.metrics.runtime_suspended_books = 0
        self.metrics.hot_exec_not_ready_books = 0

        for book in self.books.values():
            if book.status == L2BookStatus.BOOTSTRAPPING:
                self.metrics.bootstrapping_books += 1
            elif book.status == L2BookStatus.REBUILDING:
                self.metrics.rebuilding_books += 1
            elif book.status == L2BookStatus.RESUME_WAITING:
                self.metrics.resume_waiting_books += 1

            if book.pool == L2PoolAssignment.RETAINED:
                self.metrics.retained_books += 1
            elif book.pool in (L2PoolAssignment.HOT_EXEC, L2PoolAssignment.WARM):
                if book.status == L2BookStatus.HOT:
                    self.metrics.active_books += 1
                else:
                    self.metrics.hot_exec_not_ready_books += 1

            if book.status == L2BookStatus.SUSPENDED:
                if "budget" in (book.fault_reason or ""):
                    self.metrics.budget_suspended_books += 1
                else:
                    self.metrics.runtime_suspended_books += 1

    # ------------------------------------------------------------------
    # Diagnostics snapshot
    # ------------------------------------------------------------------

    def diagnostics_snapshot(self) -> dict:
        return {
            "book_count": len(self.books),
            "assignment_count": len(self.assignments),
            "pending_event_count": len(self.pending_events),
            "active_books": self.metrics.active_books,
            "retained_books": self.metrics.retained_books,
            "bootstrapping_books": self.metrics.bootstrapping_books,
            "rebuilding_books": self.metrics.rebuilding_books,
            "resume_waiting_books": self.metrics.resume_waiting_books,
            "budget_suspended_books": self.metrics.budget_suspended_books,
            "runtime_suspended_books": self.metrics.runtime_suspended_books,
            "hot_exec_not_ready_books": self.metrics.hot_exec_not_ready_books,
            "rebuild_total": self.metrics.rebuild_total,
            "data_integrity_rebuild_total": self.metrics.data_integrity_rebuild_total,
            "fallback_total": self.metrics.fallback_total,
        }
