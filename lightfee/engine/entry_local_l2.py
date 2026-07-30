"""Entry local-L2 sessions — tracked opportunities, readiness, promotion/demotion.

Rust V1 references:
  - src/execution_core/entry_local_l2.rs (tracked candidates, selection)
  - src/execution_core/entry_local_l2_sessions.rs (session state, leg state, arming)

Key concepts:
  - TrackedOpportunity: primary/shadow pair with pair_id, symbol, venues, ranking edge
  - EntryLocalL2LegSession: per-venue/symbol leg state (arming/ready/faulted)
  - EntryLocalL2Session: per-opportunity pair session (arming/ready/faulted/closed)
  - Session refresh: prewarm window, quiet book grace, readiness downgrade
  - Shadow promotion / primary demotion based on ranking edge delta and hold windows
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional


LocalL2StaleAfter = int | Callable[[str], int]


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class TrackedOpportunityClass(Enum):
    PRIMARY = "primary_tracked"
    SHADOW = "shadow_tracked"


class EntryLocalL2SessionState(Enum):
    ARMING = "arming"
    READY = "ready"
    FAULTED = "faulted"
    CLOSED = "closed"


class EntryLocalL2LegState(Enum):
    ARMING = "arming"
    READY = "ready"
    FAULTED = "faulted"


class SessionArmingReason(Enum):
    FIRST_SESSION = "session_arming_first"
    SEQUENCE_GAP = "session_arming_sequence_gap"
    STALE_BOOK_RECOVERY = "session_arming_stale_recovery"
    TRANSPORT_FAULT_RECOVERY = "session_arming_transport_fault_recovery"
    BOOK_STATUS_TRANSITION = "session_arming_book_status_transition"


class EntryLocalL2LegFault(Enum):
    GATE_OBU_GAP = "gate_obu_gap"
    OKX_PREV_SEQ_MISMATCH = "okx_prev_seq_mismatch"
    OKX_CHECKSUM_MISMATCH = "okx_checksum_mismatch"
    HYPERLIQUID_DISCONNECT = "hyperliquid_disconnect"
    CROSSED_OR_LOCKED_BOOK = "crossed_or_locked_book"
    STALE_BOOK = "stale_book"
    RUNTIME_SUSPENDED = "runtime_suspended"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class TrackedOpportunity:
    pair_id: str
    symbol: str
    long_venue: str
    short_venue: str
    ranking_edge_bps: float
    class_: TrackedOpportunityClass = TrackedOpportunityClass.SHADOW


@dataclass
class EntryLocalL2LegSession:
    venue: str
    symbol: str
    state: EntryLocalL2LegState = EntryLocalL2LegState.ARMING
    last_seen_at_ms: int = 0
    fault: Optional[EntryLocalL2LegFault] = None
    fault_detail: str = ""
    arming_reason: Optional[SessionArmingReason] = None

    def is_stale(self, now_ms: int, stale_after_ms: int) -> bool:
        if stale_after_ms <= 0:
            return False
        if self.last_seen_at_ms <= 0:
            return True
        return (now_ms - self.last_seen_at_ms) > stale_after_ms

    def is_ready(self, now_ms: int, stale_after_ms: int) -> bool:
        return (
            self.state == EntryLocalL2LegState.READY
            and self.fault is None
            and not self.is_stale(now_ms, stale_after_ms)
        )

    def mark_ready(self, seen_at_ms: int) -> None:
        self.state = EntryLocalL2LegState.READY
        self.last_seen_at_ms = seen_at_ms
        self.fault = None
        self.fault_detail = ""
        self.arming_reason = None

    def mark_faulted(
        self, fault: EntryLocalL2LegFault, detail: str = "", seen_at_ms: int = 0
    ) -> None:
        self.state = EntryLocalL2LegState.FAULTED
        self.fault = fault
        self.fault_detail = detail
        if seen_at_ms > 0:
            self.last_seen_at_ms = seen_at_ms

    def mark_arming(self, reason: SessionArmingReason) -> None:
        self.state = EntryLocalL2LegState.ARMING
        self.arming_reason = reason
        self.fault = None
        self.fault_detail = ""


@dataclass
class EntryLocalL2Session:
    pair_id: str
    state: EntryLocalL2SessionState = EntryLocalL2SessionState.ARMING
    legs: dict[str, EntryLocalL2LegSession] = field(default_factory=dict)  # venue -> leg
    primary_assigned_at_ms: int = 0
    shadow_promoted_at_ms: int = 0

    def leg_for(self, venue: str) -> Optional[EntryLocalL2LegSession]:
        return self.legs.get(venue)

    def ensure_leg(self, venue: str, symbol: str) -> EntryLocalL2LegSession:
        leg = self.legs.get(venue)
        if leg is None:
            leg = EntryLocalL2LegSession(venue=venue, symbol=symbol)
            self.legs[venue] = leg
            return leg

        # V1 ``ensure_candidate`` always refreshes the symbol binding and
        # converts a retained faulted leg back to arming when its primary
        # candidate returns.  The next local-book observation must establish
        # readiness again; preserving the old fault would make a recovered
        # sticky session permanently fail closed.
        leg.symbol = symbol
        if leg.state == EntryLocalL2LegState.FAULTED:
            if leg.fault == EntryLocalL2LegFault.STALE_BOOK:
                arming_reason = SessionArmingReason.STALE_BOOK_RECOVERY
            elif leg.fault in {
                EntryLocalL2LegFault.GATE_OBU_GAP,
                EntryLocalL2LegFault.OKX_PREV_SEQ_MISMATCH,
                EntryLocalL2LegFault.OKX_CHECKSUM_MISMATCH,
            }:
                arming_reason = SessionArmingReason.SEQUENCE_GAP
            elif leg.fault in {
                EntryLocalL2LegFault.HYPERLIQUID_DISCONNECT,
                EntryLocalL2LegFault.RUNTIME_SUSPENDED,
            }:
                arming_reason = SessionArmingReason.TRANSPORT_FAULT_RECOVERY
            elif leg.fault == EntryLocalL2LegFault.CROSSED_OR_LOCKED_BOOK:
                arming_reason = SessionArmingReason.BOOK_STATUS_TRANSITION
            else:
                arming_reason = SessionArmingReason.FIRST_SESSION
            leg.mark_arming(arming_reason)
        return leg

    @staticmethod
    def _stale_after_ms_for_leg(
        stale_after_ms: LocalL2StaleAfter,
        leg: EntryLocalL2LegSession,
    ) -> int:
        value = stale_after_ms(leg.venue) if callable(stale_after_ms) else stale_after_ms
        try:
            return max(int(value), 1)
        except (TypeError, ValueError, OverflowError):
            return 1

    def both_legs_ready(self, now_ms: int, stale_after_ms: LocalL2StaleAfter) -> bool:
        if len(self.legs) < 2:
            return False
        return all(
            leg.is_ready(now_ms, self._stale_after_ms_for_leg(stale_after_ms, leg))
            for leg in self.legs.values()
        )

    def ready_leg_count(self, now_ms: int, stale_after_ms: LocalL2StaleAfter) -> int:
        return sum(
            1
            for leg in self.legs.values()
            if leg.is_ready(now_ms, self._stale_after_ms_for_leg(stale_after_ms, leg))
        )

    def faulted_leg_count(self) -> int:
        return sum(1 for leg in self.legs.values() if leg.state == EntryLocalL2LegState.FAULTED)

    def stale_leg_count(self, now_ms: int, stale_after_ms: LocalL2StaleAfter) -> int:
        return sum(
            1
            for leg in self.legs.values()
            if leg.is_stale(now_ms, self._stale_after_ms_for_leg(stale_after_ms, leg))
        )

    def refresh_state(self, now_ms: int, stale_after_ms: LocalL2StaleAfter) -> None:
        """Recompute session state from leg states."""
        if self.state == EntryLocalL2SessionState.CLOSED:
            return
        if self.both_legs_ready(now_ms, stale_after_ms):
            self.state = EntryLocalL2SessionState.READY
        elif any(leg.state == EntryLocalL2LegState.FAULTED for leg in self.legs.values()):
            # V1: ANY leg faulted → session FAULTED (entry_local_l2_sessions.rs:286-291)
            self.state = EntryLocalL2SessionState.FAULTED
        else:
            self.state = EntryLocalL2SessionState.ARMING

    def diagnostics_snapshot(self, now_ms: int, stale_after_ms: LocalL2StaleAfter) -> dict:
        return {
            "pair_id": self.pair_id,
            "state": self.state.value,
            "dual_ready": self.both_legs_ready(now_ms, stale_after_ms),
            "ready_leg_count": self.ready_leg_count(now_ms, stale_after_ms),
            "faulted_leg_count": self.faulted_leg_count(),
            "stale_leg_count": self.stale_leg_count(now_ms, stale_after_ms),
            "leg_count": len(self.legs),
        }


def _derive_arming_reason_from_book(book, status_value: str) -> SessionArmingReason:
    """Derive a specific arming reason from the book's fault context.

    V1 parity: ensure_candidate() maps prior leg fault → arming_reason:
      - StaleBook          → StaleBookRecovery
      - GateObuGap / OkxPrevSeqMismatch / OkxChecksumMismatch → SequenceGap
      - HyperliquidDisconnect → TransportFaultRecovery
      - CrossedOrLockedBook → BookStatusTransition
      - None               → FirstSession

    V2 derives from book.fault_reason keywords because the entry session
    leg doesn't carry V1's EntryLocalL2LegFault enum at this layer.
    """
    fault = str(getattr(book, "fault_reason", "") or "").lower()

    # COLD book with no prior fault → first session
    if status_value == "cold" and not fault:
        return SessionArmingReason.FIRST_SESSION

    if not fault:
        # No fault context at all — first time arming
        if status_value == "cold":
            return SessionArmingReason.FIRST_SESSION
        return SessionArmingReason.BOOK_STATUS_TRANSITION

    # Stale-related faults → StaleBookRecovery
    # V1: QuoteAgeTriggered, IdleTimeout, ResumeWindowExpired → StaleBook → StaleBookRecovery
    if any(kw in fault for kw in ("stale", "quote_age", "idle", "resume")):
        return SessionArmingReason.STALE_BOOK_RECOVERY

    # Sequence/checksum faults → SequenceGap
    if any(kw in fault for kw in ("sequence_gap", "checksum", "prev_seq", "obu_gap", "previous_link_mismatch")):
        return SessionArmingReason.SEQUENCE_GAP

    # Transport/connection faults → TransportFaultRecovery
    if any(kw in fault for kw in ("transport", "connection", "disconnect", "stream", "timeout", "snapshot_bootstrap", "suspended", "runtime")):
        return SessionArmingReason.TRANSPORT_FAULT_RECOVERY

    # Crossed/locked book or other book structure faults → BookStatusTransition
    if any(kw in fault for kw in ("crossed", "locked", "non_positive", "buffer_overflow")):
        return SessionArmingReason.BOOK_STATUS_TRANSITION

    return SessionArmingReason.BOOK_STATUS_TRANSITION


def apply_book_readiness_to_leg(leg, book, now_ms, stale_after_ms):
    """Apply local-L2 book readiness to one entry-L2 leg and return diagnostics."""
    venue = getattr(leg, "venue", "")
    symbol = getattr(leg, "symbol", "")
    if book is None:
        leg.mark_arming(SessionArmingReason.FIRST_SESSION)
        return {
            "venue": venue,
            "symbol": symbol,
            "ready": False,
            "reason": "book_missing",
            "detail": "book not found",
            "book_status": "missing",
            "age_ms": None,
            "observed_at_ms": 0,
            "sequence": 0,
        }

    status_value = (
        book.status.value if hasattr(getattr(book, "status", None), "value")
        else str(getattr(book, "status", "unknown"))
    )
    observed_at_ms = int(getattr(book, "observed_at_ms", 0) or 0)
    sequence = int(getattr(book, "sequence", 0) or 0)
    age_ms = book.age_ms(now_ms) if hasattr(book, "age_ms") else (
        now_ms - observed_at_ms if observed_at_ms > 0 else 0
    )
    diag = {
        "venue": str(getattr(book, "venue", venue)),
        "symbol": str(getattr(book, "symbol", symbol)),
        "ready": False,
        "reason": "",
        "detail": "",
        "book_status": status_value,
        "age_ms": age_ms,
        "observed_at_ms": observed_at_ms,
        "sequence": sequence,
    }

    arming_statuses = {"bootstrapping", "cold", "rebuilding", "resume_waiting"}
    faulted_statuses = {"degraded", "suspended"}

    if status_value in arming_statuses:
        # V1 parity: derive specific arming_reason from book's prior fault
        # context, mirroring V1 ensure_candidate() fault→arming_reason mapping.
        arming_reason = _derive_arming_reason_from_book(book, status_value)
        leg.mark_arming(arming_reason)
        fault_ctx = str(getattr(book, "fault_reason", "") or "")
        diag["reason"] = f"book_{status_value}"
        diag["detail"] = (
            f"book_status={status_value}"
            if not fault_ctx
            else f"book_status={status_value}:{fault_ctx}"
        )
        return diag

    if status_value in faulted_statuses:
        detail = (
            str(getattr(book, "fault_reason", "") or "")
            or str(getattr(book, "last_error", "") or "")
            or f"book_status={status_value}"
        )
        leg.mark_faulted(
            EntryLocalL2LegFault.RUNTIME_SUSPENDED,
            detail,
            seen_at_ms=observed_at_ms,
        )
        diag["reason"] = f"book_{status_value}"
        diag["detail"] = detail
        return diag

    # Timestamp missing is a distinct reason from stale
    if observed_at_ms <= 0:
        leg.mark_faulted(
            EntryLocalL2LegFault.STALE_BOOK,
            "book_timestamp_missing",
            seen_at_ms=0,
        )
        diag["reason"] = "book_timestamp_missing"
        diag["detail"] = "book_timestamp_missing"
        return diag

    # V1: staleness check — use age_ms if is_stale not available
    is_stale_fn = getattr(book, "is_stale", None)
    if stale_after_ms > 0 and (
        (callable(is_stale_fn) and is_stale_fn(stale_after_ms, now_ms))
        or (not callable(is_stale_fn) and age_ms > stale_after_ms)
    ):
        detail = f"age_ms={age_ms} stale_after_ms={stale_after_ms}"
        leg.mark_faulted(
            EntryLocalL2LegFault.STALE_BOOK,
            detail,
            seen_at_ms=observed_at_ms,
        )
        diag["reason"] = "stale_book"
        diag["detail"] = detail
        return diag

    # V1: crossed book detection — fall back to bid/ask comparison
    has_crossed_fn = getattr(book, "has_crossed_book", None)
    is_crossed = has_crossed_fn() if callable(has_crossed_fn) else False
    if not is_crossed:
        # Fallback: check bid/ask directly
        bid = getattr(book, "best_bid", lambda: 0.0)()
        ask = getattr(book, "best_ask", lambda: float("inf"))()
        is_crossed = bid > ask > 0
    if is_crossed:
        detail = f"best_bid={getattr(book, 'best_bid', lambda: 0.0)()} best_ask={getattr(book, 'best_ask', lambda: 0.0)()}"
        leg.mark_faulted(
            EntryLocalL2LegFault.CROSSED_OR_LOCKED_BOOK,
            detail,
            seen_at_ms=observed_at_ms,
        )
        diag["reason"] = "crossed_or_locked_book"
        diag["detail"] = detail
        return diag

    if status_value == "hot":
        # HOT book — readiness determined by bid/ask and timestamp (already
        # checked above).  V1 parity: a healthy HOT book must clear any prior
        # fault; fault_reason is only meaningful for non-HOT statuses.
        bid_fn = getattr(book, "best_bid", None)
        ask_fn = getattr(book, "best_ask", None)
        bid = bid_fn() if callable(bid_fn) else 1.0
        ask = ask_fn() if callable(ask_fn) else 1.0
        if bid <= 0 or ask <= 0:
            side = "bid" if bid <= 0 else "ask"
            leg.mark_faulted(
                EntryLocalL2LegFault.STALE_BOOK,
                f"book_empty_side_{side}",
                seen_at_ms=observed_at_ms,
            )
            diag["reason"] = "book_empty_side"
            diag["detail"] = f"book_empty_side_{side}"
            return diag
        leg.mark_ready(observed_at_ms)
        diag["ready"] = True
        diag["reason"] = "ready"
        diag["detail"] = "local_l2_book_hot_fresh"
        return diag

    runtime_fault = str(getattr(book, "fault_reason", "") or "")
    if runtime_fault:
        leg.mark_faulted(
            EntryLocalL2LegFault.RUNTIME_SUSPENDED,
            runtime_fault,
            seen_at_ms=observed_at_ms,
        )
        diag["reason"] = f"book_{status_value}"
        diag["detail"] = runtime_fault
        return diag

    leg.mark_faulted(
        EntryLocalL2LegFault.RUNTIME_SUSPENDED,
        f"book_status={status_value}",
        seen_at_ms=observed_at_ms,
    )
    diag["reason"] = f"book_{status_value}"
    diag["detail"] = f"book_status={status_value}"
    return diag


def local_l2_tracking_book_ready(book, now_ms: int, stale_after_ms: int) -> bool:
    """Return V1 shadow-tracking readiness without creating a pair session.

    V1 evaluates a shadow directly from an assigned ``HOT_EXEC`` or ``WARM``
    book; shadow candidates never own an ``EntryLocalL2Session``.  Reuse the
    exact one-leg readiness transition on a disposable leg so primary-session
    and shadow-promotion decisions cannot drift on status, staleness, crossed
    book, or empty-side handling.
    """
    if book is None:
        return False
    pool = getattr(getattr(book, "pool", None), "value", getattr(book, "pool", ""))
    if str(pool) not in {"hot_exec", "warm"}:
        return False
    probe = EntryLocalL2LegSession(
        venue=str(getattr(book, "venue", "") or ""),
        symbol=str(getattr(book, "symbol", "") or ""),
    )
    return bool(
        apply_book_readiness_to_leg(
            probe,
            book,
            now_ms=now_ms,
            stale_after_ms=stale_after_ms,
        ).get("ready")
    )


# ---------------------------------------------------------------------------
# Session runtime
# ---------------------------------------------------------------------------


@dataclass
class EntryLocalL2SessionRuntime:
    sessions: dict[str, EntryLocalL2Session] = field(default_factory=dict)  # pair_id → session
    sticky_pair_ids: set[str] = field(default_factory=set)

    def get_or_create_session(self, pair_id: str) -> EntryLocalL2Session:
        if pair_id not in self.sessions:
            self.sessions[pair_id] = EntryLocalL2Session(pair_id=pair_id)
        return self.sessions[pair_id]

    def track_opportunity(
        self, opp: TrackedOpportunity, now_ms: int
    ) -> EntryLocalL2Session:
        """Create or update session legs for a tracked opportunity."""
        session = self.get_or_create_session(opp.pair_id)
        # V1 ``ensure_candidate`` revives a previously closed retained
        # session when the pair returns to the primary scope.  Without this,
        # a bounded sticky prewarm cache can keep a pair permanently closed
        # after a brief rank churn.
        if session.state == EntryLocalL2SessionState.CLOSED:
            session.state = EntryLocalL2SessionState.ARMING
        session.ensure_leg(opp.long_venue, opp.symbol)
        session.ensure_leg(opp.short_venue, opp.symbol)
        if (
            opp.class_ == TrackedOpportunityClass.PRIMARY
            and session.primary_assigned_at_ms <= 0
        ):
            session.primary_assigned_at_ms = now_ms
        return session

    def close_session(self, pair_id: str) -> None:
        session = self.sessions.get(pair_id)
        if session is not None:
            session.state = EntryLocalL2SessionState.CLOSED

    def mark_sticky_prewarm(self, pair_id: str) -> None:
        """Retain a bounded V1 primary prewarm cache across rank churn."""
        self.sticky_pair_ids.add(str(pair_id))
        # V1 uses a fixed 64-pair cap.  Sorting makes the deterministic Python
        # set eviction independent of hash iteration order.
        while len(self.sticky_pair_ids) > 64:
            self.sticky_pair_ids.discard(sorted(self.sticky_pair_ids)[0])

    def close_missing(self, active_pair_ids: set[str]) -> None:
        """Close and reclaim sessions outside the V1 active/sticky scope."""
        active_pair_ids = {str(pair_id) for pair_id in active_pair_ids}
        for pair_id, session in self.sessions.items():
            if (
                pair_id not in active_pair_ids
                and pair_id not in self.sticky_pair_ids
            ):
                session.state = EntryLocalL2SessionState.CLOSED
        self.sessions = {
            pair_id: session
            for pair_id, session in self.sessions.items()
            if (
                pair_id in active_pair_ids
                or pair_id in self.sticky_pair_ids
                or session.state != EntryLocalL2SessionState.CLOSED
            )
        }

    def remove_session(self, pair_id: str) -> None:
        self.sessions.pop(pair_id, None)
        self.sticky_pair_ids.discard(pair_id)


# ---------------------------------------------------------------------------
# Opportunity selection (mirrors Rust entry_local_l2.rs)
# ---------------------------------------------------------------------------


def make_candidate_pair_id(symbol: str, long_venue: str, short_venue: str) -> str:
    """Create a stable candidate pair id matching entry_sync/pending pair key.

    V1: CandidateOpportunity.pair_id is the stable identity used across
    tracked opportunities, sessions, and final gate.  The canonical form is:
        "{symbol.lower()}:{long_venue}->{short_venue}"
    """
    return f"{symbol.lower()}:{long_venue}->{short_venue}"


def select_tracked_opportunities(
    candidates: list,
    primary_count: int,
    shadow_count: int,
    *,
    primary_excluded_pair_ids: set[str] | None = None,
) -> list[TrackedOpportunity]:
    """Allocate a bounded L2 resource window from the complete frontier.

    Primary slots are symbol-unique so duplicate routes cannot monopolize the
    scarce HOT_EXEC pool.  Remaining ranked routes, including alternatives for
    an already-primary symbol, stay eligible for the shadow window.  A caller
    may temporarily exclude faulted or lease-expired routes from primary
    ownership without removing them from the complete opportunity frontier.
    """
    source_candidates = list(candidates or [])
    excluded = set(primary_excluded_pair_ids or set())
    normalized: list[tuple[object, str, str, str, str]] = []
    seen_pair_ids: set[str] = set()
    for candidate in source_candidates:
        symbol = str(getattr(candidate, "symbol", "") or "")
        long_venue = str(getattr(candidate, "long_venue", "") or "")
        short_venue = str(getattr(candidate, "short_venue", "") or "")
        pair_id = str(getattr(candidate, "pair_id", "") or "")
        if not pair_id:
            pair_id = make_candidate_pair_id(symbol, long_venue, short_venue)
        if not pair_id or pair_id in seen_pair_ids:
            continue
        seen_pair_ids.add(pair_id)
        normalized.append((candidate, pair_id, symbol, long_venue, short_venue))

    selected_primary_ids: set[str] = set()
    primary_symbols: set[str] = set()
    primary_rows: list[tuple[object, str, str, str, str]] = []
    for row in normalized:
        _candidate, pair_id, symbol, _long_venue, _short_venue = row
        symbol_key = symbol.upper()
        if pair_id in excluded or not symbol_key or symbol_key in primary_symbols:
            continue
        primary_rows.append(row)
        selected_primary_ids.add(pair_id)
        primary_symbols.add(symbol_key)
        if len(primary_rows) >= max(int(primary_count), 0):
            break

    shadow_rows: list[tuple[object, str, str, str, str]] = []
    for row in normalized:
        _candidate, pair_id, _symbol, _long_venue, _short_venue = row
        if pair_id in selected_primary_ids:
            continue
        shadow_rows.append(row)
        if len(shadow_rows) >= max(int(shadow_count), 0):
            break

    result: list[TrackedOpportunity] = []
    for class_, rows in (
        (TrackedOpportunityClass.PRIMARY, primary_rows),
        (TrackedOpportunityClass.SHADOW, shadow_rows),
    ):
        for candidate, pair_id, symbol, long_venue, short_venue in rows:
            result.append(TrackedOpportunity(
                pair_id=pair_id,
                symbol=symbol,
                long_venue=long_venue,
                short_venue=short_venue,
                ranking_edge_bps=getattr(candidate, "ranking_edge_bps", 0.0),
                class_=class_,
            ))
    return result


def primary_hold_window_allows_replacement(
    primary_assigned_at_ms: int, now_ms: int, primary_min_hold_ms: int
) -> bool:
    """V1 primary_hold_window_allows_replacement (entry_local_l2.rs:93-97).

    ``0`` is V2's persisted representation of V1's missing assignment time.
    V1 permits replacement in that case: there is no established hold to
    preserve.  A selected primary will receive its real assignment timestamp
    as soon as the runtime gives it session ownership.
    """
    if primary_min_hold_ms <= 0:
        return True
    if primary_assigned_at_ms <= 0:
        return True
    return (now_ms - primary_assigned_at_ms) >= primary_min_hold_ms


def shadow_promotion_is_eligible(
    primary: TrackedOpportunity,
    shadow: TrackedOpportunity,
    primary_assigned_at_ms: int,
    now_ms: int,
    primary_min_hold_ms: int,
    shadow_promotion_score_delta_bps: float,
    primary_executing: bool = False,
    shadow_ready: bool = True,
) -> bool:
    """V1 shadow_promotion_is_eligible (entry_local_l2.rs:99-115).

    Rejects promotion when:
    - primary is currently executing (would lose local-L2 tracking)
    - shadow is not ready (book not yet hot)
    """
    if primary_executing or not shadow_ready:
        return False
    score_delta = shadow.ranking_edge_bps - primary.ranking_edge_bps
    return (
        score_delta >= shadow_promotion_score_delta_bps
        and primary_hold_window_allows_replacement(
            primary_assigned_at_ms, now_ms, primary_min_hold_ms
        )
    )


def deduplicated_tracked_legs(
    tracked: list[TrackedOpportunity],
) -> list[tuple[str, str]]:
    """Return unique (venue, symbol) pairs from tracked opportunities."""
    seen: set[tuple[str, str]] = set()
    result: list[tuple[str, str]] = []
    for opp in tracked:
        for venue in (opp.long_venue, opp.short_venue):
            pair = (venue, opp.symbol)
            if pair not in seen:
                seen.add(pair)
                result.append(pair)
    return result
