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
from typing import Optional


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
        if venue not in self.legs:
            self.legs[venue] = EntryLocalL2LegSession(venue=venue, symbol=symbol)
        return self.legs[venue]

    def both_legs_ready(self, now_ms: int, stale_after_ms: int) -> bool:
        if len(self.legs) < 2:
            return False
        return all(leg.is_ready(now_ms, stale_after_ms) for leg in self.legs.values())

    def ready_leg_count(self, now_ms: int, stale_after_ms: int) -> int:
        return sum(1 for leg in self.legs.values() if leg.is_ready(now_ms, stale_after_ms))

    def faulted_leg_count(self) -> int:
        return sum(1 for leg in self.legs.values() if leg.state == EntryLocalL2LegState.FAULTED)

    def stale_leg_count(self, now_ms: int, stale_after_ms: int) -> int:
        return sum(1 for leg in self.legs.values() if leg.is_stale(now_ms, stale_after_ms))

    def refresh_state(self, now_ms: int, stale_after_ms: int) -> None:
        """Recompute session state from leg states."""
        if self.state == EntryLocalL2SessionState.CLOSED:
            return
        if self.both_legs_ready(now_ms, stale_after_ms):
            self.state = EntryLocalL2SessionState.READY
        elif any(leg.state == EntryLocalL2LegState.FAULTED for leg in self.legs.values()):
            # All legs faulted → session fault; some legs arming/ready → stay arming
            if all(leg.state == EntryLocalL2LegState.FAULTED for leg in self.legs.values()):
                self.state = EntryLocalL2SessionState.FAULTED
            else:
                self.state = EntryLocalL2SessionState.ARMING
        else:
            self.state = EntryLocalL2SessionState.ARMING

    def diagnostics_snapshot(self, now_ms: int, stale_after_ms: int) -> dict:
        return {
            "pair_id": self.pair_id,
            "state": self.state.value,
            "dual_ready": self.both_legs_ready(now_ms, stale_after_ms),
            "ready_leg_count": self.ready_leg_count(now_ms, stale_after_ms),
            "faulted_leg_count": self.faulted_leg_count(),
            "stale_leg_count": self.stale_leg_count(now_ms, stale_after_ms),
            "leg_count": len(self.legs),
        }


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
        leg.mark_arming(SessionArmingReason.BOOK_STATUS_TRANSITION)
        diag["reason"] = f"book_{status_value}"
        diag["detail"] = f"book_status={status_value}"
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

    if stale_after_ms > 0 and hasattr(book, "is_stale") and book.is_stale(stale_after_ms, now_ms):
        detail = f"age_ms={age_ms} stale_after_ms={stale_after_ms}"
        leg.mark_faulted(
            EntryLocalL2LegFault.STALE_BOOK,
            detail,
            seen_at_ms=observed_at_ms,
        )
        diag["reason"] = "stale_book"
        diag["detail"] = detail
        return diag

    if hasattr(book, "has_crossed_book") and book.has_crossed_book():
        detail = f"best_bid={book.best_bid()} best_ask={book.best_ask()}"
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
        bid = book.best_bid() if hasattr(book, "best_bid") else 1.0
        ask = book.best_ask() if hasattr(book, "best_ask") else 1.0
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
        session.ensure_leg(opp.long_venue, opp.symbol)
        session.ensure_leg(opp.short_venue, opp.symbol)
        if opp.class_ == TrackedOpportunityClass.PRIMARY:
            session.primary_assigned_at_ms = now_ms
        return session

    def close_session(self, pair_id: str) -> None:
        session = self.sessions.get(pair_id)
        if session is not None:
            session.state = EntryLocalL2SessionState.CLOSED

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
    candidates: list, primary_count: int, shadow_count: int
) -> list[TrackedOpportunity]:
    """Select primary and shadow tracked opportunities from candidates.

    Top primary_count become PRIMARY, next shadow_count become SHADOW.
    Uses stable pair_id from make_candidate_pair_id() when candidate lacks one.
    """
    tracked_count = primary_count + shadow_count
    result: list[TrackedOpportunity] = []
    for i, c in enumerate(candidates[:tracked_count]):
        class_ = (
            TrackedOpportunityClass.PRIMARY if i < primary_count
            else TrackedOpportunityClass.SHADOW
        )
        symbol = getattr(c, "symbol", "")
        long_venue = str(getattr(c, "long_venue", ""))
        short_venue = str(getattr(c, "short_venue", ""))
        pair_id = getattr(c, "pair_id", None)
        if not pair_id:
            pair_id = make_candidate_pair_id(symbol, long_venue, short_venue)
        result.append(TrackedOpportunity(
            pair_id=pair_id,
            symbol=symbol,
            long_venue=long_venue,
            short_venue=short_venue,
            ranking_edge_bps=getattr(c, "ranking_edge_bps", 0.0),
            class_=class_,
        ))
    return result


def primary_hold_window_allows_replacement(
    primary_assigned_at_ms: int, now_ms: int, primary_min_hold_ms: int
) -> bool:
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
) -> bool:
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
