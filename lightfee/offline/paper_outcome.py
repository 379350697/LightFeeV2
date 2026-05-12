"""Paper outcome tracking — V1 PaperOpportunityTracker equivalent.

V1: src/execution_core/paper_outcome.rs
Tracks paper (hypothetical) opportunity outcomes for comparison against
realized trades. Emits markout, settlement-closed, and real-vs-paper-joined
events that feed offline analysis and operator diagnostics.

Config surface (supplied by config layer, NOT defined here):
- paper_outcome_tracking_enabled: bool (default False)
- paper_outcome_finalist_limit: int (default 0 = track none)
- paper_outcome_markout_secs: list[int] (e.g. [300, 1800])
- paper_outcome_settlement_grace_secs: int (default 0)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PaperOutcomeConfig:
    """V1-equivalent PaperOutcome config block.

    Mirrors V1 PersistenceConfig paper_outcome_* fields.
    Owned by the config layer; passed here as a value object.
    """
    tracking_enabled: bool = False
    finalist_limit: int = 0          # 0 = track none
    markout_secs: list[int] = field(default_factory=lambda: [300, 1800])
    settlement_grace_secs: int = 0


@dataclass
class PaperOpportunityRegistration:
    """V1 PaperOpportunityRegistration — a single paper opportunity to track."""
    paper_id: str
    review_id: Optional[str]
    symbol: str
    pair_id: str
    long_venue: str
    short_venue: str
    finalist_rank: int
    selected_real_trade: bool
    not_selected_reason: Optional[str]
    registered_at_ms: int
    target_settlement_ts_ms: Optional[int]
    markout_secs: list[int] = field(default_factory=list)
    entry_notional_quote: float = 0.0
    fee_quote: float = 0.0
    expected_funding_quote: float = 0.0
    entry_slippage_quote: float = 0.0


def classify_paper_outcome(
    selected_real_trade: bool,
    paper_net_quote: Optional[float],
    real_net_quote: Optional[float] = None,
) -> str:
    """V1 classify_paper_outcome: label the paper outcome.

    Mirrors V1 paper_outcome.rs classify_paper_outcome() exactly.
    """
    if selected_real_trade:
        if real_net_quote is not None:
            if real_net_quote > 0.0:
                return "good_trade_executed"
            return "bad_trade_executed"
        return "unknown_due_to_incomplete_lifecycle"

    if paper_net_quote is not None:
        if paper_net_quote > 0.0:
            return "good_trade_missed"
        return "bad_trade_correctly_rejected"
    return "unknown_due_to_missing_snapshot"


class _TrackedPaperOpportunity:
    """Internal tracking state for a single paper opportunity."""

    def __init__(self, registration: PaperOpportunityRegistration) -> None:
        self.registration = registration
        self.emitted_horizons: set[str] = set()
        self.latest_outcome_payload: Optional[dict] = None
        self.real_join_emitted: bool = False

        # Build due horizons from markout_secs + settlement grace
        self.due_horizons: list[dict] = []
        for secs in registration.markout_secs:
            self.due_horizons.append({
                "kind": f"markout_{secs}s",
                "due_at_ms": registration.registered_at_ms + secs * 1000,
                "terminal": False,
            })
        if registration.target_settlement_ts_ms is not None:
            self.due_horizons.append({
                "kind": "settlement",
                "due_at_ms": registration.target_settlement_ts_ms,
                "terminal": True,
            })
        self.due_horizons.sort(key=lambda h: h["due_at_ms"])


class PaperOutcomeTracker:
    """V1 PaperOpportunityTracker equivalent.

    Tracks paper (hypothetical) opportunities through markout horizons
    and settlement, emitting journal events at each due horizon.
    Supports joining real outcomes via review_id for comparison.
    """

    def __init__(self, config: PaperOutcomeConfig) -> None:
        self.config = config
        self._tracked: dict[str, _TrackedPaperOpportunity] = {}

    @property
    def tracked_count(self) -> int:
        return len(self._tracked)

    @property
    def enabled(self) -> bool:
        return self.config.tracking_enabled and self.config.finalist_limit > 0

    def register(self, registration: PaperOpportunityRegistration) -> bool:
        """Register a paper opportunity for tracking.

        Idempotent by paper_id. Returns True if it was newly registered.
        Silently no-ops when tracking is disabled or finalist limit is 0.
        """
        if not self.enabled:
            return False
        if registration.finalist_rank >= self.config.finalist_limit:
            return False
        if registration.paper_id in self._tracked:
            return False
        self._tracked[registration.paper_id] = _TrackedPaperOpportunity(registration)
        return True

    def evaluate_due(
        self,
        now_ms: int,
        market_snapshots: Optional[dict[str, dict]] = None,
    ) -> list[dict]:
        """Evaluate and emit due paper outcome events.

        Returns list of event dicts with keys 'kind' and 'payload'.
        Each horizon fires at most once.

        Args:
            now_ms: Current timestamp for horizon evaluation.
            market_snapshots: Optional market data keyed by venue_symbol.
                              Dict of {venue_symbol: {mid: float}}.
                              When missing, events report "unknown_due_to_missing_snapshot".
        """
        if not self.enabled:
            return []
        events: list[dict] = []
        for tracked in self._tracked.values():
            for horizon in tracked.due_horizons:
                if horizon["due_at_ms"] > now_ms:
                    continue
                if horizon["kind"] in tracked.emitted_horizons:
                    continue
                tracked.emitted_horizons.add(horizon["kind"])
                event = self._build_due_event(tracked, horizon, now_ms, market_snapshots or {})
                tracked.latest_outcome_payload = event["payload"]
                events.append(event)
        return events

    def join_real_outcome(
        self,
        review_id: str,
        position_id: str,
        real_net_quote: float,
        real_net_bps: Optional[float] = None,
        now_ms: int = 0,
    ) -> Optional[dict]:
        """Join a realized trade outcome with its paper counterpart.

        Returns an 'opportunity.real_vs_paper_joined' event if a matching
        paper registration is found that hasn't been joined yet.
        """
        tracked = None
        for t in self._tracked.values():
            if (t.registration.review_id == review_id
                    and t.registration.selected_real_trade
                    and not t.real_join_emitted):
                tracked = t
                break
        if tracked is None:
            return None
        tracked.real_join_emitted = True
        latest = tracked.latest_outcome_payload or {}
        paper_net_quote = latest.get("paper_net_quote")
        paper_net_bps = latest.get("paper_net_bps")
        horizon_kind = latest.get("horizon_kind", "real_close_join")
        reg = tracked.registration
        return {
            "kind": "opportunity.real_vs_paper_joined",
            "payload": {
                "paper_id": reg.paper_id,
                "review_id": reg.review_id,
                "position_id": position_id,
                "symbol": reg.symbol,
                "pair_id": reg.pair_id,
                "long_venue": reg.long_venue,
                "short_venue": reg.short_venue,
                "horizon_kind": horizon_kind,
                "evaluated_at_ms": now_ms,
                "selected_real_trade": True,
                "paper_net_quote": paper_net_quote,
                "paper_net_bps": paper_net_bps,
                "real_net_quote": real_net_quote,
                "real_net_bps": real_net_bps,
                "opportunity_label": classify_paper_outcome(
                    True, paper_net_quote, real_net_quote,
                ),
            },
        }

    def _build_due_event(
        self,
        tracked: _TrackedPaperOpportunity,
        horizon: dict,
        now_ms: int,
        market_snapshots: dict[str, dict],
    ) -> dict:
        reg = tracked.registration
        long_key = f"{reg.long_venue}:{reg.symbol}"
        short_key = f"{reg.short_venue}:{reg.symbol}"

        long_snap = market_snapshots.get(long_key)
        short_snap = market_snapshots.get(short_key)

        paper_net_quote: Optional[float] = None
        long_mid: Optional[float] = None
        short_mid: Optional[float] = None
        snapshot_available = False

        if long_snap is not None and short_snap is not None:
            long_mid = long_snap.get("mid")
            short_mid = short_snap.get("mid")
            if long_mid is not None and short_mid is not None:
                snapshot_available = True
                avg_mid = max(abs((long_mid + short_mid) / 2.0), 1e-12)
                markout_quote = ((short_mid - long_mid) / avg_mid
                                 * reg.entry_notional_quote)
                paper_net_quote = (reg.expected_funding_quote + markout_quote
                                   - reg.fee_quote - reg.entry_slippage_quote)

        paper_net_bps = None
        if paper_net_quote is not None:
            denom = max(reg.entry_notional_quote, 1e-12)
            paper_net_bps = paper_net_quote / denom * 10000.0

        opportunity_label = classify_paper_outcome(
            reg.selected_real_trade, paper_net_quote, None,
        )

        kind = "opportunity.paper_closed" if horizon["terminal"] else "opportunity.paper_markout"
        return {
            "kind": kind,
            "payload": {
                "paper_id": reg.paper_id,
                "review_id": reg.review_id,
                "symbol": reg.symbol,
                "pair_id": reg.pair_id,
                "long_venue": reg.long_venue,
                "short_venue": reg.short_venue,
                "horizon_kind": horizon["kind"],
                "evaluated_at_ms": now_ms,
                "selected_real_trade": reg.selected_real_trade,
                "not_selected_reason": reg.not_selected_reason,
                "paper_entry_notional_quote": reg.entry_notional_quote,
                "paper_fee_quote": reg.fee_quote,
                "paper_funding_quote": reg.expected_funding_quote,
                "paper_slippage_quote": reg.entry_slippage_quote,
                "paper_net_quote": paper_net_quote,
                "paper_net_bps": paper_net_bps,
                "opportunity_label": opportunity_label,
                "market_snapshot": {
                    "long_mid": long_mid,
                    "short_mid": short_mid,
                    "snapshot_available": snapshot_available,
                },
            },
        }
