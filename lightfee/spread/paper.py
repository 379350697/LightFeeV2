"""Shadow paper tracking for spread-reversion candidates."""

from __future__ import annotations

from dataclasses import dataclass, field

from lightfee.offline.paper_outcome import classify_paper_outcome
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.models import SpreadReversionCandidate


@dataclass(frozen=True)
class SpreadPaperConfig:
    enabled: bool = False
    finalist_limit: int = 0
    markout_secs: list[int] = field(default_factory=lambda: [60, 300, 900, 1800])
    terminal_secs: int = 1800
    taker_fee_bps_by_venue: dict[str, float] = field(default_factory=dict)
    slippage_buffer_bps: float = 0.0
    default_funding_interval_ms: int = 28_800_000


@dataclass(frozen=True)
class SpreadPaperLeg:
    venue: str
    side: str
    entry_raw_price: float
    entry_price: float
    qty: float
    entry_notional_quote: float
    entry_fee_quote: float
    entry_slippage_quote: float
    funding_rate_bps: float
    funding_timestamp_ms: int


@dataclass(frozen=True)
class SpreadPaperPosition:
    paper_id: str
    candidate_id: str
    symbol: str
    long_venue: str
    short_venue: str
    candidate_opportunity_label: str
    finalist_rank: int
    registered_at_ms: int
    entry_notional_quote: float
    long_leg: SpreadPaperLeg
    short_leg: SpreadPaperLeg
    due_horizons: list[dict]


class SpreadPaperTracker:
    def __init__(self, config: SpreadPaperConfig) -> None:
        self.config = config
        self._positions: dict[str, SpreadPaperPosition] = {}
        self._emitted_horizons: dict[str, set[str]] = {}
        self._known_paper_ids: set[str] = set()

    @property
    def enabled(self) -> bool:
        return self.config.enabled and self.config.finalist_limit > 0

    @property
    def tracked_count(self) -> int:
        return len(self._positions)

    def register(
        self,
        candidate: SpreadReversionCandidate,
        quotes: dict[str, QuoteSnapshot],
        *,
        finalist_rank: int,
    ) -> dict | None:
        if not self.enabled:
            return None
        if finalist_rank >= self.config.finalist_limit:
            return None
        if str(candidate.signal_status) != "entry_ready":
            return None
        paper_id = _paper_id(candidate)
        if paper_id in self._known_paper_ids:
            return None
        long_quote = _quote_for(quotes, candidate.long_venue, candidate.symbol)
        short_quote = _quote_for(quotes, candidate.short_venue, candidate.symbol)
        if long_quote is None or short_quote is None:
            return None
        position = self._build_position(
            paper_id=paper_id,
            candidate=candidate,
            long_quote=long_quote,
            short_quote=short_quote,
            finalist_rank=finalist_rank,
        )
        if position is None:
            return None
        self._positions[paper_id] = position
        self._emitted_horizons[paper_id] = set()
        self._known_paper_ids.add(paper_id)
        return _registration_event(position)

    def restore_from_records(self, records: list[dict]) -> None:
        """Restore open paper orders from the local paper journal."""
        self._positions.clear()
        self._emitted_horizons.clear()
        self._known_paper_ids.clear()
        for record in records:
            kind = str(record.get("kind", "") or "")
            payload = record.get("payload", {})
            if not isinstance(payload, dict):
                continue
            paper_id = str(payload.get("paper_id", "") or "")
            if not paper_id:
                continue
            if kind == "opportunity.paper_registered":
                position = _position_from_registration_payload(payload)
                if position is None:
                    continue
                self._known_paper_ids.add(paper_id)
                self._positions[paper_id] = position
                self._emitted_horizons.setdefault(paper_id, set())
            elif kind in {"opportunity.paper_markout", "opportunity.paper_closed"}:
                self._known_paper_ids.add(paper_id)
                horizon_kind = str(payload.get("horizon_kind", "") or "")
                if horizon_kind:
                    self._emitted_horizons.setdefault(paper_id, set()).add(horizon_kind)
                if kind == "opportunity.paper_closed":
                    self._positions.pop(paper_id, None)
                    self._emitted_horizons.pop(paper_id, None)

    def evaluate_due(
        self,
        now_ms: int,
        quotes: dict[str, QuoteSnapshot],
    ) -> list[dict]:
        if not self.enabled:
            return []
        events: list[dict] = []
        closed_ids: list[str] = []
        for position in list(self._positions.values()):
            emitted = self._emitted_horizons.setdefault(position.paper_id, set())
            for horizon in position.due_horizons:
                horizon_kind = str(horizon["kind"])
                if int(horizon["due_at_ms"]) > now_ms:
                    continue
                if horizon_kind in emitted:
                    continue
                emitted.add(horizon_kind)
                event = self._build_due_event(
                    position=position,
                    horizon=horizon,
                    now_ms=now_ms,
                    quotes=quotes,
                )
                events.append(event)
                if bool(horizon["terminal"]):
                    closed_ids.append(position.paper_id)
        for paper_id in closed_ids:
            self._positions.pop(paper_id, None)
            self._emitted_horizons.pop(paper_id, None)
        return events

    def _build_position(
        self,
        *,
        paper_id: str,
        candidate: SpreadReversionCandidate,
        long_quote: QuoteSnapshot,
        short_quote: QuoteSnapshot,
        finalist_rank: int,
    ) -> SpreadPaperPosition | None:
        entry_notional = float(candidate.entry_notional_quote or 0.0)
        if entry_notional <= 0.0:
            return None
        long_leg = self._entry_leg(
            venue=candidate.long_venue,
            side="long",
            raw_price=float(long_quote.ask or 0.0),
            notional_quote=entry_notional,
            quote=long_quote,
        )
        short_leg = self._entry_leg(
            venue=candidate.short_venue,
            side="short",
            raw_price=float(short_quote.bid or 0.0),
            notional_quote=entry_notional,
            quote=short_quote,
        )
        if long_leg is None or short_leg is None:
            return None
        return SpreadPaperPosition(
            paper_id=paper_id,
            candidate_id=candidate.candidate_id,
            symbol=candidate.symbol,
            long_venue=candidate.long_venue,
            short_venue=candidate.short_venue,
            candidate_opportunity_label=str(
                getattr(candidate, "opportunity_label", "") or "spread_reversion"
            ),
            finalist_rank=finalist_rank,
            registered_at_ms=int(candidate.signal_ts_ms or 0),
            entry_notional_quote=entry_notional,
            long_leg=long_leg,
            short_leg=short_leg,
            due_horizons=self._due_horizons(int(candidate.signal_ts_ms or 0)),
        )

    def _entry_leg(
        self,
        *,
        venue: str,
        side: str,
        raw_price: float,
        notional_quote: float,
        quote: QuoteSnapshot,
    ) -> SpreadPaperLeg | None:
        if raw_price <= 0.0 or notional_quote <= 0.0:
            return None
        entry_price = _apply_slippage(
            raw_price,
            bps=self.config.slippage_buffer_bps,
            action="buy" if side == "long" else "sell",
        )
        qty = notional_quote / entry_price
        entry_slippage_quote = abs(entry_price - raw_price) * qty
        return SpreadPaperLeg(
            venue=str(venue).lower(),
            side=side,
            entry_raw_price=raw_price,
            entry_price=entry_price,
            qty=qty,
            entry_notional_quote=notional_quote,
            entry_fee_quote=notional_quote * _fee_bps(self.config, venue) / 10_000.0,
            entry_slippage_quote=entry_slippage_quote,
            funding_rate_bps=float(getattr(quote, "funding_rate_bps", 0.0) or 0.0),
            funding_timestamp_ms=int(getattr(quote, "funding_timestamp_ms", 0) or 0),
        )

    def _due_horizons(self, registered_at_ms: int) -> list[dict]:
        horizons: list[dict] = []
        for secs in self.config.markout_secs:
            sec = int(secs or 0)
            horizons.append(
                {
                    "kind": f"markout_{sec}s",
                    "due_at_ms": registered_at_ms + sec * 1000,
                    "terminal": False,
                }
            )
        terminal_secs = int(self.config.terminal_secs or 0)
        horizons.append(
            {
                "kind": f"terminal_{terminal_secs}s",
                "due_at_ms": registered_at_ms + terminal_secs * 1000,
                "terminal": True,
            }
        )
        horizons.sort(key=lambda item: (int(item["due_at_ms"]), str(item["kind"])))
        return horizons

    def _build_due_event(
        self,
        *,
        position: SpreadPaperPosition,
        horizon: dict,
        now_ms: int,
        quotes: dict[str, QuoteSnapshot],
    ) -> dict:
        long_quote = _quote_for(quotes, position.long_venue, position.symbol)
        short_quote = _quote_for(quotes, position.short_venue, position.symbol)
        terminal = bool(horizon["terminal"])
        kind = "opportunity.paper_closed" if terminal else "opportunity.paper_markout"
        payload = self._build_payload(
            position=position,
            horizon_kind=str(horizon["kind"]),
            now_ms=now_ms,
            long_quote=long_quote,
            short_quote=short_quote,
        )
        return {"kind": kind, "payload": payload}

    def _build_payload(
        self,
        *,
        position: SpreadPaperPosition,
        horizon_kind: str,
        now_ms: int,
        long_quote: QuoteSnapshot | None,
        short_quote: QuoteSnapshot | None,
    ) -> dict:
        long_mid = _mid(long_quote)
        short_mid = _mid(short_quote)
        base_payload = {
            "paper_id": position.paper_id,
            "candidate_id": position.candidate_id,
            "review_id": None,
            "symbol": position.symbol,
            "pair_id": f"{position.long_venue}:{position.short_venue}:{position.symbol}",
            "long_venue": position.long_venue,
            "short_venue": position.short_venue,
            "horizon_kind": horizon_kind,
            "registered_at_ms": position.registered_at_ms,
            "evaluated_at_ms": now_ms,
            "selected_real_trade": False,
            "not_selected_reason": "spread_shadow_paper",
            "candidate_opportunity_label": position.candidate_opportunity_label,
            "paper_entry_notional_quote": position.entry_notional_quote,
            "market_snapshot": {
                "long_mid": long_mid,
                "short_mid": short_mid,
                "snapshot_available": long_quote is not None and short_quote is not None,
            },
        }
        if long_quote is None or short_quote is None:
            base_payload.update(
                {
                    "paper_gross_quote": None,
                    "paper_fee_quote": _entry_fee_quote(position),
                    "paper_entry_fee_quote": _entry_fee_quote(position),
                    "paper_exit_fee_quote": 0.0,
                    "paper_funding_quote": 0.0,
                    "accrued_funding_estimate_quote": 0.0,
                    "settlement_realized_funding_quote": 0.0,
                    "paper_slippage_quote": _entry_slippage_quote(position),
                    "paper_entry_slippage_quote": _entry_slippage_quote(position),
                    "paper_exit_slippage_quote": 0.0,
                    "paper_net_quote": None,
                    "paper_net_bps": None,
                    "opportunity_label": classify_paper_outcome(False, None, None),
                    "long_leg": _leg_payload(position.long_leg),
                    "short_leg": _leg_payload(position.short_leg),
                }
            )
            return base_payload

        markout = self._markout(position, now_ms, long_quote, short_quote)
        paper_net_bps = markout["paper_net_quote"] / position.entry_notional_quote * 10_000.0
        base_payload.update(markout)
        base_payload.update(
            {
                "paper_net_bps": paper_net_bps,
                "opportunity_label": classify_paper_outcome(
                    False,
                    markout["paper_net_quote"],
                    None,
                ),
            }
        )
        return base_payload

    def _markout(
        self,
        position: SpreadPaperPosition,
        now_ms: int,
        long_quote: QuoteSnapshot,
        short_quote: QuoteSnapshot,
    ) -> dict:
        long_exit_raw = float(long_quote.bid or 0.0)
        short_exit_raw = float(short_quote.ask or 0.0)
        long_exit = _apply_slippage(
            long_exit_raw,
            bps=self.config.slippage_buffer_bps,
            action="sell",
        )
        short_exit = _apply_slippage(
            short_exit_raw,
            bps=self.config.slippage_buffer_bps,
            action="buy",
        )
        long_gross = position.long_leg.qty * (
            long_exit_raw - position.long_leg.entry_raw_price
        )
        short_gross = position.short_leg.qty * (
            position.short_leg.entry_raw_price - short_exit_raw
        )
        paper_gross_quote = long_gross + short_gross
        long_exit_notional = position.long_leg.qty * long_exit_raw
        short_exit_notional = position.short_leg.qty * short_exit_raw
        long_exit_fee = long_exit_notional * _fee_bps(self.config, position.long_venue) / 10_000.0
        short_exit_fee = short_exit_notional * _fee_bps(self.config, position.short_venue) / 10_000.0
        entry_fee_quote = _entry_fee_quote(position)
        exit_fee_quote = long_exit_fee + short_exit_fee
        entry_slippage_quote = _entry_slippage_quote(position)
        exit_slippage_quote = (
            abs(long_exit_raw - long_exit) * position.long_leg.qty
            + abs(short_exit - short_exit_raw) * position.short_leg.qty
        )
        fee_quote = entry_fee_quote + exit_fee_quote
        slippage_quote = entry_slippage_quote + exit_slippage_quote
        accrued_funding = _accrued_funding_quote(
            position,
            now_ms,
            max(int(self.config.default_funding_interval_ms or 0), 1),
        )
        settlement_funding = _settlement_funding_quote(position, now_ms)
        paper_net_quote = paper_gross_quote + accrued_funding - fee_quote - slippage_quote
        return {
            "paper_gross_quote": paper_gross_quote,
            "paper_fee_quote": fee_quote,
            "paper_entry_fee_quote": entry_fee_quote,
            "paper_exit_fee_quote": exit_fee_quote,
            "paper_funding_quote": accrued_funding,
            "accrued_funding_estimate_quote": accrued_funding,
            "settlement_realized_funding_quote": settlement_funding,
            "paper_slippage_quote": slippage_quote,
            "paper_entry_slippage_quote": entry_slippage_quote,
            "paper_exit_slippage_quote": exit_slippage_quote,
            "paper_net_quote": paper_net_quote,
            "long_leg": _leg_payload(
                position.long_leg,
                exit_raw_price=long_exit_raw,
                exit_price=long_exit,
                exit_fee_quote=long_exit_fee,
                exit_slippage_quote=abs(long_exit_raw - long_exit) * position.long_leg.qty,
                gross_quote=long_gross,
            ),
            "short_leg": _leg_payload(
                position.short_leg,
                exit_raw_price=short_exit_raw,
                exit_price=short_exit,
                exit_fee_quote=short_exit_fee,
                exit_slippage_quote=abs(short_exit - short_exit_raw)
                * position.short_leg.qty,
                gross_quote=short_gross,
            ),
        }


def _paper_id(candidate: SpreadReversionCandidate) -> str:
    return f"spread:{candidate.candidate_id}:{int(candidate.signal_ts_ms or 0)}"


def _registration_event(position: SpreadPaperPosition) -> dict:
    return {
        "kind": "opportunity.paper_registered",
        "payload": {
            "paper_id": position.paper_id,
            "candidate_id": position.candidate_id,
            "review_id": None,
            "symbol": position.symbol,
            "pair_id": f"{position.long_venue}:{position.short_venue}:{position.symbol}",
            "long_venue": position.long_venue,
            "short_venue": position.short_venue,
            "candidate_opportunity_label": position.candidate_opportunity_label,
            "finalist_rank": position.finalist_rank,
            "registered_at_ms": position.registered_at_ms,
            "selected_real_trade": False,
            "not_selected_reason": "spread_shadow_paper",
            "paper_order_status": "open",
            "paper_entry_notional_quote": position.entry_notional_quote,
            "paper_entry_fee_quote": _entry_fee_quote(position),
            "paper_entry_slippage_quote": _entry_slippage_quote(position),
            "paper_fee_quote": _entry_fee_quote(position),
            "paper_slippage_quote": _entry_slippage_quote(position),
            "paper_funding_quote": 0.0,
            "accrued_funding_estimate_quote": 0.0,
            "settlement_realized_funding_quote": 0.0,
            "due_horizons": position.due_horizons,
            "long_leg": _leg_payload(position.long_leg),
            "short_leg": _leg_payload(position.short_leg),
        },
    }


def _position_from_registration_payload(payload: dict) -> SpreadPaperPosition | None:
    long_leg = _leg_from_payload(payload.get("long_leg", {}))
    short_leg = _leg_from_payload(payload.get("short_leg", {}))
    if long_leg is None or short_leg is None:
        return None
    due_horizons_raw = payload.get("due_horizons", [])
    due_horizons = [dict(item) for item in due_horizons_raw if isinstance(item, dict)]
    if not due_horizons:
        return None
    paper_id = str(payload.get("paper_id", "") or "")
    candidate_id = str(payload.get("candidate_id", "") or "")
    symbol = str(payload.get("symbol", "") or "")
    long_venue = str(payload.get("long_venue", "") or "")
    short_venue = str(payload.get("short_venue", "") or "")
    if not paper_id or not candidate_id or not symbol or not long_venue or not short_venue:
        return None
    return SpreadPaperPosition(
        paper_id=paper_id,
        candidate_id=candidate_id,
        symbol=symbol,
        long_venue=long_venue,
        short_venue=short_venue,
        candidate_opportunity_label=str(
            payload.get("candidate_opportunity_label", "") or "spread_reversion"
        ),
        finalist_rank=int(payload.get("finalist_rank", 0) or 0),
        registered_at_ms=int(payload.get("registered_at_ms", 0) or 0),
        entry_notional_quote=float(payload.get("paper_entry_notional_quote", 0.0) or 0.0),
        long_leg=long_leg,
        short_leg=short_leg,
        due_horizons=due_horizons,
    )


def _leg_from_payload(payload: object) -> SpreadPaperLeg | None:
    if not isinstance(payload, dict):
        return None
    venue = str(payload.get("venue", "") or "")
    side = str(payload.get("side", "") or "")
    if not venue or side not in {"long", "short"}:
        return None
    return SpreadPaperLeg(
        venue=venue,
        side=side,
        entry_raw_price=float(payload.get("entry_raw_price", 0.0) or 0.0),
        entry_price=float(payload.get("entry_price", 0.0) or 0.0),
        qty=float(payload.get("qty", 0.0) or 0.0),
        entry_notional_quote=float(payload.get("entry_notional_quote", 0.0) or 0.0),
        entry_fee_quote=float(payload.get("entry_fee_quote", 0.0) or 0.0),
        entry_slippage_quote=float(payload.get("entry_slippage_quote", 0.0) or 0.0),
        funding_rate_bps=float(payload.get("funding_rate_bps", 0.0) or 0.0),
        funding_timestamp_ms=int(payload.get("funding_timestamp_ms", 0) or 0),
    )


def _quote_for(
    quotes: dict[str, QuoteSnapshot],
    venue: str,
    symbol: str,
) -> QuoteSnapshot | None:
    direct = quotes.get(f"{str(venue).lower()}:{str(symbol).upper()}")
    if direct is not None:
        return direct
    for quote in quotes.values():
        if (
            str(getattr(quote, "venue", "") or "").lower() == str(venue).lower()
            and str(getattr(quote, "symbol", "") or "").upper() == str(symbol).upper()
        ):
            return quote
    return None


def _fee_bps(config: SpreadPaperConfig, venue: str) -> float:
    return float(config.taker_fee_bps_by_venue.get(str(venue).lower(), 0.0) or 0.0)


def _apply_slippage(raw_price: float, *, bps: float, action: str) -> float:
    factor = max(float(bps or 0.0), 0.0) / 10_000.0
    if action == "buy":
        return raw_price * (1.0 + factor)
    return raw_price * (1.0 - factor)


def _entry_fee_quote(position: SpreadPaperPosition) -> float:
    return position.long_leg.entry_fee_quote + position.short_leg.entry_fee_quote


def _entry_slippage_quote(position: SpreadPaperPosition) -> float:
    return position.long_leg.entry_slippage_quote + position.short_leg.entry_slippage_quote


def _accrued_funding_quote(
    position: SpreadPaperPosition,
    now_ms: int,
    funding_interval_ms: int,
) -> float:
    held_ratio = max(now_ms - position.registered_at_ms, 0) / funding_interval_ms
    long_quote = -(
        position.long_leg.entry_notional_quote
        * position.long_leg.funding_rate_bps
        / 10_000.0
        * held_ratio
    )
    short_quote = (
        position.short_leg.entry_notional_quote
        * position.short_leg.funding_rate_bps
        / 10_000.0
        * held_ratio
    )
    return long_quote + short_quote


def _settlement_funding_quote(position: SpreadPaperPosition, now_ms: int) -> float:
    return _settled_leg_funding(position.long_leg, position.registered_at_ms, now_ms) + (
        _settled_leg_funding(position.short_leg, position.registered_at_ms, now_ms)
    )


def _settled_leg_funding(leg: SpreadPaperLeg, registered_at_ms: int, now_ms: int) -> float:
    funding_ts = int(leg.funding_timestamp_ms or 0)
    if funding_ts <= registered_at_ms or funding_ts > now_ms:
        return 0.0
    quote = leg.entry_notional_quote * leg.funding_rate_bps / 10_000.0
    if leg.side == "long":
        return -quote
    return quote


def _leg_payload(
    leg: SpreadPaperLeg,
    *,
    exit_raw_price: float | None = None,
    exit_price: float | None = None,
    exit_fee_quote: float = 0.0,
    exit_slippage_quote: float = 0.0,
    gross_quote: float | None = None,
) -> dict:
    return {
        "venue": leg.venue,
        "side": leg.side,
        "entry_raw_price": leg.entry_raw_price,
        "entry_price": leg.entry_price,
        "exit_raw_price": exit_raw_price,
        "exit_price": exit_price,
        "qty": leg.qty,
        "entry_notional_quote": leg.entry_notional_quote,
        "entry_fee_quote": leg.entry_fee_quote,
        "exit_fee_quote": exit_fee_quote,
        "entry_slippage_quote": leg.entry_slippage_quote,
        "exit_slippage_quote": exit_slippage_quote,
        "gross_quote": gross_quote,
        "funding_rate_bps": leg.funding_rate_bps,
        "funding_timestamp_ms": leg.funding_timestamp_ms,
    }


def _mid(quote: QuoteSnapshot | None) -> float | None:
    if quote is None:
        return None
    bid = float(getattr(quote, "bid", 0.0) or 0.0)
    ask = float(getattr(quote, "ask", 0.0) or 0.0)
    if bid <= 0.0 or ask <= 0.0:
        return None
    return (bid + ask) / 2.0
