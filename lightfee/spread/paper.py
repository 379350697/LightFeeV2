"""Shadow paper tracking for spread-reversion candidates."""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from lightfee.offline.paper_outcome import classify_paper_outcome
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.models import SpreadReversionCandidate


DEFAULT_SPREAD_PAPER_BOT_IDS = (
    "tt_conservative",
    "mt_long_maker",
    "mt_short_maker",
    "mt_selected_maker",
    "mt_selected_maker_delay_1000ms",
    "core_v1_bot",
    "core_v1_exec100_bot",
    "core_v1_z10_bot",
    "bad_pair_control_bot",
    "low_liquidity_control_bot",
    "low_edge_control_bot",
)

GOOD_SPREAD_PAIRS = {
    ("binance", "gate"),
    ("gate", "binance"),
    ("gate", "bybit"),
    ("gate", "bitget"),
    ("bybit", "gate"),
    ("gate", "aster"),
}

BAD_SPREAD_PAIRS = {
    ("hyperliquid", "aster"),
    ("hyperliquid", "gate"),
    ("aster", "bitget"),
    ("binance", "bybit"),
    ("binance", "bitget"),
    ("aster", "bybit"),
}


@dataclass(frozen=True)
class SpreadPaperBotSpec:
    bot_id: str
    cohort: str
    entry_long_role: str = "taker"
    entry_short_role: str = "taker"
    exit_long_role: str = "taker"
    exit_short_role: str = "taker"
    maker_leg: str = ""
    hedge_delay_ms: int = 0
    delayed_leg: str = ""
    control_group: bool = False
    min_executable_spread_bps: float | None = None
    min_net_edge_bps: float | None = None
    min_z_score: float | None = None
    require_good_pair: bool = False
    require_bad_pair: bool = False
    require_low_liquidity: bool = False
    require_low_edge: bool = False


@dataclass(frozen=True)
class SpreadPaperConfig:
    enabled: bool = False
    finalist_limit: int = 0
    markout_secs: list[int] = field(default_factory=lambda: [60, 300, 900, 1800])
    terminal_secs: int = 1800
    active_exit_enabled: bool = False
    exit_z: float = 0.5
    stop_z: float = 3.5
    max_hold_ms: int = 0
    taker_fee_bps_by_venue: dict[str, float] = field(default_factory=dict)
    maker_fee_bps_by_venue: dict[str, float] = field(default_factory=dict)
    slippage_buffer_bps: float = 0.0
    default_funding_interval_ms: int = 28_800_000
    excluded_symbols: list[str] = field(default_factory=lambda: ["BBUSDT", "QNTUSDT"])
    allowed_opportunity_labels: list[str] = field(
        default_factory=lambda: ["spread_reversion"]
    )
    episode_cooldown_ms: int = 1_800_000
    paper_bot_ids: list[str] = field(default_factory=lambda: ["tt_conservative"])


@dataclass(frozen=True)
class SpreadPaperLeg:
    venue: str
    side: str
    entry_liquidity_role: str
    exit_liquidity_role: str
    entry_pending: bool
    entry_bid: float
    entry_ask: float
    entry_bid_size: float
    entry_ask_size: float
    entry_observed_at_ms: int
    mark_price: float
    index_price: float
    volume_24h_quote: float
    open_interest: float
    entry_raw_price: float | None
    entry_price: float | None
    qty: float
    entry_notional_quote: float
    entry_fee_bps: float
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
    paper_bot_id: str
    paper_cohort: str
    paper_entry_mode: str
    paper_exit_mode: str
    paper_maker_leg: str
    paper_hedge_delay_ms: int
    paper_control_group: bool
    paper_fill_assumption: str
    finalist_rank: int
    registered_at_ms: int
    entry_notional_quote: float
    long_leg: SpreadPaperLeg
    short_leg: SpreadPaperLeg
    candidate_snapshot: dict
    entry_market_snapshot: dict
    due_horizons: list[dict]


class SpreadPaperTracker:
    def __init__(self, config: SpreadPaperConfig) -> None:
        self.config = config
        self._positions: dict[str, SpreadPaperPosition] = {}
        self._emitted_horizons: dict[str, set[str]] = {}
        self._skipped_horizons: dict[str, set[str]] = {}
        self._known_paper_ids: set[str] = set()
        self._episode_started_at_ms: dict[tuple[str, str, str, str], int] = {}

    @property
    def enabled(self) -> bool:
        return self.config.enabled and self.config.finalist_limit > 0

    @property
    def tracked_count(self) -> int:
        return len(self._positions)

    def missing_due_quote_keys(
        self,
        now_ms: int,
        quotes: dict[str, QuoteSnapshot],
    ) -> set[tuple[str, str]]:
        return self.missing_evaluation_quote_keys(now_ms, quotes)

    def missing_evaluation_quote_keys(
        self,
        now_ms: int,
        quotes: dict[str, QuoteSnapshot],
    ) -> set[tuple[str, str]]:
        if not self.enabled:
            return set()
        missing: set[tuple[str, str]] = set()
        for position in self._positions.values():
            if _position_has_pending_entry(position):
                delay_ms = max(int(position.paper_hedge_delay_ms or 0), 0)
                if now_ms < position.registered_at_ms + delay_ms:
                    continue
                missing.update(self._missing_entry_quote_keys(position, quotes))
                continue
            if (
                self.config.active_exit_enabled
                and now_ms > int(position.registered_at_ms or 0)
            ):
                missing.update(self._missing_exit_quote_keys(position, quotes))
            emitted = self._emitted_horizons.get(position.paper_id, set())
            for horizon in position.due_horizons:
                horizon_kind = str(horizon["kind"])
                if int(horizon["due_at_ms"]) > now_ms:
                    continue
                if horizon_kind in emitted:
                    continue
                missing.update(self._missing_exit_quote_keys(position, quotes))
                break
        return missing

    def register(
        self,
        candidate: SpreadReversionCandidate,
        quotes: dict[str, QuoteSnapshot],
        *,
        finalist_rank: int,
    ) -> dict | None:
        events = self.register_many(candidate, quotes, finalist_rank=finalist_rank)
        return events[0] if events else None

    def register_many(
        self,
        candidate: SpreadReversionCandidate,
        quotes: dict[str, QuoteSnapshot],
        *,
        finalist_rank: int,
    ) -> list[dict]:
        if not self.enabled:
            return []
        if finalist_rank >= self.config.finalist_limit:
            return []
        if str(candidate.signal_status) != "entry_ready":
            return []
        opportunity_label = str(
            getattr(candidate, "opportunity_label", "") or "spread_reversion"
        )
        if str(candidate.symbol).upper() in _excluded_symbols(self.config):
            return []
        allowed_labels = _allowed_labels(self.config)
        if allowed_labels and opportunity_label not in allowed_labels:
            return []
        long_quote = _quote_for(quotes, candidate.long_venue, candidate.symbol)
        short_quote = _quote_for(quotes, candidate.short_venue, candidate.symbol)
        if long_quote is None or short_quote is None:
            return []
        events: list[dict] = []
        for bot in _paper_bot_specs(self.config):
            if not _bot_accepts_candidate(bot, candidate, long_quote, short_quote):
                continue
            if self._episode_in_cooldown(candidate, opportunity_label, bot.bot_id):
                continue
            paper_id = _paper_id(candidate, bot.bot_id)
            if paper_id in self._known_paper_ids:
                continue
            position = self._build_position(
                paper_id=paper_id,
                candidate=candidate,
                long_quote=long_quote,
                short_quote=short_quote,
                finalist_rank=finalist_rank,
                bot=bot,
            )
            if position is None:
                continue
            self._positions[paper_id] = position
            self._emitted_horizons[paper_id] = set()
            self._known_paper_ids.add(paper_id)
            self._record_episode(position)
            events.append(_registration_event(position))
        return events

    def restore_from_records(self, records: list[dict]) -> None:
        """Restore open paper orders from the local paper journal."""
        self._positions.clear()
        self._emitted_horizons.clear()
        self._skipped_horizons.clear()
        self._known_paper_ids.clear()
        self._episode_started_at_ms.clear()
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
                self._skipped_horizons.setdefault(paper_id, set())
                self._record_episode(position)
            elif kind == "opportunity.paper_hedge_filled":
                position = _position_from_registration_payload(payload)
                if position is None:
                    continue
                self._known_paper_ids.add(paper_id)
                self._positions[paper_id] = position
                self._emitted_horizons.setdefault(paper_id, set())
                self._skipped_horizons.setdefault(paper_id, set())
                self._record_episode(position)
            elif kind == "opportunity.paper_evaluation_skipped":
                self._record_episode_from_payload(payload)
                self._known_paper_ids.add(paper_id)
                horizon_kind = str(payload.get("horizon_kind", "") or "")
                if horizon_kind:
                    self._skipped_horizons.setdefault(paper_id, set()).add(horizon_kind)
            elif kind in {"opportunity.paper_markout", "opportunity.paper_closed"}:
                self._record_episode_from_payload(payload)
                self._known_paper_ids.add(paper_id)
                horizon_kind = str(payload.get("horizon_kind", "") or "")
                if horizon_kind:
                    self._emitted_horizons.setdefault(paper_id, set()).add(horizon_kind)
                if kind == "opportunity.paper_closed":
                    self._positions.pop(paper_id, None)
                    self._emitted_horizons.pop(paper_id, None)
                    self._skipped_horizons.pop(paper_id, None)

    def evaluate_due(
        self,
        now_ms: int,
        quotes: dict[str, QuoteSnapshot],
    ) -> list[dict]:
        if not self.enabled:
            return []
        events: list[dict] = self._fill_due_pending_hedges(now_ms, quotes)
        closed_ids: list[str] = []
        for position in list(self._positions.values()):
            if _position_has_pending_entry(position):
                continue
            emitted = self._emitted_horizons.setdefault(position.paper_id, set())
            due_horizons = self._pending_due_horizons(position, now_ms, emitted)
            due_horizon = due_horizons[0] if due_horizons else None
            active_check_due = (
                self.config.active_exit_enabled
                and now_ms > int(position.registered_at_ms or 0)
            )
            if active_check_due:
                skip_reason = self._exit_pricing_skip_reason(position, quotes)
                if skip_reason is not None:
                    horizon = due_horizon or {
                        "kind": "active_exit_check",
                        "due_at_ms": now_ms,
                        "terminal": False,
                    }
                    horizon_kind = str(horizon["kind"])
                    skipped = self._skipped_horizons.setdefault(position.paper_id, set())
                    if horizon_kind not in skipped:
                        events.append(
                            self._build_skipped_event(
                                position=position,
                                horizon=horizon,
                                now_ms=now_ms,
                                quotes=quotes,
                                reason=skip_reason,
                            )
                        )
                        skipped.add(horizon_kind)
                    continue
                active_horizon = self._active_exit_horizon(position, now_ms, quotes)
                if active_horizon is not None:
                    emitted.add(str(active_horizon["kind"]))
                    events.append(
                        self._build_due_event(
                            position=position,
                            horizon=active_horizon,
                            now_ms=now_ms,
                            quotes=quotes,
                        )
                    )
                    closed_ids.append(position.paper_id)
                    continue
            if not due_horizons:
                continue
            for due_horizon in due_horizons:
                horizon_kind = str(due_horizon["kind"])
                skip_reason = self._exit_pricing_skip_reason(position, quotes)
                if skip_reason is not None:
                    skipped = self._skipped_horizons.setdefault(position.paper_id, set())
                    if horizon_kind not in skipped:
                        events.append(
                            self._build_skipped_event(
                                position=position,
                                horizon=due_horizon,
                                now_ms=now_ms,
                                quotes=quotes,
                                reason=skip_reason,
                            )
                        )
                        skipped.add(horizon_kind)
                    continue
                emitted.add(horizon_kind)
                event = self._build_due_event(
                    position=position,
                    horizon=due_horizon,
                    now_ms=now_ms,
                    quotes=quotes,
                )
                events.append(event)
                if bool(due_horizon["terminal"]):
                    closed_ids.append(position.paper_id)
        for paper_id in closed_ids:
            self._positions.pop(paper_id, None)
            self._emitted_horizons.pop(paper_id, None)
            self._skipped_horizons.pop(paper_id, None)
        return events

    def _pending_due_horizons(
        self,
        position: SpreadPaperPosition,
        now_ms: int,
        emitted: set[str],
    ) -> list[dict]:
        due: list[dict] = []
        for horizon in position.due_horizons:
            horizon_kind = str(horizon["kind"])
            if int(horizon["due_at_ms"]) > now_ms:
                continue
            if horizon_kind in emitted:
                continue
            due.append(horizon)
        return due

    def _active_exit_horizon(
        self,
        position: SpreadPaperPosition,
        now_ms: int,
        quotes: dict[str, QuoteSnapshot],
    ) -> dict | None:
        reason, spread_mid_bps, z_score = self._active_exit_reason(
            position,
            now_ms,
            quotes,
        )
        if reason is None:
            return None
        return {
            "kind": f"active_exit:{reason}",
            "due_at_ms": now_ms,
            "terminal": True,
            "close_reason": reason,
            "exit_spread_mid_bps": spread_mid_bps,
            "exit_z_score": z_score,
        }

    def _active_exit_reason(
        self,
        position: SpreadPaperPosition,
        now_ms: int,
        quotes: dict[str, QuoteSnapshot],
    ) -> tuple[str | None, float | None, float | None]:
        max_hold_ms = max(int(self.config.max_hold_ms or 0), 0)
        if (
            max_hold_ms > 0
            and now_ms - int(position.registered_at_ms or 0) > max_hold_ms
        ):
            return "spread_max_hold_elapsed", None, None

        long_quote = _quote_for(quotes, position.long_venue, position.symbol)
        short_quote = _quote_for(quotes, position.short_venue, position.symbol)
        spread_mid_bps = _spread_mid_bps(long_quote, short_quote)
        z_score = _position_exit_z_score(position, spread_mid_bps)
        if z_score is None:
            return None, spread_mid_bps, None
        stop_z = max(float(self.config.stop_z or 0.0), 0.0)
        if stop_z > 0.0 and z_score >= stop_z:
            return "spread_stop_z_reached", spread_mid_bps, z_score
        exit_z = max(float(self.config.exit_z or 0.0), 0.0)
        if abs(z_score) <= exit_z:
            return "spread_converged", spread_mid_bps, z_score
        return None, spread_mid_bps, z_score

    def _exit_pricing_skip_reason(
        self,
        position: SpreadPaperPosition,
        quotes: dict[str, QuoteSnapshot],
    ) -> str | None:
        long_quote = _quote_for(quotes, position.long_venue, position.symbol)
        short_quote = _quote_for(quotes, position.short_venue, position.symbol)
        if long_quote is None or short_quote is None:
            return "missing_exit_quotes"
        long_exit_raw = _exit_raw_price(
            long_quote,
            side="long",
            role=position.long_leg.exit_liquidity_role,
        )
        short_exit_raw = _exit_raw_price(
            short_quote,
            side="short",
            role=position.short_leg.exit_liquidity_role,
        )
        if long_exit_raw <= 0.0 or short_exit_raw <= 0.0:
            return "invalid_exit_prices"
        if (
            position.long_leg.entry_raw_price is None
            or position.short_leg.entry_raw_price is None
        ):
            return "missing_entry_prices"
        return None

    def _missing_entry_quote_keys(
        self,
        position: SpreadPaperPosition,
        quotes: dict[str, QuoteSnapshot],
    ) -> set[tuple[str, str]]:
        missing: set[tuple[str, str]] = set()
        long_quote = _quote_for(quotes, position.long_venue, position.symbol)
        short_quote = _quote_for(quotes, position.short_venue, position.symbol)
        if long_quote is None or _entry_raw_price(
            long_quote,
            "long",
            position.long_leg.entry_liquidity_role,
        ) <= 0.0:
            missing.add((position.long_venue, position.symbol))
        if short_quote is None or _entry_raw_price(
            short_quote,
            "short",
            position.short_leg.entry_liquidity_role,
        ) <= 0.0:
            missing.add((position.short_venue, position.symbol))
        return missing

    def _missing_exit_quote_keys(
        self,
        position: SpreadPaperPosition,
        quotes: dict[str, QuoteSnapshot],
    ) -> set[tuple[str, str]]:
        missing: set[tuple[str, str]] = set()
        long_quote = _quote_for(quotes, position.long_venue, position.symbol)
        short_quote = _quote_for(quotes, position.short_venue, position.symbol)
        if long_quote is None or _exit_raw_price(
            long_quote,
            side="long",
            role=position.long_leg.exit_liquidity_role,
        ) <= 0.0:
            missing.add((position.long_venue, position.symbol))
        if short_quote is None or _exit_raw_price(
            short_quote,
            side="short",
            role=position.short_leg.exit_liquidity_role,
        ) <= 0.0:
            missing.add((position.short_venue, position.symbol))
        return missing

    def _build_skipped_event(
        self,
        *,
        position: SpreadPaperPosition,
        horizon: dict,
        now_ms: int,
        quotes: dict[str, QuoteSnapshot],
        reason: str,
    ) -> dict:
        long_quote = _quote_for(quotes, position.long_venue, position.symbol)
        short_quote = _quote_for(quotes, position.short_venue, position.symbol)
        payload = self._build_payload(
            position=position,
            horizon_kind=str(horizon["kind"]),
            now_ms=now_ms,
            long_quote=long_quote,
            short_quote=short_quote,
        )
        payload["paper_skip_reason"] = reason
        payload["paper_skip_terminal"] = bool(horizon["terminal"])
        return {"kind": "opportunity.paper_evaluation_skipped", "payload": payload}

    def _fill_due_pending_hedges(
        self,
        now_ms: int,
        quotes: dict[str, QuoteSnapshot],
    ) -> list[dict]:
        events: list[dict] = []
        for position in list(self._positions.values()):
            if not _position_has_pending_entry(position):
                continue
            delay_ms = max(int(position.paper_hedge_delay_ms or 0), 0)
            if now_ms < position.registered_at_ms + delay_ms:
                continue
            long_quote = _quote_for(quotes, position.long_venue, position.symbol)
            short_quote = _quote_for(quotes, position.short_venue, position.symbol)
            if long_quote is None or short_quote is None:
                continue
            filled = self._fill_pending_hedge(position, long_quote, short_quote)
            if filled is None:
                continue
            self._positions[position.paper_id] = filled
            events.append(_hedge_filled_event(filled))
        return events

    def _fill_pending_hedge(
        self,
        position: SpreadPaperPosition,
        long_quote: QuoteSnapshot,
        short_quote: QuoteSnapshot,
    ) -> SpreadPaperPosition | None:
        if position.long_leg.entry_pending:
            long_leg = self._entry_leg(
                venue=position.long_venue,
                side="long",
                role=position.long_leg.entry_liquidity_role,
                notional_quote=position.entry_notional_quote,
                quote=long_quote,
            )
            if long_leg is None:
                return None
            return replace(position, long_leg=long_leg)
        if position.short_leg.entry_pending:
            short_leg = self._entry_leg(
                venue=position.short_venue,
                side="short",
                role=position.short_leg.entry_liquidity_role,
                notional_quote=position.entry_notional_quote,
                quote=short_quote,
            )
            if short_leg is None:
                return None
            return replace(position, short_leg=short_leg)
        return position

    def _episode_in_cooldown(
        self,
        candidate: SpreadReversionCandidate,
        opportunity_label: str,
        paper_bot_id: str,
    ) -> bool:
        cooldown_ms = max(int(self.config.episode_cooldown_ms or 0), 0)
        if cooldown_ms <= 0:
            return False
        signal_ts_ms = int(candidate.signal_ts_ms or 0)
        if signal_ts_ms <= 0:
            return False
        key = _episode_key(
            candidate.symbol,
            candidate.long_venue,
            candidate.short_venue,
            opportunity_label,
            paper_bot_id,
        )
        last_started_at_ms = int(self._episode_started_at_ms.get(key, 0) or 0)
        return last_started_at_ms > 0 and signal_ts_ms - last_started_at_ms < cooldown_ms

    def _record_episode(self, position: SpreadPaperPosition) -> None:
        key = _episode_key(
            position.symbol,
            position.long_venue,
            position.short_venue,
            position.candidate_opportunity_label,
            position.paper_bot_id,
        )
        started_at_ms = int(position.registered_at_ms or 0)
        if started_at_ms <= 0:
            return
        self._episode_started_at_ms[key] = max(
            int(self._episode_started_at_ms.get(key, 0) or 0),
            started_at_ms,
        )

    def _record_episode_from_payload(self, payload: dict) -> None:
        symbol = str(payload.get("symbol", "") or "")
        long_venue = str(payload.get("long_venue", "") or "")
        short_venue = str(payload.get("short_venue", "") or "")
        label = str(payload.get("candidate_opportunity_label", "") or "spread_reversion")
        paper_bot_id = str(payload.get("paper_bot_id", "") or "tt_conservative")
        started_at_ms = int(payload.get("registered_at_ms", 0) or 0)
        if not symbol or not long_venue or not short_venue or started_at_ms <= 0:
            return
        key = _episode_key(symbol, long_venue, short_venue, label, paper_bot_id)
        self._episode_started_at_ms[key] = max(
            int(self._episode_started_at_ms.get(key, 0) or 0),
            started_at_ms,
        )

    def _build_position(
        self,
        *,
        paper_id: str,
        candidate: SpreadReversionCandidate,
        long_quote: QuoteSnapshot,
        short_quote: QuoteSnapshot,
        finalist_rank: int,
        bot: SpreadPaperBotSpec,
    ) -> SpreadPaperPosition | None:
        entry_notional = float(candidate.entry_notional_quote or 0.0)
        if entry_notional <= 0.0:
            return None
        resolved = _resolve_bot_roles(bot, long_quote, short_quote)
        delayed_leg = resolved.delayed_leg
        long_leg = self._entry_leg(
            venue=candidate.long_venue,
            side="long",
            role=resolved.entry_long_role,
            notional_quote=entry_notional,
            quote=long_quote,
            pending=delayed_leg == "long",
        )
        short_leg = self._entry_leg(
            venue=candidate.short_venue,
            side="short",
            role=resolved.entry_short_role,
            notional_quote=entry_notional,
            quote=short_quote,
            pending=delayed_leg == "short",
        )
        if long_leg is None or short_leg is None:
            return None
        long_leg = replace(long_leg, exit_liquidity_role=resolved.exit_long_role)
        short_leg = replace(short_leg, exit_liquidity_role=resolved.exit_short_role)
        entry_mode = f"long_{resolved.entry_long_role}:short_{resolved.entry_short_role}"
        exit_mode = f"long_{resolved.exit_long_role}:short_{resolved.exit_short_role}"
        return SpreadPaperPosition(
            paper_id=paper_id,
            candidate_id=candidate.candidate_id,
            symbol=candidate.symbol,
            long_venue=candidate.long_venue,
            short_venue=candidate.short_venue,
            candidate_opportunity_label=str(
                getattr(candidate, "opportunity_label", "") or "spread_reversion"
            ),
            paper_bot_id=resolved.bot_id,
            paper_cohort=resolved.cohort,
            paper_entry_mode=entry_mode,
            paper_exit_mode=exit_mode,
            paper_maker_leg=resolved.maker_leg,
            paper_hedge_delay_ms=resolved.hedge_delay_ms,
            paper_control_group=resolved.control_group,
            paper_fill_assumption=_fill_assumption(resolved),
            finalist_rank=finalist_rank,
            registered_at_ms=int(candidate.signal_ts_ms or 0),
            entry_notional_quote=entry_notional,
            long_leg=long_leg,
            short_leg=short_leg,
            candidate_snapshot=_candidate_snapshot(candidate),
            entry_market_snapshot=_market_snapshot_payload(long_quote, short_quote),
            due_horizons=self._due_horizons(int(candidate.signal_ts_ms or 0)),
        )

    def _entry_leg(
        self,
        *,
        venue: str,
        side: str,
        role: str,
        notional_quote: float,
        quote: QuoteSnapshot,
        pending: bool = False,
    ) -> SpreadPaperLeg | None:
        if notional_quote <= 0.0:
            return None
        role = _liquidity_role(role)
        raw_price = None if pending else _entry_raw_price(quote, side, role)
        if raw_price is not None and raw_price <= 0.0:
            return None
        entry_price = None
        qty = 0.0
        entry_slippage_quote = 0.0
        entry_fee_bps = _fee_bps(self.config, venue, role)
        entry_fee_quote = 0.0
        if raw_price is not None:
            entry_price = _apply_slippage(
                raw_price,
                bps=self.config.slippage_buffer_bps,
                action="buy" if side == "long" else "sell",
            )
            qty = notional_quote / entry_price
            entry_slippage_quote = abs(entry_price - raw_price) * qty
            entry_fee_quote = notional_quote * entry_fee_bps / 10_000.0
        return SpreadPaperLeg(
            venue=str(venue).lower(),
            side=side,
            entry_liquidity_role=role,
            exit_liquidity_role="taker",
            entry_pending=pending,
            entry_bid=float(getattr(quote, "bid", 0.0) or 0.0),
            entry_ask=float(getattr(quote, "ask", 0.0) or 0.0),
            entry_bid_size=float(getattr(quote, "bid_size", 0.0) or 0.0),
            entry_ask_size=float(getattr(quote, "ask_size", 0.0) or 0.0),
            entry_observed_at_ms=int(getattr(quote, "observed_at_ms", 0) or 0),
            mark_price=float(getattr(quote, "mark_price", 0.0) or 0.0),
            index_price=float(getattr(quote, "index_price", 0.0) or 0.0),
            volume_24h_quote=float(getattr(quote, "volume_24h_quote", 0.0) or 0.0),
            open_interest=float(getattr(quote, "open_interest", 0.0) or 0.0),
            entry_raw_price=raw_price,
            entry_price=entry_price,
            qty=qty,
            entry_notional_quote=notional_quote,
            entry_fee_bps=entry_fee_bps,
            entry_fee_quote=entry_fee_quote,
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
        close_reason = str(horizon.get("close_reason", "") or "")
        if close_reason:
            payload["paper_close_reason"] = close_reason
        if horizon.get("exit_spread_mid_bps") is not None:
            payload["paper_exit_spread_mid_bps"] = float(horizon["exit_spread_mid_bps"])
        if horizon.get("exit_z_score") is not None:
            payload["paper_exit_z_score"] = float(horizon["exit_z_score"])
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
            "paper_bot_id": position.paper_bot_id,
            "paper_cohort": position.paper_cohort,
            "paper_entry_mode": position.paper_entry_mode,
            "paper_exit_mode": position.paper_exit_mode,
            "paper_execution_model": position.paper_entry_mode,
            "paper_maker_leg": position.paper_maker_leg,
            "paper_hedge_delay_ms": position.paper_hedge_delay_ms,
            "paper_control_group": position.paper_control_group,
            "paper_fill_assumption": position.paper_fill_assumption,
            "paper_order_status": (
                "entry_pending" if _position_has_pending_entry(position) else "hedged"
            ),
            "paper_entry_notional_quote": position.entry_notional_quote,
            "candidate_snapshot": dict(position.candidate_snapshot),
            "entry_market_snapshot": dict(position.entry_market_snapshot),
            "exit_market_snapshot": _market_snapshot_payload(long_quote, short_quote),
            "funding_advantage_bps": (
                position.short_leg.funding_rate_bps - position.long_leg.funding_rate_bps
            ),
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
        paper_net_quote = markout.get("paper_net_quote")
        paper_net_bps = (
            paper_net_quote / position.entry_notional_quote * 10_000.0
            if paper_net_quote is not None
            else None
        )
        base_payload.update(markout)
        base_payload.update(
            {
                "paper_net_bps": paper_net_bps,
                "opportunity_label": classify_paper_outcome(
                    False,
                    paper_net_quote,
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
        long_exit_raw = _exit_raw_price(
            long_quote,
            side="long",
            role=position.long_leg.exit_liquidity_role,
        )
        short_exit_raw = _exit_raw_price(
            short_quote,
            side="short",
            role=position.short_leg.exit_liquidity_role,
        )
        if (
            long_exit_raw <= 0.0
            or short_exit_raw <= 0.0
            or position.long_leg.entry_raw_price is None
            or position.short_leg.entry_raw_price is None
        ):
            return {
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
                "long_leg": _leg_payload(position.long_leg),
                "short_leg": _leg_payload(position.short_leg),
            }
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
        long_exit_fee_bps = _fee_bps(
            self.config,
            position.long_venue,
            position.long_leg.exit_liquidity_role,
        )
        short_exit_fee_bps = _fee_bps(
            self.config,
            position.short_venue,
            position.short_leg.exit_liquidity_role,
        )
        long_exit_fee = long_exit_notional * long_exit_fee_bps / 10_000.0
        short_exit_fee = (
            short_exit_notional * short_exit_fee_bps / 10_000.0
        )
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
                exit_fee_bps=long_exit_fee_bps,
                exit_slippage_quote=abs(long_exit_raw - long_exit) * position.long_leg.qty,
                gross_quote=long_gross,
            ),
            "short_leg": _leg_payload(
                position.short_leg,
                exit_raw_price=short_exit_raw,
                exit_price=short_exit,
                exit_fee_quote=short_exit_fee,
                exit_fee_bps=short_exit_fee_bps,
                exit_slippage_quote=abs(short_exit - short_exit_raw)
                * position.short_leg.qty,
                gross_quote=short_gross,
            ),
        }


def _excluded_symbols(config: SpreadPaperConfig) -> set[str]:
    return {
        str(symbol).upper()
        for symbol in (config.excluded_symbols or [])
        if str(symbol).strip()
    }


def _paper_bot_specs(config: SpreadPaperConfig) -> list[SpreadPaperBotSpec]:
    specs: list[SpreadPaperBotSpec] = []
    for raw_bot_id in config.paper_bot_ids or ["tt_conservative"]:
        spec = _paper_bot_spec(str(raw_bot_id or "").strip())
        if spec is not None:
            specs.append(spec)
    if not specs:
        specs.append(_paper_bot_spec("tt_conservative"))
    return [spec for spec in specs if spec is not None]


def _paper_bot_spec(bot_id: str) -> SpreadPaperBotSpec | None:
    if bot_id == "tt_conservative":
        return SpreadPaperBotSpec(bot_id=bot_id, cohort="baseline_current")
    if bot_id == "mt_long_maker":
        return SpreadPaperBotSpec(
            bot_id=bot_id,
            cohort="maker_taker_control",
            entry_long_role="maker",
            entry_short_role="taker",
            maker_leg="long",
            control_group=True,
        )
    if bot_id == "mt_short_maker":
        return SpreadPaperBotSpec(
            bot_id=bot_id,
            cohort="maker_taker_control",
            entry_long_role="taker",
            entry_short_role="maker",
            maker_leg="short",
            control_group=True,
        )
    if bot_id == "mt_selected_maker":
        return SpreadPaperBotSpec(
            bot_id=bot_id,
            cohort="maker_taker_control",
            entry_long_role="auto",
            entry_short_role="auto",
            maker_leg="auto",
            control_group=True,
        )
    if bot_id == "mt_selected_maker_delay_1000ms":
        return SpreadPaperBotSpec(
            bot_id=bot_id,
            cohort="maker_taker_delay_control",
            entry_long_role="auto",
            entry_short_role="auto",
            maker_leg="auto",
            hedge_delay_ms=1000,
            delayed_leg="hedge",
            control_group=True,
        )
    if bot_id == "core_v1_bot":
        return SpreadPaperBotSpec(
            bot_id=bot_id,
            cohort="core_v1",
            min_executable_spread_bps=80.0,
            min_net_edge_bps=80.0,
            require_good_pair=True,
        )
    if bot_id == "core_v1_exec100_bot":
        return SpreadPaperBotSpec(
            bot_id=bot_id,
            cohort="core_v1_exec100",
            min_executable_spread_bps=100.0,
            min_net_edge_bps=80.0,
            require_good_pair=True,
        )
    if bot_id == "core_v1_z10_bot":
        return SpreadPaperBotSpec(
            bot_id=bot_id,
            cohort="core_v1_z10",
            min_executable_spread_bps=80.0,
            min_net_edge_bps=80.0,
            min_z_score=10.0,
            require_good_pair=True,
        )
    if bot_id == "bad_pair_control_bot":
        return SpreadPaperBotSpec(
            bot_id=bot_id,
            cohort="bad_pair_control",
            require_bad_pair=True,
            control_group=True,
        )
    if bot_id == "low_liquidity_control_bot":
        return SpreadPaperBotSpec(
            bot_id=bot_id,
            cohort="low_liquidity_control",
            require_low_liquidity=True,
            control_group=True,
        )
    if bot_id == "low_edge_control_bot":
        return SpreadPaperBotSpec(
            bot_id=bot_id,
            cohort="low_edge_control",
            require_low_edge=True,
            control_group=True,
        )
    return None


def _resolve_bot_roles(
    bot: SpreadPaperBotSpec,
    long_quote: QuoteSnapshot,
    short_quote: QuoteSnapshot,
) -> SpreadPaperBotSpec:
    if bot.maker_leg != "auto":
        return bot
    maker_leg = _selected_maker_leg(long_quote, short_quote)
    delayed_leg = bot.delayed_leg
    if delayed_leg == "hedge":
        delayed_leg = "short" if maker_leg == "long" else "long"
    return replace(
        bot,
        entry_long_role="maker" if maker_leg == "long" else "taker",
        entry_short_role="maker" if maker_leg == "short" else "taker",
        maker_leg=maker_leg,
        delayed_leg=delayed_leg,
    )


def _selected_maker_leg(long_quote: QuoteSnapshot, short_quote: QuoteSnapshot) -> str:
    long_spread = float(getattr(long_quote, "ask", 0.0) or 0.0) - float(
        getattr(long_quote, "bid", 0.0) or 0.0
    )
    short_spread = float(getattr(short_quote, "ask", 0.0) or 0.0) - float(
        getattr(short_quote, "bid", 0.0) or 0.0
    )
    return "long" if long_spread >= short_spread else "short"


def _bot_accepts_candidate(
    bot: SpreadPaperBotSpec,
    candidate: SpreadReversionCandidate,
    long_quote: QuoteSnapshot,
    short_quote: QuoteSnapshot,
) -> bool:
    executable_spread_bps = float(getattr(candidate, "executable_spread_bps", 0.0) or 0.0)
    net_edge_bps = float(getattr(candidate, "net_edge_bps", 0.0) or 0.0)
    z_score = float(getattr(candidate, "z_score", 0.0) or 0.0)
    if (
        bot.min_executable_spread_bps is not None
        and executable_spread_bps < bot.min_executable_spread_bps
    ):
        return False
    if bot.min_net_edge_bps is not None and net_edge_bps < bot.min_net_edge_bps:
        return False
    if bot.min_z_score is not None and z_score < bot.min_z_score:
        return False
    pair = (
        str(getattr(candidate, "long_venue", "") or "").lower(),
        str(getattr(candidate, "short_venue", "") or "").lower(),
    )
    if bot.require_good_pair and pair not in GOOD_SPREAD_PAIRS:
        return False
    if bot.require_bad_pair and pair not in BAD_SPREAD_PAIRS:
        return False
    if bot.require_low_edge and not (executable_spread_bps < 60.0 or net_edge_bps < 40.0):
        return False
    if bot.require_low_liquidity and not _is_low_liquidity_candidate(
        candidate,
        long_quote,
        short_quote,
    ):
        return False
    return True


def _is_low_liquidity_candidate(
    candidate: SpreadReversionCandidate,
    long_quote: QuoteSnapshot,
    short_quote: QuoteSnapshot,
) -> bool:
    capacity_quote = float(getattr(candidate, "capacity_quote", 0.0) or 0.0)
    long_volume = float(getattr(long_quote, "volume_24h_quote", 0.0) or 0.0)
    short_volume = float(getattr(short_quote, "volume_24h_quote", 0.0) or 0.0)
    min_volume = min(long_volume, short_volume)
    return capacity_quote < 50.0 or min_volume < 50_000.0


def _fill_assumption(bot: SpreadPaperBotSpec) -> str:
    if bot.hedge_delay_ms > 0:
        return "conditional_maker_fill_then_delayed_taker_hedge"
    if bot.maker_leg:
        return "conditional_maker_fill_then_taker_hedge"
    return "taker_top_of_book"


def _allowed_labels(config: SpreadPaperConfig) -> set[str]:
    return {
        str(label)
        for label in (config.allowed_opportunity_labels or [])
        if str(label).strip()
    }


def _episode_key(
    symbol: str,
    long_venue: str,
    short_venue: str,
    opportunity_label: str,
    paper_bot_id: str,
) -> tuple[str, str, str, str, str]:
    return (
        str(symbol).upper(),
        str(long_venue).lower(),
        str(short_venue).lower(),
        str(opportunity_label or "spread_reversion"),
        str(paper_bot_id or "tt_conservative"),
    )


def _candidate_snapshot(candidate: SpreadReversionCandidate) -> dict:
    return {
        "executable_spread_bps": float(
            getattr(candidate, "executable_spread_bps", 0.0) or 0.0
        ),
        "spread_mid_bps": float(getattr(candidate, "spread_mid_bps", 0.0) or 0.0),
        "rolling_mean_bps": float(
            getattr(candidate, "rolling_mean_bps", 0.0) or 0.0
        ),
        "rolling_std_bps": float(getattr(candidate, "rolling_std_bps", 0.0) or 0.0),
        "z_score": float(getattr(candidate, "z_score", 0.0) or 0.0),
        "net_edge_bps": float(getattr(candidate, "net_edge_bps", 0.0) or 0.0),
        "fair_price": float(getattr(candidate, "fair_price", 0.0) or 0.0),
        "venue_premium_bps": float(getattr(candidate, "venue_premium_bps", 0.0) or 0.0),
        "capacity_quote": float(getattr(candidate, "capacity_quote", 0.0) or 0.0),
        "liquidity_evidence_status": str(
            getattr(candidate, "liquidity_evidence_status", "") or ""
        ),
        "fee_bps": float(getattr(candidate, "fee_bps", 0.0) or 0.0),
        "slippage_reserve_bps": float(
            getattr(candidate, "slippage_reserve_bps", 0.0) or 0.0
        ),
        "funding_carry_bps": float(
            getattr(candidate, "funding_carry_bps", 0.0) or 0.0
        ),
        "funding_carry_cost_bps": float(
            getattr(candidate, "funding_carry_cost_bps", 0.0) or 0.0
        ),
    }


def _market_snapshot_payload(
    long_quote: QuoteSnapshot | None,
    short_quote: QuoteSnapshot | None,
) -> dict:
    return {
        "long_quote": _quote_payload(long_quote),
        "short_quote": _quote_payload(short_quote),
    }


def _quote_payload(quote: QuoteSnapshot | None) -> dict | None:
    if quote is None:
        return None
    return {
        "venue": str(getattr(quote, "venue", "") or "").lower(),
        "symbol": str(getattr(quote, "symbol", "") or "").upper(),
        "source": str(getattr(quote, "source", "") or ""),
        "bid": float(getattr(quote, "bid", 0.0) or 0.0),
        "ask": float(getattr(quote, "ask", 0.0) or 0.0),
        "bid_size": float(getattr(quote, "bid_size", 0.0) or 0.0),
        "ask_size": float(getattr(quote, "ask_size", 0.0) or 0.0),
        "observed_at_ms": int(getattr(quote, "observed_at_ms", 0) or 0),
        "funding_rate_bps": float(getattr(quote, "funding_rate_bps", 0.0) or 0.0),
        "funding_timestamp_ms": int(getattr(quote, "funding_timestamp_ms", 0) or 0),
        "mark_price": float(getattr(quote, "mark_price", 0.0) or 0.0),
        "index_price": float(getattr(quote, "index_price", 0.0) or 0.0),
        "volume_24h_quote": float(getattr(quote, "volume_24h_quote", 0.0) or 0.0),
        "open_interest": float(getattr(quote, "open_interest", 0.0) or 0.0),
    }


def _dict_payload(payload: object) -> dict:
    return dict(payload) if isinstance(payload, dict) else {}


def _paper_id(candidate: SpreadReversionCandidate, paper_bot_id: str = "tt_conservative") -> str:
    base = f"spread:{candidate.candidate_id}:{int(candidate.signal_ts_ms or 0)}"
    if str(paper_bot_id or "tt_conservative") == "tt_conservative":
        return base
    return f"{base}:bot:{paper_bot_id}"


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
            "paper_bot_id": position.paper_bot_id,
            "paper_cohort": position.paper_cohort,
            "paper_entry_mode": position.paper_entry_mode,
            "paper_exit_mode": position.paper_exit_mode,
            "paper_execution_model": position.paper_entry_mode,
            "paper_maker_leg": position.paper_maker_leg,
            "paper_hedge_delay_ms": position.paper_hedge_delay_ms,
            "paper_control_group": position.paper_control_group,
            "paper_fill_assumption": position.paper_fill_assumption,
            "finalist_rank": position.finalist_rank,
            "registered_at_ms": position.registered_at_ms,
            "selected_real_trade": False,
            "not_selected_reason": "spread_shadow_paper",
            "paper_order_status": (
                "entry_pending" if _position_has_pending_entry(position) else "open"
            ),
            "paper_entry_notional_quote": position.entry_notional_quote,
            "candidate_snapshot": dict(position.candidate_snapshot),
            "entry_market_snapshot": dict(position.entry_market_snapshot),
            "funding_advantage_bps": (
                position.short_leg.funding_rate_bps - position.long_leg.funding_rate_bps
            ),
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


def _hedge_filled_event(position: SpreadPaperPosition) -> dict:
    payload = _registration_event(position)["payload"]
    payload["paper_order_status"] = "hedged"
    return {"kind": "opportunity.paper_hedge_filled", "payload": payload}


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
        paper_bot_id=str(payload.get("paper_bot_id", "") or "tt_conservative"),
        paper_cohort=str(payload.get("paper_cohort", "") or "baseline_current"),
        paper_entry_mode=str(payload.get("paper_entry_mode", "") or "long_taker:short_taker"),
        paper_exit_mode=str(payload.get("paper_exit_mode", "") or "long_taker:short_taker"),
        paper_maker_leg=str(payload.get("paper_maker_leg", "") or ""),
        paper_hedge_delay_ms=int(payload.get("paper_hedge_delay_ms", 0) or 0),
        paper_control_group=bool(payload.get("paper_control_group", False)),
        paper_fill_assumption=str(
            payload.get("paper_fill_assumption", "") or "taker_top_of_book"
        ),
        finalist_rank=int(payload.get("finalist_rank", 0) or 0),
        registered_at_ms=int(payload.get("registered_at_ms", 0) or 0),
        entry_notional_quote=float(payload.get("paper_entry_notional_quote", 0.0) or 0.0),
        long_leg=long_leg,
        short_leg=short_leg,
        candidate_snapshot=_dict_payload(payload.get("candidate_snapshot", {})),
        entry_market_snapshot=_dict_payload(payload.get("entry_market_snapshot", {})),
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
        entry_liquidity_role=str(payload.get("entry_liquidity_role", "") or "taker"),
        exit_liquidity_role=str(payload.get("exit_liquidity_role", "") or "taker"),
        entry_pending=bool(payload.get("entry_pending", False)),
        entry_bid=float(payload.get("entry_bid", 0.0) or 0.0),
        entry_ask=float(payload.get("entry_ask", 0.0) or 0.0),
        entry_bid_size=float(payload.get("entry_bid_size", 0.0) or 0.0),
        entry_ask_size=float(payload.get("entry_ask_size", 0.0) or 0.0),
        entry_observed_at_ms=int(payload.get("entry_observed_at_ms", 0) or 0),
        mark_price=float(payload.get("mark_price", 0.0) or 0.0),
        index_price=float(payload.get("index_price", 0.0) or 0.0),
        volume_24h_quote=float(payload.get("volume_24h_quote", 0.0) or 0.0),
        open_interest=float(payload.get("open_interest", 0.0) or 0.0),
        entry_raw_price=_optional_float(payload.get("entry_raw_price")),
        entry_price=_optional_float(payload.get("entry_price")),
        qty=float(payload.get("qty", 0.0) or 0.0),
        entry_notional_quote=float(payload.get("entry_notional_quote", 0.0) or 0.0),
        entry_fee_bps=float(payload.get("entry_fee_bps", 0.0) or 0.0),
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


def _fee_bps(config: SpreadPaperConfig, venue: str, role: str = "taker") -> float:
    venue_key = str(venue).lower()
    taker_fee_bps = float(config.taker_fee_bps_by_venue.get(venue_key, 0.0) or 0.0)
    if _liquidity_role(role) == "maker":
        return float(config.maker_fee_bps_by_venue.get(venue_key, taker_fee_bps) or 0.0)
    return taker_fee_bps


def _liquidity_role(role: str) -> str:
    normalized = str(role or "taker").lower()
    return "maker" if normalized == "maker" else "taker"


def _entry_raw_price(quote: QuoteSnapshot, side: str, role: str) -> float:
    role = _liquidity_role(role)
    if side == "long":
        return float(getattr(quote, "bid" if role == "maker" else "ask", 0.0) or 0.0)
    return float(getattr(quote, "ask" if role == "maker" else "bid", 0.0) or 0.0)


def _exit_raw_price(quote: QuoteSnapshot, side: str, role: str) -> float:
    role = _liquidity_role(role)
    if side == "long":
        return float(getattr(quote, "ask" if role == "maker" else "bid", 0.0) or 0.0)
    return float(getattr(quote, "bid" if role == "maker" else "ask", 0.0) or 0.0)


def _apply_slippage(raw_price: float, *, bps: float, action: str) -> float:
    factor = max(float(bps or 0.0), 0.0) / 10_000.0
    if action == "buy":
        return raw_price * (1.0 + factor)
    return raw_price * (1.0 - factor)


def _entry_fee_quote(position: SpreadPaperPosition) -> float:
    return position.long_leg.entry_fee_quote + position.short_leg.entry_fee_quote


def _entry_slippage_quote(position: SpreadPaperPosition) -> float:
    return position.long_leg.entry_slippage_quote + position.short_leg.entry_slippage_quote


def _position_has_pending_entry(position: SpreadPaperPosition) -> bool:
    return bool(position.long_leg.entry_pending or position.short_leg.entry_pending)


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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
    exit_fee_bps: float = 0.0,
    exit_fee_quote: float = 0.0,
    exit_slippage_quote: float = 0.0,
    gross_quote: float | None = None,
) -> dict:
    return {
        "venue": leg.venue,
        "side": leg.side,
        "entry_liquidity_role": leg.entry_liquidity_role,
        "exit_liquidity_role": leg.exit_liquidity_role,
        "entry_pending": leg.entry_pending,
        "entry_bid": leg.entry_bid,
        "entry_ask": leg.entry_ask,
        "entry_bid_size": leg.entry_bid_size,
        "entry_ask_size": leg.entry_ask_size,
        "entry_observed_at_ms": leg.entry_observed_at_ms,
        "mark_price": leg.mark_price,
        "index_price": leg.index_price,
        "volume_24h_quote": leg.volume_24h_quote,
        "open_interest": leg.open_interest,
        "entry_raw_price": leg.entry_raw_price,
        "entry_price": leg.entry_price,
        "exit_raw_price": exit_raw_price,
        "exit_price": exit_price,
        "qty": leg.qty,
        "entry_notional_quote": leg.entry_notional_quote,
        "entry_fee_bps": leg.entry_fee_bps,
        "entry_fee_quote": leg.entry_fee_quote,
        "exit_fee_bps": exit_fee_bps,
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


def _spread_mid_bps(
    long_quote: QuoteSnapshot | None,
    short_quote: QuoteSnapshot | None,
) -> float | None:
    long_mid = _mid(long_quote)
    short_mid = _mid(short_quote)
    if long_mid is None or short_mid is None:
        return None
    reference_mid = (long_mid + short_mid) / 2.0
    if reference_mid <= 0.0:
        return None
    return ((short_mid - long_mid) / reference_mid) * 10_000.0


def _position_exit_z_score(
    position: SpreadPaperPosition,
    spread_mid_bps: float | None,
) -> float | None:
    if spread_mid_bps is None:
        return None
    snapshot = position.candidate_snapshot or {}
    rolling_mean = float(snapshot.get("rolling_mean_bps", 0.0) or 0.0)
    rolling_std = float(snapshot.get("rolling_std_bps", 0.0) or 0.0)
    if rolling_std <= 0.0:
        return None
    return (spread_mid_bps - rolling_mean) / rolling_std
