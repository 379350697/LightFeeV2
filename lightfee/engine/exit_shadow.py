"""Read-only exit shadow strategies and markout tracking.

The advisor records what a family of close-ordering bots would have preferred
at the instant a real close trigger fired. It never submits, cancels, delays, or
modifies real close execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lightfee.marketdata.l2 import LocalL2Book


DIRECTION_BULLISH = "bullish"
DIRECTION_BEARISH = "bearish"
DIRECTION_NEUTRAL = "neutral"

PATH_SIMULTANEOUS = "simultaneous_close"
PATH_SHORT_FIRST = "short_first_then_long"
PATH_LONG_FIRST = "long_first_then_short"

DEFAULT_BOT_IDS = (
    "top_book_imbalance",
    "multi_level_l2_imbalance",
    "micro_mid_momentum",
    "cross_venue_pressure",
    "cost_aware_vote",
)
PATHS = (PATH_SIMULTANEOUS, PATH_SHORT_FIRST, PATH_LONG_FIRST)


@dataclass(frozen=True)
class ExitShadowConfig:
    enabled: bool = False
    bot_ids: tuple[str, ...] = DEFAULT_BOT_IDS
    markout_horizons_ms: tuple[int, ...] = (1000, 2000, 5000)
    take_profit_bps: tuple[float, ...] = (10.0, 20.0)
    adverse_stop_bps: float = 3.0
    max_quote_age_ms: int = 1000
    max_l2_age_ms: int = 1000
    cost_buffer_bps: float = 3.0
    l2_depth_levels: int = 3


@dataclass(frozen=True)
class ExitShadowQuote:
    venue: str
    symbol: str
    bid: float
    ask: float
    bid_size: float = 0.0
    ask_size: float = 0.0
    observed_at_ms: int = 0
    source: str = ""

    @property
    def mid(self) -> float:
        if self.bid <= 0.0 or self.ask <= self.bid:
            return 0.0
        return (self.bid + self.ask) / 2.0

    @property
    def spread_bps(self) -> float:
        mid = self.mid
        if mid <= 0.0:
            return 0.0
        return (self.ask - self.bid) / mid * 10000.0


@dataclass(frozen=True)
class ExitShadowMarket:
    long_quote: ExitShadowQuote | None = None
    short_quote: ExitShadowQuote | None = None
    long_book: LocalL2Book | None = None
    short_book: LocalL2Book | None = None
    now_ms: int = 0


@dataclass(frozen=True)
class ExitShadowSnapshot:
    position: Any
    reason: str
    market: ExitShadowMarket


@dataclass(frozen=True)
class ExitShadowDecision:
    bot_id: str
    direction: str
    confidence: float
    recommended_path: str
    reason: str
    features: dict[str, Any] = field(default_factory=dict)


@dataclass
class _PendingShadow:
    shadow_id: str
    snapshot: ExitShadowSnapshot
    decisions: list[ExitShadowDecision]
    emitted_horizons_ms: set[int] = field(default_factory=lambda: {0})


def recommended_path_for_direction(direction: str) -> str:
    if direction == DIRECTION_BULLISH:
        return PATH_SHORT_FIRST
    if direction == DIRECTION_BEARISH:
        return PATH_LONG_FIRST
    return PATH_SIMULTANEOUS


def evaluate_exit_shadow_strategies(
    snapshot: ExitShadowSnapshot,
    config: ExitShadowConfig,
) -> list[ExitShadowDecision]:
    if not config.enabled:
        return []

    decisions: list[ExitShadowDecision] = []
    preliminary: dict[str, ExitShadowDecision] = {}
    for bot_id in config.bot_ids:
        if bot_id == "cost_aware_vote":
            decision = _cost_aware_vote(snapshot.market, config, preliminary)
        else:
            decision = _evaluate_single_bot(bot_id, snapshot.market, config)
        decisions.append(decision)
        preliminary[bot_id] = decision
    return decisions


class ExitShadowTracker:
    def __init__(self, config: ExitShadowConfig) -> None:
        self.config = config
        self._pending: dict[str, _PendingShadow] = {}

    def on_close_trigger(self, snapshot: ExitShadowSnapshot) -> list[dict[str, Any]]:
        if not self.config.enabled:
            return []
        decisions = evaluate_exit_shadow_strategies(snapshot, self.config)
        shadow_id = _shadow_id(snapshot)
        pending = _PendingShadow(shadow_id=shadow_id, snapshot=snapshot, decisions=decisions)
        self._pending[shadow_id] = pending

        events: list[dict[str, Any]] = []
        for decision in decisions:
            events.append(
                _strategy_decision_event(
                    shadow_id,
                    snapshot,
                    decision,
                    self.config,
                )
            )
        for path in PATHS:
            events.append(
                _path_markout_event(
                    shadow_id,
                    snapshot,
                    snapshot.market,
                    path=path,
                    horizon_ms=0,
                    config=self.config,
                )
            )
        return events

    def evaluate_markouts(self, market: ExitShadowMarket) -> list[dict[str, Any]]:
        if not self.config.enabled:
            return []
        events: list[dict[str, Any]] = []
        for shadow_id in list(self._pending):
            events.extend(self.evaluate_markouts_for_shadow(shadow_id, market))
        return events

    def evaluate_markouts_for_shadow(
        self,
        shadow_id: str,
        market: ExitShadowMarket,
    ) -> list[dict[str, Any]]:
        pending = self._pending.get(shadow_id)
        if pending is None or not self.config.enabled:
            return []
        elapsed_ms = max(market.now_ms - pending.snapshot.market.now_ms, 0)
        due_horizons = [
            int(horizon)
            for horizon in self.config.markout_horizons_ms
            if horizon not in pending.emitted_horizons_ms and elapsed_ms >= horizon
        ]
        if not due_horizons:
            return []
        events: list[dict[str, Any]] = []
        for horizon_ms in due_horizons:
            path_net_bps: dict[str, float] = {}
            path_adverse_bps: dict[str, float] = {}
            for path in PATHS:
                event = _path_markout_event(
                    shadow_id,
                    pending.snapshot,
                    market,
                    path=path,
                    horizon_ms=horizon_ms,
                    config=self.config,
                )
                payload = event["payload"]
                path_net_bps[path] = float(payload.get("net_bps", 0.0) or 0.0)
                path_adverse_bps[path] = float(
                    payload.get("max_adverse_bps", 0.0) or 0.0
                )
                events.append(event)
            for decision in pending.decisions:
                events.append(
                    _strategy_summary_event(
                        shadow_id,
                        pending.snapshot,
                        market,
                        decision,
                        horizon_ms=horizon_ms,
                        path_net_bps=path_net_bps,
                        path_adverse_bps=path_adverse_bps,
                        config=self.config,
                    )
                )
            pending.emitted_horizons_ms.add(horizon_ms)
        if all(
            int(horizon) in pending.emitted_horizons_ms
            for horizon in self.config.markout_horizons_ms
        ):
            self._pending.pop(shadow_id, None)
        return events

    def pending_items(self) -> list[tuple[str, ExitShadowSnapshot]]:
        return [
            (shadow_id, pending.snapshot)
            for shadow_id, pending in self._pending.items()
        ]


def _evaluate_single_bot(
    bot_id: str,
    market: ExitShadowMarket,
    config: ExitShadowConfig,
) -> ExitShadowDecision:
    if bot_id == "top_book_imbalance":
        stale = _quote_stale_reason(market, config)
        if stale:
            return _neutral(bot_id, stale)
        imbalance = _top_book_imbalance(market)
        return _decision_from_imbalance(bot_id, imbalance, "top_book_imbalance")

    if bot_id == "multi_level_l2_imbalance":
        stale = _l2_stale_reason(market, config)
        if stale:
            return _neutral(bot_id, stale)
        imbalance = _multi_level_l2_imbalance(market, config.l2_depth_levels)
        return _decision_from_imbalance(bot_id, imbalance, "multi_level_l2_imbalance")

    if bot_id == "micro_mid_momentum":
        stale = _quote_stale_reason(market, config)
        if stale:
            return _neutral(bot_id, stale)
        return _neutral(bot_id, "missing_mid_momentum_history")

    if bot_id == "cross_venue_pressure":
        stale = _quote_stale_reason(market, config)
        if stale:
            return _neutral(bot_id, stale)
        imbalance = _top_book_imbalance(market)
        premium_bps = _cross_venue_premium_bps(market)
        score = imbalance - min(max(premium_bps / 200.0, -0.25), 0.25)
        return _decision_from_imbalance(bot_id, score, "cross_venue_pressure")

    return _neutral(bot_id, "unknown_bot")


def _cost_aware_vote(
    market: ExitShadowMarket,
    config: ExitShadowConfig,
    prior: dict[str, ExitShadowDecision],
) -> ExitShadowDecision:
    stale = _quote_stale_reason(market, config)
    if stale:
        return _neutral("cost_aware_vote", stale)

    bullish = sum(1 for d in prior.values() if d.direction == DIRECTION_BULLISH)
    bearish = sum(1 for d in prior.values() if d.direction == DIRECTION_BEARISH)
    confidence = 0.01
    direction = DIRECTION_NEUTRAL
    if bullish > bearish:
        direction = DIRECTION_BULLISH
    elif bearish > bullish:
        direction = DIRECTION_BEARISH

    average_confidence = (
        sum(float(d.confidence) for d in prior.values()) / max(len(prior), 1)
    )
    estimated_edge_bps = abs(_top_book_imbalance(market)) * 20.0
    confidence = max(average_confidence, 0.01)
    if direction == DIRECTION_NEUTRAL or estimated_edge_bps < config.cost_buffer_bps:
        return ExitShadowDecision(
            bot_id="cost_aware_vote",
            direction=DIRECTION_NEUTRAL,
            confidence=confidence,
            recommended_path=PATH_SIMULTANEOUS,
            reason="edge_below_cost_buffer",
            features={
                "bullish_votes": bullish,
                "bearish_votes": bearish,
                "estimated_edge_bps": estimated_edge_bps,
                "cost_buffer_bps": config.cost_buffer_bps,
            },
        )

    return ExitShadowDecision(
        bot_id="cost_aware_vote",
        direction=direction,
        confidence=min(max(confidence, 0.01), 1.0),
        recommended_path=recommended_path_for_direction(direction),
        reason="vote_edge_above_cost_buffer",
        features={
            "bullish_votes": bullish,
            "bearish_votes": bearish,
            "estimated_edge_bps": estimated_edge_bps,
            "cost_buffer_bps": config.cost_buffer_bps,
        },
    )


def _decision_from_imbalance(bot_id: str, imbalance: float, reason: str) -> ExitShadowDecision:
    threshold = 0.10
    confidence = min(max(abs(imbalance), 0.01), 1.0)
    if imbalance > threshold:
        direction = DIRECTION_BULLISH
    elif imbalance < -threshold:
        direction = DIRECTION_BEARISH
    else:
        direction = DIRECTION_NEUTRAL
    return ExitShadowDecision(
        bot_id=bot_id,
        direction=direction,
        confidence=confidence,
        recommended_path=recommended_path_for_direction(direction),
        reason=reason if direction != DIRECTION_NEUTRAL else f"{reason}_neutral",
        features={"imbalance": imbalance},
    )


def _neutral(bot_id: str, reason: str) -> ExitShadowDecision:
    return ExitShadowDecision(
        bot_id=bot_id,
        direction=DIRECTION_NEUTRAL,
        confidence=0.01,
        recommended_path=PATH_SIMULTANEOUS,
        reason=reason,
        features={},
    )


def _quote_stale_reason(market: ExitShadowMarket, config: ExitShadowConfig) -> str:
    for leg_name, quote in (("long", market.long_quote), ("short", market.short_quote)):
        if quote is None:
            return f"missing_{leg_name}_quote"
        if quote.mid <= 0.0:
            return f"invalid_{leg_name}_quote"
        age_ms = market.now_ms - int(quote.observed_at_ms or 0)
        if quote.observed_at_ms <= 0 or age_ms > config.max_quote_age_ms:
            return f"stale_{leg_name}_quote"
    return ""


def _l2_stale_reason(market: ExitShadowMarket, config: ExitShadowConfig) -> str:
    for leg_name, book in (("long", market.long_book), ("short", market.short_book)):
        if book is None:
            return f"missing_{leg_name}_l2"
        is_ready = getattr(book, "is_ready", None)
        if callable(is_ready):
            try:
                if not bool(is_ready(config.max_l2_age_ms, market.now_ms)):
                    return f"stale_{leg_name}_l2"
                continue
            except Exception:
                return f"invalid_{leg_name}_l2"
        observed_at_ms = int(getattr(book, "observed_at_ms", 0) or 0)
        if observed_at_ms <= 0 or market.now_ms - observed_at_ms > config.max_l2_age_ms:
            return f"stale_{leg_name}_l2"
    return ""


def _top_book_imbalance(market: ExitShadowMarket) -> float:
    bid_size = float(getattr(market.long_quote, "bid_size", 0.0) or 0.0) + float(
        getattr(market.short_quote, "bid_size", 0.0) or 0.0
    )
    ask_size = float(getattr(market.long_quote, "ask_size", 0.0) or 0.0) + float(
        getattr(market.short_quote, "ask_size", 0.0) or 0.0
    )
    denom = bid_size + ask_size
    if denom <= 0.0:
        return 0.0
    return (bid_size - ask_size) / denom


def _multi_level_l2_imbalance(market: ExitShadowMarket, depth_levels: int) -> float:
    bid_depth = _weighted_depth(market.long_book, "bids", depth_levels) + _weighted_depth(
        market.short_book, "bids", depth_levels
    )
    ask_depth = _weighted_depth(market.long_book, "asks", depth_levels) + _weighted_depth(
        market.short_book, "asks", depth_levels
    )
    denom = bid_depth + ask_depth
    if denom <= 0.0:
        return 0.0
    return (bid_depth - ask_depth) / denom


def _weighted_depth(book: LocalL2Book | None, side: str, depth_levels: int) -> float:
    levels = list(getattr(book, side, []) or [])[: max(depth_levels, 1)]
    total = 0.0
    for idx, level in enumerate(levels):
        weight = 1.0 / float(idx + 1)
        total += float(getattr(level, "quantity", 0.0) or 0.0) * weight
    return total


def _cross_venue_premium_bps(market: ExitShadowMarket) -> float:
    long_mid = _quote_mid(market.long_quote)
    short_mid = _quote_mid(market.short_quote)
    if long_mid <= 0.0 or short_mid <= 0.0:
        return 0.0
    return (short_mid - long_mid) / ((short_mid + long_mid) / 2.0) * 10000.0


def _path_net_bps(
    start: ExitShadowMarket,
    future: ExitShadowMarket,
    path: str,
) -> float:
    start_long_mid = _quote_mid(start.long_quote)
    start_short_mid = _quote_mid(start.short_quote)
    future_long_mid = _quote_mid(future.long_quote)
    future_short_mid = _quote_mid(future.short_quote)
    if path == PATH_SHORT_FIRST and start_long_mid > 0.0 and future_long_mid > 0.0:
        return (future_long_mid - start_long_mid) / start_long_mid * 10000.0
    if path == PATH_LONG_FIRST and start_short_mid > 0.0 and future_short_mid > 0.0:
        return (start_short_mid - future_short_mid) / start_short_mid * 10000.0
    return 0.0


def _quote_mid(quote: ExitShadowQuote | None) -> float:
    if quote is None:
        return 0.0
    return quote.mid


def _shadow_id(snapshot: ExitShadowSnapshot) -> str:
    position_id = str(getattr(snapshot.position, "position_id", "") or "")
    symbol = str(getattr(snapshot.position, "symbol", "") or "")
    return f"{position_id}:{symbol}:{snapshot.reason}:{snapshot.market.now_ms}"


def _position_fields(snapshot: ExitShadowSnapshot) -> dict[str, Any]:
    position = snapshot.position
    long_venue = getattr(position, "long_venue", "")
    short_venue = getattr(position, "short_venue", "")
    return {
        "position_id": str(getattr(position, "position_id", "") or ""),
        "symbol": str(getattr(position, "symbol", "") or ""),
        "reason": snapshot.reason,
        "long_venue": getattr(long_venue, "value", str(long_venue)),
        "short_venue": getattr(short_venue, "value", str(short_venue)),
        "matched_quantity": float(getattr(position, "matched_quantity", 0.0) or 0.0),
    }


def _market_snapshot(market: ExitShadowMarket, config: ExitShadowConfig) -> dict[str, Any]:
    return {
        "now_ms": int(market.now_ms or 0),
        "long_quote": _quote_snapshot(market.long_quote, market.now_ms),
        "short_quote": _quote_snapshot(market.short_quote, market.now_ms),
        "long_l2": _book_snapshot(market.long_book, market.now_ms, config),
        "short_l2": _book_snapshot(market.short_book, market.now_ms, config),
        "cross_venue_premium_bps": _cross_venue_premium_bps(market),
        "top_book_imbalance": (
            _top_book_imbalance(market) if not _quote_stale_reason(market, config) else 0.0
        ),
        "multi_level_l2_imbalance": (
            _multi_level_l2_imbalance(market, config.l2_depth_levels)
            if not _l2_stale_reason(market, config)
            else 0.0
        ),
    }


def _quote_snapshot(quote: ExitShadowQuote | None, now_ms: int) -> dict[str, Any]:
    if quote is None:
        return {
            "available": False,
            "venue": "",
            "symbol": "",
            "bid": 0.0,
            "ask": 0.0,
            "mid": 0.0,
            "spread_bps": 0.0,
            "bid_size": 0.0,
            "ask_size": 0.0,
            "observed_at_ms": 0,
            "age_ms": 0,
            "source": "",
        }
    observed_at_ms = int(quote.observed_at_ms or 0)
    return {
        "available": True,
        "venue": quote.venue,
        "symbol": quote.symbol,
        "bid": float(quote.bid or 0.0),
        "ask": float(quote.ask or 0.0),
        "mid": quote.mid,
        "spread_bps": quote.spread_bps,
        "bid_size": float(quote.bid_size or 0.0),
        "ask_size": float(quote.ask_size or 0.0),
        "observed_at_ms": observed_at_ms,
        "age_ms": max(int(now_ms or 0) - observed_at_ms, 0) if observed_at_ms > 0 else 0,
        "source": quote.source,
    }


def _book_snapshot(
    book: LocalL2Book | None,
    now_ms: int,
    config: ExitShadowConfig,
) -> dict[str, Any]:
    if book is None:
        return {
            "available": False,
            "ready": False,
            "venue": "",
            "symbol": "",
            "status": "",
            "source": "",
            "observed_at_ms": 0,
            "age_ms": 0,
            "depth_levels": 0,
            "bid_depth": 0.0,
            "ask_depth": 0.0,
            "top_bid": 0.0,
            "top_ask": 0.0,
            "top_bid_size": 0.0,
            "top_ask_size": 0.0,
        }
    observed_at_ms = int(getattr(book, "observed_at_ms", 0) or 0)
    bids = list(getattr(book, "bids", []) or [])
    asks = list(getattr(book, "asks", []) or [])
    status = getattr(book, "status", "")
    is_ready = getattr(book, "is_ready", None)
    ready = False
    if callable(is_ready):
        try:
            ready = bool(is_ready(config.max_l2_age_ms, int(now_ms or 0)))
        except Exception:
            ready = False
    else:
        ready = observed_at_ms > 0 and int(now_ms or 0) - observed_at_ms <= config.max_l2_age_ms
    return {
        "available": True,
        "ready": ready,
        "venue": str(getattr(book, "venue", "") or ""),
        "symbol": str(getattr(book, "symbol", "") or ""),
        "status": getattr(status, "value", str(status)),
        "source": str(getattr(book, "source", "") or "local_l2"),
        "observed_at_ms": observed_at_ms,
        "age_ms": max(int(now_ms or 0) - observed_at_ms, 0) if observed_at_ms > 0 else 0,
        "depth_levels": min(len(bids), len(asks), max(int(config.l2_depth_levels), 0)),
        "bid_depth": _weighted_depth(book, "bids", config.l2_depth_levels),
        "ask_depth": _weighted_depth(book, "asks", config.l2_depth_levels),
        "top_bid": float(getattr(bids[0], "price", 0.0) or 0.0) if bids else 0.0,
        "top_ask": float(getattr(asks[0], "price", 0.0) or 0.0) if asks else 0.0,
        "top_bid_size": float(getattr(bids[0], "quantity", 0.0) or 0.0) if bids else 0.0,
        "top_ask_size": float(getattr(asks[0], "quantity", 0.0) or 0.0) if asks else 0.0,
    }


def _market_data_quality(
    market: ExitShadowMarket,
    config: ExitShadowConfig,
    *,
    prefix: str = "",
) -> dict[str, Any]:
    data = {
        f"{prefix}long_quote_status": _quote_quality(
            market.long_quote,
            market.now_ms,
            config,
        ),
        f"{prefix}short_quote_status": _quote_quality(
            market.short_quote,
            market.now_ms,
            config,
        ),
        f"{prefix}long_l2_status": _book_quality(
            market.long_book,
            market.now_ms,
            config,
        ),
        f"{prefix}short_l2_status": _book_quality(
            market.short_book,
            market.now_ms,
            config,
        ),
    }
    return data


def _combined_market_data_quality(
    trigger_market: ExitShadowMarket,
    future_market: ExitShadowMarket,
    config: ExitShadowConfig,
) -> dict[str, Any]:
    quality = _market_data_quality(trigger_market, config)
    quality.update(_market_data_quality(trigger_market, config, prefix="trigger_"))
    quality.update(_market_data_quality(future_market, config, prefix="future_"))
    return quality


def _quote_quality(
    quote: ExitShadowQuote | None,
    now_ms: int,
    config: ExitShadowConfig,
) -> str:
    if quote is None:
        return "missing"
    if quote.mid <= 0.0:
        return "invalid"
    observed_at_ms = int(quote.observed_at_ms or 0)
    if observed_at_ms <= 0 or int(now_ms or 0) - observed_at_ms > config.max_quote_age_ms:
        return "stale"
    return "fresh"


def _book_quality(
    book: LocalL2Book | None,
    now_ms: int,
    config: ExitShadowConfig,
) -> str:
    if book is None:
        return "missing"
    is_ready = getattr(book, "is_ready", None)
    if callable(is_ready):
        try:
            return "fresh" if bool(is_ready(config.max_l2_age_ms, int(now_ms or 0))) else "stale"
        except Exception:
            return "invalid"
    observed_at_ms = int(getattr(book, "observed_at_ms", 0) or 0)
    if observed_at_ms <= 0 or int(now_ms or 0) - observed_at_ms > config.max_l2_age_ms:
        return "stale"
    return "fresh"


def _strategy_decision_event(
    shadow_id: str,
    snapshot: ExitShadowSnapshot,
    decision: ExitShadowDecision,
    config: ExitShadowConfig,
) -> dict[str, Any]:
    payload = {
        "shadow_id": shadow_id,
        "bot_id": decision.bot_id,
        "direction": decision.direction,
        "confidence": decision.confidence,
        "recommended_path": decision.recommended_path,
        "decision_reason": decision.reason,
        "features": dict(decision.features),
        "trigger_market": _market_snapshot(snapshot.market, config),
        "data_quality": _market_data_quality(snapshot.market, config),
        "ts_ms": snapshot.market.now_ms,
    }
    payload.update(_position_fields(snapshot))
    return {"kind": "exit_shadow.strategy_decision", "payload": payload}


def _path_markout_event(
    shadow_id: str,
    snapshot: ExitShadowSnapshot,
    future_market: ExitShadowMarket,
    *,
    path: str,
    horizon_ms: int,
    config: ExitShadowConfig,
) -> dict[str, Any]:
    net_bps = _path_net_bps(snapshot.market, future_market, path)
    max_adverse_bps = max(-net_bps, 0.0)
    take_profit_hits = [target for target in config.take_profit_bps if net_bps >= target]
    payload = {
        "shadow_id": shadow_id,
        "path": path,
        "horizon_ms": int(horizon_ms),
        "net_bps": net_bps,
        "incremental_net_bps": net_bps,
        "max_adverse_bps": max_adverse_bps,
        "take_profit_hit_bps": max(take_profit_hits) if take_profit_hits else 0.0,
        "stop_loss_hit": max_adverse_bps >= config.adverse_stop_bps,
        "trigger_market": _market_snapshot(snapshot.market, config),
        "future_market": _market_snapshot(future_market, config),
        "data_quality": _combined_market_data_quality(
            snapshot.market,
            future_market,
            config,
        ),
        "ts_ms": future_market.now_ms,
    }
    payload.update(_position_fields(snapshot))
    return {"kind": "exit_shadow.path_markout", "payload": payload}


def _strategy_summary_event(
    shadow_id: str,
    snapshot: ExitShadowSnapshot,
    future_market: ExitShadowMarket,
    decision: ExitShadowDecision,
    *,
    horizon_ms: int,
    path_net_bps: dict[str, float],
    path_adverse_bps: dict[str, float],
    config: ExitShadowConfig,
) -> dict[str, Any]:
    recommended_net_bps = float(path_net_bps.get(decision.recommended_path, 0.0) or 0.0)
    baseline_net_bps = float(path_net_bps.get(PATH_SIMULTANEOUS, 0.0) or 0.0)
    incremental_net_bps = recommended_net_bps - baseline_net_bps
    excluded = decision.direction == DIRECTION_NEUTRAL
    direction_correct = False
    if decision.direction == DIRECTION_BULLISH:
        direction_correct = path_net_bps.get(PATH_SHORT_FIRST, 0.0) > path_net_bps.get(
            PATH_LONG_FIRST, 0.0
        )
    elif decision.direction == DIRECTION_BEARISH:
        direction_correct = path_net_bps.get(PATH_LONG_FIRST, 0.0) > path_net_bps.get(
            PATH_SHORT_FIRST, 0.0
        )
    payload = {
        "shadow_id": shadow_id,
        "bot_id": decision.bot_id,
        "direction": decision.direction,
        "confidence": decision.confidence,
        "recommended_path": decision.recommended_path,
        "baseline_path": PATH_SIMULTANEOUS,
        "horizon_ms": int(horizon_ms),
        "direction_correct": direction_correct,
        "recommended_net_bps": recommended_net_bps,
        "baseline_net_bps": baseline_net_bps,
        "incremental_net_bps": incremental_net_bps,
        "max_adverse_bps": float(
            path_adverse_bps.get(decision.recommended_path, 0.0) or 0.0
        ),
        "excluded": excluded,
        "exclude_reason": decision.reason if excluded else "",
        "trigger_market": _market_snapshot(snapshot.market, config),
        "future_market": _market_snapshot(future_market, config),
        "data_quality": _combined_market_data_quality(
            snapshot.market,
            future_market,
            config,
        ),
        "ts_ms": snapshot.market.now_ms + int(horizon_ms),
    }
    payload.update(_position_fields(snapshot))
    return {"kind": "exit_shadow.strategy_summary", "payload": payload}
