"""Small paper-only execution ledger for the spread sidecar.

The production spread process needs a simulator, not a research evidence
platform.  This module deliberately owns only four things: candidate
registration, delayed public-quote fills, mark-to-market PnL, and a compact
restart checkpoint.  Research manifests, signed fee documents, cohort epochs,
and rollback witnesses remain outside the production import graph.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import math
import os
from pathlib import Path
import tempfile
import time
from typing import Iterable

from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.models import SpreadReversionCandidate


PAPER_CHECKPOINT_SCHEMA_VERSION = 1


class SpreadPaperJournal:
    """Minimal JSONL writer for paper events.

    The atomic checkpoint owns restart state; this file is only an append-only
    human-readable history.  There are no epochs, signatures, batch chains, or
    rollback anchors in the production paper process.
    """

    def __init__(self, path: str | Path, *, max_bytes: int = 0) -> None:
        self.path = Path(path)
        self.max_bytes = max(int(max_bytes), 0)
        self._file = None

    def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.path, "a", encoding="utf-8")

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def append_many(
        self,
        events: Iterable[tuple[str, dict[str, object]]],
        *,
        ts_ms: int | None = None,
    ) -> None:
        if self._file is None:
            raise RuntimeError("paper journal not open")
        observed_at_ms = int(ts_ms if ts_ms is not None else time.time() * 1000)
        lines = [
            json.dumps(
                {"ts_ms": observed_at_ms, "kind": kind, "payload": payload},
                ensure_ascii=False,
                separators=(",", ":"),
            )
            + "\n"
            for kind, payload in events
        ]
        if not lines:
            return
        encoded_size = sum(len(line.encode("utf-8")) for line in lines)
        self._file.seek(0, os.SEEK_END)
        if self.max_bytes > 0 and self._file.tell() + encoded_size > self.max_bytes:
            self._file.close()
            archive = self.path.with_name(f"{self.path.name}.1")
            archive.unlink(missing_ok=True)
            if self.path.exists():
                self.path.replace(archive)
            self._file = open(self.path, "a", encoding="utf-8")
        self._file.writelines(lines)
        self._file.flush()


@dataclass
class SpreadPaperConfig:
    enabled: bool = False
    finalist_limit: int = 10
    min_decision_latency_ms: int = 250
    markout_secs: list[int] = field(default_factory=lambda: [60, 300, 900, 1800])
    terminal_secs: int = 1800
    exit_z: float = 0.5
    stop_z: float = 3.5
    max_hold_ms: int = 1_800_000
    taker_fee_bps_by_venue: dict[str, float] = field(default_factory=dict)
    slippage_buffer_bps: float = 0.0
    latency_buffer_bps: float = 0.0
    require_l2_vwap: bool = True
    excluded_symbols: list[str] = field(default_factory=list)
    allowed_opportunity_labels: list[str] = field(
        default_factory=lambda: ["spread_reversion"]
    )
    episode_cooldown_ms: int = 1_800_000
    quote_ttl_ms: int = 1_000
    quote_skew_ms: int = 250


@dataclass
class _PaperPosition:
    paper_id: str
    candidate_id: str
    symbol: str
    long_venue: str
    short_venue: str
    registered_at_ms: int
    eligible_fill_at_ms: int
    terminal_at_ms: int
    entry_notional_quote: float
    entry_z_score: float
    equilibrium_spread_bps: float
    rolling_std_bps: float
    canonical_venue_a: str
    canonical_venue_b: str
    state: str = "pending"
    opened_at_ms: int = 0
    base_quantity: float = 0.0
    entry_long_price: float = 0.0
    entry_short_price: float = 0.0
    entry_fee_quote: float = 0.0
    emitted_markout_secs: list[int] = field(default_factory=list)


def _quote_key(venue: str, symbol: str) -> str:
    return f"{str(venue).lower()}:{str(symbol).upper()}"


def _quote_for(
    quotes: dict[str, QuoteSnapshot], venue: str, symbol: str
) -> QuoteSnapshot | None:
    return quotes.get(_quote_key(venue, symbol))


def _quotes_fresh(
    long_quote: QuoteSnapshot,
    short_quote: QuoteSnapshot,
    *,
    now_ms: int,
    ttl_ms: int,
    skew_ms: int,
) -> bool:
    timestamps = (
        int(long_quote.observed_at_ms or 0),
        int(short_quote.observed_at_ms or 0),
    )
    return bool(
        all(0 < timestamp <= now_ms for timestamp in timestamps)
        and now_ms - min(timestamps) <= max(int(ttl_ms), 0)
        and abs(timestamps[0] - timestamps[1]) <= max(int(skew_ms), 0)
    )


def _vwap(
    levels: Iterable[tuple[float, float]],
    quantity: float,
) -> float | None:
    remaining = float(quantity)
    value = 0.0
    for raw_price, raw_size in levels:
        price = float(raw_price or 0.0)
        size = float(raw_size or 0.0)
        if not math.isfinite(price) or not math.isfinite(size) or price <= 0.0 or size <= 0.0:
            continue
        taken = min(remaining, size)
        value += taken * price
        remaining -= taken
        if remaining <= 1e-12:
            return value / quantity
    return None


def _raw_execution_price(
    quote: QuoteSnapshot,
    *,
    side: str,
    quantity: float,
    require_l2: bool,
) -> float | None:
    levels = quote.ask_depth if side == "buy" else quote.bid_depth
    if levels:
        return _vwap(levels, quantity)
    if require_l2:
        return None
    price = float(quote.ask if side == "buy" else quote.bid)
    size = float(quote.ask_size if side == "buy" else quote.bid_size)
    if not math.isfinite(price) or price <= 0.0:
        return None
    if size > 0.0 and size + 1e-12 < quantity:
        return None
    return price


def _adjusted_price(price: float, *, side: str, buffer_bps: float) -> float:
    direction = 1.0 if side == "buy" else -1.0
    return price * (1.0 + direction * max(float(buffer_bps), 0.0) / 10_000.0)


def _fee_bps(config: SpreadPaperConfig, venue: str) -> float:
    return max(float(config.taker_fee_bps_by_venue.get(str(venue).lower(), 0.0)), 0.0)


def _position_payload(position: _PaperPosition) -> dict[str, object]:
    return asdict(position)


class SpreadPaperTracker:
    """One-process spread paper ledger with no exchange/private dependencies."""

    def __init__(self, config: SpreadPaperConfig) -> None:
        self.config = config
        self._positions: dict[str, _PaperPosition] = {}
        self._episode_started_at_ms: dict[str, int] = {}
        self._replay_valid = True

    @property
    def enabled(self) -> bool:
        return bool(
            self._replay_valid
            and self.config.enabled is True
            and int(self.config.finalist_limit) > 0
            and int(self.config.min_decision_latency_ms) > 0
            and int(self.config.terminal_secs) > 0
        )

    @property
    def tracked_count(self) -> int:
        return len(self._positions)

    def invalidate_replay(self) -> None:
        self._positions.clear()
        self._episode_started_at_ms.clear()
        self._replay_valid = False

    def register_many(
        self,
        candidate: SpreadReversionCandidate,
        quotes: dict[str, QuoteSnapshot],
        *,
        finalist_rank: int,
        decision_at_ms: int,
        rejection_counts: dict[str, int] | None = None,
    ) -> list[dict[str, object]]:
        def reject(reason: str) -> list[dict[str, object]]:
            if rejection_counts is not None:
                rejection_counts[reason] = int(rejection_counts.get(reason, 0)) + 1
            return []

        if not self.enabled:
            return reject("paper_tracker_disabled")
        if finalist_rank >= int(self.config.finalist_limit):
            return reject("paper_finalist_limit")
        if candidate.signal_status != "entry_ready" or candidate.economics_complete is not True:
            return reject("paper_candidate_not_entry_ready")
        if str(candidate.contract_normalization_status).lower() != "complete":
            return reject("paper_contract_normalization_incomplete")
        if candidate.symbol.upper() in {
            symbol.upper() for symbol in self.config.excluded_symbols
        }:
            return reject("paper_symbol_excluded")
        if (
            self.config.allowed_opportunity_labels
            and candidate.opportunity_label not in self.config.allowed_opportunity_labels
        ):
            return reject("paper_opportunity_label_disallowed")
        long_quote = _quote_for(quotes, candidate.long_venue, candidate.symbol)
        short_quote = _quote_for(quotes, candidate.short_venue, candidate.symbol)
        if long_quote is None or short_quote is None:
            return reject("paper_quote_missing")
        if not _quotes_fresh(
            long_quote,
            short_quote,
            now_ms=decision_at_ms,
            ttl_ms=self.config.quote_ttl_ms,
            skew_ms=self.config.quote_skew_ms,
        ):
            return reject("paper_quote_stale_or_skewed")
        episode_key = (
            f"{candidate.symbol.upper()}:{candidate.long_venue.lower()}:"
            f"{candidate.short_venue.lower()}"
        )
        last_episode_ms = int(self._episode_started_at_ms.get(episode_key, 0))
        if (
            last_episode_ms > 0
            and decision_at_ms - last_episode_ms < max(int(self.config.episode_cooldown_ms), 0)
        ):
            return reject("paper_episode_cooldown")
        if float(candidate.entry_notional_quote or 0.0) <= 0.0:
            return reject("paper_notional_invalid")
        paper_id = f"{candidate.candidate_id}:{decision_at_ms}"
        position = _PaperPosition(
            paper_id=paper_id,
            candidate_id=candidate.candidate_id,
            symbol=candidate.symbol.upper(),
            long_venue=candidate.long_venue.lower(),
            short_venue=candidate.short_venue.lower(),
            registered_at_ms=decision_at_ms,
            eligible_fill_at_ms=decision_at_ms + int(self.config.min_decision_latency_ms),
            terminal_at_ms=decision_at_ms + int(self.config.terminal_secs) * 1_000,
            entry_notional_quote=float(candidate.entry_notional_quote),
            entry_z_score=float(candidate.z_score),
            equilibrium_spread_bps=float(candidate.equilibrium_spread_bps),
            rolling_std_bps=max(float(candidate.rolling_std_bps), 0.0),
            canonical_venue_a=(candidate.canonical_venue_a or candidate.long_venue).lower(),
            canonical_venue_b=(candidate.canonical_venue_b or candidate.short_venue).lower(),
        )
        self._positions[paper_id] = position
        self._episode_started_at_ms[episode_key] = decision_at_ms
        return [
            {
                "kind": "opportunity.paper_registered",
                "payload": _position_payload(position),
            }
        ]

    def record_observed_public_funding_settlements(
        self,
        now_ms: int,
        quotes: dict[str, QuoteSnapshot],
    ) -> list[dict[str, object]]:
        # Funding carry is already part of signal economics.  This lightweight
        # public simulator does not pretend that public rates are an account
        # funding ledger.
        return []

    def evaluate_due(
        self,
        now_ms: int,
        quotes: dict[str, QuoteSnapshot],
    ) -> list[dict[str, object]]:
        events: list[dict[str, object]] = []
        for paper_id, position in list(self._positions.items()):
            if position.state == "pending":
                if now_ms >= position.terminal_at_ms:
                    events.append(
                        {
                            "kind": "opportunity.paper_expired",
                            "payload": {**_position_payload(position), "reason": "entry_not_filled"},
                        }
                    )
                    self._positions.pop(paper_id, None)
                    continue
                fill = self._try_fill(position, quotes, now_ms)
                if fill is not None:
                    self._positions[paper_id] = fill
                    position = fill
                    events.append(
                        {
                            "kind": "opportunity.paper_filled",
                            "payload": _position_payload(position),
                        }
                    )
                else:
                    continue

            close_reason = self._close_reason(position, quotes, now_ms)
            due_markouts = [
                seconds
                for seconds in sorted({int(value) for value in self.config.markout_secs if int(value) > 0})
                if seconds not in position.emitted_markout_secs
                and now_ms >= position.opened_at_ms + seconds * 1_000
            ]
            for seconds in due_markouts:
                mark = self._mark(position, quotes, now_ms)
                if mark is None:
                    continue
                position.emitted_markout_secs.append(seconds)
                events.append(
                    {
                        "kind": "opportunity.paper_markout",
                        "payload": {**mark, "horizon_secs": seconds},
                    }
                )
            if close_reason:
                mark = self._mark(position, quotes, now_ms)
                if mark is not None:
                    events.append(
                        {
                            "kind": "opportunity.paper_closed",
                            "payload": {**mark, "reason": close_reason},
                        }
                    )
                    self._positions.pop(paper_id, None)
        return events

    def _try_fill(
        self,
        position: _PaperPosition,
        quotes: dict[str, QuoteSnapshot],
        now_ms: int,
    ) -> _PaperPosition | None:
        if now_ms < position.eligible_fill_at_ms:
            return None
        long_quote = _quote_for(quotes, position.long_venue, position.symbol)
        short_quote = _quote_for(quotes, position.short_venue, position.symbol)
        if long_quote is None or short_quote is None or not _quotes_fresh(
            long_quote,
            short_quote,
            now_ms=now_ms,
            ttl_ms=self.config.quote_ttl_ms,
            skew_ms=self.config.quote_skew_ms,
        ):
            return None
        reference = max(float(long_quote.ask), float(short_quote.bid))
        if reference <= 0.0:
            return None
        quantity = position.entry_notional_quote / reference
        long_raw = _raw_execution_price(
            long_quote,
            side="buy",
            quantity=quantity,
            require_l2=self.config.require_l2_vwap,
        )
        short_raw = _raw_execution_price(
            short_quote,
            side="sell",
            quantity=quantity,
            require_l2=self.config.require_l2_vwap,
        )
        if long_raw is None or short_raw is None:
            return None
        execution_buffer = max(
            float(self.config.slippage_buffer_bps), 0.0
        ) + max(float(self.config.latency_buffer_bps), 0.0)
        long_price = _adjusted_price(long_raw, side="buy", buffer_bps=execution_buffer)
        short_price = _adjusted_price(short_raw, side="sell", buffer_bps=execution_buffer)
        entry_fee = quantity * (
            long_price * _fee_bps(self.config, position.long_venue)
            + short_price * _fee_bps(self.config, position.short_venue)
        ) / 10_000.0
        position.state = "open"
        position.opened_at_ms = now_ms
        position.terminal_at_ms = now_ms + int(self.config.terminal_secs) * 1_000
        position.base_quantity = quantity
        position.entry_long_price = long_price
        position.entry_short_price = short_price
        position.entry_fee_quote = entry_fee
        return position

    def _close_reason(
        self,
        position: _PaperPosition,
        quotes: dict[str, QuoteSnapshot],
        now_ms: int,
    ) -> str:
        if now_ms >= position.terminal_at_ms:
            return "terminal"
        if int(self.config.max_hold_ms) > 0 and now_ms - position.opened_at_ms >= int(
            self.config.max_hold_ms
        ):
            return "max_hold"
        z_score = self._current_z(position, quotes)
        if z_score is None:
            return ""
        if abs(z_score) <= max(float(self.config.exit_z), 0.0):
            return "mean_reversion"
        if abs(z_score) >= max(float(self.config.stop_z), 0.0):
            return "stop_z"
        return ""

    def _current_z(
        self,
        position: _PaperPosition,
        quotes: dict[str, QuoteSnapshot],
    ) -> float | None:
        quote_a = _quote_for(quotes, position.canonical_venue_a, position.symbol)
        quote_b = _quote_for(quotes, position.canonical_venue_b, position.symbol)
        if quote_a is None or quote_b is None or position.rolling_std_bps <= 0.0:
            return None
        mid_a = (float(quote_a.bid) + float(quote_a.ask)) / 2.0
        mid_b = (float(quote_b.bid) + float(quote_b.ask)) / 2.0
        denominator = (mid_a + mid_b) / 2.0
        if denominator <= 0.0:
            return None
        signed_spread = (mid_a - mid_b) / denominator * 10_000.0
        return (signed_spread - position.equilibrium_spread_bps) / position.rolling_std_bps

    def _mark(
        self,
        position: _PaperPosition,
        quotes: dict[str, QuoteSnapshot],
        now_ms: int,
    ) -> dict[str, object] | None:
        long_quote = _quote_for(quotes, position.long_venue, position.symbol)
        short_quote = _quote_for(quotes, position.short_venue, position.symbol)
        if long_quote is None or short_quote is None or not _quotes_fresh(
            long_quote,
            short_quote,
            now_ms=now_ms,
            ttl_ms=self.config.quote_ttl_ms,
            skew_ms=self.config.quote_skew_ms,
        ):
            return None
        quantity = position.base_quantity
        long_raw = _raw_execution_price(
            long_quote,
            side="sell",
            quantity=quantity,
            require_l2=self.config.require_l2_vwap,
        )
        short_raw = _raw_execution_price(
            short_quote,
            side="buy",
            quantity=quantity,
            require_l2=self.config.require_l2_vwap,
        )
        if long_raw is None or short_raw is None:
            return None
        execution_buffer = max(
            float(self.config.slippage_buffer_bps), 0.0
        ) + max(float(self.config.latency_buffer_bps), 0.0)
        exit_long = _adjusted_price(long_raw, side="sell", buffer_bps=execution_buffer)
        exit_short = _adjusted_price(short_raw, side="buy", buffer_bps=execution_buffer)
        gross_pnl = quantity * (
            exit_long - position.entry_long_price
            + position.entry_short_price - exit_short
        )
        exit_fee = quantity * (
            exit_long * _fee_bps(self.config, position.long_venue)
            + exit_short * _fee_bps(self.config, position.short_venue)
        ) / 10_000.0
        net_pnl = gross_pnl - position.entry_fee_quote - exit_fee
        gross_exposure = quantity * (
            position.entry_long_price + position.entry_short_price
        )
        return {
            "paper_id": position.paper_id,
            "candidate_id": position.candidate_id,
            "symbol": position.symbol,
            "long_venue": position.long_venue,
            "short_venue": position.short_venue,
            "observed_at_ms": now_ms,
            "base_quantity": quantity,
            "exit_long_price": exit_long,
            "exit_short_price": exit_short,
            "gross_pnl_quote": gross_pnl,
            "entry_fee_quote": position.entry_fee_quote,
            "exit_fee_quote": exit_fee,
            "net_pnl_quote": net_pnl,
            "net_pnl_bps": (
                net_pnl / gross_exposure * 10_000.0 if gross_exposure > 0.0 else 0.0
            ),
        }

    def checkpoint(self) -> dict[str, object]:
        return {
            "schema_version": PAPER_CHECKPOINT_SCHEMA_VERSION,
            "positions": [
                _position_payload(position) for position in self._positions.values()
            ],
            "episode_started_at_ms": dict(self._episode_started_at_ms),
        }

    def restore_checkpoint(self, payload: object) -> bool:
        if not isinstance(payload, dict):
            self.invalidate_replay()
            return False
        if payload.get("schema_version") != PAPER_CHECKPOINT_SCHEMA_VERSION:
            self.invalidate_replay()
            return False
        raw_positions = payload.get("positions")
        raw_episodes = payload.get("episode_started_at_ms")
        if not isinstance(raw_positions, list) or not isinstance(raw_episodes, dict):
            self.invalidate_replay()
            return False
        try:
            positions = [_PaperPosition(**row) for row in raw_positions if isinstance(row, dict)]
            if len(positions) != len(raw_positions):
                raise ValueError("invalid paper checkpoint row")
            episodes = {str(key): int(value) for key, value in raw_episodes.items()}
            if any(position.state not in {"pending", "open"} for position in positions):
                raise ValueError("invalid paper position state")
        except (TypeError, ValueError, OverflowError):
            self.invalidate_replay()
            return False
        self._positions = {position.paper_id: position for position in positions}
        self._episode_started_at_ms = episodes
        self._replay_valid = True
        return True


def load_paper_checkpoint(path: str | Path) -> dict[str, object] | None:
    checkpoint_path = Path(path)
    if not checkpoint_path.exists():
        return None
    try:
        payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def publish_paper_checkpoint(path: str | Path, payload: dict[str, object]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(dir=str(target.parent), prefix=f".{target.name}.")
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        with open(temporary, "w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, sort_keys=True)
            file.flush()
            os.fsync(file.fileno())
        temporary.replace(target)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
