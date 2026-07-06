"""Spread-reversion sidecar service."""

from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from typing import Optional

from lightfee.config.schema import AppConfig
from lightfee.core.domain import Venue
from lightfee.sidecar.publisher import load_snapshot
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.persistence.journal import Journal
from lightfee.spread.models import SpreadSnapshot
from lightfee.spread.paper import SpreadPaperConfig, SpreadPaperTracker
from lightfee.spread.publisher import publish_spread_snapshot
from lightfee.spread.reversion import (
    SpreadReversionConfig,
    SpreadStatsTracker,
    build_spread_reversion_candidates,
)
from lightfee.venues.specs import get_spec


_PAPER_LAST_GOOD_QUOTE_SOURCE = "spread_paper_last_good_quote"
_PAPER_QUOTE_REPAIR_SOURCE = "spread_paper_quote_repair"


def _paper_quote_key(venue: str, symbol: str) -> str:
    return f"{str(venue).lower()}:{str(symbol).upper()}"


def _venue_maker_fee_bps(venue_config: object) -> float:
    maker_fee = getattr(venue_config, "maker_fee_bps", None)
    if maker_fee is None:
        maker_fee = getattr(venue_config, "taker_fee_bps", 0.0)
    return float(maker_fee or 0.0)


class SpreadSidecarService:
    """Public-data signal process for spread reversion.

    It has no private credentials and no order submission path.
    """

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.snapshot_path = config.runtime.spread_sidecar_snapshot_path
        self.sidecar_snapshot_path = config.runtime.sidecar_snapshot_path
        self.refresh_timeout_s = float(
            getattr(config.runtime, "spread_sidecar_fetch_timeout_s", 10.0) or 10.0
        )
        self.source_mode = str(
            getattr(config.runtime, "spread_sidecar_source_mode", "sidecar_snapshot")
            or "sidecar_snapshot"
        ).lower()
        self.direct_fetch_enabled = bool(
            getattr(config.runtime, "spread_sidecar_direct_fetch_enabled", False)
        )
        self.signal_config = SpreadReversionConfig.from_app_config(config)
        self.stats = SpreadStatsTracker()
        self._exchange_sources: dict[str, object] = {}
        self._paper_journal: Journal | None = None
        self._paper_tracker = SpreadPaperTracker(self._paper_config(config))
        self._paper_last_good_quotes: dict[str, QuoteSnapshot] = {}
        self._paper_quote_repair_enabled = bool(
            getattr(
                config.strategy,
                "spread_paper_quote_repair_enabled",
                True,
            )
        )
        self._paper_quote_repair_timeout_s = float(
            getattr(
                config.strategy,
                "spread_paper_quote_repair_timeout_s",
                3.0,
            )
            or 0.0
        )
        if self._paper_tracker.enabled:
            persistence = config.persistence
            self._paper_journal = Journal(
                persistence.spread_paper_event_log_path,
                max_bytes=persistence.event_log_compaction_max_bytes,
                archive_count=persistence.event_log_archive_count,
                retention_hours=persistence.event_log_retention_hours,
            )
            self._paper_journal.open()
            self._paper_tracker.restore_from_records(self._paper_journal.read_all())

        needs_exchange_sources = (
            self.source_mode == "direct_market" and self.direct_fetch_enabled
        ) or (self._paper_tracker.enabled and self._paper_quote_repair_enabled)
        if not needs_exchange_sources:
            return

        from lightfee.sidecar.sources.exchange import ExchangeSource
        from lightfee.venues.transport import EndpointRateLimiter

        for vc in config.venues:
            venue_name = str(vc.venue or "").lower()
            if not venue_name:
                continue
            try:
                venue = Venue.from_str(venue_name)
                spec = get_spec(venue)
            except Exception:
                continue
            self._exchange_sources[venue_name] = ExchangeSource(
                spec,
                rate_limiter=EndpointRateLimiter(1000, 8000, 50),
            )

    async def close(self) -> None:
        if self._paper_journal is not None:
            self._paper_journal.close()
            self._paper_journal = None
        for source in self._exchange_sources.values():
            close = getattr(source, "close", None)
            if close is not None:
                result = close()
                if hasattr(result, "__await__"):
                    await result

    async def refresh_once(self, *, now_ms: int | None = None) -> SpreadSnapshot:
        observed_ms = int(now_ms if now_ms is not None else time.time() * 1000)
        quotes, degraded_venues, source_mode = await self._fetch_quotes(observed_ms)
        candidates = build_spread_reversion_candidates(
            quotes,
            list(self.config.symbols),
            tracker=self.stats,
            config=self.signal_config,
            now_ms=observed_ms,
        )
        snapshot = SpreadSnapshot(
            published_at_ms=observed_ms,
            market_observed_at_ms=observed_ms,
            snapshot_path=str(self.snapshot_path),
            source_mode=source_mode,
            degraded_venues=sorted(degraded_venues),
            candidates=candidates,
        )
        publish_spread_snapshot(snapshot, self.snapshot_path)
        await self._refresh_paper(candidates, quotes, observed_ms)
        return snapshot

    async def _refresh_paper(
        self,
        candidates: list,
        quotes: dict[str, QuoteSnapshot],
        observed_ms: int,
    ) -> None:
        if self._paper_journal is None or not self._paper_tracker.enabled:
            return
        for rank, candidate in enumerate(candidates):
            if rank >= self._paper_tracker.config.finalist_limit:
                break
            for registered_event in self._paper_tracker.register_many(
                candidate,
                quotes,
                finalist_rank=rank,
            ):
                self._paper_journal.append(
                    str(registered_event["kind"]),
                    dict(registered_event["payload"]),
                    ts_ms=observed_ms,
                )
        if quotes:
            self._remember_paper_quotes(quotes)
        paper_quotes = await self._repair_paper_quotes_if_needed(
            dict(quotes),
            observed_ms,
        )
        if paper_quotes:
            self._remember_paper_quotes(paper_quotes)
        paper_quotes = self._paper_quotes_for_evaluation(paper_quotes, observed_ms)
        for event in self._paper_tracker.evaluate_due(observed_ms, paper_quotes):
            self._paper_journal.append(
                str(event["kind"]),
                dict(event["payload"]),
                ts_ms=observed_ms,
            )

    def _paper_quotes_for_evaluation(
        self,
        quotes: dict[str, QuoteSnapshot],
        observed_ms: int,
    ) -> dict[str, QuoteSnapshot]:
        max_age_ms = int(
            getattr(
                self.config.strategy,
                "spread_paper_last_good_quote_max_age_ms",
                60_000,
            )
            or 0
        )
        if max_age_ms <= 0 or not self._paper_last_good_quotes:
            return dict(quotes)
        paper_quotes: dict[str, QuoteSnapshot] = {}
        for key, quote in self._paper_last_good_quotes.items():
            quote_observed_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
            if quote_observed_ms <= 0:
                continue
            if observed_ms - quote_observed_ms > max_age_ms:
                continue
            paper_quotes[str(key)] = replace(
                quote,
                source=_PAPER_LAST_GOOD_QUOTE_SOURCE,
            )
        paper_quotes.update(quotes)
        return paper_quotes

    async def _repair_paper_quotes_if_needed(
        self,
        quotes: dict[str, QuoteSnapshot],
        observed_ms: int,
    ) -> dict[str, QuoteSnapshot]:
        if not self._paper_quote_repair_enabled:
            return quotes
        missing = self._paper_tracker.missing_evaluation_quote_keys(observed_ms, quotes)
        if not missing:
            return quotes
        repairs = await self._fetch_paper_quote_repairs(missing, observed_ms)
        if not repairs:
            return quotes
        repaired_quotes = dict(quotes)
        repaired_quotes.update(repairs)
        return repaired_quotes

    async def _fetch_paper_quote_repairs(
        self,
        requests: set[tuple[str, str]],
        observed_ms: int,
    ) -> dict[str, QuoteSnapshot]:
        by_venue: dict[str, set[str]] = {}
        requested_keys: set[str] = set()
        for venue, symbol in requests:
            venue_name = str(venue).lower()
            symbol_name = str(symbol).upper()
            if not venue_name or not symbol_name:
                continue
            by_venue.setdefault(venue_name, set()).add(symbol_name)
            requested_keys.add(_paper_quote_key(venue_name, symbol_name))
        if not by_venue:
            return {}

        timeout_s = self._paper_quote_repair_timeout_s
        if timeout_s <= 0.0:
            timeout_s = self.refresh_timeout_s

        async def _fetch_venue(
            venue_name: str,
            symbols: set[str],
        ) -> dict[str, QuoteSnapshot]:
            source = self._exchange_sources.get(venue_name)
            if source is None:
                return {}
            try:
                result = await asyncio.wait_for(
                    source.fetch_all(sorted(symbols)),
                    timeout=timeout_s,
                )
            except Exception:
                return {}
            repaired: dict[str, QuoteSnapshot] = {}
            for raw_key, quote in (result or {}).items():
                quote_venue = str(getattr(quote, "venue", "") or venue_name).lower()
                quote_symbol = str(getattr(quote, "symbol", "") or "").upper()
                key = _paper_quote_key(quote_venue, quote_symbol)
                if key not in requested_keys and str(raw_key) in requested_keys:
                    key = str(raw_key)
                if key not in requested_keys:
                    continue
                bid = float(getattr(quote, "bid", 0.0) or 0.0)
                ask = float(getattr(quote, "ask", 0.0) or 0.0)
                if bid <= 0.0 or ask <= 0.0:
                    continue
                observed_at_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
                repaired[key] = replace(
                    quote,
                    venue=key.split(":", 1)[0],
                    symbol=key.split(":", 1)[1],
                    observed_at_ms=observed_at_ms if observed_at_ms > 0 else observed_ms,
                    source=_PAPER_QUOTE_REPAIR_SOURCE,
                )
            return repaired

        results = await asyncio.gather(
            *[_fetch_venue(venue, symbols) for venue, symbols in by_venue.items()],
            return_exceptions=False,
        )
        repairs: dict[str, QuoteSnapshot] = {}
        for result in results:
            repairs.update(result)
        return repairs

    def _remember_paper_quotes(self, quotes: dict[str, QuoteSnapshot]) -> None:
        for key, quote in quotes.items():
            bid = float(getattr(quote, "bid", 0.0) or 0.0)
            ask = float(getattr(quote, "ask", 0.0) or 0.0)
            observed_at_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
            if bid <= 0.0 or ask <= 0.0 or observed_at_ms <= 0:
                continue
            self._paper_last_good_quotes[str(key)] = quote

    async def _fetch_quotes(
        self,
        observed_ms: int,
    ) -> tuple[dict[str, QuoteSnapshot], set[str], str]:
        if self.source_mode == "direct_market" and self.direct_fetch_enabled:
            quotes, degraded_venues = await self._fetch_quotes_direct(observed_ms)
            return quotes, degraded_venues, "direct_market_fallback"

        snapshot = load_snapshot(self.sidecar_snapshot_path)
        configured_venues = {
            str(getattr(vc, "venue", "") or "").lower()
            for vc in self.config.venues
            if str(getattr(vc, "venue", "") or "").strip()
        }
        if snapshot is None:
            return {}, configured_venues, "sidecar_snapshot_unavailable"

        max_age_ms = int(
            getattr(self.config.runtime, "sidecar_snapshot_max_age_ms", 10000) or 10000
        )
        published_at_ms = int(getattr(snapshot, "published_at_ms", 0) or 0)
        if published_at_ms <= 0 or observed_ms - published_at_ms > max_age_ms:
            return {}, configured_venues, "sidecar_snapshot_stale"

        quotes: dict[str, QuoteSnapshot] = {}
        for key, quote in (getattr(snapshot, "quotes", {}) or {}).items():
            if int(getattr(quote, "observed_at_ms", 0) or 0) <= 0:
                quote.observed_at_ms = observed_ms
            quotes[str(key)] = quote
        degraded_venues = {
            str(venue).lower()
            for venue in (getattr(snapshot, "degraded_venues", []) or [])
            if str(venue)
        }
        return quotes, degraded_venues, "sidecar_snapshot"

    def _paper_config(self, config: AppConfig) -> SpreadPaperConfig:
        strategy = config.strategy
        slippage_bps = float(
            getattr(strategy, "spread_paper_slippage_buffer_bps", 0.0) or 0.0
        )
        if slippage_bps <= 0.0:
            slippage_bps = float(getattr(strategy, "spread_slippage_reserve_bps", 0.0) or 0.0)
        return SpreadPaperConfig(
            enabled=bool(getattr(strategy, "spread_paper_enabled", False)),
            finalist_limit=int(getattr(strategy, "spread_paper_finalist_limit", 0) or 0),
            markout_secs=list(getattr(strategy, "spread_paper_markout_secs", []) or []),
            terminal_secs=int(getattr(strategy, "spread_paper_terminal_secs", 0) or 0),
            active_exit_enabled=True,
            exit_z=float(getattr(strategy, "spread_exit_z", 0.5) or 0.0),
            stop_z=float(getattr(strategy, "spread_stop_z", 3.5) or 0.0),
            max_hold_ms=int(getattr(strategy, "spread_max_hold_ms", 0) or 0),
            taker_fee_bps_by_venue={
                str(getattr(venue, "venue", "") or "").lower(): float(
                    getattr(venue, "taker_fee_bps", 0.0) or 0.0
                )
                for venue in config.venues
                if str(getattr(venue, "venue", "") or "").strip()
            },
            maker_fee_bps_by_venue={
                str(getattr(venue, "venue", "") or "").lower(): _venue_maker_fee_bps(
                    venue
                )
                for venue in config.venues
                if str(getattr(venue, "venue", "") or "").strip()
            },
            slippage_buffer_bps=slippage_bps,
            default_funding_interval_ms=int(
                getattr(strategy, "spread_paper_default_funding_interval_ms", 0) or 0
            ),
            excluded_symbols=list(
                getattr(strategy, "spread_paper_excluded_symbols", []) or []
            ),
            allowed_opportunity_labels=list(
                getattr(strategy, "spread_paper_allowed_opportunity_labels", []) or []
            ),
            episode_cooldown_ms=int(
                getattr(strategy, "spread_paper_episode_cooldown_ms", 0) or 0
            ),
            paper_bot_ids=list(getattr(strategy, "spread_paper_bot_ids", []) or []),
        )

    async def _fetch_quotes_direct(
        self,
        observed_ms: int,
    ) -> tuple[dict[str, QuoteSnapshot], set[str]]:
        async def _fetch_one(
            venue_name: str,
            source: object,
        ) -> tuple[str, Optional[dict[str, QuoteSnapshot]], Optional[Exception]]:
            try:
                result = await asyncio.wait_for(
                    source.fetch_all(list(self.config.symbols)),
                    timeout=self.refresh_timeout_s,
                )
                return venue_name, result, None
            except Exception as exc:
                return venue_name, None, exc

        results = await asyncio.gather(
            *[_fetch_one(venue, source) for venue, source in self._exchange_sources.items()],
            return_exceptions=False,
        )
        quotes: dict[str, QuoteSnapshot] = {}
        degraded_venues: set[str] = set()
        for venue_name, venue_quotes, error in results:
            if error is not None:
                degraded_venues.add(venue_name)
                continue
            for key, quote in (venue_quotes or {}).items():
                if int(getattr(quote, "observed_at_ms", 0) or 0) <= 0:
                    quote.observed_at_ms = observed_ms
                quotes[key] = quote
        return quotes, degraded_venues
