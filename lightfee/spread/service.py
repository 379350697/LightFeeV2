"""Spread-reversion sidecar service."""

from __future__ import annotations

import asyncio
import time
from typing import Optional

from lightfee.config.schema import AppConfig
from lightfee.core.domain import Venue
from lightfee.sidecar.publisher import load_snapshot
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.models import SpreadSnapshot
from lightfee.spread.publisher import publish_spread_snapshot
from lightfee.spread.reversion import (
    SpreadReversionConfig,
    SpreadStatsTracker,
    build_spread_reversion_candidates,
)
from lightfee.venues.specs import get_spec


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

        if self.source_mode != "direct_market" or not self.direct_fetch_enabled:
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
        return snapshot

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
