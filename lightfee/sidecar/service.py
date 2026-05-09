"""Sidecar refresh service: gathers exchange-native data, builds pairs, publishes snapshot."""

from __future__ import annotations

import time
from typing import Optional

from lightfee.config.schema import AppConfig, VenueConfig
from lightfee.core.domain import Venue
from lightfee.sidecar.pairing import build_same_symbol_pairs
from lightfee.sidecar.publisher import publish_snapshot
from lightfee.sidecar.snapshot import (
    CandidateInput,
    FundingLifecycle,
    MarketLifecycle,
    QuoteSnapshot,
    SidecarSnapshot,
)
from lightfee.sidecar.sources.exchange import ExchangeSource
from lightfee.sidecar.sources.liquidity import LiquiditySource


class SidecarService:
    """Exchange-native only sidecar. No Chillybot sources."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.snapshot_path = config.runtime.sidecar_snapshot_path
        self._sources: dict[str, ExchangeSource] = {}
        self._liquidity_sources: dict[str, LiquiditySource] = {}
        for vc in config.venues:
            venue = Venue.from_str(vc.venue)
            self._sources[vc.venue] = ExchangeSource(venue)
            self._liquidity_sources[vc.venue] = LiquiditySource(venue)

    async def refresh_once(self) -> SidecarSnapshot:
        """Fetch all exchange data and build candidate snapshot."""
        now_ms = int(time.time() * 1000)
        symbols = self.config.symbols

        quotes: dict[str, QuoteSnapshot] = {}
        funding_lifecycle: list[FundingLifecycle] = []
        market_lifecycle: list[MarketLifecycle] = []
        degraded: list[str] = []

        for vc in self.config.venues:
            source = self._sources.get(vc.venue)
            if source is None:
                continue
            try:
                venue_quotes = await source.fetch_all(symbols)
                for key, q in venue_quotes.items():
                    quotes[key] = q
                market_lifecycle.append(
                    MarketLifecycle(venue=vc.venue, observed_at_ms=now_ms, symbol_count=len(venue_quotes))
                )
                funding_lifecycle.append(
                    FundingLifecycle(venue=vc.venue, observed_at_ms=now_ms, symbol_count=len(venue_quotes))
                )
            except Exception:
                degraded.append(vc.venue)

        candidates = build_same_symbol_pairs(quotes, symbols)

        snapshot = SidecarSnapshot(
            published_at_ms=now_ms,
            market_observed_at_ms=now_ms,
            funding_lifecycle=funding_lifecycle,
            market_lifecycle=market_lifecycle,
            degraded_venues=degraded,
            quotes=quotes,
            candidates=candidates,
        )

        publish_snapshot(snapshot, self.snapshot_path)
        return snapshot
