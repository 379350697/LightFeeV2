"""Exchange-native funding and market data sources (no Chillybot)."""

from __future__ import annotations

from typing import Optional

from lightfee.core.domain import Venue
from lightfee.sidecar.snapshot import QuoteSnapshot


class ExchangeSource:
    """Fetches funding data and market quotes from exchange REST/WS."""

    def __init__(self, venue: Venue) -> None:
        self.venue = venue

    async def fetch_funding_rates(self, symbols: list[str]) -> dict[str, float]:
        """Fetch current funding rates for symbols. Returns {symbol: rate_bps}."""
        return {}

    async def fetch_market_quotes(self, symbols: list[str]) -> dict[str, QuoteSnapshot]:
        """Fetch bid/ask/mark for symbols."""
        return {}

    async def fetch_all(
        self, symbols: list[str]
    ) -> dict[str, QuoteSnapshot]:
        """Fetch full quote snapshots with funding and market data."""
        return {}
