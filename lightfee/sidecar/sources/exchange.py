"""Exchange-native funding and market data source backed by MarketDataClient."""

from __future__ import annotations

import time
from typing import Optional

from lightfee.core.domain import Venue
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.venues.market_data import FundingTicker, MarketDataClient
from lightfee.venues.specs import VenueSpec, get_spec


class ExchangeSource:
    """Fetches funding data and market quotes from public exchange REST endpoints.

    Holds a MarketDataClient per venue. No credential required.
    """

    def __init__(self, spec: VenueSpec) -> None:
        self._client = MarketDataClient(spec)
        self.venue = spec.venue_id.value

    @classmethod
    def for_venue(cls, venue: Venue) -> ExchangeSource:
        return cls(get_spec(venue))

    async def close(self) -> None:
        await self._client.close()

    @staticmethod
    def _from_funding_ticker(ft: FundingTicker) -> QuoteSnapshot:
        return QuoteSnapshot(
            venue=ft.venue,
            symbol=ft.symbol,
            bid=ft.bid,
            ask=ft.ask,
            observed_at_ms=int(time.time() * 1000),
            source="sidecar_quote",
            bid_size=ft.bid_size,
            ask_size=ft.ask_size,
            funding_rate_bps=ft.funding_rate_bps,
            funding_timestamp_ms=ft.funding_timestamp_ms,
            mark_price=ft.mark_price,
            index_price=ft.index_price,
            volume_24h_quote=ft.volume_24h_quote,
            open_interest=ft.open_interest_quote,
            open_interest_evidence_status=ft.open_interest_evidence_status,
        )

    async def fetch_funding_rates(self, symbols: list[str]) -> dict[str, float]:
        """Fetch current funding rates for symbols. Returns {symbol: rate_bps}."""
        tickers = await self._client.fetch_funding_tickers(symbols)
        result: dict[str, float] = {}
        for key, ft in tickers.items():
            result[key] = ft.funding_rate_bps
        return result

    async def fetch_market_quotes(self, symbols: list[str]) -> dict[str, QuoteSnapshot]:
        """Fetch bid/ask/mark for symbols as QuoteSnapshot."""
        tickers = await self._client.fetch_funding_tickers(symbols)
        result: dict[str, QuoteSnapshot] = {}
        for key, ft in tickers.items():
            result[key] = self._from_funding_ticker(ft)
        return result

    async def fetch_all(self, symbols: list[str]) -> dict[str, QuoteSnapshot]:
        """Fetch full quote snapshots with funding and market data."""
        return await self.fetch_market_quotes(symbols)
