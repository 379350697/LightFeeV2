"""Exchange-native funding and market data source backed by MarketDataClient."""

from __future__ import annotations

import time
from typing import Optional

from lightfee.core.domain import Venue
from lightfee.marketdata.ws_bbo import TopBookQuote
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.venues.market_data import FundingTicker, MarketDataClient
from lightfee.venues.specs import VenueSpec, get_spec


class ExchangeSource:
    """Fetches funding data and market quotes from public exchange REST endpoints.

    Holds a MarketDataClient per venue. No credential required.
    """

    def __init__(
        self,
        spec: VenueSpec,
        rate_limiter: Optional[object] = None,
        http_max_connections: int | None = None,
        consume_global_rate_limit_budget: bool = True,
    ) -> None:
        self._client = MarketDataClient(
            spec,
            rate_limiter=rate_limiter,
            http_max_connections=http_max_connections,
            consume_global_rate_limit_budget=consume_global_rate_limit_budget,
        )
        self.venue = spec.venue_id.value

    @classmethod
    def for_venue(
        cls,
        venue: Venue,
        rate_limiter: Optional[object] = None,
        http_max_connections: int | None = None,
        consume_global_rate_limit_budget: bool = True,
    ) -> ExchangeSource:
        return cls(
            get_spec(venue),
            rate_limiter=rate_limiter,
            http_max_connections=http_max_connections,
            consume_global_rate_limit_budget=consume_global_rate_limit_budget,
        )

    async def close(self) -> None:
        await self._client.close()

    def share_contract_metadata_cache_from(self, other: "ExchangeSource") -> None:
        self._client.share_contract_metadata_cache_from(other._client)

    def prime_funding_schedule(self, quotes: list[QuoteSnapshot]) -> None:
        """Restore observed funding cadence from a prior published snapshot."""
        self._client.prime_funding_schedule(
            FundingTicker(
                venue=quote.venue,
                symbol=quote.symbol,
                bid=quote.bid,
                ask=quote.ask,
                funding_timestamp_ms=quote.funding_timestamp_ms,
                funding_interval_ms=quote.funding_interval_ms,
            )
            for quote in quotes
        )

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
            funding_interval_ms=ft.funding_interval_ms,
            predicted_funding_rate_bps=ft.predicted_funding_rate_bps,
            funding_forecast_source=ft.funding_forecast_source,
            funding_forecast_sample_count=ft.funding_forecast_sample_count,
            settled_funding_rate_bps=ft.settled_funding_rate_bps,
            mark_price=ft.mark_price,
            index_price=ft.index_price,
            volume_24h_quote=ft.volume_24h_quote,
            open_interest=ft.open_interest_quote,
            open_interest_evidence_status=ft.open_interest_evidence_status,
            open_interest_evidence_reason=ft.open_interest_evidence_reason,
            oi_candidate_count=ft.oi_candidate_count,
            oi_cache_hit_count=ft.oi_cache_hit_count,
            oi_cache_miss_count=ft.oi_cache_miss_count,
            oi_refresh_attempt_count=ft.oi_refresh_attempt_count,
            oi_refresh_cap=ft.oi_refresh_cap,
            oi_deferred_count=ft.oi_deferred_count,
            oi_timeout_count=ft.oi_timeout_count,
            oi_refresh_elapsed_ms=ft.oi_refresh_elapsed_ms,
            underlying=ft.underlying,
            quote_currency=ft.quote_currency,
            contract_type=ft.contract_type,
            contract_multiplier=ft.contract_multiplier,
            mark_index_source=ft.mark_index_source,
            price_precision=ft.price_precision,
            quantity_precision=ft.quantity_precision,
            price_tick=ft.price_tick,
            quantity_step_base=ft.quantity_step_base,
            min_quantity_base=ft.min_quantity_base,
            min_notional_quote=ft.min_notional_quote,
            min_notional_evidence_complete=ft.min_notional_evidence_complete,
            venue_status=ft.venue_status,
            contract_normalization_complete=ft.contract_normalization_complete,
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

    async def fetch_spread_bbo(self, symbols: list[str]) -> dict[str, TopBookQuote]:
        """Fetch the lightweight BBO-only universe for spread sampling."""
        return await self._client.fetch_top_book_quotes(symbols)
