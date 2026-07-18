"""Exchange-native funding and market data source backed by MarketDataClient."""

from __future__ import annotations
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
            observed_at_ms=int(ft.market_received_at_ms or 0),
            market_event_at_ms=int(ft.market_event_at_ms or 0),
            source="sidecar_quote",
            bid_size=ft.bid_size,
            ask_size=ft.ask_size,
            funding_rate_bps=ft.funding_rate_bps,
            funding_rate_observed_at_ms=int(
                ft.funding_rate_observed_at_ms or 0
            ),
            funding_rate_event_at_ms=int(ft.funding_rate_event_at_ms or 0),
            funding_rate_received_at_ms=int(
                ft.funding_rate_received_at_ms or 0
            ),
            funding_rate_source=ft.funding_rate_source,
            funding_rate_sample_id=ft.funding_rate_sample_id,
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
            open_interest_observed_at_ms=int(ft.open_interest_observed_at_ms or 0),
            open_interest_event_at_ms=int(ft.open_interest_event_at_ms or 0),
            open_interest_received_at_ms=int(ft.open_interest_received_at_ms or 0),
            open_interest_source=ft.open_interest_source,
            open_interest_sample_id=ft.open_interest_sample_id,
            open_interest_venue_symbol=ft.open_interest_venue_symbol,
            raw_open_interest=ft.raw_open_interest,
            raw_open_interest_unit=ft.raw_open_interest_unit,
            open_interest_contract_multiplier=ft.open_interest_contract_multiplier,
            open_interest_conversion_mark_price=ft.open_interest_conversion_mark_price,
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
        tickers = await self._client.fetch_funding_tickers(
            symbols,
            include_open_interest=False,
        )
        result: dict[str, QuoteSnapshot] = {}
        for key, ft in tickers.items():
            listing_status = str(
                ft.open_interest_evidence_status or ""
            ).strip().lower()
            if (
                listing_status in {"symbol_not_listed", "ambiguous_mapping"}
                and float(ft.bid or 0.0) <= 0.0
                and float(ft.ask or 0.0) <= 0.0
            ):
                # Venue-wide requests receive the cross-venue symbol union.
                # Known non-listings and non-unique venue mappings are absent
                # rows, not failed market observations.  Keep a genuinely
                # listed row with a missing final BBO below so it still fails
                # closed as degraded evidence.
                continue
            result[key] = self._from_funding_ticker(ft)
        # Funding, contract, and fee metadata are slow variables. Reacquire a
        # lightweight BBO after that work so candidate prices keep the actual
        # response-arrival clock instead of inheriting an aged first request.
        try:
            top_books = await self._client.fetch_top_book_quotes(symbols)
        except Exception:
            top_books = {}
        for key, quote in result.items():
            top = top_books.get(key)
            if top is None:
                # The funding ticker remains useful for slow funding/contract
                # metadata, but its embedded price was observed before that
                # work completed.  It must never masquerade as a fresh Top-K
                # ranking seed when the final BBO request failed or omitted the
                # symbol.
                quote.bid = 0.0
                quote.ask = 0.0
                quote.bid_size = 0.0
                quote.ask_size = 0.0
                quote.observed_at_ms = 0
                quote.market_event_at_ms = 0
                quote.source = "sidecar_bbo_unavailable"
                continue
            received_at_ms = int(top.received_at_ms or top.observed_at_ms or 0)
            if received_at_ms <= 0:
                quote.bid = 0.0
                quote.ask = 0.0
                quote.bid_size = 0.0
                quote.ask_size = 0.0
                quote.observed_at_ms = 0
                quote.market_event_at_ms = 0
                quote.source = "sidecar_bbo_unavailable"
                continue
            quote.bid = float(top.bid)
            quote.ask = float(top.ask)
            quote.bid_size = float(top.bid_size or 0.0)
            quote.ask_size = float(top.ask_size or 0.0)
            quote.observed_at_ms = received_at_ms
            quote.market_event_at_ms = int(top.exchange_event_at_ms or 0)
            quote.source = str(top.source or "sidecar_rest_bbo")
        return result

    async def fetch_all(self, symbols: list[str]) -> dict[str, QuoteSnapshot]:
        """Fetch full quote snapshots with funding and market data."""
        return await self.fetch_market_quotes(symbols)

    async def fetch_spread_bbo(self, symbols: list[str]) -> dict[str, TopBookQuote]:
        """Fetch the lightweight BBO-only universe for spread sampling."""
        return await self._client.fetch_top_book_quotes(symbols)
