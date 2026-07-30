"""Exchange-native funding and market data source backed by MarketDataClient."""

from __future__ import annotations
import asyncio
from dataclasses import replace
import logging
import time
from typing import Optional

from lightfee.core.domain import Venue
from lightfee.sidecar.snapshot import (
    SLOW_LIQUIDITY_EVIDENCE_PENDING_REASON,
    QuoteSnapshot,
)
from lightfee.venues.market_data import FundingTicker, MarketDataClient
from lightfee.venues.specs import VenueSpec, get_spec


logger = logging.getLogger(__name__)


class ExchangeSource:
    """Fetches funding data and market quotes from public exchange REST endpoints.

    Holds a MarketDataClient per venue. No credential required.
    """

    _SLOW_LIQUIDITY_REFRESH_INTERVAL_MS = 30_000

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
        self._slow_liquidity_tickers: dict[str, FundingTicker] = {}
        self._slow_liquidity_refresh_task: asyncio.Task | None = None
        self._slow_liquidity_last_requested_at_ms = 0

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
        task = getattr(self, "_slow_liquidity_refresh_task", None)
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self._client.close()

    async def _refresh_slow_liquidity(self, symbols: list[str]) -> None:
        """Refresh only missing OI evidence off the scan critical path."""
        try:
            tickers = await self._client.fetch_entry_open_interest_evidence(
                symbols,
                force_refresh=False,
            )
        except Exception as exc:
            # The fast funding/BBO scan remains publishable.  Missing slow
            # proof is represented when shortlist construction sees the cache.
            logger.warning(
                "sidecar slow-liquidity refresh failed venue=%s symbols=%d error=%s",
                self.venue,
                len(symbols),
                type(exc).__name__,
            )
            return
        merged = dict(self._slow_liquidity_tickers)
        for key, ticker in (tickers or {}).items():
            prior = merged.get(key)
            if prior is not None:
                ticker = replace(
                    ticker,
                    volume_24h_quote=prior.volume_24h_quote,
                )
            merged[key] = ticker
        self._slow_liquidity_tickers = merged

    def _remember_inline_slow_liquidity(
        self,
        tickers: dict[str, FundingTicker],
    ) -> list[str]:
        """Keep slow facts already present in the fast response locally.

        Several venues include OI in their market ticker.  Re-querying those
        rows would add pressure without improving evidence, so only rows
        lacking observed OI are sent to the dedicated slow refresh.
        """
        cached = dict(getattr(self, "_slow_liquidity_tickers", {}))
        missing: list[str] = []
        for key, ticker in tickers.items():
            observed = str(ticker.open_interest_evidence_status or "").lower() == "observed"
            if observed:
                cached[key] = ticker
            else:
                prior = cached.get(key)
                # The fast scan owns the current 24h-volume sample even when
                # it deliberately omitted OI.  Retain only prior OI proof.
                if prior is not None:
                    cached[key] = replace(
                        prior,
                        volume_24h_quote=ticker.volume_24h_quote,
                    )
                else:
                    cached[key] = ticker
                missing.append(str(ticker.symbol).upper())
        self._slow_liquidity_tickers = cached
        return missing

    def _schedule_slow_liquidity_refresh(self, symbols: list[str]) -> None:
        symbols = list(dict.fromkeys(symbol for symbol in symbols if symbol))
        if not symbols or not callable(
            getattr(self._client, "fetch_entry_open_interest_evidence", None)
        ):
            return
        now_ms = int(time.time() * 1_000)
        task = getattr(self, "_slow_liquidity_refresh_task", None)
        if task is not None and not task.done():
            return
        last_requested_at_ms = int(
            getattr(self, "_slow_liquidity_last_requested_at_ms", 0) or 0
        )
        if now_ms - last_requested_at_ms < self._SLOW_LIQUIDITY_REFRESH_INTERVAL_MS:
            return
        self._slow_liquidity_last_requested_at_ms = now_ms
        self._slow_liquidity_refresh_task = asyncio.create_task(
            self._refresh_slow_liquidity(list(symbols)),
            name=f"sidecar-slow-liquidity:{self.venue}",
        )

    @staticmethod
    def _with_cached_slow_liquidity(
        ticker: FundingTicker,
        cached: FundingTicker | None,
    ) -> FundingTicker:
        if cached is None or str(cached.open_interest_evidence_status or "").lower() != "observed":
            if str(ticker.open_interest_evidence_status or "").lower() == "unavailable":
                return replace(
                    ticker,
                    open_interest_evidence_reason=(
                        SLOW_LIQUIDITY_EVIDENCE_PENDING_REASON
                    ),
                )
            return ticker
        return replace(
            ticker,
            volume_24h_quote=cached.volume_24h_quote,
            open_interest_quote=cached.open_interest_quote,
            open_interest_evidence_status=cached.open_interest_evidence_status,
            open_interest_evidence_reason=cached.open_interest_evidence_reason,
            open_interest_observed_at_ms=cached.open_interest_observed_at_ms,
            open_interest_event_at_ms=cached.open_interest_event_at_ms,
            open_interest_received_at_ms=cached.open_interest_received_at_ms,
            open_interest_source=cached.open_interest_source,
            open_interest_sample_id=cached.open_interest_sample_id,
            open_interest_venue_symbol=cached.open_interest_venue_symbol,
            raw_open_interest=cached.raw_open_interest,
            raw_open_interest_unit=cached.raw_open_interest_unit,
            open_interest_contract_multiplier=cached.open_interest_contract_multiplier,
            open_interest_conversion_mark_price=(
                cached.open_interest_conversion_mark_price
            ),
            oi_candidate_count=cached.oi_candidate_count,
            oi_cache_hit_count=cached.oi_cache_hit_count,
            oi_cache_miss_count=cached.oi_cache_miss_count,
            oi_refresh_attempt_count=cached.oi_refresh_attempt_count,
            oi_refresh_cap=cached.oi_refresh_cap,
            oi_deferred_count=cached.oi_deferred_count,
            oi_timeout_count=cached.oi_timeout_count,
            oi_refresh_elapsed_ms=cached.oi_refresh_elapsed_ms,
        )

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

    def prime_slow_liquidity(self, quotes: list[QuoteSnapshot]) -> None:
        """Hydrate the local slow-evidence cache from the persisted snapshot."""
        restored: dict[str, FundingTicker] = {}
        for quote in quotes:
            symbol = str(getattr(quote, "symbol", "") or "").upper()
            if not symbol:
                continue
            ticker = FundingTicker(
                venue=self.venue,
                symbol=symbol,
                bid=0.0,
                ask=0.0,
                volume_24h_quote=float(getattr(quote, "volume_24h_quote", 0.0) or 0.0),
                open_interest_quote=float(getattr(quote, "open_interest", 0.0) or 0.0),
                open_interest_evidence_status=str(getattr(quote, "open_interest_evidence_status", "") or ""),
                open_interest_evidence_reason=str(getattr(quote, "open_interest_evidence_reason", "") or ""),
                open_interest_observed_at_ms=int(getattr(quote, "open_interest_observed_at_ms", 0) or 0),
                open_interest_event_at_ms=int(getattr(quote, "open_interest_event_at_ms", 0) or 0),
                open_interest_received_at_ms=int(getattr(quote, "open_interest_received_at_ms", 0) or 0),
                open_interest_source=str(getattr(quote, "open_interest_source", "") or ""),
                open_interest_sample_id=str(getattr(quote, "open_interest_sample_id", "") or ""),
                open_interest_venue_symbol=str(getattr(quote, "open_interest_venue_symbol", "") or ""),
            )
            restored[f"{self.venue}:{symbol}"] = ticker
        self._slow_liquidity_tickers = restored

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

    async def fetch_funding_metadata(
        self,
        symbols: list[str],
    ) -> dict[str, QuoteSnapshot]:
        """Fetch funding and contract evidence without acquiring a top book."""
        tickers = await self._client.fetch_funding_tickers(
            symbols,
            include_open_interest=False,
        )
        result: dict[str, QuoteSnapshot] = {}
        for key, ticker in tickers.items():
            quote = self._from_funding_ticker(ticker)
            # Single-process entry supplies executable prices from the runtime
            # WebSocket cache.  Metadata responses must not substitute stale
            # embedded ticker prices when a WS book is absent.
            quote.bid = 0.0
            quote.ask = 0.0
            quote.bid_size = 0.0
            quote.ask_size = 0.0
            quote.observed_at_ms = 0
            quote.market_event_at_ms = 0
            quote.source = "funding_metadata"
            result[key] = quote
        return result

    async def fetch_market_quotes(self, symbols: list[str]) -> dict[str, QuoteSnapshot]:
        """Fetch scan-time bid/ask/mark for symbols as QuoteSnapshot."""
        tickers = await self._client.fetch_funding_tickers(
            symbols,
            include_open_interest=False,
        )
        # OI carried by the fast ticker is already local evidence.  Only
        # contracts without it need the dedicated, slow endpoint.
        self._schedule_slow_liquidity_refresh(
            self._remember_inline_slow_liquidity(tickers)
        )
        cached_slow = getattr(self, "_slow_liquidity_tickers", {})
        result: dict[str, QuoteSnapshot] = {}
        for key, ft in tickers.items():
            ft = self._with_cached_slow_liquidity(
                ft,
                cached_slow.get(key),
            )
            listing_status = str(
                ft.open_interest_evidence_status or ""
            ).strip().lower()
            if (
                listing_status
                in {"symbol_not_listed", "ambiguous_mapping", "unavailable"}
                and float(ft.bid or 0.0) <= 0.0
                and float(ft.ask or 0.0) <= 0.0
                and str(ft.venue_status or "").strip().lower() != "active"
                and ft.contract_normalization_complete is not True
            ):
                # Venue-wide requests receive the cross-venue symbol union.
                # An absent scan row is not a degraded market quote.
                continue
            quote = self._from_funding_ticker(ft)
            if (
                float(quote.bid or 0.0) <= 0.0
                or float(quote.ask or 0.0) <= 0.0
                or int(quote.observed_at_ms or 0) <= 0
            ):
                # The single sidecar scan BBO is discovery evidence only.  A
                # missing row cannot become executable through stale metadata;
                # the live entry path always reacquires its candidate two-leg
                # BBO/L2 immediately before ordering.
                quote.bid = 0.0
                quote.ask = 0.0
                quote.bid_size = 0.0
                quote.ask_size = 0.0
                quote.observed_at_ms = 0
                quote.market_event_at_ms = 0
                quote.source = "sidecar_bbo_unavailable"
            result[key] = quote
        return result

    async def fetch_all(self, symbols: list[str]) -> dict[str, QuoteSnapshot]:
        """Fetch full quote snapshots with funding and market data."""
        return await self.fetch_market_quotes(symbols)
