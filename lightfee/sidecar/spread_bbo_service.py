"""Dedicated-process service for the spread BBO data plane."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import logging
from pathlib import Path
import time

from lightfee.config.schema import AppConfig
from lightfee.core.domain import Venue
from lightfee.marketdata.ws_bbo import (
    HyperliquidMultiplexBboWsClient,
    TopBookQuote,
    VenueBboCache,
)
from lightfee.sidecar.sources.exchange import ExchangeSource
from lightfee.sidecar.spread_bbo import SpreadBboDataPlane
from lightfee.spread.metadata_cache import SpreadMetadataSnapshotCache
from lightfee.spread.quote_snapshot import spread_quote_snapshot_path
from lightfee.spread.universe import resolve_spread_sampling_symbols
from lightfee.venues.specs import get_spec
from lightfee.venues.transport import EndpointRateLimiter


logger = logging.getLogger("lightfee.sidecar.spread_bbo_service")


class HyperliquidSpreadBboSource:
    """Fresh Hyperliquid BBOs backed by the official BBO WebSocket."""

    venue = "hyperliquid"

    def __init__(
        self,
        *,
        max_age_ms: int,
        rest_fallback: ExchangeSource | None = None,
    ) -> None:
        self.max_age_ms = max(int(max_age_ms or 0), 1)
        self.refresh_age_ms = max(self.max_age_ms // 2, 1)
        self._cache = VenueBboCache()
        self._clients: dict[str, HyperliquidMultiplexBboWsClient] = {}
        self._multiplex_client: HyperliquidMultiplexBboWsClient | None = None
        self._spec = get_spec(Venue.HYPERLIQUID)
        # A WS gap must not fan out into one /info request per symbol.  The
        # venue-wide ``metaAndAssetCtxs`` request covers the complete frozen
        # research universe in one response; pace it independently of the
        # funding sidecar and retain WS as the primary source.
        self._rest_fallback = rest_fallback or ExchangeSource(
            get_spec(Venue.HYPERLIQUID),
            rate_limiter=EndpointRateLimiter(1000, 8000, 1000),
            http_max_connections=8,
            consume_global_rate_limit_budget=False,
        )

    async def start(self, symbols: list[str]) -> None:
        mappings: dict[str, str] = {}
        for raw_symbol in symbols:
            symbol = str(raw_symbol or "").strip().upper()
            if not symbol or symbol in self._clients:
                continue
            venue_symbol = (
                self._spec.symbol_to_venue(symbol)
                if self._spec.symbol_to_venue is not None
                else symbol
            )
            mappings[symbol] = str(venue_symbol or symbol)
        if not mappings:
            return
        client = self._multiplex_client
        if client is None:
            client = HyperliquidMultiplexBboWsClient(self._cache)
            self._multiplex_client = client
        await client.add_symbols(mappings)
        self._clients.update({symbol: client for symbol in mappings})
        await client.start()

    async def fetch_spread_bbo(self, symbols: list[str]) -> dict[str, TopBookQuote]:
        now_ms = int(time.time() * 1000)
        quotes: dict[str, TopBookQuote] = {}
        fallback_symbols: list[str] = []
        for raw_symbol in symbols:
            symbol = str(raw_symbol or "").strip().upper()
            quote = self._cache.get_quote(self.venue, symbol)
            if quote is None:
                if symbol in self._clients:
                    fallback_symbols.append(symbol)
                continue
            received_at_ms = int(quote.received_at_ms or 0)
            if received_at_ms <= 0 or received_at_ms > now_ms or now_ms - received_at_ms > self.max_age_ms:
                if symbol in self._clients:
                    fallback_symbols.append(symbol)
                continue
            quotes[f"{self.venue}:{symbol}"] = replace(
                quote,
                observed_at_ms=received_at_ms,
                received_at_ms=received_at_ms,
                exchange_event_at_ms=int(quote.exchange_event_at_ms or quote.observed_at_ms or 0),
            )
            if now_ms - received_at_ms >= self.refresh_age_ms and symbol in self._clients:
                fallback_symbols.append(symbol)
        if fallback_symbols:
            raw_fallback = await self._rest_fallback.fetch_spread_bbo(
                fallback_symbols
            )
            received_now_ms = int(time.time() * 1000)
            for symbol in fallback_symbols:
                quote = raw_fallback.get(f"{self.venue}:{symbol}")
                received_at_ms = int(getattr(quote, "received_at_ms", 0) or 0)
                if (
                    quote is None
                    or str(quote.venue or "").strip().lower() != self.venue
                    or str(quote.symbol or "").strip().upper() != symbol
                    or received_at_ms <= 0
                    or received_at_ms > received_now_ms
                    or received_now_ms - received_at_ms > self.max_age_ms
                ):
                    continue
                normalized = replace(quote, observed_at_ms=received_at_ms, received_at_ms=received_at_ms)
                self._cache.update_quote(
                    normalized,
                    now_ms=received_now_ms,
                    current_max_age_ms=self.max_age_ms,
                )
                quotes[f"{self.venue}:{symbol}"] = normalized
        return quotes

    async def close(self) -> None:
        clients = list({id(client): client for client in self._clients.values()}.values())
        results = await asyncio.gather(
            self._rest_fallback.close(),
            *(client.stop() for client in clients),
            return_exceptions=True,
        )
        self._clients.clear()
        self._multiplex_client = None
        failures = [result for result in results if isinstance(result, Exception)]
        if failures:
            raise ExceptionGroup("Hyperliquid spread BBO shutdown failed", failures)


class SpreadMetadataCache:
    """Async facade for the atomic last-good primary-sidecar metadata cache."""

    def __init__(self, sidecar_snapshot_path: str | Path, *, max_age_ms: int) -> None:
        self._cache = SpreadMetadataSnapshotCache(
            sidecar_snapshot_path,
            max_age_ms=max_age_ms,
        )

    @property
    def generation(self):
        return self._cache.generation

    def quote_eligible(self, quote, *, now_ms=None, generation=None) -> bool:
        return self._cache.quote_eligible(quote, now_ms=now_ms, generation=generation)

    async def refresh_once(self) -> bool:
        return await asyncio.to_thread(self._cache.refresh)

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await self.refresh_once()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                continue


class SpreadBboProcessService:
    """Own BBO transports and publication in a separate process."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.metadata = SpreadMetadataCache(
            config.runtime.sidecar_snapshot_path,
            # Slow evidence has its own last-good lifetime; it is intentionally
            # independent from the one-second executable BBO admission gate.
            max_age_ms=config.runtime.sidecar_snapshot_max_age_ms,
        )
        self.sources: dict[str, object] = {}
        self.hyperliquid_source: HyperliquidSpreadBboSource | None = None
        for venue_config in config.venues:
            venue_name = str(venue_config.venue or "").strip().lower()
            if not venue_name or venue_name in self.sources:
                continue
            if venue_name == Venue.HYPERLIQUID.value:
                self.hyperliquid_source = HyperliquidSpreadBboSource(
                    max_age_ms=config.strategy.spread_signal_ttl_ms
                )
                self.sources[venue_name] = self.hyperliquid_source
                continue
            self.sources[venue_name] = ExchangeSource(
                get_spec(Venue.from_str(venue_name)),
                rate_limiter=EndpointRateLimiter(1000, 8000, 250),
                http_max_connections=32,
                consume_global_rate_limit_budget=False,
            )
        self.data_plane = SpreadBboDataPlane(
            config,
            sources=self.sources,
            metadata_quotes=lambda: self.metadata.generation,
            metadata_quote_eligible=self.metadata.quote_eligible,
            snapshot_path=spread_quote_snapshot_path(config.runtime.sidecar_snapshot_path),
        )

    @property
    def collection_enabled(self) -> bool:
        """Whether an operator has enabled a spread consumer for this feed.

        BBO collection is not funding-market data. Keeping its systemd unit
        alive while both spread signal collection and paper trading are off
        must not create exchange traffic merely because the unit is installed.
        """

        strategy = self.config.strategy
        return bool(
            strategy.spread_reversion_enabled is True
            or strategy.spread_paper_enabled is True
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        if not self.collection_enabled:
            logger.info(
                "spread BBO collection disabled; waiting without exchange requests"
            )
            try:
                await stop_event.wait()
            finally:
                await self.close()
            return
        while not stop_event.is_set():
            await self.metadata.refresh_once()
            generation = self.metadata.generation
            sampling_symbols = resolve_spread_sampling_symbols(
                self.config,
                generation.quotes,
                quote_eligible=lambda quote: self.metadata.quote_eligible(
                    quote, generation=generation
                ),
            )
            if sampling_symbols:
                self.data_plane.set_sampling_symbols(sampling_symbols)
                if self.hyperliquid_source is not None:
                    await self.hyperliquid_source.start(sampling_symbols)
                logger.info(
                    "spread BBO sampling universe frozen: symbols=%d global_symbols=%d",
                    len(sampling_symbols),
                    len(self.config.symbols),
                )
                break
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=0.25)
            except asyncio.TimeoutError:
                continue
        if stop_event.is_set():
            return
        metadata_task = asyncio.create_task(self.metadata.run(stop_event))
        data_plane_task = asyncio.create_task(self.data_plane.run(stop_event))
        try:
            done, _pending = await asyncio.wait(
                {metadata_task, data_plane_task}, return_when=asyncio.FIRST_COMPLETED
            )
            if not stop_event.is_set():
                failed = next(task for task in done)
                await failed
                raise RuntimeError("spread BBO process worker exited unexpectedly")
        finally:
            stop_event.set()
            for task in (metadata_task, data_plane_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(metadata_task, data_plane_task, return_exceptions=True)
            await self.close()

    async def close(self) -> None:
        for source in self.sources.values():
            try:
                await source.close()
            except Exception:
                logger.exception("spread BBO source close failed")
