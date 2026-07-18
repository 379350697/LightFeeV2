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
    HyperliquidBboWsClient,
    RestTopBookQuoteRefresher,
    TopBookQuote,
    VenueBboCache,
)
from lightfee.sidecar.sources.exchange import ExchangeSource
from lightfee.sidecar.spread_bbo import SpreadBboDataPlane
from lightfee.spread.metadata_cache import SpreadMetadataSnapshotCache
from lightfee.spread.quote_snapshot import (
    SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
    spread_quote_snapshot_path,
)
from lightfee.spread.universe import resolve_spread_sampling_symbols
from lightfee.venues.specs import get_spec
from lightfee.venues.transport import EndpointRateLimiter


logger = logging.getLogger("lightfee.sidecar.spread_bbo_service")


class HyperliquidSpreadBboSource:
    """Fresh Hyperliquid BBOs backed only by the official BBO WebSocket."""

    venue = "hyperliquid"

    def __init__(
        self,
        *,
        max_age_ms: int,
        rest_fallback: RestTopBookQuoteRefresher | None = None,
    ) -> None:
        self.max_age_ms = max(int(max_age_ms or 0), 1)
        self._cache = VenueBboCache()
        self._clients: dict[str, HyperliquidBboWsClient] = {}
        self._spec = get_spec(Venue.HYPERLIQUID)
        self._rest_fallback = rest_fallback or RestTopBookQuoteRefresher(
            timeout_ms=min(self.max_age_ms, 750),
            venue_async_concurrency=RestTopBookQuoteRefresher.GLOBAL_ASYNC_CONCURRENCY,
        )

    def _new_client(self, symbol: str) -> HyperliquidBboWsClient:
        canonical = str(symbol or "").strip().upper()
        venue_symbol = canonical
        if self._spec.symbol_to_venue is not None:
            venue_symbol = self._spec.symbol_to_venue(canonical)
        return HyperliquidBboWsClient(
            venue=self.venue,
            symbol=canonical,
            venue_symbol=str(venue_symbol or canonical),
            cache=self._cache,
        )

    async def start(self, symbols: list[str]) -> None:
        for raw_symbol in symbols:
            symbol = str(raw_symbol or "").strip().upper()
            if not symbol or symbol in self._clients:
                continue
            client = self._new_client(symbol)
            self._clients[symbol] = client
            await client.start()

    async def fetch_spread_bbo(
        self,
        symbols: list[str],
    ) -> dict[str, TopBookQuote]:
        now_ms = int(time.time() * 1000)
        quotes: dict[str, TopBookQuote] = {}
        fallback_symbols: list[str] = []
        for raw_symbol in symbols:
            symbol = str(raw_symbol or "").strip().upper()
            quote = self._cache.get_quote(self.venue, symbol)
            if quote is None:
                # Keep the fallback scoped to symbols already started by this
                # source, while allowing REST to bridge WS reconnect windows.
                if symbol in self._clients:
                    fallback_symbols.append(symbol)
                continue
            received_at_ms = int(quote.received_at_ms or 0)
            if (
                received_at_ms <= 0
                or received_at_ms > now_ms
                or now_ms - received_at_ms > self.max_age_ms
            ):
                if symbol in self._clients:
                    fallback_symbols.append(symbol)
                continue
            # The spread producer contract uses the local receipt timestamp as
            # its anti-skew decision clock. The exchange event timestamp stays
            # available separately on TopBookQuote.
            quotes[f"{self.venue}:{symbol}"] = replace(
                quote,
                observed_at_ms=received_at_ms,
                received_at_ms=received_at_ms,
                exchange_event_at_ms=int(
                    quote.exchange_event_at_ms or quote.observed_at_ms or 0
                ),
            )
        if fallback_symbols:
            results = await asyncio.gather(
                *(
                    self._rest_fallback.arefresh_quote_result(
                        self.venue,
                        symbol,
                        now_ms=now_ms,
                    )
                    for symbol in fallback_symbols
                )
            )
            received_now_ms = int(time.time() * 1000)
            for symbol, result in zip(fallback_symbols, results, strict=True):
                quote = result.quote
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
                normalized = replace(
                    quote,
                    observed_at_ms=received_at_ms,
                    received_at_ms=received_at_ms,
                )
                self._cache.update_quote(
                    normalized,
                    now_ms=received_now_ms,
                    current_max_age_ms=self.max_age_ms,
                )
                quotes[f"{self.venue}:{symbol}"] = normalized
        return quotes

    async def close(self) -> None:
        clients = list(self._clients.items())
        results = await asyncio.gather(
            self._rest_fallback.aclose(),
            *(client.stop() for _symbol, client in clients),
            return_exceptions=True,
        )
        failures: list[Exception] = []
        fallback_result, *client_results = results
        if isinstance(fallback_result, BaseException):
            failure = (
                fallback_result
                if isinstance(fallback_result, Exception)
                else RuntimeError(
                    f"{type(fallback_result).__name__}: {fallback_result}"
                )
            )
            failures.append(failure)
            logger.error(
                "Hyperliquid spread BBO REST fallback close failed",
                exc_info=(type(failure), failure, failure.__traceback__),
            )
        for (symbol, client), result in zip(
            clients,
            client_results,
            strict=True,
        ):
            if isinstance(result, BaseException):
                failure = (
                    result
                    if isinstance(result, Exception)
                    else RuntimeError(f"{type(result).__name__}: {result}")
                )
                failures.append(failure)
                logger.error(
                    "Hyperliquid spread BBO client stop failed: symbol=%s",
                    symbol,
                    exc_info=(type(failure), failure, failure.__traceback__),
                )
                continue
            if self._clients.get(symbol) is client:
                self._clients.pop(symbol, None)
        if failures:
            raise ExceptionGroup(
                "Hyperliquid spread BBO client shutdown failed",
                failures,
            )


class SpreadMetadataCache:
    """Atomic last-good cache refreshed from the slow sidecar handoff."""

    def __init__(
        self,
        sidecar_snapshot_path: str | Path,
        *,
        max_age_ms: int,
    ) -> None:
        self.sidecar_snapshot_path = Path(sidecar_snapshot_path)
        self._cache = SpreadMetadataSnapshotCache(
            self.sidecar_snapshot_path,
            max_age_ms=max_age_ms,
        )

    @property
    def quotes(self):
        return self._cache.quotes

    @property
    def generation(self):
        return self._cache.generation

    @property
    def max_age_ms(self) -> int:
        return self._cache.max_age_ms

    def quote_eligible(self, quote, *, now_ms=None, generation=None) -> bool:
        return self._cache.quote_eligible(
            quote,
            now_ms=now_ms,
            generation=generation,
        )

    async def refresh_once(self) -> bool:
        return await asyncio.to_thread(self._cache.refresh)

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await asyncio.to_thread(self._cache.refresh)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                continue


class SpreadBboProcessService:
    """Own BBO transports and publication in a GIL-independent process."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.metadata = SpreadMetadataCache(
            config.runtime.sidecar_snapshot_path,
            # Contract/funding evidence is produced by the slow sidecar lane.
            # Its last-good policy is intentionally independent from the
            # sub-second BBO publication TTL.
            max_age_ms=config.runtime.live_scan_last_good_max_age_ms,
        )
        self.sources: dict[str, object] = {}
        self.hyperliquid_source: HyperliquidSpreadBboSource | None = None
        for venue_config in config.venues:
            venue_name = str(venue_config.venue or "").strip().lower()
            if not venue_name or venue_name in self.sources:
                continue
            if venue_name == Venue.HYPERLIQUID.value:
                self.hyperliquid_source = HyperliquidSpreadBboSource(
                    max_age_ms=config.strategy.spread_signal_ttl_ms,
                )
                self.sources[venue_name] = self.hyperliquid_source
                continue
            spec = get_spec(Venue.from_str(venue_name))
            self.sources[venue_name] = ExchangeSource(
                spec,
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
            snapshot_schema_version=SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await self.metadata.refresh_once()
            metadata_generation = self.metadata.generation
            sampling_symbols = resolve_spread_sampling_symbols(
                self.config,
                metadata_generation.quotes,
                quote_eligible=lambda quote: self.metadata.quote_eligible(
                    quote,
                    generation=metadata_generation,
                ),
            )
            if sampling_symbols:
                self.data_plane.set_sampling_symbols(sampling_symbols)
                if self.hyperliquid_source is not None:
                    # Start the complete bounded producer universe.  Filtering
                    # by the current metadata generation would make a
                    # transiently missing Hyperliquid generation permanent:
                    # the source would own no stream when metadata later
                    # recovered. Unsupported symbols simply produce no cached
                    # quote and are not expected unless metadata contains them.
                    await self.hyperliquid_source.start(sampling_symbols)
                logger.info(
                    "spread BBO sampling universe frozen: symbols=%d "
                    "global_symbols=%d hyperliquid_ws_symbols=%d",
                    len(sampling_symbols),
                    len(self.config.symbols),
                    len(sampling_symbols)
                    if self.hyperliquid_source is not None
                    else 0,
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
                {metadata_task, data_plane_task},
                return_when=asyncio.FIRST_COMPLETED,
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
            await asyncio.gather(
                metadata_task,
                data_plane_task,
                return_exceptions=True,
            )
            await self.close()

    async def close(self) -> None:
        for source in self.sources.values():
            try:
                await source.close()
            except Exception:
                logger.exception("spread BBO source close failed")
