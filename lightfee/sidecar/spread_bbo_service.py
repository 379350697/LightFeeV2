"""Dedicated-process service for the spread BBO data plane."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from lightfee.config.schema import AppConfig
from lightfee.core.domain import Venue
from lightfee.sidecar.sources.exchange import ExchangeSource
from lightfee.sidecar.spread_bbo import SpreadBboDataPlane
from lightfee.spread.metadata_cache import SpreadMetadataSnapshotCache
from lightfee.spread.quote_snapshot import (
    SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
    spread_quote_snapshot_path,
)
from lightfee.venues.specs import get_spec
from lightfee.venues.transport import EndpointRateLimiter


logger = logging.getLogger("lightfee.sidecar.spread_bbo_service")


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
    def max_age_ms(self) -> int:
        return self._cache.max_age_ms

    def quote_eligible(self, quote) -> bool:
        return self._cache.quote_eligible(quote)

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
        self.sources: dict[str, ExchangeSource] = {}
        for venue_config in config.venues:
            venue_name = str(venue_config.venue or "").strip().lower()
            if not venue_name or venue_name in self.sources:
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
            metadata_quotes=lambda: self.metadata.quotes,
            metadata_quote_eligible=self.metadata.quote_eligible,
            snapshot_path=spread_quote_snapshot_path(config.runtime.sidecar_snapshot_path),
            snapshot_schema_version=SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
        )

    async def run(self, stop_event: asyncio.Event) -> None:
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
