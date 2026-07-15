"""Independent, per-venue spread quote publication data plane."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from math import isfinite
import logging
import time
from collections.abc import Callable
from pathlib import Path

from lightfee.config.schema import AppConfig
from lightfee.marketdata.ws_bbo import TopBookQuote
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.quote_snapshot import (
    SpreadQuoteSnapshot,
    publish_spread_quote_snapshot,
)

logger = logging.getLogger("lightfee.sidecar.spread_bbo")


class SpreadBboDataPlane:
    """Own isolated venue workers; no slowest-venue gather exists here."""

    def __init__(
        self,
        config: AppConfig,
        *,
        sources: dict[str, object],
        metadata_quotes: Callable[[], dict[str, QuoteSnapshot]],
        metadata_quote_eligible: Callable[[QuoteSnapshot], bool] | None = None,
        snapshot_path: str | Path,
    ) -> None:
        self.config = config
        self.sources = sources
        self.metadata_quotes = metadata_quotes
        self.metadata_quote_eligible = (
            metadata_quote_eligible
            if metadata_quote_eligible is not None
            else lambda quote: quote.contract_normalization_complete is True
        )
        self.snapshot_path = Path(snapshot_path)
        self.active = False
        self._quotes_by_venue: dict[str, dict[str, QuoteSnapshot]] = {}
        self._request_started_at_ms: dict[str, int] = {}
        self._degraded_venues = set(sources)
        self._degraded_symbols: dict[str, set[str]] = {}
        self._last_error_by_venue: dict[str, str] = {}

    async def run(self, stop_event: asyncio.Event) -> None:
        if self.active:
            raise RuntimeError("spread BBO data plane already running")
        self.active = True
        update_event = asyncio.Event()
        workers = [
            asyncio.create_task(self._venue_worker(venue, stop_event, update_event))
            for venue in self.sources
        ]
        workers.append(asyncio.create_task(self._publisher(stop_event, update_event)))
        stop_task = asyncio.create_task(stop_event.wait())
        try:
            done, _pending = await asyncio.wait(
                {*workers, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_task not in done:
                failed_task = next(task for task in workers if task in done)
                await failed_task
                raise RuntimeError("spread BBO worker exited unexpectedly")
        finally:
            if not stop_task.done():
                stop_task.cancel()
            for task in workers:
                if not task.done():
                    task.cancel()
            await asyncio.gather(stop_task, *workers, return_exceptions=True)
            self.active = False

    async def _venue_worker(
        self,
        venue: str,
        stop_event: asyncio.Event,
        update_event: asyncio.Event,
    ) -> None:
        source = self.sources[venue]
        symbols = list(
            dict.fromkeys(
                str(symbol).strip().upper()
                for symbol in self.config.symbols
                if str(symbol).strip()
            )
        )
        interval_s = max(
            int(self.config.runtime.spread_sidecar_refresh_ms or 0) / 1000.0,
            0.05,
        )
        # Transport latency and quote freshness are different clocks.  A slow
        # response is timestamped only when received, so cancelling it at the
        # quote TTL creates needless permanent degradation.  Per-venue workers
        # remain isolated while this bounded transport timeout is in flight.
        timeout_s = max(
            min(float(self.config.runtime.spread_sidecar_fetch_timeout_s or 0.0), 3.0),
            0.25,
        )
        loop = asyncio.get_running_loop()
        while not stop_event.is_set():
            cycle_started = loop.time()
            self._request_started_at_ms[venue] = int(time.time() * 1000)
            try:
                raw = await asyncio.wait_for(
                    source.fetch_spread_bbo(symbols),
                    timeout=timeout_s,
                )
                changed = self._accept_venue_update(venue, raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                changed = venue not in self._degraded_venues
                self._degraded_venues.add(venue)
                error = f"{type(exc).__name__}: {exc}"[:240]
                if self._last_error_by_venue.get(venue) != error:
                    self._last_error_by_venue[venue] = error
                    logger.warning(
                        "spread BBO venue refresh degraded: venue=%s error=%s",
                        venue,
                        error,
                    )
            if changed:
                update_event.set()

            remaining_s = interval_s - (loop.time() - cycle_started)
            if remaining_s <= 0.0:
                await asyncio.sleep(0)
                continue
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=remaining_s)
            except asyncio.TimeoutError:
                continue

    async def _publisher(
        self,
        stop_event: asyncio.Event,
        update_event: asyncio.Event,
    ) -> None:
        loop = asyncio.get_running_loop()
        refresh_s = max(
            int(self.config.runtime.spread_sidecar_refresh_ms or 0) / 1000.0,
            0.05,
        )
        publish_interval_s = min(max(refresh_s, 0.1), 0.25)
        next_publish_at = 0.0
        while not stop_event.is_set():
            update_task = asyncio.create_task(update_event.wait())
            stop_task = asyncio.create_task(stop_event.wait())
            try:
                done, _pending = await asyncio.wait(
                    {update_task, stop_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for task in (update_task, stop_task):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(
                    update_task,
                    stop_task,
                    return_exceptions=True,
                )
            if stop_task in done and stop_event.is_set():
                break
            # Clear before coalescing so an update arriving during the wait is
            # retained for the next generation instead of being lost.
            update_event.clear()
            remaining_s = next_publish_at - loop.time()
            if remaining_s > 0.0:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=remaining_s)
                except asyncio.TimeoutError:
                    pass
            if stop_event.is_set():
                break
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=0.025)
            except asyncio.TimeoutError:
                pass
            if stop_event.is_set():
                break
            snapshot = self._build_snapshot()
            if snapshot is None:
                continue
            try:
                await asyncio.to_thread(
                    publish_spread_quote_snapshot,
                    snapshot,
                    self.snapshot_path,
                )
                next_publish_at = loop.time() + publish_interval_s
            except (OSError, TypeError, ValueError):
                logger.exception("spread BBO snapshot publication failed")

    def _accept_venue_update(
        self,
        venue_name: str,
        raw_quotes: dict[str, TopBookQuote] | None,
    ) -> bool:
        venue_name = str(venue_name).strip().lower()
        before_quotes = self._quotes_by_venue.get(venue_name)
        before_degraded = venue_name in self._degraded_venues
        before_symbols = self._degraded_symbols.get(venue_name)
        requested = {
            str(symbol).strip().upper()
            for symbol in self.config.symbols
            if str(symbol).strip()
        }
        metadata = self.metadata_quotes()
        accepted: dict[str, QuoteSnapshot] = {}
        returned_symbols: set[str] = set()
        for raw_key, top in (raw_quotes or {}).items():
            venue = str(getattr(top, "venue", "") or "").strip().lower()
            symbol = str(getattr(top, "symbol", "") or "").strip().upper()
            key = f"{venue}:{symbol}"
            received_at_ms = int(getattr(top, "received_at_ms", 0) or 0)
            if (
                venue != venue_name
                or symbol not in requested
                or str(raw_key) != key
                or received_at_ms <= 0
                or int(getattr(top, "observed_at_ms", 0) or 0) != received_at_ms
            ):
                continue
            bid = float(getattr(top, "bid", 0.0) or 0.0)
            ask = float(getattr(top, "ask", 0.0) or 0.0)
            bid_size = float(getattr(top, "bid_size", 0.0) or 0.0)
            ask_size = float(getattr(top, "ask_size", 0.0) or 0.0)
            if not all(isfinite(value) for value in (bid, ask, bid_size, ask_size)):
                continue
            if bid <= 0.0 or ask <= 0.0 or bid > ask or bid_size < 0.0 or ask_size < 0.0:
                continue
            base = metadata.get(key)
            if base is None or not self.metadata_quote_eligible(base):
                continue
            accepted[key] = replace(
                base,
                bid=bid,
                ask=ask,
                bid_size=bid_size,
                ask_size=ask_size,
                bid_depth=(),
                ask_depth=(),
                observed_at_ms=received_at_ms,
                source=str(getattr(top, "source", "") or "sidecar_bulk_bbo_rest"),
            )
            returned_symbols.add(symbol)

        expected_symbols = {
            str(quote.symbol).strip().upper()
            for quote in metadata.values()
            if str(quote.venue).strip().lower() == venue_name
            and self.metadata_quote_eligible(quote)
        }
        missing = expected_symbols - returned_symbols
        if missing:
            self._degraded_symbols[venue_name] = missing
        else:
            self._degraded_symbols.pop(venue_name, None)
        if accepted:
            self._quotes_by_venue[venue_name] = accepted
            self._degraded_venues.discard(venue_name)
            self._last_error_by_venue.pop(venue_name, None)
        else:
            self._degraded_venues.add(venue_name)
        return (
            before_quotes != self._quotes_by_venue.get(venue_name)
            or before_degraded != (venue_name in self._degraded_venues)
            or before_symbols != self._degraded_symbols.get(venue_name)
        )

    def _build_snapshot(self) -> SpreadQuoteSnapshot | None:
        quotes = {
            key: quote
            for venue_quotes in self._quotes_by_venue.values()
            for key, quote in venue_quotes.items()
        }
        if not quotes:
            return None
        observed_at_ms = max(int(quote.observed_at_ms or 0) for quote in quotes.values())
        published_at_ms = max(int(time.time() * 1000), observed_at_ms)
        starts = [
            self._request_started_at_ms.get(venue, 0)
            for venue in self._quotes_by_venue
        ]
        starts = [int(value) for value in starts if int(value) > 0]
        return SpreadQuoteSnapshot(
            published_at_ms=published_at_ms,
            market_observed_at_ms=observed_at_ms,
            batch_started_at_ms=min(
                min(starts, default=observed_at_ms),
                published_at_ms,
            ),
            configured_venues=sorted(self.sources),
            degraded_venues=sorted(self._degraded_venues),
            degraded_symbols={
                venue: sorted(symbols)
                for venue, symbols in self._degraded_symbols.items()
                if symbols
            },
            quotes=quotes,
        )
