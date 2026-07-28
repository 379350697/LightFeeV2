"""Independent, per-venue spread quote publication data plane.

This process deliberately owns only the volatile top-of-book transport.  Slow
funding, contract and liquidity evidence remains in the primary sidecar and is
joined by exact venue/symbol key at the process boundary.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import replace
import logging
from math import isfinite
from pathlib import Path
import time

from lightfee.config.schema import AppConfig
from lightfee.marketdata.ws_bbo import TopBookQuote
from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.metadata_cache import (
    SpreadMetadataGeneration,
    quote_cache_contract_eligible,
)
from lightfee.spread.quote_snapshot import (
    SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
    SpreadQuoteSnapshot,
    publish_spread_quote_snapshot,
)
from lightfee.spread.universe import (
    SPREAD_SAMPLING_MAX_PAIR_COUNT,
    spread_sampling_pair_bound,
)


logger = logging.getLogger("lightfee.sidecar.spread_bbo")


class SpreadBboDataPlane:
    """Own isolated venue workers; a slow venue never blocks another one."""

    def __init__(
        self,
        config: AppConfig,
        *,
        sources: dict[str, object],
        metadata_quotes: Callable[
            [], Mapping[str, QuoteSnapshot] | SpreadMetadataGeneration
        ],
        metadata_quote_eligible: Callable[..., bool] | None = None,
        snapshot_path: str | Path,
    ) -> None:
        self.config = config
        self.sources = sources
        self.metadata_quotes = metadata_quotes
        self.metadata_quote_eligible = (
            metadata_quote_eligible
            if metadata_quote_eligible is not None
            else quote_cache_contract_eligible
        )
        self.snapshot_path = Path(snapshot_path)
        self.active = False
        self._quotes_by_venue: dict[str, dict[str, QuoteSnapshot]] = {}
        self._request_started_at_ms: dict[str, int] = {}
        self._accepted_request_started_at_ms: dict[str, int] = {}
        self._degraded_venues = set(sources)
        self._degraded_symbols: dict[str, set[str]] = {}
        self._last_error_by_venue: dict[str, str] = {}
        self._sampling_symbols = tuple(
            dict.fromkeys(
                str(symbol).strip().upper()
                for symbol in config.symbols
                if str(symbol).strip()
            )
        )

    @property
    def sampling_symbols(self) -> tuple[str, ...]:
        return self._sampling_symbols

    def set_sampling_symbols(self, symbols: list[str] | tuple[str, ...]) -> None:
        """Freeze the bounded spread-only universe before workers start."""

        if self.active:
            raise RuntimeError("spread BBO sampling universe cannot change while running")
        normalized = tuple(
            dict.fromkeys(
                str(symbol).strip().upper()
                for symbol in symbols
                if str(symbol).strip()
            )
        )
        if not normalized:
            raise ValueError("spread BBO sampling universe must not be empty")
        self._require_sampling_budget(normalized)
        self._sampling_symbols = normalized

    def _require_sampling_budget(self, symbols: tuple[str, ...]) -> None:
        if not symbols:
            raise ValueError("spread BBO sampling universe must not be empty")
        pair_bound = spread_sampling_pair_bound(symbols, self.sources)
        if pair_bound > SPREAD_SAMPLING_MAX_PAIR_COUNT:
            raise ValueError(
                "spread BBO sampling universe exceeds worst-case pair budget: "
                f"{pair_bound}>{SPREAD_SAMPLING_MAX_PAIR_COUNT}"
            )

    async def run(self, stop_event: asyncio.Event) -> None:
        if self.active:
            raise RuntimeError("spread BBO data plane already running")
        self._require_sampling_budget(self._sampling_symbols)
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
                {*workers, stop_task}, return_when=asyncio.FIRST_COMPLETED
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
        symbols = list(self._sampling_symbols)
        interval_s = max(int(self.config.runtime.spread_sidecar_refresh_ms or 0) / 1000.0, 0.05)
        # This bounds an individual transport without making transport latency
        # the quote-freshness clock. The receipt timestamp is checked later.
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
                    source.fetch_spread_bbo(symbols), timeout=timeout_s
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
                    logger.warning("spread BBO venue refresh degraded: venue=%s error=%s", venue, error)
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
        refresh_s = max(int(self.config.runtime.spread_sidecar_refresh_ms or 0) / 1000.0, 0.05)
        next_publish_at = 0.0
        while not stop_event.is_set():
            update_task = asyncio.create_task(update_event.wait())
            stop_task = asyncio.create_task(stop_event.wait())
            try:
                done, _pending = await asyncio.wait(
                    {update_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
                )
            finally:
                for task in (update_task, stop_task):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(update_task, stop_task, return_exceptions=True)
            if stop_task in done and stop_event.is_set():
                break
            update_event.clear()
            remaining_s = next_publish_at - loop.time()
            if remaining_s > 0.0:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=remaining_s)
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
                    validate_contract=False,
                )
                next_publish_at = loop.time() + min(max(refresh_s, 0.1), 0.25)
            except (OSError, TypeError, ValueError):
                logger.exception("spread BBO snapshot publication failed")

    def _accept_venue_update(
        self, venue_name: str, raw_quotes: dict[str, TopBookQuote] | None
    ) -> bool:
        venue_name = str(venue_name).strip().lower()
        before_quotes = self._quotes_by_venue.get(venue_name)
        before_degraded = venue_name in self._degraded_venues
        before_symbols = self._degraded_symbols.get(venue_name)
        requested = set(self._sampling_symbols)
        metadata_state = self.metadata_quotes()
        if isinstance(metadata_state, SpreadMetadataGeneration):
            metadata = metadata_state.quotes

            def metadata_eligible(quote: QuoteSnapshot) -> bool:
                return self.metadata_quote_eligible(quote, generation=metadata_state)
        else:
            metadata = metadata_state
            metadata_eligible = self.metadata_quote_eligible
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
            if bid <= 0.0 or ask <= 0.0 or bid > ask or bid_size <= 0.0 or ask_size <= 0.0:
                continue
            base = metadata.get(key)
            if base is None or not metadata_eligible(base):
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
            and str(quote.symbol).strip().upper() in requested
            and metadata_eligible(quote)
        }
        missing = expected_symbols - returned_symbols
        if missing:
            self._degraded_symbols[venue_name] = missing
        else:
            self._degraded_symbols.pop(venue_name, None)
        if accepted:
            self._quotes_by_venue[venue_name] = accepted
            accepted_observed_at_ms = min(int(quote.observed_at_ms or 0) for quote in accepted.values())
            request_started_at_ms = int(self._request_started_at_ms.get(venue_name, 0) or 0)
            self._accepted_request_started_at_ms[venue_name] = (
                request_started_at_ms
                if 0 < request_started_at_ms <= accepted_observed_at_ms
                else accepted_observed_at_ms
            )
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
        published_at_ms = int(time.time() * 1000)
        signal_ttl_ms = max(int(self.config.strategy.spread_signal_ttl_ms or 0), 1)
        quotes: dict[str, QuoteSnapshot] = {}
        fresh_venues: set[str] = set()
        expired_symbols: dict[str, set[str]] = {}
        for venue, venue_quotes in self._quotes_by_venue.items():
            for key, quote in venue_quotes.items():
                quote_age_ms = published_at_ms - int(quote.observed_at_ms or 0)
                if 0 <= quote_age_ms <= signal_ttl_ms:
                    quotes[key] = quote
                    fresh_venues.add(venue)
                else:
                    expired_symbols.setdefault(venue, set()).add(str(quote.symbol).strip().upper())
        if not quotes:
            return None
        observed_at_ms = max(int(quote.observed_at_ms or 0) for quote in quotes.values())
        starts = [
            int(self._accepted_request_started_at_ms.get(venue, 0) or 0)
            for venue in fresh_venues
        ]
        starts = [value for value in starts if value > 0]
        degraded_venues = set(self._degraded_venues)
        degraded_venues.update(set(self._quotes_by_venue) - fresh_venues)
        degraded_symbols = {venue: set(symbols) for venue, symbols in self._degraded_symbols.items()}
        for venue, symbols in expired_symbols.items():
            degraded_symbols.setdefault(venue, set()).update(symbols)
        return SpreadQuoteSnapshot(
            schema_version=SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
            published_at_ms=published_at_ms,
            market_observed_at_ms=observed_at_ms,
            batch_started_at_ms=min(min(starts, default=observed_at_ms), published_at_ms),
            configured_venues=sorted(self.sources),
            degraded_venues=sorted(degraded_venues),
            degraded_symbols={venue: sorted(symbols) for venue, symbols in degraded_symbols.items() if symbols},
            quotes=quotes,
            sampling_symbols=list(self._sampling_symbols),
            source_mode="sidecar_market_fast_path",
        )
