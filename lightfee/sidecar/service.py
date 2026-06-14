"""Sidecar refresh service: concurrent per-venue fetch, per-domain timeouts,
last-good fallback, and per-symbol degradation tracking (V1 parity)."""

from __future__ import annotations

import asyncio
import time
from typing import Optional

from lightfee.config.schema import AppConfig
from lightfee.core.domain import Venue
from lightfee.sidecar.pairing import build_same_symbol_pairs
from lightfee.sidecar.publisher import publish_snapshot
from lightfee.sidecar.snapshot import (
    CandidateInput,
    FundingLifecycle,
    LiquidityLifecycle,
    MarketLifecycle,
    QuoteSnapshot,
    SidecarSnapshot,
    TransferLifecycle,
)
from lightfee.sidecar.sources.exchange import ExchangeSource
from lightfee.sidecar.sources.liquidity import LiquiditySource
from lightfee.sidecar.sources.transfer import TransferSource
from lightfee.venues.specs import get_spec

# V1 parity: per-domain timeout defaults (matching V1 sidecar_budget_ms configs)
DEFAULT_FUNDING_TIMEOUT_S = 30.0  # V1 parity: allow cold-cache warm for large-universe venues (OKX has 620 symbols)
DEFAULT_LIQUIDITY_TIMEOUT_S = 10.0
DEFAULT_TRANSFER_TIMEOUT_S = 5.0
DEFAULT_PER_VENUE_TIMEOUT_S = 15.0


class SidecarService:
    """Exchange-native sidecar with V1 parity: per-domain timeouts, last-good
    fallback, and per-symbol degradation.

    Partial venue failure → degraded_venues. Partial symbol failure →
    degraded_symbols. Timeout or error on a domain → inject last-good
    quotes for that venue so candidates are not lost.
    """

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.snapshot_path = config.runtime.sidecar_snapshot_path
        runtime = config.runtime
        self._funding_timeout_s = getattr(runtime, "sidecar_funding_timeout_s", DEFAULT_FUNDING_TIMEOUT_S)
        self._liquidity_timeout_s = getattr(runtime, "sidecar_liquidity_timeout_s", DEFAULT_LIQUIDITY_TIMEOUT_S)
        self._transfer_timeout_s = getattr(runtime, "sidecar_transfer_timeout_s", DEFAULT_TRANSFER_TIMEOUT_S)

        self._exchange_sources: dict[str, ExchangeSource] = {}
        self._liquidity_sources: dict[str, LiquiditySource] = {}
        self._transfer_sources: list[TransferSource] = []
        from lightfee.venues.transport import EndpointRateLimiter

        self._public_rate_limiter = EndpointRateLimiter(1000, 8000, 50)

        for vc in config.venues:
            venue = Venue.from_str(vc.venue)
            spec = get_spec(venue)
            self._exchange_sources[vc.venue] = ExchangeSource(
                spec,
                rate_limiter=self._public_rate_limiter,
            )
            self._liquidity_sources[vc.venue] = LiquiditySource(
                spec,
                rate_limiter=self._public_rate_limiter,
            )

        venue_names = [vc.venue for vc in config.venues]
        for i, from_name in enumerate(venue_names):
            for to_name in venue_names[i + 1:]:
                from_v = Venue.from_str(from_name)
                to_v = Venue.from_str(to_name)
                self._transfer_sources.append(
                    TransferSource.for_venue_pair(
                        from_v,
                        to_v,
                        rate_limiter=self._public_rate_limiter,
                    )
                )

        # V1 parity: last-good fallback cache
        self._last_good_quotes: dict[str, QuoteSnapshot] = {}
        self._last_good_at_ms: int = 0
        self._last_liquidity_publish_at_ms: int = 0
        self._last_liquidity_publish_at_ms_by_key: dict[tuple[str, str, str], int] = {}

    async def close(self) -> None:
        for src in self._exchange_sources.values():
            await src.close()
        for src in self._liquidity_sources.values():
            await src.close()
        for src in self._transfer_sources:
            await src.close()

    # ------------------------------------------------------------------
    # Main refresh
    # ------------------------------------------------------------------

    async def refresh_once(self) -> SidecarSnapshot:
        observed_ms = int(time.time() * 1000)
        had_last_good_before_refresh = bool(self._last_good_quotes)
        symbols = self.config.symbols

        quotes: dict[str, QuoteSnapshot] = {}
        funding_lifecycle: list[FundingLifecycle] = []
        market_lifecycle: list[MarketLifecycle] = []
        liquidity_lifecycle: list[LiquidityLifecycle] = []
        degraded_venues: set[str] = set()
        degraded_symbols: dict[str, list[str]] = {}

        # --- Funding + Market fetch (per venue, funding timeout) ---
        funding_results = await self._fetch_all_venues(
            symbols, timeout_s=self._funding_timeout_s,
        )

        for venue_name, venue_quotes, error, failed_symbols in funding_results:
            if error is not None:
                degraded_venues.add(venue_name)
                # V1 parity: last-good fallback — inject previous quotes
                fallback = self._inject_last_good(venue_name, symbols)
                if fallback:
                    for key, q in fallback.items():
                        if int(getattr(q, "observed_at_ms", 0) or 0) <= 0:
                            q.observed_at_ms = int(self._last_good_at_ms or 0)
                        if not str(getattr(q, "source", "") or ""):
                            q.source = "sidecar_quote"
                        quotes[key] = q
                funding_lifecycle.append(FundingLifecycle(
                    venue=venue_name, observed_at_ms=observed_ms, symbol_count=len(fallback),
                    coverage_usable=len(fallback), degraded_reason=str(error),
                ))
                market_lifecycle.append(MarketLifecycle(
                    venue=venue_name, observed_at_ms=observed_ms, symbol_count=len(fallback),
                    coverage_usable=len(fallback), degraded_reason=str(error),
                ))
                continue

            if failed_symbols:
                degraded_symbols[venue_name] = list(failed_symbols)

            count = len(venue_quotes) if venue_quotes else 0
            usable = count - len(failed_symbols)

            if venue_quotes:
                for key, q in venue_quotes.items():
                    if int(getattr(q, "observed_at_ms", 0) or 0) <= 0:
                        q.observed_at_ms = observed_ms
                    if not str(getattr(q, "source", "") or ""):
                        q.source = "sidecar_quote"
                    quotes[key] = q

            funding_lifecycle.append(FundingLifecycle(
                venue=venue_name, observed_at_ms=observed_ms, symbol_count=count,
                coverage_usable=usable,
                degraded_reason="; ".join(f"{s}: fetch failed" for s in failed_symbols) if failed_symbols else "",
            ))
            market_lifecycle.append(MarketLifecycle(
                venue=venue_name, observed_at_ms=observed_ms, symbol_count=count,
                coverage_usable=usable,
                degraded_reason="; ".join(f"{s}: fetch failed" for s in failed_symbols) if failed_symbols else "",
            ))

        # --- Liquidity fetch (independent domain, own timeout, own source) ---
        liquidity_results = await self._fetch_liquidity_all_venues(
            symbols, timeout_s=self._liquidity_timeout_s,
        )
        for venue_name, liq_data, liq_error, liq_failed_symbols in liquidity_results:
            if liq_error is not None:
                degraded_venues.add(venue_name)
                liquidity_lifecycle.append(LiquidityLifecycle(
                    venue=venue_name, observed_at_ms=observed_ms, symbol_count=0,
                    coverage_usable=0, degraded_reason=str(liq_error),
                ))
                continue

            if liq_failed_symbols:
                existing = degraded_symbols.get(venue_name, [])
                degraded_symbols[venue_name] = sorted(set(existing) | liq_failed_symbols)

            count = len(liq_data) if liq_data else 0
            usable = count - len(liq_failed_symbols)
            liquidity_lifecycle.append(LiquidityLifecycle(
                venue=venue_name, observed_at_ms=observed_ms, symbol_count=count,
                coverage_usable=max(0, usable),
                degraded_reason="; ".join(f"{s}: fetch failed" for s in liq_failed_symbols) if liq_failed_symbols else "",
            ))

        # --- Transfer lifecycle (empty-compatible, independent) ---
        transfer_lifecycle: list[TransferLifecycle] = []
        for ts in self._transfer_sources:
            transfer_lifecycle.append(TransferLifecycle(
                from_venue=ts.from_venue, to_venue=ts.to_venue,
                observed_at_ms=observed_ms, coverage_usable=0, degraded_reason="",
            ))

        last_good_for_acquisition = self._last_good_quotes if had_last_good_before_refresh else {}

        # --- Build candidates ---
        candidates = build_same_symbol_pairs(quotes, symbols)
        published_ms = int(time.time() * 1000)
        legacy_liquidity_publish_ms = int(getattr(self, "_last_liquidity_publish_at_ms", 0) or 0)
        liquidity_publish_by_key = getattr(self, "_last_liquidity_publish_at_ms_by_key", None)
        if not isinstance(liquidity_publish_by_key, dict):
            liquidity_publish_by_key = {}
        use_legacy_liquidity_publish_ms = (
            not liquidity_publish_by_key and legacy_liquidity_publish_ms > 0
        )
        liquidity_successful_publish = False
        for row in liquidity_lifecycle:
            row.domain = "perp_liquidity"
            row.source = "sidecar_perp_liquidity"
            key = (row.domain, row.source, row.venue)
            previous_publish_ms = int(liquidity_publish_by_key.get(key, 0) or 0)
            if previous_publish_ms <= 0 and use_legacy_liquidity_publish_ms:
                previous_publish_ms = legacy_liquidity_publish_ms
            has_usable_publish = int(getattr(row, "coverage_usable", 0) or 0) > 0
            if has_usable_publish:
                row.publish_interval_ms = (
                    max(published_ms - previous_publish_ms, 0)
                    if previous_publish_ms > 0
                    else 0
                )
                row.published_at_ms = published_ms
                liquidity_publish_by_key[key] = published_ms
                liquidity_successful_publish = True
            else:
                row.publish_interval_ms = 0
                row.published_at_ms = previous_publish_ms
        self._last_liquidity_publish_at_ms_by_key = liquidity_publish_by_key
        if liquidity_successful_publish:
            self._last_liquidity_publish_at_ms = published_ms

        # --- Cache last-good quotes ---
        if quotes:
            self._last_good_quotes = dict(quotes)
            self._last_good_at_ms = published_ms

        snapshot = SidecarSnapshot(
            published_at_ms=published_ms,
            market_observed_at_ms=observed_ms,
            funding_lifecycle=funding_lifecycle,
            market_lifecycle=market_lifecycle,
            transfer_lifecycle=transfer_lifecycle,
            liquidity_lifecycle=liquidity_lifecycle,
            degraded_venues=sorted(degraded_venues),
            degraded_domains=[],
            degraded_symbols=degraded_symbols,
            source_mode="direct_market",
            acquisition_mode=_resolve_acquisition_mode(degraded_venues, last_good_for_acquisition),
            quotes=quotes,
            candidates=candidates,
        )

        publish_snapshot(snapshot, self.snapshot_path)
        return snapshot

    # ------------------------------------------------------------------
    # Per-venue concurrent fetch with per-symbol error tracking
    # ------------------------------------------------------------------

    async def _fetch_all_venues(
        self, symbols: list[str], timeout_s: float,
    ) -> list[tuple[str, Optional[dict[str, QuoteSnapshot]], Optional[Exception], set[str]]]:
        """Fetch quotes from all venues concurrently. Returns per-venue results
        with degraded symbol tracking."""

        async def _fetch_one(venue_name: str) -> tuple[str, Optional[dict[str, QuoteSnapshot]], Optional[Exception], set[str]]:
            source = self._exchange_sources.get(venue_name)
            if source is None:
                return (venue_name, None, None, set())
            try:
                result = await asyncio.wait_for(source.fetch_all(symbols), timeout=timeout_s)
                return (venue_name, result, None, set())
            except asyncio.TimeoutError:
                return (venue_name, None, TimeoutError(f"funding timeout {timeout_s}s"), set())
            except Exception as e:
                return (venue_name, None, e, set())

        results = await asyncio.gather(
            *[_fetch_one(vc.venue) for vc in self.config.venues],
            return_exceptions=False,
        )
        return list(results)

    # ------------------------------------------------------------------
    # Per-venue liquidity fetch (independent timeout, independent source)
    # ------------------------------------------------------------------

    async def _fetch_liquidity_all_venues(
        self, symbols: list[str], timeout_s: float,
    ) -> list[tuple[str, Optional[dict], Optional[Exception], set[str]]]:
        """Fetch perp liquidity from all venues concurrently with independent timeout."""

        async def _fetch_one(venue_name: str) -> tuple[str, Optional[dict], Optional[Exception], set[str]]:
            source = self._liquidity_sources.get(venue_name)
            if source is None:
                return (venue_name, None, None, set())
            try:
                result = await asyncio.wait_for(source.fetch_perp_liquidity(symbols), timeout=timeout_s)
                return (venue_name, result, None, set())
            except asyncio.TimeoutError:
                return (venue_name, None, TimeoutError(f"liquidity timeout {timeout_s}s"), set())
            except Exception as e:
                return (venue_name, None, e, set())

        results = await asyncio.gather(
            *[_fetch_one(vc.venue) for vc in self.config.venues],
            return_exceptions=False,
        )
        return list(results)

    # ------------------------------------------------------------------
    # Last-good fallback
    # ------------------------------------------------------------------

    def _inject_last_good(self, venue_name: str, symbols: list[str]) -> dict[str, QuoteSnapshot]:
        """Return last-good quotes for a degraded venue."""
        result: dict[str, QuoteSnapshot] = {}
        if not self._last_good_quotes:
            return result
        for sym in symbols:
            key = f"{venue_name}:{sym.upper()}"
            q = self._last_good_quotes.get(key)
            if q is not None:
                result[key] = q
        return result


def _resolve_acquisition_mode(degraded_venues: set[str], last_good: dict) -> str:
    """Resolve acquisition_mode matching V1 semantics.

    - No degradation → fresh_sidecar
    - Degradation + last-good cache available → last_good_sidecar
    - Degradation + no last-good cache → degraded_sidecar (not fresh!)
    """
    if not degraded_venues:
        return "fresh_sidecar"
    if last_good:
        return "last_good_sidecar"
    return "degraded_sidecar"
