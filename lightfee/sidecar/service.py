"""Sidecar refresh service: concurrent per-venue fetch, per-domain timeouts,
last-good fallback, and per-symbol degradation tracking (V1 parity)."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import logging
import threading
import time
from math import isfinite
from pathlib import Path
from typing import Optional

from lightfee.config.schema import AppConfig, VenueConfig
from lightfee.core.domain import PerpLiquiditySnapshot, Venue
from lightfee.sidecar.pairing import FundingCandidateService
from lightfee.sidecar.publisher import load_snapshot, publish_snapshot
from lightfee.sidecar.snapshot import (
    SNAPSHOT_SCHEMA_VERSION,
    FundingLifecycle,
    LiquidityLifecycle,
    MarketLifecycle,
    QuoteSnapshot,
    SidecarSnapshot,
    TransferLifecycle,
)
from lightfee.spread.quote_snapshot import (
    FULL_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
    SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
    SpreadQuoteSnapshot,
    publish_spread_quote_snapshot,
    spread_metadata_snapshot_path,
    spread_quote_snapshot_path,
)
from lightfee.spread.universe import (
    resolve_spread_sampling_symbols,
    spread_sampling_selection_required,
)
from lightfee.strategy.fee_evidence import effective_fee_maps, load_fee_evidence
from lightfee.sidecar.sources.exchange import ExchangeSource
from lightfee.sidecar.sources.liquidity import LiquiditySource
from lightfee.sidecar.spread_bbo import (
    SpreadBboDataPlane,
    quote_cache_contract_eligible as _quote_cache_contract_eligible,
)
from lightfee.strategy.funding_forecast_calibrator import FundingForecastCalibrator
from lightfee.venues.specs import get_spec

# V1 parity: per-domain timeout defaults (matching V1 sidecar_budget_ms configs)
DEFAULT_FUNDING_TIMEOUT_S = (
    30.0  # V1 parity: allow cold-cache warm for large-universe venues (OKX has 620 symbols)
)
DEFAULT_LIQUIDITY_TIMEOUT_S = 10.0
DEFAULT_PER_VENUE_TIMEOUT_S = 15.0
SIDECAR_PUBLIC_HTTP_MAX_CONNECTIONS = 32

logger = logging.getLogger("lightfee.sidecar.service")


class SidecarService:
    """Exchange-native sidecar with V1 parity: per-domain timeouts, last-good
    fallback, and per-symbol degradation.

    Partial venue failure → degraded_venues. Partial symbol failure →
    degraded_symbols. Timeout or error on a domain → inject last-good
    quotes for that venue so candidates are not lost.
    """

    def __init__(self, config: AppConfig, *, enable_spread_bbo: bool = True) -> None:
        self.config = config
        self.embedded_spread_bbo_enabled = bool(enable_spread_bbo)
        self._venue_configs_by_name = _canonical_venue_configs(config.venues)
        self.snapshot_path = config.runtime.sidecar_snapshot_path
        self._forecast_calibrator = FundingForecastCalibrator(
            Path(self.snapshot_path).with_name(
                f"{Path(self.snapshot_path).name}.funding-forecast-calibration.json"
            ),
            min_samples=config.strategy.funding_forecast_min_samples,
            max_quantile_drift_bps=(
                config.strategy.funding_forecast_stability_max_quantile_drift_bps
            ),
        )
        runtime = config.runtime
        self._funding_timeout_s = runtime.sidecar_funding_timeout_s
        self._liquidity_timeout_s = runtime.sidecar_liquidity_timeout_s
        self._candidate_service = self._new_candidate_service()

        self._exchange_sources: dict[str, ExchangeSource] = {}
        self._spread_bbo_sources: dict[str, ExchangeSource] = {}
        self._liquidity_sources: dict[str, LiquiditySource] = {}
        from lightfee.venues.transport import EndpointRateLimiter

        self._public_rate_limiters: dict[str, EndpointRateLimiter] = {}
        self._spread_bbo_rate_limiters: dict[str, EndpointRateLimiter] = {}

        # V1 parity: last-good fallback cache.  Initialise before reading the
        # prior snapshot so a restart can retain still-valid per-key evidence.
        self._last_good_quotes: dict[str, QuoteSnapshot] = {}
        self._last_good_at_ms: int = 0
        self._last_good_at_ms_by_key: dict[str, int] = {}
        self._last_liquidity_publish_at_ms: int = 0
        self._last_liquidity_publish_at_ms_by_key: dict[tuple[str, str, str], int] = {}

        for venue_name in self._venue_configs_by_name:
            venue = Venue.from_str(venue_name)
            spec = get_spec(venue)
            rate_limiter = EndpointRateLimiter(1000, 8000, 50)
            self._public_rate_limiters[venue_name] = rate_limiter
            self._exchange_sources[venue_name] = ExchangeSource(
                spec,
                rate_limiter=rate_limiter,
                http_max_connections=SIDECAR_PUBLIC_HTTP_MAX_CONNECTIONS,
            )
            # BBO owns a reserved, per-venue public budget.  It still paces
            # requests and honours any 429 it receives, but funding/OI cannot
            # consume its slots or bind it to the main event loop's cooldown
            # runtime.  Four bulk requests/sec is deliberately conservative.
            if self.embedded_spread_bbo_enabled:
                bbo_rate_limiter = EndpointRateLimiter(1000, 8000, 250)
                self._spread_bbo_rate_limiters[venue_name] = bbo_rate_limiter
                self._spread_bbo_sources[venue_name] = ExchangeSource(
                    spec,
                    rate_limiter=bbo_rate_limiter,
                    http_max_connections=SIDECAR_PUBLIC_HTTP_MAX_CONNECTIONS,
                    consume_global_rate_limit_budget=False,
                )
                self._spread_bbo_sources[
                    venue_name
                ].share_contract_metadata_cache_from(
                    self._exchange_sources[venue_name]
                )
            self._liquidity_sources[venue_name] = LiquiditySource(
                spec,
                rate_limiter=rate_limiter,
                http_max_connections=SIDECAR_PUBLIC_HTTP_MAX_CONNECTIONS,
            )

        # A restart must not erase a cadence that the exchange has already
        # demonstrated.  This is a local snapshot read only; it adds no public
        # REST request and leaves unknown schedules unknown.
        try:
            prior_snapshot = load_snapshot(self.snapshot_path)
        except (KeyError, TypeError, ValueError):
            # A malformed or legacy snapshot must not turn a safe restart into
            # an outage.  It merely leaves funding cadence cold/unknown.
            logger.warning("sidecar funding schedule restore skipped: malformed snapshot")
            prior_snapshot = None
        if prior_snapshot is not None:
            self._forecast_calibrator.prime(prior_snapshot.quotes)
            quotes_by_venue: dict[str, list[QuoteSnapshot]] = {}
            for quote in prior_snapshot.quotes.values():
                quotes_by_venue.setdefault(str(quote.venue).lower(), []).append(quote)
            for venue_name, source in self._exchange_sources.items():
                source.prime_funding_schedule(quotes_by_venue.get(str(venue_name).lower(), []))
            restored = _restorable_prior_last_good_quotes(
                prior_snapshot,
                configured_venues=set(self._venue_configs_by_name),
                configured_symbols=_canonical_symbol_set(config.symbols),
                now_ms=int(time.time() * 1000),
                max_age_ms=max(
                    int(runtime.live_scan_last_good_max_age_ms or 0),
                    0,
                ),
            )
            self._last_good_quotes = restored
            self._last_good_at_ms_by_key = {
                key: int(quote.observed_at_ms)
                for key, quote in restored.items()
            }
            self._last_good_at_ms = max(
                self._last_good_at_ms_by_key.values(),
                default=0,
            )

        self._spread_bbo_data_plane = (
            SpreadBboDataPlane(
                config,
                sources=self._spread_bbo_sources,
                metadata_quotes=lambda: self._last_good_quotes,
                metadata_quote_eligible=_quote_cache_contract_eligible,
                snapshot_path=spread_quote_snapshot_path(self.snapshot_path),
                snapshot_schema_version=SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
            )
            if self.embedded_spread_bbo_enabled
            else None
        )

    async def close(self) -> None:
        for group_name, sources in (
            ("exchange", list(getattr(self, "_exchange_sources", {}).values())),
            ("spread_bbo", list(getattr(self, "_spread_bbo_sources", {}).values())),
            ("liquidity", list(getattr(self, "_liquidity_sources", {}).values())),
        ):
            for src in sources:
                try:
                    await src.close()
                except Exception:
                    logger.exception(
                        "sidecar source close failed; continuing resource cleanup",
                        extra={"source_group": group_name},
                    )

    async def run_spread_bbo_data_plane(self, stop_event: asyncio.Event) -> None:
        if self._spread_bbo_data_plane is None:
            raise RuntimeError("embedded spread BBO data plane is disabled")
        if spread_sampling_selection_required(self.config):
            while not stop_event.is_set():
                sampling_symbols = resolve_spread_sampling_symbols(
                    self.config,
                    self._last_good_quotes,
                    quote_eligible=_quote_cache_contract_eligible,
                )
                if sampling_symbols:
                    self._spread_bbo_data_plane.set_sampling_symbols(sampling_symbols)
                    logger.info(
                        "embedded spread BBO sampling universe frozen: "
                        "symbols=%d global_symbols=%d",
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
        thread_stop = threading.Event()
        runner = asyncio.create_task(
            asyncio.to_thread(
                _run_spread_bbo_in_thread,
                self._spread_bbo_data_plane,
                list(self._spread_bbo_sources.values()),
                thread_stop,
            )
        )
        stop_task = asyncio.create_task(stop_event.wait())
        try:
            done, _pending = await asyncio.wait(
                {runner, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if runner in done and not stop_event.is_set():
                await runner
                raise RuntimeError("spread BBO thread exited unexpectedly")
            thread_stop.set()
            await runner
        finally:
            thread_stop.set()
            if not stop_task.done():
                stop_task.cancel()
            await asyncio.gather(stop_task, return_exceptions=True)
            # ``asyncio.to_thread`` cancellation cannot stop the underlying
            # thread.  Keep service shutdown from racing ``close()`` against
            # BBO clients that still belong to the thread's event loop.
            if not runner.done():
                await asyncio.shield(runner)

    # ------------------------------------------------------------------
    # Main refresh
    # ------------------------------------------------------------------

    async def refresh_once(self) -> SidecarSnapshot:
        refresh_started_at_ms = int(time.time() * 1000)
        symbols = list(
            dict.fromkeys(
                str(symbol).strip().upper()
                for symbol in self.config.symbols
                if str(symbol).strip()
            )
        )

        quotes: dict[str, QuoteSnapshot] = {}
        funding_lifecycle: list[FundingLifecycle] = []
        market_lifecycle: list[MarketLifecycle] = []
        liquidity_lifecycle: list[LiquidityLifecycle] = []
        degraded_venues: set[str] = set()
        degraded_symbols: dict[str, list[str]] = {}
        fallback_used_keys: set[str] = set()
        fresh_cacheable_quote_keys: set[str] = set()
        market_quality_failed_symbols: dict[str, set[str]] = {}
        spread_market_degraded_venues: set[str] = set()
        quote_liquidity_by_venue: dict[str, dict[str, PerpLiquiditySnapshot]] = {}
        requested_symbol_count = len(_canonical_symbol_set(symbols))
        listed_symbols_by_venue: dict[str, set[str]] = {}

        # --- Funding + Market fetch (per venue, funding timeout) ---
        funding_results = await self._fetch_all_venues(
            symbols,
            timeout_s=self._funding_timeout_s,
        )

        for venue_name, venue_quotes, error, failed_symbols in funding_results:
            if error is not None:
                degraded_venues.add(venue_name)
                spread_market_degraded_venues.add(venue_name)
                # V1 parity: last-good fallback — inject previous quotes
                raw_fallback = self._inject_last_good(
                    venue_name,
                    symbols,
                    now_ms=refresh_started_at_ms,
                )
                fallback_market_failures = _market_failure_reasons(raw_fallback)
                fallback_crossed_symbols = {
                    symbol
                    for symbol, reason in fallback_market_failures.items()
                    if reason == "crossed BBO"
                }
                fallback_unpublishable_symbols = (
                    set(fallback_market_failures) - fallback_crossed_symbols
                )
                fallback = {
                    key: quote
                    for key, quote in (raw_fallback or {}).items()
                    if _snapshot_item_symbol(key, quote)
                    not in fallback_unpublishable_symbols
                }
                if fallback:
                    fallback_used_keys.update(fallback)
                    for key, q in fallback.items():
                        if int(getattr(q, "observed_at_ms", 0) or 0) <= 0:
                            q.observed_at_ms = int(self._last_good_at_ms or 0)
                        if not str(getattr(q, "source", "") or ""):
                            q.source = "sidecar_quote"
                        quotes[key] = q
                fallback_symbols = _snapshot_map_symbols(fallback)
                fallback_funding_failures = _funding_failure_reasons(fallback)
                fallback_funding_symbols = (
                    fallback_symbols - set(fallback_funding_failures)
                )
                if fallback_market_failures:
                    market_quality_failed_symbols[venue_name] = set(
                        fallback_market_failures
                    )
                missing_fallback_symbols = (
                    _canonical_symbol_set(symbols) - fallback_symbols
                )
                all_degraded_symbols = (
                    missing_fallback_symbols
                    | set(fallback_market_failures)
                    | set(fallback_funding_failures)
                )
                if all_degraded_symbols:
                    degraded_symbols[venue_name] = sorted(all_degraded_symbols)
                fallback_market_reason = "; ".join(
                    [
                        str(error),
                        *(
                            f"{symbol}: fallback unavailable"
                            for symbol in sorted(missing_fallback_symbols)
                        ),
                        *(
                            f"{symbol}: {reason}"
                            for symbol, reason in sorted(
                                fallback_market_failures.items()
                            )
                        ),
                    ]
                )
                fallback_funding_reason = "; ".join(
                    [
                        str(error),
                        *(
                            f"{symbol}: fallback unavailable"
                            for symbol in sorted(missing_fallback_symbols)
                        ),
                        *(
                            f"{symbol}: {reason}"
                            for symbol, reason in sorted(
                                fallback_funding_failures.items()
                            )
                        ),
                        *(
                            f"{symbol}: {reason}"
                            for symbol, reason in sorted(
                                fallback_market_failures.items()
                            )
                            if symbol in fallback_unpublishable_symbols
                            and symbol not in fallback_funding_failures
                        ),
                    ]
                )
                funding_lifecycle.append(
                    FundingLifecycle(
                        venue=venue_name,
                        observed_at_ms=refresh_started_at_ms,
                        symbol_count=requested_symbol_count,
                        coverage_usable=len(fallback_funding_symbols),
                        degraded_reason=fallback_funding_reason,
                    )
                )
                market_lifecycle.append(
                    MarketLifecycle(
                        venue=venue_name,
                        observed_at_ms=refresh_started_at_ms,
                        symbol_count=requested_symbol_count,
                        coverage_usable=len(
                            fallback_symbols - fallback_crossed_symbols
                        ),
                        degraded_reason=fallback_market_reason,
                    )
                )
                continue

            failed_symbols = {str(symbol).upper() for symbol in failed_symbols}
            venue_quotes, identity_failures = _canonicalize_venue_quotes(
                venue_name,
                venue_quotes,
                requested_symbols=_canonical_symbol_set(symbols),
            )
            listed_symbols = _snapshot_map_symbols(venue_quotes)
            listed_symbols_by_venue[venue_name] = set(listed_symbols)
            market_failures = _market_failure_reasons(venue_quotes)
            market_failures = {**identity_failures, **market_failures}
            crossed_symbols = {
                symbol
                for symbol, reason in market_failures.items()
                if reason == "crossed BBO"
            }
            unpublishable_symbols = (
                (set(market_failures) - crossed_symbols) | failed_symbols
            )
            venue_quotes = {
                key: quote
                for key, quote in venue_quotes.items()
                if _snapshot_item_symbol(key, quote) not in unpublishable_symbols
            }
            if market_failures:
                market_quality_failed_symbols[venue_name] = set(market_failures)
            returned_symbols = _snapshot_map_symbols(venue_quotes)
            funding_failures = _funding_failure_reasons(venue_quotes)
            fresh_cacheable_symbols = (
                returned_symbols
                - set(market_failures)
                - set(funding_failures)
                - failed_symbols
            )
            fresh_cacheable_quote_keys.update(
                key
                for key, quote in venue_quotes.items()
                if _snapshot_item_symbol(key, quote) in fresh_cacheable_symbols
                and _quote_cache_contract_eligible(quote)
            )
            all_degraded_symbols = (
                failed_symbols | set(market_failures) | set(funding_failures)
            )
            if all_degraded_symbols:
                degraded_symbols[venue_name] = sorted(all_degraded_symbols)
            funding_usable = len(
                returned_symbols - set(funding_failures) - failed_symbols
            )
            market_usable = len(
                returned_symbols - crossed_symbols - failed_symbols
            )
            funding_reason = (
                "; ".join(
                    [
                        *(f"{symbol}: fetch failed" for symbol in sorted(failed_symbols)),
                        *(
                            f"{symbol}: {reason}"
                            for symbol, reason in sorted(funding_failures.items())
                        ),
                        *(
                            f"{symbol}: {reason}"
                            for symbol, reason in sorted(market_failures.items())
                            if symbol in unpublishable_symbols
                            and symbol not in funding_failures
                        ),
                    ]
                )
                if failed_symbols or funding_failures or unpublishable_symbols
                else ("no usable funding quotes" if funding_usable <= 0 else "")
            )
            market_failures = [
                *(f"{symbol}: fetch failed" for symbol in sorted(failed_symbols)),
                *(
                    f"{symbol}: {reason}"
                    for symbol, reason in sorted(market_failures.items())
                ),
            ]
            market_reason = "; ".join(market_failures) or (
                "no usable market quotes" if market_usable <= 0 else ""
            )
            if funding_usable <= 0 or market_usable <= 0:
                degraded_venues.add(venue_name)
            if market_usable <= 0:
                spread_market_degraded_venues.add(venue_name)

            if venue_quotes:
                derived_liquidity: dict[str, PerpLiquiditySnapshot] = {}
                for key, q in venue_quotes.items():
                    if _snapshot_item_symbol(key, q) in funding_failures:
                        q = _diagnostic_quote_without_funding_truth(q)
                    if int(getattr(q, "observed_at_ms", 0) or 0) <= 0:
                        q.observed_at_ms = refresh_started_at_ms
                    if not str(getattr(q, "source", "") or ""):
                        q.source = "sidecar_quote"
                    quotes[key] = q
                    derived_liquidity[key] = PerpLiquiditySnapshot(
                        venue=Venue.from_str(getattr(q, "venue", venue_name) or venue_name),
                        symbol=getattr(q, "symbol", key.split(":", 1)[-1]),
                        observed_at_ms=int(
                            getattr(q, "observed_at_ms", 0) or refresh_started_at_ms
                        ),
                    )
                quote_liquidity_by_venue[venue_name] = derived_liquidity

            funding_lifecycle.append(
                FundingLifecycle(
                    venue=venue_name,
                    observed_at_ms=refresh_started_at_ms,
                    symbol_count=len(listed_symbols),
                    coverage_usable=funding_usable,
                    degraded_reason=funding_reason,
                )
            )
            market_lifecycle.append(
                MarketLifecycle(
                    venue=venue_name,
                    observed_at_ms=refresh_started_at_ms,
                    symbol_count=len(listed_symbols),
                    coverage_usable=market_usable,
                    degraded_reason=market_reason,
                )
            )

        # Publish the spread-only market view before liquidity enrichment,
        # funding candidate construction, and the much larger V4 snapshot.
        # The view contains no private data and does not alter the V1/live
        # opportunity-input contract consumed by the trading runtime.
        # Capture the fast-path publication time as soon as the concurrent
        # market fetch completes. The later candidate-build watermark remains
        # separate because liquidity evidence is fetched after this point.
        spread_quote_published_at_ms = int(time.time() * 1000)
        spread_quotes = {
            key: quote
            for key, quote in quotes.items()
            if 0 < int(getattr(quote, "observed_at_ms", 0) or 0)
            <= spread_quote_published_at_ms
        }
        spread_future_symbols: dict[str, set[str]] = {}
        for key, quote in quotes.items():
            if key in spread_quotes:
                continue
            venue = str(getattr(quote, "venue", "") or "").strip().lower()
            symbol = _snapshot_item_symbol(key, quote)
            if venue and symbol:
                spread_future_symbols.setdefault(venue, set()).add(symbol)
        spread_degraded_symbols = {
            venue: sorted(symbols)
            for venue, symbols in market_quality_failed_symbols.items()
            if symbols
        }
        for venue, symbols in spread_future_symbols.items():
            spread_degraded_symbols[venue] = sorted(
                set(spread_degraded_symbols.get(venue, [])) | symbols
            )
        data_plane = getattr(self, "_spread_bbo_data_plane", None)
        if bool(getattr(self, "embedded_spread_bbo_enabled", True)) and not bool(
            getattr(data_plane, "active", False)
        ):
            try:
                publish_spread_quote_snapshot(
                    SpreadQuoteSnapshot(
                        schema_version=FULL_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
                        published_at_ms=spread_quote_published_at_ms,
                        market_observed_at_ms=_latest_valid_quote_observation_ms(
                            spread_quotes,
                            decision_at_ms=spread_quote_published_at_ms,
                            fallback_ms=refresh_started_at_ms,
                        ),
                        batch_started_at_ms=refresh_started_at_ms,
                        configured_venues=sorted(self._configured_venue_names()),
                        degraded_venues=sorted(spread_market_degraded_venues),
                        degraded_symbols=spread_degraded_symbols,
                        quotes=spread_quotes,
                    ),
                    spread_quote_snapshot_path(self.snapshot_path),
                )
            except (OSError, TypeError, ValueError):
                # One-shot/recovery compatibility only.  The normal service
                # gives compact publication ownership to the BBO data plane.
                logger.exception("spread quote compatibility publication failed")

        # --- Liquidity fetch (independent domain, own timeout, own source) ---
        liquidity_results = await self._fetch_liquidity_all_venues(
            symbols,
            timeout_s=self._liquidity_timeout_s,
            quote_liquidity_by_venue=quote_liquidity_by_venue,
            skip_venues=degraded_venues,
        )
        for venue_name, liq_data, liq_error, liq_failed_symbols in liquidity_results:
            if liq_error is not None:
                degraded_venues.add(venue_name)
                liquidity_lifecycle.append(
                    LiquidityLifecycle(
                        venue=venue_name,
                        observed_at_ms=refresh_started_at_ms,
                        symbol_count=len(
                            listed_symbols_by_venue.get(
                                venue_name,
                                _canonical_symbol_set(symbols),
                            )
                        ),
                        coverage_usable=0,
                        degraded_reason=str(liq_error),
                    )
                )
                continue

            liq_failed_symbols = {
                str(symbol).upper() for symbol in liq_failed_symbols
            } | market_quality_failed_symbols.get(venue_name, set())
            if liq_failed_symbols:
                existing = degraded_symbols.get(venue_name, [])
                degraded_symbols[venue_name] = sorted(set(existing) | liq_failed_symbols)

            returned_liquidity_symbols = _snapshot_map_symbols(liq_data)
            usable = len(returned_liquidity_symbols - liq_failed_symbols)
            coverage_reason = (
                "; ".join(f"{s}: fetch failed" for s in liq_failed_symbols)
                if liq_failed_symbols
                else ("no usable perp liquidity evidence" if usable <= 0 else "")
            )
            if usable <= 0:
                degraded_venues.add(venue_name)
            liquidity_lifecycle.append(
                LiquidityLifecycle(
                    venue=venue_name,
                    observed_at_ms=refresh_started_at_ms,
                    symbol_count=len(
                        returned_liquidity_symbols | liq_failed_symbols
                        | _snapshot_map_symbols(
                            quote_liquidity_by_venue.get(venue_name, {})
                        )
                    ),
                    coverage_usable=max(0, usable),
                    degraded_reason=coverage_reason,
                )
            )

        # Transfer/inventory preference is live-admission evidence, not a
        # public-sidecar resource.  No synthetic pairwise clients or inferred
        # balances exist on this path.
        transfer_lifecycle: list[TransferLifecycle] = []

        # This consumes only the already-fetched public payload: no extra REST
        # requests, and no sample is created unless the exchange advanced its
        # next settlement while exposing a confirmed previous settled rate.
        candidate_build_observed_at_ms = int(time.time() * 1000)
        future_quote_symbols_by_venue: dict[str, set[str]] = {}
        future_market_counted_by_venue: dict[str, set[str]] = {}
        future_funding_counted_by_venue: dict[str, set[str]] = {}
        future_liquidity_counted_by_venue: dict[str, set[str]] = {}
        future_quote_keys = {
            key
            for key, quote in quotes.items()
            if int(getattr(quote, "observed_at_ms", 0) or 0)
            > candidate_build_observed_at_ms
        }
        for key in sorted(future_quote_keys):
            quote = quotes.pop(key)
            venue = str(getattr(quote, "venue", "") or "").strip().lower()
            symbol = _snapshot_item_symbol(key, quote)
            if not venue or not symbol:
                continue
            future_quote_symbols_by_venue.setdefault(venue, set()).add(symbol)
            if not _market_failure_reasons({key: quote}):
                future_market_counted_by_venue.setdefault(venue, set()).add(symbol)
                future_liquidity_counted_by_venue.setdefault(venue, set()).add(symbol)
            if not _funding_failure_reasons({key: quote}):
                future_funding_counted_by_venue.setdefault(venue, set()).add(symbol)
            fresh_cacheable_quote_keys.discard(key)
            fallback_used_keys.discard(key)

        if future_quote_symbols_by_venue:
            for venue, future_symbols in future_quote_symbols_by_venue.items():
                degraded_venues.add(venue)
                degraded_symbols[venue] = sorted(
                    set(degraded_symbols.get(venue, [])) | future_symbols
                )
            _apply_future_quote_degradation(
                funding_lifecycle,
                future_quote_symbols_by_venue,
                future_funding_counted_by_venue,
            )
            _apply_future_quote_degradation(
                market_lifecycle,
                future_quote_symbols_by_venue,
                future_market_counted_by_venue,
            )
            _apply_future_quote_degradation(
                liquidity_lifecycle,
                future_quote_symbols_by_venue,
                future_liquidity_counted_by_venue,
            )

        self._ensure_forecast_calibrator().apply(
            quotes,
            now_ms=candidate_build_observed_at_ms,
        )

        # --- Build candidates ---
        # Fee schedules may be refreshed independently of market data.  Build
        # a new immutable candidate service for this snapshot so a verified
        # account tier is never silently retained past its evidence TTL.
        self._candidate_service = self._new_candidate_service(now_ms=candidate_build_observed_at_ms)
        candidate_build_diagnostics: dict[str, object] = {}
        candidates = self._ensure_candidate_service().build(
            quotes,
            symbols,
            observed_at_ms=candidate_build_observed_at_ms,
            diagnostics=candidate_build_diagnostics,
        )
        candidate_build_diagnostics["quarantined_future_quote_count"] = len(
            future_quote_keys
        )
        candidate_build_diagnostics["quarantined_future_quote_keys"] = sorted(
            future_quote_keys
        )
        candidate_build_diagnostics["requested_venues"] = sorted(
            self._configured_venue_names()
        )
        # Publication retains its established completion-time semantics.  It
        # must not be overloaded as the earlier candidate decision watermark.
        published_ms = max(
            int(time.time() * 1000),
            candidate_build_observed_at_ms,
        )
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
                    max(published_ms - previous_publish_ms, 0) if previous_publish_ms > 0 else 0
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

        self._attach_local_l2_depth_bridge(quotes, candidate_build_observed_at_ms)

        # --- Cache last-good quotes ---
        self._update_last_good_quote_cache(
            quotes,
            fresh_cacheable_quote_keys,
            published_at_ms=published_ms,
        )
        # Both dedicated and embedded BBO modes publish hot-only v5 rows. The
        # consumer therefore always needs the same full v3 metadata handoff;
        # omitting it in embedded mode would turn every valid BBO into
        # metadata_unavailable and create a permanent zero-sampling service.
        try:
            metadata_quotes = dict(self._last_good_quotes)
            if metadata_quotes:
                publish_spread_quote_snapshot(
                    SpreadQuoteSnapshot(
                        schema_version=FULL_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
                        published_at_ms=published_ms,
                        market_observed_at_ms=_latest_valid_quote_observation_ms(
                            metadata_quotes,
                            decision_at_ms=published_ms,
                            fallback_ms=refresh_started_at_ms,
                        ),
                        batch_started_at_ms=refresh_started_at_ms,
                        configured_venues=sorted(self._configured_venue_names()),
                        degraded_venues=sorted(degraded_venues),
                        degraded_symbols={
                            venue: list(symbols)
                            for venue, symbols in degraded_symbols.items()
                            if symbols
                        },
                        quotes=metadata_quotes,
                    ),
                    spread_metadata_snapshot_path(self.snapshot_path),
                    validate_contract=False,
                )
        except (OSError, TypeError, ValueError):
            logger.exception("spread metadata handoff publication failed")

        snapshot = SidecarSnapshot(
            published_at_ms=published_ms,
            market_observed_at_ms=_latest_valid_quote_observation_ms(
                quotes,
                decision_at_ms=candidate_build_observed_at_ms,
                fallback_ms=refresh_started_at_ms,
            ),
            funding_lifecycle=funding_lifecycle,
            market_lifecycle=market_lifecycle,
            transfer_lifecycle=transfer_lifecycle,
            liquidity_lifecycle=liquidity_lifecycle,
            degraded_venues=sorted(degraded_venues),
            degraded_domains=[],
            degraded_symbols=degraded_symbols,
            source_mode="direct_market",
            acquisition_mode=_resolve_acquisition_mode(
                degraded_venues,
                degraded_symbols,
                fallback_used_keys,
                has_usable_payload=bool(quotes),
            ),
            candidate_build_observed_at_ms=candidate_build_observed_at_ms,
            candidate_build_diagnostics=candidate_build_diagnostics,
            quotes=quotes,
            candidates=candidates,
        )

        publish_snapshot(snapshot, self.snapshot_path)
        return snapshot

    def _update_last_good_quote_cache(
        self,
        quotes: dict[str, QuoteSnapshot],
        fresh_keys: set[str],
        *,
        published_at_ms: int,
    ) -> None:
        """Update per-key last-good truth without evicting unrelated venues.

        Fallback and quarantined quotes are excluded by the caller, so a
        partial refresh cannot refresh stale epochs or erase another venue's
        last contract-valid record.
        """
        current_cache = getattr(self, "_last_good_quotes", None)
        cache = dict(current_cache) if isinstance(current_cache, dict) else {}
        current_epochs = getattr(self, "_last_good_at_ms_by_key", None)
        epochs = dict(current_epochs) if isinstance(current_epochs, dict) else {}
        updated = False
        for key in sorted(fresh_keys):
            quote = quotes.get(key)
            if quote is None:
                continue
            cache[key] = replace(quote)
            epochs[key] = int(published_at_ms)
            updated = True
        if updated:
            # Atomic reference replacement gives the independent BBO thread a
            # stable metadata generation without a cross-thread async lock.
            self._last_good_quotes = cache
            self._last_good_at_ms_by_key = epochs
            self._last_good_at_ms = max(
                int(getattr(self, "_last_good_at_ms", 0) or 0),
                int(published_at_ms),
            )

    def _attach_local_l2_depth_bridge(
        self,
        quotes: dict[str, QuoteSnapshot],
        observed_ms: int,
    ) -> None:
        """Merge only fresh Local-L2 evidence without another public request."""
        runtime = self.config.runtime
        if not runtime.local_l2_depth_bridge_enabled or not quotes:
            return
        from lightfee.marketdata.l2_depth_bridge import (
            attach_local_l2_depth,
            load_local_l2_depth_bridge,
        )

        bridge = load_local_l2_depth_bridge(
            runtime.local_l2_depth_bridge_path,
            now_ms=observed_ms,
            # Spread paper already requires fresh market quotes.  Keeping the
            # bridge no older than that domain prevents an otherwise current
            # sidecar refresh from carrying an old executable ladder.
            max_age_ms=runtime.max_market_age_ms,
        )
        # The bridge book must be contemporaneous with the BBO snapshot.  A
        # matching top price alone cannot prove that its lower levels remain
        # executable, so use the same cross-venue skew budget as the spread
        # signal.  Rejected depth simply falls back to BBO-only capacity.
        attach_local_l2_depth(
            quotes,
            bridge,
            max_quote_skew_ms=self.config.strategy.spread_quote_skew_ms,
        )

    # ------------------------------------------------------------------
    # Per-venue concurrent fetch with per-symbol error tracking
    # ------------------------------------------------------------------

    async def _fetch_all_venues(
        self,
        symbols: list[str],
        timeout_s: float,
    ) -> list[tuple[str, Optional[dict[str, QuoteSnapshot]], Optional[Exception], set[str]]]:
        """Fetch quotes from all venues concurrently. Returns per-venue results
        with degraded symbol tracking."""

        async def _fetch_one(
            venue_name: str,
        ) -> tuple[str, Optional[dict[str, QuoteSnapshot]], Optional[Exception], set[str]]:
            source = self._exchange_sources.get(venue_name)
            if source is None:
                requested = _canonical_symbol_set(symbols)
                return (venue_name, None, None, requested)
            try:
                result = await asyncio.wait_for(source.fetch_all(symbols), timeout=timeout_s)
                requested = _canonical_symbol_set(symbols)
                result = {
                    key: quote
                    for key, quote in (result or {}).items()
                    if _snapshot_item_symbol(key, quote) in requested
                }
                # Public bulk endpoints return the venue's listed universe.
                # The caller supplies the cross-venue union, so an omitted row
                # normally means "not listed here", not a per-symbol fetch
                # failure.  Transport failures are already represented by the
                # exception branch; malformed returned rows are attributed by
                # the canonicalisation and market/funding validators.
                return (venue_name, result, None, set())
            except asyncio.TimeoutError:
                return (venue_name, None, TimeoutError(f"funding timeout {timeout_s}s"), set())
            except Exception as e:
                return (venue_name, None, e, set())

        results = await asyncio.gather(
            *[_fetch_one(venue_name) for venue_name in self._configured_venue_names()],
            return_exceptions=False,
        )
        return list(results)

    def _configured_venue_names(self) -> list[str]:
        """Return the canonical, stable, unique operational venue scope."""
        configured = getattr(self, "_venue_configs_by_name", None)
        if not isinstance(configured, dict):
            configured = _canonical_venue_configs(self.config.venues)
            self._venue_configs_by_name = configured
        return list(configured)

    def _ensure_forecast_calibrator(self) -> FundingForecastCalibrator:
        """Keep direct test/recovery construction compatible with the service."""
        calibrator = getattr(self, "_forecast_calibrator", None)
        if calibrator is None:
            snapshot_path = Path(self.snapshot_path)
            strategy = self.config.strategy
            calibrator = FundingForecastCalibrator(
                snapshot_path.with_name(f"{snapshot_path.name}.funding-forecast-calibration.json"),
                min_samples=strategy.funding_forecast_min_samples,
                max_quantile_drift_bps=(strategy.funding_forecast_stability_max_quantile_drift_bps),
            )
            self._forecast_calibrator = calibrator
        return calibrator

    def _new_candidate_service(
        self,
        *,
        now_ms: int | None = None,
    ) -> FundingCandidateService:
        """Build the configuration-derived shortlist service once per runtime."""
        config = self.config
        venue_configs = getattr(self, "_venue_configs_by_name", None)
        if not isinstance(venue_configs, dict):
            venue_configs = _canonical_venue_configs(config.venues)
            self._venue_configs_by_name = venue_configs
        evidence = load_fee_evidence(
            getattr(
                config.runtime,
                "funding_fee_evidence_path",
                config.runtime.fee_evidence_path,
            ),
            now_ms=int(now_ms if now_ms is not None else time.time() * 1000),
            max_age_ms=int(
                getattr(
                    config.runtime,
                    "funding_fee_evidence_max_age_ms",
                    config.runtime.fee_evidence_max_age_ms,
                )
            ),
        )
        configured_taker = {
            venue_name: float(venue.taker_fee_bps or 0.0)
            for venue_name, venue in venue_configs.items()
        }
        configured_maker = {
            venue_name: float(
                venue.maker_fee_bps
                if venue.maker_fee_bps is not None
                else venue.taker_fee_bps or 0.0
            )
            for venue_name, venue in venue_configs.items()
        }
        taker_fees, maker_fees = effective_fee_maps(
            configured_taker,
            configured_maker,
            evidence,
            allow_verified_maker_rebates=True,
        )
        return FundingCandidateService(
            strategy=config.strategy,
            venue_fee_bps=taker_fees,
            venue_maker_fee_bps=maker_fees,
            venue_notional_caps={
                venue_name: float(venue.max_notional or 0.0)
                for venue_name, venue in venue_configs.items()
            },
            passive_execution_enabled=(str(config.runtime.mode or "").lower() == "live"),
            fee_evidence=evidence,
            expected_fee_identity_hashes=dict(
                config.runtime.fee_evidence_account_identity_hashes
            ),
        )

    def _ensure_candidate_service(self) -> FundingCandidateService:
        """Retain direct test and recovery construction compatibility.

        Normal startup eagerly creates the service to reuse immutable fee and
        sizing context.  V1 lifecycle tests and recovery tooling may construct
        this class directly without ``__init__``; their first refresh must use
        the same service, rather than a simplified candidate path.
        """
        candidate_service = getattr(self, "_candidate_service", None)
        if candidate_service is None:
            candidate_service = self._new_candidate_service()
            self._candidate_service = candidate_service
        return candidate_service

    # ------------------------------------------------------------------
    # Per-venue liquidity fetch (independent timeout, independent source)
    # ------------------------------------------------------------------

    async def _fetch_liquidity_all_venues(
        self,
        symbols: list[str],
        timeout_s: float,
        quote_liquidity_by_venue: Optional[dict[str, dict[str, PerpLiquiditySnapshot]]] = None,
        skip_venues: Optional[set[str]] = None,
    ) -> list[tuple[str, Optional[dict], Optional[Exception], set[str]]]:
        """Fetch perp liquidity from all venues concurrently with independent timeout."""
        quote_liquidity_by_venue = quote_liquidity_by_venue or {}
        skip_venues = {str(venue).lower() for venue in (skip_venues or set())}

        async def _fetch_one(
            venue_name: str,
        ) -> tuple[str, Optional[dict], Optional[Exception], set[str]]:
            if venue_name.lower() in skip_venues:
                return (
                    venue_name,
                    None,
                    RuntimeError("liquidity skipped after market data degradation"),
                    set(),
                )
            derived = quote_liquidity_by_venue.get(venue_name)
            if derived:
                return (venue_name, derived, None, set())
            source = self._liquidity_sources.get(venue_name)
            if source is None:
                requested = _canonical_symbol_set(symbols)
                return (venue_name, None, None, requested)
            try:
                result = await asyncio.wait_for(
                    source.fetch_perp_liquidity(symbols), timeout=timeout_s
                )
                requested = _canonical_symbol_set(symbols)
                result = {
                    key: value
                    for key, value in (result or {}).items()
                    if _snapshot_item_symbol(key, value) in requested
                }
                return (
                    venue_name,
                    result,
                    None,
                    set(),
                )
            except asyncio.TimeoutError:
                return (venue_name, None, TimeoutError(f"liquidity timeout {timeout_s}s"), set())
            except Exception as e:
                return (venue_name, None, e, set())

        results = await asyncio.gather(
            *[_fetch_one(venue_name) for venue_name in self._configured_venue_names()],
            return_exceptions=False,
        )
        return list(results)

    # ------------------------------------------------------------------
    # Last-good fallback
    # ------------------------------------------------------------------

    def _inject_last_good(
        self,
        venue_name: str,
        symbols: list[str],
        *,
        now_ms: int | None = None,
    ) -> dict[str, QuoteSnapshot]:
        """Return only still-valid per-key last-good quotes for one venue."""
        result: dict[str, QuoteSnapshot] = {}
        if not self._last_good_quotes:
            return result
        decision_at_ms = int(
            now_ms if now_ms is not None else time.time() * 1000
        )
        runtime_config = getattr(getattr(self, "config", None), "runtime", None)
        max_age_ms = max(
            int(
                getattr(
                    runtime_config,
                    "live_scan_last_good_max_age_ms",
                    600_000,
                )
                or 0
            ),
            0,
        )
        epochs = getattr(self, "_last_good_at_ms_by_key", None)
        if not isinstance(epochs, dict):
            epochs = {}
        for sym in symbols:
            key = f"{str(venue_name).strip().lower()}:{str(sym).strip().upper()}"
            q = self._last_good_quotes.get(key)
            if q is None:
                continue
            epoch_ms = int(
                epochs.get(
                    key,
                    int(getattr(q, "observed_at_ms", 0) or 0)
                    or int(getattr(self, "_last_good_at_ms", 0) or 0),
                )
                or 0
            )
            observed_at_ms = int(getattr(q, "observed_at_ms", 0) or 0)
            evidence_at_ms = (
                min(epoch_ms, observed_at_ms)
                if epoch_ms > 0 and observed_at_ms > 0
                else max(epoch_ms, observed_at_ms)
            )
            age_ms = decision_at_ms - evidence_at_ms
            if evidence_at_ms <= 0 or age_ms < 0 or age_ms > max_age_ms:
                continue
            result[key] = replace(q)
        return result


def _run_spread_bbo_in_thread(
    data_plane: SpreadBboDataPlane,
    sources: list[ExchangeSource],
    thread_stop: threading.Event,
) -> None:
    """Own the BBO event loop and HTTP clients outside the heavy sidecar loop."""

    async def run() -> None:
        local_stop = asyncio.Event()

        async def bridge_stop() -> None:
            while not thread_stop.is_set():
                await asyncio.sleep(0.05)
            local_stop.set()

        bridge_task = asyncio.create_task(bridge_stop())
        try:
            await data_plane.run(local_stop)
        finally:
            bridge_task.cancel()
            await asyncio.gather(bridge_task, return_exceptions=True)
            for source in sources:
                try:
                    await source.close()
                except Exception:
                    logger.exception("spread BBO thread source close failed")

    asyncio.run(run())


def _resolve_acquisition_mode(
    degraded_venues: set[str],
    degraded_symbols: dict[str, list[str]],
    fallback_used_keys: set[str],
    *,
    has_usable_payload: bool = True,
) -> str:
    """Resolve acquisition_mode matching V1 semantics.

    - No degradation → fresh_sidecar
    - No usable quote payload → unavailable
    - Degradation + fallback records actually injected → last_good_sidecar
    - Degradation without an injected fallback → degraded_sidecar (not fresh!)
    """
    if not has_usable_payload:
        return "unavailable"
    if not degraded_venues and not any(degraded_symbols.values()):
        return "fresh_sidecar"
    if fallback_used_keys:
        return "last_good_sidecar"
    return "degraded_sidecar"


def _canonical_symbol_set(symbols: list[str]) -> set[str]:
    return {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}


def _restorable_prior_last_good_quotes(
    snapshot: SidecarSnapshot,
    *,
    configured_venues: set[str],
    configured_symbols: set[str],
    now_ms: int,
    max_age_ms: int,
) -> dict[str, QuoteSnapshot]:
    """Restore only fresh direct-market evidence across a process restart."""
    if (
        snapshot.schema_version != SNAPSHOT_SCHEMA_VERSION
        or snapshot.acquisition_mode != "fresh_sidecar"
        or snapshot.source_mode != "direct_market"
        or snapshot.degraded_venues
        or snapshot.degraded_domains
        or any(snapshot.degraded_symbols.values())
    ):
        return {}
    restored: dict[str, QuoteSnapshot] = {}
    for quote in snapshot.quotes.values():
        venue = str(quote.venue or "").strip().lower()
        symbol = str(quote.symbol or "").strip().upper()
        key = f"{venue}:{symbol}"
        observed_at_ms = int(quote.observed_at_ms or 0)
        age_ms = int(now_ms) - observed_at_ms
        if (
            venue not in configured_venues
            or symbol not in configured_symbols
            or observed_at_ms <= 0
            or age_ms < 0
            or age_ms > max_age_ms
            or not _quote_cache_contract_eligible(quote)
            or _market_failure_reasons({key: quote})
            or _funding_failure_reasons({key: quote})
        ):
            continue
        restored[key] = replace(quote)
    return restored


def _apply_future_quote_degradation(
    lifecycle_rows: list[FundingLifecycle | MarketLifecycle | LiquidityLifecycle],
    future_symbols_by_venue: dict[str, set[str]],
    counted_symbols_by_venue: dict[str, set[str]],
) -> None:
    """Reconcile lifecycle proof after future-dated quote quarantine."""
    for row in lifecycle_rows:
        venue = str(row.venue).strip().lower()
        future_symbols = future_symbols_by_venue.get(venue, set())
        if not future_symbols:
            continue
        counted_symbols = counted_symbols_by_venue.get(venue, set())
        row.coverage_usable = max(
            0,
            int(row.coverage_usable) - len(future_symbols & counted_symbols),
        )
        evidence = "; ".join(
            f"{symbol}: observed_at_ms_after_candidate_build"
            for symbol in sorted(future_symbols)
        )
        row.degraded_reason = "; ".join(
            part for part in (str(row.degraded_reason or ""), evidence) if part
        )


def _canonical_venue_configs(venues: list[VenueConfig]) -> dict[str, VenueConfig]:
    """Collapse case/whitespace aliases without duplicating venue side effects.

    The first declaration is authoritative.  Fetching, lifecycle accounting,
    and fee/sizing inputs therefore share one deterministic venue identity.
    """
    canonical: dict[str, VenueConfig] = {}
    for venue in venues:
        venue_name = str(venue.venue).strip().lower()
        if venue_name and venue_name not in canonical:
            canonical[venue_name] = venue
    return canonical


def _snapshot_item_symbol(key: object, value: object) -> str:
    raw = getattr(value, "symbol", "") or str(key).split(":", 1)[-1]
    return str(raw).strip().upper()


def _snapshot_map_symbols(values: object) -> set[str]:
    if not isinstance(values, dict):
        return set()
    return {
        symbol
        for key, value in values.items()
        if (symbol := _snapshot_item_symbol(key, value))
    }


def _canonicalize_venue_quotes(
    venue_name: str,
    quotes: dict[str, QuoteSnapshot] | None,
    *,
    requested_symbols: set[str],
) -> tuple[dict[str, QuoteSnapshot], dict[str, str]]:
    """Canonicalize source-owned identities and quarantine local corruption.

    Identity errors are per-symbol data-quality failures.  They must not reach
    candidate construction or turn a single bad provider record into a failed
    atomic publication of every healthy venue.
    """
    expected_venue = str(venue_name).strip().lower()
    accepted: dict[str, QuoteSnapshot] = {}
    identities_seen: set[tuple[str, str]] = set()
    duplicate_identities: set[tuple[str, str]] = set()
    failures: dict[str, str] = {}

    for raw_key, quote in (quotes or {}).items():
        quote_venue = str(getattr(quote, "venue", "") or "").strip().lower()
        quote_symbol = str(getattr(quote, "symbol", "") or "").strip().upper()
        key_text = str(raw_key).strip()
        key_venue, separator, key_symbol = key_text.partition(":")
        key_venue = key_venue.strip().lower() if separator else expected_venue
        key_symbol = (key_symbol if separator else key_text).strip().upper()
        attributable_symbol = quote_symbol or key_symbol

        reasons: list[str] = []
        if not quote_symbol or quote_symbol not in requested_symbols:
            reasons.append("quote_symbol_out_of_scope")
        if quote_venue != expected_venue:
            reasons.append("quote_source_venue_mismatch")
        if key_venue != expected_venue or key_symbol != quote_symbol:
            reasons.append("quote_key_identity_mismatch")
        if reasons:
            if attributable_symbol in requested_symbols:
                failures[attributable_symbol] = ",".join(reasons)
            continue

        identity = (quote_venue, quote_symbol)
        if identity in identities_seen:
            duplicate_identities.add(identity)
            continue
        identities_seen.add(identity)
        accepted[f"{quote_venue}:{quote_symbol}"] = replace(
            quote,
            venue=quote_venue,
            symbol=quote_symbol,
        )

    for identity in duplicate_identities:
        accepted.pop(f"{identity[0]}:{identity[1]}", None)
        failures[identity[1]] = "duplicate_quote_identity"
    return accepted, failures


def _market_failure_reasons(
    quotes: dict[str, QuoteSnapshot] | None,
) -> dict[str, str]:
    """Return per-symbol BBO failures; only crossed quotes remain publishable."""
    failures: dict[str, str] = {}
    for key, quote in (quotes or {}).items():
        symbol = _snapshot_item_symbol(key, quote)
        bid_raw = getattr(quote, "bid", None)
        ask_raw = getattr(quote, "ask", None)
        try:
            bid = float(bid_raw)
        except (TypeError, ValueError, OverflowError):
            bid = float("nan")
        try:
            ask = float(ask_raw)
        except (TypeError, ValueError, OverflowError):
            ask = float("nan")
        reasons: list[str] = []
        observed_at_ms = getattr(quote, "observed_at_ms", None)
        if (
            isinstance(observed_at_ms, bool)
            or not isinstance(observed_at_ms, int)
            or observed_at_ms <= 0
        ):
            reasons.append("observed_at_ms_invalid")
        if isinstance(bid_raw, bool) or not isfinite(bid):
            reasons.append("bid_invalid")
        elif bid <= 0:
            reasons.append("bid_nonpositive")
        if isinstance(ask_raw, bool) or not isfinite(ask):
            reasons.append("ask_invalid")
        elif ask <= 0:
            reasons.append("ask_nonpositive")
        if not reasons and bid > ask:
            reasons.append("crossed BBO")
        if reasons:
            failures[symbol] = ",".join(reasons)
    return failures


def _crossed_quote_symbols(
    quotes: dict[str, QuoteSnapshot] | None,
) -> set[str]:
    """Compatibility helper for callers interested only in crossed BBOs."""
    return {
        symbol
        for symbol, reason in _market_failure_reasons(quotes).items()
        if reason == "crossed BBO"
    }


def _funding_failure_reasons(
    quotes: dict[str, QuoteSnapshot] | None,
) -> dict[str, str]:
    """Return per-symbol proof gaps that make funding data unusable."""
    failures: dict[str, str] = {}
    for key, quote in (quotes or {}).items():
        reasons: list[str] = []
        rate = getattr(quote, "funding_rate_bps", None)
        try:
            numeric_rate = float(rate)
        except (TypeError, ValueError, OverflowError):
            numeric_rate = float("nan")
        if (
            isinstance(rate, bool)
            or not isinstance(rate, (int, float))
            or not isfinite(numeric_rate)
        ):
            reasons.append("funding_rate_invalid")
        for field in ("predicted_funding_rate_bps", "settled_funding_rate_bps"):
            value = getattr(quote, field, None)
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                reasons.append(f"{field}_invalid")
                continue
            if not isfinite(float(value)):
                reasons.append(f"{field}_invalid")
        for field in ("funding_timestamp_ms", "funding_interval_ms"):
            value = getattr(quote, field, None)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                reasons.append(f"{field}_invalid")
        if reasons:
            failures[_snapshot_item_symbol(key, quote)] = ",".join(reasons)
    return failures


def _diagnostic_quote_without_funding_truth(quote: QuoteSnapshot) -> QuoteSnapshot:
    """Copy a BBO quote while revoking malformed funding/contract evidence."""
    return replace(
        quote,
        funding_rate_bps=0.0,
        funding_timestamp_ms=0,
        funding_interval_ms=0,
        predicted_funding_rate_bps=None,
        funding_forecast_source="invalid_funding_quarantined",
        funding_forecast_sample_count=0,
        funding_forecast_uncertainty_bps=0.0,
        funding_forecast_started_at_ms=0,
        funding_forecast_distribution_stable=False,
        funding_forecast_stability_reason="invalid_funding_quarantined",
        funding_forecast_median_drift_bps=0.0,
        funding_forecast_p90_drift_bps=0.0,
        settled_funding_rate_bps=None,
        contract_normalization_complete=False,
    )


def _latest_valid_quote_observation_ms(
    quotes: dict[str, QuoteSnapshot],
    *,
    decision_at_ms: int,
    fallback_ms: int,
) -> int:
    observations = [int(getattr(quote, "observed_at_ms", 0) or 0) for quote in quotes.values()]
    valid = [value for value in observations if 0 < value <= decision_at_ms]
    return max(valid, default=max(int(fallback_ms or 0), 0))
