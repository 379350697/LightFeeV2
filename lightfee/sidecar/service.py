"""Sidecar refresh service: concurrent per-venue fetch, per-domain timeouts,
last-good fallback, and per-symbol degradation tracking (V1 parity)."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import logging
import time
from math import ceil, isfinite
from pathlib import Path
from typing import Mapping, Optional

from lightfee.config.schema import AppConfig, VenueConfig
from lightfee.core.domain import Venue
from lightfee.sidecar.pairing import FundingCandidateService
from lightfee.sidecar.publisher import (
    load_snapshot,
    publish_snapshot,
)
from lightfee.sidecar.snapshot import (
    SNAPSHOT_SCHEMA_VERSION,
    FundingLifecycle,
    LiquidityLifecycle,
    MarketLifecycle,
    QuoteSnapshot,
    SidecarSnapshot,
    TransferLifecycle,
    entry_targeted_oi_revalidation_required,
    funding_rate_evidence_reason,
)
from lightfee.sidecar.sources.exchange import ExchangeSource
from lightfee.strategy.funding_forecast_calibrator import FundingForecastCalibrator
from lightfee.venues.specs import get_spec

# V1 parity: per-domain timeout defaults (matching V1 sidecar_budget_ms configs)
DEFAULT_FUNDING_TIMEOUT_S = (
    30.0  # V1 parity: allow cold-cache warm for large-universe venues (OKX has 620 symbols)
)
DEFAULT_PER_VENUE_TIMEOUT_S = 15.0
SIDECAR_PUBLIC_HTTP_MAX_CONNECTIONS = 32

logger = logging.getLogger("lightfee.sidecar.service")


def _quote_cache_contract_eligible(quote: QuoteSnapshot) -> bool:
    """Return whether a quote is complete enough for restart last-good use."""

    if quote.contract_normalization_complete is not True:
        return False
    if str(quote.contract_type or "").strip().lower() != "linear":
        return False
    if str(quote.venue_status or "").strip().lower() != "active":
        return False
    if not all(
        str(value or "").strip()
        for value in (quote.underlying, quote.quote_currency, quote.mark_index_source)
    ):
        return False
    numeric_contract = (
        quote.contract_multiplier,
        quote.price_tick,
        quote.quantity_step_base,
        quote.min_quantity_base,
    )
    if not all(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and isfinite(float(value))
        and float(value) > 0.0
        for value in numeric_contract
    ):
        return False
    if not all(
        isinstance(value, int) and not isinstance(value, bool) and value >= 0
        for value in (quote.price_precision, quote.quantity_precision)
    ):
        return False
    if not (
        quote.min_notional_evidence_complete is True
        and isinstance(quote.min_notional_quote, (int, float))
        and not isinstance(quote.min_notional_quote, bool)
        and isfinite(float(quote.min_notional_quote))
        and float(quote.min_notional_quote) >= 0.0
    ):
        return False
    return all(
        isinstance(value, int) and not isinstance(value, bool) and value > 0
        for value in (quote.funding_timestamp_ms, quote.funding_interval_ms)
    )


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
        self._venue_configs_by_name = _canonical_venue_configs(config.venues)
        forecast_path = Path(self.snapshot_path).with_name(
            f"{Path(self.snapshot_path).name}.funding-forecast-calibration.json"
        )
        self._forecast_calibrator = FundingForecastCalibrator(
            forecast_path,
            min_samples=config.strategy.funding_forecast_min_samples,
            max_quantile_drift_bps=(
                config.strategy.funding_forecast_stability_max_quantile_drift_bps
            ),
        )
        runtime = config.runtime
        self._funding_timeout_s = runtime.sidecar_funding_timeout_s
        self._candidate_service = self._new_candidate_service()
        self._entry_venue_fetch_tasks: dict[str, asyncio.Task] = {}
        self._entry_venue_latest_results: dict[str, tuple] = {}
        self._entry_venue_late_tasks: set[asyncio.Task] = set()
        self.entry_venue_republish_event = asyncio.Event()
        self._entry_cache_only_refresh = False

        self._exchange_sources: dict[str, ExchangeSource] = {}
        from lightfee.venues.transport import EndpointRateLimiter

        self._public_rate_limiters: dict[str, EndpointRateLimiter] = {}

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
            # Slow funding/contract metadata remains governed by the shared
            # exchange budget.
            self._exchange_sources[venue_name] = ExchangeSource(
                spec,
                rate_limiter=rate_limiter,
                http_max_connections=SIDECAR_PUBLIC_HTTP_MAX_CONNECTIONS,
            )

        # A restart must not erase a cadence that the exchange has already
        # demonstrated. This local read adds no public REST request.
        try:
            prior_snapshot = load_snapshot(self.snapshot_path)
        except (KeyError, TypeError, ValueError):
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
                max_age_ms=max(int(runtime.live_scan_last_good_max_age_ms or 0), 0),
            )
            self._last_good_quotes = restored
            self._last_good_at_ms_by_key = {
                key: int(quote.observed_at_ms) for key, quote in restored.items()
            }
            self._last_good_at_ms = max(self._last_good_at_ms_by_key.values(), default=0)

    async def close(self) -> None:
        entry_fetch_tasks = list(
            getattr(self, "_entry_venue_fetch_tasks", {}).values()
        )
        for task in entry_fetch_tasks:
            if not task.done():
                task.cancel()
        if entry_fetch_tasks:
            await asyncio.gather(*entry_fetch_tasks, return_exceptions=True)
        self._entry_venue_fetch_tasks = {}
        self._entry_venue_late_tasks = set()
        republish_event = getattr(self, "entry_venue_republish_event", None)
        if isinstance(republish_event, asyncio.Event):
            republish_event.clear()
        for group_name, sources in (
            ("exchange", list(getattr(self, "_exchange_sources", {}).values())),
        ):
            for src in sources:
                try:
                    await src.close()
                except Exception:
                    logger.exception(
                        "sidecar source close failed; continuing resource cleanup",
                        extra={"source_group": group_name},
                    )

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
                fallback_funding_failures = _funding_failure_reasons(
                    fallback,
                    decision_at_ms=refresh_started_at_ms,
                )
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
                        observed_at_ms=(
                            _oldest_funding_observation_ms(
                                fallback,
                                symbols=fallback_funding_symbols,
                            )
                            or refresh_started_at_ms
                        ),
                        symbol_count=requested_symbol_count,
                        coverage_usable=len(fallback_funding_symbols),
                        degraded_reason=fallback_funding_reason,
                    )
                )
                market_lifecycle.append(
                    MarketLifecycle(
                        venue=venue_name,
                        observed_at_ms=(
                            _oldest_market_observation_ms(
                                fallback,
                                symbols=fallback_symbols - fallback_crossed_symbols,
                            )
                            or refresh_started_at_ms
                        ),
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
            funding_failures = _funding_failure_reasons(
                venue_quotes,
                decision_at_ms=refresh_started_at_ms,
            )
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
            if venue_quotes:
                for key, q in venue_quotes.items():
                    if int(getattr(q, "observed_at_ms", 0) or 0) <= 0:
                        q.observed_at_ms = refresh_started_at_ms
                    if not str(getattr(q, "source", "") or ""):
                        q.source = "sidecar_quote"
                    # The main quote source is the only broad-universe
                    # liquidity evidence plane.  Missing OI is deliberately
                    # handed to candidate-scoped runtime revalidation, never
                    # to a second all-symbol audit client.
                    if _snapshot_item_symbol(key, q) not in funding_failures:
                        quotes[key] = q

            funding_lifecycle.append(
                FundingLifecycle(
                    venue=venue_name,
                    observed_at_ms=(
                        _oldest_funding_observation_ms(
                            venue_quotes,
                            symbols=(
                                returned_symbols
                                - set(funding_failures)
                                - failed_symbols
                            ),
                        )
                        or refresh_started_at_ms
                    ),
                    symbol_count=len(listed_symbols),
                    coverage_usable=funding_usable,
                    degraded_reason=funding_reason,
                )
            )
            market_lifecycle.append(
                MarketLifecycle(
                    venue=venue_name,
                    observed_at_ms=(
                        _oldest_market_observation_ms(
                            venue_quotes,
                            symbols=returned_symbols - crossed_symbols - failed_symbols,
                        )
                        or refresh_started_at_ms
                    ),
                    symbol_count=len(listed_symbols),
                    coverage_usable=market_usable,
                    degraded_reason=market_reason,
                )
            )

        # The live entry generation must not await a seven-venue/full-symbol
        # liquidity pass.  Derive only strict proof already present on the
        # funding quotes; candidate-scoped OI is refreshed by the runtime.
        # The background audit is CPU-only and cannot start a second market
        # data collection pass.
        liquidity_lifecycle = _liquidity_lifecycle_from_quotes(
            configured_venues=self._configured_venue_names(),
            quotes=quotes,
            listed_symbols_by_venue=listed_symbols_by_venue,
            market_quality_failed_symbols=market_quality_failed_symbols,
            observed_at_ms=refresh_started_at_ms,
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
            if not _funding_failure_reasons(
                {key: quote},
                decision_at_ms=candidate_build_observed_at_ms,
            ):
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
                decision_at_ms=candidate_build_observed_at_ms,
            )
            _apply_future_quote_degradation(
                market_lifecycle,
                future_quote_symbols_by_venue,
                future_market_counted_by_venue,
                decision_at_ms=candidate_build_observed_at_ms,
            )
            _apply_future_quote_degradation(
                liquidity_lifecycle,
                future_quote_symbols_by_venue,
                future_liquidity_counted_by_venue,
                decision_at_ms=candidate_build_observed_at_ms,
            )

        # The V5 contract requires every lifecycle degradation to be owned by
        # a venue, symbol, or domain.  Lifecycle rows are already venue-scoped;
        # carry that source fact into the snapshot instead of letting the audit
        # writer reject an otherwise valid refresh.
        for lifecycle_rows in (
            funding_lifecycle,
            market_lifecycle,
            liquidity_lifecycle,
        ):
            degraded_venues.update(
                row.venue
                for row in lifecycle_rows
                if str(row.degraded_reason or "").strip()
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
        # The existing decision watermark is captured immediately before the
        # candidate pass.  Reuse it as the start marker so diagnostics do not
        # introduce another wall-clock read or re-date source evidence.
        candidate_build_started_at_ms = candidate_build_observed_at_ms
        candidate_timing_diagnostics: dict[str, object] = {
            # Keep global refresh health separate from candidate-level final
            # admission.  These are evidence timestamps only; they never
            # relax quote/OI freshness or the live revalidator's hard gates.
            "refresh_started_at_ms": refresh_started_at_ms,
            "quote_collection_completed_at_ms": candidate_build_observed_at_ms,
            # Quotes have no independent transport-receipt field.  Their
            # per-venue observation time is the original sidecar arrival
            # evidence; do not replace it with this refresh clock.
            "venue_quote_observed_at_ms": _venue_quote_receipt_timestamps(
                quotes,
                field_name="observed_at_ms",
            ),
            "open_interest_collection_completed_at_ms": (
                _latest_quote_evidence_timestamp(
                    quotes,
                    field_name="open_interest_received_at_ms",
                    fallback_ms=candidate_build_observed_at_ms,
                )
            ),
            "venue_open_interest_received_at_ms": _venue_quote_receipt_timestamps(
                quotes,
                field_name="open_interest_received_at_ms",
            ),
            "candidate_build_started_at_ms": candidate_build_started_at_ms,
        }
        candidate_build_diagnostics = dict(candidate_timing_diagnostics)
        candidate_service = self._ensure_candidate_service()
        candidates = await asyncio.to_thread(
            candidate_service.build,
            quotes,
            symbols,
            observed_at_ms=candidate_build_observed_at_ms,
            diagnostics=candidate_build_diagnostics,
        )
        # ``build`` replaces its diagnostic mapping to enforce pair-count
        # conservation. Re-attach collection timing afterwards; it describes
        # the same build and is never an eligibility input.
        candidate_build_diagnostics.update(candidate_timing_diagnostics)
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
        # Publication begins only after the synchronous candidate build has
        # returned, so this is a conservative completion bound without a
        # second, independently sampled process clock.
        candidate_build_diagnostics["candidate_build_completed_at_ms"] = published_ms
        candidate_build_diagnostics["entry_publish_started_at_ms"] = published_ms
        candidate_build_diagnostics["entry_snapshot_install_started_at_ms"] = (
            published_ms
        )
        candidate_build_diagnostics["refresh_latency_quantiles_ms"] = (
            self._record_refresh_latency_quantiles(
                max(published_ms - refresh_started_at_ms, 0)
            )
        )
        legacy_liquidity_publish_ms = int(
            getattr(self, "_last_liquidity_publish_at_ms", 0) or 0
        )
        liquidity_publish_by_key = getattr(
            self,
            "_last_liquidity_publish_at_ms_by_key",
            None,
        )
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
            has_usable_publish = (
                int(getattr(row, "coverage_usable", 0) or 0) > 0
            )
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

        self._attach_local_l2_depth_bridge(quotes, candidate_build_observed_at_ms)

        # --- Cache last-good quotes ---
        self._update_last_good_quote_cache(
            quotes,
            fresh_cacheable_quote_keys,
            published_at_ms=published_ms,
        )
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

        # The single sidecar snapshot is the live input and audit record.
        # Atomic replacement keeps readers from observing a partial refresh;
        # no candidate-only manifest, page set, or background audit projection
        # participates in entry.
        await asyncio.to_thread(publish_snapshot, snapshot, self.snapshot_path)
        return snapshot

    async def refresh_entry_from_latest_cache(self) -> SidecarSnapshot:
        """Republish the current sidecar view without starting public HTTP work."""
        self._entry_cache_only_refresh = True
        try:
            return await self.refresh_once()
        finally:
            self._entry_cache_only_refresh = False

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
        *,
        funding_metadata_only: bool = False,
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
                fetch = (
                    source.fetch_funding_metadata
                    if funding_metadata_only
                    else source.fetch_all
                )
                result = await asyncio.wait_for(fetch(symbols), timeout=timeout_s)
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

        # The live-entry publication is a bounded completion frontier, not an
        # all-venue barrier. Slow venue tasks stay alive as singleflight work.
        # Their completion updates a per-venue cache and wakes the service loop
        # so a fresh V6 generation is published immediately rather than at the
        # next multi-second polling interval.
        # Full/audit publication has its own background path and may use the
        # much larger configured transport timeout.
        venue_names = self._configured_venue_names()
        inflight = getattr(self, "_entry_venue_fetch_tasks", None)
        if not isinstance(inflight, dict):
            inflight = {}
            self._entry_venue_fetch_tasks = inflight
        latest = getattr(self, "_entry_venue_latest_results", None)
        if not isinstance(latest, dict):
            latest = {}
            self._entry_venue_latest_results = latest
        late_tasks = getattr(self, "_entry_venue_late_tasks", None)
        if not isinstance(late_tasks, set):
            late_tasks = set()
            self._entry_venue_late_tasks = late_tasks
        republish_event = getattr(self, "entry_venue_republish_event", None)
        if not isinstance(republish_event, asyncio.Event):
            republish_event = asyncio.Event()
            self.entry_venue_republish_event = republish_event
        if bool(getattr(self, "_entry_cache_only_refresh", False)):
            return [
                latest.get(
                    venue_name,
                    (
                        venue_name,
                        None,
                        RuntimeError("entry venue latest cache unavailable"),
                        set(),
                    ),
                )
                for venue_name in venue_names
            ]

        def _record_completion(task: asyncio.Task, venue_name: str) -> None:
            if not task.cancelled():
                try:
                    latest[venue_name] = task.result()
                except Exception as exc:
                    latest[venue_name] = (venue_name, None, exc, set())
            if task in late_tasks:
                late_tasks.discard(task)
                republish_event.set()

        task_to_venue: dict[asyncio.Task, str] = {}
        for venue_name in venue_names:
            task = inflight.get(venue_name)
            if task is not None and task.done():
                _record_completion(task, venue_name)
                inflight.pop(venue_name, None)
                task = None
            if task is None:
                task = asyncio.create_task(
                    _fetch_one(venue_name),
                    name=f"funding-entry-venue-fetch:{venue_name}",
                )
                inflight[venue_name] = task
                task.add_done_callback(
                    lambda completed, venue=venue_name: _record_completion(
                        completed, venue
                    )
                )
                if venue_name in latest:
                    # This is a scheduled refresh of an already published
                    # venue. Its completion must republish from cache, but
                    # must not start another network generation.
                    late_tasks.add(task)
            task_to_venue[task] = venue_name

        entry_deadline_s = min(max(float(timeout_s), 0.0), 0.45)
        missing_tasks = {
            task
            for task, venue_name in task_to_venue.items()
            if venue_name not in latest
        }
        if missing_tasks:
            done, pending = await asyncio.wait(
                missing_tasks,
                timeout=entry_deadline_s,
                return_when=asyncio.ALL_COMPLETED,
            )
        else:
            done, pending = set(), set()
        results_by_venue: dict[
            str,
            tuple[str, Optional[dict[str, QuoteSnapshot]], Optional[Exception], set[str]],
        ] = {}
        for task in done:
            venue_name = task_to_venue[task]
            if inflight.get(venue_name) is task:
                inflight.pop(venue_name, None)
            _record_completion(task, venue_name)
            results_by_venue[venue_name] = latest[venue_name]
        for task in pending:
            venue_name = task_to_venue[task]
            late_tasks.add(task)
            if venue_name in latest:
                results_by_venue[venue_name] = latest[venue_name]
            else:
                results_by_venue[venue_name] = (
                    venue_name,
                    None,
                    TimeoutError(
                        f"entry venue evidence deadline {entry_deadline_s:.3f}s; "
                        "background fetch inflight"
                    ),
                    set(),
                )
        for task in task_to_venue:
            if not task.done() and task not in pending:
                late_tasks.add(task)
        for venue_name in venue_names:
            if venue_name not in results_by_venue and venue_name in latest:
                results_by_venue[venue_name] = latest[venue_name]
        return [results_by_venue[venue_name] for venue_name in venue_names]

    def _configured_venue_names(self) -> list[str]:
        """Return the canonical, stable, unique operational venue scope."""
        configured = getattr(self, "_venue_configs_by_name", None)
        if not isinstance(configured, dict):
            configured = _canonical_venue_configs(self.config.venues)
            self._venue_configs_by_name = configured
        return list(configured)

    def _record_refresh_latency_quantiles(self, latency_ms: int) -> dict[str, int]:
        samples = getattr(self, "_refresh_latency_samples_ms", None)
        if not isinstance(samples, list):
            samples = []
            self._refresh_latency_samples_ms = samples
        if latency_ms >= 0:
            samples.append(int(latency_ms))
        del samples[:-128]
        if not samples:
            return {
                "sample_count": 0,
                "window_size": 128,
                "p50": 0,
                "p95": 0,
                "p99": 0,
            }
        ordered = sorted(samples)

        def _percentile(percentile: float) -> int:
            index = min(
                max(int(ceil(len(ordered) * percentile)) - 1, 0),
                len(ordered) - 1,
            )
            return int(ordered[index])

        return {
            "sample_count": len(ordered),
            "window_size": 128,
            "p50": _percentile(0.50),
            "p95": _percentile(0.95),
            "p99": _percentile(0.99),
        }

    def _ensure_forecast_calibrator(self) -> FundingForecastCalibrator:
        """Keep direct test/recovery construction compatible with the service."""
        calibrator = getattr(self, "_forecast_calibrator", None)
        if calibrator is None:
            snapshot_path = Path(self.snapshot_path)
            forecast_path = snapshot_path.with_name(
                f"{snapshot_path.name}.funding-forecast-calibration.json"
            )
            strategy = self.config.strategy
            calibrator = FundingForecastCalibrator(
                forecast_path,
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
        return FundingCandidateService(
            strategy=config.strategy,
            venue_fee_bps=configured_taker,
            venue_maker_fee_bps=configured_maker,
            venue_notional_caps={
                venue_name: float(venue.max_notional or 0.0)
                for venue_name, venue in venue_configs.items()
            },
            passive_execution_enabled=(str(config.runtime.mode or "").lower() == "live"),
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
            or _funding_failure_reasons(
                {key: quote},
                decision_at_ms=now_ms,
            )
        ):
            continue
        restored[key] = replace(quote)
    return restored


def _apply_future_quote_degradation(
    lifecycle_rows: list[FundingLifecycle | MarketLifecycle | LiquidityLifecycle],
    future_symbols_by_venue: dict[str, set[str]],
    counted_symbols_by_venue: dict[str, set[str]],
    *,
    decision_at_ms: int,
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
        # Once every future sample is removed, this row is a degradation
        # assessment made at the candidate watermark, not proof observed in
        # the future.  Keep the lifecycle clock publishable and fail-closed.
        row.observed_at_ms = min(int(row.observed_at_ms), int(decision_at_ms))
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


def _quote_has_strict_liquidity_evidence(quote: QuoteSnapshot) -> bool:
    """Use the same proof semantics for live and full-audit liquidity health."""
    try:
        volume = float(quote.volume_24h_quote)
        open_interest = float(quote.open_interest)
        observed_at_ms = int(quote.open_interest_observed_at_ms or 0)
        event_at_ms = int(quote.open_interest_event_at_ms or 0)
        received_at_ms = int(quote.open_interest_received_at_ms or 0)
    except (TypeError, ValueError, OverflowError):
        return False
    return bool(
        isfinite(volume)
        and volume > 0.0
        and quote.open_interest_evidence_status == "observed"
        and isfinite(open_interest)
        and open_interest >= 0.0
        and observed_at_ms > 0
        and received_at_ms >= observed_at_ms
        and event_at_ms >= 0
        and event_at_ms <= received_at_ms + 5_000
        and str(quote.open_interest_source or "").strip()
        and str(quote.open_interest_sample_id or "").strip()
        and str(quote.open_interest_venue_symbol or "").strip()
    )


def _venue_quote_receipt_timestamps(
    quotes: Mapping[str, QuoteSnapshot],
    *,
    field_name: str,
) -> dict[str, int]:
    """Return original per-venue receipt timestamps without re-dating quotes."""
    timestamps: dict[str, int] = {}
    for quote in quotes.values():
        venue = str(getattr(quote, "venue", "") or "").strip().lower()
        try:
            received_at_ms = int(getattr(quote, field_name, 0) or 0)
        except (TypeError, ValueError):
            received_at_ms = 0
        if venue and received_at_ms > 0:
            timestamps[venue] = max(timestamps.get(venue, 0), received_at_ms)
    return dict(sorted(timestamps.items()))


def _latest_quote_evidence_timestamp(
    quotes: Mapping[str, QuoteSnapshot],
    *,
    field_name: str,
    fallback_ms: int,
) -> int:
    timestamps = _venue_quote_receipt_timestamps(quotes, field_name=field_name)
    return max(timestamps.values(), default=int(fallback_ms))


def _quote_requires_entry_targeted_oi_revalidation(quote: QuoteSnapshot) -> bool:
    """Return whether OI is deliberately deferred to live admission.

    This is the shared marker for both compact entry publication and the
    full-audit path.  It is never a substitute for the runtime's strict OI
    fetch before an order is admitted.
    """
    return entry_targeted_oi_revalidation_required(
        evidence_status=quote.open_interest_evidence_status,
        evidence_reason=quote.open_interest_evidence_reason,
        volume_24h_quote=quote.volume_24h_quote,
    )


def _liquidity_lifecycle_from_quotes(
    *,
    configured_venues: list[str],
    quotes: dict[str, QuoteSnapshot],
    listed_symbols_by_venue: dict[str, set[str]],
    market_quality_failed_symbols: dict[str, set[str]],
    observed_at_ms: int,
    transport_errors: dict[str, Exception] | None = None,
) -> list[LiquidityLifecycle]:
    """Build lifecycle counts from actual volume plus typed OI proof."""
    transport_errors = transport_errors or {}
    rows: list[LiquidityLifecycle] = []
    for venue in configured_venues:
        venue_key = str(venue or "").lower()
        venue_quotes = [
            quote
            for quote in quotes.values()
            if str(quote.venue or "").lower() == venue_key
        ]
        usable = sum(_quote_has_strict_liquidity_evidence(q) for q in venue_quotes)
        deferred_oi = sum(
            _quote_requires_entry_targeted_oi_revalidation(q)
            for q in venue_quotes
        )
        listed = set(listed_symbols_by_venue.get(venue_key, set()))
        listed.update(str(q.symbol or "").upper() for q in venue_quotes)
        failed = set(market_quality_failed_symbols.get(venue_key, set()))
        quoted_symbols = {
            str(q.symbol or "").upper()
            for q in venue_quotes
            if str(q.symbol or "").strip()
        }
        unavailable_quote_symbols = (listed | failed) - quoted_symbols
        # An explicit deferred marker carries valid volume evidence and is
        # revalidated by the runtime for the candidate that reaches admission.
        # It must not be reported as a global strict-proof failure, while all
        # other missing proof remains fail-closed and visible to the audit.
        proof_missing = max(len(venue_quotes) - usable - deferred_oi, 0)
        reasons: list[str] = []
        error = transport_errors.get(venue_key)
        if error is not None:
            reasons.append(f"transport:{type(error).__name__}:{error}"[:200])
        if failed:
            reasons.append(f"market_failed_symbols:{len(failed)}")
        if unavailable_quote_symbols:
            # The authoritative quote data plane intentionally excludes rows
            # with invalid/stale funding or unusable market identity.  They
            # remain in the listed-symbol denominator, so record that real
            # source gap rather than publishing an unexplained coverage hole.
            reasons.append(
                f"liquidity_quote_unavailable:{len(unavailable_quote_symbols)}"
            )
        if proof_missing:
            reasons.append(f"strict_liquidity_proof_missing:{proof_missing}")
        if not venue_quotes:
            reasons.append("no_liquidity_quotes")
        rows.append(
            LiquidityLifecycle(
                venue=venue_key,
                observed_at_ms=int(observed_at_ms),
                symbol_count=len(listed | failed),
                coverage_usable=int(usable),
                degraded_reason="; ".join(reasons),
            )
        )
    return rows


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
    *,
    decision_at_ms: int | None = None,
) -> dict[str, str]:
    """Return per-symbol proof gaps that make funding data unusable."""
    failures: dict[str, str] = {}
    for key, quote in (quotes or {}).items():
        reasons: list[str] = []
        funding_reason = funding_rate_evidence_reason(
            venue=str(getattr(quote, "venue", "") or ""),
            symbol=str(getattr(quote, "symbol", "") or ""),
            rate_bps=getattr(quote, "funding_rate_bps", None),
            funding_timestamp_ms=getattr(quote, "funding_timestamp_ms", 0),
            observed_at_ms=getattr(quote, "funding_rate_observed_at_ms", 0),
            event_at_ms=getattr(quote, "funding_rate_event_at_ms", 0),
            received_at_ms=getattr(quote, "funding_rate_received_at_ms", 0),
            source=getattr(quote, "funding_rate_source", ""),
            sample_id=getattr(quote, "funding_rate_sample_id", ""),
            decision_at_ms=(
                int(decision_at_ms)
                if decision_at_ms is not None
                else max(
                    int(getattr(quote, "funding_rate_received_at_ms", 0) or 0),
                    int(getattr(quote, "observed_at_ms", 0) or 0),
                )
            ),
        )
        if funding_reason:
            reasons.append(funding_reason)
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
        funding_rate_observed_at_ms=0,
        funding_rate_event_at_ms=0,
        funding_rate_received_at_ms=0,
        funding_rate_source="invalid_funding_quarantined",
        funding_rate_sample_id="",
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


def _oldest_funding_observation_ms(
    quotes: dict[str, QuoteSnapshot],
    *,
    symbols: set[str],
) -> int:
    observations = [
        int(getattr(quote, "funding_rate_observed_at_ms", 0) or 0)
        for key, quote in quotes.items()
        if _snapshot_item_symbol(key, quote) in symbols
        and int(getattr(quote, "funding_rate_observed_at_ms", 0) or 0) > 0
    ]
    return min(observations) if observations else 0


def _oldest_market_observation_ms(
    quotes: dict[str, QuoteSnapshot],
    *,
    symbols: set[str],
) -> int:
    observations = [
        int(getattr(quote, "observed_at_ms", 0) or 0)
        for key, quote in quotes.items()
        if _snapshot_item_symbol(key, quote) in symbols
        and int(getattr(quote, "observed_at_ms", 0) or 0) > 0
    ]
    return min(observations) if observations else 0


def _latest_valid_quote_observation_ms(
    quotes: dict[str, QuoteSnapshot],
    *,
    decision_at_ms: int,
    fallback_ms: int,
) -> int:
    observations = [int(getattr(quote, "observed_at_ms", 0) or 0) for quote in quotes.values()]
    valid = [value for value in observations if 0 < value <= decision_at_ms]
    return max(valid, default=max(int(fallback_ms or 0), 0))
