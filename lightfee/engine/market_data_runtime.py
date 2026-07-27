"""Market data runtime delegate.

This module owns behavior mechanically moved from LiveRuntime.
Do not change market-data business conditions while extracting it.
"""

from __future__ import annotations

import asyncio
import copy
import math
from collections import Counter, OrderedDict, deque
from typing import Any

from lightfee.config.paths import resolve_config_artifact_path
from lightfee.core.contracts import VenueAdapter
from lightfee.core.domain import Venue
from lightfee.engine.business_contract import quote_rewarm_handoff_contract
from lightfee.engine.bootstrap import wall_clock_now_ms
from lightfee.engine.lifecycle_sla import phase_budgets_from_strategy
from lightfee.engine.runtime_context import MarketDataRuntimeContext
from lightfee.marketdata.l2 import L2BookStatus, L2PoolAssignment, LocalL2BookKey
from lightfee.marketdata.open_interest import (
    ENTRY_OPEN_INTEREST_CACHE_FALLBACK_MAX_AGE_MS,
    bounded_open_interest_cache_fallback_max_age_ms,
    normalize_open_interest_status,
    observed_open_interest_proof_reason,
    open_interest_max_age_ms_for_evidence,
    open_interest_timestamps_are_fresh,
    open_interest_uses_cache_fallback,
)
from lightfee.persistence.open_interest_store import OpenInterestEvidenceStore
from lightfee.sidecar.snapshot import funding_rate_evidence_reason

ENTRY_OPEN_INTEREST_HOT_CACHE_MAX_ENTRIES = 256


def _mark_entry_evidence_domain_state(
    coordinator: dict[str, Any] | None,
    *,
    domain: str,
    candidate_index: int,
    state: str,
) -> bool:
    """Publish one domain result and report cross-domain selection readiness."""
    if coordinator is None:
        return state == "ready"
    domain_states = coordinator.setdefault(domain, {})
    domain_states[int(candidate_index)] = state
    quote_states = coordinator.setdefault("quote", {})
    oi_states = coordinator.setdefault("open_interest", {})
    economics_states = coordinator.setdefault("economics", {})
    selection_blocked_indices = coordinator.setdefault(
        "selection_blocked_indices",
        set(),
    )
    candidate_count = int(coordinator.get("candidate_count", 0) or 0)
    validator = coordinator.get("candidate_ready_validator")
    # Validate every row whose two public evidence domains are complete.
    # Selection later reprices and reranks this entire ready set; economics is
    # not a one-winner latch tied to stale snapshot rank.
    for index in range(candidate_count):
        if economics_states.get(index) in {"ready", "failed"}:
            continue
        if quote_states.get(index) != "ready" or oi_states.get(index) != "ready":
            continue
        if callable(validator):
            validation = validator(index)
            if validation == "selection_blocked":
                # The public economics proof is complete, but a later entry
                # gate (for example the finalization window) rejects this row
                # for the current tick.  Preserve it for final diagnostics
                # while allowing a lower executable row to wake selection.
                economics_states[index] = "ready"
                selection_blocked_indices.add(index)
                continue
            if not bool(validation):
                economics_states[index] = "failed"
                continue
        economics_states[index] = "ready"

    ready_indices = [
        index
        for index in range(candidate_count)
        if economics_states.get(index) == "ready"
        and index not in selection_blocked_indices
    ]
    if not ready_indices:
        return False
    # Fast path only when the highest unresolved prior snapshot rank is now
    # terminal.  If a lower row finishes first, the common deadline remains
    # open so a higher row can join the same fresh-economics rerank batch.
    selected_index = min(ready_indices)
    if not all(
        quote_states.get(prior) == "failed"
        or oi_states.get(prior) == "failed"
        or economics_states.get(prior) == "failed"
        or prior in selection_blocked_indices
        for prior in range(selected_index)
    ):
        return False
    coordinator["selected_index"] = selected_index
    ready_event = coordinator.get("selection_ready_event")
    if isinstance(ready_event, asyncio.Event):
        ready_event.set()
    return True


def _targeted_open_interest_observed_proof_valid(
    *,
    venue: str,
    symbol: str,
    result: dict[str, Any],
) -> bool:
    """Validate the full economic/sample identity before mutating a quote."""
    return not observed_open_interest_proof_reason(
        venue=venue,
        canonical_symbol=symbol,
        venue_symbol=str(result.get("open_interest_venue_symbol") or ""),
        value_quote=result.get("open_interest_quote"),
        raw_value=result.get("raw_open_interest"),
        raw_unit=str(result.get("raw_open_interest_unit") or ""),
        contract_multiplier=result.get("open_interest_contract_multiplier"),
        conversion_mark_price=result.get("open_interest_conversion_mark_price"),
        observed_at_ms=result.get("open_interest_observed_at_ms"),
        event_at_ms=result.get("open_interest_event_at_ms"),
        received_at_ms=result.get("open_interest_received_at_ms"),
        source=str(result.get("open_interest_source") or ""),
        sample_id=str(result.get("open_interest_sample_id") or ""),
    )


def _open_interest_cache_fallback_payload(
    *,
    venue: str,
    symbol: str,
    payload: dict[str, Any] | None,
    now_ms: int,
    reason: str,
    max_age_ms: int = ENTRY_OPEN_INTEREST_CACHE_FALLBACK_MAX_AGE_MS,
) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    fallback_max_age_ms = bounded_open_interest_cache_fallback_max_age_ms(max_age_ms)
    result = dict(payload)
    if (
        normalize_open_interest_status(
            result.get("open_interest_evidence_status", "unavailable")
        )
        != "observed"
        or not _targeted_open_interest_observed_proof_valid(
            venue=venue,
            symbol=symbol,
            result=result,
        )
        or not open_interest_timestamps_are_fresh(
            observed_at_ms=int(result.get("open_interest_observed_at_ms", 0) or 0),
            received_at_ms=int(result.get("open_interest_received_at_ms", 0) or 0),
            event_at_ms=int(result.get("open_interest_event_at_ms", 0) or 0),
            now_ms=now_ms,
            max_age_ms=fallback_max_age_ms,
        )
    ):
        return None
    observed_at_ms = int(result.get("open_interest_observed_at_ms", 0) or 0)
    result["open_interest_evidence_reason"] = str(reason or "targeted_refresh_cache_fallback")
    result["open_interest_cache_fallback"] = True
    result["open_interest_cache_fallback_max_age_ms"] = fallback_max_age_ms
    result["open_interest_cache_fallback_age_ms"] = max(int(now_ms) - observed_at_ms, 0)
    return result


def _open_interest_exception_status(exc: Exception) -> str:
    """Preserve transport/parse semantics at the entry evidence boundary."""
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return "timeout"
    response = getattr(exc, "response", None)
    status_code = int(
        getattr(exc, "status_code", 0)
        or getattr(response, "status_code", 0)
        or 0
    )
    message = str(exc).lower()
    if status_code in {418, 429} or any(
        marker in message
        for marker in ("rate limit", "rate-limit", "too many requests")
    ):
        return "rate_limited"
    if (
        str(getattr(exc, "category", "") or "") == "parse_failure"
        or isinstance(exc, (ValueError, TypeError, KeyError, OverflowError))
    ):
        return "parse_error"
    return "http_error"


def _open_interest_exception_phase_timings(exc: Exception) -> dict[str, Any]:
    phase = getattr(exc, "phase_timings", None)
    if not isinstance(phase, dict):
        return {}
    return {
        "oi_dns_ms": phase.get("dns_ms"),
        "oi_connect_ms": int(phase.get("connect_ms", 0) or 0),
        "oi_pool_wait_ms": int(phase.get("pool_wait_ms", 0) or 0),
        "oi_rate_limit_wait_ms": int(
            phase.get("rate_limit_wait_ms", 0) or 0
        ),
        "oi_transport_total_ms": int(
            phase.get("transport_total_ms", 0) or 0
        ),
        "oi_http_ms": int(phase.get("http_ms", 0) or 0),
        "oi_parse_ms": int(phase.get("parse_ms", 0) or 0),
        "oi_dns_timing_status": str(
            phase.get("dns_timing_status", "unavailable") or "unavailable"
        ),
    }


def _evidence_clock_or_zero(value: object) -> int:
    """Parse an evidence clock without letting malformed payloads crash a tick."""
    if isinstance(value, bool):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return 0


def _positive_runtime_ms(value: object, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        parsed = 0
    if parsed <= 0:
        parsed = int(default)
    return max(parsed, 1)


class EntryOpenInterestRefresher:
    """Candidate-scoped public OI refresher for entry liquidity evidence."""

    SUPPORTED_VENUES = {
        "aster",
        "binance",
        "bitget",
        "bybit",
        "gate",
        "hyperliquid",
        "okx",
    }

    def __init__(
        self,
        *,
        targeted_budget_s: float | None = None,
        durable_store: OpenInterestEvidenceStore | None = None,
        durable_store_path: str | None = None,
        prewarm_interval_ms: int = 15 * 60_000,
    ) -> None:
        self._clients: dict[str, Any] = {}
        self._cache: OrderedDict[tuple[str, str], dict[str, Any]] = OrderedDict()
        self._inflight: dict[tuple[str, str], asyncio.Task] = {}
        self._inflight_started_at_ms: dict[tuple[str, str], int] = {}
        self._prewarm_inflight_keys: set[tuple[str, str]] = set()
        self._venue_semaphores: dict[str, asyncio.Semaphore] = {}
        self._prewarm_venue_gates: dict[str, asyncio.Lock] = {}
        self._cache_max_entries = ENTRY_OPEN_INTEREST_HOT_CACHE_MAX_ENTRIES
        self._durable_store = durable_store
        if self._durable_store is None and str(durable_store_path or "").strip():
            self._durable_store = OpenInterestEvidenceStore(durable_store_path)
        self._prewarm_interval_ms = max(int(prewarm_interval_ms or 0), 1)
        # This is a transport single-flight bound, not an opportunity-membership
        # cap.  The caller keeps the complete eligible frontier queued and
        # launches more targets as capacity is released.
        self._max_inflight = 64
        self._max_prewarm_inflight = 4
        self._last_prewarm_started_ms = 0
        self._reused_count = 0
        self._deferred_count = 0
        self._cancelled_count = 0
        if targeted_budget_s is None:
            from lightfee.venues.market_data import (
                BINANCE_STYLE_ENTRY_OPEN_INTEREST_BUDGET_S,
            )

            targeted_budget_s = BINANCE_STYLE_ENTRY_OPEN_INTEREST_BUDGET_S
        self._targeted_budget_s = max(float(targeted_budget_s or 0.0), 0.0)

    async def close(self) -> None:
        inflight = list(self._inflight.values())
        for task in inflight:
            if not task.done():
                self._cancelled_count += 1
            task.cancel()
        if inflight:
            await asyncio.gather(*inflight, return_exceptions=True)
        self._inflight.clear()
        self._inflight_started_at_ms.clear()
        self._prewarm_inflight_keys.clear()
        self._cache.clear()
        self._venue_semaphores.clear()
        self._prewarm_venue_gates.clear()
        for client in list(self._clients.values()):
            close = getattr(client, "close", None)
            if callable(close):
                await close()
        self._clients.clear()

    def cancel_prewarm(self) -> None:
        """Preempt low-priority OI maintenance in favor of entry execution."""
        for key in list(self._prewarm_inflight_keys):
            task = self._inflight.pop(key, None)
            self._inflight_started_at_ms.pop(key, None)
            self._prewarm_inflight_keys.discard(key)
            if task is not None and not task.done():
                self._cancelled_count += 1
                task.cancel()

    def prewarm_due(self, *, now_ms: int) -> bool:
        return (
            self._last_prewarm_started_ms <= 0
            or int(now_ms) - self._last_prewarm_started_ms >= self._prewarm_interval_ms
        )

    def mark_prewarm_started(self, *, now_ms: int) -> None:
        self._last_prewarm_started_ms = int(now_ms)

    def delete_expired(
        self,
        *,
        now_ms: int,
        max_age_ms: int = ENTRY_OPEN_INTEREST_CACHE_FALLBACK_MAX_AGE_MS,
    ) -> int:
        store = self._durable_store
        if store is None:
            return 0
        return store.delete_expired(now_ms=now_ms, max_age_ms=max_age_ms)

    def _active_inflight_count(self, *, include_prewarm: bool = True) -> int:
        return sum(
            1
            for key, task in self._inflight.items()
            if not task.done()
            and (include_prewarm or key not in self._prewarm_inflight_keys)
        )

    def _prewarm_venue_gate(self, venue: str) -> asyncio.Lock:
        venue_key = str(venue or "").strip().lower()
        gate = self._prewarm_venue_gates.get(venue_key)
        if gate is None:
            gate = asyncio.Lock()
            self._prewarm_venue_gates[venue_key] = gate
        return gate

    def _remember_observed_open_interest(
        self,
        *,
        venue: str,
        symbol: str,
        payload: dict[str, Any] | None,
        now_ms: int,
        persist: bool,
    ) -> bool:
        venue_key = str(venue or "").strip().lower()
        symbol_key = str(symbol or "").strip().upper()
        if (
            not venue_key
            or not symbol_key
            or not isinstance(payload, dict)
            or normalize_open_interest_status(
                payload.get("open_interest_evidence_status", "unavailable")
            )
            != "observed"
            or open_interest_uses_cache_fallback(payload)
            or not _targeted_open_interest_observed_proof_valid(
                venue=venue_key,
                symbol=symbol_key,
                result=payload,
            )
            or not open_interest_timestamps_are_fresh(
                observed_at_ms=int(payload.get("open_interest_observed_at_ms", 0) or 0),
                received_at_ms=int(payload.get("open_interest_received_at_ms", 0) or 0),
                event_at_ms=int(payload.get("open_interest_event_at_ms", 0) or 0),
                now_ms=now_ms,
                max_age_ms=ENTRY_OPEN_INTEREST_CACHE_FALLBACK_MAX_AGE_MS,
            )
        ):
            return False
        cache_key = (venue_key, symbol_key)
        previous = self._cache.get(cache_key)
        new_order = (
            int(payload.get("open_interest_observed_at_ms", 0) or 0),
            int(payload.get("open_interest_received_at_ms", 0) or 0),
            str(payload.get("open_interest_sample_id", "") or ""),
        )
        previous_order = (
            int((previous or {}).get("open_interest_observed_at_ms", 0) or 0),
            int((previous or {}).get("open_interest_received_at_ms", 0) or 0),
            str((previous or {}).get("open_interest_sample_id", "") or ""),
        )
        if previous is not None and new_order < previous_order:
            return False
        self._cache[cache_key] = dict(payload)
        self._cache.move_to_end(cache_key)
        while len(self._cache) > self._cache_max_entries:
            self._cache.popitem(last=False)
        store = self._durable_store
        if persist and store is not None:
            store.store_observed(
                venue=venue_key,
                symbol=symbol_key,
                payload=payload,
                now_ms=now_ms,
            )
        return True

    def scheduler_metrics(self, *, now_ms: int) -> dict[str, int]:
        started_values = [
            int(self._inflight_started_at_ms.get(key, 0) or 0)
            for key, task in self._inflight.items()
            if not task.done()
        ]
        oldest_started = min(
            (value for value in started_values if value > 0),
            default=0,
        )
        return {
            "inflight_count": self._active_inflight_count(),
            "queued_count": 0,
            "max_inflight": self._max_inflight,
            "oldest_age_ms": (
                max(int(now_ms) - oldest_started, 0) if oldest_started else 0
            ),
            "reused_count": self._reused_count,
            "deferred_count": self._deferred_count,
            "cancelled_count": self._cancelled_count,
        }

    def cached_open_interest(
        self,
        venue: str,
        symbol: str,
        *,
        now_ms: int,
        max_age_ms: int = ENTRY_OPEN_INTEREST_CACHE_FALLBACK_MAX_AGE_MS,
        reason: str = "targeted_refresh_cache_fallback",
    ) -> dict[str, Any] | None:
        venue_key = str(venue or "").strip().lower()
        symbol_key = str(symbol or "").strip().upper()
        cached = self._cache.get((venue_key, symbol_key))
        if cached is not None:
            self._cache.move_to_end((venue_key, symbol_key))
        else:
            store = self._durable_store
            if store is not None:
                cached = store.load_observed(
                    venue=venue_key,
                    symbol=symbol_key,
                    now_ms=now_ms,
                    max_age_ms=max_age_ms,
                )
                if cached is not None:
                    self._remember_observed_open_interest(
                        venue=venue_key,
                        symbol=symbol_key,
                        payload=cached,
                        now_ms=now_ms,
                        persist=False,
                    )
        return _open_interest_cache_fallback_payload(
            venue=venue_key,
            symbol=symbol_key,
            payload=cached,
            now_ms=now_ms,
            reason=reason,
            max_age_ms=max_age_ms,
        )

    def _client_for_venue(self, venue: str):
        venue_key = str(venue or "").strip().lower()
        client = self._clients.get(venue_key)
        if client is not None:
            return client
        from lightfee.venues.market_data import MarketDataClient
        from lightfee.venues.specs import get_spec

        venue_enum = Venue.from_str(venue_key)
        client = MarketDataClient(get_spec(venue_enum))
        client.binance_style_open_interest_enrichment_budget_s = self._targeted_budget_s
        self._clients[venue_key] = client
        return client

    async def refresh_open_interest(
        self,
        venue: str,
        symbol: str,
        *,
        now_ms: int,
        force_refresh: bool = False,
        max_age_ms: int = 30_000,
        cache_fallback_max_age_ms: int = (
            ENTRY_OPEN_INTEREST_CACHE_FALLBACK_MAX_AGE_MS
        ),
        priority: str = "entry_execution",
    ) -> dict[str, Any] | None:
        venue_key = str(venue or "").strip().lower()
        symbol_key = str(symbol or "").strip().upper()
        priority_key = str(priority or "entry_execution").strip().lower()
        is_prewarm = priority_key in {
            "prewarm",
            "prewarm_only",
            "maintenance",
        } or priority_key.startswith("prewarm")
        if not symbol_key:
            return {
                "open_interest_quote": None,
                "open_interest_evidence_status": "unsupported",
                "open_interest_evidence_reason": "unsupported_targeted_refresh",
            }
        cache_key = (venue_key, symbol_key)
        inflight_key = (venue_key, symbol_key)
        cached = self._cache.get(cache_key)
        observed_at_ms = int((cached or {}).get("open_interest_observed_at_ms", 0) or 0)
        received_at_ms = int((cached or {}).get("open_interest_received_at_ms", 0) or 0)
        event_at_ms = int((cached or {}).get("open_interest_event_at_ms", 0) or 0)
        if (
            not is_prewarm
            and cached is not None
            and normalize_open_interest_status(
                cached.get("open_interest_evidence_status", "unavailable")
            ) == "observed"
            and not open_interest_uses_cache_fallback(cached)
            and _targeted_open_interest_observed_proof_valid(
                venue=venue_key,
                symbol=symbol_key,
                result=cached,
            )
            and open_interest_timestamps_are_fresh(
                observed_at_ms=observed_at_ms,
                received_at_ms=received_at_ms,
                event_at_ms=event_at_ms,
                now_ms=now_ms,
                max_age_ms=max_age_ms,
            )
        ):
            return dict(cached)

        if not is_prewarm:
            store = self._durable_store
            durable = None
            if store is not None:
                durable = store.load_observed(
                    venue=venue_key,
                    symbol=symbol_key,
                    now_ms=now_ms,
                    max_age_ms=cache_fallback_max_age_ms,
                )
            if durable is not None:
                self._remember_observed_open_interest(
                    venue=venue_key,
                    symbol=symbol_key,
                    payload=durable,
                    now_ms=now_ms,
                    persist=False,
                )
                fallback = _open_interest_cache_fallback_payload(
                    venue=venue_key,
                    symbol=symbol_key,
                    payload=durable,
                    now_ms=now_ms,
                    reason="entry_oi_durable_cache_fallback",
                    max_age_ms=cache_fallback_max_age_ms,
                )
                if fallback is not None:
                    return {
                        **fallback,
                        **self.scheduler_metrics(now_ms=now_ms),
                    }

        if not is_prewarm:
            self.cancel_prewarm()
        task = self._inflight.get(inflight_key)
        owns_inflight_task = False
        if task is None or task.done():
            if is_prewarm:
                same_venue_prewarm_active = False
                for existing_key in self._prewarm_inflight_keys:
                    if existing_key == inflight_key or existing_key[0] != venue_key:
                        continue
                    existing_task = self._inflight.get(existing_key)
                    if existing_task is not None and not existing_task.done():
                        same_venue_prewarm_active = True
                        break
                if same_venue_prewarm_active:
                    self._deferred_count += 1
                    fallback = self.cached_open_interest(
                        venue_key,
                        symbol_key,
                        now_ms=now_ms,
                        max_age_ms=cache_fallback_max_age_ms,
                        reason="entry_oi_prewarm_venue_capacity_cache_fallback",
                    )
                    if fallback is not None:
                        return {
                            **fallback,
                            "oi_scheduler_deferred_count": 1,
                            **self.scheduler_metrics(now_ms=now_ms),
                        }
                    return {
                        "open_interest_quote": None,
                        "open_interest_evidence_status": "deferred",
                        "open_interest_evidence_reason": (
                            "entry_oi_prewarm_venue_capacity_reserved"
                        ),
                        "oi_scheduler_deferred_count": 1,
                        **self.scheduler_metrics(now_ms=now_ms),
                    }
            live_inflight = self._active_inflight_count(include_prewarm=False)
            prewarm_inflight = self._active_inflight_count() - live_inflight
            capacity_exceeded = (
                prewarm_inflight >= self._max_prewarm_inflight
                or self._active_inflight_count() >= self._max_inflight
            )
            if not is_prewarm:
                capacity_exceeded = live_inflight >= self._max_inflight
            if capacity_exceeded:
                self._deferred_count += 1
                fallback = self.cached_open_interest(
                    venue_key,
                    symbol_key,
                    now_ms=now_ms,
                    max_age_ms=cache_fallback_max_age_ms,
                    reason="entry_evidence_scheduler_cache_fallback",
                )
                if fallback is not None:
                    return {
                        **fallback,
                        "oi_scheduler_deferred_count": 1,
                        **self.scheduler_metrics(now_ms=now_ms),
                    }
                deferred_reason = (
                    "entry_oi_prewarm_scheduler_capacity_reserved"
                    if is_prewarm
                    else "entry_evidence_scheduler_capacity_exceeded"
                )
                return {
                    "open_interest_quote": None,
                    "open_interest_evidence_status": "deferred",
                    "open_interest_evidence_reason": deferred_reason,
                    "oi_scheduler_deferred_count": 1,
                    **self.scheduler_metrics(now_ms=now_ms),
                }
            async def _load() -> dict[str, Any] | None:
                if is_prewarm:
                    async with self._prewarm_venue_gate(venue_key):
                        batch = await self.refresh_open_interest_batch(
                            venue_key,
                            [symbol_key],
                            now_ms=now_ms,
                            force_refresh=force_refresh,
                        )
                else:
                    batch = await self.refresh_open_interest_batch(
                        venue_key,
                        [symbol_key],
                        now_ms=now_ms,
                        force_refresh=force_refresh,
                    )
                return batch.get(symbol_key)

            task = asyncio.create_task(
                _load(),
                name=f"entry-oi:{venue_key}:{symbol_key}",
            )
            self._inflight[inflight_key] = task
            self._inflight_started_at_ms[inflight_key] = int(now_ms)
            owns_inflight_task = True
            if is_prewarm:
                self._prewarm_inflight_keys.add(inflight_key)

            def _completed(done: asyncio.Task) -> None:
                if self._inflight.get(inflight_key) is done:
                    self._prewarm_inflight_keys.discard(inflight_key)
                    self._inflight.pop(inflight_key, None)
                    self._inflight_started_at_ms.pop(inflight_key, None)
                if done.cancelled():
                    return
                try:
                    payload = done.result()
                except Exception:
                    return
                if (
                    payload is None
                    or normalize_open_interest_status(
                        payload.get("open_interest_evidence_status", "unavailable")
                    )
                    != "observed"
                    or not _targeted_open_interest_observed_proof_valid(
                        venue=venue_key,
                        symbol=symbol_key,
                        result=payload,
                    )
                ):
                    # A failed refresh is actionable evidence for this caller,
                    # but must never overwrite a newer last-good observation.
                    return
                completion_now_ms = int(wall_clock_now_ms())
                self._remember_observed_open_interest(
                    venue=venue_key,
                    symbol=symbol_key,
                    payload=payload,
                    now_ms=completion_now_ms,
                    persist=True,
                )

            task.add_done_callback(_completed)
        else:
            self._reused_count += 1
        # The target deadline belongs to the caller.  It must not cancel the
        # singleflight request and leave a connection/rate-limit task orphaned.
        try:
            result = await asyncio.shield(task)
        except asyncio.CancelledError:
            if is_prewarm and task.cancelled():
                return {
                    "open_interest_quote": None,
                    "open_interest_evidence_status": "deferred",
                    "open_interest_evidence_reason": "entry_oi_prewarm_cancelled",
                    **self.scheduler_metrics(now_ms=now_ms),
                }
            if priority_key == "prewarm_only" and owns_inflight_task:
                if self._inflight.get(inflight_key) is task:
                    self._inflight.pop(inflight_key, None)
                    self._inflight_started_at_ms.pop(inflight_key, None)
                    self._prewarm_inflight_keys.discard(inflight_key)
                if not task.done():
                    self._cancelled_count += 1
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            raise
        except Exception as exc:
            status = _open_interest_exception_status(exc)
            if not is_prewarm:
                fallback = self.cached_open_interest(
                    venue_key,
                    symbol_key,
                    now_ms=now_ms,
                    max_age_ms=cache_fallback_max_age_ms,
                    reason=f"{status}_cache_fallback",
                )
                if fallback is not None:
                    return {
                        **fallback,
                        **_open_interest_exception_phase_timings(exc),
                        **self.scheduler_metrics(now_ms=now_ms),
                    }
            raise
        if result is None:
            return None
        if (
            normalize_open_interest_status(
                result.get("open_interest_evidence_status", "unavailable")
            )
            != "observed"
            or not _targeted_open_interest_observed_proof_valid(
                venue=venue_key,
                symbol=symbol_key,
                result=result,
            )
            or not open_interest_timestamps_are_fresh(
                observed_at_ms=int(result.get("open_interest_observed_at_ms", 0) or 0),
                received_at_ms=int(result.get("open_interest_received_at_ms", 0) or 0),
                event_at_ms=int(result.get("open_interest_event_at_ms", 0) or 0),
                now_ms=now_ms,
                max_age_ms=open_interest_max_age_ms_for_evidence(
                    result,
                    default_max_age_ms=max_age_ms,
                ),
            )
        ):
            reason = str(
                result.get("open_interest_evidence_reason")
                or result.get("open_interest_evidence_status")
                or "targeted_refresh_failed"
            )
            if not is_prewarm:
                fallback = self.cached_open_interest(
                    venue_key,
                    symbol_key,
                    now_ms=now_ms,
                    max_age_ms=cache_fallback_max_age_ms,
                    reason=f"{reason}_cache_fallback",
                )
                if fallback is not None:
                    result = {**result, **fallback}
        return {
            **result,
            **self.scheduler_metrics(now_ms=now_ms),
        }

    async def refresh_open_interest_batch(
        self,
        venue: str,
        symbols: list[str],
        *,
        now_ms: int,
        force_refresh: bool = False,
    ) -> dict[str, dict[str, Any]]:
        venue_key = str(venue or "").strip().lower()
        symbol_keys = [
            str(symbol or "").strip().upper()
            for symbol in symbols
            if str(symbol or "").strip()
        ]
        symbol_keys = list(dict.fromkeys(symbol_keys))
        if venue_key not in self.SUPPORTED_VENUES:
            return {
                symbol: {
                    "open_interest_quote": None,
                    "open_interest_evidence_status": "unsupported",
                    "open_interest_evidence_reason": "unsupported_targeted_refresh",
                }
                for symbol in symbol_keys
            }
        if not symbol_keys:
            return {}
        try:
            semaphore = self._venue_semaphores.setdefault(
                venue_key,
                asyncio.Semaphore(2),
            )
            async with semaphore:
                result = await self._client_for_venue(
                    venue_key
                ).fetch_entry_open_interest_evidence(
                    symbol_keys,
                    force_refresh=force_refresh,
                )
        except Exception as exc:
            status = _open_interest_exception_status(exc)
            return {
                symbol: {
                    "open_interest_quote": None,
                    "open_interest_evidence_status": status,
                    "open_interest_evidence_reason": f"{type(exc).__name__}: {exc}"[:200],
                    **_open_interest_exception_phase_timings(exc),
                }
                for symbol in symbol_keys
            }
        payloads: dict[str, dict[str, Any]] = {}
        for symbol_key in symbol_keys:
            ticker = result.get(f"{venue_key}:{symbol_key}")
            if ticker is None:
                payloads[symbol_key] = {
                    "open_interest_quote": None,
                    "open_interest_evidence_status": "parse_error",
                    "open_interest_evidence_reason": "missing_targeted_ticker",
                }
                continue
            payloads[symbol_key] = {
                "open_interest_quote": getattr(ticker, "open_interest_quote", None),
                "open_interest_evidence_status": normalize_open_interest_status(
                    getattr(ticker, "open_interest_evidence_status", "unavailable")
                ),
                "open_interest_evidence_reason": str(
                    getattr(ticker, "open_interest_evidence_reason", "")
                    or "targeted_refresh"
                ),
                "open_interest_observed_at_ms": int(
                    getattr(ticker, "open_interest_observed_at_ms", 0) or 0
                ),
                "open_interest_event_at_ms": int(
                    getattr(ticker, "open_interest_event_at_ms", 0) or 0
                ),
                "open_interest_received_at_ms": int(
                    getattr(ticker, "open_interest_received_at_ms", 0) or 0
                ),
                "open_interest_source": str(
                    getattr(ticker, "open_interest_source", "") or ""
                ),
                "open_interest_sample_id": str(
                    getattr(ticker, "open_interest_sample_id", "") or ""
                ),
                "open_interest_venue_symbol": str(
                    getattr(ticker, "open_interest_venue_symbol", "") or ""
                ),
                "raw_open_interest": getattr(ticker, "raw_open_interest", None),
                "raw_open_interest_unit": str(
                    getattr(ticker, "raw_open_interest_unit", "") or ""
                ),
                "open_interest_contract_multiplier": getattr(
                    ticker, "open_interest_contract_multiplier", None
                ),
                "open_interest_conversion_mark_price": getattr(
                    ticker, "open_interest_conversion_mark_price", None
                ),
                "oi_candidate_count": int(getattr(ticker, "oi_candidate_count", 0) or 0),
                "oi_cache_hit_count": int(getattr(ticker, "oi_cache_hit_count", 0) or 0),
                "oi_cache_miss_count": int(getattr(ticker, "oi_cache_miss_count", 0) or 0),
                "oi_refresh_attempt_count": int(
                    getattr(ticker, "oi_refresh_attempt_count", 0) or 0
                ),
                "oi_refresh_cap": int(getattr(ticker, "oi_refresh_cap", 0) or 0),
                "oi_deferred_count": int(getattr(ticker, "oi_deferred_count", 0) or 0),
                "oi_timeout_count": int(getattr(ticker, "oi_timeout_count", 0) or 0),
                "oi_refresh_elapsed_ms": int(
                    getattr(ticker, "oi_refresh_elapsed_ms", 0) or 0
                ),
                "oi_dns_ms": getattr(ticker, "oi_dns_ms", None),
                "oi_connect_ms": int(getattr(ticker, "oi_connect_ms", 0) or 0),
                "oi_pool_wait_ms": int(
                    getattr(ticker, "oi_pool_wait_ms", 0) or 0
                ),
                "oi_rate_limit_wait_ms": int(
                    getattr(ticker, "oi_rate_limit_wait_ms", 0) or 0
                ),
                "oi_transport_total_ms": int(
                    getattr(ticker, "oi_transport_total_ms", 0) or 0
                ),
                "oi_http_ms": int(getattr(ticker, "oi_http_ms", 0) or 0),
                "oi_parse_ms": int(getattr(ticker, "oi_parse_ms", 0) or 0),
                "oi_dns_timing_status": str(
                    getattr(ticker, "oi_dns_timing_status", "unavailable")
                    or "unavailable"
                ),
            }
        return payloads


class MarketDataRuntime:
    SNAPSHOT_LATENCY_QUANTILE_WINDOW = 128

    def __init__(self, ctx: MarketDataRuntimeContext) -> None:
        self.ctx = ctx
        self._last_local_l2_depth_bridge_publish_ms = 0
        self._last_local_l2_depth_bridge_error_ms = 0
        self._snapshot_latency_samples_ms: dict[str, deque[int]] = {}

    def _current_wall_clock_ms(self) -> int:
        """Read the owner runtime clock so tests and production share one clock.

        The value is deliberately fetched on every call.  Entry evidence must
        not keep using the tick-start wall clock after an ``await``.
        """

        provider = getattr(self.ctx, "_entry_wall_clock_now_ms", None)
        if callable(provider):
            return int(provider())
        return int(wall_clock_now_ms())

    @classmethod
    def _snapshot_latency_quantiles_ms(cls, samples: list[int]) -> dict[str, int]:
        if not samples:
            return {
                "sample_count": 0,
                "window_size": cls.SNAPSHOT_LATENCY_QUANTILE_WINDOW,
                "p50": 0,
                "p95": 0,
                "p99": 0,
            }
        ordered = sorted(int(sample) for sample in samples)

        def percentile(value: float) -> int:
            index = min(
                max(math.ceil(len(ordered) * value) - 1, 0),
                len(ordered) - 1,
            )
            return ordered[index]

        return {
            "sample_count": len(ordered),
            "window_size": cls.SNAPSHOT_LATENCY_QUANTILE_WINDOW,
            "p50": percentile(0.50),
            "p95": percentile(0.95),
            "p99": percentile(0.99),
        }

    def _snapshot_latency_quantile_summary_ms(
        self,
        key: str,
        latency_ms: int,
    ) -> dict[str, int]:
        samples = self._snapshot_latency_samples_ms.setdefault(
            key,
            deque(maxlen=self.SNAPSHOT_LATENCY_QUANTILE_WINDOW),
        )
        if latency_ms > 0:
            samples.append(int(latency_ms))
        return self._snapshot_latency_quantiles_ms(list(samples))

    @property
    def ws_bbo_rest_refresher(self):
        return getattr(self.ctx, "ws_bbo_rest_refresher", None)

    @ws_bbo_rest_refresher.setter
    def ws_bbo_rest_refresher(self, value) -> None:
        setattr(self.ctx, "ws_bbo_rest_refresher", value)

    @property
    def entry_open_interest_refresher(self):
        return getattr(self.ctx, "entry_open_interest_refresher", None)

    @entry_open_interest_refresher.setter
    def entry_open_interest_refresher(self, value) -> None:
        setattr(self.ctx, "entry_open_interest_refresher", value)

    @property
    def _entry_bbo_subscription_budgeted_keys(self) -> set[tuple[str, str]]:
        return self.ctx._entry_bbo_subscription_budgeted_keys

    @_entry_bbo_subscription_budgeted_keys.setter
    def _entry_bbo_subscription_budgeted_keys(self, value: set[tuple[str, str]]) -> None:
        self.ctx._entry_bbo_subscription_budgeted_keys = value

    @property
    def _entry_bbo_subscription_budget_excluded_keys(self) -> set[tuple[str, str]]:
        return self.ctx._entry_bbo_subscription_budget_excluded_keys

    @_entry_bbo_subscription_budget_excluded_keys.setter
    def _entry_bbo_subscription_budget_excluded_keys(self, value: set[tuple[str, str]]) -> None:
        self.ctx._entry_bbo_subscription_budget_excluded_keys = value

    @property
    def _entry_bbo_subscription_per_venue_budget(self) -> int:
        return self.ctx._entry_bbo_subscription_per_venue_budget

    @_entry_bbo_subscription_per_venue_budget.setter
    def _entry_bbo_subscription_per_venue_budget(self, value: int) -> None:
        self.ctx._entry_bbo_subscription_per_venue_budget = value

    @property
    def _last_snapshot_freshness_filter_blockers(self):
        return self.ctx._last_snapshot_freshness_filter_blockers

    @_last_snapshot_freshness_filter_blockers.setter
    def _last_snapshot_freshness_filter_blockers(self, value) -> None:
        self.ctx._last_snapshot_freshness_filter_blockers = value

    @property
    def _last_snapshot_freshness_filter_samples(self):
        return self.ctx._last_snapshot_freshness_filter_samples

    @_last_snapshot_freshness_filter_samples.setter
    def _last_snapshot_freshness_filter_samples(self, value) -> None:
        self.ctx._last_snapshot_freshness_filter_samples = value

    @property
    def _snapshot_freshness_decision_last_emit_ms(self):
        return self.ctx._snapshot_freshness_decision_last_emit_ms

    @property
    def _snapshot_freshness_decision_suppressed(self):
        return self.ctx._snapshot_freshness_decision_suppressed

    @property
    def _SNAPSHOT_FRESHNESS_DECISION_LOG_INTERVAL_MS(self) -> int:
        return self.ctx._SNAPSHOT_FRESHNESS_DECISION_LOG_INTERVAL_MS

    def get_venue_adapter(self, venue: Venue) -> VenueAdapter | None:
        return self.ctx.get_venue_adapter(venue)

    def _entry_readiness_provider_name(self) -> str:
        return self.ctx._entry_effective_readiness_provider_name()

    def _entry_readiness_provider_uses_local_l2(self) -> bool:
        return self.ctx._entry_effective_readiness_provider_uses_local_l2()

    def _entry_readiness_provider_uses_ws_bbo(self) -> bool:
        return self.ctx._entry_effective_readiness_provider_uses_ws_bbo()

    def _local_l2_effective_enabled(self) -> bool:
        return self.ctx._entry_local_l2_effective_enabled()

    def _entry_local_l2_stale_after_ms(self) -> int:
        return self.ctx._entry_local_l2_stale_after_ms()

    async def _filter_symbols_supported_by_venue(
        self,
        venue: Venue,
        adapter: VenueAdapter,
        symbols: list[str],
        *,
        skip_event_kind: str,
    ) -> list[str]:
        return await self.ctx._filter_symbols_supported_by_venue(
            venue,
            adapter,
            symbols,
            skip_event_kind=skip_event_kind,
        )

    def _append_runtime_diagnostic_event(self, *args, **kwargs) -> None:
        return self.ctx._append_runtime_diagnostic_event(*args, **kwargs)

    def _candidate_pair_id(self, candidate) -> str:
        return self.ctx._candidate_pair_id(candidate)

    def _clear_local_l2_runtime_state(self) -> None:
        return self.ctx._clear_local_l2_runtime_state()

    def _record_snapshot_scoped_status(self, *args, **kwargs) -> None:
        return self.ctx._record_snapshot_scoped_status(*args, **kwargs)

    def _candidate_requires_sidecar_perp_liquidity(self, candidate) -> bool:
        return self.ctx._candidate_requires_sidecar_perp_liquidity(candidate)

    def _entry_liquidity_qualification_decisions(self, *args, **kwargs):
        return self.ctx._entry_liquidity_qualification_decisions(*args, **kwargs)

    def _liquidity_degraded_reason_blocks_symbol(self, reason: str, symbol: str) -> bool:
        return self.ctx._liquidity_degraded_reason_blocks_symbol(reason, symbol)

    def _liquidity_lifecycle_payload(self, *args, **kwargs) -> dict:
        return self.ctx._liquidity_lifecycle_payload(*args, **kwargs)

    def _select_v1_entry_tracked_scope(self, candidates) -> tuple[list, list]:
        return self.ctx._select_v1_entry_tracked_scope(candidates)

    def _runtime_method_override(self, method_name: str):
        method = getattr(self.ctx, method_name, None)
        class_method = getattr(type(self.ctx), method_name, None)
        if getattr(method, "__func__", None) is class_method:
            return None
        return method if callable(method) else None

    async def _call_ensure_entry_bbo_active_for_candidates(
        self, candidates, now_ms: int
    ) -> None:
        override = self._runtime_method_override("_ensure_entry_bbo_active_for_candidates")
        if override is not None:
            return await override(candidates, now_ms)
        return await self._ensure_entry_bbo_active_for_candidates(candidates, now_ms)

    def _call_candidate_snapshot_freshness_decisions(self, candidate, **kwargs):
        override = self._runtime_method_override("_candidate_snapshot_freshness_decisions")
        if override is not None:
            return override(candidate, **kwargs)
        return self._candidate_snapshot_freshness_decisions(candidate, **kwargs)

    def _runtime_market_data_config_summary(self) -> dict[str, Any]:
        provider = self._entry_readiness_provider_name()
        return {
            **self.ctx._entry_effective_readiness_provider_diagnostics(),
            "local_l2_configured_enabled": self.ctx.config.strategy.local_l2_enabled,
            "local_l2_ws_configured_enabled": self.ctx.config.strategy.local_l2_ws_enabled,
            "local_l2_effective_enabled": self._local_l2_effective_enabled(),
            "local_l2_effective_disabled_reason": (
                "ws_bbo_l2_on_demand_requires_local_l2"
                if (
                    provider == "ws_bbo_l2_on_demand"
                    and not self.ctx.config.strategy.local_l2_enabled
                )
                else ""
            ),
        }

    def _refresh_runtime_market_data_config_state(self) -> None:
        self.ctx.state.runtime_market_data_config = (
            self._runtime_market_data_config_summary()
        )

    def _entry_quote_lease_max_age_ms(self) -> int:
        budgets = [
            value
            for value in (
                self.ctx.config.runtime.max_market_age_ms,
                self.ctx.config.strategy.entry_quote_lease_ttl_ms,
            )
            if value > 0
        ]
        return min(budgets) if budgets else 0

    async def _activate_local_l2_phase(self, now_ms: int) -> None:
        """Phase 5: Activate local-L2 books — WS streams first, then background bootstrap.

        V1 parity with live_startup_activate_local_l2():
        1. Derive target pairs from retained state (retained_local_l2_books) and
           hot positions — NOT all config.symbols (V1: startup_local_l2_symbols)
        2. Create LocalL2Book for each target pair
        3. Start WS depth streams FIRST (deltas buffered during bootstrap gap)
        4. Start per-venue background bootstrap workers (REST snapshots)
        5. Return immediately — bootstrap completes asynchronously in background

        WS updates received while a book is BOOTSTRAPPING are buffered and
        replayed after the REST snapshot completes (V1 pre-snapshot buffer pattern).

        Runtime L2 activation for new entry symbols is handled separately by
        _ensure_l2_active_for_candidates() on each tick.
        """
        self._refresh_runtime_market_data_config_state()
        if not self._local_l2_effective_enabled():
            return

        self.ctx.journal.append(
            "runtime.local_l2_phase_start",
            {"ts_ms": now_ms},
        )

        # V1: startup_local_l2_symbols() → retained + hot symbols only
        # NOT all config.symbols — L2 is only bootstrapped for symbols with activity
        target_pairs: set[tuple[str, str]] = set()
        if self._local_l2_effective_enabled():
            active_venues = list(self.ctx.venue_adapters.keys())
            venue_set = {
                v.value if hasattr(v, 'value') else str(v)
                for v in active_venues
            }

            # 1. Retained books from previous run (V1: retained_local_l2_books)
            for book in (self.ctx.state.retained_local_l2_books or []):
                ven = book.get("venue", "")
                sym = book.get("symbol", "")
                if ven in venue_set and sym:
                    target_pairs.add((ven, sym))

            # 2. Hot symbols from active positions (V1: hot_local_l2_symbols)
            hot_budget = max(self.ctx.config.strategy.local_l2_hot_exec_per_venue_budget, 1)
            hot_global_budget = max(self.ctx.config.strategy.local_l2_hot_exec_global_budget, 0)
            hot_count = 0
            hot_global_count = 0
            for pos in getattr(self.ctx.state, 'open_positions', []) or []:
                if hot_count >= hot_budget:
                    break
                if hot_global_budget > 0 and hot_global_count >= hot_global_budget:
                    break
                ven = getattr(pos, 'venue', '')
                sym = getattr(pos, 'symbol', '')
                if isinstance(ven, str) and ven in venue_set and sym:
                    target_pairs.add((ven, sym))
                    hot_count += 1
                    hot_global_count += 1
                elif hasattr(ven, 'value'):
                    ven_str = ven.value
                    if ven_str in venue_set and sym:
                        target_pairs.add((ven_str, sym))
                        hot_count += 1
                        hot_global_count += 1

        if not target_pairs:
            self.ctx.journal.append(
                "runtime.local_l2_phase_complete",
                {
                    "books_bootstrapped": 0,
                    "reason": "no target pairs — local_l2 disabled or no venues/symbols",
                    "phase_ms": wall_clock_now_ms() - now_ms,
                },
            )
            return

        if self.ctx.config.runtime.mode != "paper":
            from lightfee.core.domain import Venue as VenueEnum

            filtered_pairs: set[tuple[str, str]] = set()
            venue_symbols_for_filter: dict[str, list[str]] = {}
            for venue_str, symbol in target_pairs:
                venue_symbols_for_filter.setdefault(venue_str, []).append(symbol)

            for venue_str, symbols in venue_symbols_for_filter.items():
                try:
                    ven = VenueEnum.from_str(venue_str)
                    adapter = self.get_venue_adapter(ven) if ven in self.ctx.venue_adapters else None
                except (ValueError, KeyError):
                    adapter = None
                    ven = None
                if adapter is None or ven is None:
                    filtered_pairs.update((venue_str, sym) for sym in symbols)
                    continue
                filtered_symbols = await self._filter_symbols_supported_by_venue(
                    ven,
                    adapter,
                    sorted(symbols),
                    skip_event_kind="runtime.local_l2_symbol_skipped",
                )
                filtered_pairs.update((venue_str, sym) for sym in filtered_symbols)

            target_pairs = filtered_pairs

        if not target_pairs:
            self.ctx.journal.append(
                "runtime.local_l2_phase_complete",
                {
                    "books_bootstrapped": 0,
                    "reason": "no target pairs after venue symbol catalog filtering",
                    "phase_ms": wall_clock_now_ms() - now_ms,
                },
            )
            return

        from lightfee.marketdata.local_l2_venues import get_venue_rules

        # Step 1: Create books for all target pairs (V1: mark_binance_local_l2_bootstrapping)
        books_created = 0
        for venue_str, symbol in sorted(target_pairs):
            rules = get_venue_rules(venue_str)
            book = self.ctx.local_l2_runtime.ensure_book(venue_str, symbol)
            book.max_depth = rules.default_depth
            book.max_sequence_gap = rules.max_sequence_gap
            if book.status == L2BookStatus.COLD:
                if self.ctx.config.runtime.mode == "paper":
                    book.transition_to_hot()
                else:
                    book.transition_to_bootstrapping(now_ms)
                books_created += 1

        venue_symbols: dict[str, list[str]] = {}
        for venue_str, symbol in target_pairs:
            venue_symbols.setdefault(venue_str, []).append(symbol)

        # Step 2: Start WS streams FIRST for all venues (V1: start_local_l2_ws)
        # This ensures delta updates are captured (buffered) during bootstrap gap
        if (
            self._local_l2_effective_enabled()
            and self.ctx.config.strategy.local_l2_ws_enabled
            and self.ctx.config.runtime.mode != "paper"
        ):
            ws_started = 0
            for venue_str, symbols in venue_symbols.items():
                try:
                    from lightfee.core.domain import Venue as VenueEnum
                    ven = VenueEnum.from_str(venue_str)
                    adapter = self.get_venue_adapter(ven) if ven in self.ctx.venue_adapters else None
                except (ValueError, KeyError):
                    adapter = None

                registered = self.ctx.l2_data_plane.start_ws_streams(
                    venue_str, symbols, adapter=adapter,
                )
                if registered > 0:
                    ws_started += registered

            if ws_started > 0:
                connected = await self.ctx.l2_data_plane.connect_ws_streams()
                ws_started = connected
                self.ctx.journal.append(
                    "runtime.local_l2_ws_started",
                    {
                        "stream_count": ws_started,
                        "venues": sorted(venue_symbols.keys()),
                        "ts_ms": wall_clock_now_ms(),
                    },
                )

        # Step 3: Start per-venue background bootstrap workers (V1: start_local_l2_bootstrap)
        # Each worker fetches REST snapshots with concurrency control and retry
        if self.ctx.config.runtime.mode != "paper":
            bs_total = 0
            bs_batch = self.ctx.config.strategy.local_l2_bootstrap_batch_size
            bs_jitter = self.ctx.config.strategy.local_l2_bootstrap_jitter_ms
            bs_retry = self.ctx.config.strategy.local_l2_bootstrap_retry_backoff_ms

            for venue_str, symbols in venue_symbols.items():
                try:
                    from lightfee.core.domain import Venue as VenueEnum
                    ven = VenueEnum.from_str(venue_str)
                    adapter = self.get_venue_adapter(ven) if ven in self.ctx.venue_adapters else None
                except (ValueError, KeyError):
                    adapter = None

                if adapter is None or not hasattr(adapter, 'fetch_l2_snapshot'):
                    continue

                self.ctx.l2_data_plane.start_background_bootstrap(
                    venue=venue_str,
                    symbols=symbols,
                    adapter=adapter,
                    batch_size=bs_batch,
                    jitter_ms=bs_jitter,
                    retry_backoff_ms=bs_retry,
                )
                bs_total += len(symbols)

            self.ctx.journal.append(
                "runtime.local_l2_bootstrap_started",
                {
                    "venues": sorted(venue_symbols.keys()),
                    "total_symbols": bs_total,
                    "ts_ms": wall_clock_now_ms(),
                },
            )

        # Restore retained books from previous state
        books_retained = 0
        if hasattr(self.ctx.state, "retained_local_l2_books"):
            for entry in getattr(self.ctx.state, "retained_local_l2_books", []):
                venue = entry.get("venue", "")
                sym = entry.get("symbol", "")
                if (venue, sym) not in target_pairs:
                    continue
                if venue and sym:
                    book = self.ctx.local_l2_runtime.ensure_book(venue, sym)
                    if book.status == L2BookStatus.COLD:
                        book.pool = L2PoolAssignment.RETAINED
                        book.transition_to_bootstrapping(now_ms)
                        books_retained += 1

        self.ctx.journal.append(
            "runtime.local_l2_phase_complete",
            {
                "books_created": books_created,
                "books_retained": books_retained,
                "target_pairs": len(target_pairs),
                "phase_ms": wall_clock_now_ms() - now_ms,
                "bootstrap_mode": "background_per_venue",
            },
        )

    async def _ensure_l2_active_for_candidates(
        self,
        candidates,
        now_ms: int,
        *,
        tracked_opportunities=None,
    ) -> None:
        """Ensure L2 books are active for candidate entry symbols.

        V1 parity: activity_local_l2_symbols() → live_startup_activate_local_l2().

        Called on each tick when tradeable candidates are discovered.  For each
        candidate's long/short venue+symbol pair that does NOT already have an
        active L2 book, create the book, start a WS stream, and spawn a
        background bootstrap worker.

        Respects local_l2_hot_exec_per_venue_budget (V1).
        """
        self._refresh_runtime_market_data_config_state()
        if not self._local_l2_effective_enabled():
            return
        if self.ctx.config.runtime.mode == "paper":
            return

        candidates = list(candidates or [])
        tracked_opportunities = list(tracked_opportunities or [])
        tracked_keys: set[LocalL2BookKey] = set()
        pending_l2_keys: set[LocalL2BookKey] = set()
        pool_by_key: dict[LocalL2BookKey, L2PoolAssignment] = {}
        pool_rank = {
            L2PoolAssignment.HOT_EXEC: 0,
            L2PoolAssignment.WARM: 1,
            L2PoolAssignment.RETAINED: 2,
        }

        def venue_name(venue) -> str:
            return venue.value if hasattr(venue, "value") else str(venue or "")

        def candidate_oi_allows_l2(candidate) -> bool:
            evidence = getattr(candidate, "entry_open_interest_evidence", None)
            if not isinstance(evidence, dict):
                return False
            revision_id = str(
                getattr(candidate, "evidence_candidate_revision_id", "")
                or getattr(candidate, "candidate_revision_id", "")
                or ""
            )
            if not revision_id:
                return False
            if str(evidence.get("candidate_revision_id") or "") != revision_id:
                return False
            symbol = str(getattr(candidate, "symbol", "") or "").strip().upper()
            if not symbol:
                return False
            max_age_ms = max(
                int(
                    getattr(
                        self.ctx.config.runtime,
                        "sidecar_perp_liquidity_budget_ms",
                        30_000,
                    )
                    or 0
                ),
                1,
            )
            expected_venues = {
                "long": str(getattr(candidate, "long_venue", "") or "").strip().lower(),
                "short": str(getattr(candidate, "short_venue", "") or "").strip().lower(),
            }
            for leg in ("long", "short"):
                row = evidence.get(leg)
                if not isinstance(row, dict):
                    return False
                expected_venue = expected_venues[leg]
                if (
                    not expected_venue
                    or str(row.get("venue") or "").strip().lower() != expected_venue
                    or str(row.get("canonical_symbol") or "").strip().upper() != symbol
                    or str(row.get("status") or "").strip().lower() != "observed"
                ):
                    return False
                try:
                    observed_at_ms = int(row.get("observed_at_ms") or 0)
                    event_at_ms = int(row.get("event_at_ms") or 0)
                    received_at_ms = int(row.get("received_at_ms") or 0)
                    value_quote = float(row.get("value_quote"))
                    raw_value = float(row.get("raw_value"))
                except (TypeError, ValueError, OverflowError):
                    return False
                if observed_open_interest_proof_reason(
                    venue=expected_venue,
                    canonical_symbol=symbol,
                    venue_symbol=str(row.get("venue_symbol") or ""),
                    value_quote=value_quote,
                    raw_value=raw_value,
                    raw_unit=str(row.get("raw_unit") or ""),
                    contract_multiplier=row.get("contract_multiplier"),
                    conversion_mark_price=row.get("conversion_mark_price"),
                    observed_at_ms=observed_at_ms,
                    event_at_ms=event_at_ms,
                    received_at_ms=received_at_ms,
                    source=str(row.get("source") or ""),
                    sample_id=str(row.get("sample_id") or ""),
                ):
                    return False
                open_interest_floor = float(
                    self.ctx.config.strategy.entry_open_interest_floor_quote(
                        expected_venue
                    )
                )
                if value_quote + 1e-9 < open_interest_floor:
                    return False
                if not open_interest_timestamps_are_fresh(
                    observed_at_ms=observed_at_ms,
                    received_at_ms=received_at_ms,
                    event_at_ms=event_at_ms,
                    now_ms=now_ms,
                    max_age_ms=open_interest_max_age_ms_for_evidence(
                        row,
                        default_max_age_ms=max_age_ms,
                    ),
                ):
                    return False
            return True

        oi_valid_candidates: list[Any] = []
        for candidate in candidates:
            if not candidate_oi_allows_l2(candidate):
                continue
            oi_valid_candidates.append(candidate)
            if len(oi_valid_candidates) >= 3:
                break
        candidates = oi_valid_candidates

        def remember_key(venue, symbol, pool: L2PoolAssignment) -> LocalL2BookKey | None:
            ven_str = venue_name(venue)
            sym = str(symbol or "")
            if not ven_str or not sym:
                return None
            key = LocalL2BookKey(venue=ven_str, symbol=sym)
            tracked_keys.add(key)
            existing = pool_by_key.get(key)
            if existing is None or pool_rank[pool] < pool_rank[existing]:
                pool_by_key[key] = pool
            return key

        for opportunity in tracked_opportunities:
            pool = (
                L2PoolAssignment.HOT_EXEC
                if getattr(getattr(opportunity, "class_", None), "value", "") == "primary_tracked"
                else L2PoolAssignment.WARM
            )
            sym = getattr(opportunity, "symbol", "")
            for venue in (
                getattr(opportunity, "long_venue", ""),
                getattr(opportunity, "short_venue", ""),
            ):
                remember_key(venue, sym, pool)

        # Collect (venue, symbol) pairs from candidates that need L2
        # CandidateInput has long_venue/short_venue as str fields (not leg objects)
        needed: dict[str, set[str]] = {}  # venue -> {symbols}
        registered_total = 0
        registered_venues: set[str] = set()
        connect_ws_streams_needed = False
        stale_after_ms = self._entry_local_l2_stale_after_ms()
        from lightfee.marketdata.local_l2_policy import BridgeMode, policy_for_venue

        def hot_book_needs_ws_lifecycle_attention(venue: str, symbol: str) -> bool:
            if not self.ctx.config.strategy.local_l2_ws_enabled:
                return False
            policy = policy_for_venue(venue)
            if policy.bridge_mode not in (
                BridgeMode.WS_SNAPSHOT_AUTHORITATIVE,
                BridgeMode.STREAM_ONLY,
            ):
                return False
            stream_state_fn = getattr(self.ctx.l2_data_plane, "ws_stream_state", None)
            if not callable(stream_state_fn):
                return False
            stream_state = stream_state_fn(venue, symbol)
            return (
                not bool(stream_state.get("registered"))
                or not bool(stream_state.get("connected"))
            )

        def venue_adapter_for_local_l2(venue: str):
            try:
                ven = Venue.from_str(venue)
                return self.get_venue_adapter(ven) if ven in self.ctx.venue_adapters else None
            except (ValueError, KeyError):
                return None

        def require_l2_key(
            venue,
            symbol,
            pool: L2PoolAssignment,
            *,
            pending_entry: bool = False,
        ) -> None:
            key = remember_key(venue, symbol, pool)
            if key is None:
                return
            needed.setdefault(key.venue, set()).add(key.symbol)
            if pending_entry:
                pending_l2_keys.add(key)

        def ensure_hot_ws_lifecycle(venue: str, symbol: str) -> None:
            nonlocal registered_total, connect_ws_streams_needed
            adapter = venue_adapter_for_local_l2(venue)
            if adapter is None or not hasattr(adapter, 'fetch_l2_snapshot'):
                return
            before_state = self.ctx.l2_data_plane.ws_stream_state(venue, symbol)
            registered = self.ctx.l2_data_plane.start_ws_streams(
                venue, [symbol], adapter=adapter,
            )
            after_state = self.ctx.l2_data_plane.ws_stream_state(venue, symbol)
            if registered > 0:
                registered_total += registered
            if (
                registered > 0
                or (
                    bool(before_state.get("registered"))
                    and not bool(before_state.get("connected"))
                )
                or (
                    bool(after_state.get("registered"))
                    and not bool(after_state.get("connected"))
                )
            ):
                connect_ws_streams_needed = True
                registered_venues.add(venue)

        async def connect_registered_ws_streams() -> None:
            nonlocal connect_ws_streams_needed
            if not connect_ws_streams_needed:
                return
            connected = await self.ctx.l2_data_plane.connect_ws_streams()
            self.ctx.journal.append(
                "runtime.local_l2_dynamic_ws_started",
                {
                    "registered_stream_count": registered_total,
                    "connected_stream_count": connected,
                    "venues": sorted(registered_venues),
                    "ts_ms": wall_clock_now_ms(),
                },
            )
            connect_ws_streams_needed = False

        for c in candidates:
            sym = getattr(c, 'symbol', '')
            for ven_str in (getattr(c, 'long_venue', ''), getattr(c, 'short_venue', '')):
                if not ven_str or not sym:
                    continue
                key = LocalL2BookKey(venue=ven_str, symbol=str(sym))
                tracked_keys.add(key)
                pool_by_key.setdefault(key, L2PoolAssignment.HOT_EXEC)
                desired_pool = pool_by_key.get(key, L2PoolAssignment.HOT_EXEC)
                # Skip if already active
                book = self.ctx.local_l2_runtime.get_book(ven_str, sym)
                if book is not None:
                    self.ctx.local_l2_runtime.assign(
                        ven_str, sym, desired_pool, now_ms=now_ms,
                    )
                    if book.status == L2BookStatus.HOT:
                        stale = book.is_stale(stale_after_ms, now_ms)
                        crossed = book.has_crossed_book()
                        if not stale and not crossed:
                            if hot_book_needs_ws_lifecycle_attention(ven_str, str(sym)):
                                ensure_hot_ws_lifecycle(ven_str, str(sym))
                            continue
                        book.transition_to_rebuilding(now_ms)
                        book.fault_reason = (
                            "crossed_or_locked_book"
                            if crossed and not stale
                            else "stale_hot_book"
                        )
                    elif book.status == L2BookStatus.BOOTSTRAPPING:
                        continue
                needed.setdefault(ven_str, set()).add(sym)

        for position in getattr(self.ctx.state, "open_positions", {}).values():
            sym = getattr(position, "symbol", "")
            remember_key(getattr(position, "long_venue", ""), sym, L2PoolAssignment.RETAINED)
            remember_key(getattr(position, "short_venue", ""), sym, L2PoolAssignment.RETAINED)

        for pending in getattr(self.ctx.state, "pending_entries", {}).values():
            sym = getattr(pending, "symbol", "")
            require_l2_key(
                getattr(pending, "long_venue", ""),
                sym,
                L2PoolAssignment.HOT_EXEC,
                pending_entry=True,
            )
            require_l2_key(
                getattr(pending, "short_venue", ""),
                sym,
                L2PoolAssignment.HOT_EXEC,
                pending_entry=True,
            )

        for pending_close in getattr(self.ctx.state, "pending_passive_closes", {}).values():
            position = getattr(pending_close, "position_snapshot", None)
            if position is None:
                continue
            sym = getattr(position, "symbol", "")
            remember_key(getattr(position, "long_venue", ""), sym, L2PoolAssignment.HOT_EXEC)
            remember_key(getattr(position, "short_venue", ""), sym, L2PoolAssignment.HOT_EXEC)

        if not needed:
            await connect_registered_ws_streams()
            self.ctx.l2_data_plane.prune_untracked_books(
                tracked_keys,
                now_ms,
                retained_max_age_ms=max(stale_after_ms, 300_000),
            )
            return

        per_venue_budget = max(self.ctx.config.strategy.local_l2_hot_exec_per_venue_budget, 1)
        from lightfee.marketdata.local_l2_venues import get_venue_rules

        for ven_str, symbols in needed.items():
            # Limit per venue budget (V1: take(per_venue_budget))
            pending_symbols = [
                symbol
                for symbol in sorted(symbols)
                if LocalL2BookKey(venue=ven_str, symbol=symbol) in pending_l2_keys
            ]
            budgeted_symbols = [
                symbol for symbol in sorted(symbols) if symbol not in pending_symbols
            ][:per_venue_budget]
            symbols_list = list(dict.fromkeys([*pending_symbols, *budgeted_symbols]))
            if not symbols_list:
                continue

            try:
                from lightfee.core.domain import Venue as VenueEnum
                ven = VenueEnum.from_str(ven_str)
                adapter = self.get_venue_adapter(ven) if ven in self.ctx.venue_adapters else None
            except (ValueError, KeyError):
                adapter = None
            if adapter is None or not hasattr(adapter, 'fetch_l2_snapshot'):
                continue

            # Ensure books exist
            filtered_symbols = await self._filter_symbols_supported_by_venue(
                ven,
                adapter,
                symbols_list,
                skip_event_kind="runtime.local_l2_symbol_skipped",
            )
            filtered_pending = [
                symbol
                for symbol in filtered_symbols
                if LocalL2BookKey(venue=ven_str, symbol=symbol) in pending_l2_keys
            ]
            filtered_budgeted = [
                symbol for symbol in filtered_symbols if symbol not in filtered_pending
            ][:per_venue_budget]
            symbols_list = list(dict.fromkeys([*filtered_pending, *filtered_budgeted]))
            if not symbols_list:
                continue

            for sym in symbols_list:
                rules = get_venue_rules(ven_str)
                key = LocalL2BookKey(venue=ven_str, symbol=sym)
                desired_pool = pool_by_key.get(key, L2PoolAssignment.HOT_EXEC)
                book = self.ctx.local_l2_runtime.ensure_book(ven_str, sym)
                self.ctx.local_l2_runtime.assign(
                    ven_str, sym, desired_pool, now_ms=now_ms,
                )
                book.max_depth = rules.default_depth
                book.max_sequence_gap = rules.max_sequence_gap
                if book.status == L2BookStatus.COLD:
                    book.transition_to_bootstrapping(now_ms)

            if self.ctx.config.strategy.local_l2_ws_enabled:
                stream_state_fn = getattr(self.ctx.l2_data_plane, "ws_stream_state", None)
                before_states = (
                    {
                        sym: stream_state_fn(ven_str, sym)
                        for sym in symbols_list
                    }
                    if callable(stream_state_fn)
                    else {}
                )
                registered = self.ctx.l2_data_plane.start_ws_streams(
                    ven_str, symbols_list, adapter=adapter,
                )
                after_states = (
                    {
                        sym: stream_state_fn(ven_str, sym)
                        for sym in symbols_list
                    }
                    if callable(stream_state_fn)
                    else {}
                )
                if registered > 0:
                    registered_total += registered
                disconnected_registered = any(
                    bool(state.get("registered")) and not bool(state.get("connected"))
                    for state in [*before_states.values(), *after_states.values()]
                )
                if registered > 0 or disconnected_registered:
                    registered_venues.add(ven_str)
                    connect_ws_streams_needed = True

            # Start background bootstrap worker
            bs_batch = self.ctx.config.strategy.local_l2_bootstrap_batch_size
            bs_jitter = self.ctx.config.strategy.local_l2_bootstrap_jitter_ms
            bs_retry = self.ctx.config.strategy.local_l2_bootstrap_retry_backoff_ms
            self.ctx.l2_data_plane.start_background_bootstrap(
                venue=ven_str,
                symbols=symbols_list,
                adapter=adapter,
                batch_size=bs_batch,
                jitter_ms=bs_jitter,
                retry_backoff_ms=bs_retry,
            )

        await connect_registered_ws_streams()

        self.ctx.l2_data_plane.prune_untracked_books(
            tracked_keys,
            now_ms,
            retained_max_age_ms=max(stale_after_ms, 300_000),
        )

    async def _ensure_entry_bbo_active_for_candidates(
        self,
        candidates,
        now_ms: int,
    ) -> None:
        """Start independent per-venue BBO streams for entry candidates.

        This is separate from LocalL2Runtime: it does not create books, bootstrap
        snapshots, replay deltas, or update entry L2 sessions.
        """
        baseline_keys = set(
            getattr(self.ctx, "_single_process_entry_bbo_baseline_keys", set()) or set()
        )
        if not self._entry_readiness_provider_uses_ws_bbo():
            self._entry_bbo_subscription_budgeted_keys = set(baseline_keys)
            self._entry_bbo_subscription_budget_excluded_keys = set()
            self._entry_bbo_subscription_per_venue_budget = (
                max(self.ctx.config.strategy.entry_ws_bbo_per_venue_budget, 1)
                if baseline_keys
                else 0
            )
            reconcile_streams = getattr(
                self.ctx.ws_bbo_data_plane,
                "reconcile_ws_streams",
                None,
            )
            if callable(reconcile_streams):
                await reconcile_streams(baseline_keys, per_client_timeout_s=0.05)
            self.ctx.ws_bbo_data_plane.prune_untracked_quotes(
                baseline_keys,
                now_ms,
                retained_max_age_ms=300_000,
            )
            return
        if self.ctx.config.runtime.mode == "paper" and not baseline_keys:
            self._entry_bbo_subscription_budgeted_keys = set()
            self._entry_bbo_subscription_budget_excluded_keys = set()
            self._entry_bbo_subscription_per_venue_budget = 0
            reconcile_streams = getattr(
                self.ctx.ws_bbo_data_plane,
                "reconcile_ws_streams",
                None,
            )
            if callable(reconcile_streams):
                await reconcile_streams(set(), per_client_timeout_s=0.05)
            return

        needed: dict[str, list[str]] = {}
        seen_by_venue: dict[str, set[str]] = {}
        tracked_keys: set[tuple[str, str]] = set()
        sticky_warm_until_ms: dict[tuple[str, str], int] = dict(
            getattr(self.ctx, "_entry_bbo_sticky_warm_until_ms", {}) or {}
        )
        sticky_ttl_ms = max(
            int(
                getattr(
                    self.ctx.config.strategy,
                    "entry_ws_bbo_sticky_warm_ms",
                    120_000,
                )
                or 120_000
            ),
            self._entry_quote_lease_max_age_ms(),
        )
        retained_sticky_keys = {
            key: expires_at
            for key, expires_at in sticky_warm_until_ms.items()
            if int(expires_at or 0) > now_ms
        }
        for candidate in list(candidates or []):
            symbol = str(getattr(candidate, "symbol", "") or "").strip().upper()
            if not symbol:
                continue
            for raw_venue in (
                getattr(candidate, "long_venue", ""),
                getattr(candidate, "short_venue", ""),
            ):
                venue = str(raw_venue or "").strip().lower()
                if not venue:
                    continue
                seen = seen_by_venue.setdefault(venue, set())
                if symbol not in seen:
                    needed.setdefault(venue, []).append(symbol)
                    seen.add(symbol)
                tracked_keys.add((venue, symbol))
        current_tracked_keys = set(tracked_keys)
        for venue, symbol in sorted(retained_sticky_keys):
            seen = seen_by_venue.setdefault(venue, set())
            if symbol not in seen:
                needed.setdefault(venue, []).append(symbol)
                seen.add(symbol)
            tracked_keys.add((venue, symbol))
        sticky_warm_until_ms = {
            **retained_sticky_keys,
            **{
                key: now_ms + sticky_ttl_ms
                for key in current_tracked_keys
            },
        }
        setattr(self.ctx, "_entry_bbo_sticky_warm_until_ms", sticky_warm_until_ms)

        if not needed:
            self._entry_bbo_subscription_budgeted_keys = set(baseline_keys)
            self._entry_bbo_subscription_budget_excluded_keys = set()
            self._entry_bbo_subscription_per_venue_budget = (
                max(self.ctx.config.strategy.entry_ws_bbo_per_venue_budget, 1)
                if baseline_keys
                else 0
            )
            reconcile_streams = getattr(
                self.ctx.ws_bbo_data_plane,
                "reconcile_ws_streams",
                None,
            )
            if callable(reconcile_streams):
                await reconcile_streams(baseline_keys, per_client_timeout_s=0.05)
            self.ctx.ws_bbo_data_plane.prune_untracked_quotes(
                tracked_keys | baseline_keys,
                now_ms,
                retained_max_age_ms=300_000,
            )
            return

        per_venue_budget = max(self.ctx.config.strategy.entry_ws_bbo_per_venue_budget, 1)
        budgeted_keys: set[tuple[str, str]] = set()
        budget_excluded_keys: set[tuple[str, str]] = set()
        for venue_str, symbols in needed.items():
            venue_symbols = list(symbols)
            for symbol in venue_symbols[:per_venue_budget]:
                budgeted_keys.add((venue_str, symbol))
            for symbol in venue_symbols[per_venue_budget:]:
                budget_excluded_keys.add((venue_str, symbol))
        tracked_keys |= baseline_keys
        budgeted_keys |= baseline_keys
        budget_excluded_keys -= baseline_keys
        self._entry_bbo_subscription_budgeted_keys = budgeted_keys
        self._entry_bbo_subscription_budget_excluded_keys = budget_excluded_keys
        self._entry_bbo_subscription_per_venue_budget = per_venue_budget

        # Reconcile candidate replacements before registering them. The
        # single-process source baseline remains tracked independently.
        reconcile_streams = getattr(
            self.ctx.ws_bbo_data_plane,
            "reconcile_ws_streams",
            None,
        )
        if callable(reconcile_streams):
            await reconcile_streams(
                budgeted_keys,
                per_client_timeout_s=0.05,
            )

        registered_total = 0
        registered_venues: set[str] = set()
        for venue_str, symbols in needed.items():
            symbols_list = list(symbols)[:per_venue_budget]
            if not symbols_list:
                continue
            adapter = None
            venue_enum = None
            try:
                venue_enum = Venue.from_str(venue_str)
                adapter = (
                    self.get_venue_adapter(venue_enum)
                    if venue_enum in self.ctx.venue_adapters
                    else None
                )
            except (ValueError, KeyError):
                adapter = None

            if adapter is not None and venue_enum is not None:
                symbols_list = await self._filter_symbols_supported_by_venue(
                    venue_enum,
                    adapter,
                    symbols_list,
                    skip_event_kind="runtime.ws_bbo_symbol_skipped",
                )
            if not symbols_list:
                continue

            registered = self.ctx.ws_bbo_data_plane.start_ws_streams(
                venue_str,
                symbols_list,
                adapter=adapter,
            )
            if registered > 0:
                registered_total += registered
                registered_venues.add(venue_str)

        if registered_total > 0:
            connected = await self.ctx.ws_bbo_data_plane.connect_ws_streams()
            self.ctx.journal.append(
                "runtime.ws_bbo_dynamic_ws_started",
                {
                    "registered_stream_count": registered_total,
                    "connected_stream_count": connected,
                    "venues": sorted(registered_venues),
                    "ts_ms": wall_clock_now_ms(),
                },
            )

        self.ctx.ws_bbo_data_plane.prune_untracked_quotes(
            tracked_keys,
            now_ms,
            retained_max_age_ms=300_000,
        )

    async def _ensure_single_process_entry_bbo_source_active(
        self,
        now_ms: int,
    ) -> None:
        """Maintain the uncapped WebSocket BBO source universe for entry input."""
        uses_single_process_entry = getattr(
            self.ctx,
            "_uses_single_process_entry_input",
            None,
        )
        if not callable(uses_single_process_entry) or not uses_single_process_entry():
            return

        symbols = sorted(
            {
                str(symbol or "").strip().upper()
                for symbol in self.ctx.config.symbols
                if str(symbol or "").strip()
            }
        )
        venue_symbols: dict[str, list[str]] = {}
        for venue_config in self.ctx.config.venues:
            venue = str(getattr(venue_config, "venue", "") or "").strip().lower()
            if venue:
                venue_symbols.setdefault(venue, symbols)
        baseline_keys = {
            (venue, symbol)
            for venue, configured_symbols in venue_symbols.items()
            for symbol in configured_symbols
        }
        setattr(self.ctx, "_single_process_entry_bbo_baseline_keys", baseline_keys)

        reconcile_streams = getattr(
            self.ctx.ws_bbo_data_plane,
            "reconcile_ws_streams",
            None,
        )
        if callable(reconcile_streams):
            await reconcile_streams(baseline_keys, per_client_timeout_s=0.05)

        registered_total = 0
        registered_venues: set[str] = set()
        for venue, configured_symbols in venue_symbols.items():
            registered = self.ctx.ws_bbo_data_plane.start_ws_streams(
                venue,
                configured_symbols,
            )
            if registered > 0:
                registered_total += registered
                registered_venues.add(venue)

        connected = await self.ctx.ws_bbo_data_plane.connect_ws_streams()
        journal = getattr(self.ctx, "journal", None)
        if registered_total > 0 and getattr(journal, "_file", None) is not None:
            journal.append(
                "runtime.single_process_entry_ws_bbo_source_started",
                {
                    "registered_stream_count": registered_total,
                    "connected_stream_count": connected,
                    "venues": sorted(registered_venues),
                    "symbol_count": len(symbols),
                    "ts_ms": now_ms,
                },
            )
        self.ctx.ws_bbo_data_plane.prune_untracked_quotes(
            baseline_keys,
            now_ms,
            retained_max_age_ms=300_000,
        )

    @staticmethod
    def _entry_quote_truth_empty_stats() -> dict[str, Any]:
        return {
            "candidate_scope": "",
            "candidate_count": 0,
            "target_count": 0,
            "all_target_count": 0,
            "must_resolve_count": 0,
            "budgeted_target_count": 0,
            "budget_exhausted_count": 0,
            "budget_excluded_without_rest_count": 0,
            "skipped_unbudgeted_count": 0,
            "skipped_untracked_count": 0,
            "cache_initial_hit_count": 0,
            "cache_wait_hit_count": 0,
            "ws_resolved_count": 0,
            "rest_attempt_count": 0,
            "rest_resolved_count": 0,
            "rest_failed_count": 0,
            "rest_throttled_count": 0,
            "entry_evidence_deadline_exceeded_count": 0,
            "superseded_by_ready_candidate_count": 0,
            "deferred_count": 0,
            "wait_budget_ms": 0,
            "wait_elapsed_ms": 0,
            "resolved_count": 0,
            "failed_count": 0,
            "sources": Counter(),
            "top_quote_blocker_buckets": Counter(),
            "quote_lease_failure_counts": Counter(),
        }

    def _entry_quote_truth_record_last_scan(self, stats: dict[str, Any]) -> None:
        evidence_role = str(stats.get("evidence_role") or "entry_execution")
        if evidence_role == "prewarm_only":
            self.ctx.state.last_scan["quote_prewarm_extra_candidate_scope"] = str(
                stats.get("candidate_scope", "") or ""
            )
            self.ctx.state.last_scan["quote_prewarm_extra_candidate_count"] = int(
                stats.get("candidate_count", 0) or 0
            )
            self.ctx.state.last_scan["quote_prewarm_extra_target_count"] = int(
                stats.get("target_count", 0) or 0
            )
            self.ctx.state.last_scan["quote_prewarm_extra_resolved_count"] = int(
                stats.get("resolved_count", 0) or 0
            )
            self.ctx.state.last_scan["quote_prewarm_extra_failed_count"] = int(
                stats.get("failed_count", 0) or 0
            )
            self.ctx.state.last_scan["quote_prewarm_extra_skipped_untracked_count"] = int(
                stats.get("skipped_untracked_count", 0) or 0
            )
            return
        self.ctx.state.last_scan["quote_revalidate_candidate_scope"] = str(
            stats.get("candidate_scope", "") or ""
        )
        self.ctx.state.last_scan["quote_revalidate_candidate_count"] = int(
            stats.get("candidate_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_revalidate_all_target_count"] = int(
            stats.get("all_target_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_revalidate_target_count"] = int(
            stats.get("target_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_revalidate_skipped_untracked_count"] = int(
            stats.get("skipped_untracked_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_revalidate_resolved_count"] = int(
            stats.get("resolved_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_revalidate_failed_count"] = int(
            stats.get("failed_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_truth_must_resolve_count"] = int(
            stats.get("must_resolve_count", stats.get("target_count", 0)) or 0
        )
        self.ctx.state.last_scan["quote_truth_resolved_count"] = int(
            stats.get("resolved_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_truth_failed_count"] = int(
            stats.get("failed_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_truth_ws_resolved_count"] = int(
            stats.get("ws_resolved_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_truth_rest_resolved_count"] = int(
            stats.get("rest_resolved_count", 0) or 0
        )
        self.ctx.state.last_scan["quote_truth_rest_throttled_count"] = int(
            stats.get("rest_throttled_count", 0) or 0
        )
        self.ctx.state.last_scan["entry_evidence_deadline_exceeded_count"] = int(
            stats.get("entry_evidence_deadline_exceeded_count", 0) or 0
        )
        self.ctx.state.last_scan[
            "quote_revalidate_superseded_count"
        ] = int(stats.get("superseded_by_ready_candidate_count", 0) or 0)
        self.ctx.state.last_scan["budget_excluded_without_rest_count"] = int(
            stats.get("budget_excluded_without_rest_count", 0) or 0
        )
        sources = stats.get("sources", Counter())
        self.ctx.state.last_scan["quote_revalidate_sources"] = dict(
            sorted((str(k), int(v)) for k, v in sources.items())
        )
        buckets = stats.get("top_quote_blocker_buckets", Counter())
        self.ctx.state.last_scan["top_quote_blocker_buckets"] = dict(
            sorted((str(k), int(v)) for k, v in buckets.items())
        )
        lease_buckets = stats.get("quote_lease_failure_counts", Counter())
        self.ctx.state.last_scan["quote_lease_failure_counts"] = dict(
            sorted((str(k), int(v)) for k, v in lease_buckets.items())
        )

    def _entry_quote_probe_diagnostics_enabled(self) -> bool:
        return bool(self.ctx.config.runtime.debug_journal_diagnostics_enabled)

    def _emit_entry_quote_revalidate_probe(
        self,
        *,
        stats: dict[str, Any],
        candidate_count: int,
        now_ms: int,
    ) -> None:
        if not self._entry_quote_probe_diagnostics_enabled():
            return
        payload = {
            "enabled": True,
            "candidate_scope": str(stats.get("candidate_scope", "") or ""),
            "candidate_count": int(candidate_count or 0),
            "all_target_count": int(stats.get("all_target_count", 0) or 0),
            "target_count": int(stats.get("target_count", 0) or 0),
            "must_resolve_count": int(stats.get("must_resolve_count", 0) or 0),
            "budgeted_target_count": int(stats.get("budgeted_target_count", 0) or 0),
            "budget_exhausted_count": int(stats.get("budget_exhausted_count", 0) or 0),
            "budget_excluded_without_rest_count": int(
                stats.get("budget_excluded_without_rest_count", 0) or 0
            ),
            "skipped_unbudgeted_count": int(stats.get("skipped_unbudgeted_count", 0) or 0),
            "skipped_untracked_count": int(stats.get("skipped_untracked_count", 0) or 0),
            "cache_initial_hit_count": int(stats.get("cache_initial_hit_count", 0) or 0),
            "cache_wait_hit_count": int(stats.get("cache_wait_hit_count", 0) or 0),
            "ws_resolved_count": int(stats.get("ws_resolved_count", 0) or 0),
            "rest_attempt_count": int(stats.get("rest_attempt_count", 0) or 0),
            "rest_resolved_count": int(stats.get("rest_resolved_count", 0) or 0),
            "rest_failed_count": int(stats.get("rest_failed_count", 0) or 0),
            "resolved_count": int(stats.get("resolved_count", 0) or 0),
            "failed_count": int(stats.get("failed_count", 0) or 0),
            "wait_budget_ms": int(stats.get("wait_budget_ms", 0) or 0),
            "wait_elapsed_ms": int(stats.get("wait_elapsed_ms", 0) or 0),
            "resolved_sources": dict(
                sorted((str(k), int(v)) for k, v in stats.get("sources", Counter()).items())
            ),
            "top_quote_blocker_buckets": dict(
                sorted(
                    (str(k), int(v))
                    for k, v in stats.get("top_quote_blocker_buckets", Counter()).items()
                )
            ),
            "ts_ms": now_ms,
        }
        self._append_runtime_diagnostic_event(
            "runtime.entry_quote_revalidate_probe",
            payload,
            now_ms=now_ms,
            key_parts=("entry_quote_revalidate",),
            interval_ms=1000,
        )

    def _entry_quote_truth_overlay_quote(
        self,
        overlay: dict[tuple[str, str], Any] | None,
        venue: str,
        symbol: str,
    ) -> Any | None:
        if not overlay:
            return None
        return overlay.get((str(venue or "").lower(), str(symbol or "").upper()))

    def _entry_quote_truth_market_quotes(
        self,
        market_quotes: Any,
        overlay: dict[tuple[str, str], Any] | None,
    ) -> dict:
        merged = dict(market_quotes or {})
        for (venue, symbol), quote in (overlay or {}).items():
            key = f"{venue}:{symbol}"
            base_quote = merged.get(key) or merged.get((venue, symbol))
            if base_quote is None:
                merged[key] = quote
                continue
            # REST/WS BBO truth intentionally contains only executable price
            # fields. Preserve the authenticated contract/OI metadata from
            # the installed snapshot while replacing its market observation.
            enriched = copy.copy(base_quote)
            for field in (
                "venue",
                "symbol",
                "bid",
                "ask",
                "bid_size",
                "ask_size",
                "observed_at_ms",
                "received_at_ms",
                "exchange_event_at_ms",
                "source",
            ):
                if hasattr(quote, field):
                    setattr(enriched, field, getattr(quote, field))
            merged[key] = enriched
        return merged

    def _entry_quote_truth_price_hint(
        self,
        candidate: Any,
        *,
        price_hints: dict[str, float],
        overlay: dict[tuple[str, str], Any] | None,
    ) -> float:
        symbol = str(getattr(candidate, "symbol", "") or "").upper()
        mids: list[float] = []
        for venue_attr in ("long_venue", "short_venue"):
            venue = str(getattr(candidate, venue_attr, "") or "").lower()
            quote = self._entry_quote_truth_overlay_quote(overlay, venue, symbol)
            if quote is None:
                continue
            bid = float(getattr(quote, "bid", 0.0) or 0.0)
            ask = float(getattr(quote, "ask", 0.0) or 0.0)
            if bid > 0.0 and ask > bid:
                mids.append((bid + ask) / 2.0)
        if mids:
            return sum(mids) / len(mids)
        return float(price_hints.get(symbol, 0.0) or 0.0)

    def _entry_quote_revalidate_need(
        self,
        *,
        snapshot,
        quote: Any,
        now_ms: int,
        fallback_source: str,
    ) -> tuple[bool, str, dict[str, Any]]:
        if quote is None:
            return False, "", {}
        observed_at_ms = self._snapshot_quote_observed_at_ms(snapshot, quote)
        direct_observed_at_ms = self._snapshot_quote_direct_observed_at_ms(quote)
        age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
        budget_ms = self._snapshot_domain_budget_ms("quote")
        bid = float(getattr(quote, "bid", 0.0) or 0.0)
        ask = float(getattr(quote, "ask", 0.0) or 0.0)
        if direct_observed_at_ms <= 0 or bid <= 0.0 or ask <= 0.0 or ask <= bid:
            return False, "", {}
        evidence = {
            "sidecar_source": self._snapshot_quote_source(quote),
            "sidecar_observed_at_ms": observed_at_ms,
            "sidecar_age_ms": age_ms,
            "sidecar_budget_ms": budget_ms,
            "fallback_source": fallback_source,
        }
        if age_ms > budget_ms:
            return True, "quote_stale", evidence
        if fallback_source == "last_good_sidecar":
            return True, "last_good_sidecar", evidence
        # A sidecar BBO is a ranking seed, never final execution evidence.
        # Every eligible-frontier leg must obtain a current WS/REST overlay so repricing
        # and final ranking use the same quotes that authorize submit.
        return True, "entry_final_revalidation", evidence

    def _entry_quote_revalidate_targets(
        self,
        candidates: list,
        *,
        snapshot,
        now_ms: int,
    ) -> list[dict[str, Any]]:
        quote_lookup = self._market_quote_lookup(getattr(snapshot, "quotes", {}) or {})
        fallback_source = self._snapshot_fallback_source(snapshot)
        targets: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for rank, candidate in enumerate(list(candidates or []), start=1):
            symbol = str(getattr(candidate, "symbol", "") or "").upper()
            if not symbol:
                continue
            for venue_attr in ("long_venue", "short_venue"):
                venue = str(getattr(candidate, venue_attr, "") or "").lower()
                if not venue:
                    continue
                key = (venue, symbol)
                if key in seen:
                    continue
                quote = quote_lookup.get(key)
                needs, reason, evidence = self._entry_quote_revalidate_need(
                    snapshot=snapshot,
                    quote=quote,
                    now_ms=now_ms,
                    fallback_source=fallback_source,
                )
                if not needs:
                    continue
                seen.add(key)
                targets.append({
                    "venue": venue,
                    "symbol": symbol,
                    "candidate_rank": rank,
                    "pair_id": self._candidate_pair_id(candidate),
                    "reason": reason,
                    **evidence,
                })
        return targets

    def _entry_quote_truth_fresh_quote(
        self,
        venue: str,
        symbol: str,
        *,
        now_ms: int,
    ) -> Any | None:
        cache = self.ctx.ws_bbo_cache
        if cache is None:
            return None
        budget_ms = self._entry_quote_lease_max_age_ms()
        if budget_ms <= 0:
            return None
        return cache.fresh_quote(venue, symbol, now_ms=now_ms, max_age_ms=budget_ms)

    def _entry_quote_truth_refresher(self) -> Any:
        refresher = getattr(self, "ws_bbo_rest_refresher", None)
        if refresher is not None:
            return refresher
        from lightfee.marketdata.ws_bbo import RestTopBookQuoteRefresher

        refresher = RestTopBookQuoteRefresher(timeout_ms=750)
        setattr(self, "ws_bbo_rest_refresher", refresher)
        return refresher

    def _entry_quote_truth_accept_quote(
        self,
        quote: Any,
        *,
        now_ms: int,
    ) -> bool:
        return self._entry_quote_truth_reject_reason(quote, now_ms=now_ms) == ""

    def _schedule_entry_quote_rewarm_after_rest_stale(
        self,
        target: dict[str, Any],
        *,
        now_ms: int,
    ) -> dict[str, Any] | None:
        venue = str(target.get("venue") or "").strip().lower()
        symbol = str(target.get("symbol") or "").strip().upper()
        if not venue or not symbol:
            return None
        key = (venue, symbol)
        budgets = phase_budgets_from_strategy(self.ctx.config.strategy)
        hard_ms = int(budgets["quote_rewarm"].hard_ms or 0)
        cooldown_until_ms: dict[tuple[str, str], int] = dict(
            getattr(self.ctx, "_entry_quote_rewarm_cooldown_until_ms", {}) or {}
        )
        if int(cooldown_until_ms.get(key, 0) or 0) > now_ms:
            return None
        scheduled_at_ms: dict[tuple[str, str], int] = dict(
            getattr(self.ctx, "_entry_quote_rewarm_scheduled_at_ms", {}) or {}
        )
        first_scheduled_at_ms = int(scheduled_at_ms.get(key, 0) or 0)
        if first_scheduled_at_ms > 0 and hard_ms > 0:
            age_ms = max(now_ms - first_scheduled_at_ms, 0)
            if age_ms >= hard_ms:
                cooldown_ttl_ms = max(
                    int(
                        getattr(
                            self.ctx.config.strategy,
                            "entry_ws_bbo_sticky_warm_ms",
                            120_000,
                        )
                        or 120_000
                    ),
                    self._entry_quote_lease_max_age_ms(),
                    hard_ms,
                )
                blocked_until_ms = now_ms + cooldown_ttl_ms
                cooldown_until_ms[key] = blocked_until_ms
                scheduled_at_ms.pop(key, None)
                setattr(
                    self.ctx,
                    "_entry_quote_rewarm_cooldown_until_ms",
                    cooldown_until_ms,
                )
                setattr(
                    self.ctx,
                    "_entry_quote_rewarm_scheduled_at_ms",
                    scheduled_at_ms,
                )
                handoff = quote_rewarm_handoff_contract(
                    phase="quote_rewarm",
                    status="hard_over_budget",
                    configured_action=budgets["quote_rewarm"].action,
                    terminal_kind="runtime.entry_quote_rewarm_terminal_stale",
                )
                payload = {
                    "venue": venue,
                    "symbol": symbol,
                    "pair_id": str(target.get("pair_id") or ""),
                    "candidate_rank": int(target.get("candidate_rank") or 0),
                    "reason_bucket": "rest_resolved_but_stale",
                    "reason_family": "rest_invalid_quote",
                    "scheduled_at_ms": first_scheduled_at_ms,
                    "age_ms": age_ms,
                    "hard_ms": hard_ms,
                    "blocked_until_ms": blocked_until_ms,
                    "cooldown_ttl_ms": cooldown_ttl_ms,
                    "action_taken": handoff.get(
                        "action_taken",
                        budgets["quote_rewarm"].action,
                    ),
                    "action_evidence_kind": handoff.get(
                        "action_evidence_kind",
                        "runtime.entry_quote_rewarm_terminal_stale",
                    ),
                    "source": "entry_quote_truth",
                    "ts_ms": now_ms,
                }
                self.ctx.journal.append(
                    "runtime.entry_quote_rewarm_terminal_stale",
                    payload,
                )
                return payload
        sticky_ttl_ms = max(
            int(
                getattr(
                    self.ctx.config.strategy,
                    "entry_ws_bbo_sticky_warm_ms",
                    120_000,
                )
                or 120_000
            ),
            self._entry_quote_lease_max_age_ms(),
        )
        sticky_warm_until_ms: dict[tuple[str, str], int] = dict(
            getattr(self.ctx, "_entry_bbo_sticky_warm_until_ms", {}) or {}
        )
        expires_at_ms = now_ms + sticky_ttl_ms
        sticky_warm_until_ms[(venue, symbol)] = expires_at_ms
        setattr(self.ctx, "_entry_bbo_sticky_warm_until_ms", sticky_warm_until_ms)
        scheduled_at_ms.setdefault(key, now_ms)
        setattr(self.ctx, "_entry_quote_rewarm_scheduled_at_ms", scheduled_at_ms)
        payload = {
            "venue": venue,
            "symbol": symbol,
            "pair_id": str(target.get("pair_id") or ""),
            "candidate_rank": int(target.get("candidate_rank") or 0),
            "reason_bucket": "rest_resolved_but_stale",
            "reason_family": "rest_invalid_quote",
            "sticky_warm_until_ms": expires_at_ms,
            "sticky_ttl_ms": sticky_ttl_ms,
            "rest_quote_observed_at_ms": target.get("rest_quote_observed_at_ms"),
            "rest_quote_received_at_ms": target.get("rest_quote_received_at_ms"),
            "rest_quote_exchange_event_at_ms": target.get(
                "rest_quote_exchange_event_at_ms"
            ),
            "rest_quote_age_ms": target.get("rest_quote_age_ms"),
            "quote_validation_reject_reason": str(
                target.get("quote_validation_reject_reason") or ""
            ),
            "source": "entry_quote_truth",
            "ts_ms": now_ms,
        }
        self.ctx.journal.append(
            "runtime.entry_quote_rewarm_scheduled_after_rest_stale",
            payload,
        )
        return payload

    def _entry_quote_truth_reject_reason(
        self,
        quote: Any,
        *,
        now_ms: int,
    ) -> str:
        if quote is None:
            return "missing_quote"
        observed_at_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
        age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
        bid = float(getattr(quote, "bid", 0.0) or 0.0)
        ask = float(getattr(quote, "ask", 0.0) or 0.0)
        if observed_at_ms <= 0:
            return "missing_observed_at"
        if observed_at_ms > now_ms:
            return "timestamp_after_now"
        if age_ms > self._entry_quote_lease_max_age_ms():
            return "stale"
        if bid <= 0.0 or ask <= bid:
            return "invalid_bid_ask"
        return ""

    async def _entry_quote_revalidate_for_candidates(
        self,
        candidates: list,
        *,
        snapshot,
        now_ms: int,
        candidate_scope: str = "",
        skipped_untracked_count: int = 0,
        evidence_role: str = "entry_execution",
        activation_candidates: list | None = None,
        evidence_coordinator: dict[str, Any] | None = None,
    ) -> tuple[dict[tuple[str, str], Any], dict[str, Any]]:
        overlay: dict[tuple[str, str], Any] = {}
        if evidence_coordinator is not None:
            # Share the incrementally accepted quote view with the runtime's
            # pure candidate validator.  The dict identity stays stable while
            # target completions arrive on this event loop.
            evidence_coordinator["quote_overlay"] = overlay
        stats = self._entry_quote_truth_empty_stats()
        loop = asyncio.get_running_loop()
        evidence_started_monotonic = loop.time()
        stats["candidate_scope"] = candidate_scope
        stats["evidence_role"] = evidence_role
        stats["candidate_count"] = len(candidates or [])
        stats["skipped_untracked_count"] = max(int(skipped_untracked_count or 0), 0)
        if not candidates:
            self._entry_quote_truth_record_last_scan(stats)
            if evidence_role != "prewarm_only":
                self._emit_entry_quote_revalidate_probe(
                    stats=stats,
                    candidate_count=len(candidates or []),
                    now_ms=now_ms,
                )
            return overlay, stats

        activation_budget_s = max(
            min(0.100, 0.750 - (loop.time() - evidence_started_monotonic)),
            0.0,
        )
        if activation_budget_s > 0.0 and activation_candidates != []:
            try:
                await asyncio.wait_for(
                    self._call_ensure_entry_bbo_active_for_candidates(
                        candidates
                        if activation_candidates is None
                        else activation_candidates,
                        now_ms,
                    ),
                    timeout=activation_budget_s,
                )
            except asyncio.TimeoutError:
                stats["ws_activation_deadline_exceeded_count"] = len(candidates)
        now_ms = self._current_wall_clock_ms()
        all_targets = self._entry_quote_revalidate_targets(
            candidates,
            snapshot=snapshot,
            now_ms=now_ms,
        )
        stats["all_target_count"] = len(all_targets)
        if not all_targets:
            # Existing sidecar/WS evidence can already satisfy both legs.  It
            # is still a completed quote-domain result and must participate in
            # cross-domain early selection; otherwise a fresh cache waits for
            # an unrelated OI timeout merely because no REST work was needed.
            for candidate_index in range(len(candidates or [])):
                _mark_entry_evidence_domain_state(
                    evidence_coordinator,
                    domain="quote",
                    candidate_index=candidate_index,
                    state="ready",
                )
            self._entry_quote_truth_record_last_scan(stats)
            if evidence_role != "prewarm_only":
                self._emit_entry_quote_revalidate_probe(
                    stats=stats,
                    candidate_count=len(candidates or []),
                    now_ms=now_ms,
                )
            return overlay, stats

        budgeted_keys = set(getattr(self, "_entry_bbo_subscription_budgeted_keys", set()) or set())
        budget_excluded_keys = set(
            getattr(self, "_entry_bbo_subscription_budget_excluded_keys", set()) or set()
        )
        targets: list[dict[str, Any]] = []
        for target in all_targets:
            key = (target["venue"], target["symbol"])
            if key in budget_excluded_keys:
                stats["budget_exhausted_count"] += 1
                target["ws_budget_excluded"] = True
                target["rest_fallback_planned"] = True
                self.ctx.journal.append(
                    "runtime.entry_ws_bbo_top_candidate_rewarm_budget_exhausted",
                    {
                        **target,
                        "outcome": "rest_fallback_planned",
                        "candidate_scope": candidate_scope,
                        "evidence_role": evidence_role,
                        "ts_ms": now_ms,
                    },
                )
            if (
                budgeted_keys
                and key not in budgeted_keys
                and not bool(target.get("ws_budget_excluded"))
            ):
                stats["skipped_unbudgeted_count"] += 1
                continue
            targets.append(target)

        stats["target_count"] = len(targets)
        stats["must_resolve_count"] = len(targets)
        stats["budgeted_target_count"] = sum(
            1 for target in targets if not bool(target.get("ws_budget_excluded"))
        )
        if targets:
            self.ctx.journal.append(
                "runtime.entry_quote_revalidate_targeted",
                {
                    "target_count": len(targets),
                    "targets": targets[:24],
                    "wait_budget_ms": min(self._entry_quote_lease_max_age_ms(), 750),
                    "candidate_scope": candidate_scope,
                    "evidence_role": evidence_role,
                    "ts_ms": now_ms,
                },
            )
            self.ctx.journal.append(
                "runtime.entry_ws_bbo_top_candidate_rewarm_started",
                {
                    "target_count": len(targets),
                    "targets": targets[:24],
                    "candidate_scope": candidate_scope,
                    "evidence_role": evidence_role,
                    "ts_ms": now_ms,
                },
            )

        unresolved: dict[tuple[str, str], dict[str, Any]] = {
            (target["venue"], target["symbol"]): target
            for target in targets
        }

        def collect_fresh_from_cache(stage: str) -> None:
            validation_now_ms = self._current_wall_clock_ms()
            for key, target in list(unresolved.items()):
                quote = self._entry_quote_truth_fresh_quote(
                    target["venue"],
                    target["symbol"],
                    now_ms=validation_now_ms,
                )
                if quote is None:
                    continue
                overlay[key] = quote
                unresolved.pop(key, None)
                if stage == "initial":
                    stats["cache_initial_hit_count"] += 1
                else:
                    stats["cache_wait_hit_count"] += 1
                stats["ws_resolved_count"] += 1

        collect_fresh_from_cache("initial")
        # Keep a small WS handoff window, leaving most of the shared 750ms
        # evidence deadline for concurrent REST fallback.
        wait_budget_ms = min(self._entry_quote_lease_max_age_ms(), 100)
        stats["wait_budget_ms"] = wait_budget_ms
        elapsed_ms = 0
        while unresolved and elapsed_ms < wait_budget_ms:
            await asyncio.sleep(0.05)
            elapsed_ms += 50
            collect_fresh_from_cache("wait")
        stats["wait_elapsed_ms"] = elapsed_ms

        refresher = self._entry_quote_truth_refresher()
        arefresh_quote_result = getattr(refresher, "arefresh_quote_result", None)
        arefresh_quote = getattr(refresher, "arefresh_quote", None)
        refresh_quote_result = getattr(refresher, "refresh_quote_result", None)
        refresh_quote = getattr(refresher, "refresh_quote", None)
        if any(
            callable(method)
            for method in (
                arefresh_quote_result,
                arefresh_quote,
                refresh_quote_result,
                refresh_quote,
            )
        ):
            async def _refresh_target(
                key: tuple[str, str],
                target: dict[str, Any],
            ) -> tuple[tuple[str, str], dict[str, Any], Any, Any, str]:
                refreshed = None
                result = None
                error = ""
                try:
                    request_now_ms = self._current_wall_clock_ms()
                    if callable(arefresh_quote_result):
                        result = await arefresh_quote_result(
                            target["venue"], target["symbol"], now_ms=request_now_ms
                        )
                        refreshed = getattr(result, "quote", None)
                    elif callable(arefresh_quote):
                        refreshed = await arefresh_quote(
                            target["venue"], target["symbol"], now_ms=request_now_ms
                        )
                    elif callable(refresh_quote_result):
                        result = await asyncio.to_thread(
                            refresh_quote_result,
                            target["venue"],
                            target["symbol"],
                            now_ms=request_now_ms,
                        )
                        refreshed = getattr(result, "quote", None)
                    else:
                        refreshed = await asyncio.to_thread(
                            refresh_quote,
                            target["venue"],
                            target["symbol"],
                            now_ms=request_now_ms,
                        )
                except Exception as exc:  # pragma: no cover - defensive telemetry
                    error = f"{type(exc).__name__}: {exc}"[:240]
                return key, target, result, refreshed, error

            # Every complete-frontier target joins this evidence generation.
            # Venue clients retain their own connection-pool and rate-limit
            # concurrency; this layer imposes no candidate-membership cutoff.
            max_in_flight = max(len(unresolved), 1)
            queued_keys = list(unresolved)
            task_by_key: dict[tuple[str, str], asyncio.Task] = {}
            task_key: dict[asyncio.Task, tuple[str, str]] = {}

            def _launch_more() -> None:
                while queued_keys and len(task_key) < max_in_flight:
                    key = queued_keys.pop(0)
                    target = unresolved.get(key)
                    if target is None:
                        continue
                    target["rest_revalidate_attempted"] = True
                    task = asyncio.create_task(_refresh_target(key, target))
                    task_by_key[key] = task
                    task_key[task] = key
                    stats["rest_attempt_count"] += 1

            _launch_more()
            completed_keys: set[tuple[str, str]] = set(overlay)
            required_target_keys = {
                (target["venue"], target["symbol"])
                for target in all_targets
            }
            # A target excluded before REST is already a resolved failure for
            # ranking purposes; it must not keep every lower candidate behind
            # an impossible higher row until the batch deadline.
            completed_keys.update(
                required_target_keys - set(unresolved) - set(overlay)
            )

            def _highest_resolved_candidate_is_ready() -> bool:
                selection_ready = False
                for candidate_index, candidate in enumerate(candidates):
                    symbol = str(
                        getattr(candidate, "symbol", "") or ""
                    ).strip().upper()
                    candidate_required_keys = {
                        (
                            str(getattr(candidate, venue_attr, "") or "")
                            .strip()
                            .lower(),
                            symbol,
                        )
                        for venue_attr in ("long_venue", "short_venue")
                    } & required_target_keys
                    if not candidate_required_keys.issubset(completed_keys):
                        return selection_ready
                    candidate_ready = candidate_required_keys.issubset(
                        set(overlay)
                    )
                    if evidence_coordinator is not None:
                        selection_ready = (
                            _mark_entry_evidence_domain_state(
                                evidence_coordinator,
                                domain="quote",
                                candidate_index=candidate_index,
                                state="ready" if candidate_ready else "failed",
                            )
                            or selection_ready
                        )
                    # Standalone domain refresh has no cross-domain economic
                    # validator, so evidence-ready cannot authorize pruning.
                    # This row is fully resolved but unusable. Continue down
                    # the ranked frontier rather than waiting for it again.
                return selection_ready

            def _apply_completed_quote_refresh(completed_item) -> None:
                if isinstance(completed_item, BaseException):
                    return
                key, target, result, refreshed, refresh_error = completed_item
                if result is not None:
                    rest_outcome = str(getattr(result, "outcome", "") or "")
                    target["rest_outcome"] = rest_outcome
                    target["venue_symbol"] = str(
                        getattr(result, "venue_symbol", "") or ""
                    )
                    target["url"] = str(getattr(result, "url", "") or "")
                    target["endpoint"] = str(
                        getattr(result, "endpoint", "") or "rest_topbook"
                    )
                    target["http_status"] = int(
                        getattr(result, "http_status", 0) or 0
                    )
                    target["body_excerpt"] = str(
                        getattr(result, "body_excerpt", "") or ""
                    )
                    target["attempt_interval_outcome"] = str(
                        getattr(result, "attempt_interval_outcome", "") or ""
                    )
                    target["rest_error"] = str(getattr(result, "error", "") or "")
                    if rest_outcome == "throttled":
                        stats["rest_throttled_count"] += 1
                else:
                    target["rest_outcome"] = (
                        "resolved" if refreshed is not None else "missing_quote"
                    )
                if refresh_error:
                    target["rest_error"] = refresh_error
                    target["rest_outcome"] = "http_error"
                    refreshed = None
                validation_now_ms = max(
                    self._current_wall_clock_ms(),
                    int(getattr(refreshed, "received_at_ms", 0) or 0),
                )
                reject_reason = self._entry_quote_truth_reject_reason(
                    refreshed,
                    now_ms=validation_now_ms,
                )
                if refreshed is not None:
                    observed_at_ms = int(getattr(refreshed, "observed_at_ms", 0) or 0)
                    received_at_ms = int(getattr(refreshed, "received_at_ms", 0) or 0)
                    exchange_event_at_ms = int(
                        getattr(refreshed, "exchange_event_at_ms", 0) or 0
                    )
                    target["rest_quote_observed_at_ms"] = observed_at_ms
                    target["rest_quote_received_at_ms"] = received_at_ms
                    target["rest_quote_exchange_event_at_ms"] = exchange_event_at_ms
                    target["rest_quote_age_ms"] = (
                        max(validation_now_ms - observed_at_ms, 0)
                        if observed_at_ms > 0
                        else None
                    )
                    target["rest_quote_bid"] = float(
                        getattr(refreshed, "bid", 0.0) or 0.0
                    )
                    target["rest_quote_ask"] = float(
                        getattr(refreshed, "ask", 0.0) or 0.0
                    )
                target["quote_validation_reject_reason"] = reject_reason
                if reject_reason:
                    stats["rest_failed_count"] += 1
                    return
                cache = self.ctx.ws_bbo_cache
                if cache is not None and hasattr(cache, "update_quote"):
                    accepted = bool(
                        cache.update_quote(
                            refreshed,
                            now_ms=validation_now_ms,
                            current_max_age_ms=(
                                self._entry_quote_lease_max_age_ms()
                            ),
                        )
                    )
                    if not accepted:
                        # A WS update may have arrived after this REST request
                        # started.  Never return the delayed REST payload as an
                        # execution overlay when cache ordering rejected it.
                        current_quote = self._entry_quote_truth_fresh_quote(
                            target["venue"],
                            target["symbol"],
                            now_ms=validation_now_ms,
                        )
                        if current_quote is not None:
                            target["rest_outcome"] = (
                                "superseded_by_newer_cache_quote"
                            )
                            overlay[key] = current_quote
                            unresolved.pop(key, None)
                            stats["cache_wait_hit_count"] += 1
                            stats["ws_resolved_count"] += 1
                        else:
                            stats["rest_failed_count"] += 1
                        return
                overlay[key] = refreshed
                unresolved.pop(key, None)
                stats["rest_resolved_count"] += 1

            pending = set(task_by_key.values())
            _highest_resolved_candidate_is_ready()
            try:
                while pending:
                    remaining_s = max(
                        0.750 - (loop.time() - evidence_started_monotonic),
                        0.0,
                    )
                    if remaining_s <= 0.0:
                        break
                    done, _still_pending = await asyncio.wait(
                        pending,
                        timeout=remaining_s,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if not done:
                        break
                    pending.difference_update(done)
                    done_list = list(done)
                    completed = await asyncio.gather(
                        *done_list,
                        return_exceptions=True,
                    )
                    for task, completed_item in zip(done_list, completed):
                        completed_keys.add(task_key[task])
                        _apply_completed_quote_refresh(completed_item)
                        task_key.pop(task, None)
                    _highest_resolved_candidate_is_ready()
                    _launch_more()
                    pending.update(task_key)
            except asyncio.CancelledError:
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                raise

            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            remaining_keys = [
                *[task_key[task] for task in pending if task in task_key],
                *queued_keys,
            ]
            if remaining_keys:
                stats["entry_evidence_deadline_exceeded_count"] = len(
                    remaining_keys
                )
                for key in remaining_keys:
                    unresolved[key]["rest_outcome"] = (
                        "entry_evidence_deadline_exceeded"
                    )
                    completed_keys.add(key)
                # A deadline is a terminal result for each affected target,
                # not an unresolved batch. Publish those failed leaves so a
                # lower-ranked candidate whose own legs completed can advance.
                _highest_resolved_candidate_is_ready()
        else:
            for target in unresolved.values():
                if bool(target.get("ws_budget_excluded")):
                    stats["budget_excluded_without_rest_count"] += 1

        for key, quote in overlay.items():
            source = str(getattr(quote, "source", "") or "entry_quote_truth")
            stats["resolved_count"] += 1
            stats["sources"][source] += 1
            target = next(
                (item for item in targets if (item["venue"], item["symbol"]) == key),
                {"venue": key[0], "symbol": key[1]},
            )
            rest_revalidate_attempted = bool(
                target.get("rest_revalidate_attempted")
            ) or bool(str(target.get("rest_outcome") or ""))
            payload = {
                **target,
                "source": source,
                "reason_bucket": "rest_resolved"
                if rest_revalidate_attempted
                else "fresh_ws_quote",
                "observed_at_ms": int(getattr(quote, "observed_at_ms", 0) or 0),
                "received_at_ms": int(getattr(quote, "received_at_ms", 0) or 0),
                "exchange_event_at_ms": int(
                    getattr(quote, "exchange_event_at_ms", 0) or 0
                ),
                "age_ms": max(
                    now_ms - int(getattr(quote, "observed_at_ms", 0) or 0),
                    0,
                ),
                "quote_bid": float(getattr(quote, "bid", 0.0) or 0.0),
                "quote_ask": float(getattr(quote, "ask", 0.0) or 0.0),
                "sidecar_reason": str(target.get("reason") or ""),
                "ws_bbo_lease_hit": not rest_revalidate_attempted,
                "rest_revalidate_hit": rest_revalidate_attempted,
                "rest_revalidate_terminal_stale": False,
                "outcome": "resolved",
                "candidate_scope": candidate_scope,
                "evidence_role": evidence_role,
                "ts_ms": now_ms,
            }
            self.ctx.journal.append("runtime.entry_quote_revalidate_resolved", payload)
            self.ctx.journal.append(
                "runtime.entry_ws_bbo_top_candidate_rewarm_succeeded",
                payload,
            )

        for target in unresolved.values():
            stats["failed_count"] += 1
            rest_outcome = str(target.get("rest_outcome") or "")
            stream_state: dict[str, Any] = {}
            data_plane = getattr(self.ctx, "ws_bbo_data_plane", None)
            if data_plane is not None and hasattr(data_plane, "stream_state"):
                try:
                    stream_state = data_plane.stream_state(
                        target["venue"],
                        target["symbol"],
                        now_ms=now_ms,
                        max_age_ms=self._entry_quote_lease_max_age_ms(),
                    )
                except TypeError:
                    stream_state = data_plane.stream_state(
                        target["venue"],
                        target["symbol"],
                    )
                except Exception:
                    stream_state = {}
            if bool(target.get("ws_budget_excluded")) and not (
                callable(refresh_quote_result) or callable(refresh_quote)
            ):
                outcome = "budget_excluded_rest_unavailable"
                bucket = "budget_excluded_without_rest"
                reason_bucket = "budget_excluded_without_rest"
                reason_family = reason_bucket
            elif rest_outcome == "entry_evidence_deadline_exceeded":
                outcome = "entry_evidence_deadline_exceeded"
                bucket = "entry_evidence_deadline_exceeded"
                reason_bucket = "entry_evidence_deadline_exceeded"
                reason_family = "entry_evidence_deadline_exceeded"
            elif rest_outcome == "throttled":
                outcome = "rest_attempt_throttled"
                bucket = "rest_topbook_attempt_throttled"
                reason_bucket = "rest_throttled"
                reason_family = reason_bucket
            elif rest_outcome == "unsupported_symbol":
                outcome = "rest_unsupported_symbol"
                bucket = "rest_topbook_unsupported_symbol"
                reason_bucket = "rest_unsupported_symbol"
                reason_family = "rest_invalid_quote"
            elif rest_outcome in {"http_error", "parse_error"}:
                outcome = f"rest_{rest_outcome}"
                bucket = "rest_topbook_revalidate_failed"
                reason_bucket = f"rest_{rest_outcome}"
                reason_family = "rest_invalid_quote"
            elif rest_outcome == "invalid_quote":
                outcome = "rest_invalid_quote"
                bucket = "rest_topbook_revalidate_failed"
                reason_bucket = "rest_resolved_invalid_bid_ask"
                reason_family = "rest_invalid_quote"
            elif target.get("rest_error"):
                outcome = "rest_timeout"
                bucket = "rest_topbook_revalidate_failed"
                reason_bucket = "rest_timeout_or_exception"
                reason_family = "rest_invalid_quote"
            elif rest_outcome == "resolved":
                outcome = "rest_invalid_quote"
                bucket = "rest_topbook_revalidate_failed"
                reject_reason = str(
                    target.get("quote_validation_reject_reason") or ""
                )
                if reject_reason == "stale":
                    reason_bucket = "rest_resolved_but_stale"
                elif reject_reason == "missing_observed_at":
                    reason_bucket = "rest_resolved_missing_observed_at"
                else:
                    reason_bucket = "rest_resolved_invalid_bid_ask"
                reason_family = "rest_invalid_quote"
            elif stats.get("rest_attempt_count", 0):
                outcome = "rest_invalid_quote"
                bucket = "rest_topbook_revalidate_failed"
                reason_bucket = "rest_timeout_or_exception"
                reason_family = "rest_invalid_quote"
            else:
                outcome = "ws_timeout"
                bucket = "quote_revalidate_unavailable"
                reason_bucket = str(
                    stream_state.get("reason_bucket")
                    or stream_state.get("lease_state")
                    or "not_tracked"
                )
                reason_family = reason_bucket
            stats["top_quote_blocker_buckets"][bucket] += 1
            stats["quote_lease_failure_counts"][reason_bucket] += 1
            age_ms = target.get("age_ms")
            if age_ms is None:
                age_ms = target.get("sidecar_age_ms")
            if age_ms is None:
                observed_at_ms = int(target.get("observed_at_ms") or 0)
                age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else None
            payload = {
                **target,
                "outcome": outcome,
                "reason_bucket": reason_bucket,
                "reason_family": reason_family,
                "quote_validation_reject_reason": str(
                    target.get("quote_validation_reject_reason") or ""
                ),
                "sidecar_reason": str(target.get("reason") or ""),
                "ws_bbo_lease_hit": False,
                "rest_revalidate_attempted": bool(
                    target.get("rest_revalidate_attempted")
                ),
                "rest_revalidate_hit": rest_outcome == "resolved",
                "rest_revalidate_terminal_stale": (
                    rest_outcome == "resolved"
                    and str(target.get("quote_validation_reject_reason") or "") == "stale"
                ),
                "rest_quote_observed_at_ms": target.get("rest_quote_observed_at_ms"),
                "rest_quote_received_at_ms": target.get("rest_quote_received_at_ms"),
                "rest_quote_exchange_event_at_ms": target.get(
                    "rest_quote_exchange_event_at_ms"
                ),
                "rest_quote_age_ms": target.get("rest_quote_age_ms"),
                "rest_quote_bid": float(target.get("rest_quote_bid") or 0.0),
                "rest_quote_ask": float(target.get("rest_quote_ask") or 0.0),
                "source": "entry_quote_truth",
                "age_ms": age_ms,
                "budget_ms": self._entry_quote_lease_max_age_ms(),
                "lease_state": str(stream_state.get("lease_state") or ""),
                "stream_tracked": bool(stream_state.get("tracked")),
                "stream_subscribed": bool(stream_state.get("subscribed")),
                "stream_connected": bool(stream_state.get("connected")),
                "stream_message_count": int(stream_state.get("message_count") or 0),
                "last_quote_age_ms": stream_state.get("last_quote_age_ms"),
                "last_error": str(stream_state.get("last_error") or ""),
                "endpoint": str(target.get("endpoint") or "rest_topbook"),
                "venue_symbol": str(target.get("venue_symbol") or ""),
                "url": str(target.get("url") or ""),
                "http_status": int(target.get("http_status") or 0),
                "body_excerpt": str(target.get("body_excerpt") or ""),
                "attempt_interval_outcome": str(
                    target.get("attempt_interval_outcome") or ""
                ),
                "rest_error": str(target.get("rest_error") or ""),
                "ws_budget_excluded": bool(target.get("ws_budget_excluded")),
                "candidate_scope": candidate_scope,
                "evidence_role": evidence_role,
                "ts_ms": now_ms,
            }
            self.ctx.journal.append("runtime.entry_quote_revalidate_failed", payload)
            self.ctx.journal.append(
                "runtime.entry_ws_bbo_top_candidate_rewarm_failed",
                payload,
            )
            if reason_bucket == "rest_resolved_but_stale":
                self._schedule_entry_quote_rewarm_after_rest_stale(
                    payload,
                    now_ms=now_ms,
                )

        # Publish a terminal quote-domain state for every candidate even when
        # no REST adapter exists, every target came from cache, or the batch
        # ended at its deadline. Incremental publication above is the latency
        # fast path; this is the completeness invariant used by the final
        # ready-only frontier.
        if evidence_coordinator is not None:
            required_target_keys = {
                (target["venue"], target["symbol"])
                for target in all_targets
            }
            overlay_keys = set(overlay)
            for candidate_index, candidate in enumerate(candidates or []):
                symbol = str(
                    getattr(candidate, "symbol", "") or ""
                ).strip().upper()
                candidate_keys = {
                    (
                        str(getattr(candidate, venue_attr, "") or "")
                        .strip()
                        .lower(),
                        symbol,
                    )
                    for venue_attr in ("long_venue", "short_venue")
                } & required_target_keys
                _mark_entry_evidence_domain_state(
                    evidence_coordinator,
                    domain="quote",
                    candidate_index=candidate_index,
                    state=(
                        "ready"
                        if candidate_keys.issubset(overlay_keys)
                        else "failed"
                    ),
                )

        self._entry_quote_truth_record_last_scan(stats)
        if evidence_role != "prewarm_only":
            self._emit_entry_quote_revalidate_probe(
                stats=stats,
                candidate_count=len(candidates or []),
                now_ms=now_ms,
            )
        return overlay, stats

    def _snapshot_local_l2_state(self) -> None:
        """Snapshot local-L2 runtime state into EngineState for persistence/recovery.

        V1: PersistedRetainedLocalL2Book with bids/asks + generation tracking.
        """
        now_ms = wall_clock_now_ms()
        if not self._local_l2_effective_enabled():
            self._publish_local_l2_depth_bridge(now_ms, force_empty=True)
            self._clear_local_l2_runtime_state()
            return
        diag = self.ctx.local_l2_runtime.diagnostics_snapshot()
        # Retained books metadata (V1: persisted with full book data)
        self.ctx.state.retained_local_l2_books = [
            {
                "venue": b.venue,
                "symbol": b.symbol,
                "status": b.status.value,
                "pool": b.pool.value,
                "sequence": b.sequence,
                "last_snapshot_ms": b.last_snapshot_ms,
                "last_delta_ms": b.last_delta_ms,
                "last_update_id": b.last_update_id,
                "generation": getattr(b, 'generation', 1),
                "bids": [{"price": l.price, "quantity": l.quantity} for l in b.bids] if hasattr(b, 'bids') else [],
                "asks": [{"price": l.price, "quantity": l.quantity} for l in b.asks] if hasattr(b, 'asks') else [],
            }
            for b in self.ctx.local_l2_runtime.books.values()
            if b.pool == L2PoolAssignment.RETAINED
        ]
        # Full books snapshot for recovery
        self.ctx.state.local_l2_books_snapshot = [
            {
                "venue": b.venue,
                "symbol": b.symbol,
                "status": b.status.value,
                "pool": b.pool.value,
                "last_update_id": b.last_update_id,
                "sequence": b.sequence,
                "last_snapshot_ms": b.last_snapshot_ms,
                "last_delta_ms": b.last_delta_ms,
                "observed_at_ms": b.observed_at_ms,
                "generation": getattr(b, 'generation', 1),
                "bids": [{"price": l.price, "quantity": l.quantity} for l in b.bids] if hasattr(b, 'bids') else [],
                "asks": [{"price": l.price, "quantity": l.quantity} for l in b.asks] if hasattr(b, 'asks') else [],
            }
            for b in self.ctx.local_l2_runtime.books.values()
        ]
        # Session snapshot
        self.ctx.state.local_l2_session_snapshot = [
            s.diagnostics_snapshot(now_ms=wall_clock_now_ms(), stale_after_ms=5000)
            for s in self.ctx.entry_l2_sessions.sessions.values()
        ]
        self._publish_local_l2_depth_bridge(now_ms)

    def _publish_local_l2_depth_bridge(
        self,
        now_ms: int,
        *,
        force_empty: bool = False,
    ) -> None:
        """Publish a bounded local-L2 bridge without adding market requests."""
        runtime = self.ctx.config.runtime
        if not runtime.local_l2_depth_bridge_enabled:
            return
        interval_ms = runtime.local_l2_depth_bridge_publish_interval_ms
        if (
            not force_empty
            and self._last_local_l2_depth_bridge_publish_ms > 0
            and now_ms - self._last_local_l2_depth_bridge_publish_ms < interval_ms
        ):
            return
        try:
            from lightfee.marketdata.l2_depth_bridge import publish_local_l2_depth_bridge

            publish_local_l2_depth_bridge(
                runtime.local_l2_depth_bridge_path,
                () if force_empty else self.ctx.local_l2_runtime.books.values(),
                now_ms=now_ms,
                max_age_ms=self._entry_local_l2_stale_after_ms(),
                max_levels=runtime.local_l2_depth_bridge_max_levels,
            )
            self._last_local_l2_depth_bridge_publish_ms = now_ms
        except (OSError, TypeError, ValueError) as exc:
            # The bridge is optional evidence.  A failed write must leave
            # execution, recovery and closing paths untouched; the consumer
            # safely falls back to BBO when the last bridge becomes stale.
            if (
                self._last_local_l2_depth_bridge_error_ms <= 0
                or now_ms - self._last_local_l2_depth_bridge_error_ms >= 30_000
            ):
                self._last_local_l2_depth_bridge_error_ms = now_ms
                self.ctx.journal.append(
                    "runtime.local_l2_depth_bridge_publish_failed",
                    {
                        "path": runtime.local_l2_depth_bridge_path,
                        "error": str(exc)[:300],
                        "ts_ms": now_ms,
                    },
                )

    def _snapshot_domain_budget_ms(self, domain: str, row=None) -> int:
        domain_s = str(domain or "").lower()
        if domain_s == "liquidity":
            configured_ms = int(
                getattr(
                    self.ctx.config.runtime,
                    "sidecar_perp_liquidity_budget_ms",
                    self.ctx.config.strategy.max_liquidity_snapshot_age_ms,
                )
                or 0
            )
            refresh_ms = self.ctx.config.runtime.sidecar_refresh_ms
            timeout_ms = int(
                float(
                    getattr(
                        self.ctx.config.runtime,
                        "sidecar_liquidity_timeout_s",
                        10.0,
                    )
                    or 0.0
                )
                * 1000.0
            )
            publish_interval_ms = (
                int(getattr(row, "publish_interval_ms", 0) or 0)
                if row is not None else 0
            )
            return int(
                max(
                    configured_ms,
                    int(self.ctx.config.strategy.max_liquidity_snapshot_age_ms or 0),
                    refresh_ms * 3 if refresh_ms > 0 else 0,
                    refresh_ms + timeout_ms * 2 if timeout_ms > 0 else 0,
                    publish_interval_ms * 2 if publish_interval_ms > 0 else 0,
                    30_000,
                )
            )
        if domain_s == "quote":
            return int(
                self.ctx.config.runtime.max_order_quote_age_ms
                or self.ctx.config.runtime.max_market_age_ms
                or self.ctx.config.runtime.sidecar_snapshot_max_age_ms
            )
        if domain_s == "market":
            return int(
                self.ctx.config.runtime.max_market_age_ms
                or self.ctx.config.runtime.sidecar_snapshot_max_age_ms
            )
        if domain_s == "funding":
            return int(self.ctx.config.runtime.sidecar_snapshot_max_age_ms)
        return int(self.ctx.config.runtime.sidecar_snapshot_max_age_ms)

    @staticmethod
    def _snapshot_metric_key(venue: str, symbol: str, domain: str) -> str:
        return f"{str(venue).lower()}|{str(symbol).upper()}|{str(domain).lower()}"

    @staticmethod
    def _record_snapshot_metric(metrics: dict, key: str, fresh: bool) -> None:
        row = metrics.setdefault(key, {"fresh": 0, "stale": 0})
        row["fresh" if fresh else "stale"] = int(row.get("fresh" if fresh else "stale", 0)) + 1

    def _snapshot_fallback_source(self, snapshot) -> str:
        source = str(getattr(snapshot, "acquisition_mode", "") or "")
        return source or "fresh_sidecar"

    def _market_quote_lookup(self, market_quotes) -> dict[tuple[str, str], object]:
        if not market_quotes:
            return {}
        items = market_quotes.items() if hasattr(market_quotes, "items") else enumerate(market_quotes)
        lookup: dict[tuple[str, str], object] = {}
        for key, quote in items:
            if isinstance(key, tuple) and len(key) == 2:
                venue = str(key[0])
                symbol = str(key[1])
            else:
                venue = str(getattr(quote, "venue", "") or "")
                symbol = str(getattr(quote, "symbol", "") or "")
                if (not venue or not symbol) and isinstance(key, str) and ":" in key:
                    venue, symbol = key.split(":", 1)
            if venue and symbol:
                lookup[(venue.lower(), symbol.upper())] = quote
        return lookup

    @staticmethod
    def _snapshot_quote_direct_observed_at_ms(quote) -> int:
        return int(getattr(quote, "observed_at_ms", 0) or 0)

    @staticmethod
    def _snapshot_quote_source(quote) -> str:
        return str(getattr(quote, "source", "") or "sidecar_quote")

    @staticmethod
    def _snapshot_publication_at_ms(snapshot) -> int:
        """Use the verified V7 install watermark, with legacy compatibility."""
        return int(
            getattr(snapshot, "ready_at_ms", 0)
            or getattr(snapshot, "published_at_ms", 0)
            or 0
        )

    def _snapshot_quote_observed_at_ms(self, snapshot, quote) -> int:
        return (
            self._snapshot_quote_direct_observed_at_ms(quote)
            or int(getattr(snapshot, "market_observed_at_ms", 0) or 0)
            or int(getattr(snapshot, "published_at_ms", 0) or 0)
        )

    @staticmethod
    def _snapshot_scoped_status_key(
        domain: str,
        venue: str,
        symbol: str,
        source: str,
    ) -> str:
        return (
            f"{str(domain).lower()}|{str(venue).lower()}|"
            f"{str(symbol).upper()}|{str(source).lower()}"
        )

    def _snapshot_lifecycle_rows_by_venue(self, snapshot, domain: str) -> dict[str, object]:
        attr = {
            "funding": "funding_lifecycle",
            "market": "market_lifecycle",
            "liquidity": "liquidity_lifecycle",
        }.get(domain)
        if not attr:
            return {}
        rows = getattr(snapshot, attr, []) or []
        result: dict[str, object] = {}
        for row in rows:
            venue = str(getattr(row, "venue", "") or "").lower()
            if venue:
                result[venue] = row
        return result

    def _snapshot_freshness_observability(
        self,
        *,
        snapshot,
        candidates: list,
        now_ms: int,
    ) -> tuple[
        dict[str, dict[str, int]],
        dict[str, int],
        dict[str, int],
        dict[str, int],
        dict[str, dict],
    ]:
        metrics: dict[str, dict[str, int]] = {}
        ages: dict[str, int] = {}
        budgets: dict[str, int] = {}
        publish_intervals: dict[str, int] = {}
        statuses: dict[str, dict] = {}
        if snapshot is None:
            return metrics, ages, budgets, publish_intervals, statuses

        market_observed_at_ms = int(getattr(snapshot, "market_observed_at_ms", 0) or 0)
        market_age_ms = (
            max(now_ms - market_observed_at_ms, 0)
            if market_observed_at_ms > 0 else 0
        )
        market_budget_ms = int(
            self.ctx.config.runtime.max_market_age_ms
            or self._snapshot_domain_budget_ms("market")
        )
        self._record_snapshot_scoped_status(
            statuses,
            domain="market",
            venue="global",
            symbol="*",
            source="snapshot.market_observed_at_ms",
            observed_at_ms=market_observed_at_ms,
            age_ms=market_age_ms,
            budget_ms=market_budget_ms,
            fresh=market_observed_at_ms > 0 and market_age_ms <= market_budget_ms,
        )

        for quote in getattr(snapshot, "quotes", {}).values():
            venue = str(getattr(quote, "venue", "") or "").lower()
            symbol = str(getattr(quote, "symbol", "") or "").upper()
            if not venue or not symbol:
                continue
            observed_at_ms = self._snapshot_quote_observed_at_ms(snapshot, quote)
            age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
            budget_ms = self._snapshot_domain_budget_ms("quote")
            key = self._snapshot_metric_key(venue, symbol, "quote")
            fresh = observed_at_ms > 0 and age_ms <= budget_ms
            self._record_snapshot_metric(metrics, key, fresh)
            ages[key] = age_ms
            budgets[key] = budget_ms
            source = self._snapshot_quote_source(quote)
            if self._snapshot_quote_direct_observed_at_ms(quote) <= 0:
                source = "snapshot.market_observed_at_ms"
            self._record_snapshot_scoped_status(
                statuses,
                domain="quote",
                venue=venue,
                symbol=symbol,
                source=source,
                observed_at_ms=observed_at_ms,
                age_ms=age_ms,
                budget_ms=budget_ms,
                fresh=fresh,
            )

        lifecycle_by_domain = {
            "market": self._snapshot_lifecycle_rows_by_venue(snapshot, "market"),
            "funding": self._snapshot_lifecycle_rows_by_venue(snapshot, "funding"),
            "liquidity": self._snapshot_lifecycle_rows_by_venue(snapshot, "liquidity"),
        }
        seen: set[tuple[str, str, str]] = set()
        for candidate in candidates:
            symbol = str(getattr(candidate, "symbol", "") or "").upper()
            for venue_attr in ("long_venue", "short_venue"):
                venue = str(getattr(candidate, venue_attr, "") or "").lower()
                if not venue or not symbol:
                    continue
                for domain, rows in lifecycle_by_domain.items():
                    row = rows.get(venue)
                    if row is None:
                        continue
                    marker = (venue, symbol, domain)
                    if marker in seen:
                        continue
                    seen.add(marker)
                    observed_at_ms = int(getattr(row, "observed_at_ms", 0) or 0)
                    age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
                    budget_ms = self._snapshot_domain_budget_ms(domain, row)
                    key = self._snapshot_metric_key(venue, symbol, domain)
                    fresh = observed_at_ms > 0 and age_ms <= budget_ms
                    self._record_snapshot_metric(metrics, key, fresh)
                    ages[key] = age_ms
                    budgets[key] = budget_ms
                    self._record_snapshot_scoped_status(
                        statuses,
                        domain=domain,
                        venue=venue,
                        symbol=symbol,
                        source=str(
                            getattr(row, "source", f"sidecar_{domain}") or f"sidecar_{domain}"
                        ),
                        observed_at_ms=observed_at_ms,
                        age_ms=age_ms,
                        budget_ms=budget_ms,
                        fresh=fresh,
                    )
                    if domain == "liquidity":
                        publish_intervals[key] = int(
                            getattr(row, "publish_interval_ms", 0) or 0
                        )

        transfer_rows = getattr(snapshot, "transfer_lifecycle", []) or []
        candidate_symbols = {
            str(getattr(candidate, "symbol", "") or "").upper()
            for candidate in candidates
            if str(getattr(candidate, "symbol", "") or "")
        } or {"*"}
        for row in transfer_rows:
            from_venue = str(getattr(row, "from_venue", "") or "").lower()
            to_venue = str(getattr(row, "to_venue", "") or "").lower()
            if not from_venue or not to_venue:
                continue
            observed_at_ms = int(getattr(row, "observed_at_ms", 0) or 0)
            age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
            budget_ms = self._snapshot_domain_budget_ms("transfer", row)
            venue = f"{from_venue}->{to_venue}"
            for symbol in sorted(candidate_symbols):
                self._record_snapshot_scoped_status(
                    statuses,
                    domain="transfer",
                    venue=venue,
                    symbol=symbol,
                    source=str(getattr(row, "source", "") or "sidecar_transfer"),
                    observed_at_ms=observed_at_ms,
                    age_ms=age_ms,
                    budget_ms=budget_ms,
                    fresh=observed_at_ms > 0 and age_ms <= budget_ms,
                )

        return metrics, ages, budgets, publish_intervals, statuses

    def _candidate_snapshot_freshness_decisions(
        self,
        candidate,
        *,
        snapshot,
        now_ms: int,
        record_liquidity_qualification: bool = False,
        entry_quote_truth_overlay: dict[tuple[str, str], Any] | None = None,
    ) -> list[dict]:
        if snapshot is None:
            return []
        quote_lookup = self._market_quote_lookup(getattr(snapshot, "quotes", {}) or {})
        funding_rows = self._snapshot_lifecycle_rows_by_venue(snapshot, "funding")
        liquidity_rows = self._snapshot_lifecycle_rows_by_venue(snapshot, "liquidity")
        fallback_source = self._snapshot_fallback_source(snapshot)
        decisions: list[dict] = []
        symbol = str(getattr(candidate, "symbol", "") or "").upper()
        requires_sidecar_liquidity = (
            self._candidate_requires_sidecar_perp_liquidity(candidate)
        )

        for venue_attr in ("long_venue", "short_venue"):
            venue = str(getattr(candidate, venue_attr, "") or "").lower()
            if not venue or not symbol:
                continue

            quote = quote_lookup.get((venue, symbol))
            funding_row = funding_rows.get(venue)
            funding_budget_ms = self._snapshot_domain_budget_ms(
                "funding",
                funding_row,
            )
            if quote is None:
                decisions.append(
                    {
                        "venue": venue,
                        "symbol": symbol,
                        "domain": "funding",
                        "source": "sidecar_funding",
                        "observed_at_ms": 0,
                        "age_ms": 0,
                        "budget_ms": funding_budget_ms,
                        "decision": "skip_entry",
                        "fallback_source": fallback_source,
                        "reason": "missing_funding_rate_evidence",
                        "blocking": True,
                    }
                )
            else:
                funding_observed_at_ms = _evidence_clock_or_zero(
                    getattr(quote, "funding_rate_observed_at_ms", 0)
                )
                funding_received_at_ms = _evidence_clock_or_zero(
                    getattr(quote, "funding_rate_received_at_ms", 0)
                )
                funding_event_at_ms = _evidence_clock_or_zero(
                    getattr(quote, "funding_rate_event_at_ms", 0)
                )
                funding_age_ms = (
                    max(now_ms - funding_observed_at_ms, 0)
                    if funding_observed_at_ms > 0
                    else 0
                )
                funding_rate = getattr(quote, "funding_rate_bps", None)
                funding_source = str(
                    getattr(quote, "funding_rate_source", "") or ""
                )
                funding_sample_id = str(
                    getattr(quote, "funding_rate_sample_id", "") or ""
                )
                proof_reason = funding_rate_evidence_reason(
                    venue=venue,
                    symbol=symbol,
                    rate_bps=funding_rate,
                    funding_timestamp_ms=getattr(
                        quote, "funding_timestamp_ms", 0
                    ),
                    observed_at_ms=funding_observed_at_ms,
                    event_at_ms=funding_event_at_ms,
                    received_at_ms=funding_received_at_ms,
                    source=funding_source,
                    sample_id=funding_sample_id,
                    decision_at_ms=now_ms,
                )
                funding_reason = (
                    "invalid_funding_rate_evidence" if proof_reason else ""
                )
                if not funding_reason and funding_age_ms > funding_budget_ms:
                    funding_reason = "funding_rate_stale"
                if funding_reason:
                    decisions.append(
                        {
                            "venue": venue,
                            "symbol": symbol,
                            "domain": "funding",
                            "source": funding_source or "sidecar_funding",
                            "sample_id": funding_sample_id,
                            "event_at_ms": funding_event_at_ms,
                            "received_at_ms": funding_received_at_ms,
                            "observed_at_ms": funding_observed_at_ms,
                            "age_ms": funding_age_ms,
                            "budget_ms": funding_budget_ms,
                            "decision": "skip_entry",
                            "fallback_source": fallback_source,
                            "reason": funding_reason,
                            "evidence_reason": proof_reason,
                            "blocking": True,
                        }
                    )
            quote_budget_ms = self._snapshot_domain_budget_ms("quote")
            if quote is None:
                decisions.append({
                    "venue": venue,
                    "symbol": symbol,
                    "domain": "quote",
                    "source": "sidecar_quote",
                    "observed_at_ms": 0,
                    "age_ms": 0,
                    "budget_ms": quote_budget_ms,
                    "decision": "skip_entry",
                    "fallback_source": fallback_source,
                    "reason": "missing_quote",
                    "blocking": True,
                })
            else:
                observed_at_ms = self._snapshot_quote_observed_at_ms(snapshot, quote)
                age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
                source = self._snapshot_quote_source(quote)
                if self._snapshot_quote_direct_observed_at_ms(quote) <= 0:
                    source = "snapshot.market_observed_at_ms"
                bid = float(getattr(quote, "bid", 0.0) or 0.0)
                ask = float(getattr(quote, "ask", 0.0) or 0.0)
                overlay_quote = self._entry_quote_truth_overlay_quote(
                    entry_quote_truth_overlay,
                    venue,
                    symbol,
                )
                overlay_resolved = self._entry_quote_truth_decision(
                    venue=venue,
                    symbol=symbol,
                    quote=overlay_quote,
                    now_ms=now_ms,
                    fallback_source=fallback_source,
                    sidecar_source=source,
                    sidecar_observed_at_ms=observed_at_ms,
                    sidecar_age_ms=age_ms,
                    sidecar_budget_ms=quote_budget_ms,
                    sidecar_reason=(
                        "last_good_sidecar"
                        if fallback_source == "last_good_sidecar"
                        else "quote_stale"
                        if age_ms > quote_budget_ms
                        else "fresh_sidecar"
                    ),
                )
                if (
                    observed_at_ms <= 0
                    or age_ms > quote_budget_ms
                    or bid <= 0.0
                    or ask <= 0.0
                    or ask <= bid
                ):
                    reason = "quote_stale" if age_ms > quote_budget_ms else "invalid_quote"
                    if reason == "quote_stale" and overlay_resolved is not None:
                        decisions.append(overlay_resolved)
                        continue
                    if reason == "quote_stale":
                        resolved = self._ws_bbo_entry_quote_resolution(
                            venue=venue,
                            symbol=symbol,
                            now_ms=now_ms,
                            sidecar_reason=reason,
                            sidecar_source=source,
                            sidecar_observed_at_ms=observed_at_ms,
                            sidecar_age_ms=age_ms,
                            sidecar_budget_ms=quote_budget_ms,
                            fallback_source=fallback_source,
                        )
                        if resolved is not None:
                            decisions.append(resolved)
                            continue
                    payload = {
                        "venue": venue,
                        "symbol": symbol,
                        "domain": "quote",
                        "source": source,
                        "observed_at_ms": observed_at_ms,
                        "age_ms": age_ms,
                        "budget_ms": quote_budget_ms,
                        "decision": "skip_entry",
                        "fallback_source": fallback_source,
                        "reason": reason,
                        "blocking": True,
                    }
                    payload.update(
                        self._snapshot_quote_evidence(
                            quote=quote,
                            observed_at_ms=observed_at_ms,
                            age_ms=age_ms,
                            budget_ms=quote_budget_ms,
                        )
                    )
                    if reason == "quote_stale":
                        payload["event_kind"] = "runtime.quote_stale"
                    decisions.append(payload)
                elif (
                    fallback_source == "last_good_sidecar"
                    and self._entry_readiness_provider_uses_ws_bbo()
                ):
                    if overlay_resolved is not None:
                        decisions.append(overlay_resolved)
                        continue
                    payload = {
                        "venue": venue,
                        "symbol": symbol,
                        "domain": "quote",
                        "source": source,
                        "observed_at_ms": observed_at_ms,
                        "age_ms": age_ms,
                        "budget_ms": quote_budget_ms,
                        "decision": "skip_entry",
                        "fallback_source": fallback_source,
                        "reason": "last_good_sidecar_revalidate_required",
                        "blocking": True,
                        "event_kind": "runtime.entry_quote_revalidate_failed",
                    }
                    payload.update(
                        self._snapshot_quote_evidence(
                            quote=quote,
                            observed_at_ms=observed_at_ms,
                            age_ms=age_ms,
                            budget_ms=quote_budget_ms,
                        )
                    )
                    decisions.append(payload)

            liquidity = liquidity_rows.get(venue)
            liq_budget_ms = self._snapshot_domain_budget_ms("liquidity", liquidity)
            liq_observed_at_ms = (
                int(getattr(liquidity, "observed_at_ms", 0) or 0)
                if liquidity is not None else 0
            )
            liq_coverage_usable = (
                int(getattr(liquidity, "coverage_usable", 0) or 0)
                if liquidity is not None else 0
            )
            liq_degraded_reason = (
                str(getattr(liquidity, "degraded_reason", "") or "")
                if liquidity is not None else ""
            )
            liq_degraded_blocks_symbol = (
                self._liquidity_degraded_reason_blocks_symbol(
                    liq_degraded_reason, symbol
                )
            )
            liq_age_ms = (
                max(now_ms - liq_observed_at_ms, 0)
                if liq_observed_at_ms > 0 else 0
            )
            liq_stale_or_missing = (
                liquidity is None
                or liq_observed_at_ms <= 0
                or liq_age_ms > liq_budget_ms
                or liq_coverage_usable <= 0
                or liq_degraded_blocks_symbol
            )
            if liq_stale_or_missing:
                reason = (
                    "perp_liquidity_stale_blocking"
                    if requires_sidecar_liquidity
                    else "perp_liquidity_stale_advisory"
                )
                decisions.append(
                    self._liquidity_lifecycle_payload(
                        row=liquidity,
                        venue=venue,
                        symbol=symbol,
                        now_ms=now_ms,
                        decision="skip_entry" if requires_sidecar_liquidity else "continue",
                        reason=reason,
                        fallback_source=fallback_source,
                    )
                )

        decisions.extend(
            self._entry_liquidity_qualification_decisions(
                candidate,
                snapshot=snapshot,
                quote_lookup=quote_lookup,
                now_ms=now_ms,
                fallback_source=fallback_source,
                record_result=record_liquidity_qualification,
            )
        )

        return decisions

    def _entry_open_interest_refresher(self) -> Any:
        refresher = getattr(self, "entry_open_interest_refresher", None)
        if refresher is not None:
            return refresher
        runtime_config = self.ctx.config.runtime
        durable_store_path = str(
            getattr(
                runtime_config,
                "entry_open_interest_store_path",
                "runtime/entry-open-interest-evidence-v1.sqlite3",
            )
            or ""
        ).strip()
        if durable_store_path:
            durable_store_path = str(
                resolve_config_artifact_path(self.ctx.config, durable_store_path)
            )
        refresher = EntryOpenInterestRefresher(
            targeted_budget_s=self._entry_open_interest_refresh_timeout_s(),
            durable_store_path=durable_store_path,
            prewarm_interval_ms=_positive_runtime_ms(
                getattr(
                    runtime_config,
                    "entry_open_interest_background_refresh_ms",
                    15 * 60_000,
                ),
                default=15 * 60_000,
            ),
        )
        setattr(self, "entry_open_interest_refresher", refresher)
        return refresher

    def _entry_open_interest_refresh_timeout_ms(self) -> int:
        return _positive_runtime_ms(
            getattr(
                self.ctx.config.runtime,
                "entry_open_interest_refresh_timeout_ms",
                750,
            ),
            default=750,
        )

    def _entry_open_interest_refresh_timeout_s(self) -> float:
        return self._entry_open_interest_refresh_timeout_ms() / 1_000.0

    def _entry_open_interest_cache_fallback_max_age_ms(self) -> int:
        return bounded_open_interest_cache_fallback_max_age_ms(
            getattr(
                self.ctx.config.runtime,
                "entry_open_interest_cache_fallback_max_age_ms",
                ENTRY_OPEN_INTEREST_CACHE_FALLBACK_MAX_AGE_MS,
            )
        )

    async def _refresh_entry_candidate_open_interest_evidence(
        self,
        candidates: list,
        *,
        snapshot,
        now_ms: int,
        evidence_role: str = "entry_execution",
        candidate_scope: str = "",
        evidence_coordinator: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        stats = {
            "evidence_role": evidence_role,
            "candidate_scope": candidate_scope,
            "candidate_count": len(candidates or []),
            "target_count": 0,
            "attempt_count": 0,
            "resolved_count": 0,
            "failed_count": 0,
            "unsupported_count": 0,
            "timeout_count": 0,
            "entry_evidence_deadline_exceeded_count": 0,
            "superseded_by_ready_candidate_count": 0,
            "deferred_count": 0,
            "blocked_after_targeted_refresh_count": 0,
            "targets": [],
            "cleanup_deleted_count": 0,
            "cleanup_failed_count": 0,
        }
        if snapshot is None or not self.ctx.config.strategy.execution_liquidity_enabled:
            return stats
        if getattr(self.ctx.state, "last_scan", None) is None:
            self.ctx.state.last_scan = {}
        oi_cache_fallback_max_age_ms = (
            self._entry_open_interest_cache_fallback_max_age_ms()
        )

        def _record_prewarm_scan_stats() -> None:
            self.ctx.state.last_scan["oi_prewarm_extra_candidate_scope"] = (
                candidate_scope
            )
            self.ctx.state.last_scan["oi_prewarm_extra_candidate_count"] = stats[
                "candidate_count"
            ]
            self.ctx.state.last_scan["oi_prewarm_extra_target_count"] = stats[
                "target_count"
            ]
            self.ctx.state.last_scan["oi_prewarm_extra_attempt_count"] = stats[
                "attempt_count"
            ]
            self.ctx.state.last_scan["oi_prewarm_extra_resolved_count"] = stats[
                "resolved_count"
            ]
            self.ctx.state.last_scan["oi_prewarm_extra_failed_count"] = stats[
                "failed_count"
            ]
            self.ctx.state.last_scan["oi_prewarm_extra_timeout_count"] = stats[
                "timeout_count"
            ]
            self.ctx.state.last_scan["oi_prewarm_extra_unsupported_count"] = stats[
                "unsupported_count"
            ]
            self.ctx.state.last_scan["oi_prewarm_extra_deferred_count"] = stats[
                "deferred_count"
            ]
            self.ctx.state.last_scan["oi_prewarm_extra_cleanup_deleted_count"] = stats[
                "cleanup_deleted_count"
            ]
            self.ctx.state.last_scan["oi_prewarm_extra_cleanup_failed_count"] = stats[
                "cleanup_failed_count"
            ]
            if stats.get("prewarm_skipped_reason"):
                self.ctx.state.last_scan["oi_prewarm_extra_skipped_reason"] = stats[
                    "prewarm_skipped_reason"
                ]

        refresher = None
        if evidence_role == "prewarm_only":
            refresher = self._entry_open_interest_refresher()
            prewarm_due = getattr(refresher, "prewarm_due", None)
            if callable(prewarm_due) and not prewarm_due(now_ms=now_ms):
                stats["deferred_count"] = len(candidates or [])
                stats["prewarm_skipped_reason"] = (
                    "entry_oi_prewarm_cadence_not_due"
                )
                _record_prewarm_scan_stats()
                return stats
            delete_expired = getattr(refresher, "delete_expired", None)
            if callable(delete_expired):
                try:
                    stats["cleanup_deleted_count"] = int(
                        delete_expired(
                            now_ms=now_ms,
                            max_age_ms=oi_cache_fallback_max_age_ms,
                        )
                        or 0
                    )
                except Exception as exc:  # pragma: no cover - defensive telemetry
                    stats["cleanup_failed_count"] = 1
                    self.ctx.journal.append(
                        "runtime.entry_oi_durable_cleanup_failed",
                        {
                            "error": f"{type(exc).__name__}: {exc}"[:240],
                            "candidate_scope": candidate_scope,
                            "evidence_role": evidence_role,
                            "ts_ms": now_ms,
                        },
                    )
            mark_prewarm_started = getattr(refresher, "mark_prewarm_started", None)
            if callable(mark_prewarm_started):
                mark_prewarm_started(now_ms=now_ms)
            if not candidates:
                _record_prewarm_scan_stats()
                return stats
        elif not candidates:
            return stats

        quote_lookup = self._market_quote_lookup(getattr(snapshot, "quotes", {}) or {})
        structural_probe_due: set[tuple[str, str]] = set()
        for record in (
            getattr(self.ctx.state, "entry_liquidity_qualification_records", []) or []
        ):
            if not isinstance(record, dict):
                continue
            if str(record.get("last_class", "")) != "structural_ineligibility":
                continue
            last_probe_ms = int(record.get("last_structural_probe_at_ms", 0) or 0)
            if last_probe_ms > 0 and now_ms - last_probe_ms < 60_000:
                continue
            structural_probe_due.add(
                (
                    str(record.get("venue", "") or "").lower(),
                    str(record.get("symbol", "") or "").upper(),
                )
            )
        targets: list[tuple[str, str, Any, str]] = []
        seen: set[tuple[str, str]] = set()
        target_candidate_identities: dict[
            tuple[str, str],
            dict[str, set[str]],
        ] = {}
        oi_max_age_ms = max(
            int(
                getattr(
                    self.ctx.config.runtime,
                    "sidecar_perp_liquidity_budget_ms",
                    30_000,
                )
                or 0
            ),
            1,
        )
        oi_refresh_timeout_ms = self._entry_open_interest_refresh_timeout_ms()
        oi_refresh_timeout_s = oi_refresh_timeout_ms / 1_000.0
        for candidate in list(candidates or []):
            symbol = str(getattr(candidate, "symbol", "") or "").strip().upper()
            if not symbol:
                continue
            for venue_attr in ("long_venue", "short_venue"):
                venue = str(getattr(candidate, venue_attr, "") or "").strip().lower()
                if venue not in EntryOpenInterestRefresher.SUPPORTED_VENUES:
                    continue
                floor_getter = getattr(
                    self.ctx,
                    "_entry_liquidity_open_interest_floor_quote",
                    None,
                )
                if callable(floor_getter) and floor_getter(venue) <= 0.0:
                    continue
                key = (venue, symbol)
                quote = quote_lookup.get(key)
                if quote is None:
                    continue
                identities = target_candidate_identities.setdefault(
                    key,
                    {
                        "candidate_revision_ids": set(),
                        "opportunity_lease_ids": set(),
                    },
                )
                candidate_revision_id = str(
                    getattr(candidate, "candidate_revision_id", "") or ""
                ).strip()
                opportunity_lease_id = str(
                    getattr(candidate, "opportunity_lease_id", "") or ""
                ).strip()
                if candidate_revision_id:
                    identities["candidate_revision_ids"].add(candidate_revision_id)
                if opportunity_lease_id:
                    identities["opportunity_lease_ids"].add(opportunity_lease_id)
                if key in seen:
                    continue
                evidence_status = normalize_open_interest_status(
                    getattr(quote, "open_interest_evidence_status", "unavailable")
                )
                observed_at_ms = int(
                    getattr(quote, "open_interest_observed_at_ms", 0) or 0
                )
                received_at_ms = int(
                    getattr(quote, "open_interest_received_at_ms", 0) or 0
                )
                event_at_ms = int(
                    getattr(quote, "open_interest_event_at_ms", 0) or 0
                )
                evidence_is_fresh = open_interest_timestamps_are_fresh(
                    observed_at_ms=observed_at_ms,
                    received_at_ms=received_at_ms,
                    event_at_ms=event_at_ms,
                    now_ms=now_ms,
                    max_age_ms=open_interest_max_age_ms_for_evidence(
                        quote,
                        default_max_age_ms=oi_max_age_ms,
                    ),
                )
                if (
                    evidence_role != "prewarm_only"
                    and evidence_status == "observed"
                    and evidence_is_fresh
                    and key not in structural_probe_due
                ):
                    continue
                seen.add(key)
                targets.append((venue, symbol, quote, evidence_status))

        def _target_candidate_identity_fields(
            venue: str,
            symbol: str,
        ) -> dict[str, Any]:
            identities = target_candidate_identities.get((venue, symbol), {})
            revision_ids = sorted(identities.get("candidate_revision_ids", set()))
            lease_ids = sorted(identities.get("opportunity_lease_ids", set()))
            limit = 24
            fields: dict[str, Any] = {
                "candidate_revision_ids": revision_ids[:limit],
                "candidate_revision_ids_suppressed_count": max(
                    len(revision_ids) - limit,
                    0,
                ),
                "opportunity_lease_ids": lease_ids[:limit],
                "opportunity_lease_ids_suppressed_count": max(
                    len(lease_ids) - limit,
                    0,
                ),
            }
            # Preserve the established scalar leaf for the common one-candidate
            # target without misrepresenting a shared target as one candidate.
            if len(revision_ids) == 1:
                fields["candidate_revision_id"] = revision_ids[0]
            if len(lease_ids) == 1:
                fields["opportunity_lease_id"] = lease_ids[0]
            return fields

        stats["target_count"] = len(targets)
        stats["targets"] = [
            {
                "venue": venue,
                "symbol": symbol,
                "open_interest_evidence_status": status,
                **_target_candidate_identity_fields(venue, symbol),
            }
            for venue, symbol, _quote, status in targets[:24]
        ]
        if not targets:
            if evidence_coordinator is not None:
                for candidate_index, _candidate in enumerate(candidates):
                    _mark_entry_evidence_domain_state(
                        evidence_coordinator,
                        domain="open_interest",
                        candidate_index=candidate_index,
                        state="ready",
                    )
            if evidence_role == "prewarm_only":
                _record_prewarm_scan_stats()
            else:
                self.ctx.state.last_scan["oi_targeted_refresh_attempt_count"] = 0
                self.ctx.state.last_scan["oi_targeted_refresh_resolved_count"] = 0
                self.ctx.state.last_scan["oi_targeted_refresh_failed_count"] = 0
            return stats

        if refresher is None:
            refresher = self._entry_open_interest_refresher()
        refresh = getattr(refresher, "refresh_open_interest", None)
        batch_refresh = getattr(refresher, "refresh_open_interest_batch", None)
        if not callable(refresh) and not callable(batch_refresh):
            return stats

        def _quote_open_interest_payload(quote) -> dict[str, Any]:
            return {
                "open_interest_quote": getattr(quote, "open_interest", None),
                "open_interest_evidence_status": normalize_open_interest_status(
                    getattr(quote, "open_interest_evidence_status", "unavailable")
                ),
                "open_interest_evidence_reason": str(
                    getattr(quote, "open_interest_evidence_reason", "")
                    or "quote_open_interest_cache"
                ),
                "open_interest_observed_at_ms": int(
                    getattr(quote, "open_interest_observed_at_ms", 0) or 0
                ),
                "open_interest_event_at_ms": int(
                    getattr(quote, "open_interest_event_at_ms", 0) or 0
                ),
                "open_interest_received_at_ms": int(
                    getattr(quote, "open_interest_received_at_ms", 0) or 0
                ),
                "open_interest_source": str(
                    getattr(quote, "open_interest_source", "") or ""
                ),
                "open_interest_sample_id": str(
                    getattr(quote, "open_interest_sample_id", "") or ""
                ),
                "open_interest_venue_symbol": str(
                    getattr(quote, "open_interest_venue_symbol", "") or ""
                ),
                "raw_open_interest": getattr(quote, "raw_open_interest", None),
                "raw_open_interest_unit": str(
                    getattr(quote, "raw_open_interest_unit", "") or ""
                ),
                "open_interest_contract_multiplier": getattr(
                    quote,
                    "open_interest_contract_multiplier",
                    None,
                ),
                "open_interest_conversion_mark_price": getattr(
                    quote,
                    "open_interest_conversion_mark_price",
                    None,
                ),
            }

        def _cached_open_interest_fallback(
            *,
            venue: str,
            symbol: str,
            quote,
            reason: str,
        ) -> dict[str, Any] | None:
            cached_method = getattr(refresher, "cached_open_interest", None)
            if callable(cached_method):
                try:
                    cached = cached_method(
                        venue,
                        symbol,
                        now_ms=now_ms,
                        max_age_ms=oi_cache_fallback_max_age_ms,
                        reason=reason,
                    )
                except TypeError:
                    cached = cached_method(venue, symbol, now_ms=now_ms)
                if cached is not None:
                    return cached
            return _open_interest_cache_fallback_payload(
                venue=venue,
                symbol=symbol,
                payload=_quote_open_interest_payload(quote),
                now_ms=now_ms,
                reason=reason,
                max_age_ms=oi_cache_fallback_max_age_ms,
            )

        self.ctx.journal.append(
            "runtime.entry_oi_targeted_refresh_started",
            {
                "target_count": len(targets),
                "targets": stats["targets"],
                "candidate_scope": candidate_scope,
                "evidence_role": evidence_role,
                "ts_ms": now_ms,
            },
        )
        def _apply_refresh_result(
            *,
            venue: str,
            symbol: str,
            quote,
            previous_status: str,
            result: dict[str, Any] | None,
            default_elapsed_ms: int,
        ) -> None:
            stats["attempt_count"] += 1
            elapsed_ms = int(
                (result or {}).get(
                    "oi_targeted_refresh_elapsed_ms",
                    (result or {}).get("oi_refresh_elapsed_ms", default_elapsed_ms),
                )
                or 0
            )
            raw_status = normalize_open_interest_status(
                (result or {}).get("open_interest_evidence_status")
                or "unavailable"
            )
            raw_result_valid = (
                raw_status == "observed"
                and _targeted_open_interest_observed_proof_valid(
                    venue=venue,
                    symbol=symbol,
                    result=result or {},
                )
                and open_interest_timestamps_are_fresh(
                    observed_at_ms=int(
                        (result or {}).get("open_interest_observed_at_ms", 0) or 0
                    ),
                    received_at_ms=int(
                        (result or {}).get("open_interest_received_at_ms", 0) or 0
                    ),
                    event_at_ms=int(
                        (result or {}).get("open_interest_event_at_ms", 0) or 0
                    ),
                    now_ms=now_ms,
                    max_age_ms=open_interest_max_age_ms_for_evidence(
                        result or {},
                        default_max_age_ms=oi_max_age_ms,
                    ),
                )
            )
            if not raw_result_valid and evidence_role != "prewarm_only":
                fallback_reason = str(
                    (result or {}).get("open_interest_evidence_reason")
                    or raw_status
                    or "targeted_refresh_failed"
                )
                fallback = _cached_open_interest_fallback(
                    venue=venue,
                    symbol=symbol,
                    quote=quote,
                    reason=f"{fallback_reason}_cache_fallback",
                )
                if fallback is not None:
                    result = {**(result or {}), **fallback}
            status = normalize_open_interest_status(
                (result or {}).get("open_interest_evidence_status")
                or "unavailable"
            )
            reason = str(
                (result or {}).get("open_interest_evidence_reason")
                or status
            )
            open_interest_raw = (result or {}).get("open_interest_quote")
            try:
                open_interest_quote = (
                    float(open_interest_raw)
                    if open_interest_raw is not None
                    else None
                )
            except (TypeError, ValueError, OverflowError):
                open_interest_quote = None
            if status == "observed" and not _targeted_open_interest_observed_proof_valid(
                venue=venue,
                symbol=symbol,
                result=result or {},
            ):
                status = "parse_error"
                reason = "targeted_observed_oi_proof_invalid"
                open_interest_quote = None
            payload = {
                "venue": venue,
                "symbol": symbol,
                "blocking_stage": "entry_oi_revalidation",
                "blocking_domain": "open_interest",
                "blocking_status": status,
                "blocking_reason": reason if status != "observed" else "",
                "sample_id": str(
                    (result or {}).get("open_interest_sample_id", "") or ""
                ),
                "previous_open_interest_evidence_status": previous_status,
                "open_interest_evidence_status": status,
                "open_interest_evidence_reason": reason,
                "open_interest_quote": open_interest_quote,
                "elapsed_ms": elapsed_ms,
                "oi_dns_ms": (result or {}).get("oi_dns_ms"),
                "oi_connect_ms": int((result or {}).get("oi_connect_ms", 0) or 0),
                "oi_pool_wait_ms": int(
                    (result or {}).get("oi_pool_wait_ms", 0) or 0
                ),
                "oi_rate_limit_wait_ms": int(
                    (result or {}).get("oi_rate_limit_wait_ms", 0) or 0
                ),
                "oi_transport_total_ms": int(
                    (result or {}).get("oi_transport_total_ms", 0) or 0
                ),
                "oi_http_ms": int((result or {}).get("oi_http_ms", 0) or 0),
                "oi_parse_ms": int((result or {}).get("oi_parse_ms", 0) or 0),
                "oi_dns_timing_status": str(
                    (result or {}).get("oi_dns_timing_status", "unavailable")
                    or "unavailable"
                ),
                "oi_scheduler_inflight_count": int(
                    (result or {}).get("inflight_count", 0) or 0
                ),
                "oi_scheduler_queued_count": int(
                    (result or {}).get("queued_count", 0) or 0
                ),
                "oi_scheduler_max_inflight": int(
                    (result or {}).get("max_inflight", 0) or 0
                ),
                "oi_scheduler_oldest_age_ms": int(
                    (result or {}).get("oldest_age_ms", 0) or 0
                ),
                "oi_scheduler_reused_count": int(
                    (result or {}).get("reused_count", 0) or 0
                ),
                "oi_scheduler_deferred_count": int(
                    (result or {}).get("deferred_count", 0) or 0
                ),
                "oi_scheduler_cancelled_count": int(
                    (result or {}).get("cancelled_count", 0) or 0
                ),
                "open_interest_cache_fallback": bool(
                    (result or {}).get("open_interest_cache_fallback", False)
                ),
                "open_interest_cache_fallback_max_age_ms": int(
                    (result or {}).get(
                        "open_interest_cache_fallback_max_age_ms",
                        0,
                    )
                    or 0
                ),
                "open_interest_cache_fallback_age_ms": int(
                    (result or {}).get("open_interest_cache_fallback_age_ms", 0) or 0
                ),
                "candidate_scope": candidate_scope,
                "evidence_role": evidence_role,
                "ts_ms": now_ms,
                **_target_candidate_identity_fields(venue, symbol),
            }
            if status == "observed" and open_interest_quote is not None:
                quote.open_interest = open_interest_quote
                quote.open_interest_evidence_status = "observed"
                quote.open_interest_evidence_reason = reason or "targeted_refresh"
                for field in (
                    "open_interest_observed_at_ms",
                    "open_interest_event_at_ms",
                    "open_interest_received_at_ms",
                    "open_interest_source",
                    "open_interest_sample_id",
                    "open_interest_venue_symbol",
                    "raw_open_interest",
                    "raw_open_interest_unit",
                    "open_interest_contract_multiplier",
                    "open_interest_conversion_mark_price",
                    "open_interest_cache_fallback",
                    "open_interest_cache_fallback_max_age_ms",
                    "open_interest_cache_fallback_age_ms",
                ):
                    if field in (result or {}):
                        setattr(quote, field, (result or {}).get(field))
                for field in (
                    "oi_candidate_count",
                    "oi_cache_hit_count",
                    "oi_cache_miss_count",
                    "oi_refresh_attempt_count",
                    "oi_refresh_cap",
                    "oi_deferred_count",
                    "oi_timeout_count",
                    "oi_refresh_elapsed_ms",
                ):
                    if field in (result or {}):
                        setattr(quote, field, int((result or {}).get(field) or 0))
                stats["resolved_count"] += 1
                self.ctx.journal.append(
                    "runtime.entry_oi_targeted_refresh_resolved",
                    payload,
                )
            else:
                quote.open_interest = None
                quote.open_interest_evidence_status = status
                quote.open_interest_evidence_reason = reason
                stats["failed_count"] += 1
                stats["blocked_after_targeted_refresh_count"] += 1
                if status == "timeout":
                    stats["timeout_count"] += 1
                    if reason == "entry_evidence_deadline_exceeded":
                        stats["entry_evidence_deadline_exceeded_count"] += 1
                if status == "unsupported":
                    stats["unsupported_count"] += 1
                if reason == "entry_evidence_scheduler_capacity_exceeded":
                    stats["deferred_count"] += 1
                self.ctx.journal.append(
                    "runtime.entry_oi_targeted_refresh_failed",
                    payload,
                )

        def _candidate_oi_ready(candidate) -> bool:
            """Whether every OI-required leg has fresh observed evidence now."""
            symbol = str(getattr(candidate, "symbol", "") or "").strip().upper()
            if not symbol:
                return False
            current_now_ms = wall_clock_now_ms()
            for venue_attr in ("long_venue", "short_venue"):
                venue = str(
                    getattr(candidate, venue_attr, "") or ""
                ).strip().lower()
                if venue not in EntryOpenInterestRefresher.SUPPORTED_VENUES:
                    continue
                floor_getter = getattr(
                    self.ctx,
                    "_entry_liquidity_open_interest_floor_quote",
                    None,
                )
                if callable(floor_getter) and floor_getter(venue) <= 0.0:
                    continue
                quote = quote_lookup.get((venue, symbol))
                if quote is None:
                    return False
                status = normalize_open_interest_status(
                    getattr(quote, "open_interest_evidence_status", "unavailable")
                )
                observed_at_ms = int(
                    getattr(quote, "open_interest_observed_at_ms", 0) or 0
                )
                received_at_ms = int(
                    getattr(quote, "open_interest_received_at_ms", 0) or 0
                )
                event_at_ms = int(
                    getattr(quote, "open_interest_event_at_ms", 0) or 0
                )
                if (
                    status != "observed"
                    or not open_interest_timestamps_are_fresh(
                        observed_at_ms=observed_at_ms,
                        received_at_ms=received_at_ms,
                        event_at_ms=event_at_ms,
                        now_ms=current_now_ms,
                        max_age_ms=open_interest_max_age_ms_for_evidence(
                            quote,
                            default_max_age_ms=oi_max_age_ms,
                        ),
                    )
                ):
                    return False
            return True

        # Batch-only test doubles retain compatibility, but production uses
        # target-isolated singleflight.  A slow symbol must not convert every
        # symbol at the venue into the same synthetic timeout.
        if callable(batch_refresh) and not isinstance(
            refresher, EntryOpenInterestRefresher
        ):
            grouped: dict[str, list[tuple[str, Any, str]]] = {}
            for venue, symbol, quote, previous_status in targets:
                grouped.setdefault(venue, []).append((symbol, quote, previous_status))

            async def _refresh_venue_batch(
                venue: str,
                entries: list[tuple[str, Any, str]],
            ) -> tuple[str, list[tuple[str, Any, str]], dict[str, Any], int]:
                symbols = [symbol for symbol, _quote, _status in entries]
                loop = asyncio.get_running_loop()
                started_monotonic = loop.time()
                try:
                    batch_results = await asyncio.wait_for(
                        batch_refresh(
                            venue,
                            symbols,
                            now_ms=wall_clock_now_ms(),
                        ),
                        timeout=oi_refresh_timeout_s,
                    )
                except asyncio.TimeoutError:
                    batch_results = {
                        symbol: {
                            "open_interest_quote": None,
                            "open_interest_evidence_status": "timeout",
                            "open_interest_evidence_reason": (
                                "entry_evidence_deadline_exceeded"
                            ),
                        }
                        for symbol in symbols
                    }
                except Exception as exc:  # pragma: no cover - defensive telemetry
                    status = _open_interest_exception_status(exc)
                    batch_results = {
                        symbol: {
                            "open_interest_quote": None,
                            "open_interest_evidence_status": status,
                            "open_interest_evidence_reason": f"{type(exc).__name__}: {exc}"[:200],
                        }
                        for symbol in symbols
                    }
                elapsed_ms = max(int((loop.time() - started_monotonic) * 1_000), 0)
                return venue, entries, batch_results or {}, elapsed_ms

            venue_batches = await asyncio.gather(
                *(
                    _refresh_venue_batch(venue, entries)
                    for venue, entries in grouped.items()
                )
            )
            for venue, entries, batch_results, elapsed_ms in venue_batches:
                for symbol, quote, previous_status in entries:
                    result = (batch_results or {}).get(symbol)
                    if result is None:
                        result = {
                            "open_interest_quote": None,
                            "open_interest_evidence_status": "parse_error",
                            "open_interest_evidence_reason": "missing_targeted_ticker",
                        }
                    _apply_refresh_result(
                        venue=venue,
                        symbol=symbol,
                        quote=quote,
                        previous_status=previous_status,
                        result=result,
                        default_elapsed_ms=elapsed_ms,
                    )
        else:
            async def _refresh_single_target(
                venue: str,
                symbol: str,
                quote,
                previous_status: str,
            ) -> tuple[str, str, Any, str, dict[str, Any] | None, int]:
                loop = asyncio.get_running_loop()
                started_monotonic = loop.time()
                try:
                    structural_force_refresh = (venue, symbol) in structural_probe_due
                    force_refresh = (
                        evidence_role == "prewarm_only"
                        or structural_force_refresh
                    )
                    if structural_force_refresh:
                        # Throttle the *attempt*, not only successful low/high
                        # observations.  A timeout/rate-limit must not cause a
                        # structural target to force-refresh again every tick.
                        probe_started_at_ms = wall_clock_now_ms()
                        for record in (
                            getattr(
                                self.ctx.state,
                                "entry_liquidity_qualification_records",
                                [],
                            )
                            or []
                        ):
                            if not isinstance(record, dict):
                                continue
                            if (
                                str(record.get("venue", "") or "").lower()
                                == venue
                                and str(record.get("symbol", "") or "").upper()
                                == symbol
                            ):
                                record["last_structural_probe_at_ms"] = (
                                    probe_started_at_ms
                                )
                                break
                    try:
                        refresh_coro = refresh(
                            venue,
                            symbol,
                            now_ms=wall_clock_now_ms(),
                            force_refresh=force_refresh,
                            max_age_ms=oi_max_age_ms,
                            cache_fallback_max_age_ms=oi_cache_fallback_max_age_ms,
                            priority=evidence_role,
                        )
                    except TypeError:
                        # Compatibility for injected legacy/test refreshers.
                        try:
                            refresh_coro = refresh(
                                venue,
                                symbol,
                                now_ms=wall_clock_now_ms(),
                                force_refresh=force_refresh,
                                max_age_ms=oi_max_age_ms,
                            )
                        except TypeError:
                            try:
                                refresh_coro = refresh(
                                    venue,
                                    symbol,
                                    now_ms=wall_clock_now_ms(),
                                    force_refresh=force_refresh,
                                )
                            except TypeError:
                                refresh_coro = refresh(
                                    venue,
                                    symbol,
                                    now_ms=wall_clock_now_ms(),
                                )
                    result = await asyncio.wait_for(
                        refresh_coro,
                        timeout=oi_refresh_timeout_s,
                    )
                except asyncio.TimeoutError:
                    result = {
                        "open_interest_quote": None,
                        "open_interest_evidence_status": "timeout",
                        "open_interest_evidence_reason": (
                            "entry_evidence_deadline_exceeded"
                        ),
                    }
                except Exception as exc:  # pragma: no cover - defensive telemetry
                    status = _open_interest_exception_status(exc)
                    result = {
                        "open_interest_quote": None,
                        "open_interest_evidence_status": status,
                        "open_interest_evidence_reason": f"{type(exc).__name__}: {exc}"[:200],
                        **_open_interest_exception_phase_timings(exc),
                    }
                elapsed_ms = max(
                    int((loop.time() - started_monotonic) * 1_000),
                    0,
                )
                return (
                    venue,
                    symbol,
                    quote,
                    previous_status,
                    result,
                    elapsed_ms,
                )

            target_rows = {
                (venue, symbol): (venue, symbol, quote, previous_status)
                for venue, symbol, quote, previous_status in targets
            }
            queued_keys = list(target_rows)
            target_tasks: dict[tuple[str, str], asyncio.Task] = {}
            task_keys: dict[asyncio.Task, tuple[str, str]] = {}
            # Queue the complete frontier behind the refresher's transport
            # single-flight capacity.  As each target settles, the next ranked
            # target is launched, so capacity pressure delays lower routes
            # instead of permanently classifying them as ineligible.
            transport_limit = int(getattr(refresher, "_max_inflight", 0) or 0)
            max_in_flight = max(len(target_rows), 1)
            if transport_limit > 0:
                max_in_flight = min(max_in_flight, transport_limit)
            if evidence_role == "prewarm_only":
                prewarm_limit = int(
                    getattr(refresher, "_max_prewarm_inflight", 0) or 0
                )
                if prewarm_limit > 0:
                    max_in_flight = min(max_in_flight, prewarm_limit)

            def _launch_more_targets() -> None:
                while queued_keys and len(task_keys) < max_in_flight:
                    next_index = 0
                    if evidence_role == "prewarm_only":
                        active_venues = {key[0] for key in task_keys.values()}
                        next_index = next(
                            (
                                index
                                for index, queued_key in enumerate(queued_keys)
                                if queued_key[0] not in active_venues
                            ),
                            -1,
                        )
                        if next_index < 0:
                            break
                    key = queued_keys.pop(next_index)
                    venue, symbol, quote, previous_status = target_rows[key]
                    task = asyncio.create_task(
                        _refresh_single_target(
                            venue,
                            symbol,
                            quote,
                            previous_status,
                        ),
                        name=f"entry-oi-target:{venue}:{symbol}",
                    )
                    target_tasks[key] = task
                    task_keys[task] = key

            _launch_more_targets()
            target_keys = set(target_rows)
            completed_keys: set[tuple[str, str]] = set()

            async def _cancel_active_target_tasks() -> None:
                active_tasks = [
                    task for task in task_keys if not task.done()
                ]
                for task in active_tasks:
                    task.cancel()
                if active_tasks:
                    await asyncio.gather(*active_tasks, return_exceptions=True)

            def _highest_resolved_candidate_is_ready() -> bool:
                selection_ready = False
                for candidate_index, candidate in enumerate(candidates):
                    symbol = str(
                        getattr(candidate, "symbol", "") or ""
                    ).strip().upper()
                    unresolved = False
                    for venue_attr in ("long_venue", "short_venue"):
                        venue = str(
                            getattr(candidate, venue_attr, "") or ""
                        ).strip().lower()
                        key = (venue, symbol)
                        if key in target_keys and key not in completed_keys:
                            unresolved = True
                            break
                    if unresolved:
                        return selection_ready
                    candidate_ready = _candidate_oi_ready(candidate)
                    if evidence_coordinator is not None:
                        selection_ready = (
                            _mark_entry_evidence_domain_state(
                                evidence_coordinator,
                                domain="open_interest",
                                candidate_index=candidate_index,
                                state="ready" if candidate_ready else "failed",
                            )
                            or selection_ready
                        )
                    # Standalone domain refresh has no cross-domain economic
                    # validator, so evidence-ready cannot authorize pruning.
                    # This higher-ranked candidate is fully resolved but
                    # failed OI evidence; continue to the next ranked row.
                return selection_ready

            loop = asyncio.get_running_loop()
            cycle_deadline = None
            if evidence_role != "prewarm_only":
                cycle_deadline = loop.time() + oi_refresh_timeout_s
            pending = set(target_tasks.values())
            _highest_resolved_candidate_is_ready()
            try:
                while pending:
                    wait_timeout_s = None
                    if cycle_deadline is not None:
                        wait_timeout_s = max(cycle_deadline - loop.time(), 0.0)
                        if wait_timeout_s <= 0.0:
                            break
                    done, _still_pending = await asyncio.wait(
                        pending,
                        timeout=wait_timeout_s,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if not done:
                        break
                    pending.difference_update(done)
                    for task in done:
                        key = task_keys.pop(task)
                        try:
                            (
                                venue,
                                symbol,
                                quote,
                                previous_status,
                                result,
                                elapsed_ms,
                            ) = task.result()
                        except asyncio.CancelledError:
                            continue
                        _apply_refresh_result(
                            venue=venue,
                            symbol=symbol,
                            quote=quote,
                            previous_status=previous_status,
                            result=result,
                            default_elapsed_ms=elapsed_ms,
                        )
                        completed_keys.add(key)
                    _highest_resolved_candidate_is_ready()
                    _launch_more_targets()
                    pending.update(task_keys)
            except asyncio.CancelledError:
                await _cancel_active_target_tasks()
                raise

            timed_out_tasks = list(pending)
            for task in timed_out_tasks:
                task.cancel()
            if timed_out_tasks:
                await asyncio.gather(*timed_out_tasks, return_exceptions=True)
            for task in timed_out_tasks:
                key = task_keys.get(task)
                if key is None:
                    continue
                venue, symbol, quote, previous_status = target_rows[key]
                _apply_refresh_result(
                    venue=venue,
                    symbol=symbol,
                    quote=quote,
                    previous_status=previous_status,
                    result={
                        "open_interest_quote": None,
                        "open_interest_evidence_status": "timeout",
                        "open_interest_evidence_reason": (
                            "entry_evidence_deadline_exceeded"
                        ),
                    },
                    default_elapsed_ms=oi_refresh_timeout_ms,
                )
                completed_keys.add(key)
            for key in queued_keys:
                venue, symbol, quote, previous_status = target_rows[key]
                quote.open_interest = None
                quote.open_interest_evidence_status = "deferred"
                quote.open_interest_evidence_reason = (
                    "entry_evidence_deadline_deferred"
                )
                stats["failed_count"] += 1
                stats["blocked_after_targeted_refresh_count"] += 1
                stats["deferred_count"] += 1
                completed_keys.add(key)
                self.ctx.journal.append(
                    "runtime.entry_oi_targeted_refresh_deferred",
                    {
                        "venue": venue,
                        "symbol": symbol,
                        "blocking_stage": "entry_oi_revalidation",
                        "blocking_domain": "open_interest",
                        "blocking_status": "deferred",
                        "blocking_reason": "entry_evidence_deadline_deferred",
                        "previous_open_interest_evidence_status": previous_status,
                        "candidate_scope": candidate_scope,
                        "evidence_role": evidence_role,
                        "ts_ms": wall_clock_now_ms(),
                        **_target_candidate_identity_fields(venue, symbol),
                    },
                )
            _highest_resolved_candidate_is_ready()

        if evidence_role == "prewarm_only":
            _record_prewarm_scan_stats()
        else:
            self.ctx.state.last_scan["oi_targeted_refresh_attempt_count"] = stats[
                "attempt_count"
            ]
            self.ctx.state.last_scan["oi_targeted_refresh_resolved_count"] = stats[
                "resolved_count"
            ]
            self.ctx.state.last_scan["oi_targeted_refresh_failed_count"] = stats[
                "failed_count"
            ]
            self.ctx.state.last_scan["oi_targeted_refresh_timeout_count"] = stats[
                "timeout_count"
            ]
            self.ctx.state.last_scan[
                "oi_targeted_refresh_superseded_count"
            ] = stats["superseded_by_ready_candidate_count"]
            self.ctx.state.last_scan["oi_targeted_refresh_deferred_count"] = stats[
                "deferred_count"
            ]
            self.ctx.state.last_scan[
                "oi_entry_evidence_deadline_exceeded_count"
            ] = stats["entry_evidence_deadline_exceeded_count"]
            self.ctx.state.last_scan["oi_targeted_refresh_unsupported_count"] = stats[
                "unsupported_count"
            ]
            self.ctx.state.last_scan[
                "entry_blocked_after_targeted_refresh_count"
            ] = stats["blocked_after_targeted_refresh_count"]
        return stats

    @staticmethod
    def _snapshot_quote_evidence(
        *,
        quote,
        observed_at_ms: int,
        age_ms: int,
        budget_ms: int,
    ) -> dict:
        bid = float(getattr(quote, "bid", 0.0) or 0.0)
        ask = float(getattr(quote, "ask", 0.0) or 0.0)
        bid_size = float(getattr(quote, "bid_size", 0.0) or 0.0)
        ask_size = float(getattr(quote, "ask_size", 0.0) or 0.0)
        invalid_fields: list[str] = []
        if observed_at_ms <= 0:
            invalid_fields.append("observed_at_ms")
        if age_ms > budget_ms:
            invalid_fields.append("age_ms")
        if bid <= 0.0:
            invalid_fields.append("bid")
        if ask <= 0.0:
            invalid_fields.append("ask")
        if bid_size <= 0.0:
            invalid_fields.append("bid_size")
        if ask_size <= 0.0:
            invalid_fields.append("ask_size")
        return {
            "quote_bid": bid,
            "quote_ask": ask,
            "quote_bid_size": bid_size,
            "quote_ask_size": ask_size,
            "quote_mark_price": float(getattr(quote, "mark_price", 0.0) or 0.0),
            "quote_index_price": float(getattr(quote, "index_price", 0.0) or 0.0),
            "quote_funding_timestamp_ms": int(
                getattr(quote, "funding_timestamp_ms", 0) or 0
            ),
            "invalid_quote_fields": invalid_fields,
        }

    def _ws_bbo_entry_quote_resolution(
        self,
        *,
        venue: str,
        symbol: str,
        now_ms: int,
        sidecar_reason: str,
        sidecar_source: str,
        sidecar_observed_at_ms: int,
        sidecar_age_ms: int,
        sidecar_budget_ms: int,
        fallback_source: str,
    ) -> dict | None:
        if sidecar_reason != "quote_stale":
            return None
        if not self._entry_readiness_provider_uses_ws_bbo():
            return None
        cache = self.ctx.ws_bbo_cache
        if cache is None:
            return None
        budget_ms = self._entry_quote_lease_max_age_ms()
        if budget_ms <= 0:
            return None
        quote = cache.fresh_quote(
            venue,
            symbol,
            now_ms=now_ms,
            max_age_ms=budget_ms,
        )
        if quote is None:
            return None
        observed_at_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
        age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
        bid = float(getattr(quote, "bid", 0.0) or 0.0)
        ask = float(getattr(quote, "ask", 0.0) or 0.0)
        if observed_at_ms <= 0 or age_ms > budget_ms or bid <= 0.0 or ask <= bid:
            return None
        return {
            "venue": str(venue or "").lower(),
            "symbol": str(symbol or "").upper(),
            "domain": "quote",
            "source": "ws_bbo_quote_lease",
            "provider": "ws_bbo_quote_lease",
            "observed_at_ms": observed_at_ms,
            "age_ms": age_ms,
            "budget_ms": budget_ms,
            "decision": "continue",
            "fallback_source": fallback_source,
            "reason": f"{sidecar_reason}_resolved_by_ws_bbo",
            "blocking": False,
            "event_kind": "runtime.entry_quote_evidence_resolved_by_ws_bbo",
            "sidecar_reason": str(sidecar_reason or ""),
            "sidecar_source": str(sidecar_source or ""),
            "sidecar_observed_at_ms": int(sidecar_observed_at_ms or 0),
            "sidecar_age_ms": int(sidecar_age_ms or 0),
            "sidecar_budget_ms": int(sidecar_budget_ms or 0),
            "ws_bbo_lease_hit": True,
            "rest_revalidate_hit": False,
            "rest_revalidate_terminal_stale": False,
            "ws_bbo_source": str(getattr(quote, "source", "") or ""),
            "quote_bid": bid,
            "quote_ask": ask,
            "quote_bid_size": float(getattr(quote, "bid_size", 0.0) or 0.0),
            "quote_ask_size": float(getattr(quote, "ask_size", 0.0) or 0.0),
            "invalid_quote_fields": [],
            "blocker_family": "quote_evidence_resolved",
            "metric_fresh": True,
        }

    def _entry_quote_truth_decision(
        self,
        *,
        venue: str,
        symbol: str,
        quote: Any | None,
        now_ms: int,
        fallback_source: str,
        sidecar_source: str,
        sidecar_observed_at_ms: int,
        sidecar_age_ms: int,
        sidecar_budget_ms: int,
        sidecar_reason: str,
    ) -> dict | None:
        if not self._entry_quote_truth_accept_quote(quote, now_ms=now_ms):
            return None
        observed_at_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
        age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else 0
        bid = float(getattr(quote, "bid", 0.0) or 0.0)
        ask = float(getattr(quote, "ask", 0.0) or 0.0)
        quote_source = str(getattr(quote, "source", "") or "")
        rest_revalidate_hit = "rest" in quote_source.lower()
        reason = (
            "last_good_revalidated_by_entry_quote_truth"
            if sidecar_reason == "last_good_sidecar"
            else f"{sidecar_reason}_resolved_by_entry_quote_truth"
        )
        event_kind = (
            "runtime.last_good_revalidated_by_entry_quote_truth"
            if sidecar_reason == "last_good_sidecar"
            else "runtime.entry_quote_evidence_resolved_by_ws_bbo"
        )
        return {
            "venue": str(venue or "").lower(),
            "symbol": str(symbol or "").upper(),
            "domain": "quote",
            "source": "entry_quote_truth",
            "provider": "entry_quote_revalidator",
            "observed_at_ms": observed_at_ms,
            "age_ms": age_ms,
            "budget_ms": self._entry_quote_lease_max_age_ms(),
            "decision": "continue",
            "fallback_source": fallback_source,
            "reason": reason,
            "blocking": False,
            "event_kind": event_kind,
            "sidecar_reason": str(sidecar_reason or ""),
            "sidecar_source": str(sidecar_source or ""),
            "sidecar_observed_at_ms": int(sidecar_observed_at_ms or 0),
            "sidecar_age_ms": int(sidecar_age_ms or 0),
            "sidecar_budget_ms": int(sidecar_budget_ms or 0),
            "ws_bbo_lease_hit": not rest_revalidate_hit,
            "rest_revalidate_hit": rest_revalidate_hit,
            "rest_revalidate_terminal_stale": False,
            "entry_quote_truth_source": quote_source,
            "quote_bid": bid,
            "quote_ask": ask,
            "quote_bid_size": float(getattr(quote, "bid_size", 0.0) or 0.0),
            "quote_ask_size": float(getattr(quote, "ask_size", 0.0) or 0.0),
            "invalid_quote_fields": [],
            "blocker_family": "quote_revalidate_resolved",
            "metric_fresh": True,
        }

    @staticmethod
    def _snapshot_freshness_evidence_fields(decision: dict) -> dict:
        keys = (
            "quote_bid",
            "quote_ask",
            "quote_bid_size",
            "quote_ask_size",
            "quote_mark_price",
            "quote_index_price",
            "quote_funding_timestamp_ms",
            "invalid_quote_fields",
            "observed_volume_24h_quote",
            "min_volume_24h_quote",
            "observed_open_interest_quote",
            "min_open_interest_quote",
            "eligibility_class",
            "consecutive_failures",
            "suppress_until_ms",
            "last_failure_at_ms",
            "last_structural_probe_at_ms",
        )
        return {key: decision[key] for key in keys if key in decision}

    def _candidate_snapshot_freshness_failures(
        self,
        candidate,
        *,
        snapshot,
        now_ms: int,
        entry_quote_truth_overlay: dict[tuple[str, str], Any] | None = None,
    ) -> list[dict]:
        return [
            decision
            for decision in self._call_candidate_snapshot_freshness_decisions(
                candidate,
                snapshot=snapshot,
                now_ms=now_ms,
                entry_quote_truth_overlay=entry_quote_truth_overlay,
            )
            if decision.get("decision") == "skip_entry"
        ]

    def _snapshot_fallback_duration_ms(
        self,
        *,
        snapshot,
        now_ms: int,
        max_age_ms: int | None = None,
    ) -> int:
        if snapshot is None:
            return 0
        snapshot_max_age_ms = int(
            max_age_ms
            if max_age_ms is not None
            else self.ctx.config.runtime.sidecar_snapshot_max_age_ms
        )
        market_max_age_ms = int(
            self.ctx.config.runtime.max_market_age_ms or snapshot_max_age_ms
        )
        stale_overages: list[int] = []
        published_at_ms = self._snapshot_publication_at_ms(snapshot)
        market_observed_at_ms = int(getattr(snapshot, "market_observed_at_ms", 0) or 0)
        if published_at_ms > 0:
            snapshot_publish_age_ms = max(now_ms - published_at_ms, 0)
            if snapshot_publish_age_ms > snapshot_max_age_ms:
                stale_overages.append(snapshot_publish_age_ms - snapshot_max_age_ms)
        if market_observed_at_ms > 0:
            market_observed_age_ms = max(now_ms - market_observed_at_ms, 0)
            if market_observed_age_ms > market_max_age_ms:
                stale_overages.append(market_observed_age_ms - market_max_age_ms)
        return max(stale_overages) if stale_overages else 0

    def _snapshot_candidate_scope_sample(
        self,
        *,
        candidate,
        domain: str,
        venue: str,
        source: str,
        source_age_ms: int,
        fallback_duration_ms: int,
        blocked: bool,
        block_reason: str = "",
    ) -> dict:
        symbol = str(getattr(candidate, "symbol", "") or "").upper()
        return {
            "candidate_symbol": symbol,
            "candidate_pair_id": self._candidate_pair_id(candidate),
            "domain": str(domain or "").lower(),
            "venue": str(venue or "").lower(),
            "source": str(source or ""),
            "source_age_ms": int(source_age_ms or 0),
            "fallback_duration_ms": int(fallback_duration_ms or 0),
            "blocked": bool(blocked),
            "block_reason": str(block_reason or "") if blocked else "",
        }

    @staticmethod
    def _canonical_degraded_domain(domain: str) -> str:
        domain_s = str(domain or "").lower()
        if domain_s == "market_observed_stale":
            return "market_observed"
        if domain_s == "snapshot_publish_stale":
            return "snapshot_publish"
        if domain_s.endswith("_stale"):
            return domain_s[:-6]
        return domain_s

    def _snapshot_health_candidate_freshness_scope(
        self,
        *,
        snapshot,
        now_ms: int,
        degraded_domains: list[str],
        stale_degraded_domains: list[str],
        fallback_duration_ms: int,
        candidates: list | None = None,
    ) -> list[dict]:
        scope: list[dict] = []
        if snapshot is None:
            return scope

        candidates = (
            list(candidates)
            if candidates is not None
            else list(getattr(snapshot, "candidates", []) or [])
        )
        if not candidates:
            return scope

        seen: set[tuple[str, str, str, str, str]] = set()

        def add_sample(sample: dict) -> None:
            marker = (
                str(sample.get("candidate_pair_id", "")),
                str(sample.get("candidate_symbol", "")),
                str(sample.get("domain", "")),
                str(sample.get("venue", "")),
                str(sample.get("source", "")),
            )
            if marker in seen or len(scope) >= 48:
                return
            seen.add(marker)
            scope.append(sample)

        all_domains = [
            self._canonical_degraded_domain(domain)
            for domain in list(degraded_domains) + list(stale_degraded_domains)
        ]
        market_observed_age_ms = max(
            now_ms - int(getattr(snapshot, "market_observed_at_ms", 0) or 0),
            0,
        )
        snapshot_publish_age_ms = max(
            now_ms - self._snapshot_publication_at_ms(snapshot),
            0,
        )
        for candidate in candidates:
            if "market_observed" in all_domains:
                add_sample(
                    self._snapshot_candidate_scope_sample(
                        candidate=candidate,
                        domain="market_observed",
                        venue="global",
                        source="snapshot.market_observed_at_ms",
                        source_age_ms=market_observed_age_ms,
                        fallback_duration_ms=fallback_duration_ms,
                        blocked=False,
                    )
                )
            if "snapshot_publish" in all_domains:
                add_sample(
                    self._snapshot_candidate_scope_sample(
                        candidate=candidate,
                        domain="snapshot_publish",
                        venue="global",
                        source="snapshot.published_at_ms",
                        source_age_ms=snapshot_publish_age_ms,
                        fallback_duration_ms=fallback_duration_ms,
                        blocked=False,
                    )
                )

            for decision in self._call_candidate_snapshot_freshness_decisions(
                candidate,
                snapshot=snapshot,
                now_ms=now_ms,
            ):
                blocked = bool(
                    decision.get("blocking", False)
                    or decision.get("decision") == "skip_entry"
                )
                sample = self._snapshot_candidate_scope_sample(
                    candidate=candidate,
                    domain=str(decision.get("domain", "")),
                    venue=str(decision.get("venue", "")),
                    source=str(decision.get("source", "")),
                    source_age_ms=int(decision.get("age_ms", 0) or 0),
                    fallback_duration_ms=fallback_duration_ms,
                    blocked=blocked,
                    block_reason=str(decision.get("reason", "")),
                )
                sample.update(self._snapshot_freshness_evidence_fields(decision))
                add_sample(sample)

        if "liquidity" in all_domains:
            liquidity_rows = self._snapshot_lifecycle_rows_by_venue(snapshot, "liquidity")
            degraded_symbols = getattr(snapshot, "degraded_symbols", {}) or {}
            degraded_venues = {
                str(venue).lower()
                for venue in list(getattr(snapshot, "degraded_venues", []) or [])
            }
            if isinstance(degraded_symbols, dict):
                degraded_venues.update(
                    str(venue).lower()
                    for venue, symbols in degraded_symbols.items()
                    if symbols
                )
            for candidate in candidates:
                symbol = str(getattr(candidate, "symbol", "") or "").upper()
                for venue_attr in ("long_venue", "short_venue"):
                    venue = str(getattr(candidate, venue_attr, "") or "").lower()
                    row = liquidity_rows.get(venue)
                    degraded_reason = (
                        str(getattr(row, "degraded_reason", "") or "")
                        if row is not None else ""
                    )
                    if venue not in degraded_venues and not degraded_reason:
                        continue
                    observed_at_ms = (
                        int(getattr(row, "observed_at_ms", 0) or 0)
                        if row is not None else 0
                    )
                    source_age_ms = (
                        max(now_ms - observed_at_ms, 0)
                        if observed_at_ms > 0 else 0
                    )
                    source = (
                        str(getattr(row, "source", "") or "sidecar_perp_liquidity")
                        if row is not None else "sidecar_perp_liquidity"
                    )
                    degraded_symbols_for_venue = []
                    if isinstance(degraded_symbols, dict):
                        degraded_symbols_for_venue = [
                            str(v).upper()
                            for v in degraded_symbols.get(venue, []) or []
                        ]
                    candidate_hit = (
                        symbol in degraded_symbols_for_venue
                        or self._liquidity_degraded_reason_blocks_symbol(
                            degraded_reason, symbol
                        )
                    )
                    add_sample(
                        self._snapshot_candidate_scope_sample(
                            candidate=candidate,
                            domain="liquidity",
                            venue=venue,
                            source=source,
                            source_age_ms=source_age_ms,
                            fallback_duration_ms=fallback_duration_ms,
                            blocked=False,
                            block_reason=(
                                "candidate_symbol_degraded"
                                if candidate_hit else ""
                            ),
                        )
                    )

        return scope

    def _snapshot_health_candidate_scope_candidates(self, snapshot) -> tuple[list, str, int, int]:
        all_candidates = list(getattr(snapshot, "candidates", []) or []) if snapshot is not None else []
        if not all_candidates:
            return [], "empty", 0, 0
        if self._entry_readiness_provider_uses_ws_bbo() or self._local_l2_effective_enabled():
            _, tracked_candidates = self._select_v1_entry_tracked_scope(all_candidates)
            return (
                tracked_candidates,
                "v1_primary_shadow",
                len(all_candidates),
                max(len(all_candidates) - len(tracked_candidates), 0),
            )
        return all_candidates, "all_snapshot_candidates", len(all_candidates), 0

    def _snapshot_freshness_decision_log_key(
        self,
        payload: dict,
    ) -> tuple[str, str, str, str, str, str]:
        return (
            str(payload.get("venue", "") or "").lower(),
            str(payload.get("symbol", "") or "").upper(),
            str(payload.get("domain", "") or ""),
            str(payload.get("reason", "") or payload.get("decision", "") or ""),
            str(payload.get("candidate_revision_id", "") or ""),
            str(payload.get("opportunity_lease_id", "") or ""),
        )

    def _append_snapshot_freshness_decision_event(
        self,
        *,
        payload: dict,
        event_kind: str,
        now_ms: int,
    ) -> None:
        key = self._snapshot_freshness_decision_log_key(payload)
        # Revisions and leases make the diagnostic exact, but they also make
        # the key space generation-shaped.  Keep the rate-limit state bounded
        # so a long-running process cannot retain every expired opportunity.
        max_keys = 4_096
        if (
            key not in self._snapshot_freshness_decision_last_emit_ms
            and len(self._snapshot_freshness_decision_last_emit_ms) >= max_keys
        ):
            oldest_key = min(
                self._snapshot_freshness_decision_last_emit_ms,
                key=self._snapshot_freshness_decision_last_emit_ms.get,
            )
            self._snapshot_freshness_decision_last_emit_ms.pop(oldest_key, None)
            self._snapshot_freshness_decision_suppressed.pop(oldest_key, None)
        last_emit_ms = self._snapshot_freshness_decision_last_emit_ms.get(key)
        suppressed = int(self._snapshot_freshness_decision_suppressed.get(key, 0))
        due = (
            last_emit_ms is None
            or now_ms - last_emit_ms >= self._SNAPSHOT_FRESHNESS_DECISION_LOG_INTERVAL_MS
        )
        if not due:
            self._snapshot_freshness_decision_suppressed[key] += 1
            return

        event_payload = dict(payload)
        if suppressed > 0:
            event_payload["compact"] = True
            event_payload["suppressed_count"] = suppressed
        else:
            event_payload["suppressed_count"] = 0
        self._snapshot_freshness_decision_last_emit_ms[key] = now_ms
        self._snapshot_freshness_decision_suppressed.pop(key, None)
        self.ctx.journal.append("runtime.snapshot_freshness_decision", event_payload)
        if event_kind:
            self.ctx.journal.append(event_kind, event_payload)

    def _filter_candidates_by_snapshot_freshness(
        self,
        candidates: list,
        *,
        snapshot,
        now_ms: int,
        metrics: dict,
        ages: dict,
        budgets: dict | None = None,
        publish_intervals: dict | None = None,
        entry_quote_truth_overlay: dict[tuple[str, str], Any] | None = None,
        record_result: bool = True,
        emit_events: bool = True,
        mutate_liquidity_state: bool = True,
    ) -> list:
        filtered = []
        blockers: Counter[str] = Counter()
        samples: list[dict[str, Any]] = []
        if record_result:
            self._last_snapshot_freshness_filter_blockers = blockers
            self._last_snapshot_freshness_filter_samples = samples
        fallback_duration_ms = self._snapshot_fallback_duration_ms(
            snapshot=snapshot,
            now_ms=now_ms,
        )
        for candidate in candidates:
            decisions = self._call_candidate_snapshot_freshness_decisions(
                candidate,
                snapshot=snapshot,
                now_ms=now_ms,
                record_liquidity_qualification=(
                    record_result and mutate_liquidity_state
                ),
                entry_quote_truth_overlay=entry_quote_truth_overlay,
            )
            if not decisions:
                filtered.append(candidate)
                continue
            blocking = False
            for failure in decisions:
                key = self._snapshot_metric_key(
                    failure["venue"],
                    failure["symbol"],
                    failure["domain"],
                )
                if key not in metrics:
                    self._record_snapshot_metric(
                        metrics,
                        key,
                        bool(failure.get("metric_fresh", False)),
                    )
                ages[key] = int(failure.get("age_ms", 0) or 0)
                if budgets is not None:
                    budgets[key] = int(failure.get("budget_ms", 0) or 0)
                if publish_intervals is not None and "publish_interval_ms" in failure:
                    publish_intervals[key] = int(failure.get("publish_interval_ms", 0) or 0)
                payload = dict(failure)
                event_kind = str(payload.pop("event_kind", "") or "")
                payload["ts_ms"] = now_ms
                pair_id = self._candidate_pair_id(candidate)
                symbol = str(getattr(candidate, "symbol", "") or "").upper()
                blocked = bool(
                    failure.get("blocking", False)
                    or failure.get("decision") == "skip_entry"
                )
                reason = str(failure.get("reason", "snapshot_domain_stale"))
                payload["pair_id"] = pair_id
                payload["candidate_pair_id"] = pair_id
                payload["candidate_symbol"] = symbol
                payload["source_age_ms"] = int(failure.get("age_ms", 0) or 0)
                payload["fallback_duration_ms"] = fallback_duration_ms
                payload["blocked"] = blocked
                payload["block_reason"] = reason if blocked else ""
                payload.update(
                    {
                        "blocking_stage": "entry_snapshot_freshness",
                        "blocking_domain": str(failure.get("domain", "")),
                        "blocking_status": str(failure.get("decision", "")),
                        "blocking_reason": reason if blocked else "",
                        "sample_id": str(failure.get("sample_id", "") or ""),
                        "candidate_revision_id": str(
                            getattr(candidate, "candidate_revision_id", "") or ""
                        ),
                        "opportunity_lease_id": str(
                            getattr(candidate, "opportunity_lease_id", "") or ""
                        ),
                    }
                )
                if record_result and emit_events:
                    self._append_snapshot_freshness_decision_event(
                        payload=payload,
                        event_kind=event_kind,
                        now_ms=now_ms,
                    )
                if failure.get("decision") == "skip_entry":
                    blocking = True
                    blockers[reason] += 1
                    if len(samples) < 24:
                        sample = {
                            "pair_id": pair_id,
                            "candidate_pair_id": pair_id,
                            "candidate_symbol": symbol,
                            "venue": str(failure.get("venue", "")),
                            "symbol": str(failure.get("symbol", "")),
                            "domain": str(failure.get("domain", "")),
                            "source": str(failure.get("source", "")),
                            "reason": reason,
                            "source_age_ms": int(failure.get("age_ms", 0) or 0),
                            "fallback_duration_ms": fallback_duration_ms,
                            "blocked": True,
                            "block_reason": reason,
                            "age_ms": int(failure.get("age_ms", 0) or 0),
                            "budget_ms": int(failure.get("budget_ms", 0) or 0),
                            "blocking_stage": "entry_snapshot_freshness",
                            "blocking_domain": str(failure.get("domain", "")),
                            "blocking_status": str(failure.get("decision", "")),
                            "blocking_reason": reason,
                            "sample_id": str(failure.get("sample_id", "") or ""),
                            "candidate_revision_id": str(
                                getattr(candidate, "candidate_revision_id", "") or ""
                            ),
                            "opportunity_lease_id": str(
                                getattr(candidate, "opportunity_lease_id", "") or ""
                            ),
                        }
                        sample.update(self._snapshot_freshness_evidence_fields(failure))
                        samples.append(sample)
            if not blocking:
                filtered.append(candidate)
        return filtered

    def _snapshot_health_payload(
        self,
        *,
        snapshot,
        now_ms: int,
        max_age_ms: int,
        freshness: str,
    ) -> dict:
        from collections import Counter as _Counter
        import hashlib

        per_venue_quote_count: _Counter[str] = _Counter()
        per_venue_candidate_count: _Counter[str] = _Counter()
        for quote in getattr(snapshot, "quotes", {}).values():
            venue = str(getattr(quote, "venue", "") or "")
            if venue:
                per_venue_quote_count[venue] += 1
        for candidate in getattr(snapshot, "candidates", []) or []:
            for venue_attr in ("long_venue", "short_venue"):
                venue = str(getattr(candidate, venue_attr, "") or "")
                if venue:
                    per_venue_candidate_count[venue] += 1

        published_at_ms = self._snapshot_publication_at_ms(snapshot)
        ready_at_ms = int(getattr(snapshot, "ready_at_ms", 0) or 0)
        candidate_build_at_ms = int(
            getattr(snapshot, "candidate_build_observed_at_ms", 0) or 0
        )
        market_observed_at_ms = int(getattr(snapshot, "market_observed_at_ms", 0) or 0)
        snapshot_publish_age_ms = now_ms - published_at_ms if published_at_ms > 0 else 0
        market_observed_age_ms = (
            now_ms - market_observed_at_ms if market_observed_at_ms > 0 else 0
        )
        market_max_age_ms = int(self.ctx.config.runtime.max_market_age_ms or max_age_ms)
        degraded_domains = [str(v) for v in getattr(snapshot, "degraded_domains", []) or []]
        degraded_venues = [str(v) for v in getattr(snapshot, "degraded_venues", []) or []]
        degraded_symbols = getattr(snapshot, "degraded_symbols", {}) or {}
        candidate_diagnostics = dict(
            getattr(snapshot, "candidate_build_diagnostics", {}) or {}
        )

        def _diagnostic_int(name: str) -> int:
            value = candidate_diagnostics.get(name, 0)
            if isinstance(value, bool):
                return 0
            try:
                return int(value or 0)
            except (TypeError, ValueError):
                return 0

        top_degraded_symbols: list[str] = []
        if isinstance(degraded_symbols, dict):
            for symbols in degraded_symbols.values():
                for symbol in symbols:
                    symbol_s = str(symbol)
                    if symbol_s and symbol_s not in top_degraded_symbols:
                        top_degraded_symbols.append(symbol_s)
                    if len(top_degraded_symbols) >= 24:
                        break
                if len(top_degraded_symbols) >= 24:
                    break

        domains = list(degraded_domains)
        if snapshot_publish_age_ms > max_age_ms:
            domains.append("snapshot_publish_stale")
        if market_observed_age_ms > market_max_age_ms:
            domains.append("market_observed_stale")
        for lifecycle_name, rows in (
            ("market", getattr(snapshot, "market_lifecycle", []) or []),
            ("funding", getattr(snapshot, "funding_lifecycle", []) or []),
            ("liquidity", getattr(snapshot, "liquidity_lifecycle", []) or []),
            ("transfer", getattr(snapshot, "transfer_lifecycle", []) or []),
        ):
            for row in rows:
                reason = str(getattr(row, "degraded_reason", "") or "")
                if reason and lifecycle_name not in domains:
                    domains.append(lifecycle_name)

        snapshot_path = str(self.ctx.config.runtime.sidecar_snapshot_path)
        config_hash = hashlib.sha256(
            f"{snapshot_path}|{max_age_ms}|{self.ctx.config.runtime.mode}".encode()
        ).hexdigest()[:12]
        fallback_duration_ms = self._snapshot_fallback_duration_ms(
            snapshot=snapshot,
            now_ms=now_ms,
            max_age_ms=max_age_ms,
        )
        fresh_source_ages = []
        for quote in getattr(snapshot, "quotes", {}).values():
            observed_at_ms = self._snapshot_quote_direct_observed_at_ms(quote)
            if observed_at_ms > 0:
                age_ms = max(now_ms - observed_at_ms, 0)
                if age_ms <= self._snapshot_domain_budget_ms("quote"):
                    fresh_source_ages.append(age_ms)
        fresh_source_age_ms = min(fresh_source_ages) if fresh_source_ages else 0
        acquisition_to_publish_latency_ms = (
            max(published_at_ms - market_observed_at_ms, 0)
            if published_at_ms > 0 and market_observed_at_ms > 0
            else 0
        )
        stage_latency_ms = {
            "market_observed_to_candidate_build": (
                max(candidate_build_at_ms - market_observed_at_ms, 0)
                if candidate_build_at_ms > 0 and market_observed_at_ms > 0
                else 0
            ),
            "candidate_build_to_ready": (
                max(ready_at_ms - candidate_build_at_ms, 0)
                if ready_at_ms > 0 and candidate_build_at_ms > 0
                else 0
            ),
            "ready_to_publish": (
                max(published_at_ms - ready_at_ms, 0)
                if published_at_ms > 0 and ready_at_ms > 0
                else 0
            ),
            "market_observed_to_publish": acquisition_to_publish_latency_ms,
        }
        stage_latency_quantiles_ms = {
            stage: self._snapshot_latency_quantile_summary_ms(
                f"stage:{stage}",
                latency_ms,
            )
            for stage, latency_ms in stage_latency_ms.items()
        }
        acquisition_latency_summary_ms = self._snapshot_latency_quantile_summary_ms(
            "acquisition_to_publish",
            acquisition_to_publish_latency_ms,
        )
        (
            candidate_scope_candidates,
            candidate_scope_mode,
            candidate_scope_all_count,
            candidate_scope_skipped_count,
        ) = self._snapshot_health_candidate_scope_candidates(snapshot)

        return {
            "freshness": freshness,
            "venues": degraded_venues,
            "degraded_venues": degraded_venues,
            "degraded_domains": degraded_domains,
            "stale_degraded_domains": domains,
            "top_degraded_symbols": top_degraded_symbols,
            "snapshot_publish_age_ms": max(snapshot_publish_age_ms, 0),
            "market_observed_age_ms": max(market_observed_age_ms, 0),
            "snapshot_stage_timestamps_ms": {
                "market_observed_at_ms": market_observed_at_ms,
                "candidate_build_observed_at_ms": candidate_build_at_ms,
                "ready_at_ms": ready_at_ms,
                "published_at_ms": published_at_ms,
            },
            "snapshot_stage_latency_ms": stage_latency_ms,
            "snapshot_stage_latency_quantiles_ms": stage_latency_quantiles_ms,
            "snapshot_acquisition_to_publish_latency_ms": (
                acquisition_to_publish_latency_ms
            ),
            "snapshot_acquisition_to_publish_latency_quantiles_ms": (
                acquisition_latency_summary_ms
            ),
            "fallback_duration_ms": fallback_duration_ms,
            "last_good_age_ms": max(snapshot_publish_age_ms, 0),
            "fresh_source_age_ms": fresh_source_age_ms,
            "candidate_freshness_candidate_scope": candidate_scope_mode,
            "candidate_freshness_candidate_count": len(candidate_scope_candidates),
            "candidate_freshness_all_candidate_count": candidate_scope_all_count,
            "candidate_freshness_skipped_untracked_count": candidate_scope_skipped_count,
            "candidate_freshness_scope": self._snapshot_health_candidate_freshness_scope(
                snapshot=snapshot,
                now_ms=now_ms,
                degraded_domains=degraded_domains,
                stale_degraded_domains=domains,
                fallback_duration_ms=fallback_duration_ms,
                candidates=candidate_scope_candidates,
            ),
            "per_venue_quote_count": dict(sorted(per_venue_quote_count.items())),
            "per_venue_candidate_count": dict(sorted(per_venue_candidate_count.items())),
            "source_mode": str(getattr(snapshot, "source_mode", "") or ""),
            "acquisition_mode": str(getattr(snapshot, "acquisition_mode", "") or ""),
            "source_data_ready": candidate_diagnostics.get("source_data_ready") is True,
            "eligible_frontier_complete": (
                candidate_diagnostics.get("eligible_frontier_complete") is True
            ),
            "entry_frontier_ready": (
                candidate_diagnostics.get("entry_frontier_ready") is True
            ),
            "frontier_input_pair_count": _diagnostic_int("seed_pair_count"),
            "pair_decision_count": _diagnostic_int("pair_decision_count"),
            "eligible_candidate_count": _diagnostic_int(
                "eligible_candidate_count"
            ),
            "omitted_eligible_count": _diagnostic_int(
                "omitted_eligible_count"
            ),
            "frontier_stop_reason": str(
                candidate_diagnostics.get("frontier_stop_reason", "") or ""
            ),
            "snapshot_path": snapshot_path,
            "config_hash": config_hash,
            "ts_ms": now_ms,
        }
