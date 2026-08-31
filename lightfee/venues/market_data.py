"""Credential-free public market data client for V2 sidecar.

MarketDataClient provides public HTTP access to exchange funding, ticker,
order-book, and liquidity data. It never requires LiveCredential and never
touches order/position/account endpoints.

VenueTransport inherits from MarketDataClient for its public-data needs
while adding private trading methods (order, position, account risk).
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Any, Mapping, Optional

import httpx

from lightfee.core.domain import Venue
from lightfee.venues.specs import VenueSpec


# ---------------------------------------------------------------------------
# Unified sidecar output types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FundingTicker:
    venue: str
    symbol: str
    bid: float
    ask: float
    bid_size: float = 0.0
    ask_size: float = 0.0
    mark_price: float = 0.0
    index_price: float = 0.0
    funding_rate_bps: float = 0.0
    funding_timestamp_ms: int = 0
    volume_24h_quote: float = 0.0
    open_interest_quote: float = 0.0
    open_interest_evidence_status: str = "available"
    open_interest_evidence_reason: str = ""
    open_interest_http_status_code: int = 0
    open_interest_retry_after_ms: int = 0
    open_interest_request_phase: str = ""
    open_interest_transport_error_type: str = ""
    open_interest_transport_error_detail: str = ""
    open_interest_transport_error_cause_type: str = ""
    open_interest_transport_error_cause: str = ""
    open_interest_client_generation: int = 0
    open_interest_client_retired: bool = False
    oi_candidate_count: int = 0
    oi_cache_hit_count: int = 0
    oi_cache_miss_count: int = 0
    oi_refresh_attempt_count: int = 0
    oi_refresh_cap: int = 0
    oi_deferred_count: int = 0
    oi_timeout_count: int = 0
    oi_refresh_elapsed_ms: int = 0


@dataclass(frozen=True)
class PerpLiquidity:
    venue: str
    symbol: str
    volume_24h_quote: float
    open_interest_quote: float
    observed_at_ms: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_float(value: Any, *, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, str) and value.strip() == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _now_ms() -> int:
    return int(time.time() * 1000)


# V1 parity: Binance-compatible OI is a per-symbol endpoint, fetched with bounded
# concurrency and normalized to quote notional via premiumIndex mark price.
_BINANCE_STYLE_OPEN_INTEREST_CONCURRENCY = 16
BINANCE_STYLE_OPEN_INTEREST_ENRICHMENT_BUDGET_S = 0.1
BINANCE_STYLE_ENTRY_OPEN_INTEREST_BUDGET_S = 2.0
BINANCE_STYLE_OPEN_INTEREST_CACHE_MAX_AGE_MS = 10 * 60 * 1_000
# A failed sidecar OI sample is still fail-closed evidence, but retrying every
# symbol on every three-second cycle turns one venue outage into a socket storm.
# Match V1's one-minute OI refresh cadence for this bounded retry cooldown.
BINANCE_STYLE_OPEN_INTEREST_FAILURE_CACHE_MAX_AGE_MS = 60 * 1_000
BINANCE_STYLE_OPEN_INTEREST_REFRESH_CAP = 128
_BINANCE_STYLE_TRANSIENT_OPEN_INTEREST_STATUSES = frozenset(
    {"timeout", "rate_limited", "http_error"}
)
# V1 multi-symbol contract: bulk snapshots may use only a small, bounded
# funding REST fallback when the cache is cold.  Do not fan out one request per
# requested symbol and then cancel the batch to protect quote publication.
OKX_BULK_FUNDING_REST_FALLBACK_LIMIT = 4
_OKX_FUNDING_RATE_PER_SYMBOL_TIMEOUT_S = 6.0

# Binance-style OI is the largest remaining bounded public fan-out (16).
MARKET_DATA_MAX_CONNECTIONS = 48

# V1 parity: OKX funding cache TTL (10 min) — src/live/okx.rs OKX_FUNDING_CACHE_MAX_OBSERVED_AGE_MS
_FUNDING_CACHE_MAX_OBSERVED_AGE_MS = 10 * 60 * 1_000  # 10 minutes
# V1 parity: funding timestamp must be at least this far in the future to be cache-usable.
# When funding just settled, the exchange publishes the next funding time, so stale-on-settlement
# avoids using a just-expired timestamp.
_FUNDING_CACHE_MIN_FUTURE_MS = 30_000  # 30 seconds


def _next_hour_boundary(now_ms: int) -> int:
    """Round up to the next hour boundary in milliseconds.

    V1 anchor: src/live/hyperliquid.rs  next_hour_boundary
    Hyperliquid funding settles at the top of each hour.
    """
    hour_ms = 3_600_000
    return ((now_ms // hour_ms) + 1) * hour_ms


def _funding_timestamp_ms(item: dict[str, Any], *, fallback_ms: int = 0) -> int:
    """Return the first explicit funding timestamp field, or a venue fallback."""
    for key in ("nextFundingTime", "fundingTime"):
        ts_ms = int(_safe_float(item.get(key, 0)))
        if ts_ms > 0:
            return ts_ms
    return fallback_ms


def _funding_timestamp_ms_or_seconds(value: Any) -> int:
    """Normalize an explicit exchange funding time without inventing one."""
    ts = int(_safe_float(value, default=0.0))
    if ts <= 0:
        return 0
    return ts * 1000 if ts < 10_000_000_000 else ts


# ---------------------------------------------------------------------------
# MarketDataClient
# ---------------------------------------------------------------------------


class MarketDataClient:
    """Credential-free public market data access.

    Construct with a VenueSpec and optional httpx client settings.
    Never validates credentials — this is for public endpoints only.
    """

    def __init__(
        self,
        spec: VenueSpec,
        exchange_http_timeout_ms: int = 10000,
        rate_limiter: Optional[object] = None,
    ) -> None:
        self._spec = spec
        self._exchange_http_timeout_ms = exchange_http_timeout_ms
        self._rate_limiter = rate_limiter
        self._client: Optional[httpx.AsyncClient] = None
        # Retire a failed shared client only after its in-flight borrowers
        # release it, so one peer-side failure cannot interrupt a healthy peer.
        self._client_lifecycle_lock = asyncio.Lock()
        self._client_generation = 0
        self._client_generations: dict[int, int] = {}
        self._client_leases: dict[int, int] = {}
        self._retired_clients: dict[int, httpx.AsyncClient] = {}
        # V1 parity: per-symbol funding rate cache (OKX, etc.)
        # {venue_key:symbol -> FundingTicker} with observed_at_ms
        self._funding_cache: dict[str, tuple[float, int, int]] = {}  # (rate_bps, timestamp_ms, observed_at_ms)
        self._funding_cache_observed_at_ms: int = 0
        # V2 has no V1-equivalent persistent public funding stream.  Advance
        # the bounded REST fallback across cache misses so cold symbols are not
        # permanently starved while retaining V1's four-request ceiling.
        self._okx_funding_refresh_after_symbol: str | None = None
        # V1-style evidence cache for Binance-compatible per-symbol OI.
        # key -> (open_interest_quote, mark_price, observed_at_ms, status, reason)
        self._binance_style_open_interest_cache: dict[
            str, tuple[float, float, int, str, str]
        ] = {}

    @property
    def venue(self) -> Venue:
        return self._spec.venue_id

    @property
    def spec(self) -> VenueSpec:
        return self._spec

    # ------------------------------------------------------------------
    # HTTP lifecycle
    # ------------------------------------------------------------------

    async def _get_client(self) -> httpx.AsyncClient:
        async with self._client_lifecycle_lock:
            if self._client is None:
                timeout_s = self._exchange_http_timeout_ms / 1000.0
                self._client = httpx.AsyncClient(
                    timeout=httpx.Timeout(timeout_s),
                    limits=httpx.Limits(
                        max_connections=MARKET_DATA_MAX_CONNECTIONS,
                        max_keepalive_connections=4,
                    ),
                )
            self._register_client_locked(self._client)
            return self._client

    def _register_client_locked(self, client: httpx.AsyncClient) -> int:
        """Return the stable generation for ``client`` while holding the lock."""
        key = id(client)
        generation = self._client_generations.get(key)
        if generation is None:
            self._client_generation += 1
            generation = self._client_generation
            self._client_generations[key] = generation
        return generation

    async def _borrow_client(self) -> tuple[httpx.AsyncClient, int]:
        """Lease a client so retirement cannot interrupt another request."""
        client = await self._get_client()
        async with self._client_lifecycle_lock:
            generation = self._register_client_locked(client)
            key = id(client)
            self._client_leases[key] = self._client_leases.get(key, 0) + 1
            return client, generation

    async def _release_client(self, client: httpx.AsyncClient) -> None:
        client_to_close: httpx.AsyncClient | None = None
        async with self._client_lifecycle_lock:
            key = id(client)
            remaining = self._client_leases.get(key, 0) - 1
            if remaining > 0:
                self._client_leases[key] = remaining
            else:
                self._client_leases.pop(key, None)
                client_to_close = self._retired_clients.pop(key, None)
                if client_to_close is not None:
                    self._client_generations.pop(key, None)
        if client_to_close is not None:
            await client_to_close.aclose()

    async def _retire_failed_client(self, client: httpx.AsyncClient) -> bool:
        """Remove a peer-failed client from reuse and close it after all leases end."""
        client_to_close: httpx.AsyncClient | None = None
        retired = False
        async with self._client_lifecycle_lock:
            if self._client is client:
                self._client = None
                retired = True
            key = id(client)
            already_retired = key in self._retired_clients
            if retired or already_retired:
                self._retired_clients[key] = client
                if self._client_leases.get(key, 0) == 0:
                    client_to_close = self._retired_clients.pop(key)
                    self._client_generations.pop(key, None)
        if client_to_close is not None:
            await client_to_close.aclose()
        return retired or already_retired

    def _network_failure_diagnostics(
        self,
        exc: BaseException,
        *,
        client_generation: int,
        client_retired: bool,
    ) -> dict[str, Any]:
        """Return safe, structured diagnostics for an httpx transport failure."""
        if isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout)):
            phase = "connect"
        elif isinstance(exc, (httpx.ReadError, httpx.ReadTimeout)):
            phase = "read"
        elif isinstance(exc, (httpx.WriteError, httpx.WriteTimeout)):
            phase = "write"
        elif isinstance(exc, httpx.PoolTimeout):
            phase = "pool"
        else:
            phase = "network"

        cause = exc.__cause__ or exc.__context__
        seen = {id(exc)}
        while cause is not None and id(cause) not in seen:
            seen.add(id(cause))
            next_cause = cause.__cause__ or cause.__context__
            if next_cause is None or id(next_cause) in seen:
                break
            cause = next_cause

        return {
            "request_phase": phase,
            "transport_error_type": type(exc).__name__,
            "transport_error_detail": str(exc)[:200],
            "transport_error_cause_type": (
                type(cause).__name__ if cause is not None else ""
            ),
            "transport_error_cause": str(cause)[:200] if cause is not None else "",
            "client_generation": client_generation,
            "client_retired": client_retired,
        }

    @staticmethod
    def _network_failure_message(
        prefix: str,
        method: str,
        path: str,
        diagnostics: Mapping[str, Any],
    ) -> str:
        cause_type = str(diagnostics.get("transport_error_cause_type", "") or "")
        cause = str(diagnostics.get("transport_error_cause", "") or "")
        detail = str(diagnostics.get("transport_error_detail", "") or "")
        cause_summary = cause_type or "none"
        if cause:
            cause_summary = f"{cause_summary}: {cause}"
        elif detail:
            cause_summary = f"{cause_summary}: {detail}"
        return (
            f"{prefix}: {method} {path}; "
            f"phase={diagnostics.get('request_phase', 'network')}; "
            f"error={diagnostics.get('transport_error_type', 'HTTPError')}; "
            f"cause={cause_summary}; "
            f"client_generation={diagnostics.get('client_generation', 0)}; "
            f"client_retired={bool(diagnostics.get('client_retired', False))}"
        )

    async def close(self) -> None:
        async with self._client_lifecycle_lock:
            clients = {
                id(client): client
                for client in [self._client, *self._retired_clients.values()]
                if client is not None
            }
            self._client = None
            self._client_generations.clear()
            self._client_leases.clear()
            self._retired_clients.clear()
        for client in clients.values():
            await client.aclose()

    # ------------------------------------------------------------------
    # Symbol conversion
    # ------------------------------------------------------------------

    def _to_venue_symbol(self, symbol: str) -> str:
        fn = self._spec.symbol_to_venue
        return fn(symbol) if fn else symbol

    def _from_venue_symbol(self, symbol: str) -> str:
        fn = self._spec.symbol_from_venue
        return fn(symbol) if fn else symbol

    # ------------------------------------------------------------------
    # Public HTTP request
    # ------------------------------------------------------------------

    def _public_rate_limit_scopes(self, method: str, path: str) -> list[str]:
        """Derive rate-limit scopes for a public request."""
        from lightfee.rate_limit.config import built_in_defaults
        from lightfee.rate_limit.engine import global_rate_limit_runtime as _get_global_rt

        endpoint = f"{method.upper()} {path}"
        venue_id = self._spec.venue_id.value
        scopes = [endpoint, f"venue:{venue_id}"]

        global_rt = _get_global_rt()
        config = global_rt.config_manager.config if (global_rt is not None and global_rt.config_manager is not None) else built_in_defaults()

        venue_config = config.venues.get(venue_id) if config else None
        if venue_config is not None:
            scope_map = getattr(venue_config, "scopes", {}) or {}
            if endpoint in scope_map:
                group_name = scope_map[endpoint]
                scopes.append(f"group:{venue_id}:{group_name}")
                scopes.append(f"group:{group_name}")
        return scopes

    async def _public_get(self, path: str, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        """Execute a public GET request with rate-limit pacing."""
        return await self._public_request("GET", path, params=params)

    async def _public_post(self, path: str, body: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        """Execute a public POST request with rate-limit pacing."""
        return await self._public_request("POST", path, body=body)

    async def _public_request(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
        body: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Send a public HTTP request with scoped rate limiting."""
        base_url = self._spec.public_base_url
        scopes = self._public_rate_limit_scopes(method, path)

        if self._rate_limiter is not None:
            await self._rate_limiter.wait_until_ready_for_scopes(scopes)
            await self._rate_limiter.pace_for_scopes(scopes)

        from lightfee.rate_limit.engine import global_rate_limit_runtime as _get_global_rt
        global_rt = _get_global_rt()
        if global_rt is not None:
            await global_rt.async_wait_until_ready_for_scopes(scopes)

        url = base_url + path
        client, client_generation = await self._borrow_client()
        try:
            if method.upper() == "GET":
                resp = await client.get(url, params=params)
            elif method.upper() == "POST":
                import json
                resp = await client.post(url, json=body)
            else:
                raise ValueError(f"unsupported method: {method}")

            if resp.status_code >= 400:
                retry_after = None
                if resp.status_code in (429, 418):
                    retry_after = _parse_retry_after_ms(dict(resp.headers))
                    if self._rate_limiter is not None:
                        self._rate_limiter.record_rate_limit_for_scopes(
                            scopes,
                            retry_after_ms=retry_after,
                        )
                    if global_rt is not None:
                        global_rt.record_rate_limit_for_scopes(
                            scopes,
                            retry_after_ms=retry_after,
                        )
                raise PublicTransportError(
                    PublicTransportErrorCategory.TRANSPORT_FAILURE,
                    f"HTTP {resp.status_code}: {resp.text[:200]}",
                    status_code=resp.status_code,
                    retry_after_ms=int(retry_after or 0),
                )

            if self._rate_limiter is not None:
                self._rate_limiter.record_success_for_scopes(scopes)

            if not resp.text:
                return {}
            return resp.json()
        except httpx.TimeoutException as exc:
            diagnostics = self._network_failure_diagnostics(
                exc,
                client_generation=client_generation,
                client_retired=False,
            )
            raise PublicTransportError(
                PublicTransportErrorCategory.TIMEOUT,
                self._network_failure_message("timeout", method, path, diagnostics),
                **diagnostics,
            ) from exc
        except httpx.NetworkError as e:
            retired = await self._retire_failed_client(client)
            diagnostics = self._network_failure_diagnostics(
                e,
                client_generation=client_generation,
                client_retired=retired,
            )
            raise PublicTransportError(
                PublicTransportErrorCategory.TRANSPORT_FAILURE,
                self._network_failure_message("network", method, path, diagnostics),
                **diagnostics,
            ) from e
        except asyncio.CancelledError:
            await self._retire_failed_client(client)
            raise
        finally:
            await self._release_client(client)

    # ------------------------------------------------------------------
    # Funding ticker fetch — main sidecar entry point
    # ------------------------------------------------------------------

    async def fetch_funding_tickers(self, symbols: list[str]) -> dict[str, FundingTicker]:
        """Fetch funding + bid/ask + mark + volume + OI for requested symbols.

        Returns {venue_key:symbol -> FundingTicker}.
        """
        venue_id = self._spec.venue_id

        if venue_id in (Venue.BINANCE, Venue.ASTER):
            return await self._fetch_binance_style(symbols)
        elif venue_id == Venue.OKX:
            return await self._fetch_okx_style(symbols)
        elif venue_id == Venue.BYBIT:
            return await self._fetch_bybit_style(symbols)
        elif venue_id == Venue.BITGET:
            return await self._fetch_bitget_style(symbols)
        elif venue_id == Venue.GATE:
            return await self._fetch_gate_style(symbols)
        elif venue_id == Venue.HYPERLIQUID:
            return await self._fetch_hyperliquid_style(symbols)
        return {}

    async def fetch_entry_open_interest_evidence(
        self,
        symbols: list[str],
        *,
        mark_prices: Mapping[str, float],
    ) -> dict[str, FundingTicker]:
        """Fetch candidate-scoped OI evidence without widening sidecar scope.

        Binance-compatible venues expose OI as a mandatory per-symbol public
        endpoint. Entry revalidation can therefore refresh only candidate
        symbols that missed sidecar enrichment, instead of inheriting a
        full-universe cap/timeout result.
        """
        scoped_symbols = [
            str(symbol or "").strip().upper()
            for symbol in symbols
            if str(symbol or "").strip()
        ]
        scoped_symbols = list(dict.fromkeys(scoped_symbols))
        if not scoped_symbols:
            return {}
        venue = self._spec.venue_id
        venue_key = venue.value
        if venue not in (Venue.BINANCE, Venue.ASTER):
            return {
                f"{venue_key}:{symbol}": FundingTicker(
                    venue=venue_key,
                    symbol=symbol,
                    bid=0.0,
                    ask=0.0,
                    open_interest_evidence_status="unsupported",
                    open_interest_evidence_reason="unsupported_targeted_refresh",
                    oi_candidate_count=len(scoped_symbols),
                )
                for symbol in scoped_symbols
            }

        now_ms = _now_ms()
        budget_s = max(
            float(
                getattr(
                    self,
                    "binance_style_open_interest_enrichment_budget_s",
                    BINANCE_STYLE_ENTRY_OPEN_INTEREST_BUDGET_S,
                )
                or 0.0
            ),
            0.0,
        )

        async def fetch_symbol(symbol: str) -> tuple[str, FundingTicker]:
            started_ms = _now_ms()
            venue_symbol = self._to_venue_symbol(symbol)
            mark_price = _safe_float(
                mark_prices.get(symbol, mark_prices.get(venue_symbol, 0.0))
            )
            common = {
                "venue": venue_key,
                "symbol": symbol,
                "bid": 0.0,
                "ask": 0.0,
                "mark_price": mark_price,
                "oi_candidate_count": len(scoped_symbols),
                "oi_refresh_cap": len(scoped_symbols),
            }
            if not self._spec.open_interest_path:
                return symbol, FundingTicker(
                    **common,
                    open_interest_evidence_status="unsupported",
                    open_interest_evidence_reason="open_interest_endpoint_unavailable",
                )
            if mark_price <= 0.0:
                return symbol, FundingTicker(
                    **common,
                    open_interest_evidence_status="missing_mark_price",
                    open_interest_evidence_reason="snapshot_mark_price_unavailable",
                )
            cached = self._binance_style_cached_open_interest(
                venue_symbol,
                mark_price=mark_price,
                now_ms=now_ms,
            )
            if cached is not None and cached[1] == "available":
                open_interest_quote, status, reason = cached
                return symbol, FundingTicker(
                    **common,
                    open_interest_quote=open_interest_quote,
                    open_interest_evidence_status=status,
                    open_interest_evidence_reason=reason or "cache_hit",
                    oi_cache_hit_count=1,
                )
            if budget_s <= 0.0:
                return symbol, FundingTicker(
                    **common,
                    open_interest_evidence_status="timeout",
                    open_interest_evidence_reason="targeted_refresh_budget_exhausted",
                    oi_cache_miss_count=1,
                    oi_timeout_count=1,
                )
            try:
                raw = await asyncio.wait_for(
                    self._public_get(
                        self._spec.open_interest_path,
                        params={"symbol": venue_symbol},
                    ),
                    timeout=budget_s,
                )
            except (asyncio.TimeoutError, TimeoutError):
                return symbol, FundingTicker(
                    **common,
                    open_interest_evidence_status="timeout",
                    open_interest_evidence_reason="timeout_waiting_for_oi",
                    oi_cache_miss_count=1,
                    oi_refresh_attempt_count=1,
                    oi_timeout_count=1,
                    oi_refresh_elapsed_ms=max(_now_ms() - started_ms, 0),
                )
            except Exception as exc:
                status = self._binance_style_oi_status_from_error(exc)
                return symbol, FundingTicker(
                    **common,
                    open_interest_evidence_status=status,
                    open_interest_evidence_reason=str(exc)[:200] or status,
                    open_interest_http_status_code=int(
                        getattr(exc, "status_code", 0) or 0
                    ),
                    open_interest_retry_after_ms=int(
                        getattr(exc, "retry_after_ms", 0) or 0
                    ),
                    open_interest_request_phase=str(
                        getattr(exc, "request_phase", "") or ""
                    ),
                    open_interest_transport_error_type=str(
                        getattr(exc, "transport_error_type", "") or ""
                    ),
                    open_interest_transport_error_detail=str(
                        getattr(exc, "transport_error_detail", "") or ""
                    ),
                    open_interest_transport_error_cause_type=str(
                        getattr(exc, "transport_error_cause_type", "") or ""
                    ),
                    open_interest_transport_error_cause=str(
                        getattr(exc, "transport_error_cause", "") or ""
                    ),
                    open_interest_client_generation=int(
                        getattr(exc, "client_generation", 0) or 0
                    ),
                    open_interest_client_retired=bool(
                        getattr(exc, "client_retired", False)
                    ),
                    oi_cache_miss_count=1,
                    oi_refresh_attempt_count=1,
                    oi_timeout_count=1 if status == "timeout" else 0,
                    oi_refresh_elapsed_ms=max(_now_ms() - started_ms, 0),
                )
            item = raw[0] if isinstance(raw, list) and raw else raw
            if not isinstance(item, dict):
                return symbol, FundingTicker(
                    **common,
                    open_interest_evidence_status="parse_error",
                    open_interest_evidence_reason="invalid_open_interest_payload",
                    oi_cache_miss_count=1,
                    oi_refresh_attempt_count=1,
                    oi_refresh_elapsed_ms=max(_now_ms() - started_ms, 0),
                )
            open_interest = _safe_float(item.get("openInterest", 0.0))
            open_interest_quote = open_interest * mark_price
            if open_interest_quote <= 0.0:
                return symbol, FundingTicker(
                    **common,
                    open_interest_evidence_status="parse_error",
                    open_interest_evidence_reason="nonpositive_open_interest",
                    oi_cache_miss_count=1,
                    oi_refresh_attempt_count=1,
                    oi_refresh_elapsed_ms=max(_now_ms() - started_ms, 0),
                )
            self._binance_style_store_open_interest(
                venue_symbol,
                open_interest_quote=open_interest_quote,
                mark_price=mark_price,
                observed_at_ms=now_ms,
                status="available",
                reason="fresh_targeted_refresh",
            )
            return symbol, FundingTicker(
                **common,
                open_interest_quote=open_interest_quote,
                open_interest_evidence_status="available",
                open_interest_evidence_reason="fresh_targeted_refresh",
                oi_cache_miss_count=1,
                oi_refresh_attempt_count=1,
                oi_refresh_elapsed_ms=max(_now_ms() - started_ms, 0),
            )

        refreshed = await asyncio.gather(
            *(fetch_symbol(symbol) for symbol in scoped_symbols)
        )
        return {
            f"{venue_key}:{symbol}": ticker
            for symbol, ticker in refreshed
        }

    # ------------------------------------------------------------------
    # Per-symbol L2 snapshot
    # ------------------------------------------------------------------

    async def fetch_l2_snapshot(
        self, symbol: str, depth: int = 50,
        retry_initial_ms: int = 5000, retry_max_ms: int = 40000, max_retries: int = 8,
    ) -> dict[str, Any]:
        """Fetch REST order book depth snapshot (public)."""
        from lightfee.marketdata.local_l2_venues import parse_l2_update

        spec = self._spec
        venue_sym = self._to_venue_symbol(symbol)
        now_ms = _now_ms()

        if not spec.l2_snapshot_path:
            raise PublicTransportError(PublicTransportErrorCategory.UNSUPPORTED_CAPABILITY,
                                 f"L2 snapshot not supported for {spec.venue_id.value}")

        failures = 0
        while True:
            try:
                if spec.venue_id == Venue.HYPERLIQUID:
                    body = {"type": "l2Book", "coin": venue_sym}
                    raw = await self._public_post(spec.l2_snapshot_path, body=body)
                else:
                    params: dict[str, Any] = {}
                    if spec.venue_id in (Venue.BINANCE, Venue.ASTER):
                        params["symbol"] = venue_sym
                        params["limit"] = str(depth)
                    elif spec.venue_id == Venue.OKX:
                        params["instId"] = venue_sym
                        params["sz"] = str(depth)
                    elif spec.venue_id == Venue.BYBIT:
                        params["category"] = "linear"
                        params["symbol"] = venue_sym
                        params["limit"] = str(depth)
                    elif spec.venue_id == Venue.BITGET:
                        params["category"] = "USDT-FUTURES"
                        params["symbol"] = venue_sym
                        params["limit"] = str(depth)
                    elif spec.venue_id == Venue.GATE:
                        params["contract"] = venue_sym
                        params["limit"] = str(depth)
                    raw = await self._public_get(spec.l2_snapshot_path, params=params)

                result = parse_l2_update(spec.venue_id.value, payload=raw, symbol=venue_sym, now_ms=now_ms)
                result.symbol = symbol
                return {"venue": spec.venue_id.value, "symbol": symbol,
                        "bids": getattr(result, "bids", []), "asks": getattr(result, "asks", []),
                        "received_at_ms": now_ms}
            except PublicTransportError:
                failures += 1
                if failures > max_retries:
                    raise
                shift = min(failures - 1, 20)
                backoff_ms = min(retry_initial_ms << shift, retry_max_ms)
                jitter = random.randint(0, max(backoff_ms // 5, 1))
                await asyncio.sleep((backoff_ms + jitter) / 1000.0)

    # ------------------------------------------------------------------
    # Market snapshot (V1-compatible)
    # ------------------------------------------------------------------

    async def fetch_market_snapshot(self, symbols: list[str]) -> dict[str, Any]:
        """Fetch venue market snapshot. Returns raw dict for V1 compat."""
        spec = self._spec
        now_ms = _now_ms()

        try:
            raw = await self._public_get(spec.market_snapshot_path)
        except PublicTransportError:
            raise
        except Exception as e:
            raise PublicTransportError(PublicTransportErrorCategory.TRANSPORT_FAILURE,
                                 f"market snapshot failed: {e}")

        return {"raw": raw, "observed_at_ms": now_ms, "venue": spec.venue_id.value}

    # ------------------------------------------------------------------
    # Transfer status (initially empty, structurally compatible)
    # ------------------------------------------------------------------

    async def fetch_transfer_statuses(self, assets: list[str]) -> list[dict[str, Any]]:
        """Fetch transfer statuses. Returns empty list — structurally compatible,
        not a placeholder sentinel."""
        return []

    # ==================================================================
    # Per-venue fetchers
    # ==================================================================

    # -- Binance / Aster (shared parser) ---------------------------------

    @staticmethod
    def _binance_style_oi_status_from_error(exc: Exception) -> str:
        return open_interest_evidence_status_from_error(exc)

    def _binance_style_oi_cache_key(self, venue_sym: str) -> str:
        return f"{self._spec.venue_id.value}:{venue_sym}"

    def _binance_style_cached_open_interest(
        self,
        venue_sym: str,
        *,
        mark_price: float,
        now_ms: int,
    ) -> tuple[float, str, str] | None:
        entry = self._binance_style_open_interest_cache.get(
            self._binance_style_oi_cache_key(venue_sym)
        )
        if entry is None:
            return None
        open_interest_quote, cached_mark_price, observed_at_ms, status, reason = entry
        max_age_ms = (
            BINANCE_STYLE_OPEN_INTEREST_CACHE_MAX_AGE_MS
            if status == "available"
            else BINANCE_STYLE_OPEN_INTEREST_FAILURE_CACHE_MAX_AGE_MS
        )
        if now_ms - int(observed_at_ms or 0) > max_age_ms:
            return None
        if mark_price <= 0.0 or cached_mark_price <= 0.0:
            return None
        return open_interest_quote, status, reason

    def _binance_style_store_open_interest(
        self,
        venue_sym: str,
        *,
        open_interest_quote: float,
        mark_price: float,
        observed_at_ms: int,
        status: str,
        reason: str,
    ) -> None:
        is_available = status == "available" and open_interest_quote > 0.0
        is_transient_failure = status in _BINANCE_STYLE_TRANSIENT_OPEN_INTEREST_STATUSES
        if not (is_available or is_transient_failure) or mark_price <= 0.0:
            return
        self._binance_style_open_interest_cache[
            self._binance_style_oi_cache_key(venue_sym)
        ] = (open_interest_quote, mark_price, observed_at_ms, status, reason)

    async def _fetch_binance_style(self, symbols: list[str]) -> dict[str, FundingTicker]:
        spec = self._spec
        venue_str = spec.venue_id.value
        now_ms = _now_ms()
        canonical_symbols = {s.upper() for s in symbols}
        venue_sym_to_canon: dict[str, str] = {}
        for s in symbols:
            venue_sym_to_canon[self._to_venue_symbol(s)] = s.upper()

        # 1. bookTicker (bid/ask)
        raw_tickers = await self._public_get(spec.funding_ticker_path)
        items = raw_tickers if isinstance(raw_tickers, list) else [raw_tickers]

        ticker_map: dict[str, dict] = {}
        for item in items:
            sym = str(item.get("symbol", ""))
            if sym in venue_sym_to_canon:
                ticker_map[sym] = item

        # 2. premiumIndex (mark, index, funding rate, funding time)
        raw_pi: list[dict] = []
        if spec.premium_index_path:
            pi_resp = await self._public_get(spec.premium_index_path)
            raw_pi = pi_resp if isinstance(pi_resp, list) else [pi_resp]

        pi_map: dict[str, dict] = {}
        for item in raw_pi:
            sym = str(item.get("symbol", ""))
            if sym in venue_sym_to_canon:
                pi_map[sym] = item

        # 3. 24hr (volume)
        vol_map: dict[str, float] = {}
        if spec.volume_24h_path:
            raw_24 = await self._public_get(spec.volume_24h_path)
            items_24 = raw_24 if isinstance(raw_24, list) else [raw_24]
            for item in items_24:
                sym = str(item.get("symbol", ""))
                if sym in venue_sym_to_canon:
                    vol_map[sym] = _safe_float(item.get("quoteVolume", item.get("volume", 0)))

        # 4. openInterest enrichment. Binance-compatible venues expose this per
        # symbol. V1 computes quote-notional OI as openInterest * markPrice.
        # This endpoint is evidence-only here: slow/error/cooldown OI must not
        # hold quote coverage hostage.
        oi_map: dict[str, float] = {}
        oi_evidence_status: dict[str, str] = {
            venue_sym: "unavailable" if spec.open_interest_path else "unsupported"
            for venue_sym in venue_sym_to_canon
        }
        oi_evidence_reason: dict[str, str] = {
            venue_sym: "not_refreshed" if spec.open_interest_path else "unsupported"
            for venue_sym in venue_sym_to_canon
        }
        if spec.open_interest_path:
            sem = asyncio.Semaphore(_BINANCE_STYLE_OPEN_INTEREST_CONCURRENCY)
            oi_candidate_count = 0
            oi_cache_hit_count = 0
            oi_cache_miss_count = 0
            oi_refresh_attempt_count = 0
            oi_deferred_count = 0
            oi_timeout_count = 0
            oi_refresh_elapsed_ms = 0

            def _cache_timeout(venue_sym: str) -> None:
                self._binance_style_store_open_interest(
                    venue_sym,
                    open_interest_quote=0.0,
                    mark_price=_safe_float(
                        pi_map.get(venue_sym, {}).get("markPrice", 0)
                    ),
                    observed_at_ms=now_ms,
                    status="timeout",
                    reason="timeout_waiting_for_oi",
                )

            async def _fetch_oi(venue_sym: str) -> tuple[str, float, str]:
                async with sem:
                    try:
                        raw_oi = await self._public_get(
                            spec.open_interest_path,
                            params={"symbol": venue_sym},
                        )
                    except Exception as exc:
                        return venue_sym, 0.0, self._binance_style_oi_status_from_error(exc)
                    item = raw_oi[0] if isinstance(raw_oi, list) and raw_oi else raw_oi
                    if isinstance(item, dict):
                        open_interest = _safe_float(item.get("openInterest", 0))
                        mark_price = _safe_float(
                            pi_map.get(venue_sym, {}).get("markPrice", 0)
                        )
                        if mark_price > 0.0:
                            return venue_sym, open_interest * mark_price, "available"
                        return venue_sym, 0.0, "missing_mark_price"
                    return venue_sym, 0.0, "parse_error"

            oi_symbols: list[str] = []
            for sym in venue_sym_to_canon:
                if sym not in pi_map:
                    continue
                oi_candidate_count += 1
                mark_price = _safe_float(pi_map.get(sym, {}).get("markPrice", 0))
                cached = self._binance_style_cached_open_interest(
                    sym,
                    mark_price=mark_price,
                    now_ms=now_ms,
                )
                if cached is not None:
                    oi_cache_hit_count += 1
                    oi_value, status, reason = cached
                    if status == "available":
                        oi_map[sym] = oi_value
                    oi_evidence_status[sym] = status
                    oi_evidence_reason[sym] = reason or "cache_hit"
                    continue
                oi_cache_miss_count += 1
                oi_symbols.append(sym)
            refresh_cap = int(
                getattr(
                    self,
                    "binance_style_open_interest_refresh_cap",
                    BINANCE_STYLE_OPEN_INTEREST_REFRESH_CAP,
                )
                or BINANCE_STYLE_OPEN_INTEREST_REFRESH_CAP
            )
            refresh_symbols = oi_symbols[:max(refresh_cap, 0)]
            oi_refresh_attempt_count = len(refresh_symbols)
            for deferred_sym in oi_symbols[len(refresh_symbols):]:
                oi_evidence_status[deferred_sym] = "deferred_by_cap"
                oi_evidence_reason[deferred_sym] = "refresh_cap_exceeded"
            oi_deferred_count = max(len(oi_symbols) - len(refresh_symbols), 0)
            tasks = [
                asyncio.create_task(_fetch_oi(sym), name=sym)
                for sym in refresh_symbols
            ]
            if tasks:
                try:
                    refresh_started_ms = _now_ms()
                    oi_budget_s = float(
                        getattr(
                            self,
                            "binance_style_open_interest_enrichment_budget_s",
                            BINANCE_STYLE_OPEN_INTEREST_ENRICHMENT_BUDGET_S,
                        )
                        or 0.0
                    )
                    done, pending = await asyncio.wait(
                        tasks,
                        timeout=max(oi_budget_s, 0.0),
                    )
                    oi_refresh_elapsed_ms = max(_now_ms() - refresh_started_ms, 0)
                    for task in done:
                        try:
                            venue_sym, open_interest_quote, status = task.result()
                        except Exception:
                            continue
                        oi_evidence_status[venue_sym] = status
                        oi_evidence_reason[venue_sym] = status
                        if status == "available":
                            oi_map[venue_sym] = open_interest_quote
                        mark_price = _safe_float(
                            pi_map.get(venue_sym, {}).get("markPrice", 0)
                        )
                        self._binance_style_store_open_interest(
                            venue_sym,
                            open_interest_quote=open_interest_quote,
                            mark_price=mark_price,
                            observed_at_ms=now_ms,
                            status=status,
                            reason="fresh_refresh" if status == "available" else status,
                        )
                    for task in pending:
                        try:
                            venue_sym = task.get_name()
                        except Exception:
                            venue_sym = ""
                        if venue_sym:
                            oi_evidence_status[venue_sym] = "timeout"
                            oi_evidence_reason[venue_sym] = "timeout_waiting_for_oi"
                            _cache_timeout(venue_sym)
                    oi_timeout_count = len(pending)
                finally:
                    # The sidecar owns this whole fetch through an outer timeout.
                    # Its cancellation can interrupt asyncio.wait before the local
                    # budget expires, so every child must be cancelled and awaited
                    # here rather than only on the normal timeout path.
                    remaining = [task for task in tasks if not task.done()]
                    for task in remaining:
                        venue_sym = task.get_name()
                        _cache_timeout(venue_sym)
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
        else:
            oi_candidate_count = 0
            oi_cache_hit_count = 0
            oi_cache_miss_count = 0
            oi_refresh_attempt_count = 0
            refresh_cap = 0
            oi_deferred_count = 0
            oi_timeout_count = 0
            oi_refresh_elapsed_ms = 0

        result: dict[str, FundingTicker] = {}
        for venue_sym, canon in venue_sym_to_canon.items():
            t = ticker_map.get(venue_sym, {})
            pi = pi_map.get(venue_sym, {})
            if not pi:
                # V1 parity: not a perpetual contract — no premiumIndex data → skip
                continue
            result[f"{venue_str}:{canon}"] = FundingTicker(
                venue=venue_str,
                symbol=canon,
                bid=_safe_float(t.get("bidPrice", 0)),
                ask=_safe_float(t.get("askPrice", 0)),
                bid_size=_safe_float(t.get("bidQty", 0)),
                ask_size=_safe_float(t.get("askQty", 0)),
                mark_price=_safe_float(pi.get("markPrice", 0)),
                index_price=_safe_float(pi.get("indexPrice", 0)),
                funding_rate_bps=_safe_float(pi.get("lastFundingRate", 0)) * 10000.0,
                funding_timestamp_ms=int(_safe_float(pi.get("nextFundingTime", 0))),
                volume_24h_quote=vol_map.get(venue_sym, 0.0),
                open_interest_quote=oi_map.get(venue_sym, 0.0),
                open_interest_evidence_status=oi_evidence_status.get(
                    venue_sym,
                    "unavailable" if spec.open_interest_path else "unsupported",
                ),
                open_interest_evidence_reason=oi_evidence_reason.get(venue_sym, ""),
                oi_candidate_count=oi_candidate_count,
                oi_cache_hit_count=oi_cache_hit_count,
                oi_cache_miss_count=oi_cache_miss_count,
                oi_refresh_attempt_count=oi_refresh_attempt_count,
                oi_refresh_cap=refresh_cap,
                oi_deferred_count=oi_deferred_count,
                oi_timeout_count=oi_timeout_count,
                oi_refresh_elapsed_ms=oi_refresh_elapsed_ms,
            )
        return result

    # -- OKX (with V1 funding cache) -------------------------------------

    def _funding_rate_is_fresh(self, cache_key: str, now_ms: int) -> bool:
        """V1 parity: okx_funding_cache_entry_is_fresh.

        Two conditions must hold (src/live/okx.rs:313-316):
        1. funding_timestamp_ms is sufficiently in the future
        2. observed_at_ms is within _FUNDING_CACHE_MAX_OBSERVED_AGE_MS
        """
        entry = self._funding_cache.get(cache_key)
        if entry is None:
            return False
        _rate_bps, funding_ts_ms, observed_at_ms = entry
        if funding_ts_ms <= now_ms + _FUNDING_CACHE_MIN_FUTURE_MS:
            return False
        if now_ms - observed_at_ms > _FUNDING_CACHE_MAX_OBSERVED_AGE_MS:
            return False
        return True

    async def _fetch_okx_style(self, symbols: list[str]) -> dict[str, FundingTicker]:
        spec = self._spec
        venue_str = spec.venue_id.value
        canonical_symbols = {s.upper() for s in symbols}
        venue_sym_to_canon: dict[str, str] = {}
        for s in symbols:
            venue_sym = self._to_venue_symbol(s)
            venue_sym_to_canon[venue_sym] = s.upper()
            # V1 parity: OKX drops 1000/1000000 prefix from some contracts
            # e.g. 1000BONKUSDT → BONK-USDT-SWAP (not 1000BONK-USDT-SWAP)
            for prefix in ("1000000", "1000"):
                if s.upper().startswith(prefix):
                    stripped_sym = s.upper()[len(prefix):]
                    stripped_venue = self._to_venue_symbol(stripped_sym)
                    if stripped_venue != venue_sym:
                        venue_sym_to_canon[stripped_venue] = s.upper()
                    break  # only strip the longest matching prefix

        now_ms = _now_ms()

        # 1. market/tickers?instType=SWAP (bid/ask, volume, OI from volCcy24h)
        ticker_path = spec.funding_ticker_path
        raw = await self._public_get(ticker_path, params={"instType": "SWAP"})
        data = raw.get("data", [])
        items = data if isinstance(data, list) else [data]

        ticker_map: dict[str, dict] = {}
        for item in items:
            sym = str(item.get("instId", ""))
            if sym in venue_sym_to_canon:
                ticker_map[sym] = item

        # 2. per-symbol funding-rate with V1 parity cache
        funding_map: dict[str, dict] = {}
        if spec.funding_rate_path:
            # Separate symbols into cache-hit (fresh) and cache-miss (need fetch)
            symbols_to_fetch: list[str] = []
            for venue_sym in venue_sym_to_canon:
                cache_key = f"{venue_str}:{venue_sym_to_canon[venue_sym]}"
                if self._funding_rate_is_fresh(cache_key, now_ms):
                    rate_bps, ts_ms, _ = self._funding_cache[cache_key]
                    funding_map[venue_sym] = {
                        "fundingRate": str(rate_bps / 10000.0),
                        "nextFundingTime": str(ts_ms),
                    }
                else:
                    symbols_to_fetch.append(venue_sym)

            # Fetch only stale or missing symbols
            if symbols_to_fetch:
                symbols_to_fetch.sort()
                refresh_count = min(
                    len(symbols_to_fetch),
                    OKX_BULK_FUNDING_REST_FALLBACK_LIMIT,
                )
                refresh_start = 0
                if self._okx_funding_refresh_after_symbol is not None:
                    refresh_start = next(
                        (
                            index
                            for index, symbol in enumerate(symbols_to_fetch)
                            if symbol > self._okx_funding_refresh_after_symbol
                        ),
                        0,
                    )
                refresh_symbols = [
                    symbols_to_fetch[(refresh_start + index) % len(symbols_to_fetch)]
                    for index in range(refresh_count)
                ]
                self._okx_funding_refresh_after_symbol = refresh_symbols[-1]

                async def _fetch_funding(venue_sym: str) -> None:
                    try:
                        fr = await asyncio.wait_for(
                            self._public_get(
                                spec.funding_rate_path,
                                params={"instId": venue_sym},
                            ),
                            timeout=_OKX_FUNDING_RATE_PER_SYMBOL_TIMEOUT_S,
                        )
                    except (PublicTransportError, asyncio.TimeoutError):
                        return
                    fr_data = fr.get("data", [])
                    if isinstance(fr_data, list) and fr_data:
                        item = fr_data[0]
                        funding_map[venue_sym] = item
                        # V1 parity: update cache
                        cache_key = f"{venue_str}:{venue_sym_to_canon.get(venue_sym, venue_sym)}"
                        rate_bps = _safe_float(item.get("fundingRate", 0)) * 10000.0
                        ts_ms = _funding_timestamp_ms(item)
                        if ts_ms > 0:
                            self._funding_cache[cache_key] = (rate_bps, ts_ms, now_ms)

                tasks = [asyncio.create_task(_fetch_funding(sym)) for sym in refresh_symbols]
                try:
                    await asyncio.gather(*tasks)
                finally:
                    # The service may still cancel the whole refresh during
                    # shutdown or its outer deadline.  That lifecycle path
                    # must await every child; ordinary quote publication never
                    # cancels an already-open funding request for speed.
                    remaining = [task for task in tasks if not task.done()]
                    for task in remaining:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)

        # 3. open-interest?instType=SWAP
        oi_map: dict[str, float] = {}
        if spec.open_interest_path:
            try:
                oi_raw = await self._public_get(spec.open_interest_path, params={"instType": "SWAP"})
                oi_data = oi_raw.get("data", [])
                for item in (oi_data if isinstance(oi_data, list) else [oi_data]):
                    sym = str(item.get("instId", ""))
                    if sym in venue_sym_to_canon:
                        oi_map[sym] = _safe_float(item.get("oi", 0))
            except PublicTransportError:
                pass

        result: dict[str, FundingTicker] = {}
        seen_canon: set[str] = set()  # dedup: 1000-prefix stripping may produce duplicate entries
        for venue_sym, canon in venue_sym_to_canon.items():
            if canon in seen_canon:
                continue
            t = ticker_map.get(venue_sym, {})
            if not t:
                # V1 parity: symbol not listed on OKX — skip (no quote row)
                continue
            seen_canon.add(canon)
            fr = funding_map.get(venue_sym, {})
            vol_ccy = _safe_float(t.get("volCcy24h", 0))
            last = _safe_float(t.get("last", 0))
            result[f"{venue_str}:{canon}"] = FundingTicker(
                venue=venue_str,
                symbol=canon,
                bid=_safe_float(t.get("bidPx", 0)),
                ask=_safe_float(t.get("askPx", 0)),
                bid_size=_safe_float(t.get("bidSz", 0)),
                ask_size=_safe_float(t.get("askSz", 0)),
                mark_price=_safe_float(fr.get("markPrice", t.get("markPx", 0))),
                index_price=_safe_float(fr.get("indexPrice", 0)),
                funding_rate_bps=_safe_float(fr.get("fundingRate", 0)) * 10000.0,
                funding_timestamp_ms=_funding_timestamp_ms(fr),
                volume_24h_quote=vol_ccy * last if vol_ccy > 0 and last > 0 else vol_ccy,
                open_interest_quote=oi_map.get(venue_sym, 0.0),
            )
        return result

    # -- Bybit -------------------------------------------------------------

    async def _fetch_bybit_style(self, symbols: list[str]) -> dict[str, FundingTicker]:
        spec = self._spec
        venue_str = spec.venue_id.value
        venue_sym_to_canon: dict[str, str] = {}
        for s in symbols:
            venue_sym_to_canon[self._to_venue_symbol(s)] = s.upper()

        raw = await self._public_get(spec.funding_ticker_path, params={"category": "linear"})
        result_wrap = raw.get("result", raw)
        items = result_wrap.get("list", []) if isinstance(result_wrap, dict) else (result_wrap if isinstance(result_wrap, list) else [])

        result: dict[str, FundingTicker] = {}
        for item in items:
            sym = str(item.get("symbol", ""))
            canon = venue_sym_to_canon.get(sym)
            if canon is None:
                continue
            result[f"{venue_str}:{canon}"] = FundingTicker(
                venue=venue_str,
                symbol=canon,
                bid=_safe_float(item.get("bid1Price", 0)),
                ask=_safe_float(item.get("ask1Price", 0)),
                bid_size=_safe_float(item.get("bid1Size", 0)),
                ask_size=_safe_float(item.get("ask1Size", 0)),
                mark_price=_safe_float(item.get("markPrice", 0)),
                index_price=_safe_float(item.get("indexPrice", 0)),
                funding_rate_bps=_safe_float(item.get("fundingRate", 0)) * 10000.0,
                funding_timestamp_ms=_funding_timestamp_ms(item, fallback_ms=_now_ms()),
                volume_24h_quote=_safe_float(item.get("turnover24h", 0)),
                open_interest_quote=_safe_float(item.get("openInterestValue", 0)),
            )
        return result

    # -- Bitget ------------------------------------------------------------

    async def _fetch_bitget_style(self, symbols: list[str]) -> dict[str, FundingTicker]:
        spec = self._spec
        venue_str = spec.venue_id.value
        venue_sym_to_canon: dict[str, str] = {}
        for s in symbols:
            venue_sym_to_canon[self._to_venue_symbol(s)] = s.upper()

        raw = await self._public_get(spec.funding_ticker_path, params={"productType": "USDT-FUTURES"})
        data = raw.get("data", [])
        items = data if isinstance(data, list) else [data]

        now_ms = _now_ms()
        funding_map: dict[str, dict[str, Any]] = {}
        if spec.funding_rate_path:
            try:
                funding_raw = await self._public_get(
                    spec.funding_rate_path,
                    params={"productType": "USDT-FUTURES"},
                )
            except PublicTransportError:
                funding_raw = {}
            funding_data = funding_raw.get("data", []) if isinstance(funding_raw, dict) else []
            funding_items = funding_data if isinstance(funding_data, list) else [funding_data]
            for funding_item in funding_items:
                if not isinstance(funding_item, dict):
                    continue
                symbol = str(funding_item.get("symbol", ""))
                if symbol in venue_sym_to_canon:
                    funding_map[symbol] = funding_item

        result: dict[str, FundingTicker] = {}
        for item in items:
            sym = str(item.get("symbol", ""))
            canon = venue_sym_to_canon.get(sym)
            if canon is None:
                continue
            mark = _safe_float(item.get("markPrice", item.get("lastPr", item.get("last", 0))))
            holding_amount = _safe_float(item.get("holdingAmount", item.get("openInterest", 0)))
            funding_item = funding_map.get(sym, {})
            funding_timestamp_ms = _funding_timestamp_ms_or_seconds(
                funding_item.get("nextUpdate", 0)
            )
            if funding_timestamp_ms <= now_ms + _FUNDING_CACHE_MIN_FUTURE_MS:
                funding_timestamp_ms = 0
            result[f"{venue_str}:{canon}"] = FundingTicker(
                venue=venue_str,
                symbol=canon,
                bid=_safe_float(item.get("bidPr", item.get("bestBid", 0))),
                ask=_safe_float(item.get("askPr", item.get("bestAsk", 0))),
                bid_size=_safe_float(item.get("bidSz", 0)),
                ask_size=_safe_float(item.get("askSz", 0)),
                mark_price=mark,
                index_price=_safe_float(item.get("indexPrice", 0)),
                funding_rate_bps=_safe_float(
                    funding_item.get("fundingRate", item.get("fundingRate", 0))
                ) * 10000.0,
                funding_timestamp_ms=funding_timestamp_ms,
                volume_24h_quote=_safe_float(item.get("usdtVolume", item.get("quoteVolume", 0))),
                open_interest_quote=holding_amount * mark
                if holding_amount > 0 and mark > 0
                else 0.0,
            )
        return result

    # -- Gate --------------------------------------------------------------

    async def _fetch_gate_style(self, symbols: list[str]) -> dict[str, FundingTicker]:
        spec = self._spec
        venue_str = spec.venue_id.value
        venue_sym_to_canon: dict[str, str] = {}
        for s in symbols:
            venue_sym_to_canon[self._to_venue_symbol(s)] = s.upper()

        raw = await self._public_get(spec.funding_ticker_path)
        items = raw if isinstance(raw, list) else [raw]

        now_ms = _now_ms()
        contract_map: dict[str, dict[str, Any]] = {}
        missing_contract_symbols: set[str] = set()
        for venue_sym, canon in venue_sym_to_canon.items():
            cache_key = f"{venue_str}:{canon}"
            if self._funding_rate_is_fresh(cache_key, now_ms):
                rate_bps, timestamp_ms, _observed_at_ms = self._funding_cache[cache_key]
                contract_map[venue_sym] = {
                    "funding_rate": str(rate_bps / 10000.0),
                    "funding_next_apply": timestamp_ms,
                }
            else:
                missing_contract_symbols.add(venue_sym)

        if spec.funding_contracts_path and missing_contract_symbols:
            try:
                contracts_raw = await self._public_get(spec.funding_contracts_path)
            except PublicTransportError:
                contracts_raw = []
            contract_items = contracts_raw if isinstance(contracts_raw, list) else [contracts_raw]
            for contract_item in contract_items:
                if not isinstance(contract_item, dict):
                    continue
                symbol = str(contract_item.get("name", contract_item.get("contract", "")))
                if not symbol:
                    continue
                funding_timestamp_ms = _funding_timestamp_ms_or_seconds(
                    contract_item.get("funding_next_apply", 0)
                )
                funding_is_fresh = funding_timestamp_ms > now_ms + _FUNDING_CACHE_MIN_FUTURE_MS
                funding_rate = contract_item.get("funding_rate")
                if funding_is_fresh and funding_rate not in (None, ""):
                    canon = venue_sym_to_canon.get(symbol, self._from_venue_symbol(symbol).upper())
                    self._funding_cache[f"{venue_str}:{canon}"] = (
                        _safe_float(funding_rate) * 10000.0,
                        funding_timestamp_ms,
                        now_ms,
                    )
                if symbol in venue_sym_to_canon and (
                    funding_is_fresh or symbol not in contract_map
                ):
                    contract_map[symbol] = contract_item

        result: dict[str, FundingTicker] = {}
        for item in items:
            sym = str(item.get("contract", ""))
            canon = venue_sym_to_canon.get(sym)
            if canon is None:
                continue
            mark = _safe_float(item.get("mark_price", 0))
            quanto = _safe_float(item.get("quanto_multiplier", 1.0))
            oi_contracts = _safe_float(item.get("total_size", 0))
            contract_item = contract_map.get(sym, {})
            funding_timestamp_ms = _funding_timestamp_ms_or_seconds(
                contract_item.get("funding_next_apply", 0)
            )
            if funding_timestamp_ms <= now_ms + _FUNDING_CACHE_MIN_FUTURE_MS:
                funding_timestamp_ms = 0
            result[f"{venue_str}:{canon}"] = FundingTicker(
                venue=venue_str,
                symbol=canon,
                bid=_safe_float(item.get("highest_bid", 0)),
                ask=_safe_float(item.get("lowest_ask", 0)),
                bid_size=_safe_float(item.get("highest_size", item.get("bid_size", 0))) * quanto,
                ask_size=_safe_float(item.get("lowest_size", item.get("ask_size", 0))) * quanto,
                mark_price=mark,
                index_price=_safe_float(item.get("index_price", 0)),
                funding_rate_bps=_safe_float(
                    contract_item.get("funding_rate", item.get("funding_rate", 0))
                ) * 10000.0,
                funding_timestamp_ms=funding_timestamp_ms,
                volume_24h_quote=_safe_float(item.get("volume_24h_quote", item.get("volume_24h", 0))),
                open_interest_quote=oi_contracts * quanto * mark if quanto > 0 and mark > 0 else oi_contracts,
            )
        return result

    # -- Hyperliquid -------------------------------------------------------

    async def _fetch_hyperliquid_style(self, symbols: list[str]) -> dict[str, FundingTicker]:
        spec = self._spec
        venue_str = spec.venue_id.value
        canonical_set = {s.upper() for s in symbols}
        observed_at_ms = _now_ms()

        # Use metaAndAssetCtxs as a bulk call (plan requirement)
        body = {"type": "metaAndAssetCtxs"}
        raw = await self._public_post(spec.funding_ticker_path, body=body)

        # Response: [universe, assetCtxs]
        universe: list[dict] = []
        asset_ctxs: list[dict] = []
        if isinstance(raw, list) and len(raw) >= 2:
            if isinstance(raw[0], dict):
                maybe_universe = raw[0].get("universe", [])
                universe = maybe_universe if isinstance(maybe_universe, list) else []
            else:
                universe = raw[0] if isinstance(raw[0], list) else []
            asset_ctxs = raw[1] if isinstance(raw[1], list) else []

        # Official metaAndAssetCtxs returns asset contexts parallel to universe.
        # Keep name lookup for wrappers that add "coin", but trust index parity.
        ctx_by_name: dict[str, dict] = {}
        ctx_by_index: dict[int, dict] = {}
        for idx, ctx in enumerate(asset_ctxs):
            if not isinstance(ctx, dict):
                continue
            ctx_by_index[idx] = ctx
            name = str(ctx.get("coin", "") or "")
            if name:
                ctx_by_name[name] = ctx

        # V1 parity: funding timestamp is next hour boundary
        funding_ts = _next_hour_boundary(observed_at_ms)

        result: dict[str, FundingTicker] = {}
        for idx, item in enumerate(universe):
            if not isinstance(item, dict):
                continue
            if bool(item.get("isDelisted", False)):
                continue
            name = str(item.get("name", ""))
            canon = name + "USDT"
            if canon.upper() not in canonical_set and canon not in canonical_set:
                continue

            ctx = ctx_by_name.get(name) or ctx_by_index.get(idx, {})
            mark = _safe_float(ctx.get("markPx", item.get("markPx", 0)))

            # V1 parity: mid price fallback from midPx → markPx
            mid_price = _safe_float(ctx.get("midPx", 0))
            if mid_price <= 0:
                mid_price = mark

            # V1 parity: bid/ask from impact prices list (sorted: [bid, ask, ...])
            impact_pxs = ctx.get("impactPxs")
            if isinstance(impact_pxs, list) and len(impact_pxs) >= 2:
                pxs = sorted(_safe_float(v) for v in impact_pxs[:2])
                best_bid = pxs[0] if pxs[0] > 0 else mid_price
                best_ask = pxs[1] if pxs[1] > 0 else mid_price
            else:
                best_bid = mid_price
                best_ask = mid_price

            if best_bid <= 0 or best_ask <= 0:
                continue

            # V1 parity: bid/ask sizes from impact notional
            impact_notional = 6_000.0 if name not in ("BTC", "ETH") else 20_000.0
            bid_size = impact_notional / best_bid if best_bid > 0 else 0.0
            ask_size = impact_notional / best_ask if best_ask > 0 else 0.0

            result[f"{venue_str}:{canon.upper()}"] = FundingTicker(
                venue=venue_str,
                symbol=canon.upper(),
                bid=best_bid,
                ask=best_ask,
                bid_size=bid_size,
                ask_size=ask_size,
                mark_price=mark,
                index_price=_safe_float(item.get("indexPx", 0)),
                funding_rate_bps=_safe_float(ctx.get("funding", 0)) * 10000.0,
                funding_timestamp_ms=funding_ts,
                volume_24h_quote=_safe_float(ctx.get("dayNtlVlm", 0)),
                open_interest_quote=_safe_float(ctx.get("openInterest", 0)),
            )
        return result

    # -- Perp liquidity (volume + OI snapshot) -----------------------------

    async def fetch_perp_liquidity(self, symbols: list[str]) -> dict[str, PerpLiquidity]:
        """Fetch perp liquidity snapshot for symbols."""
        tickers = await self.fetch_funding_tickers(symbols)
        now_ms = _now_ms()
        result: dict[str, PerpLiquidity] = {}
        for key, ft in tickers.items():
            result[key] = PerpLiquidity(
                venue=ft.venue,
                symbol=ft.symbol,
                volume_24h_quote=ft.volume_24h_quote,
                open_interest_quote=ft.open_interest_quote,
                observed_at_ms=now_ms,
            )
        return result


# ---------------------------------------------------------------------------
# Transport error (lightweight, no dependency on the full transport module)
# ---------------------------------------------------------------------------


class PublicTransportErrorCategory:
    TRANSPORT_FAILURE = "transport_failure"
    TIMEOUT = "timeout"
    UNSUPPORTED_CAPABILITY = "unsupported_capability"


class PublicTransportError(Exception):
    def __init__(
        self,
        category: str,
        message: str,
        status_code: int = 0,
        retry_after_ms: int = 0,
        request_phase: str = "",
        transport_error_type: str = "",
        transport_error_detail: str = "",
        transport_error_cause_type: str = "",
        transport_error_cause: str = "",
        client_generation: int = 0,
        client_retired: bool = False,
    ) -> None:
        super().__init__(message)
        self.category = category
        self.status_code = status_code
        self.retry_after_ms = retry_after_ms
        self.request_phase = request_phase
        self.transport_error_type = transport_error_type
        self.transport_error_detail = transport_error_detail
        self.transport_error_cause_type = transport_error_cause_type
        self.transport_error_cause = transport_error_cause
        self.client_generation = client_generation
        self.client_retired = client_retired


def open_interest_evidence_status_from_error(exc: Exception) -> str:
    """Map transport failures to the canonical candidate-OI evidence status."""
    status_code = int(getattr(exc, "status_code", 0) or 0)
    category = str(getattr(exc, "category", "") or "")
    message = str(exc).lower()
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError, httpx.TimeoutException)):
        return "timeout"
    if category == PublicTransportErrorCategory.TIMEOUT:
        return "timeout"
    if status_code in (429, 418) or "429" in message or "too many" in message:
        return "rate_limited"
    if status_code in (400, 404) and (
        "invalid symbol" in message
        or "unknown symbol" in message
        or "symbol not found" in message
        or "-1121" in message
    ):
        return "unsupported"
    return "http_error"


def _parse_retry_after_ms(headers: dict[str, str]) -> Optional[int]:
    retry_after = headers.get("Retry-After", headers.get("retry-after", ""))
    if not retry_after:
        return None
    try:
        return int(retry_after) * 1000
    except ValueError:
        return None
